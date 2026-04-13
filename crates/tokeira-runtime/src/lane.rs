use std::sync::{Arc, RwLock};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use smallvec::SmallVec;
use time::OffsetDateTime;
use tokeira_kernel::{
    Command, DispatchOp, HistoryEvent, HistoryEventKind, Kernel, LoadedRun,
    StartRequest,
};
use tokeira_storage::{CommitResult, RunRepository};
use tokeira_types::{ExecutionStatus, RunKey, ShardEpoch};
use tokio::sync::{mpsc, oneshot};

use crate::{
    UpdateRegistry, UpdateResolution,
    shard::{ShardOwner, shard_for},
};

/// Configuration knobs for a single lane executor.
///
/// See [`spawn_lane`] and the
/// [runtime architecture](../../../docs/crates/runtime.md)
/// for how these values influence command processing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LaneConfig {
    /// Maximum optimistic-concurrency-control retries
    /// before surfacing a conflict error to the caller.
    pub max_occ_retries: u32,
    /// Maximum commands drained from the channel for the
    /// same run in a single activation before yielding.
    pub max_drain_per_activation: u32,
}

impl Default for LaneConfig {
    fn default() -> Self {
        Self {
            max_occ_retries: 5,
            max_drain_per_activation: 16,
        }
    }
}

/// Publishes dispatch operations produced by a committed
/// transition (workflow tasks, activity tasks, etc.).
///
/// Implementations are expected to be cheap and
/// non-blocking; the lane holds no locks while calling
/// [`publish`](DispatchPublisher::publish).
#[async_trait]
pub trait DispatchPublisher: Send + Sync {
    /// Publish a batch of [`DispatchOp`]s for `run_key`.
    async fn publish(&self, run_key: RunKey, ops: &[DispatchOp]) -> Result<()>;

    /// Submit a command to a specific run, used by
    /// orchestration follow-up paths such as child
    /// resolution delivery.
    async fn submit_to_run(
        &self,
        run_key: RunKey,
        command: Command,
    ) -> Result<CommitResult>;
}

/// A lane is a single serial command processor.
///
/// Insight: lanes are *execution locality* devices. They reduce lock pressure
/// and make it obvious which piece of code serializes commands for a run, but
/// they do not define correctness. If a run moves between lanes later, the run's
/// durable state remains the source of truth.
#[derive(Clone)]
pub struct LaneHandle {
    tx: mpsc::Sender<LaneMessage>,
}

impl LaneHandle {
    /// Submit a command for `run_key` and wait for the
    /// commit result.
    ///
    /// The command is serialized through the lane's
    /// single-threaded executor, so callers never need
    /// external locking on the run.
    pub async fn submit(
        &self,
        run_key: RunKey,
        command: Command,
    ) -> Result<CommitResult> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(LaneMessage {
                run_key,
                command,
                reply_tx,
            })
            .await?;
        reply_rx.await?
    }
}

struct LaneMessage {
    run_key: RunKey,
    command: Command,
    reply_tx: oneshot::Sender<Result<CommitResult>>,
}

/// Spawn a new lane executor as a background Tokio task.
///
/// Each lane owns a bounded channel and processes commands
/// serially. Commands for the same run are coalesced within
/// a single activation up to
/// [`LaneConfig::max_drain_per_activation`].
///
/// Returns a [`LaneHandle`] that callers use to submit
/// commands.
pub fn spawn_lane<K, R, P>(
    kernel: K,
    repo: R,
    publisher: P,
    shard_owner: Arc<RwLock<ShardOwner>>,
    activity_tracking: crate::activity_timeout::ActivityTrackingState,
    workflow_timeout_tracking: crate::timeout::WorkflowTimeoutTrackingState,
    nexus_timeout_tracking: crate::nexus::NexusTimeoutTrackingState,
    update_registry: UpdateRegistry,
    config: LaneConfig,
) -> LaneHandle
where
    K: Kernel + Send + Sync + 'static,
    R: RunRepository + 'static,
    P: DispatchPublisher + Clone + 'static,
{
    let (tx, mut rx) = mpsc::channel::<LaneMessage>(1024);
    let requeue_tx = tx.clone();
    tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            let buffered = run_activation(
                &kernel,
                &repo,
                &publisher,
                &shard_owner,
                &activity_tracking,
                &workflow_timeout_tracking,
                &nexus_timeout_tracking,
                &update_registry,
                &mut rx,
                message,
                &config,
            )
            .await;
            for message in buffered {
                if requeue_tx.send(message).await.is_err() {
                    break;
                }
            }
        }
    });
    LaneHandle { tx }
}

async fn run_activation<K, R, P>(
    kernel: &K,
    repo: &R,
    publisher: &P,
    shard_owner: &Arc<RwLock<ShardOwner>>,
    activity_tracking: &crate::activity_timeout::ActivityTrackingState,
    workflow_timeout_tracking: &crate::timeout::WorkflowTimeoutTrackingState,
    nexus_timeout_tracking: &crate::nexus::NexusTimeoutTrackingState,
    update_registry: &UpdateRegistry,
    rx: &mut mpsc::Receiver<LaneMessage>,
    first_message: LaneMessage,
    config: &LaneConfig,
) -> Vec<LaneMessage>
where
    K: Kernel + Send + Sync + 'static,
    R: RunRepository + 'static,
    P: DispatchPublisher + Clone + 'static,
{
    let active_run_key = first_message.run_key;
    let mut current = Some(first_message);
    let mut buffered = Vec::new();
    let mut drained = 0usize;
    let drain_limit = config.max_drain_per_activation.max(1) as usize;

    while let Some(message) = current.take() {
        let committed_command = message.command.clone();
            let result = handle_message(
                kernel,
                repo,
                shard_owner,
                message.run_key,
                message.command,
                config.max_occ_retries,
        )
        .await;

        let stop_draining = result.is_err();
        let reply = match result {
            Ok((commit_result, dispatch_ops, history_events)) => {
                let mut reset_materialization_error = None;
                if let CommitResult::Applied { new_state } = &commit_result {
                    for event in &history_events {
                        match &event.kind {
                            HistoryEventKind::ActivityTaskCancelRequested {
                                activity_id,
                            } => activity_tracking
                                .mark_cancel_requested(message.run_key, activity_id),
                            HistoryEventKind::ActivityTaskCompleted {
                                activity_id, ..
                            }
                            | HistoryEventKind::ActivityTaskFailed {
                                activity_id, ..
                            }
                            | HistoryEventKind::ActivityTaskTimedOut {
                                activity_id, ..
                            }
                            | HistoryEventKind::ActivityTaskCanceled {
                                activity_id, ..
                            } => activity_tracking
                                .remove(message.run_key, activity_id),
                            HistoryEventKind::WorkflowExecutionUpdateCompleted {
                                update_id,
                                result,
                            } => {
                                update_registry.notify(
                                    message.run_key,
                                    update_id,
                                    UpdateResolution::Completed {
                                        result: result.clone(),
                                    },
                                );
                            }
                            HistoryEventKind::WorkflowExecutionUpdateRejected {
                                update_id,
                                failure,
                            } => {
                                update_registry.notify(
                                    message.run_key,
                                    update_id,
                                    UpdateResolution::Rejected {
                                        failure: failure.clone(),
                                    },
                                );
                            }
                            _ => {}
                        }
                    }
                    if new_state.closed_at.is_some() {
                        workflow_timeout_tracking.remove(message.run_key);
                        nexus_timeout_tracking.remove_all_for_run(message.run_key);
                        update_registry.drain_for_run(message.run_key);
                    }
                    if new_state.closed_at.is_some() {
                        if let Some((successor_run_id, fork_event_id)) =
                            extract_reset_metadata(&history_events)
                        {
                            let successor_run_key = RunKey(successor_run_id.0);
                            if let Err(error) = repo
                                .materialize_reset_successor(
                                    message.run_key,
                                    fork_event_id,
                                    successor_run_key,
                                    successor_run_id,
                                )
                                .await
                            {
                                tracing::error!(
                                    ?error,
                                    predecessor_run_key = ?message.run_key,
                                    successor_run_key = ?successor_run_key,
                                    "failed to materialize reset successor"
                                );
                                reset_materialization_error = Some(error);
                            } else if let Ok(LoadedRun::Existing(successor_state)) =
                                repo.load_run(successor_run_key).await
                            {
                                let shard_id = {
                                    let owner = shard_owner.read().unwrap();
                                    shard_for(successor_run_key, owner.shard_count())
                                };
                                if successor_state.workflow_execution_timeout.is_some()
                                    || successor_state.workflow_run_timeout.is_some()
                                {
                                    workflow_timeout_tracking.insert(
                                        crate::timeout::WorkflowTimeoutEntry {
                                            run_key: successor_state.run_key,
                                            shard_id,
                                            workflow_execution_timeout: successor_state
                                                .workflow_execution_timeout,
                                            workflow_run_timeout: successor_state
                                                .workflow_run_timeout,
                                            started_at: successor_state.started_at,
                                            first_run_started_at: successor_state
                                                .first_run_started_at,
                                            has_retry_policy: successor_state
                                                .retry_policy
                                                .is_some(),
                                        },
                                    );
                                }
                                for activity in successor_state.activities.values() {
                                    activity_tracking.insert(
                                        crate::activity_timeout::ActivityTrackingEntry {
                                            run_key: successor_state.run_key,
                                            shard_id,
                                            activity_id: activity.activity_id.clone(),
                                            original_scheduled_at: activity.scheduled_at,
                                            last_dispatched_at: activity.scheduled_at,
                                            started_at: activity.started_at,
                                            last_heartbeat_at: None,
                                            cancel_requested: false,
                                        },
                                    );
                                }
                                for nexus in successor_state.pending_nexus_operations.values() {
                                    if let Some(schedule_to_close_timeout) =
                                        nexus.schedule_to_close_timeout
                                    {
                                        nexus_timeout_tracking.insert(
                                            crate::nexus::NexusTimeoutEntry {
                                                run_key: successor_state.run_key,
                                                shard_id,
                                                operation_id: nexus.operation_id.clone(),
                                                scheduled_event_id: nexus.scheduled_event_id,
                                                schedule_to_close_timeout,
                                                scheduled_at: nexus.scheduled_at,
                                            },
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
                if !dispatch_ops.is_empty() {
                    if let Err(error) =
                        publisher.publish(message.run_key, &dispatch_ops).await
                    {
                        tracing::warn!(?error, run_key = ?message.run_key, "failed to publish dispatch ops");
                    }
                }
                if let Some(error) = reset_materialization_error {
                    Err(error)
                } else {
                if let CommitResult::Applied { new_state } = &commit_result {
                    if let Command::NexusOperationResolved(request) = &committed_command {
                        if matches!(
                            request.resolution,
                            tokeira_kernel::NexusResolution::Completed { .. }
                                | tokeira_kernel::NexusResolution::Failed { .. }
                                | tokeira_kernel::NexusResolution::Canceled
                                | tokeira_kernel::NexusResolution::TimedOut
                        ) {
                            nexus_timeout_tracking
                                .remove(message.run_key, &request.operation_id);
                        }
                    }
                    if new_state.closed_at.is_some() {
                        if let Some(parent_run_key) = new_state.parent_run_key {
                            if extract_reset_metadata(&history_events).is_none() {
                            let maybe_resolution = match new_state.status {
                                tokeira_types::ExecutionStatus::Completed => {
                                    Some(tokeira_kernel::ChildResolution::Completed {
                                        result: new_state
                                            .close_result
                                            .clone()
                                            .unwrap_or_default(),
                                    })
                                }
                                tokeira_types::ExecutionStatus::Failed => {
                                    Some(tokeira_kernel::ChildResolution::Failed {
                                        failure: new_state
                                            .close_failure
                                            .clone()
                                            .unwrap_or_else(|| {
                                                "child workflow failed".to_string()
                                            }),
                                    })
                                }
                                tokeira_types::ExecutionStatus::Cancelled => {
                                    Some(tokeira_kernel::ChildResolution::Canceled)
                                }
                                tokeira_types::ExecutionStatus::Terminated => {
                                    Some(tokeira_kernel::ChildResolution::Terminated)
                                }
                                tokeira_types::ExecutionStatus::TimedOut => {
                                    Some(tokeira_kernel::ChildResolution::TimedOut)
                                }
                                _ => None,
                            };
                            if let Some(resolution) = maybe_resolution {
                                let command = tokeira_kernel::Command::ChildResolved(
                                    tokeira_kernel::ChildResolvedRequest {
                                        child_workflow_id: new_state.workflow_id.clone(),
                                        resolution,
                                        now: time::OffsetDateTime::now_utc(),
                                    },
                                );
                                let publisher = publisher.clone();
                                let child_run_key = message.run_key;
                                tokio::spawn(async move {
                                    if let Err(error) = publisher
                                        .submit_to_run(parent_run_key, command)
                                        .await
                                    {
                                        let error_message = error.to_string();
                                        if error_message.contains("kernel rejected")
                                            || error_message.contains("not found")
                                        {
                                            tracing::debug!(?error, parent_run_key = ?parent_run_key, child_run_key = ?child_run_key, "failed to deliver child resolution to parent");
                                        } else {
                                            tracing::warn!(?error, parent_run_key = ?parent_run_key, child_run_key = ?child_run_key, "failed to deliver child resolution to parent");
                                        }
                                    }
                                });
                            }
                            }
                        }
                        if new_state.status == ExecutionStatus::ContinuedAsNew {
                            let successor_event =
                                history_events.iter().find_map(|event| {
                                    match &event.kind {
                                    HistoryEventKind::WorkflowExecutionContinuedAsNew {
                                        new_run_id,
                                        workflow_type,
                                        task_queue,
                                        input,
                                        memo,
                                        search_attributes,
                                        workflow_execution_timeout,
                                        workflow_run_timeout,
                                        workflow_task_timeout,
                                    } => Some((
                                        *new_run_id,
                                        workflow_type.clone(),
                                        task_queue.clone(),
                                        input.clone(),
                                        memo.clone(),
                                        search_attributes.clone(),
                                        *workflow_execution_timeout,
                                        *workflow_run_timeout,
                                        *workflow_task_timeout,
                                    )),
                                    _ => None,
                                }
                                });
                            if let Some((
                                successor_run_id,
                                workflow_type,
                                task_queue,
                                input,
                                memo,
                                search_attributes,
                                workflow_execution_timeout,
                                workflow_run_timeout,
                                workflow_task_timeout,
                            )) = successor_event
                            {
                                let first_execution_run_id = Some(
                                    new_state
                                        .first_execution_run_id
                                        .unwrap_or(new_state.run_id),
                                );
                                let first_run_started_at = Some(
                                    new_state
                                        .first_run_started_at
                                        .unwrap_or(new_state.started_at),
                                );
                                let successor_run_key = RunKey::new();
                                let start_request = StartRequest {
                                    run_key: successor_run_key,
                                    namespace_id: new_state.namespace_id,
                                    workflow_id: new_state.workflow_id.clone(),
                                    run_id: successor_run_id,
                                    workflow_type,
                                    task_queue,
                                    deployment: new_state.deployment.clone(),
                                    build_id: new_state.build_id.clone(),
                                    input,
                                    memo,
                                    search_attributes,
                                    workflow_execution_timeout,
                                    workflow_run_timeout,
                                    workflow_task_timeout,
                                    retry_policy: new_state.retry_policy.clone(),
                                    conflict_policy: tokeira_kernel::WorkflowIdConflictPolicy::Fail,
                                    reuse_policy: tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
                                    attempt: 1,
                                    continued_execution_run_id: Some(new_state.run_id),
                                    first_execution_run_id,
                                    parent_run_key: None,
                                    parent_workflow_id: None,
                                    first_run_started_at,
                                    request: tokeira_types::RequestContext {
                                        request_id: tokeira_types::RequestId(format!(
                                            "continue-as-new:{}:{}",
                                            new_state.run_id.0, successor_run_id.0
                                        )),
                                        caller_identity: None,
                                        received_at: OffsetDateTime::now_utc(),
                                    },
                                    now: OffsetDateTime::now_utc(),
                                };
                                let publisher = publisher.clone();
                                let workflow_timeout_tracking =
                                    workflow_timeout_tracking.clone();
                                let predecessor_run_key = message.run_key;
                                tokio::spawn(async move {
                                    match publisher
                                        .submit_to_run(
                                            successor_run_key,
                                            Command::Start(start_request),
                                        )
                                        .await
                                    {
                                        Ok(CommitResult::Applied { new_state }) => {
                                            if new_state
                                                .workflow_execution_timeout
                                                .is_some()
                                                || new_state
                                                    .workflow_run_timeout
                                                    .is_some()
                                            {
                                                workflow_timeout_tracking.insert(
                                                    crate::timeout::WorkflowTimeoutEntry {
                                                        run_key: new_state.run_key,
                                                        shard_id: crate::shard::shard_for(
                                                            new_state.run_key,
                                                            1,
                                                        ),
                                                        workflow_execution_timeout: new_state
                                                            .workflow_execution_timeout,
                                                        workflow_run_timeout: new_state
                                                            .workflow_run_timeout,
                                                        started_at: new_state.started_at,
                                                        first_run_started_at: new_state
                                                            .first_run_started_at,
                                                        has_retry_policy: new_state
                                                            .retry_policy
                                                            .is_some(),
                                                    },
                                                );
                                            }
                                        }
                                        Ok(CommitResult::Duplicate) => {
                                            tracing::error!(
                                                predecessor_run_key = ?predecessor_run_key,
                                                successor_run_key = ?successor_run_key,
                                                "unexpected duplicate when starting continue-as-new successor"
                                            );
                                        }
                                        Ok(CommitResult::Conflict { reason }) => {
                                            tracing::error!(
                                                predecessor_run_key = ?predecessor_run_key,
                                                successor_run_key = ?successor_run_key,
                                                %reason,
                                                "unexpected conflict when starting continue-as-new successor"
                                            );
                                        }
                                        Err(error) => {
                                            tracing::error!(
                                                ?error,
                                                predecessor_run_key = ?predecessor_run_key,
                                                successor_run_key = ?successor_run_key,
                                                "failed to start continue-as-new successor"
                                            );
                                        }
                                    }
                                });
                            } else {
                                tracing::error!(
                                    predecessor_run_key = ?message.run_key,
                                    "continued-as-new close missing WorkflowExecutionContinuedAsNew history event"
                                );
                            }
                        }
                    }
                }
                Ok(commit_result)
                }
            }
            Err(error) => Err(error),
        };
        let _ = message.reply_tx.send(reply);
        drained += 1;

        if stop_draining || drained >= drain_limit {
            break;
        }

        match rx.try_recv() {
            Ok(next) if next.run_key == active_run_key => {
                current = Some(next);
            }
            Ok(other) => {
                buffered.push(other);
                break;
            }
            Err(mpsc::error::TryRecvError::Empty) => break,
            Err(mpsc::error::TryRecvError::Disconnected) => break,
        }
    }

    buffered
}

async fn handle_message<K, R>(
    kernel: &K,
    repo: &R,
    shard_owner: &Arc<RwLock<ShardOwner>>,
    run_key: RunKey,
    command: Command,
    max_retries: u32,
) -> Result<(
    CommitResult,
    SmallVec<[DispatchOp; 4]>,
    SmallVec<[HistoryEvent; 8]>,
)>
where
    K: Kernel + Send + Sync + 'static,
    R: RunRepository + 'static,
{
    let mut attempts = 0u32;
    loop {
        let loaded = repo.load_run(run_key).await?;
        let epoch = {
            let owner = shard_owner.read().unwrap();
            let shard_id = shard_for(run_key, owner.shard_count());
            owner.epoch_of(shard_id).unwrap_or(ShardEpoch::ZERO)
        };
        let transition = kernel
            .apply(loaded, command.clone())
            .map_err(|reject| anyhow!("kernel rejected command: {reject}"))?;
        let dispatch_ops = transition.dispatch_ops.clone();
        let history_events = transition.history_events.clone();

        match repo.commit_transition(run_key, transition, epoch).await? {
            CommitResult::Applied { new_state } => {
                return Ok((
                    CommitResult::Applied { new_state },
                    dispatch_ops,
                    history_events,
                ));
            }
            CommitResult::Duplicate => {
                return Ok((CommitResult::Duplicate, SmallVec::new(), SmallVec::new()));
            }
            CommitResult::Conflict { reason } => {
                if attempts >= max_retries {
                    return Err(anyhow!(
                        "lane OCC retry exhausted after {} conflicts for {:?}: {}",
                        attempts + 1,
                        run_key,
                        reason
                    ));
                }
                attempts += 1;
            }
        }
    }
}

fn extract_reset_metadata(history_events: &[HistoryEvent]) -> Option<(tokeira_types::RunId, i64)> {
    history_events.iter().find_map(|event| match &event.kind {
        HistoryEventKind::WorkflowTaskFailed {
            failure_cause: tokeira_kernel::WorkflowTaskFailedCause::ResetWorkflow,
            new_run_id: Some(new_run_id),
            fork_event_id: Some(fork_event_id),
            ..
        } => Some((*new_run_id, *fork_event_id)),
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, VecDeque},
        sync::{Arc, Mutex},
    };

    use proptest::prelude::*;
    use smallvec::smallvec;
    use time::{Duration, OffsetDateTime};
    use tokeira_kernel::{
        ActivityState, HistoryEvent, LoadedRun, PendingWorkflowTask, ProjectionOp,
        Reject, RequestDedupeOp, TimerOp, Transition, WorkflowState,
    };
    use tokeira_storage::{
        BacklogEntry, CommitResult, DispatchableActivityTask, DispatchableWorkflowTask,
        DueTimer, LeaseOutcome, LeaseRepository, ProjectionBatch, ProjectionLog,
        ProjectionRecord, RequestRecord, TransitionAuditRecord,
    };
    use tokeira_types::{
        ExecutionRef, ExecutionStatus, LogicalTaskSeq, Memo, NamespaceId, Payloads,
        ProjectionCursor, QueueKey, RequestContext, RequestId, RunId, RunKey,
        SearchAttributes, ShardEpoch, ShardId, TaskKind, TaskQueueName,
        TransitionSeq as DurableTransitionSeq, WorkerIdentity, WorkflowId, WorkflowType,
    };
    use tokio::runtime::Runtime;
    use tokio::sync::Mutex as AsyncMutex;

    use super::*;

    #[derive(Clone)]
    struct MockKernel {
        state: Arc<Mutex<MockKernelState>>,
    }

    struct MockKernelState {
        applied_commands: Vec<Command>,
        loaded_runs: Vec<LoadedRun>,
        dispatch_ops: SmallVec<[DispatchOp; 4]>,
        reject: bool,
    }

    impl MockKernel {
        fn new(dispatch_ops: SmallVec<[DispatchOp; 4]>) -> Self {
            Self {
                state: Arc::new(Mutex::new(MockKernelState {
                    applied_commands: Vec::new(),
                    loaded_runs: Vec::new(),
                    dispatch_ops,
                    reject: false,
                })),
            }
        }

        fn with_reject(self) -> Self {
            self.state.lock().unwrap().reject = true;
            self
        }

        fn snapshot(&self) -> (Vec<Command>, Vec<LoadedRun>) {
            let state = self.state.lock().unwrap();
            (state.applied_commands.clone(), state.loaded_runs.clone())
        }
    }

    impl Kernel for MockKernel {
        fn apply(
            &self,
            loaded: LoadedRun,
            command: Command,
        ) -> Result<Transition, Reject> {
            let mut state = self.state.lock().unwrap();
            state.applied_commands.push(command);
            state.loaded_runs.push(loaded.clone());
            if state.reject {
                return Err(Reject::WorkflowPaused);
            }

            let LoadedRun::Existing(current) = loaded else {
                panic!("tests expect an existing run");
            };

            let mut next_state = current.clone();
            next_state.transition_seq = current.transition_seq.next();
            next_state.last_event_id += 1;

            Ok(Transition {
                expected_seq: current.transition_seq,
                next_state,
                history_events: smallvec![HistoryEvent {
                    event_id: current.last_event_id + 1,
                    happened_at: OffsetDateTime::now_utc(),
                    kind: tokeira_kernel::HistoryEventKind::WorkflowExecutionSignaled {
                        signal_name: "test".to_string(),
                        input: Payloads::default(),
                        request_id: "req".to_string(),
                        identity: None,
                    },
                }],
                request_dedupe_ops: SmallVec::<[RequestDedupeOp; 1]>::new(),
                activity_ops: SmallVec::<[tokeira_kernel::ActivityOp; 4]>::new(),
                timer_ops: SmallVec::<[TimerOp; 4]>::new(),
                dispatch_ops: state.dispatch_ops.clone(),
                projection_ops: SmallVec::<[ProjectionOp; 8]>::new(),
            })
        }
    }

    #[derive(Clone)]
    struct MockRepo {
        state: Arc<AsyncMutex<MockRepoState>>,
    }

    struct MockRepoState {
        loaded: LoadedRun,
        load_calls: usize,
        commit_calls: usize,
        commit_behaviors: VecDeque<CommitBehavior>,
    }

    #[derive(Clone, Copy)]
    enum CommitBehavior {
        Applied,
        Conflict,
        Duplicate,
        Error,
    }

    impl MockRepo {
        fn new(initial: LoadedRun, commit_behaviors: Vec<CommitBehavior>) -> Self {
            Self {
                state: Arc::new(AsyncMutex::new(MockRepoState {
                    loaded: initial,
                    load_calls: 0,
                    commit_calls: 0,
                    commit_behaviors: commit_behaviors.into(),
                })),
            }
        }

        async fn snapshot(&self) -> (usize, usize, LoadedRun) {
            let state = self.state.lock().await;
            (state.load_calls, state.commit_calls, state.loaded.clone())
        }
    }

    #[async_trait]
    impl RunRepository for MockRepo {
        async fn resolve_execution(
            &self,
            _execution: &ExecutionRef,
        ) -> Result<Option<RunKey>> {
            Ok(None)
        }

        async fn find_latest_run(
            &self,
            _namespace_id: tokeira_types::NamespaceId,
            _workflow_id: &tokeira_types::WorkflowId,
        ) -> Result<Option<RunKey>> {
            Ok(None)
        }

        async fn load_run(&self, _run_key: RunKey) -> Result<LoadedRun> {
            let mut state = self.state.lock().await;
            state.load_calls += 1;
            Ok(state.loaded.clone())
        }

        async fn read_history(
            &self,
            _run_key: RunKey,
            _after_event_id: i64,
            _limit: usize,
        ) -> Result<Vec<HistoryEvent>> {
            Ok(Vec::new())
        }

        async fn lookup_request_dedupe(
            &self,
            _execution: &ExecutionRef,
            _request_id: &RequestId,
        ) -> Result<Option<RequestRecord>> {
            Ok(None)
        }

        async fn read_transition_audit(
            &self,
            _run_key: RunKey,
        ) -> Result<Vec<TransitionAuditRecord>> {
            Ok(Vec::new())
        }

        async fn commit_transition(
            &self,
            _run_key: RunKey,
            transition: Transition,
            _epoch: ShardEpoch,
        ) -> Result<CommitResult> {
            let mut state = self.state.lock().await;
            state.commit_calls += 1;
            match state
                .commit_behaviors
                .pop_front()
                .unwrap_or(CommitBehavior::Applied)
            {
                CommitBehavior::Applied => {
                    state.loaded = LoadedRun::Existing(transition.next_state.clone());
                    Ok(CommitResult::Applied {
                        new_state: transition.next_state,
                    })
                }
                CommitBehavior::Conflict => Ok(CommitResult::Conflict {
                    reason: "conflict".to_string(),
                }),
                CommitBehavior::Duplicate => Ok(CommitResult::Duplicate),
                CommitBehavior::Error => Err(anyhow!("commit failed")),
            }
        }

        async fn materialize_reset_successor(
            &self,
            _base_run_key: RunKey,
            _fork_event_id: i64,
            successor_run_key: RunKey,
            successor_run_id: RunId,
        ) -> Result<()> {
            let mut state = self.state.lock().await;
            state.loaded = LoadedRun::Existing(sample_state(successor_run_key));
            if let LoadedRun::Existing(run) = &mut state.loaded {
                run.run_id = successor_run_id;
            }
            Ok(())
        }

        async fn list_dispatchable_workflow_tasks(
            &self,
            _queue: &QueueKey,
            _limit: usize,
        ) -> Result<Vec<DispatchableWorkflowTask>> {
            Ok(Vec::new())
        }

        async fn list_dispatchable_activity_tasks(
            &self,
            _queue: &QueueKey,
            _limit: usize,
        ) -> Result<Vec<DispatchableActivityTask>> {
            Ok(Vec::new())
        }

        async fn persist_to_backlog(&self, _entries: Vec<BacklogEntry>) -> Result<()> {
            Ok(())
        }

        async fn drain_backlog(
            &self,
            _queue: &QueueKey,
            _limit: usize,
        ) -> Result<Vec<BacklogEntry>> {
            Ok(Vec::new())
        }

        async fn list_due_timers(
            &self,
            _now: OffsetDateTime,
            _limit: usize,
        ) -> Result<Vec<DueTimer>> {
            Ok(Vec::new())
        }

        async fn list_dispatchable_workflow_tasks_for_shard(
            &self,
            _shard_id: ShardId,
            _limit: usize,
        ) -> Result<Vec<DispatchableWorkflowTask>> {
            Ok(Vec::new())
        }

        async fn list_dispatchable_activity_tasks_for_shard(
            &self,
            _shard_id: ShardId,
            _limit: usize,
        ) -> Result<Vec<DispatchableActivityTask>> {
            Ok(Vec::new())
        }

        async fn list_due_timers_for_shard(
            &self,
            _shard_id: ShardId,
            _now: OffsetDateTime,
            _limit: usize,
        ) -> Result<Vec<DueTimer>> {
            Ok(Vec::new())
        }

        async fn list_runs_with_workflow_timeouts_for_shard(
            &self,
            _shard_id: ShardId,
            _limit: usize,
        ) -> Result<Vec<tokeira_storage::WorkflowTimeoutSweepEntry>> {
            Ok(Vec::new())
        }

        async fn list_open_activities_for_shard(
            &self,
            _shard_id: ShardId,
            _limit: usize,
        ) -> Result<Vec<tokeira_storage::ActivitySweepEntry>> {
            Ok(Vec::new())
        }

        async fn list_pending_nexus_operations_for_shard(
            &self,
            _shard_id: ShardId,
            _limit: usize,
        ) -> Result<Vec<tokeira_storage::NexusSweepEntry>> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl ProjectionLog for MockRepo {
        async fn read_from(
            &self,
            _cursor: &ProjectionCursor,
            _limit: usize,
        ) -> Result<ProjectionBatch> {
            Ok(ProjectionBatch {
                records: Vec::<ProjectionRecord>::new(),
                next_cursor: ProjectionCursor::beginning(0, 1),
            })
        }
    }

    #[async_trait]
    impl LeaseRepository for MockRepo {
        async fn try_acquire_bundle(
            &self,
            _bundle: ShardId,
            _owner: String,
        ) -> Result<LeaseOutcome> {
            Ok(LeaseOutcome::Acquired {
                epoch: ShardEpoch::ZERO,
            })
        }

        async fn renew_bundle(
            &self,
            _bundle: ShardId,
            _owner: String,
            _epoch: ShardEpoch,
        ) -> Result<LeaseOutcome> {
            Ok(LeaseOutcome::Renewed {
                epoch: ShardEpoch::ZERO,
            })
        }
    }

    #[derive(Clone)]
    struct MockPublisher {
        state: Arc<AsyncMutex<MockPublisherState>>,
    }

    #[derive(Default)]
    struct MockPublisherState {
        publishes: Vec<(RunKey, Vec<DispatchOp>)>,
        submits: Vec<(RunKey, Command)>,
        submit_result: Option<CommitResult>,
        fail: bool,
    }

    impl MockPublisher {
        fn new() -> Self {
            Self {
                state: Arc::new(AsyncMutex::new(MockPublisherState::default())),
            }
        }

        async fn with_failure(self) -> Self {
            self.state.lock().await.fail = true;
            self
        }

        async fn with_submit_result(self, submit_result: CommitResult) -> Self {
            self.state.lock().await.submit_result = Some(submit_result);
            self
        }

        async fn snapshot(&self) -> MockPublisherStateSnapshot {
            let state = self.state.lock().await;
            MockPublisherStateSnapshot {
                publishes: state.publishes.clone(),
                submits: state.submits.clone(),
            }
        }
    }

    #[derive(Debug, PartialEq)]
    struct MockPublisherStateSnapshot {
        publishes: Vec<(RunKey, Vec<DispatchOp>)>,
        submits: Vec<(RunKey, Command)>,
    }

    #[async_trait]
    impl DispatchPublisher for MockPublisher {
        async fn publish(&self, run_key: RunKey, ops: &[DispatchOp]) -> Result<()> {
            let mut state = self.state.lock().await;
            state.publishes.push((run_key, ops.to_vec()));
            if state.fail {
                return Err(anyhow!("publisher failed"));
            }
            Ok(())
        }

        async fn submit_to_run(
            &self,
            run_key: RunKey,
            command: Command,
        ) -> Result<CommitResult> {
            let mut state = self.state.lock().await;
            state.submits.push((run_key, command));
            if state.fail {
                return Err(anyhow!("publisher failed"));
            }
            Ok(state
                .submit_result
                .clone()
                .unwrap_or(CommitResult::Duplicate))
        }
    }

    #[derive(Clone)]
    struct ContinueAsNewKernel {
        status: ExecutionStatus,
        include_continue_event: bool,
        workflow_execution_timeout: Option<Duration>,
        workflow_run_timeout: Option<Duration>,
        first_run_started_at: Option<OffsetDateTime>,
    }

    impl ContinueAsNewKernel {
        fn continued_as_new() -> Self {
            Self {
                status: ExecutionStatus::ContinuedAsNew,
                include_continue_event: true,
                workflow_execution_timeout: Some(Duration::minutes(30)),
                workflow_run_timeout: Some(Duration::minutes(5)),
                first_run_started_at: None,
            }
        }
    }

    impl Kernel for ContinueAsNewKernel {
        fn apply(
            &self,
            loaded: LoadedRun,
            _command: Command,
        ) -> Result<Transition, Reject> {
            let LoadedRun::Existing(current) = loaded else {
                panic!("tests expect an existing run");
            };

            let mut next_state = current.clone();
            next_state.transition_seq = current.transition_seq.next();
            next_state.last_event_id += 1;
            next_state.status = self.status;
            next_state.closed_at = Some(OffsetDateTime::now_utc());
            next_state.first_run_started_at =
                self.first_run_started_at.or(current.first_run_started_at);
            next_state.pending_workflow_task = None;

            let history_events = if self.include_continue_event {
                smallvec![HistoryEvent {
                    event_id: current.last_event_id + 1,
                    happened_at: OffsetDateTime::now_utc(),
                    kind: tokeira_kernel::HistoryEventKind::WorkflowExecutionContinuedAsNew {
                        new_run_id: RunId::new(),
                        workflow_type: WorkflowType("continued".to_string()),
                        task_queue: TaskQueueName("continued-q".to_string()),
                        input: Payloads(vec![]),
                        memo: Memo::default(),
                        search_attributes: SearchAttributes::default(),
                        workflow_execution_timeout: self.workflow_execution_timeout,
                        workflow_run_timeout: self.workflow_run_timeout,
                        workflow_task_timeout: Duration::seconds(15),
                    },
                }]
            } else {
                smallvec![HistoryEvent {
                    event_id: current.last_event_id + 1,
                    happened_at: OffsetDateTime::now_utc(),
                    kind: tokeira_kernel::HistoryEventKind::WorkflowExecutionCompleted {
                        result: Payloads::default(),
                    },
                }]
            };

            Ok(Transition {
                expected_seq: current.transition_seq,
                next_state,
                history_events,
                request_dedupe_ops: SmallVec::new(),
                activity_ops: SmallVec::new(),
                timer_ops: SmallVec::new(),
                dispatch_ops: SmallVec::new(),
                projection_ops: SmallVec::new(),
            })
        }
    }

    fn sample_state(run_key: RunKey) -> WorkflowState {
        let namespace_id = NamespaceId::new();
        WorkflowState {
            run_key,
            namespace_id,
            workflow_id: WorkflowId("workflow".to_string()),
            run_id: RunId::new(),
            workflow_type: WorkflowType("example".to_string()),
            task_queue: TaskQueueName("queue-a".to_string()),
            deployment: None,
            build_id: None,
            status: ExecutionStatus::Running,
            transition_seq: DurableTransitionSeq::ZERO,
            last_event_id: 0,
            next_workflow_task_seq: LogicalTaskSeq::ONE,
            pending_workflow_task: Some(PendingWorkflowTask {
                logical_seq: LogicalTaskSeq::ONE,
                scheduled_event_id: 1,
                started_event_id: None,
                attempt: 1,
            }),
            sticky: None,
            pause_info: None,
            wft_stamp: 0,
            memo: Memo::default(),
            search_attributes: SearchAttributes::default(),
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: Duration::seconds(10),
            retry_policy: None,
            attempt: 1,
            first_execution_run_id: None,
            parent_run_key: None,
            parent_workflow_id: None,
            activities: BTreeMap::<String, ActivityState>::new(),
            timers: BTreeMap::new(),
            children: BTreeMap::new(),
            pending_external_signals: BTreeMap::new(),
            pending_external_cancels: BTreeMap::new(),
            pending_updates: BTreeMap::new(),
            pending_nexus_operations: BTreeMap::new(),
            versioning_override: None,
            completion_callbacks: Vec::new(),
            started_at: OffsetDateTime::now_utc(),
            first_run_started_at: None,
            closed_at: None,
            close_result: None,
            close_failure: None,
        }
    }

    fn sample_command(label: &str) -> Command {
        Command::Signal(tokeira_kernel::SignalRequest {
            signal_name: label.to_string(),
            input: Payloads::default(),
            request: RequestContext {
                request_id: RequestId(format!("req-{label}")),
                caller_identity: None,
                received_at: OffsetDateTime::now_utc(),
            },
            now: OffsetDateTime::now_utc(),
        })
    }

    fn sample_dispatch_ops(namespace_id: NamespaceId) -> SmallVec<[DispatchOp; 4]> {
        smallvec![DispatchOp::EnqueueWorkflowTask {
            queue: QueueKey {
                namespace_id,
                task_queue: TaskQueueName("queue-a".to_string()),
                task_kind: TaskKind::Workflow,
                deployment: None,
                build_id: None,
            },
            logical_seq: LogicalTaskSeq::ONE,
            sticky_preferred: Some(WorkerIdentity("worker-a".to_string())),
        }]
    }

    fn lane_message(
        run_key: RunKey,
        label: &str,
    ) -> (LaneMessage, oneshot::Receiver<Result<CommitResult>>) {
        let (reply_tx, reply_rx) = oneshot::channel();
        (
            LaneMessage {
                run_key,
                command: sample_command(label),
                reply_tx,
            },
            reply_rx,
        )
    }

    fn test_shard_owner() -> Arc<RwLock<ShardOwner>> {
        let owner = Arc::new(RwLock::new(ShardOwner::new(1)));
        {
            let mut guard = owner.write().unwrap();
            let _ = guard.record_acquired(ShardId(0), ShardEpoch::ZERO);
            guard.mark_active(ShardId(0));
        }
        owner
    }

    #[test]
    fn lane_config_defaults() {
        let config = LaneConfig::default();
        assert_eq!(config.max_occ_retries, 5);
        assert_eq!(config.max_drain_per_activation, 16);
    }

    #[test]
    fn lane_config_edge_values_are_representable() {
        let config = LaneConfig {
            max_occ_retries: 0,
            max_drain_per_activation: 1,
        };
        assert_eq!(config.max_occ_retries, 0);
        assert_eq!(config.max_drain_per_activation, 1);
    }

    proptest! {
        #[test]
        fn property_reload_and_recompute_on_conflict(conflicts in 0u32..4) {
            let rt = Runtime::new().unwrap();
            let (result, load_calls, commit_calls, command_len, loaded_len) = rt.block_on(async move {
                let run_key = RunKey::new();
                let state = sample_state(run_key);
                let repo = MockRepo::new(
                    LoadedRun::Existing(state.clone()),
                    std::iter::repeat(CommitBehavior::Conflict)
                        .take(conflicts as usize)
                        .chain(std::iter::once(CommitBehavior::Applied))
                        .collect(),
                );
                let kernel = MockKernel::new(sample_dispatch_ops(state.namespace_id));
                let shard_owner = test_shard_owner();

                let (result, _, _) = handle_message(&kernel, &repo, &shard_owner, run_key, sample_command("a"), 8).await.unwrap();
                let (load_calls, commit_calls, _) = repo.snapshot().await;
                let (commands, loaded_runs) = kernel.snapshot();
                (result, load_calls, commit_calls, commands.len(), loaded_runs.len())
            });
            let applied = matches!(result, CommitResult::Applied { .. });
            prop_assert!(applied);
            prop_assert_eq!(load_calls, conflicts as usize + 1);
            prop_assert_eq!(commit_calls, conflicts as usize + 1);
            prop_assert_eq!(command_len, conflicts as usize + 1);
            prop_assert_eq!(loaded_len, conflicts as usize + 1);
        }

        #[test]
        fn property_same_command_across_retries(conflicts in 0u32..4) {
            let rt = Runtime::new().unwrap();
            let commands = rt.block_on(async move {
                let run_key = RunKey::new();
                let state = sample_state(run_key);
                let repo = MockRepo::new(
                    LoadedRun::Existing(state.clone()),
                    std::iter::repeat(CommitBehavior::Conflict)
                        .take(conflicts as usize)
                        .chain(std::iter::once(CommitBehavior::Applied))
                        .collect(),
                );
                let kernel = MockKernel::new(sample_dispatch_ops(state.namespace_id));
                let command = sample_command("stable");
                let shard_owner = test_shard_owner();

                let _ = handle_message(&kernel, &repo, &shard_owner, run_key, command.clone(), 8).await.unwrap();
                kernel.snapshot().0
            });
            prop_assert!(!commands.is_empty());
            let expected = commands[0].clone();
            for seen in commands {
                prop_assert_eq!(seen, expected.clone());
            }
        }

        #[test]
        fn property_retry_bound_and_exhaustion(max_retries in 0u32..8) {
            let rt = Runtime::new().unwrap();
            let (message, load_calls, commit_calls) = rt.block_on(async move {
                let run_key = RunKey::new();
                let state = sample_state(run_key);
                let repo = MockRepo::new(
                    LoadedRun::Existing(state.clone()),
                    std::iter::repeat(CommitBehavior::Conflict)
                        .take(max_retries as usize + 1)
                        .collect(),
                );
                let kernel = MockKernel::new(sample_dispatch_ops(state.namespace_id));
                let shard_owner = test_shard_owner();

                let error = handle_message(&kernel, &repo, &shard_owner, run_key, sample_command("bound"), max_retries)
                    .await
                    .expect_err("retry exhaustion should surface as an error");
                let (load_calls, commit_calls, _) = repo.snapshot().await;
                (error.to_string(), load_calls, commit_calls)
            });
            prop_assert!(message.contains("retry exhausted"));
            prop_assert_eq!(load_calls, max_retries as usize + 1);
            prop_assert_eq!(commit_calls, max_retries as usize + 1);
        }

        #[test]
        fn property_duplicate_passthrough_without_retry(seed in 0u8..4) {
            let rt = Runtime::new().unwrap();
            let (result, ops, load_calls, commit_calls) = rt.block_on(async move {
                let run_key = RunKey::new();
                let state = sample_state(run_key);
                let repo = MockRepo::new(
                    LoadedRun::Existing(state.clone()),
                    vec![CommitBehavior::Duplicate],
                );
                let kernel = MockKernel::new(sample_dispatch_ops(state.namespace_id));
                let shard_owner = test_shard_owner();

                let (result, ops, _) = handle_message(
                    &kernel,
                    &repo,
                    &shard_owner,
                    run_key,
                    sample_command(&format!("dup-{seed}")),
                    5,
                )
                .await
                .unwrap();

                let (load_calls, commit_calls, _) = repo.snapshot().await;
                (result, ops, load_calls, commit_calls)
            });
            let _ = seed;
            prop_assert_eq!(result, CommitResult::Duplicate);
            prop_assert!(ops.is_empty());
            prop_assert_eq!(load_calls, 1);
            prop_assert_eq!(commit_calls, 1);
        }
    }

    #[tokio::test]
    async fn run_activation_coalesces_same_run_and_uses_fresh_state() {
        let run_key = RunKey::new();
        let state = sample_state(run_key);
        let repo = MockRepo::new(
            LoadedRun::Existing(state.clone()),
            vec![CommitBehavior::Applied, CommitBehavior::Applied],
        );
        let kernel = MockKernel::new(sample_dispatch_ops(state.namespace_id));
        let publisher = MockPublisher::new();
        let (first, first_reply) = lane_message(run_key, "first");
        let (_foreign, _foreign_reply) = lane_message(RunKey::new(), "foreign");
        let (second, second_reply) = lane_message(run_key, "second");
        let (tx, mut rx) = mpsc::channel(8);
        let activity_tracking =
            crate::activity_timeout::ActivityTrackingState::default();
        let tracking = crate::timeout::WorkflowTimeoutTrackingState::default();
        let nexus_tracking = crate::nexus::NexusTimeoutTrackingState::default();
        let update_registry = crate::UpdateRegistry::new();
        let shard_owner = test_shard_owner();
        tx.send(second).await.unwrap();
        tx.send(_foreign).await.unwrap();

        let buffered = run_activation(
            &kernel,
            &repo,
            &publisher,
            &shard_owner,
            &activity_tracking,
            &tracking,
            &nexus_tracking,
            &update_registry,
            &mut rx,
            first,
            &LaneConfig {
                max_occ_retries: 5,
                max_drain_per_activation: 4,
            },
        )
        .await;

        assert!(matches!(
            first_reply.await.unwrap().unwrap(),
            CommitResult::Applied { .. }
        ));
        assert!(matches!(
            second_reply.await.unwrap().unwrap(),
            CommitResult::Applied { .. }
        ));
        assert_eq!(buffered.len(), 1);

        let (commands, loaded_runs) = kernel.snapshot();
        assert_eq!(commands.len(), 2);
        assert_eq!(loaded_runs.len(), 2);
        assert_eq!(
            loaded_runs,
            vec![
                LoadedRun::Existing(state.clone()),
                LoadedRun::Existing({
                    let mut next = state.clone();
                    next.transition_seq = state.transition_seq.next();
                    next.last_event_id = 1;
                    next
                }),
            ]
        );
        assert_eq!(publisher.snapshot().await.publishes.len(), 2);
    }

    #[tokio::test]
    async fn run_activation_honors_drain_limit() {
        let run_key = RunKey::new();
        let state = sample_state(run_key);
        let repo = MockRepo::new(
            LoadedRun::Existing(state.clone()),
            vec![
                CommitBehavior::Applied,
                CommitBehavior::Applied,
                CommitBehavior::Applied,
            ],
        );
        let kernel = MockKernel::new(sample_dispatch_ops(state.namespace_id));
        let publisher = MockPublisher::new();
        let (first, first_reply) = lane_message(run_key, "first");
        let (second, second_reply) = lane_message(run_key, "second");
        let (third, _third_reply) = lane_message(run_key, "third");
        let (tx, mut rx) = mpsc::channel(8);
        let activity_tracking =
            crate::activity_timeout::ActivityTrackingState::default();
        let tracking = crate::timeout::WorkflowTimeoutTrackingState::default();
        let nexus_tracking = crate::nexus::NexusTimeoutTrackingState::default();
        let update_registry = crate::UpdateRegistry::new();
        let shard_owner = test_shard_owner();
        tx.send(second).await.unwrap();
        tx.send(third).await.unwrap();

        let buffered = run_activation(
            &kernel,
            &repo,
            &publisher,
            &shard_owner,
            &activity_tracking,
            &tracking,
            &nexus_tracking,
            &update_registry,
            &mut rx,
            first,
            &LaneConfig {
                max_occ_retries: 5,
                max_drain_per_activation: 2,
            },
        )
        .await;

        assert!(buffered.is_empty());
        assert!(matches!(
            first_reply.await.unwrap().unwrap(),
            CommitResult::Applied { .. }
        ));
        assert!(matches!(
            second_reply.await.unwrap().unwrap(),
            CommitResult::Applied { .. }
        ));
        assert!(rx.try_recv().is_ok());
    }

    #[tokio::test]
    async fn run_activation_stops_drain_on_error() {
        let run_key = RunKey::new();
        let state = sample_state(run_key);
        let repo = MockRepo::new(
            LoadedRun::Existing(state.clone()),
            vec![
                CommitBehavior::Applied,
                CommitBehavior::Error,
                CommitBehavior::Applied,
            ],
        );
        let kernel = MockKernel::new(sample_dispatch_ops(state.namespace_id));
        let publisher = MockPublisher::new();
        let (first, first_reply) = lane_message(run_key, "first");
        let (second, second_reply) = lane_message(run_key, "second");
        let (third, _third_reply) = lane_message(run_key, "third");
        let (tx, mut rx) = mpsc::channel(8);
        let activity_tracking =
            crate::activity_timeout::ActivityTrackingState::default();
        let tracking = crate::timeout::WorkflowTimeoutTrackingState::default();
        let nexus_tracking = crate::nexus::NexusTimeoutTrackingState::default();
        let update_registry = crate::UpdateRegistry::new();
        let shard_owner = test_shard_owner();
        tx.send(second).await.unwrap();
        tx.send(third).await.unwrap();

        let buffered = run_activation(
            &kernel,
            &repo,
            &publisher,
            &shard_owner,
            &activity_tracking,
            &tracking,
            &nexus_tracking,
            &update_registry,
            &mut rx,
            first,
            &LaneConfig::default(),
        )
        .await;

        assert!(buffered.is_empty());
        assert!(matches!(
            first_reply.await.unwrap().unwrap(),
            CommitResult::Applied { .. }
        ));
        assert!(second_reply.await.unwrap().is_err());
        assert!(rx.try_recv().is_ok());
    }

    #[tokio::test]
    async fn run_activation_publishes_dispatch_ops_and_swallow_publisher_errors() {
        let run_key = RunKey::new();
        let state = sample_state(run_key);
        let dispatch_ops = sample_dispatch_ops(state.namespace_id);
        let repo = MockRepo::new(
            LoadedRun::Existing(state.clone()),
            vec![CommitBehavior::Applied],
        );
        let kernel = MockKernel::new(dispatch_ops.clone());
        let publisher = MockPublisher::new().with_failure().await;
        let (first, first_reply) = lane_message(run_key, "first");
        let (_tx, mut rx) = mpsc::channel(8);
        let activity_tracking =
            crate::activity_timeout::ActivityTrackingState::default();
        let tracking = crate::timeout::WorkflowTimeoutTrackingState::default();
        let nexus_tracking = crate::nexus::NexusTimeoutTrackingState::default();
        let update_registry = crate::UpdateRegistry::new();
        let shard_owner = test_shard_owner();

        let buffered = run_activation(
            &kernel,
            &repo,
            &publisher,
            &shard_owner,
            &activity_tracking,
            &tracking,
            &nexus_tracking,
            &update_registry,
            &mut rx,
            first,
            &LaneConfig::default(),
        )
        .await;

        assert!(buffered.is_empty());
        assert!(matches!(
            first_reply.await.unwrap().unwrap(),
            CommitResult::Applied { .. }
        ));
        assert_eq!(
            publisher.snapshot().await.publishes,
            vec![(run_key, dispatch_ops.into_vec())]
        );
    }

    #[tokio::test]
    async fn run_activation_does_not_publish_when_commit_fails() {
        let run_key = RunKey::new();
        let state = sample_state(run_key);
        let repo = MockRepo::new(
            LoadedRun::Existing(state.clone()),
            vec![CommitBehavior::Conflict],
        );
        let kernel = MockKernel::new(sample_dispatch_ops(state.namespace_id));
        let publisher = MockPublisher::new();
        let (first, first_reply) = lane_message(run_key, "first");
        let (_tx, mut rx) = mpsc::channel(8);
        let activity_tracking =
            crate::activity_timeout::ActivityTrackingState::default();
        let tracking = crate::timeout::WorkflowTimeoutTrackingState::default();
        let nexus_tracking = crate::nexus::NexusTimeoutTrackingState::default();
        let update_registry = crate::UpdateRegistry::new();
        let shard_owner = test_shard_owner();

        let _ = run_activation(
            &kernel,
            &repo,
            &publisher,
            &shard_owner,
            &activity_tracking,
            &tracking,
            &nexus_tracking,
            &update_registry,
            &mut rx,
            first,
            &LaneConfig {
                max_occ_retries: 0,
                max_drain_per_activation: 16,
            },
        )
        .await;

        assert!(first_reply.await.unwrap().is_err());
        let snapshot = publisher.snapshot().await;
        assert!(snapshot.publishes.is_empty());
        assert!(snapshot.submits.is_empty());
    }

    #[tokio::test]
    async fn run_activation_delivers_child_resolution_to_parent_on_child_close() {
        let child_run_key = RunKey::new();
        let parent_run_key = RunKey::new();
        let mut state = sample_state(child_run_key);
        state.workflow_id = WorkflowId("child-workflow".to_string());
        state.status = ExecutionStatus::Completed;
        state.parent_run_key = Some(parent_run_key);
        state.parent_workflow_id = Some(WorkflowId("parent-workflow".to_string()));
        state.close_result = Some(Payloads::default());
        state.closed_at = Some(OffsetDateTime::now_utc());

        let repo = MockRepo::new(
            LoadedRun::Existing(state.clone()),
            vec![CommitBehavior::Applied],
        );
        let kernel = MockKernel::new(SmallVec::new());
        let publisher = MockPublisher::new();
        let (first, first_reply) = lane_message(child_run_key, "first");
        let (_tx, mut rx) = mpsc::channel(8);
        let activity_tracking =
            crate::activity_timeout::ActivityTrackingState::default();
        let tracking = crate::timeout::WorkflowTimeoutTrackingState::default();
        let nexus_tracking = crate::nexus::NexusTimeoutTrackingState::default();
        let update_registry = crate::UpdateRegistry::new();
        let shard_owner = test_shard_owner();

        let buffered = run_activation(
            &kernel,
            &repo,
            &publisher,
            &shard_owner,
            &activity_tracking,
            &tracking,
            &nexus_tracking,
            &update_registry,
            &mut rx,
            first,
            &LaneConfig::default(),
        )
        .await;

        assert!(buffered.is_empty());
        assert!(matches!(
            first_reply.await.unwrap().unwrap(),
            CommitResult::Applied { .. }
        ));

        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        let snapshot = publisher.snapshot().await;
        assert_eq!(snapshot.publishes.len(), 0);
        assert_eq!(snapshot.submits.len(), 1);
        assert_eq!(snapshot.submits[0].0, parent_run_key);
        match &snapshot.submits[0].1 {
            Command::ChildResolved(request) => {
                assert_eq!(
                    request.child_workflow_id,
                    WorkflowId("child-workflow".to_string())
                );
                assert!(matches!(
                    request.resolution,
                    tokeira_kernel::ChildResolution::Completed { .. }
                ));
            }
            other => panic!("expected ChildResolved command, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn run_activation_does_not_deliver_child_resolution_for_non_child_run() {
        let run_key = RunKey::new();
        let mut state = sample_state(run_key);
        state.status = ExecutionStatus::Completed;
        state.closed_at = Some(OffsetDateTime::now_utc());

        let repo = MockRepo::new(
            LoadedRun::Existing(state.clone()),
            vec![CommitBehavior::Applied],
        );
        let kernel = MockKernel::new(SmallVec::new());
        let publisher = MockPublisher::new();
        let (first, first_reply) = lane_message(run_key, "first");
        let (_tx, mut rx) = mpsc::channel(8);
        let activity_tracking =
            crate::activity_timeout::ActivityTrackingState::default();
        let tracking = crate::timeout::WorkflowTimeoutTrackingState::default();
        let nexus_tracking = crate::nexus::NexusTimeoutTrackingState::default();
        let update_registry = crate::UpdateRegistry::new();
        let shard_owner = test_shard_owner();

        let buffered = run_activation(
            &kernel,
            &repo,
            &publisher,
            &shard_owner,
            &activity_tracking,
            &tracking,
            &nexus_tracking,
            &update_registry,
            &mut rx,
            first,
            &LaneConfig::default(),
        )
        .await;

        assert!(buffered.is_empty());
        assert!(matches!(
            first_reply.await.unwrap().unwrap(),
            CommitResult::Applied { .. }
        ));

        let snapshot = publisher.snapshot().await;
        assert!(snapshot.submits.is_empty());
    }

    #[tokio::test]
    async fn run_activation_submits_continue_as_new_successor_with_chain_fields() {
        let run_key = RunKey::new();
        let mut state = sample_state(run_key);
        let chain_start = OffsetDateTime::now_utc() - Duration::hours(2);
        let first_run_id = RunId::new();
        state.run_id = RunId::new();
        state.first_execution_run_id = Some(first_run_id);
        state.first_run_started_at = Some(chain_start);
        state.retry_policy = Some(tokeira_types::RetryPolicy {
            initial_interval: Duration::seconds(1),
            backoff_coefficient: 2.0,
            maximum_interval: Some(Duration::seconds(30)),
            maximum_attempts: 3,
            non_retryable_error_types: vec![],
        });

        let successor_run_key = RunKey::new();
        let mut successor_state = sample_state(successor_run_key);
        successor_state.run_key = successor_run_key;
        successor_state.started_at = OffsetDateTime::now_utc();
        successor_state.first_run_started_at = Some(chain_start);
        successor_state.first_execution_run_id = Some(first_run_id);
        successor_state.workflow_execution_timeout = Some(Duration::minutes(30));
        successor_state.workflow_run_timeout = Some(Duration::minutes(5));

        let repo = MockRepo::new(
            LoadedRun::Existing(state.clone()),
            vec![CommitBehavior::Applied],
        );
        let kernel = ContinueAsNewKernel::continued_as_new();
        let publisher = MockPublisher::new()
            .with_submit_result(CommitResult::Applied {
                new_state: successor_state.clone(),
            })
            .await;
        let (first, first_reply) = lane_message(run_key, "continue");
        let (_tx, mut rx) = mpsc::channel(8);
        let activity_tracking =
            crate::activity_timeout::ActivityTrackingState::default();
        let tracking = crate::timeout::WorkflowTimeoutTrackingState::default();
        let nexus_tracking = crate::nexus::NexusTimeoutTrackingState::default();
        let update_registry = crate::UpdateRegistry::new();
        let shard_owner = test_shard_owner();

        let buffered = run_activation(
            &kernel,
            &repo,
            &publisher,
            &shard_owner,
            &activity_tracking,
            &tracking,
            &nexus_tracking,
            &update_registry,
            &mut rx,
            first,
            &LaneConfig::default(),
        )
        .await;

        assert!(buffered.is_empty());
        assert!(matches!(
            first_reply.await.unwrap().unwrap(),
            CommitResult::Applied { .. }
        ));

        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        let snapshot = publisher.snapshot().await;
        assert_eq!(snapshot.submits.len(), 1);
        match &snapshot.submits[0].1 {
            Command::Start(request) => {
                assert_eq!(request.workflow_id, state.workflow_id);
                assert_eq!(request.namespace_id, state.namespace_id);
                assert_eq!(request.continued_execution_run_id, Some(state.run_id));
                assert_eq!(request.first_execution_run_id, Some(first_run_id));
                assert_eq!(request.first_run_started_at, Some(chain_start));
                assert_eq!(request.retry_policy, state.retry_policy);
                assert_eq!(request.attempt, 1);
                assert_eq!(
                    request.workflow_execution_timeout,
                    Some(Duration::minutes(30))
                );
                assert_eq!(request.workflow_run_timeout, Some(Duration::minutes(5)));
            }
            other => panic!("expected successor Start request, got {other:?}"),
        }

        let tracking_snapshot = tracking.snapshot();
        assert_eq!(tracking_snapshot.len(), 1);
        assert_eq!(tracking_snapshot[0].run_key, successor_run_key);
        assert_eq!(tracking_snapshot[0].first_run_started_at, Some(chain_start));
    }

    proptest! {
        #[test]
        fn property_continue_as_new_detection_triggers_only_for_continued_as_new(
            is_continued_as_new in any::<bool>(),
        ) {
            let rt = Runtime::new().unwrap();
            rt.block_on(async move {
                let run_key = RunKey::new();
                let state = sample_state(run_key);
                let repo = MockRepo::new(
                    LoadedRun::Existing(state.clone()),
                    vec![CommitBehavior::Applied],
                );
                let mut kernel = ContinueAsNewKernel::continued_as_new();
                kernel.status = if is_continued_as_new {
                    ExecutionStatus::ContinuedAsNew
                } else {
                    ExecutionStatus::Completed
                };
                kernel.include_continue_event = is_continued_as_new;
                let publisher = MockPublisher::new();
                let (first, first_reply) = lane_message(run_key, "continue");
                let (_tx, mut rx) = mpsc::channel(8);
                let activity_tracking =
                    crate::activity_timeout::ActivityTrackingState::default();
                let tracking = crate::timeout::WorkflowTimeoutTrackingState::default();
                let nexus_tracking = crate::nexus::NexusTimeoutTrackingState::default();
                let update_registry = crate::UpdateRegistry::new();
                let shard_owner = test_shard_owner();

                let _ = run_activation(
                    &kernel,
                    &repo,
                    &publisher,
                    &shard_owner,
                    &activity_tracking,
                    &tracking,
                    &nexus_tracking,
                    &update_registry,
                    &mut rx,
                    first,
                    &LaneConfig::default(),
                ).await;

                let _ = first_reply.await.unwrap().unwrap();
                tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
                let snapshot = publisher.snapshot().await;
                if is_continued_as_new {
                    prop_assert_eq!(snapshot.submits.len(), 1);
                } else {
                    prop_assert!(snapshot.submits.is_empty());
                }
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }

    #[tokio::test]
    async fn run_activation_returns_predecessor_commit_even_when_successor_start_fails() {
        let run_key = RunKey::new();
        let state = sample_state(run_key);
        let repo = MockRepo::new(
            LoadedRun::Existing(state.clone()),
            vec![CommitBehavior::Applied],
        );
        let kernel = ContinueAsNewKernel::continued_as_new();
        let publisher = MockPublisher::new().with_failure().await;
        let (first, first_reply) = lane_message(run_key, "continue");
        let (_tx, mut rx) = mpsc::channel(8);
        let activity_tracking =
            crate::activity_timeout::ActivityTrackingState::default();
        let tracking = crate::timeout::WorkflowTimeoutTrackingState::default();
        let nexus_tracking = crate::nexus::NexusTimeoutTrackingState::default();
        let update_registry = crate::UpdateRegistry::new();
        let shard_owner = test_shard_owner();

        let _ = run_activation(
            &kernel,
            &repo,
            &publisher,
            &shard_owner,
            &activity_tracking,
            &tracking,
            &nexus_tracking,
            &update_registry,
            &mut rx,
            first,
            &LaneConfig::default(),
        )
        .await;

        assert!(matches!(
            first_reply.await.unwrap().unwrap(),
            CommitResult::Applied { .. }
        ));
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        assert!(tracking.snapshot().is_empty());
    }

    #[tokio::test]
    async fn handle_message_returns_kernel_reject_without_retry() {
        let run_key = RunKey::new();
        let state = sample_state(run_key);
        let repo = MockRepo::new(
            LoadedRun::Existing(state.clone()),
            vec![CommitBehavior::Applied],
        );
        let kernel =
            MockKernel::new(sample_dispatch_ops(state.namespace_id)).with_reject();
        let shard_owner = test_shard_owner();

        let error = handle_message(&kernel, &repo, &shard_owner, run_key, sample_command("reject"), 5)
            .await
            .expect_err("reject should surface as error");
        assert!(error.to_string().contains("kernel rejected command"));

        let (load_calls, commit_calls, _) = repo.snapshot().await;
        assert_eq!(load_calls, 1);
        assert_eq!(commit_calls, 0);
    }
}
