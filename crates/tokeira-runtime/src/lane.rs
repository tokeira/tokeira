//! Serial command execution lanes.
//!
//! A lane is the serialization boundary for all state mutations on a run.
//! Every command — signal, timer fire, activity resolution, WFT completion —
//! enters through a lane's bounded channel and is processed one at a time.
//! This eliminates the need for per-run locking: the lane *is* the lock.
//!
//! When a run closes via continue-as-new, the lane is responsible for
//! constructing and submitting the successor `StartRequest`. The successor
//! inherits the predecessor's execution chain metadata (`first_execution_run_id`,
//! `first_run_started_at`) so the server can enforce execution-level timeouts
//! across the entire chain, not just the current run.

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, RwLock},
};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use smallvec::SmallVec;
use time::OffsetDateTime;
use tokeira_kernel::{
    Command, DispatchOp, HistoryEvent, HistoryEventKind, Kernel, LoadedRun, StartRequest,
};
use tokeira_proto::{
    conversions::common::failure_to_payload, public::temporal::api::failure::v1 as failure_proto,
};
use tokeira_storage::{CommitResult, RunRepository, metrics as storage_metrics};
use tokeira_types::{ExecutionStatus, RunKey, ShardEpoch, execution_home_bundle};
use tokio::sync::{mpsc, oneshot};

use crate::{
    UpdateRegistry, UpdateResolution, metrics as runtime_metrics,
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
    /// Whether durable placement-controller takeover fencing is enabled.
    ///
    /// No-controller single-node deployments can pass `ShardEpoch::ZERO` to
    /// storage after local ownership validation. Controller-managed deployments
    /// must pass the real local epoch so storage keeps its durable lease read.
    pub controller_managed_placement: bool,
    /// Maximum loaded runs retained by a lane between activations.
    pub cache_max_entries: usize,
    /// Maximum idle age for a cached run before the lane reloads it.
    pub cache_idle_timeout: std::time::Duration,
}

impl Default for LaneConfig {
    fn default() -> Self {
        Self {
            max_occ_retries: 5,
            max_drain_per_activation: 16,
            controller_managed_placement: false,
            cache_max_entries: 4096,
            cache_idle_timeout: std::time::Duration::from_secs(60),
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
    async fn submit_to_run(&self, run_key: RunKey, command: Command) -> Result<CommitResult>;
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
    pub async fn submit(&self, run_key: RunKey, command: Command) -> Result<CommitResult> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(LaneMessage {
                run_key,
                command,
                reply_tx,
                enqueued_at: std::time::Instant::now(),
            })
            .await?;
        reply_rx.await?
    }

    /// Current number of queued lane messages waiting behind the bounded channel.
    pub fn queued_depth(&self) -> usize {
        self.tx.max_capacity().saturating_sub(self.tx.capacity())
    }
}

struct LaneMessage {
    run_key: RunKey,
    command: Command,
    reply_tx: oneshot::Sender<Result<CommitResult>>,
    enqueued_at: std::time::Instant,
}

struct LaneCache {
    max_entries: usize,
    idle_timeout: std::time::Duration,
    entries: HashMap<RunKey, CachedRun>,
    order: VecDeque<RunKey>,
}

#[derive(Clone)]
struct CachedRun {
    loaded: LoadedRun,
    last_accessed: std::time::Instant,
}

impl LaneCache {
    fn new(config: &LaneConfig) -> Self {
        Self {
            max_entries: config.cache_max_entries,
            idle_timeout: config.cache_idle_timeout,
            entries: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn get(&mut self, run_key: RunKey) -> Option<LoadedRun> {
        let now = std::time::Instant::now();
        let entry = self.entries.get_mut(&run_key)?;
        if now.duration_since(entry.last_accessed) > self.idle_timeout {
            self.entries.remove(&run_key);
            return None;
        }
        entry.last_accessed = now;
        Some(entry.loaded.clone())
    }

    fn insert(&mut self, run_key: RunKey, loaded: LoadedRun) {
        if self.max_entries == 0 {
            return;
        }
        let now = std::time::Instant::now();
        self.order.retain(|key| *key != run_key);
        self.order.push_back(run_key);
        self.entries.insert(
            run_key,
            CachedRun {
                loaded,
                last_accessed: now,
            },
        );
        self.evict_over_capacity();
    }

    fn evict(&mut self, run_key: RunKey) {
        self.entries.remove(&run_key);
        self.order.retain(|key| *key != run_key);
    }

    fn evict_over_capacity(&mut self) {
        while self.entries.len() > self.max_entries {
            let Some(run_key) = self.order.pop_front() else {
                break;
            };
            self.entries.remove(&run_key);
        }
    }
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
    wft_timeout_tracking: crate::wft_timeout::WftTimeoutTrackingState,
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
        let mut cache = LaneCache::new(&config);
        while let Some(message) = rx.recv().await {
            let buffered = run_activation_with_cache(
                &kernel,
                &repo,
                &publisher,
                &shard_owner,
                &activity_tracking,
                &workflow_timeout_tracking,
                &wft_timeout_tracking,
                &nexus_timeout_tracking,
                &update_registry,
                &mut rx,
                message,
                &config,
                &mut cache,
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

#[cfg(test)]
async fn run_activation<K, R, P>(
    kernel: &K,
    repo: &R,
    publisher: &P,
    shard_owner: &Arc<RwLock<ShardOwner>>,
    activity_tracking: &crate::activity_timeout::ActivityTrackingState,
    workflow_timeout_tracking: &crate::timeout::WorkflowTimeoutTrackingState,
    wft_timeout_tracking: &crate::wft_timeout::WftTimeoutTrackingState,
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
    let mut cache = LaneCache::new(config);
    run_activation_with_cache(
        kernel,
        repo,
        publisher,
        shard_owner,
        activity_tracking,
        workflow_timeout_tracking,
        wft_timeout_tracking,
        nexus_timeout_tracking,
        update_registry,
        rx,
        first_message,
        config,
        &mut cache,
    )
    .await
}

async fn run_activation_with_cache<K, R, P>(
    kernel: &K,
    repo: &R,
    publisher: &P,
    shard_owner: &Arc<RwLock<ShardOwner>>,
    activity_tracking: &crate::activity_timeout::ActivityTrackingState,
    workflow_timeout_tracking: &crate::timeout::WorkflowTimeoutTrackingState,
    wft_timeout_tracking: &crate::wft_timeout::WftTimeoutTrackingState,
    nexus_timeout_tracking: &crate::nexus::NexusTimeoutTrackingState,
    update_registry: &UpdateRegistry,
    rx: &mut mpsc::Receiver<LaneMessage>,
    first_message: LaneMessage,
    config: &LaneConfig,
    cache: &mut LaneCache,
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
        let command_type = command_type_name(&message.command);
        runtime_metrics::record_lane_queue_wait(message.enqueued_at.elapsed());
        let processing_start = std::time::Instant::now();
        let result = handle_message_with_cache(
            kernel,
            repo,
            shard_owner,
            message.run_key,
            message.command,
            config,
            config.max_occ_retries,
            cache,
        )
        .await;

        let stop_draining = result.is_err();
        let reply = match result {
            Ok((commit_result, mut dispatch_ops, history_events)) => {
                let mut reset_materialization_error = None;
                if let CommitResult::Applied { new_state } = &commit_result {
                    for event in &history_events {
                        match &event.kind {
                            HistoryEventKind::ActivityTaskCancelRequested {
                                activity_id, ..
                            } => activity_tracking
                                .mark_cancel_requested(message.run_key, activity_id),
                            HistoryEventKind::ActivityTaskCompleted { activity_id, .. }
                            | HistoryEventKind::ActivityTaskFailed { activity_id, .. }
                            | HistoryEventKind::ActivityTaskTimedOut { activity_id, .. }
                            | HistoryEventKind::ActivityTaskCanceled { activity_id, .. } => {
                                activity_tracking.remove(message.run_key, activity_id)
                            }
                            HistoryEventKind::WorkflowExecutionUpdateCompleted {
                                update_id,
                                result,
                                ..
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
                                ..
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
                        wft_timeout_tracking.remove(message.run_key);
                        nexus_timeout_tracking.remove_all_for_run(message.run_key);
                        update_registry.drain_for_run(message.run_key);
                    }
                    if new_state.closed_at.is_some()
                        && let Some((successor_run_id, fork_event_id)) =
                            extract_reset_metadata(&history_events)
                    {
                        let successor_run_key = RunKey::derive(
                            new_state.namespace_id,
                            &new_state.workflow_id,
                            successor_run_id,
                        );
                        if let Err(error) = repo
                            .materialize_reset_successor(
                                message.run_key,
                                fork_event_id,
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
                                        workflow_run_timeout: successor_state.workflow_run_timeout,
                                        started_at: successor_state.started_at,
                                        first_run_started_at: successor_state.first_run_started_at,
                                        has_retry_policy: successor_state.retry_policy.is_some(),
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
                if reserved_start_has_direct_delivery(&committed_command) {
                    dispatch_ops.retain(|op| !matches!(op, DispatchOp::EnqueueWorkflowTask { .. }));
                }
                if !dispatch_ops.is_empty()
                    && let Err(error) = publisher.publish(message.run_key, &dispatch_ops).await
                {
                    tracing::warn!(?error, run_key = ?message.run_key, "failed to publish dispatch ops");
                }
                if let Some(error) = reset_materialization_error {
                    Err(error)
                } else {
                    if let CommitResult::Applied { new_state } = &commit_result {
                        if let Command::NexusOperationResolved(request) = &committed_command
                            && matches!(
                                request.resolution,
                                tokeira_kernel::NexusResolution::Completed { .. }
                                    | tokeira_kernel::NexusResolution::Failed { .. }
                                    | tokeira_kernel::NexusResolution::Canceled
                                    | tokeira_kernel::NexusResolution::TimedOut
                            )
                        {
                            nexus_timeout_tracking.remove(message.run_key, &request.operation_id);
                        }
                        if new_state.closed_at.is_some() {
                            if let Some(parent_run_key) = new_state.parent_run_key
                                && extract_reset_metadata(&history_events).is_none()
                            {
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
                                                    failure_to_payload(&failure_proto::Failure {
                                                        message: "child workflow failed"
                                                            .to_string(),
                                                        ..Default::default()
                                                    })
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
                                        if let Err(error) =
                                            publisher.submit_to_run(parent_run_key, command).await
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
                            if new_state.status == ExecutionStatus::ContinuedAsNew {
                                let successor_event =
                                    history_events.iter().find_map(|event| match &event.kind {
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
                                            retry_policy,
                                            ..
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
                                            retry_policy.clone(),
                                        )),
                                        _ => None,
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
                                    retry_policy,
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
                                    let successor_run_key = RunKey::derive(
                                        new_state.namespace_id,
                                        &new_state.workflow_id,
                                        successor_run_id,
                                    );
                                    let start_request = StartRequest {
                                        run_key: successor_run_key,
                                        namespace_id: new_state.namespace_id,
                                        workflow_id: new_state.workflow_id.clone(),
                                        run_id: successor_run_id,
                                        workflow_type,
                                        // Empty task_queue means "reuse the predecessor's queue"
                                        task_queue: if task_queue.0.is_empty() {
                                            new_state.task_queue.clone()
                                        } else {
                                            task_queue
                                        },
                                        deployment: new_state.deployment.clone(),
                                        build_id: new_state.build_id.clone(),
                                        input,
                                        header: None,
                                        memo,
                                        search_attributes,
                                        workflow_execution_timeout,
                                        workflow_run_timeout,
                                        workflow_task_timeout,
                                        retry_policy,
                                        conflict_policy:
                                            tokeira_kernel::WorkflowIdConflictPolicy::Fail,
                                        reuse_policy:
                                            tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
                                        attempt: 1,
                                        continued_execution_run_id: Some(new_state.run_id),
                                        first_execution_run_id,
                                        parent_run_key: None,
                                        parent_workflow_id: None,
                                        parent_run_id: None,
                                        parent_namespace_id: None,
                                        parent_initiated_event_id: 0,
                                        original_execution_run_id: Some(
                                            new_state
                                                .original_execution_run_id
                                                .unwrap_or(new_state.run_id),
                                        ),
                                        continued_failure: new_state.close_failure.clone(),
                                        last_completion_result: new_state.close_result.clone(),
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
                                        cron_schedule: None,
                                        reserved_poller_identity: None,
                                    };
                                    let publisher = publisher.clone();
                                    let workflow_timeout_tracking =
                                        workflow_timeout_tracking.clone();
                                    let predecessor_run_key = message.run_key;
                                    let shard_count = shard_owner.read().unwrap().shard_count();
                                    tokio::spawn(async move {
                                        match publisher
                                            .submit_to_run(
                                                successor_run_key,
                                                Command::Start(start_request),
                                            )
                                            .await
                                        {
                                            Ok(CommitResult::Applied { new_state }) => {
                                                if new_state.workflow_execution_timeout.is_some()
                                                    || new_state.workflow_run_timeout.is_some()
                                                {
                                                    workflow_timeout_tracking.insert(
                                                        crate::timeout::WorkflowTimeoutEntry {
                                                            run_key: new_state.run_key,
                                                            shard_id: crate::shard::shard_for(
                                                                new_state.run_key,
                                                                shard_count,
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
        runtime_metrics::record_lane_processing_duration(command_type, processing_start.elapsed());
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

#[cfg(test)]
async fn handle_message<K, R>(
    kernel: &K,
    repo: &R,
    shard_owner: &Arc<RwLock<ShardOwner>>,
    run_key: RunKey,
    command: Command,
    config: &LaneConfig,
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
    let mut cache = LaneCache::new(config);
    handle_message_with_cache(
        kernel,
        repo,
        shard_owner,
        run_key,
        command,
        config,
        max_retries,
        &mut cache,
    )
    .await
}

async fn handle_message_with_cache<K, R>(
    kernel: &K,
    repo: &R,
    shard_owner: &Arc<RwLock<ShardOwner>>,
    run_key: RunKey,
    command: Command,
    config: &LaneConfig,
    max_retries: u32,
    cache: &mut LaneCache,
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
        let transition_span = tracing::info_span!(
            "kernel.transition",
            command_type = command_type_name(&command),
            run_key = %run_key.0,
            transition_seq = tracing::field::Empty,
        );
        let loaded = match cache.get(run_key) {
            Some(loaded) => loaded,
            None => {
                let loaded = repo.load_run(run_key).await?;
                cache.insert(run_key, loaded.clone());
                loaded
            }
        };
        let transition = transition_span.in_scope(|| {
            kernel
                .apply(loaded, command.clone())
                .map_err(|reject| anyhow!("kernel rejected command: {reject}"))
        })?;
        let (execution_home_bundle, epoch) = {
            let owner = shard_owner.read().unwrap();
            let bundle_id = execution_home_bundle(
                transition.next_state.namespace_id.0.as_bytes(),
                transition.next_state.workflow_id.0.as_bytes(),
                owner.shard_count(),
            );
            let Some(local_epoch) = owner.epoch_of(bundle_id) else {
                return Ok((
                    CommitResult::Conflict {
                        reason: format!("not owner of execution-home shard {bundle_id:?}"),
                    },
                    SmallVec::new(),
                    SmallVec::new(),
                ));
            };
            let commit_epoch = if config.controller_managed_placement {
                local_epoch
            } else {
                ShardEpoch::ZERO
            };
            (bundle_id, commit_epoch)
        };
        transition_span.record(
            "transition_seq",
            transition.next_state.transition_seq.0 as i64,
        );
        let dispatch_ops = transition.dispatch_ops.clone();
        let history_events = transition.history_events.clone();

        match repo
            .commit_transition_for_bundle(run_key, execution_home_bundle, transition, epoch)
            .await?
        {
            CommitResult::Applied { new_state } => {
                cache.insert(run_key, LoadedRun::Existing(new_state.clone()));
                if attempts > 0 {
                    storage_metrics::record_dsql_retry("commit_transition_for_bundle", "success");
                }
                runtime_metrics::record_transition_committed(
                    &new_state.namespace_id.0.to_string(),
                    command_type_name(&command),
                );
                runtime_metrics::record_commands_processed(command_type_name(&command));
                for event in &history_events {
                    runtime_metrics::record_events_emitted(history_event_type_name(event), 1);
                }
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
                cache.evict(run_key);
                runtime_metrics::record_occ_retry("retry");
                if attempts >= max_retries {
                    runtime_metrics::record_occ_retry("exhausted");
                    storage_metrics::record_dsql_retry("commit_transition_for_bundle", "exhausted");
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

fn command_type_name(command: &Command) -> &'static str {
    match command {
        Command::Start(_) => "Start",
        Command::SignalWithStart(_) => "SignalWithStart",
        Command::Update(_) => "Update",
        Command::Signal(_) => "Signal",
        Command::Cancel(_) => "Cancel",
        Command::Terminate(_) => "Terminate",
        Command::Reset(_) => "Reset",
        Command::PauseWorkflow(_) => "PauseWorkflow",
        Command::UnpauseWorkflow(_) => "UnpauseWorkflow",
        Command::UpdateActivityOptions(_) => "UpdateActivityOptions",
        Command::PauseActivity(_) => "PauseActivity",
        Command::UnpauseActivity(_) => "UnpauseActivity",
        Command::ResetActivity(_) => "ResetActivity",
        Command::UpdateExecutionOptions(_) => "UpdateExecutionOptions",
        Command::WorkflowExecutionTimedOut(_) => "WorkflowExecutionTimedOut",
        Command::WorkflowTaskStarted(_) => "WorkflowTaskStarted",
        Command::WorkflowTaskCompleted(_) => "WorkflowTaskCompleted",
        Command::WorkflowTaskFailed(_) => "WorkflowTaskFailed",
        Command::WorkflowTaskTimedOut(_) => "WorkflowTaskTimedOut",
        Command::ActivityResolved(_) => "ActivityResolved",
        Command::ChildStartConfirmed(_) => "ChildStartConfirmed",
        Command::ChildResolved(_) => "ChildResolved",
        Command::ExternalSignalResolved(_) => "ExternalSignalResolved",
        Command::ExternalCancelResolved(_) => "ExternalCancelResolved",
        Command::NexusOperationResolved(_) => "NexusOperationResolved",
        Command::TimerDue(_) => "TimerDue",
        Command::ScheduleQueryTask(_) => "ScheduleQueryTask",
    }
}

fn reserved_start_has_direct_delivery(command: &Command) -> bool {
    matches!(
        command,
        Command::Start(StartRequest {
            reserved_poller_identity: Some(_),
            ..
        })
    )
}

fn history_event_type_name(event: &HistoryEvent) -> &'static str {
    match &event.kind {
        HistoryEventKind::WorkflowExecutionStarted { .. } => "WorkflowExecutionStarted",
        HistoryEventKind::WorkflowExecutionSignaled { .. } => "WorkflowExecutionSignaled",
        HistoryEventKind::WorkflowExecutionCancelRequested { .. } => {
            "WorkflowExecutionCancelRequested"
        }
        HistoryEventKind::WorkflowExecutionPaused { .. } => "WorkflowExecutionPaused",
        HistoryEventKind::WorkflowExecutionUnpaused { .. } => "WorkflowExecutionUnpaused",
        HistoryEventKind::WorkflowExecutionTerminated { .. } => "WorkflowExecutionTerminated",
        HistoryEventKind::WorkflowExecutionTimedOut { .. } => "WorkflowExecutionTimedOut",
        HistoryEventKind::WorkflowTaskScheduled { .. } => "WorkflowTaskScheduled",
        HistoryEventKind::WorkflowTaskStarted { .. } => "WorkflowTaskStarted",
        HistoryEventKind::WorkflowTaskCompleted { .. } => "WorkflowTaskCompleted",
        HistoryEventKind::WorkflowTaskFailed { .. } => "WorkflowTaskFailed",
        HistoryEventKind::WorkflowTaskTimedOut { .. } => "WorkflowTaskTimedOut",
        HistoryEventKind::ActivityTaskScheduled { .. } => "ActivityTaskScheduled",
        HistoryEventKind::ActivityTaskStarted { .. } => "ActivityTaskStarted",
        HistoryEventKind::ActivityTaskCompleted { .. } => "ActivityTaskCompleted",
        HistoryEventKind::ActivityTaskFailed { .. } => "ActivityTaskFailed",
        HistoryEventKind::ActivityTaskTimedOut { .. } => "ActivityTaskTimedOut",
        HistoryEventKind::ActivityTaskCanceled { .. } => "ActivityTaskCanceled",
        HistoryEventKind::TimerStarted { .. } => "TimerStarted",
        HistoryEventKind::MarkerRecorded { .. } => "MarkerRecorded",
        HistoryEventKind::TimerCanceled { .. } => "TimerCanceled",
        HistoryEventKind::TimerFired { .. } => "TimerFired",
        HistoryEventKind::ActivityTaskCancelRequested { .. } => "ActivityTaskCancelRequested",
        HistoryEventKind::StartChildWorkflowExecutionInitiated { .. } => {
            "StartChildWorkflowExecutionInitiated"
        }
        HistoryEventKind::ChildWorkflowExecutionStarted { .. } => "ChildWorkflowExecutionStarted",
        HistoryEventKind::StartChildWorkflowExecutionFailed { .. } => {
            "StartChildWorkflowExecutionFailed"
        }
        HistoryEventKind::ChildWorkflowExecutionCompleted { .. } => {
            "ChildWorkflowExecutionCompleted"
        }
        HistoryEventKind::ChildWorkflowExecutionFailed { .. } => "ChildWorkflowExecutionFailed",
        HistoryEventKind::ChildWorkflowExecutionCanceled { .. } => "ChildWorkflowExecutionCanceled",
        HistoryEventKind::ChildWorkflowExecutionTerminated { .. } => {
            "ChildWorkflowExecutionTerminated"
        }
        HistoryEventKind::ChildWorkflowExecutionTimedOut { .. } => "ChildWorkflowExecutionTimedOut",
        HistoryEventKind::SignalExternalWorkflowExecutionInitiated { .. } => {
            "SignalExternalWorkflowExecutionInitiated"
        }
        HistoryEventKind::ExternalWorkflowExecutionSignaled { .. } => {
            "ExternalWorkflowExecutionSignaled"
        }
        HistoryEventKind::SignalExternalWorkflowExecutionFailed { .. } => {
            "SignalExternalWorkflowExecutionFailed"
        }
        HistoryEventKind::RequestCancelExternalWorkflowExecutionInitiated { .. } => {
            "RequestCancelExternalWorkflowExecutionInitiated"
        }
        HistoryEventKind::ExternalWorkflowExecutionCancelRequested { .. } => {
            "ExternalWorkflowExecutionCancelRequested"
        }
        HistoryEventKind::RequestCancelExternalWorkflowExecutionFailed { .. } => {
            "RequestCancelExternalWorkflowExecutionFailed"
        }
        HistoryEventKind::NexusOperationScheduled { .. } => "NexusOperationScheduled",
        HistoryEventKind::NexusOperationStarted { .. } => "NexusOperationStarted",
        HistoryEventKind::NexusOperationCompleted { .. } => "NexusOperationCompleted",
        HistoryEventKind::NexusOperationFailed { .. } => "NexusOperationFailed",
        HistoryEventKind::NexusOperationCanceled { .. } => "NexusOperationCanceled",
        HistoryEventKind::NexusOperationTimedOut { .. } => "NexusOperationTimedOut",
        HistoryEventKind::NexusOperationCancelRequested { .. } => "NexusOperationCancelRequested",
        HistoryEventKind::WorkflowExecutionUpdateAccepted { .. } => {
            "WorkflowExecutionUpdateAccepted"
        }
        HistoryEventKind::WorkflowExecutionUpdateCompleted { .. } => {
            "WorkflowExecutionUpdateCompleted"
        }
        HistoryEventKind::WorkflowExecutionUpdateRejected { .. } => {
            "WorkflowExecutionUpdateRejected"
        }
        HistoryEventKind::WorkflowExecutionOptionsUpdated { .. } => {
            "WorkflowExecutionOptionsUpdated"
        }
        HistoryEventKind::WorkflowExecutionCompleted { .. } => "WorkflowExecutionCompleted",
        HistoryEventKind::WorkflowExecutionFailed { .. } => "WorkflowExecutionFailed",
        HistoryEventKind::WorkflowExecutionContinuedAsNew { .. } => {
            "WorkflowExecutionContinuedAsNew"
        }
        HistoryEventKind::WorkflowExecutionCanceled { .. } => "WorkflowExecutionCanceled",
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
        time::{Duration as StdDuration, Instant},
    };

    use proptest::prelude::*;
    use smallvec::smallvec;
    use time::{Duration, OffsetDateTime};
    use tokeira_kernel::{
        ActivityState, HistoryEvent, LoadedRun, PendingWorkflowTask, ProjectionOp, Reject,
        RequestDedupeOp, TimerOp, Transition, WorkflowState,
    };
    use tokeira_storage::{
        BacklogEntry, CommitResult, DispatchableActivityTask, DispatchableWorkflowTask, DueTimer,
        LeaseOutcome, LeaseRepository, ProjectionBatch, ProjectionLog, ProjectionRecord,
        RequestRecord, TransitionAuditRecord,
    };
    use tokeira_types::{
        ExecutionRef, ExecutionStatus, LogicalTaskSeq, Memo, NamespaceId, Payloads,
        ProjectionCursor, QueueKey, RequestContext, RequestId, RunId, RunKey, SearchAttributes,
        ShardEpoch, ShardId, TaskKind, TaskQueueName, TransitionSeq as DurableTransitionSeq,
        WorkerIdentity, WorkflowId, WorkflowType,
    };
    use tokio::{
        runtime::Runtime,
        sync::{Mutex as AsyncMutex, Notify},
    };
    use tracing_subscriber::layer::SubscriberExt;

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
        fn apply(&self, loaded: LoadedRun, command: Command) -> Result<Transition, Reject> {
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
                        header: None,
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
        async fn resolve_execution(&self, _execution: &ExecutionRef) -> Result<Option<RunKey>> {
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

        // Test mock: ShardEpoch is intentionally ignored. The lane tests
        // validate OCC retry logic and dispatch behaviour, not epoch fencing.
        // Production code routes through commit_transition_for_bundle which
        // carries the real epoch from the ShardOwner.
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

        async fn commit_transition_for_bundle(
            &self,
            run_key: RunKey,
            _execution_home_bundle: ShardId,
            transition: Transition,
            epoch: ShardEpoch,
        ) -> Result<CommitResult> {
            self.commit_transition(run_key, transition, epoch).await
        }

        async fn materialize_reset_successor(
            &self,
            _base_run_key: RunKey,
            _fork_event_id: i64,
            successor_run_id: RunId,
        ) -> Result<()> {
            let mut state = self.state.lock().await;
            let successor_run_key = RunKey::derive(
                NamespaceId(uuid::Uuid::nil()),
                &WorkflowId("test".to_owned()),
                successor_run_id,
            );
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

        async fn list_started_workflow_tasks_for_shard(
            &self,
            _shard_id: ShardId,
            _limit: usize,
        ) -> Result<Vec<tokeira_storage::WftTimeoutSweepEntry>> {
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
            _node_endpoint: String,
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
            _node_endpoint: String,
        ) -> Result<LeaseOutcome> {
            Ok(LeaseOutcome::Renewed {
                epoch: ShardEpoch::ZERO,
            })
        }

        async fn list_bundle_leases(&self) -> Result<Vec<tokeira_storage::BundleLease>> {
            Ok(Vec::new())
        }

        async fn relinquish_bundle(
            &self,
            _bundle: ShardId,
            _owner: String,
            _epoch: ShardEpoch,
        ) -> Result<LeaseOutcome> {
            Ok(LeaseOutcome::Acquired {
                epoch: ShardEpoch::ZERO,
            })
        }
    }

    #[derive(Clone)]
    struct MockPublisher {
        state: Arc<AsyncMutex<MockPublisherState>>,
        wake: Arc<Notify>,
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
                wake: Arc::new(Notify::new()),
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

        async fn wait_for_submits(&self, expected: usize) {
            loop {
                if self.state.lock().await.submits.len() >= expected {
                    return;
                }
                self.wake.notified().await;
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
            drop(state);
            self.wake.notify_waiters();
            let state = self.state.lock().await;
            if state.fail {
                return Err(anyhow!("publisher failed"));
            }
            Ok(())
        }

        async fn submit_to_run(&self, run_key: RunKey, command: Command) -> Result<CommitResult> {
            let mut state = self.state.lock().await;
            state.submits.push((run_key, command));
            drop(state);
            self.wake.notify_waiters();
            let state = self.state.lock().await;
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
        retry_policy: Option<tokeira_types::RetryPolicy>,
        first_run_started_at: Option<OffsetDateTime>,
    }

    impl ContinueAsNewKernel {
        fn continued_as_new() -> Self {
            Self {
                status: ExecutionStatus::ContinuedAsNew,
                include_continue_event: true,
                workflow_execution_timeout: Some(Duration::minutes(30)),
                workflow_run_timeout: Some(Duration::minutes(5)),
                retry_policy: None,
                first_run_started_at: None,
            }
        }
    }

    impl Kernel for ContinueAsNewKernel {
        fn apply(&self, loaded: LoadedRun, _command: Command) -> Result<Transition, Reject> {
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
                        retry_policy: self.retry_policy.clone(),
                        initiator: tokeira_kernel::ContinueAsNewInitiator::Workflow,
                        failure: None,
                        last_completion_result: None,
                        workflow_task_completed_event_id: 0,
                    },
                }]
            } else {
                smallvec![HistoryEvent {
                    event_id: current.last_event_id + 1,
                    happened_at: OffsetDateTime::now_utc(),
                    kind: tokeira_kernel::HistoryEventKind::WorkflowExecutionCompleted {
                        result: Payloads::default(),
                        workflow_task_completed_event_id: 0,
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

    #[derive(Clone)]
    struct CapturingKernel {
        observed: Arc<Mutex<Vec<String>>>,
    }

    impl Kernel for CapturingKernel {
        fn apply(&self, loaded: LoadedRun, _command: Command) -> Result<Transition, Reject> {
            if let Some(metadata) = tracing::Span::current().metadata() {
                self.observed
                    .lock()
                    .unwrap()
                    .push(metadata.name().to_string());
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
                history_events: SmallVec::new(),
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
                scheduled_at: OffsetDateTime::now_utc(),
                started_event_id: None,
                started_at: None,
                attempt: 1,
            }),
            previous_started_event_id: 0,
            workflow_task_attempt: 1,
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
            original_execution_run_id: None,
            parent_run_key: None,
            parent_workflow_id: None,
            parent_run_id: None,
            parent_namespace_id: None,
            parent_initiated_event_id: 0,
            last_completion_result: None,
            activities: BTreeMap::<String, ActivityState>::new(),
            timers: BTreeMap::new(),
            children: BTreeMap::new(),
            pending_external_signals: BTreeMap::new(),
            pending_external_cancels: BTreeMap::new(),
            pending_updates: BTreeMap::new(),
            admitted_updates: std::collections::HashSet::new(),
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
                enqueued_at: std::time::Instant::now(),
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
        assert!(!config.controller_managed_placement);
        assert_eq!(config.cache_max_entries, 4096);
    }

    #[test]
    fn lane_config_edge_values_are_representable() {
        let config = LaneConfig {
            max_occ_retries: 0,
            max_drain_per_activation: 1,
            controller_managed_placement: true,
            cache_max_entries: 0,
            cache_idle_timeout: std::time::Duration::ZERO,
        };
        assert_eq!(config.max_occ_retries, 0);
        assert_eq!(config.max_drain_per_activation, 1);
        assert!(config.controller_managed_placement);
    }

    #[tokio::test]
    async fn lane_cache_reuses_loaded_state_between_commands() {
        let run_key = RunKey::new();
        let state = sample_state(run_key);
        let repo = MockRepo::new(
            LoadedRun::Existing(state.clone()),
            vec![CommitBehavior::Applied, CommitBehavior::Applied],
        );
        let kernel = MockKernel::new(SmallVec::new());
        let shard_owner = test_shard_owner();
        let config = LaneConfig::default();
        let mut cache = LaneCache::new(&config);

        let first = handle_message_with_cache(
            &kernel,
            &repo,
            &shard_owner,
            run_key,
            sample_command("first"),
            &config,
            config.max_occ_retries,
            &mut cache,
        )
        .await
        .unwrap();
        assert!(matches!(first.0, CommitResult::Applied { .. }));

        let second = handle_message_with_cache(
            &kernel,
            &repo,
            &shard_owner,
            run_key,
            sample_command("second"),
            &config,
            config.max_occ_retries,
            &mut cache,
        )
        .await
        .unwrap();
        assert!(matches!(second.0, CommitResult::Applied { .. }));

        let (load_calls, commit_calls, _) = repo.snapshot().await;
        assert_eq!(load_calls, 1);
        assert_eq!(commit_calls, 2);
    }

    #[tokio::test]
    async fn lane_cache_evicts_on_occ_conflict_before_retry() {
        let run_key = RunKey::new();
        let state = sample_state(run_key);
        let repo = MockRepo::new(
            LoadedRun::Existing(state.clone()),
            vec![CommitBehavior::Conflict, CommitBehavior::Applied],
        );
        let kernel = MockKernel::new(SmallVec::new());
        let shard_owner = test_shard_owner();
        let config = LaneConfig::default();
        let mut cache = LaneCache::new(&config);

        let result = handle_message_with_cache(
            &kernel,
            &repo,
            &shard_owner,
            run_key,
            sample_command("conflict"),
            &config,
            config.max_occ_retries,
            &mut cache,
        )
        .await
        .unwrap();

        assert!(matches!(result.0, CommitResult::Applied { .. }));
        let (load_calls, commit_calls, _) = repo.snapshot().await;
        assert_eq!(load_calls, 2);
        assert_eq!(commit_calls, 2);
    }

    #[test]
    fn lane_cache_idle_timeout_evicts_stale_entries_without_sleeping() {
        let run_key = RunKey::new();
        let config = LaneConfig {
            cache_idle_timeout: StdDuration::from_secs(1),
            ..LaneConfig::default()
        };
        let mut cache = LaneCache::new(&config);
        cache.insert(run_key, LoadedRun::Existing(sample_state(run_key)));
        cache.entries.get_mut(&run_key).unwrap().last_accessed = Instant::now()
            .checked_sub(StdDuration::from_secs(2))
            .unwrap();

        assert!(cache.get(run_key).is_none());
        assert!(!cache.entries.contains_key(&run_key));
    }

    proptest! {
        #[test]
        fn lane_cache_never_exceeds_configured_capacity(max_entries in 1usize..32, count in 1usize..128) {
            let config = LaneConfig {
                cache_max_entries: max_entries,
                ..LaneConfig::default()
            };
            let mut cache = LaneCache::new(&config);

            for index in 0..count {
                let run_key = RunKey(uuid::Uuid::from_u128(index as u128));
                cache.insert(run_key, LoadedRun::Existing(sample_state(run_key)));
                prop_assert!(cache.entries.len() <= max_entries);
            }
        }
    }

    #[test]
    fn queued_depth_reflects_bounded_channel_occupancy() {
        let (tx, _rx) = mpsc::channel(4);
        let handle = LaneHandle { tx };
        assert_eq!(handle.queued_depth(), 0);

        let (reply_tx, _reply_rx) = oneshot::channel();
        handle
            .tx
            .try_send(LaneMessage {
                run_key: RunKey::new(),
                command: sample_command("queued"),
                reply_tx,
                enqueued_at: std::time::Instant::now(),
            })
            .unwrap();

        assert_eq!(handle.queued_depth(), 1);
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

                let (result, _, _) = handle_message(&kernel, &repo, &shard_owner, run_key, sample_command("a"), &LaneConfig::default(), 8).await.unwrap();
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

                let _ = handle_message(&kernel, &repo, &shard_owner, run_key, command.clone(), &LaneConfig::default(), 8).await.unwrap();
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

                let error = handle_message(&kernel, &repo, &shard_owner, run_key, sample_command("bound"), &LaneConfig::default(), max_retries)
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
                    &LaneConfig::default(),
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
        let activity_tracking = crate::activity_timeout::ActivityTrackingState::default();
        let tracking = crate::timeout::WorkflowTimeoutTrackingState::default();
        let wft_tracking = crate::wft_timeout::WftTimeoutTrackingState::default();
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
            &wft_tracking,
            &nexus_tracking,
            &update_registry,
            &mut rx,
            first,
            &LaneConfig {
                max_occ_retries: 5,
                max_drain_per_activation: 4,
                ..LaneConfig::default()
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
        let activity_tracking = crate::activity_timeout::ActivityTrackingState::default();
        let tracking = crate::timeout::WorkflowTimeoutTrackingState::default();
        let wft_tracking = crate::wft_timeout::WftTimeoutTrackingState::default();
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
            &wft_tracking,
            &nexus_tracking,
            &update_registry,
            &mut rx,
            first,
            &LaneConfig {
                max_occ_retries: 5,
                max_drain_per_activation: 2,
                ..LaneConfig::default()
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
        let activity_tracking = crate::activity_timeout::ActivityTrackingState::default();
        let tracking = crate::timeout::WorkflowTimeoutTrackingState::default();
        let wft_tracking = crate::wft_timeout::WftTimeoutTrackingState::default();
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
            &wft_tracking,
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
        let activity_tracking = crate::activity_timeout::ActivityTrackingState::default();
        let tracking = crate::timeout::WorkflowTimeoutTrackingState::default();
        let wft_tracking = crate::wft_timeout::WftTimeoutTrackingState::default();
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
            &wft_tracking,
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
        let activity_tracking = crate::activity_timeout::ActivityTrackingState::default();
        let tracking = crate::timeout::WorkflowTimeoutTrackingState::default();
        let wft_tracking = crate::wft_timeout::WftTimeoutTrackingState::default();
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
            &wft_tracking,
            &nexus_tracking,
            &update_registry,
            &mut rx,
            first,
            &LaneConfig {
                max_occ_retries: 0,
                max_drain_per_activation: 16,
                ..LaneConfig::default()
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
        let activity_tracking = crate::activity_timeout::ActivityTrackingState::default();
        let tracking = crate::timeout::WorkflowTimeoutTrackingState::default();
        let wft_tracking = crate::wft_timeout::WftTimeoutTrackingState::default();
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
            &wft_tracking,
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
        let activity_tracking = crate::activity_timeout::ActivityTrackingState::default();
        let tracking = crate::timeout::WorkflowTimeoutTrackingState::default();
        let wft_tracking = crate::wft_timeout::WftTimeoutTrackingState::default();
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
            &wft_tracking,
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
        let successor_retry_policy = Some(tokeira_types::RetryPolicy {
            initial_interval: Duration::seconds(5),
            backoff_coefficient: 1.5,
            maximum_interval: Some(Duration::seconds(60)),
            maximum_attempts: 9,
            non_retryable_error_types: vec!["fatal".to_string()],
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
        let mut kernel = ContinueAsNewKernel::continued_as_new();
        kernel.retry_policy = successor_retry_policy.clone();
        let publisher = MockPublisher::new()
            .with_submit_result(CommitResult::Applied {
                new_state: successor_state.clone(),
            })
            .await;
        let (first, first_reply) = lane_message(run_key, "continue");
        let (_tx, mut rx) = mpsc::channel(8);
        let activity_tracking = crate::activity_timeout::ActivityTrackingState::default();
        let tracking = crate::timeout::WorkflowTimeoutTrackingState::default();
        let wft_tracking = crate::wft_timeout::WftTimeoutTrackingState::default();
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
            &wft_tracking,
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
                assert_eq!(request.retry_policy, successor_retry_policy);
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
                let wft_tracking = crate::wft_timeout::WftTimeoutTrackingState::default();
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
                    &wft_tracking,
                    &nexus_tracking,
                    &update_registry,
                    &mut rx,
                    first,
                    &LaneConfig::default(),
                ).await;

                let _ = first_reply.await.unwrap().unwrap();
                if is_continued_as_new {
                    publisher.wait_for_submits(1).await;
                }
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
        let activity_tracking = crate::activity_timeout::ActivityTrackingState::default();
        let tracking = crate::timeout::WorkflowTimeoutTrackingState::default();
        let wft_tracking = crate::wft_timeout::WftTimeoutTrackingState::default();
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
            &wft_tracking,
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
        let kernel = MockKernel::new(sample_dispatch_ops(state.namespace_id)).with_reject();
        let shard_owner = test_shard_owner();

        let error = handle_message(
            &kernel,
            &repo,
            &shard_owner,
            run_key,
            sample_command("reject"),
            &LaneConfig::default(),
            5,
        )
        .await
        .expect_err("reject should surface as error");
        assert!(error.to_string().contains("kernel rejected command"));

        let (load_calls, commit_calls, _) = repo.snapshot().await;
        assert_eq!(load_calls, 1);
        assert_eq!(commit_calls, 0);
    }

    #[tokio::test]
    async fn handle_message_uses_kernel_transition_span_name() {
        let run_key = RunKey::new();
        let state = sample_state(run_key);
        let repo = MockRepo::new(LoadedRun::Existing(state), vec![CommitBehavior::Applied]);
        let observed = Arc::new(Mutex::new(Vec::new()));
        let kernel = CapturingKernel {
            observed: observed.clone(),
        };
        let shard_owner = test_shard_owner();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().with_test_writer());
        let dispatch = tracing::Dispatch::new(subscriber);
        let _guard = tracing::dispatcher::set_default(&dispatch);

        let _ = handle_message(
            &kernel,
            &repo,
            &shard_owner,
            run_key,
            sample_command("span"),
            &LaneConfig::default(),
            0,
        )
        .await
        .unwrap();

        let observed = observed.lock().unwrap();
        assert!(observed.iter().any(|name| name == "kernel.transition"));
    }
}
