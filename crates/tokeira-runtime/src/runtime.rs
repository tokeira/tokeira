use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use smallvec::{SmallVec, smallvec};
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    ActivityOp, ActivityResolution, ActivityResolvedRequest, BasicKernel, Command,
    DispatchOp, LoadedRun, SignalRequest, StartRequest, StartWorkflowTaskRequest,
    Transition, WorkflowTaskCompletedRequest,
};
use tokeira_storage::{
    CommitResult, DispatchableActivityTask, DispatchableWorkflowTask, RunRepository,
};
use tokeira_types::{
    ActivityTaskToken, ExecutionRef, Payloads, QueueKey, RetryPolicy, RunKey, ShardEpoch,
    TaskQueueName, WorkerIdentity, WorkflowTaskToken,
};

use crate::{
    broker::{InMemoryActivityBroker, InMemoryBroker},
    lane::{DispatchPublisher, LaneConfig, LaneHandle, spawn_lane},
};

/// Public runtime facade.
///
/// This is intentionally small. The point is to expose the
/// core server actions without dragging transport or
/// authentication into the same crate.
///
/// See [`docs/crates/runtime.md`] for the full
/// orchestration flow and module map.
pub struct TokeiraRuntime<R> {
    /// Shared handle to the durable run repository.
    repo: Arc<R>,
    /// In-memory workflow-task broker.
    broker: InMemoryBroker,
    /// In-memory activity-task broker.
    activity_broker: InMemoryActivityBroker,
    /// Lane executor handles (one per lane).
    lanes: Vec<LaneHandle>,
    /// Lane configuration shared across all lanes.
    config: LaneConfig,
}

/// Outcome of evaluating an activity retry policy.
#[derive(Clone, Debug, PartialEq)]
pub enum RetryDecision {
    /// The activity should be retried at `next_attempt`.
    Retry { next_attempt: u32 },
    /// All retry attempts have been exhausted (or the
    /// error is non-retryable).
    Exhausted,
}

impl<R> TokeiraRuntime<R>
where
    R: RunRepository + 'static,
{
    /// Create a new runtime with `lane_count` parallel
    /// lane executors backed by `repo`.
    pub fn new(repo: Arc<R>, lane_count: usize, config: LaneConfig) -> Self {
        let broker = InMemoryBroker::default();
        let activity_broker = InMemoryActivityBroker::default();
        let lanes = (0..lane_count.max(1))
            .map(|_| {
                let publisher = RuntimeDispatchPublisher::new(
                    broker.clone(),
                    activity_broker.clone(),
                );
                spawn_lane(
                    BasicKernel::default(),
                    repo.clone(),
                    publisher,
                    config.clone(),
                )
            })
            .collect();
        Self {
            repo,
            broker,
            activity_broker,
            lanes,
            config,
        }
    }

    /// Return a clone of the workflow-task broker.
    pub fn broker(&self) -> InMemoryBroker {
        self.broker.clone()
    }

    /// Return a clone of the activity-task broker.
    pub fn activity_broker(&self) -> InMemoryActivityBroker {
        self.activity_broker.clone()
    }

    /// Return a shared reference to the run repository.
    pub fn repo(&self) -> Arc<R> {
        self.repo.clone()
    }

    /// Start a new workflow execution.
    pub async fn start_workflow(&self, request: StartRequest) -> Result<CommitResult> {
        self.submit(request.run_key, Command::Start(request)).await
    }

    /// Deliver an external signal to a running workflow.
    pub async fn signal_workflow(
        &self,
        execution: ExecutionRef,
        request: SignalRequest,
    ) -> Result<CommitResult> {
        let run_key = self
            .repo
            .resolve_execution(&execution)
            .await?
            .ok_or_else(|| anyhow!("execution not found"))?;
        self.submit(run_key, Command::Signal(request)).await
    }

    /// Long-poll for a workflow task, then atomically
    /// mark it as started.
    pub async fn poll_workflow_task(
        &self,
        queue: QueueKey,
        worker_identity: WorkerIdentity,
        timeout_after: tokio::time::Duration,
    ) -> Result<Option<StartedWorkflowTask>> {
        let offered = match self
            .broker
            .poll_workflow_task(&queue, &worker_identity, timeout_after)
            .await?
        {
            Some(task) => task,
            None => return Ok(None),
        };

        let started = self
            .start_polled_workflow_task(offered, worker_identity)
            .await?;
        Ok(Some(started))
    }

    /// Record the completion of a workflow task and
    /// apply any resulting commands.
    pub async fn complete_workflow_task(
        &self,
        req: WorkflowTaskCompletedRequest,
    ) -> Result<CommitResult> {
        let run_key = req.token.run_key;
        self.submit(run_key, Command::WorkflowTaskCompleted(req))
            .await
    }

    /// Long-poll for an activity task, then atomically
    /// mark it as started.
    pub async fn poll_activity_task(
        &self,
        queue: QueueKey,
        worker_identity: WorkerIdentity,
        timeout_after: tokio::time::Duration,
    ) -> Result<Option<StartedActivityTask>> {
        let offered = match self
            .activity_broker
            .poll_activity_task(&queue, timeout_after)
            .await?
        {
            Some(task) => task,
            None => return Ok(None),
        };

        self.start_activity_task(&offered, &worker_identity).await
    }

    /// Record a successful activity completion and
    /// resolve it in the owning workflow.
    pub async fn complete_activity_task(
        &self,
        token: ActivityTaskToken,
        result: Payloads,
    ) -> Result<CommitResult> {
        self.validate_activity_token(&token).await?;
        self.submit(
            token.run_key,
            Command::ActivityResolved(ActivityResolvedRequest {
                activity_id: token.activity_id,
                resolution: ActivityResolution::Completed { result },
                worker_identity: None,
                now: OffsetDateTime::now_utc(),
            }),
        )
        .await
    }

    /// Record an activity failure. If the retry policy
    /// allows, the activity is re-dispatched at the next
    /// attempt; otherwise it is resolved as failed.
    pub async fn fail_activity_task(
        &self,
        token: ActivityTaskToken,
        failure_message: String,
        failure_error_type: Option<String>,
    ) -> Result<()> {
        let (activity, workflow_retry_policy) =
            self.validate_activity_token(&token).await?;
        let retry_policy = activity.retry_policy.clone().or(workflow_retry_policy);

        let should_retry = retry_policy.as_ref().map(|policy| {
            evaluate_activity_retry(
                policy,
                activity.attempt,
                failure_error_type.as_deref(),
            )
        });

        if let Some(RetryDecision::Retry { next_attempt }) = should_retry {
            self.retry_activity_task(&token, next_attempt).await?;
            return Ok(());
        }

        let _ = self
            .submit(
                token.run_key,
                Command::ActivityResolved(ActivityResolvedRequest {
                    activity_id: token.activity_id,
                    resolution: ActivityResolution::Failed {
                        message: failure_message,
                    },
                    worker_identity: None,
                    now: OffsetDateTime::now_utc(),
                }),
            )
            .await?;
        Ok(())
    }

    async fn start_polled_workflow_task(
        &self,
        offered: DispatchableWorkflowTask,
        worker_identity: WorkerIdentity,
    ) -> Result<StartedWorkflowTask> {
        let now = OffsetDateTime::now_utc();
        let request = StartWorkflowTaskRequest {
            logical_seq: offered.logical_seq,
            worker_identity: worker_identity.clone(),
            sticky_ttl: Some(Duration::seconds(30)),
            now,
        };
        let result = self
            .submit(offered.run_key, Command::WorkflowTaskStarted(request))
            .await?;

        let new_state = match result {
            CommitResult::Applied { new_state } => new_state,
            CommitResult::Conflict { reason } => {
                return Err(anyhow!(
                    "failed to start workflow task due to conflict: {reason}"
                ));
            }
            CommitResult::Duplicate => {
                return Err(anyhow!("unexpected duplicate while starting workflow task"));
            }
        };

        let pending = new_state
            .pending_workflow_task
            .clone()
            .ok_or_else(|| anyhow!("workflow task missing after start"))?;
        let started_event_id = pending
            .started_event_id
            .ok_or_else(|| anyhow!("workflow task started without started_event_id"))?;

        let token = WorkflowTaskToken {
            run_key: new_state.run_key,
            logical_seq: pending.logical_seq,
            started_event_id,
            attempt: pending.attempt,
            shard_epoch: tokeira_types::ShardEpoch::ZERO,
        };

        Ok(StartedWorkflowTask {
            run_key: new_state.run_key,
            workflow_id: new_state.workflow_id,
            task_queue: new_state.task_queue,
            token,
        })
    }

    async fn submit(&self, run_key: RunKey, command: Command) -> Result<CommitResult> {
        let lane = self.pick_lane(run_key);
        lane.submit(run_key, command).await
    }

    fn pick_lane(&self, run_key: RunKey) -> &LaneHandle {
        let idx = self.lane_index(run_key);
        &self.lanes[idx]
    }

    fn lane_index(&self, run_key: RunKey) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        run_key.hash(&mut hasher);
        (hasher.finish() as usize) % self.lanes.len()
    }

    /// Sweep helper used by recovery/admin flows.
    ///
    /// Re-publishes up to `limit` dispatchable workflow
    /// tasks from durable storage into the in-memory
    /// broker.
    pub async fn republish_queue(&self, queue: QueueKey, limit: usize) -> Result<usize> {
        let tasks = self
            .repo
            .list_dispatchable_workflow_tasks(&queue, limit)
            .await?;
        let count = tasks.len();
        for task in tasks {
            self.broker.publish_workflow_task(task).await;
        }
        Ok(count)
    }

    /// Like [`republish_queue`](Self::republish_queue) but
    /// for activity tasks.
    pub async fn republish_activity_queue(
        &self,
        queue: QueueKey,
        limit: usize,
    ) -> Result<usize> {
        let tasks = self
            .repo
            .list_dispatchable_activity_tasks(&queue, limit)
            .await?;
        let count = tasks.len();
        for task in tasks {
            self.activity_broker.publish_activity_task(task).await?;
        }
        Ok(count)
    }

    async fn start_activity_task(
        &self,
        task: &DispatchableActivityTask,
        _worker_identity: &WorkerIdentity,
    ) -> Result<Option<StartedActivityTask>> {
        let mut attempts = 0u32;
        loop {
            let LoadedRun::Existing(state) = self.repo.load_run(task.run_key).await?
            else {
                return Ok(None);
            };
            if !state.is_open() {
                return Ok(None);
            }
            let Some(current) = state.activities.get(&task.activity_id).cloned() else {
                return Ok(None);
            };
            if current.attempt != task.attempt
                || current.schedule_event_id != task.schedule_event_id
            {
                return Ok(None);
            }

            let mut next_state = state.clone();
            next_state.transition_seq = state.transition_seq.next();
            let mut next_activity = current.clone();
            next_activity.stamp += 1;
            next_state
                .activities
                .insert(task.activity_id.clone(), next_activity.clone());

            let transition = Transition {
                expected_seq: state.transition_seq,
                next_state,
                history_events: SmallVec::new(),
                request_dedupe_ops: SmallVec::new(),
                activity_ops: smallvec![ActivityOp::Upsert(next_activity.clone())],
                timer_ops: SmallVec::new(),
                dispatch_ops: SmallVec::new(),
                projection_ops: SmallVec::new(),
            };

            match self
                .repo
                .commit_transition(task.run_key, transition)
                .await?
            {
                CommitResult::Applied { .. } => {
                    return Ok(Some(StartedActivityTask {
                        run_key: task.run_key,
                        activity_id: next_activity.activity_id.clone(),
                        task_queue: next_activity.task_queue.clone(),
                        token: ActivityTaskToken {
                            run_key: task.run_key,
                            activity_id: next_activity.activity_id.clone(),
                            schedule_event_id: next_activity.schedule_event_id,
                            attempt: next_activity.attempt,
                            shard_epoch: ShardEpoch::ZERO,
                        },
                        input: next_activity.input.clone(),
                        attempt: next_activity.attempt,
                        schedule_to_close_timeout: next_activity
                            .schedule_to_close_timeout,
                        start_to_close_timeout: next_activity.start_to_close_timeout,
                        heartbeat_timeout: next_activity.heartbeat_timeout,
                    }));
                }
                CommitResult::Conflict { .. } => {
                    if attempts >= self.config.max_occ_retries {
                        if let Err(error) = self
                            .activity_broker
                            .publish_activity_task(task.clone())
                            .await
                        {
                            tracing::warn!(?error, run_key = ?task.run_key, activity_id = task.activity_id, "failed to republish activity task after start conflict exhaustion");
                        }
                        return Ok(None);
                    }
                    attempts += 1;
                }
                CommitResult::Duplicate => return Ok(None),
            }
        }
    }

    async fn validate_activity_token(
        &self,
        token: &ActivityTaskToken,
    ) -> Result<(tokeira_kernel::ActivityState, Option<RetryPolicy>)> {
        let LoadedRun::Existing(state) = self.repo.load_run(token.run_key).await? else {
            return Err(anyhow!("run not found for activity token"));
        };
        let Some(activity) = state.activities.get(&token.activity_id).cloned() else {
            return Err(anyhow!("activity not found for token"));
        };
        if activity.schedule_event_id != token.schedule_event_id {
            return Err(anyhow!("activity schedule_event_id mismatch"));
        }
        if activity.attempt != token.attempt {
            return Err(anyhow!("activity attempt mismatch"));
        }
        if token.shard_epoch != ShardEpoch::ZERO {
            return Err(anyhow!("activity shard epoch mismatch"));
        }
        Ok((activity, state.retry_policy.clone()))
    }

    async fn retry_activity_task(
        &self,
        token: &ActivityTaskToken,
        next_attempt: u32,
    ) -> Result<()> {
        let mut attempts = 0u32;
        loop {
            let LoadedRun::Existing(state) = self.repo.load_run(token.run_key).await?
            else {
                return Err(anyhow!("run not found for activity retry"));
            };
            let Some(current) = state.activities.get(&token.activity_id).cloned() else {
                return Err(anyhow!("activity not found for retry"));
            };
            if current.attempt != token.attempt
                || current.schedule_event_id != token.schedule_event_id
            {
                return Err(anyhow!("stale activity token for retry"));
            }

            let mut next_state = state.clone();
            next_state.transition_seq = state.transition_seq.next();
            let mut next_activity = current.clone();
            next_activity.attempt = next_attempt;
            next_activity.stamp += 1;
            next_state
                .activities
                .insert(token.activity_id.clone(), next_activity.clone());

            let queue = QueueKey {
                namespace_id: state.namespace_id,
                task_queue: next_activity.task_queue.clone(),
                task_kind: tokeira_types::TaskKind::Activity,
                deployment: None,
                build_id: None,
            };
            let dispatch_task = DispatchableActivityTask {
                run_key: token.run_key,
                queue: queue.clone(),
                activity_id: next_activity.activity_id.clone(),
                input: next_activity.input.clone(),
                schedule_event_id: next_activity.schedule_event_id,
                attempt: next_activity.attempt,
            };
            let transition = Transition {
                expected_seq: state.transition_seq,
                next_state,
                history_events: SmallVec::new(),
                request_dedupe_ops: SmallVec::new(),
                activity_ops: smallvec![ActivityOp::Upsert(next_activity.clone())],
                timer_ops: SmallVec::new(),
                dispatch_ops: smallvec![DispatchOp::EnqueueActivityTask {
                    queue,
                    activity_id: next_activity.activity_id.clone(),
                    input: next_activity.input.clone(),
                    schedule_event_id: next_activity.schedule_event_id,
                    attempt: next_activity.attempt,
                    schedule_to_close_timeout: next_activity.schedule_to_close_timeout,
                    schedule_to_start_timeout: next_activity.schedule_to_start_timeout,
                    start_to_close_timeout: next_activity.start_to_close_timeout,
                    heartbeat_timeout: next_activity.heartbeat_timeout,
                }],
                projection_ops: SmallVec::new(),
            };

            match self
                .repo
                .commit_transition(token.run_key, transition)
                .await?
            {
                CommitResult::Applied { .. } => {
                    if let Err(error) = self
                        .activity_broker
                        .publish_activity_task(dispatch_task)
                        .await
                    {
                        tracing::warn!(?error, run_key = ?token.run_key, activity_id = token.activity_id, "failed to publish retried activity task");
                    }
                    return Ok(());
                }
                CommitResult::Conflict { .. } => {
                    if attempts >= self.config.max_occ_retries {
                        return Err(anyhow!("activity retry OCC exhausted"));
                    }
                    attempts += 1;
                }
                CommitResult::Duplicate => return Ok(()),
            }
        }
    }
}

/// A workflow task that has been polled and started.
#[derive(Clone, Debug)]
pub struct StartedWorkflowTask {
    /// Unique key for the workflow run.
    pub run_key: RunKey,
    /// Human-readable workflow identifier.
    pub workflow_id: tokeira_types::WorkflowId,
    /// Task queue the task was dispatched on.
    pub task_queue: TaskQueueName,
    /// Opaque token used to complete the task.
    pub token: WorkflowTaskToken,
}

/// An activity task that has been polled and started.
#[derive(Clone, Debug)]
pub struct StartedActivityTask {
    /// Unique key for the owning workflow run.
    pub run_key: RunKey,
    /// Identifier of the activity within the workflow.
    pub activity_id: String,
    /// Task queue the task was dispatched on.
    pub task_queue: TaskQueueName,
    /// Opaque token used to complete or fail the task.
    pub token: ActivityTaskToken,
    /// Input payloads passed to the activity.
    pub input: Payloads,
    /// Current attempt number (starts at 1).
    pub attempt: u32,
    /// Maximum time from schedule to close.
    pub schedule_to_close_timeout: Option<Duration>,
    /// Maximum time from start to close.
    pub start_to_close_timeout: Option<Duration>,
    /// Heartbeat interval; missing heartbeats trigger
    /// a timeout.
    pub heartbeat_timeout: Option<Duration>,
}

/// [`DispatchPublisher`] that forwards dispatch ops to
/// the runtime's in-memory brokers.
#[derive(Clone)]
pub struct RuntimeDispatchPublisher {
    broker: InMemoryBroker,
    activity_broker: InMemoryActivityBroker,
}

impl RuntimeDispatchPublisher {
    /// Create a publisher wired to the given brokers.
    pub fn new(broker: InMemoryBroker, activity_broker: InMemoryActivityBroker) -> Self {
        Self {
            broker,
            activity_broker,
        }
    }
}

#[async_trait]
impl DispatchPublisher for RuntimeDispatchPublisher {
    async fn publish(&self, run_key: RunKey, ops: &[DispatchOp]) -> Result<()> {
        for op in ops {
            match op {
                DispatchOp::EnqueueWorkflowTask {
                    queue,
                    logical_seq,
                    sticky_preferred,
                } => {
                    self.broker
                        .publish_workflow_task(DispatchableWorkflowTask {
                            run_key,
                            queue: queue.clone(),
                            logical_seq: *logical_seq,
                            sticky_preferred: sticky_preferred.clone(),
                            sticky_expires_at: None,
                        })
                        .await;
                }
                DispatchOp::EnqueueActivityTask { .. } => {
                    if let DispatchOp::EnqueueActivityTask {
                        queue,
                        activity_id,
                        input,
                        schedule_event_id,
                        attempt,
                        ..
                    } = op
                    {
                        if let Err(error) = self
                            .activity_broker
                            .publish_activity_task(DispatchableActivityTask {
                                run_key,
                                queue: queue.clone(),
                                activity_id: activity_id.clone(),
                                input: input.clone(),
                                schedule_event_id: *schedule_event_id,
                                attempt: *attempt,
                            })
                            .await
                        {
                            tracing::warn!(?error, run_key = ?run_key, activity_id, "failed to publish activity task");
                        }
                    }
                }
                other => {
                    tracing::info!(?other, run_key = ?run_key, "orchestration dispatch op (handler not yet wired)");
                }
            }
        }
        Ok(())
    }
}

/// Evaluate whether a failed activity should be retried.
///
/// Returns [`RetryDecision::Exhausted`] when the attempt
/// count has reached `maximum_attempts` or the error type
/// is listed in `non_retryable_error_types`.
pub fn evaluate_activity_retry(
    policy: &RetryPolicy,
    current_attempt: u32,
    failure_error_type: Option<&str>,
) -> RetryDecision {
    if let Some(error_type) = failure_error_type {
        if policy
            .non_retryable_error_types
            .iter()
            .any(|candidate| candidate == error_type)
        {
            return RetryDecision::Exhausted;
        }
    }

    if policy.maximum_attempts > 0 && current_attempt >= policy.maximum_attempts {
        return RetryDecision::Exhausted;
    }

    RetryDecision::Retry {
        next_attempt: current_attempt.saturating_add(1),
    }
}

/// Compute the backoff duration for a retry attempt
/// using exponential backoff with an optional cap.
pub fn compute_retry_backoff(policy: &RetryPolicy, attempt: u32) -> Duration {
    if policy.initial_interval.is_zero() {
        return Duration::ZERO;
    }

    let coefficient = policy.backoff_coefficient.max(1.0);
    let exponent = attempt.saturating_sub(1) as i32;
    let millis = (policy.initial_interval.whole_milliseconds() as f64)
        * coefficient.powi(exponent);
    let mut computed = Duration::milliseconds(millis.round() as i64);
    if let Some(maximum) = policy.maximum_interval {
        if computed > maximum {
            computed = maximum;
        }
    }
    computed
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use proptest::prelude::*;
    use tokio::runtime::Runtime;
    use uuid::Uuid;

    use super::*;
    use crate::broker::InMemoryBroker;
    use tokeira_storage::{DispatchableWorkflowTask, InMemoryStore};
    use tokeira_types::{LogicalTaskSeq, NamespaceId, Payloads, TaskKind};

    proptest! {
        #[test]
        fn property_deterministic_hash_routing(run in any::<u128>(), lane_count in 1usize..16usize) {
            let rt = Runtime::new().unwrap();
            let (first, second) = rt.block_on(async move {
                let repo = Arc::new(InMemoryStore::default());
                let runtime = TokeiraRuntime::new(repo, lane_count, LaneConfig::default());
                let run_key = RunKey(Uuid::from_u128(run));
                (runtime.lane_index(run_key), runtime.lane_index(run_key))
            });
            prop_assert_eq!(first, second);
            prop_assert!(first < lane_count);
        }
    }

    proptest! {
        #[test]
        fn property_idempotent_workflow_task_publication(run in any::<u128>(), logical_seq in 1u64..8u64) {
            let rt = Runtime::new().unwrap();
            let (first_some, second_none) = rt.block_on(async move {
                let broker = InMemoryBroker::default();
                let run_key = RunKey(Uuid::from_u128(run));
                let queue = QueueKey {
                    namespace_id: NamespaceId::new(),
                    task_queue: TaskQueueName("queue-a".to_string()),
                    task_kind: TaskKind::Workflow,
                    deployment: None,
                    build_id: None,
                };
                let task = DispatchableWorkflowTask {
                    run_key,
                    queue: queue.clone(),
                    logical_seq: LogicalTaskSeq(logical_seq),
                    sticky_preferred: None,
                    sticky_expires_at: None,
                };

                broker.publish_workflow_task(task.clone()).await;
                broker.publish_workflow_task(task).await;

                let worker = WorkerIdentity("worker-a".to_string());
                let first = broker
                    .poll_workflow_task(&queue, &worker, tokio::time::Duration::from_millis(1))
                    .await
                    .unwrap();
                let second = broker
                    .poll_workflow_task(&queue, &worker, tokio::time::Duration::from_millis(1))
                    .await
                    .unwrap();

                (first.is_some(), second.is_none())
            });
            prop_assert!(first_some);
            prop_assert!(second_none);
        }
    }

    #[test]
    fn evaluate_activity_retry_respects_attempt_limit_and_non_retryable_errors() {
        let policy = RetryPolicy {
            initial_interval: Duration::seconds(1),
            backoff_coefficient: 2.0,
            maximum_interval: Some(Duration::seconds(10)),
            maximum_attempts: 3,
            non_retryable_error_types: vec!["fatal".to_string()],
        };

        assert_eq!(
            evaluate_activity_retry(&policy, 1, None),
            RetryDecision::Retry { next_attempt: 2 }
        );
        assert_eq!(
            evaluate_activity_retry(&policy, 3, None),
            RetryDecision::Exhausted
        );
        assert_eq!(
            evaluate_activity_retry(&policy, 1, Some("fatal")),
            RetryDecision::Exhausted
        );
    }

    #[test]
    fn compute_retry_backoff_caps_at_maximum_interval() {
        let policy = RetryPolicy {
            initial_interval: Duration::seconds(2),
            backoff_coefficient: 3.0,
            maximum_interval: Some(Duration::seconds(10)),
            maximum_attempts: 0,
            non_retryable_error_types: Vec::new(),
        };

        assert_eq!(compute_retry_backoff(&policy, 1), Duration::seconds(2));
        assert_eq!(compute_retry_backoff(&policy, 2), Duration::seconds(6));
        assert_eq!(compute_retry_backoff(&policy, 3), Duration::seconds(10));
    }

    #[tokio::test]
    async fn runtime_dispatch_publisher_wires_activity_dispatch_to_broker() {
        let workflow_broker = InMemoryBroker::default();
        let activity_broker = InMemoryActivityBroker::default();
        let publisher =
            RuntimeDispatchPublisher::new(workflow_broker, activity_broker.clone());
        let queue = QueueKey {
            namespace_id: NamespaceId::new(),
            task_queue: TaskQueueName("activity-q".to_string()),
            task_kind: TaskKind::Activity,
            deployment: None,
            build_id: None,
        };
        let run_key = RunKey::new();

        publisher
            .publish(
                run_key,
                &[DispatchOp::EnqueueActivityTask {
                    queue: queue.clone(),
                    activity_id: "activity-1".to_string(),
                    input: Payloads::default(),
                    schedule_event_id: 11,
                    attempt: 2,
                    schedule_to_close_timeout: None,
                    schedule_to_start_timeout: None,
                    start_to_close_timeout: None,
                    heartbeat_timeout: None,
                }],
            )
            .await
            .unwrap();

        let task = activity_broker
            .poll_activity_task(&queue, std::time::Duration::from_millis(5))
            .await
            .unwrap()
            .expect("activity dispatch should be published");
        assert_eq!(task.run_key, run_key);
        assert_eq!(task.activity_id, "activity-1");
        assert_eq!(task.attempt, 2);
    }
}
