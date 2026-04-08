use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    sync::{Arc, Mutex},
};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use smallvec::{SmallVec, smallvec};
use time::{Duration, OffsetDateTime};
use tokio_util::sync::CancellationToken;
use tokeira_kernel::{
    ActivityOp, ActivityResolution, ActivityResolvedRequest, BasicKernel,
    CancelRequest, ChildStartConfirmedRequest, ChildStartResult, Command,
    DispatchOp, ExternalCancelResolvedRequest, ExternalCancelResult,
    ExternalSignalResolvedRequest, ExternalSignalResult,
    ExternalWorkflowExecution, LoadedRun, RetryState, SignalRequest,
    StartRequest, StartWorkflowTaskRequest, TerminateRequest, TimerDueRequest,
    Transition, WorkflowExecutionTimedOutRequest, WorkflowTaskCompletedRequest,
    WorkflowTimeoutType,
};
use tokeira_storage::{
    CommitResult, DispatchableActivityTask, DispatchableWorkflowTask, DueTimer,
    RunRepository,
};
use tokeira_types::{
    ActivityTaskToken, ExecutionRef, Memo, NamespaceId, Payloads, QueueKey,
    RequestContext, RequestId, RetryPolicy, RunId, RunKey, SearchAttributes,
    ShardEpoch, TaskQueueName, WorkerIdentity, WorkflowId, WorkflowTaskToken,
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
    /// Background timer scanner task.
    timer_scanner_handle: Option<tokio::task::JoinHandle<()>>,
    /// Cancellation token for the timer scanner loop.
    timer_scanner_cancel: CancellationToken,
    /// Runtime-local workflow timeout tracking.
    workflow_timeout_tracking: WorkflowTimeoutTrackingState,
    /// Background workflow-timeout scanner task.
    workflow_timeout_scanner_handle: Option<tokio::task::JoinHandle<()>>,
    /// Cancellation token for the workflow-timeout scanner.
    workflow_timeout_scanner_cancel: CancellationToken,
}

/// Configuration knobs for the background timer scanner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimerScannerConfig {
    /// Delay between storage scans for due timers.
    pub scan_interval: tokio::time::Duration,
    /// Maximum timers loaded from storage per scan cycle.
    pub max_timers_per_scan: usize,
}

impl Default for TimerScannerConfig {
    fn default() -> Self {
        Self {
            scan_interval: tokio::time::Duration::from_millis(200),
            max_timers_per_scan: 100,
        }
    }
}

/// Runtime-local timeout tracking entry for one open run.
#[derive(Clone, Debug, PartialEq)]
pub struct WorkflowTimeoutEntry {
    pub run_key: RunKey,
    pub workflow_execution_timeout: Option<Duration>,
    pub workflow_run_timeout: Option<Duration>,
    pub started_at: OffsetDateTime,
    pub first_run_started_at: Option<OffsetDateTime>,
    pub has_retry_policy: bool,
}

/// Shared in-memory tracking state for workflow timeouts.
#[derive(Clone, Default)]
pub struct WorkflowTimeoutTrackingState {
    inner: Arc<Mutex<HashMap<RunKey, WorkflowTimeoutEntry>>>,
}

impl WorkflowTimeoutTrackingState {
    pub fn insert(&self, entry: WorkflowTimeoutEntry) {
        self.inner.lock().unwrap().insert(entry.run_key, entry);
    }

    pub fn remove(&self, run_key: RunKey) {
        self.inner.lock().unwrap().remove(&run_key);
    }

    pub fn snapshot(&self) -> Vec<WorkflowTimeoutEntry> {
        self.inner.lock().unwrap().values().cloned().collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowTimeoutScannerConfig {
    pub scan_interval: tokio::time::Duration,
    pub max_timeouts_per_scan: usize,
}

impl Default for WorkflowTimeoutScannerConfig {
    fn default() -> Self {
        Self {
            scan_interval: tokio::time::Duration::from_secs(1),
            max_timeouts_per_scan: 100,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum WorkflowTimeoutViolation {
    ExecutionTimeout,
    RunTimeout,
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

pub fn evaluate_workflow_timeout(
    entry: &WorkflowTimeoutEntry,
    now: OffsetDateTime,
) -> Option<WorkflowTimeoutViolation> {
    let execution_started_at = entry.first_run_started_at.unwrap_or(entry.started_at);
    if let Some(timeout) = entry.workflow_execution_timeout {
        if now - execution_started_at > timeout
            || timeout.is_zero() && now >= execution_started_at
        {
            return Some(WorkflowTimeoutViolation::ExecutionTimeout);
        }
    }

    if let Some(timeout) = entry.workflow_run_timeout {
        if now - entry.started_at > timeout || timeout.is_zero() && now >= entry.started_at {
            return Some(WorkflowTimeoutViolation::RunTimeout);
        }
    }

    None
}

fn workflow_timeout_retry_state(entry: &WorkflowTimeoutEntry) -> RetryState {
    if entry.has_retry_policy {
        RetryState::Timeout
    } else {
        RetryState::RetryPolicyNotSet
    }
}

fn lane_index_for(run_key: RunKey, lane_count: usize) -> usize {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    run_key.hash(&mut hasher);
    (hasher.finish() as usize) % lane_count.max(1)
}

fn pick_lane(
    lanes: &[LaneHandle],
    lane_count: usize,
    run_key: RunKey,
) -> &LaneHandle {
    debug_assert!(!lanes.is_empty());
    debug_assert_eq!(lanes.len(), lane_count.max(1));
    &lanes[lane_index_for(run_key, lane_count.max(1)) % lanes.len()]
}

async fn scan_due_timers_once<R, F, Fut>(
    repo: &R,
    config: &TimerScannerConfig,
    mut submit_due_timer: F,
) where
    R: RunRepository + ?Sized,
    F: FnMut(DueTimer, OffsetDateTime) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let fired_at = OffsetDateTime::now_utc();
    let due_timers = match repo
        .list_due_timers(fired_at, config.max_timers_per_scan)
        .await
    {
        Ok(due_timers) => due_timers,
        Err(error) => {
            tracing::warn!(?error, "timer scanner failed to list due timers");
            return;
        }
    };

    for due in due_timers {
        if let Err(error) = submit_due_timer(due.clone(), fired_at).await {
            let message = error.to_string();
            if message.contains("kernel rejected") {
                tracing::debug!(
                    ?error,
                    run_key = ?due.run_key,
                    timer_id = due.timer_id,
                    "timer scanner due timer rejected by kernel"
                );
            } else {
                tracing::warn!(
                    ?error,
                    run_key = ?due.run_key,
                    timer_id = due.timer_id,
                    "timer scanner failed to submit due timer"
                );
            }
        }
    }
}

async fn scan_workflow_timeouts_once<F, Fut>(
    tracking: &WorkflowTimeoutTrackingState,
    config: &WorkflowTimeoutScannerConfig,
    mut submit_timeout: F,
) where
    F: FnMut(WorkflowTimeoutEntry, WorkflowTimeoutViolation, OffsetDateTime) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let now = OffsetDateTime::now_utc();
    let entries = tracking.snapshot();
    let mut submitted = 0usize;

    for entry in entries {
        if submitted >= config.max_timeouts_per_scan {
            break;
        }
        let Some(violation) = evaluate_workflow_timeout(&entry, now) else {
            continue;
        };

        match submit_timeout(entry.clone(), violation, now).await {
            Ok(()) => tracking.remove(entry.run_key),
            Err(error) => {
                let message = error.to_string();
                if message.contains("kernel rejected") {
                    tracing::debug!(
                        ?error,
                        run_key = ?entry.run_key,
                        "workflow timeout scanner timeout rejected by kernel"
                    );
                    tracking.remove(entry.run_key);
                } else {
                    tracing::warn!(
                        ?error,
                        run_key = ?entry.run_key,
                        "workflow timeout scanner failed to submit timeout"
                    );
                }
            }
        }
        submitted += 1;
    }
}

async fn run_timer_scanner<R>(
    repo: Arc<R>,
    lanes: Vec<LaneHandle>,
    lane_count: usize,
    config: TimerScannerConfig,
    cancel: CancellationToken,
) where
    R: RunRepository + 'static,
{
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(config.scan_interval) => {}
        }

        scan_due_timers_once(&*repo, &config, |due, fired_at| {
            let lane = pick_lane(&lanes, lane_count, due.run_key).clone();
            async move {
                lane.submit(
                    due.run_key,
                    Command::TimerDue(TimerDueRequest {
                        timer_id: due.timer_id,
                        fired_at,
                    }),
                )
                .await
                .map(|_| ())
            }
        })
        .await;
    }
}

async fn run_workflow_timeout_scanner(
    tracking: WorkflowTimeoutTrackingState,
    lanes: Vec<LaneHandle>,
    lane_count: usize,
    config: WorkflowTimeoutScannerConfig,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(config.scan_interval) => {}
        }

        scan_workflow_timeouts_once(&tracking, &config, |entry, violation, now| {
            let lane = pick_lane(&lanes, lane_count, entry.run_key).clone();
            async move {
                lane.submit(
                    entry.run_key,
                    Command::WorkflowExecutionTimedOut(WorkflowExecutionTimedOutRequest {
                        timeout_type: match violation {
                            WorkflowTimeoutViolation::ExecutionTimeout => WorkflowTimeoutType::ExecutionTimeout,
                            WorkflowTimeoutViolation::RunTimeout => WorkflowTimeoutType::RunTimeout,
                        },
                        retry_state: workflow_timeout_retry_state(&entry),
                        now,
                    }),
                )
                .await
                .map(|_| ())
            }
        })
        .await;
    }
}

impl<R> TokeiraRuntime<R>
where
    R: RunRepository + 'static,
{
    /// Create a new runtime with `lane_count` parallel
    /// lane executors backed by `repo`.
    pub fn new(
        repo: Arc<R>,
        lane_count: usize,
        config: LaneConfig,
        timer_config: TimerScannerConfig,
        workflow_timeout_config: WorkflowTimeoutScannerConfig,
    ) -> Self {
        let broker = InMemoryBroker::default();
        let activity_broker = InMemoryActivityBroker::default();
        let workflow_timeout_tracking = WorkflowTimeoutTrackingState::default();
        let lane_count = lane_count.max(1);
        let shared_lanes = Arc::new(Mutex::new(Vec::with_capacity(lane_count)));
        let lanes: Vec<_> = (0..lane_count)
            .map(|_| {
                let publisher = RuntimeDispatchPublisher::new(
                    broker.clone(),
                    activity_broker.clone(),
                    repo.clone(),
                    shared_lanes.clone(),
                    lane_count,
                );
                spawn_lane(
                    BasicKernel::default(),
                    repo.clone(),
                    publisher,
                    workflow_timeout_tracking.clone(),
                    config.clone(),
                )
            })
            .collect();
        *shared_lanes.lock().unwrap() = lanes.clone();
        let timer_scanner_cancel = CancellationToken::new();
        let timer_scanner_handle = Some(tokio::spawn(run_timer_scanner(
            repo.clone(),
            lanes.clone(),
            lane_count,
            timer_config,
            timer_scanner_cancel.clone(),
        )));
        let workflow_timeout_scanner_cancel = CancellationToken::new();
        let workflow_timeout_scanner_handle = Some(tokio::spawn(
            run_workflow_timeout_scanner(
                workflow_timeout_tracking.clone(),
                lanes.clone(),
                lane_count,
                workflow_timeout_config,
                workflow_timeout_scanner_cancel.clone(),
            ),
        ));
        Self {
            repo,
            broker,
            activity_broker,
            lanes,
            config,
            timer_scanner_handle,
            timer_scanner_cancel,
            workflow_timeout_tracking,
            workflow_timeout_scanner_handle,
            workflow_timeout_scanner_cancel,
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

    pub fn workflow_timeout_tracking(&self) -> WorkflowTimeoutTrackingState {
        self.workflow_timeout_tracking.clone()
    }

    /// Start a new workflow execution.
    pub async fn start_workflow(&self, request: StartRequest) -> Result<CommitResult> {
        let result = self
            .submit(request.run_key, Command::Start(request.clone()))
            .await?;
        if matches!(result, CommitResult::Applied { .. })
            && (request.workflow_execution_timeout.is_some()
                || request.workflow_run_timeout.is_some())
        {
            self.workflow_timeout_tracking.insert(WorkflowTimeoutEntry {
                run_key: request.run_key,
                workflow_execution_timeout: request.workflow_execution_timeout,
                workflow_run_timeout: request.workflow_run_timeout,
                started_at: request.now,
                first_run_started_at: request.first_run_started_at,
                has_retry_policy: request.retry_policy.is_some(),
            });
        }
        Ok(result)
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

    /// Forcibly terminate a workflow execution.
    pub async fn terminate_workflow(
        &self,
        execution: ExecutionRef,
        request: TerminateRequest,
    ) -> Result<CommitResult> {
        let run_key = self
            .repo
            .resolve_execution(&execution)
            .await?
            .ok_or_else(|| anyhow!("execution not found"))?;
        self.submit(run_key, Command::Terminate(request)).await
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
        pick_lane(&self.lanes, self.lanes.len(), run_key)
    }

    #[cfg(test)]
    fn lane_index(&self, run_key: RunKey) -> usize {
        lane_index_for(run_key, self.lanes.len())
    }

    /// Cancel the background timer scanner and wait for
    /// it to stop.
    pub async fn shutdown_timer_scanner(&mut self) -> Result<()> {
        self.timer_scanner_cancel.cancel();
        if let Some(handle) = self.timer_scanner_handle.take() {
            tokio::time::timeout(tokio::time::Duration::from_secs(5), handle)
                .await
                .map_err(|_| anyhow!("timer scanner shutdown timed out"))?
                .map_err(|error| anyhow!("timer scanner join failed: {error}"))?;
        }
        Ok(())
    }

    pub async fn shutdown_workflow_timeout_scanner(&mut self) -> Result<()> {
        self.workflow_timeout_scanner_cancel.cancel();
        if let Some(handle) = self.workflow_timeout_scanner_handle.take() {
            tokio::time::timeout(tokio::time::Duration::from_secs(5), handle)
                .await
                .map_err(|_| anyhow!("workflow timeout scanner shutdown timed out"))?
                .map_err(|error| anyhow!("workflow timeout scanner join failed: {error}"))?;
        }
        Ok(())
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
pub struct RuntimeDispatchPublisher<R> {
    broker: InMemoryBroker,
    activity_broker: InMemoryActivityBroker,
    repo: Arc<R>,
    lanes: Arc<Mutex<Vec<LaneHandle>>>,
    lane_count: usize,
}

impl<R> Clone for RuntimeDispatchPublisher<R> {
    fn clone(&self) -> Self {
        Self {
            broker: self.broker.clone(),
            activity_broker: self.activity_broker.clone(),
            repo: self.repo.clone(),
            lanes: self.lanes.clone(),
            lane_count: self.lane_count,
        }
    }
}

impl<R> RuntimeDispatchPublisher<R>
where
    R: RunRepository + 'static,
{
    /// Create a publisher wired to the given brokers.
    pub fn new(
        broker: InMemoryBroker,
        activity_broker: InMemoryActivityBroker,
        repo: Arc<R>,
        lanes: Arc<Mutex<Vec<LaneHandle>>>,
        lane_count: usize,
    ) -> Self {
        Self {
            broker,
            activity_broker,
            repo,
            lanes,
            lane_count,
        }
    }

    fn pick_lane(&self, run_key: RunKey) -> LaneHandle {
        let lanes = self.lanes.lock().unwrap();
        pick_lane(&lanes, self.lane_count, run_key).clone()
    }

    async fn resolve_child_run_key(
        &self,
        namespace_id: NamespaceId,
        child_workflow_id: &WorkflowId,
        child_run_id: RunId,
    ) -> Result<Option<RunKey>> {
        self.repo
            .resolve_execution(&ExecutionRef {
                namespace_id,
                workflow_id: child_workflow_id.clone(),
                run_id: Some(child_run_id),
            })
            .await
    }

    async fn handle_start_child_workflow(
        &self,
        child_workflow_id: WorkflowId,
        namespace_id: NamespaceId,
        workflow_type: tokeira_types::WorkflowType,
        task_queue: TaskQueueName,
        input: Payloads,
        parent_run_key: RunKey,
        parent_workflow_id: WorkflowId,
        initiated_event_id: i64,
    ) {
        let child_run_key = RunKey::new();
        let child_run_id = RunId::new();
        let start_request = StartRequest {
            run_key: child_run_key,
            namespace_id,
            workflow_id: child_workflow_id.clone(),
            run_id: child_run_id,
            workflow_type: workflow_type.clone(),
            task_queue,
            input,
            memo: Memo::default(),
            search_attributes: SearchAttributes::default(),
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: Duration::seconds(10),
            retry_policy: None,
            attempt: 1,
            continued_execution_run_id: None,
            first_execution_run_id: None,
            parent_run_key: Some(parent_run_key),
            parent_workflow_id: Some(parent_workflow_id),
            first_run_started_at: None,
            request: RequestContext {
                request_id: RequestId(format!("child-start-{child_run_key:?}")),
                caller_identity: Some("runtime-child-orchestrator".to_string()),
                received_at: OffsetDateTime::now_utc(),
            },
            now: OffsetDateTime::now_utc(),
        };

        let result = self
            .pick_lane(child_run_key)
            .submit(child_run_key, Command::Start(start_request))
            .await;
        let confirmation = match result {
            Ok(CommitResult::Applied { .. }) => ChildStartResult::Started {
                child_run_id,
                workflow_type,
            },
            Ok(CommitResult::Conflict { reason }) => ChildStartResult::Failed {
                cause: reason,
            },
            Ok(CommitResult::Duplicate) => ChildStartResult::Failed {
                cause: "duplicate start request".to_string(),
            },
            Err(error) => ChildStartResult::Failed {
                cause: error.to_string(),
            },
        };

        let confirm = Command::ChildStartConfirmed(ChildStartConfirmedRequest {
            child_workflow_id: child_workflow_id.clone(),
            initiated_event_id,
            result: confirmation,
            now: OffsetDateTime::now_utc(),
        });
        if let Err(error) = self
            .pick_lane(parent_run_key)
            .submit(parent_run_key, confirm)
            .await
        {
            tracing::warn!(
                ?error,
                parent_run_key = ?parent_run_key,
                child_workflow_id = ?child_workflow_id,
                "failed to deliver child start confirmation"
            );
        }
    }

    async fn handle_terminate_child(
        &self,
        namespace_id: NamespaceId,
        child_workflow_id: WorkflowId,
        child_run_id: RunId,
        reason: String,
    ) {
        match self
            .resolve_child_run_key(namespace_id, &child_workflow_id, child_run_id)
            .await
        {
            Ok(Some(child_run_key)) => {
                let command = Command::Terminate(TerminateRequest {
                    reason,
                    details: None,
                    identity: "parent-close-policy".to_string(),
                    request: RequestContext {
                        request_id: RequestId(format!("terminate-child-{child_run_id:?}")),
                        caller_identity: Some("runtime-child-orchestrator".to_string()),
                        received_at: OffsetDateTime::now_utc(),
                    },
                    now: OffsetDateTime::now_utc(),
                });
                if let Err(error) = self
                    .pick_lane(child_run_key)
                    .submit(child_run_key, command)
                    .await
                {
                    let message = error.to_string();
                    if message.contains("kernel rejected")
                        || message.contains("not found")
                    {
                        tracing::debug!(
                            ?error,
                            child_run_key = ?child_run_key,
                            child_workflow_id = ?child_workflow_id,
                            "terminate child no-op"
                        );
                    } else {
                        tracing::warn!(
                            ?error,
                            child_run_key = ?child_run_key,
                            child_workflow_id = ?child_workflow_id,
                            "terminate child dispatch failed"
                        );
                    }
                }
            }
            Ok(None) => {
                tracing::debug!(
                    child_workflow_id = ?child_workflow_id,
                    child_run_id = ?child_run_id,
                    "terminate child skipped because child was not found"
                );
            }
            Err(error) => {
                tracing::warn!(
                    ?error,
                    child_workflow_id = ?child_workflow_id,
                    child_run_id = ?child_run_id,
                    "terminate child resolution failed"
                );
            }
        }
    }

    async fn handle_cancel_child(
        &self,
        namespace_id: NamespaceId,
        child_workflow_id: WorkflowId,
        child_run_id: RunId,
        reason: String,
    ) {
        match self
            .resolve_child_run_key(namespace_id, &child_workflow_id, child_run_id)
            .await
        {
            Ok(Some(child_run_key)) => {
                let command = Command::Cancel(CancelRequest {
                    reason,
                    external_initiator: None,
                    request: RequestContext {
                        request_id: RequestId(format!("cancel-child-{child_run_id:?}")),
                        caller_identity: Some("runtime-child-orchestrator".to_string()),
                        received_at: OffsetDateTime::now_utc(),
                    },
                    now: OffsetDateTime::now_utc(),
                });
                if let Err(error) = self
                    .pick_lane(child_run_key)
                    .submit(child_run_key, command)
                    .await
                {
                    let message = error.to_string();
                    if message.contains("kernel rejected")
                        || message.contains("not found")
                    {
                        tracing::debug!(
                            ?error,
                            child_run_key = ?child_run_key,
                            child_workflow_id = ?child_workflow_id,
                            "cancel child no-op"
                        );
                    } else {
                        tracing::warn!(
                            ?error,
                            child_run_key = ?child_run_key,
                            child_workflow_id = ?child_workflow_id,
                            "cancel child dispatch failed"
                        );
                    }
                }
            }
            Ok(None) => {
                tracing::debug!(
                    child_workflow_id = ?child_workflow_id,
                    child_run_id = ?child_run_id,
                    "cancel child skipped because child was not found"
                );
            }
            Err(error) => {
                tracing::warn!(
                    ?error,
                    child_workflow_id = ?child_workflow_id,
                    child_run_id = ?child_run_id,
                    "cancel child resolution failed"
                );
            }
        }
    }

    async fn handle_signal_external_workflow(
        &self,
        target_workflow_id: WorkflowId,
        target_run_id: Option<RunId>,
        signal_name: String,
        input: Payloads,
        originator_run_key: RunKey,
        namespace_id: NamespaceId,
        initiated_event_id: i64,
    ) {
        let signal_result = match self
            .repo
            .resolve_execution(&ExecutionRef {
                namespace_id,
                workflow_id: target_workflow_id.clone(),
                run_id: target_run_id,
            })
            .await
        {
            Ok(Some(target_run_key)) => {
                let command = Command::Signal(SignalRequest {
                    signal_name,
                    input,
                    request: RequestContext {
                        request_id: RequestId(format!(
                            "ext-signal-{originator_run_key:?}-{initiated_event_id}"
                        )),
                        caller_identity: Some(
                            "runtime-external-signal-orchestrator".to_string(),
                        ),
                        received_at: OffsetDateTime::now_utc(),
                    },
                    now: OffsetDateTime::now_utc(),
                });
                match self
                    .pick_lane(target_run_key)
                    .submit(target_run_key, command)
                    .await
                {
                    Ok(CommitResult::Applied { .. }) | Ok(CommitResult::Duplicate) => {
                        ExternalSignalResult::Signaled
                    }
                    Ok(CommitResult::Conflict { reason }) => {
                        ExternalSignalResult::Failed { cause: reason }
                    }
                    Err(error) => ExternalSignalResult::Failed {
                        cause: error.to_string(),
                    },
                }
            }
            Ok(None) => ExternalSignalResult::Failed {
                cause: format!(
                    "target workflow not found: {}",
                    target_workflow_id.0
                ),
            },
            Err(error) => ExternalSignalResult::Failed {
                cause: error.to_string(),
            },
        };

        let resolve = Command::ExternalSignalResolved(ExternalSignalResolvedRequest {
            initiated_event_id,
            result: signal_result,
            now: OffsetDateTime::now_utc(),
        });
        if let Err(error) = self
            .pick_lane(originator_run_key)
            .submit(originator_run_key, resolve)
            .await
        {
            tracing::warn!(
                ?error,
                originator_run_key = ?originator_run_key,
                initiated_event_id,
                "failed to deliver ExternalSignalResolved to originator"
            );
        }
    }

    async fn handle_cancel_external_workflow(
        &self,
        target_workflow_id: WorkflowId,
        target_run_id: Option<RunId>,
        originator_run_key: RunKey,
        originator_namespace_id: NamespaceId,
        originator_workflow_id: WorkflowId,
        originator_run_id: RunId,
        namespace_id: NamespaceId,
        initiated_event_id: i64,
        reason: String,
    ) {
        let cancel_result = match self
            .repo
            .resolve_execution(&ExecutionRef {
                namespace_id,
                workflow_id: target_workflow_id.clone(),
                run_id: target_run_id,
            })
            .await
        {
            Ok(Some(target_run_key)) => {
                let command = Command::Cancel(CancelRequest {
                    reason,
                    external_initiator: Some(ExternalWorkflowExecution {
                        namespace_id: originator_namespace_id,
                        workflow_id: originator_workflow_id,
                        run_id: originator_run_id,
                    }),
                    request: RequestContext {
                        request_id: RequestId(format!(
                            "ext-cancel-{originator_run_key:?}-{initiated_event_id}"
                        )),
                        caller_identity: Some(
                            "runtime-external-cancel-orchestrator".to_string(),
                        ),
                        received_at: OffsetDateTime::now_utc(),
                    },
                    now: OffsetDateTime::now_utc(),
                });
                match self
                    .pick_lane(target_run_key)
                    .submit(target_run_key, command)
                    .await
                {
                    Ok(CommitResult::Applied { .. }) | Ok(CommitResult::Duplicate) => {
                        ExternalCancelResult::CancelRequested
                    }
                    Ok(CommitResult::Conflict { reason }) => {
                        ExternalCancelResult::Failed { cause: reason }
                    }
                    Err(error) => ExternalCancelResult::Failed {
                        cause: error.to_string(),
                    },
                }
            }
            Ok(None) => ExternalCancelResult::Failed {
                cause: format!(
                    "target workflow not found: {}",
                    target_workflow_id.0
                ),
            },
            Err(error) => ExternalCancelResult::Failed {
                cause: error.to_string(),
            },
        };

        let resolve = Command::ExternalCancelResolved(ExternalCancelResolvedRequest {
            initiated_event_id,
            result: cancel_result,
            now: OffsetDateTime::now_utc(),
        });
        if let Err(error) = self
            .pick_lane(originator_run_key)
            .submit(originator_run_key, resolve)
            .await
        {
            tracing::warn!(
                ?error,
                originator_run_key = ?originator_run_key,
                initiated_event_id,
                "failed to deliver ExternalCancelResolved to originator"
            );
        }
    }
}

#[async_trait]
impl<R> DispatchPublisher for RuntimeDispatchPublisher<R>
where
    R: RunRepository + 'static,
{
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
                DispatchOp::StartChildWorkflow {
                    child_workflow_id,
                    namespace_id,
                    workflow_type,
                    task_queue,
                    input,
                    parent_run_key,
                    parent_workflow_id,
                    initiated_event_id,
                } => {
                    let publisher = RuntimeDispatchPublisher::clone(self);
                    let child_workflow_id = child_workflow_id.clone();
                    let workflow_type = workflow_type.clone();
                    let task_queue = task_queue.clone();
                    let input = input.clone();
                    let parent_workflow_id = parent_workflow_id.clone();
                    let namespace_id = *namespace_id;
                    let parent_run_key = *parent_run_key;
                    let initiated_event_id = *initiated_event_id;
                    tokio::spawn(async move {
                        publisher
                            .handle_start_child_workflow(
                                child_workflow_id,
                                namespace_id,
                                workflow_type,
                                task_queue,
                                input,
                                parent_run_key,
                                parent_workflow_id,
                                initiated_event_id,
                            )
                            .await;
                    });
                }
                DispatchOp::TerminateChild {
                    namespace_id,
                    child_workflow_id,
                    child_run_id,
                    reason,
                } => {
                    let publisher = RuntimeDispatchPublisher::clone(self);
                    let namespace_id = *namespace_id;
                    let child_workflow_id = child_workflow_id.clone();
                    let child_run_id = *child_run_id;
                    let reason = reason.clone();
                    tokio::spawn(async move {
                        publisher
                            .handle_terminate_child(
                                namespace_id,
                                child_workflow_id,
                                child_run_id,
                                reason,
                            )
                            .await;
                    });
                }
                DispatchOp::CancelChild {
                    namespace_id,
                    child_workflow_id,
                    child_run_id,
                    reason,
                } => {
                    let publisher = RuntimeDispatchPublisher::clone(self);
                    let namespace_id = *namespace_id;
                    let child_workflow_id = child_workflow_id.clone();
                    let child_run_id = *child_run_id;
                    let reason = reason.clone();
                    tokio::spawn(async move {
                        publisher
                            .handle_cancel_child(
                                namespace_id,
                                child_workflow_id,
                                child_run_id,
                                reason,
                            )
                            .await;
                    });
                }
                DispatchOp::SignalExternalWorkflow {
                    originator_run_key,
                    namespace_id,
                    initiated_event_id,
                    target_workflow_id,
                    target_run_id,
                    signal_name,
                    input,
                } => {
                    let publisher = RuntimeDispatchPublisher::clone(self);
                    let target_workflow_id = target_workflow_id.clone();
                    let target_run_id = *target_run_id;
                    let signal_name = signal_name.clone();
                    let input = input.clone();
                    let originator_run_key = *originator_run_key;
                    let namespace_id = *namespace_id;
                    let initiated_event_id = *initiated_event_id;
                    tokio::spawn(async move {
                        publisher
                            .handle_signal_external_workflow(
                                target_workflow_id,
                                target_run_id,
                                signal_name,
                                input,
                                originator_run_key,
                                namespace_id,
                                initiated_event_id,
                            )
                            .await;
                    });
                }
                DispatchOp::RequestCancelExternalWorkflow {
                    originator_run_key,
                    originator_namespace_id,
                    originator_workflow_id,
                    originator_run_id,
                    namespace_id,
                    initiated_event_id,
                    reason,
                    target_workflow_id,
                    target_run_id,
                } => {
                    let publisher = RuntimeDispatchPublisher::clone(self);
                    let target_workflow_id = target_workflow_id.clone();
                    let target_run_id = *target_run_id;
                    let originator_run_key = *originator_run_key;
                    let originator_namespace_id = *originator_namespace_id;
                    let originator_workflow_id = originator_workflow_id.clone();
                    let originator_run_id = *originator_run_id;
                    let namespace_id = *namespace_id;
                    let initiated_event_id = *initiated_event_id;
                    let reason = reason.clone();
                    tokio::spawn(async move {
                        publisher
                            .handle_cancel_external_workflow(
                                target_workflow_id,
                                target_run_id,
                                originator_run_key,
                                originator_namespace_id,
                                originator_workflow_id,
                                originator_run_id,
                                namespace_id,
                                initiated_event_id,
                                reason,
                            )
                            .await;
                    });
                }
                other => {
                    tracing::info!(?other, run_key = ?run_key, "orchestration dispatch op (handler not yet wired)");
                }
            }
        }
        Ok(())
    }

    async fn submit_to_run(
        &self,
        run_key: RunKey,
        command: Command,
    ) -> Result<CommitResult> {
        self.pick_lane(run_key).submit(run_key, command).await
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
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };

    use proptest::prelude::*;
    use tokio::runtime::Runtime;
    use uuid::Uuid;

    use super::*;
    use crate::broker::InMemoryBroker;
    use tokeira_storage::{
        BacklogEntry, CommitResult, DispatchableWorkflowTask, InMemoryStore,
        RequestRecord, TransitionAuditRecord,
    };
    use tokeira_types::{
        ExecutionRef, LogicalTaskSeq, Memo, NamespaceId, Payloads,
        RequestContext, RequestId, SearchAttributes, TaskKind, WorkflowId,
    };

    proptest! {
        #[test]
        fn property_deterministic_hash_routing(run in any::<u128>(), lane_count in 1usize..16usize) {
            let rt = Runtime::new().unwrap();
            let (first, second) = rt.block_on(async move {
                let repo = Arc::new(InMemoryStore::default());
                let runtime = TokeiraRuntime::new(
                    repo,
                    lane_count,
                    LaneConfig::default(),
                    TimerScannerConfig::default(),
                    WorkflowTimeoutScannerConfig::default(),
                );
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
        let repo = Arc::new(MockTimerRepo::from_responses(Vec::new()));
        let publisher = RuntimeDispatchPublisher::new(
            workflow_broker,
            activity_broker.clone(),
            repo,
            Arc::new(Mutex::new(Vec::new())),
            1,
        );
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

    #[test]
    fn timer_scanner_config_default_values() {
        let config = TimerScannerConfig::default();
        assert_eq!(config.scan_interval, tokio::time::Duration::from_millis(200));
        assert_eq!(config.max_timers_per_scan, 100);
    }

    #[test]
    fn evaluate_workflow_timeout_cases() {
        let started_at = OffsetDateTime::now_utc() - Duration::seconds(10);

        let both = WorkflowTimeoutEntry {
            run_key: RunKey::new(),
            workflow_execution_timeout: Some(Duration::seconds(1)),
            workflow_run_timeout: Some(Duration::seconds(2)),
            started_at,
            first_run_started_at: None,
            has_retry_policy: true,
        };
        assert_eq!(
            evaluate_workflow_timeout(&both, OffsetDateTime::now_utc()),
            Some(WorkflowTimeoutViolation::ExecutionTimeout)
        );

        let zero = WorkflowTimeoutEntry {
            run_key: RunKey::new(),
            workflow_execution_timeout: Some(Duration::ZERO),
            workflow_run_timeout: None,
            started_at: OffsetDateTime::now_utc(),
            first_run_started_at: None,
            has_retry_policy: false,
        };
        assert_eq!(
            evaluate_workflow_timeout(&zero, zero.started_at),
            Some(WorkflowTimeoutViolation::ExecutionTimeout)
        );

        let none = WorkflowTimeoutEntry {
            run_key: RunKey::new(),
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            started_at,
            first_run_started_at: None,
            has_retry_policy: false,
        };
        assert_eq!(evaluate_workflow_timeout(&none, OffsetDateTime::now_utc()), None);

        let run_only = WorkflowTimeoutEntry {
            run_key: RunKey::new(),
            workflow_execution_timeout: None,
            workflow_run_timeout: Some(Duration::seconds(1)),
            started_at,
            first_run_started_at: None,
            has_retry_policy: false,
        };
        assert_eq!(
            evaluate_workflow_timeout(&run_only, OffsetDateTime::now_utc()),
            Some(WorkflowTimeoutViolation::RunTimeout)
        );

        let exec_only = WorkflowTimeoutEntry {
            run_key: RunKey::new(),
            workflow_execution_timeout: Some(Duration::seconds(1)),
            workflow_run_timeout: None,
            started_at,
            first_run_started_at: None,
            has_retry_policy: false,
        };
        assert_eq!(
            evaluate_workflow_timeout(&exec_only, OffsetDateTime::now_utc()),
            Some(WorkflowTimeoutViolation::ExecutionTimeout)
        );

        let not_elapsed = WorkflowTimeoutEntry {
            run_key: RunKey::new(),
            workflow_execution_timeout: Some(Duration::seconds(30)),
            workflow_run_timeout: Some(Duration::seconds(20)),
            started_at,
            first_run_started_at: None,
            has_retry_policy: false,
        };
        assert_eq!(
            evaluate_workflow_timeout(&not_elapsed, started_at + Duration::seconds(5)),
            None
        );
    }

    #[test]
    fn workflow_timeout_scanner_config_default_values() {
        let config = WorkflowTimeoutScannerConfig::default();
        assert_eq!(config.scan_interval, tokio::time::Duration::from_secs(1));
        assert_eq!(config.max_timeouts_per_scan, 100);
    }

    #[test]
    fn workflow_timeout_tracking_state_crud() {
        let tracking = WorkflowTimeoutTrackingState::default();
        let entry = WorkflowTimeoutEntry {
            run_key: RunKey::new(),
            workflow_execution_timeout: Some(Duration::seconds(1)),
            workflow_run_timeout: None,
            started_at: OffsetDateTime::now_utc(),
            first_run_started_at: None,
            has_retry_policy: true,
        };
        tracking.insert(entry.clone());
        assert_eq!(tracking.snapshot(), vec![entry.clone()]);
        tracking.remove(entry.run_key);
        assert!(tracking.snapshot().is_empty());
    }

    proptest! {
        #[test]
        fn property_workflow_timeout_evaluation_correctness(
            exec_secs in proptest::option::of(0i64..20),
            run_secs in proptest::option::of(0i64..20),
            elapsed_secs in 0i64..40,
            chain_extra_secs in 0i64..40,
            use_chain_origin in any::<bool>(),
        ) {
            let now = OffsetDateTime::now_utc();
            let started_at = now - Duration::seconds(elapsed_secs);
            let first_run_started_at = use_chain_origin.then(|| {
                started_at - Duration::seconds(chain_extra_secs)
            });
            let entry = WorkflowTimeoutEntry {
                run_key: RunKey::new(),
                workflow_execution_timeout: exec_secs.map(Duration::seconds),
                workflow_run_timeout: run_secs.map(Duration::seconds),
                started_at,
                first_run_started_at,
                has_retry_policy: false,
            };

            let result = evaluate_workflow_timeout(&entry, now);
            let execution_origin =
                entry.first_run_started_at.unwrap_or(entry.started_at);
            if let Some(exec) = entry.workflow_execution_timeout {
                if now - execution_origin > exec
                    || (exec.is_zero() && now >= execution_origin)
                {
                    prop_assert_eq!(result, Some(WorkflowTimeoutViolation::ExecutionTimeout));
                    return Ok(());
                }
            }
            if let Some(run) = entry.workflow_run_timeout {
                if now - started_at > run || (run.is_zero() && now >= started_at) {
                    prop_assert_eq!(result, Some(WorkflowTimeoutViolation::RunTimeout));
                    return Ok(());
                }
            }
            prop_assert_eq!(result, None);
        }
    }

    proptest! {
        #[test]
        fn property_workflow_timeout_retry_state_derivation(has_retry_policy in any::<bool>()) {
            let entry = WorkflowTimeoutEntry {
                run_key: RunKey::new(),
                workflow_execution_timeout: Some(Duration::seconds(1)),
                workflow_run_timeout: None,
                started_at: OffsetDateTime::now_utc() - Duration::seconds(10),
                first_run_started_at: None,
                has_retry_policy,
            };
            let expected = if has_retry_policy {
                RetryState::Timeout
            } else {
                RetryState::RetryPolicyNotSet
            };
            prop_assert_eq!(workflow_timeout_retry_state(&entry), expected);
        }
    }

    proptest! {
        #[test]
        fn property_pick_lane_matches_runtime_lane_index(run in any::<u128>(), lane_count in 1usize..16usize) {
            let rt = Runtime::new().unwrap();
            let picked = rt.block_on(async move {
                let repo = Arc::new(InMemoryStore::default());
                let runtime = TokeiraRuntime::new(
                    repo,
                    lane_count,
                    LaneConfig::default(),
                    TimerScannerConfig::default(),
                    WorkflowTimeoutScannerConfig::default(),
                );
                let run_key = RunKey(Uuid::from_u128(run));
                let lane_ptr = pick_lane(&runtime.lanes, lane_count, run_key) as *const LaneHandle as usize;
                let expected_ptr = &runtime.lanes[runtime.lane_index(run_key)] as *const LaneHandle as usize;
                (lane_ptr, expected_ptr)
            });
            prop_assert_eq!(picked.0, picked.1);
        }
    }

    proptest! {
        #[test]
        fn property_due_timers_produce_timer_due_submissions(
            runs in proptest::collection::vec(any::<u128>(), 0..20),
            timer_ids in proptest::collection::vec("[a-z0-9]{1,8}", 0..20),
            lane_count in 1usize..16usize,
        ) {
            let rt = Runtime::new().unwrap();
            rt.block_on(async move {
                let due_timers: Vec<_> = runs
                    .into_iter()
                    .zip(timer_ids.into_iter())
                    .map(|(run, timer_id)| DueTimer {
                        run_key: RunKey(Uuid::from_u128(run)),
                        timer_id,
                    })
                    .collect();
                let repo = MockTimerRepo::from_responses(vec![TimerListResponse::Ok(due_timers.clone())]);
                let captured = Arc::new(Mutex::new(Vec::new()));
                let captured_clone = captured.clone();
                let config = TimerScannerConfig {
                    scan_interval: tokio::time::Duration::from_millis(1),
                    max_timers_per_scan: 500,
                };

                scan_due_timers_once(&repo, &config, move |due, fired_at| {
                    let captured = captured_clone.clone();
                    async move {
                        captured.lock().unwrap().push((
                            due.run_key,
                            due.timer_id,
                            lane_index_for(due.run_key, lane_count),
                            fired_at,
                        ));
                        Ok(())
                    }
                }).await;

                let captured = captured.lock().unwrap();
                prop_assert_eq!(captured.len(), due_timers.len());
                for (index, due) in due_timers.iter().enumerate() {
                    let (run_key, timer_id, lane_index, _) = &captured[index];
                    prop_assert_eq!(*run_key, due.run_key);
                    prop_assert_eq!(timer_id, &due.timer_id);
                    prop_assert_eq!(*lane_index, lane_index_for(due.run_key, lane_count));
                }
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }

    proptest! {
        #[test]
        fn property_batch_limit_is_respected(limit in 1usize..200usize) {
            let rt = Runtime::new().unwrap();
            rt.block_on(async move {
                let repo = MockTimerRepo::from_responses(vec![TimerListResponse::Ok(Vec::new())]);
                let config = TimerScannerConfig {
                    scan_interval: tokio::time::Duration::from_millis(1),
                    max_timers_per_scan: limit,
                };
                scan_due_timers_once(&repo, &config, |_due, _fired_at| async move { Ok(()) }).await;
                let recorded = repo.recorded_limits();
                prop_assert_eq!(recorded, vec![limit]);
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }

    proptest! {
        #[test]
        fn property_consistent_fired_at_within_scan(
            runs in proptest::collection::vec(any::<u128>(), 2..10usize),
        ) {
            let rt = Runtime::new().unwrap();
            rt.block_on(async move {
                let due_timers: Vec<_> = runs
                    .into_iter()
                    .enumerate()
                    .map(|(index, run)| DueTimer {
                        run_key: RunKey(Uuid::from_u128(run)),
                        timer_id: format!("timer-{index}"),
                    })
                    .collect();
                let repo = MockTimerRepo::from_responses(vec![TimerListResponse::Ok(due_timers)]);
                let fired_ats = Arc::new(Mutex::new(Vec::new()));
                let fired_ats_clone = fired_ats.clone();
                scan_due_timers_once(&repo, &TimerScannerConfig::default(), move |_due, fired_at| {
                    let fired_ats = fired_ats_clone.clone();
                    async move {
                        fired_ats.lock().unwrap().push(fired_at);
                        Ok(())
                    }
                }).await;

                let fired_ats = fired_ats.lock().unwrap();
                prop_assert!(!fired_ats.is_empty());
                let first = fired_ats[0];
                prop_assert!(fired_ats.iter().all(|value| *value == first));
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }

    proptest! {
        #[test]
        fn property_per_entry_failure_resilience(
            runs in proptest::collection::vec(any::<u128>(), 1..20usize),
            fail_pattern in proptest::collection::vec(any::<bool>(), 1..20usize),
        ) {
            let rt = Runtime::new().unwrap();
            rt.block_on(async move {
                let due_timers: Vec<_> = runs
                    .into_iter()
                    .enumerate()
                    .map(|(index, run)| DueTimer {
                        run_key: RunKey(Uuid::from_u128(run)),
                        timer_id: format!("timer-{index}"),
                    })
                    .collect();
                let repo = MockTimerRepo::from_responses(vec![TimerListResponse::Ok(due_timers.clone())]);
                let successes = Arc::new(Mutex::new(Vec::new()));
                let successes_clone = successes.clone();
                let fail_pattern_for_submit = fail_pattern.clone();
                scan_due_timers_once(&repo, &TimerScannerConfig::default(), move |due, _fired_at| {
                    let successes = successes_clone.clone();
                    let should_fail = fail_pattern_for_submit
                        .get(due.timer_id.trim_start_matches("timer-").parse::<usize>().unwrap_or(0) % fail_pattern_for_submit.len())
                        .copied()
                        .unwrap_or(false);
                    async move {
                        if should_fail {
                            Err(anyhow!("lane channel closed"))
                        } else {
                            successes.lock().unwrap().push(due.timer_id);
                            Ok(())
                        }
                    }
                }).await;

                let expected_successes = due_timers
                    .iter()
                    .filter(|due| {
                        !fail_pattern
                            .get(due.timer_id.trim_start_matches("timer-").parse::<usize>().unwrap_or(0) % fail_pattern.len())
                            .copied()
                            .unwrap_or(false)
                    })
                    .count();
                prop_assert_eq!(successes.lock().unwrap().len(), expected_successes);
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }

    proptest! {
        #[test]
        fn property_storage_error_resilience(
            runs in proptest::collection::vec(any::<u128>(), 1..10usize),
        ) {
            let rt = Runtime::new().unwrap();
            rt.block_on(async move {
                let due_timers: Vec<_> = runs
                    .into_iter()
                    .enumerate()
                    .map(|(index, run)| DueTimer {
                        run_key: RunKey(Uuid::from_u128(run)),
                        timer_id: format!("timer-{index}"),
                    })
                    .collect();
                let repo = MockTimerRepo::from_responses(vec![
                    TimerListResponse::Err("transient storage failure".to_string()),
                    TimerListResponse::Ok(due_timers.clone()),
                ]);
                let captured = Arc::new(Mutex::new(Vec::new()));
                let captured_clone = captured.clone();
                let config = TimerScannerConfig::default();

                scan_due_timers_once(&repo, &config, |_due, _fired_at| async move { Ok(()) }).await;
                scan_due_timers_once(&repo, &config, move |due, _fired_at| {
                    let captured = captured_clone.clone();
                    async move {
                        captured.lock().unwrap().push(due.timer_id);
                        Ok(())
                    }
                }).await;

                prop_assert_eq!(captured.lock().unwrap().len(), due_timers.len());
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }

    #[tokio::test]
    async fn timer_scanner_handle_is_present_after_runtime_new() {
        let repo = Arc::new(InMemoryStore::default());
        let mut runtime = TokeiraRuntime::new(
            repo,
            2,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
        );
        assert!(runtime.timer_scanner_handle.is_some());
        runtime.shutdown_timer_scanner().await.unwrap();
    }

    #[tokio::test]
    async fn timer_scanner_shutdown_completes_promptly() {
        let repo = Arc::new(InMemoryStore::default());
        let mut runtime = TokeiraRuntime::new(
            repo,
            2,
            LaneConfig::default(),
            TimerScannerConfig {
                scan_interval: tokio::time::Duration::from_secs(60),
                max_timers_per_scan: 100,
            },
            WorkflowTimeoutScannerConfig::default(),
        );
        runtime.shutdown_timer_scanner().await.unwrap();
        assert!(runtime.timer_scanner_handle.is_none());
    }

    #[tokio::test]
    async fn workflow_timeout_scanner_handle_is_present_after_runtime_new() {
        let repo = Arc::new(InMemoryStore::default());
        let mut runtime = TokeiraRuntime::new(
            repo,
            2,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
        );
        assert!(runtime.workflow_timeout_scanner_handle.is_some());
        runtime.shutdown_workflow_timeout_scanner().await.unwrap();
    }

    #[tokio::test]
    async fn workflow_timeout_scanner_shutdown_completes_promptly() {
        let repo = Arc::new(InMemoryStore::default());
        let mut runtime = TokeiraRuntime::new(
            repo,
            2,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig {
                scan_interval: tokio::time::Duration::from_secs(60),
                max_timeouts_per_scan: 100,
            },
        );
        runtime.shutdown_workflow_timeout_scanner().await.unwrap();
        assert!(runtime.workflow_timeout_scanner_handle.is_none());
    }

    #[tokio::test]
    async fn start_workflow_with_timeout_populates_tracking_state() {
        let repo = Arc::new(InMemoryStore::default());
        let mut runtime = TokeiraRuntime::new(
            repo,
            2,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
        );
        let request = sample_start_request(Some(Duration::seconds(5)), Some(Duration::seconds(3)));
        let result = runtime.start_workflow(request.clone()).await.unwrap();
        assert!(matches!(result, CommitResult::Applied { .. }));

        let snapshot = runtime.workflow_timeout_tracking().snapshot();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].run_key, request.run_key);
        assert_eq!(snapshot[0].workflow_execution_timeout, request.workflow_execution_timeout);
        assert_eq!(snapshot[0].workflow_run_timeout, request.workflow_run_timeout);
        assert_eq!(snapshot[0].started_at, request.now);
        assert!(snapshot[0].has_retry_policy == request.retry_policy.is_some());

        runtime.shutdown_timer_scanner().await.unwrap();
        runtime.shutdown_workflow_timeout_scanner().await.unwrap();
    }

    proptest! {
        #[test]
        fn property_start_with_timeout_populates_tracking_state(
            execution_timeout_secs in proptest::option::of(1i64..20),
            run_timeout_secs in proptest::option::of(1i64..20),
            has_retry_policy in any::<bool>(),
        ) {
            let rt = Runtime::new().unwrap();
            rt.block_on(async move {
                let repo = Arc::new(InMemoryStore::default());
                let mut runtime = TokeiraRuntime::new(
                    repo,
                    2,
                    LaneConfig::default(),
                    TimerScannerConfig::default(),
                    WorkflowTimeoutScannerConfig::default(),
                );
                let mut request = sample_start_request(
                    execution_timeout_secs.map(Duration::seconds),
                    run_timeout_secs.map(Duration::seconds),
                );
                if has_retry_policy {
                    request.retry_policy = Some(RetryPolicy {
                        initial_interval: Duration::seconds(1),
                        backoff_coefficient: 2.0,
                        maximum_interval: Some(Duration::seconds(10)),
                        maximum_attempts: 3,
                        non_retryable_error_types: Vec::new(),
                    });
                }
                runtime.start_workflow(request.clone()).await.unwrap();
                let snapshot = runtime.workflow_timeout_tracking().snapshot();
                if request.workflow_execution_timeout.is_some() || request.workflow_run_timeout.is_some() {
                    prop_assert_eq!(snapshot.len(), 1);
                    let entry = &snapshot[0];
                    prop_assert_eq!(entry.run_key, request.run_key);
                    prop_assert_eq!(entry.workflow_execution_timeout, request.workflow_execution_timeout);
                    prop_assert_eq!(entry.workflow_run_timeout, request.workflow_run_timeout);
                    prop_assert_eq!(entry.started_at, request.now);
                    prop_assert_eq!(entry.has_retry_policy, has_retry_policy);
                } else {
                    prop_assert!(snapshot.is_empty());
                }
                runtime.shutdown_timer_scanner().await.unwrap();
                runtime.shutdown_workflow_timeout_scanner().await.unwrap();
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }

    proptest! {
        #[test]
        fn property_scan_workflow_timeouts_consistent_now_and_batch_bound(
            count in 2usize..20usize,
            max_batch in 1usize..10usize,
        ) {
            let rt = Runtime::new().unwrap();
            rt.block_on(async move {
                let tracking = WorkflowTimeoutTrackingState::default();
                for index in 0..count {
                    tracking.insert(WorkflowTimeoutEntry {
                        run_key: RunKey::new(),
                        workflow_execution_timeout: Some(Duration::seconds(1)),
                        workflow_run_timeout: None,
                        started_at: OffsetDateTime::now_utc() - Duration::seconds(10 + index as i64),
                        first_run_started_at: None,
                        has_retry_policy: index % 2 == 0,
                    });
                }
                let seen = Arc::new(Mutex::new(Vec::new()));
                let seen_clone = seen.clone();
                scan_workflow_timeouts_once(
                    &tracking,
                    &WorkflowTimeoutScannerConfig {
                        scan_interval: tokio::time::Duration::from_secs(1),
                        max_timeouts_per_scan: max_batch,
                    },
                    move |entry, violation, now| {
                        let seen = seen_clone.clone();
                        async move {
                            seen.lock().unwrap().push((entry.run_key, violation, now));
                            Ok(())
                        }
                    }
                ).await;
                let seen = seen.lock().unwrap();
                prop_assert!(seen.len() <= max_batch);
                if !seen.is_empty() {
                    let now = seen[0].2;
                    prop_assert!(seen.iter().all(|(_, _, candidate)| *candidate == now));
                }
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }

    proptest! {
        #[test]
        fn property_scan_workflow_timeouts_handles_kernel_rejections_and_lane_errors(
            count in 1usize..20usize,
            rejection_mod in 1usize..5usize,
            lane_error_mod in 1usize..5usize,
        ) {
            let rt = Runtime::new().unwrap();
            rt.block_on(async move {
                let tracking = WorkflowTimeoutTrackingState::default();
                let mut entries = Vec::new();
                for _ in 0..count {
                    let entry = WorkflowTimeoutEntry {
                        run_key: RunKey::new(),
                        workflow_execution_timeout: Some(Duration::seconds(1)),
                        workflow_run_timeout: None,
                        started_at: OffsetDateTime::now_utc() - Duration::seconds(10),
                        first_run_started_at: None,
                        has_retry_policy: false,
                    };
                    tracking.insert(entry.clone());
                    entries.push(entry);
                }

                let entries_for_submit = entries.clone();
                scan_workflow_timeouts_once(
                    &tracking,
                    &WorkflowTimeoutScannerConfig::default(),
                    move |entry, _violation, _now| {
                        let entries_for_submit = entries_for_submit.clone();
                        async move {
                            let idx = entries_for_submit.iter().position(|candidate| candidate.run_key == entry.run_key).unwrap_or(0);
                            if idx % rejection_mod == 0 {
                                Err(anyhow!("kernel rejected command: closed"))
                            } else if idx % lane_error_mod == 0 {
                                Err(anyhow!("lane channel closed"))
                            } else {
                                Ok(())
                            }
                        }
                    }
                ).await;

                let remaining = tracking.snapshot();
                for (idx, entry) in entries.iter().enumerate() {
                    let should_remove = idx % rejection_mod == 0 || idx % lane_error_mod != 0;
                    let present = remaining.iter().any(|candidate| candidate.run_key == entry.run_key);
                    if should_remove {
                        prop_assert!(!present);
                    } else {
                        prop_assert!(present);
                    }
                }
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }

    fn sample_start_request(
        workflow_execution_timeout: Option<Duration>,
        workflow_run_timeout: Option<Duration>,
    ) -> StartRequest {
        StartRequest {
            run_key: RunKey::new(),
            namespace_id: NamespaceId::new(),
            workflow_id: WorkflowId("workflow-timeout".to_string()),
            run_id: tokeira_types::RunId::new(),
            workflow_type: tokeira_types::WorkflowType("example".to_string()),
            task_queue: TaskQueueName("workflow-q".to_string()),
            input: Payloads::default(),
            memo: Memo::default(),
            search_attributes: SearchAttributes::default(),
            workflow_execution_timeout,
            workflow_run_timeout,
            workflow_task_timeout: Duration::seconds(10),
            retry_policy: None,
            attempt: 1,
            continued_execution_run_id: None,
            first_execution_run_id: None,
            parent_run_key: None,
            parent_workflow_id: None,
            first_run_started_at: None,
            request: RequestContext {
                request_id: RequestId("req-timeout".to_string()),
                caller_identity: None,
                received_at: OffsetDateTime::now_utc(),
            },
            now: OffsetDateTime::now_utc(),
        }
    }

    enum TimerListResponse {
        Ok(Vec<DueTimer>),
        Err(String),
    }

    struct MockTimerRepo {
        responses: Mutex<VecDeque<TimerListResponse>>,
        limits: Mutex<Vec<usize>>,
    }

    impl MockTimerRepo {
        fn from_responses(responses: Vec<TimerListResponse>) -> Self {
            Self {
                responses: Mutex::new(VecDeque::from(responses)),
                limits: Mutex::new(Vec::new()),
            }
        }

        fn recorded_limits(&self) -> Vec<usize> {
            self.limits.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl RunRepository for MockTimerRepo {
        async fn resolve_execution(
            &self,
            _execution: &ExecutionRef,
        ) -> Result<Option<RunKey>> {
            panic!("unused in timer scanner tests")
        }

        async fn load_run(&self, _run_key: RunKey) -> Result<LoadedRun> {
            panic!("unused in timer scanner tests")
        }

        async fn read_history(
            &self,
            _run_key: RunKey,
            _after_event_id: i64,
            _limit: usize,
        ) -> Result<Vec<tokeira_kernel::HistoryEvent>> {
            panic!("unused in timer scanner tests")
        }

        async fn lookup_request_dedupe(
            &self,
            _execution: &ExecutionRef,
            _request_id: &tokeira_types::RequestId,
        ) -> Result<Option<RequestRecord>> {
            panic!("unused in timer scanner tests")
        }

        async fn read_transition_audit(
            &self,
            _run_key: RunKey,
        ) -> Result<Vec<TransitionAuditRecord>> {
            panic!("unused in timer scanner tests")
        }

        async fn commit_transition(
            &self,
            _run_key: RunKey,
            _transition: Transition,
        ) -> Result<CommitResult> {
            panic!("unused in timer scanner tests")
        }

        async fn list_dispatchable_workflow_tasks(
            &self,
            _queue: &QueueKey,
            _limit: usize,
        ) -> Result<Vec<DispatchableWorkflowTask>> {
            panic!("unused in timer scanner tests")
        }

        async fn list_dispatchable_activity_tasks(
            &self,
            _queue: &QueueKey,
            _limit: usize,
        ) -> Result<Vec<DispatchableActivityTask>> {
            panic!("unused in timer scanner tests")
        }

        async fn persist_to_backlog(&self, _entries: Vec<BacklogEntry>) -> Result<()> {
            panic!("unused in timer scanner tests")
        }

        async fn drain_backlog(
            &self,
            _queue: &QueueKey,
            _limit: usize,
        ) -> Result<Vec<BacklogEntry>> {
            panic!("unused in timer scanner tests")
        }

        async fn list_due_timers(
            &self,
            _now: OffsetDateTime,
            limit: usize,
        ) -> Result<Vec<DueTimer>> {
            self.limits.lock().unwrap().push(limit);
            match self.responses.lock().unwrap().pop_front() {
                Some(TimerListResponse::Ok(due_timers)) => Ok(due_timers),
                Some(TimerListResponse::Err(message)) => Err(anyhow!(message)),
                None => Ok(Vec::new()),
            }
        }
    }
}
