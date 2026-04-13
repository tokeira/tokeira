use std::collections::BTreeMap;

use time::{Duration, OffsetDateTime};
use tokeira_types::{
    BuildId, DeploymentId, ExecutionStatus, Headers, LogicalTaskSeq, Memo,
    NamespaceId, Payloads, RetryPolicy, RunId, RunKey, SearchAttributes,
    StickyAffinity, TaskQueueName, TransitionSeq, WorkflowId, WorkflowType,
};

/// Durable state for an open or closed workflow run.
///
/// This state is intentionally *summary shaped*. The authoritative event stream
/// is still history, but the runtime needs a compact, mutation-friendly view so
/// it can process commands without replaying the whole run every time.
#[derive(Clone, Debug, PartialEq)]
pub struct WorkflowState {
    /// Composite storage key for this run.
    pub run_key: RunKey,
    /// Namespace that owns the execution.
    pub namespace_id: NamespaceId,
    /// User-assigned workflow identifier.
    pub workflow_id: WorkflowId,
    /// Unique identifier for this specific run.
    pub run_id: RunId,
    /// Workflow type name (maps to an SDK handler).
    pub workflow_type: WorkflowType,
    /// Task queue where workflow tasks are dispatched.
    pub task_queue: TaskQueueName,
    /// Optional deployment for versioned task routing.
    pub deployment: Option<DeploymentId>,
    /// Optional build identifier for versioned task routing.
    pub build_id: Option<BuildId>,

    /// Current lifecycle status (Running, Paused, or a
    /// terminal state).
    pub status: ExecutionStatus,
    /// Optimistic-concurrency fence for committed transitions.
    /// See `docs/architecture/020-kernel.md`.
    pub transition_seq: TransitionSeq,
    /// Highest event ID assigned so far in this run.
    pub last_event_id: i64,
    /// Next logical task sequence to assign when scheduling a
    /// workflow task.
    pub next_workflow_task_seq: LogicalTaskSeq,
    /// The currently pending workflow task, if any. At most
    /// one WFT is pending at a time.
    pub pending_workflow_task: Option<PendingWorkflowTask>,
    /// Sticky execution affinity recorded when a worker
    /// provides a `sticky_ttl`.
    pub sticky: Option<StickyAffinity>,
    /// Pause metadata when the workflow is paused.
    pub pause_info: Option<PauseInfo>,
    /// Monotonic stamp incremented on pause/unpause to
    /// invalidate in-flight workflow task deliveries.
    pub wft_stamp: u64,

    /// Unindexed key-value metadata attached to the execution.
    pub memo: Memo,
    /// Indexed attributes for visibility queries.
    pub search_attributes: SearchAttributes,
    /// Maximum wall-clock time for the entire execution chain.
    pub workflow_execution_timeout: Option<Duration>,
    /// Maximum wall-clock time for a single run.
    pub workflow_run_timeout: Option<Duration>,
    /// Maximum time a worker may hold a workflow task.
    pub workflow_task_timeout: Duration,
    /// Retry policy governing automatic retries on failure.
    pub retry_policy: Option<RetryPolicy>,
    /// Current retry attempt number (1-based).
    pub attempt: u32,
    /// Run ID of the very first run in the execution chain.
    pub first_execution_run_id: Option<RunId>,
    /// Parent run identity if this execution is a child.
    pub parent_run_key: Option<RunKey>,
    /// Parent workflow identity if this execution is a child.
    pub parent_workflow_id: Option<WorkflowId>,
    /// Open activities keyed by activity ID.
    pub activities: BTreeMap<String, ActivityState>,
    /// Open timers keyed by timer ID.
    pub timers: BTreeMap<String, TimerState>,
    /// Open child workflows keyed by child workflow ID.
    pub children: BTreeMap<WorkflowId, ChildWorkflowState>,
    /// Pending external signal deliveries keyed by initiated
    /// event ID.
    pub pending_external_signals: BTreeMap<i64, PendingExternalSignal>,
    /// Pending external cancel deliveries keyed by initiated
    /// event ID.
    pub pending_external_cancels: BTreeMap<i64, PendingExternalCancel>,
    /// Pending workflow updates keyed by update ID.
    pub pending_updates: BTreeMap<String, PendingUpdate>,
    /// Pending Nexus operations keyed by operation ID.
    pub pending_nexus_operations: BTreeMap<String, PendingNexusOperation>,
    /// Versioning override for this execution, if set.
    pub versioning_override: Option<VersioningOverride>,
    /// Completion callbacks attached to this execution.
    pub completion_callbacks: Vec<CompletionCallback>,

    /// Timestamp when the first event was recorded.
    pub started_at: OffsetDateTime,
    /// Timestamp when the very first run in the execution
    /// chain started.
    pub first_run_started_at: Option<OffsetDateTime>,
    /// Timestamp when the execution reached a terminal state.
    /// `None` while the execution is still open.
    pub closed_at: Option<OffsetDateTime>,
    /// Result payload retained for terminal completion.
    pub close_result: Option<Payloads>,
    /// Failure message retained for terminal failure.
    pub close_failure: Option<String>,
}

impl WorkflowState {
    /// Returns `true` when the execution is still in progress
    /// (`Running` or `Paused`).
    pub fn is_open(&self) -> bool {
        self.status.is_open()
    }
}

/// Authoritative record of a pending workflow task.
///
/// The kernel uses this to validate that starts, completions,
/// failures, and timeouts reference the correct task. At most
/// one `PendingWorkflowTask` exists per run at any time.
///
/// See `docs/architecture/020-kernel.md` §Pending workflow
/// task model.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingWorkflowTask {
    /// Logical task sequence assigned at schedule time.
    pub logical_seq: LogicalTaskSeq,
    /// Event ID of the `WorkflowTaskScheduled` event.
    pub scheduled_event_id: i64,
    /// Event ID of the `WorkflowTaskStarted` event, or `None`
    /// if the task has not yet been picked up by a worker.
    pub started_event_id: Option<i64>,
    /// Number of times this task has been started (incremented
    /// on each start, including retries after failure/timeout).
    pub attempt: u32,
}

/// Durable state for a single open activity.
///
/// Carries the full set of parameters needed to re-dispatch
/// the activity after pause/unpause or retry. The `stamp`
/// field is a monotonic invalidation counter used to detect
/// stale deliveries.
#[derive(Clone, Debug, PartialEq)]
pub struct ActivityState {
    /// User-assigned activity identifier.
    pub activity_id: String,
    /// Activity type name (maps to an SDK handler).
    pub activity_type: String,
    /// Event ID of the `ActivityTaskScheduled` event.
    pub schedule_event_id: i64,
    /// Task queue where the activity is dispatched.
    pub task_queue: TaskQueueName,
    /// Optional deployment override for this activity.
    pub deployment: Option<DeploymentId>,
    /// Optional build identifier override for this activity.
    pub build_id: Option<BuildId>,
    /// Arguments passed to the activity function.
    pub input: Payloads,
    /// Transport headers carried with the activity task.
    pub header: Option<Headers>,
    /// Current attempt number (1-based, incremented on retry).
    pub attempt: u32,
    /// Retry policy for this activity.
    pub retry_policy: Option<RetryPolicy>,
    /// Maximum time from schedule to completion.
    pub schedule_to_close_timeout: Option<Duration>,
    /// Maximum time from schedule to worker pickup.
    pub schedule_to_start_timeout: Option<Duration>,
    /// Maximum time from worker pickup to completion.
    pub start_to_close_timeout: Option<Duration>,
    /// Maximum time between heartbeats.
    pub heartbeat_timeout: Option<Duration>,
    /// When the activity was originally scheduled.
    pub scheduled_at: OffsetDateTime,
    /// When the activity was started by a worker, if it
    /// has started.
    pub started_at: Option<OffsetDateTime>,
    /// Event ID of the `ActivityTaskStarted` event, if the
    /// activity has been started.
    pub started_event_id: Option<i64>,
    /// Pause metadata when the activity is individually
    /// paused.
    pub pause_info: Option<ActivityPauseInfo>,
    /// Monotonic stamp incremented on pause/unpause/option
    /// changes to invalidate in-flight deliveries.
    pub stamp: u64,
}

/// Metadata recorded when a workflow is paused.
#[derive(Clone, Debug, PartialEq)]
pub struct PauseInfo {
    /// Wall-clock time the pause was applied.
    pub pause_time: OffsetDateTime,
    /// Identity of the caller who issued the pause.
    pub identity: String,
    /// Human-readable reason for the pause.
    pub reason: String,
    /// Request ID used for idempotent re-delivery of the
    /// pause command.
    pub request_id: String,
}

/// Metadata recorded when an individual activity is paused.
#[derive(Clone, Debug, PartialEq)]
pub struct ActivityPauseInfo {
    /// Wall-clock time the pause was applied.
    pub pause_time: OffsetDateTime,
    /// Identity of the caller who issued the pause.
    pub identity: String,
    /// Human-readable reason for the pause.
    pub reason: String,
}

/// Durable state for a single open timer.
#[derive(Clone, Debug, PartialEq)]
pub struct TimerState {
    /// User-assigned timer identifier.
    pub timer_id: String,
    /// Event ID of the `TimerStarted` event.
    pub started_event_id: i64,
    /// Absolute wall-clock time when the timer should fire.
    pub fire_at: OffsetDateTime,
}

/// Durable state for a single open child workflow.
#[derive(Clone, Debug, PartialEq)]
pub struct ChildWorkflowState {
    /// Workflow ID of the child.
    pub child_workflow_id: WorkflowId,
    /// Namespace that owns the child workflow.
    pub namespace_id: NamespaceId,
    /// Run ID assigned to the child, once started.
    pub child_run_id: Option<RunId>,
    /// Event ID of the initiation event in the parent's
    /// history.
    pub initiated_event_id: i64,
    /// Event ID of the `ChildWorkflowExecutionStarted` event,
    /// once the child has started.
    pub started_event_id: Option<i64>,
    /// What to do with this child when the parent closes.
    pub parent_close_policy: ParentClosePolicy,
}

/// What happens to a child workflow when its parent closes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParentClosePolicy {
    /// Forcibly terminate the child.
    Terminate,
    /// Send a cooperative cancel request to the child.
    RequestCancel,
    /// Leave the child running (detach).
    Abandon,
}

/// Tracks an in-flight signal to an external workflow.
#[derive(Clone, Debug, PartialEq)]
pub struct PendingExternalSignal {
    /// Event ID of the initiation event (used as map key).
    pub initiated_event_id: i64,
    /// Workflow ID of the signal target.
    pub target_workflow_id: WorkflowId,
    /// Optional run ID of the signal target.
    pub target_run_id: Option<RunId>,
    /// Name of the signal being delivered.
    pub signal_name: String,
}

/// Tracks an in-flight cancel request to an external
/// workflow.
#[derive(Clone, Debug, PartialEq)]
pub struct PendingExternalCancel {
    /// Event ID of the initiation event (used as map key).
    pub initiated_event_id: i64,
    /// Workflow ID of the cancel target.
    pub target_workflow_id: WorkflowId,
    /// Optional run ID of the cancel target.
    pub target_run_id: Option<RunId>,
}

/// Tracks a workflow update that has been accepted but not
/// yet completed or rejected by the worker.
#[derive(Clone, Debug, PartialEq)]
pub struct PendingUpdate {
    /// Caller-assigned update identifier.
    pub update_id: String,
    /// Event ID of the `WorkflowExecutionUpdateAccepted`
    /// event.
    pub accepted_event_id: i64,
    /// Name of the update handler.
    pub name: String,
}

/// Tracks a Nexus operation that has been scheduled but not
/// yet reached a terminal state.
#[derive(Clone, Debug, PartialEq)]
pub struct PendingNexusOperation {
    /// Operation identifier.
    pub operation_id: String,
    /// Event ID of the `NexusOperationScheduled` event.
    pub scheduled_event_id: i64,
    /// Nexus endpoint name.
    pub endpoint: String,
    /// Nexus service name.
    pub service: String,
    /// Nexus operation name.
    pub operation: String,
    /// Maximum time from schedule to completion.
    pub schedule_to_close_timeout: Option<Duration>,
    /// When the operation was scheduled.
    pub scheduled_at: OffsetDateTime,
    /// Whether the operation has transitioned to async-started.
    pub started: bool,
}

/// Placeholder for worker versioning override configuration.
///
/// TODO(correctness): flesh out once versioning is
/// implemented.
#[derive(Clone, Debug, PartialEq)]
pub struct VersioningOverride;

/// Placeholder for completion callback configuration.
///
/// TODO(correctness): flesh out once completion callbacks are
/// implemented.
#[derive(Clone, Debug, PartialEq)]
pub struct CompletionCallback;

/// Either the run does not yet exist or it already has durable state.
#[derive(Clone, Debug, PartialEq)]
pub enum LoadedRun {
    /// The run does not yet exist in durable storage. Only
    /// the `Start` command accepts this variant.
    Absent,
    /// The run exists and carries its current durable state.
    Existing(WorkflowState),
}
