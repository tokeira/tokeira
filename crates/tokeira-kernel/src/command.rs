use serde::{Deserialize, Serialize};
use time::{Duration, OffsetDateTime};
use tokeira_types::{
    BuildId, DeploymentId, Headers, LogicalTaskSeq, Memo, NamespaceId, Payload, Payloads,
    RequestContext, RetryPolicy, RunId, RunKey, SearchAttributes, TaskQueueName, WorkerIdentity,
    WorkflowId, WorkflowTaskToken, WorkflowType,
};

use crate::{
    event::ActivityResolution,
    state::{
        CompletionCallback, Link, OnConflictOptions, ParentClosePolicy, Priority, UserMetadata,
        VersioningBehavior, VersioningOverride, WorkerDeploymentVersionRef,
    },
};

/// Commands are authoritative things that the server has decided happened.
///
/// A command is not the same as a transport message. By the time something gets
/// here, routing, auth, idempotency lookup, and request shaping should already
/// have happened.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Command {
    /// Create a brand-new workflow execution.
    Start(StartRequest),
    /// Create a workflow and deliver a signal atomically.
    SignalWithStart(SignalWithStartRequest),
    /// Deliver a synchronous update to the workflow.
    Update(UpdateRequest),
    /// Deliver an asynchronous signal to the workflow.
    Signal(SignalRequest),
    /// Request cooperative cancellation of the workflow.
    Cancel(CancelRequest),
    /// Forcibly terminate the workflow without cleanup.
    Terminate(TerminateRequest),
    /// Reset the workflow to an earlier history point.
    Reset(ResetRequest),
    /// Pause the workflow (no new tasks dispatched).
    PauseWorkflow(PauseWorkflowRequest),
    /// Resume a paused workflow.
    UnpauseWorkflow(UnpauseWorkflowRequest),
    /// Update timeout/routing options on a pending activity.
    UpdateActivityOptions(UpdateActivityOptionsRequest),
    /// Pause a specific activity.
    PauseActivity(PauseActivityRequest),
    /// Resume a paused activity.
    UnpauseActivity(UnpauseActivityRequest),
    /// Reset a pending activity back to attempt 1.
    ResetActivity(ResetActivityRequest),
    /// Update execution-level options (versioning, callbacks).
    UpdateExecutionOptions(UpdateExecutionOptionsRequest),
    /// The workflow's execution or run timeout fired.
    WorkflowExecutionTimedOut(WorkflowExecutionTimedOutRequest),
    /// A worker picked up a workflow task.
    WorkflowTaskStarted(StartWorkflowTaskRequest),
    /// Start a Worker Deployment transition decided outside the WFT-start path.
    StartDeploymentTransition(StartDeploymentTransitionRequest),
    /// A worker completed a workflow task with commands.
    WorkflowTaskCompleted(WorkflowTaskCompletedRequest),
    /// A worker completed a cron-enabled workflow task whose terminal command
    /// should author a cron continue-as-new successor.
    WorkflowTaskCompletedWithCron {
        /// Normal workflow-task completion request.
        request: WorkflowTaskCompletedRequest,
        /// Runtime-computed cron successor metadata.
        cron_continuation: CronContinuation,
    },
    /// A worker completed a workflow task whose terminal `FailWorkflow` command
    /// triggers a retry-policy continuation. The runtime evaluated the retry
    /// decision (`service/history/workflow/retry.go @ v1.31.0`) and supplies the
    /// outcome; the kernel only records it. Mutually exclusive with
    /// `WorkflowTaskCompletedWithCron` per completion — the runtime selects at
    /// most one (retry is evaluated before cron).
    WorkflowTaskCompletedWithRetry {
        /// Normal workflow-task completion request.
        request: WorkflowTaskCompletedRequest,
        /// Runtime-computed retry decision for the failing run.
        retry_continuation: RetryContinuation,
    },
    /// A workflow task failed (non-determinism, bad commands).
    WorkflowTaskFailed(WorkflowTaskFailedRequest),
    /// `RespondWorkflowTaskFailed(GRPC_MESSAGE_TOO_LARGE)`: force-close the
    /// started WFT and terminate the run instead of retrying.
    TerminateOnWorkflowTaskFailed(TerminateOnWorkflowTaskFailedRequest),
    /// A workflow task exceeded its start-to-close timeout.
    WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest),
    /// An activity reached a terminal state.
    ActivityResolved(ActivityResolvedRequest),
    /// A child workflow start was confirmed or failed.
    ChildStartConfirmed(ChildStartConfirmedRequest),
    /// A child workflow reached a terminal state.
    ChildResolved(ChildResolvedRequest),
    /// An external signal delivery completed or failed.
    ExternalSignalResolved(ExternalSignalResolvedRequest),
    /// An external cancel delivery completed or failed.
    ExternalCancelResolved(ExternalCancelResolvedRequest),
    /// A Nexus operation reached a terminal or started state.
    NexusOperationResolved(NexusOperationResolvedRequest),
    /// A backing-off Nexus operation's next-attempt time arrived; re-dispatch it.
    NexusOperationRetry(NexusOperationRetryRequest),
    /// A timer's deadline was reached.
    TimerDue(TimerDueRequest),
    /// The internal first-workflow-task delay elapsed.
    WorkflowStartDelayElapsed(WorkflowStartDelayElapsedRequest),
    /// Schedule a WFT so that a pending query can be delivered
    /// to a worker. Only schedules if no WFT is already pending.
    ScheduleQueryTask(ScheduleQueryTaskRequest),
    /// Record the outcome of a completion-callback delivery attempt, advancing the
    /// callback's durable lifecycle. Applies to a *closed* run (callbacks fire after
    /// close), unlike most commands which require an open run.
    CompletionCallbackAttempted(CompletionCallbackAttemptedRequest),
}

/// Outcome of one completion-callback delivery attempt, recorded durably so retries
/// survive crashes and the `DescribeWorkflowExecution` callback surface is accurate.
///
/// The runtime computes `next_attempt_at` from the configured backoff policy and the
/// attempt count and passes it in, so the kernel performs no backoff math and reads
/// no config (purity). Mirrors the lifecycle the v1.31.0 callbacks component drives
/// (`components/callbacks/statemachine.go @ v1.31.0`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CallbackAttemptOutcome {
    /// Delivery succeeded; the callback becomes terminal `Succeeded`.
    Succeeded,
    /// Delivery failed retryably; the callback backs off until `next_attempt_at`.
    RetryableFailure {
        failure: Payload,
        next_attempt_at: OffsetDateTime,
    },
    /// Delivery failed non-retryably; the callback becomes terminal `Failed`.
    NonRetryableFailure { failure: Payload },
}

/// Record a completion-callback delivery attempt against a (closed) run.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompletionCallbackAttemptedRequest {
    /// Index into the run's `completion_callbacks` (the callback that was attempted).
    pub callback_index: usize,
    /// What the delivery attempt produced.
    pub outcome: CallbackAttemptOutcome,
    /// Wall-clock time of the attempt.
    pub now: OffsetDateTime,
}

/// Three-valued patch for optional fields in update commands.
///
/// Used when a caller may leave a field alone, set it to a new
/// value, or explicitly remove it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum FieldChange<T> {
    /// The field should keep its current value.
    Unchanged,
    /// The field should be replaced with the given value.
    Set(T),
    /// The field should be removed (set to `None` / empty).
    Clear,
}

/// Reason a workflow task was rejected by the server.
///
/// The kernel records this in the `WorkflowTaskFailed` history
/// event so operators and SDKs can diagnose replay failures.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WorkflowTaskFailedCause {
    /// Replay produced a different command sequence than the
    /// recorded history, indicating a code change broke
    /// determinism.
    NonDeterminismError,
    /// The `ScheduleActivity` command carried invalid or
    /// missing attributes.
    BadScheduleActivityAttributes,
    /// The `StartTimer` command carried invalid attributes.
    BadStartTimerAttributes,
    /// The worker returned a command the kernel does not
    /// recognise or cannot process in the current state.
    UnhandledCommand,
    /// The `RequestCancelActivity` command referenced an
    /// invalid activity.
    BadRequestCancelActivityAttributes,
    /// The worker itself reported an unhandled failure during
    /// task processing.
    WorkflowWorkerUnhandledFailure,
    /// The `SignalExternalWorkflowExecution` command carried
    /// invalid attributes.
    BadSignalWorkflowExecutionAttributes,
    /// The workflow task failed because a reset was applied.
    ResetWorkflow,
    /// The started workflow task was force-failed as the first step of a
    /// forced workflow close (terminate / workflow timeout), pinning the
    /// close batch to the resulting `WorkflowTaskFailed` event
    /// (`WORKFLOW_TASK_FAILED_CAUSE_FORCE_CLOSE_COMMAND = 17`,
    /// `failed_cause.proto`; `TerminateWorkflow`,
    /// `service/history/workflow/util.go:115 @ v1.31.0`).
    ForceCloseCommand,
    /// The worker could not ship its WFT response because it exceeded the
    /// gRPC message size limit; `RespondWorkflowTaskFailed` with this cause
    /// terminates the run instead of retrying the task
    /// (`WORKFLOW_TASK_FAILED_CAUSE_GRPC_MESSAGE_TOO_LARGE = 36`,
    /// `failed_cause.proto`;
    /// `service/history/api/respondworkflowtaskfailed/api.go:88 @ v1.31.0`).
    GrpcMessageTooLarge,
}

/// Which timeout fired on a workflow task.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WorkflowTaskTimeoutType {
    /// The worker did not complete the task within the
    /// configured start-to-close deadline.
    StartToClose,
}

/// Which Nexus operation timeout fired.
///
/// The three timeouts are independent deadlines, mirroring v1.31.0's three
/// `hsm.Task`s (`components/nexusoperations/statemachine.go:144-167 @ v1.31.0`):
/// schedule-to-close is anchored at the scheduled time; schedule-to-start is
/// anchored at the scheduled time but only applies while the operation has not
/// yet started; start-to-close is anchored at the started time and only applies
/// once the operation has started. The fired type is carried verbatim into the
/// `NexusOperationTimedOut` event's `TimeoutFailureInfo` so the describe/poll
/// outcome reports the correct timeout type (`executors.go:583-608 @ v1.31.0`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NexusTimeoutType {
    /// Schedule-to-close deadline (scheduled time + timeout) was exceeded.
    ScheduleToClose,
    /// Schedule-to-start deadline (scheduled time + timeout) was exceeded
    /// before the operation started.
    ScheduleToStart,
    /// Start-to-close deadline (started time + timeout) was exceeded after the
    /// operation started.
    StartToClose,
}

/// Which workflow-level timeout fired.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WorkflowTimeoutType {
    /// The overall execution timeout (spans all runs including
    /// continue-as-new chains) was exceeded.
    ExecutionTimeout,
    /// The single-run timeout was exceeded.
    RunTimeout,
}

/// Outcome of the retry decision when a workflow or activity
/// fails or times out.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum RetryState {
    /// The entity is still eligible for retry and will be
    /// rescheduled.
    InProgress,
    /// The failure matched a non-retryable error type in the
    /// retry policy.
    NonRetryableFailure,
    /// The entity timed out and will not be retried.
    Timeout,
    /// The retry policy's maximum attempt count was reached.
    MaximumAttemptsReached,
    /// No retry policy was configured.
    RetryPolicyNotSet,
    /// An internal server error prevented retry evaluation.
    InternalServerError,
    /// A cancellation request arrived, so no further retries
    /// will be attempted.
    CancelRequested,
}

/// What triggered a continue-as-new transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContinueAsNewInitiator {
    Workflow,
    Retry,
    CronSchedule,
}

/// Policy for handling workflow ID conflicts with running workflows.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkflowIdConflictPolicy {
    Fail,
    UseExisting,
    TerminateExisting,
}

/// Policy for handling workflow ID reuse with closed workflows.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkflowIdReusePolicy {
    AllowDuplicate,
    AllowDuplicateFailedOnly,
    RejectDuplicate,
}

/// Request to create a brand-new workflow execution.
///
/// By the time this reaches the kernel, the runtime has
/// already resolved the namespace, assigned a run ID, and
/// performed idempotency checks.
///
/// See `docs/architecture/010-history-as-authority.md`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StartRequest {
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
    /// Task queue where workflow tasks will be dispatched.
    pub task_queue: TaskQueueName,
    /// Arguments passed to the workflow function.
    pub input: Payloads,
    /// Headers supplied on the start request and recorded in the first event.
    pub header: Option<Headers>,
    /// Unindexed key-value metadata attached to the execution.
    pub memo: Memo,
    /// Indexed attributes for visibility queries.
    pub search_attributes: SearchAttributes,
    /// Maximum wall-clock time for the entire execution chain
    /// (including continue-as-new). `None` means no limit.
    pub workflow_execution_timeout: Option<Duration>,
    /// Maximum wall-clock time for a single run. `None` means
    /// no limit.
    pub workflow_run_timeout: Option<Duration>,
    /// Maximum time a worker may hold a workflow task before
    /// the server considers it timed out.
    pub workflow_task_timeout: Duration,
    /// Retry policy governing automatic retries on failure.
    pub retry_policy: Option<RetryPolicy>,
    /// Conflict policy to apply when a running execution already exists.
    pub conflict_policy: WorkflowIdConflictPolicy,
    /// Reuse policy to apply when a closed execution already exists.
    pub reuse_policy: WorkflowIdReusePolicy,
    /// Optional deployment for versioned task routing.
    pub deployment: Option<DeploymentId>,
    /// Optional build identifier for versioned task routing.
    pub build_id: Option<BuildId>,
    /// Versioning override applied at workflow start.
    pub versioning_override: Option<VersioningOverride>,
    /// Delay before the first workflow task may be dispatched.
    pub workflow_start_delay: Option<Duration>,
    /// Completion callbacks registered by the start request.
    pub completion_callbacks: Vec<CompletionCallback>,
    /// UI-facing metadata registered at start.
    pub user_metadata: Option<UserMetadata>,
    /// Links attached to the start event.
    pub links: Vec<Link>,
    /// Options applied when this start resolves to an existing running workflow.
    pub on_conflict_options: Option<OnConflictOptions>,
    /// Matching priority metadata for tasks derived from this workflow.
    pub priority: Option<Priority>,
    /// Current retry attempt number (1-based).
    pub attempt: u32,
    /// Run ID of the previous run if this is a continue-as-new
    /// or retry.
    pub continued_execution_run_id: Option<RunId>,
    /// Run ID of the very first run in the execution chain.
    pub first_execution_run_id: Option<RunId>,
    /// Parent run identity if this start creates a child workflow.
    pub parent_run_key: Option<RunKey>,
    /// Parent workflow ID if this start creates a child workflow.
    pub parent_workflow_id: Option<WorkflowId>,
    /// Parent run ID if this start creates a child workflow.
    pub parent_run_id: Option<RunId>,
    /// Parent namespace if this start creates a child workflow.
    pub parent_namespace_id: Option<NamespaceId>,
    /// Event ID of the parent initiation event for child starts.
    pub parent_initiated_event_id: i64,
    /// Root workflow ID authored onto the start event when the source run has
    /// a parent; absent roots canonicalize to self during apply/replay.
    pub root_workflow_id: Option<WorkflowId>,
    /// Root run ID authored with `root_workflow_id`.
    pub root_run_id: Option<RunId>,
    /// Run ID of the very first run in the execution chain.
    pub original_execution_run_id: Option<RunId>,
    /// Failure carried forward from the predecessor run.
    pub continued_failure: Option<Payload>,
    /// Completion result carried forward from the predecessor run.
    pub last_completion_result: Option<Payloads>,
    /// Wall-clock `started_at` of the very first run in the
    /// execution chain.
    pub first_run_started_at: Option<OffsetDateTime>,
    /// Caller-supplied request context for dedupe and tracing.
    pub request: RequestContext,
    /// Wall-clock time the command was accepted.
    pub now: OffsetDateTime,
    /// Client-authored cron expression used to compute the first WFT backoff.
    ///
    /// This is distinct from `cron_schedule` below because Tokeira's schedule
    /// engine also uses the serialized `cron_schedule` marker for
    /// schedule-originated runs. Only client cron should delay the first WFT.
    pub client_cron_schedule: Option<String>,
    /// Schedule ID that triggered this start, if any. This is serialized as
    /// Temporal's legacy `cron_schedule` marker for schedule-originated runs.
    pub cron_schedule: Option<String>,
    /// Worker identity reserved by the runtime for synchronous first-WFT delivery.
    pub reserved_poller_identity: Option<WorkerIdentity>,
}

/// Request to create a brand-new workflow and immediately deliver a signal.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SignalWithStartRequest {
    pub run_key: RunKey,
    pub namespace_id: NamespaceId,
    pub workflow_id: WorkflowId,
    pub run_id: RunId,
    pub workflow_type: WorkflowType,
    pub task_queue: TaskQueueName,
    pub input: Payloads,
    pub memo: Memo,
    pub search_attributes: SearchAttributes,
    pub workflow_execution_timeout: Option<Duration>,
    pub workflow_run_timeout: Option<Duration>,
    pub workflow_task_timeout: Duration,
    pub retry_policy: Option<RetryPolicy>,
    pub conflict_policy: WorkflowIdConflictPolicy,
    pub reuse_policy: WorkflowIdReusePolicy,
    pub header: Option<Headers>,
    pub deployment: Option<DeploymentId>,
    pub build_id: Option<BuildId>,
    pub versioning_override: Option<VersioningOverride>,
    pub workflow_start_delay: Option<Duration>,
    pub user_metadata: Option<UserMetadata>,
    pub links: Vec<Link>,
    pub priority: Option<Priority>,
    pub cron_schedule: Option<String>,
    pub attempt: u32,
    pub continued_execution_run_id: Option<RunId>,
    pub first_execution_run_id: Option<RunId>,
    pub parent_run_key: Option<RunKey>,
    pub parent_workflow_id: Option<WorkflowId>,
    pub parent_run_id: Option<RunId>,
    pub parent_namespace_id: Option<NamespaceId>,
    pub parent_initiated_event_id: i64,
    pub root_workflow_id: Option<WorkflowId>,
    pub root_run_id: Option<RunId>,
    pub original_execution_run_id: Option<RunId>,
    pub continued_failure: Option<Payload>,
    pub last_completion_result: Option<Payloads>,
    pub first_run_started_at: Option<OffsetDateTime>,
    pub request: RequestContext,
    pub now: OffsetDateTime,
    pub client_cron_schedule: Option<String>,
    pub signal_name: String,
    pub signal_input: Payloads,
}

/// Request to deliver a signal to a running workflow.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SignalRequest {
    /// Name of the signal (matched by the SDK handler).
    pub signal_name: String,
    /// Payload arguments for the signal handler.
    pub input: Payloads,
    /// SDK-authored metadata delivered with the signal.
    pub header: Option<Headers>,
    /// History-event links associated with this signal.
    ///
    /// Temporal stores signal links on the outer `HistoryEvent.links`, not in
    /// `WorkflowExecutionSignaledEventAttributes`
    /// (`service/history/historybuilder/event_factory.go @ v1.31.0`).
    pub links: Vec<Link>,
    /// Caller-supplied request context for dedupe and tracing.
    pub request: RequestContext,
    /// Wall-clock time the command was accepted.
    pub now: OffsetDateTime,
}

/// Request to deliver a workflow update (synchronous mutation).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UpdateRequest {
    /// Caller-assigned identifier for this update instance.
    pub update_id: String,
    /// Name of the update handler on the workflow.
    pub update_name: String,
    /// Payload arguments for the update handler.
    pub input: Payloads,
    /// Caller-supplied request context for dedupe and tracing.
    pub request: RequestContext,
    /// Wall-clock time the command was accepted.
    pub now: OffsetDateTime,
}

/// Identity of an external workflow that initiated a
/// cross-workflow operation (cancel, signal).
///
/// Recorded in history so operators can trace the origin of
/// cross-workflow interactions.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExternalWorkflowExecution {
    /// Namespace of the initiating workflow.
    pub namespace_id: NamespaceId,
    /// Workflow ID of the initiating workflow.
    pub workflow_id: WorkflowId,
    /// Run ID of the initiating workflow.
    pub run_id: RunId,
}

/// Request to cancel a running workflow.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CancelRequest {
    /// Human-readable reason for the cancellation.
    pub reason: String,
    /// If the cancel was initiated by another workflow, its
    /// identity is recorded here for history.
    pub external_initiator: Option<ExternalWorkflowExecution>,
    /// Caller-supplied request context for dedupe and tracing.
    pub request: RequestContext,
    /// Wall-clock time the command was accepted.
    pub now: OffsetDateTime,
}

/// Request to forcibly terminate a workflow without running
/// cancellation logic.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TerminateRequest {
    /// Human-readable reason for the termination.
    pub reason: String,
    /// Optional details payload (e.g. stack trace).
    pub details: Option<Payloads>,
    /// Identity of the caller who issued the termination.
    pub identity: String,
    /// Caller-supplied request context for dedupe and tracing.
    pub request: RequestContext,
    /// Wall-clock time the command was accepted.
    pub now: OffsetDateTime,
}

/// Request to reset a workflow to an earlier point in its
/// history, forking into a new run.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResetRequest {
    /// History event ID to fork from. Events after this point
    /// are discarded in the new run.
    pub fork_event_id: i64,
    /// Pre-assigned run ID for the new (reset) run.
    pub new_run_id: RunId,
    /// Human-readable reason for the reset.
    pub reason: String,
    /// Caller-supplied request context for dedupe and tracing.
    pub request: RequestContext,
    /// Wall-clock time the command was accepted.
    pub now: OffsetDateTime,
}

/// Request to pause a running workflow.
///
/// While paused, no new workflow tasks are dispatched and
/// activity tasks are stamped to invalidate in-flight
/// deliveries.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PauseWorkflowRequest {
    /// Identity of the caller who issued the pause.
    pub identity: String,
    /// Human-readable reason for the pause.
    pub reason: String,
    /// Caller-supplied request context for dedupe and tracing.
    pub request: RequestContext,
    /// Wall-clock time the command was accepted.
    pub now: OffsetDateTime,
}

/// Request to resume a paused workflow.
///
/// Re-dispatches all pending activity tasks and schedules a
/// new workflow task if none is pending.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UnpauseWorkflowRequest {
    /// Identity of the caller who issued the unpause.
    pub identity: String,
    /// Human-readable reason for the unpause.
    pub reason: String,
    /// Caller-supplied request context for dedupe and tracing.
    pub request: RequestContext,
    /// Wall-clock time the command was accepted.
    pub now: OffsetDateTime,
}

/// Request to update timeout and routing options on a
/// pending activity without canceling it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UpdateActivityOptionsRequest {
    /// Activity to update (must be in the open set).
    pub activity_id: String,
    /// New task queue, if changing.
    pub task_queue: FieldChange<TaskQueueName>,
    /// New schedule-to-close timeout, if changing.
    pub schedule_to_close_timeout: FieldChange<Option<Duration>>,
    /// New schedule-to-start timeout, if changing.
    pub schedule_to_start_timeout: FieldChange<Option<Duration>>,
    /// New start-to-close timeout, if changing.
    pub start_to_close_timeout: FieldChange<Option<Duration>>,
    /// New heartbeat timeout, if changing.
    pub heartbeat_timeout: FieldChange<Option<Duration>>,
    /// Caller-supplied request context for dedupe and tracing.
    pub request: RequestContext,
    /// Wall-clock time the command was accepted.
    pub now: OffsetDateTime,
}

/// Request to pause a specific activity. Paused activities
/// are not dispatched until explicitly unpaused.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PauseActivityRequest {
    /// Activity to pause (must be in the open set).
    pub activity_id: String,
    /// Identity of the caller who issued the pause.
    pub identity: String,
    /// Human-readable reason for the pause.
    pub reason: String,
    /// Caller-supplied request context for dedupe and tracing.
    pub request: RequestContext,
    /// Wall-clock time the command was accepted.
    pub now: OffsetDateTime,
}

/// Request to resume a paused activity and re-dispatch it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UnpauseActivityRequest {
    /// Activity to unpause (must be paused).
    pub activity_id: String,
    /// Caller-supplied request context for dedupe and tracing.
    pub request: RequestContext,
    /// Wall-clock time the command was accepted.
    pub now: OffsetDateTime,
}

/// Request to reset a pending activity back to attempt 1
/// and re-dispatch it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResetActivityRequest {
    /// Activity to reset (must be in the open set).
    pub activity_id: String,
    /// Whether to also clear heartbeat progress.
    pub reset_heartbeat: bool,
    /// Caller-supplied request context for dedupe and tracing.
    pub request: RequestContext,
    /// Wall-clock time the command was accepted.
    pub now: OffsetDateTime,
}

/// Request to update execution-level options such as
/// versioning overrides and completion callbacks.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UpdateExecutionOptionsRequest {
    /// Versioning override change.
    pub versioning_override: FieldChange<VersioningOverride>,
    /// Completion callbacks change.
    pub completion_callbacks: FieldChange<Vec<CompletionCallback>>,
    /// Completion callbacks to append without replacing existing registrations.
    pub attached_completion_callbacks: Vec<CompletionCallback>,
    /// Links to append without replacing existing start/event links.
    pub attached_links: Vec<Link>,
    /// Optional request ID to attach for correlation.
    pub attached_request_id: Option<String>,
    /// Caller-supplied request context for dedupe and tracing.
    pub request: RequestContext,
    /// Wall-clock time the command was accepted.
    pub now: OffsetDateTime,
}

/// Request from the runtime indicating a worker has picked
/// up a workflow task.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StartWorkflowTaskRequest {
    /// Logical task sequence that must match the pending WFT.
    pub logical_seq: tokeira_types::LogicalTaskSeq,
    /// Identity of the worker that started the task.
    pub worker_identity: WorkerIdentity,
    /// Runtime-generated idempotency key for this task start.
    pub request_id: String,
    /// Approximate persisted history size at task start time.
    pub history_size_bytes: i64,
    /// Whether the runtime suggests continue-as-new soon.
    pub suggest_continue_as_new: bool,
    /// If set, the worker's deployment version differs from the run's effective version.
    pub deployment_transition: Option<WorkerDeploymentVersionRef>,
    /// Routing revision attached to the deployment transition decision.
    pub deployment_transition_revision_number: Option<i64>,
    /// If set, the worker requests sticky execution affinity
    /// for this duration.
    pub sticky_ttl: Option<Duration>,
    /// Wall-clock time the command was accepted.
    pub now: OffsetDateTime,
}

/// Request to apply an already-decided Worker Deployment transition.
///
/// The runtime computes whether an activity-start poller should move the
/// workflow toward a different deployment by reading live routing config. The
/// kernel only receives the resulting target and revision, then performs the
/// authoritative state mutation in one fenced transition. This keeps the
/// activity-start path aligned with Temporal's mutable-state contract without
/// letting runtime code mutate per-run correctness state directly
/// (`service/history/api/recordactivitytaskstarted/api.go:75 @ v1.31.0`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StartDeploymentTransitionRequest {
    /// Target Worker Deployment Version chosen by runtime routing.
    pub target: WorkerDeploymentVersionRef,
    /// Routing revision that justified this transition.
    ///
    /// This is the activity dispatch revision, not a freshly-loaded registry
    /// revision. Temporal uses the dispatch-time revision to distinguish a
    /// non-backlogged activity from one that is ahead of the workflow-task
    /// target (`service/history/api/recordactivitytaskstarted/api.go:188 @
    /// v1.31.0`).
    pub revision_number: i64,
    /// Wall-clock time the command was accepted.
    pub now: OffsetDateTime,
}

/// Runtime-supplied cron continue-as-new successor metadata.
///
/// Cron parsing and wall-clock calendar math stay in runtime. The kernel only
/// records the already-decided successor identity and first-WFT delay in
/// history, matching Temporal's cron continuation handoff
/// (`service/history/api/respondworkflowtaskcompleted/workflow_task_completed_handler.go:1383 @ v1.31.0`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CronContinuation {
    /// Run ID authored into the closing run's continue-as-new event.
    pub new_run_id: RunId,
    /// Concrete delay before the successor's first workflow task may fire.
    pub first_workflow_task_backoff: Duration,
    /// Original workflow input to feed into the cron successor.
    pub input: Payloads,
    /// Client-authored cron expression preserved on the successor start event.
    pub cron_schedule: String,
}

/// Runtime-supplied workflow retry decision for a failing run.
///
/// The retry *evaluation* — non-retryable error-type match, maximum attempts,
/// execution-expiration cap, and backoff magnitude — is wall-clock- and
/// policy-dependent and stays in the runtime (`service/history/workflow/retry.go`
/// @ v1.31.0), mirroring how [`CronContinuation`] keeps calendar math out of the
/// kernel. The kernel only records the outcome on `WorkflowExecutionFailed`:
/// either the successor run id (retry continues) or the terminal `RetryState`.
///
/// The first-workflow-task backoff is deliberately absent. Unlike cron's
/// calendar delay, the retry backoff is a pure function of the policy and the
/// predecessor attempt (`initial × coefficient^(attempt-1)`, capped by
/// `maximum_interval`); the runtime recomputes it deterministically when it
/// starts the successor, so it need not be threaded through the kernel.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum RetryContinuation {
    /// The retry policy continues the chain. `new_run_id` is authored into the
    /// failed run's `new_execution_run_id` and becomes the successor run.
    Retry {
        /// Run ID minted by the runtime for the attempt-N+1 successor.
        new_run_id: RunId,
    },
    /// The chain ends. `retry_state` is the reason the run will not retry
    /// (`MaximumAttemptsReached`, `NonRetryableFailure`, or `RetryPolicyNotSet`).
    Terminal {
        /// Terminal retry outcome recorded on the failed event.
        retry_state: RetryState,
    },
}

/// Request from a worker that has finished processing a
/// workflow task and is returning a batch of commands.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkflowTaskCompletedRequest {
    /// Token that proves the worker held the correct task.
    pub token: WorkflowTaskToken,
    /// Identity of the completing worker.
    pub identity: WorkerIdentity,
    /// Raw encoded Temporal SDK metadata from the completion request.
    #[serde(default)]
    pub sdk_metadata: Option<Vec<u8>>,
    /// Raw encoded Temporal metering metadata from the completion request.
    #[serde(default)]
    pub metering_metadata: Option<Vec<u8>>,
    /// Build ID from the worker version stamp, when reported by the SDK.
    #[serde(default)]
    pub worker_version: Option<String>,
    /// Versioning behavior reported by the worker that completed this task.
    #[serde(default)]
    pub versioning_behavior: VersioningBehavior,
    /// Worker Deployment Version that completed this task.
    #[serde(default)]
    pub deployment_version: Option<WorkerDeploymentVersionRef>,
    /// Worker Deployment name that completed this task.
    #[serde(default)]
    pub worker_deployment_name: Option<String>,
    /// Sticky execution TTL requested by the completing worker.
    ///
    /// Temporal's wire field names a sticky task queue, but Tokeira's current
    /// matching model keys sticky delivery by the completing worker identity.
    /// The TTL is the durable compatibility signal needed to update that
    /// existing affinity without introducing worker-lifecycle semantics here.
    #[serde(default)]
    pub sticky_ttl: Option<Duration>,
    /// Ordered list of workflow commands produced by the
    /// worker's replay/execution.
    pub commands: Vec<WorkflowCommand>,
    /// If true, the kernel schedules a new WFT even when no
    /// commands require one.
    pub force_new_workflow_task: bool,
    /// Wall-clock time the command was accepted.
    pub now: OffsetDateTime,
}

/// Request from the runtime when a workflow task fails
/// (non-determinism, bad commands, or reset).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkflowTaskFailedRequest {
    /// Logical task sequence of the failed task.
    pub logical_seq: LogicalTaskSeq,
    /// Event ID of the `WorkflowTaskStarted` event.
    pub started_event_id: i64,
    /// Why the task failed.
    pub failure_cause: WorkflowTaskFailedCause,
    /// Optional diagnostic payload (e.g. stack trace).
    pub failure_details: Option<Payload>,
    /// Identity of the worker that reported the failure.
    pub worker_identity: WorkerIdentity,
    /// Wall-clock time the command was accepted.
    pub now: OffsetDateTime,
}

/// Request from the runtime when `RespondWorkflowTaskFailed` arrives with
/// cause `GRPC_MESSAGE_TOO_LARGE`: instead of the normal WFT retry path, the
/// run is force-close-terminated — the started WFT is failed with cause
/// `ForceCloseCommand`, buffered events flush, and the run terminates
/// (`respondworkflowtaskfailed/api.go:88` + `workflow/util.go:115 @ v1.31.0`;
/// spec: `kernel-event-buffering` Req 4.2). Carries the same WFT fencing as
/// [`WorkflowTaskFailedRequest`] so a stale task token is rejected.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TerminateOnWorkflowTaskFailedRequest {
    /// Logical task sequence of the started task being force-closed.
    pub logical_seq: LogicalTaskSeq,
    /// Event ID of the `WorkflowTaskStarted` event.
    pub started_event_id: i64,
    /// Terminate reason recorded on `WorkflowExecutionTerminated`. v1.31.0
    /// passes `request.GetCause().String()`, which renders as
    /// `"GrpcMessageTooLarge"` (`WorkflowTaskFailedCause::String`,
    /// `enums/v1/failed_cause.pb.go @ go.temporal.io/api v1.62.8`).
    pub reason: String,
    /// Identity recorded on the terminated event; v1.31.0 uses the internal
    /// history-service identity (`consts.IdentityHistoryService =
    /// "history-service"`, `service/history/consts/const.go:13 @ v1.31.0`).
    pub identity: String,
    /// Caller-supplied request context for dedupe and tracing.
    pub request: RequestContext,
    /// Wall-clock time the command was accepted.
    pub now: OffsetDateTime,
}

/// Request from the runtime when a workflow task exceeds its
/// start-to-close timeout.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkflowTaskTimedOutRequest {
    /// Logical task sequence of the timed-out task.
    pub logical_seq: LogicalTaskSeq,
    /// Event ID of the `WorkflowTaskStarted` event.
    pub started_event_id: i64,
    /// Which timeout fired.
    pub timeout_type: WorkflowTaskTimeoutType,
    /// Wall-clock time the command was accepted.
    pub now: OffsetDateTime,
}

/// Request from the runtime when a workflow-level timeout
/// fires (execution or run timeout).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkflowExecutionTimedOutRequest {
    /// Which workflow-level timeout fired.
    pub timeout_type: WorkflowTimeoutType,
    /// Retry decision outcome.
    pub retry_state: RetryState,
    /// Wall-clock time the command was accepted.
    pub now: OffsetDateTime,
}

/// Request from the runtime when an activity task reaches a
/// terminal state (completed, failed, timed out, or
/// canceled).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ActivityResolvedRequest {
    /// Activity that was resolved.
    pub activity_id: String,
    /// How the activity was resolved.
    pub resolution: ActivityResolution,
    /// Identity of the worker that resolved the activity, if
    /// applicable.
    pub worker_identity: Option<WorkerIdentity>,
    /// Wall-clock time the command was accepted.
    pub now: OffsetDateTime,
}

/// Request from the runtime confirming whether a child
/// workflow was successfully started or failed to start.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ChildStartConfirmedRequest {
    /// Workflow ID of the child.
    pub child_workflow_id: WorkflowId,
    /// Event ID of the `StartChildWorkflowExecutionInitiated`
    /// event (used for staleness checks).
    pub initiated_event_id: i64,
    /// Whether the child started or failed.
    pub result: ChildStartResult,
    /// Wall-clock time the command was accepted.
    pub now: OffsetDateTime,
}

/// Outcome of a child workflow start attempt.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ChildStartResult {
    /// The child was successfully created and assigned a run.
    Started {
        child_run_id: RunId,
        workflow_type: WorkflowType,
    },
    /// The child could not be started (e.g. already exists).
    Failed { cause: String },
}

/// Request from the runtime when a child workflow reaches a
/// terminal state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ChildResolvedRequest {
    /// Workflow ID of the child.
    pub child_workflow_id: WorkflowId,
    /// How the child was resolved.
    pub resolution: ChildResolution,
    /// Wall-clock time the command was accepted.
    pub now: OffsetDateTime,
}

/// Terminal state of a child workflow execution.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ChildResolution {
    /// The child completed successfully.
    Completed { result: Payloads },
    /// The child failed with an application error.
    Failed { failure: Payload },
    /// The child was canceled.
    Canceled,
    /// The child was forcibly terminated.
    Terminated,
    /// The child exceeded its timeout.
    TimedOut,
}

/// Request from the runtime when an external signal delivery
/// completes or fails.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExternalSignalResolvedRequest {
    /// Event ID of the initiation event (used for lookup).
    pub initiated_event_id: i64,
    /// Whether the signal was delivered or failed.
    pub result: ExternalSignalResult,
    /// Wall-clock time the command was accepted.
    pub now: OffsetDateTime,
}

/// Outcome of an external signal delivery attempt.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ExternalSignalResult {
    /// The signal was successfully delivered.
    Signaled,
    /// The signal delivery failed.
    Failed { cause: String },
}

/// Request from the runtime when an external cancel delivery
/// completes or fails.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExternalCancelResolvedRequest {
    /// Event ID of the initiation event (used for lookup).
    pub initiated_event_id: i64,
    /// Whether the cancel was delivered or failed.
    pub result: ExternalCancelResult,
    /// Wall-clock time the command was accepted.
    pub now: OffsetDateTime,
}

/// Outcome of an external cancel delivery attempt.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ExternalCancelResult {
    /// The cancel request was successfully delivered.
    CancelRequested,
    /// The cancel delivery failed.
    Failed { cause: String },
}

/// Terminal state of a Nexus operation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum NexusResolution {
    /// The operation transitioned to async-started.
    Started {
        /// Handler-issued token for the running async operation, surfaced to the
        /// caller as `NexusOperationExecution.OperationToken` and recorded on the
        /// `NexusOperationStarted` event (`operation_token` field 5 @ v1.31.0).
        operation_token: String,
        /// Links the handler returned on the start response.
        links: Vec<Link>,
    },
    /// The operation completed successfully.
    Completed {
        result: Payloads,
        /// Links the handler returned on the completion response.
        links: Vec<Link>,
    },
    /// The operation failed terminally (non-retryable handler error, operation
    /// error, or schedule-to-close exhausted). `failure` is the caller-facing
    /// `NexusOperationFailureInfo`-wrapped chain recorded on `NexusOperationFailed`.
    Failed { failure: Payload },
    /// A retryable invocation attempt failed; the operation stays pending and backs
    /// off. Mirrors v1.31.0's `EventAttemptFailed` (`SCHEDULED → BACKING_OFF`,
    /// `components/nexusoperations/statemachine.go:268-289`): records the attempt
    /// failure and the next-attempt time, emits **no** history event, and schedules
    /// **no** workflow task. `failure` is the handler's own failure
    /// (NexusHandlerFailureInfo + cause), surfaced on
    /// `PendingNexusOperations[].LastAttemptFailure` — NOT the terminal
    /// `NexusOperationFailureInfo` wrapper. `next_attempt_at` is the runtime-computed
    /// backoff deadline (the kernel does no backoff math).
    AttemptFailed {
        failure: Payload,
        next_attempt_at: OffsetDateTime,
    },
    /// The operation was canceled.
    Canceled,
    /// The operation exceeded one of its timeouts.
    TimedOut {
        /// Which of the three Nexus timeouts fired.
        timeout_type: NexusTimeoutType,
    },
}

/// Request from the runtime when a Nexus operation reaches a
/// terminal or started state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NexusOperationResolvedRequest {
    /// Operation ID of the Nexus operation.
    pub operation_id: String,
    /// Event ID of the `NexusOperationScheduled` event (used
    /// for staleness checks).
    pub scheduled_event_id: i64,
    /// How the operation was resolved.
    pub resolution: NexusResolution,
    /// Wall-clock time the command was accepted.
    pub now: OffsetDateTime,
}

/// Request from the retry scanner when a backing-off Nexus operation's
/// `next_attempt_at` has arrived. The kernel re-dispatches the operation
/// (a fresh StartOperation attempt) and clears the backoff state, so long as
/// the op is still backing off and the scheduled-event fence matches.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NexusOperationRetryRequest {
    /// Operation ID of the backing-off Nexus operation.
    pub operation_id: String,
    /// Event ID of the `NexusOperationScheduled` event — the fencing key (a
    /// stale re-dispatch from a superseded attempt is rejected).
    pub scheduled_event_id: i64,
    /// Wall-clock time the command was accepted.
    pub now: OffsetDateTime,
}

/// Body of an update protocol message carried inside a
/// `WorkflowCommand::ProtocolMessage`.
///
/// Updates span two transitions (acceptance and completion),
/// so the protocol message can carry any of the three phases.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum UpdateProtocolBody {
    /// The worker accepted the update.
    Accepted {
        update_id: String,
        update_name: String,
        input: Payloads,
    },
    /// The worker completed the update with a result.
    Completed { update_id: String, result: Payloads },
    /// The worker rejected the update.
    Rejected { update_id: String, failure: Payload },
}

/// Request from the timer scanner when a timer's deadline
/// has passed.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TimerDueRequest {
    /// Timer that fired.
    pub timer_id: String,
    /// Wall-clock time the timer was observed as due.
    pub fired_at: OffsetDateTime,
}

/// Request from the timer scanner when a delayed workflow start is ready
/// to publish its first workflow task.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkflowStartDelayElapsedRequest {
    /// Wall-clock time the delayed start was observed as due.
    pub fired_at: OffsetDateTime,
}

/// Request from the runtime to schedule a WFT so a pending
/// query can be piggybacked on the worker's next poll.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScheduleQueryTaskRequest {
    pub now: OffsetDateTime,
}

/// Commands produced by workflow code when a workflow task completes.
///
/// TODO(correctness): add child workflows, updates, versioning markers, local
/// activities, cancellation scopes, patch markers, and continue-as-new.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WorkflowCommand {
    /// Schedule an activity task for execution on a worker.
    ScheduleActivity {
        activity_id: String,
        activity_type: String,
        task_queue: TaskQueueName,
        input: Payloads,
        header: Option<Headers>,
        request_eager_execution: bool,
        retry_policy: Option<RetryPolicy>,
        deployment: Option<DeploymentId>,
        build_id: Option<BuildId>,
        schedule_to_close_timeout: Option<Duration>,
        schedule_to_start_timeout: Option<Duration>,
        start_to_close_timeout: Option<Duration>,
        heartbeat_timeout: Option<Duration>,
    },
    /// Start a durable timer that fires at the given time.
    StartTimer {
        timer_id: String,
        fire_at: OffsetDateTime,
    },
    /// Replace the workflow's memo (unindexed metadata).
    UpsertMemo(Memo),
    /// Replace the workflow's indexed search attributes.
    UpsertSearchAttributes(SearchAttributes),
    /// Record an opaque marker in history. Used by SDKs for
    /// side-effect replay, version gates, and local
    /// activities.
    RecordMarker {
        marker_name: String,
        details: std::collections::BTreeMap<String, Payloads>,
        failure: Option<Payload>,
        header: Option<std::collections::BTreeMap<String, Payload>>,
    },
    /// Complete the workflow with a successful result. Closes
    /// the run.
    CompleteWorkflow { result: Payloads },
    /// Fail the workflow with an application error. Closes
    /// the run.
    FailWorkflow { failure: Payload },
    /// Close the current run and start a new one with fresh
    /// parameters (continue-as-new pattern).
    ContinueAsNew {
        new_run_id: RunId,
        workflow_type: WorkflowType,
        task_queue: TaskQueueName,
        input: Payloads,
        memo: Memo,
        search_attributes: SearchAttributes,
        workflow_execution_timeout: Option<Duration>,
        workflow_run_timeout: Option<Duration>,
        workflow_task_timeout: Duration,
        retry_policy: Option<RetryPolicy>,
    },
    /// Cancel the workflow (cooperative cancellation
    /// completed). Closes the run.
    CancelWorkflow,
    /// Request cancellation of a pending activity. The
    /// activity remains open until resolved.
    RequestCancelActivity { activity_id: String },
    /// Cancel a pending timer before it fires.
    CancelTimer { timer_id: String },
    /// Initiate a child workflow execution.
    StartChildWorkflow {
        child_workflow_id: WorkflowId,
        namespace_id: NamespaceId,
        namespace: Option<String>,
        workflow_type: WorkflowType,
        task_queue: TaskQueueName,
        input: Payloads,
        header: Option<Headers>,
        memo: Memo,
        search_attributes: SearchAttributes,
        workflow_execution_timeout: Option<Duration>,
        workflow_run_timeout: Option<Duration>,
        workflow_task_timeout: Duration,
        retry_policy: Option<RetryPolicy>,
        cron_schedule: Option<String>,
        parent_close_policy: ParentClosePolicy,
    },
    /// Send a signal to an external workflow.
    SignalExternalWorkflowExecution {
        target_namespace_id: NamespaceId,
        target_namespace: Option<String>,
        target_workflow_id: WorkflowId,
        target_run_id: Option<RunId>,
        signal_name: String,
        input: Payloads,
        header: Option<Headers>,
        control: String,
    },
    /// Request cancellation of an external workflow.
    RequestCancelExternalWorkflowExecution {
        target_namespace_id: NamespaceId,
        target_namespace: Option<String>,
        target_workflow_id: WorkflowId,
        target_run_id: Option<RunId>,
        control: String,
    },
    /// Schedule a Nexus operation.
    ScheduleNexusOperation {
        operation_id: String,
        endpoint: String,
        service: String,
        operation: String,
        input: Payloads,
        schedule_to_close_timeout: Option<Duration>,
        /// Caller's tolerance for the handler to start (or synchronously
        /// complete) the operation; only meaningful while not yet started.
        /// Wire field 7, added in v1.31.0
        /// (`command/v1/message.proto` → `ScheduleNexusOperationCommandAttributes`).
        schedule_to_start_timeout: Option<Duration>,
        /// Caller's tolerance for an async operation to complete after it has
        /// started. Wire field 8, added in v1.31.0.
        start_to_close_timeout: Option<Duration>,
    },
    /// Cancel a pending Nexus operation.
    CancelNexusOperation { scheduled_event_id: i64 },
    /// Complete a pending workflow update with a result.
    UpdateCompleted { update_id: String, result: Payloads },
    /// Reject a pending workflow update.
    UpdateRejected { update_id: String, failure: Payload },
    /// Carry an update protocol message (accept, complete,
    /// or reject) identified by a message ID.
    ProtocolMessage {
        message_id: String,
        body: UpdateProtocolBody,
    },
    /// Explicitly request a new workflow task even when no
    /// other command requires one.
    RequestNewWorkflowTask,
}
