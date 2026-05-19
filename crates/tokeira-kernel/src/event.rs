use serde::{Deserialize, Serialize};
use time::{Duration, OffsetDateTime};
use tokeira_types::{
    ExecutionStatus, Headers, LogicalTaskSeq, Memo, NamespaceId, Payload, Payloads, RetryPolicy,
    RunId, SearchAttributes, TaskQueueName, WorkerIdentity, WorkflowId, WorkflowType,
};

use crate::{
    command::{
        ContinueAsNewInitiator, ExternalWorkflowExecution, FieldChange, RetryState,
        WorkflowTaskFailedCause, WorkflowTaskTimeoutType, WorkflowTimeoutType,
    },
    state::{CompletionCallback, ParentClosePolicy, VersioningOverride},
};

/// Authoritative history event.
///
/// The exact storage encoding may change, but the semantic shape matters. Event
/// IDs are client-observable and should remain stable within a run.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HistoryEvent {
    /// Monotonically increasing event ID within this run.
    /// Contiguous and never reused.
    pub event_id: i64,
    /// Wall-clock time the event was recorded.
    pub happened_at: OffsetDateTime,
    /// Semantic content of the event.
    pub kind: HistoryEventKind,
}

/// Discriminant for the semantic content of a history event.
///
/// Each variant corresponds to one observable fact in the
/// workflow's history. The variant set is intentionally
/// aligned with the Temporal history event taxonomy so that
/// SDK replay can consume Tokeira histories without
/// translation.
///
/// See `docs/architecture/020-kernel.md` for the full command
/// taxonomy and which commands produce which events.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum HistoryEventKind {
    /// The workflow run was created. This is always the first
    /// event in a run's history.
    WorkflowExecutionStarted {
        workflow_type: WorkflowType,
        task_queue: TaskQueueName,
        input: Payloads,
        header: Option<Headers>,
        memo: Memo,
        search_attributes: SearchAttributes,
        request_id: String,
        identity: String,
        continued_execution_run_id: Option<RunId>,
        first_execution_run_id: Option<RunId>,
        retry_policy: Option<RetryPolicy>,
        attempt: u32,
        workflow_execution_timeout: Option<Duration>,
        workflow_run_timeout: Option<Duration>,
        workflow_task_timeout: Duration,
        parent_workflow_id: Option<WorkflowId>,
        parent_run_id: Option<RunId>,
        parent_namespace_id: Option<NamespaceId>,
        parent_initiated_event_id: i64,
        original_execution_run_id: Option<RunId>,
        continued_failure: Option<Payload>,
        last_completion_result: Option<Payloads>,
        cron_schedule: Option<String>,
    },
    /// An external signal was delivered to the workflow.
    WorkflowExecutionSignaled {
        signal_name: String,
        input: Payloads,
        header: Option<Headers>,
        request_id: String,
        identity: Option<String>,
    },
    /// A cancellation was requested (cooperative — the
    /// workflow code decides whether to honour it).
    WorkflowExecutionCancelRequested {
        reason: String,
        external_workflow_execution: Option<ExternalWorkflowExecution>,
        external_initiated_event_id: i64,
        identity: String,
        request_id: String,
    },
    /// The workflow was paused by an operator or API call.
    WorkflowExecutionPaused {
        identity: String,
        reason: String,
        request_id: String,
    },
    /// The workflow was resumed after a pause.
    WorkflowExecutionUnpaused {
        identity: String,
        reason: String,
        request_id: String,
    },
    /// The workflow was forcibly terminated without running
    /// cancellation logic.
    WorkflowExecutionTerminated {
        reason: String,
        details: Option<Payloads>,
        identity: String,
    },
    /// The workflow exceeded its execution or run timeout.
    WorkflowExecutionTimedOut {
        timeout_type: WorkflowTimeoutType,
        retry_state: RetryState,
        new_execution_run_id: Option<RunId>,
    },
    /// A workflow task was placed on the task queue. The
    /// kernel maintains at most one pending WFT per run.
    WorkflowTaskScheduled {
        logical_seq: LogicalTaskSeq,
        task_queue: TaskQueueName,
        workflow_task_timeout: Duration,
        attempt: u32,
    },
    /// A worker picked up the workflow task and began
    /// processing it.
    WorkflowTaskStarted {
        logical_seq: LogicalTaskSeq,
        scheduled_event_id: i64,
        attempt: u32,
        identity: WorkerIdentity,
        request_id: String,
        history_size_bytes: i64,
        suggest_continue_as_new: bool,
    },
    /// The worker successfully completed the workflow task
    /// and returned a batch of workflow commands.
    WorkflowTaskCompleted {
        logical_seq: LogicalTaskSeq,
        scheduled_event_id: i64,
        started_event_id: i64,
        identity: WorkerIdentity,
        sdk_metadata: Option<Vec<u8>>,
        worker_version: Option<String>,
    },
    /// The workflow task failed (e.g. non-determinism error,
    /// bad command attributes, or a reset).
    WorkflowTaskFailed {
        logical_seq: LogicalTaskSeq,
        scheduled_event_id: i64,
        started_event_id: i64,
        failure_cause: WorkflowTaskFailedCause,
        failure_details: Option<Payload>,
        identity: WorkerIdentity,
        base_run_id: Option<RunId>,
        new_run_id: Option<RunId>,
        fork_event_version: Option<i64>,
        fork_event_id: Option<i64>,
    },
    /// The workflow task exceeded its start-to-close timeout
    /// without the worker responding.
    WorkflowTaskTimedOut {
        logical_seq: LogicalTaskSeq,
        scheduled_event_id: i64,
        started_event_id: i64,
        timeout_type: WorkflowTaskTimeoutType,
    },
    /// An activity task was scheduled by a workflow command.
    ActivityTaskScheduled {
        workflow_task_completed_event_id: i64,
        activity_id: String,
        activity_type: String,
        task_queue: TaskQueueName,
        input: Payloads,
        header: Option<Headers>,
        retry_policy: Option<RetryPolicy>,
        schedule_to_close_timeout: Option<Duration>,
        schedule_to_start_timeout: Option<Duration>,
        start_to_close_timeout: Option<Duration>,
        heartbeat_timeout: Option<Duration>,
    },
    /// A worker picked up the activity task and began
    /// processing it.
    ActivityTaskStarted {
        activity_id: String,
        scheduled_event_id: i64,
        attempt: u32,
        identity: WorkerIdentity,
        request_id: String,
        last_failure: Option<Payload>,
    },
    /// The activity completed successfully with a result.
    ActivityTaskCompleted {
        activity_id: String,
        scheduled_event_id: i64,
        started_event_id: i64,
        identity: Option<WorkerIdentity>,
        result: Payloads,
    },
    /// The activity failed with an application-level error.
    ActivityTaskFailed {
        activity_id: String,
        scheduled_event_id: i64,
        started_event_id: i64,
        identity: Option<WorkerIdentity>,
        retry_state: RetryState,
        failure: Payload,
    },
    /// The activity exceeded one of its configured timeouts.
    ActivityTaskTimedOut {
        activity_id: String,
        scheduled_event_id: i64,
        started_event_id: i64,
        timeout_type: String,
        retry_state: RetryState,
    },
    /// The activity was canceled (cooperative cancellation).
    ActivityTaskCanceled {
        activity_id: String,
        scheduled_event_id: i64,
        started_event_id: i64,
        identity: Option<WorkerIdentity>,
        details: Option<Payloads>,
    },
    /// A timer was started by a workflow command.
    TimerStarted {
        workflow_task_completed_event_id: i64,
        timer_id: String,
        fire_at: OffsetDateTime,
    },
    /// A side-effect marker was recorded in history. Markers
    /// are opaque to the kernel; SDKs use them for
    /// side-effect replay, version gates, and local
    /// activities.
    MarkerRecorded {
        workflow_task_completed_event_id: i64,
        marker_name: String,
        details: std::collections::BTreeMap<String, Payloads>,
        failure: Option<Payload>,
        header: Option<std::collections::BTreeMap<String, Payload>>,
    },
    /// A timer was canceled before it fired.
    TimerCanceled {
        workflow_task_completed_event_id: i64,
        timer_id: String,
        started_event_id: i64,
    },
    /// A timer's deadline was reached and it fired.
    TimerFired {
        timer_id: String,
        started_event_id: i64,
    },
    /// A cancel was requested for a pending activity.
    ActivityTaskCancelRequested {
        workflow_task_completed_event_id: i64,
        activity_id: String,
        scheduled_event_id: i64,
    },
    /// A child workflow start was requested by the parent.
    StartChildWorkflowExecutionInitiated {
        workflow_task_completed_event_id: i64,
        child_workflow_id: WorkflowId,
        workflow_type: WorkflowType,
        task_queue: TaskQueueName,
        input: Payloads,
        namespace_id: NamespaceId,
        namespace: Option<String>,
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
    /// The child workflow was successfully started and
    /// assigned a run ID.
    ChildWorkflowExecutionStarted {
        child_workflow_id: WorkflowId,
        child_run_id: RunId,
        workflow_type: WorkflowType,
        initiated_event_id: i64,
    },
    /// The child workflow could not be started (e.g. already
    /// exists).
    StartChildWorkflowExecutionFailed {
        child_workflow_id: WorkflowId,
        initiated_event_id: i64,
        namespace_id: NamespaceId,
        namespace: Option<String>,
        workflow_type: WorkflowType,
        cause: String,
    },
    /// The child workflow completed successfully.
    ChildWorkflowExecutionCompleted {
        child_workflow_id: WorkflowId,
        namespace_id: NamespaceId,
        namespace: Option<String>,
        child_run_id: Option<RunId>,
        workflow_type: WorkflowType,
        result: Payloads,
        initiated_event_id: i64,
        started_event_id: i64,
    },
    /// The child workflow failed with an application error.
    ChildWorkflowExecutionFailed {
        child_workflow_id: WorkflowId,
        namespace_id: NamespaceId,
        namespace: Option<String>,
        child_run_id: Option<RunId>,
        workflow_type: WorkflowType,
        retry_state: RetryState,
        failure: Payload,
        initiated_event_id: i64,
        started_event_id: i64,
    },
    /// The child workflow was canceled.
    ChildWorkflowExecutionCanceled {
        child_workflow_id: WorkflowId,
        namespace_id: NamespaceId,
        namespace: Option<String>,
        child_run_id: Option<RunId>,
        workflow_type: WorkflowType,
        details: Option<Payloads>,
        initiated_event_id: i64,
        started_event_id: i64,
    },
    /// The child workflow was forcibly terminated.
    ChildWorkflowExecutionTerminated {
        child_workflow_id: WorkflowId,
        namespace_id: NamespaceId,
        namespace: Option<String>,
        workflow_type: WorkflowType,
        initiated_event_id: i64,
        started_event_id: i64,
    },
    /// The child workflow exceeded its timeout.
    ChildWorkflowExecutionTimedOut {
        child_workflow_id: WorkflowId,
        namespace_id: NamespaceId,
        namespace: Option<String>,
        workflow_type: WorkflowType,
        retry_state: RetryState,
        initiated_event_id: i64,
        started_event_id: i64,
    },
    /// A signal to an external workflow was initiated.
    SignalExternalWorkflowExecutionInitiated {
        workflow_task_completed_event_id: i64,
        namespace_id: NamespaceId,
        namespace: Option<String>,
        target_workflow_id: WorkflowId,
        target_run_id: Option<RunId>,
        signal_name: String,
        input: Payloads,
        header: Option<Headers>,
        control: String,
    },
    /// The external signal was successfully delivered.
    ExternalWorkflowExecutionSignaled {
        initiated_event_id: i64,
        namespace_id: NamespaceId,
        namespace: Option<String>,
        target_workflow_id: WorkflowId,
        target_run_id: Option<RunId>,
    },
    /// The external signal delivery failed.
    SignalExternalWorkflowExecutionFailed {
        initiated_event_id: i64,
        namespace_id: NamespaceId,
        namespace: Option<String>,
        target_workflow_id: WorkflowId,
        target_run_id: Option<RunId>,
        cause: String,
    },
    /// A cancel request to an external workflow was initiated.
    RequestCancelExternalWorkflowExecutionInitiated {
        workflow_task_completed_event_id: i64,
        namespace_id: NamespaceId,
        namespace: Option<String>,
        target_workflow_id: WorkflowId,
        target_run_id: Option<RunId>,
        control: String,
    },
    /// The external cancel request was successfully delivered.
    ExternalWorkflowExecutionCancelRequested {
        initiated_event_id: i64,
        namespace_id: NamespaceId,
        namespace: Option<String>,
        target_workflow_id: WorkflowId,
        target_run_id: Option<RunId>,
    },
    /// The external cancel request delivery failed.
    RequestCancelExternalWorkflowExecutionFailed {
        initiated_event_id: i64,
        namespace_id: NamespaceId,
        namespace: Option<String>,
        target_workflow_id: WorkflowId,
        target_run_id: Option<RunId>,
        cause: String,
    },
    /// A Nexus operation was scheduled.
    NexusOperationScheduled {
        workflow_task_completed_event_id: i64,
        operation_id: String,
        endpoint: String,
        endpoint_id: String,
        service: String,
        operation: String,
        input: Payloads,
        nexus_header: std::collections::BTreeMap<String, String>,
        schedule_to_close_timeout: Option<Duration>,
    },
    /// The Nexus operation transitioned to started (async).
    NexusOperationStarted {
        operation_id: String,
        scheduled_event_id: i64,
    },
    /// The Nexus operation completed successfully.
    NexusOperationCompleted {
        operation_id: String,
        scheduled_event_id: i64,
        result: Payloads,
    },
    /// The Nexus operation failed.
    NexusOperationFailed {
        operation_id: String,
        scheduled_event_id: i64,
        failure: Payload,
    },
    /// The Nexus operation was canceled.
    NexusOperationCanceled {
        operation_id: String,
        scheduled_event_id: i64,
    },
    /// The Nexus operation exceeded its timeout.
    NexusOperationTimedOut {
        operation_id: String,
        scheduled_event_id: i64,
    },
    /// A cancel was requested for a pending Nexus operation.
    NexusOperationCancelRequested { scheduled_event_id: i64 },
    /// A workflow update was accepted and is awaiting
    /// completion by the worker.
    WorkflowExecutionUpdateAccepted {
        update_id: String,
        update_name: String,
        input: Payloads,
        accepted_request_sequencing_event_id: i64,
    },
    /// A workflow update completed with a result.
    WorkflowExecutionUpdateCompleted {
        update_id: String,
        result: Payloads,
        accepted_event_id: i64,
    },
    /// A workflow update was rejected by the worker.
    WorkflowExecutionUpdateRejected {
        update_id: String,
        failure: Payload,
        rejected_request_message_id: String,
        rejected_request_sequencing_event_id: i64,
    },
    /// Execution-level options (versioning, callbacks) were
    /// updated by an operator or API call.
    WorkflowExecutionOptionsUpdated {
        versioning_override: FieldChange<VersioningOverride>,
        completion_callbacks: FieldChange<Vec<CompletionCallback>>,
        attached_request_id: Option<String>,
    },
    /// The workflow returned a successful result.
    WorkflowExecutionCompleted {
        workflow_task_completed_event_id: i64,
        result: Payloads,
    },
    /// The workflow failed with an application-level error.
    WorkflowExecutionFailed {
        workflow_task_completed_event_id: i64,
        failure: Payload,
        retry_state: RetryState,
        attempt: u32,
    },
    /// The workflow ended by spawning a new run via
    /// continue-as-new.
    WorkflowExecutionContinuedAsNew {
        workflow_task_completed_event_id: i64,
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
        initiator: ContinueAsNewInitiator,
        failure: Option<Payload>,
        last_completion_result: Option<Payloads>,
    },
    /// The workflow was canceled via a `CancelWorkflow`
    /// workflow command (cooperative cancellation completed).
    WorkflowExecutionCanceled {
        workflow_task_completed_event_id: i64,
        details: Option<Payloads>,
    },
}

/// How an activity task was resolved by the runtime.
///
/// The kernel uses this to emit the correct terminal activity
/// event and remove the activity from the open set.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ActivityResolution {
    /// The activity returned a successful result.
    Completed { result: Payloads },
    /// The activity failed with an application-level error.
    Failed { failure: Payload },
    /// The activity exceeded one of its configured timeouts.
    TimedOut { timeout_type: String },
    /// The activity was canceled (cooperative cancellation).
    Canceled { details: Option<Payloads> },
}

/// Summary of how and when a workflow execution closed.
///
/// Used by the projection plane to record the terminal state
/// without reading the full history.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CloseInfo {
    /// Terminal status the execution reached.
    pub status: ExecutionStatus,
    /// Wall-clock time the execution was closed.
    pub closed_at: OffsetDateTime,
}
