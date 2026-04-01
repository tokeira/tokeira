use time::{Duration, OffsetDateTime};
use tokeira_types::{
    LogicalTaskSeq, Memo, NamespaceId, Payload, Payloads, RequestContext, RetryPolicy,
    SearchAttributes, TaskQueueName, WorkerIdentity, WorkflowId, WorkflowTaskToken, WorkflowType,
    RunId, RunKey,
};

use crate::event::ActivityResolution;

/// Commands are authoritative things that the server has decided happened.
///
/// A command is not the same as a transport message. By the time something gets
/// here, routing, auth, idempotency lookup, and request shaping should already
/// have happened.
#[derive(Clone, Debug, PartialEq)]
pub enum Command {
    Start(StartRequest),
    Signal(SignalRequest),
    Cancel(CancelRequest),
    Terminate(TerminateRequest),
    WorkflowTaskStarted(StartWorkflowTaskRequest),
    WorkflowTaskCompleted(WorkflowTaskCompletedRequest),
    WorkflowTaskFailed(WorkflowTaskFailedRequest),
    WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest),
    ActivityResolved(ActivityResolvedRequest),
    TimerDue(TimerDueRequest),
}

#[derive(Clone, Debug, PartialEq)]
pub enum WorkflowTaskFailedCause {
    NonDeterminismError,
    BadScheduleActivityAttributes,
    BadStartTimerAttributes,
    UnhandledCommand,
    BadRequestCancelActivityAttributes,
    WorkflowWorkerUnhandledFailure,
    BadSignalWorkflowExecutionAttributes,
}

#[derive(Clone, Debug, PartialEq)]
pub enum WorkflowTaskTimeoutType {
    StartToClose,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StartRequest {
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
    pub attempt: u32,
    pub continued_execution_run_id: Option<RunId>,
    pub first_execution_run_id: Option<RunId>,
    pub request: RequestContext,
    pub now: OffsetDateTime,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SignalRequest {
    pub signal_name: String,
    pub input: Payloads,
    pub request: RequestContext,
    pub now: OffsetDateTime,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ExternalWorkflowExecution {
    pub namespace_id: NamespaceId,
    pub workflow_id: WorkflowId,
    pub run_id: RunId,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CancelRequest {
    pub reason: String,
    pub external_initiator: Option<ExternalWorkflowExecution>,
    pub request: RequestContext,
    pub now: OffsetDateTime,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TerminateRequest {
    pub reason: String,
    pub details: Option<Payloads>,
    pub identity: String,
    pub request: RequestContext,
    pub now: OffsetDateTime,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StartWorkflowTaskRequest {
    pub logical_seq: tokeira_types::LogicalTaskSeq,
    pub worker_identity: WorkerIdentity,
    pub sticky_ttl: Option<Duration>,
    pub now: OffsetDateTime,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WorkflowTaskCompletedRequest {
    pub token: WorkflowTaskToken,
    pub identity: WorkerIdentity,
    pub commands: Vec<WorkflowCommand>,
    pub force_new_workflow_task: bool,
    pub now: OffsetDateTime,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WorkflowTaskFailedRequest {
    pub logical_seq: LogicalTaskSeq,
    pub started_event_id: i64,
    pub failure_cause: WorkflowTaskFailedCause,
    pub failure_details: Option<Payload>,
    pub worker_identity: WorkerIdentity,
    pub now: OffsetDateTime,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WorkflowTaskTimedOutRequest {
    pub logical_seq: LogicalTaskSeq,
    pub started_event_id: i64,
    pub timeout_type: WorkflowTaskTimeoutType,
    pub now: OffsetDateTime,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ActivityResolvedRequest {
    pub activity_id: String,
    pub resolution: ActivityResolution,
    pub worker_identity: Option<WorkerIdentity>,
    pub now: OffsetDateTime,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TimerDueRequest {
    pub timer_id: String,
    pub fired_at: OffsetDateTime,
}

/// Commands produced by workflow code when a workflow task completes.
///
/// TODO(correctness): add child workflows, updates, versioning markers, local
/// activities, cancellation scopes, patch markers, and continue-as-new.
#[derive(Clone, Debug, PartialEq)]
pub enum WorkflowCommand {
    ScheduleActivity {
        activity_id: String,
        task_queue: TaskQueueName,
        input: Payloads,
        schedule_to_close_timeout: Option<Duration>,
        schedule_to_start_timeout: Option<Duration>,
        start_to_close_timeout: Option<Duration>,
        heartbeat_timeout: Option<Duration>,
    },
    StartTimer {
        timer_id: String,
        fire_at: OffsetDateTime,
    },
    UpsertMemo(Memo),
    UpsertSearchAttributes(SearchAttributes),
    CompleteWorkflow {
        result: Payloads,
    },
    FailWorkflow {
        message: String,
        details: Option<Payload>,
    },
    CancelWorkflow,
    RequestCancelActivity {
        activity_id: String,
    },
    CancelTimer {
        timer_id: String,
    },
    RequestNewWorkflowTask,
}
