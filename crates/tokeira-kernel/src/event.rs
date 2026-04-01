use time::{Duration, OffsetDateTime};
use tokeira_types::{
    ExecutionStatus, LogicalTaskSeq, Memo, Payload, Payloads, RetryPolicy, RunId,
    SearchAttributes, TaskQueueName, WorkerIdentity, WorkflowType,
};

/// Authoritative history event.
///
/// The exact storage encoding may change, but the semantic shape matters. Event
/// IDs are client-observable and should remain stable within a run.
#[derive(Clone, Debug, PartialEq)]
pub struct HistoryEvent {
    pub event_id: i64,
    pub happened_at: OffsetDateTime,
    pub kind: HistoryEventKind,
}

#[derive(Clone, Debug, PartialEq)]
pub enum HistoryEventKind {
    WorkflowExecutionStarted {
        workflow_type: WorkflowType,
        task_queue: TaskQueueName,
        input: Payloads,
        memo: Memo,
        search_attributes: SearchAttributes,
        request_id: String,
        continued_execution_run_id: Option<RunId>,
        first_execution_run_id: Option<RunId>,
        retry_policy: Option<RetryPolicy>,
        attempt: u32,
        workflow_execution_timeout: Option<Duration>,
        workflow_run_timeout: Option<Duration>,
        workflow_task_timeout: Duration,
    },
    WorkflowExecutionSignaled {
        signal_name: String,
        input: Payloads,
        request_id: String,
        identity: Option<String>,
    },
    WorkflowTaskScheduled {
        logical_seq: LogicalTaskSeq,
    },
    WorkflowTaskStarted {
        logical_seq: LogicalTaskSeq,
        scheduled_event_id: i64,
        attempt: u32,
        identity: WorkerIdentity,
    },
    WorkflowTaskCompleted {
        logical_seq: LogicalTaskSeq,
        scheduled_event_id: i64,
        started_event_id: i64,
        identity: WorkerIdentity,
    },
    ActivityTaskScheduled {
        activity_id: String,
        task_queue: TaskQueueName,
        input: Payloads,
        schedule_to_close_timeout: Option<Duration>,
        schedule_to_start_timeout: Option<Duration>,
        start_to_close_timeout: Option<Duration>,
        heartbeat_timeout: Option<Duration>,
    },
    ActivityTaskCompleted {
        activity_id: String,
        result: Payloads,
    },
    ActivityTaskFailed {
        activity_id: String,
        message: String,
    },
    ActivityTaskTimedOut {
        activity_id: String,
        timeout_type: String,
    },
    ActivityTaskCanceled {
        activity_id: String,
        details: Option<Payloads>,
    },
    TimerStarted {
        timer_id: String,
        fire_at: OffsetDateTime,
    },
    TimerFired {
        timer_id: String,
    },
    WorkflowExecutionCompleted {
        result: Payloads,
    },
    WorkflowExecutionFailed {
        message: String,
        details: Option<Payload>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum ActivityResolution {
    Completed { result: Payloads },
    Failed { message: String },
    TimedOut { timeout_type: String },
    Canceled { details: Option<Payloads> },
}

#[derive(Clone, Debug, PartialEq)]
pub struct CloseInfo {
    pub status: ExecutionStatus,
    pub closed_at: OffsetDateTime,
}
