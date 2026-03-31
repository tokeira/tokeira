use time::OffsetDateTime;
use tokeira_types::{ExecutionStatus, LogicalTaskSeq, Memo, Payload, Payloads, SearchAttributes, WorkerIdentity, TaskQueueName, WorkflowType};

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
    },
    ActivityTaskCompleted {
        activity_id: String,
        result: Payloads,
    },
    ActivityTaskFailed {
        activity_id: String,
        message: String,
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
}

#[derive(Clone, Debug, PartialEq)]
pub struct CloseInfo {
    pub status: ExecutionStatus,
    pub closed_at: OffsetDateTime,
}
