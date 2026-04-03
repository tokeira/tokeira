use std::time::Duration;

use time::OffsetDateTime;

use tokeira_kernel::{WorkflowCommand, event::HistoryEvent};
use tokeira_types::{ExecutionStatus, Memo, Payloads, RunId, RunKey, SearchAttributes};

pub mod from_internal;
pub mod to_internal;

/// Edge-facing request for `StartWorkflowExecution`.
///
/// This is intentionally close to the client-facing contract rather than the
/// kernel-facing contract. The translate layer is where we decide defaults for
/// request ids, timestamps, and generated run identifiers.
#[derive(Clone, Debug, PartialEq)]
pub struct StartWorkflowExecutionRequest {
    pub namespace: String,
    pub workflow_id: String,
    pub workflow_type: String,
    pub task_queue: String,
    pub input: Payloads,
    pub request_id: Option<String>,
    pub memo: Memo,
    pub search_attributes: SearchAttributes,
    pub identity: Option<String>,
    pub run_key: Option<RunKey>,
    pub run_id: Option<RunId>,
    pub now: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StartWorkflowExecutionResponse {
    pub run_key: RunKey,
    pub run_id: RunId,
    pub transition_seq: u64,
    pub last_event_id: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SignalWorkflowExecutionRequest {
    pub namespace: String,
    pub workflow_id: String,
    pub signal_name: String,
    pub input: Payloads,
    pub request_id: Option<String>,
    pub identity: Option<String>,
    pub now: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SignalWorkflowExecutionResponse {
    pub accepted: bool,
    pub transition_seq: u64,
    pub last_event_id: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PollWorkflowTaskQueueRequest {
    pub namespace: String,
    pub task_queue: String,
    pub worker_identity: String,
    pub sticky_run: Option<RunKey>,
    pub timeout: Duration,
    pub sticky_ttl: Duration,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WorkflowTaskPayloadDto {
    pub workflow_id: String,
    pub run_key: RunKey,
    pub task_queue: String,
    pub history: Vec<HistoryEvent>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PollWorkflowTaskQueueResponse {
    pub task_token: Vec<u8>,
    pub started_event_id: i64,
    pub attempt: u32,
    pub payload: WorkflowTaskPayloadDto,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RespondWorkflowTaskCompletedRequest {
    pub task_token: Vec<u8>,
    pub identity: String,
    pub commands: Vec<WorkflowCommand>,
    pub force_new_workflow_task: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RespondWorkflowTaskCompletedResponse {
    pub transition_seq: u64,
    pub last_event_id: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DescribeWorkflowExecutionRequest {
    pub namespace: String,
    pub workflow_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WorkflowExecutionDescription {
    pub namespace: String,
    pub workflow_id: String,
    pub run_key: RunKey,
    pub run_id: RunId,
    pub workflow_type: String,
    pub task_queue: String,
    pub status: ExecutionStatus,
    pub start_time: Option<OffsetDateTime>,
    pub close_time: Option<OffsetDateTime>,
    pub history_length: i64,
    pub state_transition_count: i64,
    pub memo: Memo,
    pub search_attributes: SearchAttributes,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WorkflowExecutionSummary {
    pub namespace: String,
    pub workflow_id: String,
    pub run_id: RunId,
    pub workflow_type: String,
    pub task_queue: String,
    pub status: ExecutionStatus,
    pub start_time: Option<OffsetDateTime>,
    pub close_time: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListWorkflowExecutionsRequest {
    pub namespace: String,
    pub query: Option<String>,
    pub page_size: usize,
    pub next_page_token: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListWorkflowExecutionsResponse {
    pub executions: Vec<WorkflowExecutionSummary>,
    pub next_page_token: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GroupCount {
    pub value: String,
    pub count: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CountWorkflowExecutionsRequest {
    pub namespace: String,
    pub query: Option<String>,
    pub group_by: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CountWorkflowExecutionsResponse {
    pub total_count: i64,
    pub groups: Vec<GroupCount>,
}
