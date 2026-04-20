//! Edge-layer Data Transfer Objects and proto ↔ edge translation.
//!
//! The DTOs in this module sit between the gRPC proto types and the
//! kernel/runtime domain types. They exist so that the edge layer can
//! reason about requests and responses without depending on generated
//! proto code, and so that the kernel never sees transport concerns.
//!
//! Sub-modules handle the two translation directions:
//! - [`to_internal`] — edge DTO → kernel command / runtime call
//! - [`from_internal`] — kernel result / runtime response → proto
//! - [`history_serializer`] — kernel `HistoryEvent` → proto history event

use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};

use time::OffsetDateTime;

use tokeira_kernel::{
    WorkflowCommand, WorkflowIdConflictPolicy, WorkflowIdReusePolicy,
    event::HistoryEvent, state::ParentClosePolicy,
};
use tokeira_types::{
    ActivityTaskToken, BuildId, DeploymentId, ExecutionStatus, Headers, Memo, Payload,
    Payloads, RetryPolicy, RunId, RunKey, SearchAttributes, TaskKind,
};

pub mod from_internal;
pub mod history_serializer;
pub mod to_internal;

/// Client-supplied worker-versioning override for starts.
///
/// Pinned starts bypass assignment rules and address one deployment/build
/// tuple directly. Auto-upgrade remains rule-managed, so it is represented
/// distinctly from a concrete build ID.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VersioningOverride {
    Pinned {
        deployment_series: String,
        build_id: String,
    },
    AutoUpgrade,
}

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
    pub workflow_execution_timeout: Option<time::Duration>,
    pub workflow_run_timeout: Option<time::Duration>,
    pub workflow_task_timeout: Option<time::Duration>,
    pub retry_policy: Option<RetryPolicy>,
    pub conflict_policy: WorkflowIdConflictPolicy,
    pub reuse_policy: WorkflowIdReusePolicy,
    pub header: Option<Headers>,
    pub versioning_override: Option<VersioningOverride>,
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
    pub started: bool,
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
    pub deployment: Option<DeploymentId>,
    pub build_id: Option<BuildId>,
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
    pub previous_started_event_id: i64,
    pub attempt: u32,
    pub scheduled_time: Option<OffsetDateTime>,
    pub started_time: Option<OffsetDateTime>,
    pub payload: WorkflowTaskPayloadDto,
    pub queries: HashMap<String, WorkflowQueryDto>,
    pub messages: Vec<ProtocolMessageDto>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RespondWorkflowTaskCompletedRequest {
    pub task_token: Vec<u8>,
    pub identity: String,
    pub commands: Vec<WorkflowCommand>,
    pub return_new_workflow_task: bool,
    pub force_create_new_workflow_task: bool,
    pub query_results: HashMap<String, QueryResultDto>,
    pub messages: Vec<ProtocolMessageDto>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WorkflowQueryDto {
    pub query_type: String,
    pub query_args: Payloads,
}

#[derive(Clone, Debug, PartialEq)]
pub enum QueryResultDto {
    Answered { result: Payloads },
    Failed { error_message: String },
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProtocolMessageDto {
    pub id: String,
    pub protocol_instance_id: String,
    pub body: Vec<u8>,
    pub sequencing_event_id: Option<i64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RespondWorkflowTaskCompletedResponse {
    pub transition_seq: u64,
    pub last_event_id: i64,
    pub execution_status: ExecutionStatus,
    pub new_run_id: Option<RunId>,
    pub was_duplicate: bool,
    pub workflow_task: Option<PollWorkflowTaskQueueResponse>,
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
    pub pending_activities: Vec<PendingActivityDescription>,
    pub pending_children: Vec<PendingChildDescription>,
    pub pending_workflow_task: Option<PendingWorkflowTaskDescription>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PendingActivityDescription {
    pub activity_id: String,
    pub activity_type: String,
    pub is_started: bool,
    pub attempt: u32,
    pub maximum_attempts: u32,
    pub scheduled_at: OffsetDateTime,
    pub started_at: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PendingChildDescription {
    pub workflow_id: String,
    pub run_id: Option<String>,
    pub workflow_type: String,
    pub initiated_event_id: i64,
    pub parent_close_policy: ParentClosePolicy,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PendingWorkflowTaskDescription {
    pub is_started: bool,
    pub scheduled_at: OffsetDateTime,
    pub started_at: Option<OffsetDateTime>,
    pub attempt: u32,
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
    pub history_length: i64,
    pub state_transition_count: i64,
    pub memo: Memo,
    pub search_attributes: SearchAttributes,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SystemCapabilities {
    pub signal_and_query_header: bool,
    pub internal_error_differentiation: bool,
    pub activity_failure_include_heartbeat: bool,
    pub supports_schedules: bool,
    pub encoded_failure_attributes: bool,
    pub build_id_based_versioning: bool,
    pub upsert_memo: bool,
    pub eager_workflow_start: bool,
    pub sdk_metadata: bool,
    pub count_group_by_execution_status: bool,
    pub nexus: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SystemInfo {
    pub server_version: String,
    pub capabilities: SystemCapabilities,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceDescription {
    pub name: String,
    pub namespace_id: Option<String>,
    pub is_global: bool,
    pub visibility_enabled: bool,
    pub deleted: bool,
    pub description: String,
    pub owner_email: String,
    pub cluster_name: String,
    pub custom_search_attribute_aliases: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisterNamespaceRequest {
    pub namespace: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListNamespacesResponse {
    pub namespaces: Vec<NamespaceDescription>,
    pub next_page_token: Option<String>,
}

// ── Activity endpoint DTOs ──

#[derive(Clone, Debug, PartialEq)]
pub struct PollActivityTaskQueueRequest {
    pub namespace: String,
    pub task_queue: String,
    pub worker_identity: String,
    pub timeout: Duration,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PollActivityTaskQueueResponse {
    pub task_token: Vec<u8>,
    pub activity_id: String,
    pub activity_type: String,
    pub input: Payloads,
    pub attempt: u32,
    pub workflow_id: String,
    pub workflow_type: String,
    pub workflow_namespace: String,
    pub run_key: RunKey,
    pub header: Option<Headers>,
    pub retry_policy: Option<RetryPolicy>,
    pub schedule_to_close_timeout: Option<Duration>,
    pub start_to_close_timeout: Option<Duration>,
    pub heartbeat_timeout: Option<Duration>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RespondActivityTaskCompletedRequest {
    pub token: ActivityTaskToken,
    pub result: Payloads,
    pub identity: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RespondActivityTaskCompletedResponse;

#[derive(Clone, Debug, PartialEq)]
pub struct RespondActivityTaskFailedRequest {
    pub token: ActivityTaskToken,
    pub failure: Payload,
    pub failure_error_type: Option<String>,
    pub is_non_retryable: bool,
    pub identity: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RespondActivityTaskFailedResponse;

#[derive(Clone, Debug, PartialEq)]
pub struct RecordActivityTaskHeartbeatRequest {
    pub token: ActivityTaskToken,
    pub identity: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RecordActivityTaskHeartbeatResponse {
    pub cancel_requested: bool,
}

// ── Advanced workflow endpoint DTOs ──

#[derive(Clone, Debug, PartialEq)]
pub struct TerminateWorkflowExecutionRequest {
    pub namespace: String,
    pub workflow_id: String,
    pub run_id: Option<String>,
    pub reason: String,
    pub details: Option<Payloads>,
    pub identity: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TerminateWorkflowExecutionResponse;

#[derive(Clone, Debug, PartialEq)]
pub struct RequestCancelWorkflowExecutionRequest {
    pub namespace: String,
    pub workflow_id: String,
    pub run_id: Option<String>,
    pub reason: String,
    pub identity: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RequestCancelWorkflowExecutionResponse;

#[derive(Clone, Debug, PartialEq)]
pub struct QueryWorkflowRequest {
    pub namespace: String,
    pub workflow_id: String,
    pub run_id: Option<String>,
    pub query_type: String,
    pub query_args: Payloads,
    pub timeout: Duration,
}

#[derive(Clone, Debug, PartialEq)]
pub struct QueryWorkflowResponse {
    pub result: Option<Payloads>,
    pub rejected_status: Option<ExecutionStatus>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct UpdateWorkflowExecutionRequest {
    pub namespace: String,
    pub workflow_id: String,
    pub run_id: Option<String>,
    pub update_id: String,
    pub update_name: String,
    pub input: Payloads,
    pub wait_policy: UpdateWaitPolicyDto,
    pub timeout: Duration,
}

#[derive(Clone, Debug, PartialEq)]
pub enum UpdateWaitPolicyDto {
    Accepted,
    Completed,
}

#[derive(Clone, Debug, PartialEq)]
pub enum UpdateOutcomeDto {
    Accepted {
        accepted_event_id: i64,
    },
    Completed {
        accepted_event_id: i64,
        result: Payloads,
    },
    Rejected {
        accepted_event_id: i64,
        failure: Payload,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct UpdateWorkflowExecutionResponse {
    pub outcome: UpdateOutcomeDto,
}

// ── GetWorkflowExecutionHistory DTOs ──

#[derive(Clone, Debug, PartialEq)]
pub struct GetWorkflowExecutionHistoryRequest {
    pub namespace: String,
    pub workflow_id: String,
    pub run_id: Option<String>,
    pub maximum_page_size: usize,
    pub wait_new_event: bool,
    pub history_event_filter_type: i32,
    pub next_page_token: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GetWorkflowExecutionHistoryResponse {
    pub history: Vec<HistoryEvent>,
    pub next_page_token: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GetWorkflowExecutionHistoryReverseRequest {
    pub namespace: String,
    pub workflow_id: String,
    pub run_id: Option<String>,
    pub maximum_page_size: usize,
    pub next_page_token: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GetWorkflowExecutionHistoryReverseResponse {
    pub history: Vec<HistoryEvent>,
    pub next_page_token: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteWorkflowExecutionRequest {
    pub namespace: String,
    pub workflow_id: String,
    pub run_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResetWorkflowExecutionRequest {
    pub namespace: String,
    pub workflow_id: String,
    pub run_id: Option<String>,
    pub reason: String,
    pub workflow_task_finish_event_id: i64,
    pub request_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResetWorkflowExecutionResponse {
    pub run_id: RunId,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SignalWithStartWorkflowExecutionRequest {
    pub namespace: String,
    pub workflow_id: String,
    pub workflow_type: String,
    pub task_queue: String,
    pub input: Payloads,
    pub request_id: Option<String>,
    pub memo: Memo,
    pub search_attributes: SearchAttributes,
    pub identity: Option<String>,
    pub workflow_execution_timeout: Option<time::Duration>,
    pub workflow_run_timeout: Option<time::Duration>,
    pub workflow_task_timeout: Option<time::Duration>,
    pub retry_policy: Option<RetryPolicy>,
    pub conflict_policy: WorkflowIdConflictPolicy,
    pub reuse_policy: WorkflowIdReusePolicy,
    pub header: Option<Headers>,
    pub versioning_override: Option<VersioningOverride>,
    pub signal_name: String,
    pub signal_input: Payloads,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SignalWithStartWorkflowExecutionResponse {
    pub run_id: RunId,
    pub started: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DescribeTaskQueueRequest {
    pub namespace: String,
    pub task_queue: String,
    pub task_kind: TaskKind,
    pub include_status: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PollerInfo {
    pub identity: String,
    pub last_access_time: Option<OffsetDateTime>,
    pub rate_per_second: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DescribeTaskQueueResponse {
    pub pollers: Vec<PollerInfo>,
    pub backlog_count_hint: Option<i64>,
}
