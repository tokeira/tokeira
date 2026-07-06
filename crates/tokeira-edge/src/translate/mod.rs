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
    event::HistoryEvent,
    state::{ParentClosePolicy, WorkflowVersioningInfo},
};
use tokeira_types::{
    ActivityTaskToken, BuildId, DeploymentId, ExecutionStatus, Headers, Memo, Payload, Payloads,
    RetryPolicy, RunId, RunKey, SearchAttributes, TaskKind, WorkflowId,
};

pub mod batch;
pub mod from_internal;
pub mod history_serializer;
pub mod nexus;
pub mod schedule;
pub mod to_internal;
pub mod worker_heartbeat;

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

/// The `versioning_override` change an `UpdateWorkflowExecutionOptions` request applies,
/// derived from the request's `update_mask` + `workflow_execution_options`. The mask must
/// always touch `versioning_override` (it is tokeira's only supported mutable option), so
/// the change is exactly Set-a-value or Clear — never "leave unchanged".
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VersioningOverrideChange {
    Set(VersioningOverride),
    Clear,
}

/// Edge request for `UpdateWorkflowExecutionOptions`. Only the `versioning_override`
/// option is modeled (the one mutable execution option tokeira persists, per
/// `api-conformance-workflow-options`); the mask is validated and reduced to a
/// [`VersioningOverrideChange`] at the gRPC boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateWorkflowExecutionOptionsRequest {
    pub namespace: String,
    pub workflow_id: String,
    pub run_id: Option<String>,
    pub versioning_override: VersioningOverrideChange,
    pub identity: String,
}

/// The post-update execution options echoed back to the caller. `versioning_override` is
/// `Some` after a Set and `None` after a Clear.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateWorkflowExecutionOptionsResponse {
    pub versioning_override: Option<VersioningOverride>,
}

/// User-facing summary/details metadata authored when a workflow starts.
///
/// Temporal stores this as SDK metadata, but the edge keeps it proto-free so
/// history and describe rendering can use the same deterministic payload model
/// as memo/header fields.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UserMetadata {
    /// Short UI-facing summary payload.
    pub summary: Option<Payload>,
    /// Longer UI-facing details payload.
    pub details: Option<Payload>,
}

/// Link metadata attached to workflow start or signal events.
///
/// Links are wire-level references to related Temporal entities. They are
/// preserved as domain data rather than interpreted by the server because their
/// meaning belongs to SDKs, UI, and audit tooling.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Link {
    WorkflowEvent {
        namespace: String,
        workflow_id: String,
        run_id: String,
        reference: Option<LinkWorkflowEventReference>,
    },
    BatchJob {
        job_id: String,
    },
    Activity {
        namespace: String,
        activity_id: String,
        run_id: String,
    },
    NexusOperation {
        namespace: String,
        operation_id: String,
        run_id: String,
    },
}

/// Event reference carried by a workflow-event link.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LinkWorkflowEventReference {
    Event { event_id: i64, event_type: i32 },
    RequestId { request_id: String, event_type: i32 },
}

/// Client-authored completion callback registration.
///
/// Public starts may register Nexus callbacks. Internal callbacks are rejected
/// before this DTO is built because Temporal exposes them only for replication
/// of already-authored history, not for external client authorship.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletionCallback {
    pub url: String,
    pub header: BTreeMap<String, String>,
    pub links: Vec<Link>,
}

/// Start priority metadata used by Temporal matching fairness.
#[derive(Clone, Debug, PartialEq)]
pub struct Priority {
    pub priority_key: i32,
    pub fairness_key: String,
    pub fairness_weight: f32,
}

/// Options applied when a start resolves to an existing running workflow.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OnConflictOptions {
    pub attach_request_id: bool,
    pub attach_completion_callbacks: bool,
    pub attach_links: bool,
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
    pub request_eager_execution: bool,
    pub workflow_start_delay: Option<time::Duration>,
    pub completion_callbacks: Vec<CompletionCallback>,
    pub user_metadata: Option<UserMetadata>,
    pub links: Vec<Link>,
    /// Worker deployment requested for the eagerly returned first WFT.
    ///
    /// This field has routing effect only when `request_eager_execution` is
    /// true. Keeping that gate at the DTO boundary preserves Temporal's
    /// contract that non-eager starts ignore eager-only deployment options.
    pub eager_worker_deployment_options: Option<tokeira_kernel::state::WorkerDeploymentVersionRef>,
    pub workflow_execution_timeout: Option<time::Duration>,
    pub workflow_run_timeout: Option<time::Duration>,
    pub workflow_task_timeout: Option<time::Duration>,
    pub retry_policy: Option<RetryPolicy>,
    pub conflict_policy: WorkflowIdConflictPolicy,
    pub reuse_policy: WorkflowIdReusePolicy,
    pub header: Option<Headers>,
    pub versioning_override: Option<VersioningOverride>,
    pub on_conflict_options: Option<OnConflictOptions>,
    pub priority: Option<Priority>,
    pub cron_schedule: Option<String>,
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
    /// Status of the workflow execution identified by `run_id` after the start. v1.31.0 sets
    /// this to RUNNING for a fresh start and to the incumbent run's status when attaching to an
    /// existing run (service/history/api/startworkflow/api.go @ v1.31.0).
    pub status: ExecutionStatus,
    /// Request id that authored a `WorkflowExecutionOptionsUpdated` event when this start attached
    /// to a running incumbent via `OnConflictOptions{AttachRequestId}`. `Some` selects the
    /// RequestIdRef response link (`generateRequestIdRefLink`, startworkflow/api.go:833); `None`
    /// keeps the default EventRef-to-WorkflowExecutionStarted link.
    pub attached_request_id: Option<String>,
    pub eager_workflow_task: Option<PollWorkflowTaskQueueResponse>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SignalWorkflowExecutionRequest {
    pub namespace: String,
    pub workflow_id: String,
    pub run_id: Option<String>,
    pub signal_name: String,
    pub input: Payloads,
    pub header: Option<Headers>,
    pub links: Vec<Link>,
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
    /// User-visible run id (distinct from the internal storage `RunKey`).
    pub run_id: RunId,
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
    /// Legacy single query delivered as a query task on the workflow-task poll path.
    ///
    /// This is distinct from `queries`: `query` is answered with
    /// `RespondQueryTaskCompleted`, while `queries` is the consistent-query map
    /// answered inside `RespondWorkflowTaskCompleted.query_results`.
    pub query: Option<WorkflowQueryDto>,
    pub queries: HashMap<String, WorkflowQueryDto>,
    pub messages: Vec<ProtocolMessageDto>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RespondWorkflowTaskCompletedRequest {
    pub task_token: Vec<u8>,
    pub identity: String,
    /// Decoded at the edge; downstream behaviour belongs to the `speculative-wft` spec.
    pub client_discards_speculative_with_events: bool,
    pub sdk_metadata: Option<Vec<u8>>,
    pub metering_metadata: Option<Vec<u8>>,
    pub worker_version: Option<String>,
    pub versioning_behavior: tokeira_kernel::state::VersioningBehavior,
    pub deployment_version: Option<tokeira_kernel::state::WorkerDeploymentVersionRef>,
    pub worker_deployment_name: Option<String>,
    /// Sticky queue + schedule-to-start timeout from
    /// `sticky_attributes` (v1.31.0; sticky raise S1). `None` clears sticky.
    pub sticky: Option<tokeira_kernel::StickySpec>,
    pub resource_id: String,
    pub worker_instance_key: String,
    pub worker_control_task_queue: String,
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
    Answered {
        result: Payloads,
    },
    Failed {
        error_message: String,
        /// The worker's structured failure, forwarded into the caller's
        /// typed `QueryFailed` serviceerror.
        failure: Option<Payload>,
    },
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
    pub activity_tasks: Vec<PollActivityTaskQueueResponse>,
    /// Non-zero when a SPECULATIVE workflow task DROPPED: the SDK rewinds its
    /// workflow-machine cache to this event id
    /// (`ResetHistoryEventId = LastCompletedWorkflowTaskStartedEventId` iff
    /// no completed event was written, respondworkflowtaskcompleted/
    /// api.go:770 @ v1.31.0; spec speculative-wft E2).
    pub reset_history_event_id: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DescribeWorkflowExecutionRequest {
    pub namespace: String,
    pub workflow_id: String,
    pub run_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ExecutionConfigDescription {
    pub task_queue: String,
    pub workflow_execution_timeout: Option<time::Duration>,
    pub workflow_run_timeout: Option<time::Duration>,
    pub default_workflow_task_timeout: time::Duration,
    pub user_metadata: Option<Payload>,
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
    pub execution_time: OffsetDateTime,
    pub execution_config: ExecutionConfigDescription,
    pub history_length: i64,
    pub state_transition_count: i64,
    pub parent_namespace_id: Option<String>,
    pub parent_workflow_id: Option<WorkflowId>,
    pub parent_run_id: Option<RunId>,
    pub root_workflow_id: Option<WorkflowId>,
    pub root_run_id: Option<RunId>,
    pub first_run_id: Option<RunId>,
    pub memo: Memo,
    pub search_attributes: SearchAttributes,
    pub pending_activities: Vec<PendingActivityDescription>,
    pub pending_children: Vec<PendingChildDescription>,
    pub pending_workflow_task: Option<PendingWorkflowTaskDescription>,
    pub callbacks: Vec<tokeira_kernel::state::CompletionCallback>,
    pub pending_nexus_operations: Vec<PendingNexusOperationDescription>,
    pub pause_info: Option<PauseInfoDescription>,
    pub execution_expiration_time: Option<OffsetDateTime>,
    pub run_expiration_time: Option<OffsetDateTime>,
    pub cancel_requested: bool,
    pub original_start_time: OffsetDateTime,
    pub versioning_info: Option<WorkflowVersioningInfo>,
    pub worker_deployment_name: Option<String>,
    /// Per-request → authoring-event map for `WorkflowExtendedInfo.request_id_infos`
    /// (the start request → STARTED, each UseExisting attach → OPTIONS_UPDATED).
    pub request_id_infos: std::collections::BTreeMap<String, tokeira_kernel::RequestIdInfo>,
    /// Count of external-payload references across the run's history
    /// (`executionStats.ExternalPayloadCount` surfaced by Describe,
    /// describeworkflow/api.go:166 @ v1.31.0).
    pub external_payload_count: i64,
    /// Total declared size of those external payloads in bytes.
    pub external_payload_size_bytes: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PauseInfoDescription {
    pub identity: String,
    pub paused_time: OffsetDateTime,
    pub reason: String,
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
    pub last_failure: Option<Payload>,
    /// Latest worker heartbeat progress, surfaced only once a heartbeat has
    /// been recorded (`GetPendingActivityInfo` gates `HeartbeatDetails` on a
    /// recorded heartbeat, `service/history/workflow/activity.go:147-150 @
    /// v1.31.0`).
    pub heartbeat_details: Option<Payloads>,
    /// `LastWorkerIdentity`: the starting worker's identity, falling back —
    /// only under a retry policy — to the retry bookkeeping identity written
    /// by heartbeats/retries (`GetPendingActivityInfo`,
    /// `workflow/activity.go:159-166 @ v1.31.0`). Empty = never reported.
    pub last_worker_identity: String,
    pub paused: bool,
    pub pause_info: Option<PauseInfoDescription>,
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
pub struct PendingNexusOperationDescription {
    pub endpoint: String,
    pub service: String,
    pub operation: String,
    pub scheduled_time: OffsetDateTime,
    pub scheduled_event_id: i64,
    pub schedule_to_close_timeout: Option<time::Duration>,
    pub schedule_to_start_timeout: Option<time::Duration>,
    pub start_to_close_timeout: Option<time::Duration>,
    pub started: bool,
    pub operation_token: Option<String>,
    /// Failed invocation attempts so far (`PendingNexusOperationInfo.attempt`).
    pub attempt: u32,
    /// The most recent retryable attempt's handler failure, surfaced on
    /// `PendingNexusOperationInfo.last_attempt_failure`. `Some` iff backing off.
    pub last_attempt_failure: Option<tokeira_types::Payload>,
    /// When a backing-off op is next eligible for re-dispatch
    /// (`PendingNexusOperationInfo.next_attempt_schedule_time`); drives the
    /// `BACKING_OFF` state. `None` unless backing off.
    pub next_attempt_at: Option<OffsetDateTime>,
}

// Visibility types re-exported from tokeira-projection (the authoritative owner).
pub use tokeira_projection::{
    ActivityExecutionSummary, CountActivityExecutionsRequest, CountActivityExecutionsResponse,
    CountWorkflowExecutionsRequest, CountWorkflowExecutionsResponse, GroupCount,
    ListActivityExecutionsRequest, ListActivityExecutionsResponse, ListWorkflowExecutionsRequest,
    ListWorkflowExecutionsResponse, WorkflowExecutionSummary,
};

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
    pub server_scaled_deployments: bool,
    pub worker_heartbeats: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SystemInfo {
    pub server_version: String,
    pub capabilities: SystemCapabilities,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceCapabilities {
    pub worker_heartbeats: bool,
    pub reported_problems_search_attribute: bool,
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
    pub capabilities: NamespaceCapabilities,
    /// Workflow-execution retention period, echoed as
    /// `NamespaceConfig.workflow_execution_retention_ttl`.
    pub retention: time::Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisterNamespaceRequest {
    pub namespace: String,
    /// Requested workflow-execution retention period (defaulted by the gRPC
    /// translator when the request omits a positive value).
    pub retention: time::Duration,
}

/// Target namespace lifecycle state for an [`UpdateNamespaceRequest`].
///
/// The edge stays proto-free: the gRPC layer maps Temporal's `NamespaceState`
/// enum onto this small set. `Unspecified` means "do not change state" (the
/// proto default), matching v1.31.0 `validateStateUpdate`
/// (`service/frontend/namespace_handler.go @ v1.31.0`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum NamespaceStateUpdate {
    /// No state change requested (proto default / `NAMESPACE_STATE_UNSPECIFIED`).
    #[default]
    Unspecified,
    Registered,
    Deprecated,
    Deleted,
}

/// Edge representation of `UpdateNamespaceRequest`.
///
/// Only the fields Tokeira's scoped namespace model can honour are carried: the
/// target name, an optional state transition, and an optional description update.
/// Replication, config, and security-token fields are accepted at the wire layer
/// but not modelled here — Tokeira runs a single non-global cluster.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateNamespaceRequest {
    pub namespace: String,
    /// Requested new lifecycle state. `Unspecified` leaves the state unchanged.
    pub state: NamespaceStateUpdate,
    /// Optional new description; `None` leaves it unchanged.
    pub description: Option<String>,
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
    /// User-visible run id of the owning run (distinct from `run_key`).
    pub run_id: RunId,
    pub activity_type: String,
    pub input: Payloads,
    pub attempt: u32,
    pub workflow_id: String,
    pub workflow_type: String,
    pub workflow_namespace: String,
    pub run_key: RunKey,
    pub header: Option<Headers>,
    pub retry_policy: Option<RetryPolicy>,
    pub heartbeat_details: Option<Payloads>,
    pub scheduled_time: Option<OffsetDateTime>,
    pub current_attempt_scheduled_time: Option<OffsetDateTime>,
    pub started_time: Option<OffsetDateTime>,
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
pub struct RespondActivityTaskCompletedByIdRequest {
    pub namespace: String,
    pub workflow_id: String,
    pub run_id: Option<String>,
    pub activity_id: String,
    pub result: Payloads,
    pub identity: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RespondActivityTaskCompletedByIdResponse;

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
pub struct RespondActivityTaskFailedByIdRequest {
    pub namespace: String,
    pub workflow_id: String,
    pub run_id: Option<String>,
    pub activity_id: String,
    pub failure: Payload,
    pub failure_error_type: Option<String>,
    pub is_non_retryable: bool,
    pub identity: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RespondActivityTaskFailedByIdResponse;

#[derive(Clone, Debug, PartialEq)]
pub struct RespondActivityTaskCanceledRequest {
    pub token: ActivityTaskToken,
    pub details: Option<Payloads>,
    pub identity: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RespondActivityTaskCanceledResponse;

#[derive(Clone, Debug, PartialEq)]
pub struct RespondActivityTaskCanceledByIdRequest {
    pub namespace: String,
    pub workflow_id: String,
    pub run_id: Option<String>,
    pub activity_id: String,
    pub details: Option<Payloads>,
    pub identity: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RespondActivityTaskCanceledByIdResponse;

#[derive(Clone, Debug, PartialEq)]
pub struct RecordActivityTaskHeartbeatRequest {
    pub token: ActivityTaskToken,
    pub details: Option<Payloads>,
    pub identity: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RecordActivityTaskHeartbeatResponse {
    pub cancel_requested: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RecordActivityTaskHeartbeatByIdRequest {
    pub namespace: String,
    pub workflow_id: String,
    pub run_id: Option<String>,
    pub activity_id: String,
    pub details: Option<Payloads>,
    pub identity: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RecordActivityTaskHeartbeatByIdResponse {
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
pub struct PauseWorkflowExecutionRequest {
    pub namespace: String,
    pub workflow_id: String,
    pub run_id: Option<String>,
    pub identity: String,
    pub reason: String,
    pub request_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PauseWorkflowExecutionResponse;

#[derive(Clone, Debug, PartialEq)]
pub struct UnpauseWorkflowExecutionRequest {
    pub namespace: String,
    pub workflow_id: String,
    pub run_id: Option<String>,
    pub identity: String,
    pub reason: String,
    pub request_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct UnpauseWorkflowExecutionResponse;

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
    pub first_execution_run_id: Option<String>,
    pub update_id: String,
    pub update_name: String,
    pub input: Payloads,
    pub wait_policy: UpdateWaitPolicyDto,
    pub timeout: Duration,
}

#[derive(Clone, Debug, PartialEq)]
pub enum UpdateWaitPolicyDto {
    Unspecified,
    Admitted,
    Accepted,
    Completed,
}

#[derive(Clone, Debug, PartialEq)]
pub enum UpdateLifecycleStageDto {
    Unspecified,
    Admitted,
    Accepted,
    Completed,
}

#[derive(Clone, Debug, PartialEq)]
pub struct UpdateRefDto {
    pub workflow_id: String,
    pub run_id: String,
    pub update_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum UpdateOutcomeDto {
    Completed {
        accepted_event_id: i64,
        result: Payloads,
    },
    Rejected {
        accepted_event_id: i64,
        failure: Payload,
    },
    /// Workflow closed after accepting but before completing the update;
    /// serialized as v1.31.0's `acceptedUpdateCompletedWorkflowFailure`
    /// (errors_failures.go:10-35 @ v1.31.0).
    AcceptedRunClosed,
    /// The worker completed its workflow task without processing this Sent
    /// update; serialized as v1.31.0's server-authored `unprocessedUpdateFailure`
    /// (errors_failures.go:18-25 @ v1.31.0; spec speculative-wft Req 9).
    RejectedUnprocessed,
}

#[derive(Clone, Debug, PartialEq)]
pub struct UpdateWorkflowExecutionResponse {
    pub update_ref: UpdateRefDto,
    pub stage: UpdateLifecycleStageDto,
    pub outcome: Option<UpdateOutcomeDto>,
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
    pub workflow_start_delay: Option<time::Duration>,
    pub user_metadata: Option<UserMetadata>,
    pub links: Vec<Link>,
    pub versioning_override: Option<VersioningOverride>,
    pub priority: Option<Priority>,
    pub cron_schedule: Option<String>,
    pub signal_name: String,
    pub signal_input: Payloads,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SignalWithStartWorkflowExecutionResponse {
    pub run_id: RunId,
    pub started: bool,
}

/// Edge request for `ExecuteMultiOperation` — exactly the Update-with-Start
/// `[Start, Update]` composition, the only operation list the RPC accepts at
/// v1.31.0 (proto `ExecuteMultiOperationRequest.Operation` is a two-arm oneof;
/// `workflow_handler.go:718-726 @ v1.31.0` rejects any other shape). Shape and
/// per-operation field validation happen at the gRPC boundary
/// (`multi_operation_request_to_edge`), so this DTO only exists for a
/// composition that passed every pre-mutation gate (Req 1, Property 1).
#[derive(Clone, Debug, PartialEq)]
pub struct ExecuteMultiOperationRequest {
    pub namespace: String,
    /// The start leg (`operations[0]`), already through the standalone
    /// start-request translation (standalone validation parity, Req 1.6).
    pub start: StartWorkflowExecutionRequest,
    /// The update leg (`operations[1]`), already through the standalone
    /// update-request translation; `update_id` is defaulted to a fresh UUIDv4
    /// when the client sent none, mirroring standalone update.
    pub update: UpdateWorkflowExecutionRequest,
    /// `Update.Request.Meta.identity`, threaded into the update leg's
    /// `RequestContext.caller_identity` (the shared update DTO does not carry
    /// identity; the admitted-update transport needs it).
    pub update_identity: Option<String>,
}

/// Ordered Update-with-Start success payload: the start leg's
/// `run_id`/`started`/`status` triple plus the update leg serialized exactly
/// like standalone `UpdateWorkflowExecution` (Req 3;
/// `workflow_handler.go:863-895 @ v1.31.0`).
#[derive(Clone, Debug, PartialEq)]
pub struct ExecuteMultiOperationResponse {
    pub run_id: RunId,
    /// Whether the start leg created a new run (false on attach/dedup/replay
    /// paths — `multioperation/api.go @ v1.31.0`).
    pub started: bool,
    /// Execution status reported on the start response (proto
    /// `StartWorkflowExecutionResponse.status`).
    pub status: ExecutionStatus,
    pub update: UpdateWorkflowExecutionResponse,
}

/// Success-or-per-operation-failure outcome of the edge Update-with-Start
/// orchestration. A leg failure is data here — not an `EdgeError` — because
/// the wire shape needs the failing leg's own typed error *plus* which sibling
/// aborts; the gRPC layer serializes it as the top-level status carrying the
/// `MultiOperationExecutionFailure` detail (Req 4;
/// `serviceerror.MultiOperationExecution.Status()` @ v1.31.0).
#[derive(Debug)]
pub enum ExecuteMultiOperationOutcome {
    Completed(ExecuteMultiOperationResponse),
    Failed(MultiOperationFailure),
}

/// The failing leg of an Update-with-Start, carrying the same `EdgeError` the
/// standalone RPC would surface so per-operation codes, messages, and details
/// are preserved verbatim (Req 4.2, 4.6).
#[derive(Debug)]
pub enum MultiOperationFailure {
    /// The START leg was rejected; the update leg aborts as the sibling.
    Start(crate::errors::EdgeError),
    /// The UPDATE leg failed. `started` records whether the start leg created
    /// a run (its op serializes as code OK) or aborts as the sibling
    /// (`MultiOperationError::UpdateFailed` in the runtime).
    Update {
        started: bool,
        error: crate::errors::EdgeError,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DescribeTaskQueueRequest {
    pub namespace: String,
    pub task_queue: String,
    pub task_kind: TaskKind,
    pub include_status: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListTaskQueuePartitionsRequest {
    pub namespace: String,
    pub task_queue: String,
}

/// One task-queue partition's metadata, mirroring `TaskQueuePartitionMetadata @
/// v1.31.0`: `key` is the partition's RPC name and `owner_host_name` the matching
/// host that owns it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskQueuePartition {
    pub key: String,
    pub owner_host_name: String,
}

/// Partition topology of a task queue, split by task type. tokeira runs a single
/// (root) partition per type, so each list holds exactly one entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListTaskQueuePartitionsResponse {
    pub activity_partitions: Vec<TaskQueuePartition>,
    pub workflow_partitions: Vec<TaskQueuePartition>,
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
    pub config: TaskQueueConfig,
    /// Worker Deployment versioning for this task queue, when any deployment
    /// version has polled it. `None` renders no `versioning_info` on the wire,
    /// matching a task queue with no deployment association.
    pub versioning_info: Option<TaskQueueVersioningInfo>,
}

/// A `(deployment_name, build_id)` reference to one Worker Deployment Version,
/// edge-side and proto-free.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerDeploymentVersionId {
    pub deployment_name: String,
    pub build_id: String,
}

/// Edge view of `TaskQueueVersioningInfo`. Carries only the structured version
/// refs, percentage, and update time; the gRPC layer derives the deprecated
/// `current_version`/`ramping_version` strings (a nil current renders as
/// `__unversioned__`, a nil ramping as empty) per
/// `task_queue_partition_manager.go:976 @ v1.31.0`.
#[derive(Clone, Debug, PartialEq)]
pub struct TaskQueueVersioningInfo {
    pub current_deployment_version: Option<WorkerDeploymentVersionId>,
    pub ramping_deployment_version: Option<WorkerDeploymentVersionId>,
    /// Whether the ramp targets unversioned workers (nil ramping version at a
    /// non-zero percentage); renders the deprecated `ramping_version` string as
    /// `__unversioned__` rather than empty.
    pub ramping_to_unversioned: bool,
    pub ramping_version_percentage: f32,
    pub update_time: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CountSchedulesRequest {
    pub namespace: String,
    pub query: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CountSchedulesResponse {
    pub count: u64,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TaskQueueConfig {
    pub queue_rate_limit: Option<f32>,
    pub fairness_key_rate_limit_default: Option<f32>,
    pub fairness_weight_overrides: BTreeMap<String, f32>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct UpdateTaskQueueConfigRequest {
    pub namespace: String,
    pub task_queue: String,
    pub config: TaskQueueConfig,
}

#[derive(Clone, Debug, PartialEq)]
pub struct UpdateTaskQueueConfigResponse {
    pub applied: TaskQueueConfig,
}

// ── Activity management DTOs (v1.62) ──
//
// v1.62 renamed the four activity-management RPCs from `*ById` to the
// unsuffixed form (`UpdateActivityOptions`, `PauseActivity`,
// `UnpauseActivity`, `ResetActivity`) and introduced an `activity` oneof
// selector that admits id-based, type-based, or match-all targeting. The
// Edge DTOs below mirror the v1.62 request shapes; the handler wiring in
// `workflow_service.rs` is owned by task 9.2 of `temporal-api-v1.62-sync`.

/// Target selector for the v1.62 activity-management `activity` oneof.
///
/// Each request type chooses exactly one variant. `MatchAll` is available
/// on Update and Reset; `UnpauseAll` is the Unpause-specific equivalent;
/// Pause does not admit a match-all variant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActivityTarget {
    /// Target the activity with this exact id.
    Id(String),
    /// Target every running activity of this type.
    Type(String),
    /// Target every running activity on the workflow execution.
    ///
    /// Only valid for Update (`match_all`), Unpause (`unpause_all`), and
    /// Reset (`match_all`). Rejected for Pause.
    MatchAll,
}

#[derive(Clone, Debug, PartialEq)]
pub struct UpdateActivityOptionsRequest {
    pub namespace: String,
    pub workflow_id: String,
    pub run_id: Option<String>,
    pub identity: String,
    /// Activity options to apply. Partial updates are controlled by
    /// `update_mask`; absent fields are left unchanged.
    pub activity_options: Option<ActivityOptions>,
    /// Field mask selecting which `activity_options` fields to apply.
    /// Represented as a `Vec<String>` of dotted field paths rather than the
    /// raw `google.protobuf.FieldMask`; translator normalises on ingress.
    pub update_mask: Vec<String>,
    pub target: ActivityTarget,
    /// If set, activity options are restored to the defaults captured on
    /// the first SCHEDULE event. Exclusive with every other option; the
    /// runtime wiring (task 9.2) rejects the request otherwise.
    pub restore_original: bool,
    /// v1.62 addition — overrides the activity type if the request is
    /// targeting an activity that has not yet been started. Absent
    /// resolves to the type on the SCHEDULE event.
    pub activity_type: Option<ActivityType>,
}

/// Edge DTO for `temporal.api.activity.v1.ActivityOptions`.
///
/// Mirrors the v1.62 shape field-for-field; the runtime wiring and
/// translator coverage for every field lands in task 9.2.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ActivityOptions {
    pub task_queue: Option<String>,
    pub schedule_to_close_timeout: Option<time::Duration>,
    pub schedule_to_start_timeout: Option<time::Duration>,
    pub start_to_close_timeout: Option<time::Duration>,
    pub heartbeat_timeout: Option<time::Duration>,
    pub retry_policy: Option<RetryPolicy>,
}

/// Edge DTO for `temporal.api.common.v1.ActivityType`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ActivityType {
    pub name: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct UpdateActivityOptionsResponse {
    pub activity_options: Option<ActivityOptions>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PauseActivityRequest {
    pub namespace: String,
    pub workflow_id: String,
    pub run_id: Option<String>,
    /// v1.62 addition — records the identity of the client issuing the
    /// pause. Carried through to the runtime pause record for audit.
    pub identity: String,
    pub target: ActivityTarget,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PauseActivityResponse;

#[derive(Clone, Debug, PartialEq)]
pub struct UnpauseActivityRequest {
    pub namespace: String,
    pub workflow_id: String,
    pub run_id: Option<String>,
    pub identity: String,
    pub target: ActivityTarget,
    /// Also resets the number of attempts on resume.
    pub reset_attempts: bool,
    /// v1.62 addition — also clears heartbeat details on resume so the
    /// activity restarts with a fresh heartbeat state.
    pub reset_heartbeat: bool,
    /// Schedules the resumed activity within a random offset of this
    /// duration; absent resolves to "resume immediately".
    pub jitter: Option<time::Duration>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct UnpauseActivityResponse;

#[derive(Clone, Debug, PartialEq)]
pub struct ResetActivityRequest {
    pub namespace: String,
    pub workflow_id: String,
    pub run_id: Option<String>,
    pub identity: String,
    pub target: ActivityTarget,
    pub reset_heartbeat: bool,
    /// v1.62 addition — if the activity is paused when reset lands, keep
    /// it paused after the reset. The default (`false`) resumes on reset.
    pub keep_paused: bool,
    /// Randomises the resumed activity within this offset; applies only
    /// when `keep_paused` is false.
    pub jitter: Option<time::Duration>,
    /// If set, activity options are restored to the defaults captured on
    /// the first SCHEDULE event.
    pub restore_original_options: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResetActivityResponse;

/// Sum the external-payload references across a run's history events.
///
/// Mirrors `CalculateExternalPayloadSize` (`proxy.VisitPayloads` over every
/// event, `service/history/workflow/external_payload_size.go @ v1.31.0`):
/// every `Payload` anywhere in an event contributes its `external_payloads`
/// count and declared byte sizes. Rather than enumerating every
/// payload-bearing field of every event variant (which would silently go
/// stale as variants grow), the fold walks the event's serde tree: the domain
/// `Payload` is the ONLY type that serializes an `external_payloads` array
/// (and only when non-empty), so each occurrence is exactly one payload's
/// details. Search attributes are typed values — never `Payload` — so they
/// are naturally excluded, matching v1.31.0's `SkipSearchAttributes`.
pub fn external_payload_stats(events: &[tokeira_kernel::HistoryEvent]) -> (i64, i64) {
    fn walk(value: &serde_json::Value, count: &mut i64, size: &mut i64) {
        match value {
            serde_json::Value::Object(map) => {
                if let Some(serde_json::Value::Array(details)) = map.get("external_payloads") {
                    for detail in details {
                        if let Some(bytes) = detail.get("size_bytes").and_then(|v| v.as_i64()) {
                            *count += 1;
                            *size += bytes;
                        }
                    }
                }
                for entry in map.values() {
                    walk(entry, count, size);
                }
            }
            serde_json::Value::Array(items) => {
                for item in items {
                    walk(item, count, size);
                }
            }
            _ => {}
        }
    }

    let mut count = 0;
    let mut size = 0;
    for event in events {
        if let Ok(value) = serde_json::to_value(event) {
            walk(&value, &mut count, &mut size);
        }
    }
    (count, size)
}

#[cfg(test)]
mod external_payload_stats_tests {
    use super::*;
    use tokeira_types::{ExternalPayloadDetail, Payload, Payloads};

    #[test]
    fn counts_external_payload_details_across_event_payloads() {
        let mut input_payload = Payload::new(Vec::new());
        input_payload.external_payloads = vec![ExternalPayloadDetail { size_bytes: 1024 }];
        let mut activity_payload = Payload::new(Vec::new());
        activity_payload.external_payloads = vec![
            ExternalPayloadDetail { size_bytes: 2048 },
            ExternalPayloadDetail { size_bytes: 8 },
        ];
        let events = vec![
            tokeira_kernel::HistoryEvent {
                event_id: 1,
                happened_at: time::OffsetDateTime::UNIX_EPOCH,
                kind: tokeira_kernel::HistoryEventKind::WorkflowExecutionSignaled {
                    signal_name: "s".into(),
                    input: Payloads(vec![input_payload]),
                    header: None,
                    links: Vec::new(),
                    identity: None,
                    request_id: "r".into(),
                },
            },
            tokeira_kernel::HistoryEvent {
                event_id: 2,
                happened_at: time::OffsetDateTime::UNIX_EPOCH,
                kind: tokeira_kernel::HistoryEventKind::WorkflowExecutionSignaled {
                    signal_name: "s2".into(),
                    input: Payloads(vec![activity_payload, Payload::new(b"plain".to_vec())]),
                    header: None,
                    links: Vec::new(),
                    identity: None,
                    request_id: "r2".into(),
                },
            },
        ];

        assert_eq!(external_payload_stats(&events), (3, 1024 + 2048 + 8));
    }
}
