//! Business-logic layer between gRPC handlers and the workflow runtime.
//!
//! This module translates transport concerns into runtime calls, long-poll
//! coordination, and visibility lookups. The important boundary is what does
//! *not* belong here: authoritative workflow mutation rules still live in the
//! runtime/kernel, and durable execution state still lives in storage. The
//! edge is responsible for request shaping, polling semantics, and combining
//! read-side helpers into the APIs the Temporal surface expects.
//!
//! Query and update delivery are the most nuanced paths here. Queries use a
//! two-path dispatch (direct broker dispatch for idle runs, barrier-buffered
//! attachment for active runs) to guarantee consistency without unnecessary
//! WFT round-trips. Updates flow through the `UpdateRegistry` and are
//! surfaced to workers as `ProtocolMessage` entries on the poll response.

use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use http::HeaderMap;
use prost::Message as _;
use time::OffsetDateTime;
use tokeira_compatibility::{FEATURE_MATRIX, FeatureState};
use tokeira_kernel::{
    CancelRequest, FieldChange, HistoryEvent, HistoryEventKind, LoadedRun, NexusResolution,
    ResetRequest, SignalRequest, SignalWithStartRequest, StartRequest, TerminateRequest,
    UpdateActivityOptionsRequest as KernelUpdateActivityOptionsRequest,
    WorkflowTaskCompletedRequest,
};
use tokeira_runtime::{
    ActivityTokenResolutionError, BatchError, BatchOperationEntry, BatchOperationStore,
    BatchProgressCounters, BatchResetTarget, BufferedQueryRegistry, CreateDeployment,
    CreateVersion, DeleteDeployment, DeleteVersion, DeploymentPage, DeploymentView,
    DescribeVersion, InMemoryBroker, ListDeployments, NexusTaskBroker, NexusTaskToken,
    OverlapDecision, OverlapPolicy, PendingUpdateTransport, QueryResult, RegisterPolledDeployment,
    ResetWorkflowResult, ScheduleActionResult, SchedulePatch, ScheduleStore, SetCurrent,
    SetCurrentOutcome, SetManager, SetManagerOutcome, SetRamping, SetRampingOutcome,
    SignalWithStartResult, StartWorkflowResult, StartedActivityTask, StartedWorkflowTask,
    TaskQueueConfigEntry, TaskQueueConfigStore, TaskQueueVersioningView, UpdateComputeConfig,
    UpdateLifecycleError, UpdateLifecycleSnapshot, UpdateMetadata, UpdateTransportResolution,
    UpdateWaitPolicy, ValidateComputeConfig, VersionMetadataView, VersionView, VersioningRuleStore,
    WorkerRegistry, WorkflowActivation, WorkflowExecution, WorkflowExecutionStatus,
    compute_matching_times, decide_overlap, schedule_workflow_id,
};
use tokeira_storage::{
    ConflictToken, DeploymentKey, DeploymentName, DeploymentTaskQueueType, RunRepository,
};
use tokeira_types::{
    ActivityTaskToken, ArchetypeId, ExecutionRef, ExecutionStatus, HeartbeatStore, Payload,
    Payloads, QueueKey, RequestContext, RequestId, RunId, RunKey, TaskKind, TaskQueueName,
    WorkerIdentity, WorkflowId,
};
use uuid::Uuid;

use crate::{
    batch_engine::{resolve_reset_target_from_history, run_batch_operation},
    errors::{EdgeError, EdgeResult},
    grpc::tracing_interceptor,
    history_wait::HistoryWaitRegistry,
    interceptors::{Action, EdgeContext, EdgeInterceptors},
    long_poll::LongPollGate,
    metrics as edge_metrics,
    namespace_cache::{NamespaceCache, ResolvedNamespace},
    operator_service::{ClusterInfo, OperatorApi, SearchAttributeDefinition},
    pending_queries::{LEGACY_QUERY_ID, PendingQueryStore},
    poller_registry::{ActivePoller, PollerRegistry},
    routing::{EdgeRouter, ensure_local},
    translate::{
        CountActivityExecutionsRequest, CountActivityExecutionsResponse,
        CountWorkflowExecutionsRequest, CountWorkflowExecutionsResponse,
        DeleteWorkflowExecutionRequest, DescribeTaskQueueRequest, DescribeTaskQueueResponse,
        DescribeWorkflowExecutionRequest, ListActivityExecutionsRequest,
        ListActivityExecutionsResponse, ListNamespacesResponse as EdgeListNamespacesResponse,
        ListTaskQueuePartitionsRequest, ListTaskQueuePartitionsResponse,
        ListWorkflowExecutionsRequest, ListWorkflowExecutionsResponse, NamespaceCapabilities,
        NamespaceDescription, NamespaceStateUpdate, PauseWorkflowExecutionRequest,
        PauseWorkflowExecutionResponse, PollActivityTaskQueueRequest,
        PollActivityTaskQueueResponse, PollWorkflowTaskQueueRequest, PollWorkflowTaskQueueResponse,
        ProtocolMessageDto, QueryResultDto, QueryWorkflowRequest, QueryWorkflowResponse,
        RecordActivityTaskHeartbeatByIdRequest, RecordActivityTaskHeartbeatByIdResponse,
        RecordActivityTaskHeartbeatRequest, RecordActivityTaskHeartbeatResponse,
        RegisterNamespaceRequest, RequestCancelWorkflowExecutionRequest,
        RequestCancelWorkflowExecutionResponse, ResetWorkflowExecutionRequest,
        ResetWorkflowExecutionResponse, RespondActivityTaskCanceledByIdRequest,
        RespondActivityTaskCanceledByIdResponse, RespondActivityTaskCanceledRequest,
        RespondActivityTaskCanceledResponse, RespondActivityTaskCompletedByIdRequest,
        RespondActivityTaskCompletedByIdResponse, RespondActivityTaskCompletedRequest,
        RespondActivityTaskCompletedResponse, RespondActivityTaskFailedByIdRequest,
        RespondActivityTaskFailedByIdResponse, RespondActivityTaskFailedRequest,
        RespondActivityTaskFailedResponse, RespondWorkflowTaskCompletedRequest,
        RespondWorkflowTaskCompletedResponse, SignalWithStartWorkflowExecutionRequest,
        SignalWithStartWorkflowExecutionResponse, SignalWorkflowExecutionRequest,
        SignalWorkflowExecutionResponse, StartWorkflowExecutionRequest,
        StartWorkflowExecutionResponse, SystemCapabilities, SystemInfo, TaskQueueConfig,
        TaskQueuePartition, TerminateWorkflowExecutionRequest, TerminateWorkflowExecutionResponse,
        UnpauseWorkflowExecutionRequest, UnpauseWorkflowExecutionResponse,
        UpdateActivityOptionsRequest, UpdateActivityOptionsResponse, UpdateNamespaceRequest,
        UpdateWorkflowExecutionOptionsRequest, UpdateWorkflowExecutionOptionsResponse,
        UpdateWorkflowExecutionRequest, UpdateWorkflowExecutionResponse, VersioningOverrideChange,
        WorkflowExecutionDescription, WorkflowQueryDto, from_internal, to_internal,
    },
};

#[derive(Clone, Debug)]
pub struct BatchDispatchContext {
    pub namespace_id: tokeira_types::NamespaceId,
    pub namespace_name: String,
    pub identity: String,
    pub edge_context: EdgeContext,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowMutationOutcome {
    pub transition_seq: u64,
    pub last_event_id: i64,
    pub was_duplicate: bool,
    pub execution_status: ExecutionStatus,
    pub new_run_id: Option<RunId>,
}

/// Worker-deployment mutation response with the post-commit conflict token.
///
/// Temporal's v2 deployment RPCs are CAS-shaped: mutating calls return enough state for
/// the next caller to supply an optimistic conflict token. Keeping the token alongside
/// the operation-specific view lets gRPC translators build the exact protobuf response
/// without re-reading the registry.
#[derive(Clone, Debug, PartialEq)]
pub struct DeploymentMutationOutcome<T> {
    pub conflict_token: ConflictToken,
    pub view: T,
}

fn schedule_request_context(now: OffsetDateTime) -> RequestContext {
    RequestContext {
        request_id: RequestId(Uuid::new_v4().to_string()),
        caller_identity: Some("schedule-engine".to_string()),
        received_at: now,
    }
}

fn worker_identity_from_request(identity: String) -> Option<WorkerIdentity> {
    if identity.is_empty() {
        None
    } else {
        Some(WorkerIdentity(identity))
    }
}

fn build_update_activity_options_command(
    ctx: &EdgeContext,
    activity_id: String,
    req: &UpdateActivityOptionsRequest,
) -> EdgeResult<KernelUpdateActivityOptionsRequest> {
    if req.restore_original {
        return Err(EdgeError::BadRequest(
            "restore_original is not supported by update_activity_options yet".to_string(),
        ));
    }
    let options = req
        .activity_options
        .as_ref()
        .ok_or_else(|| EdgeError::BadRequest("activity_options is required".to_string()))?;
    let task_queue_selected = option_field_selected(&req.update_mask, "task_queue");
    let schedule_to_close_selected =
        option_field_selected(&req.update_mask, "schedule_to_close_timeout");
    let schedule_to_start_selected =
        option_field_selected(&req.update_mask, "schedule_to_start_timeout");
    let start_to_close_selected = option_field_selected(&req.update_mask, "start_to_close_timeout");
    let heartbeat_selected = option_field_selected(&req.update_mask, "heartbeat_timeout");

    let task_queue = if task_queue_selected {
        match options.task_queue.as_ref() {
            Some(task_queue) => FieldChange::Set(TaskQueueName(task_queue.clone())),
            None => {
                return Err(EdgeError::BadRequest(
                    "task_queue cannot be cleared".to_string(),
                ));
            }
        }
    } else {
        FieldChange::Unchanged
    };

    let command = KernelUpdateActivityOptionsRequest {
        activity_id,
        task_queue,
        schedule_to_close_timeout: optional_duration_change(
            schedule_to_close_selected,
            options.schedule_to_close_timeout,
        ),
        schedule_to_start_timeout: optional_duration_change(
            schedule_to_start_selected,
            options.schedule_to_start_timeout,
        ),
        start_to_close_timeout: optional_duration_change(
            start_to_close_selected,
            options.start_to_close_timeout,
        ),
        heartbeat_timeout: optional_duration_change(heartbeat_selected, options.heartbeat_timeout),
        request: RequestContext {
            request_id: RequestId(ctx.request_id.as_str().to_string()),
            caller_identity: worker_identity_from_request(req.identity.clone())
                .map(|identity| identity.0),
            received_at: ctx.received_at,
        },
        now: OffsetDateTime::now_utc(),
    };
    if matches!(command.task_queue, FieldChange::Unchanged)
        && matches!(command.schedule_to_close_timeout, FieldChange::Unchanged)
        && matches!(command.schedule_to_start_timeout, FieldChange::Unchanged)
        && matches!(command.start_to_close_timeout, FieldChange::Unchanged)
        && matches!(command.heartbeat_timeout, FieldChange::Unchanged)
    {
        return Err(EdgeError::BadRequest(
            "update_activity_options requires at least one changed option".to_string(),
        ));
    }
    Ok(command)
}

fn optional_duration_change(
    selected: bool,
    value: Option<time::Duration>,
) -> FieldChange<Option<time::Duration>> {
    if selected {
        FieldChange::Set(value)
    } else {
        FieldChange::Unchanged
    }
}

fn option_field_selected(update_mask: &[String], field: &str) -> bool {
    if update_mask.is_empty() {
        return true;
    }
    update_mask
        .iter()
        .any(|path| path == field || path == &format!("activity_options.{field}"))
}

#[async_trait]
pub trait WorkflowRuntimeApi: Send + Sync + 'static {
    /// Start a workflow and return mutation metadata for callers that only
    /// care about the committed transition, not conflict-policy nuance.
    async fn start_workflow(&self, req: StartRequest) -> Result<WorkflowMutationOutcome>;

    /// Start a workflow while preserving richer conflict/reuse results needed
    /// by edge APIs such as `SignalWithStartWorkflowExecution`.
    async fn start_workflow_with_policy(&self, req: StartRequest) -> Result<StartWorkflowResult>;

    /// Start a new execution or signal an existing one according to the
    /// workflow-id conflict policy carried in the request.
    async fn signal_with_start_workflow(
        &self,
        req: SignalWithStartRequest,
    ) -> Result<SignalWithStartResult>;

    async fn signal_workflow(
        &self,
        run_key: RunKey,
        req: SignalRequest,
    ) -> Result<WorkflowMutationOutcome>;

    async fn poll_workflow_task(
        &self,
        queue: tokeira_types::QueueKey,
        worker_identity: tokeira_types::WorkerIdentity,
        timeout: std::time::Duration,
    ) -> Result<Option<StartedWorkflowTask>>;

    /// Poll the worker-facing workflow queue for either a started WFT or a direct query.
    ///
    /// The default keeps older test doubles workflow-task-only. The real
    /// runtime adapter overrides this because Temporal-compatible workers
    /// receive legacy direct queries through `PollWorkflowTaskQueue`, not a
    /// separate query-poll RPC (`service/matching/matching_engine.go:1084 @
    /// v1.31.0`).
    async fn poll_workflow_activation(
        &self,
        queue: tokeira_types::QueueKey,
        worker_identity: tokeira_types::WorkerIdentity,
        timeout: std::time::Duration,
    ) -> Result<Option<WorkflowActivation>> {
        self.poll_workflow_task(queue, worker_identity, timeout)
            .await
            .map(|task| task.map(WorkflowActivation::WorkflowTask))
    }

    async fn try_claim_workflow_task(
        &self,
        queue: tokeira_types::QueueKey,
        run_key: RunKey,
        worker_identity: tokeira_types::WorkerIdentity,
    ) -> Result<Option<StartedWorkflowTask>> {
        let _ = (queue, run_key, worker_identity);
        Ok(None)
    }

    async fn complete_workflow_task(
        &self,
        req: WorkflowTaskCompletedRequest,
    ) -> Result<WorkflowMutationOutcome>;

    /// `RespondWorkflowTaskFailed`: fail the workflow task identified by
    /// `token`, or — for cause `GrpcMessageTooLarge` — force-close-terminate
    /// the run (`respondworkflowtaskfailed/api.go:88 @ v1.31.0`). Defaulted so
    /// workflow-task-only test doubles need not implement it.
    async fn fail_workflow_task(
        &self,
        token: tokeira_types::WorkflowTaskToken,
        failure_cause: tokeira_kernel::WorkflowTaskFailedCause,
        failure_details: Option<tokeira_types::Payload>,
        worker_identity: tokeira_types::WorkerIdentity,
        request: tokeira_types::RequestContext,
        now: time::OffsetDateTime,
    ) -> Result<()> {
        let _ = (
            token,
            failure_cause,
            failure_details,
            worker_identity,
            request,
            now,
        );
        Err(anyhow::anyhow!(
            "fail_workflow_task is not supported by this runtime"
        ))
    }

    async fn poll_activity_task(
        &self,
        queue: tokeira_types::QueueKey,
        worker_identity: tokeira_types::WorkerIdentity,
        timeout: std::time::Duration,
    ) -> Result<Option<StartedActivityTask>>;

    async fn try_claim_activity_task(
        &self,
        queue: tokeira_types::QueueKey,
        run_key: RunKey,
        activity_id: String,
        worker_identity: tokeira_types::WorkerIdentity,
    ) -> Result<Option<StartedActivityTask>> {
        let _ = (queue, run_key, activity_id, worker_identity);
        Ok(None)
    }

    async fn complete_activity_task(
        &self,
        token: ActivityTaskToken,
        result: Payloads,
        worker_identity: Option<tokeira_types::WorkerIdentity>,
    ) -> Result<WorkflowMutationOutcome>;

    async fn fail_activity_task(
        &self,
        token: ActivityTaskToken,
        failure: Payload,
        failure_error_type: Option<String>,
        is_non_retryable: bool,
        worker_identity: Option<tokeira_types::WorkerIdentity>,
    ) -> Result<()>;

    async fn cancel_activity_task(
        &self,
        token: ActivityTaskToken,
        details: Option<Payloads>,
        worker_identity: Option<tokeira_types::WorkerIdentity>,
    ) -> Result<WorkflowMutationOutcome> {
        let _ = (token, details, worker_identity);
        Err(anyhow!("cancel_activity_task is not implemented"))
    }

    async fn record_activity_heartbeat(
        &self,
        token: ActivityTaskToken,
        details: Option<Payloads>,
    ) -> Result<bool>;

    async fn resolve_activity_token(
        &self,
        run_key: RunKey,
        activity_id: &str,
    ) -> std::result::Result<ActivityTaskToken, ActivityTokenResolutionError> {
        let _ = activity_id;
        Err(ActivityTokenResolutionError::RunNotFound { run_key })
    }

    async fn update_activity_options(
        &self,
        run_key: RunKey,
        req: KernelUpdateActivityOptionsRequest,
    ) -> Result<WorkflowMutationOutcome> {
        let _ = (run_key, req);
        Err(anyhow!("update_activity_options is not implemented"))
    }

    async fn terminate_workflow(
        &self,
        run_key: RunKey,
        req: TerminateRequest,
    ) -> Result<WorkflowMutationOutcome>;

    /// Apply an `UpdateWorkflowExecutionOptions` change (currently the
    /// `versioning_override`) to a running execution. Defaults to unimplemented so test
    /// doubles need no change; the runtime adapter overrides it.
    async fn update_workflow_execution_options(
        &self,
        run_key: RunKey,
        versioning_override: FieldChange<tokeira_kernel::VersioningOverride>,
        request: RequestContext,
    ) -> Result<WorkflowMutationOutcome> {
        let _ = (run_key, versioning_override, request);
        Err(anyhow!(
            "update_workflow_execution_options is not implemented"
        ))
    }

    async fn cancel_workflow(
        &self,
        run_key: RunKey,
        req: CancelRequest,
    ) -> Result<WorkflowMutationOutcome>;

    async fn pause_workflow(
        &self,
        run_key: RunKey,
        req: tokeira_kernel::PauseWorkflowRequest,
    ) -> Result<WorkflowMutationOutcome> {
        let _ = (run_key, req);
        Err(anyhow!("pause_workflow is not implemented"))
    }

    async fn unpause_workflow(
        &self,
        run_key: RunKey,
        req: tokeira_kernel::UnpauseWorkflowRequest,
    ) -> Result<WorkflowMutationOutcome> {
        let _ = (run_key, req);
        Err(anyhow!("unpause_workflow is not implemented"))
    }

    async fn reset_workflow(
        &self,
        execution: ExecutionRef,
        req: ResetRequest,
    ) -> Result<ResetWorkflowResult>;

    async fn query_workflow(
        &self,
        execution: ExecutionRef,
        query_type: String,
        query_args: Payloads,
        timeout: std::time::Duration,
    ) -> Result<QueryResult>;

    async fn update_workflow(
        &self,
        execution: ExecutionRef,
        update_id: String,
        update_name: String,
        input: Payloads,
        request: RequestContext,
        timeout: std::time::Duration,
        wait_policy: UpdateWaitPolicy,
    ) -> Result<UpdateLifecycleSnapshot>;

    async fn poll_workflow_update(
        &self,
        execution: ExecutionRef,
        update_id: String,
        wait_policy: UpdateWaitPolicy,
        timeout: std::time::Duration,
    ) -> Result<UpdateLifecycleSnapshot> {
        let _ = (execution, update_id, wait_policy, timeout);
        Err(anyhow!("poll_workflow_update is not implemented"))
    }

    async fn pending_update_transports(
        &self,
        run_key: RunKey,
    ) -> Result<Vec<PendingUpdateTransport>>;

    async fn resolve_update_transport(
        &self,
        run_key: RunKey,
        update_id: String,
        resolution: UpdateTransportResolution,
    ) -> Result<bool>;

    /// Read the update_name and input for a registered update.
    async fn peek_update_info(
        &self,
        run_key: RunKey,
        update_id: String,
    ) -> Result<Option<(String, Payloads)>>;

    async fn resolve_nexus_operation(
        &self,
        run_key: RunKey,
        operation_id: String,
        scheduled_event_id: i64,
        resolution: NexusResolution,
    ) -> Result<bool>;
}

/// Runtime-facing Worker Deployment API consumed by the edge handlers.
///
/// The edge owns protobuf/defaulting/status translation; the runtime registry owns
/// durable deployment state and CAS validation. This trait keeps that split explicit so
/// future handlers can be tested against fakes without reaching into storage tables.
#[async_trait]
pub trait WorkerDeploymentRuntimeApi: Send + Sync + 'static {
    async fn create_worker_deployment(
        &self,
        req: CreateDeployment,
    ) -> EdgeResult<DeploymentMutationOutcome<DeploymentView>>;

    async fn describe_worker_deployment(&self, key: DeploymentKey) -> EdgeResult<DeploymentView>;

    async fn delete_worker_deployment(&self, req: DeleteDeployment) -> EdgeResult<()>;

    async fn list_worker_deployments(&self, req: ListDeployments) -> EdgeResult<DeploymentPage>;

    async fn create_worker_deployment_version(&self, req: CreateVersion) -> EdgeResult<()>;

    async fn describe_worker_deployment_version(
        &self,
        req: DescribeVersion,
    ) -> EdgeResult<VersionView>;

    async fn delete_worker_deployment_version(&self, req: DeleteVersion) -> EdgeResult<()>;

    async fn set_worker_deployment_current_version(
        &self,
        req: SetCurrent,
    ) -> EdgeResult<DeploymentMutationOutcome<SetCurrentOutcome>>;

    async fn set_worker_deployment_ramping_version(
        &self,
        req: SetRamping,
    ) -> EdgeResult<DeploymentMutationOutcome<SetRampingOutcome>>;

    async fn update_worker_deployment_version_compute_config(
        &self,
        req: UpdateComputeConfig,
    ) -> EdgeResult<()>;

    async fn validate_worker_deployment_version_compute_config(
        &self,
        req: ValidateComputeConfig,
    ) -> EdgeResult<()>;

    async fn update_worker_deployment_version_metadata(
        &self,
        req: UpdateMetadata,
    ) -> EdgeResult<VersionMetadataView>;

    async fn set_worker_deployment_manager(
        &self,
        req: SetManager,
    ) -> EdgeResult<DeploymentMutationOutcome<SetManagerOutcome>>;

    /// Lazily register the deployment/version implied by a versioned worker poll.
    /// A no-op for unversioned polls. Idempotent.
    async fn register_polled_deployment(&self, req: RegisterPolledDeployment) -> EdgeResult<()>;

    /// Apply a `sync-drainage-status` signal addressed to a version entity
    /// workflow onto the registry. No-op for an absent deployment/version or a
    /// version that is currently Current/Ramping.
    async fn apply_version_drainage(
        &self,
        namespace_id: tokeira_types::NamespaceId,
        deployment_name: DeploymentName,
        build_id: tokeira_storage::BuildId,
        status: tokeira_storage::VersionDrainageStatus,
    ) -> EdgeResult<()>;

    /// Resolve the Worker Deployment versioning view for one task queue, for
    /// `DescribeTaskQueue.versioning_info`. `None` when no version polls it.
    async fn task_queue_versioning(
        &self,
        namespace_id: tokeira_types::NamespaceId,
        task_queue: String,
    ) -> EdgeResult<Option<TaskQueueVersioningView>>;
}

#[async_trait]
pub trait ExecutionResolver: Send + Sync + 'static {
    async fn current_run_key(&self, namespace: &str, workflow_id: &str) -> Result<Option<RunKey>>;

    async fn describe_execution(
        &self,
        namespace: &str,
        workflow_id: &str,
        run_id: Option<RunId>,
    ) -> Result<Option<WorkflowExecutionDescription>>;
}

// Visibility API re-exported from tokeira-projection (the authoritative owner).
pub use tokeira_projection::{EmptyVisibilityApi, VisibilityApi};

#[derive(Debug, Default)]
pub struct InMemoryExecutionResolver {
    current: tokio::sync::RwLock<std::collections::HashMap<(String, String), RunKey>>,
    descriptions: tokio::sync::RwLock<
        std::collections::HashMap<(String, String), WorkflowExecutionDescription>,
    >,
    descriptions_by_run: tokio::sync::RwLock<
        std::collections::HashMap<(String, String, String), WorkflowExecutionDescription>,
    >,
}

impl InMemoryExecutionResolver {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn set_current_run(
        &self,
        namespace: impl Into<String>,
        workflow_id: impl Into<String>,
        run_key: RunKey,
    ) {
        self.current
            .write()
            .await
            .insert((namespace.into(), workflow_id.into()), run_key);
    }

    pub async fn set_description(&self, description: WorkflowExecutionDescription) {
        let run_id = description.run_id.0.to_string();
        self.descriptions.write().await.insert(
            (
                description.namespace.clone(),
                description.workflow_id.clone(),
            ),
            description.clone(),
        );
        self.descriptions_by_run.write().await.insert(
            (
                description.namespace.clone(),
                description.workflow_id.clone(),
                run_id,
            ),
            description,
        );
    }
}

#[async_trait]
impl ExecutionResolver for InMemoryExecutionResolver {
    async fn current_run_key(&self, namespace: &str, workflow_id: &str) -> Result<Option<RunKey>> {
        Ok(self
            .current
            .read()
            .await
            .get(&(namespace.to_string(), workflow_id.to_string()))
            .copied())
    }

    async fn describe_execution(
        &self,
        namespace: &str,
        workflow_id: &str,
        run_id: Option<RunId>,
    ) -> Result<Option<WorkflowExecutionDescription>> {
        if let Some(run_id) = run_id {
            return Ok(self
                .descriptions_by_run
                .read()
                .await
                .get(&(
                    namespace.to_string(),
                    workflow_id.to_string(),
                    run_id.0.to_string(),
                ))
                .cloned());
        }
        Ok(self
            .descriptions
            .read()
            .await
            .get(&(namespace.to_string(), workflow_id.to_string()))
            .cloned())
    }
}

#[derive(Clone)]
pub struct WorkflowService {
    runtime: Arc<dyn WorkflowRuntimeApi>,
    worker_deployments: Option<Arc<dyn WorkerDeploymentRuntimeApi>>,
    resolver: Arc<dyn ExecutionResolver>,
    visibility: Arc<dyn VisibilityApi>,
    repo: Arc<dyn RunRepository>,
    operator_api: Arc<dyn OperatorApi>,
    namespaces: Arc<dyn NamespaceCache>,
    interceptors: Arc<EdgeInterceptors>,
    poller_registry: PollerRegistry,
    pending_queries: PendingQueryStore,
    buffered_queries: BufferedQueryRegistry,
    broker: InMemoryBroker,
    nexus_broker: NexusTaskBroker,
    long_polls: LongPollGate,
    router: Arc<dyn EdgeRouter>,
    history_waiters: HistoryWaitRegistry,
    versioning_rule_store: Arc<VersioningRuleStore>,
    worker_registry: WorkerRegistry,
    heartbeat_store: Arc<dyn HeartbeatStore>,
    schedule_store: Arc<ScheduleStore>,
    task_queue_config_store: Arc<dyn TaskQueueConfigStore>,
    batch_store: Arc<BatchOperationStore>,
    eager_dispatch_config: EagerDispatchConfig,
}

impl std::fmt::Debug for WorkflowService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowService").finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EagerDispatchConfig {
    pub max_eager_activity_tasks_per_response: usize,
}

impl Default for EagerDispatchConfig {
    fn default() -> Self {
        Self {
            max_eager_activity_tasks_per_response: 3,
        }
    }
}

fn system_capabilities_with_matrix_overlay(
    mut capabilities: SystemCapabilities,
) -> SystemCapabilities {
    // TODO(temporal-compatibility): once the matrix covers all capabilities with
    // conformance evidence, remove the hardcoded baseline and derive entirely
    // from FEATURE_MATRIX. Until then, the matrix overlay only restricts
    // capabilities that are explicitly Stubbed/Unsupported AND were already false.
    for feature in FEATURE_MATRIX {
        for field in feature.capability_fields() {
            apply_matrix_capability_field(&mut capabilities, field, feature.state);
        }
    }
    capabilities
}

fn apply_matrix_capability_field(
    _capabilities: &mut SystemCapabilities,
    field: &str,
    state: FeatureState,
) {
    if !matches!(state, FeatureState::Stubbed | FeatureState::Unsupported) {
        return;
    }

    match field {
        "signal_and_query_header"
        | "internal_error_differentiation"
        | "activity_failure_include_heartbeat"
        | "supports_schedules"
        | "encoded_failure_attributes"
        | "build_id_based_versioning"
        | "upsert_memo"
        | "eager_workflow_start"
        | "sdk_metadata"
        | "count_group_by_execution_status"
        | "nexus"
        | "server_scaled_deployments"
        | "worker_heartbeats" => {}
        _ => {}
    }
}

impl WorkflowService {
    async fn observe_edge_call<T, F>(
        &self,
        headers: &HeaderMap,
        method: &'static str,
        namespace: Option<&str>,
        workflow_id: Option<&str>,
        fut: F,
    ) -> EdgeResult<T>
    where
        F: Future<Output = EdgeResult<T>>,
    {
        let _active = edge_metrics::track_grpc_active_request(method);
        let namespace = namespace.unwrap_or_default().to_string();
        let started = Instant::now();
        let result = tracing_interceptor::instrument_grpc_call(
            headers,
            method,
            if namespace.is_empty() {
                None
            } else {
                Some(namespace.as_str())
            },
            workflow_id,
            fut,
        )
        .await;
        let status = if result.is_ok() { "ok" } else { "error" };
        edge_metrics::record_grpc_request(method, &namespace, status);
        edge_metrics::record_grpc_request_duration(method, &namespace, started.elapsed());
        if let Err(error) = &result {
            edge_metrics::record_grpc_error(method, &namespace, grpc_error_code(error));
        }
        result
    }

    pub fn new(
        runtime: Arc<dyn WorkflowRuntimeApi>,
        resolver: Arc<dyn ExecutionResolver>,
        visibility: Arc<dyn VisibilityApi>,
        repo: Arc<dyn RunRepository>,
        operator_api: Arc<dyn OperatorApi>,
        namespaces: Arc<dyn NamespaceCache>,
        interceptors: Arc<EdgeInterceptors>,
        poller_registry: PollerRegistry,
        pending_queries: PendingQueryStore,
        broker: InMemoryBroker,
        long_polls: LongPollGate,
        router: Arc<dyn EdgeRouter>,
    ) -> Self {
        Self::new_with_versioning_and_buffered_queries_and_history_wait_registry(
            runtime,
            resolver,
            visibility,
            repo,
            operator_api,
            namespaces,
            interceptors,
            poller_registry,
            pending_queries,
            BufferedQueryRegistry::default(),
            broker,
            NexusTaskBroker::default(),
            long_polls,
            router,
            HistoryWaitRegistry::default(),
            Arc::new(VersioningRuleStore::default()),
            WorkerRegistry::default(),
            Arc::new(tokeira_runtime::InMemoryHeartbeatStore::default()),
            Arc::new(ScheduleStore::default()),
            Arc::new(tokeira_runtime::InMemoryTaskQueueConfigStore::default()),
            Arc::new(BatchOperationStore::default()),
        )
    }

    pub fn new_with_buffered_queries(
        runtime: Arc<dyn WorkflowRuntimeApi>,
        resolver: Arc<dyn ExecutionResolver>,
        visibility: Arc<dyn VisibilityApi>,
        repo: Arc<dyn RunRepository>,
        operator_api: Arc<dyn OperatorApi>,
        namespaces: Arc<dyn NamespaceCache>,
        interceptors: Arc<EdgeInterceptors>,
        poller_registry: PollerRegistry,
        pending_queries: PendingQueryStore,
        buffered_queries: BufferedQueryRegistry,
        broker: InMemoryBroker,
        long_polls: LongPollGate,
        router: Arc<dyn EdgeRouter>,
    ) -> Self {
        Self::new_with_versioning_and_buffered_queries_and_history_wait_registry(
            runtime,
            resolver,
            visibility,
            repo,
            operator_api,
            namespaces,
            interceptors,
            poller_registry,
            pending_queries,
            buffered_queries,
            broker,
            NexusTaskBroker::default(),
            long_polls,
            router,
            HistoryWaitRegistry::default(),
            Arc::new(VersioningRuleStore::default()),
            WorkerRegistry::default(),
            Arc::new(tokeira_runtime::InMemoryHeartbeatStore::default()),
            Arc::new(ScheduleStore::default()),
            Arc::new(tokeira_runtime::InMemoryTaskQueueConfigStore::default()),
            Arc::new(BatchOperationStore::default()),
        )
    }

    pub fn new_with_history_wait_registry(
        runtime: Arc<dyn WorkflowRuntimeApi>,
        resolver: Arc<dyn ExecutionResolver>,
        visibility: Arc<dyn VisibilityApi>,
        repo: Arc<dyn RunRepository>,
        operator_api: Arc<dyn OperatorApi>,
        namespaces: Arc<dyn NamespaceCache>,
        interceptors: Arc<EdgeInterceptors>,
        poller_registry: PollerRegistry,
        pending_queries: PendingQueryStore,
        broker: InMemoryBroker,
        long_polls: LongPollGate,
        router: Arc<dyn EdgeRouter>,
        history_waiters: HistoryWaitRegistry,
    ) -> Self {
        Self::new_with_versioning_and_buffered_queries_and_history_wait_registry(
            runtime,
            resolver,
            visibility,
            repo,
            operator_api,
            namespaces,
            interceptors,
            poller_registry,
            pending_queries,
            BufferedQueryRegistry::default(),
            broker,
            NexusTaskBroker::default(),
            long_polls,
            router,
            history_waiters,
            Arc::new(VersioningRuleStore::default()),
            WorkerRegistry::default(),
            Arc::new(tokeira_runtime::InMemoryHeartbeatStore::default()),
            Arc::new(ScheduleStore::default()),
            Arc::new(tokeira_runtime::InMemoryTaskQueueConfigStore::default()),
            Arc::new(BatchOperationStore::default()),
        )
    }

    pub fn new_with_versioning_and_buffered_queries_and_history_wait_registry(
        runtime: Arc<dyn WorkflowRuntimeApi>,
        resolver: Arc<dyn ExecutionResolver>,
        visibility: Arc<dyn VisibilityApi>,
        repo: Arc<dyn RunRepository>,
        operator_api: Arc<dyn OperatorApi>,
        namespaces: Arc<dyn NamespaceCache>,
        interceptors: Arc<EdgeInterceptors>,
        poller_registry: PollerRegistry,
        pending_queries: PendingQueryStore,
        buffered_queries: BufferedQueryRegistry,
        broker: InMemoryBroker,
        nexus_broker: NexusTaskBroker,
        long_polls: LongPollGate,
        router: Arc<dyn EdgeRouter>,
        history_waiters: HistoryWaitRegistry,
        versioning_rule_store: Arc<VersioningRuleStore>,
        worker_registry: WorkerRegistry,
        heartbeat_store: Arc<dyn HeartbeatStore>,
        schedule_store: Arc<ScheduleStore>,
        task_queue_config_store: Arc<dyn TaskQueueConfigStore>,
        batch_store: Arc<BatchOperationStore>,
    ) -> Self {
        Self {
            runtime,
            worker_deployments: None,
            resolver,
            visibility,
            repo,
            operator_api,
            namespaces,
            interceptors,
            poller_registry,
            pending_queries,
            buffered_queries,
            broker,
            nexus_broker,
            long_polls,
            router,
            history_waiters,
            versioning_rule_store,
            worker_registry,
            heartbeat_store,
            schedule_store,
            task_queue_config_store,
            batch_store,
            eager_dispatch_config: EagerDispatchConfig::default(),
        }
    }

    pub fn with_eager_dispatch_config(
        mut self,
        eager_dispatch_config: EagerDispatchConfig,
    ) -> Self {
        self.eager_dispatch_config = eager_dispatch_config;
        self
    }

    /// Attach the runtime-backed Worker Deployment registry API.
    ///
    /// Most tests and legacy deployments do not configure v2 Worker Deployment storage. Keeping
    /// this as an explicit attachment prevents accidental calls from silently constructing an
    /// in-memory registry that would not match production durability.
    pub fn with_worker_deployment_runtime(
        mut self,
        runtime: Arc<dyn WorkerDeploymentRuntimeApi>,
    ) -> Self {
        self.worker_deployments = Some(runtime);
        self
    }

    pub fn worker_deployment_runtime(&self) -> EdgeResult<Arc<dyn WorkerDeploymentRuntimeApi>> {
        self.worker_deployments.clone().ok_or_else(|| {
            EdgeError::FailedPrecondition(
                "worker deployment registry is not configured for this service".to_string(),
            )
        })
    }

    pub fn versioning_rule_store(&self) -> Arc<VersioningRuleStore> {
        self.versioning_rule_store.clone()
    }

    pub fn worker_registry(&self) -> WorkerRegistry {
        self.worker_registry.clone()
    }

    pub fn heartbeat_store(&self) -> Arc<dyn HeartbeatStore> {
        self.heartbeat_store.clone()
    }

    pub fn with_heartbeat_store(mut self, heartbeat_store: Arc<dyn HeartbeatStore>) -> Self {
        self.heartbeat_store = heartbeat_store;
        self
    }

    pub fn schedule_store(&self) -> Arc<ScheduleStore> {
        self.schedule_store.clone()
    }

    pub fn task_queue_config_store(&self) -> Arc<dyn TaskQueueConfigStore> {
        self.task_queue_config_store.clone()
    }

    pub fn batch_store(&self) -> Arc<BatchOperationStore> {
        self.batch_store.clone()
    }

    pub async fn resolve_namespace_id(
        &self,
        namespace: &str,
    ) -> EdgeResult<tokeira_types::NamespaceId> {
        match self
            .namespaces
            .get(namespace)
            .await
            .map_err(EdgeError::from)?
        {
            Some(resolved) if !resolved.deleted => {
                Ok(to_internal::namespace_id_for(&resolved.name))
            }
            Some(_) => Err(EdgeError::NamespaceDeleted(namespace.to_string())),
            None => Err(EdgeError::NamespaceNotFound(namespace.to_string())),
        }
    }

    /// Reject a Start request whose search attributes include any key not
    /// registered for the namespace (system predefined or custom). Returns the
    /// verbatim v1.31.0 admission error for the first unknown key
    /// (`InvalidArgument "search attribute <key> is not defined"`,
    /// `common/searchattribute/validator.go:101 @ v1.31.0`;
    /// `standalone_activity_test.go:521`). A no-op when there are no keys or the
    /// deployment has no search-attribute registry (permissive default).
    pub async fn validate_search_attribute_keys(
        &self,
        namespace_id: tokeira_types::NamespaceId,
        keys: &[String],
    ) -> EdgeResult<()> {
        if keys.is_empty() {
            return Ok(());
        }
        if let Some(unknown) = self
            .visibility
            .unknown_search_attribute(namespace_id, keys)
            .await
            .map_err(EdgeError::from)?
        {
            return Err(EdgeError::BadRequest(format!(
                "search attribute {unknown} is not defined"
            )));
        }
        Ok(())
    }

    pub async fn poll_nexus_task_queue(
        &self,
        headers: &HeaderMap,
        req: crate::translate::nexus::PollNexusTaskQueueRequest,
    ) -> EdgeResult<Option<crate::translate::nexus::PollNexusTaskQueueResponse>> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "poll_nexus_task_queue",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::PollNexusTaskQueue,
                        true,
                    )
                    .await?;

                ensure_local(
                    self.router
                        .route_task_queue(&req.namespace, &req.task_queue, TaskKind::Workflow)
                        .await?,
                )?;

                let _permit = self.long_polls.acquire().await?;
                let (namespace_id, task_queue) =
                    crate::translate::nexus::broker_queue(&req.namespace, &req.task_queue);
                let task = self
                    .nexus_broker
                    .poll(namespace_id, task_queue, std::time::Duration::from_secs(60))
                    .await;

                match task {
                    Some(task) => {
                        // A Nexus task handed to a worker — the dispatch equivalent of
                        // v1.31.0's matching `nexus_task_requests`.
                        crate::metrics::record_nexus_task_request(&req.namespace, "dispatched");
                        Ok(Some(crate::translate::nexus::PollNexusTaskQueueResponse {
                            task_token: task.token.encode().map_err(EdgeError::from)?,
                            request: task.request,
                        }))
                    }
                    None => {
                        crate::metrics::record_nexus_task_request(&req.namespace, "timeout");
                        Ok(None)
                    }
                }
            },
        )
        .await
    }

    pub async fn respond_nexus_task_completed(
        &self,
        headers: &HeaderMap,
        req: crate::translate::nexus::RespondNexusTaskCompletedRequest,
    ) -> EdgeResult<()> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "respond_nexus_task_completed",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::RespondNexusTaskCompleted,
                        false,
                    )
                    .await?;

                if req.task_token.is_empty() {
                    return Err(EdgeError::BadRequest("task_token is required".to_string()));
                }
                let token = NexusTaskToken::decode(&req.task_token)
                    .map_err(|error| EdgeError::BadRequest(error.to_string()))?;
                let response = req
                    .response
                    .ok_or_else(|| EdgeError::BadRequest("response is required".to_string()))?;
                // The worker's response is the terminal result of a caller-side outbound
                // StartOperation/CancelOperation; record the `nexus_outbound_requests`
                // outcome from it. Latency is not recorded at this resolution point — the
                // dispatch wall-clock is not carried on the task token, so an honest duration
                // is unavailable here; the External-endpoint arm records both counter and
                // latency where it can measure the round trip directly.
                if let Some(tags) =
                    crate::translate::nexus::nexus_completed_outbound_tags(&response)
                {
                    tokeira_runtime::metrics::record_nexus_outbound_request(
                        &req.namespace,
                        tags.method,
                        tags.failure_source,
                        &tags.outcome,
                    );
                }
                // Load the pending op so an operation-unsuccessful response can be wrapped
                // in NexusOperationFailureInfo (endpoint/service/operation), exactly as the
                // worker handler-error path does. A missing/raced pending op leaves them
                // empty — the inner cause chain the SDK decodes is still intact.
                let op_ctx = match self
                    .repo
                    .load_run(token.run_key)
                    .await
                    .map_err(EdgeError::from)?
                {
                    LoadedRun::Existing(state) => state
                        .pending_nexus_operations
                        .get(&token.operation_id)
                        .map(|op| crate::translate::nexus::NexusOperationContext {
                            endpoint: op.endpoint.clone(),
                            service: op.service.clone(),
                            operation: op.operation.clone(),
                            scheduled_event_id: token.scheduled_event_id,
                        })
                        .unwrap_or_default(),
                    LoadedRun::Absent => Default::default(),
                };
                let resolution = crate::translate::nexus::proto_response_to_resolution(
                    response,
                    &token.operation_id,
                    &op_ctx,
                )
                .map_err(|error| EdgeError::BadRequest(error.to_string()))?;

                // A cancel-ack (None) does not resolve the operation — the operation resolves
                // only via its completion when the backing workflow closes (v1.31.0 decouples
                // EventCancelationSucceeded from operation resolution, statemachine.go:671).
                let Some(resolution) = resolution else {
                    return Ok(());
                };

                let applied = self
                    .runtime
                    .resolve_nexus_operation(
                        token.run_key,
                        token.operation_id.clone(),
                        token.scheduled_event_id,
                        resolution,
                    )
                    .await
                    .map_err(EdgeError::from)?;
                if applied {
                    self.notify_history_run_key(
                        token.run_key,
                        read_last_event_id(self.repo.as_ref(), token.run_key).await?,
                    )
                    .await;
                }

                Ok(())
            },
        )
        .await
    }

    pub async fn respond_nexus_task_failed(
        &self,
        headers: &HeaderMap,
        req: crate::translate::nexus::RespondNexusTaskFailedRequest,
    ) -> EdgeResult<()> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "respond_nexus_task_failed",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::RespondNexusTaskFailed,
                        false,
                    )
                    .await?;

                if req.task_token.is_empty() {
                    return Err(EdgeError::BadRequest("task_token is required".to_string()));
                }
                let token = NexusTaskToken::decode(&req.task_token)
                    .map_err(|error| EdgeError::BadRequest(error.to_string()))?;
                // A failed worker response is the terminal result of an outbound
                // StartOperation (a worker-reported handler error); capture its
                // `nexus_outbound_requests` outcome before the failure is consumed into the
                // resolution below.
                let outbound_tags = crate::translate::nexus::nexus_failed_outbound_tags(
                    req.failure.as_ref(),
                    req.error.as_ref(),
                );
                // Prefer the v1.62 structured `failure` (field 5) modern SDKs send;
                // fall back to the deprecated `error` (field 4). v1.31.0 requires
                // one of them, and a `failure` must carry a NexusHandlerFailureInfo
                // (`workflow_handler.go:6096 @ v1.31.0`).
                let resolution = if let Some(failure) = req.failure {
                    if !crate::translate::nexus::failure_has_nexus_handler_info(&failure) {
                        return Err(EdgeError::BadRequest(
                            "request Failure must contain error or failure with NexusHandlerFailureInfo"
                                .to_string(),
                        ));
                    }
                    // Load the pending op once: it supplies both the NexusOperationFailureInfo
                    // wrap context (endpoint/service/operation) for a terminal failure AND the
                    // backoff inputs (attempt/scheduled_at/schedule-to-close) for a retryable
                    // one. A missing pending op (already resolved/raced) forces a terminal
                    // resolution — there is nothing to back off.
                    let pending = match self
                        .repo
                        .load_run(token.run_key)
                        .await
                        .map_err(EdgeError::from)?
                    {
                        LoadedRun::Existing(state) => {
                            state.pending_nexus_operations.get(&token.operation_id).cloned()
                        }
                        LoadedRun::Absent => None,
                    };
                    // v1.31.0 (`components/nexusoperations/executors.go:499-532`): a *retryable*
                    // handler error backs the operation off (BACKING_OFF) — it stays pending
                    // with the failure on LastAttemptFailure — while a non-retryable one (or a
                    // retry past schedule-to-close) fails the operation terminally.
                    let next_attempt_at = if crate::translate::nexus::nexus_handler_failure_retryable(
                        &failure,
                    ) {
                        pending.as_ref().and_then(|op| {
                            tokeira_runtime::nexus::nexus_operation_next_attempt_at(
                                op.attempt,
                                op.scheduled_at,
                                op.schedule_to_close_timeout,
                                OffsetDateTime::now_utc(),
                            )
                        })
                    } else {
                        None
                    };
                    match next_attempt_at {
                        Some(next_attempt_at) => NexusResolution::AttemptFailed {
                            // LastAttemptFailure is the handler's own failure (the Describe
                            // surface), NOT the terminal NexusOperationFailureInfo wrapper.
                            failure: tokeira_proto::conversions::common::failure_to_payload(
                                &failure,
                            ),
                            next_attempt_at,
                        },
                        None => {
                            let (endpoint, service, operation) = pending
                                .map(|op| (op.endpoint, op.service, op.operation))
                                .unwrap_or_default();
                            crate::translate::nexus::wrap_handler_failure_as_resolution(
                                failure,
                                endpoint,
                                service,
                                operation,
                                token.scheduled_event_id,
                            )
                        }
                    }
                } else {
                    let error = req.error.ok_or_else(|| {
                        EdgeError::BadRequest(
                            "request Failure must contain error or failure with NexusHandlerFailureInfo"
                                .to_string(),
                        )
                    })?;
                    crate::translate::nexus::proto_handler_error_to_resolution(error)
                        .map_err(|error| EdgeError::BadRequest(error.to_string()))?
                };
                tokeira_runtime::metrics::record_nexus_outbound_request(
                    &req.namespace,
                    outbound_tags.method,
                    outbound_tags.failure_source,
                    &outbound_tags.outcome,
                );

                let applied = self
                    .runtime
                    .resolve_nexus_operation(
                        token.run_key,
                        token.operation_id.clone(),
                        token.scheduled_event_id,
                        resolution,
                    )
                    .await
                    .map_err(EdgeError::from)?;
                if applied {
                    self.notify_history_run_key(
                        token.run_key,
                        read_last_event_id(self.repo.as_ref(), token.run_key).await?,
                    )
                    .await;
                }

                Ok(())
            },
        )
        .await
    }

    pub async fn start_batch_operation(
        &self,
        headers: &HeaderMap,
        req: crate::translate::batch::StartBatchOperationRequest,
    ) -> EdgeResult<()> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "start_batch_operation",
            Some(namespace_label.as_str()),
            None,
            async move {
                let ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::StartBatchOperation,
                        false,
                    )
                    .await?;
                let namespace_id = to_internal::namespace_id_for(&req.namespace);
                let identity = if req.operation_params.identity().trim().is_empty() {
                    ctx.principal.subject.clone()
                } else {
                    req.operation_params.identity().to_string()
                };
                let cancellation_token = tokio_util::sync::CancellationToken::new();
                let entry = BatchOperationEntry {
                    job_id: req.job_id.clone(),
                    namespace_id,
                    operation_type: req.operation_type,
                    operation_params: req.operation_params,
                    state: tokeira_runtime::BatchOperationState::Running,
                    start_time: OffsetDateTime::now_utc(),
                    close_time: None,
                    counters: Arc::new(BatchProgressCounters::default()),
                    visibility_query: req.visibility_query,
                    executions: req.executions,
                    reason: req.reason,
                    identity: identity.clone(),
                    max_operations_per_second: req.max_operations_per_second,
                    cancellation_token: cancellation_token.clone(),
                    stop_reason: None,
                    stop_identity: None,
                };
                self.batch_store
                    .create(entry)
                    .map_err(|err| batch_error_to_edge(err, &req.namespace, &req.job_id))?;

                let dispatch_ctx = BatchDispatchContext {
                    namespace_id,
                    namespace_name: req.namespace,
                    identity,
                    edge_context: ctx,
                };
                tokio::spawn(run_batch_operation(
                    self.batch_store.clone(),
                    self.clone(),
                    dispatch_ctx,
                    namespace_id,
                    req.job_id,
                    cancellation_token,
                ));
                Ok(())
            },
        )
        .await
    }

    pub async fn stop_batch_operation(
        &self,
        headers: &HeaderMap,
        req: crate::translate::batch::StopBatchOperationRequest,
    ) -> EdgeResult<()> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "stop_batch_operation",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::StopBatchOperation,
                        false,
                    )
                    .await?;
                self.batch_store
                    .stop(
                        to_internal::namespace_id_for(&req.namespace),
                        &req.job_id,
                        req.reason,
                        req.identity,
                    )
                    .map_err(|err| batch_error_to_edge(err, &req.namespace, &req.job_id))
            },
        )
        .await
    }

    pub async fn describe_batch_operation(
        &self,
        headers: &HeaderMap,
        req: crate::translate::batch::DescribeBatchOperationRequest,
    ) -> EdgeResult<tokeira_runtime::BatchOperationSnapshot> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "describe_batch_operation",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::DescribeBatchOperation,
                        false,
                    )
                    .await?;
                self.batch_store
                    .describe(to_internal::namespace_id_for(&req.namespace), &req.job_id)
                    .map_err(|err| batch_error_to_edge(err, &req.namespace, &req.job_id))
            },
        )
        .await
    }

    pub async fn list_batch_operations(
        &self,
        headers: &HeaderMap,
        req: crate::translate::batch::ListBatchOperationsRequest,
    ) -> EdgeResult<(Vec<tokeira_runtime::BatchOperationInfo>, Option<Vec<u8>>)> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "list_batch_operations",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::ListBatchOperations,
                        false,
                    )
                    .await?;
                Ok(self.batch_store.list(
                    to_internal::namespace_id_for(&req.namespace),
                    req.page_size,
                    &req.next_page_token,
                ))
            },
        )
        .await
    }

    pub(crate) async fn list_workflows_batch_internal(
        &self,
        ctx: &BatchDispatchContext,
        query: Option<String>,
        next_page_token: Option<String>,
    ) -> EdgeResult<ListWorkflowExecutionsResponse> {
        self.visibility
            .list_workflows(ListWorkflowExecutionsRequest {
                namespace: ctx.namespace_name.clone(),
                query,
                page_size: 100,
                next_page_token,
            })
            .await
            .map_err(EdgeError::from)
    }

    pub(crate) async fn terminate_workflow_batch_internal(
        &self,
        ctx: &BatchDispatchContext,
        workflow_ref: &tokeira_runtime::WorkflowExecutionRef,
        details: Option<Payloads>,
        identity: String,
    ) -> EdgeResult<()> {
        ensure_local(
            self.router
                .route_workflow(&ctx.namespace_name, &workflow_ref.workflow_id)
                .await?,
        )?;
        let run_key = self.resolve_batch_run_key(ctx, workflow_ref).await?;
        let outcome = self
            .runtime
            .terminate_workflow(
                run_key,
                TerminateRequest {
                    reason: "batch terminate".to_string(),
                    details,
                    identity,
                    request: batch_request_context(ctx),
                    now: OffsetDateTime::now_utc(),
                },
            )
            .await
            .map_err(EdgeError::from)?;
        self.notify_history_run_key(run_key, outcome.last_event_id)
            .await;
        Ok(())
    }

    pub(crate) async fn cancel_workflow_batch_internal(
        &self,
        ctx: &BatchDispatchContext,
        workflow_ref: &tokeira_runtime::WorkflowExecutionRef,
    ) -> EdgeResult<()> {
        ensure_local(
            self.router
                .route_workflow(&ctx.namespace_name, &workflow_ref.workflow_id)
                .await?,
        )?;
        let run_key = self.resolve_batch_run_key(ctx, workflow_ref).await?;
        let outcome = self
            .runtime
            .cancel_workflow(
                run_key,
                CancelRequest {
                    reason: "batch cancel".to_string(),
                    external_initiator: None,
                    request: batch_request_context(ctx),
                    now: OffsetDateTime::now_utc(),
                },
            )
            .await
            .map_err(EdgeError::from)?;
        self.notify_history_run_key(run_key, outcome.last_event_id)
            .await;
        Ok(())
    }

    pub(crate) async fn signal_workflow_batch_internal(
        &self,
        ctx: &BatchDispatchContext,
        workflow_ref: &tokeira_runtime::WorkflowExecutionRef,
        signal_name: String,
        input: Payloads,
    ) -> EdgeResult<()> {
        ensure_local(
            self.router
                .route_workflow(&ctx.namespace_name, &workflow_ref.workflow_id)
                .await?,
        )?;
        let run_key = self.resolve_batch_run_key(ctx, workflow_ref).await?;
        let outcome = self
            .runtime
            .signal_workflow(
                run_key,
                SignalRequest {
                    signal_name,
                    input,
                    header: None,
                    links: Vec::new(),
                    request: batch_request_context(ctx),
                    now: OffsetDateTime::now_utc(),
                },
            )
            .await
            .map_err(EdgeError::from)?;
        self.notify_history_run_key(run_key, outcome.last_event_id)
            .await;
        Ok(())
    }

    pub(crate) async fn delete_workflow_batch_internal(
        &self,
        ctx: &BatchDispatchContext,
        workflow_ref: &tokeira_runtime::WorkflowExecutionRef,
        identity: String,
    ) -> EdgeResult<()> {
        ensure_local(
            self.router
                .route_workflow(&ctx.namespace_name, &workflow_ref.workflow_id)
                .await?,
        )?;
        let run_key = self.resolve_batch_run_key(ctx, workflow_ref).await?;
        let loaded = self.repo.load_run(run_key).await.map_err(EdgeError::from)?;
        let tokeira_kernel::LoadedRun::Existing(state) = loaded else {
            return Err(EdgeError::WorkflowNotFound {
                namespace: ctx.namespace_name.clone(),
                workflow_id: workflow_ref.workflow_id.clone(),
            });
        };
        if state.status.is_open() {
            let outcome = self
                .runtime
                .terminate_workflow(
                    run_key,
                    TerminateRequest {
                        reason: "deleted via batch operation".to_string(),
                        details: None,
                        identity,
                        request: batch_request_context(ctx),
                        now: OffsetDateTime::now_utc(),
                    },
                )
                .await
                .map_err(EdgeError::from)?;
            self.notify_history_run_key(run_key, outcome.last_event_id)
                .await;
        }
        self.visibility
            .delete_execution(run_key)
            .await
            .map_err(EdgeError::from)
    }

    pub(crate) async fn reset_workflow_batch_internal(
        &self,
        ctx: &BatchDispatchContext,
        workflow_ref: &tokeira_runtime::WorkflowExecutionRef,
        fork_event_id: i64,
        reason: String,
    ) -> EdgeResult<()> {
        ensure_local(
            self.router
                .route_workflow(&ctx.namespace_name, &workflow_ref.workflow_id)
                .await?,
        )?;
        let execution = self.execution_ref_from_batch(ctx, workflow_ref)?;
        let run_key = self.resolve_batch_run_key(ctx, workflow_ref).await?;
        let history = self
            .repo
            .read_history(run_key, 0, usize::MAX)
            .await
            .map_err(EdgeError::from)?;
        validate_reset_target(&history, fork_event_id)?;
        let new_run_id = RunId::new();
        let result = self
            .runtime
            .reset_workflow(
                execution,
                ResetRequest {
                    fork_event_id,
                    new_run_id,
                    reason,
                    request: batch_request_context(ctx),
                    now: OffsetDateTime::now_utc(),
                },
            )
            .await
            .map_err(EdgeError::from)?;
        self.notify_history_run_key(result.successor_run_key, 0)
            .await;
        Ok(())
    }

    pub(crate) async fn resolve_reset_target_batch_internal(
        &self,
        ctx: &BatchDispatchContext,
        workflow_ref: &tokeira_runtime::WorkflowExecutionRef,
        target: &BatchResetTarget,
    ) -> EdgeResult<i64> {
        let run_key = self.resolve_batch_run_key(ctx, workflow_ref).await?;
        let history = self
            .repo
            .read_history(run_key, 0, usize::MAX)
            .await
            .map_err(EdgeError::from)?;
        if let BatchResetTarget::BuildId(build_id) = target {
            let loaded = self.repo.load_run(run_key).await.map_err(EdgeError::from)?;
            let tokeira_kernel::LoadedRun::Existing(state) = loaded else {
                return Err(EdgeError::WorkflowNotFound {
                    namespace: ctx.namespace_name.clone(),
                    workflow_id: workflow_ref.workflow_id.clone(),
                });
            };
            if state
                .build_id
                .as_ref()
                .is_none_or(|value| value.0 != *build_id)
            {
                return Err(EdgeError::BadRequest(format!(
                    "workflow was not processed by build id `{build_id}`"
                )));
            }
            return resolve_reset_target_from_history(
                &history,
                &BatchResetTarget::FirstWorkflowTask,
            );
        }
        resolve_reset_target_from_history(&history, target)
    }

    pub async fn apply_schedule_patch(
        &self,
        namespace_id: tokeira_types::NamespaceId,
        schedule_id: &tokeira_runtime::ScheduleId,
        patch: SchedulePatch,
    ) -> EdgeResult<()> {
        let now = OffsetDateTime::now_utc();
        self.schedule_store
            .describe(namespace_id, schedule_id)
            .map_err(|err| EdgeError::BadRequest(err.to_string()))?;

        self.schedule_store
            .update(namespace_id, schedule_id, &[], |entry| {
                if let Some(note) = patch.pause.clone() {
                    entry.state.paused = true;
                    entry.state.notes = note;
                }
                if let Some(note) = patch.unpause.clone() {
                    entry.state.paused = false;
                    entry.state.notes = note;
                }
            })
            .map_err(|err| EdgeError::BadRequest(err.to_string()))?;

        if let Some(trigger) = patch.trigger_immediately {
            self.handle_schedule_due_action(
                namespace_id,
                schedule_id,
                now,
                Some(trigger.overlap_policy),
                now,
            )
            .await?;
        }
        for backfill in patch.backfill_request {
            let entry = self
                .schedule_store
                .describe(namespace_id, schedule_id)
                .map_err(|err| EdgeError::BadRequest(err.to_string()))?;
            let times = compute_matching_times(
                &entry.spec,
                backfill.start_time,
                backfill.end_time,
                schedule_id,
            );
            for nominal_time in times {
                self.handle_schedule_due_action(
                    namespace_id,
                    schedule_id,
                    nominal_time,
                    Some(backfill.overlap_policy),
                    now,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn handle_schedule_due_action(
        &self,
        namespace_id: tokeira_types::NamespaceId,
        schedule_id: &tokeira_runtime::ScheduleId,
        nominal_time: OffsetDateTime,
        overlap_override: Option<OverlapPolicy>,
        actual_time: OffsetDateTime,
    ) -> EdgeResult<()> {
        let entry = self
            .schedule_store
            .describe(namespace_id, schedule_id)
            .map_err(|err| EdgeError::BadRequest(err.to_string()))?;
        let policy = overlap_override.unwrap_or(entry.policies.overlap_policy);
        match decide_overlap(
            policy,
            &entry.info.running_workflows,
            entry.info.buffered_actions.len(),
        ) {
            OverlapDecision::Allow => {
                self.trigger_scheduled_workflow(
                    namespace_id,
                    schedule_id,
                    nominal_time,
                    actual_time,
                )
                .await
            }
            OverlapDecision::Skip => {
                self.schedule_store
                    .update(namespace_id, schedule_id, &[], |entry| {
                        entry.info.overlap_skipped += 1;
                    })
                    .map_err(|err| EdgeError::BadRequest(err.to_string()))?;
                Ok(())
            }
            OverlapDecision::Buffer => {
                self.schedule_store
                    .update(namespace_id, schedule_id, &[], |entry| {
                        if policy == OverlapPolicy::BufferOne
                            && !entry.info.buffered_actions.is_empty()
                        {
                            entry.info.buffered_actions.pop_front();
                            entry.info.buffer_dropped += 1;
                        }
                        entry
                            .info
                            .buffered_actions
                            .push_back(tokeira_runtime::BufferedAction {
                                nominal_time,
                                overlap_policy_override: overlap_override,
                            });
                    })
                    .map_err(|err| EdgeError::BadRequest(err.to_string()))?;
                Ok(())
            }
            OverlapDecision::CancelOther(workflows) => {
                for workflow in workflows {
                    self.runtime
                        .cancel_workflow(
                            workflow.run_key,
                            CancelRequest {
                                reason: "schedule overlap policy".to_string(),
                                external_initiator: None,
                                request: schedule_request_context(actual_time),
                                now: actual_time,
                            },
                        )
                        .await
                        .map_err(EdgeError::from)?;
                }
                Ok(())
            }
            OverlapDecision::TerminateOther(workflows) => {
                for workflow in workflows {
                    self.runtime
                        .terminate_workflow(
                            workflow.run_key,
                            TerminateRequest {
                                reason: "schedule overlap policy".to_string(),
                                details: Some(Payloads::default()),
                                identity: "schedule-engine".to_string(),
                                request: schedule_request_context(actual_time),
                                now: actual_time,
                            },
                        )
                        .await
                        .map_err(EdgeError::from)?;
                }
                self.schedule_store
                    .update(namespace_id, schedule_id, &[], |entry| {
                        entry.info.running_workflows.clear();
                    })
                    .map_err(|err| EdgeError::BadRequest(err.to_string()))?;
                self.trigger_scheduled_workflow(
                    namespace_id,
                    schedule_id,
                    nominal_time,
                    actual_time,
                )
                .await
            }
        }
    }

    async fn trigger_scheduled_workflow(
        &self,
        namespace_id: tokeira_types::NamespaceId,
        schedule_id: &tokeira_runtime::ScheduleId,
        nominal_time: OffsetDateTime,
        actual_time: OffsetDateTime,
    ) -> EdgeResult<()> {
        let entry = self
            .schedule_store
            .describe(namespace_id, schedule_id)
            .map_err(|err| EdgeError::BadRequest(err.to_string()))?;
        let workflow_id = schedule_workflow_id(
            &entry.action.start_workflow.workflow_id,
            nominal_time,
            entry.policies.keep_original_workflow_id,
        );
        let run_id = RunId::new();
        let run_key = RunKey::derive(namespace_id, &workflow_id, run_id);
        let build_id = self.versioning_rule_store.evaluate_assignment(
            namespace_id,
            &entry.action.start_workflow.task_queue,
            &workflow_id,
        );
        let request = StartRequest {
            run_key,
            namespace_id,
            workflow_id: workflow_id.clone(),
            run_id,
            workflow_type: entry.action.start_workflow.workflow_type.clone(),
            task_queue: entry.action.start_workflow.task_queue.clone(),
            input: entry.action.start_workflow.input.clone(),
            header: None,
            memo: entry.action.start_workflow.memo.clone(),
            search_attributes: entry.action.start_workflow.search_attributes.clone(),
            workflow_execution_timeout: entry.action.start_workflow.workflow_execution_timeout,
            workflow_run_timeout: entry.action.start_workflow.workflow_run_timeout,
            workflow_task_timeout: entry
                .action
                .start_workflow
                .workflow_task_timeout
                .unwrap_or(time::Duration::seconds(10)),
            retry_policy: entry.action.start_workflow.retry_policy.clone(),
            conflict_policy: tokeira_kernel::WorkflowIdConflictPolicy::Fail,
            reuse_policy: tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
            deployment: None,
            build_id,
            versioning_override: None,
            workflow_start_delay: None,
            completion_callbacks: Vec::new(),
            user_metadata: None,
            links: Vec::new(),
            on_conflict_options: None,
            priority: None,
            attempt: 1,
            continued_execution_run_id: None,
            first_execution_run_id: Some(run_id),
            parent_run_key: None,
            parent_workflow_id: None,
            parent_run_id: None,
            parent_namespace_id: None,
            parent_initiated_event_id: 0,
            root_workflow_id: None,
            root_run_id: None,
            original_execution_run_id: Some(run_id),
            continued_failure: None,
            last_completion_result: None,
            first_run_started_at: None,
            request: schedule_request_context(actual_time),
            now: actual_time,
            client_cron_schedule: None,
            cron_schedule: Some(schedule_id.0.clone()),
            reserved_poller_identity: None,
        };
        let outcome = self
            .runtime
            .start_workflow_with_policy(request)
            .await
            .map_err(EdgeError::from)?;
        let result = match outcome {
            StartWorkflowResult::Started {
                run_key, run_id, ..
            } => ScheduleActionResult {
                schedule_time: nominal_time,
                actual_time,
                start_workflow_result: Some(WorkflowExecution {
                    namespace_id,
                    workflow_id,
                    run_id,
                    run_key,
                }),
                start_workflow_status: WorkflowExecutionStatus::Running,
            },
            StartWorkflowResult::Deduped {
                run_key, run_id, ..
            } => ScheduleActionResult {
                schedule_time: nominal_time,
                actual_time,
                start_workflow_result: Some(WorkflowExecution {
                    namespace_id,
                    workflow_id,
                    run_id,
                    run_key,
                }),
                start_workflow_status: WorkflowExecutionStatus::Running,
            },
            StartWorkflowResult::UsedExisting { run_key, run_id }
            | StartWorkflowResult::Rejected { run_key, run_id } => ScheduleActionResult {
                schedule_time: nominal_time,
                actual_time,
                start_workflow_result: Some(WorkflowExecution {
                    namespace_id,
                    workflow_id,
                    run_id,
                    run_key,
                }),
                start_workflow_status: WorkflowExecutionStatus::StartFailed,
            },
        };
        self.schedule_store
            .update(namespace_id, schedule_id, &[], |entry| {
                if let Some(workflow) = result.start_workflow_result.clone()
                    && result.start_workflow_status == WorkflowExecutionStatus::Running
                {
                    entry.info.running_workflows.push(workflow);
                }
                entry.info.action_count += 1;
                entry.info.recent_actions.push(result);
                if entry.info.recent_actions.len() > 10 {
                    entry.info.recent_actions.remove(0);
                }
                entry.info.update_time = actual_time;
            })
            .map_err(|err| EdgeError::BadRequest(err.to_string()))?;
        Ok(())
    }

    pub fn broker(&self) -> InMemoryBroker {
        self.broker.clone()
    }

    pub fn new_with_buffered_queries_and_history_wait_registry(
        runtime: Arc<dyn WorkflowRuntimeApi>,
        resolver: Arc<dyn ExecutionResolver>,
        visibility: Arc<dyn VisibilityApi>,
        repo: Arc<dyn RunRepository>,
        operator_api: Arc<dyn OperatorApi>,
        namespaces: Arc<dyn NamespaceCache>,
        interceptors: Arc<EdgeInterceptors>,
        poller_registry: PollerRegistry,
        pending_queries: PendingQueryStore,
        buffered_queries: BufferedQueryRegistry,
        broker: InMemoryBroker,
        long_polls: LongPollGate,
        router: Arc<dyn EdgeRouter>,
        history_waiters: HistoryWaitRegistry,
    ) -> Self {
        Self::new_with_versioning_and_buffered_queries_and_history_wait_registry(
            runtime,
            resolver,
            visibility,
            repo,
            operator_api,
            namespaces,
            interceptors,
            poller_registry,
            pending_queries,
            buffered_queries,
            broker,
            NexusTaskBroker::default(),
            long_polls,
            router,
            history_waiters,
            Arc::new(VersioningRuleStore::default()),
            WorkerRegistry::default(),
            Arc::new(tokeira_runtime::InMemoryHeartbeatStore::default()),
            Arc::new(ScheduleStore::default()),
            Arc::new(tokeira_runtime::InMemoryTaskQueueConfigStore::default()),
            Arc::new(BatchOperationStore::default()),
        )
    }

    /// Attach buffered queries whose consistency barrier has been met.
    ///
    /// Queries in the `BufferedQueryRegistry` each carry a `required_barrier`
    /// (the `last_event_id` the caller must observe). We only drain queries
    /// whose barrier is at or below `observable_barrier` — this guarantees
    /// the worker will evaluate the query against state that includes the
    /// write the caller was waiting on. Queries whose barrier is still ahead
    /// stay buffered until the next WFT completion advances the watermark.
    async fn attach_buffered_queries(
        &self,
        run_key: RunKey,
        observable_barrier: i64,
        task_token: &[u8],
        target: &mut std::collections::HashMap<String, WorkflowQueryDto>,
    ) {
        for query in self
            .buffered_queries
            .drain_satisfied(run_key, observable_barrier)
        {
            let query_id = Uuid::new_v4().to_string();
            self.pending_queries
                .insert(task_token, query_id.clone(), query.response_tx)
                .await;
            target.insert(
                query_id,
                WorkflowQueryDto {
                    query_type: query.query_type,
                    query_args: query.query_args,
                },
            );
        }
    }

    /// Dispatch barrier-satisfied queries directly through the broker for
    /// runs that are currently quiescent (no pending WFT).
    ///
    /// When a run has no in-flight workflow task, there is no poll response
    /// to piggyback queries onto. Instead we publish each query as a
    /// standalone `QueryTask` through the broker, which will route it to a
    /// poller (preferring the sticky worker if the affinity hasn't expired).
    /// This avoids the query sitting in the buffer indefinitely when no
    /// further mutations are expected.
    async fn dispatch_queries_direct(
        &self,
        run_key: RunKey,
        state: &tokeira_kernel::WorkflowState,
        barrier: i64,
    ) {
        let now = OffsetDateTime::now_utc();
        let sticky_preferred = state.sticky.as_ref().and_then(|affinity| {
            (affinity.expires_at > now).then_some(affinity.worker_identity.clone())
        });
        let sticky_deadline = state
            .sticky
            .as_ref()
            // The kernel stores the SDK sticky
            // `schedule_to_start_timeout` as the affinity expiry. Buffered
            // queries released after WFT completion use that same concrete
            // deadline for sticky-first direct query fallback
            // (`service/history/api/queryworkflow/api.go:350-410 @ v1.31.0`).
            .and_then(|affinity| (affinity.expires_at > now).then_some(affinity.expires_at));
        let queue = QueueKey {
            namespace_id: state.namespace_id,
            task_queue: state.task_queue.clone(),
            task_kind: TaskKind::Workflow,
            deployment: state.deployment.clone(),
            build_id: state.build_id.clone(),
        };

        for query in self.buffered_queries.drain_satisfied(run_key, barrier) {
            self.broker
                .publish_query_task(tokeira_runtime::QueryTask {
                    run_key,
                    query_type: query.query_type,
                    query_args: query.query_args,
                    queue: queue.clone(),
                    sticky_preferred: sticky_preferred.clone(),
                    sticky_deadline,
                    response_tx: query.response_tx,
                })
                .await;
        }
    }

    /// Build a synthetic query-only WFT for eager return.
    ///
    /// When the SDK requests `return_new_workflow_task` and there are
    /// buffered queries ready, we construct a WFT with an empty history
    /// and `started_event_id = 0`. The zero started-event-id signals to
    /// the SDK that this is a query-only evaluation against the current
    /// cached state (sticky evaluation) — no replay is needed. This
    /// eliminates an extra poll round-trip for the common case where a
    /// query arrives just after a WFT completion.
    async fn build_eager_query_workflow_task(
        &self,
        state: &tokeira_kernel::WorkflowState,
        shard_epoch: tokeira_types::ShardEpoch,
        barrier: i64,
    ) -> Option<PollWorkflowTaskQueueResponse> {
        let queries = self
            .buffered_queries
            .drain_satisfied(state.run_key, barrier);
        if queries.is_empty() {
            return None;
        }

        let query_token = tokeira_types::WorkflowTaskToken {
            run_key: state.run_key,
            logical_seq: tokeira_types::LogicalTaskSeq(0),
            started_event_id: 0,
            attempt: 1,
            shard_epoch,
        };

        let task_token = serde_json::to_vec(&query_token).ok()?;
        let mut response = PollWorkflowTaskQueueResponse {
            task_token: task_token.clone(),
            started_event_id: 0,
            previous_started_event_id: state.previous_started_event_id,
            attempt: 1,
            scheduled_time: None,
            started_time: None,
            payload: crate::translate::WorkflowTaskPayloadDto {
                workflow_id: state.workflow_id.0.clone(),
                run_key: state.run_key,
                run_id: state.run_id,
                task_queue: state.task_queue.0.clone(),
                history: Vec::new(),
            },
            query: None,
            queries: std::collections::HashMap::new(),
            messages: Vec::new(),
        };

        for query in queries {
            let query_id = Uuid::new_v4().to_string();
            self.pending_queries
                .insert(&task_token, query_id.clone(), query.response_tx)
                .await;
            response.queries.insert(
                query_id,
                WorkflowQueryDto {
                    query_type: query.query_type,
                    query_args: query.query_args,
                },
            );
        }

        Some(response)
    }

    async fn build_direct_query_poll_response(
        &self,
        query: tokeira_runtime::QueryTask,
        worker: &WorkerIdentity,
    ) -> EdgeResult<PollWorkflowTaskQueueResponse> {
        let state = match self
            .repo
            .load_run(query.run_key)
            .await
            .map_err(EdgeError::from)?
        {
            LoadedRun::Existing(state) => state,
            LoadedRun::Absent => {
                return Err(EdgeError::WorkflowNotFound {
                    namespace: query.queue.namespace_id.0.to_string(),
                    workflow_id: query.run_key.0.to_string(),
                });
            }
        };
        let sticky_match = query.sticky_preferred.as_ref() == Some(worker);
        let history_after_event_id = if sticky_match && state.previous_started_event_id > 0 {
            state.previous_started_event_id
        } else {
            0
        };
        let history = self
            .repo
            .read_history(query.run_key, history_after_event_id, usize::MAX)
            .await
            .map_err(EdgeError::from)?;

        // Temporal returns direct queries as workflow-poll tasks with
        // `started_event_id = 0` and a query task token, because no history
        // event is authored for the read-only query
        // (`proto/upstream/temporal/api/workflowservice/v1/request_response.proto`,
        // `service/matching/matching_engine.go:1084 @ v1.31.0`). The token is
        // opaque to the SDK; the edge keys it to the parked caller in
        // `PendingQueryStore` and resolves it via `RespondQueryTaskCompleted`.
        let task_token = format!(
            "query-task:{}:{}:{}",
            query.queue.namespace_id.0,
            query.queue.task_queue.0,
            Uuid::new_v4()
        )
        .into_bytes();
        self.pending_queries
            .insert(&task_token, LEGACY_QUERY_ID.to_string(), query.response_tx)
            .await;

        Ok(PollWorkflowTaskQueueResponse {
            task_token,
            started_event_id: 0,
            previous_started_event_id: state.previous_started_event_id,
            attempt: 1,
            scheduled_time: None,
            started_time: None,
            payload: crate::translate::WorkflowTaskPayloadDto {
                workflow_id: state.workflow_id.0,
                run_key: state.run_key,
                run_id: state.run_id,
                task_queue: state.task_queue.0,
                history,
            },
            query: Some(WorkflowQueryDto {
                query_type: query.query_type,
                query_args: query.query_args,
            }),
            queries: std::collections::HashMap::new(),
            messages: Vec::new(),
        })
    }

    pub async fn start_workflow_execution(
        &self,
        headers: &HeaderMap,
        req: StartWorkflowExecutionRequest,
    ) -> EdgeResult<StartWorkflowExecutionResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "start_workflow_execution",
            Some(namespace_label.as_str()),
            None,
            async move {
                let eager_requested = req.request_eager_execution;
                let eager_identity = req.identity.clone().map(WorkerIdentity);
                let namespace = req.namespace.clone();
                let workflow_id = req.workflow_id.clone();
                let ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&namespace),
                        Action::StartWorkflowExecution,
                        false,
                    )
                    .await?;

                ensure_local(self.router.route_workflow(&namespace, &workflow_id).await?)?;

                let internal = to_internal::start_request(
                    req,
                    &ctx.request_id,
                    Some(self.versioning_rule_store.as_ref()),
                );
                let eager_queue = tokeira_types::QueueKey {
                    namespace_id: internal.namespace_id,
                    task_queue: internal.task_queue.clone(),
                    task_kind: TaskKind::Workflow,
                    deployment: internal.deployment.clone(),
                    build_id: internal.build_id.clone(),
                };
                let outcome = self
                    .runtime
                    .start_workflow_with_policy(internal.clone())
                    .await
                    .map_err(EdgeError::from)?;
                match outcome {
                    StartWorkflowResult::Started {
                        mutation_metadata, ..
                    } => {
                        self.notify_history_run_key(
                            internal.run_key,
                            mutation_metadata.last_event_id,
                        )
                        .await;
                        let mut response = from_internal::start_response(
                            &internal,
                            WorkflowMutationOutcome {
                                transition_seq: mutation_metadata.transition_seq.0,
                                last_event_id: mutation_metadata.last_event_id,
                                was_duplicate: false,
                                execution_status: mutation_metadata.execution_status,
                                new_run_id: None,
                            },
                        );
                        if eager_requested
                            && let Some(identity) = eager_identity
                            && self
                                .poller_registry
                                .has_active_poller(&eager_queue, &identity)
                            && let Some(started) = self
                                .runtime
                                .try_claim_workflow_task(eager_queue, internal.run_key, identity)
                                .await
                                .map_err(EdgeError::from)?
                        {
                            response.eager_workflow_task = Some(
                                from_internal::poll_response(started, self.repo.as_ref())
                                    .await
                                    .map_err(EdgeError::from)?,
                            );
                        }
                        Ok(response)
                    }
                    StartWorkflowResult::UsedExisting { run_key, run_id } => {
                        // UseExisting attached to a running incumbent rather than
                        // creating a new run. v1.31.0 returns success here — RunId =
                        // the existing run, Started = false, Status = RUNNING — not an
                        // AlreadyStarted error; only the Fail policy errors
                        // (handleUseExistingWorkflowOnConflictOptions vs the Fail arm,
                        // service/history/api/startworkflow/api.go @ v1.31.0). The Nexus
                        // WorkflowRunOperation relies on this: with
                        // WorkflowExecutionErrorWhenAlreadyStarted set, a UseExisting
                        // caller must see success so its operation starts against the
                        // attached run (temporalnexus/operation.go @ sdk v1.41.1).
                        Ok(StartWorkflowExecutionResponse {
                            run_key,
                            run_id,
                            transition_seq: 0,
                            last_event_id: 0,
                            started: false,
                            // Attached to a running incumbent → RUNNING (api.go:343 returns the
                            // incumbent's status, which for UseExisting-on-running is RUNNING).
                            status: ExecutionStatus::Running,
                            // When the attach recorded a WorkflowExecutionOptionsUpdated event
                            // (OnConflictOptions{AttachRequestId}), v1.31.0 returns a RequestIdRef
                            // link to it rather than the EventRef-to-start link
                            // (generateRequestIdRefLink, startworkflow/api.go:660-668/833).
                            attached_request_id: internal
                                .on_conflict_options
                                .as_ref()
                                .filter(|options| options.attach_request_id)
                                .map(|_| internal.request.request_id.0.clone()),
                            eager_workflow_task: None,
                        })
                    }
                    StartWorkflowResult::Deduped {
                        run_key,
                        run_id,
                        execution_status,
                    } => {
                        // A retried start whose RequestId already authored this run's
                        // WorkflowExecutionStarted: v1.31.0 respondToRetriedRequest returns the
                        // existing run with Started=true and the incumbent's Status
                        // (startworkflow/api.go:332-336, 563/567). The EventRef self-link to
                        // event 1 is synthesised by the proto layer from run_id.
                        Ok(StartWorkflowExecutionResponse {
                            run_key,
                            run_id,
                            transition_seq: 0,
                            last_event_id: 0,
                            started: true,
                            status: execution_status,
                            attached_request_id: None,
                            eager_workflow_task: None,
                        })
                    }
                    StartWorkflowResult::Rejected { run_id, .. } => {
                        Err(EdgeError::WorkflowAlreadyStarted {
                            namespace,
                            workflow_id,
                            run_id: run_id.0.to_string(),
                        })
                    }
                }
            },
        )
        .await
    }

    pub async fn signal_workflow_execution(
        &self,
        headers: &HeaderMap,
        req: SignalWorkflowExecutionRequest,
    ) -> EdgeResult<SignalWorkflowExecutionResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "signal_workflow_execution",
            Some(namespace_label.as_str()),
            None,
            async move {
                let ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::SignalWorkflowExecution,
                        false,
                    )
                    .await?;

                // Worker Deployment entity-workflow surface: a `sync-drainage-status`
                // signal addressed to `temporal-sys-worker-deployment-version:<name>:<build>`
                // drives registry drainage state rather than a per-run workflow (Tokeira
                // backs the entity-workflow surface with the registry; see
                // `deployment_registry`). Mirrors the version entity workflow's signal
                // handler (`version_workflow.go:119 @ v1.31.0`). A `force-continue-as-new`
                // signal to either the version or deployment entity is a no-op success:
                // tokeira holds the registry as durable state, so there is no per-run
                // history to continue-as-new. Other signals to these ids fall through to
                // normal routing (and surface NotFound).
                if req.signal_name == crate::grpc::translate::SYNC_DRAINAGE_SIGNAL_NAME
                    && let Some((deployment_name, build_id)) =
                        crate::grpc::translate::parse_worker_deployment_version_workflow_id(
                            &req.workflow_id,
                        )
                {
                    if let Some(worker_deployments) = self.worker_deployments.as_ref() {
                        let status =
                            crate::grpc::translate::decode_version_drainage_status(&req.input)
                                .map_err(|error| EdgeError::BadRequest(error.to_string()))?;
                        worker_deployments
                            .apply_version_drainage(
                                to_internal::namespace_id_for(&req.namespace),
                                tokeira_storage::DeploymentName(deployment_name),
                                tokeira_storage::BuildId(build_id),
                                status,
                            )
                            .await?;
                    }
                    return Ok(SignalWorkflowExecutionResponse {
                        accepted: true,
                        transition_seq: 0,
                        last_event_id: 0,
                    });
                }
                if req.signal_name == crate::grpc::translate::FORCE_CAN_SIGNAL_NAME
                    && crate::grpc::translate::is_worker_deployment_entity_workflow_id(
                        &req.workflow_id,
                    )
                {
                    return Ok(SignalWorkflowExecutionResponse {
                        accepted: true,
                        transition_seq: 0,
                        last_event_id: 0,
                    });
                }

                ensure_local(
                    self.router
                        .route_workflow(&req.namespace, &req.workflow_id)
                        .await?,
                )?;

                // Temporal keys SignalWorkflowExecution by the caller-supplied
                // workflow ID and run ID when present
                // (`service/history/api/signalworkflow/api.go @ v1.31.0`).
                // Empty run_id keeps the SDK-compatible current-run fallback;
                // a non-empty malformed run_id must fail before lookup.
                let run_key = self
                    .resolve_execution_run_key(
                        &req.namespace,
                        &req.workflow_id,
                        req.run_id.as_deref(),
                    )
                    .await?;

                let internal = to_internal::signal_request(req, &ctx.request_id);
                let outcome = self
                    .runtime
                    .signal_workflow(run_key, internal)
                    .await
                    .map_err(EdgeError::from)?;
                self.notify_history_run_key(run_key, outcome.last_event_id)
                    .await;

                Ok(from_internal::signal_response(outcome))
            },
        )
        .await
    }

    /// Poll for a workflow task, attaching buffered queries and pending
    /// update messages to the response.
    ///
    /// After the runtime returns a started WFT, we do two things before
    /// handing the response to the caller:
    ///
    /// 1. **Barrier-gated query attachment** — drain queries from the
    ///    `BufferedQueryRegistry` whose `required_barrier` is satisfied by
    ///    the history included in this response. These come from the
    ///    buffered registry (not the broker) because they need consistency
    ///    guarantees that the broker's fire-and-forget dispatch cannot
    ///    provide.
    ///
    /// 2. **Update message construction** — for each pending update
    ///    transport, we build a `ProtocolMessage` with the update request
    ///    body and a `sequencing_event_id` set to `started_event_id - 1`.
    ///    The SDK uses this to determine where in the history replay the
    ///    update should be processed.
    pub async fn poll_workflow_task_queue(
        &self,
        headers: &HeaderMap,
        req: PollWorkflowTaskQueueRequest,
    ) -> EdgeResult<Option<PollWorkflowTaskQueueResponse>> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "poll_workflow_task_queue",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::PollWorkflowTaskQueue,
                        true,
                    )
                    .await?;

                ensure_local(
                    self.router
                        .route_task_queue(&req.namespace, &req.task_queue, TaskKind::Workflow)
                        .await?,
                )?;

                let _permit = self.long_polls.acquire().await?;
                let _poller = self.poller_registry.register(
                    queue_key_for_poll(
                        &req.namespace,
                        &req.task_queue,
                        TaskKind::Workflow,
                        req.deployment.clone(),
                        req.build_id.clone(),
                    ),
                    WorkerIdentity(req.worker_identity.clone()),
                );
                // A versioned worker poll lazily registers its deployment and version,
                // matching v1.31.0's matching-driven auto-create
                // (`service/worker/workerdeployment/client.go:1230 @ v1.31.0`). This is
                // best-effort bookkeeping on the control plane: a registry hiccup must
                // not fail the poll itself, so failures are logged rather than
                // propagated. Unversioned polls carry no deployment/build id and skip it.
                if let (Some(worker_deployments), Some(deployment), Some(build_id)) = (
                    self.worker_deployments.as_ref(),
                    req.deployment.as_ref(),
                    req.build_id.as_ref(),
                ) && let Err(error) = worker_deployments
                    .register_polled_deployment(RegisterPolledDeployment {
                        namespace_id: to_internal::namespace_id_for(&req.namespace),
                        deployment_name: DeploymentName(deployment.0.clone()),
                        build_id: tokeira_storage::BuildId(build_id.0.clone()),
                        task_queue: req.task_queue.clone(),
                        task_queue_type: DeploymentTaskQueueType::Workflow,
                        identity: req.worker_identity.clone(),
                    })
                    .await
                {
                    tracing::warn!(
                        %error,
                        deployment = %deployment.0,
                        build_id = %build_id.0,
                        "failed to auto-register polled worker deployment"
                    );
                }
                let internal = to_internal::poll_request(req);
                let activation = self
                    .runtime
                    .poll_workflow_activation(
                        internal.queue,
                        internal.worker_identity.clone(),
                        internal.timeout,
                    )
                    .await
                    .map_err(EdgeError::from)?;

                match activation {
                    Some(WorkflowActivation::WorkflowTask(started)) => {
                        let mut response =
                            from_internal::poll_response(started.clone(), self.repo.as_ref())
                                .await
                                .map_err(EdgeError::from)?;
                        self.decorate_workflow_task_response(&started, &mut response)
                            .await?;

                        Ok(Some(response))
                    }
                    Some(WorkflowActivation::QueryTask(query)) => Ok(Some(
                        self.build_direct_query_poll_response(query, &internal.worker_identity)
                            .await?,
                    )),
                    None => Ok(None),
                }
            },
        )
        .await
    }

    async fn decorate_workflow_task_response(
        &self,
        started: &StartedWorkflowTask,
        response: &mut PollWorkflowTaskQueueResponse,
    ) -> EdgeResult<()> {
        let task_token = response.task_token.clone();
        let observable_barrier = response
            .payload
            .history
            .last()
            .map(|event| event.event_id)
            .unwrap_or(response.started_event_id);
        self.attach_buffered_queries(
            started.run_key,
            observable_barrier,
            &task_token,
            &mut response.queries,
        )
        .await;

        for update in self
            .runtime
            .pending_update_transports(started.run_key)
            .await
            .map_err(EdgeError::from)?
        {
            let request = tokeira_proto::public::temporal::api::update::v1::Request {
                meta: Some(tokeira_proto::public::temporal::api::update::v1::Meta {
                    update_id: update.update_id.clone(),
                    identity: update.identity,
                }),
                input: Some(tokeira_proto::public::temporal::api::update::v1::Input {
                    header: None,
                    name: update.update_name,
                    args: Some(tokeira_proto::conversions::common::payloads_from_domain(
                        &update.input,
                    )),
                }),
            };
            let body = prost_types::Any {
                type_url: "type.googleapis.com/temporal.api.update.v1.Request".to_string(),
                value: request.encode_to_vec(),
            };
            // The SDK requires sequencing_event_id to determine where in the
            // history replay the update should be processed. Temporal sets
            // this to workflowTaskStartedEventID - 1.
            let sequencing_event_id = started.token.started_event_id - 1;
            response.messages.push(ProtocolMessageDto {
                id: format!("{}/request", update.update_id),
                protocol_instance_id: update.update_id,
                body: body.encode_to_vec(),
                sequencing_event_id: Some(sequencing_event_id),
            });
        }

        Ok(())
    }

    /// Process a WFT completion from the SDK.
    ///
    /// Three non-obvious things happen here:
    ///
    /// 1. **ProtocolMessage command resolution** — the translate layer has
    ///    already decoded `ProtocolMessage` commands from the `messages`
    ///    field. For `Accepted` bodies we fill in `update_name`/`input`
    ///    from the `UpdateRegistry` (the SDK doesn't echo these back). For
    ///    `Completed`/`Rejected` bodies we notify the registry so the
    ///    original `UpdateWorkflowExecution` caller gets unblocked.
    ///
    /// 2. **Query-only short-circuit** — if the task token has
    ///    `logical_seq = 0` (a synthetic query-only WFT) and there are no
    ///    commands, we return immediately without touching the runtime.
    ///
    /// 3. **Post-completion quiescence check** — after committing the
    ///    completion, if the run is still open, has buffered queries, and
    ///    is now quiescent (no pending WFT), we either build an eager
    ///    inline WFT (if the SDK requested `return_new_workflow_task`) or
    ///    dispatch queries directly through the broker. This avoids
    ///    queries sitting in the buffer until the next unrelated mutation.
    pub async fn respond_workflow_task_completed(
        &self,
        headers: &HeaderMap,
        mut req: RespondWorkflowTaskCompletedRequest,
    ) -> EdgeResult<RespondWorkflowTaskCompletedResponse> {
        self.observe_edge_call(
            headers,
            "respond_workflow_task_completed",
            None,
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(headers, None, Action::RespondWorkflowTaskCompleted, false)
                    .await?;

                let query_only = {
                    let token: tokeira_types::WorkflowTaskToken =
                        serde_json::from_slice(&req.task_token).map_err(EdgeError::from)?;
                    token.logical_seq.0 == 0
                };

                for (query_id, result) in &req.query_results {
                    if let Some(sender) = self.pending_queries.take(&req.task_token, query_id).await
                    {
                        let _ = sender.send(match result {
                            QueryResultDto::Answered { result } => QueryResult::Completed {
                                result: result.clone(),
                            },
                            QueryResultDto::Failed { error_message } => QueryResult::Failed {
                                message: error_message.clone(),
                            },
                        });
                    }
                }

                let task_token: tokeira_types::WorkflowTaskToken =
                    serde_json::from_slice(&req.task_token).map_err(EdgeError::from)?;

                // Hydrate an Acceptance message's command body with the update's
                // name/input from the registry (the worker echoes only the message
                // id). We deliberately do NOT resolve Completed/Rejected waiters
                // here: the update outcome must be published only after the
                // `WorkflowExecutionUpdateCompleted`/`Rejected` event durably
                // commits, which the lane does post-commit. v1.31.0 sets the
                // outcome future in `OnAfterCommit` for exactly this reason — a
                // waiter woken before the event is durable would re-read history
                // and see only `Accepted` (`update.go:onResponseMsg @ v1.31.0`).
                // Notifying here pre-commit removed the registry waiter and made
                // the lane's correct post-commit notify a no-op, stranding the
                // COMPLETED caller at the Accepted stage.
                for cmd in &mut req.commands {
                    if let tokeira_kernel::WorkflowCommand::ProtocolMessage {
                        body:
                            tokeira_kernel::UpdateProtocolBody::Accepted {
                                update_id,
                                update_name,
                                input,
                            },
                        ..
                    } = cmd
                        && let Ok(Some((name, inp))) = self
                            .runtime
                            .peek_update_info(task_token.run_key, update_id.clone())
                            .await
                    {
                        *update_name = name;
                        *input = inp;
                    }
                }

                if query_only && req.commands.is_empty() {
                    return Ok(RespondWorkflowTaskCompletedResponse {
                        transition_seq: 0,
                        last_event_id: 0,
                        execution_status: ExecutionStatus::Running,
                        new_run_id: None,
                        was_duplicate: false,
                        workflow_task: None,
                        activity_tasks: Vec::new(),
                    });
                }

                let eager_activity_specs = collect_eager_activity_specs(
                    &req.commands,
                    self.eager_dispatch_config
                        .max_eager_activity_tasks_per_response,
                );
                let completion_identity = req.identity.clone();
                let saved_task_token = req.task_token.clone();
                let wants_eager_return = req.return_new_workflow_task;

                let internal =
                    to_internal::workflow_task_completed_request(req).map_err(EdgeError::from)?;
                let run_key = internal.token.run_key;
                let outcome = self
                    .runtime
                    .complete_workflow_task(internal)
                    .await
                    .map_err(EdgeError::from)?;
                self.notify_history_run_key(run_key, outcome.last_event_id)
                    .await;

                let mut resp = from_internal::completed_response(outcome);

                if !eager_activity_specs.is_empty() {
                    let namespace_id =
                        match self.repo.load_run(run_key).await.map_err(EdgeError::from)? {
                            tokeira_kernel::LoadedRun::Existing(state) => state.namespace_id,
                            tokeira_kernel::LoadedRun::Absent => {
                                return Err(EdgeError::Internal(format!(
                                    "completed run {:?} not found after commit",
                                    run_key
                                )));
                            }
                        };
                    for (activity_id, task_queue, deployment, build_id) in eager_activity_specs {
                        let queue = tokeira_types::QueueKey {
                            namespace_id,
                            task_queue,
                            task_kind: TaskKind::Activity,
                            deployment,
                            build_id,
                        };
                        if let Some(started) = self
                            .runtime
                            .try_claim_activity_task(
                                queue,
                                run_key,
                                activity_id,
                                WorkerIdentity(completion_identity.clone()),
                            )
                            .await
                            .map_err(EdgeError::from)?
                        {
                            resp.activity_tasks.push(
                                from_internal::poll_activity_response(started)
                                    .map_err(EdgeError::from)?,
                            );
                        }
                    }
                }

                if resp.execution_status.is_open()
                    && (wants_eager_return || self.buffered_queries.has_buffered(run_key))
                {
                    let token: tokeira_types::WorkflowTaskToken =
                        serde_json::from_slice(&saved_task_token).map_err(EdgeError::from)?;
                    let loaded = self
                        .repo
                        .load_run(token.run_key)
                        .await
                        .map_err(EdgeError::from)?;
                    if let tokeira_kernel::LoadedRun::Existing(state) = loaded {
                        if wants_eager_return && state.pending_workflow_task.is_some() {
                            let queue = tokeira_types::QueueKey {
                                namespace_id: state.namespace_id,
                                task_queue: state.task_queue.clone(),
                                task_kind: TaskKind::Workflow,
                                deployment: state.deployment.clone(),
                                build_id: state.build_id.clone(),
                            };
                            if let Some(mut started) = self
                                .runtime
                                .try_claim_workflow_task(
                                    queue,
                                    run_key,
                                    WorkerIdentity(completion_identity.clone()),
                                )
                                .await
                                .map_err(EdgeError::from)?
                            {
                                // A new WFT returned inline from RespondWorkflowTaskCompleted is
                                // delivered to the same worker and carries incremental history from
                                // the previous started event — v1.31.0 treats it as sticky
                                // (respondworkflowtaskcompleted/api.go:759-760), so the SDK receives
                                // only the events after PreviousStartedEventId rather than the full
                                // history.
                                started.is_sticky_match = true;
                                let mut workflow_task = from_internal::poll_response(
                                    started.clone(),
                                    self.repo.as_ref(),
                                )
                                .await
                                .map_err(EdgeError::from)?;
                                self.decorate_workflow_task_response(&started, &mut workflow_task)
                                    .await?;
                                resp.workflow_task = Some(workflow_task);
                            }
                        } else if state.pending_workflow_task.is_none() {
                            if wants_eager_return {
                                resp.workflow_task = self
                                    .build_eager_query_workflow_task(
                                        &state,
                                        token.shard_epoch,
                                        state.last_event_id,
                                    )
                                    .await;
                            } else {
                                self.dispatch_queries_direct(
                                    state.run_key,
                                    &state,
                                    state.last_event_id,
                                )
                                .await;
                            }
                        }
                    }
                }

                Ok(resp)
            },
        )
        .await
    }

    pub async fn respond_query_task_completed(
        &self,
        headers: &HeaderMap,
        task_token: Vec<u8>,
        result: QueryResult,
    ) -> EdgeResult<()> {
        self.observe_edge_call(
            headers,
            "respond_query_task_completed",
            None,
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(headers, None, Action::RespondQueryTaskCompleted, false)
                    .await?;

                if let Some(sender) = self
                    .pending_queries
                    .take(&task_token, LEGACY_QUERY_ID)
                    .await
                {
                    let _ = sender.send(result);
                }
                Ok(())
            },
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn insert_legacy_query_waiter(
        &self,
        task_token: Vec<u8>,
        tx: tokio::sync::oneshot::Sender<QueryResult>,
    ) {
        self.pending_queries
            .insert(&task_token, LEGACY_QUERY_ID.to_string(), tx)
            .await;
    }

    pub async fn describe_workflow_execution(
        &self,
        headers: &HeaderMap,
        req: DescribeWorkflowExecutionRequest,
    ) -> EdgeResult<WorkflowExecutionDescription> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "describe_workflow_execution",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::DescribeWorkflowExecution,
                        false,
                    )
                    .await?;

                ensure_local(
                    self.router
                        .route_workflow(&req.namespace, &req.workflow_id)
                        .await?,
                )?;

                self.resolver
                    .describe_execution(
                        &req.namespace,
                        &req.workflow_id,
                        req.run_id
                            .as_deref()
                            .map(|value| Uuid::parse_str(value).map(RunId))
                            .transpose()
                            .map_err(|err| {
                                EdgeError::BadRequest(format!(
                                    "invalid run_id `{}`: {err}",
                                    req.run_id.as_deref().unwrap_or_default()
                                ))
                            })?,
                    )
                    .await
                    .map_err(EdgeError::from)?
                    .ok_or(EdgeError::WorkflowNotFound {
                        namespace: req.namespace,
                        workflow_id: req.workflow_id,
                    })
            },
        )
        .await
    }

    pub async fn list_workflow_executions(
        &self,
        headers: &HeaderMap,
        req: ListWorkflowExecutionsRequest,
    ) -> EdgeResult<ListWorkflowExecutionsResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "list_workflow_executions",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::ListWorkflowExecutions,
                        false,
                    )
                    .await?;

                self.visibility
                    .list_workflows(req)
                    .await
                    .map_err(EdgeError::from)
            },
        )
        .await
    }

    pub async fn count_workflow_executions(
        &self,
        headers: &HeaderMap,
        req: CountWorkflowExecutionsRequest,
    ) -> EdgeResult<CountWorkflowExecutionsResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "count_workflow_executions",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::CountWorkflowExecutions,
                        false,
                    )
                    .await?;

                self.visibility
                    .count_workflows(req)
                    .await
                    .map_err(EdgeError::from)
            },
        )
        .await
    }

    /// List standalone-activity executions, scoped to the `archetype_id` the gRPC
    /// layer resolved from the activity bridge (the visibility plane is
    /// archetype-neutral; Requirement 13.1).
    pub async fn list_activity_executions(
        &self,
        headers: &HeaderMap,
        archetype_id: ArchetypeId,
        req: ListActivityExecutionsRequest,
    ) -> EdgeResult<ListActivityExecutionsResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "list_activity_executions",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::ListActivityExecutions,
                        false,
                    )
                    .await?;

                self.visibility
                    .list_activities(archetype_id, req)
                    .await
                    .map_err(EdgeError::from)
            },
        )
        .await
    }

    /// Count standalone-activity executions, scoped to the activity archetype.
    pub async fn count_activity_executions(
        &self,
        headers: &HeaderMap,
        archetype_id: ArchetypeId,
        req: CountActivityExecutionsRequest,
    ) -> EdgeResult<CountActivityExecutionsResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "count_activity_executions",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::CountActivityExecutions,
                        false,
                    )
                    .await?;

                self.visibility
                    .count_activities(archetype_id, req)
                    .await
                    .map_err(EdgeError::from)
            },
        )
        .await
    }

    pub async fn get_cluster_info(&self, headers: &HeaderMap) -> EdgeResult<ClusterInfo> {
        self.observe_edge_call(headers, "get_cluster_info", None, None, async move {
            let _ctx = self
                .interceptors
                .begin(headers, None, Action::GetClusterInfo, false)
                .await?;

            self.operator_api
                .cluster_info()
                .await
                .map_err(EdgeError::from)
        })
        .await
    }

    pub async fn get_system_info(&self, headers: &HeaderMap) -> EdgeResult<SystemInfo> {
        self.observe_edge_call(headers, "get_system_info", None, None, async move {
            let _ctx = self
                .interceptors
                .begin(headers, None, Action::GetSystemInfo, false)
                .await?;

            let cluster = self
                .operator_api
                .cluster_info()
                .await
                .map_err(EdgeError::from)?;

            Ok(SystemInfo {
                server_version: cluster.version,
                capabilities: system_capabilities_with_matrix_overlay(SystemCapabilities {
                    signal_and_query_header: true,
                    internal_error_differentiation: true,
                    activity_failure_include_heartbeat: false,
                    supports_schedules: false,
                    encoded_failure_attributes: true,
                    build_id_based_versioning: true,
                    upsert_memo: false,
                    eager_workflow_start: false,
                    sdk_metadata: false,
                    count_group_by_execution_status: true,
                    nexus: true,
                    server_scaled_deployments: false,
                    worker_heartbeats: true,
                }),
            })
        })
        .await
    }

    pub async fn list_namespaces(
        &self,
        headers: &HeaderMap,
    ) -> EdgeResult<EdgeListNamespacesResponse> {
        self.observe_edge_call(headers, "list_namespaces", None, None, async move {
            let _ctx = self
                .interceptors
                .begin(headers, None, Action::ListNamespaces, false)
                .await?;

            let mut namespaces = self.namespaces.list_all().await.map_err(EdgeError::from)?;
            namespaces.sort_by(|left, right| left.name.cmp(&right.name));

            Ok(EdgeListNamespacesResponse {
                namespaces: namespaces
                    .into_iter()
                    .map(namespace_to_description)
                    .collect(),
                next_page_token: None,
            })
        })
        .await
    }

    pub async fn describe_namespace(
        &self,
        headers: &HeaderMap,
        namespace_name: &str,
    ) -> EdgeResult<NamespaceDescription> {
        let namespace_label = namespace_name.to_string();
        self.observe_edge_call(
            headers,
            "describe_namespace",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(namespace_name),
                        Action::DescribeNamespace,
                        false,
                    )
                    .await?;

                let namespace = self
                    .namespaces
                    .get(namespace_name)
                    .await
                    .map_err(EdgeError::from)?
                    .ok_or_else(|| EdgeError::NamespaceNotFound(namespace_name.to_string()))?;

                Ok(namespace_to_description(namespace))
            },
        )
        .await
    }

    pub async fn register_namespace(
        &self,
        headers: &HeaderMap,
        req: RegisterNamespaceRequest,
    ) -> EdgeResult<()> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "register_namespace",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(headers, None, Action::RegisterNamespace, false)
                    .await?;

                if !is_valid_namespace_name(&req.namespace) {
                    return Err(EdgeError::BadRequest(format!(
                        "invalid namespace name `{}`",
                        req.namespace
                    )));
                }

                if self
                    .namespaces
                    .get(&req.namespace)
                    .await
                    .map_err(EdgeError::from)?
                    .is_some()
                {
                    return Err(EdgeError::NamespaceAlreadyExists(req.namespace));
                }

                self.namespaces
                    .insert(ResolvedNamespace {
                        retention: req.retention,
                        ..ResolvedNamespace::active(req.namespace.clone())
                    })
                    .await
                    .map_err(EdgeError::from)?;

                // Seed the namespace's predefined search attributes so visibility
                // queries in it resolve the map-backed predefined fields, matching the
                // bootstrapped `default` namespace. Without this, list/count in a
                // runtime-created namespace rejects predefined attributes as unknown.
                self.operator_api
                    .seed_predefined_search_attributes(&req.namespace)
                    .await
                    .map_err(EdgeError::from)
            },
        )
        .await
    }

    /// Update a namespace's lifecycle state and/or description.
    ///
    /// Tokeira runs a single non-global cluster, so the replication, config,
    /// and security-token request fields are accepted at the wire layer but
    /// ignored here. Only the state transition and description are honoured.
    ///
    /// State-transition validity mirrors v1.31.0 `validateStateUpdate`
    /// (`service/frontend/namespace_handler.go @ v1.31.0`): `Unspecified` or a
    /// same-state target is a no-op; `Registered → {Deleted, Deprecated}` and
    /// `Deprecated → Deleted` are allowed; every other transition (notably any
    /// transition out of `Deleted`) is rejected with `INVALID_ARGUMENT`.
    pub async fn update_namespace(
        &self,
        headers: &HeaderMap,
        req: UpdateNamespaceRequest,
    ) -> EdgeResult<NamespaceDescription> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "update_namespace",
            Some(namespace_label.as_str()),
            None,
            async move {
                // Resolve before `interceptors.begin`: begin() rejects a deleted
                // namespace with NamespaceDeleted, but UpdateNamespace is the very
                // RPC operators use to manage already-deleted namespaces. We must
                // observe the current (possibly deleted) state to validate the
                // transition rather than fail the lookup outright.
                let _ctx = self
                    .interceptors
                    .begin(headers, None, Action::UpdateNamespace, false)
                    .await?;

                let mut namespace = self
                    .namespaces
                    .get(&req.namespace)
                    .await
                    .map_err(EdgeError::from)?
                    .ok_or_else(|| EdgeError::NamespaceNotFound(req.namespace.clone()))?;

                validate_namespace_state_update(namespace.deleted, req.state)?;

                if matches!(req.state, NamespaceStateUpdate::Deleted) {
                    namespace.deleted = true;
                }

                let mut description = namespace_to_description(namespace.clone());
                if let Some(new_description) = req.description {
                    description.description = new_description;
                }

                self.namespaces
                    .insert(namespace)
                    .await
                    .map_err(EdgeError::from)?;

                Ok(description)
            },
        )
        .await
    }

    pub async fn describe_task_queue(
        &self,
        headers: &HeaderMap,
        req: DescribeTaskQueueRequest,
    ) -> EdgeResult<DescribeTaskQueueResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "describe_task_queue",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::DescribeTaskQueue,
                        false,
                    )
                    .await?;

                ensure_local(
                    self.router
                        .route_task_queue(&req.namespace, &req.task_queue, req.task_kind)
                        .await?,
                )?;

                let queue =
                    queue_key_for_poll(&req.namespace, &req.task_queue, req.task_kind, None, None);
                let namespace_id = to_internal::namespace_id_for(&req.namespace);
                let task_queue = TaskQueueName(req.task_queue.clone());
                let config = self
                    .task_queue_config_store
                    .get(&namespace_id, &task_queue)
                    .map(task_queue_config_to_edge)
                    .unwrap_or_default();

                // Surface Worker Deployment versioning for this task queue (current /
                // ramping version) the way Temporal's matching layer does from synced
                // task-queue user data (`task_queue_partition_manager.go:976 @ v1.31.0`).
                // Derived from the registry; absent when no deployment version has
                // polled the queue or when the registry is not configured.
                let versioning_info = match self.worker_deployments.as_ref() {
                    Some(worker_deployments) => worker_deployments
                        .task_queue_versioning(namespace_id, req.task_queue.clone())
                        .await?
                        .map(task_queue_versioning_view_to_edge),
                    None => None,
                };

                Ok(DescribeTaskQueueResponse {
                    pollers: self
                        .poller_registry
                        .pollers(&queue)
                        .into_iter()
                        .map(active_poller_to_edge)
                        .collect(),
                    backlog_count_hint: req.include_status.then_some(0),
                    config,
                    versioning_info,
                })
            },
        )
        .await
    }

    /// List the partition topology of a task queue.
    ///
    /// tokeira runs a single (root) partition per task queue per task type. v1.31.0's
    /// matching engine returns one `TaskQueuePartitionMetadata` per partition for the
    /// activity and workflow types (`matching_engine.go:1609 @ v1.31.0`); with a single
    /// partition the root key is the bare task-queue name (no `/_sys/<name>/<n>` suffix,
    /// which v1.31.0 only adds for partitions 1..N). `owner_host_name` is left empty: the
    /// edge plane has no matching-host membership to attribute, and the field is purely
    /// diagnostic — SDKs discover topology from `key`. Validation (namespace / task-queue
    /// presence, recognized kind) runs at the gRPC translation boundary before this call.
    pub async fn list_task_queue_partitions(
        &self,
        headers: &HeaderMap,
        req: ListTaskQueuePartitionsRequest,
    ) -> EdgeResult<ListTaskQueuePartitionsResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "list_task_queue_partitions",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::ListTaskQueuePartitions,
                        false,
                    )
                    .await?;

                let root = TaskQueuePartition {
                    key: req.task_queue,
                    owner_host_name: String::new(),
                };
                Ok(ListTaskQueuePartitionsResponse {
                    activity_partitions: vec![root.clone()],
                    workflow_partitions: vec![root],
                })
            },
        )
        .await
    }

    /// Update a running workflow's execution options (`versioning_override`).
    ///
    /// Validates the run id and resolves the target execution before mutating
    /// (`NOT_FOUND` for an absent execution; `INVALID_ARGUMENT` for a malformed run id —
    /// both surfaced by `resolve_execution_run_key`). The change has already been reduced
    /// from the `update_mask` at the gRPC boundary, so here we only translate the override
    /// to the kernel and submit the per-run command. The response echoes the post-update
    /// options.
    pub async fn update_workflow_execution_options(
        &self,
        headers: &HeaderMap,
        req: UpdateWorkflowExecutionOptionsRequest,
    ) -> EdgeResult<UpdateWorkflowExecutionOptionsResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "update_workflow_execution_options",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::UpdateWorkflowExecutionOptions,
                        false,
                    )
                    .await?;

                let run_key = self
                    .resolve_execution_run_key(
                        &req.namespace,
                        &req.workflow_id,
                        req.run_id.as_deref(),
                    )
                    .await?;

                let versioning_override = match &req.versioning_override {
                    VersioningOverrideChange::Set(override_) => {
                        FieldChange::Set(to_internal::versioning_override_to_kernel(override_))
                    }
                    VersioningOverrideChange::Clear => FieldChange::Clear,
                };
                let request = RequestContext {
                    request_id: RequestId(Uuid::new_v4().to_string()),
                    caller_identity: (!req.identity.is_empty()).then(|| req.identity.clone()),
                    received_at: OffsetDateTime::now_utc(),
                };

                self.runtime
                    .update_workflow_execution_options(run_key, versioning_override, request)
                    .await
                    .map_err(EdgeError::from)?;

                // The post-update value mirrors the applied change (the only mutable
                // option tokeira models): `Some` after a Set, `None` after a Clear.
                let versioning_override = match req.versioning_override {
                    VersioningOverrideChange::Set(override_) => Some(override_),
                    VersioningOverrideChange::Clear => None,
                };
                Ok(UpdateWorkflowExecutionOptionsResponse {
                    versioning_override,
                })
            },
        )
        .await
    }

    /// Reads the custom search-attribute catalog for WorkflowService callers.
    ///
    /// Temporal exposes this catalog on WorkflowService even though custom
    /// attribute mutation lives on OperatorService. The edge therefore
    /// authorizes it as an operator read and delegates to the same catalog
    /// source instead of creating a second registry.
    pub async fn get_search_attributes(
        &self,
        headers: &HeaderMap,
    ) -> EdgeResult<Vec<SearchAttributeDefinition>> {
        self.observe_edge_call(headers, "get_search_attributes", None, None, async move {
            let _ctx = self
                .interceptors
                .begin(headers, None, Action::OperatorRead, false)
                .await?;

            self.operator_api
                .list_search_attributes(None)
                .await
                .map_err(EdgeError::from)
        })
        .await
    }

    pub async fn delete_workflow_execution(
        &self,
        headers: &HeaderMap,
        req: DeleteWorkflowExecutionRequest,
    ) -> EdgeResult<()> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "delete_workflow_execution",
            Some(namespace_label.as_str()),
            None,
            async move {
                let ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::DeleteWorkflowExecution,
                        false,
                    )
                    .await?;

                ensure_local(
                    self.router
                        .route_workflow(&req.namespace, &req.workflow_id)
                        .await?,
                )?;

                let run_key = self
                    .resolve_execution_run_key(
                        &req.namespace,
                        &req.workflow_id,
                        req.run_id.as_deref(),
                    )
                    .await?;

                let loaded = self.repo.load_run(run_key).await.map_err(EdgeError::from)?;
                let tokeira_kernel::LoadedRun::Existing(state) = loaded else {
                    return Err(EdgeError::WorkflowNotFound {
                        namespace: req.namespace,
                        workflow_id: req.workflow_id,
                    });
                };

                if state.status.is_open() {
                    let outcome = self
                        .runtime
                        .terminate_workflow(
                            run_key,
                            TerminateRequest {
                                reason: "deleted via delete_workflow_execution".to_string(),
                                details: None,
                                identity: "temporal-ui".to_string(),
                                request: RequestContext {
                                    request_id: tokeira_types::RequestId(
                                        ctx.request_id.as_str().to_string(),
                                    ),
                                    caller_identity: None,
                                    received_at: OffsetDateTime::now_utc(),
                                },
                                now: OffsetDateTime::now_utc(),
                            },
                        )
                        .await
                        .map_err(EdgeError::from)?;
                    self.notify_history_run_key(run_key, outcome.last_event_id)
                        .await;
                }

                self.visibility
                    .delete_execution(run_key)
                    .await
                    .map_err(EdgeError::from)
            },
        )
        .await
    }

    pub async fn reset_workflow_execution(
        &self,
        headers: &HeaderMap,
        req: ResetWorkflowExecutionRequest,
    ) -> EdgeResult<ResetWorkflowExecutionResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "reset_workflow_execution",
            Some(namespace_label.as_str()),
            None,
            async move {
                let ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::ResetWorkflowExecution,
                        false,
                    )
                    .await?;

                ensure_local(
                    self.router
                        .route_workflow(&req.namespace, &req.workflow_id)
                        .await?,
                )?;

                let run_key = self
                    .resolve_execution_run_key(
                        &req.namespace,
                        &req.workflow_id,
                        req.run_id.as_deref(),
                    )
                    .await?;
                let history = self
                    .repo
                    .read_history(run_key, 0, usize::MAX)
                    .await
                    .map_err(EdgeError::from)?;
                validate_reset_target(&history, req.workflow_task_finish_event_id)?;

                let execution = ExecutionRef {
                    namespace_id: to_internal::namespace_id_for(&req.namespace),
                    workflow_id: tokeira_types::WorkflowId(req.workflow_id.clone()),
                    run_id: req
                        .run_id
                        .as_deref()
                        .and_then(|value| uuid::Uuid::parse_str(value).ok())
                        .map(RunId),
                };
                let internal = to_internal::reset_request(req, &ctx.request_id);
                let outcome = self
                    .runtime
                    .reset_workflow(execution, internal)
                    .await
                    .map_err(EdgeError::from)?;

                let last_event_id =
                    read_last_event_id(self.repo.as_ref(), outcome.successor_run_key).await?;
                self.notify_history_run_key(outcome.successor_run_key, last_event_id)
                    .await;

                Ok(from_internal::reset_response(outcome))
            },
        )
        .await
    }

    pub async fn signal_with_start_workflow_execution(
        &self,
        headers: &HeaderMap,
        req: SignalWithStartWorkflowExecutionRequest,
    ) -> EdgeResult<SignalWithStartWorkflowExecutionResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "signal_with_start_workflow_execution",
            Some(namespace_label.as_str()),
            None,
            async move {
                let ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::SignalWithStartWorkflowExecution,
                        false,
                    )
                    .await?;

                ensure_local(
                    self.router
                        .route_workflow(&req.namespace, &req.workflow_id)
                        .await?,
                )?;
                let internal = to_internal::signal_with_start_request(
                    req.clone(),
                    &ctx.request_id,
                    Some(self.versioning_rule_store.as_ref()),
                );
                match self
                    .runtime
                    .signal_with_start_workflow(internal)
                    .await
                    .map_err(EdgeError::from)?
                {
                    SignalWithStartResult::Started { run_key, run_id } => {
                        let last_event_id = read_last_event_id(self.repo.as_ref(), run_key).await?;
                        self.notify_history_run_key(run_key, last_event_id).await;
                        Ok(SignalWithStartWorkflowExecutionResponse {
                            run_id,
                            started: true,
                        })
                    }
                    SignalWithStartResult::Signaled { run_key, run_id } => {
                        let last_event_id = read_last_event_id(self.repo.as_ref(), run_key).await?;
                        self.notify_history_run_key(run_key, last_event_id).await;
                        Ok(SignalWithStartWorkflowExecutionResponse {
                            run_id,
                            started: false,
                        })
                    }
                    SignalWithStartResult::Rejected { run_id, .. } => {
                        Err(EdgeError::WorkflowAlreadyStarted {
                            namespace: req.namespace,
                            workflow_id: req.workflow_id,
                            run_id: run_id.0.to_string(),
                        })
                    }
                }
            },
        )
        .await
    }

    // ── Activity endpoints ──

    pub async fn poll_activity_task_queue(
        &self,
        headers: &HeaderMap,
        req: PollActivityTaskQueueRequest,
    ) -> EdgeResult<Option<PollActivityTaskQueueResponse>> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "poll_activity_task_queue",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::PollActivityTaskQueue,
                        true,
                    )
                    .await?;

                ensure_local(
                    self.router
                        .route_task_queue(&req.namespace, &req.task_queue, TaskKind::Activity)
                        .await?,
                )?;

                let _permit = self.long_polls.acquire().await?;
                let _poller = self.poller_registry.register(
                    queue_key_for_poll(
                        &req.namespace,
                        &req.task_queue,
                        TaskKind::Activity,
                        None,
                        None,
                    ),
                    WorkerIdentity(req.worker_identity.clone()),
                );
                let internal = to_internal::poll_activity_request(req);
                let started = self
                    .runtime
                    .poll_activity_task(internal.queue, internal.worker_identity, internal.timeout)
                    .await
                    .map_err(EdgeError::from)?;

                match started {
                    Some(started) => Ok(Some(
                        from_internal::poll_activity_response(started).map_err(EdgeError::from)?,
                    )),
                    None => Ok(None),
                }
            },
        )
        .await
    }

    pub async fn respond_activity_task_completed(
        &self,
        headers: &HeaderMap,
        req: RespondActivityTaskCompletedRequest,
    ) -> EdgeResult<RespondActivityTaskCompletedResponse> {
        self.observe_edge_call(
            headers,
            "respond_activity_task_completed",
            None,
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(headers, None, Action::RespondActivityTaskCompleted, false)
                    .await?;

                let token = req.token;
                let _outcome = self
                    .runtime
                    .complete_activity_task(
                        token.clone(),
                        req.result,
                        Some(tokeira_types::WorkerIdentity(req.identity)),
                    )
                    .await
                    .map_err(EdgeError::from)?;
                self.notify_history_run_key(
                    token.run_key,
                    read_last_event_id(self.repo.as_ref(), token.run_key).await?,
                )
                .await;

                Ok(RespondActivityTaskCompletedResponse)
            },
        )
        .await
    }

    pub async fn respond_activity_task_failed(
        &self,
        headers: &HeaderMap,
        req: RespondActivityTaskFailedRequest,
    ) -> EdgeResult<RespondActivityTaskFailedResponse> {
        self.observe_edge_call(
            headers,
            "respond_activity_task_failed",
            None,
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(headers, None, Action::RespondActivityTaskFailed, false)
                    .await?;

                let token = req.token;
                self.runtime
                    .fail_activity_task(
                        token.clone(),
                        req.failure,
                        req.failure_error_type,
                        req.is_non_retryable,
                        Some(tokeira_types::WorkerIdentity(req.identity)),
                    )
                    .await
                    .map_err(EdgeError::from)?;
                self.notify_history_run_key(
                    token.run_key,
                    read_last_event_id(self.repo.as_ref(), token.run_key).await?,
                )
                .await;

                Ok(RespondActivityTaskFailedResponse)
            },
        )
        .await
    }

    /// `RespondWorkflowTaskFailed`. Routing on cause happens in the runtime:
    /// `GrpcMessageTooLarge` force-close-terminates the run
    /// (`respondworkflowtaskfailed/api.go:88 @ v1.31.0`); other causes take
    /// the WFT-failed retry path.
    pub async fn respond_workflow_task_failed(
        &self,
        headers: &HeaderMap,
        token: tokeira_types::WorkflowTaskToken,
        failure_cause: tokeira_kernel::WorkflowTaskFailedCause,
        failure_details: Option<tokeira_types::Payload>,
        identity: String,
    ) -> EdgeResult<()> {
        self.observe_edge_call(
            headers,
            "respond_workflow_task_failed",
            None,
            None,
            async move {
                let ctx = self
                    .interceptors
                    .begin(headers, None, Action::RespondWorkflowTaskFailed, false)
                    .await?;

                let run_key = token.run_key;
                self.runtime
                    .fail_workflow_task(
                        token,
                        failure_cause,
                        failure_details,
                        tokeira_types::WorkerIdentity(identity),
                        tokeira_types::RequestContext {
                            request_id: tokeira_types::RequestId(
                                ctx.request_id.as_str().to_string(),
                            ),
                            caller_identity: None,
                            received_at: time::OffsetDateTime::now_utc(),
                        },
                        time::OffsetDateTime::now_utc(),
                    )
                    .await
                    .map_err(EdgeError::from)?;
                self.notify_history_run_key(
                    run_key,
                    read_last_event_id(self.repo.as_ref(), run_key).await?,
                )
                .await;

                Ok(())
            },
        )
        .await
    }

    pub async fn record_activity_task_heartbeat(
        &self,
        headers: &HeaderMap,
        req: RecordActivityTaskHeartbeatRequest,
    ) -> EdgeResult<RecordActivityTaskHeartbeatResponse> {
        self.observe_edge_call(
            headers,
            "record_activity_task_heartbeat",
            None,
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(headers, None, Action::RecordActivityTaskHeartbeat, false)
                    .await?;

                let cancel_requested = self
                    .runtime
                    .record_activity_heartbeat(req.token, req.details)
                    .await
                    .map_err(EdgeError::from)?;

                Ok(RecordActivityTaskHeartbeatResponse { cancel_requested })
            },
        )
        .await
    }

    pub async fn respond_activity_task_canceled(
        &self,
        headers: &HeaderMap,
        req: RespondActivityTaskCanceledRequest,
    ) -> EdgeResult<RespondActivityTaskCanceledResponse> {
        self.observe_edge_call(
            headers,
            "respond_activity_task_canceled",
            None,
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(headers, None, Action::RespondActivityTaskCanceled, false)
                    .await?;

                let token = req.token;
                let outcome = self
                    .runtime
                    .cancel_activity_task(
                        token.clone(),
                        req.details,
                        worker_identity_from_request(req.identity),
                    )
                    .await
                    .map_err(EdgeError::from)?;
                self.notify_history_run_key(token.run_key, outcome.last_event_id)
                    .await;

                Ok(RespondActivityTaskCanceledResponse)
            },
        )
        .await
    }

    pub async fn record_activity_task_heartbeat_by_id(
        &self,
        headers: &HeaderMap,
        req: RecordActivityTaskHeartbeatByIdRequest,
    ) -> EdgeResult<RecordActivityTaskHeartbeatByIdResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "record_activity_task_heartbeat_by_id",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(headers, None, Action::RecordActivityTaskHeartbeat, false)
                    .await?;
                let run_key = self
                    .resolve_execution_run_key(
                        &req.namespace,
                        &req.workflow_id,
                        req.run_id.as_deref(),
                    )
                    .await?;
                let token = match self
                    .runtime
                    .resolve_activity_token(run_key, &req.activity_id)
                    .await
                {
                    Ok(token) => token,
                    Err(ActivityTokenResolutionError::ActivityNotStarted { .. }) => {
                        return Ok(RecordActivityTaskHeartbeatByIdResponse {
                            cancel_requested: false,
                        });
                    }
                    Err(error) => {
                        return Err(self.map_activity_resolution_error(
                            error,
                            &req.namespace,
                            &req.workflow_id,
                            &req.activity_id,
                        ));
                    }
                };
                let cancel_requested = self
                    .runtime
                    .record_activity_heartbeat(token, req.details)
                    .await
                    .map_err(EdgeError::from)?;
                Ok(RecordActivityTaskHeartbeatByIdResponse { cancel_requested })
            },
        )
        .await
    }

    pub async fn respond_activity_task_completed_by_id(
        &self,
        headers: &HeaderMap,
        req: RespondActivityTaskCompletedByIdRequest,
    ) -> EdgeResult<RespondActivityTaskCompletedByIdResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "respond_activity_task_completed_by_id",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(headers, None, Action::RespondActivityTaskCompleted, false)
                    .await?;
                let run_key = self
                    .resolve_execution_run_key(
                        &req.namespace,
                        &req.workflow_id,
                        req.run_id.as_deref(),
                    )
                    .await?;
                let token = self
                    .resolve_activity_token_for_edge(
                        run_key,
                        &req.activity_id,
                        &req.namespace,
                        &req.workflow_id,
                    )
                    .await?;
                let outcome = self
                    .runtime
                    .complete_activity_task(
                        token,
                        req.result,
                        worker_identity_from_request(req.identity),
                    )
                    .await
                    .map_err(EdgeError::from)?;
                self.notify_history_run_key(run_key, outcome.last_event_id)
                    .await;
                Ok(RespondActivityTaskCompletedByIdResponse)
            },
        )
        .await
    }

    pub async fn respond_activity_task_failed_by_id(
        &self,
        headers: &HeaderMap,
        req: RespondActivityTaskFailedByIdRequest,
    ) -> EdgeResult<RespondActivityTaskFailedByIdResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "respond_activity_task_failed_by_id",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(headers, None, Action::RespondActivityTaskFailed, false)
                    .await?;
                let run_key = self
                    .resolve_execution_run_key(
                        &req.namespace,
                        &req.workflow_id,
                        req.run_id.as_deref(),
                    )
                    .await?;
                let token = self
                    .resolve_activity_token_for_edge(
                        run_key,
                        &req.activity_id,
                        &req.namespace,
                        &req.workflow_id,
                    )
                    .await?;
                self.runtime
                    .fail_activity_task(
                        token,
                        req.failure,
                        req.failure_error_type,
                        req.is_non_retryable,
                        worker_identity_from_request(req.identity),
                    )
                    .await
                    .map_err(EdgeError::from)?;
                self.notify_history_run_key(
                    run_key,
                    read_last_event_id(self.repo.as_ref(), run_key).await?,
                )
                .await;
                Ok(RespondActivityTaskFailedByIdResponse)
            },
        )
        .await
    }

    pub async fn respond_activity_task_canceled_by_id(
        &self,
        headers: &HeaderMap,
        req: RespondActivityTaskCanceledByIdRequest,
    ) -> EdgeResult<RespondActivityTaskCanceledByIdResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "respond_activity_task_canceled_by_id",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(headers, None, Action::RespondActivityTaskCanceled, false)
                    .await?;
                let run_key = self
                    .resolve_execution_run_key(
                        &req.namespace,
                        &req.workflow_id,
                        req.run_id.as_deref(),
                    )
                    .await?;
                let token = self
                    .resolve_activity_token_for_edge(
                        run_key,
                        &req.activity_id,
                        &req.namespace,
                        &req.workflow_id,
                    )
                    .await?;
                let outcome = self
                    .runtime
                    .cancel_activity_task(
                        token,
                        req.details,
                        worker_identity_from_request(req.identity),
                    )
                    .await
                    .map_err(EdgeError::from)?;
                self.notify_history_run_key(run_key, outcome.last_event_id)
                    .await;
                Ok(RespondActivityTaskCanceledByIdResponse)
            },
        )
        .await
    }

    pub async fn update_activity_options(
        &self,
        headers: &HeaderMap,
        req: UpdateActivityOptionsRequest,
    ) -> EdgeResult<UpdateActivityOptionsResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "update_activity_options",
            Some(namespace_label.as_str()),
            None,
            async move {
                let ctx = self
                    .interceptors
                    .begin(headers, None, Action::UpdateActivityOptions, false)
                    .await?;
                let activity_id = match &req.target {
                    crate::translate::ActivityTarget::Id(activity_id) => activity_id.clone(),
                    crate::translate::ActivityTarget::Type(_)
                    | crate::translate::ActivityTarget::MatchAll => {
                        return Err(EdgeError::Unimplemented(
                            "bulk activity option updates are not implemented".to_string(),
                        ));
                    }
                };
                let run_key = self
                    .resolve_execution_run_key(
                        &req.namespace,
                        &req.workflow_id,
                        req.run_id.as_deref(),
                    )
                    .await?;
                let command =
                    build_update_activity_options_command(&ctx, activity_id.clone(), &req)?;
                let outcome = self
                    .runtime
                    .update_activity_options(run_key, command)
                    .await
                    .map_err(EdgeError::from)?;
                let activity_options = self
                    .load_activity_options(run_key, &req.namespace, &req.workflow_id, &activity_id)
                    .await?;
                self.notify_history_run_key(run_key, outcome.last_event_id)
                    .await;
                Ok(UpdateActivityOptionsResponse {
                    activity_options: Some(activity_options),
                })
            },
        )
        .await
    }

    // ── Advanced workflow endpoints ──

    pub async fn terminate_workflow_execution(
        &self,
        headers: &HeaderMap,
        req: TerminateWorkflowExecutionRequest,
    ) -> EdgeResult<TerminateWorkflowExecutionResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "terminate_workflow_execution",
            Some(namespace_label.as_str()),
            None,
            async move {
                let ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::TerminateWorkflowExecution,
                        false,
                    )
                    .await?;

                ensure_local(
                    self.router
                        .route_workflow(&req.namespace, &req.workflow_id)
                        .await?,
                )?;

                let run_key = self
                    .resolve_run_key(&req.namespace, &req.workflow_id)
                    .await?;

                let internal = to_internal::terminate_request(req, &ctx.request_id);
                let outcome = self
                    .runtime
                    .terminate_workflow(run_key, internal)
                    .await
                    .map_err(EdgeError::from)?;
                self.notify_history_run_key(run_key, outcome.last_event_id)
                    .await;

                Ok(from_internal::terminate_response(outcome))
            },
        )
        .await
    }

    pub async fn pause_workflow_execution(
        &self,
        headers: &HeaderMap,
        req: PauseWorkflowExecutionRequest,
    ) -> EdgeResult<PauseWorkflowExecutionResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "pause_workflow_execution",
            Some(namespace_label.as_str()),
            None,
            async move {
                let ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::PauseWorkflowExecution,
                        false,
                    )
                    .await?;

                if req.workflow_id.is_empty() {
                    return Err(EdgeError::BadRequest(
                        "pause_workflow_execution requires workflow_id".to_string(),
                    ));
                }

                ensure_local(
                    self.router
                        .route_workflow(&req.namespace, &req.workflow_id)
                        .await?,
                )?;

                let run_key = self
                    .resolve_run_key(&req.namespace, &req.workflow_id)
                    .await?;

                let internal = to_internal::pause_request(req, &ctx.request_id);
                let outcome = self
                    .runtime
                    .pause_workflow(run_key, internal)
                    .await
                    .map_err(EdgeError::from)?;
                self.notify_history_run_key(run_key, outcome.last_event_id)
                    .await;

                Ok(PauseWorkflowExecutionResponse)
            },
        )
        .await
    }

    pub async fn unpause_workflow_execution(
        &self,
        headers: &HeaderMap,
        req: UnpauseWorkflowExecutionRequest,
    ) -> EdgeResult<UnpauseWorkflowExecutionResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "unpause_workflow_execution",
            Some(namespace_label.as_str()),
            None,
            async move {
                let ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::UnpauseWorkflowExecution,
                        false,
                    )
                    .await?;

                if req.workflow_id.is_empty() {
                    return Err(EdgeError::BadRequest(
                        "unpause_workflow_execution requires workflow_id".to_string(),
                    ));
                }

                ensure_local(
                    self.router
                        .route_workflow(&req.namespace, &req.workflow_id)
                        .await?,
                )?;

                let run_key = self
                    .resolve_run_key(&req.namespace, &req.workflow_id)
                    .await?;

                let internal = to_internal::unpause_request(req, &ctx.request_id);
                let outcome = self
                    .runtime
                    .unpause_workflow(run_key, internal)
                    .await
                    .map_err(EdgeError::from)?;
                self.notify_history_run_key(run_key, outcome.last_event_id)
                    .await;

                Ok(UnpauseWorkflowExecutionResponse)
            },
        )
        .await
    }

    pub async fn request_cancel_workflow_execution(
        &self,
        headers: &HeaderMap,
        req: RequestCancelWorkflowExecutionRequest,
    ) -> EdgeResult<RequestCancelWorkflowExecutionResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "request_cancel_workflow_execution",
            Some(namespace_label.as_str()),
            None,
            async move {
                let ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::RequestCancelWorkflowExecution,
                        false,
                    )
                    .await?;

                ensure_local(
                    self.router
                        .route_workflow(&req.namespace, &req.workflow_id)
                        .await?,
                )?;

                let run_key = self
                    .resolve_run_key(&req.namespace, &req.workflow_id)
                    .await?;

                let internal = to_internal::cancel_request(req, &ctx.request_id);
                let outcome = self
                    .runtime
                    .cancel_workflow(run_key, internal)
                    .await
                    .map_err(EdgeError::from)?;
                self.notify_history_run_key(run_key, outcome.last_event_id)
                    .await;

                Ok(from_internal::cancel_response(outcome))
            },
        )
        .await
    }

    /// Execute a synchronous query against a workflow.
    ///
    /// This delegates to the runtime's `query_workflow`, which internally
    /// uses a two-path dispatch: if the run is idle (quiescent), the query
    /// is sent directly through the broker to a poller; if the run has an
    /// active WFT, the query is buffered behind a consistency barrier and
    /// attached to the next poll response. The edge layer doesn't need to
    /// know which path was taken — the runtime handles the routing and
    /// returns the result through the same `QueryResult` channel.
    pub async fn query_workflow(
        &self,
        headers: &HeaderMap,
        req: QueryWorkflowRequest,
    ) -> EdgeResult<QueryWorkflowResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "query_workflow",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(headers, Some(&req.namespace), Action::QueryWorkflow, false)
                    .await?;

                ensure_local(
                    self.router
                        .route_workflow(&req.namespace, &req.workflow_id)
                        .await?,
                )?;

                let _run_key = self
                    .resolve_run_key(&req.namespace, &req.workflow_id)
                    .await?;

                let workflow_id = req.workflow_id.clone();
                let execution = ExecutionRef {
                    namespace_id: to_internal::namespace_id_for(&req.namespace),
                    workflow_id: tokeira_types::WorkflowId(workflow_id),
                    run_id: None,
                };

                let result = self
                    .runtime
                    .query_workflow(execution, req.query_type, req.query_args, req.timeout)
                    .await
                    .map_err(EdgeError::from)?;

                Ok(from_internal::query_response(result))
            },
        )
        .await
    }

    /// Submit a workflow update and optionally wait for its outcome.
    ///
    /// The `wait_policy` controls how long the caller blocks. The update RPC
    /// defaults an absent/unspecified policy to `Completed` and rejects
    /// `Admitted`; poll requests preserve all stages so callers can ask for the
    /// current lifecycle state without blocking.
    pub async fn update_workflow_execution(
        &self,
        headers: &HeaderMap,
        req: UpdateWorkflowExecutionRequest,
    ) -> EdgeResult<UpdateWorkflowExecutionResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "update_workflow_execution",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::UpdateWorkflowExecution,
                        false,
                    )
                    .await?;

                ensure_local(
                    self.router
                        .route_workflow(&req.namespace, &req.workflow_id)
                        .await?,
                )?;

                let run_key = self
                    .resolve_execution_run_key(
                        &req.namespace,
                        &req.workflow_id,
                        req.run_id.as_deref(),
                    )
                    .await?;
                let state = match self.repo.load_run(run_key).await.map_err(EdgeError::from)? {
                    LoadedRun::Existing(state) => state,
                    LoadedRun::Absent => {
                        return Err(EdgeError::WorkflowNotFound {
                            namespace: req.namespace.clone(),
                            workflow_id: req.workflow_id.clone(),
                        });
                    }
                };
                if let Some(first_execution_run_id) = req
                    .first_execution_run_id
                    .as_deref()
                    .filter(|value| !value.is_empty())
                {
                    let first_execution_run_id = Uuid::parse_str(first_execution_run_id)
                        .map(RunId)
                        .map_err(|err| {
                            EdgeError::BadRequest(format!(
                                "invalid first_execution_run_id `{first_execution_run_id}`: {err}"
                            ))
                        })?;
                    if state.first_execution_run_id != Some(first_execution_run_id) {
                        return Err(EdgeError::WorkflowNotFound {
                            namespace: req.namespace.clone(),
                            workflow_id: req.workflow_id.clone(),
                        });
                    }
                }

                let update_id = if req.update_id.is_empty() {
                    Uuid::new_v4().to_string()
                } else {
                    req.update_id
                };
                let execution = ExecutionRef {
                    namespace_id: to_internal::namespace_id_for(&req.namespace),
                    workflow_id: tokeira_types::WorkflowId(req.workflow_id.clone()),
                    run_id: Some(state.run_id),
                };

                let wait_policy = match req.wait_policy {
                    crate::translate::UpdateWaitPolicyDto::Unspecified => {
                        UpdateWaitPolicy::Completed
                    }
                    crate::translate::UpdateWaitPolicyDto::Admitted => {
                        return Err(EdgeError::BadRequest(
                            "UpdateWorkflowExecution does not support waiting for ADMITTED"
                                .to_string(),
                        ));
                    }
                    crate::translate::UpdateWaitPolicyDto::Accepted => UpdateWaitPolicy::Accepted,
                    crate::translate::UpdateWaitPolicyDto::Completed => UpdateWaitPolicy::Completed,
                };

                let request = RequestContext {
                    request_id: tokeira_types::RequestId(uuid::Uuid::new_v4().to_string()),
                    caller_identity: None,
                    received_at: time::OffsetDateTime::now_utc(),
                };

                let outcome = self
                    .runtime
                    .update_workflow(
                        execution,
                        update_id,
                        req.update_name,
                        req.input,
                        request,
                        req.timeout,
                        wait_policy,
                    )
                    .await
                    .map_err(|error| {
                        map_update_lifecycle_error(error, &req.namespace, &req.workflow_id)
                    })?;
                let last_event_id = read_last_event_id(self.repo.as_ref(), run_key).await?;
                self.notify_history_run_key(run_key, last_event_id).await;

                Ok(from_internal::update_response(outcome))
            },
        )
        .await
    }

    pub async fn poll_workflow_execution_update(
        &self,
        headers: &HeaderMap,
        namespace: String,
        workflow_id: String,
        run_id_str: String,
        update_id: String,
        wait_policy: UpdateWaitPolicy,
    ) -> EdgeResult<UpdateLifecycleSnapshot> {
        let namespace_label = namespace.clone();
        self.observe_edge_call(
            headers,
            "poll_workflow_execution_update",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&namespace),
                        Action::UpdateWorkflowExecution,
                        false,
                    )
                    .await?;

                ensure_local(self.router.route_workflow(&namespace, &workflow_id).await?)?;

                let run_key = self
                    .resolve_execution_run_key(
                        &namespace,
                        &workflow_id,
                        Some(run_id_str.as_str()).filter(|value| !value.is_empty()),
                    )
                    .await?;
                let state = match self.repo.load_run(run_key).await.map_err(EdgeError::from)? {
                    LoadedRun::Existing(state) => state,
                    LoadedRun::Absent => {
                        return Err(EdgeError::WorkflowNotFound {
                            namespace,
                            workflow_id,
                        });
                    }
                };
                let execution = ExecutionRef {
                    namespace_id: to_internal::namespace_id_for(&namespace),
                    workflow_id: tokeira_types::WorkflowId(workflow_id.clone()),
                    run_id: Some(state.run_id),
                };

                self.runtime
                    .poll_workflow_update(
                        execution,
                        update_id,
                        wait_policy,
                        std::time::Duration::from_secs(60),
                    )
                    .await
                    .map_err(|error| map_update_lifecycle_error(error, &namespace, &workflow_id))
            },
        )
        .await
    }

    // ── History ──

    pub async fn get_workflow_execution_history(
        &self,
        headers: &HeaderMap,
        req: crate::translate::GetWorkflowExecutionHistoryRequest,
    ) -> EdgeResult<crate::translate::GetWorkflowExecutionHistoryResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "get_workflow_execution_history",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::DescribeWorkflowExecution,
                        false,
                    )
                    .await?;

                ensure_local(
                    self.router
                        .route_workflow(&req.namespace, &req.workflow_id)
                        .await?,
                )?;

                let run_key = self
                    .resolve_execution_run_key(
                        &req.namespace,
                        &req.workflow_id,
                        req.run_id.as_deref(),
                    )
                    .await?;
                let caller_last_event_id = decode_history_page_token(&req.next_page_token)
                    .map_err(EdgeError::BadRequest)?;
                // Transient events go to every client except the CLI/UI
                // (`ClientSupportsTranOrSpecEvents`, get_history_util.go:427 @ v1.31.0).
                let client_supports_transient_events = !matches!(
                    headers
                        .get("client-name")
                        .and_then(|value| value.to_str().ok()),
                    Some("temporal-cli") | Some("temporal-ui")
                );
                let limit = if req.maximum_page_size > 0 {
                    req.maximum_page_size
                } else {
                    usize::MAX
                };

                loop {
                    let history = self
                        .repo
                        .read_history(run_key, caller_last_event_id, limit)
                        .await
                        .map_err(EdgeError::from)?;
                    let current_last_event_id = history
                        .last()
                        .map(|event| event.event_id)
                        .unwrap_or(caller_last_event_id);
                    let filtered = filter_history_events(&history, req.history_event_filter_type);

                    tracing::debug!(
                        run_key = ?run_key,
                        caller_last_event_id,
                        current_last_event_id,
                        total_events = history.len(),
                        filtered_count = filtered.len(),
                        filter_type = req.history_event_filter_type,
                        wait_new_event = req.wait_new_event,
                        "get_workflow_execution_history loop iteration"
                    );

                    if !filtered.is_empty() || !req.wait_new_event {
                        // v1.31.0 emits an empty next_page_token once there is no more history to
                        // return AND (the workflow is closed OR this is not a long-poll); a
                        // non-empty token tells the client to keep paging / following, an empty
                        // token tells it to stop. tokeira previously always encoded a token, so
                        // the Go `GetHistory` helper (loops until len(token)==0) span-looped
                        // forever against finished workflows — the source of the apparent suite
                        // hang. service/history/api/getworkflowexecutionhistory/api.go:488 (v1.31.0).
                        let more_events = history.len() >= limit;
                        let reached_close = history
                            .iter()
                            .any(|event| is_close_history_event(&event.kind));
                        let next_page_token =
                            if more_events || (req.wait_new_event && !reached_close) {
                                encode_history_page_token(current_last_event_id)
                            } else {
                                Vec::new()
                            };
                        let mut events: Vec<_> = filtered.into_iter().take(limit).collect();
                        // Transient-suffix synthesis (spec transient-wft Req B.7): on the
                        // FINAL page of an unfiltered read, append the transient (attempt>1)
                        // pending task's unpersisted Scheduled(+Started) at their virtual
                        // ids so a mid-retry reader sees the task Temporal would show
                        // (`appendTransientTasks`, getworkflowexecutionhistory/api.go:32-116
                        // @ v1.31.0). Gated off for CLI/UI clients
                        // (`ClientSupportsTranOrSpecEvents`, get_history_util.go:427: every
                        // client EXCEPT temporal-cli / temporal-ui receives them).
                        if next_page_token.is_empty()
                            && req.history_event_filter_type != 2
                            && client_supports_transient_events
                        {
                            append_transient_suffix(&mut events, self.repo.as_ref(), run_key)
                                .await?;
                        }
                        return Ok(crate::translate::GetWorkflowExecutionHistoryResponse {
                            history: events,
                            next_page_token,
                        });
                    }

                    if req.history_event_filter_type != 2
                        && current_last_event_id > caller_last_event_id
                    {
                        return Ok(crate::translate::GetWorkflowExecutionHistoryResponse {
                            history: Vec::new(),
                            next_page_token: encode_history_page_token(current_last_event_id),
                        });
                    }

                    let mut wait = self
                        .history_waiters
                        .receiver(run_key, current_last_event_id)
                        .await;
                    if tokio::time::timeout(Duration::from_secs(60), wait.changed())
                        .await
                        .is_err()
                    {
                        return Ok(crate::translate::GetWorkflowExecutionHistoryResponse {
                            history: Vec::new(),
                            next_page_token: encode_history_page_token(current_last_event_id),
                        });
                    }
                }
            },
        )
        .await
    }

    pub async fn get_workflow_execution_history_reverse(
        &self,
        headers: &HeaderMap,
        req: crate::translate::GetWorkflowExecutionHistoryReverseRequest,
    ) -> EdgeResult<crate::translate::GetWorkflowExecutionHistoryReverseResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "get_workflow_execution_history_reverse",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::GetWorkflowExecutionHistoryReverse,
                        false,
                    )
                    .await?;

                ensure_local(
                    self.router
                        .route_workflow(&req.namespace, &req.workflow_id)
                        .await?,
                )?;

                let run_key = self
                    .resolve_execution_run_key(
                        &req.namespace,
                        &req.workflow_id,
                        req.run_id.as_deref(),
                    )
                    .await?;
                let history = self
                    .repo
                    .read_history(run_key, 0, usize::MAX)
                    .await
                    .map_err(EdgeError::from)?;

                let before_event_id = decode_reverse_history_page_token(&req.next_page_token)
                    .map_err(EdgeError::BadRequest)?;
                let limit = if req.maximum_page_size > 0 {
                    req.maximum_page_size
                } else {
                    usize::MAX
                };

                let mut reversed: Vec<_> = history
                    .into_iter()
                    .filter(|event| {
                        before_event_id
                            .map(|value| event.event_id < value)
                            .unwrap_or(true)
                    })
                    .collect();
                reversed.sort_by_key(|event| std::cmp::Reverse(event.event_id));

                let page: Vec<_> = reversed.into_iter().take(limit).collect();
                let next_page_token = page
                    .last()
                    .map(|event| encode_reverse_history_page_token(event.event_id))
                    .unwrap_or_default();

                Ok(
                    crate::translate::GetWorkflowExecutionHistoryReverseResponse {
                        history: page,
                        next_page_token,
                    },
                )
            },
        )
        .await
    }

    // ── Helpers ──

    async fn resolve_run_key(
        &self,
        namespace: &str,
        workflow_id: &str,
    ) -> EdgeResult<tokeira_types::RunKey> {
        self.resolver
            .current_run_key(namespace, workflow_id)
            .await
            .map_err(EdgeError::from)?
            .ok_or(EdgeError::WorkflowNotFound {
                namespace: namespace.to_string(),
                workflow_id: workflow_id.to_string(),
            })
    }

    async fn resolve_activity_token_for_edge(
        &self,
        run_key: RunKey,
        activity_id: &str,
        namespace: &str,
        workflow_id: &str,
    ) -> EdgeResult<ActivityTaskToken> {
        self.runtime
            .resolve_activity_token(run_key, activity_id)
            .await
            .map_err(|error| {
                self.map_activity_resolution_error(error, namespace, workflow_id, activity_id)
            })
    }

    fn map_activity_resolution_error(
        &self,
        error: ActivityTokenResolutionError,
        namespace: &str,
        workflow_id: &str,
        activity_id: &str,
    ) -> EdgeError {
        match error {
            ActivityTokenResolutionError::RunNotFound { .. } => EdgeError::WorkflowNotFound {
                namespace: namespace.to_string(),
                workflow_id: workflow_id.to_string(),
            },
            ActivityTokenResolutionError::ActivityNotFound { .. } => EdgeError::ActivityNotFound {
                namespace: namespace.to_string(),
                workflow_id: workflow_id.to_string(),
                activity_id: activity_id.to_string(),
            },
            ActivityTokenResolutionError::ActivityNotStarted { .. } => {
                EdgeError::ActivityNotStarted {
                    namespace: namespace.to_string(),
                    workflow_id: workflow_id.to_string(),
                    activity_id: activity_id.to_string(),
                }
            }
            ActivityTokenResolutionError::Runtime(message) => EdgeError::Internal(message),
        }
    }

    async fn load_activity_options(
        &self,
        run_key: RunKey,
        namespace: &str,
        workflow_id: &str,
        activity_id: &str,
    ) -> EdgeResult<crate::translate::ActivityOptions> {
        let loaded = self.repo.load_run(run_key).await.map_err(EdgeError::from)?;
        let tokeira_kernel::LoadedRun::Existing(state) = loaded else {
            return Err(EdgeError::WorkflowNotFound {
                namespace: namespace.to_string(),
                workflow_id: workflow_id.to_string(),
            });
        };
        let activity =
            state
                .activities
                .get(activity_id)
                .ok_or_else(|| EdgeError::ActivityNotFound {
                    namespace: namespace.to_string(),
                    workflow_id: workflow_id.to_string(),
                    activity_id: activity_id.to_string(),
                })?;
        Ok(crate::translate::ActivityOptions {
            task_queue: Some(activity.task_queue.0.clone()),
            schedule_to_close_timeout: activity.schedule_to_close_timeout,
            schedule_to_start_timeout: activity.schedule_to_start_timeout,
            start_to_close_timeout: activity.start_to_close_timeout,
            heartbeat_timeout: activity.heartbeat_timeout,
            retry_policy: activity.retry_policy.clone(),
        })
    }

    async fn resolve_execution_run_key(
        &self,
        namespace: &str,
        workflow_id: &str,
        run_id: Option<&str>,
    ) -> EdgeResult<RunKey> {
        let run_id = match run_id.filter(|value| !value.is_empty()) {
            Some(value) => Some(Uuid::parse_str(value).map(RunId).map_err(|err| {
                EdgeError::BadRequest(format!("invalid run_id `{value}`: {err}"))
            })?),
            None => None,
        };
        let execution = ExecutionRef {
            namespace_id: to_internal::namespace_id_for(namespace),
            workflow_id: tokeira_types::WorkflowId(workflow_id.to_string()),
            run_id,
        };
        if let Some(run_key) = self
            .repo
            .resolve_execution(&execution)
            .await
            .map_err(EdgeError::from)?
        {
            return Ok(run_key);
        }
        // `resolve_execution(run_id=None)` is open-only by repo contract; history reads must
        // resolve the current execution (open or latest-closed) like v1.31.0 history-by-
        // workflow-id, which serves closed runs (`workflow_handler.go:898 @ v1.31.0`). This
        // mirrors the fallback StoreExecutionResolver already applies to describe. An explicit
        // run_id is an exact lookup and never falls back.
        if execution.run_id.is_none()
            && let Some(run_key) = self
                .repo
                .find_latest_run(execution.namespace_id, &execution.workflow_id)
                .await
                .map_err(EdgeError::from)?
        {
            return Ok(run_key);
        }
        Err(EdgeError::WorkflowNotFound {
            namespace: namespace.to_string(),
            workflow_id: workflow_id.to_string(),
        })
    }

    fn execution_ref_from_batch(
        &self,
        ctx: &BatchDispatchContext,
        workflow_ref: &tokeira_runtime::WorkflowExecutionRef,
    ) -> EdgeResult<ExecutionRef> {
        Ok(ExecutionRef {
            namespace_id: ctx.namespace_id,
            workflow_id: WorkflowId(workflow_ref.workflow_id.clone()),
            run_id: workflow_ref
                .run_id
                .as_deref()
                .map(Uuid::parse_str)
                .transpose()
                .map_err(|err| EdgeError::BadRequest(err.to_string()))?
                .map(RunId),
        })
    }

    async fn resolve_batch_run_key(
        &self,
        ctx: &BatchDispatchContext,
        workflow_ref: &tokeira_runtime::WorkflowExecutionRef,
    ) -> EdgeResult<RunKey> {
        let execution = self.execution_ref_from_batch(ctx, workflow_ref)?;
        self.repo
            .resolve_execution(&execution)
            .await
            .map_err(EdgeError::from)?
            .ok_or(EdgeError::WorkflowNotFound {
                namespace: ctx.namespace_name.clone(),
                workflow_id: workflow_ref.workflow_id.clone(),
            })
    }

    async fn notify_history_run_key(&self, run_key: RunKey, last_event_id: i64) {
        self.history_waiters.notify(run_key, last_event_id).await;
    }
}

fn grpc_error_code(error: &EdgeError) -> &'static str {
    match error {
        EdgeError::BadRequest(_) => "invalid_argument",
        EdgeError::Unimplemented(_) => "unimplemented",
        EdgeError::NotFound(_) => "not_found",
        EdgeError::AlreadyExists(_) => "already_exists",
        EdgeError::ResourceExhausted(_) => "resource_exhausted",
        EdgeError::Unauthorized(_) => "unauthenticated",
        EdgeError::Forbidden { .. } => "permission_denied",
        EdgeError::NamespaceNotFound(_)
        | EdgeError::WorkflowNotFound { .. }
        | EdgeError::ActivityNotFound { .. }
        | EdgeError::BatchOperationNotFound { .. } => "not_found",
        EdgeError::ActivityNotStarted { .. } => "failed_precondition",
        EdgeError::WorkflowAlreadyStarted { .. }
        | EdgeError::BatchOperationAlreadyExists { .. }
        | EdgeError::NamespaceAlreadyExists(_) => "already_exists",
        EdgeError::ActivityExecutionAlreadyStarted { .. } => "already_exists",
        EdgeError::NamespaceDeleted(_) => "failed_precondition",
        EdgeError::TooManyLongPolls => "resource_exhausted",
        EdgeError::LongPollAdmissionTimeout => "deadline_exceeded",
        EdgeError::RemoteRouteUnsupported { .. } => "unavailable",
        EdgeError::NotShardOwner { .. } => "aborted",
        EdgeError::FailedPrecondition(_) => "failed_precondition",
        EdgeError::Internal(_) => "internal",
    }
}

/// Append the unpersisted transient-WFT suffix to a final-page history read
/// (spec transient-wft Req B.7). A transient (attempt>1) pending task's
/// Scheduled/Started events exist only virtually (`GetTransientWorkflowTaskInfo`
/// mutable_state_impl.go:1189-1250 @ v1.31.0); mid-retry readers see them
/// appended after the last persisted event, and they vanish once the retry
/// chain materializes or the run closes. Synthesizes Scheduled always and
/// Started only when the task is started, at ids last+1 / last+2.
async fn append_transient_suffix(
    events: &mut Vec<tokeira_kernel::HistoryEvent>,
    repo: &dyn tokeira_storage::RunRepository,
    run_key: tokeira_types::RunKey,
) -> EdgeResult<()> {
    let tokeira_kernel::LoadedRun::Existing(state) =
        repo.load_run(run_key).await.map_err(EdgeError::from)?
    else {
        return Ok(());
    };
    if !state.is_open() {
        return Ok(());
    }
    let Some(pending) = state.pending_workflow_task.as_ref() else {
        return Ok(());
    };
    // Transient = attempt>1 with a virtual (unpersisted) scheduled id.
    if pending.attempt <= 1 || pending.scheduled_event_id != state.last_event_id + 1 {
        return Ok(());
    }
    // Only append when the read actually reached the end of persisted history.
    if events.last().map(|event| event.event_id) != Some(state.last_event_id)
        && !(events.is_empty() && state.last_event_id == 0)
    {
        return Ok(());
    }
    events.push(tokeira_kernel::HistoryEvent {
        event_id: pending.scheduled_event_id,
        happened_at: pending.scheduled_at,
        kind: tokeira_kernel::HistoryEventKind::WorkflowTaskScheduled {
            logical_seq: pending.logical_seq,
            task_queue: state.task_queue.clone(),
            workflow_task_timeout: state.workflow_task_timeout,
            attempt: pending.attempt,
        },
    });
    if let (Some(started_event_id), Some(started_at)) =
        (pending.started_event_id, pending.started_at)
    {
        events.push(tokeira_kernel::HistoryEvent {
            event_id: started_event_id,
            happened_at: started_at,
            kind: tokeira_kernel::HistoryEventKind::WorkflowTaskStarted {
                logical_seq: pending.logical_seq,
                scheduled_event_id: pending.scheduled_event_id,
                attempt: pending.attempt,
                identity: tokeira_types::WorkerIdentity(String::new()),
                request_id: format!("transient-{}-{}", pending.logical_seq.0, pending.attempt),
                history_size_bytes: 0,
                suggest_continue_as_new: false,
            },
        });
    }
    Ok(())
}

fn decode_history_page_token(token: &[u8]) -> std::result::Result<i64, String> {
    if token.is_empty() {
        return Ok(0);
    }
    if token.len() != 8 {
        return Err("invalid history next_page_token".to_string());
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(token);
    Ok(i64::from_be_bytes(bytes))
}

fn encode_history_page_token(last_event_id: i64) -> Vec<u8> {
    last_event_id.to_be_bytes().to_vec()
}

fn decode_reverse_history_page_token(token: &[u8]) -> std::result::Result<Option<i64>, String> {
    if token.is_empty() {
        return Ok(None);
    }
    if token.len() != 8 {
        return Err("invalid reverse history next_page_token".to_string());
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(token);
    Ok(Some(i64::from_be_bytes(bytes)))
}

fn encode_reverse_history_page_token(before_event_id: i64) -> Vec<u8> {
    before_event_id.to_be_bytes().to_vec()
}

fn filter_history_events(history: &[HistoryEvent], filter_type: i32) -> Vec<HistoryEvent> {
    if filter_type != 2 {
        return history.to_vec();
    }
    history
        .iter()
        .filter(|event| is_close_history_event(&event.kind))
        .cloned()
        .collect()
}

fn is_close_history_event(kind: &HistoryEventKind) -> bool {
    matches!(
        kind,
        HistoryEventKind::WorkflowExecutionCompleted { .. }
            | HistoryEventKind::WorkflowExecutionFailed { .. }
            | HistoryEventKind::WorkflowExecutionTimedOut { .. }
            | HistoryEventKind::WorkflowExecutionCanceled { .. }
            | HistoryEventKind::WorkflowExecutionTerminated { .. }
            | HistoryEventKind::WorkflowExecutionContinuedAsNew { .. }
    )
}

fn validate_reset_target(history: &[HistoryEvent], fork_event_id: i64) -> EdgeResult<()> {
    let Some(event) = history.iter().find(|event| event.event_id == fork_event_id) else {
        return Err(EdgeError::BadRequest(format!(
            "reset target event_id {} not found",
            fork_event_id
        )));
    };

    if !matches!(
        event.kind,
        HistoryEventKind::WorkflowTaskCompleted { .. }
            | HistoryEventKind::WorkflowTaskFailed { .. }
            | HistoryEventKind::WorkflowTaskTimedOut { .. }
            | HistoryEventKind::WorkflowTaskStarted { .. }
    ) {
        return Err(EdgeError::BadRequest(format!(
            "reset target event_id {} must be a workflow task completed/failed/timed out/started event",
            fork_event_id
        )));
    }

    Ok(())
}

fn batch_request_context(ctx: &BatchDispatchContext) -> RequestContext {
    RequestContext {
        request_id: RequestId(ctx.edge_context.request_id.as_str().to_string()),
        caller_identity: Some(ctx.identity.clone()),
        received_at: ctx.edge_context.received_at,
    }
}

fn batch_error_to_edge(
    error: BatchError,
    namespace: &str,
    job_id: &tokeira_runtime::JobId,
) -> EdgeError {
    match error {
        BatchError::AlreadyExists => EdgeError::BatchOperationAlreadyExists {
            namespace: namespace.to_string(),
            job_id: job_id.0.clone(),
        },
        BatchError::NotFound => EdgeError::BatchOperationNotFound {
            namespace: namespace.to_string(),
            job_id: job_id.0.clone(),
        },
        BatchError::InvalidArgument(message) => EdgeError::BadRequest(message),
    }
}

fn map_update_lifecycle_error(
    error: anyhow::Error,
    namespace: &str,
    workflow_id: &str,
) -> EdgeError {
    match error.downcast::<UpdateLifecycleError>() {
        Ok(UpdateLifecycleError::UpdateNotFound { update_id, .. }) => {
            EdgeError::NotFound(format!("update {update_id} not found"))
        }
        Err(error) if error.to_string().contains("execution not found") => {
            EdgeError::WorkflowNotFound {
                namespace: namespace.to_string(),
                workflow_id: workflow_id.to_string(),
            }
        }
        Err(error) => EdgeError::from(error),
    }
}

async fn read_last_event_id(repo: &dyn RunRepository, run_key: RunKey) -> Result<i64> {
    Ok(repo
        .read_history(run_key, 0, usize::MAX)
        .await?
        .last()
        .map(|event| event.event_id)
        .unwrap_or(0))
}

/// Validate a namespace state transition against the v1.31.0 rules.
///
/// Tokeira's scoped namespace model only tracks a boolean `deleted` flag, so
/// the live states reduce to `Registered` (not deleted) and `Deleted`. The
/// `Deprecated` intermediate state is accepted as a request target but, since
/// it is not persisted, behaves as a no-op against a live namespace. The
/// rejection surface still matches v1.31.0 `validateStateUpdate`
/// (`service/frontend/namespace_handler.go @ v1.31.0`): any transition out of
/// `Deleted` is rejected, and `Unspecified`/same-state targets are no-ops.
fn validate_namespace_state_update(deleted: bool, target: NamespaceStateUpdate) -> EdgeResult<()> {
    match (deleted, target) {
        // No state change requested.
        (_, NamespaceStateUpdate::Unspecified) => Ok(()),
        // A deleted namespace cannot transition to any other state. This also
        // covers the same-state `Deleted → Deleted` no-op, which is harmless.
        (true, NamespaceStateUpdate::Deleted) => Ok(()),
        (true, _) => Err(EdgeError::BadRequest(
            "invalid namespace state update: namespace is deleted".to_string(),
        )),
        // Registered (live) → {Registered, Deprecated, Deleted} are all
        // permitted: Registered is a same-state no-op, Deprecated is accepted
        // but not persisted, and Deleted is the real transition operators use.
        (false, _) => Ok(()),
    }
}

fn namespace_to_description(namespace: ResolvedNamespace) -> NamespaceDescription {
    NamespaceDescription {
        name: namespace.name,
        namespace_id: namespace.namespace_id,
        is_global: namespace.is_global,
        visibility_enabled: namespace.visibility_enabled,
        deleted: namespace.deleted,
        description: String::new(),
        owner_email: String::new(),
        cluster_name: "local".to_string(),
        custom_search_attribute_aliases: std::collections::BTreeMap::new(),
        capabilities: NamespaceCapabilities {
            worker_heartbeats: true,
            reported_problems_search_attribute: false,
        },
        retention: namespace.retention,
    }
}

fn queue_key_for_poll(
    namespace: &str,
    task_queue: &str,
    task_kind: TaskKind,
    deployment: Option<tokeira_types::DeploymentId>,
    build_id: Option<tokeira_types::BuildId>,
) -> tokeira_types::QueueKey {
    tokeira_types::QueueKey {
        namespace_id: to_internal::namespace_id_for(namespace),
        task_queue: TaskQueueName(task_queue.to_string()),
        task_kind,
        deployment,
        build_id,
    }
}

fn collect_eager_activity_specs(
    commands: &[tokeira_kernel::WorkflowCommand],
    limit: usize,
) -> Vec<(
    String,
    TaskQueueName,
    Option<tokeira_types::DeploymentId>,
    Option<tokeira_types::BuildId>,
)> {
    commands
        .iter()
        .filter_map(|command| match command {
            tokeira_kernel::WorkflowCommand::ScheduleActivity {
                activity_id,
                task_queue,
                deployment,
                build_id,
                request_eager_execution: true,
                ..
            } => Some((
                activity_id.clone(),
                task_queue.clone(),
                deployment.clone(),
                build_id.clone(),
            )),
            _ => None,
        })
        .take(limit)
        .collect()
}

fn active_poller_to_edge(poller: ActivePoller) -> crate::translate::PollerInfo {
    crate::translate::PollerInfo {
        identity: poller.identity.0,
        last_access_time: Some(poller.registered_at),
        rate_per_second: 0.0,
    }
}

fn task_queue_config_to_edge(entry: TaskQueueConfigEntry) -> TaskQueueConfig {
    TaskQueueConfig {
        queue_rate_limit: entry.queue_rate_limit,
        fairness_key_rate_limit_default: entry.fairness_key_rate_limit_default,
        fairness_weight_overrides: entry.fairness_weight_overrides,
    }
}

/// Map the registry's task-queue versioning view onto the edge DTO. The storage
/// `WorkerDeploymentVersionKey` becomes a proto-free `(deployment_name, build_id)`
/// pair; the deprecated string fields are derived later in the gRPC layer.
fn task_queue_versioning_view_to_edge(
    view: TaskQueueVersioningView,
) -> crate::translate::TaskQueueVersioningInfo {
    let to_id = |version: tokeira_storage::WorkerDeploymentVersionKey| {
        crate::translate::WorkerDeploymentVersionId {
            deployment_name: version.deployment_name.0,
            build_id: version.build_id.0,
        }
    };
    crate::translate::TaskQueueVersioningInfo {
        current_deployment_version: view.current_version.map(to_id),
        ramping_deployment_version: view.ramping_version.map(to_id),
        ramping_to_unversioned: view.ramping_to_unversioned,
        ramping_version_percentage: view.ramping_percentage,
        update_time: view.update_time,
    }
}

fn is_valid_namespace_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

impl From<serde_json::Error> for EdgeError {
    fn from(value: serde_json::Error) -> Self {
        EdgeError::BadRequest(value.to_string())
    }
}

impl From<std::io::Error> for EdgeError {
    fn from(value: std::io::Error) -> Self {
        EdgeError::Internal(value.to_string())
    }
}

pub fn not_wired_runtime() -> anyhow::Error {
    anyhow!("tokeira-edge runtime adapter is not wired to the current runtime yet")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        EmptyVisibilityApi, ExecutionResolver, WorkflowService, apply_matrix_capability_field,
        build_update_activity_options_command, collect_eager_activity_specs,
        system_capabilities_with_matrix_overlay, worker_identity_from_request,
    };
    use anyhow::Result;
    use async_trait::async_trait;
    use http::HeaderMap;
    use proptest::prelude::*;
    use time::{Duration, OffsetDateTime};
    use tokeira_compatibility::FeatureState;
    use tokeira_kernel::{FieldChange, StartRequest, WorkflowCommand};
    use tokeira_runtime::{
        BacklogConfig, InMemoryBroker, LaneConfig, TimerScannerConfig, TokeiraRuntime,
        UpdateLifecycleStage, UpdateWaitPolicy, WorkflowTimeoutScannerConfig,
    };
    use tokeira_storage::{CommitResult, InMemoryStore};
    use tokeira_types::{
        ExecutionRef, Memo, NamespaceId, Payload, Payloads, RequestContext, RequestId, RunId,
        RunKey, SearchAttributes, TaskQueueName, WorkflowId, WorkflowType,
    };

    use crate::{
        errors::EdgeError,
        grpc::runtime_adapter::RuntimeAdapter,
        interceptors::{EdgeContext, Principal},
        long_poll::{LongPollConfig, LongPollGate},
        namespace_cache::{InMemoryNamespaceCache, NamespaceCache, ResolvedNamespace},
        operator_service::InMemoryOperatorApi,
        poller_registry::PollerRegistry,
        routing::LocalOnlyRouter,
        to_internal::namespace_id_for,
        translate::{
            ActivityOptions, SignalWorkflowExecutionRequest, SystemCapabilities,
            UpdateActivityOptionsRequest, UpdateWaitPolicyDto, UpdateWorkflowExecutionRequest,
        },
    };

    fn arb_small_string() -> impl Strategy<Value = String> {
        prop::collection::vec(prop::char::range('a', 'z'), 1..8)
            .prop_map(|chars| chars.into_iter().collect())
    }

    #[test]
    fn namespace_state_update_matches_v1_31_0_rules() {
        use super::validate_namespace_state_update;
        use crate::translate::NamespaceStateUpdate;

        // Unspecified is always a no-op, regardless of current state.
        assert!(validate_namespace_state_update(false, NamespaceStateUpdate::Unspecified).is_ok());
        assert!(validate_namespace_state_update(true, NamespaceStateUpdate::Unspecified).is_ok());

        // Registered (live) → {Registered, Deprecated, Deleted} all permitted.
        assert!(validate_namespace_state_update(false, NamespaceStateUpdate::Registered).is_ok());
        assert!(validate_namespace_state_update(false, NamespaceStateUpdate::Deprecated).is_ok());
        assert!(validate_namespace_state_update(false, NamespaceStateUpdate::Deleted).is_ok());

        // Deleted → Deleted is a harmless same-state no-op.
        assert!(validate_namespace_state_update(true, NamespaceStateUpdate::Deleted).is_ok());

        // Any other transition out of Deleted is rejected (INVALID_ARGUMENT).
        assert!(matches!(
            validate_namespace_state_update(true, NamespaceStateUpdate::Registered),
            Err(EdgeError::BadRequest(_))
        ));
        assert!(matches!(
            validate_namespace_state_update(true, NamespaceStateUpdate::Deprecated),
            Err(EdgeError::BadRequest(_))
        ));
    }

    fn arb_workflow_command() -> impl Strategy<Value = WorkflowCommand> {
        prop_oneof![
            (arb_small_string(), arb_small_string(), any::<bool>(),).prop_map(
                |(activity_id, task_queue, request_eager_execution)| {
                    WorkflowCommand::ScheduleActivity {
                        activity_id,
                        activity_type: "activity-type".into(),
                        task_queue: TaskQueueName(task_queue),
                        input: Payloads::default(),
                        header: None,
                        request_eager_execution,
                        retry_policy: None,
                        deployment: None,
                        build_id: None,
                        schedule_to_close_timeout: None,
                        schedule_to_start_timeout: None,
                        start_to_close_timeout: None,
                        heartbeat_timeout: None,
                    }
                }
            ),
            arb_small_string().prop_map(|timer_id| WorkflowCommand::CancelTimer { timer_id }),
            Just(WorkflowCommand::CancelWorkflow),
        ]
    }

    fn baseline_capabilities() -> SystemCapabilities {
        SystemCapabilities {
            signal_and_query_header: true,
            internal_error_differentiation: true,
            activity_failure_include_heartbeat: false,
            supports_schedules: false,
            encoded_failure_attributes: true,
            build_id_based_versioning: true,
            upsert_memo: false,
            eager_workflow_start: false,
            sdk_metadata: false,
            count_group_by_execution_status: true,
            nexus: true,
            server_scaled_deployments: false,
            worker_heartbeats: true,
        }
    }

    fn test_edge_context() -> EdgeContext {
        EdgeContext {
            request_id: crate::request_id::RequestId::new("edge-request"),
            principal: Principal::root(),
            namespace: None,
            received_at: time::OffsetDateTime::UNIX_EPOCH,
            is_long_poll: false,
        }
    }

    #[derive(Default)]
    struct NoopResolver;

    #[async_trait]
    impl ExecutionResolver for NoopResolver {
        async fn current_run_key(
            &self,
            _namespace: &str,
            _workflow_id: &str,
        ) -> Result<Option<RunKey>> {
            Ok(None)
        }

        async fn describe_execution(
            &self,
            _namespace: &str,
            _workflow_id: &str,
            _run_id: Option<RunId>,
        ) -> Result<Option<crate::WorkflowExecutionDescription>> {
            Ok(None)
        }
    }

    async fn update_test_service() -> Result<(
        WorkflowService,
        Arc<TokeiraRuntime<InMemoryStore>>,
        NamespaceId,
        WorkflowId,
        RunId,
    )> {
        let store = Arc::new(InMemoryStore::default());
        let runtime = Arc::new(TokeiraRuntime::new(
            store.clone(),
            2,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
            BacklogConfig::default(),
        ));
        let cache = Arc::new(InMemoryNamespaceCache::new());
        cache.insert(ResolvedNamespace::active("default")).await?;
        let service = WorkflowService::new(
            Arc::new(RuntimeAdapter::new(runtime.clone())),
            Arc::new(NoopResolver),
            Arc::new(EmptyVisibilityApi),
            store,
            Arc::new(InMemoryOperatorApi::new("tokeira-local")),
            cache.clone(),
            Arc::new(crate::interceptors::EdgeInterceptors::permissive(cache)),
            PollerRegistry::default(),
            crate::PendingQueryStore::default(),
            InMemoryBroker::default(),
            LongPollGate::new(LongPollConfig::default()),
            Arc::new(LocalOnlyRouter),
        );

        let namespace_id = namespace_id_for("default");
        let workflow_id = WorkflowId("update-edge-workflow".to_string());
        let run_id = RunId::new();
        let result = runtime
            .start_workflow(StartRequest {
                run_key: RunKey::new(),
                namespace_id,
                workflow_id: workflow_id.clone(),
                run_id,
                workflow_type: WorkflowType("workflow-type".to_string()),
                task_queue: TaskQueueName("queue-a".to_string()),
                deployment: None,
                build_id: None,
                versioning_override: None,
                workflow_start_delay: None,
                client_cron_schedule: None,
                completion_callbacks: Vec::new(),
                user_metadata: None,
                links: Vec::new(),
                on_conflict_options: None,
                priority: None,
                input: Payloads::default(),
                header: None,
                memo: Memo::default(),
                search_attributes: SearchAttributes::default(),
                workflow_execution_timeout: None,
                workflow_run_timeout: None,
                workflow_task_timeout: Duration::seconds(10),
                retry_policy: None,
                conflict_policy: tokeira_kernel::WorkflowIdConflictPolicy::Fail,
                reuse_policy: tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
                continued_execution_run_id: None,
                attempt: 1,
                first_execution_run_id: Some(run_id),
                first_run_started_at: None,
                parent_run_key: None,
                parent_workflow_id: None,
                parent_run_id: None,
                parent_namespace_id: None,
                parent_initiated_event_id: 0,
                root_workflow_id: None,
                root_run_id: None,
                original_execution_run_id: Some(run_id),
                continued_failure: None,
                last_completion_result: None,
                request: RequestContext {
                    request_id: RequestId("start-edge-update".to_string()),
                    caller_identity: None,
                    received_at: OffsetDateTime::now_utc(),
                },
                now: OffsetDateTime::now_utc(),
                cron_schedule: None,
                reserved_poller_identity: None,
            })
            .await?;
        assert!(matches!(result, CommitResult::Applied { .. }));

        Ok((service, runtime, namespace_id, workflow_id, run_id))
    }

    fn update_request(
        workflow_id: &WorkflowId,
        run_id: Option<RunId>,
        wait_policy: UpdateWaitPolicyDto,
        update_id: &str,
    ) -> UpdateWorkflowExecutionRequest {
        UpdateWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: workflow_id.0.clone(),
            run_id: run_id.map(|id| id.0.to_string()),
            first_execution_run_id: None,
            update_id: update_id.to_string(),
            update_name: "update-handler".to_string(),
            input: Payloads(vec![Payload {
                metadata: Default::default(),
                data: b"input".to_vec(),
            }]),
            wait_policy,
            timeout: std::time::Duration::from_millis(20),
        }
    }

    fn signal_request(
        workflow_id: &WorkflowId,
        run_id: Option<String>,
    ) -> SignalWorkflowExecutionRequest {
        signal_request_with_request_id(workflow_id, run_id, "signal-1")
    }

    fn signal_request_with_request_id(
        workflow_id: &WorkflowId,
        run_id: Option<String>,
        request_id: &str,
    ) -> SignalWorkflowExecutionRequest {
        SignalWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: workflow_id.0.clone(),
            run_id,
            signal_name: "poke".to_string(),
            input: Payloads::default(),
            header: None,
            links: Vec::new(),
            request_id: Some(request_id.to_string()),
            identity: Some("tester".to_string()),
            now: None,
        }
    }

    fn start_request_for(
        workflow_id: &WorkflowId,
        conflict_policy: tokeira_kernel::WorkflowIdConflictPolicy,
        request_id: &str,
    ) -> crate::translate::StartWorkflowExecutionRequest {
        crate::translate::StartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: workflow_id.0.clone(),
            workflow_type: "workflow-type".to_string(),
            task_queue: "queue-a".to_string(),
            input: Payloads::default(),
            request_id: Some(request_id.to_string()),
            memo: Memo::default(),
            search_attributes: SearchAttributes::default(),
            identity: Some("tester".to_string()),
            request_eager_execution: false,
            workflow_start_delay: None,
            completion_callbacks: Vec::new(),
            user_metadata: None,
            links: Vec::new(),
            eager_worker_deployment_options: None,
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: Some(Duration::seconds(10)),
            retry_policy: None,
            conflict_policy,
            reuse_policy: tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
            header: None,
            versioning_override: None,
            on_conflict_options: None,
            priority: None,
            cron_schedule: None,
            run_key: None,
            run_id: None,
            now: None,
        }
    }

    // Conformance: a UseExisting start that attaches to a running incumbent
    // returns success (existing run id, started=false), NOT AlreadyStarted; only
    // the Fail policy errors (handleUseExistingWorkflowOnConflictOptions vs the
    // Fail arm, service/history/api/startworkflow/api.go @ v1.31.0). The Nexus
    // WorkflowRunOperation depends on this — with
    // WorkflowExecutionErrorWhenAlreadyStarted set, a UseExisting caller must see
    // success to count its operation as started (temporalnexus, sdk v1.41.1).
    #[tokio::test]
    async fn start_use_existing_attaches_without_already_started_error() -> Result<()> {
        let (service, _runtime, _namespace_id, workflow_id, existing_run_id) =
            update_test_service().await?;

        let resp = service
            .start_workflow_execution(
                &HeaderMap::new(),
                start_request_for(
                    &workflow_id,
                    tokeira_kernel::WorkflowIdConflictPolicy::UseExisting,
                    "use-existing-attach",
                ),
            )
            .await?;

        assert!(
            !resp.started,
            "attach must report started=false, not a fresh start"
        );
        assert_eq!(
            resp.run_id, existing_run_id,
            "attach must return the running incumbent's run id"
        );
        Ok(())
    }

    // Negative control: the Fail policy against the same running incumbent must
    // still surface AlreadyStarted (the conflict-policy-fail Nexus losers).
    #[tokio::test]
    async fn start_fail_policy_rejects_running_incumbent() -> Result<()> {
        let (service, _runtime, _namespace_id, workflow_id, _existing_run_id) =
            update_test_service().await?;

        let err = service
            .start_workflow_execution(
                &HeaderMap::new(),
                start_request_for(
                    &workflow_id,
                    tokeira_kernel::WorkflowIdConflictPolicy::Fail,
                    "fail-policy-reject",
                ),
            )
            .await
            .expect_err("Fail policy must reject a running incumbent");

        assert!(
            matches!(err, EdgeError::WorkflowAlreadyStarted { .. }),
            "got {err:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn signal_path_rejects_malformed_run_id_before_lookup() -> Result<()> {
        let (service, _runtime, _namespace_id, workflow_id, _run_id) =
            update_test_service().await?;
        let error = service
            .signal_workflow_execution(
                &HeaderMap::new(),
                signal_request(&workflow_id, Some("not-a-uuid".to_string())),
            )
            .await
            .expect_err("malformed run_id must not be silently ignored");

        assert!(matches!(error, EdgeError::BadRequest(_)));
        assert_eq!(error.status_code(), http::StatusCode::BAD_REQUEST);
        Ok(())
    }

    #[tokio::test]
    async fn signal_path_missing_execution_returns_not_found() -> Result<()> {
        let (service, _runtime, _namespace_id, _workflow_id, _run_id) =
            update_test_service().await?;
        let missing = WorkflowId("missing-workflow".to_string());
        let error = service
            .signal_workflow_execution(&HeaderMap::new(), signal_request(&missing, None))
            .await
            .expect_err("missing execution must map to NOT_FOUND");

        assert!(matches!(error, EdgeError::WorkflowNotFound { .. }));
        assert_eq!(error.status_code(), http::StatusCode::NOT_FOUND);
        Ok(())
    }

    #[tokio::test]
    async fn signal_path_targets_exact_or_current_run() -> Result<()> {
        let (service, _runtime, _namespace_id, workflow_id, run_id) = update_test_service().await?;

        let current_response = service
            .signal_workflow_execution(&HeaderMap::new(), signal_request(&workflow_id, None))
            .await?;
        assert!(current_response.accepted);

        let exact_response = service
            .signal_workflow_execution(
                &HeaderMap::new(),
                signal_request_with_request_id(
                    &workflow_id,
                    Some(run_id.0.to_string()),
                    "signal-2",
                ),
            )
            .await?;
        assert!(exact_response.accepted);

        let missing_run = RunId::new();
        let error = service
            .signal_workflow_execution(
                &HeaderMap::new(),
                signal_request(&workflow_id, Some(missing_run.0.to_string())),
            )
            .await
            .expect_err("valid but unknown run_id must not fall back to current");
        assert!(matches!(error, EdgeError::WorkflowNotFound { .. }));
        assert_eq!(error.status_code(), http::StatusCode::NOT_FOUND);
        Ok(())
    }

    #[tokio::test]
    async fn update_path_rejects_admitted_wait_policy() -> Result<()> {
        let (service, _runtime, _namespace_id, workflow_id, run_id) = update_test_service().await?;
        let error = service
            .update_workflow_execution(
                &HeaderMap::new(),
                update_request(
                    &workflow_id,
                    Some(run_id),
                    UpdateWaitPolicyDto::Admitted,
                    "update-1",
                ),
            )
            .await
            .expect_err("update path must reject ADMITTED wait policy");

        assert!(matches!(error, EdgeError::BadRequest(_)));
        assert_eq!(error.status_code(), http::StatusCode::BAD_REQUEST);
        Ok(())
    }

    #[tokio::test]
    async fn poll_path_accepts_admitted_and_returns_current_stage() -> Result<()> {
        let (service, runtime, namespace_id, workflow_id, run_id) = update_test_service().await?;
        let snapshot = runtime
            .update_workflow(
                ExecutionRef {
                    namespace_id,
                    workflow_id: workflow_id.clone(),
                    run_id: Some(run_id),
                },
                "update-1".to_string(),
                "update-handler".to_string(),
                Payloads::default(),
                RequestContext {
                    request_id: RequestId("update-1".to_string()),
                    caller_identity: None,
                    received_at: OffsetDateTime::now_utc(),
                },
                Duration::milliseconds(20),
                UpdateWaitPolicy::Admitted,
            )
            .await?;
        assert_eq!(snapshot.stage, UpdateLifecycleStage::Admitted);

        let polled = service
            .poll_workflow_execution_update(
                &HeaderMap::new(),
                "default".to_string(),
                workflow_id.0.clone(),
                run_id.0.to_string(),
                "update-1".to_string(),
                UpdateWaitPolicy::Admitted,
            )
            .await?;

        assert_eq!(polled.stage, UpdateLifecycleStage::Admitted);
        assert_eq!(polled.workflow_execution.run_id, Some(run_id));
        Ok(())
    }

    #[tokio::test]
    async fn update_path_targets_exact_run_and_returns_stable_ref() -> Result<()> {
        let (service, _runtime, _namespace_id, workflow_id, run_id) = update_test_service().await?;
        let response = service
            .update_workflow_execution(
                &HeaderMap::new(),
                update_request(
                    &workflow_id,
                    Some(run_id),
                    UpdateWaitPolicyDto::Unspecified,
                    "update-1",
                ),
            )
            .await?;

        assert_eq!(response.update_ref.workflow_id, workflow_id.0);
        assert_eq!(response.update_ref.run_id, run_id.0.to_string());
        assert_eq!(response.update_ref.update_id, "update-1");
        assert_eq!(
            response.stage,
            crate::translate::UpdateLifecycleStageDto::Admitted
        );
        assert!(response.outcome.is_none());
        Ok(())
    }

    #[test]
    fn matrix_capability_overlay_preserves_unmapped_and_experimental_baseline() {
        let capabilities = system_capabilities_with_matrix_overlay(baseline_capabilities());

        assert!(capabilities.signal_and_query_header);
        assert!(capabilities.build_id_based_versioning);
        assert!(!capabilities.eager_workflow_start);
    }

    #[test]
    fn mapped_stubbed_capability_preserves_true_baseline() {
        let mut capabilities = baseline_capabilities();

        apply_matrix_capability_field(
            &mut capabilities,
            "signal_and_query_header",
            FeatureState::Stubbed,
        );

        assert!(capabilities.signal_and_query_header);
        assert!(capabilities.encoded_failure_attributes);
    }

    #[test]
    fn empty_worker_identity_is_not_propagated_to_runtime() {
        assert_eq!(worker_identity_from_request(String::new()), None);
        assert_eq!(
            worker_identity_from_request("worker-a".to_string()),
            Some(tokeira_types::WorkerIdentity("worker-a".to_string()))
        );
    }

    proptest! {
        #[test]
        fn property_collect_eager_activity_specs_respects_limit(
            commands in prop::collection::vec(arb_workflow_command(), 0..20),
            limit in 0usize..8usize,
        ) {
            let eager_commands: Vec<_> = commands
                .iter()
                .filter_map(|command| match command {
                    WorkflowCommand::ScheduleActivity {
                        activity_id,
                        task_queue,
                        deployment,
                        build_id,
                        request_eager_execution: true,
                        ..
                    } => Some((
                        activity_id.clone(),
                        task_queue.clone(),
                        deployment.clone(),
                        build_id.clone(),
                    )),
                    _ => None,
                })
                .collect();

            let specs = collect_eager_activity_specs(&commands, limit);
            prop_assert!(specs.len() <= limit);
            prop_assert_eq!(
                specs,
                eager_commands.into_iter().take(limit).collect::<Vec<_>>()
            );
        }

        #[test]
        fn property_update_activity_options_command_respects_update_mask(
            mask_bits in 1u8..32u8,
        ) {
            let mut update_mask = Vec::new();
            if mask_bits & 0b00001 != 0 {
                update_mask.push("task_queue".to_string());
            }
            if mask_bits & 0b00010 != 0 {
                update_mask.push("activity_options.schedule_to_close_timeout".to_string());
            }
            if mask_bits & 0b00100 != 0 {
                update_mask.push("schedule_to_start_timeout".to_string());
            }
            if mask_bits & 0b01000 != 0 {
                update_mask.push("start_to_close_timeout".to_string());
            }
            if mask_bits & 0b10000 != 0 {
                update_mask.push("heartbeat_timeout".to_string());
            }

            let req = UpdateActivityOptionsRequest {
                namespace: "default".to_string(),
                workflow_id: "workflow".to_string(),
                run_id: None,
                identity: "operator".to_string(),
                target: crate::translate::ActivityTarget::Id("activity-1".to_string()),
                activity_options: Some(ActivityOptions {
                    task_queue: Some("queue-b".to_string()),
                    schedule_to_close_timeout: Some(time::Duration::seconds(10)),
                    schedule_to_start_timeout: Some(time::Duration::seconds(20)),
                    start_to_close_timeout: Some(time::Duration::seconds(30)),
                    heartbeat_timeout: None,
                    retry_policy: None,
                }),
                update_mask: update_mask.clone(),
                restore_original: false,
                activity_type: None,
            };

            let command = build_update_activity_options_command(
                &test_edge_context(),
                "activity-1".to_string(),
                &req,
            )
            .expect("non-empty mask should select at least one field");

            prop_assert_eq!(
                matches!(command.task_queue, FieldChange::Set(TaskQueueName(ref name)) if name == "queue-b"),
                mask_bits & 0b00001 != 0
            );
            prop_assert_eq!(
                matches!(command.schedule_to_close_timeout, FieldChange::Set(Some(value)) if value == time::Duration::seconds(10)),
                mask_bits & 0b00010 != 0
            );
            prop_assert_eq!(
                matches!(command.schedule_to_start_timeout, FieldChange::Set(Some(value)) if value == time::Duration::seconds(20)),
                mask_bits & 0b00100 != 0
            );
            prop_assert_eq!(
                matches!(command.start_to_close_timeout, FieldChange::Set(Some(value)) if value == time::Duration::seconds(30)),
                mask_bits & 0b01000 != 0
            );
            prop_assert_eq!(
                matches!(command.heartbeat_timeout, FieldChange::Set(None)),
                mask_bits & 0b10000 != 0
            );
        }
    }
}
