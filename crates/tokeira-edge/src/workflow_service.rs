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
use tokeira_kernel::{
    CancelRequest, HistoryEvent, HistoryEventKind, NexusResolution, ResetRequest,
    SignalRequest, SignalWithStartRequest, StartRequest, TerminateRequest,
    WorkflowTaskCompletedRequest,
};
use tokeira_runtime::{
    BatchError, BatchOperationEntry, BatchOperationStore, BatchProgressCounters,
    BatchResetTarget, BufferedQueryRegistry, InMemoryBroker, NexusTaskBroker,
    NexusTaskToken, OverlapDecision, OverlapPolicy, PendingUpdateTransport, QueryResult,
    ResetWorkflowResult, ScheduleActionResult, SchedulePatch, ScheduleStore,
    SignalWithStartResult, StartWorkflowResult, StartedActivityTask, StartedWorkflowTask,
    UpdateOutcome, UpdateTransportResolution, UpdateWaitPolicy, VersioningRuleStore,
    WorkerRegistry, WorkflowExecution, WorkflowExecutionStatus, compute_matching_times,
    decide_overlap, schedule_workflow_id,
};
use tokeira_storage::RunRepository;
use tokeira_types::{
    ActivityTaskToken, ExecutionRef, ExecutionStatus, Payload, Payloads, QueueKey,
    RequestContext, RequestId, RunId, RunKey, TaskKind, TaskQueueName, WorkerIdentity,
    WorkflowId,
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
    operator_service::{ClusterInfo, OperatorApi},
    pending_queries::LEGACY_QUERY_ID,
    pending_queries::PendingQueryStore,
    poller_registry::{ActivePoller, PollerRegistry},
    routing::{EdgeRouter, ensure_local},
    translate::{
        CountWorkflowExecutionsRequest, CountWorkflowExecutionsResponse,
        DeleteWorkflowExecutionRequest, DescribeTaskQueueRequest,
        DescribeTaskQueueResponse, DescribeWorkflowExecutionRequest,
        ListNamespacesResponse as EdgeListNamespacesResponse,
        ListWorkflowExecutionsRequest, ListWorkflowExecutionsResponse,
        NamespaceDescription, PollActivityTaskQueueRequest,
        PollActivityTaskQueueResponse, PollWorkflowTaskQueueRequest,
        PollWorkflowTaskQueueResponse, ProtocolMessageDto, QueryResultDto,
        QueryWorkflowRequest, QueryWorkflowResponse, RecordActivityTaskHeartbeatRequest,
        RecordActivityTaskHeartbeatResponse, RegisterNamespaceRequest,
        RequestCancelWorkflowExecutionRequest, RequestCancelWorkflowExecutionResponse,
        ResetWorkflowExecutionRequest, ResetWorkflowExecutionResponse,
        RespondActivityTaskCompletedRequest, RespondActivityTaskCompletedResponse,
        RespondActivityTaskFailedRequest, RespondActivityTaskFailedResponse,
        RespondWorkflowTaskCompletedRequest, RespondWorkflowTaskCompletedResponse,
        SignalWithStartWorkflowExecutionRequest,
        SignalWithStartWorkflowExecutionResponse, SignalWorkflowExecutionRequest,
        SignalWorkflowExecutionResponse, StartWorkflowExecutionRequest,
        StartWorkflowExecutionResponse, SystemCapabilities, SystemInfo,
        TerminateWorkflowExecutionRequest, TerminateWorkflowExecutionResponse,
        UpdateWorkflowExecutionRequest, UpdateWorkflowExecutionResponse,
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

fn schedule_request_context(now: OffsetDateTime) -> RequestContext {
    RequestContext {
        request_id: RequestId(Uuid::new_v4().to_string()),
        caller_identity: Some("schedule-engine".to_string()),
        received_at: now,
    }
}

#[async_trait]
pub trait WorkflowRuntimeApi: Send + Sync + 'static {
    /// Start a workflow and return mutation metadata for callers that only
    /// care about the committed transition, not conflict-policy nuance.
    async fn start_workflow(&self, req: StartRequest) -> Result<WorkflowMutationOutcome>;

    /// Start a workflow while preserving richer conflict/reuse results needed
    /// by edge APIs such as `SignalWithStartWorkflowExecution`.
    async fn start_workflow_with_policy(
        &self,
        req: StartRequest,
    ) -> Result<StartWorkflowResult>;

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
    ) -> Result<WorkflowMutationOutcome>;

    async fn fail_activity_task(
        &self,
        token: ActivityTaskToken,
        failure: Payload,
        failure_error_type: Option<String>,
        is_non_retryable: bool,
    ) -> Result<()>;

    async fn record_activity_heartbeat(&self, token: ActivityTaskToken) -> Result<bool>;

    async fn terminate_workflow(
        &self,
        run_key: RunKey,
        req: TerminateRequest,
    ) -> Result<WorkflowMutationOutcome>;

    async fn cancel_workflow(
        &self,
        run_key: RunKey,
        req: CancelRequest,
    ) -> Result<WorkflowMutationOutcome>;

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
    ) -> Result<UpdateOutcome>;

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

#[async_trait]
pub trait ExecutionResolver: Send + Sync + 'static {
    async fn current_run_key(
        &self,
        namespace: &str,
        workflow_id: &str,
    ) -> Result<Option<RunKey>>;

    async fn describe_execution(
        &self,
        namespace: &str,
        workflow_id: &str,
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
        self.descriptions.write().await.insert(
            (
                description.namespace.clone(),
                description.workflow_id.clone(),
            ),
            description,
        );
    }
}

#[async_trait]
impl ExecutionResolver for InMemoryExecutionResolver {
    async fn current_run_key(
        &self,
        namespace: &str,
        workflow_id: &str,
    ) -> Result<Option<RunKey>> {
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
    ) -> Result<Option<WorkflowExecutionDescription>> {
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
    schedule_store: Arc<ScheduleStore>,
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
            Arc::new(ScheduleStore::default()),
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
            Arc::new(ScheduleStore::default()),
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
            Arc::new(ScheduleStore::default()),
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
        schedule_store: Arc<ScheduleStore>,
        batch_store: Arc<BatchOperationStore>,
    ) -> Self {
        Self {
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
            nexus_broker,
            long_polls,
            router,
            history_waiters,
            versioning_rule_store,
            worker_registry,
            schedule_store,
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

    pub fn versioning_rule_store(&self) -> Arc<VersioningRuleStore> {
        self.versioning_rule_store.clone()
    }

    pub fn worker_registry(&self) -> WorkerRegistry {
        self.worker_registry.clone()
    }

    pub fn schedule_store(&self) -> Arc<ScheduleStore> {
        self.schedule_store.clone()
    }

    pub fn batch_store(&self) -> Arc<BatchOperationStore> {
        self.batch_store.clone()
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
                        .route_task_queue(&req.namespace, &req.task_queue)
                        .await?,
                )?;

                let _permit = self.long_polls.acquire().await?;
                let (namespace_id, task_queue) = crate::translate::nexus::broker_queue(
                    &req.namespace,
                    &req.task_queue,
                );
                let task = self
                    .nexus_broker
                    .poll(namespace_id, task_queue, std::time::Duration::from_secs(60))
                    .await;

                match task {
                    Some(task) => {
                        Ok(Some(crate::translate::nexus::PollNexusTaskQueueResponse {
                            task_token: task.token.encode().map_err(EdgeError::from)?,
                            request: task.request,
                        }))
                    }
                    None => Ok(None),
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
                    return Err(EdgeError::BadRequest(
                        "task_token is required".to_string(),
                    ));
                }
                let token = NexusTaskToken::decode(&req.task_token)
                    .map_err(|error| EdgeError::BadRequest(error.to_string()))?;
                let response = req.response.ok_or_else(|| {
                    EdgeError::BadRequest("response is required".to_string())
                })?;
                let resolution = crate::translate::nexus::proto_response_to_resolution(
                    response,
                    &token.operation_id,
                )
                .map_err(|error| EdgeError::BadRequest(error.to_string()))?;

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
                    return Err(EdgeError::BadRequest(
                        "task_token is required".to_string(),
                    ));
                }
                let token = NexusTaskToken::decode(&req.task_token)
                    .map_err(|error| EdgeError::BadRequest(error.to_string()))?;
                let error = req.error.ok_or_else(|| {
                    EdgeError::BadRequest("error is required".to_string())
                })?;
                let resolution =
                    crate::translate::nexus::proto_handler_error_to_resolution(error)
                        .map_err(|error| EdgeError::BadRequest(error.to_string()))?;

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
                self.batch_store.create(entry).map_err(|err| {
                    batch_error_to_edge(err, &req.namespace, &req.job_id)
                })?;

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
                            && entry.info.buffered_actions.len() >= 1
                        {
                            entry.info.buffered_actions.pop_front();
                            entry.info.buffer_dropped += 1;
                        }
                        entry.info.buffered_actions.push_back(
                            tokeira_runtime::BufferedAction {
                                nominal_time,
                                overlap_policy_override: overlap_override,
                            },
                        );
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
        let run_key = RunKey::new();
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
            memo: entry.action.start_workflow.memo.clone(),
            search_attributes: entry.action.start_workflow.search_attributes.clone(),
            workflow_execution_timeout: entry
                .action
                .start_workflow
                .workflow_execution_timeout,
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
            attempt: 1,
            continued_execution_run_id: None,
            first_execution_run_id: Some(run_id),
            parent_run_key: None,
            parent_workflow_id: None,
            parent_run_id: None,
            parent_namespace_id: None,
            parent_initiated_event_id: 0,
            original_execution_run_id: Some(run_id),
            continued_failure: None,
            last_completion_result: None,
            first_run_started_at: None,
            request: schedule_request_context(actual_time),
            now: actual_time,
            cron_schedule: Some(schedule_id.0.clone()),
        };
        let outcome = self
            .runtime
            .start_workflow_with_policy(request)
            .await
            .map_err(EdgeError::from)?;
        let result = match outcome {
            StartWorkflowResult::Started { run_key, run_id } => ScheduleActionResult {
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
            Arc::new(ScheduleStore::default()),
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
                task_queue: state.task_queue.0.clone(),
                history: Vec::new(),
            },
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

                ensure_local(
                    self.router.route_workflow(&namespace, &workflow_id).await?,
                )?;

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
                    StartWorkflowResult::Started { .. } => {
                        let loaded = self
                            .repo
                            .load_run(internal.run_key)
                            .await
                            .map_err(EdgeError::from)?;
                        let tokeira_kernel::LoadedRun::Existing(state) = loaded else {
                            return Err(EdgeError::Internal(format!(
                                "started run {:?} not found after commit",
                                internal.run_key
                            )));
                        };
                        self.notify_history_run_key(
                            internal.run_key,
                            state.last_event_id,
                        )
                        .await;
                        let mut response = from_internal::start_response(
                            &internal,
                            WorkflowMutationOutcome {
                                transition_seq: state.transition_seq.0,
                                last_event_id: state.last_event_id,
                                was_duplicate: false,
                                execution_status: state.status,
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
                                .try_claim_workflow_task(
                                    eager_queue,
                                    internal.run_key,
                                    identity,
                                )
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
                    StartWorkflowResult::UsedExisting { run_id, .. }
                    | StartWorkflowResult::Rejected { run_id, .. } => {
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

                ensure_local(
                    self.router
                        .route_workflow(&req.namespace, &req.workflow_id)
                        .await?,
                )?;

                let Some(run_key) = self
                    .resolver
                    .current_run_key(&req.namespace, &req.workflow_id)
                    .await
                    .map_err(EdgeError::from)?
                else {
                    return Err(EdgeError::WorkflowNotFound {
                        namespace: req.namespace,
                        workflow_id: req.workflow_id,
                    });
                };

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
                        .route_task_queue(&req.namespace, &req.task_queue)
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
                let internal = to_internal::poll_request(req);
                let started = self
                    .runtime
                    .poll_workflow_task(
                        internal.queue,
                        internal.worker_identity.clone(),
                        internal.timeout,
                    )
                    .await
                    .map_err(EdgeError::from)?;

                match started {
                    Some(started) => {
                        let mut response =
                            from_internal::poll_response(started.clone(), self.repo.as_ref())
                                .await
                                .map_err(EdgeError::from)?;

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
                        meta: Some(
                            tokeira_proto::public::temporal::api::update::v1::Meta {
                                update_id: update.update_id.clone(),
                                identity: update.identity,
                            },
                        ),
                        input: Some(
                            tokeira_proto::public::temporal::api::update::v1::Input {
                                header: None,
                                name: update.update_name,
                                args: Some(
                                    tokeira_proto::conversions::common::payloads_from_domain(
                                        &update.input,
                                    ),
                                ),
                            },
                        ),
                    };
                    let body = prost_types::Any {
                        type_url: "type.googleapis.com/temporal.api.update.v1.Request"
                            .to_string(),
                        value: request.encode_to_vec(),
                    };
                    // The SDK requires sequencing_event_id to determine where
                    // in the history replay the update should be processed.
                    // Temporal sets this to workflowTaskStartedEventID - 1.
                    let sequencing_event_id = started.token.started_event_id - 1;
                    response.messages.push(ProtocolMessageDto {
                        id: format!("{}/request", update.update_id),
                        protocol_instance_id: update.update_id,
                        body: body.encode_to_vec(),
                        sequencing_event_id: Some(sequencing_event_id),
                    });
                        }

                        Ok(Some(response))
                    }
                    None => Ok(None),
                }
            },
        )
        .await
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
                        serde_json::from_slice(&req.task_token)
                            .map_err(EdgeError::from)?;
                    token.logical_seq.0 == 0
                };

                for (query_id, result) in &req.query_results {
                    if let Some(sender) =
                        self.pending_queries.take(&req.task_token, query_id).await
                    {
                        let _ = sender.send(match result {
                            QueryResultDto::Answered { result } => {
                                QueryResult::Completed {
                                    result: result.clone(),
                                }
                            }
                            QueryResultDto::Failed { error_message } => {
                                QueryResult::Failed {
                                    message: error_message.clone(),
                                }
                            }
                        });
                    }
                }

                let task_token: tokeira_types::WorkflowTaskToken =
                    serde_json::from_slice(&req.task_token).map_err(EdgeError::from)?;

                for cmd in &mut req.commands {
                    if let tokeira_kernel::WorkflowCommand::ProtocolMessage {
                        body, ..
                    } = cmd
                    {
                        match body {
                            tokeira_kernel::UpdateProtocolBody::Accepted {
                                update_id,
                                update_name,
                                input,
                            } => {
                                if let Ok(Some((name, inp))) = self
                                    .runtime
                                    .peek_update_info(
                                        task_token.run_key,
                                        update_id.clone(),
                                    )
                                    .await
                                {
                                    *update_name = name;
                                    *input = inp;
                                }
                            }
                            tokeira_kernel::UpdateProtocolBody::Completed {
                                update_id,
                                result,
                            } => {
                                let _ = self
                                    .runtime
                                    .resolve_update_transport(
                                        task_token.run_key,
                                        update_id.clone(),
                                        UpdateTransportResolution::Completed {
                                            result: result.clone(),
                                        },
                                    )
                                    .await;
                            }
                            tokeira_kernel::UpdateProtocolBody::Rejected {
                                update_id,
                                failure,
                            } => {
                                let _ = self
                                    .runtime
                                    .resolve_update_transport(
                                        task_token.run_key,
                                        update_id.clone(),
                                        UpdateTransportResolution::Rejected {
                                            failure: failure.clone(),
                                        },
                                    )
                                    .await;
                            }
                        }
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

                let internal = to_internal::workflow_task_completed_request(req)
                    .map_err(EdgeError::from)?;
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
                    let namespace_id = match self
                        .repo
                        .load_run(run_key)
                        .await
                        .map_err(EdgeError::from)?
                    {
                        tokeira_kernel::LoadedRun::Existing(state) => state.namespace_id,
                        tokeira_kernel::LoadedRun::Absent => {
                            return Err(EdgeError::Internal(format!(
                                "completed run {:?} not found after commit",
                                run_key
                            )));
                        }
                    };
                    for (activity_id, task_queue, deployment, build_id) in
                        eager_activity_specs
                    {
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
                    && self.buffered_queries.has_buffered(run_key)
                {
                    let token: tokeira_types::WorkflowTaskToken =
                        serde_json::from_slice(&saved_task_token)
                            .map_err(EdgeError::from)?;
                    let loaded = self
                        .repo
                        .load_run(token.run_key)
                        .await
                        .map_err(EdgeError::from)?;
                    if let tokeira_kernel::LoadedRun::Existing(state) = loaded {
                        let quiescent = state.pending_workflow_task.is_none();
                        if quiescent {
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
                    .describe_execution(&req.namespace, &req.workflow_id)
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
                capabilities: SystemCapabilities {
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
                },
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

            let mut namespaces =
                self.namespaces.list_all().await.map_err(EdgeError::from)?;
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
                    .ok_or_else(|| {
                        EdgeError::NamespaceNotFound(namespace_name.to_string())
                    })?;

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
                    .insert(ResolvedNamespace::active(req.namespace))
                    .await
                    .map_err(EdgeError::from)
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
                        .route_task_queue(&req.namespace, &req.task_queue)
                        .await?,
                )?;

                let queue = queue_key_for_poll(
                    &req.namespace,
                    &req.task_queue,
                    req.task_kind,
                    None,
                    None,
                );

                Ok(DescribeTaskQueueResponse {
                    pollers: self
                        .poller_registry
                        .pollers(&queue)
                        .into_iter()
                        .map(active_poller_to_edge)
                        .collect(),
                    backlog_count_hint: req.include_status.then_some(0),
                })
            },
        )
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

                let loaded =
                    self.repo.load_run(run_key).await.map_err(EdgeError::from)?;
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
                                reason: "deleted via delete_workflow_execution"
                                    .to_string(),
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
                    read_last_event_id(self.repo.as_ref(), outcome.successor_run_key)
                        .await?;
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
                        let last_event_id =
                            read_last_event_id(self.repo.as_ref(), run_key).await?;
                        self.notify_history_run_key(run_key, last_event_id).await;
                        Ok(SignalWithStartWorkflowExecutionResponse {
                            run_id,
                            started: true,
                        })
                    }
                    SignalWithStartResult::Signaled { run_key, run_id } => {
                        let last_event_id =
                            read_last_event_id(self.repo.as_ref(), run_key).await?;
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
                        .route_task_queue(&req.namespace, &req.task_queue)
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
                    .poll_activity_task(
                        internal.queue,
                        internal.worker_identity,
                        internal.timeout,
                    )
                    .await
                    .map_err(EdgeError::from)?;

                match started {
                    Some(started) => Ok(Some(
                        from_internal::poll_activity_response(started)
                            .map_err(EdgeError::from)?,
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
                    .complete_activity_task(token.clone(), req.result)
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
                    .record_activity_heartbeat(req.token)
                    .await
                    .map_err(EdgeError::from)?;

                Ok(RecordActivityTaskHeartbeatResponse { cancel_requested })
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
                    .query_workflow(
                        execution,
                        req.query_type,
                        req.query_args,
                        req.timeout,
                    )
                    .await
                    .map_err(EdgeError::from)?;

                Ok(from_internal::query_response(result))
            },
        )
        .await
    }

    /// Submit a workflow update and optionally wait for its outcome.
    ///
    /// The `wait_policy` controls how long the caller blocks: `Accepted`
    /// returns as soon as the update is accepted by the workflow (the
    /// validator ran), while `Completed` waits for the update handler to
    /// finish. These map from the proto `lifecycle_stage` enum (stage 3 =
    /// Completed, anything else = Accepted). The runtime manages the
    /// wait-for-completion channel internally; the edge just translates
    /// the policy and forwards the outcome.
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

                let _run_key = self
                    .resolve_run_key(&req.namespace, &req.workflow_id)
                    .await?;

                let workflow_id = req.workflow_id.clone();
                let execution = ExecutionRef {
                    namespace_id: to_internal::namespace_id_for(&req.namespace),
                    workflow_id: tokeira_types::WorkflowId(workflow_id),
                    run_id: None,
                };

                let wait_policy = match req.wait_policy {
                    crate::translate::UpdateWaitPolicyDto::Accepted => {
                        UpdateWaitPolicy::Accepted
                    }
                    crate::translate::UpdateWaitPolicyDto::Completed => {
                        UpdateWaitPolicy::Completed
                    }
                };

                let request = RequestContext {
                    request_id: tokeira_types::RequestId(
                        uuid::Uuid::new_v4().to_string(),
                    ),
                    caller_identity: None,
                    received_at: time::OffsetDateTime::now_utc(),
                };

                let outcome = self
                    .runtime
                    .update_workflow(
                        execution,
                        req.update_id,
                        req.update_name,
                        req.input,
                        request,
                        req.timeout,
                        wait_policy,
                    )
                    .await
                    .map_err(EdgeError::from)?;
                let run_key = self
                    .resolve_execution_run_key(&req.namespace, &req.workflow_id, None)
                    .await?;
                let last_event_id =
                    read_last_event_id(self.repo.as_ref(), run_key).await?;
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
        _run_id_str: String,
        update_id: String,
    ) -> EdgeResult<Option<(tokeira_runtime::UpdateOutcome, RunKey)>> {
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

                ensure_local(
                    self.router.route_workflow(&namespace, &workflow_id).await?,
                )?;

                let run_key = self.resolve_run_key(&namespace, &workflow_id).await?;

                let timeout = Duration::from_secs(60);
                let deadline = tokio::time::Instant::now() + timeout;

                loop {
                    let history = self
                        .repo
                        .read_history(run_key, 0, usize::MAX)
                        .await
                        .map_err(EdgeError::from)?;

                    let current_last_event_id =
                        history.last().map(|e| e.event_id).unwrap_or(0);

                    for event in &history {
                        match &event.kind {
                            HistoryEventKind::WorkflowExecutionUpdateCompleted {
                                update_id: uid,
                                result,
                            } if uid == &update_id => {
                                return Ok(Some((
                                    UpdateOutcome::Completed {
                                        accepted_event_id: 0,
                                        result: result.clone(),
                                    },
                                    run_key,
                                )));
                            }
                            HistoryEventKind::WorkflowExecutionUpdateRejected {
                                update_id: uid,
                                failure,
                            } if uid == &update_id => {
                                return Ok(Some((
                                    UpdateOutcome::Rejected {
                                        accepted_event_id: 0,
                                        failure: failure.clone(),
                                    },
                                    run_key,
                                )));
                            }
                            _ => {}
                        }
                    }

                    let remaining =
                        deadline.saturating_duration_since(tokio::time::Instant::now());
                    if remaining.is_zero() {
                        return Ok(None);
                    }

                    let mut rx = self
                        .history_waiters
                        .receiver(run_key, current_last_event_id)
                        .await;

                    let wait_result = tokio::time::timeout(remaining, rx.changed()).await;
                    if wait_result.is_err() {
                        return Ok(None);
                    }
                }
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
                let caller_last_event_id =
                    decode_history_page_token(&req.next_page_token)
                        .map_err(EdgeError::BadRequest)?;
                let limit = if req.maximum_page_size > 0 {
                    req.maximum_page_size
                } else {
                    usize::MAX
                };

                loop {
                    let history = self
                        .repo
                        .read_history(run_key, caller_last_event_id, usize::MAX)
                        .await
                        .map_err(EdgeError::from)?;
                    let current_last_event_id = history
                        .last()
                        .map(|event| event.event_id)
                        .unwrap_or(caller_last_event_id);
                    let filtered =
                        filter_history_events(&history, req.history_event_filter_type);

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
                        return Ok(
                            crate::translate::GetWorkflowExecutionHistoryResponse {
                                history: filtered.into_iter().take(limit).collect(),
                                next_page_token: encode_history_page_token(
                                    current_last_event_id,
                                ),
                            },
                        );
                    }

                    if req.history_event_filter_type != 2
                        && current_last_event_id > caller_last_event_id
                    {
                        return Ok(
                            crate::translate::GetWorkflowExecutionHistoryResponse {
                                history: Vec::new(),
                                next_page_token: encode_history_page_token(
                                    current_last_event_id,
                                ),
                            },
                        );
                    }

                    let mut wait = self
                        .history_waiters
                        .receiver(run_key, current_last_event_id)
                        .await;
                    if tokio::time::timeout(Duration::from_secs(60), wait.changed())
                        .await
                        .is_err()
                    {
                        return Ok(
                            crate::translate::GetWorkflowExecutionHistoryResponse {
                                history: Vec::new(),
                                next_page_token: encode_history_page_token(
                                    current_last_event_id,
                                ),
                            },
                        );
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

                let before_event_id =
                    decode_reverse_history_page_token(&req.next_page_token)
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
                reversed.sort_by(|left, right| right.event_id.cmp(&left.event_id));

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

    async fn resolve_execution_run_key(
        &self,
        namespace: &str,
        workflow_id: &str,
        run_id: Option<&str>,
    ) -> EdgeResult<RunKey> {
        let execution = ExecutionRef {
            namespace_id: to_internal::namespace_id_for(namespace),
            workflow_id: tokeira_types::WorkflowId(workflow_id.to_string()),
            run_id: run_id
                .and_then(|value| uuid::Uuid::parse_str(value).ok())
                .map(RunId),
        };
        self.repo
            .resolve_execution(&execution)
            .await
            .map_err(EdgeError::from)?
            .ok_or(EdgeError::WorkflowNotFound {
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
        EdgeError::Unauthorized(_) => "unauthenticated",
        EdgeError::Forbidden { .. } => "permission_denied",
        EdgeError::NamespaceNotFound(_)
        | EdgeError::WorkflowNotFound { .. }
        | EdgeError::BatchOperationNotFound { .. } => "not_found",
        EdgeError::WorkflowAlreadyStarted { .. }
        | EdgeError::BatchOperationAlreadyExists { .. }
        | EdgeError::NamespaceAlreadyExists(_) => "already_exists",
        EdgeError::NamespaceDeleted(_) => "failed_precondition",
        EdgeError::TooManyLongPolls => "resource_exhausted",
        EdgeError::LongPollAdmissionTimeout => "deadline_exceeded",
        EdgeError::RemoteRouteUnsupported { .. } => "unavailable",
        EdgeError::Internal(_) => "internal",
    }
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

fn decode_reverse_history_page_token(
    token: &[u8],
) -> std::result::Result<Option<i64>, String> {
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

fn filter_history_events(
    history: &[HistoryEvent],
    filter_type: i32,
) -> Vec<HistoryEvent> {
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
            | HistoryEventKind::WorkflowExecutionCanceled
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

async fn read_last_event_id(repo: &dyn RunRepository, run_key: RunKey) -> Result<i64> {
    Ok(repo
        .read_history(run_key, 0, usize::MAX)
        .await?
        .last()
        .map(|event| event.event_id)
        .unwrap_or(0))
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
    use super::collect_eager_activity_specs;
    use proptest::prelude::*;
    use tokeira_kernel::WorkflowCommand;
    use tokeira_types::{Payloads, TaskQueueName};

    fn arb_small_string() -> impl Strategy<Value = String> {
        prop::collection::vec(prop::char::range('a', 'z'), 1..8)
            .prop_map(|chars| chars.into_iter().collect())
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
            arb_small_string()
                .prop_map(|timer_id| WorkflowCommand::CancelTimer { timer_id }),
            Just(WorkflowCommand::CancelWorkflow),
        ]
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
    }
}
