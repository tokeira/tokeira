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

use std::{sync::Arc, time::Duration};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use http::HeaderMap;
use prost::Message as _;
use time::OffsetDateTime;
use tokeira_kernel::{
    CancelRequest, HistoryEvent, HistoryEventKind, ResetRequest, SignalRequest,
    SignalWithStartRequest, StartRequest, TerminateRequest, WorkflowTaskCompletedRequest,
};
use tokeira_runtime::{
    BufferedQueryRegistry, InMemoryBroker, PendingUpdateTransport, QueryResult,
    ResetWorkflowResult, SignalWithStartResult, StartWorkflowResult, StartedActivityTask,
    StartedWorkflowTask, UpdateOutcome, UpdateTransportResolution, UpdateWaitPolicy,
    VersioningRuleStore, WorkerRegistry,
};
use tokeira_storage::RunRepository;
use tokeira_types::{
    ActivityTaskToken, ExecutionRef, ExecutionStatus, Payload, Payloads, QueueKey,
    RequestContext, RunId, RunKey, TaskKind, TaskQueueName, WorkerIdentity,
};
use uuid::Uuid;

use crate::{
    errors::{EdgeError, EdgeResult},
    history_wait::HistoryWaitRegistry,
    interceptors::{Action, EdgeInterceptors},
    long_poll::LongPollGate,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowMutationOutcome {
    pub transition_seq: u64,
    pub last_event_id: i64,
    pub was_duplicate: bool,
    pub execution_status: ExecutionStatus,
    pub new_run_id: Option<RunId>,
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

#[async_trait]
pub trait VisibilityApi: Send + Sync + 'static {
    async fn list_workflows(
        &self,
        req: ListWorkflowExecutionsRequest,
    ) -> Result<ListWorkflowExecutionsResponse>;

    async fn count_workflows(
        &self,
        req: CountWorkflowExecutionsRequest,
    ) -> Result<CountWorkflowExecutionsResponse>;

    async fn delete_execution(&self, run_key: RunKey) -> Result<()>;
}

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

#[derive(Debug, Default)]
pub struct EmptyVisibilityApi;

#[async_trait]
impl VisibilityApi for EmptyVisibilityApi {
    async fn list_workflows(
        &self,
        _req: ListWorkflowExecutionsRequest,
    ) -> Result<ListWorkflowExecutionsResponse> {
        Ok(ListWorkflowExecutionsResponse {
            executions: Vec::new(),
            next_page_token: None,
        })
    }

    async fn count_workflows(
        &self,
        _req: CountWorkflowExecutionsRequest,
    ) -> Result<CountWorkflowExecutionsResponse> {
        Ok(CountWorkflowExecutionsResponse {
            total_count: 0,
            groups: Vec::new(),
        })
    }

    async fn delete_execution(&self, _run_key: RunKey) -> Result<()> {
        Ok(())
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
    long_polls: LongPollGate,
    router: Arc<dyn EdgeRouter>,
    history_waiters: HistoryWaitRegistry,
    versioning_rule_store: Arc<VersioningRuleStore>,
    worker_registry: WorkerRegistry,
}

impl std::fmt::Debug for WorkflowService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowService").finish_non_exhaustive()
    }
}

impl WorkflowService {
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
            long_polls,
            router,
            HistoryWaitRegistry::default(),
            Arc::new(VersioningRuleStore::default()),
            WorkerRegistry::default(),
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
            long_polls,
            router,
            HistoryWaitRegistry::default(),
            Arc::new(VersioningRuleStore::default()),
            WorkerRegistry::default(),
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
            long_polls,
            router,
            history_waiters,
            Arc::new(VersioningRuleStore::default()),
            WorkerRegistry::default(),
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
        long_polls: LongPollGate,
        router: Arc<dyn EdgeRouter>,
        history_waiters: HistoryWaitRegistry,
        versioning_rule_store: Arc<VersioningRuleStore>,
        worker_registry: WorkerRegistry,
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
            long_polls,
            router,
            history_waiters,
            versioning_rule_store,
            worker_registry,
        }
    }

    pub fn versioning_rule_store(&self) -> Arc<VersioningRuleStore> {
        self.versioning_rule_store.clone()
    }

    pub fn worker_registry(&self) -> WorkerRegistry {
        self.worker_registry.clone()
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
            long_polls,
            router,
            history_waiters,
            Arc::new(VersioningRuleStore::default()),
            WorkerRegistry::default(),
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
                self.notify_history_run_key(internal.run_key, state.last_event_id)
                    .await;
                Ok(from_internal::start_response(
                    &internal,
                    WorkflowMutationOutcome {
                        transition_seq: state.transition_seq.0,
                        last_event_id: state.last_event_id,
                        was_duplicate: false,
                        execution_status: state.status,
                        new_run_id: None,
                    },
                ))
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
    }

    pub async fn signal_workflow_execution(
        &self,
        headers: &HeaderMap,
        req: SignalWorkflowExecutionRequest,
    ) -> EdgeResult<SignalWorkflowExecutionResponse> {
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
            if let Some(sender) =
                self.pending_queries.take(&req.task_token, query_id).await
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

        // ProtocolMessage commands have been resolved from the
        // messages field by the translate layer. Walk the commands
        // to fill in update_name/input for Accepted bodies (from
        // the UpdateRegistry) and notify the registry for
        // Completed/Rejected bodies.
        for cmd in &mut req.commands {
            if let tokeira_kernel::WorkflowCommand::ProtocolMessage { body, .. } = cmd {
                match body {
                    tokeira_kernel::UpdateProtocolBody::Accepted {
                        update_id,
                        update_name,
                        input,
                    } => {
                        if let Ok(Some((name, inp))) = self
                            .runtime
                            .peek_update_info(task_token.run_key, update_id.clone())
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
            });
        }

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

        // After WFT completion, if the SDK requested an eager return and
        // there are buffered queries and the run is quiescent, build an
        // inline query-only WFT so the worker evaluates against the
        // just-committed state without an extra poll round-trip.
        let mut resp = from_internal::completed_response(outcome);

        if resp.execution_status.is_open() && self.buffered_queries.has_buffered(run_key)
        {
            let token: tokeira_types::WorkflowTaskToken =
                serde_json::from_slice(&saved_task_token).map_err(EdgeError::from)?;
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
    }

    pub async fn respond_query_task_completed(
        &self,
        headers: &HeaderMap,
        task_token: Vec<u8>,
        result: QueryResult,
    ) -> EdgeResult<()> {
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
    }

    pub async fn list_workflow_executions(
        &self,
        headers: &HeaderMap,
        req: ListWorkflowExecutionsRequest,
    ) -> EdgeResult<ListWorkflowExecutionsResponse> {
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
    }

    pub async fn count_workflow_executions(
        &self,
        headers: &HeaderMap,
        req: CountWorkflowExecutionsRequest,
    ) -> EdgeResult<CountWorkflowExecutionsResponse> {
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
    }

    pub async fn get_cluster_info(&self, headers: &HeaderMap) -> EdgeResult<ClusterInfo> {
        let _ctx = self
            .interceptors
            .begin(headers, None, Action::GetClusterInfo, false)
            .await?;

        self.operator_api
            .cluster_info()
            .await
            .map_err(EdgeError::from)
    }

    pub async fn get_system_info(&self, headers: &HeaderMap) -> EdgeResult<SystemInfo> {
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
    }

    pub async fn list_namespaces(
        &self,
        headers: &HeaderMap,
    ) -> EdgeResult<EdgeListNamespacesResponse> {
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
    }

    pub async fn describe_namespace(
        &self,
        headers: &HeaderMap,
        namespace_name: &str,
    ) -> EdgeResult<NamespaceDescription> {
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
    }

    pub async fn register_namespace(
        &self,
        headers: &HeaderMap,
        req: RegisterNamespaceRequest,
    ) -> EdgeResult<()> {
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
    }

    pub async fn describe_task_queue(
        &self,
        headers: &HeaderMap,
        req: DescribeTaskQueueRequest,
    ) -> EdgeResult<DescribeTaskQueueResponse> {
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
    }

    pub async fn delete_workflow_execution(
        &self,
        headers: &HeaderMap,
        req: DeleteWorkflowExecutionRequest,
    ) -> EdgeResult<()> {
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
    }

    pub async fn reset_workflow_execution(
        &self,
        headers: &HeaderMap,
        req: ResetWorkflowExecutionRequest,
    ) -> EdgeResult<ResetWorkflowExecutionResponse> {
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
    }

    pub async fn signal_with_start_workflow_execution(
        &self,
        headers: &HeaderMap,
        req: SignalWithStartWorkflowExecutionRequest,
    ) -> EdgeResult<SignalWithStartWorkflowExecutionResponse> {
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
    }

    // ── Activity endpoints ──

    pub async fn poll_activity_task_queue(
        &self,
        headers: &HeaderMap,
        req: PollActivityTaskQueueRequest,
    ) -> EdgeResult<Option<PollActivityTaskQueueResponse>> {
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
    }

    pub async fn respond_activity_task_completed(
        &self,
        headers: &HeaderMap,
        req: RespondActivityTaskCompletedRequest,
    ) -> EdgeResult<RespondActivityTaskCompletedResponse> {
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
    }

    pub async fn respond_activity_task_failed(
        &self,
        headers: &HeaderMap,
        req: RespondActivityTaskFailedRequest,
    ) -> EdgeResult<RespondActivityTaskFailedResponse> {
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
    }

    pub async fn record_activity_task_heartbeat(
        &self,
        headers: &HeaderMap,
        req: RecordActivityTaskHeartbeatRequest,
    ) -> EdgeResult<RecordActivityTaskHeartbeatResponse> {
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
    }

    // ── Advanced workflow endpoints ──

    pub async fn terminate_workflow_execution(
        &self,
        headers: &HeaderMap,
        req: TerminateWorkflowExecutionRequest,
    ) -> EdgeResult<TerminateWorkflowExecutionResponse> {
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
    }

    pub async fn request_cancel_workflow_execution(
        &self,
        headers: &HeaderMap,
        req: RequestCancelWorkflowExecutionRequest,
    ) -> EdgeResult<RequestCancelWorkflowExecutionResponse> {
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
            crate::translate::UpdateWaitPolicyDto::Accepted => UpdateWaitPolicy::Accepted,
            crate::translate::UpdateWaitPolicyDto::Completed => {
                UpdateWaitPolicy::Completed
            }
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
        let last_event_id = read_last_event_id(self.repo.as_ref(), run_key).await?;
        self.notify_history_run_key(run_key, last_event_id).await;

        Ok(from_internal::update_response(outcome))
    }

    pub async fn poll_workflow_execution_update(
        &self,
        headers: &HeaderMap,
        namespace: String,
        workflow_id: String,
        _run_id_str: String,
        update_id: String,
    ) -> EdgeResult<Option<(tokeira_runtime::UpdateOutcome, RunKey)>> {
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

        let run_key = self.resolve_run_key(&namespace, &workflow_id).await?;

        let timeout = Duration::from_secs(60);
        let deadline = tokio::time::Instant::now() + timeout;

        // Check history for a completed/rejected update event matching update_id.
        // If not found, wait for new history events and re-check until timeout.
        loop {
            let history = self
                .repo
                .read_history(run_key, 0, usize::MAX)
                .await
                .map_err(EdgeError::from)?;

            let current_last_event_id = history.last().map(|e| e.event_id).unwrap_or(0);

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

            // Wait for a history change or timeout.
            let wait_result = tokio::time::timeout(remaining, rx.changed()).await;
            if wait_result.is_err() {
                return Ok(None);
            }
        }
    }

    // ── History ──

    pub async fn get_workflow_execution_history(
        &self,
        headers: &HeaderMap,
        req: crate::translate::GetWorkflowExecutionHistoryRequest,
    ) -> EdgeResult<crate::translate::GetWorkflowExecutionHistoryResponse> {
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
                return Ok(crate::translate::GetWorkflowExecutionHistoryResponse {
                    history: filtered.into_iter().take(limit).collect(),
                    next_page_token: encode_history_page_token(current_last_event_id),
                });
            }

            // When waiting for new events with a close-event filter,
            // don't return early just because new non-close events
            // arrived. Keep blocking until a close event appears.
            // For non-filtered requests, advance the token so the
            // caller can paginate.
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
    }

    pub async fn get_workflow_execution_history_reverse(
        &self,
        headers: &HeaderMap,
        req: crate::translate::GetWorkflowExecutionHistoryReverseRequest,
    ) -> EdgeResult<crate::translate::GetWorkflowExecutionHistoryReverseResponse> {
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

    async fn notify_history_run_key(&self, run_key: RunKey, last_event_id: i64) {
        self.history_waiters.notify(run_key, last_event_id).await;
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
