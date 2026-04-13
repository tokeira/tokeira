use std::{sync::Arc, time::Duration};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use http::HeaderMap;
use time::OffsetDateTime;
use tokeira_kernel::{
    CancelRequest, HistoryEvent, HistoryEventKind,
    ResetRequest, SignalRequest, StartRequest, TerminateRequest,
    WorkflowTaskCompletedRequest,
};
use tokeira_runtime::{
    QueryResult, ResetWorkflowResult, StartedActivityTask, StartedWorkflowTask,
    UpdateOutcome, UpdateWaitPolicy,
};
use tokeira_storage::RunRepository;
use tokeira_types::{
    ActivityTaskToken, ExecutionRef, ExecutionStatus,
    Payloads, RequestContext, RunId, RunKey, TaskKind, TaskQueueName,
    WorkerIdentity,
};

use crate::{
    errors::{EdgeError, EdgeResult},
    history_wait::HistoryWaitRegistry,
    interceptors::{Action, EdgeInterceptors},
    long_poll::LongPollGate,
    namespace_cache::{NamespaceCache, ResolvedNamespace},
    operator_service::{ClusterInfo, OperatorApi},
    poller_registry::{ActivePoller, PollerRegistry},
    routing::{EdgeRouter, ensure_local},
    translate::{
        CountWorkflowExecutionsRequest,
        CountWorkflowExecutionsResponse,
        DeleteWorkflowExecutionRequest,
        DescribeTaskQueueRequest,
        DescribeTaskQueueResponse,
        DescribeWorkflowExecutionRequest,
        ListNamespacesResponse as EdgeListNamespacesResponse,
        ListWorkflowExecutionsRequest,
        ListWorkflowExecutionsResponse,
        NamespaceDescription,
        PollActivityTaskQueueRequest,
        PollActivityTaskQueueResponse,
        PollWorkflowTaskQueueRequest,
        PollWorkflowTaskQueueResponse,
        QueryWorkflowRequest, QueryWorkflowResponse,
        RecordActivityTaskHeartbeatRequest,
        RecordActivityTaskHeartbeatResponse,
        ResetWorkflowExecutionRequest,
        ResetWorkflowExecutionResponse,
        RegisterNamespaceRequest,
        RequestCancelWorkflowExecutionRequest,
        RequestCancelWorkflowExecutionResponse,
        RespondActivityTaskCompletedRequest,
        RespondActivityTaskCompletedResponse,
        RespondActivityTaskFailedRequest,
        RespondActivityTaskFailedResponse,
        RespondWorkflowTaskCompletedRequest,
        RespondWorkflowTaskCompletedResponse,
        SignalWorkflowExecutionRequest,
        SignalWorkflowExecutionResponse,
        SignalWithStartWorkflowExecutionRequest,
        SignalWithStartWorkflowExecutionResponse,
        StartWorkflowExecutionRequest,
        StartWorkflowExecutionResponse,
        SystemCapabilities,
        SystemInfo,
        TerminateWorkflowExecutionRequest,
        TerminateWorkflowExecutionResponse,
        UpdateWorkflowExecutionRequest,
        UpdateWorkflowExecutionResponse,
        WorkflowExecutionDescription, from_internal,
        to_internal,
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
    async fn start_workflow(&self, req: StartRequest) -> Result<WorkflowMutationOutcome>;

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
        failure_message: String,
        failure_error_type: Option<String>,
    ) -> Result<()>;

    async fn record_activity_heartbeat(
        &self,
        token: ActivityTaskToken,
    ) -> Result<bool>;

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
    long_polls: LongPollGate,
    router: Arc<dyn EdgeRouter>,
    history_waiters: HistoryWaitRegistry,
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
        long_polls: LongPollGate,
        router: Arc<dyn EdgeRouter>,
    ) -> Self {
        Self::new_with_history_wait_registry(
            runtime,
            resolver,
            visibility,
            repo,
            operator_api,
            namespaces,
            interceptors,
            poller_registry,
            long_polls,
            router,
            HistoryWaitRegistry::default(),
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
        long_polls: LongPollGate,
        router: Arc<dyn EdgeRouter>,
        history_waiters: HistoryWaitRegistry,
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
            long_polls,
            router,
            history_waiters,
        }
    }

    pub async fn start_workflow_execution(
        &self,
        headers: &HeaderMap,
        req: StartWorkflowExecutionRequest,
    ) -> EdgeResult<StartWorkflowExecutionResponse> {
        let ctx = self
            .interceptors
            .begin(
                headers,
                Some(&req.namespace),
                Action::StartWorkflowExecution,
                false,
            )
            .await?;

        ensure_local(
            self.router
                .route_workflow(&req.namespace, &req.workflow_id)
                .await?,
        )?;

        let internal = to_internal::start_request(req, &ctx.request_id);
        let outcome = self
            .runtime
            .start_workflow(internal.clone())
            .await
            .map_err(EdgeError::from)?;
        self.notify_history_run_key(
            internal.run_key,
            outcome.last_event_id,
        )
        .await;

        Ok(from_internal::start_response(&internal, outcome))
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
                internal.worker_identity,
                internal.timeout,
            )
            .await
            .map_err(EdgeError::from)?;

        match started {
            Some(started) => Ok(Some(
                from_internal::poll_response(
                    started,
                    self.repo.as_ref(),
                )
                .await
                .map_err(EdgeError::from)?,
            )),
            None => Ok(None),
        }
    }

    pub async fn respond_workflow_task_completed(
        &self,
        headers: &HeaderMap,
        req: RespondWorkflowTaskCompletedRequest,
    ) -> EdgeResult<RespondWorkflowTaskCompletedResponse> {
        let _ctx = self
            .interceptors
            .begin(headers, None, Action::RespondWorkflowTaskCompleted, false)
            .await?;

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

        Ok(from_internal::completed_response(outcome))
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

    pub async fn get_cluster_info(
        &self,
        headers: &HeaderMap,
    ) -> EdgeResult<ClusterInfo> {
        let _ctx = self
            .interceptors
            .begin(headers, None, Action::GetClusterInfo, false)
            .await?;

        self.operator_api
            .cluster_info()
            .await
            .map_err(EdgeError::from)
    }

    pub async fn get_system_info(
        &self,
        headers: &HeaderMap,
    ) -> EdgeResult<SystemInfo> {
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

        let mut namespaces = self
            .namespaces
            .list_all()
            .await
            .map_err(EdgeError::from)?;
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
            .ok_or_else(|| {
                EdgeError::NamespaceNotFound(namespace_name.to_string())
            })?;

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

        let loaded = self
            .repo
            .load_run(run_key)
            .await
            .map_err(EdgeError::from)?;
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
        validate_reset_target(
            &history,
            req.workflow_task_finish_event_id,
        )?;

        let execution = ExecutionRef {
            namespace_id: to_internal::namespace_id_for(&req.namespace),
            workflow_id: tokeira_types::WorkflowId(req.workflow_id.clone()),
            run_id: req
                .run_id
                .as_deref()
                .and_then(|value| uuid::Uuid::parse_str(value).ok())
                .map(RunId),
        };
        let internal =
            to_internal::reset_request(req, &ctx.request_id);
        let outcome = self
            .runtime
            .reset_workflow(execution, internal)
            .await
            .map_err(EdgeError::from)?;

        let last_event_id = read_last_event_id(
            self.repo.as_ref(),
            outcome.successor_run_key,
        )
        .await?;
        self.notify_history_run_key(
            outcome.successor_run_key,
            last_event_id,
        )
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

        if let Some(run_key) = self
            .resolver
            .current_run_key(&req.namespace, &req.workflow_id)
            .await
            .map_err(EdgeError::from)?
        {
            let internal = tokeira_kernel::SignalRequest {
                signal_name: req.signal_name,
                input: req.signal_input,
                request: RequestContext {
                    request_id: tokeira_types::RequestId(
                        ctx.request_id.as_str().to_string(),
                    ),
                    caller_identity: req.identity.clone(),
                    received_at: OffsetDateTime::now_utc(),
                },
                now: OffsetDateTime::now_utc(),
            };
            let outcome = self
                .runtime
                .signal_workflow(run_key, internal)
                .await
                .map_err(EdgeError::from)?;
            self.notify_history_run_key(run_key, outcome.last_event_id)
                .await;

            let loaded = self
                .repo
                .load_run(run_key)
                .await
                .map_err(EdgeError::from)?;
            let tokeira_kernel::LoadedRun::Existing(state) = loaded else {
                return Err(EdgeError::WorkflowNotFound {
                    namespace: req.namespace,
                    workflow_id: req.workflow_id,
                });
            };

            return Ok(SignalWithStartWorkflowExecutionResponse {
                run_id: state.run_id,
                started: false,
            });
        }

        let start_internal = StartRequest {
            run_key: RunKey::new(),
            namespace_id: to_internal::namespace_id_for(&req.namespace),
            workflow_id: tokeira_types::WorkflowId(req.workflow_id.clone()),
            run_id: RunId(uuid::Uuid::new_v4()),
            workflow_type: tokeira_types::WorkflowType(req.workflow_type),
            task_queue: TaskQueueName(req.task_queue),
            deployment: None,
            build_id: None,
            input: req.input,
            request: RequestContext {
                request_id: tokeira_types::RequestId(
                    req.request_id
                        .clone()
                        .unwrap_or_else(|| ctx.request_id.as_str().to_string()),
                ),
                caller_identity: req.identity.clone(),
                received_at: OffsetDateTime::now_utc(),
            },
            now: OffsetDateTime::now_utc(),
            workflow_execution_timeout: req.workflow_execution_timeout,
            workflow_run_timeout: req.workflow_run_timeout,
            workflow_task_timeout: req.workflow_task_timeout.unwrap_or(time::Duration::seconds(10)),
            retry_policy: req.retry_policy,
            attempt: 1,
            first_execution_run_id: None,
            continued_execution_run_id: None,
            memo: req.memo,
            search_attributes: req.search_attributes,
            parent_run_key: None,
            parent_workflow_id: None,
            first_run_started_at: None,
        };
        let started_run_key = start_internal.run_key;
        let started_run_id = start_internal.run_id;
        let start_outcome = self
            .runtime
            .start_workflow(start_internal)
            .await
            .map_err(EdgeError::from)?;
        self.notify_history_run_key(started_run_key, start_outcome.last_event_id)
            .await;

        let signal_outcome = self
            .runtime
            .signal_workflow(
                started_run_key,
                tokeira_kernel::SignalRequest {
                    signal_name: req.signal_name,
                    input: req.signal_input,
                    request: RequestContext {
                        request_id: tokeira_types::RequestId(format!(
                            "{}:signal",
                            ctx.request_id.as_str()
                        )),
                        caller_identity: req.identity,
                        received_at: OffsetDateTime::now_utc(),
                    },
                    now: OffsetDateTime::now_utc(),
                },
            )
            .await
            .map_err(EdgeError::from)?;
        self.notify_history_run_key(started_run_key, signal_outcome.last_event_id)
            .await;

        Ok(SignalWithStartWorkflowExecutionResponse {
            run_id: started_run_id,
            started: true,
        })
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
                .route_task_queue(
                    &req.namespace,
                    &req.task_queue,
                )
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
            .begin(
                headers,
                None,
                Action::RespondActivityTaskCompleted,
                false,
            )
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
            .begin(
                headers,
                None,
                Action::RespondActivityTaskFailed,
                false,
            )
            .await?;

        let token = req.token;
        self.runtime
            .fail_activity_task(
                token.clone(),
                req.failure_message,
                req.failure_error_type,
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
            .begin(
                headers,
                None,
                Action::RecordActivityTaskHeartbeat,
                false,
            )
            .await?;

        let cancel_requested = self
            .runtime
            .record_activity_heartbeat(req.token)
            .await
            .map_err(EdgeError::from)?;

        Ok(RecordActivityTaskHeartbeatResponse {
            cancel_requested,
        })
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
                .route_workflow(
                    &req.namespace,
                    &req.workflow_id,
                )
                .await?,
        )?;

        let run_key = self
            .resolve_run_key(
                &req.namespace,
                &req.workflow_id,
            )
            .await?;

        let internal =
            to_internal::terminate_request(req, &ctx.request_id);
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
    ) -> EdgeResult<RequestCancelWorkflowExecutionResponse>
    {
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
                .route_workflow(
                    &req.namespace,
                    &req.workflow_id,
                )
                .await?,
        )?;

        let run_key = self
            .resolve_run_key(
                &req.namespace,
                &req.workflow_id,
            )
            .await?;

        let internal =
            to_internal::cancel_request(req, &ctx.request_id);
        let outcome = self
            .runtime
            .cancel_workflow(run_key, internal)
            .await
            .map_err(EdgeError::from)?;
        self.notify_history_run_key(run_key, outcome.last_event_id)
            .await;

        Ok(from_internal::cancel_response(outcome))
    }

    pub async fn query_workflow(
        &self,
        headers: &HeaderMap,
        req: QueryWorkflowRequest,
    ) -> EdgeResult<QueryWorkflowResponse> {
        let _ctx = self
            .interceptors
            .begin(
                headers,
                Some(&req.namespace),
                Action::QueryWorkflow,
                false,
            )
            .await?;

        ensure_local(
            self.router
                .route_workflow(
                    &req.namespace,
                    &req.workflow_id,
                )
                .await?,
        )?;

        let _run_key = self
            .resolve_run_key(
                &req.namespace,
                &req.workflow_id,
            )
            .await?;

        let workflow_id = req.workflow_id.clone();
        let execution = ExecutionRef {
            namespace_id: to_internal::namespace_id_for(
                &req.namespace,
            ),
            workflow_id: tokeira_types::WorkflowId(
                workflow_id,
            ),
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
    }

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
                .route_workflow(
                    &req.namespace,
                    &req.workflow_id,
                )
                .await?,
        )?;

        let _run_key = self
            .resolve_run_key(
                &req.namespace,
                &req.workflow_id,
            )
            .await?;

        let workflow_id = req.workflow_id.clone();
        let execution = ExecutionRef {
            namespace_id: to_internal::namespace_id_for(
                &req.namespace,
            ),
            workflow_id: tokeira_types::WorkflowId(
                workflow_id,
            ),
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
            .resolve_execution_run_key(
                &req.namespace,
                &req.workflow_id,
                None,
            )
            .await?;
        let last_event_id =
            read_last_event_id(self.repo.as_ref(), run_key).await?;
        self.notify_history_run_key(run_key, last_event_id)
            .await;

        Ok(from_internal::update_response(outcome))
    }

    // ── History ──

    pub async fn get_workflow_execution_history(
        &self,
        headers: &HeaderMap,
        req: crate::translate::GetWorkflowExecutionHistoryRequest,
    ) -> EdgeResult<
        crate::translate::GetWorkflowExecutionHistoryResponse,
    > {
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
                .route_workflow(
                    &req.namespace,
                    &req.workflow_id,
                )
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
            let filtered = filter_history_events(
                &history,
                req.history_event_filter_type,
            );

            if !filtered.is_empty() || !req.wait_new_event {
                return Ok(
                    crate::translate::GetWorkflowExecutionHistoryResponse {
                        history: filtered
                            .into_iter()
                            .take(limit)
                            .collect(),
                        next_page_token:
                            encode_history_page_token(current_last_event_id),
                    },
                );
            }

            if current_last_event_id > caller_last_event_id {
                return Ok(
                    crate::translate::GetWorkflowExecutionHistoryResponse {
                        history: Vec::new(),
                        next_page_token:
                            encode_history_page_token(current_last_event_id),
                    },
                );
            }

            let mut wait = self
                .history_waiters
                .receiver(run_key, current_last_event_id)
                .await;
            if tokio::time::timeout(
                Duration::from_secs(60),
                wait.changed(),
            )
            .await
            .is_err()
            {
                return Ok(
                    crate::translate::GetWorkflowExecutionHistoryResponse {
                        history: Vec::new(),
                        next_page_token:
                            encode_history_page_token(current_last_event_id),
                    },
                );
            }
        }
    }

    pub async fn get_workflow_execution_history_reverse(
        &self,
        headers: &HeaderMap,
        req: crate::translate::GetWorkflowExecutionHistoryReverseRequest,
    ) -> EdgeResult<
        crate::translate::GetWorkflowExecutionHistoryReverseResponse,
    > {
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
            .filter(|event| before_event_id.map(|value| event.event_id < value).unwrap_or(true))
            .collect();
        reversed.sort_by(|left, right| right.event_id.cmp(&left.event_id));

        let page: Vec<_> = reversed.into_iter().take(limit).collect();
        let next_page_token = page
            .last()
            .map(|event| encode_reverse_history_page_token(event.event_id))
            .unwrap_or_default();

        Ok(crate::translate::GetWorkflowExecutionHistoryReverseResponse {
            history: page,
            next_page_token,
        })
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
            workflow_id: tokeira_types::WorkflowId(
                workflow_id.to_string(),
            ),
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

    async fn notify_history_run_key(
        &self,
        run_key: RunKey,
        last_event_id: i64,
    ) {
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
            | HistoryEventKind::WorkflowExecutionCanceled { .. }
            | HistoryEventKind::WorkflowExecutionTerminated { .. }
            | HistoryEventKind::WorkflowExecutionContinuedAsNew { .. }
    )
}

fn validate_reset_target(
    history: &[HistoryEvent],
    fork_event_id: i64,
) -> EdgeResult<()> {
    let Some(event) =
        history.iter().find(|event| event.event_id == fork_event_id)
    else {
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

async fn read_last_event_id(
    repo: &dyn RunRepository,
    run_key: RunKey,
) -> Result<i64> {
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

fn active_poller_to_edge(
    poller: ActivePoller,
) -> crate::translate::PollerInfo {
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
