use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use http::HeaderMap;
use tokeira_kernel::{SignalRequest, StartRequest, WorkflowTaskCompletedRequest};
use tokeira_runtime::StartedWorkflowTask;
use tokeira_types::RunKey;

use crate::{
    errors::{EdgeError, EdgeResult},
    interceptors::{Action, EdgeInterceptors},
    long_poll::LongPollGate,
    routing::{ensure_local, EdgeRouter},
    translate::{
        self, from_internal, to_internal, CountWorkflowExecutionsRequest,
        CountWorkflowExecutionsResponse, DescribeWorkflowExecutionRequest,
        ListWorkflowExecutionsRequest, ListWorkflowExecutionsResponse,
        PollWorkflowTaskQueueRequest, PollWorkflowTaskQueueResponse,
        RespondWorkflowTaskCompletedRequest, RespondWorkflowTaskCompletedResponse,
        SignalWorkflowExecutionRequest, SignalWorkflowExecutionResponse,
        StartWorkflowExecutionRequest, StartWorkflowExecutionResponse,
        WorkflowExecutionDescription,
    },
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowMutationOutcome {
    pub transition_seq: u64,
    pub last_event_id: i64,
    pub was_duplicate: bool,
}

#[async_trait]
pub trait WorkflowRuntimeApi: Send + Sync + 'static {
    async fn start_workflow(&self, req: StartRequest) -> Result<WorkflowMutationOutcome>;

    async fn signal_workflow(&self, run_key: RunKey, req: SignalRequest) -> Result<WorkflowMutationOutcome>;

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
}

#[async_trait]
pub trait ExecutionResolver: Send + Sync + 'static {
    async fn current_run_key(&self, namespace: &str, workflow_id: &str) -> Result<Option<RunKey>>;

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
}

#[derive(Debug, Default)]
pub struct InMemoryExecutionResolver {
    current: tokio::sync::RwLock<std::collections::HashMap<(String, String), RunKey>>,
    descriptions:
        tokio::sync::RwLock<std::collections::HashMap<(String, String), WorkflowExecutionDescription>>,
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
            (description.namespace.clone(), description.workflow_id.clone()),
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
}

#[derive(Clone)]
pub struct WorkflowService {
    runtime: Arc<dyn WorkflowRuntimeApi>,
    resolver: Arc<dyn ExecutionResolver>,
    visibility: Arc<dyn VisibilityApi>,
    interceptors: Arc<EdgeInterceptors>,
    long_polls: LongPollGate,
    router: Arc<dyn EdgeRouter>,
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
        interceptors: Arc<EdgeInterceptors>,
        long_polls: LongPollGate,
        router: Arc<dyn EdgeRouter>,
    ) -> Self {
        Self {
            runtime,
            resolver,
            visibility,
            interceptors,
            long_polls,
            router,
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
        let internal = to_internal::poll_request(req);
        let started = self
            .runtime
            .poll_workflow_task(internal.queue, internal.worker_identity, internal.timeout)
            .await
            .map_err(EdgeError::from)?;

        match started {
            Some(started) => Ok(Some(from_internal::poll_response(started).map_err(EdgeError::from)?)),
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
            .begin(
                headers,
                None,
                Action::RespondWorkflowTaskCompleted,
                false,
            )
            .await?;

        let internal = to_internal::workflow_task_completed_request(req).map_err(EdgeError::from)?;
        let outcome = self
            .runtime
            .complete_workflow_task(internal)
            .await
            .map_err(EdgeError::from)?;

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
