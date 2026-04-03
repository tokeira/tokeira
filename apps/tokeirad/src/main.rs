use std::{net::SocketAddr, sync::Arc};

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use tracing::info;
use tracing_subscriber::EnvFilter;

use tokeira_edge::{
    EdgeInterceptors, EmptyVisibilityApi, InMemoryNamespaceCache, InMemoryOperatorApi,
    LocalOnlyRouter, LongPollConfig, LongPollGate, OperatorService, ResolvedNamespace,
    WorkflowExecutionDescription, WorkflowService,
    grpc::{
        operator_service::OperatorServiceGrpc, runtime_adapter::RuntimeAdapter,
        workflow_service::WorkflowServiceGrpc,
    },
    translate::to_internal::namespace_id_for,
    workflow_service::ExecutionResolver,
};
use tokeira_kernel::LoadedRun;
use tokeira_runtime::{
    LaneConfig, TimerScannerConfig, TokeiraRuntime, WorkflowTimeoutScannerConfig,
};
use tokeira_storage::{InMemoryStore, RunRepository};
use tokeira_types::{ExecutionRef, NamespaceId, WorkflowId};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let addr = grpc_addr_from_env()?;

    let store = Arc::new(InMemoryStore::default());
    let runtime = Arc::new(TokeiraRuntime::new(
        store.clone(),
        4,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
    ));

    let default_namespace = ResolvedNamespace::active("default");
    let default_namespace_id = namespace_id_for("default");

    let namespaces = Arc::new(InMemoryNamespaceCache::new());
    namespaces.insert(default_namespace).await;

    let interceptors = Arc::new(EdgeInterceptors::permissive(namespaces));
    let router = Arc::new(LocalOnlyRouter);
    let runtime_adapter = Arc::new(RuntimeAdapter::new(runtime));
    let resolver = Arc::new(StoreExecutionResolver::new(
        store.clone(),
        default_namespace_id,
    ));
    let visibility = Arc::new(EmptyVisibilityApi);
    let long_polls = LongPollGate::new(LongPollConfig::default());

    let workflow_service = WorkflowService::new(
        runtime_adapter,
        resolver,
        visibility,
        interceptors.clone(),
        long_polls,
        router,
    );
    let operator_service = OperatorService::new(
        Arc::new(InMemoryOperatorApi::new("tokeira-local")),
        interceptors,
    );

    let workflow_grpc = WorkflowServiceGrpc::new(workflow_service);
    let operator_grpc = OperatorServiceGrpc::new(operator_service);

    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(tokeira_proto::public::FILE_DESCRIPTOR_SET)
        .build()
        .context("failed to build gRPC reflection service")?;

    info!("tokeirad gRPC server listening on {addr}");

    tonic::transport::Server::builder()
        .add_service(workflow_grpc.into_service())
        .add_service(operator_grpc.into_service())
        .add_service(reflection)
        .serve(addr)
        .await
        .with_context(|| format!("failed to bind or serve gRPC transport on {addr}"))?;

    Ok(())
}

fn grpc_addr_from_env() -> Result<SocketAddr> {
    let raw =
        std::env::var("TOKEIRA_GRPC_ADDR").unwrap_or_else(|_| "[::1]:7233".to_string());
    raw.parse()
        .with_context(|| format!("invalid TOKEIRA_GRPC_ADDR value: {raw}"))
}

struct StoreExecutionResolver<R> {
    repo: Arc<R>,
    namespace_id: NamespaceId,
}

impl<R> StoreExecutionResolver<R> {
    fn new(repo: Arc<R>, namespace_id: NamespaceId) -> Self {
        Self { repo, namespace_id }
    }
}

#[async_trait]
impl<R> ExecutionResolver for StoreExecutionResolver<R>
where
    R: RunRepository + 'static,
{
    async fn current_run_key(
        &self,
        _namespace: &str,
        workflow_id: &str,
    ) -> Result<Option<tokeira_types::RunKey>> {
        self.repo
            .resolve_execution(&ExecutionRef {
                namespace_id: self.namespace_id,
                workflow_id: WorkflowId(workflow_id.to_string()),
                run_id: None,
            })
            .await
    }

    async fn describe_execution(
        &self,
        _namespace: &str,
        workflow_id: &str,
    ) -> Result<Option<WorkflowExecutionDescription>> {
        let Some(run_key) = self
            .repo
            .resolve_execution(&ExecutionRef {
                namespace_id: self.namespace_id,
                workflow_id: WorkflowId(workflow_id.to_string()),
                run_id: None,
            })
            .await?
        else {
            return Ok(None);
        };

        match self.repo.load_run(run_key).await? {
            LoadedRun::Existing(state) => Ok(Some(WorkflowExecutionDescription {
                namespace: "default".to_string(),
                workflow_id: state.workflow_id.0,
                run_key: state.run_key,
                run_id: state.run_id,
                workflow_type: state.workflow_type.0,
                task_queue: state.task_queue.0,
                status: state.status,
                start_time: Some(state.started_at),
                close_time: state.closed_at,
                history_length: state.last_event_id,
                state_transition_count: state.transition_seq.0 as i64,
                memo: state.memo,
                search_attributes: state.search_attributes,
            })),
            LoadedRun::Absent => {
                Err(anyhow!("resolved run missing from storage: {:?}", run_key))
            }
        }
    }
}
