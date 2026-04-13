use std::{net::SocketAddr, sync::Arc};

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use tonic_web::GrpcWebLayer;
use tracing::info;
use tracing_subscriber::EnvFilter;
use tokio_util::sync::CancellationToken;
use tower_http::cors::CorsLayer;

use tokeira_edge::{
    EdgeInterceptors, InMemoryNamespaceCache,
    HistoryNotifyingRepository, HistoryWaitRegistry,
    InMemoryOperatorApi, LocalOnlyRouter, LongPollConfig,
    LongPollGate, OperatorService, PollerRegistry, ResolvedNamespace,
    WorkflowExecutionDescription, WorkflowService, NamespaceCache,
    grpc::{
        operator_service::OperatorServiceGrpc,
        runtime_adapter::RuntimeAdapter,
        workflow_service::WorkflowServiceGrpc,
    },
    translate::to_internal::namespace_id_for,
    workflow_service::ExecutionResolver,
};
use tokeira_kernel::LoadedRun;
use tokeira_projection::{
    InMemoryVisibilityStore, ProjectionWorker, VisibilityQueryService, VisibilitySink,
};
use tokeira_runtime::{
    BacklogConfig, LaneConfig, TimerScannerConfig, TokeiraRuntime,
    WorkflowTimeoutScannerConfig,
};
use tokeira_storage::{InMemoryStore, RunRepository};
use tokeira_types::{ExecutionRef, NamespaceId, ProjectionCursor, WorkflowId};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let addr = grpc_addr_from_env()?;

    let store = InMemoryStore::default();
    let history_waits = HistoryWaitRegistry::default();
    let repo = Arc::new(HistoryNotifyingRepository::new(
        Arc::new(store.clone()),
        history_waits.clone(),
    ));
    let runtime = Arc::new(TokeiraRuntime::new(
        repo.clone(),
        4,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
    ));

    let default_namespace = ResolvedNamespace::active("default");
    let default_namespace_id = namespace_id_for("default");

    let namespaces = Arc::new(InMemoryNamespaceCache::new());
    namespaces.insert(default_namespace).await?;

    let interceptors = Arc::new(EdgeInterceptors::permissive(namespaces.clone()));
    let router = Arc::new(LocalOnlyRouter);
    let runtime_adapter = Arc::new(RuntimeAdapter::new(runtime));
    let resolver = Arc::new(StoreExecutionResolver::new(
        repo.clone(),
        default_namespace_id,
    ));
    let visibility_store = InMemoryVisibilityStore::default();
    let visibility = Arc::new(VisibilityQueryService::new(
        visibility_store.clone(),
    ));
    let long_polls = LongPollGate::new(LongPollConfig::default());
    let operator_api = Arc::new(InMemoryOperatorApi::new("tokeira-local"));
    for partition_id in 0..16 {
        let projection_worker = ProjectionWorker {
            log: store.clone(),
            sink: VisibilitySink::new(
                visibility_store.clone(),
                format!("visibility-{partition_id}"),
            ),
            batch_size: 256,
        };
        tokio::spawn(async move {
            let cancel = CancellationToken::new();
            if let Err(error) = projection_worker
                .run_from_cursor(
                    &format!("visibility-{partition_id}"),
                    cancel,
                    ProjectionCursor::beginning(partition_id, 1),
                )
                .await
            {
                tracing::warn!(?error, partition_id, "projection worker exited");
            }
        });
    }

    let workflow_service = WorkflowService::new_with_history_wait_registry(
        runtime_adapter,
        resolver,
        visibility,
        repo.clone(),
        operator_api.clone(),
        namespaces,
        interceptors.clone(),
        PollerRegistry::default(),
        long_polls,
        router,
        history_waits,
    );
    let operator_service = OperatorService::new(operator_api, interceptors);

    let workflow_grpc = WorkflowServiceGrpc::new(workflow_service);
    let operator_grpc = OperatorServiceGrpc::new(operator_service);

    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(tokeira_proto::public::FILE_DESCRIPTOR_SET)
        .build()
        .context("failed to build gRPC reflection service")?;

    info!("tokeirad gRPC server listening on {addr}");

    tonic::transport::Server::builder()
        .accept_http1(true)
        .layer(CorsLayer::permissive())
        .layer(GrpcWebLayer::new())
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
