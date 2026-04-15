//! Local `tokeirad` bootstrap.
//!
//! This binary wires the dev in-memory store, runtime, projection workers, and
//! edge services into one process. It is intentionally explicit so developers
//! can see which pieces are authoritative, which are transport-only, and where
//! background tasks such as projection and history notification are attached.

use std::{net::SocketAddr, sync::Arc};

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use tonic_web::GrpcWebLayer;
use tower_http::cors::CorsLayer;
use tracing::info;
use tracing_subscriber::EnvFilter;

use tokeira_edge::{
    EdgeInterceptors, HistoryNotifyingRepository, HistoryWaitRegistry,
    InMemoryNamespaceCache, InMemoryOperatorApi, LocalOnlyRouter, LongPollConfig,
    LongPollGate, NamespaceCache, OperatorService, PendingQueryStore, PollerRegistry,
    ResolvedNamespace, WorkflowExecutionDescription, WorkflowService,
    grpc::{
        operator_service::OperatorServiceGrpc, runtime_adapter::RuntimeAdapter,
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

    // Build the authoritative dev store first, then wrap it with the
    // history-notifying repository used by edge long-poll.
    let store = InMemoryStore::default();
    let history_waits = HistoryWaitRegistry::default();
    let repo = Arc::new(HistoryNotifyingRepository::new(
        Arc::new(store.clone()),
        history_waits.clone(),
    ));
    // The runtime owns execution orchestration, scanners, brokers, and all
    // run-local in-memory coordination such as buffered consistent queries.
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

    // Bootstrap edge-facing namespace/operator state.
    let namespaces = Arc::new(InMemoryNamespaceCache::new());
    namespaces.insert(default_namespace).await?;

    let interceptors = Arc::new(EdgeInterceptors::permissive(namespaces.clone()));
    let router = Arc::new(LocalOnlyRouter);
    let workflow_broker = runtime.broker();
    let buffered_queries = runtime.buffered_queries();
    let runtime_adapter = Arc::new(RuntimeAdapter::new(runtime));
    let resolver = Arc::new(StoreExecutionResolver::new(
        repo.clone(),
        default_namespace_id,
    ));
    let visibility_store = InMemoryVisibilityStore::default();
    let visibility = Arc::new(VisibilityQueryService::new(visibility_store.clone()));
    let long_polls = LongPollGate::new(LongPollConfig::default());
    let operator_api = Arc::new(InMemoryOperatorApi::new("tokeira-local"));
    // Visibility is populated from the projection log, not from ad hoc edge
    // mutation hooks. One worker per partition keeps checkpoint ownership and
    // replay boundaries explicit.
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

    // Assemble the transport services once shared runtime/read-side components
    // exist. The edge stays thin: it delegates workflow semantics back into the
    // runtime and uses visibility/operator helpers only for read APIs.
    let workflow_service =
        WorkflowService::new_with_buffered_queries_and_history_wait_registry(
            runtime_adapter,
            resolver,
            visibility,
            repo.clone(),
            operator_api.clone(),
            namespaces,
            interceptors.clone(),
            PollerRegistry::default(),
            PendingQueryStore::default(),
            buffered_queries,
            workflow_broker,
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
        // Try current open run first, then fall back to latest run (including closed)
        let result = self
            .repo
            .resolve_execution(&ExecutionRef {
                namespace_id: self.namespace_id,
                workflow_id: WorkflowId(workflow_id.to_string()),
                run_id: None,
            })
            .await?;
        if result.is_some() {
            return Ok(result);
        }
        self.repo
            .find_latest_run(self.namespace_id, &WorkflowId(workflow_id.to_string()))
            .await
    }

    async fn describe_execution(
        &self,
        _namespace: &str,
        workflow_id: &str,
    ) -> Result<Option<WorkflowExecutionDescription>> {
        // Try current open run first, then fall back to latest run (including closed)
        let run_key = match self
            .repo
            .resolve_execution(&ExecutionRef {
                namespace_id: self.namespace_id,
                workflow_id: WorkflowId(workflow_id.to_string()),
                run_id: None,
            })
            .await?
        {
            Some(rk) => rk,
            None => {
                // Workflow may be closed — find the latest run by scanning all runs
                match self
                    .repo
                    .find_latest_run(
                        self.namespace_id,
                        &WorkflowId(workflow_id.to_string()),
                    )
                    .await?
                {
                    Some(rk) => rk,
                    None => return Ok(None),
                }
            }
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
