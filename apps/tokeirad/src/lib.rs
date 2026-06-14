//! Library surface for `tokeirad`.
//!
//! `main.rs` is a thin CLI wrapper that delegates to [`run_from_cli`]. Tests and
//! embedded harnesses that need to spawn a `tokeirad` server without forking the
//! binary use [`TokeiradHandle::start_in_memory`]: it binds an ephemeral socket,
//! wires the same in-memory storage path the CLI uses when started without a
//! persistent backend, and returns a handle whose `shutdown` tears the server
//! down cleanly.
//!
//! The lifecycle facade owns the full bootstrap — store, runtime, projection
//! workers, edge services — so integration tests and dev harnesses see the same
//! topology the production binary does. The only difference is that the
//! listener is caller-provided: the facade accepts a `SocketAddr` rather than
//! reading it from `TokeiraConfig.infrastructure.network.grpc_addr`.

#![deny(rust_2018_idioms)]

use std::{
    collections::HashMap,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use clap::Parser;
use tokio::{
    net::TcpListener,
    sync::{broadcast, oneshot},
    task::JoinHandle,
};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tonic_web::GrpcWebLayer;
use tower_http::cors::CorsLayer;
use tracing::info;

pub mod correlation_format;
pub mod observability;

use tokeira_chasm::Library as _;
use tokeira_config::{Cli, ConfigStorageKind, TokeiraConfig};
use tokeira_edge::{
    CacheBackedRouter, EdgeInterceptors, EdgeRoutingConfig, HistoryNotifyingRepository,
    HistoryWaitRegistry, InMemoryNamespaceCache, InMemoryOperatorApi, LocalOnlyRouter,
    LongPollConfig, LongPollGate, NamespaceCache, OperatorService, PendingQueryStore,
    PollerRegistry, ResolvedNamespace, RoutingCache, WorkflowExecutionDescription, WorkflowService,
    conformance::{WireCoverageLayer, WireCoverageRecorder},
    grpc::{
        operator_service::OperatorServiceGrpc, runtime_adapter::RuntimeAdapter,
        workflow_service::WorkflowServiceGrpc,
    },
    operator_service::{ClusterInfo, OperatorApi, SearchAttributeDefinition},
    run_routing_subscription,
    translate::to_internal::namespace_id_for,
    workflow_service::ExecutionResolver,
};
use tokeira_kernel::LoadedRun;
use tokeira_projection::{
    DsqlVisibilityStore, InMemoryVisibilityStore, ProjectionSink, ProjectionWorker, SearchAttrType,
    VisibilityQueryService, VisibilitySink, VisibilityStore,
};
use tokeira_runtime::{
    ConnectionBudgetApplier, EndpointTarget, InMemoryTaskQueueConfigStore, MembershipConfig,
    NexusEndpointConfig, NexusEndpointRegistry, NoopNexusHttpClient, RuntimeConfig,
    ScheduleEngineConfig, ScheduleStore, TokeiraRuntime, VersioningRuleStore, run_schedule_engine,
};
use tokeira_storage::{
    InMemoryStore, LeaseOutcome, LeaseRepository, ProjectionLog, RunRepository,
    WorkerDeploymentRepository,
    dsql::{DsqlAuthConfig, DsqlCoordinationConfig, DsqlPoolConfig, DsqlStore},
};
use tokeira_types::{
    ExecutionRef, IncarnationId, NamespaceId, NodeEndpoint, PlacementConfig, ProjectionCursor,
    ShardId, WorkflowId,
};

/// Nexus-endpoint bootstrap target used by the dev startup path. Kept in the
/// library so both the CLI wrapper and the `start_in_memory` facade build the
/// same initial registry shape.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub enum BootstrapNexusEndpointTarget {
    External {
        address: String,
    },
    Worker {
        namespace_name: String,
        task_queue: String,
    },
}

#[derive(Debug)]
struct VisibilityRegistryOperatorApi<V> {
    inner: InMemoryOperatorApi,
    visibility_store: V,
}

impl<V> VisibilityRegistryOperatorApi<V> {
    fn new(inner: InMemoryOperatorApi, visibility_store: V) -> Self {
        Self {
            inner,
            visibility_store,
        }
    }
}

#[async_trait]
impl<V> OperatorApi for VisibilityRegistryOperatorApi<V>
where
    V: VisibilityStore + Clone + 'static,
{
    async fn cluster_info(&self) -> Result<ClusterInfo> {
        self.inner.cluster_info().await
    }

    async fn list_search_attributes(
        &self,
        namespace: Option<&str>,
    ) -> Result<Vec<SearchAttributeDefinition>> {
        self.inner.list_search_attributes(namespace).await
    }

    async fn upsert_search_attribute(
        &self,
        namespace: &str,
        attr: SearchAttributeDefinition,
    ) -> Result<()> {
        let attr_type = visibility_search_attr_type(&attr.attr_type)?;
        // OperatorService mutates the user-visible catalog, but projection is
        // the authority that makes a field queryable. Register first so a
        // successful AddSearchAttributes response cannot expose an attribute
        // that the visibility compiler still rejects.
        self.visibility_store
            .register_attr(namespace_id_for(namespace), attr.name.clone(), attr_type)
            .await?;
        self.inner.upsert_search_attribute(namespace, attr).await
    }

    async fn remove_search_attribute(&self, namespace: &str, attr_name: &str) -> Result<()> {
        self.inner
            .remove_search_attribute(namespace, attr_name)
            .await
    }
}

fn visibility_search_attr_type(value: &str) -> Result<SearchAttrType> {
    match value {
        "keyword" => Ok(SearchAttrType::Keyword),
        "keyword_list" => Ok(SearchAttrType::KeywordList),
        "int" => Ok(SearchAttrType::Int),
        "bool" => Ok(SearchAttrType::Bool),
        "double" => Ok(SearchAttrType::Double),
        "datetime" => Ok(SearchAttrType::Datetime),
        "text" => Ok(SearchAttrType::Text),
        other => Err(anyhow!("unsupported search attribute type `{other}`")),
    }
}

/// Companion to [`BootstrapNexusEndpointTarget`] carrying per-endpoint config
/// during bootstrap.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct BootstrapNexusEndpointConfig {
    pub target: BootstrapNexusEndpointTarget,
}

/// Handle returned by [`TokeiradHandle::start_in_memory`].
///
/// Drop-safe: the inner server task receives a shutdown signal when the handle
/// is dropped, after which the task completes on its own. For deterministic
/// teardown (tests that want to assert a clean exit) call [`Self::shutdown`]
/// explicitly.
///
/// The handle also exposes a [`log_sink`](Self::log_sink) that integration
/// tests use to observe SDK-facing RPC activity (specifically the per-call
/// `tracing::debug!` line that `record_worker_heartbeat` emits). The stream is
/// a broadcast receiver over structured tracing events captured inside the
/// facade; consumers not subscribed to it see no behaviour change.
pub struct TokeiradHandle {
    bound_addr: SocketAddr,
    shutdown_tx: Option<oneshot::Sender<()>>,
    server_task: Option<JoinHandle<Result<()>>>,
    /// Cancellation token for background workers spawned by the facade.
    ///
    /// Kept alongside the server task so that dropping the handle cancels the
    /// membership client, routing subscription, projection workers, and
    /// schedule engine in lockstep with the gRPC server shutdown.
    background_cancel: CancellationToken,
    log_broadcast: broadcast::Sender<LogEvent>,
}

/// One observable RPC event streamed through [`TokeiradHandle::log_sink`].
///
/// This is deliberately narrow: it captures only the fields integration tests
/// need to distinguish one RPC from another. The full tracing-subscriber
/// configuration still applies — this channel is a convenience fan-out, not a
/// replacement for the log pipeline.
#[derive(Clone, Debug)]
pub struct LogEvent {
    pub rpc: String,
    pub namespace: String,
    pub heartbeat_count: usize,
}

impl TokeiradHandle {
    /// Start an in-memory `tokeirad` bound to the caller-provided socket.
    ///
    /// `addr` may be `127.0.0.1:0` to request an ephemeral port; the bound
    /// port is then available via [`Self::bound_addr`]. All other server
    /// configuration uses `TokeiraConfig::default()`, which selects the
    /// in-memory storage path.
    pub async fn start_in_memory(addr: SocketAddr) -> Result<Self> {
        let effective_config = Arc::new(TokeiraConfig::default());
        let (server_task, bound_addr, shutdown_tx, background_cancel, log_broadcast, _recorder) =
            build_and_serve(addr, effective_config).await?;
        Ok(Self {
            bound_addr,
            shutdown_tx: Some(shutdown_tx),
            server_task: Some(server_task),
            background_cancel,
            log_broadcast,
        })
    }

    /// The socket address the server is bound to after startup.
    pub fn bound_addr(&self) -> SocketAddr {
        self.bound_addr
    }

    /// Subscribe to the in-process log-event broadcast. Each subscriber gets a
    /// fresh receiver; events emitted before subscription are not replayed.
    pub fn log_sink(&self) -> broadcast::Receiver<LogEvent> {
        self.log_broadcast.subscribe()
    }

    /// Signal the server to shut down and wait for the task to exit.
    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        self.background_cancel.cancel();
        if let Some(task) = self.server_task.take() {
            task.await
                .context("tokeirad server task panicked")?
                .context("tokeirad server task returned an error")?;
        }
        Ok(())
    }
}

impl Drop for TokeiradHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        self.background_cancel.cancel();
        // Server task is not awaited here; callers who need deterministic
        // teardown call `shutdown` explicitly. Tokio will reap the task.
    }
}

fn dsql_auth_config(config: &TokeiraConfig) -> Result<DsqlAuthConfig> {
    let dsql = &config.infrastructure.dsql;
    let endpoint = dsql
        .endpoint
        .clone()
        .filter(|endpoint| !endpoint.trim().is_empty())
        .ok_or_else(|| {
            anyhow!("infrastructure.dsql.endpoint must be set when infrastructure.storage is dsql")
        })?;
    Ok(DsqlAuthConfig {
        endpoint,
        region: dsql.region.clone(),
        admin_role_arn: dsql.admin_role_arn.clone(),
        runtime_role_arn: dsql.runtime_role_arn.clone(),
        readonly_role_arn: dsql.readonly_role_arn.clone(),
    })
}

fn dsql_pool_config_with_client(
    config: &TokeiraConfig,
    ddb_client: aws_sdk_dynamodb::Client,
) -> (DsqlPoolConfig, aws_sdk_dynamodb::Client) {
    let (rate_limiter_table, conn_lease_table) = dsql_coordination_table_names(config);
    let pool_config = DsqlPoolConfig {
        coordination: DsqlCoordinationConfig {
            rate_limiter_table,
            conn_lease_table,
        },
        shard_count: config.infrastructure.placement.shard_count,
        projection_partition_count: config.infrastructure.placement.partition_count,
        ..DsqlPoolConfig::default()
    };
    (pool_config, ddb_client)
}

async fn dsql_pool_config(
    config: &TokeiraConfig,
    auth: &DsqlAuthConfig,
) -> Result<(DsqlPoolConfig, aws_sdk_dynamodb::Client)> {
    let region = auth
        .resolved_region()
        .ok_or_else(|| anyhow!("dsql region must be configured or derivable from endpoint"))?;
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(region))
        .load()
        .await;
    Ok(dsql_pool_config_with_client(
        config,
        aws_sdk_dynamodb::Client::new(&sdk_config),
    ))
}

fn dsql_coordination_table_names(config: &TokeiraConfig) -> (String, String) {
    let dsql = &config.infrastructure.dsql;
    let rate_limiter = dsql
        .rate_limiter_table
        .clone()
        .unwrap_or_else(|| format!("{}-dsql-rate-limiter", config.infrastructure.cluster_name));
    let conn_lease = dsql
        .conn_lease_table
        .clone()
        .unwrap_or_else(|| format!("{}-dsql-conn-lease", config.infrastructure.cluster_name));
    (rate_limiter, conn_lease)
}

/// Entrypoint the CLI delegates to.
///
/// Parses `TokeiraConfig` from the CLI arguments, handles `--dump-config`, and
/// serves the gRPC stack on `infrastructure.network.grpc_addr` until
/// Ctrl-C.
pub async fn run_from_cli(cli: Cli) -> Result<()> {
    if cli.version {
        println!("{}", render_build_info(cli.verbose, cli.json));
        return Ok(());
    }

    let (effective_config, config_source) = TokeiraConfig::resolve(cli.config.as_deref())?;
    if cli.dump_config {
        println!("{}", effective_config.to_toml()?);
        return Ok(());
    }
    let observability = observability::ObservabilityConfig::from_tokeira_config(&effective_config)?;
    let effective_config = Arc::new(effective_config);
    let readiness = observability::TokeiradReadiness::new();
    let _observability_runtime = observability::install_process_observability(
        &observability,
        effective_config.clone(),
        readiness.registry.clone(),
    )
    .await?;
    log_build_info("tokeirad");
    for warning in effective_config.emergency_warnings() {
        tracing::warn!("{warning}");
    }

    let addr: SocketAddr = effective_config
        .infrastructure
        .network
        .grpc_addr
        .parse()
        .with_context(|| {
            format!(
                "invalid infrastructure.network.grpc_addr value: {}",
                effective_config.infrastructure.network.grpc_addr
            )
        })?;
    info!(config_source, "loaded tokeirad configuration");

    let (server_task, bound_addr, _shutdown_tx, _background_cancel, _log_broadcast, wire_recorder) =
        build_and_serve(addr, effective_config).await?;
    readiness.mark_started();
    info!("tokeirad gRPC server listening on {bound_addr}");
    let serve_result = server_task
        .await
        .context("tokeirad server task panicked")?
        .context("tokeirad server task returned an error");

    // Tier-2 conformance evidence export. When the wire-coverage recorder is present (the
    // conformance flag was set), snapshot the observed `(wire_method, status_code)` set
    // after the server has stopped serving and write it as pretty JSON for the Rust report
    // (see `.kiro/specs/temporal-functional-conformance`, task 9.x). This runs regardless
    // of how the server exited so a clean shutdown after a run still produces evidence; a
    // failed export is logged but MUST NOT mask the server's own exit status, so the
    // server result is returned after the export attempt. When the recorder is `None`
    // nothing is written and there is zero behavioural change.
    if let Some(recorder) = wire_recorder {
        let out_path = wire_coverage_out_path();
        if let Err(error) = export_wire_coverage(recorder.as_ref(), &out_path) {
            tracing::warn!(
                path = %out_path.display(),
                ?error,
                "failed to export conformance wire-coverage evidence"
            );
        }
    }

    serve_result?;
    Ok(())
}

/// Snapshot the recorder and write the wire-coverage record to `path` as pretty JSON.
///
/// Pretty (rather than compact) JSON is deliberate: the evidence file is read by humans
/// triaging conformance runs and is diffed across runs, so a stable, line-oriented layout
/// is worth the few extra bytes. The recorder's `snapshot()` already sorts rows
/// deterministically, so two runs observing the same calls produce byte-identical output.
///
/// Errors are surfaced with context rather than panicking: a failed export is a loss of
/// *evidence*, never a correctness fault, and the caller logs it without disturbing the
/// server's exit status.
fn export_wire_coverage(recorder: &WireCoverageRecorder, path: &Path) -> Result<()> {
    let record = recorder.snapshot();
    let row_count = record.rows.len();
    let json = serde_json::to_string_pretty(&record)
        .context("failed to serialize wire-coverage record to JSON")?;
    std::fs::write(path, json).with_context(|| {
        format!(
            "failed to write wire-coverage evidence to {}",
            path.display()
        )
    })?;
    info!(
        path = %path.display(),
        rows = row_count,
        "wrote conformance wire-coverage evidence"
    );
    Ok(())
}

fn log_build_info(process: &'static str) {
    let info = tokeira_build_info::summary();
    tracing::info!(
        process,
        tokeira_version = info.tokeira_version,
        tokeira_git_sha = info.tokeira_git_sha,
        temporal_proto_version = info.temporal_proto_version,
        temporal_server_compat = info.temporal_server_compat,
        rust_toolchain = info.rust_toolchain,
        source_tree_hash = info.source_tree_hash,
        feature_matrix_digest = info.feature_matrix_digest,
        sdk_matrix_digest = info.sdk_matrix_digest,
        build_mode = info.build_mode,
        "tokeira build provenance"
    );
}

fn render_build_info(verbose: bool, json: bool) -> String {
    let info = tokeira_build_info::summary();
    if json {
        return serde_json::json!({
            "tokeira_version": info.tokeira_version,
            "tokeira_git_sha": info.tokeira_git_sha,
            "temporal_proto_version": info.temporal_proto_version,
            "temporal_server_compat": info.temporal_server_compat,
            "rust_toolchain": info.rust_toolchain,
            "source_tree_hash": info.source_tree_hash,
            "feature_matrix_digest": info.feature_matrix_digest,
            "sdk_matrix_digest": info.sdk_matrix_digest,
            "build_mode": info.build_mode,
        })
        .to_string();
    }

    if verbose {
        return [
            format!("tokeira_version: {}", info.tokeira_version),
            format!("tokeira_git_sha: {}", info.tokeira_git_sha),
            format!("temporal_proto_version: {}", info.temporal_proto_version),
            format!("temporal_server_compat: {}", info.temporal_server_compat),
            format!("rust_toolchain: {}", info.rust_toolchain),
            format!("source_tree_hash: {}", info.source_tree_hash),
            format!("feature_matrix_digest: {}", info.feature_matrix_digest),
            format!("sdk_matrix_digest: {}", info.sdk_matrix_digest),
            format!("build_mode: {}", info.build_mode),
        ]
        .join("\n");
    }

    format!(
        "tokeira {}\ngit {}\nbuild {}",
        info.tokeira_version, info.tokeira_git_sha, info.build_mode
    )
}

/// What [`build_and_serve`] (and [`build_and_serve_with_storage`]) hand back to the
/// caller after the gRPC stack is spawned.
///
/// The final element — the optional `Arc<WireCoverageRecorder>` — is the Tier-2
/// conformance recorder handle. It is `Some` only when the conformance flag is set (see
/// [`wire_coverage_enabled`]); the caller snapshots it after the server task completes to
/// export wire-coverage evidence. It is carried as a tuple element rather than threaded
/// into [`TokeiradHandle`] because only the CLI entrypoint exports the evidence — the
/// in-memory test facade ignores it.
type ServerStack = (
    JoinHandle<Result<()>>,
    SocketAddr,
    oneshot::Sender<()>,
    CancellationToken,
    broadcast::Sender<LogEvent>,
    Option<Arc<WireCoverageRecorder>>,
);

/// Build the full server stack and start serving on the given address.
///
/// Storage selection is driven only by `infrastructure.storage`: endpoint
/// presence alone is not treated as intent. This keeps failed DSQL writeback
/// and explicit in-memory deployments from being conflated.
///
/// Factored out so both the CLI entrypoint and the in-memory facade share one
/// bootstrap path. Deliberately long and sequential: each block mirrors the
/// startup dependency order.
/// Default maximum identifier length for standalone-activity ids, mirroring
/// Temporal's `MaxIDLengthLimit` default of `1000` (`common/dynamicconfig` @
/// v1.31.0). Used to validate standalone-activity ids at the edge.
const DEFAULT_MAX_ID_LENGTH: usize = 1000;

async fn build_and_serve(
    addr: SocketAddr,
    effective_config: Arc<TokeiraConfig>,
) -> Result<ServerStack> {
    match effective_config.infrastructure.storage {
        ConfigStorageKind::InMemory => {
            let store = InMemoryStore::default();
            let visibility_store = InMemoryVisibilityStore::default();
            let worker_deployment_repository: Arc<dyn WorkerDeploymentRepository> =
                Arc::new(store.clone());
            build_and_serve_with_storage(
                addr,
                effective_config,
                Arc::new(store.clone()),
                worker_deployment_repository,
                store,
                visibility_store.clone(),
                {
                    let visibility_store = visibility_store.clone();
                    move |sink_id| VisibilitySink::new(visibility_store.clone(), sink_id)
                },
                None,
                Arc::new(tokeira_storage::InMemoryChasmNodeStore::new()),
            )
            .await
        }
        ConfigStorageKind::Dsql => {
            let auth = dsql_auth_config(&effective_config)?;
            let endpoint = auth.endpoint.clone();
            let (pool_config, ddb_client) = dsql_pool_config(&effective_config, &auth).await?;
            let dsql_store = DsqlStore::connect(auth, pool_config, ddb_client)
                .await
                .context("failed to connect DSQL storage backend")?;
            let (
                director,
                run_repository,
                projection_log,
                worker_deployment_repository,
                _migration_runner,
            ) = dsql_store.into_parts();
            // The CHASM node store shares the same connection director as the rest
            // of the DSQL backend, so standalone-activity node state is durable on
            // the same cluster.
            let chasm_node_repo = Arc::new(tokeira_storage::dsql::DsqlChasmNodeRepository::new(
                director.clone(),
            ));
            let visibility_store = DsqlVisibilityStore::new(director);
            let worker_deployment_repository: Arc<dyn WorkerDeploymentRepository> =
                Arc::new(worker_deployment_repository);
            build_and_serve_with_storage(
                addr,
                effective_config,
                Arc::new(run_repository),
                worker_deployment_repository,
                projection_log,
                visibility_store.clone(),
                {
                    let visibility_store = visibility_store.clone();
                    move |_sink_id| visibility_store.clone()
                },
                Some(endpoint),
                chasm_node_repo,
            )
            .await
        }
    }
}

async fn build_and_serve_with_storage<R, L, S, V, F>(
    addr: SocketAddr,
    effective_config: Arc<TokeiraConfig>,
    run_repository: Arc<R>,
    worker_deployment_repository: Arc<dyn WorkerDeploymentRepository>,
    projection_log: L,
    visibility_query_store: V,
    projection_sink: F,
    dsql_endpoint: Option<String>,
    chasm_node_repo: Arc<dyn tokeira_storage::ChasmNodeRepository>,
) -> Result<ServerStack>
where
    R: LeaseRepository + RunRepository + 'static,
    L: ProjectionLog + Clone + 'static,
    S: ProjectionSink + VisibilityStore + 'static,
    V: VisibilityStore + Clone + 'static,
    F: Fn(String) -> S + Clone + Send + Sync + 'static,
{
    // Build the authoritative store first, then wrap it with the
    // history-notifying repository used by edge long-poll.
    let node_id = IncarnationId::new();
    let node_endpoint = configured_node_endpoint(&effective_config, addr);
    let history_waits = HistoryWaitRegistry::default();
    let repo = Arc::new(HistoryNotifyingRepository::new(
        run_repository,
        history_waits.clone(),
    ));
    let default_namespace = ResolvedNamespace::active("default");
    let default_namespace_id = namespace_id_for("default");

    // Bootstrap edge-facing namespace/operator state.
    let namespaces = Arc::new(InMemoryNamespaceCache::new());
    namespaces.insert(default_namespace).await?;
    let nexus_registry = build_nexus_endpoint_registry(
        namespaces.as_ref(),
        HashMap::<String, BootstrapNexusEndpointConfig>::new(),
    )
    .await?;

    // The runtime owns execution orchestration, scanners, brokers, and all
    // run-local in-memory coordination such as buffered consistent queries.
    let versioning_rule_store = Arc::new(VersioningRuleStore::default());
    let schedule_store = Arc::new(ScheduleStore::default());
    let task_queue_config_store = Arc::new(InMemoryTaskQueueConfigStore::default());
    let mut runtime_config = RuntimeConfig::default();
    runtime_config.lane.controller_managed_placement = effective_config
        .infrastructure
        .placement
        .controller_endpoint
        .is_some();
    let seed_default_shard = dsql_endpoint.is_none()
        && effective_config
            .infrastructure
            .placement
            .controller_endpoint
            .is_none();
    let runtime = Arc::new(
        TokeiraRuntime::new_with_nexus_and_shards_and_endpoint(
            repo.clone(),
            runtime_config.lane_count,
            runtime_config.lane,
            runtime_config.timer_scanner,
            runtime_config.workflow_timeout_scanner,
            runtime_config.backlog,
            runtime_config.activity_timeout_scanner,
            runtime_config.nexus_timeout_scanner,
            nexus_registry,
            Arc::new(NoopNexusHttpClient),
            effective_config.infrastructure.placement.shard_count,
            node_id.to_string(),
            node_endpoint.as_authority(),
            seed_default_shard,
            versioning_rule_store.clone(),
        )
        // The edge always exposes Worker Deployment v2 RPCs. Wiring the
        // repository here keeps their registry durable for both in-memory and
        // DSQL backends instead of falling back to a detached test registry.
        .with_worker_deployment_repository(worker_deployment_repository),
    );

    if dsql_endpoint.is_some()
        && effective_config
            .infrastructure
            .placement
            .controller_endpoint
            .is_none()
    {
        self_assign_dsql_shards(
            runtime.as_ref(),
            repo.as_ref(),
            effective_config.infrastructure.placement.shard_count,
            &node_id,
            &node_endpoint,
        )
        .await?;
    }

    let background_cancel = CancellationToken::new();

    let _membership_client = effective_config
        .infrastructure
        .placement
        .controller_endpoint
        .clone()
        .map(|controller_endpoint| {
            runtime.spawn_membership_client(
                membership_config(
                    &effective_config,
                    node_id,
                    node_endpoint.clone(),
                    controller_endpoint,
                ),
                Arc::new(NoopConnectionBudgetApplier),
                background_cancel.clone(),
            )
        });
    let _schedule_engine = run_schedule_engine(
        schedule_store.clone(),
        runtime.clone(),
        versioning_rule_store.clone(),
        ScheduleEngineConfig::default(),
        background_cancel.clone(),
    );

    let interceptors = Arc::new(EdgeInterceptors::permissive(namespaces.clone()));
    let router: Arc<dyn tokeira_edge::EdgeRouter> = if let Some(controller_endpoint) =
        effective_config
            .infrastructure
            .placement
            .controller_endpoint
            .clone()
    {
        let routing_cache = Arc::new(RoutingCache::new(placement_config(&effective_config)));
        let routing_config = EdgeRoutingConfig {
            controller_endpoint,
            max_retries: effective_config
                .infrastructure
                .placement
                .routing_max_retries,
        };
        let subscription_cache = routing_cache.clone();
        let subscription_cancel = background_cancel.clone();
        let _routing_subscription = tokio::spawn(async move {
            if let Err(error) =
                run_routing_subscription(subscription_cache, routing_config, subscription_cancel)
                    .await
            {
                tracing::warn!(?error, "routing subscription exited");
            }
        });
        Arc::new(CacheBackedRouter::new(routing_cache, node_id))
    } else {
        Arc::new(LocalOnlyRouter)
    };
    let workflow_broker = runtime.broker();
    let buffered_queries = runtime.buffered_queries();
    let worker_registry = runtime.worker_registry();
    let nexus_task_broker = runtime.nexus_task_broker();
    let runtime_adapter = Arc::new(RuntimeAdapter::new(runtime.clone()));
    let resolver = Arc::new(StoreExecutionResolver::new(
        repo.clone(),
        default_namespace_id,
    ));
    tokeira_projection::seed_predefined_search_attributes(
        &visibility_query_store,
        default_namespace_id,
    )
    .await
    .context("failed to seed Temporal predefined search attributes")?;
    let operator_visibility_store = visibility_query_store.clone();
    let visibility = Arc::new(VisibilityQueryService::new(visibility_query_store));
    let long_polls = LongPollGate::new(LongPollConfig::default());
    let operator_api = Arc::new(VisibilityRegistryOperatorApi::new(
        InMemoryOperatorApi::new("tokeira-local"),
        operator_visibility_store,
    ));

    for partition_id in 0..effective_config.infrastructure.placement.partition_count {
        let projection_worker = ProjectionWorker {
            log: projection_log.clone(),
            sink: projection_sink(format!("visibility-{partition_id}")),
            batch_size: 256,
        };
        let projection_cancel = background_cancel.clone();
        tokio::spawn(async move {
            if let Err(error) = projection_worker
                .run_from_cursor(
                    &format!("visibility-{partition_id}"),
                    projection_cancel,
                    ProjectionCursor::beginning(partition_id, 1),
                )
                .await
            {
                tracing::warn!(?error, partition_id, "projection worker exited");
            }
        });
    }

    let workflow_service =
        WorkflowService::new_with_versioning_and_buffered_queries_and_history_wait_registry(
            runtime_adapter.clone(),
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
            nexus_task_broker,
            long_polls,
            router,
            history_waits,
            versioning_rule_store,
            worker_registry,
            runtime.heartbeat_store(),
            schedule_store,
            task_queue_config_store,
            Arc::new(tokeira_runtime::BatchOperationStore::default()),
        )
        .with_worker_deployment_runtime(runtime_adapter);
    let operator_service = OperatorService::new(operator_api, interceptors);

    // Wire the standalone-activity (CHASM) bridge onto the gRPC adapter: a CHASM
    // engine over the backend's node repository, an activity-library registry, and
    // a shared dispatch queue the engine routes committed dispatch tasks into and a
    // worker poll drains. The enable gate is operator config — off by default, so an
    // unconfigured server matches the `v1.31.0` baseline (RPCs answer
    // `UNIMPLEMENTED`); enabling it is a declared deviation (`AGENTS §8`).
    let workflow_grpc = {
        let mut registry_builder = tokeira_chasm::Registry::builder();
        tokeira_chasm_activity::ActivityLibrary::register(&mut registry_builder)
            .context("failed to register the activity CHASM library")?;
        let registry = Arc::new(registry_builder.build());
        let dispatch_queue = Arc::new(tokeira_edge::chasm_activity::ActivityDispatchQueue::new());
        let chasm_engine = Arc::new(tokeira_runtime::chasm::ChasmEngine::new(
            chasm_node_repo,
            registry,
            dispatch_queue.clone(),
            Arc::new(tokeira_runtime::chasm::NoopVisibilitySink),
        ));
        let activity_config = tokeira_chasm_activity::ActivityConfig {
            enable_standalone: effective_config
                .policy
                .compatibility
                .enable_standalone_activities,
            ..tokeira_chasm_activity::ActivityConfig::default()
        };
        let activity_bridge = Arc::new(
            tokeira_edge::chasm_activity::ActivityBridge::new(
                chasm_engine,
                activity_config,
                DEFAULT_MAX_ID_LENGTH,
            )
            .with_dispatch_queue(dispatch_queue),
        );
        WorkflowServiceGrpc::new(workflow_service).with_chasm_activity(activity_bridge)
    };
    let operator_grpc = OperatorServiceGrpc::new(operator_service);

    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(tokeira_proto::public::FILE_DESCRIPTOR_SET)
        .build()
        .context("failed to build gRPC reflection service")?;

    match dsql_endpoint {
        Some(endpoint) => info!(%endpoint, "storage backend: dsql"),
        None => info!("storage backend: in-memory"),
    }

    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind tokeirad gRPC listener on {addr}"))?;
    let bound_addr = listener
        .local_addr()
        .context("failed to resolve bound local address for tokeirad gRPC listener")?;

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let log_broadcast = broadcast::Sender::<LogEvent>::new(LOG_BROADCAST_CAPACITY);

    // Tier-2 functional conformance (see `.kiro/specs/temporal-functional-conformance`)
    // captures every served `(wire_method, status_code)` at the transport boundary. The
    // capturing tower layer is mounted ONLY when the conformance flag is set, so a normal
    // production server never installs it and pays zero per-call cost.
    //
    // The recorder is constructed here, BEFORE the server task is spawned, and an
    // `Arc::clone` is moved into the task for the layer while the original handle is
    // returned to the caller. This is what lets the caller snapshot the observed coverage
    // after the server has shut down (the layer's clone is dropped with the task, but the
    // returned `Arc` keeps the counts alive) and export it as JSON evidence — see
    // [`run_from_cli`]. When the flag is off the handle is `None`: nothing is mounted,
    // nothing is snapshotted, and there is zero behavioural change.
    let wire_coverage_recorder = wire_coverage_enabled().then(|| {
        info!("conformance wire-coverage recorder enabled");
        Arc::new(WireCoverageRecorder::new())
    });
    let server_recorder = wire_coverage_recorder.clone();

    let server_task = tokio::spawn(async move {
        let shutdown_signal = async move {
            let _ = shutdown_rx.await;
        };

        // The conformance layer changes the server's tower-stack type, so the flagged and
        // unflagged paths are distinct server builds rather than a conditionally-mutated
        // builder. They are otherwise identical; only the extra `.layer()` differs.
        match server_recorder {
            Some(recorder) => {
                Server::builder()
                    .accept_http1(true)
                    .layer(CorsLayer::permissive())
                    .layer(GrpcWebLayer::new())
                    .layer(WireCoverageLayer::new(recorder))
                    .add_service(workflow_grpc.into_service())
                    .add_service(operator_grpc.into_service())
                    .add_service(reflection)
                    .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown_signal)
                    .await
            }
            None => {
                Server::builder()
                    .accept_http1(true)
                    .layer(CorsLayer::permissive())
                    .layer(GrpcWebLayer::new())
                    .add_service(workflow_grpc.into_service())
                    .add_service(operator_grpc.into_service())
                    .add_service(reflection)
                    .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown_signal)
                    .await
            }
        }
        .with_context(|| format!("failed to serve gRPC transport on {bound_addr}"))?;
        Ok::<(), anyhow::Error>(())
    });

    Ok((
        server_task,
        bound_addr,
        shutdown_tx,
        background_cancel,
        log_broadcast,
        wire_coverage_recorder,
    ))
}

/// Broadcast buffer for [`LogEvent`]s fanned out to test harnesses.
///
/// Dimensioned so a slow consumer does not drop observation events under
/// typical integration-test cadence (one RPC per worker tick at ~30s).
const LOG_BROADCAST_CAPACITY: usize = 256;

/// Environment flag selecting the Tier-2 wire-coverage capture layer.
///
/// Set during a functional-conformance run (see
/// `.kiro/specs/temporal-functional-conformance`) so the gRPC server mounts the
/// `WireCoverageLayer`. Read as a single env var rather than threaded through
/// `TokeiraConfig` because this is a conformance-harness concern, not a server-config
/// surface, and must add nothing to the production config schema.
const WIRE_COVERAGE_ENV: &str = "TOKEIRA_CONFORMANCE_WIRE_COVERAGE";

/// Environment variable naming the file the wire-coverage evidence is written to.
///
/// Read only when the wire-coverage layer is active (see [`WIRE_COVERAGE_ENV`]). Kept a
/// separate var from the enable flag so an operator can leave the output location pinned
/// across runs while toggling capture on and off, and so the default path is never
/// silently inferred from the enable value. Like [`WIRE_COVERAGE_ENV`] it is read as a
/// single env var rather than threaded through `TokeiraConfig`, because the export path is
/// a conformance-harness concern, not a production server-config surface.
const WIRE_COVERAGE_OUT_ENV: &str = "TOKEIRA_CONFORMANCE_WIRE_COVERAGE_OUT";

/// Default wire-coverage evidence path used when capture is on but
/// [`WIRE_COVERAGE_OUT_ENV`] is unset.
///
/// A repo-relative `./wire-coverage.json` is chosen so an operator who enables capture
/// without naming a path still gets evidence in the working directory rather than nothing;
/// the report can then be pointed at the default. It is never used when capture is off.
const WIRE_COVERAGE_DEFAULT_OUT: &str = "./wire-coverage.json";

/// Whether the Tier-2 wire-coverage layer should be mounted.
///
/// True only when [`WIRE_COVERAGE_ENV`] is present and not one of the empty/false-y
/// values, so an accidentally-exported empty variable does not silently enable capture in
/// production. Any other value (e.g. `1`, `true`, `on`) enables it.
fn wire_coverage_enabled() -> bool {
    match std::env::var(WIRE_COVERAGE_ENV) {
        Ok(value) => {
            let trimmed = value.trim();
            !trimmed.is_empty()
                && !trimmed.eq_ignore_ascii_case("0")
                && !trimmed.eq_ignore_ascii_case("false")
        }
        Err(_) => false,
    }
}

/// Resolve the path the wire-coverage evidence is written to.
///
/// Returns the value of [`WIRE_COVERAGE_OUT_ENV`] when it is set to a non-empty value,
/// otherwise [`WIRE_COVERAGE_DEFAULT_OUT`]. An empty or whitespace-only override is
/// treated as unset rather than as a request to write to a blank path, mirroring the
/// false-y handling in [`wire_coverage_enabled`] so an accidentally-exported empty
/// variable falls back to the documented default instead of erroring.
fn wire_coverage_out_path() -> PathBuf {
    match std::env::var(WIRE_COVERAGE_OUT_ENV) {
        Ok(value) if !value.trim().is_empty() => PathBuf::from(value.trim()),
        _ => PathBuf::from(WIRE_COVERAGE_DEFAULT_OUT),
    }
}

async fn build_nexus_endpoint_registry(
    namespaces: &dyn NamespaceCache,
    configs: HashMap<String, BootstrapNexusEndpointConfig>,
) -> Result<NexusEndpointRegistry> {
    let mut resolved = HashMap::with_capacity(configs.len());
    for (endpoint_name, config) in configs {
        let target = match config.target {
            BootstrapNexusEndpointTarget::External { address } => {
                EndpointTarget::External { address }
            }
            BootstrapNexusEndpointTarget::Worker {
                namespace_name,
                task_queue,
            } => {
                let namespace = namespaces
                    .get(&namespace_name)
                    .await?
                    .ok_or_else(|| {
                        anyhow!(
                            "failed to register nexus worker endpoint `{endpoint_name}`: namespace `{namespace_name}` not found"
                        )
                    })?;
                EndpointTarget::Worker {
                    namespace_id: namespace_id_for(&namespace.name),
                    task_queue: tokeira_types::TaskQueueName(task_queue),
                }
            }
        };
        resolved.insert(endpoint_name, NexusEndpointConfig { target });
    }
    Ok(NexusEndpointRegistry::new(resolved))
}

async fn self_assign_dsql_shards<R>(
    runtime: &TokeiraRuntime<R>,
    lease_repository: &R,
    shard_count: u32,
    node_id: &IncarnationId,
    node_endpoint: &NodeEndpoint,
) -> Result<()>
where
    R: LeaseRepository + RunRepository + 'static,
{
    // In single-node compose mode (no controller), any existing leases are stale
    // by definition — there is no other node. Relinquish all existing leases first
    // so try_acquire_bundle succeeds regardless of prior owner or expiry.
    let existing_leases = lease_repository.list_bundle_leases().await?;
    for lease in &existing_leases {
        if let Some(owner) = &lease.owner_node_id {
            let _ = lease_repository
                .relinquish_bundle(lease.bundle_id, owner.clone(), lease.epoch)
                .await;
        }
    }

    let mut acquired = 0u32;
    for shard_index in 0..shard_count {
        let shard_id = ShardId(shard_index);
        match lease_repository
            .try_acquire_bundle(shard_id, node_id.to_string(), node_endpoint.as_authority())
            .await
        {
            Ok(LeaseOutcome::Acquired { epoch } | LeaseOutcome::Renewed { epoch }) => {
                runtime.record_self_assigned_shard(shard_id, epoch);
                acquired += 1;
            }
            Ok(LeaseOutcome::Rejected {
                current_owner,
                current_epoch,
            }) => {
                tracing::warn!(
                    shard_index,
                    %current_owner,
                    current_epoch = current_epoch.0,
                    "failed to self-assign shard: lease is held by another owner"
                );
            }
            Err(error) => {
                tracing::warn!(shard_index, ?error, "failed to self-assign shard");
            }
        }
    }
    info!(
        acquired,
        shard_count, "self-assigned DSQL shards (no controller)"
    );
    Ok(())
}

fn configured_node_endpoint(config: &TokeiraConfig, listen_addr: SocketAddr) -> NodeEndpoint {
    NodeEndpoint {
        host: config.infrastructure.placement.node_host.clone(),
        port: config
            .infrastructure
            .placement
            .node_port
            .unwrap_or_else(|| listen_addr.port()),
    }
}

fn placement_config(config: &TokeiraConfig) -> PlacementConfig {
    let placement = &config.infrastructure.placement;
    PlacementConfig {
        shard_count: placement.shard_count,
        bundle_count: placement.bundle_count,
        partition_count: placement.partition_count,
        hash_version: placement.hash_version,
    }
}

fn membership_config(
    config: &TokeiraConfig,
    node_id: IncarnationId,
    node_endpoint: NodeEndpoint,
    controller_endpoint: String,
) -> MembershipConfig {
    let placement = &config.infrastructure.placement;
    MembershipConfig {
        controller_endpoint,
        heartbeat_interval: std::time::Duration::from_millis(placement.heartbeat_interval_ms),
        reconnect_base_delay: std::time::Duration::from_millis(placement.reconnect_base_delay_ms),
        reconnect_max_delay: std::time::Duration::from_millis(placement.reconnect_max_delay_ms),
        node_id,
        node_endpoint,
        zone: None,
        version: tokeira_build_info::TOKEIRA_VERSION.to_string(),
        build_id: tokeira_build_info::TOKEIRA_GIT_SHA.to_string(),
    }
}

#[derive(Debug)]
struct NoopConnectionBudgetApplier;

impl ConnectionBudgetApplier for NoopConnectionBudgetApplier {
    fn apply_budget(
        &self,
        _rate_per_second: f64,
        _capacity: u64,
        _max_reservoir_size: u32,
    ) -> Result<()> {
        Ok(())
    }
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
        run_id: Option<tokeira_types::RunId>,
    ) -> Result<Option<WorkflowExecutionDescription>> {
        let run_key = match self
            .repo
            .resolve_execution(&ExecutionRef {
                namespace_id: self.namespace_id,
                workflow_id: WorkflowId(workflow_id.to_string()),
                run_id,
            })
            .await?
        {
            Some(rk) => rk,
            None if run_id.is_none() => {
                match self
                    .repo
                    .find_latest_run(self.namespace_id, &WorkflowId(workflow_id.to_string()))
                    .await?
                {
                    Some(rk) => rk,
                    None => return Ok(None),
                }
            }
            None => return Ok(None),
        };

        match self.repo.load_run(run_key).await? {
            LoadedRun::Existing(state) => Ok(Some(WorkflowExecutionDescription {
                namespace: "default".to_string(),
                workflow_id: state.workflow_id.0,
                run_key: state.run_key,
                run_id: state.run_id,
                workflow_type: state.workflow_type.0,
                task_queue: state.task_queue.0.clone(),
                status: state.status,
                start_time: Some(state.started_at),
                close_time: state.closed_at,
                execution_time: state.first_run_started_at.unwrap_or(state.started_at),
                execution_config: tokeira_edge::translate::ExecutionConfigDescription {
                    task_queue: state.task_queue.0.clone(),
                    workflow_execution_timeout: state.workflow_execution_timeout,
                    workflow_run_timeout: state.workflow_run_timeout,
                    default_workflow_task_timeout: state.workflow_task_timeout,
                    user_metadata: None,
                },
                history_length: state.last_event_id,
                state_transition_count: state.transition_seq.0 as i64,
                parent_namespace_id: state
                    .parent_namespace_id
                    .map(|namespace_id| namespace_id.0.to_string()),
                parent_workflow_id: state.parent_workflow_id.clone(),
                parent_run_id: state.parent_run_id,
                root_workflow_id: state.root_workflow_id.clone(),
                root_run_id: state.root_run_id,
                first_run_id: state.first_execution_run_id,
                memo: state.memo,
                search_attributes: state.search_attributes,
                pending_activities: state
                    .activities
                    .values()
                    .map(
                        |activity| tokeira_edge::translate::PendingActivityDescription {
                            activity_id: activity.activity_id.clone(),
                            activity_type: activity.activity_type.clone(),
                            is_started: activity.started_at.is_some(),
                            attempt: activity.attempt,
                            maximum_attempts: activity
                                .retry_policy
                                .as_ref()
                                .map(|policy| policy.maximum_attempts)
                                .unwrap_or_default(),
                            scheduled_at: activity.scheduled_at,
                            started_at: activity.started_at,
                            last_failure: activity.last_failure.clone(),
                            paused: activity.pause_info.is_some(),
                            pause_info: activity.pause_info.as_ref().map(|info| {
                                tokeira_edge::translate::PauseInfoDescription {
                                    identity: info.identity.clone(),
                                    paused_time: info.pause_time,
                                    reason: info.reason.clone(),
                                }
                            }),
                        },
                    )
                    .collect(),
                pending_children: state
                    .children
                    .values()
                    .map(|child| tokeira_edge::translate::PendingChildDescription {
                        workflow_id: child.child_workflow_id.0.clone(),
                        run_id: child
                            .child_run_id
                            .as_ref()
                            .map(|run_id| run_id.0.to_string()),
                        workflow_type: String::new(),
                        initiated_event_id: child.initiated_event_id,
                        parent_close_policy: child.parent_close_policy,
                    })
                    .collect(),
                pending_workflow_task: state.pending_workflow_task.as_ref().map(|task| {
                    tokeira_edge::translate::PendingWorkflowTaskDescription {
                        is_started: task.started_event_id.is_some(),
                        scheduled_at: task.scheduled_at,
                        started_at: task.started_at,
                        attempt: task.attempt,
                    }
                }),
                callbacks: state.completion_callbacks.clone(),
                pending_nexus_operations: state
                    .pending_nexus_operations
                    .values()
                    .map(
                        |operation| tokeira_edge::translate::PendingNexusOperationDescription {
                            endpoint: operation.endpoint.clone(),
                            service: operation.service.clone(),
                            operation: operation.operation.clone(),
                            scheduled_time: operation.scheduled_at,
                            scheduled_event_id: operation.scheduled_event_id,
                            schedule_to_close_timeout: operation.schedule_to_close_timeout,
                            started: operation.started,
                            operation_token: operation
                                .started
                                .then(|| operation.operation_id.clone()),
                        },
                    )
                    .collect(),
                pause_info: state.pause_info.as_ref().map(|info| {
                    tokeira_edge::translate::PauseInfoDescription {
                        identity: info.identity.clone(),
                        paused_time: info.pause_time,
                        reason: info.reason.clone(),
                    }
                }),
                execution_expiration_time: state.workflow_execution_timeout.map(|timeout| {
                    state.first_run_started_at.unwrap_or(state.started_at) + timeout
                }),
                run_expiration_time: state
                    .workflow_run_timeout
                    .map(|timeout| state.started_at + timeout),
                cancel_requested: state.cancel_requested,
                original_start_time: state.first_run_started_at.unwrap_or(state.started_at),
                versioning_info: state.versioning_info.clone(),
                worker_deployment_name: state.worker_deployment_name.clone(),
            })),
            LoadedRun::Absent => Err(anyhow!("resolved run missing from storage: {:?}", run_key)),
        }
    }
}

#[doc(hidden)]
pub fn __cli_parse() -> Cli {
    Cli::parse()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_renderer_is_deterministic() {
        let short = render_build_info(false, false);
        let verbose = render_build_info(true, false);
        let json = render_build_info(false, true);

        assert_eq!(short, render_build_info(false, false));
        assert_eq!(verbose, render_build_info(true, false));
        assert_eq!(json, render_build_info(false, true));
        assert!(json.contains("temporal_proto_version"));
    }

    #[tokio::test]
    async fn build_nexus_endpoint_registry_resolves_worker_namespace_names() {
        let cache = InMemoryNamespaceCache::new();
        let namespace = ResolvedNamespace::active("payments");
        let namespace_id = namespace_id_for("payments");
        cache.insert(namespace).await.expect("namespace insert");

        let registry = build_nexus_endpoint_registry(
            &cache,
            HashMap::from([(
                "payments-endpoint".to_string(),
                BootstrapNexusEndpointConfig {
                    target: BootstrapNexusEndpointTarget::Worker {
                        namespace_name: "payments".to_string(),
                        task_queue: "nexus-q".to_string(),
                    },
                },
            )]),
        )
        .await
        .expect("registry should build");

        let config = registry.resolve("payments-endpoint").expect("endpoint");
        assert_eq!(
            config,
            &NexusEndpointConfig {
                target: EndpointTarget::Worker {
                    namespace_id,
                    task_queue: tokeira_types::TaskQueueName("nexus-q".to_string()),
                },
            }
        );
    }

    #[tokio::test]
    async fn build_nexus_endpoint_registry_rejects_unknown_worker_namespace() {
        let cache = InMemoryNamespaceCache::new();

        let result = build_nexus_endpoint_registry(
            &cache,
            HashMap::from([(
                "payments-endpoint".to_string(),
                BootstrapNexusEndpointConfig {
                    target: BootstrapNexusEndpointTarget::Worker {
                        namespace_name: "missing".to_string(),
                        task_queue: "nexus-q".to_string(),
                    },
                },
            )]),
        )
        .await;

        assert!(result.is_err(), "missing namespace should fail");
        let error = result.err().expect("error");

        assert!(error.to_string().contains("namespace `missing` not found"));
    }

    #[test]
    fn placement_helpers_build_runtime_and_edge_config_from_server_config() {
        let mut config = TokeiraConfig::default();
        config.infrastructure.placement.controller_endpoint =
            Some("http://127.0.0.1:7240".to_string());
        config.infrastructure.placement.node_host = "10.0.0.9".to_string();
        config.infrastructure.placement.node_port = Some(8123);
        config.infrastructure.placement.shard_count = 8;
        config.infrastructure.placement.bundle_count = 4;
        config.infrastructure.placement.partition_count = 64;
        config.infrastructure.placement.hash_version = 2;
        let listen_addr = "127.0.0.1:7233".parse().unwrap();
        let node_id = IncarnationId::new();
        let endpoint = configured_node_endpoint(&config, listen_addr);
        let membership = membership_config(
            &config,
            node_id,
            endpoint.clone(),
            "http://127.0.0.1:7240".into(),
        );

        assert_eq!(endpoint.as_authority(), "10.0.0.9:8123");
        assert_eq!(membership.node_id, node_id);
        assert_eq!(membership.node_endpoint, endpoint);
        assert_eq!(
            placement_config(&config),
            PlacementConfig {
                shard_count: 8,
                bundle_count: 4,
                partition_count: 64,
                hash_version: 2,
            }
        );
    }

    #[test]
    fn dsql_pool_config_uses_effective_placement_values() {
        let mut config = TokeiraConfig::default();
        config.infrastructure.placement.shard_count = 32;
        config.infrastructure.placement.partition_count = 4;

        let (pool_config, _ddb_client) =
            dsql_pool_config_with_client(&config, tokeira_storage::dsql::offline_ddb_client());

        assert_eq!(pool_config.shard_count, 32);
        assert_eq!(pool_config.projection_partition_count, 4);
    }

    #[test]
    fn wire_coverage_out_path_prefers_env_override_then_falls_back_to_default() {
        // Exercised in one test because all cases mutate the same process-global env var;
        // splitting them risks cross-test interference under the parallel test runner.

        // Unset: the documented default path is used.
        unsafe { std::env::remove_var(WIRE_COVERAGE_OUT_ENV) };
        assert_eq!(
            wire_coverage_out_path(),
            PathBuf::from(WIRE_COVERAGE_DEFAULT_OUT)
        );

        // A real path is honoured verbatim (after trimming).
        unsafe { std::env::set_var(WIRE_COVERAGE_OUT_ENV, "  /tmp/cov.json  ") };
        assert_eq!(wire_coverage_out_path(), PathBuf::from("/tmp/cov.json"));

        // An empty/whitespace override is treated as unset, not as a blank path.
        unsafe { std::env::set_var(WIRE_COVERAGE_OUT_ENV, "   ") };
        assert_eq!(
            wire_coverage_out_path(),
            PathBuf::from(WIRE_COVERAGE_DEFAULT_OUT)
        );

        unsafe { std::env::remove_var(WIRE_COVERAGE_OUT_ENV) };
    }

    #[test]
    fn export_wire_coverage_writes_pretty_json_snapshot() {
        let recorder = WireCoverageRecorder::new();
        recorder.record(
            "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution",
            0,
        );

        let dir = std::env::temp_dir().join(format!("tokeirad-cov-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join("wire-coverage.json");

        export_wire_coverage(&recorder, &path).expect("export succeeds");

        let written = std::fs::read_to_string(&path).expect("evidence file readable");
        assert!(written.contains("StartWorkflowExecution"));
        // Pretty JSON is multi-line; a compact encoding would be a single line.
        assert!(written.contains('\n'));

        std::fs::remove_dir_all(&dir).ok();
    }
}
