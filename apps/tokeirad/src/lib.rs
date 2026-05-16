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

use std::{collections::HashMap, net::SocketAddr, sync::Arc};

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

use tokeira_config::{Cli, ConfigStorageKind, TokeiraConfig};
use tokeira_edge::{
    CacheBackedRouter, EdgeInterceptors, EdgeRoutingConfig, HistoryNotifyingRepository,
    HistoryWaitRegistry, InMemoryNamespaceCache, InMemoryOperatorApi, LocalOnlyRouter,
    LongPollConfig, LongPollGate, NamespaceCache, OperatorService, PendingQueryStore,
    PollerRegistry, ResolvedNamespace, RoutingCache, WorkflowExecutionDescription, WorkflowService,
    grpc::{
        operator_service::OperatorServiceGrpc, runtime_adapter::RuntimeAdapter,
        workflow_service::WorkflowServiceGrpc,
    },
    run_routing_subscription,
    translate::to_internal::namespace_id_for,
    workflow_service::ExecutionResolver,
};
use tokeira_kernel::LoadedRun;
use tokeira_projection::{
    DsqlVisibilityStore, InMemoryVisibilityStore, ProjectionSink, ProjectionWorker,
    VisibilityQueryService, VisibilitySink, VisibilityStore,
};
use tokeira_runtime::{
    ConnectionBudgetApplier, EndpointTarget, InMemoryTaskQueueConfigStore, MembershipConfig,
    NexusEndpointConfig, NexusEndpointRegistry, NoopNexusHttpClient, RuntimeConfig,
    ScheduleEngineConfig, ScheduleStore, TokeiraRuntime, VersioningRuleStore, run_schedule_engine,
};
use tokeira_storage::{
    InMemoryStore, LeaseOutcome, LeaseRepository, ProjectionLog, RunRepository,
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
        let (server_task, bound_addr, shutdown_tx, background_cancel, log_broadcast) =
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
) -> DsqlPoolConfig {
    let (rate_limiter_table, conn_lease_table) =
        dsql_coordination_table_names(config);
    DsqlPoolConfig {
        coordination: DsqlCoordinationConfig {
            rate_limiter_table,
            conn_lease_table,
            ddb_client,
        },
        shard_count: config.infrastructure.placement.shard_count,
        projection_partition_count: config.infrastructure.placement.partition_count,
        ..DsqlPoolConfig::default()
    }
}

async fn dsql_pool_config(config: &TokeiraConfig, auth: &DsqlAuthConfig) -> Result<DsqlPoolConfig> {
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
    let (effective_config, config_source) = TokeiraConfig::resolve(cli.config.as_deref())?;
    if cli.dump_config {
        println!("{}", effective_config.to_toml()?);
        return Ok(());
    }
    let observability = observability::ObservabilityConfig::from_tokeira_config(&effective_config)?;
    let metrics_handle = observability::install_metrics(&observability)?;
    let log_reload = observability::install_tracing(&observability)?;
    for warning in effective_config.emergency_warnings() {
        tracing::warn!("{warning}");
    }
    let effective_config = Arc::new(effective_config);
    let _observability_server = observability::spawn_observability_server(
        &observability,
        effective_config.clone(),
        metrics_handle,
        log_reload,
    );

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

    let (server_task, bound_addr, _shutdown_tx, _background_cancel, _log_broadcast) =
        build_and_serve(addr, effective_config).await?;
    info!("tokeirad gRPC server listening on {bound_addr}");
    server_task
        .await
        .context("tokeirad server task panicked")?
        .context("tokeirad server task returned an error")?;
    Ok(())
}

/// Build the full server stack and start serving on the given address.
///
/// Storage selection is driven only by `infrastructure.storage`: endpoint
/// presence alone is not treated as intent. This keeps failed DSQL writeback
/// and explicit in-memory deployments from being conflated.
///
/// Factored out so both the CLI entrypoint and the in-memory facade share one
/// bootstrap path. Deliberately long and sequential: each block mirrors the
/// startup dependency order.
async fn build_and_serve(
    addr: SocketAddr,
    effective_config: Arc<TokeiraConfig>,
) -> Result<(
    JoinHandle<Result<()>>,
    SocketAddr,
    oneshot::Sender<()>,
    CancellationToken,
    broadcast::Sender<LogEvent>,
)> {
    match effective_config.infrastructure.storage {
        ConfigStorageKind::InMemory => {
            let store = InMemoryStore::default();
            let visibility_store = InMemoryVisibilityStore::default();
            build_and_serve_with_storage(
                addr,
                effective_config,
                Arc::new(store.clone()),
                store,
                visibility_store.clone(),
                {
                    let visibility_store = visibility_store.clone();
                    move |sink_id| VisibilitySink::new(visibility_store.clone(), sink_id)
                },
                None,
            )
            .await
        }
        ConfigStorageKind::Dsql => {
            let auth = dsql_auth_config(&effective_config)?;
            let endpoint = auth.endpoint.clone();
            let pool_config = dsql_pool_config(&effective_config, &auth).await?;
            let dsql_store = DsqlStore::connect(auth, pool_config)
                .await
                .context("failed to connect DSQL storage backend")?;
            let (director, run_repository, projection_log, _migration_runner) =
                dsql_store.into_parts();
            let visibility_store = DsqlVisibilityStore::new(director);
            build_and_serve_with_storage(
                addr,
                effective_config,
                Arc::new(run_repository),
                projection_log,
                visibility_store.clone(),
                {
                    let visibility_store = visibility_store.clone();
                    move |_sink_id| visibility_store.clone()
                },
                Some(endpoint),
            )
            .await
        }
    }
}

async fn build_and_serve_with_storage<R, L, S, V, F>(
    addr: SocketAddr,
    effective_config: Arc<TokeiraConfig>,
    run_repository: Arc<R>,
    projection_log: L,
    visibility_query_store: V,
    projection_sink: F,
    dsql_endpoint: Option<String>,
) -> Result<(
    JoinHandle<Result<()>>,
    SocketAddr,
    oneshot::Sender<()>,
    CancellationToken,
    broadcast::Sender<LogEvent>,
)>
where
    R: LeaseRepository + RunRepository + 'static,
    L: ProjectionLog + Clone + 'static,
    S: ProjectionSink + VisibilityStore + 'static,
    V: VisibilityStore + 'static,
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
    let runtime = Arc::new(TokeiraRuntime::new_with_nexus_and_shards_and_endpoint(
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
    ));

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
    let visibility = Arc::new(VisibilityQueryService::new(visibility_query_store));
    let long_polls = LongPollGate::new(LongPollConfig::default());
    let operator_api = Arc::new(InMemoryOperatorApi::new("tokeira-local"));

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
            nexus_task_broker,
            long_polls,
            router,
            history_waits,
            versioning_rule_store,
            worker_registry,
            schedule_store,
            task_queue_config_store,
            Arc::new(tokeira_runtime::BatchOperationStore::default()),
        );
    let operator_service = OperatorService::new(operator_api, interceptors);

    let workflow_grpc = WorkflowServiceGrpc::new(workflow_service);
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

    let server_task = tokio::spawn(async move {
        Server::builder()
            .accept_http1(true)
            .layer(CorsLayer::permissive())
            .layer(GrpcWebLayer::new())
            .add_service(workflow_grpc.into_service())
            .add_service(operator_grpc.into_service())
            .add_service(reflection)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                let _ = shutdown_rx.await;
            })
            .await
            .with_context(|| format!("failed to serve gRPC transport on {bound_addr}"))?;
        Ok::<(), anyhow::Error>(())
    });

    Ok((
        server_task,
        bound_addr,
        shutdown_tx,
        background_cancel,
        log_broadcast,
    ))
}

/// Broadcast buffer for [`LogEvent`]s fanned out to test harnesses.
///
/// Dimensioned so a slow consumer does not drop observation events under
/// typical integration-test cadence (one RPC per worker tick at ~30s).
const LOG_BROADCAST_CAPACITY: usize = 256;

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
        version: env!("CARGO_PKG_VERSION").to_string(),
        build_id: env!("CARGO_PKG_VERSION").to_string(),
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
    ) -> Result<Option<WorkflowExecutionDescription>> {
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
            None => match self
                .repo
                .find_latest_run(self.namespace_id, &WorkflowId(workflow_id.to_string()))
                .await?
            {
                Some(rk) => rk,
                None => return Ok(None),
            },
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

        let pool_config =
            dsql_pool_config_with_client(&config, DsqlCoordinationConfig::default().ddb_client);

        assert_eq!(pool_config.shard_count, 32);
        assert_eq!(pool_config.projection_partition_count, 4);
    }
}
