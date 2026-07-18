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
// The bootstrap facade wires many subsystems and accepts them as parameters.
#![allow(clippy::too_many_arguments)]

use std::{
    collections::HashMap,
    convert::Infallible,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use clap::Parser;
use http_body_util::{BodyExt, Full};
use hyper::{
    Method, Request, Response, StatusCode,
    body::{Bytes, Incoming},
    service::service_fn,
};
use hyper_util::rt::TokioIo;
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

#[cfg(feature = "conformance")]
mod conformance_nexus_authorizer;
pub mod correlation_format;
mod nexus_http_transport;
pub mod observability;

#[cfg(feature = "conformance")]
use conformance_nexus_authorizer::ConformanceNexusHttpAuthorizer;
use nexus_http_transport::NexusHttpLayer;
use tokeira_auth::{
    Authorizer, ClaimMapper, DefaultAuthorizer, GrantRule, GrantRules, JwksKeyProvider,
    JwtAuthenticator, JwtIssuerProfile, MultiSourceClaimMapper, StsAuthenticator,
};
use tokeira_chasm::Library as _;
use tokeira_config::{Cli, ConfigStorageKind, TokeiraConfig};
use tokeira_edge::{
    Authenticator, CacheBackedRouter, CallbackResponse, EdgeInterceptors, EdgeRoutingConfig,
    HistoryNotifyingRepository, HistoryWaitRegistry, InMemoryNamespaceCache, InMemoryOperatorApi,
    LocalOnlyRouter, LongPollConfig, LongPollGate, NamespaceCache, OperatorService,
    PendingQueryStore, PollerRegistry, ResolvedNamespace, RoutingCache,
    WorkflowExecutionDescription, WorkflowService,
    conformance::{WireCoverageLayer, WireCoverageRecorder},
    grpc::{
        admin_service::AdminServiceGrpc, operator_service::OperatorServiceGrpc,
        runtime_adapter::RuntimeAdapter, workflow_service::WorkflowServiceGrpc,
    },
    handle_nexus_callback,
    operator_service::{ClusterInfo, OperatorApi, SearchAttributeDefinition},
    run_routing_subscription,
    translate::to_internal::namespace_id_for,
    workflow_service::{ExecutionResolver, WorkflowRuntimeApi},
};

struct AuthorizationStack {
    grpc: Arc<dyn Authenticator>,
    nexus: Arc<dyn tokeira_edge::nexus_http::NexusHttpAuthorizer>,
    principal_attribution: bool,
}

async fn build_authorization_stack(config: &TokeiraConfig) -> Result<AuthorizationStack> {
    let Some(authorization) = config
        .policy
        .authorization
        .as_ref()
        .filter(|authorization| authorization.has_identity_source())
    else {
        return Ok(AuthorizationStack {
            grpc: Arc::new(tokeira_edge::AllowAllAuthenticator),
            nexus: Arc::new(tokeira_edge::nexus_http::PermissiveNexusHttpAuthorizer),
            principal_attribution: false,
        });
    };

    let mut profiles = Vec::with_capacity(authorization.jwt.issuers.len());
    for issuer in &authorization.jwt.issuers {
        let grants = GrantRules::new(
            issuer
                .grants
                .iter()
                .map(|rule| {
                    GrantRule::new(
                        rule.match_sub.clone(),
                        rule.grant.iter().map(String::as_str),
                    )
                    .with_context(|| format!("invalid JWT grants for issuer {}", issuer.name))
                })
                .collect::<Result<Vec<_>>>()?,
        );
        let keys =
            JwksKeyProvider::start(issuer.jwks_uri.clone(), issuer.refresh_interval_duration())
                .await;
        profiles.push(JwtIssuerProfile::new(
            issuer.name.clone(),
            issuer.issuer.clone(),
            issuer.audience.clone(),
            issuer.permissions_claim.clone(),
            grants,
            keys,
        ));
    }
    let jwt = (!profiles.is_empty()).then(|| JwtAuthenticator::new(profiles));
    let sts = authorization
        .aws_iam
        .as_ref()
        .map(|aws_iam| {
            aws_iam
                .grants
                .iter()
                .map(|rule| {
                    GrantRule::new(
                        rule.match_arn.clone(),
                        rule.grant.iter().map(String::as_str),
                    )
                    .context("invalid AWS IAM grants")
                })
                .collect::<Result<Vec<_>>>()
                .map(GrantRules::new)
                .map(StsAuthenticator::new)
        })
        .transpose()?;
    let mapper: Arc<dyn ClaimMapper> = Arc::new(MultiSourceClaimMapper::new(jwt, sts));
    let authorizer: Arc<dyn Authorizer> = Arc::new(DefaultAuthorizer);
    Ok(AuthorizationStack {
        grpc: Arc::new(tokeira_edge::PolicyAuthenticator::new(
            mapper.clone(),
            authorizer.clone(),
            authorization.expose_authorizer_errors,
        )),
        nexus: Arc::new(tokeira_edge::nexus_http::PolicyNexusHttpAuthorizer::new(
            mapper,
            authorizer,
            authorization.expose_authorizer_errors,
        )),
        principal_attribution: authorization.principal_attribution,
    })
}
use tokeira_kernel::LoadedRun;
use tokeira_projection::{
    DsqlVisibilityStore, InMemoryVisibilityStore, ProjectionSink, ProjectionWorker, SearchAttrType,
    VisibilityQueryService, VisibilitySink, VisibilityStore,
};
use tokeira_runtime::{
    CompletionCallbackScannerConfig, ConnectionBudgetApplier, HttpNexusClient,
    HttpNexusCompletionClient, InMemoryNexusEndpointStore, InMemoryTaskQueueConfigStore,
    MembershipConfig, NEXUS_CALLBACK_PATH, NEXUS_OPERATION_STATE_HEADER, NexusCompletionDeps,
    NexusCompletionRuntimeConfig, NexusEndpointRegistry, NexusEndpointSpec,
    NexusEndpointSpecTarget, NexusEndpointStore, NexusNamespaceResolver, RuntimeConfig,
    ScheduleEngineConfig, ScheduleStore, TEMPORAL_CALLBACK_TOKEN_HEADER, TokeiraRuntime,
    WorkflowTaskReportedProblem, reported_problem_from_state, run_schedule_engine,
};
use tokeira_storage::{
    InMemoryStore, LeaseOutcome, LeaseRepository, ProjectionLog, RunRepository,
    WorkerDeploymentRepository,
    dsql::{DsqlAuthConfig, DsqlCoordinationConfig, DsqlPoolConfig, DsqlStore},
};
use tokeira_types::{
    ExecutionRef, IncarnationId, NamespaceId, NodeEndpoint, PlacementConfig, ProjectionCursor,
    SearchAttrValue, ShardId, WorkflowId,
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

    /// Register the predefined search-attribute set into the visibility store for
    /// this namespace (idempotent upserts), so visibility queries in a runtime-
    /// created namespace resolve predefined fields exactly as the bootstrapped
    /// `default` namespace does. Routed to the store, not `self.inner`, so the
    /// predefined set stays out of the user-visible catalog.
    async fn seed_predefined_search_attributes(&self, namespace: &str) -> Result<()> {
        tokeira_projection::seed_predefined_search_attributes(
            &self.visibility_store,
            namespace_id_for(namespace),
        )
        .await
    }
}

/// [`NexusNamespaceResolver`] backed by the edge namespace cache.
///
/// The runtime publisher tags the External-endpoint outbound Nexus metric with the
/// originator's namespace *name*, but holds only its [`NamespaceId`]. tokeira namespace ids
/// are a non-invertible function of the name, so resolution scans the (small) registered set
/// and matches `namespace_id_for(name)` — the inverse the runtime needs. Returns `None` when
/// no registered namespace hashes to the id, leaving the metric unrecorded rather than
/// mistagged.
struct CacheNexusNamespaceResolver {
    namespaces: Arc<dyn NamespaceCache>,
}

#[async_trait]
impl NexusNamespaceResolver for CacheNexusNamespaceResolver {
    async fn name_for_id(&self, namespace_id: NamespaceId) -> Option<String> {
        self.namespaces
            .list_all()
            .await
            .ok()?
            .into_iter()
            .map(|namespace| namespace.name)
            .find(|name| namespace_id_for(name) == namespace_id)
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
#[derive(Debug)]
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
        let mut config = TokeiraConfig::default();
        // Bind the inbound Nexus completion listener on an ephemeral loopback port so
        // parallel in-memory servers (tests, multi-node harnesses) never collide on the
        // fixed default port; the runtime resolves `temporal://system` to the bound port.
        config.policy.nexus_completion.http_addr = "127.0.0.1:0".to_string();
        config.policy.nexus_completion.system_callback_url = "http://127.0.0.1:0".to_string();
        let effective_config = Arc::new(config);
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
// `--version` / `--dump-config` are CLI outputs to stdout by contract; daemon
// logging elsewhere speaks `tracing` only.
#[allow(clippy::print_stdout)]
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
                    move || VisibilitySink::new(visibility_store.clone())
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
                    move || visibility_store.clone()
                },
                Some(endpoint),
                chasm_node_repo,
            )
            .await
        }
    }
}

/// How often the visibility repair scanner sweeps committed executions to repair any
/// projection lost by the best-effort post-commit write (Req 10.11).
const VISIBILITY_REPAIR_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

/// Spawn the background visibility repair scanner: it reconstructs each committed
/// execution's snapshot from authoritative node state and re-applies it iff-newer, so
/// a committed transition can never permanently lack a projection (Req 10.11). The
/// snapshot rebuild decodes the per-archetype node bytes (only the activity archetype
/// today); other archetypes are skipped. Runs immediately, then on an interval, until
/// `cancel` fires.
fn spawn_visibility_repair(
    nodes: Arc<dyn tokeira_storage::ChasmNodeRepository>,
    sink: Arc<dyn tokeira_projection::ProjectionSink>,
    activity_archetype: Option<u32>,
    partition_count: u32,
    cancel: CancellationToken,
) {
    let rebuild: tokeira_runtime::chasm::SnapshotRebuilder =
        Arc::new(move |archetype_id, bytes| {
            if Some(archetype_id) == activity_archetype {
                tokeira_chasm_activity::rebuild_visibility_snapshot(bytes)
            } else {
                None
            }
        });
    let scanner =
        tokeira_runtime::chasm::VisibilityRepairScanner::new(nodes, sink, rebuild, partition_count);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(VISIBILITY_REPAIR_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = interval.tick() => {
                    if let Err(error) = scanner.repair_once().await {
                        tracing::warn!(?error, "visibility repair pass failed");
                    }
                }
            }
        }
    });
}

/// The interval the CHASM timer sweeper ticks at. Activity timeouts (schedule-to-
/// start/close, start-to-close, heartbeat) fire within roughly this cadence of their
/// deadline; ~200ms is well below the smallest conformance timeout while keeping the
/// sweep cheap (a snapshot of the armed-timer map plus a fenced update per due timer).
const CHASM_TIMER_SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_millis(200);

/// Spawn the CHASM timer sweeper: it fires armed activity timeouts on a tick by
/// delegating to the [`TimeoutEvaluator`](tokeira_runtime::chasm::TimeoutEvaluator)
/// (the activity bridge), then re-arms each execution to its state-derived next
/// deadline. Runtime-only (clock + loop); all timeout/retry semantics are pure
/// (`tokeira-chasm-activity`) behind the evaluator, so the kernel-purity and
/// history-authority invariants hold. Runs until `cancel` fires. Gated on standalone
/// activities being enabled, since only they arm activity timers today.
fn spawn_chasm_timer_sweeper(
    engine: Arc<tokeira_runtime::chasm::ChasmEngine>,
    evaluator: Arc<dyn tokeira_runtime::chasm::TimeoutEvaluator>,
    cancel: CancellationToken,
) {
    let sweeper = tokeira_runtime::chasm::ChasmTimerSweeper::new(engine, evaluator);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(CHASM_TIMER_SWEEP_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = interval.tick() => {
                    sweeper.sweep_once().await;
                }
            }
        }
    });
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
    F: Fn() -> S + Clone + Send + Sync + 'static,
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
    // The Nexus endpoint store is the single source of truth shared by runtime
    // dispatch resolution and the OperatorService endpoint-admin CRUD. Seeded empty;
    // endpoints are created at runtime via `CreateNexusEndpoint`.
    let nexus_store = build_nexus_endpoint_store(
        namespaces.as_ref(),
        HashMap::<String, BootstrapNexusEndpointConfig>::new(),
    )
    .await?;
    let nexus_registry = NexusEndpointRegistry::new(nexus_store.clone());

    // The runtime owns execution orchestration, scanners, brokers, and all
    // run-local in-memory coordination such as buffered consistent queries.
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

    // Bind the inbound Nexus completion listener up front, before constructing the
    // runtime: the firing client's loopback target (`system_callback_url`) must reflect
    // the *actually bound* port, which matters when `http_addr` requests an ephemeral
    // `:0` port (parallel in-memory servers, tests). The accept loop is spawned later,
    // once `background_cancel` and the runtime adapter exist; binding here only reserves
    // the socket and reads back its address.
    let nexus_completion_cfg = effective_config.policy.nexus_completion.clone();
    let nexus_callback_listener = TcpListener::bind(&nexus_completion_cfg.http_addr)
        .await
        .with_context(|| {
            format!(
                "failed to bind nexus completion listener on {}",
                nexus_completion_cfg.http_addr
            )
        })?;
    let nexus_callback_addr = nexus_callback_listener
        .local_addr()
        .context("failed to resolve bound nexus completion listener address")?;
    let resolved_system_callback_url = with_loopback_port(
        &nexus_completion_cfg.system_callback_url,
        nexus_callback_addr.port(),
    );
    info!(
        bind = %nexus_callback_addr,
        loopback = %resolved_system_callback_url,
        "nexus completion callback listener bound"
    );
    let nexus_completion_deps = NexusCompletionDeps {
        client: Arc::new(HttpNexusCompletionClient::new()),
        config: NexusCompletionRuntimeConfig {
            system_callback_url: resolved_system_callback_url,
            retry_initial_interval: time::Duration::milliseconds(
                nexus_completion_cfg.retry_initial_interval_ms as i64,
            ),
            retry_max_interval: time::Duration::milliseconds(
                nexus_completion_cfg.retry_max_interval_ms as i64,
            ),
            retry_backoff_coefficient: nexus_completion_cfg.retry_backoff_coefficient,
            retry_max_attempts: nexus_completion_cfg.retry_max_attempts,
        },
        scanner: CompletionCallbackScannerConfig::default(),
    };

    let runtime = TokeiraRuntime::new_with_nexus_and_shards_and_endpoint(
        repo.clone(),
        runtime_config.lane_count,
        runtime_config.lane,
        runtime_config.timer_scanner,
        runtime_config.workflow_timeout_scanner,
        runtime_config.backlog,
        runtime_config.activity_timeout_scanner,
        runtime_config.nexus_timeout_scanner,
        nexus_registry,
        Arc::new(HttpNexusClient::new()),
        // Wave 5: the real `HttpNexusCompletionClient` POSTs completions to the
        // inbound `/nexus/callback` listener bound above; `system_callback_url` was
        // resolved to that listener's actual address so the loopback closes.
        nexus_completion_deps,
        effective_config.infrastructure.placement.shard_count,
        node_id.to_string(),
        node_endpoint.as_authority(),
        seed_default_shard,
        // Tag the External-endpoint outbound Nexus metric with the originator's
        // namespace name, resolved through the shared edge namespace cache.
        Some(Arc::new(CacheNexusNamespaceResolver {
            namespaces: namespaces.clone(),
        }) as Arc<dyn NexusNamespaceResolver>),
    )
    // The edge always exposes Worker Deployment v2 RPCs. Wiring the
    // repository here keeps their registry durable for both in-memory and
    // DSQL backends instead of falling back to a detached test registry.
    .with_worker_deployment_repository(worker_deployment_repository);
    let runtime = Arc::new(runtime);

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
        ScheduleEngineConfig::default(),
        background_cancel.clone(),
    );

    let authorization = build_authorization_stack(&effective_config).await?;
    let interceptors = Arc::new(EdgeInterceptors::configured(
        namespaces.clone(),
        authorization.grpc,
        authorization.principal_attribution,
    ));
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
    // Serve the inbound completion endpoint on the pre-bound listener. The adapter is the
    // same `WorkflowRuntimeApi` the gRPC surface uses, so a callback resolves the
    // originator's pending operation through the identical runtime path.
    spawn_nexus_callback_server(
        nexus_callback_listener,
        runtime_adapter.clone() as Arc<dyn WorkflowRuntimeApi>,
        background_cancel.clone(),
    );
    let resolver = Arc::new(StoreExecutionResolver::new(repo.clone()));
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
            sink: projection_sink(),
            batch_size: 256,
        };
        let projection_cancel = background_cancel.clone();
        tokio::spawn(async move {
            if let Err(error) = projection_worker
                .run_from_cursor(
                    projection_cancel,
                    ProjectionCursor::beginning(partition_id, 1),
                )
                .await
            {
                tracing::warn!(?error, partition_id, "projection worker exited");
            }
        });
    }

    let nexus_http_waiters = tokeira_edge::nexus_http::NexusHttpWaiterRegistry::default();
    let workflow_service =
        WorkflowService::new_with_stores_and_buffered_queries_and_history_wait_registry(
            runtime_adapter.clone(),
            resolver,
            visibility,
            repo.clone(),
            operator_api.clone(),
            namespaces.clone(),
            interceptors.clone(),
            PollerRegistry::default(),
            PendingQueryStore::default(),
            buffered_queries,
            workflow_broker,
            nexus_task_broker.clone(),
            long_polls,
            router,
            history_waits,
            worker_registry,
            runtime.heartbeat_store(),
            schedule_store,
            task_queue_config_store,
            Arc::new(tokeira_runtime::BatchOperationStore::default()),
        )
        .with_nexus_http_waiters(nexus_http_waiters.clone())
        .with_worker_deployment_runtime(runtime_adapter);
    // The Nexus endpoint admin shares the dispatch store, gated through the operator
    // interceptor (create/update/delete = OperatorWrite, get/list = OperatorRead).
    // Limits come from config (raise, never hardcode); the namespace resolver backs
    // the Worker-target existence check.
    let nexus_endpoint_limits = {
        let cfg = &effective_config.policy.nexus_endpoint_limits;
        tokeira_edge::nexus_endpoint::NexusEndpointLimits {
            name_max_length: cfg.name_max_length,
            external_url_max_length: cfg.external_url_max_length,
            description_max_size: cfg.description_max_size,
            task_queue_max_length: cfg.task_queue_max_length,
            list_default_page_size: cfg.list_default_page_size,
            list_max_page_size: cfg.list_max_page_size,
        }
    };
    let nexus_endpoint_admin = Arc::new(tokeira_edge::nexus_endpoint::NexusEndpointAdmin::new(
        nexus_store.clone(),
        Arc::new(tokeira_edge::nexus_endpoint::CacheNamespaceResolver::new(
            namespaces.clone(),
        )),
        nexus_endpoint_limits,
    ));
    let operator_service = OperatorService::new(operator_api, interceptors)
        .with_nexus_endpoints(nexus_endpoint_admin)
        .with_namespace_deletion(namespaces.clone(), Arc::new(workflow_service.clone()));
    let production_nexus_authorizer = authorization.nexus;
    let nexus_http_authorizer: Arc<dyn tokeira_edge::nexus_http::NexusHttpAuthorizer> = {
        #[cfg(feature = "conformance")]
        {
            ConformanceNexusHttpAuthorizer::from_environment(production_nexus_authorizer.clone())
                .map(|authorizer| {
                    Arc::new(authorizer) as Arc<dyn tokeira_edge::nexus_http::NexusHttpAuthorizer>
                })
                .unwrap_or(production_nexus_authorizer)
        }
        #[cfg(not(feature = "conformance"))]
        {
            production_nexus_authorizer
        }
    };
    let nexus_http_layer =
        NexusHttpLayer::new(Arc::new(tokeira_edge::nexus_http::NexusHttpHandler::new(
            namespaces.clone(),
            nexus_store,
            nexus_task_broker,
            nexus_http_waiters,
            nexus_http_authorizer,
        )));

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
        // Capture what the visibility repair scanner needs before the engine consumes
        // the node repo + registry: the activity archetype id (to dispatch the snapshot
        // rebuild) and a clone of the authoritative node store (Req 10.11).
        let repair_archetype = registry.archetype_id(
            <tokeira_chasm_activity::ActivityExecution as tokeira_chasm::Component>::FQN,
        );
        let repair_nodes = chasm_node_repo.clone();
        let dispatch_queue = Arc::new(tokeira_edge::chasm_activity::ActivityDispatchQueue::new());
        // Standalone activities flow into the shared visibility index via the
        // engine→projection adapter, post-commit and off the correctness path
        // (spec task 24.2). It reuses the same projection apply path as the workflow
        // visibility worker, so both archetypes land in one logical index.
        let chasm_visibility_sink =
            Arc::new(tokeira_runtime::chasm::ProjectionVisibilitySink::new(
                Arc::new(projection_sink()),
                effective_config.infrastructure.placement.partition_count,
            ));
        let chasm_engine = Arc::new(tokeira_runtime::chasm::ChasmEngine::new(
            chasm_node_repo,
            registry,
            dispatch_queue.clone(),
            chasm_visibility_sink,
        ));
        let activity_config = tokeira_chasm_activity::ActivityConfig {
            enable_standalone: effective_config
                .policy
                .compatibility
                .enable_standalone_activities,
            ..tokeira_chasm_activity::ActivityConfig::default()
        };
        let standalone_enabled = activity_config.enable_standalone;
        // Keep an engine handle for the timer sweeper before the bridge takes it.
        let sweeper_engine = chasm_engine.clone();
        let activity_bridge = Arc::new(
            tokeira_edge::chasm_activity::ActivityBridge::new(
                chasm_engine,
                activity_config,
                DEFAULT_MAX_ID_LENGTH,
            )
            .with_dispatch_queue(dispatch_queue),
        );
        // Spawn the visibility repair scanner (Req 10.11): a committed transition can
        // never permanently lack a projection — the scanner rebuilds each execution's
        // snapshot from authoritative node state and re-applies it iff-newer. This is
        // the AGENTS-3 sweeper shape (derived effect reconstructed from authoritative
        // state), the durability backstop for the best-effort post-commit write (24.2).
        spawn_visibility_repair(
            repair_nodes,
            Arc::new(projection_sink()),
            repair_archetype,
            effective_config.infrastructure.placement.partition_count,
            background_cancel.clone(),
        );
        // Spawn the CHASM timer sweeper (`chasm-activity-timeouts-and-retry`): nothing
        // else fires armed activity timeouts. A conformance build must keep it ready
        // even when the boot-time default is off, because the corpus enables
        // `activity.enableStandalone` live after server startup. Production retains
        // the configured gate and pays no idle sweep cost.
        if standalone_enabled || cfg!(feature = "conformance") {
            spawn_chasm_timer_sweeper(
                sweeper_engine,
                activity_bridge.clone(),
                background_cancel.clone(),
            );
        }
        WorkflowServiceGrpc::new(workflow_service.clone()).with_chasm_activity(activity_bridge)
    };
    // Minimal AdminService (DescribeMutableState) shares the WorkflowService's
    // run repository — the reset conformance suite reads a run's ResetRunId/status.
    let admin_grpc = AdminServiceGrpc::new(workflow_service);
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
                    .layer(nexus_http_layer.clone())
                    .layer(CorsLayer::permissive())
                    .layer(GrpcWebLayer::new())
                    .layer(WireCoverageLayer::new(recorder))
                    .add_service(workflow_grpc.into_service())
                    .add_service(operator_grpc.into_service())
                    .add_service(admin_grpc.into_service())
                    .add_service(reflection)
                    .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown_signal)
                    .await
            }
            None => {
                Server::builder()
                    .accept_http1(true)
                    .layer(nexus_http_layer)
                    .layer(CorsLayer::permissive())
                    .layer(GrpcWebLayer::new())
                    .add_service(workflow_grpc.into_service())
                    .add_service(operator_grpc.into_service())
                    .add_service(admin_grpc.into_service())
                    .add_service(reflection)
                    .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown_signal)
                    .await
            }
        }
        .with_context(|| format!("failed to serve gRPC transport on {bound_addr}"))?;
        Ok::<(), anyhow::Error>(())
    });

    // Conformance-only dynamic-config control listener (spec
    // `.kiro/specs/conformance-config-override/`). Mounted on a SEPARATE
    // loopback listener — never the public gRPC router — and only when the fork
    // harness set `TOKEIRA_CONFORMANCE_CONTROL_ADDR`. The whole block is behind
    // the `conformance` feature, so a production build never contains it.
    #[cfg(feature = "conformance")]
    {
        if let Some(control_addr) = conformance_control_addr() {
            let control_cancel = background_cancel.clone();
            tokio::spawn(async move {
                let router = connectrpc::Router::new().add_service(std::sync::Arc::new(
                    tokeira_conformance_control::ConformanceControlHandler,
                ));
                match connectrpc::Server::bind(control_addr).await {
                    Ok(bound) => {
                        if let Err(error) = bound
                            .serve_with_graceful_shutdown(router, control_cancel.cancelled_owned())
                            .await
                        {
                            tracing::error!(%error, "conformance control service exited with error");
                        }
                    }
                    Err(error) => tracing::error!(
                        %error,
                        %control_addr,
                        "failed to bind conformance control listener"
                    ),
                }
            });
            tracing::warn!(%control_addr, "conformance control service mounted (conformance build)");
        }
    }

    Ok((
        server_task,
        bound_addr,
        shutdown_tx,
        background_cancel,
        log_broadcast,
        wire_coverage_recorder,
    ))
}

/// Replace the port in a `scheme://host[:port]` loopback base URL with the actually-bound
/// listener `port`. This keeps `system_callback_url` (scheme + host from config) pointed at
/// the listener even when `http_addr` requested an ephemeral `:0` port.
fn with_loopback_port(base: &str, port: u16) -> String {
    let trimmed = base.trim_end_matches('/');
    match trimmed.rsplit_once(':') {
        // Only treat the suffix as a port when it is all digits — otherwise the match is
        // the `:` inside the scheme (`http://host` with no port) and we append instead.
        Some((prefix, suffix))
            if !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit()) =>
        {
            format!("{prefix}:{port}")
        }
        _ => format!("{trimmed}:{port}"),
    }
}

/// Serve the inbound `POST /nexus/callback` completion endpoint on `listener` until
/// `cancel` fires. Each request is delegated to [`handle_nexus_callback`], which resolves
/// the originator workflow's pending operation — the inbound half of the async-completion
/// loopback the runtime's firing client drives.
fn spawn_nexus_callback_server(
    listener: TcpListener,
    runtime: Arc<dyn WorkflowRuntimeApi>,
    cancel: CancellationToken,
) {
    tokio::spawn(async move {
        loop {
            let stream = tokio::select! {
                _ = cancel.cancelled() => break,
                accept = listener.accept() => match accept {
                    Ok((stream, _peer)) => stream,
                    Err(error) => {
                        tracing::warn!(?error, "nexus callback listener accept failed");
                        continue;
                    }
                },
            };
            let io = TokioIo::new(stream);
            let runtime = runtime.clone();
            let conn_cancel = cancel.clone();
            tokio::spawn(async move {
                let service = service_fn(move |request| {
                    let runtime = runtime.clone();
                    async move {
                        Ok::<_, Infallible>(
                            nexus_callback_response(runtime.as_ref(), request).await,
                        )
                    }
                });
                let conn = hyper::server::conn::http1::Builder::new().serve_connection(io, service);
                tokio::pin!(conn);
                tokio::select! {
                    result = conn.as_mut() => {
                        if let Err(error) = result {
                            tracing::debug!(?error, "nexus callback connection closed with error");
                        }
                    }
                    _ = conn_cancel.cancelled() => {
                        conn.as_mut().graceful_shutdown();
                        let _ = conn.await;
                    }
                }
            });
        }
    });
}

/// Adapt a hyper request to the transport-agnostic edge handler. Non-`POST` requests and
/// any path other than `/nexus/callback` are 404; the handler owns all other status
/// mapping (decode failures → 400, accepted → 200, not-found/stale → 404, internal → 503).
async fn nexus_callback_response(
    runtime: &dyn WorkflowRuntimeApi,
    request: Request<Incoming>,
) -> Response<Full<Bytes>> {
    let (parts, body) = request.into_parts();

    if parts.method != Method::POST || parts.uri.path() != NEXUS_CALLBACK_PATH {
        return nexus_response(StatusCode::NOT_FOUND, None);
    }

    let header = |name: &str| {
        parts
            .headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
    };
    let token = header(TEMPORAL_CALLBACK_TOKEN_HEADER);
    let state = header(NEXUS_OPERATION_STATE_HEADER);
    let content_type = header(hyper::header::CONTENT_TYPE.as_str());

    let body_bytes = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(error) => {
            tracing::debug!(?error, "failed to read nexus callback request body");
            return nexus_response(
                StatusCode::BAD_REQUEST,
                Some(b"failed to read request body".to_vec()),
            );
        }
    };

    let CallbackResponse { status, body } = handle_nexus_callback(
        runtime,
        token.as_deref(),
        state.as_deref(),
        content_type.as_deref(),
        &body_bytes,
    )
    .await;

    let status = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    nexus_response(status, body)
}

fn nexus_response(status: StatusCode, body: Option<Vec<u8>>) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(Bytes::from(body.unwrap_or_default())));
    *response.status_mut() = status;
    response
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

/// The loopback address the conformance dynamic-config control service binds to,
/// if the fork harness enabled it.
///
/// Conformance-only (spec `.kiro/specs/conformance-config-override/`): the
/// harness sets `TOKEIRA_CONFORMANCE_CONTROL_ADDR` to a concrete loopback
/// address when booting `tokeirad` for the corpus; its presence enables the
/// control listener and its value is the bind address. Read only in a
/// `conformance` build — production never references it. Like the wire-coverage
/// enable seam this is a test-harness switch, not a production configuration
/// surface: it carries no dynamic-config value, only where to listen.
#[cfg(feature = "conformance")]
fn conformance_control_addr() -> Option<std::net::SocketAddr> {
    std::env::var("TOKEIRA_CONFORMANCE_CONTROL_ADDR")
        .ok()
        .and_then(|value| value.trim().parse().ok())
}

/// Build the shared Nexus endpoint store, seeding any bootstrap-configured
/// endpoints. The same store backs both runtime dispatch resolution (via
/// [`NexusEndpointRegistry`]) and the OperatorService endpoint-admin CRUD, so a
/// `CreateNexusEndpoint` at runtime is immediately resolvable for dispatch and a
/// seeded endpoint is visible to admin reads. Worker targets resolve the namespace
/// name to its id up front (failing if the namespace is unknown), storing both so
/// reads echo the name and dispatch routes on the id.
async fn build_nexus_endpoint_store(
    namespaces: &dyn NamespaceCache,
    configs: HashMap<String, BootstrapNexusEndpointConfig>,
) -> Result<Arc<InMemoryNexusEndpointStore>> {
    let store = Arc::new(InMemoryNexusEndpointStore::new());
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0);
    for (endpoint_name, config) in configs {
        let target = match config.target {
            BootstrapNexusEndpointTarget::External { address } => {
                NexusEndpointSpecTarget::External { url: address }
            }
            BootstrapNexusEndpointTarget::Worker {
                namespace_name,
                task_queue,
            } => {
                let namespace = namespaces.get(&namespace_name).await?.ok_or_else(|| {
                    anyhow!(
                        "failed to register nexus worker endpoint `{endpoint_name}`: namespace `{namespace_name}` not found"
                    )
                })?;
                NexusEndpointSpecTarget::Worker {
                    namespace_name: namespace.name.clone(),
                    namespace_id: namespace_id_for(&namespace.name).0.to_string(),
                    task_queue,
                }
            }
        };
        store
            .create(
                NexusEndpointSpec {
                    name: endpoint_name.clone(),
                    description: Vec::new(),
                    target,
                },
                now,
            )
            .map_err(|e| anyhow!("failed to seed nexus endpoint `{endpoint_name}`: {e}"))?;
    }
    Ok(store)
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

/// Resolves executions and builds describe responses straight from the run
/// repository. The namespace is taken from each request (via `namespace_id_for`),
/// not bound at construction — a single server serves every namespace, so freezing
/// it to one (e.g. `default`) would make query/describe miss every other namespace's
/// runs while history (which derives the namespace per request) still found them.
struct StoreExecutionResolver<R> {
    repo: Arc<R>,
}

impl<R> StoreExecutionResolver<R> {
    fn new(repo: Arc<R>) -> Self {
        Self { repo }
    }
}

fn apply_reported_problem_search_attribute(
    search_attributes: &mut tokeira_types::SearchAttributes,
    problem: Option<WorkflowTaskReportedProblem>,
) {
    let Some(problem) = problem else {
        return;
    };
    // v1.31.0 exposes exactly these two KeywordList elements, derived from the
    // last NON-TRANSIENT WFT problem once AttemptsSinceLastSuccess reaches the
    // configured threshold — a `Failed`-flavored pair or a `TimedOut` pair,
    // per the persisted `LastWorkflowTaskFailure` oneof
    // (`mutable_state_impl.go:6478-6491 @ v1.31.0`; the timed-out cause
    // renders `TimeoutType.String()`, which is "StartToClose" for the only
    // type v1.31.0 ever stores).
    let entries = match &problem.problem {
        tokeira_kernel::WorkflowTaskProblem::Failed(cause) => vec![
            "category=WorkflowTaskFailed".to_string(),
            format!("cause=WorkflowTaskFailedCause{}", cause.as_str()),
        ],
        tokeira_kernel::WorkflowTaskProblem::TimedOutStartToClose => vec![
            "category=WorkflowTaskTimedOut".to_string(),
            "cause=WorkflowTaskTimedOutCauseStartToClose".to_string(),
        ],
    };
    search_attributes.0.insert(
        "TemporalReportedProblems".to_string(),
        SearchAttrValue::KeywordList(entries),
    );
}

#[async_trait]
impl<R> ExecutionResolver for StoreExecutionResolver<R>
where
    R: RunRepository + 'static,
{
    async fn current_run_key(
        &self,
        namespace: &str,
        workflow_id: &str,
    ) -> Result<Option<tokeira_types::RunKey>> {
        let namespace_id = namespace_id_for(namespace);
        let result = self
            .repo
            .resolve_execution(&ExecutionRef {
                namespace_id,
                workflow_id: WorkflowId(workflow_id.to_string()),
                run_id: None,
            })
            .await?;
        if result.is_some() {
            return Ok(result);
        }
        self.repo
            .find_latest_run(namespace_id, &WorkflowId(workflow_id.to_string()))
            .await
    }

    async fn describe_execution(
        &self,
        namespace: &str,
        workflow_id: &str,
        run_id: Option<tokeira_types::RunId>,
    ) -> Result<Option<WorkflowExecutionDescription>> {
        let namespace_id = namespace_id_for(namespace);
        let run_key = match self
            .repo
            .resolve_execution(&ExecutionRef {
                namespace_id,
                workflow_id: WorkflowId(workflow_id.to_string()),
                run_id,
            })
            .await?
        {
            Some(rk) => rk,
            None if run_id.is_none() => {
                match self
                    .repo
                    .find_latest_run(namespace_id, &WorkflowId(workflow_id.to_string()))
                    .await?
                {
                    Some(rk) => rk,
                    None => return Ok(None),
                }
            }
            None => return Ok(None),
        };

        match self.repo.load_run(run_key).await? {
            LoadedRun::Existing(state) => {
                // Describe surfaces history size and external-payload statistics
                // (`describeworkflow/api.go:126,166 @ v1.31.0`). Tokeira derives
                // both from the full committed history on read — history remains
                // authoritative and no denormalized counter can drift.
                let history = self.repo.read_history(run_key, 0, usize::MAX).await?;
                let history_size_bytes =
                    tokeira_edge::translate::history_serializer::serialized_history_size_bytes(
                        &history,
                    );
                let (external_payload_count, external_payload_size_bytes) =
                    tokeira_edge::translate::external_payload_stats(&history);
                // Derived straight from committed kernel state: the counter
                // and problem identity live on the run, the threshold is read
                // live at derive time (Tier 3.22 `DynamicConfigChanges`).
                let reported_problem = reported_problem_from_state(&state);
                let versioning_info = state.versioning_info.clone();
                let most_recent_worker_version_stamp = versioning_info
                    .as_ref()
                    .and_then(|info| info.most_recent_worker_version_stamp.clone());
                let pending_nexus_operations =
                    tokeira_edge::translate::describe_pending_nexus_operations(&state);
                let mut search_attributes = state.search_attributes;
                apply_reported_problem_search_attribute(&mut search_attributes, reported_problem);
                if let Some(build_ids) = versioning_info
                    .as_ref()
                    .map(|info| &info.build_id_search_attributes)
                    .filter(|build_ids| !build_ids.is_empty())
                {
                    search_attributes.0.insert(
                        "BuildIds".to_string(),
                        tokeira_types::SearchAttrValue::KeywordList(build_ids.clone()),
                    );
                }
                Ok(Some(WorkflowExecutionDescription {
                    namespace: namespace.to_string(),
                    workflow_id: state.workflow_id.0,
                    run_key: state.run_key,
                    run_id: state.run_id,
                    workflow_type: state.workflow_type.0,
                    task_queue: state.task_queue.0.clone(),
                    status: state.status,
                    start_time: Some(state.started_at),
                    close_time: state.closed_at,
                    // v1.31.0 ExecutionTime = this run's StartTime + FirstWorkflowTaskBackoff
                    // (mutable_state_impl.go:2859) — NOT the chain's first-run start. tokeira
                    // carries that backoff (client start delay / workflow-retry backoff) as
                    // `workflow_start_delay`; TestWorkflowRetry asserts start + backoff per attempt.
                    execution_time: state.started_at
                        + state.workflow_start_delay.unwrap_or_default(),
                    execution_config: tokeira_edge::translate::ExecutionConfigDescription {
                        task_queue: state.task_queue.0.clone(),
                        workflow_execution_timeout: state.workflow_execution_timeout,
                        workflow_run_timeout: state.workflow_run_timeout,
                        default_workflow_task_timeout: state.workflow_task_timeout,
                        // Describe returns start-event metadata verbatim, even after later
                        // transitions (`describeworkflow/api.go:98-110 @ v1.31.0`). The kernel
                        // summary is replayed from that event, so no history scan is needed here.
                        user_metadata: state.user_metadata.map(|metadata| {
                            tokeira_edge::translate::UserMetadata {
                                summary: metadata.summary,
                                details: metadata.details,
                            }
                        }),
                    },
                    history_length: state.last_event_id,
                    history_size_bytes,
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
                    search_attributes,
                    pending_activities: state
                        .activities
                        .values()
                        .map(
                            |activity| tokeira_edge::translate::PendingActivityDescription {
                                activity_id: activity.activity_id.clone(),
                                activity_type: activity.activity_type.clone(),
                                is_started: activity.started_at.is_some(),
                                cancel_requested: activity.cancel_requested,
                                attempt: activity.attempt,
                                maximum_attempts: activity
                                    .retry_policy
                                    .as_ref()
                                    .map(|policy| policy.maximum_attempts)
                                    .unwrap_or_default(),
                                scheduled_at: activity.scheduled_at,
                                started_at: activity.started_at,
                                last_failure: activity.last_failure.clone(),
                                heartbeat_details: activity.heartbeat_details.clone(),
                                last_worker_identity: {
                                    // `LastWorkerIdentity = StartedIdentity`, falling
                                    // back to `RetryLastWorkerIdentity` when empty and
                                    // a retry policy exists (`GetPendingActivityInfo`,
                                    // workflow/activity.go:159-166 @ v1.31.0).
                                    let started = activity
                                        .started_identity
                                        .clone()
                                        .map(|identity| identity.0)
                                        .unwrap_or_default();
                                    if started.is_empty() && activity.retry_policy.is_some() {
                                        activity
                                            .retry_last_worker_identity
                                            .clone()
                                            .map(|identity| identity.0)
                                            .unwrap_or_default()
                                    } else {
                                        started
                                    }
                                },
                                paused: activity.pause_info.is_some(),
                                pause_info: activity.pause_info.as_ref().map(|info| {
                                    tokeira_edge::translate::PauseInfoDescription {
                                        identity: info.identity.clone(),
                                        paused_time: info.pause_time,
                                        reason: info.reason.clone(),
                                        rule_id: info.rule_id.clone(),
                                    }
                                }),
                                activity_options: tokeira_edge::translate::ActivityOptions {
                                    task_queue: Some(activity.task_queue.0.clone()),
                                    schedule_to_close_timeout: activity.schedule_to_close_timeout,
                                    schedule_to_start_timeout: activity.schedule_to_start_timeout,
                                    start_to_close_timeout: activity.start_to_close_timeout,
                                    heartbeat_timeout: activity.heartbeat_timeout,
                                    retry_policy: activity.retry_policy.clone(),
                                },
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
                    pending_nexus_operations,
                    pause_info: state.pause_info.as_ref().map(|info| {
                        tokeira_edge::translate::PauseInfoDescription {
                            identity: info.identity.clone(),
                            paused_time: info.pause_time,
                            reason: info.reason.clone(),
                            rule_id: None,
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
                    versioning_info: versioning_info
                        .filter(|info| info.has_execution_versioning_info()),
                    worker_deployment_name: state.worker_deployment_name.clone(),
                    most_recent_worker_version_stamp,
                    request_id_infos: state.request_id_infos.clone(),
                    external_payload_count,
                    external_payload_size_bytes,
                }))
            }
            LoadedRun::Absent => Err(anyhow!("resolved run missing from storage: {:?}", run_key)),
        }
    }
}

#[doc(hidden)]
pub fn __cli_parse() -> Cli {
    Cli::parse()
}

#[cfg(test)]
// Edition-2024 `std::env` mutation in tests — each site carries its SAFETY comment.
#[allow(unsafe_code)]
mod tests {
    use super::*;
    // Endpoint target/config types are referenced only by the Nexus endpoint store
    // tests below; importing them here (not at crate scope) keeps the lib build clean.
    use tokeira_runtime::{EndpointTarget, NexusEndpointConfig};

    #[test]
    fn with_loopback_port_rewrites_or_appends_port() {
        // A base URL with an explicit port has it replaced with the bound port.
        assert_eq!(
            with_loopback_port("http://127.0.0.1:7253", 51_000),
            "http://127.0.0.1:51000"
        );
        // A trailing slash is trimmed before rewriting.
        assert_eq!(
            with_loopback_port("http://127.0.0.1:7253/", 51_000),
            "http://127.0.0.1:51000"
        );
        // A port-less base URL gets the port appended (the scheme `:` is not mistaken for
        // a port because the suffix is not all-digits).
        assert_eq!(
            with_loopback_port("http://localhost", 51_000),
            "http://localhost:51000"
        );
    }

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

    #[test]
    fn reported_problem_search_attribute_has_exact_v131_keyword_list() {
        let mut search_attributes = tokeira_types::SearchAttributes::default();
        apply_reported_problem_search_attribute(
            &mut search_attributes,
            Some(WorkflowTaskReportedProblem {
                attempts_since_last_success: 5,
                problem: tokeira_kernel::WorkflowTaskProblem::Failed(
                    tokeira_kernel::WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure,
                ),
            }),
        );

        assert_eq!(
            search_attributes.0.get("TemporalReportedProblems"),
            Some(&SearchAttrValue::KeywordList(vec![
                "category=WorkflowTaskFailed".to_string(),
                "cause=WorkflowTaskFailedCauseWorkflowWorkerUnhandledFailure".to_string(),
            ]))
        );
    }

    #[test]
    fn reported_problem_search_attribute_renders_timeout_category() {
        // A timeout-first sequence reports the TimedOut pair — v1.31.0 renders
        // `TimeoutType.String()` for the only stored type
        // (`mutable_state_impl.go:6486-6491 @ v1.31.0`).
        let mut search_attributes = tokeira_types::SearchAttributes::default();
        apply_reported_problem_search_attribute(
            &mut search_attributes,
            Some(WorkflowTaskReportedProblem {
                attempts_since_last_success: 5,
                problem: tokeira_kernel::WorkflowTaskProblem::TimedOutStartToClose,
            }),
        );

        assert_eq!(
            search_attributes.0.get("TemporalReportedProblems"),
            Some(&SearchAttrValue::KeywordList(vec![
                "category=WorkflowTaskTimedOut".to_string(),
                "cause=WorkflowTaskTimedOutCauseStartToClose".to_string(),
            ]))
        );
    }

    #[tokio::test]
    async fn build_nexus_endpoint_store_resolves_worker_namespace_names() {
        let cache = InMemoryNamespaceCache::new();
        let namespace = ResolvedNamespace::active("payments");
        let namespace_id = namespace_id_for("payments");
        cache.insert(namespace).await.expect("namespace insert");

        let store = build_nexus_endpoint_store(
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
        .expect("store should build");

        let registry = NexusEndpointRegistry::new(store);
        let config = registry.resolve("payments-endpoint").expect("endpoint");
        assert_eq!(
            config,
            NexusEndpointConfig {
                target: EndpointTarget::Worker {
                    namespace_id,
                    task_queue: tokeira_types::TaskQueueName("nexus-q".to_string()),
                },
            }
        );
    }

    #[tokio::test]
    async fn build_nexus_endpoint_store_rejects_unknown_worker_namespace() {
        let cache = InMemoryNamespaceCache::new();

        let result = build_nexus_endpoint_store(
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
        let error = result.expect_err("error");

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
        // SAFETY: all mutation of this env var is confined to this single test.
        unsafe { std::env::remove_var(WIRE_COVERAGE_OUT_ENV) };
        assert_eq!(
            wire_coverage_out_path(),
            PathBuf::from(WIRE_COVERAGE_DEFAULT_OUT)
        );

        // A real path is honoured verbatim (after trimming).
        // SAFETY: all mutation of this env var is confined to this single test.
        unsafe { std::env::set_var(WIRE_COVERAGE_OUT_ENV, "  /tmp/cov.json  ") };
        assert_eq!(wire_coverage_out_path(), PathBuf::from("/tmp/cov.json"));

        // An empty/whitespace override is treated as unset, not as a blank path.
        // SAFETY: all mutation of this env var is confined to this single test.
        unsafe { std::env::set_var(WIRE_COVERAGE_OUT_ENV, "   ") };
        assert_eq!(
            wire_coverage_out_path(),
            PathBuf::from(WIRE_COVERAGE_DEFAULT_OUT)
        );

        // SAFETY: all mutation of this env var is confined to this single test.
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
