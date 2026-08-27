//! Embeddable Temporal-compatible execution engine and `tokeirad` service bootstrap.
//!
//! [`Engine`] is the zero-listener entry point: it owns an in-memory authoritative
//! store, runtime workers, and the Temporal edge router. Its [`TemporalEndpoint`]
//! accepts raw protobuf calls and, with the default `temporalio-client` feature,
//! produces a callback service for `ConnectionOptions::service_override`.
//!
//! `tokeirad` also consumes this crate. [`TokeiradHandle::start_in_memory`] and
//! [`run_from_cli`] attach the optional gRPC, HTTP/JSON, gRPC-Web, and Nexus HTTP
//! transports after the shared service graph has been constructed. Embedded and
//! listener-backed operation therefore execute the same edge handlers and runtime
//! semantics; only their transport and shutdown handles differ.

#![deny(rust_2018_idioms)]
// The bootstrap facade wires many subsystems and accepts them as parameters.
#![allow(clippy::too_many_arguments)]

use std::{
    collections::HashMap,
    convert::Infallible,
    future::Future,
    net::SocketAddr,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc, RwLock, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration as StdDuration, Instant},
};

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use clap::Parser;
use http_body_util::{BodyExt, Full, LengthLimitError, Limited};
use hyper::{
    Method, Request, Response, StatusCode,
    body::{Bytes, Incoming},
    service::service_fn,
};
use hyper_util::rt::TokioIo;
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt as _,
    net::TcpListener,
    sync::{broadcast, mpsc, oneshot},
    task::JoinHandle,
};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tonic_web::GrpcWebLayer;
use tower_http::cors::CorsLayer;
use tracing::{error, info};

#[cfg(feature = "conformance")]
mod conformance_grpc_authenticator;
#[cfg(feature = "conformance")]
mod conformance_nexus_authorizer;
pub mod correlation_format;
mod http_api_transport;
mod nexus_http_transport;
pub mod observability;

#[cfg(feature = "conformance")]
use conformance_grpc_authenticator::ConformanceGrpcAuthenticator;
#[cfg(feature = "conformance")]
use conformance_nexus_authorizer::ConformanceNexusHttpAuthorizer;
use http_api_transport::HttpApiLayer;
use nexus_http_transport::NexusHttpLayer;
use tokeira_auth::{
    Authorizer, ClaimMapper, DefaultAuthorizer, GrantRule, GrantRules, JwksKeyProvider,
    JwtAuthenticator, JwtIssuerProfile, MultiSourceClaimMapper, StsAuthenticator, WorkerScope,
    WorkerScopeRule, WorkerScopeRules,
};
use tokeira_chasm::Library as _;
use tokeira_config::{Cli, ConfigStorageKind};
pub use tokeira_config::{
    DsqlMigrationPolicy, EmbeddedEngineConfig, EmbeddedStorageConfig, SnapshotPolicyConfig,
    TokeiraConfig,
};
use tokeira_edge::{
    Authenticator, CacheBackedRouter, CallbackResponse, EdgeInterceptors, EdgeRoutingConfig,
    HistoryNotifyingRepository, HistoryWaitRegistry, HttpApiCatalog, HttpApiPolicy,
    InMemoryNamespaceCache, InMemoryOperatorApi, InProcessGrpcService, LocalOnlyRouter,
    LongPollConfig, LongPollGate, NamespaceCache, OperatorService, PendingQueryStore,
    PollerRegistry, ResolvedNamespace, RoutingCache, ScopedWorkerSessionRegistry,
    WorkerComputeNamespaceCatalogAdapter, WorkflowExecutionDescription, WorkflowService,
    conformance::{WireCoverageLayer, WireCoverageRecorder},
    grpc::{
        admin_service::AdminServiceGrpc, operator_service::OperatorServiceGrpc,
        runtime_adapter::RuntimeAdapter, workflow_service::WorkflowServiceGrpc,
    },
    handle_nexus_callback,
    nexus_http::MAX_NEXUS_PAYLOAD_BYTES,
    operator_service::{ClusterInfo, OperatorApi, SearchAttributeDefinition},
    run_routing_subscription,
    translate::to_internal::namespace_id_for,
    workflow_service::{ExecutionResolver, WorkflowRuntimeApi},
};
pub use tokeira_edge::{InProcessGrpcRequest, InProcessGrpcResponse};

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
        #[cfg(feature = "conformance")]
        let grpc = ConformanceGrpcAuthenticator::from_environment()
            .map(|authenticator| Arc::new(authenticator) as Arc<dyn Authenticator>)
            .unwrap_or_else(|| Arc::new(tokeira_edge::AllowAllAuthenticator));
        #[cfg(not(feature = "conformance"))]
        let grpc = Arc::new(tokeira_edge::AllowAllAuthenticator) as Arc<dyn Authenticator>;
        let stack = AuthorizationStack {
            grpc,
            nexus: Arc::new(tokeira_edge::nexus_http::PermissiveNexusHttpAuthorizer),
            principal_attribution: false,
        };
        return Ok(stack);
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
        let worker_scopes = WorkerScopeRules::new(
            issuer
                .worker_scopes
                .iter()
                .map(|rule| {
                    let scope = WorkerScope::try_new(
                        rule.namespace.clone(),
                        rule.task_queues.clone(),
                        rule.deployment_name.clone(),
                        rule.build_id.clone(),
                    )
                    .with_context(|| {
                        format!("invalid JWT Worker scope for issuer {}", issuer.name)
                    })?;
                    WorkerScopeRule::new(rule.match_sub.clone(), scope).with_context(|| {
                        format!(
                            "invalid JWT Worker-scope subject for issuer {}",
                            issuer.name
                        )
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        );
        let keys =
            JwksKeyProvider::start(issuer.jwks_uri.clone(), issuer.refresh_interval_duration())
                .await;
        profiles.push(
            JwtIssuerProfile::new(
                issuer.name.clone(),
                issuer.issuer.clone(),
                issuer.audience.clone(),
                issuer.permissions_claim.clone(),
                grants,
                keys,
            )
            .with_worker_scopes(worker_scopes),
        );
    }
    let jwt = (!profiles.is_empty()).then(|| JwtAuthenticator::new(profiles));
    let sts = authorization
        .aws_iam
        .as_ref()
        .map(|aws_iam| {
            let grants = aws_iam
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
                .map(GrantRules::new)?;
            let worker_scopes = WorkerScopeRules::new(
                aws_iam
                    .worker_scopes
                    .iter()
                    .map(|rule| {
                        let scope = WorkerScope::try_new(
                            rule.namespace.clone(),
                            rule.task_queues.clone(),
                            rule.deployment_name.clone(),
                            rule.build_id.clone(),
                        )
                        .context("invalid AWS IAM Worker scope")?;
                        WorkerScopeRule::new(rule.match_arn.clone(), scope)
                            .context("invalid AWS IAM Worker-scope ARN")
                    })
                    .collect::<Result<Vec<_>>>()?,
            );
            Ok::<_, anyhow::Error>(StsAuthenticator::new(grants).with_worker_scopes(worker_scopes))
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
use tokeira_kernel::{Link, LoadedRun};
use tokeira_managed_dsql::{
    AwsDsqlControlPlane, CanonicalClusterIdentity, ClusterAction, CreateOrRecoverRequest,
    LocalClusterDescriptorStore, ManagedDsqlLifecycle, Readiness, ResolvedCluster, StartupDeadline,
    SystemLifecycleEnvironment,
};
use tokeira_observability::{
    ClusterStatusLabel, DbClassLabel, EmbeddedOperationLabel, EmbeddedStorageModeLabel,
    ErrorClassLabel, OwnershipOutcomeLabel, SchemaOutcomeLabel, record_embedded_lifecycle,
};
use tokeira_projection::{
    DsqlVisibilityStore, InMemoryVisibilityStore, ProjectionSink, ProjectionWorker, SearchAttrType,
    VisibilityQueryService, VisibilitySink, VisibilityStore,
};
use tokeira_runtime::{
    ChannelDemandObservationSink, ChannelWorkerComputeReconcileSink,
    CompletionCallbackScannerConfig, CompletionDeliveryOutcome, ConnectionBudgetApplier,
    HttpNexusClient, HttpNexusCompletionClient, InMemoryNexusEndpointStore, MembershipConfig,
    NEXUS_CALLBACK_PATH, NEXUS_OPERATION_STATE_HEADER, NEXUS_OPERATION_TOKEN_HEADER,
    NexusCompletion, NexusCompletionClient, NexusCompletionDeps, NexusCompletionRuntimeConfig,
    NexusEndpointRegistry, NexusEndpointSpec, NexusEndpointSpecTarget, NexusEndpointStore,
    NexusNamespaceResolver, NexusWorkerComputeProvider, OBSERVATION_CHANNEL_CAPACITY,
    RECONCILE_CHANNEL_CAPACITY, RepositoryBackedTaskQueueConfigStore, RuntimeConfig,
    RuntimeShutdownHandle, ScheduleEngineConfig, ScheduleStore, SystemWorkerComputeClock,
    TEMPORAL_CALLBACK_TOKEN_HEADER, TokeiraRuntime, WorkerComputeControllerService,
    WorkerComputeOutbox, WorkerComputeReconciler, WorkflowTaskReportedProblem,
    nexus_payload_to_body, reported_problem_from_state, run_schedule_engine,
    system_callback_post_url,
};
use tokeira_storage::{
    ConnectionDirector, DbClass, InMemoryStore, InMemoryWorkerComputeRepository, LeaseOutcome,
    LeaseRepository, ProjectionLog, RunRepository, TaskQueueConfigRepository,
    WorkerComputeRepository, WorkerDeploymentRepository, WorkerTaskProvenanceStore,
    dsql::{
        ConnectionControlLeaseRepository, ControlLeaseAcquireOutcome, ControlLeaseAcquireRequest,
        ControlLeaseClusterIdentity, ControlLeaseError, ControlLeaseGuard, DsqlAuthConfig,
        DsqlConnectionDirector, DsqlCoordinationConfig, DsqlPoolConfig, DsqlStore,
        EmbeddedDsqlPoolConfig, MigrationRunner, OWNER_ADMISSION_MARGIN, OWNER_LEASE_DURATION,
        OWNER_RENEW_INTERVAL, OwnershipAdmissionGate, OwnershipAdmissionState,
        SCHEMA_MIGRATION_ADMISSION_MARGIN, SCHEMA_MIGRATION_LEASE_DURATION, SchemaDecision,
        SchemaMigrationPolicy, WarmupDeadline,
    },
};
use tokeira_types::{
    ExecutionRef, IncarnationId, NamespaceId, NodeEndpoint, PlacementConfig, ProjectionCursor,
    SearchAttrValue, ShardEpoch, ShardId, WorkflowId,
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

/// A running zero-listener Tokeira engine.
///
/// The engine owns the in-memory authoritative store, runtime workers, and edge
/// services. [`Self::endpoint`] is cloneable and can be adapted directly to the
/// Temporal Rust SDK's callback transport. Dropping the engine cancels background
/// work and makes every endpoint clone reject new calls. When snapshot policy is
/// configured, call [`Self::shutdown`] for the final graceful-shutdown snapshot;
/// `Drop` cannot perform asynchronous file I/O.
#[derive(Debug)]
pub struct Engine {
    endpoint: TemporalEndpoint,
    background_cancel: CancellationToken,
    log_broadcast: broadcast::Sender<LogEvent>,
    snapshot_policy: Option<EngineSnapshotPolicy>,
    recovery_task: Option<JoinHandle<Result<()>>>,
    startup_report: EngineStartupReport,
    shutdown_coordinator: Option<EmbeddedShutdownCoordinator>,
}

/// Storage mode selected by the explicit embedded startup boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EmbeddedStorageMode {
    /// Ephemeral process-local authoritative storage.
    InMemory,
    /// Dedicated cluster created or recovered by the managed lifecycle.
    ManagedDsql,
    /// Canonical operator-supplied DSQL identity.
    ExistingDsql,
}

/// Complete redacted report returned by a successful embedded startup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineStartupReport {
    /// Explicit storage mode that reached admission-open.
    pub storage_mode: EmbeddedStorageMode,
    /// Canonical cluster observation for a durable mode.
    pub cluster: Option<ClusterStartupReport>,
    /// Release-pinned schema outcome for a durable mode.
    pub schema: Option<SchemaStartupReport>,
    /// Exclusive embedded ownership outcome for a durable mode.
    pub ownership: Option<OwnershipStartupReport>,
}

impl EngineStartupReport {
    fn in_memory() -> Self {
        Self {
            storage_mode: EmbeddedStorageMode::InMemory,
            cluster: None,
            schema: None,
            ownership: None,
        }
    }
}

/// Canonical cluster fields safe for host diagnostics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterStartupReport {
    /// AWS Region used by lifecycle and IAM signing.
    pub region: String,
    /// Canonical AWS DSQL cluster ID.
    pub cluster_id: String,
    /// Canonical AWS DSQL cluster ARN.
    pub cluster_arn: String,
    /// Current connection locator, never used as identity.
    pub endpoint: String,
    /// Whether startup created, recovered, or validated the cluster.
    pub action: ClusterAction,
}

/// Schema migration work completed during startup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchemaStartupOutcome {
    /// The database already satisfied the release contract.
    Compatible,
    /// A previously empty database received the complete Tokeira schema.
    Initialized,
    /// A verified older schema advanced to the target version.
    Migrated,
    /// Legacy compatibility metadata was backfilled without schema DDL.
    MetadataBackfilled,
}

/// Redacted release/schema compatibility evidence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaStartupReport {
    /// Highest observed migration version after policy application.
    pub observed_version: u32,
    /// Oldest schema version this release may read.
    pub minimum_supported_version: u32,
    /// Automatic migration target for this release.
    pub target_version: u32,
    /// Newest schema version this release may read.
    pub maximum_readable_version: u32,
    /// Release-pinned migration-set digest; never migration SQL.
    pub migration_set_digest: String,
    /// Startup migration outcome.
    pub outcome: SchemaStartupOutcome,
}

/// Exclusive ownership acquisition evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OwnershipStartupReport {
    /// Whether the claim was clean or replaced an expired owner.
    pub outcome: ControlLeaseAcquireOutcome,
    /// Database-issued monotonic fence token.
    pub fence_token: i64,
}

/// Ordered startup phase used by typed, redacted failures.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EmbeddedStartupPhase {
    /// Validate explicit storage intent and ordinary server policy.
    Configuration,
    /// Create, recover, or validate the canonical cluster identity.
    ClusterResolution,
    /// Establish and warm the bounded process-local DSQL pool.
    ConnectionWarmup,
    /// Assess and apply the release-pinned schema policy.
    Schema,
    /// Acquire the exclusive embedded owner claim.
    Ownership,
    /// Reconstruct runtime state and acquire self-assigned shard leases.
    RuntimeRestore,
}

impl std::fmt::Display for EmbeddedStartupPhase {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Configuration => "configuration validation",
            Self::ClusterResolution => "cluster resolution",
            Self::ConnectionWarmup => "DSQL connection warmup",
            Self::Schema => "schema compatibility",
            Self::Ownership => "embedded ownership",
            Self::RuntimeRestore => "runtime restoration",
        })
    }
}

/// Redacted failure from explicit embedded startup.
#[derive(Debug)]
pub enum EmbeddedEngineStartError {
    /// Configuration failed before any external resource was touched.
    InvalidConfiguration(tokeira_config::EmbeddedConfigError),
    /// One startup phase failed; the nested cause is deliberately discarded.
    Phase {
        /// Phase that did not complete.
        phase: EmbeddedStartupPhase,
    },
    /// The one host-configured startup budget elapsed in the named phase.
    DeadlineExceeded {
        /// Phase active when the shared deadline elapsed.
        phase: EmbeddedStartupPhase,
    },
}

impl std::fmt::Display for EmbeddedEngineStartError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidConfiguration(error) => {
                write!(formatter, "invalid embedded engine configuration: {error}")
            }
            Self::Phase { phase } => {
                write!(formatter, "embedded engine startup failed during {phase}")
            }
            Self::DeadlineExceeded { phase } => {
                write!(
                    formatter,
                    "embedded engine startup deadline exceeded during {phase}"
                )
            }
        }
    }
}

impl std::error::Error for EmbeddedEngineStartError {}

impl From<tokeira_config::EmbeddedConfigError> for EmbeddedEngineStartError {
    fn from(error: tokeira_config::EmbeddedConfigError) -> Self {
        Self::InvalidConfiguration(error)
    }
}

/// Independent cleanup stage that failed during explicit shutdown.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EmbeddedShutdownFailure {
    /// In-process handlers did not drain by the deadline.
    RpcDrain,
    /// Runtime or engine-owned tasks did not join by the deadline.
    RuntimeTasks,
    /// A self-assigned shard lease could not be relinquished.
    ShardRelease,
    /// The conditional embedded-owner release failed.
    OwnershipRelease,
    /// Embedded DSQL pool drain/closure failed.
    Storage,
    /// Final in-memory snapshot persistence failed.
    Snapshot,
}

/// Aggregated explicit-shutdown failure.
#[derive(Debug)]
pub struct EmbeddedEngineShutdownError {
    failures: Vec<EmbeddedShutdownFailure>,
}

impl std::fmt::Display for EmbeddedEngineShutdownError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "embedded engine shutdown completed with {} cleanup failure(s)",
            self.failures.len()
        )
    }
}

impl std::error::Error for EmbeddedEngineShutdownError {}

impl EmbeddedEngineShutdownError {
    /// Ordered cleanup stages that failed; later cleanup was still attempted.
    pub fn failures(&self) -> &[EmbeddedShutdownFailure] {
        &self.failures
    }
}

type CleanupFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
type CleanupAction = Box<dyn FnOnce(Instant) -> CleanupFuture + Send>;

#[derive(Clone)]
struct EmbeddedOwnership {
    repository: ConnectionControlLeaseRepository,
    guard: Arc<tokio::sync::Mutex<Option<ControlLeaseGuard>>>,
    gate: OwnershipAdmissionGate,
}

impl std::fmt::Debug for EmbeddedOwnership {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EmbeddedOwnership")
            .field("gate", &self.gate)
            .finish_non_exhaustive()
    }
}

struct EmbeddedShutdownCoordinator {
    service: InProcessGrpcService,
    runtime_tasks: RuntimeShutdownHandle,
    engine_tasks: RuntimeShutdownHandle,
    shard_cleanup: Option<CleanupAction>,
    director: Option<Arc<DsqlConnectionDirector>>,
    ownership: Option<EmbeddedOwnership>,
}

impl std::fmt::Debug for EmbeddedShutdownCoordinator {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EmbeddedShutdownCoordinator")
            .field("service", &self.service)
            .field("runtime_tasks", &self.runtime_tasks)
            .field("engine_tasks", &self.engine_tasks)
            .field("has_shard_cleanup", &self.shard_cleanup.is_some())
            .field("has_director", &self.director.is_some())
            .field("ownership", &self.ownership)
            .finish()
    }
}

#[derive(Debug)]
struct EngineSnapshotPolicy {
    store: InMemoryStore,
    location: PathBuf,
    periodic_task: Option<JoinHandle<()>>,
}

static SNAPSHOT_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

impl EngineSnapshotPolicy {
    fn start(
        store: InMemoryStore,
        config: SnapshotPolicyConfig,
        cancel: CancellationToken,
    ) -> Self {
        let interval_duration = std::time::Duration::from_millis(config.interval_ms);
        let location = config.location;
        let periodic_store = store.clone();
        let periodic_location = location.clone();
        // Anchor the cadence before spawning. A busy executor may not poll the
        // task immediately, but elapsed startup time still counts toward the
        // operator-configured interval.
        let first_tick = tokio::time::Instant::now() + interval_duration;
        let periodic_task = tokio::spawn(async move {
            // The first capture belongs one complete interval after startup. An
            // immediate tick would turn "every N milliseconds" into an
            // undocumented snapshot-at-boot policy and add I/O before any state
            // could have changed.
            let mut interval = tokio::time::interval_at(first_tick, interval_duration);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => break,
                    _ = interval.tick() => {
                        if let Err(error) = persist_snapshot(&periodic_store, &periodic_location).await {
                            error!(
                                snapshot_path = %periodic_location.display(),
                                error = %error,
                                "failed to persist periodic engine snapshot"
                            );
                        }
                    }
                }
            }
        });
        Self {
            store,
            location,
            periodic_task: Some(periodic_task),
        }
    }
}

/// Cloneable raw-protobuf endpoint for an embedded [`Engine`].
///
/// This is intentionally SDK-neutral: its request and response are the same wire
/// fields carried by `temporalio-client::callback_based::GrpcRequest` and
/// `GrpcSuccessResponse`, but the core endpoint does not expose the SDK's tonic
/// generation in its public types.
#[derive(Clone, Debug)]
pub struct TemporalEndpoint {
    service: InProcessGrpcService,
    shutdown: CancellationToken,
}

impl Engine {
    /// Start a zero-listener engine with the default in-memory configuration.
    pub async fn start() -> Result<Self> {
        Self::start_with_config(TokeiraConfig::default()).await
    }

    /// Alias for [`Self::start`] that makes the operating mode explicit at call sites.
    pub async fn embedded() -> Result<Self> {
        Self::start().await
    }

    /// Start a zero-listener engine with an explicit validated configuration.
    ///
    /// Storage is always forced to in-memory: durable external storage and cluster
    /// placement are daemon concerns, while this facade guarantees process-local
    /// ownership and no bound sockets.
    pub async fn start_with_config(config: TokeiraConfig) -> Result<Self> {
        let config = embedded_config(config)?;
        let snapshot_config = config.policy.snapshot.clone();
        let (store, restored) = restore_snapshot_store(&config).await?;
        let stack = build_embedded(Arc::new(config), store.clone(), restored).await?;
        let snapshot_policy = snapshot_config.map(|config| {
            EngineSnapshotPolicy::start(store, config, stack.background_cancel.clone())
        });
        stack.engine_tasks.close_registration();
        let shutdown_coordinator = EmbeddedShutdownCoordinator {
            service: stack.service.clone(),
            runtime_tasks: stack.runtime_tasks,
            engine_tasks: stack.engine_tasks,
            shard_cleanup: stack.shard_cleanup,
            director: None,
            ownership: None,
        };
        let engine = Self {
            endpoint: TemporalEndpoint {
                service: stack.service,
                shutdown: stack.background_cancel.clone(),
            },
            background_cancel: stack.background_cancel,
            log_broadcast: stack.log_broadcast,
            snapshot_policy,
            recovery_task: stack.recovery_task,
            startup_report: EngineStartupReport::in_memory(),
            shutdown_coordinator: Some(shutdown_coordinator),
        };
        record_embedded_startup(&engine.startup_report);
        Ok(engine)
    }

    /// Start a zero-listener engine from an explicit embedded storage decision.
    ///
    /// The legacy [`Self::start`] and [`Self::start_with_config`] methods remain
    /// in-memory only. This is the sole engine boundary that may create or
    /// recover managed Aurora DSQL infrastructure.
    pub async fn start_with_embedded_config(
        config: EmbeddedEngineConfig,
    ) -> Result<Self, EmbeddedEngineStartError> {
        if !matches!(config.storage, EmbeddedStorageConfig::InMemory)
            && config.server.policy.snapshot.is_some()
        {
            return Err(EmbeddedEngineStartError::Phase {
                phase: EmbeddedStartupPhase::Configuration,
            });
        }
        config.validate()?;
        if matches!(config.storage, EmbeddedStorageConfig::InMemory) {
            return Self::start_with_config(config.server).await.map_err(|_| {
                EmbeddedEngineStartError::Phase {
                    phase: EmbeddedStartupPhase::RuntimeRestore,
                }
            });
        }
        start_embedded_dsql(config).await
    }

    /// Return the complete redacted report for the startup that produced this engine.
    pub fn startup_report(&self) -> &EngineStartupReport {
        &self.startup_report
    }

    /// Return a cloneable raw-protobuf endpoint for this engine.
    pub fn endpoint(&self) -> TemporalEndpoint {
        self.endpoint.clone()
    }

    /// Subscribe to the same narrow RPC-event stream exposed by [`TokeiradHandle`].
    pub fn log_sink(&self) -> broadcast::Receiver<LogEvent> {
        self.log_broadcast.subscribe()
    }

    /// Adapt this engine to `ConnectionOptions::service_override`.
    ///
    /// DNS load balancing must be disabled on the SDK connection because the
    /// override performs no name resolution or network I/O.
    #[cfg(feature = "temporalio-client")]
    pub fn service_override(&self) -> temporalio_client::callback_based::CallbackBasedGrpcService {
        self.endpoint.service_override()
    }

    /// Stop accepting calls, cancel background work, and persist the final snapshot.
    ///
    /// When snapshot policy is disabled this retains the previous cancellation-only
    /// behaviour. A configured final snapshot failure is returned to the caller: a
    /// graceful shutdown must not silently claim durability it did not achieve.
    pub async fn shutdown(mut self) -> Result<(), EmbeddedEngineShutdownError> {
        let deadline = Instant::now() + StdDuration::from_secs(30);
        let mut failures = Vec::new();
        if let Some(mut coordinator) = self.shutdown_coordinator.take() {
            coordinator.begin_shutdown();
            coordinator.shutdown(deadline, &mut failures).await;
        } else {
            self.background_cancel.cancel();
        }
        if let Some(task) = self.recovery_task.take()
            && task.await.is_err()
        {
            failures.push(EmbeddedShutdownFailure::ShardRelease);
        }
        if let Some(policy) = self.snapshot_policy.take()
            && finish_snapshot_policy(policy).await.is_err()
        {
            failures.push(EmbeddedShutdownFailure::Snapshot);
        }
        record_embedded_shutdown(&self.startup_report, &failures);
        if failures.is_empty() {
            Ok(())
        } else {
            Err(EmbeddedEngineShutdownError { failures })
        }
    }
}

impl EmbeddedShutdownCoordinator {
    fn begin_shutdown(&self) {
        self.service.begin_shutdown();
        self.engine_tasks.begin_shutdown();
        self.runtime_tasks.begin_shutdown();
    }

    async fn shutdown(&mut self, deadline: Instant, failures: &mut Vec<EmbeddedShutdownFailure>) {
        if self.service.drain(deadline).await.is_err() {
            failures.push(EmbeddedShutdownFailure::RpcDrain);
        }
        if self.engine_tasks.wait(deadline).await.is_err() {
            failures.push(EmbeddedShutdownFailure::RuntimeTasks);
        }
        if self.runtime_tasks.wait(deadline).await.is_err() {
            failures.push(EmbeddedShutdownFailure::RuntimeTasks);
        }
        if let Some(cleanup) = self.shard_cleanup.take()
            && cleanup(deadline).await.is_err()
        {
            failures.push(EmbeddedShutdownFailure::ShardRelease);
        }
        // RPC admission is already closed and both task groups have joined, so
        // shard release is the last operation allowed through ordinary owner
        // admission. Closing earlier makes that conditional DSQL release unable
        // to acquire its bounded Control connection and also prevents already
        // admitted RPCs from draining gracefully.
        if let Some(ownership) = &self.ownership {
            ownership.gate.begin_closing();
        }
        if let (Some(ownership), Some(director)) = (&self.ownership, &self.director) {
            let now = Instant::now();
            let ownership_drained = now < deadline
                && tokio::time::timeout(
                    deadline.saturating_duration_since(now),
                    ownership.gate.wait_for_drain(),
                )
                .await
                .is_ok();
            if ownership_drained {
                let now = Instant::now();
                let release_result = if now < deadline {
                    tokio::time::timeout(
                        deadline.saturating_duration_since(now),
                        release_embedded_ownership(ownership, director),
                    )
                    .await
                    .map_err(|_| anyhow!("embedded ownership release deadline elapsed"))
                    .and_then(|result| result)
                } else {
                    Err(anyhow!("embedded ownership release deadline elapsed"))
                };
                if release_result.is_err() {
                    failures.push(EmbeddedShutdownFailure::OwnershipRelease);
                }
            } else {
                // Releasing while a checkout still uses the claim would let a
                // clean takeover overlap prior database work. Leave the claim
                // to expire instead and report the incomplete cleanup.
                failures.push(EmbeddedShutdownFailure::OwnershipRelease);
            }
        }
        if let Some(director) = &self.director
            && director.shutdown_with_deadline(deadline).await.is_err()
        {
            failures.push(EmbeddedShutdownFailure::Storage);
        }
    }
}

async fn start_embedded_dsql(
    config: EmbeddedEngineConfig,
) -> Result<Engine, EmbeddedEngineStartError> {
    let deadline = Instant::now() + StdDuration::from_millis(config.startup_timeout_ms);
    let storage_mode = match &config.storage {
        EmbeddedStorageConfig::ManagedDsql(_) => EmbeddedStorageMode::ManagedDsql,
        EmbeddedStorageConfig::ExistingDsql(_) => EmbeddedStorageMode::ExistingDsql,
        EmbeddedStorageConfig::InMemory => unreachable!("in-memory handled by caller"),
    };
    let region = match &config.storage {
        EmbeddedStorageConfig::ManagedDsql(managed) => managed.region.clone(),
        EmbeddedStorageConfig::ExistingDsql(existing) => existing.region.clone(),
        EmbeddedStorageConfig::InMemory => unreachable!("in-memory handled by caller"),
    };
    let control = startup_infallible_phase(
        deadline,
        EmbeddedStartupPhase::ClusterResolution,
        AwsDsqlControlPlane::from_region(region.clone()),
    )
    .await?;
    let lifecycle_deadline = StartupDeadline::at(deadline);
    let resolved = match &config.storage {
        EmbeddedStorageConfig::ManagedDsql(managed) => {
            let lifecycle = ManagedDsqlLifecycle::new(
                control.clone(),
                LocalClusterDescriptorStore::new(&managed.descriptor_path),
                SystemLifecycleEnvironment,
            );
            startup_phase(
                deadline,
                EmbeddedStartupPhase::ClusterResolution,
                lifecycle.create_or_recover(
                    CreateOrRecoverRequest {
                        region: managed.region.clone(),
                        tags: managed.tags.clone(),
                    },
                    lifecycle_deadline,
                ),
            )
            .await?
        }
        EmbeddedStorageConfig::ExistingDsql(existing) => {
            // `resolve_existing` cannot touch the descriptor seam. The inert
            // path is never opened and exists only to satisfy the lifecycle's
            // generic state-store type without adding another public adapter.
            let unused_descriptor = LocalClusterDescriptorStore::new(PathBuf::new());
            let lifecycle = ManagedDsqlLifecycle::new(
                control.clone(),
                unused_descriptor,
                SystemLifecycleEnvironment,
            );
            let identity = CanonicalClusterIdentity::new(
                &existing.region,
                &existing.cluster_id,
                &existing.cluster_arn,
            )
            .map_err(|_| EmbeddedEngineStartError::Phase {
                phase: EmbeddedStartupPhase::ClusterResolution,
            })?;
            startup_phase(
                deadline,
                EmbeddedStartupPhase::ClusterResolution,
                lifecycle.resolve_existing(identity, lifecycle_deadline),
            )
            .await?
        }
        EmbeddedStorageConfig::InMemory => unreachable!("in-memory handled by caller"),
    };
    let readiness = refresh_cluster_until_storage_handoff(
        &config.storage,
        control.clone(),
        resolved,
        lifecycle_deadline,
        deadline,
    )
    .await?;
    let cluster = match &readiness {
        Readiness::Active(usable) => usable.resolved().clone(),
        Readiness::WakeRequired(cluster) => cluster.clone(),
    };

    let migration_policy = config
        .effective_migration_policy()
        .expect("durable embedded mode always has a migration policy");
    let mut server = config.server;
    server.infrastructure.storage = ConfigStorageKind::Dsql;
    server.infrastructure.placement.controller_endpoint = None;
    server.infrastructure.dsql.endpoint = Some(cluster.endpoint.clone());
    server.infrastructure.dsql.region = Some(cluster.identity.region.clone());
    server
        .validate()
        .map_err(|_| EmbeddedEngineStartError::Phase {
            phase: EmbeddedStartupPhase::Configuration,
        })?;

    let limits = match &config.storage {
        EmbeddedStorageConfig::ManagedDsql(managed) => &managed.limits,
        EmbeddedStorageConfig::ExistingDsql(existing) => &existing.limits,
        EmbeddedStorageConfig::InMemory => unreachable!("in-memory handled by caller"),
    };
    let mut pool_config = EmbeddedDsqlPoolConfig::with_limits(
        limits.max_connections,
        limits.concurrent_connection_creations,
        limits.connection_rate_per_second,
        limits.connection_burst,
    );
    pool_config.shard_count = server.infrastructure.placement.shard_count;
    pool_config.projection_partition_count = server.infrastructure.placement.partition_count;
    let auth = DsqlAuthConfig {
        endpoint: cluster.endpoint.clone(),
        region: Some(cluster.identity.region.clone()),
        admin_role_arn: server.infrastructure.dsql.admin_role_arn.clone(),
        runtime_role_arn: server.infrastructure.dsql.runtime_role_arn.clone(),
        readonly_role_arn: server.infrastructure.dsql.readonly_role_arn.clone(),
    };
    let dsql_store = startup_phase(
        deadline,
        EmbeddedStartupPhase::ConnectionWarmup,
        DsqlStore::connect_embedded(auth, pool_config, WarmupDeadline::new(deadline)),
    )
    .await?;
    let director = dsql_store.connection_director_arc();

    let cluster = if matches!(readiness, Readiness::WakeRequired(_)) {
        match refresh_cluster_after_wake(
            &config.storage,
            control,
            cluster,
            lifecycle_deadline,
            deadline,
        )
        .await
        {
            Ok(cluster) => cluster,
            Err(error) => {
                let _ = director.shutdown_with_deadline(deadline).await;
                return Err(error);
            }
        }
    } else {
        cluster
    };

    let schema = match startup_phase(
        deadline,
        EmbeddedStartupPhase::Schema,
        apply_embedded_schema(
            director.clone(),
            &cluster,
            schema_migration_policy(migration_policy),
            deadline,
        ),
    )
    .await
    {
        Ok(report) => report,
        Err(error) => {
            let _ = director.shutdown_with_deadline(deadline).await;
            return Err(error);
        }
    };

    let (ownership, ownership_report) = match startup_phase(
        deadline,
        EmbeddedStartupPhase::Ownership,
        acquire_embedded_ownership(director.clone(), &cluster, deadline),
    )
    .await
    {
        Ok(ownership) => ownership,
        Err(error) => {
            let _ = director.shutdown_with_deadline(deadline).await;
            return Err(error);
        }
    };
    if director
        .install_ownership_gate(ownership.gate.clone())
        .is_err()
    {
        rollback_embedded_dsql(&ownership, &director, deadline).await;
        return Err(EmbeddedEngineStartError::Phase {
            phase: EmbeddedStartupPhase::Ownership,
        });
    }

    let stack = match startup_phase(
        deadline,
        EmbeddedStartupPhase::RuntimeRestore,
        build_dsql_stack(
            StackTransport::Embedded,
            Arc::new(server),
            dsql_store,
            cluster.endpoint.clone(),
        ),
    )
    .await
    {
        Ok(ConstructedStack::Embedded(stack)) => stack,
        Ok(ConstructedStack::Network(_)) => {
            rollback_embedded_dsql(&ownership, &director, deadline).await;
            return Err(EmbeddedEngineStartError::Phase {
                phase: EmbeddedStartupPhase::RuntimeRestore,
            });
        }
        Err(error) => {
            rollback_embedded_dsql(&ownership, &director, deadline).await;
            return Err(error);
        }
    };

    spawn_ownership_renewal(
        &stack.engine_tasks,
        director.clone(),
        ownership.clone(),
        stack.service.clone(),
    );
    stack.engine_tasks.close_registration();
    let report = EngineStartupReport {
        storage_mode,
        cluster: Some(ClusterStartupReport {
            region: cluster.identity.region.clone(),
            cluster_id: cluster.identity.cluster_id.clone(),
            cluster_arn: cluster.identity.cluster_arn.clone(),
            endpoint: cluster.endpoint.clone(),
            action: cluster.action,
        }),
        schema: Some(schema),
        ownership: Some(ownership_report),
    };
    let coordinator = EmbeddedShutdownCoordinator {
        service: stack.service.clone(),
        runtime_tasks: stack.runtime_tasks,
        engine_tasks: stack.engine_tasks,
        shard_cleanup: stack.shard_cleanup,
        director: Some(director),
        ownership: Some(ownership),
    };
    let engine = Engine {
        endpoint: TemporalEndpoint {
            service: stack.service,
            shutdown: stack.background_cancel.clone(),
        },
        background_cancel: stack.background_cancel,
        log_broadcast: stack.log_broadcast,
        snapshot_policy: None,
        recovery_task: stack.recovery_task,
        startup_report: report,
        shutdown_coordinator: Some(coordinator),
    };
    record_embedded_startup(&engine.startup_report);
    Ok(engine)
}

fn storage_mode_label(mode: EmbeddedStorageMode) -> EmbeddedStorageModeLabel {
    match mode {
        EmbeddedStorageMode::InMemory => EmbeddedStorageModeLabel::InMemory,
        EmbeddedStorageMode::ManagedDsql => EmbeddedStorageModeLabel::ManagedDsql,
        EmbeddedStorageMode::ExistingDsql => EmbeddedStorageModeLabel::ExistingDsql,
    }
}

fn schema_outcome_label(report: &EngineStartupReport) -> SchemaOutcomeLabel {
    match report.schema.as_ref().map(|schema| schema.outcome) {
        None => SchemaOutcomeLabel::NotApplicable,
        Some(SchemaStartupOutcome::Compatible) => SchemaOutcomeLabel::Compatible,
        Some(SchemaStartupOutcome::Initialized) => SchemaOutcomeLabel::Initialized,
        Some(SchemaStartupOutcome::Migrated) => SchemaOutcomeLabel::Migrated,
        Some(SchemaStartupOutcome::MetadataBackfilled) => SchemaOutcomeLabel::MetadataBackfilled,
    }
}

fn ownership_outcome_label(report: &EngineStartupReport) -> OwnershipOutcomeLabel {
    match report.ownership.map(|ownership| ownership.outcome) {
        None => OwnershipOutcomeLabel::NotApplicable,
        Some(ControlLeaseAcquireOutcome::Clean) => OwnershipOutcomeLabel::AcquiredClean,
        Some(ControlLeaseAcquireOutcome::ExpiredTakeover) => OwnershipOutcomeLabel::AcquiredExpired,
    }
}

/// Emit only bounded lifecycle dimensions through the host's current recorder.
///
/// This is deliberately downstream of successful construction. The metrics
/// facade is observational: it does not install a recorder, hold an exporter,
/// bind a listener, or participate in the startup decision.
fn record_embedded_startup(report: &EngineStartupReport) {
    record_embedded_lifecycle(
        storage_mode_label(report.storage_mode),
        if report.cluster.is_some() {
            ClusterStatusLabel::Active
        } else {
            ClusterStatusLabel::NotApplicable
        },
        schema_outcome_label(report),
        ownership_outcome_label(report),
        DbClassLabel::Control,
        EmbeddedOperationLabel::Startup,
        ErrorClassLabel::None,
    );
}

fn record_embedded_shutdown(report: &EngineStartupReport, failures: &[EmbeddedShutdownFailure]) {
    let ownership = if report.ownership.is_some() && failures.is_empty() {
        OwnershipOutcomeLabel::Released
    } else {
        ownership_outcome_label(report)
    };
    record_embedded_lifecycle(
        storage_mode_label(report.storage_mode),
        if report.cluster.is_some() {
            ClusterStatusLabel::Active
        } else {
            ClusterStatusLabel::NotApplicable
        },
        schema_outcome_label(report),
        ownership,
        DbClassLabel::Control,
        EmbeddedOperationLabel::Shutdown,
        if failures.is_empty() {
            ErrorClassLabel::None
        } else {
            ErrorClassLabel::Internal
        },
    );
}

async fn refresh_cluster_until_storage_handoff(
    storage: &EmbeddedStorageConfig,
    control: AwsDsqlControlPlane,
    cluster: ResolvedCluster,
    lifecycle_deadline: StartupDeadline,
    deadline: Instant,
) -> Result<Readiness, EmbeddedEngineStartError> {
    match storage {
        EmbeddedStorageConfig::ManagedDsql(managed) => {
            let lifecycle = ManagedDsqlLifecycle::new(
                control,
                LocalClusterDescriptorStore::new(&managed.descriptor_path),
                SystemLifecycleEnvironment,
            );
            startup_phase(
                deadline,
                EmbeddedStartupPhase::ClusterResolution,
                lifecycle.refresh_until_usable(cluster, lifecycle_deadline),
            )
            .await
        }
        EmbeddedStorageConfig::ExistingDsql(_) => {
            let lifecycle = ManagedDsqlLifecycle::new(
                control,
                LocalClusterDescriptorStore::new(PathBuf::new()),
                SystemLifecycleEnvironment,
            );
            startup_phase(
                deadline,
                EmbeddedStartupPhase::ClusterResolution,
                lifecycle.refresh_until_usable(cluster, lifecycle_deadline),
            )
            .await
        }
        EmbeddedStorageConfig::InMemory => unreachable!("in-memory handled by caller"),
    }
}

async fn refresh_cluster_after_wake(
    storage: &EmbeddedStorageConfig,
    control: AwsDsqlControlPlane,
    cluster: ResolvedCluster,
    lifecycle_deadline: StartupDeadline,
    deadline: Instant,
) -> Result<ResolvedCluster, EmbeddedEngineStartError> {
    let usable = match storage {
        EmbeddedStorageConfig::ManagedDsql(managed) => {
            let lifecycle = ManagedDsqlLifecycle::new(
                control,
                LocalClusterDescriptorStore::new(&managed.descriptor_path),
                SystemLifecycleEnvironment,
            );
            startup_phase(
                deadline,
                EmbeddedStartupPhase::ConnectionWarmup,
                lifecycle.refresh_after_wake(cluster, lifecycle_deadline),
            )
            .await?
        }
        EmbeddedStorageConfig::ExistingDsql(_) => {
            let lifecycle = ManagedDsqlLifecycle::new(
                control,
                LocalClusterDescriptorStore::new(PathBuf::new()),
                SystemLifecycleEnvironment,
            );
            startup_phase(
                deadline,
                EmbeddedStartupPhase::ConnectionWarmup,
                lifecycle.refresh_after_wake(cluster, lifecycle_deadline),
            )
            .await?
        }
        EmbeddedStorageConfig::InMemory => unreachable!("in-memory handled by caller"),
    };
    Ok(usable.into_resolved())
}

fn schema_migration_policy(policy: DsqlMigrationPolicy) -> SchemaMigrationPolicy {
    match policy {
        DsqlMigrationPolicy::Automatic => SchemaMigrationPolicy::Automatic,
        DsqlMigrationPolicy::ValidateOnly => SchemaMigrationPolicy::ValidateOnly,
    }
}

async fn apply_embedded_schema(
    director: Arc<DsqlConnectionDirector>,
    cluster: &ResolvedCluster,
    policy: SchemaMigrationPolicy,
    deadline: Instant,
) -> Result<SchemaStartupReport> {
    let runner = MigrationRunner::embedded();
    let contract = MigrationRunner::compatibility_contract();
    let leases = ConnectionControlLeaseRepository::new();
    let mut permit = director.acquire(DbClass::Control).await?;
    let decision = runner
        .assess_connection(permit.connection()?, &contract, policy)
        .await?;
    let outcome = match &decision {
        SchemaDecision::Compatible {
            legacy_backfill: false,
            ..
        } => SchemaStartupOutcome::Compatible,
        SchemaDecision::Compatible {
            legacy_backfill: true,
            ..
        } => SchemaStartupOutcome::MetadataBackfilled,
        SchemaDecision::Initialize { .. } => SchemaStartupOutcome::Initialized,
        SchemaDecision::Migrate { .. } => SchemaStartupOutcome::Migrated,
        SchemaDecision::MigrationRequired { current, target } => {
            return Err(anyhow!(
                "schema migration required from V{current} to V{target}"
            ));
        }
        SchemaDecision::Reject(_) => return Err(anyhow!("schema compatibility rejected")),
    };
    if !matches!(outcome, SchemaStartupOutcome::Compatible) {
        runner
            .bootstrap_migration_coordination(permit.connection()?, &decision)
            .await?;
        leases.bootstrap(permit.connection()?).await?;
        let mut migration_guard = leases
            .acquire(
                permit.connection()?,
                &ControlLeaseAcquireRequest {
                    claim_name: "schema-migration".to_owned(),
                    cluster: control_lease_identity(cluster),
                    owner_id: format!("schema-{}", IncarnationId::new()),
                    lease_duration: SCHEMA_MIGRATION_LEASE_DURATION,
                    admission_margin: SCHEMA_MIGRATION_ADMISSION_MARGIN,
                    acquire_deadline: deadline,
                },
            )
            .await?;
        let migration_gate = OwnershipAdmissionGate::for_guard(&migration_guard);
        let apply_result = runner
            .apply_decision(
                permit.connection()?,
                &decision,
                &leases,
                &mut migration_guard,
                &migration_gate,
            )
            .await;
        let release_result = leases
            .release(permit.connection()?, &migration_guard, &migration_gate)
            .await;
        apply_result?;
        release_result?;
    }
    let final_decision = runner
        .assess_connection(
            permit.connection()?,
            &contract,
            SchemaMigrationPolicy::ValidateOnly,
        )
        .await?;
    let observed_version = match final_decision {
        SchemaDecision::Compatible { current, .. } => current,
        _ => return Err(anyhow!("schema did not converge to a readable version")),
    };
    Ok(SchemaStartupReport {
        observed_version,
        minimum_supported_version: contract.minimum_supported_version,
        target_version: contract.target_version,
        maximum_readable_version: contract.maximum_readable_version,
        migration_set_digest: contract.migration_set_digest,
        outcome,
    })
}

async fn acquire_embedded_ownership(
    director: Arc<DsqlConnectionDirector>,
    cluster: &ResolvedCluster,
    deadline: Instant,
) -> Result<(EmbeddedOwnership, OwnershipStartupReport)> {
    let repository = ConnectionControlLeaseRepository::new();
    let mut permit = director.acquire(DbClass::Control).await?;
    repository.bootstrap(permit.connection()?).await?;
    let guard = repository
        .acquire(
            permit.connection()?,
            &ControlLeaseAcquireRequest {
                claim_name: "embedded-owner".to_owned(),
                cluster: control_lease_identity(cluster),
                owner_id: format!("engine-{}", IncarnationId::new()),
                lease_duration: OWNER_LEASE_DURATION,
                admission_margin: OWNER_ADMISSION_MARGIN,
                acquire_deadline: deadline,
            },
        )
        .await?;
    let gate = OwnershipAdmissionGate::for_guard(&guard);
    if let Some(quiescence_deadline) = guard.quiescence_deadline() {
        let now = Instant::now();
        if now >= deadline || quiescence_deadline > deadline {
            return Err(anyhow!("expired owner quiescence exceeds startup deadline"));
        }
        tokio::time::sleep_until(tokio::time::Instant::from_std(quiescence_deadline)).await;
        gate.finish_quiescence(&guard, Instant::now())?;
    }
    let report = OwnershipStartupReport {
        outcome: guard.outcome(),
        fence_token: guard.fence_token(),
    };
    Ok((
        EmbeddedOwnership {
            repository,
            guard: Arc::new(tokio::sync::Mutex::new(Some(guard))),
            gate,
        },
        report,
    ))
}

fn spawn_ownership_renewal(
    tasks: &RuntimeShutdownHandle,
    director: Arc<DsqlConnectionDirector>,
    ownership: EmbeddedOwnership,
    service: InProcessGrpcService,
) {
    let cancel = tasks.cancellation_token();
    let _renewal = tasks.spawn(async move {
        let interval_duration =
            StdDuration::try_from(OWNER_RENEW_INTERVAL).expect("positive owner renewal interval");
        let mut interval = tokio::time::interval(interval_duration);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await;
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = interval.tick() => {
                    let mut guard_slot = ownership.guard.lock().await;
                    let Some(guard) = guard_slot.as_mut() else {
                        break;
                    };
                    let result = match director.acquire(DbClass::Control).await {
                        Ok(mut permit) => match permit.connection() {
                            Ok(connection) => ownership.repository.renew(
                                    connection,
                                    guard,
                                    OWNER_LEASE_DURATION,
                                    OWNER_ADMISSION_MARGIN,
                                    &ownership.gate,
                                )
                                .await
                                .map_err(anyhow::Error::from),
                            Err(error) => Err(error),
                        },
                        Err(error) => Err(error),
                    };
                    if let Err(error) = result {
                        if matches!(
                            error.downcast_ref::<ControlLeaseError>(),
                            Some(ControlLeaseError::Fenced)
                        ) {
                            ownership.gate.fence();
                        } else {
                            guard.enforce_admission_deadline(Instant::now(), &ownership.gate);
                        }
                    }
                    if ownership.gate.state() != OwnershipAdmissionState::Open {
                        service.begin_shutdown();
                        break;
                    }
                }
            }
        }
    });
}

fn control_lease_identity(cluster: &ResolvedCluster) -> ControlLeaseClusterIdentity {
    ControlLeaseClusterIdentity {
        cluster_id: cluster.identity.cluster_id.clone(),
        cluster_arn: cluster.identity.cluster_arn.clone(),
    }
}

async fn rollback_embedded_dsql(
    ownership: &EmbeddedOwnership,
    director: &Arc<DsqlConnectionDirector>,
    deadline: Instant,
) {
    ownership.gate.begin_closing();
    let now = Instant::now();
    if now < deadline {
        let _ = tokio::time::timeout(
            deadline.saturating_duration_since(now),
            release_embedded_ownership(ownership, director),
        )
        .await;
    }
    let _ = director.shutdown_with_deadline(deadline).await;
}

async fn release_embedded_ownership(
    ownership: &EmbeddedOwnership,
    director: &Arc<DsqlConnectionDirector>,
) -> Result<()> {
    // Ordinary owner admission is closed here. This shutdown-only director
    // seam still consumes the Control budget and one bounded physical slot.
    let mut permit = director.acquire_shutdown_control().await?;
    let mut guard = ownership.guard.lock().await;
    if let Some(guard) = guard.take() {
        ownership
            .repository
            .release(permit.connection()?, &guard, &ownership.gate)
            .await?;
    }
    Ok(())
}

async fn startup_phase<T, E, F>(
    deadline: Instant,
    phase: EmbeddedStartupPhase,
    future: F,
) -> Result<T, EmbeddedEngineStartError>
where
    F: Future<Output = Result<T, E>>,
{
    let now = Instant::now();
    if now >= deadline {
        return Err(EmbeddedEngineStartError::DeadlineExceeded { phase });
    }
    match tokio::time::timeout(deadline.saturating_duration_since(now), future).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(_)) => Err(EmbeddedEngineStartError::Phase { phase }),
        Err(_) => Err(EmbeddedEngineStartError::DeadlineExceeded { phase }),
    }
}

async fn startup_infallible_phase<T, F>(
    deadline: Instant,
    phase: EmbeddedStartupPhase,
    future: F,
) -> Result<T, EmbeddedEngineStartError>
where
    F: Future<Output = T>,
{
    let now = Instant::now();
    if now >= deadline {
        return Err(EmbeddedEngineStartError::DeadlineExceeded { phase });
    }
    tokio::time::timeout(deadline.saturating_duration_since(now), future)
        .await
        .map_err(|_| EmbeddedEngineStartError::DeadlineExceeded { phase })
}

/// Drain a cancelled snapshot policy: await its periodic task, then persist
/// the final consistent cut.
///
/// Shared by the embedded facade and the listener-backed daemon so both
/// serving modes end with the identical durability sequence. The caller must
/// already have cancelled the policy's cancellation token.
async fn finish_snapshot_policy(mut policy: EngineSnapshotPolicy) -> Result<()> {
    if let Some(task) = policy.periodic_task.take() {
        task.await
            .context("engine snapshot task panicked during shutdown")?;
    }
    // Background workers observe the same token. Yield once so
    // cancellation-aware workers that are already runnable can leave their
    // select loops before the final consistent cut and before the caller
    // tears down its Tokio runtime.
    tokio::task::yield_now().await;
    persist_snapshot(&policy.store, &policy.location)
        .await
        .with_context(|| {
            format!(
                "failed to persist final engine snapshot to {}",
                policy.location.display()
            )
        })
}

async fn restore_snapshot_store(config: &TokeiraConfig) -> Result<(InMemoryStore, bool)> {
    let Some(snapshot) = config.policy.snapshot.as_ref() else {
        return Ok((InMemoryStore::default(), false));
    };
    match fs::read(&snapshot.location).await {
        Ok(bytes) => {
            let store = InMemoryStore::from_snapshot(&bytes).with_context(|| {
                format!(
                    "failed to restore engine snapshot from {}",
                    snapshot.location.display()
                )
            })?;
            retire_snapshot_leases(&store).await.with_context(|| {
                format!(
                    "failed to retire prior embedded-engine leases from {}",
                    snapshot.location.display()
                )
            })?;
            info!(
                snapshot_path = %snapshot.location.display(),
                "restored engine snapshot"
            );
            Ok((store, true))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok((InMemoryStore::default(), false))
        }
        Err(error) => Err(error).with_context(|| {
            format!(
                "failed to read engine snapshot from {}",
                snapshot.location.display()
            )
        }),
    }
}

async fn retire_snapshot_leases(store: &InMemoryStore) -> Result<()> {
    for lease in store
        .list_bundle_leases()
        .await
        .context("failed to list engine snapshot leases")?
    {
        let Some(owner) = lease.owner_node_id else {
            continue;
        };
        match store
            .relinquish_bundle(lease.bundle_id, owner, lease.epoch)
            .await
            .context("failed to relinquish an engine snapshot lease")?
        {
            LeaseOutcome::Acquired { .. } => {}
            LeaseOutcome::Rejected {
                current_owner,
                current_epoch,
            } => {
                return Err(anyhow!(
                    "engine snapshot lease retirement was fenced by owner {current_owner} at epoch {}",
                    current_epoch.0
                ));
            }
            LeaseOutcome::Renewed { epoch } => {
                return Err(anyhow!(
                    "engine snapshot lease retirement unexpectedly renewed epoch {}",
                    epoch.0
                ));
            }
        }
    }
    Ok(())
}

async fn persist_snapshot(store: &InMemoryStore, location: &Path) -> Result<()> {
    let bytes = store
        .snapshot()
        .await
        .context("failed to capture engine snapshot")?;
    write_snapshot_atomically(location, &bytes).await
}

async fn write_snapshot_atomically(location: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = location
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).await.with_context(|| {
            format!(
                "failed to create engine snapshot directory {}",
                parent.display()
            )
        })?;
    }

    // A same-directory temporary keeps rename atomic on the target filesystem.
    // The process/sequence suffix prevents two embedded engines in one test
    // process from sharing a staging file even if they target the same snapshot.
    let sequence = SNAPSHOT_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let mut temporary_name = location.as_os_str().to_os_string();
    temporary_name.push(format!(".tmp-{}-{sequence}", std::process::id()));
    let temporary = PathBuf::from(temporary_name);

    let write_result: Result<()> = async {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .await
            .with_context(|| {
                format!(
                    "failed to create temporary engine snapshot {}",
                    temporary.display()
                )
            })?;
        file.write_all(bytes).await.with_context(|| {
            format!(
                "failed to write temporary engine snapshot {}",
                temporary.display()
            )
        })?;
        file.sync_all().await.with_context(|| {
            format!(
                "failed to sync temporary engine snapshot {}",
                temporary.display()
            )
        })?;
        drop(file);
        fs::rename(&temporary, location)
            .await
            .with_context(|| format!("failed to replace engine snapshot {}", location.display()))?;
        Ok(())
    }
    .await;

    if write_result.is_err() {
        // Cleanup is best-effort and intentionally targets only the unique
        // staging file created above; the prior complete snapshot stays intact.
        let _ = fs::remove_file(&temporary).await;
    }
    write_result
}

fn embedded_config(mut config: TokeiraConfig) -> Result<TokeiraConfig> {
    config.infrastructure.storage = ConfigStorageKind::InMemory;
    // An embedded engine owns its only runtime node. Retaining a controller
    // endpoint would spawn membership and routing clients even though there is
    // no advertised listener for another node to reach.
    config.infrastructure.placement.controller_endpoint = None;
    config
        .validate()
        .context("invalid embedded engine config")?;
    Ok(config)
}

impl Drop for Engine {
    fn drop(&mut self) {
        if let Some(coordinator) = &self.shutdown_coordinator {
            coordinator.begin_shutdown();
        }
        self.background_cancel.cancel();
    }
}

impl TemporalEndpoint {
    /// Dispatch one raw protobuf request through the Temporal service router.
    pub async fn call(
        &self,
        request: InProcessGrpcRequest,
    ) -> Result<InProcessGrpcResponse, tonic::Status> {
        tokio::select! {
            biased;
            _ = self.shutdown.cancelled() => {
                Err(tonic::Status::unavailable("embedded Tokeira engine is shut down"))
            }
            response = self.service.call(request) => response,
        }
    }

    /// Whether the engine that owns this endpoint has begun shutdown.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.is_cancelled()
    }

    /// Build the Temporal Rust SDK callback service for this endpoint.
    #[cfg(feature = "temporalio-client")]
    pub fn service_override(&self) -> temporalio_client::callback_based::CallbackBasedGrpcService {
        let endpoint = self.clone();
        temporalio_client::callback_based::CallbackBasedGrpcService {
            callback: Arc::new(move |request| {
                let endpoint = endpoint.clone();
                Box::pin(async move {
                    endpoint
                        .call(InProcessGrpcRequest {
                            service: request.service,
                            rpc: request.rpc,
                            headers: request.headers,
                            proto: request.proto,
                        })
                        .await
                        .map(
                            |response| temporalio_client::callback_based::GrpcSuccessResponse {
                                headers: response.headers,
                                proto: response.proto,
                            },
                        )
                        .map_err(to_sdk_status)
                })
            }),
        }
    }
}

#[cfg(feature = "temporalio-client")]
fn to_sdk_status(status: tonic::Status) -> tonic_sdk::Status {
    let code = match status.code() {
        tonic::Code::Ok => tonic_sdk::Code::Ok,
        tonic::Code::Cancelled => tonic_sdk::Code::Cancelled,
        tonic::Code::Unknown => tonic_sdk::Code::Unknown,
        tonic::Code::InvalidArgument => tonic_sdk::Code::InvalidArgument,
        tonic::Code::DeadlineExceeded => tonic_sdk::Code::DeadlineExceeded,
        tonic::Code::NotFound => tonic_sdk::Code::NotFound,
        tonic::Code::AlreadyExists => tonic_sdk::Code::AlreadyExists,
        tonic::Code::PermissionDenied => tonic_sdk::Code::PermissionDenied,
        tonic::Code::ResourceExhausted => tonic_sdk::Code::ResourceExhausted,
        tonic::Code::FailedPrecondition => tonic_sdk::Code::FailedPrecondition,
        tonic::Code::Aborted => tonic_sdk::Code::Aborted,
        tonic::Code::OutOfRange => tonic_sdk::Code::OutOfRange,
        tonic::Code::Unimplemented => tonic_sdk::Code::Unimplemented,
        tonic::Code::Internal => tonic_sdk::Code::Internal,
        tonic::Code::Unavailable => tonic_sdk::Code::Unavailable,
        tonic::Code::DataLoss => tonic_sdk::Code::DataLoss,
        tonic::Code::Unauthenticated => tonic_sdk::Code::Unauthenticated,
    };
    let legacy_metadata = status.metadata().clone().into_headers();
    let mut metadata_headers = http::HeaderMap::new();
    for (name, value) in &legacy_metadata {
        let Ok(name) = http::header::HeaderName::from_bytes(name.as_str().as_bytes()) else {
            continue;
        };
        let Ok(value) = http::header::HeaderValue::from_bytes(value.as_bytes()) else {
            continue;
        };
        metadata_headers.append(name, value);
    }
    tonic_sdk::Status::with_details_and_metadata(
        code,
        status.message().to_owned(),
        Bytes::copy_from_slice(status.details()),
        tonic_sdk::metadata::MetadataMap::from_headers(metadata_headers),
    )
}

const EMBEDDED_NEXUS_CALLBACK_BASE: &str = "http://tokeira-engine.invalid";

/// Completion client that keeps `temporal://system` callbacks inside the engine
/// while retaining the normal HTTP client for explicitly external callback URLs.
struct InProcessNexusCompletionClient {
    runtime: RwLock<Option<Weak<dyn WorkflowRuntimeApi>>>,
    http: HttpNexusCompletionClient,
}

impl std::fmt::Debug for InProcessNexusCompletionClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InProcessNexusCompletionClient")
            .finish_non_exhaustive()
    }
}

impl InProcessNexusCompletionClient {
    fn new() -> Self {
        Self {
            runtime: RwLock::new(None),
            http: HttpNexusCompletionClient::new(),
        }
    }

    fn attach(&self, runtime: &Arc<dyn WorkflowRuntimeApi>) -> Result<()> {
        let mut slot = self
            .runtime
            .write()
            .map_err(|_| anyhow!("embedded Nexus completion runtime lock poisoned"))?;
        *slot = Some(Arc::downgrade(runtime));
        Ok(())
    }
}

#[async_trait]
impl NexusCompletionClient for InProcessNexusCompletionClient {
    async fn complete_operation(
        &self,
        url: &str,
        token: &str,
        operation_token: &str,
        completion: NexusCompletion,
        links: &[Link],
    ) -> Result<CompletionDeliveryOutcome> {
        if url != system_callback_post_url(EMBEDDED_NEXUS_CALLBACK_BASE) {
            return self
                .http
                .complete_operation(url, token, operation_token, completion, links)
                .await;
        }

        let runtime = self
            .runtime
            .read()
            .ok()
            .and_then(|slot| slot.as_ref().and_then(Weak::upgrade));
        let Some(runtime) = runtime else {
            return Ok(CompletionDeliveryOutcome::RetryableError {
                detail: "embedded Nexus completion runtime is unavailable".to_owned(),
            });
        };

        let state = completion.operation_state();
        let (body, content_type) = match completion {
            NexusCompletion::Succeeded(payloads) => nexus_payload_to_body(&payloads),
            NexusCompletion::Failed(body) | NexusCompletion::Canceled(body) => {
                (body, Some("application/json".to_owned()))
            }
        };
        let response = handle_nexus_callback(
            runtime.as_ref(),
            Some(token),
            Some(operation_token),
            Some(state),
            content_type.as_deref(),
            &body,
        )
        .await;
        let detail = response
            .body
            .map(|body| String::from_utf8_lossy(&body).into_owned())
            .unwrap_or_else(|| format!("Nexus callback returned status {}", response.status));
        Ok(match response.status {
            200..=299 => CompletionDeliveryOutcome::Delivered,
            400 | 401 | 403 | 404 | 409 | 501 => {
                CompletionDeliveryOutcome::NonRetryableError { detail }
            }
            _ => CompletionDeliveryOutcome::RetryableError { detail },
        })
    }
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
    /// In-memory snapshot policy, when configured. The final consistent cut
    /// runs in [`Self::shutdown`]; a plain drop cancels only (an async write
    /// cannot run in `Drop`), so callers needing durability call `shutdown`.
    snapshot_policy: Option<EngineSnapshotPolicy>,
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
        Self::start_in_memory_with_config(addr, TokeiraConfig::default()).await
    }

    /// Start an in-memory `tokeirad` with an explicit validated configuration.
    ///
    /// This is the production bootstrap path with only storage selection and
    /// listener addresses fixed for process-local integration tests. In
    /// particular, configured authentication, authorization, and Worker-scope
    /// policy are constructed exactly as they are by the binary.
    pub async fn start_in_memory_with_config(
        addr: SocketAddr,
        mut config: TokeiraConfig,
    ) -> Result<Self> {
        config.infrastructure.storage = ConfigStorageKind::InMemory;
        // Bind the inbound Nexus completion listener on an ephemeral loopback port so
        // parallel in-memory servers (tests, multi-node harnesses) never collide on the
        // fixed default port; the runtime resolves `temporal://system` to the bound port.
        config.policy.nexus_completion.http_addr = "127.0.0.1:0".to_string();
        config.policy.nexus_completion.system_callback_url = "http://127.0.0.1:0".to_string();
        config
            .validate()
            .context("invalid in-memory server config")?;
        let effective_config = Arc::new(config);
        let (
            (server_task, bound_addr, shutdown_tx, background_cancel, log_broadcast, _recorder),
            snapshot_policy,
        ) = build_and_serve(addr, effective_config).await?;
        Ok(Self {
            bound_addr,
            shutdown_tx: Some(shutdown_tx),
            server_task: Some(server_task),
            background_cancel,
            log_broadcast,
            snapshot_policy,
        })
    }

    /// Parse TOML and start the in-memory integration facade.
    ///
    /// Keeping parsing inside this crate lets cross-crate integration tests
    /// exercise the public configuration surface without depending directly on
    /// the configuration implementation crate.
    pub async fn start_in_memory_with_toml(addr: SocketAddr, config: &str) -> Result<Self> {
        let config = toml::from_str(config).context("parse in-memory server config")?;
        Self::start_in_memory_with_config(addr, config).await
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

    /// Signal the server to shut down, wait for the task to exit, and persist
    /// the final snapshot when a snapshot policy is configured.
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
        match self.snapshot_policy.take() {
            Some(policy) => finish_snapshot_policy(policy).await,
            None => Ok(()),
        }
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
/// serves the gRPC stack on `infrastructure.network.grpc_addr` until ctrl-C
/// or SIGTERM, then drains gracefully: in-flight requests finish, background
/// work is cancelled, and (for in-memory storage with a snapshot policy) the
/// final consistent cut is persisted before exit.
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
        // A TOML comment so the dump stays parseable while naming the winner.
        println!("# config source: {config_source}");
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
    info!(config_source = %config_source, "loaded tokeirad configuration");

    let (
        (
            mut server_task,
            bound_addr,
            shutdown_tx,
            background_cancel,
            _log_broadcast,
            wire_recorder,
        ),
        snapshot_policy,
    ) = build_and_serve(addr, effective_config).await?;
    readiness.mark_started();
    info!("tokeirad gRPC server listening on {bound_addr}");

    // Daemon-only signal handling. The embedded facade (`Engine`) deliberately
    // installs no handlers and no process observability: an embedded host owns
    // its process lifecycle, and a library that grabs SIGTERM would race the
    // host's own shutdown sequence. Only this served entrypoint reacts to
    // ctrl-C/SIGTERM, and the reaction is the graceful drain: stop accepting
    // connections via the listener's shutdown oneshot, await in-flight
    // requests, cancel background work, then take the final snapshot below.
    let serve_result = {
        let drain_signal = async {
            #[cfg(unix)]
            {
                let mut sigterm =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                        .context("failed to install the SIGTERM handler")?;
                tokio::select! {
                    result = tokio::signal::ctrl_c() => result
                        .context("failed to listen for ctrl-c")
                        .map(|()| "SIGINT"),
                    _ = sigterm.recv() => Ok("SIGTERM"),
                }
            }
            #[cfg(not(unix))]
            {
                tokio::signal::ctrl_c()
                    .await
                    .context("failed to listen for ctrl-c")
                    .map(|()| "ctrl-c")
            }
        };
        tokio::select! {
            result = &mut server_task => result
                .context("tokeirad server task panicked")?
                .context("tokeirad server task returned an error"),
            signal = drain_signal => {
                match signal {
                    Ok(signal) => info!(signal, "shutdown signal received; draining tokeirad"),
                    Err(error) => {
                        // Signal delivery is broken; a blunt stop is the only
                        // honest option left, but say why.
                        tracing::warn!(?error, "signal handling failed; shutting down");
                    }
                }
                let _ = shutdown_tx.send(());
                (&mut server_task)
                    .await
                    .context("tokeirad server task panicked")?
                    .context("tokeirad server task returned an error")
            }
        }
    };
    background_cancel.cancel();
    // The final consistent cut runs after the drain regardless of how the
    // server exited, so an error exit still leaves the freshest snapshot. A
    // snapshot failure must not mask the server's own exit status; it is
    // surfaced only after that status is known good.
    let snapshot_result = match snapshot_policy {
        Some(policy) => finish_snapshot_policy(policy).await,
        None => Ok(()),
    };

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
    snapshot_result?;
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
        server_version = tokeira_build_info::SERVER_VERSION,
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
            "server_version": tokeira_build_info::SERVER_VERSION,
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
            format!("server_version: {}", tokeira_build_info::SERVER_VERSION),
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
        "tokeira {}\ngit {}\ntemporal_proto {}\ntemporal_server {}",
        tokeira_build_info::SERVER_VERSION,
        info.tokeira_git_sha,
        info.temporal_proto_version,
        info.temporal_server_compat
    )
}

/// What [`build_and_serve`] hands back to the
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

#[derive(Clone, Copy, Debug)]
enum StackTransport {
    Network(SocketAddr),
    Embedded,
}

#[derive(Debug)]
enum ConstructedStack {
    Network(ServerStack),
    Embedded(EmbeddedStack),
}

struct EmbeddedStack {
    service: InProcessGrpcService,
    background_cancel: CancellationToken,
    log_broadcast: broadcast::Sender<LogEvent>,
    recovery_task: Option<JoinHandle<Result<()>>>,
    runtime_tasks: RuntimeShutdownHandle,
    engine_tasks: RuntimeShutdownHandle,
    shard_cleanup: Option<CleanupAction>,
}

struct StackStartupGuard {
    runtime_tasks: RuntimeShutdownHandle,
    engine_tasks: RuntimeShutdownHandle,
    armed: bool,
}

impl StackStartupGuard {
    fn new(runtime_tasks: RuntimeShutdownHandle, engine_tasks: RuntimeShutdownHandle) -> Self {
        Self {
            runtime_tasks,
            engine_tasks,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for StackStartupGuard {
    fn drop(&mut self) {
        if self.armed {
            // Async cleanup is owned by the caller's pool rollback, but every
            // background loop must first observe cancellation so it cannot
            // retain the partially constructed stack indefinitely.
            self.engine_tasks.begin_shutdown();
            self.runtime_tasks.begin_shutdown();
        }
    }
}

#[derive(Clone, Debug)]
struct SelfAssignedShardLease {
    shard_id: ShardId,
    owner: String,
    epoch: ShardEpoch,
}

impl std::fmt::Debug for EmbeddedStack {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EmbeddedStack")
            .field("service", &self.service)
            .field("background_cancel", &self.background_cancel)
            .field("runtime_tasks", &self.runtime_tasks)
            .field("engine_tasks", &self.engine_tasks)
            .field("has_shard_cleanup", &self.shard_cleanup.is_some())
            .finish_non_exhaustive()
    }
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
/// Default maximum identifier length for standalone-activity ids, mirroring
/// Temporal's `MaxIDLengthLimit` default of `1000` (`common/dynamicconfig` @
/// v1.31.0). Used to validate standalone-activity ids at the edge.
const DEFAULT_MAX_ID_LENGTH: usize = 1000;

async fn build_and_serve(
    addr: SocketAddr,
    effective_config: Arc<TokeiraConfig>,
) -> Result<(ServerStack, Option<EngineSnapshotPolicy>)> {
    let mut snapshot_inputs = None;
    let stack = match effective_config.infrastructure.storage {
        ConfigStorageKind::InMemory => {
            // The listener-backed in-memory server shares the embedded
            // facade's snapshot mechanism exactly: restore at boot, periodic
            // persistence, and a final cut at shutdown all operate on the one
            // in-memory store.
            let (store, restored) = restore_snapshot_store(&effective_config).await?;
            snapshot_inputs = effective_config
                .policy
                .snapshot
                .clone()
                .map(|config| (store.clone(), config));
            build_in_memory_stack(
                StackTransport::Network(addr),
                effective_config,
                store,
                restored,
            )
            .await
        }
        ConfigStorageKind::Dsql => {
            if effective_config.policy.snapshot.is_some() {
                return Err(anyhow!(
                    "policy.snapshot is configured but infrastructure.storage is \"dsql\": \
                     snapshots are the in-memory store's persistence mechanism, and DSQL \
                     storage is already durable — remove policy.snapshot or switch \
                     infrastructure.storage to \"memory\""
                ));
            }
            let auth = dsql_auth_config(&effective_config)?;
            let endpoint = auth.endpoint.clone();
            let (pool_config, ddb_client) = dsql_pool_config(&effective_config, &auth).await?;
            let dsql_store = DsqlStore::connect(auth, pool_config, ddb_client)
                .await
                .context("failed to connect DSQL storage backend")?;
            build_dsql_stack(
                StackTransport::Network(addr),
                effective_config,
                dsql_store,
                endpoint,
            )
            .await
        }
    }?;
    match stack {
        ConstructedStack::Network(stack) => {
            // The policy shares the stack's cancellation token so the periodic
            // task dies with the server's other background work; the final
            // consistent cut stays with the caller's shutdown sequence.
            let snapshot_policy = snapshot_inputs
                .map(|(store, config)| EngineSnapshotPolicy::start(store, config, stack.3.clone()));
            Ok((stack, snapshot_policy))
        }
        ConstructedStack::Embedded(_) => Err(anyhow!(
            "network service construction returned an embedded stack"
        )),
    }
}

async fn build_dsql_stack(
    transport: StackTransport,
    effective_config: Arc<TokeiraConfig>,
    dsql_store: DsqlStore,
    endpoint: String,
) -> Result<ConstructedStack> {
    let (director, run_repository, projection_log, worker_deployment_repository, _migration_runner) =
        dsql_store.into_parts();
    // Every repository and visibility surface shares this exact director. The
    // embedded caller retains another Arc solely for ordered pool shutdown;
    // no second pool or DynamoDB coordinator is constructed here.
    let chasm_node_repo = Arc::new(tokeira_storage::dsql::DsqlChasmNodeRepository::new(
        director.clone(),
    ));
    let task_queue_config_repository: Arc<dyn TaskQueueConfigRepository> = Arc::new(
        tokeira_storage::dsql::DsqlTaskQueueConfigRepository::new(director.clone()),
    );
    let worker_compute_repository = effective_config.policy.worker_compute.enabled.then(|| {
        Arc::new(tokeira_storage::dsql::DsqlWorkerComputeRepository::new(
            director.clone(),
        )) as Arc<dyn WorkerComputeRepository>
    });
    let worker_task_provenance: Arc<dyn WorkerTaskProvenanceStore> = Arc::new(
        tokeira_storage::dsql::DsqlWorkerTaskProvenanceStore::new(director.clone()),
    );
    let visibility_store = DsqlVisibilityStore::new(director);
    let worker_deployment_repository: Arc<dyn WorkerDeploymentRepository> =
        Arc::new(worker_deployment_repository);
    build_service_stack_with_storage(
        transport,
        effective_config,
        Arc::new(run_repository),
        worker_deployment_repository,
        worker_compute_repository,
        task_queue_config_repository,
        worker_task_provenance,
        projection_log,
        visibility_store.clone(),
        {
            let visibility_store = visibility_store.clone();
            move || visibility_store.clone()
        },
        Some(endpoint),
        chasm_node_repo,
        false,
    )
    .await
}

async fn build_embedded(
    effective_config: Arc<TokeiraConfig>,
    store: InMemoryStore,
    recover_self_assigned_shard: bool,
) -> Result<EmbeddedStack> {
    match build_in_memory_stack(
        StackTransport::Embedded,
        effective_config,
        store,
        recover_self_assigned_shard,
    )
    .await?
    {
        ConstructedStack::Embedded(stack) => Ok(stack),
        ConstructedStack::Network(_) => Err(anyhow!(
            "embedded service construction returned a network stack"
        )),
    }
}

async fn build_in_memory_stack(
    transport: StackTransport,
    effective_config: Arc<TokeiraConfig>,
    store: InMemoryStore,
    recover_self_assigned_shard: bool,
) -> Result<ConstructedStack> {
    let visibility_store = InMemoryVisibilityStore::default();
    let worker_deployment_repository: Arc<dyn WorkerDeploymentRepository> = Arc::new(store.clone());
    let worker_task_provenance: Arc<dyn WorkerTaskProvenanceStore> = Arc::new(store.clone());
    let worker_compute_repository = effective_config.policy.worker_compute.enabled.then(|| {
        Arc::new(InMemoryWorkerComputeRepository::default()) as Arc<dyn WorkerComputeRepository>
    });
    build_service_stack_with_storage(
        transport,
        effective_config,
        Arc::new(store.clone()),
        worker_deployment_repository,
        worker_compute_repository,
        Arc::new(store.clone()),
        worker_task_provenance,
        store,
        visibility_store.clone(),
        {
            let visibility_store = visibility_store.clone();
            move || VisibilitySink::new(visibility_store.clone())
        },
        None,
        Arc::new(tokeira_storage::InMemoryChasmNodeStore::new()),
        recover_self_assigned_shard,
    )
    .await
}

/// How often the visibility repair scanner sweeps committed executions to repair any
/// projection lost by the best-effort post-commit write (Req 10.11).
const VISIBILITY_REPAIR_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

/// Authorization evidence is derived from task deadlines, so cleanup needs no
/// operator TTL. One bounded batch per tick prevents expired rows from
/// monopolizing the shared DSQL connection budget.
const WORKER_TASK_PROVENANCE_CLEANUP_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(30);
const WORKER_TASK_PROVENANCE_CLEANUP_BATCH: usize = 256;

fn spawn_worker_task_provenance_cleanup(
    tasks: &RuntimeShutdownHandle,
    store: Arc<dyn WorkerTaskProvenanceStore>,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tasks.spawn(async move {
        let mut interval = tokio::time::interval(WORKER_TASK_PROVENANCE_CLEANUP_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = interval.tick() => {
                    if let Err(error) = store
                        .delete_expired(
                            time::OffsetDateTime::now_utc(),
                            WORKER_TASK_PROVENANCE_CLEANUP_BATCH,
                        )
                        .await
                    {
                        tracing::warn!(
                            error_kind = worker_task_provenance_cleanup_error_kind(&error),
                            "Worker task authorization-evidence cleanup pass failed"
                        );
                    }
                }
            }
        }
    })
}

fn worker_task_provenance_cleanup_error_kind(
    error: &tokeira_storage::WorkerTaskProvenanceError,
) -> &'static str {
    match error {
        tokeira_storage::WorkerTaskProvenanceError::Unavailable { .. } => "unavailable",
        tokeira_storage::WorkerTaskProvenanceError::DigestConflict => "conflict",
        tokeira_storage::WorkerTaskProvenanceError::Corrupt { .. } => "corrupt",
    }
}

/// Spawn the background visibility repair scanner: it reconstructs each committed
/// execution's snapshot from authoritative node state and re-applies it iff-newer, so
/// a committed transition can never permanently lack a projection (Req 10.11). The
/// snapshot rebuild decodes the per-archetype node bytes (only the activity archetype
/// today); other archetypes are skipped. Runs immediately, then on an interval, until
/// `cancel` fires.
fn spawn_visibility_repair(
    tasks: &RuntimeShutdownHandle,
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
    let _repair = tasks.spawn(async move {
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
    tasks: &RuntimeShutdownHandle,
    engine: Arc<tokeira_runtime::chasm::ChasmEngine>,
    evaluator: Arc<dyn tokeira_runtime::chasm::TimeoutEvaluator>,
    cancel: CancellationToken,
) {
    let sweeper = tokeira_runtime::chasm::ChasmTimerSweeper::new(engine, evaluator);
    let _sweeper = tasks.spawn(async move {
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

async fn build_service_stack_with_storage<R, L, S, V, F>(
    transport: StackTransport,
    effective_config: Arc<TokeiraConfig>,
    run_repository: Arc<R>,
    worker_deployment_repository: Arc<dyn WorkerDeploymentRepository>,
    worker_compute_repository: Option<Arc<dyn WorkerComputeRepository>>,
    task_queue_config_repository: Arc<dyn TaskQueueConfigRepository>,
    worker_task_provenance: Arc<dyn WorkerTaskProvenanceStore>,
    projection_log: L,
    visibility_query_store: V,
    projection_sink: F,
    dsql_endpoint: Option<String>,
    chasm_node_repo: Arc<dyn tokeira_storage::ChasmNodeRepository>,
    recover_self_assigned_shard: bool,
) -> Result<ConstructedStack>
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
    let advertised_addr = match transport {
        StackTransport::Network(addr) => addr,
        StackTransport::Embedded => SocketAddr::from(([127, 0, 0, 1], 0)),
    };
    let node_endpoint = configured_node_endpoint(&effective_config, advertised_addr);
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
    let nexus_http_client: Arc<dyn tokeira_runtime::NexusHttpClient> =
        Arc::new(HttpNexusClient::new());

    // The runtime owns execution orchestration, scanners, brokers, and all
    // run-local in-memory coordination such as buffered consistent queries.
    let schedule_store = Arc::new(ScheduleStore::default());
    let task_queue_config_store = Arc::new(RepositoryBackedTaskQueueConfigStore::new(
        task_queue_config_repository,
    ));
    // A server never fabricates an unset/default policy when durable state was
    // merely unavailable: hydrate must succeed before polls can be accepted.
    task_queue_config_store
        .hydrate()
        .await
        .context("failed to hydrate task-queue policy")?;
    let mut runtime_config = RuntimeConfig::default();
    runtime_config.lane.controller_managed_placement = effective_config
        .infrastructure
        .placement
        .controller_endpoint
        .is_some();
    let seed_default_shard = !recover_self_assigned_shard
        && dsql_endpoint.is_none()
        && effective_config
            .infrastructure
            .placement
            .controller_endpoint
            .is_none();

    let nexus_completion_cfg = effective_config.policy.nexus_completion.clone();
    let (
        nexus_callback_listener,
        in_process_nexus_client,
        nexus_completion_client,
        resolved_system_callback_url,
    ): (
        Option<TcpListener>,
        Option<Arc<InProcessNexusCompletionClient>>,
        Arc<dyn NexusCompletionClient>,
        String,
    ) = match transport {
        StackTransport::Network(_) => {
            // Bind before runtime construction because the firing client's system
            // callback URL must contain the listener's actual ephemeral port.
            let listener = TcpListener::bind(&nexus_completion_cfg.http_addr)
                .await
                .with_context(|| {
                    format!(
                        "failed to bind nexus completion listener on {}",
                        nexus_completion_cfg.http_addr
                    )
                })?;
            let callback_addr = listener
                .local_addr()
                .context("failed to resolve bound nexus completion listener address")?;
            let callback_url = with_loopback_port(
                &nexus_completion_cfg.system_callback_url,
                callback_addr.port(),
            );
            info!(
                bind = %callback_addr,
                loopback = %callback_url,
                "nexus completion callback listener bound"
            );
            (
                Some(listener),
                None,
                Arc::new(HttpNexusCompletionClient::new()),
                callback_url,
            )
        }
        StackTransport::Embedded => {
            // A weak runtime attachment closes the system-callback loop without a
            // socket and without creating a runtime→client→runtime ownership cycle.
            let client = Arc::new(InProcessNexusCompletionClient::new());
            (
                None,
                Some(client.clone()),
                client,
                EMBEDDED_NEXUS_CALLBACK_BASE.to_owned(),
            )
        }
    };
    let nexus_completion_deps = NexusCompletionDeps {
        client: nexus_completion_client,
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

    let worker_compute_deployment_repository = worker_deployment_repository.clone();
    let runtime = TokeiraRuntime::new_with_nexus_and_shards_and_endpoint(
        repo.clone(),
        runtime_config.lane_count,
        runtime_config.lane,
        runtime_config.timer_scanner,
        runtime_config.workflow_timeout_scanner,
        runtime_config.backlog,
        runtime_config.activity_timeout_scanner,
        runtime_config.nexus_timeout_scanner,
        nexus_registry.clone(),
        nexus_http_client.clone(),
        // Listener-backed mode posts through HTTP. Embedded mode intercepts the
        // reserved system target and invokes the same callback handler inline.
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
    .with_worker_deployment_repository(worker_deployment_repository)
    .with_task_queue_config_store(task_queue_config_store.clone())
    .with_delivery_mode_provider(Arc::new(
        tokeira_runtime::ConfiguredDeliveryModeProvider::new(
            tokeira_runtime::StaticDeliveryPolicy {
                enable_fairness: effective_config.policy.task_queues.enable_fairness,
            },
        ),
    ));
    let runtime = Arc::new(runtime);
    let runtime_tasks = runtime.shutdown_handle();
    let engine_tasks = RuntimeShutdownHandle::new();
    let mut startup_guard = StackStartupGuard::new(runtime_tasks.clone(), engine_tasks.clone());
    let background_cancel = engine_tasks.cancellation_token();

    let recovered_lease = if recover_self_assigned_shard {
        let shard_id = ShardId(0);
        let epoch = runtime
            .acquire_shard(shard_id)
            .await
            .context("failed to recover embedded snapshot shard")?;
        Some((shard_id, node_id.to_string(), epoch))
    } else {
        None
    };

    if matches!(transport, StackTransport::Network(_))
        && dsql_endpoint.is_some()
        && effective_config
            .infrastructure
            .placement
            .controller_endpoint
            .is_none()
    {
        let _leases = self_assign_dsql_shards(
            runtime.as_ref(),
            repo.as_ref(),
            effective_config.infrastructure.placement.shard_count,
            &node_id,
            &node_endpoint,
            false,
        )
        .await?;
    }

    let recovery_task = recovered_lease.map(|(shard_id, owner, epoch)| {
        let runtime = runtime.clone();
        let repo = repo.clone();
        let cancel = background_cancel.clone();
        tokio::spawn(async move {
            cancel.cancelled().await;
            // Drain first so the shard-scoped token stops the lease renewer,
            // then advance the durable epoch before a graceful snapshot. A
            // crash may leave an active owner in the last interval snapshot;
            // boot retires that captured owner before acquiring the new one.
            runtime.relinquish_shard(shard_id).await;
            match repo
                .relinquish_bundle(shard_id, owner, epoch)
                .await
                .context("failed to release embedded snapshot recovery lease")?
            {
                LeaseOutcome::Acquired { .. } => Ok(()),
                LeaseOutcome::Rejected {
                    current_owner,
                    current_epoch,
                } => Err(anyhow!(
                    "embedded snapshot recovery lease was fenced by owner {current_owner} at epoch {}",
                    current_epoch.0
                )),
                LeaseOutcome::Renewed { epoch } => Err(anyhow!(
                    "embedded snapshot recovery lease unexpectedly renewed at epoch {}",
                    epoch.0
                )),
            }
        })
    });
    let task_queue_config_refresh = task_queue_config_store
        .clone()
        .spawn_refresh(background_cancel.clone());
    let _task_queue_config_refresh = engine_tasks.track_join(task_queue_config_refresh);
    let _worker_task_provenance_cleanup = spawn_worker_task_provenance_cleanup(
        &engine_tasks,
        worker_task_provenance.clone(),
        background_cancel.clone(),
    );

    let _worker_compute_service = if effective_config.policy.worker_compute.enabled {
        let controller_repository = worker_compute_repository
            .clone()
            .context("worker-compute enabled without a controller repository")?;
        let (observation_sender, observation_receiver) =
            mpsc::channel(OBSERVATION_CHANNEL_CAPACITY);
        let observation_sink = Arc::new(ChannelDemandObservationSink::new(observation_sender));
        runtime
            .broker()
            .set_demand_observation_sink(observation_sink.clone());
        runtime
            .activity_broker()
            .set_demand_observation_sink(observation_sink.clone());
        runtime
            .nexus_task_broker()
            .set_demand_observation_sink(observation_sink);

        let (reconcile_sender, reconcile_receiver) = mpsc::channel(RECONCILE_CHANNEL_CAPACITY);
        runtime
            .deployment_registry()
            .expect("Worker Deployment repository was installed above")
            .set_worker_compute_reconcile_sink(Arc::new(ChannelWorkerComputeReconcileSink::new(
                reconcile_sender,
            )));

        let clock = Arc::new(SystemWorkerComputeClock);
        let catalog = Arc::new(WorkerComputeNamespaceCatalogAdapter::new(
            namespaces.clone(),
        ));
        let reconciler = WorkerComputeReconciler::new(
            worker_compute_deployment_repository,
            controller_repository.clone(),
            node_id,
        );
        let sampler = runtime.worker_compute_queue_sampler(controller_repository.clone(), node_id);
        let provider = Arc::new(NexusWorkerComputeProvider::new(
            nexus_registry,
            nexus_http_client,
            runtime.nexus_task_broker(),
        ));
        let outbox = WorkerComputeOutbox::new(
            controller_repository.clone(),
            provider,
            clock.clone(),
            node_id,
        );
        let shard_runtime = runtime.clone();
        let active_shards: tokeira_runtime::WorkerComputeActiveShards =
            Arc::new(move || shard_runtime.active_shards());
        let service = WorkerComputeControllerService::new(
            catalog,
            controller_repository,
            reconciler,
            sampler,
            outbox,
            clock,
            active_shards,
            observation_receiver,
            reconcile_receiver,
        );
        let service_cancel = background_cancel.clone();
        Some(engine_tasks.spawn(async move {
            if let Err(error) = service.run(service_cancel).await {
                tracing::warn!(?error, "worker-compute controller service exited");
            }
        }))
    } else {
        None
    };

    let membership_client = effective_config
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
    if let Some(membership_client) = membership_client {
        let _membership_client = engine_tasks.track_join(membership_client);
    }
    let schedule_engine = run_schedule_engine(
        schedule_store.clone(),
        runtime.clone(),
        ScheduleEngineConfig::default(),
        background_cancel.clone(),
    );
    let _schedule_engine = engine_tasks.track_join(schedule_engine);

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
        let _routing_subscription = engine_tasks.spawn(async move {
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
    let callback_runtime = runtime_adapter.clone() as Arc<dyn WorkflowRuntimeApi>;
    if let Some(client) = in_process_nexus_client {
        client.attach(&callback_runtime)?;
    }
    if let Some(listener) = nexus_callback_listener {
        // Listener-backed and embedded completion both delegate to this same
        // `WorkflowRuntimeApi`; only the transport around the callback differs.
        spawn_nexus_callback_server(listener, callback_runtime, background_cancel.clone());
    }
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
        InMemoryOperatorApi::new("tokeira-local", tokeira_build_info::SERVER_VERSION),
        operator_visibility_store,
    ));

    for partition_id in 0..effective_config.infrastructure.placement.partition_count {
        let projection_worker = ProjectionWorker {
            log: projection_log.clone(),
            sink: projection_sink(),
            batch_size: 256,
        };
        let projection_cancel = background_cancel.clone();
        let _projection_worker = engine_tasks.spawn(async move {
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
        .with_worker_task_provenance(worker_task_provenance)
        .with_scoped_worker_sessions(ScopedWorkerSessionRegistry::default())
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
    let http_api_layer = HttpApiLayer::new(
        HttpApiCatalog::pinned().context("failed to build Temporal HTTP API route catalog")?,
        HttpApiPolicy::new(
            effective_config.policy.http_api.allowed_hosts.clone(),
            effective_config
                .policy
                .http_api
                .additional_forwarded_headers
                .clone(),
        )
        .context("failed to compile Temporal HTTP API policy")?,
    );

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
            &engine_tasks,
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
                &engine_tasks,
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

    // Embedded startup defers self-assignment until every other fallible stack
    // construction step has completed. Admission is not opened until all
    // configured leases are held, and the returned cleanup owns the exact
    // `(shard, owner, epoch)` tuples needed for conditional release.
    let shard_cleanup = if matches!(transport, StackTransport::Embedded)
        && dsql_endpoint.is_some()
        && effective_config
            .infrastructure
            .placement
            .controller_endpoint
            .is_none()
    {
        let leases = self_assign_dsql_shards(
            runtime.as_ref(),
            repo.as_ref(),
            effective_config.infrastructure.placement.shard_count,
            &node_id,
            &node_endpoint,
            true,
        )
        .await?;
        Some(self_assigned_shard_cleanup(
            runtime.clone(),
            repo.clone(),
            leases,
        ))
    } else {
        None
    };

    match dsql_endpoint {
        Some(endpoint) => info!(%endpoint, "storage backend: dsql"),
        None => info!("storage backend: in-memory"),
    }

    let log_broadcast = broadcast::Sender::<LogEvent>::new(LOG_BROADCAST_CAPACITY);
    let addr = match transport {
        StackTransport::Embedded => {
            startup_guard.disarm();
            return Ok(ConstructedStack::Embedded(EmbeddedStack {
                service: InProcessGrpcService::new(workflow_grpc, operator_grpc, admin_grpc),
                background_cancel,
                log_broadcast,
                recovery_task,
                runtime_tasks,
                engine_tasks,
                shard_cleanup,
            }));
        }
        StackTransport::Network(addr) => addr,
    };

    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(tokeira_proto::public::FILE_DESCRIPTOR_SET)
        .build()
        .context("failed to build gRPC reflection service")?;

    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind tokeirad gRPC listener on {addr}"))?;
    let bound_addr = listener
        .local_addr()
        .context("failed to resolve bound local address for tokeirad gRPC listener")?;

    let (shutdown_tx, shutdown_rx) = oneshot::channel();

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
                    .layer(http_api_layer.clone())
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
                    .layer(http_api_layer)
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

    startup_guard.disarm();
    Ok(ConstructedStack::Network((
        server_task,
        bound_addr,
        shutdown_tx,
        background_cancel,
        log_broadcast,
        wire_coverage_recorder,
    )))
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

/// Outcome of reading a request body under a byte cap (see [`collect_bounded_body`]).
#[derive(Debug)]
enum BoundedBody {
    /// The body fit within the cap and was fully buffered.
    Collected(Bytes),
    /// The body exceeded the cap; collection stopped early. Maps to `413`.
    TooLarge,
    /// The underlying stream errored before completing. Maps to `400`.
    ReadFailed(Box<dyn std::error::Error + Send + Sync>),
}

/// Buffer `body` into memory, refusing to accumulate more than `limit` bytes.
///
/// [`Limited`] stops polling once the running total would exceed `limit` and surfaces a
/// downcastable [`LengthLimitError`], letting the caller reject an oversized body with
/// `413` *before* it is fully read — the bound that keeps the network-exposed
/// `/nexus/callback` listener from being an unauthenticated memory-exhaustion vector.
async fn collect_bounded_body<B>(body: B, limit: usize) -> BoundedBody
where
    B: hyper::body::Body<Data = Bytes>,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    match Limited::new(body, limit).collect().await {
        Ok(collected) => BoundedBody::Collected(collected.to_bytes()),
        Err(error) if error.downcast_ref::<LengthLimitError>().is_some() => BoundedBody::TooLarge,
        Err(error) => BoundedBody::ReadFailed(error),
    }
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
    let operation_token = header(NEXUS_OPERATION_TOKEN_HEADER);
    let state = header(NEXUS_OPERATION_STATE_HEADER);
    let content_type = header(hyper::header::CONTENT_TYPE.as_str());

    // Bound the body before buffering it. This listener is network-exposed (default
    // bind is loopback, but operators may widen it) and reads the whole body *before*
    // `handle_nexus_callback` validates the callback token, so an unbounded `collect()`
    // is an unauthenticated memory-exhaustion vector — and the process runs
    // `panic = "abort"`, so an OOM abort drops every in-flight workflow on the node.
    // Mirror the cap the caller-facing Nexus transport already enforces.
    let body_bytes = match collect_bounded_body(body, MAX_NEXUS_PAYLOAD_BYTES).await {
        BoundedBody::Collected(bytes) => bytes,
        BoundedBody::TooLarge => {
            tracing::debug!(
                limit = MAX_NEXUS_PAYLOAD_BYTES,
                "nexus callback body too large"
            );
            return nexus_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                Some(b"request body too large".to_vec()),
            );
        }
        BoundedBody::ReadFailed(error) => {
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
        operation_token.as_deref(),
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
    require_all: bool,
) -> Result<Vec<SelfAssignedShardLease>>
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

    let mut acquired = Vec::new();
    let owner = node_id.to_string();
    for shard_index in 0..shard_count {
        let shard_id = ShardId(shard_index);
        match lease_repository
            .try_acquire_bundle(shard_id, owner.clone(), node_endpoint.as_authority())
            .await
        {
            Ok(LeaseOutcome::Acquired { epoch } | LeaseOutcome::Renewed { epoch }) => {
                runtime.record_self_assigned_shard(shard_id, epoch);
                acquired.push(SelfAssignedShardLease {
                    shard_id,
                    owner: owner.clone(),
                    epoch,
                });
            }
            Ok(LeaseOutcome::Rejected {
                current_owner,
                current_epoch,
            }) => {
                if require_all {
                    rollback_self_assigned_shards(runtime, lease_repository, &acquired).await;
                    return Err(anyhow!(
                        "embedded DSQL shard {shard_index} is held by owner {current_owner} at epoch {}",
                        current_epoch.0
                    ));
                }
                tracing::warn!(
                    shard_index,
                    %current_owner,
                    current_epoch = current_epoch.0,
                    "failed to self-assign shard: lease is held by another owner"
                );
            }
            Err(error) => {
                if require_all {
                    rollback_self_assigned_shards(runtime, lease_repository, &acquired).await;
                    return Err(error).context(format!(
                        "failed to self-assign embedded DSQL shard {shard_index}"
                    ));
                }
                tracing::warn!(shard_index, ?error, "failed to self-assign shard");
            }
        }
    }
    info!(
        acquired = acquired.len(),
        shard_count, "self-assigned DSQL shards (no controller)"
    );
    Ok(acquired)
}

async fn rollback_self_assigned_shards<R>(
    runtime: &TokeiraRuntime<R>,
    lease_repository: &R,
    leases: &[SelfAssignedShardLease],
) where
    R: LeaseRepository + RunRepository + 'static,
{
    for lease in leases.iter().rev() {
        runtime.relinquish_shard(lease.shard_id).await;
        let _ = lease_repository
            .relinquish_bundle(lease.shard_id, lease.owner.clone(), lease.epoch)
            .await;
    }
}

fn self_assigned_shard_cleanup<R>(
    runtime: Arc<TokeiraRuntime<R>>,
    lease_repository: Arc<R>,
    leases: Vec<SelfAssignedShardLease>,
) -> CleanupAction
where
    R: LeaseRepository + RunRepository + 'static,
{
    Box::new(move |deadline| {
        Box::pin(async move {
            let mut first_error = None;
            for lease in leases.into_iter().rev() {
                runtime.relinquish_shard(lease.shard_id).await;
                let now = Instant::now();
                if now >= deadline {
                    first_error.get_or_insert_with(|| {
                        anyhow!("deadline elapsed while relinquishing embedded DSQL shards")
                    });
                    continue;
                }
                let release = tokio::time::timeout(
                    deadline.saturating_duration_since(now),
                    lease_repository.relinquish_bundle(lease.shard_id, lease.owner, lease.epoch),
                )
                .await;
                match release {
                    Ok(Ok(LeaseOutcome::Acquired { .. })) => {}
                    Ok(Ok(outcome)) => {
                        first_error.get_or_insert_with(|| {
                            anyhow!("embedded DSQL shard release was fenced: {outcome:?}")
                        });
                    }
                    Ok(Err(error)) => {
                        first_error.get_or_insert(error);
                    }
                    Err(_) => {
                        first_error.get_or_insert_with(|| {
                            anyhow!("deadline elapsed while relinquishing embedded DSQL shard")
                        });
                    }
                }
            }
            match first_error {
                Some(error) => Err(error),
                None => Ok(()),
            }
        })
    })
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
                                    priority: activity.priority.as_ref().map(|priority| {
                                        tokeira_edge::translate::Priority {
                                            priority_key: priority.priority_key,
                                            fairness_key: priority.fairness_key.clone(),
                                            fairness_weight: priority.fairness_weight,
                                        }
                                    }),
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
                    priority: state.priority.as_ref().map(|priority| {
                        tokeira_edge::translate::Priority {
                            priority_key: priority.priority_key,
                            fairness_key: priority.fairness_key.clone(),
                            fairness_weight: priority.fairness_weight,
                        }
                    }),
                    auto_reset_points: state.auto_reset_points.clone(),
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
    use metrics::with_local_recorder;
    use metrics_util::debugging::DebuggingRecorder;
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::{
        error::{OTelSdkError, OTelSdkResult},
        trace::{SdkTracerProvider, SpanData, SpanExporter},
    };
    use proptest::prelude::*;
    use tracing_subscriber::layer::SubscriberExt as _;
    // Endpoint target/config types are referenced only by the Nexus endpoint store
    // tests below; importing them here (not at crate scope) keeps the lib build clean.
    use tokeira_runtime::{EndpointTarget, NexusEndpointConfig};

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum StartupModelPhase {
        Cluster,
        Pool,
        Schema,
        Ownership,
        Runtime,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum StartupModelEvent {
        Attempt(StartupModelPhase),
        Complete(StartupModelPhase),
        ReleaseOwnership,
        ClosePool,
        ReturnHandle,
    }

    const STARTUP_MODEL_PHASES: [StartupModelPhase; 5] = [
        StartupModelPhase::Cluster,
        StartupModelPhase::Pool,
        StartupModelPhase::Schema,
        StartupModelPhase::Ownership,
        StartupModelPhase::Runtime,
    ];

    fn startup_model(failure_boundary: Option<usize>) -> Vec<StartupModelEvent> {
        let mut events = Vec::new();
        let mut pool_open = false;
        let mut ownership_held = false;
        for (index, phase) in STARTUP_MODEL_PHASES.into_iter().enumerate() {
            events.push(StartupModelEvent::Attempt(phase));
            if failure_boundary == Some(index) {
                if ownership_held {
                    events.push(StartupModelEvent::ReleaseOwnership);
                }
                if pool_open {
                    events.push(StartupModelEvent::ClosePool);
                }
                return events;
            }
            events.push(StartupModelEvent::Complete(phase));
            pool_open |= phase == StartupModelPhase::Pool;
            ownership_held |= phase == StartupModelPhase::Ownership;
        }
        events.push(StartupModelEvent::ReturnHandle);
        events
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum ShutdownModelEvent {
        CloseAdmission,
        CancelTasks,
        CallsComplete,
        DrainComplete,
        TasksComplete,
        JoinComplete,
        FinishTelemetry,
        AttemptShardRelease,
        CloseOwnershipAdmission,
        AttemptOwnershipRelease,
        AttemptPoolClose,
        ReturnToHost,
    }

    fn shutdown_model(calls_pending: bool, tasks_pending: bool) -> Vec<ShutdownModelEvent> {
        let mut events = vec![
            ShutdownModelEvent::CloseAdmission,
            ShutdownModelEvent::CancelTasks,
        ];
        if calls_pending {
            events.push(ShutdownModelEvent::CallsComplete);
        }
        events.push(ShutdownModelEvent::DrainComplete);
        if tasks_pending {
            events.push(ShutdownModelEvent::TasksComplete);
        }
        events.extend([
            ShutdownModelEvent::JoinComplete,
            ShutdownModelEvent::FinishTelemetry,
            ShutdownModelEvent::AttemptShardRelease,
            ShutdownModelEvent::CloseOwnershipAdmission,
            ShutdownModelEvent::AttemptOwnershipRelease,
            ShutdownModelEvent::AttemptPoolClose,
            ShutdownModelEvent::ReturnToHost,
        ]);
        events
    }

    #[derive(Clone, Copy, Debug)]
    enum HostInstrumentationModel {
        None,
        LocalRecorder,
        LocalSubscriber,
        RecorderAndSubscriber,
    }

    fn host_instrumentation_strategy() -> impl Strategy<Value = HostInstrumentationModel> {
        prop_oneof![
            Just(HostInstrumentationModel::None),
            Just(HostInstrumentationModel::LocalRecorder),
            Just(HostInstrumentationModel::LocalSubscriber),
            Just(HostInstrumentationModel::RecorderAndSubscriber),
        ]
    }

    #[derive(Clone, Copy, Debug)]
    enum EmbeddedStorageModeModel {
        InMemory,
        ManagedDsql,
        ExistingDsql,
    }

    fn storage_mode_strategy() -> impl Strategy<Value = EmbeddedStorageModeModel> {
        prop_oneof![
            Just(EmbeddedStorageModeModel::InMemory),
            Just(EmbeddedStorageModeModel::ManagedDsql),
            Just(EmbeddedStorageModeModel::ExistingDsql),
        ]
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct EmbeddedConstructionEffects {
        listener_attempts: usize,
        global_install_attempts: usize,
        signal_handler_attempts: usize,
        local_emission_attempts: usize,
    }

    fn embedded_construction_effects(
        _host: HostInstrumentationModel,
        _storage: EmbeddedStorageModeModel,
    ) -> EmbeddedConstructionEffects {
        // Every storage choice crosses the same library boundary. Durable
        // modes add AWS/SQL I/O but never acquire host process facilities.
        EmbeddedConstructionEffects {
            listener_attempts: 0,
            global_install_attempts: 0,
            signal_handler_attempts: 0,
            local_emission_attempts: 1,
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum TelemetryFixture {
        NoSubscriber,
        LocalRecorder,
        FailingExporter,
    }

    fn telemetry_fixture_strategy() -> impl Strategy<Value = TelemetryFixture> {
        prop_oneof![
            Just(TelemetryFixture::NoSubscriber),
            Just(TelemetryFixture::LocalRecorder),
            Just(TelemetryFixture::FailingExporter),
        ]
    }

    #[derive(Clone, Debug)]
    struct FailingSpanExporter;

    impl SpanExporter for FailingSpanExporter {
        async fn export(&self, _batch: Vec<SpanData>) -> OTelSdkResult {
            Err(OTelSdkError::InternalFailure(
                "fixture exporter rejected the batch".to_owned(),
            ))
        }
    }

    fn apply_observed_sequence(ops: &[i16]) -> Vec<u8> {
        let mut state = 0_i64;
        let mut committed = Vec::with_capacity(ops.len() * std::mem::size_of::<i64>());
        let span = tracing::info_span!(
            "embedded.observational_fixture",
            tokeira.operation = "transition_sequence"
        );
        let _entered = span.enter();
        for op in ops {
            record_embedded_lifecycle(
                EmbeddedStorageModeLabel::InMemory,
                ClusterStatusLabel::NotApplicable,
                SchemaOutcomeLabel::NotApplicable,
                OwnershipOutcomeLabel::NotApplicable,
                DbClassLabel::Commit,
                EmbeddedOperationLabel::Startup,
                ErrorClassLabel::None,
            );
            state = state.wrapping_add(i64::from(*op));
            committed.extend_from_slice(&state.to_le_bytes());
        }
        committed
    }

    fn observed_sequence_bytes(ops: &[i16], fixture: TelemetryFixture) -> Vec<u8> {
        match fixture {
            TelemetryFixture::NoSubscriber => apply_observed_sequence(ops),
            TelemetryFixture::LocalRecorder => {
                let recorder = DebuggingRecorder::new();
                with_local_recorder(&recorder, || apply_observed_sequence(ops))
            }
            TelemetryFixture::FailingExporter => {
                let provider = SdkTracerProvider::builder()
                    .with_simple_exporter(FailingSpanExporter)
                    .build();
                let tracer = provider.tracer("observational-property");
                let subscriber = tracing_subscriber::registry()
                    .with(tracing_opentelemetry::layer().with_tracer(tracer));
                let dispatch = tracing::Dispatch::new(subscriber);
                let bytes =
                    tracing::dispatcher::with_default(&dispatch, || apply_observed_sequence(ops));
                let _ = provider.force_flush();
                bytes
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        // Feature: managed-embedded-dsql, Property 12: startup is prefix-safe and failure-atomic
        #[test]
        fn startup_is_prefix_safe_and_failure_atomic(
            failure_boundary in prop::option::of(0usize..STARTUP_MODEL_PHASES.len()),
        ) {
            let events = startup_model(failure_boundary);
            let attempted = events.iter().filter_map(|event| match event {
                StartupModelEvent::Attempt(phase) => Some(*phase),
                _ => None,
            }).collect::<Vec<_>>();
            let completed = events.iter().filter_map(|event| match event {
                StartupModelEvent::Complete(phase) => Some(*phase),
                _ => None,
            }).collect::<Vec<_>>();

            prop_assert_eq!(attempted.as_slice(), &STARTUP_MODEL_PHASES[..attempted.len()]);
            prop_assert_eq!(completed.as_slice(), &STARTUP_MODEL_PHASES[..completed.len()]);
            if let Some(boundary) = failure_boundary {
                prop_assert!(!events.contains(&StartupModelEvent::ReturnHandle));
                let rollback = events.iter().filter(|event| matches!(
                    event,
                    StartupModelEvent::ReleaseOwnership | StartupModelEvent::ClosePool
                )).copied().collect::<Vec<_>>();
                let expected = match boundary {
                    0 | 1 => Vec::new(),
                    2 | 3 => vec![StartupModelEvent::ClosePool],
                    _ => vec![
                        StartupModelEvent::ReleaseOwnership,
                        StartupModelEvent::ClosePool,
                    ],
                };
                prop_assert_eq!(rollback, expected);
            } else {
                prop_assert_eq!(completed.len(), STARTUP_MODEL_PHASES.len());
                prop_assert_eq!(events.last(), Some(&StartupModelEvent::ReturnHandle));
            }
        }

        // Feature: managed-embedded-dsql, Property 19: shutdown establishes the host flush boundary
        #[test]
        fn shutdown_establishes_the_host_flush_boundary(
            calls_pending in any::<bool>(),
            tasks_pending in any::<bool>(),
            independent_failure_mask in 0u8..8,
        ) {
            let events = shutdown_model(calls_pending, tasks_pending);
            prop_assert_eq!(events.first(), Some(&ShutdownModelEvent::CloseAdmission));
            prop_assert_eq!(events.get(1), Some(&ShutdownModelEvent::CancelTasks));
            prop_assert_eq!(events.last(), Some(&ShutdownModelEvent::ReturnToHost));
            for cleanup in [
                ShutdownModelEvent::AttemptShardRelease,
                ShutdownModelEvent::AttemptOwnershipRelease,
                ShutdownModelEvent::AttemptPoolClose,
            ] {
                prop_assert!(events.contains(&cleanup));
            }
            prop_assert!(
                events.iter().position(|event| *event == ShutdownModelEvent::AttemptShardRelease)
                    < events.iter().position(|event| *event == ShutdownModelEvent::CloseOwnershipAdmission)
            );
            prop_assert!(
                events.iter().position(|event| *event == ShutdownModelEvent::CloseOwnershipAdmission)
                    < events.iter().position(|event| *event == ShutdownModelEvent::AttemptOwnershipRelease)
            );
            let failures = (0..3)
                .filter(|bit| independent_failure_mask & (1 << bit) != 0)
                .count();
            prop_assert_eq!(failures, independent_failure_mask.count_ones() as usize);
            prop_assert!(
                events.iter().position(|event| *event == ShutdownModelEvent::FinishTelemetry)
                    < events.iter().position(|event| *event == ShutdownModelEvent::ReturnToHost)
            );
        }

        // Feature: managed-embedded-dsql, Property 14: embedded construction is transport- and global-state-neutral
        #[test]
        fn embedded_construction_is_transport_and_global_state_neutral(
            host in host_instrumentation_strategy(),
            storage in storage_mode_strategy(),
        ) {
            let effects = embedded_construction_effects(host, storage);
            prop_assert_eq!(effects.listener_attempts, 0);
            prop_assert_eq!(effects.global_install_attempts, 0);
            prop_assert_eq!(effects.signal_handler_attempts, 0);
            prop_assert_eq!(effects.local_emission_attempts, 1);
        }

        // Feature: managed-embedded-dsql, Property 20: telemetry is observational only
        #[test]
        fn telemetry_is_observational_only(
            operations in prop::collection::vec(any::<i16>(), 0..64),
            first in telemetry_fixture_strategy(),
            second in telemetry_fixture_strategy(),
        ) {
            let expected = observed_sequence_bytes(&operations, TelemetryFixture::NoSubscriber);
            prop_assert_eq!(observed_sequence_bytes(&operations, first), expected.clone());
            prop_assert_eq!(observed_sequence_bytes(&operations, second), expected);
        }
    }

    #[test]
    fn every_startup_failure_boundary_rolls_back_without_returning_a_handle() {
        for boundary in 0..STARTUP_MODEL_PHASES.len() {
            let events = startup_model(Some(boundary));
            assert!(!events.contains(&StartupModelEvent::ReturnHandle));
        }
    }

    #[test]
    fn durable_startup_report_exposes_only_approved_evidence() {
        let report = EngineStartupReport {
            storage_mode: EmbeddedStorageMode::ManagedDsql,
            cluster: Some(ClusterStartupReport {
                region: "eu-west-1".to_owned(),
                cluster_id: "cluster-1".to_owned(),
                cluster_arn: "arn:aws:dsql:eu-west-1:123456789012:cluster/cluster-1".to_owned(),
                endpoint: "cluster-1.dsql.eu-west-1.on.aws".to_owned(),
                action: ClusterAction::Recovered,
            }),
            schema: Some(SchemaStartupReport {
                observed_version: 1,
                minimum_supported_version: 1,
                target_version: 1,
                maximum_readable_version: 1,
                migration_set_digest: "release-digest".to_owned(),
                outcome: SchemaStartupOutcome::Compatible,
            }),
            ownership: Some(OwnershipStartupReport {
                outcome: ControlLeaseAcquireOutcome::Clean,
                fence_token: 7,
            }),
        };
        let diagnostic = format!("{report:?}");
        assert!(!diagnostic.contains("descriptor-path-secret"));
        assert!(!diagnostic.contains("create-token-secret"));
        assert!(!diagnostic.contains("credential-secret"));
    }

    #[tokio::test]
    async fn explicit_in_memory_start_preserves_legacy_mode_and_shuts_down_cleanly() {
        let engine = Engine::start_with_embedded_config(EmbeddedEngineConfig::default())
            .await
            .expect("explicit in-memory startup succeeds");
        assert_eq!(engine.startup_report(), &EngineStartupReport::in_memory());
        engine.shutdown().await.expect("tracked shutdown succeeds");
    }

    #[test]
    fn embedded_lifecycle_composes_with_a_host_local_recorder() {
        let recorder = DebuggingRecorder::new();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("embedded fixture runtime");

        with_local_recorder(&recorder, || {
            runtime.block_on(async {
                // Starting twice in one process also proves the library did not
                // consume the one-shot process observability reservation.
                for _ in 0..2 {
                    let engine =
                        Engine::start_with_embedded_config(EmbeddedEngineConfig::default())
                            .await
                            .expect("embedded startup");
                    engine.shutdown().await.expect("embedded shutdown");
                }
            });
        });

        let lifecycle_observations = recorder
            .snapshotter()
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(key, _, _, _)| {
                key.key().name() == tokeira_observability::EMBEDDED_LIFECYCLE_OPERATIONS_TOTAL
            })
            .count();
        assert_eq!(lifecycle_observations, 2);
    }

    #[tokio::test]
    async fn invalid_explicit_config_fails_before_startup() {
        let config = EmbeddedEngineConfig {
            startup_timeout_ms: 0,
            ..EmbeddedEngineConfig::default()
        };
        assert!(matches!(
            Engine::start_with_embedded_config(config).await,
            Err(EmbeddedEngineStartError::InvalidConfiguration(_))
        ));
    }

    #[tokio::test]
    async fn durable_snapshot_conflict_fails_before_descriptor_or_aws_work() {
        let descriptor_path = std::env::temp_dir().join(format!(
            "tokeira-managed-dsql-preflight-{}.json",
            IncarnationId::new()
        ));
        let mut config = EmbeddedEngineConfig::default();
        config.server.policy.snapshot = Some(SnapshotPolicyConfig {
            location: descriptor_path.with_extension("snapshot"),
            interval_ms: 1_000,
        });
        config.storage =
            EmbeddedStorageConfig::ManagedDsql(tokeira_config::ManagedEmbeddedDsqlConfig {
                intent: tokeira_config::ManagedClusterIntent::CreateOrRecover,
                descriptor_path: descriptor_path.clone(),
                region: "eu-west-2".to_owned(),
                migration_policy: None,
                limits: tokeira_config::EmbeddedDsqlLimits::default(),
                tags: std::collections::BTreeMap::new(),
            });

        assert!(matches!(
            Engine::start_with_embedded_config(config).await,
            Err(EmbeddedEngineStartError::Phase {
                phase: EmbeddedStartupPhase::Configuration
            })
        ));
        assert!(!descriptor_path.exists());
    }

    #[tokio::test]
    async fn drop_synchronously_closes_endpoint_admission() {
        let engine = Engine::start().await.expect("engine starts");
        let endpoint = engine.endpoint();
        drop(engine);

        let error = endpoint
            .call(InProcessGrpcRequest {
                service: "temporal.api.workflowservice.v1.WorkflowService".to_owned(),
                rpc: "GetSystemInfo".to_owned(),
                headers: http::HeaderMap::new(),
                proto: Bytes::new(),
            })
            .await
            .expect_err("dropped engine rejects endpoint clones");
        assert_eq!(error.code(), tonic::Code::Unavailable);
    }

    #[tokio::test]
    async fn snapshot_policy_over_dsql_storage_is_rejected() {
        let mut config = TokeiraConfig::default();
        config.infrastructure.storage = ConfigStorageKind::Dsql;
        config.policy.snapshot = Some(SnapshotPolicyConfig {
            location: std::path::PathBuf::from("unused.snapshot"),
            interval_ms: 60_000,
        });
        let error = build_and_serve(
            "127.0.0.1:0".parse().expect("loopback addr"),
            Arc::new(config),
        )
        .await
        .expect_err("snapshot policy over DSQL must be rejected before any I/O");
        let message = format!("{error:#}");
        assert!(
            message.contains("in-memory store's persistence mechanism"),
            "the rejection must name the mechanism: {message}"
        );
    }

    /// The daemon drain regression: a real SIGTERM to the serving process
    /// finishes in-flight work, persists the final snapshot, and the next
    /// boot restores from it. Runs in-process (nextest isolates one process
    /// per test, so signalling ourselves is safe); the restart half uses the
    /// listener facade, which shares the same build path minus the
    /// signal/observability install that must stay daemon-only.
    #[tokio::test(flavor = "multi_thread")]
    async fn sigterm_drains_persists_final_snapshot_and_next_boot_restores() {
        use tokeira_proto::workflowservice::{
            DescribeWorkflowExecutionRequest, StartWorkflowExecutionRequest,
            workflow_service_client::WorkflowServiceClient,
        };

        let scratch =
            std::env::temp_dir().join(format!("tokeira-engine-sigterm-{}", std::process::id()));
        std::fs::create_dir_all(&scratch).expect("create scratch dir");
        let snapshot_location = scratch.join("tokeirad.snapshot");

        // Reserve a loopback port for the daemon config. The tiny window
        // between drop and rebind is acceptable in a single-purpose test
        // process.
        let reserved = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve port");
        let addr = reserved.local_addr().expect("reserved addr");
        drop(reserved);

        let mut config = TokeiraConfig::default();
        config.infrastructure.network.grpc_addr = addr.to_string();
        // Loopback-ephemeral Nexus completion listener, exactly as the
        // in-memory facade configures it, so parallel tests never collide.
        config.policy.nexus_completion.http_addr = "127.0.0.1:0".to_string();
        config.policy.nexus_completion.system_callback_url = "http://127.0.0.1:0".to_string();
        config.policy.snapshot = Some(SnapshotPolicyConfig {
            location: snapshot_location.clone(),
            // Effectively periodic-off: this regression is about the FINAL
            // cut on the signal path.
            interval_ms: 3_600_000,
        });
        let config_path = scratch.join("tokeirad.toml");
        std::fs::write(&config_path, config.to_toml().expect("render config"))
            .expect("write config");

        let mut daemon = tokio::spawn(run_from_cli(Cli {
            config: Some(config_path.display().to_string()),
            dump_config: false,
            version: false,
            verbose: false,
            json: false,
        }));

        // Connect-poll until the daemon serves. The retry interval is a
        // polling cadence against an external TCP bind (the one boundary a
        // channel cannot observe), bounded by the deadline; a daemon that
        // exits early fails the wait immediately with its own error.
        let target = format!("http://{addr}");
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
        let mut client = loop {
            assert!(
                tokio::time::Instant::now() < deadline,
                "daemon must start serving"
            );
            tokio::select! {
                result = &mut daemon => {
                    panic!("daemon exited before serving: {result:?}");
                }
                connected = WorkflowServiceClient::connect(target.clone()) => {
                    match connected {
                        Ok(client) => break client,
                        Err(_) => {
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        }
                    }
                }
            }
        };
        client
            .start_workflow_execution(StartWorkflowExecutionRequest {
                namespace: "default".to_owned(),
                workflow_id: "sigterm-canary".to_owned(),
                workflow_type: Some(tokeira_proto::common::WorkflowType {
                    name: "sigterm-workflow".to_owned(),
                }),
                task_queue: Some(tokeira_proto::taskqueue::TaskQueue {
                    name: "sigterm-queue".to_owned(),
                    ..Default::default()
                }),
                request_id: "sigterm-start".to_owned(),
                ..Default::default()
            })
            .await
            .expect("start canary workflow");

        // The real signal, sent to this very process; the drain must treat it
        // exactly as an operator's `kill`.
        let delivered = std::process::Command::new("kill")
            .args(["-TERM", &std::process::id().to_string()])
            .status()
            .expect("send SIGTERM");
        assert!(delivered.success(), "kill -TERM must be deliverable");

        daemon
            .await
            .expect("daemon task must not panic")
            .expect("SIGTERM drain must exit cleanly");
        assert!(
            snapshot_location.is_file(),
            "the drain must persist the final snapshot"
        );

        // Next boot restores the canary from the snapshot.
        let restored =
            TokeiradHandle::start_in_memory_with_config("127.0.0.1:0".parse().expect("addr"), {
                let mut config = TokeiraConfig::default();
                config.policy.snapshot = Some(SnapshotPolicyConfig {
                    location: snapshot_location.clone(),
                    interval_ms: 3_600_000,
                });
                config
            })
            .await
            .expect("restart over the final snapshot");
        let mut client =
            WorkflowServiceClient::connect(format!("http://{}", restored.bound_addr()))
                .await
                .expect("connect to restarted server");
        let described = client
            .describe_workflow_execution(DescribeWorkflowExecutionRequest {
                namespace: "default".to_owned(),
                execution: Some(tokeira_proto::common::WorkflowExecution {
                    workflow_id: "sigterm-canary".to_owned(),
                    run_id: String::new(),
                }),
            })
            .await
            .expect("restored workflow must be describable")
            .into_inner();
        assert!(
            described.workflow_execution_info.is_some(),
            "state must survive the SIGTERM drain via snapshot restore"
        );
        restored.shutdown().await.expect("clean restart shutdown");
        let _ = std::fs::remove_dir_all(&scratch);
    }

    #[cfg(feature = "temporalio-client")]
    #[test]
    fn sdk_status_bridge_preserves_details_and_metadata() {
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert("x-retry-class", "busy".parse().expect("static metadata"));
        let status = tonic::Status::with_details_and_metadata(
            tonic::Code::ResourceExhausted,
            "try later",
            Bytes::from_static(b"typed-detail"),
            metadata,
        );

        let bridged = to_sdk_status(status);

        assert_eq!(bridged.code(), tonic_sdk::Code::ResourceExhausted);
        assert_eq!(bridged.message(), "try later");
        assert_eq!(bridged.details(), b"typed-detail");
        assert_eq!(
            bridged
                .metadata()
                .get("x-retry-class")
                .expect("metadata retained"),
            "busy"
        );
    }

    #[test]
    fn embedded_config_removes_daemon_storage_and_placement() {
        let mut config = TokeiraConfig::default();
        config.infrastructure.storage = ConfigStorageKind::Dsql;
        config.infrastructure.placement.controller_endpoint =
            Some("http://controller.internal:8080".to_owned());

        let config = embedded_config(config).expect("embedded defaults are valid");

        assert_eq!(config.infrastructure.storage, ConfigStorageKind::InMemory);
        assert!(
            config
                .infrastructure
                .placement
                .controller_endpoint
                .is_none()
        );
    }

    #[tokio::test]
    async fn embedded_system_nexus_callback_never_falls_through_to_http() {
        let client = InProcessNexusCompletionClient::new();
        let target = system_callback_post_url(EMBEDDED_NEXUS_CALLBACK_BASE);
        let outcome = client
            .complete_operation(
                &target,
                "not-yet-attached",
                "operation-token",
                NexusCompletion::Succeeded(tokeira_types::Payloads::default()),
                &[],
            )
            .await
            .expect("the in-process preflight outcome is classifiable");

        assert!(matches!(
            outcome,
            CompletionDeliveryOutcome::RetryableError { detail }
                if detail == "embedded Nexus completion runtime is unavailable"
        ));
    }

    // Security F1 (apps scope): the network-exposed `/nexus/callback` listener must cap
    // its request body before buffering, so an unauthenticated caller cannot exhaust
    // memory and abort the `panic = "abort"` process.
    #[tokio::test]
    async fn bounded_body_caps_oversized_payload() {
        // Within the cap: fully collected.
        match collect_bounded_body(Full::new(Bytes::from(vec![0u8; 8])), 16).await {
            BoundedBody::Collected(bytes) => assert_eq!(bytes.len(), 8),
            other => panic!("expected Collected, got {other:?}"),
        }
        // Over the cap: rejected before the whole body is buffered.
        assert!(matches!(
            collect_bounded_body(Full::new(Bytes::from(vec![0u8; 17])), 16).await,
            BoundedBody::TooLarge
        ));
    }

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
        assert!(short.contains(tokeira_build_info::SERVER_VERSION));
        assert!(short.contains(tokeira_build_info::TEMPORAL_PROTO_VERSION));
        assert!(short.contains(tokeira_build_info::TEMPORAL_SERVER_COMPAT));
        assert!(json.contains("server_version"));
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
