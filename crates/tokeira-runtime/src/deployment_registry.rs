//! Durable Worker Deployment registry: the namespace-scoped control-plane state
//! machine behind the v2 worker-deployment RPCs (deployment + version CRUD,
//! current/ramping routing selection, compute config, version metadata, manager
//! identity, and drainage).
//!
//! # Placement
//!
//! Temporal implements Worker Deployments as system workflows
//! (`service/worker/workerdeployment/workflow.go @ v1.31.0`). Tokeira deliberately does
//! not port that: holding control-plane correctness in the per-run history of a
//! synthetic workflow would collide with "history is authority" for user runs. Instead
//! the registry is durable single-document state in `tokeira-storage`
//! (`WorkerDeploymentRepository`) guarded by compare-and-swap, with the decision logic
//! living here as pure transition functions — the same split the codebase already uses
//! for shard leases and control/budget rows. It is *not* per-run kernel state.
//!
//! # Mutation discipline
//!
//! Every mutating method runs a load → validate → CAS-commit loop (see
//! [`DeploymentRegistry::mutate_deployment`]): load the record with its current
//! conflict token, evaluate all preconditions against that exact snapshot, then commit
//! conditioned on that token. A lost CAS reloads and re-validates, so a request that a
//! fresher snapshot would reject never leaves a partial mutation behind. This mirrors
//! the optimistic-concurrency model used by `RunRepository::commit_transition`.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt,
    future::Future,
    sync::{Arc, Mutex},
};

use thiserror::Error;
use time::{Duration, OffsetDateTime};
use tokeira_storage::{
    BuildId, ComputeConfig, ComputeConfigScalingGroup, ComputeProvider, ComputeScaler,
    ConflictToken, DeploymentCasResult, DeploymentKey, DeploymentName, DeploymentTaskQueueType,
    DrainageInfo, RoutingConfigUpdateState, RunRepository, StoredRoutingConfig, StoredVersion,
    StoredWorkerDeployment, VersionDrainageStatus, VersionMetadata, VersionTaskQueue,
    WorkerDeploymentRepository, WorkerDeploymentVersionKey, WorkerDeploymentVersionStatus,
};
use tokeira_types::{
    BuildId as RuntimeBuildId, DeploymentId, NamespaceId, Payload, TaskKind, TaskQueueName,
};

use crate::WorkerRegistry;

const MAX_CAS_ATTEMPTS: usize = 32;
const MAX_DEPLOYMENT_PAGE_SIZE: usize = 100;
const MAX_VERSIONS_PER_DEPLOYMENT: usize = 100;
const MAX_TASK_QUEUE_FAMILIES_PER_VERSION: usize = 100;
const ACTIVE_POLLER_WINDOW: Duration = Duration::minutes(5);
const DRAINAGE_VISIBILITY_GRACE_PERIOD: Duration = Duration::minutes(3);
const DRAINAGE_REFRESH_INTERVAL: Duration = Duration::minutes(3);
const VERSION_MEMBERSHIP_CACHE_TTL: Duration = Duration::seconds(1);
const VERSION_REACTIVATION_CACHE_TTL: Duration = Duration::seconds(10);
const TASK_ADD_RATE_WINDOW: Duration = Duration::seconds(30);
/// Reserved version string for unversioned workers; a `version` field carrying this
/// (or empty) resolves to a nil Version rather than a real `(deployment, build_id)`
/// pair (`common/worker_versioning/worker_versioning.go:1103 @ v1.31.0`).
const UNVERSIONED_VERSION_SENTINEL: &str = "__unversioned__";

/// Request-id prefix stamped on deployments and versions created lazily by a
/// versioned worker poll (rather than an explicit `CreateWorkerDeployment`).
///
/// v1.31.0 uses this prefix so a later explicit create that collides with an
/// auto-created deployment can report the "(auto-created from worker polls)"
/// provenance (`service/worker/workerdeployment/util.go:108` +
/// `client.go:1230 @ v1.31.0`).
const AUTO_CREATE_REQUEST_ID_PREFIX: &str = "_auto_create_";

#[cfg(not(feature = "conformance"))]
fn max_versions_per_deployment() -> usize {
    MAX_VERSIONS_PER_DEPLOYMENT
}

#[cfg(feature = "conformance")]
fn max_versions_per_deployment() -> usize {
    tokeira_conformance::overrides()
        .get_i64("matching.maxVersionsInDeployment")
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(MAX_VERSIONS_PER_DEPLOYMENT)
}

#[cfg(not(feature = "conformance"))]
fn max_task_queue_families_per_version() -> usize {
    MAX_TASK_QUEUE_FAMILIES_PER_VERSION
}

#[cfg(feature = "conformance")]
fn max_task_queue_families_per_version() -> usize {
    tokeira_conformance::overrides()
        .get_i64("matching.maxTaskQueuesInDeploymentVersion")
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(MAX_TASK_QUEUE_FAMILIES_PER_VERSION)
}

#[cfg(not(feature = "conformance"))]
fn active_poller_window() -> Duration {
    ACTIVE_POLLER_WINDOW
}

#[cfg(feature = "conformance")]
fn active_poller_window() -> Duration {
    tokeira_conformance::overrides()
        .get_duration("matching.PollerHistoryTTL")
        .and_then(|value| Duration::try_from(value).ok())
        .unwrap_or(ACTIVE_POLLER_WINDOW)
}

#[cfg(not(feature = "conformance"))]
fn drainage_visibility_grace_period() -> Duration {
    DRAINAGE_VISIBILITY_GRACE_PERIOD
}

#[cfg(feature = "conformance")]
fn drainage_visibility_grace_period() -> Duration {
    tokeira_conformance::overrides()
        .get_duration("matching.wv.VersionDrainageStatusVisibilityGracePeriod")
        .and_then(|value| Duration::try_from(value).ok())
        .unwrap_or(DRAINAGE_VISIBILITY_GRACE_PERIOD)
}

#[cfg(not(feature = "conformance"))]
fn drainage_refresh_interval() -> Duration {
    DRAINAGE_REFRESH_INTERVAL
}

#[cfg(not(feature = "conformance"))]
fn version_membership_cache_ttl() -> Duration {
    VERSION_MEMBERSHIP_CACHE_TTL
}

#[cfg(feature = "conformance")]
fn version_membership_cache_ttl() -> Duration {
    tokeira_conformance::overrides()
        .get_duration("history.versionMembershipCacheTTL")
        .and_then(|value| Duration::try_from(value).ok())
        .unwrap_or(VERSION_MEMBERSHIP_CACHE_TTL)
        .max(Duration::seconds(1))
}

#[cfg(not(feature = "conformance"))]
fn version_reactivation_cache_ttl() -> Duration {
    VERSION_REACTIVATION_CACHE_TTL
}

#[cfg(feature = "conformance")]
fn version_reactivation_cache_ttl() -> Duration {
    tokeira_conformance::overrides()
        .get_duration("history.versionReactivationSignalCacheTTL")
        .and_then(|value| Duration::try_from(value).ok())
        .unwrap_or(VERSION_REACTIVATION_CACHE_TTL)
        .max(Duration::seconds(1))
}

#[cfg(not(feature = "conformance"))]
fn version_reactivation_enabled() -> bool {
    false
}

#[cfg(feature = "conformance")]
fn version_reactivation_enabled() -> bool {
    tokeira_conformance::overrides()
        .get_bool("history.enableVersionReactivationSignals")
        .unwrap_or(false)
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct VersionMembershipCacheKey {
    namespace_id: NamespaceId,
    task_queue: String,
    deployment_name: String,
    build_id: String,
}

#[derive(Clone, Copy, Debug)]
struct CachedMembership {
    present: bool,
    expires_at: OffsetDateTime,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct VersionReactivationCacheKey {
    namespace_id: NamespaceId,
    deployment_name: String,
    build_id: String,
}

#[derive(Clone, Copy, Debug)]
enum MissingTaskQueueGuard {
    Current,
    Ramping,
}

#[cfg(feature = "conformance")]
fn drainage_refresh_interval() -> Duration {
    tokeira_conformance::overrides()
        .get_duration("matching.wv.VersionDrainageStatusRefreshInterval")
        .and_then(|value| Duration::try_from(value).ok())
        .unwrap_or(DRAINAGE_REFRESH_INTERVAL)
}

/// Clock used by the registry to stamp caller-visible state transitions.
pub trait RegistryClock: Send + Sync {
    fn now(&self) -> OffsetDateTime;
}

/// Production registry clock backed by the system UTC clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemRegistryClock;

impl RegistryClock for SystemRegistryClock {
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::now_utc()
    }
}

/// Runtime owner for Worker Deployment registry state.
#[derive(Clone)]
pub struct DeploymentRegistry {
    repository: Arc<dyn WorkerDeploymentRepository>,
    run_repository: Option<Arc<dyn RunRepository>>,
    worker_registry: WorkerRegistry,
    clock: Arc<dyn RegistryClock>,
    membership_cache: Arc<Mutex<HashMap<VersionMembershipCacheKey, CachedMembership>>>,
    reactivation_cache: Arc<Mutex<HashMap<VersionReactivationCacheKey, OffsetDateTime>>>,
}

impl DeploymentRegistry {
    pub fn new(
        repository: Arc<dyn WorkerDeploymentRepository>,
        worker_registry: WorkerRegistry,
    ) -> Self {
        Self::with_clock(repository, worker_registry, Arc::new(SystemRegistryClock))
    }

    pub fn with_clock(
        repository: Arc<dyn WorkerDeploymentRepository>,
        worker_registry: WorkerRegistry,
        clock: Arc<dyn RegistryClock>,
    ) -> Self {
        Self {
            repository,
            run_repository: None,
            worker_registry,
            clock,
            membership_cache: Arc::new(Mutex::new(HashMap::new())),
            reactivation_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn with_repositories(
        repository: Arc<dyn WorkerDeploymentRepository>,
        run_repository: Arc<dyn RunRepository>,
        worker_registry: WorkerRegistry,
    ) -> Self {
        Self::with_clock_and_repositories(
            repository,
            run_repository,
            worker_registry,
            Arc::new(SystemRegistryClock),
        )
    }

    pub fn with_clock_and_repositories(
        repository: Arc<dyn WorkerDeploymentRepository>,
        run_repository: Arc<dyn RunRepository>,
        worker_registry: WorkerRegistry,
        clock: Arc<dyn RegistryClock>,
    ) -> Self {
        Self {
            repository,
            run_repository: Some(run_repository),
            worker_registry,
            clock,
            membership_cache: Arc::new(Mutex::new(HashMap::new())),
            reactivation_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn worker_registry(&self) -> &WorkerRegistry {
        &self.worker_registry
    }

    pub fn now(&self) -> OffsetDateTime {
        self.clock.now()
    }

    /// Determine whether a pinned workflow query has no eligible worker.
    ///
    /// A drained version is blackholed only when its workflow task-queue
    /// family has no recent workflow poller. Activity pollers and pollers on a
    /// sibling task queue cannot answer the query. This is the observable
    /// `checkQueryBlackholed` contract from
    /// `service/matching/task_queue_partition_manager.go @ v1.31.0`, derived
    /// here from Tokeira's durable deployment registry plus ephemeral poll
    /// observations rather than a Temporal matching-service topology.
    pub async fn pinned_query_is_blackholed(
        &self,
        namespace_id: NamespaceId,
        task_queue: &TaskQueueName,
        deployment_name: &str,
        build_id: &str,
    ) -> Result<bool, RegistryError> {
        let key = DeploymentKey {
            namespace_id,
            deployment_name: DeploymentName(deployment_name.to_string()),
        };
        self.refresh_due_deployment_drainage(&key).await?;
        let Some(deployment) = self
            .repository
            .load_deployment(&key)
            .await
            .map_err(RegistryError::from_storage_error)?
        else {
            return Ok(false);
        };
        let Some(version) = deployment.versions.get(&BuildId(build_id.to_string())) else {
            return Ok(false);
        };
        if version.status != WorkerDeploymentVersionStatus::Drained {
            return Ok(false);
        }

        Ok(!self
            .worker_registry
            .has_recent_poller_for_deployment_version_on_task_queue(
                namespace_id,
                &DeploymentId(deployment_name.to_string()),
                &RuntimeBuildId(build_id.to_string()),
                Some(task_queue),
                Some(TaskKind::Workflow),
                self.clock.now(),
                active_poller_window(),
            ))
    }

    /// Validate that a pinned version has polled the workflow task-queue family.
    ///
    /// Both positive and negative results are cached. That staleness is part of
    /// the v1.31.0 public behavior: a poll that creates a missing version does
    /// not invalidate an earlier negative membership result
    /// (`common/worker_versioning/version_membership_cache.go @ v1.31.0`).
    pub async fn validate_pinned_workflow_version(
        &self,
        namespace_id: NamespaceId,
        task_queue: &str,
        deployment_name: &str,
        build_id: &str,
    ) -> Result<(), RegistryError> {
        let present = self
            .version_has_workflow_task_queue_with_ttl(
                namespace_id,
                task_queue,
                deployment_name,
                build_id,
                version_membership_cache_ttl(),
            )
            .await?;
        membership_result(
            present,
            deployment_name,
            build_id,
            task_queue_family_name(task_queue),
        )
    }

    /// Return whether a Version owns a workflow task-queue family.
    ///
    /// Continue-as-New uses absence as a normal non-inheritance decision,
    /// unlike explicit pinned admission, which maps it to FAILED_PRECONDITION.
    /// Both paths share v1.31.0's positive/negative membership cache so their
    /// observations have the same staleness window.
    pub(crate) async fn version_has_workflow_task_queue(
        &self,
        namespace_id: NamespaceId,
        task_queue: &str,
        deployment_name: &str,
        build_id: &str,
    ) -> Result<bool, RegistryError> {
        self.version_has_workflow_task_queue_with_ttl(
            namespace_id,
            task_queue,
            deployment_name,
            build_id,
            version_membership_cache_ttl(),
        )
        .await
    }

    async fn version_has_workflow_task_queue_with_ttl(
        &self,
        namespace_id: NamespaceId,
        task_queue: &str,
        deployment_name: &str,
        build_id: &str,
        cache_ttl: Duration,
    ) -> Result<bool, RegistryError> {
        let task_queue = task_queue_family_name(task_queue).to_string();
        let key = VersionMembershipCacheKey {
            namespace_id,
            task_queue: task_queue.clone(),
            deployment_name: deployment_name.to_string(),
            build_id: build_id.to_string(),
        };
        let now = self.clock.now();
        if let Some(cached) = self
            .membership_cache
            .lock()
            .expect("membership cache lock poisoned")
            .get(&key)
            .copied()
            .filter(|cached| cached.expires_at > now)
        {
            return Ok(cached.present);
        }

        let deployment_key = DeploymentKey {
            namespace_id,
            deployment_name: DeploymentName(deployment_name.to_string()),
        };
        let present = self
            .repository
            .load_deployment(&deployment_key)
            .await
            .map_err(RegistryError::from_storage_error)?
            .and_then(|deployment| {
                deployment
                    .versions
                    .get(&BuildId(build_id.to_string()))
                    .cloned()
            })
            .is_some_and(|version| {
                version.polled_task_queues.iter().any(|polled| {
                    polled.name == task_queue
                        && polled.task_queue_type == DeploymentTaskQueueType::Workflow
                })
            });
        self.membership_cache
            .lock()
            .expect("membership cache lock poisoned")
            .insert(
                key,
                CachedMembership {
                    present,
                    expires_at: now + cache_ttl,
                },
            );
        Ok(present)
    }

    #[cfg(test)]
    async fn validate_pinned_workflow_version_with_ttl(
        &self,
        namespace_id: NamespaceId,
        task_queue: &str,
        deployment_name: &str,
        build_id: &str,
        cache_ttl: Duration,
    ) -> Result<(), RegistryError> {
        let present = self
            .version_has_workflow_task_queue_with_ttl(
                namespace_id,
                task_queue,
                deployment_name,
                build_id,
                cache_ttl,
            )
            .await?;
        membership_result(
            present,
            deployment_name,
            build_id,
            task_queue_family_name(task_queue),
        )
    }

    /// Best-effort reactivation of a version targeted by a successful pinned operation.
    ///
    /// The cache check and insertion are atomic, so concurrent starts emit one
    /// logical reactivation within the configured TTL. Tokeira applies that
    /// signal's observable state change directly to its durable deployment
    /// registry rather than introducing Temporal's entity-workflow mechanism.
    pub async fn reactivate_pinned_version(
        &self,
        namespace_id: NamespaceId,
        deployment_name: &str,
        build_id: &str,
    ) -> Result<(), RegistryError> {
        self.reactivate_pinned_version_with_policy(
            namespace_id,
            deployment_name,
            build_id,
            version_reactivation_enabled(),
            version_reactivation_cache_ttl(),
        )
        .await
    }

    async fn reactivate_pinned_version_with_policy(
        &self,
        namespace_id: NamespaceId,
        deployment_name: &str,
        build_id: &str,
        enabled: bool,
        cache_ttl: Duration,
    ) -> Result<(), RegistryError> {
        if !enabled {
            return Ok(());
        }
        let now = self.clock.now();
        let cache_key = VersionReactivationCacheKey {
            namespace_id,
            deployment_name: deployment_name.to_string(),
            build_id: build_id.to_string(),
        };
        {
            let mut cache = self
                .reactivation_cache
                .lock()
                .expect("reactivation cache lock poisoned");
            if cache
                .get(&cache_key)
                .is_some_and(|expires_at| *expires_at > now)
            {
                return Ok(());
            }
            cache.insert(cache_key, now + cache_ttl);
        }

        let deployment_key = DeploymentKey {
            namespace_id,
            deployment_name: DeploymentName(deployment_name.to_string()),
        };
        let build_id = BuildId(build_id.to_string());
        self.mutate_deployment(&deployment_key, |loaded, changed_at| {
            let Some(record) = loaded else {
                return Ok(RegistryMutation::Unchanged(()));
            };
            let Some(version) = record.versions.get(&build_id) else {
                return Ok(RegistryMutation::Unchanged(()));
            };
            if !matches!(
                version.status,
                WorkerDeploymentVersionStatus::Drained | WorkerDeploymentVersionStatus::Inactive
            ) {
                return Ok(RegistryMutation::Unchanged(()));
            }
            let mut next = record.clone();
            let version = next
                .versions
                .get_mut(&build_id)
                .expect("version presence checked above");
            version.status = WorkerDeploymentVersionStatus::Draining;
            version.drainage_info = Some(DrainageInfo {
                status: VersionDrainageStatus::Draining,
                last_changed_time: changed_at,
                last_checked_time: changed_at,
            });
            Ok(RegistryMutation::Put(next, ()))
        })
        .await
        .map(|_| ())
    }

    pub async fn create_deployment(
        &self,
        cmd: CreateDeployment,
    ) -> Result<DeploymentView, RegistryError> {
        let key = cmd.key();
        self.mutate_deployment(&key, |loaded, now| {
            if let Some(record) = loaded {
                // A repeat carrying a previously-seen request_id is an idempotent
                // success no-op; a fresh request_id against an existing name is a real
                // create collision. Matches the request-id dedupe in
                // `workflow_handler.go:185 @ v1.31.0`.
                if request_id_matches(&record.create_request_ids, &cmd.request_id) {
                    return Ok(RegistryMutation::Unchanged(()));
                }
                // A collision against a deployment that a versioned poll auto-created
                // reports distinct provenance, mirroring
                // `ensureWorkerDeploymentDoesNotExist` (`client.go:1228 @ v1.31.0`).
                if is_auto_created(record) {
                    return Err(RegistryError::AlreadyExistsAutoCreated);
                }
                return Err(RegistryError::AlreadyExists);
            }

            Ok(RegistryMutation::Put(
                StoredWorkerDeployment {
                    namespace_id: cmd.namespace_id,
                    name: cmd.deployment_name.clone(),
                    create_time: now,
                    routing_config: StoredRoutingConfig::default(),
                    last_modifier_identity: cmd.identity.clone(),
                    manager_identity: None,
                    routing_config_update_state: RoutingConfigUpdateState::Completed,
                    versions: BTreeMap::new(),
                    conflict_token: ConflictToken::default(),
                    create_request_ids: request_id_set(&cmd.request_id),
                },
                (),
            ))
        })
        .await?;

        self.describe_deployment(key).await
    }

    /// Lazily register the deployment and version implied by a versioned worker
    /// poll, idempotently.
    ///
    /// v1.31.0 creates a Worker Deployment (and its Version) the first time a
    /// versioned worker polls a task queue: matching forwards the poll to the
    /// deployment/version entity workflows, which are created on demand with a
    /// create request id carrying `AUTO_CREATE_REQUEST_ID_PREFIX`
    /// (`service/worker/workerdeployment/client.go:1230 @ v1.31.0`). Tokeira holds
    /// the registry as durable single-document state rather than entity workflows
    /// (see module docs), so this method performs the same lazy registration over
    /// the CAS-guarded record. A re-poll that finds the deployment, version, and
    /// task queue already present is an `Unchanged` no-op so steady-state polling
    /// does not churn the conflict token.
    ///
    /// An unversioned poll (empty deployment name or build id) registers nothing.
    pub async fn register_polled_deployment(
        &self,
        mut cmd: RegisterPolledDeployment,
    ) -> Result<(), RegistryError> {
        if cmd.deployment_name.0.is_empty() || cmd.build_id.0.is_empty() {
            return Ok(());
        }
        // Partition RPC names identify physical queues, but Deployment Version
        // membership is recorded at task-queue-family granularity. Concurrent
        // polls of `/_sys/<family>/<partition>` must therefore collapse onto the
        // root name (`tqid.PartitionFromProto` and `RegisterTaskQueueWorker @
        // v1.31.0`).
        cmd.task_queue = task_queue_family_name(&cmd.task_queue).to_string();
        let key = cmd.key();
        let auto_request_id = format!("{AUTO_CREATE_REQUEST_ID_PREFIX}{}", cmd.deployment_name.0);
        let max_versions = max_versions_per_deployment();
        let max_task_queue_families = max_task_queue_families_per_version();
        let poller_window = active_poller_window();
        self.mutate_deployment(&key, |loaded, now| {
            let task_queue = VersionTaskQueue {
                name: cmd.task_queue.clone(),
                task_queue_type: cmd.task_queue_type,
            };
            match loaded {
                None => {
                    if max_versions == 0 {
                        return Err(version_limit_error(
                            &cmd.deployment_name,
                            &cmd.build_id,
                            max_versions,
                        ));
                    }
                    if max_task_queue_families == 0 {
                        return Err(task_queue_limit_error(
                            &cmd.task_queue,
                            max_task_queue_families,
                        ));
                    }
                    let mut versions = BTreeMap::new();
                    versions.insert(
                        cmd.build_id.clone(),
                        auto_polled_version(&cmd, now, task_queue, &auto_request_id),
                    );
                    Ok(RegistryMutation::Put(
                        StoredWorkerDeployment {
                            namespace_id: cmd.namespace_id,
                            name: cmd.deployment_name.clone(),
                            create_time: now,
                            routing_config: StoredRoutingConfig::default(),
                            last_modifier_identity: cmd.identity.clone(),
                            manager_identity: None,
                            routing_config_update_state: RoutingConfigUpdateState::Completed,
                            versions,
                            conflict_token: ConflictToken::default(),
                            create_request_ids: BTreeSet::from([auto_request_id.clone()]),
                        },
                        (),
                    ))
                }
                Some(record) => {
                    let mut next = record.clone();
                    match next.versions.get_mut(&cmd.build_id) {
                        Some(version) => {
                            // A poller arriving at a version created via the API
                            // (CREATED, no poller yet) promotes it to INACTIVE; record
                            // the freshly-polled task queue. Everything else already
                            // present is a no-op.
                            let mut changed = false;
                            let family_exists = version
                                .polled_task_queues
                                .iter()
                                .any(|registered| registered.name == task_queue.name);
                            if !family_exists
                                && task_queue_family_count(version) >= max_task_queue_families
                            {
                                return Err(task_queue_limit_error(
                                    &cmd.task_queue,
                                    max_task_queue_families,
                                ));
                            }
                            if version.status == WorkerDeploymentVersionStatus::Created {
                                version.status = WorkerDeploymentVersionStatus::Inactive;
                                changed = true;
                            }
                            if version.polled_task_queues.insert(task_queue) {
                                changed = true;
                            }
                            if changed {
                                Ok(RegistryMutation::Put(next, ()))
                            } else {
                                Ok(RegistryMutation::Unchanged(()))
                            }
                        }
                        None => {
                            if next.versions.len() >= max_versions
                                && !try_delete_oldest_eligible_version(
                                    &mut next,
                                    self.worker_registry(),
                                    now,
                                    poller_window,
                                )
                            {
                                return Err(version_limit_error(
                                    &cmd.deployment_name,
                                    &cmd.build_id,
                                    max_versions,
                                ));
                            }
                            if max_task_queue_families == 0 {
                                return Err(task_queue_limit_error(
                                    &cmd.task_queue,
                                    max_task_queue_families,
                                ));
                            }
                            next.versions.insert(
                                cmd.build_id.clone(),
                                auto_polled_version(&cmd, now, task_queue, &auto_request_id),
                            );
                            Ok(RegistryMutation::Put(next, ()))
                        }
                    }
                }
            }
        })
        .await
        .map(|_| ())
    }

    /// Apply a drainage-status update for one version, mirroring the
    /// `sync-drainage-status` signal handled by Temporal's version entity
    /// workflow (`service/worker/workerdeployment/version_workflow.go:119 @ v1.31.0`).
    ///
    /// Tokeira presents the version entity-workflow *surface* (callers can signal
    /// `temporal-sys-worker-deployment-version:<name>.<build>`), but backs it with
    /// registry state rather than a per-run workflow. The signal semantics are
    /// preserved here: a drainage update for a version that is currently Current or
    /// Ramping is ignored (it cannot be draining), and a `Drained` status also moves
    /// the version's lifecycle status to `Drained`. Absent deployment/version is a
    /// no-op, matching the signal's fire-and-forget delivery.
    pub async fn apply_version_drainage(
        &self,
        namespace_id: NamespaceId,
        deployment_name: DeploymentName,
        build_id: BuildId,
        status: VersionDrainageStatus,
    ) -> Result<(), RegistryError> {
        let key = DeploymentKey {
            namespace_id,
            deployment_name,
        };
        self.mutate_deployment(&key, |loaded, now| {
            let Some(record) = loaded else {
                return Ok(RegistryMutation::Unchanged(()));
            };
            if !record.versions.contains_key(&build_id) {
                return Ok(RegistryMutation::Unchanged(()));
            }
            // A Current or Ramping version cannot be draining; the entity workflow
            // drops a late drainage signal in that case (`version_workflow.go:127`).
            let is_routing = matches!(
                &record.routing_config.current_version,
                Some(version) if version.build_id == build_id
            ) || matches!(
                &record.routing_config.ramping_version,
                Some(version) if version.build_id == build_id
            );
            if is_routing {
                return Ok(RegistryMutation::Unchanged(()));
            }

            let mut next = record.clone();
            let version = next
                .versions
                .get_mut(&build_id)
                .expect("version presence checked above");
            let unchanged = version
                .drainage_info
                .as_ref()
                .is_some_and(|info| info.status == status);
            if unchanged {
                return Ok(RegistryMutation::Unchanged(()));
            }
            version.drainage_info = Some(DrainageInfo {
                status,
                last_changed_time: now,
                last_checked_time: now,
            });
            if status == VersionDrainageStatus::Drained {
                version.status = WorkerDeploymentVersionStatus::Drained;
            }
            Ok(RegistryMutation::Put(next, ()))
        })
        .await
        .map(|_| ())
    }

    /// Resolve the Worker Deployment versioning view for one task queue, as
    /// surfaced by `DescribeTaskQueue.versioning_info`.
    ///
    /// Temporal stores this on per-task-queue user data synced from the owning
    /// deployment and recomputes it via `CalculateTaskQueueVersioningInfo`
    /// (`service/matching/task_queue_partition_manager.go:976 @ v1.31.0`). Tokeira
    /// derives the same answer from the registry: a task queue's current/ramping
    /// version is its deployment's current/ramping version *iff that version has
    /// actually polled this task queue*. A current/ramping version that does not
    /// include the task queue routes it to unversioned workers, so it is reported
    /// as nil (which the edge renders as `__unversioned__` for the deprecated
    /// string field). Returns `None` when no deployment version has ever polled
    /// the task queue.
    pub async fn task_queue_versioning(
        &self,
        namespace_id: NamespaceId,
        task_queue: &str,
    ) -> Result<Option<TaskQueueVersioningView>, RegistryError> {
        let deployments = self
            .repository
            .list_all_for_namespace(namespace_id)
            .await
            .map_err(RegistryError::from_storage_error)?;
        // Deterministic order: a task queue is normally owned by a single
        // deployment, but if more than one references it we resolve against the
        // lexicographically-first deployment so the answer is stable across calls.
        let mut deployments = deployments;
        deployments.sort_by(|left, right| left.name.cmp(&right.name));

        for record in &deployments {
            let version_polls_queue = |version: &WorkerDeploymentVersionKey| {
                record
                    .versions
                    .get(&version.build_id)
                    .is_some_and(|stored| {
                        stored
                            .polled_task_queues
                            .iter()
                            .any(|polled| polled.name == task_queue)
                    })
            };
            let any_version_polls = record.versions.values().any(|stored| {
                stored
                    .polled_task_queues
                    .iter()
                    .any(|polled| polled.name == task_queue)
            });
            if !any_version_polls {
                continue;
            }

            let current_version = record
                .routing_config
                .current_version
                .as_ref()
                .filter(|version| version_polls_queue(version))
                .cloned();
            let ramping_version = record
                .routing_config
                .ramping_version
                .as_ref()
                .filter(|version| version_polls_queue(version))
                .cloned();
            // An unversioned ramp diverts traffic from the current version to
            // unversioned workers, so it applies to the task queues served by the
            // current version. Surface it on this queue when the current version
            // polls it.
            let ramping_to_unversioned = record.routing_config.ramping_to_unversioned
                && record
                    .routing_config
                    .current_version
                    .as_ref()
                    .is_some_and(version_polls_queue);
            let ramping_active = ramping_version.is_some() || ramping_to_unversioned;
            let ramping_percentage = if ramping_active {
                record.routing_config.ramping_version_percentage
            } else {
                0.0
            };
            let update_time = [
                record.routing_config.current_version_changed_time,
                ramping_active
                    .then_some(record.routing_config.ramping_version_changed_time)
                    .flatten(),
            ]
            .into_iter()
            .flatten()
            .max();
            return Ok(Some(TaskQueueVersioningView {
                current_version,
                ramping_version,
                ramping_to_unversioned,
                ramping_percentage,
                update_time,
            }));
        }
        Ok(None)
    }

    /// Resolve the durable routing config governing a workflow task queue.
    ///
    /// A workflow that already names a deployment takes that deployment's
    /// routing config. Before the first versioned task starts, the run has no
    /// deployment name, so Tokeira discovers the deployment through the
    /// version/task-queue membership written by worker polls. This keeps queue
    /// selection a derived runtime effect of durable registry state rather than
    /// an edge decision.
    pub(crate) async fn workflow_task_routing_config(
        &self,
        namespace_id: NamespaceId,
        task_queue: &str,
        preferred_deployment: Option<&str>,
    ) -> Result<StoredRoutingConfig, RegistryError> {
        if let Some(deployment_name) = preferred_deployment {
            let key = DeploymentKey {
                namespace_id,
                deployment_name: DeploymentName(deployment_name.to_string()),
            };
            return self
                .repository
                .load_deployment(&key)
                .await
                .map_err(RegistryError::from_storage_error)
                .map(|record| {
                    record
                        .map(|record| record.routing_config)
                        .unwrap_or_default()
                });
        }

        let mut deployments = self
            .repository
            .list_all_for_namespace(namespace_id)
            .await
            .map_err(RegistryError::from_storage_error)?;
        deployments.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(deployments
            .into_iter()
            .find(|deployment| {
                deployment.versions.values().any(|version| {
                    version.polled_task_queues.iter().any(|polled| {
                        polled.name == task_queue
                            && polled.task_queue_type == DeploymentTaskQueueType::Workflow
                    })
                })
            })
            .map(|deployment| deployment.routing_config)
            .unwrap_or_default())
    }

    /// Resolve the durable routing config governing an activity task queue.
    ///
    /// The workflow's deployment is preferred only when one of its versions has
    /// actually polled this activity queue. Otherwise the activity is independent
    /// and follows the deployment registered on its own queue. This is the
    /// observable distinction made while adding an activity to matching
    /// (`service/matching/task_queue_partition_manager.go:getPhysicalQueuesForAdd
    /// @ v1.31.0`), expressed over Tokeira's durable registry rather than a
    /// matching-service-local user-data cache.
    pub(crate) async fn activity_task_routing_config(
        &self,
        namespace_id: NamespaceId,
        task_queue: &str,
        preferred_deployment: Option<&str>,
    ) -> Result<StoredRoutingConfig, RegistryError> {
        let task_queue = task_queue_family_name(task_queue);
        if let Some(deployment_name) = preferred_deployment {
            let key = DeploymentKey {
                namespace_id,
                deployment_name: DeploymentName(deployment_name.to_string()),
            };
            if let Some(record) = self
                .repository
                .load_deployment(&key)
                .await
                .map_err(RegistryError::from_storage_error)?
                && deployment_has_task_queue(&record, task_queue, DeploymentTaskQueueType::Activity)
            {
                return Ok(record.routing_config);
            }
        }

        let mut deployments = self
            .repository
            .list_all_for_namespace(namespace_id)
            .await
            .map_err(RegistryError::from_storage_error)?;
        deployments.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(deployments
            .into_iter()
            .find(|deployment| {
                deployment_has_task_queue(deployment, task_queue, DeploymentTaskQueueType::Activity)
            })
            .map(|deployment| deployment.routing_config)
            .unwrap_or_default())
    }

    /// Whether one concrete deployment version has polled an activity queue.
    ///
    /// A pinned workflow uses its pinned version only for dependent activities.
    /// If this membership is absent, v1.31.0 ignores the pinned directive for
    /// that activity and routes it through the activity queue's current version
    /// (`service/matching/task_queue_partition_manager.go:getPhysicalQueuesForAdd
    /// @ v1.31.0`).
    pub(crate) async fn version_has_activity_task_queue(
        &self,
        namespace_id: NamespaceId,
        task_queue: &str,
        deployment_name: &str,
        build_id: &str,
    ) -> Result<bool, RegistryError> {
        let key = DeploymentKey {
            namespace_id,
            deployment_name: DeploymentName(deployment_name.to_string()),
        };
        Ok(self
            .repository
            .load_deployment(&key)
            .await
            .map_err(RegistryError::from_storage_error)?
            .and_then(|record| record.versions.get(&BuildId(build_id.to_string())).cloned())
            .is_some_and(|version| {
                version.polled_task_queues.iter().any(|polled| {
                    polled.name == task_queue_family_name(task_queue)
                        && polled.task_queue_type == DeploymentTaskQueueType::Activity
                })
            }))
    }

    pub async fn describe_deployment(
        &self,
        key: DeploymentKey,
    ) -> Result<DeploymentView, RegistryError> {
        self.refresh_due_deployment_drainage(&key).await?;
        self.repository
            .load_deployment(&key)
            .await
            .map_err(RegistryError::from_storage_error)?
            .as_ref()
            .map(deployment_view)
            .ok_or(RegistryError::NotFound)
    }

    pub async fn delete_deployment(&self, cmd: DeleteDeployment) -> Result<(), RegistryError> {
        let key = cmd.key();
        // Delete runs its own CAS loop rather than `mutate_deployment` because it issues
        // a `delete_deployment` repository call (record removal) rather than a `put`.
        for _ in 0..MAX_CAS_ATTEMPTS {
            let Some(record) = self
                .repository
                .load_deployment(&key)
                .await
                .map_err(RegistryError::from_storage_error)?
            else {
                // Deleting an absent deployment is a success no-op, not NOT_FOUND
                // (`client.go:1089 @ v1.31.0`).
                return Ok(());
            };

            validate_supplied_conflict_token(cmd.conflict_token, record.conflict_token)?;
            if !record.versions.is_empty() {
                return Err(RegistryError::FailedPrecondition(
                    "worker deployment has versions".to_string(),
                ));
            }

            match self
                .repository
                .delete_deployment(&key, record.conflict_token)
                .await
                .map_err(RegistryError::from_storage_error)?
            {
                DeploymentCasResult::Applied { .. } => return Ok(()),
                // Another writer advanced (or removed) the record between load and
                // delete; reload and re-evaluate against the fresh snapshot.
                DeploymentCasResult::Conflict | DeploymentCasResult::NotFound => continue,
                DeploymentCasResult::AlreadyExists => {
                    return Err(RegistryError::FailedPrecondition(
                        "delete unexpectedly observed an existing create conflict".to_string(),
                    ));
                }
            }
        }

        Err(RegistryError::ResourceExhausted(
            "worker deployment registry resource exhausted".to_string(),
        ))
    }

    pub async fn list_deployments(
        &self,
        cmd: ListDeployments,
    ) -> Result<DeploymentPage, RegistryError> {
        let page_size = clamp_page_size(cmd.page_size);
        let after = decode_page_token(&cmd.next_page_token);
        // Over-fetch by one: the extra record (if present) signals there is another page
        // without a second round trip, and its presence is what produces a non-empty
        // continuation token below.
        let mut records = self
            .repository
            .list_deployments(cmd.namespace_id, after.as_ref(), page_size + 1)
            .await
            .map_err(RegistryError::from_storage_error)?;
        for record in &mut records {
            let key = DeploymentKey {
                namespace_id: record.namespace_id,
                deployment_name: record.name.clone(),
            };
            self.refresh_due_deployment_drainage(&key).await?;
            if let Some(refreshed) = self
                .repository
                .load_deployment(&key)
                .await
                .map_err(RegistryError::from_storage_error)?
            {
                *record = refreshed;
            }
        }
        let mut deployments: Vec<_> = records
            .iter()
            .take(page_size)
            .map(deployment_view)
            .collect();
        let next_page_token = if records.len() > page_size {
            deployments
                .last()
                .map(|deployment| encode_page_token(&deployment.name))
                .unwrap_or_default()
        } else {
            String::new()
        };

        deployments.shrink_to_fit();
        Ok(DeploymentPage {
            deployments,
            next_page_token,
        })
    }

    pub async fn create_version(&self, cmd: CreateVersion) -> Result<(), RegistryError> {
        if cmd.deployment_name.0.is_empty() {
            return Err(RegistryError::InvalidArgument(
                "deployment name cannot be empty".to_string(),
            ));
        }
        if cmd.build_id.0.is_empty() {
            return Err(RegistryError::InvalidArgument(
                "deployment build ID cannot be empty".to_string(),
            ));
        }
        validate_compute_config_shape(&cmd.compute_config)?;

        let key = cmd.key();
        // Default an empty request_id to a generated UUID so the dedupe set always has a
        // stable key, matching the server-side defaulting in `workflow_handler.go:185 @
        // v1.31.0`.
        let effective_request_id = effective_request_id(&cmd.request_id);
        let max_versions = max_versions_per_deployment();
        let poller_window = active_poller_window();
        self.mutate_deployment(&key, |loaded, now| {
            // Versions are only created under an existing parent deployment — Temporal
            // uses plain update (not update-with-start) here, so a missing parent is
            // NOT_FOUND rather than an implicit deployment create
            // (`client.go:1238` + `util.go updateWorkflow @ v1.31.0`).
            let Some(record) = loaded else {
                return Err(RegistryError::NotFound);
            };

            if let Some(existing) = record.versions.get(&cmd.build_id) {
                if request_id_matches(&existing.create_request_ids, &cmd.request_id) {
                    return Ok(RegistryMutation::Unchanged(()));
                }
                return Err(RegistryError::AlreadyExists);
            }

            let mut next = record.clone();
            if next.versions.len() >= max_versions
                && !try_delete_oldest_eligible_version(
                    &mut next,
                    self.worker_registry(),
                    now,
                    poller_window,
                )
            {
                return Err(version_limit_error(
                    &cmd.deployment_name,
                    &cmd.build_id,
                    max_versions,
                ));
            }
            record_deployment_modifier(&mut next, &cmd.identity);
            next.versions.insert(
                cmd.build_id.clone(),
                StoredVersion {
                    build_id: cmd.build_id.clone(),
                    status: WorkerDeploymentVersionStatus::Created,
                    create_time: now,
                    routing_changed_time: None,
                    current_since_time: None,
                    ramping_since_time: None,
                    first_activation_time: None,
                    last_current_time: None,
                    last_deactivation_time: None,
                    ramp_percentage: 0.0,
                    drainage_info: None,
                    metadata: VersionMetadata::default(),
                    compute_config: cmd.compute_config.clone(),
                    last_modifier_identity: cmd.identity.clone(),
                    polled_task_queues: BTreeSet::new(),
                    create_request_ids: request_id_set(&effective_request_id),
                    compute_config_request_ids: BTreeSet::new(),
                },
            );
            Ok(RegistryMutation::Put(next, ()))
        })
        .await
        .map(|_| ())
    }

    pub async fn describe_version(
        &self,
        cmd: DescribeVersion,
    ) -> Result<VersionView, RegistryError> {
        let Some(version) =
            resolve_version_selector(&cmd.deployment_name, cmd.build_id.as_ref(), &cmd.version)?
        else {
            return Err(RegistryError::NotFound);
        };

        let key = DeploymentKey {
            namespace_id: cmd.namespace_id,
            deployment_name: version.deployment_name.clone(),
        };
        self.refresh_due_deployment_drainage(&key).await?;
        let record = self
            .repository
            .load_deployment(&key)
            .await
            .map_err(RegistryError::from_storage_error)?
            .ok_or(RegistryError::NotFound)?;
        let version_record = record
            .versions
            .get(&version.build_id)
            .ok_or(RegistryError::NotFound)?;
        Ok(version_view_with_stats(
            &record.name,
            version_record,
            cmd.report_task_queue_stats,
        ))
    }

    pub async fn delete_version(&self, cmd: DeleteVersion) -> Result<(), RegistryError> {
        let Some(version) =
            resolve_version_selector(&cmd.deployment_name, cmd.build_id.as_ref(), &cmd.version)?
        else {
            return Ok(());
        };

        let key = DeploymentKey {
            namespace_id: cmd.namespace_id,
            deployment_name: version.deployment_name.clone(),
        };
        let poller_window = active_poller_window();
        self.mutate_deployment(&key, |loaded, now| {
            // Deleting an absent deployment or version is a success no-op, not NOT_FOUND
            // (`client.go:1037 @ v1.31.0`). The conflict-token check still runs first so a
            // stale token is rejected even when the target is already gone.
            let Some(record) = loaded else {
                return Ok(RegistryMutation::Unchanged(()));
            };
            validate_supplied_conflict_token(cmd.conflict_token, record.conflict_token)?;
            let Some(version_record) = record.versions.get(&version.build_id) else {
                return Ok(RegistryMutation::Unchanged(()));
            };
            validate_manager_identity(record, &cmd.identity)?;

            validate_version_delete_preconditions(
                record,
                version_record,
                &version,
                cmd.skip_drainage,
                self.worker_registry(),
                now,
                poller_window,
            )?;

            let mut next = record.clone();
            record_deployment_modifier(&mut next, &cmd.identity);
            next.versions.remove(&version.build_id);
            Ok(RegistryMutation::Put(next, ()))
        })
        .await
        .map(|_| ())
    }

    pub async fn set_current_version(
        &self,
        cmd: SetCurrent,
    ) -> Result<SetCurrentOutcome, RegistryError> {
        let key = DeploymentKey {
            namespace_id: cmd.namespace_id,
            deployment_name: cmd.deployment_name.clone(),
        };
        let max_versions = max_versions_per_deployment();
        let poller_window = active_poller_window();
        let registry = self;
        let cmd = &cmd;
        let commit = self
            .mutate_deployment_async(&key, move |loaded, now| async move {
                let synthesized;
                let record = match loaded.as_ref() {
                    Some(record) => {
                        validate_supplied_conflict_token(
                            cmd.conflict_token,
                            record.conflict_token,
                        )?;
                        validate_manager_identity(record, &cmd.identity)?;
                        record
                    }
                    None => {
                        if !cmd.allow_no_pollers {
                            return Err(RegistryError::NotFoundMessage(format!(
                                "no Worker Deployment found with name '{}'; \
                                 does your Worker Deployment have pollers?",
                                cmd.deployment_name.0
                            )));
                        }
                        synthesized = synthesized_no_pollers_deployment(
                            cmd.namespace_id,
                            &cmd.deployment_name,
                            &cmd.identity,
                            now,
                        );
                        &synthesized
                    }
                };

                let target = routing_version_key(&cmd.deployment_name, cmd.build_id.as_ref())?;
                let previous_current = record.routing_config.current_version.clone();
                let previous_current_version = previous_current
                    .as_ref()
                    .map(|version| version.build_id.clone());
                let previous_ramping_version = record
                    .routing_config
                    .ramping_version
                    .as_ref()
                    .map(|version| version.build_id.clone());

                let mut next = record.clone();
                if let Some(target) = &target {
                    ensure_version_available(
                        &mut next,
                        target,
                        cmd.allow_no_pollers,
                        &cmd.identity,
                        now,
                        max_versions,
                        self.worker_registry(),
                        poller_window,
                    )?;
                    if !cmd.ignore_missing_task_queues {
                        registry
                            .validate_missing_task_queues(
                                &next,
                                previous_current.as_ref(),
                                target,
                                MissingTaskQueueGuard::Current,
                                now,
                            )
                            .await?;
                    }
                }
                record_deployment_modifier(&mut next, &cmd.identity);
                next.routing_config.current_version = target.clone();
                next.routing_config.current_version_changed_time = Some(now);
                next.routing_config.revision_number += 1;
                next.routing_config.current_version_revision_number =
                    next.routing_config.revision_number;
                // Promoting the version that is currently ramping to Current implicitly
                // unsets the ramp: a version cannot be both Current and Ramping. Required
                // side effect per the `SetWorkerDeploymentCurrentVersion` RPC doc comment
                // and v1.31.0. Guard on `target.is_some()` so setting an *unversioned*
                // current (target None) with no existing ramp does not spuriously match
                // `None == None` and stamp the ramp-changed timestamps.
                if target.is_some() && next.routing_config.ramping_version == target {
                    next.routing_config.ramping_version = None;
                    next.routing_config.ramping_to_unversioned = false;
                    next.routing_config.ramping_version_percentage = 0.0;
                    next.routing_config.ramping_version_changed_time = Some(now);
                    next.routing_config.ramping_version_percentage_changed_time = Some(now);
                    next.routing_config.ramping_version_revision_number =
                        next.routing_config.revision_number;
                }
                refresh_version_routing_state(&mut next, now);
                Ok(RegistryMutation::Put(
                    next,
                    (previous_current_version, previous_ramping_version),
                ))
            })
            .await?;

        // Do NOT synchronously recompute drainage here. A version demoted out of
        // Current/Ramping is marked `Draining` by `refresh_version_routing_state`;
        // v1.31.0 only transitions `Draining → Drained` later, via the version entity
        // workflow's delayed drainage check / `sync-drainage-status` signal
        // (`version_workflow.go:119 @ v1.31.0`), never synchronously at the routing
        // change. Recomputing here (with no open-pinned workflows just after demotion)
        // would prematurely report `Drained`.
        let deployment = self.describe_deployment(key).await?;
        let (previous_current_version, previous_ramping_version) = commit.value;
        Ok(SetCurrentOutcome {
            conflict_token: deployment.conflict_token,
            deployment,
            previous_current_version,
            previous_ramping_version,
        })
    }

    pub async fn set_ramping_version(
        &self,
        cmd: SetRamping,
    ) -> Result<SetRampingOutcome, RegistryError> {
        if !(0.0..=100.0).contains(&cmd.ramping_percentage) {
            return Err(RegistryError::InvalidArgument(
                "ramping percentage must be in [0, 100]".to_string(),
            ));
        }

        let key = DeploymentKey {
            namespace_id: cmd.namespace_id,
            deployment_name: cmd.deployment_name.clone(),
        };
        let max_versions = max_versions_per_deployment();
        let poller_window = active_poller_window();
        let registry = self;
        let cmd = &cmd;
        let commit = self
            .mutate_deployment_async(&key, move |loaded, now| async move {
                let synthesized;
                let record = match loaded.as_ref() {
                    Some(record) => {
                        validate_supplied_conflict_token(
                            cmd.conflict_token,
                            record.conflict_token,
                        )?;
                        validate_manager_identity(record, &cmd.identity)?;
                        record
                    }
                    None => {
                        if !cmd.allow_no_pollers {
                            return Err(RegistryError::NotFoundMessage(format!(
                                "no Worker Deployment found with name '{}'; \
                                 does your Worker Deployment have pollers?",
                                cmd.deployment_name.0
                            )));
                        }
                        synthesized = synthesized_no_pollers_deployment(
                            cmd.namespace_id,
                            &cmd.deployment_name,
                            &cmd.identity,
                            now,
                        );
                        &synthesized
                    }
                };

                let target = routing_version_key(&cmd.deployment_name, cmd.build_id.as_ref())?;
                // A nil target with a non-zero percentage is a ramp to *unversioned*
                // workers (distinct from "no ramp", which is a nil target at 0%). v1.31.0
                // derives this from an empty build id + percentage > 0
                // (`workflow_handler.go:4018 @ v1.31.0`).
                let to_unversioned = target.is_none() && cmd.ramping_percentage > 0.0;
                // A ramping version must differ from Current. v1.31.0 rejects ramp ==
                // current with FAILED_PRECONDITION (`workflow.go:764 @ v1.31.0`), naming
                // the version; unversioned ramp collides with an unversioned (nil) current.
                if target == record.routing_config.current_version
                    && let Some(version) = target.as_ref()
                {
                    return Err(RegistryError::FailedPrecondition(format!(
                        "requested ramping version {} is already current",
                        format_legacy_version_string(
                            &version.deployment_name,
                            &version.build_id
                        )
                    )));
                }
                if to_unversioned && record.routing_config.current_version.is_none() {
                    return Err(RegistryError::FailedPrecondition(format!(
                        "requested ramping version {UNVERSIONED_VERSION_SENTINEL} is already current"
                    )));
                }

                let previous_ramping_version = record
                    .routing_config
                    .ramping_version
                    .as_ref()
                    .map(|version| version.build_id.clone());
                let previous_ramping_percentage = record.routing_config.ramping_version_percentage;

                let mut next = record.clone();
                let ramping_changed = next.routing_config.ramping_version != target
                    || next.routing_config.ramping_to_unversioned != to_unversioned;
                if let Some(target) = &target {
                    ensure_version_available(
                        &mut next,
                        target,
                        cmd.allow_no_pollers,
                        &cmd.identity,
                        now,
                        max_versions,
                        self.worker_registry(),
                        poller_window,
                    )?;
                    // The missing-task-queue guard runs only when the ramping version
                    // actually changes (a percentage-only adjustment keeps the same
                    // version) and compares against Current, per the
                    // `ignore_missing_task_queues` proto comment and v1.31.0.
                    if ramping_changed && !cmd.ignore_missing_task_queues {
                        registry
                            .validate_missing_task_queues(
                                &next,
                                next.routing_config.current_version.as_ref(),
                                target,
                                MissingTaskQueueGuard::Ramping,
                                now,
                            )
                            .await?;
                    }
                }
                record_deployment_modifier(&mut next, &cmd.identity);
                // Stamp the changed-times independently: a caller may shift only the
                // percentage, only the version, or both, and each timestamp tracks its own
                // field's last change.
                let percentage_changed =
                    next.routing_config.ramping_version_percentage != cmd.ramping_percentage;
                next.routing_config.ramping_version = target;
                next.routing_config.ramping_to_unversioned = to_unversioned;
                next.routing_config.ramping_version_percentage = cmd.ramping_percentage;
                if ramping_changed {
                    next.routing_config.ramping_version_changed_time = Some(now);
                }
                if percentage_changed {
                    next.routing_config.ramping_version_percentage_changed_time = Some(now);
                }
                next.routing_config.revision_number += 1;
                next.routing_config.ramping_version_revision_number =
                    next.routing_config.revision_number;
                refresh_version_routing_state(&mut next, now);
                Ok(RegistryMutation::Put(
                    next,
                    (previous_ramping_version, previous_ramping_percentage),
                ))
            })
            .await?;

        // See the note in `set_current_version`: drainage transitions to `Drained`
        // are delayed (entity-workflow drainage check / `sync-drainage-status` signal),
        // never synchronous at the routing change. A demoted version stays `Draining`.
        let deployment = self.describe_deployment(key).await?;
        let (previous_ramping_version, previous_ramping_percentage) = commit.value;
        Ok(SetRampingOutcome {
            conflict_token: deployment.conflict_token,
            deployment,
            previous_ramping_version,
            previous_ramping_percentage,
        })
    }

    pub async fn set_manager(&self, cmd: SetManager) -> Result<SetManagerOutcome, RegistryError> {
        if cmd.identity.is_empty() {
            return Err(RegistryError::InvalidArgument(
                "identity is required".to_string(),
            ));
        }
        let new_manager_identity = match cmd.new_manager_identity.clone() {
            // Resolve the oneof to a stored Option: an explicit empty string clears the
            // manager (None), `self=true` adopts the caller's identity, and an unset oneof
            // is INVALID_ARGUMENT.
            Some(NewManagerIdentity::ManagerIdentity(value)) if value.is_empty() => None,
            Some(NewManagerIdentity::ManagerIdentity(value)) => Some(value),
            Some(NewManagerIdentity::SelfIdentity) => Some(cmd.identity.clone()),
            None => {
                return Err(RegistryError::InvalidArgument(
                    "new_manager_identity is required".to_string(),
                ));
            }
        };

        let key = DeploymentKey {
            namespace_id: cmd.namespace_id,
            deployment_name: cmd.deployment_name.clone(),
        };
        let commit = self
            .mutate_deployment(&key, |loaded, _now| {
                let Some(record) = loaded else {
                    return Err(RegistryError::NotFound);
                };
                // SetManager is gated only by the conflict token, NOT by the existing
                // manager identity — unlike set-current / set-ramping / delete-version,
                // which `validate_manager_identity` guards. This asymmetry matches v1.31.0
                // (`workflow.go:1177 @ v1.31.0`): the manager can be reassigned by any
                // caller holding a fresh token.
                validate_supplied_conflict_token(cmd.conflict_token, record.conflict_token)?;

                let previous_manager_identity = record.manager_identity.clone();
                let mut next = record.clone();
                next.manager_identity = new_manager_identity.clone();
                record_deployment_modifier(&mut next, &cmd.identity);
                Ok(RegistryMutation::Put(next, previous_manager_identity))
            })
            .await?;

        let deployment = self.describe_deployment(key).await?;
        Ok(SetManagerOutcome {
            conflict_token: commit.conflict_token.unwrap_or(deployment.conflict_token),
            deployment,
            previous_manager_identity: commit.value,
        })
    }

    pub async fn refresh_version_drainage(
        &self,
        namespace_id: NamespaceId,
        deployment_name: DeploymentName,
        build_id: BuildId,
    ) -> Result<VersionView, RegistryError> {
        let key = DeploymentKey {
            namespace_id,
            deployment_name: deployment_name.clone(),
        };
        self.refresh_deployment_drainage_for_versions(&key, BTreeSet::from([build_id.clone()]))
            .await?;
        self.describe_version(DescribeVersion {
            namespace_id,
            deployment_name,
            build_id: Some(build_id),
            version: String::new(),
            report_task_queue_stats: false,
        })
        .await
    }

    pub async fn update_compute_config(
        &self,
        cmd: UpdateComputeConfig,
    ) -> Result<(), RegistryError> {
        let version = required_version_selector(&cmd.deployment_name, cmd.build_id.as_ref())?;
        let key = DeploymentKey {
            namespace_id: cmd.namespace_id,
            deployment_name: version.deployment_name.clone(),
        };
        self.mutate_deployment(&key, |loaded, _now| {
            let Some(record) = loaded else {
                return Err(RegistryError::NotFound);
            };
            let Some(version_record) = record.versions.get(&version.build_id) else {
                return Err(RegistryError::NotFound);
            };
            // Idempotency dedupe is keyed per Version (its own request-id set), so a
            // replayed compute-config update is a no-op even after other Versions changed.
            if request_id_matches(&version_record.compute_config_request_ids, &cmd.request_id) {
                return Ok(RegistryMutation::Unchanged(()));
            }
            validate_compute_config_change(&cmd.updates, &cmd.removals)?;

            let mut next = record.clone();
            {
                let version_record = next
                    .versions
                    .get_mut(&version.build_id)
                    .expect("version present: resolved on the record this was cloned from");
                apply_compute_config_change(
                    &mut version_record.compute_config,
                    &cmd.updates,
                    &cmd.removals,
                )?;
                validate_compute_config_shape(&version_record.compute_config)?;
                if !cmd.request_id.is_empty() {
                    version_record
                        .compute_config_request_ids
                        .insert(cmd.request_id.clone());
                }
                record_version_modifier(version_record, &cmd.identity);
            }
            record_deployment_modifier(&mut next, &cmd.identity);
            Ok(RegistryMutation::Put(next, ()))
        })
        .await
        .map(|_| ())
    }

    pub async fn validate_compute_config(
        &self,
        cmd: ValidateComputeConfig,
    ) -> Result<(), RegistryError> {
        // Validation is pure shape-checking: it deliberately does not load the
        // deployment, so an unknown Version is not NOT_FOUND and stored state is never
        // touched (`workflow_handler.go:258`, `client.go:2037 @ v1.31.0`).
        required_version_selector(&cmd.deployment_name, cmd.build_id.as_ref())?;
        validate_compute_config_change(&cmd.updates, &cmd.removals)?;
        let mut proposed = ComputeConfig::default();
        apply_compute_config_change(&mut proposed, &cmd.updates, &cmd.removals)?;
        validate_compute_config_shape(&proposed)
    }

    pub async fn update_version_metadata(
        &self,
        cmd: UpdateMetadata,
    ) -> Result<VersionMetadataView, RegistryError> {
        validate_metadata_change(&cmd.upsert_entries, &cmd.remove_entries)?;
        let Some(version) =
            resolve_version_selector(&cmd.deployment_name, cmd.build_id.as_ref(), &cmd.version)?
        else {
            return Err(RegistryError::InvalidArgument(
                "deployment_version or version is required".to_string(),
            ));
        };
        let key = DeploymentKey {
            namespace_id: cmd.namespace_id,
            deployment_name: version.deployment_name.clone(),
        };
        let commit = self
            .mutate_deployment(&key, |loaded, _now| {
                let Some(record) = loaded else {
                    return Err(RegistryError::NotFound);
                };
                if !record.versions.contains_key(&version.build_id) {
                    return Err(RegistryError::NotFound);
                }

                let mut next = record.clone();
                let view = {
                    let version_record = next
                        .versions
                        .get_mut(&version.build_id)
                        .expect("version present: resolved on the record this was cloned from");
                    for (key, value) in &cmd.upsert_entries {
                        version_record
                            .metadata
                            .entries
                            .insert(key.clone(), value.clone());
                    }
                    for key in &cmd.remove_entries {
                        version_record.metadata.entries.remove(key);
                    }
                    record_version_modifier(version_record, &cmd.identity);
                    VersionMetadataView {
                        deployment_name: version.deployment_name.clone(),
                        build_id: version.build_id.clone(),
                        metadata: version_record.metadata.clone(),
                        last_modifier_identity: version_record.last_modifier_identity.clone(),
                    }
                };
                record_deployment_modifier(&mut next, &cmd.identity);
                Ok(RegistryMutation::Put(next, view))
            })
            .await?;
        Ok(commit.value)
    }

    async fn refresh_deployment_drainage_for_versions(
        &self,
        key: &DeploymentKey,
        build_ids: BTreeSet<BuildId>,
    ) -> Result<(), RegistryError> {
        if build_ids.is_empty() {
            return Ok(());
        }

        // Read open-pinned-workflow presence from the run repository *before* entering the
        // CAS closure: the closure must stay pure (no I/O), and this snapshot drives the
        // DRAINING-vs-DRAINED decision applied below.
        let mut open_pinned = BTreeMap::new();
        for build_id in &build_ids {
            let version = WorkerDeploymentVersionKey {
                deployment_name: key.deployment_name.clone(),
                build_id: build_id.clone(),
            };
            let has_open = match &self.run_repository {
                Some(repository) => repository
                    .has_open_pinned_workflows(key.namespace_id, &version)
                    .await
                    .map_err(RegistryError::from_storage_error)?,
                None => false,
            };
            open_pinned.insert(build_id.clone(), has_open);
        }

        self.mutate_deployment(key, |loaded, now| {
            let Some(record) = loaded else {
                return Ok(RegistryMutation::Unchanged(()));
            };
            let mut next = record.clone();
            let mut changed = false;
            for (build_id, has_open_pinned) in &open_pinned {
                let Some(version) = next.versions.get_mut(build_id) else {
                    continue;
                };
                // A version that became Current/Ramping again between the open-pinned read
                // and this commit is accepting new workflows, so any drainage info is
                // stale and must be cleared rather than recomputed (drainage_info is never
                // populated while current/ramping).
                if version_is_accepting_new_workflows(version) {
                    if version.drainage_info.take().is_some() {
                        changed = true;
                    }
                    continue;
                }
                if recompute_version_drainage(version, *has_open_pinned, now) {
                    changed = true;
                }
            }
            if changed {
                Ok(RegistryMutation::Put(next, ()))
            } else {
                Ok(RegistryMutation::Unchanged(()))
            }
        })
        .await
        .map(|_| ())
    }

    async fn refresh_due_deployment_drainage(
        &self,
        key: &DeploymentKey,
    ) -> Result<(), RegistryError> {
        let Some(record) = self
            .repository
            .load_deployment(key)
            .await
            .map_err(RegistryError::from_storage_error)?
        else {
            return Ok(());
        };
        let due = due_drainage_build_ids(
            &record,
            self.now(),
            drainage_visibility_grace_period(),
            drainage_refresh_interval(),
        );
        self.refresh_deployment_drainage_for_versions(key, due)
            .await
    }

    /// Reject a routing change only when a target-missing historical task queue still
    /// carries live work for the comparison version.
    ///
    /// Historical membership alone is insufficient: v1.31.0 permits the change when
    /// the queue is idle or has moved to another deployment's current version, and
    /// rejects only backlog/non-zero-add-rate queues that would become unversioned
    /// (`service/worker/workerdeployment/client.go:1822-1926 @ v1.31.0`).
    async fn validate_missing_task_queues(
        &self,
        deployment: &StoredWorkerDeployment,
        comparison_version: Option<&WorkerDeploymentVersionKey>,
        target: &WorkerDeploymentVersionKey,
        guard: MissingTaskQueueGuard,
        now: OffsetDateTime,
    ) -> Result<(), RegistryError> {
        let Some(comparison_version) = comparison_version else {
            return Ok(());
        };
        let Some(comparison) = deployment.versions.get(&comparison_version.build_id) else {
            return Ok(());
        };
        let Some(candidate) = deployment.versions.get(&target.build_id) else {
            return Err(RegistryError::NotFound);
        };
        let missing: Vec<_> = comparison
            .polled_task_queues
            .difference(&candidate.polled_task_queues)
            .cloned()
            .collect();
        if missing.is_empty() {
            return Ok(());
        }

        let namespace_deployments = self
            .repository
            .list_all_for_namespace(deployment.namespace_id)
            .await
            .map_err(RegistryError::from_storage_error)?;
        for task_queue in missing {
            // Assignment to another deployment's Current version means this queue is
            // intentionally moving, rather than being orphaned by the proposed change.
            let moved = namespace_deployments.iter().any(|other| {
                other.name != deployment.name
                    && other
                        .routing_config
                        .current_version
                        .as_ref()
                        .and_then(|current| other.versions.get(&current.build_id))
                        .is_some_and(|current| current.polled_task_queues.contains(&task_queue))
            });
            if moved {
                continue;
            }

            // Production always supplies the run repository. A registry constructed
            // without one (legacy isolated tests) cannot prove a queue idle, so retain
            // the conservative pre-existing behavior and treat it as pressured.
            let has_pressure = match &self.run_repository {
                Some(repository) => repository
                    .has_deployment_task_queue_pressure(
                        deployment.namespace_id,
                        comparison_version,
                        &task_queue,
                        now - TASK_ADD_RATE_WINDOW,
                    )
                    .await
                    .map_err(RegistryError::from_storage_error)?,
                None => true,
            };
            if has_pressure {
                let version =
                    format_external_version_string(&target.deployment_name, &target.build_id);
                let message = match guard {
                    MissingTaskQueueGuard::Current => format!(
                        "proposed current version '{version}' is missing active task queues from the current version; these would become unversioned if it is set as the current version"
                    ),
                    MissingTaskQueueGuard::Ramping => format!(
                        "proposed ramping version '{version}' is missing active task queues from the current version; these would become unversioned if it is set as the ramping version"
                    ),
                };
                return Err(RegistryError::FailedPrecondition(message));
            }
        }

        Ok(())
    }

    /// Core load → validate → CAS-commit loop shared by every mutating method.
    ///
    /// `validate` is a pure closure run against the freshly loaded snapshot (and the
    /// current time): it returns either [`RegistryMutation::Unchanged`] (no write,
    /// e.g. an idempotent replay or a missing-target no-op) or
    /// [`RegistryMutation::Put`] with the next record. The commit is conditioned on the
    /// conflict token observed at load time, so two correctness guarantees fall out:
    ///
    /// - **No mutation on rejection.** If `validate` returns `Err`, the loop returns
    ///   immediately without ever calling `put_deployment`; a rejected request cannot
    ///   leave a partial write.
    /// - **No lost update.** If another writer advanced the token between load and
    ///   commit, the CAS reports `Conflict`/`NotFound`/`AlreadyExists` and the loop
    ///   reloads and re-runs `validate` against the new snapshot, bounded by
    ///   `MAX_CAS_ATTEMPTS` (then `ResourceExhausted`).
    ///
    /// `expected == None` (no record loaded) instructs the storage layer to perform a
    /// create that fails if the record now exists.
    pub async fn mutate_deployment<T, F>(
        &self,
        key: &DeploymentKey,
        mut validate: F,
    ) -> Result<RegistryCommit<T>, RegistryError>
    where
        F: FnMut(
            Option<&StoredWorkerDeployment>,
            OffsetDateTime,
        ) -> Result<RegistryMutation<T>, RegistryError>,
    {
        for _ in 0..MAX_CAS_ATTEMPTS {
            let loaded = self
                .repository
                .load_deployment(key)
                .await
                .map_err(RegistryError::from_storage_error)?;
            let expected = loaded.as_ref().map(|record| record.conflict_token);
            let now = self.now();

            match validate(loaded.as_ref(), now)? {
                RegistryMutation::Unchanged(value) => {
                    return Ok(RegistryCommit {
                        conflict_token: expected,
                        value,
                    });
                }
                RegistryMutation::Put(record, value) => {
                    match self
                        .repository
                        .put_deployment(record, expected)
                        .await
                        .map_err(RegistryError::from_storage_error)?
                    {
                        DeploymentCasResult::Applied { token } => {
                            return Ok(RegistryCommit {
                                conflict_token: Some(token),
                                value,
                            });
                        }
                        // Lost the CAS race: reload and re-validate against the snapshot
                        // the winning writer left behind.
                        DeploymentCasResult::Conflict
                        | DeploymentCasResult::NotFound
                        | DeploymentCasResult::AlreadyExists => continue,
                    }
                }
            }
        }

        Err(RegistryError::ResourceExhausted(
            "worker deployment registry resource exhausted".to_string(),
        ))
    }

    /// Async counterpart to [`Self::mutate_deployment`] for preconditions derived
    /// from other durable repository state.
    ///
    /// The validator receives an owned snapshot so it may await without borrowing a
    /// stack-local load. A lost deployment CAS still reloads and reruns the complete
    /// validator, including its durable-state reads, before attempting another write.
    async fn mutate_deployment_async<T, F, Fut>(
        &self,
        key: &DeploymentKey,
        mut validate: F,
    ) -> Result<RegistryCommit<T>, RegistryError>
    where
        F: FnMut(Option<StoredWorkerDeployment>, OffsetDateTime) -> Fut,
        Fut: Future<Output = Result<RegistryMutation<T>, RegistryError>>,
    {
        for _ in 0..MAX_CAS_ATTEMPTS {
            let loaded = self
                .repository
                .load_deployment(key)
                .await
                .map_err(RegistryError::from_storage_error)?;
            let expected = loaded.as_ref().map(|record| record.conflict_token);
            let now = self.now();

            match validate(loaded, now).await? {
                RegistryMutation::Unchanged(value) => {
                    return Ok(RegistryCommit {
                        conflict_token: expected,
                        value,
                    });
                }
                RegistryMutation::Put(record, value) => {
                    match self
                        .repository
                        .put_deployment(record, expected)
                        .await
                        .map_err(RegistryError::from_storage_error)?
                    {
                        DeploymentCasResult::Applied { token } => {
                            return Ok(RegistryCommit {
                                conflict_token: Some(token),
                                value,
                            });
                        }
                        // The deployment snapshot and every external precondition are
                        // one validation unit. Re-read both after a lost CAS rather than
                        // applying a decision made against stale routing state.
                        DeploymentCasResult::Conflict
                        | DeploymentCasResult::NotFound
                        | DeploymentCasResult::AlreadyExists => continue,
                    }
                }
            }
        }

        Err(RegistryError::ResourceExhausted(
            "worker deployment registry resource exhausted".to_string(),
        ))
    }
}

impl fmt::Debug for DeploymentRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeploymentRegistry")
            .field("repository", &"<worker-deployment-repository>")
            .field("worker_registry", &"<worker-registry>")
            .field("clock", &"<registry-clock>")
            .finish()
    }
}

/// Result of a successful registry CAS helper call.
#[derive(Clone, Debug, PartialEq)]
pub struct RegistryCommit<T> {
    pub conflict_token: Option<ConflictToken>,
    pub value: T,
}

/// Pure validation output consumed by the registry CAS helper.
///
/// `Unchanged` commits no write and returns the loaded token (idempotent replays,
/// missing-target delete no-ops); `Put` carries the next record to commit under CAS.
/// Keeping this distinct from "always write the record back" is what lets idempotent
/// and no-op paths avoid bumping the conflict token.
#[derive(Clone, Debug, PartialEq)]
pub enum RegistryMutation<T> {
    Unchanged(T),
    Put(StoredWorkerDeployment, T),
}

/// Create a Worker Deployment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateDeployment {
    pub namespace_id: NamespaceId,
    pub deployment_name: DeploymentName,
    pub request_id: String,
    pub identity: String,
}

impl CreateDeployment {
    fn key(&self) -> DeploymentKey {
        DeploymentKey {
            namespace_id: self.namespace_id,
            deployment_name: self.deployment_name.clone(),
        }
    }
}

/// Lazily register the deployment/version implied by a versioned worker poll.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisterPolledDeployment {
    pub namespace_id: NamespaceId,
    pub deployment_name: DeploymentName,
    pub build_id: BuildId,
    pub task_queue: String,
    pub task_queue_type: DeploymentTaskQueueType,
    pub identity: String,
}

impl RegisterPolledDeployment {
    fn key(&self) -> DeploymentKey {
        DeploymentKey {
            namespace_id: self.namespace_id,
            deployment_name: self.deployment_name.clone(),
        }
    }
}

/// Worker Deployment versioning view for a single task queue, surfaced by
/// `DescribeTaskQueue.versioning_info`.
///
/// `current_version`/`ramping_version` are `None` when the task queue routes to
/// unversioned workers (no current/ramping version includes it); the edge renders
/// the deprecated string form of a nil current as `__unversioned__` and a nil
/// ramping as empty, matching `task_queue_partition_manager.go:976 @ v1.31.0`.
#[derive(Clone, Debug, PartialEq)]
pub struct TaskQueueVersioningView {
    /// Current version routing this task queue, or `None` for unversioned.
    pub current_version: Option<WorkerDeploymentVersionKey>,
    /// Ramping version routing a portion of this task queue, or `None`.
    pub ramping_version: Option<WorkerDeploymentVersionKey>,
    /// Whether the ramp on this task queue targets unversioned workers (nil
    /// ramping version at a non-zero percentage). Renders the deprecated ramping
    /// string as `__unversioned__` rather than empty.
    pub ramping_to_unversioned: bool,
    /// Percentage of traffic shifted to `ramping_version` (0.0 when not ramping).
    pub ramping_percentage: f32,
    /// Most recent current/ramping routing change time for this task queue.
    pub update_time: Option<OffsetDateTime>,
}

/// Delete a Worker Deployment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteDeployment {
    pub namespace_id: NamespaceId,
    pub deployment_name: DeploymentName,
    pub conflict_token: Option<ConflictToken>,
}

impl DeleteDeployment {
    fn key(&self) -> DeploymentKey {
        DeploymentKey {
            namespace_id: self.namespace_id,
            deployment_name: self.deployment_name.clone(),
        }
    }
}

/// List Worker Deployments in a namespace.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListDeployments {
    pub namespace_id: NamespaceId,
    pub page_size: i32,
    pub next_page_token: String,
}

/// Create one Worker Deployment Version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateVersion {
    pub namespace_id: NamespaceId,
    pub deployment_name: DeploymentName,
    pub build_id: BuildId,
    pub compute_config: ComputeConfig,
    pub request_id: String,
    pub identity: String,
}

impl CreateVersion {
    fn key(&self) -> DeploymentKey {
        DeploymentKey {
            namespace_id: self.namespace_id,
            deployment_name: self.deployment_name.clone(),
        }
    }
}

/// Delete one Worker Deployment Version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteVersion {
    pub namespace_id: NamespaceId,
    pub deployment_name: DeploymentName,
    pub build_id: Option<BuildId>,
    pub version: String,
    pub skip_drainage: bool,
    pub conflict_token: Option<ConflictToken>,
    pub identity: String,
}

/// Describe one Worker Deployment Version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DescribeVersion {
    pub namespace_id: NamespaceId,
    pub deployment_name: DeploymentName,
    pub build_id: Option<BuildId>,
    pub version: String,
    pub report_task_queue_stats: bool,
}

/// Set the current Worker Deployment Version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetCurrent {
    pub namespace_id: NamespaceId,
    pub deployment_name: DeploymentName,
    pub build_id: Option<BuildId>,
    pub conflict_token: Option<ConflictToken>,
    pub identity: String,
    pub allow_no_pollers: bool,
    pub ignore_missing_task_queues: bool,
}

/// Set the ramping Worker Deployment Version.
#[derive(Clone, Debug, PartialEq)]
pub struct SetRamping {
    pub namespace_id: NamespaceId,
    pub deployment_name: DeploymentName,
    pub build_id: Option<BuildId>,
    pub ramping_percentage: f32,
    pub conflict_token: Option<ConflictToken>,
    pub identity: String,
    pub allow_no_pollers: bool,
    pub ignore_missing_task_queues: bool,
}

/// Set or clear the Worker Deployment manager identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetManager {
    pub namespace_id: NamespaceId,
    pub deployment_name: DeploymentName,
    pub new_manager_identity: Option<NewManagerIdentity>,
    pub conflict_token: Option<ConflictToken>,
    pub identity: String,
}

/// New manager identity oneof equivalent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NewManagerIdentity {
    ManagerIdentity(String),
    SelfIdentity,
}

/// One named compute-config scaling-group update.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ComputeConfigScalingGroupUpdate {
    pub scaling_group: ComputeConfigScalingGroup,
    pub update_mask: Vec<String>,
}

/// Update one Worker Deployment Version compute config.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateComputeConfig {
    pub namespace_id: NamespaceId,
    pub deployment_name: DeploymentName,
    pub build_id: Option<BuildId>,
    pub updates: BTreeMap<String, ComputeConfigScalingGroupUpdate>,
    pub removals: BTreeSet<String>,
    pub request_id: String,
    pub identity: String,
}

/// Validate a proposed Worker Deployment Version compute-config change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidateComputeConfig {
    pub namespace_id: NamespaceId,
    pub deployment_name: DeploymentName,
    pub build_id: Option<BuildId>,
    pub updates: BTreeMap<String, ComputeConfigScalingGroupUpdate>,
    pub removals: BTreeSet<String>,
    pub identity: String,
}

/// Update one Worker Deployment Version metadata map.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateMetadata {
    pub namespace_id: NamespaceId,
    pub deployment_name: DeploymentName,
    pub build_id: Option<BuildId>,
    pub version: String,
    pub upsert_entries: BTreeMap<String, Payload>,
    pub remove_entries: BTreeSet<String>,
    pub identity: String,
}

/// Caller-visible Worker Deployment record.
#[derive(Clone, Debug, PartialEq)]
pub struct DeploymentView {
    pub namespace_id: NamespaceId,
    pub name: DeploymentName,
    pub create_time: OffsetDateTime,
    pub routing_config: StoredRoutingConfig,
    pub last_modifier_identity: String,
    pub manager_identity: Option<String>,
    pub routing_config_update_state: RoutingConfigUpdateState,
    pub versions: Vec<VersionView>,
    pub conflict_token: ConflictToken,
}

/// Caller-visible Worker Deployment Version record.
#[derive(Clone, Debug, PartialEq)]
pub struct VersionView {
    pub deployment_name: DeploymentName,
    pub build_id: BuildId,
    pub record: StoredVersion,
    pub task_queues: Vec<VersionTaskQueueView>,
}

/// Caller-visible task-queue view for a Worker Deployment Version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionTaskQueueView {
    pub task_queue: VersionTaskQueue,
    pub poller_count: Option<usize>,
    /// Live matching backlog, present only when the Describe request asked for
    /// task-queue stats.
    pub stats: Option<crate::broker::BrokerBacklogStats>,
    /// The same backlog snapshot grouped by effective priority band.
    pub stats_by_priority_key: Option<crate::PriorityBacklogStats>,
}

/// Page of Worker Deployment summaries.
#[derive(Clone, Debug, PartialEq)]
pub struct DeploymentPage {
    pub deployments: Vec<DeploymentView>,
    pub next_page_token: String,
}

/// Result of setting the current Worker Deployment Version.
#[derive(Clone, Debug, PartialEq)]
pub struct SetCurrentOutcome {
    pub conflict_token: ConflictToken,
    pub deployment: DeploymentView,
    pub previous_current_version: Option<BuildId>,
    pub previous_ramping_version: Option<BuildId>,
}

/// Result of setting the ramping Worker Deployment Version.
#[derive(Clone, Debug, PartialEq)]
pub struct SetRampingOutcome {
    pub conflict_token: ConflictToken,
    pub deployment: DeploymentView,
    pub previous_ramping_version: Option<BuildId>,
    pub previous_ramping_percentage: f32,
}

/// Result of setting or clearing the Worker Deployment manager identity.
#[derive(Clone, Debug, PartialEq)]
pub struct SetManagerOutcome {
    pub conflict_token: ConflictToken,
    pub deployment: DeploymentView,
    pub previous_manager_identity: Option<String>,
}

/// Caller-visible Worker Deployment Version metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionMetadataView {
    pub deployment_name: DeploymentName,
    pub build_id: BuildId,
    pub metadata: VersionMetadata,
    pub last_modifier_identity: String,
}

/// Runtime registry errors mapped by the edge to public RPC statuses.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum RegistryError {
    #[error("worker deployment already exists")]
    AlreadyExists,
    #[error("worker deployment already exists (auto-created from worker polls)")]
    AlreadyExistsAutoCreated,
    #[error("worker deployment not found")]
    NotFound,
    /// NotFound with a v1.31.0-faithful message carrying the deployment name.
    #[error("{0}")]
    NotFoundMessage(String),
    #[error("worker deployment precondition failed: {0}")]
    FailedPrecondition(String),
    #[error("{0}")]
    ResourceExhausted(String),
    #[error("invalid worker deployment argument: {0}")]
    InvalidArgument(String),
}

impl RegistryError {
    fn from_storage_error(error: anyhow::Error) -> Self {
        Self::FailedPrecondition(format!("registry storage operation failed: {error}"))
    }
}

pub fn deployment_view(record: &StoredWorkerDeployment) -> DeploymentView {
    DeploymentView {
        namespace_id: record.namespace_id,
        name: record.name.clone(),
        create_time: record.create_time,
        routing_config: record.routing_config.clone(),
        last_modifier_identity: record.last_modifier_identity.clone(),
        manager_identity: record.manager_identity.clone(),
        routing_config_update_state: record.routing_config_update_state,
        versions: record
            .versions
            .values()
            .map(|version| version_view(&record.name, version))
            .collect(),
        conflict_token: record.conflict_token,
    }
}

pub fn version_view(deployment_name: &DeploymentName, record: &StoredVersion) -> VersionView {
    version_view_with_stats(deployment_name, record, false)
}

pub fn version_view_with_stats(
    deployment_name: &DeploymentName,
    record: &StoredVersion,
    report_task_queue_stats: bool,
) -> VersionView {
    VersionView {
        deployment_name: deployment_name.clone(),
        build_id: record.build_id.clone(),
        record: record.clone(),
        task_queues: record
            .polled_task_queues
            .iter()
            .cloned()
            .map(|task_queue| VersionTaskQueueView {
                task_queue,
                poller_count: report_task_queue_stats.then_some(0),
                stats: report_task_queue_stats.then_some(Default::default()),
                stats_by_priority_key: report_task_queue_stats.then_some(Default::default()),
            })
            .collect(),
    }
}

pub fn format_legacy_version_string(
    deployment_name: &DeploymentName,
    build_id: &BuildId,
) -> String {
    format!("{}.{}", deployment_name.0, build_id.0)
}

/// Format the structured v0.32 deployment-version identity used by current APIs.
///
/// v1.31.0 retains the dot-separated deprecated string field, but public errors
/// from the v0.32 version-management RPCs render `<deployment>:<build>` via
/// `WorkerDeploymentVersionToStringV32` (`service/worker/workerdeployment/util.go
/// @ v1.31.0`). Keeping the formatter separate prevents legacy wire fields from
/// silently changing representation.
pub fn format_external_version_string(
    deployment_name: &DeploymentName,
    build_id: &BuildId,
) -> String {
    format!("{}:{}", deployment_name.0, build_id.0)
}

pub fn resolve_version_selector(
    deployment_name: &DeploymentName,
    build_id: Option<&BuildId>,
    version: &str,
) -> Result<Option<WorkerDeploymentVersionKey>, RegistryError> {
    if let Some(build_id) = build_id {
        if deployment_name.0.is_empty() || build_id.0.is_empty() {
            return Err(RegistryError::InvalidArgument(
                "deployment_name and build_id are required".to_string(),
            ));
        }
        return Ok(Some(WorkerDeploymentVersionKey {
            deployment_name: deployment_name.clone(),
            build_id: build_id.clone(),
        }));
    }

    parse_legacy_version_string(version)
}

pub fn parse_legacy_version_string(
    version: &str,
) -> Result<Option<WorkerDeploymentVersionKey>, RegistryError> {
    // Empty and the `__unversioned__` sentinel both mean "unversioned" (nil Version),
    // not a parse error.
    if version.is_empty() || version == UNVERSIONED_VERSION_SENTINEL {
        return Ok(None);
    }

    // v1.31.0 accepts both `<deployment>.<build_id>` and `<deployment>:<build_id>`;
    // a string with neither delimiter is malformed
    // (`common/worker_versioning/worker_versioning.go:1103 @ v1.31.0`).
    let Some((deployment, build_id)) = version.split_once('.').or_else(|| version.split_once(':'))
    else {
        return Err(RegistryError::InvalidArgument(format!(
            "malformed worker deployment version {version:?}"
        )));
    };

    if deployment.is_empty() || build_id.is_empty() {
        return Err(RegistryError::InvalidArgument(
            "deployment_name and build_id are required".to_string(),
        ));
    }

    Ok(Some(WorkerDeploymentVersionKey {
        deployment_name: DeploymentName(deployment.to_string()),
        build_id: BuildId(build_id.to_string()),
    }))
}

fn routing_version_key(
    deployment_name: &DeploymentName,
    build_id: Option<&BuildId>,
) -> Result<Option<WorkerDeploymentVersionKey>, RegistryError> {
    let Some(build_id) = build_id else {
        return Ok(None);
    };
    if build_id.0.is_empty() {
        return Ok(None);
    }
    if deployment_name.0.is_empty() {
        return Err(RegistryError::InvalidArgument(
            "deployment_name is required".to_string(),
        ));
    }
    Ok(Some(WorkerDeploymentVersionKey {
        deployment_name: deployment_name.clone(),
        build_id: build_id.clone(),
    }))
}

fn task_queue_family_count(version: &StoredVersion) -> usize {
    version
        .polled_task_queues
        .iter()
        .map(|task_queue| task_queue.name.as_str())
        .collect::<BTreeSet<_>>()
        .len()
}

fn version_limit_error(
    deployment_name: &DeploymentName,
    build_id: &BuildId,
    max_versions: usize,
) -> RegistryError {
    RegistryError::ResourceExhausted(format!(
        "cannot add version {} since maximum number of versions ({max_versions}) have been registered in the deployment",
        format_legacy_version_string(deployment_name, build_id)
    ))
}

fn membership_result(
    present: bool,
    deployment_name: &str,
    build_id: &str,
    task_queue: &str,
) -> Result<(), RegistryError> {
    if present {
        return Ok(());
    }
    Err(RegistryError::FailedPrecondition(format!(
        "Pinned version '{deployment_name}:{build_id}' is not present in task queue '{task_queue}' of type 'Workflow'"
    )))
}

fn task_queue_limit_error(task_queue: &str, max_task_queues: usize) -> RegistryError {
    RegistryError::ResourceExhausted(format!(
        "cannot add task queue {task_queue} since maximum number of task queues ({max_task_queues}) have been registered in deployment"
    ))
}

/// Remove the oldest Version eligible for server-initiated maintenance.
///
/// Temporal tries Versions in ascending creation order when an add reaches the
/// configured cap. The internal deletion bypasses manager identity but retains
/// routing, active-poller, and drainage gates (`workflow.go:1485-1504 @ v1.31.0`).
/// Selection and removal happen inside the caller's CAS closure so a racing write
/// causes the entire delete-plus-insert decision to be re-evaluated.
fn try_delete_oldest_eligible_version(
    deployment: &mut StoredWorkerDeployment,
    worker_registry: &WorkerRegistry,
    now: OffsetDateTime,
    poller_window: Duration,
) -> bool {
    let candidate = deployment
        .versions
        .iter()
        .filter_map(|(build_id, version_record)| {
            let version = WorkerDeploymentVersionKey {
                deployment_name: deployment.name.clone(),
                build_id: build_id.clone(),
            };
            validate_version_delete_preconditions(
                deployment,
                version_record,
                &version,
                false,
                worker_registry,
                now,
                poller_window,
            )
            .is_ok()
            .then_some((build_id.clone(), version_record.create_time))
        })
        .min_by(|(left_id, left_time), (right_id, right_time)| {
            left_time
                .cmp(right_time)
                .then_with(|| left_id.cmp(right_id))
        })
        .map(|(build_id, _)| build_id);

    candidate
        .and_then(|build_id| deployment.versions.remove(&build_id))
        .is_some()
}

fn ensure_version_available(
    deployment: &mut StoredWorkerDeployment,
    version: &WorkerDeploymentVersionKey,
    allow_no_pollers: bool,
    identity: &str,
    now: OffsetDateTime,
    max_versions: usize,
    worker_registry: &WorkerRegistry,
    poller_window: Duration,
) -> Result<(), RegistryError> {
    if deployment.name != version.deployment_name {
        return Err(RegistryError::NotFound);
    }
    if deployment.versions.contains_key(&version.build_id) {
        return Ok(());
    }
    // An unknown target build_id is NOT_FOUND unless the caller passed
    // allow_no_pollers, in which case set-current/set-ramping auto-creates the Version
    // (`workflow.go:1230/1244` + `client.go:384 @ v1.31.0`).
    if !allow_no_pollers {
        return Err(RegistryError::NotFound);
    }
    if deployment.versions.len() >= max_versions
        && !try_delete_oldest_eligible_version(deployment, worker_registry, now, poller_window)
    {
        return Err(version_limit_error(
            &version.deployment_name,
            &version.build_id,
            max_versions,
        ));
    }

    deployment.versions.insert(
        version.build_id.clone(),
        StoredVersion {
            build_id: version.build_id.clone(),
            status: WorkerDeploymentVersionStatus::Created,
            create_time: now,
            routing_changed_time: None,
            current_since_time: None,
            ramping_since_time: None,
            first_activation_time: None,
            last_current_time: None,
            last_deactivation_time: None,
            ramp_percentage: 0.0,
            drainage_info: None,
            metadata: VersionMetadata::default(),
            compute_config: ComputeConfig::default(),
            last_modifier_identity: identity.to_string(),
            polled_task_queues: BTreeSet::new(),
            create_request_ids: BTreeSet::new(),
            compute_config_request_ids: BTreeSet::new(),
        },
    );
    Ok(())
}

/// Reconciles each version's status/timestamps with the routing config after a
/// current/ramping change: the routing config is the source of truth, and every
/// version's per-version status is a derived projection of it. A version that was
/// Current/Ramping but is named by neither anymore is demoted to Draining here.
fn refresh_version_routing_state(deployment: &mut StoredWorkerDeployment, now: OffsetDateTime) {
    let current = deployment.routing_config.current_version.clone();
    let ramping = deployment.routing_config.ramping_version.clone();
    let ramping_percentage = deployment.routing_config.ramping_version_percentage;

    for version in deployment.versions.values_mut() {
        let key = WorkerDeploymentVersionKey {
            deployment_name: deployment.name.clone(),
            build_id: version.build_id.clone(),
        };
        if Some(&key) == current.as_ref() {
            mark_version_current(version, now);
        } else if Some(&key) == ramping.as_ref() {
            mark_version_ramping(version, ramping_percentage, now);
        } else if matches!(
            version.status,
            WorkerDeploymentVersionStatus::Current | WorkerDeploymentVersionStatus::Ramping
        ) {
            mark_version_draining(version, now);
        }
    }
}

fn mark_version_current(version: &mut StoredVersion, now: OffsetDateTime) {
    version.status = WorkerDeploymentVersionStatus::Current;
    version.routing_changed_time = Some(now);
    version.current_since_time = Some(now);
    version.ramping_since_time = None;
    // first_activation_time records the *first* time this version ever became active, so
    // it is only seeded once and preserved across later re-activations.
    version.first_activation_time.get_or_insert(now);
    version.last_current_time = Some(now);
    version.last_deactivation_time = None;
    version.ramp_percentage = 0.0;
    // Becoming Current means accepting new workflows again; any prior drainage is void.
    version.drainage_info = None;
}

fn mark_version_ramping(version: &mut StoredVersion, percentage: f32, now: OffsetDateTime) {
    version.status = WorkerDeploymentVersionStatus::Ramping;
    version.routing_changed_time = Some(now);
    version.current_since_time = None;
    version.ramping_since_time.get_or_insert(now);
    version.first_activation_time.get_or_insert(now);
    version.last_deactivation_time = None;
    version.ramp_percentage = percentage;
    version.drainage_info = None;
}

fn mark_version_draining(version: &mut StoredVersion, now: OffsetDateTime) {
    version.status = WorkerDeploymentVersionStatus::Draining;
    version.routing_changed_time = Some(now);
    version.current_since_time = None;
    version.ramping_since_time = None;
    version.last_deactivation_time = Some(now);
    version.ramp_percentage = 0.0;
    // Only seed initial DRAINING state; never overwrite an existing DrainageInfo, whose
    // status/last_changed_time are owned by the open-pinned-workflow recompute path
    // (`recompute_version_drainage`).
    if version.drainage_info.is_none() {
        version.drainage_info = Some(DrainageInfo {
            status: VersionDrainageStatus::Draining,
            last_changed_time: now,
            last_checked_time: now,
        });
    }
}

fn version_is_accepting_new_workflows(version: &StoredVersion) -> bool {
    matches!(
        version.status,
        WorkerDeploymentVersionStatus::Current | WorkerDeploymentVersionStatus::Ramping
    )
}

fn due_drainage_build_ids(
    deployment: &StoredWorkerDeployment,
    now: OffsetDateTime,
    visibility_grace_period: Duration,
    refresh_interval: Duration,
) -> BTreeSet<BuildId> {
    deployment
        .versions
        .iter()
        .filter_map(|(build_id, version)| {
            let info = version.drainage_info.as_ref()?;
            if version.status != WorkerDeploymentVersionStatus::Draining
                || info.status != VersionDrainageStatus::Draining
            {
                return None;
            }
            // v1.31.0 distinguishes the first check by equal changed/checked
            // timestamps: visibility gets one grace period to catch up; later checks
            // use the regular refresh interval (`version_workflow.go:1020-1052`).
            let interval = if info.last_checked_time == info.last_changed_time {
                visibility_grace_period
            } else {
                refresh_interval
            };
            (now - info.last_checked_time >= interval).then_some(build_id.clone())
        })
        .collect()
}

fn recompute_version_drainage(
    version: &mut StoredVersion,
    has_open_pinned_workflows: bool,
    now: OffsetDateTime,
) -> bool {
    // The drainage lifecycle is driven entirely by open pinned workflows: a drained
    // version that still has open pinned workflows is DRAINING, and once none remain it
    // is DRAINED. last_changed_time advances only on a status flip; last_checked_time
    // advances on every recompute so callers can tell a fresh check from a stale one.
    let drainage_status = if has_open_pinned_workflows {
        VersionDrainageStatus::Draining
    } else {
        VersionDrainageStatus::Drained
    };
    let version_status = if has_open_pinned_workflows {
        WorkerDeploymentVersionStatus::Draining
    } else {
        WorkerDeploymentVersionStatus::Drained
    };

    let mut changed = false;
    match version.drainage_info.as_mut() {
        Some(info) => {
            if info.status != drainage_status {
                info.status = drainage_status;
                info.last_changed_time = now;
                changed = true;
            }
            if info.last_checked_time != now {
                info.last_checked_time = now;
                changed = true;
            }
        }
        None => {
            version.drainage_info = Some(DrainageInfo {
                status: drainage_status,
                last_changed_time: now,
                last_checked_time: now,
            });
            changed = true;
        }
    }
    if version.status != version_status {
        version.status = version_status;
        changed = true;
    }
    changed
}

/// Enforces the three v1.31.0 delete-version gates, all `FAILED_PRECONDITION`: a
/// version may not be deleted while it is Current/Ramping, while it is still draining
/// (unless `skip_drainage` bypasses that gate), or while it has a poller seen within the
/// configured active-poller window. Poller presence is read from the live `WorkerRegistry`, so
/// this is the one delete check that depends on transient runtime state rather than the
/// durable record.
fn validate_version_delete_preconditions(
    deployment: &StoredWorkerDeployment,
    version_record: &StoredVersion,
    version: &WorkerDeploymentVersionKey,
    skip_drainage: bool,
    worker_registry: &WorkerRegistry,
    now: OffsetDateTime,
    poller_window: Duration,
) -> Result<(), RegistryError> {
    if deployment.routing_config.current_version.as_ref() == Some(version)
        || deployment.routing_config.ramping_version.as_ref() == Some(version)
    {
        return Err(RegistryError::FailedPrecondition(format!(
            "version '{}' cannot be deleted since it is current or ramping",
            format_external_version_string(&version.deployment_name, &version.build_id)
        )));
    }

    // v1.31.0 checks drainage before poller presence. The ordering is observable
    // when both gates fail, so preserve it (`version_workflow.go @ v1.31.0`).
    if !skip_drainage
        && version_record
            .drainage_info
            .as_ref()
            .is_some_and(|info| info.status != VersionDrainageStatus::Drained)
    {
        return Err(RegistryError::FailedPrecondition(format!(
            "version '{}' cannot be deleted since it is draining",
            format_external_version_string(&version.deployment_name, &version.build_id)
        )));
    }

    if worker_registry.has_recent_poller_for_deployment_version(
        deployment.namespace_id,
        &DeploymentId(version.deployment_name.0.clone()),
        &RuntimeBuildId(version.build_id.0.clone()),
        now,
        poller_window,
    ) {
        return Err(RegistryError::FailedPrecondition(format!(
            "version '{}' cannot be deleted since it has active pollers",
            format_external_version_string(&version.deployment_name, &version.build_id)
        )));
    }

    Ok(())
}

fn required_version_selector(
    deployment_name: &DeploymentName,
    build_id: Option<&BuildId>,
) -> Result<WorkerDeploymentVersionKey, RegistryError> {
    let Some(build_id) = build_id else {
        return Err(RegistryError::InvalidArgument(
            "deployment_version is required".to_string(),
        ));
    };
    if deployment_name.0.is_empty() {
        return Err(RegistryError::InvalidArgument(
            "deployment name cannot be empty".to_string(),
        ));
    }
    if build_id.0.is_empty() {
        return Err(RegistryError::InvalidArgument(
            "deployment build ID cannot be empty".to_string(),
        ));
    }
    Ok(WorkerDeploymentVersionKey {
        deployment_name: deployment_name.clone(),
        build_id: build_id.clone(),
    })
}

fn validate_compute_config_change(
    updates: &BTreeMap<String, ComputeConfigScalingGroupUpdate>,
    removals: &BTreeSet<String>,
) -> Result<(), RegistryError> {
    for name in updates.keys() {
        validate_non_empty_key(name, "compute config scaling group")?;
    }
    for name in removals {
        validate_non_empty_key(name, "compute config scaling group")?;
        if updates.contains_key(name) {
            return Err(RegistryError::InvalidArgument(format!(
                "compute config scaling group {name:?} cannot be both updated and removed"
            )));
        }
    }
    for update in updates.values() {
        for path in &update.update_mask {
            if !is_accepted_compute_config_mask_path(path) {
                return Err(RegistryError::InvalidArgument(format!(
                    "unsupported compute config update mask path {path:?}"
                )));
            }
        }
    }
    Ok(())
}

fn validate_compute_config_shape(compute_config: &ComputeConfig) -> Result<(), RegistryError> {
    let mut catch_all_seen = false;
    let mut task_types = BTreeSet::new();
    for (name, group) in &compute_config.scaling_groups {
        validate_non_empty_key(name, "compute config scaling group")?;
        if group.task_queue_types.is_empty() {
            if catch_all_seen {
                return Err(RegistryError::InvalidArgument(format!(
                    "entry {name}: only one scaling group can have no task types defined"
                )));
            }
            catch_all_seen = true;
        }
        for task_type in &group.task_queue_types {
            if *task_type == DeploymentTaskQueueType::Unspecified {
                return Err(RegistryError::InvalidArgument(format!(
                    "entry {name}: task type undefined not allowed in compute spec"
                )));
            }
            if !task_types.insert(*task_type) {
                return Err(RegistryError::InvalidArgument(format!(
                    "entry {name}: task type {} appears in more than one entry",
                    deployment_task_queue_type_name(*task_type)
                )));
            }
        }

        let provider_type = group
            .provider
            .as_ref()
            .map(|provider| provider.provider_type.as_str())
            .unwrap_or_default();
        if !matches!(
            provider_type,
            "aws-lambda"
                | "aws-ecs"
                | "subprocess"
                | "k8s"
                | "gcp-cloud-run"
                | "test-invoke"
                | "test-worker-set"
        ) {
            return Err(RegistryError::InvalidArgument(format!(
                "entry {name}: invalid compute provider type '{provider_type}'"
            )));
        }
        if matches!(provider_type, "test-invoke" | "test-worker-set")
            && group
                .provider
                .as_ref()
                .and_then(|provider| provider.details.as_ref())
                .is_some_and(|details| {
                    serde_json::from_slice::<serde_json::Value>(&details.data)
                        .ok()
                        .and_then(|value| value.as_object().cloned())
                        .is_some_and(|object| object.contains_key("illegal_field"))
                })
        {
            // The v1.31.0 server delegates provider validation to the WCI package
            // pinned in its go.mod. Its two test providers deliberately reject this
            // field; the functional corpus exercises that public rejection.
            return Err(RegistryError::InvalidArgument(
                "illegal_field found in config".to_string(),
            ));
        }

        if let Some(scaler) = &group.scaler
            && !matches!(scaler.scaler_type.as_str(), "no-sync" | "rate-based")
        {
            return Err(RegistryError::InvalidArgument(format!(
                "entry {name}: invalid scaling algorithm type '{}'",
                scaler.scaler_type
            )));
        }
    }
    Ok(())
}

fn deployment_task_queue_type_name(task_type: DeploymentTaskQueueType) -> &'static str {
    match task_type {
        DeploymentTaskQueueType::Workflow => "Workflow",
        DeploymentTaskQueueType::Activity => "Activity",
        DeploymentTaskQueueType::Nexus => "Nexus",
        DeploymentTaskQueueType::Unspecified => "Unspecified",
    }
}

fn deployment_has_task_queue(
    deployment: &StoredWorkerDeployment,
    task_queue: &str,
    task_queue_type: DeploymentTaskQueueType,
) -> bool {
    deployment.versions.values().any(|version| {
        version
            .polled_task_queues
            .iter()
            .any(|polled| polled.name == task_queue && polled.task_queue_type == task_queue_type)
    })
}

fn task_queue_family_name(task_queue: &str) -> &str {
    let Some(physical) = task_queue.strip_prefix("/_sys/") else {
        return task_queue;
    };
    let Some((family, partition)) = physical.rsplit_once('/') else {
        return task_queue;
    };
    if family.is_empty() || partition.parse::<u32>().is_err() {
        task_queue
    } else {
        family
    }
}

fn apply_compute_config_change(
    compute_config: &mut ComputeConfig,
    updates: &BTreeMap<String, ComputeConfigScalingGroupUpdate>,
    removals: &BTreeSet<String>,
) -> Result<(), RegistryError> {
    validate_compute_config_change(updates, removals)?;
    for (name, update) in updates {
        // A new group is inserted wholesale and its update_mask is ignored; the mask only
        // has meaning for an existing group (selecting which sub-fields to overwrite).
        // Documented proto semantics for ComputeConfigScalingGroupUpdate.
        if !compute_config.scaling_groups.contains_key(name) {
            compute_config
                .scaling_groups
                .insert(name.clone(), update.scaling_group.clone());
            continue;
        }
        // Empty mask on an existing group is a no-op (no fields selected for update).
        if update.update_mask.is_empty() {
            continue;
        }

        let existing = compute_config
            .scaling_groups
            .get_mut(name)
            .expect("scaling group present: validated by validate_compute_config_change");
        for path in &update.update_mask {
            match path.as_str() {
                "task_queue_types" => {
                    existing.task_queue_types = update.scaling_group.task_queue_types.clone();
                }
                "provider" => {
                    existing.provider = update.scaling_group.provider.clone();
                }
                "provider.type" => {
                    provider_mut(existing).provider_type = update
                        .scaling_group
                        .provider
                        .as_ref()
                        .map(|provider| provider.provider_type.clone())
                        .unwrap_or_default();
                }
                "provider.details" => {
                    provider_mut(existing).details = update
                        .scaling_group
                        .provider
                        .as_ref()
                        .and_then(|provider| provider.details.clone());
                }
                "provider.nexus_endpoint" => {
                    provider_mut(existing).nexus_endpoint = update
                        .scaling_group
                        .provider
                        .as_ref()
                        .map(|provider| provider.nexus_endpoint.clone())
                        .unwrap_or_default();
                }
                "scaler" => {
                    existing.scaler = update.scaling_group.scaler.clone();
                }
                "scaler.type" => {
                    scaler_mut(existing).scaler_type = update
                        .scaling_group
                        .scaler
                        .as_ref()
                        .map(|scaler| scaler.scaler_type.clone())
                        .unwrap_or_default();
                }
                "scaler.details" => {
                    scaler_mut(existing).details = update
                        .scaling_group
                        .scaler
                        .as_ref()
                        .and_then(|scaler| scaler.details.clone());
                }
                _ => unreachable!("compute config mask path was validated before apply"),
            }
        }
    }
    for name in removals {
        compute_config.scaling_groups.remove(name);
    }
    Ok(())
}

fn provider_mut(scaling_group: &mut ComputeConfigScalingGroup) -> &mut ComputeProvider {
    scaling_group
        .provider
        .get_or_insert_with(ComputeProvider::default)
}

fn scaler_mut(scaling_group: &mut ComputeConfigScalingGroup) -> &mut ComputeScaler {
    scaling_group
        .scaler
        .get_or_insert_with(ComputeScaler::default)
}

fn is_accepted_compute_config_mask_path(path: &str) -> bool {
    matches!(
        path,
        "task_queue_types"
            | "provider"
            | "provider.type"
            | "provider.details"
            | "provider.nexus_endpoint"
            | "scaler"
            | "scaler.type"
            | "scaler.details"
    )
}

fn validate_metadata_change(
    upserts: &BTreeMap<String, Payload>,
    removals: &BTreeSet<String>,
) -> Result<(), RegistryError> {
    for key in upserts.keys() {
        validate_non_empty_key(key, "metadata key")?;
    }
    for key in removals {
        validate_non_empty_key(key, "metadata key")?;
        if upserts.contains_key(key) {
            return Err(RegistryError::InvalidArgument(format!(
                "metadata key {key:?} cannot be both upserted and removed"
            )));
        }
    }
    Ok(())
}

fn validate_non_empty_key(key: &str, label: &str) -> Result<(), RegistryError> {
    if key.is_empty() {
        Err(RegistryError::InvalidArgument(format!(
            "{label} cannot be empty"
        )))
    } else {
        Ok(())
    }
}

fn effective_request_id(request_id: &str) -> String {
    if request_id.is_empty() {
        uuid::Uuid::new_v4().to_string()
    } else {
        request_id.to_string()
    }
}

/// Whether a deployment record was created lazily by a versioned worker poll
/// rather than an explicit `CreateWorkerDeployment`. v1.31.0 keys this off the
/// stored create request id carrying [`AUTO_CREATE_REQUEST_ID_PREFIX`]
/// (`client.go:1228 @ v1.31.0`).
fn is_auto_created(record: &StoredWorkerDeployment) -> bool {
    record
        .create_request_ids
        .iter()
        .any(|id| id.starts_with(AUTO_CREATE_REQUEST_ID_PREFIX))
}

/// Build the `StoredVersion` for a version auto-created by a poll, seeded with the
/// task queue the worker just polled and the auto-create request id.
/// Build an empty deployment record synthesized on-demand by a
/// set-current/set-ramping call that carries `allow_no_pollers` against a
/// deployment that does not exist yet. v1.31.0 creates the deployment (and the
/// target version) via update-with-start in this case (`client.go:384 @ v1.31.0`),
/// so the caller is not required to have an existing deployment or pollers.
fn synthesized_no_pollers_deployment(
    namespace_id: NamespaceId,
    deployment_name: &DeploymentName,
    identity: &str,
    now: OffsetDateTime,
) -> StoredWorkerDeployment {
    StoredWorkerDeployment {
        namespace_id,
        name: deployment_name.clone(),
        create_time: now,
        routing_config: StoredRoutingConfig::default(),
        last_modifier_identity: identity.to_string(),
        manager_identity: None,
        routing_config_update_state: RoutingConfigUpdateState::Completed,
        versions: BTreeMap::new(),
        conflict_token: ConflictToken::default(),
        create_request_ids: BTreeSet::from([format!(
            "{AUTO_CREATE_REQUEST_ID_PREFIX}{}",
            deployment_name.0
        )]),
    }
}

fn auto_polled_version(
    cmd: &RegisterPolledDeployment,
    now: OffsetDateTime,
    task_queue: VersionTaskQueue,
    auto_request_id: &str,
) -> StoredVersion {
    StoredVersion {
        build_id: cmd.build_id.clone(),
        // A poller has arrived, so the version is INACTIVE (exists, has pollers, not
        // routing), not CREATED (which means "created via API, no poller seen yet")
        // — `WorkerDeploymentVersionStatus` doc @ API v1.62.11.
        status: WorkerDeploymentVersionStatus::Inactive,
        create_time: now,
        routing_changed_time: None,
        current_since_time: None,
        ramping_since_time: None,
        first_activation_time: None,
        last_current_time: None,
        last_deactivation_time: None,
        ramp_percentage: 0.0,
        drainage_info: None,
        metadata: VersionMetadata::default(),
        compute_config: ComputeConfig::default(),
        last_modifier_identity: cmd.identity.clone(),
        polled_task_queues: BTreeSet::from([task_queue]),
        create_request_ids: BTreeSet::from([auto_request_id.to_string()]),
        compute_config_request_ids: BTreeSet::new(),
    }
}

fn request_id_set(request_id: &str) -> BTreeSet<String> {
    if request_id.is_empty() {
        BTreeSet::new()
    } else {
        BTreeSet::from([request_id.to_string()])
    }
}

fn request_id_matches(request_ids: &BTreeSet<String>, request_id: &str) -> bool {
    // An empty request_id never matches a stored one: empty means "no idempotency key
    // supplied", so it can never dedupe against a prior request.
    !request_id.is_empty() && request_ids.contains(request_id)
}

fn record_deployment_modifier(deployment: &mut StoredWorkerDeployment, identity: &str) {
    if !identity.is_empty() {
        deployment.last_modifier_identity = identity.to_string();
    }
}

fn record_version_modifier(version: &mut StoredVersion, identity: &str) {
    if !identity.is_empty() {
        version.last_modifier_identity = identity.to_string();
    }
}

fn validate_supplied_conflict_token(
    supplied: Option<ConflictToken>,
    current: ConflictToken,
) -> Result<(), RegistryError> {
    // A nil/absent supplied token bypasses the check (unconditional write); only a
    // non-nil token that disagrees with the stored generation is a stale write. Matches
    // the `args.ConflictToken != nil` guards in v1.31.0's validateStateBeforeAccepting*.
    if supplied.is_some_and(|token| token != current) {
        Err(RegistryError::FailedPrecondition(
            "conflict token mismatch".to_string(),
        ))
    } else {
        Ok(())
    }
}

/// Gates set-current / set-ramping / delete-version: when a deployment has a
/// `manager_identity`, only that identity may mutate routing or delete versions
/// (v1.31.0 `ErrManagerIdentityMismatch`). An unset manager leaves the deployment open
/// to any caller. SetManager itself is intentionally *not* gated this way (see
/// [`DeploymentRegistry::set_manager`]).
fn validate_manager_identity(
    deployment: &StoredWorkerDeployment,
    identity: &str,
) -> Result<(), RegistryError> {
    if deployment
        .manager_identity
        .as_ref()
        .is_some_and(|manager| manager != identity)
    {
        Err(RegistryError::FailedPrecondition(format!(
            // v1.31.0 `ErrManagerIdentityMismatch` (`util.go:101`), asserted via Contains
            // in the SetManagerIdentity tests.
            "ManagerIdentity '{}' is set and does not match user identity '{}'; \
             to proceed, set your own identity as the ManagerIdentity, remove the \
             ManagerIdentity, or wait for the other client to do so",
            deployment.manager_identity.as_deref().unwrap_or_default(),
            identity
        )))
    } else {
        Ok(())
    }
}

fn clamp_page_size(page_size: i32) -> usize {
    // Non-positive and over-max page sizes are clamped to the server max rather than
    // rejected, matching `workflow_handler.go:4078 @ v1.31.0`.
    match usize::try_from(page_size) {
        Ok(size) if (1..=MAX_DEPLOYMENT_PAGE_SIZE).contains(&size) => size,
        _ => MAX_DEPLOYMENT_PAGE_SIZE,
    }
}

fn encode_page_token(deployment_name: &DeploymentName) -> String {
    deployment_name.0.clone()
}

fn decode_page_token(token: &str) -> Option<DeploymentName> {
    (!token.is_empty()).then(|| DeploymentName(token.to_string()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    #[cfg(feature = "conformance")]
    use std::time::Duration as StdDuration;

    use proptest::prelude::*;
    use time::OffsetDateTime;
    use tokeira_kernel::{
        BasicKernel, Command, Kernel, LoadedRun, StartRequest, TerminateRequest,
        WorkflowIdConflictPolicy, WorkflowIdReusePolicy,
        state::{VersioningOverride, WorkerDeploymentVersionRef},
    };
    use tokeira_storage::{
        CommitResult, ComputeConfig, ComputeConfigScalingGroup, ComputeProvider, ComputeScaler,
        DeploymentTaskQueueType, DrainageInfo, InMemoryStore, StoredVersion, VersionDrainageStatus,
        VersionMetadata, WorkerDeploymentRepository, WorkerDeploymentVersionStatus,
    };
    use tokeira_types::{
        Memo, NamespaceId, Payloads, RequestContext, RequestId, RunId, RunKey, SearchAttributes,
        ShardEpoch, TaskQueueName, WorkerIdentity, WorkflowId, WorkflowType,
    };
    use tokio::task::JoinSet;

    #[cfg(feature = "conformance")]
    use tokeira_conformance::OverrideValue;

    use crate::{WorkerRegistrationKey, WorkerVersionMetadata};

    use super::*;

    #[derive(Debug)]
    struct FixedClock(OffsetDateTime);

    impl RegistryClock for FixedClock {
        fn now(&self) -> OffsetDateTime {
            self.0
        }
    }

    #[derive(Debug)]
    struct AdjustableClock(Mutex<OffsetDateTime>);

    impl AdjustableClock {
        fn advance(&self, duration: Duration) {
            let mut now = self.0.lock().expect("test clock lock poisoned");
            *now += duration;
        }
    }

    impl RegistryClock for AdjustableClock {
        fn now(&self) -> OffsetDateTime {
            *self.0.lock().expect("test clock lock poisoned")
        }
    }

    fn registry_with_store() -> (DeploymentRegistry, Arc<InMemoryStore>) {
        registry_with_store_at(OffsetDateTime::UNIX_EPOCH)
    }

    fn registry_with_store_at(now: OffsetDateTime) -> (DeploymentRegistry, Arc<InMemoryStore>) {
        let store = Arc::new(InMemoryStore::default());
        let registry = registry_for_store_at(store.clone(), now);
        (registry, store)
    }

    fn registry_for_store_at(store: Arc<InMemoryStore>, now: OffsetDateTime) -> DeploymentRegistry {
        let repository: Arc<dyn WorkerDeploymentRepository> = store.clone();
        let run_repository: Arc<dyn RunRepository> = store.clone();
        DeploymentRegistry::with_clock_and_repositories(
            repository,
            run_repository,
            WorkerRegistry::default(),
            Arc::new(FixedClock(now)),
        )
    }

    fn create_cmd(namespace_id: NamespaceId, name: &str, request_id: &str) -> CreateDeployment {
        CreateDeployment {
            namespace_id,
            deployment_name: DeploymentName(name.to_string()),
            request_id: request_id.to_string(),
            identity: "operator-a".to_string(),
        }
    }

    fn delete_cmd(
        namespace_id: NamespaceId,
        name: &str,
        conflict_token: Option<ConflictToken>,
    ) -> DeleteDeployment {
        DeleteDeployment {
            namespace_id,
            deployment_name: DeploymentName(name.to_string()),
            conflict_token,
        }
    }

    fn list_cmd(
        namespace_id: NamespaceId,
        page_size: i32,
        next_page_token: String,
    ) -> ListDeployments {
        ListDeployments {
            namespace_id,
            page_size,
            next_page_token,
        }
    }

    fn create_version_cmd(
        namespace_id: NamespaceId,
        deployment_name: &str,
        build_id: &str,
        request_id: &str,
    ) -> CreateVersion {
        CreateVersion {
            namespace_id,
            deployment_name: DeploymentName(deployment_name.to_string()),
            build_id: BuildId(build_id.to_string()),
            compute_config: ComputeConfig::default(),
            request_id: request_id.to_string(),
            identity: "operator-a".to_string(),
        }
    }

    fn register_polled_cmd(
        namespace_id: NamespaceId,
        deployment_name: &str,
        build_id: &str,
        task_queue: &str,
        task_queue_type: DeploymentTaskQueueType,
    ) -> RegisterPolledDeployment {
        RegisterPolledDeployment {
            namespace_id,
            deployment_name: DeploymentName(deployment_name.to_string()),
            build_id: BuildId(build_id.to_string()),
            task_queue: task_queue.to_string(),
            task_queue_type,
            identity: "worker-a".to_string(),
        }
    }

    fn describe_version_cmd(
        namespace_id: NamespaceId,
        deployment_name: &str,
        build_id: Option<&str>,
        version: &str,
        report_task_queue_stats: bool,
    ) -> DescribeVersion {
        DescribeVersion {
            namespace_id,
            deployment_name: DeploymentName(deployment_name.to_string()),
            build_id: build_id.map(|value| BuildId(value.to_string())),
            version: version.to_string(),
            report_task_queue_stats,
        }
    }

    fn delete_version_cmd(
        namespace_id: NamespaceId,
        deployment_name: &str,
        build_id: Option<&str>,
        version: &str,
        skip_drainage: bool,
    ) -> DeleteVersion {
        DeleteVersion {
            namespace_id,
            deployment_name: DeploymentName(deployment_name.to_string()),
            build_id: build_id.map(|value| BuildId(value.to_string())),
            version: version.to_string(),
            skip_drainage,
            conflict_token: None,
            identity: "operator-a".to_string(),
        }
    }

    fn delete_version_cmd_with_auth(
        namespace_id: NamespaceId,
        deployment_name: &str,
        build_id: Option<&str>,
        conflict_token: Option<ConflictToken>,
        identity: &str,
    ) -> DeleteVersion {
        DeleteVersion {
            namespace_id,
            deployment_name: DeploymentName(deployment_name.to_string()),
            build_id: build_id.map(|value| BuildId(value.to_string())),
            version: String::new(),
            skip_drainage: false,
            conflict_token,
            identity: identity.to_string(),
        }
    }

    fn set_current_cmd(
        namespace_id: NamespaceId,
        deployment_name: &str,
        build_id: Option<&str>,
        conflict_token: Option<ConflictToken>,
    ) -> SetCurrent {
        SetCurrent {
            namespace_id,
            deployment_name: DeploymentName(deployment_name.to_string()),
            build_id: build_id.map(|value| BuildId(value.to_string())),
            conflict_token,
            identity: "operator-a".to_string(),
            allow_no_pollers: false,
            ignore_missing_task_queues: true,
        }
    }

    fn set_ramping_cmd(
        namespace_id: NamespaceId,
        deployment_name: &str,
        build_id: Option<&str>,
        ramping_percentage: f32,
        conflict_token: Option<ConflictToken>,
    ) -> SetRamping {
        SetRamping {
            namespace_id,
            deployment_name: DeploymentName(deployment_name.to_string()),
            build_id: build_id.map(|value| BuildId(value.to_string())),
            ramping_percentage,
            conflict_token,
            identity: "operator-a".to_string(),
            allow_no_pollers: false,
            ignore_missing_task_queues: true,
        }
    }

    fn set_manager_cmd(
        namespace_id: NamespaceId,
        deployment_name: &str,
        new_manager_identity: Option<NewManagerIdentity>,
        conflict_token: Option<ConflictToken>,
        identity: &str,
    ) -> SetManager {
        SetManager {
            namespace_id,
            deployment_name: DeploymentName(deployment_name.to_string()),
            new_manager_identity,
            conflict_token,
            identity: identity.to_string(),
        }
    }

    fn compute_update_cmd(
        namespace_id: NamespaceId,
        deployment_name: &str,
        build_id: Option<&str>,
        request_id: &str,
    ) -> UpdateComputeConfig {
        UpdateComputeConfig {
            namespace_id,
            deployment_name: DeploymentName(deployment_name.to_string()),
            build_id: build_id.map(|value| BuildId(value.to_string())),
            updates: BTreeMap::new(),
            removals: BTreeSet::new(),
            request_id: request_id.to_string(),
            identity: "operator-a".to_string(),
        }
    }

    fn validate_compute_cmd(
        namespace_id: NamespaceId,
        deployment_name: &str,
        build_id: Option<&str>,
    ) -> ValidateComputeConfig {
        ValidateComputeConfig {
            namespace_id,
            deployment_name: DeploymentName(deployment_name.to_string()),
            build_id: build_id.map(|value| BuildId(value.to_string())),
            updates: BTreeMap::new(),
            removals: BTreeSet::new(),
            identity: "operator-a".to_string(),
        }
    }

    fn metadata_cmd(
        namespace_id: NamespaceId,
        deployment_name: &str,
        build_id: Option<&str>,
        version: &str,
    ) -> UpdateMetadata {
        UpdateMetadata {
            namespace_id,
            deployment_name: DeploymentName(deployment_name.to_string()),
            build_id: build_id.map(|value| BuildId(value.to_string())),
            version: version.to_string(),
            upsert_entries: BTreeMap::new(),
            remove_entries: BTreeSet::new(),
            identity: "operator-a".to_string(),
        }
    }

    fn scaling_group(provider_type: &str, scaler_type: &str) -> ComputeConfigScalingGroup {
        ComputeConfigScalingGroup {
            task_queue_types: vec![DeploymentTaskQueueType::Workflow],
            provider: Some(ComputeProvider {
                provider_type: provider_type.to_string(),
                details: Some(Payload::new(format!("provider-{provider_type}"))),
                nexus_endpoint: format!("https://{provider_type}.example.com"),
            }),
            scaler: Some(ComputeScaler {
                scaler_type: scaler_type.to_string(),
                details: Some(Payload::new(format!("scaler-{scaler_type}"))),
            }),
        }
    }

    fn deployment_key(namespace_id: NamespaceId, name: &str) -> DeploymentKey {
        DeploymentKey {
            namespace_id,
            deployment_name: DeploymentName(name.to_string()),
        }
    }

    async fn add_polled_task_queue(
        store: &InMemoryStore,
        namespace_id: NamespaceId,
        deployment_name: &str,
        build_id: &str,
        task_queue: &str,
    ) {
        let key = deployment_key(namespace_id, deployment_name);
        let mut record = store.load_deployment(&key).await.unwrap().unwrap();
        let expected = record.conflict_token;
        record
            .versions
            .get_mut(&BuildId(build_id.to_string()))
            .unwrap()
            .polled_task_queues
            .insert(VersionTaskQueue {
                name: task_queue.to_string(),
                task_queue_type: DeploymentTaskQueueType::Workflow,
            });
        store.put_deployment(record, Some(expected)).await.unwrap();
    }

    #[tokio::test]
    async fn pinned_query_blackhole_requires_matching_workflow_poller() {
        let now = OffsetDateTime::UNIX_EPOCH;
        let (registry, store) = registry_with_store_at(now);
        let namespace_id = NamespaceId::new();
        registry
            .create_deployment(create_cmd(
                namespace_id,
                "deployment-a",
                "create-deployment",
            ))
            .await
            .unwrap();
        registry
            .create_version(create_version_cmd(
                namespace_id,
                "deployment-a",
                "build-a",
                "create-version",
            ))
            .await
            .unwrap();

        let key = deployment_key(namespace_id, "deployment-a");
        let mut deployment = store.load_deployment(&key).await.unwrap().unwrap();
        let expected = deployment.conflict_token;
        let version = deployment
            .versions
            .get_mut(&BuildId("build-a".to_string()))
            .unwrap();
        version.status = WorkerDeploymentVersionStatus::Drained;
        version.drainage_info = Some(DrainageInfo {
            status: VersionDrainageStatus::Drained,
            last_changed_time: now,
            last_checked_time: now,
        });
        store
            .put_deployment(deployment, Some(expected))
            .await
            .unwrap();

        let queue = TaskQueueName("queue-a".to_string());
        assert!(
            registry
                .pinned_query_is_blackholed(namespace_id, &queue, "deployment-a", "build-a")
                .await
                .unwrap()
        );

        for (worker, task_queue, task_kind) in [
            ("activity", "queue-a", TaskKind::Activity),
            ("other-queue", "queue-b", TaskKind::Workflow),
        ] {
            registry.worker_registry().register(
                WorkerRegistrationKey {
                    worker_identity: WorkerIdentity(worker.to_string()),
                    namespace_id,
                    task_queue: TaskQueueName(task_queue.to_string()),
                    task_kind,
                },
                WorkerVersionMetadata {
                    deployment: Some(DeploymentId("deployment-a".to_string())),
                    build_id: Some(RuntimeBuildId("build-a".to_string())),
                    last_seen_at: Some(now),
                },
            );
        }
        assert!(
            registry
                .pinned_query_is_blackholed(namespace_id, &queue, "deployment-a", "build-a")
                .await
                .unwrap()
        );

        registry.worker_registry().register(
            WorkerRegistrationKey {
                worker_identity: WorkerIdentity("workflow".to_string()),
                namespace_id,
                task_queue: queue.clone(),
                task_kind: TaskKind::Workflow,
            },
            WorkerVersionMetadata {
                deployment: Some(DeploymentId("deployment-a".to_string())),
                build_id: Some(RuntimeBuildId("build-a".to_string())),
                last_seen_at: Some(now),
            },
        );
        assert!(
            !registry
                .pinned_query_is_blackholed(namespace_id, &queue, "deployment-a", "build-a")
                .await
                .unwrap()
        );
    }

    async fn seed_open_pinned_workflow(
        store: &InMemoryStore,
        namespace_id: NamespaceId,
        deployment_name: &str,
        build_id: &str,
        task_queue: &str,
        workflow_id: &str,
    ) -> RunKey {
        let run_id = RunId::new();
        let workflow_id_value = workflow_id.to_string();
        let workflow_id = WorkflowId(workflow_id_value.clone());
        let run_key = RunKey::derive(namespace_id, &workflow_id, run_id);
        let start = StartRequest {
            initiator: None,
            run_key,
            namespace_id,
            workflow_id,
            run_id,
            workflow_type: WorkflowType("wf-type".to_string()),
            task_queue: TaskQueueName(task_queue.to_string()),
            deployment: Some(DeploymentId(deployment_name.to_string())),
            build_id: Some(RuntimeBuildId(build_id.to_string())),
            versioning_override: Some(VersioningOverride::Pinned {
                version: WorkerDeploymentVersionRef {
                    deployment_name: deployment_name.to_string(),
                    build_id: build_id.to_string(),
                },
            }),
            workflow_start_delay: None,
            client_cron_schedule: None,
            completion_callbacks: Vec::new(),
            user_metadata: None,
            links: Vec::new(),
            on_conflict_options: None,
            priority: None,
            input: Payloads::default(),
            header: None,
            memo: Memo::default(),
            search_attributes: SearchAttributes::default(),
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: Duration::seconds(10),
            retry_policy: None,
            conflict_policy: WorkflowIdConflictPolicy::Fail,
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            attempt: 1,
            continued_execution_run_id: None,
            first_execution_run_id: Some(run_id),
            parent_run_key: None,
            parent_workflow_id: None,
            parent_run_id: None,
            parent_namespace_id: None,
            parent_namespace_name: None,
            parent_initiated_event_id: 0,
            root_workflow_id: None,
            root_run_id: None,
            original_execution_run_id: Some(run_id),
            continued_failure: None,
            last_completion_result: None,
            first_run_started_at: None,
            request: RequestContext {
                request_id: RequestId(format!("start-{build_id}-{workflow_id_value}")),
                caller_identity: None,
                principal: None,
                received_at: OffsetDateTime::UNIX_EPOCH,
            },
            now: OffsetDateTime::UNIX_EPOCH,
            cron_schedule: None,
            eager_execution_accepted: false,
            reserved_poller_identity: None,
            inherited_versioning_info: None,
        };
        let transition = BasicKernel
            .apply(LoadedRun::Absent, Command::Start(start))
            .unwrap();
        assert!(matches!(
            store
                .commit_transition(run_key, transition, ShardEpoch::ZERO)
                .await
                .unwrap(),
            CommitResult::Applied { .. }
        ));
        run_key
    }

    async fn close_workflow(store: &InMemoryStore, run_key: RunKey, request_id: &str) {
        let loaded = store.load_run(run_key).await.unwrap();
        let terminate = TerminateRequest {
            reason: "drainage-test".to_string(),
            details: None,
            identity: "operator-a".to_string(),
            links: Vec::new(),
            request: RequestContext {
                request_id: RequestId(request_id.to_string()),
                caller_identity: None,
                principal: None,
                received_at: OffsetDateTime::UNIX_EPOCH,
            },
            now: OffsetDateTime::UNIX_EPOCH,
        };
        let transition = BasicKernel
            .apply(loaded, Command::Terminate(terminate))
            .unwrap();
        assert!(matches!(
            store
                .commit_transition(run_key, transition, ShardEpoch::ZERO)
                .await
                .unwrap(),
            CommitResult::Applied { .. }
        ));
    }

    #[derive(Clone, Copy, Debug)]
    enum ManagerPropertyOp {
        SetManager,
        SetCurrent,
        SetRamping,
        DeleteVersion,
    }

    fn manager_property_op(index: u8) -> ManagerPropertyOp {
        match index {
            0 => ManagerPropertyOp::SetManager,
            1 => ManagerPropertyOp::SetCurrent,
            2 => ManagerPropertyOp::SetRamping,
            _ => ManagerPropertyOp::DeleteVersion,
        }
    }

    fn manager_identity_arm(index: u8) -> NewManagerIdentity {
        match index {
            0 => NewManagerIdentity::ManagerIdentity("next-manager".to_string()),
            1 => NewManagerIdentity::ManagerIdentity(String::new()),
            _ => NewManagerIdentity::SelfIdentity,
        }
    }

    #[derive(Clone, Debug)]
    enum MetadataOp {
        Upsert { key_index: u8, value_index: u8 },
        Remove { key_index: u8 },
        Overlap { key_index: u8 },
    }

    fn arb_metadata_op() -> impl Strategy<Value = MetadataOp> {
        prop_oneof![
            (0u8..5, 0u8..20).prop_map(|(key_index, value_index)| MetadataOp::Upsert {
                key_index,
                value_index,
            }),
            (0u8..5).prop_map(|key_index| MetadataOp::Remove { key_index }),
            (0u8..5).prop_map(|key_index| MetadataOp::Overlap { key_index }),
        ]
    }

    fn metadata_key(index: u8) -> String {
        format!("key-{index}")
    }

    fn metadata_payload(index: u8) -> Payload {
        Payload::new(format!("value-{index}"))
    }

    #[derive(Clone, Debug)]
    enum ComputeConfigOp {
        Update { group_index: u8, mask_index: u8 },
        Remove { group_index: u8 },
        InvalidOverlap { group_index: u8 },
        Validate { group_index: u8, invalid_mask: bool },
    }

    fn arb_compute_config_op() -> impl Strategy<Value = ComputeConfigOp> {
        prop_oneof![
            (0u8..4, 0u8..5).prop_map(|(group_index, mask_index)| {
                ComputeConfigOp::Update {
                    group_index,
                    mask_index,
                }
            }),
            (0u8..4).prop_map(|group_index| ComputeConfigOp::Remove { group_index }),
            (0u8..4).prop_map(|group_index| ComputeConfigOp::InvalidOverlap { group_index }),
            (0u8..4, any::<bool>()).prop_map(|(group_index, invalid_mask)| {
                ComputeConfigOp::Validate {
                    group_index,
                    invalid_mask,
                }
            }),
        ]
    }

    fn compute_group_name(index: u8) -> String {
        format!("group-{index}")
    }

    fn compute_update(
        group: ComputeConfigScalingGroup,
        mask_index: u8,
    ) -> ComputeConfigScalingGroupUpdate {
        let update_mask = match mask_index {
            0 => Vec::new(),
            1 => vec!["provider.type".to_string()],
            2 => vec!["provider".to_string()],
            3 => vec!["scaler".to_string(), "task_queue_types".to_string()],
            _ => vec!["provider.details".to_string(), "scaler.details".to_string()],
        };
        ComputeConfigScalingGroupUpdate {
            scaling_group: group,
            update_mask,
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum CasMutationKind {
        SetCurrent,
        SetRamping,
        SetManager,
    }

    #[derive(Clone, Copy, Debug)]
    enum CasTokenMode {
        Nil,
        Current,
        Stale,
    }

    fn cas_token(mode: CasTokenMode, current: ConflictToken) -> Option<ConflictToken> {
        match mode {
            CasTokenMode::Nil => None,
            CasTokenMode::Current => Some(current),
            CasTokenMode::Stale => Some(ConflictToken::from_generation(current.generation() + 100)),
        }
    }

    #[derive(Clone, Debug)]
    enum RoutingOp {
        SetCurrent { target_index: u8 },
        SetRamping { target_index: u8, percentage: f32 },
    }

    fn arb_routing_op() -> impl Strategy<Value = RoutingOp> {
        prop_oneof![
            (0u8..4).prop_map(|target_index| RoutingOp::SetCurrent { target_index }),
            (0u8..4, -10i32..111i32).prop_map(|(target_index, percentage)| {
                RoutingOp::SetRamping {
                    target_index,
                    percentage: percentage as f32,
                }
            }),
        ]
    }

    fn routing_target(target_index: u8) -> Option<String> {
        (target_index < 3).then(|| model_build_id(target_index))
    }

    #[derive(Clone, Debug)]
    enum VersionCrudOp {
        Create {
            deployment_index: u8,
            build_index: u8,
            request_index: u8,
        },
        Describe {
            deployment_index: u8,
            build_index: u8,
            report_stats: bool,
        },
        Delete {
            deployment_index: u8,
            build_index: u8,
            precondition: VersionDeletePrecondition,
        },
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum VersionDeletePrecondition {
        None,
        Current,
        Ramping,
        Draining,
    }

    #[derive(Clone, Debug, Default)]
    struct ModelVersion {
        create_request_ids: BTreeSet<String>,
    }

    fn arb_version_crud_op() -> impl Strategy<Value = VersionCrudOp> {
        prop_oneof![
            (0u8..2, 0u8..5, 0u8..4).prop_map(|(deployment_index, build_index, request_index)| {
                VersionCrudOp::Create {
                    deployment_index,
                    build_index,
                    request_index,
                }
            },),
            (0u8..2, 0u8..5, any::<bool>()).prop_map(
                |(deployment_index, build_index, report_stats)| VersionCrudOp::Describe {
                    deployment_index,
                    build_index,
                    report_stats,
                },
            ),
            (0u8..2, 0u8..5, 0u8..4).prop_map(
                |(deployment_index, build_index, precondition_index)| {
                    let precondition = match precondition_index {
                        0 => VersionDeletePrecondition::None,
                        1 => VersionDeletePrecondition::Current,
                        2 => VersionDeletePrecondition::Ramping,
                        _ => VersionDeletePrecondition::Draining,
                    };
                    VersionCrudOp::Delete {
                        deployment_index,
                        build_index,
                        precondition,
                    }
                },
            ),
        ]
    }

    async fn prepare_version_delete_precondition(
        store: &InMemoryStore,
        namespace_id: NamespaceId,
        deployment_name: &str,
        build_id: &str,
        precondition: VersionDeletePrecondition,
    ) {
        let key = deployment_key(namespace_id, deployment_name);
        let Some(mut record) = store.load_deployment(&key).await.unwrap() else {
            return;
        };
        if !record.versions.contains_key(&BuildId(build_id.to_string())) {
            return;
        }

        let expected = record.conflict_token;
        let version_key = WorkerDeploymentVersionKey {
            deployment_name: DeploymentName(deployment_name.to_string()),
            build_id: BuildId(build_id.to_string()),
        };
        record.routing_config.current_version = None;
        record.routing_config.ramping_version = None;
        let version = record.versions.get_mut(&version_key.build_id).unwrap();
        version.status = WorkerDeploymentVersionStatus::Created;
        version.drainage_info = None;
        match precondition {
            VersionDeletePrecondition::None => {}
            VersionDeletePrecondition::Current => {
                record.routing_config.current_version = Some(version_key);
            }
            VersionDeletePrecondition::Ramping => {
                record.routing_config.ramping_version = Some(version_key);
            }
            VersionDeletePrecondition::Draining => {
                version.status = WorkerDeploymentVersionStatus::Draining;
                version.drainage_info = Some(DrainageInfo {
                    status: VersionDrainageStatus::Draining,
                    last_changed_time: OffsetDateTime::UNIX_EPOCH,
                    last_checked_time: OffsetDateTime::UNIX_EPOCH,
                });
            }
        }
        store.put_deployment(record, Some(expected)).await.unwrap();
    }

    #[derive(Clone, Debug)]
    enum DeploymentCrudOp {
        Create {
            name_index: u8,
            request_index: u8,
        },
        Describe {
            name_index: u8,
        },
        Delete {
            name_index: u8,
            token_mode: DeleteTokenMode,
        },
        InjectVersion {
            name_index: u8,
            build_index: u8,
        },
    }

    #[derive(Clone, Copy, Debug)]
    enum DeleteTokenMode {
        None,
        Current,
        Stale,
    }

    #[derive(Clone, Debug, Default)]
    struct ModelDeployment {
        create_request_ids: BTreeSet<String>,
        build_ids: BTreeSet<String>,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum RegistryErrorKind {
        AlreadyExists,
        NotFound,
        FailedPrecondition,
        ResourceExhausted,
        InvalidArgument,
    }

    fn arb_deployment_crud_op() -> impl Strategy<Value = DeploymentCrudOp> {
        prop_oneof![
            (0u8..5, 0u8..4).prop_map(|(name_index, request_index)| {
                DeploymentCrudOp::Create {
                    name_index,
                    request_index,
                }
            }),
            (0u8..5).prop_map(|name_index| DeploymentCrudOp::Describe { name_index }),
            (0u8..5, 0u8..3).prop_map(|(name_index, token_index)| {
                let token_mode = match token_index {
                    0 => DeleteTokenMode::None,
                    1 => DeleteTokenMode::Current,
                    _ => DeleteTokenMode::Stale,
                };
                DeploymentCrudOp::Delete {
                    name_index,
                    token_mode,
                }
            }),
            (0u8..5, 0u8..3).prop_map(|(name_index, build_index)| {
                DeploymentCrudOp::InjectVersion {
                    name_index,
                    build_index,
                }
            }),
        ]
    }

    fn model_deployment_name(index: u8) -> String {
        format!("deployment-{index}")
    }

    fn model_request_id(index: u8) -> String {
        match index {
            0 => String::new(),
            _ => format!("request-{index}"),
        }
    }

    fn model_build_id(index: u8) -> String {
        format!("build-{index}")
    }

    fn registry_error_kind(error: &RegistryError) -> RegistryErrorKind {
        match error {
            RegistryError::AlreadyExists => RegistryErrorKind::AlreadyExists,
            RegistryError::AlreadyExistsAutoCreated => RegistryErrorKind::AlreadyExists,
            RegistryError::NotFound => RegistryErrorKind::NotFound,
            RegistryError::NotFoundMessage(_) => RegistryErrorKind::NotFound,
            RegistryError::FailedPrecondition(_) => RegistryErrorKind::FailedPrecondition,
            RegistryError::ResourceExhausted(_) => RegistryErrorKind::ResourceExhausted,
            RegistryError::InvalidArgument(_) => RegistryErrorKind::InvalidArgument,
        }
    }

    async fn inject_model_version(
        store: &InMemoryStore,
        namespace_id: NamespaceId,
        deployment_name: &str,
        build_id: &str,
    ) {
        let key = deployment_key(namespace_id, deployment_name);
        let Some(mut record) = store.load_deployment(&key).await.unwrap() else {
            return;
        };
        let expected = record.conflict_token;
        record
            .versions
            .insert(BuildId(build_id.to_string()), stored_version(build_id));
        store.put_deployment(record, Some(expected)).await.unwrap();
    }

    async fn current_delete_token(
        registry: &DeploymentRegistry,
        namespace_id: NamespaceId,
        deployment_name: &str,
        token_mode: DeleteTokenMode,
    ) -> Option<ConflictToken> {
        match token_mode {
            DeleteTokenMode::None => None,
            DeleteTokenMode::Current => registry
                .describe_deployment(deployment_key(namespace_id, deployment_name))
                .await
                .ok()
                .map(|deployment| deployment.conflict_token),
            DeleteTokenMode::Stale => Some(ConflictToken::from_generation(u64::MAX)),
        }
    }

    async fn assert_deployment_model_matches_registry(
        registry: &DeploymentRegistry,
        namespace_id: NamespaceId,
        model: &BTreeMap<String, ModelDeployment>,
        seen_names: &BTreeSet<String>,
    ) -> Result<(), proptest::test_runner::TestCaseError> {
        for name in seen_names {
            let described = registry
                .describe_deployment(deployment_key(namespace_id, name))
                .await;
            match model.get(name) {
                Some(expected) => {
                    let described = described.map_err(|error| {
                        TestCaseError::fail(format!(
                            "expected deployment {name:?}, got error {error:?}"
                        ))
                    })?;
                    prop_assert_eq!(described.name, DeploymentName(name.clone()));
                    let actual_build_ids: BTreeSet<_> = described
                        .versions
                        .iter()
                        .map(|version| version.build_id.0.clone())
                        .collect();
                    prop_assert_eq!(&actual_build_ids, &expected.build_ids);
                }
                None => {
                    let error = described.unwrap_err();
                    prop_assert_eq!(registry_error_kind(&error), RegistryErrorKind::NotFound);
                }
            }
        }
        Ok(())
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        // Feature: worker-deployments, Property 19: task-queue admission counts queue
        // families independent of type, and cap recovery evicts the oldest Version that
        // satisfies every ordinary deletion precondition.
        #[test]
        fn property_poll_admission_limits_and_oldest_eligible_eviction(
            queue_observations in proptest::collection::vec((0u8..12, any::<bool>()), 0..50),
            eligibility in proptest::collection::vec(any::<bool>(), 1..24)
                .prop_filter("at least one Version must be eligible", |values| values.iter().any(|value| *value)),
        ) {
            let mut queue_version = stored_version("queue-build");
            for (family, workflow) in &queue_observations {
                queue_version.polled_task_queues.insert(VersionTaskQueue {
                    name: format!("queue-{family}"),
                    task_queue_type: if *workflow {
                        DeploymentTaskQueueType::Workflow
                    } else {
                        DeploymentTaskQueueType::Activity
                    },
                });
            }
            let expected_families = queue_observations
                .iter()
                .map(|(family, _)| family)
                .collect::<BTreeSet<_>>()
                .len();
            prop_assert_eq!(task_queue_family_count(&queue_version), expected_families);

            let namespace_id = NamespaceId::new();
            let deployment_name = DeploymentName("deployment-a".to_string());
            let mut deployment = StoredWorkerDeployment {
                namespace_id,
                name: deployment_name.clone(),
                create_time: OffsetDateTime::UNIX_EPOCH,
                routing_config: StoredRoutingConfig::default(),
                last_modifier_identity: "operator-a".to_string(),
                manager_identity: None,
                routing_config_update_state: RoutingConfigUpdateState::Completed,
                versions: BTreeMap::new(),
                conflict_token: ConflictToken::default(),
                create_request_ids: BTreeSet::new(),
            };
            let expected_eviction_index = eligibility
                .iter()
                .position(|eligible| *eligible)
                .expect("strategy guarantees an eligible Version");
            for (index, eligible) in eligibility.iter().enumerate() {
                let build_id = format!("build-{index}");
                let mut version = stored_version(&build_id);
                version.create_time = OffsetDateTime::UNIX_EPOCH + Duration::seconds(index as i64);
                if !eligible {
                    version.status = WorkerDeploymentVersionStatus::Draining;
                    version.drainage_info = Some(DrainageInfo {
                        status: VersionDrainageStatus::Draining,
                        last_changed_time: OffsetDateTime::UNIX_EPOCH,
                        last_checked_time: OffsetDateTime::UNIX_EPOCH,
                    });
                }
                deployment.versions.insert(BuildId(build_id), version);
            }

            let removed = try_delete_oldest_eligible_version(
                &mut deployment,
                &WorkerRegistry::default(),
                OffsetDateTime::UNIX_EPOCH,
                ACTIVE_POLLER_WINDOW,
            );
            prop_assert!(removed);
            prop_assert_eq!(deployment.versions.len(), eligibility.len() - 1);
            let expected_eviction = BuildId(format!("build-{expected_eviction_index}"));
            prop_assert!(!deployment.versions.contains_key(&expected_eviction));
        }

        // Feature: worker-deployments, Property 15: a write carrying a non-empty identity
        // records it as the affected record's last_modifier_identity.
        #[test]
        fn property_identity_propagation(
            op_index in 0u8..7,
            identity_index in 0u8..20,
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async move {
                let (registry, _) = registry_with_store();
                let namespace_id = NamespaceId::new();
                let deployment_name = "deployment-a";
                let identity = format!("operator-{identity_index}");

                if op_index == 0 {
                    let created = registry
                        .create_deployment(CreateDeployment {
                            namespace_id,
                            deployment_name: DeploymentName(deployment_name.to_string()),
                            request_id: "request-a".to_string(),
                            identity: identity.clone(),
                        })
                        .await
                        .map_err(|error| TestCaseError::fail(format!("create deployment failed: {error:?}")))?;
                    prop_assert_eq!(created.last_modifier_identity, identity);
                    return Ok::<(), proptest::test_runner::TestCaseError>(());
                }

                registry
                    .create_deployment(create_cmd(namespace_id, deployment_name, "request-a"))
                    .await
                    .map_err(|error| TestCaseError::fail(format!("seed deployment failed: {error:?}")))?;
                registry
                    .create_version(create_version_cmd(
                        namespace_id,
                        deployment_name,
                        "build-a",
                        "version-request-a",
                    ))
                    .await
                    .map_err(|error| TestCaseError::fail(format!("seed version failed: {error:?}")))?;
                if op_index == 2 || op_index == 4 || op_index == 5 || op_index == 6 {
                    registry
                        .create_version(create_version_cmd(
                            namespace_id,
                            deployment_name,
                            "build-b",
                            "version-request-b",
                        ))
                        .await
                        .map_err(|error| TestCaseError::fail(format!("seed second version failed: {error:?}")))?;
                }

                match op_index {
                    1 => {
                        let mut cmd = create_version_cmd(
                            namespace_id,
                            deployment_name,
                            "build-new",
                            "version-request-new",
                        );
                        cmd.identity = identity.clone();
                        registry.create_version(cmd).await.map_err(|error| {
                            TestCaseError::fail(format!("create version failed: {error:?}"))
                        })?;
                        let version = registry
                            .describe_version(describe_version_cmd(
                                namespace_id,
                                deployment_name,
                                Some("build-new"),
                                "",
                                false,
                            ))
                            .await
                            .unwrap();
                        prop_assert_eq!(version.record.last_modifier_identity, identity.clone());
                    }
                    2 => {
                        registry
                            .set_current_version(SetCurrent {
                                identity: identity.clone(),
                                ..set_current_cmd(namespace_id, deployment_name, Some("build-b"), None)
                            })
                            .await
                            .map_err(|error| TestCaseError::fail(format!("set current failed: {error:?}")))?;
                    }
                    3 => {
                        registry
                            .set_ramping_version(SetRamping {
                                identity: identity.clone(),
                                ..set_ramping_cmd(namespace_id, deployment_name, Some("build-a"), 10.0, None)
                            })
                            .await
                            .map_err(|error| TestCaseError::fail(format!("set ramping failed: {error:?}")))?;
                    }
                    4 => {
                        registry
                            .delete_version(DeleteVersion {
                                identity: identity.clone(),
                                ..delete_version_cmd(namespace_id, deployment_name, Some("build-b"), "", true)
                            })
                            .await
                            .map_err(|error| TestCaseError::fail(format!("delete version failed: {error:?}")))?;
                    }
                    5 => {
                        let mut cmd = compute_update_cmd(
                            namespace_id,
                            deployment_name,
                            Some("build-b"),
                            "compute-request",
                        );
                        cmd.identity = identity.clone();
                        cmd.updates.insert(
                            "primary".to_string(),
                            compute_update(scaling_group("test-invoke", "no-sync"), 1),
                        );
                        registry.update_compute_config(cmd).await.map_err(|error| {
                            TestCaseError::fail(format!("update compute failed: {error:?}"))
                        })?;
                        let version = registry
                            .describe_version(describe_version_cmd(
                                namespace_id,
                                deployment_name,
                                Some("build-b"),
                                "",
                                false,
                            ))
                            .await
                            .unwrap();
                        prop_assert_eq!(version.record.last_modifier_identity, identity.clone());
                    }
                    6 => {
                        let mut cmd = metadata_cmd(namespace_id, deployment_name, Some("build-b"), "");
                        cmd.identity = identity.clone();
                        cmd.upsert_entries.insert("key".to_string(), Payload::new("value"));
                        let response = registry.update_version_metadata(cmd).await.map_err(|error| {
                            TestCaseError::fail(format!("update metadata failed: {error:?}"))
                        })?;
                        prop_assert_eq!(response.last_modifier_identity, identity.clone());
                    }
                    _ => unreachable!(),
                }

                let deployment = registry
                    .describe_deployment(deployment_key(namespace_id, deployment_name))
                    .await
                    .map_err(|error| TestCaseError::fail(format!("describe deployment failed: {error:?}")))?;
                prop_assert_eq!(deployment.last_modifier_identity, identity);

                let manager = registry
                    .set_manager(set_manager_cmd(
                        namespace_id,
                        deployment_name,
                        Some(NewManagerIdentity::SelfIdentity),
                        None,
                        "self-manager",
                    ))
                    .await
                    .map_err(|error| TestCaseError::fail(format!("self manager failed: {error:?}")))?;
                prop_assert_eq!(manager.deployment.manager_identity.as_deref(), Some("self-manager"));

                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }

        // Feature: worker-deployments, Property 16: a request rejected for any reason
        // leaves durable state byte-identical (no partial mutation).
        #[test]
        fn property_no_mutation_on_rejected_request(
            rejection_index in 0u8..8,
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async move {
                let (registry, store) = registry_with_store();
                let namespace_id = NamespaceId::new();
                let deployment_name = "deployment-a";
                registry
                    .create_deployment(create_cmd(namespace_id, deployment_name, "request-a"))
                    .await
                    .map_err(|error| TestCaseError::fail(format!("seed deployment failed: {error:?}")))?;
                registry
                    .create_version(create_version_cmd(
                        namespace_id,
                        deployment_name,
                        "build-a",
                        "version-request-a",
                    ))
                    .await
                    .map_err(|error| TestCaseError::fail(format!("seed version failed: {error:?}")))?;
                let key = deployment_key(namespace_id, deployment_name);

                if rejection_index == 5 {
                    registry
                        .set_manager(set_manager_cmd(
                            namespace_id,
                            deployment_name,
                            Some(NewManagerIdentity::ManagerIdentity("manager-a".to_string())),
                            None,
                            "operator-a",
                        ))
                        .await
                        .map_err(|error| TestCaseError::fail(format!("seed manager failed: {error:?}")))?;
                }
                if rejection_index == 6 {
                    let mut record = store.load_deployment(&key).await.unwrap().unwrap();
                    let expected = record.conflict_token;
                    if let Some(version) = record.versions.get_mut(&BuildId("build-a".to_string())) {
                        version.status = WorkerDeploymentVersionStatus::Draining;
                        version.drainage_info = Some(DrainageInfo {
                            status: VersionDrainageStatus::Draining,
                            last_changed_time: OffsetDateTime::UNIX_EPOCH,
                            last_checked_time: OffsetDateTime::UNIX_EPOCH,
                        });
                    }
                    for index in 1..MAX_VERSIONS_PER_DEPLOYMENT {
                        let build_id = format!("build-extra-{index}");
                        let mut version = stored_version(&build_id);
                        version.status = WorkerDeploymentVersionStatus::Draining;
                        version.drainage_info = Some(DrainageInfo {
                            status: VersionDrainageStatus::Draining,
                            last_changed_time: OffsetDateTime::UNIX_EPOCH,
                            last_checked_time: OffsetDateTime::UNIX_EPOCH,
                        });
                        record.versions.insert(BuildId(build_id), version);
                    }
                    store.put_deployment(record, Some(expected)).await.unwrap();
                }

                let before = store.load_deployment(&key).await.unwrap();
                let result = match rejection_index {
                    0 => registry
                        .set_ramping_version(set_ramping_cmd(
                            namespace_id,
                            deployment_name,
                            Some("build-a"),
                            -1.0,
                            None,
                        ))
                        .await
                        .map(|_| ()),
                    1 => registry
                        .create_version(create_version_cmd(
                            namespace_id,
                            "missing-deployment",
                            "build-a",
                            "request-missing",
                        ))
                        .await,
                    2 => registry
                        .set_current_version(set_current_cmd(
                            namespace_id,
                            deployment_name,
                            Some("build-a"),
                            Some(ConflictToken::from_generation(u64::MAX)),
                        ))
                        .await
                        .map(|_| ()),
                    3 => registry
                        .create_deployment(create_cmd(namespace_id, deployment_name, "request-b"))
                        .await
                        .map(|_| ()),
                    4 => {
                        let mut cmd = metadata_cmd(namespace_id, deployment_name, Some("build-a"), "");
                        cmd.upsert_entries.insert("key".to_string(), Payload::new("value"));
                        cmd.remove_entries.insert("key".to_string());
                        registry.update_version_metadata(cmd).await.map(|_| ())
                    }
                    5 => registry
                        .set_current_version(SetCurrent {
                            identity: "operator-b".to_string(),
                            ..set_current_cmd(namespace_id, deployment_name, Some("build-a"), None)
                        })
                        .await
                        .map(|_| ()),
                    6 => registry
                        .create_version(create_version_cmd(
                            namespace_id,
                            deployment_name,
                            "overflow-build",
                            "overflow-request",
                        ))
                        .await,
                    _ => registry
                        .delete_version(delete_version_cmd(
                            namespace_id,
                            deployment_name,
                            Some("missing-build"),
                            "",
                            false,
                        ))
                        .await,
                };

                if rejection_index == 7 {
                    result.map_err(|error| {
                        TestCaseError::fail(format!("missing delete should no-op: {error:?}"))
                    })?;
                } else {
                    prop_assert!(result.is_err());
                }
                let after = store.load_deployment(&key).await.unwrap();
                prop_assert_eq!(after, before);

                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }

        // Feature: worker-deployments, Property 11: drainage follows DRAINING (open pinned
        // workflows remain) → DRAINED (none remain), and is cleared while current/ramping.
        #[test]
        fn property_drainage_lifecycle(
            has_open_pinned in any::<bool>(),
            close_before_refresh in any::<bool>(),
            reactivate_as_ramping in any::<bool>(),
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async move {
                let (registry, store) = registry_with_store();
                let namespace_id = NamespaceId::new();
                let deployment_name = "deployment-a";
                registry
                    .create_deployment(create_cmd(namespace_id, deployment_name, "request-a"))
                    .await
                    .map_err(|error| TestCaseError::fail(format!("seed deployment failed: {error:?}")))?;
                for build_id in ["build-a", "build-b"] {
                    registry
                        .create_version(create_version_cmd(
                            namespace_id,
                            deployment_name,
                            build_id,
                            &format!("version-request-{build_id}"),
                        ))
                        .await
                        .map_err(|error| TestCaseError::fail(format!("seed version failed: {error:?}")))?;
                }
                registry
                    .set_current_version(set_current_cmd(
                        namespace_id,
                        deployment_name,
                        Some("build-a"),
                        None,
                    ))
                    .await
                    .map_err(|error| TestCaseError::fail(format!("initial current failed: {error:?}")))?;
                let run_key = if has_open_pinned {
                    Some(
                        seed_open_pinned_workflow(
                            &store,
                            namespace_id,
                            deployment_name,
                            "build-a",
                            "workflow-tq",
                            "workflow-a",
                        )
                        .await,
                    )
                } else {
                    None
                };

                let promoted = registry
                    .set_current_version(set_current_cmd(
                        namespace_id,
                        deployment_name,
                        Some("build-b"),
                        None,
                    ))
                    .await
                    .map_err(|error| TestCaseError::fail(format!("promote current failed: {error:?}")))?;
                let old = promoted
                    .deployment
                    .versions
                    .iter()
                    .find(|version| version.build_id == BuildId("build-a".to_string()))
                    .unwrap();
                // Demotion is synchronously Draining regardless of open pinned
                // workflows; the Draining→Drained transition only happens on the
                // explicit drainage refresh below, not at the routing change.
                prop_assert_eq!(
                    old.record.drainage_info.as_ref().unwrap().status,
                    VersionDrainageStatus::Draining
                );
                prop_assert!(!matches!(
                    old.record.status,
                    WorkerDeploymentVersionStatus::Current | WorkerDeploymentVersionStatus::Ramping
                ));

                if close_before_refresh
                    && let Some(run_key) = run_key {
                        close_workflow(&store, run_key, "terminate-drainage").await;
                    }
                let refreshed = registry
                    .refresh_version_drainage(
                        namespace_id,
                        DeploymentName(deployment_name.to_string()),
                        BuildId("build-a".to_string()),
                    )
                    .await
                    .map_err(|error| TestCaseError::fail(format!("drainage refresh failed: {error:?}")))?;
                let expected_after_refresh = if has_open_pinned && !close_before_refresh {
                    VersionDrainageStatus::Draining
                } else {
                    VersionDrainageStatus::Drained
                };
                prop_assert_eq!(
                    refreshed.record.drainage_info.as_ref().unwrap().status,
                    expected_after_refresh
                );
                prop_assert_eq!(
                    refreshed.record.drainage_info.as_ref().unwrap().last_checked_time,
                    OffsetDateTime::UNIX_EPOCH
                );

                if reactivate_as_ramping {
                    let ramping = registry
                        .set_ramping_version(set_ramping_cmd(
                            namespace_id,
                            deployment_name,
                            Some("build-a"),
                            25.0,
                            None,
                        ))
                        .await
                        .map_err(|error| TestCaseError::fail(format!("reactivate ramping failed: {error:?}")))?;
                    let reactivated = ramping
                        .deployment
                        .versions
                        .iter()
                        .find(|version| version.build_id == BuildId("build-a".to_string()))
                        .unwrap();
                    prop_assert_eq!(reactivated.record.status, WorkerDeploymentVersionStatus::Ramping);
                    prop_assert!(reactivated.record.drainage_info.is_none());
                }

                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }

        // Feature: worker-deployments, Property 10: a set manager_identity gates
        // set-current/set-ramping/delete-version, while SetManager itself is gated only by
        // the conflict token.
        #[test]
        fn property_manager_identity_and_authorization(
            initial_manager_set in any::<bool>(),
            op_index in 0u8..4,
            identity_matches in any::<bool>(),
            manager_arm_index in 0u8..3,
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async move {
                let (registry, _) = registry_with_store();
                let namespace_id = NamespaceId::new();
                let deployment_name = "deployment-a";
                registry
                    .create_deployment(create_cmd(namespace_id, deployment_name, "request-a"))
                    .await
                    .map_err(|error| TestCaseError::fail(format!("seed deployment failed: {error:?}")))?;
                for build_id in ["build-a", "build-b", "build-c"] {
                    registry
                        .create_version(create_version_cmd(
                            namespace_id,
                            deployment_name,
                            build_id,
                            &format!("version-request-{build_id}"),
                        ))
                        .await
                        .map_err(|error| TestCaseError::fail(format!("seed version failed: {error:?}")))?;
                }
                let mut expected_manager: Option<String> = None;
                if initial_manager_set {
                    registry
                        .set_manager(set_manager_cmd(
                            namespace_id,
                            deployment_name,
                            Some(NewManagerIdentity::ManagerIdentity("manager-a".to_string())),
                            None,
                            "operator-a",
                        ))
                        .await
                        .map_err(|error| TestCaseError::fail(format!("seed manager failed: {error:?}")))?;
                    expected_manager = Some("manager-a".to_string());
                }

                let op = manager_property_op(op_index);
                let identity = if identity_matches {
                    expected_manager.as_deref().unwrap_or("operator-a")
                } else {
                    "operator-b"
                };

                match op {
                    ManagerPropertyOp::SetManager => {
                        let previous = expected_manager.clone();
                        let arm = manager_identity_arm(manager_arm_index);
                        let outcome = registry
                            .set_manager(set_manager_cmd(
                                namespace_id,
                                deployment_name,
                                Some(arm.clone()),
                                None,
                                identity,
                            ))
                            .await
                            .map_err(|error| TestCaseError::fail(format!("set-manager failed: {error:?}")))?;
                        let expected = match arm {
                            NewManagerIdentity::ManagerIdentity(value) if value.is_empty() => None,
                            NewManagerIdentity::ManagerIdentity(value) => Some(value),
                            NewManagerIdentity::SelfIdentity => Some(identity.to_string()),
                        };
                        prop_assert_eq!(outcome.previous_manager_identity, previous);
                        prop_assert_eq!(outcome.deployment.manager_identity, expected);
                    }
                    ManagerPropertyOp::SetCurrent => {
                        let result = registry
                            .set_current_version(SetCurrent {
                                identity: identity.to_string(),
                                ..set_current_cmd(
                                    namespace_id,
                                    deployment_name,
                                    Some("build-a"),
                                    None,
                                )
                            })
                            .await;
                        if initial_manager_set && !identity_matches {
                            let error = result.unwrap_err();
                            prop_assert_eq!(
                                registry_error_kind(&error),
                                RegistryErrorKind::FailedPrecondition
                            );
                        } else {
                            result.map_err(|error| {
                                TestCaseError::fail(format!("set-current failed: {error:?}"))
                            })?;
                        }
                    }
                    ManagerPropertyOp::SetRamping => {
                        let result = registry
                            .set_ramping_version(SetRamping {
                                identity: identity.to_string(),
                                ..set_ramping_cmd(
                                    namespace_id,
                                    deployment_name,
                                    Some("build-b"),
                                    10.0,
                                    None,
                                )
                            })
                            .await;
                        if initial_manager_set && !identity_matches {
                            let error = result.unwrap_err();
                            prop_assert_eq!(
                                registry_error_kind(&error),
                                RegistryErrorKind::FailedPrecondition
                            );
                        } else {
                            result.map_err(|error| {
                                TestCaseError::fail(format!("set-ramping failed: {error:?}"))
                            })?;
                        }
                    }
                    ManagerPropertyOp::DeleteVersion => {
                        let result = registry
                            .delete_version(DeleteVersion {
                                identity: identity.to_string(),
                                ..delete_version_cmd(
                                    namespace_id,
                                    deployment_name,
                                    Some("build-c"),
                                    "",
                                    true,
                                )
                            })
                            .await;
                        if initial_manager_set && !identity_matches {
                            let error = result.unwrap_err();
                            prop_assert_eq!(
                                registry_error_kind(&error),
                                RegistryErrorKind::FailedPrecondition
                            );
                        } else {
                            result.map_err(|error| {
                                TestCaseError::fail(format!("delete-version failed: {error:?}"))
                            })?;
                        }
                    }
                }

                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }

        // Feature: worker-deployments, Property 9: metadata upserts/removes converge to a
        // reference key-value model and the response returns the full stored entries.
        #[test]
        fn property_version_metadata_crud(
            ops in proptest::collection::vec(arb_metadata_op(), 1..80),
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async move {
                let (registry, _) = registry_with_store();
                let namespace_id = NamespaceId::new();
                let deployment_name = "deployment-a";
                let build_id = "build-a";
                registry
                    .create_deployment(create_cmd(namespace_id, deployment_name, "request-a"))
                    .await
                    .map_err(|error| TestCaseError::fail(format!("seed deployment failed: {error:?}")))?;
                registry
                    .create_version(create_version_cmd(
                        namespace_id,
                        deployment_name,
                        build_id,
                        "version-request-a",
                    ))
                    .await
                    .map_err(|error| TestCaseError::fail(format!("seed version failed: {error:?}")))?;
                let mut model = BTreeMap::<String, Payload>::new();

                for op in ops {
                    match op {
                        MetadataOp::Upsert {
                            key_index,
                            value_index,
                        } => {
                            let key = metadata_key(key_index);
                            let value = metadata_payload(value_index);
                            let mut cmd = metadata_cmd(
                                namespace_id,
                                deployment_name,
                                Some(build_id),
                                "",
                            );
                            cmd.upsert_entries.insert(key.clone(), value.clone());
                            let response = registry.update_version_metadata(cmd).await.map_err(|error| {
                                TestCaseError::fail(format!("metadata upsert failed: {error:?}"))
                            })?;
                            model.insert(key, value);
                            prop_assert_eq!(&response.metadata.entries, &model);
                        }
                        MetadataOp::Remove { key_index } => {
                            let key = metadata_key(key_index);
                            let mut cmd = metadata_cmd(
                                namespace_id,
                                deployment_name,
                                Some(build_id),
                                "",
                            );
                            cmd.remove_entries.insert(key.clone());
                            let response = registry.update_version_metadata(cmd).await.map_err(|error| {
                                TestCaseError::fail(format!("metadata remove failed: {error:?}"))
                            })?;
                            model.remove(&key);
                            prop_assert_eq!(&response.metadata.entries, &model);
                        }
                        MetadataOp::Overlap { key_index } => {
                            let key = metadata_key(key_index);
                            let mut cmd = metadata_cmd(
                                namespace_id,
                                deployment_name,
                                Some(build_id),
                                "",
                            );
                            cmd.upsert_entries.insert(key.clone(), metadata_payload(0));
                            cmd.remove_entries.insert(key);
                            let error = registry.update_version_metadata(cmd).await.unwrap_err();
                            prop_assert_eq!(
                                registry_error_kind(&error),
                                RegistryErrorKind::InvalidArgument
                            );
                        }
                    }
                    let described = registry
                        .describe_version(describe_version_cmd(
                            namespace_id,
                            deployment_name,
                            Some(build_id),
                            "",
                            false,
                        ))
                        .await
                        .map_err(|error| {
                            TestCaseError::fail(format!("describe version failed: {error:?}"))
                        })?;
                    prop_assert_eq!(&described.record.metadata.entries, &model);
                }

                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }

        // Feature: worker-deployments, Property 8: compute-config updates obey update_mask
        // semantics, and Validate leaves stored state byte-identical without a Version lookup.
        #[test]
        fn property_compute_config_update_and_validate(
            ops in proptest::collection::vec(arb_compute_config_op(), 1..80),
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async move {
                let (registry, store) = registry_with_store();
                let namespace_id = NamespaceId::new();
                let deployment_name = "deployment-a";
                let build_id = "build-a";
                registry
                    .create_deployment(create_cmd(namespace_id, deployment_name, "request-a"))
                    .await
                    .map_err(|error| TestCaseError::fail(format!("seed deployment failed: {error:?}")))?;
                registry
                    .create_version(create_version_cmd(
                        namespace_id,
                        deployment_name,
                        build_id,
                        "version-request-a",
                    ))
                    .await
                    .map_err(|error| TestCaseError::fail(format!("seed version failed: {error:?}")))?;
                let mut model = ComputeConfig::default();
                let mut request_index = 0usize;

                for op in ops {
                    match op {
                        ComputeConfigOp::Update {
                            group_index,
                            mask_index,
                        } => {
                            let name = compute_group_name(group_index);
                            let providers = [
                                "aws-lambda",
                                "aws-ecs",
                                "subprocess",
                                "k8s",
                                "gcp-cloud-run",
                                "test-invoke",
                                "test-worker-set",
                            ];
                            let scalers = ["no-sync", "rate-based"];
                            let mut group = scaling_group(
                                    providers[request_index % providers.len()],
                                    scalers[request_index % scalers.len()],
                                );
                            group.task_queue_types = match group_index {
                                0 => vec![DeploymentTaskQueueType::Workflow],
                                1 => vec![DeploymentTaskQueueType::Activity],
                                2 => vec![DeploymentTaskQueueType::Nexus],
                                _ => Vec::new(),
                            };
                            let update = compute_update(group, mask_index);
                            let mut updates = BTreeMap::new();
                            updates.insert(name.clone(), update.clone());
                            let removals = BTreeSet::new();
                            let mut cmd = compute_update_cmd(
                                namespace_id,
                                deployment_name,
                                Some(build_id),
                                &format!("compute-request-{request_index}"),
                            );
                            cmd.updates = updates.clone();
                            registry.update_compute_config(cmd).await.map_err(|error| {
                                TestCaseError::fail(format!("compute update failed: {error:?}"))
                            })?;
                            apply_compute_config_change(&mut model, &updates, &removals).unwrap();
                            request_index += 1;
                        }
                        ComputeConfigOp::Remove { group_index } => {
                            let name = compute_group_name(group_index);
                            let mut removals = BTreeSet::new();
                            removals.insert(name);
                            let updates = BTreeMap::new();
                            let mut cmd = compute_update_cmd(
                                namespace_id,
                                deployment_name,
                                Some(build_id),
                                &format!("compute-request-{request_index}"),
                            );
                            cmd.removals = removals.clone();
                            registry.update_compute_config(cmd).await.map_err(|error| {
                                TestCaseError::fail(format!("compute remove failed: {error:?}"))
                            })?;
                            apply_compute_config_change(&mut model, &updates, &removals).unwrap();
                            request_index += 1;
                        }
                        ComputeConfigOp::InvalidOverlap { group_index } => {
                            let name = compute_group_name(group_index);
                            let mut updates = BTreeMap::new();
                            updates.insert(
                                name.clone(),
                                compute_update(scaling_group("invalid", "invalid"), 1),
                            );
                            let removals = BTreeSet::from([name]);
                            let before = store
                                .load_deployment(&deployment_key(namespace_id, deployment_name))
                                .await
                                .unwrap();
                            let mut cmd = compute_update_cmd(
                                namespace_id,
                                deployment_name,
                                Some(build_id),
                                &format!("compute-request-{request_index}"),
                            );
                            cmd.updates = updates;
                            cmd.removals = removals;
                            let error = registry.update_compute_config(cmd).await.unwrap_err();
                            prop_assert_eq!(
                                registry_error_kind(&error),
                                RegistryErrorKind::InvalidArgument
                            );
                            let after = store
                                .load_deployment(&deployment_key(namespace_id, deployment_name))
                                .await
                                .unwrap();
                            prop_assert_eq!(after, before);
                        }
                        ComputeConfigOp::Validate {
                            group_index,
                            invalid_mask,
                        } => {
                            let before = store
                                .load_deployment(&deployment_key(namespace_id, deployment_name))
                                .await
                                .unwrap();
                            let name = compute_group_name(group_index);
                            let mut cmd = validate_compute_cmd(
                                namespace_id,
                                deployment_name,
                                Some("missing-build"),
                            );
                            cmd.updates.insert(
                                name,
                                ComputeConfigScalingGroupUpdate {
                                    scaling_group: scaling_group("test-invoke", "no-sync"),
                                    update_mask: if invalid_mask {
                                        vec!["provider.unknown".to_string()]
                                    } else {
                                        vec!["provider.type".to_string()]
                                    },
                                },
                            );
                            let result = registry.validate_compute_config(cmd).await;
                            if invalid_mask {
                                let error = result.unwrap_err();
                                prop_assert_eq!(
                                    registry_error_kind(&error),
                                    RegistryErrorKind::InvalidArgument
                                );
                            } else {
                                result.map_err(|error| {
                                    TestCaseError::fail(format!("validate compute failed: {error:?}"))
                                })?;
                            }
                            let after = store
                                .load_deployment(&deployment_key(namespace_id, deployment_name))
                                .await
                                .unwrap();
                            prop_assert_eq!(after, before);
                        }
                    }

                    let described = registry
                        .describe_version(describe_version_cmd(
                            namespace_id,
                            deployment_name,
                            Some(build_id),
                            "",
                            false,
                        ))
                        .await
                        .map_err(|error| {
                            TestCaseError::fail(format!("describe version failed: {error:?}"))
                        })?;
                    prop_assert_eq!(&described.record.compute_config, &model);
                }

                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }

        // Feature: worker-deployments, Property 20
        #[test]
        fn property_pinned_membership_cache_fidelity(
            initially_present in any::<bool>(),
            present_after_mutation in any::<bool>(),
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async move {
                let store = Arc::new(InMemoryStore::default());
                let clock = Arc::new(AdjustableClock(Mutex::new(OffsetDateTime::UNIX_EPOCH)));
                let repository: Arc<dyn WorkerDeploymentRepository> = store.clone();
                let run_repository: Arc<dyn RunRepository> = store.clone();
                let registry = DeploymentRegistry::with_clock_and_repositories(
                    repository,
                    run_repository,
                    WorkerRegistry::default(),
                    clock.clone(),
                );
                let namespace_id = NamespaceId::new();
                let ttl = Duration::seconds(1);
                if initially_present {
                    registry
                        .register_polled_deployment(register_polled_cmd(
                            namespace_id,
                            "deployment-a",
                            "build-a",
                            "queue-a",
                            DeploymentTaskQueueType::Workflow,
                        ))
                        .await
                        .unwrap();
                }

                let first = registry
                    .validate_pinned_workflow_version_with_ttl(
                        namespace_id,
                        "queue-a",
                        "deployment-a",
                        "build-a",
                        ttl,
                    )
                    .await;
                prop_assert_eq!(first.is_ok(), initially_present);

                match (initially_present, present_after_mutation) {
                    (false, true) => {
                        registry
                            .register_polled_deployment(register_polled_cmd(
                                namespace_id,
                                "deployment-a",
                                "build-a",
                                "queue-a",
                                DeploymentTaskQueueType::Workflow,
                            ))
                            .await
                            .unwrap();
                    }
                    (true, false) => {
                        let key = deployment_key(namespace_id, "deployment-a");
                        let mut record = store.load_deployment(&key).await.unwrap().unwrap();
                        let expected = record.conflict_token;
                        record
                            .versions
                            .get_mut(&BuildId("build-a".to_string()))
                            .unwrap()
                            .polled_task_queues
                            .clear();
                        store.put_deployment(record, Some(expected)).await.unwrap();
                    }
                    (false, false) | (true, true) => {}
                }

                // Clones are public callers sharing the same runtime-scoped cache. A
                // membership mutation must remain invisible until the TTL boundary.
                let second = registry
                    .clone()
                    .validate_pinned_workflow_version_with_ttl(
                        namespace_id,
                        "queue-a",
                        "deployment-a",
                        "build-a",
                        ttl,
                    )
                    .await;
                prop_assert_eq!(second.is_ok(), initially_present);

                clock.advance(ttl);
                let refreshed = registry
                    .validate_pinned_workflow_version_with_ttl(
                        namespace_id,
                        "queue-a",
                        "deployment-a",
                        "build-a",
                        ttl,
                    )
                    .await;
                prop_assert_eq!(refreshed.is_ok(), present_after_mutation);
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }

        // Feature: worker-deployments, Property 21
        #[test]
        fn property_post_commit_reactivation_deduplication(
            operation_committed in any::<bool>(),
            concrete_pin in any::<bool>(),
            enabled in any::<bool>(),
            status_index in 0u8..6,
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async move {
                let store = Arc::new(InMemoryStore::default());
                let clock = Arc::new(AdjustableClock(Mutex::new(OffsetDateTime::UNIX_EPOCH)));
                let repository: Arc<dyn WorkerDeploymentRepository> = store.clone();
                let run_repository: Arc<dyn RunRepository> = store.clone();
                let registry = DeploymentRegistry::with_clock_and_repositories(
                    repository,
                    run_repository,
                    WorkerRegistry::default(),
                    clock.clone(),
                );
                let namespace_id = NamespaceId::new();
                let key = deployment_key(namespace_id, "deployment-a");
                let build_id = BuildId("build-a".to_string());
                let ttl = Duration::seconds(1);
                registry
                    .register_polled_deployment(register_polled_cmd(
                        namespace_id,
                        "deployment-a",
                        "build-a",
                        "queue-a",
                        DeploymentTaskQueueType::Workflow,
                    ))
                    .await
                    .unwrap();

                let initial_status = match status_index {
                    0 => WorkerDeploymentVersionStatus::Inactive,
                    1 => WorkerDeploymentVersionStatus::Drained,
                    2 => WorkerDeploymentVersionStatus::Current,
                    3 => WorkerDeploymentVersionStatus::Ramping,
                    4 => WorkerDeploymentVersionStatus::Draining,
                    _ => WorkerDeploymentVersionStatus::Created,
                };
                let mut record = store.load_deployment(&key).await.unwrap().unwrap();
                let expected = record.conflict_token;
                record.versions.get_mut(&build_id).unwrap().status = initial_status;
                store.put_deployment(record, Some(expected)).await.unwrap();

                let reactivation_requested = operation_committed && concrete_pin;
                if reactivation_requested {
                    registry
                        .reactivate_pinned_version_with_policy(
                            namespace_id,
                            "deployment-a",
                            "build-a",
                            enabled,
                            ttl,
                        )
                        .await
                        .unwrap();
                }
                let after = store.load_deployment(&key).await.unwrap().unwrap();
                let expected_status = if reactivation_requested
                    && enabled
                    && matches!(
                        initial_status,
                        WorkerDeploymentVersionStatus::Inactive
                            | WorkerDeploymentVersionStatus::Drained
                    )
                {
                    WorkerDeploymentVersionStatus::Draining
                } else {
                    initial_status
                };
                prop_assert_eq!(after.versions.get(&build_id).unwrap().status, expected_status);

                if reactivation_requested && enabled {
                    let mut reset = after;
                    let expected = reset.conflict_token;
                    reset.versions.get_mut(&build_id).unwrap().status =
                        WorkerDeploymentVersionStatus::Drained;
                    store.put_deployment(reset, Some(expected)).await.unwrap();

                    registry
                        .reactivate_pinned_version_with_policy(
                            namespace_id,
                            "deployment-a",
                            "build-a",
                            true,
                            ttl,
                        )
                        .await
                        .unwrap();
                    let deduplicated = store.load_deployment(&key).await.unwrap().unwrap();
                    prop_assert_eq!(
                        deduplicated.versions.get(&build_id).unwrap().status,
                        WorkerDeploymentVersionStatus::Drained
                    );

                    clock.advance(ttl);
                    registry
                        .reactivate_pinned_version_with_policy(
                            namespace_id,
                            "deployment-a",
                            "build-a",
                            true,
                            ttl,
                        )
                        .await
                        .unwrap();
                    let expired = store.load_deployment(&key).await.unwrap().unwrap();
                    prop_assert_eq!(
                        expired.versions.get(&build_id).unwrap().status,
                        WorkerDeploymentVersionStatus::Draining
                    );
                }
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }

        // Feature: worker-deployments, Property 7: allow_no_pollers and
        // ignore_missing_task_queues gate set-current/set-ramping (NOT_FOUND vs auto-create,
        // and the missing-task-queue precondition).
        #[test]
        fn property_poller_presence_preconditions(
            use_ramping in any::<bool>(),
            target_exists in any::<bool>(),
            current_has_queue in any::<bool>(),
            target_has_queue in any::<bool>(),
            queue_has_pressure in any::<bool>(),
            allow_no_pollers in any::<bool>(),
            ignore_missing_task_queues in any::<bool>(),
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async move {
                let (registry, store) = registry_with_store();
                let namespace_id = NamespaceId::new();
                let deployment_name = "deployment-a";
                registry
                    .create_deployment(create_cmd(namespace_id, deployment_name, "request-a"))
                    .await
                    .map_err(|error| TestCaseError::fail(format!("seed deployment failed: {error:?}")))?;
                registry
                    .create_version(create_version_cmd(
                        namespace_id,
                        deployment_name,
                        "build-current",
                        "version-request-current",
                    ))
                    .await
                    .map_err(|error| TestCaseError::fail(format!("seed current failed: {error:?}")))?;
                if target_exists {
                    registry
                        .create_version(create_version_cmd(
                            namespace_id,
                            deployment_name,
                            "build-target",
                            "version-request-target",
                        ))
                        .await
                        .map_err(|error| TestCaseError::fail(format!("seed target failed: {error:?}")))?;
                }
                if current_has_queue {
                    add_polled_task_queue(
                        &store,
                        namespace_id,
                        deployment_name,
                        "build-current",
                        "workflow-tq",
                    )
                    .await;
                }
                if target_exists && target_has_queue {
                    add_polled_task_queue(
                        &store,
                        namespace_id,
                        deployment_name,
                        "build-target",
                        "workflow-tq",
                    )
                    .await;
                }
                registry
                    .set_current_version(set_current_cmd(
                        namespace_id,
                        deployment_name,
                        Some("build-current"),
                        None,
                    ))
                    .await
                    .map_err(|error| TestCaseError::fail(format!("initial set-current failed: {error:?}")))?;
                if current_has_queue && queue_has_pressure {
                    seed_open_pinned_workflow(
                        &store,
                        namespace_id,
                        deployment_name,
                        "build-current",
                        "workflow-tq",
                        "pressure-workflow",
                    )
                    .await;
                }

                let result = if use_ramping {
                    let mut cmd = set_ramping_cmd(
                        namespace_id,
                        deployment_name,
                        Some("build-target"),
                        10.0,
                        None,
                    );
                    cmd.allow_no_pollers = allow_no_pollers;
                    cmd.ignore_missing_task_queues = ignore_missing_task_queues;
                    registry.set_ramping_version(cmd).await.map(|_| ())
                } else {
                    let mut cmd = set_current_cmd(
                        namespace_id,
                        deployment_name,
                        Some("build-target"),
                        None,
                    );
                    cmd.allow_no_pollers = allow_no_pollers;
                    cmd.ignore_missing_task_queues = ignore_missing_task_queues;
                    registry.set_current_version(cmd).await.map(|_| ())
                };

                let missing_target = !target_exists && !allow_no_pollers;
                let target_covers_queue = target_exists && target_has_queue;
                let missing_task_queue = current_has_queue
                    && !target_covers_queue
                    && queue_has_pressure
                    && !ignore_missing_task_queues;
                match (missing_target, missing_task_queue) {
                    (true, _) => {
                        let error = result.unwrap_err();
                        prop_assert_eq!(registry_error_kind(&error), RegistryErrorKind::NotFound);
                    }
                    (false, true) => {
                        let error = result.unwrap_err();
                        let expected = if use_ramping {
                            "proposed ramping version 'deployment-a:build-target' is missing active task queues from the current version; these would become unversioned if it is set as the ramping version"
                        } else {
                            "proposed current version 'deployment-a:build-target' is missing active task queues from the current version; these would become unversioned if it is set as the current version"
                        };
                        prop_assert_eq!(error, RegistryError::FailedPrecondition(expected.to_string()));
                    }
                    (false, false) => {
                        result.map_err(|error| {
                            TestCaseError::fail(format!("poller precondition success case failed: {error:?}"))
                        })?;
                        let described = registry
                            .describe_version(describe_version_cmd(
                                namespace_id,
                                deployment_name,
                                Some("build-target"),
                                "",
                                false,
                            ))
                            .await
                            .map_err(|error| {
                                TestCaseError::fail(format!("target version missing after success: {error:?}"))
                            })?;
                        prop_assert_eq!(described.build_id, BuildId("build-target".to_string()));
                    }
                }

                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }

        // Feature: worker-deployments, Property 6: a stale non-nil conflict token is
        // rejected with FAILED_PRECONDITION and leaves state unchanged; current/nil tokens
        // commit and yield a fresh distinct token.
        #[test]
        fn property_conflict_token_cas_rejects_stale_writes_without_mutation(
            mutation_index in 0u8..3,
            token_index in 0u8..3,
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async move {
                let (registry, store) = registry_with_store();
                let namespace_id = NamespaceId::new();
                let deployment_name = "deployment-a";
                registry
                    .create_deployment(create_cmd(namespace_id, deployment_name, "request-a"))
                    .await
                    .map_err(|error| TestCaseError::fail(format!("seed deployment failed: {error:?}")))?;
                for build_id in ["build-a", "build-b"] {
                    registry
                        .create_version(create_version_cmd(
                            namespace_id,
                            deployment_name,
                            build_id,
                            &format!("version-request-{build_id}"),
                        ))
                        .await
                        .map_err(|error| TestCaseError::fail(format!("seed version failed: {error:?}")))?;
                }
                let key = deployment_key(namespace_id, deployment_name);
                let before = store.load_deployment(&key).await.unwrap().unwrap();
                let token_mode = match token_index {
                    0 => CasTokenMode::Nil,
                    1 => CasTokenMode::Current,
                    _ => CasTokenMode::Stale,
                };
                let token = cas_token(token_mode, before.conflict_token);
                let mutation = match mutation_index {
                    0 => CasMutationKind::SetCurrent,
                    1 => CasMutationKind::SetRamping,
                    _ => CasMutationKind::SetManager,
                };

                let result_token = match mutation {
                    CasMutationKind::SetCurrent => registry
                        .set_current_version(set_current_cmd(
                            namespace_id,
                            deployment_name,
                            Some("build-a"),
                            token,
                        ))
                        .await
                        .map(|outcome| outcome.conflict_token),
                    CasMutationKind::SetRamping => registry
                        .set_ramping_version(set_ramping_cmd(
                            namespace_id,
                            deployment_name,
                            Some("build-b"),
                            10.0,
                            token,
                        ))
                        .await
                        .map(|outcome| outcome.conflict_token),
                    CasMutationKind::SetManager => registry
                        .set_manager(set_manager_cmd(
                            namespace_id,
                            deployment_name,
                            Some(NewManagerIdentity::ManagerIdentity("manager-a".to_string())),
                            token,
                            "operator-a",
                        ))
                        .await
                        .map(|outcome| outcome.conflict_token),
                };

                if matches!(token_mode, CasTokenMode::Stale) {
                    let error = result_token.unwrap_err();
                    prop_assert_eq!(
                        registry_error_kind(&error),
                        RegistryErrorKind::FailedPrecondition
                    );
                    let after = store.load_deployment(&key).await.unwrap().unwrap();
                    prop_assert_eq!(after, before);
                } else {
                    let result_token = result_token.map_err(|error| {
                        TestCaseError::fail(format!("CAS mutation failed: {error:?}"))
                    })?;
                    prop_assert_ne!(result_token, before.conflict_token);
                    let after = store.load_deployment(&key).await.unwrap().unwrap();
                    prop_assert_ne!(after, before);
                }

                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }

        // Feature: worker-deployments, Property 5: set-current/set-ramping evolve the
        // routing config per v1.31.0 (revision bump, ramp-unset-on-promote, ramp != current,
        // fresh token + previous_* values).
        #[test]
        fn property_routing_config_state_machine(
            ops in proptest::collection::vec(arb_routing_op(), 1..80),
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async move {
                let (registry, _) = registry_with_store();
                let namespace_id = NamespaceId::new();
                let deployment_name = "deployment-a";
                registry
                    .create_deployment(create_cmd(namespace_id, deployment_name, "request-a"))
                    .await
                    .map_err(|error| TestCaseError::fail(format!("seed deployment failed: {error:?}")))?;
                for build_index in 0u8..3u8 {
                    let build_id = model_build_id(build_index);
                    registry
                        .create_version(create_version_cmd(
                            namespace_id,
                            deployment_name,
                            &build_id,
                            &format!("version-request-{build_id}"),
                        ))
                        .await
                        .map_err(|error| TestCaseError::fail(format!("seed version failed: {error:?}")))?;
                }

                let mut current: Option<String> = None;
                let mut ramping: Option<String> = None;
                let mut ramp_percentage = 0.0f32;
                let mut revision = 0i64;

                for op in ops {
                    match op {
                        RoutingOp::SetCurrent { target_index } => {
                            let target = routing_target(target_index);
                            let previous_current = current.clone();
                            let previous_ramping = ramping.clone();
                            let result = registry
                                .set_current_version(set_current_cmd(
                                    namespace_id,
                                    deployment_name,
                                    target.as_deref(),
                                    None,
                                ))
                                .await
                                .map_err(|error| {
                                    TestCaseError::fail(format!("set-current failed: {error:?}"))
                                })?;
                            prop_assert_eq!(
                                result.previous_current_version.map(|build| build.0),
                                previous_current
                            );
                            prop_assert_eq!(
                                result.previous_ramping_version.map(|build| build.0),
                                previous_ramping.clone()
                            );
                            current = target.clone();
                            revision += 1;
                            // Promoting a versioned target equal to the current ramping
                            // version unsets the ramp. An unversioned current (target
                            // None) never matches and leaves an unversioned ramp
                            // (ramping_version None at percentage > 0) intact, so the
                            // model must guard on `target.is_some()` to mirror
                            // `set_current_version` (workflow.go @ v1.31.0).
                            if target.is_some() && ramping == target {
                                ramping = None;
                                ramp_percentage = 0.0;
                            }
                            prop_assert_eq!(
                                result
                                    .deployment
                                    .routing_config
                                    .current_version
                                    .as_ref()
                                    .map(|version| version.build_id.0.as_str()),
                                current.as_deref()
                            );
                            prop_assert_eq!(
                                result
                                    .deployment
                                    .routing_config
                                    .ramping_version
                                    .as_ref()
                                    .map(|version| version.build_id.0.as_str()),
                                ramping.as_deref()
                            );
                            prop_assert_eq!(
                                result.deployment.routing_config.ramping_version_percentage,
                                ramp_percentage
                            );
                            prop_assert_eq!(result.deployment.routing_config.revision_number, revision);
                        }
                        RoutingOp::SetRamping {
                            target_index,
                            percentage,
                        } => {
                            let target = routing_target(target_index);
                            let previous_ramping = ramping.clone();
                            let previous_percentage = ramp_percentage;
                            let result = registry
                                .set_ramping_version(set_ramping_cmd(
                                    namespace_id,
                                    deployment_name,
                                    target.as_deref(),
                                    percentage,
                                    None,
                                ))
                                .await;
                            if !(0.0..=100.0).contains(&percentage) {
                                let error = result.unwrap_err();
                                prop_assert_eq!(
                                    registry_error_kind(&error),
                                    RegistryErrorKind::InvalidArgument
                                );
                                continue;
                            }
                            if target.is_some() && target == current {
                                let error = result.unwrap_err();
                                prop_assert_eq!(
                                    registry_error_kind(&error),
                                    RegistryErrorKind::FailedPrecondition
                                );
                                continue;
                            }
                            // A nil target with percentage > 0 ramps to *unversioned*
                            // workers (distinct from a nil target at 0%, which clears the
                            // ramp). It collides with an unversioned (nil) current and is
                            // rejected with FAILED_PRECONDITION (workflow.go @ v1.31.0).
                            let to_unversioned = target.is_none() && percentage > 0.0;
                            if to_unversioned && current.is_none() {
                                let error = result.unwrap_err();
                                prop_assert_eq!(
                                    registry_error_kind(&error),
                                    RegistryErrorKind::FailedPrecondition
                                );
                                continue;
                            }

                            let result = result.map_err(|error| {
                                TestCaseError::fail(format!("set-ramping failed: {error:?}"))
                            })?;
                            prop_assert_eq!(
                                result.previous_ramping_version.map(|build| build.0),
                                previous_ramping
                            );
                            prop_assert_eq!(result.previous_ramping_percentage, previous_percentage);
                            ramping = target.clone();
                            ramp_percentage = percentage;
                            revision += 1;
                            prop_assert_eq!(
                                result
                                    .deployment
                                    .routing_config
                                    .ramping_version
                                    .as_ref()
                                    .map(|version| version.build_id.0.as_str()),
                                ramping.as_deref()
                            );
                            prop_assert_eq!(
                                result.deployment.routing_config.ramping_version_percentage,
                                ramp_percentage
                            );
                            prop_assert_eq!(result.deployment.routing_config.revision_number, revision);
                        }
                    }
                }

                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }

        // Feature: worker-deployments, Property 3: version create/describe/delete match a
        // reference model, including parent-must-exist, duplicate ALREADY_EXISTS, the delete
        // preconditions, and delete-missing-target as a success no-op.
        #[test]
        fn property_version_crud_and_deletion_precondition_correctness(
            ops in proptest::collection::vec(arb_version_crud_op(), 1..80),
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async move {
                let (registry, store) = registry_with_store();
                let namespace_id = NamespaceId::new();
                let parent = model_deployment_name(0);
                registry
                    .create_deployment(create_cmd(namespace_id, &parent, "deployment-request"))
                    .await
                    .map_err(|error| {
                        TestCaseError::fail(format!("failed to seed parent deployment: {error:?}"))
                    })?;
                let mut model = BTreeMap::<String, ModelVersion>::new();

                for op in ops {
                    match op {
                        VersionCrudOp::Create {
                            deployment_index,
                            build_index,
                            request_index,
                        } => {
                            let deployment_name = model_deployment_name(deployment_index);
                            let build_id = model_build_id(build_index);
                            let request_id = model_request_id(request_index);
                            let result = registry
                                .create_version(create_version_cmd(
                                    namespace_id,
                                    &deployment_name,
                                    &build_id,
                                    &request_id,
                                ))
                                .await;

                            if deployment_index != 0 {
                                let error = result.unwrap_err();
                                prop_assert_eq!(registry_error_kind(&error), RegistryErrorKind::NotFound);
                            } else {
                                match model.get_mut(&build_id) {
                                    Some(existing)
                                        if !request_id.is_empty()
                                            && existing.create_request_ids.contains(&request_id) =>
                                    {
                                        result.map_err(|error| {
                                            TestCaseError::fail(format!(
                                                "idempotent version create failed: {error:?}"
                                            ))
                                        })?;
                                    }
                                    Some(_) => {
                                        let error = result.unwrap_err();
                                        prop_assert_eq!(
                                            registry_error_kind(&error),
                                            RegistryErrorKind::AlreadyExists
                                        );
                                    }
                                    None => {
                                        result.map_err(|error| {
                                            TestCaseError::fail(format!(
                                                "initial version create failed: {error:?}"
                                            ))
                                        })?;
                                        model.insert(
                                            build_id,
                                            ModelVersion {
                                                create_request_ids: request_id_set(&request_id),
                                            },
                                        );
                                    }
                                }
                            }
                        }
                        VersionCrudOp::Describe {
                            deployment_index,
                            build_index,
                            report_stats,
                        } => {
                            let deployment_name = model_deployment_name(deployment_index);
                            let build_id = model_build_id(build_index);
                            let result = registry
                                .describe_version(describe_version_cmd(
                                    namespace_id,
                                    &deployment_name,
                                    Some(&build_id),
                                    "",
                                    report_stats,
                                ))
                                .await;
                            if deployment_index == 0 && model.contains_key(&build_id) {
                                let described = result.map_err(|error| {
                                    TestCaseError::fail(format!(
                                        "describe existing version failed: {error:?}"
                                    ))
                                })?;
                                prop_assert_eq!(described.build_id, BuildId(build_id));
                            } else {
                                let error = result.unwrap_err();
                                prop_assert_eq!(registry_error_kind(&error), RegistryErrorKind::NotFound);
                            }
                        }
                        VersionCrudOp::Delete {
                            deployment_index,
                            build_index,
                            precondition,
                        } => {
                            let deployment_name = model_deployment_name(deployment_index);
                            let build_id = model_build_id(build_index);
                            if deployment_index == 0 && model.contains_key(&build_id) {
                                prepare_version_delete_precondition(
                                    &store,
                                    namespace_id,
                                    &deployment_name,
                                    &build_id,
                                    precondition,
                                )
                                .await;
                            }
                            let result = registry
                                .delete_version(delete_version_cmd(
                                    namespace_id,
                                    &deployment_name,
                                    Some(&build_id),
                                    "",
                                    false,
                                ))
                                .await;
                            if deployment_index == 0 && model.contains_key(&build_id) {
                                if precondition == VersionDeletePrecondition::None {
                                    result.map_err(|error| {
                                        TestCaseError::fail(format!(
                                            "delete existing version failed: {error:?}"
                                        ))
                                    })?;
                                    model.remove(&build_id);
                                } else {
                                    let error = result.unwrap_err();
                                    prop_assert_eq!(
                                        registry_error_kind(&error),
                                        RegistryErrorKind::FailedPrecondition
                                    );
                                }
                            } else {
                                result.map_err(|error| {
                                    TestCaseError::fail(format!(
                                        "delete missing version should no-op, got {error:?}"
                                    ))
                                })?;
                            }
                        }
                    }

                    let deployment = registry
                        .describe_deployment(deployment_key(namespace_id, &parent))
                        .await
                        .map_err(|error| {
                            TestCaseError::fail(format!("parent deployment missing: {error:?}"))
                        })?;
                    let actual: BTreeSet<_> = deployment
                        .versions
                        .iter()
                        .map(|version| version.build_id.0.clone())
                        .collect();
                    let expected: BTreeSet<_> = model.keys().cloned().collect();
                    prop_assert_eq!(actual, expected);
                }

                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }

        // Feature: worker-deployments, Property 4: legacy `.`/`:` version strings round-trip
        // to the same version; empty/`__unversioned__` resolve to nil; delimiter-less
        // strings are INVALID_ARGUMENT.
        #[test]
        fn property_deprecated_version_string_round_trip(
            deployment in "[a-z][a-z0-9_-]{0,12}",
            build in "[a-z][a-z0-9_-]{0,12}",
        ) {
            let deployment = DeploymentName(deployment);
            let build = BuildId(build);
            let dot = format!("{}.{}", deployment.0, build.0);
            let colon = format!("{}:{}", deployment.0, build.0);
            let expected = WorkerDeploymentVersionKey {
                deployment_name: deployment.clone(),
                build_id: build.clone(),
            };

            prop_assert_eq!(
                parse_legacy_version_string(&format_legacy_version_string(&deployment, &build))?,
                Some(expected.clone())
            );
            prop_assert_eq!(parse_legacy_version_string(&dot)?, Some(expected.clone()));
            prop_assert_eq!(parse_legacy_version_string(&colon)?, Some(expected));
            prop_assert_eq!(parse_legacy_version_string("")?, None);
            prop_assert_eq!(parse_legacy_version_string(UNVERSIONED_VERSION_SENTINEL)?, None);

            let malformed = format!("{}{}", deployment.0, build.0);
            prop_assume!(malformed != UNVERSIONED_VERSION_SENTINEL);
            prop_assert!(matches!(
                parse_legacy_version_string(&malformed),
                Err(RegistryError::InvalidArgument(_))
            ));
        }

        // Feature: worker-deployments, Property 2: paging with the returned token yields
        // each deployment exactly once with no gaps/dupes, and out-of-range page_size is
        // clamped rather than rejected.
        #[test]
        fn property_deployment_list_pagination_round_trip(
            name_indices in proptest::collection::vec(0u8..40, 0..30),
            page_size in -20i32..140i32,
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async move {
                let (registry, _) = registry_with_store();
                let namespace_id = NamespaceId::new();
                let expected_names: BTreeSet<_> = name_indices
                    .into_iter()
                    .map(model_deployment_name)
                    .collect();

                for name in &expected_names {
                    registry
                        .create_deployment(create_cmd(
                            namespace_id,
                            name,
                            &format!("request-{name}"),
                        ))
                        .await
                        .map_err(|error| {
                            TestCaseError::fail(format!(
                                "failed to seed deployment {name:?}: {error:?}"
                            ))
                        })?;
                }

                let mut next_page_token = String::new();
                let mut actual_names = Vec::new();
                let mut page_count = 0usize;
                loop {
                    let page = registry
                        .list_deployments(list_cmd(
                            namespace_id,
                            page_size,
                            next_page_token.clone(),
                        ))
                        .await
                        .map_err(|error| {
                            TestCaseError::fail(format!(
                                "list deployment page failed for token {next_page_token:?}: {error:?}"
                            ))
                        })?;
                    actual_names.extend(
                        page.deployments
                            .into_iter()
                            .map(|deployment| deployment.name.0),
                    );
                    page_count += 1;
                    prop_assert!(
                        page_count <= expected_names.len() + 1,
                        "pagination did not terminate"
                    );
                    if page.next_page_token.is_empty() {
                        break;
                    }
                    next_page_token = page.next_page_token;
                }

                let actual_name_set: BTreeSet<_> = actual_names.iter().cloned().collect();
                prop_assert_eq!(actual_names.len(), actual_name_set.len());
                prop_assert_eq!(&actual_name_set, &expected_names);
                let mut sorted_actual_names = actual_names.clone();
                sorted_actual_names.sort();
                prop_assert_eq!(actual_names, sorted_actual_names);
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }

        // Feature: worker-deployments, Property 1: deployment create/describe/delete match a
        // reference model, including request_id idempotency, ALREADY_EXISTS, the
        // delete-with-versions precondition, and delete-missing-target as a success no-op.
        #[test]
        fn property_deployment_crud_correctness(
            ops in proptest::collection::vec(arb_deployment_crud_op(), 1..80),
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async move {
                let (registry, store) = registry_with_store();
                let namespace_id = NamespaceId::new();
                let mut model = BTreeMap::<String, ModelDeployment>::new();
                let mut seen_names = BTreeSet::<String>::new();

                for op in ops {
                    match op {
                        DeploymentCrudOp::Create {
                            name_index,
                            request_index,
                        } => {
                            let name = model_deployment_name(name_index);
                            let request_id = model_request_id(request_index);
                            seen_names.insert(name.clone());
                            let result = registry
                                .create_deployment(create_cmd(namespace_id, &name, &request_id))
                                .await;

                            match model.get_mut(&name) {
                                Some(existing)
                                    if !request_id.is_empty()
                                        && existing.create_request_ids.contains(&request_id) =>
                                {
                                    let created = result.map_err(|error| {
                                        TestCaseError::fail(format!(
                                            "idempotent create for {name:?} failed: {error:?}"
                                        ))
                                    })?;
                                    prop_assert_eq!(created.name, DeploymentName(name));
                                }
                                Some(_) => {
                                    let error = result.unwrap_err();
                                    prop_assert_eq!(
                                        registry_error_kind(&error),
                                        RegistryErrorKind::AlreadyExists
                                    );
                                }
                                None => {
                                    let created = result.map_err(|error| {
                                        TestCaseError::fail(format!(
                                            "initial create for {name:?} failed: {error:?}"
                                        ))
                                    })?;
                                    prop_assert_eq!(created.name, DeploymentName(name.clone()));
                                    let create_request_ids = request_id_set(&request_id);
                                    model.insert(
                                        name,
                                        ModelDeployment {
                                            create_request_ids,
                                            build_ids: BTreeSet::new(),
                                        },
                                    );
                                }
                            }
                        }
                        DeploymentCrudOp::Describe { name_index } => {
                            let name = model_deployment_name(name_index);
                            seen_names.insert(name.clone());
                            let result = registry
                                .describe_deployment(deployment_key(namespace_id, &name))
                                .await;
                            if model.contains_key(&name) {
                                let described = result.map_err(|error| {
                                    TestCaseError::fail(format!(
                                        "describe existing deployment {name:?} failed: {error:?}"
                                    ))
                                })?;
                                prop_assert_eq!(described.name, DeploymentName(name));
                            } else {
                                let error = result.unwrap_err();
                                prop_assert_eq!(
                                    registry_error_kind(&error),
                                    RegistryErrorKind::NotFound
                                );
                            }
                        }
                        DeploymentCrudOp::Delete {
                            name_index,
                            token_mode,
                        } => {
                            let name = model_deployment_name(name_index);
                            seen_names.insert(name.clone());
                            let conflict_token = current_delete_token(
                                &registry,
                                namespace_id,
                                &name,
                                token_mode,
                            )
                            .await;
                            let result = registry
                                .delete_deployment(delete_cmd(namespace_id, &name, conflict_token))
                                .await;

                            match model.get(&name) {
                                None => {
                                    result.map_err(|error| {
                                        TestCaseError::fail(format!(
                                            "delete missing deployment {name:?} failed: {error:?}"
                                        ))
                                    })?;
                                }
                                Some(_) if matches!(token_mode, DeleteTokenMode::Stale) => {
                                    let error = result.unwrap_err();
                                    prop_assert_eq!(
                                        registry_error_kind(&error),
                                        RegistryErrorKind::FailedPrecondition
                                    );
                                }
                                Some(existing) if !existing.build_ids.is_empty() => {
                                    let error = result.unwrap_err();
                                    prop_assert_eq!(
                                        registry_error_kind(&error),
                                        RegistryErrorKind::FailedPrecondition
                                    );
                                }
                                Some(_) => {
                                    result.map_err(|error| {
                                        TestCaseError::fail(format!(
                                            "delete empty deployment {name:?} failed: {error:?}"
                                        ))
                                    })?;
                                    model.remove(&name);
                                }
                            }
                        }
                        DeploymentCrudOp::InjectVersion {
                            name_index,
                            build_index,
                        } => {
                            let name = model_deployment_name(name_index);
                            let build_id = model_build_id(build_index);
                            seen_names.insert(name.clone());
                            if let Some(existing) = model.get_mut(&name) {
                                inject_model_version(&store, namespace_id, &name, &build_id).await;
                                existing.build_ids.insert(build_id);
                            }
                        }
                    }

                    assert_deployment_model_matches_registry(
                        &registry,
                        namespace_id,
                        &model,
                        &seen_names,
                    )
                    .await?;
                }

                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }

    fn stored_version(build_id: &str) -> StoredVersion {
        StoredVersion {
            build_id: BuildId(build_id.to_string()),
            status: WorkerDeploymentVersionStatus::Created,
            create_time: OffsetDateTime::UNIX_EPOCH,
            routing_changed_time: None,
            current_since_time: None,
            ramping_since_time: None,
            first_activation_time: None,
            last_current_time: None,
            last_deactivation_time: None,
            ramp_percentage: 0.0,
            drainage_info: None,
            metadata: VersionMetadata::default(),
            compute_config: ComputeConfig::default(),
            last_modifier_identity: "operator-a".to_string(),
            polled_task_queues: BTreeSet::new(),
            create_request_ids: BTreeSet::new(),
            compute_config_request_ids: BTreeSet::new(),
        }
    }

    #[cfg(feature = "conformance")]
    #[test]
    fn conformance_worker_deployment_policy_reads_live_overrides() {
        let overrides = tokeira_conformance::overrides();
        overrides.reset();
        overrides
            .set("matching.maxVersionsInDeployment", OverrideValue::Int(1))
            .unwrap();
        overrides
            .set(
                "matching.maxTaskQueuesInDeploymentVersion",
                OverrideValue::Int(2),
            )
            .unwrap();
        overrides
            .set(
                "matching.PollerHistoryTTL",
                OverrideValue::Duration(StdDuration::from_millis(500)),
            )
            .unwrap();
        overrides
            .set(
                "matching.wv.VersionDrainageStatusVisibilityGracePeriod",
                OverrideValue::Duration(StdDuration::from_secs(3)),
            )
            .unwrap();
        overrides
            .set(
                "matching.wv.VersionDrainageStatusRefreshInterval",
                OverrideValue::Duration(StdDuration::from_secs(4)),
            )
            .unwrap();
        overrides
            .set(
                "history.versionMembershipCacheTTL",
                OverrideValue::Duration(StdDuration::from_secs(5)),
            )
            .unwrap();
        overrides
            .set(
                "history.versionReactivationSignalCacheTTL",
                OverrideValue::Duration(StdDuration::from_secs(6)),
            )
            .unwrap();
        overrides
            .set(
                "history.enableVersionReactivationSignals",
                OverrideValue::Bool(true),
            )
            .unwrap();

        assert_eq!(max_versions_per_deployment(), 1);
        assert_eq!(max_task_queue_families_per_version(), 2);
        assert_eq!(active_poller_window(), Duration::milliseconds(500));
        assert_eq!(drainage_visibility_grace_period(), Duration::seconds(3));
        assert_eq!(drainage_refresh_interval(), Duration::seconds(4));
        assert_eq!(version_membership_cache_ttl(), Duration::seconds(5));
        assert_eq!(version_reactivation_cache_ttl(), Duration::seconds(6));
        assert!(version_reactivation_enabled());
        overrides.reset();
    }

    #[tokio::test]
    async fn create_describe_and_idempotent_retry_project_stored_deployment() {
        let (registry, _) = registry_with_store();
        let namespace_id = NamespaceId::new();

        let created = registry
            .create_deployment(create_cmd(namespace_id, "deployment-a", "request-a"))
            .await
            .unwrap();
        assert_eq!(created.name, DeploymentName("deployment-a".to_string()));
        assert_eq!(created.create_time, OffsetDateTime::UNIX_EPOCH);
        assert_eq!(created.last_modifier_identity, "operator-a");
        assert_eq!(
            created.routing_config_update_state,
            RoutingConfigUpdateState::Completed
        );

        let described = registry
            .describe_deployment(deployment_key(namespace_id, "deployment-a"))
            .await
            .unwrap();
        assert_eq!(described, created);

        let retried = registry
            .create_deployment(create_cmd(namespace_id, "deployment-a", "request-a"))
            .await
            .unwrap();
        assert_eq!(retried, created);

        let duplicate = registry
            .create_deployment(create_cmd(namespace_id, "deployment-a", "request-b"))
            .await
            .unwrap_err();
        assert_eq!(duplicate, RegistryError::AlreadyExists);
    }

    #[tokio::test]
    async fn delete_missing_is_noop_and_existing_without_versions_removes_record() {
        let (registry, _) = registry_with_store();
        let namespace_id = NamespaceId::new();

        registry
            .delete_deployment(delete_cmd(namespace_id, "missing", None))
            .await
            .unwrap();

        let created = registry
            .create_deployment(create_cmd(namespace_id, "deployment-a", "request-a"))
            .await
            .unwrap();
        registry
            .delete_deployment(delete_cmd(
                namespace_id,
                "deployment-a",
                Some(created.conflict_token),
            ))
            .await
            .unwrap();

        let described = registry
            .describe_deployment(deployment_key(namespace_id, "deployment-a"))
            .await
            .unwrap_err();
        assert_eq!(described, RegistryError::NotFound);
    }

    #[tokio::test]
    async fn delete_rejects_versions_and_stale_conflict_tokens_without_mutation() {
        let (registry, store) = registry_with_store();
        let namespace_id = NamespaceId::new();
        let created = registry
            .create_deployment(create_cmd(namespace_id, "deployment-a", "request-a"))
            .await
            .unwrap();

        let stale_token = ConflictToken::from_generation(created.conflict_token.generation() + 10);
        let stale = registry
            .delete_deployment(delete_cmd(namespace_id, "deployment-a", Some(stale_token)))
            .await
            .unwrap_err();
        assert!(matches!(stale, RegistryError::FailedPrecondition(_)));

        let key = deployment_key(namespace_id, "deployment-a");
        let mut record = store.load_deployment(&key).await.unwrap().unwrap();
        let expected = record.conflict_token;
        record
            .versions
            .insert(BuildId("build-a".to_string()), stored_version("build-a"));
        store.put_deployment(record, Some(expected)).await.unwrap();

        let rejected = registry
            .delete_deployment(delete_cmd(namespace_id, "deployment-a", None))
            .await
            .unwrap_err();
        assert!(matches!(rejected, RegistryError::FailedPrecondition(_)));
        assert!(store.load_deployment(&key).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn list_deployments_pages_by_name_and_clamps_out_of_range_sizes() {
        let (registry, _) = registry_with_store();
        let namespace_id = NamespaceId::new();
        for name in ["deployment-b", "deployment-a", "deployment-c"] {
            registry
                .create_deployment(create_cmd(namespace_id, name, &format!("request-{name}")))
                .await
                .unwrap();
        }

        let first = registry
            .list_deployments(list_cmd(namespace_id, 1, String::new()))
            .await
            .unwrap();
        assert_eq!(first.deployments.len(), 1);
        assert_eq!(
            first.deployments[0].name,
            DeploymentName("deployment-a".to_string())
        );
        assert!(!first.next_page_token.is_empty());

        let second = registry
            .list_deployments(list_cmd(namespace_id, 1, first.next_page_token))
            .await
            .unwrap();
        assert_eq!(second.deployments.len(), 1);
        assert_eq!(
            second.deployments[0].name,
            DeploymentName("deployment-b".to_string())
        );

        let all = registry
            .list_deployments(list_cmd(namespace_id, -1, String::new()))
            .await
            .unwrap();
        let names: Vec<_> = all
            .deployments
            .into_iter()
            .map(|deployment| deployment.name.0)
            .collect();
        assert_eq!(names, ["deployment-a", "deployment-b", "deployment-c"]);
        assert!(all.next_page_token.is_empty());
    }

    #[tokio::test]
    async fn create_describe_and_idempotent_retry_project_version() {
        let (registry, store) = registry_with_store();
        let namespace_id = NamespaceId::new();
        registry
            .create_deployment(create_cmd(namespace_id, "deployment-a", "request-a"))
            .await
            .unwrap();

        registry
            .create_version(create_version_cmd(
                namespace_id,
                "deployment-a",
                "build-a",
                "version-request-a",
            ))
            .await
            .unwrap();
        registry
            .create_version(create_version_cmd(
                namespace_id,
                "deployment-a",
                "build-a",
                "version-request-a",
            ))
            .await
            .unwrap();

        let duplicate = registry
            .create_version(create_version_cmd(
                namespace_id,
                "deployment-a",
                "build-a",
                "version-request-b",
            ))
            .await
            .unwrap_err();
        assert_eq!(duplicate, RegistryError::AlreadyExists);

        let key = deployment_key(namespace_id, "deployment-a");
        let mut record = store.load_deployment(&key).await.unwrap().unwrap();
        let expected = record.conflict_token;
        record
            .versions
            .get_mut(&BuildId("build-a".to_string()))
            .unwrap()
            .polled_task_queues
            .insert(VersionTaskQueue {
                name: "queue-a".to_string(),
                task_queue_type: DeploymentTaskQueueType::Workflow,
            });
        store.put_deployment(record, Some(expected)).await.unwrap();

        let hidden_stats = registry
            .describe_version(describe_version_cmd(
                namespace_id,
                "deployment-a",
                Some("build-a"),
                "",
                false,
            ))
            .await
            .unwrap();
        assert_eq!(
            hidden_stats.record.status,
            WorkerDeploymentVersionStatus::Created
        );
        assert_eq!(hidden_stats.record.last_modifier_identity, "operator-a");
        assert_eq!(hidden_stats.task_queues[0].poller_count, None);

        let visible_stats = registry
            .describe_version(describe_version_cmd(
                namespace_id,
                "ignored-by-legacy-version",
                None,
                "deployment-a:build-a",
                true,
            ))
            .await
            .unwrap();
        assert_eq!(visible_stats.build_id, BuildId("build-a".to_string()));
        assert_eq!(visible_stats.task_queues[0].poller_count, Some(0));
    }

    #[tokio::test]
    async fn create_version_rejects_missing_parent_empty_keys_and_max_versions() {
        let (registry, store) = registry_with_store();
        let namespace_id = NamespaceId::new();

        let missing_parent = registry
            .create_version(create_version_cmd(
                namespace_id,
                "missing",
                "build-a",
                "version-request-a",
            ))
            .await
            .unwrap_err();
        assert_eq!(missing_parent, RegistryError::NotFound);

        let empty_build = registry
            .create_version(create_version_cmd(
                namespace_id,
                "missing",
                "",
                "version-request-a",
            ))
            .await
            .unwrap_err();
        assert!(matches!(empty_build, RegistryError::InvalidArgument(_)));

        registry
            .create_deployment(create_cmd(namespace_id, "deployment-a", "request-a"))
            .await
            .unwrap();
        let key = deployment_key(namespace_id, "deployment-a");
        let mut record = store.load_deployment(&key).await.unwrap().unwrap();
        let expected = record.conflict_token;
        for idx in 0..MAX_VERSIONS_PER_DEPLOYMENT {
            let build_id = format!("build-{idx}");
            let mut version = stored_version(&build_id);
            version.status = WorkerDeploymentVersionStatus::Draining;
            version.drainage_info = Some(DrainageInfo {
                status: VersionDrainageStatus::Draining,
                last_changed_time: OffsetDateTime::UNIX_EPOCH,
                last_checked_time: OffsetDateTime::UNIX_EPOCH,
            });
            record.versions.insert(BuildId(build_id), version);
        }
        store.put_deployment(record, Some(expected)).await.unwrap();

        let exhausted = registry
            .create_version(create_version_cmd(
                namespace_id,
                "deployment-a",
                "build-extra",
                "version-request-extra",
            ))
            .await
            .unwrap_err();
        assert_eq!(
            exhausted,
            RegistryError::ResourceExhausted(
                "cannot add version deployment-a.build-extra since maximum number of versions (100) have been registered in the deployment".to_string()
            )
        );
    }

    #[tokio::test]
    async fn poll_registration_enforces_task_queue_family_limit_without_double_counting_types() {
        let (registry, store) = registry_with_store();
        let namespace_id = NamespaceId::new();
        registry
            .register_polled_deployment(register_polled_cmd(
                namespace_id,
                "deployment-a",
                "build-a",
                "queue-0",
                DeploymentTaskQueueType::Workflow,
            ))
            .await
            .unwrap();

        let key = deployment_key(namespace_id, "deployment-a");
        let mut record = store.load_deployment(&key).await.unwrap().unwrap();
        let expected = record.conflict_token;
        let version = record
            .versions
            .get_mut(&BuildId("build-a".to_string()))
            .unwrap();
        for index in 1..MAX_TASK_QUEUE_FAMILIES_PER_VERSION {
            version.polled_task_queues.insert(VersionTaskQueue {
                name: format!("queue-{index}"),
                task_queue_type: DeploymentTaskQueueType::Workflow,
            });
        }
        store.put_deployment(record, Some(expected)).await.unwrap();

        registry
            .register_polled_deployment(register_polled_cmd(
                namespace_id,
                "deployment-a",
                "build-a",
                "queue-0",
                DeploymentTaskQueueType::Activity,
            ))
            .await
            .unwrap();
        let exhausted = registry
            .register_polled_deployment(register_polled_cmd(
                namespace_id,
                "deployment-a",
                "build-a",
                "queue-over-limit",
                DeploymentTaskQueueType::Workflow,
            ))
            .await
            .unwrap_err();
        assert_eq!(
            exhausted,
            RegistryError::ResourceExhausted(
                "cannot add task queue queue-over-limit since maximum number of task queues (100) have been registered in deployment".to_string()
            )
        );
    }

    #[tokio::test]
    async fn pinned_membership_caches_negative_results_until_ttl_expiry() {
        let store = Arc::new(InMemoryStore::default());
        let clock = Arc::new(AdjustableClock(Mutex::new(OffsetDateTime::UNIX_EPOCH)));
        let repository: Arc<dyn WorkerDeploymentRepository> = store.clone();
        let run_repository: Arc<dyn RunRepository> = store;
        let registry = DeploymentRegistry::with_clock_and_repositories(
            repository,
            run_repository,
            WorkerRegistry::default(),
            clock.clone(),
        );
        let namespace_id = NamespaceId::new();

        let missing = registry
            .validate_pinned_workflow_version_with_ttl(
                namespace_id,
                "queue-a",
                "deployment-a",
                "build-a",
                Duration::seconds(1),
            )
            .await
            .unwrap_err();
        assert_eq!(
            missing,
            RegistryError::FailedPrecondition(
                "Pinned version 'deployment-a:build-a' is not present in task queue 'queue-a' of type 'Workflow'"
                    .to_string()
            )
        );

        registry
            .register_polled_deployment(register_polled_cmd(
                namespace_id,
                "deployment-a",
                "build-a",
                "/_sys/queue-a/3",
                DeploymentTaskQueueType::Workflow,
            ))
            .await
            .unwrap();
        assert!(
            registry
                .validate_pinned_workflow_version_with_ttl(
                    namespace_id,
                    "queue-a",
                    "deployment-a",
                    "build-a",
                    Duration::seconds(1),
                )
                .await
                .is_err(),
            "a worker poll does not invalidate v1.31.0's negative membership cache"
        );

        clock.advance(Duration::seconds(1));
        registry
            .validate_pinned_workflow_version_with_ttl(
                namespace_id,
                "queue-a",
                "deployment-a",
                "build-a",
                Duration::seconds(1),
            )
            .await
            .expect("the durable membership is observed at the TTL boundary");
    }

    #[tokio::test]
    async fn pinned_reactivation_is_gated_deduplicated_and_ttl_bounded() {
        let store = Arc::new(InMemoryStore::default());
        let clock = Arc::new(AdjustableClock(Mutex::new(OffsetDateTime::UNIX_EPOCH)));
        let repository: Arc<dyn WorkerDeploymentRepository> = store.clone();
        let run_repository: Arc<dyn RunRepository> = store.clone();
        let registry = DeploymentRegistry::with_clock_and_repositories(
            repository,
            run_repository,
            WorkerRegistry::default(),
            clock.clone(),
        );
        let namespace_id = NamespaceId::new();
        registry
            .register_polled_deployment(register_polled_cmd(
                namespace_id,
                "deployment-a",
                "build-a",
                "queue-a",
                DeploymentTaskQueueType::Workflow,
            ))
            .await
            .unwrap();
        let key = deployment_key(namespace_id, "deployment-a");

        registry
            .reactivate_pinned_version_with_policy(
                namespace_id,
                "deployment-a",
                "build-a",
                false,
                Duration::seconds(10),
            )
            .await
            .unwrap();
        let inactive = store.load_deployment(&key).await.unwrap().unwrap();
        assert_eq!(
            inactive
                .versions
                .get(&BuildId("build-a".to_string()))
                .unwrap()
                .status,
            WorkerDeploymentVersionStatus::Inactive
        );

        registry
            .reactivate_pinned_version_with_policy(
                namespace_id,
                "deployment-a",
                "build-a",
                true,
                Duration::seconds(10),
            )
            .await
            .unwrap();
        let draining = store.load_deployment(&key).await.unwrap().unwrap();
        assert_eq!(
            draining
                .versions
                .get(&BuildId("build-a".to_string()))
                .unwrap()
                .status,
            WorkerDeploymentVersionStatus::Draining
        );

        let mut reset_to_drained = draining;
        let expected = reset_to_drained.conflict_token;
        reset_to_drained
            .versions
            .get_mut(&BuildId("build-a".to_string()))
            .unwrap()
            .status = WorkerDeploymentVersionStatus::Drained;
        store
            .put_deployment(reset_to_drained, Some(expected))
            .await
            .unwrap();
        registry
            .reactivate_pinned_version_with_policy(
                namespace_id,
                "deployment-a",
                "build-a",
                true,
                Duration::seconds(10),
            )
            .await
            .unwrap();
        assert_eq!(
            store
                .load_deployment(&key)
                .await
                .unwrap()
                .unwrap()
                .versions
                .get(&BuildId("build-a".to_string()))
                .unwrap()
                .status,
            WorkerDeploymentVersionStatus::Drained,
            "a duplicate signal inside the cache TTL has no observable effect"
        );

        clock.advance(Duration::seconds(10));
        registry
            .reactivate_pinned_version_with_policy(
                namespace_id,
                "deployment-a",
                "build-a",
                true,
                Duration::seconds(10),
            )
            .await
            .unwrap();
        assert_eq!(
            store
                .load_deployment(&key)
                .await
                .unwrap()
                .unwrap()
                .versions
                .get(&BuildId("build-a".to_string()))
                .unwrap()
                .status,
            WorkerDeploymentVersionStatus::Draining
        );
    }

    #[tokio::test]
    async fn concurrent_pinned_reactivation_commits_once_per_cache_window() {
        let store = Arc::new(InMemoryStore::default());
        let clock = Arc::new(AdjustableClock(Mutex::new(OffsetDateTime::UNIX_EPOCH)));
        let repository: Arc<dyn WorkerDeploymentRepository> = store.clone();
        let run_repository: Arc<dyn RunRepository> = store.clone();
        let registry = DeploymentRegistry::with_clock_and_repositories(
            repository,
            run_repository,
            WorkerRegistry::default(),
            clock,
        );
        let namespace_id = NamespaceId::new();
        let key = deployment_key(namespace_id, "deployment-a");
        registry
            .register_polled_deployment(register_polled_cmd(
                namespace_id,
                "deployment-a",
                "build-a",
                "queue-a",
                DeploymentTaskQueueType::Workflow,
            ))
            .await
            .unwrap();
        let before = store
            .load_deployment(&key)
            .await
            .unwrap()
            .unwrap()
            .conflict_token
            .generation();

        let mut calls = JoinSet::new();
        for _ in 0..16 {
            let registry = registry.clone();
            calls.spawn(async move {
                registry
                    .reactivate_pinned_version_with_policy(
                        namespace_id,
                        "deployment-a",
                        "build-a",
                        true,
                        Duration::seconds(10),
                    )
                    .await
            });
        }
        while let Some(result) = calls.join_next().await {
            result.unwrap().unwrap();
        }

        let after = store.load_deployment(&key).await.unwrap().unwrap();
        assert_eq!(after.conflict_token.generation(), before + 1);
        assert_eq!(
            after
                .versions
                .get(&BuildId("build-a".to_string()))
                .unwrap()
                .status,
            WorkerDeploymentVersionStatus::Draining
        );
    }

    #[tokio::test]
    async fn version_limit_recovery_atomically_replaces_the_oldest_eligible_version() {
        let (registry, store) = registry_with_store();
        let namespace_id = NamespaceId::new();
        registry
            .create_deployment(create_cmd(namespace_id, "deployment-a", "request-a"))
            .await
            .unwrap();
        let key = deployment_key(namespace_id, "deployment-a");
        let mut record = store.load_deployment(&key).await.unwrap().unwrap();
        let expected = record.conflict_token;
        for index in 0..MAX_VERSIONS_PER_DEPLOYMENT {
            let build_id = format!("build-{index}");
            let mut version = stored_version(&build_id);
            version.create_time =
                OffsetDateTime::UNIX_EPOCH + Duration::seconds(i64::try_from(index).unwrap());
            record.versions.insert(BuildId(build_id), version);
        }
        store.put_deployment(record, Some(expected)).await.unwrap();

        registry
            .register_polled_deployment(register_polled_cmd(
                namespace_id,
                "deployment-a",
                "build-new",
                "queue-a",
                DeploymentTaskQueueType::Workflow,
            ))
            .await
            .unwrap();
        let after = store.load_deployment(&key).await.unwrap().unwrap();
        assert_eq!(after.versions.len(), MAX_VERSIONS_PER_DEPLOYMENT);
        assert!(!after.versions.contains_key(&BuildId("build-0".to_string())));
        assert!(
            after
                .versions
                .contains_key(&BuildId("build-new".to_string()))
        );
        assert_eq!(after.last_modifier_identity, "operator-a");
    }

    #[tokio::test]
    async fn delete_version_noops_missing_and_removes_inactive_existing_version() {
        let (registry, _) = registry_with_store();
        let namespace_id = NamespaceId::new();

        registry
            .delete_version(delete_version_cmd(
                namespace_id,
                "missing",
                Some("build-a"),
                "",
                false,
            ))
            .await
            .unwrap();
        registry
            .delete_version(delete_version_cmd(
                namespace_id,
                "deployment-a",
                None,
                UNVERSIONED_VERSION_SENTINEL,
                false,
            ))
            .await
            .unwrap();

        registry
            .create_deployment(create_cmd(namespace_id, "deployment-a", "request-a"))
            .await
            .unwrap();
        registry
            .create_version(create_version_cmd(
                namespace_id,
                "deployment-a",
                "build-a",
                "version-request-a",
            ))
            .await
            .unwrap();
        registry
            .delete_version(delete_version_cmd(
                namespace_id,
                "deployment-a",
                Some("build-a"),
                "",
                false,
            ))
            .await
            .unwrap();

        let described = registry
            .describe_version(describe_version_cmd(
                namespace_id,
                "deployment-a",
                Some("build-a"),
                "",
                false,
            ))
            .await
            .unwrap_err();
        assert_eq!(described, RegistryError::NotFound);
    }

    #[tokio::test]
    async fn delete_version_enforces_routing_drainage_and_poller_preconditions() {
        let (registry, store) = registry_with_store();
        let namespace_id = NamespaceId::new();
        registry
            .create_deployment(create_cmd(namespace_id, "deployment-a", "request-a"))
            .await
            .unwrap();
        for build_id in ["build-current", "build-draining", "build-active"] {
            registry
                .create_version(create_version_cmd(
                    namespace_id,
                    "deployment-a",
                    build_id,
                    &format!("request-{build_id}"),
                ))
                .await
                .unwrap();
        }

        let key = deployment_key(namespace_id, "deployment-a");
        let mut record = store.load_deployment(&key).await.unwrap().unwrap();
        let expected = record.conflict_token;
        record.routing_config.current_version = Some(WorkerDeploymentVersionKey {
            deployment_name: DeploymentName("deployment-a".to_string()),
            build_id: BuildId("build-current".to_string()),
        });
        record
            .versions
            .get_mut(&BuildId("build-draining".to_string()))
            .unwrap()
            .drainage_info = Some(DrainageInfo {
            status: VersionDrainageStatus::Draining,
            last_changed_time: OffsetDateTime::UNIX_EPOCH,
            last_checked_time: OffsetDateTime::UNIX_EPOCH,
        });
        store.put_deployment(record, Some(expected)).await.unwrap();

        let current = registry
            .delete_version(delete_version_cmd(
                namespace_id,
                "deployment-a",
                Some("build-current"),
                "",
                false,
            ))
            .await
            .unwrap_err();
        assert!(matches!(current, RegistryError::FailedPrecondition(_)));

        let draining = registry
            .delete_version(delete_version_cmd(
                namespace_id,
                "deployment-a",
                Some("build-draining"),
                "",
                false,
            ))
            .await
            .unwrap_err();
        assert!(matches!(draining, RegistryError::FailedPrecondition(_)));

        registry
            .delete_version(delete_version_cmd(
                namespace_id,
                "deployment-a",
                Some("build-draining"),
                "",
                true,
            ))
            .await
            .unwrap();

        registry.worker_registry().register(
            WorkerRegistrationKey {
                worker_identity: WorkerIdentity("worker-a".to_string()),
                namespace_id,
                task_queue: TaskQueueName("queue-a".to_string()),
                task_kind: TaskKind::Workflow,
            },
            WorkerVersionMetadata {
                deployment: Some(DeploymentId("deployment-a".to_string())),
                build_id: Some(RuntimeBuildId("build-active".to_string())),
                last_seen_at: Some(OffsetDateTime::UNIX_EPOCH),
            },
        );
        let active = registry
            .delete_version(delete_version_cmd(
                namespace_id,
                "deployment-a",
                Some("build-active"),
                "",
                false,
            ))
            .await
            .unwrap_err();
        assert!(matches!(active, RegistryError::FailedPrecondition(_)));
    }

    #[tokio::test]
    async fn set_current_updates_routing_status_revision_and_unsets_matching_ramp() {
        let (registry, _) = registry_with_store();
        let namespace_id = NamespaceId::new();
        registry
            .create_deployment(create_cmd(namespace_id, "deployment-a", "request-a"))
            .await
            .unwrap();
        for build_id in ["build-a", "build-b"] {
            registry
                .create_version(create_version_cmd(
                    namespace_id,
                    "deployment-a",
                    build_id,
                    &format!("request-{build_id}"),
                ))
                .await
                .unwrap();
        }

        let ramping = registry
            .set_ramping_version(set_ramping_cmd(
                namespace_id,
                "deployment-a",
                Some("build-b"),
                25.0,
                None,
            ))
            .await
            .unwrap();
        assert_eq!(ramping.previous_ramping_version, None);
        assert_eq!(ramping.previous_ramping_percentage, 0.0);
        assert_eq!(
            ramping.deployment.routing_config.ramping_version.as_ref(),
            Some(&WorkerDeploymentVersionKey {
                deployment_name: DeploymentName("deployment-a".to_string()),
                build_id: BuildId("build-b".to_string()),
            })
        );

        let current = registry
            .set_current_version(set_current_cmd(
                namespace_id,
                "deployment-a",
                Some("build-b"),
                Some(ramping.conflict_token),
            ))
            .await
            .unwrap();
        assert_eq!(current.previous_current_version, None);
        assert_eq!(
            current.previous_ramping_version,
            Some(BuildId("build-b".to_string()))
        );
        assert_eq!(current.deployment.routing_config.ramping_version, None);
        assert_eq!(
            current.deployment.routing_config.ramping_version_percentage,
            0.0
        );
        assert_eq!(current.deployment.routing_config.revision_number, 2);

        let promoted = current
            .deployment
            .versions
            .iter()
            .find(|version| version.build_id == BuildId("build-b".to_string()))
            .unwrap();
        assert_eq!(
            promoted.record.status,
            WorkerDeploymentVersionStatus::Current
        );
        assert_eq!(
            promoted.record.current_since_time,
            Some(OffsetDateTime::UNIX_EPOCH)
        );
        assert_eq!(promoted.record.ramping_since_time, None);

        let unset = registry
            .set_current_version(set_current_cmd(
                namespace_id,
                "deployment-a",
                None,
                Some(current.conflict_token),
            ))
            .await
            .unwrap();
        assert_eq!(
            unset.previous_current_version,
            Some(BuildId("build-b".to_string()))
        );
        assert_eq!(unset.deployment.routing_config.current_version, None);
        let drained = unset
            .deployment
            .versions
            .iter()
            .find(|version| version.build_id == BuildId("build-b".to_string()))
            .unwrap();
        // A version demoted out of Current is Draining immediately; the
        // Draining→Drained transition is delayed (entity-workflow drainage check /
        // sync-drainage signal), not synchronous at the routing change.
        assert_eq!(
            drained.record.status,
            WorkerDeploymentVersionStatus::Draining
        );
        assert_eq!(
            drained.record.drainage_info.as_ref().unwrap().status,
            VersionDrainageStatus::Draining
        );
    }

    #[tokio::test]
    async fn set_ramping_validates_percentage_current_conflict_and_missing_versions() {
        let (registry, _) = registry_with_store();
        let namespace_id = NamespaceId::new();
        registry
            .create_deployment(create_cmd(namespace_id, "deployment-a", "request-a"))
            .await
            .unwrap();
        for build_id in ["build-a", "build-b"] {
            registry
                .create_version(create_version_cmd(
                    namespace_id,
                    "deployment-a",
                    build_id,
                    &format!("request-{build_id}"),
                ))
                .await
                .unwrap();
        }

        let invalid_percentage = registry
            .set_ramping_version(set_ramping_cmd(
                namespace_id,
                "deployment-a",
                Some("build-a"),
                101.0,
                None,
            ))
            .await
            .unwrap_err();
        assert!(matches!(
            invalid_percentage,
            RegistryError::InvalidArgument(_)
        ));

        let missing = registry
            .set_ramping_version(set_ramping_cmd(
                namespace_id,
                "deployment-a",
                Some("missing"),
                10.0,
                None,
            ))
            .await
            .unwrap_err();
        assert_eq!(missing, RegistryError::NotFound);

        let current = registry
            .set_current_version(set_current_cmd(
                namespace_id,
                "deployment-a",
                Some("build-a"),
                None,
            ))
            .await
            .unwrap();
        let same_as_current = registry
            .set_ramping_version(set_ramping_cmd(
                namespace_id,
                "deployment-a",
                Some("build-a"),
                10.0,
                Some(current.conflict_token),
            ))
            .await
            .unwrap_err();
        assert!(matches!(
            same_as_current,
            RegistryError::FailedPrecondition(_)
        ));

        let stale_token = ConflictToken::from_generation(current.conflict_token.generation() + 10);
        let stale = registry
            .set_ramping_version(set_ramping_cmd(
                namespace_id,
                "deployment-a",
                Some("build-b"),
                10.0,
                Some(stale_token),
            ))
            .await
            .unwrap_err();
        assert!(matches!(stale, RegistryError::FailedPrecondition(_)));

        let ramping = registry
            .set_ramping_version(set_ramping_cmd(
                namespace_id,
                "deployment-a",
                Some("build-b"),
                10.0,
                Some(current.conflict_token),
            ))
            .await
            .unwrap();
        assert_eq!(ramping.previous_ramping_version, None);
        assert_eq!(ramping.previous_ramping_percentage, 0.0);
        assert_eq!(
            ramping
                .deployment
                .routing_config
                .current_version_revision_number,
            1
        );
        assert_eq!(
            ramping
                .deployment
                .routing_config
                .ramping_version_revision_number,
            2
        );
        assert_eq!(
            ramping.deployment.routing_config.ramping_version_percentage,
            10.0
        );
        let version = ramping
            .deployment
            .versions
            .iter()
            .find(|version| version.build_id == BuildId("build-b".to_string()))
            .unwrap();
        assert_eq!(
            version.record.status,
            WorkerDeploymentVersionStatus::Ramping
        );
        assert_eq!(version.record.ramp_percentage, 10.0);

        let unversioned_ramp = registry
            .set_ramping_version(set_ramping_cmd(
                namespace_id,
                "deployment-a",
                None,
                5.0,
                Some(ramping.conflict_token),
            ))
            .await
            .unwrap();
        assert_eq!(
            unversioned_ramp.previous_ramping_version,
            Some(BuildId("build-b".to_string()))
        );
        assert_eq!(unversioned_ramp.previous_ramping_percentage, 10.0);
        assert_eq!(
            unversioned_ramp.deployment.routing_config.ramping_version,
            None
        );
        assert_eq!(
            unversioned_ramp
                .deployment
                .routing_config
                .ramping_version_percentage,
            5.0
        );
    }

    #[tokio::test]
    async fn allow_no_pollers_controls_unknown_current_and_ramping_versions() {
        let (registry, _) = registry_with_store();
        let namespace_id = NamespaceId::new();
        registry
            .create_deployment(create_cmd(namespace_id, "deployment-a", "request-a"))
            .await
            .unwrap();

        let rejected_current = registry
            .set_current_version(set_current_cmd(
                namespace_id,
                "deployment-a",
                Some("build-auto-current"),
                None,
            ))
            .await
            .unwrap_err();
        assert_eq!(rejected_current, RegistryError::NotFound);

        let mut current_cmd = set_current_cmd(
            namespace_id,
            "deployment-a",
            Some("build-auto-current"),
            None,
        );
        current_cmd.allow_no_pollers = true;
        let current = registry.set_current_version(current_cmd).await.unwrap();
        assert!(current.deployment.versions.iter().any(|version| {
            version.build_id == BuildId("build-auto-current".to_string())
                && version.record.status == WorkerDeploymentVersionStatus::Current
        }));

        let rejected_ramping = registry
            .set_ramping_version(set_ramping_cmd(
                namespace_id,
                "deployment-a",
                Some("build-auto-ramping"),
                10.0,
                Some(current.conflict_token),
            ))
            .await
            .unwrap_err();
        assert_eq!(rejected_ramping, RegistryError::NotFound);

        let mut ramping_cmd = set_ramping_cmd(
            namespace_id,
            "deployment-a",
            Some("build-auto-ramping"),
            10.0,
            Some(current.conflict_token),
        );
        ramping_cmd.allow_no_pollers = true;
        let ramping = registry.set_ramping_version(ramping_cmd).await.unwrap();
        assert!(ramping.deployment.versions.iter().any(|version| {
            version.build_id == BuildId("build-auto-ramping".to_string())
                && version.record.status == WorkerDeploymentVersionStatus::Ramping
        }));
    }

    #[tokio::test]
    async fn missing_task_queue_guard_uses_durable_task_queue_pressure() {
        let (registry, store) = registry_with_store();
        let namespace_id = NamespaceId::new();
        registry
            .create_deployment(create_cmd(namespace_id, "deployment-a", "request-a"))
            .await
            .unwrap();
        for build_id in ["build-a", "build-b", "build-c"] {
            registry
                .create_version(create_version_cmd(
                    namespace_id,
                    "deployment-a",
                    build_id,
                    &format!("request-{build_id}"),
                ))
                .await
                .unwrap();
        }
        add_polled_task_queue(&store, namespace_id, "deployment-a", "build-a", "queue-a").await;
        add_polled_task_queue(&store, namespace_id, "deployment-a", "build-a", "queue-b").await;
        add_polled_task_queue(&store, namespace_id, "deployment-a", "build-b", "queue-a").await;
        add_polled_task_queue(&store, namespace_id, "deployment-a", "build-c", "queue-a").await;
        add_polled_task_queue(&store, namespace_id, "deployment-a", "build-c", "queue-b").await;

        let current = registry
            .set_current_version(set_current_cmd(
                namespace_id,
                "deployment-a",
                Some("build-a"),
                None,
            ))
            .await
            .unwrap();
        seed_open_pinned_workflow(
            &store,
            namespace_id,
            "deployment-a",
            "build-a",
            "queue-b",
            "pressure-workflow",
        )
        .await;

        let mut guarded_current = set_current_cmd(
            namespace_id,
            "deployment-a",
            Some("build-b"),
            Some(current.conflict_token),
        );
        guarded_current.ignore_missing_task_queues = false;
        let missing_current = registry
            .set_current_version(guarded_current)
            .await
            .unwrap_err();
        assert_eq!(
            missing_current,
            RegistryError::FailedPrecondition(
                "proposed current version 'deployment-a:build-b' is missing active task queues from the current version; these would become unversioned if it is set as the current version".to_string()
            )
        );

        let mut bypass_current = set_current_cmd(
            namespace_id,
            "deployment-a",
            Some("build-b"),
            Some(current.conflict_token),
        );
        bypass_current.ignore_missing_task_queues = true;
        let current_b = registry.set_current_version(bypass_current).await.unwrap();

        let mut guarded_ramping = set_ramping_cmd(
            namespace_id,
            "deployment-a",
            Some("build-c"),
            20.0,
            Some(current_b.conflict_token),
        );
        guarded_ramping.ignore_missing_task_queues = false;
        let ramping = registry.set_ramping_version(guarded_ramping).await.unwrap();
        assert_eq!(
            ramping.deployment.routing_config.ramping_version_percentage,
            20.0
        );
    }

    #[tokio::test]
    async fn set_manager_sets_unsets_self_and_returns_previous_manager() {
        let (registry, _) = registry_with_store();
        let namespace_id = NamespaceId::new();
        let created = registry
            .create_deployment(create_cmd(namespace_id, "deployment-a", "request-a"))
            .await
            .unwrap();

        let set = registry
            .set_manager(set_manager_cmd(
                namespace_id,
                "deployment-a",
                Some(NewManagerIdentity::ManagerIdentity("manager-a".to_string())),
                Some(created.conflict_token),
                "operator-a",
            ))
            .await
            .unwrap();
        assert_eq!(set.previous_manager_identity, None);
        assert_eq!(
            set.deployment.manager_identity.as_deref(),
            Some("manager-a")
        );
        assert_ne!(set.conflict_token, created.conflict_token);

        let self_manager = registry
            .set_manager(set_manager_cmd(
                namespace_id,
                "deployment-a",
                Some(NewManagerIdentity::SelfIdentity),
                Some(set.conflict_token),
                "operator-b",
            ))
            .await
            .unwrap();
        assert_eq!(
            self_manager.previous_manager_identity.as_deref(),
            Some("manager-a")
        );
        assert_eq!(
            self_manager.deployment.manager_identity.as_deref(),
            Some("operator-b")
        );

        let unset = registry
            .set_manager(set_manager_cmd(
                namespace_id,
                "deployment-a",
                Some(NewManagerIdentity::ManagerIdentity(String::new())),
                Some(self_manager.conflict_token),
                "operator-c",
            ))
            .await
            .unwrap();
        assert_eq!(
            unset.previous_manager_identity.as_deref(),
            Some("operator-b")
        );
        assert_eq!(unset.deployment.manager_identity, None);
        assert_eq!(unset.deployment.last_modifier_identity, "operator-c");
    }

    #[tokio::test]
    async fn set_manager_rejects_empty_identity_unset_oneof_and_stale_token() {
        let (registry, _) = registry_with_store();
        let namespace_id = NamespaceId::new();
        let created = registry
            .create_deployment(create_cmd(namespace_id, "deployment-a", "request-a"))
            .await
            .unwrap();

        let empty_identity = registry
            .set_manager(set_manager_cmd(
                namespace_id,
                "deployment-a",
                Some(NewManagerIdentity::SelfIdentity),
                None,
                "",
            ))
            .await
            .unwrap_err();
        assert!(matches!(empty_identity, RegistryError::InvalidArgument(_)));

        let unset_oneof = registry
            .set_manager(set_manager_cmd(
                namespace_id,
                "deployment-a",
                None,
                None,
                "operator-a",
            ))
            .await
            .unwrap_err();
        assert!(matches!(unset_oneof, RegistryError::InvalidArgument(_)));

        let stale = registry
            .set_manager(set_manager_cmd(
                namespace_id,
                "deployment-a",
                Some(NewManagerIdentity::ManagerIdentity("manager-a".to_string())),
                Some(ConflictToken::from_generation(
                    created.conflict_token.generation() + 10,
                )),
                "operator-a",
            ))
            .await
            .unwrap_err();
        assert!(matches!(stale, RegistryError::FailedPrecondition(_)));
    }

    #[tokio::test]
    async fn manager_identity_gates_current_ramping_and_delete_version_only() {
        let (registry, _) = registry_with_store();
        let namespace_id = NamespaceId::new();
        registry
            .create_deployment(create_cmd(namespace_id, "deployment-a", "request-a"))
            .await
            .unwrap();
        for build_id in ["build-a", "build-b", "build-c"] {
            registry
                .create_version(create_version_cmd(
                    namespace_id,
                    "deployment-a",
                    build_id,
                    &format!("request-{build_id}"),
                ))
                .await
                .unwrap();
        }
        let manager = registry
            .set_manager(set_manager_cmd(
                namespace_id,
                "deployment-a",
                Some(NewManagerIdentity::ManagerIdentity("manager-a".to_string())),
                None,
                "operator-a",
            ))
            .await
            .unwrap();

        let mut current_bad = set_current_cmd(
            namespace_id,
            "deployment-a",
            Some("build-a"),
            Some(manager.conflict_token),
        );
        current_bad.identity = "operator-b".to_string();
        assert!(matches!(
            registry.set_current_version(current_bad).await.unwrap_err(),
            RegistryError::FailedPrecondition(_)
        ));

        let mut current_good = set_current_cmd(
            namespace_id,
            "deployment-a",
            Some("build-a"),
            Some(manager.conflict_token),
        );
        current_good.identity = "manager-a".to_string();
        let current = registry.set_current_version(current_good).await.unwrap();

        let mut ramping_bad = set_ramping_cmd(
            namespace_id,
            "deployment-a",
            Some("build-b"),
            10.0,
            Some(current.conflict_token),
        );
        ramping_bad.identity = "operator-b".to_string();
        assert!(matches!(
            registry.set_ramping_version(ramping_bad).await.unwrap_err(),
            RegistryError::FailedPrecondition(_)
        ));

        let mut ramping_good = set_ramping_cmd(
            namespace_id,
            "deployment-a",
            Some("build-b"),
            10.0,
            Some(current.conflict_token),
        );
        ramping_good.identity = "manager-a".to_string();
        let ramping = registry.set_ramping_version(ramping_good).await.unwrap();

        let delete_bad = delete_version_cmd_with_auth(
            namespace_id,
            "deployment-a",
            Some("build-c"),
            Some(ramping.conflict_token),
            "operator-b",
        );
        assert!(matches!(
            registry.delete_version(delete_bad).await.unwrap_err(),
            RegistryError::FailedPrecondition(_)
        ));

        let delete_good = delete_version_cmd_with_auth(
            namespace_id,
            "deployment-a",
            Some("build-c"),
            Some(ramping.conflict_token),
            "manager-a",
        );
        registry.delete_version(delete_good).await.unwrap();
    }

    #[tokio::test]
    async fn write_paths_record_non_empty_identity_without_clearing_on_empty_identity() {
        let (registry, _) = registry_with_store();
        let namespace_id = NamespaceId::new();
        registry
            .create_deployment(CreateDeployment {
                namespace_id,
                deployment_name: DeploymentName("deployment-a".to_string()),
                request_id: "request-a".to_string(),
                identity: "creator".to_string(),
            })
            .await
            .unwrap();

        let mut create_version =
            create_version_cmd(namespace_id, "deployment-a", "build-a", "version-request-a");
        create_version.identity = "version-creator".to_string();
        registry.create_version(create_version).await.unwrap();
        let deployment = registry
            .describe_deployment(deployment_key(namespace_id, "deployment-a"))
            .await
            .unwrap();
        assert_eq!(deployment.last_modifier_identity, "version-creator");
        let version = registry
            .describe_version(describe_version_cmd(
                namespace_id,
                "deployment-a",
                Some("build-a"),
                "",
                false,
            ))
            .await
            .unwrap();
        assert_eq!(version.record.last_modifier_identity, "version-creator");

        let mut empty_identity_current = set_current_cmd(
            namespace_id,
            "deployment-a",
            Some("build-a"),
            Some(deployment.conflict_token),
        );
        empty_identity_current.identity = String::new();
        let current = registry
            .set_current_version(empty_identity_current)
            .await
            .unwrap();
        assert_eq!(current.deployment.last_modifier_identity, "version-creator");

        let mut operator_current = set_current_cmd(
            namespace_id,
            "deployment-a",
            None,
            Some(current.conflict_token),
        );
        operator_current.identity = "router".to_string();
        let unset = registry
            .set_current_version(operator_current)
            .await
            .unwrap();
        assert_eq!(unset.deployment.last_modifier_identity, "router");

        let mut metadata = metadata_cmd(namespace_id, "deployment-a", Some("build-a"), "");
        metadata.identity = "metadata-writer".to_string();
        metadata
            .upsert_entries
            .insert("key".to_string(), Payload::new("value"));
        registry.update_version_metadata(metadata).await.unwrap();
        let version = registry
            .describe_version(describe_version_cmd(
                namespace_id,
                "deployment-a",
                Some("build-a"),
                "",
                false,
            ))
            .await
            .unwrap();
        assert_eq!(version.record.last_modifier_identity, "metadata-writer");

        let manager = registry
            .set_manager(set_manager_cmd(
                namespace_id,
                "deployment-a",
                Some(NewManagerIdentity::SelfIdentity),
                None,
                "manager-a",
            ))
            .await
            .unwrap();
        assert_eq!(
            manager.deployment.manager_identity.as_deref(),
            Some("manager-a")
        );
        assert_eq!(manager.deployment.last_modifier_identity, "manager-a");
    }

    #[tokio::test]
    async fn drainage_due_times_use_initial_visibility_grace_then_refresh_interval() {
        let (registry, store) = registry_with_store();
        let namespace_id = NamespaceId::new();
        registry
            .create_deployment(create_cmd(namespace_id, "deployment-a", "request-a"))
            .await
            .unwrap();
        registry
            .create_version(create_version_cmd(
                namespace_id,
                "deployment-a",
                "build-a",
                "version-request-a",
            ))
            .await
            .unwrap();
        let key = deployment_key(namespace_id, "deployment-a");
        let mut record = store.load_deployment(&key).await.unwrap().unwrap();
        let version = record
            .versions
            .get_mut(&BuildId("build-a".to_string()))
            .unwrap();
        version.status = WorkerDeploymentVersionStatus::Draining;
        version.drainage_info = Some(DrainageInfo {
            status: VersionDrainageStatus::Draining,
            last_changed_time: OffsetDateTime::UNIX_EPOCH,
            last_checked_time: OffsetDateTime::UNIX_EPOCH,
        });

        assert!(
            due_drainage_build_ids(
                &record,
                OffsetDateTime::UNIX_EPOCH + DRAINAGE_VISIBILITY_GRACE_PERIOD
                    - Duration::nanoseconds(1),
                DRAINAGE_VISIBILITY_GRACE_PERIOD,
                DRAINAGE_REFRESH_INTERVAL,
            )
            .is_empty()
        );
        assert_eq!(
            due_drainage_build_ids(
                &record,
                OffsetDateTime::UNIX_EPOCH + DRAINAGE_VISIBILITY_GRACE_PERIOD,
                DRAINAGE_VISIBILITY_GRACE_PERIOD,
                DRAINAGE_REFRESH_INTERVAL,
            ),
            BTreeSet::from([BuildId("build-a".to_string())])
        );

        let later_check_time = OffsetDateTime::UNIX_EPOCH + Duration::minutes(1);
        record
            .versions
            .get_mut(&BuildId("build-a".to_string()))
            .unwrap()
            .drainage_info
            .as_mut()
            .unwrap()
            .last_checked_time = later_check_time;
        assert!(
            due_drainage_build_ids(
                &record,
                later_check_time + DRAINAGE_REFRESH_INTERVAL - Duration::nanoseconds(1),
                DRAINAGE_VISIBILITY_GRACE_PERIOD,
                DRAINAGE_REFRESH_INTERVAL,
            )
            .is_empty()
        );
        assert_eq!(
            due_drainage_build_ids(
                &record,
                later_check_time + DRAINAGE_REFRESH_INTERVAL,
                DRAINAGE_VISIBILITY_GRACE_PERIOD,
                DRAINAGE_REFRESH_INTERVAL,
            ),
            BTreeSet::from([BuildId("build-a".to_string())])
        );
    }

    #[tokio::test]
    async fn describe_lazily_refreshes_drainage_once_visibility_grace_is_due() {
        let (registry, store) = registry_with_store();
        let namespace_id = NamespaceId::new();
        registry
            .create_deployment(create_cmd(namespace_id, "deployment-a", "request-a"))
            .await
            .unwrap();
        registry
            .create_version(create_version_cmd(
                namespace_id,
                "deployment-a",
                "build-a",
                "version-request-a",
            ))
            .await
            .unwrap();
        let key = deployment_key(namespace_id, "deployment-a");
        let mut record = store.load_deployment(&key).await.unwrap().unwrap();
        let expected = record.conflict_token;
        let version = record
            .versions
            .get_mut(&BuildId("build-a".to_string()))
            .unwrap();
        version.status = WorkerDeploymentVersionStatus::Draining;
        version.drainage_info = Some(DrainageInfo {
            status: VersionDrainageStatus::Draining,
            last_changed_time: OffsetDateTime::UNIX_EPOCH,
            last_checked_time: OffsetDateTime::UNIX_EPOCH,
        });
        store.put_deployment(record, Some(expected)).await.unwrap();

        let before_due = registry_for_store_at(
            store.clone(),
            OffsetDateTime::UNIX_EPOCH + DRAINAGE_VISIBILITY_GRACE_PERIOD
                - Duration::nanoseconds(1),
        )
        .describe_deployment(key.clone())
        .await
        .unwrap();
        assert_eq!(
            before_due.versions[0].record.status,
            WorkerDeploymentVersionStatus::Draining
        );

        let due = registry_for_store_at(
            store,
            OffsetDateTime::UNIX_EPOCH + DRAINAGE_VISIBILITY_GRACE_PERIOD,
        )
        .describe_deployment(key)
        .await
        .unwrap();
        assert_eq!(
            due.versions[0].record.status,
            WorkerDeploymentVersionStatus::Drained
        );
        assert_eq!(
            due.versions[0]
                .record
                .drainage_info
                .as_ref()
                .unwrap()
                .last_checked_time,
            OffsetDateTime::UNIX_EPOCH + DRAINAGE_VISIBILITY_GRACE_PERIOD
        );
    }

    #[tokio::test]
    async fn drainage_lifecycle_tracks_open_pinned_workflows_and_refreshes_to_drained() {
        let (registry, store) = registry_with_store();
        let namespace_id = NamespaceId::new();
        registry
            .create_deployment(create_cmd(namespace_id, "deployment-a", "request-a"))
            .await
            .unwrap();
        for build_id in ["build-a", "build-b"] {
            registry
                .create_version(create_version_cmd(
                    namespace_id,
                    "deployment-a",
                    build_id,
                    &format!("version-request-{build_id}"),
                ))
                .await
                .unwrap();
        }
        registry
            .set_current_version(set_current_cmd(
                namespace_id,
                "deployment-a",
                Some("build-a"),
                None,
            ))
            .await
            .unwrap();
        let run_key = seed_open_pinned_workflow(
            &store,
            namespace_id,
            "deployment-a",
            "build-a",
            "workflow-tq",
            "workflow-a",
        )
        .await;

        let promoted = registry
            .set_current_version(set_current_cmd(
                namespace_id,
                "deployment-a",
                Some("build-b"),
                None,
            ))
            .await
            .unwrap();
        let draining = promoted
            .deployment
            .versions
            .iter()
            .find(|version| version.build_id == BuildId("build-a".to_string()))
            .unwrap();
        assert_eq!(
            draining.record.status,
            WorkerDeploymentVersionStatus::Draining
        );
        let drainage_info = draining.record.drainage_info.as_ref().unwrap();
        assert_eq!(drainage_info.status, VersionDrainageStatus::Draining);
        assert_eq!(drainage_info.last_changed_time, OffsetDateTime::UNIX_EPOCH);
        assert_eq!(drainage_info.last_checked_time, OffsetDateTime::UNIX_EPOCH);

        close_workflow(&store, run_key, "terminate-a").await;
        let refreshed = registry
            .refresh_version_drainage(
                namespace_id,
                DeploymentName("deployment-a".to_string()),
                BuildId("build-a".to_string()),
            )
            .await
            .unwrap();
        assert_eq!(
            refreshed.record.status,
            WorkerDeploymentVersionStatus::Drained
        );
        let drainage_info = refreshed.record.drainage_info.as_ref().unwrap();
        assert_eq!(drainage_info.status, VersionDrainageStatus::Drained);
        assert_eq!(drainage_info.last_changed_time, OffsetDateTime::UNIX_EPOCH);
        assert_eq!(drainage_info.last_checked_time, OffsetDateTime::UNIX_EPOCH);
    }

    #[tokio::test]
    async fn drainage_info_is_cleared_when_version_becomes_current_or_ramping_again() {
        let (registry, store) = registry_with_store();
        let namespace_id = NamespaceId::new();
        registry
            .create_deployment(create_cmd(namespace_id, "deployment-a", "request-a"))
            .await
            .unwrap();
        for build_id in ["build-a", "build-b"] {
            registry
                .create_version(create_version_cmd(
                    namespace_id,
                    "deployment-a",
                    build_id,
                    &format!("version-request-{build_id}"),
                ))
                .await
                .unwrap();
        }
        registry
            .set_current_version(set_current_cmd(
                namespace_id,
                "deployment-a",
                Some("build-a"),
                None,
            ))
            .await
            .unwrap();
        seed_open_pinned_workflow(
            &store,
            namespace_id,
            "deployment-a",
            "build-a",
            "workflow-tq",
            "workflow-a",
        )
        .await;
        registry
            .set_current_version(set_current_cmd(
                namespace_id,
                "deployment-a",
                Some("build-b"),
                None,
            ))
            .await
            .unwrap();

        let ramping = registry
            .set_ramping_version(set_ramping_cmd(
                namespace_id,
                "deployment-a",
                Some("build-a"),
                10.0,
                None,
            ))
            .await
            .unwrap();
        let reactivated = ramping
            .deployment
            .versions
            .iter()
            .find(|version| version.build_id == BuildId("build-a".to_string()))
            .unwrap();
        assert_eq!(
            reactivated.record.status,
            WorkerDeploymentVersionStatus::Ramping
        );
        assert!(reactivated.record.drainage_info.is_none());
    }

    #[tokio::test]
    async fn update_compute_config_applies_masks_removals_and_idempotency() {
        let (registry, _) = registry_with_store();
        let namespace_id = NamespaceId::new();
        registry
            .create_deployment(create_cmd(namespace_id, "deployment-a", "request-a"))
            .await
            .unwrap();
        registry
            .create_version(create_version_cmd(
                namespace_id,
                "deployment-a",
                "build-a",
                "version-request-a",
            ))
            .await
            .unwrap();

        let mut add =
            compute_update_cmd(namespace_id, "deployment-a", Some("build-a"), "compute-a");
        add.updates.insert(
            "primary".to_string(),
            ComputeConfigScalingGroupUpdate {
                scaling_group: scaling_group("aws-lambda", "rate-based"),
                update_mask: vec!["ignored_for_new_group".to_string()],
            },
        );
        assert!(matches!(
            registry.update_compute_config(add).await.unwrap_err(),
            RegistryError::InvalidArgument(_)
        ));

        let mut add =
            compute_update_cmd(namespace_id, "deployment-a", Some("build-a"), "compute-a");
        add.updates.insert(
            "primary".to_string(),
            ComputeConfigScalingGroupUpdate {
                scaling_group: scaling_group("aws-lambda", "rate-based"),
                update_mask: vec!["provider.type".to_string()],
            },
        );
        registry.update_compute_config(add.clone()).await.unwrap();

        let mut no_op =
            compute_update_cmd(namespace_id, "deployment-a", Some("build-a"), "compute-b");
        no_op.updates.insert(
            "primary".to_string(),
            ComputeConfigScalingGroupUpdate {
                scaling_group: scaling_group("aws-ecs", "no-sync"),
                update_mask: Vec::new(),
            },
        );
        registry.update_compute_config(no_op).await.unwrap();
        let unchanged = registry
            .describe_version(describe_version_cmd(
                namespace_id,
                "deployment-a",
                Some("build-a"),
                "",
                false,
            ))
            .await
            .unwrap();
        assert_eq!(
            unchanged
                .record
                .compute_config
                .scaling_groups
                .get("primary")
                .unwrap()
                .provider
                .as_ref()
                .unwrap()
                .provider_type,
            "aws-lambda"
        );

        let mut masked =
            compute_update_cmd(namespace_id, "deployment-a", Some("build-a"), "compute-c");
        masked.updates.insert(
            "primary".to_string(),
            ComputeConfigScalingGroupUpdate {
                scaling_group: scaling_group("aws-ecs", "no-sync"),
                update_mask: vec![
                    "provider.type".to_string(),
                    "scaler".to_string(),
                    "task_queue_types".to_string(),
                ],
            },
        );
        registry.update_compute_config(masked).await.unwrap();
        let updated = registry
            .describe_version(describe_version_cmd(
                namespace_id,
                "deployment-a",
                Some("build-a"),
                "",
                false,
            ))
            .await
            .unwrap();
        let group = updated
            .record
            .compute_config
            .scaling_groups
            .get("primary")
            .unwrap();
        assert_eq!(group.provider.as_ref().unwrap().provider_type, "aws-ecs");
        assert_eq!(
            group.provider.as_ref().unwrap().nexus_endpoint,
            "https://aws-lambda.example.com"
        );
        assert_eq!(group.scaler.as_ref().unwrap().scaler_type, "no-sync");
        assert_eq!(
            group.task_queue_types,
            vec![DeploymentTaskQueueType::Workflow]
        );
        assert_eq!(updated.record.last_modifier_identity, "operator-a");

        let mut repeat_with_invalid_body =
            compute_update_cmd(namespace_id, "deployment-a", Some("build-a"), "compute-c");
        repeat_with_invalid_body
            .removals
            .insert("primary".to_string());
        repeat_with_invalid_body.updates.insert(
            "primary".to_string(),
            ComputeConfigScalingGroupUpdate {
                scaling_group: scaling_group("invalid", "invalid"),
                update_mask: vec!["unknown".to_string()],
            },
        );
        registry
            .update_compute_config(repeat_with_invalid_body)
            .await
            .unwrap();
        let after_repeat = registry
            .describe_version(describe_version_cmd(
                namespace_id,
                "deployment-a",
                Some("build-a"),
                "",
                false,
            ))
            .await
            .unwrap();
        assert_eq!(
            after_repeat.record.compute_config,
            updated.record.compute_config
        );

        let mut remove =
            compute_update_cmd(namespace_id, "deployment-a", Some("build-a"), "compute-d");
        remove.removals.insert("primary".to_string());
        registry.update_compute_config(remove).await.unwrap();
        let removed = registry
            .describe_version(describe_version_cmd(
                namespace_id,
                "deployment-a",
                Some("build-a"),
                "",
                false,
            ))
            .await
            .unwrap();
        assert!(removed.record.compute_config.scaling_groups.is_empty());
    }

    #[tokio::test]
    async fn validate_compute_config_checks_input_without_registry_mutation_or_version_lookup() {
        let (registry, store) = registry_with_store();
        let namespace_id = NamespaceId::new();
        registry
            .create_deployment(create_cmd(namespace_id, "deployment-a", "request-a"))
            .await
            .unwrap();
        registry
            .create_version(create_version_cmd(
                namespace_id,
                "deployment-a",
                "build-a",
                "version-request-a",
            ))
            .await
            .unwrap();
        let before = store
            .load_deployment(&deployment_key(namespace_id, "deployment-a"))
            .await
            .unwrap();

        let mut valid = validate_compute_cmd(namespace_id, "deployment-a", Some("missing-build"));
        valid.updates.insert(
            "primary".to_string(),
            ComputeConfigScalingGroupUpdate {
                scaling_group: scaling_group("aws-lambda", "rate-based"),
                update_mask: vec!["provider.type".to_string()],
            },
        );
        registry.validate_compute_config(valid).await.unwrap();
        let after = store
            .load_deployment(&deployment_key(namespace_id, "deployment-a"))
            .await
            .unwrap();
        assert_eq!(after, before);

        let invalid_selector = validate_compute_cmd(namespace_id, "deployment-a", None);
        assert!(matches!(
            registry
                .validate_compute_config(invalid_selector)
                .await
                .unwrap_err(),
            RegistryError::InvalidArgument(_)
        ));

        let mut invalid_mask =
            validate_compute_cmd(namespace_id, "deployment-a", Some("missing-build"));
        invalid_mask.updates.insert(
            "primary".to_string(),
            ComputeConfigScalingGroupUpdate {
                scaling_group: scaling_group("aws-lambda", "rate-based"),
                update_mask: vec!["provider.unknown".to_string()],
            },
        );
        assert!(matches!(
            registry
                .validate_compute_config(invalid_mask)
                .await
                .unwrap_err(),
            RegistryError::InvalidArgument(_)
        ));
    }

    #[tokio::test]
    async fn update_version_metadata_applies_upserts_removals_and_returns_full_metadata() {
        let (registry, _) = registry_with_store();
        let namespace_id = NamespaceId::new();
        registry
            .create_deployment(create_cmd(namespace_id, "deployment-a", "request-a"))
            .await
            .unwrap();
        registry
            .create_version(create_version_cmd(
                namespace_id,
                "deployment-a",
                "build-a",
                "version-request-a",
            ))
            .await
            .unwrap();

        let mut upsert = metadata_cmd(namespace_id, "deployment-a", Some("build-a"), "");
        upsert
            .upsert_entries
            .insert("pipeline".to_string(), Payload::new("build-123"));
        upsert
            .upsert_entries
            .insert("owner".to_string(), Payload::new("team-a"));
        let first = registry.update_version_metadata(upsert).await.unwrap();
        assert_eq!(first.metadata.entries.len(), 2);
        assert_eq!(first.last_modifier_identity, "operator-a");

        let mut update = metadata_cmd(namespace_id, "deployment-a", None, "deployment-a:build-a");
        update
            .upsert_entries
            .insert("pipeline".to_string(), Payload::new("build-456"));
        update.remove_entries.insert("owner".to_string());
        let second = registry.update_version_metadata(update).await.unwrap();
        assert_eq!(second.metadata.entries.len(), 1);
        assert_eq!(
            second.metadata.entries.get("pipeline"),
            Some(&Payload::new("build-456"))
        );
        assert!(!second.metadata.entries.contains_key("owner"));

        let described = registry
            .describe_version(describe_version_cmd(
                namespace_id,
                "deployment-a",
                Some("build-a"),
                "",
                false,
            ))
            .await
            .unwrap();
        assert_eq!(described.record.metadata, second.metadata);
        assert_eq!(described.record.last_modifier_identity, "operator-a");

        let mut overlap = metadata_cmd(namespace_id, "deployment-a", Some("build-a"), "");
        overlap
            .upsert_entries
            .insert("pipeline".to_string(), Payload::new("ignored"));
        overlap.remove_entries.insert("pipeline".to_string());
        assert!(matches!(
            registry.update_version_metadata(overlap).await.unwrap_err(),
            RegistryError::InvalidArgument(_)
        ));
    }

    #[test]
    fn legacy_version_strings_parse_format_and_reject_malformed_values() {
        let deployment = DeploymentName("deployment-a".to_string());
        let build = BuildId("build-a".to_string());
        let formatted = format_legacy_version_string(&deployment, &build);
        assert_eq!(formatted, "deployment-a.build-a");
        assert_eq!(
            parse_legacy_version_string(&formatted).unwrap().unwrap(),
            WorkerDeploymentVersionKey {
                deployment_name: deployment.clone(),
                build_id: build.clone(),
            }
        );
        assert_eq!(
            parse_legacy_version_string("deployment-a:build-a")
                .unwrap()
                .unwrap(),
            WorkerDeploymentVersionKey {
                deployment_name: deployment,
                build_id: build,
            }
        );
        assert!(parse_legacy_version_string("").unwrap().is_none());
        assert!(
            parse_legacy_version_string(UNVERSIONED_VERSION_SENTINEL)
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            parse_legacy_version_string("malformed"),
            Err(RegistryError::InvalidArgument(_))
        ));
    }
}
