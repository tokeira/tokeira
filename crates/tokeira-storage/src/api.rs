//! Durable persistence contract (`RunRepository`) for the runtime and kernel.
//!
//! The interfaces here are intentionally semantic — callers talk in terms of
//! runs, history, timers, backlog, and leases rather than tables or SQL. This
//! keeps the rest of the workspace honest while allowing different backends to
//! materialise the same guarantees in very different physical layouts.
//!
//! Writes use an optimistic concurrency model: `commit_transition` checks the
//! caller's `TransitionSeq` against the durable value and returns `Conflict` on
//! mismatch, so the runtime can reload and retry without distributed locks.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use time::OffsetDateTime;
use tokeira_kernel::{
    ActivityOp, DispatchOp, HistoryEvent, LoadedRun, RequestIdInfo, TimerOp, Transition,
    WorkflowState,
    state::{
        NexusOperationCancellationState, Priority, VersioningBehavior, VersioningOverride,
        WorkerDeploymentVersionRef,
    },
};
use tokeira_types::{
    ArchetypeId, BuildId as RuntimeBuildId, DeploymentId, EventPrincipal, ExecutionRef,
    ExecutionStatus, GenerationCounter, Memo, NamespaceId, Payload, Payloads, ProjectionCursor,
    QueueKey, RequestId, RunId, RunKey, SearchAttrValue, SearchAttributes, ShardEpoch, ShardId,
    TaskKind, TaskQueueName, TransitionSeq, VisibilityLifecycleState, WorkerIdentity,
    WorkerTaskOrigin, WorkflowId, WorkflowRuleRecord, WorkflowType,
};
use uuid::Uuid;

const WORKER_TASK_PROVENANCE_DIGEST_DOMAIN: &[u8] = b"tokeira-worker-task-provenance-v1\0";

/// Compute the durable provenance key for exact public task-token bytes.
///
/// The digest is an index key, not a signature. Authorization comes from the
/// server-side provenance row, which a caller cannot create by changing token
/// bytes.
#[must_use]
pub fn worker_task_token_digest(token: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(WORKER_TASK_PROVENANCE_DIGEST_DOMAIN);
    hasher.update(token);
    hasher.finalize().into()
}

/// Server-authored authorization evidence for one public Worker task token.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerTaskProvenance {
    /// Domain-separated digest of the exact public token bytes.
    pub token_digest: [u8; 32],
    /// Exact final task origin observed after routing and task start.
    pub origin: WorkerTaskOrigin,
    /// Existing task deadline after which this row grants no authority.
    pub expires_at: OffsetDateTime,
    /// Edge insertion time retained only for diagnostics.
    pub created_at: OffsetDateTime,
}

/// Result of idempotently inserting one provenance record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProvenancePut {
    /// No row previously existed for the digest.
    Inserted,
    /// An exact equal row already existed.
    AlreadyPresent,
}

/// Durable provenance repository failure.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum WorkerTaskProvenanceError {
    /// Storage could not be reached or complete the operation.
    #[error("Worker task provenance storage unavailable: {message}")]
    Unavailable {
        /// Backend diagnostic without token or credential material.
        message: String,
    },
    /// The same cryptographic digest identified different record contents.
    #[error("Worker task provenance digest conflict")]
    DigestConflict,
    /// Durable row contents could not be decoded safely.
    #[error("Worker task provenance row is corrupt: {message}")]
    Corrupt {
        /// Bounded row-shape diagnostic without stored values.
        message: String,
    },
}

/// Durable authorization-evidence repository for scoped Worker task tokens.
///
/// This store never starts or completes work. Runtime fencing and task
/// correlation remain mandatory after any successful lookup.
#[async_trait]
pub trait WorkerTaskProvenanceStore: Send + Sync {
    /// Insert one record, accepting an exact duplicate idempotently.
    async fn put(
        &self,
        record: WorkerTaskProvenance,
    ) -> Result<ProvenancePut, WorkerTaskProvenanceError>;

    /// Load one non-expired record by exact token digest.
    async fn get(
        &self,
        token_digest: [u8; 32],
    ) -> Result<Option<WorkerTaskProvenance>, WorkerTaskProvenanceError>;

    /// Idempotently delete one record.
    async fn delete(&self, token_digest: [u8; 32]) -> Result<(), WorkerTaskProvenanceError>;

    /// Delete at most `limit` records expired at or before `now`.
    async fn delete_expired(
        &self,
        now: OffsetDateTime,
        limit: usize,
    ) -> Result<usize, WorkerTaskProvenanceError>;
}

/// Write result from persisting one authoritative
/// run transition.
///
/// See [`RunRepository::commit_transition`] for the
/// commit protocol and OCC fencing semantics.
#[derive(Clone, Debug, PartialEq)]
pub enum CommitResult {
    /// Transition was durably applied; contains the
    /// new authoritative workflow state.
    Applied { new_state: WorkflowState },
    /// OCC fence check failed — the durable
    /// `transition_seq` has moved past the expected
    /// value. The runtime should reload and retry.
    Conflict { reason: String },
    /// A start collided with an existing OPEN current execution for the same
    /// `(namespace, workflow_id)`. Unlike [`Conflict`](Self::Conflict) — a
    /// transient OCC/CAS collision the runtime retries — this is a
    /// **non-retryable** current-execution conflict: the runtime resolves it by
    /// the request's `WorkflowIdConflictPolicy` (Fail → already-started;
    /// UseExisting → attach; TerminateExisting → terminate+start). It carries the
    /// incumbent's identity and its request-id map so the runtime/edge can build
    /// the already-started error (and its request-id detail) without a second
    /// load. This is an internal commit outcome; it MUST NOT reach a client.
    CurrentExecutionConflict {
        /// Run that currently owns the `(namespace, workflow_id)` pointer.
        existing_run_key: RunKey,
        /// Lifecycle status of the incumbent (always open here).
        existing_status: ExecutionStatus,
        /// The incumbent's request-id → authoring-event map, for the
        /// already-started error detail (`WorkflowExecutionInfo.request_ids @
        /// v1.31.0`).
        request_ids: Vec<(String, RequestIdInfo)>,
    },
    /// A request with the same dedupe key was already
    /// committed. The caller can short-circuit.
    Duplicate,
}

/// Inputs to one authoritative workflow-run deletion.
///
/// Deletion is operational cleanup rather than a kernel transition, but it is
/// fenced by the same per-run sequence used for normal commits. `deleted_at` is
/// supplied by the runtime so projection output remains deterministic in tests
/// and storage never invents request time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeleteRunRequest {
    /// Durable transition sequence observed immediately before deletion.
    pub expected_seq: TransitionSeq,
    /// Admission time recorded on the deletion visibility tombstone.
    pub deleted_at: OffsetDateTime,
}

/// Result of attempting an authoritative workflow-run deletion.
#[derive(Clone, Debug, PartialEq)]
pub enum DeleteRunResult {
    /// The run was purged and the returned tombstone was durably appended to
    /// the projection log in the same semantic write.
    Deleted {
        /// Versioned deletion image for synchronous and background projection.
        tombstone: ProjectionRecord,
    },
    /// The target run did not exist when the fenced deletion was admitted.
    NotFound,
    /// The sequence or shard-epoch fence no longer matched durable state.
    Conflict {
        /// Diagnostic reason; callers reload the same run and retry.
        reason: String,
    },
}

/// Encoded byte length for Worker Deployment conflict tokens.
pub const CONFLICT_TOKEN_BYTES: usize = 8;

/// Namespace-scoped Worker Deployment name.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DeploymentName(pub String);

/// Worker Deployment Version build identifier.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BuildId(pub String);

/// Storage key for a Worker Deployment registry record.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DeploymentKey {
    /// Namespace that owns the deployment.
    pub namespace_id: NamespaceId,
    /// Deployment name unique within the namespace.
    pub deployment_name: DeploymentName,
}

/// Proto-shaped reference to one Worker Deployment Version.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct WorkerDeploymentVersionKey {
    /// Deployment that owns the version.
    pub deployment_name: DeploymentName,
    /// Build identifier unique within the deployment.
    pub build_id: BuildId,
}

/// Opaque optimistic-concurrency token for Worker Deployment records.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConflictToken(pub [u8; CONFLICT_TOKEN_BYTES]);

impl ConflictToken {
    /// Encode a monotonic generation as an opaque token.
    pub fn from_generation(generation: u64) -> Self {
        Self(generation.to_be_bytes())
    }

    /// Decode the monotonic generation carried by this token.
    pub fn generation(self) -> u64 {
        u64::from_be_bytes(self.0)
    }
}

/// CAS write result for Worker Deployment registry records.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeploymentCasResult {
    /// The write was applied and a fresh token was assigned.
    Applied { token: ConflictToken },
    /// The expected token did not match the stored token.
    Conflict,
    /// The existing-record operation targeted a missing record.
    NotFound,
    /// The create operation targeted an existing record.
    AlreadyExists,
}

/// Task category whose live queue policy is stored independently.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum StoredTaskQueueConfigKind {
    /// Workflow task queue.
    Workflow,
    /// Activity task queue.
    Activity,
    /// Nexus worker task queue.
    Nexus,
}

/// Durable identity of one task-queue policy record.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StoredTaskQueueConfigKey {
    /// Namespace containing the task queue.
    pub namespace_id: NamespaceId,
    /// Logical task-queue name.
    pub task_queue: TaskQueueName,
    /// Independently configured task category.
    pub kind: StoredTaskQueueConfigKind,
}

/// Audit metadata retained with a rate-limit update or explicit unset.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredTaskQueueConfigMetadata {
    /// Caller-supplied reason.
    pub reason: String,
    /// Identity that issued the update.
    pub update_identity: String,
    /// Time the update was accepted.
    pub update_time: OffsetDateTime,
}

/// Complete durable policy for one task queue.
///
/// This control-plane record is not workflow state and never enters history.
/// Its revision fences concurrent public API updates while the runtime cache
/// remains disposable and reconstructible.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StoredTaskQueueConfig {
    /// Namespace containing the task queue.
    pub namespace_id: NamespaceId,
    /// Logical task-queue name.
    pub task_queue: TaskQueueName,
    /// Independently configured task category.
    pub kind: StoredTaskQueueConfigKind,
    /// Monotonic repository-assigned revision, starting at one.
    pub revision: u64,
    /// Optional total dispatch rate in tasks per second.
    pub queue_rate_limit: Option<f32>,
    /// Metadata for the latest total-rate update or explicit unset.
    pub queue_rate_limit_metadata: Option<StoredTaskQueueConfigMetadata>,
    /// Optional default rate for each fairness key.
    pub fairness_key_rate_limit_default: Option<f32>,
    /// Metadata for the latest per-key-rate update or explicit unset.
    pub fairness_key_rate_limit_metadata: Option<StoredTaskQueueConfigMetadata>,
    /// Complete fairness-key weight override map.
    pub fairness_weight_overrides: BTreeMap<String, f32>,
}

impl StoredTaskQueueConfig {
    /// Return the durable identity of this record.
    #[must_use]
    pub fn key(&self) -> StoredTaskQueueConfigKey {
        StoredTaskQueueConfigKey {
            namespace_id: self.namespace_id,
            task_queue: self.task_queue.clone(),
            kind: self.kind,
        }
    }
}

/// Result of conditionally writing a task-queue policy record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskQueueConfigCasResult {
    /// The write applied at the returned repository-assigned revision.
    Applied {
        /// Fresh durable revision.
        revision: u64,
    },
    /// The expected revision did not match durable state.
    Conflict,
}

/// Durable repository for public task-queue policy.
#[async_trait]
pub trait TaskQueueConfigRepository: Send + Sync {
    /// Load one policy record by its complete identity.
    async fn load_task_queue_config(
        &self,
        key: &StoredTaskQueueConfigKey,
    ) -> Result<Option<StoredTaskQueueConfig>>;

    /// Conditionally create or replace one complete record.
    ///
    /// `None` is the create fence and requires no current record. `Some(n)`
    /// requires revision `n`. The repository assigns `1` or `n + 1` and ignores
    /// the candidate's incoming revision so stale caller metadata cannot choose
    /// a durable fence.
    async fn compare_and_swap_task_queue_config(
        &self,
        record: StoredTaskQueueConfig,
        expected_revision: Option<u64>,
    ) -> Result<TaskQueueConfigCasResult>;

    /// List every record in deterministic key order for startup hydration.
    async fn list_all_task_queue_configs(&self) -> Result<Vec<StoredTaskQueueConfig>>;
}

/// Atomic result of creating a namespace Workflow Rule.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkflowRuleCreateResult {
    /// The rule was stored, possibly after capacity-driven expiration eviction.
    Created,
    /// The namespace already contains the requested rule id.
    AlreadyExists,
    /// No capacity was available after applying v1.31.0's eviction rule.
    LimitExceeded,
}

/// Atomic result of deleting a namespace Workflow Rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkflowRuleDeleteResult {
    /// The named rule was removed.
    Deleted,
    /// The namespace did not contain the named rule.
    NotFound,
}

/// Stored form of `RoutingConfigUpdateState`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoutingConfigUpdateState {
    /// No routing propagation state has been assigned yet.
    #[default]
    Unspecified,
    /// Routing propagation is still in progress.
    InProgress,
    /// Routing propagation has completed.
    Completed,
}

/// Stored form of `WorkerDeploymentVersionStatus`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkerDeploymentVersionStatus {
    /// No version status has been assigned yet.
    #[default]
    Unspecified,
    /// Version exists but is not current, ramping, or draining.
    Inactive,
    /// Version is the deployment current version.
    Current,
    /// Version is the deployment ramping version.
    Ramping,
    /// Version is draining open pinned workflows.
    Draining,
    /// Version has drained open pinned workflows.
    Drained,
    /// Version was explicitly created before pollers appeared.
    Created,
}

/// Stored form of `VersionDrainageStatus`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum VersionDrainageStatus {
    /// No drainage status has been assigned yet.
    #[default]
    Unspecified,
    /// Version still has open pinned workflows.
    Draining,
    /// Version no longer has open pinned workflows.
    Drained,
}

/// Stored form of `TaskQueueType`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum DeploymentTaskQueueType {
    /// No task queue type has been assigned yet.
    #[default]
    Unspecified,
    /// Workflow task queue.
    Workflow,
    /// Activity task queue.
    Activity,
    /// Nexus task queue.
    Nexus,
}

/// Task queue ever polled by a Worker Deployment Version.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct VersionTaskQueue {
    /// Task queue name.
    pub name: String,
    /// Task queue type.
    pub task_queue_type: DeploymentTaskQueueType,
}

/// Stored form of `ComputeProvider`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComputeProvider {
    /// Implementation-specific provider type.
    pub provider_type: String,
    /// Opaque provider-specific configuration.
    pub details: Option<Payload>,
    /// Optional Nexus endpoint for remote providers.
    pub nexus_endpoint: String,
}

/// Stored form of `ComputeScaler`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComputeScaler {
    /// Implementation-specific scaler type.
    pub scaler_type: String,
    /// Opaque scaler-specific configuration.
    pub details: Option<Payload>,
}

/// Stored form of `ComputeConfigScalingGroup`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComputeConfigScalingGroup {
    /// Task queue types served by this scaling group.
    pub task_queue_types: Vec<DeploymentTaskQueueType>,
    /// Worker lifecycle provider instructions.
    pub provider: Option<ComputeProvider>,
    /// Worker lifecycle scaling instructions.
    pub scaler: Option<ComputeScaler>,
}

/// Stored form of `ComputeConfig`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComputeConfig {
    /// Scaling groups keyed by caller-supplied group id.
    pub scaling_groups: BTreeMap<String, ComputeConfigScalingGroup>,
}

/// Stored form of `VersionMetadata`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionMetadata {
    /// User-defined opaque metadata values.
    pub entries: BTreeMap<String, Payload>,
}

/// Stored form of `RoutingConfig`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StoredRoutingConfig {
    /// Current version for auto-upgrade traffic; absent means unversioned.
    pub current_version: Option<WorkerDeploymentVersionKey>,
    /// Ramping version; absent means unversioned workers.
    pub ramping_version: Option<WorkerDeploymentVersionKey>,
    /// Percentage of eligible traffic shifted to the ramping version.
    pub ramping_version_percentage: f32,
    /// Whether the ramp targets unversioned workers (nil ramping version) at a
    /// non-zero percentage. Distinguishes "ramping to unversioned" from "no ramp":
    /// both leave `ramping_version` nil, but v1.31.0 renders the deprecated
    /// `ramping_version` string as `__unversioned__` only for the former
    /// (`ExternalWorkerDeploymentVersionToStringV31` of nil, gated on a real ramp).
    #[serde(default)]
    pub ramping_to_unversioned: bool,
    /// Last current-version change time.
    pub current_version_changed_time: Option<OffsetDateTime>,
    /// Last ramping-version change time.
    pub ramping_version_changed_time: Option<OffsetDateTime>,
    /// Last ramping percentage change time.
    pub ramping_version_percentage_changed_time: Option<OffsetDateTime>,
    /// Monotonic routing revision.
    pub revision_number: i64,
    /// Revision at which `current_version` last changed.
    ///
    /// Dispatch compares this target-specific revision with inherited
    /// AutoUpgrade source state so a later ramp update cannot make an older
    /// Current target appear newer (`chooseTargetQueueByFlag`,
    /// `task_queue_partition_manager.go:2061-2078 @ v1.31.0`).
    #[serde(default)]
    pub current_version_revision_number: i64,
    /// Revision at which the ramping Version or percentage last changed.
    ///
    /// Kept separately from the aggregate revision for the same no-bounce
    /// comparison as `current_version_revision_number`.
    #[serde(default)]
    pub ramping_version_revision_number: i64,
}

impl Default for StoredRoutingConfig {
    fn default() -> Self {
        Self {
            current_version: None,
            ramping_version: None,
            ramping_version_percentage: 0.0,
            ramping_to_unversioned: false,
            current_version_changed_time: None,
            ramping_version_changed_time: None,
            ramping_version_percentage_changed_time: None,
            revision_number: 0,
            current_version_revision_number: 0,
            ramping_version_revision_number: 0,
        }
    }
}

/// Stored form of `VersionDrainageInfo`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DrainageInfo {
    /// Drainage lifecycle status.
    pub status: VersionDrainageStatus,
    /// Last status change time.
    pub last_changed_time: OffsetDateTime,
    /// Last drainage check time.
    pub last_checked_time: OffsetDateTime,
}

/// Stored Worker Deployment Version record.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StoredVersion {
    /// Build id of this version within its deployment.
    pub build_id: BuildId,
    /// Version lifecycle status.
    pub status: WorkerDeploymentVersionStatus,
    /// Version creation time.
    pub create_time: OffsetDateTime,
    /// Last current/ramping/ramp-percentage change time for this version.
    pub routing_changed_time: Option<OffsetDateTime>,
    /// Time this version most recently became current.
    pub current_since_time: Option<OffsetDateTime>,
    /// Time this version most recently became ramping.
    pub ramping_since_time: Option<OffsetDateTime>,
    /// First time this version became current or ramping.
    pub first_activation_time: Option<OffsetDateTime>,
    /// Last time this version became current.
    pub last_current_time: Option<OffsetDateTime>,
    /// Last time this version stopped being current or ramping.
    pub last_deactivation_time: Option<OffsetDateTime>,
    /// Current ramp percentage for this version.
    pub ramp_percentage: f32,
    /// Drainage information, absent while current or ramping.
    pub drainage_info: Option<DrainageInfo>,
    /// User-defined version metadata.
    pub metadata: VersionMetadata,
    /// Worker compute configuration.
    pub compute_config: ComputeConfig,
    /// Identity of the last caller that modified this version.
    pub last_modifier_identity: String,
    /// Task queues ever polled by this version.
    pub polled_task_queues: BTreeSet<VersionTaskQueue>,
    /// Create-version request ids accepted for this version.
    pub create_request_ids: BTreeSet<String>,
    /// Compute-config update request ids accepted for this version.
    #[serde(default)]
    pub compute_config_request_ids: BTreeSet<String>,
}

/// Stored Worker Deployment registry record.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StoredWorkerDeployment {
    /// Namespace that owns this deployment.
    pub namespace_id: NamespaceId,
    /// Deployment name unique within the namespace.
    pub name: DeploymentName,
    /// Deployment creation time.
    pub create_time: OffsetDateTime,
    /// Routing configuration and revision.
    pub routing_config: StoredRoutingConfig,
    /// Identity of the last caller that modified deployment-level configuration.
    pub last_modifier_identity: String,
    /// Manager identity, absent when unset.
    pub manager_identity: Option<String>,
    /// Routing propagation state reported to callers.
    pub routing_config_update_state: RoutingConfigUpdateState,
    /// Versions keyed by build id so one CAS write covers routing and version state.
    pub versions: BTreeMap<BuildId, StoredVersion>,
    /// Current optimistic-concurrency token.
    pub conflict_token: ConflictToken,
    /// Create-deployment request ids accepted for this deployment.
    pub create_request_ids: BTreeSet<String>,
}

#[async_trait]
pub trait WorkerDeploymentRepository: Send + Sync {
    /// Load a deployment registry record by namespace/name.
    async fn load_deployment(&self, key: &DeploymentKey) -> Result<Option<StoredWorkerDeployment>>;

    /// Conditionally write a deployment registry record.
    ///
    /// `expected == None` is the create path and requires the record to be absent.
    /// `expected == Some(token)` requires the stored token to match before applying.
    async fn put_deployment(
        &self,
        record: StoredWorkerDeployment,
        expected: Option<ConflictToken>,
    ) -> Result<DeploymentCasResult>;

    /// Delete a deployment registry record only if its token matches `expected`.
    async fn delete_deployment(
        &self,
        key: &DeploymentKey,
        expected: ConflictToken,
    ) -> Result<DeploymentCasResult>;

    /// List deployment records in deterministic name order after an optional cursor.
    async fn list_deployments(
        &self,
        namespace_id: NamespaceId,
        after: Option<&DeploymentName>,
        limit: usize,
    ) -> Result<Vec<StoredWorkerDeployment>>;

    /// Load every deployment record for restart recovery.
    async fn list_all_for_namespace(
        &self,
        namespace_id: NamespaceId,
    ) -> Result<Vec<StoredWorkerDeployment>>;
}

/// Durable request-dedupe record.
///
/// Insight: the request id is stored at workflow scope here because the minimal
/// workspace has not yet implemented continue-as-new chains or reuse policies.
/// That is intentionally conservative: it prevents ambiguous duplicate handling
/// now, and leaves a clear TODO for future refinement when execution-chain
/// semantics become richer.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RequestRecord {
    /// Namespace that owns this workflow.
    pub namespace_id: NamespaceId,
    /// Workflow-scoped identifier for dedupe lookup.
    pub workflow_id: WorkflowId,
    /// Run that first committed this request.
    pub run_id: RunId,
    /// Durable storage key for the owning run.
    pub run_key: RunKey,
    /// The external request id being deduplicated.
    pub request_id: RequestId,
    /// Transition that first persisted this request.
    pub first_seen_transition_seq: TransitionSeq,
}

/// Development/test view of one persisted transition.
///
/// Production DSQL storage will likely materialize this information across
/// multiple tables. The dev store keeps an audit-style record so semantic tests
/// can verify that history and derived ops are all persisted together.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TransitionAuditRecord {
    /// Durable storage key for the run.
    pub run_key: RunKey,
    /// Monotonic sequence assigned to this transition.
    pub transition_seq: TransitionSeq,
    /// History events appended by this transition.
    pub history_events: Vec<HistoryEvent>,
    /// Activity side-table mutations.
    pub activity_ops: Vec<ActivityOp>,
    /// Timer bucket mutations.
    pub timer_ops: Vec<TimerOp>,
    /// Dispatch queue operations (enqueue tasks).
    pub dispatch_ops: Vec<DispatchOp>,
}

/// One authoritative history event paired with its server-computed attribution.
///
/// Attribution is physically stored as a batch-aligned sidecar so the existing
/// positional `HistoryEvent` postcard encoding remains stable. The repository
/// exposes the logical pair to prevent callers from accidentally losing that
/// association while paging or reversing history.
#[derive(Clone, Debug, PartialEq)]
pub struct AttributedHistoryEvent {
    /// Authoritative kernel history event.
    pub event: HistoryEvent,
    /// Authenticated caller that authored the event, if propagation was active.
    pub principal: Option<EventPrincipal>,
}

/// Query surface the runtime needs from storage.
///
/// The interface is intentionally shaped around semantics rather than a
/// particular SQL schema. That keeps the rest of the workspace honest: callers
/// ask for "resolve this execution reference" or "read history", not "query
/// table X with join Y".
#[async_trait]
pub trait RunRepository: Send + Sync {
    /// Resolve an execution reference to a concrete durable run key.
    ///
    /// Semantics:
    /// - when `execution.run_id` is `None`, return the current open run if one
    ///   exists;
    /// - when `execution.run_id` is `Some`, return that specific run if known,
    ///   even if it is closed.
    async fn resolve_execution(&self, execution: &ExecutionRef) -> Result<Option<RunKey>>;

    /// Resolve the latest known run for a workflow, whether open or closed.
    ///
    /// This is used by workflow-id reuse/conflict resolution paths that need
    /// to distinguish "no run has ever existed" from "the last run is closed".
    async fn find_latest_run(
        &self,
        namespace_id: NamespaceId,
        workflow_id: &WorkflowId,
    ) -> Result<Option<RunKey>>;

    /// List every authoritative run owned by `namespace_id`.
    ///
    /// Namespace reclaim must not trust visibility because projection lag or a
    /// missing visibility row could otherwise strand mutable state. Implementations
    /// therefore enumerate their authoritative hot-state records and return keys in
    /// deterministic UUID order.
    async fn list_runs_for_namespace(&self, namespace_id: NamespaceId) -> Result<Vec<RunKey>>;

    /// Load the full durable state for a run, or
    /// [`LoadedRun::Absent`] if the key is unknown.
    async fn load_run(&self, run_key: RunKey) -> Result<LoadedRun>;

    /// Read the authoritative history stream after a known event id.
    async fn read_history(
        &self,
        run_key: RunKey,
        after_event_id: i64,
        limit: usize,
    ) -> Result<Vec<HistoryEvent>>;

    /// Read authoritative history with durable event attribution.
    ///
    /// Implementations predating attribution may rely on this default, which
    /// treats every event as principal-absent. Durable production backends
    /// override it to decode the batch sidecar atomically with the event blob.
    async fn read_attributed_history(
        &self,
        run_key: RunKey,
        after_event_id: i64,
        limit: usize,
    ) -> Result<Vec<AttributedHistoryEvent>> {
        Ok(self
            .read_history(run_key, after_event_id, limit)
            .await?
            .into_iter()
            .map(|event| AttributedHistoryEvent {
                event,
                principal: None,
            })
            .collect())
    }

    /// Lookup request dedupe state for a workflow execution reference.
    async fn lookup_request_dedupe(
        &self,
        execution: &ExecutionRef,
        request_id: &RequestId,
    ) -> Result<Option<RequestRecord>>;

    /// Read the persisted transition audit log for a run.
    ///
    /// TODO(storage): once a real DSQL backend lands, decide whether this stays
    /// in the main trait, moves behind a test-only feature, or becomes an admin
    /// API. It is extremely useful for semantic tests right now.
    async fn read_transition_audit(&self, run_key: RunKey) -> Result<Vec<TransitionAuditRecord>>;

    /// Return whether any open execution is pinned to the given Worker
    /// Deployment Version.
    async fn has_open_pinned_workflows(
        &self,
        _namespace_id: NamespaceId,
        _version: &WorkerDeploymentVersionKey,
    ) -> Result<bool> {
        Ok(false)
    }

    /// Return whether an exact Deployment-Version task queue has authoritative task
    /// pressure since `rate_window_start`.
    ///
    /// v1.31.0 protects a current/ramping change only when a target-missing queue has
    /// backlog or non-zero recent add-rate (`isTaskQueueExpectedInNewVersion`,
    /// `service/worker/workerdeployment/client.go:1859-1926 @ v1.31.0`). Tokeira
    /// derives both signals from durable dispatch state rather than its disposable
    /// brokers: currently dispatchable tasks are backlog, while recently committed
    /// enqueue effects are the add-rate window.
    async fn has_deployment_task_queue_pressure(
        &self,
        namespace_id: NamespaceId,
        version: &WorkerDeploymentVersionKey,
        task_queue: &VersionTaskQueue,
        rate_window_start: OffsetDateTime,
    ) -> Result<bool> {
        let task_kind = match task_queue.task_queue_type {
            DeploymentTaskQueueType::Workflow => TaskKind::Workflow,
            DeploymentTaskQueueType::Activity => TaskKind::Activity,
            // Nexus queue resolution passes through endpoint state after the kernel
            // emits its durable operation effect, so no exact physical QueueKey is
            // available at this repository boundary.
            DeploymentTaskQueueType::Nexus | DeploymentTaskQueueType::Unspecified => {
                return Ok(false);
            }
        };
        let queue = QueueKey {
            namespace_id,
            task_queue: TaskQueueName(task_queue.name.clone()),
            task_kind,
            deployment: Some(DeploymentId(version.deployment_name.0.clone())),
            build_id: Some(RuntimeBuildId(version.build_id.0.clone())),
        };

        let has_backlog = match task_kind {
            TaskKind::Workflow => !self
                .list_dispatchable_workflow_tasks(&queue, 1)
                .await?
                .is_empty(),
            // Drainage counts a future-eligible retry as pending work, so this
            // deliberately uses the all-row inspection query, not the due-only
            // delivery query.
            TaskKind::Activity => !self
                .list_all_dispatchable_activity_tasks(&queue, 1)
                .await?
                .is_empty(),
        };
        if has_backlog {
            return Ok(true);
        }

        for run_key in self.list_runs_for_namespace(namespace_id).await? {
            for audit in self.read_transition_audit(run_key).await? {
                let happened_at = audit
                    .history_events
                    .iter()
                    .map(|event| event.happened_at)
                    .max();
                let recently_committed = happened_at.is_some_and(|at| at >= rate_window_start);
                if !recently_committed {
                    continue;
                }
                if audit.dispatch_ops.iter().any(|operation| match operation {
                    DispatchOp::EnqueueWorkflowTask { queue: added, .. } => {
                        task_kind == TaskKind::Workflow && added == &queue
                    }
                    DispatchOp::EnqueueActivityTask { queue: added, .. } => {
                        task_kind == TaskKind::Activity && added == &queue
                    }
                    _ => false,
                }) {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Atomically create a durable namespace Workflow Rule.
    ///
    /// When the namespace is at `max_rules`, the implementation first evicts the stored rule with
    /// the earliest non-null expiration time, matching namespace configuration behavior in
    /// `service/frontend/namespace_handler.go @ v1.31.0`.
    async fn create_workflow_rule(
        &self,
        _namespace_id: NamespaceId,
        _rule: WorkflowRuleRecord,
        _max_rules: usize,
    ) -> Result<WorkflowRuleCreateResult> {
        Err(anyhow!("workflow rule storage is not supported"))
    }

    /// Load one durable namespace Workflow Rule without applying expiration filtering.
    async fn get_workflow_rule(
        &self,
        _namespace_id: NamespaceId,
        _rule_id: &str,
    ) -> Result<Option<WorkflowRuleRecord>> {
        Ok(None)
    }

    /// Delete one durable namespace Workflow Rule atomically.
    async fn delete_workflow_rule(
        &self,
        _namespace_id: NamespaceId,
        _rule_id: &str,
    ) -> Result<WorkflowRuleDeleteResult> {
        Ok(WorkflowRuleDeleteResult::NotFound)
    }

    /// List every durable Workflow Rule for a namespace in stable id order.
    ///
    /// Expired records remain visible until explicit deletion or capacity eviction.
    async fn list_workflow_rules(
        &self,
        _namespace_id: NamespaceId,
    ) -> Result<Vec<WorkflowRuleRecord>> {
        Ok(Vec::new())
    }

    /// Atomically persist a kernel-produced transition.
    ///
    /// The implementation must check `transition.expected_seq`
    /// against the durable `transition_seq` and return
    /// [`CommitResult::Conflict`] on mismatch (OCC fence).
    /// See the [storage architecture docs](../../docs/crates/storage.md)
    /// for the full fenced-commit model.
    ///
    /// A successful implementation must persist the post-transition state
    /// together with history and derived side effects as one semantic unit, or
    /// fail without partially exposing the result.
    async fn commit_transition(
        &self,
        run_key: RunKey,
        transition: Transition,
        epoch: ShardEpoch,
    ) -> Result<CommitResult>;

    /// Atomically persist a transition fenced by the caller-resolved
    /// execution-home bundle.
    ///
    /// This is the placement-aware form used by runtime/edge paths. The legacy
    /// [`RunRepository::commit_transition`] entry point remains for tests and
    /// pre-placement callers, but production routing must carry the bundle that
    /// was resolved from `(namespace_id, workflow_id)`.
    async fn commit_transition_for_bundle(
        &self,
        run_key: RunKey,
        execution_home_bundle: ShardId,
        transition: Transition,
        epoch: ShardEpoch,
    ) -> Result<CommitResult>;

    /// Atomically purge one run under OCC and execution-home shard fencing.
    ///
    /// A successful implementation must append the returned deletion
    /// tombstone and remove the run's mutable state, history, current pointer
    /// (only when it still names this run), and run-owned dispatch/sweep rows as
    /// one semantic write. A mismatch returns [`DeleteRunResult::Conflict`]
    /// without exposing a partial purge.
    async fn delete_run_for_bundle(
        &self,
        run_key: RunKey,
        execution_home_bundle: ShardId,
        request: DeleteRunRequest,
        epoch: ShardEpoch,
    ) -> Result<DeleteRunResult>;

    /// Materialize a reset successor by copying the base run's committed
    /// history prefix through `fork_event_id` and deriving the successor state
    /// by replaying that prefix.
    async fn materialize_reset_successor(
        &self,
        base_run_key: RunKey,
        fork_event_id: i64,
        successor_run_id: RunId,
    ) -> Result<()>;

    /// Return workflow tasks that are scheduled but not
    /// yet started for the given queue, up to `limit`.
    async fn list_dispatchable_workflow_tasks(
        &self,
        queue: &QueueKey,
        limit: usize,
    ) -> Result<Vec<DispatchableWorkflowTask>>;

    /// Return activity tasks whose durable eligibility time (`dispatch_at`)
    /// is at or before `now` for the given queue, up to `limit`, in
    /// `(dispatch_at, insertion)` order.
    ///
    /// This is the DELIVERY query: a retry attempt inside its backoff window
    /// is durably present but deliberately not returned, mirroring v1.31.0
    /// where the retry reaches matching only when its durable retry timer
    /// fires (`GenerateActivityRetryTasks` → `executeActivityRetryTimerTask`,
    /// timer_queue_active_task_executor.go:522-620 @ v1.31.0).
    async fn list_due_dispatchable_activity_tasks(
        &self,
        queue: &QueueKey,
        now: OffsetDateTime,
        limit: usize,
    ) -> Result<Vec<DispatchableActivityTask>>;

    /// Return every activity dispatch row for the queue regardless of
    /// eligibility time, up to `limit`.
    ///
    /// Inspection/drainage only — a future-eligible retry still counts as
    /// undrained pending work. NEVER use this for delivery; delivery goes
    /// through [`RunRepository::list_due_dispatchable_activity_tasks`].
    async fn list_all_dispatchable_activity_tasks(
        &self,
        queue: &QueueKey,
        limit: usize,
    ) -> Result<Vec<DispatchableActivityTask>>;

    /// Durably persist unmatched tasks to the dispatch
    /// backlog for later retry.
    async fn persist_to_backlog(&self, entries: Vec<BacklogEntry>) -> Result<()>;

    /// Remove and return up to `limit` backlog entries for the given queue in
    /// ascending [`DeliveryOrder`].
    async fn drain_backlog(&self, queue: &QueueKey, limit: usize) -> Result<Vec<BacklogEntry>>;

    /// Snapshot durable backlog counts and oldest schedule time by priority band.
    async fn backlog_stats_by_priority(
        &self,
        _queue: &QueueKey,
    ) -> Result<BTreeMap<i16, BacklogBandStats>> {
        Ok(BTreeMap::new())
    }

    /// Enumerate exact-version durable backlog queue identities.
    ///
    /// This low-frequency advisory scan lets capacity sampling recover queue
    /// discovery after process-local broker/counter loss. Implementations that
    /// predate worker compute may return an empty set.
    async fn list_versioned_backlog_queue_keys(&self) -> Result<Vec<QueueKey>> {
        Ok(Vec::new())
    }

    /// Return timers whose `fire_at` is at or before
    /// `now`, up to `limit`.
    async fn list_due_timers(&self, now: OffsetDateTime, limit: usize) -> Result<Vec<DueTimer>>;

    // ── Shard-filtered sweep queries ────────────────────

    /// List dispatchable workflow tasks for a specific
    /// shard.
    async fn list_dispatchable_workflow_tasks_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<DispatchableWorkflowTask>>;

    /// List activity dispatch rows due at `now` for a specific shard, in
    /// `(dispatch_at, insertion)` order, carrying each row's durable
    /// eligibility time so reconciliation can identify the exact row version
    /// it observed (see
    /// [`RunRepository::delete_activity_dispatch_if_matches`]).
    ///
    /// Due-only for the same reason as the queue variant: recovery and
    /// reconciliation must not surface a retry before its backoff elapses.
    async fn list_due_dispatchable_activity_tasks_for_shard(
        &self,
        shard_id: ShardId,
        now: OffsetDateTime,
        limit: usize,
    ) -> Result<Vec<DueActivityDispatch>>;

    /// Delete one activity dispatch row, but only if the durable row still
    /// matches the exact version `candidate` was observed at.
    ///
    /// This is the reconciler's cleanup for rows proven permanently stale
    /// against authoritative run state. The version comparison (schedule
    /// event, attempt, stamp, routing revision, and eligibility time) makes
    /// the delete a no-op when a concurrent retry/options transition has
    /// already replaced the row with a newer live dispatch — every such
    /// transition changes at least one compared field. Returns whether a row
    /// was removed.
    async fn delete_activity_dispatch_if_matches(
        &self,
        candidate: &ActivityDispatchIdentity,
    ) -> Result<bool>;

    /// List due timers for a specific shard.
    async fn list_due_timers_for_shard(
        &self,
        shard_id: ShardId,
        now: OffsetDateTime,
        limit: usize,
    ) -> Result<Vec<DueTimer>>;

    /// List open runs with workflow timeout configuration
    /// for a shard (for sweep reconstruction).
    async fn list_runs_with_workflow_timeouts_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<WorkflowTimeoutSweepEntry>>;

    /// List started workflow tasks for a shard (for WFT timeout
    /// tracking reconstruction).
    async fn list_started_workflow_tasks_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<WftTimeoutSweepEntry>>;

    /// List open activities for a shard (for timeout
    /// tracking reconstruction).
    async fn list_open_activities_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<ActivitySweepEntry>>;

    /// List pending Nexus operations with timeouts for a
    /// shard (for timeout tracking reconstruction).
    async fn list_pending_nexus_operations_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<NexusSweepEntry>>;

    /// List currently eligible exact-version Nexus deliveries for advisory backlog
    /// reconstruction after broker-memory loss.
    ///
    /// Endpoint targets remain unresolved here; runtime owns that registry and
    /// filters Worker targets without moving routing policy into storage.
    async fn list_reconstructible_nexus_deliveries_for_shard(
        &self,
        _shard_id: ShardId,
        _now: OffsetDateTime,
        _limit: usize,
    ) -> Result<Vec<ReconstructibleNexusDelivery>> {
        Ok(Vec::new())
    }

    /// List the *pending* (`Scheduled` or `BackingOff`) completion callbacks of runs homed
    /// on `shard_id`, so the completion-callback retry scanner can rebuild its volatile
    /// index after a shard takeover (mirrors `list_pending_nexus_operations_for_shard`).
    /// Both non-terminal states are included so a `Scheduled` callback whose first delivery
    /// was lost to a crash is re-driven (see [`CompletionCallbackSweepEntry`]). Completion
    /// callbacks live in the run blob, so backends enumerate the shard's runs and filter;
    /// losing the index only delays a retry until the rebuild, never changes the outcome.
    async fn list_runs_with_pending_completion_callbacks_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<CompletionCallbackSweepEntry>>;

    // TODO(storage): add sweep methods for activity tasks, archival eligibility,
    // namespace-scoped pagination, and explicit current-execution conflict
    // policies (reuse, reject, allow-after-close, continue-as-new chains).
}

/// A workflow task that is ready for dispatch to a
/// worker. Produced by [`RunRepository::list_dispatchable_workflow_tasks`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DispatchableWorkflowTask {
    /// Durable storage key for the owning run.
    pub run_key: RunKey,
    /// Task queue this task belongs to.
    pub queue: QueueKey,
    /// Monotonic sequence within the run's workflow
    /// task chain.
    pub logical_seq: tokeira_types::LogicalTaskSeq,
    /// Worker that holds sticky cache affinity, if any.
    pub sticky_preferred: Option<WorkerIdentity>,
    /// Fully resolved normal destination used when the sticky worker is absent.
    pub normal_queue: Option<QueueKey>,
    /// Schedule-to-start deadline of this pending sticky task.
    ///
    /// This derives from `PendingWorkflowTask`, not from the run's affinity.
    pub sticky_deadline: Option<OffsetDateTime>,
    /// Workflow Priority metadata used by runtime delivery policy.
    pub priority: Option<Priority>,
    /// Runtime-assigned delivery order, preserved across durable backlog parking.
    ///
    /// `None` means this is a fresh or recovery-derived publication and the
    /// receiving broker must assign an order.
    pub order: Option<DeliveryOrder>,
}

impl PartialEq for DispatchableWorkflowTask {
    fn eq(&self, other: &Self) -> bool {
        self.run_key == other.run_key
            && self.queue == other.queue
            && self.logical_seq == other.logical_seq
            && self.sticky_preferred == other.sticky_preferred
            && self.normal_queue == other.normal_queue
            && self.sticky_deadline == other.sticky_deadline
            && self.priority == other.priority
        // Delivery order is disposable queue policy, not logical task identity.
    }
}

/// Derive one workflow-task delivery envelope from committed run state.
///
/// The preferred sticky queue and normal fallback are disposable delivery
/// choices only. The pending task's own deadline is surfaced for timeout
/// fencing; no read mutates or expires the run's durable sticky affinity.
pub(crate) fn dispatchable_workflow_task(
    state: &WorkflowState,
) -> Option<DispatchableWorkflowTask> {
    if state.status != ExecutionStatus::Running {
        return None;
    }
    let pending = state.pending_workflow_task.as_ref()?;
    if pending.started_event_id.is_some() {
        return None;
    }

    let normal_queue = QueueKey {
        namespace_id: state.namespace_id,
        task_queue: state.task_queue.clone(),
        task_kind: TaskKind::Workflow,
        deployment: state.deployment.clone(),
        build_id: state.build_id.clone(),
    };
    let real_sticky = state
        .sticky
        .as_ref()
        .filter(|sticky| !sticky.sticky_queue.0.is_empty());
    let (queue, fallback, sticky_preferred, sticky_deadline) = if let Some(sticky) = real_sticky {
        (
            QueueKey {
                task_queue: sticky.sticky_queue.clone(),
                ..normal_queue.clone()
            },
            Some(normal_queue),
            Some(sticky.worker_identity.clone()),
            pending.schedule_to_start_deadline,
        )
    } else {
        (normal_queue, None, None, None)
    };

    Some(DispatchableWorkflowTask {
        run_key: state.run_key,
        queue,
        logical_seq: pending.logical_seq,
        sticky_preferred,
        normal_queue: fallback,
        sticky_deadline,
        priority: state.priority.clone(),
        order: None,
    })
}

/// An activity task that is ready for dispatch to a
/// worker. Produced by [`RunRepository::list_due_dispatchable_activity_tasks`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DispatchableActivityTask {
    /// Durable storage key for the owning run.
    pub run_key: RunKey,
    /// Task queue this activity is assigned to.
    pub queue: QueueKey,
    /// Application-level activity identifier.
    pub activity_id: String,
    /// Serialized input payloads for the activity.
    pub input: Payloads,
    /// History event id that scheduled this activity.
    pub schedule_event_id: i64,
    /// Current retry attempt (starts at 1).
    pub attempt: u32,
    /// Worker Deployment routing revision stamped when this activity was dispatched.
    ///
    /// Activity start compares this dispatch-time value against the live WFT
    /// target revision. Persisting the stamp keeps backlog replay from
    /// reinterpreting an old dispatch under a newer routing config.
    pub dispatch_revision: i64,
    /// Activity stamp captured when this task was dispatched.
    ///
    /// Activity start drops the task when this no longer equals the live
    /// activity stamp, fencing an offer that a later pause/unpause/reset/options
    /// update has superseded.
    pub stamp: u64,
    /// Effective Priority after field-wise workflow/activity inheritance.
    pub priority: Option<Priority>,
    /// Runtime-assigned delivery order, preserved across durable backlog parking.
    pub order: Option<DeliveryOrder>,
}

/// One due activity dispatch row: the deliverable task plus the durable
/// eligibility time the row was observed with.
///
/// `dispatch_at` completes the row-version identity reconciliation needs for
/// conditional stale cleanup; it is not part of the worker-facing task.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DueActivityDispatch {
    /// The deliverable task exactly as the broker would publish it.
    pub task: DispatchableActivityTask,
    /// The row's durable eligibility time at observation.
    pub dispatch_at: OffsetDateTime,
}

impl DueActivityDispatch {
    /// The exact row version this observation corresponds to, for
    /// [`RunRepository::delete_activity_dispatch_if_matches`].
    pub fn identity(&self) -> ActivityDispatchIdentity {
        ActivityDispatchIdentity {
            run_key: self.task.run_key,
            activity_id: self.task.activity_id.clone(),
            schedule_event_id: self.task.schedule_event_id,
            attempt: self.task.attempt,
            stamp: self.task.stamp,
            dispatch_revision: self.task.dispatch_revision,
            dispatch_at: self.dispatch_at,
        }
    }
}

/// The observed version of one activity dispatch row.
///
/// Every field participates in the conditional-delete comparison: a
/// retry/options/routing transition that replaces the row changes at least
/// one of them, so deleting "if matches" can never remove a newer live row.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ActivityDispatchIdentity {
    /// Durable storage key for the owning run.
    pub run_key: RunKey,
    /// Application-level activity identifier.
    pub activity_id: String,
    /// History event id that scheduled this activity.
    pub schedule_event_id: i64,
    /// Attempt the row was written for.
    pub attempt: u32,
    /// Activity stamp the row was written with.
    pub stamp: u64,
    /// Worker Deployment routing revision stamped on the row.
    pub dispatch_revision: i64,
    /// Durable eligibility time stamped on the row.
    pub dispatch_at: OffsetDateTime,
}

impl PartialEq for DispatchableActivityTask {
    fn eq(&self, other: &Self) -> bool {
        self.run_key == other.run_key
            && self.queue == other.queue
            && self.activity_id == other.activity_id
            && self.input == other.input
            && self.schedule_event_id == other.schedule_event_id
            && self.attempt == other.attempt
            && self.dispatch_revision == other.dispatch_revision
            && self.stamp == other.stamp
            && self.priority == other.priority
        // Delivery order is disposable queue policy, not logical task identity.
    }
}

/// Durable payload stored for one backlog task.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum BacklogPayload {
    /// Workflow backlog entry keyed by logical workflow-task sequence.
    Workflow {
        /// Monotonic workflow-task sequence.
        logical_seq: tokeira_types::LogicalTaskSeq,
    },
    /// Activity backlog entry carrying the full dispatch payload.
    Activity {
        /// Application-level activity identifier.
        activity_id: String,
        /// Serialized input payloads for the activity.
        input: Payloads,
        /// History event id that scheduled this activity.
        schedule_event_id: i64,
        /// Current retry attempt.
        attempt: u32,
        /// Worker Deployment routing revision stamped when this activity was dispatched.
        ///
        /// Defaults to zero for backlog rows written before Worker Deployment
        /// routing began stamping activity tasks; zero can never be "ahead" of
        /// a real routing revision, so legacy entries do not spuriously start
        /// workflow deployment transitions.
        #[serde(default)]
        dispatch_revision: i64,
        /// Activity stamp captured when this task was dispatched, carried so a
        /// grace-demoted offer is still fenced at start against a superseding
        /// mutation. Defaults to zero for backlog rows written before stamping.
        #[serde(default)]
        stamp: u64,
    },
}

/// Reconstructible ordering assigned to one dispatch before it enters a broker.
///
/// This value is delivery policy, not workflow authority. Storage preserves and
/// indexes it but never chooses a priority band or advances a fairness frontier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DeliveryOrder {
    /// Effective priority band, where a lower value is served first.
    pub priority_key: i16,
    /// Weighted-fair scheduling pass within the priority band.
    pub fair_pass: i64,
    /// Stable FIFO tie assigned by the queue-home runtime.
    pub insertion_tie: u64,
}

impl Default for DeliveryOrder {
    fn default() -> Self {
        Self {
            priority_key: 3,
            fair_pass: 0,
            insertion_tie: 0,
        }
    }
}

/// A single entry in the durable dispatch backlog.
///
/// Tasks land here when no worker is immediately
/// available. The runtime drains the backlog on the
/// next sweep cycle.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BacklogEntry {
    /// Durable storage key for the owning run.
    pub run_key: RunKey,
    /// Task queue this entry targets.
    pub queue: QueueKey,
    /// Serialized task payload.
    pub payload: BacklogPayload,
    /// Effective task priority retained across broker demotion and rehydration.
    pub priority: Option<Priority>,
    /// Original broker publish time used for backlog age.
    pub scheduled_at: OffsetDateTime,
    /// Runtime-assigned delivery ordering retained by storage without reinterpretation.
    pub order: DeliveryOrder,
}

/// Durable backlog observation for one effective priority band.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BacklogBandStats {
    /// Number of rows in the band.
    pub count: usize,
    /// Oldest original broker schedule time in the band.
    pub oldest_scheduled_at: OffsetDateTime,
}

/// Policy for handling a start-workflow request when
/// a current execution already exists for the same
/// `(namespace, workflow_id)`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum CurrentExecutionConflictPolicy {
    /// Reject the new start if an open execution
    /// already exists (default).
    #[default]
    Reject,
    /// Allow the new start only after the existing
    /// execution has closed.
    AllowAfterClose,
}

/// A timer whose `fire_at` deadline has been reached.
#[derive(Clone, Debug, PartialEq)]
pub struct DueTimer {
    /// Durable storage key for the owning run.
    pub run_key: RunKey,
    /// Application-level timer identifier.
    pub timer_id: String,
}

/// Sweep entry for reconstructing workflow timeout tracking
/// after shard acquisition.
#[derive(Clone, Debug, PartialEq)]
pub struct WorkflowTimeoutSweepEntry {
    /// Durable storage key for the run.
    pub run_key: RunKey,
    /// Maximum wall-clock time for the execution chain.
    pub workflow_execution_timeout: Option<time::Duration>,
    /// Maximum wall-clock time for a single run.
    pub workflow_run_timeout: Option<time::Duration>,
    /// When this run started.
    pub started_at: OffsetDateTime,
    /// First-workflow-task backoff, so the run-timeout anchor
    /// (`started_at + workflow_start_delay`) survives shard recovery.
    pub workflow_start_delay: Option<time::Duration>,
    /// When the first run in the chain started.
    pub first_run_started_at: Option<OffsetDateTime>,
    /// Whether the run has a retry policy configured.
    pub has_retry_policy: bool,
}

/// Sweep entry for reconstructing workflow-task timeout
/// tracking after shard acquisition.
#[derive(Clone, Debug, PartialEq)]
pub struct WftTimeoutSweepEntry {
    /// Durable storage key for the owning run.
    pub run_key: RunKey,
    /// Logical workflow-task sequence currently in progress.
    pub logical_seq: tokeira_types::LogicalTaskSeq,
    /// History event ID that recorded worker pickup.
    pub started_event_id: i64,
    /// Wall-clock time when the workflow task was started.
    pub started_at: time::OffsetDateTime,
    /// Timeout configured for this workflow task attempt.
    pub workflow_task_timeout: time::Duration,
}

/// Sweep entry for reconstructing activity timeout tracking
/// after shard acquisition.
#[derive(Clone, Debug, PartialEq)]
pub struct ActivitySweepEntry {
    /// Durable storage key for the owning run.
    pub run_key: RunKey,
    /// Application-level activity identifier.
    pub activity_id: String,
    /// History event ID of the schedule event.
    pub schedule_event_id: i64,
    /// Current retry attempt (1-based).
    pub attempt: u32,
    /// When the activity was originally scheduled.
    pub original_scheduled_at: OffsetDateTime,
    /// When the CURRENT attempt became (or becomes) dispatchable.
    ///
    /// The schedule-to-start anchor for retries: attempt N's
    /// schedule-to-start clock runs from its own dispatch time, not from the
    /// original schedule (`original_scheduled_at` stays the schedule-to-close
    /// anchor spanning the whole retry chain, retry.go:108-110 @ v1.31.0).
    pub current_attempt_scheduled_at: Option<OffsetDateTime>,
    /// When the activity was started (None if not yet
    /// started).
    pub started_at: Option<OffsetDateTime>,
    /// Maximum time from schedule to completion.
    pub schedule_to_close_timeout: Option<time::Duration>,
    /// Maximum time from schedule to worker pickup.
    pub schedule_to_start_timeout: Option<time::Duration>,
    /// Maximum time from worker pickup to completion.
    pub start_to_close_timeout: Option<time::Duration>,
    /// Maximum time between heartbeats.
    pub heartbeat_timeout: Option<time::Duration>,
}

/// Sweep entry for reconstructing Nexus timeout tracking
/// after shard acquisition.
///
/// Only Nexus operations with at least one timeout configured
/// (schedule-to-close, schedule-to-start, or start-to-close)
/// are included — operations without any timeout do not need
/// timeout tracking reconstruction. The entry is a pure anchor:
/// the scanner reloads the durable `PendingNexusOperation` to
/// read the actual deadlines and the started state, so the
/// timeout values are intentionally not carried here.
#[derive(Clone, Debug, PartialEq)]
pub struct NexusSweepEntry {
    /// Durable storage key for the owning run.
    pub run_key: RunKey,
    /// Nexus operation identifier.
    pub operation_id: String,
    /// History event ID of the scheduled event.
    pub scheduled_event_id: i64,
    /// When the operation was scheduled.
    pub scheduled_at: OffsetDateTime,
}

/// One authoritative pending Nexus delivery available for exact-version backlog
/// reconstruction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReconstructibleNexusDelivery {
    /// Durable owner used for stable diagnostics and deduplication.
    pub run_key: RunKey,
    /// Operation identifier within the run.
    pub operation_id: String,
    /// Endpoint name resolved through runtime's current endpoint registry.
    pub endpoint: String,
    /// Effective Deployment Version from the committed run snapshot.
    pub version: WorkerDeploymentVersionRef,
}

/// Derive advisory Nexus backlog candidates from one committed run snapshot.
///
/// Scheduled work is included even when it may already be in flight: the sampler
/// deliberately prefers a conservative capacity hint after disposable broker loss.
/// Backing-off work appears only once its durable retry deadline is due.
#[must_use]
pub fn reconstructible_nexus_deliveries(
    state: &WorkflowState,
    now: OffsetDateTime,
) -> Vec<ReconstructibleNexusDelivery> {
    if !state.status.is_open() {
        return Vec::new();
    }
    let Some(version) = state.effective_deployment().cloned() else {
        return Vec::new();
    };

    state
        .pending_nexus_operations
        .values()
        .filter(|operation| {
            if !operation.started {
                return operation.next_attempt_at.is_none_or(|due| due <= now);
            }
            operation
                .cancellation
                .as_ref()
                .is_some_and(|cancellation| match cancellation.state {
                    NexusOperationCancellationState::Scheduled => true,
                    NexusOperationCancellationState::BackingOff => {
                        cancellation.next_attempt_at.is_some_and(|due| due <= now)
                    }
                    NexusOperationCancellationState::Unspecified
                    | NexusOperationCancellationState::Succeeded
                    | NexusOperationCancellationState::Failed => false,
                })
        })
        .map(|operation| ReconstructibleNexusDelivery {
            run_key: state.run_key,
            operation_id: operation.operation_id.clone(),
            endpoint: operation.endpoint.clone(),
            version: version.clone(),
        })
        .collect()
}

/// A *pending* (`Scheduled` or `BackingOff`) completion callback that the
/// completion-callback retry scanner must re-watch after a shard takeover. Produced by
/// [`RunRepository::list_runs_with_pending_completion_callbacks_for_shard`]. Like
/// [`NexusSweepEntry`] this is only an index seed: the durable `CompletionCallback`
/// remains the authority for the callback's current state and `next_attempt_at`, re-read
/// at scan time. Both non-terminal states are included so a callback whose first attempt
/// was lost mid-flight (process crash after `Scheduled` committed but before the attempt
/// was recorded) is re-driven on recovery — mirroring v1.31.0's `RegenerateTasks`, which
/// re-issues an invocation task for `CALLBACK_STATE_SCHEDULED` and a backoff task for
/// `CALLBACK_STATE_BACKING_OFF` (`components/callbacks/statemachine.go:76-96 @ v1.31.0`).
#[derive(Clone, Debug, PartialEq)]
pub struct CompletionCallbackSweepEntry {
    /// Durable storage key for the owning run.
    pub run_key: RunKey,
    /// Index into the run's `completion_callbacks`.
    pub callback_index: usize,
}

pub fn workflow_is_open_and_pinned_to_version(
    state: &WorkflowState,
    namespace_id: NamespaceId,
    version: &WorkerDeploymentVersionKey,
) -> bool {
    if !state.status.is_open() || state.namespace_id != namespace_id {
        return false;
    }
    state.effective_behavior() == VersioningBehavior::Pinned
        && state.effective_deployment().is_some_and(|effective| {
            effective.deployment_name == version.deployment_name.0
                && effective.build_id == version.build_id.0
        })
}

/// Read-only interface for projection workers.
///
/// Projection sinks consume a partitioned log of complete post-transition
/// visibility snapshots. The log remains partitioned by run so workers can
/// checkpoint incrementally, but each record is self-contained and does not
/// require replaying prior projection deltas.
#[async_trait]
pub trait ProjectionLog: Send + Sync {
    /// Read projection records from `cursor` forward,
    /// returning at most `limit` records and an
    /// updated cursor for the next call.
    async fn read_from(&self, cursor: &ProjectionCursor, limit: usize) -> Result<ProjectionBatch>;
}

/// One row in the projection log, grouping all
/// projection ops from a single transition.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProjectionContext {
    /// Execution archetype for the shared visibility row.
    ///
    /// Workflow transitions use [`ArchetypeId::WORKFLOW`]. CHASM components use
    /// registry-assigned non-zero ids so the projection plane never relies on a
    /// nullable workflow discriminator.
    #[serde(default = "workflow_archetype_id")]
    pub archetype_id: ArchetypeId,
    /// Namespace owning the workflow execution.
    pub namespace_id: NamespaceId,
    /// Generic business identifier for the archetype.
    ///
    /// This mirrors `workflow_id` for workflow records. Activity and other
    /// CHASM archetypes use their own stable execution id here, allowing the
    /// shared current table to serve heterogeneous list/count APIs.
    #[serde(default)]
    pub business_id: String,
    /// Projection producer authority fence.
    ///
    /// The existing workflow producer starts at zero. A future authority
    /// migration must bump this value so stale producers cannot overwrite rows
    /// from the new owner of the same execution.
    #[serde(default)]
    pub authority_epoch: i64,
    /// Generic status keyword interpreted by the row's archetype.
    #[serde(default)]
    pub status_keyword: String,
    /// Generic open/closed/deleted lifecycle discriminator.
    #[serde(default = "open_lifecycle_state")]
    pub lifecycle_state: VisibilityLifecycleState,
    /// Workflow identifier visible to operators and SDKs.
    pub workflow_id: WorkflowId,
    /// Run identifier visible through the Temporal compatibility surface.
    pub run_id: RunId,
    /// Workflow type used by visibility filters and aggregations.
    pub workflow_type: WorkflowType,
    /// Primary workflow task queue at the time of the transition.
    pub task_queue: TaskQueueName,
    /// Execution status after the transition.
    pub execution_status: ExecutionStatus,
    /// Workflow start timestamp.
    pub start_time: OffsetDateTime,
    /// Last-update timestamp represented by this projection context.
    #[serde(default = "unix_epoch")]
    pub update_time: OffsetDateTime,
    /// Scheduled execution timestamp when distinct from start time.
    pub execution_time: Option<OffsetDateTime>,
    /// Close timestamp for terminal transitions.
    pub close_time: Option<OffsetDateTime>,
    /// Durable history length after the transition.
    pub history_length: i64,
    /// Closed-run duration in nanoseconds, matching Temporal's integer
    /// visibility representation of `ExecutionDuration`.
    #[serde(default)]
    pub execution_duration: Option<i64>,
    /// Number of state transitions after this transition.
    pub state_transition_count: i64,
    /// Generic transition counter for archetype-neutral visibility.
    #[serde(default)]
    pub transition_count: i64,
    /// Approximate serialized history size in bytes.
    ///
    /// Tokeira does not yet maintain Temporal's exact byte accounting, but the
    /// field must exist so visibility queries over `HistorySizeBytes` compile
    /// against the v1.31.0 system search-attribute surface.
    #[serde(default)]
    pub history_size_bytes: i64,
    /// Parent workflow ID for child executions.
    #[serde(default)]
    pub parent_workflow_id: Option<WorkflowId>,
    /// Parent run ID for child executions.
    #[serde(default)]
    pub parent_run_id: Option<RunId>,
    /// Canonical root workflow ID for this execution.
    #[serde(default)]
    pub root_workflow_id: Option<WorkflowId>,
    /// Canonical root run ID for this execution.
    #[serde(default)]
    pub root_run_id: Option<RunId>,
    /// Search-attribute generation pointer for snapshot application.
    #[serde(default)]
    pub search_attr_generation: u64,
    /// Full memo image after the transition.
    ///
    /// The old projection contract emitted incremental memo patches. Snapshots
    /// carry the complete image so applying a later version does not depend on
    /// seeing every earlier delta in order.
    #[serde(default)]
    pub memo: Memo,
    /// Full search-attribute image after the transition.
    ///
    /// The sink writes this image as one generation. That makes projection
    /// retries and out-of-order delivery idempotent because the newest snapshot
    /// replaces the visible generation instead of incrementally folding patches.
    #[serde(default)]
    pub search_attributes: SearchAttributes,
}

fn workflow_archetype_id() -> ArchetypeId {
    ArchetypeId::WORKFLOW
}

fn open_lifecycle_state() -> VisibilityLifecycleState {
    VisibilityLifecycleState::Open
}

fn unix_epoch() -> OffsetDateTime {
    OffsetDateTime::UNIX_EPOCH
}

/// Build a complete workflow visibility image while retaining projection-owned
/// historical worker-deployment observations from the preceding image.
///
/// `TemporalUsedWorkerDeploymentVersions` is a visibility-only accumulator:
/// v1.31.0 appends a version only after a WFT completes and preserves every
/// earlier version (`addUsedDeploymentVersionToLoadedSearchAttribute`,
/// `service/history/workflow/mutable_state_impl.go @ v1.31.0`). Tokeira keeps
/// that read-model concern outside the kernel. Storage commits merge the prior
/// atomically-written projection image so every emitted record remains a full
/// post-transition snapshot and projection replay stays self-contained.
pub(crate) fn workflow_projection_context_with_previous(
    state: &WorkflowState,
    previous: Option<&ProjectionContext>,
) -> Result<ProjectionContext> {
    let mut context = projection_context(
        state,
        if state.status.is_open() {
            VisibilityLifecycleState::Open
        } else {
            VisibilityLifecycleState::Closed
        },
        state.closed_at.unwrap_or(state.started_at),
        false,
    )?;

    let mut used_versions = previous
        .and_then(|previous| {
            previous
                .search_attributes
                .0
                .get("TemporalUsedWorkerDeploymentVersions")
        })
        .and_then(|value| match value {
            SearchAttrValue::KeywordList(values) => Some(values.clone()),
            _ => None,
        })
        .unwrap_or_default();
    if let Some(SearchAttrValue::KeywordList(current)) = context
        .search_attributes
        .0
        .get("TemporalUsedWorkerDeploymentVersions")
    {
        for version in current {
            if !used_versions.contains(version) {
                used_versions.push(version.clone());
            }
        }
    }
    if !used_versions.is_empty() {
        context.search_attributes.0.insert(
            "TemporalUsedWorkerDeploymentVersions".to_owned(),
            SearchAttrValue::KeywordList(used_versions),
        );
    }

    Ok(context)
}

/// Build the non-queryable high-water visibility image for a deleted run.
///
/// Identity and ordering fields survive so an older delayed projection cannot
/// recreate the row. User memo and search attributes are deliberately erased.
pub(crate) fn deleted_workflow_projection_context(
    state: &WorkflowState,
    deleted_at: OffsetDateTime,
) -> Result<ProjectionContext> {
    projection_context(state, VisibilityLifecycleState::Deleted, deleted_at, true)
}

fn projection_context(
    state: &WorkflowState,
    lifecycle_state: VisibilityLifecycleState,
    update_time: OffsetDateTime,
    redact_user_data: bool,
) -> Result<ProjectionContext> {
    let transition_count = i64::try_from(state.transition_seq.0).map_err(|_| {
        anyhow!(
            "workflow transition sequence {} exceeds visibility i64 range",
            state.transition_seq.0
        )
    })?;
    let execution_duration = state
        .closed_at
        .map(|closed_at| (closed_at - state.started_at).whole_nanoseconds() as i64);
    let search_attributes = if redact_user_data {
        SearchAttributes::default()
    } else {
        let mut search_attributes = state.search_attributes.clone();
        if let Some(info) = state.versioning_info.as_ref() {
            let mut build_ids = info.build_id_search_attributes.clone();
            build_ids.retain(|value| !value.starts_with("pinned:"));
            if state.effective_behavior() == VersioningBehavior::Pinned
                && let Some(version) = state.effective_deployment()
                && !version.deployment_name.is_empty()
                && !version.build_id.is_empty()
            {
                // v1.31.0 replaces any prior pinned reachability tag with the
                // effective pinned version and puts it first
                // (`addBuildIdToLoadedSearchAttribute`,
                // mutable_state_impl.go @ v1.31.0). This is visibility state,
                // so deriving it here avoids mutating authoritative history.
                build_ids.insert(
                    0,
                    format!("pinned:{}:{}", version.deployment_name, version.build_id),
                );
            }
            if !build_ids.is_empty() {
                // BuildIds is server-managed visibility state, not a user SA and
                // not part of continue-as-new inheritance. Project it from the
                // history-derived per-run summary (`updateBuildIdsAndDeploymentSearchAttributes`,
                // mutable_state_impl.go @ v1.31.0).
                search_attributes.0.insert(
                    "BuildIds".to_owned(),
                    SearchAttrValue::KeywordList(build_ids),
                );
            }
        }
        if let Some(info) = state.versioning_info.as_ref() {
            // These are mutable-state-derived visibility attributes, not client-authored
            // WorkflowExecutionStarted attributes. Deriving them in the complete projection
            // image mirrors `addBuildIDAndDeploymentInfoToSearchAttributesWithNoVisibilityTask`
            // without leaking server-managed values into history
            // (`service/history/workflow/mutable_state_impl.go:2870-2990,3767-3835 @ v1.31.0`).
            let (deployment, version, behavior) = match info.versioning_override.as_ref() {
                Some(VersioningOverride::Pinned { version }) => (
                    Some(version.deployment_name.as_str()),
                    Some(version),
                    Some("Pinned"),
                ),
                Some(VersioningOverride::AutoUpgrade) => (
                    info.deployment_version
                        .as_ref()
                        .map(|version| version.deployment_name.as_str())
                        .or(state.worker_deployment_name.as_deref()),
                    info.deployment_version.as_ref(),
                    Some("AutoUpgrade"),
                ),
                None => (
                    info.deployment_version
                        .as_ref()
                        .map(|version| version.deployment_name.as_str())
                        .or(state.worker_deployment_name.as_deref()),
                    info.deployment_version.as_ref(),
                    match info.behavior {
                        VersioningBehavior::Pinned => Some("Pinned"),
                        VersioningBehavior::AutoUpgrade => Some("AutoUpgrade"),
                        VersioningBehavior::Unspecified => None,
                    },
                ),
            };
            if let Some(deployment) = deployment.filter(|value| !value.is_empty()) {
                search_attributes.0.insert(
                    "TemporalWorkerDeployment".to_owned(),
                    SearchAttrValue::Keyword(deployment.to_owned()),
                );
            }
            if let Some(version) = version.filter(|version| {
                !version.deployment_name.is_empty() && !version.build_id.is_empty()
            }) {
                search_attributes.0.insert(
                    "TemporalWorkerDeploymentVersion".to_owned(),
                    SearchAttrValue::Keyword(format!(
                        "{}:{}",
                        version.deployment_name, version.build_id
                    )),
                );
            }
            if let Some(behavior) = behavior {
                search_attributes.0.insert(
                    "TemporalWorkflowVersioningBehavior".to_owned(),
                    SearchAttrValue::Keyword(behavior.to_owned()),
                );
            }
            if let Some(version) = info.deployment_version.as_ref().filter(|version| {
                !version.deployment_name.is_empty() && !version.build_id.is_empty()
            }) {
                // Only a successfully completed WFT populates
                // `deployment_version`; a start-time pinned override therefore
                // cannot appear in the used-version index prematurely. Storage
                // folds this observation into the preceding projection image
                // above, matching v1.31.0's completion-side update.
                search_attributes.0.insert(
                    "TemporalUsedWorkerDeploymentVersions".to_owned(),
                    SearchAttrValue::KeywordList(vec![format!(
                        "{}:{}",
                        version.deployment_name, version.build_id
                    )]),
                );
            }
        }
        let mut pause_entries = Vec::new();
        if let Some(pause) = state.pause_info.as_ref() {
            pause_entries.push(format!("Workflow:{}", state.workflow_id.0));
            if !pause.reason.is_empty() {
                pause_entries.push(format!("Reason:{}", pause.reason));
            }
        }
        let paused_activity_types = state
            .activities
            .values()
            .filter(|activity| activity.pause_info.is_some())
            .map(|activity| activity.activity_type.as_str())
            .collect::<BTreeSet<_>>();
        pause_entries.extend(
            paused_activity_types
                .into_iter()
                .map(|activity_type| format!("property:activityType={activity_type}")),
        );
        if !pause_entries.is_empty() {
            // Temporal regenerates this server-managed KeywordList from the
            // current workflow/activity pause state on every mutation; batch
            // activity operations discover targets through the same visibility
            // attribute (`buildTemporalPauseInfoEntries`,
            // mutable_state_impl.go:6431-6475 @ v1.31.0).
            search_attributes.0.insert(
                "TemporalPauseInfo".to_owned(),
                SearchAttrValue::KeywordList(pause_entries),
            );
        } else {
            search_attributes.0.remove("TemporalPauseInfo");
        }
        if state.external_payload_count > 0 {
            search_attributes.0.insert(
                "TemporalExternalPayloadCount".to_owned(),
                SearchAttrValue::Int(state.external_payload_count),
            );
            search_attributes.0.insert(
                "TemporalExternalPayloadSizeBytes".to_owned(),
                SearchAttrValue::Int(state.external_payload_size_bytes),
            );
        }
        search_attributes
    };

    Ok(ProjectionContext {
        archetype_id: ArchetypeId::WORKFLOW,
        namespace_id: state.namespace_id,
        business_id: state.workflow_id.0.clone(),
        // The workflow producer does not yet carry a namespace failover
        // version. Transition sequence therefore remains its monotonic fence.
        authority_epoch: 0,
        status_keyword: format!("{:?}", state.status),
        lifecycle_state,
        workflow_id: state.workflow_id.clone(),
        run_id: state.run_id,
        workflow_type: state.workflow_type.clone(),
        task_queue: state.task_queue.clone(),
        execution_status: state.status,
        start_time: state.started_at,
        update_time,
        // v1.31.0 derives ExecutionTime from start plus first-WFT backoff
        // (`mutable_state_impl.go:2859 @ v1.31.0`).
        execution_time: Some(state.started_at + state.workflow_start_delay.unwrap_or_default()),
        close_time: state.closed_at,
        history_length: state.last_event_id,
        execution_duration,
        state_transition_count: transition_count,
        transition_count,
        history_size_bytes: 0,
        parent_workflow_id: state.parent_workflow_id.clone(),
        parent_run_id: state.parent_run_id,
        root_workflow_id: state
            .root_workflow_id
            .clone()
            .or_else(|| Some(state.workflow_id.clone())),
        root_run_id: state.root_run_id.or(Some(state.run_id)),
        search_attr_generation: state.transition_seq.0,
        memo: if redact_user_data {
            Memo::default()
        } else {
            state.memo.clone()
        },
        search_attributes,
    })
}

/// One row in the projection log, carrying the complete visibility image for a transition.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProjectionRecord {
    /// Hash-based partition for fan-out distribution.
    pub partition_id: u32,
    /// Fanout factor (typically 1 for the dev store).
    pub fanout: u16,
    /// Durable storage key for the source run.
    pub run_key: RunKey,
    /// Transition that produced this snapshot.
    pub transition_seq: tokeira_types::TransitionSeq,
    /// Execution metadata snapshot for visibility sinks.
    ///
    /// Projection consumers intentionally receive a full post-transition image
    /// rather than kernel delta operations. That makes the visibility plane
    /// idempotent under retry and out-of-order delivery: newer versions replace
    /// older images instead of replaying a partial patch stream.
    pub context: ProjectionContext,
}

/// A page of projection records returned by
/// [`ProjectionLog::read_from`].
#[derive(Clone, Debug, PartialEq)]
pub struct ProjectionBatch {
    /// The projection records in this page.
    pub records: Vec<ProjectionRecord>,
    /// Cursor to pass into the next `read_from` call.
    pub next_cursor: ProjectionCursor,
}

/// Lease repository for shard or bundle ownership.
///
/// Leases are epoch-fenced: a successful acquire or
/// renew returns the current epoch, and a stale epoch
/// causes rejection. See the
/// [storage docs](../../docs/crates/storage.md) for
/// the fenced-lease model.
#[async_trait]
pub trait LeaseRepository: Send + Sync {
    /// Attempt to acquire ownership of `bundle`.
    ///
    /// Returns [`LeaseOutcome::Acquired`] on success,
    /// or [`LeaseOutcome::Rejected`] if another owner
    /// already holds the lease.
    async fn try_acquire_bundle(
        &self,
        bundle: ShardId,
        owner: String,
        node_endpoint: String,
    ) -> Result<LeaseOutcome>;
    /// Renew an existing lease for `bundle` at the
    /// given `epoch`. Fails if the epoch is stale.
    async fn renew_bundle(
        &self,
        bundle: ShardId,
        owner: String,
        epoch: ShardEpoch,
        node_endpoint: String,
    ) -> Result<LeaseOutcome>;

    /// List all bundle leases known to the repository.
    async fn list_bundle_leases(&self) -> Result<Vec<BundleLease>>;

    /// Release ownership with epoch-checked compare-and-swap semantics.
    async fn relinquish_bundle(
        &self,
        bundle: ShardId,
        owner: String,
        epoch: ShardEpoch,
    ) -> Result<LeaseOutcome>;
}

/// Current persisted bundle lease row.
#[derive(Clone, Debug, PartialEq)]
pub struct BundleLease {
    pub bundle_id: ShardId,
    pub owner_node_id: Option<String>,
    pub epoch: ShardEpoch,
    pub lease_until: OffsetDateTime,
    pub node_endpoint: Option<String>,
}

/// Repository for controller coordination state.
#[async_trait]
pub trait ControlRepository: Send + Sync {
    /// Advance the singleton routing generation row with CAS protection.
    async fn advance_generation(
        &self,
        expected: GenerationCounter,
    ) -> Result<GenerationAdvanceResult>;

    /// Read the current routing generation.
    async fn current_generation(&self) -> Result<GenerationCounter>;

    /// Allocate connection budget with CAS protection.
    async fn allocate_budget(
        &self,
        expected_version: u64,
        allocator_id: Uuid,
        rate_budget: f64,
        capacity_budget: u64,
    ) -> Result<BudgetAllocationResult>;

    /// Read the current budget allocation version.
    async fn current_budget_version(&self) -> Result<u64>;
}

/// Result of a generation-counter CAS attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GenerationAdvanceResult {
    Advanced(GenerationCounter),
    Conflict(GenerationCounter),
}

/// Result of a budget-allocation CAS attempt.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum BudgetAllocationResult {
    Allocated { version: u64 },
    Conflict { current_version: u64 },
}

/// Outcome of a lease acquire or renew attempt.
#[derive(Clone, Debug, PartialEq)]
pub enum LeaseOutcome {
    /// Lease successfully acquired at this epoch.
    Acquired { epoch: ShardEpoch },
    /// Lease successfully renewed at this epoch.
    Renewed { epoch: ShardEpoch },
    /// Lease is held by another owner; includes the
    /// current owner and epoch for diagnostics.
    Rejected {
        current_owner: String,
        current_epoch: ShardEpoch,
    },
}

/// Database work classes used by the future connection
/// director.
///
/// Priority order (highest first): `Control` >
/// `Commit` > `Read` > `Projection` > `Maintenance`.
/// See [060-connection-management](../../docs/architecture/060-connection-management.md).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DbClass {
    /// Shard lease and cluster-control operations.
    Control,
    /// State-transition commits (OCC fenced writes).
    Commit,
    /// Read-path queries (load run, read history).
    Read,
    /// Projection sink writes.
    Projection,
    /// Background housekeeping (archival, cleanup).
    Maintenance,
}

/// Small abstraction for future DSQL connection-budget
/// control.
///
/// In the dev store this is a no-op. A production
/// implementation will enforce per-class connection
/// limits and open-rate budgets.
#[async_trait]
pub trait ConnectionDirector: Send + Sync {
    /// Permit type returned by this director.
    type Permit: Send;

    /// Acquire a connection permit for the given work
    /// class. Blocks until a permit is available.
    async fn acquire(&self, class: DbClass) -> Result<Self::Permit>;
}

/// A held connection permit, scoped to a [`DbClass`].
///
/// TODO(storage): replace this trivial type with a
/// real session/lease wrapper in the DSQL-backed
/// implementation.
#[derive(Debug)]
pub struct DbPermit {
    /// The work class this permit was issued for.
    pub class: DbClass,
}

#[async_trait]
impl<T> RunRepository for std::sync::Arc<T>
where
    T: RunRepository + ?Sized,
{
    async fn resolve_execution(&self, execution: &ExecutionRef) -> Result<Option<RunKey>> {
        (**self).resolve_execution(execution).await
    }

    async fn find_latest_run(
        &self,
        namespace_id: NamespaceId,
        workflow_id: &WorkflowId,
    ) -> Result<Option<RunKey>> {
        (**self).find_latest_run(namespace_id, workflow_id).await
    }

    async fn list_runs_for_namespace(&self, namespace_id: NamespaceId) -> Result<Vec<RunKey>> {
        (**self).list_runs_for_namespace(namespace_id).await
    }

    async fn load_run(&self, run_key: RunKey) -> Result<LoadedRun> {
        (**self).load_run(run_key).await
    }

    async fn read_history(
        &self,
        run_key: RunKey,
        after_event_id: i64,
        limit: usize,
    ) -> Result<Vec<HistoryEvent>> {
        (**self).read_history(run_key, after_event_id, limit).await
    }

    async fn read_attributed_history(
        &self,
        run_key: RunKey,
        after_event_id: i64,
        limit: usize,
    ) -> Result<Vec<AttributedHistoryEvent>> {
        (**self)
            .read_attributed_history(run_key, after_event_id, limit)
            .await
    }

    async fn lookup_request_dedupe(
        &self,
        execution: &ExecutionRef,
        request_id: &RequestId,
    ) -> Result<Option<RequestRecord>> {
        (**self).lookup_request_dedupe(execution, request_id).await
    }

    async fn read_transition_audit(&self, run_key: RunKey) -> Result<Vec<TransitionAuditRecord>> {
        (**self).read_transition_audit(run_key).await
    }

    async fn has_open_pinned_workflows(
        &self,
        namespace_id: NamespaceId,
        version: &WorkerDeploymentVersionKey,
    ) -> Result<bool> {
        (**self)
            .has_open_pinned_workflows(namespace_id, version)
            .await
    }

    async fn create_workflow_rule(
        &self,
        namespace_id: NamespaceId,
        rule: WorkflowRuleRecord,
        max_rules: usize,
    ) -> Result<WorkflowRuleCreateResult> {
        (**self)
            .create_workflow_rule(namespace_id, rule, max_rules)
            .await
    }

    async fn get_workflow_rule(
        &self,
        namespace_id: NamespaceId,
        rule_id: &str,
    ) -> Result<Option<WorkflowRuleRecord>> {
        (**self).get_workflow_rule(namespace_id, rule_id).await
    }

    async fn delete_workflow_rule(
        &self,
        namespace_id: NamespaceId,
        rule_id: &str,
    ) -> Result<WorkflowRuleDeleteResult> {
        (**self).delete_workflow_rule(namespace_id, rule_id).await
    }

    async fn list_workflow_rules(
        &self,
        namespace_id: NamespaceId,
    ) -> Result<Vec<WorkflowRuleRecord>> {
        (**self).list_workflow_rules(namespace_id).await
    }

    async fn commit_transition(
        &self,
        run_key: RunKey,
        transition: Transition,
        epoch: ShardEpoch,
    ) -> Result<CommitResult> {
        (**self).commit_transition(run_key, transition, epoch).await
    }

    async fn commit_transition_for_bundle(
        &self,
        run_key: RunKey,
        execution_home_bundle: ShardId,
        transition: Transition,
        epoch: ShardEpoch,
    ) -> Result<CommitResult> {
        (**self)
            .commit_transition_for_bundle(run_key, execution_home_bundle, transition, epoch)
            .await
    }

    async fn delete_run_for_bundle(
        &self,
        run_key: RunKey,
        execution_home_bundle: ShardId,
        request: DeleteRunRequest,
        epoch: ShardEpoch,
    ) -> Result<DeleteRunResult> {
        (**self)
            .delete_run_for_bundle(run_key, execution_home_bundle, request, epoch)
            .await
    }

    async fn materialize_reset_successor(
        &self,
        base_run_key: RunKey,
        fork_event_id: i64,
        successor_run_id: RunId,
    ) -> Result<()> {
        (**self)
            .materialize_reset_successor(base_run_key, fork_event_id, successor_run_id)
            .await
    }

    async fn list_dispatchable_workflow_tasks(
        &self,
        queue: &QueueKey,
        limit: usize,
    ) -> Result<Vec<DispatchableWorkflowTask>> {
        (**self)
            .list_dispatchable_workflow_tasks(queue, limit)
            .await
    }

    async fn list_due_dispatchable_activity_tasks(
        &self,
        queue: &QueueKey,
        now: OffsetDateTime,
        limit: usize,
    ) -> Result<Vec<DispatchableActivityTask>> {
        (**self)
            .list_due_dispatchable_activity_tasks(queue, now, limit)
            .await
    }

    async fn list_all_dispatchable_activity_tasks(
        &self,
        queue: &QueueKey,
        limit: usize,
    ) -> Result<Vec<DispatchableActivityTask>> {
        (**self)
            .list_all_dispatchable_activity_tasks(queue, limit)
            .await
    }

    async fn delete_activity_dispatch_if_matches(
        &self,
        candidate: &ActivityDispatchIdentity,
    ) -> Result<bool> {
        (**self)
            .delete_activity_dispatch_if_matches(candidate)
            .await
    }

    async fn persist_to_backlog(&self, entries: Vec<BacklogEntry>) -> Result<()> {
        (**self).persist_to_backlog(entries).await
    }

    async fn drain_backlog(&self, queue: &QueueKey, limit: usize) -> Result<Vec<BacklogEntry>> {
        (**self).drain_backlog(queue, limit).await
    }

    async fn backlog_stats_by_priority(
        &self,
        queue: &QueueKey,
    ) -> Result<BTreeMap<i16, BacklogBandStats>> {
        (**self).backlog_stats_by_priority(queue).await
    }

    async fn list_versioned_backlog_queue_keys(&self) -> Result<Vec<QueueKey>> {
        (**self).list_versioned_backlog_queue_keys().await
    }

    async fn list_due_timers(&self, now: OffsetDateTime, limit: usize) -> Result<Vec<DueTimer>> {
        (**self).list_due_timers(now, limit).await
    }

    async fn list_dispatchable_workflow_tasks_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<DispatchableWorkflowTask>> {
        (**self)
            .list_dispatchable_workflow_tasks_for_shard(shard_id, limit)
            .await
    }

    async fn list_due_dispatchable_activity_tasks_for_shard(
        &self,
        shard_id: ShardId,
        now: OffsetDateTime,
        limit: usize,
    ) -> Result<Vec<DueActivityDispatch>> {
        (**self)
            .list_due_dispatchable_activity_tasks_for_shard(shard_id, now, limit)
            .await
    }

    async fn list_due_timers_for_shard(
        &self,
        shard_id: ShardId,
        now: OffsetDateTime,
        limit: usize,
    ) -> Result<Vec<DueTimer>> {
        (**self)
            .list_due_timers_for_shard(shard_id, now, limit)
            .await
    }

    async fn list_runs_with_workflow_timeouts_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<WorkflowTimeoutSweepEntry>> {
        (**self)
            .list_runs_with_workflow_timeouts_for_shard(shard_id, limit)
            .await
    }

    async fn list_started_workflow_tasks_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<WftTimeoutSweepEntry>> {
        (**self)
            .list_started_workflow_tasks_for_shard(shard_id, limit)
            .await
    }

    async fn list_open_activities_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<ActivitySweepEntry>> {
        (**self)
            .list_open_activities_for_shard(shard_id, limit)
            .await
    }

    async fn list_pending_nexus_operations_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<NexusSweepEntry>> {
        (**self)
            .list_pending_nexus_operations_for_shard(shard_id, limit)
            .await
    }

    async fn list_reconstructible_nexus_deliveries_for_shard(
        &self,
        shard_id: ShardId,
        now: OffsetDateTime,
        limit: usize,
    ) -> Result<Vec<ReconstructibleNexusDelivery>> {
        (**self)
            .list_reconstructible_nexus_deliveries_for_shard(shard_id, now, limit)
            .await
    }

    async fn list_runs_with_pending_completion_callbacks_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<CompletionCallbackSweepEntry>> {
        (**self)
            .list_runs_with_pending_completion_callbacks_for_shard(shard_id, limit)
            .await
    }
}

#[async_trait]
impl<T> WorkerDeploymentRepository for std::sync::Arc<T>
where
    T: WorkerDeploymentRepository + ?Sized,
{
    async fn load_deployment(&self, key: &DeploymentKey) -> Result<Option<StoredWorkerDeployment>> {
        (**self).load_deployment(key).await
    }

    async fn put_deployment(
        &self,
        record: StoredWorkerDeployment,
        expected: Option<ConflictToken>,
    ) -> Result<DeploymentCasResult> {
        (**self).put_deployment(record, expected).await
    }

    async fn delete_deployment(
        &self,
        key: &DeploymentKey,
        expected: ConflictToken,
    ) -> Result<DeploymentCasResult> {
        (**self).delete_deployment(key, expected).await
    }

    async fn list_deployments(
        &self,
        namespace_id: NamespaceId,
        after: Option<&DeploymentName>,
        limit: usize,
    ) -> Result<Vec<StoredWorkerDeployment>> {
        (**self).list_deployments(namespace_id, after, limit).await
    }

    async fn list_all_for_namespace(
        &self,
        namespace_id: NamespaceId,
    ) -> Result<Vec<StoredWorkerDeployment>> {
        (**self).list_all_for_namespace(namespace_id).await
    }
}

#[async_trait]
impl<T> ProjectionLog for std::sync::Arc<T>
where
    T: ProjectionLog + ?Sized,
{
    async fn read_from(&self, cursor: &ProjectionCursor, limit: usize) -> Result<ProjectionBatch> {
        (**self).read_from(cursor, limit).await
    }
}

#[async_trait]
impl<T> LeaseRepository for std::sync::Arc<T>
where
    T: LeaseRepository + ?Sized,
{
    async fn try_acquire_bundle(
        &self,
        bundle: ShardId,
        owner: String,
        node_endpoint: String,
    ) -> Result<LeaseOutcome> {
        (**self)
            .try_acquire_bundle(bundle, owner, node_endpoint)
            .await
    }

    async fn renew_bundle(
        &self,
        bundle: ShardId,
        owner: String,
        epoch: ShardEpoch,
        node_endpoint: String,
    ) -> Result<LeaseOutcome> {
        (**self)
            .renew_bundle(bundle, owner, epoch, node_endpoint)
            .await
    }

    async fn list_bundle_leases(&self) -> Result<Vec<BundleLease>> {
        (**self).list_bundle_leases().await
    }

    async fn relinquish_bundle(
        &self,
        bundle: ShardId,
        owner: String,
        epoch: ShardEpoch,
    ) -> Result<LeaseOutcome> {
        (**self).relinquish_bundle(bundle, owner, epoch).await
    }
}

#[async_trait]
impl<T> ControlRepository for std::sync::Arc<T>
where
    T: ControlRepository + ?Sized,
{
    async fn advance_generation(
        &self,
        expected: GenerationCounter,
    ) -> Result<GenerationAdvanceResult> {
        (**self).advance_generation(expected).await
    }

    async fn current_generation(&self) -> Result<GenerationCounter> {
        (**self).current_generation().await
    }

    async fn allocate_budget(
        &self,
        expected_version: u64,
        allocator_id: Uuid,
        rate_budget: f64,
        capacity_budget: u64,
    ) -> Result<BudgetAllocationResult> {
        (**self)
            .allocate_budget(expected_version, allocator_id, rate_budget, capacity_budget)
            .await
    }

    async fn current_budget_version(&self) -> Result<u64> {
        (**self).current_budget_version().await
    }
}

#[async_trait]
impl<T> ConnectionDirector for std::sync::Arc<T>
where
    T: ConnectionDirector + ?Sized,
{
    type Permit = T::Permit;

    async fn acquire(&self, class: DbClass) -> Result<Self::Permit> {
        (**self).acquire(class).await
    }
}
