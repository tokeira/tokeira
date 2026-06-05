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

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tokeira_kernel::{
    ActivityOp, DispatchOp, HistoryEvent, LoadedRun, ProjectionOp, TimerOp, Transition,
    WorkflowState, state::VersioningBehavior,
};
use tokeira_types::{
    ExecutionRef, ExecutionStatus, GenerationCounter, NamespaceId, Payload, Payloads,
    ProjectionCursor, QueueKey, RequestId, RunId, RunKey, ShardEpoch, ShardId, TaskQueueName,
    TransitionSeq, WorkerIdentity, WorkflowId, WorkflowType,
};
use uuid::Uuid;

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
    /// A request with the same dedupe key was already
    /// committed. The caller can short-circuit.
    Duplicate,
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
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    /// Last current-version change time.
    pub current_version_changed_time: Option<OffsetDateTime>,
    /// Last ramping-version change time.
    pub ramping_version_changed_time: Option<OffsetDateTime>,
    /// Last ramping percentage change time.
    pub ramping_version_percentage_changed_time: Option<OffsetDateTime>,
    /// Monotonic routing revision.
    pub revision_number: i64,
}

impl Default for StoredRoutingConfig {
    fn default() -> Self {
        Self {
            current_version: None,
            ramping_version: None,
            ramping_version_percentage: 0.0,
            current_version_changed_time: None,
            ramping_version_changed_time: None,
            ramping_version_percentage_changed_time: None,
            revision_number: 0,
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
#[derive(Clone, Debug, PartialEq, Eq)]
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
#[derive(Clone, Debug, PartialEq)]
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
    /// Projection log entries for visibility sinks.
    pub projection_ops: Vec<ProjectionOp>,
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

    /// Return activity tasks awaiting dispatch for the
    /// given queue, up to `limit`.
    async fn list_dispatchable_activity_tasks(
        &self,
        queue: &QueueKey,
        limit: usize,
    ) -> Result<Vec<DispatchableActivityTask>>;

    /// Durably persist unmatched tasks to the dispatch
    /// backlog for later retry.
    async fn persist_to_backlog(&self, entries: Vec<BacklogEntry>) -> Result<()>;

    /// Remove and return up to `limit` backlog entries
    /// for the given queue (FIFO order).
    async fn drain_backlog(&self, queue: &QueueKey, limit: usize) -> Result<Vec<BacklogEntry>>;

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

    /// List dispatchable activity tasks for a specific
    /// shard.
    async fn list_dispatchable_activity_tasks_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<DispatchableActivityTask>>;

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

    // TODO(storage): add sweep methods for activity tasks, archival eligibility,
    // namespace-scoped pagination, and explicit current-execution conflict
    // policies (reuse, reject, allow-after-close, continue-as-new chains).
}

/// A workflow task that is ready for dispatch to a
/// worker. Produced by [`RunRepository::list_dispatchable_workflow_tasks`].
#[derive(Clone, Debug, PartialEq)]
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
    /// When the sticky affinity expires.
    pub sticky_expires_at: Option<OffsetDateTime>,
}

/// An activity task that is ready for dispatch to a
/// worker. Produced by [`RunRepository::list_dispatchable_activity_tasks`].
#[derive(Clone, Debug, PartialEq)]
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
    },
}

/// A single entry in the durable dispatch backlog.
///
/// Tasks land here when no worker is immediately
/// available. The runtime drains the backlog on the
/// next sweep cycle.
#[derive(Clone, Debug, PartialEq)]
pub struct BacklogEntry {
    /// Durable storage key for the owning run.
    pub run_key: RunKey,
    /// Task queue this entry targets.
    pub queue: QueueKey,
    /// Serialized task payload.
    pub payload: BacklogPayload,
    /// Original broker publish time used for backlog age.
    pub scheduled_at: OffsetDateTime,
    /// Monotonic insertion order within the backlog.
    pub insertion_seq: u64,
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
/// Only Nexus operations that have a `schedule_to_close_timeout`
/// configured are included — operations without a timeout do
/// not need timeout tracking reconstruction. This is why
/// `schedule_to_close_timeout` is non-optional here even though
/// `PendingNexusOperation.schedule_to_close_timeout` is
/// `Option<Duration>`.
#[derive(Clone, Debug, PartialEq)]
pub struct NexusSweepEntry {
    /// Durable storage key for the owning run.
    pub run_key: RunKey,
    /// Nexus operation identifier.
    pub operation_id: String,
    /// History event ID of the scheduled event.
    pub scheduled_event_id: i64,
    /// Maximum time from schedule to completion.
    pub schedule_to_close_timeout: time::Duration,
    /// When the operation was scheduled.
    pub scheduled_at: OffsetDateTime,
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
/// Projection sinks consume a partitioned log of
/// [`ProjectionOp`](tokeira_kernel::ProjectionOp)s
/// to maintain visibility and search-attribute tables.
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
    /// Namespace owning the workflow execution.
    pub namespace_id: NamespaceId,
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
    /// Scheduled execution timestamp when distinct from start time.
    pub execution_time: Option<OffsetDateTime>,
    /// Close timestamp for terminal transitions.
    pub close_time: Option<OffsetDateTime>,
    /// Durable history length after the transition.
    pub history_length: i64,
    /// Number of state transitions after this transition.
    pub state_transition_count: i64,
}

/// One row in the projection log, grouping all
/// projection ops from a single transition.
#[derive(Clone, Debug, PartialEq)]
pub struct ProjectionRecord {
    /// Hash-based partition for fan-out distribution.
    pub partition_id: u32,
    /// Fanout factor (typically 1 for the dev store).
    pub fanout: u16,
    /// Durable storage key for the source run.
    pub run_key: RunKey,
    /// Transition that produced these ops.
    pub transition_seq: tokeira_types::TransitionSeq,
    /// Execution metadata snapshot for visibility sinks.
    pub context: ProjectionContext,
    /// The projection operations to apply.
    pub ops: Vec<tokeira_kernel::ProjectionOp>,
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

    async fn list_dispatchable_activity_tasks(
        &self,
        queue: &QueueKey,
        limit: usize,
    ) -> Result<Vec<DispatchableActivityTask>> {
        (**self)
            .list_dispatchable_activity_tasks(queue, limit)
            .await
    }

    async fn persist_to_backlog(&self, entries: Vec<BacklogEntry>) -> Result<()> {
        (**self).persist_to_backlog(entries).await
    }

    async fn drain_backlog(&self, queue: &QueueKey, limit: usize) -> Result<Vec<BacklogEntry>> {
        (**self).drain_backlog(queue, limit).await
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

    async fn list_dispatchable_activity_tasks_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<DispatchableActivityTask>> {
        (**self)
            .list_dispatchable_activity_tasks_for_shard(shard_id, limit)
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
