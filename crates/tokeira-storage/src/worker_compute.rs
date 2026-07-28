//! Durable Worker Compute Controller records and repository contract.
//!
//! This state governs advisory capacity policy, never workflow correctness. The
//! runtime owns reconciliation and provider I/O; storage owns namespace capacity,
//! short controller claims, atomic scaler-state/outbox commits, queue samples, and
//! action delivery fences. The kernel therefore remains unaware of this module.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tokeira_types::{
    ConfigurationFingerprint, ControllerInstanceKey, IncarnationId, NamespaceId, ScalingGroupId,
    WorkerComputeControllerLifecycle, WorkerComputeFailureCategory, WorkerComputeGroupEligibility,
    WorkerComputeHealth, WorkerComputeInvokeReason, WorkerComputeProviderActionStatus,
    WorkerComputeQueueKey, WorkerComputeTaskType,
};
use tokio::sync::Mutex;
use uuid::Uuid;

/// Current postcard document version for controller and action records.
pub const WORKER_COMPUTE_RECORD_FORMAT_VERSION: u16 = 1;
/// Hard safety bound represented by namespace slot rows `0..=99`.
pub const WORKER_COMPUTE_NAMESPACE_SLOT_LIMIT: usize = 100;
/// Number of UUID-derived action due buckets.
pub const WORKER_COMPUTE_ACTION_BUCKETS: u8 = 64;
/// Maximum actions claimed in one bounded repository transaction.
pub const WORKER_COMPUTE_ACTION_CLAIM_LIMIT: usize = 100;

/// Durable state for one effective ComputeConfig scaling group.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkerComputeScalingGroupState {
    /// Canonical fence over every behavior-affecting configuration field.
    pub fingerprint: ConfigurationFingerprint,
    /// Task families assigned after explicit/catch-all partitioning.
    pub effective_task_types: BTreeSet<WorkerComputeTaskType>,
    /// Whether this group belongs to the active controller slice.
    pub eligibility: WorkerComputeGroupEligibility,
    /// Bounded operator-facing status.
    pub health: WorkerComputeHealth,
    /// Fingerprint for which initial activation has been committed.
    pub activation_fingerprint: Option<ConfigurationFingerprint>,
    /// Delivery status of the most recent activation action.
    pub activation_status: Option<WorkerComputeProviderActionStatus>,
    /// Shared cooloff timestamp used by the pinned `no-sync` scaler.
    pub last_scale_up_at: Option<OffsetDateTime>,
    /// Previous dispatch rate independently per effective task family.
    pub prior_dispatch_rates: BTreeMap<WorkerComputeTaskType, f64>,
    /// Most recent immutable action identifier.
    pub last_action_id: Option<Uuid>,
    /// Most recent bounded provider failure category.
    pub last_failure_category: Option<WorkerComputeFailureCategory>,
}

/// Durable state for one exact Worker Deployment Version controller.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkerComputeControllerRecord {
    /// Version of this serialized record.
    pub format_version: u16,
    /// Namespace/Deployment/Build identity.
    pub key: ControllerInstanceKey,
    /// Namespace name snapshot used in provider requests and diagnostics.
    pub namespace_name: String,
    /// Monotonic decision compare-and-swap revision.
    pub revision: u64,
    /// Active, capacity-limited, or retained inactive lifecycle.
    pub lifecycle: WorkerComputeControllerLifecycle,
    /// Namespace capacity slot while active.
    pub slot: Option<u8>,
    /// Owner of the current short evaluation claim.
    pub owner: Option<IncarnationId>,
    /// Monotonic owner fence, advanced on every successful claim.
    pub owner_epoch: u64,
    /// Liveness deadline for the current claim.
    pub lease_until: Option<OffsetDateTime>,
    /// Durable scaler state by caller-supplied Scaling Group ID.
    pub groups: BTreeMap<ScalingGroupId, WorkerComputeScalingGroupState>,
    /// Earliest metrics evaluation requested by any group.
    pub next_metrics_poll_at: Option<OffsetDateTime>,
    /// Last successful catalog reconciliation.
    pub reconciled_at: OffsetDateTime,
}

/// Immutable provider action plus mutable delivery/audit fields.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerComputeProviderAction {
    /// Idempotency key sent to the remote provider.
    pub action_id: Uuid,
    /// UUID-derived due-scan bucket in `0..64`.
    pub due_bucket: u8,
    /// Owning controller.
    pub controller_key: ControllerInstanceKey,
    /// Effective scaling group.
    pub scaling_group: ScalingGroupId,
    /// Configuration fence captured when the decision committed.
    pub configuration_fingerprint: ConfigurationFingerprint,
    /// Nexus endpoint name, re-resolved before every attempt.
    pub endpoint_name: String,
    /// Why the controller requested capacity.
    pub reason: WorkerComputeInvokeReason,
    /// Exact encoded `InvokeWorkerRequest`, frozen across retries.
    pub request_data: Vec<u8>,
    /// Monotonic delivery state.
    pub status: WorkerComputeProviderActionStatus,
    /// Number of attempts durably begun.
    pub attempts: u64,
    /// Start of the current in-flight attempt, if any.
    pub attempt_started_at: Option<OffsetDateTime>,
    /// Monotonic delivery-claim fence retained while no claim is active.
    pub claim_epoch: u64,
    /// Earliest eligible retry time.
    pub next_attempt_at: OffsetDateTime,
    /// Current delivery claim.
    pub claim: Option<WorkerComputeActionClaim>,
    /// Time newer configuration made this action stale.
    pub superseded_at: Option<OffsetDateTime>,
    /// Latest bounded failure category.
    pub last_error_category: Option<WorkerComputeFailureCategory>,
    /// Initial decision time.
    pub created_at: OffsetDateTime,
    /// Latest durable state change.
    pub updated_at: OffsetDateTime,
}

impl WorkerComputeProviderAction {
    /// Derive the fixed due bucket from the first six UUID bits.
    #[must_use]
    pub fn due_bucket(action_id: Uuid) -> u8 {
        action_id.as_bytes()[0] >> 2
    }
}

/// One periodically replaced exact-version task-queue sample.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkerComputeQueueSample {
    /// Exact-version queue identity.
    pub key: WorkerComputeQueueKey,
    /// Queue-home process that produced the sample.
    pub writer_id: IncarnationId,
    /// Monotonic sequence within one writer incarnation.
    pub writer_sequence: u64,
    /// Approximate reconstructible backlog.
    pub backlog_count: u64,
    /// Recent additions per second.
    pub add_rate: f64,
    /// Recent successful dispatches per second.
    pub dispatch_rate: f64,
    /// Observation time used for expiry and cross-writer replacement.
    pub sampled_at: OffsetDateTime,
}

/// Short fenced ownership token for one controller evaluation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerComputeControllerClaim {
    /// Claimed controller.
    pub key: ControllerInstanceKey,
    /// Process incarnation holding the claim.
    pub owner: IncarnationId,
    /// Monotonic owner fence.
    pub owner_epoch: u64,
    /// Claim liveness deadline.
    pub lease_until: OffsetDateTime,
}

/// Controller snapshot returned with its claim.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClaimedWorkerComputeController {
    /// Fenced ownership token.
    pub claim: WorkerComputeControllerClaim,
    /// Snapshot at the claimed revision.
    pub record: WorkerComputeControllerRecord,
}

/// Result of namespace-capacity admission.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WorkerComputeControllerAdmission {
    /// A free slot was assigned and the instance is active.
    Admitted(WorkerComputeControllerRecord),
    /// The already-active instance was retained unchanged.
    Existing(WorkerComputeControllerRecord),
    /// No namespace slot was free; the retained record exposes that status.
    CapacityLimited(WorkerComputeControllerRecord),
}

/// Result of a fenced controller decision commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerComputeControllerCommitResult {
    /// State and optional outbox action committed atomically.
    Applied,
    /// The expected decision revision no longer matches.
    Conflict,
    /// Claim owner, epoch, or lease is stale.
    Fenced,
    /// Controller no longer exists.
    NotFound,
}

/// Fenced delivery claim for one provider action.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerComputeActionClaim {
    /// Claimed immutable action.
    pub action_id: Uuid,
    /// Delivery worker incarnation.
    pub owner: IncarnationId,
    /// Monotonic claim fence.
    pub claim_epoch: u64,
    /// Liveness deadline for this delivery attempt.
    pub claim_until: OffsetDateTime,
}

/// Claimed action returned by a namespace-scoped due scan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimedWorkerComputeProviderAction {
    /// Fenced delivery token.
    pub claim: WorkerComputeActionClaim,
    /// Current durable action snapshot.
    pub action: WorkerComputeProviderAction,
}

/// Result of durably starting provider I/O.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerComputeActionAttemptStart {
    /// Claim and fingerprint are current; provider I/O may begin.
    Started(WorkerComputeProviderAction),
    /// Configuration changed before provider I/O, so the action was superseded.
    Superseded,
    /// Another claimant owns the current epoch.
    StaleClaim,
    /// Action no longer exists.
    NotFound,
}

/// Durable outcome supplied after provider I/O.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerComputeActionFinalization {
    /// Exact synchronous provider acknowledgement.
    Delivered {
        /// Completion time.
        completed_at: OffsetDateTime,
    },
    /// Retryable failure with an already-computed bounded backoff deadline.
    RetryableFailure {
        /// Bounded category safe for persistence and metrics.
        category: WorkerComputeFailureCategory,
        /// Next eligible attempt.
        next_attempt_at: OffsetDateTime,
        /// Completion time.
        completed_at: OffsetDateTime,
    },
    /// Terminal provider failure.
    TerminalFailure {
        /// Bounded category safe for persistence and metrics.
        category: WorkerComputeFailureCategory,
        /// Completion time.
        completed_at: OffsetDateTime,
    },
    /// Explicit cancellation caused by newer configuration.
    Superseded {
        /// Supersession time.
        superseded_at: OffsetDateTime,
    },
}

/// Result of finalizing a claimed action.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerComputeActionFinalizeResult {
    /// Audit state and any still-current group health were updated.
    Applied {
        /// Actual durable status, including stale-fingerprint supersession.
        status: WorkerComputeProviderActionStatus,
    },
    /// Another claimant owns the current epoch.
    StaleClaim,
    /// Action no longer exists.
    NotFound,
}

/// Optional namespace-scoped diagnostics filter.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkerComputeHealthFilter {
    /// Restrict to one deployment name.
    pub deployment_name: Option<String>,
    /// Restrict to one exact Build ID.
    pub build_id: Option<String>,
    /// Restrict to one Scaling Group ID.
    pub scaling_group: Option<String>,
}

/// Stable, redacted diagnostics row for one scaling group.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkerComputeControllerHealthView {
    /// Public namespace snapshot.
    pub namespace_name: String,
    /// Exact controller identity.
    pub controller_key: ControllerInstanceKey,
    /// Scaling Group ID.
    pub scaling_group: ScalingGroupId,
    /// Current configuration fence.
    pub fingerprint: ConfigurationFingerprint,
    /// Bounded group health.
    pub health: WorkerComputeHealth,
    /// Most recent action ID.
    pub last_action_id: Option<Uuid>,
    /// Most recent failure category.
    pub last_failure_category: Option<WorkerComputeFailureCategory>,
    /// Next metrics evaluation, when scheduled.
    pub next_metrics_poll_at: Option<OffsetDateTime>,
}

/// Provider-neutral durable contract consumed by controller orchestration.
#[async_trait]
pub trait WorkerComputeRepository: Send + Sync {
    /// List retained controller records for one namespace in deterministic order.
    async fn list_controllers(
        &self,
        namespace_id: NamespaceId,
    ) -> Result<Vec<WorkerComputeControllerRecord>>;

    /// Release any namespace slot and retain one controller as inactive history.
    async fn inactivate_controller(
        &self,
        key: &ControllerInstanceKey,
        now: OffsetDateTime,
    ) -> Result<Option<WorkerComputeControllerRecord>>;

    /// Admit or reload one controller under the fixed namespace slot bound.
    async fn admit_controller(
        &self,
        candidate: WorkerComputeControllerRecord,
        namespace_limit: usize,
        now: OffsetDateTime,
    ) -> Result<WorkerComputeControllerAdmission>;

    /// Acquire a short claim, advancing the owner epoch.
    async fn claim_controller(
        &self,
        key: &ControllerInstanceKey,
        owner: IncarnationId,
        now: OffsetDateTime,
        lease_until: OffsetDateTime,
    ) -> Result<Option<ClaimedWorkerComputeController>>;

    /// Atomically commit next scaler state and an optional immutable action.
    async fn commit_decision(
        &self,
        claim: &WorkerComputeControllerClaim,
        expected_revision: u64,
        next: WorkerComputeControllerRecord,
        action: Option<WorkerComputeProviderAction>,
    ) -> Result<WorkerComputeControllerCommitResult>;

    /// Replace one queue sample subject to same-writer sequence fencing.
    async fn put_queue_sample(&self, sample: WorkerComputeQueueSample) -> Result<()>;

    /// List non-expired samples for one exact version in deterministic queue order.
    async fn list_queue_samples(
        &self,
        key: &ControllerInstanceKey,
        not_before: OffsetDateTime,
    ) -> Result<Vec<WorkerComputeQueueSample>>;

    /// Claim a bounded page of due actions from one namespace.
    async fn claim_due_actions(
        &self,
        namespace_id: NamespaceId,
        owner: IncarnationId,
        now: OffsetDateTime,
        claim_until: OffsetDateTime,
        limit: usize,
    ) -> Result<Vec<ClaimedWorkerComputeProviderAction>>;

    /// Revalidate configuration and durably mark provider I/O as begun.
    async fn begin_action_attempt(
        &self,
        claim: &WorkerComputeActionClaim,
        now: OffsetDateTime,
    ) -> Result<WorkerComputeActionAttemptStart>;

    /// Finalize only the current action claim.
    async fn finalize_action(
        &self,
        claim: &WorkerComputeActionClaim,
        result: WorkerComputeActionFinalization,
    ) -> Result<WorkerComputeActionFinalizeResult>;

    /// Return stable, redacted health rows for one namespace.
    async fn list_health(
        &self,
        namespace_id: NamespaceId,
        filter: WorkerComputeHealthFilter,
    ) -> Result<Vec<WorkerComputeControllerHealthView>>;
}

/// Lock-based semantic reference implementation used by local runtime and tests.
#[derive(Clone, Debug, Default)]
pub struct InMemoryWorkerComputeRepository {
    state: Arc<Mutex<InMemoryWorkerComputeState>>,
}

#[derive(Debug, Default)]
struct InMemoryWorkerComputeState {
    controllers: BTreeMap<ControllerInstanceKey, WorkerComputeControllerRecord>,
    slots: HashMap<NamespaceId, BTreeMap<u8, ControllerInstanceKey>>,
    actions: BTreeMap<Uuid, WorkerComputeProviderAction>,
    samples: BTreeMap<WorkerComputeQueueKey, WorkerComputeQueueSample>,
}

impl InMemoryWorkerComputeState {
    fn release_slot(&mut self, record: &WorkerComputeControllerRecord) {
        if let Some(slot) = record.slot
            && let Some(namespace_slots) = self.slots.get_mut(&record.key.namespace_id)
            && namespace_slots.get(&slot) == Some(&record.key)
        {
            namespace_slots.remove(&slot);
        }
    }

    fn action_fingerprint_is_current(&self, action: &WorkerComputeProviderAction) -> bool {
        self.controllers
            .get(&action.controller_key)
            .and_then(|controller| controller.groups.get(&action.scaling_group))
            .is_some_and(|group| group.fingerprint == action.configuration_fingerprint)
    }
}

#[async_trait]
impl WorkerComputeRepository for InMemoryWorkerComputeRepository {
    async fn list_controllers(
        &self,
        namespace_id: NamespaceId,
    ) -> Result<Vec<WorkerComputeControllerRecord>> {
        Ok(self
            .state
            .lock()
            .await
            .controllers
            .values()
            .filter(|record| record.key.namespace_id == namespace_id)
            .cloned()
            .collect())
    }

    async fn inactivate_controller(
        &self,
        key: &ControllerInstanceKey,
        now: OffsetDateTime,
    ) -> Result<Option<WorkerComputeControllerRecord>> {
        let mut state = self.state.lock().await;
        let Some(mut record) = state.controllers.get(key).cloned() else {
            return Ok(None);
        };
        if record.lifecycle == WorkerComputeControllerLifecycle::Inactive {
            return Ok(Some(record));
        }
        state.release_slot(&record);
        record.lifecycle = WorkerComputeControllerLifecycle::Inactive;
        record.slot = None;
        record.owner = None;
        record.lease_until = None;
        record.next_metrics_poll_at = None;
        record.revision = record.revision.saturating_add(1);
        record.reconciled_at = now;
        for group in record.groups.values_mut() {
            group.health = WorkerComputeHealth::Inactive;
        }
        state.controllers.insert(key.clone(), record.clone());
        Ok(Some(record))
    }

    async fn admit_controller(
        &self,
        mut candidate: WorkerComputeControllerRecord,
        namespace_limit: usize,
        now: OffsetDateTime,
    ) -> Result<WorkerComputeControllerAdmission> {
        let mut state = self.state.lock().await;
        if let Some(existing) = state.controllers.get(&candidate.key)
            && existing.lifecycle == WorkerComputeControllerLifecycle::Active
        {
            return Ok(WorkerComputeControllerAdmission::Existing(existing.clone()));
        }
        if let Some(existing) = state.controllers.get(&candidate.key) {
            candidate.revision = existing.revision.saturating_add(1);
            candidate.owner_epoch = existing.owner_epoch;
        }

        let limit = namespace_limit.min(WORKER_COMPUTE_NAMESPACE_SLOT_LIMIT);
        let namespace_slots = state.slots.entry(candidate.key.namespace_id).or_default();
        let free_slot = (0..limit)
            .map(|slot| u8::try_from(slot).expect("namespace slot bound is at most 100"))
            .find(|slot| !namespace_slots.contains_key(slot));
        candidate.reconciled_at = now;
        candidate.owner = None;
        candidate.lease_until = None;

        let result = if let Some(slot) = free_slot {
            candidate.lifecycle = WorkerComputeControllerLifecycle::Active;
            candidate.slot = Some(slot);
            namespace_slots.insert(slot, candidate.key.clone());
            WorkerComputeControllerAdmission::Admitted(candidate.clone())
        } else {
            candidate.lifecycle = WorkerComputeControllerLifecycle::CapacityLimited;
            candidate.slot = None;
            for group in candidate.groups.values_mut() {
                if group.eligibility == WorkerComputeGroupEligibility::Eligible {
                    group.health = WorkerComputeHealth::CapacityLimited;
                }
            }
            WorkerComputeControllerAdmission::CapacityLimited(candidate.clone())
        };
        state.controllers.insert(candidate.key.clone(), candidate);
        Ok(result)
    }

    async fn claim_controller(
        &self,
        key: &ControllerInstanceKey,
        owner: IncarnationId,
        now: OffsetDateTime,
        lease_until: OffsetDateTime,
    ) -> Result<Option<ClaimedWorkerComputeController>> {
        let mut state = self.state.lock().await;
        let Some(record) = state.controllers.get_mut(key) else {
            return Ok(None);
        };
        if record.lifecycle != WorkerComputeControllerLifecycle::Active
            || lease_until <= now
            || record
                .lease_until
                .is_some_and(|current| current > now && record.owner != Some(owner))
        {
            return Ok(None);
        }
        record.owner_epoch = record.owner_epoch.saturating_add(1);
        record.owner = Some(owner);
        record.lease_until = Some(lease_until);
        let claim = WorkerComputeControllerClaim {
            key: key.clone(),
            owner,
            owner_epoch: record.owner_epoch,
            lease_until,
        };
        Ok(Some(ClaimedWorkerComputeController {
            claim,
            record: record.clone(),
        }))
    }

    async fn commit_decision(
        &self,
        claim: &WorkerComputeControllerClaim,
        expected_revision: u64,
        next: WorkerComputeControllerRecord,
        action: Option<WorkerComputeProviderAction>,
    ) -> Result<WorkerComputeControllerCommitResult> {
        let mut state = self.state.lock().await;
        let Some(current) = state.controllers.get(&claim.key) else {
            return Ok(WorkerComputeControllerCommitResult::NotFound);
        };
        if current.owner != Some(claim.owner)
            || current.owner_epoch != claim.owner_epoch
            || current.lease_until != Some(claim.lease_until)
            || claim.lease_until <= OffsetDateTime::now_utc()
        {
            return Ok(WorkerComputeControllerCommitResult::Fenced);
        }
        if current.revision != expected_revision {
            return Ok(WorkerComputeControllerCommitResult::Conflict);
        }
        if next.key != claim.key || next.revision != expected_revision.saturating_add(1) {
            return Ok(WorkerComputeControllerCommitResult::Conflict);
        }
        if let Some(action) = action.as_ref()
            && (action.controller_key != claim.key
                || action.due_bucket >= WORKER_COMPUTE_ACTION_BUCKETS
                || action.due_bucket != WorkerComputeProviderAction::due_bucket(action.action_id)
                || state.actions.contains_key(&action.action_id))
        {
            return Ok(WorkerComputeControllerCommitResult::Conflict);
        }

        if current.lifecycle == WorkerComputeControllerLifecycle::Active
            && next.lifecycle != WorkerComputeControllerLifecycle::Active
        {
            let current = current.clone();
            state.release_slot(&current);
        }
        state.controllers.insert(claim.key.clone(), next);
        if let Some(action) = action {
            state.actions.insert(action.action_id, action);
        }
        Ok(WorkerComputeControllerCommitResult::Applied)
    }

    async fn put_queue_sample(&self, sample: WorkerComputeQueueSample) -> Result<()> {
        let mut state = self.state.lock().await;
        let replace = state.samples.get(&sample.key).is_none_or(|current| {
            if current.writer_id == sample.writer_id {
                sample.writer_sequence > current.writer_sequence
            } else {
                sample.sampled_at >= current.sampled_at
            }
        });
        if replace {
            state.samples.insert(sample.key.clone(), sample);
        }
        Ok(())
    }

    async fn list_queue_samples(
        &self,
        key: &ControllerInstanceKey,
        not_before: OffsetDateTime,
    ) -> Result<Vec<WorkerComputeQueueSample>> {
        let state = self.state.lock().await;
        Ok(state
            .samples
            .values()
            .filter(|sample| {
                sample.key.namespace_id == key.namespace_id
                    && sample.key.deployment_name == key.deployment_name
                    && sample.key.build_id == key.build_id
                    && sample.sampled_at >= not_before
            })
            .cloned()
            .collect())
    }

    async fn claim_due_actions(
        &self,
        namespace_id: NamespaceId,
        owner: IncarnationId,
        now: OffsetDateTime,
        claim_until: OffsetDateTime,
        limit: usize,
    ) -> Result<Vec<ClaimedWorkerComputeProviderAction>> {
        let limit = limit.min(WORKER_COMPUTE_ACTION_CLAIM_LIMIT);
        if claim_until <= now || limit == 0 {
            return Ok(Vec::new());
        }
        let mut state = self.state.lock().await;
        let start_bucket = owner.0.as_bytes()[0] >> 2;
        let mut due = state
            .actions
            .values()
            .filter(|action| {
                action.controller_key.namespace_id == namespace_id
                    && action.next_attempt_at <= now
                    && (action.status == WorkerComputeProviderActionStatus::Pending
                        || (action.status == WorkerComputeProviderActionStatus::Claimed
                            && action
                                .claim
                                .as_ref()
                                .is_some_and(|claim| claim.claim_until <= now)))
            })
            .map(|action| {
                (
                    action.due_bucket.wrapping_sub(start_bucket)
                        & (WORKER_COMPUTE_ACTION_BUCKETS - 1),
                    action.next_attempt_at,
                    action.action_id,
                )
            })
            .collect::<Vec<_>>();
        due.sort_unstable();

        let mut claimed = Vec::with_capacity(limit.min(due.len()));
        for (_, _, action_id) in due.into_iter().take(limit) {
            let action = state
                .actions
                .get_mut(&action_id)
                .expect("due action came from this map");
            let claim_epoch = action.claim_epoch.saturating_add(1);
            let claim = WorkerComputeActionClaim {
                action_id,
                owner,
                claim_epoch,
                claim_until,
            };
            action.status = WorkerComputeProviderActionStatus::Claimed;
            action.claim_epoch = claim_epoch;
            action.claim = Some(claim.clone());
            action.updated_at = now;
            claimed.push(ClaimedWorkerComputeProviderAction {
                claim,
                action: action.clone(),
            });
        }
        Ok(claimed)
    }

    async fn begin_action_attempt(
        &self,
        claim: &WorkerComputeActionClaim,
        now: OffsetDateTime,
    ) -> Result<WorkerComputeActionAttemptStart> {
        let mut state = self.state.lock().await;
        let Some(snapshot) = state.actions.get(&claim.action_id).cloned() else {
            return Ok(WorkerComputeActionAttemptStart::NotFound);
        };
        if snapshot.claim.as_ref() != Some(claim)
            || snapshot.status != WorkerComputeProviderActionStatus::Claimed
            || claim.claim_until <= now
        {
            return Ok(WorkerComputeActionAttemptStart::StaleClaim);
        }
        if !state.action_fingerprint_is_current(&snapshot) {
            let action = state
                .actions
                .get_mut(&claim.action_id)
                .expect("action snapshot came from this map");
            action.status = WorkerComputeProviderActionStatus::Superseded;
            action.superseded_at = Some(now);
            action.updated_at = now;
            return Ok(WorkerComputeActionAttemptStart::Superseded);
        }
        let action = state
            .actions
            .get_mut(&claim.action_id)
            .expect("action snapshot came from this map");
        action.attempts = action.attempts.saturating_add(1);
        action.attempt_started_at = Some(now);
        action.updated_at = now;
        Ok(WorkerComputeActionAttemptStart::Started(action.clone()))
    }

    async fn finalize_action(
        &self,
        claim: &WorkerComputeActionClaim,
        result: WorkerComputeActionFinalization,
    ) -> Result<WorkerComputeActionFinalizeResult> {
        let mut state = self.state.lock().await;
        let Some(snapshot) = state.actions.get(&claim.action_id).cloned() else {
            return Ok(WorkerComputeActionFinalizeResult::NotFound);
        };
        let completed_at = match &result {
            WorkerComputeActionFinalization::Delivered { completed_at }
            | WorkerComputeActionFinalization::RetryableFailure { completed_at, .. }
            | WorkerComputeActionFinalization::TerminalFailure { completed_at, .. } => {
                *completed_at
            }
            WorkerComputeActionFinalization::Superseded { superseded_at } => *superseded_at,
        };
        if snapshot.claim.as_ref() != Some(claim)
            || snapshot.status != WorkerComputeProviderActionStatus::Claimed
            || claim.claim_until <= completed_at
        {
            return Ok(WorkerComputeActionFinalizeResult::StaleClaim);
        }
        let fingerprint_current = state.action_fingerprint_is_current(&snapshot);
        let (status, category, next_attempt_at, superseded_at, completed_at) = match result {
            WorkerComputeActionFinalization::Delivered { completed_at } => (
                WorkerComputeProviderActionStatus::Delivered,
                None,
                snapshot.next_attempt_at,
                None,
                completed_at,
            ),
            WorkerComputeActionFinalization::RetryableFailure {
                category,
                next_attempt_at,
                completed_at,
            } if fingerprint_current => (
                WorkerComputeProviderActionStatus::Pending,
                Some(category),
                next_attempt_at,
                None,
                completed_at,
            ),
            WorkerComputeActionFinalization::RetryableFailure { completed_at, .. } => (
                WorkerComputeProviderActionStatus::Superseded,
                None,
                snapshot.next_attempt_at,
                Some(completed_at),
                completed_at,
            ),
            WorkerComputeActionFinalization::TerminalFailure {
                category,
                completed_at,
            } if fingerprint_current => (
                WorkerComputeProviderActionStatus::TerminalFailed,
                Some(category),
                snapshot.next_attempt_at,
                None,
                completed_at,
            ),
            WorkerComputeActionFinalization::TerminalFailure { completed_at, .. } => (
                WorkerComputeProviderActionStatus::Superseded,
                None,
                snapshot.next_attempt_at,
                Some(completed_at),
                completed_at,
            ),
            WorkerComputeActionFinalization::Superseded { superseded_at } => (
                WorkerComputeProviderActionStatus::Superseded,
                None,
                snapshot.next_attempt_at,
                Some(superseded_at),
                superseded_at,
            ),
        };

        let action = state
            .actions
            .get_mut(&claim.action_id)
            .expect("action snapshot came from this map");
        action.status = status;
        action.last_error_category = category;
        action.next_attempt_at = next_attempt_at;
        action.superseded_at = superseded_at;
        action.claim = None;
        action.updated_at = completed_at;

        if fingerprint_current
            && let Some(controller) = state.controllers.get_mut(&snapshot.controller_key)
            && let Some(group) = controller.groups.get_mut(&snapshot.scaling_group)
        {
            group.last_action_id = Some(snapshot.action_id);
            group.last_failure_category = category;
            group.health = match status {
                WorkerComputeProviderActionStatus::Pending => WorkerComputeHealth::DeliveryRetrying,
                WorkerComputeProviderActionStatus::TerminalFailed => {
                    WorkerComputeHealth::DeliveryTerminalFailure
                }
                _ => WorkerComputeHealth::Active,
            };
            if snapshot.reason == WorkerComputeInvokeReason::ConfigurationActivation {
                group.activation_status = Some(status);
            }
            // Health shares the controller document with scaler state. Advancing
            // the revision prevents an evaluation claimed before finalization from
            // overwriting the newer audit/health view with an older snapshot.
            controller.revision = controller.revision.saturating_add(1);
        }
        Ok(WorkerComputeActionFinalizeResult::Applied { status })
    }

    async fn list_health(
        &self,
        namespace_id: NamespaceId,
        filter: WorkerComputeHealthFilter,
    ) -> Result<Vec<WorkerComputeControllerHealthView>> {
        let state = self.state.lock().await;
        let mut rows = state
            .controllers
            .values()
            .filter(|controller| controller.key.namespace_id == namespace_id)
            .filter(|controller| {
                filter
                    .deployment_name
                    .as_ref()
                    .is_none_or(|value| controller.key.deployment_name.0 == *value)
                    && filter
                        .build_id
                        .as_ref()
                        .is_none_or(|value| controller.key.build_id.0 == *value)
            })
            .flat_map(|controller| {
                controller.groups.iter().filter_map(|(group_id, group)| {
                    if filter
                        .scaling_group
                        .as_ref()
                        .is_some_and(|value| group_id.0 != *value)
                    {
                        return None;
                    }
                    Some(WorkerComputeControllerHealthView {
                        namespace_name: controller.namespace_name.clone(),
                        controller_key: controller.key.clone(),
                        scaling_group: group_id.clone(),
                        fingerprint: group.fingerprint,
                        health: group.health,
                        last_action_id: group.last_action_id,
                        last_failure_category: group.last_failure_category,
                        next_metrics_poll_at: controller.next_metrics_poll_at,
                    })
                })
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            left.controller_key
                .cmp(&right.controller_key)
                .then_with(|| left.scaling_group.cmp(&right.scaling_group))
        });
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use proptest::prelude::*;
    use time::Duration;
    use tokeira_types::{
        BuildId, DeploymentId, TaskQueueName, WorkerComputeQueueKey, WorkerComputeTaskQueueBinding,
    };

    use super::*;

    fn key(namespace_id: NamespaceId, index: usize) -> ControllerInstanceKey {
        ControllerInstanceKey {
            namespace_id,
            deployment_name: DeploymentId(format!("deployment-{index:03}")),
            build_id: BuildId(format!("build-{index:03}")),
        }
    }

    fn group_state(seed: u8) -> WorkerComputeScalingGroupState {
        WorkerComputeScalingGroupState {
            fingerprint: ConfigurationFingerprint::from_canonical_bytes(&[seed]),
            effective_task_types: BTreeSet::from([WorkerComputeTaskType::Workflow]),
            eligibility: WorkerComputeGroupEligibility::Eligible,
            health: WorkerComputeHealth::Active,
            activation_fingerprint: None,
            activation_status: None,
            last_scale_up_at: None,
            prior_dispatch_rates: BTreeMap::new(),
            last_action_id: None,
            last_failure_category: None,
        }
    }

    fn controller(
        namespace_id: NamespaceId,
        index: usize,
        now: OffsetDateTime,
    ) -> WorkerComputeControllerRecord {
        WorkerComputeControllerRecord {
            format_version: WORKER_COMPUTE_RECORD_FORMAT_VERSION,
            key: key(namespace_id, index),
            namespace_name: "namespace-a".to_owned(),
            revision: 0,
            lifecycle: WorkerComputeControllerLifecycle::Inactive,
            slot: None,
            owner: None,
            owner_epoch: 0,
            lease_until: None,
            groups: BTreeMap::from([(ScalingGroupId("primary".to_owned()), group_state(1))]),
            next_metrics_poll_at: Some(now),
            reconciled_at: now,
        }
    }

    fn action(
        controller: &WorkerComputeControllerRecord,
        action_id: Uuid,
        now: OffsetDateTime,
    ) -> WorkerComputeProviderAction {
        WorkerComputeProviderAction {
            action_id,
            due_bucket: WorkerComputeProviderAction::due_bucket(action_id),
            controller_key: controller.key.clone(),
            scaling_group: ScalingGroupId("primary".to_owned()),
            configuration_fingerprint: controller
                .groups
                .get(&ScalingGroupId("primary".to_owned()))
                .expect("fixture group exists")
                .fingerprint,
            endpoint_name: "worker-compute".to_owned(),
            reason: WorkerComputeInvokeReason::NoSyncMatch,
            request_data: vec![1, 2, 3],
            status: WorkerComputeProviderActionStatus::Pending,
            attempts: 0,
            attempt_started_at: None,
            claim_epoch: 0,
            next_attempt_at: now,
            claim: None,
            superseded_at: None,
            last_error_category: None,
            created_at: now,
            updated_at: now,
        }
    }

    async fn admit_and_claim(
        repository: &InMemoryWorkerComputeRepository,
        namespace_id: NamespaceId,
        now: OffsetDateTime,
    ) -> ClaimedWorkerComputeController {
        let candidate = controller(namespace_id, 0, now);
        let admitted = repository
            .admit_controller(candidate, WORKER_COMPUTE_NAMESPACE_SLOT_LIMIT, now)
            .await
            .expect("admission succeeds");
        assert!(matches!(
            admitted,
            WorkerComputeControllerAdmission::Admitted(_)
        ));
        repository
            .claim_controller(
                &key(namespace_id, 0),
                IncarnationId::new(),
                now,
                now + Duration::minutes(1),
            )
            .await
            .expect("claim succeeds")
            .expect("controller is claimable")
    }

    #[tokio::test]
    async fn namespace_admission_uses_fixed_slots_and_retains_capacity_limited_records() {
        let repository = InMemoryWorkerComputeRepository::default();
        let namespace_id = NamespaceId::new();
        let now = OffsetDateTime::now_utc();

        for index in 0..3 {
            let result = repository
                .admit_controller(controller(namespace_id, index, now), 2, now)
                .await
                .unwrap();
            if index < 2 {
                assert!(matches!(
                    result,
                    WorkerComputeControllerAdmission::Admitted(_)
                ));
            } else {
                assert!(matches!(
                    result,
                    WorkerComputeControllerAdmission::CapacityLimited(_)
                ));
            }
        }

        let rows = repository
            .list_health(namespace_id, WorkerComputeHealthFilter::default())
            .await
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert!(rows.windows(2).all(|pair| {
            pair[0].controller_key.cmp(&pair[1].controller_key) != std::cmp::Ordering::Greater
        }));
    }

    #[tokio::test]
    async fn samples_suppress_stale_same_writer_sequences() {
        let repository = InMemoryWorkerComputeRepository::default();
        let namespace_id = NamespaceId::new();
        let writer_id = IncarnationId::new();
        let now = OffsetDateTime::now_utc();
        let sample_key = WorkerComputeQueueKey {
            namespace_id,
            deployment_name: DeploymentId("deployment-a".to_owned()),
            build_id: BuildId("build-a".to_owned()),
            task_type: WorkerComputeTaskType::Activity,
            task_queue: TaskQueueName("queue-a".to_owned()),
        };
        for (sequence, backlog_count) in [(2, 20), (1, 10)] {
            repository
                .put_queue_sample(WorkerComputeQueueSample {
                    key: sample_key.clone(),
                    writer_id,
                    writer_sequence: sequence,
                    backlog_count,
                    add_rate: 1.0,
                    dispatch_rate: 2.0,
                    sampled_at: now,
                })
                .await
                .unwrap();
        }
        let samples = repository
            .list_queue_samples(
                &ControllerInstanceKey {
                    namespace_id,
                    deployment_name: sample_key.deployment_name.clone(),
                    build_id: sample_key.build_id.clone(),
                },
                now - Duration::seconds(1),
            )
            .await
            .unwrap();
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].writer_sequence, 2);
        assert_eq!(samples[0].backlog_count, 20);
    }

    #[tokio::test]
    async fn stale_fingerprint_is_superseded_before_provider_io() {
        let repository = InMemoryWorkerComputeRepository::default();
        let namespace_id = NamespaceId::new();
        let now = OffsetDateTime::now_utc();
        let claimed = admit_and_claim(&repository, namespace_id, now).await;
        let action_id = Uuid::new_v4();
        let pending = action(&claimed.record, action_id, now);
        let mut next = claimed.record.clone();
        next.revision += 1;
        next.groups
            .get_mut(&ScalingGroupId("primary".to_owned()))
            .unwrap()
            .fingerprint = ConfigurationFingerprint::from_canonical_bytes(b"new");
        assert_eq!(
            repository
                .commit_decision(&claimed.claim, claimed.record.revision, next, Some(pending))
                .await
                .unwrap(),
            WorkerComputeControllerCommitResult::Applied
        );

        let claimed_actions = repository
            .claim_due_actions(
                namespace_id,
                IncarnationId::new(),
                now,
                now + Duration::minutes(1),
                1,
            )
            .await
            .unwrap();
        assert_eq!(claimed_actions.len(), 1);
        assert_eq!(
            repository
                .begin_action_attempt(&claimed_actions[0].claim, now)
                .await
                .unwrap(),
            WorkerComputeActionAttemptStart::Superseded
        );
    }

    #[tokio::test]
    async fn inactivation_releases_a_slot_for_capacity_limited_promotion() {
        let repository = InMemoryWorkerComputeRepository::default();
        let namespace_id = NamespaceId::new();
        let now = OffsetDateTime::now_utc();
        let first = repository
            .admit_controller(controller(namespace_id, 0, now), 1, now)
            .await
            .unwrap();
        assert!(matches!(
            first,
            WorkerComputeControllerAdmission::Admitted(_)
        ));
        let waiting = repository
            .admit_controller(controller(namespace_id, 1, now), 1, now)
            .await
            .unwrap();
        assert!(matches!(
            waiting,
            WorkerComputeControllerAdmission::CapacityLimited(_)
        ));

        let claimed = repository
            .claim_controller(
                &key(namespace_id, 0),
                IncarnationId::new(),
                now,
                now + Duration::minutes(1),
            )
            .await
            .unwrap()
            .expect("active controller is claimable");
        let mut inactive = claimed.record.clone();
        inactive.revision += 1;
        inactive.lifecycle = WorkerComputeControllerLifecycle::Inactive;
        inactive.slot = None;
        assert_eq!(
            repository
                .commit_decision(&claimed.claim, claimed.record.revision, inactive, None)
                .await
                .unwrap(),
            WorkerComputeControllerCommitResult::Applied
        );
        assert!(matches!(
            repository
                .admit_controller(controller(namespace_id, 1, now), 1, now)
                .await
                .unwrap(),
            WorkerComputeControllerAdmission::Admitted(_)
        ));
    }

    #[tokio::test]
    async fn retry_claim_epochs_survive_pending_state_and_stale_finalizers() {
        let repository = InMemoryWorkerComputeRepository::default();
        let namespace_id = NamespaceId::new();
        let now = OffsetDateTime::now_utc();
        let claimed = admit_and_claim(&repository, namespace_id, now).await;
        let action_id = Uuid::new_v4();
        let pending = action(&claimed.record, action_id, now);
        let mut next = claimed.record.clone();
        next.revision += 1;
        repository
            .commit_decision(&claimed.claim, claimed.record.revision, next, Some(pending))
            .await
            .unwrap();

        let first = repository
            .claim_due_actions(
                namespace_id,
                IncarnationId::new(),
                now,
                now + Duration::seconds(30),
                1,
            )
            .await
            .unwrap()
            .pop()
            .expect("pending action is claimable");
        let WorkerComputeActionAttemptStart::Started(started) = repository
            .begin_action_attempt(&first.claim, now)
            .await
            .unwrap()
        else {
            panic!("current action should begin");
        };
        assert_eq!(started.attempts, 1);
        assert_eq!(started.attempt_started_at, Some(now));
        assert_eq!(
            repository
                .finalize_action(
                    &first.claim,
                    WorkerComputeActionFinalization::RetryableFailure {
                        category: WorkerComputeFailureCategory::Transport,
                        next_attempt_at: now + Duration::seconds(31),
                        completed_at: now + Duration::seconds(1),
                    },
                )
                .await
                .unwrap(),
            WorkerComputeActionFinalizeResult::Applied {
                status: WorkerComputeProviderActionStatus::Pending,
            }
        );

        let second = repository
            .claim_due_actions(
                namespace_id,
                IncarnationId::new(),
                now + Duration::seconds(31),
                now + Duration::minutes(2),
                1,
            )
            .await
            .unwrap()
            .pop()
            .expect("retry is claimable");
        assert!(second.claim.claim_epoch > first.claim.claim_epoch);
        assert_eq!(
            repository
                .finalize_action(
                    &first.claim,
                    WorkerComputeActionFinalization::Delivered {
                        completed_at: now + Duration::seconds(32),
                    },
                )
                .await
                .unwrap(),
            WorkerComputeActionFinalizeResult::StaleClaim
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        // Feature: worker-compute-controller, Property 10: concurrent decision commit creates at most one action
        #[test]
        fn property_concurrent_decision_commit_creates_at_most_one_action(
            duplicate_commits in 2usize..20,
        ) {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let repository = InMemoryWorkerComputeRepository::default();
                let namespace_id = NamespaceId::new();
                let now = OffsetDateTime::now_utc();
                let claimed = admit_and_claim(&repository, namespace_id, now).await;
                let action_id = Uuid::new_v4();
                let pending = action(&claimed.record, action_id, now);
                let mut next = claimed.record.clone();
                next.revision += 1;
                let mut applied = 0;
                for _ in 0..duplicate_commits {
                    if repository
                        .commit_decision(
                            &claimed.claim,
                            claimed.record.revision,
                            next.clone(),
                            Some(pending.clone()),
                        )
                        .await
                        .unwrap()
                        == WorkerComputeControllerCommitResult::Applied
                    {
                        applied += 1;
                    }
                }
                prop_assert_eq!(applied, 1);
                let actions = repository
                    .claim_due_actions(
                        namespace_id,
                        IncarnationId::new(),
                        now,
                        now + Duration::minutes(1),
                        duplicate_commits,
                    )
                    .await
                    .unwrap();
                prop_assert_eq!(actions.len(), 1);
                Ok::<(), TestCaseError>(())
            })?;
        }

        // Feature: worker-compute-controller, Property 11: restart, capacity, and fingerprint fences survive
        #[test]
        fn property_restart_capacity_and_fingerprint_fences_survive(
            namespace_limit in 1usize..=WORKER_COMPUTE_NAMESPACE_SLOT_LIMIT,
            excess in 1usize..20,
        ) {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let repository = InMemoryWorkerComputeRepository::default();
                let namespace_id = NamespaceId::new();
                let now = OffsetDateTime::now_utc();
                let mut active = 0;
                for index in 0..namespace_limit.saturating_add(excess) {
                    match repository
                        .admit_controller(controller(namespace_id, index, now), namespace_limit, now)
                        .await
                        .unwrap()
                    {
                        WorkerComputeControllerAdmission::Admitted(_) => active += 1,
                        WorkerComputeControllerAdmission::CapacityLimited(_) => {}
                        WorkerComputeControllerAdmission::Existing(_) => {
                            return Err(TestCaseError::fail("fresh key was reported as existing"));
                        }
                    }
                }
                prop_assert_eq!(active, namespace_limit);

                let first_owner = IncarnationId::new();
                let first = repository
                    .claim_controller(
                        &key(namespace_id, 0),
                        first_owner,
                        now,
                        now + Duration::minutes(1),
                    )
                    .await
                    .unwrap()
                    .expect("active controller is claimable");
                let second = repository
                    .claim_controller(
                        &key(namespace_id, 0),
                        first_owner,
                        now,
                        now + Duration::minutes(1),
                    )
                    .await
                    .unwrap()
                    .expect("same owner may advance its epoch");
                let mut stale_next = first.record.clone();
                stale_next.revision += 1;
                prop_assert_eq!(
                    repository
                        .commit_decision(&first.claim, first.record.revision, stale_next, None)
                        .await
                        .unwrap(),
                    WorkerComputeControllerCommitResult::Fenced
                );
                let mut current_next = second.record.clone();
                current_next.revision += 1;
                prop_assert_eq!(
                    repository
                        .commit_decision(
                            &second.claim,
                            second.record.revision,
                            current_next,
                            None,
                        )
                        .await
                        .unwrap(),
                    WorkerComputeControllerCommitResult::Applied
                );

                let third = repository
                    .claim_controller(
                        &key(namespace_id, 0),
                        first_owner,
                        now,
                        now + Duration::minutes(1),
                    )
                    .await
                    .unwrap()
                    .expect("controller remains claimable after restart");
                let pending = action(&third.record, Uuid::new_v4(), now);
                let mut changed = third.record.clone();
                changed.revision += 1;
                changed
                    .groups
                    .get_mut(&ScalingGroupId("primary".to_owned()))
                    .expect("fixture group exists")
                    .fingerprint = ConfigurationFingerprint::from_canonical_bytes(b"changed");
                prop_assert_eq!(
                    repository
                        .commit_decision(
                            &third.claim,
                            third.record.revision,
                            changed,
                            Some(pending),
                        )
                        .await
                        .unwrap(),
                    WorkerComputeControllerCommitResult::Applied
                );
                let claimed_action = repository
                    .claim_due_actions(
                        namespace_id,
                        IncarnationId::new(),
                        now,
                        now + Duration::minutes(1),
                        1,
                    )
                    .await
                    .unwrap()
                    .pop()
                    .expect("durable action survives controller restart");
                prop_assert_eq!(
                    repository
                        .begin_action_attempt(&claimed_action.claim, now)
                        .await
                        .unwrap(),
                    WorkerComputeActionAttemptStart::Superseded
                );
                Ok::<(), TestCaseError>(())
            })?;
        }
    }

    #[test]
    fn action_request_helpers_are_stable_and_secret_free() {
        let binding = WorkerComputeTaskQueueBinding {
            name: TaskQueueName("queue-a".to_owned()),
            task_type: WorkerComputeTaskType::Workflow,
        };
        assert_eq!(binding.name.0, "queue-a");
        let action_id =
            Uuid::parse_str("fc000000-0000-0000-0000-000000000000").expect("valid UUID");
        assert_eq!(
            WorkerComputeProviderAction::due_bucket(action_id),
            WORKER_COMPUTE_ACTION_BUCKETS - 1
        );
    }
}
