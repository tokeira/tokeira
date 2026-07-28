//! Durable worker-compute provider-action delivery.
//!
//! The outbox claims only one namespace at a time, persists the attempt boundary
//! before calling a provider, and finalizes through the repository's claim epoch.
//! Provider I/O never holds a storage transaction or any workflow/task-delivery
//! resource.

use std::sync::Arc;

use anyhow::{Context, Result};
use prost::Message;
use tokeira_storage::{
    ClaimedWorkerComputeProviderAction, WORKER_COMPUTE_ACTION_CLAIM_LIMIT,
    WorkerComputeActionAttemptStart, WorkerComputeActionFinalization,
    WorkerComputeActionFinalizeResult, WorkerComputeRepository,
};
use tokeira_types::{IncarnationId, NamespaceId, WorkerComputeProviderActionStatus};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use super::{
    ACTION_CLAIM_LEASE, ACTION_DELIVERY_IDLE_INTERVAL, ACTION_RETRY_COEFFICIENT,
    ACTION_RETRY_INITIAL_INTERVAL, ACTION_RETRY_MAXIMUM_INTERVAL, WorkerComputeClock,
    WorkerComputeProvider, WorkerComputeProviderOutcome,
};
use crate::metrics as runtime_metrics;

/// Bounded result counters for one namespace-scoped delivery sweep.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WorkerComputeOutboxSweep {
    /// Actions whose claim epoch advanced.
    pub claimed: u64,
    /// Provider attempts durably begun.
    pub attempted: u64,
    /// Exact synchronous acknowledgements.
    pub delivered: u64,
    /// Retryable failures returned to Pending.
    pub retrying: u64,
    /// Non-retryable failures retained for audit.
    pub terminal_failed: u64,
    /// Actions suppressed by a newer configuration fingerprint.
    pub superseded: u64,
    /// Lost claims or absent actions that could not be finalized.
    pub fenced: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AttemptDisposition {
    Delivered,
    Retrying,
    TerminalFailed,
    SupersededBeforeAttempt,
    SupersededAfterAttempt,
    Fenced,
}

/// Namespace-scoped durable action delivery orchestrator.
#[derive(Clone)]
pub struct WorkerComputeOutbox {
    repository: Arc<dyn WorkerComputeRepository>,
    provider: Arc<dyn WorkerComputeProvider>,
    clock: Arc<dyn WorkerComputeClock>,
    owner: IncarnationId,
}

impl std::fmt::Debug for WorkerComputeOutbox {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkerComputeOutbox")
            .field("owner", &self.owner)
            .finish_non_exhaustive()
    }
}

impl WorkerComputeOutbox {
    /// Construct one process-incarnation delivery worker.
    #[must_use]
    pub fn new(
        repository: Arc<dyn WorkerComputeRepository>,
        provider: Arc<dyn WorkerComputeProvider>,
        clock: Arc<dyn WorkerComputeClock>,
        owner: IncarnationId,
    ) -> Self {
        Self {
            repository,
            provider,
            clock,
            owner,
        }
    }

    /// Claim and concurrently deliver one bounded page for `namespace_id`.
    pub async fn deliver_namespace_once(
        &self,
        namespace_id: NamespaceId,
    ) -> Result<WorkerComputeOutboxSweep> {
        let now = self.clock.now();
        let claim_until = now
            + time::Duration::try_from(ACTION_CLAIM_LEASE)
                .expect("fixed action claim lease fits time::Duration");
        let claimed = self
            .repository
            .claim_due_actions(
                namespace_id,
                self.owner,
                now,
                claim_until,
                WORKER_COMPUTE_ACTION_CLAIM_LIMIT,
            )
            .await
            .context("claiming due worker-compute actions")?;
        let mut sweep = WorkerComputeOutboxSweep {
            claimed: u64::try_from(claimed.len()).expect("claim limit fits u64"),
            ..WorkerComputeOutboxSweep::default()
        };
        let mut attempts = JoinSet::new();
        for claimed_action in claimed {
            let outbox = self.clone();
            attempts.spawn(async move { outbox.deliver_claimed(claimed_action).await });
        }

        let mut first_error = None;
        while let Some(result) = attempts.join_next().await {
            match result {
                Ok(Ok(disposition)) => sweep.record(disposition),
                Ok(Err(error)) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(
                            anyhow::Error::new(error)
                                .context("worker-compute delivery task failed"),
                        );
                    }
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(sweep)
    }

    /// Run one namespace until cancellation, finishing any already-begun sweep.
    pub async fn run_namespace(
        &self,
        namespace_id: NamespaceId,
        shutdown: CancellationToken,
    ) -> Result<()> {
        loop {
            if shutdown.is_cancelled() {
                return Ok(());
            }
            let sweep = self.deliver_namespace_once(namespace_id).await?;
            if sweep.claimed != 0 {
                continue;
            }
            tokio::select! {
                () = shutdown.cancelled() => return Ok(()),
                () = tokio::time::sleep(ACTION_DELIVERY_IDLE_INTERVAL) => {}
            }
        }
    }

    async fn deliver_claimed(
        &self,
        claimed: ClaimedWorkerComputeProviderAction,
    ) -> Result<AttemptDisposition> {
        let attempt_started_at = self.clock.now();
        let started = self
            .repository
            .begin_action_attempt(&claimed.claim, attempt_started_at)
            .await
            .context("beginning worker-compute provider attempt")?;
        let action = match started {
            WorkerComputeActionAttemptStart::Started(action) => action,
            WorkerComputeActionAttemptStart::Superseded => {
                return Ok(AttemptDisposition::SupersededBeforeAttempt);
            }
            WorkerComputeActionAttemptStart::StaleClaim
            | WorkerComputeActionAttemptStart::NotFound => {
                return Ok(AttemptDisposition::Fenced);
            }
        };

        let delivery_started = std::time::Instant::now();
        let attempt = self
            .provider
            .deliver(&action, claimed.claim.claim_epoch, attempt_started_at)
            .await;
        let completed_at = self.clock.now();
        let finalization = match attempt.outcome {
            WorkerComputeProviderOutcome::Delivered => {
                WorkerComputeActionFinalization::Delivered { completed_at }
            }
            WorkerComputeProviderOutcome::RetryableFailure(category) => {
                WorkerComputeActionFinalization::RetryableFailure {
                    category,
                    next_attempt_at: completed_at
                        + time::Duration::try_from(action_retry_delay(action.attempts))
                            .expect("bounded action retry delay fits time::Duration"),
                    completed_at,
                }
            }
            WorkerComputeProviderOutcome::TerminalFailure(category) => {
                WorkerComputeActionFinalization::TerminalFailure {
                    category,
                    completed_at,
                }
            }
        };
        let finalized = self
            .repository
            .finalize_action(&claimed.claim, finalization)
            .await
            .context("finalizing worker-compute provider attempt")?;
        let disposition = match finalized {
            WorkerComputeActionFinalizeResult::Applied {
                status: WorkerComputeProviderActionStatus::Delivered,
            } => AttemptDisposition::Delivered,
            WorkerComputeActionFinalizeResult::Applied {
                status: WorkerComputeProviderActionStatus::Pending,
            } => AttemptDisposition::Retrying,
            WorkerComputeActionFinalizeResult::Applied {
                status: WorkerComputeProviderActionStatus::TerminalFailed,
            } => AttemptDisposition::TerminalFailed,
            WorkerComputeActionFinalizeResult::Applied {
                status: WorkerComputeProviderActionStatus::Superseded,
            } => AttemptDisposition::SupersededAfterAttempt,
            WorkerComputeActionFinalizeResult::Applied {
                status: WorkerComputeProviderActionStatus::Claimed,
            }
            | WorkerComputeActionFinalizeResult::StaleClaim
            | WorkerComputeActionFinalizeResult::NotFound => AttemptDisposition::Fenced,
        };
        runtime_metrics::record_worker_compute_action(
            attempt.target_kind,
            disposition.metric_outcome(),
            delivery_started.elapsed(),
        );
        let namespace_name =
            tokeira_proto::compute::v1::InvokeWorkerRequest::decode(action.request_data.as_slice())
                .map_or_else(|_| "_unknown_".to_owned(), |request| request.namespace);
        tracing::info!(
            namespace = %namespace_name,
            deployment = %action.controller_key.deployment_name.0,
            build_id = %action.controller_key.build_id.0,
            scaling_group = %action.scaling_group.0,
            reason = ?action.reason,
            action_id = %action.action_id,
            outcome = disposition.metric_outcome(),
            "worker-compute provider action finished"
        );
        Ok(disposition)
    }
}

impl WorkerComputeOutboxSweep {
    fn record(&mut self, disposition: AttemptDisposition) {
        match disposition {
            AttemptDisposition::Delivered => {
                self.attempted = self.attempted.saturating_add(1);
                self.delivered = self.delivered.saturating_add(1);
            }
            AttemptDisposition::Retrying => {
                self.attempted = self.attempted.saturating_add(1);
                self.retrying = self.retrying.saturating_add(1);
            }
            AttemptDisposition::TerminalFailed => {
                self.attempted = self.attempted.saturating_add(1);
                self.terminal_failed = self.terminal_failed.saturating_add(1);
            }
            AttemptDisposition::SupersededBeforeAttempt => {
                self.superseded = self.superseded.saturating_add(1);
            }
            AttemptDisposition::SupersededAfterAttempt => {
                self.attempted = self.attempted.saturating_add(1);
                self.superseded = self.superseded.saturating_add(1);
            }
            AttemptDisposition::Fenced => {
                self.fenced = self.fenced.saturating_add(1);
            }
        }
    }
}

impl AttemptDisposition {
    const fn metric_outcome(self) -> &'static str {
        match self {
            Self::Delivered => "delivered",
            Self::Retrying => "retrying",
            Self::TerminalFailed => "terminal_failure",
            Self::SupersededBeforeAttempt | Self::SupersededAfterAttempt => "superseded",
            Self::Fenced => "fenced",
        }
    }
}

/// Retry delay after the durably-recorded `attempts` count.
#[must_use]
pub fn action_retry_delay(attempts: u64) -> std::time::Duration {
    let exponent = u32::try_from(attempts.saturating_sub(1)).unwrap_or(u32::MAX);
    let multiplier = u64::from(ACTION_RETRY_COEFFICIENT).saturating_pow(exponent);
    let seconds = ACTION_RETRY_INITIAL_INTERVAL
        .as_secs()
        .saturating_mul(multiplier)
        .min(ACTION_RETRY_MAXIMUM_INTERVAL.as_secs());
    std::time::Duration::from_secs(seconds)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet, VecDeque},
        sync::Mutex,
    };

    use async_trait::async_trait;
    use proptest::prelude::*;
    use time::OffsetDateTime;
    use tokeira_storage::{
        InMemoryWorkerComputeRepository, WORKER_COMPUTE_NAMESPACE_SLOT_LIMIT,
        WORKER_COMPUTE_RECORD_FORMAT_VERSION, WorkerComputeControllerAdmission,
        WorkerComputeControllerCommitResult, WorkerComputeControllerRecord,
        WorkerComputeHealthFilter, WorkerComputeScalingGroupState,
    };
    use tokeira_types::{
        BuildId, ConfigurationFingerprint, ControllerInstanceKey, DeploymentId, ScalingGroupId,
        WorkerComputeControllerLifecycle, WorkerComputeFailureCategory,
        WorkerComputeGroupEligibility, WorkerComputeHealth, WorkerComputeInvokeReason,
        WorkerComputeProviderActionStatus, WorkerComputeTaskType,
    };
    use tokio::sync::Notify;
    use uuid::Uuid;

    use super::*;
    use crate::{
        WorkerComputeProviderAttempt, WorkerComputeProviderTargetKind,
        worker_compute::{ProviderActionInput, RemoteNexusProvider, build_provider_action},
    };

    #[derive(Debug)]
    struct TestClock {
        now: Mutex<OffsetDateTime>,
    }

    impl TestClock {
        fn new(now: OffsetDateTime) -> Self {
            Self {
                now: Mutex::new(now),
            }
        }

        fn advance(&self, duration: std::time::Duration) {
            let mut now = self.now.lock().expect("test clock lock poisoned");
            *now += time::Duration::try_from(duration).expect("test duration fits");
        }
    }

    impl WorkerComputeClock for TestClock {
        fn now(&self) -> OffsetDateTime {
            *self.now.lock().expect("test clock lock poisoned")
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct ProviderCall {
        action_id: Uuid,
        request_data: Vec<u8>,
        claim_epoch: u64,
    }

    #[derive(Debug)]
    struct ScriptedProvider {
        outcomes: Mutex<VecDeque<WorkerComputeProviderOutcome>>,
        calls: Mutex<Vec<ProviderCall>>,
    }

    impl ScriptedProvider {
        fn new(outcomes: impl IntoIterator<Item = WorkerComputeProviderOutcome>) -> Self {
            Self {
                outcomes: Mutex::new(outcomes.into_iter().collect()),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<ProviderCall> {
            self.calls
                .lock()
                .expect("provider calls lock poisoned")
                .clone()
        }
    }

    #[async_trait]
    impl WorkerComputeProvider for ScriptedProvider {
        async fn deliver(
            &self,
            action: &tokeira_storage::WorkerComputeProviderAction,
            claim_epoch: u64,
            _now: OffsetDateTime,
        ) -> WorkerComputeProviderAttempt {
            self.calls
                .lock()
                .expect("provider calls lock poisoned")
                .push(ProviderCall {
                    action_id: action.action_id,
                    request_data: action.request_data.clone(),
                    claim_epoch,
                });
            let outcome = self
                .outcomes
                .lock()
                .expect("provider outcomes lock poisoned")
                .pop_front()
                .unwrap_or(WorkerComputeProviderOutcome::Delivered);
            WorkerComputeProviderAttempt {
                target_kind: WorkerComputeProviderTargetKind::External,
                outcome,
            }
        }
    }

    #[derive(Clone, Debug)]
    struct SeededAction {
        namespace_id: NamespaceId,
        controller_key: ControllerInstanceKey,
        controller_owner: IncarnationId,
        action: tokeira_storage::WorkerComputeProviderAction,
    }

    fn group_state(fingerprint: ConfigurationFingerprint) -> WorkerComputeScalingGroupState {
        WorkerComputeScalingGroupState {
            fingerprint,
            effective_task_types: BTreeSet::from([WorkerComputeTaskType::Workflow]),
            eligibility: WorkerComputeGroupEligibility::Eligible,
            health: WorkerComputeHealth::Active,
            activation_fingerprint: Some(fingerprint),
            activation_status: Some(WorkerComputeProviderActionStatus::Delivered),
            last_scale_up_at: None,
            prior_dispatch_rates: BTreeMap::new(),
            last_action_id: None,
            last_failure_category: None,
        }
    }

    async fn seed_action(
        repository: &InMemoryWorkerComputeRepository,
        now: OffsetDateTime,
    ) -> SeededAction {
        let namespace_id = NamespaceId::new();
        let controller_key = ControllerInstanceKey {
            namespace_id,
            deployment_name: DeploymentId("deployment".to_owned()),
            build_id: BuildId("build".to_owned()),
        };
        let fingerprint = ConfigurationFingerprint::from_canonical_bytes(b"current");
        let candidate = WorkerComputeControllerRecord {
            format_version: WORKER_COMPUTE_RECORD_FORMAT_VERSION,
            key: controller_key.clone(),
            namespace_name: "namespace".to_owned(),
            revision: 0,
            lifecycle: WorkerComputeControllerLifecycle::Inactive,
            slot: None,
            owner: None,
            owner_epoch: 0,
            lease_until: None,
            groups: BTreeMap::from([(
                ScalingGroupId("group".to_owned()),
                group_state(fingerprint),
            )]),
            next_metrics_poll_at: None,
            reconciled_at: now,
        };
        assert!(matches!(
            repository
                .admit_controller(candidate, WORKER_COMPUTE_NAMESPACE_SLOT_LIMIT, now,)
                .await
                .expect("controller admission"),
            WorkerComputeControllerAdmission::Admitted(_),
        ));
        let controller_owner = IncarnationId::new();
        let claimed = repository
            .claim_controller(
                &controller_key,
                controller_owner,
                now,
                now + time::Duration::hours(1),
            )
            .await
            .expect("controller claim")
            .expect("active controller");
        let action = build_provider_action(ProviderActionInput {
            action_id: Uuid::new_v4(),
            controller_key: controller_key.clone(),
            namespace_name: "namespace".to_owned(),
            scaling_group: ScalingGroupId("group".to_owned()),
            fingerprint,
            provider: RemoteNexusProvider {
                provider_type: "remote".to_owned(),
                details: None,
                nexus_endpoint: "provider".to_owned(),
            },
            reason: WorkerComputeInvokeReason::NoSyncMatch,
            task_queues: Vec::new(),
            now,
        })
        .expect("provider action");
        let mut next = claimed.record.clone();
        next.revision = next.revision.saturating_add(1);
        next.groups
            .get_mut(&ScalingGroupId("group".to_owned()))
            .expect("fixture group")
            .last_action_id = Some(action.action_id);
        assert_eq!(
            repository
                .commit_decision(
                    &claimed.claim,
                    claimed.record.revision,
                    next,
                    Some(action.clone()),
                )
                .await
                .expect("action commit"),
            WorkerComputeControllerCommitResult::Applied,
        );
        SeededAction {
            namespace_id,
            controller_key,
            controller_owner,
            action,
        }
    }

    async fn commit_followup_action(
        repository: &InMemoryWorkerComputeRepository,
        seeded: &SeededAction,
        now: OffsetDateTime,
    ) -> tokeira_storage::WorkerComputeProviderAction {
        let claimed = repository
            .claim_controller(
                &seeded.controller_key,
                seeded.controller_owner,
                now,
                now + time::Duration::hours(1),
            )
            .await
            .expect("controller claim")
            .expect("active controller");
        let fingerprint = claimed
            .record
            .groups
            .get(&ScalingGroupId("group".to_owned()))
            .expect("fixture group")
            .fingerprint;
        let action = build_provider_action(ProviderActionInput {
            action_id: Uuid::new_v4(),
            controller_key: seeded.controller_key.clone(),
            namespace_name: "namespace".to_owned(),
            scaling_group: ScalingGroupId("group".to_owned()),
            fingerprint,
            provider: RemoteNexusProvider {
                provider_type: "remote".to_owned(),
                details: None,
                nexus_endpoint: "provider".to_owned(),
            },
            reason: WorkerComputeInvokeReason::WorkerRefresh,
            task_queues: Vec::new(),
            now,
        })
        .expect("follow-up action");
        let mut next = claimed.record.clone();
        next.revision = next.revision.saturating_add(1);
        next.groups
            .get_mut(&ScalingGroupId("group".to_owned()))
            .expect("fixture group")
            .last_action_id = Some(action.action_id);
        assert_eq!(
            repository
                .commit_decision(
                    &claimed.claim,
                    claimed.record.revision,
                    next,
                    Some(action.clone()),
                )
                .await
                .expect("follow-up action commit"),
            WorkerComputeControllerCommitResult::Applied,
        );
        action
    }

    #[test]
    fn retry_delay_is_one_second_exponential_and_hour_capped() {
        assert_eq!(action_retry_delay(0), std::time::Duration::from_secs(1));
        assert_eq!(action_retry_delay(1), std::time::Duration::from_secs(1));
        assert_eq!(action_retry_delay(2), std::time::Duration::from_secs(2));
        assert_eq!(action_retry_delay(12), std::time::Duration::from_secs(2048));
        assert_eq!(action_retry_delay(13), std::time::Duration::from_secs(3600));
        assert_eq!(
            action_retry_delay(u64::MAX),
            std::time::Duration::from_secs(3600)
        );
    }

    #[tokio::test]
    async fn stale_pending_action_is_superseded_before_provider_io() {
        let repository = Arc::new(InMemoryWorkerComputeRepository::default());
        let now = OffsetDateTime::now_utc();
        let seeded = seed_action(&repository, now).await;
        let claimed = repository
            .claim_controller(
                &seeded.controller_key,
                seeded.controller_owner,
                now,
                now + time::Duration::hours(1),
            )
            .await
            .expect("controller claim")
            .expect("controller");
        let mut next = claimed.record.clone();
        next.revision = next.revision.saturating_add(1);
        next.groups
            .get_mut(&ScalingGroupId("group".to_owned()))
            .expect("fixture group")
            .fingerprint = ConfigurationFingerprint::from_canonical_bytes(b"changed");
        assert_eq!(
            repository
                .commit_decision(&claimed.claim, claimed.record.revision, next, None,)
                .await
                .expect("configuration change"),
            WorkerComputeControllerCommitResult::Applied,
        );

        let provider = Arc::new(ScriptedProvider::new([
            WorkerComputeProviderOutcome::Delivered,
        ]));
        let outbox = WorkerComputeOutbox::new(
            repository,
            provider.clone(),
            Arc::new(TestClock::new(now)),
            IncarnationId::new(),
        );
        assert_eq!(
            outbox
                .deliver_namespace_once(seeded.namespace_id)
                .await
                .expect("delivery sweep"),
            WorkerComputeOutboxSweep {
                claimed: 1,
                superseded: 1,
                ..WorkerComputeOutboxSweep::default()
            },
        );
        assert!(provider.calls().is_empty());
    }

    #[derive(Debug)]
    struct FingerprintMutatingProvider {
        repository: Arc<InMemoryWorkerComputeRepository>,
        controller_key: ControllerInstanceKey,
        controller_owner: IncarnationId,
    }

    #[async_trait]
    impl WorkerComputeProvider for FingerprintMutatingProvider {
        async fn deliver(
            &self,
            _action: &tokeira_storage::WorkerComputeProviderAction,
            _claim_epoch: u64,
            now: OffsetDateTime,
        ) -> WorkerComputeProviderAttempt {
            let claimed = self
                .repository
                .claim_controller(
                    &self.controller_key,
                    self.controller_owner,
                    now,
                    now + time::Duration::hours(1),
                )
                .await
                .expect("controller claim")
                .expect("active controller");
            let mut next = claimed.record.clone();
            next.revision = next.revision.saturating_add(1);
            next.groups
                .get_mut(&ScalingGroupId("group".to_owned()))
                .expect("fixture group")
                .fingerprint = ConfigurationFingerprint::from_canonical_bytes(b"in-flight-change");
            assert_eq!(
                self.repository
                    .commit_decision(&claimed.claim, claimed.record.revision, next, None,)
                    .await
                    .expect("configuration change"),
                WorkerComputeControllerCommitResult::Applied,
            );
            WorkerComputeProviderAttempt {
                target_kind: WorkerComputeProviderTargetKind::External,
                outcome: WorkerComputeProviderOutcome::RetryableFailure(
                    WorkerComputeFailureCategory::Transport,
                ),
            }
        }
    }

    #[tokio::test]
    async fn in_flight_failure_after_configuration_change_is_audit_only() {
        let repository = Arc::new(InMemoryWorkerComputeRepository::default());
        let now = OffsetDateTime::now_utc();
        let seeded = seed_action(&repository, now).await;
        let outbox = WorkerComputeOutbox::new(
            repository.clone(),
            Arc::new(FingerprintMutatingProvider {
                repository: repository.clone(),
                controller_key: seeded.controller_key.clone(),
                controller_owner: seeded.controller_owner,
            }),
            Arc::new(TestClock::new(now)),
            IncarnationId::new(),
        );
        assert_eq!(
            outbox
                .deliver_namespace_once(seeded.namespace_id)
                .await
                .expect("delivery sweep"),
            WorkerComputeOutboxSweep {
                claimed: 1,
                attempted: 1,
                superseded: 1,
                ..WorkerComputeOutboxSweep::default()
            },
        );
        let health = repository
            .list_health(seeded.namespace_id, WorkerComputeHealthFilter::default())
            .await
            .expect("health");
        assert_eq!(health[0].health, WorkerComputeHealth::Active);
        assert_eq!(health[0].last_failure_category, None);
        assert!(
            repository
                .claim_due_actions(
                    seeded.namespace_id,
                    IncarnationId::new(),
                    now,
                    now + time::Duration::minutes(1),
                    1,
                )
                .await
                .expect("due actions")
                .is_empty(),
            "a stale failure must not return to Pending",
        );
    }

    #[tokio::test]
    async fn terminal_failure_does_not_block_a_later_action() {
        let repository = Arc::new(InMemoryWorkerComputeRepository::default());
        let now = OffsetDateTime::now_utc();
        let seeded = seed_action(&repository, now).await;
        let provider = Arc::new(ScriptedProvider::new([
            WorkerComputeProviderOutcome::TerminalFailure(
                WorkerComputeFailureCategory::NonRetryableHandler,
            ),
            WorkerComputeProviderOutcome::Delivered,
        ]));
        let outbox = WorkerComputeOutbox::new(
            repository.clone(),
            provider.clone(),
            Arc::new(TestClock::new(now)),
            IncarnationId::new(),
        );
        assert_eq!(
            outbox
                .deliver_namespace_once(seeded.namespace_id)
                .await
                .expect("terminal sweep")
                .terminal_failed,
            1,
        );
        let health = repository
            .list_health(seeded.namespace_id, WorkerComputeHealthFilter::default())
            .await
            .expect("terminal health");
        assert_eq!(
            health[0].health,
            WorkerComputeHealth::DeliveryTerminalFailure,
        );

        let followup = commit_followup_action(&repository, &seeded, now).await;
        assert_ne!(followup.action_id, seeded.action.action_id);
        assert_eq!(
            outbox
                .deliver_namespace_once(seeded.namespace_id)
                .await
                .expect("follow-up sweep")
                .delivered,
            1,
        );
        let health = repository
            .list_health(seeded.namespace_id, WorkerComputeHealthFilter::default())
            .await
            .expect("recovered health");
        assert_eq!(health[0].health, WorkerComputeHealth::Active);
        assert_eq!(health[0].last_action_id, Some(followup.action_id));
    }

    #[derive(Debug)]
    struct BlockingProvider {
        entered: Arc<Notify>,
        release: Arc<Notify>,
    }

    #[async_trait]
    impl WorkerComputeProvider for BlockingProvider {
        async fn deliver(
            &self,
            _action: &tokeira_storage::WorkerComputeProviderAction,
            _claim_epoch: u64,
            _now: OffsetDateTime,
        ) -> WorkerComputeProviderAttempt {
            self.entered.notify_one();
            self.release.notified().await;
            WorkerComputeProviderAttempt {
                target_kind: WorkerComputeProviderTargetKind::External,
                outcome: WorkerComputeProviderOutcome::Delivered,
            }
        }
    }

    #[tokio::test]
    async fn shutdown_stops_new_claims_but_finishes_the_current_attempt() {
        let repository = Arc::new(InMemoryWorkerComputeRepository::default());
        let now = OffsetDateTime::now_utc();
        let seeded = seed_action(&repository, now).await;
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let outbox = WorkerComputeOutbox::new(
            repository.clone(),
            Arc::new(BlockingProvider {
                entered: entered.clone(),
                release: release.clone(),
            }),
            Arc::new(TestClock::new(now)),
            IncarnationId::new(),
        );
        let shutdown = CancellationToken::new();
        let running = {
            let shutdown = shutdown.clone();
            tokio::spawn(async move { outbox.run_namespace(seeded.namespace_id, shutdown).await })
        };
        entered.notified().await;
        shutdown.cancel();
        release.notify_one();
        running
            .await
            .expect("outbox task")
            .expect("clean outbox shutdown");
        let health = repository
            .list_health(seeded.namespace_id, WorkerComputeHealthFilter::default())
            .await
            .expect("health");
        assert_eq!(health[0].health, WorkerComputeHealth::Active);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        // Feature: worker-compute-controller, Property 14: retry preserves action identity and isolates delivery
        #[test]
        fn property_retry_preserves_action_identity_and_isolates_delivery(
            retryable_failures in 0usize..8,
            abandon_first_claim in any::<bool>(),
        ) {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime");
            runtime.block_on(async {
                let repository = Arc::new(InMemoryWorkerComputeRepository::default());
                let now = OffsetDateTime::now_utc();
                let clock = Arc::new(TestClock::new(now));
                let seeded = seed_action(&repository, now).await;
                let stale_claim = if abandon_first_claim {
                    let claim = repository
                        .claim_due_actions(
                            seeded.namespace_id,
                            IncarnationId::new(),
                            now,
                            now + time::Duration::seconds(1),
                            1,
                        )
                        .await
                        .expect("abandoned claim")
                        .pop()
                        .expect("pending action")
                        .claim;
                    clock.advance(std::time::Duration::from_secs(2));
                    Some(claim)
                } else {
                    None
                };
                let provider = Arc::new(ScriptedProvider::new(
                    std::iter::repeat_n(
                        WorkerComputeProviderOutcome::RetryableFailure(
                            WorkerComputeFailureCategory::Transport,
                        ),
                        retryable_failures,
                    )
                    .chain(std::iter::once(WorkerComputeProviderOutcome::Delivered)),
                ));
                let outbox = WorkerComputeOutbox::new(
                    repository.clone(),
                    provider.clone(),
                    clock.clone(),
                    IncarnationId::new(),
                );

                for attempt in 1..=retryable_failures + 1 {
                    let sweep = outbox
                        .deliver_namespace_once(seeded.namespace_id)
                        .await
                        .expect("delivery sweep");
                    prop_assert_eq!(sweep.claimed, 1);
                    prop_assert_eq!(sweep.attempted, 1);
                    if attempt <= retryable_failures {
                        prop_assert_eq!(sweep.retrying, 1);
                        clock.advance(action_retry_delay(
                            u64::try_from(attempt).expect("small generated attempt"),
                        ));
                    } else {
                        prop_assert_eq!(sweep.delivered, 1);
                    }
                }

                let calls = provider.calls();
                prop_assert_eq!(calls.len(), retryable_failures + 1);
                let identity_is_stable = calls.iter().all(|call| {
                    call.action_id == seeded.action.action_id
                        && call.request_data == seeded.action.request_data
                });
                prop_assert!(identity_is_stable);
                let epochs_increase = calls
                    .windows(2)
                    .all(|pair| pair[0].claim_epoch < pair[1].claim_epoch);
                prop_assert!(epochs_increase);
                prop_assert_eq!(
                    outbox
                        .deliver_namespace_once(seeded.namespace_id)
                        .await
                        .expect("delivered action is not due"),
                    WorkerComputeOutboxSweep::default(),
                );
                if let Some(stale_claim) = stale_claim {
                    prop_assert_eq!(
                        repository
                            .finalize_action(
                                &stale_claim,
                                WorkerComputeActionFinalization::Delivered {
                                    completed_at: clock.now(),
                                },
                            )
                            .await
                            .expect("stale finalization"),
                        WorkerComputeActionFinalizeResult::StaleClaim,
                    );
                }
                Ok::<(), TestCaseError>(())
            })?;
        }
    }
}
