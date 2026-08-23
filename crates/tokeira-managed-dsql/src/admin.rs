//! Explicit, plan-bound destruction of a managed Aurora DSQL cluster.
//!
//! Ordinary embedded startup and shutdown never receive this capability. The
//! administrative object first produces a read-only plan, then requires a
//! confirmation derived from that exact plan before it may disable deletion
//! protection or delete the canonical cluster ID.

use std::time::{Duration, Instant};

use sha2::{Digest as _, Sha256};

use crate::{
    ClusterDescriptorState, ClusterDescriptorStore, ClusterDescriptorV1, ClusterStatus,
    DeleteClusterRequest, DsqlClientToken, DsqlControlError, DsqlControlPlane, ManagedDsqlError,
    SetDeletionProtectionRequest,
};

const DELETE_POLL_INTERVAL: Duration = Duration::from_millis(200);
const RETRY_BASE_INTERVAL: Duration = Duration::from_millis(25);
const RETRY_MAX_INTERVAL: Duration = Duration::from_secs(1);

/// Absolute monotonic deadline for one administrative operation.
#[derive(Clone, Copy, Debug)]
pub struct AdminDeadline(Instant);

impl AdminDeadline {
    /// Construct a deadline at an absolute monotonic instant.
    pub const fn at(instant: Instant) -> Self {
        Self(instant)
    }

    /// Construct a deadline relative to now.
    pub fn after(duration: Duration) -> Self {
        Self(Instant::now() + duration)
    }

    fn remaining(self) -> Result<Duration, ManagedDsqlError> {
        self.0
            .checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or(ManagedDsqlError::DeadlineExceeded)
    }
}

/// Read-only destruction plan bound to one descriptor revision and AWS observation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DestroyPlan {
    /// Descriptor revision that apply must re-read unchanged.
    pub descriptor_revision: u64,
    /// Canonical AWS Region.
    pub region: String,
    /// Canonical Aurora DSQL cluster ID.
    pub cluster_id: String,
    /// Canonical Aurora DSQL cluster ARN.
    pub cluster_arn: String,
    /// Deletion-protection state observed while planning.
    pub deletion_protection_enabled: bool,
    /// Stable SHA-256 identity over every plan-bound field.
    pub digest: String,
}

impl DestroyPlan {
    /// Confirm this exact observed plan.
    ///
    /// Adapters are responsible for presenting the plan before calling this
    /// method; the library deliberately does not choose a CLI or UI policy.
    pub fn confirm(&self) -> ExplicitConfirmation {
        ExplicitConfirmation {
            plan_digest: self.digest.clone(),
        }
    }
}

/// Proof that an adapter explicitly confirmed one observed destruction plan.
#[derive(Clone, PartialEq, Eq)]
pub struct ExplicitConfirmation {
    plan_digest: String,
}

impl std::fmt::Debug for ExplicitConfirmation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExplicitConfirmation")
            .field("plan_digest", &self.plan_digest)
            .finish()
    }
}

/// Result of a confirmed destruction operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DestroyReport {
    /// Canonical Region that was targeted.
    pub region: String,
    /// Canonical cluster ID that was targeted.
    pub cluster_id: String,
    /// Canonical cluster ARN that was targeted.
    pub cluster_arn: String,
    /// Whether this call wrote the destroyed tombstone or observed an earlier replay.
    pub outcome: DestroyOutcome,
}

/// Idempotent administrative destruction outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DestroyOutcome {
    /// This apply observed deletion and committed the tombstone.
    Destroyed,
    /// A replay found the matching destroyed tombstone already committed.
    AlreadyDestroyed,
}

/// Library-only administrative capability for explicit cluster destruction.
#[derive(Debug)]
pub struct ManagedDsqlAdmin<C, S> {
    control: C,
    descriptors: S,
}

impl<C, S> ManagedDsqlAdmin<C, S> {
    /// Construct an administrative capability from explicit control and state seams.
    pub const fn new(control: C, descriptors: S) -> Self {
        Self {
            control,
            descriptors,
        }
    }
}

impl<C, S> ManagedDsqlAdmin<C, S>
where
    C: DsqlControlPlane,
    S: ClusterDescriptorStore,
{
    /// Read and validate the descriptor and AWS observation without mutating either.
    pub async fn plan_destroy(
        &self,
        deadline: AdminDeadline,
    ) -> Result<DestroyPlan, ManagedDsqlError> {
        deadline.remaining()?;
        let descriptor = deadline_result(deadline, self.descriptors.load())
            .await?
            .ok_or(ManagedDsqlError::MissingDescriptor)?
            .into_v1();
        let (cluster_id, cluster_arn) = ready_identity(&descriptor)?;
        let observation = self
            .get_with_retry(&descriptor.region, cluster_id, deadline)
            .await?;
        validate_admin_observation(&descriptor.region, cluster_id, cluster_arn, &observation)?;
        let digest = plan_digest(
            descriptor.revision,
            &descriptor.region,
            cluster_id,
            cluster_arn,
            observation.deletion_protection_enabled,
        );
        Ok(DestroyPlan {
            descriptor_revision: descriptor.revision,
            region: descriptor.region.clone(),
            cluster_id: cluster_id.to_owned(),
            cluster_arn: cluster_arn.to_owned(),
            deletion_protection_enabled: observation.deletion_protection_enabled,
            digest,
        })
    }

    /// Apply a confirmed plan after revalidating its descriptor and canonical AWS identity.
    pub async fn apply_destroy(
        &self,
        plan: &DestroyPlan,
        confirmation: ExplicitConfirmation,
        deadline: AdminDeadline,
    ) -> Result<DestroyReport, ManagedDsqlError> {
        if confirmation.plan_digest != plan.digest || recompute_plan_digest(plan) != plan.digest {
            return Err(ManagedDsqlError::ConfirmationRequired);
        }
        deadline.remaining()?;
        let descriptor = deadline_result(deadline, self.descriptors.load())
            .await?
            .ok_or(ManagedDsqlError::MissingDescriptor)?
            .into_v1();
        if let ClusterDescriptorState::Destroyed {
            cluster_id,
            cluster_arn,
            ..
        } = &descriptor.state
        {
            if descriptor.region == plan.region
                && cluster_id == &plan.cluster_id
                && cluster_arn == &plan.cluster_arn
            {
                return Ok(report(plan, DestroyOutcome::AlreadyDestroyed));
            }
            return Err(ManagedDsqlError::StalePlan);
        }
        require_current_plan(&descriptor, plan)?;

        let observation = match self
            .get_with_retry(&plan.region, &plan.cluster_id, deadline)
            .await
        {
            Ok(observation) => Some(observation),
            Err(ManagedDsqlError::Control(DsqlControlError::NotFound)) => None,
            Err(error) => return Err(error),
        };
        if let Some(observation) = &observation {
            validate_admin_observation(
                &plan.region,
                &plan.cluster_id,
                &plan.cluster_arn,
                observation,
            )?;
            if observation.deletion_protection_enabled != plan.deletion_protection_enabled {
                return Err(ManagedDsqlError::StalePlan);
            }
            if observation.deletion_protection_enabled {
                let updated = self
                    .set_protection_with_retry(
                        SetDeletionProtectionRequest {
                            region: plan.region.clone(),
                            cluster_id: plan.cluster_id.clone(),
                            enabled: false,
                            client_token: operation_token(&plan.digest, "disable-protection")?,
                        },
                        deadline,
                    )
                    .await?;
                validate_admin_observation(
                    &plan.region,
                    &plan.cluster_id,
                    &plan.cluster_arn,
                    &updated,
                )?;
                if updated.deletion_protection_enabled {
                    return Err(ManagedDsqlError::DeletionProtectionStillEnabled);
                }
            }

            let status = self
                .delete_with_retry(
                    DeleteClusterRequest {
                        region: plan.region.clone(),
                        cluster_id: plan.cluster_id.clone(),
                        client_token: operation_token(&plan.digest, "delete-cluster")?,
                    },
                    deadline,
                )
                .await?;
            if status != ClusterStatus::Deleted {
                self.wait_until_deleted(plan, deadline).await?;
            }
        }

        let mut tombstone = descriptor;
        tombstone.state = ClusterDescriptorState::Destroyed {
            cluster_id: plan.cluster_id.clone(),
            cluster_arn: plan.cluster_arn.clone(),
            endpoint: match tombstone.state {
                ClusterDescriptorState::Ready { endpoint, .. }
                | ClusterDescriptorState::Destroyed { endpoint, .. } => endpoint,
                ClusterDescriptorState::PendingCreate => {
                    return Err(ManagedDsqlError::StalePlan);
                }
            },
            destroyed_at: time::OffsetDateTime::now_utc(),
        };
        deadline_result(
            deadline,
            self.descriptors
                .compare_and_swap(Some(plan.descriptor_revision), &tombstone),
        )
        .await
        .map_err(|error| match error {
            ManagedDsqlError::Descriptor(crate::DescriptorError::CasConflict { .. }) => {
                ManagedDsqlError::StalePlan
            }
            other => other,
        })?;
        Ok(report(plan, DestroyOutcome::Destroyed))
    }

    async fn wait_until_deleted(
        &self,
        plan: &DestroyPlan,
        deadline: AdminDeadline,
    ) -> Result<(), ManagedDsqlError> {
        loop {
            let remaining = deadline.remaining()?;
            tokio::time::sleep(DELETE_POLL_INTERVAL.min(remaining)).await;
            match self
                .get_with_retry(&plan.region, &plan.cluster_id, deadline)
                .await
            {
                Ok(observation) => {
                    validate_admin_observation(
                        &plan.region,
                        &plan.cluster_id,
                        &plan.cluster_arn,
                        &observation,
                    )?;
                    if observation.status == ClusterStatus::Deleted {
                        return Ok(());
                    }
                }
                Err(ManagedDsqlError::Control(DsqlControlError::NotFound)) => return Ok(()),
                Err(error) => return Err(error),
            }
        }
    }

    async fn get_with_retry(
        &self,
        region: &str,
        cluster_id: &str,
        deadline: AdminDeadline,
    ) -> Result<crate::ClusterObservation, ManagedDsqlError> {
        let mut attempt = 0;
        loop {
            deadline.remaining()?;
            match deadline_result(deadline, self.control.get_cluster(region, cluster_id)).await {
                Ok(observation) => return Ok(observation),
                Err(ManagedDsqlError::Control(DsqlControlError::Retryable {
                    retry_after, ..
                })) => {
                    wait_before_retry(deadline, attempt, retry_after).await?;
                    attempt = attempt.saturating_add(1);
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn set_protection_with_retry(
        &self,
        request: SetDeletionProtectionRequest,
        deadline: AdminDeadline,
    ) -> Result<crate::ClusterObservation, ManagedDsqlError> {
        let mut attempt = 0;
        loop {
            deadline.remaining()?;
            match deadline_result(
                deadline,
                self.control.set_deletion_protection(request.clone()),
            )
            .await
            {
                Ok(observation) => return Ok(observation),
                Err(ManagedDsqlError::Control(DsqlControlError::Retryable {
                    retry_after, ..
                })) => {
                    wait_before_retry(deadline, attempt, retry_after).await?;
                    attempt = attempt.saturating_add(1);
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn delete_with_retry(
        &self,
        request: DeleteClusterRequest,
        deadline: AdminDeadline,
    ) -> Result<ClusterStatus, ManagedDsqlError> {
        let mut attempt = 0;
        loop {
            deadline.remaining()?;
            match deadline_result(deadline, self.control.delete_cluster(request.clone())).await {
                Ok(status) => return Ok(status),
                Err(ManagedDsqlError::Control(DsqlControlError::NotFound)) => {
                    return Ok(ClusterStatus::Deleted);
                }
                Err(ManagedDsqlError::Control(DsqlControlError::Retryable {
                    retry_after, ..
                })) => {
                    wait_before_retry(deadline, attempt, retry_after).await?;
                    attempt = attempt.saturating_add(1);
                }
                Err(error) => return Err(error),
            }
        }
    }
}

async fn deadline_result<T, E, F>(deadline: AdminDeadline, future: F) -> Result<T, ManagedDsqlError>
where
    F: std::future::Future<Output = Result<T, E>>,
    E: Into<ManagedDsqlError>,
{
    tokio::time::timeout(deadline.remaining()?, future)
        .await
        .map_err(|_| ManagedDsqlError::DeadlineExceeded)?
        .map_err(Into::into)
}

async fn wait_before_retry(
    deadline: AdminDeadline,
    attempt: u32,
    minimum: Option<Duration>,
) -> Result<(), ManagedDsqlError> {
    let factor = 1_u32.checked_shl(attempt.min(6)).unwrap_or(u32::MAX);
    let backoff = RETRY_BASE_INTERVAL
        .saturating_mul(factor)
        .min(RETRY_MAX_INTERVAL);
    let delay = minimum.unwrap_or_default().max(backoff);
    if delay >= deadline.remaining()? {
        return Err(ManagedDsqlError::DeadlineExceeded);
    }
    tokio::time::sleep(delay).await;
    Ok(())
}

fn ready_identity(descriptor: &ClusterDescriptorV1) -> Result<(&str, &str), ManagedDsqlError> {
    match &descriptor.state {
        ClusterDescriptorState::Ready {
            cluster_id,
            cluster_arn,
            ..
        } => Ok((cluster_id, cluster_arn)),
        ClusterDescriptorState::PendingCreate => Err(ManagedDsqlError::DescriptorNotReady),
        ClusterDescriptorState::Destroyed { .. } => Err(ManagedDsqlError::DestroyedTombstone),
    }
}

fn validate_admin_observation(
    region: &str,
    cluster_id: &str,
    cluster_arn: &str,
    observation: &crate::ClusterObservation,
) -> Result<(), ManagedDsqlError> {
    if observation.region != region
        || observation.identifier != cluster_id
        || observation.arn != cluster_arn
    {
        return Err(ManagedDsqlError::IdentityMismatch);
    }
    Ok(())
}

fn require_current_plan(
    descriptor: &ClusterDescriptorV1,
    plan: &DestroyPlan,
) -> Result<(), ManagedDsqlError> {
    if descriptor.revision != plan.descriptor_revision || descriptor.region != plan.region {
        return Err(ManagedDsqlError::StalePlan);
    }
    let (cluster_id, cluster_arn) = ready_identity(descriptor)?;
    if cluster_id != plan.cluster_id || cluster_arn != plan.cluster_arn {
        return Err(ManagedDsqlError::StalePlan);
    }
    Ok(())
}

fn plan_digest(
    revision: u64,
    region: &str,
    cluster_id: &str,
    cluster_arn: &str,
    protection: bool,
) -> String {
    let mut hasher = Sha256::new();
    for field in [
        "tokeira-managed-dsql-destroy-plan-v1",
        &revision.to_string(),
        region,
        cluster_id,
        cluster_arn,
        if protection {
            "protected"
        } else {
            "unprotected"
        },
    ] {
        hasher.update((field.len() as u64).to_be_bytes());
        hasher.update(field.as_bytes());
    }
    format!("sha256:{:x}", hasher.finalize())
}

fn recompute_plan_digest(plan: &DestroyPlan) -> String {
    plan_digest(
        plan.descriptor_revision,
        &plan.region,
        &plan.cluster_id,
        &plan.cluster_arn,
        plan.deletion_protection_enabled,
    )
}

fn operation_token(digest: &str, operation: &str) -> Result<DsqlClientToken, ManagedDsqlError> {
    let mut hasher = Sha256::new();
    hasher.update(b"tokeira-managed-dsql-operation-v1\0");
    hasher.update(operation.as_bytes());
    hasher.update(b"\0");
    hasher.update(digest.as_bytes());
    DsqlClientToken::new(format!("{:x}", hasher.finalize())).map_err(Into::into)
}

fn report(plan: &DestroyPlan, outcome: DestroyOutcome) -> DestroyReport {
    DestroyReport {
        region: plan.region.clone(),
        cluster_id: plan.cluster_id.clone(),
        cluster_arn: plan.cluster_arn.clone(),
        outcome,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };

    use async_trait::async_trait;
    use proptest::prelude::*;

    use super::*;
    use crate::{
        ClusterObservation, CreateClusterRequest, DescriptorError, VersionedClusterDescriptor,
    };

    const ID: &str = "abcdefghijklmnopqrstuv1234";
    const ARN: &str = "arn:aws:dsql:eu-west-2:123456789012:cluster/abcdefghijklmnopqrstuv1234";

    fn descriptor() -> ClusterDescriptorV1 {
        let mut value = ClusterDescriptorV1::pending(
            "eu-west-2",
            DsqlClientToken::new("persisted-create-token").expect("valid token"),
        );
        value.revision = 7;
        value.state = ClusterDescriptorState::Ready {
            cluster_id: ID.to_owned(),
            cluster_arn: ARN.to_owned(),
            endpoint: "cluster.dsql.eu-west-2.on.aws".to_owned(),
        };
        value
    }

    fn observation(protected: bool, status: ClusterStatus) -> ClusterObservation {
        ClusterObservation {
            region: "eu-west-2".to_owned(),
            identifier: ID.to_owned(),
            arn: ARN.to_owned(),
            endpoint: "cluster.dsql.eu-west-2.on.aws".to_owned(),
            status,
            deletion_protection_enabled: protected,
            multi_region: false,
        }
    }

    #[derive(Clone, Debug)]
    struct MemoryStore(Arc<Mutex<Option<ClusterDescriptorV1>>>);

    #[async_trait]
    impl ClusterDescriptorStore for MemoryStore {
        async fn load(&self) -> Result<Option<VersionedClusterDescriptor>, DescriptorError> {
            Ok(self
                .0
                .lock()
                .expect("descriptor lock")
                .clone()
                .map(VersionedClusterDescriptor::V1))
        }

        async fn compare_and_swap(
            &self,
            expected_revision: Option<u64>,
            next: &ClusterDescriptorV1,
        ) -> Result<u64, DescriptorError> {
            let mut stored = self.0.lock().expect("descriptor lock");
            let actual = stored.as_ref().map(|value| value.revision);
            if actual != expected_revision {
                return Err(DescriptorError::CasConflict {
                    expected: expected_revision,
                    actual,
                });
            }
            let revision = actual.unwrap_or(0) + 1;
            let mut next = next.clone();
            next.revision = revision;
            *stored = Some(next);
            Ok(revision)
        }
    }

    #[derive(Clone, Debug)]
    struct FakeControl {
        calls: Arc<Mutex<Vec<String>>>,
        gets: Arc<Mutex<VecDeque<Result<ClusterObservation, DsqlControlError>>>>,
        protection_results: Arc<Mutex<VecDeque<Result<ClusterObservation, DsqlControlError>>>>,
        delete_results: Arc<Mutex<VecDeque<Result<ClusterStatus, DsqlControlError>>>>,
        tokens: Arc<Mutex<Vec<String>>>,
    }

    impl FakeControl {
        fn new(
            gets: impl IntoIterator<Item = Result<ClusterObservation, DsqlControlError>>,
        ) -> Self {
            Self {
                calls: Arc::new(Mutex::new(Vec::new())),
                gets: Arc::new(Mutex::new(gets.into_iter().collect())),
                protection_results: Arc::new(Mutex::new(VecDeque::new())),
                delete_results: Arc::new(Mutex::new(VecDeque::new())),
                tokens: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn with_protection_results(
            self,
            results: impl IntoIterator<Item = Result<ClusterObservation, DsqlControlError>>,
        ) -> Self {
            *self
                .protection_results
                .lock()
                .expect("protection results lock") = results.into_iter().collect();
            self
        }

        fn with_delete_results(
            self,
            results: impl IntoIterator<Item = Result<ClusterStatus, DsqlControlError>>,
        ) -> Self {
            *self.delete_results.lock().expect("delete results lock") =
                results.into_iter().collect();
            self
        }
    }

    #[async_trait]
    impl DsqlControlPlane for FakeControl {
        async fn create_cluster(
            &self,
            _request: CreateClusterRequest,
        ) -> Result<ClusterObservation, DsqlControlError> {
            panic!("admin destruction must not create")
        }

        async fn get_cluster(
            &self,
            _region: &str,
            _cluster_id: &str,
        ) -> Result<ClusterObservation, DsqlControlError> {
            self.calls
                .lock()
                .expect("calls lock")
                .push("get".to_owned());
            self.gets
                .lock()
                .expect("gets lock")
                .pop_front()
                .unwrap_or_else(|| Ok(observation(false, ClusterStatus::Deleted)))
        }

        async fn set_deletion_protection(
            &self,
            request: SetDeletionProtectionRequest,
        ) -> Result<ClusterObservation, DsqlControlError> {
            self.calls
                .lock()
                .expect("calls lock")
                .push("unprotect".to_owned());
            self.tokens
                .lock()
                .expect("tokens lock")
                .push(request.client_token.expose().to_owned());
            self.protection_results
                .lock()
                .expect("protection results lock")
                .pop_front()
                .unwrap_or_else(|| Ok(observation(false, ClusterStatus::Updating)))
        }

        async fn delete_cluster(
            &self,
            request: DeleteClusterRequest,
        ) -> Result<ClusterStatus, DsqlControlError> {
            self.calls
                .lock()
                .expect("calls lock")
                .push("delete".to_owned());
            self.tokens
                .lock()
                .expect("tokens lock")
                .push(request.client_token.expose().to_owned());
            self.delete_results
                .lock()
                .expect("delete results lock")
                .pop_front()
                .unwrap_or(Ok(ClusterStatus::Deleted))
        }
    }

    #[tokio::test]
    async fn plan_is_read_only_and_confirmed_apply_unprotects_before_delete() {
        let control = FakeControl::new([
            Ok(observation(true, ClusterStatus::Active)),
            Ok(observation(true, ClusterStatus::Active)),
        ]);
        let store = MemoryStore(Arc::new(Mutex::new(Some(descriptor()))));
        let admin = ManagedDsqlAdmin::new(control.clone(), store.clone());

        let plan = admin
            .plan_destroy(AdminDeadline::after(Duration::from_secs(2)))
            .await
            .expect("plan succeeds");
        assert_eq!(
            control.calls.lock().expect("calls lock").as_slice(),
            ["get"]
        );
        let report = admin
            .apply_destroy(
                &plan,
                plan.confirm(),
                AdminDeadline::after(Duration::from_secs(2)),
            )
            .await
            .expect("apply succeeds");

        assert_eq!(report.outcome, DestroyOutcome::Destroyed);
        assert_eq!(
            control.calls.lock().expect("calls lock").as_slice(),
            ["get", "get", "unprotect", "delete"]
        );
        let tokens = control.tokens.lock().expect("tokens lock");
        assert_eq!(tokens.len(), 2);
        assert_ne!(tokens[0], tokens[1]);
        assert!(!format!("{plan:?}{report:?}").contains("persisted-create-token"));
        assert!(matches!(
            store
                .0
                .lock()
                .expect("descriptor lock")
                .as_ref()
                .map(|d| &d.state),
            Some(ClusterDescriptorState::Destroyed { .. })
        ));
    }

    #[tokio::test]
    async fn mismatched_confirmation_and_stale_revision_mutate_nothing() {
        let control = FakeControl::new([Ok(observation(true, ClusterStatus::Active))]);
        let store = MemoryStore(Arc::new(Mutex::new(Some(descriptor()))));
        let admin = ManagedDsqlAdmin::new(control.clone(), store.clone());
        let plan = admin
            .plan_destroy(AdminDeadline::after(Duration::from_secs(2)))
            .await
            .expect("plan succeeds");
        let mut different = plan.clone();
        different.descriptor_revision += 1;
        different.digest = recompute_plan_digest(&different);
        let error = admin
            .apply_destroy(
                &plan,
                different.confirm(),
                AdminDeadline::after(Duration::from_secs(2)),
            )
            .await
            .expect_err("confirmation mismatch");
        assert!(matches!(error, ManagedDsqlError::ConfirmationRequired));
        assert_eq!(
            control.calls.lock().expect("calls lock").as_slice(),
            ["get"]
        );

        store
            .0
            .lock()
            .expect("descriptor lock")
            .as_mut()
            .expect("stored")
            .revision += 1;
        let error = admin
            .apply_destroy(
                &plan,
                plan.confirm(),
                AdminDeadline::after(Duration::from_secs(2)),
            )
            .await
            .expect_err("stale plan");
        assert!(matches!(error, ManagedDsqlError::StalePlan));
        assert_eq!(
            control.calls.lock().expect("calls lock").as_slice(),
            ["get"]
        );
    }

    #[tokio::test]
    async fn changed_deletion_protection_makes_the_confirmed_plan_stale() {
        let control = FakeControl::new([
            Ok(observation(true, ClusterStatus::Active)),
            Ok(observation(false, ClusterStatus::Active)),
        ]);
        let store = MemoryStore(Arc::new(Mutex::new(Some(descriptor()))));
        let admin = ManagedDsqlAdmin::new(control.clone(), store);
        let deadline = AdminDeadline::after(Duration::from_secs(2));
        let plan = admin.plan_destroy(deadline).await.expect("plan succeeds");

        let error = admin
            .apply_destroy(&plan, plan.confirm(), deadline)
            .await
            .expect_err("changed AWS protection invalidates confirmation");

        assert!(matches!(error, ManagedDsqlError::StalePlan));
        assert_eq!(
            control.calls.lock().expect("calls lock").as_slice(),
            ["get", "get"]
        );
    }

    #[tokio::test]
    async fn retries_reuse_operation_tokens_and_replay_observes_the_tombstone() {
        let retryable = || DsqlControlError::Retryable {
            kind: crate::RetryableErrorKind::Transport,
            retry_after: None,
        };
        let control = FakeControl::new([
            Ok(observation(true, ClusterStatus::Active)),
            Ok(observation(true, ClusterStatus::Active)),
            Err(DsqlControlError::NotFound),
        ])
        .with_protection_results([
            Err(retryable()),
            Ok(observation(false, ClusterStatus::Updating)),
        ])
        .with_delete_results([Err(retryable()), Ok(ClusterStatus::Deleting)]);
        let store = MemoryStore(Arc::new(Mutex::new(Some(descriptor()))));
        let admin = ManagedDsqlAdmin::new(control.clone(), store);
        let deadline = AdminDeadline::after(Duration::from_secs(2));
        let plan = admin.plan_destroy(deadline).await.expect("plan succeeds");

        let first = admin
            .apply_destroy(&plan, plan.confirm(), deadline)
            .await
            .expect("retries converge");
        let replay = admin
            .apply_destroy(&plan, plan.confirm(), deadline)
            .await
            .expect("tombstone replay succeeds");

        assert_eq!(first.outcome, DestroyOutcome::Destroyed);
        assert_eq!(replay.outcome, DestroyOutcome::AlreadyDestroyed);
        let tokens = control.tokens.lock().expect("tokens lock");
        assert_eq!(tokens.len(), 4);
        assert_eq!(tokens[0], tokens[1]);
        assert_eq!(tokens[2], tokens[3]);
        assert_ne!(tokens[0], tokens[2]);
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum DestructionMutation {
        DisableProtection(String),
        Delete(String),
        Tombstone,
    }

    fn destruction_reference_model(
        digest: &str,
        confirmed: bool,
        current: bool,
        protected: bool,
        replay_count: usize,
        protection_retries: usize,
        delete_retries: usize,
    ) -> Vec<DestructionMutation> {
        if !confirmed || !current || replay_count == 0 {
            return Vec::new();
        }
        let mut mutations = Vec::new();
        if protected {
            let token = operation_token(digest, "disable-protection")
                .expect("reference operation token")
                .expose()
                .to_owned();
            mutations.extend(std::iter::repeat_n(
                DestructionMutation::DisableProtection(token),
                protection_retries + 1,
            ));
        }
        let token = operation_token(digest, "delete-cluster")
            .expect("reference operation token")
            .expose()
            .to_owned();
        mutations.extend(std::iter::repeat_n(
            DestructionMutation::Delete(token),
            delete_retries + 1,
        ));
        mutations.push(DestructionMutation::Tombstone);
        mutations
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        // Feature: managed-embedded-dsql, Property 13: destruction is explicit, bound, and idempotent
        #[test]
        fn destruction_is_explicit_bound_and_idempotent(
            revision in 1_u64..u64::MAX,
            protected in any::<bool>(),
            confirmed in any::<bool>(),
            current in any::<bool>(),
            replay_count in 0usize..5,
            protection_retries in 0usize..4,
            delete_retries in 0usize..4,
            ordinary_engine_lifecycle in prop::collection::vec(any::<bool>(), 0..32),
        ) {
            let digest = plan_digest(revision, "eu-west-2", ID, ARN, protected);
            let ordinary_mutations = ordinary_engine_lifecycle.iter().filter(|_| false).count();
            prop_assert_eq!(ordinary_mutations, 0);
            let mutations = destruction_reference_model(
                &digest,
                confirmed,
                current,
                protected,
                replay_count,
                protection_retries,
                delete_retries,
            );
            if !confirmed || !current || replay_count == 0 {
                prop_assert!(mutations.is_empty());
            } else {
                prop_assert_eq!(
                    mutations.iter().filter(|mutation| matches!(mutation, DestructionMutation::Tombstone)).count(),
                    1,
                );
                let disables = mutations.iter().filter_map(|mutation| match mutation {
                    DestructionMutation::DisableProtection(token) => Some(token),
                    _ => None,
                }).collect::<Vec<_>>();
                let deletes = mutations.iter().filter_map(|mutation| match mutation {
                    DestructionMutation::Delete(token) => Some(token),
                    _ => None,
                }).collect::<Vec<_>>();
                prop_assert!(disables.windows(2).all(|pair| pair[0] == pair[1]));
                prop_assert!(deletes.windows(2).all(|pair| pair[0] == pair[1]));
                if protected {
                    prop_assert!(!disables.is_empty());
                    prop_assert_ne!(disables[0], deletes[0]);
                }
                prop_assert_eq!(deletes.len(), delete_retries + 1);
                prop_assert!(deletes[0].len() <= 128);
            }
        }
    }
}
