//! Idempotent managed-cluster creation and canonical recovery state machine.

use std::{
    cmp,
    collections::BTreeMap,
    fmt,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use thiserror::Error;

use crate::{
    control::{
        ClusterObservation, ClusterStatus, CreateClusterRequest, DsqlControlError, DsqlControlPlane,
    },
    descriptor::{
        ClusterDescriptorState, ClusterDescriptorStore, ClusterDescriptorV1, DescriptorError,
        DsqlClientToken,
    },
    identity::{CanonicalClusterIdentity, IdentityError},
};

/// Absolute startup deadline shared by all managed lifecycle phases.
#[derive(Clone, Copy, Debug)]
pub struct StartupDeadline(Instant);

impl StartupDeadline {
    /// Constructs an absolute deadline.
    pub fn at(instant: Instant) -> Self {
        Self(instant)
    }

    /// Constructs a deadline relative to an injected lifecycle clock.
    pub fn after<T: LifecycleEnvironment>(environment: &T, duration: Duration) -> Self {
        Self(environment.now() + duration)
    }

    /// Returns the remaining duration, or `None` once expired.
    pub fn remaining<T: LifecycleEnvironment>(&self, environment: &T) -> Option<Duration> {
        self.0.checked_duration_since(environment.now())
    }
}

/// Injected monotonic time, sleeping, and token generation.
#[async_trait]
pub trait LifecycleEnvironment: Send + Sync + fmt::Debug {
    /// Returns the current monotonic instant.
    fn now(&self) -> Instant;

    /// Generates one valid candidate client token.
    fn new_client_token(&self) -> Result<DsqlClientToken, DescriptorError>;

    /// Waits or deterministically advances fake time by `duration`.
    async fn sleep(&self, duration: Duration);
}

/// Production lifecycle environment using Tokio and UUID v4 tokens.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemLifecycleEnvironment;

#[async_trait]
impl LifecycleEnvironment for SystemLifecycleEnvironment {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn new_client_token(&self) -> Result<DsqlClientToken, DescriptorError> {
        DsqlClientToken::new(uuid::Uuid::new_v4().to_string())
    }

    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

/// Bounded exponential retry policy with deterministic per-attempt jitter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryPolicy {
    initial: Duration,
    maximum: Duration,
    jitter: bool,
}

impl RetryPolicy {
    /// Creates a retry policy. The maximum must be at least the initial delay.
    pub fn new(
        initial: Duration,
        maximum: Duration,
        jitter: bool,
    ) -> Result<Self, ManagedDsqlError> {
        if initial.is_zero() || maximum < initial {
            return Err(ManagedDsqlError::InvalidRetryPolicy);
        }
        Ok(Self {
            initial,
            maximum,
            jitter,
        })
    }

    fn delay(&self, attempt: u32, minimum: Option<Duration>) -> Duration {
        let exponent = attempt.min(31);
        let factor = 1_u32 << exponent;
        let base = self.initial.saturating_mul(factor).min(self.maximum);
        let jittered = if self.jitter {
            // This deterministic mixing avoids process-global RNG state while ensuring
            // independent startups do not all follow one exact exponential cadence.
            let mixed = u64::from(attempt)
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let percentage = 75 + (mixed % 51);
            base.saturating_mul(percentage as u32) / 100
        } else {
            base
        };
        cmp::max(jittered, minimum.unwrap_or(Duration::ZERO))
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            initial: Duration::from_millis(200),
            maximum: Duration::from_secs(5),
            jitter: true,
        }
    }
}

/// Explicit create-or-recover inputs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateOrRecoverRequest {
    /// Region in which the dedicated cluster is owned.
    pub region: String,
    /// Optional AWS metadata, never queried for recovery.
    pub tags: BTreeMap<String, String>,
}

/// How a cluster was resolved for this startup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClusterAction {
    /// This startup completed or replayed the create operation.
    Created,
    /// This startup recovered a durable managed identity.
    Recovered,
    /// Operator-supplied identity was validated without managed mutation.
    Existing,
}

/// Canonical cluster identity plus its current connection/status observation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedCluster {
    /// Immutable canonical identity.
    pub identity: CanonicalClusterIdentity,
    /// Refreshable connection locator.
    pub endpoint: String,
    /// Most recently observed AWS state.
    pub status: ClusterStatus,
    /// Observed AWS deletion-protection state.
    pub deletion_protection_enabled: bool,
    /// Resolution path used by startup.
    pub action: ClusterAction,
}

/// A cluster proven ACTIVE and therefore allowed to enter schema checks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UsableCluster(ResolvedCluster);

impl UsableCluster {
    /// Borrows the active resolved cluster.
    pub fn resolved(&self) -> &ResolvedCluster {
        &self.0
    }

    /// Consumes the wrapper.
    pub fn into_resolved(self) -> ResolvedCluster {
        self.0
    }
}

/// Result of status recovery before storage-owned scale-to-zero wakeup is wired.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Readiness {
    /// AWS reported ACTIVE; schema checks may proceed.
    Active(UsableCluster),
    /// Storage must attempt a bounded database connection, then call `refresh_after_wake`.
    WakeRequired(ResolvedCluster),
}

/// Managed create/recovery state machine over injected AWS, descriptor, and time seams.
#[derive(Debug)]
pub struct ManagedDsqlLifecycle<C, S, T> {
    control: C,
    descriptors: S,
    time: T,
    retry: RetryPolicy,
}

impl<C, S, T> ManagedDsqlLifecycle<C, S, T> {
    /// Constructs a lifecycle with the production retry policy.
    pub fn new(control: C, descriptors: S, time: T) -> Self {
        Self {
            control,
            descriptors,
            time,
            retry: RetryPolicy::default(),
        }
    }

    /// Replaces the retry policy, primarily for deterministic hosts and tests.
    pub fn with_retry_policy(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }
}

impl<C, S, T> ManagedDsqlLifecycle<C, S, T>
where
    C: DsqlControlPlane,
    S: ClusterDescriptorStore,
    T: LifecycleEnvironment,
{
    /// Creates or recovers a managed cluster without ever discovering by tag or endpoint.
    pub async fn create_or_recover(
        &self,
        request: CreateOrRecoverRequest,
        deadline: StartupDeadline,
    ) -> Result<ResolvedCluster, ManagedDsqlError> {
        loop {
            self.ensure_time(deadline)?;
            let descriptor = match self.descriptors.load().await? {
                Some(value) => value.into_v1(),
                None => {
                    let candidate = ClusterDescriptorV1::pending(
                        &request.region,
                        self.time.new_client_token()?,
                    );
                    match self.descriptors.compare_and_swap(None, &candidate).await {
                        Ok(_) => continue,
                        Err(DescriptorError::CasConflict { .. }) => continue,
                        Err(error) => return Err(error.into()),
                    }
                }
            };
            if descriptor.region != request.region {
                return Err(ManagedDsqlError::DescriptorRegionMismatch {
                    descriptor: descriptor.region,
                    request: request.region,
                });
            }
            match &descriptor.state {
                ClusterDescriptorState::PendingCreate => {
                    let observation = self
                        .create_with_retry(
                            CreateClusterRequest {
                                region: descriptor.region.clone(),
                                client_token: descriptor.creation_client_token.clone(),
                                deletion_protection_enabled: true,
                                tags: request.tags.clone(),
                            },
                            deadline,
                        )
                        .await?;
                    let resolved =
                        validate_observation(&observation, None, ClusterAction::Created)?;
                    let mut ready = descriptor.clone();
                    ready.state = ClusterDescriptorState::Ready {
                        cluster_id: resolved.identity.cluster_id.clone(),
                        cluster_arn: resolved.identity.cluster_arn.clone(),
                        endpoint: resolved.endpoint.clone(),
                    };
                    match self
                        .descriptors
                        .compare_and_swap(Some(descriptor.revision), &ready)
                        .await
                    {
                        Ok(_) => return Ok(resolved),
                        Err(DescriptorError::CasConflict { .. }) => continue,
                        Err(error) => return Err(error.into()),
                    }
                }
                ClusterDescriptorState::Ready {
                    cluster_id,
                    cluster_arn,
                    endpoint,
                } => {
                    let expected =
                        CanonicalClusterIdentity::new(&descriptor.region, cluster_id, cluster_arn)?;
                    let observation = self
                        .get_with_retry(&descriptor.region, cluster_id, deadline)
                        .await?;
                    let resolved = validate_observation(
                        &observation,
                        Some(&expected),
                        ClusterAction::Recovered,
                    )?;
                    if endpoint != &resolved.endpoint {
                        let mut refreshed = descriptor.clone();
                        refreshed.state = ClusterDescriptorState::Ready {
                            cluster_id: cluster_id.clone(),
                            cluster_arn: cluster_arn.clone(),
                            endpoint: resolved.endpoint.clone(),
                        };
                        match self
                            .descriptors
                            .compare_and_swap(Some(descriptor.revision), &refreshed)
                            .await
                        {
                            Ok(_) => {}
                            Err(DescriptorError::CasConflict { .. }) => continue,
                            Err(error) => return Err(error.into()),
                        }
                    }
                    return Ok(resolved);
                }
                ClusterDescriptorState::Destroyed { .. } => {
                    return Err(ManagedDsqlError::DestroyedTombstone);
                }
            }
        }
    }

    /// Validates an operator-supplied cluster without reading or mutating a descriptor.
    pub async fn resolve_existing(
        &self,
        identity: CanonicalClusterIdentity,
        deadline: StartupDeadline,
    ) -> Result<ResolvedCluster, ManagedDsqlError> {
        identity.validate()?;
        let observation = self
            .get_with_retry(&identity.region, &identity.cluster_id, deadline)
            .await?;
        validate_observation(&observation, Some(&identity), ClusterAction::Existing)
    }

    /// Polls transitional statuses, returning a storage wake handoff for IDLE/INACTIVE.
    pub async fn refresh_until_usable(
        &self,
        cluster: ResolvedCluster,
        deadline: StartupDeadline,
    ) -> Result<Readiness, ManagedDsqlError> {
        self.refresh(cluster, deadline, false).await
    }

    /// Polls after storage has attempted the connection that wakes IDLE/INACTIVE DSQL.
    pub async fn refresh_after_wake(
        &self,
        cluster: ResolvedCluster,
        deadline: StartupDeadline,
    ) -> Result<UsableCluster, ManagedDsqlError> {
        match self.refresh(cluster, deadline, true).await? {
            Readiness::Active(cluster) => Ok(cluster),
            Readiness::WakeRequired(_) => Err(ManagedDsqlError::WakeDidNotProgress),
        }
    }

    async fn refresh(
        &self,
        mut cluster: ResolvedCluster,
        deadline: StartupDeadline,
        wake_attempted: bool,
    ) -> Result<Readiness, ManagedDsqlError> {
        let mut attempt = 0;
        loop {
            match status_decision(&cluster.status) {
                StatusDecision::Active => return Ok(Readiness::Active(UsableCluster(cluster))),
                StatusDecision::Wake if !wake_attempted => {
                    return Ok(Readiness::WakeRequired(cluster));
                }
                StatusDecision::Poll | StatusDecision::Wake => {
                    self.wait(attempt, None, deadline).await?;
                    attempt = attempt.saturating_add(1);
                    let observation = self
                        .get_with_retry(
                            &cluster.identity.region,
                            &cluster.identity.cluster_id,
                            deadline,
                        )
                        .await?;
                    cluster = validate_observation(
                        &observation,
                        Some(&cluster.identity),
                        cluster.action,
                    )?;
                }
                StatusDecision::Terminal => {
                    return Err(ManagedDsqlError::UnsupportedStatus {
                        status: cluster.status,
                        identity: cluster.identity,
                    });
                }
            }
        }
    }

    async fn create_with_retry(
        &self,
        request: CreateClusterRequest,
        deadline: StartupDeadline,
    ) -> Result<ClusterObservation, ManagedDsqlError> {
        let mut attempt = 0;
        loop {
            self.ensure_time(deadline)?;
            match self.control.create_cluster(request.clone()).await {
                Ok(observation) => return Ok(observation),
                Err(DsqlControlError::Retryable { retry_after, .. }) => {
                    self.wait(attempt, retry_after, deadline).await?;
                    attempt = attempt.saturating_add(1);
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    async fn get_with_retry(
        &self,
        region: &str,
        cluster_id: &str,
        deadline: StartupDeadline,
    ) -> Result<ClusterObservation, ManagedDsqlError> {
        let mut attempt = 0;
        loop {
            self.ensure_time(deadline)?;
            match self.control.get_cluster(region, cluster_id).await {
                Ok(observation) => return Ok(observation),
                Err(DsqlControlError::Retryable { retry_after, .. }) => {
                    self.wait(attempt, retry_after, deadline).await?;
                    attempt = attempt.saturating_add(1);
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    async fn wait(
        &self,
        attempt: u32,
        minimum: Option<Duration>,
        deadline: StartupDeadline,
    ) -> Result<(), ManagedDsqlError> {
        let delay = self.retry.delay(attempt, minimum);
        let remaining = deadline
            .remaining(&self.time)
            .ok_or(ManagedDsqlError::DeadlineExceeded)?;
        if delay > remaining {
            return Err(ManagedDsqlError::DeadlineExceeded);
        }
        self.time.sleep(delay).await;
        Ok(())
    }

    fn ensure_time(&self, deadline: StartupDeadline) -> Result<(), ManagedDsqlError> {
        if deadline
            .remaining(&self.time)
            .is_some_and(|value| !value.is_zero())
        {
            Ok(())
        } else {
            Err(ManagedDsqlError::DeadlineExceeded)
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StatusDecision {
    Active,
    Poll,
    Wake,
    Terminal,
}

fn status_decision(status: &ClusterStatus) -> StatusDecision {
    match status {
        ClusterStatus::Active => StatusDecision::Active,
        ClusterStatus::Creating | ClusterStatus::Updating => StatusDecision::Poll,
        ClusterStatus::Idle | ClusterStatus::Inactive => StatusDecision::Wake,
        ClusterStatus::Failed
        | ClusterStatus::Deleting
        | ClusterStatus::Deleted
        | ClusterStatus::PendingSetup
        | ClusterStatus::PendingDelete
        | ClusterStatus::Unknown(_) => StatusDecision::Terminal,
    }
}

fn validate_observation(
    observation: &ClusterObservation,
    expected: Option<&CanonicalClusterIdentity>,
    action: ClusterAction,
) -> Result<ResolvedCluster, ManagedDsqlError> {
    let identity = CanonicalClusterIdentity::new(
        &observation.region,
        &observation.identifier,
        &observation.arn,
    )?;
    if expected.is_some_and(|expected| expected != &identity) {
        return Err(ManagedDsqlError::IdentityMismatch);
    }
    if observation.endpoint.is_empty() {
        return Err(ManagedDsqlError::EmptyEndpoint);
    }
    if observation.multi_region {
        return Err(ManagedDsqlError::MultiRegionCluster);
    }
    if action != ClusterAction::Existing && !observation.deletion_protection_enabled {
        return Err(ManagedDsqlError::DeletionProtectionDisabled);
    }
    Ok(ResolvedCluster {
        identity,
        endpoint: observation.endpoint.clone(),
        status: observation.status.clone(),
        deletion_protection_enabled: observation.deletion_protection_enabled,
        action,
    })
}

/// Managed lifecycle failure, with client tokens excluded from every variant.
#[derive(Debug, Error)]
pub enum ManagedDsqlError {
    /// Durable descriptor operation failed.
    #[error(transparent)]
    Descriptor(#[from] DescriptorError),
    /// AWS control-plane operation failed.
    #[error(transparent)]
    Control(#[from] DsqlControlError),
    /// Canonical identity validation failed.
    #[error(transparent)]
    Identity(#[from] IdentityError),
    /// Descriptor and explicit request disagree about Region.
    #[error("managed descriptor Region {descriptor} does not match requested Region {request}")]
    DescriptorRegionMismatch {
        /// Durable descriptor Region.
        descriptor: String,
        /// Explicit request Region.
        request: String,
    },
    /// GetCluster disagreed with the durable/configured canonical identity.
    #[error("Aurora DSQL observation disagrees with canonical cluster identity")]
    IdentityMismatch,
    /// AWS omitted the connection locator.
    #[error("Aurora DSQL observation has no endpoint")]
    EmptyEndpoint,
    /// A multi-Region cluster is outside managed embedded mode.
    #[error("multi-Region Aurora DSQL clusters are unsupported in managed embedded mode")]
    MultiRegionCluster,
    /// Managed startup refuses a cluster whose deletion protection is off.
    #[error("managed Aurora DSQL cluster does not have deletion protection enabled")]
    DeletionProtectionDisabled,
    /// Normal startup found an explicit destruction tombstone.
    #[error(
        "managed Aurora DSQL descriptor is a destroyed tombstone; explicit new create intent is required"
    )]
    DestroyedTombstone,
    /// An administrative operation requires an existing descriptor.
    #[error("managed Aurora DSQL cluster descriptor is missing")]
    MissingDescriptor,
    /// Destruction cannot target a create operation that has not recorded an identity.
    #[error("managed Aurora DSQL cluster descriptor is not ready for destruction")]
    DescriptorNotReady,
    /// The supplied confirmation was not derived from the exact observed plan.
    #[error("explicit confirmation of the current destruction plan is required")]
    ConfirmationRequired,
    /// Descriptor revision or canonical identity changed after planning.
    #[error("managed Aurora DSQL destruction plan is stale; create and confirm a new plan")]
    StalePlan,
    /// AWS still reports protection after the explicit disable operation.
    #[error("Aurora DSQL deletion protection remains enabled")]
    DeletionProtectionStillEnabled,
    /// AWS status cannot safely enter connection/schema work.
    #[error("Aurora DSQL cluster {identity:?} has unsupported startup status {status:?}")]
    UnsupportedStatus {
        /// Observed status.
        status: ClusterStatus,
        /// Canonical identity attached to the failure.
        identity: CanonicalClusterIdentity,
    },
    /// The one shared startup deadline was exhausted.
    #[error("managed Aurora DSQL startup deadline exceeded")]
    DeadlineExceeded,
    /// A wake connection attempt did not move the cluster toward ACTIVE.
    #[error("Aurora DSQL wake attempt did not progress before polling")]
    WakeDidNotProgress,
    /// Retry parameters could not produce a positive bounded schedule.
    #[error("invalid managed Aurora DSQL retry policy")]
    InvalidRetryPolicy,
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashMap, VecDeque},
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    };

    use async_trait::async_trait;
    use proptest::prelude::*;

    use super::{
        ClusterAction, CreateOrRecoverRequest, LifecycleEnvironment, ManagedDsqlError,
        ManagedDsqlLifecycle, Readiness, RetryPolicy, StartupDeadline, StatusDecision,
        status_decision,
    };
    use crate::{
        control::{
            ClusterObservation, ClusterStatus, CreateClusterRequest, DeleteClusterRequest,
            DsqlControlError, DsqlControlPlane, SetDeletionProtectionRequest,
        },
        descriptor::{
            ClusterDescriptorState, ClusterDescriptorStore, ClusterDescriptorV1, DescriptorError,
            DsqlClientToken, VersionedClusterDescriptor,
        },
        identity::CanonicalClusterIdentity,
    };

    const ID: &str = "abcdefghijklmnopqrstuv1234";
    const ARN: &str = "arn:aws:dsql:eu-west-2:123456789012:cluster/abcdefghijklmnopqrstuv1234";

    fn observation(status: ClusterStatus) -> ClusterObservation {
        ClusterObservation {
            region: "eu-west-2".to_owned(),
            identifier: ID.to_owned(),
            arn: ARN.to_owned(),
            endpoint: "cluster.dsql.eu-west-2.on.aws".to_owned(),
            status,
            deletion_protection_enabled: true,
            multi_region: false,
        }
    }

    #[derive(Clone, Debug)]
    struct FakeTime {
        start: Instant,
        elapsed: Arc<Mutex<Duration>>,
        sleeps: Arc<Mutex<Vec<Duration>>>,
    }

    impl FakeTime {
        fn new() -> Self {
            Self {
                start: Instant::now(),
                elapsed: Arc::new(Mutex::new(Duration::ZERO)),
                sleeps: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl LifecycleEnvironment for FakeTime {
        fn now(&self) -> Instant {
            self.start
                + *self
                    .elapsed
                    .lock()
                    .expect("fake clock lock is not poisoned")
        }

        fn new_client_token(&self) -> Result<DsqlClientToken, DescriptorError> {
            DsqlClientToken::new("durable-test-token")
        }

        async fn sleep(&self, duration: Duration) {
            self.sleeps
                .lock()
                .expect("sleep log lock is not poisoned")
                .push(duration);
            *self
                .elapsed
                .lock()
                .expect("fake clock lock is not poisoned") += duration;
        }
    }

    #[derive(Clone, Debug, Default)]
    struct MemoryStore {
        value: Arc<Mutex<Option<ClusterDescriptorV1>>>,
        fail_next_ready: Arc<Mutex<bool>>,
    }

    #[async_trait]
    impl ClusterDescriptorStore for MemoryStore {
        async fn load(&self) -> Result<Option<VersionedClusterDescriptor>, DescriptorError> {
            Ok(self
                .value
                .lock()
                .expect("descriptor lock is not poisoned")
                .clone()
                .map(VersionedClusterDescriptor::V1))
        }

        async fn compare_and_swap(
            &self,
            expected_revision: Option<u64>,
            next: &ClusterDescriptorV1,
        ) -> Result<u64, DescriptorError> {
            if matches!(next.state, ClusterDescriptorState::Ready { .. }) {
                let mut fail = self
                    .fail_next_ready
                    .lock()
                    .expect("failure lock is not poisoned");
                if *fail {
                    *fail = false;
                    return Err(DescriptorError::Io("injected crash boundary".to_owned()));
                }
            }
            let mut value = self.value.lock().expect("descriptor lock is not poisoned");
            let actual = value.as_ref().map(|item| item.revision);
            if actual != expected_revision {
                return Err(DescriptorError::CasConflict {
                    expected: expected_revision,
                    actual,
                });
            }
            let revision = actual.unwrap_or(0) + 1;
            let mut next = next.clone();
            next.revision = revision;
            *value = Some(next);
            Ok(revision)
        }
    }

    #[derive(Clone, Debug)]
    struct FakeControl {
        created: Arc<Mutex<HashMap<String, ClusterObservation>>>,
        gets: Arc<Mutex<VecDeque<Result<ClusterObservation, DsqlControlError>>>>,
    }

    impl FakeControl {
        fn new() -> Self {
            Self {
                created: Arc::new(Mutex::new(HashMap::new())),
                gets: Arc::new(Mutex::new(VecDeque::new())),
            }
        }
    }

    #[async_trait]
    impl DsqlControlPlane for FakeControl {
        async fn create_cluster(
            &self,
            request: CreateClusterRequest,
        ) -> Result<ClusterObservation, DsqlControlError> {
            let mut created = self.created.lock().expect("create lock is not poisoned");
            Ok(created
                .entry(request.client_token.expose().to_owned())
                .or_insert_with(|| observation(ClusterStatus::Creating))
                .clone())
        }

        async fn get_cluster(
            &self,
            _region: &str,
            _cluster_id: &str,
        ) -> Result<ClusterObservation, DsqlControlError> {
            self.gets
                .lock()
                .expect("get lock is not poisoned")
                .pop_front()
                .unwrap_or_else(|| Ok(observation(ClusterStatus::Active)))
        }

        async fn set_deletion_protection(
            &self,
            _request: SetDeletionProtectionRequest,
        ) -> Result<ClusterObservation, DsqlControlError> {
            panic!("startup must not update deletion protection")
        }

        async fn delete_cluster(
            &self,
            _request: DeleteClusterRequest,
        ) -> Result<ClusterStatus, DsqlControlError> {
            panic!("startup must not delete clusters")
        }
    }

    fn ready_descriptor(token: &str) -> ClusterDescriptorV1 {
        ClusterDescriptorV1 {
            format_version: 1,
            revision: 1,
            region: "eu-west-2".to_owned(),
            creation_client_token: DsqlClientToken::new(token).expect("fixture token is valid"),
            state: ClusterDescriptorState::Ready {
                cluster_id: ID.to_owned(),
                cluster_arn: ARN.to_owned(),
                endpoint: "old.dsql.eu-west-2.on.aws".to_owned(),
            },
        }
    }

    #[tokio::test]
    async fn existing_resolution_never_touches_descriptor_or_mutation_apis() {
        let control = FakeControl::new();
        let store = MemoryStore::default();
        let time = FakeTime::new();
        let deadline = StartupDeadline::after(&time, Duration::from_secs(10));
        let lifecycle = ManagedDsqlLifecycle::new(control, store.clone(), time);
        let identity =
            CanonicalClusterIdentity::new("eu-west-2", ID, ARN).expect("fixture identity is valid");
        let result = lifecycle
            .resolve_existing(identity, deadline)
            .await
            .expect("existing cluster resolves");
        assert_eq!(result.action, ClusterAction::Existing);
        assert!(
            store
                .value
                .lock()
                .expect("descriptor lock is not poisoned")
                .is_none()
        );
    }

    #[tokio::test]
    async fn ready_recovery_refreshes_only_the_endpoint() {
        let control = FakeControl::new();
        let store = MemoryStore::default();
        *store.value.lock().expect("descriptor lock is not poisoned") =
            Some(ready_descriptor("durable-test-token"));
        let time = FakeTime::new();
        let deadline = StartupDeadline::after(&time, Duration::from_secs(10));
        let lifecycle = ManagedDsqlLifecycle::new(control.clone(), store.clone(), time);
        let resolved = lifecycle
            .create_or_recover(
                CreateOrRecoverRequest {
                    region: "eu-west-2".to_owned(),
                    tags: BTreeMap::new(),
                },
                deadline,
            )
            .await
            .expect("ready descriptor recovers");
        assert_eq!(resolved.identity.cluster_id, ID);
        assert_eq!(resolved.identity.cluster_arn, ARN);
        assert_eq!(resolved.endpoint, "cluster.dsql.eu-west-2.on.aws");
        let descriptor = store
            .value
            .lock()
            .expect("descriptor lock is not poisoned")
            .clone()
            .expect("descriptor remains durable");
        assert!(matches!(
            descriptor.state,
            ClusterDescriptorState::Ready {
                cluster_id,
                cluster_arn,
                endpoint,
            } if cluster_id == ID
                && cluster_arn == ARN
                && endpoint == "cluster.dsql.eu-west-2.on.aws"
        ));
        assert!(
            control
                .created
                .lock()
                .expect("create lock is not poisoned")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn ready_recovery_rejects_canonical_identity_disagreement() {
        let control = FakeControl::new();
        let mut conflicting = observation(ClusterStatus::Active);
        conflicting.identifier = "zzzzzzzzzzzzzzzzzzzzzzzzzz".to_owned();
        conflicting.arn =
            "arn:aws:dsql:eu-west-2:123456789012:cluster/zzzzzzzzzzzzzzzzzzzzzzzzzz".to_owned();
        control
            .gets
            .lock()
            .expect("get lock is not poisoned")
            .push_back(Ok(conflicting));
        let store = MemoryStore::default();
        *store.value.lock().expect("descriptor lock is not poisoned") =
            Some(ready_descriptor("durable-test-token"));
        let time = FakeTime::new();
        let deadline = StartupDeadline::after(&time, Duration::from_secs(10));
        let lifecycle = ManagedDsqlLifecycle::new(control, store, time);
        let result = lifecycle
            .create_or_recover(
                CreateOrRecoverRequest {
                    region: "eu-west-2".to_owned(),
                    tags: BTreeMap::new(),
                },
                deadline,
            )
            .await;
        assert!(matches!(result, Err(ManagedDsqlError::IdentityMismatch)));
    }

    #[tokio::test]
    async fn idle_hands_off_to_connection_wake_then_polls_active() {
        let control = FakeControl::new();
        let store = MemoryStore::default();
        let time = FakeTime::new();
        let deadline = StartupDeadline::after(&time, Duration::from_secs(10));
        let lifecycle = ManagedDsqlLifecycle::new(control, store, time).with_retry_policy(
            RetryPolicy::new(Duration::from_millis(1), Duration::from_millis(2), false)
                .expect("policy is valid"),
        );
        let cluster = super::validate_observation(
            &observation(ClusterStatus::Idle),
            None,
            ClusterAction::Recovered,
        )
        .expect("fixture observation is valid");
        let handed_off = lifecycle
            .refresh_until_usable(cluster, deadline)
            .await
            .expect("idle status is a wake handoff");
        let wake = match handed_off {
            Readiness::WakeRequired(cluster) => cluster,
            Readiness::Active(_) => panic!("idle cluster must require a connection wake"),
        };
        let active = lifecycle
            .refresh_after_wake(wake, deadline)
            .await
            .expect("post-wake poll becomes active");
        assert_eq!(active.resolved().status, ClusterStatus::Active);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        // Feature: managed-embedded-dsql, Property 3: creation is idempotent across every crash point
        #[test]
        fn creation_is_idempotent_across_every_crash_point(crash_point in 0_u8..4) {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime builds");
            runtime.block_on(async {
                let control = FakeControl::new();
                let store = MemoryStore::default();
                if crash_point == 1 || crash_point == 2 {
                    let pending = ClusterDescriptorV1::pending(
                        "eu-west-2",
                        DsqlClientToken::new("durable-test-token")
                            .expect("fixture token is valid"),
                    );
                    store.compare_and_swap(None, &pending).await.expect("pending descriptor persists");
                }
                if crash_point == 2 {
                    *store.fail_next_ready.lock().expect("failure lock is not poisoned") = true;
                }
                if crash_point == 3 {
                    *store.value.lock().expect("descriptor lock is not poisoned") =
                        Some(ready_descriptor("durable-test-token"));
                }
                let time = FakeTime::new();
                let deadline = StartupDeadline::after(&time, Duration::from_secs(30));
                let lifecycle = ManagedDsqlLifecycle::new(control.clone(), store.clone(), time);
                let request = CreateOrRecoverRequest {
                    region: "eu-west-2".to_owned(),
                    tags: BTreeMap::new(),
                };
                let first = lifecycle.create_or_recover(request.clone(), deadline).await;
                if crash_point == 2 {
                    let descriptor_failed = matches!(first, Err(ManagedDsqlError::Descriptor(_)));
                    prop_assert!(descriptor_failed);
                } else {
                    prop_assert!(first.is_ok());
                }
                let recovered = lifecycle.create_or_recover(request, deadline).await
                    .expect("replay resolves the canonical cluster");
                prop_assert_eq!(recovered.identity.cluster_id, ID);
                let created = control.created.lock().expect("create lock is not poisoned");
                prop_assert!(created.len() <= 1);
                if !created.is_empty() {
                    prop_assert!(created.contains_key("durable-test-token"));
                }
                Ok(())
            })?;
        }

        // Feature: managed-embedded-dsql, Property 5: recovery follows the cluster-status reference model
        #[test]
        fn recovery_follows_the_cluster_status_reference_model(
            indices in prop::collection::vec(0_usize..11, 1..8),
            inject_retry in any::<bool>(),
            retry_after in 0_u64..30
        ) {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime builds");
            runtime.block_on(async {
                let all_statuses = [
                    ClusterStatus::Creating,
                    ClusterStatus::Active,
                    ClusterStatus::Idle,
                    ClusterStatus::Inactive,
                    ClusterStatus::Updating,
                    ClusterStatus::Deleting,
                    ClusterStatus::Deleted,
                    ClusterStatus::Failed,
                    ClusterStatus::PendingSetup,
                    ClusterStatus::PendingDelete,
                    ClusterStatus::Unknown("FUTURE".to_owned()),
                ];
                let statuses = indices
                    .iter()
                    .map(|index| all_statuses[*index].clone())
                    .collect::<Vec<_>>();
                let expected = statuses
                    .iter()
                    .map(status_decision)
                    .find(|decision| *decision != StatusDecision::Poll)
                    .unwrap_or(StatusDecision::Active);

                let control = FakeControl::new();
                if inject_retry && status_decision(&statuses[0]) == StatusDecision::Poll {
                    control.gets.lock().expect("get lock is not poisoned").push_back(Err(
                        DsqlControlError::Retryable {
                            kind: crate::control::RetryableErrorKind::Throttling,
                            retry_after: Some(Duration::from_secs(retry_after)),
                        },
                    ));
                }
                control
                    .gets
                    .lock()
                    .expect("get lock is not poisoned")
                    .extend(statuses.iter().skip(1).cloned().map(|status| Ok(observation(status))));
                let time = FakeTime::new();
                let deadline = StartupDeadline::after(&time, Duration::from_secs(120));
                let lifecycle = ManagedDsqlLifecycle::new(
                    control,
                    MemoryStore::default(),
                    time.clone(),
                )
                .with_retry_policy(
                    RetryPolicy::new(Duration::from_millis(1), Duration::from_millis(2), false)
                        .expect("policy is valid"),
                );
                let cluster = super::validate_observation(
                    &observation(statuses[0].clone()),
                    None,
                    ClusterAction::Recovered,
                )
                .expect("generated observation is valid");
                let result = lifecycle.refresh_until_usable(cluster, deadline).await;
                match expected {
                    StatusDecision::Active => {
                        prop_assert!(matches!(result, Ok(Readiness::Active(_))));
                    }
                    StatusDecision::Wake => {
                        prop_assert!(matches!(result, Ok(Readiness::WakeRequired(_))));
                    }
                    StatusDecision::Terminal => {
                        let terminal = matches!(
                            result,
                            Err(ManagedDsqlError::UnsupportedStatus { .. })
                        );
                        prop_assert!(terminal);
                    }
                    StatusDecision::Poll => unreachable!("reference consumes every poll sequence"),
                }
                if inject_retry && status_decision(&statuses[0]) == StatusDecision::Poll {
                    let minimum = Duration::from_secs(retry_after);
                    let slept = time.sleeps.lock().expect("sleep log lock is not poisoned");
                    prop_assert!(slept.iter().any(|delay| *delay >= minimum));
                }
                Ok(())
            })?;
        }
    }
}
