//! Fenced Aurora DSQL control leases and process-local ownership admission.
//!
//! Control leases serialize schema migration and exclude concurrent managed
//! embedded owners. They are deliberately storage/runtime coordination: claim
//! owners and fence tokens never enter workflow commands or history. Database
//! time decides lease expiry, while a conservative monotonic deadline closes
//! local admission before an unconfirmed owner can outlive its database lease.

use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicUsize, Ordering},
    },
    time::{Duration as StdDuration, Instant},
};

use sqlx::PgPool;
use time::{Duration, OffsetDateTime};
use tokio::sync::Notify;

use super::migration::control_lease_bootstrap_sql;

const DEFAULT_MAX_OCC_RETRIES: u32 = 5;
/// Default lifetime for the managed embedded owner claim.
pub const OWNER_LEASE_DURATION: Duration = Duration::seconds(60);
/// Normal interval between managed embedded owner renewals.
pub const OWNER_RENEW_INTERVAL: Duration = Duration::seconds(20);
/// Safety margin between local admission closure and database expiry.
pub const OWNER_ADMISSION_MARGIN: Duration = Duration::seconds(20);
/// Quiescence after an expired takeover, bounded by DSQL's transaction limit.
pub const EXPIRED_TAKEOVER_QUIESCENCE: Duration = Duration::minutes(5);

const INSERT_CLAIM_SQL: &str = "INSERT INTO tokeira_control_lease \
    (claim_name, cluster_id, cluster_arn, owner_id, fence_token, expires_at, updated_at) \
    VALUES ($1, $2, $3, NULL, 0, now(), now()) ON CONFLICT (claim_name) DO NOTHING";
const LOCK_CLAIM_SQL: &str = "SELECT cluster_id, cluster_arn, owner_id, fence_token, \
    expires_at, now() FROM tokeira_control_lease WHERE claim_name = $1 FOR UPDATE";
const ACQUIRE_CLAIM_SQL: &str = "UPDATE tokeira_control_lease SET owner_id = $2, \
    fence_token = $3, expires_at = now() + ($4::BIGINT * INTERVAL '1 millisecond'), \
    updated_at = now() WHERE claim_name = $1 AND fence_token = $5 RETURNING expires_at";
const RENEW_CLAIM_SQL: &str = "UPDATE tokeira_control_lease SET expires_at = \
    now() + ($4::BIGINT * INTERVAL '1 millisecond'), updated_at = now() WHERE \
    claim_name = $1 AND owner_id = $2 AND fence_token = $3 AND expires_at > now() \
    RETURNING expires_at";
const RELEASE_CLAIM_SQL: &str = "UPDATE tokeira_control_lease SET owner_id = NULL, \
    expires_at = now(), updated_at = now() WHERE claim_name = $1 AND owner_id = $2 \
    AND fence_token = $3";

/// Exact canonical identity stored with every control claim.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ControlLeaseClusterIdentity {
    /// AWS DSQL cluster identifier.
    pub cluster_id: String,
    /// AWS DSQL cluster ARN.
    pub cluster_arn: String,
}

/// Parameters for one control-lease acquisition.
#[derive(Clone, Debug)]
pub struct ControlLeaseAcquireRequest {
    /// Stable claim namespace, such as `schema-migration` or `embedded-owner`.
    pub claim_name: String,
    /// Canonical target cluster identity.
    pub cluster: ControlLeaseClusterIdentity,
    /// Unique process incarnation or schema-operation identifier.
    pub owner_id: String,
    /// Database lease duration.
    pub lease_duration: Duration,
    /// Conservative local margin subtracted from the database duration.
    pub admission_margin: Duration,
    /// Monotonic deadline bounding OCC retries.
    pub acquire_deadline: Instant,
}

/// How an acquisition obtained the claim.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ControlLeaseAcquireOutcome {
    /// A new or cleanly released claim was acquired.
    Clean,
    /// An expired owner was replaced and requires a quiescence interval.
    ExpiredTakeover,
}

/// In-memory proof of a successfully acquired database claim.
///
/// The type intentionally has no serialization implementation. Its database
/// expiry is diagnostic; `local_admission_deadline` is the process-local safety
/// authority for admitting new work.
pub struct ControlLeaseGuard {
    claim_name: String,
    cluster: ControlLeaseClusterIdentity,
    owner_id: String,
    fence_token: i64,
    database_expires_at: OffsetDateTime,
    local_admission_deadline: Instant,
    quiescence_deadline: Option<Instant>,
    outcome: ControlLeaseAcquireOutcome,
}

impl fmt::Debug for ControlLeaseGuard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlLeaseGuard")
            .field("claim_name", &self.claim_name)
            .field("owner_id", &self.owner_id)
            .field("fence_token", &self.fence_token)
            .field("database_expires_at", &self.database_expires_at)
            .field("outcome", &self.outcome)
            .finish_non_exhaustive()
    }
}

impl ControlLeaseGuard {
    /// Claim namespace protected by this guard.
    pub fn claim_name(&self) -> &str {
        &self.claim_name
    }

    /// Canonical cluster identity bound to the claim.
    pub const fn cluster(&self) -> &ControlLeaseClusterIdentity {
        &self.cluster
    }

    /// Unique owner incarnation recorded in DSQL.
    pub fn owner_id(&self) -> &str {
        &self.owner_id
    }

    /// Monotonic fence token assigned by the database claim row.
    pub const fn fence_token(&self) -> i64 {
        self.fence_token
    }

    /// Database-time expiry returned by the successful acquire or renewal.
    pub const fn database_expires_at(&self) -> OffsetDateTime {
        self.database_expires_at
    }

    /// Conservative monotonic deadline for local admission.
    pub const fn local_admission_deadline(&self) -> Instant {
        self.local_admission_deadline
    }

    /// Monotonic quiescence deadline after an unclean takeover, if any.
    pub const fn quiescence_deadline(&self) -> Option<Instant> {
        self.quiescence_deadline
    }

    /// Whether acquisition was clean or replaced an expired owner.
    pub const fn outcome(&self) -> ControlLeaseAcquireOutcome {
        self.outcome
    }

    /// Close the shared gate when the conservative renewal deadline passes.
    pub fn enforce_admission_deadline(&self, now: Instant, gate: &OwnershipAdmissionGate) {
        if now >= self.local_admission_deadline {
            gate.begin_closing();
        }
    }
}

/// Clock seam used to make deadline and fencing tests deterministic.
pub trait MonotonicClock: fmt::Debug + Send + Sync {
    /// Current process-monotonic time.
    fn now(&self) -> Instant;
}

/// System monotonic clock used outside tests.
#[derive(Debug, Default)]
pub struct SystemMonotonicClock;

impl MonotonicClock for SystemMonotonicClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// Failures from fenced control-lease operations.
#[derive(Debug, thiserror::Error)]
pub enum ControlLeaseError {
    /// The request contains an invalid name, owner, duration, or margin.
    #[error("invalid control lease request: {0}")]
    InvalidRequest(String),
    /// The claim row belongs to a different canonical cluster.
    #[error("control lease cluster identity does not match the target cluster")]
    ClusterIdentityMismatch,
    /// Another live owner currently holds the claim.
    #[error("control lease is busy until {expires_at} (owner {owner_id})")]
    Busy {
        /// Current non-secret owner incarnation.
        owner_id: String,
        /// Database-time expiry of the current owner.
        expires_at: OffsetDateTime,
    },
    /// The operation exceeded its monotonic retry deadline.
    #[error("control lease acquisition deadline elapsed")]
    DeadlineElapsed,
    /// A conditional renewal or release proved that this owner was fenced.
    #[error("control lease owner was fenced")]
    Fenced,
    /// A DSQL operation failed.
    #[error("control lease database operation failed")]
    Database(#[source] sqlx::Error),
}

impl ControlLeaseError {
    fn is_occ_conflict(&self) -> bool {
        matches!(
            self,
            Self::Database(sqlx::Error::Database(error))
                if error.code().as_deref() == Some("40001")
        )
    }
}

impl From<sqlx::Error> for ControlLeaseError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

/// DSQL repository for fenced control claims.
#[derive(Debug)]
pub struct ControlLeaseRepository {
    pool: PgPool,
    clock: Arc<dyn MonotonicClock>,
    max_occ_retries: u32,
}

impl ControlLeaseRepository {
    /// Construct a repository with system monotonic time and bounded OCC retry.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            clock: Arc::new(SystemMonotonicClock),
            max_occ_retries: DEFAULT_MAX_OCC_RETRIES,
        }
    }

    /// Construct a repository with an injected monotonic clock.
    pub fn with_clock(pool: PgPool, clock: Arc<dyn MonotonicClock>, max_occ_retries: u32) -> Self {
        Self {
            pool,
            clock,
            max_occ_retries,
        }
    }

    /// Create the idempotent bootstrap table needed before the lease-protected migrations.
    pub async fn bootstrap(&self) -> Result<(), ControlLeaseError> {
        sqlx::query(control_lease_bootstrap_sql())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Acquire a claim with exact identity, database expiry, and monotonic fencing.
    pub async fn acquire(
        &self,
        request: &ControlLeaseAcquireRequest,
    ) -> Result<ControlLeaseGuard, ControlLeaseError> {
        let timing = validate_request(request)?;
        sqlx::query(INSERT_CLAIM_SQL)
            .bind(&request.claim_name)
            .bind(&request.cluster.cluster_id)
            .bind(&request.cluster.cluster_arn)
            .execute(&self.pool)
            .await?;

        let mut conflicts = 0;
        loop {
            if self.clock.now() >= request.acquire_deadline {
                return Err(ControlLeaseError::DeadlineElapsed);
            }
            match self.acquire_once(request, timing).await {
                Ok(guard) => return Ok(guard),
                Err(error) if error.is_occ_conflict() && conflicts < self.max_occ_retries => {
                    conflicts += 1;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn acquire_once(
        &self,
        request: &ControlLeaseAcquireRequest,
        timing: ValidatedLeaseTiming,
    ) -> Result<ControlLeaseGuard, ControlLeaseError> {
        // A fresh transaction is mandatory after every 40001: DSQL's
        // repeatable-read snapshot cannot be reused after a concurrent winner.
        let mut transaction = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut *transaction)
            .await?;
        let row = sqlx::query_as::<
            _,
            (
                String,
                String,
                Option<String>,
                i64,
                OffsetDateTime,
                OffsetDateTime,
            ),
        >(LOCK_CLAIM_SQL)
        .bind(&request.claim_name)
        .fetch_one(&mut *transaction)
        .await?;
        let snapshot = LeaseSnapshot {
            cluster: ControlLeaseClusterIdentity {
                cluster_id: row.0,
                cluster_arn: row.1,
            },
            owner_id: row.2,
            fence_token: row.3,
            expires_at: row.4,
        };
        let outcome = acquisition_outcome(&snapshot, &request.cluster, row.5)?;
        let next_fence = snapshot
            .fence_token
            .checked_add(1)
            .ok_or_else(|| ControlLeaseError::InvalidRequest("fence token exhausted".to_owned()))?;
        let database_expires_at = sqlx::query_scalar::<_, OffsetDateTime>(ACQUIRE_CLAIM_SQL)
            .bind(&request.claim_name)
            .bind(&request.owner_id)
            .bind(next_fence)
            .bind(timing.lease_millis)
            .bind(snapshot.fence_token)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(ControlLeaseError::Fenced)?;
        transaction.commit().await?;

        let confirmed_at = self.clock.now();
        let quiescence_deadline = (outcome == ControlLeaseAcquireOutcome::ExpiredTakeover)
            .then(|| confirmed_at + timing.quiescence);
        Ok(ControlLeaseGuard {
            claim_name: request.claim_name.clone(),
            cluster: request.cluster.clone(),
            owner_id: request.owner_id.clone(),
            fence_token: next_fence,
            database_expires_at,
            local_admission_deadline: confirmed_at + timing.admission_duration,
            quiescence_deadline,
            outcome,
        })
    }

    /// Renew an unexpired guard, fencing and closing admission on any lost claim.
    pub async fn renew(
        &self,
        guard: &mut ControlLeaseGuard,
        lease_duration: Duration,
        admission_margin: Duration,
        gate: &OwnershipAdmissionGate,
    ) -> Result<(), ControlLeaseError> {
        let timing = validate_timing(lease_duration, admission_margin)?;
        guard.enforce_admission_deadline(self.clock.now(), gate);
        let renewed = sqlx::query_scalar::<_, OffsetDateTime>(RENEW_CLAIM_SQL)
            .bind(&guard.claim_name)
            .bind(&guard.owner_id)
            .bind(guard.fence_token)
            .bind(timing.lease_millis)
            .fetch_optional(&self.pool)
            .await?;
        let database_expires_at = require_conditional_match(renewed, gate)?;
        guard.database_expires_at = database_expires_at;
        guard.local_admission_deadline = self.clock.now() + timing.admission_duration;
        Ok(())
    }

    /// Cleanly release a guard after closing new local admission.
    pub async fn release(
        &self,
        guard: &ControlLeaseGuard,
        gate: &OwnershipAdmissionGate,
    ) -> Result<(), ControlLeaseError> {
        gate.begin_closing();
        let result = sqlx::query(RELEASE_CLAIM_SQL)
            .bind(&guard.claim_name)
            .bind(&guard.owner_id)
            .bind(guard.fence_token)
            .execute(&self.pool)
            .await?;
        require_conditional_match((result.rows_affected() > 0).then_some(()), gate)?;
        Ok(())
    }
}

fn require_conditional_match<T>(
    value: Option<T>,
    gate: &OwnershipAdmissionGate,
) -> Result<T, ControlLeaseError> {
    value.ok_or_else(|| {
        gate.fence();
        ControlLeaseError::Fenced
    })
}

#[derive(Clone, Debug)]
struct LeaseSnapshot {
    cluster: ControlLeaseClusterIdentity,
    owner_id: Option<String>,
    fence_token: i64,
    expires_at: OffsetDateTime,
}

fn acquisition_outcome(
    snapshot: &LeaseSnapshot,
    expected_cluster: &ControlLeaseClusterIdentity,
    database_now: OffsetDateTime,
) -> Result<ControlLeaseAcquireOutcome, ControlLeaseError> {
    if snapshot.cluster != *expected_cluster {
        return Err(ControlLeaseError::ClusterIdentityMismatch);
    }
    match &snapshot.owner_id {
        Some(owner_id) if snapshot.expires_at > database_now => Err(ControlLeaseError::Busy {
            owner_id: owner_id.clone(),
            expires_at: snapshot.expires_at,
        }),
        Some(_) => Ok(ControlLeaseAcquireOutcome::ExpiredTakeover),
        None => Ok(ControlLeaseAcquireOutcome::Clean),
    }
}

#[derive(Clone, Copy, Debug)]
struct ValidatedLeaseTiming {
    lease_millis: i64,
    admission_duration: StdDuration,
    quiescence: StdDuration,
}

fn validate_request(
    request: &ControlLeaseAcquireRequest,
) -> Result<ValidatedLeaseTiming, ControlLeaseError> {
    if request.claim_name.trim().is_empty()
        || request.owner_id.trim().is_empty()
        || request.cluster.cluster_id.trim().is_empty()
        || request.cluster.cluster_arn.trim().is_empty()
    {
        return Err(ControlLeaseError::InvalidRequest(
            "claim, owner, cluster ID, and cluster ARN must be non-empty".to_owned(),
        ));
    }
    validate_timing(request.lease_duration, request.admission_margin)
}

fn validate_timing(
    lease_duration: Duration,
    admission_margin: Duration,
) -> Result<ValidatedLeaseTiming, ControlLeaseError> {
    if lease_duration <= Duration::ZERO
        || admission_margin <= Duration::ZERO
        || admission_margin >= lease_duration
    {
        return Err(ControlLeaseError::InvalidRequest(
            "lease duration must be positive and exceed its admission margin".to_owned(),
        ));
    }
    let lease_millis = i64::try_from(lease_duration.whole_milliseconds()).map_err(|_| {
        ControlLeaseError::InvalidRequest("lease duration exceeds DSQL interval range".to_owned())
    })?;
    let admission_duration = std_duration(lease_duration - admission_margin)?;
    Ok(ValidatedLeaseTiming {
        lease_millis,
        admission_duration,
        quiescence: std_duration(EXPIRED_TAKEOVER_QUIESCENCE)?,
    })
}

fn std_duration(duration: Duration) -> Result<StdDuration, ControlLeaseError> {
    StdDuration::try_from(duration).map_err(|_| {
        ControlLeaseError::InvalidRequest("duration must fit monotonic time".to_owned())
    })
}

/// Process-local state shared by the in-process edge and DSQL director.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum OwnershipAdmissionState {
    /// New operations may enter.
    Open = 0,
    /// New operations are rejected while admitted work drains.
    Closing = 1,
    /// The database claim was lost; reopening is forbidden.
    Fenced = 2,
}

#[derive(Debug)]
struct OwnershipAdmissionInner {
    state: AtomicU8,
    in_flight: AtomicUsize,
    drained: Notify,
}

/// Atomic admission gate for all managed embedded workflow and storage work.
#[derive(Clone, Debug)]
pub struct OwnershipAdmissionGate {
    inner: Arc<OwnershipAdmissionInner>,
}

impl OwnershipAdmissionGate {
    /// Build a gate for an acquired owner, closed during expired-takeover quiescence.
    pub fn for_guard(guard: &ControlLeaseGuard) -> Self {
        let state = if guard.quiescence_deadline.is_some() {
            OwnershipAdmissionState::Closing
        } else {
            OwnershipAdmissionState::Open
        };
        Self::new(state)
    }

    /// Build a gate in an explicit initial state.
    pub fn new(state: OwnershipAdmissionState) -> Self {
        Self {
            inner: Arc::new(OwnershipAdmissionInner {
                state: AtomicU8::new(state as u8),
                in_flight: AtomicUsize::new(0),
                drained: Notify::new(),
            }),
        }
    }

    /// Observe the current admission state.
    pub fn state(&self) -> OwnershipAdmissionState {
        decode_state(self.inner.state.load(Ordering::Acquire))
    }

    /// Atomically admit one operation or reject it after closure/fencing.
    pub fn admit(&self) -> Result<OwnershipAdmissionPermit, OwnershipAdmissionError> {
        if self.state() != OwnershipAdmissionState::Open {
            return Err(OwnershipAdmissionError::from_state(self.state()));
        }
        self.inner.in_flight.fetch_add(1, Ordering::AcqRel);
        // Recheck after increment so a concurrent close either observes this
        // permit in the drain count or causes us to undo admission.
        let state = self.state();
        if state != OwnershipAdmissionState::Open {
            release_admission(&self.inner);
            return Err(OwnershipAdmissionError::from_state(state));
        }
        Ok(OwnershipAdmissionPermit {
            inner: Arc::clone(&self.inner),
        })
    }

    /// Stop admitting new work while allowing existing permits to drain.
    pub fn begin_closing(&self) {
        let _ = self.inner.state.compare_exchange(
            OwnershipAdmissionState::Open as u8,
            OwnershipAdmissionState::Closing as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        if self.inner.in_flight.load(Ordering::Acquire) == 0 {
            self.inner.drained.notify_waiters();
        }
    }

    /// Permanently fence the process after ownership loss.
    pub fn fence(&self) {
        self.inner
            .state
            .store(OwnershipAdmissionState::Fenced as u8, Ordering::Release);
        if self.inner.in_flight.load(Ordering::Acquire) == 0 {
            self.inner.drained.notify_waiters();
        }
    }

    /// Open an expired-takeover gate once its monotonic quiescence completes.
    pub fn finish_quiescence(
        &self,
        guard: &ControlLeaseGuard,
        now: Instant,
    ) -> Result<(), OwnershipAdmissionError> {
        let Some(deadline) = guard.quiescence_deadline else {
            return Ok(());
        };
        if now < deadline {
            return Err(OwnershipAdmissionError::Quiescing);
        }
        self.inner
            .state
            .compare_exchange(
                OwnershipAdmissionState::Closing as u8,
                OwnershipAdmissionState::Open as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map(|_| ())
            .map_err(|state| OwnershipAdmissionError::from_state(decode_state(state)))
    }

    /// Wait until every already-admitted operation has released its permit.
    pub async fn wait_for_drain(&self) {
        loop {
            let notified = self.inner.drained.notified();
            if self.inner.in_flight.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }

    /// Current count of admitted operations, intended for diagnostics and tests.
    pub fn in_flight(&self) -> usize {
        self.inner.in_flight.load(Ordering::Acquire)
    }
}

fn decode_state(state: u8) -> OwnershipAdmissionState {
    match state {
        0 => OwnershipAdmissionState::Open,
        1 => OwnershipAdmissionState::Closing,
        _ => OwnershipAdmissionState::Fenced,
    }
}

/// Rejection returned when managed embedded ownership cannot admit new work.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum OwnershipAdmissionError {
    /// Ownership is closing and existing work is draining.
    #[error("managed embedded ownership is closing")]
    Closing,
    /// An expired takeover is still in its safety quiescence interval.
    #[error("managed embedded ownership is quiescing after expired takeover")]
    Quiescing,
    /// Ownership was lost and this process is permanently fenced.
    #[error("managed embedded ownership was fenced")]
    Fenced,
}

impl OwnershipAdmissionError {
    fn from_state(state: OwnershipAdmissionState) -> Self {
        match state {
            OwnershipAdmissionState::Open | OwnershipAdmissionState::Closing => Self::Closing,
            OwnershipAdmissionState::Fenced => Self::Fenced,
        }
    }
}

/// RAII proof that one operation passed the ownership admission gate.
pub struct OwnershipAdmissionPermit {
    inner: Arc<OwnershipAdmissionInner>,
}

impl fmt::Debug for OwnershipAdmissionPermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnershipAdmissionPermit")
            .finish_non_exhaustive()
    }
}

impl Drop for OwnershipAdmissionPermit {
    fn drop(&mut self) {
        release_admission(&self.inner);
    }
}

fn release_admission(inner: &OwnershipAdmissionInner) {
    if inner.in_flight.fetch_sub(1, Ordering::AcqRel) == 1 {
        inner.drained.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn identity() -> ControlLeaseClusterIdentity {
        ControlLeaseClusterIdentity {
            cluster_id: "cluster-1".to_owned(),
            cluster_arn: "arn:aws:dsql:eu-west-1:123456789012:cluster/cluster-1".to_owned(),
        }
    }

    fn snapshot(owner_id: Option<&str>, expires_at: OffsetDateTime) -> LeaseSnapshot {
        LeaseSnapshot {
            cluster: identity(),
            owner_id: owner_id.map(str::to_owned),
            fence_token: 4,
            expires_at,
        }
    }

    #[test]
    fn database_time_distinguishes_busy_clean_and_expired_takeover() {
        let database_now = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        assert!(matches!(
            acquisition_outcome(
                &snapshot(Some("owner-a"), database_now + Duration::seconds(1)),
                &identity(),
                database_now,
            ),
            Err(ControlLeaseError::Busy { .. })
        ));
        assert_eq!(
            acquisition_outcome(&snapshot(None, database_now), &identity(), database_now)
                .expect("released row is available"),
            ControlLeaseAcquireOutcome::Clean
        );
        assert_eq!(
            acquisition_outcome(
                &snapshot(Some("owner-a"), database_now),
                &identity(),
                database_now,
            )
            .expect("database expiry permits takeover"),
            ControlLeaseAcquireOutcome::ExpiredTakeover
        );
    }

    #[test]
    fn identity_mismatch_is_redacted() {
        let mut wrong = identity();
        wrong.cluster_id = "other".to_owned();
        let error = acquisition_outcome(
            &snapshot(None, OffsetDateTime::UNIX_EPOCH),
            &wrong,
            OffsetDateTime::UNIX_EPOCH,
        )
        .expect_err("different canonical identity must fail");
        let diagnostic = format!("{error:?} {error}");
        assert!(!diagnostic.contains("cluster-1"));
        assert!(!diagnostic.contains("arn:aws"));
    }

    #[tokio::test]
    async fn admission_closes_atomically_and_drains_exactly_once() {
        let gate = OwnershipAdmissionGate::new(OwnershipAdmissionState::Open);
        let first = gate.admit().expect("open gate admits");
        let second = gate.admit().expect("open gate admits");
        assert_eq!(gate.in_flight(), 2);
        gate.begin_closing();
        assert!(matches!(
            gate.admit(),
            Err(OwnershipAdmissionError::Closing)
        ));
        drop(first);
        assert_eq!(gate.in_flight(), 1);
        drop(second);
        gate.wait_for_drain().await;
        assert_eq!(gate.in_flight(), 0);
    }

    #[test]
    fn fenced_gate_never_reopens() {
        let gate = OwnershipAdmissionGate::new(OwnershipAdmissionState::Open);
        gate.fence();
        gate.begin_closing();
        assert_eq!(gate.state(), OwnershipAdmissionState::Fenced);
        assert!(matches!(gate.admit(), Err(OwnershipAdmissionError::Fenced)));
    }

    #[test]
    fn renewal_and_release_zero_rows_permanently_fence_admission() {
        for gate in [
            OwnershipAdmissionGate::new(OwnershipAdmissionState::Open),
            OwnershipAdmissionGate::new(OwnershipAdmissionState::Closing),
        ] {
            assert!(matches!(
                require_conditional_match::<()>(None, &gate),
                Err(ControlLeaseError::Fenced)
            ));
            assert_eq!(gate.state(), OwnershipAdmissionState::Fenced);
            assert!(matches!(gate.admit(), Err(OwnershipAdmissionError::Fenced)));
        }
    }

    #[test]
    fn monotonic_deadline_closes_before_database_expiry() {
        let now = Instant::now();
        let guard = ControlLeaseGuard {
            claim_name: "embedded-owner".to_owned(),
            cluster: identity(),
            owner_id: "owner-a".to_owned(),
            fence_token: 5,
            database_expires_at: OffsetDateTime::UNIX_EPOCH + Duration::days(1),
            local_admission_deadline: now + StdDuration::from_secs(40),
            quiescence_deadline: None,
            outcome: ControlLeaseAcquireOutcome::Clean,
        };
        let gate = OwnershipAdmissionGate::for_guard(&guard);

        guard.enforce_admission_deadline(now + StdDuration::from_secs(39), &gate);
        assert_eq!(gate.state(), OwnershipAdmissionState::Open);
        guard.enforce_admission_deadline(now + StdDuration::from_secs(40), &gate);
        assert_eq!(gate.state(), OwnershipAdmissionState::Closing);
    }

    #[test]
    fn expired_takeover_quiesces_but_clean_takeover_opens_immediately() {
        let now = Instant::now();
        let expired = ControlLeaseGuard {
            claim_name: "embedded-owner".to_owned(),
            cluster: identity(),
            owner_id: "owner-b".to_owned(),
            fence_token: 6,
            database_expires_at: OffsetDateTime::UNIX_EPOCH,
            local_admission_deadline: now + StdDuration::from_secs(40),
            quiescence_deadline: Some(now + StdDuration::from_secs(300)),
            outcome: ControlLeaseAcquireOutcome::ExpiredTakeover,
        };
        let expired_gate = OwnershipAdmissionGate::for_guard(&expired);
        assert_eq!(expired_gate.state(), OwnershipAdmissionState::Closing);
        assert!(matches!(
            expired_gate.finish_quiescence(&expired, now),
            Err(OwnershipAdmissionError::Quiescing)
        ));
        expired_gate
            .finish_quiescence(&expired, now + StdDuration::from_secs(300))
            .expect("quiescence completed");
        assert_eq!(expired_gate.state(), OwnershipAdmissionState::Open);

        let clean = ControlLeaseGuard {
            quiescence_deadline: None,
            outcome: ControlLeaseAcquireOutcome::Clean,
            ..expired
        };
        assert_eq!(
            OwnershipAdmissionGate::for_guard(&clean).state(),
            OwnershipAdmissionState::Open
        );
    }

    #[test]
    fn guard_debug_omits_canonical_cluster_identity() {
        let guard = ControlLeaseGuard {
            claim_name: "embedded-owner".to_owned(),
            cluster: identity(),
            owner_id: "owner-a".to_owned(),
            fence_token: 5,
            database_expires_at: OffsetDateTime::UNIX_EPOCH,
            local_admission_deadline: Instant::now(),
            quiescence_deadline: None,
            outcome: ControlLeaseAcquireOutcome::Clean,
        };
        let diagnostic = format!("{guard:?}");
        assert!(!diagnostic.contains("cluster-1"));
        assert!(!diagnostic.contains("arn:aws"));
    }

    #[derive(Clone, Copy, Debug)]
    enum ModelOperation {
        Acquire { owner: u8, duration: u8 },
        Renew { owner: u8, duration: u8 },
        Release { owner: u8 },
        Advance { amount: u8 },
    }

    fn operation_strategy() -> impl Strategy<Value = ModelOperation> {
        prop_oneof![
            (0_u8..8, 1_u8..20)
                .prop_map(|(owner, duration)| ModelOperation::Acquire { owner, duration }),
            (0_u8..8, 1_u8..20)
                .prop_map(|(owner, duration)| ModelOperation::Renew { owner, duration }),
            (0_u8..8).prop_map(|owner| ModelOperation::Release { owner }),
            (0_u8..20).prop_map(|amount| ModelOperation::Advance { amount }),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        // Feature: managed-embedded-dsql, Property 11: embedded ownership has at most one admitted owner
        #[test]
        fn embedded_ownership_model_admits_at_most_one_owner(
            operations in prop::collection::vec(operation_strategy(), 1..200),
        ) {
            let mut now = 0_u64;
            let mut row: Option<(u8, u64, u64)> = None;
            let mut next_fence = 0_u64;
            for operation in operations {
                match operation {
                    ModelOperation::Acquire { owner, duration } => {
                        if row.is_none_or(|(_, _, expiry)| expiry <= now) {
                            next_fence += 1;
                            row = Some((owner, next_fence, now + u64::from(duration)));
                        }
                    }
                    ModelOperation::Renew { owner, duration } => {
                        if let Some((current, fence, expiry)) = row
                            && current == owner
                            && expiry > now
                        {
                            row = Some((owner, fence, now + u64::from(duration)));
                        }
                    }
                    ModelOperation::Release { owner } => {
                        if row.is_some_and(|(current, _, _)| current == owner) {
                            row = None;
                        }
                    }
                    ModelOperation::Advance { amount } => now += u64::from(amount),
                }

                let admitted = (0_u8..8)
                    .filter(|owner| {
                        row.is_some_and(|(current, _, expiry)| current == *owner && expiry > now)
                    })
                    .count();
                prop_assert!(admitted <= 1);
            }
        }
    }

    #[test]
    fn lease_sql_carries_required_fencing_predicates() {
        assert!(INSERT_CLAIM_SQL.contains("ON CONFLICT (claim_name) DO NOTHING"));
        assert!(LOCK_CLAIM_SQL.contains("WHERE claim_name = $1 FOR UPDATE"));
        assert!(RENEW_CLAIM_SQL.contains("owner_id = $2 AND fence_token = $3"));
        assert!(RENEW_CLAIM_SQL.contains("expires_at > now()"));
        assert!(RELEASE_CLAIM_SQL.contains("owner_id = $2"));
        assert!(RELEASE_CLAIM_SQL.contains("fence_token = $3"));
    }
}
