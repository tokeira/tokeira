//! The single-node claim of a listener-backed distributed node without a
//! placement controller (`.kiro/specs/single-node-claim`).
//!
//! Such a node assigns itself every execution home and writes unfenced, which
//! is safe only while it is its cluster's one node. So before it builds
//! anything it takes the `tokeirad-owner` row of `tokeira_control_lease`, as
//! the embedded engine takes `embedded-owner`: a claim held on the database's
//! clock for [`OWNER_LEASE_DURATION`] and renewed every [`OWNER_RENEW_INTERVAL`].
//! While another owner's hold is live, the node tries again for
//! [`ACQUIRE_WINDOW`] and then refuses to start. Once it holds the claim, its
//! DSQL connection director admits work only while the claim is confirmed.
//!
//! The decisions are pure functions over a monotonic clock and the control-lease
//! repository's outcomes, [`after_attempt`] and [`Hold`], so their properties
//! run offline. [`acquire`] and the task [`AcquiredClaim::start`] spawns apply
//! them to the repository and the director's admission gate.

use std::{
    sync::{Arc, Mutex},
    time::{Duration as StdDuration, Instant},
};

use anyhow::{Context, Result, anyhow};
use time::OffsetDateTime;
use tokeira_storage::{
    ConnectionDirector, DbClass,
    dsql::{
        ConnectionControlLeaseRepository, ControlLeaseAcquireRequest, ControlLeaseClusterIdentity,
        ControlLeaseError, ControlLeaseGuard, DsqlConnectionDirector, DsqlPermit,
        OWNER_ADMISSION_MARGIN, OWNER_LEASE_DURATION, OWNER_RENEW_INTERVAL, OwnershipAdmissionGate,
        OwnershipAdmissionState,
    },
};
use tokeira_types::IncarnationId;
use tokio::{sync::watch, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// The claim's row in `tokeira_control_lease`.
pub(crate) const CLAIM_NAME: &str = "tokeirad-owner";
/// What the claim stores as its cluster's ID and ARN. A node knows its
/// endpoint, which can change, and not its cluster's ARN, and the row lives in
/// the cluster's own database, so the identity is a constant.
pub(crate) const CLAIM_CLUSTER_IDENTITY: &str = "tokeirad";
/// An owner is this prefix and the process's incarnation.
pub(crate) const OWNER_PREFIX: &str = "tokeirad-";
/// How long a node waits before it tries a held claim again.
pub(crate) const ACQUIRE_RETRY_INTERVAL: StdDuration = StdDuration::from_secs(5);
/// How long a node tries a held claim before it refuses to start: one hold
/// and one renewal interval.
pub(crate) const ACQUIRE_WINDOW: StdDuration = StdDuration::from_secs(80);
/// How long a stopping node waits for admitted work before it releases the
/// claim.
pub(crate) const RELEASE_DRAIN_TIMEOUT: StdDuration = StdDuration::from_secs(30);
/// How soon a failed renewal is tried again. The admission deadline still
/// decides when the claim counts as lost.
pub(crate) const RENEW_RETRY_INTERVAL: StdDuration = StdDuration::from_secs(2);
/// The longest one acquisition attempt may take, write-conflict retries
/// included.
const ATTEMPT_DEADLINE: StdDuration = StdDuration::from_secs(30);
/// The longest one renewal or the release may take. A renewal never runs past
/// the admission deadline either, so a hung connection can't keep the gate
/// open after it.
const REQUEST_TIMEOUT: StdDuration = StdDuration::from_secs(10);

/// The identity every node without a controller stores in the claim.
pub(crate) fn cluster_identity() -> ControlLeaseClusterIdentity {
    ControlLeaseClusterIdentity {
        cluster_id: CLAIM_CLUSTER_IDENTITY.to_owned(),
        cluster_arn: CLAIM_CLUSTER_IDENTITY.to_owned(),
    }
}

/// A new owner, for one process.
pub(crate) fn new_owner_id() -> String {
    format!("{OWNER_PREFIX}{}", IncarnationId::new())
}

/// One attempt to acquire the claim, as the repository answered it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Attempt {
    /// Another owner's hold is live until `expires_at`, on the database's clock.
    Busy {
        owner_id: String,
        expires_at: OffsetDateTime,
    },
    /// The claim is this node's. After an expired hold, it waits until
    /// `quiescence_deadline`.
    Acquired {
        quiescence_deadline: Option<Instant>,
    },
}

/// What a node does after an attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AcquireStep {
    /// Try again at this time.
    RetryAt(Instant),
    /// Refuse to start: the claim was held at every attempt for
    /// [`ACQUIRE_WINDOW`].
    Refuse {
        owner_id: String,
        expires_at: OffsetDateTime,
    },
    /// Go on at once: the claim had no owner, or its owner released it.
    Serve,
    /// Renew, and go on once the quiescence after an expired hold has passed.
    QuiesceUntil(Instant),
}

/// Decide what follows an attempt made at `attempted_at`, the node's first
/// having been made at `first_attempt` (2.1, 2.2, 2.4).
pub(crate) fn after_attempt(
    first_attempt: Instant,
    attempted_at: Instant,
    attempt: Attempt,
) -> AcquireStep {
    match attempt {
        Attempt::Busy {
            owner_id,
            expires_at,
        } => {
            let next = attempted_at + ACQUIRE_RETRY_INTERVAL;
            if next.saturating_duration_since(first_attempt) <= ACQUIRE_WINDOW {
                AcquireStep::RetryAt(next)
            } else {
                AcquireStep::Refuse {
                    owner_id,
                    expires_at,
                }
            }
        }
        Attempt::Acquired {
            quiescence_deadline: None,
        } => AcquireStep::Serve,
        Attempt::Acquired {
            quiescence_deadline: Some(deadline),
        } => AcquireStep::QuiesceUntil(deadline),
    }
}

/// The error a node refuses to start with while another node holds the claim
/// (2.2).
pub(crate) fn refusal(owner_id: &str, expires_at: OffsetDateTime) -> anyhow::Error {
    anyhow!(
        "tokeirad refuses to start: another tokeirad holds this DSQL cluster's single-node \
         claim (owner {owner_id}, held until {expires_at}). A DSQL cluster serves one tokeirad \
         without a placement controller: stop that node first, then start this one"
    )
}

/// Where a node's hold on the claim stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Phase {
    /// Acquired after an expired hold: renewing, with the gate closed until
    /// the quiescence has passed.
    Quiescing,
    /// Renewing, with the gate open.
    Serving,
    /// Stopping: renewing, with the gate closed while admitted work drains.
    Draining,
    /// The claim is lost: the gate admits nothing again, and the node drains
    /// and exits.
    Lost(Loss),
    /// The claim is released.
    Released,
    /// Renewals stopped, and the claim is left to expire.
    Abandoned,
}

/// How a claim was lost.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Loss {
    /// A renewal or the release found another owner.
    Fenced,
    /// No renewal was confirmed before the admission deadline.
    Unconfirmed,
}

/// What the node does to its admission gate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GateEffect {
    /// Leave it as it is.
    None,
    /// Open it: the quiescence has passed.
    Open,
    /// Close it: no new work, while admitted work drains.
    Close,
    /// Fence it for good.
    Fence,
}

/// What a stopping node does with its claim.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StopStep {
    /// Close the gate and let admitted work drain, renewing meanwhile, then
    /// release.
    Drain,
    /// Stop renewing and leave the claim to expire: the quiescence is still
    /// running.
    Abandon,
    /// Nothing to release: the claim is already lost, released or abandoned.
    Nothing,
}

/// What follows the drain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReleaseStep {
    /// Release the claim.
    Release,
    /// Stop renewing and leave the claim to expire: admitted work is still out.
    Abandon,
    /// Nothing to release.
    Nothing,
}

/// A node's hold on the claim: when it stops admitting work unless a renewal
/// is confirmed, when an expired takeover's quiescence ends, and its phase.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Hold {
    admission_deadline: Instant,
    quiescence_deadline: Option<Instant>,
    phase: Phase,
}

impl Hold {
    /// The hold an acquisition confirmed, with its admission deadline and,
    /// after an expired hold, its quiescence deadline.
    pub(crate) fn acquired(
        admission_deadline: Instant,
        quiescence_deadline: Option<Instant>,
    ) -> Self {
        Self {
            admission_deadline,
            quiescence_deadline,
            phase: if quiescence_deadline.is_some() {
                Phase::Quiescing
            } else {
                Phase::Serving
            },
        }
    }

    pub(crate) const fn phase(&self) -> Phase {
        self.phase
    }

    pub(crate) const fn admission_deadline(&self) -> Instant {
        self.admission_deadline
    }

    /// The quiescence deadline while the node waits it out.
    pub(crate) fn pending_quiescence(&self) -> Option<Instant> {
        match self.phase {
            Phase::Quiescing => self.quiescence_deadline,
            _ => None,
        }
    }

    /// Whether the node renews the claim.
    pub(crate) const fn renewing(&self) -> bool {
        matches!(
            self.phase,
            Phase::Quiescing | Phase::Serving | Phase::Draining
        )
    }

    /// Whether the gate admits work.
    #[cfg(test)]
    pub(crate) const fn gate_open(&self) -> bool {
        matches!(self.phase, Phase::Serving)
    }

    /// Time has reached `now`. The gate closes for good when the admission
    /// deadline has passed without a confirmed renewal (2.6), and opens once
    /// an expired takeover's quiescence has passed (2.4).
    pub(crate) fn tick(&mut self, now: Instant) -> GateEffect {
        if !self.renewing() {
            return GateEffect::None;
        }
        if now >= self.admission_deadline {
            self.phase = Phase::Lost(Loss::Unconfirmed);
            return GateEffect::Close;
        }
        if self.phase == Phase::Quiescing
            && self
                .quiescence_deadline
                .is_some_and(|deadline| now >= deadline)
        {
            self.phase = Phase::Serving;
            return GateEffect::Open;
        }
        GateEffect::None
    }

    /// A renewal confirmed at `confirmed_at` holds the claim until the new
    /// `admission_deadline` (2.5). A confirmation that arrives after the
    /// deadline has passed is too late: the gate has already closed.
    pub(crate) fn renewed(
        &mut self,
        confirmed_at: Instant,
        admission_deadline: Instant,
    ) -> GateEffect {
        let effect = self.tick(confirmed_at);
        if self.renewing() {
            self.admission_deadline = admission_deadline;
        }
        effect
    }

    /// A renewal or the release found another owner (2.6).
    pub(crate) fn fenced(&mut self) -> GateEffect {
        match self.phase {
            Phase::Quiescing | Phase::Serving | Phase::Draining => {
                self.phase = Phase::Lost(Loss::Fenced);
                GateEffect::Fence
            }
            Phase::Lost(_) | Phase::Released | Phase::Abandoned => GateEffect::None,
        }
    }

    /// The node stops at `now`, cleanly or after a failed start (2.7, 2.8).
    pub(crate) fn stop(&mut self, now: Instant) -> StopStep {
        if !matches!(self.phase, Phase::Quiescing | Phase::Serving) {
            return StopStep::Nothing;
        }
        if now >= self.admission_deadline {
            self.phase = Phase::Lost(Loss::Unconfirmed);
            return StopStep::Nothing;
        }
        if self.phase == Phase::Quiescing
            && self
                .quiescence_deadline
                .is_some_and(|deadline| now < deadline)
        {
            self.phase = Phase::Abandoned;
            return StopStep::Abandon;
        }
        self.phase = Phase::Draining;
        StopStep::Drain
    }

    /// The drain ended at `now`, with every admitted checkout returned or not
    /// (2.7).
    pub(crate) fn drained(&mut self, now: Instant, all_returned: bool) -> ReleaseStep {
        if self.phase != Phase::Draining {
            return ReleaseStep::Nothing;
        }
        if now >= self.admission_deadline {
            self.phase = Phase::Lost(Loss::Unconfirmed);
            return ReleaseStep::Nothing;
        }
        if all_returned {
            ReleaseStep::Release
        } else {
            self.phase = Phase::Abandoned;
            ReleaseStep::Abandon
        }
    }

    /// The release committed.
    pub(crate) fn released(&mut self) {
        if self.phase == Phase::Draining {
            self.phase = Phase::Released;
        }
    }

    /// The release failed for a reason other than another owner: the claim is
    /// left to expire.
    pub(crate) fn release_failed(&mut self) {
        if self.phase == Phase::Draining {
            self.phase = Phase::Abandoned;
        }
    }
}

/// A claim this node acquired, before its renewals start.
pub(crate) struct AcquiredClaim {
    repository: ConnectionControlLeaseRepository,
    guard: ControlLeaseGuard,
    gate: OwnershipAdmissionGate,
    hold: Hold,
}

impl std::fmt::Debug for AcquiredClaim {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AcquiredClaim")
            .field("guard", &self.guard)
            .field("hold", &self.hold)
            .finish_non_exhaustive()
    }
}

/// Acquire the claim through one control connection at a time from
/// `director`, before any gate is installed on it (2.1, 2.2). A busy claim is
/// tried again every [`ACQUIRE_RETRY_INTERVAL`] for [`ACQUIRE_WINDOW`], then
/// refused. The schema check has already found V067 applied, so this runs no
/// bootstrap DDL.
pub(crate) async fn acquire(director: &DsqlConnectionDirector) -> Result<AcquiredClaim> {
    let repository = ConnectionControlLeaseRepository::new();
    let owner_id = new_owner_id();
    let first_attempt = Instant::now();
    loop {
        let attempted_at = Instant::now();
        let result = tokio::time::timeout(ATTEMPT_DEADLINE, async {
            let mut permit = director
                .acquire(DbClass::Control)
                .await
                .context("failed to check out a DSQL connection for the single-node claim")?;
            Ok::<_, anyhow::Error>(
                repository
                    .acquire(
                        permit.connection()?,
                        &ControlLeaseAcquireRequest {
                            claim_name: CLAIM_NAME.to_owned(),
                            cluster: cluster_identity(),
                            owner_id: owner_id.clone(),
                            lease_duration: OWNER_LEASE_DURATION,
                            admission_margin: OWNER_ADMISSION_MARGIN,
                            acquire_deadline: attempted_at + ATTEMPT_DEADLINE,
                        },
                    )
                    .await,
            )
        })
        .await
        .map_err(|_| anyhow!("an attempt to take the single-node claim timed out"))??;
        let (attempt, guard) = match result {
            Ok(guard) => (
                Attempt::Acquired {
                    quiescence_deadline: guard.quiescence_deadline(),
                },
                Some(guard),
            ),
            Err(ControlLeaseError::Busy {
                owner_id,
                expires_at,
            }) => {
                info!(
                    holder = %owner_id,
                    %expires_at,
                    "the DSQL cluster's single-node claim is held"
                );
                (
                    Attempt::Busy {
                        owner_id,
                        expires_at,
                    },
                    None,
                )
            }
            Err(error) => {
                return Err(anyhow::Error::new(error))
                    .context("failed to take the DSQL cluster's single-node claim");
            }
        };
        match (after_attempt(first_attempt, attempted_at, attempt), guard) {
            (AcquireStep::RetryAt(at), _) => {
                tokio::time::sleep_until(tokio::time::Instant::from_std(at)).await;
            }
            (
                AcquireStep::Refuse {
                    owner_id,
                    expires_at,
                },
                _,
            ) => return Err(refusal(&owner_id, expires_at)),
            (step @ (AcquireStep::Serve | AcquireStep::QuiesceUntil(_)), Some(guard)) => {
                // The gate is closed while the quiescence runs, open otherwise.
                let quiescence_deadline = match step {
                    AcquireStep::QuiesceUntil(deadline) => Some(deadline),
                    _ => None,
                };
                let gate = OwnershipAdmissionGate::new(if quiescence_deadline.is_some() {
                    OwnershipAdmissionState::Closing
                } else {
                    OwnershipAdmissionState::Open
                });
                let hold = Hold::acquired(guard.local_admission_deadline(), quiescence_deadline);
                info!(
                    owner = %guard.owner_id(),
                    outcome = ?guard.outcome(),
                    fence_token = guard.fence_token(),
                    "took the DSQL cluster's single-node claim"
                );
                return Ok(AcquiredClaim {
                    repository,
                    guard,
                    gate,
                    hold,
                });
            }
            (step, None) => {
                return Err(anyhow!(
                    "the single-node claim's acquisition decided {step:?} without the claim"
                ));
            }
        }
    }
}

impl AcquiredClaim {
    /// The admission gate to install on the store's director: closed during
    /// an expired takeover's quiescence, open otherwise.
    pub(crate) fn gate(&self) -> OwnershipAdmissionGate {
        self.gate.clone()
    }

    /// Start renewing from acquisition onwards (2.5), returning the node's
    /// claim. The renewals and the release check out past the gate.
    pub(crate) fn start(self, director: Arc<DsqlConnectionDirector>) -> NodeClaim {
        let (phase_tx, phase_rx) = watch::channel(self.hold.phase());
        let stop = CancellationToken::new();
        let drain_on_loss = Arc::new(Mutex::new(None::<CancellationToken>));
        let gate = self.gate.clone();
        let task = tokio::spawn(run_claim(ClaimTask {
            director,
            repository: self.repository,
            guard: self.guard,
            gate: self.gate,
            hold: self.hold,
            stop: stop.clone(),
            phase: phase_tx,
            drain_on_loss: drain_on_loss.clone(),
        }));
        NodeClaim {
            gate,
            phase: phase_rx,
            stop,
            drain_on_loss,
            task: Some(task),
        }
    }
}

/// A held claim while its task renews it.
///
/// The task stops and releases the claim when [`NodeClaim::shut_down`] runs or
/// the handle drops. If the claim is lost, the task cancels the token given to
/// [`NodeClaim::drain_on_loss`], which starts the node's drain.
pub(crate) struct NodeClaim {
    gate: OwnershipAdmissionGate,
    phase: watch::Receiver<Phase>,
    stop: CancellationToken,
    drain_on_loss: Arc<Mutex<Option<CancellationToken>>>,
    task: Option<JoinHandle<Phase>>,
}

impl std::fmt::Debug for NodeClaim {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NodeClaim")
            .field("phase", &*self.phase.borrow())
            .field("gate", &self.gate)
            .finish_non_exhaustive()
    }
}

impl NodeClaim {
    /// The claim's phase now.
    pub(crate) fn phase(&self) -> Phase {
        *self.phase.borrow()
    }

    /// Wait until the gate opens: at once after a clean acquisition, after the
    /// quiescence after an expired one (2.4). An error means the claim was lost
    /// or given up during the wait.
    pub(crate) async fn wait_until_serving(&mut self) -> Result<()> {
        let phase = *self
            .phase
            .wait_for(|phase| *phase != Phase::Quiescing)
            .await
            .map_err(|_| anyhow!("the single-node claim's task ended during its quiescence"))?;
        match phase {
            Phase::Serving => Ok(()),
            Phase::Lost(loss) => Err(lost_error(loss)).context(
                "tokeirad lost its single-node claim while it waited out the previous holder's \
                 transactions",
            ),
            other => Err(anyhow!(
                "the single-node claim left its quiescence as {other:?}, not serving"
            )),
        }
    }

    /// Wait until the claim is lost.
    #[cfg(all(test, feature = "dsql-integration"))]
    pub(crate) async fn lost(&mut self) -> Loss {
        loop {
            if let Phase::Lost(loss) = *self.phase.borrow_and_update() {
                return loss;
            }
            if self.phase.changed().await.is_err() {
                if let Phase::Lost(loss) = *self.phase.borrow() {
                    return loss;
                }
                std::future::pending::<()>().await;
            }
        }
    }

    /// Cancel `token` when the claim is lost, to start the node's drain (2.6).
    /// The task publishes a loss before it reads the token, so a loss is never
    /// missed between the two.
    pub(crate) fn drain_on_loss(&self, token: CancellationToken) {
        *self
            .drain_on_loss
            .lock()
            .expect("single-node claim drain slot poisoned") = Some(token.clone());
        if matches!(self.phase(), Phase::Lost(_)) {
            token.cancel();
        }
    }

    /// Stop: close the gate, let admitted work drain for up to
    /// [`RELEASE_DRAIN_TIMEOUT`], renewing meanwhile, and release the claim,
    /// or leave it to expire while an expired takeover's quiescence is
    /// running or work is still out (2.7, 2.8). An error means the claim was
    /// lost.
    pub(crate) async fn shut_down(mut self) -> Result<()> {
        self.stop.cancel();
        let Some(task) = self.task.take() else {
            return Ok(());
        };
        match task
            .await
            .context("the single-node claim's task panicked")?
        {
            Phase::Lost(loss) => Err(lost_error(loss)),
            _ => Ok(()),
        }
    }

    /// Simulate the process dying: stop renewing without a release, and admit
    /// nothing more.
    #[cfg(all(test, feature = "dsql-integration"))]
    pub(crate) fn crash(mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
        self.gate.fence();
    }
}

impl Drop for NodeClaim {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

/// The error a node exits with when it has lost the claim (2.6).
fn lost_error(loss: Loss) -> anyhow::Error {
    match loss {
        Loss::Fenced => anyhow!(
            "tokeirad lost its single-node claim: another node took the DSQL cluster's claim, \
             so this node stopped admitting work and exited"
        ),
        Loss::Unconfirmed => anyhow!(
            "tokeirad lost its single-node claim: no renewal was confirmed within {} s, so this \
             node stopped admitting work and exited",
            (OWNER_LEASE_DURATION - OWNER_ADMISSION_MARGIN).whole_seconds()
        ),
    }
}

/// The control connection of one renewal or release. It is discarded unless
/// the request finishes: a request its timeout cancels may leave the
/// connection in the middle of an exchange with DSQL.
struct RequestConnection(Option<DsqlPermit>);

impl RequestConnection {
    fn permit(&mut self) -> Result<&mut DsqlPermit> {
        self.0
            .as_mut()
            .ok_or_else(|| anyhow!("the claim request's connection was already returned"))
    }

    /// The request finished: return the connection to the reservoir.
    fn finish(mut self) {
        self.0 = None;
    }
}

impl Drop for RequestConnection {
    fn drop(&mut self) {
        if let Some(permit) = self.0.as_mut() {
            permit.mark_bad();
        }
    }
}

/// Why a renewal or the release failed.
#[derive(Debug)]
enum RequestFailure {
    /// The claim has another owner.
    Fenced,
    /// Anything else: the claim's state is unknown.
    Other(anyhow::Error),
}

impl From<ControlLeaseError> for RequestFailure {
    fn from(error: ControlLeaseError) -> Self {
        match error {
            ControlLeaseError::Fenced => Self::Fenced,
            other => Self::Other(other.into()),
        }
    }
}

impl From<anyhow::Error> for RequestFailure {
    fn from(error: anyhow::Error) -> Self {
        Self::Other(error)
    }
}

/// What the renewal task owns.
struct ClaimTask {
    director: Arc<DsqlConnectionDirector>,
    repository: ConnectionControlLeaseRepository,
    guard: ControlLeaseGuard,
    gate: OwnershipAdmissionGate,
    hold: Hold,
    stop: CancellationToken,
    phase: watch::Sender<Phase>,
    drain_on_loss: Arc<Mutex<Option<CancellationToken>>>,
}

impl ClaimTask {
    /// Apply a gate effect the hold decided.
    fn apply(&mut self, effect: GateEffect) {
        match effect {
            GateEffect::None => {}
            GateEffect::Open => {
                if self
                    .gate
                    .finish_quiescence(&self.guard, Instant::now())
                    .is_err()
                {
                    // The gate was fenced meanwhile: the repository found
                    // another owner.
                    let _ = self.hold.fenced();
                    self.gate.fence();
                }
            }
            GateEffect::Close => self.gate.begin_closing(),
            GateEffect::Fence => self.gate.fence(),
        }
        self.publish();
    }

    fn publish(&self) {
        self.phase.send_replace(self.hold.phase());
    }

    /// Renew once, through a control connection past the gate, giving up at
    /// the admission deadline or after [`REQUEST_TIMEOUT`].
    async fn renew(&mut self) -> Result<(), RequestFailure> {
        let limit = self
            .hold
            .admission_deadline()
            .saturating_duration_since(Instant::now())
            .min(REQUEST_TIMEOUT);
        let Self {
            director,
            repository,
            guard,
            gate,
            ..
        } = self;
        tokio::time::timeout(limit, async {
            let mut held = RequestConnection(Some(director.acquire_shutdown_control().await?));
            let result = repository
                .renew(
                    held.permit()?.connection()?,
                    guard,
                    OWNER_LEASE_DURATION,
                    OWNER_ADMISSION_MARGIN,
                    gate,
                )
                .await;
            held.finish();
            result.map_err(RequestFailure::from)
        })
        .await
        .unwrap_or_else(|_| Err(RequestFailure::Other(anyhow!("the renewal timed out"))))
    }

    /// Release once, through a control connection past the gate, giving up
    /// after [`REQUEST_TIMEOUT`].
    async fn release(&mut self) -> Result<(), RequestFailure> {
        let Self {
            director,
            repository,
            guard,
            gate,
            ..
        } = self;
        tokio::time::timeout(REQUEST_TIMEOUT, async {
            let mut held = RequestConnection(Some(director.acquire_shutdown_control().await?));
            let result = repository
                .release(held.permit()?.connection()?, guard, gate)
                .await;
            held.finish();
            result.map_err(RequestFailure::from)
        })
        .await
        .unwrap_or_else(|_| Err(RequestFailure::Other(anyhow!("the release timed out"))))
    }

    fn start_drain_on_loss(&self) {
        if let Some(token) = self
            .drain_on_loss
            .lock()
            .expect("single-node claim drain slot poisoned")
            .as_ref()
        {
            token.cancel();
        }
    }
}

/// Renew the claim every [`OWNER_RENEW_INTERVAL`] from acquisition onwards,
/// open the gate after an expired takeover's quiescence, close it for good
/// when the claim is lost, and release the claim when the node stops.
async fn run_claim(mut task: ClaimTask) -> Phase {
    let renew_interval =
        StdDuration::try_from(OWNER_RENEW_INTERVAL).expect("positive owner renewal interval");
    let mut next_renewal = Instant::now() + renew_interval;
    let mut drain_started = None::<Instant>;
    loop {
        let effect = task.hold.tick(Instant::now());
        task.apply(effect);
        match task.hold.phase() {
            Phase::Lost(loss) => {
                error!(?loss, "tokeirad lost its single-node claim; draining");
                task.start_drain_on_loss();
                return task.hold.phase();
            }
            Phase::Released | Phase::Abandoned => return task.hold.phase(),
            Phase::Quiescing | Phase::Serving | Phase::Draining => {}
        }
        let wake = [
            Some(next_renewal),
            Some(task.hold.admission_deadline()),
            task.hold.pending_quiescence(),
            drain_started.map(|started| started + RELEASE_DRAIN_TIMEOUT),
        ]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(next_renewal);
        let draining = task.hold.phase() == Phase::Draining;
        let gate = task.gate.clone();
        tokio::select! {
            biased;
            () = task.stop.cancelled(), if drain_started.is_none() => {
                match task.hold.stop(Instant::now()) {
                    StopStep::Drain => {
                        task.apply(GateEffect::Close);
                        drain_started = Some(Instant::now());
                    }
                    StopStep::Abandon => {
                        warn!(
                            "tokeirad stopped during its single-node claim's quiescence; \
                             leaving the claim to expire"
                        );
                        task.publish();
                    }
                    StopStep::Nothing => task.publish(),
                }
            }
            () = gate.wait_for_drain(), if draining => {
                match task.hold.drained(Instant::now(), true) {
                    ReleaseStep::Release => match task.release().await {
                        Ok(()) => {
                            task.hold.released();
                            info!("released the DSQL cluster's single-node claim");
                        }
                        Err(RequestFailure::Fenced) => {
                            let effect = task.hold.fenced();
                            task.apply(effect);
                        }
                        Err(RequestFailure::Other(error)) => {
                            warn!(
                                error = %format!("{error:#}"),
                                "failed to release the single-node claim; leaving it to expire"
                            );
                            task.hold.release_failed();
                        }
                    },
                    ReleaseStep::Abandon | ReleaseStep::Nothing => {}
                }
                task.publish();
            }
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(wake)) => {
                let now = Instant::now();
                if draining && drain_started.is_some_and(|started| now >= started + RELEASE_DRAIN_TIMEOUT) {
                    if task.hold.drained(now, false) == ReleaseStep::Abandon {
                        warn!(
                            in_flight = task.gate.in_flight(),
                            "admitted DSQL work outlasted the drain; leaving the single-node \
                             claim to expire"
                        );
                    }
                    task.publish();
                } else if now >= next_renewal && task.hold.renewing() {
                    match task.renew().await {
                        Ok(()) => {
                            let confirmed_at = Instant::now();
                            let deadline = task.guard.local_admission_deadline();
                            let effect = task.hold.renewed(confirmed_at, deadline);
                            task.apply(effect);
                            next_renewal = confirmed_at + renew_interval;
                        }
                        Err(RequestFailure::Fenced) => {
                            let effect = task.hold.fenced();
                            task.apply(effect);
                        }
                        Err(RequestFailure::Other(error)) => {
                            warn!(
                                error = %format!("{error:#}"),
                                "failed to renew the single-node claim; trying again"
                            );
                            next_renewal = Instant::now() + RENEW_RETRY_INTERVAL;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    const HOLD: StdDuration = StdDuration::from_secs(60);
    const ADMISSION: StdDuration = StdDuration::from_secs(40);
    const QUIESCENCE: StdDuration = StdDuration::from_secs(300);

    fn at(base: Instant, millis: u64) -> Instant {
        base + StdDuration::from_millis(millis)
    }

    #[test]
    fn the_claim_is_named_and_owned_as_the_design_gives() {
        assert_eq!(CLAIM_NAME, "tokeirad-owner");
        assert_eq!(
            cluster_identity(),
            ControlLeaseClusterIdentity {
                cluster_id: "tokeirad".to_owned(),
                cluster_arn: "tokeirad".to_owned(),
            }
        );
        let owner = new_owner_id();
        assert!(owner.starts_with("tokeirad-"), "{owner}");
        assert_ne!(owner, new_owner_id(), "each process is its own owner");
        assert_eq!(
            StdDuration::try_from(OWNER_LEASE_DURATION + OWNER_RENEW_INTERVAL).unwrap(),
            ACQUIRE_WINDOW,
            "a node tries for one hold and one renewal interval"
        );
        assert_eq!(
            StdDuration::try_from(OWNER_LEASE_DURATION).unwrap(),
            HOLD,
            "the embedded engine's hold"
        );
        assert_eq!(
            StdDuration::try_from(OWNER_LEASE_DURATION - OWNER_ADMISSION_MARGIN).unwrap(),
            ADMISSION
        );
        assert_eq!(
            StdDuration::try_from(tokeira_storage::dsql::EXPIRED_TAKEOVER_QUIESCENCE).unwrap(),
            QUIESCENCE
        );
    }

    #[test]
    fn the_refusal_names_the_holder_the_expiry_and_what_to_do() {
        let expires_at = time::macros::datetime!(2030-01-02 03:04:05 UTC);
        let message = format!("{:#}", refusal("tokeirad-holder", expires_at));
        assert!(message.contains("tokeirad refuses to start"), "{message}");
        assert!(message.contains("tokeirad-holder"), "{message}");
        assert!(message.contains(&expires_at.to_string()), "{message}");
        assert!(message.contains("stop that node first"), "{message}");
    }

    /// A claim's state when a node starts, on one clock shared with the
    /// database's: `expires_at` is the hold's expiry in milliseconds.
    #[derive(Clone, Debug)]
    enum ClaimState {
        NoRow,
        Released,
        Live { expires_at: u64, renewed: bool },
        Expired,
    }

    fn claim_state() -> impl Strategy<Value = ClaimState> {
        prop_oneof![
            Just(ClaimState::NoRow),
            Just(ClaimState::Released),
            (1u64..=200_000, any::<bool>()).prop_map(|(expires_at, renewed)| ClaimState::Live {
                expires_at,
                renewed
            }),
            Just(ClaimState::Expired),
        ]
    }

    /// One attempt against the fake claim at `now` milliseconds, confirmed
    /// `reply` milliseconds later.
    fn attempt(state: &ClaimState, base: Instant, now: u64, reply: u64) -> Attempt {
        let holder_expiry = match state {
            ClaimState::Live { renewed: true, .. } => Some(u64::MAX),
            ClaimState::Live {
                expires_at,
                renewed: false,
            } => Some(*expires_at),
            ClaimState::NoRow | ClaimState::Released | ClaimState::Expired => None,
        };
        match holder_expiry {
            Some(expires_at) if expires_at > now => Attempt::Busy {
                owner_id: "tokeirad-holder".to_owned(),
                expires_at: OffsetDateTime::UNIX_EPOCH,
            },
            _ => {
                let expired = matches!(state, ClaimState::Expired | ClaimState::Live { .. });
                Attempt::Acquired {
                    quiescence_deadline: expired.then(|| at(base, now + reply) + QUIESCENCE),
                }
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        // Feature: single-node-claim, Property 1: A start follows the claim
        #[test]
        fn a_start_follows_the_claim(
            state in claim_state(),
            reply in 0u64..=2_000,
            work in 0u64..=500,
        ) {
            let base = Instant::now();
            let mut now = 0u64;
            let mut attempts = Vec::new();
            let step = loop {
                attempts.push(now);
                let step = after_attempt(at(base, 0), at(base, now), attempt(&state, base, now, reply));
                match step {
                    AcquireStep::RetryAt(next) => {
                        let next = u64::try_from(next.duration_since(base).as_millis()).unwrap();
                        prop_assert_eq!(next, now + 5_000, "a busy claim is tried again 5 s later");
                        // An attempt takes `work`, which never moves the next one later.
                        prop_assert!(next >= now + work.min(5_000));
                        now = next;
                    }
                    step => break step,
                }
            };
            // The hold an acquisition starts with admits work only once any
            // quiescence has passed.
            let quiescence = match &step {
                AcquireStep::QuiesceUntil(deadline) => Some(*deadline),
                _ => None,
            };
            if matches!(step, AcquireStep::Serve | AcquireStep::QuiesceUntil(_)) {
                let taken_at = attempts.last().copied().unwrap() + reply;
                let hold = Hold::acquired(at(base, taken_at) + ADMISSION, quiescence);
                prop_assert_eq!(hold.gate_open(), quiescence.is_none());
            }
            match &state {
                ClaimState::NoRow | ClaimState::Released => {
                    prop_assert_eq!(step, AcquireStep::Serve);
                    prop_assert_eq!(attempts.len(), 1, "a free claim is taken at once");
                }
                ClaimState::Expired => {
                    prop_assert_eq!(step, AcquireStep::QuiesceUntil(at(base, reply) + QUIESCENCE));
                }
                ClaimState::Live { renewed: true, .. } => {
                    prop_assert!(matches!(step, AcquireStep::Refuse { .. }), "{:?}", step);
                    prop_assert_eq!(*attempts.last().unwrap(), 80_000, "the node tries for 80 s");
                    prop_assert_eq!(attempts.len(), 17);
                }
                ClaimState::Live { expires_at, renewed: false } => {
                    if *expires_at <= 80_000 {
                        let taken_at = attempts.last().copied().unwrap();
                        prop_assert!(taken_at >= *expires_at, "taken only once the hold expired");
                        prop_assert!(taken_at < expires_at + 5_000, "taken at the first attempt after");
                        prop_assert_eq!(
                            step,
                            AcquireStep::QuiesceUntil(at(base, taken_at + reply) + QUIESCENCE),
                            "an expired hold is waited out"
                        );
                    } else {
                        prop_assert!(matches!(step, AcquireStep::Refuse { .. }), "{:?}", step);
                        prop_assert_eq!(*attempts.last().unwrap(), 80_000);
                    }
                }
            }
        }
    }

    /// One renewal's outcome in a generated history.
    #[derive(Clone, Copy, Debug)]
    enum Renewal {
        Confirmed { after: u64, round_trip: u64 },
        Failed { after: u64 },
        Fenced { after: u64 },
    }

    fn renewal() -> impl Strategy<Value = Renewal> {
        prop_oneof![
            6 => (1u64..=25_000, 0u64..=15_000)
                .prop_map(|(after, round_trip)| Renewal::Confirmed { after, round_trip }),
            3 => (1u64..=25_000).prop_map(|after| Renewal::Failed { after }),
            1 => (1u64..=25_000).prop_map(|after| Renewal::Fenced { after }),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        // Feature: single-node-claim, Property 3: A lost claim stops the node
        #[test]
        fn a_lost_claim_stops_the_node(
            quiescing in any::<bool>(),
            renewals in proptest::collection::vec(renewal(), 0..40),
        ) {
            let base = Instant::now();
            let mut hold = Hold::acquired(
                at(base, 40_000),
                quiescing.then(|| at(base, 300_000)),
            );
            let mut gate_open = hold.gate_open();
            let mut last_confirmed = 0u64;
            let mut lost_at = None::<u64>;
            let mut now = 0u64;
            for renewal in renewals {
                let (after, outcome) = match renewal {
                    Renewal::Confirmed { after, round_trip } => (after, Some(Ok(round_trip))),
                    Renewal::Failed { after } => (after, None),
                    Renewal::Fenced { after } => (after, Some(Err(()))),
                };
                // Time passes in ticks of a second, as the task wakes.
                let target = now + after;
                while now < target {
                    now = (now + 1_000).min(target);
                    match hold.tick(at(base, now)) {
                        GateEffect::Open => gate_open = true,
                        GateEffect::Close | GateEffect::Fence => gate_open = false,
                        GateEffect::None => {}
                    }
                    if lost_at.is_none() && now >= last_confirmed + 40_000 {
                        lost_at = Some(now);
                    }
                }
                if !hold.renewing() {
                    continue;
                }
                match outcome {
                    Some(Ok(round_trip)) => {
                        let confirmed = now + round_trip;
                        let effect = hold.renewed(at(base, confirmed), at(base, confirmed + 40_000));
                        now = confirmed;
                        match effect {
                            GateEffect::Open => gate_open = true,
                            GateEffect::Close | GateEffect::Fence => gate_open = false,
                            GateEffect::None => {}
                        }
                        if lost_at.is_none() && confirmed >= last_confirmed + 40_000 {
                            lost_at = Some(confirmed);
                        } else if lost_at.is_none() {
                            last_confirmed = confirmed;
                        }
                    }
                    Some(Err(())) => {
                        if hold.fenced() == GateEffect::Fence {
                            gate_open = false;
                        }
                        lost_at.get_or_insert(now);
                    }
                    None => {}
                }
                if let Some(lost) = lost_at {
                    prop_assert!(!gate_open, "the gate admits nothing once the claim is lost at {lost}");
                    prop_assert!(matches!(hold.phase(), Phase::Lost(_)), "{:?}", hold.phase());
                    prop_assert!(!hold.renewing(), "a lost claim is not renewed again");
                    prop_assert_eq!(hold.stop(at(base, now)), StopStep::Nothing, "and never released");
                } else {
                    prop_assert!(hold.renewing());
                    let quiescence_passed = !quiescing || now >= 300_000;
                    if quiescence_passed {
                        let _ = hold.tick(at(base, now));
                        prop_assert!(hold.gate_open(), "a confirmed claim keeps the gate open");
                    } else {
                        prop_assert!(!hold.gate_open(), "the gate stays shut during the quiescence");
                    }
                }
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        // Feature: single-node-claim, Property 4: A release never shortens a wait
        #[test]
        fn a_release_never_shortens_a_wait(
            quiescing in any::<bool>(),
            stop_at in 0u64..=600_000,
            drain in 0u64..=40_000,
            returned in any::<bool>(),
        ) {
            let base = Instant::now();
            let mut hold = Hold::acquired(
                at(base, 40_000),
                quiescing.then(|| at(base, 300_000)),
            );
            // Renewals confirm every 20 s until the node stops.
            let mut now = 0;
            let mut gate_opened = !quiescing;
            while now + 20_000 <= stop_at {
                now += 20_000;
                if hold.renewed(at(base, now), at(base, now + 40_000)) == GateEffect::Open {
                    gate_opened = true;
                }
            }
            if hold.tick(at(base, stop_at)) == GateEffect::Open {
                gate_opened = true;
            }
            let step = hold.stop(at(base, stop_at));
            let quiescence_running = quiescing && stop_at < 300_000;
            if quiescence_running {
                prop_assert_eq!(step, StopStep::Abandon, "a running quiescence leaves the claim to expire");
                prop_assert_eq!(hold.phase(), Phase::Abandoned);
                prop_assert!(!gate_opened);
                prop_assert_eq!(hold.drained(at(base, stop_at + drain), returned), ReleaseStep::Nothing);
                return Ok(());
            }
            prop_assert_eq!(step, StopStep::Drain);
            prop_assert!(!hold.gate_open(), "the gate closes before the drain");
            // The drain ends at its timeout unless everything returned sooner,
            // with renewals continuing meanwhile.
            let ended = stop_at + drain.min(30_000);
            let mut renewed_until = now;
            while renewed_until + 20_000 <= ended {
                renewed_until += 20_000;
                let _ = hold.renewed(at(base, renewed_until), at(base, renewed_until + 40_000));
            }
            let all_returned = returned && drain <= 30_000;
            let release = hold.drained(at(base, ended), all_returned);
            if all_returned {
                prop_assert_eq!(release, ReleaseStep::Release, "a drained node releases");
            } else {
                prop_assert_eq!(release, ReleaseStep::Abandon, "work still out leaves the claim to expire");
                prop_assert_eq!(hold.phase(), Phase::Abandoned);
            }
        }
    }

    /// A node's run in the two-node schedule.
    #[derive(Clone, Copy, Debug)]
    struct Run {
        start: u64,
        end: RunEnd,
    }

    #[derive(Clone, Copy, Debug)]
    enum RunEnd {
        Crash(u64),
        Shutdown(u64),
        Never,
    }

    /// What one node does in the two-node schedule, in milliseconds of true
    /// time. Its requests take up to `outbound` to reach the database and up
    /// to `inbound` to come back, and its clock runs `drift_ppm` fast or slow.
    #[derive(Clone, Debug)]
    struct NodeScript {
        runs: Vec<Run>,
        partitions: Vec<(u64, u64)>,
        outbound: u64,
        inbound: u64,
        drift_ppm: i64,
        seed: u64,
    }

    fn node_script(
        first_start: std::ops::RangeInclusive<u64>,
    ) -> impl Strategy<Value = NodeScript> {
        let run_end = prop_oneof![
            (1_000u64..=600_000).prop_map(RunEnd::Crash),
            (1_000u64..=600_000).prop_map(RunEnd::Shutdown),
            Just(RunEnd::Never),
        ];
        (
            first_start,
            run_end.clone(),
            proptest::option::of((0u64..=400_000, run_end)),
            proptest::collection::vec((0u64..=1_200_000, 1_000u64..=90_000), 0..=2),
            1u64..=70,
            1u64..=70,
            -5_000i64..=5_000,
            any::<u64>(),
        )
            .prop_map(
                |(start, end, second, partitions, outbound, inbound, drift_ppm, seed)| {
                    let mut runs = vec![Run { start, end }];
                    if let Some((gap, second_end)) = second {
                        match end {
                            RunEnd::Crash(after) | RunEnd::Shutdown(after) => runs.push(Run {
                                start: start + after + gap,
                                end: second_end,
                            }),
                            RunEnd::Never => {}
                        }
                    }
                    NodeScript {
                        runs,
                        partitions: partitions
                            .into_iter()
                            .map(|(from, length)| (from, from + length))
                            .collect(),
                        // From a tenth of a second to 7 s each way, so that a
                        // round trip and the drift stay within the margin.
                        outbound: outbound * 100,
                        inbound: inbound * 100,
                        drift_ppm,
                        seed,
                    }
                },
            )
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Request {
        Acquire,
        Renew,
        Release,
    }

    #[derive(Clone, Copy, Debug)]
    enum Answer {
        Busy,
        Acquired { clean: bool, fence: u64 },
        Renewed,
        Released,
        Fenced,
    }

    #[derive(Clone, Copy, Debug)]
    struct InFlight {
        request: Request,
        arrives: Option<u64>,
        replies: u64,
        answer: Option<Answer>,
    }

    #[derive(Debug, Default)]
    enum NodeState {
        #[default]
        Idle,
        Acquiring {
            first: u64,
            next: u64,
        },
        Holding {
            hold: Hold,
            fence: u64,
            next_renewal: u64,
            takeover_at: Option<u64>,
            stopped: bool,
        },
        Done,
    }

    #[derive(Debug)]
    struct SimNode {
        script: NodeScript,
        run: usize,
        state: NodeState,
        in_flight: Option<InFlight>,
        open_since: Option<u64>,
        rng: u64,
    }

    /// Two nodes on one claim, with the database's semantics of
    /// `ConnectionControlLeaseRepository` on one true clock.
    #[derive(Debug)]
    struct Sim {
        base: Instant,
        owner: Option<(usize, usize)>,
        fence: u64,
        expires_at: u64,
        nodes: Vec<SimNode>,
        /// When any gate last closed, in true time.
        last_closed: Option<u64>,
    }

    impl Sim {
        fn new(a: NodeScript, b: NodeScript) -> Self {
            Self {
                base: Instant::now(),
                owner: None,
                fence: 0,
                expires_at: 0,
                nodes: [a, b]
                    .into_iter()
                    .map(|script| SimNode {
                        rng: script.seed,
                        script,
                        run: 0,
                        state: NodeState::Idle,
                        in_flight: None,
                        open_since: None,
                    })
                    .collect(),
                last_closed: None,
            }
        }

        /// Run 25 minutes of true time in steps of 100 ms, with at most one gate
        /// open at every step.
        fn run(&mut self) -> Result<(), TestCaseError> {
            let mut now = 0u64;
            while now <= 1_500_000 {
                self.step_node(0, now)?;
                self.step_node(1, now)?;
                prop_assert!(
                    !(self.nodes[0].open_since.is_some() && self.nodes[1].open_since.is_some()),
                    "both gates are open at {}",
                    now
                );
                now += 100;
            }
            Ok(())
        }

        fn local(&self, index: usize, now: u64) -> u64 {
            let drift =
                i128::from(now) * i128::from(self.nodes[index].script.drift_ppm) / 1_000_000;
            u64::try_from(i128::from(now) + drift).unwrap()
        }

        fn local_instant(&self, index: usize, now: u64) -> Instant {
            at(self.base, self.local(index, now))
        }

        fn partitioned(&self, index: usize, now: u64) -> bool {
            self.nodes[index]
                .script
                .partitions
                .iter()
                .any(|(from, to)| (*from..*to).contains(&now))
        }

        /// A delay from one step to `bound`, so that a request sent on one step
        /// arrives on a later one.
        fn delay(&mut self, index: usize, bound: u64) -> u64 {
            let node = &mut self.nodes[index];
            node.rng = node
                .rng
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (node.rng >> 33) % (bound / 100) * 100 + 100
        }

        fn close_gate(&mut self, index: usize, now: u64) {
            if self.nodes[index].open_since.take().is_some() {
                self.last_closed = Some(now);
            }
        }

        fn open_gate(
            &mut self,
            index: usize,
            now: u64,
            takeover_at: Option<u64>,
        ) -> Result<(), TestCaseError> {
            // Every close counts, one after the takeover included: a holder whose local
            // deadline outlives the database's expiry closes late, and that is the
            // close the quiescence guards against.
            if let (Some(acquired_at), Some(closed)) = (takeover_at, self.last_closed) {
                prop_assert!(
                    now >= closed + 300_000,
                    "after an expired takeover at {}, a gate opened at {}, within 5 minutes of a close at {}",
                    acquired_at,
                    now,
                    closed
                );
            }
            self.nodes[index].open_since = Some(now);
            Ok(())
        }

        fn apply(
            &mut self,
            index: usize,
            now: u64,
            effect: GateEffect,
            takeover_at: Option<u64>,
        ) -> Result<(), TestCaseError> {
            match effect {
                GateEffect::None => Ok(()),
                GateEffect::Open => self.open_gate(index, now, takeover_at),
                GateEffect::Close | GateEffect::Fence => {
                    self.close_gate(index, now);
                    Ok(())
                }
            }
        }

        fn database(&mut self, index: usize, request: Request, now: u64) -> Answer {
            let me = (index, self.nodes[index].run);
            let fence = match self.nodes[index].state {
                NodeState::Holding { fence, .. } => Some(fence),
                _ => None,
            };
            match request {
                Request::Acquire => {
                    if self.owner.is_some() && self.expires_at > now {
                        Answer::Busy
                    } else {
                        let clean = self.owner.is_none();
                        self.fence += 1;
                        self.owner = Some(me);
                        self.expires_at = now + 60_000;
                        Answer::Acquired {
                            clean,
                            fence: self.fence,
                        }
                    }
                }
                Request::Renew => {
                    if self.owner == Some(me) && fence == Some(self.fence) && self.expires_at > now
                    {
                        self.expires_at = now + 60_000;
                        Answer::Renewed
                    } else {
                        Answer::Fenced
                    }
                }
                Request::Release => {
                    if self.owner == Some(me) && fence == Some(self.fence) {
                        self.owner = None;
                        self.expires_at = now;
                        Answer::Released
                    } else {
                        Answer::Fenced
                    }
                }
            }
        }

        fn reply(
            &mut self,
            index: usize,
            now: u64,
            request: Request,
            answer: Option<Answer>,
        ) -> Result<(), TestCaseError> {
            let local_now = self.local_instant(index, now);
            let local_ms = self.local(index, now);
            let mut state = std::mem::take(&mut self.nodes[index].state);
            match (&mut state, request) {
                (NodeState::Acquiring { first, next }, Request::Acquire) => {
                    let attempt = match answer {
                        Some(Answer::Acquired { clean, .. }) => Attempt::Acquired {
                            quiescence_deadline: (!clean).then(|| local_now + QUIESCENCE),
                        },
                        _ => Attempt::Busy {
                            owner_id: String::new(),
                            expires_at: OffsetDateTime::UNIX_EPOCH,
                        },
                    };
                    match after_attempt(at(self.base, *first), at(self.base, *next), attempt) {
                        AcquireStep::RetryAt(retry) => {
                            *next =
                                u64::try_from(retry.duration_since(self.base).as_millis()).unwrap();
                        }
                        AcquireStep::Refuse { .. } => state = NodeState::Done,
                        step @ (AcquireStep::Serve | AcquireStep::QuiesceUntil(_)) => {
                            let Some(Answer::Acquired { clean, fence }) = answer else {
                                return Err(TestCaseError::fail("acquired without an acquisition"));
                            };
                            // As `acquire` does, the hold follows the decision.
                            let quiescence = match step {
                                AcquireStep::QuiesceUntil(deadline) => Some(deadline),
                                _ => None,
                            };
                            let hold = Hold::acquired(local_now + ADMISSION, quiescence);
                            let takeover_at = (!clean).then_some(now);
                            if hold.gate_open() {
                                self.open_gate(index, now, takeover_at)?;
                            }
                            state = NodeState::Holding {
                                hold,
                                fence,
                                next_renewal: local_ms + 20_000,
                                takeover_at,
                                stopped: false,
                            };
                        }
                    }
                }
                (
                    NodeState::Holding {
                        hold,
                        next_renewal,
                        takeover_at,
                        ..
                    },
                    Request::Renew,
                ) => {
                    let effect = match answer {
                        Some(Answer::Renewed) => {
                            *next_renewal = local_ms + 20_000;
                            hold.renewed(local_now, local_now + ADMISSION)
                        }
                        Some(Answer::Fenced) => hold.fenced(),
                        _ => {
                            *next_renewal = local_ms + 2_000;
                            GateEffect::None
                        }
                    };
                    let takeover_at = *takeover_at;
                    self.apply(index, now, effect, takeover_at)?;
                }
                (NodeState::Holding { hold, .. }, Request::Release) => match answer {
                    Some(Answer::Released) => hold.released(),
                    Some(Answer::Fenced) => {
                        let _ = hold.fenced();
                    }
                    _ => hold.release_failed(),
                },
                _ => {}
            }
            self.nodes[index].state = state;
            Ok(())
        }

        fn step_node(&mut self, index: usize, now: u64) -> Result<(), TestCaseError> {
            let Some(run) = self.nodes[index]
                .script
                .runs
                .get(self.nodes[index].run)
                .copied()
            else {
                return Ok(());
            };
            if matches!(self.nodes[index].state, NodeState::Idle) {
                if now < run.start {
                    return Ok(());
                }
                let first = self.local(index, now);
                self.nodes[index].state = NodeState::Acquiring { first, next: first };
            }
            let crashed = matches!(run.end, RunEnd::Crash(after) if now >= run.start + after);
            let stopping = matches!(run.end, RunEnd::Shutdown(after) if now >= run.start + after);
            if crashed && !matches!(self.nodes[index].state, NodeState::Done) {
                // A crashed process admits nothing more and releases nothing.
                self.close_gate(index, now);
                self.nodes[index].state = NodeState::Done;
                self.nodes[index].in_flight = None;
            }
            if let Some(flight) = self.nodes[index].in_flight
                && flight.arrives == Some(now)
            {
                let answer = self.database(index, flight.request, now);
                if let Some(flight) = self.nodes[index].in_flight.as_mut() {
                    flight.answer = Some(answer);
                }
            }
            if let Some(flight) = self.nodes[index].in_flight
                && flight.replies == now
            {
                self.nodes[index].in_flight = None;
                let answer = if self.partitioned(index, now) {
                    None
                } else {
                    flight.answer
                };
                self.reply(index, now, flight.request, answer)?;
            }
            self.advance(index, now, stopping)?;
            if matches!(self.nodes[index].state, NodeState::Done)
                && self.nodes[index].in_flight.is_none()
            {
                self.close_gate(index, now);
                self.nodes[index].run += 1;
                self.nodes[index].state = NodeState::Idle;
            }
            Ok(())
        }

        /// Tick, stop when scripted, and send the next request.
        fn advance(&mut self, index: usize, now: u64, stopping: bool) -> Result<(), TestCaseError> {
            let local_now = self.local_instant(index, now);
            let local_ms = self.local(index, now);
            let idle = self.nodes[index].in_flight.is_none();
            let mut state = std::mem::take(&mut self.nodes[index].state);
            let mut request = None;
            match &mut state {
                NodeState::Acquiring { next, .. } => {
                    if stopping {
                        state = NodeState::Done;
                    } else if idle && local_ms >= *next {
                        request = Some(Request::Acquire);
                    }
                }
                NodeState::Holding {
                    hold,
                    next_renewal,
                    takeover_at,
                    stopped,
                    ..
                } => {
                    let effect = hold.tick(local_now);
                    let takeover = *takeover_at;
                    self.apply(index, now, effect, takeover)?;
                    if stopping && !*stopped {
                        *stopped = true;
                        if hold.stop(local_now) == StopStep::Drain {
                            self.close_gate(index, now);
                        }
                    }
                    if hold.phase() == Phase::Draining {
                        // Admitted work drains at once; the release follows.
                        if idle && hold.drained(local_now, true) == ReleaseStep::Release {
                            request = Some(Request::Release);
                        }
                    } else if hold.renewing() && idle && local_ms >= *next_renewal {
                        request = Some(Request::Renew);
                    }
                    if !hold.renewing() && idle && request.is_none() {
                        self.close_gate(index, now);
                        state = NodeState::Done;
                    }
                }
                NodeState::Idle | NodeState::Done => {}
            }
            self.nodes[index].state = state;
            if let Some(request) = request {
                let partitioned = self.partitioned(index, now);
                let outbound = self.nodes[index].script.outbound;
                let inbound = self.nodes[index].script.inbound;
                let there = self.delay(index, outbound);
                let back = self.delay(index, inbound);
                self.nodes[index].in_flight = Some(InFlight {
                    request,
                    arrives: (!partitioned).then_some(now + there),
                    // A lost request fails after five seconds.
                    replies: if partitioned {
                        now + 5_000
                    } else {
                        now + there + back
                    },
                    answer: None,
                });
            }
            Ok(())
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(96))]

        // Feature: single-node-claim, Property 2: One node admits work at a time
        #[test]
        fn one_node_admits_work_at_a_time(
            a in node_script(0..=30_000),
            b in node_script(0..=600_000),
        ) {
            Sim::new(a, b).run()?;
        }
    }

    /// Property 2's quiescence check on a schedule the generator reaches only
    /// now and then: the holder is cut off from the database but keeps running,
    /// and the other node is retrying when the hold expires. The holder closes
    /// its gate at its admission deadline, before the hold expires, so the
    /// taker's quiescence outlasts that close.
    #[test]
    fn a_cut_off_holder_closes_before_its_claim_is_taken() {
        let script = |start, partitions| NodeScript {
            runs: vec![Run {
                start,
                end: RunEnd::Never,
            }],
            partitions,
            outbound: 100,
            inbound: 100,
            drift_ppm: 0,
            seed: 0,
        };
        let mut sim = Sim::new(
            script(0, vec![(100_000, 190_000)]),
            script(120_000, Vec::new()),
        );
        sim.run().unwrap();
        assert!(
            matches!(
                sim.nodes[1].state,
                NodeState::Holding {
                    takeover_at: Some(_),
                    ..
                }
            ) && sim.nodes[1].open_since.is_some(),
            "the second node takes the expired claim and opens its gate: {:?}",
            sim.nodes[1]
        );
    }
}
