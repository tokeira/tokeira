//! Warm raw DSQL connection reservoir.
//!
//! The reservoir owns physical `PgConnection`s. A caller first obtains an
//! operation-class permit, then checkout takes a warm connection, waiting up
//! to [`CHECKOUT_WAIT`] for one to be returned or created, or reports `Empty`
//! so the caller can shed work. Background tasks refill, scan and process
//! returns independently; checkout never connects or calls DynamoDB itself.
//!
//! The invariants that keep a node within its connection budget and keep
//! checkout from finding the reservoir empty while the class budgets still
//! admit work:
//!
//! - **A node holds no more live connections than its target.** Live
//!   connections are the ready ones, the ones checked out and the ones being
//!   created. The refiller takes a place in the `LiveCount` before it
//!   connects, as a `LiveCharge`; the charge travels with the attempt, then
//!   with the connection through the ready queue, the caller's permit and the
//!   return path, and gives its place back when it is dropped. No path
//!   releases a place by hand, so none can release one twice or forget to.
//! - **A returned connection always fits in the ready queue.** The queue holds
//!   the configured target; a directive can lower the target but never raise
//!   it, and ready connections are a subset of live ones.
//! - **A class permit travels with its connection.** It is released only once
//!   the return processor has put the connection back in the ready queue (or
//!   retired it). Releasing it first would let the next caller of that class
//!   find the queue empty while the connection it needs is still on its way
//!   back.
//! - **A connection's lifetime fits inside DSQL's connection duration.** DSQL
//!   checks a connection's IAM token only when it opens and closes it after 60
//!   minutes; lifetimes of 50 to 54 minutes plus the guard window leave room
//!   for a 5-minute transaction begun just before the guard window.
//! - The refiller makes up to the in-flight limit of connections at once, and
//!   a failing refill backs off exponentially, shared across every attempt.

use std::{
    error::Error as StdError,
    fmt,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicUsize, Ordering},
    },
    time::{Duration as StdDuration, Instant},
};

use anyhow::Result;
use async_channel::{TryRecvError, TrySendError};
use rand::Rng;
use sqlx::PgConnection;
use tokeira_observability::OutcomeLabel;
use tokio::{
    sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc},
    task::{JoinHandle, JoinSet},
};

use crate::metrics;

use super::{
    DistributedTokenBucket, MIN_TARGET, PhysicalConnectionFactory, ReservoirConfig,
    connection_factory::create_within_timeout, operation::BrokenFlag,
};

/// Default warm connection target for one node.
// Pinned here for reservoir startup wiring and tested as part of the DSQL connection-duration contract.
#[allow(dead_code)]
pub(crate) const TARGET_READY: usize = 50;
/// Maximum entries inspected by one scanner pass.
///
/// The scanner only inspects half of the target pool so a scan cannot drain the
/// channel and starve concurrent checkouts.
// Pinned here for reservoir startup wiring and tested as part of the DSQL connection-duration contract.
#[allow(dead_code)]
pub(crate) const SCAN_BUDGET: usize = TARGET_READY / 2;
/// Base age before a connection becomes eligible for retirement.
pub(crate) const BASE_LIFETIME: StdDuration = StdDuration::from_secs(50 * 60);
/// Most jitter added to [`BASE_LIFETIME`], spreading the retirement of
/// connections created together over four minutes.
pub(crate) const LIFETIME_JITTER: StdDuration = StdDuration::from_secs(4 * 60);
/// No connection is handed out this close to the end of its lifetime.
pub(crate) const GUARD_WINDOW: StdDuration = StdDuration::from_secs(45);
/// Maximum concurrent physical connection creation attempts.
// Pinned here for reservoir startup wiring and tested as part of the DSQL connection-duration contract.
#[allow(dead_code)]
pub(crate) const INFLIGHT_LIMIT: usize = 8;
pub(crate) const SCAN_INTERVAL: StdDuration = StdDuration::from_secs(1);
/// Longest a start waits for its reservoir to fill. A start that runs out of
/// time serves with the connections it holds while the refiller fills the
/// rest: a fleet started at once shares the cluster's connection burst and
/// then its rate, so a node may not fill within this time however healthy.
pub(crate) const WARMUP_TIMEOUT: StdDuration = StdDuration::from_secs(30);
/// How often warmup looks at the ready queue. Polling with a sleep, not a
/// yield, keeps warmup from spinning a runtime worker until the queue fills.
const WARMUP_POLL_INTERVAL: StdDuration = StdDuration::from_millis(10);
pub(crate) const REFILLER_IDLE_INTERVAL: StdDuration = StdDuration::from_millis(100);
/// Delay before the next refill attempt after the first failure in a row.
pub(crate) const REFILLER_ERROR_BACKOFF: StdDuration = StdDuration::from_millis(250);
/// Longest delay between refill attempts while they keep failing.
///
/// The delay doubles with each failure in a row, so a cluster that refuses
/// connections (too many, or too fast) sees this node's attempts thin out to
/// one every ten seconds; the first success restores full speed.
pub(crate) const REFILLER_MAX_BACKOFF: StdDuration = StdDuration::from_secs(10);
/// Longest checkout waits for a connection when none is ready.
///
/// A caller admitted by its class finds a connection unless one is between
/// retirement and its replacement: the scanner retired it, an operation broke
/// it, or the refiller cannot connect. A returned connection arrives within
/// microseconds and a replacement within one connect, a few hundred
/// milliseconds against a loaded cluster, so one second covers the gap with
/// room to spare while staying far below the deadlines clients set on their
/// calls. The storage layer does not see a request's deadline; a cancelled
/// request drops the wait.
pub const CHECKOUT_WAIT: StdDuration = StdDuration::from_secs(1);

/// The number of live connections a reservoir holds, against its target.
///
/// Live connections are the ready ones, the ones checked out and the ones
/// being created. Each holds one [`LiveCharge`], so the count changes only when
/// a charge is taken or dropped.
#[derive(Debug)]
pub(crate) struct LiveCount {
    count: AtomicUsize,
    target: AtomicUsize,
}

impl LiveCount {
    pub(crate) fn new(target: usize) -> Arc<Self> {
        metrics::set_dsql_reservoir_live_connections(0);
        Arc::new(Self {
            count: AtomicUsize::new(0),
            target: AtomicUsize::new(target),
        })
    }

    /// Take a place if the count is below the target.
    ///
    /// A compare-and-swap loop, so two creations can never both take the last
    /// place, and the count never passes a target that was not lowered under
    /// it.
    pub(crate) fn try_charge(self: &Arc<Self>) -> Option<LiveCharge> {
        let mut current = self.count.load(Ordering::Acquire);
        loop {
            if current >= self.target.load(Ordering::Acquire) {
                return None;
            }
            match self.count.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    metrics::set_dsql_reservoir_live_connections(current + 1);
                    return Some(LiveCharge {
                        live: Arc::clone(self),
                    });
                }
                Err(actual) => current = actual,
            }
        }
    }

    pub(crate) fn count(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }

    pub(crate) fn target(&self) -> usize {
        self.target.load(Ordering::Acquire)
    }

    fn set_target(&self, target: usize) {
        self.target.store(target, Ordering::Release);
    }

    /// Live connections above the current target, which the scanner and the
    /// return processor retire as they meet them. Positive only after a
    /// directive lowered the target under connections already live.
    pub(crate) fn excess(&self) -> usize {
        self.count().saturating_sub(self.target())
    }
}

/// One place in a reservoir's [`LiveCount`].
///
/// Taken by the refiller before a connect, never cloned, and given back
/// exactly once, when it is dropped — with a failed attempt, or with the
/// connection it travels with, wherever that connection is dropped.
#[derive(Debug)]
pub(crate) struct LiveCharge {
    live: Arc<LiveCount>,
}

impl Drop for LiveCharge {
    fn drop(&mut self) {
        let previous = self.live.count.fetch_sub(1, Ordering::AcqRel);
        metrics::set_dsql_reservoir_live_connections(previous.saturating_sub(1));
    }
}

#[derive(Debug)]
pub struct ReservoirEntry {
    /// The physical connection.
    pub(crate) connection: PgConnection,
    /// Creation time, used for lifetime retirement rather than checkout age.
    pub(crate) created_at: Instant,
    /// Per-connection lifetime after jitter was assigned at creation.
    pub(crate) max_lifetime: StdDuration,
    /// Set by a failed operation that left this connection unusable.
    pub(crate) broken: BrokenFlag,
    /// This connection's place in the live count, given back when the entry
    /// is dropped.
    pub(crate) live: LiveCharge,
}

impl ReservoirEntry {
    fn should_retire(&self, guard_window: StdDuration) -> bool {
        within_guard_window(self.created_at, self.max_lifetime, guard_window)
    }
}

#[derive(Debug)]
pub struct ReturnedConnection {
    /// Connection returned by a completed storage operation.
    pub(crate) entry: ReservoirEntry,
    /// Caller-set health flag. The return processor does not ping; bad-flagged
    /// connections are retired immediately.
    pub(crate) marked_bad: bool,
    /// The returning caller's class permit, released once the connection is
    /// back in the ready queue or retired.
    pub(crate) class_permit: Option<OwnedSemaphorePermit>,
}

#[derive(Debug)]
pub enum ReservoirError {
    Empty,
    Closed,
}

impl fmt::Display for ReservoirError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("DSQL reservoir is empty"),
            Self::Closed => formatter.write_str("DSQL reservoir is closed"),
        }
    }
}

impl StdError for ReservoirError {}

/// How far warmup filled the reservoir.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WarmupOutcome {
    /// The ready queue reached the target.
    Full { held: usize },
    /// The 30-second warmup passed with `held` of `target` connections ready.
    Partial { held: usize, target: usize },
}

#[derive(Debug)]
pub struct Reservoir {
    ready: async_channel::Receiver<ReservoirEntry>,
    return_tx: mpsc::UnboundedSender<ReturnedConnection>,
    live: Arc<LiveCount>,
    limiter: Arc<DistributedTokenBucket>,
    refiller_handle: JoinHandle<()>,
    scanner_handle: JoinHandle<()>,
    return_processor_handle: JoinHandle<()>,
    config: ReservoirConfig,
}

impl Reservoir {
    /// Start the reservoir's background tasks and wait for warmup.
    ///
    /// A warmup that times out does not fail the start: the reservoir serves
    /// with the connections it holds and the refiller keeps filling.
    pub async fn start(
        config: ReservoirConfig,
        factory: Arc<dyn PhysicalConnectionFactory>,
        limiter: Arc<DistributedTokenBucket>,
    ) -> Result<Self> {
        config.validate_distributed()?;
        // Ready connections are a subset of live ones, which never exceed the
        // configured target, so a return or a creation never waits for room.
        let (ready_tx, ready) = async_channel::bounded(config.target_ready);
        let (return_tx, return_rx) = mpsc::unbounded_channel();
        let inflight = Arc::new(Semaphore::new(config.inflight_limit));
        let live = LiveCount::new(config.target_ready);
        metrics::set_dsql_reservoir_target_connections(config.target_ready);
        metrics::set_dsql_reservoir_ready_connections(0);
        let refiller_handle = spawn_refiller(
            config.clone(),
            factory,
            ready_tx.clone(),
            Arc::clone(&limiter),
            inflight,
            Arc::clone(&live),
        );
        let scanner_handle = spawn_scanner(
            config.clone(),
            ready_tx.clone(),
            ready.clone(),
            Arc::clone(&live),
        );
        let return_processor_handle =
            spawn_return_processor(config.clone(), ready_tx, return_rx, Arc::clone(&live));
        let reservoir = Self {
            ready,
            return_tx,
            live,
            limiter,
            refiller_handle,
            scanner_handle,
            return_processor_handle,
            config,
        };
        if let WarmupOutcome::Partial { held, target } = reservoir.warmup().await {
            tracing::warn!(
                ready_connections = held,
                target_connections = target,
                warmup_timeout_seconds = WARMUP_TIMEOUT.as_secs(),
                "the DSQL connection reservoir did not fill within its warmup time; serving with \
                 the connections it holds while it keeps filling"
            );
        }
        Ok(reservoir)
    }

    /// Wait until the ready queue reaches the target, or the 30-second warmup
    /// passes, and report how far it filled.
    pub async fn warmup(&self) -> WarmupOutcome {
        let started = tokio::time::Instant::now();
        loop {
            let held = self.ready.len();
            let target = self.live.target();
            if held >= target {
                metrics::record_dsql_pool_connections_total(held);
                metrics::set_dsql_reservoir_ready_connections(held);
                return WarmupOutcome::Full { held };
            }
            if started.elapsed() > WARMUP_TIMEOUT {
                metrics::record_dsql_pool_connections_total(held);
                metrics::set_dsql_reservoir_ready_connections(held);
                return WarmupOutcome::Partial { held, target };
            }
            tokio::time::sleep(WARMUP_POLL_INTERVAL).await;
        }
    }

    /// Take a ready connection at once, or one that arrives within
    /// [`CHECKOUT_WAIT`], or report the reservoir empty.
    ///
    /// Waiting is the channel's `recv`, which is cancel-safe: dropping this
    /// future while it waits takes no connection out of the ready queue.
    pub async fn checkout(&self) -> Result<ReservoirEntry, ReservoirError> {
        let deadline = tokio::time::Instant::now() + CHECKOUT_WAIT;
        loop {
            // The fast path takes a ready connection without arming a timer.
            let entry = match self.ready.try_recv() {
                Ok(entry) => entry,
                Err(TryRecvError::Closed) => return Err(ReservoirError::Closed),
                Err(TryRecvError::Empty) => {
                    match tokio::time::timeout_at(deadline, self.ready.recv()).await {
                        Ok(Ok(entry)) => entry,
                        Ok(Err(_closed)) => return Err(ReservoirError::Closed),
                        Err(_elapsed) => {
                            metrics::record_dsql_pool_empty_reservoir();
                            return Err(ReservoirError::Empty);
                        }
                    }
                }
            };
            // An operation that failed on this connection flagged it after
            // returning it; it never reaches another caller.
            if entry.broken.is_set() {
                record_retirement("broken", entry.created_at.elapsed());
                continue;
            }
            metrics::record_dsql_pool_connections_total(self.ready.len());
            metrics::set_dsql_reservoir_ready_connections(self.ready.len());
            return Ok(entry);
        }
    }

    pub fn return_sender(&self) -> mpsc::UnboundedSender<ReturnedConnection> {
        self.return_tx.clone()
    }

    pub fn ready_count(&self) -> usize {
        self.ready.len()
    }

    /// Connections this reservoir holds: ready, checked out and being created.
    pub fn live_count(&self) -> usize {
        self.live.count()
    }

    /// Current target: the configured `target_ready` until a connection budget
    /// directive lowers it.
    pub fn target_ready(&self) -> usize {
        self.live.target()
    }

    /// The cluster-wide rate limiter this reservoir's refiller admits through.
    pub fn limiter(&self) -> &DistributedTokenBucket {
        &self.limiter
    }

    /// Set the target to `new_target`, at most the configured target and at
    /// least [`MIN_TARGET`], and retire idle connections above it now; returns
    /// how many were retired and the target applied.
    ///
    /// A connection checked out under the old target is retired as it comes
    /// back (the return processor's `budget_cap`), so a lower target never
    /// interrupts work. A directive can only lower the target: the cluster's
    /// share is a ceiling on this node, not a licence to exceed its
    /// configuration.
    pub fn reconfigure_target(&self, new_target: usize) -> (usize, usize) {
        let target = new_target.min(self.config.target_ready).max(MIN_TARGET);
        self.live.set_target(target);
        metrics::set_dsql_reservoir_target_connections(target);
        let mut retired = 0usize;
        while self.live.excess() > 0 {
            let Ok(entry) = self.ready.try_recv() else {
                break;
            };
            retired += 1;
            record_retirement("budget_cap", entry.created_at.elapsed());
        }
        metrics::record_dsql_pool_connections_total(self.ready.len());
        metrics::set_dsql_reservoir_ready_connections(self.ready.len());
        (retired, target)
    }

    /// Retire ready connections until `keep` remain, as a mass retirement
    /// would, so tests can watch the refill. Returns how many were retired.
    #[cfg(any(test, feature = "dsql-integration"))]
    pub fn retire_ready_for_tests(&self, keep: usize) -> usize {
        let mut retired = 0usize;
        while self.ready.len() > keep {
            let Ok(entry) = self.ready.try_recv() else {
                break;
            };
            retired += 1;
            record_retirement("guard_window", entry.created_at.elapsed());
        }
        retired
    }

    pub fn config(&self) -> &ReservoirConfig {
        &self.config
    }

    /// A reservoir with an empty ready queue and no refiller, scanner, or
    /// return processor, for tests of target and retirement bookkeeping.
    ///
    /// A physical `PgConnection` cannot be fabricated offline, so tests that
    /// need the queue populated must run against a live database instead.
    /// Must be called inside a Tokio runtime: the idle task that keeps the
    /// ready channel open is spawned on it.
    #[cfg(any(test, feature = "dsql-integration"))]
    pub fn idle_for_tests(config: ReservoirConfig) -> Self {
        let (mut reservoir, ready_tx) = Self::idle_with_ready_sender(config);
        // The idle refiller keeps the ready channel open until the reservoir
        // drops and aborts it.
        reservoir.refiller_handle.abort();
        reservoir.refiller_handle = tokio::spawn(async move {
            let _ready_tx = ready_tx;
            std::future::pending::<()>().await;
        });
        reservoir
    }

    /// An idle reservoir and the sender of its ready queue, so a test can
    /// make connections arrive when it chooses.
    #[cfg(any(test, feature = "dsql-integration"))]
    pub(crate) fn idle_with_ready_sender(
        config: ReservoirConfig,
    ) -> (Self, async_channel::Sender<ReservoirEntry>) {
        let (ready_tx, ready) = async_channel::bounded(config.target_ready);
        let (return_tx, _return_rx) = mpsc::unbounded_channel();
        let idle = || tokio::spawn(std::future::pending::<()>());
        let reservoir = Self {
            ready,
            return_tx,
            live: LiveCount::new(config.target_ready),
            limiter: Arc::new(DistributedTokenBucket::local_for_tests(1_000.0, 1_000)),
            refiller_handle: idle(),
            scanner_handle: idle(),
            return_processor_handle: idle(),
            config,
        };
        (reservoir, ready_tx)
    }

    /// Stop the background tasks. Connections still held drop with the
    /// reservoir and its callers' permits.
    pub async fn shutdown(&self) -> Result<()> {
        self.refiller_handle.abort();
        self.scanner_handle.abort();
        self.return_processor_handle.abort();
        Ok(())
    }
}

impl Drop for Reservoir {
    fn drop(&mut self) {
        self.refiller_handle.abort();
        self.scanner_handle.abort();
        self.return_processor_handle.abort();
    }
}

/// Keep the reservoir at its target with up to `inflight` creations at once.
fn spawn_refiller(
    config: ReservoirConfig,
    factory: Arc<dyn PhysicalConnectionFactory>,
    ready_tx: async_channel::Sender<ReservoirEntry>,
    limiter: Arc<DistributedTokenBucket>,
    inflight: Arc<Semaphore>,
    live: Arc<LiveCount>,
) -> JoinHandle<()> {
    tokio::spawn(refill(
        inflight,
        live,
        limiter,
        move |charge, refill_started| {
            create_into_ready(
                assign_lifetime(&config),
                Arc::clone(&factory),
                ready_tx.clone(),
                charge,
                refill_started,
            )
        },
    ))
}

/// The refiller's loop. `create` makes one connection under the charge it is
/// given and reports whether it connected; the fleet scenarios drive this
/// same loop with a simulated cluster.
///
/// One task decides, in this order: an in-flight permit, a place in the live
/// count, a cluster rate admission. Each admitted creation then runs as its own
/// task in a `JoinSet` this task owns, holding its in-flight permit throughout,
/// so aborting the refiller at shutdown cancels every creation it started, and
/// each cancelled creation's [`LiveCharge`] gives its place back. A connection
/// checked out is still live, so it is not replaced: the node never holds more
/// connections than its target, however many are in use.
///
/// Failures anywhere in a refill (a refused admission, a refused or timed-out
/// connect) count toward one shared streak, and every new attempt waits
/// [`refill_backoff`] of that streak first. The first created connection ends
/// the streak and cuts short a wait already under way, so a cluster that
/// recovers is refilled at once rather than after the longest delay.
pub(crate) async fn refill<C, F>(
    inflight: Arc<Semaphore>,
    live: Arc<LiveCount>,
    limiter: Arc<DistributedTokenBucket>,
    create: C,
) where
    C: Fn(LiveCharge, Instant) -> F,
    F: Future<Output = bool> + Send + 'static,
{
    let mut creating = JoinSet::new();
    let failures = Arc::new(AtomicU32::new(0));
    let recovered = Arc::new(Notify::new());
    loop {
        let streak = failures.load(Ordering::Acquire);
        if streak > 0 {
            let mut success = std::pin::pin!(recovered.notified());
            success.as_mut().enable();
            // Registered before the second look, so a success between the two
            // cannot be missed.
            if failures.load(Ordering::Acquire) > 0 {
                tokio::select! {
                    () = tokio::time::sleep(refill_backoff(streak)) => {}
                    () = success => {}
                }
            }
        }
        let Ok(inflight_permit) = inflight.clone().acquire_owned().await else {
            return;
        };
        while creating.try_join_next().is_some() {}
        let Some(charge) = live.try_charge() else {
            drop(inflight_permit);
            tokio::time::sleep(REFILLER_IDLE_INTERVAL).await;
            continue;
        };
        let refill_started = Instant::now();
        // The place is taken before the admission, so this node never spends
        // the cluster's rate on a connection it has no room for.
        if let Err(error) = limiter.admit().await {
            drop(charge);
            metrics::record_dsql_reservoir_refill_error("rate_limiter");
            metrics::record_dsql_reservoir_refill_duration(
                OutcomeLabel::Failure,
                refill_started.elapsed(),
            );
            tracing::warn!(error = %error, "the DSQL connection rate limiter refused a connection creation");
            failures.fetch_add(1, Ordering::AcqRel);
            continue;
        }
        let attempt = create(charge, refill_started);
        let failures = Arc::clone(&failures);
        let recovered = Arc::clone(&recovered);
        creating.spawn(async move {
            // Held for the whole attempt, so at most the in-flight limit of
            // connects run at once.
            let _inflight_permit = inflight_permit;
            if attempt.await {
                failures.store(0, Ordering::Release);
                recovered.notify_waiters();
            } else {
                failures.fetch_add(1, Ordering::AcqRel);
            }
        });
    }
}

/// Make one connection and hand it to the ready queue; reports whether it
/// connected. A failed, timed-out or cancelled attempt drops `live` and gives
/// its place back.
async fn create_into_ready(
    max_lifetime: StdDuration,
    factory: Arc<dyn PhysicalConnectionFactory>,
    ready_tx: async_channel::Sender<ReservoirEntry>,
    live: LiveCharge,
    refill_started: Instant,
) -> bool {
    let create_started = Instant::now();
    let connection = match create_within_timeout(factory.as_ref()).await {
        Ok(connection) => connection,
        Err(error) => {
            metrics::record_dsql_reservoir_connection_create_duration(create_started.elapsed());
            metrics::record_dsql_reservoir_refill_error("factory");
            metrics::record_dsql_reservoir_refill_duration(
                OutcomeLabel::Failure,
                refill_started.elapsed(),
            );
            metrics::record_dsql_connection_error(error.kind());
            tracing::warn!(error = %error, error_kind = error.kind(), "failed to create DSQL connection");
            return false;
        }
    };
    metrics::record_dsql_reservoir_connection_create_duration(create_started.elapsed());
    metrics::record_dsql_pool_connection_created();
    let entry = ReservoirEntry {
        connection,
        created_at: Instant::now(),
        max_lifetime,
        broken: BrokenFlag::default(),
        live,
    };
    // The queue always has room (see `Reservoir::start`), so this send never
    // waits. A closed queue means shutdown won the race after the connection
    // was made; the entry and its charge drop here.
    if ready_tx.send(entry).await.is_err() {
        metrics::record_dsql_reservoir_refill_error("ready_channel_closed");
        metrics::record_dsql_reservoir_refill_duration(
            OutcomeLabel::Failure,
            refill_started.elapsed(),
        );
        return true;
    }
    metrics::record_dsql_pool_connections_total(ready_tx.len());
    metrics::set_dsql_reservoir_ready_connections(ready_tx.len());
    metrics::record_dsql_reservoir_refill_duration(OutcomeLabel::Success, refill_started.elapsed());
    true
}

/// The delay before a refill attempt after `failures` failures in a row:
/// [`REFILLER_ERROR_BACKOFF`] doubled per failure, capped at
/// [`REFILLER_MAX_BACKOFF`], with equal jitter (half fixed, half random) so
/// nodes refused together do not retry together.
pub(crate) fn refill_backoff(failures: u32) -> StdDuration {
    let doublings = failures.saturating_sub(1).min(16);
    let ceiling = REFILLER_ERROR_BACKOFF
        .saturating_mul(1 << doublings)
        .min(REFILLER_MAX_BACKOFF);
    let half = ceiling / 2;
    let jitter_millis = u64::try_from(half.as_millis()).unwrap_or(u64::MAX);
    half + StdDuration::from_millis(rand::thread_rng().gen_range(0..=jitter_millis))
}

/// Retire ready connections that are broken, inside their guard window, or
/// above a lowered target, examining at most [`scan_budget`] entries a pass.
///
/// Every connection the scanner drops carries its [`LiveCharge`], so dropping
/// it gives its place back; the scanner never touches the count.
fn spawn_scanner(
    config: ReservoirConfig,
    ready_tx: async_channel::Sender<ReservoirEntry>,
    ready_rx: async_channel::Receiver<ReservoirEntry>,
    live: Arc<LiveCount>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let sleep_for = duration_or_default(config.scan_interval, SCAN_INTERVAL);
            tokio::time::sleep(sleep_for).await;

            let guard_window = duration_or_default(config.guard_window, GUARD_WINDOW);
            let batch_size = scan_budget(live.target());
            let mut scanned = 0;
            while scanned < batch_size {
                let Ok(entry) = ready_rx.try_recv() else {
                    break;
                };
                scanned += 1;
                let retirement = idle_retirement_reason(
                    entry.broken.is_set(),
                    entry.should_retire(guard_window),
                    live.excess() > 0,
                );
                if let Some(reason) = retirement {
                    record_retirement(reason, entry.created_at.elapsed());
                } else if let Err(error) = ready_tx.try_send(entry) {
                    // The queue cannot be full: the scanner took this entry
                    // out of it. Closed means shutdown; the entry drops.
                    drop(error.into_inner());
                    break;
                }
            }
            metrics::record_dsql_pool_connections_total(ready_tx.len());
            metrics::set_dsql_reservoir_ready_connections(ready_tx.len());
        }
    })
}

/// Put returned connections back in the ready queue, or retire them, and
/// release each caller's class permit last.
fn spawn_return_processor(
    config: ReservoirConfig,
    ready_tx: async_channel::Sender<ReservoirEntry>,
    mut return_rx: mpsc::UnboundedReceiver<ReturnedConnection>,
    live: Arc<LiveCount>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let guard_window = duration_or_default(config.guard_window, GUARD_WINDOW);
        while let Some(returned) = return_rx.recv().await {
            let ReturnedConnection {
                entry,
                marked_bad,
                class_permit,
            } = returned;
            let validate_started = Instant::now();
            let retirement = return_retirement_reason(
                marked_bad,
                entry.broken.is_set(),
                entry.should_retire(guard_window),
                live.excess() > 0,
            );
            metrics::record_dsql_reservoir_connection_validate_duration(validate_started.elapsed());
            if let Some(reason) = retirement {
                record_retirement(reason, entry.created_at.elapsed());
                // The connection's place goes back before its class permit, so
                // the refiller can start the replacement the next caller of
                // this class will wait for.
                drop(entry);
                drop(class_permit);
                continue;
            }
            metrics::record_dsql_pool_connection_returned();
            match ready_tx.try_send(entry) {
                Ok(()) => {}
                // Unreachable: ready connections are a subset of live ones,
                // which never exceed the queue's capacity. Retiring rather
                // than waiting keeps one stuck return from holding up every
                // return queued behind it, each with its caller's permit.
                Err(TrySendError::Full(entry)) => {
                    record_retirement("ready_channel_full", entry.created_at.elapsed());
                }
                Err(TrySendError::Closed(entry)) => {
                    record_retirement("return_channel_closed", entry.created_at.elapsed());
                    return;
                }
            }
            metrics::record_dsql_pool_connections_total(ready_tx.len());
            metrics::set_dsql_reservoir_ready_connections(ready_tx.len());
            // Only now may the next caller of this class take the permit: the
            // connection it will check out is already in the ready queue.
            drop(class_permit);
        }
    })
}

/// Why a returned connection retires instead of going back to the ready
/// queue, if it does. Validation reads only flags, the lifetime and the live
/// count, never the network (Req 6.3).
fn return_retirement_reason(
    marked_bad: bool,
    broken: bool,
    inside_guard_window: bool,
    over_target: bool,
) -> Option<&'static str> {
    if marked_bad {
        Some("bad_flag")
    } else {
        idle_retirement_reason(broken, inside_guard_window, over_target)
    }
}

/// Why a connection the reservoir holds retires, if it does: an operation
/// broke it, it is inside its guard window, or the node holds more live
/// connections than a lowered target allows.
fn idle_retirement_reason(
    broken: bool,
    inside_guard_window: bool,
    over_target: bool,
) -> Option<&'static str> {
    if broken {
        Some("broken")
    } else if inside_guard_window {
        Some("guard_window")
    } else if over_target {
        Some("budget_cap")
    } else {
        None
    }
}

/// Record a retirement. The caller drops the entry, whose charge gives its
/// place in the live count back.
fn record_retirement(reason: &'static str, age: StdDuration) {
    metrics::record_dsql_reservoir_connection_age(reason, age);
    metrics::record_dsql_pool_connection_retired(reason);
}

pub(crate) fn assign_lifetime(config: &ReservoirConfig) -> StdDuration {
    let base = duration_or_default(config.base_lifetime, BASE_LIFETIME);
    let jitter_max = duration_or_default(config.lifetime_jitter, LIFETIME_JITTER);
    if jitter_max.is_zero() {
        return base;
    }
    let jitter = rand::thread_rng().gen_range(0..=jitter_max.as_secs());
    base + StdDuration::from_secs(jitter)
}

pub(crate) fn within_guard_window(
    created_at: Instant,
    max_lifetime: StdDuration,
    guard_window: StdDuration,
) -> bool {
    // Saturating arithmetic treats already-expired connections as definitely
    // inside the guard window without panicking on clock skew or long pauses.
    created_at.elapsed().saturating_add(guard_window) >= max_lifetime
}

pub(crate) fn scan_budget(target_ready: usize) -> usize {
    (target_ready / 2).max(1)
}

fn duration_or_default(value: time::Duration, fallback: StdDuration) -> StdDuration {
    value.try_into().unwrap_or(fallback)
}

#[cfg(test)]
mod fleet_tests;
#[cfg(test)]
mod scenario_tests;

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use time::Duration;

    use super::*;
    use crate::dsql::{DSQL_MAX_CONNECTION_DURATION, DSQL_MAX_TRANSACTION};

    fn lifetimes(base_secs: u64, jitter_secs: u64, guard_secs: u64) -> ReservoirConfig {
        ReservoirConfig {
            target_ready: MIN_TARGET,
            inflight_limit: 1,
            base_lifetime: Duration::seconds(i64::try_from(base_secs).unwrap()),
            lifetime_jitter: Duration::seconds(i64::try_from(jitter_secs).unwrap()),
            guard_window: Duration::seconds(i64::try_from(guard_secs).unwrap()),
            scan_interval: Duration::seconds(1),
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        // Feature: dsql-reservoir-redesign, Property 1: Lifetime Jitter Bounds
        #[test]
        fn assigned_lifetimes_stay_within_the_jitter_bounds(_seed in any::<u64>()) {
            let lifetime = assign_lifetime(&ReservoirConfig::default());
            prop_assert!(lifetime >= BASE_LIFETIME);
            prop_assert!(lifetime <= BASE_LIFETIME + LIFETIME_JITTER);
            prop_assert!(lifetime >= StdDuration::from_secs(50 * 60));
            prop_assert!(lifetime <= StdDuration::from_secs(54 * 60));
        }

        #[test]
        fn assigned_lifetimes_follow_the_configuration(
            base_secs in 1u64..3_300,
            jitter_secs in 0u64..600,
            guard_secs in 1u64..120,
        ) {
            let config = lifetimes(base_secs, jitter_secs, guard_secs);
            prop_assume!(config.validate().is_ok());
            let lifetime = assign_lifetime(&config);
            prop_assert!(lifetime >= StdDuration::from_secs(base_secs));
            prop_assert!(lifetime <= StdDuration::from_secs(base_secs + jitter_secs));
        }

        // Feature: dsql-reservoir-redesign, Property 2: Lifetime Within DSQL's Connection Duration
        #[test]
        fn accepted_lifetimes_fit_dsqls_connection_duration(
            base_secs in 1u64..4_000,
            jitter_secs in 0u64..900,
            guard_secs in 1u64..300,
        ) {
            let config = lifetimes(base_secs, jitter_secs, guard_secs);
            let longest = Duration::seconds(i64::try_from(base_secs + jitter_secs + guard_secs).unwrap());
            let fits = longest + DSQL_MAX_TRANSACTION <= DSQL_MAX_CONNECTION_DURATION;
            prop_assert_eq!(config.validate().is_ok(), fits);
        }

        #[test]
        fn scan_budget_is_bounded_by_half_target(target_ready in 1usize..500) {
            let budget = scan_budget(target_ready);
            prop_assert!(budget <= target_ready.max(2) / 2);
            prop_assert!(budget >= 1);
        }
    }

    #[test]
    fn refill_backoff_doubles_from_the_initial_delay_up_to_the_cap() {
        for failures in 1..=64 {
            let ceiling = REFILLER_ERROR_BACKOFF
                .saturating_mul(1 << (failures - 1).min(16))
                .min(REFILLER_MAX_BACKOFF);
            for _ in 0..32 {
                let delay = refill_backoff(failures);
                assert!(delay >= ceiling / 2, "{failures}: {delay:?}");
                assert!(delay <= ceiling, "{failures}: {delay:?}");
            }
        }
        assert!(refill_backoff(1) <= REFILLER_ERROR_BACKOFF);
        assert!(refill_backoff(u32::MAX) >= REFILLER_MAX_BACKOFF / 2);
    }

    #[test]
    fn returns_retire_on_the_bad_flag_a_break_the_guard_window_or_a_lowered_target() {
        assert_eq!(return_retirement_reason(false, false, false, false), None);
        assert_eq!(
            return_retirement_reason(true, true, true, true),
            Some("bad_flag")
        );
        assert_eq!(
            return_retirement_reason(false, true, true, true),
            Some("broken")
        );
        assert_eq!(
            return_retirement_reason(false, false, true, true),
            Some("guard_window")
        );
        assert_eq!(
            return_retirement_reason(false, false, false, true),
            Some("budget_cap")
        );
    }

    #[test]
    fn default_internal_constants_fit_dsqls_connection_duration() {
        assert_eq!(TARGET_READY, 50);
        assert_eq!(SCAN_BUDGET, 25);
        assert_eq!(INFLIGHT_LIMIT, 8);
        assert_eq!(SCAN_INTERVAL, StdDuration::from_secs(1));
        assert_eq!(CHECKOUT_WAIT, StdDuration::from_secs(1));
        let longest = BASE_LIFETIME + LIFETIME_JITTER + GUARD_WINDOW;
        assert_eq!(longest, StdDuration::from_secs(54 * 60 + 45));
        assert!(
            longest + StdDuration::try_from(DSQL_MAX_TRANSACTION).unwrap()
                <= StdDuration::try_from(DSQL_MAX_CONNECTION_DURATION).unwrap()
        );
    }

    #[test]
    fn guard_window_enforces_retirement_boundary() {
        let created_at = Instant::now() - StdDuration::from_secs(100);
        assert!(within_guard_window(
            created_at,
            StdDuration::from_secs(120),
            StdDuration::from_secs(25)
        ));
        assert!(!within_guard_window(
            created_at,
            StdDuration::from_secs(140),
            StdDuration::from_secs(25)
        ));
    }

    #[tokio::test]
    async fn a_directive_lowers_the_target_never_raises_it_and_floors_at_one_per_class() {
        let reservoir = Reservoir::idle_for_tests(ReservoirConfig {
            target_ready: 8,
            ..ReservoirConfig::default()
        });
        assert_eq!(reservoir.target_ready(), 8);

        assert_eq!(reservoir.reconfigure_target(6), (0, 6));
        assert_eq!(reservoir.target_ready(), 6);
        assert_eq!(reservoir.config().target_ready, 8);

        assert_eq!(reservoir.reconfigure_target(100).1, 8);
        assert_eq!(reservoir.reconfigure_target(0).1, MIN_TARGET);
        assert_eq!(reservoir.target_ready(), MIN_TARGET);
    }

    #[tokio::test(start_paused = true)]
    async fn an_empty_reservoir_waits_one_checkout_wait_then_reports_empty() {
        let reservoir = Reservoir::idle_for_tests(ReservoirConfig::default());
        let started = tokio::time::Instant::now();
        assert!(matches!(
            reservoir.checkout().await,
            Err(ReservoirError::Empty)
        ));
        assert_eq!(started.elapsed(), CHECKOUT_WAIT);
    }

    #[derive(Clone, Debug)]
    enum LiveEvent {
        Charge,
        DropOne(usize),
        SetTarget(usize),
    }

    fn live_event() -> impl Strategy<Value = LiveEvent> {
        prop_oneof![
            4 => Just(LiveEvent::Charge),
            3 => any::<usize>().prop_map(LiveEvent::DropOne),
            1 => (MIN_TARGET..=40usize).prop_map(LiveEvent::SetTarget),
        ]
    }

    proptest! {
        // Feature: dsql-reservoir-redesign, Property 9: Live Connections Within the Target
        #[test]
        fn the_live_count_matches_the_charges_held_and_never_grows_past_the_target(
            configured in MIN_TARGET..=40usize,
            events in prop::collection::vec(live_event(), 1..400),
        ) {
            let live = LiveCount::new(configured);
            let mut held = Vec::new();
            let mut target = configured;
            for event in events {
                match event {
                    LiveEvent::Charge => {
                        let before = live.count();
                        match live.try_charge() {
                            Some(charge) => {
                                prop_assert!(before < target);
                                held.push(charge);
                            }
                            None => prop_assert!(before >= target),
                        }
                    }
                    LiveEvent::DropOne(index) if !held.is_empty() => {
                        held.swap_remove(index % held.len());
                    }
                    LiveEvent::DropOne(_) => {}
                    LiveEvent::SetTarget(new_target) => {
                        target = new_target.min(configured);
                        live.set_target(target);
                    }
                }
                prop_assert_eq!(live.count(), held.len());
                prop_assert_eq!(live.excess(), held.len().saturating_sub(target));
            }
            // Once every charge taken under an older target is dropped, the
            // count never passes the current one.
            held.clear();
            while let Some(charge) = live.try_charge() {
                held.push(charge);
            }
            prop_assert_eq!(live.count(), target);
        }
    }

    #[test]
    fn two_threads_never_both_take_the_last_place() {
        for _ in 0..200 {
            let live = LiveCount::new(MIN_TARGET);
            let taken: Vec<_> = std::thread::scope(|scope| {
                let workers: Vec<_> = (0..4)
                    .map(|_| {
                        let live = Arc::clone(&live);
                        scope.spawn(move || {
                            let mut held = Vec::new();
                            while let Some(charge) = live.try_charge() {
                                held.push(charge);
                            }
                            held
                        })
                    })
                    .collect();
                workers
                    .into_iter()
                    .flat_map(|worker| worker.join().unwrap())
                    .collect()
            });
            assert_eq!(taken.len(), MIN_TARGET);
            assert_eq!(live.count(), MIN_TARGET);
            drop(taken);
            assert_eq!(live.count(), 0);
        }
    }
}
