//! Warm raw DSQL connection reservoir.
//!
//! The reservoir owns physical `PgConnection`s. Callers do not wait here: they
//! first obtain an operation-class permit, then checkout either returns a warm
//! connection immediately or reports `Empty` so the caller can shed/backpressure
//! work. Background tasks refill, scan, and process returns independently.
//!
//! Four invariants keep checkout from reporting `Empty` while the class
//! budgets still admit work, without wasting connections:
//!
//! - A returned connection's class permit travels with it and is released
//!   only once the return processor has put the connection back in the ready
//!   queue (or retired it). Releasing the permit first would let the next
//!   caller of that class find the queue empty while the connection it needs
//!   is still on its way back.
//! - The ready queue has room for twice the target, which is every connection
//!   the node can hold: the refiller adds connections only while the ready
//!   ones plus those being made stay below the target, and the class budgets
//!   cap checked-out connections at the target. So a return never waits for
//!   room, and never retires a healthy connection that a later checkout would
//!   have to replace.
//! - The refiller makes up to the in-flight limit of connections at once, so a
//!   mass retirement refills in a fraction of the serial time.
//! - A failing refill backs off exponentially, shared across every attempt, so
//!   a cluster refusing connections is not hammered while it recovers.

use std::{
    error::Error as StdError,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicUsize, Ordering},
    },
    time::{Duration as StdDuration, Instant},
};

use anyhow::Result;
use async_channel::TrySendError;
use rand::Rng;
use sqlx::PgConnection;
use tokeira_observability::OutcomeLabel;
use tokio::{
    sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc},
    task::{JoinHandle, JoinSet},
};

use crate::metrics;

use super::{
    DistributedTokenBucket, PhysicalConnectionFactory, ReservoirConfig, SlotBlockManager,
    connection_factory::create_within_timeout, operation::BrokenFlag,
};

/// Default warm connection target for one node.
// Pinned here for reservoir startup wiring and tested as part of the DSQL token-safety contract.
#[allow(dead_code)]
pub(crate) const TARGET_READY: usize = 50;
/// Maximum entries inspected by one scanner pass.
///
/// The scanner only inspects half of the target pool so a scan cannot drain the
/// channel and starve concurrent checkouts.
// Pinned here for reservoir startup wiring and tested as part of the DSQL token-safety contract.
#[allow(dead_code)]
pub(crate) const SCAN_BUDGET: usize = TARGET_READY / 2;
/// Base age before a connection becomes eligible for retirement.
pub(crate) const BASE_LIFETIME: StdDuration = StdDuration::from_secs(10 * 60);
/// Positive jitter applied to spread retirements across time.
pub(crate) const LIFETIME_JITTER: StdDuration = StdDuration::from_secs(2 * 60);
/// Safety margin before the DSQL IAM token hard cutoff.
pub(crate) const GUARD_WINDOW: StdDuration = StdDuration::from_secs(45);
/// Maximum concurrent physical connection creation attempts.
// Pinned here for reservoir startup wiring and tested as part of the DSQL token-safety contract.
#[allow(dead_code)]
pub(crate) const INFLIGHT_LIMIT: usize = 8;
pub(crate) const SCAN_INTERVAL: StdDuration = StdDuration::from_secs(1);
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

#[derive(Debug)]
pub struct ReservoirEntry {
    /// The physical connection whose DSQL slot is currently reserved.
    pub(crate) connection: PgConnection,
    /// Creation time, used for lifetime retirement rather than checkout age.
    pub(crate) created_at: Instant,
    /// Per-connection lifetime after jitter was assigned at creation.
    pub(crate) max_lifetime: StdDuration,
    /// Set by a failed operation that left this connection unusable.
    pub(crate) broken: BrokenFlag,
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

#[derive(Debug)]
pub struct Reservoir {
    ready: async_channel::Receiver<ReservoirEntry>,
    return_tx: mpsc::UnboundedSender<ReturnedConnection>,
    target_ready: Arc<AtomicUsize>,
    slot_manager: Arc<SlotBlockManager>,
    refiller_handle: JoinHandle<()>,
    scanner_handle: JoinHandle<()>,
    return_processor_handle: JoinHandle<()>,
    config: ReservoirConfig,
}

impl Reservoir {
    /// Start the reservoir background tasks and block until warmup completes.
    ///
    /// Refiller ordering is correctness-sensitive: local in-flight permit,
    /// global slot reservation, distributed rate token, then physical
    /// connection creation. Any failure after slot reservation must release the
    /// slot exactly once.
    pub async fn start(
        config: ReservoirConfig,
        factory: Arc<dyn PhysicalConnectionFactory>,
        distributed_bucket: Arc<DistributedTokenBucket>,
        slot_manager: Arc<SlotBlockManager>,
    ) -> Result<Self> {
        config.validate()?;
        let (ready_tx, ready) = async_channel::bounded(ready_capacity(config.target_ready));
        let (return_tx, return_rx) = mpsc::unbounded_channel();
        let inflight = Arc::new(Semaphore::new(config.inflight_limit));
        let target_ready = Arc::new(AtomicUsize::new(config.target_ready));
        metrics::set_dsql_reservoir_target_connections(config.target_ready);
        metrics::set_dsql_reservoir_ready_connections(0);
        let refiller_handle = spawn_refiller(
            config.clone(),
            Arc::clone(&factory),
            ready_tx.clone(),
            Arc::clone(&distributed_bucket),
            Arc::clone(&slot_manager),
            Arc::clone(&inflight),
            Arc::clone(&target_ready),
        );
        let scanner_handle = spawn_scanner(
            config.clone(),
            ready_tx.clone(),
            ready.clone(),
            Arc::clone(&slot_manager),
            Arc::clone(&target_ready),
        );
        let return_processor_handle = spawn_return_processor(
            config.clone(),
            ready_tx,
            return_rx,
            Arc::clone(&slot_manager),
        );
        let reservoir = Self {
            ready,
            return_tx,
            target_ready,
            slot_manager,
            refiller_handle,
            scanner_handle,
            return_processor_handle,
            config,
        };
        reservoir.warmup().await?;
        Ok(reservoir)
    }

    pub async fn warmup(&self) -> Result<()> {
        let started = tokio::time::Instant::now();
        while self.ready.len() < self.config.target_ready {
            if started.elapsed() > WARMUP_TIMEOUT {
                anyhow::bail!("timed out warming DSQL connection reservoir");
            }
            tokio::time::sleep(WARMUP_POLL_INTERVAL).await;
        }
        metrics::record_dsql_pool_connections_total(self.ready.len());
        metrics::set_dsql_reservoir_ready_connections(self.ready.len());
        Ok(())
    }

    pub fn checkout(&self) -> Result<ReservoirEntry, ReservoirError> {
        // Non-blocking by design. Queueing is handled by class budgets before
        // checkout; an empty reservoir is immediate backpressure, not another
        // hidden wait queue.
        loop {
            match self.ready.try_recv() {
                // An operation that failed on this connection flagged it after
                // returning it; it never reaches another caller.
                Ok(entry) if entry.broken.is_set() => {
                    record_retirement(&self.slot_manager, "broken", entry.created_at.elapsed());
                }
                Ok(entry) => {
                    metrics::record_dsql_pool_connections_total(self.ready.len());
                    metrics::set_dsql_reservoir_ready_connections(self.ready.len());
                    return Ok(entry);
                }
                Err(async_channel::TryRecvError::Empty) => {
                    metrics::record_dsql_pool_empty_reservoir();
                    return Err(ReservoirError::Empty);
                }
                Err(async_channel::TryRecvError::Closed) => return Err(ReservoirError::Closed),
            }
        }
    }

    pub fn return_sender(&self) -> mpsc::UnboundedSender<ReturnedConnection> {
        self.return_tx.clone()
    }

    pub fn slot_manager(&self) -> Arc<SlotBlockManager> {
        Arc::clone(&self.slot_manager)
    }

    pub fn ready_count(&self) -> usize {
        self.ready.len()
    }

    /// Live warm-connection target: the configured `target_ready` until a
    /// controller connection budget caps it.
    pub fn target_ready(&self) -> usize {
        self.target_ready.load(Ordering::Acquire)
    }

    /// Cap the warm-connection target to a controller-issued budget.
    ///
    /// Floored at one: a node with no warm connection could not renew or
    /// relinquish its own leases, so a directive may throttle a node but never
    /// silence it. The refiller reads the target on its next idle check; warm
    /// connections already above the cap are retired by
    /// [`retire_excess`](Self::retire_excess), not here.
    pub fn reconfigure_target(&self, new_target: u32) {
        self.target_ready
            .store((new_target as usize).max(1), Ordering::Release);
        metrics::set_dsql_reservoir_target_connections((new_target as usize).max(1));
    }

    /// Retire idle warm connections above `new_target` now instead of waiting
    /// for their lifetimes to expire, so a lowered cluster budget takes effect
    /// promptly. Only entries in the ready queue are touched: a connection
    /// checked out by in-flight work returns through the normal path and the
    /// refiller simply declines to replace it, so a cap never interrupts work.
    pub fn retire_excess(&self, new_target: u32) -> usize {
        let target = (new_target as usize).max(1);
        let mut retired = 0usize;
        while self.ready.len() > target {
            match self.ready.try_recv() {
                Ok(entry) => {
                    retired += 1;
                    record_retirement(&self.slot_manager, "budget_cap", entry.created_at.elapsed());
                }
                Err(_) => break,
            }
        }
        metrics::record_dsql_pool_connections_total(self.ready.len());
        metrics::set_dsql_reservoir_ready_connections(self.ready.len());
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
        let (ready_tx, ready) = async_channel::bounded(ready_capacity(config.target_ready));
        let (return_tx, _return_rx) = mpsc::unbounded_channel();
        let idle = |sender: async_channel::Sender<ReservoirEntry>| {
            tokio::spawn(async move {
                let _keep_open = sender;
                std::future::pending::<()>().await;
            })
        };
        Self {
            ready,
            return_tx,
            target_ready: Arc::new(AtomicUsize::new(config.target_ready)),
            slot_manager: SlotBlockManager::local_for_tests(config.target_ready),
            refiller_handle: idle(ready_tx.clone()),
            scanner_handle: idle(ready_tx.clone()),
            return_processor_handle: idle(ready_tx),
            config,
        }
    }

    pub async fn shutdown(&self) -> Result<()> {
        // Abort background producers before releasing slot blocks. Otherwise a
        // refiller could acquire a fresh slot while shutdown is trying to
        // relinquish capacity.
        self.refiller_handle.abort();
        self.scanner_handle.abort();
        self.return_processor_handle.abort();
        self.slot_manager.release_all().await
    }
}

impl Drop for Reservoir {
    fn drop(&mut self) {
        self.refiller_handle.abort();
        self.scanner_handle.abort();
        self.return_processor_handle.abort();
    }
}

/// Keep the ready queue at its target with up to `inflight` creations at once.
///
/// One task decides, in the order Req 4.3 fixes: an in-flight permit, a slot,
/// a cluster rate token. Each admitted creation then runs as its own task in a
/// `JoinSet` this task owns, so aborting the refiller at shutdown cancels every
/// creation it started, and each cancelled creation's [`SlotCharge`] gives its
/// slot back. Ready connections plus creations in flight never exceed the
/// target, so parallel creation cannot overfill the queue.
///
/// Failures anywhere in a refill (no slot, no rate token, a refused or timed-out
/// connect) count toward one shared streak, and every new attempt waits
/// [`refill_backoff`] of that streak first. The first created connection ends
/// the streak and cuts short a wait already under way, so a cluster that
/// recovers is refilled at once rather than after the longest delay.
fn spawn_refiller(
    config: ReservoirConfig,
    factory: Arc<dyn PhysicalConnectionFactory>,
    ready_tx: async_channel::Sender<ReservoirEntry>,
    distributed_bucket: Arc<DistributedTokenBucket>,
    slot_manager: Arc<SlotBlockManager>,
    inflight: Arc<Semaphore>,
    target_ready: Arc<AtomicUsize>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut creating = JoinSet::new();
        let failures = Arc::new(AtomicU32::new(0));
        let recovered = Arc::new(Notify::new());
        loop {
            let streak = failures.load(Ordering::Acquire);
            if streak > 0 {
                let mut success = std::pin::pin!(recovered.notified());
                success.as_mut().enable();
                // Registered before the second look, so a success between
                // the two cannot be missed.
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
            // A creation that finished has already put its connection in the
            // ready queue (or failed), so reaping before counting keeps
            // `ready + creating` exact.
            while creating.try_join_next().is_some() {}
            if ready_tx.len() + creating.len() >= target_ready.load(Ordering::Acquire) {
                drop(inflight_permit);
                tokio::time::sleep(REFILLER_IDLE_INTERVAL).await;
                continue;
            }
            let refill_started = Instant::now();
            if slot_manager.acquire_slot().await.is_err() {
                metrics::record_dsql_reservoir_refill_error("slot_unavailable");
                failures.fetch_add(1, Ordering::AcqRel);
                continue;
            }
            let slot = SlotCharge::new(Arc::clone(&slot_manager));
            if let Err(error) = distributed_bucket.wait().await {
                // A slot is reserved before the global rate token so local
                // refillers cannot collectively overshoot capacity while
                // waiting for token-bucket admission. Dropping the charge
                // releases it.
                drop(slot);
                metrics::record_dsql_reservoir_refill_error("rate_limiter");
                metrics::record_dsql_reservoir_refill_duration(
                    OutcomeLabel::Failure,
                    refill_started.elapsed(),
                );
                tracing::warn!(error = %error, "failed to acquire DSQL distributed rate token");
                failures.fetch_add(1, Ordering::AcqRel);
                continue;
            }
            creating.spawn(create_into_ready(
                assign_lifetime(&config),
                Arc::clone(&factory),
                ready_tx.clone(),
                slot,
                inflight_permit,
                Arc::clone(&failures),
                Arc::clone(&recovered),
                refill_started,
            ));
        }
    })
}

/// Make one connection and hand it to the ready queue.
///
/// Holds `_inflight_permit` for the whole attempt, so at most the in-flight
/// limit of connects run at once.
async fn create_into_ready(
    max_lifetime: StdDuration,
    factory: Arc<dyn PhysicalConnectionFactory>,
    ready_tx: async_channel::Sender<ReservoirEntry>,
    slot: SlotCharge,
    _inflight_permit: OwnedSemaphorePermit,
    failures: Arc<AtomicU32>,
    recovered: Arc<Notify>,
    refill_started: Instant,
) {
    let create_started = Instant::now();
    let connection = match create_within_timeout(factory.as_ref()).await {
        Ok(connection) => connection,
        Err(error) => {
            // The slot charge drops with this frame and releases the slot.
            failures.fetch_add(1, Ordering::AcqRel);
            metrics::record_dsql_reservoir_connection_create_duration(create_started.elapsed());
            metrics::record_dsql_reservoir_refill_error("factory");
            metrics::record_dsql_reservoir_refill_duration(
                OutcomeLabel::Failure,
                refill_started.elapsed(),
            );
            metrics::record_dsql_connection_error(error.kind());
            tracing::warn!(error = %error, "failed to create DSQL connection");
            return;
        }
    };
    failures.store(0, Ordering::Release);
    recovered.notify_waiters();
    metrics::record_dsql_reservoir_connection_create_duration(create_started.elapsed());
    metrics::record_dsql_pool_connection_created();
    let entry = ReservoirEntry {
        connection,
        created_at: Instant::now(),
        max_lifetime,
        broken: BrokenFlag::default(),
    };
    // The queue always has room (see `ready_capacity`); a full queue would
    // only make this creation wait, still counted, so the refiller starts
    // nothing new meanwhile.
    if ready_tx.send(entry).await.is_err() {
        // Channel closure means shutdown won the race after the physical
        // connection was created. The slot charge drops with the connection
        // and releases its slot (Req 4.8).
        metrics::record_dsql_reservoir_refill_error("ready_channel_closed");
        metrics::record_dsql_reservoir_refill_duration(
            OutcomeLabel::Failure,
            refill_started.elapsed(),
        );
        return;
    }
    slot.transfer_to_connection();
    metrics::record_dsql_pool_connections_total(ready_tx.len());
    metrics::set_dsql_reservoir_ready_connections(ready_tx.len());
    metrics::record_dsql_reservoir_refill_duration(OutcomeLabel::Success, refill_started.elapsed());
}

/// A slot reserved for a connection that does not exist yet.
///
/// Dropping the charge releases the slot, so a refill that fails, times out,
/// or is cancelled at shutdown gives its slot back exactly once (Req 4.4, 4.8).
/// A connection placed in the ready queue takes the slot over.
#[derive(Debug)]
struct SlotCharge {
    slot_manager: Option<Arc<SlotBlockManager>>,
}

impl SlotCharge {
    fn new(slot_manager: Arc<SlotBlockManager>) -> Self {
        Self {
            slot_manager: Some(slot_manager),
        }
    }

    fn transfer_to_connection(mut self) {
        self.slot_manager.take();
    }
}

impl Drop for SlotCharge {
    fn drop(&mut self) {
        if let Some(slot_manager) = self.slot_manager.take() {
            slot_manager.release_slot();
        }
    }
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

fn spawn_scanner(
    config: ReservoirConfig,
    ready_tx: async_channel::Sender<ReservoirEntry>,
    ready_rx: async_channel::Receiver<ReservoirEntry>,
    slot_manager: Arc<SlotBlockManager>,
    target_ready: Arc<AtomicUsize>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let sleep_for = duration_or_default(config.scan_interval, SCAN_INTERVAL);
            tokio::time::sleep(sleep_for).await;

            let guard_window = duration_or_default(config.guard_window, GUARD_WINDOW);
            let batch_size = scan_budget(target_ready.load(Ordering::Acquire));
            let mut scanned = 0;
            while scanned < batch_size {
                match ready_rx.try_recv() {
                    Ok(entry) => {
                        scanned += 1;
                        if entry.broken.is_set() {
                            record_retirement(&slot_manager, "broken", entry.created_at.elapsed());
                        } else if entry.should_retire(guard_window) {
                            record_retirement(
                                &slot_manager,
                                "guard_window",
                                entry.created_at.elapsed(),
                            );
                        } else if ready_tx.try_send(entry).is_err() {
                            // The scanner took a healthy connection out of the
                            // ready queue. If it cannot put it back, it becomes
                            // the owner of the discard and must release the slot.
                            slot_manager.release_slot();
                            metrics::record_dsql_pool_connection_retired("ready_channel_full");
                            metrics::set_dsql_reservoir_ready_connections(ready_tx.len());
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            metrics::record_dsql_pool_connections_total(ready_tx.len());
            metrics::set_dsql_reservoir_ready_connections(ready_tx.len());
        }
    })
}

fn spawn_return_processor(
    config: ReservoirConfig,
    ready_tx: async_channel::Sender<ReservoirEntry>,
    mut return_rx: mpsc::UnboundedReceiver<ReturnedConnection>,
    slot_manager: Arc<SlotBlockManager>,
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
            );
            metrics::record_dsql_reservoir_connection_validate_duration(validate_started.elapsed());
            if let Some(reason) = retirement {
                record_retirement(&slot_manager, reason, entry.created_at.elapsed());
                continue;
            }
            metrics::record_dsql_pool_connection_returned();
            match ready_tx.try_send(entry) {
                Ok(()) => {}
                // Unreachable while the class budgets sum to at most the
                // target (see `ready_capacity`). If a reconfigured budget ever
                // admits more, the connection retires (Req 6.7) rather than
                // the processor waiting for room: a wait would hold up every
                // return queued behind it, each holding its caller's class
                // permit.
                Err(TrySendError::Full(entry)) => {
                    record_retirement(
                        &slot_manager,
                        "ready_channel_full",
                        entry.created_at.elapsed(),
                    );
                }
                Err(TrySendError::Closed(_entry)) => {
                    // The return processor is the last owner when the ready
                    // channel is closed. Retire the connection instead of
                    // leaking capacity.
                    slot_manager.release_slot();
                    metrics::record_dsql_pool_connection_retired("return_channel_closed");
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
/// queue, if it does. Validation reads only flags and the lifetime, never the
/// network (Req 6.3).
fn return_retirement_reason(
    marked_bad: bool,
    broken: bool,
    inside_guard_window: bool,
) -> Option<&'static str> {
    if marked_bad {
        Some("bad_flag")
    } else if broken {
        Some("broken")
    } else if inside_guard_window {
        Some("guard_window")
    } else {
        None
    }
}

/// The ready queue's capacity for a reservoir of `target_ready`: every
/// connection the node can hold. Ready connections plus creations stay below
/// the target when a creation starts, and the class budgets, which sum to the
/// target, cap the connections checked out, so returns can always be queued.
pub(crate) fn ready_capacity(target_ready: usize) -> usize {
    target_ready.saturating_mul(2)
}

fn record_retirement(slot_manager: &SlotBlockManager, reason: &'static str, age: StdDuration) {
    metrics::record_dsql_reservoir_connection_age(reason, age);
    metrics::record_dsql_pool_connection_retired(reason);
    slot_manager.release_slot();
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
mod scenario_tests;

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use time::Duration;

    use super::*;

    proptest! {
        #[test]
        fn connection_lifetime_jitter_range(
            base_secs in 1u64..600,
            jitter_secs in 0u64..120,
            guard_secs in 1u64..60,
        ) {
            let config = ReservoirConfig {
                target_ready: 1,
                inflight_limit: 1,
                base_lifetime: Duration::seconds(i64::try_from(base_secs).unwrap()),
                lifetime_jitter: Duration::seconds(i64::try_from(jitter_secs).unwrap()),
                guard_window: Duration::seconds(i64::try_from(guard_secs).unwrap()),
                scan_interval: Duration::seconds(1),
            };
            prop_assume!(config.validate().is_ok());

            let lifetime = assign_lifetime(&config);
            prop_assert!(lifetime >= StdDuration::from_secs(base_secs));
            prop_assert!(lifetime <= StdDuration::from_secs(base_secs + jitter_secs));
        }

        #[test]
        fn lifetime_safety_against_token_ttl(
            base_secs in 1u64..600,
            jitter_secs in 0u64..120,
            guard_secs in 1u64..120,
        ) {
            let config = ReservoirConfig {
                target_ready: 1,
                inflight_limit: 1,
                base_lifetime: Duration::seconds(i64::try_from(base_secs).unwrap()),
                lifetime_jitter: Duration::seconds(i64::try_from(jitter_secs).unwrap()),
                guard_window: Duration::seconds(i64::try_from(guard_secs).unwrap()),
                scan_interval: Duration::seconds(1),
            };
            if config.validate().is_ok() {
                prop_assert!(base_secs + jitter_secs + guard_secs <= 15 * 60);
            }
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
    fn returns_retire_on_the_bad_flag_a_break_or_the_guard_window() {
        assert_eq!(return_retirement_reason(false, false, false), None);
        assert_eq!(return_retirement_reason(true, true, true), Some("bad_flag"));
        assert_eq!(return_retirement_reason(false, true, true), Some("broken"));
        assert_eq!(
            return_retirement_reason(false, false, true),
            Some("guard_window")
        );
    }

    #[test]
    fn default_internal_constants_match_dsql_token_safety() {
        assert_eq!(TARGET_READY, 50);
        assert_eq!(SCAN_BUDGET, 25);
        assert_eq!(INFLIGHT_LIMIT, 8);
        assert_eq!(SCAN_INTERVAL, StdDuration::from_secs(1));
        assert!(BASE_LIFETIME + LIFETIME_JITTER + GUARD_WINDOW < StdDuration::from_secs(15 * 60));
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
    async fn budget_cap_lowers_live_target_and_floors_at_one() {
        let reservoir = Reservoir::idle_for_tests(ReservoirConfig {
            target_ready: 8,
            ..ReservoirConfig::default()
        });
        assert_eq!(reservoir.target_ready(), 8);

        reservoir.reconfigure_target(3);
        assert_eq!(reservoir.target_ready(), 3);
        assert_eq!(reservoir.config().target_ready, 8);

        reservoir.reconfigure_target(0);
        assert_eq!(reservoir.target_ready(), 1);
    }

    #[tokio::test]
    async fn retire_excess_leaves_a_pool_within_target_untouched() {
        let reservoir = Reservoir::idle_for_tests(ReservoirConfig {
            target_ready: 4,
            ..ReservoirConfig::default()
        });

        assert_eq!(reservoir.retire_excess(2), 0);
        assert_eq!(reservoir.ready_count(), 0);
        assert!(matches!(reservoir.checkout(), Err(ReservoirError::Empty)));
    }
}
