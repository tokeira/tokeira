//! Warm raw DSQL connection reservoir.
//!
//! The reservoir owns physical `PgConnection`s. Callers do not wait here: they
//! first obtain an operation-class permit, then checkout either returns a warm
//! connection immediately or reports `Empty` so the caller can shed/backpressure
//! work. Background tasks refill, scan, and process returns independently.

use std::{
    error::Error as StdError,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration as StdDuration, Instant},
};

use anyhow::Result;
use rand::Rng;
use sqlx::PgConnection;
use tokio::{
    sync::{Semaphore, mpsc},
    task::JoinHandle,
};

use crate::metrics;

use super::{DistributedTokenBucket, PhysicalConnectionFactory, ReservoirConfig, SlotBlockManager};

/// Default warm connection target for one node.
pub(crate) const TARGET_READY: usize = 50;
/// Maximum entries inspected by one scanner pass.
///
/// The scanner only inspects half of the target pool so a scan cannot drain the
/// channel and starve concurrent checkouts.
pub(crate) const SCAN_BUDGET: usize = TARGET_READY / 2;
/// Base age before a connection becomes eligible for retirement.
pub(crate) const BASE_LIFETIME: StdDuration = StdDuration::from_secs(10 * 60);
/// Positive jitter applied to spread retirements across time.
pub(crate) const LIFETIME_JITTER: StdDuration = StdDuration::from_secs(2 * 60);
/// Safety margin before the DSQL IAM token hard cutoff.
pub(crate) const GUARD_WINDOW: StdDuration = StdDuration::from_secs(45);
/// Maximum concurrent physical connection creation attempts.
pub(crate) const INFLIGHT_LIMIT: usize = 8;
pub(crate) const SCAN_INTERVAL: StdDuration = StdDuration::from_secs(1);
pub(crate) const WARMUP_TIMEOUT: StdDuration = StdDuration::from_secs(30);
pub(crate) const REFILLER_IDLE_INTERVAL: StdDuration = StdDuration::from_millis(100);
pub(crate) const REFILLER_ERROR_BACKOFF: StdDuration = StdDuration::from_millis(250);

#[derive(Debug)]
pub struct ReservoirEntry {
    /// The physical connection whose DSQL slot is currently reserved.
    pub(crate) connection: PgConnection,
    /// Creation time, used for lifetime retirement rather than checkout age.
    pub(crate) created_at: Instant,
    /// Per-connection lifetime after jitter was assigned at creation.
    pub(crate) max_lifetime: StdDuration,
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
        let (ready_tx, ready) = async_channel::bounded(config.target_ready);
        let (return_tx, return_rx) = mpsc::unbounded_channel();
        let inflight = Arc::new(Semaphore::new(config.inflight_limit));
        let target_ready = Arc::new(AtomicUsize::new(config.target_ready));
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
        let started = Instant::now();
        while self.ready.len() < self.config.target_ready {
            if started.elapsed() > WARMUP_TIMEOUT {
                anyhow::bail!("timed out warming DSQL connection reservoir");
            }
            tokio::task::yield_now().await;
        }
        metrics::record_dsql_pool_connections_total(self.ready.len());
        Ok(())
    }

    pub fn checkout(&self) -> Result<ReservoirEntry, ReservoirError> {
        // Non-blocking by design. Queueing is handled by class budgets before
        // checkout; an empty reservoir is immediate backpressure, not another
        // hidden wait queue.
        match self.ready.try_recv() {
            Ok(entry) => {
                metrics::record_dsql_pool_connections_total(self.ready.len());
                Ok(entry)
            }
            Err(async_channel::TryRecvError::Empty) => {
                metrics::record_dsql_pool_empty_reservoir();
                Err(ReservoirError::Empty)
            }
            Err(async_channel::TryRecvError::Closed) => Err(ReservoirError::Closed),
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

    pub fn reconfigure_target(&self, new_target: u32) {
        self.target_ready
            .store((new_target as usize).max(1), Ordering::Release);
    }

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
        retired
    }

    pub fn config(&self) -> &ReservoirConfig {
        &self.config
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
        loop {
            while ready_tx.len() >= target_ready.load(Ordering::Acquire) {
                tokio::time::sleep(REFILLER_IDLE_INTERVAL).await;
            }
            let Ok(_inflight_guard) = inflight.clone().acquire_owned().await else {
                return;
            };
            let Ok(_slot) = slot_manager.acquire_slot().await else {
                tokio::time::sleep(StdDuration::from_secs(1)).await;
                continue;
            };
            if let Err(error) = distributed_bucket.wait().await {
                // A slot is reserved before the global rate token so local
                // refillers cannot collectively overshoot capacity while
                // waiting for token-bucket admission.
                slot_manager.release_slot();
                tracing::warn!(error = %error, "failed to acquire DSQL distributed rate token");
                tokio::time::sleep(REFILLER_ERROR_BACKOFF).await;
                continue;
            }
            let create_started = Instant::now();
            match factory.create_connection().await {
                Ok(connection) => {
                    metrics::record_dsql_reservoir_connection_create_duration(
                        create_started.elapsed(),
                    );
                    metrics::record_dsql_pool_connection_created();
                    let entry = ReservoirEntry {
                        connection,
                        created_at: Instant::now(),
                        max_lifetime: assign_lifetime(&config),
                    };
                    if ready_tx.send(entry).await.is_err() {
                        // Channel closure means shutdown won the race after the
                        // physical connection was created. Dropping the
                        // connection must also release its reserved slot.
                        slot_manager.release_slot();
                        return;
                    }
                    metrics::record_dsql_pool_connections_total(ready_tx.len());
                }
                Err(error) => {
                    slot_manager.release_slot();
                    metrics::record_dsql_reservoir_connection_create_duration(
                        create_started.elapsed(),
                    );
                    metrics::record_dsql_connection_error(error.kind());
                    tracing::warn!(error = %error, "failed to create DSQL connection");
                    tokio::time::sleep(REFILLER_ERROR_BACKOFF).await;
                }
            }
        }
    })
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
                        if entry.should_retire(guard_window) {
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
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            metrics::record_dsql_pool_connections_total(ready_tx.len());
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
            let validate_started = Instant::now();
            let entry = returned.entry;
            if returned.marked_bad {
                metrics::record_dsql_reservoir_connection_validate_duration(
                    validate_started.elapsed(),
                );
                record_retirement(&slot_manager, "bad_flag", entry.created_at.elapsed());
                continue;
            }
            if entry.should_retire(guard_window) {
                metrics::record_dsql_reservoir_connection_validate_duration(
                    validate_started.elapsed(),
                );
                record_retirement(&slot_manager, "guard_window", entry.created_at.elapsed());
                continue;
            }
            metrics::record_dsql_reservoir_connection_validate_duration(validate_started.elapsed());
            metrics::record_dsql_pool_connection_returned();
            if ready_tx.send(entry).await.is_err() {
                // The return processor is the last owner when the ready channel
                // is closed. Retire the connection instead of leaking capacity.
                slot_manager.release_slot();
                metrics::record_dsql_pool_connection_retired("return_channel_closed");
                return;
            }
            metrics::record_dsql_pool_connections_total(ready_tx.len());
        }
    })
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
}
