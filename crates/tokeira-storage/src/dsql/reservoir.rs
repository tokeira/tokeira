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
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration as StdDuration, Instant},
};

use anyhow::Result;
use rand::Rng;
use sqlx::PgConnection;
use tokeira_observability::OutcomeLabel;
use tokio::{
    sync::{Notify, Semaphore, mpsc},
    task::JoinHandle,
};

use crate::metrics;

use super::{ConnectionCoordinator, PhysicalConnectionFactory, ReservoirConfig};

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
pub(crate) const REFILLER_IDLE_INTERVAL: StdDuration = StdDuration::from_millis(100);
pub(crate) const REFILLER_ERROR_BACKOFF: StdDuration = StdDuration::from_millis(250);

/// Caller-owned monotonic deadline for embedded reservoir warmup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WarmupDeadline {
    at: Instant,
}

impl WarmupDeadline {
    /// Wrap an absolute process-monotonic startup deadline.
    pub const fn new(at: Instant) -> Self {
        Self { at }
    }

    /// Absolute process-monotonic instant at which warmup must stop.
    pub const fn instant(self) -> Instant {
        self.at
    }
}

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
struct PendingSlotCharge {
    coordinator: Option<Arc<dyn ConnectionCoordinator>>,
}

impl PendingSlotCharge {
    fn new(coordinator: Arc<dyn ConnectionCoordinator>) -> Self {
        Self {
            coordinator: Some(coordinator),
        }
    }

    fn transfer_to_connection(mut self) {
        self.coordinator.take();
    }
}

impl Drop for PendingSlotCharge {
    fn drop(&mut self) {
        if let Some(coordinator) = self.coordinator.take() {
            coordinator.release_slot();
        }
    }
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
    return_tx: Mutex<Option<mpsc::UnboundedSender<ReturnedConnection>>>,
    target_ready: Arc<AtomicUsize>,
    coordinator: Arc<dyn ConnectionCoordinator>,
    ready_changed: Arc<Notify>,
    refiller_handles: Mutex<Vec<JoinHandle<()>>>,
    scanner_handle: Mutex<Option<JoinHandle<()>>>,
    return_processor_handle: Mutex<Option<JoinHandle<()>>>,
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
        coordinator: Arc<dyn ConnectionCoordinator>,
    ) -> Result<Self> {
        Self::start_with_deadline(
            config,
            factory,
            coordinator,
            Instant::now() + WARMUP_TIMEOUT,
        )
        .await
    }

    /// Start the reservoir without exceeding a caller-owned startup deadline.
    pub async fn start_with_deadline(
        config: ReservoirConfig,
        factory: Arc<dyn PhysicalConnectionFactory>,
        coordinator: Arc<dyn ConnectionCoordinator>,
        deadline: Instant,
    ) -> Result<Self> {
        config.validate()?;
        coordinator.validate().await?;
        let (ready_tx, ready) = async_channel::bounded(config.target_ready);
        let (return_tx, return_rx) = mpsc::unbounded_channel();
        let inflight = Arc::new(Semaphore::new(config.inflight_limit));
        let target_ready = Arc::new(AtomicUsize::new(config.target_ready));
        let ready_changed = Arc::new(Notify::new());
        metrics::set_dsql_reservoir_target_connections(config.target_ready);
        metrics::set_dsql_reservoir_ready_connections(0);
        let refiller_handles = spawn_refillers(
            config.clone(),
            Arc::clone(&factory),
            ready_tx.clone(),
            Arc::clone(&coordinator),
            Arc::clone(&inflight),
            Arc::clone(&target_ready),
            Arc::clone(&ready_changed),
        );
        let scanner_handle = spawn_scanner(
            config.clone(),
            ready_tx.clone(),
            ready.clone(),
            Arc::clone(&coordinator),
            Arc::clone(&target_ready),
        );
        let return_processor_handle = spawn_return_processor(
            config.clone(),
            ready_tx,
            return_rx,
            Arc::clone(&coordinator),
        );
        let reservoir = Self {
            ready,
            return_tx: Mutex::new(Some(return_tx)),
            target_ready,
            coordinator,
            ready_changed,
            refiller_handles: Mutex::new(refiller_handles),
            scanner_handle: Mutex::new(Some(scanner_handle)),
            return_processor_handle: Mutex::new(Some(return_processor_handle)),
            config,
        };
        if let Err(error) = reservoir.warmup_until(deadline).await {
            reservoir.begin_shutdown();
            let _ = reservoir.finish_shutdown().await;
            return Err(error);
        }
        Ok(reservoir)
    }

    pub async fn warmup(&self) -> Result<()> {
        self.warmup_until(Instant::now() + WARMUP_TIMEOUT).await
    }

    /// Wait for the configured idle capacity within the remaining startup budget.
    pub async fn warmup_until(&self, deadline: Instant) -> Result<()> {
        while self.ready.len() < self.config.target_ready {
            let notified = self.ready_changed.notified();
            if self.ready.len() >= self.config.target_ready {
                break;
            }
            if self.ready.is_closed() {
                anyhow::bail!("DSQL connection reservoir closed during warmup");
            }
            let now = Instant::now();
            if now >= deadline {
                anyhow::bail!("timed out warming DSQL connection reservoir");
            }
            if tokio::time::timeout(deadline.saturating_duration_since(now), notified)
                .await
                .is_err()
            {
                anyhow::bail!("timed out warming DSQL connection reservoir");
            }
        }
        metrics::record_dsql_pool_connections_total(self.ready.len());
        metrics::set_dsql_reservoir_ready_connections(self.ready.len());
        Ok(())
    }

    pub fn checkout(&self) -> Result<ReservoirEntry, ReservoirError> {
        // Non-blocking by design. Queueing is handled by class budgets before
        // checkout; an empty reservoir is immediate backpressure, not another
        // hidden wait queue.
        match self.ready.try_recv() {
            Ok(entry) => {
                metrics::record_dsql_pool_connections_total(self.ready.len());
                metrics::set_dsql_reservoir_ready_connections(self.ready.len());
                Ok(entry)
            }
            Err(async_channel::TryRecvError::Empty) => {
                metrics::record_dsql_pool_empty_reservoir();
                Err(ReservoirError::Empty)
            }
            Err(async_channel::TryRecvError::Closed) => Err(ReservoirError::Closed),
        }
    }

    /// Wait for a warm connection, returning promptly when shutdown closes admission.
    pub async fn checkout_wait(&self) -> Result<ReservoirEntry, ReservoirError> {
        match self.ready.recv().await {
            Ok(entry) => {
                metrics::record_dsql_pool_connections_total(self.ready.len());
                metrics::set_dsql_reservoir_ready_connections(self.ready.len());
                Ok(entry)
            }
            Err(_) => Err(ReservoirError::Closed),
        }
    }

    pub fn return_sender(&self) -> Option<mpsc::UnboundedSender<ReturnedConnection>> {
        self.return_tx
            .lock()
            .expect("DSQL reservoir return sender poisoned")
            .as_ref()
            .cloned()
    }

    pub fn coordinator(&self) -> Arc<dyn ConnectionCoordinator> {
        Arc::clone(&self.coordinator)
    }

    pub fn ready_count(&self) -> usize {
        self.ready.len()
    }

    pub fn reconfigure_target(&self, new_target: u32) {
        self.target_ready
            .store((new_target as usize).max(1), Ordering::Release);
        metrics::set_dsql_reservoir_target_connections((new_target as usize).max(1));
    }

    pub fn retire_excess(&self, new_target: u32) -> usize {
        let target = (new_target as usize).max(1);
        let mut retired = 0usize;
        while self.ready.len() > target {
            match self.ready.try_recv() {
                Ok(entry) => {
                    retired += 1;
                    record_retirement(
                        self.coordinator.as_ref(),
                        "budget_cap",
                        entry.created_at.elapsed(),
                    );
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

    pub async fn shutdown(&self) -> Result<()> {
        self.begin_shutdown();
        self.finish_shutdown().await
    }

    /// Close ready admission and retire every idle physical connection.
    pub fn begin_shutdown(&self) {
        self.ready.close();
        if let Ok(handles) = self.refiller_handles.lock() {
            for handle in handles.iter() {
                handle.abort();
            }
        }
        if let Ok(handle) = self.scanner_handle.lock()
            && let Some(handle) = handle.as_ref()
        {
            handle.abort();
        }
        if let Ok(mut sender) = self.return_tx.lock() {
            sender.take();
        }
        while let Ok(entry) = self.ready.try_recv() {
            record_retirement(
                self.coordinator.as_ref(),
                "shutdown",
                entry.created_at.elapsed(),
            );
        }
        self.ready_changed.notify_waiters();
        metrics::record_dsql_pool_connections_total(0);
        metrics::set_dsql_reservoir_ready_connections(0);
    }

    /// Abort return processing after a caller-owned shutdown deadline expires.
    pub fn abort_return_processing(&self) {
        if let Ok(handle) = self.return_processor_handle.lock()
            && let Some(handle) = handle.as_ref()
        {
            handle.abort();
        }
    }

    /// Join closed return processing and relinquish coordinator resources.
    pub async fn finish_shutdown(&self) -> Result<()> {
        let refiller_handles = self
            .refiller_handles
            .lock()
            .map(|mut handles| std::mem::take(&mut *handles))
            .unwrap_or_default();
        for handle in refiller_handles {
            let _ = handle.await;
        }
        let scanner_handle = self
            .scanner_handle
            .lock()
            .ok()
            .and_then(|mut handle| handle.take());
        if let Some(handle) = scanner_handle {
            let _ = handle.await;
        }
        let return_processor_handle = self
            .return_processor_handle
            .lock()
            .ok()
            .and_then(|mut handle| handle.take());
        if let Some(handle) = return_processor_handle {
            let _ = handle.await;
        }
        self.coordinator.shutdown().await
    }
}

impl Drop for Reservoir {
    fn drop(&mut self) {
        self.ready.close();
        if let Ok(handles) = self.refiller_handles.get_mut() {
            for handle in handles.iter() {
                handle.abort();
            }
        }
        if let Ok(handle) = self.scanner_handle.get_mut()
            && let Some(handle) = handle.as_ref()
        {
            handle.abort();
        }
        if let Ok(handle) = self.return_processor_handle.get_mut()
            && let Some(handle) = handle.as_ref()
        {
            handle.abort();
        }
        while let Ok(entry) = self.ready.try_recv() {
            self.coordinator.release_slot();
            drop(entry);
        }
    }
}

fn spawn_refillers(
    config: ReservoirConfig,
    factory: Arc<dyn PhysicalConnectionFactory>,
    ready_tx: async_channel::Sender<ReservoirEntry>,
    coordinator: Arc<dyn ConnectionCoordinator>,
    inflight: Arc<Semaphore>,
    target_ready: Arc<AtomicUsize>,
    ready_changed: Arc<Notify>,
) -> Vec<JoinHandle<()>> {
    (0..config.inflight_limit)
        .map(|_| {
            let config = config.clone();
            let factory = Arc::clone(&factory);
            let ready_tx = ready_tx.clone();
            let coordinator = Arc::clone(&coordinator);
            let inflight = Arc::clone(&inflight);
            let target_ready = Arc::clone(&target_ready);
            let ready_changed = Arc::clone(&ready_changed);
            tokio::spawn(async move {
        loop {
            while ready_tx.len() >= target_ready.load(Ordering::Acquire) {
                tokio::time::sleep(REFILLER_IDLE_INTERVAL).await;
            }
            let Ok(_inflight_guard) = inflight.clone().acquire_owned().await else {
                return;
            };
            let Ok(()) = coordinator.acquire_slot().await else {
                metrics::record_dsql_reservoir_refill_error("slot_unavailable");
                tokio::time::sleep(StdDuration::from_secs(1)).await;
                continue;
            };
            // Cancellation can land while waiting for a token, IAM, TCP, or
            // the ready channel. The pending RAII charge makes every such path
            // release exactly once; success explicitly transfers the charge
            // to the physical connection's reservoir entry.
            let slot_charge = PendingSlotCharge::new(Arc::clone(&coordinator));
            let refill_started = Instant::now();
            if let Err(error) = coordinator.acquire_creation_token().await {
                // A slot is reserved before the global rate token so local
                // refillers cannot collectively overshoot capacity while
                // waiting for token-bucket admission.
                metrics::record_dsql_reservoir_refill_error("rate_limiter");
                metrics::record_dsql_reservoir_refill_duration(
                    OutcomeLabel::Failure,
                    refill_started.elapsed(),
                );
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
                        metrics::record_dsql_reservoir_refill_error("ready_channel_closed");
                        metrics::record_dsql_reservoir_refill_duration(
                            OutcomeLabel::Failure,
                            refill_started.elapsed(),
                        );
                        return;
                    }
                    slot_charge.transfer_to_connection();
                    metrics::record_dsql_pool_connections_total(ready_tx.len());
                    metrics::set_dsql_reservoir_ready_connections(ready_tx.len());
                    metrics::record_dsql_reservoir_refill_duration(
                        OutcomeLabel::Success,
                        refill_started.elapsed(),
                    );
                    ready_changed.notify_waiters();
                }
                Err(error) => {
                    metrics::record_dsql_reservoir_connection_create_duration(
                        create_started.elapsed(),
                    );
                    metrics::record_dsql_reservoir_refill_error("factory");
                    metrics::record_dsql_reservoir_refill_duration(
                        OutcomeLabel::Failure,
                        refill_started.elapsed(),
                    );
                    metrics::record_dsql_connection_error(error.kind());
                    tracing::warn!(error = %error, "failed to create DSQL connection");
                    tokio::time::sleep(REFILLER_ERROR_BACKOFF).await;
                }
            }
        }
            })
        })
        .collect()
}

fn spawn_scanner(
    config: ReservoirConfig,
    ready_tx: async_channel::Sender<ReservoirEntry>,
    ready_rx: async_channel::Receiver<ReservoirEntry>,
    coordinator: Arc<dyn ConnectionCoordinator>,
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
                                coordinator.as_ref(),
                                "guard_window",
                                entry.created_at.elapsed(),
                            );
                        } else if ready_tx.try_send(entry).is_err() {
                            // The scanner took a healthy connection out of the
                            // ready queue. If it cannot put it back, it becomes
                            // the owner of the discard and must release the slot.
                            coordinator.release_slot();
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
    coordinator: Arc<dyn ConnectionCoordinator>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let guard_window = duration_or_default(config.guard_window, GUARD_WINDOW);
        while let Some(returned) = return_rx.recv().await {
            let validate_started = Instant::now();
            let entry = returned.entry;
            if let Some(reason) =
                return_retirement_reason(returned.marked_bad, entry.should_retire(guard_window))
            {
                metrics::record_dsql_reservoir_connection_validate_duration(
                    validate_started.elapsed(),
                );
                record_retirement(coordinator.as_ref(), reason, entry.created_at.elapsed());
                continue;
            }
            metrics::record_dsql_reservoir_connection_validate_duration(validate_started.elapsed());
            metrics::record_dsql_pool_connection_returned();
            if ready_tx.send(entry).await.is_err() {
                // The return processor is the last owner when the ready channel
                // is closed. Retire the connection instead of leaking capacity.
                coordinator.release_slot();
                metrics::record_dsql_pool_connection_retired("return_channel_closed");
                continue;
            }
            metrics::record_dsql_pool_connections_total(ready_tx.len());
            metrics::set_dsql_reservoir_ready_connections(ready_tx.len());
        }
    })
}

fn return_retirement_reason(marked_bad: bool, inside_guard_window: bool) -> Option<&'static str> {
    if marked_bad {
        Some("bad_flag")
    } else if inside_guard_window {
        Some("guard_window")
    } else {
        None
    }
}

fn record_retirement(
    coordinator: &dyn ConnectionCoordinator,
    reason: &'static str,
    age: StdDuration,
) {
    metrics::record_dsql_reservoir_connection_age(reason, age);
    metrics::record_dsql_pool_connection_retired(reason);
    coordinator.release_slot();
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
    use std::sync::atomic::AtomicUsize;

    use anyhow::Result;
    use proptest::prelude::*;
    use time::Duration;

    use super::*;

    #[derive(Debug, Default)]
    struct FailingFactory {
        attempts: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl PhysicalConnectionFactory for FailingFactory {
        async fn create_connection(
            &self,
        ) -> Result<PgConnection, crate::dsql::ConnectionFactoryError> {
            self.attempts.fetch_add(1, Ordering::AcqRel);
            Err(crate::dsql::ConnectionFactoryError::Connection(
                "injected connection failure".to_owned(),
            ))
        }
    }

    #[derive(Debug, Default)]
    struct RecordingCoordinator {
        events: Mutex<Vec<&'static str>>,
        used: AtomicUsize,
    }

    impl RecordingCoordinator {
        fn events(&self) -> Vec<&'static str> {
            self.events.lock().expect("recording coordinator").clone()
        }
    }

    #[async_trait::async_trait]
    impl ConnectionCoordinator for RecordingCoordinator {
        async fn validate(&self) -> Result<()> {
            self.events
                .lock()
                .expect("recording coordinator")
                .push("validate");
            Ok(())
        }

        async fn acquire_slot(&self) -> Result<()> {
            self.events
                .lock()
                .expect("recording coordinator")
                .push("slot");
            self.used.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }

        async fn acquire_creation_token(&self) -> Result<()> {
            self.events
                .lock()
                .expect("recording coordinator")
                .push("token");
            Ok(())
        }

        fn release_slot(&self) {
            self.events
                .lock()
                .expect("recording coordinator")
                .push("release");
            self.used.fetch_sub(1, Ordering::AcqRel);
        }

        fn used_slots(&self) -> usize {
            self.used.load(Ordering::Acquire)
        }

        async fn shutdown(&self) -> Result<()> {
            self.events
                .lock()
                .expect("recording coordinator")
                .push("shutdown");
            anyhow::ensure!(self.used_slots() == 0, "slot leak");
            Ok(())
        }
    }

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

    #[test]
    fn bad_returns_are_unconditionally_retired() {
        assert_eq!(return_retirement_reason(true, false), Some("bad_flag"));
        assert_eq!(return_retirement_reason(true, true), Some("bad_flag"));
        assert_eq!(return_retirement_reason(false, true), Some("guard_window"));
        assert_eq!(return_retirement_reason(false, false), None);
    }

    #[tokio::test]
    async fn warmup_obeys_expired_caller_deadline_without_retry_sleep() {
        let factory = Arc::new(FailingFactory::default());
        let coordinator = Arc::new(RecordingCoordinator::default());
        let coordinator_trait: Arc<dyn ConnectionCoordinator> = coordinator.clone();
        let result = Reservoir::start_with_deadline(
            ReservoirConfig {
                target_ready: 1,
                inflight_limit: 1,
                ..ReservoirConfig::default()
            },
            factory.clone(),
            coordinator_trait,
            Instant::now(),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(factory.attempts.load(Ordering::Acquire), 0);
        assert_eq!(coordinator.used_slots(), 0);
        assert_eq!(coordinator.events(), vec!["validate", "shutdown"]);
    }

    #[tokio::test]
    async fn failed_creation_releases_one_slot_after_rate_admission() {
        let factory = Arc::new(FailingFactory::default());
        let coordinator = Arc::new(RecordingCoordinator::default());
        let coordinator_trait: Arc<dyn ConnectionCoordinator> = coordinator.clone();
        let result = Reservoir::start_with_deadline(
            ReservoirConfig {
                target_ready: 1,
                inflight_limit: 1,
                ..ReservoirConfig::default()
            },
            factory,
            coordinator_trait,
            Instant::now() + StdDuration::from_millis(20),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(coordinator.used_slots(), 0);
        let events = coordinator.events();
        assert!(events.starts_with(&["validate", "slot", "token", "release"]));
        assert_eq!(events.last(), Some(&"shutdown"));
    }
}
