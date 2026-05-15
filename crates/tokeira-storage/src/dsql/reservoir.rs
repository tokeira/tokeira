//! Warm raw DSQL connection reservoir.

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

#[derive(Debug)]
pub struct ReservoirEntry {
    pub(crate) connection: PgConnection,
    pub(crate) created_at: Instant,
    pub(crate) max_lifetime: StdDuration,
}

impl ReservoirEntry {
    fn should_retire(&self, guard_window: StdDuration) -> bool {
        self.created_at.elapsed().saturating_add(guard_window) >= self.max_lifetime
    }
}

#[derive(Debug)]
pub struct ReturnedConnection {
    pub(crate) entry: ReservoirEntry,
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
            if started.elapsed() > StdDuration::from_secs(30) {
                anyhow::bail!("timed out warming DSQL connection reservoir");
            }
            tokio::task::yield_now().await;
        }
        metrics::record_dsql_pool_connections_total(self.ready.len());
        Ok(())
    }

    pub fn checkout(&self) -> Result<ReservoirEntry, ReservoirError> {
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
                tokio::time::sleep(StdDuration::from_millis(50)).await;
            }
            let Ok(_inflight_guard) = inflight.clone().acquire_owned().await else {
                return;
            };
            let Ok(_slot) = slot_manager.acquire_slot().await else {
                tokio::time::sleep(StdDuration::from_secs(1)).await;
                continue;
            };
            if let Err(error) = distributed_bucket.wait().await {
                slot_manager.release_slot();
                tracing::warn!(error = %error, "failed to acquire DSQL distributed rate token");
                tokio::time::sleep(StdDuration::from_secs(1)).await;
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
                    tokio::time::sleep(StdDuration::from_secs(1)).await;
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
            let sleep_for = duration_or_default(config.scan_interval, StdDuration::from_secs(1));
            tokio::time::sleep(sleep_for).await;

            let guard_window = duration_or_default(config.guard_window, StdDuration::from_secs(45));
            let batch_size = (target_ready.load(Ordering::Acquire) / 2).max(1);
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
        let guard_window = duration_or_default(config.guard_window, StdDuration::from_secs(45));
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
    let base = duration_or_default(config.base_lifetime, StdDuration::from_secs(10 * 60));
    let jitter_max = duration_or_default(config.lifetime_jitter, StdDuration::from_secs(2 * 60));
    if jitter_max.is_zero() {
        return base;
    }
    let jitter = rand::thread_rng().gen_range(0..=jitter_max.as_secs());
    base + StdDuration::from_secs(jitter)
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
    }
}
