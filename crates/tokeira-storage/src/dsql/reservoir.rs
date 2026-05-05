//! Warm DSQL connection reservoir.
//!
//! The runtime wants fast checkouts, but DSQL connections have finite IAM token
//! lifetimes. The reservoir continuously fills a bounded ready pool, retires
//! connections before the guard window, and validates returned connections
//! before making them available again.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration as StdDuration, Instant},
};

use anyhow::Result;
use rand::Rng;
use sqlx::{Connection, Postgres, pool::PoolConnection};
use tokio::{
    sync::{Semaphore, mpsc},
    task::JoinHandle,
};

use crate::metrics;

use super::{DsqlConnector, ReservoirConfig, TokenBucketRateLimiter};

#[derive(Debug)]
pub struct ReservoirEntry {
    /// Checked-out physical SQLx connection.
    pub(crate) connection: PoolConnection<Postgres>,
    /// Physical creation time. Used for lifetime enforcement even after many
    /// logical checkouts.
    pub(crate) created_at: Instant,
    /// Jittered lifetime assigned at creation.
    pub(crate) max_lifetime: StdDuration,
}

impl ReservoirEntry {
    /// Return true when a connection is too close to expiry to safely reuse.
    fn should_retire(&self, guard_window: StdDuration) -> bool {
        self.created_at.elapsed().saturating_add(guard_window) >= self.max_lifetime
    }
}

#[derive(Debug)]
pub struct Reservoir {
    /// Bounded ready pool. The sender lives in background tasks; checkouts only
    /// need the receiver.
    ready: async_channel::Receiver<ReservoirEntry>,
    /// Non-async return path used by `DsqlPermit::drop`.
    return_tx: mpsc::UnboundedSender<ReservoirEntry>,
    /// Runtime-adjustable ready target used by controller budget directives.
    target_ready: Arc<AtomicUsize>,
    /// Background task that opens new physical connections.
    refiller_handle: JoinHandle<()>,
    /// Background task that retires near-expired idle connections.
    scanner_handle: JoinHandle<()>,
    /// Background task that validates returned connections before reuse.
    return_processor_handle: JoinHandle<()>,
    /// Retained for inspection and tests.
    config: ReservoirConfig,
}

impl Reservoir {
    /// Start all reservoir background tasks.
    ///
    /// The ready pool is bounded at `target_ready`; connection creation is also
    /// bounded by `inflight_limit` and the token bucket. Those three controls
    /// together prevent both memory growth and endpoint hammering.
    pub async fn start(
        config: ReservoirConfig,
        connector: DsqlConnector,
        rate_limiter: TokenBucketRateLimiter,
    ) -> Result<Self> {
        config.validate()?;
        let (ready_tx, ready) = async_channel::bounded(config.target_ready);
        let (return_tx, return_rx) = mpsc::unbounded_channel();
        let inflight = Arc::new(Semaphore::new(config.inflight_limit));
        let target_ready = Arc::new(AtomicUsize::new(config.target_ready));
        let refiller_handle = spawn_refiller(
            config.clone(),
            connector,
            ready_tx.clone(),
            rate_limiter,
            inflight,
            Arc::clone(&target_ready),
        );
        let scanner_handle = spawn_scanner(
            config.clone(),
            ready_tx.clone(),
            ready.clone(),
            Arc::clone(&target_ready),
        );
        let return_processor_handle = spawn_return_processor(config.clone(), ready_tx, return_rx);
        Ok(Self {
            ready,
            return_tx,
            target_ready,
            refiller_handle,
            scanner_handle,
            return_processor_handle,
            config,
        })
    }

    /// Checkout one ready connection, waiting until the refiller supplies one.
    pub async fn checkout(&self) -> Result<ReservoirEntry> {
        if self.ready.is_empty() {
            metrics::record_dsql_pool_empty_reservoir();
        }
        let entry = self.ready.recv().await?;
        metrics::record_dsql_pool_connections_total(self.ready.len());
        Ok(entry)
    }

    /// Clone the return channel used by permits.
    pub fn return_sender(&self) -> mpsc::UnboundedSender<ReservoirEntry> {
        self.return_tx.clone()
    }

    /// Number of currently ready connections.
    pub fn ready_count(&self) -> usize {
        self.ready.len()
    }

    /// Apply a controller-provided ready-pool cap.
    pub fn reconfigure_target(&self, new_target: u32) {
        self.target_ready
            .store((new_target as usize).max(1), Ordering::Release);
    }

    /// Retire idle connections above the supplied target.
    pub fn retire_excess(&self, new_target: u32) -> usize {
        let target = (new_target as usize).max(1);
        let mut retired = 0usize;
        while self.ready.len() > target {
            match self.ready.try_recv() {
                Ok(_entry) => {
                    retired += 1;
                    metrics::record_dsql_pool_connection_retired("budget_cap");
                }
                Err(_) => break,
            }
        }
        metrics::record_dsql_pool_connections_total(self.ready.len());
        retired
    }

    /// Original reservoir configuration.
    pub fn config(&self) -> &ReservoirConfig {
        &self.config
    }
}

impl Drop for Reservoir {
    fn drop(&mut self) {
        // The reservoir owns these infinite-loop tasks. Aborting them on drop
        // avoids detached background work after a test or store shuts down.
        self.refiller_handle.abort();
        self.scanner_handle.abort();
        self.return_processor_handle.abort();
    }
}

fn spawn_refiller(
    config: ReservoirConfig,
    connector: DsqlConnector,
    ready_tx: async_channel::Sender<ReservoirEntry>,
    rate_limiter: TokenBucketRateLimiter,
    inflight: Arc<Semaphore>,
    target_ready: Arc<AtomicUsize>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            // Bounded channel provides natural backpressure — send().await
            // blocks when the channel is full, avoiding a busy-loop.
            while ready_tx.len() >= target_ready.load(Ordering::Acquire) {
                tokio::time::sleep(StdDuration::from_millis(50)).await;
            }
            let Ok(_inflight_guard) = inflight.clone().acquire_owned().await else {
                return;
            };
            rate_limiter.acquire().await;
            match connector.acquire().await {
                Ok(connection) => {
                    metrics::record_dsql_pool_connection_created();
                    let entry = ReservoirEntry {
                        connection,
                        created_at: Instant::now(),
                        max_lifetime: assign_lifetime(&config),
                    };
                    // Blocks when channel is full — no busy-loop needed.
                    if ready_tx.send(entry).await.is_err() {
                        return;
                    }
                    metrics::record_dsql_pool_connections_total(ready_tx.len());
                }
                Err(error) => {
                    tracing::warn!(error = %error, "failed to create DSQL connection");
                    // Back off on creation failure to avoid hammering a broken endpoint.
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
    target_ready: Arc<AtomicUsize>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let sleep_for = duration_or_default(config.scan_interval, StdDuration::from_secs(10));
            tokio::time::sleep(sleep_for).await;

            let guard_window = duration_or_default(config.guard_window, StdDuration::from_secs(45));
            // Scan a bounded batch per interval to avoid draining the entire
            // ready channel and starving concurrent checkout() callers.
            let batch_size = target_ready.load(Ordering::Acquire).max(1);
            let mut scanned = 0;
            while scanned < batch_size {
                match ready_rx.try_recv() {
                    Ok(entry) => {
                        scanned += 1;
                        if entry.should_retire(guard_window) {
                            metrics::record_dsql_pool_connection_retired("guard_window");
                        } else if ready_tx.try_send(entry).is_err() {
                            // Channel full — stop scanning this interval.
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
    mut return_rx: mpsc::UnboundedReceiver<ReservoirEntry>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let guard_window = duration_or_default(config.guard_window, StdDuration::from_secs(45));
        while let Some(mut entry) = return_rx.recv().await {
            if entry.should_retire(guard_window) {
                metrics::record_dsql_pool_connection_retired("expired");
                continue;
            }
            if entry.connection.ping().await.is_err() {
                metrics::record_dsql_pool_connection_retired("unhealthy");
                continue;
            }
            metrics::record_dsql_pool_connection_returned();
            if ready_tx.send(entry).await.is_err() {
                return;
            }
            metrics::record_dsql_pool_connections_total(ready_tx.len());
        }
    })
}

pub(crate) fn assign_lifetime(config: &ReservoirConfig) -> StdDuration {
    let base = duration_or_default(config.base_lifetime, StdDuration::from_secs(50 * 60));
    let jitter_max = duration_or_default(config.lifetime_jitter, StdDuration::from_secs(5 * 60));
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
            base_secs in 1u64..3_000,
            jitter_secs in 0u64..300,
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
