//! Nexus operation types, endpoint registry, and timeout scanning.
//!
//! Contains the HTTP client trait for Nexus operations, endpoint configuration
//! and registry, timeout tracking state, and the background scanner that
//! detects timed-out Nexus operations.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use time::{Duration, OffsetDateTime};
use tokeira_kernel::Command;
use tokeira_types::{Payloads, RunKey};
use tokio_util::sync::CancellationToken;

use crate::lane::LaneHandle;
use crate::scanner::pick_lane;

#[derive(Clone, Debug, PartialEq)]
pub enum NexusStartResult {
    SyncCompleted { result: Payloads },
    SyncFailed { message: String },
    AsyncAccepted,
}

#[async_trait]
pub trait NexusHttpClient: Send + Sync {
    async fn start_operation(
        &self,
        address: &str,
        operation_id: &str,
        service: &str,
        operation: &str,
        input: &Payloads,
        schedule_to_close_timeout: Option<Duration>,
    ) -> Result<NexusStartResult>;

    async fn cancel_operation(
        &self,
        address: &str,
        operation_id: &str,
        service: &str,
    ) -> Result<()>;
}

#[derive(Clone, Debug, PartialEq)]
pub struct NexusEndpointConfig {
    pub address: String,
}

#[derive(Clone, Default)]
pub struct NexusEndpointRegistry {
    endpoints: Arc<HashMap<String, NexusEndpointConfig>>,
}

impl NexusEndpointRegistry {
    pub fn new(endpoints: HashMap<String, NexusEndpointConfig>) -> Self {
        Self {
            endpoints: Arc::new(endpoints),
        }
    }

    pub fn resolve(&self, endpoint_name: &str) -> Option<&NexusEndpointConfig> {
        self.endpoints.get(endpoint_name)
    }
}

pub struct NoopNexusHttpClient;

#[async_trait]
impl NexusHttpClient for NoopNexusHttpClient {
    async fn start_operation(
        &self,
        _address: &str,
        _operation_id: &str,
        _service: &str,
        _operation: &str,
        _input: &Payloads,
        _schedule_to_close_timeout: Option<Duration>,
    ) -> Result<NexusStartResult> {
        Err(anyhow!("nexus http client not configured"))
    }

    async fn cancel_operation(
        &self,
        _address: &str,
        _operation_id: &str,
        _service: &str,
    ) -> Result<()> {
        Err(anyhow!("nexus http client not configured"))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct NexusTimeoutEntry {
    pub run_key: RunKey,
    pub operation_id: String,
    pub scheduled_event_id: i64,
    pub schedule_to_close_timeout: Duration,
    pub scheduled_at: OffsetDateTime,
}

#[derive(Clone, Default)]
pub struct NexusTimeoutTrackingState {
    inner: Arc<Mutex<HashMap<(RunKey, String), NexusTimeoutEntry>>>,
}

impl NexusTimeoutTrackingState {
    pub fn insert(&self, entry: NexusTimeoutEntry) {
        self.inner
            .lock()
            .unwrap()
            .insert((entry.run_key, entry.operation_id.clone()), entry);
    }

    pub fn remove(&self, run_key: RunKey, operation_id: &str) {
        self.inner
            .lock()
            .unwrap()
            .remove(&(run_key, operation_id.to_string()));
    }

    pub fn remove_all_for_run(&self, run_key: RunKey) {
        self.inner
            .lock()
            .unwrap()
            .retain(|(candidate, _), _| *candidate != run_key);
    }

    pub fn snapshot(&self) -> Vec<NexusTimeoutEntry> {
        self.inner.lock().unwrap().values().cloned().collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NexusTimeoutScannerConfig {
    pub scan_interval: tokio::time::Duration,
    pub max_timeouts_per_scan: usize,
}

impl Default for NexusTimeoutScannerConfig {
    fn default() -> Self {
        Self {
            scan_interval: tokio::time::Duration::from_secs(1),
            max_timeouts_per_scan: 100,
        }
    }
}

pub fn evaluate_nexus_timeout(entry: &NexusTimeoutEntry, now: OffsetDateTime) -> bool {
    let elapsed = now - entry.scheduled_at;
    elapsed > entry.schedule_to_close_timeout
        || (entry.schedule_to_close_timeout.is_zero() && now >= entry.scheduled_at)
}

pub(crate) async fn scan_nexus_timeouts_once<F, Fut>(
    tracking: &NexusTimeoutTrackingState,
    config: &NexusTimeoutScannerConfig,
    mut submit_timeout: F,
) where
    F: FnMut(NexusTimeoutEntry, OffsetDateTime) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let now = OffsetDateTime::now_utc();
    let entries = tracking.snapshot();
    let mut submitted = 0usize;

    for entry in entries {
        if submitted >= config.max_timeouts_per_scan {
            break;
        }
        if !evaluate_nexus_timeout(&entry, now) {
            continue;
        }

        match submit_timeout(entry.clone(), now).await {
            Ok(()) => tracking.remove(entry.run_key, &entry.operation_id),
            Err(error) => {
                let message = error.to_string();
                if message.contains("kernel rejected") {
                    tracing::debug!(
                        ?error,
                        run_key = ?entry.run_key,
                        operation_id = entry.operation_id,
                        "nexus timeout scanner timeout rejected by kernel"
                    );
                    tracking.remove(entry.run_key, &entry.operation_id);
                } else {
                    tracing::warn!(
                        ?error,
                        run_key = ?entry.run_key,
                        operation_id = entry.operation_id,
                        "nexus timeout scanner failed to submit timeout"
                    );
                }
            }
        }
        submitted += 1;
    }
}

pub(crate) async fn run_nexus_timeout_scanner(
    tracking: NexusTimeoutTrackingState,
    lanes: Vec<LaneHandle>,
    lane_count: usize,
    config: NexusTimeoutScannerConfig,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(config.scan_interval) => {}
        }

        scan_nexus_timeouts_once(&tracking, &config, |entry, now| {
            let lane = pick_lane(&lanes, lane_count, entry.run_key).clone();
            async move {
                lane.submit(
                    entry.run_key,
                    Command::NexusOperationResolved(
                        tokeira_kernel::NexusOperationResolvedRequest {
                            operation_id: entry.operation_id,
                            scheduled_event_id: entry.scheduled_event_id,
                            resolution: tokeira_kernel::NexusResolution::TimedOut,
                            now,
                        },
                    ),
                )
                .await
                .map(|_| ())
            }
        })
        .await;
    }
}
