//! Nexus operation types, endpoint registry, and timeout scanning.
//!
//! Contains the HTTP client trait for Nexus operations, endpoint configuration
//! and registry, timeout tracking state, and the background scanner that
//! detects timed-out Nexus operations.

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex, RwLock},
};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use opentelemetry::KeyValue;
use serde::{Deserialize, Serialize};
use time::{Duration, OffsetDateTime};
use tokeira_kernel::Command;
use tokeira_types::{NamespaceId, Payload, Payloads, RunKey, ShardId, TaskQueueName};
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio_util::sync::CancellationToken;

use crate::lane::LaneHandle;
use crate::metrics as runtime_metrics;
use crate::scanner::pick_lane;
use crate::shard::ShardOwner;

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
        trace_headers: &[KeyValue],
    ) -> Result<NexusStartResult>;

    async fn cancel_operation(
        &self,
        address: &str,
        operation_id: &str,
        service: &str,
        trace_headers: &[KeyValue],
    ) -> Result<()>;
}

#[derive(Clone, Debug, PartialEq)]
pub enum EndpointTarget {
    External {
        address: String,
    },
    Worker {
        namespace_id: NamespaceId,
        task_queue: TaskQueueName,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct NexusEndpointConfig {
    pub target: EndpointTarget,
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NexusTaskToken {
    pub run_key: RunKey,
    pub operation_id: String,
    pub scheduled_event_id: i64,
}

impl NexusTaskToken {
    pub fn encode(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(Into::into)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes)
            .map_err(|error| anyhow!("invalid nexus task token: {error}"))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum NexusTaskRequest {
    StartOperation {
        service: String,
        operation: String,
        request_id: String,
        payload: Option<Payload>,
        scheduled_time: Option<OffsetDateTime>,
    },
    CancelOperation {
        service: String,
        operation: String,
        operation_id: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct NexusTask {
    pub token: NexusTaskToken,
    pub request: NexusTaskRequest,
}

#[derive(Default, Clone)]
pub struct NexusTaskBroker {
    inner: Arc<AsyncMutex<NexusBrokerState>>,
    wake: Arc<Notify>,
}

#[derive(Default)]
struct NexusBrokerState {
    ready: HashMap<(NamespaceId, TaskQueueName), VecDeque<NexusTask>>,
}

impl NexusTaskBroker {
    pub async fn publish(
        &self,
        namespace_id: NamespaceId,
        task_queue: TaskQueueName,
        task: NexusTask,
    ) {
        let mut inner = self.inner.lock().await;
        inner
            .ready
            .entry((namespace_id, task_queue))
            .or_default()
            .push_back(task);
        drop(inner);
        self.wake.notify_waiters();
    }

    pub async fn poll(
        &self,
        namespace_id: NamespaceId,
        task_queue: TaskQueueName,
        wait_for: tokio::time::Duration,
    ) -> Option<NexusTask> {
        if let Some(task) = self.try_take(namespace_id, &task_queue).await {
            return Some(task);
        }

        let notified = self.wake.notified();
        tokio::pin!(notified);

        if let Some(task) = self.try_take(namespace_id, &task_queue).await {
            return Some(task);
        }

        if tokio::time::timeout(wait_for, notified).await.is_err() {
            return None;
        }

        self.try_take(namespace_id, &task_queue).await
    }

    async fn try_take(
        &self,
        namespace_id: NamespaceId,
        task_queue: &TaskQueueName,
    ) -> Option<NexusTask> {
        let mut inner = self.inner.lock().await;
        inner
            .ready
            .get_mut(&(namespace_id, task_queue.clone()))
            .and_then(VecDeque::pop_front)
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
        _trace_headers: &[KeyValue],
    ) -> Result<NexusStartResult> {
        Err(anyhow!("nexus http client not configured"))
    }

    async fn cancel_operation(
        &self,
        _address: &str,
        _operation_id: &str,
        _service: &str,
        _trace_headers: &[KeyValue],
    ) -> Result<()> {
        Err(anyhow!("nexus http client not configured"))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct NexusTimeoutEntry {
    pub run_key: RunKey,
    pub shard_id: ShardId,
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

    pub fn remove_all_for_shard(&self, shard_id: ShardId) {
        self.inner
            .lock()
            .unwrap()
            .retain(|_, entry| entry.shard_id != shard_id);
    }

    pub fn snapshot(&self) -> Vec<NexusTimeoutEntry> {
        self.inner.lock().unwrap().values().cloned().collect()
    }

    pub fn snapshot_for_shard(&self, shard_id: ShardId) -> Vec<NexusTimeoutEntry> {
        self.inner
            .lock()
            .unwrap()
            .values()
            .filter(|entry| entry.shard_id == shard_id)
            .cloned()
            .collect()
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
    shard_id: Option<ShardId>,
    config: &NexusTimeoutScannerConfig,
    mut submit_timeout: F,
) where
    F: FnMut(NexusTimeoutEntry, OffsetDateTime) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let now = OffsetDateTime::now_utc();
    let entries = match shard_id {
        Some(shard_id) => tracking.snapshot_for_shard(shard_id),
        None => tracking.snapshot(),
    };
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
    shard_owner: Arc<RwLock<ShardOwner>>,
    config: NexusTimeoutScannerConfig,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(config.scan_interval) => {}
        }

        let active_shards: Vec<_> = shard_owner.read().unwrap().active_shards().collect();
        for shard_id in active_shards {
            runtime_metrics::record_scanner_tick("nexus_timeout", shard_id.0);
            scan_nexus_timeouts_once(&tracking, Some(shard_id), &config, |entry, now| {
                runtime_metrics::record_scanner_dispatched("nexus_timeout", shard_id.0);
                let lane = pick_lane(&lanes, lane_count, entry.shard_id).clone();
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
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use tokio::runtime::Runtime;
    use uuid::Uuid;

    use super::*;

    // Feature: edge-nexus-task-transport, Property 1: Task token round-trip
    proptest! {
        #[test]
        fn property_task_token_roundtrip(
            run in any::<u128>(),
            operation_id in "[a-z0-9_-]{1,24}",
            scheduled_event_id in 0i64..10_000,
        ) {
            let token = NexusTaskToken {
                run_key: RunKey(Uuid::from_u128(run)),
                operation_id,
                scheduled_event_id,
            };
            let encoded = token.encode().expect("token should encode");
            let decoded = NexusTaskToken::decode(&encoded).expect("token should decode");
            prop_assert_eq!(decoded, token);
        }
    }

    // Feature: edge-nexus-task-transport, Property 2: Broker queue isolation
    proptest! {
        #[test]
        fn property_broker_queue_isolation(
            namespace_seed in any::<u128>(),
            queue_suffix in "[a-z]{1,8}",
            first_operation in "[a-z0-9_-]{1,16}",
            second_operation in "[a-z0-9_-]{1,16}",
            third_operation in "[a-z0-9_-]{1,16}",
        ) {
            let rt = Runtime::new().expect("runtime");
            rt.block_on(async move {
                let broker = NexusTaskBroker::default();
                let namespace_a = NamespaceId(Uuid::from_u128(namespace_seed));
                let namespace_b = NamespaceId(Uuid::from_u128(namespace_seed.wrapping_add(1)));
                let queue_a = TaskQueueName(format!("queue-a-{queue_suffix}"));
                let queue_b = TaskQueueName(format!("queue-b-{queue_suffix}"));

                let first = NexusTask {
                    token: NexusTaskToken {
                        run_key: RunKey::new(),
                        operation_id: first_operation.clone(),
                        scheduled_event_id: 1,
                    },
                    request: NexusTaskRequest::CancelOperation {
                        service: "svc".to_string(),
                        operation: "cancel".to_string(),
                        operation_id: first_operation.clone(),
                    },
                };
                let second = NexusTask {
                    token: NexusTaskToken {
                        run_key: RunKey::new(),
                        operation_id: second_operation.clone(),
                        scheduled_event_id: 2,
                    },
                    request: NexusTaskRequest::CancelOperation {
                        service: "svc".to_string(),
                        operation: "cancel".to_string(),
                        operation_id: second_operation.clone(),
                    },
                };
                let third = NexusTask {
                    token: NexusTaskToken {
                        run_key: RunKey::new(),
                        operation_id: third_operation.clone(),
                        scheduled_event_id: 3,
                    },
                    request: NexusTaskRequest::CancelOperation {
                        service: "svc".to_string(),
                        operation: "cancel".to_string(),
                        operation_id: third_operation.clone(),
                    },
                };

                broker
                    .publish(namespace_a, queue_a.clone(), first.clone())
                    .await;
                broker
                    .publish(namespace_b, queue_b.clone(), second.clone())
                    .await;
                broker
                    .publish(namespace_a, queue_a.clone(), third.clone())
                    .await;

                let polled_second = broker
                    .poll(namespace_b, queue_b.clone(), tokio::time::Duration::from_millis(1))
                    .await
                    .expect("queue b task");
                let polled_first = broker
                    .poll(namespace_a, queue_a.clone(), tokio::time::Duration::from_millis(1))
                    .await
                    .expect("first queue a task");
                let polled_third = broker
                    .poll(namespace_a, queue_a.clone(), tokio::time::Duration::from_millis(1))
                    .await
                    .expect("second queue a task");
                let empty = broker
                    .poll(namespace_a, queue_b, tokio::time::Duration::from_millis(1))
                    .await;

                prop_assert_eq!(polled_second, second);
                prop_assert_eq!(polled_first, first);
                prop_assert_eq!(polled_third, third);
                prop_assert_eq!(empty, None);
                Ok(())
            })?;
        }
    }
}
