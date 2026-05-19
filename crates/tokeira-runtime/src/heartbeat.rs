use std::{collections::HashMap, sync::Arc};

use dashmap::DashMap;
use time::OffsetDateTime;
use tokeira_types::{
    EvictionReport, HeartbeatStore, HeartbeatStoreError, NamespaceId, WorkerHeartbeat,
    WorkerInstanceKey,
};
use tokio_util::sync::CancellationToken;

use crate::metrics as runtime_metrics;

pub const DEFAULT_ENTRY_TTL: time::Duration = time::Duration::hours(24);
pub const DEFAULT_MIN_EVICT_AGE: time::Duration = time::Duration::minutes(10);
pub const DEFAULT_MAX_ENTRIES: usize = 1_000_000;
pub const DEFAULT_MAINTENANCE_INTERVAL: time::Duration = time::Duration::seconds(10);

type WorkerHeartbeatKey = (NamespaceId, WorkerInstanceKey);

#[derive(Debug, Default)]
pub struct InMemoryHeartbeatStore {
    entries: DashMap<WorkerHeartbeatKey, WorkerHeartbeat>,
}

impl InMemoryHeartbeatStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn namespace_counts(live: &[WorkerHeartbeat]) -> Vec<(NamespaceId, usize)> {
        let mut counts = HashMap::<NamespaceId, usize>::new();
        for heartbeat in live {
            *counts.entry(heartbeat.namespace_id).or_default() += 1;
        }
        let mut counts: Vec<_> = counts.into_iter().collect();
        counts.sort_by_key(|(namespace, _)| namespace.0);
        counts
    }
}

impl HeartbeatStore for InMemoryHeartbeatStore {
    fn insert(&self, heartbeat: WorkerHeartbeat) -> Result<(), HeartbeatStoreError> {
        let key = (
            heartbeat.namespace_id,
            heartbeat.worker_instance_key.clone(),
        );
        match self.entries.get_mut(&key) {
            Some(mut existing) if existing.last_seen > heartbeat.last_seen => {
                let last_seen = existing.last_seen;
                *existing = heartbeat;
                existing.last_seen = last_seen;
            }
            Some(mut existing) => {
                *existing = heartbeat;
            }
            None => {
                self.entries.insert(key, heartbeat);
            }
        }
        Ok(())
    }

    fn get_worker(
        &self,
        namespace: &NamespaceId,
        worker_instance_key: &WorkerInstanceKey,
    ) -> Result<Option<WorkerHeartbeat>, HeartbeatStoreError> {
        Ok(self
            .entries
            .get(&(*namespace, worker_instance_key.clone()))
            .map(|entry| entry.clone()))
    }

    fn list_workers(
        &self,
        namespace: &NamespaceId,
    ) -> Result<Vec<WorkerHeartbeat>, HeartbeatStoreError> {
        Ok(self
            .entries
            .iter()
            .filter(|entry| entry.key().0 == *namespace)
            .map(|entry| entry.value().clone())
            .collect())
    }

    fn maintain(
        &self,
        now: OffsetDateTime,
        ttl: time::Duration,
        min_evict_age: time::Duration,
        max_entries: usize,
    ) -> Result<EvictionReport, HeartbeatStoreError> {
        let cutoff = now - ttl;
        let mut ttl_evicted = Vec::new();
        for entry in self.entries.iter() {
            if entry.last_seen < cutoff {
                ttl_evicted.push(entry.key().clone());
            }
        }
        for key in &ttl_evicted {
            self.entries.remove(key);
        }

        let mut capacity_evicted = Vec::new();
        if self.entries.len() > max_entries {
            let min_cutoff = now - min_evict_age;
            let mut candidates: Vec<_> = self
                .entries
                .iter()
                .filter(|entry| entry.last_seen <= min_cutoff)
                .map(|entry| (entry.key().clone(), entry.last_seen))
                .collect();
            candidates.sort_by(|(left_key, left_seen), (right_key, right_seen)| {
                left_seen
                    .cmp(right_seen)
                    .then_with(|| left_key.0.0.cmp(&right_key.0.0))
                    .then_with(|| left_key.1.0.cmp(&right_key.1.0))
            });
            for (key, _) in candidates {
                if self.entries.len() <= max_entries {
                    break;
                }
                if self.entries.remove(&key).is_some() {
                    capacity_evicted.push(key);
                }
            }
        }

        let mut live: Vec<_> = self
            .entries
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
        live.sort_by(|left, right| {
            left.worker_instance_key
                .0
                .cmp(&right.worker_instance_key.0)
                .then_with(|| left.last_seen.cmp(&right.last_seen))
        });
        let namespace_counts = Self::namespace_counts(&live);
        let remaining = live.len();
        Ok(EvictionReport {
            ttl_evicted,
            capacity_evicted,
            live,
            namespace_counts,
            remaining,
        })
    }
}

pub fn spawn_heartbeat_maintenance(
    store: Arc<dyn HeartbeatStore>,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(DEFAULT_MAINTENANCE_INTERVAL.unsigned_abs());
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = interval.tick() => {
                    let now = OffsetDateTime::now_utc();
                    match store.maintain(
                        now,
                        DEFAULT_ENTRY_TTL,
                        DEFAULT_MIN_EVICT_AGE,
                        DEFAULT_MAX_ENTRIES,
                    ) {
                        Ok(report) => record_maintenance_report(now, report),
                        Err(error) => tracing::warn!(?error, "heartbeat maintenance failed"),
                    }
                }
            }
        }
    })
}

pub fn record_maintenance_report(now: OffsetDateTime, report: EvictionReport) {
    for heartbeat in &report.live {
        runtime_metrics::record_worker_last_heartbeat_age(
            heartbeat.namespace_id,
            now - heartbeat.last_seen,
        );
        runtime_metrics::record_worker_heartbeat_active(
            heartbeat.namespace_id,
            &heartbeat.worker_instance_key,
            true,
        );
    }
    for (namespace_id, key) in report
        .ttl_evicted
        .iter()
        .chain(report.capacity_evicted.iter())
    {
        runtime_metrics::record_worker_heartbeat_active(*namespace_id, key, false);
    }
    for (namespace_id, count) in &report.namespace_counts {
        runtime_metrics::set_workers_observed(*namespace_id, *count);
    }
    runtime_metrics::set_workers_total(report.remaining);
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_types::{TaskQueueName, WorkerHeartbeatStatus, WorkerIdentity};
    use uuid::Uuid;

    fn namespace(n: u128) -> NamespaceId {
        NamespaceId(Uuid::from_u128(n))
    }

    fn heartbeat(
        namespace_id: NamespaceId,
        key: &str,
        last_seen: OffsetDateTime,
    ) -> WorkerHeartbeat {
        WorkerHeartbeat {
            namespace_id,
            worker_instance_key: WorkerInstanceKey(key.to_string()),
            task_queue: TaskQueueName("queue".to_string()),
            worker_identity: WorkerIdentity(format!("worker-{key}")),
            last_seen,
            status: WorkerHeartbeatStatus(1),
            build_id: None,
            deployment_name: None,
            sdk_name: None,
            sdk_version: None,
        }
    }

    #[test]
    fn insert_preserves_monotonic_last_seen() {
        let store = InMemoryHeartbeatStore::new();
        let namespace_id = namespace(1);
        let newer = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(10);
        let older = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(5);
        store.insert(heartbeat(namespace_id, "a", newer)).unwrap();
        store.insert(heartbeat(namespace_id, "a", older)).unwrap();
        let stored = store
            .get_worker(&namespace_id, &WorkerInstanceKey("a".to_string()))
            .unwrap()
            .unwrap();
        assert_eq!(stored.last_seen, newer);
    }

    #[test]
    fn maintain_reports_evicted_keys_and_live_counts() {
        let store = InMemoryHeartbeatStore::new();
        let namespace_id = namespace(1);
        let now = OffsetDateTime::UNIX_EPOCH + time::Duration::hours(48);
        store
            .insert(heartbeat(
                namespace_id,
                "old",
                now - time::Duration::hours(25),
            ))
            .unwrap();
        store
            .insert(heartbeat(
                namespace_id,
                "live",
                now - time::Duration::seconds(5),
            ))
            .unwrap();

        let report = store
            .maintain(
                now,
                time::Duration::hours(24),
                time::Duration::minutes(10),
                1_000_000,
            )
            .unwrap();

        assert_eq!(
            report.ttl_evicted,
            vec![(namespace_id, WorkerInstanceKey("old".to_string()))]
        );
        assert_eq!(report.live.len(), 1);
        assert_eq!(report.namespace_counts, vec![(namespace_id, 1)]);
        assert_eq!(report.remaining, 1);
    }
}
