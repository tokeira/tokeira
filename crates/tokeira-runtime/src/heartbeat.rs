//! In-memory worker heartbeat store.
//!
//! Owns the runtime's view of which workers are alive: a process-local map of
//! `(namespace, worker instance) -> latest heartbeat`, plus the background
//! maintenance loop that ages entries out and emits observability metrics.
//!
//! This state is deliberately **volatile and non-authoritative**. Heartbeats
//! describe transient worker liveness, not workflow history, so losing the
//! whole map on restart is correct behaviour — workers re-announce on their
//! next heartbeat. Nothing in the correctness path (dispatch, transitions,
//! projection) may read this store to make a decision; it exists only to power
//! operator-facing observability (worker listings, last-seen age, observed
//! counts). A single short-lived lock makes repeated heartbeat requests
//! atomically visible without putting this volatile state on the durable path.
//!
//! Two eviction forces keep the map bounded: TTL (entries older than the TTL
//! are stale and removed) and capacity (a hard ceiling on total entries). See
//! [`InMemoryHeartbeatStore::maintain`] for why capacity eviction additionally
//! respects a minimum age, and [`InMemoryHeartbeatStore::insert`] for why
//! `last_seen` is held monotonic under last-write-wins.

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};
use time::OffsetDateTime;
use tokeira_types::{
    EvictionReport, HeartbeatStore, HeartbeatStoreError, NamespaceId, WorkerHeartbeat,
    WorkerInstanceKey,
};
use tokio_util::sync::CancellationToken;

use crate::metrics as runtime_metrics;

/// Age past which a heartbeat is considered stale and TTL-evicted by
/// [`maintain`](InMemoryHeartbeatStore::maintain). Five minutes is Temporal
/// v1.31.0's stock `WorkerRegistryTTL` and bounds how long an abruptly vanished
/// worker remains visible (`common/dynamicconfig/constants.go @ v1.31.0`).
pub const DEFAULT_ENTRY_TTL: time::Duration = time::Duration::minutes(5);
/// Minimum age an entry must reach before it is eligible for *capacity* (not
/// TTL) eviction. This protects freshly-seen workers from being dropped just
/// because the map is momentarily over capacity — evicting an entry that is
/// still actively heartbeating would make the worker flicker out of operator
/// views and immediately reappear on its next heartbeat.
pub const DEFAULT_MIN_EVICT_AGE: time::Duration = time::Duration::minutes(1);
/// Hard ceiling on tracked entries. Bounds memory for an unbounded, untrusted
/// population of worker instances; the store is observability state, so shedding
/// the oldest entries past this cap is preferable to growing without limit.
pub const DEFAULT_MAX_ENTRIES: usize = 1_000_000;
/// Cadence of the background maintenance loop (TTL/capacity eviction plus metric
/// emission). Short relative to the TTL so stale entries and metrics are never
/// far behind reality.
pub const DEFAULT_MAINTENANCE_INTERVAL: time::Duration = time::Duration::minutes(1);

// A final heartbeat is a removal instruction, not a queryable tombstone
// (`service/matching/workers/registry_impl.go:76-108 @ v1.31.0`). Keeping the
// raw value here avoids a runtime dependency on Temporal's generated enums.
const WORKER_STATUS_SHUTDOWN: i32 = 3;

type WorkerHeartbeatKey = (NamespaceId, WorkerInstanceKey);

/// Process-local [`HeartbeatStore`] backed by one batch-atomic map.
///
/// Holds the latest heartbeat per `(namespace, worker instance)`. It is volatile
/// observability state — see the module docs — and never consulted on the
/// correctness path. Bounded by the TTL and capacity policy applied in
/// [`maintain`](Self::maintain).
#[derive(Debug, Default)]
pub struct InMemoryHeartbeatStore {
    entries: RwLock<HashMap<WorkerHeartbeatKey, WorkerHeartbeat>>,
}

impl InMemoryHeartbeatStore {
    /// Create an empty store. Entries appear only as workers heartbeat in.
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
        self.insert_batch(vec![heartbeat])
    }

    fn insert_batch(&self, heartbeats: Vec<WorkerHeartbeat>) -> Result<(), HeartbeatStoreError> {
        for heartbeat in &heartbeats {
            validate_heartbeat(heartbeat)?;
        }
        let mut entries = self.entries.write().expect("heartbeat store poisoned");
        for heartbeat in heartbeats {
            apply_heartbeat(&mut entries, heartbeat);
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
            .read()
            .expect("heartbeat store poisoned")
            .get(&(*namespace, worker_instance_key.clone()))
            .cloned())
    }

    fn list_workers(
        &self,
        namespace: &NamespaceId,
    ) -> Result<Vec<WorkerHeartbeat>, HeartbeatStoreError> {
        Ok(self
            .entries
            .read()
            .expect("heartbeat store poisoned")
            .iter()
            .filter(|(key, _)| key.0 == *namespace)
            .map(|(_, heartbeat)| heartbeat.clone())
            .collect())
    }

    fn maintain(
        &self,
        now: OffsetDateTime,
        ttl: time::Duration,
        min_evict_age: time::Duration,
        max_entries: usize,
    ) -> Result<EvictionReport, HeartbeatStoreError> {
        let mut entries = self.entries.write().expect("heartbeat store poisoned");
        let cutoff = now - ttl;
        let mut ttl_evicted = Vec::new();
        for (key, heartbeat) in entries.iter() {
            if heartbeat.last_seen < cutoff {
                ttl_evicted.push(key.clone());
            }
        }
        for key in &ttl_evicted {
            entries.remove(key);
        }

        let mut capacity_evicted = Vec::new();
        if entries.len() > max_entries {
            // Capacity eviction only targets entries already older than
            // `min_evict_age`. Evicting a still-fresh worker to honour the cap
            // would drop someone actively heartbeating — they would vanish from
            // operator views and reappear seconds later, churning metrics for no
            // benefit. If every entry is fresh we accept a transient overshoot of
            // the cap rather than evict live workers; TTL will reclaim them once
            // they age out.
            let min_cutoff = now - min_evict_age;
            let mut candidates: Vec<_> = entries
                .iter()
                .filter(|(_, heartbeat)| heartbeat.last_seen <= min_cutoff)
                .map(|(key, heartbeat)| (key.clone(), heartbeat.last_seen))
                .collect();
            // Oldest-first, with key as a deterministic tiebreak so the report is
            // reproducible regardless of DashMap iteration order.
            candidates.sort_by(|(left_key, left_seen), (right_key, right_seen)| {
                left_seen
                    .cmp(right_seen)
                    .then_with(|| left_key.0.0.cmp(&right_key.0.0))
                    .then_with(|| left_key.1.0.cmp(&right_key.1.0))
            });
            for (key, _) in candidates {
                if entries.len() <= max_entries {
                    break;
                }
                if entries.remove(&key).is_some() {
                    capacity_evicted.push(key);
                }
            }
        }

        let mut live: Vec<_> = entries.values().cloned().collect();
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

fn validate_heartbeat(heartbeat: &WorkerHeartbeat) -> Result<(), HeartbeatStoreError> {
    if heartbeat.worker_instance_key.0.trim().is_empty() {
        return Err(HeartbeatStoreError::Invalid(
            "worker_instance_key must not be empty".to_owned(),
        ));
    }
    if heartbeat.task_queue.0.trim().is_empty() {
        return Err(HeartbeatStoreError::Invalid(
            "task_queue must not be empty".to_owned(),
        ));
    }
    Ok(())
}

fn apply_heartbeat(
    entries: &mut HashMap<WorkerHeartbeatKey, WorkerHeartbeat>,
    heartbeat: WorkerHeartbeat,
) {
    let key = (
        heartbeat.namespace_id,
        heartbeat.worker_instance_key.clone(),
    );
    if heartbeat.status.0 == WORKER_STATUS_SHUTDOWN {
        entries.remove(&key);
        return;
    }
    match entries.get_mut(&key) {
        // Last-write-wins on the body (status, build/SDK metadata) but
        // monotonic on `last_seen`: heartbeats can arrive out of order
        // (retries, multiplexed connections, clock skew across the worker
        // fleet), and a later-delivered-but-older sample must not pull the
        // observed liveness backwards.
        Some(existing) if existing.last_seen > heartbeat.last_seen => {
            let last_seen = existing.last_seen;
            *existing = heartbeat;
            existing.last_seen = last_seen;
        }
        Some(existing) => {
            *existing = heartbeat;
        }
        None => {
            entries.insert(key, heartbeat);
        }
    }
}

/// Spawn the background loop that periodically calls
/// [`HeartbeatStore::maintain`] and publishes the resulting metrics.
///
/// Runs until `cancel` fires. Errors from a single maintenance tick are logged
/// and the loop continues — a failed eviction pass degrades observability
/// freshness but must never take down the runtime, since this state is not on
/// the correctness path.
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

/// Translate an [`EvictionReport`] into observability metrics: per-worker
/// last-heartbeat age and active flag for survivors, an inactive flag for
/// everything evicted this pass, and per-namespace plus total observed counts.
///
/// `now` is threaded in (rather than read here) so the reported ages match the
/// instant the maintenance pass used for its TTL/capacity decisions.
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
    use proptest::prelude::*;
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
            encoded_heartbeat: Vec::new(),
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

    #[test]
    fn shutdown_heartbeat_removes_only_the_matching_worker_and_is_idempotent() {
        // Feature: worker-heartbeat-observability, Property 10: shutdown removal.
        let store = InMemoryHeartbeatStore::new();
        let namespace_id = namespace(1);
        let now = OffsetDateTime::UNIX_EPOCH;
        store.insert(heartbeat(namespace_id, "a", now)).unwrap();
        store.insert(heartbeat(namespace_id, "b", now)).unwrap();

        let mut shutdown = heartbeat(namespace_id, "a", now);
        shutdown.status = WorkerHeartbeatStatus(WORKER_STATUS_SHUTDOWN);
        store.insert(shutdown.clone()).unwrap();
        store.insert(shutdown).unwrap();

        assert!(
            store
                .get_worker(&namespace_id, &WorkerInstanceKey("a".to_string()))
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .get_worker(&namespace_id, &WorkerInstanceKey("b".to_string()))
                .unwrap()
                .is_some()
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn property_heartbeat_batch_atomicity(
            existing_keys in proptest::collection::btree_set("[a-z]{1,8}", 0..8),
            batch_keys in proptest::collection::vec("[a-z]{1,8}", 0..8),
            invalid_index in proptest::option::of(0_usize..8),
        ) {
            // Feature: scoped-worker-authorization, Property 8: Heartbeat batch atomicity
            let store = InMemoryHeartbeatStore::new();
            let namespace_id = namespace(1);
            let now = OffsetDateTime::UNIX_EPOCH;
            for key in &existing_keys {
                store.insert(heartbeat(namespace_id, key, now)).expect("seed");
            }
            let mut batch = batch_keys
                .iter()
                .map(|key| heartbeat(namespace_id, key, now + time::Duration::seconds(1)))
                .collect::<Vec<_>>();
            let invalid = invalid_index.filter(|index| *index < batch.len());
            if let Some(index) = invalid {
                batch[index].worker_instance_key = WorkerInstanceKey(String::new());
            }

            let result = store.insert_batch(batch);
            let actual = store
                .list_workers(&namespace_id)
                .expect("list")
                .into_iter()
                .map(|heartbeat| heartbeat.worker_instance_key.0)
                .collect::<std::collections::BTreeSet<_>>();
            let mut expected = existing_keys;
            if invalid.is_none() {
                expected.extend(batch_keys);
                prop_assert!(result.is_ok());
            } else {
                prop_assert!(matches!(result, Err(HeartbeatStoreError::Invalid(_))));
            }
            prop_assert_eq!(actual, expected);
        }
    }
}
