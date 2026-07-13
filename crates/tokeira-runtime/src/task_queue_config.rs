//! Per-task-queue runtime configuration store.
//!
//! Owns the runtime's mutable, per-`(namespace, task queue)` tuning knobs:
//! queue-level rate limits and fairness weights. These are operator-set
//! overrides that shape dispatch *throughput and fairness* — they tune how fast
//! and in what proportion work is handed out, never *what* work is correct. This
//! is configuration state, not authoritative history, which is why the default
//! implementation is a volatile in-memory map: losing it on restart reverts to
//! built-in defaults rather than corrupting any run.
//!
//! Entries are keyed by `(NamespaceId, TaskQueueName)` and scoped to a namespace
//! on listing, so one namespace's overrides never leak into another's dispatch.

use std::collections::BTreeMap;

use dashmap::DashMap;
use time::OffsetDateTime;
use tokeira_types::{NamespaceId, TaskQueueName};

/// Operator-set dispatch tuning for a single task queue.
#[derive(Clone, Debug, PartialEq)]
pub struct TaskQueueConfigEntry {
    /// Namespace this entry belongs to.
    pub namespace_id: NamespaceId,
    /// Task queue this entry tunes.
    pub task_queue: TaskQueueName,
    /// Optional ceiling on total dispatch rate for the queue (tasks/sec). `None`
    /// leaves the queue at the built-in default.
    pub queue_rate_limit: Option<f32>,
    /// Metadata is retained even when the update explicitly unsets the limit.
    pub queue_rate_limit_metadata: Option<TaskQueueConfigMetadata>,
    /// Optional default per-fairness-key rate limit applied when a key has no
    /// explicit override.
    pub fairness_key_rate_limit_default: Option<f32>,
    /// Metadata for the default fairness-key limit update/unset.
    pub fairness_key_rate_limit_metadata: Option<TaskQueueConfigMetadata>,
    /// Per-fairness-key weight overrides, keyed by fairness key. Weights bias the
    /// share of dispatch a key receives relative to others.
    pub fairness_weight_overrides: BTreeMap<String, f32>,
}

/// Audit metadata attached to a task-queue configuration update.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskQueueConfigMetadata {
    /// Caller-supplied reason.
    pub reason: String,
    /// Identity that issued the update.
    pub update_identity: String,
    /// Time the update was accepted.
    pub update_time: OffsetDateTime,
}

/// v1.31.0's maximum fairness-weight override count (default 1000).
const MAX_FAIRNESS_WEIGHT_OVERRIDES: usize = 1000;

/// Live limit used by the UpdateTaskQueueConfig admission path.
#[cfg(not(feature = "conformance"))]
pub fn max_fairness_weight_overrides() -> usize {
    MAX_FAIRNESS_WEIGHT_OVERRIDES
}

/// Conformance builds honour the corpus's per-test override at the live edge
/// admission site; production remains pinned to the release default.
#[cfg(feature = "conformance")]
pub fn max_fairness_weight_overrides() -> usize {
    tokeira_conformance::overrides()
        .get_i64("matching.maxFairnessKeyWeightOverrides")
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(MAX_FAIRNESS_WEIGHT_OVERRIDES)
}

/// Storage for per-task-queue configuration entries.
///
/// Reads happen on the dispatch path, so implementations must be cheap to query
/// and safe for concurrent access. The trait is namespace-scoped: [`list`](Self::list)
/// returns only the requested namespace's entries.
pub trait TaskQueueConfigStore: Send + Sync + 'static {
    /// Fetch the entry for a specific queue, or `None` if no override is set.
    fn get(
        &self,
        namespace_id: &NamespaceId,
        task_queue: &TaskQueueName,
    ) -> Option<TaskQueueConfigEntry>;

    /// Insert or replace the entry for its `(namespace, task queue)` key.
    fn set(&self, entry: TaskQueueConfigEntry);

    /// List all entries for a namespace.
    fn list(&self, namespace_id: &NamespaceId) -> Vec<TaskQueueConfigEntry>;
}

/// Process-local [`TaskQueueConfigStore`] backed by a lock-free map.
///
/// The default store. Volatile by design — see the module docs: task-queue
/// config is tuning, not authoritative state, so it is safe to rebuild from
/// defaults after a restart.
#[derive(Debug, Default)]
pub struct InMemoryTaskQueueConfigStore {
    entries: DashMap<(NamespaceId, TaskQueueName), TaskQueueConfigEntry>,
}

impl InMemoryTaskQueueConfigStore {
    /// Create an empty store. Queues use built-in defaults until an entry is set.
    pub fn new() -> Self {
        Self::default()
    }
}

impl TaskQueueConfigStore for InMemoryTaskQueueConfigStore {
    fn get(
        &self,
        namespace_id: &NamespaceId,
        task_queue: &TaskQueueName,
    ) -> Option<TaskQueueConfigEntry> {
        self.entries
            .get(&(*namespace_id, task_queue.clone()))
            .map(|entry| entry.clone())
    }

    fn set(&self, entry: TaskQueueConfigEntry) {
        self.entries
            .insert((entry.namespace_id, entry.task_queue.clone()), entry);
    }

    fn list(&self, namespace_id: &NamespaceId) -> Vec<TaskQueueConfigEntry> {
        self.entries
            .iter()
            .filter(|entry| entry.key().0 == *namespace_id)
            .map(|entry| entry.value().clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_get_round_trip_is_key_isolated() {
        let store = InMemoryTaskQueueConfigStore::new();
        let namespace_a = NamespaceId(uuid::Uuid::from_u128(1));
        let namespace_b = NamespaceId(uuid::Uuid::from_u128(2));
        let queue = TaskQueueName("main".to_string());
        let entry = TaskQueueConfigEntry {
            namespace_id: namespace_a,
            task_queue: queue.clone(),
            queue_rate_limit: Some(10.0),
            queue_rate_limit_metadata: None,
            fairness_key_rate_limit_default: Some(5.0),
            fairness_key_rate_limit_metadata: None,
            fairness_weight_overrides: BTreeMap::from([("tenant-a".to_string(), 2.0)]),
        };

        store.set(entry.clone());

        assert_eq!(store.get(&namespace_a, &queue), Some(entry));
        assert_eq!(store.get(&namespace_b, &queue), None);
    }
}
