//! Per-task-queue runtime configuration store.
//!
//! Owns the runtime's mutable, per-`(namespace, task queue, task kind)` tuning knobs:
//! queue-level rate limits and fairness weights. These are operator-set
//! overrides that shape dispatch *throughput and fairness* — they tune how fast
//! and in what proportion work is handed out, never *what* work is correct. This
//! is configuration state, not authoritative history, which is why the default
//! implementation is a volatile in-memory map: losing it on restart reverts to
//! built-in defaults rather than corrupting any run.
//!
//! Entries are task-kind isolated and namespace-scoped on listing, so an
//! activity policy cannot overwrite a same-named workflow or Nexus queue and
//! one namespace's overrides never leak into another's dispatch.

use std::{collections::BTreeMap, sync::Arc};

use dashmap::{DashMap, mapref::entry::Entry};
use thiserror::Error;
use time::OffsetDateTime;
use tokeira_types::{NamespaceId, TaskQueueName};
use tokio::sync::Notify;

/// Task category whose independently configured handout policy is addressed.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TaskQueueConfigKind {
    /// Workflow task queue.
    Workflow,
    /// Activity task queue.
    Activity,
    /// Nexus task queue.
    Nexus,
}

/// Complete identity of one task-queue configuration record.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TaskQueueConfigKey {
    /// Namespace containing the queue.
    pub namespace_id: NamespaceId,
    /// Logical task-queue name.
    pub task_queue: TaskQueueName,
    /// Independently configured task category.
    pub kind: TaskQueueConfigKind,
}

/// Operator-set dispatch tuning for a single task queue.
#[derive(Clone, Debug, PartialEq)]
pub struct TaskQueueConfigEntry {
    /// Namespace this entry belongs to.
    pub namespace_id: NamespaceId,
    /// Task queue this entry tunes.
    pub task_queue: TaskQueueName,
    /// Task category this entry configures.
    pub kind: TaskQueueConfigKind,
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

impl TaskQueueConfigEntry {
    /// Return the store key for this entry.
    #[must_use]
    pub fn key(&self) -> TaskQueueConfigKey {
        TaskQueueConfigKey {
            namespace_id: self.namespace_id,
            task_queue: self.task_queue.clone(),
            kind: self.kind,
        }
    }
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

/// One field in an atomic task-queue configuration patch.
#[derive(Clone, Debug, Default, PartialEq)]
pub enum TaskQueueConfigFieldPatch<T> {
    /// Preserve the current field.
    #[default]
    Unchanged,
    /// Replace the field.
    Set(T),
}

/// Atomic task-queue configuration mutation.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TaskQueueConfigPatch {
    /// Queue-wide rate mutation and its audit metadata.
    pub queue_rate_limit: TaskQueueConfigFieldPatch<(Option<f32>, TaskQueueConfigMetadata)>,
    /// Per-fairness-key default rate mutation and its audit metadata.
    pub fairness_key_rate_limit_default:
        TaskQueueConfigFieldPatch<(Option<f32>, TaskQueueConfigMetadata)>,
    /// Overrides to add or replace.
    pub set_fairness_weight_overrides: BTreeMap<String, f32>,
    /// Overrides to remove.
    pub unset_fairness_weight_overrides: Vec<String>,
}

/// Rejection from an atomic task-queue configuration update.
#[derive(Clone, Debug, Error, PartialEq)]
pub enum TaskQueueConfigError {
    /// Workflow handout cannot be rate-limited.
    #[error("Setting rate limit on workflow task queues is not allowed.")]
    WorkflowQueueRateLimit,
    /// Workflow fairness-key handout cannot be rate-limited.
    #[error("Setting fairness key rate limit on workflow task queues is not allowed.")]
    WorkflowFairnessKeyRateLimit,
    /// A supplied rate was below zero.
    #[error("RequestsPerSecond for {label} rate limit must be non-negative.")]
    NegativeRate {
        /// Public update-field label.
        label: &'static str,
    },
    /// A fairness override key was empty.
    #[error("fairness weight override key must not be empty")]
    EmptyFairnessKey,
    /// A fairness override key exceeded 64 encoded bytes.
    #[error("fairness key length exceeds limit")]
    FairnessKeyTooLong,
    /// A fairness override weight was not strictly positive.
    #[error("invalid fairness weight weight for key {key:?}: must be greater than zero")]
    InvalidFairnessWeight {
        /// Invalid fairness key.
        key: String,
    },
    /// One key appeared in both mutation sets.
    #[error("fairness weight override key {key:?} present in both set and unset lists")]
    SetUnsetConflict {
        /// Conflicting key.
        key: String,
    },
    /// Request or resulting configuration exceeded the active cap.
    #[error("too many fairness weight overrides in request: got {got}, maximum {maximum}")]
    TooManyOverrides {
        /// Observed count.
        got: usize,
        /// Active maximum.
        maximum: usize,
    },
    /// Applying an otherwise valid patch would leave too many stored overrides.
    #[error("fairness weight overrides update rejected: exceeding maximum key size")]
    FairnessOverridesUpdateRejected,
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
    fn get(&self, key: &TaskQueueConfigKey) -> Option<TaskQueueConfigEntry>;

    /// Validate and apply one patch atomically.
    fn apply(
        &self,
        key: TaskQueueConfigKey,
        patch: TaskQueueConfigPatch,
        max_overrides: usize,
    ) -> Result<TaskQueueConfigEntry, TaskQueueConfigError>;

    /// List all entries for a namespace.
    fn list(&self, namespace_id: &NamespaceId) -> Vec<TaskQueueConfigEntry>;

    /// Return the process-local wake used by blocked handout decisions.
    fn changed(&self, key: &TaskQueueConfigKey) -> Arc<Notify>;
}

/// Process-local [`TaskQueueConfigStore`] backed by a lock-free map.
///
/// The default store. Volatile by design — see the module docs: task-queue
/// config is tuning, not authoritative state, so it is safe to rebuild from
/// defaults after a restart.
#[derive(Debug, Default)]
pub struct InMemoryTaskQueueConfigStore {
    entries: DashMap<TaskQueueConfigKey, TaskQueueConfigEntry>,
    changes: DashMap<TaskQueueConfigKey, Arc<Notify>>,
}

impl InMemoryTaskQueueConfigStore {
    /// Create an empty store. Queues use built-in defaults until an entry is set.
    pub fn new() -> Self {
        Self::default()
    }
}

impl TaskQueueConfigStore for InMemoryTaskQueueConfigStore {
    fn get(&self, key: &TaskQueueConfigKey) -> Option<TaskQueueConfigEntry> {
        self.entries.get(key).map(|entry| entry.clone())
    }

    fn apply(
        &self,
        key: TaskQueueConfigKey,
        patch: TaskQueueConfigPatch,
        max_overrides: usize,
    ) -> Result<TaskQueueConfigEntry, TaskQueueConfigError> {
        validate_patch(&key, &patch, max_overrides)?;
        let updated = match self.entries.entry(key.clone()) {
            Entry::Occupied(mut occupied) => {
                let mut candidate = occupied.get().clone();
                apply_patch(&mut candidate, patch);
                validate_result(&candidate, max_overrides)?;
                occupied.insert(candidate.clone());
                candidate
            }
            Entry::Vacant(vacant) => {
                let mut candidate = TaskQueueConfigEntry {
                    namespace_id: key.namespace_id,
                    task_queue: key.task_queue.clone(),
                    kind: key.kind,
                    queue_rate_limit: None,
                    queue_rate_limit_metadata: None,
                    fairness_key_rate_limit_default: None,
                    fairness_key_rate_limit_metadata: None,
                    fairness_weight_overrides: BTreeMap::new(),
                };
                apply_patch(&mut candidate, patch);
                validate_result(&candidate, max_overrides)?;
                vacant.insert(candidate.clone());
                candidate
            }
        };
        self.changed(&key).notify_waiters();
        Ok(updated)
    }

    fn list(&self, namespace_id: &NamespaceId) -> Vec<TaskQueueConfigEntry> {
        self.entries
            .iter()
            .filter(|entry| entry.key().namespace_id == *namespace_id)
            .map(|entry| entry.value().clone())
            .collect()
    }

    fn changed(&self, key: &TaskQueueConfigKey) -> Arc<Notify> {
        self.changes
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone()
    }
}

fn validate_patch(
    key: &TaskQueueConfigKey,
    patch: &TaskQueueConfigPatch,
    max_overrides: usize,
) -> Result<(), TaskQueueConfigError> {
    if key.kind == TaskQueueConfigKind::Workflow {
        if matches!(
            patch.queue_rate_limit,
            TaskQueueConfigFieldPatch::Set((Some(_), _))
        ) {
            return Err(TaskQueueConfigError::WorkflowQueueRateLimit);
        }
        if matches!(
            patch.fairness_key_rate_limit_default,
            TaskQueueConfigFieldPatch::Set((Some(_), _))
        ) {
            return Err(TaskQueueConfigError::WorkflowFairnessKeyRateLimit);
        }
    }
    validate_rate(&patch.queue_rate_limit, "UpdateQueueRateLimit")?;
    validate_rate(
        &patch.fairness_key_rate_limit_default,
        "UpdateFairnessKeyRateLimitDefault",
    )?;
    let request_count =
        patch.set_fairness_weight_overrides.len() + patch.unset_fairness_weight_overrides.len();
    if request_count > max_overrides {
        return Err(TaskQueueConfigError::TooManyOverrides {
            got: request_count,
            maximum: max_overrides,
        });
    }
    for (fairness_key, weight) in &patch.set_fairness_weight_overrides {
        validate_fairness_key(fairness_key)?;
        if !weight.is_finite() || *weight <= 0.0 {
            return Err(TaskQueueConfigError::InvalidFairnessWeight {
                key: fairness_key.clone(),
            });
        }
        if patch.unset_fairness_weight_overrides.contains(fairness_key) {
            return Err(TaskQueueConfigError::SetUnsetConflict {
                key: fairness_key.clone(),
            });
        }
    }
    for fairness_key in &patch.unset_fairness_weight_overrides {
        validate_fairness_key(fairness_key)?;
    }
    Ok(())
}

fn validate_rate(
    patch: &TaskQueueConfigFieldPatch<(Option<f32>, TaskQueueConfigMetadata)>,
    label: &'static str,
) -> Result<(), TaskQueueConfigError> {
    if let TaskQueueConfigFieldPatch::Set((Some(rate), _)) = patch
        && (!rate.is_finite() || *rate < 0.0)
    {
        return Err(TaskQueueConfigError::NegativeRate { label });
    }
    Ok(())
}

fn validate_fairness_key(fairness_key: &str) -> Result<(), TaskQueueConfigError> {
    if fairness_key.is_empty() {
        return Err(TaskQueueConfigError::EmptyFairnessKey);
    }
    if fairness_key.len() > 64 {
        return Err(TaskQueueConfigError::FairnessKeyTooLong);
    }
    Ok(())
}

fn apply_patch(entry: &mut TaskQueueConfigEntry, patch: TaskQueueConfigPatch) {
    if let TaskQueueConfigFieldPatch::Set((rate, metadata)) = patch.queue_rate_limit {
        entry.queue_rate_limit = rate;
        entry.queue_rate_limit_metadata = Some(metadata);
    }
    if let TaskQueueConfigFieldPatch::Set((rate, metadata)) = patch.fairness_key_rate_limit_default
    {
        entry.fairness_key_rate_limit_default = rate;
        entry.fairness_key_rate_limit_metadata = Some(metadata);
    }
    for fairness_key in patch.unset_fairness_weight_overrides {
        entry.fairness_weight_overrides.remove(&fairness_key);
    }
    entry
        .fairness_weight_overrides
        .extend(patch.set_fairness_weight_overrides);
}

fn validate_result(
    entry: &TaskQueueConfigEntry,
    max_overrides: usize,
) -> Result<(), TaskQueueConfigError> {
    if entry.fairness_weight_overrides.len() > max_overrides {
        return Err(TaskQueueConfigError::FairnessOverridesUpdateRejected);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use proptest::prelude::*;

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
            kind: TaskQueueConfigKind::Activity,
            queue_rate_limit: Some(10.0),
            queue_rate_limit_metadata: None,
            fairness_key_rate_limit_default: Some(5.0),
            fairness_key_rate_limit_metadata: None,
            fairness_weight_overrides: BTreeMap::from([("tenant-a".to_string(), 2.0)]),
        };

        store
            .apply(
                entry.key(),
                TaskQueueConfigPatch {
                    queue_rate_limit: TaskQueueConfigFieldPatch::Set((
                        entry.queue_rate_limit,
                        TaskQueueConfigMetadata {
                            reason: "test".to_string(),
                            update_identity: "test".to_string(),
                            update_time: OffsetDateTime::UNIX_EPOCH,
                        },
                    )),
                    fairness_key_rate_limit_default: TaskQueueConfigFieldPatch::Set((
                        entry.fairness_key_rate_limit_default,
                        TaskQueueConfigMetadata {
                            reason: "test".to_string(),
                            update_identity: "test".to_string(),
                            update_time: OffsetDateTime::UNIX_EPOCH,
                        },
                    )),
                    set_fairness_weight_overrides: entry.fairness_weight_overrides.clone(),
                    unset_fairness_weight_overrides: Vec::new(),
                },
                1000,
            )
            .expect("valid patch");

        assert_eq!(
            store
                .get(&entry.key())
                .map(|stored| stored.fairness_weight_overrides),
            Some(entry.fairness_weight_overrides)
        );
        assert_eq!(
            store.get(&TaskQueueConfigKey {
                namespace_id: namespace_b,
                task_queue: queue,
                kind: TaskQueueConfigKind::Activity,
            }),
            None
        );
    }

    #[test]
    fn duplicate_unsets_count_toward_the_request_cap() {
        let store = InMemoryTaskQueueConfigStore::new();
        let key = TaskQueueConfigKey {
            namespace_id: NamespaceId(uuid::Uuid::from_u128(3)),
            task_queue: TaskQueueName("duplicates".to_string()),
            kind: TaskQueueConfigKind::Activity,
        };

        let error = store
            .apply(
                key,
                TaskQueueConfigPatch {
                    unset_fairness_weight_overrides: vec![
                        "tenant".to_string(),
                        "tenant".to_string(),
                    ],
                    ..TaskQueueConfigPatch::default()
                },
                1,
            )
            .expect_err("v1.31.0 counts raw unset entries before deduplication");

        assert_eq!(
            error,
            TaskQueueConfigError::TooManyOverrides { got: 2, maximum: 1 }
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        // Feature: task-queue-priority-fairness, Property 13
        #[test]
        fn atomic_patch_state_machine_is_kind_isolated_and_rejection_preserves_state(
            actions in prop::collection::vec(
                (
                    0u8..3,
                    "[a-z]{1,8}",
                    -1.0f32..5.0,
                    any::<bool>(),
                    any::<bool>(),
                ),
                1..64,
            ),
        ) {
            let store = InMemoryTaskQueueConfigStore::new();
            let namespace_id = NamespaceId(uuid::Uuid::from_u128(7));
            let task_queue = TaskQueueName("shared-name".to_string());
            let mut reference = HashMap::<TaskQueueConfigKind, BTreeMap<String, f32>>::new();

            for (kind_index, fairness_key, weight, unset, conflict) in actions {
                let kind = match kind_index {
                    0 => TaskQueueConfigKind::Workflow,
                    1 => TaskQueueConfigKind::Activity,
                    _ => TaskQueueConfigKind::Nexus,
                };
                let key = TaskQueueConfigKey {
                    namespace_id,
                    task_queue: task_queue.clone(),
                    kind,
                };
                let before = store.get(&key);
                let mut patch = TaskQueueConfigPatch::default();
                if unset {
                    patch
                        .unset_fairness_weight_overrides
                        .push(fairness_key.clone());
                } else {
                    patch
                        .set_fairness_weight_overrides
                        .insert(fairness_key.clone(), weight);
                }
                if conflict {
                    patch
                        .set_fairness_weight_overrides
                        .insert(fairness_key.clone(), weight.max(0.1));
                    patch
                        .unset_fairness_weight_overrides
                        .push(fairness_key.clone());
                }

                let result = store.apply(key.clone(), patch, 1_000);
                let valid = !conflict && (unset || weight > 0.0);
                prop_assert_eq!(result.is_ok(), valid);
                if valid {
                    let expected = reference.entry(kind).or_default();
                    if unset {
                        expected.remove(&fairness_key);
                    } else {
                        expected.insert(fairness_key, weight);
                    }
                    prop_assert_eq!(
                        store
                            .get(&key)
                            .expect("successful patch creates the entry")
                            .fairness_weight_overrides,
                        expected.clone()
                    );
                } else {
                    prop_assert_eq!(store.get(&key), before);
                }
            }

            for kind in [
                TaskQueueConfigKind::Workflow,
                TaskQueueConfigKind::Activity,
                TaskQueueConfigKind::Nexus,
            ] {
                let key = TaskQueueConfigKey {
                    namespace_id,
                    task_queue: task_queue.clone(),
                    kind,
                };
                let actual = store
                    .get(&key)
                    .map(|entry| entry.fairness_weight_overrides)
                    .unwrap_or_default();
                prop_assert_eq!(
                    actual,
                    reference.get(&kind).cloned().unwrap_or_default()
                );
            }
        }
    }
}
