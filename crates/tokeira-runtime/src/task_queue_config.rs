//! Per-task-queue runtime configuration store.
//!
//! Owns the runtime's mutable, per-`(namespace, task queue, task kind)` tuning knobs:
//! queue-level rate limits and fairness weights. These are operator-set
//! overrides that shape dispatch *throughput and fairness* — they tune how fast
//! and in what proportion work is handed out, never *what* work is correct. This
//! is configuration state, not authoritative history. Production persists it
//! in a dedicated control-plane repository, while the runtime keeps a
//! disposable cache hydrated before traffic and refreshed after remote writes.
//!
//! Entries are task-kind isolated and namespace-scoped on listing, so an
//! activity policy cannot overwrite a same-named workflow or Nexus queue and
//! one namespace's overrides never leak into another's dispatch.

use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use dashmap::{DashMap, mapref::entry::Entry};
use thiserror::Error;
use time::OffsetDateTime;
use tokeira_storage::{
    StoredTaskQueueConfig, StoredTaskQueueConfigKey, StoredTaskQueueConfigKind,
    StoredTaskQueueConfigMetadata, TaskQueueConfigCasResult, TaskQueueConfigRepository,
};
use tokeira_types::{NamespaceId, TaskQueueName};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

const TASK_QUEUE_CONFIG_CAS_ATTEMPTS: usize = 8;
const TASK_QUEUE_CONFIG_REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

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

/// Failure from the live task-queue policy facade.
#[derive(Clone, Debug, Error, PartialEq)]
pub enum TaskQueueConfigStoreError {
    /// The public patch violates v1.31.0 validation.
    #[error(transparent)]
    Validation(#[from] TaskQueueConfigError),
    /// The durable repository could not complete an operation.
    #[error("task-queue configuration storage unavailable: {message}")]
    Storage {
        /// Repository diagnostic retained for operators.
        message: String,
    },
    /// Repeated concurrent writers exhausted the bounded CAS loop.
    #[error("task-queue configuration update conflicted repeatedly; retry the request")]
    ConflictExhausted,
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
    crate::conformance::reads()
        .get_i64("matching.maxFairnessKeyWeightOverrides")
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(MAX_FAIRNESS_WEIGHT_OVERRIDES)
}

/// Storage for per-task-queue configuration entries.
///
/// Reads happen on the dispatch path, so implementations must be cheap to query
/// and safe for concurrent access. The trait is namespace-scoped: [`list`](Self::list)
/// returns only the requested namespace's entries.
#[async_trait]
pub trait TaskQueueConfigStore: Send + Sync + 'static {
    /// Fetch the entry for a specific queue, or `None` if no override is set.
    async fn get(
        &self,
        key: &TaskQueueConfigKey,
    ) -> Result<Option<TaskQueueConfigEntry>, TaskQueueConfigStoreError>;

    /// Validate and apply one patch atomically.
    async fn apply(
        &self,
        key: TaskQueueConfigKey,
        patch: TaskQueueConfigPatch,
        max_overrides: usize,
    ) -> Result<TaskQueueConfigEntry, TaskQueueConfigStoreError>;

    /// List all entries for a namespace.
    async fn list(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<Vec<TaskQueueConfigEntry>, TaskQueueConfigStoreError>;

    /// Return the process-local wake used by blocked handout decisions.
    fn changed(&self, key: &TaskQueueConfigKey) -> Arc<Notify>;
}

/// Process-local [`TaskQueueConfigStore`] backed by a lock-free map.
///
/// This implementation remains a focused test double. Production construction
/// uses [`RepositoryBackedTaskQueueConfigStore`] even for in-memory storage so
/// restart and CAS behavior pass through one facade.
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

#[async_trait]
impl TaskQueueConfigStore for InMemoryTaskQueueConfigStore {
    async fn get(
        &self,
        key: &TaskQueueConfigKey,
    ) -> Result<Option<TaskQueueConfigEntry>, TaskQueueConfigStoreError> {
        Ok(self.entries.get(key).map(|entry| entry.clone()))
    }

    async fn apply(
        &self,
        key: TaskQueueConfigKey,
        patch: TaskQueueConfigPatch,
        max_overrides: usize,
    ) -> Result<TaskQueueConfigEntry, TaskQueueConfigStoreError> {
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

    async fn list(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<Vec<TaskQueueConfigEntry>, TaskQueueConfigStoreError> {
        let mut entries = self
            .entries
            .iter()
            .filter(|entry| entry.key().namespace_id == *namespace_id)
            .map(|entry| entry.value().clone())
            .collect::<Vec<_>>();
        entries.sort_by(task_queue_config_entry_order);
        Ok(entries)
    }

    fn changed(&self, key: &TaskQueueConfigKey) -> Arc<Notify> {
        self.changes
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone()
    }
}

#[derive(Clone, Debug, PartialEq)]
struct CachedTaskQueueConfig {
    revision: u64,
    entry: TaskQueueConfigEntry,
}

/// Repository-backed task-queue policy facade used by production wiring.
///
/// The repository is the committed source. Cache publication and waiter wakes
/// happen only after a successful CAS; startup hydration and bounded refresh
/// make the cache reconstructible across process replacement and remote writes.
pub struct RepositoryBackedTaskQueueConfigStore {
    repository: Arc<dyn TaskQueueConfigRepository>,
    entries: DashMap<TaskQueueConfigKey, CachedTaskQueueConfig>,
    changes: DashMap<TaskQueueConfigKey, Arc<Notify>>,
}

impl std::fmt::Debug for RepositoryBackedTaskQueueConfigStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RepositoryBackedTaskQueueConfigStore")
            .field("cached_entries", &self.entries.len())
            .finish_non_exhaustive()
    }
}

impl RepositoryBackedTaskQueueConfigStore {
    /// Construct an empty cache over a durable repository.
    #[must_use]
    pub fn new(repository: Arc<dyn TaskQueueConfigRepository>) -> Self {
        Self {
            repository,
            entries: DashMap::new(),
            changes: DashMap::new(),
        }
    }

    /// Hydrate the complete cache before task polls are admitted.
    pub async fn hydrate(&self) -> Result<(), TaskQueueConfigStoreError> {
        self.refresh_once().await
    }

    /// Refresh the cache once from the durable source.
    pub async fn refresh_once(&self) -> Result<(), TaskQueueConfigStoreError> {
        let records = self
            .repository
            .list_all_task_queue_configs()
            .await
            .map_err(storage_error)?;
        self.replace_cache(records);
        Ok(())
    }

    /// Start bounded remote-write refresh until `cancel` is triggered.
    ///
    /// Refresh failures retain the last successfully loaded revision and are
    /// retried on the next internal tick. That is delivery-policy staleness,
    /// never workflow-state loss.
    #[must_use]
    pub fn spawn_refresh(
        self: Arc<Self>,
        cancel: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(TASK_QUEUE_CONFIG_REFRESH_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            interval.tick().await;
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = interval.tick() => {
                        if let Err(error) = self.refresh_once().await {
                            tracing::warn!(%error, "task-queue policy refresh failed");
                        }
                    }
                }
            }
        })
    }

    fn replace_cache(&self, records: Vec<StoredTaskQueueConfig>) {
        let incoming = records
            .into_iter()
            .map(|record| {
                let key = from_stored_key(&record.key());
                (
                    key,
                    CachedTaskQueueConfig {
                        revision: record.revision,
                        entry: from_stored_entry(record),
                    },
                )
            })
            .collect::<std::collections::HashMap<_, _>>();
        let stale = self
            .entries
            .iter()
            .filter_map(|entry| (!incoming.contains_key(entry.key())).then(|| entry.key().clone()))
            .collect::<Vec<_>>();
        for key in stale {
            self.entries.remove(&key);
            self.changed(&key).notify_waiters();
        }
        for (key, cached) in incoming {
            let changed = self
                .entries
                .get(&key)
                .is_none_or(|current| *current != cached);
            if changed {
                self.entries.insert(key.clone(), cached);
                self.changed(&key).notify_waiters();
            }
        }
    }

    async fn apply_durable(
        &self,
        key: TaskQueueConfigKey,
        patch: TaskQueueConfigPatch,
        max_overrides: usize,
    ) -> Result<TaskQueueConfigEntry, TaskQueueConfigStoreError> {
        validate_patch(&key, &patch, max_overrides)?;
        let stored_key = to_stored_key(&key);
        for _ in 0..TASK_QUEUE_CONFIG_CAS_ATTEMPTS {
            let current = self
                .repository
                .load_task_queue_config(&stored_key)
                .await
                .map_err(storage_error)?;
            let expected_revision = current.as_ref().map(|record| record.revision);
            let mut candidate = current
                .map(from_stored_entry)
                .unwrap_or_else(|| empty_entry(&key));
            apply_patch(&mut candidate, patch.clone());
            validate_result(&candidate, max_overrides)?;
            let stored_candidate = to_stored_entry(&candidate, 0);
            match self
                .repository
                .compare_and_swap_task_queue_config(stored_candidate, expected_revision)
                .await
                .map_err(storage_error)?
            {
                TaskQueueConfigCasResult::Applied { revision } => {
                    self.entries.insert(
                        key.clone(),
                        CachedTaskQueueConfig {
                            revision,
                            entry: candidate.clone(),
                        },
                    );
                    self.changed(&key).notify_waiters();
                    return Ok(candidate);
                }
                TaskQueueConfigCasResult::Conflict => {}
            }
        }
        Err(TaskQueueConfigStoreError::ConflictExhausted)
    }
}

#[async_trait]
impl TaskQueueConfigStore for RepositoryBackedTaskQueueConfigStore {
    async fn get(
        &self,
        key: &TaskQueueConfigKey,
    ) -> Result<Option<TaskQueueConfigEntry>, TaskQueueConfigStoreError> {
        Ok(self.entries.get(key).map(|cached| cached.entry.clone()))
    }

    async fn apply(
        &self,
        key: TaskQueueConfigKey,
        patch: TaskQueueConfigPatch,
        max_overrides: usize,
    ) -> Result<TaskQueueConfigEntry, TaskQueueConfigStoreError> {
        self.apply_durable(key, patch, max_overrides).await
    }

    async fn list(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<Vec<TaskQueueConfigEntry>, TaskQueueConfigStoreError> {
        let mut entries = self
            .entries
            .iter()
            .filter(|entry| entry.key().namespace_id == *namespace_id)
            .map(|entry| entry.value().entry.clone())
            .collect::<Vec<_>>();
        entries.sort_by(task_queue_config_entry_order);
        Ok(entries)
    }

    fn changed(&self, key: &TaskQueueConfigKey) -> Arc<Notify> {
        self.changes
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone()
    }
}

fn storage_error(error: anyhow::Error) -> TaskQueueConfigStoreError {
    TaskQueueConfigStoreError::Storage {
        message: error.to_string(),
    }
}

fn empty_entry(key: &TaskQueueConfigKey) -> TaskQueueConfigEntry {
    TaskQueueConfigEntry {
        namespace_id: key.namespace_id,
        task_queue: key.task_queue.clone(),
        kind: key.kind,
        queue_rate_limit: None,
        queue_rate_limit_metadata: None,
        fairness_key_rate_limit_default: None,
        fairness_key_rate_limit_metadata: None,
        fairness_weight_overrides: BTreeMap::new(),
    }
}

fn task_queue_config_entry_order(
    left: &TaskQueueConfigEntry,
    right: &TaskQueueConfigEntry,
) -> std::cmp::Ordering {
    left.task_queue
        .0
        .cmp(&right.task_queue.0)
        .then_with(|| task_kind_code(left.kind).cmp(&task_kind_code(right.kind)))
}

fn task_kind_code(kind: TaskQueueConfigKind) -> u8 {
    match kind {
        TaskQueueConfigKind::Workflow => 1,
        TaskQueueConfigKind::Activity => 2,
        TaskQueueConfigKind::Nexus => 3,
    }
}

fn to_stored_kind(kind: TaskQueueConfigKind) -> StoredTaskQueueConfigKind {
    match kind {
        TaskQueueConfigKind::Workflow => StoredTaskQueueConfigKind::Workflow,
        TaskQueueConfigKind::Activity => StoredTaskQueueConfigKind::Activity,
        TaskQueueConfigKind::Nexus => StoredTaskQueueConfigKind::Nexus,
    }
}

fn from_stored_kind(kind: StoredTaskQueueConfigKind) -> TaskQueueConfigKind {
    match kind {
        StoredTaskQueueConfigKind::Workflow => TaskQueueConfigKind::Workflow,
        StoredTaskQueueConfigKind::Activity => TaskQueueConfigKind::Activity,
        StoredTaskQueueConfigKind::Nexus => TaskQueueConfigKind::Nexus,
    }
}

fn to_stored_key(key: &TaskQueueConfigKey) -> StoredTaskQueueConfigKey {
    StoredTaskQueueConfigKey {
        namespace_id: key.namespace_id,
        task_queue: key.task_queue.clone(),
        kind: to_stored_kind(key.kind),
    }
}

fn from_stored_key(key: &StoredTaskQueueConfigKey) -> TaskQueueConfigKey {
    TaskQueueConfigKey {
        namespace_id: key.namespace_id,
        task_queue: key.task_queue.clone(),
        kind: from_stored_kind(key.kind),
    }
}

fn to_stored_metadata(metadata: &TaskQueueConfigMetadata) -> StoredTaskQueueConfigMetadata {
    StoredTaskQueueConfigMetadata {
        reason: metadata.reason.clone(),
        update_identity: metadata.update_identity.clone(),
        update_time: metadata.update_time,
    }
}

fn from_stored_metadata(metadata: StoredTaskQueueConfigMetadata) -> TaskQueueConfigMetadata {
    TaskQueueConfigMetadata {
        reason: metadata.reason,
        update_identity: metadata.update_identity,
        update_time: metadata.update_time,
    }
}

fn to_stored_entry(entry: &TaskQueueConfigEntry, revision: u64) -> StoredTaskQueueConfig {
    StoredTaskQueueConfig {
        namespace_id: entry.namespace_id,
        task_queue: entry.task_queue.clone(),
        kind: to_stored_kind(entry.kind),
        revision,
        queue_rate_limit: entry.queue_rate_limit,
        queue_rate_limit_metadata: entry
            .queue_rate_limit_metadata
            .as_ref()
            .map(to_stored_metadata),
        fairness_key_rate_limit_default: entry.fairness_key_rate_limit_default,
        fairness_key_rate_limit_metadata: entry
            .fairness_key_rate_limit_metadata
            .as_ref()
            .map(to_stored_metadata),
        fairness_weight_overrides: entry.fairness_weight_overrides.clone(),
    }
}

fn from_stored_entry(entry: StoredTaskQueueConfig) -> TaskQueueConfigEntry {
    TaskQueueConfigEntry {
        namespace_id: entry.namespace_id,
        task_queue: entry.task_queue,
        kind: from_stored_kind(entry.kind),
        queue_rate_limit: entry.queue_rate_limit,
        queue_rate_limit_metadata: entry.queue_rate_limit_metadata.map(from_stored_metadata),
        fairness_key_rate_limit_default: entry.fairness_key_rate_limit_default,
        fairness_key_rate_limit_metadata: entry
            .fairness_key_rate_limit_metadata
            .map(from_stored_metadata),
        fairness_weight_overrides: entry.fairness_weight_overrides,
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

    #[tokio::test]
    async fn set_get_round_trip_is_key_isolated() {
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
            .await
            .expect("valid patch");

        assert_eq!(
            store
                .get(&entry.key())
                .await
                .expect("in-memory read")
                .map(|stored| stored.fairness_weight_overrides),
            Some(entry.fairness_weight_overrides)
        );
        assert_eq!(
            store
                .get(&TaskQueueConfigKey {
                    namespace_id: namespace_b,
                    task_queue: queue,
                    kind: TaskQueueConfigKind::Activity,
                })
                .await
                .expect("in-memory read"),
            None
        );
    }

    #[tokio::test]
    async fn duplicate_unsets_count_toward_the_request_cap() {
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
            .await
            .expect_err("v1.31.0 counts raw unset entries before deduplication");

        assert_eq!(
            error,
            TaskQueueConfigStoreError::Validation(TaskQueueConfigError::TooManyOverrides {
                got: 2,
                maximum: 1,
            })
        );
    }

    #[tokio::test]
    async fn refresh_publishes_remote_revisions_and_wakes_waiters() {
        let repository = Arc::new(tokeira_storage::InMemoryStore::default());
        let writer = RepositoryBackedTaskQueueConfigStore::new(repository.clone());
        let reader = RepositoryBackedTaskQueueConfigStore::new(repository);
        writer.hydrate().await.expect("writer hydration");
        reader.hydrate().await.expect("reader hydration");
        let key = TaskQueueConfigKey {
            namespace_id: NamespaceId(uuid::Uuid::from_u128(4)),
            task_queue: TaskQueueName("remote-refresh".to_string()),
            kind: TaskQueueConfigKind::Activity,
        };
        let notify = reader.changed(&key);
        let notified = notify.notified();
        tokio::pin!(notified);
        assert!(!notified.as_mut().enable());

        let expected = writer
            .apply(
                key.clone(),
                TaskQueueConfigPatch {
                    set_fairness_weight_overrides: BTreeMap::from([("tenant".to_string(), 2.0)]),
                    ..TaskQueueConfigPatch::default()
                },
                1_000,
            )
            .await
            .expect("remote update commits");
        assert_eq!(reader.get(&key).await.expect("cached read"), None);

        reader.refresh_once().await.expect("remote refresh");
        notified.await;
        assert_eq!(
            reader.get(&key).await.expect("refreshed read"),
            Some(expected)
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
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                let store = InMemoryTaskQueueConfigStore::new();
                let namespace_id = NamespaceId(uuid::Uuid::from_u128(7));
                let task_queue = TaskQueueName("shared-name".to_string());
                let mut reference =
                    HashMap::<TaskQueueConfigKind, BTreeMap<String, f32>>::new();

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
                    let before = store.get(&key).await.unwrap();
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

                    let result = store.apply(key.clone(), patch, 1_000).await;
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
                                .await
                                .unwrap()
                                .expect("successful patch creates the entry")
                                .fairness_weight_overrides,
                            expected.clone()
                        );
                    } else {
                        prop_assert_eq!(store.get(&key).await.unwrap(), before);
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
                        .await
                        .unwrap()
                        .map(|entry| entry.fairness_weight_overrides)
                        .unwrap_or_default();
                    prop_assert_eq!(
                        actual,
                        reference.get(&kind).cloned().unwrap_or_default()
                    );
                }
                Ok(())
            })?;
        }

        // Feature: configuration-policy, Property 8: task-queue patch state machine
        #[test]
        fn repository_patch_state_machine_matches_atomic_reference(
            actions in prop::collection::vec(
                (0u8..3, "[a-z]{1,8}", 0.001f32..1000.0, any::<bool>()),
                1..48,
            ),
        ) {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                let repository = Arc::new(tokeira_storage::InMemoryStore::default());
                let store = RepositoryBackedTaskQueueConfigStore::new(repository);
                store.hydrate().await.unwrap();
                let namespace_id = NamespaceId(uuid::Uuid::from_u128(44));
                let task_queue = TaskQueueName("repository-state-machine".to_owned());
                let mut reference =
                    HashMap::<TaskQueueConfigKind, BTreeMap<String, f32>>::new();

                for (kind_index, fairness_key, weight, unset) in actions {
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
                    let patch = if unset {
                        TaskQueueConfigPatch {
                            unset_fairness_weight_overrides: vec![fairness_key.clone()],
                            ..TaskQueueConfigPatch::default()
                        }
                    } else {
                        TaskQueueConfigPatch {
                            set_fairness_weight_overrides: BTreeMap::from([(
                                fairness_key.clone(),
                                weight,
                            )]),
                            ..TaskQueueConfigPatch::default()
                        }
                    };
                    let updated = store.apply(key.clone(), patch, 1_000).await.unwrap();
                    let expected = reference.entry(kind).or_default();
                    if unset {
                        expected.remove(&fairness_key);
                    } else {
                        expected.insert(fairness_key, weight);
                    }
                    prop_assert_eq!(updated.fairness_weight_overrides, expected.clone());
                    prop_assert_eq!(
                        store
                            .get(&key)
                            .await
                            .unwrap()
                            .expect("successful patch is cached")
                            .fairness_weight_overrides,
                        expected.clone()
                    );
                }
                Ok(())
            })?;
        }

        // Feature: configuration-policy, Property 11: restart recovery
        #[test]
        fn repository_backed_store_recovers_committed_policy_after_cache_loss(
            actions in prop::collection::vec(
                ("[a-z]{1,8}", 0.001f32..1000.0, any::<bool>()),
                1..48,
            ),
        ) {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                let repository = Arc::new(tokeira_storage::InMemoryStore::default());
                let first = RepositoryBackedTaskQueueConfigStore::new(repository.clone());
                first.hydrate().await.unwrap();
                let key = TaskQueueConfigKey {
                    namespace_id: NamespaceId(uuid::Uuid::from_u128(45)),
                    task_queue: TaskQueueName("restart-recovery".to_owned()),
                    kind: TaskQueueConfigKind::Activity,
                };

                for (fairness_key, weight, unset) in actions {
                    let patch = if unset {
                        TaskQueueConfigPatch {
                            unset_fairness_weight_overrides: vec![fairness_key],
                            ..TaskQueueConfigPatch::default()
                        }
                    } else {
                        TaskQueueConfigPatch {
                            set_fairness_weight_overrides: BTreeMap::from([(
                                fairness_key,
                                weight,
                            )]),
                            ..TaskQueueConfigPatch::default()
                        }
                    };
                    first.apply(key.clone(), patch, 1_000).await.unwrap();
                }
                let before_restart = first.get(&key).await.unwrap();

                let restarted = RepositoryBackedTaskQueueConfigStore::new(repository);
                restarted.hydrate().await.unwrap();
                prop_assert_eq!(restarted.get(&key).await.unwrap(), before_restart.clone());
                prop_assert_eq!(
                    restarted.list(&key.namespace_id).await.unwrap(),
                    before_restart.into_iter().collect::<Vec<_>>()
                );
                Ok(())
            })?;
        }
    }
}
