//! Runtime-owned batch operation state.
//!
//! The edge crate owns batch execution because it needs visibility and
//! transport-facing workflow service operations. The store lives here so batch
//! state stays independent from generated proto and edge authentication types.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use dashmap::DashMap;
use thiserror::Error;
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    ActivityControlTarget, ActivityRetryPolicyPatch, FieldChange, VersioningOverrideChange,
};
use tokeira_types::{NamespaceId, Payloads, TaskQueueName};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct JobId(pub String);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BatchOperationType {
    Terminate,
    Cancel,
    Signal,
    Delete,
    Reset,
    /// Mutate workflow-execution options in every selected workflow.
    UpdateWorkflowExecutionOptions,
    /// Unpause matching pending activities in every selected workflow.
    UnpauseActivity,
    /// Patch matching pending activity options in every selected workflow.
    UpdateActivityOptions,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BatchOperationState {
    Running,
    Completed,
    Failed,
}

#[derive(Clone, Debug, PartialEq)]
pub enum BatchOperationParams {
    Terminate {
        details: Option<Payloads>,
        identity: String,
    },
    Cancel {
        identity: String,
    },
    Signal {
        signal_name: String,
        input: Option<Payloads>,
        identity: String,
    },
    Delete {
        identity: String,
    },
    Reset {
        identity: String,
        target: BatchResetTarget,
        reason: String,
    },
    /// Workflow-execution options mutation carried by a batch.
    UpdateWorkflowExecutionOptions {
        /// Client identity recorded on each workflow mutation.
        identity: String,
        /// Validated versioning-override change selected by the update mask.
        versioning_override: VersioningOverrideChange,
    },
    /// Workflow-scoped activity-unpause parameters carried by a batch.
    UnpauseActivity {
        /// Client identity recorded on each workflow mutation.
        identity: String,
        /// Activity type/all selector; batch activity operations do not admit ids.
        target: ActivityControlTarget,
        /// Whether unpause resets the attempt counter.
        reset_attempts: bool,
        /// Whether unpause clears heartbeat details.
        reset_heartbeat: bool,
        /// Maximum randomized redispatch delay.
        jitter: Option<Duration>,
    },
    /// Workflow-scoped activity-options patch carried by a batch.
    UpdateActivityOptions {
        /// Client identity recorded on each workflow mutation.
        identity: String,
        /// Fully validated patch, without per-run request context.
        patch: BatchActivityOptionsPatch,
    },
}

impl BatchOperationParams {
    pub fn identity(&self) -> &str {
        match self {
            BatchOperationParams::Terminate { identity, .. }
            | BatchOperationParams::Cancel { identity }
            | BatchOperationParams::Signal { identity, .. }
            | BatchOperationParams::Delete { identity }
            | BatchOperationParams::Reset { identity, .. }
            | BatchOperationParams::UpdateWorkflowExecutionOptions { identity, .. }
            | BatchOperationParams::UnpauseActivity { identity, .. }
            | BatchOperationParams::UpdateActivityOptions { identity, .. } => identity,
        }
    }
}

/// Validated activity-options mutation shared by every workflow in a batch.
///
/// Request identity and admission time are deliberately absent: the edge adds
/// fresh per-run request context when it dispatches each batch item, preventing
/// two batches from colliding in a workflow's request-deduplication window.
#[derive(Clone, Debug, PartialEq)]
pub struct BatchActivityOptionsPatch {
    /// Pending activities selected by type or all.
    pub target: ActivityControlTarget,
    /// New task queue, if selected by the field mask.
    pub task_queue: FieldChange<TaskQueueName>,
    /// New schedule-to-close timeout, if selected.
    pub schedule_to_close_timeout: FieldChange<Option<Duration>>,
    /// New schedule-to-start timeout, if selected.
    pub schedule_to_start_timeout: FieldChange<Option<Duration>>,
    /// New start-to-close timeout, if selected.
    pub start_to_close_timeout: FieldChange<Option<Duration>>,
    /// New heartbeat timeout, if selected.
    pub heartbeat_timeout: FieldChange<Option<Duration>>,
    /// Retry-policy field-mask patch.
    pub retry_policy: ActivityRetryPolicyPatch,
    /// Whether every selected activity restores its first-schedule options.
    pub restore_original_options: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BatchResetTarget {
    WorkflowTaskId(i64),
    FirstWorkflowTask,
    LastWorkflowTask,
    BuildId(String),
}

#[derive(Debug, Default)]
pub struct BatchProgressCounters {
    pub total: AtomicU64,
    pub complete: AtomicU64,
    pub failure: AtomicU64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowExecutionRef {
    pub workflow_id: String,
    pub run_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct BatchOperationEntry {
    pub job_id: JobId,
    pub namespace_id: NamespaceId,
    pub operation_type: BatchOperationType,
    pub operation_params: BatchOperationParams,
    pub state: BatchOperationState,
    pub start_time: OffsetDateTime,
    pub close_time: Option<OffsetDateTime>,
    pub counters: Arc<BatchProgressCounters>,
    pub visibility_query: Option<String>,
    pub executions: Option<Vec<WorkflowExecutionRef>>,
    pub reason: String,
    pub identity: String,
    pub max_operations_per_second: f32,
    pub cancellation_token: CancellationToken,
    pub stop_reason: Option<String>,
    pub stop_identity: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BatchOperationSnapshot {
    pub job_id: JobId,
    pub namespace_id: NamespaceId,
    pub operation_type: BatchOperationType,
    pub state: BatchOperationState,
    pub start_time: OffsetDateTime,
    pub close_time: Option<OffsetDateTime>,
    pub total_operation_count: u64,
    pub complete_operation_count: u64,
    pub failure_operation_count: u64,
    pub identity: String,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BatchOperationInfo {
    pub job_id: JobId,
    pub state: BatchOperationState,
    pub start_time: OffsetDateTime,
    pub close_time: Option<OffsetDateTime>,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum BatchError {
    #[error("batch operation already exists")]
    AlreadyExists,
    #[error("batch operation not found")]
    NotFound,
    #[error("invalid batch operation argument: {0}")]
    InvalidArgument(String),
}

#[derive(Default, Debug)]
pub struct BatchOperationStore {
    entries: DashMap<(NamespaceId, JobId), BatchOperationEntry>,
}

impl BatchOperationStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create(&self, entry: BatchOperationEntry) -> Result<(), BatchError> {
        let key = (entry.namespace_id, entry.job_id.clone());
        match self.entries.entry(key) {
            dashmap::mapref::entry::Entry::Occupied(_) => Err(BatchError::AlreadyExists),
            dashmap::mapref::entry::Entry::Vacant(slot) => {
                slot.insert(entry);
                Ok(())
            }
        }
    }

    pub fn describe(
        &self,
        namespace_id: NamespaceId,
        job_id: &JobId,
    ) -> Result<BatchOperationSnapshot, BatchError> {
        self.entries
            .get(&(namespace_id, job_id.clone()))
            .map(|entry| snapshot_from_entry(&entry))
            .ok_or(BatchError::NotFound)
    }

    pub fn entry(
        &self,
        namespace_id: NamespaceId,
        job_id: &JobId,
    ) -> Result<BatchOperationEntry, BatchError> {
        self.entries
            .get(&(namespace_id, job_id.clone()))
            .map(|entry| entry.clone())
            .ok_or(BatchError::NotFound)
    }

    pub fn stop(
        &self,
        namespace_id: NamespaceId,
        job_id: &JobId,
        reason: String,
        identity: String,
    ) -> Result<(), BatchError> {
        let mut entry = self
            .entries
            .get_mut(&(namespace_id, job_id.clone()))
            .ok_or(BatchError::NotFound)?;
        entry.stop_reason = Some(reason);
        entry.stop_identity = Some(identity);
        entry.cancellation_token.cancel();
        Ok(())
    }

    pub fn set_state(
        &self,
        namespace_id: NamespaceId,
        job_id: &JobId,
        state: BatchOperationState,
        close_time: Option<OffsetDateTime>,
    ) -> Result<(), BatchError> {
        let mut entry = self
            .entries
            .get_mut(&(namespace_id, job_id.clone()))
            .ok_or(BatchError::NotFound)?;
        entry.state = state;
        entry.close_time = close_time;
        Ok(())
    }

    pub fn list(
        &self,
        namespace_id: NamespaceId,
        page_size: usize,
        page_token: &[u8],
    ) -> (Vec<BatchOperationInfo>, Option<Vec<u8>>) {
        let mut entries: Vec<_> = self
            .entries
            .iter()
            .filter(|entry| entry.key().0 == namespace_id)
            .map(|entry| info_from_entry(entry.value()))
            .collect();
        // ListBatchOperations delegates to visibility in v1.31.0, whose SQL
        // listing puts the newest running execution first. Job ID is only a
        // deterministic tie-break for equal process-clock timestamps.
        // (`service/frontend/workflow_handler.go` and
        // `common/persistence/visibility/store/sql/query_converter_legacy_postgresql.go`
        // @ v1.31.0.)
        entries.sort_by(|a, b| {
            b.start_time
                .cmp(&a.start_time)
                .then_with(|| a.job_id.cmp(&b.job_id))
        });
        let start = (decode_page_token(page_token).unwrap_or(0) as usize).min(entries.len());
        let limit = page_size.max(1);
        let end = (start + limit).min(entries.len());
        let next = (end < entries.len()).then(|| encode_page_token(end as u64));
        (entries[start..end].to_vec(), next)
    }
}

fn snapshot_from_entry(entry: &BatchOperationEntry) -> BatchOperationSnapshot {
    BatchOperationSnapshot {
        job_id: entry.job_id.clone(),
        namespace_id: entry.namespace_id,
        operation_type: entry.operation_type,
        state: entry.state,
        start_time: entry.start_time,
        close_time: entry.close_time,
        total_operation_count: entry.counters.total.load(Ordering::Relaxed),
        complete_operation_count: entry.counters.complete.load(Ordering::Relaxed),
        failure_operation_count: entry.counters.failure.load(Ordering::Relaxed),
        identity: entry.identity.clone(),
        reason: entry
            .stop_reason
            .clone()
            .unwrap_or_else(|| entry.reason.clone()),
    }
}

fn info_from_entry(entry: &BatchOperationEntry) -> BatchOperationInfo {
    BatchOperationInfo {
        job_id: entry.job_id.clone(),
        state: entry.state,
        start_time: entry.start_time,
        close_time: entry.close_time,
    }
}

fn encode_page_token(value: u64) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}

fn decode_page_token(value: &[u8]) -> Option<u64> {
    if value.is_empty() {
        return Some(0);
    }
    let bytes: [u8; 8] = value.try_into().ok()?;
    Some(u64::from_be_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use proptest::prelude::*;
    use time::OffsetDateTime;
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    use super::*;

    fn sample_entry(namespace_id: NamespaceId, job_id: &str) -> BatchOperationEntry {
        BatchOperationEntry {
            job_id: JobId(job_id.to_string()),
            namespace_id,
            operation_type: BatchOperationType::Cancel,
            operation_params: BatchOperationParams::Cancel {
                identity: "worker".to_string(),
            },
            state: BatchOperationState::Running,
            start_time: OffsetDateTime::UNIX_EPOCH,
            close_time: None,
            counters: Arc::new(BatchProgressCounters::default()),
            visibility_query: Some("WorkflowType = 'demo'".to_string()),
            executions: None,
            reason: "reason".to_string(),
            identity: "starter".to_string(),
            max_operations_per_second: 25.0,
            cancellation_token: CancellationToken::new(),
            stop_reason: None,
            stop_identity: None,
        }
    }

    #[test]
    fn list_orders_newest_batch_first() {
        let store = BatchOperationStore::default();
        let namespace_id = NamespaceId(Uuid::nil());
        let older = sample_entry(namespace_id, "z-older");
        let mut newer = sample_entry(namespace_id, "a-newer");
        newer.start_time += time::Duration::seconds(1);
        store.create(older).expect("older batch must be created");
        store.create(newer).expect("newer batch must be created");

        let (listed, next) = store.list(namespace_id, 100, &[]);

        assert_eq!(next, None);
        assert_eq!(
            listed
                .iter()
                .map(|entry| entry.job_id.0.as_str())
                .collect::<Vec<_>>(),
            vec!["a-newer", "z-older"]
        );
    }

    // Feature: edge-batch-operations-transport, Property 1: Batch store CRUD correctness
    proptest! {
        #[test]
        fn property_batch_store_crud_correctness(job_suffix in "[a-z0-9]{1,12}") {
            let store = BatchOperationStore::default();
            let namespace_id = NamespaceId(Uuid::nil());
            let entry = sample_entry(namespace_id, &format!("job-{job_suffix}"));

            prop_assert_eq!(
                store.describe(namespace_id, &entry.job_id),
                Err(BatchError::NotFound)
            );

            store.create(entry.clone()).expect("initial create must succeed");

            let snapshot = store
                .describe(namespace_id, &entry.job_id)
                .expect("created entry must describe");
            prop_assert_eq!(snapshot.job_id, entry.job_id.clone());
            prop_assert_eq!(snapshot.namespace_id, entry.namespace_id);
            prop_assert_eq!(snapshot.operation_type, entry.operation_type);
            prop_assert_eq!(snapshot.state, entry.state);
            prop_assert_eq!(snapshot.identity, entry.identity.clone());
            prop_assert_eq!(snapshot.reason, entry.reason.clone());
            prop_assert_eq!(snapshot.total_operation_count, 0);
            prop_assert_eq!(snapshot.complete_operation_count, 0);
            prop_assert_eq!(snapshot.failure_operation_count, 0);

            prop_assert_eq!(store.create(entry), Err(BatchError::AlreadyExists));
        }

        #[test]
        fn property_pagination_completeness(job_ids in proptest::collection::btree_set("[a-z0-9]{1,10}", 1..30), page_size in 1usize..8usize) {
            let store = BatchOperationStore::default();
            let namespace_id = NamespaceId(Uuid::nil());
            let expected: Vec<_> = job_ids
                .iter()
                .map(|job_id| {
                    let entry = sample_entry(namespace_id, job_id);
                    store.create(entry).expect("create must succeed");
                    JobId(job_id.clone())
                })
                .collect();

            let mut token = Vec::new();
            let mut seen = Vec::new();

            loop {
                let (page, next) = store.list(namespace_id, page_size, &token);
                seen.extend(page.into_iter().map(|info| info.job_id));
                match next {
                    Some(next_token) => token = next_token,
                    None => break,
                }
            }

            prop_assert_eq!(seen, expected);
        }

        #[test]
        fn property_idempotent_stop_on_terminal_state(job_suffix in "[a-z0-9]{1,12}", terminal_state in prop_oneof![Just(BatchOperationState::Completed), Just(BatchOperationState::Failed)]) {
            let store = BatchOperationStore::default();
            let namespace_id = NamespaceId(Uuid::nil());
            let mut entry = sample_entry(namespace_id, &format!("job-{job_suffix}"));
            entry.state = terminal_state;
            let token = entry.cancellation_token.clone();
            store.create(entry.clone()).expect("create must succeed");

            let stop_result = store.stop(
                namespace_id,
                &entry.job_id,
                "stop-reason".to_string(),
                "stopper".to_string(),
            );

            prop_assert_eq!(stop_result, Ok(()));
            prop_assert!(token.is_cancelled());

            let snapshot = store
                .describe(namespace_id, &entry.job_id)
                .expect("entry must remain present");
            prop_assert_eq!(snapshot.state, terminal_state);
            prop_assert_eq!(snapshot.reason, "stop-reason");
        }
    }
}
