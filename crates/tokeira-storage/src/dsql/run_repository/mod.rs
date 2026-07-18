//! DSQL-backed implementation of the semantic `RunRepository` contract.
//!
//! The physical schema is spread-key-first: hot write tables use UUID keys
//! derived from logical identifiers, while secondary indexes serve targeted
//! read paths. The repository is the only module that should know how those
//! tables combine into workflow semantics; callers continue to use the storage
//! trait in terms of runs, history, leases, dispatch, and sweep entries.

use std::{sync::Arc, time::Instant};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use sqlx::Connection;
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    ActivityOp, BasicKernel, CallbackState, DispatchOp, HistoryEvent, LoadedRun, ReplayContext,
    TimerOp, Transition, WorkflowState,
};
use tokeira_types::{
    BuildId, DeploymentId, ExecutionRef, ExecutionStatus, NamespaceId, Payloads, QueueKey,
    RequestId, RunId, RunKey, ShardEpoch, ShardId, TaskKind, TaskQueueName, TransitionSeq,
    WorkerIdentity, WorkflowId, WorkflowRuleRecord, dsql_spread_uuid,
};
use tracing::{Instrument, instrument};
use uuid::Uuid;

use crate::{
    ActivitySweepEntry, AttributedHistoryEvent, BacklogEntry, CommitResult,
    CompletionCallbackSweepEntry, CurrentExecutionConflictPolicy, DbClass, DeleteRunRequest,
    DeleteRunResult, DispatchableActivityTask, DispatchableWorkflowTask, DueTimer, NexusSweepEntry,
    ProjectionRecord, RequestRecord, RunRepository, TransitionAuditRecord, WftTimeoutSweepEntry,
    WorkerDeploymentVersionKey, WorkflowRuleCreateResult, WorkflowRuleDeleteResult,
    WorkflowTimeoutSweepEntry, deleted_workflow_projection_context, metrics,
    workflow_is_open_and_pinned_to_version, workflow_projection_context,
};

use super::{DsqlConnectionAcquirer, DsqlConnectionDirector, codec, convert};

/// Current projection fanout for records written by this repository.
///
/// The schema supports partitioned projection scans; the MVP uses one logical
/// fanout value while still assigning deterministic partitions.
const PROJECTION_FANOUT: i16 = 1;
const DEFAULT_HISTORY_PAGE_SIZE: usize = 1000;

fn effective_history_limit(limit: usize) -> usize {
    if limit == usize::MAX {
        DEFAULT_HISTORY_PAGE_SIZE
    } else {
        limit
    }
}

fn dsql_retry_operation_label(
    operation: &str,
) -> Option<tokeira_observability::StorageOperationLabel> {
    match operation {
        "commit_transition" => Some(tokeira_observability::StorageOperationLabel::CommitTransition),
        "commit_transition_for_bundle" => {
            Some(tokeira_observability::StorageOperationLabel::CommitTransitionForBundle)
        }
        _ => None,
    }
}

macro_rules! record_dsql_operation {
    ($repo:expr, $operation:expr, $shard_id:expr, $body:block) => {{
        let started = std::time::Instant::now();
        let result = (async $body).await;
        // Instrument at the repository boundary so helper functions can stay
        // focused on persistence semantics and every public operation gets the
        // same outcome/error classification.
        $repo.record_operation_result($operation, $shard_id, started.elapsed(), &result);
        result
    }};
}

macro_rules! record_dsql_commit_operation {
    ($repo:expr, $operation:expr, $shard_id:expr, $body:block) => {{
        let started = std::time::Instant::now();
        let result = (async $body).await;
        $repo.record_commit_operation_result($operation, $shard_id, started.elapsed(), &result);
        result
    }};
}

mod activity;
mod commit;
mod delete;
mod dispatch;
mod leases;
mod load;
mod timers;
mod visibility;
mod workflow_rules;

#[cfg(test)]
use activity::{ActivityDispatchRow, activity_dispatch_from_row, collect_activity_sweep_entries};
#[cfg(test)]
use dispatch::{collect_dispatchable_workflow_tasks, sticky_fields};
#[cfg(test)]
use leases::{RenewDecision, decide_renew, interpret_acquire};
#[cfg(test)]
use visibility::{
    collect_nexus_sweep_entries, collect_started_workflow_task_entries,
    collect_workflow_timeout_entries,
};

/// Production `RunRepository` backed by Aurora DSQL.
#[derive(Debug)]
pub struct DsqlRunRepository {
    /// Abstracted acquisition boundary.
    ///
    /// Production uses `DsqlConnectionDirector`; tests use a fake acquirer to
    /// prove routing and zero-limit behavior without opening SQL connections.
    director: Arc<dyn DsqlConnectionAcquirer>,
    /// Non-zero runtime shard count used to map run keys to shard ownership.
    shard_count: u32,
    /// Non-zero projection partition count used when writing projection logs.
    projection_partition_count: u32,
    /// Workflow-id conflict policy applied during start commits.
    conflict_policy: CurrentExecutionConflictPolicy,
    /// Duration added to one captured application timestamp for lease expiry.
    lease_duration: Duration,
}

impl DsqlRunRepository {
    /// Build a repository using the production DSQL connection director.
    pub fn new(
        director: Arc<DsqlConnectionDirector>,
        shard_count: u32,
        projection_partition_count: u32,
        conflict_policy: CurrentExecutionConflictPolicy,
        lease_duration: Duration,
    ) -> Result<Self> {
        if shard_count == 0 {
            bail!("shard_count must be greater than zero");
        }
        if projection_partition_count == 0 {
            bail!("projection_partition_count must be greater than zero");
        }
        if lease_duration <= Duration::ZERO {
            bail!("lease_duration must be positive");
        }
        Ok(Self {
            director: director as Arc<dyn DsqlConnectionAcquirer>,
            shard_count,
            projection_partition_count,
            conflict_policy,
            lease_duration,
        })
    }

    #[cfg(test)]
    fn new_with_acquirer(
        director: Arc<dyn DsqlConnectionAcquirer>,
        shard_count: u32,
        projection_partition_count: u32,
        conflict_policy: CurrentExecutionConflictPolicy,
        lease_duration: Duration,
    ) -> Result<Self> {
        if shard_count == 0 {
            bail!("shard_count must be greater than zero");
        }
        if projection_partition_count == 0 {
            bail!("projection_partition_count must be greater than zero");
        }
        if lease_duration <= Duration::ZERO {
            bail!("lease_duration must be positive");
        }
        Ok(Self {
            director,
            shard_count,
            projection_partition_count,
            conflict_policy,
            lease_duration,
        })
    }

    #[cfg(test)]
    pub(crate) fn shard_for_run_key_with_count(
        run_key: RunKey,
        shard_count: u32,
    ) -> Result<ShardId> {
        if shard_count == 0 {
            bail!("shard_count must be greater than zero");
        }
        Ok(ShardId((run_key.0.as_u128() as u32) % shard_count))
    }

    pub(crate) fn shard_for_run_key(&self, run_key: RunKey) -> ShardId {
        debug_assert!(self.shard_count > 0);
        // The run key is already a spread UUID. Taking its low bits is stable
        // and cheap, and keeps routing aligned with rows written by this
        // repository.
        ShardId((run_key.0.as_u128() as u32) % self.shard_count)
    }

    /// Stable encoding of `ShardId(u32)` to UUID for SQL binding.
    ///
    /// Feature specs originally modeled shard IDs as UUID columns. This helper
    /// preserves that schema without changing the public `ShardId(pub u32)`
    /// type used throughout runtime ownership code.
    pub(crate) fn shard_id_to_uuid(shard_id: ShardId) -> Uuid {
        let mut bytes = *b"tokeira-shard-id";
        bytes[12..16].copy_from_slice(&shard_id.0.to_be_bytes());
        Uuid::from_bytes(bytes)
    }

    pub(crate) fn shard_id_from_uuid(value: Uuid) -> Result<ShardId> {
        let bytes = value.into_bytes();
        if &bytes[0..12] != b"tokeira-shar" {
            bail!("shard UUID does not use reversible shard-id encoding");
        }
        Ok(ShardId(u32::from_be_bytes(
            bytes[12..16]
                .try_into()
                .context("invalid shard UUID length")?,
        )))
    }

    pub(crate) fn current_execution_key(
        namespace_id: NamespaceId,
        workflow_id: &WorkflowId,
    ) -> Uuid {
        // Workflow-id uniqueness is a workflow-level invariant, so this key
        // deliberately excludes run_id.
        dsql_spread_uuid(&[
            b"current-execution",
            namespace_id.0.as_bytes(),
            workflow_id.0.as_bytes(),
        ])
    }

    pub(crate) fn request_dedupe_key(
        namespace_id: NamespaceId,
        workflow_id: &WorkflowId,
        request_id: &RequestId,
    ) -> Uuid {
        // Request dedupe is workflow-scoped for start/signal/update style
        // operations. Explicit run filtering is applied when the row is read.
        dsql_spread_uuid(&[
            b"request-dedupe",
            namespace_id.0.as_bytes(),
            workflow_id.0.as_bytes(),
            request_id.0.as_bytes(),
        ])
    }

    pub(crate) fn dispatch_backlog_key(
        partition_id: u32,
        queue_namespace: NamespaceId,
        queue_name: &str,
        task_kind: TaskKind,
        deployment: Option<&str>,
        build_id: Option<&str>,
        insertion_seq: u64,
    ) -> Uuid {
        // Include optional deployment/build identity in the key input so
        // versioned and unversioned queue entries cannot collide.
        let deployment = option_key_part(deployment);
        let build_id = option_key_part(build_id);
        let task_kind = (task_kind.to_db_smallint() as u16).to_le_bytes();
        dsql_spread_uuid(&[
            b"dispatch-backlog",
            &partition_id.to_le_bytes(),
            queue_namespace.0.as_bytes(),
            queue_name.as_bytes(),
            &task_kind,
            &deployment,
            &build_id,
            &insertion_seq.to_be_bytes(),
        ])
    }

    pub(crate) fn activity_dispatch_key(run_key: RunKey, activity_id: &str) -> Uuid {
        dsql_spread_uuid(&[
            b"activity-dispatch",
            run_key.0.as_bytes(),
            activity_id.as_bytes(),
        ])
    }

    pub(crate) fn is_serialization_failure(err: &sqlx::Error) -> bool {
        matches!(err, sqlx::Error::Database(db_err) if db_err.code().as_deref() == Some("40001"))
    }

    fn record_operation_result<T>(
        &self,
        operation: &'static str,
        shard_id: Option<ShardId>,
        duration: std::time::Duration,
        result: &Result<T>,
    ) {
        let outcome = classify_outcome(result);
        metrics::record_dsql_operation_duration(operation, outcome, duration);
        metrics::record_dsql_query_duration(operation, outcome, duration);
        metrics::record_dsql_operation_total(operation, outcome);
        metrics::record_storage_operation(operation, outcome);
        match operation {
            "load_run" => metrics::record_load_run_duration(duration),
            "read_history" => metrics::record_read_history_duration(duration),
            _ => {}
        }
        if let Some(shard_id) = shard_id {
            metrics::record_dsql_shard_operation(shard_id.0, operation);
            metrics::record_dsql_shard_duration(shard_id.0, duration);
            if outcome == "conflict" {
                metrics::record_dsql_shard_conflict(shard_id.0);
            }
        }
        if let Err(error) = result {
            if outcome == "conflict"
                && let Some(operation) = dsql_retry_operation_label(operation)
            {
                metrics::record_dsql_occ_conflict(operation);
            }
            if let Some(sqlstate) = extract_sqlstate(error) {
                metrics::record_dsql_error_code(&sqlstate);
            }
            if let Some(kind) = classify_connection_error(error) {
                metrics::record_dsql_connection_error(kind);
            }
        }
    }

    fn record_commit_operation_result(
        &self,
        operation: &'static str,
        shard_id: Option<ShardId>,
        duration: std::time::Duration,
        result: &Result<CommitResult>,
    ) {
        let outcome = match result {
            Ok(CommitResult::Conflict { .. }) => "conflict",
            Ok(_) => "success",
            Err(error) if is_serialization_failure_error(error) => "conflict",
            Err(_) => "error",
        };
        metrics::record_dsql_operation_duration(operation, outcome, duration);
        metrics::record_dsql_query_duration(operation, outcome, duration);
        metrics::record_dsql_operation_total(operation, outcome);
        metrics::record_storage_operation(operation, outcome);
        if matches!(
            operation,
            "commit_transition" | "commit_transition_for_bundle"
        ) {
            metrics::record_commit_transition_duration(None, outcome, duration);
        }
        if let Some(shard_id) = shard_id {
            metrics::record_dsql_shard_operation(shard_id.0, operation);
            metrics::record_dsql_shard_duration(shard_id.0, duration);
            if outcome == "conflict" {
                metrics::record_dsql_shard_conflict(shard_id.0);
            }
        }
        if outcome == "conflict"
            && let Some(operation) = dsql_retry_operation_label(operation)
        {
            metrics::record_dsql_occ_conflict(operation);
        }
        if let Err(error) = result {
            if let Some(sqlstate) = extract_sqlstate(error) {
                metrics::record_dsql_error_code(&sqlstate);
            }
            if let Some(kind) = classify_connection_error(error) {
                metrics::record_dsql_connection_error(kind);
            }
        }
    }
}

fn classify_outcome<T>(result: &Result<T>) -> &'static str {
    match result {
        Ok(_) => "success",
        Err(error) if is_serialization_failure_error(error) => "conflict",
        Err(_) => "error",
    }
}

fn is_serialization_failure_error(error: &anyhow::Error) -> bool {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<sqlx::Error>())
        .is_some_and(DsqlRunRepository::is_serialization_failure)
}

fn extract_sqlstate(error: &anyhow::Error) -> Option<String> {
    // SQLSTATE values are the most stable way to alert on DSQL/PostgreSQL
    // classes across connector versions.
    error
        .chain()
        .find_map(|cause| match cause.downcast_ref::<sqlx::Error>() {
            Some(sqlx::Error::Database(database_error)) => {
                database_error.code().map(|code| code.into_owned())
            }
            _ => None,
        })
}

fn classify_connection_error(error: &anyhow::Error) -> Option<&'static str> {
    // Keep this intentionally coarse. Operators need to distinguish transport,
    // timeout, refusal, and TLS classes; SQL semantics are reported separately
    // through SQLSTATE.
    error
        .chain()
        .find_map(|cause| match cause.downcast_ref::<sqlx::Error>() {
            Some(sqlx::Error::Io(io_error)) => Some(match io_error.kind() {
                std::io::ErrorKind::ConnectionReset => "reset",
                std::io::ErrorKind::TimedOut => "timeout",
                std::io::ErrorKind::ConnectionRefused => "refused",
                _ => "reset",
            }),
            Some(sqlx::Error::Tls(_)) => Some("tls"),
            _ => None,
        })
}

#[async_trait]
impl RunRepository for DsqlRunRepository {
    async fn resolve_execution(&self, execution: &ExecutionRef) -> Result<Option<RunKey>> {
        self.do_resolve_execution(execution).await
    }

    async fn find_latest_run(
        &self,
        namespace_id: NamespaceId,
        workflow_id: &WorkflowId,
    ) -> Result<Option<RunKey>> {
        self.do_find_latest_run(namespace_id, workflow_id).await
    }

    async fn list_runs_for_namespace(&self, namespace_id: NamespaceId) -> Result<Vec<RunKey>> {
        self.do_list_runs_for_namespace(namespace_id).await
    }

    async fn load_run(&self, run_key: RunKey) -> Result<LoadedRun> {
        self.do_load_run(run_key).await
    }

    async fn read_history(
        &self,
        run_key: RunKey,
        after_event_id: i64,
        limit: usize,
    ) -> Result<Vec<HistoryEvent>> {
        self.do_read_history(run_key, after_event_id, limit).await
    }

    async fn read_attributed_history(
        &self,
        run_key: RunKey,
        after_event_id: i64,
        limit: usize,
    ) -> Result<Vec<AttributedHistoryEvent>> {
        self.do_read_attributed_history(run_key, after_event_id, limit)
            .await
    }

    async fn lookup_request_dedupe(
        &self,
        execution: &ExecutionRef,
        request_id: &RequestId,
    ) -> Result<Option<RequestRecord>> {
        self.do_lookup_request_dedupe(execution, request_id).await
    }

    async fn read_transition_audit(&self, run_key: RunKey) -> Result<Vec<TransitionAuditRecord>> {
        self.do_read_transition_audit(run_key).await
    }

    async fn has_open_pinned_workflows(
        &self,
        namespace_id: NamespaceId,
        version: &WorkerDeploymentVersionKey,
    ) -> Result<bool> {
        self.do_has_open_pinned_workflows(namespace_id, version)
            .await
    }

    async fn create_workflow_rule(
        &self,
        namespace_id: NamespaceId,
        rule: WorkflowRuleRecord,
        max_rules: usize,
    ) -> Result<WorkflowRuleCreateResult> {
        self.do_create_workflow_rule(namespace_id, rule, max_rules)
            .await
    }

    async fn get_workflow_rule(
        &self,
        namespace_id: NamespaceId,
        rule_id: &str,
    ) -> Result<Option<WorkflowRuleRecord>> {
        self.do_get_workflow_rule(namespace_id, rule_id).await
    }

    async fn delete_workflow_rule(
        &self,
        namespace_id: NamespaceId,
        rule_id: &str,
    ) -> Result<WorkflowRuleDeleteResult> {
        self.do_delete_workflow_rule(namespace_id, rule_id).await
    }

    async fn list_workflow_rules(
        &self,
        namespace_id: NamespaceId,
    ) -> Result<Vec<WorkflowRuleRecord>> {
        self.do_list_workflow_rules(namespace_id).await
    }

    async fn commit_transition(
        &self,
        run_key: RunKey,
        transition: Transition,
        epoch: ShardEpoch,
    ) -> Result<CommitResult> {
        self.do_commit_transition(run_key, transition, epoch).await
    }

    async fn commit_transition_for_bundle(
        &self,
        run_key: RunKey,
        execution_home_bundle: ShardId,
        transition: Transition,
        epoch: ShardEpoch,
    ) -> Result<CommitResult> {
        self.do_commit_transition_for_bundle(run_key, execution_home_bundle, transition, epoch)
            .await
    }

    async fn delete_run_for_bundle(
        &self,
        run_key: RunKey,
        execution_home_bundle: ShardId,
        request: DeleteRunRequest,
        epoch: ShardEpoch,
    ) -> Result<DeleteRunResult> {
        self.do_delete_run_for_bundle(run_key, execution_home_bundle, request, epoch)
            .await
    }

    async fn materialize_reset_successor(
        &self,
        base_run_key: RunKey,
        fork_event_id: i64,
        successor_run_id: RunId,
    ) -> Result<()> {
        self.do_materialize_reset_successor(base_run_key, fork_event_id, successor_run_id)
            .await
    }

    async fn list_dispatchable_workflow_tasks(
        &self,
        queue: &QueueKey,
        limit: usize,
    ) -> Result<Vec<DispatchableWorkflowTask>> {
        self.do_list_dispatchable_workflow_tasks(queue, limit).await
    }

    async fn list_dispatchable_activity_tasks(
        &self,
        queue: &QueueKey,
        limit: usize,
    ) -> Result<Vec<DispatchableActivityTask>> {
        self.do_list_dispatchable_activity_tasks(queue, limit).await
    }

    async fn persist_to_backlog(&self, entries: Vec<BacklogEntry>) -> Result<()> {
        self.do_persist_to_backlog(entries).await
    }

    async fn drain_backlog(&self, queue: &QueueKey, limit: usize) -> Result<Vec<BacklogEntry>> {
        self.do_drain_backlog(queue, limit).await
    }

    async fn list_due_timers(&self, now: OffsetDateTime, limit: usize) -> Result<Vec<DueTimer>> {
        self.do_list_due_timers(now, limit).await
    }

    async fn list_dispatchable_workflow_tasks_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<DispatchableWorkflowTask>> {
        self.do_list_dispatchable_workflow_tasks_for_shard(shard_id, limit)
            .await
    }

    async fn list_dispatchable_activity_tasks_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<DispatchableActivityTask>> {
        self.do_list_dispatchable_activity_tasks_for_shard(shard_id, limit)
            .await
    }

    async fn list_due_timers_for_shard(
        &self,
        shard_id: ShardId,
        now: OffsetDateTime,
        limit: usize,
    ) -> Result<Vec<DueTimer>> {
        self.do_list_due_timers_for_shard(shard_id, now, limit)
            .await
    }

    async fn list_runs_with_workflow_timeouts_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<WorkflowTimeoutSweepEntry>> {
        self.do_list_runs_with_workflow_timeouts_for_shard(shard_id, limit)
            .await
    }

    async fn list_started_workflow_tasks_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<WftTimeoutSweepEntry>> {
        self.do_list_started_workflow_tasks_for_shard(shard_id, limit)
            .await
    }

    async fn list_open_activities_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<ActivitySweepEntry>> {
        self.do_list_open_activities_for_shard(shard_id, limit)
            .await
    }

    async fn list_pending_nexus_operations_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<NexusSweepEntry>> {
        self.do_list_pending_nexus_operations_for_shard(shard_id, limit)
            .await
    }

    async fn list_runs_with_pending_completion_callbacks_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<CompletionCallbackSweepEntry>> {
        self.do_list_runs_with_pending_completion_callbacks_for_shard(shard_id, limit)
            .await
    }
}

fn partition_for(run_key: RunKey, partition_count: u32) -> u32 {
    debug_assert!(partition_count > 0);
    // The projection partition is stable because it is derived from the spread
    // run UUID, not from insertion order.
    (run_key.0.as_u128() as u32) % partition_count
}

/// Encode an optional string for use in spread-key hash input.
///
/// Uses an explicit tag byte (0x00 for None, 0x01 for Some) so that
/// `None` and `Some("")` produce different hash inputs. This is
/// important because the type system does not prevent empty strings
/// in `DeploymentId` or `BuildId`.
fn option_key_part(value: Option<&str>) -> Vec<u8> {
    match value {
        Some(value) => {
            let mut bytes = Vec::with_capacity(value.len() + 1);
            bytes.push(1);
            bytes.extend_from_slice(value.as_bytes());
            bytes
        }
        None => vec![0],
    }
}

pub(crate) fn epoch_to_sql(epoch: ShardEpoch) -> Result<i64> {
    // DSQL stores epochs in BIGINT. Rejecting overflow here is preferable to a
    // connector/database error after a transaction has started.
    convert::i64_from_u64(epoch.0, "shard_lease.epoch")
}

pub(crate) fn epoch_from_sql(value: i64) -> Result<ShardEpoch> {
    // Negative epochs indicate corrupt storage or an incompatible manual edit.
    Ok(ShardEpoch(convert::u64_from_i64(
        value,
        "shard_lease.epoch",
    )?))
}

fn should_check_epoch(epoch: ShardEpoch) -> bool {
    // `ShardEpoch::ZERO` is reserved for tests and unfenced local flows. Real
    // DSQL commit paths should pass a lease epoch obtained from the controller.
    epoch != ShardEpoch::ZERO
}

#[cfg(test)]
mod tests {
    use std::{
        borrow::Cow,
        error::Error,
        fmt,
        sync::{Arc, Mutex},
    };

    use anyhow::{Result, anyhow, bail};
    use async_trait::async_trait;
    use proptest::prelude::*;
    use time::{Duration, OffsetDateTime};
    use tokeira_kernel::{
        ActivityState, HistoryEvent, HistoryEventKind, PendingNexusOperation, PendingWorkflowTask,
        ProjectionOp, TimerState, Transition, WorkflowState,
    };
    use tokeira_types::{
        ArchetypeId, BuildId, DeploymentId, ExecutionRef, ExecutionStatus, LogicalTaskSeq, Memo,
        NamespaceId, Payloads, QueueKey, RequestId, RunId, RunKey, SearchAttributes, ShardEpoch,
        StickyAffinity, TaskKind, TaskQueueName, TransitionSeq, VisibilityLifecycleState,
        WorkerIdentity, WorkflowId, WorkflowType,
    };

    use super::{
        ActivityDispatchRow, DEFAULT_HISTORY_PAGE_SIZE, DsqlConnectionAcquirer, DsqlRunRepository,
        RenewDecision, activity_dispatch_from_row, classify_connection_error, classify_outcome,
        collect_activity_sweep_entries, collect_dispatchable_workflow_tasks,
        collect_nexus_sweep_entries, collect_started_workflow_task_entries,
        collect_workflow_timeout_entries, decide_renew, effective_history_limit, epoch_from_sql,
        epoch_to_sql, extract_sqlstate, interpret_acquire, partition_for, should_check_epoch,
        sticky_fields,
    };
    use crate::{
        CurrentExecutionConflictPolicy, DbClass, LeaseOutcome, LeaseRepository, ProjectionContext,
        RunRepository,
        dsql::{DsqlPermit, codec},
    };

    proptest! {
        #[test]
        fn shard_assignment_is_deterministic(run_key_bytes in any::<[u8; 16]>(), shard_count in 1u32..1024) {
            let run_key = RunKey(uuid::Uuid::from_bytes(run_key_bytes));
            let first = DsqlRunRepository::shard_for_run_key_with_count(run_key, shard_count).unwrap();
            let second = DsqlRunRepository::shard_for_run_key_with_count(run_key, shard_count).unwrap();
            prop_assert_eq!(first, second);
            prop_assert_eq!(first.0, (run_key.0.as_u128() as u32) % shard_count);
        }

        #[test]
        fn configured_projection_partition_count_bounds_written_partitions(
            run_key_bytes in any::<[u8; 16]>(),
            partition_count in 1u32..64,
        ) {
            let run_key = RunKey(uuid::Uuid::from_bytes(run_key_bytes));
            let partition_id = partition_for(run_key, partition_count);

            prop_assert!(partition_id < partition_count);
        }

        #[test]
        fn read_history_effective_limit_preserves_finite_limits(limit in 0usize..10_000) {
            prop_assert_eq!(effective_history_limit(limit), limit);
        }

        #[test]
        fn read_history_legacy_unbounded_limit_uses_default_page_size(_seed in any::<u64>()) {
            prop_assert_eq!(effective_history_limit(usize::MAX), DEFAULT_HISTORY_PAGE_SIZE);
        }

        #[test]
        fn shard_id_uuid_encoding_is_deterministic(shard_id in any::<u32>()) {
            let first = DsqlRunRepository::shard_id_to_uuid(tokeira_types::ShardId(shard_id));
            let second = DsqlRunRepository::shard_id_to_uuid(tokeira_types::ShardId(shard_id));
            prop_assert_eq!(first, second);
        }

        #[test]
        fn shard_id_uuid_encoding_spreads_across_keyspace(a in 0u32..1024, b in 0u32..1024) {
            prop_assume!(a != b);
            let uuid_a = DsqlRunRepository::shard_id_to_uuid(tokeira_types::ShardId(a));
            let uuid_b = DsqlRunRepository::shard_id_to_uuid(tokeira_types::ShardId(b));
            // Different shard IDs must produce different UUIDs.
            prop_assert_ne!(uuid_a, uuid_b);
        }

        #[test]
        fn table_key_helpers_are_deterministic(seed in any::<u128>(), seq in any::<u64>()) {
            let namespace_id = NamespaceId(uuid::Uuid::from_u128(seed));
            let workflow_id = WorkflowId(format!("workflow-{seed}"));
            let request_id = RequestId(format!("request-{seed}"));

            prop_assert_eq!(
                DsqlRunRepository::current_execution_key(namespace_id, &workflow_id),
                DsqlRunRepository::current_execution_key(namespace_id, &workflow_id)
            );
            prop_assert_eq!(
                DsqlRunRepository::request_dedupe_key(namespace_id, &workflow_id, &request_id),
                DsqlRunRepository::request_dedupe_key(namespace_id, &workflow_id, &request_id)
            );
            prop_assert_eq!(
                DsqlRunRepository::dispatch_backlog_key(
                    7,
                    namespace_id,
                    "queue",
                    TaskKind::Workflow,
                    Some("deployment"),
                    Some("build"),
                    seq,
                ),
                DsqlRunRepository::dispatch_backlog_key(
                    7,
                    namespace_id,
                    "queue",
                    TaskKind::Workflow,
                    Some("deployment"),
                    Some("build"),
                    seq,
                )
            );
            prop_assert_eq!(
                DsqlRunRepository::activity_dispatch_key(RunKey(uuid::Uuid::from_u128(seed)), "activity"),
                DsqlRunRepository::activity_dispatch_key(RunKey(uuid::Uuid::from_u128(seed)), "activity")
            );
        }

        #[test]
        fn backlog_key_includes_full_queue_identity(
            namespace in any::<u128>(),
            seq in any::<u64>(),
        ) {
            let namespace_id = NamespaceId(uuid::Uuid::from_u128(namespace));
            let base = DsqlRunRepository::dispatch_backlog_key(
                1,
                namespace_id,
                "queue",
                TaskKind::Workflow,
                None,
                None,
                seq,
            );

            prop_assert_ne!(
                base,
                DsqlRunRepository::dispatch_backlog_key(
                    1,
                    namespace_id,
                    "queue",
                    TaskKind::Activity,
                    None,
                    None,
                    seq,
                )
            );
            prop_assert_ne!(
                base,
                DsqlRunRepository::dispatch_backlog_key(
                    1,
                    namespace_id,
                    "queue",
                    TaskKind::Workflow,
                    Some("deployment"),
                    None,
                    seq,
                )
            );
            prop_assert_ne!(
                base,
                DsqlRunRepository::dispatch_backlog_key(
                    1,
                    namespace_id,
                    "queue",
                    TaskKind::Workflow,
                    None,
                    Some("build"),
                    seq,
                )
            );
        }

        #[test]
        fn activity_dispatch_key_includes_activity_identity(seed in any::<u128>()) {
            let run_key = RunKey(uuid::Uuid::from_u128(seed));
            prop_assert_ne!(
                DsqlRunRepository::activity_dispatch_key(run_key, "activity-a"),
                DsqlRunRepository::activity_dispatch_key(run_key, "activity-b")
            );
        }

        #[test]
        fn codec_round_trips_core_persistence_types(seed in 1u64..1_000_000) {
            let run_key = RunKey(uuid::Uuid::from_u128(seed as u128));
            let workflow_state = sample_state(run_key);
            let history_events = vec![sample_history_event(seed as i64)];
            let activity_state = sample_activity_state(seed);
            let timer_state = sample_timer_state(seed);
            let projection_context = sample_projection_context(&workflow_state);
            let projection_ops = vec![ProjectionOp::CloseExecution {
                status: ExecutionStatus::Completed,
                closed_at: fixed_now(),
            }];

            prop_assert_eq!(
                codec::decode_workflow_state(&codec::encode_workflow_state(&workflow_state).unwrap()).unwrap(),
                workflow_state
            );
            prop_assert_eq!(
                codec::decode_history_events(&codec::encode_history_events(&history_events).unwrap()).unwrap(),
                history_events
            );
            prop_assert_eq!(
                codec::decode_activity_state(&codec::encode_activity_state(&activity_state).unwrap()).unwrap(),
                activity_state
            );
            prop_assert_eq!(
                codec::decode_timer_state(&codec::encode_timer_state(&timer_state).unwrap()).unwrap(),
                timer_state
            );
            prop_assert_eq!(
                codec::decode_projection_context(&codec::encode_projection_context(&projection_context).unwrap()).unwrap(),
                projection_context
            );
            prop_assert_eq!(
                codec::decode_projection_ops(&codec::encode_projection_ops(&projection_ops).unwrap()).unwrap(),
                projection_ops
            );
        }

        #[test]
        fn sticky_affinity_expiry_clearing(delta_seconds in -100i64..100i64) {
            let now = fixed_now();
            let mut state = sample_state(RunKey::new());
            state.sticky = Some(StickyAffinity {
                sticky_queue: tokeira_types::TaskQueueName(String::new()),
                schedule_to_start_timeout: time::Duration::ZERO,
                worker_identity: WorkerIdentity("sticky-worker".to_owned()),
                expires_at: now + Duration::seconds(delta_seconds),
            });

            let (worker, expires_at) = sticky_fields(&state, now);
            if delta_seconds > 0 {
                prop_assert_eq!(worker, Some(WorkerIdentity("sticky-worker".to_owned())));
                prop_assert_eq!(expires_at, Some(now + Duration::seconds(delta_seconds)));
            } else {
                prop_assert_eq!(worker, None);
                prop_assert_eq!(expires_at, None);
            }
        }

        #[test]
        fn result_limit_invariant_for_collect_helpers(limit in 0usize..16usize) {
            let rows = vec![
                encoded_workflow_row(sample_state(RunKey::new())),
                encoded_workflow_row(sample_state(RunKey::new())),
                encoded_workflow_row(sample_state(RunKey::new())),
            ];
            prop_assert!(collect_dispatchable_workflow_tasks(rows.clone(), None, limit).unwrap().len() <= limit);
            prop_assert!(collect_workflow_timeout_entries(rows.clone(), limit).unwrap().len() <= limit);
            prop_assert!(collect_started_workflow_task_entries(rows.clone(), limit).unwrap().len() <= limit);
            prop_assert!(collect_nexus_sweep_entries(rows, limit).unwrap().len() <= limit);
        }

        #[test]
        fn shard_epoch_round_trip(value in 1u64..=i64::MAX as u64) {
            let epoch = ShardEpoch(value);

            prop_assert_eq!(epoch_from_sql(epoch_to_sql(epoch).unwrap()).unwrap(), epoch);
        }

        #[test]
        fn acquire_interpretation_accepts_valid_outcomes(epoch in 1i64..i64::MAX) {
            prop_assert_eq!(
                interpret_acquire(1, 0, Some(1), None).unwrap(),
                LeaseOutcome::Acquired { epoch: ShardEpoch(1) }
            );
            prop_assert_eq!(
                interpret_acquire(0, 1, Some(epoch), None).unwrap(),
                LeaseOutcome::Acquired { epoch: ShardEpoch(epoch as u64) }
            );
            prop_assert_eq!(
                interpret_acquire(0, 0, None, Some(("owner".to_owned(), epoch))).unwrap(),
                LeaseOutcome::Rejected {
                    current_owner: "owner".to_owned(),
                    current_epoch: ShardEpoch(epoch as u64),
                }
            );
        }

        #[test]
        fn active_lease_rejection_preserves_owner_and_epoch(owner in "\\PC{1,64}", epoch in 1i64..i64::MAX) {
            let outcome = interpret_acquire(0, 0, None, Some((owner.clone(), epoch))).unwrap();

            prop_assert_eq!(
                outcome,
                LeaseOutcome::Rejected {
                    current_owner: owner,
                    current_epoch: ShardEpoch(epoch as u64),
                }
            );
        }

        #[test]
        fn renew_decision_requires_owner_and_epoch_match(
            current_owner in "\\PC{1,64}",
            caller_owner in "\\PC{1,64}",
            current_epoch in 1i64..i64::MAX,
            caller_epoch in 1i64..i64::MAX,
        ) {
            let decision = decide_renew(
                Some((current_owner.as_str(), current_epoch)),
                caller_owner.as_str(),
                caller_epoch,
            )
            .unwrap();

            if current_owner == caller_owner && current_epoch == caller_epoch {
                prop_assert_eq!(decision, RenewDecision::Renew);
            } else {
                prop_assert_eq!(
                    decision,
                    RenewDecision::Reject {
                        current_owner,
                        current_epoch: ShardEpoch(current_epoch as u64),
                    }
                );
            }
        }
    }

    #[test]
    fn shard_id_uuid_encoding_changed_from_old_sha256_scheme() {
        // This test verifies the BLAKE3-based shard UUID differs from the old
        // SHA-256 scheme. It imports sha2 directly — if sha2 is ever removed
        // from tokeira-storage's dependencies, replace this with a hardcoded
        // expected UUID value.
        use sha2::{Digest, Sha256};

        let shard_id = tokeira_types::ShardId(42);
        let mut hasher = Sha256::new();
        hasher.update(b"tokeira-shard-id:");
        hasher.update(shard_id.0.to_le_bytes());
        let hash = hasher.finalize();
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&hash[..16]);
        let old_uuid = uuid::Uuid::from_bytes(bytes);

        assert_ne!(DsqlRunRepository::shard_id_to_uuid(shard_id), old_uuid);
    }

    #[test]
    fn shard_assignment_rejects_zero_count() {
        assert!(DsqlRunRepository::shard_for_run_key_with_count(RunKey::new(), 0).is_err());
    }

    #[test]
    fn constructor_rejects_zero_shard_count() {
        let recorder = Arc::new(RecordingAcquirer::default());
        assert!(
            DsqlRunRepository::new_with_acquirer(
                recorder,
                0,
                16,
                CurrentExecutionConflictPolicy::Reject,
                Duration::seconds(30),
            )
            .is_err()
        );
    }

    #[test]
    fn constructor_rejects_non_positive_lease_duration() {
        let recorder = Arc::new(RecordingAcquirer::default());
        assert!(
            DsqlRunRepository::new_with_acquirer(
                recorder,
                4,
                16,
                CurrentExecutionConflictPolicy::Reject,
                Duration::ZERO,
            )
            .is_err()
        );
    }

    #[test]
    fn epoch_conversion_rejects_out_of_range_values() {
        assert!(epoch_to_sql(ShardEpoch(i64::MAX as u64 + 1)).is_err());
        assert!(epoch_from_sql(-1).is_err());
    }

    #[test]
    fn acquire_interpretation_rejects_invalid_sql_outcomes() {
        assert!(interpret_acquire(1, 1, Some(1), None).is_err());
        assert!(interpret_acquire(0, 0, None, None).is_err());
        assert!(interpret_acquire(1, 0, None, None).is_err());
        assert!(interpret_acquire(0, 1, None, None).is_err());
        assert!(interpret_acquire(1, 0, Some(2), None).is_err());
        assert!(interpret_acquire(0, 1, Some(-1), None).is_err());
        assert!(interpret_acquire(0, 0, None, Some(("owner".to_owned(), -1))).is_err());
    }

    #[test]
    fn absent_lease_renewal_is_rejected() {
        assert_eq!(
            decide_renew(None, "owner", 1).unwrap(),
            RenewDecision::Reject {
                current_owner: String::new(),
                current_epoch: ShardEpoch::ZERO,
            }
        );
    }

    #[test]
    fn serialization_failure_detection_is_false_for_non_database_errors() {
        assert!(!DsqlRunRepository::is_serialization_failure(
            &sqlx::Error::RowNotFound
        ));
    }

    #[test]
    fn serialization_failure_detection_matches_sqlstate_40001() {
        let retryable = sqlx::Error::Database(Box::new(TestDatabaseError {
            code: Some("40001"),
        }));
        let other = sqlx::Error::Database(Box::new(TestDatabaseError {
            code: Some("23505"),
        }));

        assert!(DsqlRunRepository::is_serialization_failure(&retryable));
        assert!(!DsqlRunRepository::is_serialization_failure(&other));
    }

    #[test]
    fn outcome_and_error_classification_helpers_match_contract() {
        let ok: Result<()> = Ok(());
        let generic_error: Result<()> = Err(anyhow!("boom"));
        let serialization_error: Result<()> =
            Err(sqlx::Error::Database(Box::new(TestDatabaseError {
                code: Some("40001"),
            }))
            .into());

        assert_eq!(classify_outcome(&ok), "success");
        assert_eq!(classify_outcome(&generic_error), "error");
        assert_eq!(classify_outcome(&serialization_error), "conflict");
    }

    #[test]
    fn sqlstate_extraction_returns_database_error_code() {
        let error = anyhow!(sqlx::Error::Database(Box::new(TestDatabaseError {
            code: Some("23505"),
        })));

        assert_eq!(extract_sqlstate(&error), Some("23505".to_owned()));
    }

    #[test]
    fn connection_error_classification_maps_io_kinds() {
        let timeout = anyhow!(sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "timeout",
        )));
        let refused = anyhow!(sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "refused",
        )));

        assert_eq!(classify_connection_error(&timeout), Some("timeout"));
        assert_eq!(classify_connection_error(&refused), Some("refused"));
    }

    #[test]
    fn zero_epoch_bypasses_fence_check() {
        assert!(!should_check_epoch(tokeira_types::ShardEpoch::ZERO));
        assert!(should_check_epoch(tokeira_types::ShardEpoch(1)));
    }

    #[test]
    fn sticky_fields_without_affinity_or_expired_affinity_returns_none() {
        let now = fixed_now();
        let mut state = sample_state(RunKey::new());

        assert_eq!(sticky_fields(&state, now), (None, None));

        state.sticky = Some(StickyAffinity {
            sticky_queue: tokeira_types::TaskQueueName(String::new()),
            schedule_to_start_timeout: time::Duration::ZERO,
            worker_identity: WorkerIdentity("worker".to_owned()),
            expires_at: now,
        });
        assert_eq!(sticky_fields(&state, now), (None, None));
    }

    #[test]
    fn sticky_fields_with_live_affinity_returns_values() {
        let now = fixed_now();
        let mut state = sample_state(RunKey::new());
        let expires_at = now + Duration::seconds(30);
        state.sticky = Some(StickyAffinity {
            sticky_queue: tokeira_types::TaskQueueName(String::new()),
            schedule_to_start_timeout: time::Duration::ZERO,
            worker_identity: WorkerIdentity("worker".to_owned()),
            expires_at,
        });

        assert_eq!(
            sticky_fields(&state, now),
            (Some(WorkerIdentity("worker".to_owned())), Some(expires_at))
        );
    }

    #[test]
    fn workflow_dispatch_collects_only_scheduled_unstarted_tasks() {
        let run_key = RunKey::new();
        let eligible = sample_state(run_key);
        let mut no_task = sample_state(RunKey::new());
        no_task.pending_workflow_task = None;
        let mut started = sample_state(RunKey::new());
        if let Some(task) = started.pending_workflow_task.as_mut() {
            task.started_event_id = Some(10);
        }

        let tasks = collect_dispatchable_workflow_tasks(
            vec![
                encoded_workflow_row(no_task),
                encoded_workflow_row(started),
                encoded_workflow_row(eligible),
            ],
            None,
            10,
        )
        .unwrap();

        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].run_key, run_key);
        assert_eq!(tasks[0].queue.task_kind, TaskKind::Workflow);
    }

    #[test]
    fn workflow_timeout_collects_only_open_runs_with_timeouts() {
        let run_key = RunKey::new();
        let mut eligible = sample_state(run_key);
        eligible.workflow_run_timeout = Some(Duration::seconds(60));
        let mut no_timeout = sample_state(RunKey::new());
        no_timeout.workflow_run_timeout = None;
        no_timeout.workflow_execution_timeout = None;
        let mut closed = sample_state(RunKey::new());
        closed.status = ExecutionStatus::Completed;
        closed.workflow_run_timeout = Some(Duration::seconds(60));

        let entries = collect_workflow_timeout_entries(
            vec![
                encoded_workflow_row(no_timeout),
                encoded_workflow_row(closed),
                encoded_workflow_row(eligible.clone()),
            ],
            10,
        )
        .unwrap();

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].run_key, run_key);
        assert_eq!(
            entries[0].workflow_run_timeout,
            eligible.workflow_run_timeout
        );
    }

    #[test]
    fn started_wft_collects_only_started_pending_tasks() {
        let run_key = RunKey::new();
        let mut started = sample_state(run_key);
        if let Some(task) = started.pending_workflow_task.as_mut() {
            task.started_event_id = Some(10);
            task.started_at = Some(fixed_now());
        }
        let scheduled = sample_state(RunKey::new());

        let entries = collect_started_workflow_task_entries(
            vec![
                encoded_workflow_row(scheduled),
                encoded_workflow_row(started.clone()),
            ],
            10,
        )
        .unwrap();

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].run_key, run_key);
        assert_eq!(entries[0].started_event_id, 10);
    }

    #[test]
    fn nexus_collect_applies_total_limit_across_runs() {
        let mut first = sample_state(RunKey::new());
        first
            .pending_nexus_operations
            .insert("op-1".to_owned(), sample_nexus_operation("op-1", true));
        first
            .pending_nexus_operations
            .insert("op-2".to_owned(), sample_nexus_operation("op-2", false));
        let mut second = sample_state(RunKey::new());
        second
            .pending_nexus_operations
            .insert("op-3".to_owned(), sample_nexus_operation("op-3", true));

        let entries = collect_nexus_sweep_entries(
            vec![encoded_workflow_row(first), encoded_workflow_row(second)],
            1,
        )
        .unwrap();

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].operation_id, "op-1");
    }

    #[test]
    fn activity_dispatch_row_preserves_field_fidelity() {
        let run_key = RunKey::new();
        let namespace_id = NamespaceId::new();
        let payloads = Payloads::default();
        let row: ActivityDispatchRow = (
            run_key.0,
            "activity".to_owned(),
            namespace_id.0,
            "queue".to_owned(),
            TaskKind::Activity.to_db_smallint(),
            Some("deployment".to_owned()),
            Some("build".to_owned()),
            42,
            3,
            17,
            5,
            codec::encode_payloads(&payloads).unwrap(),
        );

        let task = activity_dispatch_from_row(row).unwrap();

        assert_eq!(task.run_key, run_key);
        assert_eq!(task.activity_id, "activity");
        assert_eq!(task.input, payloads);
        assert_eq!(task.schedule_event_id, 42);
        assert_eq!(task.attempt, 3);
        assert_eq!(task.dispatch_revision, 17);
        assert_eq!(task.stamp, 5);
        assert_eq!(task.queue.namespace_id, namespace_id);
        assert_eq!(task.queue.task_kind, TaskKind::Activity);
        assert_eq!(
            task.queue.deployment,
            Some(DeploymentId("deployment".to_owned()))
        );
        assert_eq!(task.queue.build_id, Some(BuildId("build".to_owned())));
    }

    #[test]
    fn activity_sweep_mapping_preserves_state_fields() {
        let run_key = RunKey::new();
        let activity = sample_activity_state(7);

        let entries = collect_activity_sweep_entries(vec![(
            run_key.0,
            codec::encode_activity_state(&activity).unwrap(),
        )])
        .unwrap();

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].run_key, run_key);
        assert_eq!(entries[0].activity_id, activity.activity_id);
        assert_eq!(entries[0].schedule_event_id, activity.schedule_event_id);
        assert_eq!(entries[0].attempt, activity.attempt);
        assert_eq!(entries[0].original_scheduled_at, activity.scheduled_at);
        assert_eq!(entries[0].started_at, activity.started_at);
        assert_eq!(
            entries[0].schedule_to_close_timeout,
            activity.schedule_to_close_timeout
        );
        assert_eq!(
            entries[0].schedule_to_start_timeout,
            activity.schedule_to_start_timeout
        );
        assert_eq!(
            entries[0].start_to_close_timeout,
            activity.start_to_close_timeout
        );
        assert_eq!(entries[0].heartbeat_timeout, activity.heartbeat_timeout);
    }

    #[tokio::test]
    async fn read_operations_request_read_class() {
        let recorder = Arc::new(RecordingAcquirer::default());
        let repo = test_repo(Arc::clone(&recorder));
        let run_key = RunKey::new();
        let execution = ExecutionRef {
            namespace_id: NamespaceId::new(),
            workflow_id: WorkflowId("workflow".to_owned()),
            run_id: None,
        };

        let _ = repo.load_run(run_key).await;
        let _ = repo.resolve_execution(&execution).await;
        let _ = repo
            .find_latest_run(execution.namespace_id, &execution.workflow_id)
            .await;
        let _ = repo.read_history(run_key, 0, 1).await;
        let _ = repo
            .lookup_request_dedupe(&execution, &tokeira_types::RequestId("request".to_owned()))
            .await;
        let _ = repo.read_transition_audit(run_key).await;

        assert_eq!(
            recorder.classes(),
            vec![
                DbClass::Read,
                DbClass::Read,
                DbClass::Read,
                DbClass::Read,
                DbClass::Read,
                DbClass::Read,
            ]
        );
    }

    #[tokio::test]
    async fn write_operations_request_commit_class() {
        let recorder = Arc::new(RecordingAcquirer::default());
        let repo = test_repo(Arc::clone(&recorder));
        let run_key = RunKey::new();

        let _ = repo
            .commit_transition(
                run_key,
                sample_transition(run_key),
                tokeira_types::ShardEpoch::ZERO,
            )
            .await;
        let _ = repo
            .materialize_reset_successor(run_key, 1, RunId::new())
            .await;

        assert_eq!(recorder.classes(), vec![DbClass::Commit, DbClass::Commit]);
    }

    #[tokio::test]
    async fn lease_operations_request_control_class() {
        let recorder = Arc::new(RecordingAcquirer::default());
        let repo = test_repo(Arc::clone(&recorder));

        let _ = repo
            .try_acquire_bundle(
                tokeira_types::ShardId(1),
                "owner".to_owned(),
                "127.0.0.1:7233".to_owned(),
            )
            .await;
        let _ = repo
            .renew_bundle(
                tokeira_types::ShardId(1),
                "owner".to_owned(),
                ShardEpoch(1),
                "127.0.0.1:7233".to_owned(),
            )
            .await;

        assert_eq!(recorder.classes(), vec![DbClass::Control, DbClass::Control]);
    }

    #[tokio::test]
    async fn read_history_zero_limit_does_not_acquire_connection() {
        let recorder = Arc::new(RecordingAcquirer::default());
        let repo = test_repo(Arc::clone(&recorder));

        let result = repo.read_history(RunKey::new(), 0, 0).await.unwrap();

        assert!(result.is_empty());
        assert!(recorder.classes().is_empty());
    }

    #[tokio::test]
    async fn side_table_queries_zero_limit_do_not_acquire_connection() {
        let recorder = Arc::new(RecordingAcquirer::default());
        let repo = test_repo(Arc::clone(&recorder));
        let queue = QueueKey {
            namespace_id: NamespaceId::new(),
            task_queue: TaskQueueName("queue".to_owned()),
            task_kind: TaskKind::Workflow,
            deployment: None,
            build_id: None,
        };
        let shard_id = tokeira_types::ShardId(0);

        assert!(
            repo.list_dispatchable_workflow_tasks(&queue, 0)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            repo.list_dispatchable_activity_tasks(&queue, 0)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            repo.list_due_timers(fixed_now(), 0)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            repo.list_dispatchable_workflow_tasks_for_shard(shard_id, 0)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            repo.list_dispatchable_activity_tasks_for_shard(shard_id, 0)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            repo.list_due_timers_for_shard(shard_id, fixed_now(), 0)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            repo.list_runs_with_workflow_timeouts_for_shard(shard_id, 0)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            repo.list_started_workflow_tasks_for_shard(shard_id, 0)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            repo.list_open_activities_for_shard(shard_id, 0)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            repo.list_pending_nexus_operations_for_shard(shard_id, 0)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(recorder.classes().is_empty());
    }

    #[derive(Debug, Default)]
    struct RecordingAcquirer {
        classes: Mutex<Vec<DbClass>>,
    }

    impl RecordingAcquirer {
        fn classes(&self) -> Vec<DbClass> {
            self.classes.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl DsqlConnectionAcquirer for RecordingAcquirer {
        async fn acquire(&self, class: DbClass) -> Result<DsqlPermit> {
            self.classes.lock().unwrap().push(class);
            bail!("test acquirer has no database connection")
        }
    }

    fn test_repo(recorder: Arc<RecordingAcquirer>) -> DsqlRunRepository {
        DsqlRunRepository::new_with_acquirer(
            recorder,
            4,
            16,
            CurrentExecutionConflictPolicy::Reject,
            Duration::seconds(30),
        )
        .unwrap()
    }

    fn fixed_now() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()
    }

    fn encoded_workflow_row(state: WorkflowState) -> (uuid::Uuid, Vec<u8>) {
        (
            state.run_key.0,
            codec::encode_workflow_state(&state).unwrap(),
        )
    }

    fn sample_nexus_operation(operation_id: &str, with_timeout: bool) -> PendingNexusOperation {
        PendingNexusOperation {
            operation_id: operation_id.to_owned(),
            scheduled_event_id: 42,
            endpoint: "endpoint".to_owned(),
            service: "service".to_owned(),
            operation: "operation".to_owned(),
            schedule_to_close_timeout: with_timeout.then_some(Duration::seconds(30)),
            schedule_to_start_timeout: None,
            start_to_close_timeout: None,
            scheduled_at: fixed_now(),
            started: false,
            started_at: None,
            attempt: 0,
            last_attempt_failure: None,
            next_attempt_at: None,
            operation_token: String::new(),
            input: Default::default(),
            cancellation: None,
        }
    }

    fn sample_transition(run_key: RunKey) -> Transition {
        Transition {
            expected_seq: TransitionSeq::ZERO,
            next_state: sample_state(run_key),
            history_events: Default::default(),
            event_principals: Default::default(),
            request_dedupe_ops: Default::default(),
            activity_ops: Default::default(),
            timer_ops: Default::default(),
            dispatch_ops: Default::default(),
            projection_ops: Default::default(),
        }
    }

    fn sample_state(run_key: RunKey) -> WorkflowState {
        WorkflowState {
            run_key,
            namespace_id: NamespaceId::new(),
            workflow_id: WorkflowId("workflow".to_owned()),
            run_id: RunId::new(),
            workflow_type: WorkflowType("workflow-type".to_owned()),
            task_queue: TaskQueueName("queue".to_owned()),
            deployment: None,
            build_id: None,
            status: ExecutionStatus::Running,
            transition_seq: TransitionSeq(1),
            last_event_id: 0,
            external_payload_count: 0,
            external_payload_size_bytes: 0,
            next_workflow_task_seq: LogicalTaskSeq(1),
            pending_workflow_task: Some(PendingWorkflowTask {
                task_type: tokeira_kernel::WorkflowTaskType::Normal,
                schedule_to_start_deadline: None,
                logical_seq: LogicalTaskSeq(1),
                scheduled_event_id: 1,
                scheduled_at: fixed_now(),
                started_event_id: None,
                started_at: None,
                attempt: 1,
            }),
            previous_started_event_id: 0,
            workflow_task_attempt: 1,
            workflow_task_attempts_since_last_success: 0,
            last_workflow_task_problem: None,
            sticky: None,
            pause_info: None,
            cancel_requested: false,
            wft_stamp: 0,
            memo: Memo::default(),
            search_attributes: SearchAttributes::default(),
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: Duration::seconds(10),
            retry_policy: None,
            attempt: 1,
            first_execution_run_id: None,
            original_execution_run_id: None,
            reset_run_id: None,
            parent_run_key: None,
            parent_workflow_id: None,
            parent_run_id: None,
            parent_namespace_id: None,
            parent_namespace_name: None,
            parent_initiated_event_id: 0,
            root_workflow_id: None,
            root_run_id: None,
            last_completion_result: None,
            activities: Default::default(),
            timers: Default::default(),
            children: Default::default(),
            pending_external_signals: Default::default(),
            pending_external_cancels: Default::default(),
            pending_updates: Default::default(),
            admitted_updates: Default::default(),
            pending_nexus_operations: Default::default(),
            versioning_info: None,
            worker_deployment_name: None,
            completion_callbacks: Vec::new(),
            user_metadata: None,
            links: Vec::new(),
            workflow_start_delay: None,
            priority: None,
            started_at: fixed_now(),
            first_run_started_at: None,
            closed_at: None,
            close_result: None,
            close_failure: None,
            request_id_infos: std::collections::BTreeMap::new(),
            buffered_events: Vec::new(),
        }
    }

    fn sample_history_event(event_id: i64) -> HistoryEvent {
        HistoryEvent {
            event_id,
            happened_at: fixed_now(),
            kind: HistoryEventKind::WorkflowExecutionSignaled {
                signal_name: format!("signal-{event_id}"),
                input: Payloads::default(),
                header: None,
                links: Vec::new(),
                request_id: format!("request-{event_id}"),
                identity: Some("tester".to_owned()),
            },
        }
    }

    fn sample_activity_state(seed: u64) -> ActivityState {
        ActivityState {
            cancel_requested: false,
            activity_reset: false,
            reset_heartbeats: false,
            started_identity: None,
            retry_last_worker_identity: None,
            activity_id: format!("activity-{seed}"),
            activity_type: "activity-type".to_owned(),
            schedule_event_id: i64::try_from(seed).unwrap(),
            task_queue: TaskQueueName("activity-queue".to_owned()),
            deployment: None,
            build_id: None,
            input: Payloads::default(),
            header: None,
            last_failure: None,
            heartbeat_details: None,
            attempt: 1,
            retry_policy: None,
            schedule_to_close_timeout: Some(Duration::seconds(30)),
            schedule_to_start_timeout: Some(Duration::seconds(10)),
            start_to_close_timeout: Some(Duration::seconds(20)),
            heartbeat_timeout: Some(Duration::seconds(5)),
            scheduled_at: fixed_now(),
            current_attempt_scheduled_at: None,
            started_at: None,
            started_event_id: None,
            pause_info: None,
            stamp: seed,
        }
    }

    fn sample_timer_state(seed: u64) -> TimerState {
        TimerState {
            timer_id: format!("timer-{seed}"),
            started_event_id: i64::try_from(seed).unwrap(),
            fire_at: fixed_now() + Duration::seconds(i64::try_from(seed % 60).unwrap()),
        }
    }

    fn sample_projection_context(state: &WorkflowState) -> ProjectionContext {
        ProjectionContext {
            archetype_id: ArchetypeId::WORKFLOW,
            namespace_id: state.namespace_id,
            business_id: state.workflow_id.0.clone(),
            authority_epoch: 0,
            status_keyword: format!("{:?}", state.status),
            lifecycle_state: if state.status.is_open() {
                VisibilityLifecycleState::Open
            } else {
                VisibilityLifecycleState::Closed
            },
            workflow_id: state.workflow_id.clone(),
            run_id: state.run_id,
            workflow_type: state.workflow_type.clone(),
            task_queue: state.task_queue.clone(),
            execution_status: state.status,
            start_time: state.started_at,
            update_time: state.closed_at.unwrap_or(state.started_at),
            // v1.31.0 ExecutionTime = StartTime + FirstWorkflowTaskBackoff
            // (mutable_state_impl.go:2859); tokeira carries that backoff (client
            // start delay / workflow-retry backoff) as `workflow_start_delay`.
            execution_time: Some(state.started_at + state.workflow_start_delay.unwrap_or_default()),
            close_time: state.closed_at,
            history_length: state.last_event_id,
            execution_duration: None,
            state_transition_count: state.transition_seq.0 as i64,
            transition_count: state.transition_seq.0 as i64,
            history_size_bytes: 0,
            parent_workflow_id: state.parent_workflow_id.clone(),
            parent_run_id: state.parent_run_id,
            root_workflow_id: state
                .root_workflow_id
                .clone()
                .or_else(|| Some(state.workflow_id.clone())),
            root_run_id: state.root_run_id.or(Some(state.run_id)),
            search_attr_generation: state.transition_seq.0,
            memo: state.memo.clone(),
            search_attributes: state.search_attributes.clone(),
        }
    }

    #[derive(Debug)]
    struct TestDatabaseError {
        code: Option<&'static str>,
    }

    impl fmt::Display for TestDatabaseError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("test database error")
        }
    }

    impl Error for TestDatabaseError {}

    impl sqlx::error::DatabaseError for TestDatabaseError {
        fn message(&self) -> &str {
            "test database error"
        }

        fn code(&self) -> Option<Cow<'_, str>> {
            self.code.map(Cow::Borrowed)
        }

        fn as_error(&self) -> &(dyn Error + Send + Sync + 'static) {
            self
        }

        fn as_error_mut(&mut self) -> &mut (dyn Error + Send + Sync + 'static) {
            self
        }

        fn into_error(self: Box<Self>) -> Box<dyn Error + Send + Sync + 'static> {
            self
        }

        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }
    }
}
