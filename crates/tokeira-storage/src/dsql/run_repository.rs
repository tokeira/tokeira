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
    ActivityOp, BasicKernel, DispatchOp, HistoryEvent, LoadedRun, ProjectionOp, ReplayContext,
    TimerOp, Transition, WorkflowState,
};
use tokeira_types::{
    BuildId, DeploymentId, ExecutionRef, ExecutionStatus, GenerationCounter, NamespaceId, Payloads,
    QueueKey, RequestId, RunId, RunKey, ShardEpoch, ShardId, TaskKind, TaskQueueName,
    TransitionSeq, WorkerIdentity, WorkflowId, dsql_spread_uuid,
};
use tracing::instrument;
use uuid::Uuid;

use crate::{
    ActivitySweepEntry, BacklogEntry, BudgetAllocationResult, BundleLease, CommitResult,
    ControlRepository, CurrentExecutionConflictPolicy, DbClass, DispatchableActivityTask,
    DispatchableWorkflowTask, DueTimer, GenerationAdvanceResult, LeaseOutcome, LeaseRepository,
    NexusSweepEntry, ProjectionContext, RequestRecord, RunRepository, TransitionAuditRecord,
    WftTimeoutSweepEntry, WorkflowTimeoutSweepEntry, metrics,
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

macro_rules! record_dsql_operation {
    ($repo:expr, $operation:expr, $shard_id:expr, $body:block) => {{
        let started = Instant::now();
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
        let started = Instant::now();
        let result = (async $body).await;
        $repo.record_commit_operation_result($operation, $shard_id, started.elapsed(), &result);
        result
    }};
}

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
            if outcome == "conflict" {
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
        if outcome == "conflict" {
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
    #[instrument(name = "dsql.resolve_execution", skip(self), fields(namespace_id = %execution.namespace_id.0, workflow_id = %execution.workflow_id.0))]
    async fn resolve_execution(&self, execution: &ExecutionRef) -> Result<Option<RunKey>> {
        record_dsql_operation!(self, "resolve_execution", None, {
            let mut permit = self.director.acquire(DbClass::Read).await?;
            if let Some(requested_run_id) = execution.run_id {
                // Explicit run IDs do not use `current_execution`; that row is
                // intentionally replaced by newer runs of the same workflow.
                let run_key = RunKey::derive(
                    execution.namespace_id,
                    &execution.workflow_id,
                    requested_run_id,
                );
                let row =
                    sqlx::query_as::<_, (i32,)>("SELECT 1 FROM workflow_hot WHERE run_key = $1")
                        .bind(run_key.0)
                        .fetch_optional(permit.connection()?)
                        .await?;
                return Ok(row.map(|_| run_key));
            }

            let key = Self::current_execution_key(execution.namespace_id, &execution.workflow_id);
            let row = sqlx::query_as::<_, (Uuid,)>(
                "SELECT run_key FROM current_execution
             WHERE key = $1 AND is_open = true",
            )
            .bind(key)
            .fetch_optional(permit.connection()?)
            .await?;
            Ok(row.map(|(run_key,)| RunKey(run_key)))
        })
    }

    #[instrument(name = "dsql.find_latest_run", skip(self), fields(namespace_id = %namespace_id.0, workflow_id = %workflow_id.0))]
    async fn find_latest_run(
        &self,
        namespace_id: NamespaceId,
        workflow_id: &WorkflowId,
    ) -> Result<Option<RunKey>> {
        record_dsql_operation!(self, "find_latest_run", None, {
            let mut permit = self.director.acquire(DbClass::Read).await?;
            let key = Self::current_execution_key(namespace_id, workflow_id);
            // `current_execution` is the latest-run pointer even after the run is
            // closed. Only `resolve_execution(None)` filters this row to open runs.
            let row = sqlx::query_as::<_, (Uuid,)>(
                "SELECT run_key FROM current_execution
             WHERE key = $1",
            )
            .bind(key)
            .fetch_optional(permit.connection()?)
            .await?;
            Ok(row.map(|(run_key,)| RunKey(run_key)))
        })
    }

    #[instrument(name = "dsql.load_run", skip(self), fields(run_key = %run_key.0))]
    async fn load_run(&self, run_key: RunKey) -> Result<LoadedRun> {
        record_dsql_operation!(self, "load_run", Some(self.shard_for_run_key(run_key)), {
            let mut permit = self.director.acquire(DbClass::Read).await?;
            let row = sqlx::query_as::<_, (Vec<u8>,)>(
                "SELECT state_data FROM workflow_hot WHERE run_key = $1",
            )
            .bind(run_key.0)
            .fetch_optional(permit.connection()?)
            .await?;
            match row {
                Some((state_data,)) => Ok(LoadedRun::Existing(codec::decode_workflow_state(
                    &state_data,
                )?)),
                None => Ok(LoadedRun::Absent),
            }
        })
    }

    #[instrument(name = "dsql.read_history", skip(self), fields(run_key = %run_key.0, after_event_id, limit))]
    async fn read_history(
        &self,
        run_key: RunKey,
        after_event_id: i64,
        limit: usize,
    ) -> Result<Vec<HistoryEvent>> {
        let result = record_dsql_operation!(
            self,
            "read_history",
            Some(self.shard_for_run_key(run_key)),
            {
                let effective_limit = effective_history_limit(limit);
                if effective_limit == 0 {
                    metrics::record_dsql_rows_read("read_history", 0);
                    return Ok(Vec::new());
                }

                let mut permit = self.director.acquire(DbClass::Read).await?;
                // History is stored in transition batches. A batch may straddle
                // `after_event_id`, so decoding and per-event filtering remain in Rust.
                let rows = sqlx::query_as::<_, (i64, i64, Vec<u8>)>(
                    "SELECT first_event_id, last_event_id, events_data
             FROM history_batch
             WHERE run_key = $1 AND last_event_id > $2
             ORDER BY first_event_id ASC",
                )
                .bind(run_key.0)
                .bind(after_event_id)
                .fetch_all(permit.connection()?)
                .await?;
                metrics::record_dsql_rows_read("read_history", rows.len());

                let mut events = Vec::new();
                for (_first_event_id, _last_event_id, events_data) in rows {
                    for event in codec::decode_history_events(&events_data)? {
                        if event.event_id <= after_event_id {
                            continue;
                        }
                        events.push(event);
                        if events.len() == effective_limit {
                            return Ok(events);
                        }
                    }
                }
                Ok(events)
            }
        );
        if let Ok(events) = &result {
            metrics::record_read_history_events(events.len());
        }
        result
    }

    #[instrument(name = "dsql.lookup_request_dedupe", skip(self), fields(namespace_id = %execution.namespace_id.0, workflow_id = %execution.workflow_id.0, request_id = %request_id.0))]
    async fn lookup_request_dedupe(
        &self,
        execution: &ExecutionRef,
        request_id: &RequestId,
    ) -> Result<Option<RequestRecord>> {
        record_dsql_operation!(self, "lookup_request_dedupe", None, {
            let mut permit = self.director.acquire(DbClass::Read).await?;
            let key = Self::request_dedupe_key(
                execution.namespace_id,
                &execution.workflow_id,
                request_id,
            );
            let row = sqlx::query_as::<_, (Uuid, String, i64, Uuid)>(
                "SELECT run_key, request_id, first_seen_transition_seq, run_id
             FROM request_dedupe
             WHERE key = $1",
            )
            .bind(key)
            .fetch_optional(permit.connection()?)
            .await?;

            let Some((run_key, stored_request_id, transition_seq, stored_run_id)) = row else {
                return Ok(None);
            };
            let run_id = RunId(stored_run_id);
            // A workflow-scoped dedupe key can still be queried through an
            // execution reference that names a specific run. In that case the
            // stored run must match the caller's run filter.
            if execution
                .run_id
                .is_some_and(|requested| requested != run_id)
            {
                return Ok(None);
            }

            Ok(Some(RequestRecord {
                namespace_id: execution.namespace_id,
                workflow_id: execution.workflow_id.clone(),
                run_id,
                run_key: RunKey(run_key),
                request_id: RequestId(stored_request_id),
                first_seen_transition_seq: TransitionSeq(convert::u64_from_i64(
                    transition_seq,
                    "request_dedupe.first_seen_transition_seq",
                )?),
            }))
        })
    }

    #[instrument(name = "dsql.read_transition_audit", skip(self), fields(run_key = %run_key.0))]
    async fn read_transition_audit(&self, run_key: RunKey) -> Result<Vec<TransitionAuditRecord>> {
        record_dsql_operation!(
            self,
            "read_transition_audit",
            Some(self.shard_for_run_key(run_key)),
            {
                let mut permit = self.director.acquire(DbClass::Read).await?;
                let rows = sqlx::query_as::<_, (i64, Vec<u8>)>(
                    "SELECT transition_seq, events_data FROM history_batch
             WHERE run_key = $1 ORDER BY first_event_id ASC",
                )
                .bind(run_key.0)
                .fetch_all(permit.connection()?)
                .await?;

                rows.into_iter()
                    .map(|(transition_seq, events_data)| {
                        Ok(TransitionAuditRecord {
                            run_key,
                            transition_seq: TransitionSeq(convert::u64_from_i64(
                                transition_seq,
                                "history_batch.transition_seq",
                            )?),
                            history_events: codec::decode_history_events(&events_data)?,
                            activity_ops: Vec::new(),
                            timer_ops: Vec::new(),
                            dispatch_ops: Vec::new(),
                            projection_ops: Vec::new(),
                        })
                    })
                    .collect()
            }
        )
    }

    #[instrument(name = "dsql.commit_transition", skip(self, transition), fields(run_key = %run_key.0, expected_seq = transition.expected_seq.0, epoch = epoch.0))]
    async fn commit_transition(
        &self,
        run_key: RunKey,
        transition: Transition,
        epoch: ShardEpoch,
    ) -> Result<CommitResult> {
        record_dsql_commit_operation!(
            self,
            "commit_transition",
            Some(self.shard_for_run_key(run_key)),
            {
                // Validate i64 conversions before acquiring a connection or starting a
                // transaction. This prevents mid-transaction failures from overflow on
                // values that are structurally u64 but stored as BIGINT (i64) in DSQL.
                convert::i64_from_u64(transition.next_state.transition_seq.0, "transition_seq")?;
                if should_check_epoch(epoch) {
                    convert::i64_from_u64(epoch.0, "caller shard epoch")?;
                }

                let mut permit = self.director.acquire(DbClass::Commit).await?;
                let mut tx = permit.connection()?.begin().await?;
                let state = transition.next_state.clone();
                let shard_id = tokeira_types::execution_home_bundle(
                    state.namespace_id.0.as_bytes(),
                    state.workflow_id.0.as_bytes(),
                    self.shard_count,
                );
                // Commit routing is derived from the same shard_count used by the
                // runtime ShardOwner. A mismatch here would make leases and rows
                // disagree about execution-home ownership.

                if should_check_epoch(epoch) {
                    // Epoch fencing ties a commit to the lane/shard lease that produced
                    // it. A stale owner must fail before reading or writing run state.
                    let row = sqlx::query_as::<_, (i64,)>(
                        "SELECT epoch FROM shard_lease WHERE shard_id = $1",
                    )
                    .bind(Self::shard_id_to_uuid(shard_id))
                    .fetch_optional(&mut *tx)
                    .await?;
                    let Some((durable_epoch,)) = row else {
                        tx.rollback().await?;
                        return Ok(CommitResult::Conflict {
                            reason: format!(
                                "no active lease for shard {:?} at epoch {:?}",
                                shard_id, epoch
                            ),
                        });
                    };
                    if durable_epoch != convert::i64_from_u64(epoch.0, "caller shard epoch")? {
                        tx.rollback().await?;
                        return Ok(CommitResult::Conflict {
                            reason: format!(
                                "stale shard epoch {:?} for shard {:?}; current {}",
                                epoch, shard_id, durable_epoch
                            ),
                        });
                    }
                }

                let started = Instant::now();
                let row = sqlx::query_as::<_, (i64,)>(
                    "SELECT transition_seq FROM workflow_hot WHERE run_key = $1 FOR UPDATE",
                )
                .bind(run_key.0)
                .fetch_optional(&mut *tx)
                .await?;
                metrics::record_dsql_statement_duration(
                    "commit_transition",
                    "load_hot",
                    started.elapsed(),
                );
                let current_seq = match row {
                    Some((seq,)) => {
                        TransitionSeq(convert::u64_from_i64(seq, "workflow_hot.transition_seq")?)
                    }
                    None => TransitionSeq::ZERO,
                };
                // The transition sequence is the per-run OCC fence. We check it inside
                // the same transaction as the write set so successful commits remain
                // linearizable for a single run.
                if current_seq != transition.expected_seq {
                    tx.rollback().await?;
                    return Ok(CommitResult::Conflict {
                        reason: format!(
                            "expected seq {:?}, found {:?}",
                            transition.expected_seq, current_seq
                        ),
                    });
                }

                for op in &transition.request_dedupe_ops {
                    let key = Self::request_dedupe_key(
                        state.namespace_id,
                        &state.workflow_id,
                        &op.request_id,
                    );
                    let started = Instant::now();
                    let row = sqlx::query_as::<_, (i32,)>(
                        "SELECT 1 FROM request_dedupe
                 WHERE key = $1",
                    )
                    .bind(key)
                    .fetch_optional(&mut *tx)
                    .await?;
                    metrics::record_dsql_statement_duration(
                        "commit_transition",
                        "dedupe_check",
                        started.elapsed(),
                    );
                    if row.is_some() {
                        // Dedupe is checked before any state mutation. Returning
                        // Duplicate lets callers short-circuit idempotent requests
                        // without turning them into conflicts.
                        tx.rollback().await?;
                        return Ok(CommitResult::Duplicate);
                    }
                }

                if transition.expected_seq == TransitionSeq::ZERO && state.status.is_open() {
                    let key = Self::current_execution_key(state.namespace_id, &state.workflow_id);
                    let started = Instant::now();
                    let row = sqlx::query_as::<_, (Uuid, bool)>(
                        "SELECT run_key, is_open FROM current_execution
                 WHERE key = $1",
                    )
                    .bind(key)
                    .fetch_optional(&mut *tx)
                    .await?;
                    metrics::record_dsql_statement_duration(
                        "commit_transition",
                        "current_execution_check",
                        started.elapsed(),
                    );
                    // Both Reject and AllowAfterClose reject when an open execution
                    // exists for a different run. When is_open is false under
                    // AllowAfterClose, the code intentionally falls through — the
                    // write set will replace the closed row via upsert_current_execution_start.
                    if let Some((existing_run_key, is_open)) = row
                        && is_open
                        && existing_run_key != run_key.0
                        && matches!(
                            self.conflict_policy,
                            CurrentExecutionConflictPolicy::Reject
                                | CurrentExecutionConflictPolicy::AllowAfterClose
                        )
                    {
                        tx.rollback().await?;
                        return Ok(CommitResult::Conflict {
                            reason: format!(
                                "current execution already exists for {}: {:?}",
                                state.workflow_id.0,
                                RunKey(existing_run_key)
                            ),
                        });
                    }
                }

                write_transition(
                    &mut tx,
                    run_key,
                    shard_id,
                    self.projection_partition_count,
                    &transition,
                    &state,
                )
                .await?;
                match tx.commit().await {
                    Ok(()) => {
                        metrics::record_dsql_commit_retries(0);
                        Ok(CommitResult::Applied { new_state: state })
                    }
                    // Aurora DSQL can reject a transaction at commit because another
                    // transaction won serialization. The runtime already knows how to
                    // reload and retry `Conflict`, so normalize SQLSTATE 40001 here.
                    Err(err) if Self::is_serialization_failure(&err) => {
                        Ok(CommitResult::Conflict {
                            reason: "DSQL serialization conflict".to_owned(),
                        })
                    }
                    Err(err) => Err(err.into()),
                }
            }
        )
    }

    #[instrument(name = "dsql.commit_transition_for_bundle", skip(self, transition), fields(run_key = %run_key.0, bundle = execution_home_bundle.0, expected_seq = transition.expected_seq.0, epoch = epoch.0))]
    async fn commit_transition_for_bundle(
        &self,
        run_key: RunKey,
        execution_home_bundle: ShardId,
        transition: Transition,
        epoch: ShardEpoch,
    ) -> Result<CommitResult> {
        record_dsql_commit_operation!(
            self,
            "commit_transition_for_bundle",
            Some(execution_home_bundle),
            {
                if should_check_epoch(epoch) {
                    // Multi-node/controller-managed deployments keep the
                    // durable shard_lease fence. Single-node compose passes
                    // ShardEpoch::ZERO and skips this read because there is no
                    // takeover actor that can advance the epoch.
                    convert::i64_from_u64(epoch.0, "caller shard epoch")?;
                    let mut permit = self.director.acquire(DbClass::Commit).await?;
                    let mut tx = permit.connection()?.begin().await?;
                    let row = sqlx::query_as::<_, (i64,)>(
                        "SELECT epoch FROM shard_lease WHERE shard_id = $1",
                    )
                    .bind(Self::shard_id_to_uuid(execution_home_bundle))
                    .fetch_optional(&mut *tx)
                    .await?;
                    let Some((durable_epoch,)) = row else {
                        tx.rollback().await?;
                        return Ok(CommitResult::Conflict {
                            reason: format!(
                                "no active lease for execution-home bundle {:?} at epoch {:?}",
                                execution_home_bundle, epoch
                            ),
                        });
                    };
                    if durable_epoch != convert::i64_from_u64(epoch.0, "caller shard epoch")? {
                        tx.rollback().await?;
                        return Ok(CommitResult::Conflict {
                            reason: format!(
                                "stale shard epoch {:?} for execution-home bundle {:?}; current {}",
                                epoch, execution_home_bundle, durable_epoch
                            ),
                        });
                    }
                    tx.rollback().await?;
                }

                metrics::increment_dsql_commits_in_flight();
                let result = self
                    .commit_transition(run_key, transition, ShardEpoch::ZERO)
                    .await;
                metrics::decrement_dsql_commits_in_flight();
                result
            }
        )
    }

    #[instrument(name = "dsql.materialize_reset_successor", skip(self), fields(base_run_key = %base_run_key.0, fork_event_id, successor_run_id = %successor_run_id.0))]
    async fn materialize_reset_successor(
        &self,
        base_run_key: RunKey,
        fork_event_id: i64,
        successor_run_id: RunId,
    ) -> Result<()> {
        record_dsql_operation!(
            self,
            "materialize_reset_successor",
            Some(self.shard_for_run_key(base_run_key)),
            {
                let mut permit = self.director.acquire(DbClass::Commit).await?;
                let mut tx = permit.connection()?.begin().await?;

                let base_row = sqlx::query_as::<_, (Vec<u8>,)>(
                    "SELECT state_data FROM workflow_hot WHERE run_key = $1",
                )
                .bind(base_run_key.0)
                .fetch_optional(&mut *tx)
                .await?;
                let Some((base_state_data,)) = base_row else {
                    tx.rollback().await?;
                    bail!("base run not found: {:?}", base_run_key);
                };
                let base_state = codec::decode_workflow_state(&base_state_data)?;
                let successor_run_key = RunKey::derive(
                    base_state.namespace_id,
                    &base_state.workflow_id,
                    successor_run_id,
                );

                let history_rows = sqlx::query_as::<_, (Vec<u8>, i64, i64)>(
                    "SELECT events_data, first_event_id, last_event_id FROM history_batch
             WHERE run_key = $1 ORDER BY first_event_id ASC",
                )
                .bind(base_run_key.0)
                .fetch_all(&mut *tx)
                .await?;
                let mut copied_events = Vec::new();
                let mut found_fork = false;
                // Reset materialization copies only the committed prefix through the
                // fork event. Replay then derives the successor state, avoiding a
                // second source of truth for reset snapshots.
                for (events_data, _first_event_id, _last_event_id) in history_rows {
                    for event in codec::decode_history_events(&events_data)? {
                        let is_fork = event.event_id == fork_event_id;
                        copied_events.push(event);
                        if is_fork {
                            found_fork = true;
                            break;
                        }
                    }
                    if found_fork {
                        break;
                    }
                }
                if !found_fork {
                    tx.rollback().await?;
                    bail!(
                        "fork_event_id {} outside committed history for {:?}",
                        fork_event_id,
                        base_run_key
                    );
                }

                let replay_ctx = ReplayContext {
                    run_key: successor_run_key,
                    namespace_id: base_state.namespace_id,
                    workflow_id: base_state.workflow_id.clone(),
                    run_id: successor_run_id,
                    deployment: base_state.deployment.clone(),
                    build_id: base_state.build_id.clone(),
                    parent_run_key: base_state.parent_run_key,
                    parent_workflow_id: base_state.parent_workflow_id.clone(),
                    first_run_started_at: base_state.first_run_started_at,
                };
                let successor_state = BasicKernel
                    .replay_history_prefix(replay_ctx, &copied_events)
                    .map_err(anyhow::Error::from)?;
                let successor_shard = self.shard_for_run_key(successor_run_key);
                insert_workflow_hot(
                    &mut tx,
                    successor_run_key,
                    successor_shard,
                    &successor_state,
                )
                .await?;
                insert_history_batch(
                    &mut tx,
                    successor_run_key,
                    successor_state.transition_seq,
                    &copied_events,
                )
                .await?;
                upsert_current_execution_start(&mut tx, successor_run_key, &successor_state)
                    .await?;
                for activity in successor_state.activities.values() {
                    upsert_activity(
                        &mut tx,
                        successor_run_key,
                        successor_shard,
                        successor_state.namespace_id,
                        activity,
                    )
                    .await?;
                }
                for timer in successor_state.timers.values() {
                    upsert_timer(&mut tx, successor_run_key, successor_shard, timer).await?;
                }
                tx.commit().await?;
                Ok(())
            }
        )
    }

    #[instrument(name = "dsql.list_dispatchable_workflow_tasks", skip(self), fields(namespace_id = %queue.namespace_id.0, task_queue = %queue.task_queue.0, limit))]
    async fn list_dispatchable_workflow_tasks(
        &self,
        queue: &QueueKey,
        limit: usize,
    ) -> Result<Vec<DispatchableWorkflowTask>> {
        record_dsql_operation!(self, "list_dispatchable_workflow_tasks", None, {
            if limit == 0 {
                metrics::record_dsql_rows_read("list_dispatchable_workflow_tasks", 0);
                return Ok(Vec::new());
            }

            let mut permit = self.director.acquire(DbClass::Read).await?;
            let rows = sqlx::query_as::<_, (Uuid, Vec<u8>)>(
                "SELECT run_key, state_data
             FROM workflow_hot
             WHERE namespace_id = $1",
            )
            .bind(queue.namespace_id.0)
            .fetch_all(permit.connection()?)
            .await?;
            metrics::record_dsql_rows_read("list_dispatchable_workflow_tasks", rows.len());

            collect_dispatchable_workflow_tasks(rows, Some(queue), limit)
        })
    }

    #[instrument(name = "dsql.list_dispatchable_activity_tasks", skip(self), fields(namespace_id = %queue.namespace_id.0, task_queue = %queue.task_queue.0, limit))]
    async fn list_dispatchable_activity_tasks(
        &self,
        queue: &QueueKey,
        limit: usize,
    ) -> Result<Vec<DispatchableActivityTask>> {
        record_dsql_operation!(self, "list_dispatchable_activity_tasks", None, {
            if limit == 0 {
                metrics::record_dsql_rows_read("list_dispatchable_activity_tasks", 0);
                return Ok(Vec::new());
            }

            let mut permit = self.director.acquire(DbClass::Read).await?;
            let deployment = queue.deployment.as_ref().map(|value| value.0.as_str());
            let build_id = queue.build_id.as_ref().map(|value| value.0.as_str());
            let rows = sqlx::query_as::<_, ActivityDispatchRow>(
                "SELECT run_key, activity_id, queue_namespace, queue_name, task_kind,
                    deployment, build_id, schedule_event_id, attempt, input_data
             FROM activity_dispatch
             WHERE queue_namespace = $1
               AND queue_name = $2
               AND task_kind = $3
               AND deployment IS NOT DISTINCT FROM $4
               AND build_id IS NOT DISTINCT FROM $5
             ORDER BY created_at ASC
             LIMIT $6",
            )
            .bind(queue.namespace_id.0)
            .bind(&queue.task_queue.0)
            .bind(queue.task_kind.to_db_smallint())
            .bind(deployment)
            .bind(build_id)
            .bind(i64::try_from(limit)?)
            .fetch_all(permit.connection()?)
            .await?;
            metrics::record_dsql_rows_read("list_dispatchable_activity_tasks", rows.len());

            rows.into_iter().map(activity_dispatch_from_row).collect()
        })
    }

    async fn persist_to_backlog(&self, entries: Vec<BacklogEntry>) -> Result<()> {
        record_dsql_operation!(self, "persist_to_backlog", None, {
            if entries.is_empty() {
                metrics::record_dsql_rows_written("persist_to_backlog", 0);
                return Ok(());
            }

            let row_count = entries.len() as u64;
            let mut permit = self.director.acquire(DbClass::Commit).await?;
            let mut tx = permit.connection()?.begin().await?;
            for entry in entries {
                let partition_id = partition_for(entry.run_key, self.projection_partition_count);
                let deployment = entry
                    .queue
                    .deployment
                    .as_ref()
                    .map(|value| value.0.as_str());
                let build_id = entry.queue.build_id.as_ref().map(|value| value.0.as_str());
                let key = Self::dispatch_backlog_key(
                    partition_id,
                    entry.queue.namespace_id,
                    &entry.queue.task_queue.0,
                    entry.queue.task_kind,
                    deployment,
                    build_id,
                    entry.insertion_seq,
                );
                sqlx::query(
                "INSERT INTO dispatch_backlog
                 (key, partition_id, queue_namespace, queue_name, task_kind, deployment, build_id, insertion_seq, run_key, payload_data, scheduled_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
            )
            .bind(key)
            .bind(i32::try_from(partition_id)?)
            .bind(entry.queue.namespace_id.0)
            .bind(&entry.queue.task_queue.0)
            .bind(entry.queue.task_kind.to_db_smallint())
            .bind(deployment)
            .bind(build_id)
            .bind(convert::i64_from_u64(entry.insertion_seq, "dispatch_backlog.insertion_seq")?)
            .bind(entry.run_key.0)
            .bind(codec::encode_backlog_payload(&entry.payload)?)
            .bind(entry.scheduled_at)
            .execute(&mut *tx)
            .await?;
            }
            tx.commit().await?;
            metrics::record_dsql_rows_written("persist_to_backlog", row_count);
            Ok(())
        })
    }

    async fn drain_backlog(&self, queue: &QueueKey, limit: usize) -> Result<Vec<BacklogEntry>> {
        record_dsql_operation!(self, "drain_backlog", None, {
            if limit == 0 {
                metrics::record_dsql_rows_read("drain_backlog", 0);
                metrics::record_dsql_rows_written("drain_backlog", 0);
                return Ok(Vec::new());
            }

            let mut permit = self.director.acquire(DbClass::Commit).await?;
            let mut tx = permit.connection()?.begin().await?;
            let deployment = queue.deployment.as_ref().map(|value| value.0.as_str());
            let build_id = queue.build_id.as_ref().map(|value| value.0.as_str());
            let rows = sqlx::query_as::<_, (Uuid, Uuid, Vec<u8>, OffsetDateTime, i64, i16, Option<String>, Option<String>)>(
            "SELECT key, run_key, payload_data, scheduled_at, insertion_seq, task_kind, deployment, build_id
             FROM dispatch_backlog
             WHERE queue_namespace = $1
               AND queue_name = $2
               AND task_kind = $3
               AND deployment IS NOT DISTINCT FROM $4
               AND build_id IS NOT DISTINCT FROM $5
             ORDER BY insertion_seq ASC
             LIMIT $6",
        )
        .bind(queue.namespace_id.0)
        .bind(&queue.task_queue.0)
        .bind(queue.task_kind.to_db_smallint())
        .bind(deployment)
        .bind(build_id)
        .bind(i64::try_from(limit)?)
        .fetch_all(&mut *tx)
        .await?;
            metrics::record_dsql_rows_read("drain_backlog", rows.len());

            let mut drained = Vec::with_capacity(rows.len());
            for (
                key,
                run_key,
                payload_data,
                scheduled_at,
                insertion_seq,
                task_kind_raw,
                stored_deployment,
                stored_build_id,
            ) in rows
            {
                sqlx::query("DELETE FROM dispatch_backlog WHERE key = $1")
                    .bind(key)
                    .execute(&mut *tx)
                    .await?;
                drained.push(BacklogEntry {
                    run_key: RunKey(run_key),
                    queue: QueueKey {
                        namespace_id: queue.namespace_id,
                        task_queue: queue.task_queue.clone(),
                        task_kind: TaskKind::try_from(task_kind_raw)?,
                        deployment: stored_deployment.map(DeploymentId),
                        build_id: stored_build_id.map(BuildId),
                    },
                    payload: codec::decode_backlog_payload(&payload_data)?,
                    scheduled_at,
                    insertion_seq: convert::u64_from_i64(
                        insertion_seq,
                        "dispatch_backlog.insertion_seq",
                    )?,
                });
            }
            tx.commit().await?;
            metrics::record_dsql_rows_written("drain_backlog", drained.len() as u64);
            Ok(drained)
        })
    }

    #[instrument(name = "dsql.list_due_timers", skip(self), fields(limit))]
    async fn list_due_timers(&self, now: OffsetDateTime, limit: usize) -> Result<Vec<DueTimer>> {
        record_dsql_operation!(self, "list_due_timers", None, {
            if limit == 0 {
                metrics::record_dsql_rows_read("list_due_timers", 0);
                return Ok(Vec::new());
            }

            let mut due = Vec::new();
            for shard_index in 0..self.shard_count {
                let remaining = limit - due.len();
                if remaining == 0 {
                    break;
                }
                due.extend(
                    self.list_due_timers_for_shard(ShardId(shard_index), now, remaining)
                        .await?,
                );
            }
            due.truncate(limit);
            metrics::record_dsql_rows_read("list_due_timers", due.len());
            Ok(due)
        })
    }

    #[instrument(name = "dsql.list_dispatchable_workflow_tasks_for_shard", skip(self), fields(shard_id = shard_id.0, limit))]
    async fn list_dispatchable_workflow_tasks_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<DispatchableWorkflowTask>> {
        record_dsql_operation!(
            self,
            "list_dispatchable_workflow_tasks_for_shard",
            Some(shard_id),
            {
                if limit == 0 {
                    metrics::record_dsql_rows_read("list_dispatchable_workflow_tasks_for_shard", 0);
                    return Ok(Vec::new());
                }

                let mut permit = self.director.acquire(DbClass::Read).await?;
                let rows = sqlx::query_as::<_, (Uuid, Vec<u8>)>(
                    "SELECT run_key, state_data
             FROM workflow_hot
             WHERE shard_id = $1",
                )
                .bind(Self::shard_id_to_uuid(shard_id))
                .fetch_all(permit.connection()?)
                .await?;
                metrics::record_dsql_rows_read(
                    "list_dispatchable_workflow_tasks_for_shard",
                    rows.len(),
                );

                collect_dispatchable_workflow_tasks(rows, None, limit)
            }
        )
    }

    #[instrument(name = "dsql.list_dispatchable_activity_tasks_for_shard", skip(self), fields(shard_id = shard_id.0, limit))]
    async fn list_dispatchable_activity_tasks_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<DispatchableActivityTask>> {
        record_dsql_operation!(
            self,
            "list_dispatchable_activity_tasks_for_shard",
            Some(shard_id),
            {
                if limit == 0 {
                    metrics::record_dsql_rows_read("list_dispatchable_activity_tasks_for_shard", 0);
                    return Ok(Vec::new());
                }

                let mut permit = self.director.acquire(DbClass::Read).await?;
                let rows = sqlx::query_as::<_, ActivityDispatchRow>(
                    "SELECT run_key, activity_id, queue_namespace, queue_name, task_kind,
                    deployment, build_id, schedule_event_id, attempt, input_data
             FROM activity_dispatch
             WHERE shard_id = $1
             ORDER BY created_at ASC
             LIMIT $2",
                )
                .bind(Self::shard_id_to_uuid(shard_id))
                .bind(i64::try_from(limit)?)
                .fetch_all(permit.connection()?)
                .await?;
                metrics::record_dsql_rows_read(
                    "list_dispatchable_activity_tasks_for_shard",
                    rows.len(),
                );

                rows.into_iter().map(activity_dispatch_from_row).collect()
            }
        )
    }

    #[instrument(name = "dsql.list_due_timers_for_shard", skip(self), fields(shard_id = shard_id.0, limit))]
    async fn list_due_timers_for_shard(
        &self,
        shard_id: ShardId,
        now: OffsetDateTime,
        limit: usize,
    ) -> Result<Vec<DueTimer>> {
        record_dsql_operation!(self, "list_due_timers_for_shard", Some(shard_id), {
            if limit == 0 {
                metrics::record_dsql_rows_read("list_due_timers_for_shard", 0);
                return Ok(Vec::new());
            }

            let mut permit = self.director.acquire(DbClass::Read).await?;
            let rows = sqlx::query_as::<_, (Uuid, String)>(
                "SELECT run_key, timer_id
             FROM timer_bucket
             WHERE shard_id = $1 AND fire_at <= $2
             ORDER BY fire_at ASC
             LIMIT $3",
            )
            .bind(Self::shard_id_to_uuid(shard_id))
            .bind(now)
            .bind(i64::try_from(limit)?)
            .fetch_all(permit.connection()?)
            .await?;
            metrics::record_dsql_rows_read("list_due_timers_for_shard", rows.len());

            Ok(rows
                .into_iter()
                .map(|(run_key, timer_id)| DueTimer {
                    run_key: RunKey(run_key),
                    timer_id,
                })
                .collect())
        })
    }

    #[instrument(name = "dsql.list_runs_with_workflow_timeouts_for_shard", skip(self), fields(shard_id = shard_id.0, limit))]
    async fn list_runs_with_workflow_timeouts_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<WorkflowTimeoutSweepEntry>> {
        record_dsql_operation!(
            self,
            "list_runs_with_workflow_timeouts_for_shard",
            Some(shard_id),
            {
                if limit == 0 {
                    metrics::record_dsql_rows_read("list_runs_with_workflow_timeouts_for_shard", 0);
                    return Ok(Vec::new());
                }

                let mut permit = self.director.acquire(DbClass::Read).await?;
                let rows = sqlx::query_as::<_, (Uuid, Vec<u8>)>(
                    "SELECT run_key, state_data
             FROM workflow_hot
             WHERE shard_id = $1",
                )
                .bind(Self::shard_id_to_uuid(shard_id))
                .fetch_all(permit.connection()?)
                .await?;
                metrics::record_dsql_rows_read(
                    "list_runs_with_workflow_timeouts_for_shard",
                    rows.len(),
                );

                collect_workflow_timeout_entries(rows, limit)
            }
        )
    }

    #[instrument(name = "dsql.list_started_workflow_tasks_for_shard", skip(self), fields(shard_id = shard_id.0, limit))]
    async fn list_started_workflow_tasks_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<WftTimeoutSweepEntry>> {
        record_dsql_operation!(
            self,
            "list_started_workflow_tasks_for_shard",
            Some(shard_id),
            {
                if limit == 0 {
                    metrics::record_dsql_rows_read("list_started_workflow_tasks_for_shard", 0);
                    return Ok(Vec::new());
                }

                let mut permit = self.director.acquire(DbClass::Read).await?;
                let rows = sqlx::query_as::<_, (Uuid, Vec<u8>)>(
                    "SELECT run_key, state_data
             FROM workflow_hot
             WHERE shard_id = $1",
                )
                .bind(Self::shard_id_to_uuid(shard_id))
                .fetch_all(permit.connection()?)
                .await?;
                metrics::record_dsql_rows_read("list_started_workflow_tasks_for_shard", rows.len());

                collect_started_workflow_task_entries(rows, limit)
            }
        )
    }

    #[instrument(name = "dsql.list_open_activities_for_shard", skip(self), fields(shard_id = shard_id.0, limit))]
    async fn list_open_activities_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<ActivitySweepEntry>> {
        record_dsql_operation!(self, "list_open_activities_for_shard", Some(shard_id), {
            if limit == 0 {
                metrics::record_dsql_rows_read("list_open_activities_for_shard", 0);
                return Ok(Vec::new());
            }

            let mut permit = self.director.acquire(DbClass::Read).await?;
            let rows = sqlx::query_as::<_, (Uuid, Vec<u8>)>(
                "SELECT run_key, state_data
             FROM activity_state
             WHERE shard_id = $1
             LIMIT $2",
            )
            .bind(Self::shard_id_to_uuid(shard_id))
            .bind(i64::try_from(limit)?)
            .fetch_all(permit.connection()?)
            .await?;
            metrics::record_dsql_rows_read("list_open_activities_for_shard", rows.len());

            collect_activity_sweep_entries(rows)
        })
    }

    #[instrument(name = "dsql.list_pending_nexus_operations_for_shard", skip(self), fields(shard_id = shard_id.0, limit))]
    async fn list_pending_nexus_operations_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<NexusSweepEntry>> {
        record_dsql_operation!(
            self,
            "list_pending_nexus_operations_for_shard",
            Some(shard_id),
            {
                if limit == 0 {
                    metrics::record_dsql_rows_read("list_pending_nexus_operations_for_shard", 0);
                    return Ok(Vec::new());
                }

                let mut permit = self.director.acquire(DbClass::Read).await?;
                let rows = sqlx::query_as::<_, (Uuid, Vec<u8>)>(
                    "SELECT run_key, state_data
             FROM workflow_hot
             WHERE shard_id = $1",
                )
                .bind(Self::shard_id_to_uuid(shard_id))
                .fetch_all(permit.connection()?)
                .await?;
                metrics::record_dsql_rows_read(
                    "list_pending_nexus_operations_for_shard",
                    rows.len(),
                );

                collect_nexus_sweep_entries(rows, limit)
            }
        )
    }
}

#[async_trait]
impl LeaseRepository for DsqlRunRepository {
    #[instrument(name = "dsql.try_acquire_bundle", skip(self), fields(shard_id = bundle.0, owner = %owner))]
    async fn try_acquire_bundle(
        &self,
        bundle: ShardId,
        owner: String,
        node_endpoint: String,
    ) -> Result<LeaseOutcome> {
        record_dsql_operation!(self, "try_acquire_bundle", Some(bundle), {
            let shard_uuid = Self::shard_id_to_uuid(bundle);
            let app_now = OffsetDateTime::now_utc();
            let new_expiry = app_now + self.lease_duration;

            let mut permit = self.director.acquire(DbClass::Control).await?;
            let mut tx = permit.connection()?.begin().await?;

            let insert_result = sqlx::query(
                "INSERT INTO shard_lease (shard_id, owner, epoch, lease_expiry, node_endpoint)
             VALUES ($1, $2, 1, $3, $4)
             ON CONFLICT (shard_id) DO NOTHING",
            )
            .bind(shard_uuid)
            .bind(&owner)
            .bind(new_expiry)
            .bind(&node_endpoint)
            .execute(&mut *tx)
            .await;

            let insert_rows_affected = match insert_result {
                Ok(result) => result.rows_affected(),
                Err(err) if Self::is_serialization_failure(&err) => {
                    tx.rollback().await?;
                    return Err(anyhow!(err))
                        .context("DSQL serialization conflict during lease acquire");
                }
                Err(err) => {
                    tx.rollback().await?;
                    return Err(err).context("failed to insert shard lease");
                }
            };

            let mut update_rows_affected = 0;
            if insert_rows_affected == 0 {
                let update_result = sqlx::query(
                    "UPDATE shard_lease
                 SET owner = $2,
                     epoch = CASE
                         WHEN owner = $2 AND lease_expiry > $4 THEN epoch
                         ELSE epoch + 1
                     END,
                     lease_expiry = $3,
                     node_endpoint = $5
                 WHERE shard_id = $1
                   AND (owner = $2 OR owner IS NULL OR lease_expiry <= $4)",
                )
                .bind(shard_uuid)
                .bind(&owner)
                .bind(new_expiry)
                .bind(app_now)
                .bind(&node_endpoint)
                .execute(&mut *tx)
                .await;

                update_rows_affected = match update_result {
                    Ok(result) => result.rows_affected(),
                    Err(err) if Self::is_serialization_failure(&err) => {
                        tx.rollback().await?;
                        return Err(anyhow!(err))
                            .context("DSQL serialization conflict during lease acquire");
                    }
                    Err(err) => {
                        tx.rollback().await?;
                        return Err(err).context("failed to update shard lease");
                    }
                };
            }

            let outcome = if insert_rows_affected == 1 || update_rows_affected == 1 {
                let (epoch,) = sqlx::query_as::<_, (i64,)>(
                    "SELECT epoch FROM shard_lease WHERE shard_id = $1",
                )
                .bind(shard_uuid)
                .fetch_one(&mut *tx)
                .await
                .context("failed to read acquired shard lease epoch")?;
                interpret_acquire(
                    insert_rows_affected,
                    update_rows_affected,
                    Some(epoch),
                    None,
                )?
            } else {
                let row = sqlx::query_as::<_, (Option<String>, i64)>(
                    "SELECT owner, epoch FROM shard_lease WHERE shard_id = $1",
                )
                .bind(shard_uuid)
                .fetch_optional(&mut *tx)
                .await
                .context("failed to read rejected shard lease holder")?;
                let rejected_row = row.map(|(owner, epoch)| (owner.unwrap_or_default(), epoch));
                interpret_acquire(
                    insert_rows_affected,
                    update_rows_affected,
                    None,
                    rejected_row,
                )?
            };

            match outcome {
                LeaseOutcome::Acquired { .. } => match tx.commit().await {
                    Ok(()) => Ok(outcome),
                    Err(err) if Self::is_serialization_failure(&err) => Err(anyhow!(err))
                        .context("DSQL serialization conflict during lease acquire"),
                    Err(err) => Err(err).context("failed to commit shard lease acquire"),
                },
                LeaseOutcome::Rejected { .. } => {
                    tx.rollback().await?;
                    Ok(outcome)
                }
                LeaseOutcome::Renewed { .. } => {
                    tx.rollback().await?;
                    bail!("acquire interpretation unexpectedly returned renewed outcome");
                }
            }
        })
    }

    #[instrument(name = "dsql.renew_bundle", skip(self), fields(shard_id = bundle.0, owner = %owner, epoch = epoch.0))]
    async fn renew_bundle(
        &self,
        bundle: ShardId,
        owner: String,
        epoch: ShardEpoch,
        node_endpoint: String,
    ) -> Result<LeaseOutcome> {
        record_dsql_operation!(self, "renew_bundle", Some(bundle), {
            let caller_epoch = epoch_to_sql(epoch)?;
            let shard_uuid = Self::shard_id_to_uuid(bundle);
            let new_expiry = OffsetDateTime::now_utc() + self.lease_duration;

            let mut permit = self.director.acquire(DbClass::Control).await?;
            let mut tx = permit.connection()?.begin().await?;

            let row = sqlx::query_as::<_, (Option<String>, i64)>(
                "SELECT owner, epoch
             FROM shard_lease
             WHERE shard_id = $1
             FOR UPDATE",
            )
            .bind(shard_uuid)
            .fetch_optional(&mut *tx)
            .await
            .context("failed to read shard lease for renewal")?;

            let decision = decide_renew(
                row.as_ref()
                    .map(|(owner, epoch)| (owner.as_deref().unwrap_or(""), *epoch)),
                &owner,
                caller_epoch,
            )?;
            match decision {
                RenewDecision::Renew => {
                    let update_result = sqlx::query(
                        "UPDATE shard_lease
                     SET lease_expiry = $1,
                         node_endpoint = $3
                     WHERE shard_id = $2",
                    )
                    .bind(new_expiry)
                    .bind(shard_uuid)
                    .bind(&node_endpoint)
                    .execute(&mut *tx)
                    .await;

                    match update_result {
                        Ok(_) => match tx.commit().await {
                            Ok(()) => Ok(LeaseOutcome::Renewed { epoch }),
                            Err(err) if Self::is_serialization_failure(&err) => Err(anyhow!(err))
                                .context("DSQL serialization conflict during lease renewal"),
                            Err(err) => Err(err).context("failed to commit shard lease renewal"),
                        },
                        Err(err) if Self::is_serialization_failure(&err) => {
                            tx.rollback().await?;
                            Err(anyhow!(err))
                                .context("DSQL serialization conflict during lease renewal")
                        }
                        Err(err) => {
                            tx.rollback().await?;
                            Err(err).context("failed to update shard lease renewal")
                        }
                    }
                }
                RenewDecision::Reject {
                    current_owner,
                    current_epoch,
                } => {
                    tx.rollback().await?;
                    Ok(LeaseOutcome::Rejected {
                        current_owner,
                        current_epoch,
                    })
                }
            }
        })
    }

    #[instrument(name = "dsql.list_bundle_leases", skip(self))]
    async fn list_bundle_leases(&self) -> Result<Vec<BundleLease>> {
        record_dsql_operation!(self, "list_bundle_leases", None, {
            let mut permit = self.director.acquire(DbClass::Control).await?;
            let rows =
                sqlx::query_as::<_, (Uuid, Option<String>, i64, OffsetDateTime, Option<String>)>(
                    "SELECT shard_id, owner, epoch, lease_expiry, node_endpoint FROM shard_lease",
                )
                .fetch_all(permit.connection()?)
                .await
                .context("failed to list shard leases")?;

            let mut leases = Vec::with_capacity(rows.len());
            for (shard_uuid, owner, epoch, lease_until, endpoint) in rows {
                let bundle_id = match Self::shard_id_from_uuid(shard_uuid) {
                    Ok(id) => id,
                    Err(_) => {
                        // Row written by a previous code version with a different
                        // UUID encoding. Skip it — try_acquire_bundle will overwrite
                        // it via the ON CONFLICT + expiry-based UPDATE path.
                        tracing::debug!(
                            %shard_uuid,
                            "skipping shard_lease row with unrecognized UUID encoding"
                        );
                        continue;
                    }
                };
                leases.push(BundleLease {
                    bundle_id,
                    owner_node_id: owner,
                    epoch: epoch_from_sql(epoch)?,
                    lease_until,
                    node_endpoint: endpoint,
                });
            }
            Ok(leases)
        })
    }

    #[instrument(name = "dsql.relinquish_bundle", skip(self), fields(shard_id = bundle.0, owner = %owner, epoch = epoch.0))]
    async fn relinquish_bundle(
        &self,
        bundle: ShardId,
        owner: String,
        epoch: ShardEpoch,
    ) -> Result<LeaseOutcome> {
        record_dsql_operation!(self, "relinquish_bundle", Some(bundle), {
            let shard_uuid = Self::shard_id_to_uuid(bundle);
            let caller_epoch = epoch_to_sql(epoch)?;
            let mut permit = self.director.acquire(DbClass::Control).await?;
            let mut tx = permit.connection()?.begin().await?;
            let row = sqlx::query_as::<_, (i64,)>(
                "UPDATE shard_lease
             SET owner = NULL,
                 epoch = epoch + 1,
                 node_endpoint = NULL
             WHERE shard_id = $1 AND owner = $2 AND epoch = $3
             RETURNING epoch",
            )
            .bind(shard_uuid)
            .bind(&owner)
            .bind(caller_epoch)
            .fetch_optional(&mut *tx)
            .await
            .context("failed to relinquish shard lease")?;

            if let Some((new_epoch,)) = row {
                tx.commit().await?;
                return Ok(LeaseOutcome::Acquired {
                    epoch: epoch_from_sql(new_epoch)?,
                });
            }

            let current = sqlx::query_as::<_, (Option<String>, i64)>(
                "SELECT owner, epoch FROM shard_lease WHERE shard_id = $1",
            )
            .bind(shard_uuid)
            .fetch_optional(&mut *tx)
            .await
            .context("failed to read rejected shard lease holder after relinquish")?;
            tx.rollback().await?;
            let (current_owner, current_epoch) = current.unwrap_or((None, 0));
            Ok(LeaseOutcome::Rejected {
                current_owner: current_owner.unwrap_or_default(),
                current_epoch: epoch_from_sql(current_epoch)?,
            })
        })
    }
}

#[async_trait]
impl ControlRepository for DsqlRunRepository {
    #[instrument(name = "dsql.advance_generation", skip(self), fields(expected = expected.0))]
    async fn advance_generation(
        &self,
        expected: GenerationCounter,
    ) -> Result<GenerationAdvanceResult> {
        record_dsql_operation!(self, "advance_generation", None, {
            let expected = convert::i64_from_u64(expected.0, "routing_generation.generation")?;
            let mut permit = self.director.acquire(DbClass::Control).await?;
            let row = sqlx::query_as::<_, (i64,)>(
                "UPDATE routing_generation
             SET generation = generation + 1,
                 updated_at = now()
             WHERE id = 1 AND generation = $1
             RETURNING generation",
            )
            .bind(expected)
            .fetch_optional(permit.connection()?)
            .await
            .context("failed to advance routing generation")?;

            match row {
                Some((generation,)) => Ok(GenerationAdvanceResult::Advanced(GenerationCounter(
                    convert::u64_from_i64(generation, "routing_generation.generation")?,
                ))),
                None => Ok(GenerationAdvanceResult::Conflict(
                    self.current_generation().await?,
                )),
            }
        })
    }

    #[instrument(name = "dsql.current_generation", skip(self))]
    async fn current_generation(&self) -> Result<GenerationCounter> {
        record_dsql_operation!(self, "current_generation", None, {
            let mut permit = self.director.acquire(DbClass::Control).await?;
            let (generation,) = sqlx::query_as::<_, (i64,)>(
                "SELECT generation FROM routing_generation WHERE id = 1",
            )
            .fetch_one(permit.connection()?)
            .await
            .context("failed to read routing generation")?;
            Ok(GenerationCounter(convert::u64_from_i64(
                generation,
                "routing_generation.generation",
            )?))
        })
    }

    #[instrument(name = "dsql.allocate_budget", skip(self), fields(expected_version, allocator_id = %allocator_id))]
    async fn allocate_budget(
        &self,
        expected_version: u64,
        allocator_id: Uuid,
        rate_budget: f64,
        capacity_budget: u64,
    ) -> Result<BudgetAllocationResult> {
        record_dsql_operation!(self, "allocate_budget", None, {
            let expected_version =
                convert::i64_from_u64(expected_version, "budget_allocation.version")?;
            let capacity_budget =
                convert::i64_from_u64(capacity_budget, "budget_allocation.capacity_budget")?;
            let mut permit = self.director.acquire(DbClass::Control).await?;
            let row = sqlx::query_as::<_, (i64,)>(
                "UPDATE budget_allocation
             SET version = version + 1,
                 allocator_id = $2,
                 allocated_at = now(),
                 rate_budget = $3,
                 capacity_budget = $4
             WHERE id = 1 AND version = $1
             RETURNING version",
            )
            .bind(expected_version)
            .bind(allocator_id)
            .bind(rate_budget)
            .bind(capacity_budget)
            .fetch_optional(permit.connection()?)
            .await
            .context("failed to allocate connection budget")?;

            match row {
                Some((version,)) => Ok(BudgetAllocationResult::Allocated {
                    version: convert::u64_from_i64(version, "budget_allocation.version")?,
                }),
                None => Ok(BudgetAllocationResult::Conflict {
                    current_version: self.current_budget_version().await?,
                }),
            }
        })
    }

    #[instrument(name = "dsql.current_budget_version", skip(self))]
    async fn current_budget_version(&self) -> Result<u64> {
        record_dsql_operation!(self, "current_budget_version", None, {
            let mut permit = self.director.acquire(DbClass::Control).await?;
            let (version,) =
                sqlx::query_as::<_, (i64,)>("SELECT version FROM budget_allocation WHERE id = 1")
                    .fetch_one(permit.connection()?)
                    .await
                    .context("failed to read budget allocation version")?;
            convert::u64_from_i64(version, "budget_allocation.version")
        })
    }
}

type ActivityDispatchRow = (
    Uuid,
    String,
    Uuid,
    String,
    i16,
    Option<String>,
    Option<String>,
    i64,
    i32,
    Vec<u8>,
);

fn collect_dispatchable_workflow_tasks(
    rows: Vec<(Uuid, Vec<u8>)>,
    queue_filter: Option<&QueueKey>,
    limit: usize,
) -> Result<Vec<DispatchableWorkflowTask>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let now = OffsetDateTime::now_utc();
    let mut tasks = Vec::new();
    for (run_key, state_data) in rows {
        // Workflow task dispatch is derived from the hot state snapshot. There
        // is no separate workflow-task queue table to repair; replaying history
        // can rebuild this materialization.
        let state = codec::decode_workflow_state(&state_data)?;
        if state.status != ExecutionStatus::Running {
            continue;
        }
        let Some(task) = state.pending_workflow_task.as_ref() else {
            continue;
        };
        if task.started_event_id.is_some() {
            continue;
        }
        let queue = QueueKey {
            namespace_id: state.namespace_id,
            task_queue: state.task_queue.clone(),
            task_kind: TaskKind::Workflow,
            deployment: state.deployment.clone(),
            build_id: state.build_id.clone(),
        };
        if queue_filter.is_some_and(|filter| filter != &queue) {
            continue;
        }
        let (sticky_preferred, sticky_expires_at) = sticky_fields(&state, now);
        tasks.push(DispatchableWorkflowTask {
            run_key: RunKey(run_key),
            queue,
            logical_seq: task.logical_seq,
            sticky_preferred,
            sticky_expires_at,
        });
        if tasks.len() == limit {
            break;
        }
    }
    Ok(tasks)
}

fn sticky_fields(
    state: &WorkflowState,
    now: OffsetDateTime,
) -> (Option<WorkerIdentity>, Option<OffsetDateTime>) {
    let Some(sticky) = &state.sticky else {
        return (None, None);
    };
    if sticky.expires_at <= now {
        return (None, None);
    }
    (
        Some(sticky.worker_identity.clone()),
        Some(sticky.expires_at),
    )
}

fn activity_dispatch_from_row(row: ActivityDispatchRow) -> Result<DispatchableActivityTask> {
    let (
        run_key,
        activity_id,
        queue_namespace,
        queue_name,
        task_kind,
        deployment,
        build_id,
        schedule_event_id,
        attempt,
        input_data,
    ) = row;
    Ok(DispatchableActivityTask {
        run_key: RunKey(run_key),
        queue: QueueKey {
            namespace_id: NamespaceId(queue_namespace),
            task_queue: TaskQueueName(queue_name),
            task_kind: TaskKind::try_from(task_kind)?,
            deployment: deployment.map(DeploymentId),
            build_id: build_id.map(BuildId),
        },
        activity_id,
        input: codec::decode_payloads(&input_data)?,
        schedule_event_id,
        attempt: convert::u32_from_i32(attempt, "activity_dispatch.attempt")?,
    })
}

fn collect_workflow_timeout_entries(
    rows: Vec<(Uuid, Vec<u8>)>,
    limit: usize,
) -> Result<Vec<WorkflowTimeoutSweepEntry>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let mut entries = Vec::new();
    for (run_key, state_data) in rows {
        // Timeout scanners need enough state to evaluate both execution and
        // run timeout policies in runtime without reopening history.
        let state = codec::decode_workflow_state(&state_data)?;
        if !state.status.is_open()
            || (state.workflow_execution_timeout.is_none() && state.workflow_run_timeout.is_none())
        {
            continue;
        }
        entries.push(WorkflowTimeoutSweepEntry {
            run_key: RunKey(run_key),
            workflow_execution_timeout: state.workflow_execution_timeout,
            workflow_run_timeout: state.workflow_run_timeout,
            started_at: state.started_at,
            first_run_started_at: state.first_run_started_at,
            has_retry_policy: state.retry_policy.is_some(),
        });
        if entries.len() == limit {
            break;
        }
    }
    Ok(entries)
}

fn collect_started_workflow_task_entries(
    rows: Vec<(Uuid, Vec<u8>)>,
    limit: usize,
) -> Result<Vec<WftTimeoutSweepEntry>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let mut entries = Vec::new();
    for (run_key, state_data) in rows {
        let state = codec::decode_workflow_state(&state_data)?;
        let Some(task) = state.pending_workflow_task else {
            continue;
        };
        let (Some(started_event_id), Some(started_at)) = (task.started_event_id, task.started_at)
        else {
            continue;
        };
        entries.push(WftTimeoutSweepEntry {
            run_key: RunKey(run_key),
            logical_seq: task.logical_seq,
            started_event_id,
            started_at,
            workflow_task_timeout: state.workflow_task_timeout,
        });
        if entries.len() == limit {
            break;
        }
    }
    Ok(entries)
}

fn collect_activity_sweep_entries(rows: Vec<(Uuid, Vec<u8>)>) -> Result<Vec<ActivitySweepEntry>> {
    rows.into_iter()
        .map(|(run_key, state_data)| {
            let activity = codec::decode_activity_state(&state_data)?;
            Ok(ActivitySweepEntry {
                run_key: RunKey(run_key),
                activity_id: activity.activity_id,
                schedule_event_id: activity.schedule_event_id,
                attempt: activity.attempt,
                original_scheduled_at: activity.scheduled_at,
                started_at: activity.started_at,
                schedule_to_close_timeout: activity.schedule_to_close_timeout,
                schedule_to_start_timeout: activity.schedule_to_start_timeout,
                start_to_close_timeout: activity.start_to_close_timeout,
                heartbeat_timeout: activity.heartbeat_timeout,
            })
        })
        .collect()
}

fn collect_nexus_sweep_entries(
    rows: Vec<(Uuid, Vec<u8>)>,
    limit: usize,
) -> Result<Vec<NexusSweepEntry>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let mut entries = Vec::new();
    for (run_key, state_data) in rows {
        // Nexus timeout tracking currently lives in the workflow snapshot so
        // this scan filters in Rust after shard-local row selection.
        let state = codec::decode_workflow_state(&state_data)?;
        if !state.status.is_open() {
            continue;
        }
        for operation in state.pending_nexus_operations.values() {
            let Some(schedule_to_close_timeout) = operation.schedule_to_close_timeout else {
                continue;
            };
            entries.push(NexusSweepEntry {
                run_key: RunKey(run_key),
                operation_id: operation.operation_id.clone(),
                scheduled_event_id: operation.scheduled_event_id,
                schedule_to_close_timeout,
                scheduled_at: operation.scheduled_at,
            });
            if entries.len() == limit {
                return Ok(entries);
            }
        }
    }
    Ok(entries)
}

async fn write_transition(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_key: RunKey,
    shard_id: ShardId,
    projection_partition_count: u32,
    transition: &Transition,
    state: &WorkflowState,
) -> Result<()> {
    // The commit path intentionally writes the hot state first, then derives
    // every side table from the same transition/state pair. History remains the
    // authority; side tables are rebuildable projections that make dispatch and
    // sweep queries efficient.
    insert_workflow_hot(tx, run_key, shard_id, state).await?;
    if !transition.history_events.is_empty() {
        insert_history_batch(
            tx,
            run_key,
            state.transition_seq,
            transition.history_events.as_slice(),
        )
        .await?;
    }
    for op in &transition.request_dedupe_ops {
        let key = DsqlRunRepository::request_dedupe_key(
            state.namespace_id,
            &state.workflow_id,
            &op.request_id,
        );
        sqlx::query(
            "INSERT INTO request_dedupe
             (key, namespace_id, workflow_id, request_id, run_key, run_id, first_seen_transition_seq, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, now())",
        )
        .bind(key)
        .bind(state.namespace_id.0)
        .bind(&state.workflow_id.0)
        .bind(&op.request_id.0)
        .bind(run_key.0)
        .bind(state.run_id.0)
        .bind(convert::i64_from_u64(state.transition_seq.0, "transition_seq")?)
        .execute(&mut **tx)
        .await?;
    }
    for op in &transition.activity_ops {
        match op {
            ActivityOp::Upsert(activity) => {
                upsert_activity(tx, run_key, shard_id, state.namespace_id, activity).await?;
                // `activity_dispatch` is the durable dispatch source, not
                // `activity_state`. Started or paused activities must disappear
                // from dispatch immediately; still-dispatchable upserts only
                // update an existing row so a paused workflow cannot create a
                // dispatch row by changing activity options.
                if activity.started_at.is_some() || activity.pause_info.is_some() {
                    delete_activity_dispatch(tx, run_key, &activity.activity_id).await?;
                } else {
                    update_existing_activity_dispatch(tx, run_key, shard_id, state, activity)
                        .await?;
                }
            }
            ActivityOp::Delete { activity_id } => {
                sqlx::query("DELETE FROM activity_state WHERE run_key = $1 AND activity_id = $2")
                    .bind(run_key.0)
                    .bind(activity_id)
                    .execute(&mut **tx)
                    .await?;
                delete_activity_dispatch(tx, run_key, activity_id).await?;
            }
        }
    }
    for op in &transition.dispatch_ops {
        if let DispatchOp::EnqueueActivityTask {
            queue,
            activity_id,
            input,
            schedule_event_id,
            attempt,
            ..
        } = op
        {
            // Enqueue is the only path that creates a dispatch row. Re-enqueue
            // after retry/reset/unpause is idempotent via ON CONFLICT.
            upsert_activity_dispatch_from_dispatch_op(
                tx,
                run_key,
                shard_id,
                queue,
                activity_id,
                input,
                *schedule_event_id,
                *attempt,
            )
            .await?;
        }
    }
    if state.status == ExecutionStatus::Paused {
        // Workflow pause suppresses all activity dispatch for the run. The
        // state table still carries activities for later unpause/retry logic.
        delete_activity_dispatch_for_run(tx, run_key).await?;
    }
    for op in &transition.timer_ops {
        match op {
            TimerOp::Upsert(timer) => upsert_timer(tx, run_key, shard_id, timer).await?,
            TimerOp::Delete { timer_id } => {
                sqlx::query("DELETE FROM timer_bucket WHERE run_key = $1 AND timer_id = $2")
                    .bind(run_key.0)
                    .bind(timer_id)
                    .execute(&mut **tx)
                    .await?;
            }
        }
    }
    if transition.expected_seq == TransitionSeq::ZERO && state.status.is_open() {
        // Only start transitions publish a new current-execution open pointer.
        upsert_current_execution_start(tx, run_key, state).await?;
    } else if !state.status.is_open() {
        let key = DsqlRunRepository::current_execution_key(state.namespace_id, &state.workflow_id);
        // Closing an older run must not close a successor that has already
        // replaced this workflow-level pointer, hence the run_key guard.
        sqlx::query(
            "UPDATE current_execution SET is_open = false
             WHERE key = $1 AND run_key = $2",
        )
        .bind(key)
        .bind(run_key.0)
        .execute(&mut **tx)
        .await?;
    }
    if !transition.projection_ops.is_empty() {
        insert_projection_log(
            tx,
            run_key,
            state,
            projection_partition_count,
            transition.projection_ops.as_slice(),
        )
        .await?;
    }
    Ok(())
}

async fn insert_workflow_hot(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_key: RunKey,
    shard_id: ShardId,
    state: &WorkflowState,
) -> Result<()> {
    // `workflow_hot` is a materialized snapshot for recovery and read paths.
    // It is not the audit trail; history_batch carries the append-only events.
    let started = Instant::now();
    sqlx::query(
        "INSERT INTO workflow_hot
         (run_key, namespace_id, workflow_id, shard_id, transition_seq, state_data, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, now())
         ON CONFLICT (run_key) DO UPDATE SET
             transition_seq = EXCLUDED.transition_seq,
             state_data = EXCLUDED.state_data,
             shard_id = EXCLUDED.shard_id,
             updated_at = EXCLUDED.updated_at",
    )
    .bind(run_key.0)
    .bind(state.namespace_id.0)
    .bind(&state.workflow_id.0)
    .bind(DsqlRunRepository::shard_id_to_uuid(shard_id))
    .bind(convert::i64_from_u64(
        state.transition_seq.0,
        "transition_seq",
    )?)
    .bind(codec::encode_workflow_state(state)?)
    .execute(&mut **tx)
    .await?;
    metrics::record_dsql_statement_duration(
        "commit_transition",
        "update_execution",
        started.elapsed(),
    );
    Ok(())
}

async fn insert_history_batch(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_key: RunKey,
    transition_seq: TransitionSeq,
    events: &[HistoryEvent],
) -> Result<()> {
    let first_event_id = events
        .first()
        .ok_or_else(|| anyhow!("cannot insert empty history batch"))?
        .event_id;
    let last_event_id = events
        .last()
        .ok_or_else(|| anyhow!("cannot insert empty history batch"))?
        .event_id;
    let started = Instant::now();
    sqlx::query(
        "INSERT INTO history_batch
         (run_key, first_event_id, last_event_id, transition_seq, events_data, created_at)
         VALUES ($1, $2, $3, $4, $5, now())",
    )
    .bind(run_key.0)
    .bind(first_event_id)
    .bind(last_event_id)
    .bind(convert::i64_from_u64(transition_seq.0, "transition_seq")?)
    .bind(codec::encode_history_events(events)?)
    .execute(&mut **tx)
    .await?;
    metrics::record_dsql_statement_duration(
        "commit_transition",
        "append_history",
        started.elapsed(),
    );
    Ok(())
}

async fn upsert_activity(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_key: RunKey,
    shard_id: ShardId,
    namespace_id: NamespaceId,
    activity: &tokeira_kernel::ActivityState,
) -> Result<()> {
    // Activity state is keyed by schedule_event_id for timer/sweep stability.
    // The human activity_id is still stored for operator-facing mapping and
    // secondary delete predicates.
    sqlx::query(
        "INSERT INTO activity_state
         (run_key, schedule_event_id, shard_id, activity_id, queue_namespace, queue_name, attempt, state_data, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
         ON CONFLICT (run_key, schedule_event_id) DO UPDATE SET
             state_data = EXCLUDED.state_data,
             attempt = EXCLUDED.attempt,
             updated_at = EXCLUDED.updated_at",
    )
    .bind(run_key.0)
    .bind(activity.schedule_event_id)
    .bind(DsqlRunRepository::shard_id_to_uuid(shard_id))
    .bind(&activity.activity_id)
    .bind(namespace_id.0)
    .bind(&activity.task_queue.0)
    .bind(i32::try_from(activity.attempt)?)
    .bind(codec::encode_activity_state(activity)?)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn upsert_activity_dispatch_from_dispatch_op(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_key: RunKey,
    shard_id: ShardId,
    queue: &QueueKey,
    activity_id: &str,
    input: &Payloads,
    schedule_event_id: i64,
    attempt: u32,
) -> Result<()> {
    let key = DsqlRunRepository::activity_dispatch_key(run_key, activity_id);
    let deployment = queue.deployment.as_ref().map(|value| value.0.as_str());
    let build_id = queue.build_id.as_ref().map(|value| value.0.as_str());
    sqlx::query(
        "INSERT INTO activity_dispatch
         (key, run_key, activity_id, shard_id, queue_namespace, queue_name, task_kind,
          deployment, build_id, schedule_event_id, attempt, input_data, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, now())
         ON CONFLICT (key) DO UPDATE SET
             shard_id = EXCLUDED.shard_id,
             queue_namespace = EXCLUDED.queue_namespace,
             queue_name = EXCLUDED.queue_name,
             task_kind = EXCLUDED.task_kind,
             deployment = EXCLUDED.deployment,
             build_id = EXCLUDED.build_id,
             schedule_event_id = EXCLUDED.schedule_event_id,
             attempt = EXCLUDED.attempt,
             input_data = EXCLUDED.input_data",
    )
    .bind(key)
    .bind(run_key.0)
    .bind(activity_id)
    .bind(DsqlRunRepository::shard_id_to_uuid(shard_id))
    .bind(queue.namespace_id.0)
    .bind(&queue.task_queue.0)
    .bind(queue.task_kind.to_db_smallint())
    .bind(deployment)
    .bind(build_id)
    .bind(schedule_event_id)
    .bind(i32::try_from(attempt)?)
    .bind(codec::encode_payloads(input)?)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn update_existing_activity_dispatch(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_key: RunKey,
    shard_id: ShardId,
    state: &WorkflowState,
    activity: &tokeira_kernel::ActivityState,
) -> Result<()> {
    let key = DsqlRunRepository::activity_dispatch_key(run_key, &activity.activity_id);
    // This is deliberately UPDATE-only. If no dispatch row exists, the activity
    // is not currently dispatchable; an ActivityOp::Upsert must not invent one.
    let deployment = activity.deployment.as_ref().or(state.deployment.as_ref());
    let build_id = activity.build_id.as_ref().or(state.build_id.as_ref());
    sqlx::query(
        "UPDATE activity_dispatch SET
             shard_id = $2,
             queue_namespace = $3,
             queue_name = $4,
             task_kind = $5,
             deployment = $6,
             build_id = $7,
             schedule_event_id = $8,
             attempt = $9,
             input_data = $10
         WHERE key = $1",
    )
    .bind(key)
    .bind(DsqlRunRepository::shard_id_to_uuid(shard_id))
    .bind(state.namespace_id.0)
    .bind(&activity.task_queue.0)
    .bind(TaskKind::Activity.to_db_smallint())
    .bind(deployment.map(|value| value.0.as_str()))
    .bind(build_id.map(|value| value.0.as_str()))
    .bind(activity.schedule_event_id)
    .bind(i32::try_from(activity.attempt)?)
    .bind(codec::encode_payloads(&activity.input)?)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn delete_activity_dispatch(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_key: RunKey,
    activity_id: &str,
) -> Result<()> {
    let key = DsqlRunRepository::activity_dispatch_key(run_key, activity_id);
    sqlx::query("DELETE FROM activity_dispatch WHERE key = $1")
        .bind(key)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn delete_activity_dispatch_for_run(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_key: RunKey,
) -> Result<()> {
    sqlx::query("DELETE FROM activity_dispatch WHERE run_key = $1")
        .bind(run_key.0)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn upsert_timer(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_key: RunKey,
    shard_id: ShardId,
    timer: &tokeira_kernel::TimerState,
) -> Result<()> {
    // Timer rows are keyed by shard and fire time so sweepers can ask one shard
    // for due work without scanning all timers.
    sqlx::query(
        "INSERT INTO timer_bucket
         (shard_id, fire_at, run_key, timer_id, timer_data, created_at)
         VALUES ($1, $2, $3, $4, $5, now())
         ON CONFLICT (shard_id, fire_at, run_key, timer_id) DO UPDATE SET
             timer_data = EXCLUDED.timer_data",
    )
    .bind(DsqlRunRepository::shard_id_to_uuid(shard_id))
    .bind(timer.fire_at)
    .bind(run_key.0)
    .bind(&timer.timer_id)
    .bind(codec::encode_timer_state(timer)?)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn upsert_current_execution_start(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_key: RunKey,
    state: &WorkflowState,
) -> Result<()> {
    let key = DsqlRunRepository::current_execution_key(state.namespace_id, &state.workflow_id);
    // This row is a workflow-level pointer, so the primary key is derived from
    // namespace/workflow_id rather than run_id. Explicit run lookup goes
    // through `workflow_hot`.
    sqlx::query(
        "INSERT INTO current_execution
         (key, namespace_id, workflow_id, run_key, run_id, is_open, created_at)
         VALUES ($1, $2, $3, $4, $5, true, now())
         ON CONFLICT (key) DO UPDATE SET
             run_key = EXCLUDED.run_key,
             run_id = EXCLUDED.run_id,
             is_open = true",
    )
    .bind(key)
    .bind(state.namespace_id.0)
    .bind(&state.workflow_id.0)
    .bind(run_key.0)
    .bind(state.run_id.0)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn insert_projection_log(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_key: RunKey,
    state: &WorkflowState,
    projection_partition_count: u32,
    ops: &[ProjectionOp],
) -> Result<()> {
    // Projection log rows are grouped per transition. Visibility sinks can
    // replay the projection stream without rereading workflow state/history.
    let context = ProjectionContext {
        namespace_id: state.namespace_id,
        workflow_id: state.workflow_id.clone(),
        run_id: state.run_id,
        workflow_type: state.workflow_type.clone(),
        task_queue: state.task_queue.clone(),
        execution_status: state.status,
        start_time: state.started_at,
        execution_time: None,
        close_time: state.closed_at,
        history_length: state.last_event_id,
        state_transition_count: convert::i64_from_u64(state.transition_seq.0, "transition_seq")?,
    };
    sqlx::query(
        "INSERT INTO projection_log
         (partition_id, fanout, run_key, transition_seq, context_data, ops_data, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, now())",
    )
    .bind(i32::try_from(partition_for(
        run_key,
        projection_partition_count,
    ))?)
    .bind(PROJECTION_FANOUT)
    .bind(run_key.0)
    .bind(convert::i64_from_u64(
        state.transition_seq.0,
        "transition_seq",
    )?)
    .bind(codec::encode_projection_context(&context)?)
    .bind(codec::encode_projection_ops(ops)?)
    .execute(&mut **tx)
    .await?;
    Ok(())
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

fn epoch_to_sql(epoch: ShardEpoch) -> Result<i64> {
    // DSQL stores epochs in BIGINT. Rejecting overflow here is preferable to a
    // connector/database error after a transaction has started.
    convert::i64_from_u64(epoch.0, "shard_lease.epoch")
}

fn epoch_from_sql(value: i64) -> Result<ShardEpoch> {
    // Negative epochs indicate corrupt storage or an incompatible manual edit.
    Ok(ShardEpoch(convert::u64_from_i64(
        value,
        "shard_lease.epoch",
    )?))
}

#[derive(Debug, PartialEq, Eq)]
enum RenewDecision {
    Renew,
    Reject {
        current_owner: String,
        current_epoch: ShardEpoch,
    },
}

fn interpret_acquire(
    insert_rows_affected: u64,
    update_rows_affected: u64,
    acquired_epoch: Option<i64>,
    rejected_row: Option<(String, i64)>,
) -> Result<LeaseOutcome> {
    // The lease acquire SQL is split into INSERT and UPDATE for portability.
    // These row-count combinations are the contract that maps SQL effects back
    // into the storage trait's semantic outcome.
    match (insert_rows_affected, update_rows_affected) {
        (1, 0) => {
            let epoch = acquired_epoch
                .map(epoch_from_sql)
                .transpose()?
                .ok_or_else(|| anyhow!("acquired shard lease epoch was not returned"))?;
            if epoch != ShardEpoch(1) {
                bail!("new shard lease returned unexpected epoch {}", epoch.0);
            }
            Ok(LeaseOutcome::Acquired { epoch })
        }
        (0, 1) => {
            let epoch = acquired_epoch
                .map(epoch_from_sql)
                .transpose()?
                .ok_or_else(|| anyhow!("updated shard lease epoch was not returned"))?;
            Ok(LeaseOutcome::Acquired { epoch })
        }
        (0, 0) => {
            let (current_owner, current_epoch) =
                rejected_row.ok_or_else(|| anyhow!("rejected shard lease holder was not found"))?;
            Ok(LeaseOutcome::Rejected {
                current_owner,
                current_epoch: epoch_from_sql(current_epoch)?,
            })
        }
        _ => bail!(
            "invalid shard lease acquire row counts: insert={insert_rows_affected}, update={update_rows_affected}"
        ),
    }
}

fn decide_renew(
    row: Option<(&str, i64)>,
    caller_owner: &str,
    caller_epoch: i64,
) -> Result<RenewDecision> {
    // Renewal is stricter than acquire: only the current owner at the exact
    // epoch can extend a lease. Expired-takeover behavior belongs to acquire.
    let Some((current_owner, current_epoch)) = row else {
        return Ok(RenewDecision::Reject {
            current_owner: String::new(),
            current_epoch: ShardEpoch::ZERO,
        });
    };
    if current_owner == caller_owner && current_epoch == caller_epoch {
        return Ok(RenewDecision::Renew);
    }
    Ok(RenewDecision::Reject {
        current_owner: current_owner.to_owned(),
        current_epoch: epoch_from_sql(current_epoch)?,
    })
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
        BuildId, DeploymentId, ExecutionRef, ExecutionStatus, LogicalTaskSeq, Memo, NamespaceId,
        Payloads, QueueKey, RequestId, RunId, RunKey, SearchAttributes, ShardEpoch, StickyAffinity,
        TaskKind, TaskQueueName, TransitionSeq, WorkerIdentity, WorkflowId, WorkflowType,
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
            codec::encode_payloads(&payloads).unwrap(),
        );

        let task = activity_dispatch_from_row(row).unwrap();

        assert_eq!(task.run_key, run_key);
        assert_eq!(task.activity_id, "activity");
        assert_eq!(task.input, payloads);
        assert_eq!(task.schedule_event_id, 42);
        assert_eq!(task.attempt, 3);
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
            scheduled_at: fixed_now(),
            started: false,
        }
    }

    fn sample_transition(run_key: RunKey) -> Transition {
        Transition {
            expected_seq: TransitionSeq::ZERO,
            next_state: sample_state(run_key),
            history_events: Default::default(),
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
            next_workflow_task_seq: LogicalTaskSeq(1),
            pending_workflow_task: Some(PendingWorkflowTask {
                logical_seq: LogicalTaskSeq(1),
                scheduled_event_id: 1,
                scheduled_at: fixed_now(),
                started_event_id: None,
                started_at: None,
                attempt: 1,
            }),
            previous_started_event_id: 0,
            workflow_task_attempt: 1,
            sticky: None,
            pause_info: None,
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
            parent_run_key: None,
            parent_workflow_id: None,
            parent_run_id: None,
            parent_namespace_id: None,
            parent_initiated_event_id: 0,
            last_completion_result: None,
            activities: Default::default(),
            timers: Default::default(),
            children: Default::default(),
            pending_external_signals: Default::default(),
            pending_external_cancels: Default::default(),
            pending_updates: Default::default(),
            admitted_updates: Default::default(),
            pending_nexus_operations: Default::default(),
            versioning_override: None,
            completion_callbacks: Vec::new(),
            started_at: fixed_now(),
            first_run_started_at: None,
            closed_at: None,
            close_result: None,
            close_failure: None,
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
                request_id: format!("request-{event_id}"),
                identity: Some("tester".to_owned()),
            },
        }
    }

    fn sample_activity_state(seed: u64) -> ActivityState {
        ActivityState {
            activity_id: format!("activity-{seed}"),
            activity_type: "activity-type".to_owned(),
            schedule_event_id: i64::try_from(seed).unwrap(),
            task_queue: TaskQueueName("activity-queue".to_owned()),
            deployment: None,
            build_id: None,
            input: Payloads::default(),
            header: None,
            last_failure: None,
            attempt: 1,
            retry_policy: None,
            schedule_to_close_timeout: Some(Duration::seconds(30)),
            schedule_to_start_timeout: Some(Duration::seconds(10)),
            start_to_close_timeout: Some(Duration::seconds(20)),
            heartbeat_timeout: Some(Duration::seconds(5)),
            scheduled_at: fixed_now(),
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
            namespace_id: state.namespace_id,
            workflow_id: state.workflow_id.clone(),
            run_id: state.run_id,
            workflow_type: state.workflow_type.clone(),
            task_queue: state.task_queue.clone(),
            execution_status: state.status,
            start_time: state.started_at,
            execution_time: None,
            close_time: state.closed_at,
            history_length: state.last_event_id,
            state_transition_count: state.transition_seq.0 as i64,
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
