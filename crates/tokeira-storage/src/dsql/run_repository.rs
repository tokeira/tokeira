use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use sqlx::Connection;
use time::OffsetDateTime;
use tokeira_kernel::{
    ActivityOp, BasicKernel, HistoryEvent, LoadedRun, ProjectionOp, ReplayContext, TimerOp,
    Transition, WorkflowState,
};
use tokeira_types::{
    BuildId, DeploymentId, ExecutionRef, NamespaceId, QueueKey, RequestId, RunId, RunKey,
    ShardEpoch, ShardId, TaskKind, TransitionSeq, WorkflowId, dsql_spread_uuid,
};
use tracing::instrument;
use uuid::Uuid;

use crate::{
    ActivitySweepEntry, BacklogEntry, CommitResult, CurrentExecutionConflictPolicy, DbClass,
    DispatchableActivityTask, DispatchableWorkflowTask, DueTimer, NexusSweepEntry,
    ProjectionContext, RequestRecord, RunRepository, TransitionAuditRecord, WftTimeoutSweepEntry,
    WorkflowTimeoutSweepEntry,
};

use super::{DsqlConnectionDirector, DsqlPermit, codec};
use crate::ConnectionDirector;

const PROJECTION_FANOUT: i16 = 1;
const PROJECTION_PARTITION_COUNT: u32 = 16;

/// Production `RunRepository` backed by Aurora DSQL.
#[derive(Debug)]
pub struct DsqlRunRepository {
    director: Arc<dyn DsqlConnectionAcquirer>,
    shard_count: u32,
    conflict_policy: CurrentExecutionConflictPolicy,
}

#[async_trait]
trait DsqlConnectionAcquirer: std::fmt::Debug + Send + Sync {
    async fn acquire(&self, class: DbClass) -> Result<DsqlPermit>;
}

#[async_trait]
impl DsqlConnectionAcquirer for DsqlConnectionDirector {
    async fn acquire(&self, class: DbClass) -> Result<DsqlPermit> {
        ConnectionDirector::acquire(self, class).await
    }
}

impl DsqlRunRepository {
    pub fn new(
        director: Arc<DsqlConnectionDirector>,
        shard_count: u32,
        conflict_policy: CurrentExecutionConflictPolicy,
    ) -> Result<Self> {
        if shard_count == 0 {
            bail!("shard_count must be greater than zero");
        }
        Ok(Self {
            director: director as Arc<dyn DsqlConnectionAcquirer>,
            shard_count,
            conflict_policy,
        })
    }

    #[cfg(test)]
    fn new_with_acquirer(
        director: Arc<dyn DsqlConnectionAcquirer>,
        shard_count: u32,
        conflict_policy: CurrentExecutionConflictPolicy,
    ) -> Result<Self> {
        if shard_count == 0 {
            bail!("shard_count must be greater than zero");
        }
        Ok(Self {
            director,
            shard_count,
            conflict_policy,
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
        ShardId((run_key.0.as_u128() as u32) % self.shard_count)
    }

    /// Stable encoding of `ShardId(u32)` to UUID for SQL binding.
    pub(crate) fn shard_id_to_uuid(shard_id: ShardId) -> Uuid {
        dsql_spread_uuid(&[b"shard", &shard_id.0.to_le_bytes()])
    }

    pub(crate) fn current_execution_key(
        namespace_id: NamespaceId,
        workflow_id: &WorkflowId,
    ) -> Uuid {
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

    pub(crate) fn is_serialization_failure(err: &sqlx::Error) -> bool {
        matches!(err, sqlx::Error::Database(db_err) if db_err.code().as_deref() == Some("40001"))
    }
}

#[async_trait]
impl RunRepository for DsqlRunRepository {
    #[instrument(name = "dsql.resolve_execution", skip(self), fields(namespace_id = %execution.namespace_id.0, workflow_id = %execution.workflow_id.0))]
    async fn resolve_execution(&self, execution: &ExecutionRef) -> Result<Option<RunKey>> {
        let mut permit = self.director.acquire(DbClass::Read).await?;
        if let Some(requested_run_id) = execution.run_id {
            let run_key = RunKey::derive(
                execution.namespace_id,
                &execution.workflow_id,
                requested_run_id,
            );
            let row = sqlx::query_as::<_, (i32,)>("SELECT 1 FROM workflow_hot WHERE run_key = $1")
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
    }

    #[instrument(name = "dsql.find_latest_run", skip(self), fields(namespace_id = %namespace_id.0, workflow_id = %workflow_id.0))]
    async fn find_latest_run(
        &self,
        namespace_id: NamespaceId,
        workflow_id: &WorkflowId,
    ) -> Result<Option<RunKey>> {
        let mut permit = self.director.acquire(DbClass::Read).await?;
        let key = Self::current_execution_key(namespace_id, workflow_id);
        let row = sqlx::query_as::<_, (Uuid,)>(
            "SELECT run_key FROM current_execution
             WHERE key = $1",
        )
        .bind(key)
        .fetch_optional(permit.connection()?)
        .await?;
        Ok(row.map(|(run_key,)| RunKey(run_key)))
    }

    #[instrument(name = "dsql.load_run", skip(self), fields(run_key = %run_key.0))]
    async fn load_run(&self, run_key: RunKey) -> Result<LoadedRun> {
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
    }

    #[instrument(name = "dsql.read_history", skip(self), fields(run_key = %run_key.0, after_event_id, limit))]
    async fn read_history(
        &self,
        run_key: RunKey,
        after_event_id: i64,
        limit: usize,
    ) -> Result<Vec<HistoryEvent>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut permit = self.director.acquire(DbClass::Read).await?;
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

        let mut events = Vec::new();
        for (_first_event_id, _last_event_id, events_data) in rows {
            for event in codec::decode_history_events(&events_data)? {
                if event.event_id <= after_event_id {
                    continue;
                }
                events.push(event);
                if events.len() == limit {
                    return Ok(events);
                }
            }
        }
        Ok(events)
    }

    #[instrument(name = "dsql.lookup_request_dedupe", skip(self), fields(namespace_id = %execution.namespace_id.0, workflow_id = %execution.workflow_id.0, request_id = %request_id.0))]
    async fn lookup_request_dedupe(
        &self,
        execution: &ExecutionRef,
        request_id: &RequestId,
    ) -> Result<Option<RequestRecord>> {
        let mut permit = self.director.acquire(DbClass::Read).await?;
        let key =
            Self::request_dedupe_key(execution.namespace_id, &execution.workflow_id, request_id);
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
            first_seen_transition_seq: TransitionSeq(u64_from_i64(
                transition_seq,
                "request_dedupe.first_seen_transition_seq",
            )?),
        }))
    }

    #[instrument(name = "dsql.read_transition_audit", skip(self), fields(run_key = %run_key.0))]
    async fn read_transition_audit(&self, run_key: RunKey) -> Result<Vec<TransitionAuditRecord>> {
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
                    transition_seq: TransitionSeq(u64_from_i64(
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

    #[instrument(name = "dsql.commit_transition", skip(self, transition), fields(run_key = %run_key.0, expected_seq = transition.expected_seq.0, epoch = epoch.0))]
    async fn commit_transition(
        &self,
        run_key: RunKey,
        transition: Transition,
        epoch: ShardEpoch,
    ) -> Result<CommitResult> {
        // Validate i64 conversions before acquiring a connection or starting a
        // transaction. This prevents mid-transaction failures from overflow on
        // values that are structurally u64 but stored as BIGINT (i64) in DSQL.
        i64_from_u64(transition.next_state.transition_seq.0, "transition_seq")?;
        if should_check_epoch(epoch) {
            i64_from_u64(epoch.0, "caller shard epoch")?;
        }

        let mut permit = self.director.acquire(DbClass::Commit).await?;
        let mut tx = permit.connection()?.begin().await?;
        let state = transition.next_state.clone();
        let shard_id = self.shard_for_run_key(run_key);

        if should_check_epoch(epoch) {
            let row =
                sqlx::query_as::<_, (i64,)>("SELECT epoch FROM shard_lease WHERE shard_id = $1")
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
            if durable_epoch != i64_from_u64(epoch.0, "caller shard epoch")? {
                tx.rollback().await?;
                return Ok(CommitResult::Conflict {
                    reason: format!(
                        "stale shard epoch {:?} for shard {:?}; current {}",
                        epoch, shard_id, durable_epoch
                    ),
                });
            }
        }

        let row = sqlx::query_as::<_, (i64,)>(
            "SELECT transition_seq FROM workflow_hot WHERE run_key = $1 FOR UPDATE",
        )
        .bind(run_key.0)
        .fetch_optional(&mut *tx)
        .await?;
        let current_seq = match row {
            Some((seq,)) => TransitionSeq(u64_from_i64(seq, "workflow_hot.transition_seq")?),
            None => TransitionSeq::ZERO,
        };
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
            let key =
                Self::request_dedupe_key(state.namespace_id, &state.workflow_id, &op.request_id);
            let row = sqlx::query_as::<_, (i32,)>(
                "SELECT 1 FROM request_dedupe
                 WHERE key = $1",
            )
            .bind(key)
            .fetch_optional(&mut *tx)
            .await?;
            if row.is_some() {
                tx.rollback().await?;
                return Ok(CommitResult::Duplicate);
            }
        }

        if transition.expected_seq == TransitionSeq::ZERO && state.status.is_open() {
            let key = Self::current_execution_key(state.namespace_id, &state.workflow_id);
            let row = sqlx::query_as::<_, (Uuid, bool)>(
                "SELECT run_key, is_open FROM current_execution
                 WHERE key = $1",
            )
            .bind(key)
            .fetch_optional(&mut *tx)
            .await?;
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

        write_transition(&mut tx, run_key, shard_id, &transition, &state).await?;
        match tx.commit().await {
            Ok(()) => Ok(CommitResult::Applied { new_state: state }),
            Err(err) if Self::is_serialization_failure(&err) => Ok(CommitResult::Conflict {
                reason: "DSQL serialization conflict".to_owned(),
            }),
            Err(err) => Err(err.into()),
        }
    }

    #[instrument(name = "dsql.materialize_reset_successor", skip(self), fields(base_run_key = %base_run_key.0, fork_event_id, successor_run_id = %successor_run_id.0))]
    async fn materialize_reset_successor(
        &self,
        base_run_key: RunKey,
        fork_event_id: i64,
        successor_run_id: RunId,
    ) -> Result<()> {
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
        upsert_current_execution_start(&mut tx, successor_run_key, &successor_state).await?;
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

    async fn list_dispatchable_workflow_tasks(
        &self,
        _queue: &QueueKey,
        _limit: usize,
    ) -> Result<Vec<DispatchableWorkflowTask>> {
        bail!("Feature 3: dsql-side-tables")
    }

    async fn list_dispatchable_activity_tasks(
        &self,
        _queue: &QueueKey,
        _limit: usize,
    ) -> Result<Vec<DispatchableActivityTask>> {
        bail!("Feature 3: dsql-side-tables")
    }

    async fn persist_to_backlog(&self, entries: Vec<BacklogEntry>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut permit = self.director.acquire(DbClass::Commit).await?;
        let mut tx = permit.connection()?.begin().await?;
        for entry in entries {
            let partition_id = partition_for(entry.run_key);
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
            .bind(i64_from_u64(entry.insertion_seq, "dispatch_backlog.insertion_seq")?)
            .bind(entry.run_key.0)
            .bind(codec::encode_backlog_payload(&entry.payload)?)
            .bind(entry.scheduled_at)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn drain_backlog(&self, queue: &QueueKey, limit: usize) -> Result<Vec<BacklogEntry>> {
        if limit == 0 {
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

        let mut drained = Vec::with_capacity(rows.len());
        for (key, run_key, payload_data, scheduled_at, insertion_seq, task_kind_raw, stored_deployment, stored_build_id) in rows {
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
                insertion_seq: u64_from_i64(insertion_seq, "dispatch_backlog.insertion_seq")?,
            });
        }
        tx.commit().await?;
        Ok(drained)
    }

    async fn list_due_timers(&self, _now: OffsetDateTime, _limit: usize) -> Result<Vec<DueTimer>> {
        bail!("Feature 3: dsql-side-tables")
    }

    async fn list_dispatchable_workflow_tasks_for_shard(
        &self,
        _shard_id: ShardId,
        _limit: usize,
    ) -> Result<Vec<DispatchableWorkflowTask>> {
        bail!("Feature 3: dsql-side-tables")
    }

    async fn list_dispatchable_activity_tasks_for_shard(
        &self,
        _shard_id: ShardId,
        _limit: usize,
    ) -> Result<Vec<DispatchableActivityTask>> {
        bail!("Feature 3: dsql-side-tables")
    }

    async fn list_due_timers_for_shard(
        &self,
        _shard_id: ShardId,
        _now: OffsetDateTime,
        _limit: usize,
    ) -> Result<Vec<DueTimer>> {
        bail!("Feature 3: dsql-side-tables")
    }

    async fn list_runs_with_workflow_timeouts_for_shard(
        &self,
        _shard_id: ShardId,
        _limit: usize,
    ) -> Result<Vec<WorkflowTimeoutSweepEntry>> {
        bail!("Feature 3: dsql-side-tables")
    }

    async fn list_started_workflow_tasks_for_shard(
        &self,
        _shard_id: ShardId,
        _limit: usize,
    ) -> Result<Vec<WftTimeoutSweepEntry>> {
        bail!("Feature 3: dsql-side-tables")
    }

    async fn list_open_activities_for_shard(
        &self,
        _shard_id: ShardId,
        _limit: usize,
    ) -> Result<Vec<ActivitySweepEntry>> {
        bail!("Feature 3: dsql-side-tables")
    }

    async fn list_pending_nexus_operations_for_shard(
        &self,
        _shard_id: ShardId,
        _limit: usize,
    ) -> Result<Vec<NexusSweepEntry>> {
        bail!("Feature 3: dsql-side-tables")
    }
}

async fn write_transition(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_key: RunKey,
    shard_id: ShardId,
    transition: &Transition,
    state: &WorkflowState,
) -> Result<()> {
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
        .bind(i64_from_u64(state.transition_seq.0, "transition_seq")?)
        .execute(&mut **tx)
        .await?;
    }
    for op in &transition.activity_ops {
        match op {
            ActivityOp::Upsert(activity) => {
                upsert_activity(tx, run_key, shard_id, state.namespace_id, activity).await?
            }
            ActivityOp::Delete { activity_id } => {
                sqlx::query("DELETE FROM activity_state WHERE run_key = $1 AND activity_id = $2")
                    .bind(run_key.0)
                    .bind(activity_id)
                    .execute(&mut **tx)
                    .await?;
            }
        }
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
        upsert_current_execution_start(tx, run_key, state).await?;
    } else if !state.status.is_open() {
        let key = DsqlRunRepository::current_execution_key(state.namespace_id, &state.workflow_id);
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
        insert_projection_log(tx, run_key, state, transition.projection_ops.as_slice()).await?;
    }
    Ok(())
}

async fn insert_workflow_hot(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_key: RunKey,
    shard_id: ShardId,
    state: &WorkflowState,
) -> Result<()> {
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
    .bind(i64_from_u64(state.transition_seq.0, "transition_seq")?)
    .bind(codec::encode_workflow_state(state)?)
    .execute(&mut **tx)
    .await?;
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
    sqlx::query(
        "INSERT INTO history_batch
         (run_key, first_event_id, last_event_id, transition_seq, events_data, created_at)
         VALUES ($1, $2, $3, $4, $5, now())",
    )
    .bind(run_key.0)
    .bind(first_event_id)
    .bind(last_event_id)
    .bind(i64_from_u64(transition_seq.0, "transition_seq")?)
    .bind(codec::encode_history_events(events)?)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn upsert_activity(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_key: RunKey,
    shard_id: ShardId,
    namespace_id: NamespaceId,
    activity: &tokeira_kernel::ActivityState,
) -> Result<()> {
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

async fn upsert_timer(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_key: RunKey,
    shard_id: ShardId,
    timer: &tokeira_kernel::TimerState,
) -> Result<()> {
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
    ops: &[ProjectionOp],
) -> Result<()> {
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
        state_transition_count: i64_from_u64(state.transition_seq.0, "transition_seq")?,
    };
    sqlx::query(
        "INSERT INTO projection_log
         (partition_id, fanout, run_key, transition_seq, context_data, ops_data, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, now())",
    )
    .bind(i32::try_from(partition_for(run_key))?)
    .bind(PROJECTION_FANOUT)
    .bind(run_key.0)
    .bind(i64_from_u64(state.transition_seq.0, "transition_seq")?)
    .bind(codec::encode_projection_context(&context)?)
    .bind(codec::encode_projection_ops(ops)?)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

fn partition_for(run_key: RunKey) -> u32 {
    (run_key.0.as_u128() as u32) % PROJECTION_PARTITION_COUNT
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

fn i64_from_u64(value: u64, field: &str) -> Result<i64> {
    i64::try_from(value).with_context(|| format!("{field} exceeds i64 range"))
}

fn u64_from_i64(value: i64, field: &str) -> Result<u64> {
    u64::try_from(value).with_context(|| format!("{field} is negative"))
}

fn should_check_epoch(epoch: ShardEpoch) -> bool {
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

    use anyhow::{Result, bail};
    use async_trait::async_trait;
    use proptest::prelude::*;
    use time::{Duration, OffsetDateTime};
    use tokeira_kernel::{
        ActivityState, HistoryEvent, HistoryEventKind, PendingWorkflowTask, ProjectionOp,
        TimerState, Transition, WorkflowState,
    };
    use tokeira_types::{
        ExecutionRef, ExecutionStatus, LogicalTaskSeq, Memo, NamespaceId, Payloads, RequestId,
        RunId, RunKey, SearchAttributes, TaskKind, TaskQueueName, TransitionSeq, WorkflowId,
        WorkflowType,
    };

    use super::{DsqlConnectionAcquirer, DsqlRunRepository, should_check_epoch};
    use crate::{
        CurrentExecutionConflictPolicy, DbClass, ProjectionContext, RunRepository, dsql::codec,
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
                CurrentExecutionConflictPolicy::Reject,
            )
            .is_err()
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
    fn zero_epoch_bypasses_fence_check() {
        assert!(!should_check_epoch(tokeira_types::ShardEpoch::ZERO));
        assert!(should_check_epoch(tokeira_types::ShardEpoch(1)));
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
    async fn read_history_zero_limit_does_not_acquire_connection() {
        let recorder = Arc::new(RecordingAcquirer::default());
        let repo = test_repo(Arc::clone(&recorder));

        let result = repo.read_history(RunKey::new(), 0, 0).await.unwrap();

        assert!(result.is_empty());
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
        async fn acquire(&self, class: DbClass) -> Result<super::DsqlPermit> {
            self.classes.lock().unwrap().push(class);
            bail!("test acquirer has no database connection")
        }
    }

    fn test_repo(recorder: Arc<RecordingAcquirer>) -> DsqlRunRepository {
        DsqlRunRepository::new_with_acquirer(recorder, 4, CurrentExecutionConflictPolicy::Reject)
            .unwrap()
    }

    fn fixed_now() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()
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
