//! The DSQL repository's paged bulk writes (`bounded-bulk-writes`): a run's
//! purge, the abandonment of a reset's materialization, and the materialization
//! itself.
//!
//! DSQL refuses a transaction that changes more than 3,000 rows or writes more
//! than 10 MiB, and any value over 1 MiB ([`crate::write_budget`]). These writes
//! would put a set of unbounded size into one transaction, so they cut it into
//! pages within the budget, each a transaction of its own, behind a
//! `run_bulk_write` record:
//! - A run with a record has no `workflow_hot` row, so no lookup finds the run
//!   while its rows are written or removed.
//! - The record is the durable task of an unfinished write: [`purge_run`]
//!   finishes it, from any node, as often as it is asked.
//! - Every transaction of a materialization reads the record `FOR UPDATE`, so a
//!   switch of the record to purging fences the materialization: DSQL commits at
//!   most one of two transactions that overlap on the record, and a later one
//!   reads the switch.
//!
//! [`purge_run`]: crate::RunRepository::purge_run

use super::*;
use crate::{
    BulkWritePhase, RunBulkWrite,
    write_budget::{
        MAX_RESET_BATCH_BYTES, MAX_ROWS_PER_TRANSACTION, WriteBudget, WriteCost, pages,
    },
};

/// Attempts at one paged transaction before a serialization conflict fails
/// the write.
const TRANSACTION_ATTEMPTS: usize = 5;

/// One run-owned table a purge pages through, with the statements that select a
/// page's keys in order and delete the page by its last key.
pub(super) struct PurgeTable {
    pub(super) table: &'static str,
    select_page: &'static str,
    delete_page: &'static str,
}

/// The tables a purge empties, in order, history last: the run's authority goes
/// after every row derived from it. `workflow_dispatch` and `workflow_hot` go in
/// the deletion's first transaction. Each page is selected in its table's key
/// order and deleted by its last key, so a page is exactly the rows selected:
/// nothing writes a row of a recorded run.
pub(super) const PURGE_TABLES: [PurgeTable; 6] = [
    PurgeTable {
        table: "request_dedupe",
        select_page: "SELECT key::text FROM request_dedupe WHERE run_key = $1 ORDER BY key LIMIT $2",
        delete_page: "DELETE FROM request_dedupe WHERE run_key = $1 AND key <= $2::uuid",
    },
    // Nothing writes `activity_state` any more; this clears the rows that
    // earlier releases wrote (`activity-state-writes` criterion 3.2).
    PurgeTable {
        table: "activity_state",
        select_page: "SELECT schedule_event_id::text FROM activity_state WHERE run_key = $1 \
                      ORDER BY schedule_event_id LIMIT $2",
        delete_page: "DELETE FROM activity_state WHERE run_key = $1 AND schedule_event_id <= $2::bigint",
    },
    PurgeTable {
        table: "timer_bucket",
        select_page: "SELECT timer_id FROM timer_bucket WHERE run_key = $1 ORDER BY timer_id LIMIT $2",
        delete_page: "DELETE FROM timer_bucket WHERE run_key = $1 AND timer_id <= $2",
    },
    PurgeTable {
        table: "activity_dispatch",
        select_page: "SELECT key::text FROM activity_dispatch WHERE run_key = $1 ORDER BY key LIMIT $2",
        delete_page: "DELETE FROM activity_dispatch WHERE run_key = $1 AND key <= $2::uuid",
    },
    PurgeTable {
        table: "dispatch_backlog",
        select_page: "SELECT key::text FROM dispatch_backlog WHERE run_key = $1 ORDER BY key LIMIT $2",
        delete_page: "DELETE FROM dispatch_backlog WHERE run_key = $1 AND key <= $2::uuid",
    },
    PurgeTable {
        table: "history_batch",
        select_page: "SELECT first_event_id::text FROM history_batch WHERE run_key = $1 \
                      ORDER BY first_event_id LIMIT $2",
        delete_page: "DELETE FROM history_batch WHERE run_key = $1 AND first_event_id <= $2::bigint",
    },
];

pub(super) const SELECT_RECORD_FOR_UPDATE: &str =
    "SELECT phase FROM run_bulk_write WHERE run_key = $1 FOR UPDATE";

/// Run one transaction of a paged write, again when DSQL refuses it with a
/// serialization conflict. Every transaction of these writes is safe to repeat.
async fn retry_on_conflict<T, F, Fut>(mut transaction: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut attempt = 1;
    loop {
        match transaction().await {
            Err(error)
                if attempt < TRANSACTION_ATTEMPTS && is_serialization_failure_error(&error) =>
            {
                attempt += 1;
            }
            result => return result,
        }
    }
}

/// One item a materialization copies before its final transaction.
pub(super) enum CopyItem<'a> {
    /// A history batch: its first and last event ids, encoded events and
    /// principals (NULL when no event has one, as `insert_history_batch` writes).
    Batch {
        first_event_id: i64,
        last_event_id: i64,
        events_data: Vec<u8>,
        principals_data: Option<Vec<u8>>,
    },
    /// A timer row.
    Timer(&'a tokeira_kernel::TimerState),
}

impl CopyItem<'_> {
    fn cost(&self) -> Result<WriteCost> {
        Ok(match self {
            Self::Batch {
                events_data,
                principals_data,
                ..
            } => WriteCost::write(events_data.len() + principals_data.as_ref().map_or(0, Vec::len)),
            Self::Timer(timer) => WriteCost::write(codec::encode_timer_state(timer)?.len()),
        })
    }
}

impl DsqlRunRepository {
    pub(super) async fn do_abandon_materialization(&self, run_key: RunKey) -> Result<()> {
        record_dsql_operation!(self, "abandon_materialization", None, {
            retry_on_conflict(|| async {
                let mut permit = self.director.acquire(DbClass::Commit).await?;
                // The update writes the record, which every transaction of the
                // materialization reads `FOR UPDATE`: once it commits, none of
                // them can commit a write after it.
                sqlx::query(
                    "UPDATE run_bulk_write SET phase = $2 WHERE run_key = $1 AND phase = $3",
                )
                .bind(run_key.0)
                .bind(BulkWritePhase::Purging.to_db_smallint())
                .bind(BulkWritePhase::Materializing.to_db_smallint())
                .execute(permit.connection()?)
                .await?;
                Ok(())
            })
            .await
        })
    }

    pub(super) async fn do_purge_run(&self, run_key: RunKey) -> Result<()> {
        self.do_abandon_materialization(run_key).await?;
        record_dsql_operation!(self, "purge_run", None, {
            // A run without a record has mutable state, or is already purged;
            // either way it isn't this purge's to touch.
            let record = {
                let mut permit = self.director.acquire(DbClass::Commit).await?;
                sqlx::query_as::<_, (i16,)>("SELECT phase FROM run_bulk_write WHERE run_key = $1")
                    .bind(run_key.0)
                    .fetch_optional(permit.connection()?)
                    .await?
            };
            if record.is_none() {
                return Ok(());
            }
            let (history, derived) = PURGE_TABLES
                .split_last()
                .expect("the purge has a history table");
            for table in derived {
                while retry_on_conflict(|| {
                    self.purge_page(run_key, table, MAX_ROWS_PER_TRANSACTION)
                })
                .await?
                    == MAX_ROWS_PER_TRANSACTION
                {}
            }
            // History last, and the record with its last page: the record goes
            // only when every table is empty, so a purge that stops early leaves
            // it for the next attempt.
            while !retry_on_conflict(|| self.purge_history_page(run_key, history)).await? {}
            Ok(())
        })
    }

    /// Delete one page of `table`'s rows of the run, and return how many it
    /// held.
    pub(super) async fn purge_page(
        &self,
        run_key: RunKey,
        table: &PurgeTable,
        limit: usize,
    ) -> Result<usize> {
        let mut permit = self.director.acquire(DbClass::Commit).await?;
        let mut tx = permit.connection()?.begin().await?;
        let keys = sqlx::query_as::<_, (String,)>(table.select_page)
            .bind(run_key.0)
            .bind(i64::try_from(limit)?)
            .fetch_all(&mut *tx)
            .await?;
        let Some((last,)) = keys.last() else {
            tx.rollback().await?;
            return Ok(0);
        };
        sqlx::query(table.delete_page)
            .bind(run_key.0)
            .bind(last)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        tracing::debug!(
            table = table.table,
            rows = keys.len(),
            "purged a page of a run's rows"
        );
        metrics::record_dsql_rows_written("purge_run", keys.len() as u64);
        Ok(keys.len())
    }

    /// Delete one page of the run's history, with the record when it is the
    /// last. Returns whether it was.
    async fn purge_history_page(&self, run_key: RunKey, history: &PurgeTable) -> Result<bool> {
        // A page leaves one row of the budget for the record.
        let page = MAX_ROWS_PER_TRANSACTION - 1;
        let mut permit = self.director.acquire(DbClass::Commit).await?;
        let mut tx = permit.connection()?.begin().await?;
        let keys = sqlx::query_as::<_, (String,)>(history.select_page)
            .bind(run_key.0)
            .bind(i64::try_from(page + 1)?)
            .fetch_all(&mut *tx)
            .await?;
        let last = keys.len() <= page;
        let deleted = keys.len().min(page);
        if deleted > 0 {
            sqlx::query(history.delete_page)
                .bind(run_key.0)
                .bind(&keys[deleted - 1].0)
                .execute(&mut *tx)
                .await?;
        }
        if last {
            sqlx::query("DELETE FROM run_bulk_write WHERE run_key = $1")
                .bind(run_key.0)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        metrics::record_dsql_rows_written("purge_run", (deleted + usize::from(last)) as u64);
        Ok(last)
    }

    pub(super) async fn do_list_run_bulk_writes(
        &self,
        shard_id: ShardId,
        after: Option<RunKey>,
        limit: usize,
    ) -> Result<Vec<RunBulkWrite>> {
        record_dsql_operation!(self, "list_run_bulk_writes", Some(shard_id), {
            let mut permit = self.director.acquire(DbClass::Read).await?;
            let shard = Self::shard_id_to_uuid(shard_id);
            let limit = i64::try_from(limit)?;
            let rows = match after {
                Some(after) => {
                    sqlx::query_as::<_, (Uuid, i16)>(
                        "SELECT run_key, phase FROM run_bulk_write
                         WHERE shard_id = $1 AND run_key > $2 ORDER BY run_key LIMIT $3",
                    )
                    .bind(shard)
                    .bind(after.0)
                    .bind(limit)
                    .fetch_all(permit.connection()?)
                    .await?
                }
                None => {
                    sqlx::query_as::<_, (Uuid, i16)>(
                        "SELECT run_key, phase FROM run_bulk_write
                         WHERE shard_id = $1 ORDER BY run_key LIMIT $2",
                    )
                    .bind(shard)
                    .bind(limit)
                    .fetch_all(permit.connection()?)
                    .await?
                }
            };
            metrics::record_dsql_rows_read("list_run_bulk_writes", rows.len());
            rows.into_iter()
                .map(|(run_key, phase)| {
                    Ok(RunBulkWrite {
                        run_key: RunKey(run_key),
                        shard_id,
                        phase: BulkWritePhase::from_db_smallint(phase)?,
                    })
                })
                .collect()
        })
    }

    #[instrument(name = "dsql.materialize_reset_successor", skip(self), fields(base_run_key = %base_run_key.0, fork_event_id, successor_run_id = %successor_run_id.0))]
    pub(super) async fn do_materialize_reset_successor(
        &self,
        base_run_key: RunKey,
        fork_event_id: i64,
        successor_run_id: RunId,
        expected_current: Option<RunKey>,
    ) -> Result<()> {
        record_dsql_operation!(
            self,
            "materialize_reset_successor",
            Some(self.shard_for_run_key(base_run_key)),
            {
                let (base_state, copied_events, copied_principals) =
                    self.read_reset_base(base_run_key, fork_event_id).await?;
                let successor_run_key = RunKey::derive(
                    base_state.namespace_id,
                    &base_state.workflow_id,
                    successor_run_id,
                );
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
                let mut successor_state = BasicKernel
                    .replay_history_prefix(replay_ctx, &copied_events)
                    .map_err(anyhow::Error::from)?;
                // Parity with the memory store's materialization: the reset run
                // inherits the chain origin, and its run/execution-timeout
                // windows restart at reset time (v1.31.0
                // `RefreshExpirationTimeoutTask`, mutable_state_impl.go:8417 —
                // the replayed prefix's original timestamps must not leave the
                // successor born expired).
                successor_state.original_execution_run_id = base_state
                    .original_execution_run_id
                    .or(Some(base_state.run_id));
                let materialized_at = time::OffsetDateTime::now_utc();
                successor_state.started_at = materialized_at;
                successor_state.first_run_started_at = Some(materialized_at);
                // Reset must use the same placement as a regular commit: its
                // timers, dispatch row and record live at the execution home.
                let successor_shard = tokeira_types::execution_home_bundle(
                    successor_state.namespace_id.0.as_bytes(),
                    successor_state.workflow_id.0.as_bytes(),
                    self.shard_count,
                );

                // The copied history in batches DSQL can store. Its History
                // Size is the sum of their encoded sizes, as for any run
                // (`continue-as-new-advice` Requirement 1.6).
                let mut items = Vec::new();
                let mut history_size = 0i64;
                for batch in codec::reset_history_batches(
                    &copied_events,
                    &copied_principals,
                    MAX_RESET_BATCH_BYTES,
                )? {
                    let events = &copied_events[batch.clone()];
                    let principals = &copied_principals[batch];
                    let events_data = codec::encode_history_events(events)?;
                    history_size = history_size
                        .saturating_add(i64::try_from(events_data.len()).unwrap_or(i64::MAX));
                    items.push(CopyItem::Batch {
                        first_event_id: events.first().map_or(0, |event| event.event_id),
                        last_event_id: events.last().map_or(0, |event| event.event_id),
                        events_data,
                        principals_data: principals
                            .iter()
                            .any(Option::is_some)
                            .then(|| codec::encode_history_principals(principals))
                            .transpose()?,
                    });
                }
                items.extend(successor_state.timers.values().map(CopyItem::Timer));
                let costs = items
                    .iter()
                    .map(CopyItem::cost)
                    .collect::<Result<Vec<_>>>()?;

                retry_on_conflict(|| {
                    self.record_materialization(successor_run_key, successor_shard)
                })
                .await?;
                for page in pages(&costs, WriteBudget::TRANSACTION) {
                    let page_items = &items[page];
                    retry_on_conflict(|| {
                        self.copy_materialization_page(
                            successor_run_key,
                            successor_shard,
                            successor_state.transition_seq,
                            page_items,
                        )
                    })
                    .await?;
                }
                let state_data = codec::encode_workflow_state(&successor_state)?;
                retry_on_conflict(|| {
                    self.finish_materialization(
                        successor_run_key,
                        successor_shard,
                        &successor_state,
                        &state_data,
                        history_size,
                        expected_current,
                    )
                })
                .await
            }
        )
    }

    /// The base's state and its history before `fork_event_id`, with
    /// principals, read in one read transaction.
    async fn read_reset_base(
        &self,
        base_run_key: RunKey,
        fork_event_id: i64,
    ) -> Result<(
        WorkflowState,
        Vec<HistoryEvent>,
        Vec<Option<tokeira_types::EventPrincipal>>,
    )> {
        // The commit class, as for every step of this write operation.
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
        let base_state = codec::decode_workflow_state(base_run_key, &base_state_data)?;
        let history_rows = sqlx::query_as::<_, (Vec<u8>, Option<Vec<u8>>)>(
            "SELECT events_data, principals_data FROM history_batch
             WHERE run_key = $1 ORDER BY first_event_id ASC",
        )
        .bind(base_run_key.0)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;

        // Reset materialization copies only the committed prefix BEFORE the
        // fork event: the fork is the WFT-FINISH event being reset, and v1.31.0
        // rebuilds mutable state to `WorkflowTaskFinishEventId - 1`
        // (`baseRebuildLastEventID`, resetworkflow/api.go:119 @ v1.31.0).
        // Replay then derives the successor state — ending with the reset WFT
        // still started, ready for the ResetWorkflow failure — avoiding a second
        // source of truth for reset snapshots.
        let mut events = Vec::new();
        let mut principals = Vec::new();
        for (events_data, principals_data) in history_rows {
            for attributed in super::load::decode_attributed_history_batch(
                base_run_key,
                &events_data,
                principals_data.as_deref(),
            )? {
                if attributed.event.event_id == fork_event_id {
                    codec::ensure_contiguous_prefix(base_run_key, &events)?;
                    return Ok((base_state, events, principals));
                }
                events.push(attributed.event);
                principals.push(attributed.principal);
            }
        }
        bail!(
            "fork_event_id {} outside committed history for {:?}",
            fork_event_id,
            base_run_key
        )
    }

    /// Record that the successor is being materialized, while it has no
    /// mutable state.
    pub(super) async fn record_materialization(
        &self,
        run_key: RunKey,
        shard_id: ShardId,
    ) -> Result<()> {
        let mut permit = self.director.acquire(DbClass::Commit).await?;
        let mut tx = permit.connection()?.begin().await?;
        let existing = sqlx::query_as::<_, (i32,)>("SELECT 1 FROM workflow_hot WHERE run_key = $1")
            .bind(run_key.0)
            .fetch_optional(&mut *tx)
            .await?;
        if existing.is_some() {
            tx.rollback().await?;
            bail!("successor run already exists: {run_key:?}");
        }
        // A plain insert: a record already present belongs to another attempt
        // for the same successor, which this one must not take over.
        sqlx::query(
            "INSERT INTO run_bulk_write (run_key, shard_id, phase, created_at)
             VALUES ($1, $2, $3, now())",
        )
        .bind(run_key.0)
        .bind(Self::shard_id_to_uuid(shard_id))
        .bind(BulkWritePhase::Materializing.to_db_smallint())
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Fail unless the run's record still says it is being materialized. The
    /// read is `FOR UPDATE`, which is what fences this transaction against a
    /// switch of the record to purging.
    async fn ensure_materializing(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        run_key: RunKey,
    ) -> Result<()> {
        let phase = sqlx::query_as::<_, (i16,)>(SELECT_RECORD_FOR_UPDATE)
            .bind(run_key.0)
            .fetch_optional(&mut **tx)
            .await?;
        match phase
            .map(|(phase,)| BulkWritePhase::from_db_smallint(phase))
            .transpose()?
        {
            Some(BulkWritePhase::Materializing) => Ok(()),
            _ => bail!("the materialization of {run_key:?} was abandoned"),
        }
    }

    pub(super) async fn copy_materialization_page(
        &self,
        run_key: RunKey,
        shard_id: ShardId,
        transition_seq: TransitionSeq,
        items: &[CopyItem<'_>],
    ) -> Result<()> {
        let mut permit = self.director.acquire(DbClass::Commit).await?;
        let mut tx = permit.connection()?.begin().await?;
        Self::ensure_materializing(&mut tx, run_key).await?;
        for item in items {
            match item {
                CopyItem::Batch {
                    first_event_id,
                    last_event_id,
                    events_data,
                    principals_data,
                } => {
                    // `DO NOTHING` makes a repeated page a no-op: a batch's
                    // bytes are a function of the copied events alone.
                    sqlx::query(
                        "INSERT INTO history_batch
                         (run_key, first_event_id, last_event_id, transition_seq, events_data,
                          principals_data, created_at)
                         VALUES ($1, $2, $3, $4, $5, $6, now())
                         ON CONFLICT (run_key, first_event_id) DO NOTHING",
                    )
                    .bind(run_key.0)
                    .bind(first_event_id)
                    .bind(last_event_id)
                    .bind(convert::i64_from_u64(transition_seq.0, "transition_seq")?)
                    .bind(events_data)
                    .bind(principals_data)
                    .execute(&mut *tx)
                    .await?;
                }
                CopyItem::Timer(timer) => {
                    commit::upsert_timer(&mut tx, run_key, shard_id, timer).await?;
                }
            }
        }
        tx.commit().await?;
        Ok(())
    }

    async fn finish_materialization(
        &self,
        run_key: RunKey,
        shard_id: ShardId,
        state: &WorkflowState,
        state_data: &[u8],
        history_size: i64,
        expected_current: Option<RunKey>,
    ) -> Result<()> {
        let mut permit = self.director.acquire(DbClass::Commit).await?;
        let mut tx = permit.connection()?.begin().await?;
        Self::ensure_materializing(&mut tx, run_key).await?;
        // v1.31.0 moves the current pointer to a reset's run only while the
        // pointer names the run its reset updates
        // (`assertRunIDAndUpdateCurrentExecution`,
        // common/persistence/sql/execution_util.go:966-1009 @ v1.31.0). Here
        // that is the run the pointer named when the reset was admitted: a start
        // that landed since keeps the pointer, and the materialization fails.
        let current = sqlx::query_as::<_, (Uuid,)>(
            "SELECT run_key FROM current_execution WHERE key = $1 FOR UPDATE",
        )
        .bind(Self::current_execution_key(
            state.namespace_id,
            &state.workflow_id,
        ))
        .fetch_optional(&mut *tx)
        .await?
        .map(|(run_key,)| RunKey(run_key));
        if current != expected_current {
            tx.rollback().await?;
            bail!(
                "the current run of workflow {:?} changed while its reset was materialized: \
                 expected {expected_current:?}, found {current:?}",
                state.workflow_id
            );
        }
        commit::insert_workflow_hot(&mut tx, run_key, shard_id, state, state_data, history_size)
            .await?;
        super::workflow_dispatch::maintain(&mut tx, state, shard_id).await?;
        commit::upsert_current_execution_start(&mut tx, run_key, state).await?;
        sqlx::query("DELETE FROM run_bulk_write WHERE run_key = $1")
            .bind(run_key.0)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::PURGE_TABLES;

    #[test]
    fn the_purge_covers_every_run_owned_table_and_history_is_last() {
        let tables = PURGE_TABLES
            .iter()
            .map(|table| table.table)
            .collect::<Vec<_>>();
        assert_eq!(
            tables,
            [
                "request_dedupe",
                "activity_state",
                "timer_bucket",
                "activity_dispatch",
                "dispatch_backlog",
                "history_batch",
            ]
        );
        for table in &PURGE_TABLES {
            assert!(
                table
                    .select_page
                    .contains(&format!("FROM {} WHERE run_key = $1", table.table))
            );
            assert!(
                table
                    .delete_page
                    .starts_with(&format!("DELETE FROM {} WHERE run_key = $1", table.table))
            );
        }
    }
}
