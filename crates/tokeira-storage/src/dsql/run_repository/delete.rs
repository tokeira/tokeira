//! Fenced authoritative workflow-run deletion for the DSQL repository.
//!
//! Deletion is deliberately separate from the kernel transition writer: an
//! open run first closes through the ordinary lane/kernel path, then this module
//! makes the closed run unreachable in one transaction: it appends a visibility
//! tombstone, removes the current pointer, the run's mutable state and its
//! workflow dispatch row, and records the run for purging. The sequence and
//! execution-home epoch are checked inside that transaction, so a stale owner
//! cannot erase any of a run.
//!
//! The run's other rows can outnumber what one DSQL transaction may change, so
//! the purge in `bulk_write` removes them in pages, history last
//! (`bounded-bulk-writes`).

use super::*;

const CURRENT_EXECUTION_DELETE_STATEMENT: &str =
    "DELETE FROM current_execution WHERE key = $1 AND run_key = $2";
/// The first transaction's removals besides the pointer: the dispatch row a
/// closed run no longer needs, then the mutable state every lookup reads.
const FIRST_TRANSACTION_DELETE_STATEMENTS: [&str; 2] = [
    "DELETE FROM workflow_dispatch WHERE run_key = $1",
    "DELETE FROM workflow_hot WHERE run_key = $1",
];

/// The record the purge finishes from. A plain insert: the run has mutable
/// state, so it has no record (`bounded-bulk-writes`).
const RECORD_PURGE_STATEMENT: &str =
    "INSERT INTO run_bulk_write (run_key, shard_id, phase, created_at)
     VALUES ($1, $2, $3, now())";

impl DsqlRunRepository {
    pub(super) async fn do_delete_run_for_bundle(
        &self,
        run_key: RunKey,
        execution_home_bundle: ShardId,
        request: DeleteRunRequest,
        epoch: ShardEpoch,
    ) -> Result<DeleteRunResult> {
        let span = tracing::info_span!(
            "dsql.delete_run_for_bundle",
            run_key = %run_key.0,
            bundle = execution_home_bundle.0,
            expected_seq = request.expected_seq.0,
            epoch = epoch.0,
            tokeira.storage_operation = "delete_run_for_bundle",
            tokeira.dsql_class = "commit",
            tokeira.bundle_id = execution_home_bundle.0,
        );
        async move {
            record_dsql_operation!(self, "delete_run_for_bundle", Some(execution_home_bundle), {
                convert::i64_from_u64(request.expected_seq.0, "delete expected transition_seq")?;
                if epoch != ShardEpoch::ZERO {
                    convert::i64_from_u64(epoch.0, "caller shard epoch")?;
                }

                let mut permit = self.director.acquire(DbClass::Commit).await?;
                let mut tx = permit.connection()?.begin().await?;

                if epoch != ShardEpoch::ZERO {
                    let row = sqlx::query_as::<_, (i64,)>(
                        "SELECT epoch FROM shard_lease WHERE shard_id = $1",
                    )
                    .bind(Self::shard_id_to_uuid(execution_home_bundle))
                    .fetch_optional(&mut *tx)
                    .await?;
                    let Some((durable_epoch,)) = row else {
                        tx.rollback().await?;
                        return Ok(DeleteRunResult::Conflict {
                            reason: format!(
                                "no active lease for execution-home bundle {execution_home_bundle:?} at epoch {epoch:?}"
                            ),
                        });
                    };
                    if durable_epoch != convert::i64_from_u64(epoch.0, "caller shard epoch")? {
                        tx.rollback().await?;
                        return Ok(DeleteRunResult::Conflict {
                            reason: format!(
                                "stale shard epoch {epoch:?} for execution-home bundle {execution_home_bundle:?}; current {durable_epoch}"
                            ),
                        });
                    }
                }

                let row = sqlx::query_as::<_, (i64, Option<i64>, Vec<u8>)>(
                    "SELECT transition_seq, history_size_bytes, state_data
                     FROM workflow_hot
                     WHERE run_key = $1
                     FOR UPDATE",
                )
                .bind(run_key.0)
                .fetch_optional(&mut *tx)
                .await?;
                let Some((durable_seq, history_size_bytes, state_data)) = row else {
                    tx.rollback().await?;
                    return Ok(DeleteRunResult::NotFound);
                };
                let state = codec::decode_workflow_state(run_key, &state_data)?;
                let durable_seq = TransitionSeq(convert::u64_from_i64(
                    durable_seq,
                    "workflow_hot.transition_seq",
                )?);
                let derived_bundle = tokeira_types::execution_home_bundle(
                    state.namespace_id.0.as_bytes(),
                    state.workflow_id.0.as_bytes(),
                    self.shard_count,
                );
                if derived_bundle != execution_home_bundle {
                    tx.rollback().await?;
                    return Ok(DeleteRunResult::Conflict {
                        reason: format!(
                            "execution-home bundle mismatch for {run_key:?}: expected {derived_bundle:?}, got {execution_home_bundle:?}"
                        ),
                    });
                }
                if durable_seq != request.expected_seq || state.transition_seq != durable_seq {
                    tx.rollback().await?;
                    return Ok(DeleteRunResult::Conflict {
                        reason: format!(
                            "expected seq {:?}, found durable {:?} / state {:?}",
                            request.expected_seq, durable_seq, state.transition_seq
                        ),
                    });
                }
                if state.status.is_open() {
                    tx.rollback().await?;
                    return Ok(DeleteRunResult::Conflict {
                        reason: "workflow must be closed before authoritative deletion".to_owned(),
                    });
                }

                let tombstone_seq = durable_seq.next();
                let mut tombstone_state = state.clone();
                tombstone_state.transition_seq = tombstone_seq;
                let tombstone = ProjectionRecord {
                    partition_id: partition_for(run_key, self.projection_partition_count),
                    fanout: u16::try_from(PROJECTION_FANOUT)?,
                    run_key,
                    transition_seq: tombstone_seq,
                    context: deleted_workflow_projection_context(
                        &tombstone_state,
                        request.deleted_at,
                        history_size_bytes.unwrap_or(0),
                    )?,
                };

                // Temporal deletes visibility/current/mutable/history in that
                // order, each stage safe to retry
                // (`service/history/shard/context_impl.go:941-963 @ v1.31.0`).
                // This transaction is the first three stages at once; the
                // purge is the fourth, history last.
                sqlx::query(
                    "INSERT INTO projection_log
                     (partition_id, fanout, run_key, transition_seq, context_data, ops_data, created_at)
                     VALUES ($1, $2, $3, $4, $5, $6, now())",
                )
                .bind(i32::try_from(tombstone.partition_id)?)
                .bind(PROJECTION_FANOUT)
                .bind(run_key.0)
                .bind(convert::i64_from_u64(
                    tombstone_seq.0,
                    "delete tombstone transition_seq",
                )?)
                .bind(codec::encode_projection_context(&tombstone.context)?)
                .bind(codec::LEGACY_EMPTY_PROJECTION_OPS_DATA)
                .execute(&mut *tx)
                .await?;

                let current_key =
                    Self::current_execution_key(state.namespace_id, &state.workflow_id);
                sqlx::query(CURRENT_EXECUTION_DELETE_STATEMENT)
                    .bind(current_key)
                    .bind(run_key.0)
                    .execute(&mut *tx)
                    .await?;

                for statement in FIRST_TRANSACTION_DELETE_STATEMENTS {
                    sqlx::query(statement)
                        .bind(run_key.0)
                        .execute(&mut *tx)
                        .await?;
                }
                sqlx::query(RECORD_PURGE_STATEMENT)
                    .bind(run_key.0)
                    .bind(Self::shard_id_to_uuid(execution_home_bundle))
                    .bind(crate::BulkWritePhase::Purging.to_db_smallint())
                    .execute(&mut *tx)
                    .await?;

                match tx.commit().await {
                    Ok(()) => Ok(DeleteRunResult::Deleted { tombstone }),
                    Err(error) if Self::is_serialization_failure(&error) => {
                        Ok(DeleteRunResult::Conflict {
                            reason: "DSQL serialization conflict".to_owned(),
                        })
                    }
                    Err(error) => Err(error.into()),
                }
            })
        }
        .instrument(span)
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::{CURRENT_EXECUTION_DELETE_STATEMENT, FIRST_TRANSACTION_DELETE_STATEMENTS};

    #[test]
    fn current_execution_delete_is_conditional_on_pointer_and_target() {
        assert_eq!(
            CURRENT_EXECUTION_DELETE_STATEMENT,
            "DELETE FROM current_execution WHERE key = $1 AND run_key = $2"
        );
    }

    #[test]
    fn the_first_transaction_removes_the_dispatch_row_and_the_mutable_state() {
        let tables: Vec<_> = FIRST_TRANSACTION_DELETE_STATEMENTS
            .iter()
            .map(|statement| {
                statement
                    .strip_prefix("DELETE FROM ")
                    .and_then(|rest| rest.split_whitespace().next())
                    .expect("delete statement table")
            })
            .collect();
        assert_eq!(tables, ["workflow_dispatch", "workflow_hot"]);
    }
}
