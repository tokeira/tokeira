//! Fenced authoritative workflow-run deletion for the DSQL repository.
//!
//! Deletion is deliberately separate from the kernel transition writer: an
//! open run first closes through the ordinary lane/kernel path, then this module
//! atomically appends a visibility tombstone and purges the closed run's
//! authoritative and dispatch rows. The sequence and execution-home epoch are
//! checked inside the same transaction so a stale owner cannot partially erase
//! a run.

use super::*;

const CURRENT_EXECUTION_DELETE_STATEMENT: &str =
    "DELETE FROM current_execution WHERE key = $1 AND run_key = $2";
const RUN_OWNED_DELETE_STATEMENTS: [&str; 7] = [
    "DELETE FROM request_dedupe WHERE run_key = $1",
    "DELETE FROM activity_state WHERE run_key = $1",
    "DELETE FROM timer_bucket WHERE run_key = $1",
    "DELETE FROM activity_dispatch WHERE run_key = $1",
    "DELETE FROM dispatch_backlog WHERE run_key = $1",
    "DELETE FROM workflow_hot WHERE run_key = $1",
    // History is last so a future non-transactional backend never exposes
    // mutable state whose branch is gone.
    "DELETE FROM history_batch WHERE run_key = $1",
];

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

                let row = sqlx::query_as::<_, (i64, Vec<u8>)>(
                    "SELECT transition_seq, state_data
                     FROM workflow_hot
                     WHERE run_key = $1
                     FOR UPDATE",
                )
                .bind(run_key.0)
                .fetch_optional(&mut *tx)
                .await?;
                let Some((durable_seq, state_data)) = row else {
                    tx.rollback().await?;
                    return Ok(DeleteRunResult::NotFound);
                };
                let state = codec::decode_workflow_state(&state_data)?;
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
                    )?,
                };

                // Temporal deletes visibility/current/mutable/history in that
                // order (`service/history/shard/context_impl.go @ v1.31.0`).
                // DSQL makes the full write set atomic, while retaining the
                // same logical order and keeping history last.
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
                .bind(codec::encode_projection_ops(&[])?)
                .execute(&mut *tx)
                .await?;

                let current_key =
                    Self::current_execution_key(state.namespace_id, &state.workflow_id);
                sqlx::query(CURRENT_EXECUTION_DELETE_STATEMENT)
                    .bind(current_key)
                    .bind(run_key.0)
                    .execute(&mut *tx)
                    .await?;

                for statement in RUN_OWNED_DELETE_STATEMENTS {
                    sqlx::query(statement)
                        .bind(run_key.0)
                        .execute(&mut *tx)
                        .await?;
                }

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
    use super::{CURRENT_EXECUTION_DELETE_STATEMENT, RUN_OWNED_DELETE_STATEMENTS};

    #[test]
    fn current_execution_delete_is_conditional_on_pointer_and_target() {
        assert_eq!(
            CURRENT_EXECUTION_DELETE_STATEMENT,
            "DELETE FROM current_execution WHERE key = $1 AND run_key = $2"
        );
    }

    #[test]
    fn authoritative_delete_covers_every_run_owned_table_and_history_is_last() {
        let tables: Vec<_> = RUN_OWNED_DELETE_STATEMENTS
            .iter()
            .map(|statement| {
                statement
                    .strip_prefix("DELETE FROM ")
                    .and_then(|rest| rest.split_whitespace().next())
                    .expect("delete statement table")
            })
            .collect();

        assert_eq!(
            tables,
            [
                "request_dedupe",
                "activity_state",
                "timer_bucket",
                "activity_dispatch",
                "dispatch_backlog",
                "workflow_hot",
                "history_batch",
            ]
        );
    }
}
