use super::*;

impl DsqlRunRepository {
    #[instrument(name = "dsql.list_due_timers", skip(self), fields(limit))]
    pub(super) async fn do_list_due_timers(
        &self,
        now: OffsetDateTime,
        limit: usize,
    ) -> Result<Vec<DueTimer>> {
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
                    self.do_list_due_timers_for_shard(ShardId(shard_index), now, None, remaining)
                        .await?,
                );
            }
            due.truncate(limit);
            metrics::record_dsql_rows_read("list_due_timers", due.len());
            Ok(due)
        })
    }

    #[instrument(name = "dsql.list_due_timers_for_shard", skip(self), fields(shard_id = shard_id.0, limit))]
    pub(super) async fn do_list_due_timers_for_shard(
        &self,
        shard_id: ShardId,
        now: OffsetDateTime,
        after: Option<&DueTimer>,
        limit: usize,
    ) -> Result<Vec<DueTimer>> {
        record_dsql_operation!(self, "list_due_timers_for_shard", Some(shard_id), {
            if limit == 0 {
                metrics::record_dsql_rows_read("list_due_timers_for_shard", 0);
                return Ok(Vec::new());
            }

            let mut permit = self.director.acquire(DbClass::Read).await?;
            // Ordered by `timer_bucket`'s primary key after `shard_id`, so the
            // keyset resume below is an index range scan.
            let rows = match after {
                None => {
                    sqlx::query_as::<_, (Uuid, String, OffsetDateTime)>(
                        "SELECT run_key, timer_id, fire_at
                 FROM timer_bucket
                 WHERE shard_id = $1 AND fire_at <= $2
                 ORDER BY fire_at ASC, run_key ASC, timer_id ASC
                 LIMIT $3",
                    )
                    .bind(Self::shard_id_to_uuid(shard_id))
                    .bind(now)
                    .bind(i64::try_from(limit)?)
                    .fetch_all(permit.connection()?)
                    .await?
                }
                Some(after) => {
                    sqlx::query_as::<_, (Uuid, String, OffsetDateTime)>(
                        "SELECT run_key, timer_id, fire_at
                 FROM timer_bucket
                 WHERE shard_id = $1 AND fire_at <= $2
                   AND (fire_at > $3
                        OR (fire_at = $3
                            AND (run_key > $4 OR (run_key = $4 AND timer_id > $5))))
                 ORDER BY fire_at ASC, run_key ASC, timer_id ASC
                 LIMIT $6",
                    )
                    .bind(Self::shard_id_to_uuid(shard_id))
                    .bind(now)
                    .bind(after.fire_at)
                    .bind(after.run_key.0)
                    .bind(&after.timer_id)
                    .bind(i64::try_from(limit)?)
                    .fetch_all(permit.connection()?)
                    .await?
                }
            };
            metrics::record_dsql_rows_read("list_due_timers_for_shard", rows.len());

            Ok(rows
                .into_iter()
                .map(|(run_key, timer_id, fire_at)| DueTimer {
                    run_key: RunKey(run_key),
                    timer_id,
                    fire_at,
                })
                .collect())
        })
    }

    #[instrument(name = "dsql.delete_due_timer_if_matches", skip(self, timer), fields(run_key = %timer.run_key.0, timer_id = %timer.timer_id))]
    pub(super) async fn do_delete_due_timer_if_matches(
        &self,
        timer: &DueTimer,
        reason: crate::StaleTimer,
    ) -> Result<bool> {
        record_dsql_operation!(self, "delete_due_timer_if_matches", None, {
            let mut permit = self.director.acquire(DbClass::Commit).await?;
            let mut tx = permit.connection()?.begin().await?;
            if reason == crate::StaleTimer::RunMissing {
                // A reset's successor holds its timer rows before its final
                // transaction makes it visible (`bounded-bulk-writes`). Plain
                // reads suffice, in this transaction's one snapshot: if the
                // final transaction committed before it, the run has mutable
                // state, and if not, it has its materializing record. The row
                // stays either way; only a run with neither loses it.
                let (kept,) = sqlx::query_as::<_, (bool,)>(
                    "SELECT EXISTS (SELECT 1 FROM workflow_hot WHERE run_key = $1)
                         OR EXISTS (SELECT 1 FROM run_bulk_write
                                    WHERE run_key = $1 AND phase = $2)",
                )
                .bind(timer.run_key.0)
                .bind(crate::BulkWritePhase::Materializing.to_db_smallint())
                .fetch_one(&mut *tx)
                .await?;
                if kept {
                    tx.rollback().await?;
                    return Ok(false);
                }
            }
            // `fire_at` is part of the row's primary key, so matching it
            // deletes exactly the row the scan read: a later timer that
            // reuses the id has a different fire time and survives.
            let result = sqlx::query(
                "DELETE FROM timer_bucket
             WHERE run_key = $1 AND timer_id = $2 AND fire_at = $3",
            )
            .bind(timer.run_key.0)
            .bind(&timer.timer_id)
            .bind(timer.fire_at)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok(result.rows_affected() > 0)
        })
    }
}
