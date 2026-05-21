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
                    self.do_list_due_timers_for_shard(ShardId(shard_index), now, remaining)
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
}
