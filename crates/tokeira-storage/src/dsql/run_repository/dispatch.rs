use super::*;

const BACKLOG_STATS_BY_PRIORITY_SQL: &str = "
    SELECT priority_key, COUNT(*), MIN(scheduled_at)
    FROM dispatch_backlog
    WHERE queue_namespace = $1
      AND queue_name = $2
      AND task_kind = $3
      AND deployment IS NOT DISTINCT FROM $4
      AND build_id IS NOT DISTINCT FROM $5
    GROUP BY priority_key";

const DRAIN_BACKLOG_SQL: &str = "
    SELECT key, run_key, payload_data, scheduled_at, priority_key, fair_pass,
           insertion_tie, task_kind, deployment, build_id
    FROM dispatch_backlog
    WHERE queue_namespace = $1
      AND queue_name = $2
      AND task_kind = $3
      AND deployment IS NOT DISTINCT FROM $4
      AND build_id IS NOT DISTINCT FROM $5
    ORDER BY priority_key ASC, fair_pass ASC, insertion_tie ASC
    LIMIT $6";

impl DsqlRunRepository {
    pub(super) async fn do_backlog_stats_by_priority(
        &self,
        queue: &QueueKey,
    ) -> Result<std::collections::BTreeMap<i16, crate::BacklogBandStats>> {
        record_dsql_operation!(self, "backlog_stats_by_priority", None, {
            let mut permit = self.director.acquire(DbClass::Read).await?;
            let deployment = queue.deployment.as_ref().map(|value| value.0.as_str());
            let build_id = queue.build_id.as_ref().map(|value| value.0.as_str());
            let rows =
                sqlx::query_as::<_, (i16, i64, OffsetDateTime)>(BACKLOG_STATS_BY_PRIORITY_SQL)
                    .bind(queue.namespace_id.0)
                    .bind(&queue.task_queue.0)
                    .bind(queue.task_kind.to_db_smallint())
                    .bind(deployment)
                    .bind(build_id)
                    .fetch_all(permit.connection()?)
                    .await?;
            rows.into_iter()
                .map(|(priority_key, count, oldest_scheduled_at)| {
                    Ok((
                        priority_key,
                        crate::BacklogBandStats {
                            count: usize::try_from(count)?,
                            oldest_scheduled_at,
                        },
                    ))
                })
                .collect()
        })
    }

    #[instrument(name = "dsql.list_dispatchable_workflow_tasks", skip(self), fields(namespace_id = %queue.namespace_id.0, task_queue = %queue.task_queue.0, limit))]
    pub(super) async fn do_list_dispatchable_workflow_tasks(
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

    pub(super) async fn do_persist_to_backlog(&self, entries: Vec<BacklogEntry>) -> Result<()> {
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
                    entry.run_key,
                    &entry.payload,
                );
                sqlx::query(
                "INSERT INTO dispatch_backlog
                 (key, partition_id, queue_namespace, queue_name, task_kind, deployment, build_id, priority_key, fair_pass, insertion_tie, run_key, payload_data, scheduled_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
            )
            .bind(key)
            .bind(i32::try_from(partition_id)?)
            .bind(entry.queue.namespace_id.0)
            .bind(&entry.queue.task_queue.0)
            .bind(entry.queue.task_kind.to_db_smallint())
            .bind(deployment)
            .bind(build_id)
            .bind(entry.order.priority_key)
            .bind(entry.order.fair_pass)
            .bind(convert::i64_from_u64(
                entry.order.insertion_tie,
                "dispatch_backlog.insertion_tie",
            )?)
            .bind(entry.run_key.0)
            .bind(codec::encode_backlog_payload(
                &entry.payload,
                entry.priority.as_ref(),
            )?)
            .bind(entry.scheduled_at)
            .execute(&mut *tx)
            .await?;
            }
            tx.commit().await?;
            metrics::record_dsql_rows_written("persist_to_backlog", row_count);
            Ok(())
        })
    }

    pub(super) async fn do_drain_backlog(
        &self,
        queue: &QueueKey,
        limit: usize,
    ) -> Result<Vec<BacklogEntry>> {
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
            let rows = sqlx::query_as::<
                _,
                (
                    Uuid,
                    Uuid,
                    Vec<u8>,
                    OffsetDateTime,
                    i16,
                    i64,
                    i64,
                    i16,
                    Option<String>,
                    Option<String>,
                ),
            >(DRAIN_BACKLOG_SQL)
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
                priority_key,
                fair_pass,
                insertion_tie,
                task_kind_raw,
                stored_deployment,
                stored_build_id,
            ) in rows
            {
                sqlx::query("DELETE FROM dispatch_backlog WHERE key = $1")
                    .bind(key)
                    .execute(&mut *tx)
                    .await?;
                let decoded = codec::decode_backlog_payload(&payload_data)?;
                drained.push(BacklogEntry {
                    run_key: RunKey(run_key),
                    queue: QueueKey {
                        namespace_id: queue.namespace_id,
                        task_queue: queue.task_queue.clone(),
                        task_kind: TaskKind::try_from(task_kind_raw)?,
                        deployment: stored_deployment.map(DeploymentId),
                        build_id: stored_build_id.map(BuildId),
                    },
                    payload: decoded.0,
                    priority: decoded.1,
                    scheduled_at,
                    order: DeliveryOrder {
                        priority_key,
                        fair_pass,
                        insertion_tie: convert::u64_from_i64(
                            insertion_tie,
                            "dispatch_backlog.insertion_tie",
                        )?,
                    },
                });
            }
            tx.commit().await?;
            metrics::record_dsql_rows_written("drain_backlog", drained.len() as u64);
            Ok(drained)
        })
    }

    #[instrument(name = "dsql.list_dispatchable_workflow_tasks_for_shard", skip(self), fields(shard_id = shard_id.0, limit))]
    pub(super) async fn do_list_dispatchable_workflow_tasks_for_shard(
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
}

pub(super) fn collect_dispatchable_workflow_tasks(
    rows: Vec<(Uuid, Vec<u8>)>,
    queue_filter: Option<&QueueKey>,
    limit: usize,
) -> Result<Vec<DispatchableWorkflowTask>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let mut tasks = Vec::new();
    for (run_key, state_data) in rows {
        // Workflow task dispatch is derived from the hot state snapshot. There
        // is no separate workflow-task queue table to repair; replaying history
        // can rebuild this materialization.
        let state = codec::decode_workflow_state(&state_data)?;
        let Some(task) = dispatchable_workflow_task(&state) else {
            continue;
        };
        let scan_queue = task.normal_queue.as_ref().unwrap_or(&task.queue);
        if queue_filter.is_some_and(|filter| filter != scan_queue) {
            continue;
        }
        debug_assert_eq!(task.run_key, RunKey(run_key));
        tasks.push(task);
        if tasks.len() == limit {
            break;
        }
    }
    Ok(tasks)
}

#[cfg(test)]
mod tests {
    use super::{BACKLOG_STATS_BY_PRIORITY_SQL, DRAIN_BACKLOG_SQL};

    #[test]
    fn priority_backlog_queries_keep_grouping_and_order_shape() {
        assert!(BACKLOG_STATS_BY_PRIORITY_SQL.contains("GROUP BY priority_key"));
        assert!(
            DRAIN_BACKLOG_SQL
                .contains("ORDER BY priority_key ASC, fair_pass ASC, insertion_tie ASC")
        );
    }
}
