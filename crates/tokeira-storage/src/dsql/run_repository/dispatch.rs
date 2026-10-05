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

/// Inserts one backlog entry. `key` derives from the entry's backlog identity, so an
/// entry whose identity is already stored keeps the stored row instead of failing the
/// batch (`runtime-durable-backlog` criterion 3.8).
const PERSIST_BACKLOG_SQL: &str = "
    INSERT INTO dispatch_backlog
      (key, partition_id, queue_namespace, queue_name, task_kind, deployment, build_id,
       priority_key, fair_pass, insertion_tie, run_key, payload_data, scheduled_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
    ON CONFLICT (key) DO NOTHING";

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
    pub(super) async fn do_list_versioned_backlog_queue_keys(&self) -> Result<Vec<QueueKey>> {
        record_dsql_operation!(self, "list_versioned_backlog_queue_keys", None, {
            let mut permit = self.director.acquire(DbClass::Read).await?;
            let rows = sqlx::query_as::<_, (Uuid, String, i16, String, String)>(
                "SELECT DISTINCT queue_namespace, queue_name, task_kind, deployment, build_id
                 FROM dispatch_backlog
                 WHERE deployment IS NOT NULL
                   AND build_id IS NOT NULL
                 ORDER BY queue_namespace, queue_name, task_kind, deployment, build_id",
            )
            .fetch_all(permit.connection()?)
            .await?;
            metrics::record_dsql_rows_read("list_versioned_backlog_queue_keys", rows.len());
            rows.into_iter()
                .map(
                    |(namespace_id, task_queue, task_kind, deployment, build_id)| {
                        Ok(QueueKey {
                            namespace_id: NamespaceId(namespace_id),
                            task_queue: TaskQueueName(task_queue),
                            task_kind: TaskKind::try_from(task_kind)?,
                            deployment: Some(DeploymentId(deployment)),
                            build_id: Some(BuildId(build_id)),
                        })
                    },
                )
                .collect()
        })
    }

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

            let mut rows_written = 0;
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
                let inserted = sqlx::query(PERSIST_BACKLOG_SQL)
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
                rows_written += inserted.rows_affected();
            }
            tx.commit().await?;
            metrics::record_dsql_rows_written("persist_to_backlog", rows_written);
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
        let state = codec::decode_workflow_state(RunKey(run_key), &state_data)?;
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
    use time::OffsetDateTime;
    use tokeira_types::{
        ids::{LogicalTaskSeq, NamespaceId, RunKey},
        payload::Payloads,
        task_queue::{BuildId, DeploymentId, QueueKey, TaskKind, TaskQueueName},
    };
    use uuid::Uuid;

    use super::{
        BACKLOG_STATS_BY_PRIORITY_SQL, DRAIN_BACKLOG_SQL, DsqlRunRepository, PERSIST_BACKLOG_SQL,
        partition_for,
    };
    use crate::api::{BacklogEntry, BacklogPayload, DeliveryOrder};

    #[test]
    fn priority_backlog_queries_keep_grouping_and_order_shape() {
        assert!(BACKLOG_STATS_BY_PRIORITY_SQL.contains("GROUP BY priority_key"));
        assert!(
            DRAIN_BACKLOG_SQL
                .contains("ORDER BY priority_key ASC, fair_pass ASC, insertion_tie ASC")
        );
    }

    #[test]
    fn backlog_insert_keeps_the_stored_row_for_a_known_identity() {
        assert!(PERSIST_BACKLOG_SQL.contains("INSERT INTO dispatch_backlog"));
        assert!(
            PERSIST_BACKLOG_SQL
                .trim_end()
                .ends_with("ON CONFLICT (key) DO NOTHING")
        );
    }

    /// Every pair from a set of entries that varies each identity field, and fields
    /// outside the identity, has equal backlog identities exactly when DSQL derives
    /// equal row keys. So both stores agree on which entries are duplicates.
    #[test]
    fn backlog_identity_and_row_key_agree_on_duplicates() {
        let keyed: Vec<_> = backlog_entries()
            .into_iter()
            .map(|entry| (entry.identity(), row_key(&entry), entry))
            .collect();
        for (identity_a, key_a, a) in &keyed {
            for (identity_b, key_b, b) in &keyed {
                assert_eq!(identity_a == identity_b, key_a == key_b, "{a:?}\n{b:?}");
            }
        }
    }

    /// DSQL's row key for `entry`, derived as `do_persist_to_backlog` derives it.
    fn row_key(entry: &BacklogEntry) -> Uuid {
        DsqlRunRepository::dispatch_backlog_key(
            partition_for(entry.run_key, 8),
            entry.queue.namespace_id,
            &entry.queue.task_queue.0,
            entry.queue.task_kind,
            entry
                .queue
                .deployment
                .as_ref()
                .map(|value| value.0.as_str()),
            entry.queue.build_id.as_ref().map(|value| value.0.as_str()),
            entry.run_key,
            &entry.payload,
        )
    }

    fn backlog_entries() -> Vec<BacklogEntry> {
        let namespace_id = NamespaceId(Uuid::from_u128(1));
        let queue = |name: &str, task_kind, versioned: bool| QueueKey {
            namespace_id,
            task_queue: TaskQueueName(name.into()),
            task_kind,
            deployment: versioned.then(|| DeploymentId("deployment".into())),
            build_id: versioned.then(|| BuildId("build".into())),
        };
        let queues = [
            queue("orders", TaskKind::Workflow, false),
            queue("orders", TaskKind::Workflow, true),
            queue("orders", TaskKind::Activity, false),
            queue("billing", TaskKind::Workflow, false),
        ];
        let runs = [RunKey(Uuid::from_u128(10)), RunKey(Uuid::from_u128(11))];
        let mut entries = Vec::new();
        for queue in &queues {
            for run_key in runs {
                // `variant` changes only fields outside the identity.
                for variant in 0..2 {
                    for payload in payloads(variant) {
                        entries.push(BacklogEntry {
                            run_key,
                            queue: queue.clone(),
                            payload,
                            priority: None,
                            scheduled_at: OffsetDateTime::UNIX_EPOCH
                                + time::Duration::seconds(variant),
                            order: DeliveryOrder {
                                priority_key: 3,
                                fair_pass: variant,
                                insertion_tie: variant.unsigned_abs(),
                            },
                        });
                    }
                }
            }
        }
        entries
    }

    fn payloads(variant: i64) -> Vec<BacklogPayload> {
        let activity = |activity_id: &str, attempt, stamp| BacklogPayload::Activity {
            activity_id: activity_id.into(),
            input: Payloads::default(),
            schedule_event_id: 7 + variant,
            attempt,
            dispatch_revision: variant,
            stamp,
        };
        vec![
            BacklogPayload::Workflow {
                logical_seq: LogicalTaskSeq(1),
            },
            BacklogPayload::Workflow {
                logical_seq: LogicalTaskSeq(2),
            },
            activity("a1", 1, 0),
            activity("a1", 2, 0),
            activity("a1", 1, 1),
            activity("a2", 1, 0),
        ]
    }
}
