use super::*;

pub(super) type ActivityDispatchRow = (
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

impl DsqlRunRepository {
    #[instrument(name = "dsql.list_dispatchable_activity_tasks", skip(self), fields(namespace_id = %queue.namespace_id.0, task_queue = %queue.task_queue.0, limit))]
    pub(super) async fn do_list_dispatchable_activity_tasks(
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

    #[instrument(name = "dsql.list_dispatchable_activity_tasks_for_shard", skip(self), fields(shard_id = shard_id.0, limit))]
    pub(super) async fn do_list_dispatchable_activity_tasks_for_shard(
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

    #[instrument(name = "dsql.list_open_activities_for_shard", skip(self), fields(shard_id = shard_id.0, limit))]
    pub(super) async fn do_list_open_activities_for_shard(
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
}

pub(super) fn activity_dispatch_from_row(
    row: ActivityDispatchRow,
) -> Result<DispatchableActivityTask> {
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

pub(super) fn collect_activity_sweep_entries(
    rows: Vec<(Uuid, Vec<u8>)>,
) -> Result<Vec<ActivitySweepEntry>> {
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
