use super::*;

impl DsqlRunRepository {
    #[instrument(name = "dsql.has_open_pinned_workflows", skip(self), fields(namespace_id = %namespace_id.0, deployment_name = %version.deployment_name.0, build_id = %version.build_id.0))]
    pub(super) async fn do_has_open_pinned_workflows(
        &self,
        namespace_id: NamespaceId,
        version: &WorkerDeploymentVersionKey,
    ) -> Result<bool> {
        record_dsql_operation!(self, "has_open_pinned_workflows", None, {
            let mut permit = self.director.acquire(DbClass::Read).await?;
            let rows = sqlx::query_as::<_, (Vec<u8>,)>(
                "SELECT state_data
             FROM workflow_hot
             WHERE namespace_id = $1",
            )
            .bind(namespace_id.0)
            .fetch_all(permit.connection()?)
            .await?;
            metrics::record_dsql_rows_read("has_open_pinned_workflows", rows.len());

            for (state_data,) in rows {
                let state = codec::decode_workflow_state(&state_data)?;
                if workflow_is_open_and_pinned_to_version(&state, namespace_id, version) {
                    return Ok(true);
                }
            }
            Ok(false)
        })
    }

    #[instrument(name = "dsql.list_runs_with_workflow_timeouts_for_shard", skip(self), fields(shard_id = shard_id.0, limit))]
    pub(super) async fn do_list_runs_with_workflow_timeouts_for_shard(
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
    pub(super) async fn do_list_started_workflow_tasks_for_shard(
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

    #[instrument(name = "dsql.list_pending_nexus_operations_for_shard", skip(self), fields(shard_id = shard_id.0, limit))]
    pub(super) async fn do_list_pending_nexus_operations_for_shard(
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

pub(super) fn collect_workflow_timeout_entries(
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

pub(super) fn collect_started_workflow_task_entries(
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

pub(super) fn collect_nexus_sweep_entries(
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
