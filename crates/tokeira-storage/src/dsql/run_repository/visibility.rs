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
            let rows = sqlx::query_as::<_, (Uuid, Vec<u8>)>(
                "SELECT run_key, state_data
             FROM workflow_hot
             WHERE namespace_id = $1",
            )
            .bind(namespace_id.0)
            .fetch_all(permit.connection()?)
            .await?;
            metrics::record_dsql_rows_read("has_open_pinned_workflows", rows.len());

            for (run_key, state_data) in rows {
                let state = codec::decode_workflow_state(RunKey(run_key), &state_data)?;
                if workflow_is_open_and_pinned_to_version(&state, namespace_id, version) {
                    return Ok(true);
                }
            }
            Ok(false)
        })
    }

    #[instrument(name = "dsql.list_recovery_candidates_for_shard", skip(self, cursor), fields(shard_id = shard_id.0, limit))]
    pub(super) async fn do_list_recovery_candidates_for_shard(
        &self,
        shard_id: ShardId,
        cursor: Option<&RecoveryCursor>,
        limit: usize,
    ) -> Result<RecoveryPage> {
        record_dsql_operation!(
            self,
            "list_recovery_candidates_for_shard",
            Some(shard_id),
            {
                let shard = Self::shard_id_to_uuid(shard_id);
                let (rows, next) =
                    read_candidate_page(cursor, limit, |phase, after, limit| async move {
                        let mut permit = self.director.acquire(DbClass::Read).await?;
                        let limit = i64::try_from(limit)?;
                        // Each phase is an ordered range of
                        // `idx_workflow_hot_recovery (shard_id, recovery_needed, run_key)`.
                        let rows = match (phase, after) {
                            (RecoveryPhase::Legacy, None) => {
                                sqlx::query_as::<_, (Uuid, Vec<u8>)>(
                                    "SELECT run_key, state_data FROM workflow_hot
                                 WHERE shard_id = $1 AND recovery_needed IS NULL
                                 ORDER BY run_key ASC LIMIT $2",
                                )
                                .bind(shard)
                                .bind(limit)
                                .fetch_all(permit.connection()?)
                                .await?
                            }
                            (RecoveryPhase::Legacy, Some(after)) => {
                                sqlx::query_as::<_, (Uuid, Vec<u8>)>(
                                    "SELECT run_key, state_data FROM workflow_hot
                                     WHERE shard_id = $1 AND recovery_needed IS NULL
                                       AND run_key > $2
                                     ORDER BY run_key ASC LIMIT $3",
                                )
                                .bind(shard)
                                .bind(after.0)
                                .bind(limit)
                                .fetch_all(permit.connection()?)
                                .await?
                            }
                            (RecoveryPhase::Flagged, None) => {
                                sqlx::query_as::<_, (Uuid, Vec<u8>)>(
                                    "SELECT run_key, state_data FROM workflow_hot
                                 WHERE shard_id = $1 AND recovery_needed = true
                                 ORDER BY run_key ASC LIMIT $2",
                                )
                                .bind(shard)
                                .bind(limit)
                                .fetch_all(permit.connection()?)
                                .await?
                            }
                            (RecoveryPhase::Flagged, Some(after)) => {
                                sqlx::query_as::<_, (Uuid, Vec<u8>)>(
                                    "SELECT run_key, state_data FROM workflow_hot
                                     WHERE shard_id = $1 AND recovery_needed = true
                                       AND run_key > $2
                                     ORDER BY run_key ASC LIMIT $3",
                                )
                                .bind(shard)
                                .bind(after.0)
                                .bind(limit)
                                .fetch_all(permit.connection()?)
                                .await?
                            }
                        };
                        metrics::record_dsql_rows_read(
                            "list_recovery_candidates_for_shard",
                            rows.len(),
                        );
                        rows.into_iter()
                            .map(|(run_key, state_data)| {
                                let run_key = RunKey(run_key);
                                codec::decode_workflow_state(run_key, &state_data)
                                    .map(|state| (run_key, state))
                            })
                            .collect::<Result<Vec<_>>>()
                    })
                    .await?;
                Ok(RecoveryPage {
                    states: rows.into_iter().map(|(_, state)| state).collect(),
                    next,
                })
            }
        )
    }
}
