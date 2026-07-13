use super::*;

impl DsqlRunRepository {
    #[instrument(name = "dsql.resolve_execution", skip(self), fields(namespace_id = %execution.namespace_id.0, workflow_id = %execution.workflow_id.0))]
    pub(super) async fn do_resolve_execution(
        &self,
        execution: &ExecutionRef,
    ) -> Result<Option<RunKey>> {
        record_dsql_operation!(self, "resolve_execution", None, {
            let mut permit = self.director.acquire(DbClass::Read).await?;
            if let Some(requested_run_id) = execution.run_id {
                // Explicit run IDs do not use `current_execution`; that row is
                // intentionally replaced by newer runs of the same workflow.
                let run_key = RunKey::derive(
                    execution.namespace_id,
                    &execution.workflow_id,
                    requested_run_id,
                );
                let row =
                    sqlx::query_as::<_, (i32,)>("SELECT 1 FROM workflow_hot WHERE run_key = $1")
                        .bind(run_key.0)
                        .fetch_optional(permit.connection()?)
                        .await?;
                return Ok(row.map(|_| run_key));
            }

            let key = Self::current_execution_key(execution.namespace_id, &execution.workflow_id);
            let row = sqlx::query_as::<_, (Uuid,)>(
                "SELECT run_key FROM current_execution
             WHERE key = $1 AND is_open = true",
            )
            .bind(key)
            .fetch_optional(permit.connection()?)
            .await?;
            Ok(row.map(|(run_key,)| RunKey(run_key)))
        })
    }

    #[instrument(name = "dsql.find_latest_run", skip(self), fields(namespace_id = %namespace_id.0, workflow_id = %workflow_id.0))]
    pub(super) async fn do_find_latest_run(
        &self,
        namespace_id: NamespaceId,
        workflow_id: &WorkflowId,
    ) -> Result<Option<RunKey>> {
        record_dsql_operation!(self, "find_latest_run", None, {
            let mut permit = self.director.acquire(DbClass::Read).await?;
            let key = Self::current_execution_key(namespace_id, workflow_id);
            // `current_execution` is the latest-run pointer even after the run is
            // closed. Only `resolve_execution(None)` filters this row to open runs.
            let row = sqlx::query_as::<_, (Uuid,)>(
                "SELECT run_key FROM current_execution
             WHERE key = $1",
            )
            .bind(key)
            .fetch_optional(permit.connection()?)
            .await?;
            Ok(row.map(|(run_key,)| RunKey(run_key)))
        })
    }

    #[instrument(name = "dsql.list_runs_for_namespace", skip(self), fields(namespace_id = %namespace_id.0))]
    pub(super) async fn do_list_runs_for_namespace(
        &self,
        namespace_id: NamespaceId,
    ) -> Result<Vec<RunKey>> {
        record_dsql_operation!(self, "list_runs_for_namespace", None, {
            let mut permit = self.director.acquire(DbClass::Read).await?;
            // Namespace reclaim reads the authoritative hot table rather than
            // visibility: deletion must also find runs whose projection row is
            // delayed or missing (`deleteWorkflowExecutions`,
            // `service/worker/deletenamespace/reclaimresources/workflow.go @ v1.31.0`).
            let rows = sqlx::query_as::<_, (Uuid,)>(
                "SELECT run_key FROM workflow_hot
                 WHERE namespace_id = $1
                 ORDER BY run_key ASC",
            )
            .bind(namespace_id.0)
            .fetch_all(permit.connection()?)
            .await?;
            metrics::record_dsql_rows_read("list_runs_for_namespace", rows.len());
            Ok(rows.into_iter().map(|(run_key,)| RunKey(run_key)).collect())
        })
    }

    #[instrument(name = "dsql.load_run", skip(self), fields(run_key = %run_key.0))]
    pub(super) async fn do_load_run(&self, run_key: RunKey) -> Result<LoadedRun> {
        record_dsql_operation!(self, "load_run", Some(self.shard_for_run_key(run_key)), {
            let mut permit = self.director.acquire(DbClass::Read).await?;
            let row = sqlx::query_as::<_, (Vec<u8>,)>(
                "SELECT state_data FROM workflow_hot WHERE run_key = $1",
            )
            .bind(run_key.0)
            .fetch_optional(permit.connection()?)
            .await?;
            match row {
                Some((state_data,)) => Ok(LoadedRun::Existing(codec::decode_workflow_state(
                    &state_data,
                )?)),
                None => Ok(LoadedRun::Absent),
            }
        })
    }

    #[instrument(name = "dsql.read_history", skip(self), fields(run_key = %run_key.0, after_event_id, limit))]
    pub(super) async fn do_read_history(
        &self,
        run_key: RunKey,
        after_event_id: i64,
        limit: usize,
    ) -> Result<Vec<HistoryEvent>> {
        let result = record_dsql_operation!(
            self,
            "read_history",
            Some(self.shard_for_run_key(run_key)),
            {
                let effective_limit = effective_history_limit(limit);
                if effective_limit == 0 {
                    metrics::record_dsql_rows_read("read_history", 0);
                    return Ok(Vec::new());
                }

                let mut permit = self.director.acquire(DbClass::Read).await?;
                // History is stored in transition batches. A batch may straddle
                // `after_event_id`, so decoding and per-event filtering remain in Rust.
                let rows = sqlx::query_as::<_, (i64, i64, Vec<u8>)>(
                    "SELECT first_event_id, last_event_id, events_data
             FROM history_batch
             WHERE run_key = $1 AND last_event_id > $2
             ORDER BY first_event_id ASC",
                )
                .bind(run_key.0)
                .bind(after_event_id)
                .fetch_all(permit.connection()?)
                .await?;
                metrics::record_dsql_rows_read("read_history", rows.len());

                let mut events = Vec::new();
                for (_first_event_id, _last_event_id, events_data) in rows {
                    for event in codec::decode_history_events(&events_data)? {
                        if event.event_id <= after_event_id {
                            continue;
                        }
                        events.push(event);
                        if events.len() == effective_limit {
                            return Ok(events);
                        }
                    }
                }
                Ok(events)
            }
        );
        if let Ok(events) = &result {
            metrics::record_read_history_events(events.len());
        }
        result
    }

    #[instrument(name = "dsql.lookup_request_dedupe", skip(self), fields(namespace_id = %execution.namespace_id.0, workflow_id = %execution.workflow_id.0, request_id = %request_id.0))]
    pub(super) async fn do_lookup_request_dedupe(
        &self,
        execution: &ExecutionRef,
        request_id: &RequestId,
    ) -> Result<Option<RequestRecord>> {
        record_dsql_operation!(self, "lookup_request_dedupe", None, {
            let mut permit = self.director.acquire(DbClass::Read).await?;
            let key = Self::request_dedupe_key(
                execution.namespace_id,
                &execution.workflow_id,
                request_id,
            );
            let row = sqlx::query_as::<_, (Uuid, String, i64, Uuid)>(
                "SELECT run_key, request_id, first_seen_transition_seq, run_id
             FROM request_dedupe
             WHERE key = $1",
            )
            .bind(key)
            .fetch_optional(permit.connection()?)
            .await?;

            let Some((run_key, stored_request_id, transition_seq, stored_run_id)) = row else {
                return Ok(None);
            };
            let run_id = RunId(stored_run_id);
            // A workflow-scoped dedupe key can still be queried through an
            // execution reference that names a specific run. In that case the
            // stored run must match the caller's run filter.
            if execution
                .run_id
                .is_some_and(|requested| requested != run_id)
            {
                return Ok(None);
            }

            Ok(Some(RequestRecord {
                namespace_id: execution.namespace_id,
                workflow_id: execution.workflow_id.clone(),
                run_id,
                run_key: RunKey(run_key),
                request_id: RequestId(stored_request_id),
                first_seen_transition_seq: TransitionSeq(convert::u64_from_i64(
                    transition_seq,
                    "request_dedupe.first_seen_transition_seq",
                )?),
            }))
        })
    }

    #[instrument(name = "dsql.read_transition_audit", skip(self), fields(run_key = %run_key.0))]
    pub(super) async fn do_read_transition_audit(
        &self,
        run_key: RunKey,
    ) -> Result<Vec<TransitionAuditRecord>> {
        record_dsql_operation!(
            self,
            "read_transition_audit",
            Some(self.shard_for_run_key(run_key)),
            {
                let mut permit = self.director.acquire(DbClass::Read).await?;
                let rows = sqlx::query_as::<_, (i64, Vec<u8>)>(
                    "SELECT transition_seq, events_data FROM history_batch
             WHERE run_key = $1 ORDER BY first_event_id ASC",
                )
                .bind(run_key.0)
                .fetch_all(permit.connection()?)
                .await?;

                rows.into_iter()
                    .map(|(transition_seq, events_data)| {
                        Ok(TransitionAuditRecord {
                            run_key,
                            transition_seq: TransitionSeq(convert::u64_from_i64(
                                transition_seq,
                                "history_batch.transition_seq",
                            )?),
                            history_events: codec::decode_history_events(&events_data)?,
                            activity_ops: Vec::new(),
                            timer_ops: Vec::new(),
                            dispatch_ops: Vec::new(),
                            projection_ops: Vec::new(),
                        })
                    })
                    .collect()
            }
        )
    }

    #[instrument(name = "dsql.materialize_reset_successor", skip(self), fields(base_run_key = %base_run_key.0, fork_event_id, successor_run_id = %successor_run_id.0))]
    pub(super) async fn do_materialize_reset_successor(
        &self,
        base_run_key: RunKey,
        fork_event_id: i64,
        successor_run_id: RunId,
    ) -> Result<()> {
        record_dsql_operation!(
            self,
            "materialize_reset_successor",
            Some(self.shard_for_run_key(base_run_key)),
            {
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
                let base_state = codec::decode_workflow_state(&base_state_data)?;
                let successor_run_key = RunKey::derive(
                    base_state.namespace_id,
                    &base_state.workflow_id,
                    successor_run_id,
                );

                let history_rows = sqlx::query_as::<_, (Vec<u8>, i64, i64)>(
                    "SELECT events_data, first_event_id, last_event_id FROM history_batch
             WHERE run_key = $1 ORDER BY first_event_id ASC",
                )
                .bind(base_run_key.0)
                .fetch_all(&mut *tx)
                .await?;
                let mut copied_events = Vec::new();
                let mut found_fork = false;
                // Reset materialization copies only the committed prefix BEFORE the
                // fork event: the fork is the WFT-FINISH event being reset, and
                // v1.31.0 rebuilds mutable state to `WorkflowTaskFinishEventId - 1`
                // (`baseRebuildLastEventID`, resetworkflow/api.go:119 @ v1.31.0).
                // Replay then derives the successor state — ending with the reset
                // WFT still started, ready for the ResetWorkflow failure — avoiding
                // a second source of truth for reset snapshots.
                for (events_data, _first_event_id, _last_event_id) in history_rows {
                    for event in codec::decode_history_events(&events_data)? {
                        if event.event_id == fork_event_id {
                            found_fork = true;
                            break;
                        }
                        copied_events.push(event);
                    }
                    if found_fork {
                        break;
                    }
                }
                if !found_fork {
                    tx.rollback().await?;
                    bail!(
                        "fork_event_id {} outside committed history for {:?}",
                        fork_event_id,
                        base_run_key
                    );
                }

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
                let successor_shard = self.shard_for_run_key(successor_run_key);
                crate::dsql::run_repository::commit::insert_workflow_hot(
                    &mut tx,
                    successor_run_key,
                    successor_shard,
                    &successor_state,
                )
                .await?;
                crate::dsql::run_repository::commit::insert_history_batch(
                    &mut tx,
                    successor_run_key,
                    successor_state.transition_seq,
                    &copied_events,
                )
                .await?;
                crate::dsql::run_repository::commit::upsert_current_execution_start(
                    &mut tx,
                    successor_run_key,
                    &successor_state,
                )
                .await?;
                for activity in successor_state.activities.values() {
                    crate::dsql::run_repository::commit::upsert_activity(
                        &mut tx,
                        successor_run_key,
                        successor_shard,
                        successor_state.namespace_id,
                        activity,
                    )
                    .await?;
                }
                for timer in successor_state.timers.values() {
                    crate::dsql::run_repository::commit::upsert_timer(
                        &mut tx,
                        successor_run_key,
                        successor_shard,
                        timer,
                    )
                    .await?;
                }
                tx.commit().await?;
                Ok(())
            }
        )
    }
}
