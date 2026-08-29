use super::*;

impl DsqlRunRepository {
    pub(super) async fn do_commit_transition(
        &self,
        run_key: RunKey,
        transition: Transition,
        epoch: ShardEpoch,
    ) -> Result<CommitResult> {
        let shard_id = self.shard_for_run_key(run_key);
        let span = tracing::info_span!(
            "dsql.commit_transition",
            run_key = %run_key.0,
            expected_seq = transition.expected_seq.0,
            epoch = epoch.0,
            tokeira.storage_operation = "commit_transition",
            tokeira.dsql_class = "commit",
            tokeira.shard_id = shard_id.0,
        );
        async move {
            record_dsql_commit_operation!(self, "commit_transition", Some(shard_id), {
                // Validate i64 conversions before acquiring a connection or starting a
                // transaction. This prevents mid-transaction failures from overflow on
                // values that are structurally u64 but stored as BIGINT (i64) in DSQL.
                convert::i64_from_u64(transition.next_state.transition_seq.0, "transition_seq")?;
                if should_check_epoch(epoch) {
                    convert::i64_from_u64(epoch.0, "caller shard epoch")?;
                }

                let mut permit = self.director.acquire(DbClass::Commit).await?;
                let mut tx = permit.connection()?.begin().await?;
                let state = transition.next_state.clone();
                let shard_id = tokeira_types::execution_home_bundle(
                    state.namespace_id.0.as_bytes(),
                    state.workflow_id.0.as_bytes(),
                    self.shard_count,
                );
                // Commit routing is derived from the same shard_count used by the
                // runtime ShardOwner. A mismatch here would make leases and rows
                // disagree about execution-home ownership.

                if should_check_epoch(epoch) {
                    // Epoch fencing ties a commit to the lane/shard lease that produced
                    // it. A stale owner must fail before reading or writing run state.
                    let row = sqlx::query_as::<_, (i64,)>(
                        "SELECT epoch FROM shard_lease WHERE shard_id = $1",
                    )
                    .bind(Self::shard_id_to_uuid(shard_id))
                    .fetch_optional(&mut *tx)
                    .await?;
                    let Some((durable_epoch,)) = row else {
                        tx.rollback().await?;
                        return Ok(CommitResult::Conflict {
                            reason: format!(
                                "no active lease for shard {:?} at epoch {:?}",
                                shard_id, epoch
                            ),
                        });
                    };
                    if durable_epoch != convert::i64_from_u64(epoch.0, "caller shard epoch")? {
                        tx.rollback().await?;
                        return Ok(CommitResult::Conflict {
                            reason: format!(
                                "stale shard epoch {:?} for shard {:?}; current {}",
                                epoch, shard_id, durable_epoch
                            ),
                        });
                    }
                }

                let started = Instant::now();
                let row = sqlx::query_as::<_, (i64,)>(
                    "SELECT transition_seq FROM workflow_hot WHERE run_key = $1 FOR UPDATE",
                )
                .bind(run_key.0)
                .fetch_optional(&mut *tx)
                .await?;
                metrics::record_dsql_statement_duration(
                    "commit_transition",
                    "load_hot",
                    started.elapsed(),
                );
                let current_seq = match row {
                    Some((seq,)) => {
                        TransitionSeq(convert::u64_from_i64(seq, "workflow_hot.transition_seq")?)
                    }
                    None => TransitionSeq::ZERO,
                };
                // The transition sequence is the per-run OCC fence. We check it inside
                // the same transaction as the write set so successful commits remain
                // linearizable for a single run.
                if current_seq != transition.expected_seq {
                    tx.rollback().await?;
                    return Ok(CommitResult::Conflict {
                        reason: format!(
                            "expected seq {:?}, found {:?}",
                            transition.expected_seq, current_seq
                        ),
                    });
                }

                for op in &transition.request_dedupe_ops {
                    let key = Self::request_dedupe_key(
                        state.namespace_id,
                        &state.workflow_id,
                        &op.request_id,
                    );
                    let started = Instant::now();
                    let row = sqlx::query_as::<_, (Uuid,)>(
                        "SELECT run_id FROM request_dedupe
                 WHERE key = $1",
                    )
                    .bind(key)
                    .fetch_optional(&mut *tx)
                    .await?;
                    metrics::record_dsql_statement_duration(
                        "commit_transition",
                        "dedupe_check",
                        started.elapsed(),
                    );
                    // Request-id dedupe is scoped to the RUN, mirroring v1.31.0
                    // where request ids live in the run's mutable state: a
                    // request id reused against a NEW run (e.g. signal-with-start
                    // after the prior run closed) is fresh, not a duplicate
                    // (mutable_state_impl.go:2361-2398 @ v1.31.0). Dedupe is
                    // checked before any state mutation so idempotent retries
                    // short-circuit without turning into conflicts.
                    if row.is_some_and(|(run_id,)| run_id == state.run_id.0) {
                        tx.rollback().await?;
                        return Ok(CommitResult::Duplicate);
                    }
                }

                if transition.expected_seq == TransitionSeq::ZERO && state.status.is_open() {
                    let key = Self::current_execution_key(state.namespace_id, &state.workflow_id);
                    let started = Instant::now();
                    let row = sqlx::query_as::<_, (Uuid, bool)>(
                        "SELECT run_key, is_open FROM current_execution
                 WHERE key = $1",
                    )
                    .bind(key)
                    .fetch_optional(&mut *tx)
                    .await?;
                    metrics::record_dsql_statement_duration(
                        "commit_transition",
                        "current_execution_check",
                        started.elapsed(),
                    );
                    // Both Reject and AllowAfterClose reject when an open execution
                    // exists for a different run. When is_open is false under
                    // AllowAfterClose, the code intentionally falls through — the
                    // write set will replace the closed row via upsert_current_execution_start.
                    if let Some((existing_run_key, is_open)) = row
                        && is_open
                        && existing_run_key != run_key.0
                        && matches!(
                            self.conflict_policy,
                            CurrentExecutionConflictPolicy::Reject
                                | CurrentExecutionConflictPolicy::AllowAfterClose
                        )
                    {
                        tx.rollback().await?;
                        // Report the current-execution collision so the runtime
                        // applies the request's WorkflowIdConflictPolicy (Fail /
                        // UseExisting / TerminateExisting); distinct from a transient
                        // `Conflict` so the lane does not OCC-retry it. The incumbent
                        // is open here (is_open), so its status is Running; DSQL does
                        // not eagerly load the incumbent's request-id map (the
                        // already-started error's request-id detail is best-effort and
                        // can be loaded by a follow-up if needed).
                        return Ok(CommitResult::CurrentExecutionConflict {
                            existing_run_key: RunKey(existing_run_key),
                            existing_status: ExecutionStatus::Running,
                            request_ids: Vec::new(),
                        });
                    }
                }

                write_transition(
                    &mut tx,
                    run_key,
                    shard_id,
                    self.projection_partition_count,
                    &transition,
                    &state,
                )
                .await?;
                match tx.commit().await {
                    Ok(()) => {
                        metrics::record_dsql_commit_retries(0);
                        Ok(CommitResult::Applied { new_state: state })
                    }
                    // Aurora DSQL can reject a transaction at commit because another
                    // transaction won serialization. The runtime already knows how to
                    // reload and retry `Conflict`, so normalize SQLSTATE 40001 here.
                    Err(err) if Self::is_serialization_failure(&err) => {
                        Ok(CommitResult::Conflict {
                            reason: "DSQL serialization conflict".to_owned(),
                        })
                    }
                    Err(err) => {
                        tokeira_observability::mark_error_biased_sample(
                            tokeira_observability::ErrorBiasedSamplingReason::StorageCommitError,
                        );
                        Err(err.into())
                    }
                }
            })
        }
        .instrument(span)
        .await
    }

    pub(super) async fn do_commit_transition_for_bundle(
        &self,
        run_key: RunKey,
        execution_home_bundle: ShardId,
        transition: Transition,
        epoch: ShardEpoch,
    ) -> Result<CommitResult> {
        let span = tracing::info_span!(
            "dsql.commit_transition_for_bundle",
            run_key = %run_key.0,
            bundle = execution_home_bundle.0,
            expected_seq = transition.expected_seq.0,
            epoch = epoch.0,
            tokeira.storage_operation = "commit_transition_for_bundle",
            tokeira.dsql_class = "commit",
            tokeira.bundle_id = execution_home_bundle.0,
        );
        async move {
            record_dsql_commit_operation!(
                self,
                "commit_transition_for_bundle",
                Some(execution_home_bundle),
                {
                if should_check_epoch(epoch) {
                    // Multi-node/controller-managed deployments keep the
                    // durable shard_lease fence. Single-node compose passes
                    // ShardEpoch::ZERO and skips this read because there is no
                    // takeover actor that can advance the epoch.
                    convert::i64_from_u64(epoch.0, "caller shard epoch")?;
                    let mut permit = self.director.acquire(DbClass::Commit).await?;
                    let mut tx = permit.connection()?.begin().await?;
                    let row = sqlx::query_as::<_, (i64,)>(
                        "SELECT epoch FROM shard_lease WHERE shard_id = $1",
                    )
                    .bind(Self::shard_id_to_uuid(execution_home_bundle))
                    .fetch_optional(&mut *tx)
                    .await?;
                    let Some((durable_epoch,)) = row else {
                        tx.rollback().await?;
                        return Ok(CommitResult::Conflict {
                            reason: format!(
                                "no active lease for execution-home bundle {:?} at epoch {:?}",
                                execution_home_bundle, epoch
                            ),
                        });
                    };
                    if durable_epoch != convert::i64_from_u64(epoch.0, "caller shard epoch")? {
                        tx.rollback().await?;
                        return Ok(CommitResult::Conflict {
                            reason: format!(
                                "stale shard epoch {:?} for execution-home bundle {:?}; current {}",
                                epoch, execution_home_bundle, durable_epoch
                            ),
                        });
                    }
                    tx.rollback().await?;
                }

                metrics::increment_dsql_commits_in_flight();
                let result = self
                    .commit_transition(run_key, transition, ShardEpoch::ZERO)
                    .await;
                metrics::decrement_dsql_commits_in_flight();
                result
                }
            )
        }
        .instrument(span)
        .await
    }
}

async fn write_transition(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_key: RunKey,
    shard_id: ShardId,
    projection_partition_count: u32,
    transition: &Transition,
    state: &WorkflowState,
) -> Result<()> {
    if transition.history_events.len() != transition.event_principals.len() {
        bail!(
            "history/principal sidecar length mismatch: {} events, {} principals",
            transition.history_events.len(),
            transition.event_principals.len()
        );
    }
    // The commit path intentionally writes the hot state first, then derives
    // every side table from the same transition/state pair. History remains the
    // authority; side tables are rebuildable projections that make dispatch and
    // sweep queries efficient.
    insert_workflow_hot(tx, run_key, shard_id, state).await?;
    if !transition.history_events.is_empty() {
        insert_history_batch(
            tx,
            run_key,
            state.transition_seq,
            transition.history_events.as_slice(),
            transition.event_principals.as_slice(),
        )
        .await?;
    }
    for op in &transition.request_dedupe_ops {
        let key = DsqlRunRepository::request_dedupe_key(
            state.namespace_id,
            &state.workflow_id,
            &op.request_id,
        );
        // A new run reusing a prior run's request id takes over the record
        // (matching the in-memory store's overwrite): the table answers "which
        // run most recently accepted this request id" for lookups, while the
        // duplicate CHECK above is run-scoped.
        sqlx::query(
            "INSERT INTO request_dedupe
             (key, namespace_id, workflow_id, request_id, run_key, run_id, first_seen_transition_seq, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, now())
             ON CONFLICT (key) DO UPDATE SET
                 run_key = EXCLUDED.run_key,
                 run_id = EXCLUDED.run_id,
                 first_seen_transition_seq = EXCLUDED.first_seen_transition_seq,
                 created_at = EXCLUDED.created_at",
        )
        .bind(key)
        .bind(state.namespace_id.0)
        .bind(&state.workflow_id.0)
        .bind(&op.request_id.0)
        .bind(run_key.0)
        .bind(state.run_id.0)
        .bind(convert::i64_from_u64(state.transition_seq.0, "transition_seq")?)
        .execute(&mut **tx)
        .await?;
    }
    for op in &transition.activity_ops {
        match op {
            ActivityOp::Upsert(activity) => {
                upsert_activity(tx, run_key, shard_id, state.namespace_id, activity).await?;
                // `activity_dispatch` is the durable dispatch source, not
                // `activity_state`. Started or paused activities must disappear
                // from dispatch immediately; still-dispatchable upserts only
                // update an existing row so a paused workflow cannot create a
                // dispatch row by changing activity options.
                if activity.started_at.is_some() || activity.pause_info.is_some() {
                    delete_activity_dispatch(tx, run_key, &activity.activity_id).await?;
                } else {
                    update_existing_activity_dispatch(tx, run_key, shard_id, state, activity)
                        .await?;
                }
            }
            ActivityOp::Delete { activity_id } => {
                sqlx::query("DELETE FROM activity_state WHERE run_key = $1 AND activity_id = $2")
                    .bind(run_key.0)
                    .bind(activity_id)
                    .execute(&mut **tx)
                    .await?;
                delete_activity_dispatch(tx, run_key, activity_id).await?;
            }
        }
    }
    for op in &transition.dispatch_ops {
        if let DispatchOp::EnqueueActivityTask {
            queue,
            activity_id,
            input,
            schedule_event_id,
            attempt,
            dispatch_revision,
            stamp,
            dispatch_at,
            priority,
            ..
        } = op
        {
            // Enqueue is the only path that creates a dispatch row. Re-enqueue
            // after retry/reset/unpause is idempotent via ON CONFLICT.
            upsert_activity_dispatch_from_dispatch_op(
                tx,
                run_key,
                shard_id,
                queue,
                activity_id,
                input,
                *schedule_event_id,
                *attempt,
                *dispatch_revision,
                *stamp,
                *dispatch_at,
                priority.as_ref(),
            )
            .await?;
        }
    }
    if state.status == ExecutionStatus::Paused {
        // Workflow pause suppresses all activity dispatch for the run. The
        // state table still carries activities for later unpause/retry logic.
        delete_activity_dispatch_for_run(tx, run_key).await?;
    }
    for op in &transition.timer_ops {
        match op {
            TimerOp::Upsert(timer) => upsert_timer(tx, run_key, shard_id, timer).await?,
            TimerOp::Delete { timer_id } => {
                sqlx::query("DELETE FROM timer_bucket WHERE run_key = $1 AND timer_id = $2")
                    .bind(run_key.0)
                    .bind(timer_id)
                    .execute(&mut **tx)
                    .await?;
            }
        }
    }
    if transition.expected_seq == TransitionSeq::ZERO && state.status.is_open() {
        // Only start transitions publish a new current-execution open pointer.
        upsert_current_execution_start(tx, run_key, state).await?;
    } else if !state.status.is_open() {
        let key = DsqlRunRepository::current_execution_key(state.namespace_id, &state.workflow_id);
        // Closing an older run must not close a successor that has already
        // replaced this workflow-level pointer, hence the run_key guard.
        sqlx::query(
            "UPDATE current_execution SET is_open = false
             WHERE key = $1 AND run_key = $2",
        )
        .bind(key)
        .bind(run_key.0)
        .execute(&mut **tx)
        .await?;
    }
    // Visibility is a post-transition snapshot, not a delta side effect. Every
    // transition publishes its context so list/count state advances monotonically
    // from the committed run image rather than an incomplete stream of patches.
    insert_projection_log(tx, run_key, state, projection_partition_count).await?;
    Ok(())
}

pub(super) async fn insert_workflow_hot(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_key: RunKey,
    shard_id: ShardId,
    state: &WorkflowState,
) -> Result<()> {
    // `workflow_hot` is a materialized snapshot for recovery and read paths.
    // It is not the audit trail; history_batch carries the append-only events.
    let started = Instant::now();
    sqlx::query(
        "INSERT INTO workflow_hot
         (run_key, namespace_id, workflow_id, shard_id, transition_seq, state_data, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, now())
         ON CONFLICT (run_key) DO UPDATE SET
             transition_seq = EXCLUDED.transition_seq,
             state_data = EXCLUDED.state_data,
             shard_id = EXCLUDED.shard_id,
             updated_at = EXCLUDED.updated_at",
    )
    .bind(run_key.0)
    .bind(state.namespace_id.0)
    .bind(&state.workflow_id.0)
    .bind(DsqlRunRepository::shard_id_to_uuid(shard_id))
    .bind(convert::i64_from_u64(
        state.transition_seq.0,
        "transition_seq",
    )?)
    .bind(codec::encode_workflow_state(state)?)
    .execute(&mut **tx)
    .await?;
    metrics::record_dsql_statement_duration(
        "commit_transition",
        "update_execution",
        started.elapsed(),
    );
    Ok(())
}

pub(super) async fn insert_history_batch(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_key: RunKey,
    transition_seq: TransitionSeq,
    events: &[HistoryEvent],
    principals: &[Option<tokeira_types::EventPrincipal>],
) -> Result<()> {
    if events.len() != principals.len() {
        bail!(
            "history/principal sidecar length mismatch: {} events, {} principals",
            events.len(),
            principals.len()
        );
    }
    let first_event_id = events
        .first()
        .ok_or_else(|| anyhow!("cannot insert empty history batch"))?
        .event_id;
    let last_event_id = events
        .last()
        .ok_or_else(|| anyhow!("cannot insert empty history batch"))?
        .event_id;
    let started = Instant::now();
    let principals_data = principals
        .iter()
        .any(Option::is_some)
        .then(|| codec::encode_history_principals(principals))
        .transpose()?;
    sqlx::query(
        "INSERT INTO history_batch
         (run_key, first_event_id, last_event_id, transition_seq, events_data, principals_data, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, now())",
    )
    .bind(run_key.0)
    .bind(first_event_id)
    .bind(last_event_id)
    .bind(convert::i64_from_u64(transition_seq.0, "transition_seq")?)
    .bind(codec::encode_history_events(events)?)
    .bind(principals_data)
    .execute(&mut **tx)
    .await?;
    metrics::record_dsql_statement_duration(
        "commit_transition",
        "append_history",
        started.elapsed(),
    );
    Ok(())
}

pub(super) async fn upsert_activity(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_key: RunKey,
    shard_id: ShardId,
    namespace_id: NamespaceId,
    activity: &tokeira_kernel::ActivityState,
) -> Result<()> {
    // Activity state is keyed by schedule_event_id for timer/sweep stability.
    // The human activity_id is still stored for operator-facing mapping and
    // secondary delete predicates.
    sqlx::query(
        "INSERT INTO activity_state
         (run_key, schedule_event_id, shard_id, activity_id, queue_namespace, queue_name, attempt, state_data, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
         ON CONFLICT (run_key, schedule_event_id) DO UPDATE SET
             state_data = EXCLUDED.state_data,
             attempt = EXCLUDED.attempt,
             updated_at = EXCLUDED.updated_at",
    )
    .bind(run_key.0)
    .bind(activity.schedule_event_id)
    .bind(DsqlRunRepository::shard_id_to_uuid(shard_id))
    .bind(&activity.activity_id)
    .bind(namespace_id.0)
    .bind(&activity.task_queue.0)
    .bind(i32::try_from(activity.attempt)?)
    .bind(codec::encode_activity_state(activity)?)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn upsert_activity_dispatch_from_dispatch_op(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_key: RunKey,
    shard_id: ShardId,
    queue: &QueueKey,
    activity_id: &str,
    input: &Payloads,
    schedule_event_id: i64,
    attempt: u32,
    dispatch_revision: i64,
    stamp: u64,
    dispatch_at: OffsetDateTime,
    priority: Option<&tokeira_kernel::Priority>,
) -> Result<()> {
    let key = DsqlRunRepository::activity_dispatch_key(run_key, activity_id);
    let deployment = queue.deployment.as_ref().map(|value| value.0.as_str());
    let build_id = queue.build_id.as_ref().map(|value| value.0.as_str());
    sqlx::query(
        "INSERT INTO activity_dispatch
         (key, run_key, activity_id, shard_id, queue_namespace, queue_name, task_kind,
          deployment, build_id, schedule_event_id, attempt, dispatch_revision, stamp, input_data,
          priority_data, dispatch_at, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, now())
         ON CONFLICT (key) DO UPDATE SET
             shard_id = EXCLUDED.shard_id,
             queue_namespace = EXCLUDED.queue_namespace,
             queue_name = EXCLUDED.queue_name,
             task_kind = EXCLUDED.task_kind,
             deployment = EXCLUDED.deployment,
             build_id = EXCLUDED.build_id,
             schedule_event_id = EXCLUDED.schedule_event_id,
             attempt = EXCLUDED.attempt,
             dispatch_revision = EXCLUDED.dispatch_revision,
             stamp = EXCLUDED.stamp,
             input_data = EXCLUDED.input_data,
             priority_data = EXCLUDED.priority_data,
             dispatch_at = EXCLUDED.dispatch_at",
    )
    .bind(key)
    .bind(run_key.0)
    .bind(activity_id)
    .bind(DsqlRunRepository::shard_id_to_uuid(shard_id))
    .bind(queue.namespace_id.0)
    .bind(&queue.task_queue.0)
    .bind(queue.task_kind.to_db_smallint())
    .bind(deployment)
    .bind(build_id)
    .bind(schedule_event_id)
    .bind(i32::try_from(attempt)?)
    .bind(dispatch_revision)
    .bind(i64::try_from(stamp).unwrap_or(i64::MAX))
    .bind(codec::encode_payloads(input)?)
    .bind(priority.map(codec::encode_priority).transpose()?)
    .bind(dispatch_at)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn update_existing_activity_dispatch(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_key: RunKey,
    shard_id: ShardId,
    state: &WorkflowState,
    activity: &tokeira_kernel::ActivityState,
) -> Result<()> {
    let key = DsqlRunRepository::activity_dispatch_key(run_key, &activity.activity_id);
    // This is deliberately UPDATE-only. If no dispatch row exists, the activity
    // is not currently dispatchable; an ActivityOp::Upsert must not invent one.
    let deployment = activity.deployment.as_ref().or(state.deployment.as_ref());
    let build_id = activity.build_id.as_ref().or(state.build_id.as_ref());
    sqlx::query(
        "UPDATE activity_dispatch SET
             shard_id = $2,
             queue_namespace = $3,
             queue_name = $4,
             task_kind = $5,
             deployment = $6,
             build_id = $7,
             schedule_event_id = $8,
             attempt = $9,
             dispatch_revision = $10,
             stamp = $11,
             input_data = $12,
             priority_data = $13,
             dispatch_at = COALESCE($14, activity_dispatch.dispatch_at)
         WHERE key = $1",
    )
    .bind(key)
    .bind(DsqlRunRepository::shard_id_to_uuid(shard_id))
    .bind(state.namespace_id.0)
    .bind(&activity.task_queue.0)
    .bind(TaskKind::Activity.to_db_smallint())
    .bind(deployment.map(|value| value.0.as_str()))
    .bind(build_id.map(|value| value.0.as_str()))
    .bind(activity.schedule_event_id)
    .bind(i32::try_from(activity.attempt)?)
    .bind(
        state
            .versioning_info
            .as_ref()
            .map(|info| info.revision_number)
            .unwrap_or_default(),
    )
    // Keep the durable dispatch row's stamp in step with the activity so a
    // recovery-reconstructed task carries the current stamp and is never
    // spuriously fenced at start.
    .bind(i64::try_from(activity.stamp).unwrap_or(i64::MAX))
    .bind(codec::encode_payloads(&activity.input)?)
    .bind(
        merge_priority(state.priority.as_ref(), activity.priority.as_ref())
            .as_ref()
            .map(codec::encode_priority)
            .transpose()?,
    )
    // Options/retry updates re-anchor eligibility from the authoritative
    // attempt schedule; a state without one (legacy/initial) keeps the row's
    // existing eligibility time via COALESCE.
    .bind(activity.current_attempt_scheduled_at)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn delete_activity_dispatch(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_key: RunKey,
    activity_id: &str,
) -> Result<()> {
    let key = DsqlRunRepository::activity_dispatch_key(run_key, activity_id);
    sqlx::query("DELETE FROM activity_dispatch WHERE key = $1")
        .bind(key)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn delete_activity_dispatch_for_run(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_key: RunKey,
) -> Result<()> {
    sqlx::query("DELETE FROM activity_dispatch WHERE run_key = $1")
        .bind(run_key.0)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub(super) async fn upsert_timer(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_key: RunKey,
    shard_id: ShardId,
    timer: &tokeira_kernel::TimerState,
) -> Result<()> {
    // Timer rows are keyed by shard and fire time so sweepers can ask one shard
    // for due work without scanning all timers.
    sqlx::query(
        "INSERT INTO timer_bucket
         (shard_id, fire_at, run_key, timer_id, timer_data, created_at)
         VALUES ($1, $2, $3, $4, $5, now())
         ON CONFLICT (shard_id, fire_at, run_key, timer_id) DO UPDATE SET
             timer_data = EXCLUDED.timer_data",
    )
    .bind(DsqlRunRepository::shard_id_to_uuid(shard_id))
    .bind(timer.fire_at)
    .bind(run_key.0)
    .bind(&timer.timer_id)
    .bind(codec::encode_timer_state(timer)?)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub(super) async fn upsert_current_execution_start(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_key: RunKey,
    state: &WorkflowState,
) -> Result<()> {
    let key = DsqlRunRepository::current_execution_key(state.namespace_id, &state.workflow_id);
    // This row is a workflow-level pointer, so the primary key is derived from
    // namespace/workflow_id rather than run_id. Explicit run lookup goes
    // through `workflow_hot`.
    sqlx::query(
        "INSERT INTO current_execution
         (key, namespace_id, workflow_id, run_key, run_id, is_open, created_at)
         VALUES ($1, $2, $3, $4, $5, true, now())
         ON CONFLICT (key) DO UPDATE SET
             run_key = EXCLUDED.run_key,
             run_id = EXCLUDED.run_id,
             is_open = true",
    )
    .bind(key)
    .bind(state.namespace_id.0)
    .bind(&state.workflow_id.0)
    .bind(run_key.0)
    .bind(state.run_id.0)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn insert_projection_log(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_key: RunKey,
    state: &WorkflowState,
    projection_partition_count: u32,
) -> Result<()> {
    // Projection log rows are grouped per transition. Visibility sinks can
    // replay the projection stream without rereading workflow state/history.
    let previous_context = sqlx::query_as::<_, (Vec<u8>,)>(
        "SELECT context_data
         FROM projection_log
         WHERE run_key = $1
         ORDER BY transition_seq DESC
         LIMIT 1",
    )
    .bind(run_key.0)
    .fetch_optional(&mut **tx)
    .await?
    .map(|(data,)| codec::decode_projection_context(&data))
    .transpose()?;
    let context = workflow_projection_context_with_previous(state, previous_context.as_ref())?;
    sqlx::query(
        "INSERT INTO projection_log
         (partition_id, fanout, run_key, transition_seq, context_data, ops_data, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, now())",
    )
    .bind(i32::try_from(partition_for(
        run_key,
        projection_partition_count,
    ))?)
    .bind(PROJECTION_FANOUT)
    .bind(run_key.0)
    .bind(convert::i64_from_u64(
        state.transition_seq.0,
        "transition_seq",
    )?)
    .bind(codec::encode_projection_context(&context)?)
    .bind(codec::LEGACY_EMPTY_PROJECTION_OPS_DATA)
    .execute(&mut **tx)
    .await?;
    Ok(())
}
