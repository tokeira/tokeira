//! Query and update dispatch methods of [`TokeiraRuntime`].
//!
//! This `impl` continuation owns the two read-adjacent worker interactions that
//! must be ordered against a run's pending workflow task (WFT) rather than
//! simply mutating it: consistent queries and synchronous updates. Both are
//! transport-coordinated — the runtime parks a caller on a `oneshot` and the
//! lane/transport layer resolves it — so the contract here is mostly about
//! *when* a request is allowed to reach a worker and how the caller is woken.
//!
//! Key invariants:
//! - A consistent query never observes stale state. While a WFT is in flight
//!   the query is buffered behind a barrier (the run's `last_event_id` at query
//!   time) and only released once the run advances past it; otherwise it is
//!   dispatched directly because the run is quiescent.
//! - Updates are two-phase (admit, then worker-accept). The wait policy decides
//!   whether the caller returns at admission or blocks for the final
//!   resolution. The caller registration is always cleaned up on every error
//!   and timeout path so a failed update cannot leak a parked waiter.
use super::*;
use crate::errors::UpdateLifecycleError;
use tokeira_observability::{QueryDispatchOutcomeLabel, QueryDispatchPathLabel};

fn query_sticky_deadline(
    enqueued_at: OffsetDateTime,
    schedule_to_start_timeout: Duration,
) -> OffsetDateTime {
    enqueued_at + schedule_to_start_timeout
}

impl<R> TokeiraRuntime<R>
where
    R: RunRepository + 'static,
{
    /// Dispatch a read-only query to a workflow worker and await the response.
    ///
    /// Two delivery paths exist depending on whether the run has a pending
    /// workflow task (WFT). When a WFT is in flight, the query is buffered
    /// behind a *barrier* — the run's `last_event_id` at query time — so the
    /// worker cannot evaluate it against stale state. Once the WFT completes
    /// and the run advances past the barrier, the transport layer releases the
    /// query for delivery. When no WFT is pending the query goes directly to
    /// the broker for immediate dispatch, because the run is quiescent and the
    /// worker already has up-to-date state.
    pub async fn query_workflow(
        &self,
        execution: ExecutionRef,
        query_type: String,
        query_args: Payloads,
        timeout_after: Duration,
    ) -> Result<QueryResult> {
        // A query resolves the *current* execution — the open run if one exists, else the
        // latest closed run — matching v1.31.0, which serves queries against closed
        // workflows by worker replay (`service/frontend/workflow_handler.go:3123 @ v1.31.0`).
        // `resolve_execution(run_id=None)` is open-only by repo contract, so fall back to
        // `find_latest_run`, mirroring the fallback `StoreExecutionResolver` already applies
        // to describe. An explicit run_id is an exact lookup and never falls back.
        let run_key = match self.repo.resolve_execution(&execution).await? {
            Some(rk) => rk,
            None if execution.run_id.is_none() => self
                .repo
                .find_latest_run(execution.namespace_id, &execution.workflow_id)
                .await?
                .ok_or_else(|| anyhow!("execution not found"))?,
            None => return Err(anyhow!("execution not found")),
        };

        let state = match self.repo.load_run(run_key).await? {
            LoadedRun::Existing(state) => state,
            LoadedRun::Absent => {
                return Err(anyhow!("execution disappeared before query dispatch"));
            }
        };

        // A paused workflow rejects queries without dispatch, matching Temporal:
        // the run is frozen, so there is no worker turn to evaluate the query
        // against. The edge translates this into a `QueryRejected` response.
        if state.status == tokeira_types::ExecutionStatus::Paused {
            runtime_metrics::record_query_dispatch(
                QueryDispatchPathLabel::Direct,
                QueryDispatchOutcomeLabel::Rejected,
            );
            return Ok(QueryResult::Rejected {
                status: tokeira_types::ExecutionStatus::Paused,
            });
        }

        // A pinned query must not enter the broker when its target version is
        // drained and its workflow task-queue family has no eligible poller:
        // there is no worker that can ever answer it. v1.31.0 fails this
        // synchronously with ErrBlackholedQuery (`checkQueryBlackholed`,
        // task_queue_partition_manager.go @ v1.31.0). The runtime combines
        // authoritative per-run routing with the deployment registry's durable
        // status and ephemeral poll observations; the edge remains translation-only.
        if state.effective_behavior() == tokeira_kernel::VersioningBehavior::Pinned
            && let Some(version) = state.effective_deployment()
            && let Some(registry) = self.deployment_registry()
            && registry
                .pinned_query_is_blackholed(
                    state.namespace_id,
                    &state.task_queue,
                    &version.deployment_name,
                    &version.build_id,
                )
                .await?
        {
            return Err(crate::errors::BlackholedQuery {
                version: version.build_id.clone(),
            }
            .into());
        }

        // v1.31.0's pre-dispatch guards (queryworkflow/api.go:116-143): a
        // query can only ever be answered by a worker that has completed at
        // least one workflow task for this run (the SDK cannot evaluate a
        // handler on a run it has never seen — api.go:153).
        let has_completed_any_wft = state.previous_started_event_id > 0;
        tracing::debug!(
            run_key = ?run_key,
            has_completed_any_wft,
            pending = state.pending_workflow_task.is_some(),
            delay_timer = ?state
                .timers
                .get(tokeira_kernel::WORKFLOW_START_DELAY_TIMER_ID)
                .map(|t| t.fire_at),
            ?timeout_after,
            wft_attempt = state.workflow_task_attempt,
            "query admission guards"
        );
        if !has_completed_any_wft {
            // (i) Closed without ever starting a workflow task: nothing can
            // answer, now or ever (`ErrWorkflowClosedBeforeWorkflowTaskStarted`,
            // consts/const.go:114-115).
            if !state.status.is_open() {
                return Err(crate::errors::WorkflowNotReady {
                    message: "Workflow execution closed before WorkflowTaskStarted event",
                }
                .into());
            }
            // (ii) First WFT still pending on start-delay backoff and the
            // query's deadline lands before it would even be scheduled: fail
            // fast so the SDK's retry loop keeps polling until the backoff
            // elapses (`ErrWorkflowTaskNotScheduled` +
            // queryWillTimeoutsBeforeFirstWorkflowTaskStart,
            // queryworkflow/api.go:121-132,288-310; consts/const.go:96-97).
            if let Some(delay_timer) = state
                .timers
                .get(tokeira_kernel::WORKFLOW_START_DELAY_TIMER_ID)
                .filter(|_| state.pending_workflow_task.is_none())
                && OffsetDateTime::now_utc() + timeout_after < delay_timer.fire_at
            {
                return Err(crate::errors::WorkflowNotReady {
                    message: "Workflow task is not scheduled yet.",
                }
                .into());
            }
        }
        // (iii) The workflow task is stuck failing: after 3 attempts the
        // query fails fast instead of buffering behind a task that will not
        // complete (`failQueryWorkflowTaskAttemptCount`,
        // queryworkflow/api.go:31,134-143).
        if state.workflow_task_attempt >= 3 {
            return Err(crate::errors::WorkflowNotReady {
                message: "Unable to query workflow due to Workflow Task in failed state.",
            }
            .into());
        }

        let mut queue = QueueKey {
            namespace_id: state.namespace_id,
            task_queue: state.task_queue.clone(),
            task_kind: TaskKind::Workflow,
            deployment: state.deployment.clone(),
            build_id: state.build_id.clone(),
        };

        // V3 query routing follows the same live target as a workflow task.
        // In particular, AUTO_UPGRADE queries move to a newly-current version
        // without waiting for another WFT to record that version on the run
        // (`TestUnpinnedQuery_Sticky`, versioning_3_test.go @ v1.31.0).
        // Reuse Tokeira's runtime resolver so routing remains a derived effect
        // of authoritative run + deployment state rather than edge state.
        let mut routed_version = state.effective_deployment().cloned();
        if state.versioning_info.is_some()
            && let Some(registry) = self.deployment_registry()
        {
            let preferred_deployment = state
                .effective_deployment()
                .map(|version| version.deployment_name.as_str())
                .or(state.worker_deployment_name.as_deref());
            let routing = registry
                .workflow_task_routing_config(
                    state.namespace_id,
                    &state.task_queue.0,
                    preferred_deployment,
                )
                .await?;
            routed_version =
                super::workflow_task::resolve_workflow_task_target_version(&routing, &state)
                    .deployment_version;
            queue.deployment = routed_version
                .as_ref()
                .map(|version| DeploymentId(version.deployment_name.clone()));
            queue.build_id = routed_version
                .as_ref()
                .map(|version| BuildId(version.build_id.clone()));
        }

        let now = OffsetDateTime::now_utc();
        // A sticky worker is eligible only while it still owns the version to
        // which the query routes. If AUTO_UPGRADE moved the target, v1.31.0
        // bypasses the old sticky queue and sends the query to the new normal
        // queue (`TestUnpinnedQuery_Sticky`, versioning_3_test.go @ v1.31.0).
        let sticky_route_eligible = state.versioning_info.is_none()
            || routed_version.as_ref() == state.effective_deployment();
        let sticky_affinity = state
            .sticky
            .as_ref()
            .filter(|affinity| sticky_route_eligible && !affinity.sticky_queue.0.is_empty());
        let sticky_preferred = sticky_affinity.map(|affinity| affinity.worker_identity.clone());
        let sticky_queue = sticky_affinity.map(|affinity| affinity.sticky_queue.clone());
        // Each direct query gets a fresh sticky attempt window. Affinity
        // itself has no expiry; v1.31.0 derives this request deadline from
        // StickyTaskQueueScheduleToStartTimeout when the query is enqueued
        // (`service/history/api/queryworkflow/api.go:350-410 @ v1.31.0`).
        let sticky_deadline = sticky_affinity
            .map(|affinity| query_sticky_deadline(now, affinity.schedule_to_start_timeout));
        let required_barrier = state.last_event_id;

        let (response_tx, response_rx) = oneshot::channel();
        let query_id = Uuid::new_v4().to_string();
        // Direct dispatch requires BOTH a completed workflow task (else no
        // worker can answer) AND a quiescent run — closed, or open with no
        // in-flight WFT (`safeToDispatchDirectly`, queryworkflow/api.go:
        // 147-183 @ v1.31.0). Everything else buffers behind the barrier and
        // rides (or is unblocked by) the next workflow task.
        let safe_to_dispatch_directly = has_completed_any_wft
            && (!state.status.is_open() || state.pending_workflow_task.is_none());
        let buffer_query = !safe_to_dispatch_directly;

        if buffer_query {
            self.buffered_queries
                .buffer(
                    run_key,
                    BufferedQuery {
                        query_id: query_id.clone(),
                        query_type,
                        query_args,
                        required_barrier,
                        enqueued_at: std::time::Instant::now(),
                        response_tx,
                    },
                )
                .map_err(|_| {
                    runtime_metrics::record_query_dispatch(
                        QueryDispatchPathLabel::Buffered,
                        QueryDispatchOutcomeLabel::Rejected,
                    );
                    // v1.31.0's `ErrConsistentQueryBufferExceeded` — the edge
                    // renders RESOURCE_EXHAUSTED with cause BUSY_WORKFLOW and
                    // this exact message (queryworkflow/api.go:191-194
                    // @ v1.31.0).
                    anyhow::Error::new(crate::errors::ConsistentQueryBufferExceeded)
                })?;
            runtime_metrics::record_query_dispatch(
                QueryDispatchPathLabel::Buffered,
                QueryDispatchOutcomeLabel::Queued,
            );
        } else {
            self.broker
                .publish_query_task(QueryTask {
                    run_key,
                    query_type,
                    query_args,
                    queue,
                    sticky_preferred,
                    sticky_queue,
                    sticky_deadline,
                    response_tx,
                })
                .await;
            runtime_metrics::record_query_dispatch(
                QueryDispatchPathLabel::Direct,
                QueryDispatchOutcomeLabel::Published,
            );
        }

        let timeout_after: std::time::Duration = timeout_after
            .try_into()
            .map_err(|_| anyhow!("query timeout must be non-negative"))?;

        let cleanup = BufferedQueryCleanup {
            registry: self.buffered_queries.clone(),
            run_key,
            query_id,
            enabled: buffer_query,
        };

        match tokio::time::timeout(timeout_after, response_rx).await {
            Ok(Ok(result)) => {
                cleanup.disarm();
                Ok(result)
            }
            Ok(Err(_)) => Err(anyhow!("query response channel closed")),
            Err(_) => Err(crate::errors::QueryTimedOut.into()),
        }
    }

    /// Dispatch a synchronous update and optionally wait for completion.
    ///
    /// Updates follow a two-phase lifecycle: the kernel first *admits* the
    /// update (recording it in `admitted_updates`), then the worker *accepts*
    /// it during a subsequent WFT, which promotes it to `pending_updates` and
    /// writes the acceptance event. This split lets the API return quickly for
    /// `Accepted` wait policy (phase 1) while `Completed` callers block on a
    /// oneshot until the lane notifies the `UpdateRegistry` with the final
    /// resolution.
    pub async fn update_workflow(
        &self,
        execution: ExecutionRef,
        update_id: String,
        update_name: String,
        input: Payloads,
        request: RequestContext,
        timeout_after: Duration,
        wait_policy: UpdateWaitPolicy,
    ) -> Result<UpdateLifecycleSnapshot> {
        let run_key = self
            .repo
            .resolve_execution(&execution)
            .await?
            .ok_or_else(|| anyhow!("execution not found"))?;

        // Dedupe by update id BEFORE admitting: an update already known
        // durably (accepted/completed events, kernel-admitted state) attaches
        // to the in-flight update or replays its outcome — repeated requests
        // sharing an id get one update and one result, and never a second
        // WFT (`FindOrCreate` attach + `GetUpdateOutcome` replay,
        // registry.go:398-484 @ v1.31.0).
        if self
            .update_lifecycle_snapshot(run_key, execution.clone(), &update_id)
            .await?
            .is_some()
        {
            return self
                .wait_for_update_stage(run_key, execution, update_id, wait_policy, timeout_after)
                .await;
        }

        let (wait_tx, wait_rx) = oneshot::channel::<UpdateResolution>();
        let created = self.update_registry.register(
            run_key,
            update_id.clone(),
            update_name.clone(),
            input.clone(),
            request.caller_identity.clone().unwrap_or_default(),
            wait_policy.clone(),
            wait_tx,
        );
        if !created {
            // Raced another caller registering the same id: our waiter is
            // attached to the shared entry; the other caller admits.
            return self
                .wait_for_update_stage_with_receiver(
                    run_key,
                    execution,
                    update_id,
                    wait_policy,
                    timeout_after,
                    wait_rx,
                )
                .await;
        }

        let command = Command::Update(UpdateRequest {
            update_id: update_id.clone(),
            update_name,
            input,
            request,
            now: OffsetDateTime::now_utc(),
        });

        let submit_result = self.submit(run_key, command).await;
        let commit_result = match submit_result {
            Ok(result) => result,
            Err(error) if is_duplicate_update_reject(&error) => {
                // Admission raced an earlier request for the same id that the
                // snapshot pre-check missed. Our waiter is already attached;
                // the earlier admission delivers the update.
                return self
                    .wait_for_update_stage_with_receiver(
                        run_key,
                        execution,
                        update_id,
                        wait_policy,
                        timeout_after,
                        wait_rx,
                    )
                    .await;
            }
            Err(error) => {
                self.update_registry.remove(run_key, &update_id);
                return Err(error);
            }
        };

        // The update has been admitted (tracked in admitted_updates).
        // The accepted_event_id is not yet known — it will be assigned
        // when the worker sends an Acceptance message. For now, use 0
        // as a placeholder for the Accepted wait policy.
        match commit_result {
            CommitResult::Applied { .. } => {}
            CommitResult::Duplicate => {
                self.update_registry.remove(run_key, &update_id);
                return self
                    .wait_for_update_stage(
                        run_key,
                        execution,
                        update_id,
                        wait_policy,
                        timeout_after,
                    )
                    .await;
            }
            CommitResult::Conflict { reason } => {
                self.update_registry.remove(run_key, &update_id);
                return Err(anyhow!("update commit conflicted: {reason}"));
            }
            CommitResult::CurrentExecutionConflict { .. } => {
                // Unreachable for an update (not a zero-seq start).
                self.update_registry.remove(run_key, &update_id);
                return Err(anyhow!(
                    "unexpected current-execution conflict on update commit"
                ));
            }
        };

        self.wait_for_update_stage_with_receiver(
            run_key,
            execution,
            update_id,
            wait_policy,
            timeout_after,
            wait_rx,
        )
        .await
    }

    pub async fn poll_workflow_update(
        &self,
        execution: ExecutionRef,
        update_id: String,
        wait_policy: UpdateWaitPolicy,
        timeout_after: Duration,
    ) -> Result<UpdateLifecycleSnapshot> {
        let run_key = self
            .repo
            .resolve_execution(&execution)
            .await?
            .ok_or_else(|| anyhow!("execution not found"))?;

        self.wait_for_update_stage(run_key, execution, update_id, wait_policy, timeout_after)
            .await
    }

    pub(crate) async fn wait_for_update_stage(
        &self,
        run_key: RunKey,
        execution: ExecutionRef,
        update_id: String,
        wait_policy: UpdateWaitPolicy,
        timeout_after: Duration,
    ) -> Result<UpdateLifecycleSnapshot> {
        let (tx, rx) = oneshot::channel::<UpdateResolution>();
        let snapshot = self
            .update_lifecycle_snapshot(run_key, execution.clone(), &update_id)
            .await?
            .ok_or_else(|| UpdateLifecycleError::UpdateNotFound {
                run_key,
                update_id: update_id.clone(),
            })?;
        let requested = requested_stage(wait_policy.clone());
        if requested <= UpdateLifecycleStage::Admitted || snapshot.stage >= requested {
            return Ok(snapshot);
        }

        if matches!(
            wait_policy,
            UpdateWaitPolicy::Accepted | UpdateWaitPolicy::Completed
        ) && self
            .update_registry
            .contains_registered_update(run_key, &update_id)
            && self
                .update_registry
                .attach_waiter(run_key, &update_id, wait_policy.clone(), tx)
        {
            return self
                .wait_for_update_stage_with_receiver(
                    run_key,
                    execution,
                    update_id,
                    wait_policy,
                    timeout_after,
                    rx,
                )
                .await;
        }

        self.wait_for_history_stage(
            run_key,
            execution,
            update_id,
            requested,
            timeout_after,
            snapshot,
        )
        .await
    }

    pub(crate) async fn wait_for_update_stage_with_receiver(
        &self,
        run_key: RunKey,
        execution: ExecutionRef,
        update_id: String,
        wait_policy: UpdateWaitPolicy,
        timeout_after: Duration,
        response_rx: oneshot::Receiver<UpdateResolution>,
    ) -> Result<UpdateLifecycleSnapshot> {
        let requested = requested_stage(wait_policy.clone());
        if requested <= UpdateLifecycleStage::Admitted {
            return self
                .snapshot_or_update_not_found(run_key, execution, &update_id)
                .await;
        }

        if let Some(snapshot) = self
            .update_lifecycle_snapshot(run_key, execution.clone(), &update_id)
            .await?
            .filter(|snapshot| snapshot.stage >= requested)
        {
            return Ok(snapshot);
        }

        let timeout_after: std::time::Duration = timeout_after
            .try_into()
            .map_err(|_| anyhow!("update timeout must be non-negative"))?;

        match tokio::time::timeout(timeout_after, response_rx).await {
            Ok(Ok(UpdateResolution::Accepted { accepted_event_id })) => {
                let _ = accepted_event_id;
                self.snapshot_or_update_not_found(run_key, execution, &update_id)
                    .await
            }
            Ok(Ok(UpdateResolution::Completed { result })) => {
                let _ = result;
                self.snapshot_or_update_not_found(run_key, execution, &update_id)
                    .await
            }
            Ok(Ok(UpdateResolution::Rejected { failure })) => {
                // A rejection leaves NO durable trace (no history event), so
                // the snapshot cannot be re-read — the resolution itself IS
                // the outcome: COMPLETED stage carrying the rejection failure
                // (WaitLifecycleStage rejection handling, update.go:141-239
                // @ v1.31.0).
                Ok(UpdateLifecycleSnapshot {
                    workflow_execution: ExecutionRef {
                        run_id: self.resolve_run_id_for_snapshot(run_key, &execution).await,
                        ..execution
                    },
                    update_id: update_id.clone(),
                    update_name: String::new(),
                    stage: UpdateLifecycleStage::Completed,
                    outcome: Some(UpdateOutcome::Rejected {
                        accepted_event_id: 0,
                        failure,
                    }),
                })
            }
            // Abort taxonomy per v1.31.0 (abort_reason.go:25-121): pre-accepted
            // aborts surface as typed RPC errors (the edge maps them); an
            // accepted update's close is a NON-error COMPLETED outcome with the
            // server-authored completed-before failure.
            Ok(Ok(UpdateResolution::AbortedByClosingWorkflow)) => {
                Err(crate::errors::UpdateAbortedByClosingWorkflow.into())
            }
            Ok(Ok(UpdateResolution::WorkflowContinuing)) => {
                Err(crate::errors::WorkflowClosing.into())
            }
            Ok(Ok(UpdateResolution::AcceptedRunClosed)) => Ok(UpdateLifecycleSnapshot {
                workflow_execution: ExecutionRef {
                    run_id: self.resolve_run_id_for_snapshot(run_key, &execution).await,
                    ..execution
                },
                update_id: update_id.clone(),
                update_name: String::new(),
                stage: UpdateLifecycleStage::Completed,
                outcome: Some(UpdateOutcome::AcceptedRunClosed),
            }),
            // The server failed the workflow task while this update was Sent
            // (bad-message validation, not an explicit RespondWorkflowTaskFailed):
            // v1.31.0 aborts with the non-retryable WorkflowNotReady
            // `workflowTaskFailErr` (abort_reason.go:86-103 @ v1.31.0), which the
            // edge maps to FAILED_PRECONDITION with `response` nil.
            Ok(Ok(UpdateResolution::AbortedByWftFailure)) => Err(crate::errors::WorkflowNotReady {
                message: "Unable to perform workflow execution update due to unexpected \
                              workflow task failure.",
            }
            .into()),
            // The worker completed its workflow task without processing this
            // Sent update: the server auto-rejects it with the
            // `unprocessedUpdateFailure` server outcome (Req 9). Like a normal
            // rejection this is a COMPLETED-stage response with a failure
            // outcome (no RPC error); the edge authors the failure proto.
            Ok(Ok(UpdateResolution::RejectedUnprocessed)) => Ok(UpdateLifecycleSnapshot {
                workflow_execution: ExecutionRef {
                    run_id: self.resolve_run_id_for_snapshot(run_key, &execution).await,
                    ..execution
                },
                update_id: update_id.clone(),
                update_name: String::new(),
                stage: UpdateLifecycleStage::Completed,
                outcome: Some(UpdateOutcome::RejectedUnprocessed),
            }),
            Ok(Err(_)) => Err(anyhow!("update response channel closed")),
            Err(_) => {
                self.update_registry.clear_waiter(run_key, &update_id);
                self.snapshot_or_update_not_found(run_key, execution, &update_id)
                    .await
            }
        }
    }

    async fn resolve_run_id_for_snapshot(
        &self,
        run_key: RunKey,
        execution: &ExecutionRef,
    ) -> Option<tokeira_types::RunId> {
        if execution.run_id.is_some() {
            return execution.run_id;
        }
        match self.repo.load_run(run_key).await {
            Ok(LoadedRun::Existing(state)) => Some(state.run_id),
            _ => None,
        }
    }

    async fn snapshot_or_update_not_found(
        &self,
        run_key: RunKey,
        execution: ExecutionRef,
        update_id: &str,
    ) -> Result<UpdateLifecycleSnapshot> {
        self.update_lifecycle_snapshot(run_key, execution, update_id)
            .await?
            .ok_or_else(|| {
                UpdateLifecycleError::UpdateNotFound {
                    run_key,
                    update_id: update_id.to_string(),
                }
                .into()
            })
    }

    pub(crate) async fn update_lifecycle_snapshot(
        &self,
        run_key: RunKey,
        execution: ExecutionRef,
        update_id: &str,
    ) -> Result<Option<UpdateLifecycleSnapshot>> {
        let state = match self.repo.load_run(run_key).await? {
            LoadedRun::Existing(state) => state,
            LoadedRun::Absent => return Ok(None),
        };
        let workflow_execution = ExecutionRef {
            run_id: Some(state.run_id),
            ..execution
        };
        let history = self.repo.read_history(run_key, 0, usize::MAX).await?;

        let mut accepted = None;
        for event in history {
            match event.kind {
                HistoryEventKind::WorkflowExecutionUpdateAccepted {
                    update_id: event_update_id,
                    update_name,
                    ..
                } if event_update_id == update_id => {
                    accepted = Some((event.event_id, update_name));
                }
                HistoryEventKind::WorkflowExecutionUpdateCompleted {
                    update_id: event_update_id,
                    accepted_event_id,
                    result,
                } if event_update_id == update_id => {
                    return Ok(Some(UpdateLifecycleSnapshot {
                        workflow_execution: workflow_execution.clone(),
                        update_id: update_id.to_string(),
                        update_name: accepted
                            .as_ref()
                            .map(|(_, name)| name.clone())
                            .unwrap_or_default(),
                        stage: UpdateLifecycleStage::Completed,
                        outcome: Some(UpdateOutcome::Completed {
                            accepted_event_id,
                            result,
                        }),
                    }));
                }
                // Failure-capable successor of the arm above (spec
                // speculative-wft K6, Req 7.2): a Success outcome reads as
                // Completed; a Failure outcome reads as the failure-shaped
                // outcome — the same caller-visible `Outcome{Failure}` a
                // rejection produces (v1.31.0 renders both identically on
                // the wire).
                HistoryEventKind::WorkflowExecutionUpdateCompletedV2 {
                    update_id: event_update_id,
                    update_name,
                    accepted_event_id,
                    outcome,
                } if event_update_id == update_id => {
                    let resolved_name = if update_name.is_empty() {
                        accepted
                            .as_ref()
                            .map(|(_, name)| name.clone())
                            .unwrap_or_default()
                    } else {
                        update_name
                    };
                    let outcome = match outcome {
                        tokeira_kernel::UpdateEventOutcome::Success(result) => {
                            UpdateOutcome::Completed {
                                accepted_event_id,
                                result,
                            }
                        }
                        tokeira_kernel::UpdateEventOutcome::Failure(failure) => {
                            UpdateOutcome::Rejected {
                                accepted_event_id,
                                failure,
                            }
                        }
                    };
                    return Ok(Some(UpdateLifecycleSnapshot {
                        workflow_execution: workflow_execution.clone(),
                        update_id: update_id.to_string(),
                        update_name: resolved_name,
                        stage: UpdateLifecycleStage::Completed,
                        outcome: Some(outcome),
                    }));
                }
                HistoryEventKind::WorkflowExecutionUpdateRejected {
                    update_id: event_update_id,
                    failure,
                    rejected_request_sequencing_event_id,
                    ..
                } if event_update_id == update_id => {
                    return Ok(Some(UpdateLifecycleSnapshot {
                        workflow_execution: workflow_execution.clone(),
                        update_id: update_id.to_string(),
                        update_name: accepted
                            .as_ref()
                            .map(|(_, name)| name.clone())
                            .unwrap_or_default(),
                        stage: UpdateLifecycleStage::Completed,
                        outcome: Some(UpdateOutcome::Rejected {
                            accepted_event_id: rejected_request_sequencing_event_id,
                            failure,
                        }),
                    }));
                }
                _ => {}
            }
        }

        if let Some((_accepted_event_id, update_name)) = accepted {
            // An accepted-but-never-completed update on a CLOSED run resolves
            // as COMPLETED with the server-authored completed-before failure —
            // v1.31.0's registry rebuild aborts the rehydrated Accepted state
            // with `acceptedUpdateCompletedWorkflowFailure` when the workflow
            // is no longer running (registry.go:455-484 @ v1.31.0).
            if !state.is_open() {
                return Ok(Some(UpdateLifecycleSnapshot {
                    workflow_execution: workflow_execution.clone(),
                    update_id: update_id.to_string(),
                    update_name,
                    stage: UpdateLifecycleStage::Completed,
                    outcome: Some(UpdateOutcome::AcceptedRunClosed),
                }));
            }
            return Ok(Some(UpdateLifecycleSnapshot {
                workflow_execution: workflow_execution.clone(),
                update_id: update_id.to_string(),
                update_name,
                stage: UpdateLifecycleStage::Accepted,
                outcome: None,
            }));
        }

        if state.admitted_updates.contains(update_id)
            || self
                .update_registry
                .contains_registered_update(run_key, update_id)
        {
            let (update_name, _) = self
                .update_registry
                .peek_update_info(run_key, update_id)
                .unwrap_or_default();
            return Ok(Some(UpdateLifecycleSnapshot {
                workflow_execution: workflow_execution.clone(),
                update_id: update_id.to_string(),
                update_name,
                stage: UpdateLifecycleStage::Admitted,
                outcome: None,
            }));
        }

        Ok(None)
    }

    async fn wait_for_history_stage(
        &self,
        run_key: RunKey,
        execution: ExecutionRef,
        update_id: String,
        requested: UpdateLifecycleStage,
        timeout_after: Duration,
        mut latest: UpdateLifecycleSnapshot,
    ) -> Result<UpdateLifecycleSnapshot> {
        let timeout_after: std::time::Duration = timeout_after
            .try_into()
            .map_err(|_| anyhow!("update timeout must be non-negative"))?;
        let deadline = tokio::time::Instant::now() + timeout_after;
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(50));

        while tokio::time::Instant::now() < deadline {
            interval.tick().await;
            if let Some(snapshot) = self
                .update_lifecycle_snapshot(run_key, execution.clone(), &update_id)
                .await?
            {
                if snapshot.stage >= requested {
                    return Ok(snapshot);
                }
                latest = snapshot;
            }
        }

        Ok(latest)
    }
}

fn requested_stage(wait_policy: UpdateWaitPolicy) -> UpdateLifecycleStage {
    match wait_policy {
        UpdateWaitPolicy::Unspecified => UpdateLifecycleStage::Unspecified,
        UpdateWaitPolicy::Admitted => UpdateLifecycleStage::Admitted,
        UpdateWaitPolicy::Accepted => UpdateLifecycleStage::Accepted,
        UpdateWaitPolicy::Completed => UpdateLifecycleStage::Completed,
    }
}

/// A kernel `DuplicateUpdateId` reject surfaced through the lane — the update
/// id is already admitted or accepted on the run; the caller ATTACHES instead
/// of erroring (`FindOrCreate` dedupe, registry.go:398-452 @ v1.31.0).
fn is_duplicate_update_reject(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<crate::KernelRejected>()
        .is_some_and(|rejected| matches!(rejected.0, tokeira_kernel::Reject::DuplicateUpdateId(_)))
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use time::{Duration, OffsetDateTime};

    use super::query_sticky_deadline;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn query_deadline_is_derived_from_enqueue_time(
            completion_age_seconds in 0i64..86_400,
            enqueue_seconds in 0i64..86_400,
            timeout_seconds in 1i64..600,
        ) {
            // Feature: api-conformance-client-misc, Property 6: derived dispatch and query deadlines
            let completion_time =
                OffsetDateTime::UNIX_EPOCH + Duration::seconds(enqueue_seconds - completion_age_seconds);
            let enqueued_at = OffsetDateTime::UNIX_EPOCH + Duration::seconds(enqueue_seconds);
            let deadline =
                query_sticky_deadline(enqueued_at, Duration::seconds(timeout_seconds));

            prop_assert_eq!(
                deadline,
                enqueued_at + Duration::seconds(timeout_seconds)
            );
            prop_assert_eq!(
                deadline - completion_time,
                Duration::seconds(timeout_seconds + completion_age_seconds)
            );
        }
    }
}
