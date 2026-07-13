//! Workflow lifecycle methods of [`TokeiraRuntime`].
//!
//! This `impl` continuation owns the client-facing execution-lifecycle surface:
//! starting runs (plain, policy-resolved, and signal-with-start), and the
//! external mutations that steer a live run — signal, terminate, cancel,
//! pause/unpause, and reset. Each public method resolves the target run and
//! applies a single kernel command through [`submit`](TokeiraRuntime::submit),
//! so history remains the authority and dispatch is a derived effect.
//!
//! The non-obvious weight here is WorkflowId conflict/reuse resolution: before
//! a start is admitted, [`resolve_conflict`](TokeiraRuntime::resolve_conflict)
//! decides whether to start fresh, reuse an existing run, terminate-then-start,
//! or reject — matching Temporal's `WorkflowIdConflictPolicy` (against an
//! *open* run) and `WorkflowIdReusePolicy` (against the *latest closed* run).
use super::*;

/// Temporal v1.31.0's eager-workflow-start compatibility value is enabled by
/// default. Keeping it pinned avoids an operator setting changing API behaviour
/// (`common/dynamicconfig/constants.go @ v1.31.0`).
const ENABLE_EAGER_WORKFLOW_START: bool = true;

fn workflow_deletion_termination(request: &DeleteWorkflowRequest) -> TerminateRequest {
    let mut terminate_context = request.request.clone();
    // This internal termination has its own stable dedupe identity: reusing an
    // arbitrary client request id could collide with an earlier signal/update
    // and leave the run open forever. Temporal authors both values in the
    // history service (`service/history/api/deleteworkflow/api.go @ v1.31.0`).
    terminate_context.request_id = tokeira_types::RequestId(format!(
        "delete-workflow-execution:{}",
        terminate_context.request_id.0
    ));
    terminate_context.caller_identity = Some("history-service".to_owned());
    TerminateRequest {
        reason: "Delete workflow execution".to_owned(),
        details: None,
        identity: "history-service".to_owned(),
        links: Vec::new(),
        request: terminate_context,
        now: request.now,
    }
}

#[derive(Debug)]
struct StartCommitOutcome {
    commit_result: CommitResult,
    eager_workflow_task: Option<StartedWorkflowTask>,
}

impl<R> TokeiraRuntime<R>
where
    R: RunRepository + 'static,
{
    /// Start a new workflow execution.
    ///
    /// This low-level entry point cannot return an eager task, so it disables
    /// public eager admission. It may still reserve a parked broker poller for
    /// the independent synchronous-match optimisation. That reservation is
    /// returned on every non-`Applied` outcome and submit error, so a failed
    /// start never leaks a poller. Execution/run timeout tracking begins only
    /// after the start is durably `Applied`.
    pub async fn start_workflow(&self, request: StartRequest) -> Result<CommitResult> {
        Ok(self
            .start_workflow_inner(request, None, false)
            .await?
            .commit_result)
    }

    /// Start with an optional Update-with-Start fold: `Some(update_id)` swaps
    /// the submitted command for `Command::StartAndUpdate` so the run creation
    /// and the update admission commit in ONE transition (the fresh-start leg
    /// of ExecuteMultiOperation; multioperation/api.go @ v1.31.0 admits the
    /// update into the new run's registry before the persistence write). All
    /// other start effects (reserved-poller sync match, timeout tracking) are
    /// identical.
    async fn start_workflow_inner(
        &self,
        mut request: StartRequest,
        update_fold: Option<String>,
        allow_eager_response: bool,
    ) -> Result<StartCommitOutcome> {
        prepare_start_request(&mut request, allow_eager_response)?;
        self.start_workflow_prepared(request, update_fold).await
    }

    async fn start_workflow_prepared(
        &self,
        mut request: StartRequest,
        update_fold: Option<String>,
    ) -> Result<StartCommitOutcome> {
        // A delayed start (client start-delay or cron initial backoff) arms
        // the start-delay timer instead of scheduling a first WFT — there is
        // nothing to hand a reserved poller, so do not reserve one (the
        // sync-match delivery path requires a pending WFT and would
        // otherwise fail the whole Start RPC with "workflow task missing
        // after reserved start").
        let has_positive_backoff = request
            .workflow_start_delay
            .is_some_and(|delay| delay > Duration::ZERO);
        let eager_worker_identity = request.eager_execution_accepted.then(|| {
            // Temporal passes the request identity straight into the eager
            // WorkflowTaskStarted event; the proto string may be empty
            // (create_workflow_util.go @ v1.31.0).
            WorkerIdentity(request.request.caller_identity.clone().unwrap_or_default())
        });
        // Runtime admission owns this direct-delivery identity; never trust a
        // pre-populated value from an internal caller.
        request.reserved_poller_identity = eager_worker_identity.clone();
        let reserved_poller = if eager_worker_identity.is_none() && !has_positive_backoff {
            self.try_reserve_start_poller(&request).await
        } else {
            None
        };
        if let Some(reserved) = &reserved_poller {
            request.reserved_poller_identity = Some(reserved.worker_identity().clone());
        }

        let command = match update_fold {
            Some(update_id) => Command::StartAndUpdate(tokeira_kernel::StartAndUpdateRequest {
                start: request.clone(),
                update_id,
            }),
            None => Command::Start(request.clone()),
        };
        let result = match self.submit(request.run_key, command).await {
            Ok(result) => result,
            Err(error) => {
                if let Some(reserved) = reserved_poller {
                    self.broker.return_reserved_poller(reserved).await;
                }
                return Err(error);
            }
        };
        // Register run-level deadlines before response materialization: task
        // token construction is fallible after the authoritative commit.
        if matches!(result, CommitResult::Applied { .. })
            && (request.workflow_execution_timeout.is_some()
                || request.workflow_run_timeout.is_some())
        {
            let shard_id = self.shard_id_for(request.run_key).await;
            self.workflow_timeout_tracking.insert(WorkflowTimeoutEntry {
                run_key: request.run_key,
                shard_id,
                workflow_execution_timeout: request.workflow_execution_timeout,
                workflow_run_timeout: request.workflow_run_timeout,
                started_at: request.now,
                workflow_start_delay: request.workflow_start_delay,
                first_run_started_at: request.first_run_started_at,
                has_retry_policy: request.retry_policy.is_some(),
            });
        }

        let eager_workflow_task = match (&result, eager_worker_identity, reserved_poller) {
            (CommitResult::Applied { new_state }, Some(worker_identity), None) => Some(
                self.started_workflow_task_from_state(new_state, false, worker_identity)
                    .await?,
            ),
            (CommitResult::Applied { new_state }, None, Some(reserved)) => {
                // The run is durably started; a failed sync-match delivery
                // must not fail the Start RPC. The broker-enqueue op was
                // suppressed for a reserved start, so recovery is the WFT
                // start-to-close timeout scanner (the tracking entry is
                // inserted before delivery) or, on a shard handover, the new
                // owner's sweep reconstructing it from committed state.
                if let Err(error) = self
                    .deliver_reserved_start_workflow_task(new_state, reserved)
                    .await
                {
                    tracing::warn!(
                        ?error,
                        run_key = ?request.run_key,
                        "reserved-start sync-match delivery failed; WFT timeout scanner will recover"
                    );
                }
                None
            }
            (_, _, Some(reserved)) => {
                self.broker.return_reserved_poller(reserved).await;
                None
            }
            (_, _, None) => None,
        };
        Ok(StartCommitOutcome {
            commit_result: result,
            eager_workflow_task,
        })
    }

    /// Start a workflow, first resolving any WorkflowId conflict/reuse policy.
    ///
    /// This is the policy-aware entry point behind StartWorkflowExecution. It
    /// consults `resolve_conflict` and then either
    /// starts a new run, returns the existing run (UseExisting), terminates the
    /// running execution before starting (TerminateExisting), or rejects. A
    /// `Duplicate` commit here is treated as a hard error rather than success:
    /// conflict resolution already established that a fresh start was warranted,
    /// so a dedupe hit indicates an unexpected racing start for the same run key.
    pub async fn start_workflow_with_policy(
        &self,
        mut request: StartRequest,
    ) -> Result<StartWorkflowResult> {
        // v1.31.0 applies the effective first-WFT backoff before eager gating
        // and conflict handling, including the request-id retry path
        // (service/history/api/startworkflow/api.go:137-158 @ v1.31.0).
        prepare_start_request(&mut request, true)?;
        // Bounded re-resolution loop. `resolve_conflict` is a pre-check that can
        // race a concurrent start: N starts for the same workflow id can all see
        // "absent" and proceed, then collide at commit. The loser's commit returns
        // `CurrentExecutionConflict` (the lane does not OCC-retry it); we loop and
        // re-resolve, now seeing the committed incumbent, and apply the request's
        // WorkflowIdConflictPolicy deterministically — exactly one start wins, the
        // rest Fail→Rejected / UseExisting→attach / TerminateExisting→terminate+start
        // (`ResolveWorkflowIDConflictPolicy @ v1.31.0`). The bound guards against a
        // pathological churn of repeatedly-terminated incumbents.
        const MAX_RESOLUTION_ATTEMPTS: usize = 5;
        for _ in 0..MAX_RESOLUTION_ATTEMPTS {
            let resolution = self
                .resolve_conflict(
                    request.namespace_id,
                    &request.workflow_id,
                    &request.request.request_id.0,
                    request.conflict_policy,
                    request.reuse_policy,
                )
                .await?;
            match resolution {
                ConflictResolution::Absent | ConflictResolution::ClosedAllowReuse => {
                    let StartCommitOutcome {
                        commit_result,
                        eager_workflow_task,
                    } = self.start_workflow_prepared(request.clone(), None).await?;
                    match commit_result {
                        CommitResult::Applied { new_state } => {
                            return Ok(StartWorkflowResult::Started {
                                run_key: request.run_key,
                                run_id: request.run_id,
                                mutation_metadata: mutation_metadata(&new_state),
                                eager_workflow_task,
                            });
                        }
                        CommitResult::Duplicate => {
                            return Err(anyhow!(
                                "unexpected duplicate start commit for {:?}",
                                request.run_key
                            ));
                        }
                        CommitResult::Conflict { reason } => {
                            return Err(anyhow!("conflict: {reason}"));
                        }
                        // Lost the start race; the incumbent is now committed, so
                        // re-resolve and apply the conflict policy against it.
                        CommitResult::CurrentExecutionConflict { .. } => continue,
                    }
                }
                ConflictResolution::UseExisting { run_key, run_id } => {
                    self.apply_start_on_conflict_options(run_key, &request)
                        .await?;
                    return Ok(StartWorkflowResult::UsedExisting { run_key, run_id });
                }
                ConflictResolution::TerminateAndStart { run_key } => {
                    self.terminate_existing_for_conflict(
                        request.namespace_id,
                        request.workflow_id.clone(),
                        run_key,
                        request.request.clone(),
                    )
                    .await?;
                    let StartCommitOutcome {
                        commit_result,
                        eager_workflow_task,
                    } = self.start_workflow_prepared(request.clone(), None).await?;
                    match commit_result {
                        CommitResult::Applied { new_state } => {
                            return Ok(StartWorkflowResult::Started {
                                run_key: request.run_key,
                                run_id: request.run_id,
                                mutation_metadata: mutation_metadata(&new_state),
                                eager_workflow_task,
                            });
                        }
                        CommitResult::Duplicate => {
                            return Err(anyhow!(
                                "unexpected duplicate start commit for {:?}",
                                request.run_key
                            ));
                        }
                        CommitResult::Conflict { reason } => {
                            return Err(anyhow!("conflict: {reason}"));
                        }
                        // Another start claimed the slot we just freed; re-resolve.
                        CommitResult::CurrentExecutionConflict { .. } => continue,
                    }
                }
                ConflictResolution::Rejected {
                    run_key,
                    run_id,
                    reason,
                } => {
                    return Ok(StartWorkflowResult::Rejected {
                        run_key,
                        run_id,
                        reason,
                    });
                }
                ConflictResolution::DedupRetried {
                    run_key,
                    run_id,
                    execution_status,
                } => {
                    let eager_workflow_task = self
                        .reconstruct_retried_eager_workflow_task(run_key, &request)
                        .await?;
                    return Ok(StartWorkflowResult::Deduped {
                        run_key,
                        run_id,
                        execution_status,
                        eager_workflow_task,
                    });
                }
            }
        }
        Err(anyhow!(
            "workflow start for {:?} did not converge after {MAX_RESOLUTION_ATTEMPTS} conflict-resolution attempts",
            request.run_key
        ))
    }

    /// Rebuild the first eager WFT for an idempotent start retry without
    /// authoring another transition.
    ///
    /// Temporal returns it only while event 3 remains the started first
    /// attempt (`respondToRetriedRequest`, startworkflow/api.go:538-576 @
    /// v1.31.0). Tokeira additionally checks the absolute deadline because its
    /// non-speculative timeout sweep is coarse: elapsed work is no longer live
    /// even if the timeout transition lands on the next scan.
    async fn reconstruct_retried_eager_workflow_task(
        &self,
        run_key: RunKey,
        request: &StartRequest,
    ) -> Result<Option<StartedWorkflowTask>> {
        if !request.eager_execution_accepted {
            return Ok(None);
        }
        let start_event = self.repo.read_history(run_key, 0, 1).await?;
        if !start_event
            .first()
            .is_some_and(|event| event.kind.eager_execution_accepted())
        {
            // A duplicate request may change its eager flag. Event 1 records
            // the original admission decision, so request shape alone must not
            // turn a non-eager sync match into an eager response.
            return Ok(None);
        }
        let LoadedRun::Existing(state) = self.repo.load_run(run_key).await? else {
            return Ok(None);
        };
        let Some(pending) = state.pending_workflow_task.as_ref() else {
            return Ok(None);
        };
        let Some(started_at) = pending.started_at else {
            return Ok(None);
        };
        if pending.started_event_id != Some(3)
            || pending.attempt != 1
            || request.now >= started_at + state.workflow_task_timeout
        {
            return Ok(None);
        }

        let worker_identity =
            WorkerIdentity(request.request.caller_identity.clone().unwrap_or_default());
        self.started_workflow_task_from_state(&state, false, worker_identity)
            .await
            .map(Some)
    }

    async fn apply_start_on_conflict_options(
        &self,
        run_key: RunKey,
        request: &StartRequest,
    ) -> Result<()> {
        let Some(options) = &request.on_conflict_options else {
            return Ok(());
        };
        let attached_request_id = options
            .attach_request_id
            .then(|| request.request.request_id.0.clone());
        let attached_completion_callbacks = if options.attach_completion_callbacks {
            request.completion_callbacks.clone()
        } else {
            Vec::new()
        };
        let attached_links = if options.attach_links {
            request.links.clone()
        } else {
            Vec::new()
        };
        if attached_request_id.is_none()
            && attached_completion_callbacks.is_empty()
            && attached_links.is_empty()
        {
            return Ok(());
        }

        // Temporal applies `OnConflictOptions` to the already-running
        // execution instead of rewriting the original start. Keeping that as a
        // kernel command preserves Tokeira's "history is authority" rule.
        let update = UpdateExecutionOptionsRequest {
            versioning_override: FieldChange::Unchanged,
            completion_callbacks: FieldChange::Unchanged,
            attached_completion_callbacks,
            attached_links,
            attached_request_id,
            request: request.request.clone(),
            now: request.now,
        };
        match self
            .submit(run_key, Command::UpdateExecutionOptions(update))
            .await?
        {
            CommitResult::Applied { .. } | CommitResult::Duplicate => Ok(()),
            CommitResult::Conflict { reason } => Err(anyhow!("conflict: {reason}")),
            CommitResult::CurrentExecutionConflict {
                existing_run_key, ..
            } => Err(anyhow!(
                "current execution already exists: {existing_run_key:?}"
            )),
        }
    }

    /// Signal a workflow, starting it first if it does not already exist.
    ///
    /// Resolves the WorkflowId conflict/reuse policy the same way as
    /// [`start_workflow_with_policy`](Self::start_workflow_with_policy): an
    /// absent-or-reusable target is signal-with-started atomically; an existing
    /// open run is signalled in place (UseExisting); TerminateExisting
    /// terminates then starts. Signalling an existing run tolerates a
    /// `Duplicate` commit as success because the signal request may be retried,
    /// whereas the start branches reject duplicates as unexpected.
    /// Execute the composed Update-with-Start: exactly `[Start, Update]`.
    ///
    /// Decision ladder mirrors `multioperation/api.go @ v1.31.0`, consulting
    /// the current/latest run BEFORE start adjudication: (1) an update id the
    /// run already knows replays/attaches with NO mutation — durable outcomes
    /// replay even on CLOSED workflows, and an in-flight update attaches even
    /// under TERMINATE_EXISTING (the "given an accepted update, attach to it"
    /// behavior); (2) request-id dedup and USE_EXISTING attach to the running
    /// incumbent; (3) conflict-policy FAIL rejects the start leg; (4) fresh
    /// starts fold run creation + update admission into ONE
    /// `Command::StartAndUpdate` transition so a crash can never separate
    /// them.
    #[allow(clippy::too_many_arguments)]
    pub async fn execute_multi_operation(
        &self,
        request: StartRequest,
        update_id: String,
        update_name: String,
        update_input: Payloads,
        update_request: RequestContext,
        update_timeout: Duration,
        wait_policy: UpdateWaitPolicy,
    ) -> Result<MultiOperationResult> {
        let current = ExecutionRef {
            namespace_id: request.namespace_id,
            workflow_id: request.workflow_id.clone(),
            run_id: None,
        };
        let latest_run = match self.repo.resolve_execution(&current).await? {
            Some(run_key) => Some(run_key),
            None => {
                self.repo
                    .find_latest_run(request.namespace_id, &request.workflow_id)
                    .await?
            }
        };
        if let Some(run_key) = latest_run
            && self
                .update_lifecycle_snapshot(run_key, current.clone(), &update_id)
                .await?
                .is_some()
        {
            let LoadedRun::Existing(state) = self.repo.load_run(run_key).await? else {
                return Err(anyhow!("latest run vanished during update-with-start"));
            };
            let run_id = state.run_id;
            let execution_status = state.status;
            let update = self
                .wait_for_update_stage(
                    run_key,
                    current.clone(),
                    update_id,
                    wait_policy,
                    update_timeout,
                )
                .await
                .map_err(|source| MultiOperationError::UpdateFailed {
                    started: false,
                    source,
                })?;
            return Ok(MultiOperationResult {
                run_key,
                run_id,
                started: false,
                execution_status,
                update,
            });
        }

        // Same bounded re-resolution loop as `start_workflow_with_policy`:
        // a lost start race re-resolves against the committed incumbent.
        const MAX_RESOLUTION_ATTEMPTS: usize = 5;
        for _ in 0..MAX_RESOLUTION_ATTEMPTS {
            let resolution = self
                .resolve_conflict(
                    request.namespace_id,
                    &request.workflow_id,
                    &request.request.request_id.0,
                    request.conflict_policy,
                    request.reuse_policy,
                )
                .await?;
            match resolution {
                ConflictResolution::Absent | ConflictResolution::ClosedAllowReuse => {
                    match self
                        .start_and_update_fresh(
                            &request,
                            &update_id,
                            &update_name,
                            &update_input,
                            &update_request,
                            update_timeout,
                            wait_policy.clone(),
                        )
                        .await?
                    {
                        Some(result) => return Ok(result),
                        None => continue,
                    }
                }
                ConflictResolution::TerminateAndStart { run_key } => {
                    self.terminate_existing_for_conflict(
                        request.namespace_id,
                        request.workflow_id.clone(),
                        run_key,
                        request.request.clone(),
                    )
                    .await?;
                    match self
                        .start_and_update_fresh(
                            &request,
                            &update_id,
                            &update_name,
                            &update_input,
                            &update_request,
                            update_timeout,
                            wait_policy.clone(),
                        )
                        .await?
                    {
                        Some(result) => return Ok(result),
                        None => continue,
                    }
                }
                ConflictResolution::UseExisting { run_key, run_id }
                | ConflictResolution::DedupRetried {
                    run_key, run_id, ..
                } => {
                    let execution = ExecutionRef {
                        namespace_id: request.namespace_id,
                        workflow_id: request.workflow_id.clone(),
                        run_id: Some(run_id),
                    };
                    let update = self
                        .update_workflow(
                            execution,
                            update_id,
                            update_name,
                            update_input,
                            update_request,
                            update_timeout,
                            wait_policy,
                        )
                        .await
                        .map_err(|source| MultiOperationError::UpdateFailed {
                            started: false,
                            source,
                        })?;
                    return Ok(MultiOperationResult {
                        run_key,
                        run_id,
                        started: false,
                        execution_status: ExecutionStatus::Running,
                        update,
                    });
                }
                ConflictResolution::Rejected {
                    run_key,
                    run_id,
                    reason,
                } => {
                    return Err(MultiOperationError::StartRejected {
                        run_key,
                        run_id,
                        reason,
                    }
                    .into());
                }
            }
        }
        Err(anyhow!(
            "update-with-start exceeded conflict resolution attempts"
        ))
    }

    /// Fresh-start leg: register the update transport BEFORE the commit (the
    /// registry carries the name/input the first WFT delivery needs; the
    /// kernel folds only the id), submit the atomic `StartAndUpdate`, then
    /// drive the update wait. Returns `None` on a lost start race so the
    /// caller re-resolves.
    #[allow(clippy::too_many_arguments)]
    async fn start_and_update_fresh(
        &self,
        request: &StartRequest,
        update_id: &str,
        update_name: &str,
        update_input: &Payloads,
        update_request: &RequestContext,
        update_timeout: Duration,
        wait_policy: UpdateWaitPolicy,
    ) -> Result<Option<MultiOperationResult>> {
        let (wait_tx, wait_rx) = oneshot::channel();
        self.update_registry.register(
            request.run_key,
            update_id.to_string(),
            update_name.to_string(),
            update_input.clone(),
            update_request.caller_identity.clone().unwrap_or_default(),
            wait_policy.clone(),
            wait_tx,
        );
        match self
            .start_workflow_inner(request.clone(), Some(update_id.to_string()), false)
            .await
        {
            Ok(StartCommitOutcome {
                commit_result: CommitResult::Applied { .. } | CommitResult::Duplicate,
                ..
            }) => {}
            Ok(StartCommitOutcome {
                commit_result: CommitResult::CurrentExecutionConflict { .. },
                ..
            }) => {
                self.update_registry.remove(request.run_key, update_id);
                return Ok(None);
            }
            Ok(StartCommitOutcome {
                commit_result: CommitResult::Conflict { reason },
                ..
            }) => {
                self.update_registry.remove(request.run_key, update_id);
                return Err(anyhow!("update-with-start start conflicted: {reason}"));
            }
            Err(error) => {
                self.update_registry.remove(request.run_key, update_id);
                return Err(error);
            }
        }
        let execution = ExecutionRef {
            namespace_id: request.namespace_id,
            workflow_id: request.workflow_id.clone(),
            run_id: Some(request.run_id),
        };
        let update = self
            .wait_for_update_stage_with_receiver(
                request.run_key,
                execution,
                update_id.to_string(),
                wait_policy,
                update_timeout,
                wait_rx,
            )
            .await
            .map_err(|source| MultiOperationError::UpdateFailed {
                started: true,
                source,
            })?;
        Ok(Some(MultiOperationResult {
            run_key: request.run_key,
            run_id: request.run_id,
            started: true,
            execution_status: ExecutionStatus::Running,
            update,
        }))
    }

    pub async fn signal_with_start_workflow(
        &self,
        mut request: SignalWithStartRequest,
    ) -> Result<SignalWithStartResult> {
        apply_client_cron_signal_backoff(&mut request)?;
        let resolution = self
            .resolve_conflict(
                request.namespace_id,
                &request.workflow_id,
                &request.request.request_id.0,
                request.conflict_policy,
                request.reuse_policy,
            )
            .await?;
        match resolution {
            ConflictResolution::Absent | ConflictResolution::ClosedAllowReuse => {
                match self
                    .submit(request.run_key, Command::SignalWithStart(request.clone()))
                    .await?
                {
                    CommitResult::Applied { .. } => Ok(SignalWithStartResult::Started {
                        run_key: request.run_key,
                        run_id: request.run_id,
                    }),
                    // A duplicate here means THIS run already accepted this
                    // request id (the dedupe table is run-scoped) — a redriven
                    // commit of the same start. Effectively unreachable (the
                    // OCC seq check fires first for an existing run), mapped
                    // to idempotent success defensively rather than erroring.
                    CommitResult::Duplicate => Ok(SignalWithStartResult::Started {
                        run_key: request.run_key,
                        run_id: request.run_id,
                    }),
                    CommitResult::Conflict { reason } => Err(anyhow!("conflict: {reason}")),
                    CommitResult::CurrentExecutionConflict {
                        existing_run_key, ..
                    } => Err(anyhow!(
                        "current execution already exists: {existing_run_key:?}"
                    )),
                }
            }
            ConflictResolution::UseExisting { run_key, run_id } => {
                let execution = ExecutionRef {
                    namespace_id: request.namespace_id,
                    workflow_id: request.workflow_id.clone(),
                    run_id: Some(run_id),
                };
                match self
                    .signal_workflow(
                        execution,
                        SignalRequest {
                            signal_name: request.signal_name,
                            input: request.signal_input,
                            header: request.header,
                            links: request.links,
                            request: request.request,
                            now: request.now,
                        },
                    )
                    .await?
                {
                    CommitResult::Applied { .. } | CommitResult::Duplicate => {
                        Ok(SignalWithStartResult::Signaled { run_key, run_id })
                    }
                    CommitResult::Conflict { reason } => Err(anyhow!("conflict: {reason}")),
                    CommitResult::CurrentExecutionConflict {
                        existing_run_key, ..
                    } => Err(anyhow!(
                        "current execution already exists: {existing_run_key:?}"
                    )),
                }
            }
            ConflictResolution::TerminateAndStart { run_key } => {
                self.terminate_existing_for_conflict(
                    request.namespace_id,
                    request.workflow_id.clone(),
                    run_key,
                    request.request.clone(),
                )
                .await?;
                match self
                    .submit(request.run_key, Command::SignalWithStart(request.clone()))
                    .await?
                {
                    CommitResult::Applied { .. } => Ok(SignalWithStartResult::Started {
                        run_key: request.run_key,
                        run_id: request.run_id,
                    }),
                    // A duplicate here means THIS run already accepted this
                    // request id (the dedupe table is run-scoped) — a redriven
                    // commit of the same start. Effectively unreachable (the
                    // OCC seq check fires first for an existing run), mapped
                    // to idempotent success defensively rather than erroring.
                    CommitResult::Duplicate => Ok(SignalWithStartResult::Started {
                        run_key: request.run_key,
                        run_id: request.run_id,
                    }),
                    CommitResult::Conflict { reason } => Err(anyhow!("conflict: {reason}")),
                    CommitResult::CurrentExecutionConflict {
                        existing_run_key, ..
                    } => Err(anyhow!(
                        "current execution already exists: {existing_run_key:?}"
                    )),
                }
            }
            ConflictResolution::Rejected {
                run_key,
                run_id,
                reason,
            } => Ok(SignalWithStartResult::Rejected {
                run_key,
                run_id,
                reason,
            }),
            // A retried signal-with-start whose RequestId authored the incumbent's start is
            // idempotent: return the existing run (mirrors the start dedup path, api.go:332).
            ConflictResolution::DedupRetried {
                run_key, run_id, ..
            } => Ok(SignalWithStartResult::Started { run_key, run_id }),
        }
    }

    /// Deliver an external signal to a running workflow.
    pub async fn signal_workflow(
        &self,
        execution: ExecutionRef,
        request: SignalRequest,
    ) -> Result<CommitResult> {
        let run_key = self
            .repo
            .resolve_execution(&execution)
            .await?
            .ok_or_else(|| anyhow!("execution not found"))?;
        // A workflow that already ATTEMPTED to close (a close command bounced
        // off buffered events with UnhandledCommand) rejects new signals while
        // the retrying WFT is started — otherwise a steady signal stream could
        // keep the close bouncing forever
        // (`IsWorkflowCloseAttempted() && HasStartedWorkflowTask()` →
        // ErrWorkflowClosing, signal_workflow_util.go:63-70 @ v1.31.0).
        if self
            .close_attempt_tracking
            .lock()
            .expect("close-attempt tracking lock")
            .contains(&run_key)
        {
            match self.repo.load_run(run_key).await? {
                LoadedRun::Existing(state) if state.status.is_open() => {
                    if state
                        .pending_workflow_task
                        .as_ref()
                        .is_some_and(|pending| pending.started_event_id.is_some())
                    {
                        return Err(crate::errors::WorkflowClosing.into());
                    }
                }
                // The run closed (or vanished) without another successful WFT
                // completion — drop the stale bit so the set stays bounded.
                _ => {
                    self.close_attempt_tracking
                        .lock()
                        .expect("close-attempt tracking lock")
                        .remove(&run_key);
                }
            }
        }
        self.submit(run_key, Command::Signal(request)).await
    }

    /// Clear a run's sticky affinity (`ResetStickyTaskQueue` @ v1.31.0;
    /// sticky raise S5). Any pending sticky-dispatched WFT keeps its
    /// schedule-to-start deadline — the reset only drops the affinity, so a
    /// parked sticky task still times out onto the normal queue.
    pub async fn reset_sticky_task_queue(&self, execution: ExecutionRef) -> Result<CommitResult> {
        let run_key = self
            .repo
            .resolve_execution(&execution)
            .await?
            .ok_or_else(|| anyhow!("execution not found"))?;
        self.submit(
            run_key,
            Command::ResetSticky(tokeira_kernel::ResetStickyRequest {
                now: OffsetDateTime::now_utc(),
            }),
        )
        .await
    }

    /// Forcibly terminate a workflow execution.
    pub async fn terminate_workflow(
        &self,
        execution: ExecutionRef,
        request: TerminateRequest,
    ) -> Result<CommitResult> {
        let run_key = self
            .repo
            .resolve_execution(&execution)
            .await?
            .ok_or_else(|| anyhow!("execution not found"))?;
        self.submit(run_key, Command::Terminate(request)).await
    }

    /// Terminate an open workflow if necessary, then authoritatively delete the
    /// exact run selected by the edge.
    ///
    /// Temporal v1.31.0 terminates an open target with reason `Delete workflow
    /// execution`, identity `history-service`, and `deleteAfterTerminate=true`
    /// before its durable delete task removes visibility/current/mutable/history
    /// (`service/history/api/deleteworkflow/api.go @ v1.31.0`). Tokeira keeps
    /// the semantic split: the ordinary kernel transition records termination;
    /// the repository performs the subsequent operational purge.
    pub async fn delete_workflow(
        &self,
        run_key: RunKey,
        request: DeleteWorkflowRequest,
    ) -> Result<WorkflowDeletion> {
        let mut conflicts = 0;
        loop {
            let state = match self.repo.load_run(run_key).await? {
                LoadedRun::Existing(state) => state,
                LoadedRun::Absent => return Err(WorkflowDeletionNotFound { run_key }.into()),
            };

            if state.status.is_open() {
                match self
                    .submit(
                        run_key,
                        Command::Terminate(workflow_deletion_termination(&request)),
                    )
                    .await
                {
                    Ok(_) => continue,
                    Err(error)
                        if error
                            .downcast_ref::<crate::lane::KernelRejected>()
                            .is_some_and(|rejected| {
                                matches!(rejected.0, tokeira_kernel::Reject::RunClosed(_))
                            }) =>
                    {
                        // Another serialized command closed the run first. A
                        // fresh load decides whether it is now deletable.
                        continue;
                    }
                    Err(error) => return Err(error),
                }
            }

            let (bundle, epoch) = {
                let owner = self.shard_owner.read().unwrap();
                let bundle = execution_home_bundle(
                    state.namespace_id.0.as_bytes(),
                    state.workflow_id.0.as_bytes(),
                    owner.shard_count(),
                );
                let local_epoch = owner
                    .epoch_of(bundle)
                    .ok_or_else(|| NotShardOwner::local(bundle, ShardEpoch::ZERO))?;
                if self.runtime_drain.is_draining() || !owner.is_active(bundle) {
                    return Err(NotShardOwner::local(bundle, local_epoch).into());
                }
                let commit_epoch = if self.config.controller_managed_placement {
                    local_epoch
                } else {
                    ShardEpoch::ZERO
                };
                (bundle, commit_epoch)
            };

            match self
                .repo
                .delete_run_for_bundle(
                    run_key,
                    bundle,
                    DeleteRunRequest {
                        expected_seq: state.transition_seq,
                        deleted_at: request.now,
                    },
                    epoch,
                )
                .await?
            {
                DeleteRunResult::Deleted { tombstone } => {
                    self.cleanup_deleted_run(run_key).await;
                    return Ok(WorkflowDeletion { tombstone });
                }
                DeleteRunResult::NotFound => {
                    return Err(WorkflowDeletionNotFound { run_key }.into());
                }
                DeleteRunResult::Conflict { reason } => {
                    if conflicts >= self.config.max_occ_retries {
                        return Err(anyhow!(
                            "workflow deletion OCC retries exhausted for {run_key:?}: {reason}"
                        ));
                    }
                    conflicts += 1;
                }
            }
        }
    }

    async fn cleanup_deleted_run(&self, run_key: RunKey) {
        self.workflow_timeout_tracking.remove(run_key);
        self.wft_timeout_tracking.remove(run_key);
        self.wft_timeout_tracking.disarm_speculative(run_key);
        self.activity_tracking.remove_all_for_run(run_key);
        self.nexus_timeout_tracking.remove_all_for_run(run_key);
        self.completion_callback_tracking
            .remove_all_for_run(run_key);
        self.update_registry.drain_for_run(run_key, false);
        self.buffered_queries
            .fail_run_queries(run_key, "workflow execution was deleted");
        self.close_attempt_tracking
            .lock()
            .expect("close-attempt tracking lock")
            .remove(&run_key);
        self.broker.remove_run(run_key).await;
        self.activity_broker.remove_run(run_key).await;
        self.nexus_task_broker.remove_run(run_key).await;
    }

    /// Apply an `UpdateWorkflowExecutionOptions` change to a running execution as a
    /// per-run `UpdateExecutionOptions` transition (the same kernel command the
    /// UseExisting-conflict attach path uses, but carrying only the `versioning_override`
    /// change). The kernel emits `WorkflowExecutionOptionsUpdated` and persists the
    /// override, so it survives restart and steers subsequent workflow-task dispatch.
    pub async fn update_workflow_execution_options(
        &self,
        execution: ExecutionRef,
        versioning_override: FieldChange<tokeira_kernel::VersioningOverride>,
        request: RequestContext,
    ) -> Result<CommitResult> {
        let run_key = self
            .repo
            .resolve_execution(&execution)
            .await?
            .ok_or_else(|| anyhow!("execution not found"))?;
        let update = UpdateExecutionOptionsRequest {
            versioning_override,
            completion_callbacks: FieldChange::Unchanged,
            attached_completion_callbacks: Vec::new(),
            attached_links: Vec::new(),
            attached_request_id: None,
            request,
            now: OffsetDateTime::now_utc(),
        };
        self.submit(run_key, Command::UpdateExecutionOptions(update))
            .await
    }

    /// Request cooperative cancellation of a workflow.
    pub async fn cancel_workflow(
        &self,
        execution: ExecutionRef,
        request: tokeira_kernel::CancelRequest,
    ) -> Result<CommitResult> {
        let run_key = self
            .repo
            .resolve_execution(&execution)
            .await?
            .ok_or_else(|| anyhow!("execution not found"))?;
        self.submit(run_key, Command::Cancel(request)).await
    }

    /// Pause a workflow execution so no new workflow tasks are dispatched.
    pub async fn pause_workflow(
        &self,
        execution: ExecutionRef,
        request: PauseWorkflowRequest,
    ) -> Result<CommitResult> {
        let run_key = self
            .repo
            .resolve_execution(&execution)
            .await?
            .ok_or_else(|| anyhow!("execution not found"))?;
        self.submit(run_key, Command::PauseWorkflow(request)).await
    }

    /// Resume a paused workflow execution. The committed transition carries the
    /// wakeup workflow-task dispatch op, which flows through the standard
    /// post-commit broker path like any other mutation.
    pub async fn unpause_workflow(
        &self,
        execution: ExecutionRef,
        request: UnpauseWorkflowRequest,
    ) -> Result<CommitResult> {
        let run_key = self
            .repo
            .resolve_execution(&execution)
            .await?
            .ok_or_else(|| anyhow!("execution not found"))?;
        self.submit(run_key, Command::UnpauseWorkflow(request))
            .await
    }

    /// Reset a workflow execution and synchronously materialize the replayed successor.
    pub async fn reset_workflow(
        &self,
        execution: ExecutionRef,
        request: tokeira_kernel::ResetRequest,
    ) -> Result<ResetWorkflowResult> {
        let run_key = self
            .repo
            .resolve_execution(&execution)
            .await?
            .ok_or_else(|| anyhow!("execution not found"))?;
        // v1.31.0 resets the BASE run but supersedes and terminates the chain's
        // CURRENT run when it is a DIFFERENT, still-running run
        // (`workflow_resetter.go` terminateWorkflow, identity=IdentityResetter). The
        // base==current-running case is already handled by `apply_reset` terminating
        // the base. The running current is the OPEN-execution pointer — NOT
        // `find_latest_run`, whose `started_at` ordering is unreliable for a reset
        // successor (its start time is replayed from the base's prefix, so it looks
        // older than the original run it superseded). Resolve it BEFORE the fork so
        // the freshly materialized successor does not become the pointer first.
        let current_run_key = self
            .repo
            .resolve_execution(&ExecutionRef {
                namespace_id: execution.namespace_id,
                workflow_id: execution.workflow_id.clone(),
                run_id: None,
            })
            .await?;
        let successor_run_key = RunKey::derive(
            execution.namespace_id,
            &execution.workflow_id,
            request.new_run_id,
        );
        let reset_reason = request.reason.clone();
        let reset_now = request.now;
        let successor_run_id = request.new_run_id;
        let fork_event_id = request.fork_event_id;
        match self.submit(run_key, Command::Reset(request)).await? {
            CommitResult::Applied { .. } => {}
            CommitResult::Duplicate => {
                return Err(anyhow!(
                    "unexpected duplicate reset commit for {:?}",
                    run_key
                ));
            }
            CommitResult::Conflict { reason } => return Err(anyhow!("conflict: {reason}")),
            CommitResult::CurrentExecutionConflict {
                existing_run_key, ..
            } => {
                return Err(anyhow!(
                    "current execution already exists: {existing_run_key:?}"
                ));
            }
        }
        // Terminate the current run iff it is distinct from the base and still open.
        if let Some(current_run_key) = current_run_key
            && current_run_key != run_key
            && let tokeira_kernel::LoadedRun::Existing(current) =
                self.repo.load_run(current_run_key).await?
            && current.is_open()
        {
            let terminate = tokeira_kernel::TerminateRequest {
                reason: reset_reason,
                details: None,
                identity: "reset".to_string(),
                links: Vec::new(),
                request: RequestContext {
                    request_id: tokeira_types::RequestId(format!(
                        "reset-terminate:{}",
                        successor_run_id.0
                    )),
                    caller_identity: None,
                    received_at: reset_now,
                },
                now: reset_now,
            };
            self.submit(current_run_key, Command::Terminate(terminate))
                .await?;
        }
        // v1.31.0's reset RPC is synchronous: fork, WFTFailed(RESET), reapply,
        // and the fresh WorkflowTaskScheduled are all durable before it returns,
        // and callers assert the successor's history immediately. Tokeira's lane
        // materializes the successor inline with the base commit (done by the
        // time submit() replies) but authors the fork-point WFT failure via an
        // off-lane task — wait until the successor's history holds a
        // `WorkflowTaskFailed(ResetWorkflow)` at or after the fork point. The
        // copied prefix ends before `fork_event_id` (any earlier reset marker
        // from a prior reset sits strictly inside the prefix), so this
        // condition is unique to the boundary; raw event-id growth is NOT
        // sufficient — when the fork WFT was scheduled-not-started, a
        // concurrent signal can take `fork_event_id` before the failure lands.
        // Once present, the marker persists (monotone), which keeps the wait
        // robust against a fast worker consuming the fresh task concurrently.
        const RESET_BOUNDARY_WAIT_ATTEMPTS: usize = 250;
        const RESET_BOUNDARY_WAIT_INTERVAL: std::time::Duration =
            std::time::Duration::from_millis(20);
        const RESET_BOUNDARY_SCAN_PAGE: usize = 64;
        for _ in 0..RESET_BOUNDARY_WAIT_ATTEMPTS {
            let boundary_window = self
                .repo
                .read_history(
                    successor_run_key,
                    fork_event_id - 1,
                    RESET_BOUNDARY_SCAN_PAGE,
                )
                .await?;
            let boundary_landed = boundary_window.iter().any(|event| {
                matches!(
                    &event.kind,
                    tokeira_kernel::HistoryEventKind::WorkflowTaskFailed {
                        failure_cause: tokeira_kernel::WorkflowTaskFailedCause::ResetWorkflow,
                        ..
                    }
                )
            });
            if boundary_landed {
                return Ok(ResetWorkflowResult {
                    successor_run_key,
                    successor_run_id,
                });
            }
            tokio::time::sleep(RESET_BOUNDARY_WAIT_INTERVAL).await;
        }
        tracing::warn!(
            successor_run_key = ?successor_run_key,
            "reset successor's fork-point workflow task failure did not land before the RPC deadline; returning anyway"
        );
        Ok(ResetWorkflowResult {
            successor_run_key,
            successor_run_id,
        })
    }

    /// Decide how a start/signal-with-start should proceed given the target
    /// WorkflowId's current and historical runs.
    ///
    /// Two tiers, in order:
    /// 1. If there is a *currently-open* run for this WorkflowId, the
    ///    `WorkflowIdConflictPolicy` decides: Fail → Rejected, UseExisting →
    ///    reuse it, TerminateExisting → terminate-then-start. Reuse policy never
    ///    enters into it, because reuse only concerns *closed* runs.
    /// 2. Otherwise the `WorkflowIdReusePolicy` is evaluated against the latest
    ///    run: AllowDuplicate always permits, AllowDuplicateFailedOnly permits
    ///    only when the prior run ended in a non-success terminal state
    ///    (Failed/Cancelled/Terminated/TimedOut), and RejectDuplicate refuses.
    ///
    /// The open-run lookup uses the current-execution pointer while the reuse
    /// check uses `find_latest_run`; both reload the run because the pointer can
    /// race a just-closed run, and the status read must come from authoritative
    /// state, not the index.
    async fn resolve_conflict(
        &self,
        namespace_id: NamespaceId,
        workflow_id: &tokeira_types::WorkflowId,
        request_id: &str,
        conflict_policy: WorkflowIdConflictPolicy,
        reuse_policy: WorkflowIdReusePolicy,
    ) -> Result<ConflictResolution> {
        let current_execution = ExecutionRef {
            namespace_id,
            workflow_id: workflow_id.clone(),
            run_id: None,
        };
        if let Some(run_key) = self.repo.resolve_execution(&current_execution).await? {
            let LoadedRun::Existing(state) = self.repo.load_run(run_key).await? else {
                return Ok(ConflictResolution::Absent);
            };
            if state.status.is_open() {
                // v1.31.0 handleConflict (startworkflow/api.go:328-336): a retried start whose
                // RequestId already authored this run's WorkflowExecutionStarted is deduped to the
                // incumbent BEFORE any conflict policy applies.
                if let Some(info) = state.request_id_infos.get(request_id)
                    && info.event_type == tokeira_kernel::EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
                {
                    return Ok(ConflictResolution::DedupRetried {
                        run_key,
                        run_id: state.run_id,
                        execution_status: state.status,
                    });
                }
                return Ok(match conflict_policy {
                    WorkflowIdConflictPolicy::Fail => ConflictResolution::Rejected {
                        run_key,
                        run_id: state.run_id,
                        reason: StartRejectReason::ConflictPolicyFail,
                    },
                    WorkflowIdConflictPolicy::UseExisting => ConflictResolution::UseExisting {
                        run_key,
                        run_id: state.run_id,
                    },
                    WorkflowIdConflictPolicy::TerminateExisting => {
                        ConflictResolution::TerminateAndStart { run_key }
                    }
                });
            }
        }

        let Some(run_key) = self.repo.find_latest_run(namespace_id, workflow_id).await? else {
            return Ok(ConflictResolution::Absent);
        };
        let LoadedRun::Existing(state) = self.repo.load_run(run_key).await? else {
            return Ok(ConflictResolution::Absent);
        };
        if state.status.is_open() {
            if let Some(info) = state.request_id_infos.get(request_id)
                && info.event_type == tokeira_kernel::EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
            {
                return Ok(ConflictResolution::DedupRetried {
                    run_key,
                    run_id: state.run_id,
                    execution_status: state.status,
                });
            }
            return Ok(match conflict_policy {
                WorkflowIdConflictPolicy::Fail => ConflictResolution::Rejected {
                    run_key,
                    run_id: state.run_id,
                    reason: StartRejectReason::ConflictPolicyFail,
                },
                WorkflowIdConflictPolicy::UseExisting => ConflictResolution::UseExisting {
                    run_key,
                    run_id: state.run_id,
                },
                WorkflowIdConflictPolicy::TerminateExisting => {
                    ConflictResolution::TerminateAndStart { run_key }
                }
            });
        }

        // A retried start whose RequestId authored THIS (now closed) run's
        // WorkflowExecutionStarted dedupes to it BEFORE any reuse policy —
        // v1.31.0's handleConflict consults the current run's request ids
        // regardless of open/closed and responds to the retried request with
        // the incumbent (startworkflow/api.go handleConflict +
        // signal_with_start_workflow.go:264-266 @ v1.31.0). Without this, a
        // network-retried start after a fast-closing first run would silently
        // create a second run.
        if let Some(info) = state.request_id_infos.get(request_id)
            && info.event_type == tokeira_kernel::EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
        {
            return Ok(ConflictResolution::DedupRetried {
                run_key,
                run_id: state.run_id,
                execution_status: state.status,
            });
        }

        Ok(match reuse_policy {
            // TerminateIfRunning governs a RUNNING conflict (migrated to a
            // TERMINATE_EXISTING conflict policy before the kernel start); a
            // CLOSED-run conflict simply allows reuse, like AllowDuplicate.
            WorkflowIdReusePolicy::AllowDuplicate | WorkflowIdReusePolicy::TerminateIfRunning => {
                ConflictResolution::ClosedAllowReuse
            }
            WorkflowIdReusePolicy::AllowDuplicateFailedOnly => {
                if matches!(
                    state.status,
                    tokeira_types::ExecutionStatus::Failed
                        | tokeira_types::ExecutionStatus::Cancelled
                        | tokeira_types::ExecutionStatus::Terminated
                        | tokeira_types::ExecutionStatus::TimedOut
                ) {
                    ConflictResolution::ClosedAllowReuse
                } else {
                    ConflictResolution::Rejected {
                        run_key,
                        run_id: state.run_id,
                        reason: StartRejectReason::ReuseAllowFailedOnly,
                    }
                }
            }
            WorkflowIdReusePolicy::RejectDuplicate => ConflictResolution::Rejected {
                run_key,
                run_id: state.run_id,
                reason: StartRejectReason::ReuseRejectDuplicate,
            },
        })
    }

    /// Terminate the open run that a TerminateExisting conflict policy is
    /// displacing, before a replacement start is committed.
    ///
    /// Resolves the live run id from durable state and issues a terminate
    /// carrying a policy-derived reason/identity. A `Conflict` is surfaced as an
    /// error so the caller does not start a replacement over a run that failed
    /// to terminate; `Duplicate` is tolerated as already-terminated.
    async fn terminate_existing_for_conflict(
        &self,
        namespace_id: NamespaceId,
        workflow_id: tokeira_types::WorkflowId,
        run_key: RunKey,
        request: RequestContext,
    ) -> Result<()> {
        let LoadedRun::Existing(state) = self.repo.load_run(run_key).await? else {
            return Err(anyhow!("execution not found"));
        };
        let execution = ExecutionRef {
            namespace_id,
            workflow_id,
            run_id: Some(state.run_id),
        };
        match self
            .terminate_workflow(
                execution,
                TerminateRequest {
                    reason: "terminated by workflow id conflict policy".to_string(),
                    details: None,
                    identity: request
                        .caller_identity
                        .clone()
                        .unwrap_or_else(|| "workflow-id-conflict-policy".to_string()),
                    links: Vec::new(),
                    request: RequestContext {
                        // v1.31.0's internal conflict-terminate (workflow_id_dedup.go:202
                        // terminateWorkflowAction) runs under IdentityHistoryService and does NOT
                        // consume the start's RequestId — that id is reserved for the new run's
                        // WorkflowExecutionStarted. tokeira records a request-dedupe op for every
                        // terminate (kernel.rs apply_terminate); the start's id must stay free to
                        // author the replacement run's start event (and its request_id_infos
                        // entry, which the retry-dedup path reads). Derive a distinct,
                        // deterministic id keyed on the displaced run so retries of the same
                        // terminate stay idempotent while the start's id stays free.
                        request_id: tokeira_types::RequestId(format!(
                            "conflict-terminate:{}:{}",
                            request.request_id.0, state.run_id.0
                        )),
                        caller_identity: request.caller_identity,
                        received_at: request.received_at,
                    },
                    now: OffsetDateTime::now_utc(),
                },
            )
            .await?
        {
            CommitResult::Applied { .. } | CommitResult::Duplicate => Ok(()),
            CommitResult::Conflict { reason } => Err(anyhow!("conflict: {reason}")),
            CommitResult::CurrentExecutionConflict {
                existing_run_key, ..
            } => Err(anyhow!(
                "current execution already exists: {existing_run_key:?}"
            )),
        }
    }
}

fn prepare_start_request(request: &mut StartRequest, allow_eager_response: bool) -> Result<()> {
    apply_client_cron_start_backoff(request)?;
    request.eager_execution_accepted = eager_workflow_start_is_accepted(
        request.eager_execution_accepted,
        allow_eager_response && ENABLE_EAGER_WORKFLOW_START,
        request.workflow_start_delay,
    );
    Ok(())
}

fn eager_workflow_start_is_accepted(
    requested: bool,
    enabled: bool,
    first_workflow_task_backoff: Option<Duration>,
) -> bool {
    requested
        && enabled
        && first_workflow_task_backoff.is_none_or(|backoff| backoff <= Duration::ZERO)
}

fn apply_client_cron_start_backoff(request: &mut StartRequest) -> Result<()> {
    let Some(cron_schedule) = request.client_cron_schedule.as_deref() else {
        return Ok(());
    };
    if request.workflow_start_delay.is_some() {
        return Err(anyhow!(
            "CronSchedule and WorkflowStartDelay may not be used together."
        ));
    }
    request.workflow_start_delay = Some(cron_initial_backoff(cron_schedule, request.now)?);
    Ok(())
}

fn apply_client_cron_signal_backoff(request: &mut SignalWithStartRequest) -> Result<()> {
    let Some(cron_schedule) = request.client_cron_schedule.as_deref() else {
        return Ok(());
    };
    if request.workflow_start_delay.is_some() {
        return Err(anyhow!(
            "CronSchedule and WorkflowStartDelay may not be used together."
        ));
    }
    request.workflow_start_delay = Some(cron_initial_backoff(cron_schedule, request.now)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use proptest::prelude::*;
    use tokeira_storage::{InMemoryStore, RunRepository};
    use tokeira_types::{
        ExecutionRef, Memo, NamespaceId, Payloads, RequestContext, RequestId, RunId, RunKey,
        SearchAttributes, TaskQueueName, WorkflowId, WorkflowType,
    };

    fn deletion_start_request() -> StartRequest {
        let now = OffsetDateTime::UNIX_EPOCH;
        let run_id = RunId::new();
        StartRequest {
            initiator: None,
            run_key: RunKey::new(),
            namespace_id: NamespaceId::new(),
            workflow_id: WorkflowId("delete-workflow".to_string()),
            run_id,
            workflow_type: WorkflowType("DeleteWorkflow".to_string()),
            task_queue: TaskQueueName("workflow-q".to_string()),
            deployment: None,
            build_id: None,
            versioning_override: None,
            workflow_start_delay: None,
            client_cron_schedule: None,
            completion_callbacks: Vec::new(),
            user_metadata: None,
            links: Vec::new(),
            on_conflict_options: None,
            priority: None,
            input: Payloads::default(),
            header: None,
            memo: Memo::default(),
            search_attributes: SearchAttributes::default(),
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: Duration::seconds(10),
            retry_policy: None,
            conflict_policy: tokeira_kernel::WorkflowIdConflictPolicy::Fail,
            reuse_policy: tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
            attempt: 1,
            continued_execution_run_id: None,
            first_execution_run_id: None,
            parent_run_key: None,
            parent_workflow_id: None,
            parent_run_id: None,
            parent_namespace_id: None,
            parent_namespace_name: None,
            parent_initiated_event_id: 0,
            root_workflow_id: None,
            root_run_id: None,
            original_execution_run_id: Some(run_id),
            continued_failure: None,
            last_completion_result: None,
            first_run_started_at: None,
            request: RequestContext {
                request_id: RequestId("start-delete".to_string()),
                caller_identity: Some("client".to_string()),
                received_at: now,
            },
            now,
            cron_schedule: None,
            eager_execution_accepted: false,
            reserved_poller_identity: None,
        }
    }

    fn deletion_runtime(repo: Arc<InMemoryStore>) -> TokeiraRuntime<InMemoryStore> {
        TokeiraRuntime::new(
            repo,
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
            BacklogConfig::default(),
        )
    }

    fn deletion_request(request_id: &str) -> DeleteWorkflowRequest {
        DeleteWorkflowRequest {
            request: RequestContext {
                request_id: RequestId(request_id.to_string()),
                caller_identity: Some("operator".to_string()),
                received_at: OffsetDateTime::UNIX_EPOCH,
            },
            now: OffsetDateTime::UNIX_EPOCH + Duration::seconds(5),
        }
    }

    #[test]
    fn workflow_deletion_uses_temporal_termination_reason_and_identity() {
        let request = deletion_request("delete-1");
        let termination = workflow_deletion_termination(&request);

        assert_eq!(termination.reason, "Delete workflow execution");
        assert_eq!(termination.identity, "history-service");
        assert!(termination.details.is_none());
        assert_eq!(
            termination.request.request_id.0,
            "delete-workflow-execution:delete-1"
        );
        assert_eq!(
            termination.request.caller_identity.as_deref(),
            Some("history-service")
        );
        assert_eq!(termination.now, request.now);
    }

    #[tokio::test]
    async fn running_workflow_deletion_terminates_then_purges() {
        let repo = Arc::new(InMemoryStore::default());
        let runtime = deletion_runtime(repo.clone());
        let start = deletion_start_request();
        let run_key = start.run_key;
        let execution = ExecutionRef {
            namespace_id: start.namespace_id,
            workflow_id: start.workflow_id.clone(),
            run_id: Some(start.run_id),
        };
        runtime.start_workflow(start).await.unwrap();

        let deletion = runtime
            .delete_workflow(run_key, deletion_request("delete-running"))
            .await
            .unwrap();

        // Start=1, internal termination=2, tombstone=3.
        assert_eq!(deletion.tombstone.transition_seq, TransitionSeq(3));
        assert_eq!(
            deletion.tombstone.context.lifecycle_state,
            tokeira_types::VisibilityLifecycleState::Deleted
        );
        assert_eq!(
            deletion.tombstone.context.execution_status,
            ExecutionStatus::Terminated
        );
        assert!(matches!(
            repo.load_run(run_key).await.unwrap(),
            LoadedRun::Absent
        ));
        assert!(
            repo.read_history(run_key, 0, usize::MAX)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(repo.resolve_execution(&execution).await.unwrap(), None);
    }

    #[tokio::test]
    async fn closed_workflow_deletion_does_not_add_termination_transition() {
        let repo = Arc::new(InMemoryStore::default());
        let runtime = deletion_runtime(repo.clone());
        let start = deletion_start_request();
        let run_key = start.run_key;
        let execution = ExecutionRef {
            namespace_id: start.namespace_id,
            workflow_id: start.workflow_id.clone(),
            run_id: Some(start.run_id),
        };
        runtime.start_workflow(start).await.unwrap();
        runtime
            .terminate_workflow(
                execution,
                TerminateRequest {
                    reason: "operator termination".to_string(),
                    details: None,
                    identity: "operator".to_string(),
                    links: Vec::new(),
                    request: RequestContext {
                        request_id: RequestId("terminate-before-delete".to_string()),
                        caller_identity: Some("operator".to_string()),
                        received_at: OffsetDateTime::UNIX_EPOCH,
                    },
                    now: OffsetDateTime::UNIX_EPOCH + Duration::seconds(1),
                },
            )
            .await
            .unwrap();
        let LoadedRun::Existing(before) = repo.load_run(run_key).await.unwrap() else {
            panic!("terminated run should remain until deletion");
        };

        let deletion = runtime
            .delete_workflow(run_key, deletion_request("delete-closed"))
            .await
            .unwrap();

        assert_eq!(
            deletion.tombstone.transition_seq,
            before.transition_seq.next(),
            "closed deletion adds only its tombstone version"
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        // Feature: edge-eager-dispatch, Property 2: Runtime eager admission.
        fn eager_workflow_start_admission_matches_truth_table(
            requested in any::<bool>(),
            enabled in any::<bool>(),
            backoff_case in 0u8..4,
        ) {
            let backoff = match backoff_case {
                0 => None,
                1 => Some(Duration::seconds(-1)),
                2 => Some(Duration::ZERO),
                _ => Some(Duration::seconds(1)),
            };

            prop_assert_eq!(
                eager_workflow_start_is_accepted(requested, enabled, backoff),
                requested
                    && enabled
                    && backoff.is_none_or(|duration| duration <= Duration::ZERO)
            );
        }
    }
}
