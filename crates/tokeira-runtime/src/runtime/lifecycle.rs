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

impl<R> TokeiraRuntime<R>
where
    R: RunRepository + 'static,
{
    /// Start a new workflow execution.
    ///
    /// Before committing, this optimistically reserves a workflow-task poller so
    /// that, on a successful start, the first workflow task can be handed
    /// directly to a waiting worker (eager start) instead of round-tripping
    /// through the broker. The reservation is returned to the broker on every
    /// non-`Applied` outcome and on submit error, so a failed start never leaks
    /// a parked poller. Execution/run timeout tracking is registered only after
    /// the start is durably `Applied`.
    pub async fn start_workflow(&self, mut request: StartRequest) -> Result<CommitResult> {
        apply_client_cron_start_backoff(&mut request)?;
        let reserved_poller = self.try_reserve_start_poller(&request).await;
        if let Some(reserved) = &reserved_poller {
            request.reserved_poller_identity = Some(reserved.worker_identity().clone());
        }

        let result = match self
            .submit(request.run_key, Command::Start(request.clone()))
            .await
        {
            Ok(result) => result,
            Err(error) => {
                if let Some(reserved) = reserved_poller {
                    self.broker.return_reserved_poller(reserved).await;
                }
                return Err(error);
            }
        };
        match (&result, reserved_poller) {
            (CommitResult::Applied { new_state }, Some(reserved)) => {
                self.deliver_reserved_start_workflow_task(new_state, reserved)
                    .await?;
            }
            (_, Some(reserved)) => {
                self.broker.return_reserved_poller(reserved).await;
            }
            (_, None) => {}
        }
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
                first_run_started_at: request.first_run_started_at,
                has_retry_policy: request.retry_policy.is_some(),
            });
        }
        Ok(result)
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
        request: StartRequest,
    ) -> Result<StartWorkflowResult> {
        let resolution = self
            .resolve_conflict(
                request.namespace_id,
                &request.workflow_id,
                request.conflict_policy,
                request.reuse_policy,
            )
            .await?;
        match resolution {
            ConflictResolution::Absent | ConflictResolution::ClosedAllowReuse => {
                match self.start_workflow(request.clone()).await? {
                    CommitResult::Applied { new_state } => Ok(StartWorkflowResult::Started {
                        run_key: request.run_key,
                        run_id: request.run_id,
                        mutation_metadata: mutation_metadata(&new_state),
                    }),
                    CommitResult::Duplicate => Err(anyhow!(
                        "unexpected duplicate start commit for {:?}",
                        request.run_key
                    )),
                    CommitResult::Conflict { reason } => Err(anyhow!("conflict: {reason}")),
                }
            }
            ConflictResolution::UseExisting { run_key, run_id } => {
                self.apply_start_on_conflict_options(run_key, &request)
                    .await?;
                Ok(StartWorkflowResult::UsedExisting { run_key, run_id })
            }
            ConflictResolution::TerminateAndStart { run_key } => {
                self.terminate_existing_for_conflict(
                    request.namespace_id,
                    request.workflow_id.clone(),
                    run_key,
                    request.request.clone(),
                )
                .await?;
                match self.start_workflow(request.clone()).await? {
                    CommitResult::Applied { new_state } => Ok(StartWorkflowResult::Started {
                        run_key: request.run_key,
                        run_id: request.run_id,
                        mutation_metadata: mutation_metadata(&new_state),
                    }),
                    CommitResult::Duplicate => Err(anyhow!(
                        "unexpected duplicate start commit for {:?}",
                        request.run_key
                    )),
                    CommitResult::Conflict { reason } => Err(anyhow!("conflict: {reason}")),
                }
            }
            ConflictResolution::Rejected { run_key, run_id } => {
                Ok(StartWorkflowResult::Rejected { run_key, run_id })
            }
        }
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
    pub async fn signal_with_start_workflow(
        &self,
        mut request: SignalWithStartRequest,
    ) -> Result<SignalWithStartResult> {
        apply_client_cron_signal_backoff(&mut request)?;
        let resolution = self
            .resolve_conflict(
                request.namespace_id,
                &request.workflow_id,
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
                    CommitResult::Duplicate => Err(anyhow!(
                        "unexpected duplicate signal-with-start commit for {:?}",
                        request.run_key
                    )),
                    CommitResult::Conflict { reason } => Err(anyhow!("conflict: {reason}")),
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
                    CommitResult::Duplicate => Err(anyhow!(
                        "unexpected duplicate signal-with-start commit for {:?}",
                        request.run_key
                    )),
                    CommitResult::Conflict { reason } => Err(anyhow!("conflict: {reason}")),
                }
            }
            ConflictResolution::Rejected { run_key, run_id } => {
                Ok(SignalWithStartResult::Rejected { run_key, run_id })
            }
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
        self.submit(run_key, Command::Signal(request)).await
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
        let successor_run_key = RunKey::derive(
            execution.namespace_id,
            &execution.workflow_id,
            request.new_run_id,
        );
        match self
            .submit(run_key, Command::Reset(request.clone()))
            .await?
        {
            CommitResult::Applied { .. } => Ok(ResetWorkflowResult {
                successor_run_key,
                successor_run_id: request.new_run_id,
            }),
            CommitResult::Duplicate => Err(anyhow!(
                "unexpected duplicate reset commit for {:?}",
                run_key
            )),
            CommitResult::Conflict { reason } => Err(anyhow!("conflict: {reason}")),
        }
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
                return Ok(match conflict_policy {
                    WorkflowIdConflictPolicy::Fail => ConflictResolution::Rejected {
                        run_key,
                        run_id: state.run_id,
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
            return Ok(match conflict_policy {
                WorkflowIdConflictPolicy::Fail => ConflictResolution::Rejected {
                    run_key,
                    run_id: state.run_id,
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

        Ok(match reuse_policy {
            WorkflowIdReusePolicy::AllowDuplicate => ConflictResolution::ClosedAllowReuse,
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
                    }
                }
            }
            WorkflowIdReusePolicy::RejectDuplicate => ConflictResolution::Rejected {
                run_key,
                run_id: state.run_id,
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
                    request: RequestContext {
                        request_id: request.request_id,
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
        }
    }
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
