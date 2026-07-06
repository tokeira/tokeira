//! Workflow-task poll and start path, including Worker Deployment routing.
//!
//! This `impl TokeiraRuntime` continuation owns the workflow-task half of
//! delivery: long-polling the broker, resolving the deployment version a polled
//! task should target, starting the version transition when a differing poller
//! arrives, and driving the kernel `WorkflowTaskStarted`/`Completed`
//! transitions. Routing decisions are derived effects of the durable deployment
//! registry plus per-run versioning state — no correctness weight rests on the
//! transient broker (per "history is authority").
//!
//! The versioning routing here mirrors Temporal server v1.31.0
//! (`service/history/api/recordworkflowtaskstarted` and
//! `service/history/workflow/util.go`); the inline comments cite the specific
//! source for each non-obvious decision.

use super::*;
use prost::Message as _;
use tokeira_kernel::{
    RetryContinuation, RetryState, VersioningBehavior, VersioningOverride,
    WorkerDeploymentVersionRef,
};
use tokeira_observability::OutcomeLabel;
use tokeira_proto::failure::{Failure, failure::FailureInfo};
use tokeira_storage::{
    DeploymentKey, DeploymentName, StoredRoutingConfig, WorkerDeploymentVersionKey,
};
use tokeira_types::WorkflowId;

use crate::{timeout::WorkflowTimeoutEntry, versioning::deterministic_bucket};

/// Workflow-task routing target computed from durable run state plus registry config.
///
/// The value is shared with activity-start transition checks because Temporal
/// decides whether an activity poller should transition the workflow by asking
/// where the workflow task would route now
/// (`service/history/api/recordactivitytaskstarted/api.go:283 @ v1.31.0`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ResolvedWorkflowTaskTarget {
    /// Deployment version selected for the workflow task, or `None` for unversioned routing.
    pub deployment_version: Option<WorkerDeploymentVersionRef>,
    /// Registry/run revision that produced `deployment_version`.
    pub revision_number: i64,
    /// Whether the target is pinned and therefore cannot initiate a transition.
    pub pinned: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PolledWorkflowTaskTarget {
    resolved: ResolvedWorkflowTaskTarget,
    deployment_transition: Option<WorkerDeploymentVersionRef>,
}

/// Resolve the deployment version a polled workflow task should target.
///
/// Precedence mirrors v1.31.0 `GetEffectiveDeployment`
/// (`service/history/workflow/util.go @ v1.31.0`):
/// version_transition > pinned override > auto-upgrade override > pinned
/// behavior > SDK behavior + routing config. A pinned target (override or
/// behavior) carries `pinned: true` so the caller never starts a transition
/// for it. AUTO_UPGRADE / unversioned runs follow the deployment's routing
/// config (current/ramping), so they pick up the deployment's revision number;
/// pinned/transition targets carry the run's own revision number because their
/// routing decision was already made and recorded on the run.
pub(crate) fn resolve_workflow_task_target_version(
    routing_config: &StoredRoutingConfig,
    state: &WorkflowState,
) -> ResolvedWorkflowTaskTarget {
    let Some(info) = state.versioning_info.as_ref() else {
        return ResolvedWorkflowTaskTarget {
            deployment_version: routing_config_target(routing_config, &state.workflow_id),
            revision_number: routing_config.revision_number,
            pinned: false,
        };
    };

    if let Some(transition) = &info.version_transition {
        return ResolvedWorkflowTaskTarget {
            deployment_version: Some(transition.clone()),
            revision_number: info.revision_number,
            pinned: false,
        };
    }

    match &info.versioning_override {
        Some(VersioningOverride::Pinned { version }) => ResolvedWorkflowTaskTarget {
            deployment_version: Some(version.clone()),
            revision_number: info.revision_number,
            pinned: true,
        },
        Some(VersioningOverride::AutoUpgrade) => ResolvedWorkflowTaskTarget {
            deployment_version: routing_config_target(routing_config, &state.workflow_id),
            revision_number: routing_config.revision_number,
            pinned: false,
        },
        None if info.behavior == VersioningBehavior::Pinned => ResolvedWorkflowTaskTarget {
            deployment_version: info.deployment_version.clone(),
            revision_number: info.revision_number,
            pinned: true,
        },
        None => ResolvedWorkflowTaskTarget {
            deployment_version: routing_config_target(routing_config, &state.workflow_id),
            revision_number: routing_config.revision_number,
            pinned: false,
        },
    }
}

/// Pick the routing target for AUTO_UPGRADE / unversioned traffic from a
/// deployment's routing config, splitting by ramp percentage.
///
/// A nil current version means the deployment routes this traffic to
/// unversioned workers (returns None). When a ramping version is set with a
/// non-zero percentage, a deterministic fraction of workflow ids route to it;
/// the rest stay on current. The split is keyed on workflow id (not random) so
/// the same run always resolves to the same version across polls, which is
/// required for replay determinism.
///
/// Bucketing: `deterministic_bucket` returns `[0, 9999]`, so the ramp
/// percentage P is scaled to `P * 100` buckets — e.g. P=50 sends ids with
/// bucket `< 5000` (half) to the ramping version. This gives 1/100-percent
/// resolution matching the `float` percentage field.
fn routing_config_target(
    routing_config: &StoredRoutingConfig,
    workflow_id: &WorkflowId,
) -> Option<WorkerDeploymentVersionRef> {
    let current = routing_config.current_version.as_ref()?;
    if let Some(ramping) = routing_config.ramping_version.as_ref()
        && routing_config.ramping_version_percentage > 0.0
        && deterministic_bucket(&workflow_id.0)
            < (f64::from(routing_config.ramping_version_percentage) * 100.0) as u64
    {
        return Some(version_key_to_ref(ramping));
    }
    Some(version_key_to_ref(current))
}

fn version_key_to_ref(version: &WorkerDeploymentVersionKey) -> WorkerDeploymentVersionRef {
    WorkerDeploymentVersionRef {
        deployment_name: version.deployment_name.0.clone(),
        build_id: version.build_id.0.clone(),
    }
}

/// Reconstruct the worker deployment version from the Matching queue key.
///
/// Unversioned pollers intentionally return `None`; they may start work but
/// cannot prove a target deployment for a version transition.
pub(super) fn poller_deployment_version(queue: &QueueKey) -> Option<WorkerDeploymentVersionRef> {
    Some(WorkerDeploymentVersionRef {
        deployment_name: queue.deployment.as_ref()?.0.clone(),
        build_id: queue.build_id.as_ref()?.0.clone(),
    })
}

/// Decide whether a polled workflow task should start a deployment-version
/// transition toward the poller's version.
///
/// Mirrors v1.31.0 `recordworkflowtaskstarted/api.go @ v1.31.0`: a transition
/// starts whenever the polling worker's deployment differs from the workflow's
/// *effective* deployment (`!pollerDeployment.Equal(wfDeployment)`). We do NOT
/// additionally require the poller to equal the freshly-resolved routing target
/// — Matching already chose this poller when it dispatched the task, and
/// re-deriving the routing target here would suppress a legitimate transition
/// when the routing config advanced between dispatch and start (the staleness
/// window the dispatch revision number guards). Pinned runs never transition;
/// `start_version_transition` itself rejects them
/// (`ErrPinnedWorkflowCannotTransition`), and we also short-circuit on the
/// resolved-target pin so a pinned run is never offered a transition.
fn transition_for_polled_workflow_task(
    state: &WorkflowState,
    target: &ResolvedWorkflowTaskTarget,
    queue: &QueueKey,
) -> Option<WorkerDeploymentVersionRef> {
    if target.pinned {
        return None;
    }
    let poller_version = poller_deployment_version(queue)?;
    (state.effective_deployment() != Some(&poller_version)).then_some(poller_version)
}

fn routing_deployment_name(state: &WorkflowState, queue: &QueueKey) -> Option<DeploymentName> {
    state
        .worker_deployment_name
        .as_ref()
        .cloned()
        .or_else(|| {
            state
                .versioning_info
                .as_ref()
                .and_then(|info| info.deployment_version.as_ref())
                .map(|version| version.deployment_name.clone())
        })
        .or_else(|| {
            queue
                .deployment
                .as_ref()
                .map(|deployment| deployment.0.clone())
        })
        .map(DeploymentName)
}

impl<R> TokeiraRuntime<R>
where
    R: RunRepository + 'static,
{
    pub(super) async fn try_reserve_start_poller(
        &self,
        request: &StartRequest,
    ) -> Option<ReservedPoller> {
        let queue = QueueKey {
            namespace_id: request.namespace_id,
            task_queue: request.task_queue.clone(),
            task_kind: TaskKind::Workflow,
            deployment: request.deployment.clone(),
            build_id: request.build_id.clone(),
        };
        self.broker.try_reserve_poller(&queue).await
    }

    pub(super) async fn deliver_reserved_start_workflow_task(
        &self,
        new_state: &WorkflowState,
        reserved: ReservedPoller,
    ) -> Result<()> {
        let worker_identity = reserved.worker_identity().clone();
        // A start sync-match delivers on the run's NORMAL queue: not a sticky
        // match (the flag licenses partial-history attach, moot here anyway
        // since a fresh run has previous_started_event_id == 0).
        let task = self
            .started_workflow_task_from_state(new_state, false, worker_identity)
            .await?;
        if !reserved.deliver(task) {
            tracing::warn!(
                run_key = %new_state.run_key.0,
                "reserved workflow task poller disappeared after durable start commit; timeout scanner will recover"
            );
        }
        Ok(())
    }

    pub async fn poll_workflow_task(
        &self,
        queue: QueueKey,
        worker_identity: WorkerIdentity,
        timeout_after: tokio::time::Duration,
    ) -> Result<Option<StartedWorkflowTask>> {
        let polled = match self
            .broker
            .poll_workflow_task(&queue, &worker_identity, timeout_after)
            .await?
        {
            Some(polled) => {
                self.delivery_metrics.record_poll_success(&queue);
                polled
            }
            None => {
                self.delivery_metrics.record_poll_timeout(&queue);
                return Ok(None);
            }
        };

        match polled {
            WorkflowPollResult::Queued(offered, entered_at) => {
                let started = self
                    .start_polled_workflow_task(offered, entered_at, worker_identity)
                    .await?;
                Ok(Some(started))
            }
            WorkflowPollResult::Started(started) => Ok(Some(started)),
            // The workflow-task-only API calls the WFT-only broker method, so
            // this arm is defensive. If a future refactor routes it through
            // the unified poll, do not consume direct query work here: callers
            // of this method are not prepared to register a query waiter.
            WorkflowPollResult::Query(_) => Ok(None),
        }
    }

    /// Poll the workflow task queue for either a workflow task or a direct query.
    ///
    /// This is the public-api path used by `PollWorkflowTaskQueue`: Temporal
    /// delivers legacy direct queries as matched workflow poll tasks rather
    /// than through a separate query-poll RPC
    /// (`service/matching/matching_engine.go:1084 @ v1.31.0`). The older
    /// [`Self::poll_workflow_task`] method remains workflow-task-only for
    /// internal eager-claim paths that cannot safely consume query work.
    pub async fn poll_workflow_activation(
        &self,
        queue: QueueKey,
        worker_identity: WorkerIdentity,
        timeout_after: tokio::time::Duration,
    ) -> Result<Option<WorkflowActivation>> {
        let polled = match self
            .broker
            .poll_workflow_activation(&queue, &worker_identity, timeout_after)
            .await?
        {
            Some(polled) => {
                self.delivery_metrics.record_poll_success(&queue);
                polled
            }
            None => {
                self.delivery_metrics.record_poll_timeout(&queue);
                return Ok(None);
            }
        };

        match polled {
            WorkflowPollResult::Queued(offered, entered_at) => {
                let started = self
                    .start_polled_workflow_task(offered, entered_at, worker_identity)
                    .await?;
                Ok(Some(WorkflowActivation::WorkflowTask(started)))
            }
            WorkflowPollResult::Started(started) => {
                Ok(Some(WorkflowActivation::WorkflowTask(started)))
            }
            WorkflowPollResult::Query(query) => Ok(Some(WorkflowActivation::QueryTask(query))),
        }
    }

    pub async fn try_claim_workflow_task(
        &self,
        queue: QueueKey,
        run_key: RunKey,
        worker_identity: WorkerIdentity,
    ) -> Result<Option<StartedWorkflowTask>> {
        let Some(offered) = self
            .broker
            .try_claim_workflow_task_for_worker(&queue, run_key, Some(&worker_identity))
            .await
        else {
            return Ok(None);
        };
        self.delivery_metrics.record_poll_success(&queue);
        match self
            .start_polled_workflow_task(offered.0, offered.1, worker_identity)
            .await
        {
            Ok(started) => Ok(Some(started)),
            Err(error) => {
                tracing::debug!(?error, "eager workflow task claim did not start");
                Ok(None)
            }
        }
    }

    async fn resolve_polled_workflow_task_target(
        &self,
        offered: &DispatchableWorkflowTask,
    ) -> Result<PolledWorkflowTaskTarget> {
        let LoadedRun::Existing(state) = self.repo.load_run(offered.run_key).await? else {
            return Ok(PolledWorkflowTaskTarget {
                resolved: ResolvedWorkflowTaskTarget {
                    deployment_version: None,
                    revision_number: 0,
                    pinned: false,
                },
                deployment_transition: None,
            });
        };
        let routing_config = self
            .load_worker_deployment_routing_config(&state, &offered.queue)
            .await?;
        let resolved = resolve_workflow_task_target_version(&routing_config, &state);
        let deployment_transition =
            transition_for_polled_workflow_task(&state, &resolved, &offered.queue);
        Ok(PolledWorkflowTaskTarget {
            resolved,
            deployment_transition,
        })
    }

    pub(super) async fn load_worker_deployment_routing_config(
        &self,
        state: &WorkflowState,
        queue: &QueueKey,
    ) -> Result<StoredRoutingConfig> {
        let Some(repository) = &self.worker_deployment_repository else {
            return Ok(StoredRoutingConfig::default());
        };
        let Some(deployment_name) = routing_deployment_name(state, queue) else {
            return Ok(StoredRoutingConfig::default());
        };
        let key = DeploymentKey {
            namespace_id: state.namespace_id,
            deployment_name,
        };
        Ok(repository
            .load_deployment(&key)
            .await?
            .map(|record| record.routing_config)
            .unwrap_or_default())
    }

    /// Record the completion of a workflow task and
    /// apply any resulting commands.
    ///
    /// After the kernel commits the completion, it checks whether events
    /// arrived between WFT-Started and now (buffered events, e.g. signals).
    /// If so, a new WFT is scheduled immediately so the worker replays those
    /// events. The transport layer also uses this commit point to release any
    /// buffered queries whose barrier has been satisfied.
    pub async fn complete_workflow_task(
        &self,
        req: WorkflowTaskCompletedRequest,
    ) -> Result<CommitResult> {
        if let Err(error) = self.validate_workflow_task_token(&req.token).await {
            runtime_metrics::record_workflow_task_completed(OutcomeLabel::Rejected);
            return Err(error);
        }
        let run_key = req.token.run_key;
        let completion_token = req.token.clone();
        let completion_identity = req.identity.clone();
        // Rejections write NO history event (v1.31.0's
        // RejectWorkflowExecutionUpdate is a documented no-op), so the lane's
        // post-commit event scan cannot resolve their waiters — capture them
        // before `req` moves and publish the rejection outcomes once the
        // completion durably commits.
        let rejected_updates: Vec<(String, tokeira_types::Payload)> = req
            .commands
            .iter()
            .filter_map(|command| match command {
                tokeira_kernel::WorkflowCommand::ProtocolMessage {
                    body: tokeira_kernel::UpdateProtocolBody::Rejected { update_id, failure },
                    ..
                }
                | tokeira_kernel::WorkflowCommand::UpdateRejected { update_id, failure } => {
                    Some((update_id.clone(), failure.clone()))
                }
                _ => None,
            })
            .collect();
        let cron_continuation = self.cron_continuation_for_completion(run_key, &req).await?;
        // Retry is evaluated before cron (retry.go @ v1.31.0). The cron helper
        // already declines when a retry policy is present on a FailWorkflow, so
        // the two are mutually exclusive here; compute retry only when cron did
        // not claim the completion.
        let retry_continuation = if cron_continuation.is_none() {
            self.retry_continuation_for_completion(run_key, &req)
                .await?
        } else {
            None
        };
        // Capture the successor id before `req` moves into the command: a Retry
        // continuation means the committed WorkflowExecutionFailed will carry
        // this new_execution_run_id, and we must start that successor run after
        // the predecessor's close commits (Req 2.2).
        let retry_successor_run_id = match &retry_continuation {
            Some(RetryContinuation::Retry { new_run_id }) => Some(*new_run_id),
            _ => None,
        };
        let command = match (retry_continuation, cron_continuation) {
            (Some(retry_continuation), _) => Command::WorkflowTaskCompletedWithRetry {
                request: req,
                retry_continuation,
            },
            (None, Some(cron_continuation)) => Command::WorkflowTaskCompletedWithCron {
                request: req,
                cron_continuation,
            },
            (None, None) => Command::WorkflowTaskCompleted(req),
        };
        let result = self.submit_for_owned_shard(run_key, command).await;
        // v1.31.0's invalid-command contract: the completion is discarded, the
        // WORKFLOW TASK is failed with the command's cause (persisted — unless
        // a transient attempt keeps failing on a non-UnhandledCommand cause,
        // which is dropped to time out instead), and the completion call
        // errors with INVALID_ARGUMENT carrying the cause message
        // (respondworkflowtaskcompleted/api.go:455-485,739-742).
        if let Err(error) = &result
            && let Some(crate::lane::KernelRejected(
                tokeira_kernel::Reject::InvalidCommandAttributes { cause, message },
            )) = error.downcast_ref()
        {
            let cause = cause.clone();
            // The wire message mirrors `workflowTaskFailedCause.Message()`:
            // `"{cause}: {causeErr}"` when a cause error exists, the bare
            // cause name otherwise (workflow_task_completed_handler.go:
            // 1502-1510 @ v1.31.0). The same rendering is persisted as the
            // WFT-failed event's server failure
            // (`failure.NewServerFailure(wtFailedCause.Message(), false)`,
            // respondworkflowtaskcompleted/api.go:1049-1059 @ v1.31.0).
            let wire_message = match message {
                Some(cause_err) => format!("{}: {}", cause.as_str(), cause_err),
                None => cause.as_str().to_string(),
            };
            runtime_metrics::record_workflow_task_completed(OutcomeLabel::Rejected);
            let is_unhandled_command =
                cause == tokeira_kernel::WorkflowTaskFailedCause::UnhandledCommand;
            let drop_without_failing = completion_token.attempt > 1 && !is_unhandled_command;
            if !drop_without_failing
                && let Err(error) = self
                    .submit_for_owned_shard(
                        run_key,
                        Command::WorkflowTaskFailed(tokeira_kernel::WorkflowTaskFailedRequest {
                            logical_seq: completion_token.logical_seq,
                            started_event_id: completion_token.started_event_id,
                            failure_cause: cause,
                            failure_details: Some(server_failure_payload(&wire_message)),
                            worker_identity: completion_identity,
                            now: OffsetDateTime::now_utc(),
                        }),
                    )
                    .await
            {
                tracing::warn!(
                    ?error,
                    run_key = ?run_key,
                    "failed to persist WFT failure for invalid command"
                );
            }
            // The workflow tried to close and bounced off buffered events:
            // while the RETRY WFT is started, new signals are rejected with
            // WorkflowClosing. Set only after the WFT-failed persist so a
            // concurrent signal cannot be rejected while the OLD attempt
            // still shows as started — v1.31.0 sets `workflowCloseAttempted`
            // atomically with the UNHANDLED_COMMAND failure event
            // (workflow_task_state_machine.go:924-928).
            if is_unhandled_command {
                self.close_attempt_tracking
                    .lock()
                    .expect("close-attempt tracking lock")
                    .insert(run_key);
            }
            return Err(crate::errors::InvalidWorkflowCommand {
                message: wire_message,
            }
            .into());
        }
        match &result {
            Ok(CommitResult::Applied { .. } | CommitResult::Duplicate) => {
                // A successfully completed WFT means the workflow observed the
                // buffered events and moved on — a stale close-attempt must not
                // keep rejecting signals (v1.31.0's volatile bit dies with the
                // reloaded mutable state).
                self.close_attempt_tracking
                    .lock()
                    .expect("close-attempt tracking lock")
                    .remove(&run_key);
                // Publish rejection outcomes now that the completion is
                // durable: the rejected update left the kernel's admitted set
                // with no event, and its waiters resolve off the event log.
                for (update_id, failure) in rejected_updates {
                    self.update_registry.notify(
                        run_key,
                        &update_id,
                        crate::update::UpdateResolution::Rejected { failure },
                    );
                }
                runtime_metrics::record_workflow_task_completed(OutcomeLabel::Success);
            }
            Ok(CommitResult::Conflict { .. } | CommitResult::CurrentExecutionConflict { .. })
            | Err(_) => {
                runtime_metrics::record_workflow_task_completed(OutcomeLabel::Failure);
            }
        }
        // Start the attempt-N+1 successor once the predecessor's Failed-with-retry
        // close is durable. The successor start is a derived effect of the
        // authoritative close: if it fails, the predecessor close still stands
        // (Req 2.2.3), mirroring the continue-as-new successor posture. The
        // successor run id is fixed by the committed new_execution_run_id, so a
        // re-drive is idempotent on the derived run key.
        if let (Ok(CommitResult::Applied { .. }), Some(new_run_id)) =
            (&result, retry_successor_run_id)
            && let Err(error) = self.start_retry_successor(run_key, new_run_id).await
        {
            tracing::error!(
                ?error,
                predecessor_run_key = ?run_key,
                "failed to start workflow retry successor",
            );
        }
        result
    }

    /// `RespondWorkflowTaskFailed`: route on the reported cause.
    /// `GrpcMessageTooLarge` terminates the run through the kernel's
    /// force-close-then-terminate command instead of retrying the task
    /// (`respondworkflowtaskfailed/api.go:88 @ v1.31.0`); every other cause
    /// takes the WFT-failed retry path. The terminate reason is the cause's
    /// v1.31.0 `String()` rendering and the identity is the internal
    /// history-service identity (`consts.IdentityHistoryService`).
    pub async fn fail_workflow_task(
        &self,
        token: WorkflowTaskToken,
        failure_cause: tokeira_kernel::WorkflowTaskFailedCause,
        failure_details: Option<Payload>,
        worker_identity: WorkerIdentity,
        request: RequestContext,
        now: OffsetDateTime,
    ) -> Result<CommitResult> {
        self.validate_workflow_task_token(&token).await?;
        let run_key = token.run_key;
        let command = if matches!(
            failure_cause,
            tokeira_kernel::WorkflowTaskFailedCause::GrpcMessageTooLarge
        ) {
            Command::TerminateOnWorkflowTaskFailed(
                tokeira_kernel::TerminateOnWorkflowTaskFailedRequest {
                    logical_seq: token.logical_seq,
                    started_event_id: token.started_event_id,
                    reason: "GrpcMessageTooLarge".to_string(),
                    identity: "history-service".to_string(),
                    request,
                    now,
                },
            )
        } else {
            Command::WorkflowTaskFailed(tokeira_kernel::WorkflowTaskFailedRequest {
                logical_seq: token.logical_seq,
                started_event_id: token.started_event_id,
                failure_cause,
                failure_details,
                worker_identity,
                now,
            })
        };
        self.submit_for_owned_shard(run_key, command).await
    }

    async fn cron_continuation_for_completion(
        &self,
        run_key: RunKey,
        req: &WorkflowTaskCompletedRequest,
    ) -> Result<Option<CronContinuation>> {
        let closes_for_cron = req.commands.iter().any(|command| {
            matches!(command, WorkflowCommand::CompleteWorkflow { .. })
                || matches!(command, WorkflowCommand::FailWorkflow { .. })
        });
        if !closes_for_cron {
            return Ok(None);
        }

        let LoadedRun::Existing(state) = self.repo.load_run(run_key).await? else {
            return Ok(None);
        };
        if state.retry_policy.is_some()
            && req
                .commands
                .iter()
                .any(|command| matches!(command, WorkflowCommand::FailWorkflow { .. }))
        {
            return Ok(None);
        }

        let start_event = self.repo.read_history(run_key, 0, 1).await?;
        let Some(HistoryEventKind::WorkflowExecutionStarted {
            input,
            cron_schedule: Some(cron_schedule),
            ..
        }) = start_event.first().map(|event| &event.kind)
        else {
            return Ok(None);
        };
        if cron_schedule.is_empty() {
            return Ok(None);
        }

        // Temporal's cron close path creates the successor immediately but
        // records a first-WFT backoff on that successor
        // (`service/history/api/respondworkflowtaskcompleted/workflow_task_completed_handler.go:1383`,
        // `service/history/workflow/mutable_state_impl.go:2601 @ v1.31.0`).
        // Runtime owns the wall-clock cron calculation; kernel owns the
        // durable event that makes the successor replayable.
        Ok(Some(CronContinuation {
            new_run_id: RunId::new(),
            first_workflow_task_backoff: cron_initial_backoff(cron_schedule, req.now)?,
            input: input.clone(),
            cron_schedule: cron_schedule.clone(),
        }))
    }

    /// Evaluate the workflow retry decision for a WFT completion that fails the
    /// run, returning the [`RetryContinuation`] to record on
    /// `WorkflowExecutionFailed`, or `None` when no retry continuation applies (no
    /// `FailWorkflow` command, or no retry policy — the kernel maps those to the
    /// terminal `RetryPolicyNotSet`).
    ///
    /// Mirrors `service/history/workflow/retry.go:32-116` +
    /// `mutable_state_impl.go:1630 @ v1.31.0`: a run with a retry policy retries on
    /// failure unless the failure is non-retryable, the maximum attempts are
    /// reached, or the successor's first attempt would begin at/after the
    /// workflow-execution deadline. The evaluation (failure decoding, wall clock,
    /// backoff) is a runtime concern; the kernel only records the outcome (Req 2.1).
    async fn retry_continuation_for_completion(
        &self,
        run_key: RunKey,
        req: &WorkflowTaskCompletedRequest,
    ) -> Result<Option<RetryContinuation>> {
        let Some(failure) = req.commands.iter().find_map(|command| match command {
            WorkflowCommand::FailWorkflow { failure } => Some(failure.clone()),
            _ => None,
        }) else {
            return Ok(None);
        };
        let LoadedRun::Existing(state) = self.repo.load_run(run_key).await? else {
            return Ok(None);
        };
        let Some(policy) = state.retry_policy.clone() else {
            return Ok(None);
        };

        if !workflow_failure_is_retryable(&failure, &policy.non_retryable_error_types) {
            return Ok(Some(RetryContinuation::Terminal {
                retry_state: RetryState::NonRetryableFailure,
            }));
        }
        // `maximum_attempts == 0` means unlimited (v1.31.0 NoInterval semantics).
        if policy.maximum_attempts > 0 && state.attempt >= policy.maximum_attempts {
            return Ok(Some(RetryContinuation::Terminal {
                retry_state: RetryState::MaximumAttemptsReached,
            }));
        }
        // Execution-expiration cap: if the next attempt could not begin before the
        // workflow-execution deadline, the chain ends as Timeout (retry.go). The
        // deadline is anchored on the first run's start so it spans the whole chain.
        let backoff = retry_backoff(&policy, state.attempt);
        if let Some(execution_timeout) = state.workflow_execution_timeout {
            let anchor = state.first_run_started_at.unwrap_or(state.started_at);
            if req.now + backoff >= anchor + execution_timeout {
                return Ok(Some(RetryContinuation::Terminal {
                    retry_state: RetryState::Timeout,
                }));
            }
        }
        Ok(Some(RetryContinuation::Retry {
            new_run_id: RunId::new(),
        }))
    }

    /// Start the attempt-N+1 successor after a Failed-with-retry close commits.
    ///
    /// Mirrors the continue-as-new / cron successor start in `lane.rs`: the
    /// successor inherits the run's type/queue/input/policy/timeouts, chains its
    /// lineage (`continued_execution_run_id`, first-run identity), carries the
    /// predecessor's failure as `continued_failure`, and delays its first workflow
    /// task by the recomputed backoff. The original input re-runs on retry, read
    /// from the run's `WorkflowExecutionStarted` event (the same source cron uses).
    /// The successor start is submitted through the normal run-routing path so it
    /// lands on the owning lane; `Duplicate` is treated as success because a
    /// re-driven close mints the same derived run key (Req 2.2, 5.1.3).
    async fn start_retry_successor(
        &self,
        predecessor_run_key: RunKey,
        new_run_id: RunId,
    ) -> Result<()> {
        let LoadedRun::Existing(state) = self.repo.load_run(predecessor_run_key).await? else {
            return Err(anyhow!("predecessor run not found for retry successor"));
        };
        let Some(policy) = state.retry_policy.clone() else {
            return Err(anyhow!("retry successor requested without a retry policy"));
        };
        let start_event = self.repo.read_history(predecessor_run_key, 0, 1).await?;
        let input = match start_event.first().map(|event| &event.kind) {
            Some(HistoryEventKind::WorkflowExecutionStarted { input, .. }) => input.clone(),
            _ => Payloads::default(),
        };
        let start_request = build_retry_successor_start(&state, &policy, input, new_run_id);
        let successor_run_key = start_request.run_key;
        let shard_id = self.shard_id_for(successor_run_key).await;
        match self
            .submit(successor_run_key, Command::Start(start_request))
            .await?
        {
            CommitResult::Applied { new_state } => {
                if new_state.workflow_execution_timeout.is_some()
                    || new_state.workflow_run_timeout.is_some()
                {
                    self.workflow_timeout_tracking.insert(WorkflowTimeoutEntry {
                        run_key: new_state.run_key,
                        shard_id,
                        workflow_execution_timeout: new_state.workflow_execution_timeout,
                        workflow_run_timeout: new_state.workflow_run_timeout,
                        started_at: new_state.started_at,
                        first_run_started_at: new_state.first_run_started_at,
                        has_retry_policy: new_state.retry_policy.is_some(),
                    });
                }
                Ok(())
            }
            CommitResult::Duplicate => Ok(()),
            CommitResult::Conflict { reason } => {
                Err(anyhow!("retry successor start conflicted: {reason}"))
            }
            CommitResult::CurrentExecutionConflict {
                existing_run_key, ..
            } => Err(anyhow!(
                "retry successor current-execution conflict: {existing_run_key:?}"
            )),
        }
    }

    /// Atomically transition a polled workflow task into the Started state.
    async fn start_polled_workflow_task(
        &self,
        offered: DispatchableWorkflowTask,
        entered_at: tokio::time::Instant,
        worker_identity: WorkerIdentity,
    ) -> Result<StartedWorkflowTask> {
        let target = self.resolve_polled_workflow_task_target(&offered).await?;
        tracing::debug!(
            run_key = %offered.run_key.0,
            task_queue = %offered.queue.task_queue.0,
            target_deployment = target
                .resolved
                .deployment_version
                .as_ref()
                .map(|version| version.deployment_name.as_str()),
            target_build_id = target
                .resolved
                .deployment_version
                .as_ref()
                .map(|version| version.build_id.as_str()),
            transition_deployment = target
                .deployment_transition
                .as_ref()
                .map(|version| version.deployment_name.as_str()),
            transition_build_id = target
                .deployment_transition
                .as_ref()
                .map(|version| version.build_id.as_str()),
            revision_number = target.resolved.revision_number,
            pinned = target.resolved.pinned,
            "resolved worker deployment target for polled workflow task"
        );
        let now = OffsetDateTime::now_utc();
        let request = StartWorkflowTaskRequest {
            logical_seq: offered.logical_seq,
            worker_identity: worker_identity.clone(),
            request_id: uuid::Uuid::new_v4().to_string(),
            history_size_bytes: 0,
            suggest_continue_as_new: false,
            deployment_transition: target.deployment_transition.clone(),
            deployment_transition_revision_number: target
                .deployment_transition
                .as_ref()
                .map(|_| target.resolved.revision_number),
            sticky_ttl: Some(Duration::seconds(30)),
            now,
        };
        let result = match self
            .submit(offered.run_key, Command::WorkflowTaskStarted(request))
            .await
        {
            Ok(result) => result,
            Err(error) => {
                runtime_metrics::record_workflow_task_started(OutcomeLabel::Failure);
                return Err(error);
            }
        };

        let new_state = match result {
            CommitResult::Applied { new_state } => {
                runtime_metrics::record_workflow_task_started(OutcomeLabel::Success);
                new_state
            }
            CommitResult::Conflict { reason } => {
                runtime_metrics::record_workflow_task_started(OutcomeLabel::Failure);
                return Err(anyhow!(
                    "failed to start workflow task due to conflict: {reason}"
                ));
            }
            CommitResult::CurrentExecutionConflict { .. } => {
                // Unreachable for a workflow-task start (not a zero-seq start).
                runtime_metrics::record_workflow_task_started(OutcomeLabel::Failure);
                return Err(anyhow!(
                    "unexpected current-execution conflict while starting workflow task"
                ));
            }
            CommitResult::Duplicate => {
                runtime_metrics::record_workflow_task_started(OutcomeLabel::Failure);
                return Err(anyhow!("unexpected duplicate while starting workflow task"));
            }
        };

        let pending = new_state
            .pending_workflow_task
            .clone()
            .ok_or_else(|| anyhow!("workflow task missing after start"))?;
        let started_event_id = pending
            .started_event_id
            .ok_or_else(|| anyhow!("workflow task started without started_event_id"))?;

        let token = WorkflowTaskToken {
            run_key: new_state.run_key,
            logical_seq: pending.logical_seq,
            started_event_id,
            attempt: pending.attempt,
            shard_epoch: self.current_shard_epoch(new_state.run_key).await?,
        };
        let shard_id = self.shard_id_for(new_state.run_key).await;
        self.wft_timeout_tracking.insert(WftTimeoutEntry {
            kind: WftTimeoutKind::StartToClose,
            run_key: new_state.run_key,
            shard_id,
            logical_seq: pending.logical_seq,
            started_event_id,
            started_at: pending.started_at.unwrap_or(now),
            workflow_task_timeout: new_state.workflow_task_timeout,
        });
        self.delivery_metrics
            .record_latency(&offered.queue, entered_at.elapsed());

        // Partial-history attach is licensed by STICKY-QUEUE dispatch, not by
        // worker-identity affinity: v1.31.0 trims history to
        // previous_started+1 only when the run's sticky queue is set
        // (`setHistoryForRecordWfTaskStartedResp`,
        // recordworkflowtaskstarted/api.go:272-278; `IsStickyTaskQueueSet`,
        // api.go:418 @ v1.31.0). Tokeira's degraded identity-only hint (empty
        // sticky queue, e.g. a transient retry preferring the failing worker)
        // dispatches on the NORMAL queue and must attach full history — the
        // dispatch queue differing from the run's queue is the sticky-dispatch
        // marker (`schedule_workflow_task` dispatches on the sticky queue
        // name; broker redirects rewrite only build ids, never queue names).
        let is_sticky_match = offered.queue.task_queue != new_state.task_queue
            && offered.sticky_preferred.as_ref() == Some(&worker_identity);
        Ok(StartedWorkflowTask {
            run_key: new_state.run_key,
            run_id: new_state.run_id,
            workflow_id: new_state.workflow_id,
            task_queue: new_state.task_queue,
            previous_started_event_id: new_state.previous_started_event_id,
            is_sticky_match,
            scheduled_time: pending.scheduled_at,
            started_time: pending.started_at.unwrap_or(now),
            workflow_task_timeout: new_state.workflow_task_timeout,
            worker_identity,
            token,
        })
    }

    async fn started_workflow_task_from_state(
        &self,
        state: &WorkflowState,
        is_sticky_match: bool,
        worker_identity: WorkerIdentity,
    ) -> Result<StartedWorkflowTask> {
        let pending = state
            .pending_workflow_task
            .clone()
            .ok_or_else(|| anyhow!("workflow task missing after reserved start"))?;
        let started_event_id = pending
            .started_event_id
            .ok_or_else(|| anyhow!("reserved workflow task missing started_event_id"))?;
        let started_at = pending
            .started_at
            .ok_or_else(|| anyhow!("reserved workflow task missing started_at"))?;
        let token = WorkflowTaskToken {
            run_key: state.run_key,
            logical_seq: pending.logical_seq,
            started_event_id,
            attempt: pending.attempt,
            shard_epoch: self.current_shard_epoch(state.run_key).await?,
        };
        let shard_id = self.shard_id_for(state.run_key).await;
        self.wft_timeout_tracking.insert(WftTimeoutEntry {
            kind: WftTimeoutKind::StartToClose,
            run_key: state.run_key,
            shard_id,
            logical_seq: pending.logical_seq,
            started_event_id,
            started_at,
            workflow_task_timeout: state.workflow_task_timeout,
        });
        Ok(StartedWorkflowTask {
            run_key: state.run_key,
            run_id: state.run_id,
            workflow_id: state.workflow_id.clone(),
            task_queue: state.task_queue.clone(),
            previous_started_event_id: state.previous_started_event_id,
            is_sticky_match,
            scheduled_time: pending.scheduled_at,
            started_time: started_at,
            workflow_task_timeout: state.workflow_task_timeout,
            worker_identity: worker_identity.clone(),
            token,
        })
    }
}

/// Workflow-retry failure classification, mirroring `isRetryable`
/// (`service/history/workflow/retry.go:115 @ v1.31.0`) exactly:
///
/// - no / undecodable / info-less failure → **retryable** (nil → true and the
///   trailing default → true);
/// - Terminated / Canceled info → not retryable;
/// - Timeout info → retryable only for StartToClose / Heartbeat, and then only
///   when `"TemporalTimeout:" + type` is absent from the policy's
///   non-retryable types (`retrypolicy.TimeoutFailureTypePrefix`,
///   retry_policy.go:19);
/// - Server info → `!non_retryable` (the corpus drives workflow retry with
///   `failure.NewServerFailure`, tests/workflow_test.go:1543);
/// - Application info → `!non_retryable` and `type` not excluded.
///
/// Decoding the proto failure lives here because the retry decision is a
/// runtime concern (the kernel stays proto-free).
fn workflow_failure_is_retryable(failure: &Payload, non_retryable_types: &[String]) -> bool {
    use tokeira_proto::enums::TimeoutType;
    let Ok(decoded) = Failure::decode(failure.data.as_slice()) else {
        return true;
    };
    match decoded.failure_info {
        Some(FailureInfo::TerminatedFailureInfo(_)) | Some(FailureInfo::CanceledFailureInfo(_)) => {
            false
        }
        Some(FailureInfo::TimeoutFailureInfo(timeout)) => {
            let (retryable_kind, type_name) = match TimeoutType::try_from(timeout.timeout_type) {
                Ok(TimeoutType::StartToClose) => (true, "StartToClose"),
                Ok(TimeoutType::Heartbeat) => (true, "Heartbeat"),
                _ => (false, ""),
            };
            retryable_kind
                && !non_retryable_types
                    .iter()
                    .any(|excluded| excluded == &format!("TemporalTimeout:{type_name}"))
        }
        Some(FailureInfo::ServerFailureInfo(server)) => !server.non_retryable,
        Some(FailureInfo::ApplicationFailureInfo(app)) => {
            !app.non_retryable
                && !non_retryable_types
                    .iter()
                    .any(|excluded| excluded == &app.r#type)
        }
        _ => true,
    }
}

/// Exponential retry backoff for the attempt-N+1 successor:
/// `initial_interval × backoff_coefficient^(attempt-1)`, capped by
/// `maximum_interval` when set (`retry.go @ v1.31.0`). Wall-clock-free and
/// deterministic, so the retry decision and the successor start compute the same
/// delay without threading it through the kernel.
pub(crate) fn retry_backoff(policy: &RetryPolicy, attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1) as f64;
    let seconds =
        policy.initial_interval.as_seconds_f64() * policy.backoff_coefficient.powf(exponent);
    let capped = match policy.maximum_interval {
        Some(max) if max.as_seconds_f64() > 0.0 => seconds.min(max.as_seconds_f64()),
        _ => seconds,
    };
    Duration::seconds_f64(capped.max(0.0))
}

/// The server failure persisted on an invalid-command `WorkflowTaskFailed`
/// event: message = the rendered cause message, `ServerFailureInfo`,
/// non-retryable false (`failure.NewServerFailure(wtFailedCause.Message(),
/// false)` in `failWorkflowTask`, respondworkflowtaskcompleted/api.go:1049-1059
/// @ v1.31.0).
fn server_failure_payload(message: &str) -> Payload {
    tokeira_proto::conversions::common::failure_to_payload(&Failure {
        message: message.to_string(),
        failure_info: Some(FailureInfo::ServerFailureInfo(
            tokeira_proto::failure::ServerFailureInfo {
                non_retryable: false,
            },
        )),
        ..Default::default()
    })
}

/// Build the attempt-N+1 retry successor start from the CLOSED predecessor's
/// state — shared by the WFT-completion failure path and the workflow-timeout
/// scanner (a RUN timeout with attempts remaining also continues the retry
/// chain, timer_queue_active_task_executor.go:713-796 @ v1.31.0). The
/// successor inherits type/queue/input/policy/timeouts, chains its lineage
/// (`continued_execution_run_id`, first-run identity), and delays its first
/// workflow task by the recomputed backoff.
pub(crate) fn build_retry_successor_start(
    state: &tokeira_kernel::WorkflowState,
    policy: &tokeira_types::RetryPolicy,
    input: Payloads,
    new_run_id: RunId,
) -> StartRequest {
    let backoff = retry_backoff(policy, state.attempt);
    let successor_run_key = RunKey::derive(state.namespace_id, &state.workflow_id, new_run_id);
    // Chain origin propagates so execution-level timeouts and lineage queries
    // span the whole retry chain, not just this hop.
    let first_execution_run_id = Some(state.first_execution_run_id.unwrap_or(state.run_id));
    let first_run_started_at = Some(state.first_run_started_at.unwrap_or(state.started_at));
    // Root identity only propagates within a child lineage.
    let (root_workflow_id, root_run_id) = if state.parent_run_key.is_some() {
        (state.root_workflow_id.clone(), state.root_run_id)
    } else {
        (None, None)
    };
    StartRequest {
        run_key: successor_run_key,
        namespace_id: state.namespace_id,
        workflow_id: state.workflow_id.clone(),
        run_id: new_run_id,
        workflow_type: state.workflow_type.clone(),
        task_queue: state.task_queue.clone(),
        deployment: state.deployment.clone(),
        build_id: state.build_id.clone(),
        versioning_override: state.versioning_override().cloned(),
        workflow_start_delay: Some(backoff),
        completion_callbacks: state.completion_callbacks.clone(),
        user_metadata: state.user_metadata.clone(),
        links: Vec::new(),
        on_conflict_options: None,
        priority: state.priority.clone(),
        input,
        header: None,
        memo: state.memo.clone(),
        search_attributes: state.search_attributes.clone(),
        workflow_execution_timeout: state.workflow_execution_timeout,
        workflow_run_timeout: state.workflow_run_timeout,
        workflow_task_timeout: state.workflow_task_timeout,
        retry_policy: Some(policy.clone()),
        conflict_policy: tokeira_kernel::WorkflowIdConflictPolicy::Fail,
        reuse_policy: tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
        attempt: state.attempt + 1,
        continued_execution_run_id: Some(state.run_id),
        first_execution_run_id,
        parent_run_key: None,
        parent_workflow_id: None,
        parent_run_id: None,
        parent_namespace_id: None,
        parent_initiated_event_id: 0,
        root_workflow_id,
        root_run_id,
        original_execution_run_id: Some(state.original_execution_run_id.unwrap_or(state.run_id)),
        continued_failure: state.close_failure.clone(),
        last_completion_result: state.close_result.clone(),
        first_run_started_at,
        request: RequestContext {
            // Deterministic request id keyed on (predecessor, successor) so a
            // replayed close dedupes to the same successor start instead of
            // forking a duplicate run (mirrors the continue-as-new key).
            request_id: tokeira_types::RequestId(format!(
                "workflow-retry:{}:{}",
                state.run_id.0, new_run_id.0
            )),
            caller_identity: None,
            received_at: OffsetDateTime::now_utc(),
        },
        now: OffsetDateTime::now_utc(),
        client_cron_schedule: None,
        cron_schedule: None,
        reserved_poller_identity: None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashSet};

    use proptest::prelude::*;
    use tokeira_kernel::{ContinueAsNewVersioningBehavior, WorkflowVersioningInfo};
    use tokeira_storage::{BuildId as StorageBuildId, DeploymentName};
    use tokeira_types::{
        BuildId as RuntimeBuildId, DeploymentId, LogicalTaskSeq, Memo, SearchAttributes, TaskKind,
        WorkflowType,
    };

    use super::*;

    fn app_failure(error_type: &str, non_retryable: bool) -> Payload {
        use prost::Message as _;
        use tokeira_proto::failure::{ApplicationFailureInfo, Failure, failure::FailureInfo};
        let failure = Failure {
            message: "boom".to_string(),
            failure_info: Some(FailureInfo::ApplicationFailureInfo(
                ApplicationFailureInfo {
                    r#type: error_type.to_string(),
                    non_retryable,
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        Payload::new(failure.encode_to_vec())
    }

    #[test]
    fn workflow_failure_retryable_classification() {
        // Feature: workflow-retry-chain — classification mirrors `isRetryable`
        // (service/history/workflow/retry.go:115 @ v1.31.0) across every
        // failure-info class, not just ApplicationFailureInfo.
        use prost::Message as _;
        use tokeira_proto::failure::{
            CanceledFailureInfo, Failure, ServerFailureInfo, TerminatedFailureInfo,
            TimeoutFailureInfo, failure::FailureInfo,
        };
        let encoded = |info: FailureInfo| {
            Payload::new(
                Failure {
                    message: "boom".to_string(),
                    failure_info: Some(info),
                    ..Default::default()
                }
                .encode_to_vec(),
            )
        };

        // Application failures: flag, then excluded-type check.
        assert!(workflow_failure_is_retryable(
            &app_failure("Boom", false),
            &[]
        ));
        assert!(!workflow_failure_is_retryable(
            &app_failure("Boom", true),
            &[]
        ));
        assert!(!workflow_failure_is_retryable(
            &app_failure("Fatal", false),
            &["Fatal".to_string()],
        ));

        // Server failures use only their non_retryable flag — the corpus drives
        // workflow retry with failure.NewServerFailure
        // (tests/workflow_test.go:1543 @ v1.31.0), and the excluded-type list
        // does NOT apply to them (retry.go:138-140).
        assert!(workflow_failure_is_retryable(
            &encoded(FailureInfo::ServerFailureInfo(ServerFailureInfo {
                non_retryable: false,
            })),
            &["boom".to_string()],
        ));
        assert!(!workflow_failure_is_retryable(
            &encoded(FailureInfo::ServerFailureInfo(ServerFailureInfo {
                non_retryable: true,
            })),
            &[],
        ));

        // Terminated / Canceled → never retryable (retry.go:120-122).
        assert!(!workflow_failure_is_retryable(
            &encoded(FailureInfo::TerminatedFailureInfo(
                TerminatedFailureInfo::default()
            )),
            &[],
        ));
        assert!(!workflow_failure_is_retryable(
            &encoded(FailureInfo::CanceledFailureInfo(
                CanceledFailureInfo::default()
            )),
            &[],
        ));

        // Timeouts: StartToClose/Heartbeat retry unless excluded via the
        // "TemporalTimeout:" prefix; other timeout kinds never retry
        // (retry.go:124-136; TimeoutFailureTypePrefix, retry_policy.go:19).
        let timeout = |timeout_type: tokeira_proto::enums::TimeoutType| {
            encoded(FailureInfo::TimeoutFailureInfo(TimeoutFailureInfo {
                timeout_type: timeout_type as i32,
                ..Default::default()
            }))
        };
        assert!(workflow_failure_is_retryable(
            &timeout(tokeira_proto::enums::TimeoutType::StartToClose),
            &[],
        ));
        assert!(!workflow_failure_is_retryable(
            &timeout(tokeira_proto::enums::TimeoutType::StartToClose),
            &["TemporalTimeout:StartToClose".to_string()],
        ));
        assert!(!workflow_failure_is_retryable(
            &timeout(tokeira_proto::enums::TimeoutType::ScheduleToClose),
            &[],
        ));

        // No failure info / undecodable → retryable (nil → true and the
        // trailing default → true, retry.go:116-118,152).
        assert!(workflow_failure_is_retryable(
            &Payload::new(Vec::new()),
            &[]
        ));
        assert!(workflow_failure_is_retryable(
            &encoded(FailureInfo::ApplicationFailureInfo(Default::default())),
            &[]
        ));
    }

    #[test]
    fn retry_backoff_matches_exponential_formula() {
        // Feature: workflow-retry-chain — backoff = initial × coeff^(attempt-1),
        // capped by maximum_interval.
        let policy = RetryPolicy {
            initial_interval: Duration::seconds(1),
            backoff_coefficient: 2.0,
            maximum_interval: Some(Duration::seconds(10)),
            maximum_attempts: 0,
            non_retryable_error_types: Vec::new(),
        };
        assert_eq!(retry_backoff(&policy, 1), Duration::seconds(1));
        assert_eq!(retry_backoff(&policy, 2), Duration::seconds(2));
        assert_eq!(retry_backoff(&policy, 3), Duration::seconds(4));
        assert_eq!(retry_backoff(&policy, 6), Duration::seconds(10));

        // TestWorkflowRetry uses coefficient 1.0 → a constant 1s backoff per attempt.
        let flat = RetryPolicy {
            initial_interval: Duration::seconds(1),
            backoff_coefficient: 1.0,
            maximum_interval: None,
            maximum_attempts: 3,
            non_retryable_error_types: Vec::new(),
        };
        assert_eq!(retry_backoff(&flat, 1), Duration::seconds(1));
        assert_eq!(retry_backoff(&flat, 3), Duration::seconds(1));
    }

    #[derive(Clone, Debug)]
    enum VersioningCase {
        Transition,
        OverridePinned,
        OverrideAutoUpgrade,
        BehaviorPinned,
        BehaviorAutoUpgrade,
        Unversioned,
    }

    fn arb_versioning_case() -> impl Strategy<Value = VersioningCase> {
        prop_oneof![
            Just(VersioningCase::Transition),
            Just(VersioningCase::OverridePinned),
            Just(VersioningCase::OverrideAutoUpgrade),
            Just(VersioningCase::BehaviorPinned),
            Just(VersioningCase::BehaviorAutoUpgrade),
            Just(VersioningCase::Unversioned),
        ]
    }

    fn now() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid timestamp")
    }

    fn version_ref(deployment_name: &str, build_id: &str) -> WorkerDeploymentVersionRef {
        WorkerDeploymentVersionRef {
            deployment_name: deployment_name.into(),
            build_id: build_id.into(),
        }
    }

    fn version_key(deployment_name: &str, build_id: &str) -> WorkerDeploymentVersionKey {
        WorkerDeploymentVersionKey {
            deployment_name: DeploymentName(deployment_name.into()),
            build_id: StorageBuildId(build_id.into()),
        }
    }

    fn routing_config(
        current: Option<WorkerDeploymentVersionKey>,
        ramping: Option<WorkerDeploymentVersionKey>,
        ramping_percentage: f32,
    ) -> StoredRoutingConfig {
        StoredRoutingConfig {
            current_version: current,
            ramping_version: ramping,
            ramping_version_percentage: ramping_percentage,
            ramping_to_unversioned: false,
            current_version_changed_time: None,
            ramping_version_changed_time: None,
            ramping_version_percentage_changed_time: None,
            revision_number: 100,
        }
    }

    fn open_state(workflow_id: String, info: Option<WorkflowVersioningInfo>) -> WorkflowState {
        WorkflowState {
            run_key: RunKey::new(),
            namespace_id: NamespaceId::new(),
            workflow_id: WorkflowId(workflow_id),
            run_id: RunId::new(),
            workflow_type: WorkflowType("workflow-type".into()),
            task_queue: TaskQueueName("workflow-task-queue".into()),
            deployment: None,
            build_id: None,
            versioning_info: info,
            worker_deployment_name: None,
            status: ExecutionStatus::Running,
            transition_seq: TransitionSeq(1),
            last_event_id: 1,
            next_workflow_task_seq: LogicalTaskSeq(1),
            pending_workflow_task: None,
            previous_started_event_id: 0,
            workflow_task_attempt: 1,
            sticky: None,
            pause_info: None,
            cancel_requested: false,
            wft_stamp: 0,
            memo: Memo(BTreeMap::new()),
            search_attributes: SearchAttributes(BTreeMap::new()),
            workflow_execution_timeout: Some(Duration::minutes(10)),
            workflow_run_timeout: Some(Duration::minutes(5)),
            workflow_task_timeout: Duration::seconds(10),
            retry_policy: None,
            attempt: 1,
            first_execution_run_id: Some(RunId::new()),
            original_execution_run_id: None,
            parent_run_key: None,
            parent_workflow_id: None,
            parent_run_id: None,
            parent_namespace_id: None,
            parent_initiated_event_id: 0,
            root_workflow_id: None,
            root_run_id: None,
            last_completion_result: None,
            activities: BTreeMap::new(),
            timers: BTreeMap::new(),
            children: BTreeMap::new(),
            pending_external_signals: BTreeMap::new(),
            pending_external_cancels: BTreeMap::new(),
            pending_updates: BTreeMap::new(),
            admitted_updates: HashSet::new(),
            pending_nexus_operations: BTreeMap::new(),
            completion_callbacks: Vec::new(),
            user_metadata: None,
            links: Vec::new(),
            workflow_start_delay: None,
            priority: None,
            started_at: now(),
            first_run_started_at: Some(now()),
            closed_at: None,
            close_result: None,
            close_failure: None,
            request_id_infos: std::collections::BTreeMap::new(),
            buffered_events: Vec::new(),
        }
    }

    fn info_for_case(case: &VersioningCase) -> Option<WorkflowVersioningInfo> {
        let behavior_version = version_ref("deployment", "behavior");
        let override_version = version_ref("deployment", "override");
        let transition_version = version_ref("deployment", "transition");
        match case {
            VersioningCase::Transition => Some(WorkflowVersioningInfo {
                behavior: VersioningBehavior::AutoUpgrade,
                deployment_version: Some(behavior_version),
                versioning_override: Some(VersioningOverride::Pinned {
                    version: override_version,
                }),
                version_transition: Some(transition_version),
                revision_number: 7,
                continue_as_new_initial_versioning_behavior:
                    ContinueAsNewVersioningBehavior::Unspecified,
            }),
            VersioningCase::OverridePinned => Some(WorkflowVersioningInfo {
                behavior: VersioningBehavior::AutoUpgrade,
                deployment_version: Some(behavior_version),
                versioning_override: Some(VersioningOverride::Pinned {
                    version: override_version,
                }),
                version_transition: None,
                revision_number: 8,
                continue_as_new_initial_versioning_behavior:
                    ContinueAsNewVersioningBehavior::Unspecified,
            }),
            VersioningCase::OverrideAutoUpgrade => Some(WorkflowVersioningInfo {
                behavior: VersioningBehavior::Pinned,
                deployment_version: Some(behavior_version),
                versioning_override: Some(VersioningOverride::AutoUpgrade),
                version_transition: None,
                revision_number: 9,
                continue_as_new_initial_versioning_behavior:
                    ContinueAsNewVersioningBehavior::Unspecified,
            }),
            VersioningCase::BehaviorPinned => Some(WorkflowVersioningInfo {
                behavior: VersioningBehavior::Pinned,
                deployment_version: Some(behavior_version),
                versioning_override: None,
                version_transition: None,
                revision_number: 10,
                continue_as_new_initial_versioning_behavior:
                    ContinueAsNewVersioningBehavior::Unspecified,
            }),
            VersioningCase::BehaviorAutoUpgrade => Some(WorkflowVersioningInfo {
                behavior: VersioningBehavior::AutoUpgrade,
                deployment_version: Some(behavior_version),
                versioning_override: None,
                version_transition: None,
                revision_number: 11,
                continue_as_new_initial_versioning_behavior:
                    ContinueAsNewVersioningBehavior::Unspecified,
            }),
            VersioningCase::Unversioned => None,
        }
    }

    fn expected_routing_target(
        routing_config: &StoredRoutingConfig,
        workflow_id: &WorkflowId,
    ) -> Option<WorkerDeploymentVersionRef> {
        let current = routing_config.current_version.as_ref()?;
        if let Some(ramping) = routing_config.ramping_version.as_ref()
            && routing_config.ramping_version_percentage > 0.0
            && deterministic_bucket(&workflow_id.0)
                < (f64::from(routing_config.ramping_version_percentage) * 100.0) as u64
        {
            return Some(version_key_to_ref(ramping));
        }
        Some(version_key_to_ref(current))
    }

    fn workflow_queue(deployment_name: Option<&str>, build_id: Option<&str>) -> QueueKey {
        QueueKey {
            namespace_id: NamespaceId::new(),
            task_queue: TaskQueueName("workflow-task-queue".into()),
            task_kind: TaskKind::Workflow,
            deployment: deployment_name.map(|name| DeploymentId(name.to_string())),
            build_id: build_id.map(|id| RuntimeBuildId(id.to_string())),
        }
    }

    fn expected_for_case(
        case: &VersioningCase,
        routing_config: &StoredRoutingConfig,
        workflow_id: &WorkflowId,
    ) -> ResolvedWorkflowTaskTarget {
        match case {
            VersioningCase::Transition => ResolvedWorkflowTaskTarget {
                deployment_version: Some(version_ref("deployment", "transition")),
                revision_number: 7,
                pinned: false,
            },
            VersioningCase::OverridePinned => ResolvedWorkflowTaskTarget {
                deployment_version: Some(version_ref("deployment", "override")),
                revision_number: 8,
                pinned: true,
            },
            VersioningCase::BehaviorPinned => ResolvedWorkflowTaskTarget {
                deployment_version: Some(version_ref("deployment", "behavior")),
                revision_number: 10,
                pinned: true,
            },
            VersioningCase::OverrideAutoUpgrade
            | VersioningCase::BehaviorAutoUpgrade
            | VersioningCase::Unversioned => ResolvedWorkflowTaskTarget {
                deployment_version: expected_routing_target(routing_config, workflow_id),
                revision_number: routing_config.revision_number,
                pinned: false,
            },
        }
    }

    #[test]
    fn transition_for_polled_workflow_task_starts_when_poller_differs_from_effective() {
        let routing = routing_config(Some(version_key("deployment", "current")), None, 0.0);
        let state = open_state("workflow".into(), None);
        let target = resolve_workflow_task_target_version(&routing, &state);

        // Unversioned run (effective deployment None): any versioned poller
        // differs from the effective deployment and starts a transition toward
        // the poller's version — matching v1.31.0's `!pollerDeployment.Equal(
        // wfDeployment)`. This holds even when the poller is NOT the current
        // routing target ("stale"): Matching already dispatched the task here,
        // and re-checking against the routing config would suppress a
        // legitimate transition.
        assert_eq!(
            transition_for_polled_workflow_task(
                &state,
                &target,
                &workflow_queue(Some("deployment"), Some("current")),
            ),
            Some(version_ref("deployment", "current"))
        );
        assert_eq!(
            transition_for_polled_workflow_task(
                &state,
                &target,
                &workflow_queue(Some("deployment"), Some("stale")),
            ),
            Some(version_ref("deployment", "stale"))
        );
        // An unversioned poller (no deployment/build_id) cannot start a
        // transition.
        assert_eq!(
            transition_for_polled_workflow_task(&state, &target, &workflow_queue(None, None)),
            None
        );

        let already_effective = open_state(
            "workflow".into(),
            Some(WorkflowVersioningInfo {
                behavior: VersioningBehavior::AutoUpgrade,
                deployment_version: Some(version_ref("deployment", "current")),
                versioning_override: None,
                version_transition: None,
                revision_number: 100,
                continue_as_new_initial_versioning_behavior:
                    ContinueAsNewVersioningBehavior::Unspecified,
            }),
        );
        let target = resolve_workflow_task_target_version(&routing, &already_effective);
        // Poller already equals the run's effective deployment: no transition.
        assert_eq!(
            transition_for_polled_workflow_task(
                &already_effective,
                &target,
                &workflow_queue(Some("deployment"), Some("current")),
            ),
            None
        );

        let pinned = open_state(
            "workflow".into(),
            Some(WorkflowVersioningInfo {
                behavior: VersioningBehavior::Pinned,
                deployment_version: Some(version_ref("deployment", "current")),
                versioning_override: None,
                version_transition: None,
                revision_number: 100,
                continue_as_new_initial_versioning_behavior:
                    ContinueAsNewVersioningBehavior::Unspecified,
            }),
        );
        let target = resolve_workflow_task_target_version(&routing, &pinned);
        // Pinned runs never transition, even when a differing poller arrives.
        assert_eq!(
            transition_for_polled_workflow_task(
                &pinned,
                &target,
                &workflow_queue(Some("deployment"), Some("current")),
            ),
            None
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        fn property_routing_determinism_and_effective_version_precedence(
            case in arb_versioning_case(),
            workflow_suffix in 0u32..10_000,
            current_present in any::<bool>(),
            ramping_present in any::<bool>(),
            ramping_percentage in prop_oneof![Just(0.0f32), Just(1.0), Just(25.0), Just(50.0), Just(75.0), Just(100.0)],
        ) {
            let routing = routing_config(
                current_present.then(|| version_key("deployment", "current")),
                ramping_present.then(|| version_key("deployment", "ramping")),
                ramping_percentage,
            );
            let state = open_state(format!("workflow-{workflow_suffix}"), info_for_case(&case));

            let resolved = resolve_workflow_task_target_version(&routing, &state);
            let resolved_again = resolve_workflow_task_target_version(&routing, &state);
            let expected = expected_for_case(&case, &routing, &state.workflow_id);

            prop_assert_eq!(&resolved, &resolved_again);
            prop_assert_eq!(&resolved, &expected);
            if !current_present
                && matches!(
                    case,
                    VersioningCase::OverrideAutoUpgrade
                        | VersioningCase::BehaviorAutoUpgrade
                        | VersioningCase::Unversioned
                )
            {
                prop_assert!(resolved.deployment_version.is_none());
            }
        }
    }
}
