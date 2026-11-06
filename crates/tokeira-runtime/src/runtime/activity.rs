//! Activity-task lifecycle methods of [`TokeiraRuntime`].
//!
//! This `impl` continuation owns the worker-facing activity-task surface:
//! ordinary local and forwarded delivery, the atomic poll→Started transition, and
//! the completion / failure / cancel / heartbeat resolutions that feed an
//! activity outcome back into its owning run. Every mutating path applies a
//! kernel command on the run's owned shard via
//! [`submit_for_owned_shard`](TokeiraRuntime::submit_for_owned_shard), so it is
//! fenced by the run's shard epoch rather than trusting the in-memory broker.
//!
//! Invariants this slice upholds:
//! - The broker and `activity_tracking` are delivery aids, never authority. A
//!   task token is honoured only after its `(schedule_event_id, attempt,
//!   shard_epoch)` identity is revalidated against durable state, so a stale
//!   or replayed token cannot mutate a run.
//! - Retry transitions retain canonical eligibility; discovery reconstructs lost
//!   offers. Start OCC retries retain their request UUID and original acquisition,
//!   so neither retry nor lost replies can authorize a second start.
//!
//! Each worker-facing method runs in a span whose `tokeira.command_type` names
//! its Temporal request, the cause the DSQL round-trip metric charges its reads
//! and commits to (`tokeira_observability::statement_accounting`): these paths
//! load runs, and starts, heartbeats and retries commit, outside the span of
//! any lane command. A transition a method submits to the lane is charged to
//! the lane's command instead. The spans are
//! DEBUG so they add nothing to logs or exported traces at the default filter.
//! Each method boxes its body before instrumenting it, as the start path does:
//! the body holds whole run images across awaits, and `#[instrument]` would
//! hold the body twice in the caller's future, which overflows the stack of an
//! unoptimized build.
use tokeira_kernel::deadline_after;

use super::*;
use crate::{
    ActivityStartOutcome, ActivityTaskStartRequest,
    lane::{KernelRejected, LaneHandle},
    runtime::workflow_task::{
        ResolvedWorkflowTaskTarget, poller_deployment_version,
        resolve_workflow_task_target_version, route_activity_task_queue,
    },
};
use tokeira_kernel::{ActivityStartMode, ActivityStartRequest, limits::RunLimitExceeded};
use tokeira_observability::OutcomeLabel;
use tokeira_types::{TaskKind, WorkerTaskClass, WorkerTaskOrigin};
use tracing::Instrument as _;

#[derive(Clone, Debug)]
pub(super) struct ActivitySchedule {
    pub(super) input: Payloads,
    pub(super) header: Option<Headers>,
    pub(super) scheduled_time: OffsetDateTime,
    pub(super) schedule_to_close_timeout: Option<Duration>,
    pub(super) start_to_close_timeout: Option<Duration>,
    pub(super) heartbeat_timeout: Option<Duration>,
}

pub(super) fn activity_start_response(
    state: &WorkflowState,
    activity: &tokeira_kernel::ActivityState,
    task: &DispatchableActivityTask,
    scheduled: &ActivitySchedule,
    epoch: ShardEpoch,
    replayed: bool,
) -> StartedActivityTask {
    // Matching builds a duplicate-start response from the scheduled event;
    // history's early same-request branch omits these fresh-only fields.
    // service/matching/matching_engine.go createPollActivityTaskQueueResponse
    // and service/history/api/recordactivitytaskstarted/api.go @ v1.31.0.
    StartedActivityTask {
        replayed,
        priority: merge_priority(state.priority.as_ref(), activity.priority.as_ref()),
        run_key: task.run_key,
        run_id: state.run_id,
        activity_id: activity.activity_id.clone(),
        activity_type: activity.activity_type.clone(),
        task_queue: task.queue.task_queue.clone(),
        token: ActivityTaskToken {
            run_key: task.run_key,
            activity_id: activity.activity_id.clone(),
            schedule_event_id: activity.schedule_event_id,
            attempt: activity.attempt,
            shard_epoch: epoch,
        },
        input: scheduled.input.clone(),
        attempt: activity.attempt,
        workflow_id: state.workflow_id.0.clone(),
        workflow_type: if replayed {
            String::new()
        } else {
            state.workflow_type.0.clone()
        },
        header: scheduled.header.clone(),
        retry_policy: (!replayed).then(|| activity.retry_policy.clone()).flatten(),
        heartbeat_details: (!replayed)
            .then(|| activity.heartbeat_details.clone())
            .flatten(),
        scheduled_time: if replayed {
            scheduled.scheduled_time
        } else {
            activity.scheduled_at
        },
        current_attempt_scheduled_time: activity.current_attempt_scheduled_at,
        started_time: activity
            .started_at
            .expect("a start response requires a started attempt"),
        schedule_to_close_timeout: if replayed {
            Some(
                scheduled
                    .schedule_to_close_timeout
                    .unwrap_or(Duration::ZERO),
            )
        } else {
            activity.schedule_to_close_timeout
        },
        start_to_close_timeout: if replayed {
            scheduled.start_to_close_timeout
        } else {
            activity.start_to_close_timeout
        },
        heartbeat_timeout: if replayed {
            scheduled.heartbeat_timeout
        } else {
            activity.heartbeat_timeout
        },
        origin: WorkerTaskOrigin::from_queue_key(
            &task.queue,
            task.queue.task_queue.clone(),
            WorkerTaskClass::Activity,
        ),
    }
}

/// Decide whether an activity-start poller should move the workflow deployment.
///
/// Activity starts are stricter than workflow-task starts. Temporal v1.31.0
/// allows independently-versioned activities, so a differing activity poller
/// starts a workflow transition only when it is the same deployment the WFT
/// would currently target, or when it is in that deployment and was dispatched
/// at a revision strictly newer than the WFT target
/// (`service/history/api/recordactivitytaskstarted/api.go:188, :283 @
/// v1.31.0`). Equal revision is intentionally not enough: it represents a
/// non-backlogged activity whose deployment choice should not pull the workflow
/// away from its current effective deployment.
fn transition_for_polled_activity_task(
    state: &WorkflowState,
    wft_target: &ResolvedWorkflowTaskTarget,
    queue: &QueueKey,
    dispatch_revision: i64,
) -> Option<tokeira_kernel::WorkerDeploymentVersionRef> {
    if state.effective_behavior() == tokeira_kernel::VersioningBehavior::Pinned {
        return None;
    }

    let poller_version = poller_deployment_version(queue)?;
    if state.effective_deployment() == Some(&poller_version) {
        return None;
    }

    let wft_version = wft_target.deployment_version.as_ref()?;
    let same_deployment = poller_version.deployment_name == wft_version.deployment_name;
    let ahead_of_wft_revision = dispatch_revision > wft_target.revision_number;
    (poller_version == *wft_version || (same_deployment && ahead_of_wft_revision))
        .then_some(poller_version)
}

fn activity_start_rejected_by_in_flight_transition(state: &WorkflowState) -> bool {
    state
        .versioning_info
        .as_ref()
        .and_then(|info| info.version_transition.as_ref())
        .is_some()
}

/// Refuse to act on an activity of a closed run.
///
/// A closed run keeps its pending activities in state, as v1.31.0 keeps their
/// infos for Describe, so finding the activity proves nothing. Every v1.31.0
/// activity API checks that the workflow is running before it looks at the
/// activity and answers `ErrWorkflowCompleted` otherwise
/// (recordactivitytaskheartbeat/api.go:51-53, respondactivitytaskcompleted/api.go:60,
/// respondactivitytaskfailed/api.go:61, respondactivitytaskcanceled/api.go:59
/// @ v1.31.0); the edge renders `RunClosed` as that error.
fn ensure_run_open(state: &WorkflowState) -> Result<()> {
    if state.is_open() {
        return Ok(());
    }
    Err(KernelRejected(tokeira_kernel::Reject::RunClosed(state.status)).into())
}

/// Map the runtime's retry-exhaustion reason onto the wire `RetryState`
/// carried by terminal activity resolutions (`nextBackoffInterval`,
/// retry.go:96-110 @ v1.31.0).
pub(crate) fn exhausted_reason_to_retry_state(reason: RetryExhaustedReason) -> RetryState {
    match reason {
        RetryExhaustedReason::NonRetryableFailure => RetryState::NonRetryableFailure,
        RetryExhaustedReason::MaximumAttemptsReached => RetryState::MaximumAttemptsReached,
        RetryExhaustedReason::Timeout => RetryState::Timeout,
    }
}

impl<R> TokeiraRuntime<R>
where
    R: RunRepository + 'static,
{
    /// Re-evaluate unversioned activity backlog after a deployment version first
    /// registers this activity task queue.
    ///
    /// v1.31.0 reprocesses already-spooled tasks when deployment user data
    /// changes, so an activity initially classified as independent can become
    /// dependent on its pinned workflow version before dispatch
    /// (`TestPinnedWorkflowWithLateActivityPoller`; task-queue user-data change
    /// handling in `service/matching/task_queue_partition_manager.go @
    /// v1.31.0`). Tokeira applies the observable behavior to its disposable
    /// bounded live offers, re-deriving every candidate from
    /// authoritative run and deployment state.
    pub async fn reprocess_unversioned_activity_backlog(&self, target: &QueueKey) -> Result<()> {
        self.activity_discovery
            .wake_family(target, tokio::time::Instant::now());
        let Some(registry) = self.deployment_registry() else {
            return Ok(());
        };
        let source = QueueKey {
            namespace_id: target.namespace_id,
            task_queue: target.task_queue.clone(),
            task_kind: TaskKind::Activity,
            deployment: None,
            build_id: None,
        };

        let mut routes = std::collections::HashMap::new();
        for task in self.activity_broker.unversioned_ready_tasks(target).await {
            let LoadedRun::Existing(state) = self.repo.load_run(task.run_key).await? else {
                continue;
            };
            let (queue, _) = route_activity_task_queue(
                Some(&registry),
                &state,
                task.queue.clone(),
                task.dispatch_revision,
            )
            .await?;
            if queue != source {
                routes.insert(
                    (task.run_key, task.activity_id.clone(), task.attempt),
                    queue,
                );
            }
        }
        self.activity_broker
            .reroute_unversioned_ready_tasks(target, &routes)
            .await;

        Ok(())
    }

    /// Long-poll for an activity task, then atomically mark it as started.
    pub async fn poll_activity_task(
        &self,
        queue: QueueKey,
        worker_identity: WorkerIdentity,
        timeout_after: tokio::time::Duration,
    ) -> Result<Option<StartedActivityTask>> {
        let deadline = tokio::time::Instant::now() + timeout_after;
        loop {
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Ok(None);
            }
            let Some(offer) = self
                .poll_activity_task_offer(queue.clone(), worker_identity.clone(), deadline - now)
                .await?
            else {
                return Ok(None);
            };
            let request = ActivityTaskStartRequest {
                offered: offer.task.clone(),
                request_id: offer.request_id,
                worker: worker_identity.clone(),
            };
            let mut outcome = None;
            for attempt in 0..3 {
                let until = deadline
                    .min(tokio::time::Instant::now() + tokio::time::Duration::from_secs(10));
                let result = tokio::time::timeout_at(
                    until,
                    self.route_activity_start(request.clone(), deadline),
                )
                .await
                .unwrap_or_else(|_| {
                    Err(crate::RetryableActivityTaskStart("owner attempt timed out".into()).into())
                });
                match result {
                    Ok(result) => {
                        outcome = Some(result);
                        break;
                    }
                    Err(error)
                        if (error.is::<crate::RetryableActivityTaskStart>()
                            || error.is::<crate::NotShardOwner>())
                            && attempt < 2
                            && tokio::time::Instant::now() < deadline =>
                    {
                        tokio::task::yield_now().await;
                    }
                    Err(error) => return Err(error),
                }
            }
            // Keep the offer lease through every ambiguous attempt. A replacement
            // poll must use a different request identity, never recover this one.
            drop(offer);
            if let Some(task) = outcome.and_then(ActivityStartOutcome::into_task) {
                return Ok(Some(task));
            }
        }
    }

    async fn route_activity_start(
        &self,
        request: ActivityTaskStartRequest,
        deadline: tokio::time::Instant,
    ) -> Result<ActivityStartOutcome> {
        let LoadedRun::Existing(state) = tokeira_storage::read_probe::scope(
            "queue-home/activity-owner",
            self.repo.load_run(request.offered.run_key),
        )
        .await?
        else {
            return Ok(ActivityStartOutcome::Stale);
        };
        if state.namespace_id != request.offered.queue.namespace_id {
            return Ok(ActivityStartOutcome::Stale);
        }
        let home = self.execution_home(&state);
        let local = self
            .shard_owner
            .read()
            .expect("shard owner lock poisoned")
            .is_active(home);
        if !local {
            let starter = self
                .activity_task_starter
                .read()
                .expect("activity starter lock poisoned")
                .clone();
            if let Some(starter) = starter {
                return starter.start(home, request, deadline).await;
            }
        }
        self.start_forwarded_activity_task(request).await
    }

    /// Poll matching without committing the Started transition.
    ///
    /// The edge uses this narrow seam to evaluate namespace workflow rules at
    /// the same "about to start" point as v1.31.0. The offer remains only a
    /// delivery premise; the subsequent start call still revalidates durable
    /// run state and commits atomically.
    pub async fn poll_activity_task_offer(
        &self,
        queue: QueueKey,
        worker_identity: WorkerIdentity,
        timeout_after: tokio::time::Duration,
    ) -> Result<Option<ActivityTaskOffer>> {
        self.ready().await?;
        let home =
            crate::discovery::QueueHomeProvider::local_home(self.discovery.1.as_ref(), &queue)
                .ok_or_else(|| anyhow::anyhow!("activity queue home is not active"))?;
        let _demand = self.activity_discovery.register_poll(
            queue.clone(),
            home,
            tokio::time::Instant::now(),
        )?;
        match self
            .activity_broker
            .poll_activity_offer_for_worker(&queue, &worker_identity, timeout_after)
            .await?
        {
            Some(offer) => {
                self.delivery_metrics.record_poll_success(&queue);
                Ok(Some(offer))
            }
            None => {
                self.delivery_metrics.record_poll_timeout(&queue);
                Ok(None)
            }
        }
    }

    /// Revalidate and start a previously polled activity offer.
    pub async fn start_activity_task_offer(
        &self,
        offer: ActivityTaskOffer,
        worker_identity: WorkerIdentity,
    ) -> Result<Option<StartedActivityTask>> {
        self.start_activity_task(
            &offer.task,
            offer.request_id,
            offer.entered_at,
            &worker_identity,
        )
        .await
    }

    /// Start or recover one admitted activity match at its execution owner.
    /// The receiver performs worker admission and installs its response preflight
    /// before this call; this method never forwards to another owner.
    pub async fn start_forwarded_activity_task(
        &self,
        request: ActivityTaskStartRequest,
    ) -> Result<ActivityStartOutcome> {
        self.start_activity_task_traced(
            &request.offered,
            request.request_id,
            tokio::time::Instant::now(),
            &request.worker,
        )
        .await
    }

    async fn commit_activity_start_gate(
        &self,
        state: &WorkflowState,
        attempt: &crate::outbound::driver::Attempt,
        command: Command,
    ) -> Result<bool> {
        // Rules and routing were evaluated before awaiting the lane. Bind both
        // the state image and acquisition so that an ID replacement or takeover
        // cannot inherit the decision, while retaining normal lane side effects.
        let lane =
            crate::scanner::pick_lane_for_run_key(&self.lanes, self.lanes.len(), state.run_key);
        let result = lane
            .submit_observed(
                state.run_key,
                command,
                crate::lane::ObservedSubmission {
                    attempt: attempt.clone(),
                    sequence: state.transition_seq,
                    run_id: state.run_id,
                },
            )
            .await?;
        Ok(matches!(result, CommitResult::Applied { .. }))
    }

    async fn activity_start_rule_command(
        &self,
        state: &WorkflowState,
        activity: &tokeira_kernel::ActivityState,
        backoff_interval_seconds: Option<i64>,
    ) -> Result<Option<Command>> {
        let rules = self.repo.list_workflow_rules(state.namespace_id).await?;
        let now = self.clock.now();
        let Some(rule) =
            matching_pause_rule(state, activity, &rules, now, backoff_interval_seconds)
        else {
            return Ok(None);
        };
        let rule_id = rule.id.clone();
        Ok(Some(Command::PauseActivity(PauseActivityRequest {
            target: ActivityControlTarget::Id(activity.activity_id.clone()),
            identity: rule.created_by_identity.clone(),
            reason: rule.description.clone(),
            request: RequestContext {
                request_id: RequestId(format!(
                    "workflow-rule-{rule_id}-{}-{}",
                    activity.activity_id, activity.attempt
                )),
                caller_identity: (!rule.created_by_identity.is_empty())
                    .then(|| rule.created_by_identity.clone()),
                principal: None,
                received_at: now,
            },
            now,
            rule_id: Some(rule_id),
        })))
    }

    /// Record a successful activity completion and
    /// resolve it in the owning workflow.
    pub async fn complete_activity_task(
        &self,
        token: ActivityTaskToken,
        result: Payloads,
        worker_identity: Option<WorkerIdentity>,
        request: RequestContext,
    ) -> Result<CommitResult> {
        Box::pin(self.complete_activity_task_inner(token, result, worker_identity, request))
            .instrument(tracing::debug_span!(
                "activity_task.complete",
                tokeira.command_type = "RespondActivityTaskCompleted"
            ))
            .await
    }

    async fn complete_activity_task_inner(
        &self,
        token: ActivityTaskToken,
        result: Payloads,
        worker_identity: Option<WorkerIdentity>,
        request: RequestContext,
    ) -> Result<CommitResult> {
        let activity_id = token.activity_id.clone();
        let (_, _, home) = self
            .validate_activity_token(&token)
            .await
            .inspect_err(|_| {
                runtime_metrics::record_activity_task_completed(OutcomeLabel::Rejected);
            })?;
        let result = self
            .submit_for_owned_home(
                token.run_key,
                home,
                Command::ActivityResolved(ActivityResolvedRequest {
                    activity_id,
                    resolution: ActivityResolution::Completed { result },
                    worker_identity,
                    request,
                    now: self.clock.now(),
                }),
            )
            .await?;
        match &result {
            CommitResult::Applied { .. } | CommitResult::Duplicate => {
                runtime_metrics::record_activity_task_completed(OutcomeLabel::Success);
            }
            CommitResult::Conflict { .. } | CommitResult::CurrentExecutionConflict { .. } => {
                runtime_metrics::record_activity_task_completed(OutcomeLabel::Failure);
            }
        }
        if matches!(
            result,
            CommitResult::Applied { .. } | CommitResult::Duplicate
        ) {
            self.activity_tracking
                .remove(token.run_key, &token.activity_id);
        }
        Ok(result)
    }

    /// Record an activity failure. If the retry policy
    /// allows, the activity is re-dispatched at the next
    /// attempt; otherwise it is resolved as failed.
    ///
    /// `last_heartbeat_details` are the progress a worker reports with the
    /// failure. A retry records them, and their time, as a heartbeat does
    /// (respondactivitytaskfailed/api.go:87-94 @ v1.31.0); a terminal failure
    /// removes the activity, so they have nowhere to go.
    /// `RespondActivityTaskFailedById` passes `None`: v1.31.0's frontend drops
    /// them on that path (frontend/workflow_handler.go:1965-1969).
    pub async fn fail_activity_task(
        &self,
        token: ActivityTaskToken,
        failure: Payload,
        failure_error_type: Option<String>,
        is_non_retryable: bool,
        last_heartbeat_details: Option<Payloads>,
        worker_identity: Option<WorkerIdentity>,
        request: RequestContext,
    ) -> Result<()> {
        Box::pin(self.fail_activity_task_inner(
            token,
            failure,
            failure_error_type,
            is_non_retryable,
            last_heartbeat_details,
            worker_identity,
            request,
        ))
        .instrument(tracing::debug_span!(
            "activity_task.fail",
            tokeira.command_type = "RespondActivityTaskFailed"
        ))
        .await
    }

    async fn fail_activity_task_inner(
        &self,
        token: ActivityTaskToken,
        failure: Payload,
        failure_error_type: Option<String>,
        is_non_retryable: bool,
        last_heartbeat_details: Option<Payloads>,
        worker_identity: Option<WorkerIdentity>,
        request: RequestContext,
    ) -> Result<()> {
        let (activity, workflow_retry_policy, home) =
            match self.validate_activity_token(&token).await {
                Ok(validated) => validated,
                Err(error) => {
                    runtime_metrics::record_activity_task_failed(OutcomeLabel::Rejected);
                    return Err(error);
                }
            };
        let activity_id = token.activity_id.clone();
        let retry_policy = activity.retry_policy.clone().or(workflow_retry_policy);

        // Retry-expiration anchor: the FIRST schedule + schedule-to-close (the
        // deadline spans the whole retry chain; `retry.go:108-110 @ v1.31.0`).
        let now = self.clock.now();
        let expiration = activity
            .schedule_to_close_timeout
            .filter(|timeout| !timeout.is_zero())
            .map(|timeout| deadline_after(activity.scheduled_at, timeout));
        let should_retry = retry_policy.as_ref().map(|policy| {
            evaluate_activity_retry(
                policy,
                activity.attempt,
                failure_error_type.as_deref(),
                is_non_retryable,
                now,
                expiration,
            )
        });

        if let Some(RetryDecision::Retry {
            next_attempt,
            backoff,
        }) = should_retry
        {
            match self
                .retry_activity_task(
                    &token,
                    next_attempt,
                    backoff,
                    Some(failure.clone()),
                    last_heartbeat_details,
                )
                .await
            {
                Ok(()) => runtime_metrics::record_activity_task_retry(OutcomeLabel::Success),
                Err(error) => {
                    runtime_metrics::record_activity_task_retry(OutcomeLabel::Failure);
                    return Err(error);
                }
            }
            return Ok(());
        }

        // Terminal failure: the resolution carries WHY retries stopped
        // (`RetryActivity` → `AddActivityTaskFailedEvent(..., retryState)`,
        // mutable_state_impl.go:6235-6320 @ v1.31.0). No policy →
        // RETRY_POLICY_NOT_SET. CANCEL_REQUESTED lands with kernel raise K4
        // (durable cancel_requested).
        let retry_state = match should_retry {
            None => RetryState::RetryPolicyNotSet,
            Some(RetryDecision::Exhausted { reason }) => exhausted_reason_to_retry_state(reason),
            // `Retry` returned above.
            Some(RetryDecision::Retry { .. }) => unreachable!("retry path returns early"),
        };
        let result = self
            .submit_for_owned_home(
                token.run_key,
                home,
                Command::ActivityResolved(ActivityResolvedRequest {
                    activity_id,
                    resolution: ActivityResolution::Failed {
                        failure,
                        retry_state,
                    },
                    worker_identity,
                    request,
                    now: self.clock.now(),
                }),
            )
            .await;
        match &result {
            Ok(CommitResult::Applied { .. } | CommitResult::Duplicate) => {
                runtime_metrics::record_activity_task_failed(OutcomeLabel::Success);
            }
            Ok(CommitResult::Conflict { .. })
            | Ok(CommitResult::CurrentExecutionConflict { .. })
            | Err(_) => {
                runtime_metrics::record_activity_task_failed(OutcomeLabel::Failure);
            }
        }
        result?;
        self.activity_tracking
            .remove(token.run_key, &token.activity_id);
        Ok(())
    }

    /// Resolve an activity as cancelled and notify the owning workflow.
    ///
    /// `details` are the worker-reported cancellation payloads recorded on the
    /// resolution. Like the other resolutions, the in-memory tracking entry is
    /// only removed once the kernel actually applies the transition (Applied or
    /// Duplicate); on a Conflict the entry is left so a retry can still resolve.
    pub async fn cancel_activity_task(
        &self,
        token: ActivityTaskToken,
        details: Option<Payloads>,
        worker_identity: Option<WorkerIdentity>,
        request: RequestContext,
    ) -> Result<CommitResult> {
        Box::pin(self.cancel_activity_task_inner(token, details, worker_identity, request))
            .instrument(tracing::debug_span!(
                "activity_task.cancel",
                tokeira.command_type = "RespondActivityTaskCanceled"
            ))
            .await
    }

    async fn cancel_activity_task_inner(
        &self,
        token: ActivityTaskToken,
        details: Option<Payloads>,
        worker_identity: Option<WorkerIdentity>,
        request: RequestContext,
    ) -> Result<CommitResult> {
        let activity_id = token.activity_id.clone();
        let (_, _, home) = self.validate_activity_token(&token).await?;
        let result = self
            .submit_for_owned_home(
                token.run_key,
                home,
                Command::ActivityResolved(ActivityResolvedRequest {
                    activity_id,
                    resolution: ActivityResolution::Canceled { details },
                    worker_identity,
                    request,
                    now: self.clock.now(),
                }),
            )
            .await?;
        if matches!(
            result,
            CommitResult::Applied { .. } | CommitResult::Duplicate
        ) {
            self.activity_tracking
                .remove(token.run_key, &token.activity_id);
        }
        Ok(result)
    }

    /// Record an activity heartbeat and return the independent worker-control
    /// flags attached to the authoritative activity state.
    ///
    /// The heartbeat persists progress on durable activity state without
    /// emitting history, matching Temporal's mutable activity-info update.
    /// Its response returns cancel, pause, and reset independently
    /// (`recordactivitytaskheartbeat/api.go:103-105 @ v1.31.0`).
    /// Volatile tracking remains responsible only for timeout and cooperative
    /// cancellation liveness.
    pub async fn record_activity_heartbeat(
        &self,
        token: ActivityTaskToken,
        details: Option<Payloads>,
        identity: Option<WorkerIdentity>,
    ) -> Result<ActivityHeartbeatOutcome> {
        Box::pin(self.record_activity_heartbeat_inner(token, details, identity))
            .instrument(tracing::debug_span!(
                "activity_task.heartbeat",
                tokeira.command_type = "RecordActivityTaskHeartbeat"
            ))
            .await
    }

    async fn record_activity_heartbeat_inner(
        &self,
        token: ActivityTaskToken,
        details: Option<Payloads>,
        identity: Option<WorkerIdentity>,
    ) -> Result<ActivityHeartbeatOutcome> {
        self.ready().await?;
        let mut attempts = 0u32;
        loop {
            // Identity failures are the typed [`ActivityTaskNotFound`] —
            // v1.31.0 heartbeats reject stale/unknown tokens AND
            // not-yet-started activities with `ErrActivityTaskNotFound`
            // (recordactivitytaskheartbeat/api.go:73-75 +
            // IsActivityTaskNotFoundForToken, activity_util.go:58 @ v1.31.0).
            let LoadedRun::Existing(state) = self.repo.load_run(token.run_key).await? else {
                return Err(ActivityTaskNotFound {
                    reason: "run not found for activity heartbeat",
                }
                .into());
            };
            ensure_run_open(&state)?;
            let Some(current) = state.activities.get(&token.activity_id).cloned() else {
                return Err(ActivityTaskNotFound {
                    reason: "activity not found for heartbeat",
                }
                .into());
            };
            if current.schedule_event_id != token.schedule_event_id {
                return Err(ActivityTaskNotFound {
                    reason: "activity heartbeat schedule_event_id mismatch",
                }
                .into());
            }
            if current.attempt != token.attempt {
                return Err(ActivityTaskNotFound {
                    reason: "activity heartbeat attempt mismatch",
                }
                .into());
            }
            if current.started_event_id.is_none() {
                return Err(ActivityTaskNotFound {
                    reason: "activity heartbeat for activity that has not started",
                }
                .into());
            }
            if token.shard_epoch != self.shard_epoch_for_completion(&state).await? {
                return Err(anyhow!("activity heartbeat shard epoch mismatch"));
            }

            let mut next_state = state.clone();
            next_state.transition_seq = state.transition_seq.next();
            let mut next_activity = current;
            next_activity.heartbeat_details = details.clone();
            // `UpdateActivityProgress` records the time with the details
            // (mutable_state_impl.go:1964-1966 @ v1.31.0). The same instant
            // goes to volatile tracking below, so the deadline the owning node
            // evaluates and the one a new owner restores agree.
            let now = self.clock.now();
            next_activity.last_heartbeat_at = Some(now);
            // A heartbeat carrying a non-empty identity updates the retry
            // bookkeeping identity, Describe's `LastWorkerIdentity` fallback
            // (`ai.RetryLastWorkerIdentity = req.HeartbeatRequest.Identity`,
            // recordactivitytaskheartbeat/api.go:79-81 @ v1.31.0; K3).
            if let Some(identity) = identity.clone().filter(|identity| !identity.0.is_empty()) {
                next_activity.retry_last_worker_identity = Some(identity);
            }
            next_state
                .activities
                .insert(token.activity_id.clone(), next_activity.clone());

            let outcome = ActivityHeartbeatOutcome {
                cancel_requested: next_activity.cancel_requested,
                activity_paused: next_activity.pause_info.is_some(),
                activity_reset: next_activity.activity_reset,
            };
            let transition = Transition {
                expected_seq: state.transition_seq,
                next_state,
                history_events: SmallVec::new(),
                event_principals: SmallVec::new(),
                request_dedupe_ops: SmallVec::new(),
                activity_ops: smallvec![ActivityOp::Upsert(next_activity)],
                timer_ops: SmallVec::new(),
                dispatch_ops: SmallVec::new(),
                events_numbered_at_close: 0,
                workflow_task_heartbeat_timed_out: false,
                growth_limits: Some(crate::run_growth::run_growth_limits()),
            };

            let (bundle, fence) = {
                let owner = self.shard_owner.read().expect("shard_owner lock poisoned");
                let bundle_id = execution_home_bundle(
                    state.namespace_id.0.as_bytes(),
                    state.workflow_id.0.as_bytes(),
                    owner.shard_count(),
                );
                let fence = owner.write_fence(bundle_id).ok_or_else(|| {
                    crate::errors::NotShardOwner::local(bundle_id, ShardEpoch::ZERO)
                })?;
                (bundle_id, fence)
            };

            let mut write_admission = crate::serving_gate::admit(&self.shard_owner, bundle).await?;
            let committed = self
                .repo
                .commit_transition_for_bundle(token.run_key, bundle, transition, fence)
                .await
                .map_err(|error| crate::errors::lease_loss(error, &self.shard_owner));
            match terminate_on_growth_breach(
                &self.lanes,
                token.run_key,
                committed,
                &mut write_admission,
                self.clock.as_ref(),
            )
            .await?
            {
                CommitResult::Applied { .. } | CommitResult::Duplicate => {
                    // Reset the volatile heartbeat clock; the CANCEL signal
                    // itself comes from the durable state loaded above — the
                    // kernel's `cancel_requested` bit is the authority
                    // (`cancelRequested = ai.CancelRequested`,
                    // recordactivitytaskheartbeat/api.go:83 @ v1.31.0; K4).
                    let tracking_cancel = self
                        .activity_tracking
                        .record_heartbeat(token.run_key, &token.activity_id, now)
                        .unwrap_or(false);
                    return Ok(ActivityHeartbeatOutcome {
                        cancel_requested: outcome.cancel_requested || tracking_cancel,
                        ..outcome
                    });
                }
                CommitResult::Conflict { .. } | CommitResult::CurrentExecutionConflict { .. } => {
                    if attempts >= self.config.max_occ_retries {
                        return Err(anyhow!("activity heartbeat OCC exhausted"));
                    }
                    attempts += 1;
                }
            }
        }
    }

    /// Reconstruct an [`ActivityTaskToken`] for an already-started activity
    /// from durable state.
    ///
    /// Used by RecordActivityTaskHeartbeatById / RespondActivityTask*ById,
    /// where the caller addresses the activity by `(run_key, activity_id)`
    /// instead of presenting a token. Stamps the token with the *live* shard
    /// epoch so a token minted here is fenced identically to one handed out at
    /// poll time. Fails with [`ActivityTokenResolutionError::RunClosed`] if the
    /// run is closed, and otherwise with
    /// [`ActivityTokenResolutionError::ActivityNotStarted`] if the activity has
    /// no `started_event_id`, because a not-yet-started activity has no
    /// completion identity to address.
    pub async fn resolve_activity_token(
        &self,
        run_key: RunKey,
        activity_id: &str,
    ) -> std::result::Result<ActivityTaskToken, ActivityTokenResolutionError> {
        let loaded = self
            .repo
            .load_run(run_key)
            .await
            .map_err(|error| ActivityTokenResolutionError::Runtime(error.to_string()))?;
        let LoadedRun::Existing(state) = loaded else {
            return Err(ActivityTokenResolutionError::RunNotFound { run_key });
        };
        // Every by-id verb checks that the run is running before it resolves
        // the activity id, so a closed run answers `ErrWorkflowCompleted` even
        // when its activity is gone or never started
        // (recordactivitytaskheartbeat/api.go:51-53,
        // respondactivitytaskcompleted/api.go:60-61,
        // respondactivitytaskfailed/api.go:61-62,
        // respondactivitytaskcanceled/api.go:59-60 @ v1.31.0).
        if !state.is_open() {
            return Err(ActivityTokenResolutionError::RunClosed { run_key });
        }
        let activity = state.activities.get(activity_id).ok_or_else(|| {
            ActivityTokenResolutionError::ActivityNotFound {
                run_key,
                activity_id: activity_id.to_string(),
            }
        })?;
        if activity.started_event_id.is_none() {
            return Err(ActivityTokenResolutionError::ActivityNotStarted {
                run_key,
                activity_id: activity_id.to_string(),
            });
        }
        let shard_epoch = self
            .shard_epoch_for_completion(&state)
            .await
            .map_err(|error| ActivityTokenResolutionError::Runtime(error.to_string()))?;
        Ok(ActivityTaskToken {
            run_key,
            activity_id: activity_id.to_string(),
            schedule_event_id: activity.schedule_event_id,
            attempt: activity.attempt,
            shard_epoch,
        })
    }

    /// Fabricate the `ActivityTaskStarted` event for a not-yet-started
    /// activity so a by-id completion can force-complete it, returning a
    /// token for the (now started) activity.
    ///
    /// `RespondActivityTaskCompletedById` on an unstarted activity does not
    /// reject: v1.31.0 adds a started event carrying the *completing
    /// caller's* identity and then completes
    /// (`respondactivitytaskcompleted/api.go:89-105 @ v1.31.0`; the
    /// `isCompletedByID` escape in `IsActivityTaskNotFoundForToken`,
    /// activity_util.go:58-67). Only the completed-by-id verb gets this —
    /// failed/canceled/heartbeat pass a nil `isCompletedByID` and reject.
    ///
    /// If a worker races us and starts the activity first, the freshly loaded
    /// `started_event_id` is honoured and a token for the real start is
    /// returned instead. The started event fabricated here is identical in
    /// shape to `Self::start_activity_task`'s; a stale broker offer for
    /// this activity dies at claim time against `started_event_id`.
    pub async fn force_start_activity_for_completion(
        &self,
        run_key: RunKey,
        activity_id: &str,
        identity: WorkerIdentity,
        request: RequestContext,
    ) -> Result<ActivityTaskToken> {
        Box::pin(self.force_start_activity_for_completion_inner(
            run_key,
            activity_id,
            identity,
            request,
        ))
        .instrument(tracing::debug_span!(
            "activity_task.force_start",
            tokeira.command_type = "RespondActivityTaskCompleted"
        ))
        .await
    }

    async fn force_start_activity_for_completion_inner(
        &self,
        run_key: RunKey,
        activity_id: &str,
        identity: WorkerIdentity,
        request: RequestContext,
    ) -> Result<ActivityTaskToken> {
        self.ready().await?;
        let mut attempts = 0u32;
        loop {
            let LoadedRun::Existing(state) = self.repo.load_run(run_key).await? else {
                return Err(ActivityTaskNotFound {
                    reason: "run not found for by-id force start",
                }
                .into());
            };
            ensure_run_open(&state)?;
            let Some(current) = state.activities.get(activity_id).cloned() else {
                return Err(ActivityTaskNotFound {
                    reason: "activity not found for by-id force start",
                }
                .into());
            };
            let token = ActivityTaskToken {
                run_key,
                activity_id: activity_id.to_string(),
                schedule_event_id: current.schedule_event_id,
                attempt: current.attempt,
                shard_epoch: self.shard_epoch_for_completion(&state).await?,
            };
            if current.started_event_id.is_some() {
                return Ok(token);
            }

            // The kernel computes the start, so its event follows the same
            // append rule as every other — buffered while a workflow task is
            // started, appended after a scheduled speculative task converts —
            // and passes the transition check. The fabricated start carries the
            // completing caller's identity (respondactivitytaskcompleted/api.go:96
            // @ v1.31.0).
            let mut transition = BasicKernel
                .apply_activity_started(
                    LoadedRun::Existing(state.clone()),
                    ActivityStartRequest {
                        request_id: None,
                        activity_id: activity_id.to_string(),
                        identity: identity.clone(),
                        principal: request.principal.clone(),
                        mode: ActivityStartMode::CompletedById,
                        now: self.clock.now(),
                    },
                )
                .map_err(|reject| anyhow::Error::new(KernelRejected(reject)))?;
            transition.growth_limits = Some(crate::run_growth::run_growth_limits());

            // Same fence as start_activity_task (`bundle-lease-fence`).
            let (bundle, fence) = {
                let owner = self.shard_owner.read().expect("shard_owner lock poisoned");
                let bundle_id = execution_home_bundle(
                    state.namespace_id.0.as_bytes(),
                    state.workflow_id.0.as_bytes(),
                    owner.shard_count(),
                );
                let fence = owner.write_fence(bundle_id).ok_or_else(|| {
                    crate::errors::NotShardOwner::local(bundle_id, ShardEpoch::ZERO)
                })?;
                (bundle_id, fence)
            };

            let mut write_admission = crate::serving_gate::admit(&self.shard_owner, bundle).await?;
            let committed = self
                .repo
                .commit_transition_for_bundle(run_key, bundle, transition, fence)
                .await
                .map_err(|error| crate::errors::lease_loss(error, &self.shard_owner));
            match terminate_on_growth_breach(
                &self.lanes,
                run_key,
                committed,
                &mut write_admission,
                self.clock.as_ref(),
            )
            .await?
            {
                CommitResult::Applied { new_state } => {
                    runtime_metrics::record_activity_task_started(OutcomeLabel::Success);
                    self.activity_tracking.record_started_with_timeouts(
                        run_key,
                        activity_id,
                        self.clock.now(),
                        new_state
                            .activities
                            .get(activity_id)
                            .map(crate::activity_timeout::ActivityTimeouts::from),
                    );
                    return Ok(token);
                }
                CommitResult::Conflict { .. } | CommitResult::CurrentExecutionConflict { .. } => {
                    if attempts >= self.config.max_occ_retries {
                        return Err(anyhow!("by-id force start OCC exhausted"));
                    }
                    attempts += 1;
                }
                // A duplicate means this exact transition already committed;
                // re-loop to observe the started activity and mint its token.
                CommitResult::Duplicate => {
                    attempts += 1;
                }
            }
        }
    }

    /// Persist a worker outcome using the acquisition that published its task.
    /// A stale, expired or consumed incarnation is acknowledged without changing
    /// history; a transient repository error leaves discovery responsible for retry.
    pub async fn resolve_nexus_worker_result(
        &self,
        authority: crate::NexusDeliveryAuthority,
        result: crate::NexusWorkerResult,
    ) -> Result<bool> {
        if !authority.active() {
            return Ok(false);
        }
        authority.attempt.validate(&self.shard_owner)?;
        let row = &authority.row;
        let LoadedRun::Existing(state) = self.repo.load_run(row.incarnation.key.source_run).await?
        else {
            return Ok(false);
        };
        if !tokeira_storage::derive_operations(&state, row.source_home)?
            .iter()
            .any(|pending| pending.incarnation == row.incarnation)
        {
            return Ok(false);
        }
        let Some(pending) = state
            .pending_nexus_operations
            .values()
            .find(|p| p.scheduled_event_id == row.incarnation.key.initiating_event)
        else {
            return Ok(false);
        };
        let command = match (row.incarnation.key.kind, result) {
            (
                tokeira_kernel::OperationKind::NexusStart,
                crate::NexusWorkerResult::Start(resolution),
            ) => Command::NexusOperationResolved(tokeira_kernel::NexusOperationResolvedRequest {
                expected_attempt: Some(row.incarnation.attempt),
                operation_id: pending.operation_id.clone(),
                scheduled_event_id: pending.scheduled_event_id,
                resolution,
                now: self.clock.now(),
            }),
            (
                tokeira_kernel::OperationKind::NexusCancel,
                crate::NexusWorkerResult::Cancel(outcome),
            ) => Command::NexusCancellationAttempted(
                tokeira_kernel::NexusCancellationAttemptedRequest {
                    expected_attempt: row.incarnation.attempt,
                    operation_id: pending.operation_id.clone(),
                    scheduled_event_id: pending.scheduled_event_id,
                    requested_event_id: row.incarnation.requested_event,
                    outcome,
                    now: self.clock.now(),
                },
            ),
            _ => return Ok(false),
        };
        let lane =
            crate::scanner::pick_lane_for_run_key(&self.lanes, self.lanes.len(), state.run_key);
        let result = lane
            .submit_outbound(
                state.run_key,
                command,
                crate::lane::OutboundSubmission {
                    source_fence: authority.attempt.fence.clone(),
                    attempt: authority.attempt,
                    authority: tokeira_storage::DeliveryGuard::Operation {
                        incarnation: row.incarnation,
                    },
                },
            )
            .await;
        match result {
            Ok(CommitResult::Applied { .. } | CommitResult::Duplicate) => Ok(true),
            Ok(_) => Ok(false),
            Err(error)
                if error.downcast_ref::<KernelRejected>().is_some()
                    || error
                        .downcast_ref::<tokeira_storage::RecoveryError>()
                        .is_some_and(|e| matches!(e, tokeira_storage::RecoveryError::Obsolete)) =>
            {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    /// Resolve a Nexus operation back into its originator workflow.
    ///
    /// Returns `Ok(false)` when the kernel rejects the resolution as stale or
    /// otherwise already-applied. That lets the edge treat duplicate worker
    /// completions as idempotent success.
    pub async fn resolve_nexus_operation(
        &self,
        run_key: RunKey,
        operation_id: String,
        scheduled_event_id: i64,
        expected_attempt: Option<u32>,
        resolution: tokeira_kernel::NexusResolution,
    ) -> Result<bool> {
        match self
            .submit_for_owned_shard(
                run_key,
                Command::NexusOperationResolved(tokeira_kernel::NexusOperationResolvedRequest {
                    expected_attempt,
                    operation_id,
                    scheduled_event_id,
                    resolution,
                    now: self.clock.now(),
                }),
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(error)
                if error
                    .downcast_ref::<crate::lane::KernelRejected>()
                    .is_some() =>
            {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    /// Record one worker-delivered Nexus cancellation attempt without resolving
    /// the parent operation. The kernel fences both the scheduled operation and
    /// cancel-request event; stale duplicate worker responses are idempotent success.
    pub async fn record_nexus_cancellation_attempt(
        &self,
        run_key: RunKey,
        operation_id: String,
        scheduled_event_id: i64,
        requested_event_id: i64,
        expected_attempt: u32,
        outcome: tokeira_kernel::NexusCancellationAttemptOutcome,
    ) -> Result<bool> {
        match self
            .submit_for_owned_shard(
                run_key,
                Command::NexusCancellationAttempted(
                    tokeira_kernel::NexusCancellationAttemptedRequest {
                        expected_attempt,
                        operation_id,
                        scheduled_event_id,
                        requested_event_id,
                        outcome,
                        now: self.clock.now(),
                    },
                ),
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(error)
                if error
                    .downcast_ref::<crate::lane::KernelRejected>()
                    .is_some() =>
            {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    /// Atomically transition a polled activity task into the Started state,
    /// emitting `ActivityTaskStarted` and returning the dispatchable task.
    ///
    /// The transition is guarded by an OCC loop: it reloads the run, re-checks
    /// that the offer still matches the current `(attempt, schedule_event_id)`
    /// and is not already started, then commits. A mismatch means the offer is
    /// stale (run closed, activity already started/retried) and yields
    /// `Ok(None)` so the poller simply sees no work. On commit conflict the
    /// start is retried up to `max_occ_retries`; if that is exhausted the offer
    /// returns a retryable owner error. The same request can retry, and canonical
    /// discovery recovers the work if that poll ends.
    ///
    /// The input and header come from the immutable ActivityTaskScheduled event.
    async fn start_activity_task(
        &self,
        task: &DispatchableActivityTask,
        request_id: Uuid,
        entered_at: tokio::time::Instant,
        worker_identity: &WorkerIdentity,
    ) -> Result<Option<StartedActivityTask>> {
        self.start_activity_task_traced(task, request_id, entered_at, worker_identity)
            .await
            .map(ActivityStartOutcome::into_task)
    }

    // Local polls and forwarded owner starts share the same span boundary:
    // changing placement must not remove host-owned activity processing telemetry.
    async fn start_activity_task_traced(
        &self,
        task: &DispatchableActivityTask,
        request_id: Uuid,
        entered_at: tokio::time::Instant,
        worker_identity: &WorkerIdentity,
    ) -> Result<ActivityStartOutcome> {
        let span = tracing::info_span!(
            "activity_task.process",
            tokeira.command_type = "RecordActivityTaskStarted",
            tokeira.run_key = %task.run_key.0,
            tokeira.workflow_id = tracing::field::Empty,
            tokeira.run_id = tracing::field::Empty,
            tokeira.activity_id = task.activity_id.as_str(),
            tokeira.activity_type = tracing::field::Empty,
            tokeira.task_queue = %task.queue.task_queue.0,
            tokeira.attempt = i64::from(task.attempt),
        );
        // Owner processing retains whole run images across awaits. Keeping its
        // future on the heap prevents poll/forwarding wrappers from
        // multiplying that state on the caller's stack in unoptimized builds.
        let result =
            Box::pin(self.start_activity_task_inner(task, request_id, entered_at, worker_identity))
                .instrument(span.clone())
                .await;
        if let Ok(
            ActivityStartOutcome::Started(started) | ActivityStartOutcome::Replayed(started),
        ) = &result
        {
            span.record("tokeira.workflow_id", started.workflow_id.as_str());
            span.record("tokeira.run_id", started.run_id.0.to_string());
            span.record("tokeira.activity_type", started.activity_type.as_str());
        }
        result
    }

    async fn start_activity_task_inner(
        &self,
        task: &DispatchableActivityTask,
        request_id: Uuid,
        entered_at: tokio::time::Instant,
        worker_identity: &WorkerIdentity,
    ) -> Result<ActivityStartOutcome> {
        ActivityTaskStartRequest {
            offered: task.clone(),
            request_id,
            worker: worker_identity.clone(),
        }
        .validate()?;
        self.ready().await?;
        let mut scheduled: Option<ActivitySchedule> = None;
        let mut attempts = 0u32;
        let mut captured_owner = None;
        loop {
            let LoadedRun::Existing(state) = tokeira_storage::read_probe::scope(
                "execution-owner/activity-start",
                self.repo.load_run(task.run_key),
            )
            .await?
            else {
                return Ok(ActivityStartOutcome::Stale);
            };
            if !state.is_open() {
                return Ok(ActivityStartOutcome::Stale);
            }
            let Some(current) = state.activities.get(&task.activity_id).cloned() else {
                return Ok(ActivityStartOutcome::Stale);
            };
            if state.namespace_id != task.queue.namespace_id {
                return Ok(ActivityStartOutcome::Stale);
            }
            let bundle = self.execution_home(&state);
            let owner_attempt = match &captured_owner {
                Some(attempt) => attempt,
                None => captured_owner.insert(
                    crate::outbound::driver::Attempt::capture(&self.shard_owner, bundle)
                        .ok_or_else(|| {
                            crate::errors::NotShardOwner::local(bundle, ShardEpoch::ZERO)
                        })?,
                ),
            };
            // OCC retries belong to the acquisition that admitted this call.
            // Only a new owner RPC may capture a replacement's fence; an old
            // in-flight attempt cannot silently become that replacement.
            owner_attempt.validate(&self.shard_owner)?;
            let start_tracking = self
                .activity_tracking
                .for_acquisition(owner_attempt.acquisition.clone())
                .for_activity(task.run_key, &task.activity_id);
            if current.started_event_id.is_some() {
                // Temporal checks same-request identity before pause/stamp gates.
                // Control edits can advance the current stamp after this start;
                // only the receipt's consumed stamp binds the original request.
                // service/history/api/recordactivitytaskstarted/api.go @ v1.31.0.
                let matching = state
                    .activity_start_receipts
                    .get(&task.activity_id)
                    .is_some_and(|receipt| {
                        receipt.request_id == request_id
                            && receipt.schedule_event_id == task.schedule_event_id
                            && receipt.attempt == task.attempt
                            && receipt.offered_stamp == task.stamp
                            && receipt.worker_identity == *worker_identity
                            && receipt.selected_queue == task.queue
                            && receipt.dispatch_revision == task.dispatch_revision
                            && current.schedule_event_id == task.schedule_event_id
                            && current.attempt == task.attempt
                            && current.started_at.is_some()
                    });
                if !matching {
                    return Ok(ActivityStartOutcome::Stale);
                }
                if scheduled.is_none() {
                    scheduled = Some(self.read_scheduled_input(task).await?);
                }
                let started = activity_start_response(
                    &state,
                    &current,
                    task,
                    scheduled.as_ref().expect("loaded schedule"),
                    owner_attempt.acquisition.epoch,
                    true,
                );
                crate::activity_task_start::preflight_activity_start(&started)?;
                let _admission = crate::serving_gate::admit(&self.shard_owner, bundle).await?;
                owner_attempt.validate(&self.shard_owner)?;
                let current_observation = tokeira_storage::read_probe::scope(
                    "execution-owner/activity-replay",
                    self.repo.validate_activity_start_observation(
                        &state,
                        &task.activity_id,
                        bundle,
                        owner_attempt.fence.clone(),
                    ),
                )
                .await
                .map_err(|error| crate::errors::lease_loss(error, &self.shard_owner))?;
                owner_attempt.validate(&self.shard_owner)?;
                if current_observation {
                    return Ok(ActivityStartOutcome::Replayed(started));
                }
                if attempts >= self.config.max_occ_retries {
                    return Err(crate::RetryableActivityTaskStart(
                        "replay observation changed repeatedly".into(),
                    )
                    .into());
                }
                attempts += 1;
                continue;
            }
            if current.task_queue != task.queue.task_queue {
                return Ok(ActivityStartOutcome::Stale);
            }
            if current
                .current_attempt_scheduled_at
                .is_none_or(|at| at > self.clock.now())
            {
                return Ok(ActivityStartOutcome::Suppressed);
            }
            // Durable dispatch rows are deleted by the pause transition, but
            // an ephemeral broker offer may already be in flight. Recheck the
            // authoritative pause bits before starting so that stale offer is
            // consumed without defeating either workflow- or activity-level
            // pause (`PauseActivity`, activity.go:263-284 @ v1.31.0).
            if state.status == ExecutionStatus::Paused || current.pause_info.is_some() {
                return Ok(ActivityStartOutcome::Suppressed);
            }
            if current.attempt != task.attempt
                || current.schedule_event_id != task.schedule_event_id
            {
                return Ok(ActivityStartOutcome::Stale);
            }
            // Fence a superseded offer via the activity stamp. Every mutation
            // that invalidates an outstanding dispatch — pause, unpause, reset,
            // and options updates (including a task-queue move or a lengthened
            // retry backoff) — bumps the stamp, so an offer published before the
            // change carries a stale stamp and must not start the activity. This
            // is v1.31.0's `ObsoleteMatchingTask` stamp check
            // (recordactivitytaskstarted/api.go @ v1.31.0).
            if current.stamp != task.stamp {
                return Ok(ActivityStartOutcome::Stale);
            }
            if current.started_event_id.is_some() {
                return Ok(ActivityStartOutcome::Stale);
            }

            // Ordinary local and forwarded polls evaluate rules immediately
            // before Started. Completion-owned eager starts bypass this gate.
            // Reading durable rules here avoids the frontend admission race and mirrors
            // `recordactivitytaskstarted/api.go:332-372 @ v1.31.0`. The
            // backoff interval is derived from durable state so a
            // `BackoffInterval` rule created after publication but before
            // start is still enforced.
            if let Some(command) = self
                .activity_start_rule_command(
                    &state,
                    &current,
                    activity_backoff_interval_seconds(&current)?,
                )
                .await?
            {
                if self
                    .commit_activity_start_gate(&state, owner_attempt, command)
                    .await?
                {
                    return Ok(ActivityStartOutcome::Suppressed);
                }
                if attempts >= self.config.max_occ_retries {
                    return Err(crate::RetryableActivityTaskStart(
                        "rule observation changed repeatedly".into(),
                    )
                    .into());
                }
                attempts += 1;
                continue;
            }

            if activity_start_rejected_by_in_flight_transition(&state) {
                return Ok(ActivityStartOutcome::Suppressed);
            }

            // The activity poller is on an activity task queue, but the
            // transition question is "where would the workflow task route now?"
            // Temporal reads workflow-task routing when evaluating an activity
            // start transition (`getDeploymentVersionAndRevisionNumberForWorkflowID`,
            // recordactivitytaskstarted/api.go:283 @ v1.31.0).
            let workflow_queue = QueueKey {
                namespace_id: state.namespace_id,
                task_queue: state.task_queue.clone(),
                task_kind: TaskKind::Workflow,
                // An unversioned run has no durable deployment family yet. In
                // that case v1.31.0 still looks up the workflow queue's Current
                // target using the polling activity worker's Deployment before
                // deciding whether to start a transition and withhold the
                // activity (`recordactivitytaskstarted/api.go:188-225 @
                // v1.31.0`). The poller's version is only a registry lookup
                // hint; the workflow task queue name remains authoritative.
                deployment: state
                    .deployment
                    .clone()
                    .or_else(|| task.queue.deployment.clone()),
                build_id: state
                    .build_id
                    .clone()
                    .or_else(|| task.queue.build_id.clone()),
            };
            let routing_config = self
                .load_worker_deployment_routing_config(&state, &workflow_queue)
                .await?;
            let wft_target = resolve_workflow_task_target_version(&routing_config, &state);
            if let Some(target) = transition_for_polled_activity_task(
                &state,
                &wft_target,
                &task.queue,
                task.dispatch_revision,
            ) {
                // Temporal v1.31.0 `recordactivitytaskstarted/api.go:188`
                // rejects activity starts during/for deployment transitions.
                // The matching mutable-state path starts the transition and
                // requests `CreateWorkflowTask`; this kernel command applies
                // the same transition and schedules a WFT when needed.
                let command = Command::StartDeploymentTransition(
                    tokeira_kernel::StartDeploymentTransitionRequest {
                        target,
                        revision_number: task.dispatch_revision,
                        now: self.clock.now(),
                    },
                );
                if self
                    .commit_activity_start_gate(&state, owner_attempt, command)
                    .await?
                {
                    return Ok(ActivityStartOutcome::Suppressed);
                }
                if attempts >= self.config.max_occ_retries {
                    return Err(crate::RetryableActivityTaskStart(
                        "routing observation changed repeatedly".into(),
                    )
                    .into());
                }
                attempts += 1;
                continue;
            }

            // The kernel computes the start, so its event follows the same
            // append rule as every other — buffered while a workflow task is
            // started, appended after a scheduled speculative task converts —
            // and passes the transition check. A retry-policy start stays
            // transient, with no event until the activity resolves. The polling
            // worker's identity becomes `started_identity`, Describe's primary
            // `LastWorkerIdentity` source (workflow/activity.go:159 @ v1.31.0).
            let now = self.clock.now();
            let mut transition = BasicKernel
                .apply_activity_started(
                    LoadedRun::Existing(state.clone()),
                    ActivityStartRequest {
                        request_id: Some(request_id),
                        activity_id: task.activity_id.clone(),
                        identity: worker_identity.clone(),
                        principal: None,
                        mode: ActivityStartMode::Poll,
                        now,
                    },
                )
                .map_err(|reject| anyhow::Error::new(KernelRejected(reject)))?;
            transition.next_state.activity_start_receipts.insert(
                task.activity_id.clone(),
                tokeira_kernel::ActivityStartReceipt {
                    request_id,
                    schedule_event_id: task.schedule_event_id,
                    attempt: task.attempt,
                    offered_stamp: task.stamp,
                    worker_identity: worker_identity.clone(),
                    selected_queue: task.queue.clone(),
                    dispatch_revision: task.dispatch_revision,
                },
            );
            transition.growth_limits = Some(crate::run_growth::run_growth_limits());
            let next_activity = transition
                .next_state
                .activities
                .get(&task.activity_id)
                .cloned()
                .expect("the kernel keeps the activity it just started");

            // Every check that can refuse the start has passed, so a refused
            // start reads nothing. The read precedes the commit, as v1.31.0 reads
            // the event before it records the start
            // (`recordactivitytaskstarted/api.go:139-145, 242 @ v1.31.0`). The
            // event never changes, so a retry after a conflict keeps what the
            // first pass read.
            if scheduled.is_none() {
                scheduled = Some(self.read_scheduled_input(task).await?);
            }

            let started = activity_start_response(
                &state,
                &next_activity,
                task,
                scheduled
                    .as_ref()
                    .expect("the start reads its input before commit"),
                owner_attempt.acquisition.epoch,
                false,
            );
            crate::activity_task_start::preflight_activity_start(&started)?;
            let mut write_admission = crate::serving_gate::admit(&self.shard_owner, bundle).await?;
            owner_attempt.validate(&self.shard_owner)?;
            let committed = self
                .repo
                .commit_transition_for_bundle(
                    task.run_key,
                    bundle,
                    transition,
                    owner_attempt.fence.clone(),
                )
                .await
                .map_err(|error| crate::errors::lease_loss(error, &self.shard_owner));
            let committed = terminate_on_growth_breach(
                &self.lanes,
                task.run_key,
                committed,
                &mut write_admission,
                self.clock.as_ref(),
            )
            .await;
            // A start refused for a growth limit terminated the run, so the poll
            // goes on, as for a task whose run has closed (`run-growth-limits`
            // criterion 2.4).
            if committed
                .as_ref()
                .is_err_and(|error| error.downcast_ref::<RunLimitExceeded>().is_some())
            {
                runtime_metrics::record_activity_task_started(OutcomeLabel::Failure);
                return Ok(ActivityStartOutcome::Stale);
            }
            match committed? {
                CommitResult::Applied { .. } => {
                    runtime_metrics::record_activity_task_started(OutcomeLabel::Success);
                    self.delivery_metrics
                        .record_latency(&task.queue, entered_at.elapsed());
                    // Callback authority stays with the acquisition that committed;
                    // a replacement owner must reconstruct its own timeout tracking.
                    start_tracking.record_started_with_timeouts(
                        task.run_key,
                        &next_activity.activity_id,
                        now,
                        Some(crate::activity_timeout::ActivityTimeouts::from(
                            &next_activity,
                        )),
                    );
                    return Ok(ActivityStartOutcome::Started(started));
                }
                CommitResult::Conflict { .. } | CommitResult::CurrentExecutionConflict { .. } => {
                    if attempts >= self.config.max_occ_retries {
                        runtime_metrics::record_activity_task_started(OutcomeLabel::Failure);
                        return Err(crate::RetryableActivityTaskStart(
                            "activity start OCC budget exhausted".into(),
                        )
                        .into());
                    }
                    attempts += 1;
                }
                CommitResult::Duplicate => {
                    runtime_metrics::record_activity_task_started(OutcomeLabel::Failure);
                    return Ok(ActivityStartOutcome::Stale);
                }
            }
        }
    }

    /// The input and header of `task`'s ActivityTaskScheduled event, the only
    /// place they live (`activity-inputs-from-history` criterion 2.2), read with
    /// one lookup (criterion 2.3).
    ///
    /// When the read fails, or the history holds no such event, the start
    /// commits nothing: the task goes back to its queue, as when the start's
    /// commit retries run out, and the start answers the read's error, or an
    /// internal error for the missing event, as v1.31.0 answers
    /// `ErrMissingActivityScheduledEvent` (`mutable_state_impl.go:109-110,
    /// 1476-1481 @ v1.31.0`; criterion 2.4).
    async fn read_scheduled_input(
        &self,
        task: &DispatchableActivityTask,
    ) -> Result<ActivitySchedule> {
        match self
            .repo
            .read_history_event(task.run_key, task.schedule_event_id)
            .await
        {
            // An event of another kind or activity at the id means history and
            // state disagree; it answers as a missing event does.
            Ok(Some(tokeira_kernel::HistoryEvent {
                kind:
                    HistoryEventKind::ActivityTaskScheduled {
                        activity_id,
                        input,
                        header,
                        schedule_to_close_timeout,
                        start_to_close_timeout,
                        heartbeat_timeout,
                        ..
                    },
                happened_at,
                ..
            })) if activity_id == task.activity_id => Ok(ActivitySchedule {
                input,
                header,
                scheduled_time: happened_at,
                schedule_to_close_timeout,
                start_to_close_timeout,
                heartbeat_timeout,
            }),
            Ok(_) => Err(anyhow!(
                "unable to get activity scheduled event {} of activity {} in run {}",
                task.schedule_event_id,
                task.activity_id,
                task.run_key.0
            )),
            Err(error) => Err(error),
        }
    }

    /// Revalidate a worker-presented activity token against durable state.
    ///
    /// This is the front-line fence for every activity completion path: a token
    /// is honoured only if its `(schedule_event_id, attempt)` still match the
    /// live activity AND its `shard_epoch` matches the shard's current epoch.
    /// The epoch check rejects completions from a worker that polled while this
    /// node owned the shard but is reporting after ownership moved, so a stale
    /// owner cannot resolve an activity on a shard it no longer owns. Returns
    /// the activity state plus the workflow-level retry policy (the fallback
    /// when the activity has no policy of its own).
    ///
    /// Identity failures (gone run/activity, stale `schedule_event_id` or
    /// `attempt`) are the typed [`ActivityTaskNotFound`] so the edge can
    /// surface v1.31.0's `ErrActivityTaskNotFound` (`RespondActivityTask*` /
    /// `RecordActivityTaskHeartbeat` all funnel token mismatches there via
    /// `IsActivityTaskNotFoundForToken`, activity_util.go:58 @ v1.31.0). The
    /// epoch check stays an internal error: it is tokeira placement fencing,
    /// not a Temporal-visible condition.
    async fn validate_activity_token(
        &self,
        token: &ActivityTaskToken,
    ) -> Result<(tokeira_kernel::ActivityState, Option<RetryPolicy>, ShardId)> {
        self.ready().await?;
        let LoadedRun::Existing(state) = self.repo.load_run(token.run_key).await? else {
            return Err(ActivityTaskNotFound {
                reason: "run not found for activity token",
            }
            .into());
        };
        ensure_run_open(&state)?;
        let Some(activity) = state.activities.get(&token.activity_id).cloned() else {
            return Err(ActivityTaskNotFound {
                reason: "activity not found for token",
            }
            .into());
        };
        if activity.schedule_event_id != token.schedule_event_id {
            return Err(ActivityTaskNotFound {
                reason: "activity schedule_event_id mismatch",
            }
            .into());
        }
        if activity.attempt != token.attempt {
            return Err(ActivityTaskNotFound {
                reason: "activity attempt mismatch",
            }
            .into());
        }
        if token.shard_epoch != self.shard_epoch_for_completion(&state).await? {
            return Err(anyhow!("activity shard epoch mismatch"));
        }
        Ok((
            activity,
            state.retry_policy.clone(),
            self.execution_home(&state),
        ))
    }

    /// Re-dispatch an activity at its next attempt after a retryable failure.
    ///
    /// Thin wrapper over [`commit_activity_retry`]: the worker's task token
    /// supplies the staleness fence (`attempt`, `schedule_event_id`) that the
    /// shared commit revalidates against durable state.
    async fn retry_activity_task(
        &self,
        token: &ActivityTaskToken,
        next_attempt: u32,
        backoff: time::Duration,
        failure: Option<Payload>,
        progress: Option<Payloads>,
    ) -> Result<()> {
        commit_activity_retry(
            &self.activity_retry_deps(),
            ActivityRetryTarget {
                run_key: token.run_key,
                activity_id: &token.activity_id,
                expected_attempt: token.attempt,
                expected_schedule_event_id: token.schedule_event_id,
            },
            next_attempt,
            backoff,
            failure,
            progress,
        )
        .await
    }

    /// Bundle the runtime handles [`commit_activity_retry`] needs, so the
    /// activity timeout scanner (a free task without `&self`) can run the
    /// same retry transition as the worker-reported failure path.
    pub(crate) fn activity_retry_deps(&self) -> ActivityRetryDeps<R> {
        ActivityRetryDeps {
            repo: self.repo.clone(),
            shard_owner: self.shard_owner.clone(),
            controller_managed_placement: self.config.controller_managed_placement,
            max_occ_retries: self.config.max_occ_retries,
            broker: self.activity_broker.clone(),
            delivery_metrics: self.delivery_metrics.clone(),
            tracking: self.activity_tracking.clone(),
            worker_deployment_registry: self.worker_deployment_registry.clone(),
            lanes: self.lanes.clone(),
            clock: self.clock.clone(),
        }
    }
}

/// Runtime handles shared by the failure-path retry
/// ([`TokeiraRuntime::retry_activity_task`]) and the timeout scanner's
/// retry-on-timeout, which runs outside the runtime `impl`.
pub(crate) struct ActivityRetryDeps<R> {
    pub repo: Arc<R>,
    pub shard_owner: Arc<RwLock<ShardOwner>>,
    pub controller_managed_placement: bool,
    pub max_occ_retries: u32,
    pub broker: InMemoryActivityBroker,
    pub delivery_metrics: DeliveryMetrics,
    pub tracking: ActivityTrackingState,
    pub worker_deployment_registry: Arc<RwLock<Option<DeploymentRegistry>>>,
    /// The runtime's lanes, through which a run whose activity write was
    /// refused for a growth limit is terminated.
    pub lanes: Vec<LaneHandle>,
    /// The runtime's clock, which stamps retry and timeout deadlines.
    pub clock: Arc<dyn Clock>,
}

impl<R> Clone for ActivityRetryDeps<R> {
    fn clone(&self) -> Self {
        Self {
            repo: self.repo.clone(),
            shard_owner: self.shard_owner.clone(),
            controller_managed_placement: self.controller_managed_placement,
            max_occ_retries: self.max_occ_retries,
            broker: self.broker.clone(),
            delivery_metrics: self.delivery_metrics.clone(),
            tracking: self.tracking.clone(),
            worker_deployment_registry: self.worker_deployment_registry.clone(),
            lanes: self.lanes.clone(),
            clock: self.clock.clone(),
        }
    }
}

/// Terminate the run through its lane when an activity write's commit was
/// refused for a growth limit that terminates the run, and hand the commit's
/// outcome on, so the writer's caller answers with the breach
/// (`run-growth-limits` criteria 2.4 and 2.5).
pub(crate) async fn terminate_on_growth_breach(
    lanes: &[LaneHandle],
    run_key: RunKey,
    committed: Result<CommitResult>,
    write_admission: &mut Option<crate::serving_gate::WritePermit>,
    clock: &dyn Clock,
) -> Result<CommitResult> {
    let breach = match &committed {
        Err(error) => crate::run_growth::terminating_breach(error, false),
        Ok(_) => None,
    };
    let Some(breach) = breach else {
        return committed;
    };
    // The refused transition wrote nothing. Release its permit before asking
    // the lane for another write; a queued exclusive recovery must not deadlock
    // behind a writer waiting for its own second read permit.
    write_admission.take();
    if lanes.is_empty() {
        tracing::error!(
            ?run_key,
            limit = ?breach.limit,
            "no lane to terminate a run over a growth limit"
        );
        return committed;
    }
    let lane = crate::scanner::pick_lane_for_run_key(lanes, lanes.len(), run_key);
    if let Err(error) = lane
        .submit(
            run_key,
            crate::run_growth::terminate_command(run_key, &breach, clock.now()),
        )
        .await
    {
        tracing::error!(
            ?error,
            ?run_key,
            limit = ?breach.limit,
            "failed to terminate a run over a growth limit"
        );
    }
    committed
}

/// The staleness fence for a retry commit: which activity incarnation the
/// caller decided to retry. The commit revalidates `(expected_attempt,
/// expected_schedule_event_id)` against reloaded durable state so a
/// concurrent resolution or competing retry abandons rather than double-fires.
pub(crate) struct ActivityRetryTarget<'a> {
    pub run_key: RunKey,
    pub activity_id: &'a str,
    pub expected_attempt: u32,
    pub expected_schedule_event_id: i64,
}

/// Commit an activity's next attempt and durable eligibility time.
///
/// Canonical storage maintenance retains the future attempt's row; owner
/// eligibility tracking evaluates rules and offers locally at the deadline; discovery
/// reconstructs offers. The timeout scanner uses the same transition when a
/// fired attempt timeout is retryable (`RetryActivity` →
/// `UpdateActivityInfoForRetries`, mutable_state_impl.go + activity.go:63-97
/// @ v1.31.0). The reload-and-recheck OCC loop guards against a concurrent
/// resolution: if the target no longer matches the current `(attempt,
/// schedule_event_id)` the retry is abandoned with an error, and a commit
/// conflict is retried up to `max_occ_retries`.
pub(crate) async fn commit_activity_retry<R>(
    deps: &ActivityRetryDeps<R>,
    target: ActivityRetryTarget<'_>,
    next_attempt: u32,
    backoff: time::Duration,
    failure: Option<Payload>,
    progress: Option<Payloads>,
) -> Result<()>
where
    R: RunRepository + 'static,
{
    let run_key = target.run_key;
    let activity_id = target.activity_id;
    // The failure is stored cut to the stored activity failure limit, as
    // v1.31.0 stores it, before anything reads it (`run-growth-limits`
    // criterion 2.8).
    let failure = failure.map(|failure| {
        crate::run_growth::stored_activity_failure(
            failure,
            crate::run_growth::stored_activity_failure_limit(),
        )
    });
    let mut attempts = 0u32;
    loop {
        let LoadedRun::Existing(state) = deps.repo.load_run(run_key).await? else {
            return Err(anyhow!("run not found for activity retry"));
        };
        // A closed run never schedules another attempt, whoever asks.
        ensure_run_open(&state)?;
        let Some(current) = state.activities.get(activity_id).cloned() else {
            return Err(anyhow!("activity not found for retry"));
        };
        if current.attempt != target.expected_attempt
            || current.schedule_event_id != target.expected_schedule_event_id
        {
            return Err(anyhow!("stale activity token for retry"));
        }

        let mut next_state = state.clone();
        next_state.transition_seq = state.transition_seq.next();
        let mut next_activity = current.clone();
        next_activity.attempt = next_attempt;
        // A retry is a new incarnation: the preceding start can no longer be
        // replayed even if its queue owner retries an ambiguous response.
        next_state.activity_start_receipts.remove(activity_id);
        next_activity.stamp += 1;
        next_activity.started_at = None;
        next_activity.started_event_id = None;
        // One timestamp per OCC attempt anchors BOTH durable retry-timing
        // fields, so the attempt's backoff is derivable from durable state
        // alone (`current_attempt_scheduled_at - last_attempt_complete_time`)
        // — the way v1.31.0 stamps `LastAttemptCompleteTime` at retry and
        // derives `BackoffInterval` from mutable state
        // (mutable_state_impl.go:6338, matcher/activity_evaluator.go:251 @
        // v1.31.0). The next attempt is scheduled AFTER the retry backoff —
        // v1.31.0 advances ScheduledTime to now+backoff and dispatches on a
        // durable retry timer, not immediately
        // (`UpdateActivityInfoForRetries`, activity.go:74 +
        // GenerateActivityRetryTasks @ v1.31.0).
        let completed_at = deps.clock.now();
        let dispatch_at = deadline_after(completed_at, backoff);
        next_activity.last_attempt_complete_time = Some(completed_at);
        next_activity.current_attempt_scheduled_at = Some(dispatch_at);
        // The failed attempt's failure is durable retry bookkeeping,
        // surfaced by Describe as LastFailure and by the next
        // ActivityTaskStarted's last_failure (`RetryLastFailure`,
        // activity.go:82 @ v1.31.0).
        if let Some(failure) = failure.clone() {
            next_activity.last_failure = Some(failure);
        }
        // `ai.RetryLastWorkerIdentity = ai.StartedIdentity` — the FAILING
        // attempt's starter, not the request identity; `started_identity`
        // itself is deliberately NOT cleared
        // (`UpdateActivityInfoForRetries`, activity.go:81 @ v1.31.0).
        next_activity.retry_last_worker_identity = next_activity.started_identity.clone();
        // A failure that carries heartbeat details records them as a
        // heartbeat does, before the retry applies any pending heartbeat reset
        // (`UpdateActivityProgress` ahead of `RetryActivity`,
        // respondactivitytaskfailed/api.go:87-105 @ v1.31.0).
        if let Some(details) = progress.clone() {
            next_activity.heartbeat_details = Some(details);
            next_activity.last_heartbeat_at = Some(completed_at);
        }
        // Reset-heartbeat applies to the NEXT instance, never destructively to
        // a still-running worker. Retry preparation is the point at which
        // v1.31.0 clears those details and their time, then always consumes
        // both reset flags so a later retry cannot repeat the reset side
        // effects (`UpdateActivityInfoForRetries`, activity.go:63-97 @
        // v1.31.0). Without a reset, the retry keeps both.
        if next_activity.reset_heartbeats {
            next_activity.heartbeat_details = None;
            next_activity.last_heartbeat_at = None;
        }
        next_activity.activity_reset = false;
        next_activity.reset_heartbeats = false;
        if next_activity.pause_info.is_none() {
            let rules = deps.repo.list_workflow_rules(state.namespace_id).await?;
            // The rule evaluator reads the durable interval the new attempt
            // just recorded, so this evaluation and every later re-evaluation
            // (delayed publish, reconciliation, start) see the same value.
            let backoff_interval_seconds = activity_backoff_interval_seconds(&next_activity)?;
            if let Some(rule) = matching_pause_rule(
                &state,
                &next_activity,
                &rules,
                completed_at,
                backoff_interval_seconds,
            ) {
                // Retry preparation knows a next attempt exists, so this is the first retry-rule
                // evaluation point. Terminal failures never enter this function
                // (`mutable_state_impl.go:6274 @ v1.31.0`). Persisting the pause in the same
                // transition prevents any retry dispatch from escaping between the decision and
                // the state change.
                next_activity.pause_info = Some(pause_info_for_rule(rule, completed_at));
            }
        }
        let paused = next_activity.pause_info.is_some();
        // A paused park has no eligibility time; the unpause transition
        // re-anchors it (`RetryActivity`, mutable_state_impl.go:6278-6289 @
        // v1.31.0).
        if paused {
            next_activity.current_attempt_scheduled_at = None;
        }
        next_state
            .activities
            .insert(activity_id.to_string(), next_activity.clone());

        let dispatch_task =
            (!paused).then(|| activity_dispatch_task(run_key, &state, &next_activity));
        let dispatch_ops = match &dispatch_task {
            // A retryable failure advances and clears the running attempt, but
            // a paused activity is parked without a retry timer or dispatch
            // (`RetryActivity`, mutable_state_impl.go:6278-6289 @ v1.31.0).
            None => SmallVec::new(),
            Some(task) => smallvec![DispatchOp::EnqueueActivityTask {
                queue: task.queue.clone(),
                activity_id: task.activity_id.clone(),
                schedule_event_id: task.schedule_event_id,
                attempt: task.attempt,
                dispatch_revision: task.dispatch_revision,
                stamp: task.stamp,
                // The durable eligibility time: storage keeps the row from the
                // commit but the delivery queries surface it only once due —
                // the persisted analog of v1.31.0's retry timer firing time.
                dispatch_at,
                schedule_to_close_timeout: next_activity.schedule_to_close_timeout,
                schedule_to_start_timeout: next_activity.schedule_to_start_timeout,
                start_to_close_timeout: next_activity.start_to_close_timeout,
                heartbeat_timeout: next_activity.heartbeat_timeout,
                priority: task.priority.clone(),
            }],
        };
        let transition = Transition {
            expected_seq: state.transition_seq,
            next_state,
            history_events: SmallVec::new(),
            event_principals: SmallVec::new(),
            request_dedupe_ops: SmallVec::new(),
            activity_ops: smallvec![ActivityOp::Upsert(next_activity.clone())],
            timer_ops: SmallVec::new(),
            dispatch_ops,
            events_numbered_at_close: 0,
            workflow_task_heartbeat_timed_out: false,
            growth_limits: Some(crate::run_growth::run_growth_limits()),
        };

        // Same fence as start_activity_task (`bundle-lease-fence`).
        let (bundle, fence, acquisition) = {
            let owner = deps.shard_owner.read().expect("shard_owner lock poisoned");
            let bundle_id = execution_home_bundle(
                state.namespace_id.0.as_bytes(),
                state.workflow_id.0.as_bytes(),
                owner.shard_count(),
            );
            let fence = owner
                .write_fence(bundle_id)
                .ok_or_else(|| crate::errors::NotShardOwner::local(bundle_id, ShardEpoch::ZERO))?;
            (
                bundle_id,
                fence,
                owner
                    .acquisition(bundle_id)
                    .expect("fenced activity retry has an acquisition"),
            )
        };

        let mut write_admission = crate::serving_gate::admit(&deps.shard_owner, bundle).await?;
        if let Some(captured) = deps.tracking.inner.captured_acquisition()
            && captured.generation != acquisition.generation
        {
            return Err(
                crate::errors::NotShardOwner::local(captured.shard_id, captured.epoch).into(),
            );
        }
        crate::outbound::driver::Attempt {
            acquisition: acquisition.clone(),
            fence: fence.clone(),
        }
        .validate(&deps.shard_owner)?;
        let committed = deps
            .repo
            .commit_transition_for_bundle(run_key, bundle, transition, fence)
            .await
            .map_err(|error| crate::errors::lease_loss(error, &deps.shard_owner));
        match terminate_on_growth_breach(
            &deps.lanes,
            run_key,
            committed,
            &mut write_admission,
            deps.clock.as_ref(),
        )
        .await?
        {
            CommitResult::Applied { new_state } => {
                let tracking = deps.tracking.for_acquisition(acquisition);
                tracking.eligibility.refresh(&new_state, bundle);
                if paused {
                    tracking.remove(run_key, activity_id);
                    return Ok(());
                }
                tracking.record_retry_with_timeouts(
                    run_key,
                    activity_id,
                    dispatch_at,
                    new_state
                        .activities
                        .get(activity_id)
                        .map(crate::activity_timeout::ActivityTimeouts::from),
                );
                // The committed canonical row is discoverable independently of
                // this callback; eligibility tracking applies later owner rules.
                return Ok(());
            }
            CommitResult::Conflict { .. } | CommitResult::CurrentExecutionConflict { .. } => {
                if attempts >= deps.max_occ_retries {
                    return Err(anyhow!("activity retry OCC exhausted"));
                }
                attempts += 1;
            }
            CommitResult::Duplicate => return Ok(()),
        }
    }
}

/// Build the dispatchable envelope for an activity's current unstarted
/// attempt exactly as the retry commit publishes it: queue coordinates from
/// the activity with workflow-level fallback, the workflow's routing
/// revision, the live stamp, the field-wise merged Priority, and no runtime
/// delivery order. Shared by the retry commit and durable reconciliation so
/// envelope construction cannot diverge between them.
pub(crate) fn activity_dispatch_task(
    run_key: RunKey,
    state: &WorkflowState,
    activity: &tokeira_kernel::ActivityState,
) -> DispatchableActivityTask {
    DispatchableActivityTask {
        run_key,
        queue: QueueKey {
            namespace_id: state.namespace_id,
            task_queue: activity.task_queue.clone(),
            task_kind: tokeira_types::TaskKind::Activity,
            deployment: activity
                .deployment
                .clone()
                .or_else(|| state.deployment.clone()),
            build_id: activity.build_id.clone().or_else(|| state.build_id.clone()),
        },
        activity_id: activity.activity_id.clone(),
        schedule_event_id: activity.schedule_event_id,
        attempt: activity.attempt,
        dispatch_revision: state
            .versioning_info
            .as_ref()
            .map(|info| info.revision_number)
            .unwrap_or_default(),
        stamp: activity.stamp,
        priority: merge_priority(state.priority.as_ref(), activity.priority.as_ref()),
        order: None,
    }
}

/// Execution-owner eligibility outcome; eligible due attempts are offered locally.
#[derive(Debug)]
pub(crate) enum ActivityDispatchPreparation {
    /// The due attempt passed owner rule evaluation.
    Eligible,
    /// A workflow rule paused the activity. The pause transition committed
    /// (its activity upsert deletes the durable dispatch row) and nothing
    /// publishes.
    SuppressedByRule,
    /// Authoritative state proves the observed dispatch identity can never
    /// become publishable again: the run is absent or not running, the
    /// activity is absent, paused, or already started, or its schedule
    /// event / attempt / stamp / eligibility no longer match. These are
    /// row-lifecycle invariant violations — the observer may conditionally
    /// prune the exact observed row version.
    PermanentlyStale,
    /// The dispatch is durable and live but must not publish now: its
    /// eligibility time has not arrived, or a fired schedule deadline is
    /// owned by timeout processing.
    NotYetDispatchable,
}

/// The attempt's backoff interval, derived from durable state alone.
///
/// `ScheduledTime - LastAttemptCompleteTime`, exactly how v1.31.0 evaluates
/// `BackoffInterval` rules from mutable state; the first attempt is a
/// recognized-but-absent value (every `BackoffInterval` predicate is a clean
/// non-match, `matchActivityBackoffInterval` returns `(false, nil)` before
/// reading `LastAttemptCompleteTime`, matcher/activity_evaluator.go:245-253
/// @ v1.31.0). NEVER substitute zero for `None`: `BackoffInterval = 0` would
/// then incorrectly match. For attempts after the first, a missing durable
/// timestamp is an invariant failure that prevents publication rather than a
/// reason to skip the rule.
pub(crate) fn activity_backoff_interval_seconds(
    activity: &tokeira_kernel::ActivityState,
) -> Result<Option<i64>> {
    if activity.attempt < 2 {
        return Ok(None);
    }
    let scheduled_at = activity
        .current_attempt_scheduled_at
        .ok_or_else(|| anyhow!("retry attempt is missing its scheduled time"))?;
    let completed_at = activity
        .last_attempt_complete_time
        .ok_or_else(|| anyhow!("retry attempt is missing its prior completion time"))?;
    Ok(Some((scheduled_at - completed_at).whole_seconds()))
}

/// Evaluate the captured execution-owner timer and offer eligible work locally.
/// A pause uses the captured fence and publishes nothing. Queue homes on another
/// node still discover the durable attempt on their next pass, as for first starts.
pub(crate) async fn evaluate_activity_eligibility<R: RunRepository + 'static>(
    deps: &ActivityRetryDeps<R>,
    task: &DispatchableActivityTask,
    now: OffsetDateTime,
    attempt: &crate::outbound::driver::Attempt,
) -> Result<bool> {
    Ok(!matches!(
        prepare_activity_dispatch(deps, task, now, attempt).await?,
        ActivityDispatchPreparation::NotYetDispatchable
    ))
}

async fn prepare_activity_dispatch<R: RunRepository + 'static>(
    deps: &ActivityRetryDeps<R>,
    task: &DispatchableActivityTask,
    now: OffsetDateTime,
    captured: &crate::outbound::driver::Attempt,
) -> Result<ActivityDispatchPreparation> {
    use ActivityDispatchPreparation as Preparation;
    let mut attempts = 0u32;
    loop {
        captured.validate(&deps.shard_owner)?;
        let LoadedRun::Existing(state) = deps.repo.load_run(task.run_key).await? else {
            return Ok(Preparation::PermanentlyStale);
        };
        {
            captured.validate(&deps.shard_owner)?;
            let home = execution_home_bundle(
                state.namespace_id.0.as_bytes(),
                state.workflow_id.0.as_bytes(),
                deps.shard_owner
                    .read()
                    .expect("shard owner lock poisoned")
                    .shard_count(),
            );
            anyhow::ensure!(
                home == captured.acquisition.shard_id,
                "activity eligibility run belongs to another execution home"
            );
        }
        // Exactly `Running`: `is_open()` also admits `Paused`, and workflow
        // pause deletes the run's dispatch rows (resume recreates them), so a
        // row observed against a paused workflow is lifecycle-stale.
        if state.status != ExecutionStatus::Running {
            return Ok(Preparation::PermanentlyStale);
        }
        let Some(current) = state.activities.get(&task.activity_id).cloned() else {
            return Ok(Preparation::PermanentlyStale);
        };
        if current.attempt != task.attempt
            || current.schedule_event_id != task.schedule_event_id
            || current.stamp != task.stamp
            || current.started_event_id.is_some()
            || current.pause_info.is_some()
        {
            return Ok(Preparation::PermanentlyStale);
        }
        let Some(dispatch_at) = current.current_attempt_scheduled_at else {
            // Unstarted, unpaused attempts always carry an eligibility time;
            // its absence means the row's attempt was superseded mid-read.
            return Ok(Preparation::PermanentlyStale);
        };
        if dispatch_at > now {
            return Ok(Preparation::NotYetDispatchable);
        }
        // Fired schedule deadlines belong to timeout processing; republishing
        // would race the resolution that must terminally close the attempt.
        if let Some(timeout) = current.schedule_to_close_timeout
            && !timeout.is_zero()
            && deadline_after(current.scheduled_at, timeout) <= now
        {
            return Ok(Preparation::NotYetDispatchable);
        }
        if let Some(timeout) = current.schedule_to_start_timeout
            && !timeout.is_zero()
            && deadline_after(dispatch_at, timeout) <= now
        {
            return Ok(Preparation::NotYetDispatchable);
        }

        let rules = deps.repo.list_workflow_rules(state.namespace_id).await?;
        captured.validate(&deps.shard_owner)?;
        let backoff_interval_seconds = activity_backoff_interval_seconds(&current)?;
        let Some(rule) =
            matching_pause_rule(&state, &current, &rules, now, backoff_interval_seconds)
        else {
            if dispatch_at > deps.clock.now() {
                return Ok(Preparation::NotYetDispatchable);
            }
            let registry = deps
                .worker_deployment_registry
                .read()
                .expect("worker deployment registry lock poisoned")
                .clone();
            let mut offer = activity_dispatch_task(task.run_key, &state, &current);
            (offer.queue, offer.dispatch_revision) =
                super::workflow_task::route_activity_task_queue(
                    registry.as_ref(),
                    &state,
                    offer.queue,
                    offer.dispatch_revision,
                )
                .await?;
            captured.validate(&deps.shard_owner)?;
            // The retry timer supplies matching at its due time
            // (service/history/workflow/task_generator.go:567-577 @ v1.31.0).
            // Discovery remains the durable backstop; broker deduplication
            // tolerates either path arriving first.
            deps.broker
                .publish_activity_task(offer, Some(&deps.delivery_metrics))
                .await?;
            return Ok(Preparation::Eligible);
        };

        let mut next_state = state.clone();
        next_state.transition_seq = state.transition_seq.next();
        let mut next_activity = current;
        next_activity.stamp += 1;
        next_activity.current_attempt_scheduled_at = None;
        next_activity.pause_info = Some(pause_info_for_rule(rule, now));
        next_state
            .activities
            .insert(task.activity_id.clone(), next_activity.clone());
        let transition = Transition {
            expected_seq: state.transition_seq,
            next_state,
            history_events: SmallVec::new(),
            event_principals: SmallVec::new(),
            request_dedupe_ops: SmallVec::new(),
            activity_ops: smallvec![ActivityOp::Upsert(next_activity)],
            timer_ops: SmallVec::new(),
            dispatch_ops: SmallVec::new(),
            events_numbered_at_close: 0,
            workflow_task_heartbeat_timed_out: false,
            growth_limits: Some(crate::run_growth::run_growth_limits()),
        };
        captured.validate(&deps.shard_owner)?;
        let bundle = captured.acquisition.shard_id;
        let fence = captured.fence.clone();

        let mut write_admission = crate::serving_gate::admit(&deps.shard_owner, bundle).await?;
        captured.validate(&deps.shard_owner)?;
        let committed = deps
            .repo
            .commit_transition_for_bundle(task.run_key, bundle, transition, fence)
            .await
            .map_err(|error| crate::errors::lease_loss(error, &deps.shard_owner));
        match terminate_on_growth_breach(
            &deps.lanes,
            task.run_key,
            committed,
            &mut write_admission,
            deps.clock.as_ref(),
        )
        .await?
        {
            CommitResult::Applied { .. } => {
                // Eligibility completion retires its submitted revision in the
                // scanner. An older pause reply must not remove tracking that
                // a concurrent unpause installed while this commit was pending.
                return Ok(Preparation::SuppressedByRule);
            }
            CommitResult::Conflict { .. } | CommitResult::CurrentExecutionConflict { .. } => {
                if attempts >= deps.max_occ_retries {
                    return Err(anyhow!("workflow-rule retry-timer pause OCC exhausted"));
                }
                attempts += 1;
            }
            CommitResult::Duplicate => return Ok(Preparation::SuppressedByRule),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashSet},
        sync::Arc,
    };
    use tokeira_storage::WriteFence;

    use proptest::prelude::*;
    use time::{Duration, OffsetDateTime};
    use tokeira_kernel::{
        ContinueAsNewVersioningBehavior, VersioningBehavior, VersioningOverride,
        WorkerDeploymentVersionRef, WorkflowVersioningInfo,
    };
    use tokeira_storage::{
        BuildId as StoredBuildId, DeploymentName, DeploymentTaskQueueType, InMemoryStore,
        WorkflowRuleCreateResult,
    };
    use tokeira_types::{
        BuildId as RuntimeBuildId, DeploymentId, LogicalTaskSeq, Memo, NamespaceId, Payload,
        RequestId, SearchAttributes, TaskKind, TaskQueueName, WorkflowId, WorkflowRuleAction,
        WorkflowRuleRecord, WorkflowRuleTrigger, WorkflowType,
    };

    use crate::{RegisterPolledDeployment, SetCurrent};

    use super::*;

    #[derive(Clone, Debug)]
    enum ActivityTransitionCase {
        DeploymentEquality,
        NameAheadRevision,
        EqualRevision,
        Pinned,
        UnversionedPoller,
        InFlightTransition,
    }

    fn arb_activity_transition_case() -> impl Strategy<Value = ActivityTransitionCase> {
        prop_oneof![
            Just(ActivityTransitionCase::DeploymentEquality),
            Just(ActivityTransitionCase::NameAheadRevision),
            Just(ActivityTransitionCase::EqualRevision),
            Just(ActivityTransitionCase::Pinned),
            Just(ActivityTransitionCase::UnversionedPoller),
            Just(ActivityTransitionCase::InFlightTransition),
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

    fn activity_queue(deployment_name: Option<&str>, build_id: Option<&str>) -> QueueKey {
        QueueKey {
            namespace_id: NamespaceId::new(),
            task_queue: TaskQueueName("activity-task-queue".into()),
            task_kind: TaskKind::Activity,
            deployment: deployment_name.map(|name| DeploymentId(name.to_string())),
            build_id: build_id.map(|id| RuntimeBuildId(id.to_string())),
        }
    }

    fn wft_target(
        deployment_name: Option<&str>,
        build_id: Option<&str>,
        revision_number: i64,
        pinned: bool,
    ) -> ResolvedWorkflowTaskTarget {
        ResolvedWorkflowTaskTarget {
            deployment_version: deployment_name
                .zip(build_id)
                .map(|(deployment_name, build_id)| version_ref(deployment_name, build_id)),
            revision_number,
            pinned,
        }
    }

    fn open_state(info: Option<WorkflowVersioningInfo>) -> WorkflowState {
        WorkflowState {
            activity_start_receipts: std::collections::BTreeMap::new(),
            used_worker_deployment_versions: Some(Vec::new()),
            completed_update_count: 0,
            signal_count: 0,
            outbound: Some(Default::default()),
            run_key: RunKey::new(),
            namespace_id: NamespaceId::new(),
            workflow_id: WorkflowId("workflow".into()),
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
            external_payload_count: 0,
            external_payload_size_bytes: 0,
            next_workflow_task_seq: LogicalTaskSeq(1),
            pending_workflow_task: None,
            previous_started_event_id: 0,
            workflow_task_attempt: 1,
            workflow_task_attempts_since_last_success: 0,
            last_workflow_task_problem: None,
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
            reset_run_id: None,
            parent_run_key: None,
            parent_workflow_id: None,
            parent_run_id: None,
            parent_namespace_id: None,
            parent_namespace_name: None,
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
            history_admitted_updates: Default::default(),
            completed_updates: Default::default(),
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
            auto_reset_points: Vec::new(),
        }
    }

    fn auto_upgrade_state(effective_build: &str) -> WorkflowState {
        open_state(Some(WorkflowVersioningInfo {
            behavior: VersioningBehavior::AutoUpgrade,
            deployment_version: Some(version_ref("deployment", effective_build)),
            versioning_override: None,
            version_transition: None,
            revision_number: 10,
            continue_as_new_initial_versioning_behavior:
                ContinueAsNewVersioningBehavior::Unspecified,
            ..WorkflowVersioningInfo::default()
        }))
    }

    fn pinned_state() -> WorkflowState {
        open_state(Some(WorkflowVersioningInfo {
            behavior: VersioningBehavior::Pinned,
            deployment_version: Some(version_ref("deployment", "pinned")),
            versioning_override: None,
            version_transition: None,
            revision_number: 10,
            continue_as_new_initial_versioning_behavior:
                ContinueAsNewVersioningBehavior::Unspecified,
            ..WorkflowVersioningInfo::default()
        }))
    }

    fn transitioning_state() -> WorkflowState {
        open_state(Some(WorkflowVersioningInfo {
            behavior: VersioningBehavior::AutoUpgrade,
            deployment_version: Some(version_ref("deployment", "current")),
            versioning_override: Some(VersioningOverride::AutoUpgrade),
            version_transition: Some(version_ref("deployment", "transition")),
            revision_number: 10,
            continue_as_new_initial_versioning_behavior:
                ContinueAsNewVersioningBehavior::Unspecified,
            ..WorkflowVersioningInfo::default()
        }))
    }

    fn payloads(bytes: &[u8]) -> Payloads {
        Payloads(vec![Payload {
            data: bytes.to_vec(),
            metadata: BTreeMap::new(),
            external_payloads: Vec::new(),
        }])
    }

    fn payload(bytes: &[u8]) -> Payload {
        Payload {
            data: bytes.to_vec(),
            metadata: BTreeMap::new(),
            external_payloads: Vec::new(),
        }
    }

    fn retry_policy() -> RetryPolicy {
        RetryPolicy {
            initial_interval: Duration::seconds(1),
            backoff_coefficient: 2.0,
            maximum_interval: Some(Duration::seconds(10)),
            maximum_attempts: 3,
            non_retryable_error_types: Vec::new(),
        }
    }

    async fn register_activity_version(
        registry: &DeploymentRegistry,
        namespace_id: NamespaceId,
        deployment_name: &str,
        build_id: &str,
        task_queue: &str,
    ) {
        registry
            .register_polled_deployment(RegisterPolledDeployment {
                namespace_id,
                deployment_name: DeploymentName(deployment_name.to_string()),
                build_id: StoredBuildId(build_id.to_string()),
                task_queue: task_queue.to_string(),
                task_queue_type: DeploymentTaskQueueType::Activity,
                identity: format!("worker-{build_id}"),
            })
            .await
            .expect("register activity worker deployment version");
    }

    async fn set_current_activity_version(
        registry: &DeploymentRegistry,
        namespace_id: NamespaceId,
        deployment_name: &str,
        build_id: &str,
    ) {
        registry
            .set_current_version(SetCurrent {
                namespace_id,
                deployment_name: DeploymentName(deployment_name.to_string()),
                build_id: Some(StoredBuildId(build_id.to_string())),
                conflict_token: None,
                identity: "operator".to_string(),
                allow_no_pollers: false,
                ignore_missing_task_queues: true,
            })
            .await
            .expect("set current activity worker deployment version");
    }

    /// The ActivityTaskScheduled event a seeded activity's start reads its input
    /// and header from.
    fn scheduled_event(activity: &tokeira_kernel::ActivityState) -> tokeira_kernel::HistoryEvent {
        tokeira_kernel::HistoryEvent {
            event_id: activity.schedule_event_id,
            happened_at: activity.scheduled_at,
            kind: HistoryEventKind::ActivityTaskScheduled {
                workflow_task_completed_event_id: activity.schedule_event_id - 1,
                activity_id: activity.activity_id.clone(),
                activity_type: activity.activity_type.clone(),
                task_queue: activity.task_queue.clone(),
                input: payloads(b"input"),
                header: None,
                retry_policy: activity.retry_policy.clone(),
                schedule_to_close_timeout: activity.schedule_to_close_timeout,
                schedule_to_start_timeout: activity.schedule_to_start_timeout,
                start_to_close_timeout: activity.start_to_close_timeout,
                heartbeat_timeout: activity.heartbeat_timeout,
                priority: activity.priority.clone(),
            },
        }
    }

    async fn seed_started_activity(
        runtime: &TokeiraRuntime<InMemoryStore>,
        repo: &InMemoryStore,
        details: Option<Payloads>,
    ) -> (WorkflowState, ActivityTaskToken, QueueKey) {
        let mut state = open_state(None);
        let count = runtime.shard_owner.read().unwrap().shard_count();
        if count > 1 {
            let home = runtime.execution_home(&state);
            state.run_key = RunKey(uuid::Uuid::from_u128(u128::from((home.0 + 1) % count)));
        }
        let run_key = state.run_key;
        // Anchor at real time: the retry path measures the schedule-to-close
        // retry-expiration against `OffsetDateTime::now_utc()` (retry.go:108-110
        // @ v1.31.0), so a fixed-epoch anchor would classify every retry as
        // expired.
        let scheduled_at = OffsetDateTime::now_utc();
        let started_at = scheduled_at + Duration::seconds(1);
        let activity = tokeira_kernel::ActivityState {
            last_attempt_complete_time: None,
            cancel_requested: false,
            activity_reset: false,
            reset_heartbeats: false,
            started_identity: None,
            retry_last_worker_identity: None,
            activity_id: "activity-1".to_string(),
            activity_type: "activity-type".to_string(),
            schedule_event_id: 7,
            task_queue: TaskQueueName("activity-q".to_string()),
            deployment: None,
            build_id: None,
            retired_input: Default::default(),
            retired_header: Default::default(),
            attempt: 1,
            retry_policy: Some(retry_policy()),
            schedule_to_close_timeout: Some(Duration::minutes(5)),
            schedule_to_start_timeout: Some(Duration::seconds(30)),
            start_to_close_timeout: Some(Duration::minutes(1)),
            heartbeat_timeout: Some(Duration::seconds(10)),
            scheduled_at,
            current_attempt_scheduled_at: Some(scheduled_at),
            started_at: Some(started_at),
            started_event_id: Some(8),
            last_failure: None,
            heartbeat_details: details,
            pause_info: None,
            stamp: 0,
            priority: None,
            last_heartbeat_at: None,
        };
        state
            .activities
            .insert(activity.activity_id.clone(), activity.clone());
        let queue = QueueKey {
            namespace_id: state.namespace_id,
            task_queue: activity.task_queue.clone(),
            task_kind: TaskKind::Activity,
            deployment: None,
            build_id: None,
        };
        let transition = Transition {
            expected_seq: TransitionSeq::ZERO,
            next_state: state.clone(),
            history_events: smallvec![scheduled_event(&activity)],
            event_principals: smallvec![None],
            request_dedupe_ops: SmallVec::new(),
            activity_ops: smallvec![ActivityOp::Upsert(activity.clone())],
            timer_ops: SmallVec::new(),
            dispatch_ops: SmallVec::new(),
            events_numbered_at_close: 0,
            workflow_task_heartbeat_timed_out: false,
            growth_limits: None,
        };
        repo.commit_transition(run_key, transition, WriteFence::Unfenced)
            .await
            .expect("seed activity state");
        let token = ActivityTaskToken {
            run_key,
            activity_id: activity.activity_id,
            schedule_event_id: activity.schedule_event_id,
            attempt: activity.attempt,
            shard_epoch: runtime
                .shard_epoch_for_completion(&state)
                .await
                .expect("runtime owns seeded run"),
        };
        (state, token, queue)
    }

    fn pause_rule(id: &str, predicate: &str) -> WorkflowRuleRecord {
        WorkflowRuleRecord {
            id: id.to_string(),
            create_time: now(),
            created_by_identity: "rule-owner".to_string(),
            description: "policy pause".to_string(),
            trigger: WorkflowRuleTrigger::ActivityStart {
                predicate: predicate.to_string(),
            },
            visibility_query: String::new(),
            actions: vec![WorkflowRuleAction::ActivityPause],
            expiration_time: None,
        }
    }

    async fn seed_scheduled_activity(
        runtime: &TokeiraRuntime<InMemoryStore>,
        repo: &InMemoryStore,
    ) -> (WorkflowState, QueueKey, DispatchableActivityTask) {
        seed_scheduled_activity_with_versioning(runtime, repo, None).await
    }

    async fn seed_scheduled_activity_with_versioning(
        runtime: &TokeiraRuntime<InMemoryStore>,
        repo: &InMemoryStore,
        versioning_info: Option<WorkflowVersioningInfo>,
    ) -> (WorkflowState, QueueKey, DispatchableActivityTask) {
        seed_scheduled(runtime, repo, versioning_info, true).await
    }

    /// A scheduled activity whose run's history lacks its ActivityTaskScheduled
    /// event, so any read of the event fails: a start that answers without
    /// reading it shows it read nothing.
    async fn seed_scheduled_activity_lacking_its_event(
        runtime: &TokeiraRuntime<InMemoryStore>,
        repo: &InMemoryStore,
    ) -> (WorkflowState, QueueKey, DispatchableActivityTask) {
        seed_scheduled(runtime, repo, None, false).await
    }

    async fn seed_scheduled(
        runtime: &TokeiraRuntime<InMemoryStore>,
        repo: &InMemoryStore,
        versioning_info: Option<WorkflowVersioningInfo>,
        with_event: bool,
    ) -> (WorkflowState, QueueKey, DispatchableActivityTask) {
        let mut state = open_state(versioning_info);
        let run_key = state.run_key;
        let scheduled_at = OffsetDateTime::now_utc();
        let activity = tokeira_kernel::ActivityState {
            last_attempt_complete_time: None,
            cancel_requested: false,
            activity_reset: false,
            reset_heartbeats: false,
            started_identity: None,
            retry_last_worker_identity: None,
            activity_id: "activity-1".to_string(),
            activity_type: "activity-type".to_string(),
            schedule_event_id: 7,
            task_queue: TaskQueueName("activity-q".to_string()),
            deployment: None,
            build_id: None,
            retired_input: Default::default(),
            retired_header: Default::default(),
            attempt: 1,
            retry_policy: Some(retry_policy()),
            schedule_to_close_timeout: Some(Duration::minutes(5)),
            schedule_to_start_timeout: Some(Duration::seconds(30)),
            start_to_close_timeout: Some(Duration::minutes(1)),
            heartbeat_timeout: Some(Duration::seconds(10)),
            scheduled_at,
            current_attempt_scheduled_at: Some(scheduled_at),
            started_at: None,
            started_event_id: None,
            last_failure: None,
            heartbeat_details: None,
            pause_info: None,
            stamp: 0,
            priority: None,
            last_heartbeat_at: None,
        };
        state
            .activities
            .insert(activity.activity_id.clone(), activity.clone());
        let queue = QueueKey {
            namespace_id: state.namespace_id,
            task_queue: activity.task_queue.clone(),
            task_kind: TaskKind::Activity,
            deployment: None,
            build_id: None,
        };
        let task = DispatchableActivityTask {
            run_key,
            queue: queue.clone(),
            activity_id: activity.activity_id.clone(),
            schedule_event_id: activity.schedule_event_id,
            attempt: activity.attempt,
            dispatch_revision: 0,
            stamp: activity.stamp,
            priority: None,
            order: None,
        };
        let (history_events, event_principals) = if with_event {
            (smallvec![scheduled_event(&activity)], smallvec![None])
        } else {
            (SmallVec::new(), SmallVec::new())
        };
        let transition = Transition {
            expected_seq: TransitionSeq::ZERO,
            next_state: state.clone(),
            history_events,
            event_principals,
            request_dedupe_ops: SmallVec::new(),
            activity_ops: smallvec![ActivityOp::Upsert(activity.clone())],
            timer_ops: SmallVec::new(),
            // Real schedule transitions always carry the durable dispatch op;
            // the row is what reconciliation rediscovers after a lost take.
            dispatch_ops: smallvec![DispatchOp::EnqueueActivityTask {
                queue: queue.clone(),
                activity_id: activity.activity_id.clone(),
                schedule_event_id: activity.schedule_event_id,
                attempt: activity.attempt,
                dispatch_revision: 0,
                stamp: activity.stamp,
                dispatch_at: scheduled_at,
                schedule_to_close_timeout: activity.schedule_to_close_timeout,
                schedule_to_start_timeout: activity.schedule_to_start_timeout,
                start_to_close_timeout: activity.start_to_close_timeout,
                heartbeat_timeout: activity.heartbeat_timeout,
                priority: None,
            }],
            events_numbered_at_close: 0,
            workflow_task_heartbeat_timed_out: false,
            growth_limits: None,
        };
        repo.commit_transition(run_key, transition, WriteFence::Unfenced)
            .await
            .expect("seed scheduled activity state");
        runtime
            .activity_broker
            .publish_activity_task(task.clone(), Some(&runtime.delivery_metrics))
            .await
            .expect("publish scheduled activity");
        (state, queue, task)
    }

    #[tokio::test]
    async fn late_pinned_activity_poller_reprocesses_unversioned_ready_task() {
        let repo = Arc::new(InMemoryStore::default());
        let runtime = TokeiraRuntime::new(
            repo.clone(),
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
        )
        .with_worker_deployment_repository(repo.clone());
        let pinned = pinned_state().versioning_info;
        let (state, source, task) =
            seed_scheduled_activity_with_versioning(&runtime, &repo, pinned).await;
        let registry = runtime
            .deployment_registry()
            .expect("deployment registry configured");

        // The activity was published before this membership existed, so its
        // disposable queue coordinate is still unversioned at this point.
        register_activity_version(
            &registry,
            state.namespace_id,
            "deployment",
            "pinned",
            &source.task_queue.0,
        )
        .await;
        let target = QueueKey {
            deployment: Some(DeploymentId("deployment".to_string())),
            build_id: Some(RuntimeBuildId("pinned".to_string())),
            ..source.clone()
        };
        runtime
            .reprocess_unversioned_activity_backlog(&target)
            .await
            .expect("reprocess late activity membership");

        assert_eq!(
            runtime.activity_broker.backlog_stats(&source).await.count,
            0
        );
        assert_eq!(
            runtime.activity_broker.backlog_stats(&target).await.count,
            1
        );
        let offered = runtime
            .activity_broker
            .poll_activity_task(&target, tokio::time::Duration::from_millis(1))
            .await
            .expect("poll rerouted activity")
            .expect("rerouted activity available")
            .0;
        assert_eq!(offered.run_key, task.run_key);
        assert_eq!(offered.activity_id, task.activity_id);
    }

    #[tokio::test]
    async fn auto_upgrade_activity_retry_rederives_current_version_at_publish_time() {
        let repo = Arc::new(InMemoryStore::default());
        let runtime = TokeiraRuntime::new(
            repo.clone(),
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
        )
        .with_worker_deployment_repository(repo);
        let registry = runtime
            .deployment_registry()
            .expect("deployment registry configured");
        let state = auto_upgrade_state("v1");
        let namespace_id = state.namespace_id;
        for build_id in ["v1", "v2"] {
            register_activity_version(
                &registry,
                namespace_id,
                "deployment",
                build_id,
                "activity-task-queue",
            )
            .await;
        }
        set_current_activity_version(&registry, namespace_id, "deployment", "v1").await;

        let mut original_queue = activity_queue(Some("deployment"), Some("v1"));
        original_queue.namespace_id = namespace_id;
        let (before, before_revision) =
            route_activity_task_queue(Some(&registry), &state, original_queue.clone(), 10)
                .await
                .expect("route activity before deployment change");
        assert_eq!(
            before.deployment.as_ref().map(|value| value.0.as_str()),
            Some("deployment")
        );
        assert_eq!(
            before.build_id.as_ref().map(|value| value.0.as_str()),
            Some("v1")
        );

        set_current_activity_version(&registry, namespace_id, "deployment", "v2").await;
        let (after, after_revision) =
            route_activity_task_queue(Some(&registry), &state, original_queue, before_revision)
                .await
                .expect("route activity retry after deployment change");
        assert_eq!(
            after.deployment.as_ref().map(|value| value.0.as_str()),
            Some("deployment")
        );
        assert_eq!(
            after.build_id.as_ref().map(|value| value.0.as_str()),
            Some("v2")
        );
        assert!(after_revision > before_revision);
    }

    #[tokio::test]
    async fn unversioned_run_uses_activity_poller_family_to_find_workflow_current() {
        let repo = Arc::new(InMemoryStore::default());
        let runtime = TokeiraRuntime::new(
            repo.clone(),
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
        )
        .with_worker_deployment_repository(repo.clone());
        let (state, source, mut task) = seed_scheduled_activity(&runtime, &repo).await;
        let registry = runtime
            .deployment_registry()
            .expect("deployment registry configured");
        register_activity_version(
            &registry,
            state.namespace_id,
            "deployment",
            "current",
            &source.task_queue.0,
        )
        .await;
        set_current_activity_version(&registry, state.namespace_id, "deployment", "current").await;
        task.queue.deployment = Some(DeploymentId("deployment".to_string()));
        task.queue.build_id = Some(RuntimeBuildId("current".to_string()));

        let started = runtime
            .start_activity_task(
                &task,
                Uuid::new_v4(),
                tokio::time::Instant::now(),
                &WorkerIdentity("activity-worker".to_string()),
            )
            .await
            .expect("activity poll should be evaluated");
        assert!(
            started.is_none(),
            "the activity is withheld while the workflow transitions"
        );

        let LoadedRun::Existing(after) = repo.load_run(task.run_key).await.unwrap() else {
            panic!("seeded run should exist");
        };
        assert_eq!(
            after
                .versioning_info
                .as_ref()
                .and_then(|info| info.version_transition.as_ref()),
            Some(&version_ref("deployment", "current"))
        );
        assert!(after.pending_workflow_task.is_some());
        assert!(
            after.activities[&task.activity_id]
                .started_event_id
                .is_none()
        );
    }

    #[tokio::test]
    async fn repair_defers_overdue_timers_and_activity_pause_rules_until_active() {
        for interrupted in [false, true] {
            let repo = Arc::new(InMemoryStore::default());
            let runtime = Arc::new(TokeiraRuntime::new(
                repo.clone(),
                1,
                LaneConfig::default(),
                TimerScannerConfig {
                    scan_interval: std::time::Duration::from_secs(86400),
                    ..Default::default()
                },
                WorkflowTimeoutScannerConfig::default(),
            ));
            let (state, _, task) = seed_scheduled_activity(&runtime, &repo).await;
            runtime.activity_broker.remove_run(state.run_key).await;
            repo.create_workflow_rule(
                state.namespace_id,
                pause_rule("repair-pause", "ActivityType = 'activity-type'"),
                10,
            )
            .await
            .unwrap();
            let mut transition = tokeira_kernel::Kernel::apply(
                &BasicKernel,
                LoadedRun::Existing(state.clone()),
                Command::Signal(tokeira_kernel::SignalRequest {
                    signal_name: "timer-fixture".into(),
                    input: Payloads::default(),
                    header: None,
                    links: vec![],
                    request: tokeira_types::RequestContext {
                        request_id: RequestId("repair-test-request".into()),
                        caller_identity: None,
                        principal: None,
                        received_at: OffsetDateTime::now_utc(),
                    },
                    now: OffsetDateTime::now_utc(),
                }),
            )
            .unwrap();
            let timer = tokeira_kernel::TimerState {
                timer_id: "overdue-repair".into(),
                started_event_id: 1,
                fire_at: OffsetDateTime::now_utc() - Duration::seconds(1),
            };
            transition
                .next_state
                .timers
                .insert(timer.timer_id.clone(), timer.clone());
            transition
                .timer_ops
                .push(tokeira_kernel::TimerOp::Upsert(timer.clone()));
            repo.commit_transition(state.run_key, transition, WriteFence::Unfenced)
                .await
                .unwrap();
            let before = repo.snapshot().await.unwrap();
            let (entered, resume) = runtime.wft_timeout_tracking.pause_next_recovery();
            let acquiring = runtime.clone();
            let recovery = tokio::spawn(async move {
                acquiring
                    .recover_self_assigned_shard(ShardId(0), ShardEpoch::ZERO)
                    .await
            });
            entered.await.unwrap();
            assert!(runtime.active_shards().is_empty());
            crate::activity_eligibility::scan_activity_eligibility_once(
                &runtime.activity_retry_deps(),
                ShardId(0),
                OffsetDateTime::now_utc(),
            )
            .await;
            assert_eq!(repo.snapshot().await.unwrap(), before);
            if interrupted {
                drop(resume);
                assert!(recovery.await.unwrap().is_err());
                assert!(runtime.active_shards().is_empty());
                assert_eq!(repo.snapshot().await.unwrap(), before);
                runtime
                    .recover_self_assigned_shard(ShardId(0), ShardEpoch::ZERO)
                    .await
                    .unwrap();
            } else {
                resume.send(()).unwrap();
                let result = recovery.await.unwrap().unwrap();
                assert_eq!(result.due_timers_injected, 0);
                assert_eq!(result.activity_tasks_republished, 0);
            }
            assert_eq!(repo.snapshot().await.unwrap(), before);
            let due = repo
                .list_due_timers_for_shard(ShardId(0), OffsetDateTime::now_utc(), None, 64)
                .await
                .unwrap();
            assert_eq!(due.len(), 1);
            runtime.lanes[0]
                .submit(
                    state.run_key,
                    crate::scanner::command_for_due_timer(
                        due[0].clone(),
                        OffsetDateTime::now_utc(),
                    ),
                )
                .await
                .unwrap();
            crate::activity_eligibility::scan_activity_eligibility_once(
                &runtime.activity_retry_deps(),
                ShardId(0),
                OffsetDateTime::now_utc(),
            )
            .await;
            let LoadedRun::Existing(after) = repo.load_run(state.run_key).await.unwrap() else {
                panic!("run missing")
            };
            assert!(!after.timers.contains_key(&timer.timer_id));
            assert!(after.activities[&task.activity_id].pause_info.is_some());
            runtime.runtime_shutdown.begin_shutdown();
            runtime
                .runtime_shutdown
                .wait(std::time::Instant::now() + std::time::Duration::from_secs(10))
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn ordinary_starts_apply_rules_before_started() {
        let repo = Arc::new(InMemoryStore::default());
        let runtime = TokeiraRuntime::new(
            repo.clone(),
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
        );
        let (state, queue, task) = seed_scheduled_activity(&runtime, &repo).await;
        assert_eq!(
            repo.create_workflow_rule(
                state.namespace_id,
                pause_rule("pause", "ActivityType = 'activity-type'"),
                10,
            )
            .await
            .expect("store rule"),
            WorkflowRuleCreateResult::Created,
        );

        let started = runtime
            .poll_activity_task(
                queue,
                WorkerIdentity("worker".into()),
                tokio::time::Duration::from_millis(100),
            )
            .await
            .expect("ordinary poll");
        assert!(started.is_none());
        let LoadedRun::Existing(after) = repo.load_run(task.run_key).await.unwrap() else {
            panic!("seeded run should exist");
        };
        let pause = after.activities[&task.activity_id]
            .pause_info
            .as_ref()
            .expect("rule must pause before Started");
        assert_eq!(pause.rule_id.as_deref(), Some("pause"));
        assert!(
            after.activities[&task.activity_id]
                .started_event_id
                .is_none()
        );
    }

    #[tokio::test]
    // Feature: workflow-rules, Property 6: poll-admission independence
    async fn rule_created_after_poll_admission_still_pauses_before_started() {
        let repo = Arc::new(InMemoryStore::default());
        let runtime = Arc::new(TokeiraRuntime::new(
            repo.clone(),
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
        ));
        let (state, queue, task) = seed_scheduled_activity(&runtime, &repo).await;
        let seed_offer = runtime
            .activity_broker
            .try_claim_activity_offer(&queue, task.run_key, &task.activity_id)
            .await
            .expect("remove the seed offer before admitting the test poll");

        let polling_runtime = runtime.clone();
        let polling_queue = queue.clone();
        let poll = tokio::spawn(async move {
            polling_runtime
                .poll_activity_task(
                    polling_queue,
                    WorkerIdentity("worker".to_string()),
                    tokio::time::Duration::from_secs(5),
                )
                .await
        });
        loop {
            if runtime
                .activity_broker
                .queues_with_waiters()
                .await
                .contains(&queue)
            {
                break;
            }
            tokio::task::yield_now().await;
        }

        // The poll is now parked under the pre-rule admission state. Creating the rule and only
        // then publishing the offer deterministically induces the ordering that the former edge
        // gate snapshot mishandled.
        repo.create_workflow_rule(
            state.namespace_id,
            pause_rule("after-admission", "ActivityType = 'activity-type'"),
            10,
        )
        .await
        .expect("store rule after poll admission");
        // Keep the taken incarnation in flight until admission is observed;
        // otherwise durable discovery can refill it before the poll parks.
        drop(seed_offer);
        runtime
            .activity_broker
            .publish_activity_task(task.clone(), Some(&runtime.delivery_metrics))
            .await
            .expect("publish after rule creation");

        assert!(
            poll.await
                .expect("poll task should not panic")
                .expect("poll should not fail")
                .is_none()
        );
        let LoadedRun::Existing(after) = repo.load_run(task.run_key).await.unwrap() else {
            panic!("seeded run should exist");
        };
        let activity = &after.activities[&task.activity_id];
        assert!(activity.started_event_id.is_none());
        assert_eq!(
            activity
                .pause_info
                .as_ref()
                .and_then(|pause| pause.rule_id.as_deref()),
            Some("after-admission"),
        );
    }

    #[tokio::test]
    async fn retry_rule_applies_only_when_failure_has_a_next_attempt() {
        for retryable in [false, true] {
            let repo = Arc::new(InMemoryStore::default());
            let runtime = TokeiraRuntime::new(
                repo.clone(),
                1,
                LaneConfig::default(),
                TimerScannerConfig::default(),
                WorkflowTimeoutScannerConfig::default(),
            );
            let (state, token, _queue) = seed_started_activity(&runtime, &repo, None).await;
            repo.create_workflow_rule(
                state.namespace_id,
                pause_rule("retry-pause", "Attempts >= 2"),
                10,
            )
            .await
            .expect("store retry rule");

            runtime
                .fail_activity_task(
                    token.clone(),
                    payload(b"failure"),
                    None,
                    !retryable,
                    None,
                    None,
                    RequestContext::unattributed(OffsetDateTime::UNIX_EPOCH),
                )
                .await
                .expect("activity failure");
            let LoadedRun::Existing(after) = repo.load_run(token.run_key).await.unwrap() else {
                panic!("seeded run should exist");
            };
            if retryable {
                let retried = &after.activities[&token.activity_id];
                assert_eq!(retried.attempt, 2);
                assert_eq!(
                    retried
                        .pause_info
                        .as_ref()
                        .and_then(|pause| pause.rule_id.as_deref()),
                    Some("retry-pause"),
                );
                assert!(retried.current_attempt_scheduled_at.is_none());
            } else {
                assert!(!after.activities.contains_key(&token.activity_id));
            }
        }
    }

    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(100))]

        // Feature: activity-dispatch, Property 10: delayed control operations restore owner eligibility without poll demand.
        #[test]
        fn delayed_activity_controls_evaluate_rules_at_eligibility(reset in proptest::bool::ANY, restart in proptest::bool::ANY, seconds in 2i64..30) {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let repo = Arc::new(InMemoryStore::default());
                let runtime = quiet_runtime(repo.clone());
                let (mut state, queue, task) = seed_scheduled_activity(&runtime, &repo).await;
                state.workflow_run_timeout = None;
                state.workflow_execution_timeout = None;
                replace_activity_fixture(&repo, state).await;
                let now = OffsetDateTime::now_utc();
                let request = |id: &str| RequestContext { request_id: RequestId(id.into()), ..RequestContext::unattributed(now) };
                runtime.submit(task.run_key, Command::PauseActivity(tokeira_kernel::PauseActivityRequest {
                    target: tokeira_kernel::ActivityControlTarget::Id(task.activity_id.clone()),
                    identity: "operator".into(), reason: "control fixture".into(), rule_id: None,
                    request: request("pause"), now,
                })).await.unwrap();
                let due = now + Duration::seconds(seconds);
                let control = if reset { Command::ResetActivity(tokeira_kernel::ResetActivityRequest {
                    target: tokeira_kernel::ActivityControlTarget::Id(task.activity_id.clone()), reset_heartbeat: true,
                    keep_paused: false, dispatch_at: due, original_options: BTreeMap::new(), restore_original_options: false,
                    request: request("reset"), now,
                }) } else { Command::UnpauseActivity(tokeira_kernel::UnpauseActivityRequest {
                    target: tokeira_kernel::ActivityControlTarget::Id(task.activity_id.clone()), reset_attempts: false,
                    reset_heartbeat: false, dispatch_at: due, request: request("unpause"), now,
                }) };
                runtime.submit(task.run_key, control).await.unwrap();
                repo.create_workflow_rule(queue.namespace_id, pause_rule("delayed-control", "Attempts >= 1"), 10).await.unwrap();
                if restart { runtime.recover_self_assigned_shard(ShardId(0), ShardEpoch::ZERO).await.unwrap(); }
                let before = repo.load_run(task.run_key).await.unwrap();
                let deps = runtime.activity_retry_deps();
                crate::activity_eligibility::scan_activity_eligibility_once(&deps, ShardId(0), due - Duration::microseconds(1)).await;
                prop_assert_eq!(&repo.load_run(task.run_key).await.unwrap(), &before);
                crate::activity_eligibility::scan_activity_eligibility_once(&deps, ShardId(0), due).await;
                let LoadedRun::Existing(after) = repo.load_run(task.run_key).await.unwrap() else { panic!("run"); };
                let activity = &after.activities[&task.activity_id];
                prop_assert_eq!(activity.pause_info.as_ref().and_then(|pause| pause.rule_id.as_deref()), Some("delayed-control"));
                prop_assert!(activity.started_event_id.is_none() && activity.current_attempt_scheduled_at.is_none());
                prop_assert!(repo.list_all_dispatchable_activity_tasks(&queue, 10).await.unwrap().is_empty());
                runtime.runtime_shutdown.begin_shutdown();
                runtime.runtime_shutdown.wait(std::time::Instant::now() + std::time::Duration::from_secs(10)).await.unwrap();
                Ok(())
            })?;
        }

        // Feature: activity-dispatch, Property 10
        // Due rule checks survive acquisition without pollers, and successful non-matches stay retired until a new acquisition.
        #[test]
        fn activity_eligibility_rules_survive_restart(backoff_seconds in 2i64..120, restart in proptest::bool::ANY, matches_rule in proptest::bool::ANY) {
            tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
                let repo = Arc::new(InMemoryStore::default());
                let runtime = TokeiraRuntime::new(repo.clone(), 1, LaneConfig::default(), TimerScannerConfig::default(), WorkflowTimeoutScannerConfig::default());
                let (state, token, queue) = seed_started_activity(&runtime, &repo, None).await;
                let deps = runtime.activity_retry_deps();
                commit_activity_retry(&deps, ActivityRetryTarget { run_key: token.run_key, activity_id: &token.activity_id,
                    expected_attempt: token.attempt, expected_schedule_event_id: token.schedule_event_id },
                    2, Duration::seconds(backoff_seconds), None, None).await.unwrap();
                let LoadedRun::Existing(retried) = repo.load_run(token.run_key).await.unwrap() else { panic!("retry exists"); };
                let due = retried.activities[&token.activity_id].current_attempt_scheduled_at.unwrap();
                repo.create_workflow_rule(state.namespace_id, pause_rule("after-retry", if matches_rule { "Attempts >= 2" } else { "Attempts > 10" }), 10).await.unwrap();
                if restart { runtime.recover_self_assigned_shard(ShardId(0), ShardEpoch::ZERO).await.unwrap(); }
                crate::activity_eligibility::scan_activity_eligibility_once(&deps, ShardId(0), due - Duration::microseconds(1)).await;
                assert_eq!(repo.load_run(token.run_key).await.unwrap(), LoadedRun::Existing(retried.clone()));
                crate::activity_eligibility::scan_activity_eligibility_once(&deps, ShardId(0), due).await;
                let LoadedRun::Existing(after) = repo.load_run(token.run_key).await.unwrap() else { panic!("run exists"); };
                assert_eq!(after.activities[&token.activity_id].pause_info.is_some(), matches_rule);
                if !matches_rule {
                    assert_eq!(after, retried);
                    repo.create_workflow_rule(state.namespace_id, pause_rule("after-evaluation", "Attempts >= 2"), 10).await.unwrap();
                    deps.tracking.eligibility.refresh(&after, ShardId(0));
                    crate::activity_eligibility::scan_activity_eligibility_once(&deps, ShardId(0), due).await;
                    assert_eq!(repo.load_run(token.run_key).await.unwrap(), LoadedRun::Existing(after));
                    runtime.recover_self_assigned_shard(ShardId(0), ShardEpoch::ZERO).await.unwrap();
                    crate::activity_eligibility::scan_activity_eligibility_once(&deps, ShardId(0), due).await;
                    let LoadedRun::Existing(after) = repo.load_run(token.run_key).await.unwrap() else { panic!("run exists"); };
                    assert!(after.activities[&token.activity_id].pause_info.is_some());
                }
                assert_eq!(runtime.activity_broker.backlog_stats(&queue).await.count, 0);
                runtime.runtime_shutdown.begin_shutdown();
                runtime.runtime_shutdown.wait(std::time::Instant::now() + std::time::Duration::from_secs(10)).await.unwrap();
            });
        }

        // Feature: activity-dispatch, Property 10
        // A failing due prefix larger than one quantum cannot hide later eligibility obligations.
        #[test]
        fn activity_eligibility_failing_prefix_is_bounded(prefix in 65usize..90, tail in 1usize..15) {
            tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
                let repo = Arc::new(InMemoryStore::default());
                let runtime = TokeiraRuntime::new(repo.clone(), 1, LaneConfig::default(), TimerScannerConfig::default(), WorkflowTimeoutScannerConfig::default());
                let (mut state, queue, template) = seed_scheduled_activity(&runtime, &repo).await;
                runtime.activity_broker.remove_run(state.run_key).await;
                let activity_template = state.activities[&template.activity_id].clone();
                let expected_seq = state.transition_seq;
                state.transition_seq = expected_seq.next();
                state.activities.clear();
                let due = OffsetDateTime::now_utc() + Duration::minutes(2);
                for i in 0..prefix + tail {
                    let mut activity = activity_template.clone();
                    activity.activity_id = format!("eligibility-{i:03}");
                    activity.schedule_event_id = i as i64 + 10;
                    activity.attempt = 2;
                    activity.current_attempt_scheduled_at = Some(due);
                    activity.last_attempt_complete_time = (i >= prefix).then_some(due - Duration::seconds(2));
                    activity.schedule_to_start_timeout = None;
                    activity.schedule_to_close_timeout = None;
                    state.activities.insert(activity.activity_id.clone(), activity);
                }
                repo.commit_transition(state.run_key, Transition { expected_seq, next_state: state.clone(), history_events: SmallVec::new(),
                    event_principals: SmallVec::new(), request_dedupe_ops: SmallVec::new(), activity_ops: SmallVec::new(), timer_ops: SmallVec::new(),
                    dispatch_ops: SmallVec::new(), events_numbered_at_close: 0, workflow_task_heartbeat_timed_out: false, growth_limits: None }, WriteFence::Unfenced).await.unwrap();
                runtime.activity_tracking.eligibility.refresh(&state, ShardId(0));
                repo.create_workflow_rule(state.namespace_id, pause_rule("bounded", "Attempts >= 2"), 10).await.unwrap();
                let deps = runtime.activity_retry_deps();
                crate::activity_eligibility::scan_activity_eligibility_once(&deps, ShardId(0), due).await;
                assert_eq!(repo.load_run(state.run_key).await.unwrap(), LoadedRun::Existing(state.clone()));
                crate::activity_eligibility::scan_activity_eligibility_once(&deps, ShardId(0), due).await;
                let LoadedRun::Existing(after) = repo.load_run(state.run_key).await.unwrap() else { panic!("run exists"); };
                for (i, activity) in after.activities.values().enumerate() { assert_eq!(activity.pause_info.is_some(), i >= prefix); }
                assert_eq!(after.transition_seq.0 - state.transition_seq.0, tail as u64);
                assert_eq!(repo.list_all_dispatchable_activity_tasks(&queue, 200).await.unwrap().len(), prefix);
                runtime.runtime_shutdown.begin_shutdown();
                runtime.runtime_shutdown.wait(std::time::Instant::now() + std::time::Duration::from_secs(10)).await.unwrap();
            });
        }
    }

    #[tokio::test]
    async fn retry_timer_applies_rule_created_during_backoff_before_publish() {
        let repo = Arc::new(InMemoryStore::default());
        let runtime = TokeiraRuntime::new(
            repo.clone(),
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
        );
        let (state, token, queue) = seed_started_activity(&runtime, &repo, None).await;
        let backoff = Duration::minutes(1);
        let deps = runtime.activity_retry_deps();
        commit_activity_retry(
            &deps,
            ActivityRetryTarget {
                run_key: token.run_key,
                activity_id: &token.activity_id,
                expected_attempt: token.attempt,
                expected_schedule_event_id: token.schedule_event_id,
            },
            2,
            backoff,
            Some(payload(b"retryable")),
            None,
        )
        .await
        .expect("commit delayed retry");

        let LoadedRun::Existing(retried) = repo.load_run(token.run_key).await.unwrap() else {
            panic!("seeded run should exist after retry");
        };
        // The durable row exists during the backoff window (inspection view)
        // even though the delivery view withholds it until due.
        assert_eq!(
            repo.list_all_dispatchable_activity_tasks(&queue, 10)
                .await
                .unwrap()
                .len(),
            1,
        );

        repo.create_workflow_rule(
            state.namespace_id,
            pause_rule("during-backoff", "Attempts >= 2"),
            10,
        )
        .await
        .expect("store rule during retry backoff");
        crate::activity_eligibility::scan_activity_eligibility_once(
            &deps,
            ShardId(0),
            retried.activities[&token.activity_id]
                .current_attempt_scheduled_at
                .unwrap(),
        )
        .await;

        let LoadedRun::Existing(after) = repo.load_run(token.run_key).await.unwrap() else {
            panic!("seeded run should exist after timer evaluation");
        };
        let paused = &after.activities[&token.activity_id];
        assert_eq!(
            paused
                .pause_info
                .as_ref()
                .and_then(|pause| pause.rule_id.as_deref()),
            Some("during-backoff"),
        );
        assert!(paused.current_attempt_scheduled_at.is_none());
        assert!(
            repo.list_all_dispatchable_activity_tasks(&queue, 10)
                .await
                .unwrap()
                .is_empty()
        );
    }

    /// Close a seeded run the way a worker's close does: its activities stay in state.
    async fn close_seeded_run(repo: &InMemoryStore, run_key: RunKey) -> WorkflowState {
        let LoadedRun::Existing(current) = repo.load_run(run_key).await.unwrap() else {
            panic!("seeded run should exist");
        };
        let mut closed = current.clone();
        closed.status = ExecutionStatus::Completed;
        closed.closed_at = Some(OffsetDateTime::now_utc());
        closed.transition_seq = current.transition_seq.next();
        let transition = Transition {
            expected_seq: current.transition_seq,
            next_state: closed.clone(),
            history_events: SmallVec::new(),
            event_principals: SmallVec::new(),
            request_dedupe_ops: SmallVec::new(),
            activity_ops: SmallVec::new(),
            timer_ops: SmallVec::new(),
            dispatch_ops: SmallVec::new(),
            events_numbered_at_close: 0,
            workflow_task_heartbeat_timed_out: false,
            growth_limits: None,
        };
        repo.commit_transition(run_key, transition, WriteFence::Unfenced)
            .await
            .expect("close seeded run");
        closed
    }

    fn rejected_as_closed(error: &anyhow::Error) -> bool {
        error
            .downcast_ref::<KernelRejected>()
            .is_some_and(|rejected| {
                matches!(
                    rejected.0,
                    tokeira_kernel::Reject::RunClosed(ExecutionStatus::Completed)
                )
            })
    }

    #[tokio::test]
    async fn closed_run_refuses_heartbeat_completion_and_retryable_failure() {
        let repo = Arc::new(InMemoryStore::default());
        let runtime = TokeiraRuntime::new(
            repo.clone(),
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
        );
        let (_state, token, queue) = seed_started_activity(&runtime, &repo, None).await;
        let closed = close_seeded_run(&repo, token.run_key).await;

        let heartbeat = runtime
            .record_activity_heartbeat(token.clone(), Some(payloads(b"late")), None)
            .await
            .expect_err("a closed run refuses a heartbeat");
        assert!(rejected_as_closed(&heartbeat), "{heartbeat:?}");
        let failure = runtime
            .fail_activity_task(
                token.clone(),
                payload(b"retryable"),
                None,
                false,
                None,
                None,
                RequestContext::unattributed(OffsetDateTime::UNIX_EPOCH),
            )
            .await
            .expect_err("a closed run refuses a retryable failure");
        assert!(rejected_as_closed(&failure), "{failure:?}");
        let completion = runtime
            .complete_activity_task(
                token.clone(),
                payloads(b"done"),
                None,
                RequestContext::unattributed(OffsetDateTime::UNIX_EPOCH),
            )
            .await
            .expect_err("a closed run refuses a completion");
        assert!(rejected_as_closed(&completion), "{completion:?}");

        let LoadedRun::Existing(after) = repo.load_run(token.run_key).await.unwrap() else {
            panic!("closed run should exist");
        };
        assert_eq!(after, closed);
        assert!(
            repo.list_all_dispatchable_activity_tasks(&queue, 10)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn timeout_scan_untracks_a_closed_runs_activity_without_retrying_it() {
        use crate::activity_timeout::{ActivityTrackingEntry, scan_activity_timeouts_once};

        let repo = Arc::new(InMemoryStore::default());
        let runtime = TokeiraRuntime::new(
            repo.clone(),
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
        );
        let (state, token, _queue) = seed_started_activity(&runtime, &repo, None).await;
        let activity = &state.activities[&token.activity_id];
        let long_ago = OffsetDateTime::now_utc() - Duration::hours(1);
        let deps = runtime.activity_retry_deps();
        deps.tracking.insert(ActivityTrackingEntry {
            timeouts: None,
            run_key: token.run_key,
            shard_id: ShardId(0),
            activity_id: token.activity_id.clone(),
            original_scheduled_at: activity.scheduled_at,
            last_dispatched_at: long_ago,
            started_at: Some(long_ago),
            last_heartbeat_at: None,
            cancel_requested: false,
        });
        let closed = close_seeded_run(&repo, token.run_key).await;

        scan_activity_timeouts_once(
            &deps,
            None,
            &runtime.lanes,
            runtime.lanes.len(),
            &ActivityTimeoutScannerConfig::default(),
        )
        .await;

        assert!(deps.tracking.snapshot().is_empty());
        let LoadedRun::Existing(after) = repo.load_run(token.run_key).await.unwrap() else {
            panic!("closed run should exist");
        };
        assert_eq!(after, closed);
    }

    #[tokio::test]
    async fn heartbeat_details_persist_and_return_on_retry_poll() {
        let repo = Arc::new(InMemoryStore::default());
        let runtime = TokeiraRuntime::new(
            repo.clone(),
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
        );
        let (_state, token, queue) = seed_started_activity(&runtime, &repo, None).await;
        let details = payloads(b"checkpoint-1");

        let outcome = runtime
            .record_activity_heartbeat(token.clone(), Some(details.clone()), None)
            .await
            .expect("heartbeat should persist");
        assert_eq!(outcome, ActivityHeartbeatOutcome::default());
        let LoadedRun::Existing(after_heartbeat) = repo.load_run(token.run_key).await.unwrap()
        else {
            panic!("seeded run should exist");
        };
        assert_eq!(
            after_heartbeat.activities["activity-1"].heartbeat_details,
            Some(details.clone())
        );
        let heartbeat_at = after_heartbeat.activities["activity-1"].last_heartbeat_at;
        assert!(
            heartbeat_at.is_some(),
            "the heartbeat commit records its time"
        );

        runtime
            .fail_activity_task(
                token,
                payload(b"retryable"),
                None,
                false,
                None,
                None,
                RequestContext::unattributed(OffsetDateTime::UNIX_EPOCH),
            )
            .await
            .expect("retryable failure should re-dispatch activity");
        let LoadedRun::Existing(after_retry) =
            repo.load_run(after_heartbeat.run_key).await.unwrap()
        else {
            panic!("seeded run should exist after retry");
        };
        let retried = after_retry.activities.get("activity-1").unwrap();
        assert_eq!(retried.attempt, 2);
        assert_eq!(retried.heartbeat_details, Some(details.clone()));
        // A retry without a heartbeat reset keeps the time
        // (`UpdateActivityInfoForRetries`, activity.go:86-90 @ v1.31.0).
        assert_eq!(retried.last_heartbeat_at, heartbeat_at);
        assert!(retried.current_attempt_scheduled_at.is_some());

        // The retried attempt is published only after the 1s retry backoff
        // (v1.31.0 dispatches retries on a retry timer, `activity.go:74` +
        // `GenerateActivityRetryTasks`), so the poll window must cover it.
        let started = runtime
            .poll_activity_task(
                queue,
                WorkerIdentity("activity-worker".to_string()),
                tokio::time::Duration::from_secs(5),
            )
            .await
            .expect("poll should not fail")
            .expect("retried activity should be available");
        assert_eq!(started.attempt, 2);
        assert_eq!(started.heartbeat_details, Some(details));
        assert_eq!(started.scheduled_time, retried.scheduled_at);
        assert_eq!(
            started.current_attempt_scheduled_time,
            retried.current_attempt_scheduled_at
        );
        // Starting the attempt leaves the time alone.
        let LoadedRun::Existing(after_start) =
            repo.load_run(after_heartbeat.run_key).await.unwrap()
        else {
            panic!("seeded run should exist after start");
        };
        let restarted = &after_start.activities["activity-1"];
        assert!(restarted.started_at.is_some());
        assert_eq!(restarted.last_heartbeat_at, heartbeat_at);
    }

    // Feature: projection-accumulator, Property 2: complete baseline image equivalence
    // Feature: projection-accumulator, Property 4: consistent non-durable legacy seeding
    // Every direct writer reloads legacy state and persists its seeded observations.
    #[tokio::test]
    async fn projection_accumulator_all_direct_activity_writers_seed_legacy_state() {
        for path in 0..5 {
            let seed_repo = Arc::new(InMemoryStore::default());
            let seed_runtime = TokeiraRuntime::new(
                seed_repo.clone(),
                1,
                LaneConfig::default(),
                TimerScannerConfig::default(),
                WorkflowTimeoutScannerConfig::default(),
            );
            let (key, token, task) = if path < 2 {
                let (state, token, _) =
                    seed_started_activity(&seed_runtime, &seed_repo, None).await;
                (state.run_key, Some(token), None)
            } else {
                let (state, _, task) = seed_scheduled_activity(&seed_runtime, &seed_repo).await;
                (state.run_key, None, Some(task))
            };
            let repo = Arc::new(
                crate::projection_accumulator_tests::legacy_copy(
                    &seed_repo,
                    key,
                    vec!["legacy".into(), "".into(), "legacy".into()],
                )
                .await,
            );
            let runtime = TokeiraRuntime::new(
                repo.clone(),
                1,
                LaneConfig::default(),
                TimerScannerConfig::default(),
                WorkflowTimeoutScannerConfig::default(),
            );
            let LoadedRun::Existing(before) = repo.load_run(key).await.unwrap() else {
                panic!("missing legacy run")
            };
            let recovered = repo
                .list_recovery_candidates_for_shard(tokeira_types::ShardId(0), None, 100)
                .await
                .unwrap();
            assert!(recovered.states.iter().any(
                |state| state.run_key == key && state.used_worker_deployment_versions.is_none()
            ));
            runtime.ready().await.unwrap();
            match path {
                0 => {
                    runtime
                        .record_activity_heartbeat(
                            token.unwrap(),
                            Some(payloads(b"checkpoint")),
                            None,
                        )
                        .await
                        .unwrap();
                }
                1 => {
                    let token = token.unwrap();
                    commit_activity_retry(
                        &runtime.activity_retry_deps(),
                        ActivityRetryTarget {
                            run_key: key,
                            activity_id: &token.activity_id,
                            expected_attempt: token.attempt,
                            expected_schedule_event_id: token.schedule_event_id,
                        },
                        2,
                        Duration::minutes(1),
                        Some(payload(b"retryable")),
                        None,
                    )
                    .await
                    .unwrap();
                }
                2 => {
                    runtime
                        .force_start_activity_for_completion(
                            key,
                            "activity-1",
                            WorkerIdentity("by-id".into()),
                            RequestContext::unattributed(OffsetDateTime::now_utc()),
                        )
                        .await
                        .unwrap();
                }
                3 => {
                    assert!(
                        runtime
                            .start_activity_task(
                                &task.unwrap(),
                                Uuid::new_v4(),
                                tokio::time::Instant::now(),
                                &WorkerIdentity("poller".into()),
                            )
                            .await
                            .unwrap()
                            .is_some()
                    );
                }
                _ => {
                    repo.create_workflow_rule(
                        before.namespace_id,
                        pause_rule("pause", "Attempts >= 1"),
                        10,
                    )
                    .await
                    .unwrap();
                    assert!(matches!(
                        prepare_activity_dispatch(
                            &runtime.activity_retry_deps(),
                            &task.unwrap(),
                            OffsetDateTime::now_utc(),
                            &crate::outbound::driver::Attempt::capture(
                                &runtime.shard_owner,
                                ShardId(0)
                            )
                            .unwrap(),
                        )
                        .await
                        .unwrap(),
                        ActivityDispatchPreparation::SuppressedByRule
                    ));
                }
            }
            let restored = InMemoryStore::from_snapshot(&repo.snapshot().await.unwrap()).unwrap();
            let LoadedRun::Existing(after) = restored.load_run(key).await.unwrap() else {
                panic!("missing committed run")
            };
            assert!(
                after.transition_seq > before.transition_seq,
                "path {path} must commit"
            );
            assert_eq!(
                after.used_worker_deployment_versions,
                Some(vec!["legacy".into(), "".into(), "legacy".into()]),
                "path {path}"
            );
        }
    }

    #[tokio::test]
    async fn running_pause_projects_on_heartbeat_and_parks_retry() {
        let repo = Arc::new(InMemoryStore::default());
        let runtime = TokeiraRuntime::new(
            repo.clone(),
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
        );
        let (_state, token, _queue) = seed_started_activity(&runtime, &repo, None).await;
        let now = OffsetDateTime::now_utc();
        runtime
            .pause_activities(
                token.run_key,
                tokeira_kernel::PauseActivityRequest {
                    target: ActivityControlTarget::Id(token.activity_id.clone()),
                    identity: "operator".to_string(),
                    reason: "investigate".to_string(),
                    rule_id: None,
                    request: RequestContext {
                        request_id: RequestId("pause-running".to_string()),
                        caller_identity: Some("operator".to_string()),
                        principal: None,
                        received_at: now,
                    },
                    now,
                },
            )
            .await
            .expect("running activity should pause");

        let outcome = runtime
            .record_activity_heartbeat(token.clone(), Some(payloads(b"running")), None)
            .await
            .expect("running token remains valid after pause");
        assert!(outcome.activity_paused);
        assert!(!outcome.activity_reset);

        runtime
            .fail_activity_task(
                token.clone(),
                payload(b"retryable"),
                None,
                false,
                None,
                None,
                RequestContext::unattributed(OffsetDateTime::UNIX_EPOCH),
            )
            .await
            .expect("paused retryable failure should park");
        let LoadedRun::Existing(after) = repo.load_run(token.run_key).await.unwrap() else {
            panic!("seeded run should exist");
        };
        let parked = &after.activities[&token.activity_id];
        assert_eq!(parked.attempt, 2);
        assert!(parked.pause_info.is_some());
        assert!(parked.started_at.is_none());
        assert!(parked.started_event_id.is_none());
        assert!(parked.current_attempt_scheduled_at.is_none());
    }

    #[tokio::test]
    async fn running_reset_defers_heartbeat_clear_until_retry_preparation() {
        let repo = Arc::new(InMemoryStore::default());
        let runtime = TokeiraRuntime::new(
            repo.clone(),
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
        );
        let original = payloads(b"before-reset");
        let (_state, token, _queue) =
            seed_started_activity(&runtime, &repo, Some(original.clone())).await;
        let now = OffsetDateTime::now_utc();
        runtime
            .reset_activities(
                token.run_key,
                ResetActivitiesRequest {
                    target: ActivityControlTarget::Id(token.activity_id.clone()),
                    reset_heartbeat: true,
                    keep_paused: false,
                    jitter: None,
                    restore_original_options: false,
                    request: RequestContext {
                        request_id: RequestId("reset-running".to_string()),
                        caller_identity: Some("operator".to_string()),
                        principal: None,
                        received_at: now,
                    },
                    now,
                },
            )
            .await
            .expect("running activity should reset");
        let LoadedRun::Existing(reset_state) = repo.load_run(token.run_key).await.unwrap() else {
            panic!("seeded run should exist");
        };
        let reset = &reset_state.activities[&token.activity_id];
        assert_eq!(reset.heartbeat_details, Some(original));
        assert!(reset.activity_reset);
        assert!(reset.reset_heartbeats);

        let outcome = runtime
            .record_activity_heartbeat(token.clone(), Some(payloads(b"after-reset")), None)
            .await
            .expect("attempt-one token remains valid after reset");
        assert!(outcome.activity_reset);
        let LoadedRun::Existing(heartbeated) = repo.load_run(token.run_key).await.unwrap() else {
            panic!("seeded run should exist");
        };
        assert!(
            heartbeated.activities[&token.activity_id]
                .last_heartbeat_at
                .is_some(),
            "the running attempt keeps recording progress after the reset"
        );

        runtime
            .fail_activity_task(
                token.clone(),
                payload(b"retryable"),
                None,
                false,
                None,
                None,
                RequestContext::unattributed(OffsetDateTime::UNIX_EPOCH),
            )
            .await
            .expect("retry preparation should consume reset flags");
        let LoadedRun::Existing(after) = repo.load_run(token.run_key).await.unwrap() else {
            panic!("seeded run should exist");
        };
        let retried = &after.activities[&token.activity_id];
        assert!(retried.heartbeat_details.is_none());
        assert!(retried.last_heartbeat_at.is_none());
        assert!(!retried.activity_reset);
        assert!(!retried.reset_heartbeats);
    }

    /// Load the seeded run's activity.
    async fn seeded_activity(
        repo: &InMemoryStore,
        token: &ActivityTaskToken,
    ) -> tokeira_kernel::ActivityState {
        let LoadedRun::Existing(state) = repo.load_run(token.run_key).await.unwrap() else {
            panic!("seeded run should exist");
        };
        state.activities[&token.activity_id].clone()
    }

    /// Mark the activity's current attempt started, the way the start path
    /// does, and return its token. This skips the retry backoff that the
    /// dispatch path waits out.
    async fn start_current_attempt(
        runtime: &TokeiraRuntime<InMemoryStore>,
        repo: &InMemoryStore,
        token: &ActivityTaskToken,
    ) -> ActivityTaskToken {
        let LoadedRun::Existing(state) = repo.load_run(token.run_key).await.unwrap() else {
            panic!("seeded run should exist");
        };
        let mut next_state = state.clone();
        next_state.transition_seq = state.transition_seq.next();
        let activity = next_state
            .activities
            .get_mut(&token.activity_id)
            .expect("seeded activity");
        activity.started_at = Some(OffsetDateTime::now_utc());
        activity.started_event_id = Some(tokeira_kernel::TRANSIENT_ACTIVITY_STARTED_EVENT_ID);
        let activity = activity.clone();
        let transition = Transition {
            expected_seq: state.transition_seq,
            next_state,
            history_events: SmallVec::new(),
            event_principals: SmallVec::new(),
            request_dedupe_ops: SmallVec::new(),
            activity_ops: smallvec![ActivityOp::Upsert(activity.clone())],
            timer_ops: SmallVec::new(),
            dispatch_ops: SmallVec::new(),
            events_numbered_at_close: 0,
            workflow_task_heartbeat_timed_out: false,
            growth_limits: None,
        };
        repo.commit_transition(token.run_key, transition, WriteFence::Unfenced)
            .await
            .expect("start the current attempt");
        ActivityTaskToken {
            attempt: activity.attempt,
            shard_epoch: runtime
                .shard_epoch_for_completion(&state)
                .await
                .expect("runtime owns seeded run"),
            ..token.clone()
        }
    }

    #[tokio::test]
    async fn heartbeat_commit_records_the_same_time_in_tracking() {
        use crate::activity_timeout::ActivityTrackingEntry;

        let repo = Arc::new(InMemoryStore::default());
        let runtime = TokeiraRuntime::new(
            repo.clone(),
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
        );
        let (state, token, _queue) = seed_started_activity(&runtime, &repo, None).await;
        let activity = &state.activities[&token.activity_id];
        let deps = runtime.activity_retry_deps();
        deps.tracking.insert(ActivityTrackingEntry {
            timeouts: None,
            run_key: token.run_key,
            shard_id: ShardId(0),
            activity_id: token.activity_id.clone(),
            original_scheduled_at: activity.scheduled_at,
            last_dispatched_at: activity.scheduled_at,
            started_at: activity.started_at,
            last_heartbeat_at: None,
            cancel_requested: false,
        });

        runtime
            .record_activity_heartbeat(token.clone(), Some(payloads(b"progress")), None)
            .await
            .expect("heartbeat should persist");

        let persisted = seeded_activity(&repo, &token).await.last_heartbeat_at;
        assert!(persisted.is_some());
        let tracked = deps.tracking.snapshot();
        assert_eq!(tracked.len(), 1);
        assert_eq!(tracked[0].last_heartbeat_at, persisted);
    }

    #[tokio::test]
    async fn failure_with_heartbeat_details_records_progress_in_the_retry() {
        let repo = Arc::new(InMemoryStore::default());
        let runtime = TokeiraRuntime::new(
            repo.clone(),
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
        );
        let (_state, token, _queue) =
            seed_started_activity(&runtime, &repo, Some(payloads(b"heartbeat"))).await;
        let reported = payloads(b"throttled-progress");

        runtime
            .fail_activity_task(
                token.clone(),
                payload(b"retryable"),
                None,
                false,
                Some(reported.clone()),
                None,
                RequestContext::unattributed(OffsetDateTime::UNIX_EPOCH),
            )
            .await
            .expect("retryable failure should re-dispatch activity");

        // The failure's details become the activity's progress, timed by the
        // retry commit (respondactivitytaskfailed/api.go:87-94 @ v1.31.0).
        let retried = seeded_activity(&repo, &token).await;
        assert_eq!(retried.attempt, 2);
        assert_eq!(retried.heartbeat_details, Some(reported));
        assert!(retried.last_heartbeat_at.is_some());
        assert_eq!(
            retried.last_heartbeat_at,
            retried.last_attempt_complete_time
        );
    }

    #[tokio::test]
    async fn running_reset_clears_progress_reported_with_the_failure() {
        let repo = Arc::new(InMemoryStore::default());
        let runtime = TokeiraRuntime::new(
            repo.clone(),
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
        );
        let (_state, token, _queue) =
            seed_started_activity(&runtime, &repo, Some(payloads(b"before-reset"))).await;
        let now = OffsetDateTime::now_utc();
        runtime
            .reset_activities(
                token.run_key,
                ResetActivitiesRequest {
                    target: ActivityControlTarget::Id(token.activity_id.clone()),
                    reset_heartbeat: true,
                    keep_paused: false,
                    jitter: None,
                    restore_original_options: false,
                    request: RequestContext {
                        request_id: RequestId("reset-running".to_string()),
                        caller_identity: Some("operator".to_string()),
                        principal: None,
                        received_at: now,
                    },
                    now,
                },
            )
            .await
            .expect("running activity should reset");

        runtime
            .fail_activity_task(
                token.clone(),
                payload(b"retryable"),
                None,
                false,
                Some(payloads(b"reported-with-failure")),
                None,
                RequestContext::unattributed(OffsetDateTime::UNIX_EPOCH),
            )
            .await
            .expect("retry preparation should consume reset flags");

        // Progress is recorded first and the pending reset then clears it,
        // the order of `UpdateActivityProgress` and `RetryActivity`
        // (respondactivitytaskfailed/api.go:87-105 @ v1.31.0).
        let retried = seeded_activity(&repo, &token).await;
        assert!(retried.heartbeat_details.is_none());
        assert!(retried.last_heartbeat_at.is_none());
        assert!(!retried.reset_heartbeats);
    }

    /// One worker call in a heartbeat-time scenario.
    #[derive(Clone, Copy, Debug)]
    enum ProgressOp {
        Heartbeat,
        FailWithDetails,
        FailWithoutDetails,
    }

    fn arb_progress_ops() -> impl Strategy<Value = Vec<ProgressOp>> {
        prop::collection::vec(
            prop_oneof![
                Just(ProgressOp::Heartbeat),
                Just(ProgressOp::FailWithDetails),
                Just(ProgressOp::FailWithoutDetails),
            ],
            0..8,
        )
        .prop_map(|ops| {
            // The seeded retry policy allows three attempts, so keep at most
            // two retries and the activity never resolves.
            let mut failures = 0;
            ops.into_iter()
                .take_while(|op| {
                    if matches!(op, ProgressOp::Heartbeat) {
                        return true;
                    }
                    failures += 1;
                    failures <= 2
                })
                .collect()
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]

        // Feature: activity-heartbeat-time, Property 7: The time follows progress
        // records and resets
        #[test]
        fn property_heartbeat_time_follows_progress_records(ops in arb_progress_ops()) {
            let runtime_handle = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            runtime_handle.block_on(async {
                let repo = Arc::new(InMemoryStore::default());
                let runtime = TokeiraRuntime::new(
                    repo.clone(),
                    1,
                    LaneConfig::default(),
                    TimerScannerConfig::default(),
                    WorkflowTimeoutScannerConfig::default(),
                );
                let (_state, mut token, _queue) =
                    seed_started_activity(&runtime, &repo, None).await;
                // The model: the commit time of the latest progress record.
                let mut expected: Option<OffsetDateTime> = None;
                for (step, op) in ops.into_iter().enumerate() {
                    match op {
                        ProgressOp::Heartbeat => {
                            runtime
                                .record_activity_heartbeat(
                                    token.clone(),
                                    Some(payloads(format!("beat-{step}").as_bytes())),
                                    None,
                                )
                                .await
                                .expect("heartbeat should persist");
                            let recorded = seeded_activity(&repo, &token).await.last_heartbeat_at;
                            prop_assert!(recorded.is_some());
                            prop_assert!(recorded >= expected);
                            expected = recorded;
                        }
                        ProgressOp::FailWithDetails | ProgressOp::FailWithoutDetails => {
                            let with_details = matches!(op, ProgressOp::FailWithDetails);
                            runtime
                                .fail_activity_task(
                                    token.clone(),
                                    payload(b"retryable"),
                                    None,
                                    false,
                                    with_details
                                        .then(|| payloads(format!("fail-{step}").as_bytes())),
                                    None,
                                    RequestContext::unattributed(OffsetDateTime::UNIX_EPOCH),
                                )
                                .await
                                .expect("retryable failure should re-dispatch activity");
                            let retried = seeded_activity(&repo, &token).await;
                            if with_details {
                                prop_assert_eq!(
                                    retried.last_heartbeat_at,
                                    retried.last_attempt_complete_time
                                );
                                expected = retried.last_heartbeat_at;
                            }
                            prop_assert_eq!(retried.last_heartbeat_at, expected);
                            token = start_current_attempt(&runtime, &repo, &token).await;
                            // Starting the next attempt leaves the time alone.
                            prop_assert_eq!(
                                seeded_activity(&repo, &token).await.last_heartbeat_at,
                                expected
                            );
                        }
                    }
                }
                Ok(())
            })?;
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        // Feature: worker-deployments, Property 13: deployment-version transition lifecycle
        // Feature: activity-dispatch, Property 11: equal revisions do not turn independent activities into workflow transitions.
        // **Validates: Requirements 9.2, 9.5, 9.6**
        #[test]
        fn property_activity_start_transition_lifecycle(
            case in arb_activity_transition_case(),
            wft_revision in 1i64..10_000,
        ) {
            match case {
                ActivityTransitionCase::DeploymentEquality => {
                    let state = auto_upgrade_state("current");
                    let target = wft_target(Some("deployment"), Some("target"), wft_revision, false);
                    let queue = activity_queue(Some("deployment"), Some("target"));

                    prop_assert!(!activity_start_rejected_by_in_flight_transition(&state));
                    prop_assert_eq!(
                        transition_for_polled_activity_task(
                            &state,
                            &target,
                            &queue,
                            wft_revision,
                        ),
                        Some(version_ref("deployment", "target"))
                    );
                }
                ActivityTransitionCase::NameAheadRevision => {
                    let state = auto_upgrade_state("current");
                    let target = wft_target(Some("deployment"), Some("target"), wft_revision, false);
                    let queue = activity_queue(Some("deployment"), Some("ahead"));

                    prop_assert!(!activity_start_rejected_by_in_flight_transition(&state));
                    prop_assert_eq!(
                        transition_for_polled_activity_task(
                            &state,
                            &target,
                            &queue,
                            wft_revision + 1,
                        ),
                        Some(version_ref("deployment", "ahead"))
                    );
                }
                ActivityTransitionCase::EqualRevision => {
                    let state = auto_upgrade_state("current");
                    let target = wft_target(Some("deployment"), Some("target"), wft_revision, false);
                    let queue = activity_queue(Some("deployment"), Some("ahead"));

                    prop_assert!(!activity_start_rejected_by_in_flight_transition(&state));
                    prop_assert_eq!(
                        transition_for_polled_activity_task(
                            &state,
                            &target,
                            &queue,
                            wft_revision,
                        ),
                        None
                    );
                }
                ActivityTransitionCase::Pinned => {
                    let state = pinned_state();
                    let target = wft_target(Some("deployment"), Some("target"), wft_revision, true);
                    let queue = activity_queue(Some("deployment"), Some("target"));

                    prop_assert!(!activity_start_rejected_by_in_flight_transition(&state));
                    prop_assert_eq!(
                        transition_for_polled_activity_task(
                            &state,
                            &target,
                            &queue,
                            wft_revision + 1,
                        ),
                        None
                    );
                }
                ActivityTransitionCase::UnversionedPoller => {
                    let state = auto_upgrade_state("current");
                    let target = wft_target(Some("deployment"), Some("target"), wft_revision, false);
                    let queue = activity_queue(None, None);

                    prop_assert!(!activity_start_rejected_by_in_flight_transition(&state));
                    prop_assert_eq!(
                        transition_for_polled_activity_task(
                            &state,
                            &target,
                            &queue,
                            wft_revision + 1,
                        ),
                        None
                    );
                }
                ActivityTransitionCase::InFlightTransition => {
                    let state = transitioning_state();
                    prop_assert!(activity_start_rejected_by_in_flight_transition(&state));
                }
            }
        }
    }

    fn backoff_fixture_activity(attempt: u32) -> tokeira_kernel::ActivityState {
        tokeira_kernel::ActivityState {
            last_attempt_complete_time: None,
            cancel_requested: false,
            activity_reset: false,
            reset_heartbeats: false,
            started_identity: None,
            retry_last_worker_identity: None,
            activity_id: "activity-1".into(),
            activity_type: "activity-type".into(),
            schedule_event_id: 7,
            task_queue: TaskQueueName("activity-q".into()),
            deployment: None,
            build_id: None,
            retired_input: Default::default(),
            retired_header: Default::default(),
            last_failure: None,
            heartbeat_details: None,
            attempt,
            retry_policy: None,
            schedule_to_close_timeout: None,
            schedule_to_start_timeout: None,
            start_to_close_timeout: None,
            heartbeat_timeout: None,
            scheduled_at: now(),
            current_attempt_scheduled_at: None,
            started_at: None,
            started_event_id: None,
            pause_info: None,
            stamp: 0,
            priority: None,
            last_heartbeat_at: None,
        }
    }

    #[test]
    fn backoff_interval_is_absent_for_first_attempt_and_exact_for_retries() {
        // v1.31.0 first-attempt semantics: recognized-but-absent, so every
        // BackoffInterval predicate is a clean non-match — never zero
        // (matcher/activity_evaluator.go:245-253 @ v1.31.0).
        let first = backoff_fixture_activity(1);
        assert_eq!(
            activity_backoff_interval_seconds(&first).expect("first attempt"),
            None
        );

        let mut retry = backoff_fixture_activity(2);
        retry.last_attempt_complete_time = Some(now());
        retry.current_attempt_scheduled_at = Some(now() + Duration::seconds(5));
        assert_eq!(
            activity_backoff_interval_seconds(&retry).expect("derivable retry"),
            Some(5)
        );

        // For attempts after the first, a missing durable timestamp is an
        // invariant failure that blocks publication, never a skipped rule.
        let mut missing_completed = backoff_fixture_activity(2);
        missing_completed.current_attempt_scheduled_at = Some(now());
        assert!(activity_backoff_interval_seconds(&missing_completed).is_err());
        let mut missing_scheduled = backoff_fixture_activity(2);
        missing_scheduled.last_attempt_complete_time = Some(now());
        assert!(activity_backoff_interval_seconds(&missing_scheduled).is_err());
    }

    #[tokio::test]
    async fn retained_activity_head_page_cannot_starve_later_due_work() {
        let repo = Arc::new(InMemoryStore::default());
        let runtime = TokeiraRuntime::new(
            repo.clone(),
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
        );
        let (mut state, queue, template) = seed_scheduled_activity(&runtime, &repo).await;
        runtime.activity_broker.remove_run(state.run_key).await;
        let activity_template = state.activities[&template.activity_id].clone();
        let expected_seq = state.transition_seq;
        state.transition_seq = TransitionSeq(expected_seq.0 + 1);
        state.activities.clear();
        let cutoff = OffsetDateTime::now_utc();
        let mut dispatch_ops = SmallVec::new();
        let mut activity_ops = smallvec![ActivityOp::Delete {
            activity_id: template.activity_id.clone()
        }];
        let mut offers = Vec::new();
        for index in 0..65 {
            let mut activity = activity_template.clone();
            activity.activity_id = format!("retained-{index:03}");
            activity.schedule_event_id = 10 + index;
            activity.scheduled_at = cutoff - Duration::seconds(1) + Duration::microseconds(index);
            activity.current_attempt_scheduled_at = Some(activity.scheduled_at);
            let task = DispatchableActivityTask {
                activity_id: activity.activity_id.clone(),
                schedule_event_id: activity.schedule_event_id,
                ..template.clone()
            };
            dispatch_ops.push(DispatchOp::EnqueueActivityTask {
                queue: queue.clone(),
                activity_id: activity.activity_id.clone(),
                schedule_event_id: activity.schedule_event_id,
                attempt: 1,
                dispatch_revision: 0,
                stamp: 0,
                dispatch_at: activity.scheduled_at,
                schedule_to_close_timeout: activity.schedule_to_close_timeout,
                schedule_to_start_timeout: activity.schedule_to_start_timeout,
                start_to_close_timeout: activity.start_to_close_timeout,
                heartbeat_timeout: activity.heartbeat_timeout,
                priority: None,
            });
            activity_ops.push(ActivityOp::Upsert(activity.clone()));
            state
                .activities
                .insert(activity.activity_id.clone(), activity);
            offers.push(task);
        }
        repo.commit_transition(
            state.run_key,
            Transition {
                expected_seq,
                next_state: state.clone(),
                history_events: SmallVec::new(),
                event_principals: SmallVec::new(),
                request_dedupe_ops: SmallVec::new(),
                activity_ops,
                timer_ops: SmallVec::new(),
                dispatch_ops,
                events_numbered_at_close: 0,
                workflow_task_heartbeat_timed_out: false,
                growth_limits: None,
            },
            WriteFence::Unfenced,
        )
        .await
        .expect("seed more than one due page");
        for task in &offers[..64] {
            runtime
                .activity_broker
                .publish_activity_task(task.clone(), None)
                .await
                .unwrap();
        }
        let tail = &offers[64];
        let discovery = crate::activity_discovery::ActivityDiscoveryRegistry::default();
        let home =
            crate::discovery::QueueHomeProvider::local_home(runtime.discovery.1.as_ref(), &queue)
                .unwrap();
        let instant = tokio::time::Instant::now();
        let _demand = discovery
            .register_poll(queue.clone(), home, instant)
            .unwrap();
        let source = crate::activity_discovery::ActivitySource {
            repo: repo.clone(),
            registry: runtime.worker_deployment_registry.clone(),
            owner: runtime.shard_owner.clone(),
        };
        // Resident offers retain their durable rows; a complete pass must still
        // reach the lost offer after them without deleting the healthy prefix.
        for _ in 0..4 {
            let Some((key, home, mut pass)) = discovery.select(instant, cutoff) else {
                break;
            };
            let outcome = crate::activity_discovery::run_slice(
                &source,
                &discovery,
                &runtime.activity_broker,
                &key,
                &home,
                &mut pass,
            )
            .await
            .unwrap();
            discovery.complete(&key, outcome, pass, instant);
        }
        assert!(
            runtime.activity_broker.contains_activity_task(tail).await,
            "65th due activity is starved behind 64 retained healthy rows"
        );
        assert_eq!(
            repo.list_all_dispatchable_activity_tasks(&queue, 100)
                .await
                .unwrap()
                .len(),
            65
        );
        runtime.runtime_shutdown.begin_shutdown();
    }

    async fn discover_activity_page(
        runtime: &TokeiraRuntime<InMemoryStore>,
        queue: &QueueKey,
    ) -> usize {
        let before = runtime.activity_broker.backlog_stats(queue).await.count;
        let registry = crate::activity_discovery::ActivityDiscoveryRegistry::default();
        let home =
            crate::discovery::QueueHomeProvider::local_home(runtime.discovery.1.as_ref(), queue)
                .unwrap();
        let now = tokio::time::Instant::now();
        let _demand = registry.register_poll(queue.clone(), home, now).unwrap();
        let source = crate::activity_discovery::ActivitySource {
            repo: runtime.repo.clone(),
            registry: runtime.worker_deployment_registry.clone(),
            owner: runtime.shard_owner.clone(),
        };
        let (key, home, mut pass) = registry.select(now, OffsetDateTime::now_utc()).unwrap();
        let outcome = crate::activity_discovery::run_slice(
            &source,
            &registry,
            &runtime.activity_broker,
            &key,
            &home,
            &mut pass,
        )
        .await
        .unwrap();
        registry.complete(&key, outcome, pass, now);
        (runtime.activity_broker.backlog_stats(queue).await.count - before) as usize
    }

    #[tokio::test]
    async fn discovery_skips_activity_already_resident_in_broker() {
        let repo = Arc::new(InMemoryStore::default());
        let runtime = TokeiraRuntime::new(
            repo.clone(),
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
        );
        let (state, queue, _task) = seed_scheduled_activity(&runtime, &repo).await;
        let deps = runtime.activity_retry_deps();

        let published = discover_activity_page(&runtime, &queue).await;

        assert_eq!(published, 0, "resident work needs no reconciliation");
        let offered = deps
            .broker
            .poll_activity_task(&queue, std::time::Duration::ZERO)
            .await
            .expect("poll broker")
            .expect("original broker offer remains available");
        assert_eq!(offered.0.run_key, state.run_key);
    }

    #[tokio::test]
    async fn obsolete_dispatch_effect_cannot_hide_live_row_under_limit_one() {
        let repo = Arc::new(InMemoryStore::default());
        let runtime = TokeiraRuntime::new(
            repo.clone(),
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
        );
        // A live scheduled activity whose broker entry is consumed without a
        // Started commit (the lost-take shape).
        let (state, queue, _task) = seed_scheduled_activity(&runtime, &repo).await;
        let offer = runtime
            .poll_activity_task_offer(
                queue.clone(),
                tokeira_types::WorkerIdentity("worker".into()),
                tokio::time::Duration::from_millis(5),
            )
            .await
            .expect("poll offer")
            .expect("live offer");
        drop(offer);

        // An obsolete effect cannot create a zombie ahead of live work: the
        // committed state, rather than the dispatch operation, defines the rows.
        let zombie_run = RunKey::new();
        let mut zombie_state = state.clone();
        zombie_state.run_key = zombie_run;
        zombie_state.run_id = tokeira_types::RunId::new();
        zombie_state.workflow_id = WorkflowId("zombie".into());
        zombie_state.transition_seq = tokeira_types::TransitionSeq(1);
        zombie_state.activities.clear();
        let zombie_transition = Transition {
            expected_seq: tokeira_types::TransitionSeq::ZERO,
            next_state: zombie_state,
            history_events: Default::default(),
            event_principals: Default::default(),
            request_dedupe_ops: Default::default(),
            activity_ops: Default::default(),
            timer_ops: Default::default(),
            dispatch_ops: smallvec![DispatchOp::EnqueueActivityTask {
                queue: queue.clone(),
                activity_id: "zombie".into(),
                schedule_event_id: 3,
                attempt: 1,
                dispatch_revision: 0,
                stamp: 0,
                dispatch_at: OffsetDateTime::UNIX_EPOCH,
                schedule_to_close_timeout: None,
                schedule_to_start_timeout: None,
                start_to_close_timeout: None,
                heartbeat_timeout: None,
                priority: None,
            }],
            events_numbered_at_close: 0,
            workflow_task_heartbeat_timed_out: false,
            growth_limits: None,
        };
        repo.commit_transition(zombie_run, zombie_transition, WriteFence::Unfenced)
            .await
            .expect("commit zombie row");

        let deps = runtime.activity_retry_deps();
        let shard_id = tokeira_types::ShardId(0);
        let now = OffsetDateTime::now_utc();
        // Even a one-row scan reaches the live incarnation immediately.
        let published = discover_activity_page(&runtime, &queue).await;
        assert_eq!(published, 1, "obsolete effects must not occupy the head");
        assert!(
            repo.list_all_dispatchable_activity_tasks(&queue, 10)
                .await
                .expect("inspect rows")
                .iter()
                .all(|task| task.run_key != zombie_run),
            "the obsolete effect must not create a row"
        );
        let due_after_prune = repo
            .list_due_dispatchable_activity_tasks_for_shard(shard_id, now, None, 10)
            .await
            .expect("list due rows");
        assert_eq!(
            due_after_prune
                .iter()
                .map(|due| due.task.run_key)
                .collect::<Vec<_>>(),
            vec![state.run_key],
            "exactly the authoritative live row is due"
        );
        let published = discover_activity_page(&runtime, &queue).await;
        assert_eq!(published, 0, "the resident live offer must not duplicate");
        let redelivered = deps
            .broker
            .poll_activity_task(&queue, std::time::Duration::ZERO)
            .await
            .expect("poll broker")
            .expect("republished live offer");
        assert_eq!(redelivered.0.run_key, state.run_key);
    }

    #[tokio::test]
    async fn execution_home_activity_heartbeat_and_completion_check_current_and_stale_epochs() {
        for (repair, reacquire) in [(false, false), (true, false), (false, true), (true, true)] {
            let repo = Arc::new(InMemoryStore::with_shard_count(8));
            let runtime =
                super::super::repair_tests::home_runtime(repo.clone(), ShardId(0), repair);
            for index in 0..8 {
                let mut owner = runtime.shard_owner.write().unwrap();
                owner.record_acquired(ShardId(index), ShardEpoch(17));
                owner.mark_active(ShardId(index));
            }
            let (state, token, _) = seed_started_activity(&runtime, &repo, None).await;
            let home = runtime.execution_home(&state);
            let other = crate::shard::shard_for(state.run_key, 8);
            assert_ne!(other, home);
            {
                let mut owner = runtime.shard_owner.write().unwrap();
                *owner = ShardOwner::new(8);
                owner.record_acquired(home, ShardEpoch(17));
                owner.mark_active(home);
            }
            runtime
                .record_activity_heartbeat(token.clone(), Some(payloads(b"home-heartbeat")), None)
                .await
                .unwrap();
            {
                let mut owner = runtime.shard_owner.write().unwrap();
                owner.record_acquired(other, ShardEpoch(33));
                owner.mark_active(other);
            }
            assert_eq!(token.shard_epoch, ShardEpoch(17));
            let wrong_epoch = ActivityTaskToken {
                shard_epoch: ShardEpoch(33),
                ..token.clone()
            };
            assert!(
                runtime
                    .record_activity_heartbeat(wrong_epoch.clone(), None, None)
                    .await
                    .is_err()
            );
            assert!(
                runtime
                    .complete_activity_task(
                        wrong_epoch,
                        Payloads::default(),
                        None,
                        RequestContext::unattributed(OffsetDateTime::now_utc())
                    )
                    .await
                    .is_err()
            );
            if reacquire {
                {
                    let mut owner = runtime.shard_owner.write().unwrap();
                    owner.record_acquired(home, ShardEpoch(18));
                    owner.mark_active(home);
                }
                let before = repo.snapshot().await.unwrap();
                let error = runtime
                    .record_activity_heartbeat(token, Some(payloads(b"stale-heartbeat")), None)
                    .await
                    .unwrap_err();
                assert_eq!(error.to_string(), "activity heartbeat shard epoch mismatch");
                assert_eq!(repo.snapshot().await.unwrap(), before);
            } else {
                let identity_requests = runtime
                    .pick_lane(token.run_key)
                    .identity_requests
                    .load(std::sync::atomic::Ordering::Relaxed);
                assert!(matches!(
                    runtime
                        .complete_activity_task(
                            token,
                            Payloads::default(),
                            None,
                            RequestContext::unattributed(OffsetDateTime::now_utc())
                        )
                        .await
                        .unwrap(),
                    CommitResult::Applied { .. }
                ));
                assert_eq!(
                    runtime.lanes[0]
                        .identity_requests
                        .load(std::sync::atomic::Ordering::Relaxed),
                    identity_requests,
                    "completion already holds the execution home"
                );
            }
            runtime.runtime_shutdown.begin_shutdown();
            runtime
                .runtime_shutdown
                .wait(std::time::Instant::now() + std::time::Duration::from_secs(10))
                .await
                .unwrap();
        }
    }

    /// An activity heartbeat, which commits directly, ends as `NotShardOwner`
    /// once another node has taken the home over, changing nothing, and the
    /// home's acquisition is cancelled.
    // Feature: bundle-lease-fence, Property 4: A lost fence is final
    #[tokio::test]
    async fn a_heartbeat_under_a_lost_fence_ends_the_homes_acquisition() {
        use super::super::lease_fence_tests::{HOME, fenced_runtime, take_over_behind_its_back};

        let repo = Arc::new(InMemoryStore::with_shard_count(1));
        let runtime = fenced_runtime(repo.clone());
        let epoch = runtime.acquire_shard(HOME).await.unwrap();
        let (_state, token, _queue) = seed_started_activity(&runtime, &repo, None).await;
        let taken = take_over_behind_its_back(&repo, epoch).await;
        let acquisition = runtime
            .shard_owner
            .read()
            .unwrap()
            .acquisition(HOME)
            .unwrap();

        let before = repo.snapshot().await.unwrap();
        let error = runtime
            .record_activity_heartbeat(token, Some(payloads(b"late-heartbeat")), None)
            .await
            .unwrap_err();
        let not_owner = error
            .downcast_ref::<crate::errors::NotShardOwner>()
            .unwrap_or_else(|| panic!("{error:?}"));
        assert_eq!(not_owner.current_epoch, taken);
        assert_eq!(repo.snapshot().await.unwrap(), before);
        assert!(acquisition.cancel.is_cancelled());
    }

    // ---- The start's read of the scheduled event (`activity-inputs-from-history`) ----

    /// A runtime whose activity scanner doesn't re-offer dispatch rows while a
    /// test runs, so a task that a start didn't return to its queue stays gone.
    fn quiet_runtime(repo: Arc<InMemoryStore>) -> TokeiraRuntime<InMemoryStore> {
        TokeiraRuntime::new_with_nexus(
            repo,
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
            crate::ActivityTimeoutScannerConfig {
                scan_interval: tokio::time::Duration::from_secs(3600),
                ..crate::ActivityTimeoutScannerConfig::default()
            },
            crate::NexusTimeoutScannerConfig::default(),
            crate::NexusEndpointRegistry::default(),
            Arc::new(crate::NoopNexusHttpClient),
            crate::NexusCompletionDeps::default(),
        )
    }

    fn inputs_runtime(repo: Arc<InMemoryStore>) -> TokeiraRuntime<InMemoryStore> {
        TokeiraRuntime::new(
            repo,
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
        )
    }

    /// Each check that can refuse a start comes before the read of the scheduled
    /// event: a start refused for a stale attempt or stamp, a paused activity or
    /// workflow, a workflow rule or a closed run answers no task, never the
    /// missing event's error (criterion 3.2).
    #[tokio::test]
    async fn a_refused_start_reads_nothing() {
        for refusal in 0..6 {
            let repo = Arc::new(InMemoryStore::default());
            let runtime = inputs_runtime(repo.clone());
            let (state, _queue, task) =
                seed_scheduled_activity_lacking_its_event(&runtime, &repo).await;
            let mut offered = task.clone();
            let request = RequestContext::unattributed(OffsetDateTime::now_utc());
            match refusal {
                0 => offered.attempt += 1,
                1 => offered.stamp += 1,
                2 => {
                    runtime
                        .pause_activities(
                            state.run_key,
                            tokeira_kernel::PauseActivityRequest {
                                target: ActivityControlTarget::Id(task.activity_id.clone()),
                                identity: "operator".to_string(),
                                reason: "refused".to_string(),
                                rule_id: None,
                                request,
                                now: OffsetDateTime::now_utc(),
                            },
                        )
                        .await
                        .expect("pauses the activity");
                }
                3 => {
                    runtime
                        .pause_workflow(
                            ExecutionRef {
                                namespace_id: state.namespace_id,
                                workflow_id: state.workflow_id.clone(),
                                run_id: Some(state.run_id),
                            },
                            PauseWorkflowRequest {
                                identity: "operator".to_string(),
                                reason: "refused".to_string(),
                                request,
                                now: OffsetDateTime::now_utc(),
                            },
                        )
                        .await
                        .expect("pauses the workflow");
                }
                4 => {
                    repo.create_workflow_rule(
                        state.namespace_id,
                        pause_rule("pause", "ActivityType = 'activity-type'"),
                        10,
                    )
                    .await
                    .expect("stores the rule");
                }
                _ => {
                    let LoadedRun::Existing(open) = repo.load_run(state.run_key).await.unwrap()
                    else {
                        panic!("the seeded run");
                    };
                    let mut closed = open.clone();
                    closed.status = ExecutionStatus::Completed;
                    closed.closed_at = Some(OffsetDateTime::now_utc());
                    closed.transition_seq = open.transition_seq.next();
                    repo.commit_transition(
                        state.run_key,
                        Transition {
                            expected_seq: open.transition_seq,
                            next_state: closed,
                            history_events: SmallVec::new(),
                            event_principals: SmallVec::new(),
                            request_dedupe_ops: SmallVec::new(),
                            activity_ops: SmallVec::new(),
                            timer_ops: SmallVec::new(),
                            dispatch_ops: SmallVec::new(),
                            events_numbered_at_close: 0,
                            workflow_task_heartbeat_timed_out: false,
                            growth_limits: None,
                        },
                        WriteFence::Unfenced,
                    )
                    .await
                    .expect("closes the run");
                }
            }
            let started = runtime
                .start_activity_task(
                    &offered,
                    Uuid::new_v4(),
                    tokio::time::Instant::now(),
                    &WorkerIdentity("poller".into()),
                )
                .await;
            assert!(
                matches!(started, Ok(None)),
                "refusal {refusal} answered {started:?}"
            );
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(16))]

        // Feature: activity-inputs-from-history, Property 4: A start whose read
        // fails changes nothing
        #[test]
        fn property_a_start_whose_read_fails_changes_nothing(
            missing in any::<bool>(),
            failures in 1usize..4,
        ) {
            tokio::runtime::Runtime::new().unwrap().block_on(async move {
                let repo = Arc::new(InMemoryStore::default());
                let runtime = quiet_runtime(repo.clone());
                let (state, queue, _task) = if missing {
                    seed_scheduled_activity_lacking_its_event(&runtime, &repo).await
                } else {
                    seed_scheduled_activity(&runtime, &repo).await
                };
                if !missing {
                    repo.fail_history_event_reads(failures).await;
                }
                let before = repo.load_run(state.run_key).await.unwrap();
                let rows = repo.list_all_dispatchable_activity_tasks(&queue, 10).await.unwrap();
                // Rediscovery has a one-second cadence: the assertion is eventual
                // delivery with unchanged authority, not an immediate queue write.
                for _ in 0..failures {
                    let error = runtime
                        .poll_activity_task(
                            queue.clone(),
                            WorkerIdentity("poller".into()),
                            tokio::time::Duration::from_secs(10),
                        )
                        .await
                        .expect_err("the start answers the failed read");
                    let message = error.to_string();
                    prop_assert!(
                        message.contains(if missing {
                            "unable to get activity scheduled event 7"
                        } else {
                            "injected failure"
                        }),
                        "{}",
                        message
                    );
                    prop_assert_eq!(&repo.load_run(state.run_key).await.unwrap(), &before);
                    prop_assert_eq!(
                        &repo.list_all_dispatchable_activity_tasks(&queue, 10).await.unwrap(),
                        &rows
                    );
                }
                if !missing {
                    let started = runtime
                        .poll_activity_task(
                            queue.clone(),
                            WorkerIdentity("poller".into()),
                            tokio::time::Duration::from_secs(10),
                        )
                        .await
                        .unwrap()
                        .expect("the task is back in its queue");
                    prop_assert_eq!(started.input, payloads(b"input"));
                }
                Ok(())
            })?;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn default_activity_scanner_recovers_a_lost_publication() {
        let repo = Arc::new(InMemoryStore::default());
        let runtime = TokeiraRuntime::new(
            repo.clone(),
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
        );
        let (state, queue, _) = seed_scheduled_activity(&runtime, &repo).await;
        runtime.activity_broker.remove_run(state.run_key).await;
        // The actual background scanner must wake and reconstruct from activity
        // dispatch. Paused time advances to its tick while this poll is parked.
        let task = runtime
            .poll_activity_task(
                queue,
                WorkerIdentity("recovered-activity".into()),
                std::time::Duration::from_secs(30),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.run_key, state.run_key);
        runtime.runtime_shutdown.begin_shutdown();
        runtime
            .runtime_shutdown
            .wait(std::time::Instant::now() + std::time::Duration::from_secs(10))
            .await
            .unwrap();
    }
    #[derive(Debug)]
    struct RefuseActivityResponse;

    impl crate::ActivityStartResponsePreflight for RefuseActivityResponse {
        fn validate(&self, _task: &StartedActivityTask) -> Result<()> {
            Err(crate::ActivityStartTooLarge.into())
        }
    }

    async fn replace_activity_fixture(repo: &InMemoryStore, mut state: WorkflowState) {
        let expected_seq = state.transition_seq;
        state.transition_seq = expected_seq.next();
        let result = repo
            .commit_transition(
                state.run_key,
                Transition {
                    expected_seq,
                    next_state: state,
                    history_events: SmallVec::new(),
                    event_principals: SmallVec::new(),
                    request_dedupe_ops: SmallVec::new(),
                    activity_ops: SmallVec::new(),
                    timer_ops: SmallVec::new(),
                    dispatch_ops: SmallVec::new(),
                    events_numbered_at_close: 0,
                    workflow_task_heartbeat_timed_out: false,
                    growth_limits: None,
                },
                WriteFence::Unfenced,
            )
            .await
            .unwrap();
        assert!(matches!(result, CommitResult::Applied { .. }));
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        // Feature: activity-dispatch, Property 11: owner starts preserve pinned, independent and revision-ordered routing decisions.
        #[test]
        fn activity_owner_preserves_registry_routing(case in 0u8..6, revision_delta in 0i64..1000, worker in "[a-z]{0,12}") {
            tokio::runtime::Runtime::new().unwrap().block_on(async move {
                let repo = Arc::new(InMemoryStore::default());
                let runtime = inputs_runtime(repo.clone()).with_worker_deployment_repository(repo.clone());
                let versioning = match case { 4 => pinned_state(), 5 => transitioning_state(), _ => auto_upgrade_state("previous") }.versioning_info;
                let (mut state, queue, mut task) = seed_scheduled_activity_with_versioning(&runtime, &repo, versioning).await;
                state.previous_started_event_id = 2;
                state.last_event_id = task.schedule_event_id;
                replace_activity_fixture(&repo, state).await;
                let LoadedRun::Existing(state) = repo.load_run(task.run_key).await.unwrap() else { panic!("run"); };
                let registry = runtime.deployment_registry().unwrap();
                register_activity_version(&registry, state.namespace_id, "deployment", "current", &queue.task_queue.0).await;
                registry.register_polled_deployment(RegisterPolledDeployment {
                    namespace_id: state.namespace_id, deployment_name: DeploymentName("deployment".into()),
                    build_id: StoredBuildId("current".into()), task_queue: state.task_queue.0.clone(),
                    task_queue_type: DeploymentTaskQueueType::Workflow, identity: "workflow-worker".into(),
                }).await.unwrap();
                set_current_activity_version(&registry, state.namespace_id, "deployment", "current").await;
                task.queue.deployment = Some(DeploymentId(if case == 3 { "independent" } else { "deployment" }.into()));
                task.queue.build_id = Some(RuntimeBuildId(if case == 0 { "current" } else { "other" }.into()));
                task.dispatch_revision = if case == 1 || case == 3 { i64::MAX - revision_delta } else { 0 };
                let selected = task.queue.clone();
                let outcome = runtime.start_forwarded_activity_task(ActivityTaskStartRequest { offered: task.clone(), request_id: Uuid::new_v4(), worker: WorkerIdentity(worker) }).await.unwrap();
                let started = matches!(case, 2..=4);
                prop_assert_eq!(matches!(&outcome, ActivityStartOutcome::Started(_)), started);
                if let ActivityStartOutcome::Started(result) = outcome { prop_assert_eq!(result.origin, WorkerTaskOrigin::from_queue_key(&selected, selected.task_queue.clone(), WorkerTaskClass::Activity)); }
                let LoadedRun::Existing(after) = repo.load_run(state.run_key).await.unwrap() else { panic!("run"); };
                prop_assert_eq!(after.transition_seq, if case == 5 { state.transition_seq } else { state.transition_seq.next() });
                prop_assert_eq!(after.activity_start_receipts.len(), usize::from(started));
                if case < 2 {
                    prop_assert_eq!(after.versioning_info.as_ref().unwrap().version_transition.as_ref(), Some(&version_ref("deployment", if case == 0 { "current" } else { "other" })));
                    prop_assert!(after.pending_workflow_task.is_some());
                } else {
                    prop_assert_eq!(after.versioning_info.as_ref().unwrap().version_transition.as_ref(), state.versioning_info.as_ref().unwrap().version_transition.as_ref());
                }
                runtime.runtime_shutdown.begin_shutdown();
                runtime.runtime_shutdown.wait(std::time::Instant::now() + std::time::Duration::from_secs(10)).await.unwrap();
                Ok(())
            })?;
        }

        // Feature: activity-dispatch, Property 7: competing matches commit exactly one start.
        #[test]
        fn activity_owner_competing_starts(worker in "[a-z]{0,12}", stale in 0u8..4) {
            tokio::runtime::Runtime::new().unwrap().block_on(async move {
                let repo = Arc::new(InMemoryStore::default());
                let runtime = inputs_runtime(repo.clone());
                let (state, _, task) = seed_scheduled_activity(&runtime, &repo).await;
                let request = ActivityTaskStartRequest { offered: task.clone(), request_id: Uuid::new_v4(), worker: WorkerIdentity(worker) };
                let mut obsolete = request.clone();
                match stale {
                    0 => obsolete.offered.attempt += 1,
                    1 => obsolete.offered.schedule_event_id += 1,
                    2 => obsolete.offered.stamp += 1,
                    _ => obsolete.offered.queue.namespace_id = NamespaceId::new(),
                }
                prop_assert!(matches!(runtime.start_forwarded_activity_task(obsolete).await.unwrap(), ActivityStartOutcome::Stale));
                let mut competing = request.clone();
                competing.request_id = Uuid::new_v4();
                let (left, right) = tokio::join!(runtime.start_forwarded_activity_task(request), runtime.start_forwarded_activity_task(competing));
                let results = [left.unwrap(), right.unwrap()];
                prop_assert_eq!(results.iter().filter(|result| matches!(result, ActivityStartOutcome::Started(_))).count(), 1);
                prop_assert_eq!(results.iter().filter(|result| matches!(result, ActivityStartOutcome::Stale)).count(), 1);
                let LoadedRun::Existing(after) = repo.load_run(state.run_key).await.unwrap() else { panic!("run"); };
                prop_assert_eq!(after.transition_seq, state.transition_seq.next());
                prop_assert_eq!(after.activity_start_receipts.len(), 1);
                runtime.runtime_shutdown.begin_shutdown();
                runtime.runtime_shutdown.wait(std::time::Instant::now() + std::time::Duration::from_secs(10)).await.unwrap();
                Ok(())
            })?;
        }

        // Feature: activity-dispatch, Property 8: replay observes one start and never resurrects resolved work.
        #[test]
        fn activity_owner_replay_is_observation(worker in "[a-z]{0,12}", stamp_delta in 1u64..100, resolution in 0u8..3) {
            tokio::runtime::Runtime::new().unwrap().block_on(async move {
                let repo = Arc::new(InMemoryStore::default());
                let runtime = inputs_runtime(repo.clone());
                let (state, _, task) = seed_scheduled_activity(&runtime, &repo).await;
                let request = ActivityTaskStartRequest { offered: task.clone(), request_id: Uuid::new_v4(), worker: WorkerIdentity(worker) };
                let ActivityStartOutcome::Started(started) = runtime.start_forwarded_activity_task(request.clone()).await.unwrap() else { panic!("first start"); };
                let LoadedRun::Existing(mut after) = repo.load_run(state.run_key).await.unwrap() else { panic!("run"); };
                after.activities.get_mut(&task.activity_id).unwrap().stamp += stamp_delta;
                replace_activity_fixture(&repo, after).await;
                let before_replay = repo.load_run(state.run_key).await.unwrap();
                let ActivityStartOutcome::Replayed(replayed) = runtime.start_forwarded_activity_task(request.clone()).await.unwrap() else { panic!("same request"); };
                prop_assert_eq!(&replayed.token, &started.token);
                prop_assert_eq!(replayed.started_time, started.started_time);
                prop_assert_eq!(replayed.input, started.input);
                prop_assert!(replayed.replayed && replayed.workflow_type.is_empty() && replayed.retry_policy.is_none() && replayed.heartbeat_details.is_none());
                prop_assert_eq!(&repo.load_run(state.run_key).await.unwrap(), &before_replay);
                let mut changed = request.clone();
                changed.worker.0.push('x');
                prop_assert!(matches!(runtime.start_forwarded_activity_task(changed).await.unwrap(), ActivityStartOutcome::Stale));
                let LoadedRun::Existing(mut resolved) = before_replay else { panic!("run"); };
                match resolution {
                    0 => { resolved.activities.remove(&task.activity_id); resolved.activity_start_receipts.remove(&task.activity_id); }
                    1 => { resolved.status = ExecutionStatus::Completed; resolved.closed_at = Some(OffsetDateTime::now_utc()); }
                    _ => { let activity = resolved.activities.get_mut(&task.activity_id).unwrap(); activity.attempt += 1; activity.started_at = None; activity.started_event_id = None; resolved.activity_start_receipts.remove(&task.activity_id); }
                }
                replace_activity_fixture(&repo, resolved).await;
                let resolved = repo.load_run(state.run_key).await.unwrap();
                prop_assert!(matches!(runtime.start_forwarded_activity_task(request).await.unwrap(), ActivityStartOutcome::Stale));
                prop_assert_eq!(repo.load_run(state.run_key).await.unwrap(), resolved);
                runtime.runtime_shutdown.begin_shutdown();
                runtime.runtime_shutdown.wait(std::time::Instant::now() + std::time::Duration::from_secs(10)).await.unwrap();
                Ok(())
            })?;
        }

        // Feature: activity-dispatch, Property 9: response refusal precedes the authoritative start.
        #[test]
        fn activity_owner_response_refusal_is_read_only(worker in "[a-z]{0,12}") {
            tokio::runtime::Runtime::new().unwrap().block_on(async move {
                let repo = Arc::new(InMemoryStore::default());
                let runtime = inputs_runtime(repo.clone());
                let (state, _, task) = seed_scheduled_activity(&runtime, &repo).await;
                let request = ActivityTaskStartRequest { offered: task, request_id: Uuid::new_v4(), worker: WorkerIdentity(worker) };
                let before = repo.load_run(state.run_key).await.unwrap();
                let result = crate::scope_activity_start_response(Arc::new(RefuseActivityResponse), runtime.start_forwarded_activity_task(request)).await;
                prop_assert!(result.unwrap_err().is::<crate::ActivityStartTooLarge>());
                prop_assert_eq!(repo.load_run(state.run_key).await.unwrap(), before);
                runtime.runtime_shutdown.begin_shutdown();
                runtime.runtime_shutdown.wait(std::time::Instant::now() + std::time::Duration::from_secs(10)).await.unwrap();
                Ok(())
            })?;
        }
    }
}
