//! Activity-task lifecycle methods of [`TokeiraRuntime`].
//!
//! This `impl` continuation owns the worker-facing activity-task surface:
//! long-poll / eager-claim delivery, the atomic poll→Started transition, and
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
//! - Activity transitions reject-and-reschedule rather than block: a retryable
//!   failure re-enqueues the next attempt, and an OCC conflict at start
//!   republishes the offer instead of making the worker wait on a lock.
use super::*;
use crate::runtime::workflow_task::{
    ResolvedWorkflowTaskTarget, poller_deployment_version, resolve_workflow_task_target_version,
    route_activity_task_queue,
};
use tokeira_observability::OutcomeLabel;
use tokeira_types::{TaskKind, WorkerTaskClass, WorkerTaskOrigin};

const ACTIVITY_BACKLOG_REPROCESS_BATCH: usize = 100;

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
    /// live and durable backlog coordinates, re-deriving every candidate from
    /// authoritative run and deployment state.
    pub async fn reprocess_unversioned_activity_backlog(&self, target: &QueueKey) -> Result<()> {
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

        loop {
            let entries = self
                .repo
                .drain_backlog(&source, ACTIVITY_BACKLOG_REPROCESS_BATCH)
                .await?;
            let exhausted = entries.len() < ACTIVITY_BACKLOG_REPROCESS_BATCH;
            for entry in entries {
                let tokeira_storage::BacklogPayload::Activity {
                    activity_id,
                    input,
                    schedule_event_id,
                    attempt,
                    dispatch_revision,
                    stamp,
                } = entry.payload.clone()
                else {
                    // A workflow payload under an activity QueueKey is corrupt
                    // derived state. Put it back rather than discarding work.
                    self.repo.persist_to_backlog(vec![entry]).await?;
                    return Err(anyhow!(
                        "workflow payload found in unversioned activity backlog"
                    ));
                };
                let LoadedRun::Existing(state) = self.repo.load_run(entry.run_key).await? else {
                    // The authoritative run no longer exists, so the derived
                    // backlog entry is stale and can be discarded.
                    continue;
                };
                let (queue, dispatch_revision) = match route_activity_task_queue(
                    Some(&registry),
                    &state,
                    source.clone(),
                    dispatch_revision,
                )
                .await
                {
                    Ok(route) => route,
                    Err(error) => {
                        self.repo.persist_to_backlog(vec![entry]).await?;
                        return Err(error);
                    }
                };
                let task = DispatchableActivityTask {
                    run_key: entry.run_key,
                    queue,
                    activity_id,
                    input,
                    schedule_event_id,
                    attempt,
                    dispatch_revision,
                    stamp,
                    priority: entry.priority.clone(),
                    order: None,
                };
                if let Err(error) = self
                    .activity_broker
                    .publish_activity_task(task, Some(&self.delivery_metrics))
                    .await
                {
                    self.repo.persist_to_backlog(vec![entry]).await?;
                    return Err(error);
                }
            }
            if exhausted {
                break;
            }
        }
        Ok(())
    }

    /// Long-poll for an activity task, then atomically mark it as started.
    pub async fn poll_activity_task(
        &self,
        queue: QueueKey,
        worker_identity: WorkerIdentity,
        timeout_after: tokio::time::Duration,
    ) -> Result<Option<StartedActivityTask>> {
        let Some(offer) = self
            .poll_activity_task_offer(queue, worker_identity.clone(), timeout_after)
            .await?
        else {
            return Ok(None);
        };
        self.start_activity_task_offer(offer, worker_identity).await
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
        match self
            .activity_broker
            .poll_activity_task_for_worker(&queue, &worker_identity, timeout_after)
            .await?
        {
            Some((task, entered_at)) => {
                self.delivery_metrics.record_poll_success(&queue);
                Ok(Some(ActivityTaskOffer { task, entered_at }))
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
        self.start_activity_task(&offer.task, offer.entered_at, &worker_identity)
            .await
    }

    async fn pause_activity_for_matching_workflow_rule(
        &self,
        state: &WorkflowState,
        activity: &tokeira_kernel::ActivityState,
        backoff_interval_seconds: Option<i64>,
    ) -> Result<bool> {
        let rules = self.repo.list_workflow_rules(state.namespace_id).await?;
        let now = OffsetDateTime::now_utc();
        let Some(rule) =
            matching_pause_rule(state, activity, &rules, now, backoff_interval_seconds)
        else {
            return Ok(false);
        };
        let rule_id = rule.id.clone();
        self.pause_activities(
            state.run_key,
            PauseActivityRequest {
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
            },
        )
        .await?;
        Ok(true)
    }

    /// Eagerly claim a specific activity task by `(run_key, activity_id)`
    /// without waiting in the long-poll queue, then start it.
    ///
    /// Returns `Ok(None)` when the broker has no matching offer (already
    /// claimed by another poller, or not yet dispatched); the caller treats
    /// that as "no work available" rather than an error. Used by the eager
    /// dispatch path where a worker that just completed a task is handed the
    /// next one directly, skipping a broker round-trip.
    pub async fn try_claim_activity_task(
        &self,
        queue: QueueKey,
        run_key: RunKey,
        activity_id: String,
        worker_identity: WorkerIdentity,
    ) -> Result<Option<StartedActivityTask>> {
        let Some(offered) = self
            .activity_broker
            .try_claim_activity_task(&queue, run_key, &activity_id)
            .await
        else {
            return Ok(None);
        };
        self.delivery_metrics.record_poll_success(&queue);
        self.start_activity_task(&offered.0, offered.1, &worker_identity)
            .await
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
        let activity_id = token.activity_id.clone();
        if let Err(error) = self.validate_activity_token(&token).await {
            runtime_metrics::record_activity_task_completed(OutcomeLabel::Rejected);
            return Err(error);
        }
        let result = self
            .submit_for_owned_shard(
                token.run_key,
                Command::ActivityResolved(ActivityResolvedRequest {
                    activity_id,
                    resolution: ActivityResolution::Completed { result },
                    worker_identity,
                    request,
                    now: OffsetDateTime::now_utc(),
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
    pub async fn fail_activity_task(
        &self,
        token: ActivityTaskToken,
        failure: Payload,
        failure_error_type: Option<String>,
        is_non_retryable: bool,
        worker_identity: Option<WorkerIdentity>,
        request: RequestContext,
    ) -> Result<()> {
        let (activity, workflow_retry_policy) = match self.validate_activity_token(&token).await {
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
        let now = OffsetDateTime::now_utc();
        let expiration = activity
            .schedule_to_close_timeout
            .filter(|timeout| !timeout.is_zero())
            .map(|timeout| activity.scheduled_at + timeout);
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
                    worker_identity.clone(),
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
            .submit_for_owned_shard(
                token.run_key,
                Command::ActivityResolved(ActivityResolvedRequest {
                    activity_id,
                    resolution: ActivityResolution::Failed {
                        failure,
                        retry_state,
                    },
                    worker_identity,
                    request,
                    now: OffsetDateTime::now_utc(),
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
        let activity_id = token.activity_id.clone();
        self.validate_activity_token(&token).await?;
        let result = self
            .submit_for_owned_shard(
                token.run_key,
                Command::ActivityResolved(ActivityResolvedRequest {
                    activity_id,
                    resolution: ActivityResolution::Canceled { details },
                    worker_identity,
                    request,
                    now: OffsetDateTime::now_utc(),
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
            if token.shard_epoch != self.shard_epoch_for_completion(token.run_key).await? {
                return Err(anyhow!("activity heartbeat shard epoch mismatch"));
            }

            let mut next_state = state.clone();
            next_state.transition_seq = state.transition_seq.next();
            let mut next_activity = current;
            next_activity.heartbeat_details = details.clone();
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
                projection_ops: SmallVec::new(),
            };

            let (bundle, commit_epoch) = {
                let owner = self.shard_owner.read().expect("shard_owner lock poisoned");
                let bundle_id = execution_home_bundle(
                    state.namespace_id.0.as_bytes(),
                    state.workflow_id.0.as_bytes(),
                    owner.shard_count(),
                );
                let local_epoch = owner.epoch_of(bundle_id).unwrap_or(ShardEpoch::ZERO);
                let epoch = if self.config.controller_managed_placement {
                    local_epoch
                } else {
                    ShardEpoch::ZERO
                };
                (bundle_id, epoch)
            };

            match self
                .repo
                .commit_transition_for_bundle(token.run_key, bundle, transition, commit_epoch)
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
                        .record_heartbeat(
                            token.run_key,
                            &token.activity_id,
                            OffsetDateTime::now_utc(),
                        )
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
    /// poll time. Fails with [`ActivityTokenResolutionError::ActivityNotStarted`]
    /// if the activity has no `started_event_id`, because a not-yet-started
    /// activity has no completion identity to address.
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
            .shard_epoch_for_completion(run_key)
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
        let mut attempts = 0u32;
        loop {
            let LoadedRun::Existing(state) = self.repo.load_run(run_key).await? else {
                return Err(ActivityTaskNotFound {
                    reason: "run not found for by-id force start",
                }
                .into());
            };
            if !state.is_open() {
                return Err(ActivityTaskNotFound {
                    reason: "workflow closed for by-id force start",
                }
                .into());
            }
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
                shard_epoch: self.shard_epoch_for_completion(run_key).await?,
            };
            if current.started_event_id.is_some() {
                return Ok(token);
            }

            let mut next_state = state.clone();
            next_state.transition_seq = state.transition_seq.next();
            let mut next_activity = current.clone();
            next_activity.stamp += 1;
            let now = OffsetDateTime::now_utc();
            next_activity.started_at = Some(now);
            // The fabricated start carries the completing caller's identity
            // (respondactivitytaskcompleted/api.go:96 @ v1.31.0; K3 makes it
            // durable as `started_identity`).
            next_activity.started_identity = Some(identity.clone());
            let started_event_id = next_state.last_event_id + 1;
            next_state.last_event_id = started_event_id;
            next_activity.started_event_id = Some(started_event_id);
            next_state
                .activities
                .insert(activity_id.to_string(), next_activity.clone());

            let started_event = HistoryEvent {
                event_id: started_event_id,
                happened_at: now,
                kind: HistoryEventKind::ActivityTaskStarted {
                    activity_id: activity_id.to_string(),
                    scheduled_event_id: current.schedule_event_id,
                    attempt: current.attempt,
                    identity: identity.clone(),
                    request_id: format!("activity-start-{}-{}", activity_id, current.attempt),
                    last_failure: current.last_failure.clone(),
                },
            };
            let transition = Transition {
                expected_seq: state.transition_seq,
                next_state,
                history_events: smallvec![started_event],
                event_principals: smallvec![request.principal.clone()],
                request_dedupe_ops: SmallVec::new(),
                activity_ops: smallvec![ActivityOp::Upsert(next_activity.clone())],
                timer_ops: SmallVec::new(),
                dispatch_ops: SmallVec::new(),
                projection_ops: SmallVec::new(),
            };

            // Same commit-epoch rule as start_activity_task: ZERO without a
            // placement controller (no lease to fence), real local epoch under
            // one so storage lease-fences a superseded owner's write.
            let (bundle, commit_epoch) = {
                let owner = self.shard_owner.read().expect("shard_owner lock poisoned");
                let bundle_id = execution_home_bundle(
                    state.namespace_id.0.as_bytes(),
                    state.workflow_id.0.as_bytes(),
                    owner.shard_count(),
                );
                let local_epoch = owner.epoch_of(bundle_id).unwrap_or(ShardEpoch::ZERO);
                let epoch = if self.config.controller_managed_placement {
                    local_epoch
                } else {
                    ShardEpoch::ZERO
                };
                (bundle_id, epoch)
            };

            match self
                .repo
                .commit_transition_for_bundle(run_key, bundle, transition, commit_epoch)
                .await?
            {
                CommitResult::Applied { .. } => {
                    runtime_metrics::record_activity_task_started(OutcomeLabel::Success);
                    self.activity_tracking.record_started(
                        run_key,
                        activity_id,
                        OffsetDateTime::now_utc(),
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
        resolution: tokeira_kernel::NexusResolution,
    ) -> Result<bool> {
        match self
            .submit_for_owned_shard(
                run_key,
                Command::NexusOperationResolved(tokeira_kernel::NexusOperationResolvedRequest {
                    operation_id,
                    scheduled_event_id,
                    resolution,
                    now: OffsetDateTime::now_utc(),
                }),
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(error) if error.to_string().contains("kernel rejected command") => Ok(false),
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
        outcome: tokeira_kernel::NexusCancellationAttemptOutcome,
    ) -> Result<bool> {
        match self
            .submit_for_owned_shard(
                run_key,
                Command::NexusCancellationAttempted(
                    tokeira_kernel::NexusCancellationAttemptedRequest {
                        operation_id,
                        scheduled_event_id,
                        requested_event_id,
                        outcome,
                        now: OffsetDateTime::now_utc(),
                    },
                ),
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(error) if error.to_string().contains("kernel rejected command") => Ok(false),
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
    /// is republished to the broker (reject-and-reschedule) rather than dropped,
    /// so the task is not lost when contention is high.
    async fn start_activity_task(
        &self,
        task: &DispatchableActivityTask,
        entered_at: tokio::time::Instant,
        worker_identity: &WorkerIdentity,
    ) -> Result<Option<StartedActivityTask>> {
        let mut attempts = 0u32;
        loop {
            let LoadedRun::Existing(state) = self.repo.load_run(task.run_key).await? else {
                return Ok(None);
            };
            if !state.is_open() {
                return Ok(None);
            }
            let Some(current) = state.activities.get(&task.activity_id).cloned() else {
                return Ok(None);
            };
            // Durable dispatch rows are deleted by the pause transition, but
            // an ephemeral broker offer may already be in flight. Recheck the
            // authoritative pause bits before starting so that stale offer is
            // consumed without defeating either workflow- or activity-level
            // pause (`PauseActivity`, activity.go:263-284 @ v1.31.0).
            if state.status == ExecutionStatus::Paused || current.pause_info.is_some() {
                return Ok(None);
            }
            if current.attempt != task.attempt
                || current.schedule_event_id != task.schedule_event_id
            {
                return Ok(None);
            }
            // Fence a superseded offer via the activity stamp. Every mutation
            // that invalidates an outstanding dispatch — pause, unpause, reset,
            // and options updates (including a task-queue move or a lengthened
            // retry backoff) — bumps the stamp, so an offer published before the
            // change carries a stale stamp and must not start the activity. This
            // is v1.31.0's `ObsoleteMatchingTask` stamp check
            // (recordactivitytaskstarted/api.go @ v1.31.0).
            if current.stamp != task.stamp {
                return Ok(None);
            }
            if current.started_event_id.is_some() {
                return Ok(None);
            }

            // All delivery shapes converge here: ordinary long-poll and eager claim both call
            // `start_activity_task`. Reading durable rules immediately before Started avoids the
            // frontend gate/poll-admission race and mirrors
            // `recordactivitytaskstarted/api.go:332-372 @ v1.31.0`. The
            // backoff interval is derived from durable state so a
            // `BackoffInterval` rule created after publication but before
            // start is still enforced.
            if self
                .pause_activity_for_matching_workflow_rule(
                    &state,
                    &current,
                    activity_backoff_interval_seconds(&current)?,
                )
                .await?
            {
                return Ok(None);
            }

            if activity_start_rejected_by_in_flight_transition(&state) {
                self.republish_rejected_activity_task(task).await;
                return Ok(None);
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
                let result = self
                    .submit_for_owned_shard(
                        task.run_key,
                        Command::StartDeploymentTransition(
                            tokeira_kernel::StartDeploymentTransitionRequest {
                                target,
                                revision_number: task.dispatch_revision,
                                now: OffsetDateTime::now_utc(),
                            },
                        ),
                    )
                    .await;
                match result {
                    Ok(_) => self.republish_rejected_activity_task(task).await,
                    Err(error)
                        if error.to_string().contains(
                            "pinned workflow cannot start a deployment-version transition",
                        ) =>
                    {
                        // A concurrent update can pin the run after the runtime
                        // made its routing decision. The kernel is the final
                        // authority; once it rejects, this poll is stale and
                        // must not start the activity against the old premise.
                        return Ok(None);
                    }
                    Err(error) => return Err(error),
                }
                return Ok(None);
            }

            let mut next_state = state.clone();
            next_state.transition_seq = state.transition_seq.next();
            let mut next_activity = current.clone();
            next_activity.stamp += 1;
            let now = OffsetDateTime::now_utc();
            next_activity.started_at = Some(now);
            // `ai.StartedIdentity` (kernel raise K3): the polling worker's
            // identity, Describe's primary `LastWorkerIdentity` source
            // (workflow/activity.go:159 @ v1.31.0). May be empty — v1.31.0
            // stores it verbatim and Describe falls back on empty.
            next_activity.started_identity = Some(worker_identity.clone());

            // A retry-policy activity start is transient: v1.31.0 persists
            // mutable start metadata but does not consume a history event until
            // terminal resolution (`AddActivityTaskStartedEvent`,
            // mutable_state_impl.go:4082-4152 @ v1.31.0). A retryable failure
            // clears this marker when advancing the attempt, so neither Started
            // nor Failed appears for an intermediate attempt.
            //
            // A non-retry start remains immediate unless a WFT is running. In
            // that case the worker's history view is frozen, so Started buffers
            // and is flushed before its matching resolution (`bufferEvent`,
            // event_store.go:263 @ v1.31.0; TestBufferedEventsOutOfOrder).
            let started_activity_kind = HistoryEventKind::ActivityTaskStarted {
                activity_id: task.activity_id.clone(),
                scheduled_event_id: current.schedule_event_id,
                attempt: current.attempt,
                identity: worker_identity.clone(),
                request_id: format!("activity-start-{}-{}", task.activity_id, current.attempt),
                last_failure: current.last_failure.clone(),
            };
            let workflow_task_started = state
                .pending_workflow_task
                .as_ref()
                .is_some_and(|pending| pending.started_event_id.is_some());
            let started_history_events = if current.retry_policy.is_some() {
                next_activity.started_event_id =
                    Some(tokeira_kernel::TRANSIENT_ACTIVITY_STARTED_EVENT_ID);
                SmallVec::new()
            } else if workflow_task_started {
                next_activity.started_event_id = Some(tokeira_kernel::BUFFERED_EVENT_ID);
                next_state
                    .buffered_events
                    .push(tokeira_kernel::BufferedEvent {
                        admitted_at: now,
                        kind: started_activity_kind,
                        principal: None,
                    });
                SmallVec::new()
            } else {
                let started_event_id = next_state.last_event_id + 1;
                next_state.last_event_id = started_event_id;
                next_activity.started_event_id = Some(started_event_id);
                smallvec![HistoryEvent {
                    event_id: started_event_id,
                    happened_at: now,
                    kind: started_activity_kind,
                }]
            };

            next_state
                .activities
                .insert(task.activity_id.clone(), next_activity.clone());

            let event_principals = if started_history_events.is_empty() {
                SmallVec::new()
            } else {
                smallvec![None]
            };
            let transition = Transition {
                expected_seq: state.transition_seq,
                next_state,
                history_events: started_history_events,
                event_principals,
                request_dedupe_ops: SmallVec::new(),
                activity_ops: smallvec![ActivityOp::Upsert(next_activity.clone())],
                timer_ops: SmallVec::new(),
                dispatch_ops: SmallVec::new(),
                projection_ops: SmallVec::new(),
            };

            // Mirror the lane's commit-epoch rule (see lane.rs): with no
            // placement controller there is no durable lease to fence against,
            // so commit at ZERO and skip the lease read. Under a controller the
            // real local epoch must travel to storage so a superseded owner's
            // write is rejected by lease fencing — the authoritative ownership
            // check.
            let (bundle, commit_epoch) = {
                let owner = self.shard_owner.read().expect("shard_owner lock poisoned");
                let bundle_id = execution_home_bundle(
                    state.namespace_id.0.as_bytes(),
                    state.workflow_id.0.as_bytes(),
                    owner.shard_count(),
                );
                let local_epoch = owner.epoch_of(bundle_id).unwrap_or(ShardEpoch::ZERO);
                let epoch = if self.config.controller_managed_placement {
                    local_epoch
                } else {
                    ShardEpoch::ZERO
                };
                (bundle_id, epoch)
            };

            match self
                .repo
                .commit_transition_for_bundle(task.run_key, bundle, transition, commit_epoch)
                .await?
            {
                CommitResult::Applied { .. } => {
                    runtime_metrics::record_activity_task_started(OutcomeLabel::Success);
                    self.delivery_metrics
                        .record_latency(&task.queue, entered_at.elapsed());
                    self.activity_tracking.record_started(
                        task.run_key,
                        &next_activity.activity_id,
                        OffsetDateTime::now_utc(),
                    );
                    let origin = WorkerTaskOrigin::from_queue_key(
                        &task.queue,
                        next_activity.task_queue.clone(),
                        WorkerTaskClass::Activity,
                    );
                    return Ok(Some(StartedActivityTask {
                        run_key: task.run_key,
                        run_id: state.run_id,
                        activity_id: next_activity.activity_id.clone(),
                        activity_type: next_activity.activity_type.clone(),
                        task_queue: next_activity.task_queue.clone(),
                        token: ActivityTaskToken {
                            run_key: task.run_key,
                            activity_id: next_activity.activity_id.clone(),
                            schedule_event_id: next_activity.schedule_event_id,
                            attempt: next_activity.attempt,
                            shard_epoch: self.current_shard_epoch(task.run_key).await?,
                        },
                        input: next_activity.input.clone(),
                        attempt: next_activity.attempt,
                        workflow_id: state.workflow_id.0.clone(),
                        workflow_type: state.workflow_type.0.clone(),
                        header: next_activity.header.clone(),
                        retry_policy: next_activity.retry_policy.clone(),
                        heartbeat_details: next_activity.heartbeat_details.clone(),
                        scheduled_time: next_activity.scheduled_at,
                        current_attempt_scheduled_time: next_activity.current_attempt_scheduled_at,
                        started_time: now,
                        schedule_to_close_timeout: next_activity.schedule_to_close_timeout,
                        start_to_close_timeout: next_activity.start_to_close_timeout,
                        heartbeat_timeout: next_activity.heartbeat_timeout,
                        origin,
                    }));
                }
                CommitResult::Conflict { .. } | CommitResult::CurrentExecutionConflict { .. } => {
                    if attempts >= self.config.max_occ_retries {
                        runtime_metrics::record_activity_task_started(OutcomeLabel::Failure);
                        if let Err(error) = self
                            .activity_broker
                            .publish_activity_task(task.clone(), Some(&self.delivery_metrics))
                            .await
                        {
                            tracing::warn!(?error, run_key = ?task.run_key, activity_id = task.activity_id, "failed to republish activity task after start conflict exhaustion");
                        }
                        return Ok(None);
                    }
                    attempts += 1;
                }
                CommitResult::Duplicate => {
                    runtime_metrics::record_activity_task_started(OutcomeLabel::Failure);
                    return Ok(None);
                }
            }
        }
    }

    async fn republish_rejected_activity_task(&self, task: &DispatchableActivityTask) {
        if let Err(error) = self
            .activity_broker
            .publish_activity_task(task.clone(), Some(&self.delivery_metrics))
            .await
        {
            tracing::warn!(
                ?error,
                run_key = ?task.run_key,
                activity_id = task.activity_id,
                "failed to republish activity task rejected during worker-deployment transition"
            );
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
    ) -> Result<(tokeira_kernel::ActivityState, Option<RetryPolicy>)> {
        let LoadedRun::Existing(state) = self.repo.load_run(token.run_key).await? else {
            return Err(ActivityTaskNotFound {
                reason: "run not found for activity token",
            }
            .into());
        };
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
        if token.shard_epoch != self.shard_epoch_for_completion(token.run_key).await? {
            return Err(anyhow!("activity shard epoch mismatch"));
        }
        Ok((activity, state.retry_policy.clone()))
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
        _worker_identity: Option<WorkerIdentity>,
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
        }
    }
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

/// Commit an activity's next attempt and (re)publish its dispatch task after
/// the retry backoff.
///
/// Bumps the attempt, clears the started markers, and commits a transition
/// whose dispatch op re-enqueues the activity task — the reject-and-retry
/// half of activity failure handling, also run by the timeout scanner when a
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
) -> Result<()>
where
    R: RunRepository + 'static,
{
    let run_key = target.run_key;
    let activity_id = target.activity_id;
    let mut attempts = 0u32;
    loop {
        let LoadedRun::Existing(state) = deps.repo.load_run(run_key).await? else {
            return Err(anyhow!("run not found for activity retry"));
        };
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
        let completed_at = OffsetDateTime::now_utc();
        let dispatch_at = completed_at + backoff;
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
        // Reset-heartbeat applies to the NEXT instance, never destructively to
        // a still-running worker. Retry preparation is the point at which
        // v1.31.0 clears those details, then always consumes both reset flags
        // so a later retry cannot repeat the reset side effects
        // (`UpdateActivityInfoForRetries`, activity.go:63-97 @ v1.31.0).
        if next_activity.reset_heartbeats {
            next_activity.heartbeat_details = None;
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
                input: task.input.clone(),
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
            projection_ops: SmallVec::new(),
        };

        // Same commit-epoch rule as start_activity_task: ZERO without a
        // placement controller (no lease to fence), real local epoch under
        // one so storage lease-fences a superseded owner's write.
        let (bundle, commit_epoch) = {
            let owner = deps.shard_owner.read().expect("shard_owner lock poisoned");
            let bundle_id = execution_home_bundle(
                state.namespace_id.0.as_bytes(),
                state.workflow_id.0.as_bytes(),
                owner.shard_count(),
            );
            let local_epoch = owner.epoch_of(bundle_id).unwrap_or(ShardEpoch::ZERO);
            let epoch = if deps.controller_managed_placement {
                local_epoch
            } else {
                ShardEpoch::ZERO
            };
            (bundle_id, epoch)
        };

        match deps
            .repo
            .commit_transition_for_bundle(run_key, bundle, transition, commit_epoch)
            .await?
        {
            CommitResult::Applied { .. } => {
                if paused {
                    deps.tracking.remove(run_key, activity_id);
                    return Ok(());
                }
                deps.tracking
                    .record_retry(run_key, activity_id, dispatch_at);
                let Some(dispatch_task) = dispatch_task else {
                    return Err(anyhow!("dispatchable activity retry missing after commit"));
                };
                if backoff.is_positive() {
                    // Backoff-delayed dispatch: the commit above recorded the
                    // durable eligibility time; this in-memory timer is only
                    // the LOW-LATENCY publication path. The durable dispatch
                    // row is the correctness obligation — if this timer is
                    // lost for any reason, the activity scanner's durable
                    // reconciliation republishes the row once due, mirroring
                    // v1.31.0's retried durable retry timer
                    // (`executeActivityRetryTimerTask`,
                    // timer_queue_active_task_executor.go:522-620 @ v1.31.0).
                    let timer_deps = deps.clone();
                    let activity_id = activity_id.to_string();
                    let delay = std::time::Duration::from_millis(
                        backoff.whole_milliseconds().max(0) as u64,
                    );
                    tokio::spawn(async move {
                        tokio::time::sleep(delay).await;
                        match prepare_activity_dispatch_publish(
                            &timer_deps,
                            &dispatch_task,
                            OffsetDateTime::now_utc(),
                        )
                        .await
                        {
                            Ok(ActivityDispatchPreparation::Publish(dispatch_task)) => {
                                if let Err(error) = timer_deps
                                    .broker
                                    .publish_activity_task(
                                        dispatch_task,
                                        Some(&timer_deps.delivery_metrics),
                                    )
                                    .await
                                {
                                    tracing::warn!(?error, run_key = ?run_key, activity_id, "failed to publish backoff-delayed activity retry");
                                }
                            }
                            Ok(_) => {}
                            Err(error) => {
                                // Rule evaluation is a correctness gate. The durable
                                // row stays; the scanner's reconciliation retries the
                                // publish later. Publishing without the check could
                                // defeat a namespace pause policy.
                                tracing::warn!(?error, run_key = ?run_key, activity_id, "failed to evaluate workflow rules for backoff-delayed activity retry");
                            }
                        }
                    });
                } else if let ActivityDispatchPreparation::Publish(dispatch_task) =
                    prepare_activity_dispatch_publish(
                        deps,
                        &dispatch_task,
                        OffsetDateTime::now_utc(),
                    )
                    .await?
                    && let Err(error) = deps
                        .broker
                        .publish_activity_task(dispatch_task, Some(&deps.delivery_metrics))
                        .await
                {
                    tracing::warn!(?error, run_key = ?run_key, activity_id, "failed to publish retried activity task");
                }
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
        input: activity.input.clone(),
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

/// The classified outcome of preparing one durable activity dispatch for
/// broker publication.
#[derive(Debug)]
pub(crate) enum ActivityDispatchPreparation {
    /// Authoritative state accepts the dispatch: publish this routed task.
    Publish(DispatchableActivityTask),
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

/// One bounded reconciliation pass over a shard's due durable dispatch rows.
///
/// For each row due at `now`: revalidate through the shared preparation gate
/// and publish accepted tasks (broker dedupe on `(run, activity, attempt,
/// stamp)` makes a still-parked offer a no-op, and a taken-but-unstarted
/// offer republishable); conditionally prune rows the gate proves permanently
/// stale, using the exact observed row version so a concurrently replaced
/// live row is untouched — pruning is also what keeps a bounded pass making
/// forward progress instead of re-reading zombie rows forever. Transient
/// errors and not-yet-dispatchable rows are left intact for a later pass;
/// publication failure never removes the durable row.
pub(crate) async fn reconcile_due_activity_dispatches_once<R>(
    deps: &ActivityRetryDeps<R>,
    shard_id: ShardId,
    now: OffsetDateTime,
    limit: usize,
) -> usize
where
    R: RunRepository + 'static,
{
    let due = match deps
        .repo
        .list_due_dispatchable_activity_tasks_for_shard(shard_id, now, limit)
        .await
    {
        Ok(due) => due,
        Err(error) => {
            tracing::warn!(
                ?error,
                shard_id = shard_id.0,
                "failed to list due activity dispatches"
            );
            return 0;
        }
    };
    reconcile_activity_dispatch_candidates(deps, due, now).await
}

/// Run listed due rows through the shared gate; returns how many published.
///
/// Split from [`reconcile_due_activity_dispatches_once`] so shard recovery
/// can list with its own error propagation (a failing sweep query aborts the
/// takeover) while sharing the exact per-candidate semantics.
pub(crate) async fn reconcile_activity_dispatch_candidates<R>(
    deps: &ActivityRetryDeps<R>,
    candidates: Vec<tokeira_storage::DueActivityDispatch>,
    now: OffsetDateTime,
) -> usize
where
    R: RunRepository + 'static,
{
    let mut published = 0usize;
    for candidate in candidates {
        match prepare_activity_dispatch_publish(deps, &candidate.task, now).await {
            Ok(ActivityDispatchPreparation::Publish(task)) => {
                if let Err(error) = deps
                    .broker
                    .publish_activity_task(task, Some(&deps.delivery_metrics))
                    .await
                {
                    tracing::warn!(
                        ?error,
                        run_key = ?candidate.task.run_key,
                        activity_id = candidate.task.activity_id,
                        "failed to publish reconciled activity dispatch"
                    );
                } else {
                    published += 1;
                }
            }
            Ok(
                ActivityDispatchPreparation::SuppressedByRule
                | ActivityDispatchPreparation::NotYetDispatchable,
            ) => {}
            Ok(ActivityDispatchPreparation::PermanentlyStale) => {
                if let Err(error) = deps
                    .repo
                    .delete_activity_dispatch_if_matches(&candidate.identity())
                    .await
                {
                    tracing::warn!(
                        ?error,
                        run_key = ?candidate.task.run_key,
                        activity_id = candidate.task.activity_id,
                        "failed to prune permanently stale activity dispatch row"
                    );
                }
            }
            Err(error) => {
                // Transient: authoritative reads and rule evaluation are
                // correctness gates; the durable row stays for the next pass.
                tracing::warn!(
                    ?error,
                    run_key = ?candidate.task.run_key,
                    activity_id = candidate.task.activity_id,
                    "failed to prepare reconciled activity dispatch"
                );
            }
        }
    }
    published
}

/// Revalidate a durable activity dispatch against authoritative state and
/// apply any workflow rule created since it was recorded.
///
/// The shared publication gate for the backoff timer, the scanner's durable
/// reconciliation, and shard recovery — the local equivalent of v1.31.0's
/// retry-timer executor, which reloads mutable state, validates stamp, pause,
/// attempt, `StartedEventId`, and workflow-running state, evaluates current
/// Workflow Rules, and only then adds the task to matching
/// (`executeActivityRetryTimerTask`,
/// timer_queue_active_task_executor.go:522-620,945-977 @ v1.31.0). Errors are
/// transient: the durable row stays for a later pass, and publication never
/// proceeds past a failed correctness gate.
async fn prepare_activity_dispatch_publish<R>(
    deps: &ActivityRetryDeps<R>,
    task: &DispatchableActivityTask,
    now: OffsetDateTime,
) -> Result<ActivityDispatchPreparation>
where
    R: RunRepository + 'static,
{
    use ActivityDispatchPreparation as Preparation;
    let mut attempts = 0u32;
    loop {
        let LoadedRun::Existing(state) = deps.repo.load_run(task.run_key).await? else {
            return Ok(Preparation::PermanentlyStale);
        };
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
            && current.scheduled_at + timeout <= now
        {
            return Ok(Preparation::NotYetDispatchable);
        }
        if let Some(timeout) = current.schedule_to_start_timeout
            && !timeout.is_zero()
            && dispatch_at + timeout <= now
        {
            return Ok(Preparation::NotYetDispatchable);
        }

        let rules = deps.repo.list_workflow_rules(state.namespace_id).await?;
        let backoff_interval_seconds = activity_backoff_interval_seconds(&current)?;
        let Some(rule) =
            matching_pause_rule(&state, &current, &rules, now, backoff_interval_seconds)
        else {
            let registry = deps
                .worker_deployment_registry
                .read()
                .expect("worker deployment registry lock poisoned")
                .clone();
            let (queue, dispatch_revision) = route_activity_task_queue(
                registry.as_ref(),
                &state,
                task.queue.clone(),
                task.dispatch_revision,
            )
            .await?;
            let mut routed = task.clone();
            routed.queue = queue;
            routed.dispatch_revision = dispatch_revision;
            return Ok(Preparation::Publish(routed));
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
            projection_ops: SmallVec::new(),
        };
        let (bundle, commit_epoch) = {
            let owner = deps.shard_owner.read().expect("shard_owner lock poisoned");
            let bundle_id = execution_home_bundle(
                state.namespace_id.0.as_bytes(),
                state.workflow_id.0.as_bytes(),
                owner.shard_count(),
            );
            let local_epoch = owner.epoch_of(bundle_id).unwrap_or(ShardEpoch::ZERO);
            let epoch = if deps.controller_managed_placement {
                local_epoch
            } else {
                ShardEpoch::ZERO
            };
            (bundle_id, epoch)
        };

        match deps
            .repo
            .commit_transition_for_bundle(task.run_key, bundle, transition, commit_epoch)
            .await?
        {
            CommitResult::Applied { .. } => {
                deps.tracking.remove(task.run_key, &task.activity_id);
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

    async fn seed_started_activity(
        runtime: &TokeiraRuntime<InMemoryStore>,
        repo: &InMemoryStore,
        details: Option<Payloads>,
    ) -> (WorkflowState, ActivityTaskToken, QueueKey) {
        let mut state = open_state(None);
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
            input: payloads(b"input"),
            header: None,
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
            history_events: SmallVec::new(),
            event_principals: SmallVec::new(),
            request_dedupe_ops: SmallVec::new(),
            activity_ops: smallvec![ActivityOp::Upsert(activity.clone())],
            timer_ops: SmallVec::new(),
            dispatch_ops: SmallVec::new(),
            projection_ops: SmallVec::new(),
        };
        repo.commit_transition(run_key, transition, ShardEpoch::ZERO)
            .await
            .expect("seed activity state");
        let token = ActivityTaskToken {
            run_key,
            activity_id: activity.activity_id,
            schedule_event_id: activity.schedule_event_id,
            attempt: activity.attempt,
            shard_epoch: runtime
                .shard_epoch_for_completion(run_key)
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
            input: payloads(b"input"),
            header: None,
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
            input: activity.input.clone(),
            schedule_event_id: activity.schedule_event_id,
            attempt: activity.attempt,
            dispatch_revision: 0,
            stamp: activity.stamp,
            priority: None,
            order: None,
        };
        let transition = Transition {
            expected_seq: TransitionSeq::ZERO,
            next_state: state.clone(),
            history_events: SmallVec::new(),
            event_principals: SmallVec::new(),
            request_dedupe_ops: SmallVec::new(),
            activity_ops: smallvec![ActivityOp::Upsert(activity.clone())],
            timer_ops: SmallVec::new(),
            // Real schedule transitions always carry the durable dispatch op;
            // the row is what reconciliation rediscovers after a lost take.
            dispatch_ops: smallvec![DispatchOp::EnqueueActivityTask {
                queue: queue.clone(),
                activity_id: activity.activity_id.clone(),
                input: activity.input.clone(),
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
            projection_ops: SmallVec::new(),
        };
        repo.commit_transition(run_key, transition, ShardEpoch::ZERO)
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
            BacklogConfig::default(),
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
            BacklogConfig::default(),
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
            BacklogConfig::default(),
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
    async fn ordinary_and_eager_starts_apply_the_same_rule_before_started() {
        for eager in [false, true] {
            let repo = Arc::new(InMemoryStore::default());
            let runtime = TokeiraRuntime::new(
                repo.clone(),
                1,
                LaneConfig::default(),
                TimerScannerConfig::default(),
                WorkflowTimeoutScannerConfig::default(),
                BacklogConfig::default(),
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

            let started = if eager {
                runtime
                    .try_claim_activity_task(
                        queue,
                        task.run_key,
                        task.activity_id.clone(),
                        WorkerIdentity("worker".to_string()),
                    )
                    .await
                    .expect("eager claim")
            } else {
                runtime
                    .poll_activity_task(
                        queue,
                        WorkerIdentity("worker".to_string()),
                        tokio::time::Duration::from_millis(100),
                    )
                    .await
                    .expect("ordinary poll")
            };
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
            BacklogConfig::default(),
        ));
        let (state, queue, task) = seed_scheduled_activity(&runtime, &repo).await;
        runtime
            .activity_broker
            .try_claim_activity_task(&queue, task.run_key, &task.activity_id)
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
                BacklogConfig::default(),
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

    #[tokio::test]
    async fn retry_timer_applies_rule_created_during_backoff_before_publish() {
        let repo = Arc::new(InMemoryStore::default());
        let runtime = TokeiraRuntime::new(
            repo.clone(),
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
            BacklogConfig::default(),
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
        )
        .await
        .expect("commit delayed retry");

        let LoadedRun::Existing(retried) = repo.load_run(token.run_key).await.unwrap() else {
            panic!("seeded run should exist after retry");
        };
        let activity = retried.activities[&token.activity_id].clone();
        let task = DispatchableActivityTask {
            run_key: token.run_key,
            queue: queue.clone(),
            activity_id: activity.activity_id.clone(),
            input: activity.input.clone(),
            schedule_event_id: activity.schedule_event_id,
            attempt: activity.attempt,
            dispatch_revision: 0,
            stamp: activity.stamp,
            priority: None,
            order: None,
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
        assert!(matches!(
            prepare_activity_dispatch_publish(&deps, &task, OffsetDateTime::now_utc() + backoff)
                .await
                .expect("evaluate delayed retry"),
            ActivityDispatchPreparation::SuppressedByRule
        ));

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

    #[tokio::test]
    async fn heartbeat_details_persist_and_return_on_retry_poll() {
        let repo = Arc::new(InMemoryStore::default());
        let runtime = TokeiraRuntime::new(
            repo.clone(),
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
            BacklogConfig::default(),
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

        runtime
            .fail_activity_task(
                token,
                payload(b"retryable"),
                None,
                false,
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
            BacklogConfig::default(),
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
            BacklogConfig::default(),
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

        runtime
            .fail_activity_task(
                token.clone(),
                payload(b"retryable"),
                None,
                false,
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
        assert!(!retried.activity_reset);
        assert!(!retried.reset_heartbeats);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        // Feature: worker-deployments, Property 13: deployment-version transition lifecycle
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
            input: tokeira_types::Payloads::default(),
            header: None,
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
    async fn reconciliation_prunes_stale_head_and_admits_live_row_under_limit_one() {
        let repo = Arc::new(InMemoryStore::default());
        let runtime = TokeiraRuntime::new(
            repo.clone(),
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
            BacklogConfig::default(),
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

        // A zombie row ordered before the live one: a dispatch op whose
        // activity does not exist in any run state, eligibility at epoch so it
        // heads the (dispatch_at, ...) ordering.
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
                input: tokeira_types::Payloads::default(),
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
            projection_ops: Default::default(),
        };
        repo.commit_transition(zombie_run, zombie_transition, ShardEpoch::ZERO)
            .await
            .expect("commit zombie row");

        let deps = runtime.activity_retry_deps();
        let shard_id = tokeira_types::ShardId(0);
        let now = OffsetDateTime::now_utc();
        // Pass 1 (budget one): only the stale head is scanned; it is
        // conditionally pruned rather than retried forever.
        let published = reconcile_due_activity_dispatches_once(&deps, shard_id, now, 1).await;
        assert_eq!(published, 0, "the stale head publishes nothing");
        assert!(
            repo.list_all_dispatchable_activity_tasks(&queue, 10)
                .await
                .expect("inspect rows")
                .iter()
                .all(|task| task.run_key != zombie_run),
            "the stale head must be pruned"
        );
        // Pass 2 (same budget): forward progress — the live row is admitted
        // and republished into the broker.
        let due_after_prune = repo
            .list_due_dispatchable_activity_tasks_for_shard(shard_id, now, 10)
            .await
            .expect("list due rows");
        assert_eq!(
            due_after_prune
                .iter()
                .map(|due| due.task.run_key)
                .collect::<Vec<_>>(),
            vec![state.run_key],
            "after pruning, exactly the live row is due"
        );
        let published = reconcile_due_activity_dispatches_once(&deps, shard_id, now, 1).await;
        assert_eq!(published, 1, "the live row must be admitted after pruning");
        let redelivered = deps
            .broker
            .poll_activity_task(&queue, std::time::Duration::ZERO)
            .await
            .expect("poll broker")
            .expect("republished live offer");
        assert_eq!(redelivered.0.run_key, state.run_key);
    }
}
