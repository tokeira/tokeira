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
};
use tokeira_observability::OutcomeLabel;
use tokeira_types::TaskKind;

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
    /// Long-poll for an activity task, then atomically mark it as started.
    pub async fn poll_activity_task(
        &self,
        queue: QueueKey,
        worker_identity: WorkerIdentity,
        timeout_after: tokio::time::Duration,
    ) -> Result<Option<StartedActivityTask>> {
        let offered = match self
            .activity_broker
            .poll_activity_task(&queue, timeout_after)
            .await?
        {
            Some(offered) => {
                self.delivery_metrics.record_poll_success(&queue);
                offered
            }
            None => {
                self.delivery_metrics.record_poll_timeout(&queue);
                return Ok(None);
            }
        };

        self.start_activity_task(&offered.0, offered.1, &worker_identity)
            .await
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

    /// Record an activity heartbeat, returning whether cancellation has been
    /// requested for this activity.
    ///
    /// `Ok(true)` tells the worker to begin cooperative cancellation. The
    /// heartbeat persists progress on durable activity state without emitting
    /// history, matching Temporal's mutable activity-info update
    /// (`service/history/workflow/mutable_state_impl.go:1956 @ v1.31.0`).
    /// Volatile tracking remains responsible only for timeout and cooperative
    /// cancellation liveness.
    pub async fn record_activity_heartbeat(
        &self,
        token: ActivityTaskToken,
        details: Option<Payloads>,
        identity: Option<WorkerIdentity>,
    ) -> Result<bool> {
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

            let cancel_requested = next_activity.cancel_requested;
            let transition = Transition {
                expected_seq: state.transition_seq,
                next_state,
                history_events: SmallVec::new(),
                request_dedupe_ops: SmallVec::new(),
                activity_ops: smallvec![ActivityOp::Upsert(next_activity)],
                timer_ops: SmallVec::new(),
                dispatch_ops: SmallVec::new(),
                projection_ops: SmallVec::new(),
            };

            let (bundle, commit_epoch) = {
                let owner = self.shard_owner.read().unwrap();
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
                    return Ok(cancel_requested || tracking_cancel);
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
    /// shape to [`Self::start_activity_task`]'s; a stale broker offer for
    /// this activity dies at claim time against `started_event_id`.
    pub async fn force_start_activity_for_completion(
        &self,
        run_key: RunKey,
        activity_id: &str,
        identity: WorkerIdentity,
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
                let owner = self.shard_owner.read().unwrap();
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
            if current.attempt != task.attempt
                || current.schedule_event_id != task.schedule_event_id
            {
                return Ok(None);
            }
            if current.started_event_id.is_some() {
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
                deployment: state.deployment.clone(),
                build_id: state.build_id.clone(),
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

            // Emit ActivityTaskStarted so the SDK's activity state machine
            // sees the required Scheduled → Started → Completed sequence — UNLESS
            // a workflow task is currently started, in which case the event
            // buffers: the worker's history view is frozen, so it is held on
            // state and flushed (reordered before its resolution) when the WFT
            // closes (`bufferEvent`, event_store.go:263 @ v1.31.0;
            // TestBufferedEventsOutOfOrder). The activity task still dispatches —
            // its token is keyed on the scheduled event id, not the started id —
            // and the matching resolution buffers in the kernel, wired to this
            // event's real id at flush.
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
            let started_history_events = if workflow_task_started {
                next_activity.started_event_id = Some(tokeira_kernel::BUFFERED_EVENT_ID);
                next_state
                    .buffered_events
                    .push(tokeira_kernel::BufferedEvent {
                        admitted_at: now,
                        kind: started_activity_kind,
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

            let transition = Transition {
                expected_seq: state.transition_seq,
                next_state,
                history_events: started_history_events,
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
                let owner = self.shard_owner.read().unwrap();
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
                        workflow_namespace: state.namespace_id.0.to_string(),
                        header: next_activity.header.clone(),
                        retry_policy: next_activity.retry_policy.clone(),
                        heartbeat_details: next_activity.heartbeat_details.clone(),
                        scheduled_time: next_activity.scheduled_at,
                        current_attempt_scheduled_time: next_activity.current_attempt_scheduled_at,
                        started_time: now,
                        schedule_to_close_timeout: next_activity.schedule_to_close_timeout,
                        start_to_close_timeout: next_activity.start_to_close_timeout,
                        heartbeat_timeout: next_activity.heartbeat_timeout,
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
        // The next attempt is scheduled AFTER the retry backoff — v1.31.0
        // advances ScheduledTime to now+backoff and dispatches on a retry
        // timer, not immediately (`UpdateActivityInfoForRetries`,
        // activity.go:74 + GenerateActivityRetryTasks @ v1.31.0).
        let dispatch_at = OffsetDateTime::now_utc() + backoff;
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
        next_state
            .activities
            .insert(activity_id.to_string(), next_activity.clone());

        let queue = QueueKey {
            namespace_id: state.namespace_id,
            task_queue: next_activity.task_queue.clone(),
            task_kind: tokeira_types::TaskKind::Activity,
            deployment: next_activity
                .deployment
                .clone()
                .or_else(|| state.deployment.clone()),
            build_id: next_activity
                .build_id
                .clone()
                .or_else(|| state.build_id.clone()),
        };
        let dispatch_task = DispatchableActivityTask {
            run_key,
            queue: queue.clone(),
            activity_id: next_activity.activity_id.clone(),
            input: next_activity.input.clone(),
            schedule_event_id: next_activity.schedule_event_id,
            attempt: next_activity.attempt,
            dispatch_revision: state
                .versioning_info
                .as_ref()
                .map(|info| info.revision_number)
                .unwrap_or_default(),
        };
        let transition = Transition {
            expected_seq: state.transition_seq,
            next_state,
            history_events: SmallVec::new(),
            request_dedupe_ops: SmallVec::new(),
            activity_ops: smallvec![ActivityOp::Upsert(next_activity.clone())],
            timer_ops: SmallVec::new(),
            dispatch_ops: smallvec![DispatchOp::EnqueueActivityTask {
                queue,
                activity_id: next_activity.activity_id.clone(),
                input: next_activity.input.clone(),
                schedule_event_id: next_activity.schedule_event_id,
                attempt: next_activity.attempt,
                dispatch_revision: state
                    .versioning_info
                    .as_ref()
                    .map(|info| info.revision_number)
                    .unwrap_or_default(),
                schedule_to_close_timeout: next_activity.schedule_to_close_timeout,
                schedule_to_start_timeout: next_activity.schedule_to_start_timeout,
                start_to_close_timeout: next_activity.start_to_close_timeout,
                heartbeat_timeout: next_activity.heartbeat_timeout,
            }],
            projection_ops: SmallVec::new(),
        };

        // Same commit-epoch rule as start_activity_task: ZERO without a
        // placement controller (no lease to fence), real local epoch under
        // one so storage lease-fences a superseded owner's write.
        let (bundle, commit_epoch) = {
            let owner = deps.shard_owner.read().unwrap();
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
                deps.tracking
                    .record_retry(run_key, activity_id, dispatch_at);
                if backoff.is_positive() {
                    // Backoff-delayed dispatch: the commit above already
                    // recorded the future schedule time durably; the publish
                    // itself waits out the backoff (v1.31.0 uses a retry
                    // timer task for this window). Recovery note: a process
                    // restart inside the window relies on the timeout
                    // scanner's schedule anchors rather than this in-memory
                    // timer.
                    let broker = deps.broker.clone();
                    let metrics = deps.delivery_metrics.clone();
                    let activity_id = activity_id.to_string();
                    let delay = std::time::Duration::from_millis(
                        backoff.whole_milliseconds().max(0) as u64,
                    );
                    tokio::spawn(async move {
                        tokio::time::sleep(delay).await;
                        if let Err(error) = broker
                            .publish_activity_task(dispatch_task, Some(&metrics))
                            .await
                        {
                            tracing::warn!(?error, run_key = ?run_key, activity_id, "failed to publish backoff-delayed activity retry");
                        }
                    });
                } else if let Err(error) = deps
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
    use tokeira_storage::InMemoryStore;
    use tokeira_types::{
        BuildId as RuntimeBuildId, DeploymentId, LogicalTaskSeq, Memo, NamespaceId, Payload,
        SearchAttributes, TaskKind, TaskQueueName, WorkflowId, WorkflowType,
    };

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
            cancel_requested: false,
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

        let cancel_requested = runtime
            .record_activity_heartbeat(token.clone(), Some(details.clone()), None)
            .await
            .expect("heartbeat should persist");
        assert!(!cancel_requested);
        let LoadedRun::Existing(after_heartbeat) = repo.load_run(token.run_key).await.unwrap()
        else {
            panic!("seeded run should exist");
        };
        assert_eq!(
            after_heartbeat.activities["activity-1"].heartbeat_details,
            Some(details.clone())
        );

        runtime
            .fail_activity_task(token, payload(b"retryable"), None, false, None)
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
}
