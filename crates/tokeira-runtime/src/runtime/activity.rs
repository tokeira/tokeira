use super::*;
use tokeira_observability::OutcomeLabel;

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
            CommitResult::Conflict { .. } => {
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

        let should_retry = retry_policy.as_ref().map(|policy| {
            evaluate_activity_retry(
                policy,
                activity.attempt,
                if is_non_retryable {
                    Some("__tokeira_non_retryable__")
                } else {
                    failure_error_type.as_deref()
                },
            )
        });

        if let Some(RetryDecision::Retry { next_attempt }) = should_retry {
            match self.retry_activity_task(&token, next_attempt).await {
                Ok(()) => runtime_metrics::record_activity_task_retry(OutcomeLabel::Success),
                Err(error) => {
                    runtime_metrics::record_activity_task_retry(OutcomeLabel::Failure);
                    return Err(error);
                }
            }
            return Ok(());
        }

        let result = self
            .submit_for_owned_shard(
                token.run_key,
                Command::ActivityResolved(ActivityResolvedRequest {
                    activity_id,
                    resolution: ActivityResolution::Failed { failure },
                    worker_identity,
                    now: OffsetDateTime::now_utc(),
                }),
            )
            .await;
        match &result {
            Ok(CommitResult::Applied { .. } | CommitResult::Duplicate) => {
                runtime_metrics::record_activity_task_failed(OutcomeLabel::Success);
            }
            Ok(CommitResult::Conflict { .. }) | Err(_) => {
                runtime_metrics::record_activity_task_failed(OutcomeLabel::Failure);
            }
        }
        result?;
        self.activity_tracking
            .remove(token.run_key, &token.activity_id);
        Ok(())
    }

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

    pub async fn record_activity_heartbeat(
        &self,
        token: ActivityTaskToken,
        details: Option<Payloads>,
    ) -> Result<bool> {
        // Heartbeat details cross the runtime boundary for API parity, but the
        // current in-memory tracker only stores liveness/cancel state.
        let _ = details;
        self.validate_activity_token(&token).await?;
        Ok(self
            .activity_tracking
            .record_heartbeat(token.run_key, &token.activity_id, OffsetDateTime::now_utc())
            .unwrap_or(false))
    }

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

    /// Atomically transition a polled workflow task into the Started state.
    ///
    /// Sets a sticky TTL so subsequent tasks for this run are preferentially
    /// routed back to the same worker, avoiding full-history replay when the
    /// worker's cache is still warm.
    async fn start_activity_task(
        &self,
        task: &DispatchableActivityTask,
        entered_at: tokio::time::Instant,
        _worker_identity: &WorkerIdentity,
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

            let mut next_state = state.clone();
            next_state.transition_seq = state.transition_seq.next();
            let mut next_activity = current.clone();
            next_activity.stamp += 1;
            let now = OffsetDateTime::now_utc();
            next_activity.started_at = Some(now);

            // Emit ActivityTaskStarted so the SDK's activity state machine
            // sees the required Scheduled → Started → Completed sequence.
            let started_event_id = next_state.last_event_id + 1;
            next_state.last_event_id = started_event_id;
            next_activity.started_event_id = Some(started_event_id);

            next_state
                .activities
                .insert(task.activity_id.clone(), next_activity.clone());

            let started_event = HistoryEvent {
                event_id: started_event_id,
                happened_at: now,
                kind: HistoryEventKind::ActivityTaskStarted {
                    activity_id: task.activity_id.clone(),
                    scheduled_event_id: current.schedule_event_id,
                    attempt: current.attempt,
                    identity: _worker_identity.clone(),
                    request_id: format!("activity-start-{}-{}", task.activity_id, current.attempt),
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
                        schedule_to_close_timeout: next_activity.schedule_to_close_timeout,
                        start_to_close_timeout: next_activity.start_to_close_timeout,
                        heartbeat_timeout: next_activity.heartbeat_timeout,
                    }));
                }
                CommitResult::Conflict { .. } => {
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

    async fn validate_activity_token(
        &self,
        token: &ActivityTaskToken,
    ) -> Result<(tokeira_kernel::ActivityState, Option<RetryPolicy>)> {
        let LoadedRun::Existing(state) = self.repo.load_run(token.run_key).await? else {
            return Err(anyhow!("run not found for activity token"));
        };
        let Some(activity) = state.activities.get(&token.activity_id).cloned() else {
            return Err(anyhow!("activity not found for token"));
        };
        if activity.schedule_event_id != token.schedule_event_id {
            return Err(anyhow!("activity schedule_event_id mismatch"));
        }
        if activity.attempt != token.attempt {
            return Err(anyhow!("activity attempt mismatch"));
        }
        if token.shard_epoch != self.shard_epoch_for_completion(token.run_key).await? {
            return Err(anyhow!("activity shard epoch mismatch"));
        }
        Ok((activity, state.retry_policy.clone()))
    }

    async fn retry_activity_task(
        &self,
        token: &ActivityTaskToken,
        next_attempt: u32,
    ) -> Result<()> {
        let mut attempts = 0u32;
        loop {
            let LoadedRun::Existing(state) = self.repo.load_run(token.run_key).await? else {
                return Err(anyhow!("run not found for activity retry"));
            };
            let Some(current) = state.activities.get(&token.activity_id).cloned() else {
                return Err(anyhow!("activity not found for retry"));
            };
            if current.attempt != token.attempt
                || current.schedule_event_id != token.schedule_event_id
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
            next_state
                .activities
                .insert(token.activity_id.clone(), next_activity.clone());

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
                run_key: token.run_key,
                queue: queue.clone(),
                activity_id: next_activity.activity_id.clone(),
                input: next_activity.input.clone(),
                schedule_event_id: next_activity.schedule_event_id,
                attempt: next_activity.attempt,
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
                    schedule_to_close_timeout: next_activity.schedule_to_close_timeout,
                    schedule_to_start_timeout: next_activity.schedule_to_start_timeout,
                    start_to_close_timeout: next_activity.start_to_close_timeout,
                    heartbeat_timeout: next_activity.heartbeat_timeout,
                }],
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
                CommitResult::Applied { .. } => {
                    if let Err(error) = self
                        .activity_broker
                        .publish_activity_task(dispatch_task, Some(&self.delivery_metrics))
                        .await
                    {
                        tracing::warn!(?error, run_key = ?token.run_key, activity_id = token.activity_id, "failed to publish retried activity task");
                    }
                    self.activity_tracking.record_retry(
                        token.run_key,
                        &token.activity_id,
                        OffsetDateTime::now_utc(),
                    );
                    return Ok(());
                }
                CommitResult::Conflict { .. } => {
                    if attempts >= self.config.max_occ_retries {
                        return Err(anyhow!("activity retry OCC exhausted"));
                    }
                    attempts += 1;
                }
                CommitResult::Duplicate => return Ok(()),
            }
        }
    }
}
