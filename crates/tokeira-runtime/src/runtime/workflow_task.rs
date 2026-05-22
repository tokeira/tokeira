use super::*;
use tokeira_observability::OutcomeLabel;

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
        let task = self
            .started_workflow_task_from_state(new_state, true)
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
        }
    }

    pub async fn try_claim_workflow_task(
        &self,
        queue: QueueKey,
        run_key: RunKey,
        worker_identity: WorkerIdentity,
    ) -> Result<Option<StartedWorkflowTask>> {
        let Some(offered) = self.broker.try_claim_workflow_task(&queue, run_key).await else {
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
        let result = self
            .submit_for_owned_shard(run_key, Command::WorkflowTaskCompleted(req))
            .await;
        match &result {
            Ok(CommitResult::Applied { .. } | CommitResult::Duplicate) => {
                runtime_metrics::record_workflow_task_completed(OutcomeLabel::Success);
            }
            Ok(CommitResult::Conflict { .. }) | Err(_) => {
                runtime_metrics::record_workflow_task_completed(OutcomeLabel::Failure);
            }
        }
        result
    }

    /// Atomically transition a polled workflow task into the Started state.
    async fn start_polled_workflow_task(
        &self,
        offered: DispatchableWorkflowTask,
        entered_at: tokio::time::Instant,
        worker_identity: WorkerIdentity,
    ) -> Result<StartedWorkflowTask> {
        let now = OffsetDateTime::now_utc();
        let request = StartWorkflowTaskRequest {
            logical_seq: offered.logical_seq,
            worker_identity: worker_identity.clone(),
            request_id: uuid::Uuid::new_v4().to_string(),
            history_size_bytes: 0,
            suggest_continue_as_new: false,
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
            run_key: new_state.run_key,
            shard_id,
            logical_seq: pending.logical_seq,
            started_event_id,
            started_at: pending.started_at.unwrap_or(now),
            workflow_task_timeout: new_state.workflow_task_timeout,
        });
        self.delivery_metrics
            .record_latency(&offered.queue, entered_at.elapsed());

        Ok(StartedWorkflowTask {
            run_key: new_state.run_key,
            workflow_id: new_state.workflow_id,
            task_queue: new_state.task_queue,
            previous_started_event_id: new_state.previous_started_event_id,
            is_sticky_match: offered.sticky_preferred.as_ref() == Some(&worker_identity),
            scheduled_time: pending.scheduled_at,
            started_time: pending.started_at.unwrap_or(now),
            token,
        })
    }

    async fn started_workflow_task_from_state(
        &self,
        state: &WorkflowState,
        is_sticky_match: bool,
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
            run_key: state.run_key,
            shard_id,
            logical_seq: pending.logical_seq,
            started_event_id,
            started_at,
            workflow_task_timeout: state.workflow_task_timeout,
        });
        Ok(StartedWorkflowTask {
            run_key: state.run_key,
            workflow_id: state.workflow_id.clone(),
            task_queue: state.task_queue.clone(),
            previous_started_event_id: state.previous_started_event_id,
            is_sticky_match,
            scheduled_time: pending.scheduled_at,
            started_time: started_at,
            token,
        })
    }
}
