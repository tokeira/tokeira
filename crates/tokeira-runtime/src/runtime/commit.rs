//! Commit-path helpers of [`TokeiraRuntime`]: shard-ownership gating and
//! lane submission.
//!
//! This `impl` continuation is the chokepoint every state-changing command
//! passes through on its way to a lane. Its job is to enforce, before any
//! command reaches the kernel, that this node may act on the run's shard —
//! and to expose the shard-epoch lookups that token minting and completion
//! validation depend on.
//!
//! The ownership model here is layered and deliberately distinct:
//! - [`submit`](TokeiraRuntime::submit) is the external entry point. It refuses
//!   externally-routed work while draining and refuses any run whose shard is
//!   not `Active`, so a node that is shedding or has lost a shard stops
//!   admitting new commands.
//! - [`submit_for_owned_shard`](TokeiraRuntime::submit_for_owned_shard) is the
//!   internal entry point for task-completion follow-ups. It accepts a shard in
//!   any owned state (even mid-sweep/drain) as long as ownership is recorded,
//!   so in-flight work can finish a clean handoff.
//!
//! These runtime-local admission checks reject work whose execution home is
//! unavailable and produce `NotShardOwner` for the edge. Storage retains its
//! existing epoch checks; transaction-local competing-owner fencing is separate.
use super::*;
use tokeira_observability::{
    ErrorBiasedSamplingReason, NotShardOwnerOperationLabel, mark_error_biased_sample,
};
use tokeira_types::WorkflowId;

impl<R> TokeiraRuntime<R>
where
    R: RunRepository + 'static,
{
    /// Admit an externally-originated command and submit it to its run's lane.
    ///
    /// Applies two ownership gates before touching a lane, in this order:
    /// 1. If the node is draining and the command is externally-routed (a new
    ///    client request, not in-flight machinery), reject it so the shard can
    ///    reach a clean handoff. In-flight commands are *not* rejected here, by
    ///    design — see `is_externally_routed_command`.
    /// 2. If the run's shard is not `Active`, reject regardless of origin.
    ///
    /// Both produce a [`NotShardOwner`] carrying the current local epoch, which
    /// the edge maps to the retryable not-owner status. Routing is by run key,
    /// so the same run always serializes on the same lane.
    pub async fn submit(&self, run_key: RunKey, command: Command) -> Result<CommitResult> {
        let shard_id = self.command_home(run_key, &command).await?;
        // Drain check precedes the active check on purpose: a draining shard is
        // still Active, so without this an external request would slip through
        // and reopen work the drain is trying to quiesce.
        if self.runtime_drain.is_draining() && is_externally_routed_command(&command) {
            let current_epoch = self
                .shard_owner
                .read()
                .expect("shard_owner lock poisoned")
                .epoch_of(shard_id)
                .unwrap_or(ShardEpoch::ZERO);
            runtime_metrics::record_not_shard_owner(NotShardOwnerOperationLabel::SubmitDrain);
            mark_error_biased_sample(ErrorBiasedSamplingReason::NotShardOwner);
            return Err(NotShardOwner::local(shard_id, current_epoch).into());
        }
        {
            let owner = self.shard_owner.read().expect("shard_owner lock poisoned");
            if !owner.is_active(shard_id) {
                let current_epoch = owner.epoch_of(shard_id).unwrap_or(ShardEpoch::ZERO);
                runtime_metrics::record_not_shard_owner(
                    NotShardOwnerOperationLabel::SubmitInactive,
                );
                mark_error_biased_sample(ErrorBiasedSamplingReason::NotShardOwner);
                return Err(NotShardOwner::local(shard_id, current_epoch).into());
            }
        }
        let lane = self.pick_lane(run_key);
        let lane_id = lane_index_for_run_key(run_key, self.lanes.len());
        let started = std::time::Instant::now();
        let result = lane.submit(run_key, command).await?;
        runtime_metrics::record_lane_submit_duration(lane_id, started.elapsed());
        self.handle_post_commit(run_key, &result);
        Ok(result)
    }

    /// Submit a follow-up command for a shard this node owns, bypassing the
    /// drain and `Active`-state gates that [`submit`](Self::submit) applies.
    ///
    /// This is the path for work generated *by* in-flight execution — activity
    /// resolutions, Nexus resolutions, workflow-task completions. It admits the
    /// command as long as the shard's epoch is recorded at all (owned in any
    /// state), so a sweeping or draining shard can still finish settling its
    /// outstanding tasks. New external work must go through `submit` instead.
    pub(super) async fn submit_for_owned_shard(
        &self,
        run_key: RunKey,
        command: Command,
    ) -> Result<CommitResult> {
        let shard_id = self.command_home(run_key, &command).await?;
        {
            let owner = self.shard_owner.read().expect("shard_owner lock poisoned");
            if owner.epoch_of(shard_id).is_none() {
                runtime_metrics::record_not_shard_owner(
                    NotShardOwnerOperationLabel::SubmitForOwnedShard,
                );
                mark_error_biased_sample(ErrorBiasedSamplingReason::NotShardOwner);
                return Err(NotShardOwner::local(shard_id, ShardEpoch::ZERO).into());
            }
        }
        let lane = self.pick_lane(run_key);
        let lane_id = lane_index_for_run_key(run_key, self.lanes.len());
        let started = std::time::Instant::now();
        let result = lane.submit(run_key, command).await?;
        runtime_metrics::record_lane_submit_duration(lane_id, started.elapsed());
        self.handle_post_commit(run_key, &result);
        Ok(result)
    }

    /// Reconcile runtime-local tracking state after a successful commit.
    ///
    /// Two cleanups, both keyed on the committed `new_state`:
    /// - If the run no longer has a *started* WFT pending, drop its WFT-timeout
    ///   entry so the scanner stops watching a deadline that no longer applies.
    /// - If the run just closed, UNBLOCK any buffered queries into direct
    ///   dispatch (a worker answers them against the closed run by replay,
    ///   per v1.31.0's Unblocked completion state) and drop its WFT-timeout
    ///   entry.
    ///
    /// Only `Applied` results carry a new state; `Duplicate`/`Conflict` leave
    /// tracking untouched because no transition landed.
    pub(super) fn handle_post_commit(&self, run_key: RunKey, result: &CommitResult) {
        if let CommitResult::Applied { new_state } = result {
            let pending = new_state.pending_workflow_task.as_ref();
            let started = pending.and_then(|pending| pending.started_at).is_some();
            // A sticky-dispatched UNSTARTED task is still being watched — for
            // its schedule-to-start deadline (sticky raise S2/S3; the lane's
            // post-commit hook owns that entry). Only an unstarted task with
            // no such deadline has nothing to time out.
            let s2s_armed = pending
                .map(|pending| {
                    pending.started_at.is_none() && pending.schedule_to_start_deadline.is_some()
                })
                .unwrap_or(false);
            if !started && !s2s_armed {
                self.wft_timeout_tracking.remove(run_key);
            }
            if new_state.closed_at.is_some() {
                // Buffered queries are UNBLOCKED at close, not failed:
                // v1.31.0 marks every unanswered buffered query Unblocked
                // whenever the completing WFT creates no successor —
                // explicitly including workflow close — and re-dispatches it
                // directly so a worker answers it against the closed run by
                // replay (handleBufferedQueries,
                // respondworkflowtaskcompleted/api.go:1010-1029 +
                // queryworkflow/api.go:242-260 @ v1.31.0). Sticky affinity is
                // cleared at close, so these deliver with full history.
                let unblocked = self.buffered_queries.drain_all(run_key);
                if !unblocked.is_empty() {
                    let broker = self.broker.clone();
                    let queue = QueueKey {
                        namespace_id: new_state.namespace_id,
                        task_queue: new_state.task_queue.clone(),
                        task_kind: TaskKind::Workflow,
                        deployment: new_state.deployment.clone(),
                        build_id: new_state.build_id.clone(),
                    };
                    let origin = WorkerTaskOrigin::from_queue_key(
                        &queue,
                        new_state.task_queue.clone(),
                        tokeira_types::WorkerTaskClass::Query,
                    );
                    tokio::spawn(async move {
                        for query in unblocked {
                            broker
                                .publish_query_task(crate::query::QueryTask {
                                    run_key,
                                    query_type: query.query_type,
                                    query_args: query.query_args,
                                    queue: queue.clone(),
                                    sticky_preferred: None,
                                    sticky_queue: None,
                                    sticky_deadline: None,
                                    deadline: query.deadline,
                                    response_tx: query.response_tx,
                                    origin: origin.clone(),
                                })
                                .await;
                        }
                    });
                }
                self.wft_timeout_tracking.remove(run_key);
            }
        }
    }

    /// The epoch of the loaded state's execution home, requiring it to be
    /// `Active`.
    ///
    /// Used when minting task tokens for *newly started* work: a token must
    /// carry an epoch only while the shard is fully active, so it cannot be
    /// handed out mid-sweep or mid-drain. Errors with [`NotShardOwner`] when the
    /// shard is not actively owned. Contrast
    /// [`shard_epoch_for_completion`](Self::shard_epoch_for_completion), which
    /// is laxer because it validates *already-issued* tokens.
    pub(super) async fn current_shard_epoch(&self, state: &WorkflowState) -> Result<ShardEpoch> {
        let shard_id = self.execution_home(state);
        let owner = self.shard_owner.read().expect("shard_owner lock poisoned");
        owner.owns(shard_id).ok_or_else(|| {
            runtime_metrics::record_not_shard_owner(NotShardOwnerOperationLabel::CurrentShardEpoch);
            mark_error_biased_sample(ErrorBiasedSamplingReason::NotShardOwner);
            NotShardOwner::local(
                shard_id,
                owner.epoch_of(shard_id).unwrap_or(ShardEpoch::ZERO),
            )
            .into()
        })
    }

    /// The epoch of the loaded state's execution home, accepting any owned state.
    ///
    /// Used to validate completions of work that was already started: it reads
    /// the epoch via `epoch_of`, which still returns a value while the shard is
    /// `Draining`. That is deliberate — a worker that polled before the shard
    /// began draining must still be able to report its result during the drain
    /// window, so completion validation must not require `Active`. Errors with
    /// [`NotShardOwner`] only when the shard is not owned at all.
    pub(super) async fn shard_epoch_for_completion(
        &self,
        state: &WorkflowState,
    ) -> Result<ShardEpoch> {
        self.epoch_for_completion_home(self.execution_home(state))
    }

    /// Validate an already-resolved home without loading the run again.
    pub(super) fn epoch_for_completion_home(&self, shard_id: ShardId) -> Result<ShardEpoch> {
        let owner = self.shard_owner.read().expect("shard_owner lock poisoned");
        owner.epoch_of(shard_id).ok_or_else(|| {
            runtime_metrics::record_not_shard_owner(
                NotShardOwnerOperationLabel::ShardEpochForCompletion,
            );
            mark_error_biased_sample(ErrorBiasedSamplingReason::NotShardOwner);
            NotShardOwner::local(shard_id, ShardEpoch::ZERO).into()
        })
    }

    /// Stable execution identity, never the run hash used for local lane choice.
    pub(super) fn execution_home(&self, state: &WorkflowState) -> ShardId {
        self.home_for_identity(state.namespace_id, &state.workflow_id)
    }

    fn home_for_identity(&self, namespace_id: NamespaceId, workflow_id: &WorkflowId) -> ShardId {
        tokeira_types::execution_home_bundle(
            namespace_id.0.as_bytes(),
            workflow_id.0.as_bytes(),
            self.shard_owner
                .read()
                .expect("shard_owner lock poisoned")
                .shard_count(),
        )
    }

    async fn command_home(&self, run_key: RunKey, command: &Command) -> Result<ShardId> {
        // New executions have no stored identity. Every other command shares
        // the lane's ordinary loaded state instead of reading a second copy.
        match command {
            Command::Start(start) => {
                Ok(self.home_for_identity(start.namespace_id, &start.workflow_id))
            }
            Command::StartAndUpdate(request) => {
                Ok(self.home_for_identity(request.start.namespace_id, &request.start.workflow_id))
            }
            Command::SignalWithStart(request) => {
                Ok(self.home_for_identity(request.namespace_id, &request.workflow_id))
            }
            _ => self.shard_id_for(run_key).await,
        }
    }

    /// Resolve identity through the lane's cache so routing shares its cold load.
    pub(super) async fn shard_id_for(&self, run_key: RunKey) -> Result<ShardId> {
        let (namespace, workflow) = self.pick_lane(run_key).execution_identity(run_key).await?;
        Ok(self.home_for_identity(namespace, &workflow))
    }
}
