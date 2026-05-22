use super::*;
use tokeira_observability::{
    ErrorBiasedSamplingReason, NotShardOwnerOperationLabel, mark_error_biased_sample,
};

impl<R> TokeiraRuntime<R>
where
    R: RunRepository + 'static,
{
    pub async fn submit(&self, run_key: RunKey, command: Command) -> Result<CommitResult> {
        let shard_id = self.shard_id_for(run_key).await;
        if self.runtime_drain.is_draining() && is_externally_routed_command(&command) {
            let current_epoch = self
                .shard_owner
                .read()
                .unwrap()
                .epoch_of(shard_id)
                .unwrap_or(ShardEpoch::ZERO);
            runtime_metrics::record_not_shard_owner(NotShardOwnerOperationLabel::SubmitDrain);
            mark_error_biased_sample(ErrorBiasedSamplingReason::NotShardOwner);
            return Err(NotShardOwner::local(shard_id, current_epoch).into());
        }
        {
            let owner = self.shard_owner.read().unwrap();
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

    pub(super) async fn submit_for_owned_shard(
        &self,
        run_key: RunKey,
        command: Command,
    ) -> Result<CommitResult> {
        let shard_id = self.shard_id_for(run_key).await;
        {
            let owner = self.shard_owner.read().unwrap();
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

    pub(super) fn handle_post_commit(&self, run_key: RunKey, result: &CommitResult) {
        if let CommitResult::Applied { new_state } = result {
            if new_state
                .pending_workflow_task
                .as_ref()
                .and_then(|pending| pending.started_at)
                .is_none()
            {
                self.wft_timeout_tracking.remove(run_key);
            }
            if new_state.closed_at.is_some() {
                self.buffered_queries
                    .fail_run_queries(run_key, "workflow execution completed");
                self.wft_timeout_tracking.remove(run_key);
            }
        }
    }

    pub(super) async fn current_shard_epoch(&self, run_key: RunKey) -> Result<ShardEpoch> {
        let shard_id = self.shard_id_for(run_key).await;
        let owner = self.shard_owner.read().unwrap();
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

    pub(super) async fn shard_epoch_for_completion(&self, run_key: RunKey) -> Result<ShardEpoch> {
        let shard_id = self.shard_id_for(run_key).await;
        let owner = self.shard_owner.read().unwrap();
        owner.epoch_of(shard_id).ok_or_else(|| {
            runtime_metrics::record_not_shard_owner(
                NotShardOwnerOperationLabel::ShardEpochForCompletion,
            );
            mark_error_biased_sample(ErrorBiasedSamplingReason::NotShardOwner);
            NotShardOwner::local(shard_id, ShardEpoch::ZERO).into()
        })
    }

    pub(super) async fn shard_id_for(&self, run_key: RunKey) -> ShardId {
        let shard_count = self.shard_owner.read().unwrap().shard_count();
        shard_for(run_key, shard_count)
    }
}
