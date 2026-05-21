use super::*;

impl<R> TokeiraRuntime<R>
where
    R: RunRepository + 'static,
{
    pub fn record_self_assigned_shard(&self, shard_id: ShardId, epoch: ShardEpoch) {
        let mut owner = self.shard_owner.write().unwrap();
        let _ = owner.record_acquired(shard_id, epoch);
        owner.mark_active(shard_id);
    }
    pub async fn acquire_shard(&self, shard_id: ShardId) -> Result<ShardEpoch>
    where
        R: LeaseRepository,
    {
        let outcome = self
            .repo
            .try_acquire_bundle(
                shard_id,
                self.owner_identity.clone(),
                self.node_endpoint.clone(),
            )
            .await?;
        let epoch = match outcome {
            LeaseOutcome::Acquired { epoch } => epoch,
            LeaseOutcome::Rejected { .. } => {
                return Err(lease_rejected_error(shard_id));
            }
            LeaseOutcome::Renewed { epoch } => epoch,
        };

        let cancel = {
            let mut owner = self.shard_owner.write().unwrap();
            owner.record_acquired(shard_id, epoch)
        };

        let (lost_tx, lost_rx) = oneshot::channel();
        tokio::spawn(run_lease_renewer(
            self.repo.clone(),
            shard_id,
            self.owner_identity.clone(),
            self.node_endpoint.clone(),
            epoch,
            tokio::time::Duration::from_secs(1),
            3,
            cancel.clone(),
            lost_tx,
        ));

        sweep_shard(
            shard_id,
            self.repo.as_ref(),
            &self.broker,
            &self.activity_broker,
            &self.lanes,
            self.lanes.len(),
            &self.workflow_timeout_tracking,
            &self.wft_timeout_tracking,
            &self.activity_tracking,
            &self.nexus_timeout_tracking,
        )
        .await?;

        self.shard_owner.write().unwrap().mark_active(shard_id);

        let shard_owner = self.shard_owner.clone();
        let workflow_timeout_tracking = self.workflow_timeout_tracking.clone();
        let wft_timeout_tracking = self.wft_timeout_tracking.clone();
        let activity_tracking = self.activity_tracking.clone();
        let nexus_timeout_tracking = self.nexus_timeout_tracking.clone();
        tokio::spawn(async move {
            if lost_rx.await.is_ok() {
                let mut owner = shard_owner.write().unwrap();
                owner.mark_draining(shard_id);
                drop(owner);
                workflow_timeout_tracking.remove_all_for_shard(shard_id);
                wft_timeout_tracking.remove_all_for_shard(shard_id);
                activity_tracking.remove_all_for_shard(shard_id);
                nexus_timeout_tracking.remove_all_for_shard(shard_id);
            }
        });

        Ok(epoch)
    }

    pub async fn relinquish_shard(&self, shard_id: ShardId) {
        self.shard_owner.write().unwrap().mark_draining(shard_id);
        self.workflow_timeout_tracking
            .remove_all_for_shard(shard_id);
        self.wft_timeout_tracking.remove_all_for_shard(shard_id);
        self.activity_tracking.remove_all_for_shard(shard_id);
        self.nexus_timeout_tracking.remove_all_for_shard(shard_id);
        self.shard_owner.write().unwrap().remove(shard_id);
    }
}

impl<R> TokeiraRuntime<R>
where
    R: RunRepository + LeaseRepository + 'static,
{
    pub fn spawn_membership_client(
        &self,
        config: MembershipConfig,
        budget_applier: Arc<dyn ConnectionBudgetApplier>,
        shutdown: CancellationToken,
    ) -> tokio::task::JoinHandle<Result<()>> {
        let client = MembershipClient::new(
            config,
            self.repo.clone(),
            self.shard_owner.clone(),
            self.runtime_drain.clone(),
            budget_applier,
        );
        tokio::spawn(client.run(shutdown))
    }
}
