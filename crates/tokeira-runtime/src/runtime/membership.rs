//! Membership and shard-handoff methods of [`TokeiraRuntime`].
//!
//! This `impl` continuation wires the runtime into placement: acquiring and
//! relinquishing shard leases, spawning the lease renewer that detects fencing,
//! and starting the membership client that streams to the placement controller.
//! It is the bridge between durable lease ownership (the authority) and the
//! node-local [`ShardOwner`] view that admission and token minting consult.
//!
//! The ordering in [`acquire_shard`](TokeiraRuntime::acquire_shard) is the
//! correctness-critical part and is enforced deliberately: a freshly acquired
//! shard is recorded in `Sweeping`, its volatile delivery state is rebuilt by
//! `sweep_shard`, and only *then* is it marked `Active`. This guarantees no
//! command is admitted against a shard whose in-memory dispatch/timeout state
//! has not yet been reconstructed from durable history.
use super::*;

impl<R> TokeiraRuntime<R>
where
    R: RunRepository + 'static,
{
    /// Record a shard as locally owned and immediately `Active`, skipping the
    /// sweep phase.
    ///
    /// For single-node / no-controller deployments (and tests) where there is
    /// no durable lease and no volatile state to reconstruct, so the
    /// Sweeping→Active staging that [`acquire_shard`](Self::acquire_shard)
    /// performs is unnecessary. Controller-managed deployments must use
    /// `acquire_shard` instead so the sweep runs before admission.
    pub fn record_self_assigned_shard(&self, shard_id: ShardId, epoch: ShardEpoch) {
        let mut owner = self.shard_owner.write().expect("shard_owner lock poisoned");
        let _ = owner.record_acquired(shard_id, epoch);
        owner.mark_active(shard_id);
    }
    /// Acquire a durable lease on `shard_id`, reconstruct its volatile state,
    /// and bring it into service.
    ///
    /// The sequence is ordered for correctness:
    /// 1. Take the durable lease (`try_acquire_bundle`); a `Rejected` outcome
    ///    means another node owns it, surfaced as an error.
    /// 2. Record the shard locally in `Sweeping` and spawn the lease renewer,
    ///    which signals `lost_rx` if the lease is ever fenced.
    /// 3. Run `sweep_shard` to rebuild in-memory dispatch and timeout state
    ///    from durable history.
    /// 4. Only then `mark_active`, so admission begins against fully
    ///    reconstructed state.
    ///
    /// On lease loss the spawned watcher transitions the shard to `Draining`
    /// and purges all per-shard tracking, so a fenced node stops scanning
    /// timeouts for runs it no longer owns.
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
            let mut owner = self.shard_owner.write().expect("shard_owner lock poisoned");
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
            &self.lanes,
            self.lanes.len(),
            &self.workflow_timeout_tracking,
            &self.wft_timeout_tracking,
            &self.activity_tracking,
            &self.nexus_timeout_tracking,
            &self.completion_callback_tracking,
            // Activity republication runs through the shared preparation gate
            // with the runtime's broker and deployment registry.
            &self.activity_retry_deps(),
        )
        .await?;

        // Sweep must complete before the shard goes Active: only now is the
        // in-memory delivery/timeout state a faithful rebuild of durable
        // history, so it is safe to start admitting commands against it.
        self.shard_owner
            .write()
            .expect("shard_owner lock poisoned")
            .mark_active(shard_id);

        let shard_owner = self.shard_owner.clone();
        let workflow_timeout_tracking = self.workflow_timeout_tracking.clone();
        let wft_timeout_tracking = self.wft_timeout_tracking.clone();
        let activity_tracking = self.activity_tracking.clone();
        let nexus_timeout_tracking = self.nexus_timeout_tracking.clone();
        let completion_callback_tracking = self.completion_callback_tracking.clone();
        // The renewer fires lost_rx when the lease is fenced. Move the shard to
        // Draining and drop all per-shard tracking so this node stops scanning
        // timeouts for runs whose ownership has moved elsewhere.
        tokio::spawn(async move {
            if lost_rx.await.is_ok() {
                let mut owner = shard_owner.write().expect("shard_owner lock poisoned");
                owner.mark_draining(shard_id);
                drop(owner);
                workflow_timeout_tracking.remove_all_for_shard(shard_id);
                wft_timeout_tracking.remove_all_for_shard(shard_id);
                activity_tracking.remove_all_for_shard(shard_id);
                nexus_timeout_tracking.remove_all_for_shard(shard_id);
                completion_callback_tracking.remove_all_for_shard(shard_id);
            }
        });

        Ok(epoch)
    }

    /// Voluntarily give up a shard: stop accepting work, purge its tracking,
    /// and drop ownership.
    ///
    /// Marks the shard `Draining` first (which cancels its shard-scoped tasks
    /// and halts new admission), clears every per-shard tracking map, then
    /// removes it from the ownership view. This is the graceful counterpart to
    /// lease-loss handling in [`acquire_shard`](Self::acquire_shard); the
    /// durable lease is expected to be released by the caller / controller flow.
    pub async fn relinquish_shard(&self, shard_id: ShardId) {
        self.shard_owner
            .write()
            .expect("shard_owner lock poisoned")
            .mark_draining(shard_id);
        self.workflow_timeout_tracking
            .remove_all_for_shard(shard_id);
        self.wft_timeout_tracking.remove_all_for_shard(shard_id);
        self.activity_tracking.remove_all_for_shard(shard_id);
        self.nexus_timeout_tracking.remove_all_for_shard(shard_id);
        self.completion_callback_tracking
            .remove_all_for_shard(shard_id);
        self.shard_owner
            .write()
            .expect("shard_owner lock poisoned")
            .remove(shard_id);
    }
}

impl<R> TokeiraRuntime<R>
where
    R: RunRepository + LeaseRepository + 'static,
{
    /// Spawn the placement-controller membership client and return its task
    /// handle.
    ///
    /// The client streams registration and heartbeats to the controller and
    /// applies the directives it receives (placement, connection budget, drain)
    /// by mutating the shared `ShardOwner`, `RuntimeDrain`, and the supplied
    /// `budget_applier`. It runs until `shutdown` is cancelled. Available only
    /// when the repository is also a [`LeaseRepository`], since acting on
    /// placement directives requires lease operations.
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
