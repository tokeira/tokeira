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
    /// Bring a shard whose lease this node took itself into service: record it
    /// `Sweeping`, rebuild its volatile state with the recovery sweep, and only
    /// then mark it `Active`.
    ///
    /// For deployments without a placement controller, which take every
    /// shard's lease at boot. With no other node to fence, no lease renewer
    /// runs. The sweep still must: the previous process's offered workflow
    /// tasks and timeout tracking ended with it, and Temporal likewise rebuilds
    /// a shard's pending work from its persisted task queues whenever the shard
    /// loads (`newQueueBase`, `service/history/queues/queue_base.go:104-132 @
    /// v1.31.0`; runtime-sweeper-recovery Requirement 11.6).
    ///
    /// If the sweep fails, the shard is dropped locally, so it admits nothing,
    /// and the error is returned. Releasing the durable lease is the caller's
    /// job, because the caller holds the lease's owner and epoch.
    pub async fn recover_self_assigned_shard(
        &self,
        shard_id: ShardId,
        epoch: ShardEpoch,
    ) -> Result<SweepResult> {
        let mut recovery = self.begin_acquisition(shard_id, epoch);
        let tracking = self
            .wft_timeout_tracking
            .for_acquisition(recovery.acquisition.clone());
        let workflow_tracking = self
            .workflow_timeout_tracking
            .for_acquisition(recovery.acquisition.clone());
        let activity_tracking = self
            .activity_tracking
            .for_acquisition(recovery.acquisition.clone());
        let nexus_tracking = self
            .nexus_timeout_tracking
            .for_acquisition(recovery.acquisition.clone());
        let callback_tracking = self
            .completion_callback_tracking
            .for_acquisition(recovery.acquisition.clone());
        let repair = crate::recovery::RepairAcquisition {
            owner: self.shard_owner.clone(),
            acquisition: recovery.acquisition.clone(),
            max_retries: self.config.max_occ_retries,
        };
        let reconciliation = self
            .shard_owner
            .read()
            .expect("shard owner lock poisoned")
            .reconciliation_enabled();
        let shutdown = self.runtime_shutdown.cancellation_token();
        let _exclusive = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Err(anyhow!("runtime stopped while draining recovery writers")),
            result = crate::serving_gate::quiesce(&self.shard_owner, &recovery.acquisition) => result?,
        };
        let mut retry = self.activity_retry_deps();
        retry.tracking = activity_tracking.clone();
        let shutdown = self.runtime_shutdown.cancellation_token();
        let result = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Err(anyhow!("runtime stopped during shard recovery")),
            _ = recovery.acquisition.cancel.cancelled() => return Err(anyhow!("shard recovery acquisition cancelled")),
            result = async {
                let swept = crate::recovery::sweep_shard_inner(shard_id, self.repo.as_ref(), &self.broker, &self.lanes, self.lanes.len(),
                    &workflow_tracking, &tracking,
                    &activity_tracking,
                    &nexus_tracking,
                    &callback_tracking, &retry, reconciliation.then_some(&repair)).await?;
                // Before activation, on both of the sweep's paths: switch the
                // materializations recorded on the shard and hand its records
                // to the purger, checking the acquisition as the repair does.
                crate::purge::recover_bulk_writes(shard_id, self.repo.as_ref(), &self.purger,
                    reconciliation.then_some(&repair)).await?;
                anyhow::Ok(swept)
            } => result?,
        };
        Self::settle_self_assigned_recovery(&mut recovery, Ok(result))
    }

    fn settle_self_assigned_recovery(
        recovery: &mut AcquisitionCleanup,
        swept: Result<SweepResult>,
    ) -> Result<SweepResult> {
        let result = swept?;
        recovery.activate()?;
        recovery.disarm();
        Ok(result)
    }

    fn begin_acquisition(&self, shard_id: ShardId, epoch: ShardEpoch) -> AcquisitionCleanup {
        let acquisition = {
            let mut owner = self.shard_owner.write().expect("shard owner lock poisoned");
            owner.record_acquired(shard_id, epoch);
            // Cancellation and clearing share the owner lock with scoped installs.
            // A superseded sweep cannot repopulate these entries after this clear,
            // and runs no longer eligible for recovery cannot leak old generations.
            self.workflow_timeout_tracking
                .remove_all_for_shard(shard_id);
            self.wft_timeout_tracking.remove_all_for_shard(shard_id);
            self.activity_tracking.remove_all_for_shard(shard_id);
            self.nexus_timeout_tracking.remove_all_for_shard(shard_id);
            self.completion_callback_tracking
                .remove_all_for_shard(shard_id);
            owner
                .acquisition(shard_id)
                .expect("just recorded acquisition")
        };
        AcquisitionCleanup {
            acquisition,
            owner: self.shard_owner.clone(),
            workflow: self.workflow_timeout_tracking.clone(),
            wft: self.wft_timeout_tracking.clone(),
            activity: self.activity_tracking.clone(),
            nexus: self.nexus_timeout_tracking.clone(),
            callbacks: self.completion_callback_tracking.clone(),
            armed: true,
        }
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
        let lease_request_started = tokio::time::Instant::now();
        let outcome = self
            .repo
            .try_acquire_bundle(
                shard_id,
                self.owner_identity.clone(),
                self.node_endpoint.clone(),
            )
            .await?;
        let (epoch, renewed) = match outcome {
            LeaseOutcome::Acquired { epoch } => (epoch, false),
            LeaseOutcome::Rejected { .. } => {
                return Err(lease_rejected_error(shard_id));
            }
            LeaseOutcome::Renewed { epoch } => (epoch, true),
        };

        // Placement is level-triggered, so reconnects and periodic controller
        // loops can repeat a directive already enacted by this runtime. Renew
        // against durable truth first, then leave the existing recovery state
        // and renewer alone when this exact epoch is already Active.
        if renewed
            && self
                .shard_owner
                .read()
                .expect("shard_owner lock poisoned")
                .owns(shard_id)
                == Some(epoch)
        {
            return Ok(epoch);
        }

        let recovery = self.begin_acquisition(shard_id, epoch);
        let reconciliation = self
            .shard_owner
            .read()
            .expect("shard owner lock poisoned")
            .reconciliation_enabled();
        if reconciliation && let Some(duration) = self.repo.bundle_lease_duration() {
            *recovery
                .acquisition
                .deadline
                .lock()
                .expect("acquisition deadline lock poisoned") =
                Some(lease_request_started + std::time::Duration::try_from(duration)?);
        }
        let cancel = recovery.acquisition.cancel.clone();
        let (lost_tx, mut lost_rx) = oneshot::channel();
        let _renewer = self
            .runtime_shutdown
            .spawn(crate::recovery::run_lease_renewer_scoped(
                self.repo.clone(),
                shard_id,
                self.owner_identity.clone(),
                self.node_endpoint.clone(),
                epoch,
                tokio::time::Duration::from_secs(1),
                3,
                cancel.clone(),
                lost_tx,
                reconciliation.then(|| recovery.acquisition.clone()),
            ));
        let tracking = self
            .wft_timeout_tracking
            .for_acquisition(recovery.acquisition.clone());
        let workflow_tracking = self
            .workflow_timeout_tracking
            .for_acquisition(recovery.acquisition.clone());
        let activity_tracking = self
            .activity_tracking
            .for_acquisition(recovery.acquisition.clone());
        let nexus_tracking = self
            .nexus_timeout_tracking
            .for_acquisition(recovery.acquisition.clone());
        let callback_tracking = self
            .completion_callback_tracking
            .for_acquisition(recovery.acquisition.clone());
        let repair = crate::recovery::RepairAcquisition {
            owner: self.shard_owner.clone(),
            acquisition: recovery.acquisition.clone(),
            max_retries: self.config.max_occ_retries,
        };
        let reconciliation = self
            .shard_owner
            .read()
            .expect("shard owner lock poisoned")
            .reconciliation_enabled();
        let shutdown = self.runtime_shutdown.cancellation_token();
        let _exclusive = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Err(anyhow!("runtime stopped while draining recovery writers")),
            result = crate::serving_gate::quiesce(&self.shard_owner, &recovery.acquisition) => result?,
        };
        let mut retry = self.activity_retry_deps();
        retry.tracking = activity_tracking.clone();
        let shutdown = self.runtime_shutdown.cancellation_token();
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Err(anyhow!("runtime stopped during shard recovery")),
            _ = cancel.cancelled() => return Err(anyhow!("shard acquisition cancelled during recovery")),
            _ = &mut lost_rx => return Err(anyhow!("shard lease lost during recovery")),
            result = async {
                crate::recovery::sweep_shard_inner(shard_id, self.repo.as_ref(), &self.broker, &self.lanes, self.lanes.len(),
                    &workflow_tracking, &tracking,
                    &activity_tracking,
                    &nexus_tracking,
                    &callback_tracking, &retry, reconciliation.then_some(&repair)).await?;
                // Before activation, on both of the sweep's paths: switch the
                // materializations recorded on the shard and hand its records
                // to the purger, checking the acquisition as the repair does.
                crate::purge::recover_bulk_writes(shard_id, self.repo.as_ref(), &self.purger,
                    reconciliation.then_some(&repair)).await?;
                anyhow::Ok(())
            } => { result?; }
        }
        // No await between final loss inspection and activation. Generation and
        // cancellation are checked under the same owner lock as replacement.
        if !matches!(
            lost_rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ) {
            return Err(anyhow!("shard lease lost before activation"));
        }
        recovery.activate()?;
        let _watcher = self.runtime_shutdown.spawn(async move {
            tokio::select! {
                _ = shutdown.cancelled() => {},
                _ = cancel.cancelled() => {},
                _ = lost_rx => {},
            }
            // Cleanup owns this acquisition, not the shard's future occupant.
            drop(recovery);
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
        // Hold ownership through cleanup: a replacement acquisition cannot
        // install entries between draining and removal of the old tracking.
        let mut owner = self.shard_owner.write().expect("shard owner lock poisoned");
        owner.mark_draining(shard_id);
        self.workflow_timeout_tracking
            .remove_all_for_shard(shard_id);
        self.wft_timeout_tracking.remove_all_for_shard(shard_id);
        self.activity_tracking.remove_all_for_shard(shard_id);
        self.nexus_timeout_tracking.remove_all_for_shard(shard_id);
        self.completion_callback_tracking
            .remove_all_for_shard(shard_id);
        owner.remove(shard_id);
    }
}

#[async_trait::async_trait]
impl<R> MembershipShardLifecycle for TokeiraRuntime<R>
where
    R: RunRepository + LeaseRepository + 'static,
{
    async fn acquire_shard(&self, shard_id: ShardId) -> Result<ShardEpoch> {
        TokeiraRuntime::acquire_shard(self, shard_id).await
    }

    async fn relinquish_shard(&self, shard_id: ShardId) -> Result<()> {
        let epoch = self
            .shard_owner
            .read()
            .expect("shard_owner lock poisoned")
            .epoch_of(shard_id)
            .unwrap_or(ShardEpoch::ZERO);
        if epoch == ShardEpoch::ZERO {
            return Ok(());
        }
        let outcome = self
            .repo
            .relinquish_bundle(shard_id, self.owner_identity.clone(), epoch)
            .await?;
        if matches!(outcome, LeaseOutcome::Acquired { .. }) {
            TokeiraRuntime::relinquish_shard(self, shard_id).await;
        }
        Ok(())
    }

    fn heartbeat_inputs(
        &self,
        available_connections: u32,
        connection_rate_headroom: f32,
    ) -> HeartbeatInputs {
        TokeiraRuntime::heartbeat_inputs(self, available_connections, connection_rate_headroom)
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
    /// applies the directives it receives (placement, connection budget, drain).
    /// Placement delegates back to this runtime so acquisition includes lease
    /// renewal, durable recovery, and activation; budget and drain state use the
    /// supplied collaborators. It runs until `shutdown` is cancelled. Available
    /// only when the repository is also a [`LeaseRepository`], since acting on
    /// placement directives requires lease operations.
    pub fn spawn_membership_client(
        self: &Arc<Self>,
        config: MembershipConfig,
        budget_applier: Arc<dyn ConnectionBudgetApplier>,
        shutdown: CancellationToken,
    ) -> tokio::task::JoinHandle<Result<()>> {
        let client = MembershipClient::new(
            config,
            Arc::clone(self) as Arc<dyn MembershipShardLifecycle>,
            self.shard_owner.clone(),
            self.runtime_drain.clone(),
            budget_applier,
        );
        tokio::spawn(client.run(shutdown))
    }
}

/// Synchronous cancellation cleanup also covers a caller dropping acquisition
/// mid-sweep. The owner lock serializes cleanup with replacement installation.
struct AcquisitionCleanup {
    acquisition: crate::shard::ShardAcquisition,
    owner: Arc<RwLock<ShardOwner>>,
    workflow: WorkflowTimeoutTrackingState,
    wft: WftTimeoutTrackingState,
    activity: ActivityTrackingState,
    nexus: NexusTimeoutTrackingState,
    callbacks: CompletionCallbackTrackingState,
    armed: bool,
}

impl AcquisitionCleanup {
    fn activate(&self) -> Result<()> {
        if !self
            .owner
            .write()
            .expect("shard owner lock poisoned")
            .activate_acquisition(&self.acquisition)
        {
            return Err(anyhow!("shard acquisition superseded before activation"));
        }
        Ok(())
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for AcquisitionCleanup {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.acquisition.cancel.cancel();
        let mut owner = self.owner.write().expect("shard owner lock poisoned");
        if !owner.matches_acquisition(&self.acquisition) {
            return;
        }
        let shard_id = self.acquisition.shard_id;
        owner.mark_draining(shard_id);
        self.workflow.remove_all_for_shard(shard_id);
        self.wft.remove_all_for_shard(shard_id);
        self.activity.remove_all_for_shard(shard_id);
        self.nexus.remove_all_for_shard(shard_id);
        self.callbacks.remove_all_for_shard(shard_id);
        owner.remove(shard_id);
    }
}

#[cfg(test)]
mod tests {
    use tokeira_proto::connect::tokeira::internal::controller::v1::{
        self as pb, controller_directive,
    };
    use tokeira_storage::InMemoryStore;
    use tokeira_types::{
        Memo, NodeEndpoint, RequestId, SearchAttributes, WorkflowId, WorkflowType,
    };

    use super::*;

    #[derive(Debug)]
    struct NoopBudgetApplier;

    impl ConnectionBudgetApplier for NoopBudgetApplier {
        fn apply_budget(
            &self,
            _rate_per_second: f64,
            _capacity: u64,
            _max_reservoir_size: u32,
        ) -> Result<()> {
            Ok(())
        }

        fn available_connections(&self) -> u32 {
            0
        }
    }

    fn runtime_with_membership_client(
        store: Arc<InMemoryStore>,
    ) -> (Arc<TokeiraRuntime<InMemoryStore>>, MembershipClient) {
        let node_id = IncarnationId::new();
        let endpoint = NodeEndpoint {
            host: "127.0.0.1".to_owned(),
            port: 7233,
        };
        let runtime = Arc::new(TokeiraRuntime::new_with_nexus_and_shards_and_endpoint(
            store,
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
            BacklogConfig::default(),
            ActivityTimeoutScannerConfig::default(),
            NexusTimeoutScannerConfig::default(),
            NexusEndpointRegistry::default(),
            Arc::new(NoopNexusHttpClient),
            NexusCompletionDeps::default(),
            1,
            node_id.to_string(),
            endpoint.as_authority(),
            false,
            None,
        ));
        let client = MembershipClient::new(
            MembershipConfig {
                controller_endpoint: "http://127.0.0.1:7240".to_owned(),
                heartbeat_interval: std::time::Duration::from_secs(5),
                reconnect_base_delay: std::time::Duration::from_secs(1),
                reconnect_max_delay: std::time::Duration::from_secs(30),
                node_id,
                node_endpoint: endpoint,
                zone: None,
                version: "test".to_owned(),
                build_id: "test".to_owned(),
            },
            Arc::clone(&runtime) as Arc<dyn MembershipShardLifecycle>,
            Arc::clone(&runtime.shard_owner),
            Arc::clone(&runtime.runtime_drain),
            Arc::new(NoopBudgetApplier),
        );
        (runtime, client)
    }

    fn desired_placement(acquire_bundles: Vec<u32>) -> pb::ControllerDirective {
        pb::ControllerDirective {
            directive: Some(controller_directive::Directive::DesiredPlacement(
                pb::DesiredPlacementDirective {
                    acquire_bundles,
                    ..Default::default()
                }
                .into(),
            )),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn directive_takeover_activates_shard_and_passes_commit_fence() -> Result<()> {
        let store = Arc::new(InMemoryStore::default());
        let (runtime, client) = runtime_with_membership_client(Arc::clone(&store));

        client.handle_directive(desired_placement(vec![0])).await?;

        let epoch = runtime
            .shard_owner
            .read()
            .expect("shard_owner lock poisoned")
            .owns(ShardId(0))
            .expect("directive takeover must finish in Active");
        let request = start_request();
        let result = runtime.start_workflow(request).await?;
        assert!(matches!(result, CommitResult::Applied { .. }));
        let CommitResult::Applied { new_state } = result else {
            unreachable!()
        };
        assert_eq!(runtime.current_shard_epoch(&new_state).await?, epoch);
        Ok(())
    }

    #[tokio::test]
    async fn drain_directive_relinquishes_shards_and_next_heartbeat_reports_safe() -> Result<()> {
        let store = Arc::new(InMemoryStore::default());
        let (runtime, client) = runtime_with_membership_client(Arc::clone(&store));
        client.handle_directive(desired_placement(vec![0])).await?;
        let epoch = runtime
            .shard_owner
            .read()
            .expect("shard_owner lock poisoned")
            .owns(ShardId(0))
            .expect("takeover must finish in Active");

        client
            .handle_directive(pb::ControllerDirective {
                directive: Some(controller_directive::Directive::Drain(
                    pb::DrainDirective::default().into(),
                )),
                ..Default::default()
            })
            .await?;

        // Ownership left this node through the epoch-checked relinquish: the
        // durable lease row is unowned, so a successor can acquire at a higher
        // epoch, and the local owner view no longer admits shard 0.
        let lease = store
            .list_bundle_leases()
            .await?
            .into_iter()
            .find(|lease| lease.bundle_id == ShardId(0))
            .expect("lease row for shard 0");
        assert_eq!(lease.owner_node_id, None);
        assert!(lease.epoch.0 >= epoch.0);
        assert_eq!(
            runtime
                .shard_owner
                .read()
                .expect("shard_owner lock poisoned")
                .owned_shards()
                .count(),
            0
        );

        let heartbeat = client.heartbeat_message();
        assert_eq!(heartbeat.owned_bundle_count, 0);
        assert_eq!(
            heartbeat.drain_state,
            buffa::EnumValue::Known(pb::NodeDrainState::NODE_DRAIN_STATE_SAFE_TO_TERMINATE)
        );

        // Work routed here after the drain is refused rather than committed
        // under an epoch this node no longer holds.
        let refused = runtime.start_workflow(start_request()).await;
        assert!(refused.is_err(), "start after relinquish must be refused");
        Ok(())
    }

    #[tokio::test]
    async fn self_assigned_recovery_sweeps_before_activating_the_shard() -> Result<()> {
        let store = Arc::new(InMemoryStore::default());
        // The previous process left a pending first workflow task.
        let (previous, _) = runtime_with_membership_client(Arc::clone(&store));
        previous
            .recover_self_assigned_shard(ShardId(0), ShardEpoch::ZERO)
            .await?;
        let request = start_request();
        let run_key = request.run_key;
        let queue = QueueKey {
            namespace_id: request.namespace_id,
            task_queue: request.task_queue.clone(),
            task_kind: TaskKind::Workflow,
            deployment: None,
            build_id: None,
        };
        assert!(matches!(
            previous.start_workflow(request).await?,
            CommitResult::Applied { .. }
        ));

        // A restarted process admits nothing until it has recovered the
        // shard, and the recovery offers the task again.
        let (restarted, _) = runtime_with_membership_client(store);
        assert!(restarted.active_shards().is_empty());
        let swept = restarted
            .recover_self_assigned_shard(ShardId(0), ShardEpoch::ZERO)
            .await?;
        assert_eq!(swept.workflow_tasks_republished, 1);
        assert_eq!(restarted.active_shards(), vec![ShardId(0)]);
        let task = restarted
            .poll_workflow_task(
                queue,
                WorkerIdentity("worker".to_owned()),
                tokio::time::Duration::from_secs(1),
            )
            .await?
            .expect("the recovered workflow task is offered");
        assert_eq!(task.run_key, run_key);
        Ok(())
    }

    #[tokio::test]
    async fn failed_self_assigned_recovery_relinquishes_the_shard() -> Result<()> {
        use crate::activity_timeout::ActivityTrackingEntry;

        let (runtime, _) = runtime_with_membership_client(Arc::new(InMemoryStore::default()));
        let mut recovery = runtime.begin_acquisition(ShardId(0), ShardEpoch(3));
        // Tracking the failed sweep had already rebuilt.
        let now = OffsetDateTime::now_utc();
        runtime.activity_tracking.insert(ActivityTrackingEntry {
            run_key: RunKey::new(),
            shard_id: ShardId(0),
            activity_id: "activity-1".to_owned(),
            original_scheduled_at: now,
            last_dispatched_at: now,
            started_at: Some(now),
            last_heartbeat_at: None,
            cancel_requested: false,
        });

        let error = TokeiraRuntime::<InMemoryStore>::settle_self_assigned_recovery(
            &mut recovery,
            Err(anyhow!("candidate listing failed")),
        )
        .expect_err("the sweep's error is returned");
        drop(recovery);

        assert!(error.to_string().contains("candidate listing failed"));
        assert!(runtime.active_shards().is_empty());
        assert_eq!(
            runtime
                .shard_owner
                .read()
                .expect("shard_owner lock poisoned")
                .epoch_of(ShardId(0)),
            None
        );
        assert!(runtime.activity_tracking.snapshot().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn interrupted_recovery_cannot_activate_or_clear_a_replacement_acquisition() {
        for managed in [false, true] {
            for interruption in 0..4 {
                let store = Arc::new(InMemoryStore::default());
                let mut item = tokeira_kernel::Kernel::apply(
                    &BasicKernel,
                    LoadedRun::Absent,
                    Command::Start(start_request()),
                )
                .unwrap();
                let run_key = item.next_state.run_key;
                let deadline = OffsetDateTime::now_utc() - time::Duration::seconds(1);
                item.next_state
                    .pending_workflow_task
                    .as_mut()
                    .unwrap()
                    .schedule_to_start_deadline = Some(deadline);
                store
                    .commit_transition(run_key, item, ShardEpoch::ZERO)
                    .await
                    .unwrap();
                let (runtime, _) = runtime_with_membership_client(store);
                let (installed, resume) = runtime.wft_timeout_tracking.pause_next_recovery();
                let acquiring = runtime.clone();
                let task = tokio::spawn(async move {
                    if managed {
                        acquiring.acquire_shard(ShardId(0)).await.map(|_| ())
                    } else {
                        acquiring
                            .recover_self_assigned_shard(ShardId(0), ShardEpoch::ZERO)
                            .await
                            .map(|_| ())
                    }
                });
                installed.await.unwrap();
                assert!(runtime.active_shards().is_empty());
                let entries = runtime.wft_timeout_tracking.snapshot();
                assert_eq!(entries.len(), 1);
                assert_eq!(
                    entries[0].started_at + entries[0].workflow_task_timeout,
                    deadline
                );
                match interruption {
                    0 => {
                        drop(resume);
                        assert!(task.await.unwrap().is_err());
                    }
                    1 => {
                        task.abort();
                        assert!(task.await.unwrap_err().is_cancelled());
                        drop(resume);
                    }
                    2 => {
                        runtime.relinquish_shard(ShardId(0)).await;
                        assert!(task.await.unwrap().is_err());
                        drop(resume);
                    }
                    _ => {
                        let epoch = runtime
                            .shard_owner
                            .read()
                            .unwrap()
                            .epoch_of(ShardId(0))
                            .unwrap();
                        runtime
                            .recover_self_assigned_shard(ShardId(0), epoch)
                            .await
                            .unwrap();
                        let _ = resume.send(());
                        assert!(task.await.unwrap().is_err());
                        assert_eq!(runtime.active_shards(), vec![ShardId(0)]);
                        assert_eq!(runtime.wft_timeout_tracking.snapshot(), entries);
                    }
                }
                if interruption < 3 {
                    assert!(runtime.active_shards().is_empty());
                    assert!(runtime.wft_timeout_tracking.snapshot().is_empty());
                    assert!(
                        runtime
                            .shard_owner
                            .read()
                            .unwrap()
                            .epoch_of(ShardId(0))
                            .is_none()
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
    }

    fn start_request() -> StartRequest {
        let run_id = tokeira_types::RunId::new();
        StartRequest {
            advice_policy: tokeira_kernel::ContinueAsNewAdvicePolicy::V1_31_0,
            initiator: None,
            run_key: RunKey::new(),
            namespace_id: NamespaceId::new(),
            workflow_id: WorkflowId("directive-takeover".to_owned()),
            run_id,
            workflow_type: WorkflowType("test".to_owned()),
            task_queue: TaskQueueName("test".to_owned()),
            input: Payloads::default(),
            header: None,
            memo: Memo::default(),
            search_attributes: SearchAttributes::default(),
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: Duration::seconds(10),
            retry_policy: None,
            conflict_policy: WorkflowIdConflictPolicy::Fail,
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            deployment: None,
            build_id: None,
            versioning_override: None,
            workflow_start_delay: None,
            completion_callbacks: Vec::new(),
            user_metadata: None,
            links: Vec::new(),
            on_conflict_options: None,
            priority: None,
            attempt: 1,
            continued_execution_run_id: None,
            first_execution_run_id: None,
            parent_run_key: None,
            parent_workflow_id: None,
            parent_run_id: None,
            parent_namespace_id: None,
            parent_namespace_name: None,
            parent_initiated_event_id: 0,
            root_workflow_id: None,
            root_run_id: None,
            original_execution_run_id: Some(run_id),
            continued_failure: None,
            last_completion_result: None,
            first_run_started_at: None,
            request: RequestContext {
                request_id: RequestId("directive-takeover".to_owned()),
                caller_identity: None,
                principal: None,
                received_at: OffsetDateTime::now_utc(),
            },
            now: OffsetDateTime::now_utc(),
            client_cron_schedule: None,
            cron_schedule: None,
            eager_execution_accepted: false,
            reserved_poller_identity: None,
            inherited_versioning_info: None,
        }
    }
}
