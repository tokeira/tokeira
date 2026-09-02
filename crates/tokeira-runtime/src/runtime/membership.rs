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
        let run_key = request.run_key;
        let result = runtime.start_workflow(request).await?;
        assert!(matches!(result, CommitResult::Applied { .. }));
        assert_eq!(runtime.current_shard_epoch(run_key).await?, epoch);
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

    fn start_request() -> StartRequest {
        let run_id = tokeira_types::RunId::new();
        StartRequest {
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
