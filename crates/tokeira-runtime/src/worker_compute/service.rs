//! Process-local Worker Compute Controller supervision.
//!
//! One enabled service owns four cancellable advisory loops: deployment
//! reconciliation, demand batching, queue sampling/metrics evaluation, and durable
//! outbox delivery. Child failures are logged and retried; cancellation closes new
//! work admission while already-claimed provider attempts finish their sweep.

use std::sync::Arc;

use anyhow::Result;
use time::OffsetDateTime;
use tokeira_storage::{RunRepository, WorkerComputeHealthFilter, WorkerComputeRepository};
use tokeira_types::{ControllerInstanceKey, ShardId, WorkerComputeControllerLifecycle};
use tokio::{sync::mpsc, task::JoinSet};
use tokio_util::sync::CancellationToken;

use super::{
    ACTION_DELIVERY_IDLE_INTERVAL, CATALOG_RECONCILE_INTERVAL, DemandObservation,
    ObservationBatcher, QUEUE_SAMPLE_INTERVAL, WorkerComputeClock, WorkerComputeNamespace,
    WorkerComputeNamespaceCatalog, WorkerComputeOutbox, WorkerComputeQueueSampler,
    WorkerComputeReconciler,
};
use crate::metrics as runtime_metrics;

/// Snapshot callback for the runtime's current active shard set.
pub type WorkerComputeActiveShards = Arc<dyn Fn() -> Vec<ShardId> + Send + Sync>;

/// Inputs owned by the single enabled worker-compute background service.
pub struct WorkerComputeControllerService<R> {
    catalog: Arc<dyn WorkerComputeNamespaceCatalog>,
    repository: Arc<dyn WorkerComputeRepository>,
    reconciler: WorkerComputeReconciler,
    sampler: Arc<WorkerComputeQueueSampler<R>>,
    outbox: WorkerComputeOutbox,
    clock: Arc<dyn WorkerComputeClock>,
    active_shards: WorkerComputeActiveShards,
    observations: mpsc::Receiver<DemandObservation>,
    reconcile_hints: mpsc::Receiver<ControllerInstanceKey>,
}

impl<R> std::fmt::Debug for WorkerComputeControllerService<R> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkerComputeControllerService")
            .finish_non_exhaustive()
    }
}

impl<R> WorkerComputeControllerService<R>
where
    R: RunRepository + 'static,
{
    /// Construct the enabled service after the application has wired its bounded
    /// observation and reconciliation senders into delivery/registry components.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        catalog: Arc<dyn WorkerComputeNamespaceCatalog>,
        repository: Arc<dyn WorkerComputeRepository>,
        reconciler: WorkerComputeReconciler,
        sampler: WorkerComputeQueueSampler<R>,
        outbox: WorkerComputeOutbox,
        clock: Arc<dyn WorkerComputeClock>,
        active_shards: WorkerComputeActiveShards,
        observations: mpsc::Receiver<DemandObservation>,
        reconcile_hints: mpsc::Receiver<ControllerInstanceKey>,
    ) -> Self {
        Self {
            catalog,
            repository,
            reconciler,
            sampler: Arc::new(sampler),
            outbox,
            clock,
            active_shards,
            observations,
            reconcile_hints,
        }
    }

    /// Run all four child loops until `shutdown` is cancelled.
    pub async fn run(self, shutdown: CancellationToken) -> Result<()> {
        let mut children = JoinSet::new();
        children.spawn(run_reconciliation_loop(
            self.reconciler.clone(),
            self.catalog.clone(),
            self.clock.clone(),
            self.reconcile_hints,
            shutdown.clone(),
        ));
        children.spawn(run_observation_loop(
            self.reconciler.clone(),
            self.catalog.clone(),
            self.clock.clone(),
            self.observations,
            shutdown.clone(),
        ));
        children.spawn(run_sampling_loop(
            self.reconciler,
            self.catalog.clone(),
            self.repository,
            self.sampler,
            self.clock.clone(),
            self.active_shards,
            shutdown.clone(),
        ));
        children.spawn(run_outbox_loop(self.outbox, self.catalog, shutdown.clone()));

        let mut first_failure = None;
        while let Some(result) = children.join_next().await {
            if let Err(error) = result {
                shutdown.cancel();
                if first_failure.is_none() {
                    first_failure =
                        Some(anyhow::Error::new(error).context("worker-compute child task failed"));
                }
            }
        }
        first_failure.map_or(Ok(()), Err)
    }
}

async fn run_reconciliation_loop(
    reconciler: WorkerComputeReconciler,
    catalog: Arc<dyn WorkerComputeNamespaceCatalog>,
    clock: Arc<dyn WorkerComputeClock>,
    mut hints: mpsc::Receiver<ControllerInstanceKey>,
    shutdown: CancellationToken,
) {
    reconcile_catalog(&reconciler, catalog.as_ref(), clock.now()).await;
    let mut hints_open = true;
    let mut interval = tokio::time::interval_at(
        tokio::time::Instant::now() + CATALOG_RECONCILE_INTERVAL,
        CATALOG_RECONCILE_INTERVAL,
    );
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            _ = interval.tick() => {
                reconcile_catalog(&reconciler, catalog.as_ref(), clock.now()).await;
            }
            hint = hints.recv(), if hints_open => {
                let Some(key) = hint else {
                    hints_open = false;
                    continue;
                };
                reconcile_hint(&reconciler, catalog.as_ref(), key, clock.now()).await;
            }
        }
    }
}

async fn reconcile_catalog(
    reconciler: &WorkerComputeReconciler,
    catalog: &dyn WorkerComputeNamespaceCatalog,
    now: OffsetDateTime,
) {
    if let Err(error) = reconciler.reconcile_catalog(catalog, now).await {
        tracing::warn!(?error, "worker-compute catalog reconciliation failed");
    }
}

async fn reconcile_hint(
    reconciler: &WorkerComputeReconciler,
    catalog: &dyn WorkerComputeNamespaceCatalog,
    key: ControllerInstanceKey,
    now: OffsetDateTime,
) {
    let result = match catalog.name_for_id(key.namespace_id).await {
        Ok(Some(name)) => {
            reconciler
                .reconcile_key(
                    &WorkerComputeNamespace {
                        namespace_id: key.namespace_id,
                        name,
                    },
                    &key,
                    now,
                )
                .await
        }
        Ok(None) => reconciler.inactivate_key(&key, now).await,
        Err(error) => Err(error.into()),
    };
    if let Err(error) = result {
        tracing::warn!(?error, "worker-compute hinted reconciliation failed");
    }
}

async fn run_observation_loop(
    reconciler: WorkerComputeReconciler,
    catalog: Arc<dyn WorkerComputeNamespaceCatalog>,
    clock: Arc<dyn WorkerComputeClock>,
    mut observations: mpsc::Receiver<DemandObservation>,
    shutdown: CancellationToken,
) {
    let mut batcher = ObservationBatcher::default();
    let mut observations_open = true;
    loop {
        if shutdown.is_cancelled() {
            return;
        }
        if let Some(due_at) = batcher.next_due_at() {
            let wait = duration_until(clock.now(), due_at);
            tokio::select! {
                () = shutdown.cancelled() => return,
                observation = observations.recv(), if observations_open => {
                    match observation {
                        Some(observation) => batcher.ingest(observation, clock.now()),
                        None => observations_open = false,
                    }
                }
                () = tokio::time::sleep(wait) => {}
            }
        } else if observations_open {
            tokio::select! {
                () = shutdown.cancelled() => return,
                observation = observations.recv() => {
                    match observation {
                        Some(observation) => batcher.ingest(observation, clock.now()),
                        None => observations_open = false,
                    }
                }
            }
        } else {
            shutdown.cancelled().await;
            return;
        }

        let now = clock.now();
        for (key, batch) in batcher.take_due(now) {
            let namespace = match catalog.name_for_id(key.namespace_id).await {
                Ok(Some(name)) => WorkerComputeNamespace {
                    namespace_id: key.namespace_id,
                    name,
                },
                Ok(None) => continue,
                Err(error) => {
                    tracing::warn!(?error, "worker-compute observation namespace lookup failed");
                    continue;
                }
            };
            if let Err(error) = reconciler
                .evaluate_observation_batch(&namespace, &key, &batch, now)
                .await
            {
                tracing::warn!(?error, "worker-compute observation evaluation failed");
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_sampling_loop<R>(
    reconciler: WorkerComputeReconciler,
    catalog: Arc<dyn WorkerComputeNamespaceCatalog>,
    repository: Arc<dyn WorkerComputeRepository>,
    sampler: Arc<WorkerComputeQueueSampler<R>>,
    clock: Arc<dyn WorkerComputeClock>,
    active_shards: WorkerComputeActiveShards,
    shutdown: CancellationToken,
) where
    R: RunRepository + 'static,
{
    sample_and_evaluate(
        &reconciler,
        catalog.as_ref(),
        repository.as_ref(),
        sampler.as_ref(),
        clock.now(),
        active_shards.as_ref(),
    )
    .await;
    let mut interval = tokio::time::interval_at(
        tokio::time::Instant::now() + QUEUE_SAMPLE_INTERVAL,
        QUEUE_SAMPLE_INTERVAL,
    );
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            _ = interval.tick() => {
                sample_and_evaluate(
                    &reconciler,
                    catalog.as_ref(),
                    repository.as_ref(),
                    sampler.as_ref(),
                    clock.now(),
                    active_shards.as_ref(),
                ).await;
            }
        }
    }
}

async fn sample_and_evaluate<R>(
    reconciler: &WorkerComputeReconciler,
    catalog: &dyn WorkerComputeNamespaceCatalog,
    repository: &dyn WorkerComputeRepository,
    sampler: &WorkerComputeQueueSampler<R>,
    now: OffsetDateTime,
    active_shards: &(dyn Fn() -> Vec<ShardId> + Send + Sync),
) where
    R: RunRepository + 'static,
{
    if let Err(error) = sampler.sample_once(&active_shards(), now).await {
        tracing::warn!(?error, "worker-compute queue sampling failed");
        return;
    }
    let namespaces = match catalog.list_active().await {
        Ok(namespaces) => namespaces,
        Err(error) => {
            tracing::warn!(?error, "worker-compute metrics namespace lookup failed");
            return;
        }
    };
    let mut health_values = Vec::new();
    for namespace in namespaces {
        let controllers = match repository.list_controllers(namespace.namespace_id).await {
            Ok(controllers) => controllers,
            Err(error) => {
                tracing::warn!(?error, "worker-compute due-controller lookup failed");
                continue;
            }
        };
        for controller in controllers {
            if controller.lifecycle != WorkerComputeControllerLifecycle::Active
                || controller
                    .next_metrics_poll_at
                    .is_none_or(|due_at| due_at > now)
            {
                continue;
            }
            if let Err(error) = reconciler
                .evaluate_metrics_snapshot(&namespace, &controller.key, now)
                .await
            {
                tracing::warn!(?error, "worker-compute metrics evaluation failed");
            }
        }
        match repository
            .list_health(namespace.namespace_id, WorkerComputeHealthFilter::default())
            .await
        {
            Ok(rows) => health_values.extend(rows.into_iter().map(|row| row.health)),
            Err(error) => {
                tracing::warn!(?error, "worker-compute health snapshot failed");
            }
        }
    }
    runtime_metrics::record_worker_compute_health(health_values);
}

async fn run_outbox_loop(
    outbox: WorkerComputeOutbox,
    catalog: Arc<dyn WorkerComputeNamespaceCatalog>,
    shutdown: CancellationToken,
) {
    loop {
        if shutdown.is_cancelled() {
            return;
        }
        let mut claimed = 0_u64;
        match catalog.list_active().await {
            Ok(namespaces) => {
                for namespace in namespaces {
                    match outbox.deliver_namespace_once(namespace.namespace_id).await {
                        Ok(sweep) => claimed = claimed.saturating_add(sweep.claimed),
                        Err(error) => {
                            tracing::warn!(?error, "worker-compute outbox sweep failed");
                        }
                    }
                }
            }
            Err(error) => {
                tracing::warn!(?error, "worker-compute outbox namespace lookup failed");
            }
        }
        if claimed != 0 {
            continue;
        }
        tokio::select! {
            () = shutdown.cancelled() => return,
            () = tokio::time::sleep(ACTION_DELIVERY_IDLE_INTERVAL) => {}
        }
    }
}

fn duration_until(now: OffsetDateTime, due_at: OffsetDateTime) -> std::time::Duration {
    std::time::Duration::try_from(due_at - now).unwrap_or(std::time::Duration::ZERO)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet, VecDeque},
        sync::Mutex,
    };

    use async_trait::async_trait;
    use proptest::prelude::*;
    use tokeira_storage::{
        BuildId as StoredBuildId, ComputeConfig, ComputeConfigScalingGroup, ComputeProvider,
        ComputeScaler, ConflictToken, DeploymentCasResult, DeploymentName,
        InMemoryWorkerComputeRepository, RoutingConfigUpdateState, StoredRoutingConfig,
        StoredVersion, StoredWorkerDeployment, VersionMetadata, WorkerComputeHealthFilter,
        WorkerDeploymentRepository, WorkerDeploymentVersionStatus,
    };
    use tokeira_types::{
        BuildId, ConfigurationFingerprint, ControllerInstanceKey, DeploymentId, IncarnationId,
        NamespaceId, Payload, ScalingGroupId, WorkerComputeFailureCategory,
        WorkerComputeInvokeReason, WorkerComputeTaskType,
    };
    use tokio::sync::Notify;
    use uuid::Uuid;

    use super::*;
    use crate::{
        DemandObservationSink, DisabledWorkerComputeSink, InMemoryActivityBroker, InMemoryBroker,
        NexusEndpointRegistry, NexusTaskBroker, ObserveResult, ProviderActionInput,
        RemoteNexusProvider, SystemWorkerComputeClock, WorkerComputeProvider,
        WorkerComputeProviderAction, WorkerComputeProviderAttempt, WorkerComputeProviderOutcome,
        WorkerComputeProviderTargetKind, WorkerComputeQueueMetrics, WorkerComputeReconcileSink,
        build_provider_action,
    };

    #[derive(Debug)]
    struct StaticCatalog {
        namespace: WorkerComputeNamespace,
    }

    #[async_trait]
    impl WorkerComputeNamespaceCatalog for StaticCatalog {
        async fn list_active(
            &self,
        ) -> Result<Vec<WorkerComputeNamespace>, super::super::WorkerComputeCatalogError> {
            Ok(vec![self.namespace.clone()])
        }

        async fn name_for_id(
            &self,
            namespace_id: NamespaceId,
        ) -> Result<Option<String>, super::super::WorkerComputeCatalogError> {
            Ok((namespace_id == self.namespace.namespace_id).then(|| self.namespace.name.clone()))
        }
    }

    #[derive(Debug)]
    struct InProcessWorkerComputeTestProvider {
        outcomes: Mutex<VecDeque<WorkerComputeProviderOutcome>>,
        calls: Mutex<Vec<(Uuid, Vec<u8>)>>,
        accepted: Mutex<BTreeSet<Uuid>>,
        called: Notify,
    }

    impl InProcessWorkerComputeTestProvider {
        fn new(outcomes: impl IntoIterator<Item = WorkerComputeProviderOutcome>) -> Self {
            Self {
                outcomes: Mutex::new(outcomes.into_iter().collect()),
                calls: Mutex::new(Vec::new()),
                accepted: Mutex::new(BTreeSet::new()),
                called: Notify::new(),
            }
        }

        fn calls(&self) -> Vec<(Uuid, Vec<u8>)> {
            self.calls
                .lock()
                .expect("test provider calls lock poisoned")
                .clone()
        }

        fn accepted(&self) -> BTreeSet<Uuid> {
            self.accepted
                .lock()
                .expect("test provider accepted lock poisoned")
                .clone()
        }
    }

    #[async_trait]
    impl WorkerComputeProvider for InProcessWorkerComputeTestProvider {
        async fn deliver(
            &self,
            action: &WorkerComputeProviderAction,
            _claim_epoch: u64,
            _now: OffsetDateTime,
        ) -> WorkerComputeProviderAttempt {
            self.calls
                .lock()
                .expect("test provider calls lock poisoned")
                .push((action.action_id, action.request_data.clone()));
            let outcome = if self
                .accepted
                .lock()
                .expect("test provider accepted lock poisoned")
                .contains(&action.action_id)
            {
                WorkerComputeProviderOutcome::Delivered
            } else {
                self.outcomes
                    .lock()
                    .expect("test provider outcomes lock poisoned")
                    .pop_front()
                    .unwrap_or(WorkerComputeProviderOutcome::Delivered)
            };
            if outcome == WorkerComputeProviderOutcome::Delivered {
                self.accepted
                    .lock()
                    .expect("test provider accepted lock poisoned")
                    .insert(action.action_id);
            }
            self.called.notify_one();
            WorkerComputeProviderAttempt {
                target_kind: WorkerComputeProviderTargetKind::Worker,
                outcome,
            }
        }
    }

    fn deployment(namespace_id: NamespaceId) -> StoredWorkerDeployment {
        let version = StoredVersion {
            build_id: StoredBuildId("build".to_owned()),
            status: WorkerDeploymentVersionStatus::Inactive,
            create_time: OffsetDateTime::UNIX_EPOCH,
            routing_changed_time: None,
            current_since_time: None,
            ramping_since_time: None,
            first_activation_time: None,
            last_current_time: None,
            last_deactivation_time: None,
            ramp_percentage: 0.0,
            drainage_info: None,
            metadata: VersionMetadata::default(),
            compute_config: ComputeConfig {
                scaling_groups: BTreeMap::from([(
                    "group".to_owned(),
                    ComputeConfigScalingGroup {
                        task_queue_types: vec![tokeira_storage::DeploymentTaskQueueType::Workflow],
                        provider: Some(ComputeProvider {
                            provider_type: "test-remote".to_owned(),
                            details: None,
                            nexus_endpoint: "provider".to_owned(),
                        }),
                        scaler: Some(ComputeScaler {
                            scaler_type: "no-sync".to_owned(),
                            details: None,
                        }),
                    },
                )]),
            },
            last_modifier_identity: "test".to_owned(),
            polled_task_queues: BTreeSet::new(),
            create_request_ids: BTreeSet::new(),
            compute_config_request_ids: BTreeSet::new(),
        };
        StoredWorkerDeployment {
            namespace_id,
            name: DeploymentName("deployment".to_owned()),
            create_time: OffsetDateTime::UNIX_EPOCH,
            routing_config: StoredRoutingConfig::default(),
            last_modifier_identity: "test".to_owned(),
            manager_identity: None,
            routing_config_update_state: RoutingConfigUpdateState::Completed,
            versions: BTreeMap::from([(version.build_id.clone(), version)]),
            conflict_token: ConflictToken::default(),
            create_request_ids: BTreeSet::new(),
        }
    }

    fn provider_action(now: OffsetDateTime) -> WorkerComputeProviderAction {
        build_provider_action(ProviderActionInput {
            action_id: Uuid::new_v4(),
            controller_key: ControllerInstanceKey {
                namespace_id: NamespaceId::new(),
                deployment_name: DeploymentId("deployment".to_owned()),
                build_id: BuildId("build".to_owned()),
            },
            namespace_name: "namespace".to_owned(),
            scaling_group: ScalingGroupId("group".to_owned()),
            fingerprint: ConfigurationFingerprint::from_canonical_bytes(b"config"),
            provider: RemoteNexusProvider {
                provider_type: "test-remote".to_owned(),
                details: Some(Payload::new("opaque")),
                nexus_endpoint: "provider".to_owned(),
            },
            reason: WorkerComputeInvokeReason::NoSyncMatch,
            task_queues: Vec::new(),
            now,
        })
        .expect("provider action")
    }

    #[tokio::test]
    async fn enabled_service_delivers_startup_activation_and_shuts_down_cleanly() {
        let namespace_id = NamespaceId::new();
        let namespace = WorkerComputeNamespace {
            namespace_id,
            name: "namespace".to_owned(),
        };
        let deployment_repository = Arc::new(tokeira_storage::InMemoryStore::default());
        assert!(matches!(
            deployment_repository
                .put_deployment(deployment(namespace_id), None)
                .await
                .expect("deployment insert"),
            DeploymentCasResult::Applied { .. },
        ));
        let controller_repository = Arc::new(InMemoryWorkerComputeRepository::default());
        let owner = IncarnationId::new();
        let reconciler = WorkerComputeReconciler::new(
            deployment_repository.clone(),
            controller_repository.clone(),
            owner,
        );
        let sampler = WorkerComputeQueueSampler::new(
            deployment_repository,
            controller_repository.clone(),
            InMemoryBroker::default(),
            InMemoryActivityBroker::default(),
            NexusTaskBroker::default(),
            NexusEndpointRegistry::default(),
            WorkerComputeQueueMetrics::default(),
            owner,
        );
        let provider = Arc::new(InProcessWorkerComputeTestProvider::new([
            WorkerComputeProviderOutcome::Delivered,
        ]));
        let outbox = WorkerComputeOutbox::new(
            controller_repository.clone(),
            provider.clone(),
            Arc::new(SystemWorkerComputeClock),
            owner,
        );
        let (_observation_sender, observation_receiver) = mpsc::channel(1);
        let (_reconcile_sender, reconcile_receiver) = mpsc::channel(1);
        let service = WorkerComputeControllerService::new(
            Arc::new(StaticCatalog { namespace }),
            controller_repository.clone(),
            reconciler,
            sampler,
            outbox,
            Arc::new(SystemWorkerComputeClock),
            Arc::new(Vec::new),
            observation_receiver,
            reconcile_receiver,
        );
        let shutdown = CancellationToken::new();
        let running = {
            let shutdown = shutdown.clone();
            tokio::spawn(async move { service.run(shutdown).await })
        };
        provider.called.notified().await;
        shutdown.cancel();
        running
            .await
            .expect("service task")
            .expect("clean service shutdown");
        assert_eq!(provider.accepted().len(), 1);
        let health = controller_repository
            .list_health(namespace_id, WorkerComputeHealthFilter::default())
            .await
            .expect("health");
        assert_eq!(health.len(), 1);
        assert!(health[0].last_action_id.is_some());
        assert_eq!(health[0].last_failure_category, None);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        // Feature: worker-compute-controller, Property 1: disabled configuration is inert
        #[test]
        fn property_disabled_configuration_is_inert(
            observations in proptest::collection::vec(any::<bool>(), 0..100),
        ) {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime");
            runtime.block_on(async {
                let repository = InMemoryWorkerComputeRepository::default();
                let namespace_id = NamespaceId::new();
                for no_sync in observations {
                    let observation = DemandObservation {
                        namespace_id,
                        task_queue: tokeira_types::TaskQueueName("queue".to_owned()),
                        task_type: WorkerComputeTaskType::Workflow,
                        deployment_name: DeploymentId("deployment".to_owned()),
                        build_id: BuildId("build".to_owned()),
                        match_kind: if no_sync {
                            super::super::DemandMatchKind::NoSync
                        } else {
                            super::super::DemandMatchKind::Sync
                        },
                    };
                    prop_assert_eq!(
                        DisabledWorkerComputeSink.try_observe(observation),
                        ObserveResult::Disabled,
                    );
                    prop_assert_eq!(
                        DisabledWorkerComputeSink.try_reconcile(ControllerInstanceKey {
                            namespace_id,
                            deployment_name: DeploymentId("deployment".to_owned()),
                            build_id: BuildId("build".to_owned()),
                        }),
                        ObserveResult::Disabled,
                    );
                }
                prop_assert!(
                    repository
                        .list_controllers(namespace_id)
                        .await
                        .expect("disabled controller records")
                        .is_empty(),
                );
                prop_assert!(
                    repository
                        .claim_due_actions(
                            namespace_id,
                            IncarnationId::new(),
                            OffsetDateTime::UNIX_EPOCH,
                            OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1),
                            100,
                        )
                        .await
                        .expect("disabled actions")
                        .is_empty(),
                );
                Ok::<(), TestCaseError>(())
            })?;
        }

        // Feature: worker-compute-controller, Property 17: provider-neutral tests do not require Yadori or cloud state
        #[test]
        fn property_provider_neutral_harness_has_no_external_state(
            outcomes in proptest::collection::vec(0u8..3, 1..30),
        ) {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime");
            runtime.block_on(async {
                let expected = outcomes
                    .into_iter()
                    .map(|outcome| match outcome {
                        0 => WorkerComputeProviderOutcome::Delivered,
                        1 => WorkerComputeProviderOutcome::RetryableFailure(
                            WorkerComputeFailureCategory::Transport,
                        ),
                        _ => WorkerComputeProviderOutcome::TerminalFailure(
                            WorkerComputeFailureCategory::NonRetryableHandler,
                        ),
                    })
                    .collect::<Vec<_>>();
                let provider = InProcessWorkerComputeTestProvider::new(expected.clone());
                let action = provider_action(OffsetDateTime::UNIX_EPOCH);
                let mut accepted = false;
                for (index, expected_outcome) in expected.into_iter().enumerate() {
                    let actual = provider
                        .deliver(
                            &action,
                            u64::try_from(index).expect("small generated index"),
                            OffsetDateTime::UNIX_EPOCH,
                        )
                        .await
                        .outcome;
                    if accepted {
                        prop_assert_eq!(actual, WorkerComputeProviderOutcome::Delivered);
                    } else {
                        prop_assert_eq!(actual, expected_outcome);
                        accepted = actual == WorkerComputeProviderOutcome::Delivered;
                    }
                }
                let calls = provider.calls();
                let request_is_stable = calls.iter().all(|(action_id, bytes)| {
                    *action_id == action.action_id && *bytes == action.request_data
                });
                prop_assert!(request_is_stable);
                prop_assert!(provider.accepted().len() <= 1);
                Ok::<(), TestCaseError>(())
            })?;
        }
    }
}
