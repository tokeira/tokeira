//! Public runtime orchestration surface.
//!
//! This module is the narrow waist between transports and the kernel/storage
//! internals. Callers ask the runtime to start workflows, complete tasks,
//! buffer consistent queries, and manage background scanners; they do not
//! reach into lane execution or storage tables directly.
//!
//! The durable source of truth still lives below this layer. The runtime's job
//! is to route work, retry OCC conflicts, and keep the in-memory delivery and
//! timeout machinery aligned with authoritative history.

use std::sync::{Arc, Mutex, RwLock};

use anyhow::{Result, anyhow};
use smallvec::{SmallVec, smallvec};
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    ActivityOp, ActivityResolution, ActivityResolvedRequest, BasicKernel, Command, DispatchOp,
    HistoryEvent, HistoryEventKind, LoadedRun, SignalRequest, SignalWithStartRequest, StartRequest,
    StartWorkflowTaskRequest, TerminateRequest, Transition, UpdateRequest,
    WorkflowIdConflictPolicy, WorkflowIdReusePolicy, WorkflowState, WorkflowTaskCompletedRequest,
};
use tokeira_storage::{
    CommitResult, DispatchableActivityTask, DispatchableWorkflowTask, LeaseOutcome,
    LeaseRepository, RunRepository,
};
use tokeira_types::{
    ActivityTaskToken, BuildId, DeploymentId, ExecutionRef, ExecutionStatus, Headers,
    HeartbeatStore, IncarnationId, NamespaceId, Payload, Payloads, QueueKey, RequestContext,
    RetryPolicy, RunId, RunKey, ShardEpoch, ShardId, TaskKind, TaskQueueName, TransitionSeq,
    WorkerIdentity, WorkflowTaskToken, execution_home_bundle,
};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    activity_timeout::{
        ActivityTimeoutScannerConfig, ActivityTrackingState, run_activity_timeout_scanner,
    },
    backlog::{BacklogConfig, run_drain_loop, run_grace_scanner},
    broker::{InMemoryActivityBroker, InMemoryBroker, ReservedPoller, WorkflowPollResult},
    buffered_queries::{BufferedQuery, BufferedQueryRegistry},
    drain::RuntimeDrain,
    errors::{ActivityTokenResolutionError, NotShardOwner},
    fairness::{DeliveryMetrics, FairnessState, run_control_loop},
    heartbeat::{InMemoryHeartbeatStore, spawn_heartbeat_maintenance},
    lane::{LaneConfig, LaneHandle, spawn_lane_with_id},
    membership::{ConnectionBudgetApplier, HeartbeatInputs, MembershipClient, MembershipConfig},
    metrics as runtime_metrics,
    nexus::{
        NexusEndpointRegistry, NexusHttpClient, NexusTaskBroker, NexusTimeoutScannerConfig,
        NexusTimeoutTrackingState, NoopNexusHttpClient, run_nexus_timeout_scanner,
    },
    publisher::RuntimeDispatchPublisher,
    query::{QueryResult, QueryTask},
    recovery::{lease_rejected_error, run_lease_renewer, sweep_shard},
    retry::{RetryDecision, evaluate_activity_retry},
    scanner::{
        TimerScannerConfig, lane_index_for_run_key, pick_lane_for_run_key, run_timer_scanner,
    },
    shard::{ShardOwner, shard_for},
    timeout::{
        WorkflowTimeoutEntry, WorkflowTimeoutScannerConfig, WorkflowTimeoutTrackingState,
        run_workflow_timeout_scanner,
    },
    update::{
        PendingUpdateTransport, UpdateOutcome, UpdateRegistry, UpdateResolution,
        UpdateTransportResolution, UpdateWaitPolicy,
    },
    versioning::VersioningRuleStore,
    wft_timeout::{
        WftTimeoutEntry, WftTimeoutScannerConfig, WftTimeoutTrackingState, run_wft_timeout_scanner,
    },
    worker_registry::{WorkerRegistrationKey, WorkerRegistry, WorkerVersionMetadata},
};

mod activity;
mod commit;
mod lifecycle;
mod membership;
mod query;
mod workflow_task;

/// Public runtime facade.
///
/// This is intentionally small. The point is to expose the
/// core server actions without dragging transport or
/// authentication into the same crate.
///
/// See [`docs/crates/runtime.md`] for the full
/// orchestration flow and module map.
pub struct TokeiraRuntime<R> {
    /// Shared handle to the durable run repository.
    repo: Arc<R>,
    /// In-memory workflow-task broker.
    broker: InMemoryBroker,
    /// In-memory activity-task broker.
    activity_broker: InMemoryActivityBroker,
    /// Lane executor handles (one per lane).
    lanes: Vec<LaneHandle>,
    /// Lane configuration shared across all lanes.
    config: LaneConfig,
    /// Background timer scanner task.
    timer_scanner_handle: Option<tokio::task::JoinHandle<()>>,
    /// Cancellation token for the timer scanner loop.
    timer_scanner_cancel: CancellationToken,
    /// Runtime-local workflow timeout tracking.
    workflow_timeout_tracking: WorkflowTimeoutTrackingState,
    /// Runtime-local workflow-task timeout tracking.
    wft_timeout_tracking: WftTimeoutTrackingState,
    /// Runtime-local activity timeout tracking.
    activity_tracking: ActivityTrackingState,
    /// Background workflow-timeout scanner task.
    workflow_timeout_scanner_handle: Option<tokio::task::JoinHandle<()>>,
    /// Cancellation token for the workflow-timeout scanner.
    workflow_timeout_scanner_cancel: CancellationToken,
    /// Background workflow-task-timeout scanner task.
    wft_timeout_scanner_handle: Option<tokio::task::JoinHandle<()>>,
    /// Cancellation token for the workflow-task-timeout scanner.
    wft_timeout_scanner_cancel: CancellationToken,
    /// Runtime-local Nexus timeout tracking.
    nexus_timeout_tracking: NexusTimeoutTrackingState,
    /// In-memory Nexus worker-task broker.
    nexus_task_broker: NexusTaskBroker,
    /// In-memory update caller registry.
    update_registry: UpdateRegistry,
    /// Run-local buffered consistent queries.
    buffered_queries: BufferedQueryRegistry,
    /// Observational registry of worker version metadata.
    worker_registry: WorkerRegistry,
    /// Latest worker-heartbeat observations for operator visibility.
    heartbeat_store: Arc<dyn HeartbeatStore>,
    /// Runtime-local delivery metrics for fairness/observability.
    delivery_metrics: DeliveryMetrics,
    /// Runtime-local backlog fairness state.
    fairness_state: FairnessState,
    /// Shared worker-versioning rules used by edge handlers and dispatch.
    versioning_rule_store: Arc<VersioningRuleStore>,
    /// Background Nexus-timeout scanner task.
    nexus_timeout_scanner_handle: Option<tokio::task::JoinHandle<()>>,
    /// Cancellation token for the Nexus-timeout scanner.
    nexus_timeout_scanner_cancel: CancellationToken,
    /// Background activity-timeout scanner task.
    activity_timeout_scanner_handle: Option<tokio::task::JoinHandle<()>>,
    /// Cancellation token for the activity-timeout scanner.
    activity_timeout_scanner_cancel: CancellationToken,
    /// Background grace scanner task.
    grace_scanner_handle: Option<tokio::task::JoinHandle<()>>,
    /// Cancellation token for the grace scanner.
    grace_scanner_cancel: CancellationToken,
    /// Background backlog drain task.
    drain_loop_handle: Option<tokio::task::JoinHandle<()>>,
    /// Cancellation token for the drain loop.
    drain_loop_cancel: CancellationToken,
    /// Background fairness control loop task.
    control_loop_handle: Option<tokio::task::JoinHandle<()>>,
    /// Cancellation token for the fairness control loop.
    control_loop_cancel: CancellationToken,
    /// Background worker-heartbeat maintenance task.
    heartbeat_maintenance_handle: Option<tokio::task::JoinHandle<()>>,
    /// Cancellation token for worker-heartbeat maintenance.
    heartbeat_maintenance_cancel: CancellationToken,
    /// Runtime-local shard ownership view.
    shard_owner: Arc<RwLock<ShardOwner>>,
    /// Shared runtime drain state used by membership and admission.
    runtime_drain: Arc<RuntimeDrain>,
    /// Stable owner identity for lease operations.
    owner_identity: String,
    /// Endpoint persisted with lease rows for controller-sourced routing.
    node_endpoint: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResetWorkflowResult {
    pub successor_run_key: RunKey,
    pub successor_run_id: RunId,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MutationMetadata {
    pub transition_seq: TransitionSeq,
    pub last_event_id: i64,
    pub execution_status: ExecutionStatus,
}

#[derive(Clone, Debug, PartialEq)]
pub enum StartWorkflowResult {
    Started {
        run_key: RunKey,
        run_id: RunId,
        mutation_metadata: MutationMetadata,
    },
    UsedExisting {
        run_key: RunKey,
        run_id: RunId,
    },
    Rejected {
        run_key: RunKey,
        run_id: RunId,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum SignalWithStartResult {
    Started { run_key: RunKey, run_id: RunId },
    Signaled { run_key: RunKey, run_id: RunId },
    Rejected { run_key: RunKey, run_id: RunId },
}

#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeConfig {
    pub lane_count: usize,
    pub lane: LaneConfig,
    pub timer_scanner: TimerScannerConfig,
    pub workflow_timeout_scanner: WorkflowTimeoutScannerConfig,
    pub backlog: BacklogConfig,
    pub activity_timeout_scanner: ActivityTimeoutScannerConfig,
    pub nexus_timeout_scanner: NexusTimeoutScannerConfig,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            lane_count: 32,
            lane: LaneConfig::default(),
            timer_scanner: TimerScannerConfig::default(),
            workflow_timeout_scanner: WorkflowTimeoutScannerConfig::default(),
            backlog: BacklogConfig::default(),
            activity_timeout_scanner: ActivityTimeoutScannerConfig::default(),
            nexus_timeout_scanner: NexusTimeoutScannerConfig::default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum ConflictResolution {
    Absent,
    UseExisting { run_key: RunKey, run_id: RunId },
    TerminateAndStart { run_key: RunKey },
    ClosedAllowReuse,
    Rejected { run_key: RunKey, run_id: RunId },
}

struct BufferedQueryCleanup {
    registry: BufferedQueryRegistry,
    run_key: RunKey,
    query_id: String,
    enabled: bool,
}

impl BufferedQueryCleanup {
    fn disarm(mut self) {
        self.enabled = false;
    }
}

impl Drop for BufferedQueryCleanup {
    fn drop(&mut self) {
        if self.enabled {
            let _ = self.registry.remove(self.run_key, &self.query_id);
        }
    }
}

impl<R> TokeiraRuntime<R>
where
    R: RunRepository + 'static,
{
    /// Create a new runtime with `lane_count` parallel
    /// lane executors backed by `repo`.
    pub fn new(
        repo: Arc<R>,
        lane_count: usize,
        config: LaneConfig,
        timer_config: TimerScannerConfig,
        workflow_timeout_config: WorkflowTimeoutScannerConfig,
        backlog_config: BacklogConfig,
    ) -> Self {
        let versioning_rule_store = Arc::new(VersioningRuleStore::default());
        Self::new_with_versioning(
            repo,
            lane_count,
            config,
            timer_config,
            workflow_timeout_config,
            backlog_config,
            versioning_rule_store,
        )
    }

    pub fn new_with_config(repo: Arc<R>, runtime_config: RuntimeConfig) -> Self {
        Self::new(
            repo,
            runtime_config.lane_count,
            runtime_config.lane,
            runtime_config.timer_scanner,
            runtime_config.workflow_timeout_scanner,
            runtime_config.backlog,
        )
    }

    pub fn new_with_versioning(
        repo: Arc<R>,
        lane_count: usize,
        config: LaneConfig,
        timer_config: TimerScannerConfig,
        workflow_timeout_config: WorkflowTimeoutScannerConfig,
        backlog_config: BacklogConfig,
        versioning_rule_store: Arc<VersioningRuleStore>,
    ) -> Self {
        Self::new_with_nexus_and_versioning(
            repo,
            lane_count,
            config,
            timer_config,
            workflow_timeout_config,
            backlog_config,
            ActivityTimeoutScannerConfig::default(),
            NexusTimeoutScannerConfig::default(),
            NexusEndpointRegistry::default(),
            Arc::new(NoopNexusHttpClient),
            versioning_rule_store,
        )
    }

    pub fn new_with_nexus(
        repo: Arc<R>,
        lane_count: usize,
        config: LaneConfig,
        timer_config: TimerScannerConfig,
        workflow_timeout_config: WorkflowTimeoutScannerConfig,
        backlog_config: BacklogConfig,
        activity_timeout_config: ActivityTimeoutScannerConfig,
        nexus_timeout_config: NexusTimeoutScannerConfig,
        nexus_registry: NexusEndpointRegistry,
        nexus_client: Arc<dyn NexusHttpClient>,
    ) -> Self {
        Self::new_with_nexus_and_versioning(
            repo,
            lane_count,
            config,
            timer_config,
            workflow_timeout_config,
            backlog_config,
            activity_timeout_config,
            nexus_timeout_config,
            nexus_registry,
            nexus_client,
            Arc::new(VersioningRuleStore::default()),
        )
    }

    pub fn new_with_nexus_and_versioning(
        repo: Arc<R>,
        lane_count: usize,
        config: LaneConfig,
        timer_config: TimerScannerConfig,
        workflow_timeout_config: WorkflowTimeoutScannerConfig,
        backlog_config: BacklogConfig,
        activity_timeout_config: ActivityTimeoutScannerConfig,
        nexus_timeout_config: NexusTimeoutScannerConfig,
        nexus_registry: NexusEndpointRegistry,
        nexus_client: Arc<dyn NexusHttpClient>,
        versioning_rule_store: Arc<VersioningRuleStore>,
    ) -> Self {
        Self::new_with_nexus_and_shards(
            repo,
            lane_count,
            config,
            timer_config,
            workflow_timeout_config,
            backlog_config,
            activity_timeout_config,
            nexus_timeout_config,
            nexus_registry,
            nexus_client,
            1,
            IncarnationId::new().to_string(),
            true,
            versioning_rule_store,
        )
    }

    pub fn new_with_nexus_and_versioning_config(
        repo: Arc<R>,
        runtime_config: RuntimeConfig,
        nexus_registry: NexusEndpointRegistry,
        nexus_client: Arc<dyn NexusHttpClient>,
        versioning_rule_store: Arc<VersioningRuleStore>,
    ) -> Self {
        Self::new_with_nexus_and_versioning(
            repo,
            runtime_config.lane_count,
            runtime_config.lane,
            runtime_config.timer_scanner,
            runtime_config.workflow_timeout_scanner,
            runtime_config.backlog,
            runtime_config.activity_timeout_scanner,
            runtime_config.nexus_timeout_scanner,
            nexus_registry,
            nexus_client,
            versioning_rule_store,
        )
    }

    pub fn new_with_nexus_and_shards(
        repo: Arc<R>,
        lane_count: usize,
        config: LaneConfig,
        timer_config: TimerScannerConfig,
        workflow_timeout_config: WorkflowTimeoutScannerConfig,
        backlog_config: BacklogConfig,
        activity_timeout_config: ActivityTimeoutScannerConfig,
        nexus_timeout_config: NexusTimeoutScannerConfig,
        nexus_registry: NexusEndpointRegistry,
        nexus_client: Arc<dyn NexusHttpClient>,
        shard_count: u32,
        owner_identity: String,
        seed_default_shard: bool,
        versioning_rule_store: Arc<VersioningRuleStore>,
    ) -> Self {
        Self::new_with_nexus_and_shards_and_endpoint(
            repo,
            lane_count,
            config,
            timer_config,
            workflow_timeout_config,
            backlog_config,
            activity_timeout_config,
            nexus_timeout_config,
            nexus_registry,
            nexus_client,
            shard_count,
            owner_identity,
            "127.0.0.1:0".to_owned(),
            seed_default_shard,
            versioning_rule_store,
        )
    }

    pub fn new_with_nexus_and_shards_and_endpoint(
        repo: Arc<R>,
        lane_count: usize,
        config: LaneConfig,
        timer_config: TimerScannerConfig,
        workflow_timeout_config: WorkflowTimeoutScannerConfig,
        backlog_config: BacklogConfig,
        activity_timeout_config: ActivityTimeoutScannerConfig,
        nexus_timeout_config: NexusTimeoutScannerConfig,
        nexus_registry: NexusEndpointRegistry,
        nexus_client: Arc<dyn NexusHttpClient>,
        shard_count: u32,
        owner_identity: String,
        node_endpoint: String,
        seed_default_shard: bool,
        versioning_rule_store: Arc<VersioningRuleStore>,
    ) -> Self {
        let broker = InMemoryBroker::default();
        let activity_broker = InMemoryActivityBroker::default();
        let workflow_timeout_tracking = WorkflowTimeoutTrackingState::default();
        let wft_timeout_tracking = WftTimeoutTrackingState::default();
        let activity_tracking = ActivityTrackingState::default();
        let nexus_timeout_tracking = NexusTimeoutTrackingState::default();
        let nexus_task_broker = NexusTaskBroker::default();
        let update_registry = UpdateRegistry::new();
        let buffered_queries = BufferedQueryRegistry::default();
        let worker_registry = WorkerRegistry::default();
        let heartbeat_store: Arc<dyn HeartbeatStore> = Arc::new(InMemoryHeartbeatStore::new());
        let delivery_metrics = DeliveryMetrics::new();
        let fairness_state = FairnessState::new();
        let runtime_drain = Arc::new(RuntimeDrain::default());
        let shard_count = shard_count.max(1);
        let shard_owner = Arc::new(RwLock::new(ShardOwner::new(shard_count)));
        let lane_count = lane_count.max(1);
        let shared_lanes = Arc::new(Mutex::new(Vec::with_capacity(lane_count)));
        let lanes: Vec<_> = (0..lane_count)
            .map(|lane_id| {
                let publisher = RuntimeDispatchPublisher::new(
                    broker.clone(),
                    activity_broker.clone(),
                    repo.clone(),
                    shared_lanes.clone(),
                    lane_count,
                    shard_count,
                    nexus_client.clone(),
                    nexus_registry.clone(),
                    nexus_task_broker.clone(),
                    nexus_timeout_tracking.clone(),
                    activity_tracking.clone(),
                    delivery_metrics.clone(),
                    Some(versioning_rule_store.clone()),
                );
                spawn_lane_with_id(
                    lane_id,
                    BasicKernel,
                    repo.clone(),
                    publisher,
                    shard_owner.clone(),
                    activity_tracking.clone(),
                    workflow_timeout_tracking.clone(),
                    wft_timeout_tracking.clone(),
                    nexus_timeout_tracking.clone(),
                    update_registry.clone(),
                    config.clone(),
                )
            })
            .collect();
        *shared_lanes.lock().unwrap() = lanes.clone();
        if seed_default_shard {
            let mut owner = shard_owner.write().unwrap();
            let shard_id = ShardId(0);
            let _ = owner.record_acquired(shard_id, ShardEpoch::ZERO);
            owner.mark_active(shard_id);
        }
        let timer_scanner_cancel = CancellationToken::new();
        let timer_scanner_handle = Some(tokio::spawn(run_timer_scanner(
            repo.clone(),
            lanes.clone(),
            lane_count,
            shard_owner.clone(),
            timer_config,
            timer_scanner_cancel.clone(),
        )));
        let workflow_timeout_scanner_cancel = CancellationToken::new();
        let workflow_timeout_scanner_handle = Some(tokio::spawn(run_workflow_timeout_scanner(
            workflow_timeout_tracking.clone(),
            lanes.clone(),
            lane_count,
            shard_owner.clone(),
            workflow_timeout_config,
            workflow_timeout_scanner_cancel.clone(),
        )));
        let wft_timeout_scanner_cancel = CancellationToken::new();
        let wft_timeout_scanner_handle = Some(tokio::spawn(run_wft_timeout_scanner(
            wft_timeout_tracking.clone(),
            lanes.clone(),
            lane_count,
            shard_owner.clone(),
            WftTimeoutScannerConfig::default(),
            wft_timeout_scanner_cancel.clone(),
        )));
        let activity_timeout_scanner_cancel = CancellationToken::new();
        let activity_timeout_scanner_handle = Some(tokio::spawn(run_activity_timeout_scanner(
            repo.clone(),
            activity_tracking.clone(),
            lanes.clone(),
            lane_count,
            shard_owner.clone(),
            activity_timeout_config,
            activity_timeout_scanner_cancel.clone(),
        )));
        let grace_scanner_cancel = CancellationToken::new();
        let grace_scanner_handle = Some(tokio::spawn(run_grace_scanner(
            broker.clone(),
            activity_broker.clone(),
            repo.clone(),
            backlog_config.clone(),
            grace_scanner_cancel.clone(),
        )));
        let drain_loop_cancel = CancellationToken::new();
        let drain_loop_handle = Some(tokio::spawn(run_drain_loop(
            broker.clone(),
            activity_broker.clone(),
            repo.clone(),
            backlog_config,
            fairness_state.clone(),
            delivery_metrics.clone(),
            drain_loop_cancel.clone(),
        )));
        let control_loop_cancel = CancellationToken::new();
        let control_loop_handle = Some(tokio::spawn(run_control_loop(
            delivery_metrics.clone(),
            fairness_state.clone(),
            control_loop_cancel.clone(),
        )));
        let heartbeat_maintenance_cancel = CancellationToken::new();
        let heartbeat_maintenance_handle = Some(spawn_heartbeat_maintenance(
            heartbeat_store.clone(),
            heartbeat_maintenance_cancel.clone(),
        ));
        let nexus_timeout_scanner_cancel = CancellationToken::new();
        let nexus_timeout_scanner_handle = Some(tokio::spawn(run_nexus_timeout_scanner(
            nexus_timeout_tracking.clone(),
            lanes.clone(),
            lane_count,
            shard_owner.clone(),
            nexus_timeout_config,
            nexus_timeout_scanner_cancel.clone(),
        )));
        Self {
            repo,
            broker,
            activity_broker,
            lanes,
            config,
            timer_scanner_handle,
            timer_scanner_cancel,
            workflow_timeout_tracking,
            wft_timeout_tracking,
            activity_tracking,
            workflow_timeout_scanner_handle,
            workflow_timeout_scanner_cancel,
            wft_timeout_scanner_handle,
            wft_timeout_scanner_cancel,
            nexus_timeout_tracking,
            nexus_task_broker,
            update_registry,
            buffered_queries,
            worker_registry,
            heartbeat_store,
            delivery_metrics,
            fairness_state,
            nexus_timeout_scanner_handle,
            nexus_timeout_scanner_cancel,
            activity_timeout_scanner_handle,
            activity_timeout_scanner_cancel,
            grace_scanner_handle,
            grace_scanner_cancel,
            drain_loop_handle,
            drain_loop_cancel,
            control_loop_handle,
            control_loop_cancel,
            heartbeat_maintenance_handle,
            heartbeat_maintenance_cancel,
            shard_owner,
            runtime_drain,
            owner_identity,
            node_endpoint,
            versioning_rule_store,
        }
    }

    /// Return a clone of the workflow-task broker.
    pub fn broker(&self) -> InMemoryBroker {
        self.broker.clone()
    }

    /// Return a clone of the activity-task broker.
    pub fn activity_broker(&self) -> InMemoryActivityBroker {
        self.activity_broker.clone()
    }

    pub fn buffered_queries(&self) -> BufferedQueryRegistry {
        self.buffered_queries.clone()
    }

    /// Return a shared reference to the run repository.
    pub fn repo(&self) -> Arc<R> {
        self.repo.clone()
    }

    pub fn delivery_metrics(&self) -> DeliveryMetrics {
        self.delivery_metrics.clone()
    }

    pub fn fairness_state(&self) -> FairnessState {
        self.fairness_state.clone()
    }

    pub fn workflow_timeout_tracking(&self) -> WorkflowTimeoutTrackingState {
        self.workflow_timeout_tracking.clone()
    }

    pub fn wft_timeout_tracking(&self) -> WftTimeoutTrackingState {
        self.wft_timeout_tracking.clone()
    }

    pub fn activity_tracking(&self) -> ActivityTrackingState {
        self.activity_tracking.clone()
    }

    pub fn nexus_timeout_tracking(&self) -> NexusTimeoutTrackingState {
        self.nexus_timeout_tracking.clone()
    }

    pub fn nexus_task_broker(&self) -> NexusTaskBroker {
        self.nexus_task_broker.clone()
    }

    pub fn register_worker(
        &self,
        worker_identity: WorkerIdentity,
        namespace_id: NamespaceId,
        task_queue: TaskQueueName,
        deployment: Option<DeploymentId>,
        build_id: Option<BuildId>,
    ) {
        self.worker_registry.register(
            WorkerRegistrationKey {
                worker_identity,
                namespace_id,
                task_queue,
            },
            WorkerVersionMetadata {
                deployment,
                build_id,
                last_seen_at: Some(OffsetDateTime::now_utc()),
            },
        );
    }

    pub fn worker_registry(&self) -> WorkerRegistry {
        self.worker_registry.clone()
    }

    pub fn heartbeat_store(&self) -> Arc<dyn HeartbeatStore> {
        self.heartbeat_store.clone()
    }

    pub fn versioning_rule_store(&self) -> Arc<VersioningRuleStore> {
        self.versioning_rule_store.clone()
    }

    pub fn update_registry(&self) -> UpdateRegistry {
        self.update_registry.clone()
    }

    pub fn pending_update_transports(&self, run_key: RunKey) -> Vec<PendingUpdateTransport> {
        self.update_registry
            .drain_pending_updates(run_key)
            .into_iter()
            .map(
                |(update_id, update_name, input, identity)| PendingUpdateTransport {
                    update_id,
                    update_name,
                    input,
                    identity,
                },
            )
            .collect()
    }

    pub fn resolve_update_transport(
        &self,
        run_key: RunKey,
        update_id: &str,
        resolution: UpdateTransportResolution,
    ) -> bool {
        match resolution {
            UpdateTransportResolution::Accepted => true,
            UpdateTransportResolution::Completed { result } => self.update_registry.notify(
                run_key,
                update_id,
                UpdateResolution::Completed { result },
            ),
            UpdateTransportResolution::Rejected { failure } => self.update_registry.notify(
                run_key,
                update_id,
                UpdateResolution::Rejected { failure },
            ),
        }
    }

    pub fn owner_identity(&self) -> &str {
        &self.owner_identity
    }

    pub fn node_endpoint(&self) -> &str {
        &self.node_endpoint
    }

    pub fn runtime_drain(&self) -> Arc<RuntimeDrain> {
        self.runtime_drain.clone()
    }

    pub fn heartbeat_inputs(
        &self,
        available_connections: u32,
        connection_rate_headroom: f32,
    ) -> HeartbeatInputs {
        HeartbeatInputs::from_runtime_components(
            &self.shard_owner.read().unwrap(),
            self.runtime_drain.state(),
            &self.lanes,
            available_connections,
            connection_rate_headroom,
        )
    }

    pub(super) async fn validate_workflow_task_token(
        &self,
        token: &WorkflowTaskToken,
    ) -> Result<()> {
        let current_epoch = self.shard_epoch_for_completion(token.run_key).await?;
        if token.shard_epoch != current_epoch {
            let shard_id = self.shard_id_for(token.run_key).await;
            return Err(NotShardOwner::local(shard_id, current_epoch).into());
        }
        Ok(())
    }

    pub(super) fn pick_lane(&self, run_key: RunKey) -> &LaneHandle {
        pick_lane_for_run_key(&self.lanes, self.lanes.len(), run_key)
    }

    #[cfg(test)]
    pub(super) fn lane_index(&self, run_key: RunKey) -> usize {
        crate::scanner::lane_index_for_run_key(run_key, self.lanes.len())
    }

    /// Cancel the background timer scanner and wait for
    /// it to stop.
    pub async fn shutdown_timer_scanner(&mut self) -> Result<()> {
        self.timer_scanner_cancel.cancel();
        if let Some(handle) = self.timer_scanner_handle.take() {
            tokio::time::timeout(tokio::time::Duration::from_secs(5), handle)
                .await
                .map_err(|_| anyhow!("timer scanner shutdown timed out"))?
                .map_err(|error| anyhow!("timer scanner join failed: {error}"))?;
        }
        Ok(())
    }

    pub async fn shutdown_workflow_timeout_scanner(&mut self) -> Result<()> {
        self.workflow_timeout_scanner_cancel.cancel();
        if let Some(handle) = self.workflow_timeout_scanner_handle.take() {
            tokio::time::timeout(tokio::time::Duration::from_secs(5), handle)
                .await
                .map_err(|_| anyhow!("workflow timeout scanner shutdown timed out"))?
                .map_err(|error| anyhow!("workflow timeout scanner join failed: {error}"))?;
        }
        Ok(())
    }

    pub async fn shutdown_wft_timeout_scanner(&mut self) -> Result<()> {
        self.wft_timeout_scanner_cancel.cancel();
        if let Some(handle) = self.wft_timeout_scanner_handle.take() {
            tokio::time::timeout(tokio::time::Duration::from_secs(5), handle)
                .await
                .map_err(|_| anyhow!("wft timeout scanner shutdown timed out"))?
                .map_err(|error| anyhow!("wft timeout scanner join failed: {error}"))?;
        }
        Ok(())
    }

    pub async fn shutdown_nexus_timeout_scanner(&mut self) -> Result<()> {
        self.nexus_timeout_scanner_cancel.cancel();
        if let Some(handle) = self.nexus_timeout_scanner_handle.take() {
            tokio::time::timeout(tokio::time::Duration::from_secs(5), handle)
                .await
                .map_err(|_| anyhow!("nexus timeout scanner shutdown timed out"))?
                .map_err(|error| anyhow!("nexus timeout scanner join failed: {error}"))?;
        }
        Ok(())
    }

    pub async fn shutdown_activity_timeout_scanner(&mut self) -> Result<()> {
        self.activity_timeout_scanner_cancel.cancel();
        if let Some(handle) = self.activity_timeout_scanner_handle.take() {
            tokio::time::timeout(tokio::time::Duration::from_secs(5), handle)
                .await
                .map_err(|_| anyhow!("activity timeout scanner shutdown timed out"))?
                .map_err(|error| anyhow!("activity timeout scanner join failed: {error}"))?;
        }
        Ok(())
    }

    pub async fn shutdown_grace_scanner(&mut self) -> Result<()> {
        self.grace_scanner_cancel.cancel();
        if let Some(handle) = self.grace_scanner_handle.take() {
            tokio::time::timeout(tokio::time::Duration::from_secs(5), handle)
                .await
                .map_err(|_| anyhow!("grace scanner shutdown timed out"))?
                .map_err(|error| anyhow!("grace scanner join failed: {error}"))?;
        }
        Ok(())
    }

    pub async fn shutdown_drain_loop(&mut self) -> Result<()> {
        self.drain_loop_cancel.cancel();
        if let Some(handle) = self.drain_loop_handle.take() {
            tokio::time::timeout(tokio::time::Duration::from_secs(5), handle)
                .await
                .map_err(|_| anyhow!("drain loop shutdown timed out"))?
                .map_err(|error| anyhow!("drain loop join failed: {error}"))?;
        }
        Ok(())
    }

    pub async fn shutdown_control_loop(&mut self) -> Result<()> {
        self.control_loop_cancel.cancel();
        if let Some(handle) = self.control_loop_handle.take() {
            tokio::time::timeout(tokio::time::Duration::from_secs(5), handle)
                .await
                .map_err(|_| anyhow!("control loop shutdown timed out"))?
                .map_err(|error| anyhow!("control loop join failed: {error}"))?;
        }
        Ok(())
    }

    pub async fn shutdown_heartbeat_maintenance(&mut self) -> Result<()> {
        self.heartbeat_maintenance_cancel.cancel();
        if let Some(handle) = self.heartbeat_maintenance_handle.take() {
            tokio::time::timeout(tokio::time::Duration::from_secs(5), handle)
                .await
                .map_err(|_| anyhow!("heartbeat maintenance shutdown timed out"))?
                .map_err(|error| anyhow!("heartbeat maintenance join failed: {error}"))?;
        }
        Ok(())
    }

    /// Sweep helper used by recovery/admin flows.
    ///
    /// Re-publishes up to `limit` dispatchable workflow
    /// tasks from durable storage into the in-memory
    /// broker.
    pub async fn republish_queue(&self, queue: QueueKey, limit: usize) -> Result<usize> {
        let tasks = self
            .repo
            .list_dispatchable_workflow_tasks(&queue, limit)
            .await?;
        let count = tasks.len();
        for task in tasks {
            self.broker
                .publish_workflow_task(task, Some(&self.delivery_metrics))
                .await;
        }
        Ok(count)
    }

    /// Like [`republish_queue`](Self::republish_queue) but
    /// for activity tasks.
    pub async fn republish_activity_queue(&self, queue: QueueKey, limit: usize) -> Result<usize> {
        let tasks = self
            .repo
            .list_dispatchable_activity_tasks(&queue, limit)
            .await?;
        let count = tasks.len();
        for task in tasks {
            self.activity_broker
                .publish_activity_task(task, Some(&self.delivery_metrics))
                .await?;
        }
        Ok(count)
    }
}

fn is_externally_routed_command(command: &Command) -> bool {
    matches!(
        command,
        Command::Start(_)
            | Command::SignalWithStart(_)
            | Command::Update(_)
            | Command::Signal(_)
            | Command::Cancel(_)
            | Command::Terminate(_)
            | Command::Reset(_)
            | Command::PauseWorkflow(_)
            | Command::UnpauseWorkflow(_)
            | Command::UpdateActivityOptions(_)
            | Command::PauseActivity(_)
            | Command::UnpauseActivity(_)
            | Command::ResetActivity(_)
            | Command::UpdateExecutionOptions(_)
    )
}

fn mutation_metadata(state: &WorkflowState) -> MutationMetadata {
    MutationMetadata {
        transition_seq: state.transition_seq,
        last_event_id: state.last_event_id,
        execution_status: state.status,
    }
}

/// A workflow task that has been polled and started.
#[derive(Clone, Debug, PartialEq)]
pub struct StartedWorkflowTask {
    /// Unique key for the workflow run.
    pub run_key: RunKey,
    /// Human-readable workflow identifier.
    pub workflow_id: tokeira_types::WorkflowId,
    /// Task queue the task was dispatched on.
    pub task_queue: TaskQueueName,
    /// started_event_id of the most recently completed
    /// workflow task.
    pub previous_started_event_id: i64,
    /// Whether this task was delivered to the worker that owns sticky cache.
    pub is_sticky_match: bool,
    /// Timestamp of the scheduling event for this task.
    pub scheduled_time: OffsetDateTime,
    /// Timestamp of the start event for this task.
    pub started_time: OffsetDateTime,
    /// Opaque token used to complete the task.
    pub token: WorkflowTaskToken,
}

/// An activity task that has been polled and started.
#[derive(Clone, Debug)]
pub struct StartedActivityTask {
    /// Unique key for the owning workflow run.
    pub run_key: RunKey,
    /// Identifier of the activity within the workflow.
    pub activity_id: String,
    /// Activity type name.
    pub activity_type: String,
    /// Task queue the task was dispatched on.
    pub task_queue: TaskQueueName,
    /// Opaque token used to complete or fail the task.
    pub token: ActivityTaskToken,
    /// Input payloads passed to the activity.
    pub input: Payloads,
    /// Current attempt number (starts at 1).
    pub attempt: u32,
    /// Human-readable workflow identifier.
    pub workflow_id: String,
    /// Workflow type name.
    pub workflow_type: String,
    /// Namespace name or identifier string.
    pub workflow_namespace: String,
    /// Transport headers carried with the activity task.
    pub header: Option<Headers>,
    /// Retry policy attached to the activity.
    pub retry_policy: Option<RetryPolicy>,
    /// Maximum time from schedule to close.
    pub schedule_to_close_timeout: Option<Duration>,
    /// Maximum time from start to close.
    pub start_to_close_timeout: Option<Duration>,
    /// Heartbeat interval; missing heartbeats trigger
    /// a timeout.
    pub heartbeat_timeout: Option<Duration>,
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };

    use proptest::prelude::*;
    use tokio::runtime::Runtime;
    use uuid::Uuid;

    use super::*;
    use crate::{
        broker::InMemoryBroker,
        drain::RuntimeDrainState,
        lane::DispatchPublisher,
        nexus::{
            EndpointTarget, NexusEndpointConfig, NexusEndpointRegistry, NexusTaskBroker,
            NexusTimeoutEntry, NexusTimeoutScannerConfig, NexusTimeoutTrackingState,
            NoopNexusHttpClient, evaluate_nexus_timeout,
        },
        publisher::RuntimeDispatchPublisher,
        retry::{RetryDecision, compute_retry_backoff, evaluate_activity_retry},
        scanner::{TimerScannerConfig, lane_index_for_run_key, scan_due_timers_once},
        timeout::{
            WorkflowTimeoutEntry, WorkflowTimeoutScannerConfig, WorkflowTimeoutTrackingState,
            WorkflowTimeoutViolation, evaluate_workflow_timeout, scan_workflow_timeouts_once,
            workflow_timeout_retry_state,
        },
        versioning::{RedirectRule, VersioningMutation},
    };
    use std::collections::HashMap;
    use tokeira_kernel::RetryState;
    use tokeira_storage::{
        BacklogEntry, CommitResult, DispatchableWorkflowTask, InMemoryStore, RequestRecord,
        TransitionAuditRecord,
    };
    use tokeira_types::{
        ExecutionRef, LogicalTaskSeq, Memo, NamespaceId, Payloads, RequestContext, RequestId,
        SearchAttributes, TaskKind, WorkflowId,
    };

    proptest! {
        #[test]
        fn property_deterministic_shard_routing(run in any::<u128>(), lane_count in 1usize..16usize) {
            let rt = Runtime::new().unwrap();
            let (first, second) = rt.block_on(async move {
                let repo = Arc::new(InMemoryStore::default());
                let runtime = TokeiraRuntime::new(
                    repo,
                    lane_count,
                    LaneConfig::default(),
                    TimerScannerConfig::default(),
                    WorkflowTimeoutScannerConfig::default(),
                    BacklogConfig::default(),
                );
                let run_key = RunKey(Uuid::from_u128(run));
                (runtime.lane_index(run_key), runtime.lane_index(run_key))
            });
            prop_assert_eq!(first, second);
            prop_assert!(first < lane_count);
        }
    }

    #[tokio::test]
    async fn reserved_start_direct_delivery_suppresses_broker_dispatch_and_tracks_timeout() {
        let repo = Arc::new(InMemoryStore::default());
        let runtime = Arc::new(TokeiraRuntime::new(
            repo.clone(),
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
            BacklogConfig::default(),
        ));
        let request = sample_start_request(None, None);
        let queue = QueueKey {
            namespace_id: request.namespace_id,
            task_queue: request.task_queue.clone(),
            task_kind: TaskKind::Workflow,
            deployment: None,
            build_id: None,
        };
        let worker = WorkerIdentity("reserved-worker".to_string());
        let poll_runtime = runtime.clone();
        let poll_queue = queue.clone();
        let poll_worker = worker.clone();
        let poller = tokio::spawn(async move {
            poll_runtime
                .poll_workflow_task(poll_queue, poll_worker, tokio::time::Duration::from_secs(5))
                .await
                .unwrap()
                .expect("reserved poller should receive the started WFT")
        });

        while !runtime.broker.queues_with_waiters().await.contains(&queue) {
            tokio::task::yield_now().await;
        }

        let run_key = request.run_key;
        let result = runtime.start_workflow(request).await.unwrap();
        assert!(matches!(result, CommitResult::Applied { .. }));
        let started = poller.await.unwrap();
        assert_eq!(started.run_key, run_key);
        assert_eq!(started.token.started_event_id, 3);

        let leftover = runtime
            .broker
            .poll_workflow_task(
                &queue,
                &WorkerIdentity("other-worker".to_string()),
                tokio::time::Duration::ZERO,
            )
            .await
            .unwrap();
        assert_eq!(leftover, None);

        let tracked = runtime.wft_timeout_tracking.snapshot();
        assert_eq!(tracked.len(), 1);
        assert_eq!(tracked[0].run_key, run_key);
        assert_eq!(tracked[0].started_event_id, started.token.started_event_id);

        let history = repo.read_history(run_key, 0, 16).await.unwrap();
        assert!(matches!(
            history.get(1).map(|event| &event.kind),
            Some(HistoryEventKind::WorkflowTaskScheduled { .. })
        ));
        assert!(matches!(
            history.get(2).map(|event| &event.kind),
            Some(HistoryEventKind::WorkflowTaskStarted { identity, .. }) if identity == &worker
        ));
    }

    #[test]
    fn test_pick_lane_returns_run_key_routed_handle() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async move {
            let repo = Arc::new(InMemoryStore::with_shard_count(8));
            let runtime = TokeiraRuntime::new_with_nexus_and_shards(
                repo,
                4,
                LaneConfig::default(),
                TimerScannerConfig::default(),
                WorkflowTimeoutScannerConfig::default(),
                BacklogConfig::default(),
                ActivityTimeoutScannerConfig::default(),
                NexusTimeoutScannerConfig::default(),
                NexusEndpointRegistry::default(),
                Arc::new(NoopNexusHttpClient),
                8,
                "test-owner".to_string(),
                true,
                Arc::new(VersioningRuleStore::default()),
            );
            let run_key = RunKey(Uuid::from_u128(7));
            let lane_index = lane_index_for_run_key(run_key, runtime.lanes.len());
            let lane_ptr = runtime.pick_lane(run_key) as *const LaneHandle;
            let expected_ptr = &runtime.lanes[lane_index] as *const LaneHandle;

            assert_eq!(lane_ptr, expected_ptr);
        });
    }

    #[test]
    fn same_shard_run_keys_can_route_to_different_lanes() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async move {
            let shard_count = 8;
            let repo = Arc::new(InMemoryStore::with_shard_count(shard_count));
            let runtime = TokeiraRuntime::new_with_nexus_and_shards(
                repo,
                4,
                LaneConfig::default(),
                TimerScannerConfig::default(),
                WorkflowTimeoutScannerConfig::default(),
                BacklogConfig::default(),
                ActivityTimeoutScannerConfig::default(),
                NexusTimeoutScannerConfig::default(),
                NexusEndpointRegistry::default(),
                Arc::new(NoopNexusHttpClient),
                shard_count,
                "test-owner".to_string(),
                true,
                Arc::new(VersioningRuleStore::default()),
            );
            let first = RunKey(Uuid::from_u128(3));
            let first_shard = shard_for(first, shard_count);
            let first_lane = runtime.lane_index(first);
            let second = (4u128..100_000)
                .map(|value| RunKey(Uuid::from_u128(value)))
                .find(|candidate| {
                    shard_for(*candidate, shard_count) == first_shard
                        && runtime.lane_index(*candidate) != first_lane
                })
                .expect("same-shard run key on a different lane");

            assert_eq!(shard_for(second, shard_count), first_shard);
            assert_ne!(runtime.lane_index(first), runtime.lane_index(second));
        });
    }

    #[test]
    fn runtime_constructor_preserves_membership_identity_and_endpoint() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async move {
            let repo = Arc::new(InMemoryStore::with_shard_count(4));
            let node_id = IncarnationId::new();
            let runtime = TokeiraRuntime::new_with_nexus_and_shards_and_endpoint(
                repo,
                2,
                LaneConfig::default(),
                TimerScannerConfig::default(),
                WorkflowTimeoutScannerConfig::default(),
                BacklogConfig::default(),
                ActivityTimeoutScannerConfig::default(),
                NexusTimeoutScannerConfig::default(),
                NexusEndpointRegistry::default(),
                Arc::new(NoopNexusHttpClient),
                4,
                node_id.to_string(),
                "127.0.0.1:7233".to_owned(),
                true,
                Arc::new(VersioningRuleStore::default()),
            );

            assert_eq!(runtime.owner_identity(), node_id.to_string());
            assert_eq!(runtime.node_endpoint(), "127.0.0.1:7233");
            assert_eq!(
                runtime.heartbeat_inputs(5, 0.5).drain_state,
                RuntimeDrainState::Active
            );
        });
    }

    #[test]
    fn drain_admission_classification_separates_external_from_inflight_commands() {
        let start = Command::Start(sample_start_request(None, None));
        let completion = Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
            token: WorkflowTaskToken {
                run_key: RunKey::new(),
                logical_seq: LogicalTaskSeq(1),
                started_event_id: 1,
                attempt: 1,
                shard_epoch: ShardEpoch::ZERO,
            },
            identity: WorkerIdentity("worker".to_owned()),
            sdk_metadata: None,
            worker_version: None,
            commands: Vec::new(),
            force_new_workflow_task: false,
            now: OffsetDateTime::now_utc(),
        });

        assert!(is_externally_routed_command(&start));
        assert!(!is_externally_routed_command(&completion));
    }

    proptest! {
        #[test]
        fn property_idempotent_workflow_task_publication(run in any::<u128>(), logical_seq in 1u64..8u64) {
            let rt = Runtime::new().unwrap();
            let (first_some, second_none) = rt.block_on(async move {
                let broker = InMemoryBroker::default();
                let run_key = RunKey(Uuid::from_u128(run));
                let queue = QueueKey {
                    namespace_id: NamespaceId::new(),
                    task_queue: TaskQueueName("queue-a".to_string()),
                    task_kind: TaskKind::Workflow,
                    deployment: None,
                    build_id: None,
                };
                let task = DispatchableWorkflowTask {
                    run_key,
                    queue: queue.clone(),
                    logical_seq: LogicalTaskSeq(logical_seq),
                    sticky_preferred: None,
                    sticky_expires_at: None,
                };

                broker.publish_workflow_task(task.clone(), None).await;
                broker.publish_workflow_task(task, None).await;

                let worker = WorkerIdentity("worker-a".to_string());
                let first = broker
                    .poll_workflow_task(&queue, &worker, tokio::time::Duration::from_millis(1))
                    .await
                    .unwrap();
                let second = broker
                    .poll_workflow_task(&queue, &worker, tokio::time::Duration::from_millis(1))
                    .await
                    .unwrap();

                (first.is_some(), second.is_none())
            });
            prop_assert!(first_some);
            prop_assert!(second_none);
        }
    }

    #[test]
    fn evaluate_activity_retry_respects_attempt_limit_and_non_retryable_errors() {
        let policy = RetryPolicy {
            initial_interval: Duration::seconds(1),
            backoff_coefficient: 2.0,
            maximum_interval: Some(Duration::seconds(10)),
            maximum_attempts: 3,
            non_retryable_error_types: vec!["fatal".to_string()],
        };

        assert_eq!(
            evaluate_activity_retry(&policy, 1, None),
            RetryDecision::Retry { next_attempt: 2 }
        );
        assert_eq!(
            evaluate_activity_retry(&policy, 3, None),
            RetryDecision::Exhausted
        );
        assert_eq!(
            evaluate_activity_retry(&policy, 1, Some("fatal")),
            RetryDecision::Exhausted
        );
    }

    #[test]
    fn compute_retry_backoff_caps_at_maximum_interval() {
        let policy = RetryPolicy {
            initial_interval: Duration::seconds(2),
            backoff_coefficient: 3.0,
            maximum_interval: Some(Duration::seconds(10)),
            maximum_attempts: 0,
            non_retryable_error_types: Vec::new(),
        };

        assert_eq!(compute_retry_backoff(&policy, 1), Duration::seconds(2));
        assert_eq!(compute_retry_backoff(&policy, 2), Duration::seconds(6));
        assert_eq!(compute_retry_backoff(&policy, 3), Duration::seconds(10));
    }

    #[tokio::test]
    async fn runtime_dispatch_publisher_wires_activity_dispatch_to_broker() {
        let workflow_broker = InMemoryBroker::default();
        let activity_broker = InMemoryActivityBroker::default();
        let repo = Arc::new(MockTimerRepo::from_responses(Vec::new()));
        let publisher = RuntimeDispatchPublisher::new(
            workflow_broker,
            activity_broker.clone(),
            repo,
            Arc::new(Mutex::new(Vec::new())),
            1,
            1,
            Arc::new(NoopNexusHttpClient),
            NexusEndpointRegistry::default(),
            NexusTaskBroker::default(),
            NexusTimeoutTrackingState::default(),
            ActivityTrackingState::default(),
            DeliveryMetrics::new(),
            None,
        );
        let queue = QueueKey {
            namespace_id: NamespaceId::new(),
            task_queue: TaskQueueName("activity-q".to_string()),
            task_kind: TaskKind::Activity,
            deployment: None,
            build_id: None,
        };
        let run_key = RunKey::new();

        publisher
            .publish(
                run_key,
                &[DispatchOp::EnqueueActivityTask {
                    queue: queue.clone(),
                    activity_id: "activity-1".to_string(),
                    input: Payloads::default(),
                    schedule_event_id: 11,
                    attempt: 2,
                    schedule_to_close_timeout: None,
                    schedule_to_start_timeout: None,
                    start_to_close_timeout: None,
                    heartbeat_timeout: None,
                }],
            )
            .await
            .unwrap();

        let task = activity_broker
            .poll_activity_task(&queue, std::time::Duration::from_millis(5))
            .await
            .unwrap()
            .expect("activity dispatch should be published");
        assert_eq!(task.0.run_key, run_key);
        assert_eq!(task.0.activity_id, "activity-1");
        assert_eq!(task.0.attempt, 2);
    }

    #[tokio::test]
    async fn runtime_dispatch_publisher_applies_build_id_redirects() {
        let workflow_broker = InMemoryBroker::default();
        let activity_broker = InMemoryActivityBroker::default();
        let repo = Arc::new(MockTimerRepo::from_responses(Vec::new()));
        let versioning = Arc::new(VersioningRuleStore::default());
        let namespace_id = NamespaceId::new();
        let task_queue = TaskQueueName("workflow-q".to_string());
        let token = versioning
            .get_rules(namespace_id, &task_queue)
            .conflict_token;
        versioning
            .apply_mutation(
                namespace_id,
                &task_queue,
                token,
                VersioningMutation::AddRedirectRule {
                    rule: RedirectRule {
                        source_build_id: "old".to_string(),
                        target_build_id: "new".to_string(),
                        create_time: OffsetDateTime::UNIX_EPOCH,
                    },
                },
                OffsetDateTime::UNIX_EPOCH,
            )
            .unwrap();
        let publisher = RuntimeDispatchPublisher::new(
            workflow_broker.clone(),
            activity_broker,
            repo,
            Arc::new(Mutex::new(Vec::new())),
            1,
            1,
            Arc::new(NoopNexusHttpClient),
            NexusEndpointRegistry::default(),
            NexusTaskBroker::default(),
            NexusTimeoutTrackingState::default(),
            ActivityTrackingState::default(),
            DeliveryMetrics::new(),
            Some(versioning),
        );
        let original_queue = QueueKey {
            namespace_id,
            task_queue: task_queue.clone(),
            task_kind: TaskKind::Workflow,
            deployment: None,
            build_id: Some(BuildId("old".to_string())),
        };
        let redirected_queue = QueueKey {
            build_id: Some(BuildId("new".to_string())),
            ..original_queue.clone()
        };
        let run_key = RunKey::new();

        publisher
            .publish(
                run_key,
                &[DispatchOp::EnqueueWorkflowTask {
                    queue: original_queue,
                    logical_seq: LogicalTaskSeq(1),
                    sticky_preferred: None,
                }],
            )
            .await
            .unwrap();

        let task = workflow_broker
            .poll_workflow_task(
                &redirected_queue,
                &WorkerIdentity("worker-a".to_string()),
                std::time::Duration::from_millis(5),
            )
            .await
            .unwrap()
            .expect("redirected workflow task should deliver");
        let (task, _) = task.into_queued().expect("queued workflow task");
        assert_eq!(task.run_key, run_key);
        assert_eq!(task.queue.build_id, Some(BuildId("new".to_string())));
    }

    #[tokio::test]
    async fn runtime_dispatch_publisher_skips_redirect_for_deployment_pinned_queue() {
        let workflow_broker = InMemoryBroker::default();
        let activity_broker = InMemoryActivityBroker::default();
        let repo = Arc::new(MockTimerRepo::from_responses(Vec::new()));
        let versioning = Arc::new(VersioningRuleStore::default());
        let namespace_id = NamespaceId::new();
        let task_queue = TaskQueueName("workflow-q".to_string());
        let token = versioning
            .get_rules(namespace_id, &task_queue)
            .conflict_token;
        versioning
            .apply_mutation(
                namespace_id,
                &task_queue,
                token,
                VersioningMutation::AddRedirectRule {
                    rule: RedirectRule {
                        source_build_id: "old".to_string(),
                        target_build_id: "new".to_string(),
                        create_time: OffsetDateTime::UNIX_EPOCH,
                    },
                },
                OffsetDateTime::UNIX_EPOCH,
            )
            .unwrap();
        let publisher = RuntimeDispatchPublisher::new(
            workflow_broker.clone(),
            activity_broker,
            repo,
            Arc::new(Mutex::new(Vec::new())),
            1,
            1,
            Arc::new(NoopNexusHttpClient),
            NexusEndpointRegistry::default(),
            NexusTaskBroker::default(),
            NexusTimeoutTrackingState::default(),
            ActivityTrackingState::default(),
            DeliveryMetrics::new(),
            Some(versioning),
        );
        let pinned_queue = QueueKey {
            namespace_id,
            task_queue,
            task_kind: TaskKind::Workflow,
            deployment: Some(DeploymentId("series-a".to_string())),
            build_id: Some(BuildId("old".to_string())),
        };
        let redirected_queue = QueueKey {
            build_id: Some(BuildId("new".to_string())),
            ..pinned_queue.clone()
        };

        publisher
            .publish(
                RunKey::new(),
                &[DispatchOp::EnqueueWorkflowTask {
                    queue: pinned_queue.clone(),
                    logical_seq: LogicalTaskSeq(1),
                    sticky_preferred: None,
                }],
            )
            .await
            .unwrap();

        let wrong = workflow_broker
            .poll_workflow_task(
                &redirected_queue,
                &WorkerIdentity("worker-a".to_string()),
                std::time::Duration::from_millis(5),
            )
            .await
            .unwrap();
        let right = workflow_broker
            .poll_workflow_task(
                &pinned_queue,
                &WorkerIdentity("worker-a".to_string()),
                std::time::Duration::from_millis(5),
            )
            .await
            .unwrap();

        assert!(wrong.is_none());
        assert!(right.is_some());
    }

    #[tokio::test]
    async fn runtime_dispatch_publisher_skips_redirect_for_unassigned_queue() {
        let workflow_broker = InMemoryBroker::default();
        let activity_broker = InMemoryActivityBroker::default();
        let repo = Arc::new(MockTimerRepo::from_responses(Vec::new()));
        let versioning = Arc::new(VersioningRuleStore::default());
        let namespace_id = NamespaceId::new();
        let task_queue = TaskQueueName("workflow-q".to_string());
        let token = versioning
            .get_rules(namespace_id, &task_queue)
            .conflict_token;
        versioning
            .apply_mutation(
                namespace_id,
                &task_queue,
                token,
                VersioningMutation::AddRedirectRule {
                    rule: RedirectRule {
                        source_build_id: "old".to_string(),
                        target_build_id: "new".to_string(),
                        create_time: OffsetDateTime::UNIX_EPOCH,
                    },
                },
                OffsetDateTime::UNIX_EPOCH,
            )
            .unwrap();
        let publisher = RuntimeDispatchPublisher::new(
            workflow_broker.clone(),
            activity_broker,
            repo,
            Arc::new(Mutex::new(Vec::new())),
            1,
            1,
            Arc::new(NoopNexusHttpClient),
            NexusEndpointRegistry::default(),
            NexusTaskBroker::default(),
            NexusTimeoutTrackingState::default(),
            ActivityTrackingState::default(),
            DeliveryMetrics::new(),
            Some(versioning),
        );
        let queue = QueueKey {
            namespace_id,
            task_queue,
            task_kind: TaskKind::Workflow,
            deployment: None,
            build_id: None,
        };

        publisher
            .publish(
                RunKey::new(),
                &[DispatchOp::EnqueueWorkflowTask {
                    queue: queue.clone(),
                    logical_seq: LogicalTaskSeq(1),
                    sticky_preferred: None,
                }],
            )
            .await
            .unwrap();

        let delivered = workflow_broker
            .poll_workflow_task(
                &queue,
                &WorkerIdentity("worker-a".to_string()),
                std::time::Duration::from_millis(5),
            )
            .await
            .unwrap();

        assert!(delivered.is_some());
    }

    #[test]
    fn timer_scanner_config_default_values() {
        let config = TimerScannerConfig::default();
        assert_eq!(
            config.scan_interval,
            tokio::time::Duration::from_millis(200)
        );
        assert_eq!(config.max_timers_per_scan, 100);
    }

    #[test]
    fn evaluate_workflow_timeout_cases() {
        let started_at = OffsetDateTime::now_utc() - Duration::seconds(10);

        let both = WorkflowTimeoutEntry {
            run_key: RunKey::new(),
            shard_id: ShardId(0),
            workflow_execution_timeout: Some(Duration::seconds(1)),
            workflow_run_timeout: Some(Duration::seconds(2)),
            started_at,
            first_run_started_at: None,
            has_retry_policy: true,
        };
        assert_eq!(
            evaluate_workflow_timeout(&both, OffsetDateTime::now_utc()),
            Some(WorkflowTimeoutViolation::ExecutionTimeout)
        );

        let zero = WorkflowTimeoutEntry {
            run_key: RunKey::new(),
            shard_id: ShardId(0),
            workflow_execution_timeout: Some(Duration::ZERO),
            workflow_run_timeout: None,
            started_at: OffsetDateTime::now_utc(),
            first_run_started_at: None,
            has_retry_policy: false,
        };
        assert_eq!(
            evaluate_workflow_timeout(&zero, zero.started_at),
            Some(WorkflowTimeoutViolation::ExecutionTimeout)
        );

        let none = WorkflowTimeoutEntry {
            run_key: RunKey::new(),
            shard_id: ShardId(0),
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            started_at,
            first_run_started_at: None,
            has_retry_policy: false,
        };
        assert_eq!(
            evaluate_workflow_timeout(&none, OffsetDateTime::now_utc()),
            None
        );

        let run_only = WorkflowTimeoutEntry {
            run_key: RunKey::new(),
            shard_id: ShardId(0),
            workflow_execution_timeout: None,
            workflow_run_timeout: Some(Duration::seconds(1)),
            started_at,
            first_run_started_at: None,
            has_retry_policy: false,
        };
        assert_eq!(
            evaluate_workflow_timeout(&run_only, OffsetDateTime::now_utc()),
            Some(WorkflowTimeoutViolation::RunTimeout)
        );

        let exec_only = WorkflowTimeoutEntry {
            run_key: RunKey::new(),
            shard_id: ShardId(0),
            workflow_execution_timeout: Some(Duration::seconds(1)),
            workflow_run_timeout: None,
            started_at,
            first_run_started_at: None,
            has_retry_policy: false,
        };
        assert_eq!(
            evaluate_workflow_timeout(&exec_only, OffsetDateTime::now_utc()),
            Some(WorkflowTimeoutViolation::ExecutionTimeout)
        );

        let not_elapsed = WorkflowTimeoutEntry {
            run_key: RunKey::new(),
            shard_id: ShardId(0),
            workflow_execution_timeout: Some(Duration::seconds(30)),
            workflow_run_timeout: Some(Duration::seconds(20)),
            started_at,
            first_run_started_at: None,
            has_retry_policy: false,
        };
        assert_eq!(
            evaluate_workflow_timeout(&not_elapsed, started_at + Duration::seconds(5)),
            None
        );
    }

    #[test]
    fn workflow_timeout_scanner_config_default_values() {
        let config = WorkflowTimeoutScannerConfig::default();
        assert_eq!(config.scan_interval, tokio::time::Duration::from_secs(1));
        assert_eq!(config.max_timeouts_per_scan, 100);
    }

    #[test]
    fn nexus_timeout_scanner_config_default_values() {
        let config = NexusTimeoutScannerConfig::default();
        assert_eq!(config.scan_interval, tokio::time::Duration::from_secs(1));
        assert_eq!(config.max_timeouts_per_scan, 100);
    }

    #[test]
    fn endpoint_registry_lookup_returns_registered_address() {
        let registry = NexusEndpointRegistry::new(HashMap::from([(
            "payments".to_string(),
            NexusEndpointConfig {
                target: EndpointTarget::External {
                    address: "http://payments".to_string(),
                },
            },
        )]));
        assert_eq!(
            registry
                .resolve("payments")
                .and_then(|config| match &config.target {
                    EndpointTarget::External { address } => Some(address.as_str()),
                    EndpointTarget::Worker { .. } => None,
                }),
            Some("http://payments")
        );
        assert!(registry.resolve("missing").is_none());
    }

    #[test]
    fn evaluate_nexus_timeout_cases() {
        let scheduled_at = OffsetDateTime::now_utc() - Duration::seconds(10);
        let expired = NexusTimeoutEntry {
            run_key: RunKey::new(),
            shard_id: ShardId(0),
            operation_id: "op-1".to_string(),
            scheduled_event_id: 11,
            schedule_to_close_timeout: Duration::seconds(1),
            scheduled_at,
        };
        assert!(evaluate_nexus_timeout(&expired, OffsetDateTime::now_utc()));

        let zero = NexusTimeoutEntry {
            run_key: RunKey::new(),
            shard_id: ShardId(0),
            operation_id: "op-2".to_string(),
            scheduled_event_id: 12,
            schedule_to_close_timeout: Duration::ZERO,
            scheduled_at: OffsetDateTime::now_utc(),
        };
        assert!(evaluate_nexus_timeout(&zero, zero.scheduled_at));

        let pending = NexusTimeoutEntry {
            run_key: RunKey::new(),
            shard_id: ShardId(0),
            operation_id: "op-3".to_string(),
            scheduled_event_id: 13,
            schedule_to_close_timeout: Duration::seconds(30),
            scheduled_at,
        };
        assert!(!evaluate_nexus_timeout(
            &pending,
            scheduled_at + Duration::seconds(5)
        ));
    }

    #[test]
    fn workflow_timeout_tracking_state_crud() {
        let tracking = WorkflowTimeoutTrackingState::default();
        let entry = WorkflowTimeoutEntry {
            run_key: RunKey::new(),
            shard_id: ShardId(0),
            workflow_execution_timeout: Some(Duration::seconds(1)),
            workflow_run_timeout: None,
            started_at: OffsetDateTime::now_utc(),
            first_run_started_at: None,
            has_retry_policy: true,
        };
        tracking.insert(entry.clone());
        assert_eq!(tracking.snapshot(), vec![entry.clone()]);
        tracking.remove(entry.run_key);
        assert!(tracking.snapshot().is_empty());
    }

    proptest! {
        #[test]
        fn property_workflow_timeout_evaluation_correctness(
            exec_secs in proptest::option::of(0i64..20),
            run_secs in proptest::option::of(0i64..20),
            elapsed_secs in 0i64..40,
            chain_extra_secs in 0i64..40,
            use_chain_origin in any::<bool>(),
        ) {
            let now = OffsetDateTime::now_utc();
            let started_at = now - Duration::seconds(elapsed_secs);
            let first_run_started_at = use_chain_origin.then(|| {
                started_at - Duration::seconds(chain_extra_secs)
            });
            let entry = WorkflowTimeoutEntry {
                run_key: RunKey::new(),
            shard_id: ShardId(0),
                workflow_execution_timeout: exec_secs.map(Duration::seconds),
                workflow_run_timeout: run_secs.map(Duration::seconds),
                started_at,
                first_run_started_at,
                has_retry_policy: false,
            };

            let result = evaluate_workflow_timeout(&entry, now);
            let execution_origin =
                entry.first_run_started_at.unwrap_or(entry.started_at);
            if let Some(exec) = entry.workflow_execution_timeout {
                if now - execution_origin > exec
                    || (exec.is_zero() && now >= execution_origin)
                {
                    prop_assert_eq!(result, Some(WorkflowTimeoutViolation::ExecutionTimeout));
                    return Ok(());
                }
            }
            if let Some(run) = entry.workflow_run_timeout {
                if now - started_at > run || (run.is_zero() && now >= started_at) {
                    prop_assert_eq!(result, Some(WorkflowTimeoutViolation::RunTimeout));
                    return Ok(());
                }
            }
            prop_assert_eq!(result, None);
        }
    }

    proptest! {
        #[test]
        fn property_workflow_timeout_retry_state_derivation(has_retry_policy in any::<bool>()) {
            let entry = WorkflowTimeoutEntry {
                run_key: RunKey::new(),
            shard_id: ShardId(0),
                workflow_execution_timeout: Some(Duration::seconds(1)),
                workflow_run_timeout: None,
                started_at: OffsetDateTime::now_utc() - Duration::seconds(10),
                first_run_started_at: None,
                has_retry_policy,
            };
            let expected = if has_retry_policy {
                RetryState::Timeout
            } else {
                RetryState::RetryPolicyNotSet
            };
            prop_assert_eq!(workflow_timeout_retry_state(&entry), expected);
        }
    }

    proptest! {
        #[test]
        fn property_pick_lane_matches_runtime_lane_index(run in any::<u128>(), lane_count in 1usize..16usize) {
            let rt = Runtime::new().unwrap();
            let picked = rt.block_on(async move {
                let repo = Arc::new(InMemoryStore::default());
                let runtime = TokeiraRuntime::new(
                    repo,
                    lane_count,
                    LaneConfig::default(),
                    TimerScannerConfig::default(),
                    WorkflowTimeoutScannerConfig::default(),
                    BacklogConfig::default(),
                );
                let run_key = RunKey(Uuid::from_u128(run));
                let lane_ptr = runtime.pick_lane(run_key) as *const LaneHandle as usize;
                let expected_ptr = &runtime.lanes[runtime.lane_index(run_key)] as *const LaneHandle as usize;
                (lane_ptr, expected_ptr)
            });
            prop_assert_eq!(picked.0, picked.1);
        }
    }

    proptest! {
        #[test]
        fn property_due_timers_produce_timer_due_submissions(
            runs in proptest::collection::vec(any::<u128>(), 0..20),
            timer_ids in proptest::collection::vec("[a-z0-9]{1,8}", 0..20),
            lane_count in 1usize..16usize,
        ) {
            let rt = Runtime::new().unwrap();
            rt.block_on(async move {
                let due_timers: Vec<_> = runs
                    .into_iter()
                    .zip(timer_ids.into_iter())
                    .map(|(run, timer_id)| DueTimer {
                        run_key: RunKey(Uuid::from_u128(run)),
                        timer_id,
                    })
                    .collect();
                let repo = MockTimerRepo::from_responses(vec![TimerListResponse::Ok(due_timers.clone())]);
                let captured = Arc::new(Mutex::new(Vec::new()));
                let captured_clone = captured.clone();
                let config = TimerScannerConfig {
                    scan_interval: tokio::time::Duration::from_millis(1),
                    max_timers_per_scan: 500,
                };

                scan_due_timers_once(&repo, &config, move |due, fired_at| {
                    let captured = captured_clone.clone();
                    async move {
                        captured.lock().unwrap().push((
                            due.run_key,
                            due.timer_id,
                            lane_index_for_run_key(due.run_key, lane_count),
                            fired_at,
                        ));
                        Ok(())
                    }
                }).await;

                let captured = captured.lock().unwrap();
                prop_assert_eq!(captured.len(), due_timers.len());
                for (index, due) in due_timers.iter().enumerate() {
                    let (run_key, timer_id, lane_index, _) = &captured[index];
                    prop_assert_eq!(*run_key, due.run_key);
                    prop_assert_eq!(timer_id, &due.timer_id);
                    prop_assert_eq!(
                        *lane_index,
                        lane_index_for_run_key(due.run_key, lane_count)
                    );
                }
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }

    proptest! {
        #[test]
        fn property_batch_limit_is_respected(limit in 1usize..200usize) {
            let rt = Runtime::new().unwrap();
            rt.block_on(async move {
                let repo = MockTimerRepo::from_responses(vec![TimerListResponse::Ok(Vec::new())]);
                let config = TimerScannerConfig {
                    scan_interval: tokio::time::Duration::from_millis(1),
                    max_timers_per_scan: limit,
                };
                scan_due_timers_once(&repo, &config, |_due, _fired_at| async move { Ok(()) }).await;
                let recorded = repo.recorded_limits();
                prop_assert_eq!(recorded, vec![limit]);
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }

    proptest! {
        #[test]
        fn property_consistent_fired_at_within_scan(
            runs in proptest::collection::vec(any::<u128>(), 2..10usize),
        ) {
            let rt = Runtime::new().unwrap();
            rt.block_on(async move {
                let due_timers: Vec<_> = runs
                    .into_iter()
                    .enumerate()
                    .map(|(index, run)| DueTimer {
                        run_key: RunKey(Uuid::from_u128(run)),
                        timer_id: format!("timer-{index}"),
                    })
                    .collect();
                let repo = MockTimerRepo::from_responses(vec![TimerListResponse::Ok(due_timers)]);
                let fired_ats = Arc::new(Mutex::new(Vec::new()));
                let fired_ats_clone = fired_ats.clone();
                scan_due_timers_once(&repo, &TimerScannerConfig::default(), move |_due, fired_at| {
                    let fired_ats = fired_ats_clone.clone();
                    async move {
                        fired_ats.lock().unwrap().push(fired_at);
                        Ok(())
                    }
                }).await;

                let fired_ats = fired_ats.lock().unwrap();
                prop_assert!(!fired_ats.is_empty());
                let first = fired_ats[0];
                prop_assert!(fired_ats.iter().all(|value| *value == first));
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }

    proptest! {
        #[test]
        fn property_per_entry_failure_resilience(
            runs in proptest::collection::vec(any::<u128>(), 1..20usize),
            fail_pattern in proptest::collection::vec(any::<bool>(), 1..20usize),
        ) {
            let rt = Runtime::new().unwrap();
            rt.block_on(async move {
                let due_timers: Vec<_> = runs
                    .into_iter()
                    .enumerate()
                    .map(|(index, run)| DueTimer {
                        run_key: RunKey(Uuid::from_u128(run)),
                        timer_id: format!("timer-{index}"),
                    })
                    .collect();
                let repo = MockTimerRepo::from_responses(vec![TimerListResponse::Ok(due_timers.clone())]);
                let successes = Arc::new(Mutex::new(Vec::new()));
                let successes_clone = successes.clone();
                let fail_pattern_for_submit = fail_pattern.clone();
                scan_due_timers_once(&repo, &TimerScannerConfig::default(), move |due, _fired_at| {
                    let successes = successes_clone.clone();
                    let should_fail = fail_pattern_for_submit
                        .get(due.timer_id.trim_start_matches("timer-").parse::<usize>().unwrap_or(0) % fail_pattern_for_submit.len())
                        .copied()
                        .unwrap_or(false);
                    async move {
                        if should_fail {
                            Err(anyhow!("lane channel closed"))
                        } else {
                            successes.lock().unwrap().push(due.timer_id);
                            Ok(())
                        }
                    }
                }).await;

                let expected_successes = due_timers
                    .iter()
                    .filter(|due| {
                        !fail_pattern
                            .get(due.timer_id.trim_start_matches("timer-").parse::<usize>().unwrap_or(0) % fail_pattern.len())
                            .copied()
                            .unwrap_or(false)
                    })
                    .count();
                prop_assert_eq!(successes.lock().unwrap().len(), expected_successes);
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }

    proptest! {
        #[test]
        fn property_storage_error_resilience(
            runs in proptest::collection::vec(any::<u128>(), 1..10usize),
        ) {
            let rt = Runtime::new().unwrap();
            rt.block_on(async move {
                let due_timers: Vec<_> = runs
                    .into_iter()
                    .enumerate()
                    .map(|(index, run)| DueTimer {
                        run_key: RunKey(Uuid::from_u128(run)),
                        timer_id: format!("timer-{index}"),
                    })
                    .collect();
                let repo = MockTimerRepo::from_responses(vec![
                    TimerListResponse::Err("transient storage failure".to_string()),
                    TimerListResponse::Ok(due_timers.clone()),
                ]);
                let captured = Arc::new(Mutex::new(Vec::new()));
                let captured_clone = captured.clone();
                let config = TimerScannerConfig::default();

                scan_due_timers_once(&repo, &config, |_due, _fired_at| async move { Ok(()) }).await;
                scan_due_timers_once(&repo, &config, move |due, _fired_at| {
                    let captured = captured_clone.clone();
                    async move {
                        captured.lock().unwrap().push(due.timer_id);
                        Ok(())
                    }
                }).await;

                prop_assert_eq!(captured.lock().unwrap().len(), due_timers.len());
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }

    #[tokio::test]
    async fn timer_scanner_handle_is_present_after_runtime_new() {
        let repo = Arc::new(InMemoryStore::default());
        let mut runtime = TokeiraRuntime::new(
            repo,
            2,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
            BacklogConfig::default(),
        );
        assert!(runtime.timer_scanner_handle.is_some());
        runtime.shutdown_timer_scanner().await.unwrap();
    }

    #[tokio::test]
    async fn timer_scanner_shutdown_completes_promptly() {
        let repo = Arc::new(InMemoryStore::default());
        let mut runtime = TokeiraRuntime::new(
            repo,
            2,
            LaneConfig::default(),
            TimerScannerConfig {
                scan_interval: tokio::time::Duration::from_secs(60),
                max_timers_per_scan: 100,
            },
            WorkflowTimeoutScannerConfig::default(),
            BacklogConfig::default(),
        );
        runtime.shutdown_timer_scanner().await.unwrap();
        assert!(runtime.timer_scanner_handle.is_none());
    }

    #[tokio::test]
    async fn workflow_timeout_scanner_handle_is_present_after_runtime_new() {
        let repo = Arc::new(InMemoryStore::default());
        let mut runtime = TokeiraRuntime::new(
            repo,
            2,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
            BacklogConfig::default(),
        );
        assert!(runtime.workflow_timeout_scanner_handle.is_some());
        runtime.shutdown_workflow_timeout_scanner().await.unwrap();
    }

    #[tokio::test]
    async fn workflow_timeout_scanner_shutdown_completes_promptly() {
        let repo = Arc::new(InMemoryStore::default());
        let mut runtime = TokeiraRuntime::new(
            repo,
            2,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig {
                scan_interval: tokio::time::Duration::from_secs(60),
                max_timeouts_per_scan: 100,
            },
            BacklogConfig::default(),
        );
        runtime.shutdown_workflow_timeout_scanner().await.unwrap();
        assert!(runtime.workflow_timeout_scanner_handle.is_none());
    }

    #[tokio::test]
    async fn start_workflow_with_timeout_populates_tracking_state() {
        let repo = Arc::new(InMemoryStore::default());
        let mut runtime = TokeiraRuntime::new(
            repo,
            2,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
            BacklogConfig::default(),
        );
        let request = sample_start_request(Some(Duration::seconds(5)), Some(Duration::seconds(3)));
        let result = runtime.start_workflow(request.clone()).await.unwrap();
        assert!(matches!(result, CommitResult::Applied { .. }));

        let snapshot = runtime.workflow_timeout_tracking().snapshot();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].run_key, request.run_key);
        assert_eq!(
            snapshot[0].workflow_execution_timeout,
            request.workflow_execution_timeout
        );
        assert_eq!(
            snapshot[0].workflow_run_timeout,
            request.workflow_run_timeout
        );
        assert_eq!(snapshot[0].started_at, request.now);
        assert!(snapshot[0].has_retry_policy == request.retry_policy.is_some());

        runtime.shutdown_timer_scanner().await.unwrap();
        runtime.shutdown_workflow_timeout_scanner().await.unwrap();
    }

    proptest! {
        #[test]
        fn property_start_with_timeout_populates_tracking_state(
            execution_timeout_secs in proptest::option::of(1i64..20),
            run_timeout_secs in proptest::option::of(1i64..20),
            has_retry_policy in any::<bool>(),
        ) {
            let rt = Runtime::new().unwrap();
            rt.block_on(async move {
                let repo = Arc::new(InMemoryStore::default());
                let mut runtime = TokeiraRuntime::new(
                    repo,
                    2,
                    LaneConfig::default(),
                    TimerScannerConfig::default(),
                    WorkflowTimeoutScannerConfig::default(),
                    BacklogConfig::default(),
                );
                let mut request = sample_start_request(
                    execution_timeout_secs.map(Duration::seconds),
                    run_timeout_secs.map(Duration::seconds),
                );
                if has_retry_policy {
                    request.retry_policy = Some(RetryPolicy {
                        initial_interval: Duration::seconds(1),
                        backoff_coefficient: 2.0,
                        maximum_interval: Some(Duration::seconds(10)),
                        maximum_attempts: 3,
                        non_retryable_error_types: Vec::new(),
                    });
                }
                runtime.start_workflow(request.clone()).await.unwrap();
                let snapshot = runtime.workflow_timeout_tracking().snapshot();
                if request.workflow_execution_timeout.is_some() || request.workflow_run_timeout.is_some() {
                    prop_assert_eq!(snapshot.len(), 1);
                    let entry = &snapshot[0];
                    prop_assert_eq!(entry.run_key, request.run_key);
                    prop_assert_eq!(entry.workflow_execution_timeout, request.workflow_execution_timeout);
                    prop_assert_eq!(entry.workflow_run_timeout, request.workflow_run_timeout);
                    prop_assert_eq!(entry.started_at, request.now);
                    prop_assert_eq!(entry.has_retry_policy, has_retry_policy);
                } else {
                    prop_assert!(snapshot.is_empty());
                }
                runtime.shutdown_timer_scanner().await.unwrap();
                runtime.shutdown_workflow_timeout_scanner().await.unwrap();
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }

    proptest! {
        #[test]
        fn property_scan_workflow_timeouts_consistent_now_and_batch_bound(
            count in 2usize..20usize,
            max_batch in 1usize..10usize,
        ) {
            let rt = Runtime::new().unwrap();
            rt.block_on(async move {
                let tracking = WorkflowTimeoutTrackingState::default();
                for index in 0..count {
                    tracking.insert(WorkflowTimeoutEntry {
                        run_key: RunKey::new(),
            shard_id: ShardId(0),
                        workflow_execution_timeout: Some(Duration::seconds(1)),
                        workflow_run_timeout: None,
                        started_at: OffsetDateTime::now_utc() - Duration::seconds(10 + index as i64),
                        first_run_started_at: None,
                        has_retry_policy: index % 2 == 0,
                    });
                }
                let seen = Arc::new(Mutex::new(Vec::new()));
                let seen_clone = seen.clone();
                scan_workflow_timeouts_once(
                    &tracking,
                    None,
                    &WorkflowTimeoutScannerConfig {
                        scan_interval: tokio::time::Duration::from_secs(1),
                        max_timeouts_per_scan: max_batch,
                    },
                    move |entry, violation, now| {
                        let seen = seen_clone.clone();
                        async move {
                            seen.lock().unwrap().push((entry.run_key, violation, now));
                            Ok(())
                        }
                    }
                ).await;
                let seen = seen.lock().unwrap();
                prop_assert!(seen.len() <= max_batch);
                if !seen.is_empty() {
                    let now = seen[0].2;
                    prop_assert!(seen.iter().all(|(_, _, candidate)| *candidate == now));
                }
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }

    proptest! {
        #[test]
        fn property_scan_workflow_timeouts_handles_kernel_rejections_and_lane_errors(
            count in 1usize..20usize,
            rejection_mod in 1usize..5usize,
            lane_error_mod in 1usize..5usize,
        ) {
            let rt = Runtime::new().unwrap();
            rt.block_on(async move {
                let tracking = WorkflowTimeoutTrackingState::default();
                let mut entries = Vec::new();
                for _ in 0..count {
                    let entry = WorkflowTimeoutEntry {
                        run_key: RunKey::new(),
            shard_id: ShardId(0),
                        workflow_execution_timeout: Some(Duration::seconds(1)),
                        workflow_run_timeout: None,
                        started_at: OffsetDateTime::now_utc() - Duration::seconds(10),
                        first_run_started_at: None,
                        has_retry_policy: false,
                    };
                    tracking.insert(entry.clone());
                    entries.push(entry);
                }

                let entries_for_submit = entries.clone();
                scan_workflow_timeouts_once(
                    &tracking,
                    None,
                    &WorkflowTimeoutScannerConfig::default(),
                    move |entry, _violation, _now| {
                        let entries_for_submit = entries_for_submit.clone();
                        async move {
                            let idx = entries_for_submit.iter().position(|candidate| candidate.run_key == entry.run_key).unwrap_or(0);
                            if idx % rejection_mod == 0 {
                                Err(anyhow!("kernel rejected command: closed"))
                            } else if idx % lane_error_mod == 0 {
                                Err(anyhow!("lane channel closed"))
                            } else {
                                Ok(())
                            }
                        }
                    }
                ).await;

                let remaining = tracking.snapshot();
                for (idx, entry) in entries.iter().enumerate() {
                    let should_remove = idx % rejection_mod == 0 || idx % lane_error_mod != 0;
                    let present = remaining.iter().any(|candidate| candidate.run_key == entry.run_key);
                    if should_remove {
                        prop_assert!(!present);
                    } else {
                        prop_assert!(present);
                    }
                }
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }

    fn sample_start_request(
        workflow_execution_timeout: Option<Duration>,
        workflow_run_timeout: Option<Duration>,
    ) -> StartRequest {
        let run_id = tokeira_types::RunId::new();
        StartRequest {
            run_key: RunKey::new(),
            namespace_id: NamespaceId::new(),
            workflow_id: WorkflowId("workflow-timeout".to_string()),
            run_id,
            workflow_type: tokeira_types::WorkflowType("example".to_string()),
            task_queue: TaskQueueName("workflow-q".to_string()),
            deployment: None,
            build_id: None,
            input: Payloads::default(),
            header: None,
            memo: Memo::default(),
            search_attributes: SearchAttributes::default(),
            workflow_execution_timeout,
            workflow_run_timeout,
            workflow_task_timeout: Duration::seconds(10),
            retry_policy: None,
            conflict_policy: tokeira_kernel::WorkflowIdConflictPolicy::Fail,
            reuse_policy: tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
            attempt: 1,
            continued_execution_run_id: None,
            first_execution_run_id: None,
            parent_run_key: None,
            parent_workflow_id: None,
            parent_run_id: None,
            parent_namespace_id: None,
            parent_initiated_event_id: 0,
            original_execution_run_id: Some(run_id),
            continued_failure: None,
            last_completion_result: None,
            first_run_started_at: None,
            request: RequestContext {
                request_id: RequestId("req-timeout".to_string()),
                caller_identity: None,
                received_at: OffsetDateTime::now_utc(),
            },
            now: OffsetDateTime::now_utc(),
            cron_schedule: None,
            reserved_poller_identity: None,
        }
    }

    enum TimerListResponse {
        Ok(Vec<DueTimer>),
        Err(String),
    }

    struct MockTimerRepo {
        responses: Mutex<VecDeque<TimerListResponse>>,
        limits: Mutex<Vec<usize>>,
    }

    impl MockTimerRepo {
        fn from_responses(responses: Vec<TimerListResponse>) -> Self {
            Self {
                responses: Mutex::new(VecDeque::from(responses)),
                limits: Mutex::new(Vec::new()),
            }
        }

        fn recorded_limits(&self) -> Vec<usize> {
            self.limits.lock().unwrap().clone()
        }
    }

    use async_trait::async_trait;
    use tokeira_kernel::Transition;
    use tokeira_storage::DueTimer;

    #[async_trait]
    impl RunRepository for MockTimerRepo {
        async fn resolve_execution(&self, _execution: &ExecutionRef) -> Result<Option<RunKey>> {
            panic!("unused in timer scanner tests")
        }

        async fn find_latest_run(
            &self,
            _namespace_id: tokeira_types::NamespaceId,
            _workflow_id: &tokeira_types::WorkflowId,
        ) -> Result<Option<RunKey>> {
            panic!("unused in timer scanner tests")
        }

        async fn load_run(&self, _run_key: RunKey) -> Result<LoadedRun> {
            panic!("unused in timer scanner tests")
        }

        async fn read_history(
            &self,
            _run_key: RunKey,
            _after_event_id: i64,
            _limit: usize,
        ) -> Result<Vec<tokeira_kernel::HistoryEvent>> {
            panic!("unused in timer scanner tests")
        }

        async fn lookup_request_dedupe(
            &self,
            _execution: &ExecutionRef,
            _request_id: &tokeira_types::RequestId,
        ) -> Result<Option<RequestRecord>> {
            panic!("unused in timer scanner tests")
        }

        async fn read_transition_audit(
            &self,
            _run_key: RunKey,
        ) -> Result<Vec<TransitionAuditRecord>> {
            panic!("unused in timer scanner tests")
        }

        // Test mock: unused in timer scanner tests. Production activity/retry
        // paths now route through commit_transition_for_bundle with the real
        // epoch from ShardOwner (see tasks 4.1/4.2).
        async fn commit_transition(
            &self,
            _run_key: RunKey,
            _transition: Transition,
            _epoch: ShardEpoch,
        ) -> Result<CommitResult> {
            panic!("unused in timer scanner tests")
        }

        async fn commit_transition_for_bundle(
            &self,
            _run_key: RunKey,
            _execution_home_bundle: ShardId,
            _transition: Transition,
            _epoch: ShardEpoch,
        ) -> Result<CommitResult> {
            panic!("unused in timer scanner tests")
        }

        async fn materialize_reset_successor(
            &self,
            _base_run_key: RunKey,
            _fork_event_id: i64,
            _successor_run_id: RunId,
        ) -> Result<()> {
            panic!("unused in timer scanner tests")
        }

        async fn list_dispatchable_workflow_tasks(
            &self,
            _queue: &QueueKey,
            _limit: usize,
        ) -> Result<Vec<DispatchableWorkflowTask>> {
            panic!("unused in timer scanner tests")
        }

        async fn list_dispatchable_activity_tasks(
            &self,
            _queue: &QueueKey,
            _limit: usize,
        ) -> Result<Vec<DispatchableActivityTask>> {
            panic!("unused in timer scanner tests")
        }

        async fn persist_to_backlog(&self, _entries: Vec<BacklogEntry>) -> Result<()> {
            panic!("unused in timer scanner tests")
        }

        async fn drain_backlog(
            &self,
            _queue: &QueueKey,
            _limit: usize,
        ) -> Result<Vec<BacklogEntry>> {
            panic!("unused in timer scanner tests")
        }

        async fn list_due_timers(
            &self,
            _now: OffsetDateTime,
            limit: usize,
        ) -> Result<Vec<DueTimer>> {
            self.limits.lock().unwrap().push(limit);
            match self.responses.lock().unwrap().pop_front() {
                Some(TimerListResponse::Ok(due_timers)) => Ok(due_timers),
                Some(TimerListResponse::Err(message)) => Err(anyhow!(message)),
                None => Ok(Vec::new()),
            }
        }

        async fn list_dispatchable_workflow_tasks_for_shard(
            &self,
            _shard_id: tokeira_types::ShardId,
            _limit: usize,
        ) -> Result<Vec<DispatchableWorkflowTask>> {
            panic!("unused in timer scanner tests")
        }

        async fn list_dispatchable_activity_tasks_for_shard(
            &self,
            _shard_id: tokeira_types::ShardId,
            _limit: usize,
        ) -> Result<Vec<DispatchableActivityTask>> {
            panic!("unused in timer scanner tests")
        }

        async fn list_due_timers_for_shard(
            &self,
            _shard_id: tokeira_types::ShardId,
            _now: OffsetDateTime,
            limit: usize,
        ) -> Result<Vec<DueTimer>> {
            self.list_due_timers(_now, limit).await
        }

        async fn list_runs_with_workflow_timeouts_for_shard(
            &self,
            _shard_id: tokeira_types::ShardId,
            _limit: usize,
        ) -> Result<Vec<tokeira_storage::WorkflowTimeoutSweepEntry>> {
            panic!("unused in timer scanner tests")
        }

        async fn list_started_workflow_tasks_for_shard(
            &self,
            _shard_id: tokeira_types::ShardId,
            _limit: usize,
        ) -> Result<Vec<tokeira_storage::WftTimeoutSweepEntry>> {
            panic!("unused in timer scanner tests")
        }

        async fn list_open_activities_for_shard(
            &self,
            _shard_id: tokeira_types::ShardId,
            _limit: usize,
        ) -> Result<Vec<tokeira_storage::ActivitySweepEntry>> {
            panic!("unused in timer scanner tests")
        }

        async fn list_pending_nexus_operations_for_shard(
            &self,
            _shard_id: tokeira_types::ShardId,
            _limit: usize,
        ) -> Result<Vec<tokeira_storage::NexusSweepEntry>> {
            panic!("unused in timer scanner tests")
        }
    }
}
