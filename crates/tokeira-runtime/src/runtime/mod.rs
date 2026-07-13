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
    ActivityOp, ActivityResolution, ActivityResolvedRequest, BasicKernel, Command,
    CronContinuation, DispatchOp, FieldChange, HistoryEvent, HistoryEventKind, LoadedRun,
    PauseWorkflowRequest, RetryState, SignalRequest, SignalWithStartRequest, StartRequest,
    StartWorkflowTaskRequest, TerminateRequest, Transition, UnpauseWorkflowRequest,
    UpdateExecutionOptionsRequest, UpdateRequest, WorkflowCommand, WorkflowIdConflictPolicy,
    WorkflowIdReusePolicy, WorkflowState, WorkflowTaskCompletedRequest,
};
use tokeira_storage::{
    CommitResult, DeleteRunRequest, DeleteRunResult, DispatchableActivityTask,
    DispatchableWorkflowTask, LeaseOutcome, LeaseRepository, ProjectionRecord, RunRepository,
    WorkerDeploymentRepository,
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
    deployment_registry::DeploymentRegistry,
    drain::RuntimeDrain,
    errors::{
        ActivityTaskNotFound, ActivityTokenResolutionError, NotShardOwner, WorkflowDeletionNotFound,
    },
    fairness::{DeliveryMetrics, FairnessState, run_control_loop},
    heartbeat::{InMemoryHeartbeatStore, spawn_heartbeat_maintenance},
    lane::{LaneConfig, LaneHandle, spawn_lane_with_id},
    membership::{ConnectionBudgetApplier, HeartbeatInputs, MembershipClient, MembershipConfig},
    metrics as runtime_metrics,
    nexus::{
        CompletionCallbackTrackingState, NexusCompletionDeps, NexusEndpointRegistry,
        NexusHttpClient, NexusNamespaceResolver, NexusTaskBroker, NexusTimeoutScannerConfig,
        NexusTimeoutTrackingState, NoopNexusHttpClient, run_nexus_timeout_scanner,
    },
    publisher::{RuntimeDispatchPublisher, run_completion_callback_scanner},
    query::{QueryResult, QueryTask},
    recovery::{lease_rejected_error, run_lease_renewer, sweep_shard},
    retry::{RetryDecision, RetryExhaustedReason, evaluate_activity_retry},
    scanner::{
        TimerScannerConfig, lane_index_for_run_key, pick_lane_for_run_key, run_timer_scanner,
    },
    schedule::cron_initial_backoff,
    shard::{ShardOwner, shard_for},
    timeout::{
        WorkflowTimeoutEntry, WorkflowTimeoutScannerConfig, WorkflowTimeoutTrackingState,
        run_workflow_timeout_scanner,
    },
    update::{
        PendingUpdateTransport, UpdateLifecycleSnapshot, UpdateLifecycleStage, UpdateOutcome,
        UpdateRegistry, UpdateResolution, UpdateTransportResolution, UpdateWaitPolicy,
    },
    wft_timeout::{
        WftTimeoutEntry, WftTimeoutKind, WftTimeoutScannerConfig, WftTimeoutTrackingState,
        run_wft_timeout_scanner,
    },
    worker_registry::{WorkerRegistrationKey, WorkerRegistry, WorkerVersionMetadata},
};

mod activity;
mod commit;
mod lifecycle;
mod membership;
mod query;
pub(crate) mod workflow_task;

pub(crate) use activity::{
    ActivityRetryDeps, ActivityRetryTarget, commit_activity_retry, exhausted_reason_to_retry_state,
};

/// v1.31.0 default number of consecutive workflow-task problems required
/// before `TemporalReportedProblems` is surfaced.
///
/// This remains a release-pinned constant rather than a deployment knob
/// (`common/dynamicconfig/constants.go:307-312 @ v1.31.0`).
pub const REPORTED_PROBLEMS_THRESHOLD: u32 = 5;

/// v1.31.0 defaults worker-shutdown poll cancellation off
/// (`common/dynamicconfig/constants.go:3207-3212 @ v1.31.0`).
const CANCEL_WORKER_POLLS_ON_SHUTDOWN: bool = false;

#[cfg(not(feature = "conformance"))]
#[inline]
fn cancel_worker_polls_on_shutdown() -> bool {
    CANCEL_WORKER_POLLS_ON_SHUTDOWN
}

#[cfg(feature = "conformance")]
#[inline]
fn cancel_worker_polls_on_shutdown() -> bool {
    tokeira_conformance::overrides()
        .get_bool("frontend.enableCancelWorkerPollsOnShutdown")
        .unwrap_or(CANCEL_WORKER_POLLS_ON_SHUTDOWN)
}

/// The live reported-problems threshold consulted when deriving
/// `TemporalReportedProblems`. In a production build this is exactly the pinned
/// [`REPORTED_PROBLEMS_THRESHOLD`].
#[cfg(not(feature = "conformance"))]
#[inline]
fn reported_problems_threshold() -> u32 {
    REPORTED_PROBLEMS_THRESHOLD
}

/// Conformance builds read the threshold *live* from the override registry at
/// the consult site — never cached on any state — so a mid-run change (Tier
/// 3.22 `DynamicConfigChanges` sets it 0 then 2) takes effect on the next
/// Describe. The key is honoured only when the control service set an override;
/// otherwise the pinned default stands
/// (spec `.kiro/specs/conformance-config-override/`).
#[cfg(feature = "conformance")]
#[inline]
fn reported_problems_threshold() -> u32 {
    let raw = tokeira_conformance::overrides()
        .get_i64("system.numConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute");
    raw.and_then(|value| u32::try_from(value).ok())
        .unwrap_or(REPORTED_PROBLEMS_THRESHOLD)
}

/// Reported-problem observation used to derive `TemporalReportedProblems` on
/// Describe, materialized from committed kernel state.
#[derive(Clone, Debug, PartialEq)]
pub struct WorkflowTaskReportedProblem {
    /// Consecutive problems observed since the last successful WFT completion.
    pub attempts_since_last_success: u32,
    /// Identity of the last non-transient WFT problem (v1.31.0's
    /// `LastWorkflowTaskFailure` oneof).
    pub problem: tokeira_kernel::WorkflowTaskProblem,
}

/// Derive the reported problem for a run from its committed state.
///
/// The kernel advances `workflow_task_attempts_since_last_success` and
/// `last_workflow_task_problem` under exactly v1.31.0's `failWorkflowTask`
/// rules (non-sticky failures and non-sticky start-to-close timeouts count;
/// success clears), so this is a pure read: publish once the count meets the
/// threshold and a non-transient problem identity exists
/// (`workflow_task_state_machine.go:1050-1054`,
/// `mutable_state_impl.go:6478-6491 @ v1.31.0`).
///
/// The threshold is read *live* via [`reported_problems_threshold`], so a
/// mid-run override takes effect on the next call (Tier 3.22
/// `DynamicConfigChanges`). A zero threshold disables publication, matching
/// v1.31.0's dynamic-config semantics.
pub fn reported_problem_from_state(
    state: &tokeira_kernel::WorkflowState,
) -> Option<WorkflowTaskReportedProblem> {
    let threshold = reported_problems_threshold();
    if threshold == 0 || state.workflow_task_attempts_since_last_success < threshold {
        return None;
    }
    state
        .last_workflow_task_problem
        .clone()
        .map(|problem| WorkflowTaskReportedProblem {
            attempts_since_last_success: state.workflow_task_attempts_since_last_success,
            problem,
        })
}

/// Public runtime facade.
///
/// This is intentionally small. The point is to expose the
/// core server actions without dragging transport or
/// authentication into the same crate.
///
/// See `docs/crates/runtime.md` for the full
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
    /// Runs whose worker attempted to close past buffered events (an
    /// `UnhandledCommand`-rejected close). While set AND a started WFT is in
    /// flight, new signals are rejected with `WorkflowClosing` — the
    /// in-memory mirror of v1.31.0's volatile
    /// `MutableStateImpl.workflowCloseAttempted` (mutable_state_impl.go:191,
    /// set at workflow_task_state_machine.go:924-928; checked by
    /// signal_workflow_util.go:63-70). Cleared when a later WFT completes
    /// successfully (the workflow moved on) — v1.31.0 clears on
    /// mutable-state reload, which every WFT failure triggers.
    close_attempt_tracking: Arc<std::sync::Mutex<std::collections::HashSet<RunKey>>>,
    /// Runtime-local index of `BackingOff` completion callbacks to re-fire.
    completion_callback_tracking: CompletionCallbackTrackingState,
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
    /// Optional durable Worker Deployment registry used for v2 routing decisions.
    worker_deployment_repository: Option<Arc<dyn WorkerDeploymentRepository>>,
    /// Background Nexus-timeout scanner task.
    nexus_timeout_scanner_handle: Option<tokio::task::JoinHandle<()>>,
    /// Cancellation token for the Nexus-timeout scanner.
    nexus_timeout_scanner_cancel: CancellationToken,
    /// Background completion-callback retry scanner task.
    completion_callback_scanner_handle: Option<tokio::task::JoinHandle<()>>,
    /// Cancellation token for the completion-callback retry scanner.
    completion_callback_scanner_cancel: CancellationToken,
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

/// Runtime inputs for deleting one already-resolved workflow run.
#[derive(Clone, Debug, PartialEq)]
pub struct DeleteWorkflowRequest {
    /// Request identity persisted if an open target must first terminate.
    pub request: RequestContext,
    /// Stable admission time used by both termination and the deletion tombstone.
    pub now: OffsetDateTime,
}

/// Result of an authoritative workflow deletion.
#[derive(Clone, Debug, PartialEq)]
pub struct WorkflowDeletion {
    /// Exact projection-log record persisted atomically with the purge.
    pub tombstone: ProjectionRecord,
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
        /// First WFT committed as started for inline eager delivery.
        eager_workflow_task: Option<StartedWorkflowTask>,
    },
    UsedExisting {
        run_key: RunKey,
        run_id: RunId,
    },
    /// A retried start whose RequestId already authored the open incumbent's
    /// `WorkflowExecutionStarted` is deduped to that run. v1.31.0 returns this as
    /// `Started: true` with the incumbent's status (startworkflow/api.go:332-336,
    /// respondToRetriedRequest). No new run is created.
    Deduped {
        run_key: RunKey,
        run_id: RunId,
        execution_status: ExecutionStatus,
        /// Still-live first WFT reconstructed for an eager request-id retry.
        eager_workflow_task: Option<StartedWorkflowTask>,
    },
    Rejected {
        run_key: RunKey,
        run_id: RunId,
        reason: StartRejectReason,
    },
}

/// Why a start (or signal-with-start) was rejected against the incumbent run.
/// The edge renders each as v1.31.0's exact `WorkflowExecutionAlreadyStarted`
/// message (`workflow_id_dedup.go:95-129 @ v1.31.0`) — the corpus asserts the
/// policy-specific suffixes verbatim.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartRejectReason {
    /// `WorkflowIdConflictPolicy::Fail` against a RUNNING incumbent.
    ConflictPolicyFail,
    /// `WorkflowIdReusePolicy::RejectDuplicate` against a closed incumbent.
    ReuseRejectDuplicate,
    /// `WorkflowIdReusePolicy::AllowDuplicateFailedOnly` against an incumbent
    /// that finished successfully.
    ReuseAllowFailedOnly,
}

/// Result of the composed Update-with-Start (`ExecuteMultiOperation`,
/// exactly `[Start, Update]`): both legs' outcomes plus whether the start
/// created a new run (`multioperation/api.go @ v1.31.0`).
#[derive(Clone, Debug, PartialEq)]
pub struct MultiOperationResult {
    pub run_key: RunKey,
    pub run_id: RunId,
    /// Whether the start leg created a new run (false on attach/dedup/replay).
    pub started: bool,
    /// Execution status reported on the start response.
    pub execution_status: ExecutionStatus,
    /// The update leg's lifecycle snapshot (stage + outcome).
    pub update: UpdateLifecycleSnapshot,
}

/// The failing leg of an Update-with-Start, so the edge can serialize the
/// per-operation `MultiOperationExecutionFailure`: the failing op carries its
/// own error, the sibling carries Aborted + `MultiOperationExecutionAborted`,
/// the top-level code is the first failing op's, and the message is
/// "Update-with-Start could not be executed."
/// (`serviceerror.MultiOperationExecution.Status()` @ v1.31.0).
#[derive(Debug, thiserror::Error)]
pub enum MultiOperationError {
    /// The START leg was rejected by conflict/reuse policy; the update leg
    /// aborts as the sibling.
    #[error("update-with-start start leg rejected: {reason:?}")]
    StartRejected {
        run_key: RunKey,
        run_id: RunId,
        reason: StartRejectReason,
    },
    /// The UPDATE leg failed. `started` records whether the start leg
    /// created a run (its op serializes OK) or aborts as the sibling.
    #[error("update-with-start update leg failed: {source}")]
    UpdateFailed {
        started: bool,
        #[source]
        source: anyhow::Error,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum SignalWithStartResult {
    Started {
        run_key: RunKey,
        run_id: RunId,
    },
    Signaled {
        run_key: RunKey,
        run_id: RunId,
    },
    Rejected {
        run_key: RunKey,
        run_id: RunId,
        reason: StartRejectReason,
    },
}

/// Aggregate runtime tuning passed to [`TokeiraRuntime`] at construction.
///
/// These are mechanical settings (lane count, scanner intervals, retry/drain
/// bounds), not deployment-environment knobs: `tokeirad` builds this from
/// `Default` rather than from TOML, leaving the values for auto-tune to own.
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
    UseExisting {
        run_key: RunKey,
        run_id: RunId,
    },
    TerminateAndStart {
        run_key: RunKey,
    },
    ClosedAllowReuse,
    Rejected {
        run_key: RunKey,
        run_id: RunId,
        reason: StartRejectReason,
    },
    /// The incoming RequestId already authored the open incumbent's
    /// `WorkflowExecutionStarted`: this is a retry of the original start and is
    /// deduped to the incumbent before any conflict policy applies (v1.31.0
    /// handleConflict, startworkflow/api.go:328-336).
    DedupRetried {
        run_key: RunKey,
        run_id: RunId,
        execution_status: ExecutionStatus,
    },
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
    ///
    /// This is the entry point of a constructor ladder: each `new_with_*`
    /// overload fills in one more optional dependency (Nexus, shard
    /// ownership, node endpoint) and delegates inward, so all paths
    /// converge on `new_with_nexus_and_shards_and_endpoint`, the single place
    /// that actually wires brokers, lanes, and scanners.
    pub fn new(
        repo: Arc<R>,
        lane_count: usize,
        config: LaneConfig,
        timer_config: TimerScannerConfig,
        workflow_timeout_config: WorkflowTimeoutScannerConfig,
        backlog_config: BacklogConfig,
    ) -> Self {
        Self::new_with_nexus(
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
            NexusCompletionDeps::default(),
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
        nexus_completion: NexusCompletionDeps,
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
            nexus_completion,
            1,
            IncarnationId::new().to_string(),
            true,
        )
    }

    pub fn new_with_nexus_config(
        repo: Arc<R>,
        runtime_config: RuntimeConfig,
        nexus_registry: NexusEndpointRegistry,
        nexus_client: Arc<dyn NexusHttpClient>,
        nexus_completion: NexusCompletionDeps,
    ) -> Self {
        Self::new_with_nexus(
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
            nexus_completion,
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
        nexus_completion: NexusCompletionDeps,
        shard_count: u32,
        owner_identity: String,
        seed_default_shard: bool,
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
            nexus_completion,
            shard_count,
            owner_identity,
            "127.0.0.1:0".to_owned(),
            seed_default_shard,
            None,
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
        nexus_completion: NexusCompletionDeps,
        shard_count: u32,
        owner_identity: String,
        node_endpoint: String,
        seed_default_shard: bool,
        // Resolves originator namespace names for the External-endpoint outbound metric;
        // wired by the server bootstrap, `None` for the simpler constructors and tests.
        namespace_resolver: Option<Arc<dyn NexusNamespaceResolver>>,
    ) -> Self {
        let broker = InMemoryBroker::default();
        let activity_broker = InMemoryActivityBroker::default();
        let workflow_timeout_tracking = WorkflowTimeoutTrackingState::default();
        let wft_timeout_tracking = WftTimeoutTrackingState::default();
        let activity_tracking = ActivityTrackingState::default();
        let nexus_timeout_tracking = NexusTimeoutTrackingState::default();
        let completion_callback_tracking = CompletionCallbackTrackingState::default();
        let NexusCompletionDeps {
            client: nexus_completion_client,
            config: nexus_completion_config,
            scanner: completion_callback_scanner_config,
        } = nexus_completion;
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
        // The publisher needs lane handles to route follow-up work (child
        // resolutions, continue-as-new starts), but the lanes don't exist yet.
        // Share a slot the publisher captures now and we backfill once the
        // lanes are spawned, breaking the construction-order cycle.
        let shared_lanes = Arc::new(Mutex::new(Vec::with_capacity(lane_count)));
        // Precise in-memory timers for speculative workflow tasks share the
        // `shared_lanes` slot (backfilled below) to submit timeouts, and are
        // installed into the WFT-timeout tracking so the lane's post-commit hook
        // arms/disarms them through the handle it already holds (spec
        // speculative-wft R.2).
        wft_timeout_tracking.set_speculative(crate::speculative_timer::SpeculativeTimerSet::new(
            shared_lanes.clone(),
            lane_count,
        ));
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
                    nexus_completion_client.clone(),
                    nexus_completion_config.clone(),
                    nexus_registry.clone(),
                    nexus_task_broker.clone(),
                    nexus_timeout_tracking.clone(),
                    completion_callback_tracking.clone(),
                    activity_tracking.clone(),
                    delivery_metrics.clone(),
                )
                .with_namespace_resolver(namespace_resolver.clone());
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
            // Single-node / no-controller deployments have no placement
            // controller to grant shard ownership, so seed shard 0 as locally
            // owned at the zero epoch. Controller-managed deployments pass
            // `false` and acquire ownership through durable leases instead.
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
            repo.clone(),
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
            ActivityRetryDeps {
                repo: repo.clone(),
                shard_owner: shard_owner.clone(),
                controller_managed_placement: config.controller_managed_placement,
                max_occ_retries: config.max_occ_retries,
                broker: activity_broker.clone(),
                delivery_metrics: delivery_metrics.clone(),
                tracking: activity_tracking.clone(),
            },
            lanes.clone(),
            lane_count,
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
            repo.clone(),
            nexus_timeout_tracking.clone(),
            lanes.clone(),
            lane_count,
            shard_owner.clone(),
            nexus_timeout_config,
            nexus_timeout_scanner_cancel.clone(),
        )));
        // The completion-callback scanner re-fires `BackingOff` callbacks. It reuses the
        // publisher's delivery path, so it gets its own publisher handle (mirrors how the
        // lanes each hold one) wired to the same lanes + tracking index.
        let completion_scanner_publisher = RuntimeDispatchPublisher::new(
            broker.clone(),
            activity_broker.clone(),
            repo.clone(),
            shared_lanes.clone(),
            lane_count,
            shard_count,
            nexus_client.clone(),
            nexus_completion_client.clone(),
            nexus_completion_config.clone(),
            nexus_registry.clone(),
            nexus_task_broker.clone(),
            nexus_timeout_tracking.clone(),
            completion_callback_tracking.clone(),
            activity_tracking.clone(),
            delivery_metrics.clone(),
        )
        .with_namespace_resolver(namespace_resolver.clone());
        let completion_callback_scanner_cancel = CancellationToken::new();
        let completion_callback_scanner_handle =
            Some(tokio::spawn(run_completion_callback_scanner(
                repo.clone(),
                completion_callback_tracking.clone(),
                completion_scanner_publisher,
                shard_owner.clone(),
                completion_callback_scanner_config,
                completion_callback_scanner_cancel.clone(),
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
            close_attempt_tracking: Arc::new(std::sync::Mutex::new(
                std::collections::HashSet::new(),
            )),
            completion_callback_tracking,
            nexus_task_broker,
            update_registry,
            buffered_queries,
            worker_registry,
            heartbeat_store,
            delivery_metrics,
            fairness_state,
            nexus_timeout_scanner_handle,
            nexus_timeout_scanner_cancel,
            completion_callback_scanner_handle,
            completion_callback_scanner_cancel,
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
            worker_deployment_repository: None,
        }
    }

    /// Attach a durable Worker Deployment registry, enabling version-aware
    /// (v2) task routing. Without it traffic is dispatched unversioned;
    /// routing must not depend on this being present.
    pub fn with_worker_deployment_repository(
        mut self,
        repository: Arc<dyn WorkerDeploymentRepository>,
    ) -> Self {
        self.worker_deployment_repository = Some(repository);
        self
    }

    /// Return a clone of the workflow-task broker.
    pub fn broker(&self) -> InMemoryBroker {
        self.broker.clone()
    }

    /// Return a clone of the activity-task broker.
    pub fn activity_broker(&self) -> InMemoryActivityBroker {
        self.activity_broker.clone()
    }

    /// Cancel outstanding workflow polls and reject subsequent polls for a
    /// shutting-down worker when the v1.31.0 feature flag is enabled.
    ///
    /// The broker wake makes parked long polls return normally with an empty
    /// response; correctness state is untouched because polls are disposable
    /// matching state (`workflow_handler.go:3050-3118 @ v1.31.0`).
    pub async fn cancel_outstanding_worker_polls(
        &self,
        namespace_id: NamespaceId,
        task_queue: TaskQueueName,
        worker: WorkerIdentity,
    ) -> bool {
        if !cancel_worker_polls_on_shutdown() {
            return false;
        }
        self.broker
            .deny_worker(namespace_id, task_queue.clone(), worker.clone())
            .await;
        self.activity_broker
            .deny_worker(namespace_id, task_queue, worker)
            .await;
        true
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

    pub fn completion_callback_tracking(&self) -> CompletionCallbackTrackingState {
        self.completion_callback_tracking.clone()
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

    /// Build the Worker Deployment registry view used by transport adapters.
    ///
    /// The registry object is cheap to assemble because it only clones repository
    /// handles. Keeping construction here ensures edge code never reaches directly
    /// into runtime-owned storage fields.
    pub fn deployment_registry(&self) -> Option<DeploymentRegistry>
    where
        R: 'static,
    {
        self.worker_deployment_repository
            .as_ref()
            .map(|repository| {
                DeploymentRegistry::with_repositories(
                    repository.clone(),
                    self.repo.clone(),
                    self.worker_registry.clone(),
                )
            })
    }

    pub fn heartbeat_store(&self) -> Arc<dyn HeartbeatStore> {
        self.heartbeat_store.clone()
    }

    pub fn update_registry(&self) -> UpdateRegistry {
        self.update_registry.clone()
    }

    pub fn pending_update_transports(
        &self,
        run_key: RunKey,
        include_sent: bool,
    ) -> Vec<PendingUpdateTransport> {
        self.update_registry
            .drain_pending_updates(run_key, include_sent)
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

    /// Reject a workflow-task completion whose token was minted under a
    /// superseded shard epoch.
    ///
    /// A worker may have polled a task while this node owned the shard, then
    /// completed it after ownership moved (or this node's lease was fenced).
    /// Comparing the token's epoch against the current local epoch ensures a
    /// stale worker's completion can't be admitted on a shard we no longer own;
    /// the authoritative fence is still the storage commit, this is the cheap
    /// front-line check.
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

    /// Route `run_key` to its owning lane.
    ///
    /// Routing is by run identity, not task queue: the workflow execution is
    /// the serialization boundary, so the same run always lands on the same
    /// lane and never executes concurrently with itself.
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

    pub async fn shutdown_completion_callback_scanner(&mut self) -> Result<()> {
        self.completion_callback_scanner_cancel.cancel();
        if let Some(handle) = self.completion_callback_scanner_handle.take() {
            tokio::time::timeout(tokio::time::Duration::from_secs(5), handle)
                .await
                .map_err(|_| anyhow!("completion callback scanner shutdown timed out"))?
                .map_err(|error| anyhow!("completion callback scanner join failed: {error}"))?;
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
    ///
    /// This is the sweeper contract that makes ephemeral-first delivery safe:
    /// because the broker is never authoritative, lost in-memory offers can be
    /// rebuilt from the durable dispatch backlog after a restart or shard
    /// failover.
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

/// Whether a command originates from an external caller (client/edge) rather
/// than from internal in-flight machinery (task completions, timer fires,
/// child/nexus resolutions).
///
/// Used by drain admission: while a node is draining a shard it must stop
/// accepting *new* external work but still let in-flight commands finish so
/// runs can reach a clean handoff point. Misclassifying an in-flight command
/// as external would deadlock the drain.
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
    /// User-visible run id of the run. Distinct from the internal storage
    /// `RunKey` (derived via `dsql_spread_uuid`); wire surfaces must carry
    /// this, never the key.
    pub run_id: tokeira_types::RunId,
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
    /// Run's workflow-task timeout, carried for transient-suffix synthesis
    /// (the synthesized `WorkflowTaskScheduled` needs it; nothing persisted).
    pub workflow_task_timeout: time::Duration,
    /// Identity of the polling worker (transient `WorkflowTaskStarted` synthesis).
    pub worker_identity: tokeira_types::WorkerIdentity,
    /// Opaque token used to complete the task.
    pub token: WorkflowTaskToken,
}

/// Work returned from the Temporal-compatible workflow poll path.
///
/// Temporal v1.31.0 matches direct legacy queries through the same worker poll
/// RPC as workflow tasks (`service/matching/matching_engine.go:1084 @
/// v1.31.0`). Keeping this as an explicit enum prevents query-only activations
/// from being mistaken for started workflow tasks that advanced history.
#[derive(Debug)]
pub enum WorkflowActivation {
    /// A history-advancing workflow task has been started and must be completed
    /// through `RespondWorkflowTaskCompleted`.
    WorkflowTask(StartedWorkflowTask),
    /// A read-only legacy query task must be answered through
    /// `RespondQueryTaskCompleted`.
    QueryTask(QueryTask),
}

impl WorkflowActivation {
    /// Borrow the started workflow task when this activation advanced history.
    pub fn workflow_task(&self) -> Option<&StartedWorkflowTask> {
        match self {
            Self::WorkflowTask(task) => Some(task),
            Self::QueryTask(_) => None,
        }
    }
}

/// An activity task that has been polled and started.
#[derive(Clone, Debug)]
pub struct StartedActivityTask {
    /// Unique key for the owning workflow run.
    pub run_key: RunKey,
    /// User-visible run id of the owning run (see `StartedWorkflowTask::run_id`).
    pub run_id: tokeira_types::RunId,
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
    /// Latest durable heartbeat progress for this activity.
    pub heartbeat_details: Option<Payloads>,
    /// Original activity schedule timestamp.
    pub scheduled_time: OffsetDateTime,
    /// Schedule timestamp for the current attempt, when known.
    pub current_attempt_scheduled_time: Option<OffsetDateTime>,
    /// Server-authored start timestamp for this attempt.
    pub started_time: OffsetDateTime,
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
            EndpointTarget, NexusCompletionRuntimeConfig, NexusEndpointRegistry, NexusTaskBroker,
            NexusTimeoutScannerConfig, NexusTimeoutTrackingState, NoopNexusCompletionClient,
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
    };
    use tokeira_kernel::{
        CallbackSpec, CallbackState, CallbackTrigger, CompletionCallback, Link, OnConflictOptions,
        RetryState, WorkflowTaskFailedCause,
    };
    use tokeira_storage::{
        BacklogEntry, CommitResult, DispatchableWorkflowTask, InMemoryStore, RequestRecord,
        RunRepository, TransitionAuditRecord,
    };
    use tokeira_types::{
        ExecutionRef, LogicalTaskSeq, Memo, NamespaceId, Payloads, RequestContext, RequestId,
        SearchAttributes, TaskKind, WorkflowId,
    };

    /// Minimal open-run state for exercising the reported-problems derive.
    fn reported_problem_state(
        attempts_since_last_success: u32,
        problem: Option<tokeira_kernel::WorkflowTaskProblem>,
    ) -> tokeira_kernel::WorkflowState {
        let mut state = crate::runtime::workflow_task::tests::open_state(
            "reported-problems-workflow".to_string(),
            None,
        );
        state.workflow_task_attempts_since_last_success = attempts_since_last_success;
        state.last_workflow_task_problem = problem;
        state
    }

    #[test]
    fn reported_problem_appears_at_default_threshold_and_carries_latest_cause() {
        for below in 0..REPORTED_PROBLEMS_THRESHOLD {
            let state = reported_problem_state(
                below,
                Some(tokeira_kernel::WorkflowTaskProblem::Failed(
                    WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure,
                )),
            );
            assert!(reported_problem_from_state(&state).is_none());
        }

        let state = reported_problem_state(
            REPORTED_PROBLEMS_THRESHOLD,
            Some(tokeira_kernel::WorkflowTaskProblem::Failed(
                WorkflowTaskFailedCause::NonDeterminismError,
            )),
        );
        assert_eq!(
            reported_problem_from_state(&state),
            Some(WorkflowTaskReportedProblem {
                attempts_since_last_success: REPORTED_PROBLEMS_THRESHOLD,
                problem: tokeira_kernel::WorkflowTaskProblem::Failed(
                    WorkflowTaskFailedCause::NonDeterminismError
                ),
            })
        );
    }

    #[test]
    fn reported_problem_counter_survives_rescheduling_and_clears_on_success() {
        // The kernel carries the accumulator across task rescheduling and
        // zeroes it (plus the problem identity) on completion; the derive
        // publishes only while BOTH survive. A count with no recorded
        // non-transient problem publishes nothing — v1.31.0's SA composer
        // renders nothing when `LastWorkflowTaskFailure` is nil
        // (`mutable_state_impl.go:6478-6491 @ v1.31.0`).
        let above_threshold = REPORTED_PROBLEMS_THRESHOLD + 2;
        let state = reported_problem_state(
            above_threshold,
            Some(tokeira_kernel::WorkflowTaskProblem::TimedOutStartToClose),
        );
        assert_eq!(
            reported_problem_from_state(&state)
                .expect("problem observation")
                .attempts_since_last_success,
            above_threshold
        );

        let cleared = reported_problem_state(0, None);
        assert!(reported_problem_from_state(&cleared).is_none());

        let count_without_identity = reported_problem_state(above_threshold, None);
        assert!(reported_problem_from_state(&count_without_identity).is_none());
    }

    // Feature: conformance-config-override, Property 1: off-feature equivalence.
    // A production (non-`conformance`) build resolves the reported-problems threshold
    // to the pinned constant with no registry read — the accessor is the constant by
    // construction, so a production binary cannot be influenced by any override.
    #[cfg(not(feature = "conformance"))]
    #[test]
    fn reported_problems_threshold_is_pinned_constant_off_feature() {
        assert_eq!(reported_problems_threshold(), REPORTED_PROBLEMS_THRESHOLD);
    }

    // Feature: conformance-config-override, Property 1: off-feature equivalence
    // (feature-on, no override). Building the `conformance` feature alone changes
    // nothing: with no override set, the live-read accessor still yields the pinned
    // default. Only an actual override moves it (covered by the registry's lifecycle
    // property test).
    #[cfg(feature = "conformance")]
    #[test]
    fn reported_problems_threshold_defaults_to_constant_without_override() {
        tokeira_conformance::overrides()
            .clear("system.numConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute");
        assert_eq!(reported_problems_threshold(), REPORTED_PROBLEMS_THRESHOLD);
    }

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
        let mut eager_retry = request.clone();
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
        assert!(!history[0].kind.eager_execution_accepted());

        // Feature: edge-eager-dispatch, Properties 5/6. A duplicate may flip
        // its request flag, but the original event-1 decision remains
        // authoritative and prevents a false-history/true-response mismatch.
        eager_retry.eager_execution_accepted = true;
        eager_retry.request.caller_identity = Some("late-eager-worker".to_string());
        let retried = runtime
            .start_workflow_with_policy(eager_retry)
            .await
            .unwrap();
        assert!(matches!(
            retried,
            StartWorkflowResult::Deduped {
                eager_workflow_task: None,
                ..
            }
        ));
        assert_eq!(repo.read_history(run_key, 0, 16).await.unwrap(), history);
    }

    #[tokio::test]
    async fn eager_start_with_positive_backoff_commits_without_inline_task() {
        // Feature: edge-eager-dispatch, Properties 2/5. A positive first-WFT
        // backoff is decided before the commit so a durable start cannot be
        // followed by a response-construction failure for a nonexistent WFT.
        let repo = Arc::new(InMemoryStore::default());
        let runtime = TokeiraRuntime::new(
            repo.clone(),
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
            BacklogConfig::default(),
        );
        let mut request = sample_start_request(None, None);
        request.request.caller_identity = Some("eager-worker".to_string());
        request.eager_execution_accepted = true;
        request.workflow_start_delay = Some(Duration::seconds(5));

        let result = runtime
            .start_workflow_with_policy(request.clone())
            .await
            .unwrap();
        assert!(matches!(
            result,
            StartWorkflowResult::Started {
                eager_workflow_task: None,
                ..
            }
        ));

        let history = repo.read_history(request.run_key, 0, 16).await.unwrap();
        assert!(!history[0].kind.eager_execution_accepted());
        assert!(
            !history
                .iter()
                .any(|event| matches!(event.kind, HistoryEventKind::WorkflowTaskStarted { .. }))
        );
    }

    #[tokio::test]
    async fn eager_start_retry_reconstructs_live_task_and_omits_expired() {
        // Feature: edge-eager-dispatch, Property 6: Request-ID retry reconstruction.
        // v1.31.0 returns the first eager WFT only while attempt 1 remains live;
        // reconstructing an elapsed task before the coarse timeout sweep runs
        // would hand the SDK an already-invalid token.
        let repo = Arc::new(InMemoryStore::default());
        let runtime = TokeiraRuntime::new(
            repo.clone(),
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
            BacklogConfig::default(),
        );
        let mut request = sample_start_request(None, None);
        let accepted_at = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        request.now = accepted_at;
        request.request.received_at = accepted_at;
        request.request.caller_identity = Some("eager-worker".to_string());
        request.workflow_task_timeout = Duration::seconds(10);
        request.eager_execution_accepted = true;

        let first = runtime
            .start_workflow_with_policy(request.clone())
            .await
            .unwrap();
        let first_task = match first {
            StartWorkflowResult::Started {
                eager_workflow_task: Some(task),
                ..
            } => task,
            other => panic!("expected fresh eager task, got {other:?}"),
        };
        assert_eq!(first_task.token.started_event_id, 3);
        assert_eq!(first_task.token.attempt, 1);
        let LoadedRun::Existing(before_state) = repo.load_run(request.run_key).await.unwrap()
        else {
            panic!("fresh eager start should exist");
        };
        let before_history = repo
            .read_history(request.run_key, 0, usize::MAX)
            .await
            .unwrap();

        let immediate = runtime
            .start_workflow_with_policy(request.clone())
            .await
            .unwrap();
        let retry_task = match immediate {
            StartWorkflowResult::Deduped {
                eager_workflow_task: Some(task),
                ..
            } => task,
            other => panic!("expected deduped eager task, got {other:?}"),
        };
        assert_eq!(retry_task, first_task);
        let LoadedRun::Existing(immediate_state) = repo.load_run(request.run_key).await.unwrap()
        else {
            panic!("deduped eager start should still exist");
        };
        assert_eq!(immediate_state.transition_seq, before_state.transition_seq);
        assert_eq!(
            repo.read_history(request.run_key, 0, usize::MAX)
                .await
                .unwrap(),
            before_history
        );

        request.now = accepted_at + request.workflow_task_timeout;
        let retried = runtime
            .start_workflow_with_policy(request.clone())
            .await
            .unwrap();
        assert!(matches!(
            retried,
            StartWorkflowResult::Deduped {
                eager_workflow_task: None,
                ..
            }
        ));

        let LoadedRun::Existing(after_state) = repo.load_run(request.run_key).await.unwrap() else {
            panic!("deduped eager start should still exist");
        };
        let after_history = repo
            .read_history(request.run_key, 0, usize::MAX)
            .await
            .unwrap();
        assert_eq!(after_state.transition_seq, before_state.transition_seq);
        assert_eq!(after_history, before_history);
    }

    #[tokio::test]
    async fn start_use_existing_applies_on_conflict_attachments() {
        let repo = Arc::new(InMemoryStore::default());
        let runtime = TokeiraRuntime::new(
            repo.clone(),
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
            BacklogConfig::default(),
        );
        let first = sample_start_request(None, None);
        let started = runtime
            .start_workflow_with_policy(first.clone())
            .await
            .unwrap();
        assert!(matches!(started, StartWorkflowResult::Started { .. }));

        let callback = CompletionCallback {
            spec: CallbackSpec::Nexus {
                url: "https://callback.example/run".to_string(),
                header: Default::default(),
            },
            links: Vec::new(),
            trigger: CallbackTrigger::WorkflowClosed,
            registration_time: None,
            state: CallbackState::Standby,
            attempt: 0,
            last_attempt_failure: None,
            next_attempt_at: None,
        };
        let link = Link::BatchJob {
            job_id: "batch-1".to_string(),
        };
        let mut second = sample_start_request(None, None);
        second.namespace_id = first.namespace_id;
        second.workflow_id = first.workflow_id.clone();
        second.conflict_policy = WorkflowIdConflictPolicy::UseExisting;
        second.completion_callbacks = vec![callback.clone()];
        second.links = vec![link.clone()];
        second.on_conflict_options = Some(OnConflictOptions {
            attach_request_id: true,
            attach_completion_callbacks: true,
            attach_links: true,
        });
        second.request.request_id = RequestId("attach-req".to_string());

        let reused = runtime.start_workflow_with_policy(second).await.unwrap();
        assert_eq!(
            reused,
            StartWorkflowResult::UsedExisting {
                run_key: first.run_key,
                run_id: first.run_id
            }
        );

        let LoadedRun::Existing(state) = repo.load_run(first.run_key).await.unwrap() else {
            panic!("started run should still exist");
        };
        assert_eq!(state.completion_callbacks.len(), 1);
        assert_eq!(state.completion_callbacks[0].spec, callback.spec);
        assert_eq!(state.completion_callbacks[0].links, callback.links);
        assert_eq!(state.completion_callbacks[0].trigger, callback.trigger);
        assert!(state.completion_callbacks[0].registration_time.is_some());
        assert_eq!(state.completion_callbacks[0].state, callback.state);
        assert_eq!(state.completion_callbacks[0].attempt, callback.attempt);
        assert_eq!(
            state.completion_callbacks[0].last_attempt_failure,
            callback.last_attempt_failure
        );
        assert_eq!(state.links, vec![link]);
        // request_id_infos maps the start request → STARTED and the attached
        // request → OPTIONS_UPDATED (Req 5.2/5.3), surfaced on Describe.
        let started_info = state
            .request_id_infos
            .get(&first.request.request_id.0)
            .expect("start request id recorded");
        assert_eq!(
            started_info.event_type,
            tokeira_kernel::EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
        );
        let attached_info = state
            .request_id_infos
            .get("attach-req")
            .expect("attached request id recorded");
        assert_eq!(
            attached_info.event_type,
            tokeira_kernel::EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED
        );
        let history = repo.read_history(first.run_key, 0, 10).await.unwrap();
        assert!(history.iter().any(|event| matches!(
            &event.kind,
            HistoryEventKind::WorkflowExecutionOptionsUpdated {
                attached_request_id: Some(id),
                ..
            } if id == "attach-req"
        )));
    }

    #[tokio::test]
    async fn start_fail_conflict_rejects_without_attaching() {
        let repo = Arc::new(InMemoryStore::default());
        let runtime = TokeiraRuntime::new(
            repo.clone(),
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
            BacklogConfig::default(),
        );
        let first = sample_start_request(None, None);
        let started = runtime
            .start_workflow_with_policy(first.clone())
            .await
            .unwrap();
        assert!(matches!(started, StartWorkflowResult::Started { .. }));

        // A second start for the same workflow id with Fail must resolve to the
        // existing run as Rejected (→ WorkflowExecutionAlreadyStarted at the edge),
        // not create a new run and not attach (Req 1.2, 2.x).
        let mut second = sample_start_request(None, None);
        second.namespace_id = first.namespace_id;
        second.workflow_id = first.workflow_id.clone();
        second.conflict_policy = WorkflowIdConflictPolicy::Fail;
        second.request.request_id = RequestId("fail-req".to_string());
        let rejected = runtime.start_workflow_with_policy(second).await.unwrap();
        assert_eq!(
            rejected,
            StartWorkflowResult::Rejected {
                run_key: first.run_key,
                run_id: first.run_id,
                reason: StartRejectReason::ConflictPolicyFail,
            }
        );

        // Fail does not attach: the incumbent records only its own start request.
        let LoadedRun::Existing(state) = repo.load_run(first.run_key).await.unwrap() else {
            panic!("started run should still exist");
        };
        assert!(
            state
                .request_id_infos
                .contains_key(&first.request.request_id.0)
        );
        assert!(!state.request_id_infos.contains_key("fail-req"));
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
                NexusCompletionDeps::default(),
                8,
                "test-owner".to_string(),
                true,
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
                NexusCompletionDeps::default(),
                shard_count,
                "test-owner".to_string(),
                true,
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
                NexusCompletionDeps::default(),
                4,
                node_id.to_string(),
                "127.0.0.1:7233".to_owned(),
                true,
                None,
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
            client_discards_speculative_with_events: false,
            token: WorkflowTaskToken {
                run_key: RunKey::new(),
                logical_seq: LogicalTaskSeq(1),
                started_event_id: 1,
                attempt: 1,
                shard_epoch: ShardEpoch::ZERO,
            },
            identity: WorkerIdentity("worker".to_owned()),
            sdk_metadata: None,
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: tokeira_kernel::VersioningBehavior::Unspecified,
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            commands: Vec::new(),
            force_new_workflow_task: false,
            delivered_update_ids: Vec::new(),
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

        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        assert_eq!(
            evaluate_activity_retry(&policy, 1, None, false, now, None),
            RetryDecision::Retry {
                next_attempt: 2,
                backoff: Duration::seconds(1),
            }
        );
        assert_eq!(
            evaluate_activity_retry(&policy, 3, None, false, now, None),
            RetryDecision::Exhausted {
                reason: crate::retry::RetryExhaustedReason::MaximumAttemptsReached,
            }
        );
        assert_eq!(
            evaluate_activity_retry(&policy, 1, Some("fatal"), false, now, None),
            RetryDecision::Exhausted {
                reason: crate::retry::RetryExhaustedReason::NonRetryableFailure,
            }
        );
        // A worker-flagged non-retryable failure stops retries even when its
        // type is not in the policy's list (`isRetryable` checks the flag
        // before the list, retry.go:139-147 @ v1.31.0).
        assert_eq!(
            evaluate_activity_retry(&policy, 1, None, true, now, None),
            RetryDecision::Exhausted {
                reason: crate::retry::RetryExhaustedReason::NonRetryableFailure,
            }
        );
        // Next attempt would begin past the schedule-to-close expiration →
        // terminal Timeout (retry.go:108-110 @ v1.31.0).
        assert_eq!(
            evaluate_activity_retry(
                &policy,
                1,
                None,
                false,
                now,
                Some(now + Duration::milliseconds(500))
            ),
            RetryDecision::Exhausted {
                reason: crate::retry::RetryExhaustedReason::Timeout,
            }
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
            Arc::new(NoopNexusCompletionClient),
            NexusCompletionRuntimeConfig::default(),
            NexusEndpointRegistry::default(),
            NexusTaskBroker::default(),
            NexusTimeoutTrackingState::default(),
            CompletionCallbackTrackingState::default(),
            ActivityTrackingState::default(),
            DeliveryMetrics::new(),
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
                    dispatch_revision: 0,
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
            workflow_start_delay: None,
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
            workflow_start_delay: None,
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
            workflow_start_delay: None,
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
            workflow_start_delay: None,
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
            workflow_start_delay: None,
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
            workflow_start_delay: None,
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
        use crate::nexus::{
            InMemoryNexusEndpointStore, NexusEndpointSpec, NexusEndpointSpecTarget,
            NexusEndpointStore,
        };
        // The registry now resolves against the live store; seed an endpoint through
        // the store's create path (server-authored id/version) and resolve by name.
        let store = std::sync::Arc::new(InMemoryNexusEndpointStore::new());
        store
            .create(
                NexusEndpointSpec {
                    name: "payments".to_string(),
                    description: Vec::new(),
                    target: NexusEndpointSpecTarget::External {
                        url: "http://payments".to_string(),
                    },
                },
                0,
            )
            .expect("create endpoint");
        let registry = NexusEndpointRegistry::new(store);
        assert_eq!(
            registry
                .resolve("payments")
                .and_then(|config| match &config.target {
                    EndpointTarget::External { address } => Some(address.clone()),
                    EndpointTarget::Worker { .. } => None,
                })
                .as_deref(),
            Some("http://payments")
        );
        assert!(registry.resolve("missing").is_none());
    }

    #[test]
    fn evaluate_nexus_timeout_cases() {
        use tokeira_kernel::{NexusTimeoutType, PendingNexusOperation};

        let scheduled_at = OffsetDateTime::now_utc() - Duration::seconds(10);
        let base = PendingNexusOperation {
            operation_id: "op".to_string(),
            scheduled_event_id: 11,
            endpoint: "ep".to_string(),
            service: "svc".to_string(),
            operation: "op".to_string(),
            schedule_to_close_timeout: None,
            schedule_to_start_timeout: None,
            start_to_close_timeout: None,
            scheduled_at,
            started: false,
            started_at: None,
            attempt: 0,
            last_attempt_failure: None,
            next_attempt_at: None,
            operation_token: String::new(),
            input: Default::default(),
        };

        // Schedule-to-close fires once scheduled_at + timeout has passed.
        let stc = PendingNexusOperation {
            schedule_to_close_timeout: Some(Duration::seconds(1)),
            ..base.clone()
        };
        assert_eq!(
            evaluate_nexus_timeout(&stc, OffsetDateTime::now_utc()),
            Some(NexusTimeoutType::ScheduleToClose)
        );

        // An unset (None) timeout never fires; an unstarted op with no applicable
        // deadline yields None.
        assert_eq!(
            evaluate_nexus_timeout(&base, OffsetDateTime::now_utc()),
            None
        );

        // A zero timeout means "no deadline" for Nexus (v1.31.0 only emits the
        // task when the duration is non-zero), so it must not fire.
        let zero = PendingNexusOperation {
            schedule_to_close_timeout: Some(Duration::ZERO),
            ..base.clone()
        };
        assert_eq!(
            evaluate_nexus_timeout(&zero, OffsetDateTime::now_utc()),
            None
        );

        // Schedule-to-start only applies while not started.
        let sts = PendingNexusOperation {
            schedule_to_start_timeout: Some(Duration::seconds(1)),
            ..base.clone()
        };
        assert_eq!(
            evaluate_nexus_timeout(&sts, OffsetDateTime::now_utc()),
            Some(NexusTimeoutType::ScheduleToStart)
        );
        let sts_started = PendingNexusOperation {
            started: true,
            started_at: Some(OffsetDateTime::now_utc()),
            ..sts.clone()
        };
        assert_eq!(
            evaluate_nexus_timeout(&sts_started, OffsetDateTime::now_utc()),
            None
        );

        // Start-to-close is anchored at started_at and only applies once started.
        let started_at = OffsetDateTime::now_utc() - Duration::seconds(5);
        let stc2 = PendingNexusOperation {
            start_to_close_timeout: Some(Duration::seconds(1)),
            started: true,
            started_at: Some(started_at),
            ..base.clone()
        };
        assert_eq!(
            evaluate_nexus_timeout(&stc2, OffsetDateTime::now_utc()),
            Some(NexusTimeoutType::StartToClose)
        );

        // Not-yet-due deadline yields None.
        let pending = PendingNexusOperation {
            schedule_to_close_timeout: Some(Duration::seconds(30)),
            ..base.clone()
        };
        assert_eq!(
            evaluate_nexus_timeout(&pending, scheduled_at + Duration::seconds(5)),
            None
        );
    }

    #[test]
    fn workflow_timeout_tracking_state_crud() {
        let tracking = WorkflowTimeoutTrackingState::default();
        let entry = WorkflowTimeoutEntry {
            run_key: RunKey::new(),
            shard_id: ShardId(0),
            workflow_execution_timeout: Some(Duration::seconds(1)),
            workflow_run_timeout: None,
            workflow_start_delay: None,
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
                workflow_start_delay: None,
                started_at,
                first_run_started_at,
                has_retry_policy: false,
            };

            let result = evaluate_workflow_timeout(&entry, now);
            let execution_origin =
                entry.first_run_started_at.unwrap_or(entry.started_at);
            if let Some(exec) = entry.workflow_execution_timeout
                && (now - execution_origin > exec
                    || (exec.is_zero() && now >= execution_origin))
                {
                    prop_assert_eq!(result, Some(WorkflowTimeoutViolation::ExecutionTimeout));
                    return Ok(());
                }
            if let Some(run) = entry.workflow_run_timeout
                && (now - started_at > run || (run.is_zero() && now >= started_at)) {
                    prop_assert_eq!(result, Some(WorkflowTimeoutViolation::RunTimeout));
                    return Ok(());
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
                workflow_start_delay: None,
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
                        workflow_start_delay: None,
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
                        workflow_start_delay: None,
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
            initiator: None,
            run_key: RunKey::new(),
            namespace_id: NamespaceId::new(),
            workflow_id: WorkflowId("workflow-timeout".to_string()),
            run_id,
            workflow_type: tokeira_types::WorkflowType("example".to_string()),
            task_queue: TaskQueueName("workflow-q".to_string()),
            deployment: None,
            build_id: None,
            versioning_override: None,
            workflow_start_delay: None,
            client_cron_schedule: None,
            completion_callbacks: Vec::new(),
            user_metadata: None,
            links: Vec::new(),
            on_conflict_options: None,
            priority: None,
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
            parent_namespace_name: None,
            parent_initiated_event_id: 0,
            root_workflow_id: None,
            root_run_id: None,
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
            eager_execution_accepted: false,
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

        async fn list_runs_for_namespace(
            &self,
            _namespace_id: tokeira_types::NamespaceId,
        ) -> Result<Vec<RunKey>> {
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

        async fn delete_run_for_bundle(
            &self,
            _run_key: RunKey,
            _execution_home_bundle: ShardId,
            _request: tokeira_storage::DeleteRunRequest,
            _epoch: ShardEpoch,
        ) -> Result<tokeira_storage::DeleteRunResult> {
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

        async fn list_runs_with_pending_completion_callbacks_for_shard(
            &self,
            _shard_id: tokeira_types::ShardId,
            _limit: usize,
        ) -> Result<Vec<tokeira_storage::CompletionCallbackSweepEntry>> {
            panic!("unused in timer scanner tests")
        }
    }
}
