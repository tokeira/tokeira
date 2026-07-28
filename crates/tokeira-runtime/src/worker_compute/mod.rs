//! Provider-neutral Worker Compute Controller policy and orchestration.
//!
//! This module owns capacity-demand policy outside workflow history. Delivery
//! components may report bounded observations here, but those reports never affect
//! task correctness or ordering. Durable controller and outbox state is separate from
//! authoritative per-run state and can never reconstruct or complete a task.

mod config;
mod model;
mod observation;
mod outbox;
mod ports;
mod provider;
mod provider_contract;
mod reconciliation;
mod sampling;
mod scaler;
mod service;

use std::time::Duration;

pub use tokeira_storage::{
    ClaimedWorkerComputeController, ClaimedWorkerComputeProviderAction,
    InMemoryWorkerComputeRepository, WorkerComputeActionAttemptStart, WorkerComputeActionClaim,
    WorkerComputeActionFinalization, WorkerComputeActionFinalizeResult,
    WorkerComputeControllerAdmission, WorkerComputeControllerClaim,
    WorkerComputeControllerCommitResult, WorkerComputeControllerHealthView,
    WorkerComputeControllerRecord, WorkerComputeHealthFilter, WorkerComputeProviderAction,
    WorkerComputeQueueSample, WorkerComputeRepository, WorkerComputeScalingGroupState,
};

pub use config::{
    EffectiveScalingGroup, NoSyncConfig, RemoteNexusProvider, UnsupportedScalingGroup,
    UnsupportedScalingGroupReason, ValidatedComputeConfig, ValidatedScalingGroup,
    WorkerComputeConfigError, validate_compute_config,
};
pub use model::{
    DemandMatchKind, DemandObservation, MetricsSnapshot, NoSyncState, ObservationBatch,
    ScaleUpDecision, ScalerDecision, ScalerSuppression, TaskTypeMetrics, TaskTypeObservationCounts,
    WorkerComputeNamespace,
};
pub use observation::ObservationBatcher;
pub use outbox::{WorkerComputeOutbox, WorkerComputeOutboxSweep, action_retry_delay};
pub use ports::{
    ChannelDemandObservationSink, ChannelWorkerComputeReconcileSink, DemandObservationSink,
    DisabledWorkerComputeSink, ObserveResult, SystemWorkerComputeClock, WorkerComputeCatalogError,
    WorkerComputeClock, WorkerComputeNamespaceCatalog, WorkerComputeReconcileSink,
};
pub use provider::{
    NexusWorkerComputeProvider, WorkerComputeProvider, WorkerComputeProviderAttempt,
    WorkerComputeProviderTargetKind,
};
pub use provider_contract::{
    ProviderActionInput, WorkerComputeNexusInvocation, WorkerComputeProviderCompletion,
    WorkerComputeProviderContractError, WorkerComputeProviderOutcome, build_provider_action,
    provider_nexus_invocation, validate_provider_completion,
};
pub use reconciliation::WorkerComputeReconciler;
pub use sampling::{
    WorkerComputeQueueCounters, WorkerComputeQueueMetrics, WorkerComputeQueueSampler,
    aggregate_queue_samples, metrics_for_group,
};
pub use scaler::{evaluate_metrics, evaluate_task_add};
pub use service::{WorkerComputeActiveShards, WorkerComputeControllerService};

/// Maximum number of pending best-effort demand observations per process.
pub const OBSERVATION_CHANNEL_CAPACITY: usize = 4096;
/// Maximum number of prompt deployment-reconciliation hints per process.
pub const RECONCILE_CHANNEL_CAPACITY: usize = 4096;
/// Minimum batching delay after a no-sync observation.
pub const NO_SYNC_BATCH_INTERVAL: Duration = Duration::from_millis(500);
/// Maximum batching delay for a batch containing only sync matches.
pub const SYNC_ONLY_BATCH_INTERVAL: Duration = Duration::from_secs(60);
/// Period between full deployment-catalog reconciliation passes.
pub const CATALOG_RECONCILE_INTERVAL: Duration = Duration::from_secs(60);
/// Period between queue-home metric samples.
pub const QUEUE_SAMPLE_INTERVAL: Duration = Duration::from_secs(10);
/// Maximum age of a queue-home metric sample.
pub const QUEUE_SAMPLE_TTL: Duration = Duration::from_secs(120);
/// Soft limit on active controller instances in one namespace.
pub const MAX_CONTROLLER_INSTANCES_PER_NAMESPACE: usize = 100;
/// Lease protecting one controller evaluation.
pub const CONTROLLER_CLAIM_LEASE: Duration = Duration::from_secs(30);
/// Deadline for one Nexus provider attempt.
pub const PROVIDER_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(120);
/// Initial provider-action retry interval.
pub const ACTION_RETRY_INITIAL_INTERVAL: Duration = Duration::from_secs(1);
/// Maximum provider-action retry interval.
pub const ACTION_RETRY_MAXIMUM_INTERVAL: Duration = Duration::from_secs(60 * 60);
/// Exponential provider-action retry multiplier.
pub const ACTION_RETRY_COEFFICIENT: u32 = 2;
/// Durable claim lease covering a provider attempt and finalization margin.
pub const ACTION_CLAIM_LEASE: Duration = Duration::from_secs(150);
/// Idle delay between empty namespace-scoped outbox scans.
pub const ACTION_DELIVERY_IDLE_INTERVAL: Duration = Duration::from_secs(1);
