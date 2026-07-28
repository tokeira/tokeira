//! Bounded runtime ports for catalog and delivery integration.
//!
//! Producers never await the controller. Failed hints are recoverable through
//! periodic catalog reconciliation and queue sampling, so these ports may drop work
//! without acquiring correctness responsibility.

use async_trait::async_trait;
use thiserror::Error;
use time::OffsetDateTime;
use tokeira_types::{ControllerInstanceKey, NamespaceId};
use tokio::sync::mpsc;

use super::{DemandObservation, WorkerComputeNamespace};
use crate::metrics as runtime_metrics;

/// Result of one non-blocking observation or reconciliation hint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObserveResult {
    /// Hint entered the bounded channel.
    Accepted,
    /// Channel was at capacity; periodic recovery remains authoritative.
    Full,
    /// Controller receiver has stopped.
    Closed,
    /// Startup policy disabled the controller.
    Disabled,
}

/// Non-blocking demand-observation target used by task brokers.
pub trait DemandObservationSink: Send + Sync {
    /// Attempt to enqueue one advisory observation without waiting.
    fn try_observe(&self, observation: DemandObservation) -> ObserveResult;
}

/// Bounded process-local observation channel used by delivery brokers.
///
/// The sender uses only [`mpsc::Sender::try_send`]. A slow or stopped controller
/// therefore cannot add latency or correctness responsibility to task publication.
#[derive(Clone, Debug)]
pub struct ChannelDemandObservationSink {
    sender: mpsc::Sender<DemandObservation>,
}

impl ChannelDemandObservationSink {
    /// Wrap one bounded Tokio channel sender.
    #[must_use]
    pub const fn new(sender: mpsc::Sender<DemandObservation>) -> Self {
        Self { sender }
    }
}

impl DemandObservationSink for ChannelDemandObservationSink {
    fn try_observe(&self, observation: DemandObservation) -> ObserveResult {
        let task_type = observation.task_type;
        let result = match self.sender.try_send(observation) {
            Ok(()) => ObserveResult::Accepted,
            Err(mpsc::error::TrySendError::Full(_)) => ObserveResult::Full,
            Err(mpsc::error::TrySendError::Closed(_)) => ObserveResult::Closed,
        };
        runtime_metrics::record_worker_compute_observation(task_type, result);
        result
    }
}

/// Non-blocking deployment-reconciliation target used after registry commits.
pub trait WorkerComputeReconcileSink: Send + Sync {
    /// Attempt to enqueue one advisory exact-version reconciliation hint.
    fn try_reconcile(&self, key: ControllerInstanceKey) -> ObserveResult;
}

/// Bounded process-local reconciliation channel used after registry commits.
#[derive(Clone, Debug)]
pub struct ChannelWorkerComputeReconcileSink {
    sender: mpsc::Sender<ControllerInstanceKey>,
}

impl ChannelWorkerComputeReconcileSink {
    /// Wrap one bounded Tokio channel sender.
    #[must_use]
    pub const fn new(sender: mpsc::Sender<ControllerInstanceKey>) -> Self {
        Self { sender }
    }
}

impl WorkerComputeReconcileSink for ChannelWorkerComputeReconcileSink {
    fn try_reconcile(&self, key: ControllerInstanceKey) -> ObserveResult {
        match self.sender.try_send(key) {
            Ok(()) => ObserveResult::Accepted,
            Err(mpsc::error::TrySendError::Full(_)) => ObserveResult::Full,
            Err(mpsc::error::TrySendError::Closed(_)) => ObserveResult::Closed,
        }
    }
}

/// Disabled observation/reconciliation target used on the default startup path.
#[derive(Clone, Copy, Debug, Default)]
pub struct DisabledWorkerComputeSink;

impl DemandObservationSink for DisabledWorkerComputeSink {
    fn try_observe(&self, observation: DemandObservation) -> ObserveResult {
        let result = ObserveResult::Disabled;
        runtime_metrics::record_worker_compute_observation(observation.task_type, result);
        result
    }
}

impl WorkerComputeReconcileSink for DisabledWorkerComputeSink {
    fn try_reconcile(&self, _key: ControllerInstanceKey) -> ObserveResult {
        ObserveResult::Disabled
    }
}

/// Namespace-catalog read failure with no edge/storage implementation leakage.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error("worker-compute namespace catalog failed: {message}")]
pub struct WorkerComputeCatalogError {
    /// Bounded operation context; callers must not include credentials.
    pub message: String,
}

/// Provider-neutral namespace catalog consumed by process-local reconciliation.
#[async_trait]
pub trait WorkerComputeNamespaceCatalog: Send + Sync {
    /// List active namespaces in stable namespace-ID order.
    async fn list_active(&self) -> Result<Vec<WorkerComputeNamespace>, WorkerComputeCatalogError>;

    /// Resolve the current public name for one durable namespace ID.
    async fn name_for_id(
        &self,
        namespace_id: NamespaceId,
    ) -> Result<Option<String>, WorkerComputeCatalogError>;
}

/// Explicit clock used by pure controller steps and deterministic tests.
pub trait WorkerComputeClock: Send + Sync {
    /// Return the current controller time.
    fn now(&self) -> OffsetDateTime;
}

/// Production wall clock; tests substitute a deterministic implementation.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemWorkerComputeClock;

impl WorkerComputeClock for SystemWorkerComputeClock {
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::now_utc()
    }
}

#[cfg(test)]
mod tests {
    use tokeira_types::{BuildId, DeploymentId, NamespaceId, TaskQueueName, WorkerComputeTaskType};

    use super::*;
    use crate::DemandMatchKind;

    fn observation() -> DemandObservation {
        DemandObservation {
            namespace_id: NamespaceId::new(),
            task_queue: TaskQueueName("capacity".to_owned()),
            task_type: WorkerComputeTaskType::Workflow,
            deployment_name: DeploymentId("payments".to_owned()),
            build_id: BuildId("2026-07-27".to_owned()),
            match_kind: DemandMatchKind::NoSync,
        }
    }

    #[test]
    fn bounded_observation_sink_reports_ready_full_closed_and_disabled() {
        let (sender, mut receiver) = mpsc::channel(1);
        let sink = ChannelDemandObservationSink::new(sender);
        let accepted = observation();

        assert_eq!(sink.try_observe(accepted.clone()), ObserveResult::Accepted);
        assert_eq!(sink.try_observe(observation()), ObserveResult::Full);
        assert_eq!(receiver.try_recv().expect("accepted observation"), accepted);
        drop(receiver);
        assert_eq!(sink.try_observe(observation()), ObserveResult::Closed);
        assert_eq!(
            DisabledWorkerComputeSink.try_observe(observation()),
            ObserveResult::Disabled
        );
    }

    #[test]
    fn bounded_reconcile_sink_is_non_blocking() {
        let (sender, mut receiver) = mpsc::channel(1);
        let sink = ChannelWorkerComputeReconcileSink::new(sender);
        let key = observation().controller_key();

        assert_eq!(sink.try_reconcile(key.clone()), ObserveResult::Accepted);
        assert_eq!(sink.try_reconcile(key.clone()), ObserveResult::Full);
        assert_eq!(receiver.try_recv().expect("accepted hint"), key);
        drop(receiver);
        assert_eq!(
            sink.try_reconcile(observation().controller_key()),
            ObserveResult::Closed
        );
    }
}
