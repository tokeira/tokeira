//! Volatile observations and pure controller inputs.
//!
//! These values are provider-neutral. Durable controller/action records live with
//! their repository in `tokeira-storage`; this module contains only runtime policy
//! inputs and outputs that do not carry task correctness.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tokeira_types::{
    BuildId, ControllerInstanceKey, DeploymentId, NamespaceId, TaskQueueName,
    WorkerComputeInvokeReason, WorkerComputeQueueKey, WorkerComputeTaskQueueBinding,
    WorkerComputeTaskType,
};

/// Whether publication found a compatible waiter immediately.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DemandMatchKind {
    /// A compatible waiter accepted the task immediately.
    Sync,
    /// The task entered ready/backlog delivery.
    NoSync,
}

/// Saturating observation counts for one task family within a version batch.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskTypeObservationCounts {
    /// Immediate matches for this task family.
    pub sync_count: u64,
    /// Publications that entered ready/backlog delivery for this task family.
    pub no_sync_count: u64,
}

/// Best-effort notification emitted after one unique versioned task publication.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DemandObservation {
    /// Namespace containing the task queue.
    pub namespace_id: NamespaceId,
    /// Logical task-queue family name.
    pub task_queue: TaskQueueName,
    /// Poll API family.
    pub task_type: WorkerComputeTaskType,
    /// Exact Worker Deployment name.
    pub deployment_name: DeploymentId,
    /// Exact Build ID.
    pub build_id: BuildId,
    /// Actual broker matching result.
    pub match_kind: DemandMatchKind,
}

impl DemandObservation {
    /// Controller instance affected by this observation.
    #[must_use]
    pub fn controller_key(&self) -> ControllerInstanceKey {
        ControllerInstanceKey {
            namespace_id: self.namespace_id,
            deployment_name: self.deployment_name.clone(),
            build_id: self.build_id.clone(),
        }
    }

    /// Exact queue identity shared by batching and periodic metrics.
    #[must_use]
    pub fn queue_key(&self) -> WorkerComputeQueueKey {
        WorkerComputeQueueKey {
            namespace_id: self.namespace_id,
            deployment_name: self.deployment_name.clone(),
            build_id: self.build_id.clone(),
            task_type: self.task_type,
            task_queue: self.task_queue.clone(),
        }
    }
}

/// One in-memory exact-version observation batch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservationBatch {
    /// First observation time, which anchors the sync-only deadline.
    pub first_observed_at: OffsetDateTime,
    /// First no-sync time, which anchors the shorter no-sync deadline.
    pub first_no_sync_at: Option<OffsetDateTime>,
    /// Saturating count of immediate matches.
    pub sync_count: u64,
    /// Saturating count of non-immediate matches.
    pub no_sync_count: u64,
    /// Task families represented in the batch.
    pub task_types: BTreeSet<WorkerComputeTaskType>,
    /// Sync/no-sync counts retained independently for correct group routing.
    #[serde(default)]
    pub counts_by_task_type: BTreeMap<WorkerComputeTaskType, TaskTypeObservationCounts>,
    /// Unique queue bindings represented in the batch.
    pub task_queues: BTreeSet<WorkerComputeTaskQueueBinding>,
}

/// Periodic metrics for one task family.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TaskTypeMetrics {
    /// Reconstructible tasks waiting for dispatch.
    pub backlog_count: u64,
    /// Recent successful dispatches per second.
    pub dispatch_rate: f64,
}

/// Exact-version aggregate supplied to one or more scaling groups.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    /// Workflow task metrics, absent when a group does not own Workflow.
    pub workflow: Option<TaskTypeMetrics>,
    /// Activity task metrics, absent when a group does not own Activity.
    pub activity: Option<TaskTypeMetrics>,
    /// Nexus task metrics, absent when a group does not own Nexus.
    pub nexus: Option<TaskTypeMetrics>,
}

impl MetricsSnapshot {
    /// Metrics for one task family.
    #[must_use]
    pub const fn get(&self, task_type: WorkerComputeTaskType) -> Option<TaskTypeMetrics> {
        match task_type {
            WorkerComputeTaskType::Workflow => self.workflow,
            WorkerComputeTaskType::Activity => self.activity,
            WorkerComputeTaskType::Nexus => self.nexus,
        }
    }
}

/// Durable pure-scaler state carried between evaluations.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NoSyncState {
    /// Shared most-recent scale-up timestamp.
    pub last_scale_up_at: Option<OffsetDateTime>,
    /// Most-recent dispatch rate independently per owned task type.
    pub prior_dispatch_rates: BTreeMap<WorkerComputeTaskType, f64>,
}

/// Pure-scaler output committed atomically with any provider action.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScalerDecision {
    /// Updated state, even when no action is required.
    pub next_state: NoSyncState,
    /// One bounded action; absence means no provider request.
    pub action: Option<ScaleUpDecision>,
    /// Requested metrics poll interval; absent for observation evaluation.
    pub next_poll_after: Option<Duration>,
    /// Task families for which an otherwise eligible action was suppressed.
    pub suppressions: BTreeMap<WorkerComputeTaskType, ScalerSuppression>,
}

/// Bounded reason why the scaler deliberately emitted no action.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScalerSuppression {
    /// A prior scale-up is still within the configured quiet period.
    Cooloff,
    /// Dispatch-rate movement stayed within the configured epsilon.
    Epsilon,
}

/// One scale-up decision produced by the pure policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScaleUpDecision {
    /// Why capacity is requested.
    pub reason: WorkerComputeInvokeReason,
    /// Provider invocation count; the active `no-sync` slice always emits one.
    pub count: u32,
}

/// Active namespace identity returned by the catalog port.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerComputeNamespace {
    /// Durable namespace ID used by all controller keys.
    pub namespace_id: NamespaceId,
    /// Public namespace name forwarded to providers.
    pub name: String,
}

#[cfg(test)]
mod tests {
    use tokeira_types::{TaskQueueName, WorkerComputeQueueKey};

    use super::*;

    #[test]
    fn worker_compute_domain_round_trips_without_type_erasure() {
        let value = DemandObservation {
            namespace_id: NamespaceId::new(),
            task_queue: TaskQueueName("capacity".to_owned()),
            task_type: WorkerComputeTaskType::Nexus,
            deployment_name: DeploymentId("payments".to_owned()),
            build_id: BuildId("2026-07-27".to_owned()),
            match_kind: DemandMatchKind::NoSync,
        };
        let encoded = serde_json::to_vec(&value).expect("domain value serializes");
        let decoded: DemandObservation =
            serde_json::from_slice(&encoded).expect("domain value deserializes");
        assert_eq!(decoded, value);
        assert_eq!(
            decoded.controller_key(),
            ControllerInstanceKey {
                namespace_id: decoded.namespace_id,
                deployment_name: decoded.deployment_name.clone(),
                build_id: decoded.build_id.clone(),
            }
        );

        let queue_key = WorkerComputeQueueKey {
            namespace_id: decoded.namespace_id,
            deployment_name: decoded.deployment_name,
            build_id: decoded.build_id,
            task_type: decoded.task_type,
            task_queue: decoded.task_queue,
        };
        assert_eq!(queue_key.task_type, WorkerComputeTaskType::Nexus);
    }
}
