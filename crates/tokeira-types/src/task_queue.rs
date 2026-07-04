//! Task queues as the routing dimension for work dispatch.
//!
//! `TaskQueueName` is the logical name workers poll; `WorkerIdentity` is the
//! self-reported identity of a poller; `BuildId` and `DeploymentId` carry
//! versioning information for deterministic task routing. Together with
//! `NamespaceId` and `TaskKind`, these form the composite `QueueKey` that the
//! broker uses to index its internal dispatch state.

use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::{Duration, OffsetDateTime};

use crate::{NamespaceId, QueuePartition};

/// Discriminator for the two fundamental task kinds.
///
/// The broker maintains separate dispatch queues for workflow
/// tasks and activity tasks because their scheduling,
/// concurrency, and timeout semantics differ significantly.
///
/// See `docs/architecture/040-delivery-broker.md`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TaskKind {
    /// A workflow task drives the state machine forward by
    /// replaying history and producing commands.
    Workflow,
    /// An activity task executes a side-effecting operation
    /// outside the deterministic kernel.
    Activity,
}

/// Error returned when decoding a durable task-kind value from storage.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error("unknown task kind database value {value}")]
pub struct TaskKindDecodeError {
    /// Unknown database value.
    pub value: i16,
}

impl TaskKind {
    /// Stable database encoding used by DSQL persistence.
    pub fn to_db_smallint(self) -> i16 {
        match self {
            Self::Workflow => 0,
            Self::Activity => 1,
        }
    }
}

impl TryFrom<i16> for TaskKind {
    type Error = TaskKindDecodeError;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Workflow),
            1 => Ok(Self::Activity),
            value => Err(TaskKindDecodeError { value }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TaskKind;

    #[test]
    fn task_kind_database_mapping_is_stable() {
        assert_eq!(TaskKind::Workflow.to_db_smallint(), 0);
        assert_eq!(TaskKind::Activity.to_db_smallint(), 1);
        assert_eq!(TaskKind::try_from(0), Ok(TaskKind::Workflow));
        assert_eq!(TaskKind::try_from(1), Ok(TaskKind::Activity));
        assert!(TaskKind::try_from(2).is_err());
    }
}

/// Logical task-queue name as seen by workers and the SDK.
///
/// Workers poll a named queue; the runtime matches incoming
/// tasks to waiting pollers by this name.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskQueueName(pub String);

/// Self-reported identity string from a worker process.
///
/// Used for diagnostics, sticky routing, and operator
/// visibility. The value is set by the SDK and is not
/// validated by the server beyond length limits.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkerIdentity(pub String);

/// Immutable build identifier baked into a worker binary.
///
/// Part of the worker-versioning model: the runtime can route
/// tasks to workers running a specific build so that
/// non-determinism from code changes is avoided during replay.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BuildId(pub String);

/// Deployment identifier grouping a set of workers.
///
/// Used together with [`BuildId`] for versioned task routing.
/// A deployment typically maps to a single release of the
/// worker application.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeploymentId(pub String);

/// Composite key that fully identifies a dispatch queue.
///
/// A queue family is richer than a bare queue name. Runtime
/// delivery decisions often depend on task kind and
/// worker-versioning dimensions as well. The broker indexes
/// its internal state by `QueueKey`.
///
/// See `docs/architecture/040-delivery-broker.md`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QueueKey {
    /// Namespace that owns this queue.
    pub namespace_id: NamespaceId,
    /// Logical queue name (matches what workers poll).
    pub task_queue: TaskQueueName,
    /// Whether this queue carries workflow or activity tasks.
    pub task_kind: TaskKind,
    /// Optional deployment for versioned routing.
    pub deployment: Option<DeploymentId>,
    /// Optional build identifier for versioned routing.
    pub build_id: Option<BuildId>,
}

/// Full key for a queue partition, scoped to a queue family.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QueuePartitionKey {
    pub namespace_id: NamespaceId,
    pub task_queue: TaskQueueName,
    pub task_kind: TaskKind,
    pub partition: QueuePartition,
}

/// Sticky-execution affinity binding a run to a specific
/// worker.
///
/// When a workflow task completes, the runtime may record a
/// `StickyAffinity` so that the next task for the same run is
/// dispatched to the same worker. This avoids a full history
/// replay because the worker still holds the cached state.
///
/// The affinity expires at `expires_at`; after that the
/// runtime falls back to normal (non-sticky) dispatch.
///
/// See `docs/architecture/040-delivery-broker.md` for the
/// sticky-queue protocol.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StickyAffinity {
    /// Identity of the worker that holds cached run state.
    pub worker_identity: WorkerIdentity,
    /// The sticky task queue name the worker polls (v1.31.0
    /// `executionInfo.StickyTaskQueue`; sticky raise S1). While set, the next
    /// WFT is dispatched onto THIS queue, not the workflow's normal queue. An
    /// empty name (pre-S1 encodings via the serde default) disables sticky
    /// dispatch — the affinity degrades to a sync-match hint.
    #[serde(default)]
    pub sticky_queue: TaskQueueName,
    /// Per-dispatch schedule-to-start deadline for sticky WFTs
    /// (`sticky_attributes.schedule_to_start_timeout`;
    /// `executionInfo.StickyScheduleToStartTimeout` @ v1.31.0). A sticky WFT
    /// unstarted past this window times out with
    /// `WorkflowTaskTimedOut(SCHEDULE_TO_START)` and reschedules on the
    /// normal queue.
    #[serde(default)]
    pub schedule_to_start_timeout: Duration,
    /// Wall-clock deadline after which the affinity is stale.
    pub expires_at: OffsetDateTime,
}
