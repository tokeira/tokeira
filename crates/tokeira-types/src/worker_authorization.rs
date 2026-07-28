//! Server-authored identity for work exposed to an authenticated Worker.
//!
//! These values describe where a task was actually started after routing. They
//! contain no authorization policy: the auth and edge crates decide whether an
//! origin is permitted, while runtime and storage only transport or persist it.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{BuildId, DeploymentId, NamespaceId, QueueKey, TaskQueueName};

/// Worker-facing task family used by scoped authorization and provenance.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WorkerTaskClass {
    /// A Workflow Task that advances Workflow state.
    Workflow,
    /// An Activity Task executed outside the deterministic kernel.
    Activity,
    /// A legacy Query task correlated with a Workflow poll.
    Query,
    /// A Nexus task dispatched through a Nexus task queue.
    Nexus,
}

/// Error returned when decoding a durable Worker task-class value.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error("unknown Worker task class database value {value}")]
pub struct WorkerTaskClassDecodeError {
    /// Unknown database value.
    pub value: i16,
}

impl WorkerTaskClass {
    /// Stable database encoding used by task-provenance persistence.
    pub const fn to_db_smallint(self) -> i16 {
        match self {
            Self::Workflow => 0,
            Self::Activity => 1,
            Self::Query => 2,
            Self::Nexus => 3,
        }
    }
}

impl TryFrom<i16> for WorkerTaskClass {
    type Error = WorkerTaskClassDecodeError;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Workflow),
            1 => Ok(Self::Activity),
            2 => Ok(Self::Query),
            3 => Ok(Self::Nexus),
            value => Err(WorkerTaskClassDecodeError { value }),
        }
    }
}

/// Exact server-authored origin of a task returned to a Worker.
///
/// `normal_task_queue` remains stable for sticky Workflow delivery, while
/// `deployment` and `build_id` identify the final versioned queue that actually
/// started the task.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkerTaskOrigin {
    /// Stable namespace identity, independent of a namespace display name.
    pub namespace_id: NamespaceId,
    /// Application task queue authorized by a Worker scope.
    pub normal_task_queue: TaskQueueName,
    /// Kind of Worker task represented by the public token.
    pub task_class: WorkerTaskClass,
    /// Exact Worker Deployment selected by routing.
    pub deployment: DeploymentId,
    /// Exact Build ID selected by routing.
    pub build_id: BuildId,
}

impl WorkerTaskOrigin {
    /// Construct origin metadata from a final runtime queue key.
    ///
    /// Empty version strings represent existing unversioned delivery. Scoped
    /// admission requires a non-empty exact pair and therefore cannot mistake
    /// this compatibility representation for versioned authority.
    #[must_use]
    pub fn from_queue_key(
        queue: &QueueKey,
        normal_task_queue: TaskQueueName,
        task_class: WorkerTaskClass,
    ) -> Self {
        Self {
            namespace_id: queue.namespace_id,
            normal_task_queue,
            task_class,
            deployment: queue
                .deployment
                .clone()
                .unwrap_or_else(|| DeploymentId(String::new())),
            build_id: queue
                .build_id
                .clone()
                .unwrap_or_else(|| BuildId(String::new())),
        }
    }

    /// Whether the origin carries a complete exact Deployment Version.
    #[must_use]
    pub fn is_versioned(&self) -> bool {
        !self.deployment.0.is_empty() && !self.build_id.0.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TaskKind;

    #[test]
    fn worker_task_class_database_mapping_is_stable() {
        for (class, value) in [
            (WorkerTaskClass::Workflow, 0),
            (WorkerTaskClass::Activity, 1),
            (WorkerTaskClass::Query, 2),
            (WorkerTaskClass::Nexus, 3),
        ] {
            assert_eq!(class.to_db_smallint(), value);
            assert_eq!(WorkerTaskClass::try_from(value), Ok(class));
        }
        assert!(WorkerTaskClass::try_from(-1).is_err());
        assert!(WorkerTaskClass::try_from(4).is_err());
    }

    #[test]
    fn final_queue_version_and_normal_queue_are_distinct_origin_axes() {
        let namespace_id = NamespaceId::new();
        let final_queue = QueueKey {
            namespace_id,
            task_queue: TaskQueueName("sticky-worker-cache".to_owned()),
            task_kind: TaskKind::Workflow,
            deployment: Some(DeploymentId("payments".to_owned())),
            build_id: Some(BuildId("2026.07.28".to_owned())),
        };

        let origin = WorkerTaskOrigin::from_queue_key(
            &final_queue,
            TaskQueueName("payments-workflows".to_owned()),
            WorkerTaskClass::Workflow,
        );

        assert_eq!(origin.namespace_id, namespace_id);
        assert_eq!(origin.normal_task_queue.0, "payments-workflows");
        assert_eq!(origin.deployment.0, "payments");
        assert_eq!(origin.build_id.0, "2026.07.28");
        assert!(origin.is_versioned());
    }

    #[test]
    fn unversioned_origin_uses_empty_exact_version_pair() {
        let final_queue = QueueKey {
            namespace_id: NamespaceId::new(),
            task_queue: TaskQueueName("activities".to_owned()),
            task_kind: TaskKind::Activity,
            deployment: None,
            build_id: None,
        };

        let origin = WorkerTaskOrigin::from_queue_key(
            &final_queue,
            final_queue.task_queue.clone(),
            WorkerTaskClass::Activity,
        );

        assert_eq!(origin.deployment, DeploymentId(String::new()));
        assert_eq!(origin.build_id, BuildId(String::new()));
        assert!(!origin.is_versioned());
    }
}
