use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::NamespaceId;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TaskKind {
    Workflow,
    Activity,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskQueueName(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkerIdentity(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BuildId(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeploymentId(pub String);

/// A queue family is richer than a queue name. Runtime delivery decisions often
/// depend on task kind and worker-versioning dimensions as well.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QueueKey {
    pub namespace_id: NamespaceId,
    pub task_queue: TaskQueueName,
    pub task_kind: TaskKind,
    pub deployment: Option<DeploymentId>,
    pub build_id: Option<BuildId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StickyAffinity {
    pub worker_identity: WorkerIdentity,
    pub expires_at: OffsetDateTime,
}
