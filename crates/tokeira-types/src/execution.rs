use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::{NamespaceId, RunId};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NamespaceName(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkflowId(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkflowType(pub String);

/// Lifecycle state visible to operators and projections.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ExecutionStatus {
    Running,
    Completed,
    Failed,
    Cancelled,
    Terminated,
}

impl ExecutionStatus {
    pub fn is_open(self) -> bool {
        matches!(self, Self::Running)
    }
}

/// Stable locator used by callers before a concrete run key is known.
///
/// When `run_id` is `None`, the caller is asking for the current open run for a
/// `(namespace, workflow_id)` pair. When `run_id` is present, storage must
/// honor it and resolve that specific historical or current run rather than
/// silently redirecting to the latest open run.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExecutionRef {
    pub namespace_id: NamespaceId,
    pub workflow_id: WorkflowId,
    pub run_id: Option<RunId>,
}

/// Minimal execution summary for list/count/read-model use.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionSummary {
    pub namespace_id: NamespaceId,
    pub workflow_id: WorkflowId,
    pub run_id: RunId,
    pub workflow_type: WorkflowType,
    pub status: ExecutionStatus,
    pub started_at: OffsetDateTime,
    pub closed_at: Option<OffsetDateTime>,
}
