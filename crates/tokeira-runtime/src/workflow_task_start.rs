//! Queue-home to execution-owner workflow-task start boundary.
//!
//! Transport may lose a reply. A caller must then leave its volatile offer lease
//! to expire; it must never undo the durable start or mint a token locally.

use std::fmt::Debug;

use anyhow::Result;
use async_trait::async_trait;
use tokeira_storage::DispatchableWorkflowTask;
use tokeira_types::{ShardId, WorkerIdentity};

use crate::StartedWorkflowTask;

/// Forwards one offered incarnation to its execution owner. Implementations
/// must preserve the caller's authorization and return the owner's task token.
#[async_trait]
pub trait WorkflowTaskStarter: Debug + Send + Sync {
    /// Resolve the current endpoint for `home` and attempt exactly one start.
    /// An ambiguous error is not evidence that the transition did not commit.
    async fn start(
        &self,
        home: ShardId,
        offered: DispatchableWorkflowTask,
        worker: WorkerIdentity,
    ) -> Result<StartedWorkflowTask>;
}

/// The execution owner conclusively rejected a withdrawn or superseded offer.
/// Matching may discard it and continue the poll without an acknowledgement.
#[derive(Debug, thiserror::Error)]
#[error("remote workflow offer is no longer startable")]
pub struct WorkflowOfferRejected;

/// An owner or transport is temporarily unavailable. Matching retains the
/// durable intent and continues the caller's existing long-poll deadline.
#[derive(Debug, thiserror::Error)]
#[error("workflow execution owner is temporarily unavailable: {0}")]
pub struct RetryableWorkflowTaskStart(pub String);
