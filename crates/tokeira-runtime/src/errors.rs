//! Runtime error types.
//!
//! Error types raised by the authoritative runtime when a request cannot be
//! served from local authoritative state. These sit at the runtime boundary: the
//! edge translates them into the appropriate public status codes. They are kept
//! distinct from kernel rejections (which describe *semantic* refusals) — these
//! describe *placement* and *resolution* failures: the local node is not the
//! owner, or authoritative state needed to answer is absent.

use thiserror::Error;
use tokeira_types::{BundleId, IncarnationId, RunKey, ShardEpoch};

/// Runtime rejected a mutation because the local node is not the active bundle owner.
///
/// Surfaced when a request reaches a node that does not currently hold the
/// bundle's lease. The carried epoch and owner let the edge/caller route to the
/// correct node instead of retrying blindly.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error(
    "not shard owner for bundle {bundle_id:?}: current_epoch={current_epoch:?}, current_owner={current_owner_node_id:?}"
)]
pub struct NotShardOwner {
    /// Bundle whose ownership was asserted.
    pub bundle_id: BundleId,
    /// Epoch the local node observed for the bundle.
    pub current_epoch: ShardEpoch,
    /// Owning node if known; `None` when the local node simply knows it is not
    /// the owner but cannot name who is.
    pub current_owner_node_id: Option<IncarnationId>,
}

impl NotShardOwner {
    /// Construct the error for the local-node case, where the owning node is
    /// unknown (`current_owner_node_id` is `None`) — the local node knows only
    /// that it does not own the bundle at `current_epoch`.
    pub fn local(bundle_id: BundleId, current_epoch: ShardEpoch) -> Self {
        Self {
            bundle_id,
            current_epoch,
            current_owner_node_id: None,
        }
    }
}

/// A worker-presented activity task token failed revalidation against
/// authoritative state: the run or activity is gone, or the token's identity
/// (`schedule_event_id`, `attempt`) no longer matches the live activity.
///
/// This is the typed form of v1.31.0's `ErrActivityTaskNotFound` — the edge
/// maps it to the fixed NotFound message `"invalid activityID or activity
/// already timed out or invoking workflow is completed"`
/// (`service/history/consts/const.go:44-45` +
/// `IsActivityTaskNotFoundForToken`, `service/history/api/activity_util.go:58`
/// @ v1.31.0) regardless of `reason`, which exists for diagnostics only.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error("activity task not found: {reason}")]
pub struct ActivityTaskNotFound {
    /// Which revalidation check failed (diagnostic detail; never on the wire).
    pub reason: &'static str,
}

/// Runtime could not construct an activity task token from authoritative state.
///
/// Each variant marks a distinct point where token resolution failed against the
/// run's current state, so callers can distinguish "never existed" from "not yet
/// started" rather than collapsing both into a generic error.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ActivityTokenResolutionError {
    /// No run exists for the given key.
    #[error("run not found while resolving activity token: {run_key:?}")]
    RunNotFound { run_key: RunKey },

    /// The run exists but has no activity with this id.
    #[error("activity not found while resolving activity token: {run_key:?}/{activity_id}")]
    ActivityNotFound {
        run_key: RunKey,
        activity_id: String,
    },

    /// The activity exists but has not started, so no token can be minted yet.
    #[error("activity has not started while resolving activity token: {run_key:?}/{activity_id}")]
    ActivityNotStarted {
        run_key: RunKey,
        activity_id: String,
    },

    /// Catch-all for lower-level runtime failures encountered during resolution.
    #[error("activity token resolution failed: {0}")]
    Runtime(String),
}

/// A workflow-task completion carried an invalid command: the workflow task
/// was failed with the command's cause (already persisted) and the completion
/// call errors with INVALID_ARGUMENT carrying `message`
/// ("Mutable state was already persisted … although error is returned",
/// respondworkflowtaskcompleted/api.go:739-742 @ v1.31.0).
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error("{message}")]
pub struct InvalidWorkflowCommand {
    /// The v1.31.0 `wtFailedCause.Message()` — e.g. "UnhandledCommand".
    pub message: String,
}

/// Signal admission rejected because the workflow already ATTEMPTED to close
/// (a close command bounced off buffered events with `UnhandledCommand`) and
/// a started workflow task is in flight retrying that close — v1.31.0's
/// `consts.ErrWorkflowClosing`: RESOURCE_EXHAUSTED with cause BUSY_WORKFLOW,
/// scope NAMESPACE, and this exact message
/// (`IsWorkflowCloseAttempted() && HasStartedWorkflowTask()`,
/// signal_workflow_util.go:63-70 + consts/const.go:62-67 @ v1.31.0).
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error("workflow operation can not be applied because workflow is closing")]
pub struct WorkflowClosing;

/// Query admission rejected because the workflow cannot serve a query right
/// now — v1.31.0's `serviceerror.WorkflowNotReady` (gRPC FAILED_PRECONDITION
/// with a `WorkflowNotReadyFailure` detail). The message is one of the three
/// pre-dispatch guard messages from `queryworkflow/api.go:116-143 @ v1.31.0`,
/// asserted verbatim by the corpus.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error("{message}")]
pub struct WorkflowNotReady {
    pub message: &'static str,
}

/// A query waited out its deadline without any worker answering — the
/// frontend rewrites context-deadline errors to
/// `DeadlineExceeded("query timed out before a worker could process it")`
/// (`workflow_handler.go:3178-3180 @ v1.31.0`).
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error("query timed out before a worker could process it")]
pub struct QueryTimedOut;

/// A pre-accepted (admitted/sent) update was aborted because its run closed
/// with no successor — v1.31.0's `update.AbortedByWorkflowClosingErr`, a
/// gRPC NotFound with this exact message (errors_failures.go:10-35 +
/// abort_reason.go:25-121 @ v1.31.0).
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error("workflow update was aborted by closing workflow")]
pub struct UpdateAbortedByClosingWorkflow;

/// Runtime could not resolve a requested workflow update.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum UpdateLifecycleError {
    /// No update lifecycle state exists for the requested run and update id.
    #[error("update not found while resolving lifecycle: {run_key:?}/{update_id}")]
    UpdateNotFound { run_key: RunKey, update_id: String },
}
