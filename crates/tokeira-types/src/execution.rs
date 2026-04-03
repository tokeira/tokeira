use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::{NamespaceId, RunId};

/// Human-readable namespace name.
///
/// This is the mutable display label operators see in the UI
/// and CLI. Internal routing and storage use [`NamespaceId`]
/// instead so that a rename does not invalidate keys or tokens.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NamespaceName(pub String);

/// User-assigned workflow identifier.
///
/// Together with [`NamespaceId`] this forms the logical
/// identity of a workflow. Multiple runs may share the same
/// `WorkflowId` (e.g. after continue-as-new or a cron
/// schedule), but at most one run is open at a time for a
/// given `(NamespaceId, WorkflowId)` pair.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct WorkflowId(pub String);

/// Workflow type name that maps to an SDK handler function.
///
/// The edge layer resolves this string to a registered handler
/// on the worker. It is also stored in the history so that
/// replay can locate the correct handler.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkflowType(pub String);

/// Lifecycle state visible to operators and projections.
///
/// These variants model the terminal and non-terminal states
/// a workflow execution can occupy. The projection plane
/// indexes on this value for list/count queries.
///
/// See `docs/architecture/070-projection-plane.md` for how
/// the projection plane consumes status transitions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ExecutionStatus {
    /// The workflow is actively executing or waiting for a
    /// task, timer, or activity result.
    Running,
    /// The workflow has been explicitly paused by an operator
    /// or signal. It can be resumed.
    Paused,
    /// The workflow returned a successful result.
    Completed,
    /// The workflow terminated with an application-level
    /// failure after exhausting retries.
    Failed,
    /// The workflow was cancelled via a cancellation request.
    Cancelled,
    /// The workflow was forcibly terminated by an operator
    /// without running cancellation logic.
    Terminated,
    /// The workflow ended by spawning a new run via
    /// continue-as-new.
    ContinuedAsNew,
    /// The workflow exceeded its execution timeout.
    TimedOut,
}

impl ExecutionStatus {
    /// Returns `true` for states that represent an in-progress
    /// execution (`Running` or `Paused`).
    ///
    /// Closed statuses (`Completed`, `Failed`, etc.) return
    /// `false`.
    pub fn is_open(self) -> bool {
        matches!(self, Self::Running | Self::Paused)
    }
}

/// Stable locator used by callers before a concrete run key
/// is known.
///
/// When `run_id` is `None`, the caller is asking for the
/// current open run for a `(namespace, workflow_id)` pair.
/// When `run_id` is present, storage must honour it and
/// resolve that specific historical or current run rather
/// than silently redirecting to the latest open run.
///
/// The edge layer constructs this from incoming gRPC requests
/// and passes it into the runtime for resolution.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExecutionRef {
    /// The namespace that owns this execution.
    pub namespace_id: NamespaceId,
    /// The user-assigned workflow identifier.
    pub workflow_id: WorkflowId,
    /// Optional run identifier. `None` means "current open
    /// run".
    pub run_id: Option<RunId>,
}

/// Minimal execution summary for list/count/read-model use.
///
/// The projection plane materialises one of these per run so
/// that visibility queries can be served without reading the
/// full history. See
/// `docs/architecture/070-projection-plane.md`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionSummary {
    /// Namespace that owns this execution.
    pub namespace_id: NamespaceId,
    /// User-assigned workflow identifier.
    pub workflow_id: WorkflowId,
    /// Unique run identifier for this execution.
    pub run_id: RunId,
    /// Workflow type name (maps to SDK handler).
    pub workflow_type: WorkflowType,
    /// Current lifecycle status.
    pub status: ExecutionStatus,
    /// Timestamp when the first event was recorded.
    pub started_at: OffsetDateTime,
    /// Timestamp when the execution reached a terminal state.
    /// `None` while the execution is still open.
    pub closed_at: Option<OffsetDateTime>,
}
