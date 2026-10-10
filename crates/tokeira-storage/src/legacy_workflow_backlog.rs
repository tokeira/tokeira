//! Bounded disposal of inert workflow backlog copies after reconstruction.
//!
//! Coverage is checked with the current hot state and dispatch row in the same
//! delete transaction. This module never makes or consumes workflow intent.

use uuid::Uuid;

/// Maximum legacy keys inspected by one disposal transaction.
pub const LEGACY_WORKFLOW_KEYS: usize = 64;

/// One committed page, including deferred entries passed by its volatile cursor.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkflowBacklogDisposal {
    /// Last inspected primary key; retained only for this pass.
    pub last_examined: Option<Uuid>,
    /// Entries inspected, whether covered or deferred.
    pub examined: usize,
    /// Covered workflow entries deleted atomically.
    pub deleted: usize,
    /// Whether the traversal reached its end.
    pub exhausted: bool,
}
