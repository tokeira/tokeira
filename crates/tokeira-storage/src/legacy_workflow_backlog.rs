//! Bounded disposal of inert workflow and activity copies after reconstruction.
//!
//! Coverage is checked with the current hot state and dispatch row in the same
//! delete transaction. This module never makes or consumes execution intent.

use uuid::Uuid;

/// Maximum legacy keys inspected by one disposal transaction.
pub const LEGACY_WORKFLOW_KEYS: usize = crate::write_budget::MAX_MAINTENANCE_KEYS;

/// One committed page, including deferred entries passed by its volatile cursor.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkflowBacklogDisposal {
    /// Last inspected primary key; retained only for this pass.
    pub last_examined: Option<Uuid>,
    /// Entries inspected, whether covered or deferred.
    pub examined: usize,
    /// Entries whose coverage or supported payload could not be established.
    pub deferred: usize,
    /// Covered workflow/activity entries deleted atomically.
    pub deleted: usize,
    /// Whether the traversal reached its end.
    pub exhausted: bool,
}
