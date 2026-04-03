use serde::{Deserialize, Serialize};

use crate::{RunKey, TransitionSeq};

/// Stable cursor for projector progress.
///
/// The cursor is intentionally shaped around the log rather
/// than a specific SQL implementation. That keeps the
/// projector contract decoupled from any one storage engine
/// while still supporting replay and idempotence.
///
/// The projector reads transitions in `(partition_id,
/// run_key, transition_seq)` order and advances this cursor
/// after each batch. On restart it resumes from the stored
/// cursor position.
///
/// See `docs/architecture/070-projection-plane.md`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionCursor {
    /// Partition being consumed. The log is split into
    /// `fanout` partitions for parallel projection.
    pub partition_id: u32,
    /// Total number of partitions. Stored alongside the
    /// cursor so that a fanout change can be detected.
    pub fanout: u16,
    /// Last `RunKey` that was fully projected, or `None` if
    /// the cursor is at the beginning.
    pub last_run_key: Option<RunKey>,
    /// Last `TransitionSeq` that was fully projected for
    /// `last_run_key`, or `None` at the beginning.
    pub last_transition_seq: Option<TransitionSeq>,
}

impl ProjectionCursor {
    /// Create a cursor positioned at the very beginning of
    /// the given partition.
    pub fn beginning(partition_id: u32, fanout: u16) -> Self {
        Self {
            partition_id,
            fanout,
            last_run_key: None,
            last_transition_seq: None,
        }
    }
}
