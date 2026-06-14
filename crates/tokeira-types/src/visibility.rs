use serde::{Deserialize, Serialize};

use crate::{RunKey, TransitionSeq};

/// Stable numeric identity for an execution archetype in the shared visibility plane.
///
/// Archetype ids are part of persisted projection keys, so they are deliberately
/// plain numeric values rather than enum variants owned by a single crate. CHASM
/// registries allocate non-zero ids for component roots; `0` is reserved for the
/// legacy workflow engine so workflow visibility is explicit instead of relying
/// on null-as-workflow semantics.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ArchetypeId(pub u32);

impl ArchetypeId {
    /// Reserved archetype for the existing workflow engine.
    pub const WORKFLOW: Self = Self(0);
}

/// Coarse lifecycle state used by archetype-neutral visibility queries.
///
/// `status_keyword` remains archetype-specific (`Running`, `Completed`, activity
/// statuses, etc.). This lifecycle value is the cross-archetype open/closed
/// discriminator used for list/count isolation and deletion semantics.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum VisibilityLifecycleState {
    /// The execution can still receive mutating work.
    Open,
    /// The execution reached a terminal state but remains visible.
    Closed,
    /// The execution was removed from normal visibility.
    Deleted,
}

impl VisibilityLifecycleState {
    /// Stable database encoding for DSQL `SMALLINT` columns.
    pub fn to_db_smallint(self) -> i16 {
        match self {
            Self::Open => 0,
            Self::Closed => 1,
            Self::Deleted => 2,
        }
    }
}

/// Error returned when decoding an unknown persisted lifecycle value.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
#[error("unknown visibility lifecycle database value {value}")]
pub struct VisibilityLifecycleStateDecodeError {
    /// Raw database value that did not map to a known lifecycle state.
    pub value: i16,
}

impl TryFrom<i16> for VisibilityLifecycleState {
    type Error = VisibilityLifecycleStateDecodeError;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Open),
            1 => Ok(Self::Closed),
            2 => Ok(Self::Deleted),
            value => Err(VisibilityLifecycleStateDecodeError { value }),
        }
    }
}

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
