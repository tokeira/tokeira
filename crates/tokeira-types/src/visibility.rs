use serde::{Deserialize, Serialize};

use crate::{RunKey, TransitionSeq};

/// Stable cursor for projector progress.
///
/// The cursor is intentionally shaped around the log rather than a specific SQL
/// implementation. That keeps the projector contract decoupled from any one
/// storage engine while still supporting replay and idempotence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionCursor {
    pub partition_id: u32,
    pub fanout: u16,
    pub last_run_key: Option<RunKey>,
    pub last_transition_seq: Option<TransitionSeq>,
}

impl ProjectionCursor {
    pub fn beginning(partition_id: u32, fanout: u16) -> Self {
        Self {
            partition_id,
            fanout,
            last_run_key: None,
            last_transition_seq: None,
        }
    }
}
