use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Stable identifier for a namespace.
///
/// We keep this distinct from the human-facing namespace name because storage,
/// routing, and tokens should not depend on mutable display names.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NamespaceId(pub Uuid);

impl NamespaceId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

/// User-visible workflow run identifier.
///
/// This is what a client or operator would expect to see in logs and APIs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RunId(pub Uuid);

impl RunId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

/// Internal durable row key for a run.
///
/// `RunKey` exists so storage can optimize its own layout without forcing the
/// user-visible `RunId` to play double duty as a clustering key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RunKey(pub Uuid);

impl RunKey {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

/// Routing and placement key used by the runtime/controller layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ShardId(pub u32);

/// Fence token for a shard or bundle lease.
///
/// The important property is monotonicity, not global uniqueness.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ShardEpoch(pub u64);

impl ShardEpoch {
    pub const ZERO: Self = Self(0);
}

/// Monotonic sequence number for authoritative run transitions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TransitionSeq(pub u64);

impl TransitionSeq {
    pub const ZERO: Self = Self(0);

    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

/// Logical task identity used for workflow-task lifecycle fencing.
///
/// This is intentionally not the same as a history event ID. Event IDs are part
/// of the observable history. Logical task sequence numbers exist so the server
/// can reject stale starts/completions even when event shapes evolve.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct LogicalTaskSeq(pub u64);

impl LogicalTaskSeq {
    pub const ONE: Self = Self(1);

    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }
}
