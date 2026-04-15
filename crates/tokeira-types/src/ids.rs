//! Identity types separating user-visible identifiers from internal storage keys.
//!
//! `RunId` and `WorkflowId` are what clients and operators see in APIs and logs.
//! `RunKey` and `ShardId` are internal storage and routing keys that can be
//! optimised for clustering and placement without leaking layout details into
//! the public API. `TransitionSeq` and `ShardEpoch` are monotonic fencing
//! values used for optimistic concurrency control.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Stable identifier for a namespace.
///
/// We keep this distinct from the human-facing namespace name
/// (see [`NamespaceName`](crate::NamespaceName)) because
/// storage, routing, and tokens should not depend on mutable
/// display names. A rename of the namespace in the control
/// plane must not invalidate every row key or token that
/// references it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NamespaceId(pub Uuid);

impl NamespaceId {
    /// Generate a fresh random namespace identifier.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

/// User-visible workflow run identifier.
///
/// This is what a client or operator would expect to see in
/// logs, APIs, and the Temporal UI. It is always a UUID but
/// carries no storage-layout semantics — that role belongs to
/// [`RunKey`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RunId(pub Uuid);

impl RunId {
    /// Generate a fresh random run identifier.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

/// Internal durable row key for a run.
///
/// `RunKey` exists so storage can optimise its own layout
/// (e.g. shard-prefixed clustering) without forcing the
/// user-visible [`RunId`] to play double duty as a clustering
/// key. Every persistence operation addresses a run by its
/// `RunKey`, not its `RunId`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RunKey(pub Uuid);

impl RunKey {
    /// Generate a fresh random run key.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

/// Routing and placement key used by the runtime/controller
/// layer.
///
/// Shards partition the run space so that the broker and
/// placement controller can assign non-overlapping subsets of
/// runs to lanes. See `docs/architecture/030-runtime-lanes.md`
/// and `docs/architecture/035-placement-and-membership.md`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ShardId(pub u32);

/// Fence token for a shard or bundle lease.
///
/// The important property is **monotonicity**, not global
/// uniqueness. When a lane acquires a shard it bumps the epoch;
/// any in-flight operation carrying a stale epoch is rejected.
/// See `docs/architecture/010-history-as-authority.md` for the
/// fencing model.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ShardEpoch(pub u64);

impl ShardEpoch {
    /// Epoch value used before any lease has been acquired.
    pub const ZERO: Self = Self(0);
}

/// Monotonic sequence number for authoritative run
/// transitions.
///
/// Each call to `apply` on a run's history increments this by
/// one. Storage uses it as an optimistic-concurrency fence:
/// a write is accepted only when the caller's
/// `TransitionSeq` matches the stored value.
///
/// See `docs/architecture/010-history-as-authority.md`.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct TransitionSeq(pub u64);

impl TransitionSeq {
    /// The initial sequence before any transition has been
    /// applied.
    pub const ZERO: Self = Self(0);

    /// Return the next sequence value.
    ///
    /// This is a pure function — it does not mutate `self`.
    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

/// Logical task identity used for workflow-task lifecycle
/// fencing.
///
/// This is intentionally **not** the same as a history event
/// ID. Event IDs are part of the observable history. Logical
/// task sequence numbers exist so the server can reject stale
/// starts/completions even when event shapes evolve.
///
/// The runtime embeds this in [`WorkflowTaskToken`] so that
/// a late-arriving completion from a previous attempt is
/// detected and discarded.
///
/// [`WorkflowTaskToken`]: crate::WorkflowTaskToken
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct LogicalTaskSeq(pub u64);

impl LogicalTaskSeq {
    /// The first logical task sequence (tasks start at 1).
    pub const ONE: Self = Self(1);

    /// Return the next sequence value.
    ///
    /// This is a pure function — it does not mutate `self`.
    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }
}
