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
