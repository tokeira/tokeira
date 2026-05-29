use thiserror::Error;
use tokeira_types::{BundleId, IncarnationId, RunKey, ShardEpoch};

/// Runtime rejected a mutation because the local node is not the active bundle owner.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error(
    "not shard owner for bundle {bundle_id:?}: current_epoch={current_epoch:?}, current_owner={current_owner_node_id:?}"
)]
pub struct NotShardOwner {
    pub bundle_id: BundleId,
    pub current_epoch: ShardEpoch,
    pub current_owner_node_id: Option<IncarnationId>,
}

impl NotShardOwner {
    pub fn local(bundle_id: BundleId, current_epoch: ShardEpoch) -> Self {
        Self {
            bundle_id,
            current_epoch,
            current_owner_node_id: None,
        }
    }
}

/// Runtime could not construct an activity task token from authoritative state.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ActivityTokenResolutionError {
    #[error("run not found while resolving activity token: {run_key:?}")]
    RunNotFound { run_key: RunKey },

    #[error("activity not found while resolving activity token: {run_key:?}/{activity_id}")]
    ActivityNotFound {
        run_key: RunKey,
        activity_id: String,
    },

    #[error("activity has not started while resolving activity token: {run_key:?}/{activity_id}")]
    ActivityNotStarted {
        run_key: RunKey,
        activity_id: String,
    },

    #[error("activity token resolution failed: {0}")]
    Runtime(String),
}
