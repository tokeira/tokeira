use thiserror::Error;
use tokeira_types::{BundleId, IncarnationId, ShardEpoch};

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
