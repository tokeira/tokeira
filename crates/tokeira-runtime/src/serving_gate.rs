//! Local execution-home admission during complete acquisition repair.
//!
//! A writer keeps its read permit through its commit and post-commit bookkeeping.
//! Acquisition first publishes Sweeping, then takes the exclusive permit: already
//! admitted writes finish before the first repair read, and later writers fail.
//! This is a single-owner barrier, not the transaction-local lease fence.

use std::sync::{Arc, RwLock};

use anyhow::Result;
use tokeira_types::{ShardEpoch, ShardId};
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard};

use crate::{
    errors::NotShardOwner,
    shard::{ShardAcquisition, ShardOwner},
};

pub(crate) type WritePermit = OwnedRwLockReadGuard<()>;

pub(crate) async fn admit(
    owner: &Arc<RwLock<ShardOwner>>,
    home: ShardId,
) -> Result<Option<WritePermit>> {
    let (barrier, acquisition) = {
        let owner = owner.read().expect("shard owner lock poisoned");
        if !owner.reconciliation_enabled() {
            return Ok(None);
        }
        let acquisition = owner
            .acquisition(home)
            .filter(|a| owner.acquisition_active(a))
            .ok_or_else(|| {
                NotShardOwner::local(home, owner.epoch_of(home).unwrap_or(ShardEpoch::ZERO))
            })?;
        (owner.write_barrier(home), acquisition)
    };
    let permit = tokio::select! {
        biased;
        _ = acquisition.cancel.cancelled() => return Err(NotShardOwner::local(home, acquisition.epoch).into()),
        permit = barrier.read_owned() => permit,
    };
    // Waiting for an earlier repair must not promote a command admitted by an
    // older generation into the replacement acquisition.
    if !owner
        .read()
        .expect("shard owner lock poisoned")
        .acquisition_active(&acquisition)
    {
        return Err(NotShardOwner::local(home, acquisition.epoch).into());
    }
    Ok(Some(permit))
}

pub(crate) async fn quiesce(
    owner: &Arc<RwLock<ShardOwner>>,
    acquisition: &ShardAcquisition,
) -> Result<Option<OwnedRwLockWriteGuard<()>>> {
    let barrier = {
        let owner = owner.read().expect("shard owner lock poisoned");
        if !owner.reconciliation_enabled() {
            return Ok(None);
        }
        owner.write_barrier(acquisition.shard_id)
    };
    tokio::select! {
        biased;
        _ = acquisition.cancel.cancelled() => anyhow::bail!("acquisition cancelled while draining writers"),
        permit = barrier.write_owned() => {
            validate(owner, acquisition)?;
            Ok(Some(permit))
        }
    }
}

pub(crate) fn validate(
    owner: &Arc<RwLock<ShardOwner>>,
    acquisition: &ShardAcquisition,
) -> Result<()> {
    anyhow::ensure!(
        acquisition.valid()
            && owner
                .read()
                .expect("shard owner lock poisoned")
                .matches_acquisition(acquisition),
        "workflow dispatch acquisition lost for execution home {:?}",
        acquisition.shard_id
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn acquisition_drains_writes_and_rejects_waiting_old_generation() {
        let owner = Arc::new(RwLock::new(ShardOwner::new(8)));
        let home = ShardId(3);
        owner.write().unwrap().enable_reconciliation();
        owner.write().unwrap().record_acquired(home, ShardEpoch(1));
        owner.write().unwrap().mark_active(home);
        let writer = admit(&owner, home).await.unwrap();
        owner.write().unwrap().record_acquired(home, ShardEpoch(1));
        let acquisition = owner.read().unwrap().acquisition(home).unwrap();
        assert!(admit(&owner, home).await.is_err());
        let barrier = owner.read().unwrap().write_barrier(home);
        assert!(barrier.clone().try_write_owned().is_err());
        drop(writer);
        let repair = quiesce(&owner, &acquisition).await.unwrap();
        assert!(barrier.clone().try_read_owned().is_err());
        owner.write().unwrap().activate_acquisition(&acquisition);
        drop(repair);
        assert!(admit(&owner, home).await.is_ok());
        acquisition.cancel.cancel();
        assert!(admit(&owner, home).await.is_err());
    }
    #[tokio::test]
    async fn local_expiry_blocks_activation_and_all_admission_views() {
        let owner = Arc::new(RwLock::new(ShardOwner::new(8)));
        let home = ShardId(3);
        owner.write().unwrap().enable_reconciliation();
        owner.write().unwrap().record_acquired(home, ShardEpoch(7));
        let acquisition = owner.read().unwrap().acquisition(home).unwrap();
        *acquisition.deadline.lock().unwrap() = Some(tokio::time::Instant::now());
        assert!(!owner.write().unwrap().activate_acquisition(&acquisition));
        assert!(validate(&owner, &acquisition).is_err());
        assert!(admit(&owner, home).await.is_err());
        assert!(!owner.read().unwrap().is_active(home));
        assert!(owner.read().unwrap().owns(home).is_none());
        assert_eq!(owner.read().unwrap().active_shards().count(), 0);
    }
}
