//! Singleton leadership via shard lease.
//!
//! # Why singleton leadership?
//!
//! The autoscaler must be a singleton — multiple instances making concurrent
//! scaling decisions would race against each other, causing oscillation and
//! potentially exceeding platform rate limits. Leadership election ensures
//! exactly one autoscaler instance is active at any time.
//!
//! # Why use the shard lease mechanism?
//!
//! Tokeira already has a battle-tested lease system for shard/bundle ownership
//! in the runtime. Rather than introducing a separate coordination mechanism
//! (e.g., a dedicated lock table), the autoscaler reuses the same lease
//! infrastructure with a dedicated bundle ID.
//!
//! # Why `u32::MAX` for the lease bundle?
//!
//! Runtime placement bundles use shard IDs in the normal range (0..N where N
//! is the configured shard count). By placing the autoscaler's lease at
//! `u32::MAX`, we guarantee it can never collide with any configured routing
//! bundle, regardless of how the shard count is configured. This is a
//! namespace separation trick — no coordination or configuration is needed
//! to avoid conflicts.

use std::{fmt, sync::Arc};

use anyhow::Result;
use time::Duration;
use tokeira_storage::{LeaseOutcome, LeaseRepository};
use tokeira_types::{ShardEpoch, ShardId};

/// Dedicated bundle used for the autoscaler singleton lease.
///
/// Uses the top of the u32 range to avoid collision with runtime placement
/// bundles which occupy the lower range.
pub const AUTOSCALER_LEASE_BUNDLE: ShardId = ShardId(u32::MAX);

pub struct AutoscalerLeader {
    lease_repo: Arc<dyn LeaseRepository>,
    node_id: String,
    node_endpoint: String,
    lease_bundle: ShardId,
    current_epoch: Option<ShardEpoch>,
    pub lease_duration: Duration,
    pub renewal_interval: Duration,
}

impl fmt::Debug for AutoscalerLeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AutoscalerLeader")
            .field("node_id", &self.node_id)
            .field("node_endpoint", &self.node_endpoint)
            .field("lease_bundle", &self.lease_bundle)
            .field("current_epoch", &self.current_epoch)
            .field("lease_duration", &self.lease_duration)
            .field("renewal_interval", &self.renewal_interval)
            .finish_non_exhaustive()
    }
}

impl AutoscalerLeader {
    pub fn new(
        lease_repo: Arc<dyn LeaseRepository>,
        node_id: String,
        node_endpoint: String,
        lease_duration: Duration,
        renewal_interval: Duration,
    ) -> Self {
        Self {
            lease_repo,
            node_id,
            node_endpoint,
            lease_bundle: AUTOSCALER_LEASE_BUNDLE,
            current_epoch: None,
            lease_duration,
            renewal_interval,
        }
    }

    pub fn with_bundle(mut self, lease_bundle: ShardId) -> Self {
        self.lease_bundle = lease_bundle;
        self
    }

    /// Attempt to acquire the singleton lease.
    ///
    /// Returns `true` if this node is now the leader. Safe to call repeatedly
    /// — if already the leader, this acts as a renewal.
    pub async fn try_acquire(&mut self) -> Result<bool> {
        let outcome = self
            .lease_repo
            .try_acquire_bundle(
                self.lease_bundle,
                self.node_id.clone(),
                self.node_endpoint.clone(),
            )
            .await?;
        match outcome {
            LeaseOutcome::Acquired { epoch } | LeaseOutcome::Renewed { epoch } => {
                self.current_epoch = Some(epoch);
                Ok(true)
            }
            LeaseOutcome::Rejected { .. } => {
                self.current_epoch = None;
                Ok(false)
            }
        }
    }

    /// Renew an existing lease. Returns `false` if the lease was lost (another
    /// node acquired it, or the epoch was superseded).
    pub async fn renew(&mut self) -> Result<bool> {
        let Some(epoch) = self.current_epoch else {
            return Ok(false);
        };
        let outcome = self
            .lease_repo
            .renew_bundle(
                self.lease_bundle,
                self.node_id.clone(),
                epoch,
                self.node_endpoint.clone(),
            )
            .await?;
        match outcome {
            LeaseOutcome::Renewed { epoch } | LeaseOutcome::Acquired { epoch } => {
                self.current_epoch = Some(epoch);
                Ok(true)
            }
            LeaseOutcome::Rejected { .. } => {
                self.current_epoch = None;
                Ok(false)
            }
        }
    }

    pub fn is_leader(&self) -> bool {
        self.current_epoch.is_some()
    }

    pub fn current_epoch(&self) -> Option<ShardEpoch> {
        self.current_epoch
    }
}
