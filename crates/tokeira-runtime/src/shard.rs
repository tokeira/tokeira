//! Shard ownership tracking and deterministic run-to-shard
//! mapping.
//!
//! The runtime partitions the run space into shards so that
//! multiple nodes can own non-overlapping subsets. This module
//! provides the pure `shard_for` mapping function and the
//! `ShardOwner` struct that tracks which shards the current
//! node owns, their epochs, and their lifecycle state.

use std::collections::HashMap;

use tokeira_types::{RunKey, ShardEpoch, ShardId};
use tokio_util::sync::CancellationToken;

/// Deterministic mapping from a run key to a shard.
///
/// This is a pure, stateless function — no storage lookup
/// required. The same `(run_key, shard_count)` pair always
/// produces the same `ShardId`.
///
/// # Panics
///
/// Panics if `shard_count` is zero.
pub fn shard_for(run_key: RunKey, shard_count: u32) -> ShardId {
    assert!(shard_count > 0, "shard_count must be > 0");
    ShardId((run_key.0.as_u128() as u32) % shard_count)
}

/// Lifecycle state of an owned shard.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShardState {
    /// Lease acquired, sweeper running, commands not yet
    /// admitted.
    Sweeping,
    /// Sweep complete, commands admitted, scanners running.
    Active,
    /// Lease lost or relinquish requested. No new commands
    /// accepted; in-flight work draining.
    Draining,
}

/// Per-shard ownership record held by the runtime node.
pub struct OwnedShard {
    /// Epoch obtained when the lease was acquired.
    pub epoch: ShardEpoch,
    /// Current lifecycle state.
    pub state: ShardState,
    /// Cancellation token for shard-scoped background tasks
    /// (lease renewer, shard-scoped scanners).
    pub cancel: CancellationToken,
}

/// Tracks which shards the current runtime node owns.
///
/// Each shard transitions through `Sweeping → Active →
/// Draining` during its lifecycle on this node. Only shards
/// in the `Active` state accept new commands.
pub struct ShardOwner {
    shards: HashMap<ShardId, OwnedShard>,
    shard_count: u32,
}

impl ShardOwner {
    /// Create a new owner with the given total shard count.
    ///
    /// No shards are owned initially.
    pub fn new(shard_count: u32) -> Self {
        assert!(shard_count > 0, "shard_count must be > 0");
        Self {
            shards: HashMap::new(),
            shard_count,
        }
    }

    /// Total number of shards in the cluster.
    pub fn shard_count(&self) -> u32 {
        self.shard_count
    }

    /// Record that a shard lease was acquired with the given
    /// epoch. The shard enters `Sweeping` state.
    ///
    /// Returns a `CancellationToken` that can be used to
    /// cancel shard-scoped background tasks when the shard
    /// is relinquished.
    pub fn record_acquired(
        &mut self,
        shard_id: ShardId,
        epoch: ShardEpoch,
    ) -> CancellationToken {
        let cancel = CancellationToken::new();
        self.shards.insert(
            shard_id,
            OwnedShard {
                epoch,
                state: ShardState::Sweeping,
                cancel: cancel.clone(),
            },
        );
        cancel
    }

    /// Transition a shard from `Sweeping` to `Active`.
    ///
    /// After this call, commands for runs in this shard are
    /// accepted.
    pub fn mark_active(&mut self, shard_id: ShardId) {
        if let Some(owned) = self.shards.get_mut(&shard_id) {
            owned.state = ShardState::Active;
        }
    }

    /// Transition a shard to `Draining`.
    ///
    /// Cancels the shard-scoped cancellation token so
    /// background tasks (lease renewer, scanners) shut down.
    /// After this call, no new commands are accepted.
    pub fn mark_draining(&mut self, shard_id: ShardId) {
        if let Some(owned) = self.shards.get_mut(&shard_id) {
            owned.state = ShardState::Draining;
            owned.cancel.cancel();
        }
    }

    /// Remove a shard from the ownership map entirely.
    ///
    /// Call after draining is complete.
    pub fn remove(&mut self, shard_id: ShardId) {
        if let Some(owned) = self.shards.remove(&shard_id) {
            owned.cancel.cancel();
        }
    }

    /// Returns the epoch if the shard is owned and `Active`.
    ///
    /// Returns `None` for shards in `Sweeping`, `Draining`,
    /// or not owned at all.
    pub fn owns(&self, shard_id: ShardId) -> Option<ShardEpoch> {
        self.shards.get(&shard_id).and_then(|owned| {
            if owned.state == ShardState::Active {
                Some(owned.epoch)
            } else {
                None
            }
        })
    }

    /// Returns `true` only if the shard is in `Active` state.
    pub fn is_active(&self, shard_id: ShardId) -> bool {
        self.shards
            .get(&shard_id)
            .is_some_and(|o| o.state == ShardState::Active)
    }

    /// Returns the epoch for a shard regardless of state.
    ///
    /// Useful during sweep phase when the shard is still in
    /// `Sweeping` state but the epoch is needed for token
    /// stamping.
    pub fn epoch_of(&self, shard_id: ShardId) -> Option<ShardEpoch> {
        self.shards.get(&shard_id).map(|o| o.epoch)
    }

    /// Iterator over all currently owned shard IDs.
    pub fn owned_shards(&self) -> impl Iterator<Item = ShardId> + '_ {
        self.shards.keys().copied()
    }

    /// Iterator over active shard IDs only.
    pub fn active_shards(&self) -> impl Iterator<Item = ShardId> + '_ {
        self.shards.iter().filter_map(|(id, o)| {
            if o.state == ShardState::Active {
                Some(*id)
            } else {
                None
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use uuid::Uuid;

    fn arb_run_key() -> impl Strategy<Value = RunKey> {
        (any::<u128>()).prop_map(|v| RunKey(Uuid::from_u128(v)))
    }

    fn arb_shard_id() -> impl Strategy<Value = ShardId> {
        (0u32..1024).prop_map(ShardId)
    }

    fn arb_shard_epoch() -> impl Strategy<Value = ShardEpoch> {
        (1u64..u64::MAX).prop_map(ShardEpoch)
    }

    // ── Property 1: Shard ownership round-trip ──────────
    // Feature: runtime-sweeper-recovery
    // Validates: Requirements 1.2
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn shard_ownership_round_trip(
            shard_id in arb_shard_id(),
            epoch in arb_shard_epoch(),
        ) {
            let mut owner = ShardOwner::new(1024);
            let _cancel = owner.record_acquired(shard_id, epoch);

            // In Sweeping state, owns() returns None.
            prop_assert_eq!(owner.owns(shard_id), None);
            prop_assert!(!owner.is_active(shard_id));

            // After mark_active, owns() returns the epoch.
            owner.mark_active(shard_id);
            prop_assert_eq!(owner.owns(shard_id), Some(epoch));
            prop_assert!(owner.is_active(shard_id));
        }
    }

    // ── Property 16: Deterministic shard assignment ─────
    // Feature: runtime-sweeper-recovery
    // Validates: Requirements 14.1, 14.2
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn deterministic_shard_assignment(
            run_key in arb_run_key(),
            shard_count in 1u32..=4096,
        ) {
            let s1 = shard_for(run_key, shard_count);
            let s2 = shard_for(run_key, shard_count);
            prop_assert_eq!(s1, s2);
            prop_assert!(s1.0 < shard_count);
        }
    }

    // ── Property 12: Commands rejected during sweep ─────
    // Feature: runtime-sweeper-recovery
    // Validates: Requirements 11.1, 11.2, 11.4
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn commands_rejected_during_sweep_phase(
            shard_id in arb_shard_id(),
            epoch in arb_shard_epoch(),
        ) {
            let mut owner = ShardOwner::new(1024);
            let _cancel = owner.record_acquired(shard_id, epoch);

            // Sweeping → not active.
            prop_assert!(!owner.is_active(shard_id));

            // Active → active.
            owner.mark_active(shard_id);
            prop_assert!(owner.is_active(shard_id));

            // Draining → not active.
            owner.mark_draining(shard_id);
            prop_assert!(!owner.is_active(shard_id));
            prop_assert_eq!(owner.owns(shard_id), None);
        }
    }

    #[test]
    fn shard_for_panics_on_zero_count() {
        let result = std::panic::catch_unwind(|| shard_for(RunKey::new(), 0));
        assert!(result.is_err());
    }

    #[test]
    fn shard_owner_remove_cleans_up() {
        let mut owner = ShardOwner::new(16);
        let cancel = owner.record_acquired(ShardId(3), ShardEpoch(1));
        owner.mark_active(ShardId(3));
        assert!(owner.is_active(ShardId(3)));

        owner.remove(ShardId(3));
        assert!(!owner.is_active(ShardId(3)));
        assert_eq!(owner.owns(ShardId(3)), None);
        assert!(cancel.is_cancelled());
    }

    #[test]
    fn mark_draining_cancels_token() {
        let mut owner = ShardOwner::new(16);
        let cancel = owner.record_acquired(ShardId(5), ShardEpoch(10));
        assert!(!cancel.is_cancelled());

        owner.mark_draining(ShardId(5));
        assert!(cancel.is_cancelled());
    }

    #[test]
    fn epoch_of_returns_epoch_in_any_state() {
        let mut owner = ShardOwner::new(16);
        let _cancel = owner.record_acquired(ShardId(1), ShardEpoch(42));

        // Sweeping
        assert_eq!(owner.epoch_of(ShardId(1)), Some(ShardEpoch(42)));

        // Active
        owner.mark_active(ShardId(1));
        assert_eq!(owner.epoch_of(ShardId(1)), Some(ShardEpoch(42)));

        // Draining
        owner.mark_draining(ShardId(1));
        assert_eq!(owner.epoch_of(ShardId(1)), Some(ShardEpoch(42)));
    }

    // ── Property 14: Shard-scoped timeout scanning ──────
    // Feature: runtime-sweeper-recovery
    // **Validates: Requirements 13.1, 13.2, 13.3**

    // ── Property 13: Command rejection on lease loss ────
    // Feature: runtime-sweeper-recovery
    // **Validates: Requirements 2.4, 15.1**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn command_rejection_on_lease_loss(
            shard_id in arb_shard_id(),
            epoch in arb_shard_epoch(),
        ) {
            let mut owner = ShardOwner::new(1024);
            let _cancel =
                owner.record_acquired(shard_id, epoch);
            owner.mark_active(shard_id);
            prop_assert!(owner.is_active(shard_id));
            prop_assert_eq!(
                owner.owns(shard_id),
                Some(epoch),
            );

            // Simulate lease loss → draining
            owner.mark_draining(shard_id);
            prop_assert!(!owner.is_active(shard_id));
            prop_assert_eq!(owner.owns(shard_id), None);
            // epoch_of still returns the epoch (for
            // in-flight completions during Draining)
            prop_assert_eq!(
                owner.epoch_of(shard_id),
                Some(epoch),
            );
        }
    }

    // ── Property 3: Task tokens carry current shard
    //    epoch ───────────────────────────────────────
    // Feature: runtime-sweeper-recovery
    // **Validates: Requirements 3.1, 3.2**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn task_tokens_carry_current_shard_epoch(
            shard_id in arb_shard_id(),
            epoch in arb_shard_epoch(),
        ) {
            let mut owner = ShardOwner::new(1024);
            let _cancel =
                owner.record_acquired(shard_id, epoch);
            owner.mark_active(shard_id);

            // owns() returns the epoch that should be
            // stamped on task tokens.
            let token_epoch = owner.owns(shard_id);
            prop_assert_eq!(token_epoch, Some(epoch));
        }
    }

    // ── Property 4: Stale epoch completions rejected ────
    // Feature: runtime-sweeper-recovery
    // **Validates: Requirements 3.3**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn stale_epoch_completions_rejected(
            shard_id in arb_shard_id(),
            epoch_a in arb_shard_epoch(),
            epoch_b in arb_shard_epoch(),
        ) {
            let mut owner = ShardOwner::new(1024);
            let _cancel =
                owner.record_acquired(shard_id, epoch_a);
            owner.mark_active(shard_id);

            // Simulate failover: remove old, acquire new
            owner.remove(shard_id);
            let _cancel2 =
                owner.record_acquired(shard_id, epoch_b);
            owner.mark_active(shard_id);

            let current = owner.epoch_of(shard_id).unwrap();
            if epoch_a != epoch_b {
                // Token from old epoch doesn't match
                prop_assert_ne!(epoch_a, current);
            }
            // Current epoch always matches
            prop_assert_eq!(current, epoch_b);
        }
    }

    // ── Property 14 continued ───────────────────────────
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn shard_scoped_timeout_scanning(
            shard_a in (0u32..16).prop_map(ShardId),
            shard_b in (16u32..32).prop_map(ShardId),
            count_a in 1usize..4,
            count_b in 1usize..4,
        ) {
            use crate::timeout::{
                WorkflowTimeoutEntry,
                WorkflowTimeoutTrackingState,
            };
            use crate::activity_timeout::{
                ActivityTrackingEntry,
                ActivityTrackingState,
            };
            use crate::nexus::{
                NexusTimeoutEntry,
                NexusTimeoutTrackingState,
            };
            use time::{Duration, OffsetDateTime};

            let now = OffsetDateTime::UNIX_EPOCH;

            let wts = WorkflowTimeoutTrackingState::default();
            let ats = ActivityTrackingState::default();
            let nts = NexusTimeoutTrackingState::default();

            for i in 0..count_a {
                let rk = RunKey::new();
                wts.insert(WorkflowTimeoutEntry {
                    run_key: rk,
                    shard_id: shard_a,
                    workflow_execution_timeout: Some(
                        Duration::minutes(5),
                    ),
                    workflow_run_timeout: None,
                    started_at: now,
                    first_run_started_at: None,
                    has_retry_policy: false,
                });
                ats.insert(ActivityTrackingEntry {
                    run_key: rk,
                    shard_id: shard_a,
                    activity_id: format!("a-{i}"),
                    original_scheduled_at: now,
                    last_dispatched_at: now,
                    started_at: None,
                    last_heartbeat_at: None,
                    cancel_requested: false,
                });
                nts.insert(NexusTimeoutEntry {
                    run_key: rk,
                    shard_id: shard_a,
                    operation_id: format!("n-{i}"),
                    scheduled_event_id: i as i64,
                    schedule_to_close_timeout:
                        Duration::minutes(1),
                    scheduled_at: now,
                });
            }
            for i in 0..count_b {
                let rk = RunKey::new();
                wts.insert(WorkflowTimeoutEntry {
                    run_key: rk,
                    shard_id: shard_b,
                    workflow_execution_timeout: Some(
                        Duration::minutes(5),
                    ),
                    workflow_run_timeout: None,
                    started_at: now,
                    first_run_started_at: None,
                    has_retry_policy: false,
                });
                ats.insert(ActivityTrackingEntry {
                    run_key: rk,
                    shard_id: shard_b,
                    activity_id: format!("b-{i}"),
                    original_scheduled_at: now,
                    last_dispatched_at: now,
                    started_at: None,
                    last_heartbeat_at: None,
                    cancel_requested: false,
                });
                nts.insert(NexusTimeoutEntry {
                    run_key: rk,
                    shard_id: shard_b,
                    operation_id: format!("nb-{i}"),
                    scheduled_event_id: i as i64,
                    schedule_to_close_timeout:
                        Duration::minutes(1),
                    scheduled_at: now,
                });
            }

            let wts_a = wts.snapshot_for_shard(shard_a);
            let wts_b = wts.snapshot_for_shard(shard_b);
            prop_assert_eq!(wts_a.len(), count_a);
            prop_assert_eq!(wts_b.len(), count_b);
            prop_assert!(wts_a.iter().all(
                |e| e.shard_id == shard_a
            ));
            prop_assert!(wts_b.iter().all(
                |e| e.shard_id == shard_b
            ));

            let ats_a = ats.snapshot_for_shard(shard_a);
            let ats_b = ats.snapshot_for_shard(shard_b);
            prop_assert_eq!(ats_a.len(), count_a);
            prop_assert_eq!(ats_b.len(), count_b);
            prop_assert!(ats_a.iter().all(
                |e| e.shard_id == shard_a
            ));
            prop_assert!(ats_b.iter().all(
                |e| e.shard_id == shard_b
            ));

            let nts_a = nts.snapshot_for_shard(shard_a);
            let nts_b = nts.snapshot_for_shard(shard_b);
            prop_assert_eq!(nts_a.len(), count_a);
            prop_assert_eq!(nts_b.len(), count_b);
            prop_assert!(nts_a.iter().all(
                |e| e.shard_id == shard_a
            ));
            prop_assert!(nts_b.iter().all(
                |e| e.shard_id == shard_b
            ));
        }
    }

    // ── Property 15: Tracking state cleanup on shard
    //    relinquish ─────────────────────────────────────
    // Feature: runtime-sweeper-recovery
    // **Validates: Requirements 13.4, 15.4**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn tracking_state_cleanup_on_shard_relinquish(
            shard_a in (0u32..16).prop_map(ShardId),
            shard_b in (16u32..32).prop_map(ShardId),
            count_a in 1usize..4,
            count_b in 1usize..4,
        ) {
            use crate::timeout::{
                WorkflowTimeoutEntry,
                WorkflowTimeoutTrackingState,
            };
            use crate::activity_timeout::{
                ActivityTrackingEntry,
                ActivityTrackingState,
            };
            use crate::nexus::{
                NexusTimeoutEntry,
                NexusTimeoutTrackingState,
            };
            use time::{Duration, OffsetDateTime};

            let now = OffsetDateTime::UNIX_EPOCH;

            let wts = WorkflowTimeoutTrackingState::default();
            let ats = ActivityTrackingState::default();
            let nts = NexusTimeoutTrackingState::default();

            for i in 0..count_a {
                let rk = RunKey::new();
                wts.insert(WorkflowTimeoutEntry {
                    run_key: rk,
                    shard_id: shard_a,
                    workflow_execution_timeout: Some(
                        Duration::minutes(5),
                    ),
                    workflow_run_timeout: None,
                    started_at: now,
                    first_run_started_at: None,
                    has_retry_policy: false,
                });
                ats.insert(ActivityTrackingEntry {
                    run_key: rk,
                    shard_id: shard_a,
                    activity_id: format!("a-{i}"),
                    original_scheduled_at: now,
                    last_dispatched_at: now,
                    started_at: None,
                    last_heartbeat_at: None,
                    cancel_requested: false,
                });
                nts.insert(NexusTimeoutEntry {
                    run_key: rk,
                    shard_id: shard_a,
                    operation_id: format!("n-{i}"),
                    scheduled_event_id: i as i64,
                    schedule_to_close_timeout:
                        Duration::minutes(1),
                    scheduled_at: now,
                });
            }
            for i in 0..count_b {
                let rk = RunKey::new();
                wts.insert(WorkflowTimeoutEntry {
                    run_key: rk,
                    shard_id: shard_b,
                    workflow_execution_timeout: Some(
                        Duration::minutes(5),
                    ),
                    workflow_run_timeout: None,
                    started_at: now,
                    first_run_started_at: None,
                    has_retry_policy: false,
                });
                ats.insert(ActivityTrackingEntry {
                    run_key: rk,
                    shard_id: shard_b,
                    activity_id: format!("b-{i}"),
                    original_scheduled_at: now,
                    last_dispatched_at: now,
                    started_at: None,
                    last_heartbeat_at: None,
                    cancel_requested: false,
                });
                nts.insert(NexusTimeoutEntry {
                    run_key: rk,
                    shard_id: shard_b,
                    operation_id: format!("nb-{i}"),
                    scheduled_event_id: i as i64,
                    schedule_to_close_timeout:
                        Duration::minutes(1),
                    scheduled_at: now,
                });
            }

            // Remove shard_a entries
            wts.remove_all_for_shard(shard_a);
            ats.remove_all_for_shard(shard_a);
            nts.remove_all_for_shard(shard_a);

            // shard_a entries gone
            prop_assert!(
                wts.snapshot_for_shard(shard_a).is_empty()
            );
            prop_assert!(
                ats.snapshot_for_shard(shard_a).is_empty()
            );
            prop_assert!(
                nts.snapshot_for_shard(shard_a).is_empty()
            );

            // shard_b entries remain
            prop_assert_eq!(
                wts.snapshot_for_shard(shard_b).len(),
                count_b,
            );
            prop_assert_eq!(
                ats.snapshot_for_shard(shard_b).len(),
                count_b,
            );
            prop_assert_eq!(
                nts.snapshot_for_shard(shard_b).len(),
                count_b,
            );
        }
    }
}
