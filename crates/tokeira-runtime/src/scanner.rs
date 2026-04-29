//! Background timer scanner and lane-routing helpers.
//!
//! Periodically scans durable storage for due timers and submits them
//! to the appropriate lane for processing. Also provides the deterministic
//! shard-based lane routing used across the runtime.

use std::sync::{Arc, RwLock};

use anyhow::Result;
use time::OffsetDateTime;
use tokeira_kernel::{Command, TimerDueRequest};
use tokeira_storage::{DueTimer, RunRepository};
use tokeira_types::ShardId;
use tokio_util::sync::CancellationToken;

use crate::{lane::LaneHandle, metrics as runtime_metrics, shard::ShardOwner};

/// Configuration knobs for the background timer scanner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimerScannerConfig {
    /// Delay between storage scans for due timers.
    pub scan_interval: tokio::time::Duration,
    /// Maximum timers loaded from storage per scan cycle.
    pub max_timers_per_scan: usize,
}

impl Default for TimerScannerConfig {
    fn default() -> Self {
        Self {
            scan_interval: tokio::time::Duration::from_millis(200),
            max_timers_per_scan: 100,
        }
    }
}

pub(crate) fn lane_index_for(shard_id: ShardId, lane_count: usize) -> usize {
    (shard_id.0 as usize) % lane_count.max(1)
}

pub(crate) fn pick_lane(
    lanes: &[LaneHandle],
    lane_count: usize,
    shard_id: ShardId,
) -> &LaneHandle {
    debug_assert!(!lanes.is_empty());
    debug_assert_eq!(lanes.len(), lane_count.max(1));
    &lanes[lane_index_for(shard_id, lane_count.max(1)) % lanes.len()]
}

#[allow(dead_code)]
pub(crate) async fn scan_due_timers_once<R, F, Fut>(
    repo: &R,
    config: &TimerScannerConfig,
    mut submit_due_timer: F,
) where
    R: RunRepository + ?Sized,
    F: FnMut(DueTimer, OffsetDateTime) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let fired_at = OffsetDateTime::now_utc();
    let due_timers = match repo
        .list_due_timers(fired_at, config.max_timers_per_scan)
        .await
    {
        Ok(due_timers) => due_timers,
        Err(error) => {
            tracing::warn!(?error, "timer scanner failed to list due timers");
            return;
        }
    };

    for due in due_timers {
        if let Err(error) = submit_due_timer(due.clone(), fired_at).await {
            let message = error.to_string();
            if message.contains("kernel rejected") {
                tracing::debug!(
                    ?error,
                    run_key = ?due.run_key,
                    timer_id = due.timer_id,
                    "timer scanner due timer rejected by kernel"
                );
            } else {
                tracing::warn!(
                    ?error,
                    run_key = ?due.run_key,
                    timer_id = due.timer_id,
                    "timer scanner failed to submit due timer"
                );
            }
        }
    }
}

pub(crate) async fn scan_due_timers_once_for_shard<R, F, Fut>(
    repo: &R,
    shard_id: ShardId,
    config: &TimerScannerConfig,
    mut submit_due_timer: F,
) where
    R: RunRepository + ?Sized,
    F: FnMut(DueTimer, OffsetDateTime) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let fired_at = OffsetDateTime::now_utc();
    let due_timers = match repo
        .list_due_timers_for_shard(shard_id, fired_at, config.max_timers_per_scan)
        .await
    {
        Ok(due_timers) => due_timers,
        Err(error) => {
            tracing::warn!(
                ?error,
                shard_id = ?shard_id,
                "timer scanner failed to list due timers for shard"
            );
            return;
        }
    };

    for due in due_timers {
        if let Err(error) = submit_due_timer(due.clone(), fired_at).await {
            let message = error.to_string();
            if message.contains("kernel rejected") {
                tracing::debug!(
                    ?error,
                    shard_id = ?shard_id,
                    run_key = ?due.run_key,
                    timer_id = due.timer_id,
                    "timer scanner due timer rejected by kernel"
                );
            } else {
                tracing::warn!(
                    ?error,
                    shard_id = ?shard_id,
                    run_key = ?due.run_key,
                    timer_id = due.timer_id,
                    "timer scanner failed to submit due timer"
                );
            }
        }
    }
}

pub(crate) async fn run_timer_scanner<R>(
    repo: Arc<R>,
    lanes: Vec<LaneHandle>,
    lane_count: usize,
    shard_owner: Arc<RwLock<ShardOwner>>,
    config: TimerScannerConfig,
    cancel: CancellationToken,
) where
    R: RunRepository + 'static,
{
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(config.scan_interval) => {}
        }

        let active_shards: Vec<_> = shard_owner.read().unwrap().active_shards().collect();
        for shard_id in active_shards {
            runtime_metrics::record_scanner_tick("timer", shard_id.0);
            scan_due_timers_once_for_shard(&*repo, shard_id, &config, |due, fired_at| {
                runtime_metrics::record_scanner_dispatched("timer", shard_id.0);
                let lane = pick_lane(&lanes, lane_count, shard_id).clone();
                async move {
                    lane.submit(
                        due.run_key,
                        Command::TimerDue(TimerDueRequest {
                            timer_id: due.timer_id,
                            fired_at,
                        }),
                    )
                    .await
                    .map(|_| ())
                }
            })
            .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use tokeira_types::RunKey;
    use uuid::Uuid;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        // Feature: shard-aware-lane-routing, Property 1: Shard-to-lane computation correctness
        #[test]
        fn property_shard_to_lane_computation(shard in any::<u32>(), lane_count in 1usize..64usize) {
            prop_assert_eq!(
                lane_index_for(ShardId(shard), lane_count),
                shard as usize % lane_count
            );
        }

        // Feature: shard-aware-lane-routing, Property 3: Lane index bounds
        #[test]
        fn property_lane_index_bounds(shard in any::<u32>(), lane_count in 1usize..64usize) {
            let index = lane_index_for(ShardId(shard), lane_count);
            prop_assert!(index < lane_count);
        }
    }

    #[test]
    fn test_lane_index_for_basic() {
        assert_eq!(lane_index_for(ShardId(0), 4), 0);
        assert_eq!(lane_index_for(ShardId(7), 4), 3);
    }

    #[test]
    fn test_lane_count_one_always_zero() {
        assert_eq!(lane_index_for(ShardId(u32::MAX), 1), 0);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        // Feature: shard-aware-lane-routing, Property 2: Shard-lane affinity
        #[test]
        fn property_shard_lane_affinity(run_a in any::<u128>(), run_b in any::<u128>(), shard_count in 1u32..64u32, lane_count in 1usize..64usize) {
            let a = RunKey(Uuid::from_u128(run_a));
            let b = RunKey(Uuid::from_u128(run_b));
            let shard_a = crate::shard::shard_for(a, shard_count);
            let shard_b = crate::shard::shard_for(b, shard_count);

            if shard_a == shard_b {
                prop_assert_eq!(
                    lane_index_for(shard_a, lane_count),
                    lane_index_for(shard_b, lane_count)
                );
            }
        }

        // Feature: shard-aware-lane-routing, Property 4: End-to-end routing determinism
        #[test]
        fn property_end_to_end_routing_determinism(run in any::<u128>(), shard_count in 1u32..64u32, lane_count in 1usize..64usize) {
            let run_key = RunKey(Uuid::from_u128(run));
            let shard_id = crate::shard::shard_for(run_key, shard_count);
            prop_assert_eq!(
                lane_index_for(shard_id, lane_count),
                lane_index_for(shard_id, lane_count)
            );
        }

        // Feature: shard-aware-lane-routing, Property 5: RunKey-to-lane affinity through shard derivation
        #[test]
        fn property_runkey_to_lane_affinity(run_a in any::<u128>(), run_b in any::<u128>(), shard_count in 1u32..64u32, lane_count in 1usize..64usize) {
            let a = RunKey(Uuid::from_u128(run_a));
            let b = RunKey(Uuid::from_u128(run_b));
            let shard_a = crate::shard::shard_for(a, shard_count);
            let shard_b = crate::shard::shard_for(b, shard_count);

            if shard_a == shard_b {
                prop_assert_eq!(
                    lane_index_for(shard_a, lane_count),
                    lane_index_for(shard_b, lane_count)
                );
            }
        }
    }
}
