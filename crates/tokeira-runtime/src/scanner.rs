//! Background timer scanner and lane-routing helpers.
//!
//! Two responsibilities live here. First, the background timer scanner:
//! periodically reads *durable* storage for timers that are due and submits a
//! `TimerDue` command to the owning lane. The scanner holds no authoritative
//! state of its own — storage is the source of truth for which timers are due —
//! so a missed or duplicated tick is recovered on the next scan and is safe after
//! crash or failover. Second, the deterministic run-to-lane routing
//! (`lane_index_for_run_key`, `pick_lane_for_run_key`) used by every command
//! submission path in the runtime, not just timers.
//!
//! Routing invariant: every command for a given `run_key` must land on the same
//! lane, because a lane serializes the commands for the runs it owns and that
//! serialization is what keeps per-run transitions ordered. The mapping is a pure
//! function of the run key, so all producers (poll handlers, sweepers, the
//! timeout scanners) agree on the lane without coordination.

use std::sync::{Arc, RwLock};

use anyhow::Result;
use time::OffsetDateTime;
use tokeira_kernel::{
    Command, TimerDueRequest, WORKFLOW_START_DELAY_TIMER_ID, WorkflowStartDelayElapsedRequest,
};
use tokeira_storage::{DueTimer, RunRepository};
use tokeira_types::{RunKey, ShardId, dsql_spread_uuid};
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

// Shard-scoped routing is retained for sweep enumeration and regression tests;
// all command submission paths route by RunKey through `lane_index_for_run_key`.
#[allow(dead_code)]
pub(crate) fn lane_index_for(shard_id: ShardId, lane_count: usize) -> usize {
    (shard_id.0 as usize) % lane_count.max(1)
}

// Route by run key, not shard, so a run always maps to one lane regardless of how
// shards are distributed across lanes. `dsql_spread_uuid` first diffuses the key
// so adjacent run keys do not collide onto the same lane under the modulo.
pub(crate) fn lane_index_for_run_key(run_key: RunKey, lane_count: usize) -> usize {
    let lane_key = dsql_spread_uuid(&[b"lane", run_key.0.as_bytes()]);
    (lane_key.as_u128() as usize) % lane_count.max(1)
}

pub(crate) fn pick_lane_for_run_key(
    lanes: &[LaneHandle],
    lane_count: usize,
    run_key: RunKey,
) -> &LaneHandle {
    debug_assert!(!lanes.is_empty());
    debug_assert_eq!(lanes.len(), lane_count.max(1));
    &lanes[lane_index_for_run_key(run_key, lane_count.max(1)) % lanes.len()]
}

fn command_for_due_timer(due: DueTimer, fired_at: OffsetDateTime) -> Command {
    if due.timer_id == WORKFLOW_START_DELAY_TIMER_ID {
        Command::WorkflowStartDelayElapsed(WorkflowStartDelayElapsedRequest { fired_at })
    } else {
        Command::TimerDue(TimerDueRequest {
            timer_id: due.timer_id,
            fired_at,
        })
    }
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

/// Background loop: every `scan_interval`, inject due timers for each shard this
/// node currently owns.
///
/// The active-shard set is re-read each tick so ownership changes are picked up
/// without restarting the task, and scanning is scoped per shard so a node only
/// fires timers for runs it owns. Each due timer routes by `run_key` to the
/// owning lane, keeping per-run command order intact.
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

        let active_shards: Vec<_> = shard_owner
            .read()
            .expect("shard_owner lock poisoned")
            .active_shards()
            .collect();
        for shard_id in active_shards {
            runtime_metrics::record_scanner_tick("timer", shard_id.0);
            scan_due_timers_once_for_shard(&*repo, shard_id, &config, |due, fired_at| {
                runtime_metrics::record_scanner_dispatched("timer", shard_id.0);
                let lane = pick_lane_for_run_key(&lanes, lane_count, due.run_key).clone();
                async move {
                    let run_key = due.run_key;
                    lane.submit(run_key, command_for_due_timer(due, fired_at))
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
    use std::collections::BTreeSet;

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

    #[test]
    fn run_key_routing_reaches_all_lanes_for_fixed_sample() {
        let lane_count = 64;
        let lanes = (0..10_000u128)
            .map(|value| lane_index_for_run_key(RunKey(Uuid::from_u128(value)), lane_count))
            .collect::<BTreeSet<_>>();

        assert_eq!(lanes.len(), lane_count);
    }

    #[test]
    fn delayed_start_timer_routes_to_internal_start_delay_command() {
        let fired_at = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        let run_key = RunKey::new();
        let delayed = DueTimer {
            run_key,
            timer_id: WORKFLOW_START_DELAY_TIMER_ID.to_string(),
        };
        let user_timer = DueTimer {
            run_key,
            timer_id: "timer-1".to_string(),
        };

        assert!(matches!(
            command_for_due_timer(delayed, fired_at),
            Command::WorkflowStartDelayElapsed(WorkflowStartDelayElapsedRequest {
                fired_at: observed
            }) if observed == fired_at
        ));
        assert!(matches!(
            command_for_due_timer(user_timer, fired_at),
            Command::TimerDue(TimerDueRequest {
                timer_id,
                fired_at: observed
            }) if timer_id == "timer-1" && observed == fired_at
        ));
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
