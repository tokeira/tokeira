//! Background timer scanner and lane-routing helpers.
//!
//! Periodically scans durable storage for due timers and submits them
//! to the appropriate lane for processing. Also provides the deterministic
//! hash-based lane routing used across the runtime.

use std::{
    hash::{Hash, Hasher},
    sync::{Arc, RwLock},
};

use anyhow::Result;
use time::OffsetDateTime;
use tokeira_kernel::{Command, TimerDueRequest};
use tokeira_storage::{DueTimer, RunRepository};
use tokeira_types::{RunKey, ShardId};
use tokio_util::sync::CancellationToken;

use crate::{lane::LaneHandle, shard::ShardOwner};

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

pub(crate) fn lane_index_for(run_key: RunKey, lane_count: usize) -> usize {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    run_key.hash(&mut hasher);
    (hasher.finish() as usize) % lane_count.max(1)
}

pub(crate) fn pick_lane(
    lanes: &[LaneHandle],
    lane_count: usize,
    run_key: RunKey,
) -> &LaneHandle {
    debug_assert!(!lanes.is_empty());
    debug_assert_eq!(lanes.len(), lane_count.max(1));
    &lanes[lane_index_for(run_key, lane_count.max(1)) % lanes.len()]
}

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
            scan_due_timers_once_for_shard(&*repo, shard_id, &config, |due, fired_at| {
                let lane = pick_lane(&lanes, lane_count, due.run_key).clone();
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
