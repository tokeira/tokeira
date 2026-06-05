//! Activity heartbeat tracking and timeout scanning.
//!
//! Owns the in-memory set of open activities and the background scanner that
//! resolves any whose schedule-to-close, schedule-to-start, start-to-close, or
//! heartbeat deadline passes. This is what reclaims an activity when a worker
//! stops heartbeating, never starts it, or runs it past its limit.
//!
//! The tracking state is volatile and derived: the durable transition log is
//! authoritative for which activities are open and what their timeouts are.
//! This map is the scan-time cache, rebuilt from durable history by
//! [`crate::recovery::sweep_shard`] on shard takeover — losing it costs a
//! rebuild, never correctness. Heartbeat timestamps are intentionally *not*
//! durable: they are reset to `None` on rebuild and on retry, so after a failover
//! the heartbeat clock restarts from the activity's start rather than firing
//! spuriously. Entries are keyed by `(RunKey, activity_id)` and tagged with their
//! `ShardId` so a handoff evicts exactly the activities this node no longer owns.
//!
//! Unlike the workflow and WFT scanners, this scanner reloads each run from the
//! repository before firing, because the live `ActivityState` (current timeouts,
//! attempt, whether the activity still exists) is the authority for the decision;
//! the tracking entry only supplies the timing anchors the durable state does not
//! retain.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use time::OffsetDateTime;
use tokeira_kernel::{
    ActivityResolution, ActivityResolvedRequest, ActivityState, Command, LoadedRun,
};
use tokeira_observability::OutcomeLabel;
use tokeira_storage::RunRepository;
use tokeira_types::{RunKey, ShardId};
use tokio_util::sync::CancellationToken;

use crate::{
    lane::LaneHandle, metrics as runtime_metrics, scanner::pick_lane_for_run_key, shard::ShardOwner,
};

/// One open activity being watched for timeouts.
///
/// Holds the timing anchors the scanner needs but durable state does not retain.
/// `started_at` and `last_heartbeat_at` are `None` until the activity starts /
/// first heartbeats; both are deliberately volatile (see module docs).
#[derive(Clone, Debug, PartialEq)]
pub struct ActivityTrackingEntry {
    pub run_key: RunKey,
    /// Owning shard, so a handoff can evict this node's entries selectively.
    pub shard_id: ShardId,
    pub activity_id: String,
    /// First schedule time — anchor for the schedule-to-close deadline, held fixed
    /// across retries.
    pub original_scheduled_at: OffsetDateTime,
    /// Most recent dispatch time — anchor for schedule-to-start, advanced on each
    /// retry.
    pub last_dispatched_at: OffsetDateTime,
    pub started_at: Option<OffsetDateTime>,
    pub last_heartbeat_at: Option<OffsetDateTime>,
    /// Set when a cancel has been requested, surfaced to the worker on its next
    /// heartbeat.
    pub cancel_requested: bool,
}

/// Shared in-memory tracking state for open activities.
///
/// Cloning shares the underlying map; lanes, the scanner, and shard recovery all
/// see one set. Keyed by `(RunKey, activity_id)` since a run may have many
/// activities open at once.
#[derive(Clone, Default)]
pub struct ActivityTrackingState {
    inner: Arc<Mutex<HashMap<(RunKey, String), ActivityTrackingEntry>>>,
}

impl ActivityTrackingState {
    /// Insert a fully-formed entry, replacing any existing one for the same key.
    /// Used by shard recovery to rebuild tracking from durable state.
    pub fn insert(&self, entry: ActivityTrackingEntry) {
        self.inner
            .lock()
            .unwrap()
            .insert((entry.run_key, entry.activity_id.clone()), entry);
    }

    /// Begin tracking a freshly scheduled activity, anchoring both the
    /// schedule-to-close and schedule-to-start clocks at `now`.
    pub fn record_scheduled(
        &self,
        run_key: RunKey,
        shard_id: ShardId,
        activity_id: impl Into<String>,
        now: OffsetDateTime,
    ) {
        let activity_id = activity_id.into();
        self.inner.lock().unwrap().insert(
            (run_key, activity_id.clone()),
            ActivityTrackingEntry {
                run_key,
                shard_id,
                activity_id,
                original_scheduled_at: now,
                last_dispatched_at: now,
                started_at: None,
                last_heartbeat_at: None,
                cancel_requested: false,
            },
        );
    }

    /// Record a redispatch on retry: advance the schedule-to-start anchor and
    /// clear the per-attempt start/heartbeat clocks, while leaving
    /// `original_scheduled_at` (the schedule-to-close anchor) untouched so the
    /// overall deadline still spans all attempts.
    pub fn record_retry(&self, run_key: RunKey, activity_id: &str, now: OffsetDateTime) {
        if let Some(entry) = self
            .inner
            .lock()
            .unwrap()
            .get_mut(&(run_key, activity_id.to_string()))
        {
            entry.last_dispatched_at = now;
            entry.started_at = None;
            entry.last_heartbeat_at = None;
        }
    }

    /// Record that a worker started the activity, anchoring the start-to-close and
    /// heartbeat clocks. Heartbeat is reset to `None` so the first interval is
    /// measured from start.
    pub fn record_started(&self, run_key: RunKey, activity_id: &str, now: OffsetDateTime) {
        if let Some(entry) = self
            .inner
            .lock()
            .unwrap()
            .get_mut(&(run_key, activity_id.to_string()))
        {
            entry.started_at = Some(now);
            entry.last_heartbeat_at = None;
        }
    }

    /// Record a heartbeat, resetting the heartbeat clock. Returns whether a cancel
    /// has been requested (so the caller can relay it to the worker), or `None` if
    /// the activity is no longer tracked.
    pub fn record_heartbeat(
        &self,
        run_key: RunKey,
        activity_id: &str,
        now: OffsetDateTime,
    ) -> Option<bool> {
        let mut guard = self.inner.lock().unwrap();
        let entry = guard.get_mut(&(run_key, activity_id.to_string()))?;
        entry.last_heartbeat_at = Some(now);
        Some(entry.cancel_requested)
    }

    /// Mark that a cancel has been requested for the activity; relayed to the
    /// worker on its next heartbeat.
    pub fn mark_cancel_requested(&self, run_key: RunKey, activity_id: &str) {
        if let Some(entry) = self
            .inner
            .lock()
            .unwrap()
            .get_mut(&(run_key, activity_id.to_string()))
        {
            entry.cancel_requested = true;
        }
    }

    /// Whether a cancel has been requested, or `None` if the activity is untracked.
    pub fn is_cancel_requested(&self, run_key: RunKey, activity_id: &str) -> Option<bool> {
        self.inner
            .lock()
            .unwrap()
            .get(&(run_key, activity_id.to_string()))
            .map(|entry| entry.cancel_requested)
    }

    /// Stop tracking an activity, called when it resolves so it is never scanned
    /// again.
    pub fn remove(&self, run_key: RunKey, activity_id: &str) {
        self.inner
            .lock()
            .unwrap()
            .remove(&(run_key, activity_id.to_string()));
    }

    /// Drop every entry for a shard on handoff; the new owner rebuilds them from
    /// durable state during its sweep.
    pub fn remove_all_for_shard(&self, shard_id: ShardId) {
        self.inner
            .lock()
            .unwrap()
            .retain(|_, entry| entry.shard_id != shard_id);
    }

    /// Snapshot all tracked entries; the scanner evaluates the copy without
    /// holding the lock.
    pub fn snapshot(&self) -> Vec<ActivityTrackingEntry> {
        self.inner.lock().unwrap().values().cloned().collect()
    }

    /// Snapshot only the entries owned by `shard_id`, used by the per-shard scan.
    pub fn snapshot_for_shard(&self, shard_id: ShardId) -> Vec<ActivityTrackingEntry> {
        self.inner
            .lock()
            .unwrap()
            .values()
            .filter(|entry| entry.shard_id == shard_id)
            .cloned()
            .collect()
    }
}

/// Which activity deadline was exceeded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TimeoutViolation {
    ScheduleToClose,
    ScheduleToStart,
    StartToClose,
    Heartbeat,
}

/// Tuning for the activity timeout scanner loop.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivityTimeoutScannerConfig {
    /// Delay between scans of the tracking set.
    pub scan_interval: tokio::time::Duration,
    /// Upper bound on timeouts submitted per scan, bounding the work one tick can
    /// do when many activities expire together.
    pub max_timeouts_per_scan: usize,
}

impl Default for ActivityTimeoutScannerConfig {
    fn default() -> Self {
        Self {
            scan_interval: tokio::time::Duration::from_secs(1),
            max_timeouts_per_scan: 100,
        }
    }
}

/// Decide which activity deadline, if any, `entry` has breached at `now`, given
/// the live `activity` state for the configured timeouts.
///
/// Precedence is deliberate and matches the activity lifecycle:
/// schedule-to-close bounds the whole activity and so is checked first; then, if
/// the activity has started, heartbeat is checked before start-to-close (a
/// heartbeat lapse is the tighter, more specific failure); if it has *not* started
/// yet, only schedule-to-start applies. The heartbeat clock falls back to
/// `started_at` when no heartbeat has been received, so the first interval is
/// measured from start. A zero timeout is treated as immediately due once its
/// anchor is reached rather than as "no timeout".
pub fn evaluate_activity_timeout(
    entry: &ActivityTrackingEntry,
    activity: &ActivityState,
    now: OffsetDateTime,
) -> Option<TimeoutViolation> {
    if let Some(timeout) = activity.schedule_to_close_timeout
        && (now - entry.original_scheduled_at > timeout
            || (timeout.is_zero() && now >= entry.original_scheduled_at))
    {
        return Some(TimeoutViolation::ScheduleToClose);
    }

    if let Some(started_at) = entry.started_at {
        if let Some(timeout) = activity.heartbeat_timeout {
            let heartbeat_at = entry.last_heartbeat_at.unwrap_or(started_at);
            if now - heartbeat_at > timeout || (timeout.is_zero() && now >= heartbeat_at) {
                return Some(TimeoutViolation::Heartbeat);
            }
        }

        if let Some(timeout) = activity.start_to_close_timeout
            && (now - started_at > timeout || (timeout.is_zero() && now >= started_at))
        {
            return Some(TimeoutViolation::StartToClose);
        }
    } else if let Some(timeout) = activity.schedule_to_start_timeout
        && (now - entry.last_dispatched_at > timeout
            || (timeout.is_zero() && now >= entry.last_dispatched_at))
    {
        return Some(TimeoutViolation::ScheduleToStart);
    }

    None
}

/// Evaluate the tracked activities once and resolve any that have timed out,
/// capped at `max_timeouts_per_scan`.
///
/// Each entry's run is reloaded from `repo` because the live `ActivityState` is
/// the authority for the current timeouts and for whether the activity still
/// exists; the tracking entry only supplies the timing anchors. An entry whose
/// run is absent, or whose activity is no longer present in the loaded state, is
/// dropped (it resolved by another path); a load error is transient and leaves
/// the entry for the next scan.
pub(crate) async fn scan_activity_timeouts_once<R>(
    repo: &R,
    tracking: &ActivityTrackingState,
    shard_id: Option<ShardId>,
    lanes: &[LaneHandle],
    lane_count: usize,
    config: &ActivityTimeoutScannerConfig,
) where
    R: RunRepository + 'static,
{
    let now = OffsetDateTime::now_utc();
    let mut submitted = 0usize;

    let entries = match shard_id {
        Some(shard_id) => tracking.snapshot_for_shard(shard_id),
        None => tracking.snapshot(),
    };

    for entry in entries {
        if submitted >= config.max_timeouts_per_scan {
            break;
        }

        let state = match repo.load_run(entry.run_key).await {
            Ok(LoadedRun::Existing(state)) => state,
            Ok(LoadedRun::Absent) => {
                tracking.remove(entry.run_key, &entry.activity_id);
                continue;
            }
            Err(error) => {
                tracing::warn!(
                    ?error,
                    run_key = ?entry.run_key,
                    activity_id = entry.activity_id,
                    "activity timeout scanner failed to load run"
                );
                continue;
            }
        };

        let Some(activity) = state.activities.get(&entry.activity_id).cloned() else {
            tracking.remove(entry.run_key, &entry.activity_id);
            continue;
        };

        let Some(violation) = evaluate_activity_timeout(&entry, &activity, now) else {
            continue;
        };

        let lane = pick_lane_for_run_key(lanes, lane_count, entry.run_key).clone();
        let result = lane
            .submit(
                entry.run_key,
                Command::ActivityResolved(ActivityResolvedRequest {
                    activity_id: entry.activity_id.clone(),
                    resolution: ActivityResolution::TimedOut {
                        timeout_type: match violation {
                            TimeoutViolation::ScheduleToClose => "schedule_to_close".to_string(),
                            TimeoutViolation::ScheduleToStart => "schedule_to_start".to_string(),
                            TimeoutViolation::StartToClose => "start_to_close".to_string(),
                            TimeoutViolation::Heartbeat => "heartbeat".to_string(),
                        },
                    },
                    worker_identity: None,
                    now,
                }),
            )
            .await
            .map(|_| ());

        match result {
            Ok(()) => {
                runtime_metrics::record_activity_task_timed_out(OutcomeLabel::Success);
                tracking.remove(entry.run_key, &entry.activity_id);
            }
            Err(error) => {
                let message = error.to_string();
                // Kernel rejection means the activity already resolved or advanced
                // past the state this timeout was computed against, so the entry is
                // stale: drop it instead of retrying. Other errors are transient,
                // so the entry is kept for the next scan.
                if message.contains("kernel rejected") {
                    runtime_metrics::record_activity_task_timed_out(OutcomeLabel::Rejected);
                    tracing::debug!(
                        ?error,
                        run_key = ?entry.run_key,
                        activity_id = entry.activity_id,
                        "activity timeout scanner timeout rejected by kernel"
                    );
                    tracking.remove(entry.run_key, &entry.activity_id);
                } else {
                    runtime_metrics::record_activity_task_timed_out(OutcomeLabel::Failure);
                    tracing::warn!(
                        ?error,
                        run_key = ?entry.run_key,
                        activity_id = entry.activity_id,
                        "activity timeout scanner failed to submit timeout"
                    );
                }
            }
        }

        runtime_metrics::record_scanner_dispatched("activity_timeout", entry.shard_id.0);
        submitted += 1;
    }
}

/// Background loop: every `scan_interval`, resolve timed-out activities for each
/// shard this node currently owns.
///
/// The active-shard set is re-read each tick so ownership changes (handoff, fresh
/// sweep) are picked up without restarting the task. Each firing routes by
/// `run_key` to the owning lane, keeping all commands for a run serialized on one
/// lane.
pub(crate) async fn run_activity_timeout_scanner<R>(
    repo: Arc<R>,
    tracking: ActivityTrackingState,
    lanes: Vec<LaneHandle>,
    lane_count: usize,
    shard_owner: Arc<RwLock<ShardOwner>>,
    config: ActivityTimeoutScannerConfig,
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
            runtime_metrics::record_scanner_tick("activity_timeout", shard_id.0);
            scan_activity_timeouts_once(
                repo.as_ref(),
                &tracking,
                Some(shard_id),
                &lanes,
                lane_count,
                &config,
            )
            .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use time::Duration;
    use tokeira_kernel::ActivityState;
    use tokeira_types::{Payloads, TaskQueueName};

    fn sample_activity() -> ActivityState {
        ActivityState {
            activity_id: "a".into(),
            activity_type: "activity-type".into(),
            schedule_event_id: 1,
            task_queue: TaskQueueName("q".into()),
            deployment: None,
            build_id: None,
            input: Payloads::default(),
            header: None,
            last_failure: None,
            heartbeat_details: None,
            attempt: 1,
            retry_policy: None,
            schedule_to_close_timeout: None,
            schedule_to_start_timeout: None,
            start_to_close_timeout: None,
            heartbeat_timeout: None,
            scheduled_at: OffsetDateTime::UNIX_EPOCH,
            current_attempt_scheduled_at: None,
            started_at: None,
            started_event_id: None,
            pause_info: None,
            stamp: 0,
        }
    }

    proptest! {
        #[test]
        fn record_scheduled_creates_entry(offset_secs in 0i64..1000i64) {
            let tracking = ActivityTrackingState::default();
            let run_key = RunKey::new();
            let now = OffsetDateTime::UNIX_EPOCH + Duration::seconds(offset_secs);
            tracking.record_scheduled(run_key, ShardId(0), "a", now);
            let entry = tracking.snapshot().into_iter().next().unwrap();
            prop_assert_eq!(entry.original_scheduled_at, now);
            prop_assert_eq!(entry.last_dispatched_at, now);
            prop_assert_eq!(entry.started_at, None);
        }

        #[test]
        fn remove_deletes_entry(offset_secs in 0i64..1000i64) {
            let tracking = ActivityTrackingState::default();
            let run_key = RunKey::new();
            let now = OffsetDateTime::UNIX_EPOCH + Duration::seconds(offset_secs);
            tracking.record_scheduled(run_key, ShardId(0), "a", now);
            tracking.remove(run_key, "a");
            prop_assert!(tracking.snapshot().is_empty());
        }

        #[test]
        fn no_timeout_without_configuration(offset_secs in 0i64..1000i64) {
            let now = OffsetDateTime::UNIX_EPOCH + Duration::seconds(offset_secs);
            let entry = ActivityTrackingEntry {
                run_key: RunKey::new(),
                shard_id: ShardId(0),
                activity_id: "a".into(),
                original_scheduled_at: now,
                last_dispatched_at: now,
                started_at: None,
                last_heartbeat_at: None,
                cancel_requested: false,
            };
            prop_assert_eq!(evaluate_activity_timeout(&entry, &sample_activity(), now + Duration::seconds(60)), None);
        }
    }

    #[test]
    fn schedule_to_close_takes_precedence() {
        let now = OffsetDateTime::now_utc();
        let entry = ActivityTrackingEntry {
            run_key: RunKey::new(),
            shard_id: ShardId(0),
            activity_id: "a".into(),
            original_scheduled_at: now - Duration::seconds(10),
            last_dispatched_at: now - Duration::seconds(10),
            started_at: Some(now - Duration::seconds(10)),
            last_heartbeat_at: Some(now - Duration::seconds(10)),
            cancel_requested: false,
        };
        let mut activity = sample_activity();
        activity.schedule_to_close_timeout = Some(Duration::seconds(1));
        activity.start_to_close_timeout = Some(Duration::seconds(1));
        activity.heartbeat_timeout = Some(Duration::seconds(1));
        assert_eq!(
            evaluate_activity_timeout(&entry, &activity, now),
            Some(TimeoutViolation::ScheduleToClose)
        );
    }
}
