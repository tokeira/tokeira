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
//! `crate::recovery::sweep_shard` on shard takeover — losing it costs a
//! rebuild, never correctness. Recovery restores schedule/start anchors and the
//! last durable heartbeat; retries clear the per-attempt start and heartbeat.
//! Entries are keyed by `(RunKey, activity_id)` and tagged with their
//! `ShardId` so a handoff evicts exactly the activities this node no longer owns.
//!
//! Cached timeouts select deadline wakes; each selected entry is still reloaded
//! before a decision. A periodic full pass reconciles stale caches, including
//! starts missed before schedule tracking; it restores the durable start and
//! heartbeat without replacing a newer tracked start or heartbeat. Retry
//! eligibility shares the wake, and bounded passes retain unattempted due work.

use tokeira_kernel::deadline_after;

use std::sync::{Arc, RwLock};

use crate::shard::ShardOwner;

use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    ActivityResolution, ActivityResolvedRequest, ActivityState, Command, LoadedRun, RetryState,
};
use tokeira_observability::OutcomeLabel;
use tokeira_proto::{
    conversions::common::{failure_to_payload, payloads_from_domain},
    public::temporal::api::{enums::v1 as enums_proto, failure::v1 as failure_proto},
};
use tokeira_storage::RunRepository;
use tokeira_types::{Payload, Payloads, RunKey, ShardId};
use tokio_util::sync::CancellationToken;

use crate::{
    lane::LaneHandle,
    metrics as runtime_metrics,
    retry::{RetryDecision, evaluate_activity_retry},
    runtime::{
        ActivityRetryDeps, ActivityRetryTarget, commit_activity_retry,
        exhausted_reason_to_retry_state,
    },
    scanner::pick_lane_for_run_key,
};

/// Derived timeout configuration; reloads remain authoritative before firing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct ActivityTimeouts {
    /// Whole activity budget, anchored at its first schedule across retries.
    pub schedule_to_close: Option<Duration>,
    /// Unstarted attempt budget, anchored at its latest dispatch.
    pub schedule_to_start: Option<Duration>,
    /// Started attempt budget.
    pub start_to_close: Option<Duration>,
    /// Liveness budget from the later of start and the latest heartbeat.
    pub heartbeat: Option<Duration>,
    /// Paused activities have no active timeout deadline.
    pub paused: bool,
}

impl From<&ActivityState> for ActivityTimeouts {
    fn from(activity: &ActivityState) -> Self {
        Self {
            schedule_to_close: activity.schedule_to_close_timeout,
            schedule_to_start: activity.schedule_to_start_timeout,
            start_to_close: activity.start_to_close_timeout,
            heartbeat: activity.heartbeat_timeout,
            paused: activity.pause_info.is_some(),
        }
    }
}

/// Earliest strict deadline, matching evaluation rather than choosing a violation.
/// Unknown configuration requires one reload; a saturated deadline cannot expire.
fn activity_timeout_due_at(entry: &ActivityTrackingEntry) -> Option<OffsetDateTime> {
    let Some(timeouts) = entry.timeouts else {
        return Some(time::PrimitiveDateTime::MIN.assume_utc());
    };
    if timeouts.paused {
        return None;
    }
    let due = |anchor, timeout: Option<Duration>| {
        timeout
            .filter(|timeout| !timeout.is_zero())
            .and_then(|timeout| {
                deadline_after(anchor, timeout).checked_add(Duration::nanoseconds(1))
            })
    };
    let close = due(entry.original_scheduled_at, timeouts.schedule_to_close);
    let attempt = if let Some(started) = entry.started_at {
        [
            due(started, timeouts.start_to_close),
            due(
                entry.last_heartbeat_at.unwrap_or(started).max(started),
                timeouts.heartbeat,
            ),
        ]
        .into_iter()
        .flatten()
        .min()
    } else {
        due(entry.last_dispatched_at, timeouts.schedule_to_start)
    };
    close.into_iter().chain(attempt).min()
}

/// One open activity being watched for timeouts.
///
/// Caches anchors and options from committed state. `started_at` and
/// `last_heartbeat_at` are `None` until the attempt starts / first heartbeats.
#[derive(Clone, Debug, PartialEq)]
pub struct ActivityTrackingEntry {
    /// Cached configuration; absence schedules an immediate authoritative reload.
    pub timeouts: Option<ActivityTimeouts>,
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

impl ActivityTrackingEntry {
    fn refresh_from_activity(&mut self, activity: &ActivityState) {
        self.timeouts = Some(ActivityTimeouts::from(activity));
        // An offer can reach a worker before the owner records its schedule.
        // Repair that missed start from authority, preserving any newer volatile
        // heartbeat. Never replace an already recorded start with a stale read.
        // Durable anchors drive Temporal timers too (service/history/workflow/
        // timer_sequence.go:298-355 @ v1.31.0).
        if self.started_at.is_none() && activity.started_at.is_some() {
            self.started_at = activity.started_at;
            self.last_heartbeat_at = self.last_heartbeat_at.max(activity.last_heartbeat_at);
        }
    }
}

/// Shared in-memory tracking state for open activities.
///
/// Cloning shares the underlying map; lanes, the scanner, and shard recovery all
/// see one set. Keyed by `(RunKey, activity_id)` since a run may have many
/// activities open at once.
#[derive(Clone, Debug)]
pub struct ActivityTrackingState {
    pub(crate) wake: crate::deadline_wake::DeadlineWake,
    pub(crate) eligibility: crate::activity_eligibility::ActivityEligibility,
    pub(crate) inner:
        crate::acquisition_tracking::AcquisitionEntries<(RunKey, String), ActivityTrackingEntry>,
}

impl Default for ActivityTrackingState {
    fn default() -> Self {
        let wake = crate::deadline_wake::DeadlineWake::default();
        Self {
            eligibility: crate::activity_eligibility::ActivityEligibility::with_wake(wake.clone()),
            inner: crate::acquisition_tracking::AcquisitionEntries::with_deadlines(
                wake.clone(),
                activity_timeout_due_at,
            ),
            wake,
        }
    }
}

impl ActivityTrackingState {
    pub(crate) fn set_owner(&self, owner: Arc<RwLock<ShardOwner>>) {
        self.eligibility.set_owner(owner.clone());
        self.inner.set_owner(owner);
    }
    pub(crate) fn for_acquisition(&self, acquisition: crate::shard::ShardAcquisition) -> Self {
        Self {
            eligibility: self.eligibility.for_acquisition(acquisition.clone()),
            inner: self.inner.for_acquisition(acquisition),
            ..self.clone()
        }
    }

    pub(crate) fn for_activity(&self, run: RunKey, activity: &str) -> Self {
        Self {
            eligibility: self.eligibility.for_activity(run, activity),
            inner: self.inner.for_key(&(run, activity.into())),
            ..self.clone()
        }
    }

    /// Insert a fully-formed entry, replacing any existing one for the same key.
    /// Used by shard recovery to rebuild tracking from durable state.
    pub fn insert(&self, entry: ActivityTrackingEntry) {
        self.inner.insert(
            (entry.run_key, entry.activity_id.clone()),
            entry.shard_id,
            entry,
        );
    }

    /// Begin tracking a freshly scheduled activity, anchoring both the
    /// schedule-to-close and schedule-to-start clocks at `now`.
    ///
    /// When an entry already exists — a re-dispatch after unpause, reset, or
    /// workflow unpause — the schedule-to-close anchor (`original_scheduled_at`)
    /// is preserved so the overall deadline still spans from the first schedule,
    /// matching Temporal's absolute schedule-to-close deadline
    /// (`timer_sequence.go:266-277 @ v1.31.0`). Only the per-attempt dispatch and
    /// start/heartbeat clocks reset. Without this, an unpaused activity would be
    /// granted a fresh full schedule-to-close budget on every resume.
    pub fn record_scheduled(
        &self,
        run_key: RunKey,
        shard_id: ShardId,
        activity_id: impl Into<String>,
        now: OffsetDateTime,
    ) {
        self.record_scheduled_with_timeouts(run_key, shard_id, activity_id, now, None);
    }

    pub(crate) fn record_scheduled_with_timeouts(
        &self,
        run_key: RunKey,
        shard_id: ShardId,
        activity_id: impl Into<String>,
        now: OffsetDateTime,
        timeouts: Option<ActivityTimeouts>,
    ) {
        let activity_id = activity_id.into();
        self.inner
            .upsert((run_key, activity_id.clone()), shard_id, |previous| {
                let mut entry = previous.unwrap_or(ActivityTrackingEntry {
                    timeouts: None,
                    run_key,
                    shard_id,
                    activity_id,
                    original_scheduled_at: now,
                    last_dispatched_at: now,
                    started_at: None,
                    last_heartbeat_at: None,
                    cancel_requested: false,
                });
                entry.timeouts = timeouts.or(entry.timeouts);
                entry.last_dispatched_at = now;
                entry.started_at = None;
                entry.last_heartbeat_at = None;
                entry
            });
    }

    /// Install missing timeout tracking before making an eligibility obligation
    /// visible. Existing anchors belong to their scoped start/retry callbacks.
    pub(crate) fn refresh_eligibility(
        &self,
        state: &tokeira_kernel::WorkflowState,
        home: ShardId,
        recorded_at: OffsetDateTime,
    ) {
        if state.status == tokeira_types::ExecutionStatus::Running {
            for activity in state.activities.values().filter(|activity| {
                activity.started_at.is_none()
                    && activity.started_event_id.is_none()
                    && activity.pause_info.is_none()
                    && activity.current_attempt_scheduled_at.is_some()
            }) {
                self.inner.upsert_if(
                    (state.run_key, activity.activity_id.clone()),
                    home,
                    |previous| {
                        previous.is_none().then(|| ActivityTrackingEntry {
                            timeouts: Some(ActivityTimeouts::from(activity)),
                            run_key: state.run_key,
                            shard_id: home,
                            activity_id: activity.activity_id.clone(),
                            original_scheduled_at: activity.scheduled_at,
                            last_dispatched_at: activity
                                .current_attempt_scheduled_at
                                .unwrap_or(activity.scheduled_at),
                            started_at: None,
                            last_heartbeat_at: activity.last_heartbeat_at,
                            cancel_requested: activity.cancel_requested,
                        })
                    },
                );
            }
        }
        self.eligibility.refresh(state, home, recorded_at);
    }

    /// Refresh cached options without rearming unchanged, already attempted work.
    pub(crate) fn refresh_timeouts(&self, state: &tokeira_kernel::WorkflowState) {
        for (activity_id, activity) in &state.activities {
            let key = (state.run_key, activity_id.clone());
            let Some(tracked) = self.inner.tracked(&key) else {
                continue;
            };
            let timeouts = Some(ActivityTimeouts::from(activity));
            if tracked.value.timeouts != timeouts {
                self.inner
                    .for_entry(&tracked)
                    .update(&key, |entry| entry.timeouts = timeouts);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn set_timeouts(&self, run: RunKey, id: &str, timeouts: ActivityTimeouts) {
        self.inner
            .update(&(run, id.into()), |entry| entry.timeouts = Some(timeouts));
    }

    fn begin_full_pass(&self) {
        self.inner.begin_full_pass(true);
        self.eligibility.begin_full_pass();
    }

    pub(crate) fn scan_delay(
        &self,
        now: OffsetDateTime,
        maximum: std::time::Duration,
    ) -> std::time::Duration {
        let revision = self.wake.begin_plan();
        let delay = self.inner.deadline_delay(now, maximum);
        let delay = self.eligibility.deadline_delay(now, delay);
        self.wake.plan(now, revision, delay)
    }

    /// Record a redispatch on retry: advance the schedule-to-start anchor and
    /// clear the per-attempt start/heartbeat clocks, while leaving
    /// `original_scheduled_at` (the schedule-to-close anchor) untouched so the
    /// overall deadline still spans all attempts.
    pub fn record_retry(&self, run_key: RunKey, activity_id: &str, now: OffsetDateTime) {
        self.record_retry_with_timeouts(run_key, activity_id, now, None);
    }

    pub(crate) fn record_retry_with_timeouts(
        &self,
        run_key: RunKey,
        activity_id: &str,
        now: OffsetDateTime,
        timeouts: Option<ActivityTimeouts>,
    ) {
        self.inner
            .update(&(run_key, activity_id.to_string()), |entry| {
                entry.timeouts = timeouts.or(entry.timeouts);
                entry.last_dispatched_at = now;
                entry.started_at = None;
                entry.last_heartbeat_at = None;
            });
    }

    /// Record that a worker started the activity, anchoring the start-to-close and
    /// heartbeat clocks. Heartbeat is reset to `None` so the first interval is
    /// measured from start.
    pub fn record_started(&self, run_key: RunKey, activity_id: &str, now: OffsetDateTime) {
        self.record_started_with_timeouts(run_key, activity_id, now, None);
    }

    pub(crate) fn record_started_with_timeouts(
        &self,
        run_key: RunKey,
        activity_id: &str,
        now: OffsetDateTime,
        timeouts: Option<ActivityTimeouts>,
    ) {
        self.eligibility.remove(run_key, activity_id);
        self.inner
            .update(&(run_key, activity_id.to_string()), |entry| {
                entry.timeouts = timeouts.or(entry.timeouts);
                entry.started_at = Some(now);
                entry.last_heartbeat_at = None;
            });
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
        self.inner
            .update(&(run_key, activity_id.to_string()), |entry| {
                entry.last_heartbeat_at = Some(now);
                entry.cancel_requested
            })
    }

    /// Mark that a cancel has been requested for the activity; relayed to the
    /// worker on its next heartbeat.
    pub fn mark_cancel_requested(&self, run_key: RunKey, activity_id: &str) {
        self.inner
            .update(&(run_key, activity_id.to_string()), |entry| {
                entry.cancel_requested = true;
            });
    }

    /// Whether a cancel has been requested, or `None` if the activity is untracked.
    pub fn is_cancel_requested(&self, run_key: RunKey, activity_id: &str) -> Option<bool> {
        self.inner
            .get(&(run_key, activity_id.to_string()))
            .map(|entry| entry.cancel_requested)
    }

    /// Stop tracking an activity, called when it resolves so it is never scanned
    /// again.
    pub fn remove(&self, run_key: RunKey, activity_id: &str) {
        self.eligibility.remove(run_key, activity_id);
        self.inner.remove(&(run_key, activity_id.to_string()));
    }

    /// Stop tracking every activity owned by a deleted workflow run.
    pub fn remove_all_for_run(&self, run_key: RunKey) {
        self.eligibility.remove_run(run_key);
        self.inner.retain(|(key, _), _| *key != run_key);
    }

    /// Drop every entry for a shard on handoff; the new owner rebuilds them from
    /// durable state during its sweep.
    pub fn remove_all_for_shard(&self, shard_id: ShardId) {
        self.eligibility.remove_home(shard_id);
        self.inner.retain(|_, entry| entry.shard_id != shard_id);
    }

    /// Snapshot all tracked entries; the scanner evaluates the copy without
    /// holding the lock.
    pub fn snapshot(&self) -> Vec<ActivityTrackingEntry> {
        self.inner
            .snapshot(None)
            .into_iter()
            .map(|entry| entry.value)
            .collect()
    }

    /// Snapshot only the entries owned by `shard_id`, used by the per-shard scan.
    pub fn snapshot_for_shard(&self, shard_id: ShardId) -> Vec<ActivityTrackingEntry> {
        self.inner
            .snapshot(Some(shard_id))
            .into_iter()
            .map(|entry| entry.value)
            .collect()
    }
}

/// Which activity deadline was exceeded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeoutViolation {
    ScheduleToClose,
    ScheduleToStart,
    StartToClose,
    Heartbeat,
}

/// Tuning for the activity timeout scanner loop.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivityTimeoutScannerConfig {
    /// Maximum delay between full reconciliations; earlier deadlines wake sooner.
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
/// yet, only schedule-to-start applies. The heartbeat clock runs from the later
/// of `started_at` and the last heartbeat, so the first interval is measured
/// from start and a heartbeat from an earlier attempt never shortens it. A zero timeout never fires: v1.31.0 creates a timeout
/// timer only for positive durations (`timer_sequence.go:268-271 @ v1.31.0`) —
/// zero/unset means "no deadline" (the edge already normalizes present-zero
/// SDK durations to `None`; the guards here are defense in depth).
pub fn evaluate_activity_timeout(
    entry: &ActivityTrackingEntry,
    activity: &ActivityState,
    now: OffsetDateTime,
) -> Option<TimeoutViolation> {
    if let Some(timeout) = activity.schedule_to_close_timeout
        && !timeout.is_zero()
        && now > deadline_after(entry.original_scheduled_at, timeout)
    {
        return Some(TimeoutViolation::ScheduleToClose);
    }

    if let Some(started_at) = entry.started_at {
        if let Some(timeout) = activity.heartbeat_timeout
            && !timeout.is_zero()
        {
            // The later of the attempt's start and the last heartbeat
            // (`getActivityHeartbeatTimeout`, timer_sequence.go:341-351 @
            // v1.31.0): a time restored from durable state may predate the
            // current attempt, whose clock then runs from its start.
            let heartbeat_at = entry
                .last_heartbeat_at
                .map_or(started_at, |at| at.max(started_at));
            if now > deadline_after(heartbeat_at, timeout) {
                return Some(TimeoutViolation::Heartbeat);
            }
        }

        if let Some(timeout) = activity.start_to_close_timeout
            && !timeout.is_zero()
            && now > deadline_after(started_at, timeout)
        {
            return Some(TimeoutViolation::StartToClose);
        }
    } else if let Some(timeout) = activity.schedule_to_start_timeout
        && !timeout.is_zero()
        && now > deadline_after(entry.last_dispatched_at, timeout)
    {
        return Some(TimeoutViolation::ScheduleToStart);
    }

    None
}

/// Reconcile all entries, reloading authoritative state before retry or resolution.
#[cfg(test)]
pub(crate) async fn scan_activity_timeouts_once<R: RunRepository + 'static>(
    deps: &ActivityRetryDeps<R>,
    shard_id: Option<ShardId>,
    lanes: &[LaneHandle],
    lane_count: usize,
    config: &ActivityTimeoutScannerConfig,
) {
    deps.tracking.inner.begin_full_pass(true);
    scan_activity_timeouts_at(deps, shard_id, lanes, lane_count, config, deps.clock.now()).await;
}

/// Reload due entries in stable deadline order. Full-pass obligations also refresh
/// future caches. Mark attempts before I/O so failed loads/submissions wait for the
/// next interval; a capped pass leaves unattempted entries immediately runnable.
pub(crate) async fn scan_activity_timeouts_at<R: RunRepository + 'static>(
    deps: &ActivityRetryDeps<R>,
    shard_id: Option<ShardId>,
    lanes: &[LaneHandle],
    lane_count: usize,
    config: &ActivityTimeoutScannerConfig,
    now: OffsetDateTime,
) {
    let tracking = &deps.tracking;
    let mut submitted = 0usize;

    let mut entries = tracking.inner.select_active(shard_id, |entry| {
        entry.refresh_pending
            || (entry.deadline_pending
                && activity_timeout_due_at(&entry.value).is_some_and(|due| due <= now))
    });
    entries.sort_by_key(|entry| {
        (
            activity_timeout_due_at(&entry.value)
                .unwrap_or(time::PrimitiveDateTime::MAX.assume_utc()),
            entry.value.run_key,
            entry.value.activity_id.clone(),
        )
    });

    for tracked in entries {
        if !tracking.inner.active(&tracked) {
            continue;
        }
        let mut entry = tracked.value.clone();
        if submitted >= config.max_timeouts_per_scan {
            break;
        }

        if !tracking
            .inner
            .mark_attempted(&(entry.run_key, entry.activity_id.clone()), &tracked)
        {
            continue;
        }
        let state = match deps.repo.load_run(entry.run_key).await {
            Ok(LoadedRun::Existing(state)) => state,
            Ok(LoadedRun::Absent) => {
                tracking
                    .inner
                    .remove_submitted(&(entry.run_key, entry.activity_id.clone()), &tracked);
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

        if !tracking.inner.active(&tracked) {
            continue;
        }
        // A closed run keeps its activities in state, but none of their
        // timeouts can fire: v1.31.0's activity-timeout task returns
        // `ErrWorkflowCompleted` for a closed run
        // (timer_queue_active_task_executor.go:218-221 @ v1.31.0), a NotFound
        // the queue completes rather than retries (queues/executable.go:401).
        // Untrack instead of retrying or resolving an activity of a closed run.
        if !state.is_open() {
            tracking
                .inner
                .remove_submitted(&(entry.run_key, entry.activity_id.clone()), &tracked);
            continue;
        }

        let Some(activity) = state.activities.get(&entry.activity_id).cloned() else {
            tracking
                .inner
                .remove_submitted(&(entry.run_key, entry.activity_id.clone()), &tracked);
            continue;
        };

        // Full passes also reload entries whose missing start left them without
        // a heartbeat/start-to-close deadline. Repair before evaluating this pass.
        entry.refresh_from_activity(&activity);
        tracking.inner.refresh_scanned(
            &(entry.run_key, entry.activity_id.clone()),
            &tracked,
            now,
            |entry| {
                entry.refresh_from_activity(&activity);
            },
        );

        // Temporal suspends all four activity timers while an activity is paused:
        // `LoadAndSortActivityTimers` skips paused activities
        // (timer_sequence.go:181-183 @ v1.31.0), so a pending timer task hitting
        // a paused activity fires nothing. The entry is left tracked (not removed)
        // so evaluation resumes on unpause with the original schedule-to-close
        // anchor intact. Reloading state each scan also means a paused activity
        // whose tracking was rebuilt on shard recovery is skipped here too.
        if activity.pause_info.is_some() {
            continue;
        }

        let Some(violation) = evaluate_activity_timeout(&entry, &activity, now) else {
            continue;
        };

        // A firing timeout runs the retry machinery FIRST; only a terminal
        // retry state resolves the activity to its workflow
        // (`processSingleActivityTimeoutTask`,
        // timer_queue_active_task_executor.go:281-362 @ v1.31.0).
        // ScheduleToStart / ScheduleToClose are server-enforced deadlines and
        // never retried — they short-circuit to RETRY_STATE_TIMEOUT — and a
        // requested cancel likewise suppresses retry
        // (RETRY_STATE_CANCEL_REQUESTED) (`RetryActivity`,
        // mutable_state_impl.go:6235-6266 @ v1.31.0).
        let mut exhausted_reason = None;
        let retry_policy = activity
            .retry_policy
            .clone()
            .or_else(|| state.retry_policy.clone());
        if matches!(
            violation,
            TimeoutViolation::StartToClose | TimeoutViolation::Heartbeat
        ) && !entry.cancel_requested
            && let Some(policy) = retry_policy.as_ref()
        {
            // Retry-expiration anchor: the FIRST schedule + schedule-to-close
            // (the deadline spans the whole retry chain;
            // `retry.go:108-110 @ v1.31.0`).
            let expiration = activity
                .schedule_to_close_timeout
                .filter(|timeout| !timeout.is_zero())
                .map(|timeout| deadline_after(activity.scheduled_at, timeout));
            // Timeout failures classify against the non-retryable list as
            // "TemporalTimeout:<type>" (`isRetryable`, retry.go:124-133 +
            // retrypolicy.TimeoutFailureTypePrefix @ v1.31.0).
            let error_type = format!("TemporalTimeout:{}", timeout_type_name(violation));
            match evaluate_activity_retry(
                policy,
                activity.attempt,
                Some(&error_type),
                false,
                now,
                expiration,
            ) {
                RetryDecision::Retry {
                    next_attempt,
                    backoff,
                } => {
                    // A retry's post-commit tracking belongs to the scan's
                    // acquisition and submitted revision, even if its repository
                    // reply arrives after another operation replaced the key.
                    let mut retry_deps = deps.clone();
                    let scoped_tracking = tracked.acquisition().map_or_else(
                        || tracking.clone(),
                        |acquisition| tracking.for_acquisition(acquisition),
                    );
                    retry_deps.tracking = ActivityTrackingState {
                        inner: tracking.inner.for_entry(&tracked),
                        ..scoped_tracking
                    };
                    let result = commit_activity_retry(
                        &retry_deps,
                        ActivityRetryTarget {
                            run_key: entry.run_key,
                            activity_id: &entry.activity_id,
                            expected_attempt: activity.attempt,
                            expected_schedule_event_id: activity.schedule_event_id,
                        },
                        next_attempt,
                        backoff,
                        // The timed-out attempt's failure becomes durable retry
                        // bookkeeping (`RetryLastFailure`), surfaced by Describe
                        // (`processSingleActivityTimeoutTask` builds it via
                        // `failure.NewTimeoutFailure` @ v1.31.0).
                        Some(timeout_failure_payload(violation)),
                        // A timeout carries no worker progress.
                        None,
                    )
                    .await;
                    match result {
                        Ok(()) => {
                            runtime_metrics::record_activity_task_retry(OutcomeLabel::Success);
                        }
                        Err(error) => {
                            // Lost a race with a concurrent resolution or hit a
                            // transient commit error: leave the entry; the next
                            // scan re-evaluates against reloaded state.
                            runtime_metrics::record_activity_task_retry(OutcomeLabel::Failure);
                            tracing::warn!(
                                ?error,
                                run_key = ?entry.run_key,
                                activity_id = entry.activity_id,
                                "activity timeout scanner failed to commit retry"
                            );
                        }
                    }
                    runtime_metrics::record_scanner_dispatched(
                        "activity_timeout",
                        entry.shard_id.0,
                    );
                    submitted += 1;
                    continue;
                }
                RetryDecision::Exhausted { reason } => exhausted_reason = Some(reason),
            }
        }

        // Why retries stopped, mirroring `RetryActivity`'s derivation order
        // (mutable_state_impl.go:6243-6270 @ v1.31.0): no policy →
        // RETRY_POLICY_NOT_SET; a requested cancel → CANCEL_REQUESTED; the
        // server-deadline types → TIMEOUT; otherwise the exhaustion reason
        // from `nextBackoffInterval` (retry.go:96-110).
        let retry_state = if retry_policy.is_none() {
            RetryState::RetryPolicyNotSet
        } else if entry.cancel_requested {
            RetryState::CancelRequested
        } else if matches!(
            violation,
            TimeoutViolation::ScheduleToStart | TimeoutViolation::ScheduleToClose
        ) {
            RetryState::Timeout
        } else {
            match exhausted_reason {
                Some(reason) => exhausted_reason_to_retry_state(reason),
                // Unreachable in practice: a retryable violation with a policy
                // and no cancel either retried (`continue` above) or exhausted.
                None => RetryState::Timeout,
            }
        };
        // Terminal: resolve as timed out. Whenever the retry state is TIMEOUT
        // ("no time for another attempt") the failure AND type convert to
        // ScheduleToClose — this covers both backoff exhaustion and a directly
        // fired ScheduleToClose under a retry policy; a fired ScheduleToStart
        // keeps its own type (`processSingleActivityTimeoutTask`,
        // timer_queue_active_task_executor.go:307-316 @ v1.31.0).
        let converted =
            retry_state == RetryState::Timeout && violation != TimeoutViolation::ScheduleToStart;
        let timeout_type = if converted {
            "schedule_to_close".to_string()
        } else {
            match violation {
                TimeoutViolation::ScheduleToClose => "schedule_to_close".to_string(),
                TimeoutViolation::ScheduleToStart => "schedule_to_start".to_string(),
                TimeoutViolation::StartToClose => "start_to_close".to_string(),
                TimeoutViolation::Heartbeat => "heartbeat".to_string(),
            }
        };
        let lane = pick_lane_for_run_key(lanes, lane_count, entry.run_key).clone();
        let result = lane
            .submit(
                entry.run_key,
                Command::ActivityResolved(ActivityResolvedRequest {
                    activity_id: entry.activity_id.clone(),
                    resolution: ActivityResolution::TimedOut {
                        timeout_type,
                        retry_state,
                        failure: Some(terminal_timeout_failure(
                            violation,
                            converted,
                            activity.heartbeat_details.as_ref(),
                        )),
                    },
                    worker_identity: None,
                    request: tokeira_types::RequestContext::unattributed(now),
                    now,
                }),
            )
            .await
            .map(|_| ());

        match result {
            Ok(()) => {
                runtime_metrics::record_activity_task_timed_out(OutcomeLabel::Success);
                tracking
                    .inner
                    .remove_submitted(&(entry.run_key, entry.activity_id.clone()), &tracked);
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
                    tracking
                        .inner
                        .remove_submitted(&(entry.run_key, entry.activity_id.clone()), &tracked);
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

/// The enum-name form of a fired timeout, used in failure messages and retry
/// classification — mirrors `enumspb.TimeoutType.String()` (api-go v1.62.8
/// custom Stringer: "StartToClose", "ScheduleToStart", "ScheduleToClose",
/// "Heartbeat").
fn timeout_type_name(violation: TimeoutViolation) -> &'static str {
    match violation {
        TimeoutViolation::ScheduleToClose => "ScheduleToClose",
        TimeoutViolation::ScheduleToStart => "ScheduleToStart",
        TimeoutViolation::StartToClose => "StartToClose",
        TimeoutViolation::Heartbeat => "Heartbeat",
    }
}

/// The wire `TimeoutType` for a fired violation.
fn proto_timeout_type(violation: TimeoutViolation) -> enums_proto::TimeoutType {
    match violation {
        TimeoutViolation::ScheduleToClose => enums_proto::TimeoutType::ScheduleToClose,
        TimeoutViolation::ScheduleToStart => enums_proto::TimeoutType::ScheduleToStart,
        TimeoutViolation::StartToClose => enums_proto::TimeoutType::StartToClose,
        TimeoutViolation::Heartbeat => enums_proto::TimeoutType::Heartbeat,
    }
}

/// The timeout failure recorded as retry bookkeeping for a timed-out attempt:
/// message `"activity <Type> timeout"` (`common.FailureReasonActivityTimeout`,
/// common/util.go:95 @ v1.31.0) wrapping a `TimeoutFailureInfo` of the fired
/// type (`failure.NewTimeoutFailure`, common/failure/failure.go:36 @ v1.31.0).
/// No heartbeat details: v1.31.0 folds those onto the TERMINAL failure only
/// (after the in-progress early return, timer_queue_active_task_executor.go:341).
fn timeout_failure_payload(violation: TimeoutViolation) -> Payload {
    failure_to_payload(&failure_proto::Failure {
        message: format!("activity {} timeout", timeout_type_name(violation)),
        failure_info: Some(failure_proto::failure::FailureInfo::TimeoutFailureInfo(
            failure_proto::TimeoutFailureInfo {
                timeout_type: proto_timeout_type(violation) as i32,
                ..Default::default()
            },
        )),
        ..Default::default()
    })
}

/// The failure carried on a terminal `ActivityTaskTimedOut` event (kernel
/// raise K2): the fired type's own failure, or — when the retry state is
/// TIMEOUT and the fired type is not ScheduleToStart — the converted
/// ScheduleToClose failure; the last recorded heartbeat details ride along
/// (`processSingleActivityTimeoutTask`,
/// timer_queue_active_task_executor.go:307-345 @ v1.31.0).
fn terminal_timeout_failure(
    violation: TimeoutViolation,
    converted_to_schedule_to_close: bool,
    last_heartbeat_details: Option<&Payloads>,
) -> Payload {
    let (message, timeout_type) = if converted_to_schedule_to_close {
        (
            "Not enough time to schedule next retry before activity ScheduleToClose timeout, giving up retrying"
                .to_string(),
            enums_proto::TimeoutType::ScheduleToClose,
        )
    } else {
        (
            format!("activity {} timeout", timeout_type_name(violation)),
            proto_timeout_type(violation),
        )
    };
    failure_to_payload(&failure_proto::Failure {
        message,
        failure_info: Some(failure_proto::failure::FailureInfo::TimeoutFailureInfo(
            failure_proto::TimeoutFailureInfo {
                timeout_type: timeout_type as i32,
                last_heartbeat_details: last_heartbeat_details.map(payloads_from_domain),
            },
        )),
        ..Default::default()
    })
}

/// Wake at the earliest timeout or retry deadline, with a full reconciliation
/// every `scan_interval` as a backstop for stale caches.
///
/// The active-shard set is re-read each tick so ownership changes (handoff, fresh
/// sweep) are picked up without restarting the task. Each firing routes by
/// `run_key` to the owning lane, keeping all commands for a run serialized on one
/// lane.
pub(crate) async fn run_activity_timeout_scanner<R>(
    deps: ActivityRetryDeps<R>,
    lanes: Vec<LaneHandle>,
    lane_count: usize,
    config: ActivityTimeoutScannerConfig,
    cancel: CancellationToken,
) where
    R: RunRepository + 'static,
{
    let mut next_full = deadline_after(
        deps.clock.now(),
        Duration::try_from(config.scan_interval).unwrap_or(Duration::MAX),
    );
    loop {
        let now = deps.clock.now();
        let maximum = std::time::Duration::try_from(next_full - now).unwrap_or_default();
        let delay = deps
            .tracking
            .scan_delay(now, maximum.min(config.scan_interval));
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(delay) => {},
            _ = deps.tracking.wake.notify.notified() => {}
        }

        let woke = deps.clock.now();
        deps.tracking.wake.start_scan(woke);
        if woke >= next_full {
            deps.tracking.begin_full_pass();
            next_full = deadline_after(
                woke,
                Duration::try_from(config.scan_interval).unwrap_or(Duration::MAX),
            );
        }
        let active_shards: Vec<_> = deps
            .shard_owner
            .read()
            .expect("shard_owner lock poisoned")
            .active_shards()
            .collect();
        for shard_id in active_shards {
            runtime_metrics::record_scanner_tick("activity_timeout", shard_id.0);
            scan_activity_timeouts_at(
                &deps,
                Some(shard_id),
                &lanes,
                lane_count,
                &config,
                deps.clock.now(),
            )
            .await;
            crate::activity_eligibility::scan_activity_eligibility_once(
                &deps,
                shard_id,
                deps.clock.now(),
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
    use tokeira_types::TaskQueueName;

    pub(super) fn sample_activity() -> ActivityState {
        ActivityState {
            last_attempt_complete_time: None,
            cancel_requested: false,
            activity_reset: false,
            reset_heartbeats: false,
            started_identity: None,
            retry_last_worker_identity: None,
            activity_id: "a".into(),
            activity_type: "activity-type".into(),
            schedule_event_id: 1,
            task_queue: TaskQueueName("q".into()),
            deployment: None,
            build_id: None,
            retired_input: Default::default(),
            retired_header: Default::default(),
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
            priority: None,
            last_heartbeat_at: None,
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
                timeouts: None,
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

        // Feature: activity-heartbeat-time, Property 6: The heartbeat deadline is
        // v1.31.0's
        #[test]
        fn property_heartbeat_deadline_runs_from_the_later_of_start_and_heartbeat(
            started_offset in -10_000i64..10_000,
            heartbeat_offset in proptest::option::of(-20_000i64..20_000),
            timeout_secs in 1i64..5_000,
            now_offset in -10_000i64..40_000,
        ) {
            let base = OffsetDateTime::UNIX_EPOCH + Duration::days(20_000);
            let started_at = base + Duration::seconds(started_offset);
            let last_heartbeat_at = heartbeat_offset.map(|offset| base + Duration::seconds(offset));
            let now = base + Duration::seconds(now_offset);
            let entry = ActivityTrackingEntry {
                timeouts: None,
                run_key: RunKey::new(),
                shard_id: ShardId(0),
                activity_id: "a".into(),
                original_scheduled_at: started_at,
                last_dispatched_at: started_at,
                started_at: Some(started_at),
                last_heartbeat_at,
                cancel_requested: false,
            };
            let mut activity = sample_activity();
            activity.heartbeat_timeout = Some(Duration::seconds(timeout_secs));
            let anchor = last_heartbeat_at.map_or(started_at, |at| at.max(started_at));
            prop_assert_eq!(
                evaluate_activity_timeout(&entry, &activity, now) == Some(TimeoutViolation::Heartbeat),
                now - anchor > Duration::seconds(timeout_secs),
            );
        }
    }

    #[test]
    fn schedule_to_close_takes_precedence() {
        let now = OffsetDateTime::now_utc();
        let entry = ActivityTrackingEntry {
            timeouts: None,
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

#[cfg(test)]
#[path = "activity_timeout_deadline_tests.rs"]
mod deadline_tests;
