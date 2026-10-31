//! Workflow-level timeout tracking and scanning.
//!
//! Owns the in-memory set of open workflow runs that carry an execution- or
//! run-level timeout, evaluates whether either deadline has passed, and drives a
//! background scanner that submits the corresponding timeout command to the
//! owning lane.
//!
//! The tracking state here is *volatile and derived*. The durable transition log
//! is authoritative for whether a run is open and what its timeouts are; this map
//! exists only so the scanner does not have to re-read the store for every run on
//! every tick. It is reconstructed from durable history by
//! `crate::recovery::sweep_shard` whenever this node takes over a shard, so
//! losing it to a crash or failover costs a rebuild, never correctness. Entries
//! are tagged with their `ShardId` so a handoff can drop exactly the runs this
//! node no longer owns via [`WorkflowTimeoutTrackingState::remove_all_for_shard`]
//! without disturbing co-resident shards.

use std::sync::{Arc, RwLock};

use anyhow::Result;
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{Command, RetryState, WorkflowExecutionTimedOutRequest, WorkflowTimeoutType};
use tokeira_types::{RunKey, ShardId};
use tokio_util::sync::CancellationToken;

use crate::successor::{SuccessorCause, SuccessorPlan, SuccessorTiming, load_started_event_inputs};

use crate::{
    lane::LaneHandle, metrics as runtime_metrics, scanner::pick_lane_for_run_key, shard::ShardOwner,
};

/// Runtime-local timeout tracking entry for one open run.
///
/// Captures only the fields the scanner needs to decide a timeout without
/// reloading the run from the store. A copy of the authoritative timeouts, not a
/// source of truth: it is rebuilt from durable state on shard takeover.
#[derive(Clone, Debug, PartialEq)]
pub struct WorkflowTimeoutEntry {
    pub run_key: RunKey,
    /// Shard that owns this run, so a handoff can evict exactly the entries this
    /// node no longer owns.
    pub shard_id: ShardId,
    pub workflow_execution_timeout: Option<Duration>,
    pub workflow_run_timeout: Option<Duration>,
    /// When the *current* run's `WorkflowExecutionStarted` event was written.
    pub started_at: OffsetDateTime,
    /// First-workflow-task backoff (cron/delayed/retry start). The run timeout is
    /// anchored on the run's EXECUTION time — `started_at + workflow_start_delay` —
    /// because in v1.31.0 the run does not begin executing until the backoff elapses
    /// (there the started event is itself written at that later time).
    pub workflow_start_delay: Option<Duration>,
    /// Start of the *first* run in the execution chain (continue-as-new / retry),
    /// the anchor for the execution timeout. `None` for an original run, where the
    /// execution clock coincides with `started_at`.
    pub first_run_started_at: Option<OffsetDateTime>,
    /// Whether the run has a retry policy, which selects the `RetryState` reported
    /// on a timeout (see `workflow_timeout_retry_state`).
    pub has_retry_policy: bool,
}

/// Shared in-memory tracking state for workflow timeouts.
///
/// Cloning shares the underlying map (it is `Arc`-backed), so the scanner, the
/// lanes that insert/remove entries, and shard recovery all observe one set.
#[derive(Clone, Default, Debug)]
pub struct WorkflowTimeoutTrackingState {
    pub(crate) inner: crate::acquisition_tracking::AcquisitionEntries<RunKey, WorkflowTimeoutEntry>,
}

impl WorkflowTimeoutTrackingState {
    pub(crate) fn set_owner(&self, owner: Arc<RwLock<ShardOwner>>) {
        self.inner.set_owner(owner);
    }
    pub(crate) fn for_acquisition(&self, acquisition: crate::shard::ShardAcquisition) -> Self {
        Self {
            inner: self.inner.for_acquisition(acquisition),
        }
    }
    /// Install only for the current acquisition; recovery may install before activation.
    pub fn insert(&self, entry: WorkflowTimeoutEntry) {
        self.inner.insert(entry.run_key, entry.shard_id, entry);
    }
    /// Stop tracking after the authoritative transition has resolved this key.
    pub fn remove(&self, run_key: RunKey) {
        self.inner.remove(&run_key);
    }
    /// Purge the departing home under the ownership lock, before a replacement can install.
    pub fn remove_all_for_shard(&self, shard_id: ShardId) {
        self.inner.retain(|_, entry| entry.shard_id != shard_id);
    }
    /// Snapshot values for inspection; scanners additionally validate acquisition and revision.
    pub fn snapshot(&self) -> Vec<WorkflowTimeoutEntry> {
        self.inner
            .snapshot(None)
            .into_iter()
            .map(|entry| entry.value)
            .collect()
    }
    /// Snapshot only this execution home's values.
    pub fn snapshot_for_shard(&self, shard_id: ShardId) -> Vec<WorkflowTimeoutEntry> {
        self.inner
            .snapshot(Some(shard_id))
            .into_iter()
            .map(|entry| entry.value)
            .collect()
    }
}

/// Tuning for the workflow timeout scanner loop.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowTimeoutScannerConfig {
    /// Maximum delay between scans; approaching tracked deadlines shorten it.
    pub scan_interval: tokio::time::Duration,
    /// Upper bound on timeouts submitted per scan, so a large backlog of expired
    /// runs cannot monopolize a tick; the remainder is picked up on the next pass.
    pub max_timeouts_per_scan: usize,
}

impl Default for WorkflowTimeoutScannerConfig {
    fn default() -> Self {
        Self {
            scan_interval: tokio::time::Duration::from_secs(1),
            max_timeouts_per_scan: 100,
        }
    }
}

/// Which workflow-level deadline a run has exceeded.
#[derive(Clone, Debug, PartialEq)]
pub enum WorkflowTimeoutViolation {
    /// Execution timeout: measured from the first run in the execution chain.
    ExecutionTimeout,
    /// Run timeout: measured from the start of the current run.
    RunTimeout,
}

/// Decide whether `entry` has breached either workflow deadline at `now`.
///
/// Execution timeout is checked before run timeout because it bounds the whole
/// continue-as-new / retry chain and so takes precedence when both have elapsed.
/// A zero timeout is treated as "expires the instant its anchor is reached"
/// rather than "no timeout", matching how the durable layer encodes an
/// immediately-due deadline.
pub fn evaluate_workflow_timeout(
    entry: &WorkflowTimeoutEntry,
    now: OffsetDateTime,
) -> Option<WorkflowTimeoutViolation> {
    let execution_started_at = entry.first_run_started_at.unwrap_or(entry.started_at);
    if let Some(timeout) = entry.workflow_execution_timeout
        && (now - execution_started_at > timeout
            || timeout.is_zero() && now >= execution_started_at)
    {
        return Some(WorkflowTimeoutViolation::ExecutionTimeout);
    }

    let run_started_at = entry.started_at + entry.workflow_start_delay.unwrap_or(Duration::ZERO);
    if let Some(timeout) = entry.workflow_run_timeout
        && (now - run_started_at > timeout || timeout.is_zero() && now >= run_started_at)
    {
        return Some(WorkflowTimeoutViolation::RunTimeout);
    }

    None
}

/// Map a tracked run to the `RetryState` reported when it times out: a retrying
/// workflow surfaces `Timeout`, one without a policy surfaces `RetryPolicyNotSet`.
pub(crate) fn workflow_timeout_retry_state(entry: &WorkflowTimeoutEntry) -> RetryState {
    if entry.has_retry_policy {
        RetryState::Timeout
    } else {
        RetryState::RetryPolicyNotSet
    }
}

/// Evaluate the tracked set once and submit a timeout command for each expired
/// run, capped at `max_timeouts_per_scan`. Generic over the submit closure so the
/// same pass can be unit-tested without a live lane. An expired entry is removed
/// only after the submit resolves, so a submit failure leaves it to be retried.
pub(crate) async fn scan_workflow_timeouts_once<F, Fut>(
    tracking: &WorkflowTimeoutTrackingState,
    shard_id: Option<ShardId>,
    config: &WorkflowTimeoutScannerConfig,
    now: OffsetDateTime,
    mut submit_timeout: F,
) where
    F: FnMut(WorkflowTimeoutEntry, WorkflowTimeoutViolation, OffsetDateTime) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let entries = tracking.inner.snapshot(shard_id);
    let mut submitted = 0usize;

    for tracked in entries {
        if !tracking.inner.active(&tracked) {
            continue;
        }
        let entry = tracked.value.clone();
        if submitted >= config.max_timeouts_per_scan {
            break;
        }
        let Some(violation) = evaluate_workflow_timeout(&entry, now) else {
            continue;
        };

        match submit_timeout(entry.clone(), violation, now).await {
            Ok(()) => tracking.inner.remove_submitted(&entry.run_key, &tracked),
            Err(error) => {
                let message = error.to_string();
                // A kernel rejection means the run already advanced past the state
                // this timeout was computed against (e.g. it closed concurrently),
                // so the entry is stale: drop it rather than retry a command that
                // will keep being rejected. Other errors are transient (storage,
                // lane backpressure), so the entry is kept for the next scan.
                if message.contains("kernel rejected") {
                    tracing::debug!(
                        ?error,
                        run_key = ?entry.run_key,
                        "workflow timeout scanner timeout rejected by kernel"
                    );
                    tracking.inner.remove_submitted(&entry.run_key, &tracked);
                } else {
                    tracing::warn!(
                        ?error,
                        run_key = ?entry.run_key,
                        "workflow timeout scanner failed to submit timeout"
                    );
                }
            }
        }
        submitted += 1;
    }
}

// A periodic wake can cross the next cron boundary even when a run's timeout
// precedes it. Wake for known deadlines instead of backdating the successor to
// hide that lag. Temporal schedules WorkflowRunTimeoutTask at the expiration
// time (service/history/workflow/task_generator.go @ v1.31.0).
fn workflow_timeout_scan_delay(
    tracking: &WorkflowTimeoutTrackingState,
    now: OffsetDateTime,
    maximum: tokio::time::Duration,
) -> tokio::time::Duration {
    tracking
        .inner
        .snapshot(None)
        .iter()
        .filter(|tracked| tracking.inner.active(tracked))
        .flat_map(|tracked| {
            let entry = &tracked.value;
            [
                entry.workflow_execution_timeout.map(|timeout| {
                    entry.first_run_started_at.unwrap_or(entry.started_at) + timeout
                }),
                entry.workflow_run_timeout.map(|timeout| {
                    entry.started_at
                        + entry.workflow_start_delay.unwrap_or(Duration::ZERO)
                        + timeout
                }),
            ]
            .into_iter()
            .flatten()
        })
        // The existing evaluator uses a strict comparison for nonzero timeouts.
        .map(|deadline| deadline + Duration::NANOSECOND - now)
        // An already overdue entry may have failed its storage read. Preserve
        // the normal retry interval instead of spinning on that failed entry.
        .filter(|remaining| *remaining > Duration::ZERO)
        .filter_map(|remaining| tokio::time::Duration::try_from(remaining).ok())
        .fold(maximum, std::cmp::min)
}

/// Background loop: evaluate timeouts at approaching tracked deadlines, with
/// `scan_interval` bounding discovery of new runs and retries of failed submissions.
///
/// The active-shard set is re-read each tick (rather than captured once) so the
/// loop tracks ownership changes: a shard handed away stops being scanned and a
/// freshly swept shard starts, without restarting the task. Routing by `run_key`
/// keeps every command for a run on a single lane, preserving per-run command
/// serialization.
pub(crate) async fn run_workflow_timeout_scanner<R: tokeira_storage::RunRepository + 'static>(
    repo: Arc<R>,
    tracking: WorkflowTimeoutTrackingState,
    lanes: Vec<LaneHandle>,
    lane_count: usize,
    shard_owner: Arc<RwLock<ShardOwner>>,
    config: WorkflowTimeoutScannerConfig,
    cancel: CancellationToken,
) {
    loop {
        let delay =
            workflow_timeout_scan_delay(&tracking, OffsetDateTime::now_utc(), config.scan_interval);
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(delay) => {}
        }

        let active_shards: Vec<_> = shard_owner
            .read()
            .expect("shard_owner lock poisoned")
            .active_shards()
            .collect();
        for shard_id in active_shards {
            runtime_metrics::record_scanner_tick("workflow_timeout", shard_id.0);
            scan_workflow_timeouts_once(
                &tracking,
                Some(shard_id),
                &config,
                OffsetDateTime::now_utc(),
                |entry, violation, now| {
                    runtime_metrics::record_scanner_dispatched("workflow_timeout", shard_id.0);
                    submit_workflow_timeout(
                        repo.clone(),
                        lanes.clone(),
                        lane_count,
                        entry,
                        violation,
                        now,
                    )
                },
            )
            .await;
        }
    }
}

/// Commit an expired run and start its retry or cron successor, if any.
///
/// The scan supplies one clock instant for the timeout decision and close; the
/// successor's applied Start installs its own tracking before this returns.
pub(crate) async fn submit_workflow_timeout<R: tokeira_storage::RunRepository + 'static>(
    repo: Arc<R>,
    lanes: Vec<LaneHandle>,
    lane_count: usize,
    entry: WorkflowTimeoutEntry,
    violation: WorkflowTimeoutViolation,
    now: OffsetDateTime,
) -> Result<()> {
    let lane = pick_lane_for_run_key(&lanes, lane_count, entry.run_key).clone();
    let mut retry_selected = false;
    let mut new_execution_run_id = None;
    let mut successor_plan = None;
    // Execution expiration ends the entire chain. For a run timeout, every
    // metadata read is fallible: failure leaves the tracker eligible for a later
    // scan rather than committing a terminal close from incomplete inputs.
    if matches!(violation, WorkflowTimeoutViolation::RunTimeout)
        && let tokeira_kernel::LoadedRun::Existing(state) = repo.load_run(entry.run_key).await?
    {
        let started = Box::new(load_started_event_inputs(&*repo, entry.run_key).await?);
        let retry_backoff = state.retry_policy.as_ref().and_then(|policy| {
            let attempts_ok =
                policy.maximum_attempts == 0 || state.attempt < policy.maximum_attempts;
            let backoff = crate::runtime::workflow_task::retry_backoff(policy, state.attempt);
            let within_deadline = state.workflow_execution_timeout.is_none_or(|timeout| {
                now + backoff < state.first_run_started_at.unwrap_or(state.started_at) + timeout
            });
            (attempts_ok && within_deadline).then_some(backoff)
        });
        if let Some(backoff) = retry_backoff {
            retry_selected = true;
            new_execution_run_id = Some(tokeira_types::RunId::new());
            successor_plan = Some(SuccessorPlan::Retry {
                started,
                timing: SuccessorTiming { now, backoff },
                cause: SuccessorCause::RunTimeout,
            });
        } else if let Some(schedule) = started.cron_schedule.as_deref().filter(|s| !s.is_empty()) {
            // The schedule anchor preserves phase; processing time controls the
            // new start and minimum interval (timer_queue_active_task_executor.go:
            // 743-754; mutable_state_impl.go:2675-2695 @ v1.31.0).
            let scheduled_time =
                state.started_at + state.workflow_start_delay.unwrap_or(Duration::ZERO);
            let backoff =
                crate::schedule::cron_backoff_for_next_schedule(schedule, scheduled_time, now)?;
            new_execution_run_id = Some(tokeira_types::RunId::new());
            successor_plan = Some(SuccessorPlan::Cron {
                started,
                timing: SuccessorTiming { now, backoff },
                cause: SuccessorCause::RunTimeout,
            });
        }
    }
    lane.submit_with_successor(
        entry.run_key,
        Command::WorkflowExecutionTimedOut(WorkflowExecutionTimedOutRequest {
            timeout_type: match violation {
                WorkflowTimeoutViolation::ExecutionTimeout => WorkflowTimeoutType::ExecutionTimeout,
                WorkflowTimeoutViolation::RunTimeout => WorkflowTimeoutType::RunTimeout,
            },
            // `RetryState` reflects the RETRY policy, not the
            // cron continuation: a retry successor surfaces
            // `InProgress`, while a cron successor (or terminal)
            // reports the policy-derived state — `RetryPolicyNotSet`
            // when the cron workflow has no retry policy.
            retry_state: if retry_selected {
                RetryState::InProgress
            } else {
                workflow_timeout_retry_state(&entry)
            },
            new_execution_run_id,
            now,
        }),
        successor_plan,
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod successor_scan_tests {
    use super::*;

    fn entry(started_at: OffsetDateTime) -> WorkflowTimeoutEntry {
        WorkflowTimeoutEntry {
            run_key: RunKey::new(),
            shard_id: ShardId(0),
            workflow_execution_timeout: None,
            workflow_run_timeout: Some(Duration::seconds(5)),
            started_at,
            workflow_start_delay: Some(Duration::seconds(3)),
            first_run_started_at: None,
            has_retry_policy: false,
        }
    }

    #[test]
    fn successor_scan_wakes_before_cron_boundary() {
        let start = OffsetDateTime::UNIX_EPOCH + Duration::milliseconds(20100);
        let tracking = WorkflowTimeoutTrackingState::default();
        let entry = entry(start);
        tracking.insert(entry.clone());
        let now = start + Duration::milliseconds(7800);
        let wake = now
            + Duration::try_from(workflow_timeout_scan_delay(
                &tracking,
                now,
                tokio::time::Duration::from_secs(1),
            ))
            .unwrap();
        assert_eq!(
            evaluate_workflow_timeout(&entry, wake),
            Some(WorkflowTimeoutViolation::RunTimeout)
        );
        let execution_time = start + Duration::seconds(3);
        let backoff =
            crate::schedule::cron_backoff_for_next_schedule("@every 3s", execution_time, wake)
                .unwrap();
        // A five-second timeout resumes the three-second schedule after six
        // seconds; a full polling interval here would add 800 ms of drift.
        assert!(wake + backoff - execution_time < Duration::milliseconds(6010));
    }

    #[test]
    fn successor_scan_bounds_idle_and_failed_retries() {
        let now = OffsetDateTime::UNIX_EPOCH + Duration::seconds(20);
        let tracking = WorkflowTimeoutTrackingState::default();
        let interval = tokio::time::Duration::from_secs(1);
        assert_eq!(
            workflow_timeout_scan_delay(&tracking, now, interval),
            interval
        );
        tracking.insert(entry(now - Duration::seconds(10)));
        assert_eq!(
            workflow_timeout_scan_delay(&tracking, now, interval),
            interval
        );
        let mut future = entry(now);
        future.first_run_started_at = Some(now - Duration::seconds(5));
        future.workflow_execution_timeout = Some(Duration::milliseconds(5250));
        tracking.insert(future);
        assert_eq!(
            workflow_timeout_scan_delay(&tracking, now, interval),
            tokio::time::Duration::from_millis(250) + tokio::time::Duration::from_nanos(1)
        );
    }
}
