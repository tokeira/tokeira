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
//! [`crate::recovery::sweep_shard`] whenever this node takes over a shard, so
//! losing it to a crash or failover costs a rebuild, never correctness. Entries
//! are tagged with their `ShardId` so a handoff can drop exactly the runs this
//! node no longer owns via [`WorkflowTimeoutTrackingState::remove_all_for_shard`]
//! without disturbing co-resident shards.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use anyhow::Result;
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{Command, RetryState, WorkflowExecutionTimedOutRequest, WorkflowTimeoutType};
use tokeira_types::{RunKey, ShardId};
use tokio_util::sync::CancellationToken;

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
    /// Start of the *current* run — the anchor for the run timeout.
    pub started_at: OffsetDateTime,
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
#[derive(Clone, Default)]
pub struct WorkflowTimeoutTrackingState {
    inner: Arc<Mutex<HashMap<RunKey, WorkflowTimeoutEntry>>>,
}

impl WorkflowTimeoutTrackingState {
    /// Begin tracking a run, replacing any existing entry for the same key.
    pub fn insert(&self, entry: WorkflowTimeoutEntry) {
        self.inner.lock().unwrap().insert(entry.run_key, entry);
    }

    /// Stop tracking a run. Called when the run closes or its timeout fires, so a
    /// closed run is never repeatedly evaluated.
    pub fn remove(&self, run_key: RunKey) {
        self.inner.lock().unwrap().remove(&run_key);
    }

    /// Drop every entry for a shard. Invoked on shard handoff so this node stops
    /// evaluating timeouts for runs it no longer owns; the new owner rebuilds them
    /// from durable state during its own sweep.
    pub fn remove_all_for_shard(&self, shard_id: ShardId) {
        self.inner
            .lock()
            .unwrap()
            .retain(|_, entry| entry.shard_id != shard_id);
    }

    /// Snapshot all tracked entries. The scanner copies out under the lock and
    /// evaluates without holding it, so lane inserts/removes are never blocked on
    /// timeout evaluation.
    pub fn snapshot(&self) -> Vec<WorkflowTimeoutEntry> {
        self.inner.lock().unwrap().values().cloned().collect()
    }

    /// Snapshot only the entries owned by `shard_id`, used by the per-shard scan.
    pub fn snapshot_for_shard(&self, shard_id: ShardId) -> Vec<WorkflowTimeoutEntry> {
        self.inner
            .lock()
            .unwrap()
            .values()
            .filter(|entry| entry.shard_id == shard_id)
            .cloned()
            .collect()
    }
}

/// Tuning for the workflow timeout scanner loop.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowTimeoutScannerConfig {
    /// Delay between scans of the tracking set.
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

    if let Some(timeout) = entry.workflow_run_timeout
        && (now - entry.started_at > timeout || timeout.is_zero() && now >= entry.started_at)
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
    mut submit_timeout: F,
) where
    F: FnMut(WorkflowTimeoutEntry, WorkflowTimeoutViolation, OffsetDateTime) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let now = OffsetDateTime::now_utc();
    let entries = match shard_id {
        Some(shard_id) => tracking.snapshot_for_shard(shard_id),
        None => tracking.snapshot(),
    };
    let mut submitted = 0usize;

    for entry in entries {
        if submitted >= config.max_timeouts_per_scan {
            break;
        }
        let Some(violation) = evaluate_workflow_timeout(&entry, now) else {
            continue;
        };

        match submit_timeout(entry.clone(), violation, now).await {
            Ok(()) => tracking.remove(entry.run_key),
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
                    tracking.remove(entry.run_key);
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

/// Background loop: every `scan_interval`, evaluate timeouts for each shard this
/// node currently owns and route any firings to the run's lane.
///
/// The active-shard set is re-read each tick (rather than captured once) so the
/// loop tracks ownership changes: a shard handed away stops being scanned and a
/// freshly swept shard starts, without restarting the task. Routing by `run_key`
/// keeps every command for a run on a single lane, preserving per-run command
/// serialization.
pub(crate) async fn run_workflow_timeout_scanner(
    tracking: WorkflowTimeoutTrackingState,
    lanes: Vec<LaneHandle>,
    lane_count: usize,
    shard_owner: Arc<RwLock<ShardOwner>>,
    config: WorkflowTimeoutScannerConfig,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(config.scan_interval) => {}
        }

        let active_shards: Vec<_> = shard_owner.read().unwrap().active_shards().collect();
        for shard_id in active_shards {
            runtime_metrics::record_scanner_tick("workflow_timeout", shard_id.0);
            scan_workflow_timeouts_once(
                &tracking,
                Some(shard_id),
                &config,
                |entry, violation, now| {
                    runtime_metrics::record_scanner_dispatched("workflow_timeout", shard_id.0);
                    let lane = pick_lane_for_run_key(&lanes, lane_count, entry.run_key).clone();
                    async move {
                        lane.submit(
                            entry.run_key,
                            Command::WorkflowExecutionTimedOut(WorkflowExecutionTimedOutRequest {
                                timeout_type: match violation {
                                    WorkflowTimeoutViolation::ExecutionTimeout => {
                                        WorkflowTimeoutType::ExecutionTimeout
                                    }
                                    WorkflowTimeoutViolation::RunTimeout => {
                                        WorkflowTimeoutType::RunTimeout
                                    }
                                },
                                retry_state: workflow_timeout_retry_state(&entry),
                                now,
                            }),
                        )
                        .await
                        .map(|_| ())
                    }
                },
            )
            .await;
        }
    }
}
