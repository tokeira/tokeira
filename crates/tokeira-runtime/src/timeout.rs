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
    mut submit_timeout: F,
) where
    F: FnMut(WorkflowTimeoutEntry, WorkflowTimeoutViolation, OffsetDateTime) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let now = OffsetDateTime::now_utc();
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

/// Background loop: every `scan_interval`, evaluate timeouts for each shard this
/// node currently owns and route any firings to the run's lane.
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
            runtime_metrics::record_scanner_tick("workflow_timeout", shard_id.0);
            scan_workflow_timeouts_once(
                &tracking,
                Some(shard_id),
                &config,
                |entry, violation, now| {
                    runtime_metrics::record_scanner_dispatched("workflow_timeout", shard_id.0);
                    let lane = pick_lane_for_run_key(&lanes, lane_count, entry.run_key).clone();
                    let repo = repo.clone();
                    let lanes = lanes.clone();
                    async move {
                        // A RUN timeout with retry attempts remaining continues
                        // the retry chain: the close carries the successor run
                        // id and the successor starts as a derived effect of
                        // the committed close (retryWorkflow on run-timeout,
                        // timer_queue_active_task_executor.go:713-796 @
                        // v1.31.0). Execution timeouts never retry — the
                        // chain's own deadline has passed.
                        let mut retry_successor = None;
                        if matches!(violation, WorkflowTimeoutViolation::RunTimeout)
                            && entry.has_retry_policy
                            && let Ok(tokeira_kernel::LoadedRun::Existing(state)) =
                                repo.load_run(entry.run_key).await
                            && let Some(policy) = state.retry_policy.clone()
                        {
                            // `maximum_attempts == 0` means unlimited.
                            let attempts_ok = policy.maximum_attempts == 0
                                || state.attempt < policy.maximum_attempts;
                            let backoff = crate::runtime::workflow_task::retry_backoff(
                                &policy,
                                state.attempt,
                            );
                            // Next attempt must fit before the execution-level
                            // deadline, anchored on the chain's first run.
                            let within_deadline = match state.workflow_execution_timeout {
                                Some(execution_timeout) => {
                                    let anchor =
                                        state.first_run_started_at.unwrap_or(state.started_at);
                                    now + backoff < anchor + execution_timeout
                                }
                                None => true,
                            };
                            if attempts_ok && within_deadline {
                                retry_successor =
                                    Some((state, policy, tokeira_types::RunId::new()));
                            }
                        }
                        // A RUN timeout on a cron workflow with no retry
                        // continuation restarts the cron schedule: the next cron
                        // run carries the timeout as its LastError, the same way
                        // the failure path continues cron. The cron schedule and
                        // input live on the run's start event.
                        let mut cron_successor = None;
                        if retry_successor.is_none()
                            && matches!(violation, WorkflowTimeoutViolation::RunTimeout)
                            && let Ok(events) = repo.read_history(entry.run_key, 0, 1).await
                            && let Some(
                                tokeira_kernel::HistoryEventKind::WorkflowExecutionStarted {
                                    cron_schedule: Some(cron),
                                    input,
                                    ..
                                }
                                | tokeira_kernel::HistoryEventKind::WorkflowExecutionStartedV2 {
                                    cron_schedule: Some(cron),
                                    input,
                                    ..
                                },
                            ) = events.first().map(|event| &event.kind)
                            && !cron.is_empty()
                            && let Ok(tokeira_kernel::LoadedRun::Existing(state)) =
                                repo.load_run(entry.run_key).await
                        {
                            cron_successor = Some((
                                state,
                                cron.clone(),
                                input.clone(),
                                tokeira_types::RunId::new(),
                            ));
                        }
                        let new_execution_run_id = retry_successor
                            .as_ref()
                            .map(|(_, _, run_id)| *run_id)
                            .or_else(|| cron_successor.as_ref().map(|(_, _, _, run_id)| *run_id));
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
                                // `RetryState` reflects the RETRY policy, not the
                                // cron continuation: a retry successor surfaces
                                // `InProgress`, while a cron successor (or terminal)
                                // reports the policy-derived state — `RetryPolicyNotSet`
                                // when the cron workflow has no retry policy.
                                retry_state: if retry_successor.is_some() {
                                    RetryState::InProgress
                                } else {
                                    workflow_timeout_retry_state(&entry)
                                },
                                new_execution_run_id,
                                now,
                            }),
                        )
                        .await?;
                        if let Some((state, policy, new_run_id)) = retry_successor {
                            start_timeout_retry_successor(
                                repo,
                                &lanes,
                                lane_count,
                                entry.run_key,
                                state,
                                policy,
                                new_run_id,
                            )
                            .await;
                        } else if let Some((state, cron, input, new_run_id)) = cron_successor {
                            // Anchor the cron continuation on the precise run-timeout
                            // deadline (execution start + run timeout) rather than the
                            // periodic scan instant, so the next fire lands on the
                            // schedule's phase free of up-to-`scan_interval` jitter.
                            let run_timeout_deadline = entry.started_at
                                + entry.workflow_start_delay.unwrap_or(Duration::ZERO)
                                + entry.workflow_run_timeout.unwrap_or(Duration::ZERO);
                            start_timeout_cron_successor(
                                &lanes,
                                lane_count,
                                state,
                                cron,
                                input,
                                new_run_id,
                                run_timeout_deadline,
                            )
                            .await;
                        }
                        Ok(())
                    }
                },
            )
            .await;
        }
    }
}

/// Start the retry successor after a run-timeout close committed — the same
/// derived-effect posture as the WFT-completion failure path (`Req 2.2`): the
/// close stands even if the successor start fails, and a re-driven close
/// dedupes on the deterministic request id.
#[allow(clippy::too_many_arguments)]
async fn start_timeout_retry_successor<R: tokeira_storage::RunRepository + 'static>(
    repo: Arc<R>,
    lanes: &[LaneHandle],
    lane_count: usize,
    predecessor_run_key: RunKey,
    state: tokeira_kernel::WorkflowState,
    policy: tokeira_types::RetryPolicy,
    new_run_id: tokeira_types::RunId,
) {
    let (input, started_versioning_info) = match repo
        .read_history(predecessor_run_key, 0, 1)
        .await
        .ok()
        .and_then(|events| events.first().map(|event| event.kind.clone()))
    {
        Some(tokeira_kernel::HistoryEventKind::WorkflowExecutionStarted {
            input,
            versioning_info,
            ..
        })
        | Some(tokeira_kernel::HistoryEventKind::WorkflowExecutionStartedV2 {
            input,
            versioning_info,
            ..
        }) => (input, versioning_info),
        _ => (tokeira_types::Payloads::default(), None),
    };
    let start_request = crate::runtime::workflow_task::build_retry_successor_start(
        &state,
        started_versioning_info.as_ref(),
        &policy,
        input,
        new_run_id,
    );
    let successor_run_key = start_request.run_key;
    let successor_lane = pick_lane_for_run_key(lanes, lane_count, successor_run_key).clone();
    match successor_lane
        .submit(successor_run_key, Command::Start(start_request))
        .await
    {
        // The lane installs timeout tracking before replying. Installing again
        // here could overwrite recovery performed after this awaited reply.
        Ok(
            tokeira_storage::CommitResult::Applied { .. }
            | tokeira_storage::CommitResult::Duplicate,
        ) => {}
        Ok(other) => {
            tracing::warn!(
                ?other,
                predecessor_run_key = ?predecessor_run_key,
                "timeout retry successor start not applied"
            );
        }
        Err(error) => {
            tracing::warn!(
                ?error,
                predecessor_run_key = ?predecessor_run_key,
                "failed to start timeout retry successor"
            );
        }
    }
}

/// Start the cron successor after a cron run's RUN timeout committed — the same
/// derived-effect posture as the retry successor: the close stands even if the
/// successor start fails, and a re-driven close dedupes on the deterministic
/// `cron-timeout:*` request id.
#[allow(clippy::too_many_arguments)]
async fn start_timeout_cron_successor(
    lanes: &[LaneHandle],
    lane_count: usize,
    state: tokeira_kernel::WorkflowState,
    cron_schedule: String,
    input: tokeira_types::Payloads,
    new_run_id: tokeira_types::RunId,
    now: OffsetDateTime,
) {
    let start_request = match crate::runtime::workflow_task::build_cron_successor_start(
        &state,
        cron_schedule,
        input,
        new_run_id,
        now,
        now,
        Some(crate::runtime::workflow_task::workflow_run_timeout_failure()),
        state.last_completion_result.clone(),
    ) {
        Ok(request) => request,
        Err(error) => {
            tracing::warn!(?error, run_id = ?state.run_id, "invalid cron schedule on timeout continuation");
            return;
        }
    };
    let successor_run_key = start_request.run_key;
    let successor_lane = pick_lane_for_run_key(lanes, lane_count, successor_run_key).clone();
    match successor_lane
        .submit(successor_run_key, Command::Start(start_request))
        .await
    {
        // The lane installs timeout tracking before replying. Installing again
        // here could overwrite recovery performed after this awaited reply.
        Ok(
            tokeira_storage::CommitResult::Applied { .. }
            | tokeira_storage::CommitResult::Duplicate,
        ) => {}
        Ok(other) => {
            tracing::warn!(
                ?other,
                ?new_run_id,
                "timeout cron successor start not applied"
            );
        }
        Err(error) => {
            tracing::warn!(
                ?error,
                ?new_run_id,
                "failed to start timeout cron successor"
            );
        }
    }
}
