//! Background timer scanner and lane-routing helpers.
//!
//! Two responsibilities live here. First, the background timer scanner:
//! looks ahead in *durable* storage and wakes at the next deadline to submit a
//! `TimerDue` command to the owning lane. The scanner holds no authoritative
//! state of its own — storage is the source of truth for which timers are due —
//! so full scans recover a missed notification and duplicated delivery is safe after
//! crash or failover. A due row whose run has closed or no longer exists can
//! never fire, so the scanner deletes it rather than resubmit it on every pass
//! at the front of the shard's oldest-first batch. A page of due timers is
//! submitted to its lanes concurrently, each lane's share in fire order
//! (`submit_due_page`). Second, the deterministic run-to-lane routing
//! (`lane_index_for_run_key`, `pick_lane_for_run_key`) used by every command
//! submission path in the runtime, not just timers.
//!
//! Routing invariant: every command for a given `run_key` must land on the same
//! lane, because a lane serializes each run's commands and that serialization
//! is what keeps per-run transitions ordered. The mapping is a pure
//! function of the run key, so all producers (poll handlers, sweepers, the
//! timeout scanners) agree on the lane without coordination.

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    sync::{Arc, Mutex, RwLock},
    task::Poll,
};

use anyhow::Result;
use time::OffsetDateTime;
use tokeira_clock::Clock;
use tokeira_kernel::{
    Command, Reject, TimerDueRequest, WORKFLOW_START_DELAY_TIMER_ID,
    WorkflowStartDelayElapsedRequest,
};
use tokeira_storage::{DueTimer, RunRepository, StaleTimer};
use tokeira_types::{RunKey, ShardId, dsql_spread_uuid};
use tokio_util::sync::CancellationToken;

use crate::{
    deadline_wake::{DeadlineWake, FullPass},
    lane::{KernelRejected, LaneHandle},
    metrics as runtime_metrics,
    shard::{ShardAcquisition, ShardOwner},
};

/// Configuration knobs for the background timer scanner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimerScannerConfig {
    /// Maximum interval between full storage scans; deadlines wake sooner.
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

/// The command that fires `due`: the workflow start delay elapses through
/// `WorkflowStartDelayElapsed`, every other timer through `TimerDue`.
///
/// v1.31.0 keeps the two apart: the start delay is a workflow backoff timer
/// that schedules the first workflow task (`executeWorkflowBackoffTimerTask`),
/// a user timer fires through `executeUserTimerTimeoutTask`
/// (timer_queue_active_task_executor.go:137, 462 @ v1.31.0). A `TimerDue` for
/// the start delay would record a `TimerFired` for a timer no workflow started.
pub(crate) fn command_for_due_timer(due: DueTimer, fired_at: OffsetDateTime) -> Command {
    if due.timer_id == WORKFLOW_START_DELAY_TIMER_ID {
        Command::WorkflowStartDelayElapsed(WorkflowStartDelayElapsedRequest { fired_at })
    } else {
        Command::TimerDue(TimerDueRequest {
            timer_id: due.timer_id,
            fired_at,
        })
    }
}

/// Why the kernel rejected a due timer, when its run has closed or doesn't
/// exist.
///
/// Such a timer can never fire. v1.31.0 discards the task when it runs: the
/// timer executor answers `ErrWorkflowExecutionNotFound` or
/// `ErrWorkflowCompleted`, both NotFound
/// (timer_queue_active_task_executor.go:154-157, 178-181 @ v1.31.0), and the
/// queue completes a NotFound task instead of retrying it
/// (queues/executable.go:401 @ v1.31.0). A missing run may be a reset's
/// successor still being materialized, whose timer rows precede its state
/// (`bounded-bulk-writes`), so storage keeps those rows.
fn rejected_because_run_is_gone(error: &anyhow::Error) -> Option<StaleTimer> {
    error
        .downcast_ref::<KernelRejected>()
        .and_then(|rejected| match rejected.0 {
            Reject::RunClosed(_) => Some(StaleTimer::RunClosed),
            Reject::MissingRun => Some(StaleTimer::RunMissing),
            _ => None,
        })
}

/// Settle a due timer whose submission failed, as every pass over due timers
/// does.
///
/// A rejection because the run has closed or is gone deletes the row if it is
/// still the row that was read, since that timer can never fire. Any other
/// rejection or failure is logged and the row stays for a later pass. Returns
/// whether a row was deleted.
pub(crate) async fn settle_failed_due_timer<R>(
    repo: &R,
    shard_id: Option<ShardId>,
    due: &DueTimer,
    error: &anyhow::Error,
) -> bool
where
    R: RunRepository + ?Sized,
{
    if let Some(reason) = rejected_because_run_is_gone(error) {
        return delete_stale_due_timer(repo, due, reason).await;
    }
    if error.to_string().contains("kernel rejected") {
        tracing::debug!(
            ?error,
            ?shard_id,
            run_key = ?due.run_key,
            timer_id = due.timer_id,
            "due timer rejected by kernel"
        );
    } else {
        tracing::warn!(
            ?error,
            ?shard_id,
            run_key = ?due.run_key,
            timer_id = due.timer_id,
            "failed to submit due timer"
        );
    }
    false
}

/// Delete the row of a due timer whose run is gone, if it is still the row
/// the scan read. Returns whether a row was deleted.
async fn delete_stale_due_timer<R>(repo: &R, due: &DueTimer, reason: StaleTimer) -> bool
where
    R: RunRepository + ?Sized,
{
    match repo.delete_due_timer_if_matches(due, reason).await {
        Ok(deleted) => deleted,
        Err(error) => {
            tracing::warn!(
                ?error,
                run_key = ?due.run_key,
                timer_id = due.timer_id,
                "timer scanner failed to delete a due timer of a closed run"
            );
            false
        }
    }
}

/// The identity a failed submission is remembered by until the next full pass.
type FailedTimer = (OffsetDateTime, RunKey, String);

/// What submitting one page of due timers left to the caller.
#[derive(Debug, Default)]
struct DuePage {
    failed: Vec<FailedTimer>,
    deleted: usize,
}

/// Submit one page of due timers: different lanes concurrently, each lane's
/// share in page order.
///
/// Rows are grouped by `lane_index_for_run_key`, the routing every submit path
/// uses, so each group holds exactly the rows whose commands land on one lane.
/// Groups proceed concurrently, so an idle lane does not wait behind another
/// lane's commit. Within a group each submission awaits its reply before the
/// next one starts. Submitting a group's whole share at once would occupy more
/// of the lane's 1,024-command budget (`LANE_BUDGET`) that worker completions
/// and polls share, and it would hand the lane a run's later timer before the
/// earlier one had been settled. Awaiting keeps one run's timers in fire
/// order, keeps the scanner to at most one queued command per lane, and
/// bounds the work in flight by the lane count and the page size, the two
/// limits that already exist.
/// What it gives up is same-run coalescing in the lane, which a page of
/// distinct runs' timers would rarely reach.
///
/// `admit` runs just before each submission; once it answers `false` (the
/// shard's acquisition was lost) every group stops, so nothing more is
/// submitted. Each row belongs to exactly one group and each group visits a
/// row once, so a timer is submitted at most once however the replies
/// interleave. A failed submission is settled in its group before that lane's
/// next submission and is reported in `failed`.
async fn submit_due_page<R, A, F, Fut>(
    repo: &R,
    shard_id: Option<ShardId>,
    rows: Vec<DueTimer>,
    lane_count: usize,
    fired_at: OffsetDateTime,
    admit: A,
    submit: &F,
) -> DuePage
where
    R: RunRepository + ?Sized,
    A: Fn() -> bool,
    F: Fn(DueTimer, OffsetDateTime) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let mut groups = BTreeMap::<usize, Vec<DueTimer>>::new();
    for due in rows {
        groups
            .entry(lane_index_for_run_key(due.run_key, lane_count))
            .or_default()
            .push(due);
    }
    let admit = &admit;
    let mut lanes: Vec<_> = groups
        .into_values()
        .map(|rows| {
            Box::pin(async move {
                let mut page = DuePage::default();
                for due in rows {
                    if !admit() {
                        break;
                    }
                    if let Err(error) = submit(due.clone(), fired_at).await {
                        page.deleted += usize::from(
                            settle_failed_due_timer(repo, shard_id, &due, &error).await,
                        );
                        page.failed.push((due.fire_at, due.run_key, due.timer_id));
                    }
                }
                page
            })
        })
        .collect();
    // The groups share this task rather than spawning: the submit closure may
    // borrow from its caller, and the work is waiting on lane replies, not
    // computing. Polling every unfinished group on each wake costs at most one
    // poll per lane.
    let mut page = DuePage::default();
    std::future::poll_fn(|cx| {
        lanes.retain_mut(|lane| match lane.as_mut().poll(cx) {
            Poll::Ready(done) => {
                page.failed.extend(done.failed);
                page.deleted += done.deleted;
                false
            }
            Poll::Pending => true,
        });
        if lanes.is_empty() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    page
}

// These helpers have no lanes to route to. One group submits their page in
// order through the same path, settlement and failure accounting as the
// scanner.
#[allow(dead_code)]
pub(crate) async fn scan_due_timers_once<R, F, Fut>(
    repo: &R,
    config: &TimerScannerConfig,
    fired_at: OffsetDateTime,
    submit_due_timer: F,
) where
    R: RunRepository + ?Sized,
    F: Fn(DueTimer, OffsetDateTime) -> Fut,
    Fut: Future<Output = Result<()>>,
{
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
    submit_due_page(
        repo,
        None,
        due_timers,
        1,
        fired_at,
        || true,
        &submit_due_timer,
    )
    .await;
}

#[cfg(test)]
pub(crate) async fn scan_due_timers_once_for_shard<R, F, Fut>(
    repo: &R,
    shard_id: ShardId,
    config: &TimerScannerConfig,
    fired_at: OffsetDateTime,
    submit_due_timer: F,
) where
    R: RunRepository + ?Sized,
    F: Fn(DueTimer, OffsetDateTime) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let due_timers = match repo
        .list_due_timers_for_shard(shard_id, fired_at, None, config.max_timers_per_scan)
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
    let page = submit_due_page(
        repo,
        Some(shard_id),
        due_timers,
        1,
        fired_at,
        || true,
        &submit_due_timer,
    )
    .await;
    if page.deleted > 0 {
        runtime_metrics::record_scanner_stale_deleted("timer", shard_id.0, page.deleted);
    }
}

/// Volatile per-shard invalidations shared by commit paths and acquisition.
/// Storage remains authoritative; losing a notification costs at most a full pass.
#[derive(Clone, Debug, Default)]
pub(crate) struct TimerWake {
    wake: DeadlineWake,
    changed: Arc<Mutex<BTreeMap<ShardId, OffsetDateTime>>>,
}

impl TimerWake {
    /// Announce only visible, committed timers. Preparation completion must precede
    /// this call because listings hide timers belonging to an unfinished publication.
    pub(crate) fn changed(&self, shard: ShardId, due: OffsetDateTime) {
        let mut changed = self.changed.lock().expect("timer wake lock poisoned");
        changed
            .entry(shard)
            .and_modify(|old| *old = (*old).min(due))
            .or_insert(due);
        self.wake.changed(Some(due));
    }

    /// Reconstruct the wake after a prepared run becomes visible to listings.
    /// This also repairs a lost publication reply without trusting its dispatch ops.
    pub(crate) fn published(&self, shard: ShardId, state: &tokeira_kernel::WorkflowState) {
        if let Some(due) = state.timers.values().map(|timer| timer.fire_at).min() {
            self.changed(shard, due);
        }
    }
}

/// Capture timer hints before consuming a transition; announce them only on Applied.
pub(crate) fn earliest_timer_write(ops: &[tokeira_kernel::TimerOp]) -> Option<OffsetDateTime> {
    ops.iter()
        .filter_map(|op| match op {
            tokeira_kernel::TimerOp::Upsert(timer) => Some(timer.fire_at),
            tokeira_kernel::TimerOp::Delete { .. } => None,
        })
        .min()
}

#[derive(Debug)]
struct TimerScan {
    acquisition: ShardAcquisition,
    after: Option<DueTimer>,
    next: Option<OffsetDateTime>,
    restart: Option<OffsetDateTime>,
    failed: BTreeSet<FailedTimer>,
}

/// Submit durable timers at their deadlines, listing only shards that need work.
/// Full passes reconcile missed notifications; capped pages continue immediately.
pub(crate) async fn run_timer_scanner<R>(
    repo: Arc<R>,
    lanes: Vec<LaneHandle>,
    lane_count: usize,
    shard_owner: Arc<RwLock<ShardOwner>>,
    config: TimerScannerConfig,
    cancel: CancellationToken,
    clock: Arc<dyn Clock>,
) where
    R: RunRepository + 'static,
{
    run_timer_scanner_with_submit(
        repo,
        shard_owner,
        config,
        cancel,
        clock,
        lane_count,
        |due, fired_at| {
            let lane = pick_lane_for_run_key(&lanes, lane_count, due.run_key).clone();
            async move {
                lane.submit(due.run_key, command_for_due_timer(due, fired_at))
                    .await
                    .map(|_| ())
            }
        },
    )
    .await;
}

/// Keep storage paging and wake planning identical in production and controlled
/// submission tests; the closure owns the authoritative lane transition.
///
/// `lane_count` must be the count `submit` routes with, so a page's rows are
/// grouped by the lane their commands land on (see `submit_due_page`).
pub(crate) async fn run_timer_scanner_with_submit<R, F, Fut>(
    repo: Arc<R>,
    owner: Arc<RwLock<ShardOwner>>,
    config: TimerScannerConfig,
    cancel: CancellationToken,
    clock: Arc<dyn Clock>,
    lane_count: usize,
    submit: F,
) where
    R: RunRepository + 'static,
    F: Fn(DueTimer, OffsetDateTime) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let wake = owner
        .read()
        .expect("shard owner lock poisoned")
        .timer_wake
        .clone();
    let mut full = FullPass::new(config.scan_interval, clock.now());
    let mut scans = BTreeMap::<ShardId, TimerScan>::new();
    loop {
        let now = clock.now();
        let revision = wake.wake.begin_plan();
        {
            // Ownership precedes the notification lock, as on commit/activation.
            // No storage I/O holds either lock.
            let owner = owner.read().expect("shard owner lock poisoned");
            scans.retain(|_, scan| owner.acquisition_active(&scan.acquisition));
            for shard in owner.active_shards() {
                scans.entry(shard).or_insert_with(|| TimerScan {
                    acquisition: owner.acquisition(shard).expect("active acquisition exists"),
                    after: None,
                    next: Some(now),
                    restart: None,
                    failed: BTreeSet::new(),
                });
            }
            for (shard, due) in
                std::mem::take(&mut *wake.changed.lock().expect("timer wake lock poisoned"))
            {
                if let Some(scan) = scans.get_mut(&shard) {
                    scan.restart = Some(scan.restart.map_or(due, |old| old.min(due)));
                }
            }
        }
        if full.start(now) {
            for scan in scans.values_mut() {
                scan.after = None;
                scan.next = Some(now);
                scan.failed.clear();
            }
        }
        let next = scans
            .values()
            .flat_map(|scan| scan.next.into_iter().chain(scan.restart))
            .min();
        let delay = next.map_or(full.remaining(now), |due| {
            full.remaining(now)
                .min(std::time::Duration::try_from(due - now).unwrap_or_default())
        });
        // Stored dates can exceed the monotonic clock's range. A bounded
        // segment only replans; due checks still use the unchanged node clock.
        let delay = wake.wake.plan(
            now,
            revision,
            delay.min(std::time::Duration::from_secs(86_400)),
        );
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = wake.wake.notify.notified() => continue,
            _ = tokio::time::sleep(delay) => {}
        }
        let now = clock.now();
        wake.wake.start_scan(now);
        // A full-pass deadline is handled at the top so it resets every cursor.
        for (shard, scan) in &mut scans {
            if !scan
                .next
                .into_iter()
                .chain(scan.restart)
                .any(|due| due <= now)
            {
                continue;
            }
            if !owner
                .read()
                .expect("shard owner lock poisoned")
                .acquisition_active(&scan.acquisition)
            {
                continue;
            }
            if scan.restart.is_some_and(|due| due <= now) {
                // A new timer can sort before the page cursor, including mid-backlog.
                // Failed identities survive this rewind until the next full pass.
                scan.after = None;
                scan.restart = None;
            }
            scan.next = None;
            runtime_metrics::record_scanner_tick("timer", shard.0);
            let horizon = tokeira_kernel::deadline_after(
                now,
                time::Duration::try_from(config.scan_interval).unwrap_or(time::Duration::MAX),
            );
            let rows = match repo
                .list_due_timers_for_shard(
                    *shard,
                    horizon,
                    scan.after.as_ref(),
                    config.max_timers_per_scan,
                )
                .await
            {
                Ok(rows) => rows,
                Err(error) => {
                    tracing::warn!(?error, "timer look-ahead failed");
                    continue;
                }
            };
            let full_page =
                config.max_timers_per_scan > 0 && rows.len() == config.max_timers_per_scan;
            let mut future = false;
            let mut due_rows = Vec::new();
            for due in rows {
                if due.fire_at > now {
                    scan.next = Some(due.fire_at);
                    future = true;
                    break;
                }
                // The cursor passes every due row now, before any submission
                // finishes, so out-of-order replies cannot move it backwards.
                scan.after = Some(due.clone());
                if !scan
                    .failed
                    .contains(&(due.fire_at, due.run_key, due.timer_id.clone()))
                {
                    due_rows.push(due);
                }
            }
            let acquisition = &scan.acquisition;
            let page = submit_due_page(
                repo.as_ref(),
                Some(*shard),
                due_rows,
                lane_count,
                now,
                || {
                    let active = owner
                        .read()
                        .expect("shard owner lock poisoned")
                        .acquisition_active(acquisition);
                    if active {
                        runtime_metrics::record_scanner_dispatched("timer", shard.0);
                    }
                    active
                },
                &submit,
            )
            .await;
            scan.failed.extend(page.failed);
            if full_page && !future {
                scan.next = Some(now);
            }
            if page.deleted > 0 {
                runtime_metrics::record_scanner_stale_deleted("timer", shard.0, page.deleted);
            }
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

    // A lane refusal is transient: the due timer's row stays for a later pass.
    #[tokio::test]
    async fn a_refused_due_timer_keeps_its_row() {
        let repo = tokeira_storage::InMemoryStore::default();
        let due = DueTimer {
            run_key: RunKey::new(),
            timer_id: "timer".into(),
            fire_at: OffsetDateTime::UNIX_EPOCH,
        };
        for refusal in [
            anyhow::Error::new(crate::WorkflowBusy),
            anyhow::Error::new(crate::LaneOverloaded),
        ] {
            assert!(rejected_because_run_is_gone(&refusal).is_none());
            assert!(!settle_failed_due_timer(&repo, None, &due, &refusal).await);
        }
    }

    // A timer rejected because its run is missing keeps that reason, so storage
    // can keep the row of a run still being materialized (`bounded-bulk-writes`).
    #[test]
    fn a_stale_timers_reason_is_the_kernels() {
        let rejection = |reject| anyhow::Error::new(KernelRejected(reject));
        assert_eq!(
            rejected_because_run_is_gone(&rejection(Reject::MissingRun)),
            Some(StaleTimer::RunMissing)
        );
        assert_eq!(
            rejected_because_run_is_gone(&rejection(Reject::RunClosed(
                tokeira_types::ExecutionStatus::Completed
            ))),
            Some(StaleTimer::RunClosed)
        );
        assert_eq!(
            rejected_because_run_is_gone(&rejection(Reject::UnknownTimer("t".into()))),
            None
        );
    }

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
            fire_at: fired_at,
        };
        let user_timer = DueTimer {
            run_key,
            timer_id: "timer-1".to_string(),
            fire_at: fired_at,
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

#[cfg(test)]
mod deadline_tests;
