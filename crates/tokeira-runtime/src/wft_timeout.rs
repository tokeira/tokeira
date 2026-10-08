//! Workflow-task deadline tracking reconstructed from authoritative state.
//!
//! Both unstarted normal schedule-to-start deadlines and started start-to-close
//! deadlines are volatile caches. Recovery retains their absolute deadlines even
//! when sticky affinity has been cleared. Acquisition identities prevent a stale
//! sweep from reinstalling authority, and revision-checked retirement prevents an
//! awaited timeout result from removing a worker start's replacement entry.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use anyhow::Result;
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{Command, WorkflowTaskTimedOutRequest, WorkflowTaskTimeoutType};
use tokeira_observability::OutcomeLabel;
use tokeira_types::{LogicalTaskSeq, RunKey, ShardId};
use tokio_util::sync::CancellationToken;

use crate::{
    lane::LaneHandle,
    metrics as runtime_metrics,
    scanner::pick_lane_for_run_key,
    shard::{ShardAcquisition, ShardOwner},
};

/// Which workflow-task deadline an entry watches.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WftTimeoutKind {
    /// A STARTED task's start-to-close window (anchor = `started_at`).
    StartToClose,
    /// A sticky-dispatched UNSTARTED task's schedule-to-start window
    /// (anchor plus duration = the durable absolute deadline). Fires
    /// `WorkflowTaskTimedOut(SCHEDULE_TO_START)`, which clears sticky and
    /// reschedules on the normal queue.
    ScheduleToStart,
}

/// One workflow task being watched for a deadline.
///
/// `logical_seq` (and, for start-to-close, `started_event_id`) identify the
/// exact attempt so the kernel can fence a timeout submitted against a task
/// that has since been superseded. For `ScheduleToStart` entries
/// `started_event_id` is 0; recovery stores the absolute deadline in `started_at`
/// with a zero duration, avoiding deadline extension after restart. The map
/// stays keyed by run, so the start-to-close entry inserted when a worker
/// picks the task up simply replaces the schedule-to-start one.
#[derive(Clone, Debug, PartialEq)]
pub struct WftTimeoutEntry {
    pub run_key: RunKey,
    /// Owning shard, so a handoff can evict this node's entries selectively.
    pub shard_id: ShardId,
    pub logical_seq: LogicalTaskSeq,
    pub started_event_id: i64,
    pub started_at: OffsetDateTime,
    pub workflow_task_timeout: Duration,
    pub kind: WftTimeoutKind,
}

/// Shared in-memory tracking state for started workflow tasks.
///
/// Cloning shares the underlying map; lanes, the scanner, and shard recovery all
/// see one set. It also carries the
/// [`SpeculativeTimerSet`](crate::speculative_timer::SpeculativeTimerSet)
/// (behind a shared `OnceLock` backfilled once the lanes exist) so the lane's
/// post-commit hook can arm/disarm the precise speculative timers through the
/// same handle it already holds for the coarse sweep.
#[derive(Clone, Default, Debug)]
pub struct WftTimeoutTrackingState {
    inner: Arc<Mutex<TimeoutEntries>>,
    owner: Arc<std::sync::OnceLock<Arc<RwLock<ShardOwner>>>>,
    acquisition: Option<ShardAcquisition>,
    #[cfg(test)]
    recovery_pause: Arc<Mutex<Option<RecoveryPause>>>,
    speculative: Arc<std::sync::OnceLock<crate::speculative_timer::SpeculativeTimerSet>>,
}

#[cfg(test)]
#[derive(Debug)]
struct RecoveryPause {
    installed: tokio::sync::oneshot::Sender<()>,
    resume: tokio::sync::oneshot::Receiver<()>,
}

#[derive(Clone, Debug)]
struct TrackedTimeout {
    entry: WftTimeoutEntry,
    revision: u64,
    acquisition: Option<ShardAcquisition>,
}

#[derive(Debug, Default)]
struct TimeoutEntries {
    entries: HashMap<RunKey, TrackedTimeout>,
    revision: u64,
}

impl WftTimeoutTrackingState {
    #[cfg(test)]
    pub(crate) fn pause_next_recovery(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (installed_tx, installed_rx) = tokio::sync::oneshot::channel();
        let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
        *self.recovery_pause.lock().unwrap() = Some(RecoveryPause {
            installed: installed_tx,
            resume: resume_rx,
        });
        (installed_rx, resume_tx)
    }

    #[cfg(test)]
    pub(crate) async fn pause_recovery(&self) -> Result<()> {
        let pause = self.recovery_pause.lock().unwrap().take();
        if let Some(pause) = pause {
            let _ = pause.installed.send(());
            pause
                .resume
                .await
                .map_err(|_| anyhow::anyhow!("candidate listing failed"))?;
        }
        Ok(())
    }
    pub(crate) fn set_owner(&self, owner: Arc<RwLock<ShardOwner>>) {
        let _ = self.owner.set(owner);
    }

    pub(crate) fn for_acquisition(&self, acquisition: ShardAcquisition) -> Self {
        Self {
            acquisition: Some(acquisition),
            ..self.clone()
        }
    }

    /// Track this task only under the current local acquisition. Recovery uses
    /// a captured acquisition; committed transitions use the current one.
    pub fn insert(&self, entry: WftTimeoutEntry) {
        let owner = self
            .owner
            .get()
            .map(|owner| owner.read().expect("shard owner lock poisoned"));
        let acquisition = self.acquisition.clone().or_else(|| {
            owner
                .as_ref()
                .and_then(|owner| owner.acquisition(entry.shard_id))
        });
        if let Some(owner) = &owner {
            let Some(acquisition) = &acquisition else {
                return;
            };
            if acquisition.cancel.is_cancelled() || !owner.matches_acquisition(acquisition) {
                return;
            }
        }
        // Ownership read lock precedes the tracking lock everywhere, including
        // cleanup. Replacement cannot interleave validation with installation.
        let mut inner = self.inner.lock().expect("inner lock poisoned");
        inner.revision = inner
            .revision
            .checked_add(1)
            .expect("timeout revision exhausted");
        let revision = inner.revision;
        inner.entries.insert(
            entry.run_key,
            TrackedTimeout {
                entry,
                revision,
                acquisition,
            },
        );
    }

    fn remove_submitted(&self, submitted: &TrackedTimeout) {
        let mut inner = self.inner.lock().expect("inner lock poisoned");
        if inner
            .entries
            .get(&submitted.entry.run_key)
            .is_some_and(|current| current.revision == submitted.revision)
        {
            inner.entries.remove(&submitted.entry.run_key);
        }
    }

    fn active(&self, entry: &TrackedTimeout) -> bool {
        match (self.owner.get(), &entry.acquisition) {
            (Some(owner), Some(acquisition)) => owner
                .read()
                .expect("shard owner lock poisoned")
                .acquisition_active(acquisition),
            (None, _) => true,
            _ => false,
        }
    }

    /// Number of started workflow tasks still awaiting a worker's reply.
    /// Drain reports it as `pending_wft_replies` (Req 8.2.8).
    pub fn tracked_count(&self) -> usize {
        self.inner
            .lock()
            .expect("inner lock poisoned")
            .entries
            .len()
    }

    /// Stop tracking a run's workflow task, called when it completes or times out
    /// so a finished task is never scanned again.
    pub fn remove(&self, run_key: RunKey) {
        self.inner
            .lock()
            .expect("inner lock poisoned")
            .entries
            .remove(&run_key);
    }

    /// Install the precise speculative-timer set once the runtime's lanes exist
    /// (construction-order backfill). Idempotent — a second install is ignored.
    pub fn set_speculative(&self, timers: crate::speculative_timer::SpeculativeTimerSet) {
        let _ = self.speculative.set(timers);
    }

    /// Arm (or re-arm) the precise in-memory timer for a run's SPECULATIVE task.
    /// No-op when no timer set is installed (tests / sweep-only setups).
    pub fn arm_speculative(
        &self,
        run_key: RunKey,
        shard_id: ShardId,
        logical_seq: LogicalTaskSeq,
        started_event_id: i64,
        deadline: OffsetDateTime,
        kind: WftTimeoutKind,
    ) {
        if let Some(timers) = self.speculative.get() {
            timers.arm(
                run_key,
                shard_id,
                logical_seq,
                started_event_id,
                deadline,
                kind,
            );
        }
    }

    /// Disarm a run's precise speculative timer (completion / failure /
    /// conversion-to-normal / a non-speculative pending task).
    pub fn disarm_speculative(&self, run_key: RunKey) {
        if let Some(timers) = self.speculative.get() {
            timers.disarm(run_key);
        }
    }

    /// Drop every entry for a shard on handoff; the new owner rebuilds them from
    /// durable state during its sweep. Also disarms this shard's precise
    /// speculative timers (re-derived on the new owner's sweep, F2).
    pub fn remove_all_for_shard(&self, shard_id: ShardId) {
        self.inner
            .lock()
            .expect("inner lock poisoned")
            .entries
            .retain(|_, entry| entry.entry.shard_id != shard_id);
        if let Some(timers) = self.speculative.get() {
            timers.remove_all_for_shard(shard_id);
        }
    }

    /// Snapshot all tracked entries; the scanner evaluates the copy without
    /// holding the lock.
    pub fn snapshot(&self) -> Vec<WftTimeoutEntry> {
        self.inner
            .lock()
            .expect("inner lock poisoned")
            .entries
            .values()
            .map(|tracked| tracked.entry.clone())
            .collect()
    }

    /// Snapshot only the entries owned by `shard_id`, used by the per-shard scan.
    pub fn snapshot_for_shard(&self, shard_id: ShardId) -> Vec<WftTimeoutEntry> {
        self.inner
            .lock()
            .expect("inner lock poisoned")
            .entries
            .values()
            .filter(|tracked| tracked.entry.shard_id == shard_id)
            .map(|tracked| tracked.entry.clone())
            .collect()
    }
}

/// Tuning for the workflow-task timeout scanner loop.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WftTimeoutScannerConfig {
    /// Delay between scans of the tracking set.
    pub scan_interval: tokio::time::Duration,
    /// Upper bound on timeouts submitted per scan, bounding the work one tick can
    /// do when many tasks expire together.
    pub max_timeouts_per_scan: usize,
}

impl Default for WftTimeoutScannerConfig {
    fn default() -> Self {
        Self {
            scan_interval: tokio::time::Duration::from_secs(1),
            max_timeouts_per_scan: 100,
        }
    }
}

/// Whether a started workflow task has exceeded its start-to-close deadline at
/// `now`.
///
/// A zero timeout is treated as immediately due once the task has started, rather
/// than as "no timeout", matching the durable encoding of an instantly-expiring
/// deadline.
pub fn evaluate_wft_timeout(entry: &WftTimeoutEntry, now: OffsetDateTime) -> bool {
    (entry.kind == WftTimeoutKind::ScheduleToStart
        && now >= entry.started_at + entry.workflow_task_timeout)
        || now - entry.started_at > entry.workflow_task_timeout
        || entry.workflow_task_timeout.is_zero() && now >= entry.started_at
}

/// Evaluate the tracked set once and submit a workflow-task timeout for each
/// expired entry, capped at `max_timeouts_per_scan`. Generic over the submit
/// closure so the pass is testable without a live lane. An entry is removed only
/// after the submit resolves (or is rejected as stale), so transient failures are
/// retried on the next scan.
pub(crate) async fn scan_wft_timeouts_once<F, Fut>(
    tracking: &WftTimeoutTrackingState,
    shard_id: Option<ShardId>,
    config: &WftTimeoutScannerConfig,
    submit_timeout: F,
) where
    F: FnMut(WftTimeoutEntry, OffsetDateTime) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    scan_wft_timeouts_at(
        tracking,
        shard_id,
        config,
        OffsetDateTime::now_utc(),
        submit_timeout,
    )
    .await;
}

async fn scan_wft_timeouts_at<F, Fut>(
    tracking: &WftTimeoutTrackingState,
    shard_id: Option<ShardId>,
    config: &WftTimeoutScannerConfig,
    now: OffsetDateTime,
    mut submit_timeout: F,
) where
    F: FnMut(WftTimeoutEntry, OffsetDateTime) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let mut entries: Vec<_> = tracking
        .inner
        .lock()
        .expect("inner lock poisoned")
        .entries
        .values()
        .filter(|tracked| shard_id.is_none_or(|id| tracked.entry.shard_id == id))
        .cloned()
        .collect();
    entries.sort_by_key(|tracked| tracked.entry.run_key);
    let mut submitted = 0usize;

    for tracked in entries {
        if !tracking.active(&tracked) {
            continue;
        }
        let entry = &tracked.entry;
        if submitted >= config.max_timeouts_per_scan {
            break;
        }
        if !evaluate_wft_timeout(entry, now) {
            continue;
        }

        match submit_timeout(entry.clone(), now).await {
            Ok(()) => {
                runtime_metrics::record_workflow_task_timed_out(OutcomeLabel::Success);
                tracking.remove_submitted(&tracked);
            }
            Err(error) => {
                let message = error.to_string();
                // Kernel rejection means the started task this timeout targeted is
                // no longer current (the worker completed it, or a newer attempt
                // superseded it). The entry is stale, so drop it instead of
                // retrying a command that can never apply. Other errors are
                // transient, so the entry is retained for the next scan.
                if message.contains("kernel rejected") {
                    runtime_metrics::record_workflow_task_timed_out(OutcomeLabel::Rejected);
                    tracing::debug!(
                        ?error,
                        run_key = ?entry.run_key,
                        "wft timeout scanner timeout rejected by kernel"
                    );
                    tracking.remove_submitted(&tracked);
                } else {
                    runtime_metrics::record_workflow_task_timed_out(OutcomeLabel::Failure);
                    tracing::warn!(
                        ?error,
                        run_key = ?entry.run_key,
                        "wft timeout scanner failed to submit timeout"
                    );
                }
            }
        }
        submitted += 1;
    }
}

/// Background loop: every `scan_interval`, fail any started workflow task that
/// has timed out, for each shard this node currently owns.
///
/// The active-shard set is re-read each tick so ownership changes (handoff, fresh
/// sweep) are picked up without restarting the task. Each firing routes by
/// `run_key` to the owning lane, keeping all commands for a run serialized on one
/// lane. The timeout carries `logical_seq` and `started_event_id` so the kernel
/// fences it if the targeted attempt is no longer current.
pub(crate) async fn run_wft_timeout_scanner(
    tracking: WftTimeoutTrackingState,
    lanes: Vec<LaneHandle>,
    lane_count: usize,
    shard_owner: Arc<RwLock<ShardOwner>>,
    config: WftTimeoutScannerConfig,
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
            runtime_metrics::record_scanner_tick("wft_timeout", shard_id.0);
            scan_wft_timeouts_once(&tracking, Some(shard_id), &config, |entry, now| {
                runtime_metrics::record_scanner_dispatched("wft_timeout", shard_id.0);
                let lane = pick_lane_for_run_key(&lanes, lane_count, entry.run_key).clone();
                async move {
                    let timeout_type = match entry.kind {
                        WftTimeoutKind::StartToClose => WorkflowTaskTimeoutType::StartToClose,
                        WftTimeoutKind::ScheduleToStart => WorkflowTaskTimeoutType::ScheduleToStart,
                    };
                    lane.submit(
                        entry.run_key,
                        Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest {
                            logical_seq: entry.logical_seq,
                            started_event_id: entry.started_event_id,
                            timeout_type,
                            now,
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

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        // Feature: workflow-dispatch, Property 7: Sticky recovery and affinity independence
        // A pending result can retire only its own entry, never a concurrent start or retry.
        #[test]
        fn timeout_result_preserves_replacement(rejected in any::<bool>(), newer_sequence in any::<bool>(), delay in 0i64..1000) {
            let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            runtime.block_on(async {
                let tracking = WftTimeoutTrackingState::default();
                let now = fixed_now();
                let mut original = sample_entry(RunKey::new(), now - Duration::seconds(delay));
                original.kind = WftTimeoutKind::ScheduleToStart;
                original.started_event_id = 0;
                original.workflow_task_timeout = Duration::ZERO;
                tracking.insert(original.clone());
                let mut replacement = original.clone();
                replacement.kind = WftTimeoutKind::StartToClose;
                replacement.started_event_id = 9;
                replacement.started_at = now;
                replacement.workflow_task_timeout = Duration::seconds(30);
                if newer_sequence { replacement.logical_seq.0 += 1; }
                let (started_tx, started_rx) = tokio::sync::oneshot::channel();
                let (release_tx, release_rx) = tokio::sync::oneshot::channel();
                let mut started_tx = Some(started_tx);
                let mut release_rx = Some(release_rx);
                let config = WftTimeoutScannerConfig::default();
                let scan = scan_wft_timeouts_at(&tracking, Some(ShardId(0)), &config, now, |_, _| {
                    let started_tx = started_tx.take().unwrap();
                    let release_rx = release_rx.take().unwrap();
                    async move {
                        started_tx.send(()).unwrap();
                        release_rx.await.unwrap();
                        if rejected { Err(anyhow::anyhow!("kernel rejected stale timeout")) } else { Ok(()) }
                    }
                });
                let replace = async {
                    started_rx.await.unwrap();
                    tracking.insert(replacement.clone());
                    release_tx.send(()).unwrap();
                };
                tokio::join!(scan, replace);
                assert_eq!(tracking.snapshot(), vec![replacement]);
            });
        }
    }

    #[tokio::test]
    async fn deadline_waits_for_its_acquisition_and_old_sweep_cannot_reinstall_it() {
        let owner = Arc::new(RwLock::new(ShardOwner::new(8)));
        let tracking = WftTimeoutTrackingState::default();
        tracking.set_owner(owner.clone());
        let first = {
            let mut owner = owner.write().unwrap();
            owner.record_acquired(ShardId(3), tokeira_types::ShardEpoch(7));
            owner.acquisition(ShardId(3)).unwrap()
        };
        let first_tracking = tracking.for_acquisition(first.clone());
        let mut entry = sample_entry(RunKey::new(), fixed_now());
        entry.shard_id = ShardId(3);
        entry.kind = WftTimeoutKind::ScheduleToStart;
        entry.workflow_task_timeout = Duration::ZERO;
        first_tracking.insert(entry.clone());
        scan_wft_timeouts_at(
            &tracking,
            Some(ShardId(3)),
            &WftTimeoutScannerConfig::default(),
            fixed_now(),
            |_, _| async { panic!("sweeping acquisition cannot fire") },
        )
        .await;
        let second = {
            let mut owner = owner.write().unwrap();
            owner.record_acquired(ShardId(3), tokeira_types::ShardEpoch(7));
            owner.acquisition(ShardId(3)).unwrap()
        };
        let mut replacement = entry.clone();
        replacement.logical_seq.0 += 1;
        tracking
            .for_acquisition(second.clone())
            .insert(replacement.clone());
        first_tracking.insert(entry);
        assert!(first.cancel.is_cancelled());
        assert!(!owner.write().unwrap().activate_acquisition(&first));
        assert_eq!(tracking.snapshot(), vec![replacement]);
        assert!(owner.write().unwrap().activate_acquisition(&second));
        scan_wft_timeouts_at(
            &tracking,
            Some(ShardId(3)),
            &WftTimeoutScannerConfig::default(),
            fixed_now(),
            |_, _| async { Ok(()) },
        )
        .await;
        assert!(tracking.snapshot().is_empty());
    }

    fn fixed_now() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()
    }

    fn sample_entry(run_key: RunKey, started_at: OffsetDateTime) -> WftTimeoutEntry {
        WftTimeoutEntry {
            kind: WftTimeoutKind::StartToClose,
            run_key,
            shard_id: ShardId(0),
            logical_seq: LogicalTaskSeq(7),
            started_event_id: 42,
            started_at,
            workflow_task_timeout: Duration::seconds(5),
        }
    }

    #[test]
    fn evaluate_wft_timeout_respects_elapsed_and_zero_timeouts() {
        let now = fixed_now();
        let expired = sample_entry(RunKey::new(), now - Duration::seconds(6));
        assert!(evaluate_wft_timeout(&expired, now));

        let fresh = sample_entry(RunKey::new(), now - Duration::seconds(4));
        assert!(!evaluate_wft_timeout(&fresh, now));

        let mut zero = sample_entry(RunKey::new(), now);
        zero.workflow_task_timeout = Duration::ZERO;
        assert!(evaluate_wft_timeout(&zero, now));
    }

    #[tokio::test]
    async fn scan_wft_timeouts_once_submits_and_removes_only_expired_entries() {
        let tracking = WftTimeoutTrackingState::default();
        let now = OffsetDateTime::now_utc();
        let expired = sample_entry(RunKey::new(), now - Duration::seconds(6));
        let fresh = sample_entry(RunKey::new(), now - Duration::seconds(1));
        tracking.insert(expired.clone());
        tracking.insert(fresh.clone());

        let submitted = Arc::new(Mutex::new(Vec::new()));
        scan_wft_timeouts_once(
            &tracking,
            Some(ShardId(0)),
            &WftTimeoutScannerConfig::default(),
            {
                let submitted = submitted.clone();
                move |entry, _| {
                    let submitted = submitted.clone();
                    async move {
                        submitted
                            .lock()
                            .expect("submitted lock poisoned")
                            .push(entry.run_key);
                        Ok(())
                    }
                }
            },
        )
        .await;

        assert_eq!(
            *submitted.lock().expect("submitted lock poisoned"),
            vec![expired.run_key]
        );
        let remaining = tracking.snapshot();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].run_key, fresh.run_key);
    }

    #[tokio::test]
    async fn completed_wft_removed_before_scan_does_not_submit_timeout() {
        let tracking = WftTimeoutTrackingState::default();
        let now = fixed_now();
        let entry = sample_entry(RunKey::new(), now - Duration::seconds(10));
        tracking.insert(entry.clone());
        tracking.remove(entry.run_key);

        scan_wft_timeouts_once(
            &tracking,
            Some(ShardId(0)),
            &WftTimeoutScannerConfig::default(),
            |_entry, _| async move {
                panic!("completed workflow task should not be submitted");
                #[allow(unreachable_code)]
                Ok(())
            },
        )
        .await;

        assert!(tracking.snapshot().is_empty());
    }
}
