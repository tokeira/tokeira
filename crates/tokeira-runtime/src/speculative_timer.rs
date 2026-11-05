//! Precise in-memory timers for SPECULATIVE workflow tasks.
//!
//! A speculative workflow task lives entirely in mutable state — no durable
//! `WorkflowTaskScheduled`/`Started` events, no persisted timer row — so its
//! schedule-to-start and start-to-close deadlines are enforced by
//! runtime-in-memory timers, mirroring v1.31.0's `memoryScheduledQueue`
//! (`service/history/queues/speculative_workflow_task_timeout_queue.go` +
//! `tasks.WorkflowTaskTimeoutTask{InMemory:true}` @ v1.31.0; spec speculative-wft
//! R.2). The [`crate::wft_timeout`] scanner also wakes at active tracked deadlines;
//! these per-task timers retain speculative-task replacement and cancellation
//! independently of its bounded per-shard scans.
//!
//! Each timer is a per-run `tokio` task that sleeps until the deadline and then
//! submits `Command::WorkflowTaskTimedOut` on the run's lane. The kernel fences
//! a stale firing (the task it targeted has since been completed or superseded),
//! so a late timer is harmless. Arming replaces any prior timer for the run
//! (stale-timer guard, the analogue of v1.31.0's
//! `CheckSpeculativeWorkflowTaskTimeoutTask` pointer identity); a completion,
//! failure, or conversion-to-normal disarms it. The set is volatile: on shard
//! handoff the new owner re-derives timers from persisted pending state during
//! its sweep (owner amendment F2).

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use time::OffsetDateTime;
use tokeira_clock::Clock;
use tokeira_kernel::{Command, WorkflowTaskTimedOutRequest, WorkflowTaskTimeoutType};
use tokeira_types::{LogicalTaskSeq, RunKey, ShardId};
use tokio_util::sync::CancellationToken;

use crate::{lane::LaneHandle, scanner::pick_lane_for_run_key, wft_timeout::WftTimeoutKind};

/// A live speculative timer: the deadline task plus the identity it targets so a
/// re-arm for a superseding task cancels the old one.
#[derive(Debug)]
struct SpeculativeTimer {
    shard_id: ShardId,
    cancel: CancellationToken,
}

/// Set of precise in-memory speculative-task timers, keyed by run (one
/// speculative task per run at a time). Cloning shares the underlying map and
/// lane handles.
#[derive(Clone, Debug)]
pub struct SpeculativeTimerSet {
    inner: Arc<Mutex<HashMap<RunKey, SpeculativeTimer>>>,
    /// Lane handles used to submit the timeout when a timer fires. Shares the
    /// runtime's `shared_lanes` slot, which is backfilled once the lanes are
    /// spawned (construction-order cycle), so it may be empty at construction
    /// and populated by the time any timer actually fires.
    lanes: Arc<Mutex<Vec<LaneHandle>>>,
    lane_count: usize,
    /// The clock the speculative deadlines were stamped with.
    clock: Arc<dyn Clock>,
}

impl SpeculativeTimerSet {
    pub fn new(
        lanes: Arc<Mutex<Vec<LaneHandle>>>,
        lane_count: usize,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            lanes,
            lane_count,
            clock,
        }
    }

    /// Arm (or re-arm) the precise timer for a run's speculative task, replacing
    /// any prior timer. The spawned task sleeps until `deadline` and then submits
    /// `WorkflowTaskTimedOut(kind)` on the run's lane; a deadline already in the
    /// past fires immediately. `started_event_id` is 0 for a schedule-to-start
    /// timer.
    pub fn arm(
        &self,
        run_key: RunKey,
        shard_id: ShardId,
        logical_seq: LogicalTaskSeq,
        started_event_id: i64,
        deadline: OffsetDateTime,
        kind: WftTimeoutKind,
    ) {
        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        let lanes = self.lanes.clone();
        let lane_count = self.lane_count;
        let clock = self.clock.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = task_cancel.cancelled() => return,
                _ = crate::deadline::sleep_until(clock.as_ref(), deadline) => {}
            }
            let timeout_type = match kind {
                WftTimeoutKind::StartToClose => WorkflowTaskTimeoutType::StartToClose,
                WftTimeoutKind::ScheduleToStart => WorkflowTaskTimeoutType::ScheduleToStart,
            };
            let lane = {
                let lanes = lanes.lock().expect("lanes lock poisoned");
                if lanes.is_empty() {
                    return;
                }
                pick_lane_for_run_key(&lanes, lane_count, run_key).clone()
            };
            // The kernel fences a stale firing (seq/started/attempt mismatch), so
            // a lost race with the worker's completion is a no-op reject. The
            // fired entry stays in the map until the resulting commit's lane hook
            // disarms it (or a re-arm / handoff evicts it) — a spent timer is
            // inert. The M.1 timer-task metrics are emitted by the kernel's
            // timeout handler (which knows the namespace and whether the timeout
            // applied), routed out as a post-commit op — see
            // `DispatchOp::RecordSpeculativeTimeout`.
            let _ = lane
                .submit(
                    run_key,
                    Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest {
                        logical_seq,
                        started_event_id,
                        timeout_type,
                        now: clock.now(),
                    }),
                )
                .await;
        });
        if let Some(prev) = self
            .inner
            .lock()
            .expect("inner lock poisoned")
            .insert(run_key, SpeculativeTimer { shard_id, cancel })
        {
            prev.cancel.cancel();
        }
    }

    /// Disarm the run's speculative timer, if any (completion / failure /
    /// conversion-to-normal / a non-speculative pending task).
    pub fn disarm(&self, run_key: RunKey) {
        if let Some(timer) = self
            .inner
            .lock()
            .expect("inner lock poisoned")
            .remove(&run_key)
        {
            timer.cancel.cancel();
        }
    }

    /// Drop every timer for a shard on handoff; the new owner re-derives them
    /// from persisted pending speculative state during its sweep (F2).
    pub fn remove_all_for_shard(&self, shard_id: ShardId) {
        let mut map = self.inner.lock().expect("inner lock poisoned");
        map.retain(|_, timer| {
            if timer.shard_id == shard_id {
                timer.cancel.cancel();
                false
            } else {
                true
            }
        });
    }
}
