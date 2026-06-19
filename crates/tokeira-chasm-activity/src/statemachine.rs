//! The activity legal transition table and the stamp-fenced `apply`.
//!
//! Ground truth: `chasm/lib/activity/statemachine.go @ v1.31.0`. The legal
//! `(from, event) → to` table is reproduced exactly from the `NewTransition`
//! source-state lists there (Requirement 11.4, 11.5):
//!
//! | event | legal from | to |
//! |-------|-----------|----|
//! | `Scheduled` | `Unspecified` | `Scheduled` |
//! | `Rescheduled` | `Started` | `Scheduled` |
//! | `Started` | `Scheduled` | `Started` |
//! | `Completed` | `Started`, `CancelRequested` | `Completed` |
//! | `Failed` | `Started`, `CancelRequested` | `Failed` |
//! | `CancelRequested` | `Scheduled`, `Started`, `CancelRequested` | `CancelRequested` |
//! | `Canceled` | `CancelRequested` | `Canceled` |
//! | `Terminated` | `Scheduled`, `Started`, `CancelRequested` | `Terminated` |
//! | `TimedOut` | `Scheduled`, `Started`, `CancelRequested` | `TimedOut` |
//!
//! Note this is the *ground-truth* table, which is broader than the design's
//! simplified mermaid (e.g. `Completed`/`Failed` are legal from `CancelRequested`,
//! and `CancelRequested` is idempotent). Per `AGENTS §8`, the targeted release
//! wins over the design sketch.
//!
//! An illegal `(from, event)` returns [`ChasmError::IllegalTransition`] leaving the
//! state untouched (Requirement 11.5). A [`ActivityEvent::TimedOut`] whose `stamp`
//! does not match the live attempt is **superseded** — applied as a no-op
//! (Requirement 11.6) — because a retry has already moved the attempt on and the
//! validate-then-drop gate would have reaped its timer.

use tokeira_chasm::{ChasmError, MutableContext, Task, TaskKind};

use crate::{
    state::{ActivityState, ActivityStatus},
    tasks::{
        DISPATCH_TASK_ID, DispatchTask, HEARTBEAT_TASK_ID, HeartbeatTimer,
        SCHEDULE_TO_CLOSE_TASK_ID, SCHEDULE_TO_START_TASK_ID, START_TO_CLOSE_TASK_ID,
        ScheduleToCloseTimer, ScheduleToStartTimer, StartToCloseTimer,
    },
};

/// The kind of timeout a [`ActivityEvent::TimedOut`] records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutType {
    /// Not started before the schedule-to-start deadline.
    ScheduleToStart,
    /// Not closed before the schedule-to-close deadline.
    ScheduleToClose,
    /// Not closed before the start-to-close deadline.
    StartToClose,
    /// No heartbeat before the heartbeat deadline.
    Heartbeat,
}

impl TimeoutType {
    /// The canonical timeout-type name, matching the v1.31.0 `enums.TimeoutType`
    /// short string used in the timeout failure message (`createStartToCloseTimeoutFailure`
    /// et al. `@ v1.31.0`). The edge maps this to the proto `TimeoutType` enum value.
    pub fn as_str(self) -> &'static str {
        match self {
            TimeoutType::ScheduleToStart => "ScheduleToStart",
            TimeoutType::ScheduleToClose => "ScheduleToClose",
            TimeoutType::StartToClose => "StartToClose",
            TimeoutType::Heartbeat => "Heartbeat",
        }
    }
}

/// An event applied to an activity. Payloads are the minimum the MVP transition
/// needs; richer provenance (worker identity, deployment) rides later.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActivityEvent {
    /// Initial scheduling (begins attempt 1).
    Scheduled,
    /// Retry after a failed attempt (begins the next attempt).
    Rescheduled {
        /// Recorded failure message from the prior attempt.
        failure: String,
        /// Encoded `Payloads` of the prior attempt's last heartbeat details, carried
        /// across the retry so the next attempt's poll observes them
        /// (`HeartbeatDetailsAvailableOnRetry`). Empty when none; an empty value does
        /// not clobber details a heartbeat already recorded.
        last_heartbeat_details: Vec<u8>,
        /// The backoff interval before the retry, in nanoseconds. The new attempt is
        /// scheduled at `now + interval_nanos` (`attemptScheduleTimeForRetry @
        /// v1.31.0`); the delayed dispatch and the re-anchored schedule-to-start
        /// timer both fire from there.
        interval_nanos: i64,
    },
    /// A worker picked up the activity.
    Started {
        /// Worker pickup time, Unix nanoseconds.
        started_time_nanos: i64,
        /// Identity of the worker that picked up the attempt (recorded as the
        /// activity's `last_worker_identity`).
        identity: String,
    },
    /// The activity completed successfully.
    Completed {
        /// Serialized result payload.
        result: Vec<u8>,
    },
    /// The activity failed terminally.
    Failed {
        /// Failure message (surfaced as `info.last_failure.message` and the quick
        /// outcome message).
        failure: String,
        /// The full encoded `Failure` proto the worker reported, persisted so the
        /// describe outcome round-trips it exactly (empty when the caller has only a
        /// message).
        failure_payload: Vec<u8>,
        /// Encoded `Payloads` of the worker's last heartbeat details supplied on the
        /// fail request, recorded onto the activity so describe echoes them
        /// (`statemachine.go:220 @ v1.31.0`). Empty when none were supplied.
        last_heartbeat_details: Vec<u8>,
    },
    /// A cancel was requested.
    CancelRequested {
        /// Requesting identity.
        identity: String,
        /// The cancel request's `request_id`, stored for idempotency/conflict
        /// detection on a repeated cancel (`activity.go:402-409 @ v1.31.0`).
        request_id: String,
        /// The cancel reason, echoed on `info.canceled_reason`.
        reason: String,
    },
    /// The cancel was acknowledged.
    Canceled,
    /// An operator terminated the activity.
    Terminated {
        /// Termination reason.
        reason: String,
        /// The terminate request's `request_id`, stored for idempotency/conflict
        /// detection on a repeated terminate (`activity.go:359-370 @ v1.31.0`).
        request_id: String,
    },
    /// A worker heartbeat: records the latest heartbeat details without changing
    /// status (legal only while `Started`/`CancelRequested`). Status-preserving so a
    /// heartbeat neither advances the attempt nor closes the activity
    /// (`RecordActivityTaskHeartbeat` records onto `LastHeartbeat`,
    /// `chasm/lib/activity/activity.go @ v1.31.0`).
    Heartbeat {
        /// Encoded `Payloads` of the worker's heartbeat details.
        details: Vec<u8>,
    },
    /// A timeout fired; carries the firing timer's attempt `stamp` for fencing.
    TimedOut {
        /// The attempt stamp the firing timer was scheduled for.
        stamp: i64,
        /// Which timeout fired.
        timeout_type: TimeoutType,
        /// Encoded `temporal.api.failure.v1.Failure` carrying the matching
        /// `TimeoutFailureInfo`, built at the edge (the pure crate is proto-free) so
        /// the describe/poll outcome surfaces the structured timeout type, not just a
        /// message (`standalone_activity_test.go:4509` asserts `timeout type=
        /// Heartbeat`). Empty falls back to a message-only failure.
        failure_payload: Vec<u8>,
    },
}

impl ActivityEvent {
    /// A short stable name for the event, used in [`ChasmError::IllegalTransition`].
    fn name(&self) -> &'static str {
        match self {
            ActivityEvent::Scheduled => "Scheduled",
            ActivityEvent::Rescheduled { .. } => "Rescheduled",
            ActivityEvent::Started { .. } => "Started",
            ActivityEvent::Completed { .. } => "Completed",
            ActivityEvent::Failed { .. } => "Failed",
            ActivityEvent::CancelRequested { .. } => "CancelRequested",
            ActivityEvent::Canceled => "Canceled",
            ActivityEvent::Terminated { .. } => "Terminated",
            ActivityEvent::Heartbeat { .. } => "Heartbeat",
            ActivityEvent::TimedOut { .. } => "TimedOut",
        }
    }
}

/// The legal target status for `(from, event)`, or `None` if the transition is
/// illegal. Reproduces the `NewTransition` source lists in `statemachine.go @
/// v1.31.0` exactly (Requirement 11.4, 11.5).
pub fn legal_target(from: ActivityStatus, event: &ActivityEvent) -> Option<ActivityStatus> {
    use ActivityStatus::*;
    let legal =
        |allowed: &[ActivityStatus], to: ActivityStatus| allowed.contains(&from).then_some(to);
    match event {
        ActivityEvent::Scheduled => legal(&[Unspecified], Scheduled),
        ActivityEvent::Rescheduled { .. } => legal(&[Started], Scheduled),
        ActivityEvent::Started { .. } => legal(&[Scheduled], Started),
        ActivityEvent::Completed { .. } => legal(&[Started, CancelRequested], Completed),
        ActivityEvent::Failed { .. } => legal(&[Started, CancelRequested], Failed),
        ActivityEvent::CancelRequested { .. } => {
            legal(&[Scheduled, Started, CancelRequested], CancelRequested)
        }
        ActivityEvent::Canceled => legal(&[CancelRequested], Canceled),
        ActivityEvent::Terminated { .. } => {
            legal(&[Scheduled, Started, CancelRequested], Terminated)
        }
        // Status-preserving: a heartbeat keeps the activity in its current status
        // (legal only while Started/CancelRequested), so the target is `from`.
        ActivityEvent::Heartbeat { .. } => legal(&[Started, CancelRequested], from),
        ActivityEvent::TimedOut { .. } => legal(&[Scheduled, Started, CancelRequested], TimedOut),
    }
}

/// Apply `event` to `state`, scheduling the resulting tasks via `ctx`
/// (Requirement 11.4–11.7).
///
/// - An illegal `(from, event)` returns [`ChasmError::IllegalTransition`] and
///   leaves `state` unchanged (Requirement 11.5).
/// - A [`ActivityEvent::TimedOut`] with a stale `stamp` is a superseded no-op
///   returning `Ok(())` (Requirement 11.6).
/// - On `Scheduled`/`Rescheduled` the dispatch task and the relevant pure timers
///   are scheduled; on `Started` the start-to-close and heartbeat timers are
///   scheduled. Terminal transitions schedule nothing; validate-then-drop reaps the
///   outstanding timers.
pub fn apply(
    state: &mut ActivityState,
    event: ActivityEvent,
    ctx: &mut dyn MutableContext,
) -> Result<(), ChasmError> {
    let from = state.status();

    // Stamp fence: a timeout for a superseded attempt is a no-op (Requirement 11.6).
    if let ActivityEvent::TimedOut { stamp, .. } = &event
        && *stamp != state.stamp
    {
        return Ok(());
    }

    let to = legal_target(from, &event).ok_or_else(|| ChasmError::IllegalTransition {
        from: format!("{from:?}"),
        event: event.name().to_owned(),
    })?;

    let now = ctx.now_unix_nanos();
    match &event {
        ActivityEvent::Scheduled => {
            state.attempt += 1;
            state.stamp += 1;
            state.scheduled_time_nanos = now;
            // First attempt: the per-attempt anchor equals the original schedule
            // time. schedule-to-start, schedule-to-close, and dispatch all key off
            // `now` (`statemachine.go:42-74 @ v1.31.0`).
            state.attempt_scheduled_time_nanos = now;
            schedule_attempt_timers(state, ctx, now, true)?;
        }
        ActivityEvent::Rescheduled {
            failure,
            last_heartbeat_details,
            interval_nanos,
        } => {
            state.attempt += 1;
            state.stamp += 1;
            // `scheduled_time_nanos` is the activity's ORIGINAL schedule time and is
            // NOT advanced on retry: the schedule-to-close budget and `ScheduleTime`
            // echo stay pinned to it (`activity.go:537,686 @ v1.31.0`;
            // `TransitionRescheduled` does not re-add the schedule-to-close timer).
            // The per-attempt anchor is the retry's start time.
            let retry_scheduled = now + *interval_nanos;
            state.attempt_scheduled_time_nanos = retry_scheduled;
            state.failure = failure.clone();
            state.started_time_nanos = 0;
            // Carry the prior attempt's heartbeat details forward; an empty value
            // must not clobber details already recorded (mirrors the Failed-path
            // guard, `statemachine.go:220 @ v1.31.0`).
            if !last_heartbeat_details.is_empty() {
                state.last_heartbeat_details = last_heartbeat_details.clone();
            }
            // Re-arm schedule-to-start (anchored at the retry time) and the delayed
            // dispatch; do NOT re-arm schedule-to-close (it spans attempts).
            schedule_attempt_timers(state, ctx, retry_scheduled, false)?;
        }
        ActivityEvent::Started {
            started_time_nanos,
            identity,
        } => {
            let started = if *started_time_nanos > 0 {
                *started_time_nanos
            } else {
                now
            };
            state.started_time_nanos = started;
            state.last_worker_identity = identity.clone();
            if state.start_to_close_nanos > 0 {
                schedule_pure(
                    ctx,
                    START_TO_CLOSE_TASK_ID,
                    &StartToCloseTimer {
                        stamp: state.stamp,
                        fire_at_nanos: started + state.start_to_close_nanos,
                    },
                )?;
            }
            if state.heartbeat_nanos > 0 {
                schedule_pure(
                    ctx,
                    HEARTBEAT_TASK_ID,
                    &HeartbeatTimer {
                        stamp: state.stamp,
                        fire_at_nanos: started + state.heartbeat_nanos,
                    },
                )?;
            }
        }
        ActivityEvent::Completed { result } => {
            state.result = result.clone();
        }
        ActivityEvent::Failed {
            failure,
            failure_payload,
            last_heartbeat_details,
        } => {
            state.failure = failure.clone();
            state.failure_payload = failure_payload.clone();
            // Only overwrite when the worker supplied details, mirroring v1.31.0's
            // `if details := req.GetLastHeartbeatDetails(); details != nil` guard
            // (`statemachine.go:220`) — an empty field must not clobber details a
            // prior heartbeat recorded.
            if !last_heartbeat_details.is_empty() {
                state.last_heartbeat_details = last_heartbeat_details.clone();
            }
        }
        ActivityEvent::CancelRequested {
            identity: _,
            request_id,
            reason,
        } => {
            // Store cancel request_id/reason for idempotency + `info.canceled_reason`.
            // The requester identity is NOT written to last_worker_identity (that
            // names the polling worker, not the canceller) — it rides CancelState in
            // v1.31.0, which tokeira does not yet surface.
            state.cancel_request_id = request_id.clone();
            state.cancel_reason = reason.clone();
        }
        ActivityEvent::Canceled => {
            state.failure = "Activity canceled".to_owned();
        }
        ActivityEvent::Terminated { reason, request_id } => {
            state.failure = reason.clone();
            state.terminate_request_id = request_id.clone();
        }
        // Record the latest heartbeat details and time; status is unchanged (the
        // `to == from` target above), so the attempt is untouched. The heartbeat
        // does NOT schedule a fresh pure timer: the heartbeat-timeout deadline is
        // re-derived from `max(last_heartbeat, started)` by the runtime sweeper
        // (`timeouts::due_timeout`), so pushing `last_heartbeat_time_nanos` out is
        // what keeps the activity alive between heartbeats. Re-deriving (rather than
        // arming a new per-heartbeat task) keeps the node outbox bounded — a long
        // run of heartbeats does not accumulate timer tasks. This mirrors the v1.31.0
        // *effect* (`activity.go:577-585` re-anchors the heartbeat timeout), with the
        // anchor carried on state instead of on a fresh task (a deliberate
        // history-is-authority simplification; the timer is a derived effect).
        ActivityEvent::Heartbeat { details } => {
            state.last_heartbeat_details = details.clone();
            state.last_heartbeat_time_nanos = now;
        }
        ActivityEvent::TimedOut {
            timeout_type,
            failure_payload,
            ..
        } => {
            // `FailureReasonActivityTimeout = "activity %v timeout"`
            // (`common/util.go:95 @ v1.31.0`) with the timeout type's enum name.
            state.failure = format!("activity {} timeout", timeout_type.as_str());
            // The edge supplies the structured `Failure` (with `TimeoutFailureInfo`);
            // store it so the describe/poll outcome round-trips the timeout type.
            if !failure_payload.is_empty() {
                state.failure_payload = failure_payload.clone();
            }
        }
    }

    state.set_status(to);
    // Record the close time on the first terminal transition (`now` is the
    // transition's logical clock). This persists it on the node so the visibility
    // snapshot's close time is recomputable from state alone — the repair scanner's
    // precondition (Req 10.11).
    if to.is_terminal() && state.close_time_nanos == 0 {
        state.close_time_nanos = now;
    }
    Ok(())
}

/// Schedule the schedule-to-start timer and the dispatch task for a freshly
/// scheduled attempt; on the first attempt (`initial`) also arm the schedule-to-close
/// timer. Shared by `Scheduled` (initial) and `Rescheduled` (retry).
///
/// `anchor` is the attempt's scheduled-to-start anchor (`now` for the first attempt,
/// the retry time for a reschedule). The dispatch fires immediately on the first
/// attempt and is delayed to `anchor` on a retry, so a worker cannot poll the new
/// attempt before its backoff elapses (`statemachine.go:103-122 @ v1.31.0`).
/// Schedule-to-close spans attempts and is armed once on the first schedule, so it
/// is NOT re-armed here on a retry (`TransitionRescheduled` does not re-add it).
fn schedule_attempt_timers(
    state: &ActivityState,
    ctx: &mut dyn MutableContext,
    anchor: i64,
    initial: bool,
) -> Result<(), ChasmError> {
    if state.schedule_to_start_nanos > 0 {
        schedule_pure(
            ctx,
            SCHEDULE_TO_START_TASK_ID,
            &ScheduleToStartTimer {
                stamp: state.stamp,
                fire_at_nanos: anchor + state.schedule_to_start_nanos,
            },
        )?;
    }
    if initial && state.schedule_to_close_nanos > 0 {
        schedule_pure(
            ctx,
            SCHEDULE_TO_CLOSE_TASK_ID,
            &ScheduleToCloseTimer {
                stamp: state.stamp,
                fire_at_nanos: state.scheduled_time_nanos + state.schedule_to_close_nanos,
            },
        )?;
    }
    // The dispatch side-effect task enqueues the attempt to matching post-commit.
    // On a retry it carries the retry anchor as its `fire_at` so the dispatch sink /
    // poll treats it as not pollable until the backoff elapses; the first attempt
    // dispatches immediately (`fire_at = None`).
    let dispatch = DispatchTask {
        stamp: state.stamp,
        task_queue: state.task_queue.clone(),
    };
    let fire_at = (!initial).then_some(anchor);
    ctx.add_task(
        TaskKind::SideEffect,
        DISPATCH_TASK_ID,
        encode_task(&dispatch)?,
        fire_at,
    )
}

/// Schedule one pure timer task into the owning node's outbox.
fn schedule_pure<T: Task>(
    ctx: &mut dyn MutableContext,
    task_type_id: u32,
    task: &T,
) -> Result<(), ChasmError> {
    ctx.add_task(
        TaskKind::Pure,
        task_type_id,
        encode_task(task)?,
        task.fire_at(),
    )
}

/// Serialize a task payload for the node outbox (postcard; internal to the activity
/// library, which also deserializes it in its validators).
fn encode_task<T: serde::Serialize>(task: &T) -> Result<Vec<u8>, ChasmError> {
    postcard::to_allocvec(task)
        .map_err(|e| ChasmError::Internal(format!("encode activity task: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_chasm::{Context, ExecutionInfo, ExecutionKey};

    /// Decode a postcard-encoded task payload (test-only; the runtime will use the
    /// validators directly once wired).
    fn decode_task<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, ChasmError> {
        postcard::from_bytes(bytes).map_err(|e| ChasmError::Validation(format!("decode task: {e}")))
    }

    /// A minimal transition context that records scheduled tasks.
    struct TestCtx {
        key: ExecutionKey,
        now: i64,
        tasks: Vec<(TaskKind, u32, Vec<u8>, Option<i64>)>,
    }

    impl TestCtx {
        fn new(now: i64) -> Self {
            Self {
                key: ExecutionKey::new("ns", "act-1", "run-1"),
                now,
                tasks: Vec::new(),
            }
        }
    }

    impl Context for TestCtx {
        fn execution_key(&self) -> &ExecutionKey {
            &self.key
        }
        fn execution_info(&self) -> ExecutionInfo {
            ExecutionInfo::default()
        }
        fn now_unix_nanos(&self) -> i64 {
            self.now
        }
    }

    impl MutableContext for TestCtx {
        fn add_task(
            &mut self,
            kind: TaskKind,
            task_type_id: u32,
            payload: Vec<u8>,
            fire_at_unix_nanos: Option<i64>,
        ) -> Result<(), ChasmError> {
            self.tasks
                .push((kind, task_type_id, payload, fire_at_unix_nanos));
            Ok(())
        }
        fn mark_dirty(&mut self) -> Result<(), ChasmError> {
            Ok(())
        }
    }

    const SEC: i64 = 1_000_000_000;

    fn scheduled_state() -> ActivityState {
        // A state already in Scheduled at attempt 1, stamp 1, with timeouts set.
        let mut s = ActivityState {
            attempt: 1,
            stamp: 1,
            start_to_close_nanos: 10 * SEC,
            heartbeat_nanos: 2 * SEC,
            ..ActivityState::default()
        };
        s.set_status(ActivityStatus::Scheduled);
        s
    }

    #[test]
    fn scheduled_from_unspecified_bumps_attempt_and_schedules_dispatch() {
        let mut state = ActivityState {
            schedule_to_start_nanos: 5 * SEC,
            schedule_to_close_nanos: 30 * SEC,
            ..ActivityState::default()
        };
        let mut ctx = TestCtx::new(1_000);
        apply(&mut state, ActivityEvent::Scheduled, &mut ctx).expect("scheduled");
        assert_eq!(state.status(), ActivityStatus::Scheduled);
        assert_eq!(state.attempt, 1);
        assert_eq!(state.stamp, 1);
        // schedule-to-start + schedule-to-close pure timers and the dispatch task.
        assert!(
            ctx.tasks
                .iter()
                .any(|(k, id, _, _)| *k == TaskKind::Pure && *id == SCHEDULE_TO_START_TASK_ID)
        );
        assert!(
            ctx.tasks
                .iter()
                .any(|(k, id, _, _)| *k == TaskKind::Pure && *id == SCHEDULE_TO_CLOSE_TASK_ID)
        );
        assert!(
            ctx.tasks
                .iter()
                .any(|(k, id, _, fire)| *k == TaskKind::SideEffect
                    && *id == DISPATCH_TASK_ID
                    && fire.is_none())
        );
    }

    #[test]
    fn started_schedules_start_to_close_and_heartbeat() {
        let mut state = scheduled_state();
        let mut ctx = TestCtx::new(5_000);
        apply(
            &mut state,
            ActivityEvent::Started {
                started_time_nanos: 5_000,
                identity: "worker-1".to_owned(),
            },
            &mut ctx,
        )
        .expect("started");
        assert_eq!(state.status(), ActivityStatus::Started);
        let stc = ctx
            .tasks
            .iter()
            .find(|(k, id, _, _)| *k == TaskKind::Pure && *id == START_TO_CLOSE_TASK_ID)
            .expect("start-to-close timer");
        assert_eq!(stc.3, Some(5_000 + 10 * SEC));
        assert!(
            ctx.tasks
                .iter()
                .any(|(_, id, _, _)| *id == HEARTBEAT_TASK_ID)
        );
    }

    #[test]
    fn illegal_transition_is_rejected_and_leaves_state_unchanged() {
        // Completed is illegal from Scheduled.
        let mut state = scheduled_state();
        let before = state.clone();
        let mut ctx = TestCtx::new(0);
        let err = apply(
            &mut state,
            ActivityEvent::Completed { result: vec![1] },
            &mut ctx,
        )
        .unwrap_err();
        assert!(matches!(err, ChasmError::IllegalTransition { .. }));
        assert_eq!(state, before);
        assert!(ctx.tasks.is_empty());
    }

    #[test]
    fn stale_stamp_timeout_is_superseded_noop() {
        // A timeout carrying an old stamp must not fire (Requirement 11.6).
        let mut state = scheduled_state(); // live stamp = 1
        let before = state.clone();
        let mut ctx = TestCtx::new(0);
        apply(
            &mut state,
            ActivityEvent::TimedOut {
                stamp: 0, // stale
                timeout_type: TimeoutType::ScheduleToStart,
                failure_payload: Vec::new(),
            },
            &mut ctx,
        )
        .expect("superseded no-op is Ok");
        assert_eq!(state, before);
    }

    #[test]
    fn matching_stamp_timeout_transitions_to_timed_out() {
        let mut state = scheduled_state(); // live stamp = 1
        let mut ctx = TestCtx::new(0);
        apply(
            &mut state,
            ActivityEvent::TimedOut {
                stamp: 1,
                timeout_type: TimeoutType::ScheduleToStart,
                failure_payload: Vec::new(),
            },
            &mut ctx,
        )
        .expect("timed out");
        assert_eq!(state.status(), ActivityStatus::TimedOut);
    }

    #[test]
    fn dispatch_task_payload_round_trips() {
        let mut state = ActivityState::default();
        let mut ctx = TestCtx::new(0);
        apply(&mut state, ActivityEvent::Scheduled, &mut ctx).expect("scheduled");
        let (_, _, payload, _) = ctx
            .tasks
            .iter()
            .find(|(_, id, _, _)| *id == DISPATCH_TASK_ID)
            .expect("dispatch task");
        let task: crate::tasks::DispatchTask = decode_task(payload).expect("decode");
        assert_eq!(task.stamp, state.stamp);
    }

    #[test]
    fn completed_from_started_sets_result() {
        let mut state = scheduled_state();
        let mut ctx = TestCtx::new(0);
        apply(
            &mut state,
            ActivityEvent::Started {
                started_time_nanos: 1,
                identity: "worker-1".to_owned(),
            },
            &mut ctx,
        )
        .expect("started");
        apply(
            &mut state,
            ActivityEvent::Completed { result: vec![9, 9] },
            &mut ctx,
        )
        .expect("completed");
        assert_eq!(state.status(), ActivityStatus::Completed);
        assert_eq!(state.result, vec![9, 9]);
    }

    #[test]
    fn terminal_transition_records_close_time() {
        // The close time is persisted on the terminal transition so the visibility
        // snapshot is recomputable from node state (the repair scanner precondition,
        // Req 10.11).
        let mut state = scheduled_state();
        let mut ctx = TestCtx::new(5_000);
        apply(
            &mut state,
            ActivityEvent::Started {
                started_time_nanos: 1,
                identity: "worker-1".to_owned(),
            },
            &mut ctx,
        )
        .expect("started");
        assert_eq!(
            state.close_time_nanos, 0,
            "an open activity has no close time"
        );

        apply(
            &mut state,
            ActivityEvent::Completed { result: vec![] },
            &mut ctx,
        )
        .expect("completed");
        assert_eq!(
            state.close_time_nanos, 5_000,
            "close time = the terminal transition's clock"
        );
    }
}
