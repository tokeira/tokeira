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
    },
    /// A worker picked up the activity.
    Started {
        /// Worker pickup time, Unix nanoseconds.
        started_time_nanos: i64,
    },
    /// The activity completed successfully.
    Completed {
        /// Serialized result payload.
        result: Vec<u8>,
    },
    /// The activity failed terminally.
    Failed {
        /// Failure message.
        failure: String,
    },
    /// A cancel was requested.
    CancelRequested {
        /// Requesting identity.
        identity: String,
    },
    /// The cancel was acknowledged.
    Canceled,
    /// An operator terminated the activity.
    Terminated {
        /// Termination reason.
        reason: String,
    },
    /// A timeout fired; carries the firing timer's attempt `stamp` for fencing.
    TimedOut {
        /// The attempt stamp the firing timer was scheduled for.
        stamp: i64,
        /// Which timeout fired.
        timeout_type: TimeoutType,
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
            schedule_attempt_timers(state, ctx, now)?;
        }
        ActivityEvent::Rescheduled { failure } => {
            state.attempt += 1;
            state.stamp += 1;
            state.scheduled_time_nanos = now;
            state.failure = failure.clone();
            state.started_time_nanos = 0;
            schedule_attempt_timers(state, ctx, now)?;
        }
        ActivityEvent::Started { started_time_nanos } => {
            let started = if *started_time_nanos > 0 {
                *started_time_nanos
            } else {
                now
            };
            state.started_time_nanos = started;
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
        ActivityEvent::Failed { failure } => {
            state.failure = failure.clone();
        }
        ActivityEvent::CancelRequested { .. } => {}
        ActivityEvent::Canceled => {
            state.failure = "Activity canceled".to_owned();
        }
        ActivityEvent::Terminated { reason } => {
            state.failure = reason.clone();
        }
        ActivityEvent::TimedOut { timeout_type, .. } => {
            state.failure = format!("activity timeout: {timeout_type:?}");
        }
    }

    state.set_status(to);
    Ok(())
}

/// Schedule the dispatch task and the schedule-to-start / schedule-to-close timers
/// for a freshly scheduled attempt (shared by `Scheduled` and `Rescheduled`).
fn schedule_attempt_timers(
    state: &ActivityState,
    ctx: &mut dyn MutableContext,
    now: i64,
) -> Result<(), ChasmError> {
    if state.schedule_to_start_nanos > 0 {
        schedule_pure(
            ctx,
            SCHEDULE_TO_START_TASK_ID,
            &ScheduleToStartTimer {
                stamp: state.stamp,
                fire_at_nanos: now + state.schedule_to_start_nanos,
            },
        )?;
    }
    if state.schedule_to_close_nanos > 0 {
        schedule_pure(
            ctx,
            SCHEDULE_TO_CLOSE_TASK_ID,
            &ScheduleToCloseTimer {
                stamp: state.stamp,
                fire_at_nanos: now + state.schedule_to_close_nanos,
            },
        )?;
    }
    // The dispatch side-effect task enqueues the attempt to matching post-commit.
    let dispatch = DispatchTask { stamp: state.stamp };
    ctx.add_task(
        TaskKind::SideEffect,
        DISPATCH_TASK_ID,
        encode_task(&dispatch)?,
        None,
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
}
