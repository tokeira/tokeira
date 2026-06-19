//! Pure derivation of activity timeout deadlines from [`ActivityState`].
//!
//! Ground truth: the four timeout tasks in `chasm/lib/activity/activity_tasks.go @
//! v1.31.0` and the timers armed in `statemachine.go @ v1.31.0`. The engine arms a
//! physical timer at the earliest pure-task deadline, but the *firing* decision is
//! re-derived from durable state (not from the disposable timer tasks) so a lost
//! armed entry self-heals from node state — history is authority, timers are
//! derived (`crates/tokeira-runtime/AGENTS.md`). These helpers compute, from a
//! state snapshot and the current clock, which timeout (if any) is due and the next
//! deadline to re-arm.
//!
//! Each timeout's anchor matches the v1.31.0 timer it stands in for:
//!
//! - **schedule-to-start**: `attempt_scheduled_time + schedule_to_start`, valid only
//!   while `SCHEDULED` (re-anchored per attempt, `statemachine.go:109`).
//! - **schedule-to-close**: `scheduled_time + schedule_to_close`, valid until
//!   terminal — anchored at the *original* schedule time and never re-armed on
//!   retry (`statemachine.go:58`; not re-added in `TransitionRescheduled`).
//! - **start-to-close**: `started_time + start_to_close`, valid only while
//!   `STARTED`/`CANCEL_REQUESTED` (`statemachine.go:150`).
//! - **heartbeat**: `max(last_heartbeat, started) + heartbeat`, valid only while
//!   `STARTED`/`CANCEL_REQUESTED`; a heartbeat pushes the anchor out
//!   (`activity_tasks.go` heartbeat `Validate`).

use crate::{
    state::{ActivityState, ActivityStatus},
    statemachine::TimeoutType,
};

/// One candidate timeout: its kind and the Unix-nanosecond deadline it fires at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Candidate {
    timeout_type: TimeoutType,
    deadline: i64,
}

/// The timeout candidates active for `state` (those whose status precondition
/// holds and whose timeout is set). Order is irrelevant; callers pick by deadline.
fn candidates(state: &ActivityState) -> Vec<Candidate> {
    let status = state.status();
    if status.is_terminal() {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(4);

    // schedule-to-close spans attempts and is the only timeout active in every
    // non-terminal status; anchored at the original schedule time.
    if state.schedule_to_close_nanos > 0 {
        out.push(Candidate {
            timeout_type: TimeoutType::ScheduleToClose,
            deadline: state.scheduled_time_nanos + state.schedule_to_close_nanos,
        });
    }

    if status == ActivityStatus::Scheduled {
        if state.schedule_to_start_nanos > 0 {
            out.push(Candidate {
                timeout_type: TimeoutType::ScheduleToStart,
                deadline: state.attempt_scheduled_time_nanos + state.schedule_to_start_nanos,
            });
        }
    } else if matches!(
        status,
        ActivityStatus::Started | ActivityStatus::CancelRequested
    ) {
        if state.start_to_close_nanos > 0 {
            out.push(Candidate {
                timeout_type: TimeoutType::StartToClose,
                deadline: state.started_time_nanos + state.start_to_close_nanos,
            });
        }
        if state.heartbeat_nanos > 0 {
            let anchor = state
                .last_heartbeat_time_nanos
                .max(state.started_time_nanos);
            out.push(Candidate {
                timeout_type: TimeoutType::Heartbeat,
                deadline: anchor + state.heartbeat_nanos,
            });
        }
    }
    out
}

/// The timeout that is due at `now` (deadline `<= now`), or `None` if none is due.
///
/// When several are due simultaneously, schedule-to-close wins
/// (`statemachine.go:343 @ v1.31.0` — "schedule-to-close takes precedence when
/// multiple fire"); otherwise the earliest-deadline timeout fires first, matching
/// the chronological order distinct physical timers would have fired in.
pub fn due_timeout(state: &ActivityState, now: i64) -> Option<TimeoutType> {
    let due: Vec<Candidate> = candidates(state)
        .into_iter()
        .filter(|c| c.deadline <= now)
        .collect();
    if due
        .iter()
        .any(|c| c.timeout_type == TimeoutType::ScheduleToClose)
    {
        return Some(TimeoutType::ScheduleToClose);
    }
    due.into_iter()
        .min_by_key(|c| c.deadline)
        .map(|c| c.timeout_type)
}

/// The earliest timeout deadline for `state` (due or future), or `None` when the
/// activity is terminal or has no timeout armed. The sweeper re-arms the engine's
/// physical timer to this so the next firing is precise rather than busy-polled.
pub fn next_timeout_deadline(state: &ActivityState) -> Option<i64> {
    candidates(state).into_iter().map(|c| c.deadline).min()
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEC: i64 = 1_000_000_000;

    fn scheduled() -> ActivityState {
        let mut s = ActivityState {
            attempt: 1,
            stamp: 1,
            scheduled_time_nanos: 0,
            attempt_scheduled_time_nanos: 0,
            schedule_to_start_nanos: 5 * SEC,
            schedule_to_close_nanos: 30 * SEC,
            ..ActivityState::default()
        };
        s.set_status(ActivityStatus::Scheduled);
        s
    }

    #[test]
    fn schedule_to_start_due_while_scheduled() {
        let s = scheduled();
        assert_eq!(due_timeout(&s, 4 * SEC), None);
        assert_eq!(due_timeout(&s, 5 * SEC), Some(TimeoutType::ScheduleToStart));
    }

    #[test]
    fn schedule_to_close_takes_precedence_on_simultaneous_due() {
        let mut s = scheduled();
        // Make both schedule-to-start and schedule-to-close due at the same instant.
        s.schedule_to_start_nanos = 30 * SEC;
        assert_eq!(
            due_timeout(&s, 30 * SEC),
            Some(TimeoutType::ScheduleToClose)
        );
    }

    #[test]
    fn start_to_close_and_heartbeat_pick_earliest() {
        let mut s = ActivityState {
            attempt: 1,
            stamp: 1,
            started_time_nanos: 0,
            start_to_close_nanos: 10 * SEC,
            heartbeat_nanos: 2 * SEC,
            ..ActivityState::default()
        };
        s.set_status(ActivityStatus::Started);
        // Heartbeat (2s) is earlier than start-to-close (10s).
        assert_eq!(due_timeout(&s, 2 * SEC), Some(TimeoutType::Heartbeat));
    }

    #[test]
    fn heartbeat_anchor_advances_with_last_heartbeat() {
        let mut s = ActivityState {
            attempt: 1,
            stamp: 1,
            started_time_nanos: 0,
            heartbeat_nanos: 2 * SEC,
            last_heartbeat_time_nanos: 3 * SEC,
            ..ActivityState::default()
        };
        s.set_status(ActivityStatus::Started);
        // Anchored at last heartbeat (3s) + 2s = 5s, not started (0) + 2s = 2s.
        assert_eq!(due_timeout(&s, 4 * SEC), None);
        assert_eq!(due_timeout(&s, 5 * SEC), Some(TimeoutType::Heartbeat));
    }

    #[test]
    fn terminal_has_no_timeouts() {
        let mut s = scheduled();
        s.set_status(ActivityStatus::Completed);
        assert_eq!(due_timeout(&s, i64::MAX), None);
        assert_eq!(next_timeout_deadline(&s), None);
    }

    #[test]
    fn next_deadline_is_minimum_candidate() {
        let s = scheduled(); // s2s at 5s, s2c at 30s.
        assert_eq!(next_timeout_deadline(&s), Some(5 * SEC));
    }
}
