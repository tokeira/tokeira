//! The pure activity retry decision.
//!
//! Ground truth: `chasm/lib/activity/activity.go @ v1.31.0` — `shouldRetry` +
//! `hasEnoughTimeForRetry`. Given the current [`ActivityState`], the transition
//! clock, and an optional override interval (the worker failure's
//! `NextRetryDelay`), decide whether the activity reschedules and with what
//! backoff. This is the pure, proto-free decision the edge and the timeout sweeper
//! both feed into; whether a *worker failure* is retryable at all
//! (`ApplicationFailureInfo`/`NonRetryable`/non-retryable types) is decided at the
//! edge from the failure proto, not here.

use crate::{
    backoff::exponential_retry_interval,
    state::{ActivityState, ActivityStatus},
};

/// The outcome of a retry decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryOutcome {
    /// Reschedule the activity after this backoff interval (in nanoseconds).
    Reschedule(i64),
    /// No retry — the activity goes terminal.
    Terminal,
}

/// Decide whether the activity reschedules, mirroring `shouldRetry` +
/// `hasEnoughTimeForRetry @ v1.31.0`.
///
/// Reschedules iff **all** hold:
/// 1. a reschedule is legal from the current status — `TransitionRescheduled.Possible`
///    is true only from `STARTED` (`statemachine.go:87 @ v1.31.0`), so a timeout
///    firing from `CANCEL_REQUESTED` goes terminal;
/// 2. attempts remain — `maximum_attempts == 0` (unlimited) or `attempt <
///    maximum_attempts` (`attempt.Count < MaximumAttempts`);
/// 3. there is enough time before the schedule-to-close deadline —
///    `now + interval < scheduled_time + schedule_to_close` (or no schedule-to-close,
///    which is always enough).
///
/// `override_nanos > 0` (the failure's `NextRetryDelay`) replaces the exponential
/// interval; otherwise the interval is
/// [`exponential_retry_interval`](crate::backoff::exponential_retry_interval) over
/// the current `attempt` count, exactly as `hasEnoughTimeForRetry` computes it.
pub fn retry_decision(state: &ActivityState, now: i64, override_nanos: i64) -> RetryOutcome {
    // (1) Reschedule is only possible from STARTED (the v1.31.0 transition source
    // set). A timeout firing from CANCEL_REQUESTED therefore cannot retry.
    if state.status() != ActivityStatus::Started {
        return RetryOutcome::Terminal;
    }

    // (2) Enough attempts: unlimited (max == 0) or the current count is still below
    // the cap. `state.attempt` is the 1-based count of the attempt being failed.
    let enough_attempts = state.maximum_attempts == 0 || state.attempt < state.maximum_attempts;
    if !enough_attempts {
        return RetryOutcome::Terminal;
    }

    // The interval: honour the override (NextRetryDelay) when positive, else the
    // exponential backoff for the current attempt count.
    let interval = if override_nanos > 0 {
        override_nanos
    } else {
        exponential_retry_interval(
            state.retry_initial_interval_nanos,
            state.retry_backoff_coefficient,
            state.retry_maximum_interval_nanos,
            state.attempt,
        )
    };

    // (3) Budget: an unset schedule-to-close is always enough; otherwise the next
    // attempt must start strictly before the schedule-to-close deadline anchored at
    // the *original* schedule time (`a.ScheduleTime`, which is not advanced on
    // retry — `activity.go:537 @ v1.31.0`).
    if state.schedule_to_close_nanos == 0 {
        return RetryOutcome::Reschedule(interval);
    }
    let deadline = state.scheduled_time_nanos + state.schedule_to_close_nanos;
    if now + interval < deadline {
        RetryOutcome::Reschedule(interval)
    } else {
        RetryOutcome::Terminal
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEC: i64 = 1_000_000_000;

    fn started(attempt: i32) -> ActivityState {
        let mut s = ActivityState {
            attempt,
            stamp: attempt as i64,
            retry_initial_interval_nanos: SEC,
            retry_backoff_coefficient: 2.0,
            retry_maximum_interval_nanos: 100 * SEC,
            scheduled_time_nanos: 0,
            ..ActivityState::default()
        };
        s.set_status(ActivityStatus::Started);
        s
    }

    #[test]
    fn unlimited_attempts_reschedules_with_exponential_interval() {
        // attempt 1, no schedule-to-close → reschedule after the initial interval.
        assert_eq!(
            retry_decision(&started(1), 0, 0),
            RetryOutcome::Reschedule(SEC)
        );
        // attempt 2 → 2s backoff.
        assert_eq!(
            retry_decision(&started(2), 0, 0),
            RetryOutcome::Reschedule(2 * SEC)
        );
    }

    #[test]
    fn exhausted_attempts_is_terminal() {
        let mut s = started(3);
        s.maximum_attempts = 3; // attempt 3 of 3 → no retry.
        assert_eq!(retry_decision(&s, 0, 0), RetryOutcome::Terminal);
    }

    #[test]
    fn attempt_below_max_reschedules() {
        let mut s = started(2);
        s.maximum_attempts = 3; // attempt 2 of 3 → retry.
        assert_eq!(retry_decision(&s, 0, 0), RetryOutcome::Reschedule(2 * SEC));
    }

    #[test]
    fn override_interval_takes_precedence() {
        assert_eq!(
            retry_decision(&started(2), 0, 5 * SEC),
            RetryOutcome::Reschedule(5 * SEC)
        );
    }

    #[test]
    fn insufficient_budget_is_terminal() {
        // schedule_to_close = 10s anchored at scheduled_time 0; now = 9.5s, the 1s
        // backoff would land at 10.5s > 10s deadline → no retry.
        let mut s = started(1);
        s.schedule_to_close_nanos = 10 * SEC;
        let now = 9_500_000_000; // 9.5s
        assert_eq!(retry_decision(&s, now, 0), RetryOutcome::Terminal);
    }

    #[test]
    fn sufficient_budget_reschedules() {
        let mut s = started(1);
        s.schedule_to_close_nanos = 10 * SEC;
        let now = 2 * SEC; // 2s + 1s backoff = 3s < 10s deadline.
        assert_eq!(retry_decision(&s, now, 0), RetryOutcome::Reschedule(SEC));
    }

    #[test]
    fn not_started_is_terminal() {
        // A reschedule is impossible from CANCEL_REQUESTED (only STARTED).
        let mut s = started(1);
        s.set_status(ActivityStatus::CancelRequested);
        assert_eq!(retry_decision(&s, 0, 0), RetryOutcome::Terminal);
    }
}
