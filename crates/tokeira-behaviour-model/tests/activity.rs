//! Source-boundary examples for the independent activity decisions.

use proptest::prelude::*;
use tokeira_behaviour_model::activity::*;
use tokeira_proto::{
    common::RetryPolicy,
    enums::{RetryState, TimeoutType},
    failure::{ApplicationFailureInfo, Failure, TimeoutFailureInfo, failure::FailureInfo},
};

fn policy() -> RetryPolicy {
    default_policy(RetryPolicy::default())
}
fn application(delay: Option<i64>) -> Failure {
    Failure {
        failure_info: Some(FailureInfo::ApplicationFailureInfo(
            ApplicationFailureInfo {
                next_retry_delay: delay.map(duration),
                ..Default::default()
            },
        )),
        ..Default::default()
    }
}

#[test]
fn retry_precedence_branches() {
    // service/history/workflow/mutable_state_impl.go:6243-6319 @ v1.31.0.
    let p = policy();
    for cancel in [false, true] {
        assert_eq!(
            retry_precedence(None, cancel, true, None),
            RetryPrecedence::Stop(RetryState::RetryPolicyNotSet)
        );
    }
    for kind in [
        TimeoutType::ScheduleToStart,
        TimeoutType::ScheduleToClose,
        TimeoutType::Unspecified,
        TimeoutType::StartToClose,
        TimeoutType::Heartbeat,
    ] {
        let failure = Failure {
            failure_info: Some(FailureInfo::TimeoutFailureInfo(TimeoutFailureInfo {
                timeout_type: kind as i32,
                ..Default::default()
            })),
            ..Default::default()
        };
        assert_eq!(
            retry_precedence(Some(&p), true, true, Some(&failure)),
            RetryPrecedence::Stop(RetryState::CancelRequested)
        );
        let expected = match kind {
            TimeoutType::ScheduleToStart | TimeoutType::ScheduleToClose => {
                RetryPrecedence::Stop(RetryState::Timeout)
            }
            TimeoutType::Unspecified => RetryPrecedence::Stop(RetryState::NonRetryableFailure),
            _ => RetryPrecedence::Pause,
        };
        assert_eq!(
            retry_precedence(Some(&p), false, true, Some(&failure)),
            expected
        );
    }
    for maximum_attempts in [0, 1] {
        assert_eq!(
            retry(
                &RetryInput {
                    policy: Some(RetryPolicy {
                        maximum_attempts,
                        ..p.clone()
                    }),
                    attempt: 1,
                    paused: true,
                    expiration: Some(-1),
                    ..Default::default()
                },
                None,
                0
            ),
            RetryDecision::Paused
        );
    }
}

#[test]
fn retry_window_branch_boundaries() {
    // service/history/workflow/retry.go:81-112 @ v1.31.0.
    for (attempt, attempts, interval, maximum, expiration, expected) in [
        (
            0,
            1,
            1,
            0,
            None,
            RetryDecision::Stop(RetryState::MaximumAttemptsReached),
        ),
        (
            i32::MAX,
            i32::MAX,
            1,
            0,
            None,
            RetryDecision::Stop(RetryState::MaximumAttemptsReached),
        ),
        (1, 0, 0, 0, None, RetryDecision::Stop(RetryState::Timeout)),
        (1, 0, -1, 0, None, RetryDecision::Stop(RetryState::Timeout)),
        (1, 0, 0, 5, None, RetryDecision::After(5)),
        (1, 0, -1, 5, None, RetryDecision::After(5)),
        (1, 0, 10, 5, None, RetryDecision::After(5)),
        (1, 0, 5, 5, Some(6), RetryDecision::After(5)),
        (
            1,
            0,
            5,
            5,
            Some(4),
            RetryDecision::Stop(RetryState::Timeout),
        ),
        (
            1,
            0,
            i64::MAX,
            0,
            Some(i128::from(i64::MAX) + 1),
            RetryDecision::After(i64::MAX),
        ),
    ] {
        assert_eq!(
            next_backoff(0, attempt, attempts, interval, maximum, expiration),
            expected
        );
    }
}

#[test]
fn retry_expiration_equality_boundaries() {
    // activity.retry-expiration-equality: service/history/workflow/retry.go:109-112 @ v1.31.0.
    for interval in [1, 5, i64::MAX] {
        assert_eq!(
            next_backoff(0, 1, 0, interval, interval, Some(i128::from(interval))),
            RetryDecision::After(interval)
        );
    }
}

#[test]
fn requested_delay_including_zero_and_negative() {
    // service/history/workflow/retry.go:56-68; service/history/workflow/mutable_state_impl.go:6300-6317 @ v1.31.0.
    let input = RetryInput {
        policy: Some(policy()),
        attempt: 1,
        ..Default::default()
    };
    for (delay, expected) in [
        (None, RetryDecision::After(1_000_000_000)),
        (Some(200_000_000_000), RetryDecision::After(200_000_000_000)),
        (Some(0), RetryDecision::Stop(RetryState::Timeout)),
        (Some(-1), RetryDecision::After(-1)),
    ] {
        assert_eq!(retry(&input, Some(&application(delay)), 0), expected);
    }
}

#[test]
fn options_interval_boundaries() {
    // service/history/workflow/activity.go:238-251; common/backoff/retry.go:180-185 @ v1.31.0.
    for (initial, maximum, expected) in [
        (0, Some(5), 15),
        (2, Some(0), 14),
        (1, None, 12),
        (0, None, 20),
        (5_000_000_000_000_000_000, None, i128::from(i64::MAX) + 10),
    ] {
        let p = RetryPolicy {
            initial_interval: Some(duration(initial)),
            maximum_interval: maximum.map(duration),
            backoff_coefficient: 2.0,
            ..Default::default()
        };
        assert_eq!(next_scheduled_time(&p, 1, 20, Some(10)), 20);
        assert_eq!(next_scheduled_time(&p, 2, 20, Some(10)), expected);
    }
    assert_eq!(next_scheduled_time(&policy(), 2, 20, None), 2_000_000_000);
    assert_eq!(
        duration_ns(Some(&prost_types::Duration {
            seconds: i64::MAX,
            nanos: 0
        })),
        i64::MAX
    );
    assert_eq!(exponential_interval(-1, 2.0, 2), 0);
}

#[test]
fn model_schedule_to_start_close_tie() {
    // Only ScheduleToClose is supplied: chasm/lib/activity/validator.go:166-176,
    // reached through service/history/api/command_attr_validator.go:112-131 @ v1.31.0.
    let scheduled_time = 10_000_000_000;
    let timeout = 5_000_000_000;
    let timing = ActivityTiming {
        scheduled: true,
        scheduled_time,
        first_scheduled_time: Some(scheduled_time),
        schedule_to_close: timeout,
        schedule_to_start: timeout,
        start_to_close: timeout,
        ..Default::default()
    };
    // Unstarted attempts omit StartToClose; numeric timeout order breaks the tie:
    // service/history/workflow/timer_sequence.go:229-319,393-425 @ v1.31.0.
    let candidates = deadlines(&timing);
    let deadline = scheduled_time + i128::from(timeout);
    assert_eq!(
        candidates,
        [
            (deadline, TimeoutType::ScheduleToStart),
            (deadline, TimeoutType::ScheduleToClose)
        ]
    );
    let now = deadline + 1_000_000_000;
    let &(at, kind) = candidates
        .iter()
        .find(|(at, _)| timer_due(*at, now, 0))
        .unwrap();
    assert_eq!(at, deadline);
    let failure = Failure {
        failure_info: Some(FailureInfo::TimeoutFailureInfo(TimeoutFailureInfo {
            timeout_type: kind as i32,
            ..Default::default()
        })),
        ..Default::default()
    };
    let result = retry(
        &RetryInput {
            policy: Some(policy()),
            attempt: 1,
            expiration: Some(deadline),
            ..Default::default()
        },
        Some(&failure),
        now,
    );
    // RetryActivity ends the first timer with Timeout but preserves ScheduleToStart:
    // service/history/workflow/mutable_state_impl.go:6260-6266 and
    // service/history/timer_queue_active_task_executor.go:300-316 @ v1.31.0.
    assert_eq!(
        timeout_disposition(1, 1, kind, result),
        TimeoutDecision::Closed(TimeoutType::ScheduleToStart, RetryState::Timeout)
    );
    // Terminal removal prevents the remaining ScheduleToClose timer from resolving
    // the activity again: service/history/workflow/mutable_state_impl.go:4345-4351;
    // service/history/timer_queue_active_task_executor.go:243-267 @ v1.31.0.
}

#[test]
fn timeout_sort_and_result_boundaries() {
    // service/history/workflow/timer_sequence.go:174-210,229-359,393-425 @ v1.31.0.
    let timing = ActivityTiming {
        scheduled: true,
        started_time: Some(10),
        heartbeat_time: Some(1),
        start_to_close: 5,
        heartbeat: 5,
        schedule_to_close: 15,
        ..Default::default()
    };
    assert_eq!(
        deadlines(&timing),
        vec![
            (15, TimeoutType::StartToClose),
            (15, TimeoutType::ScheduleToClose),
            (15, TimeoutType::Heartbeat)
        ]
    );
    assert!(
        deadlines(&ActivityTiming {
            paused: true,
            ..timing.clone()
        })
        .is_empty()
    );
    assert!(
        deadlines(&ActivityTiming {
            scheduled: false,
            ..timing
        })
        .is_empty()
    );
    // service/history/queues/queue_scheduled.go:280-300 @ v1.31.0.
    assert!(timer_due(1_000_001, 1_000_000, 0));
    assert!(timer_due(1_000_000, 0, 1_000_000));
    assert!(!timer_due(1_000_000, 999_999, 0));
    // service/history/timer_queue_active_task_executor.go:292-361 @ v1.31.0.
    for kind in [
        TimeoutType::ScheduleToStart,
        TimeoutType::ScheduleToClose,
        TimeoutType::StartToClose,
        TimeoutType::Heartbeat,
    ] {
        assert_eq!(
            timeout_disposition(1, 2, kind, RetryDecision::After(1)),
            TimeoutDecision::Stale
        );
        assert_eq!(
            timeout_disposition(2, 2, kind, RetryDecision::Paused),
            TimeoutDecision::Retained
        );
        assert_eq!(
            timeout_disposition(2, 2, kind, RetryDecision::Stop(RetryState::Timeout)),
            TimeoutDecision::Closed(
                if kind == TimeoutType::ScheduleToStart {
                    kind
                } else {
                    TimeoutType::ScheduleToClose
                },
                RetryState::Timeout
            )
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    // Feature: conformance-harness, Property 10. activity.retry-expiration-equality permits a start at expiration.
    #[test]
    fn exact_retry_expiration(now in -1_000_000_000_000i128..1_000_000_000_000, delay in 1i64..i64::MAX) {
        // service/history/workflow/retry.go:109-112 @ v1.31.0.
        let end = now + i128::from(delay);
        prop_assert_eq!(next_backoff(now, 1, 0, delay, 0, Some(end)), RetryDecision::After(delay));
    }

    // Feature: conformance-harness, Property 10. activity.retry-window distinguishes recordable before/after outcomes.
    #[test]
    fn retry_window_before_and_after_expiration(now in -1_000_000_000_000i128..1_000_000_000_000, delay in 1i64..i64::MAX, separation in 1_000_000i128..1_000_000_000) {
        // service/history/workflow/retry.go:109-112 @ v1.31.0.
        let end = now + i128::from(delay);
        prop_assert_eq!(next_backoff(now, 1, 0, delay, 0, Some(end + separation)), RetryDecision::After(delay));
        prop_assert_eq!(next_backoff(now, 1, 0, delay, 0, Some(end - separation)), RetryDecision::Stop(RetryState::Timeout));
    }
}
