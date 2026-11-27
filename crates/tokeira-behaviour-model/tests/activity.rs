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
fn exponential_interval_matches_go_integer_power_rounding() {
    // Expected intervals come from Go's math.Pow and the nanosecond conversion
    // in common/backoff/retry.go:183-185 @ v1.31.0. The negative exponent covers
    // the function's int32 domain, beyond the positive attempts its callers pass.
    let inputs = [
        (1_000_000_000, 1.01, 4),
        (1_000_000_000, 1.2, 4),
        (1_000_000_000, 1.2, 6),
        (1, 3.0, 35),
        (10_000_000_000_000_000, 1.01, -77),
    ];
    let actual = inputs
        .map(|(initial, coefficient, attempt)| exponential_interval(initial, coefficient, attempt));
    assert_eq!(
        actual,
        [
            1_030_300_999,
            1_728_000_000,
            2_488_320_000,
            16_677_181_699_666_568,
            4_601_854_095_137_946,
        ],
        "inputs: {inputs:?}"
    );
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

#[test]
fn policy_validation_order_and_messages() {
    let mismatch = prost_types::Duration {
        seconds: -1,
        nanos: 1,
    };
    let mut p = RetryPolicy {
        initial_interval: Some(mismatch),
        maximum_interval: Some(duration(-1)),
        backoff_coefficient: 0.5,
        maximum_attempts: -1,
        non_retryable_error_types: vec!["TemporalTimeout:bad".into()],
    };
    assert_eq!(
        validate_policy(p.clone()).unwrap_err(),
        "invalid InitialInterval set on retry policy: duration has seconds and nanos with different signs"
    );
    p.initial_interval = Some(duration(-1));
    assert_eq!(
        validate_policy(p.clone()).unwrap_err(),
        "invalid InitialInterval set on retry policy: negative duration"
    );
    p.initial_interval = Some(duration(2));
    assert_eq!(
        validate_policy(p.clone()).unwrap_err(),
        "BackoffCoefficient cannot be less than 1 on retry policy."
    );
    p.backoff_coefficient = 1.0;
    p.maximum_interval = Some(mismatch);
    assert_eq!(
        validate_policy(p.clone()).unwrap_err(),
        "invalid MaximumInterval set on retry policy: duration has seconds and nanos with different signs"
    );
    p.maximum_interval = Some(duration(-1));
    assert_eq!(
        validate_policy(p.clone()).unwrap_err(),
        "invalid MaximumInterval set on retry policy: negative duration"
    );
    p.maximum_interval = Some(duration(1));
    assert_eq!(
        validate_policy(p.clone()).unwrap_err(),
        "MaximumInterval cannot be less than InitialInterval on retry policy."
    );
    p.maximum_interval = Some(duration(2));
    assert_eq!(
        validate_policy(p.clone()).unwrap_err(),
        "MaximumAttempts cannot be negative on retry policy."
    );
    p.maximum_attempts = 0;
    assert_eq!(
        validate_policy(p.clone()).unwrap_err(),
        "Invalid timeout type value: bad."
    );
    p.non_retryable_error_types.clear();
    assert_eq!(validate_policy(p.clone()).unwrap(), p);
}

#[test]
fn policy_one_attempt_preserves_every_field() {
    let p = RetryPolicy {
        initial_interval: Some(prost_types::Duration {
            seconds: -1,
            nanos: 1,
        }),
        maximum_interval: Some(duration(i64::MAX)),
        backoff_coefficient: -1.0,
        maximum_attempts: 1,
        non_retryable_error_types: vec!["TemporalTimeout:Unspecified".into()],
    };
    assert_eq!(validate_policy(p.clone()).unwrap(), p);
}

#[test]
fn policy_caps_before_comparing_intervals() {
    let cap = 3_153_600_000_000_000_000;
    for (initial, maximum, expected_initial, expected_maximum) in [
        (cap - 1, cap, cap - 1, cap),
        (cap, cap + 1, cap, cap),
        (cap + 2, cap + 1, cap, cap),
        (i64::MAX, i64::MAX, cap, cap),
        (0, 0, 0, 0),
        (2, 0, 2, 0),
    ] {
        let p = RetryPolicy {
            initial_interval: Some(duration(initial)),
            maximum_interval: Some(duration(maximum)),
            backoff_coefficient: 1.0,
            ..Default::default()
        };
        let got = validate_policy(p).unwrap();
        assert_eq!(got.initial_interval, Some(duration(expected_initial)));
        assert_eq!(got.maximum_interval, Some(duration(expected_maximum)));
    }
    let p = RetryPolicy {
        backoff_coefficient: f64::NAN,
        ..Default::default()
    };
    let got = validate_policy(p).unwrap();
    assert!(got.backoff_coefficient.is_nan());
    assert_eq!(got.initial_interval, None);
    assert_eq!(got.maximum_interval, None);
}

#[test]
fn policy_timeout_spellings_and_first_invalid_type() {
    for name in [
        "StartToClose",
        "ScheduleToStart",
        "ScheduleToClose",
        "Heartbeat",
        "TIMEOUT_TYPE_START_TO_CLOSE",
        "TIMEOUT_TYPE_SCHEDULE_TO_START",
        "TIMEOUT_TYPE_SCHEDULE_TO_CLOSE",
        "TIMEOUT_TYPE_HEARTBEAT",
    ] {
        let p = RetryPolicy {
            non_retryable_error_types: vec![
                "application".into(),
                format!("TemporalTimeout:{name}"),
            ],
            ..policy()
        };
        assert_eq!(validate_policy(p.clone()).unwrap(), p);
    }
    for name in [
        "",
        "Unspecified",
        "TIMEOUT_TYPE_UNSPECIFIED",
        "1",
        "heartbeat",
        "Heartbeat ",
        "bad",
    ] {
        let p = RetryPolicy {
            non_retryable_error_types: vec![
                format!("TemporalTimeout:{name}"),
                "TemporalTimeout:later".into(),
            ],
            ..policy()
        };
        assert_eq!(
            validate_policy(p).unwrap_err(),
            format!("Invalid timeout type value: {name}.")
        );
    }
}

#[test]
fn policy_defaulting_precedes_validation() {
    let initial = duration(3 * 365 * 24 * 60 * 60 * 1_000_000_000);
    let p = default_policy(RetryPolicy {
        initial_interval: Some(initial),
        ..Default::default()
    });
    assert_eq!(
        p.maximum_interval,
        Some(prost_types::Duration {
            seconds: -8_985_944_073,
            nanos: -709_551_616
        })
    );
    assert_eq!(
        validate_policy(p.clone()).unwrap_err(),
        "invalid MaximumInterval set on retry policy: negative duration"
    );
    let one = RetryPolicy {
        maximum_attempts: 1,
        ..p
    };
    assert_eq!(validate_policy(one.clone()).unwrap(), one);
    assert_eq!(validate_policy(policy()).unwrap(), policy());
}

#[test]
fn duration_value_preserves_intermediate_overflow() {
    // AsDuration keeps seconds overflow even when the exact sum fits; defaulting
    // must therefore derive -100ns or zero, not multiply that exact sum
    // (types/known/durationpb/duration.pb.go:171-187 @ google.golang.org/protobuf v1.36.10;
    // common/retrypolicy/retry_policy.go:49-50 @ v1.31.0).
    for (seconds, nanos, converted, maximum) in [
        (9_223_372_037, -145_224_192, i64::MAX, -100),
        (9_223_372_037, -145_224_193, i64::MAX, -100),
        (9_223_372_037, -145_224_194, i64::MAX, -100),
        (9_223_372_037, -999_999_999, i64::MAX, -100),
        (-9_223_372_037, 145_224_191, i64::MIN, 0),
        (-9_223_372_037, 145_224_192, i64::MIN, 0),
        (-9_223_372_037, 145_224_193, i64::MIN, 0),
        (-9_223_372_037, 999_999_999, i64::MIN, 0),
    ] {
        let initial = prost_types::Duration { seconds, nanos };
        assert_eq!(duration_ns(Some(&initial)), converted);
        let p = default_policy(RetryPolicy {
            initial_interval: Some(initial),
            maximum_attempts: 1,
            ..Default::default()
        });
        assert_eq!(p.initial_interval, Some(initial));
        assert_eq!(p.maximum_interval, Some(duration(maximum)));
        assert_eq!(validate_policy(p.clone()).unwrap(), p);
    }
    for (seconds, nanos, expected) in [
        (9_223_372_036, 854_775_806, i64::MAX - 1),
        (9_223_372_036, 854_775_807, i64::MAX),
        (9_223_372_036, 854_775_808, i64::MAX),
        (-9_223_372_036, -854_775_807, i64::MIN + 1),
        (-9_223_372_036, -854_775_808, i64::MIN),
        (-9_223_372_036, -854_775_809, i64::MIN),
        (1, -1, 999_999_999),
        (-1, 1, -999_999_999),
    ] {
        assert_eq!(
            duration_ns(Some(&prost_types::Duration { seconds, nanos })),
            expected
        );
    }
    assert_eq!(duration_ns(None), 0);
}
