//! Independent comparisons for the activity inputs admitted by the compatibility edge.

use super::*;
use proptest::prelude::*;
use tokeira_behaviour_model::activity as model;
use tokeira_proto::{
    enums::TimeoutType,
    failure::{
        ApplicationFailureInfo, CanceledFailureInfo, Failure, ServerFailureInfo,
        TerminatedFailureInfo, TimeoutFailureInfo, failure::FailureInfo,
    },
};

fn classification(failure: &Failure, excluded: &[String]) {
    // service/history/workflow/retry.go:115-151 @ v1.31.0.
    let actual = activity_retry_classification(failure).is_retryable(excluded);
    assert_eq!(actual, model::retryable(Some(failure), excluded));
}

fn application(kind: &str, non_retryable: bool) -> Failure {
    Failure {
        failure_info: Some(FailureInfo::ApplicationFailureInfo(
            ApplicationFailureInfo {
                r#type: kind.into(),
                non_retryable,
                ..Default::default()
            },
        )),
        ..Default::default()
    }
}

#[test]
fn model_failure_classification_table() {
    let excluded = vec!["listed".into()];
    classification(&Failure::default(), &excluded);
    for non_retryable in [false, true] {
        for kind in ["listed", "Listed", "other"] {
            classification(&application(kind, non_retryable), &excluded);
        }
        classification(
            &Failure {
                failure_info: Some(FailureInfo::ServerFailureInfo(ServerFailureInfo {
                    non_retryable,
                })),
                ..Default::default()
            },
            &excluded,
        );
    }
    for kind in [TimeoutType::StartToClose, TimeoutType::Heartbeat] {
        classification(
            &Failure {
                failure_info: Some(FailureInfo::TimeoutFailureInfo(TimeoutFailureInfo {
                    timeout_type: kind as i32,
                    ..Default::default()
                })),
                ..Default::default()
            },
            &excluded,
        );
    }
}

#[test]
fn model_failure_canceled_and_terminated() {
    classification(
        &Failure {
            failure_info: Some(FailureInfo::CanceledFailureInfo(
                CanceledFailureInfo::default(),
            )),
            ..Default::default()
        },
        &[],
    );
    classification(
        &Failure {
            failure_info: Some(FailureInfo::TerminatedFailureInfo(
                TerminatedFailureInfo::default(),
            )),
            ..Default::default()
        },
        &[],
    );
}

#[test]
fn model_failure_timeout_types() {
    for kind in [
        TimeoutType::Unspecified,
        TimeoutType::ScheduleToStart,
        TimeoutType::ScheduleToClose,
        TimeoutType::StartToClose,
        TimeoutType::Heartbeat,
    ] {
        classification(
            &Failure {
                failure_info: Some(FailureInfo::TimeoutFailureInfo(TimeoutFailureInfo {
                    timeout_type: kind as i32,
                    ..Default::default()
                })),
                ..Default::default()
            },
            &[
                "TemporalTimeout:StartToClose".into(),
                "TemporalTimeout:Heartbeat".into(),
            ],
        );
    }
}

#[test]
fn model_failure_does_not_classify_cause() {
    classification(
        &Failure {
            cause: Some(Box::new(application("listed", true))),
            ..Default::default()
        },
        &["listed".into()],
    );
}

#[test]
fn model_failure_empty_type_matches_exactly() {
    classification(&application("", false), &[String::new()]);
}

fn defaults(policy: Option<&proto_common::RetryPolicy>) {
    let expected = model::default_policy(policy.cloned().unwrap_or_default());
    let actual = activity_retry_policy_with_defaults(policy);
    assert_eq!(actual.initial_interval, expected.initial_interval);
    assert_eq!(actual.maximum_interval, expected.maximum_interval);
    assert_eq!(
        actual.backoff_coefficient.to_bits(),
        expected.backoff_coefficient.to_bits()
    );
    assert_eq!(actual.maximum_attempts, expected.maximum_attempts);
    assert_eq!(
        actual.non_retryable_error_types,
        expected.non_retryable_error_types
    );
}

#[test]
fn model_activity_policy_defaults_table() {
    defaults(None);
    defaults(Some(&proto_common::RetryPolicy::default()));
    for attempts in [0, 1, i32::MAX] {
        for (initial, maximum, coefficient) in [
            (0, 0, 0.0),
            (1, 0, 1.0),
            (1_000_000_000, 2_000_000_000, 2.0),
        ] {
            defaults(Some(&proto_common::RetryPolicy {
                initial_interval: Some(model::duration(initial)),
                maximum_interval: Some(model::duration(maximum)),
                backoff_coefficient: coefficient,
                maximum_attempts: attempts,
                ..Default::default()
            }));
        }
    }
    // EnsureDefaults does not impose Validate's cap, and its multiplication wraps
    // at signed nanoseconds (common/retrypolicy/retry_policy.go:45-50 @ v1.31.0).
    for initial in [
        31_536_000_000_000_000 - 1,
        31_536_000_000_000_000,
        31_536_000_000_000_000 + 1,
        i64::MAX / 100 - 1,
        i64::MAX / 100,
        i64::MAX / 100 + 1,
        i64::MAX - 1,
        i64::MAX,
        i64::MIN,
        -1,
    ] {
        defaults(Some(&proto_common::RetryPolicy {
            initial_interval: Some(model::duration(initial)),
            maximum_attempts: 1,
            ..Default::default()
        }));
    }
    for (seconds, nanos) in [
        (9_223_372_037, -145_224_192),
        (9_223_372_037, -145_224_193),
        (9_223_372_037, -145_224_194),
        (9_223_372_037, -999_999_999),
        (-9_223_372_037, 145_224_191),
        (-9_223_372_037, 145_224_192),
        (-9_223_372_037, 145_224_193),
        (-9_223_372_037, 999_999_999),
        (9_223_372_036, 854_775_808),
        (-9_223_372_036, -854_775_809),
        (315_576_000_000, 999_999_999),
        (-315_576_000_000, -999_999_999),
        (1, -1),
        (-1, 1),
        (1, -1_000_000_000),
    ] {
        defaults(Some(&proto_common::RetryPolicy {
            initial_interval: Some(prost_types::Duration { seconds, nanos }),
            maximum_attempts: 1,
            ..Default::default()
        }));
    }
}

fn validation(policy: &proto_common::RetryPolicy) -> Result<RetryPolicy, String> {
    let expected = model::validate_policy(model::default_policy(policy.clone()));
    let actual = validate_activity_retry_policy(activity_retry_policy_with_defaults(Some(policy)))
        .map_err(|error| error.to_string());
    match (&actual, expected) {
        (Err(actual), Err(expected)) => assert_eq!(*actual, expected),
        (Ok(actual), Ok(expected)) => {
            assert_eq!(
                actual.initial_interval.whole_nanoseconds(),
                i128::from(model::duration_ns(expected.initial_interval.as_ref()))
            );
            assert_eq!(
                actual.maximum_interval.unwrap().whole_nanoseconds(),
                i128::from(model::duration_ns(expected.maximum_interval.as_ref()))
            );
            assert_eq!(
                actual.backoff_coefficient.to_bits(),
                expected.backoff_coefficient.to_bits()
            );
            assert_eq!(actual.maximum_attempts, expected.maximum_attempts as u32);
            assert_eq!(
                actual.non_retryable_error_types,
                expected.non_retryable_error_types
            );
        }
        (actual, expected) => panic!("policy {policy:?}: actual {actual:?}, expected {expected:?}"),
    }
    actual
}

#[test]
fn model_activity_policy_validation_table() {
    let cap = 3_153_600_000_000_000_000;
    let mismatch = prost_types::Duration {
        seconds: -1,
        nanos: 1,
    };
    let negative = model::duration(-1);
    // Multiple invalid fields establish first-error precedence, not just isolated
    // rejection (common/retrypolicy/retry_policy.go:65-94 @ v1.31.0).
    for (initial, maximum, coefficient, attempts, excluded, error) in [
        (
            mismatch,
            model::duration(i64::MAX),
            -1.0,
            1,
            "TemporalTimeout:bad",
            None,
        ),
        (
            mismatch,
            negative,
            0.5,
            -1,
            "TemporalTimeout:bad",
            Some(
                "invalid InitialInterval set on retry policy: duration has seconds and nanos with different signs",
            ),
        ),
        (
            negative,
            mismatch,
            0.5,
            -1,
            "TemporalTimeout:bad",
            Some("invalid InitialInterval set on retry policy: negative duration"),
        ),
        (
            model::duration(1),
            mismatch,
            0.5,
            -1,
            "TemporalTimeout:bad",
            Some("BackoffCoefficient cannot be less than 1 on retry policy."),
        ),
        (
            model::duration(1),
            mismatch,
            1.0,
            -1,
            "TemporalTimeout:bad",
            Some(
                "invalid MaximumInterval set on retry policy: duration has seconds and nanos with different signs",
            ),
        ),
        (
            model::duration(1),
            negative,
            1.0,
            -1,
            "TemporalTimeout:bad",
            Some("invalid MaximumInterval set on retry policy: negative duration"),
        ),
        (
            model::duration(2),
            model::duration(1),
            1.0,
            -1,
            "TemporalTimeout:bad",
            Some("MaximumInterval cannot be less than InitialInterval on retry policy."),
        ),
        (
            model::duration(1),
            model::duration(1),
            1.0,
            -1,
            "TemporalTimeout:bad",
            Some("MaximumAttempts cannot be negative on retry policy."),
        ),
        (
            model::duration(1),
            model::duration(1),
            1.0,
            0,
            "TemporalTimeout:bad",
            Some("Invalid timeout type value: bad."),
        ),
        (
            model::duration(cap + 2),
            model::duration(cap + 1),
            1.0,
            0,
            "application",
            None,
        ),
        (
            model::duration(cap - 1),
            model::duration(cap),
            f64::NAN,
            0,
            "application",
            None,
        ),
        (
            model::duration(1),
            model::duration(cap + 1),
            f64::INFINITY,
            0,
            "application",
            None,
        ),
        (
            model::duration(1i64 << 62),
            model::duration(0),
            1.0,
            0,
            "application",
            None,
        ),
        (
            prost_types::Duration {
                seconds: 1,
                nanos: -1_000_000_000,
            },
            model::duration(cap),
            1.0,
            0,
            "application",
            None,
        ),
    ] {
        let policy = proto_common::RetryPolicy {
            initial_interval: Some(initial),
            maximum_interval: Some(maximum),
            backoff_coefficient: coefficient,
            maximum_attempts: attempts,
            non_retryable_error_types: vec![excluded.into()],
        };
        assert_eq!(validation(&policy).err().as_deref(), error);
    }
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
        validation(&proto_common::RetryPolicy {
            non_retryable_error_types: vec![
                "application".into(),
                format!("TemporalTimeout:{name}"),
            ],
            ..Default::default()
        })
        .unwrap();
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
        let result = validation(&proto_common::RetryPolicy {
            non_retryable_error_types: vec![
                format!("TemporalTimeout:{name}"),
                "TemporalTimeout:later".into(),
            ],
            ..Default::default()
        });
        assert_eq!(
            result.unwrap_err(),
            format!("Invalid timeout type value: {name}.")
        );
    }
}

fn policy_duration() -> impl Strategy<Value = Option<prost_types::Duration>> {
    prop::option::of(prop_oneof![
        3 => prop::sample::select(vec![
            (9_223_372_037, -145_224_192), (9_223_372_037, -145_224_193),
            (9_223_372_037, -145_224_194), (9_223_372_037, -999_999_999),
            (-9_223_372_037, 145_224_191), (-9_223_372_037, 145_224_192),
            (-9_223_372_037, 145_224_193), (-9_223_372_037, 999_999_999),
        ]).prop_map(|(seconds, nanos)| prost_types::Duration { seconds, nanos }),
        3 => (-315_576_000_000i64..=315_576_000_000, -999_999_999i32..=999_999_999)
            .prop_map(|(seconds, nanos)| prost_types::Duration { seconds, nanos }),
        1 => (any::<i64>(), any::<i32>())
            .prop_map(|(seconds, nanos)| prost_types::Duration { seconds, nanos }),
        4 => prop::sample::select(vec![0, 1, -1, 1_000_000_000,
            31_536_000_000_000_000 - 1, 31_536_000_000_000_000 + 1,
            i64::MAX / 100, i64::MAX / 100 + 1,
            3_153_600_000_000_000_000 - 1, 3_153_600_000_000_000_000,
            3_153_600_000_000_000_000 + 1, i64::MIN, i64::MAX]).prop_map(model::duration),
    ])
}

fn policy_coefficient() -> impl Strategy<Value = f64> {
    prop_oneof![
        4 => prop::sample::select(vec![f64::NAN, f64::NEG_INFINITY, f64::INFINITY, -1.0, -0.0, 0.0, 0.5, 1.0, 2.0]),
        1 => any::<f64>(),
    ]
}

fn policy_attempts() -> impl Strategy<Value = i32> {
    prop_oneof![4 => prop::sample::select(vec![i32::MIN, -1, 0, 1, 2, i32::MAX]), 1 => any::<i32>()]
}

fn policy_exclusions() -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec(
        prop::sample::select(vec![
            "application",
            "temporalTimeout:bad",
            "TemporalTimeout:StartToClose",
            "TemporalTimeout:ScheduleToStart",
            "TemporalTimeout:ScheduleToClose",
            "TemporalTimeout:Heartbeat",
            "TemporalTimeout:TIMEOUT_TYPE_START_TO_CLOSE",
            "TemporalTimeout:TIMEOUT_TYPE_SCHEDULE_TO_START",
            "TemporalTimeout:TIMEOUT_TYPE_SCHEDULE_TO_CLOSE",
            "TemporalTimeout:TIMEOUT_TYPE_HEARTBEAT",
            "TemporalTimeout:Unspecified",
            "TemporalTimeout:TIMEOUT_TYPE_UNSPECIFIED",
            "TemporalTimeout:",
            "TemporalTimeout:1",
            "TemporalTimeout:heartbeat",
            "TemporalTimeout:Heartbeat ",
        ]),
        0..4,
    )
    .prop_map(|values| values.into_iter().map(str::to_owned).collect())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]
    // Feature: conformance-harness, Property 10. Defaulting preserves raw fields and wraps derived maxima before validation.
    #[test]
    fn model_policy_defaults_generated(initial in policy_duration(), maximum in policy_duration(), coefficient in policy_coefficient(), attempts in policy_attempts()) {
        defaults(Some(&proto_common::RetryPolicy { initial_interval: initial, maximum_interval: maximum, backoff_coefficient: coefficient, maximum_attempts: attempts, ..Default::default() }));
    }
    // Feature: conformance-harness, Property 10. Validation preserves the first error or the accepted normalized policy.
    #[test]
    fn model_policy_validation_generated(initial in policy_duration(), maximum in policy_duration(), coefficient in policy_coefficient(), attempts in policy_attempts(), excluded in policy_exclusions()) {
        let _ = validation(&proto_common::RetryPolicy { initial_interval: initial, maximum_interval: maximum, backoff_coefficient: coefficient, maximum_attempts: attempts, non_retryable_error_types: excluded });
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    // Feature: conformance-harness, Property 10. Application and server failure classification preserves exact type matching.
    #[test]
    fn model_failure_classification_generated(kind in "[a-z]{1,20}", excluded in "[a-z]{1,20}", non_retryable in any::<bool>(), server in any::<bool>()) {
        let failure = if server { Failure { failure_info: Some(FailureInfo::ServerFailureInfo(ServerFailureInfo { non_retryable })), ..Default::default() } } else { application(&kind, non_retryable) };
        classification(&failure, &[excluded, "listed".into()]);
    }
}
