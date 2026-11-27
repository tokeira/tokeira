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
    // common/retrypolicy/retry_policy.go:32-56 @ v1.31.0.
    let expected = model::default_policy(policy.cloned().unwrap_or_default());
    let actual = activity_retry_policy_with_defaults(policy);
    assert_eq!(
        actual.initial_interval.whole_nanoseconds(),
        i128::from(model::duration_ns(expected.initial_interval.as_ref()))
    );
    assert_eq!(
        actual.maximum_interval.unwrap().whole_nanoseconds(),
        i128::from(model::duration_ns(expected.maximum_interval.as_ref()))
    );
    assert_eq!(actual.backoff_coefficient, expected.backoff_coefficient);
    assert_eq!(actual.maximum_attempts, expected.maximum_attempts as u32);
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
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    // Feature: conformance-harness, Property 10. Admitted policy defaults preserve explicit nonzero values.
    #[test]
    fn model_policy_defaults_generated(initial in 0i64..1_000_000, maximum in 0i64..1_000_000, coefficient in 0u32..5, attempts in 0i32..1000) {
        defaults(Some(&proto_common::RetryPolicy { initial_interval: Some(model::duration(initial)), maximum_interval: Some(model::duration(maximum)), backoff_coefficient: f64::from(coefficient), maximum_attempts: attempts, ..Default::default() }));
    }
    // Feature: conformance-harness, Property 10. Application and server failure classification preserves exact type matching.
    #[test]
    fn model_failure_classification_generated(kind in "[a-z]{1,20}", excluded in "[a-z]{1,20}", non_retryable in any::<bool>(), server in any::<bool>()) {
        let failure = if server { Failure { failure_info: Some(FailureInfo::ServerFailureInfo(ServerFailureInfo { non_retryable })), ..Default::default() } } else { application(&kind, non_retryable) };
        classification(&failure, &[excluded, "listed".into()]);
    }
}
