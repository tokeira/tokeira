//! Activity decisions compared to independently sourced expectations on a real
//! in-memory runtime. Ordinary agreements remain provisional; exact instants use
//! source-only evidence.

use super::*;
use crate::retry::{RetryDecision as EngineRetry, evaluate_activity_retry};
use tokeira_proto::{
    common::RetryPolicy as WirePolicy, enums::RetryState as WireRetry,
    failure::ApplicationFailureInfo,
};

pub(super) fn wire_policy(policy: &RetryPolicy) -> WirePolicy {
    WirePolicy {
        initial_interval: Some(model::duration(
            policy
                .initial_interval
                .whole_nanoseconds()
                .try_into()
                .unwrap(),
        )),
        backoff_coefficient: policy.backoff_coefficient,
        maximum_interval: policy
            .maximum_interval
            .map(|d| model::duration(d.whole_nanoseconds().try_into().unwrap())),
        maximum_attempts: policy.maximum_attempts.try_into().unwrap(),
        non_retryable_error_types: policy.non_retryable_error_types.clone(),
    }
}

pub(super) fn application_failure(non_retryable: bool, kind: &str, delay: Option<i64>) -> Failure {
    Failure {
        failure_info: Some(FailureInfo::ApplicationFailureInfo(
            ApplicationFailureInfo {
                non_retryable,
                r#type: kind.into(),
                next_retry_delay: delay.map(model::duration),
                ..Default::default()
            },
        )),
        ..Default::default()
    }
}

pub(super) fn kernel_retry_state(state: WireRetry) -> RetryState {
    match state {
        WireRetry::InProgress => RetryState::InProgress,
        WireRetry::NonRetryableFailure => RetryState::NonRetryableFailure,
        WireRetry::Timeout => RetryState::Timeout,
        WireRetry::MaximumAttemptsReached => RetryState::MaximumAttemptsReached,
        WireRetry::RetryPolicyNotSet => RetryState::RetryPolicyNotSet,
        WireRetry::InternalServerError => RetryState::InternalServerError,
        WireRetry::CancelRequested => RetryState::CancelRequested,
        WireRetry::Unspecified => panic!("model must choose a retry state"),
    }
}

fn check_retry(
    policy: &RetryPolicy,
    attempt: u32,
    non_retryable: bool,
    error: &str,
    expiration: Option<i128>,
) {
    let expected = model::retry(
        &model::RetryInput {
            policy: Some(wire_policy(policy)),
            attempt: attempt as i32,
            expiration,
            ..Default::default()
        },
        Some(&application_failure(non_retryable, error, None)),
        0,
    );
    let actual = evaluate_activity_retry(
        policy,
        attempt,
        &crate::retry::ActivityFailure::Application {
            error_type: error.into(),
            non_retryable,
            next_retry_delay: None,
        },
        OffsetDateTime::UNIX_EPOCH,
        expiration.map(|t| OffsetDateTime::from_unix_timestamp_nanos(t).unwrap()),
    );
    match (actual, expected) {
        (
            EngineRetry::Retry {
                next_attempt,
                backoff,
            },
            model::RetryDecision::After(delay),
        ) => {
            assert_eq!(next_attempt, attempt + 1);
            assert_eq!(backoff.whole_nanoseconds(), i128::from(delay));
        }
        (EngineRetry::Exhausted { reason }, model::RetryDecision::Stop(expected)) => assert_eq!(
            activity::exhausted_reason_to_retry_state(reason),
            kernel_retry_state(expected)
        ),
        (actual, expected) => panic!("actual {actual:?}; model {expected:?}"),
    }
}

#[test]
fn model_retry_branch_table() {
    // service/history/workflow/retry.go:115-149,96-112 @ v1.31.0.
    for (attempt, maximum, non_retryable, kind, expiration) in [
        (1, 0, false, "retryable", None),
        (1, 1, false, "retryable", None),
        (1, 1, true, "retryable", None),
        (1, 1, false, "listed", None),
        (1, 0, false, "retryable", Some(999_999_999)),
        (1, 0, false, "retryable", Some(1_000_000_000)),
        (1, 0, false, "retryable", Some(1_000_000_001)),
        (0, 0, false, "retryable", None),
        (i32::MAX as u32, i32::MAX as u32, false, "retryable", None),
    ] {
        let mut p = policy();
        p.maximum_attempts = maximum;
        check_retry(&p, attempt, non_retryable, kind, expiration);
    }
    // common/backoff/retry.go:183-185; service/history/workflow/retry.go:105-107 @ v1.31.0.
    let mut p = policy();
    p.backoff_coefficient = 2.0;
    p.maximum_interval = Some(Duration::seconds(3));
    for attempt in [1, 2, 3, 64, i32::MAX as u32] {
        check_retry(&p, attempt, false, "retryable", None);
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 128, failure_persistence: None, ..ProptestConfig::default() })]
    // Feature: conformance-harness, Property 10. Each retry decision preserves reason and exact deadline.
    #[test]
    fn model_retry_generated(initial in 1i64..1000, attempt in 1u32..30, cap in prop_oneof![Just(0i64), 1i64..1000], maximum in 0u32..30, coefficient in prop_oneof![(1u32..=3).prop_map(f64::from), (100u32..=300).prop_map(|value| f64::from(value) / 100.0)], non_retryable in any::<bool>(), listed in any::<bool>(), expiration in prop::option::of(0i64..1_000_000)) {
        let mut p = policy();
        p.initial_interval = Duration::seconds(initial); p.backoff_coefficient = coefficient;
        p.maximum_interval = Some(Duration::seconds(cap)); p.maximum_attempts = maximum;
        check_retry(&p, attempt, non_retryable, if listed { "listed" } else { "retryable" }, expiration.map(|s| i128::from(s)*1_000_000_000));
    }

    // Feature: conformance-harness, Property 10. Retry intervals agree without attempt or expiration limits masking rounding.
    #[test]
    fn model_retry_interval_generated(
        initial in 1i64..=1000,
        attempt in 1u32..30,
        cap in prop_oneof![Just(0i64), 1i64..=1000],
        coefficient in prop_oneof![(1u32..=3).prop_map(f64::from), (100u32..=300).prop_map(|value| f64::from(value) / 100.0)],
    ) {
        let p = RetryPolicy {
            initial_interval: Duration::seconds(initial),
            backoff_coefficient: coefficient,
            maximum_interval: Some(Duration::seconds(cap)),
            maximum_attempts: 0,
            non_retryable_error_types: Vec::new(),
        };
        check_retry(&p, attempt, false, "retryable", None);
    }
}

async fn reschedule(
    initial: Duration,
    maximum: Option<Duration>,
    coefficient: f64,
    worker: bool,
    first: bool,
) {
    let f = Fixture::new(Some(policy()), false).await;
    if !first {
        if worker {
            f.fail(false, "retryable").await;
        } else {
            tokio::time::advance(std::time::Duration::from_nanos(1_000_000_001)).await;
            f.scan().await;
        }
    } else {
        f.fail(false, "retryable").await;
        let now = f.runtime.clock.now();
        f.runtime
            .reset_activities(
                f.token.run_key,
                ResetActivitiesRequest {
                    target: ActivityControlTarget::Id(f.token.activity_id.clone()),
                    reset_heartbeat: false,
                    keep_paused: false,
                    jitter: None,
                    restore_original_options: false,
                    request: RequestContext::unattributed(now),
                    now,
                },
            )
            .await
            .unwrap();
    }
    let before = f.state().await.activities[&f.token.activity_id].clone();
    let now = f.runtime.clock.now();
    let p = RetryPolicy {
        initial_interval: initial,
        maximum_interval: maximum,
        backoff_coefficient: coefficient,
        ..policy()
    };
    let expected = model::next_scheduled_time(
        &wire_policy(&p),
        before.attempt as i32,
        before
            .current_attempt_scheduled_at
            .unwrap()
            .unix_timestamp_nanos(),
        before
            .last_attempt_complete_time
            .map(|t| t.unix_timestamp_nanos()),
    );
    f.runtime
        .update_activity_options(
            f.token.run_key,
            UpdateActivitiesOptionsRequest {
                target: ActivityControlTarget::Id(f.token.activity_id.clone()),
                task_queue: FieldChange::Unchanged,
                schedule_to_close_timeout: FieldChange::Unchanged,
                schedule_to_start_timeout: FieldChange::Unchanged,
                start_to_close_timeout: FieldChange::Unchanged,
                heartbeat_timeout: FieldChange::Unchanged,
                retry_policy: ActivityRetryPolicyPatch {
                    initial_interval: FieldChange::Set(initial),
                    backoff_coefficient: FieldChange::Set(coefficient),
                    maximum_interval: FieldChange::Set(maximum),
                    ..Default::default()
                },
                priority: tokeira_kernel::ActivityPriorityPatch::Unchanged,
                restore_original_options: false,
                request: RequestContext::unattributed(now),
                now,
            },
        )
        .await
        .unwrap();
    let actual = f.state().await.activities[&f.token.activity_id]
        .current_attempt_scheduled_at
        .unwrap()
        .unix_timestamp_nanos();
    discovery_tests::stop(&f.runtime).await;
    assert_eq!(actual, expected);
}

#[tokio::test(start_paused = true)]
async fn model_options_reschedule_table() {
    reschedule(
        Duration::seconds(4),
        Some(Duration::seconds(5)),
        2.0,
        true,
        true,
    )
    .await;
    // service/history/workflow/activity.go:238-251 @ v1.31.0.
    for worker in [false, true] {
        for (initial, maximum, coefficient) in [
            (0, None, 2.0),
            (1, None, 1.0),
            (2, None, 2.0),
            (4, Some(5), 2.0),
        ] {
            reschedule(
                Duration::seconds(initial),
                maximum.map(Duration::seconds),
                coefficient,
                worker,
                false,
            )
            .await;
        }
    }
}

#[tokio::test(start_paused = true)]
async fn model_options_zero_interval_uses_maximum() {
    reschedule(Duration::ZERO, Some(Duration::seconds(5)), 2.0, true, false).await;
}

#[tokio::test(start_paused = true)]
async fn model_options_zero_maximum_is_uncapped() {
    reschedule(Duration::seconds(2), Some(Duration::ZERO), 2.0, true, false).await;
}

#[tokio::test(start_paused = true)]
async fn model_options_preserve_nanoseconds() {
    reschedule(Duration::nanoseconds(1), None, 2.0, true, false).await;
}

#[tokio::test(start_paused = true)]
async fn model_options_interval_fits_signed_nanoseconds() {
    reschedule(Duration::seconds(5_000_000_000), None, 2.0, true, false).await;
}

#[test]
fn model_retry_zero_interval_without_maximum() {
    let mut p = policy();
    p.initial_interval = Duration::ZERO;
    check_retry(&p, 1, false, "retryable", None);
}

#[test]
fn model_retry_submillisecond() {
    // service/history/workflow/retry.go:100-112; common/backoff/retry.go:183-185 @ v1.31.0.
    check_retry(
        &RetryPolicy {
            initial_interval: Duration::nanoseconds(1),
            maximum_interval: None,
            ..policy()
        },
        2,
        false,
        "retryable",
        None,
    );
}

#[test]
fn model_retry_zero_with_cap() {
    // service/history/workflow/retry.go:100-112; common/backoff/retry.go:183-185 @ v1.31.0.
    check_retry(
        &RetryPolicy {
            initial_interval: Duration::ZERO,
            maximum_interval: Some(Duration::seconds(5)),
            ..policy()
        },
        2,
        false,
        "retryable",
        None,
    );
}

#[test]
fn model_retry_zero_maximum() {
    // service/history/workflow/retry.go:100-112; common/backoff/retry.go:183-185 @ v1.31.0.
    check_retry(
        &RetryPolicy {
            initial_interval: Duration::seconds(2),
            maximum_interval: Some(Duration::ZERO),
            ..policy()
        },
        2,
        false,
        "retryable",
        None,
    );
}

#[test]
fn model_retry_negative_without_cap() {
    // service/history/workflow/retry.go:100-112; common/backoff/retry.go:183-185 @ v1.31.0.
    check_retry(
        &RetryPolicy {
            initial_interval: Duration::seconds(-1),
            maximum_interval: None,
            ..policy()
        },
        2,
        false,
        "retryable",
        None,
    );
}

async fn completion_case(worker_first: bool, heartbeat: bool) {
    let mut f = Fixture::new(Some(policy()), heartbeat).await;
    let mut expected = None;
    if worker_first {
        let at = f.runtime.clock.now().unix_timestamp_nanos();
        expected = model::completion_time(expected, at, model::CompletionOrigin::WorkerFailure);
        f.fail(false, "retryable").await;
        assert_eq!(
            f.state().await.activities[&f.token.activity_id]
                .last_attempt_complete_time
                .map(|t| t.unix_timestamp_nanos()),
            expected
        );
        f.next_attempt().await;
    }
    tokio::time::advance(std::time::Duration::from_nanos(1_000_000_001)).await;
    expected = model::completion_time(
        expected,
        f.runtime.clock.now().unix_timestamp_nanos(),
        model::CompletionOrigin::Timeout,
    );
    f.scan().await;
    assert_eq!(
        f.state().await.activities[&f.token.activity_id]
            .last_attempt_complete_time
            .map(|t| t.unix_timestamp_nanos()),
        expected
    );
    discovery_tests::stop(&f.runtime).await;
}

#[tokio::test(start_paused = true)]
async fn model_completion_provenance_table() {
    // respondactivitytaskfailed/api.go:97-105; timer_queue_active_task_executor.go:292-361 @ v1.31.0 (under service/history).
    for worker in [false, true] {
        for heartbeat in [false, true] {
            completion_case(worker, heartbeat).await;
        }
    }
}

async fn cancel_response_case(requested: bool) {
    let f = Fixture::new(Some(policy()), false).await;
    if requested {
        f.cancel().await;
    }
    let heartbeat = f
        .runtime
        .record_activity_heartbeat(f.token.clone(), None, None)
        .await
        .unwrap();
    assert_eq!(
        heartbeat.cancel_requested,
        model::heartbeat_cancel_requested(requested)
    );
    let result = f
        .runtime
        .cancel_activity_task(
            f.token.clone(),
            None,
            None,
            RequestContext::unattributed(f.runtime.clock.now()),
        )
        .await;
    let accepted = matches!(result, Ok(CommitResult::Applied { .. }));
    discovery_tests::stop(&f.runtime).await;
    assert_eq!(accepted, model::accept_cancellation(requested));
}

#[tokio::test(start_paused = true)]
async fn model_heartbeat_and_cancel_response() {
    // service/history/api/recordactivitytaskheartbeat/api.go:73-105; service/history/api/respondactivitytaskcanceled/api.go:81-107 @ v1.31.0.
    cancel_response_case(true).await;
}

#[tokio::test(start_paused = true)]
async fn model_cancel_response_requires_request() {
    cancel_response_case(false).await;
}

#[tokio::test(start_paused = true)]
async fn model_requested_delay_overrides_policy_cap() {
    // service/history/workflow/mutable_state_impl.go:6300-6317 @ v1.31.0.
    let f = Fixture::new(Some(policy()), false).await;
    let failure = application_failure(false, "retryable", Some(3_000_000_000));
    let at = f.runtime.clock.now();
    let expected = model::retry(
        &model::RetryInput {
            policy: Some(wire_policy(&policy())),
            attempt: 1,
            ..Default::default()
        },
        Some(&failure),
        at.unix_timestamp_nanos(),
    );
    f.runtime
        .fail_activity_task(
            f.token.clone(),
            failure_to_payload(&failure),
            crate::retry::ActivityFailure::Application {
                error_type: "retryable".into(),
                non_retryable: false,
                next_retry_delay: Some(Duration::seconds(3)),
            },
            None,
            None,
            RequestContext::unattributed(at),
        )
        .await
        .unwrap();
    let next = f.state().await.activities[&f.token.activity_id]
        .current_attempt_scheduled_at
        .unwrap();
    discovery_tests::stop(&f.runtime).await;
    assert_eq!(
        model::RetryDecision::After((next - at).whole_nanoseconds().try_into().unwrap()),
        expected
    );
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 100, failure_persistence: None, ..ProptestConfig::default() })]
    // Feature: conformance-harness, Property 10. Current-attempt rescheduling retains its completion anchor.
    #[test]
    fn model_options_generated(initial in 1i64..100, maximum in 1i64..100, coefficient in 1u32..4, worker in any::<bool>()) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            tokio::time::pause();
            reschedule(Duration::seconds(initial), Some(Duration::seconds(maximum)), f64::from(coefficient), worker, false).await;
        });
    }
    // Feature: conformance-harness, Property 10. Timeout retries preserve the latest worker completion.
    #[test]
    fn model_completion_generated(worker in any::<bool>(), heartbeat in any::<bool>()) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            tokio::time::pause(); completion_case(worker, heartbeat).await;
        });
    }
}

async fn timeout_result_case(
    has_policy: bool,
    cancel: bool,
    excluded: bool,
    exhausted: bool,
    heartbeat: bool,
    expires: bool,
) {
    // service/history/workflow/mutable_state_impl.go:6243-6319; service/history/timer_queue_active_task_executor.go:292-361 @ v1.31.0.
    let mut p = policy();
    p.maximum_attempts = if exhausted { 1 } else { 0 };
    if expires {
        p.initial_interval = Duration::seconds(30);
    }
    if excluded {
        p.non_retryable_error_types.push(
            if heartbeat {
                "TemporalTimeout:Heartbeat"
            } else {
                "TemporalTimeout:StartToClose"
            }
            .into(),
        );
    }
    let f = Fixture::new(has_policy.then_some(p.clone()), heartbeat).await;
    if cancel {
        f.cancel().await;
    }
    let before = f.state().await.activities[&f.token.activity_id].clone();
    tokio::time::advance(std::time::Duration::from_nanos(1_000_000_001)).await;
    let now = f.runtime.clock.now();
    let kind = if heartbeat {
        TimeoutType::Heartbeat
    } else {
        TimeoutType::StartToClose
    };
    let failure = Failure {
        failure_info: Some(FailureInfo::TimeoutFailureInfo(TimeoutFailureInfo {
            timeout_type: kind as i32,
            ..Default::default()
        })),
        ..Default::default()
    };
    let retry = model::retry(
        &model::RetryInput {
            policy: has_policy.then(|| wire_policy(&p)),
            attempt: 1,
            cancel_requested: cancel,
            paused: false,
            expiration: Some((before.scheduled_at + Duration::seconds(20)).unix_timestamp_nanos()),
        },
        Some(&failure),
        now.unix_timestamp_nanos(),
    );
    let expected = model::timeout_disposition(1, 1, kind, retry);
    f.scan().await;
    let events = f.terminal_events().await;
    match expected {
        model::TimeoutDecision::Retained => {
            assert!(events.is_empty());
            assert_eq!(f.state().await.activities[&f.token.activity_id].attempt, 2);
        }
        model::TimeoutDecision::Closed(kind, state) => {
            let [
                HistoryEventKind::ActivityTaskTimedOut {
                    retry_state,
                    failure: Some(failure),
                    ..
                },
            ] = events.as_slice()
            else {
                panic!("{events:?}");
            };
            assert_eq!(*retry_state, kernel_retry_state(state));
            let failure = payload_to_failure(failure);
            let Some(FailureInfo::TimeoutFailureInfo(info)) = failure.failure_info else {
                panic!("timeout failure");
            };
            assert_eq!(info.timeout_type, kind as i32);
        }
        model::TimeoutDecision::Stale => unreachable!(),
    }
    discovery_tests::stop(&f.runtime).await;
}

#[tokio::test(start_paused = true)]
async fn model_timeout_result_table() {
    for heartbeat in [false, true] {
        for (policy, cancel, excluded, exhausted, expires) in [
            (false, false, false, false, false),
            (true, true, true, true, true),
            (true, false, true, false, false),
            (true, false, false, true, false),
            (true, false, false, false, true),
            (true, false, false, false, false),
        ] {
            timeout_result_case(policy, cancel, excluded, exhausted, heartbeat, expires).await;
        }
    }
}

#[tokio::test(start_paused = true)]
async fn model_schedule_to_start_close_tie_unpolled() {
    let (repo, runtime) = timer_deadline_tests::fixture().await;
    let request = start(&runtime, false);
    let run = request.run_key;
    let queue = QueueKey {
        namespace_id: request.namespace_id,
        task_queue: request.task_queue.clone(),
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    };
    runtime.start_workflow(request).await.unwrap();
    let task = runtime
        .poll_workflow_task(
            queue,
            WorkerIdentity("worker".into()),
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap()
        .unwrap();
    let timeout = Duration::seconds(5);
    let retry_policy = policy();
    let mut command = completion_eager_tests::schedule("unpolled-tie", false);
    let WorkflowCommand::ScheduleActivity {
        request_eager_execution,
        retry_policy: command_policy,
        schedule_to_close_timeout,
        schedule_to_start_timeout,
        start_to_close_timeout,
        heartbeat_timeout,
        ..
    } = &mut command
    else {
        panic!("schedule command");
    };
    *request_eager_execution = false;
    *command_policy = Some(retry_policy.clone());
    *schedule_to_close_timeout = Some(timeout);
    *schedule_to_start_timeout = None;
    *start_to_close_timeout = None;
    *heartbeat_timeout = None;
    let scheduled = runtime.clock.now();
    let mut completion = completion_inline_tests::completion(&task, false);
    completion.now = scheduled;
    completion.request.received_at = scheduled;
    completion.commands = vec![command];
    runtime.complete_workflow_task(completion).await.unwrap();
    let LoadedRun::Existing(before) = repo.load_run(run).await.unwrap() else {
        panic!("scheduled workflow");
    };
    let activity = &before.activities["unpolled-tie"];
    assert!(activity.started_at.is_none());
    assert!(activity.started_event_id.is_none());
    // This tie is created by admission itself, not by supplying two deadlines:
    // chasm/lib/activity/validator.go:166-176 and
    // service/history/api/command_attr_validator.go:112-131 @ v1.31.0.
    assert_eq!(activity.schedule_to_close_timeout, Some(timeout));
    assert_eq!(activity.schedule_to_start_timeout, Some(timeout));
    let timing = model::ActivityTiming {
        scheduled: true,
        scheduled_time: scheduled.unix_timestamp_nanos(),
        first_scheduled_time: Some(scheduled.unix_timestamp_nanos()),
        schedule_to_close: timeout.whole_nanoseconds().try_into().unwrap(),
        schedule_to_start: timeout.whole_nanoseconds().try_into().unwrap(),
        start_to_close: timeout.whole_nanoseconds().try_into().unwrap(),
        ..Default::default()
    };
    tokio::time::advance(std::time::Duration::from_secs(6)).await;
    let now = runtime.clock.now().unix_timestamp_nanos();
    // service/history/workflow/timer_sequence.go:393-425 and
    // service/history/timer_queue_active_task_executor.go:243-267 @ v1.31.0.
    let (_, kind) = model::deadlines(&timing)
        .into_iter()
        .find(|(at, _)| model::timer_due(*at, now, 0))
        .unwrap();
    let failure = Failure {
        failure_info: Some(FailureInfo::TimeoutFailureInfo(TimeoutFailureInfo {
            timeout_type: kind as i32,
            ..Default::default()
        })),
        ..Default::default()
    };
    let retry = model::retry(
        &model::RetryInput {
            policy: Some(wire_policy(&retry_policy)),
            attempt: 1,
            expiration: Some(scheduled.unix_timestamp_nanos() + timeout.whole_nanoseconds()),
            ..Default::default()
        },
        Some(&failure),
        now,
    );
    // service/history/workflow/mutable_state_impl.go:6260-6266 and
    // service/history/timer_queue_active_task_executor.go:300-361 @ v1.31.0.
    let model::TimeoutDecision::Closed(expected_kind, expected_state) =
        model::timeout_disposition(1, 1, kind, retry)
    else {
        panic!("first timeout closes the unstarted activity");
    };
    scan_activity_timeouts_once(
        &runtime.activity_retry_deps(),
        Some(ShardId(0)),
        &runtime.lanes,
        runtime.lanes.len(),
        &ActivityTimeoutScannerConfig::default(),
    )
    .await;
    let history = repo.read_history_to_end(run, 0).await.unwrap();
    let LoadedRun::Existing(after) = repo.load_run(run).await.unwrap() else {
        panic!("open workflow after timeout");
    };
    discovery_tests::stop(&runtime).await;
    // Deletion makes the other timer inert: service/history/workflow/mutable_state_impl.go:4345-4351;
    // service/history/timer_queue_active_task_executor.go:251-255 @ v1.31.0.
    assert!(!after.activities.contains_key("unpolled-tie"));
    assert!(
        !history
            .iter()
            .any(|event| matches!(event.kind, HistoryEventKind::ActivityTaskStarted { .. }))
    );
    let resolutions: Vec<_> = history
        .iter()
        .filter_map(|event| {
            let HistoryEventKind::ActivityTaskTimedOut {
                timeout_type,
                retry_state,
                failure: Some(failure),
                ..
            } = &event.kind
            else {
                return None;
            };
            let failure = payload_to_failure(failure);
            let Some(FailureInfo::TimeoutFailureInfo(info)) = failure.failure_info else {
                panic!("timeout failure");
            };
            Some((timeout_type.clone(), info.timeout_type, retry_state.clone()))
        })
        .collect();
    let expected_name = expected_kind
        .as_str_name()
        .strip_prefix("TIMEOUT_TYPE_")
        .unwrap()
        .to_ascii_lowercase();
    assert_eq!(
        resolutions,
        [(
            expected_name,
            expected_kind as i32,
            kernel_retry_state(expected_state)
        )]
    );
}

async fn cancel_command_case(started: bool, duplicate: bool, missing: bool, buffered: bool) {
    // service/history/workflow/mutable_state_impl.go:4364-4396; service/history/api/respondworkflowtaskcompleted/workflow_task_completed_handler.go:647-667 @ v1.31.0.
    let f = Fixture::new(if buffered { None } else { Some(policy()) }, false).await;
    if !started {
        f.fail(false, "retryable").await;
    }
    if duplicate {
        f.cancel().await;
    }
    let now = f.runtime.clock.now();
    f.runtime
        .submit_for_owned_shard(
            f.token.run_key,
            Command::Signal(SignalRequest {
                signal_name: "wake".into(),
                input: Payloads::default(),
                header: None,
                links: Vec::new(),
                request: RequestContext {
                    request_id: RequestId("model-cancel-command".into()),
                    ..RequestContext::unattributed(now)
                },
                now,
            }),
        )
        .await
        .unwrap();
    let task = f
        .runtime
        .poll_workflow_task(
            f.queue.clone(),
            WorkerIdentity("worker".into()),
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap()
        .unwrap();
    if buffered {
        f.fail(false, "retryable").await;
    }
    let baseline = f
        .repo
        .read_history_to_end(f.token.run_key, 0)
        .await
        .unwrap()
        .len();
    let mut completion = completion_inline_tests::completion(&task, false);
    completion.now = now;
    completion.request.received_at = now;
    completion.commands = vec![WorkflowCommand::RequestCancelActivity {
        scheduled_event_id: if missing {
            999
        } else {
            f.token.schedule_event_id
        },
    }];
    let result = f.runtime.complete_workflow_task(completion).await;
    let history = f
        .repo
        .read_history_to_end(f.token.run_key, 0)
        .await
        .unwrap();
    let actual = &history[baseline..];
    let expected = model::request_cancel(
        !missing && !buffered,
        started,
        duplicate,
        buffered && !missing,
    );
    assert_eq!(result.is_err(), expected == model::CancelDecision::Invalid);
    match expected {
        model::CancelDecision::Invalid => assert!(
            actual
                .iter()
                .any(|event| matches!(event.kind, HistoryEventKind::WorkflowTaskFailed { .. }))
        ),
        model::CancelDecision::RecordOnly
        | model::CancelDecision::RequestWorker
        | model::CancelDecision::CancelImmediately => {
            let events: Vec<_> = actual
                .iter()
                .filter_map(|event| match event.kind {
                    HistoryEventKind::ActivityTaskCancelRequested { .. } => Some("request"),
                    HistoryEventKind::ActivityTaskCanceled { .. } => Some("canceled"),
                    _ => None,
                })
                .collect();
            assert_eq!(
                events,
                if expected == model::CancelDecision::CancelImmediately {
                    vec!["request", "canceled"]
                } else {
                    vec!["request"]
                }
            );
        }
    }
    discovery_tests::stop(&f.runtime).await;
}

#[tokio::test(start_paused = true)]
async fn model_cancel_command_table() {
    for (started, duplicate, missing, buffered) in [
        (true, false, false, false),
        (false, false, false, false),
        (true, true, false, false),
        (true, false, true, false),
        (true, false, false, true),
    ] {
        cancel_command_case(started, duplicate, missing, buffered).await;
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 100, failure_persistence: None, ..ProptestConfig::default() })]
    // Feature: conformance-harness, Property 10. Timeout retry states and final types match the same decision.
    #[test]
    fn model_timeout_result_generated(policy in any::<bool>(), cancel in any::<bool>(), excluded in any::<bool>(), exhausted in any::<bool>(), heartbeat in any::<bool>(), expires in any::<bool>()) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async { tokio::time::pause(); timeout_result_case(policy,cancel,excluded,exhausted,heartbeat,expires).await; });
    }
    // Feature: conformance-harness, Property 10. Scheduled retries cancel immediately; started attempts await the worker.
    #[test]
    fn model_cancel_command_generated(case in 0usize..5) {
        let (started,duplicate,missing,buffered) = [(true,false,false,false),(false,false,false,false),(true,true,false,false),(true,false,true,false),(true,false,false,true)][case];
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async { tokio::time::pause(); cancel_command_case(started,duplicate,missing,buffered).await; });
    }
    // Feature: conformance-harness, Property 10. Heartbeats reflect the durable request before acknowledging cancellation.
    #[test]
    fn model_heartbeat_generated(requested in any::<bool>()) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            tokio::time::pause(); let f = Fixture::new(Some(policy()), false).await;
            if requested { f.cancel().await; }
            assert_eq!(f.runtime.record_activity_heartbeat(f.token.clone(), None, None).await.unwrap().cancel_requested, model::heartbeat_cancel_requested(requested));
            discovery_tests::stop(&f.runtime).await;
        });
    }
}

#[tokio::test(start_paused = true)]
async fn model_paused_spent_budget_and_timer_suppression() {
    // service/history/workflow/mutable_state_impl.go:6277-6298; service/history/workflow/timer_sequence.go:180-184 @ v1.31.0.
    for maximum_attempts in [0, 1] {
        let p = RetryPolicy {
            maximum_attempts,
            ..policy()
        };
        let f = Fixture::new(Some(p.clone()), false).await;
        f.pause().await;
        let before = f.state().await;
        tokio::time::advance(std::time::Duration::from_secs(30)).await;
        assert!(
            model::deadlines(&model::ActivityTiming {
                scheduled: true,
                paused: true,
                start_to_close: 1,
                started_time: Some(0),
                ..Default::default()
            })
            .is_empty()
        );
        f.scan().await;
        assert_eq!(f.state().await, before);
        assert_eq!(
            model::retry(
                &model::RetryInput {
                    policy: Some(wire_policy(&p)),
                    attempt: 1,
                    paused: true,
                    expiration: Some(0),
                    ..Default::default()
                },
                None,
                1
            ),
            model::RetryDecision::Paused
        );
        f.fail(false, "retryable").await;
        f.assert_parked(
            before.activities[&f.token.activity_id].stamp,
            worker_failure(),
        )
        .await;
        discovery_tests::stop(&f.runtime).await;
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 100, failure_persistence: None, ..ProptestConfig::default() })]
    // Feature: conformance-harness, Property 10. Requested worker delay must survive the runtime failure path.
    #[test]

    fn model_requested_delay_generated(delay in 2i64..10) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            tokio::time::pause();
            let f = Fixture::new(Some(policy()), false).await;
            let at = f.runtime.clock.now();
            let failure = application_failure(false, "retryable", Some(delay * 1_000_000_000));
            let expected = model::retry(&model::RetryInput { policy: Some(wire_policy(&policy())), attempt: 1, ..Default::default() }, Some(&failure), at.unix_timestamp_nanos());
            f.runtime.fail_activity_task(f.token.clone(), failure_to_payload(&failure), crate::retry::ActivityFailure::Application { error_type: "retryable".into(), non_retryable: false, next_retry_delay: Some(Duration::seconds(delay)) }, None, None, RequestContext::unattributed(at)).await.unwrap();
            let next = f.state().await.activities[&f.token.activity_id].current_attempt_scheduled_at.unwrap();
            discovery_tests::stop(&f.runtime).await;
            assert_eq!(model::RetryDecision::After((next-at).whole_nanoseconds().try_into().unwrap()), expected);
        });
    }
}
