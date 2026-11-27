//! Activity failure ordering preserves cancellation, pause, and retry budgets.

use super::{
    repair_tests::{Point, Repo},
    timer_deadline_tests::{fixture_on, start},
    *,
};
use crate::{activity_timeout::scan_activity_timeouts_once, test_clock::TestClock};
use proptest::prelude::*;
use tokeira_behaviour_model::activity as model;

#[path = "activity_model_tests.rs"]
mod model_tests;
use tokeira_proto::{
    conversions::common::{failure_to_payload, payload_to_failure},
    enums::TimeoutType,
    failure::{Failure, TimeoutFailureInfo, failure::FailureInfo},
};
use tokeira_storage::InMemoryStore;
use tokeira_types::{WorkflowRuleAction, WorkflowRuleRecord, WorkflowRuleTrigger};

struct Fixture {
    repo: Arc<Repo>,
    runtime: TokeiraRuntime<Repo>,
    queue: QueueKey,
    token: ActivityTaskToken,
}

impl Fixture {
    async fn new(policy: Option<RetryPolicy>, heartbeat: bool) -> Self {
        let clock: Arc<dyn Clock> = Arc::new(TestClock::new());
        let repo = Arc::new(Repo::with_inner(
            InMemoryStore::default().with_clock(clock.clone()),
        ));
        let runtime = fixture_on(repo.clone(), clock).await;
        let request = start(&runtime, false);
        let queue = QueueKey {
            namespace_id: request.namespace_id,
            task_queue: request.task_queue.clone(),
            task_kind: TaskKind::Workflow,
            deployment: None,
            build_id: None,
        };
        runtime.start_workflow(request).await.unwrap();
        let first = runtime
            .poll_workflow_task(
                queue.clone(),
                WorkerIdentity("worker".into()),
                std::time::Duration::from_secs(1),
            )
            .await
            .unwrap()
            .unwrap();
        let mut command = completion_eager_tests::schedule("cancel-retry", false);
        if let WorkflowCommand::ScheduleActivity {
            retry_policy,
            schedule_to_close_timeout,
            start_to_close_timeout,
            heartbeat_timeout,
            ..
        } = &mut command
        {
            *retry_policy = policy;
            *schedule_to_close_timeout = Some(Duration::seconds(20));
            *start_to_close_timeout = Some(Duration::seconds(if heartbeat { 10 } else { 1 }));
            *heartbeat_timeout = heartbeat.then_some(Duration::seconds(1));
        }
        let mut completion = completion_inline_tests::completion(&first, false);
        completion.now = runtime.clock.now();
        completion.request.received_at = completion.now;
        completion.commands = vec![command];
        runtime.complete_workflow_task(completion).await.unwrap();
        let activity_queue = QueueKey {
            task_kind: TaskKind::Activity,
            task_queue: TaskQueueName("activities".into()),
            ..queue.clone()
        };
        let offer = runtime
            .activity_broker
            .try_claim_activity_offer(&activity_queue, first.run_key, "cancel-retry")
            .await
            .unwrap();
        let token = runtime
            .start_activity_task_offer(offer, WorkerIdentity("worker".into()))
            .await
            .unwrap()
            .unwrap()
            .token;
        Self {
            repo,
            runtime,
            queue,
            token,
        }
    }

    async fn pause(&self) {
        let now = self.runtime.clock.now();
        self.runtime
            .pause_activities(
                self.token.run_key,
                PauseActivityRequest {
                    target: ActivityControlTarget::Id(self.token.activity_id.clone()),
                    identity: "worker".into(),
                    reason: "pause".into(),
                    rule_id: None,
                    request: RequestContext {
                        request_id: RequestId("pause".into()),
                        ..RequestContext::unattributed(now)
                    },
                    now,
                },
            )
            .await
            .unwrap();
    }

    async fn unpause(&self) {
        let now = self.runtime.clock.now();
        self.runtime
            .unpause_activities(
                self.token.run_key,
                UnpauseActivitiesRequest {
                    target: ActivityControlTarget::Id(self.token.activity_id.clone()),
                    reset_attempts: false,
                    reset_heartbeat: false,
                    jitter: None,
                    request: RequestContext {
                        request_id: RequestId("unpause".into()),
                        ..RequestContext::unattributed(now)
                    },
                    now,
                },
            )
            .await
            .unwrap();
    }

    async fn rule(&self, predicate: &str) {
        self.repo
            .create_workflow_rule(
                self.queue.namespace_id,
                WorkflowRuleRecord {
                    id: "failure-pause".into(),
                    create_time: self.runtime.clock.now(),
                    created_by_identity: "worker".into(),
                    description: "pause on failure".into(),
                    trigger: WorkflowRuleTrigger::ActivityStart {
                        predicate: predicate.into(),
                    },
                    visibility_query: String::new(),
                    actions: vec![WorkflowRuleAction::ActivityPause],
                    expiration_time: None,
                },
                10,
            )
            .await
            .unwrap();
    }

    async fn terminal_events(&self) -> Vec<HistoryEventKind> {
        self.repo
            .read_history_to_end(self.token.run_key, 0)
            .await
            .unwrap()
            .into_iter()
            .filter_map(|event| {
                matches!(
                    event.kind,
                    HistoryEventKind::ActivityTaskFailed { .. }
                        | HistoryEventKind::ActivityTaskTimedOut { .. }
                        | HistoryEventKind::ActivityTaskCanceled { .. }
                        | HistoryEventKind::ActivityTaskCompleted { .. }
                )
                .then_some(event.kind)
            })
            .collect()
    }

    async fn assert_parked(&self, stamp: u64, failure: Payload) {
        let state = self.state().await;
        let activity = state
            .activities
            .get(&self.token.activity_id)
            .expect("failed attempt must park");
        assert_eq!(activity.attempt, self.token.attempt + 1);
        assert_eq!(activity.stamp, stamp + 1);
        assert!(activity.started_at.is_none());
        assert!(activity.started_event_id.is_none());
        assert!(activity.current_attempt_scheduled_at.is_none());
        assert!(activity.pause_info.is_some());
        assert_eq!(activity.last_failure, Some(failure));
        assert!(self.terminal_events().await.is_empty());
        assert!(
            self.repo
                .list_all_dispatchable_activity_tasks(
                    &QueueKey {
                        task_kind: TaskKind::Activity,
                        task_queue: TaskQueueName("activities".into()),
                        ..self.queue.clone()
                    },
                    100
                )
                .await
                .unwrap()
                .is_empty()
        );
    }

    async fn cancel(&self) {
        let now = self.runtime.clock.now();
        self.runtime
            .submit_for_owned_shard(
                self.token.run_key,
                Command::Signal(SignalRequest {
                    signal_name: "cancel".into(),
                    input: Payloads::default(),
                    header: None,
                    links: Vec::new(),
                    request: RequestContext::unattributed(now),
                    now,
                }),
            )
            .await
            .unwrap();
        let task = self
            .runtime
            .poll_workflow_task(
                self.queue.clone(),
                WorkerIdentity("worker".into()),
                std::time::Duration::from_secs(1),
            )
            .await
            .unwrap()
            .unwrap();
        let mut completion = completion_inline_tests::completion(&task, false);
        completion.now = now;
        completion.request.received_at = now;
        completion.commands = vec![WorkflowCommand::RequestCancelActivity {
            scheduled_event_id: self.token.schedule_event_id,
        }];
        self.runtime
            .complete_workflow_task(completion)
            .await
            .unwrap();
        assert!(self.state().await.activities[&self.token.activity_id].cancel_requested);
    }

    async fn state(&self) -> WorkflowState {
        let LoadedRun::Existing(state) = self.repo.load_run(self.token.run_key).await.unwrap()
        else {
            panic!("run")
        };
        state
    }

    async fn fail(&self, non_retryable: bool, error_type: &str) {
        self.runtime
            .fail_activity_task(
                self.token.clone(),
                worker_failure(),
                crate::retry::ActivityFailure::Application {
                    error_type: error_type.into(),
                    non_retryable,
                    next_retry_delay: None,
                },
                None,
                None,
                RequestContext::unattributed(self.runtime.clock.now()),
            )
            .await
            .unwrap();
    }

    async fn scan(&self) {
        scan_activity_timeouts_once(
            &self.runtime.activity_retry_deps(),
            Some(ShardId(0)),
            &self.runtime.lanes,
            self.runtime.lanes.len(),
            &ActivityTimeoutScannerConfig::default(),
        )
        .await;
    }

    async fn next_attempt(&mut self) {
        let scheduled = self.state().await.activities[&self.token.activity_id]
            .current_attempt_scheduled_at
            .unwrap();
        tokio::time::advance(
            std::time::Duration::try_from(scheduled - self.runtime.clock.now()).unwrap(),
        )
        .await;
        let queue = QueueKey {
            task_kind: TaskKind::Activity,
            task_queue: TaskQueueName("activities".into()),
            ..self.queue.clone()
        };
        crate::activity_eligibility::scan_activity_eligibility_once(
            &self.runtime.activity_retry_deps(),
            ShardId(0),
            self.runtime.clock.now(),
        )
        .await;
        let offer = self
            .runtime
            .activity_broker
            .try_claim_activity_offer(&queue, self.token.run_key, &self.token.activity_id)
            .await
            .unwrap();
        self.token = self
            .runtime
            .start_activity_task_offer(offer, WorkerIdentity("worker".into()))
            .await
            .unwrap()
            .unwrap()
            .token;
    }

    async fn assert_terminal(&self, timeout: Option<bool>) {
        let state = self.state().await;
        assert!(
            !state.activities.contains_key(&self.token.activity_id),
            "cancel-requested attempt must end"
        );
        assert!(state.pending_workflow_task.is_some());
        let history = self
            .repo
            .read_history_to_end(self.token.run_key, 0)
            .await
            .unwrap();
        let terminal: Vec<_> = history
            .iter()
            .filter(|event| {
                matches!(
                    event.kind,
                    HistoryEventKind::ActivityTaskFailed { .. }
                        | HistoryEventKind::ActivityTaskTimedOut { .. }
                        | HistoryEventKind::ActivityTaskCanceled { .. }
                        | HistoryEventKind::ActivityTaskCompleted { .. }
                )
            })
            .collect();
        assert_eq!(terminal.len(), 1);
        match (&terminal[0].kind, timeout) {
            (
                HistoryEventKind::ActivityTaskFailed {
                    retry_state,
                    failure,
                    ..
                },
                None,
            ) => {
                assert_eq!(*retry_state, RetryState::CancelRequested);
                assert_eq!(*failure, worker_failure());
            }
            (
                HistoryEventKind::ActivityTaskTimedOut {
                    retry_state,
                    timeout_type,
                    failure,
                    ..
                },
                Some(heartbeat),
            ) => {
                assert_eq!(*retry_state, RetryState::CancelRequested);
                assert_eq!(
                    timeout_type,
                    if heartbeat {
                        "heartbeat"
                    } else {
                        "start_to_close"
                    }
                );
                let failure = payload_to_failure(failure.as_ref().unwrap());
                let Some(tokeira_proto::failure::failure::FailureInfo::TimeoutFailureInfo(info)) =
                    failure.failure_info
                else {
                    panic!("timeout failure")
                };
                assert_eq!(
                    info.timeout_type,
                    if heartbeat {
                        tokeira_proto::enums::TimeoutType::Heartbeat
                    } else {
                        tokeira_proto::enums::TimeoutType::StartToClose
                    } as i32
                );
            }
            other => panic!("unexpected resolution: {other:?}"),
        }
        assert!(
            history
                .iter()
                .any(|event| event.event_id > terminal[0].event_id
                    && matches!(event.kind, HistoryEventKind::WorkflowTaskScheduled { .. }))
        );
        assert!(
            self.repo
                .list_all_dispatchable_activity_tasks(
                    &QueueKey {
                        task_kind: TaskKind::Activity,
                        task_queue: TaskQueueName("activities".into()),
                        ..self.queue.clone()
                    },
                    100
                )
                .await
                .unwrap()
                .is_empty()
        );
    }
}

fn policy() -> RetryPolicy {
    RetryPolicy {
        initial_interval: Duration::seconds(1),
        backoff_coefficient: 1.0,
        maximum_interval: None,
        maximum_attempts: 0,
        non_retryable_error_types: vec!["listed".into()],
    }
}

fn worker_failure() -> Payload {
    Payload::new(b"retryable worker failure".to_vec())
}

#[tokio::test(start_paused = true)]
async fn cancel_requested_worker_failure_ends_unlimited_retry_and_workflow_completes() {
    let f = Fixture::new(Some(policy()), false).await;
    f.cancel().await;
    f.fail(false, "retryable").await;
    f.assert_terminal(None).await;
    assert!(
        f.runtime
            .fail_activity_task(
                f.token.clone(),
                worker_failure(),
                crate::retry::ActivityFailure::Other,
                None,
                None,
                RequestContext::unattributed(f.runtime.clock.now())
            )
            .await
            .is_err()
    );
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
    let mut completion = completion_inline_tests::completion(&task, false);
    completion.now = f.runtime.clock.now();
    completion.request.received_at = completion.now;
    completion.commands = vec![WorkflowCommand::CompleteWorkflow {
        result: Payloads::default(),
    }];
    f.runtime.complete_workflow_task(completion).await.unwrap();
    assert_eq!(f.state().await.status, ExecutionStatus::Completed);
    discovery_tests::stop(&f.runtime).await;
}

#[tokio::test(start_paused = true)]
async fn cancel_requested_later_attempt_ends_on_next_failure() {
    let mut f = Fixture::new(Some(policy()), false).await;
    f.fail(false, "retryable").await;
    f.next_attempt().await;
    assert_eq!(f.token.attempt, 2);
    f.cancel().await;
    f.fail(false, "retryable").await;
    f.assert_terminal(None).await;
    discovery_tests::stop(&f.runtime).await;
}

async fn timeout(heartbeat: bool, stale_tracking: bool) {
    let f = Fixture::new(Some(policy()), heartbeat).await;
    f.cancel().await;
    if stale_tracking {
        let mut entry = f
            .runtime
            .activity_tracking
            .snapshot()
            .into_iter()
            .find(|entry| entry.run_key == f.token.run_key)
            .unwrap();
        entry.cancel_requested = false;
        f.runtime.activity_tracking.insert(entry);
    }
    tokio::time::advance(std::time::Duration::from_nanos(1_000_000_001)).await;
    f.scan().await;
    f.assert_terminal(Some(heartbeat)).await;
    discovery_tests::stop(&f.runtime).await;
}

#[tokio::test(start_paused = true)]
async fn cancel_requested_start_to_close_keeps_timeout_type() {
    timeout(false, false).await;
}

#[tokio::test(start_paused = true)]
async fn cancel_requested_heartbeat_keeps_timeout_type() {
    timeout(true, false).await;
}

#[tokio::test(start_paused = true)]
async fn cancel_requested_start_to_close_ignores_stale_tracking_flag() {
    timeout(false, true).await;
}

#[tokio::test(start_paused = true)]
async fn cancel_requested_heartbeat_ignores_stale_tracking_flag() {
    timeout(true, true).await;
}

async fn retry_race(timeout: Option<bool>) {
    let f = Fixture::new(Some(policy()), timeout == Some(true)).await;
    if timeout.is_some() {
        tokio::time::advance(std::time::Duration::from_nanos(1_000_000_001)).await;
    }
    let (entered, resume) = f.repo.pause(Point::Commit);
    let retry = async {
        if timeout.is_some() {
            f.scan().await;
        } else {
            f.fail(false, "retryable").await;
        }
    };
    let cancel = async {
        entered.await.unwrap();
        f.cancel().await;
        resume.send(()).unwrap();
    };
    tokio::join!(retry, cancel);
    f.assert_terminal(timeout).await;
    assert!(
        f.repo
            .commit_conflicts
            .load(std::sync::atomic::Ordering::SeqCst)
            > 0
    );
    f.scan().await;
    f.assert_terminal(timeout).await;
    discovery_tests::stop(&f.runtime).await;
}

#[tokio::test(start_paused = true)]
async fn cancel_requested_during_worker_retry_commit_ends_once() {
    retry_race(None).await;
}

#[tokio::test(start_paused = true)]
async fn cancel_requested_during_start_to_close_retry_commit_ends_once() {
    retry_race(Some(false)).await;
}

#[tokio::test(start_paused = true)]
async fn cancel_requested_during_heartbeat_retry_commit_ends_once() {
    retry_race(Some(true)).await;
}

#[tokio::test(start_paused = true)]
async fn paused_spent_attempt_parks_then_unpause_delivers_final_attempt() {
    let mut retry = policy();
    retry.maximum_attempts = 1;
    let mut f = Fixture::new(Some(retry), false).await;
    f.pause().await;
    let stamp = f.state().await.activities[&f.token.activity_id].stamp;
    f.fail(false, "retryable").await;
    f.assert_parked(stamp, worker_failure()).await;
    f.unpause().await;
    f.next_attempt().await;
    assert_eq!(f.token.attempt, 2);
    f.fail(false, "retryable").await;
    assert!(matches!(
        f.terminal_events().await.as_slice(),
        [HistoryEventKind::ActivityTaskFailed {
            retry_state: RetryState::MaximumAttemptsReached,
            ..
        }]
    ));
    assert!(
        !f.state()
            .await
            .activities
            .contains_key(&f.token.activity_id)
    );
    discovery_tests::stop(&f.runtime).await;
}

#[tokio::test(start_paused = true)]
async fn paused_expired_worker_failure_parks_then_unpause_times_out() {
    let f = Fixture::new(Some(policy()), false).await;
    f.pause().await;
    let stamp = f.state().await.activities[&f.token.activity_id].stamp;
    tokio::time::advance(std::time::Duration::from_secs(30)).await;
    f.fail(false, "retryable").await;
    f.assert_parked(stamp, worker_failure()).await;
    f.unpause().await;
    f.scan().await;
    assert!(matches!(f.terminal_events().await.as_slice(),
        [HistoryEventKind::ActivityTaskTimedOut { retry_state: RetryState::Timeout, timeout_type, .. }]
        if timeout_type == "schedule_to_close"));
    assert!(
        !f.state()
            .await
            .activities
            .contains_key(&f.token.activity_id)
    );
    discovery_tests::stop(&f.runtime).await;
}

#[tokio::test(start_paused = true)]
async fn failure_rule_parks_spent_attempt() {
    let mut retry = policy();
    retry.maximum_attempts = 1;
    let f = Fixture::new(Some(retry), false).await;
    f.rule("Attempts = 1").await;
    let stamp = f.state().await.activities[&f.token.activity_id].stamp;
    f.fail(false, "retryable").await;
    f.assert_parked(stamp, worker_failure()).await;
    assert_eq!(
        f.state().await.activities[&f.token.activity_id]
            .pause_info
            .as_ref()
            .unwrap()
            .rule_id
            .as_deref(),
        Some("failure-pause")
    );
    discovery_tests::stop(&f.runtime).await;
}

fn timeout_failure(heartbeat: bool) -> Payload {
    let timeout_type = if heartbeat {
        TimeoutType::Heartbeat
    } else {
        TimeoutType::StartToClose
    };
    failure_to_payload(&Failure {
        message: format!(
            "activity {} timeout",
            if heartbeat {
                "Heartbeat"
            } else {
                "StartToClose"
            }
        ),
        failure_info: Some(FailureInfo::TimeoutFailureInfo(TimeoutFailureInfo {
            timeout_type: timeout_type as i32,
            ..Default::default()
        })),
        ..Default::default()
    })
}

async fn timeout_rule_parks(heartbeat: bool, attempts_spent: bool) {
    let mut retry = policy();
    retry.maximum_attempts = if attempts_spent { 1 } else { 0 };
    if !attempts_spent {
        retry.initial_interval = Duration::seconds(30);
    }
    let f = Fixture::new(Some(retry), heartbeat).await;
    f.rule("Attempts = 1").await;
    let stamp = f.state().await.activities[&f.token.activity_id].stamp;
    tokio::time::advance(std::time::Duration::from_nanos(1_000_000_001)).await;
    f.scan().await;
    f.assert_parked(stamp, timeout_failure(heartbeat)).await;
    f.scan().await;
    assert!(f.terminal_events().await.is_empty());
    discovery_tests::stop(&f.runtime).await;
}

#[tokio::test(start_paused = true)]
async fn start_to_close_rule_parks_spent_attempt() {
    timeout_rule_parks(false, true).await;
}
#[tokio::test(start_paused = true)]
async fn heartbeat_rule_parks_spent_attempt() {
    timeout_rule_parks(true, true).await;
}
#[tokio::test(start_paused = true)]
async fn start_to_close_rule_parks_spent_time_budget() {
    timeout_rule_parks(false, false).await;
}
#[tokio::test(start_paused = true)]
async fn heartbeat_rule_parks_spent_time_budget() {
    timeout_rule_parks(true, false).await;
}

async fn failure_rule_attempt(predicate: &str, matches: bool) {
    let f = Fixture::new(Some(policy()), false).await;
    f.rule(predicate).await;
    f.fail(false, "retryable").await;
    let state = f.state().await;
    let activity = &state.activities[&f.token.activity_id];
    assert_eq!(activity.attempt, 2);
    assert_eq!(activity.pause_info.is_some(), matches);
    assert_eq!(activity.current_attempt_scheduled_at.is_none(), matches);
    assert!(f.terminal_events().await.is_empty());
    discovery_tests::stop(&f.runtime).await;
}

#[tokio::test(start_paused = true)]
async fn failure_rule_matches_failed_attempt() {
    failure_rule_attempt("Attempts = 1", true).await;
}
#[tokio::test(start_paused = true)]
async fn failure_rule_does_not_match_next_attempt() {
    failure_rule_attempt("Attempts = 2", false).await;
}

async fn failure_rule_fields(timeout: bool) {
    let mut f = Fixture::new(Some(policy()), false).await;
    f.fail(false, "retryable").await;
    f.next_attempt().await;
    f.rule(if timeout {
        "Attempts = 2 AND BackoffInterval = 1 AND LastFailure contains 'retryable worker failure' AND Status = 'Started'"
    } else {
        "Attempts = 2 AND BackoffInterval = -1 AND LastFailure contains 'retryable worker failure' AND Status = 'Started'"
    }).await;
    let stamp = f.state().await.activities[&f.token.activity_id].stamp;
    tokio::time::advance(std::time::Duration::from_nanos(1_000_000_001)).await;
    let failure = if timeout {
        f.scan().await;
        timeout_failure(false)
    } else {
        let failure = Payload::new(b"current failure".to_vec());
        f.runtime
            .fail_activity_task(
                f.token.clone(),
                failure.clone(),
                crate::retry::ActivityFailure::Application {
                    error_type: "retryable".into(),
                    non_retryable: false,
                    next_retry_delay: None,
                },
                None,
                None,
                RequestContext::unattributed(f.runtime.clock.now()),
            )
            .await
            .unwrap();
        failure
    };
    f.assert_parked(stamp, failure).await;
    discovery_tests::stop(&f.runtime).await;
}

#[tokio::test(start_paused = true)]
async fn worker_failure_rule_sees_failed_attempt_fields() {
    failure_rule_fields(false).await;
}
#[tokio::test(start_paused = true)]
async fn timeout_rule_sees_failed_attempt_fields() {
    failure_rule_fields(true).await;
}

#[tokio::test(start_paused = true)]
async fn paused_activity_timeouts_fire_nothing() {
    let f = Fixture::new(Some(policy()), true).await;
    f.pause().await;
    let before = f.state().await;
    tokio::time::advance(std::time::Duration::from_secs(30)).await;
    f.scan().await;
    assert_eq!(f.state().await, before);
    assert!(f.terminal_events().await.is_empty());
    discovery_tests::stop(&f.runtime).await;
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 128, failure_persistence: None, ..ProptestConfig::default() })]
    // Feature: runtime-activity-pump, Property 14: Cancellation stops retries.
    #[test]
    fn cancel_requested_retry_order_matches_temporal(
        has_policy in any::<bool>(), cancel in any::<bool>(), failure_kind in 0u8..3,
        attempt in 1u32..5, attempts_exhausted in any::<bool>(), expired in any::<bool>(), paused in any::<bool>(), rule in 0u8..3,
    ) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            tokio::time::pause();
            let mut retry = policy();
            retry.maximum_attempts = if attempts_exhausted { attempt } else { attempt + 1 };
            let mut f = Fixture::new(has_policy.then_some(retry.clone()), false).await;
            if has_policy {
                for _ in 1..attempt { f.fail(false, "retryable").await; f.next_attempt().await; }
            }
            if cancel { f.cancel().await; }
            if paused {
                let now = f.runtime.clock.now();
                f.runtime.pause_activities(f.token.run_key, PauseActivityRequest {
                    target: ActivityControlTarget::Id(f.token.activity_id.clone()), identity: "worker".into(), reason: "pause".into(), rule_id: None, request: RequestContext::unattributed(now), now,
                }).await.unwrap();
            }
            if rule != 0 { f.rule(if rule == 1 { "Attempts >= 1" } else { "Attempts > 100" }).await; }
            if expired { tokio::time::advance(std::time::Duration::from_secs(30)).await; }
            let before = f.state().await.activities[&f.token.activity_id].clone();
            let now = f.runtime.clock.now().unix_timestamp_nanos();
            f.fail(failure_kind == 1, if failure_kind == 2 { "listed" } else { "retryable" }).await;
            let failure = model_tests::application_failure(failure_kind == 1, if failure_kind == 2 { "listed" } else { "retryable" }, None);
            let expected = model::retry(&model::RetryInput {
                policy: has_policy.then(|| model_tests::wire_policy(&retry)),
                attempt: before.attempt as i32,
                cancel_requested: before.cancel_requested,
                paused: before.pause_info.is_some() || rule == 1,
                expiration: before.schedule_to_close_timeout.map(|timeout| (before.scheduled_at + timeout).unix_timestamp_nanos()),
            }, Some(&failure), now);
            let expected = match expected {
                model::RetryDecision::Stop(state) => Some(model_tests::kernel_retry_state(state)),
                model::RetryDecision::Paused | model::RetryDecision::After(_) => None,
            };
            let history = f.repo.read_history_to_end(f.token.run_key, 0).await.unwrap();
            let actual = history.iter().find_map(|event| match &event.kind { HistoryEventKind::ActivityTaskFailed { retry_state, .. } => Some(retry_state.clone()), _ => None });
            assert_eq!(actual, expected);
            let state = f.state().await;
            assert_eq!(state.activities.contains_key(&f.token.activity_id), expected.is_none());
            if expected.is_none() {
                let activity = &state.activities[&f.token.activity_id];
                assert_eq!(activity.attempt, f.token.attempt + 1);
                assert_eq!(activity.pause_info.is_some(), paused || rule == 1);
                assert_eq!(activity.current_attempt_scheduled_at.is_none(), paused || rule == 1);
            }
            discovery_tests::stop(&f.runtime).await;
        });
    }
}

async fn retry_after_timeout(
    worker_failed_first: bool,
    heartbeat: bool,
) -> (Fixture, Option<OffsetDateTime>) {
    let mut f = Fixture::new(Some(policy()), heartbeat).await;
    let completed_at = if worker_failed_first {
        let at = f.runtime.clock.now();
        f.fail(false, "retryable").await;
        f.next_attempt().await;
        Some(at)
    } else {
        None
    };
    tokio::time::advance(std::time::Duration::from_nanos(1_000_000_001)).await;
    f.scan().await;
    (f, completed_at)
}

async fn timeout_preserves_completion(worker_failed_first: bool, heartbeat: bool) {
    // Only worker failure records completion; timeout retry preserves it
    // (service/history/api/respondactivitytaskfailed/api.go:104,
    // service/history/workflow/mutable_state_impl.go:6336-6341,
    // service/history/timer_queue_active_task_executor.go:302 @ v1.31.0).
    let (mut f, completed_at) = retry_after_timeout(worker_failed_first, heartbeat).await;
    let expected_attempt = if worker_failed_first { 3 } else { 2 };
    let activity = &f.state().await.activities[&f.token.activity_id];
    assert_eq!(activity.attempt, expected_attempt);
    assert_eq!(
        activity
            .last_attempt_complete_time
            .map(|at| at.unix_timestamp_nanos()),
        model::completion_time(
            completed_at.map(|at| at.unix_timestamp_nanos()),
            f.runtime.clock.now().unix_timestamp_nanos(),
            model::CompletionOrigin::Timeout
        )
    );
    f.next_attempt().await;
    assert_eq!(f.token.attempt, expected_attempt);
    let state = f.state().await;
    let activity = &state.activities[&f.token.activity_id];
    assert!(activity.started_at.is_some());
    assert_eq!(
        activity
            .last_attempt_complete_time
            .map(|at| at.unix_timestamp_nanos()),
        model::completion_time(
            completed_at.map(|at| at.unix_timestamp_nanos()),
            f.runtime.clock.now().unix_timestamp_nanos(),
            model::CompletionOrigin::Timeout
        )
    );
    discovery_tests::stop(&f.runtime).await;
}

#[tokio::test(start_paused = true)]
async fn first_start_to_close_timeout_preserves_absent_completion_and_starts_retry() {
    timeout_preserves_completion(false, false).await;
}

#[tokio::test(start_paused = true)]
async fn first_heartbeat_timeout_preserves_absent_completion_and_starts_retry() {
    timeout_preserves_completion(false, true).await;
}

#[tokio::test(start_paused = true)]
async fn start_to_close_timeout_preserves_worker_failure_completion_and_starts_retry() {
    timeout_preserves_completion(true, false).await;
}

#[tokio::test(start_paused = true)]
async fn heartbeat_timeout_preserves_worker_failure_completion_and_starts_retry() {
    timeout_preserves_completion(true, true).await;
}

async fn timeout_backoff_rule(worker_failed_first: bool, at_start: bool) {
    // BackoffInterval is ScheduledTime minus the last worker failure, or the
    // Unix epoch when absent, truncated to seconds
    // (service/history/workflow/matcher/activity_evaluator.go:245-253 @ v1.31.0).
    let (f, completed_at) = retry_after_timeout(worker_failed_first, false).await;
    let activity = &f.state().await.activities[&f.token.activity_id];
    let scheduled = activity.current_attempt_scheduled_at.unwrap();
    let interval = (scheduled - completed_at.unwrap_or(OffsetDateTime::UNIX_EPOCH)).whole_seconds();
    let predicate = format!(
        "Attempts = {} AND BackoffInterval = {interval}",
        activity.attempt
    );
    tokio::time::advance(std::time::Duration::try_from(scheduled - f.runtime.clock.now()).unwrap())
        .await;
    if !at_start {
        f.rule(&predicate).await;
    }
    crate::activity_eligibility::scan_activity_eligibility_once(
        &f.runtime.activity_retry_deps(),
        ShardId(0),
        f.runtime.clock.now(),
    )
    .await;
    if at_start {
        f.rule(&predicate).await;
        let queue = QueueKey {
            task_kind: TaskKind::Activity,
            task_queue: TaskQueueName("activities".into()),
            ..f.queue.clone()
        };
        let offer = f
            .runtime
            .activity_broker
            .try_claim_activity_offer(&queue, f.token.run_key, &f.token.activity_id)
            .await
            .expect("retry must be published before the rule is installed");
        assert!(
            f.runtime
                .start_activity_task_offer(offer, WorkerIdentity("worker".into()))
                .await
                .unwrap()
                .is_none()
        );
    }
    assert!(
        f.state().await.activities[&f.token.activity_id]
            .pause_info
            .is_some(),
        "{predicate}"
    );
    discovery_tests::stop(&f.runtime).await;
}

#[tokio::test(start_paused = true)]
async fn timeout_backoff_rule_uses_epoch_at_eligibility() {
    timeout_backoff_rule(false, false).await;
}

#[tokio::test(start_paused = true)]
async fn timeout_backoff_rule_uses_worker_failure_at_eligibility() {
    timeout_backoff_rule(true, false).await;
}

#[tokio::test(start_paused = true)]
async fn timeout_backoff_rule_uses_epoch_at_start() {
    timeout_backoff_rule(false, true).await;
}

#[tokio::test(start_paused = true)]
async fn timeout_backoff_rule_uses_worker_failure_at_start() {
    timeout_backoff_rule(true, true).await;
}

async fn update_retry_options(f: &Fixture, coefficient: f64) {
    let now = f.runtime.clock.now();
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
                    initial_interval: FieldChange::Set(Duration::seconds(4)),
                    backoff_coefficient: FieldChange::Set(coefficient),
                    maximum_interval: FieldChange::Set(Some(Duration::seconds(5))),
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
}

async fn timeout_options_anchor(worker_failed_first: bool) {
    // GetNextScheduledTime anchors retries on LastAttemptCompleteTime.AsTime(),
    // including the Unix epoch when nil. Coefficient 1 isolates that anchor
    // (service/history/workflow/activity.go:238-248,
    // service/history/api/updateactivityoptions/api.go:401 @ v1.31.0).
    let (f, completed_at) = retry_after_timeout(worker_failed_first, false).await;
    update_retry_options(&f, 1.0).await;
    assert_eq!(
        f.state().await.activities[&f.token.activity_id].current_attempt_scheduled_at,
        Some(completed_at.unwrap_or(OffsetDateTime::UNIX_EPOCH) + Duration::seconds(4))
    );
    discovery_tests::stop(&f.runtime).await;
}

#[tokio::test(start_paused = true)]
async fn timeout_options_reschedule_from_epoch_without_worker_failure() {
    timeout_options_anchor(false).await;
}

#[tokio::test(start_paused = true)]
async fn timeout_options_reschedule_from_previous_worker_failure() {
    timeout_options_anchor(true).await;
}

#[tokio::test(start_paused = true)]
async fn options_reschedule_backoff_uses_current_attempt_exponent() {
    // An options update at attempt 2 gives 4 * 2^(2 - 1), capped to 5 seconds,
    // even though that attempt's original retry wait used the preceding exponent
    // (service/history/workflow/activity.go:241-248,
    // common/backoff/retry.go:183-185 @ v1.31.0).
    let f = Fixture::new(Some(policy()), false).await;
    let completed_at = f.runtime.clock.now();
    f.fail(false, "retryable").await;
    update_retry_options(&f, 2.0).await;
    assert_eq!(
        f.state().await.activities[&f.token.activity_id].current_attempt_scheduled_at,
        Some(completed_at + Duration::seconds(5))
    );
    discovery_tests::stop(&f.runtime).await;
}
