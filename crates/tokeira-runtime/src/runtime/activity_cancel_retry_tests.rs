//! Cancellation ends an in-flight attempt through its original failure path.

use super::{
    repair_tests::{Point, Repo},
    timer_deadline_tests::{fixture_on, start},
    *,
};
use crate::{activity_timeout::scan_activity_timeouts_once, test_clock::TestClock};
use proptest::prelude::*;
use tokeira_proto::conversions::common::payload_to_failure;
use tokeira_storage::InMemoryStore;

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
                Some(error_type.into()),
                non_retryable,
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
                None,
                false,
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

proptest! {
    #![proptest_config(ProptestConfig { cases: 128, failure_persistence: None, ..ProptestConfig::default() })]
    // Feature: runtime-activity-pump, Property 14: Cancellation stops retries.
    #[test]
    fn cancel_requested_retry_order_matches_temporal(
        has_policy in any::<bool>(), cancel in any::<bool>(), failure_kind in 0u8..3,
        attempt in 1u32..5, attempts_exhausted in any::<bool>(), expired in any::<bool>(), paused in any::<bool>(),
    ) {
        // Left out: a paused attempt that fails retryably with no attempts or
        // time left. v1.31.0 parks a paused attempt before it checks the retry
        // budget (mutable_state_impl.go:6277-6297 @ v1.31.0); the runtime
        // checks the budget first and ends the activity. That is a pause
        // difference, not a cancellation one, so this property doesn't cover it.
        prop_assume!(!(has_policy && !cancel && failure_kind == 0 && paused && (attempts_exhausted || expired)));
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            tokio::time::pause();
            let mut retry = policy();
            retry.maximum_attempts = if attempts_exhausted { attempt } else { attempt + 1 };
            let mut f = Fixture::new(has_policy.then_some(retry), false).await;
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
            if expired { tokio::time::advance(std::time::Duration::from_secs(30)).await; }
            f.fail(failure_kind == 1, if failure_kind == 2 { "listed" } else { "retryable" }).await;
            let expected = if !has_policy { Some(RetryState::RetryPolicyNotSet) }
                else if cancel { Some(RetryState::CancelRequested) }
                else if failure_kind != 0 { Some(RetryState::NonRetryableFailure) }
                else if paused { None }
                else if attempts_exhausted { Some(RetryState::MaximumAttemptsReached) }
                else if expired { Some(RetryState::Timeout) } else { None };
            let history = f.repo.read_history_to_end(f.token.run_key, 0).await.unwrap();
            let actual = history.iter().find_map(|event| match &event.kind { HistoryEventKind::ActivityTaskFailed { retry_state, .. } => Some(retry_state.clone()), _ => None });
            assert_eq!(actual, expected);
            let state = f.state().await;
            assert_eq!(state.activities.contains_key(&f.token.activity_id), expected.is_none());
            if expected.is_none() { assert_eq!(state.activities[&f.token.activity_id].attempt, f.token.attempt + 1); }
            discovery_tests::stop(&f.runtime).await;
        });
    }
}
