//! Durable timer deadlines through real lane commits and storage listings.

use std::{future::Future, task::Poll};

use super::*;
use crate::test_clock::TestClock;
use tokeira_storage::InMemoryStore;

pub(super) async fn fixture() -> (Arc<InMemoryStore>, TokeiraRuntime<InMemoryStore>) {
    let clock: Arc<dyn Clock> = Arc::new(TestClock::new());
    let store = Arc::new(InMemoryStore::default().with_clock(clock.clone()));
    let runtime = fixture_on(store.clone(), clock).await;
    (store, runtime)
}

pub(super) async fn fixture_on<R: RunRepository + 'static>(
    store: Arc<R>,
    clock: Arc<dyn Clock>,
) -> TokeiraRuntime<R> {
    let mut runtime = TokeiraRuntime::new_with_nexus_and_shards_and_endpoint(
        store,
        1,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        ActivityTimeoutScannerConfig::default(),
        NexusTimeoutScannerConfig::default(),
        NexusEndpointRegistry::default(),
        Arc::new(NoopNexusHttpClient),
        NexusCompletionDeps::default(),
        1,
        "owner".into(),
        "node".into(),
        true,
        None,
        clock,
    );
    activity_deadline_tests::stop_scanners(&mut runtime).await;
    runtime
}

pub(super) fn start<R: RunRepository + 'static>(
    runtime: &TokeiraRuntime<R>,
    delayed: bool,
) -> StartRequest {
    let mut request = tests::sample_start_request(None, None);
    request.run_key = RunKey::derive(request.namespace_id, &request.workflow_id, request.run_id);
    request.now = runtime.clock.now();
    request.request.received_at = request.now;
    request.workflow_start_delay = delayed.then_some(Duration::milliseconds(200));
    request
}

async fn timer_deadline(insert_asleep: bool, delayed: bool) {
    let (store, runtime) = fixture().await;
    let request = start(&runtime, delayed);
    let run = request.run_key;
    let queue = QueueKey {
        namespace_id: request.namespace_id,
        task_queue: request.task_queue.clone(),
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    };
    let install = async {
        runtime.start_workflow(request).await.unwrap();
        if !delayed {
            let task = runtime
                .poll_workflow_task(
                    queue.clone(),
                    WorkerIdentity("worker".into()),
                    std::time::Duration::from_secs(1),
                )
                .await
                .unwrap()
                .unwrap();
            let mut complete = completion_inline_tests::completion(&task, false);
            complete.now = runtime.clock.now();
            complete.request.received_at = complete.now;
            complete.commands = vec![WorkflowCommand::StartTimer {
                timer_id: "deadline".into(),
                fire_at: complete.now + Duration::milliseconds(200),
            }];
            runtime.complete_workflow_task(complete).await.unwrap();
        }
    };
    let mut install = Some(install);
    if !insert_asleep {
        install.take().unwrap().await;
    }
    let cancel = CancellationToken::new();
    let scanner = run_timer_scanner(
        store.clone(),
        runtime.lanes.clone(),
        runtime.lanes.len(),
        runtime.shard_owner.clone(),
        TimerScannerConfig {
            scan_interval: std::time::Duration::from_secs(3600),
            max_timers_per_scan: 100,
        },
        cancel.clone(),
        runtime.clock.clone(),
    );
    tokio::pin!(scanner);
    std::future::poll_fn(|cx| {
        assert!(scanner.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let receive = async {
        if let Some(install) = install {
            install.await;
        }
        let task = runtime
            .poll_workflow_task(
                queue.clone(),
                WorkerIdentity("next".into()),
                std::time::Duration::from_secs(2),
            )
            .await
            .unwrap()
            .expect("timer must wake the one-hour scanner");
        assert_eq!(task.run_key, run);
        let history = store.read_history_to_end(run, 0).await.unwrap();
        if !delayed {
            assert!(
                history
                    .iter()
                    .any(|event| matches!(event.kind, HistoryEventKind::TimerFired { .. }))
            );
        }
        cancel.cancel();
    };
    tokio::join!(scanner, receive);
    discovery_tests::stop(&runtime).await;
}

#[tokio::test(start_paused = true)]
async fn timer_tracked_before_sleep_fires_at_deadline() {
    timer_deadline(false, false).await;
}

#[tokio::test(start_paused = true)]
async fn timer_committed_while_asleep_fires_at_deadline() {
    timer_deadline(true, false).await;
}

#[tokio::test(start_paused = true)]
async fn start_delay_committed_while_asleep_fires_at_deadline() {
    timer_deadline(true, true).await;
}

async fn retry_delivery(pause: bool) {
    let (store, runtime) = fixture().await;
    let runtime = runtime.with_activity_eager_execution(true);
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
    let first = runtime
        .poll_workflow_task(
            queue.clone(),
            WorkerIdentity("worker".into()),
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap()
        .unwrap();
    let mut completion = completion_inline_tests::completion(&first, false);
    completion.now = runtime.clock.now();
    completion.request.received_at = completion.now;
    let mut activity = completion_eager_tests::schedule("retry", true);
    if let WorkflowCommand::ScheduleActivity {
        retry_policy: Some(policy),
        ..
    } = &mut activity
    {
        policy.initial_interval = Duration::milliseconds(300);
    }
    completion.commands = vec![
        activity,
        WorkflowCommand::StartTimer {
            timer_id: "later".into(),
            fire_at: completion.now + Duration::milliseconds(500),
        },
    ];
    let task = runtime
        .complete_workflow_task_with_result(completion)
        .await
        .unwrap()
        .activity_tasks
        .pop()
        .unwrap();
    runtime
        .fail_activity_task(
            task.token,
            Payload::new(Vec::new()),
            None,
            false,
            None,
            None,
            RequestContext::unattributed(runtime.clock.now()),
        )
        .await
        .unwrap();
    let LoadedRun::Existing(state) = store.load_run(run).await.unwrap() else {
        panic!("run")
    };
    let activity = &state.activities["retry"];
    let offer = super::activity::activity_dispatch_task(run, &state, activity);
    let attempt =
        crate::outbound::driver::Attempt::capture(&runtime.shard_owner, ShardId(0)).unwrap();
    // A stale scan observation cannot retire the obligation before the deps'
    // clock reaches it, including after a wall-clock correction.
    assert!(
        !super::activity::evaluate_activity_eligibility(
            &runtime.activity_retry_deps(),
            &offer,
            activity.current_attempt_scheduled_at.unwrap(),
            &attempt,
        )
        .await
        .unwrap()
    );
    if pause {
        store
            .create_workflow_rule(
                queue.namespace_id,
                tokeira_types::WorkflowRuleRecord {
                    id: "pause-retry".into(),
                    create_time: runtime.clock.now(),
                    created_by_identity: "test".into(),
                    description: String::new(),
                    trigger: tokeira_types::WorkflowRuleTrigger::ActivityStart {
                        predicate: "ActivityType = 'eager-type'".into(),
                    },
                    visibility_query: String::new(),
                    actions: vec![tokeira_types::WorkflowRuleAction::ActivityPause],
                    expiration_time: None,
                },
                10,
            )
            .await
            .unwrap();
    }
    let cancel = CancellationToken::new();
    let timers = run_timer_scanner(
        store.clone(),
        runtime.lanes.clone(),
        runtime.lanes.len(),
        runtime.shard_owner.clone(),
        TimerScannerConfig {
            scan_interval: std::time::Duration::from_secs(3600),
            max_timers_per_scan: 100,
        },
        cancel.clone(),
        runtime.clock.clone(),
    );
    let activities = crate::activity_timeout::run_activity_timeout_scanner(
        runtime.activity_retry_deps(),
        runtime.lanes.clone(),
        runtime.lanes.len(),
        ActivityTimeoutScannerConfig {
            scan_interval: std::time::Duration::from_secs(3600),
            ..Default::default()
        },
        cancel.clone(),
    );
    let driver = async {
        let activity_queue = QueueKey {
            task_kind: TaskKind::Activity,
            task_queue: TaskQueueName("activities".into()),
            ..queue.clone()
        };
        // No discovery demand is registered: only the eligibility wake can supply
        // this broker poll. This is stronger than a long discovery interval.
        let offer = runtime
            .activity_broker
            .poll_activity_offer_for_worker(
                &activity_queue,
                &WorkerIdentity("retry-worker".into()),
                std::time::Duration::from_secs(2),
            )
            .await
            .unwrap();
        if pause {
            assert!(offer.is_none());
            let LoadedRun::Existing(state) = store.load_run(run).await.unwrap() else {
                panic!("run")
            };
            assert!(state.activities["retry"].pause_info.is_some());
            cancel.cancel();
            return;
        }
        let offer = offer.expect("backoff must publish locally without discovery");
        let task = runtime
            .start_activity_task_offer(offer, WorkerIdentity("retry-worker".into()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.attempt, 2);
        runtime
            .complete_activity_task(
                task.token,
                Payloads::default(),
                None,
                RequestContext::unattributed(runtime.clock.now()),
            )
            .await
            .unwrap();
        let next = runtime
            .poll_workflow_task(
                queue,
                WorkerIdentity("worker".into()),
                std::time::Duration::from_secs(1),
            )
            .await
            .unwrap()
            .unwrap();
        let mut complete = completion_inline_tests::completion(&next, false);
        complete.now = runtime.clock.now();
        complete.request.received_at = complete.now;
        complete.commands = vec![WorkflowCommand::CompleteWorkflow {
            result: Payloads::default(),
        }];
        runtime.complete_workflow_task(complete).await.unwrap();
        let history = store.read_history_to_end(run, 0).await.unwrap();
        assert!(matches!(
            history.last().unwrap().kind,
            HistoryEventKind::WorkflowExecutionCompleted { .. }
        ));
        assert!(
            !history
                .iter()
                .any(|e| matches!(e.kind, HistoryEventKind::TimerFired { .. }))
        );
        cancel.cancel();
    };
    tokio::join!(timers, activities, driver);
    discovery_tests::stop(&runtime).await;
}

#[tokio::test(start_paused = true)]
async fn retry_delivery_closes_before_user_timer_without_discovery() {
    retry_delivery(false).await;
}

#[tokio::test(start_paused = true)]
async fn retry_pause_rule_publishes_nothing() {
    retry_delivery(true).await;
}

async fn successor_deadline(kind: u8, prepared: bool, lost_reply: bool) {
    let clock: Arc<dyn Clock> = Arc::new(TestClock::new());
    let store = Arc::new(repair_tests::Repo::with_inner(
        InMemoryStore::default().with_clock(clock.clone()),
    ));
    store
        .refuse_pair
        .store(prepared, std::sync::atomic::Ordering::SeqCst);
    store
        .lose_publication_reply
        .store(lost_reply, std::sync::atomic::Ordering::SeqCst);
    let runtime = fixture_on(store.clone(), clock).await;
    let mut request = start(&runtime, false);
    if kind == 1 {
        request.retry_policy = Some(RetryPolicy {
            initial_interval: Duration::milliseconds(200),
            backoff_coefficient: 1.0,
            maximum_interval: None,
            maximum_attempts: 2,
            non_retryable_error_types: Vec::new(),
        });
    }
    if kind == 2 {
        request.cron_schedule = Some("@every 1s".into());
    }
    let queue = QueueKey {
        namespace_id: request.namespace_id,
        task_queue: request.task_queue.clone(),
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    };
    runtime.start_workflow(request.clone()).await.unwrap();
    let first = runtime
        .poll_workflow_task(
            queue.clone(),
            WorkerIdentity("worker".into()),
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap()
        .unwrap();
    let cancel = CancellationToken::new();
    let scanner = run_timer_scanner(
        store.clone(),
        runtime.lanes.clone(),
        runtime.lanes.len(),
        runtime.shard_owner.clone(),
        TimerScannerConfig {
            scan_interval: std::time::Duration::from_secs(3600),
            max_timers_per_scan: 100,
        },
        cancel.clone(),
        runtime.clock.clone(),
    );
    tokio::pin!(scanner);
    std::future::poll_fn(|cx| {
        assert!(scanner.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let check = async {
        let mut complete = completion_inline_tests::completion(&first, false);
        complete.now = runtime.clock.now();
        complete.request.received_at = complete.now;
        complete.commands = vec![match kind {
            0 => WorkflowCommand::ContinueAsNew {
                new_run_id: RunId::new(),
                workflow_type: request.workflow_type.clone(),
                task_queue: request.task_queue.clone(),
                input: request.input.clone(),
                memo: request.memo.clone(),
                search_attributes: request.search_attributes.clone(),
                workflow_execution_timeout: request.workflow_execution_timeout,
                workflow_run_timeout: request.workflow_run_timeout,
                workflow_task_timeout: request.workflow_task_timeout,
                retry_policy: None,
                header: None,
                initial_versioning_behavior: Default::default(),
                successor_versioning_info: None,
            },
            1 => WorkflowCommand::FailWorkflow {
                failure: Payload::new(Vec::new()),
            },
            _ => WorkflowCommand::CompleteWorkflow {
                result: Payloads::default(),
            },
        }];
        runtime.complete_workflow_task(complete).await.unwrap();
        let next = runtime
            .poll_workflow_task(
                queue,
                WorkerIdentity("successor".into()),
                std::time::Duration::from_secs(3),
            )
            .await
            .unwrap()
            .expect("successor backoff must wake timer scanner");
        assert_ne!(next.run_key, first.run_key);
        let LoadedRun::Existing(state) = store.load_run(next.run_key).await.unwrap() else {
            panic!("run")
        };
        assert!(state.workflow_start_delay.is_some());
        assert!(runtime.clock.now() < first.started_time + Duration::seconds(3));
        cancel.cancel();
    };
    tokio::join!(scanner, check);
    discovery_tests::stop(&runtime).await;
}

#[tokio::test(start_paused = true)]
async fn successor_backoffs_wake_from_atomic_commits() {
    for kind in 0..3 {
        successor_deadline(kind, false, false).await;
    }
}

#[tokio::test(start_paused = true)]
async fn prepared_successor_backoffs_wake_after_visibility() {
    for kind in 0..3 {
        successor_deadline(kind, true, false).await;
    }
}

#[tokio::test(start_paused = true)]
async fn reconciled_preparation_wakes_after_lost_publication_reply() {
    successor_deadline(0, true, true).await;
}

#[tokio::test(start_paused = true)]
async fn reset_publication_wakes_preserved_timer() {
    let (store, runtime) = fixture().await;
    let request = start(&runtime, false);
    let queue = QueueKey {
        namespace_id: request.namespace_id,
        task_queue: request.task_queue.clone(),
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    };
    runtime.start_workflow(request.clone()).await.unwrap();
    let first = runtime
        .poll_workflow_task(
            queue.clone(),
            WorkerIdentity("worker".into()),
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap()
        .unwrap();
    let mut complete = completion_inline_tests::completion(&first, true);
    complete.force_new_workflow_task = true;
    complete.now = runtime.clock.now();
    complete.request.received_at = complete.now;
    complete.commands = vec![WorkflowCommand::StartTimer {
        timer_id: "preserved".into(),
        fire_at: complete.now + Duration::milliseconds(200),
    }];
    let second = runtime
        .complete_workflow_task_with_result(complete)
        .await
        .unwrap()
        .workflow_task
        .unwrap();
    let cancel = CancellationToken::new();
    let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
    let scanner = crate::scanner::run_timer_scanner_with_submit(
        store.clone(),
        runtime.shard_owner.clone(),
        TimerScannerConfig {
            scan_interval: std::time::Duration::from_secs(3600),
            max_timers_per_scan: 100,
        },
        cancel.clone(),
        runtime.clock.clone(),
        runtime.lanes.len(),
        |due, now| {
            let runtime = &runtime;
            let sent = sent.clone();
            async move {
                let run = due.run_key;
                runtime
                    .submit(run, crate::scanner::command_for_due_timer(due, now))
                    .await?;
                sent.send(run).unwrap();
                Ok(())
            }
        },
    );
    tokio::pin!(scanner);
    std::future::poll_fn(|cx| {
        assert!(scanner.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let check = async {
        let reset = runtime
            .reset_workflow(
                tokeira_types::ExecutionRef {
                    namespace_id: request.namespace_id,
                    workflow_id: request.workflow_id,
                    run_id: Some(request.run_id),
                },
                tokeira_kernel::ResetRequest {
                    fork_event_id: second.token.started_event_id,
                    new_run_id: RunId::new(),
                    reapply_exclude_signal: false,
                    reapply_exclude_update: false,
                    post_reset_versioning_overrides: Vec::new(),
                    expected_current_run_key: None,
                    reason: "reset".into(),
                    request: RequestContext::unattributed(runtime.clock.now()),
                    now: runtime.clock.now(),
                },
            )
            .await
            .unwrap();
        let LoadedRun::Existing(state) = store.load_run(reset.successor_run_key).await.unwrap()
        else {
            panic!("reset run")
        };
        assert!(state.timers.contains_key("preserved"));
        assert!(
            store
                .list_due_timers_for_shard(
                    ShardId(0),
                    runtime.clock.now() + Duration::seconds(1),
                    None,
                    100
                )
                .await
                .unwrap()
                .iter()
                .any(|row| row.run_key == reset.successor_run_key),
            "reset must publish timer rows reconstructed from its prefix"
        );
        let fired = tokio::time::timeout(std::time::Duration::from_secs(1), received.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fired, reset.successor_run_key);
        assert!(
            store
                .read_history_to_end(fired, 0)
                .await
                .unwrap()
                .iter()
                .any(|event| matches!(event.kind, HistoryEventKind::TimerFired { .. }))
        );
        cancel.cancel();
    };
    tokio::join!(scanner, check);
    discovery_tests::stop(&runtime).await;
}

#[tokio::test(start_paused = true)]
async fn captured_timer_listing_cannot_fire_replacement_early() {
    let (store, runtime) = fixture().await;
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
    let first = runtime
        .poll_workflow_task(
            queue,
            WorkerIdentity("worker".into()),
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap()
        .unwrap();
    let mut complete = completion_inline_tests::completion(&first, true);
    complete.force_new_workflow_task = true;
    complete.now = runtime.clock.now();
    complete.request.received_at = complete.now;
    complete.commands = vec![WorkflowCommand::StartTimer {
        timer_id: "reused".into(),
        fire_at: complete.now + Duration::milliseconds(200),
    }];
    let next = runtime
        .complete_workflow_task_with_result(complete)
        .await
        .unwrap()
        .workflow_task
        .unwrap();
    tokio::time::advance(std::time::Duration::from_millis(200)).await;
    let fired_at = runtime.clock.now();
    let captured = store
        .list_due_timers_for_shard(ShardId(0), fired_at, None, 100)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let replacement_at = fired_at + Duration::seconds(1);
    let mut replace = completion_inline_tests::completion(&next, false);
    replace.now = runtime.clock.now();
    replace.request.received_at = replace.now;
    replace.commands = vec![
        WorkflowCommand::CancelTimer {
            timer_id: "reused".into(),
        },
        WorkflowCommand::StartTimer {
            timer_id: "reused".into(),
            fire_at: replacement_at,
        },
    ];
    runtime.complete_workflow_task(replace).await.unwrap();
    let before = store.load_run(run).await.unwrap();
    let history_before = store.read_history_to_end(run, 0).await.unwrap();
    let submitted = runtime
        .submit(
            run,
            crate::scanner::command_for_due_timer(captured.clone(), fired_at),
        )
        .await;
    assert!(
        submitted.is_err(),
        "captured listing must not fire a replacement timer early"
    );
    let error = submitted.unwrap_err();
    assert!(
        matches!(error.downcast_ref::<crate::lane::KernelRejected>(),
        Some(crate::lane::KernelRejected(tokeira_kernel::Reject::UnknownTimer(id))) if id == "reused")
    );
    assert!(
        !crate::scanner::settle_failed_due_timer(
            store.as_ref(),
            Some(ShardId(0)),
            &captured,
            &error
        )
        .await
    );
    assert_eq!(store.load_run(run).await.unwrap(), before);
    assert_eq!(
        store.read_history_to_end(run, 0).await.unwrap(),
        history_before
    );
    let rows = store
        .list_due_timers_for_shard(ShardId(0), replacement_at, None, 100)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].fire_at, replacement_at);
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    crate::scanner::scan_due_timers_once_for_shard(
        store.as_ref(),
        ShardId(0),
        &TimerScannerConfig::default(),
        runtime.clock.now(),
        |due, now| {
            let runtime = &runtime;
            async move {
                runtime
                    .submit(run, crate::scanner::command_for_due_timer(due, now))
                    .await
                    .map(|_| ())
            }
        },
    )
    .await;
    let history = store.read_history_to_end(run, 0).await.unwrap();
    let fired: Vec<_> = history
        .iter()
        .filter(|event| {
            matches!(&event.kind,
        HistoryEventKind::TimerFired { timer_id, .. } if timer_id == "reused")
        })
        .collect();
    assert_eq!(fired.len(), 1);
    assert_eq!(fired[0].happened_at, replacement_at);
    let LoadedRun::Existing(state) = store.load_run(run).await.unwrap() else {
        panic!("run");
    };
    assert!(!state.timers.contains_key("reused"));
    assert!(
        store
            .list_due_timers_for_shard(ShardId(0), replacement_at, None, 100)
            .await
            .unwrap()
            .is_empty()
    );
    discovery_tests::stop(&runtime).await;
}

#[tokio::test(start_paused = true)]
async fn microsecond_timer_row_fires_current_timer_once() {
    let (store, runtime) = fixture().await;
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
    let first = runtime
        .poll_workflow_task(
            queue,
            WorkerIdentity("worker".into()),
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap()
        .unwrap();
    let mut complete = completion_inline_tests::completion(&first, false);
    complete.now = runtime.clock.now();
    complete.request.received_at = complete.now;
    let fire_at = complete.now + Duration::milliseconds(200) + Duration::nanoseconds(789);
    complete.commands = vec![WorkflowCommand::StartTimer {
        timer_id: "precision".into(),
        fire_at,
    }];
    runtime.complete_workflow_task(complete).await.unwrap();
    let mut row = store
        .list_due_timers_for_shard(ShardId(0), fire_at, None, 100)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(row.fire_at, fire_at);
    // DSQL's dispatch timestamp retains microseconds while the run keeps nanoseconds.
    row.fire_at = fire_at
        .replace_nanosecond(fire_at.nanosecond() / 1_000 * 1_000)
        .unwrap();
    assert!(row.fire_at < fire_at);
    tokio::time::advance(std::time::Duration::try_from(row.fire_at - runtime.clock.now()).unwrap())
        .await;
    let fired_at = runtime.clock.now();
    assert_eq!(fired_at, row.fire_at);
    runtime
        .submit(
            run,
            crate::scanner::command_for_due_timer(row.clone(), fired_at),
        )
        .await
        .expect("a current timer must fire at its microsecond row timestamp");
    let history = store.read_history_to_end(run, 0).await.unwrap();
    let fired: Vec<_> = history
        .iter()
        .filter(|event| {
            matches!(&event.kind,
        HistoryEventKind::TimerFired { timer_id, .. } if timer_id == "precision")
        })
        .collect();
    assert_eq!(fired.len(), 1);
    assert_eq!(fired[0].happened_at, fired_at);
    let after = store.load_run(run).await.unwrap();
    let LoadedRun::Existing(state) = &after else {
        panic!("run");
    };
    assert!(!state.timers.contains_key("precision"));
    assert!(
        store
            .list_due_timers_for_shard(ShardId(0), fire_at, None, 100)
            .await
            .unwrap()
            .is_empty()
    );
    let duplicate = runtime
        .submit(run, crate::scanner::command_for_due_timer(row, fired_at))
        .await
        .unwrap_err();
    assert!(
        matches!(duplicate.downcast_ref::<crate::lane::KernelRejected>(),
        Some(crate::lane::KernelRejected(tokeira_kernel::Reject::UnknownTimer(id))) if id == "precision")
    );
    assert_eq!(store.load_run(run).await.unwrap(), after);
    assert_eq!(store.read_history_to_end(run, 0).await.unwrap(), history);
    discovery_tests::stop(&runtime).await;
}
