//! Real deadline scanners preserve activity/workflow timeout ordering.

use std::{future::Future, task::Poll};

use tokeira_types::WorkflowId;

use super::{
    completion_eager_tests::schedule,
    completion_inline_tests::{completion, fixture_with_start},
    discovery_tests::stop,
    *,
};
use crate::activity_timeout::run_activity_timeout_scanner;

async fn scanner_fixture() -> (
    Arc<tokeira_storage::InMemoryStore>,
    TokeiraRuntime<tokeira_storage::InMemoryStore>,
    StartedWorkflowTask,
    QueueKey,
) {
    let mut start = tests::sample_start_request(None, None);
    start.workflow_task_timeout = Duration::milliseconds(500);
    let (store, mut runtime, first, queue) = fixture_with_start(start).await;
    stop_scanners(&mut runtime).await;
    (store, runtime, first, queue)
}

async fn stop_scanners<R: RunRepository + 'static>(runtime: &mut TokeiraRuntime<R>) {
    runtime.timer_scanner_cancel.cancel();
    runtime.workflow_timeout_scanner_cancel.cancel();
    runtime.wft_timeout_scanner_cancel.cancel();
    runtime.activity_timeout_scanner_cancel.cancel();
    runtime.nexus_timeout_scanner_cancel.cancel();
    runtime.completion_callback_scanner_cancel.cancel();
    for handle in [
        runtime.timer_scanner_handle.take(),
        runtime.workflow_timeout_scanner_handle.take(),
        runtime.wft_timeout_scanner_handle.take(),
        runtime.activity_timeout_scanner_handle.take(),
        runtime.nexus_timeout_scanner_handle.take(),
        runtime.completion_callback_scanner_handle.take(),
    ]
    .into_iter()
    .flatten()
    {
        handle.await.unwrap();
    }
}

async fn counting_fixture() -> (
    Arc<super::repair_tests::Repo>,
    TokeiraRuntime<super::repair_tests::Repo>,
    StartedWorkflowTask,
    QueueKey,
) {
    let store = Arc::new(super::repair_tests::Repo::default());
    let mut runtime = TokeiraRuntime::new(
        store.clone(),
        1,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
    );
    stop_scanners(&mut runtime).await;
    let request = tests::sample_start_request(None, None);
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
            WorkerIdentity("first".into()),
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap()
        .unwrap();
    (store, runtime, first, queue)
}

fn short_activity(started: bool) -> WorkflowCommand {
    let mut command = schedule("activity", false);
    if let WorkflowCommand::ScheduleActivity {
        schedule_to_start_timeout,
        start_to_close_timeout,
        heartbeat_timeout,
        ..
    } = &mut command
    {
        *schedule_to_start_timeout = (!started).then_some(Duration::milliseconds(200));
        *start_to_close_timeout = Some(if started {
            Duration::milliseconds(200)
        } else {
            Duration::seconds(10)
        });
        *heartbeat_timeout = None;
    }
    command
}

async fn timeout_loop(started: bool) {
    let (store, runtime, first, queue) = scanner_fixture().await;
    let runtime = runtime.with_activity_eager_execution(started);
    let mut complete = completion(&first, false);
    complete.commands = vec![short_activity(started)];
    if !started {
        runtime
            .complete_workflow_task_with_result(complete.clone())
            .await
            .unwrap();
    }
    let cancel = CancellationToken::new();
    let scanner = run_activity_timeout_scanner(
        runtime.activity_retry_deps(),
        runtime.lanes.clone(),
        runtime.lanes.len(),
        ActivityTimeoutScannerConfig {
            scan_interval: std::time::Duration::from_secs(3600),
            ..Default::default()
        },
        cancel.clone(),
    );
    tokio::pin!(scanner);
    std::future::poll_fn(|cx| {
        assert!(scanner.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let driver = async {
        if started {
            runtime
                .complete_workflow_task_with_result(complete)
                .await
                .unwrap();
        }
        let next = runtime
            .poll_workflow_task(
                queue,
                WorkerIdentity("next".into()),
                std::time::Duration::from_secs(3),
            )
            .await
            .unwrap()
            .expect("activity timeout must wake the workflow poll");
        assert_eq!(next.token.attempt, 1);
        let history = store.read_history_to_end(first.run_key, 0).await.unwrap();
        assert!(
            history
                .iter()
                .any(|event| matches!(event.kind, HistoryEventKind::ActivityTaskTimedOut { .. }))
        );
        cancel.cancel();
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(scanner, driver);
    })
    .await
    .unwrap();
    stop(&runtime).await;
}

#[tokio::test]
async fn scheduled_activity_deadline_wakes_one_hour_scanner() {
    timeout_loop(false).await;
}

#[tokio::test]
async fn started_activity_deadline_wakes_sleeping_one_hour_scanner() {
    timeout_loop(true).await;
}

#[tokio::test]
async fn retry_eligibility_deadline_wakes_one_hour_scanner() {
    let (store, runtime, first, _) = counting_fixture().await;
    let runtime = runtime.with_activity_eager_execution(true);
    let mut command = schedule("activity", true);
    if let WorkflowCommand::ScheduleActivity {
        retry_policy: Some(policy),
        ..
    } = &mut command
    {
        policy.initial_interval = Duration::milliseconds(200);
    }
    let mut complete = completion(&first, false);
    complete.commands = vec![command];
    let task = runtime
        .complete_workflow_task_with_result(complete)
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
            RequestContext::unattributed(OffsetDateTime::now_utc()),
        )
        .await
        .unwrap();
    let LoadedRun::Existing(state) = store.inner.load_run(first.run_key).await.unwrap() else {
        panic!("run")
    };
    let due = state.activities["activity"]
        .current_attempt_scheduled_at
        .unwrap();
    let (entered, resume) = store.pause(super::repair_tests::Point::Load);
    let cancel = CancellationToken::new();
    let scanner = run_activity_timeout_scanner(
        runtime.activity_retry_deps(),
        runtime.lanes.clone(),
        runtime.lanes.len(),
        ActivityTimeoutScannerConfig {
            scan_interval: std::time::Duration::from_secs(3600),
            ..Default::default()
        },
        cancel.clone(),
    );
    let observe = async {
        tokio::time::timeout(std::time::Duration::from_secs(3), entered)
            .await
            .expect("eligibility must reload at its due time")
            .unwrap();
        assert!(OffsetDateTime::now_utc() >= due);
        resume.send(()).unwrap();
        cancel.cancel();
    };
    tokio::join!(scanner, observe);
    runtime.runtime_shutdown.begin_shutdown();
    runtime
        .runtime_shutdown
        .wait(std::time::Instant::now() + std::time::Duration::from_secs(10))
        .await
        .unwrap();
}

#[tokio::test]
async fn activity_deadline_precedes_workflow_timeout_and_flushes_before_attempt_one() {
    let (store, runtime, first, queue) = scanner_fixture().await;
    let mut complete = completion(&first, true);
    complete.force_new_workflow_task = true;
    complete.commands = vec![short_activity(false)];
    let outcome = runtime
        .complete_workflow_task_with_result(complete)
        .await
        .unwrap();
    assert!(outcome.workflow_task.is_some());
    let cancel = CancellationToken::new();
    let activities = run_activity_timeout_scanner(
        runtime.activity_retry_deps(),
        runtime.lanes.clone(),
        runtime.lanes.len(),
        ActivityTimeoutScannerConfig {
            scan_interval: std::time::Duration::from_secs(3600),
            ..Default::default()
        },
        cancel.clone(),
    );
    let workflows = run_wft_timeout_scanner(
        runtime.wft_timeout_tracking.clone(),
        runtime.lanes.clone(),
        runtime.lanes.len(),
        runtime.shard_owner.clone(),
        WftTimeoutScannerConfig {
            scan_interval: std::time::Duration::from_secs(3600),
            ..Default::default()
        },
        cancel.clone(),
        runtime.clock.clone(),
    );
    let driver = async {
        let next = runtime
            .poll_workflow_task(
                queue,
                WorkerIdentity("next".into()),
                std::time::Duration::from_secs(3),
            )
            .await
            .unwrap()
            .expect("workflow task timeout must wake its poll");
        assert_eq!(
            next.token.attempt, 1,
            "the earlier activity timeout must have buffered"
        );
        let history = store.read_history_to_end(first.run_key, 0).await.unwrap();
        let suffix = &history[history.len() - 4..];
        assert!(matches!(
            suffix[0].kind,
            HistoryEventKind::WorkflowTaskTimedOut { .. }
        ));
        assert!(matches!(
            suffix[1].kind,
            HistoryEventKind::ActivityTaskTimedOut { .. }
        ));
        assert!(matches!(
            suffix[2].kind,
            HistoryEventKind::WorkflowTaskScheduled { attempt: 1, .. }
        ));
        assert!(matches!(
            suffix[3].kind,
            HistoryEventKind::WorkflowTaskStarted { .. }
        ));
        cancel.cancel();
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(activities, workflows, driver);
    })
    .await
    .unwrap();
    stop(&runtime).await;
}

async fn finish<R: RunRepository + 'static>(runtime: &TokeiraRuntime<R>) {
    runtime.runtime_shutdown.begin_shutdown();
    runtime
        .runtime_shutdown
        .wait(std::time::Instant::now() + std::time::Duration::from_secs(10))
        .await
        .unwrap();
}

#[tokio::test]
async fn deadline_pass_reloads_only_due_and_refreshes_early_cache_once() {
    let (store, runtime, first, _) = counting_fixture().await;
    let mut complete = completion(&first, false);
    complete.commands = vec![schedule("early-cache", false), schedule("future", false)];
    runtime
        .complete_workflow_task_with_result(complete)
        .await
        .unwrap();
    crate::activity_eligibility::scan_activity_eligibility_once(
        &runtime.activity_retry_deps(),
        ShardId(0),
        OffsetDateTime::now_utc(),
    )
    .await;
    let now = OffsetDateTime::now_utc();
    runtime.activity_tracking.set_timeouts(
        first.run_key,
        "early-cache",
        crate::activity_timeout::ActivityTimeouts {
            schedule_to_start: Some(Duration::nanoseconds(1)),
            ..Default::default()
        },
    );
    let before = store.loads.load(std::sync::atomic::Ordering::SeqCst);
    let deps = runtime.activity_retry_deps();
    let config = ActivityTimeoutScannerConfig::default();
    crate::activity_timeout::scan_activity_timeouts_at(
        &deps,
        None,
        &runtime.lanes,
        runtime.lanes.len(),
        &config,
        now,
    )
    .await;
    assert_eq!(
        store.loads.load(std::sync::atomic::Ordering::SeqCst) - before,
        1
    );
    let delay = runtime
        .activity_tracking
        .scan_delay(now, config.scan_interval);
    assert!(delay > std::time::Duration::ZERO);
    crate::activity_timeout::scan_activity_timeouts_at(
        &deps,
        None,
        &runtime.lanes,
        runtime.lanes.len(),
        &config,
        now,
    )
    .await;
    assert_eq!(
        store.loads.load(std::sync::atomic::Ordering::SeqCst) - before,
        1
    );
    let history = store
        .inner
        .read_history_to_end(first.run_key, 0)
        .await
        .unwrap();
    assert!(
        !history
            .iter()
            .any(|event| matches!(event.kind, HistoryEventKind::ActivityTaskTimedOut { .. }))
    );
    finish(&runtime).await;
}

#[tokio::test]
async fn full_pass_fires_a_deadline_hidden_by_a_late_cache() {
    let (store, runtime, first, _) = counting_fixture().await;
    let mut complete = completion(&first, false);
    complete.commands = vec![short_activity(false)];
    runtime
        .complete_workflow_task_with_result(complete)
        .await
        .unwrap();
    runtime.activity_tracking.set_timeouts(
        first.run_key,
        "activity",
        crate::activity_timeout::ActivityTimeouts::default(),
    );
    crate::activity_eligibility::scan_activity_eligibility_once(
        &runtime.activity_retry_deps(),
        ShardId(0),
        OffsetDateTime::now_utc(),
    )
    .await;
    let now = OffsetDateTime::now_utc() + Duration::seconds(1);
    let deps = runtime.activity_retry_deps();
    let config = ActivityTimeoutScannerConfig::default();
    let before = store.loads.load(std::sync::atomic::Ordering::SeqCst);
    crate::activity_timeout::scan_activity_timeouts_at(
        &deps,
        None,
        &runtime.lanes,
        runtime.lanes.len(),
        &config,
        now,
    )
    .await;
    assert_eq!(
        store.loads.load(std::sync::atomic::Ordering::SeqCst),
        before
    );
    runtime.activity_tracking.inner.begin_full_pass(true);
    crate::activity_timeout::scan_activity_timeouts_at(
        &deps,
        None,
        &runtime.lanes,
        runtime.lanes.len(),
        &config,
        now,
    )
    .await;
    let history = store
        .inner
        .read_history_to_end(first.run_key, 0)
        .await
        .unwrap();
    assert!(
        history
            .iter()
            .any(|event| matches!(event.kind, HistoryEventKind::ActivityTaskTimedOut { .. }))
    );
    finish(&runtime).await;
}

#[tokio::test]
async fn cap_continues_in_deadline_order_but_failed_submission_waits() {
    let (store, runtime, first, _) = counting_fixture().await;
    let mut complete = completion(&first, false);
    complete.commands = ["later", "earlier"]
        .into_iter()
        .map(|id| {
            let mut command = short_activity(false);
            if let WorkflowCommand::ScheduleActivity {
                activity_id,
                schedule_to_start_timeout,
                ..
            } = &mut command
            {
                *activity_id = id.into();
                *schedule_to_start_timeout = Some(Duration::milliseconds(if id == "later" {
                    200
                } else {
                    100
                }));
            }
            command
        })
        .collect();
    runtime
        .complete_workflow_task_with_result(complete)
        .await
        .unwrap();
    crate::activity_eligibility::scan_activity_eligibility_once(
        &runtime.activity_retry_deps(),
        ShardId(0),
        OffsetDateTime::now_utc(),
    )
    .await;
    let now = OffsetDateTime::now_utc() + Duration::seconds(1);
    let deps = runtime.activity_retry_deps();
    let config = ActivityTimeoutScannerConfig {
        max_timeouts_per_scan: 1,
        ..Default::default()
    };
    runtime.activity_tracking.wake.start_scan(now);
    crate::activity_timeout::scan_activity_timeouts_at(
        &deps,
        None,
        &runtime.lanes,
        runtime.lanes.len(),
        &config,
        now,
    )
    .await;
    let LoadedRun::Existing(state) = store.inner.load_run(first.run_key).await.unwrap() else {
        panic!("run");
    };
    assert!(!state.activities.contains_key("earlier"));
    assert!(state.activities.contains_key("later"));
    assert_eq!(
        runtime
            .activity_tracking
            .scan_delay(now, config.scan_interval),
        std::time::Duration::ZERO
    );
    *store.fail.lock().unwrap() = Some(super::repair_tests::Point::Commit);
    crate::activity_timeout::scan_activity_timeouts_at(
        &deps,
        None,
        &runtime.lanes,
        runtime.lanes.len(),
        &config,
        now,
    )
    .await;
    assert_eq!(
        runtime
            .activity_tracking
            .scan_delay(now, config.scan_interval),
        config.scan_interval
    );
    let LoadedRun::Existing(state) = store.inner.load_run(first.run_key).await.unwrap() else {
        panic!("run");
    };
    assert!(state.activities.contains_key("later"));
    finish(&runtime).await;
}

#[tokio::test]
async fn eligibility_quantum_continues_immediately_and_defers_failed_attempt() {
    let (store, runtime, first, _) = counting_fixture().await;
    let mut complete = completion(&first, false);
    complete.commands = (0..66)
        .map(|i| schedule(&format!("activity-{i:03}"), false))
        .collect();
    runtime
        .complete_workflow_task_with_result(complete)
        .await
        .unwrap();
    let deps = runtime.activity_retry_deps();
    let now = OffsetDateTime::now_utc();
    let before = store.loads.load(std::sync::atomic::Ordering::SeqCst);
    *store.fail.lock().unwrap() = Some(super::repair_tests::Point::Load);
    crate::activity_eligibility::scan_activity_eligibility_once(&deps, ShardId(0), now).await;
    assert_eq!(
        store.loads.load(std::sync::atomic::Ordering::SeqCst) - before,
        64
    );
    assert_eq!(
        runtime
            .activity_tracking
            .scan_delay(now, std::time::Duration::from_secs(1)),
        std::time::Duration::ZERO
    );
    crate::activity_eligibility::scan_activity_eligibility_once(&deps, ShardId(0), now).await;
    assert_eq!(
        store.loads.load(std::sync::atomic::Ordering::SeqCst) - before,
        66
    );
    assert_eq!(
        runtime
            .activity_tracking
            .scan_delay(now, std::time::Duration::from_secs(1)),
        std::time::Duration::from_secs(1)
    );
    runtime.activity_tracking.eligibility.begin_full_pass();
    crate::activity_eligibility::scan_activity_eligibility_once(&deps, ShardId(0), now).await;
    assert_eq!(
        store.loads.load(std::sync::atomic::Ordering::SeqCst) - before,
        67
    );
    finish(&runtime).await;
}

#[tokio::test]
async fn unknown_cache_is_filled_by_one_deadline_reload() {
    let (store, runtime, first, _) = counting_fixture().await;
    let mut complete = completion(&first, false);
    complete.commands = vec![schedule("activity", false)];
    runtime
        .complete_workflow_task_with_result(complete)
        .await
        .unwrap();
    let mut entry = runtime.activity_tracking.snapshot().pop().unwrap();
    entry.timeouts = None;
    runtime.activity_tracking.insert(entry);
    let before = store.loads.load(std::sync::atomic::Ordering::SeqCst);
    crate::activity_timeout::scan_activity_timeouts_at(
        &runtime.activity_retry_deps(),
        None,
        &runtime.lanes,
        runtime.lanes.len(),
        &ActivityTimeoutScannerConfig::default(),
        OffsetDateTime::now_utc(),
    )
    .await;
    assert_eq!(
        store.loads.load(std::sync::atomic::Ordering::SeqCst) - before,
        1
    );
    assert!(
        runtime
            .activity_tracking
            .snapshot()
            .pop()
            .unwrap()
            .timeouts
            .is_some()
    );
    finish(&runtime).await;
}

#[tokio::test]
async fn applied_commits_refresh_only_their_runs_changed_activity_caches() {
    let (_, runtime, first, queue) = counting_fixture().await;
    let runtime = runtime.with_activity_eager_execution(true);
    let mut complete = completion(&first, false);
    complete.commands = vec![schedule("target", false), schedule("unchanged", false)];
    runtime
        .complete_workflow_task_with_result(complete)
        .await
        .unwrap();

    let mut other_request = tests::sample_start_request(None, None);
    other_request.namespace_id = queue.namespace_id;
    other_request.workflow_id = WorkflowId("other-run".into());
    runtime.start_workflow(other_request).await.unwrap();
    let other = runtime
        .poll_workflow_task(
            queue,
            WorkerIdentity("other".into()),
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap()
        .unwrap();
    let mut complete = completion(&other, false);
    complete.commands = vec![schedule("target", false)];
    runtime
        .complete_workflow_task_with_result(complete)
        .await
        .unwrap();

    let tracking = &runtime.activity_tracking.inner;
    for entry in tracking.snapshot(None) {
        assert!(tracking.mark_attempted(
            &(entry.value.run_key, entry.value.activity_id.clone()),
            &entry
        ));
    }
    let controls: Vec<_> = [
        (first.run_key, "unchanged".to_string()),
        (other.run_key, "target".to_string()),
    ]
    .into_iter()
    .map(|key| {
        let entry = tracking.tracked(&key).unwrap();
        (key, entry.value)
    })
    .collect();
    let now = OffsetDateTime::now_utc();
    let updated = runtime
        .submit(
            first.run_key,
            Command::UpdateActivityOptions(tokeira_kernel::UpdateActivityOptionsRequest {
                target: ActivityControlTarget::Id("target".into()),
                task_queue: FieldChange::Unchanged,
                schedule_to_close_timeout: FieldChange::Unchanged,
                schedule_to_start_timeout: FieldChange::Unchanged,
                start_to_close_timeout: FieldChange::Set(Some(Duration::minutes(2))),
                heartbeat_timeout: FieldChange::Unchanged,
                retry_policy: Default::default(),
                priority: Default::default(),
                original_options: Default::default(),
                restore_original_options: false,
                reschedule_at: Default::default(),
                request: RequestContext {
                    request_id: tokeira_types::RequestId("cache-options".into()),
                    ..RequestContext::unattributed(now)
                },
                now,
            }),
        )
        .await
        .unwrap();
    assert!(matches!(updated, CommitResult::Applied { .. }));
    let target = tracking.tracked(&(first.run_key, "target".into())).unwrap();
    assert_eq!(
        target.value.timeouts.unwrap().start_to_close,
        Some(Duration::minutes(2))
    );
    assert!(!target.value.timeouts.unwrap().paused);
    assert!(target.deadline_pending);
    let paused = runtime
        .pause_activities(
            first.run_key,
            tokeira_kernel::PauseActivityRequest {
                target: ActivityControlTarget::Id("target".into()),
                identity: "test".into(),
                reason: "test".into(),
                request: RequestContext {
                    request_id: tokeira_types::RequestId("cache-pause".into()),
                    ..RequestContext::unattributed(now)
                },
                now,
                rule_id: None,
            },
        )
        .await
        .unwrap();
    assert!(matches!(paused, CommitResult::Applied { .. }));
    assert!(
        tracking
            .tracked(&(first.run_key, "target".into()))
            .unwrap()
            .value
            .timeouts
            .unwrap()
            .paused
    );
    for (key, before) in controls {
        let after = tracking.tracked(&key).unwrap();
        assert_eq!(after.value, before);
        assert!(
            !after.deadline_pending,
            "unchanged caches must not rearm attempted work"
        );
        assert!(!after.refresh_pending);
    }
    finish(&runtime).await;
}

#[tokio::test]
async fn deadline_passes_select_due_entries_among_many_future_entries() {
    let (store, runtime, first, _) = counting_fixture().await;
    let mut complete = completion(&first, false);
    complete.commands = (0..128)
        .map(|i| schedule(&format!("activity-{i:03}"), false))
        .collect();
    runtime
        .complete_workflow_task_with_result(complete)
        .await
        .unwrap();
    let LoadedRun::Existing(state) = store.inner.load_run(first.run_key).await.unwrap() else {
        panic!("run");
    };
    let now = OffsetDateTime::now_utc();
    for activity in state.activities.values() {
        let due = if activity.activity_id == "activity-000" {
            now
        } else {
            now + Duration::hours(1)
        };
        runtime.activity_tracking.eligibility.record(
            activity_dispatch_task(first.run_key, &state, activity),
            ShardId(0),
            due,
        );
    }
    runtime.activity_tracking.set_timeouts(
        first.run_key,
        "activity-001",
        crate::activity_timeout::ActivityTimeouts {
            schedule_to_start: Some(Duration::nanoseconds(1)),
            ..Default::default()
        },
    );
    let deps = runtime.activity_retry_deps();
    let before = store.loads.load(std::sync::atomic::Ordering::SeqCst);
    crate::activity_timeout::scan_activity_timeouts_at(
        &deps,
        None,
        &runtime.lanes,
        runtime.lanes.len(),
        &ActivityTimeoutScannerConfig::default(),
        now,
    )
    .await;
    assert_eq!(
        store.loads.load(std::sync::atomic::Ordering::SeqCst) - before,
        1
    );
    crate::activity_eligibility::scan_activity_eligibility_once(&deps, ShardId(0), now).await;
    assert_eq!(
        store.loads.load(std::sync::atomic::Ordering::SeqCst) - before,
        2
    );
    assert_eq!(runtime.activity_tracking.snapshot().len(), 128);
    finish(&runtime).await;
}
