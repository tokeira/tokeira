//! Offers must not outrun attempt tracking; full scans recover missed starts.

use super::{
    timer_deadline_tests::{fixture, start},
    *,
};
use crate::{
    activity_eligibility::scan_activity_eligibility_once,
    activity_timeout::{ActivityTimeouts, scan_activity_timeouts_at, scan_activity_timeouts_once},
};
use tokeira_storage::InMemoryStore;

async fn scheduled(
    retry: bool,
    lose_publication: bool,
) -> (
    Arc<InMemoryStore>,
    TokeiraRuntime<InMemoryStore>,
    RunKey,
    QueueKey,
) {
    let (store, runtime) = fixture().await;
    let request = start(&runtime, false);
    let run = request.run_key;
    let mut queue = QueueKey {
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
    let mut command = completion_eager_tests::schedule("Heartbeat", retry);
    if let WorkflowCommand::ScheduleActivity {
        start_to_close_timeout,
        heartbeat_timeout,
        retry_policy,
        ..
    } = &mut command
    {
        *start_to_close_timeout = Some(Duration::seconds(10));
        *heartbeat_timeout = Some(Duration::seconds(1));
        if let Some(policy) = retry_policy {
            policy.initial_interval = Duration::milliseconds(1);
            policy.maximum_attempts = 2;
        }
    }
    let mut complete = completion_inline_tests::completion(&first, false);
    complete.now = runtime.clock.now();
    complete.request.received_at = complete.now;
    complete.commands = vec![command];
    if lose_publication {
        *runtime
            .outbound_publisher
            .publication_failure
            .lock()
            .unwrap() = Some(TaskKind::Activity);
    }
    runtime.complete_workflow_task(complete).await.unwrap();
    queue.task_kind = TaskKind::Activity;
    queue.task_queue = TaskQueueName("activities".into());
    (store, runtime, run, queue)
}

async fn take(
    runtime: &TokeiraRuntime<InMemoryStore>,
    run: RunKey,
    queue: &QueueKey,
) -> ActivityTaskOffer {
    runtime
        .activity_broker
        .try_claim_activity_offer(queue, run, "Heartbeat")
        .await
        .expect("local offer")
}

async fn assert_heartbeat_timeout(store: &InMemoryStore, run: RunKey) {
    let history = store.read_history_to_end(run, 0).await.unwrap();
    assert!(history.iter().any(|event| matches!(&event.kind,
        HistoryEventKind::ActivityTaskTimedOut { timeout_type, .. } if timeout_type == "heartbeat")),
        "the activity must time out on its heartbeat");
}

#[tokio::test(start_paused = true)]
async fn first_attempt_is_offered_only_by_publisher() {
    for lost in [false, true] {
        let (_, runtime, run, queue) = scheduled(false, lost).await;
        let _publisher_offer = if lost {
            None
        } else {
            Some(take(&runtime, run, &queue).await)
        };
        scan_activity_eligibility_once(
            &runtime.activity_retry_deps(),
            ShardId(0),
            runtime.clock.now(),
        )
        .await;
        assert!(
            runtime
                .activity_broker
                .try_claim_activity_offer(&queue, run, "Heartbeat")
                .await
                .is_none(),
            "eligibility must not offer a first attempt already due at commit"
        );
        discovery_tests::stop(&runtime).await;
    }
}

#[tokio::test(start_paused = true)]
async fn short_retry_local_offer_keeps_started_heartbeat_deadline() {
    let (store, runtime, run, queue) = scheduled(true, false).await;
    let first = runtime
        .start_activity_task_offer(
            take(&runtime, run, &queue).await,
            WorkerIdentity("worker".into()),
        )
        .await
        .unwrap()
        .unwrap();
    let (registered, resume) = runtime.activity_tracking.eligibility.pause_next_refresh();
    let fail = runtime.fail_activity_task(
        first.token,
        Payload::new(Vec::new()),
        None,
        false,
        None,
        None,
        RequestContext::unattributed(runtime.clock.now()),
    );
    tokio::pin!(fail);
    let driver = async {
        registered.await.unwrap();
        tokio::time::advance(std::time::Duration::from_millis(1)).await;
        scan_activity_eligibility_once(
            &runtime.activity_retry_deps(),
            ShardId(0),
            runtime.clock.now(),
        )
        .await;
        let next = runtime
            .start_activity_task_offer(
                take(&runtime, run, &queue).await,
                WorkerIdentity("worker".into()),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(next.attempt, 2);
        resume.send(()).unwrap();
        next
    };
    let (failed, next) = tokio::join!(&mut fail, driver);
    failed.unwrap();
    tokio::time::advance(std::time::Duration::from_millis(800)).await;
    runtime
        .record_activity_heartbeat(next.token, None, None)
        .await
        .unwrap();
    tokio::time::advance(std::time::Duration::from_nanos(1_000_000_001)).await;
    scan_activity_timeouts_at(
        &runtime.activity_retry_deps(),
        Some(ShardId(0)),
        &runtime.lanes,
        runtime.lanes.len(),
        &ActivityTimeoutScannerConfig::default(),
        runtime.clock.now(),
    )
    .await;
    assert_heartbeat_timeout(store.as_ref(), run).await;
    discovery_tests::stop(&runtime).await;
}

async fn late_scheduled_record(missing: bool) {
    let (store, runtime, run, queue) = scheduled(false, false).await;
    let LoadedRun::Existing(before) = store.load_run(run).await.unwrap() else {
        panic!("run")
    };
    let activity = &before.activities["Heartbeat"];
    if missing {
        runtime.activity_tracking.remove(run, "Heartbeat");
    }
    let started = runtime
        .start_activity_task_offer(
            take(&runtime, run, &queue).await,
            WorkerIdentity("worker".into()),
        )
        .await
        .unwrap()
        .unwrap();
    tokio::time::advance(std::time::Duration::from_millis(800)).await;
    runtime
        .record_activity_heartbeat(started.token, None, None)
        .await
        .unwrap();
    runtime.activity_tracking.record_scheduled_with_timeouts(
        run,
        ShardId(0),
        "Heartbeat",
        activity.scheduled_at,
        Some(ActivityTimeouts::from(activity)),
    );
    tokio::time::advance(std::time::Duration::from_millis(200)).await;
    let deps = runtime.activity_retry_deps();
    scan_activity_timeouts_once(
        &deps,
        Some(ShardId(0)),
        &runtime.lanes,
        runtime.lanes.len(),
        &ActivityTimeoutScannerConfig::default(),
    )
    .await;
    assert!(
        store
            .read_history_to_end(run, 0)
            .await
            .unwrap()
            .iter()
            .all(|e| !matches!(e.kind, HistoryEventKind::ActivityTaskTimedOut { .. })),
        "durable heartbeat extends the deadline beyond the first full pass"
    );
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    scan_activity_timeouts_once(
        &deps,
        Some(ShardId(0)),
        &runtime.lanes,
        runtime.lanes.len(),
        &ActivityTimeoutScannerConfig::default(),
    )
    .await;
    assert_heartbeat_timeout(store.as_ref(), run).await;
    discovery_tests::stop(&runtime).await;
}

#[tokio::test(start_paused = true)]
async fn full_pass_repairs_a_start_that_found_no_tracking_entry() {
    late_scheduled_record(true).await;
}

#[tokio::test(start_paused = true)]
async fn full_pass_repairs_a_start_overwritten_by_late_schedule_tracking() {
    late_scheduled_record(false).await;
}
