//! Reloaded attempt anchors remain authoritative after a scoped reset loses a race.

use super::{
    repair_tests::{Point, Repo},
    timer_deadline_tests::{fixture_on, start},
    *,
};
use crate::{
    activity_timeout::{ActivityTrackingEntry, scan_activity_timeouts_once},
    test_clock::TestClock,
};
use tokeira_storage::InMemoryStore;

async fn skipped_reset(
    heartbeat: bool,
    attempts: u32,
    schedule_to_start: bool,
) -> (
    Arc<Repo>,
    TokeiraRuntime<Repo>,
    RunKey,
    ActivityTrackingEntry,
) {
    let clock: Arc<dyn Clock> = Arc::new(TestClock::new());
    let repo = Arc::new(Repo::with_inner(
        InMemoryStore::default().with_clock(clock.clone()),
    ));
    let runtime = fixture_on(repo.clone(), clock).await;
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
    let mut command = completion_eager_tests::schedule("retry", true);
    if let WorkflowCommand::ScheduleActivity {
        retry_policy,
        schedule_to_close_timeout,
        schedule_to_start_timeout,
        start_to_close_timeout,
        heartbeat_timeout,
        ..
    } = &mut command
    {
        let policy = retry_policy.as_mut().unwrap();
        policy.initial_interval = Duration::seconds(5);
        policy.maximum_attempts = attempts;
        *schedule_to_close_timeout = Some(Duration::minutes(1));
        *schedule_to_start_timeout = schedule_to_start.then_some(Duration::seconds(3));
        *start_to_close_timeout = Some(Duration::seconds(1));
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
        ..queue
    };
    let offer = runtime
        .activity_broker
        .try_claim_activity_offer(&activity_queue, run, "retry")
        .await
        .unwrap();
    let started = runtime
        .start_activity_task_offer(offer, WorkerIdentity("worker".into()))
        .await
        .unwrap()
        .unwrap();
    tokio::time::advance(std::time::Duration::from_nanos(1_000_000_001)).await;
    let (committing, resume) = repo.pause(Point::Commit);
    let deps = runtime.activity_retry_deps();
    let config = ActivityTimeoutScannerConfig::default();
    let scan = scan_activity_timeouts_once(
        &deps,
        Some(ShardId(0)),
        &runtime.lanes,
        runtime.lanes.len(),
        &config,
    );
    let race = async {
        committing.await.unwrap();
        runtime
            .record_activity_heartbeat(started.token, None, None)
            .await
            .unwrap();
        resume.send(()).unwrap();
    };
    tokio::join!(scan, race);
    let LoadedRun::Existing(state) = repo.load_run(run).await.unwrap() else {
        panic!("run")
    };
    assert_eq!(state.activities["retry"].attempt, 2);
    assert!(state.activities["retry"].started_at.is_none());
    let cached = runtime
        .activity_tracking
        .snapshot()
        .into_iter()
        .find(|entry| entry.activity_id == "retry")
        .unwrap();
    assert!(
        cached.started_at.is_some(),
        "the heartbeat must make the captured reset stale"
    );
    assert_ne!(
        Some(cached.last_dispatched_at),
        state.activities["retry"].current_attempt_scheduled_at
    );
    (repo, runtime, run, cached)
}

async fn scan(runtime: &TokeiraRuntime<Repo>) {
    scan_activity_timeouts_once(
        &runtime.activity_retry_deps(),
        Some(ShardId(0)),
        &runtime.lanes,
        runtime.lanes.len(),
        &ActivityTimeoutScannerConfig::default(),
    )
    .await;
}

async fn unstarted_retry(heartbeat: bool, attempts: u32) {
    let (repo, runtime, run, _) = skipped_reset(heartbeat, attempts, false).await;
    tokio::time::advance(std::time::Duration::from_secs(2)).await;
    scan(&runtime).await;
    let LoadedRun::Existing(state) = repo.load_run(run).await.unwrap() else {
        panic!("run")
    };
    assert!(
        state.activities.contains_key("retry"),
        "an unstarted retry must not be terminally timed out"
    );
    assert_eq!(
        state.activities["retry"].attempt, 2,
        "an unstarted retry must not spend another attempt"
    );
    assert!(
        repo.read_history_to_end(run, 0)
            .await
            .unwrap()
            .iter()
            .all(|event| !matches!(event.kind, HistoryEventKind::ActivityTaskTimedOut { .. }))
    );
    let cached = runtime
        .activity_tracking
        .snapshot()
        .into_iter()
        .find(|entry| entry.activity_id == "retry")
        .unwrap();
    assert_eq!(cached.started_at, None);
    assert_eq!(cached.last_heartbeat_at, None);
    assert_eq!(
        Some(cached.last_dispatched_at),
        state.activities["retry"].current_attempt_scheduled_at
    );
    discovery_tests::stop(&runtime).await;
}

#[tokio::test(start_paused = true)]
async fn skipped_reset_does_not_spend_an_unstarted_retry_on_heartbeat() {
    unstarted_retry(true, 3).await;
}

#[tokio::test(start_paused = true)]
async fn skipped_reset_does_not_spend_an_unstarted_retry_on_start_to_close() {
    unstarted_retry(false, 3).await;
}

#[tokio::test(start_paused = true)]
async fn skipped_reset_does_not_terminally_timeout_an_unstarted_retry_on_heartbeat() {
    unstarted_retry(true, 2).await;
}

#[tokio::test(start_paused = true)]
async fn skipped_reset_does_not_terminally_timeout_an_unstarted_retry_on_start_to_close() {
    unstarted_retry(false, 2).await;
}

#[tokio::test(start_paused = true)]
async fn skipped_reset_schedule_to_start_uses_the_new_attempt_deadline() {
    let (repo, runtime, run, old) = skipped_reset(true, 2, true).await;
    tokio::time::advance(std::time::Duration::from_secs(2)).await;
    assert!(runtime.clock.now() > old.last_dispatched_at + Duration::seconds(3));
    scan(&runtime).await;
    let LoadedRun::Existing(state) = repo.load_run(run).await.unwrap() else {
        panic!("run")
    };
    assert!(
        state.activities.contains_key("retry"),
        "the old attempt's deadlines must not close the new attempt"
    );
    let due = state.activities["retry"]
        .current_attempt_scheduled_at
        .unwrap()
        + Duration::seconds(3);
    let cached = runtime
        .activity_tracking
        .snapshot()
        .into_iter()
        .find(|entry| entry.activity_id == "retry")
        .unwrap();
    assert_eq!(cached.started_at, None);
    assert_eq!(cached.last_heartbeat_at, None);
    assert_eq!(
        Some(cached.last_dispatched_at),
        state.activities["retry"].current_attempt_scheduled_at
    );
    tokio::time::advance(std::time::Duration::try_from(due - runtime.clock.now()).unwrap()).await;
    scan(&runtime).await;
    let LoadedRun::Existing(at_deadline) = repo.load_run(run).await.unwrap() else {
        panic!("run")
    };
    assert_eq!(at_deadline.activities["retry"].attempt, 2);
    tokio::time::advance(std::time::Duration::from_nanos(1)).await;
    scan(&runtime).await;
    let history = repo.read_history_to_end(run, 0).await.unwrap();
    let timeouts: Vec<_> = history
        .iter()
        .filter_map(|event| match &event.kind {
            HistoryEventKind::ActivityTaskTimedOut { timeout_type, .. } => {
                Some(timeout_type.as_str())
            }
            _ => None,
        })
        .collect();
    assert_eq!(timeouts, ["schedule_to_start"]);
    discovery_tests::stop(&runtime).await;
}
