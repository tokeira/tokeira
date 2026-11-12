//! Timer decisions are fenced against starts, heartbeats and retries during commit,
//! and tracking updates that land late keep a later commit's start.

use std::sync::atomic::Ordering;

use super::{
    repair_tests::{Point, Repo},
    timer_deadline_tests::{fixture_on, start},
    *,
};
use crate::{
    ActivityStartOutcome, ActivityTaskStartRequest,
    activity_eligibility::scan_activity_eligibility_once,
    activity_timeout::{scan_activity_timeouts_at, scan_activity_timeouts_once},
    test_clock::TestClock,
};
use tokeira_storage::InMemoryStore;

#[derive(Clone, Copy)]
enum Change {
    Start,
    Heartbeat,
    Retry,
}

async fn race(change: Change, point: Point, retrying_scanner: bool) {
    let clock: Arc<dyn Clock> = Arc::new(TestClock::new());
    let repo = Arc::new(Repo::with_inner(
        InMemoryStore::default().with_clock(clock.clone()),
    ));
    let runtime = fixture_on(repo.clone(), clock).await;
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
    let mut command = completion_eager_tests::schedule("race", true);
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
        policy.maximum_attempts = if matches!(change, Change::Retry) || retrying_scanner {
            3
        } else {
            1
        };
        if matches!(change, Change::Retry) {
            policy.non_retryable_error_types = vec!["TemporalTimeout:Heartbeat".into()];
        }
        *schedule_to_close_timeout = Some(Duration::minutes(1));
        *schedule_to_start_timeout = Some(Duration::seconds(1));
        *start_to_close_timeout = Some(Duration::seconds(10));
        *heartbeat_timeout = Some(Duration::seconds(1));
    }
    let mut completion = completion_inline_tests::completion(&first, false);
    completion.now = runtime.clock.now();
    completion.request.received_at = completion.now;
    completion.commands = vec![command];
    runtime.complete_workflow_task(completion).await.unwrap();
    queue.task_kind = TaskKind::Activity;
    queue.task_queue = TaskQueueName("activities".into());
    let mut offer = Some(
        runtime
            .activity_broker
            .try_claim_activity_offer(&queue, run, "race")
            .await
            .unwrap(),
    );
    let token = if matches!(change, Change::Start) {
        None
    } else {
        Some(
            runtime
                .start_activity_task_offer(offer.take().unwrap(), WorkerIdentity("worker".into()))
                .await
                .unwrap()
                .unwrap()
                .token,
        )
    };
    tokio::time::advance(std::time::Duration::from_nanos(1_000_000_001)).await;
    // Keep the commit gate reachable after pre-apply validation is added: the
    // lane must build this transition from the current activity, not an older
    // cached image that could be rejected before any commit is attempted.
    runtime
        .shard_owner
        .write()
        .unwrap()
        .invalidate_published_states();
    let before_conflicts = repo.commit_conflicts.load(Ordering::SeqCst);
    let (entered, resume) = repo.pause(point);
    let deps = runtime.activity_retry_deps();
    let config = ActivityTimeoutScannerConfig::default();
    let scanning = scan_activity_timeouts_once(
        &deps,
        Some(ShardId(0)),
        &runtime.lanes,
        runtime.lanes.len(),
        &config,
    );
    let change_state = async {
        entered.await.unwrap();
        match change {
            Change::Start => {
                runtime
                    .start_activity_task_offer(
                        offer.take().unwrap(),
                        WorkerIdentity("worker".into()),
                    )
                    .await
                    .unwrap()
                    .unwrap();
            }
            Change::Heartbeat => {
                runtime
                    .record_activity_heartbeat(token.unwrap(), None, None)
                    .await
                    .unwrap();
            }
            Change::Retry => {
                runtime
                    .fail_activity_task(
                        token.unwrap(),
                        Payload::new(Vec::new()),
                        None,
                        false,
                        None,
                        None,
                        RequestContext::unattributed(runtime.clock.now()),
                    )
                    .await
                    .unwrap();
            }
        }
        resume.send(()).unwrap();
    };
    tokio::join!(scanning, change_state);
    if point == Point::Commit {
        assert!(
            repo.commit_conflicts.load(Ordering::SeqCst) > before_conflicts,
            "the observation must survive an OCC retry"
        );
    }
    let LoadedRun::Existing(state) = repo.load_run(run).await.unwrap() else {
        panic!("run")
    };
    let current = state
        .activities
        .get("race")
        .expect("a stale timeout must leave the activity pending");
    assert_eq!(
        current.attempt,
        if matches!(change, Change::Retry) {
            2
        } else {
            1
        },
        "a stale timeout must not spend an attempt"
    );
    assert!(
        repo.read_history_to_end(run, 0)
            .await
            .unwrap()
            .iter()
            .all(|event| !matches!(event.kind, HistoryEventKind::ActivityTaskTimedOut { .. }))
    );
    let tracked = runtime
        .activity_tracking
        .inner
        .tracked(&(run, "race".into()))
        .expect("stale rejection must preserve tracking");
    assert!(
        tracked.refresh_pending,
        "stale rejection must request an authoritative reload"
    );
    scan_activity_timeouts_at(
        &deps,
        Some(ShardId(0)),
        &runtime.lanes,
        runtime.lanes.len(),
        &config,
        runtime.clock.now(),
    )
    .await;
    if matches!(change, Change::Retry) {
        let due = current.current_attempt_scheduled_at.unwrap();
        tokio::time::advance(std::time::Duration::try_from(due - runtime.clock.now()).unwrap())
            .await;
        scan_activity_eligibility_once(&deps, ShardId(0), runtime.clock.now()).await;
        let offer = runtime
            .activity_broker
            .try_claim_activity_offer(&queue, run, "race")
            .await
            .unwrap();
        assert_eq!(
            runtime
                .start_activity_task_offer(offer, WorkerIdentity("worker".into()))
                .await
                .unwrap()
                .unwrap()
                .attempt,
            2
        );
    }
    tokio::time::advance(std::time::Duration::from_nanos(1_000_000_001)).await;
    scan_activity_timeouts_once(
        &deps,
        Some(ShardId(0)),
        &runtime.lanes,
        runtime.lanes.len(),
        &config,
    )
    .await;
    let LoadedRun::Existing(later) = repo.load_run(run).await.unwrap() else {
        panic!("run")
    };
    if retrying_scanner {
        assert_eq!(
            later.activities["race"].attempt, 2,
            "the later heartbeat deadline must retry the activity"
        );
    } else {
        assert!(
            !later.activities.contains_key("race"),
            "the later heartbeat deadline must still fire"
        );
        assert_eq!(
            repo.read_history_to_end(run, 0)
                .await
                .unwrap()
                .iter()
                .filter(|event| matches!(event.kind, HistoryEventKind::ActivityTaskTimedOut { .. }))
                .count(),
            1
        );
    }
    discovery_tests::stop(&runtime).await;
}

#[tokio::test(start_paused = true)]
async fn final_timeout_rechecks_start_after_reload() {
    race(Change::Start, Point::Loaded, false).await;
}
#[tokio::test(start_paused = true)]
async fn final_timeout_rechecks_start_after_conflict() {
    race(Change::Start, Point::Commit, false).await;
}
#[tokio::test(start_paused = true)]
async fn final_timeout_rechecks_heartbeat_after_reload() {
    race(Change::Heartbeat, Point::Loaded, false).await;
}
#[tokio::test(start_paused = true)]
async fn final_timeout_rechecks_heartbeat_after_conflict() {
    race(Change::Heartbeat, Point::Commit, false).await;
}
#[tokio::test(start_paused = true)]
async fn final_timeout_rechecks_worker_retry_after_reload() {
    race(Change::Retry, Point::Loaded, false).await;
}
#[tokio::test(start_paused = true)]
async fn final_timeout_rechecks_worker_retry_after_conflict() {
    race(Change::Retry, Point::Commit, false).await;
}
#[tokio::test(start_paused = true)]
async fn timeout_retry_rechecks_heartbeat_after_reload() {
    race(Change::Heartbeat, Point::Loaded, true).await;
}
#[tokio::test(start_paused = true)]
async fn timeout_retry_rechecks_heartbeat_after_conflict() {
    race(Change::Heartbeat, Point::Commit, true).await;
}

const ORDERED: &str = "ordered";

/// A run whose activity has a retry policy without backoff, a one-second
/// heartbeat timeout, and an offer ready for its first attempt.
async fn scheduled_activity() -> (Arc<Repo>, TokeiraRuntime<Repo>, RunKey, QueueKey) {
    let clock: Arc<dyn Clock> = Arc::new(TestClock::new());
    let repo = Arc::new(Repo::with_inner(
        InMemoryStore::default().with_clock(clock.clone()),
    ));
    let runtime = fixture_on(repo.clone(), clock).await;
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
    let mut command = completion_eager_tests::schedule(ORDERED, true);
    if let WorkflowCommand::ScheduleActivity {
        start_to_close_timeout,
        heartbeat_timeout,
        ..
    } = &mut command
    {
        *start_to_close_timeout = Some(Duration::seconds(10));
        *heartbeat_timeout = Some(Duration::seconds(1));
    }
    let mut completion = completion_inline_tests::completion(&first, false);
    completion.now = runtime.clock.now();
    completion.request.received_at = completion.now;
    completion.commands = vec![command];
    runtime.complete_workflow_task(completion).await.unwrap();
    queue.task_kind = TaskKind::Activity;
    queue.task_queue = TaskQueueName("activities".into());
    (repo, runtime, run, queue)
}

async fn start_offered(
    runtime: &TokeiraRuntime<Repo>,
    queue: &QueueKey,
    run: RunKey,
) -> StartedActivityTask {
    let offer = runtime
        .activity_broker
        .try_claim_activity_offer(queue, run, ORDERED)
        .await
        .unwrap();
    runtime
        .start_activity_task_offer(offer, WorkerIdentity("worker".into()))
        .await
        .unwrap()
        .unwrap()
}

/// Offer the attempt the run now holds, as delivery does once it is due.
async fn offer_current_attempt(
    repo: &Repo,
    runtime: &TokeiraRuntime<Repo>,
    queue: &QueueKey,
    run: RunKey,
) -> ActivityTaskOffer {
    let LoadedRun::Existing(state) = repo.load_run(run).await.unwrap() else {
        panic!("run")
    };
    let task = activity_dispatch_task(run, &state, &state.activities[ORDERED]);
    runtime
        .activity_broker
        .publish_activity_task(task, None)
        .await
        .unwrap();
    runtime
        .activity_broker
        .try_claim_activity_offer(queue, run, ORDERED)
        .await
        .unwrap()
}

/// The started attempt's start reached tracking, so its start-to-close and
/// heartbeat deadlines are tracked.
async fn assert_start_tracked(
    repo: &Repo,
    runtime: &TokeiraRuntime<Repo>,
    run: RunKey,
    attempt: u32,
) {
    let LoadedRun::Existing(state) = repo.load_run(run).await.unwrap() else {
        panic!("run")
    };
    let activity = &state.activities[ORDERED];
    assert_eq!(activity.attempt, attempt);
    assert!(activity.started_at.is_some());
    let cached = runtime
        .activity_tracking
        .snapshot()
        .into_iter()
        .find(|entry| entry.activity_id == ORDERED)
        .expect("the started attempt stays tracked");
    assert_eq!(
        cached.started_at, activity.started_at,
        "tracking must hold the start the run committed"
    );
}

/// A worker's retry commits the next attempt, and that attempt starts before
/// the retry's tracking update lands. The late reset must not clear the start.
#[tokio::test(start_paused = true)]
async fn late_worker_retry_update_keeps_the_next_attempts_start() {
    let (repo, runtime, run, queue) = scheduled_activity().await;
    let first = start_offered(&runtime, &queue, run).await;
    let (retried, resume) = repo.pause(Point::Committed);
    let failing = runtime.fail_activity_task(
        first.token,
        Payload::new(Vec::new()),
        None,
        false,
        None,
        None,
        RequestContext::unattributed(runtime.clock.now()),
    );
    let racing = async {
        retried.await.unwrap();
        let offer = offer_current_attempt(&repo, &runtime, &queue, run).await;
        runtime
            .start_activity_task_offer(offer, WorkerIdentity("worker".into()))
            .await
            .unwrap()
            .unwrap();
        resume.send(()).unwrap();
    };
    let (failed, ()) = tokio::join!(failing, racing);
    failed.unwrap();
    assert_start_tracked(&repo, &runtime, run, 2).await;
    discovery_tests::stop(&runtime).await;
}

/// A start's caller learns the committed start from a replayed request and
/// heartbeats before the original call's tracking update lands. The heartbeat
/// must not cost the start its deadlines.
#[tokio::test(start_paused = true)]
async fn heartbeat_before_the_start_update_keeps_the_start() {
    let (repo, runtime, run, queue) = scheduled_activity().await;
    let offer = runtime
        .activity_broker
        .try_claim_activity_offer(&queue, run, ORDERED)
        .await
        .unwrap();
    let request = ActivityTaskStartRequest {
        offered: offer.task.clone(),
        request_id: offer.request_id,
        worker: WorkerIdentity("worker".into()),
    };
    let (committed, resume) = repo.pause(Point::Committed);
    let starting = runtime.start_forwarded_activity_task(request.clone());
    let racing = async {
        committed.await.unwrap();
        let ActivityStartOutcome::Replayed(replayed) = runtime
            .start_forwarded_activity_task(request.clone())
            .await
            .unwrap()
        else {
            panic!("the committed start must replay");
        };
        runtime
            .record_activity_heartbeat(replayed.token, None, None)
            .await
            .unwrap();
        resume.send(()).unwrap();
    };
    let (started, ()) = tokio::join!(starting, racing);
    assert!(matches!(started.unwrap(), ActivityStartOutcome::Started(_)));
    drop(offer);
    assert_start_tracked(&repo, &runtime, run, 1).await;
    discovery_tests::stop(&runtime).await;
}

/// The scanner retries a timed-out attempt, and the next attempt's start reads
/// the run before the retry's tracking update lands. The start's own update,
/// which lands last, must still apply.
#[tokio::test(start_paused = true)]
async fn start_update_after_a_scanner_retry_update_keeps_the_start() {
    let (repo, runtime, run, queue) = scheduled_activity().await;
    start_offered(&runtime, &queue, run).await;
    tokio::time::advance(std::time::Duration::from_nanos(1_000_000_001)).await;
    let (retried, resume_retry) = repo.pause(Point::Committed);
    let deps = runtime.activity_retry_deps();
    let config = ActivityTimeoutScannerConfig::default();
    let scanning = scan_activity_timeouts_once(
        &deps,
        Some(ShardId(0)),
        &runtime.lanes,
        runtime.lanes.len(),
        &config,
    );
    tokio::pin!(scanning);
    tokio::select! {
        biased;
        entered = retried => entered.unwrap(),
        () = &mut scanning => panic!("the heartbeat timeout must retry the attempt"),
    }
    let offer = offer_current_attempt(&repo, &runtime, &queue, run).await;
    let (committing, resume_start) = repo.pause(Point::Commit);
    let starting = runtime.start_activity_task_offer(offer, WorkerIdentity("worker".into()));
    tokio::pin!(starting);
    tokio::select! {
        biased;
        entered = committing => entered.unwrap(),
        _ = &mut starting => panic!("the start must pause before its commit"),
    }
    resume_retry.send(()).unwrap();
    scanning.await;
    resume_start.send(()).unwrap();
    starting.await.unwrap().unwrap();
    assert_start_tracked(&repo, &runtime, run, 2).await;
    discovery_tests::stop(&runtime).await;
}

/// A full pass requests a reload while a worker's retry update is pending and
/// the next attempt's start has lost its outcome. The retry's update must not
/// clear the request, so the scanner's reload restores the start.
#[tokio::test(start_paused = true)]
async fn late_retry_update_keeps_a_requested_reload() {
    let (repo, runtime, run, queue) = scheduled_activity().await;
    let first = start_offered(&runtime, &queue, run).await;
    let (retried, resume) = repo.pause(Point::Committed);
    let failing = runtime.fail_activity_task(
        first.token,
        Payload::new(Vec::new()),
        None,
        false,
        None,
        None,
        RequestContext::unattributed(runtime.clock.now()),
    );
    let racing = async {
        retried.await.unwrap();
        let offer = offer_current_attempt(&repo, &runtime, &queue, run).await;
        // The start commits, but its caller loses the outcome, so no
        // tracking update follows.
        *repo.fail.lock().unwrap() = Some(Point::Committed);
        assert!(
            runtime
                .start_activity_task_offer(offer, WorkerIdentity("worker".into()))
                .await
                .is_err()
        );
        runtime.activity_tracking.inner.begin_full_pass(true);
        resume.send(()).unwrap();
    };
    let (failed, ()) = tokio::join!(failing, racing);
    failed.unwrap();
    assert!(
        runtime
            .activity_tracking
            .inner
            .tracked(&(run, ORDERED.into()))
            .unwrap()
            .refresh_pending,
        "an update must not clear a requested reload"
    );
    scan_activity_timeouts_at(
        &runtime.activity_retry_deps(),
        Some(ShardId(0)),
        &runtime.lanes,
        runtime.lanes.len(),
        &ActivityTimeoutScannerConfig::default(),
        runtime.clock.now(),
    )
    .await;
    assert_start_tracked(&repo, &runtime, run, 2).await;
    discovery_tests::stop(&runtime).await;
}
