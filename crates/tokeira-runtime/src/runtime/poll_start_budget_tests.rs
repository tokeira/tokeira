//! A matched task's start runs on its own budget, not on what remains of the poll's wait.

use super::{
    repair_tests::{Point, Repo},
    timer_deadline_tests::{fixture_on, start},
    *,
};
use crate::test_clock::TestClock;
use tokeira_storage::InMemoryStore;

const WAIT: std::time::Duration = std::time::Duration::from_secs(1);
const SLOW_START: std::time::Duration = std::time::Duration::from_secs(2);

async fn started_workflow() -> (Arc<Repo>, TokeiraRuntime<Repo>, QueueKey, RunKey) {
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
    (repo, runtime, queue, run)
}

/// Schedules one activity and returns its queue.
async fn scheduled_activity() -> (Arc<Repo>, TokeiraRuntime<Repo>, QueueKey) {
    let (repo, runtime, queue, _) = started_workflow().await;
    let first = runtime
        .poll_workflow_task(queue.clone(), WorkerIdentity("worker".into()), WAIT)
        .await
        .unwrap()
        .unwrap();
    let mut completion = completion_inline_tests::completion(&first, false);
    completion.now = runtime.clock.now();
    completion.request.received_at = completion.now;
    completion.commands = vec![completion_eager_tests::schedule("slow", true)];
    runtime.complete_workflow_task(completion).await.unwrap();
    let activity_queue = QueueKey {
        task_kind: TaskKind::Activity,
        task_queue: TaskQueueName("activities".into()),
        ..queue
    };
    (repo, runtime, activity_queue)
}

#[test]
fn start_budget_is_ten_seconds_within_the_caller_deadline() {
    let now = tokio::time::Instant::now();
    let ten = std::time::Duration::from_secs(10);
    assert_eq!(polled_task_start_deadline(now, None), now + ten);
    let far = now + std::time::Duration::from_secs(60);
    assert_eq!(polled_task_start_deadline(now, Some(far)), now + ten);
    let near = now + std::time::Duration::from_secs(3);
    assert_eq!(polled_task_start_deadline(now, Some(near)), near);
}

#[tokio::test(start_paused = true)]
async fn activity_matched_as_the_wait_ends_still_starts() {
    let (repo, runtime, queue) = scheduled_activity().await;
    repo.slow(Point::Load, SLOW_START);
    let polled = runtime
        .poll_activity_task_within(queue, WorkerIdentity("worker".into()), WAIT, None)
        .await;
    let task = polled
        .expect("the start has its own budget past the wait")
        .expect("the matched activity starts");
    assert_eq!(task.activity_id, "slow");
}

#[tokio::test(start_paused = true)]
async fn activity_start_never_runs_past_the_caller_deadline() {
    let (repo, runtime, queue) = scheduled_activity().await;
    repo.slow(Point::Load, SLOW_START);
    let caller_deadline = tokio::time::Instant::now() + WAIT + WAIT / 2;
    let polled = runtime
        .poll_activity_task_within(
            queue,
            WorkerIdentity("worker".into()),
            WAIT,
            Some(caller_deadline),
        )
        .await;
    assert!(
        !matches!(polled, Ok(Some(_))),
        "a start still running at the caller's deadline is abandoned"
    );
    assert_eq!(tokio::time::Instant::now(), caller_deadline);
}

#[tokio::test(start_paused = true)]
async fn workflow_task_matched_as_the_wait_ends_still_starts() {
    let (repo, runtime, queue, run) = started_workflow().await;
    repo.slow(Point::StatsLoad, SLOW_START);
    let polled = runtime
        .poll_workflow_activation_within(
            queue,
            None,
            false,
            WorkerIdentity("worker".into()),
            WAIT,
            None,
        )
        .await;
    let Some(WorkflowActivation::WorkflowTask(task)) =
        polled.expect("the start has its own budget past the wait")
    else {
        panic!("the matched workflow task starts");
    };
    assert_eq!(task.run_key, run);
}

#[tokio::test(start_paused = true)]
async fn workflow_task_start_never_runs_past_the_caller_deadline() {
    let (repo, runtime, queue, _) = started_workflow().await;
    repo.slow(Point::StatsLoad, SLOW_START);
    let caller_deadline = tokio::time::Instant::now() + WAIT + WAIT / 2;
    let polled = runtime
        .poll_workflow_activation_within(
            queue,
            None,
            false,
            WorkerIdentity("worker".into()),
            WAIT,
            Some(caller_deadline),
        )
        .await;
    assert!(
        matches!(polled, Ok(None)),
        "a start still running at the caller's deadline is abandoned"
    );
    assert_eq!(tokio::time::Instant::now(), caller_deadline);
}
