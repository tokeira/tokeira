//! A timeout pass reads each run once, however many of its activities it selects.

use std::sync::atomic::Ordering;

use super::{
    repair_tests::Repo,
    timer_deadline_tests::{fixture_on, start},
    *,
};
use crate::{activity_timeout::scan_activity_timeouts_once, test_clock::TestClock};
use tokeira_storage::InMemoryStore;

const ACTIVITIES: [&str; 3] = ["a", "b", "c"];

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

#[tokio::test(start_paused = true)]
async fn full_pass_reads_a_run_once_and_still_decides_each_activity() {
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
            queue,
            WorkerIdentity("worker".into()),
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap()
        .unwrap();
    let mut completion = completion_inline_tests::completion(&first, false);
    completion.now = runtime.clock.now();
    completion.request.received_at = completion.now;
    completion.commands = ACTIVITIES
        .iter()
        .map(|id| {
            let mut command = completion_eager_tests::schedule(id, false);
            if let WorkflowCommand::ScheduleActivity {
                schedule_to_start_timeout,
                request_eager_execution,
                ..
            } = &mut command
            {
                *schedule_to_start_timeout = Some(Duration::seconds(1));
                *request_eager_execution = false;
            }
            command
        })
        .collect();
    runtime.complete_workflow_task(completion).await.unwrap();
    let tracked = || {
        runtime
            .activity_tracking
            .snapshot()
            .into_iter()
            .filter(|entry| entry.run_key == run)
            .count()
    };
    assert_eq!(tracked(), ACTIVITIES.len());

    // Nothing is due: the reconciliation alone costs one read of the run,
    // pass after pass, rather than one per tracked activity.
    for _ in 0..2 {
        let before = repo.loads.load(Ordering::SeqCst);
        scan(&runtime).await;
        assert_eq!(repo.loads.load(Ordering::SeqCst) - before, 1);
        assert_eq!(tracked(), ACTIVITIES.len());
    }

    // Each activity still gets its own decision from the shared read, and the
    // later submissions survive commit revalidation against a newer run.
    tokio::time::advance(std::time::Duration::from_nanos(1_000_000_001)).await;
    scan(&runtime).await;
    let history = repo.read_history_to_end(run, 0).await.unwrap();
    let mut timed_out: Vec<_> = history
        .iter()
        .filter_map(|event| match &event.kind {
            HistoryEventKind::ActivityTaskTimedOut {
                activity_id,
                timeout_type,
                ..
            } => {
                assert_eq!(timeout_type, "schedule_to_start");
                Some(activity_id.as_str())
            }
            _ => None,
        })
        .collect();
    timed_out.sort_unstable();
    assert_eq!(timed_out, ACTIVITIES);
    assert_eq!(tracked(), 0);
    discovery_tests::stop(&runtime).await;
}
