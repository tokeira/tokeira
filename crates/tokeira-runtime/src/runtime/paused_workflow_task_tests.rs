//! Paused task failures through persistence, queue polling and supplied-clock starts.

use super::{completion_inline_tests::completion, discovery_tests::stop, *};
use tokeira_kernel::{
    ContinueAsNewAdvicePolicy, HistoryEvent, StickySpec, WorkflowTaskFailedCause,
    WorkflowTaskFailedRequest, WorkflowTaskTimedOutRequest, WorkflowTaskTimeoutType,
};
use tokeira_storage::InMemoryStore;

struct Fixture {
    store: Arc<InMemoryStore>,
    runtime: TokeiraRuntime<InMemoryStore>,
    key: RunKey,
    queue: QueueKey,
}

fn at(seconds: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(2_000_000_000 + seconds).unwrap()
}

fn context(id: &str, seconds: i64) -> RequestContext {
    RequestContext {
        request_id: RequestId(id.into()),
        ..RequestContext::unattributed(at(seconds))
    }
}

impl Fixture {
    async fn new() -> Self {
        let store = Arc::new(InMemoryStore::default());
        let runtime = TokeiraRuntime::new(
            store.clone(),
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
        );
        let mut request = tests::sample_start_request(None, None);
        request.now = at(0);
        request.request = context("start", 0);
        let queue = QueueKey {
            namespace_id: request.namespace_id,
            task_queue: request.task_queue.clone(),
            task_kind: TaskKind::Workflow,
            deployment: None,
            build_id: None,
        };
        let key = request.run_key;
        runtime.start_workflow(request).await.unwrap();
        Self {
            store,
            runtime,
            key,
            queue,
        }
    }

    async fn state(&self) -> WorkflowState {
        let LoadedRun::Existing(state) = self.store.load_run(self.key).await.unwrap() else {
            panic!("existing run");
        };
        state
    }

    async fn apply(&self, command: Command) {
        assert!(matches!(
            self.runtime.submit(self.key, command).await.unwrap(),
            CommitResult::Applied { .. }
        ));
    }

    async fn poll_and_start(&self, seconds: i64) -> StartedWorkflowTask {
        let sequence = self
            .state()
            .await
            .pending_workflow_task
            .unwrap()
            .logical_seq;
        for _ in 0..4 {
            let Some(WorkflowPollResult::Queued(offer, _)) = self
                .runtime
                .broker
                .poll_workflow_task(
                    &self.queue,
                    &WorkerIdentity("worker".into()),
                    std::time::Duration::from_secs(1),
                )
                .await
                .unwrap()
            else {
                panic!("queued task");
            };
            if offer.logical_seq != sequence {
                continue;
            }
            self.apply(Command::WorkflowTaskStarted(StartWorkflowTaskRequest {
                logical_seq: offer.logical_seq,
                worker_identity: WorkerIdentity("worker".into()),
                request_id: format!("start-{seconds}"),
                history_size_bytes: 0,
                advice_policy: ContinueAsNewAdvicePolicy::V1_31_0,
                deployment_transition: None,
                deployment_transition_revision_number: None,
                target_version_changed_enabled: false,
                target_deployment_version: None,
                polled_task_queue: offer.queue.task_queue,
                now: at(seconds),
            }))
            .await;
            return self
                .runtime
                .started_workflow_task_from_state(
                    &self.state().await,
                    false,
                    WorkerIdentity("worker".into()),
                )
                .await
                .unwrap();
        }
        panic!("current dispatch after stale offers");
    }

    async fn pause(&self, seconds: i64) {
        self.apply(Command::PauseWorkflow(PauseWorkflowRequest {
            identity: "operator".into(),
            reason: "pause".into(),
            request: context(&format!("pause-{seconds}"), seconds),
            now: at(seconds),
        }))
        .await;
    }

    async fn unpause(&self, seconds: i64) {
        self.apply(Command::UnpauseWorkflow(UnpauseWorkflowRequest {
            identity: "operator".into(),
            reason: "resume".into(),
            request: context(&format!("resume-{seconds}"), seconds),
            now: at(seconds),
        }))
        .await;
    }

    async fn signal(&self, seconds: i64) {
        self.apply(Command::Signal(SignalRequest {
            signal_name: "buffered".into(),
            input: Payloads::default(),
            header: None,
            links: Vec::new(),
            request: context(&format!("signal-{seconds}"), seconds),
            now: at(seconds),
        }))
        .await;
    }

    async fn complete(&self, task: &StartedWorkflowTask, seconds: i64) {
        let mut request = completion(task, false);
        request.now = at(seconds);
        request.request = context(&format!("complete-{seconds}"), seconds);
        self.runtime
            .complete_workflow_task_with_result(request)
            .await
            .unwrap();
    }

    async fn history(&self) -> Vec<HistoryEvent> {
        self.store.read_history_to_end(self.key, 0).await.unwrap()
    }
}

fn history_shape(history: &[HistoryEvent]) -> Vec<String> {
    history
        .iter()
        .enumerate()
        .map(|(index, event)| {
            assert_eq!(event.event_id, index as i64 + 1);
            match &event.kind {
                HistoryEventKind::WorkflowExecutionStartedV2 { .. } => "ExecutionStarted".into(),
                HistoryEventKind::WorkflowTaskScheduled {
                    attempt,
                    task_queue,
                    ..
                } => format!("Scheduled({attempt},{})", task_queue.0),
                HistoryEventKind::WorkflowTaskStarted {
                    scheduled_event_id,
                    attempt,
                    ..
                } => format!("Started({scheduled_event_id},{attempt})"),
                HistoryEventKind::WorkflowTaskCompleted {
                    scheduled_event_id,
                    started_event_id,
                    ..
                } => format!("Completed({scheduled_event_id},{started_event_id})"),
                HistoryEventKind::WorkflowTaskFailed {
                    scheduled_event_id,
                    started_event_id,
                    ..
                } => format!("Failed({scheduled_event_id},{started_event_id})"),
                HistoryEventKind::WorkflowTaskTimedOut {
                    scheduled_event_id,
                    started_event_id,
                    timeout_type,
                    ..
                } => format!("TimedOut({scheduled_event_id},{started_event_id},{timeout_type:?})"),
                HistoryEventKind::WorkflowExecutionPaused { .. } => "Paused".into(),
                HistoryEventKind::WorkflowExecutionUnpaused { .. } => "Unpaused".into(),
                HistoryEventKind::WorkflowExecutionSignaled { signal_name, .. } => {
                    format!("Signal({signal_name})")
                }
                other => panic!("unexpected history event {other:?}"),
            }
        })
        .collect()
}

#[derive(Clone, Copy)]
enum Problem {
    Failure,
    ScheduleToStart,
    StartToClose,
}

async fn history_case(problem: Problem, sticky: bool, signal: bool) {
    let f = Fixture::new().await;
    let first = f.poll_and_start(1).await;
    let mut expected: Vec<String> = [
        "ExecutionStarted",
        "Scheduled(1,workflow-q)",
        "Started(2,1)",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    if sticky {
        let mut request = completion(&first, !matches!(problem, Problem::ScheduleToStart));
        request.now = at(2);
        request.request = context("sticky-complete", 2);
        request.force_new_workflow_task = true;
        request.sticky = Some(StickySpec {
            queue: TaskQueueName("cache".into()),
            schedule_to_start_timeout: Duration::seconds(5),
        });
        f.runtime
            .complete_workflow_task_with_result(request)
            .await
            .unwrap();
        expected.extend(
            ["Completed(2,3)", "Scheduled(1,cache)"]
                .into_iter()
                .map(str::to_owned),
        );
        if !matches!(problem, Problem::ScheduleToStart) {
            expected.push("Started(5,1)".into());
        }
    }
    f.pause(3).await;
    if signal {
        f.signal(4).await;
    }
    let pending = f.state().await.pending_workflow_task.unwrap();
    let scheduled = pending.scheduled_event_id;
    let started = pending.started_event_id.unwrap_or(0);
    let command = match problem {
        Problem::Failure => Command::WorkflowTaskFailed(WorkflowTaskFailedRequest {
            logical_seq: pending.logical_seq,
            started_event_id: started,
            failure_cause: WorkflowTaskFailedCause::NonDeterminismError,
            failure_details: None,
            worker_identity: WorkerIdentity("worker".into()),
            request: context("fail", 12),
            now: at(12),
            reset_reapply: Vec::new(),
            history_size_bytes: 0,
            advice_policy: ContinueAsNewAdvicePolicy::V1_31_0,
            terminate_reason: None,
        }),
        Problem::ScheduleToStart | Problem::StartToClose => {
            Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest {
                logical_seq: pending.logical_seq,
                started_event_id: started,
                timeout_type: if matches!(problem, Problem::ScheduleToStart) {
                    WorkflowTaskTimeoutType::ScheduleToStart
                } else {
                    WorkflowTaskTimeoutType::StartToClose
                },
                now: at(12),
            })
        }
    };
    if matches!(problem, Problem::ScheduleToStart) {
        expected.push("Paused".into());
    }
    expected.push(match problem {
        Problem::Failure => format!("Failed({scheduled},{started})"),
        Problem::ScheduleToStart => format!("TimedOut({scheduled},0,ScheduleToStart)"),
        Problem::StartToClose => format!("TimedOut({scheduled},{started},StartToClose)"),
    });
    if !matches!(problem, Problem::ScheduleToStart) {
        expected.push("Paused".into());
    }
    if signal {
        expected.push("Signal(buffered)".into());
    }
    f.apply(command).await;
    let paused = f.state().await;
    assert_eq!(history_shape(&f.history().await), expected);
    f.unpause(20).await;
    expected.push("Unpaused".into());
    let next_scheduled = expected.len() as i64 + 1;
    let attempt = if sticky { 1 } else { 2 };
    if attempt == 1 {
        expected.push("Scheduled(1,workflow-q)".into());
    }
    assert_eq!(
        history_shape(&f.history().await),
        expected,
        "history immediately after unpause"
    );
    let task = f.poll_and_start(21).await;
    if attempt == 1 {
        expected.push(format!("Started({next_scheduled},1)"));
    }
    assert_eq!(
        history_shape(&f.history().await),
        expected,
        "history after the resumed start"
    );
    assert_eq!(task.token.attempt, attempt);
    assert_eq!(task.scheduled_time, at(20));
    assert!(
        paused.pending_workflow_task.is_none(),
        "failure/timeout leaves no pending task while paused"
    );
    assert_eq!(paused.workflow_task_attempt, attempt);
    assert_eq!(
        paused.workflow_task_attempts_since_last_success,
        u32::from(!sticky)
    );
    assert!(paused.sticky.is_none());
    if attempt > 1 {
        f.complete(&task, 22).await;
        expected.push("Scheduled(2,workflow-q)".into());
        expected.push(format!("Started({next_scheduled},2)"));
        expected.push(format!(
            "Completed({next_scheduled},{})",
            next_scheduled + 1
        ));
        let history = f.history().await;
        assert_eq!(history_shape(&history), expected);
        assert_eq!(history[(next_scheduled - 1) as usize].happened_at, at(20));
        assert_eq!(history[next_scheduled as usize].happened_at, at(21));
    }
    stop(&f.runtime).await;
}

#[tokio::test]
async fn paused_history_failure_normal() {
    history_case(Problem::Failure, false, false).await;
}
#[tokio::test]
async fn paused_history_failure_sticky() {
    history_case(Problem::Failure, true, false).await;
}
#[tokio::test]
async fn paused_history_failure_buffered() {
    history_case(Problem::Failure, false, true).await;
}
#[tokio::test]
async fn paused_history_schedule_to_start_sticky() {
    history_case(Problem::ScheduleToStart, true, false).await;
}
#[tokio::test]
async fn paused_history_start_to_close_buffered() {
    history_case(Problem::StartToClose, false, true).await;
}
#[tokio::test]
async fn paused_history_start_to_close_sticky() {
    history_case(Problem::StartToClose, true, false).await;
}

#[tokio::test]
async fn paused_history_control_unstarted_task_keeps_event_and_times() {
    let f = Fixture::new().await;
    let before = f.state().await.pending_workflow_task.unwrap();
    f.pause(3).await;
    f.unpause(20).await;
    let after = f.state().await.pending_workflow_task.unwrap();
    assert_eq!(after.scheduled_event_id, before.scheduled_event_id);
    assert_eq!(after.scheduled_at, before.scheduled_at);
    assert_eq!(after.original_scheduled_at, before.original_scheduled_at);
    assert_ne!(after.logical_seq, before.logical_seq);
    let task = f.poll_and_start(21).await;
    assert_eq!(task.token.attempt, 1);
    assert_eq!(
        history_shape(&f.history().await),
        [
            "ExecutionStarted",
            "Scheduled(1,workflow-q)",
            "Paused",
            "Unpaused",
            "Started(2,1)",
        ]
    );
    stop(&f.runtime).await;
}

#[tokio::test]
async fn paused_history_control_completion_leaves_no_task_until_unpause() {
    let f = Fixture::new().await;
    let first = f.poll_and_start(1).await;
    f.pause(3).await;
    f.complete(&first, 4).await;
    assert!(f.state().await.pending_workflow_task.is_none());
    f.unpause(20).await;
    let task = f.poll_and_start(21).await;
    assert_eq!(task.token.attempt, 1);
    assert_eq!(task.scheduled_time, at(20));
    assert_eq!(
        history_shape(&f.history().await),
        [
            "ExecutionStarted",
            "Scheduled(1,workflow-q)",
            "Started(2,1)",
            "Completed(2,3)",
            "Paused",
            "Unpaused",
            "Scheduled(1,workflow-q)",
            "Started(7,1)",
        ]
    );
    stop(&f.runtime).await;
}

#[tokio::test]
async fn paused_history_two_cycles_keep_transient_attempts() {
    let f = Fixture::new().await;
    let mut task = f.poll_and_start(1).await;
    for cycle in 0..2 {
        let base = cycle * 30;
        f.pause(base + 3).await;
        f.apply(Command::WorkflowTaskFailed(WorkflowTaskFailedRequest {
            logical_seq: task.token.logical_seq,
            started_event_id: task.token.started_event_id,
            failure_cause: WorkflowTaskFailedCause::NonDeterminismError,
            failure_details: None,
            worker_identity: WorkerIdentity("worker".into()),
            request: context(&format!("fail-cycle-{cycle}"), base + 4),
            now: at(base + 4),
            reset_reapply: Vec::new(),
            history_size_bytes: 0,
            advice_policy: ContinueAsNewAdvicePolicy::V1_31_0,
            terminate_reason: None,
        }))
        .await;
        assert!(f.state().await.pending_workflow_task.is_none());
        f.unpause(base + 20).await;
        task = f.poll_and_start(base + 21).await;
        assert_eq!(task.token.attempt, cycle as u32 + 2);
        assert_eq!(task.scheduled_time, at(base + 20));
    }
    f.complete(&task, 52).await;
    assert_eq!(
        history_shape(&f.history().await),
        [
            "ExecutionStarted",
            "Scheduled(1,workflow-q)",
            "Started(2,1)",
            "Failed(2,3)",
            "Paused",
            "Unpaused",
            "Paused",
            "Unpaused",
            "Scheduled(3,workflow-q)",
            "Started(9,3)",
            "Completed(9,10)",
        ]
    );
    stop(&f.runtime).await;
}
