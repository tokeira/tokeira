use std::collections::BTreeMap;

use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    event::HistoryEventKind, kernel::Kernel, ActivityResolvedRequest, ActivityState, BasicKernel,
    Command, DispatchOp, LoadedRun, PendingWorkflowTask, ProjectionOp, Reject, SignalRequest,
    StartRequest, StartWorkflowTaskRequest, TimerDueRequest, TimerState, WorkflowCommand,
    WorkflowState, WorkflowTaskCompletedRequest,
};
use tokeira_types::{
    ExecutionStatus, LogicalTaskSeq, Memo, NamespaceId, Payload, Payloads, RequestContext,
    RequestId, RetryPolicy, RunId, RunKey, SearchAttrValue, SearchAttributes, ShardEpoch,
    TaskQueueName, TransitionSeq, WorkerIdentity, WorkflowId, WorkflowTaskToken, WorkflowType,
};

fn now() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()
}

fn payload(data: &str) -> Payload {
    Payload::new(data.as_bytes().to_vec())
}

fn payloads(data: &str) -> Payloads {
    Payloads(vec![payload(data)])
}

fn memo() -> Memo {
    Memo(BTreeMap::from([("memo".into(), payload("memo-value"))]))
}

fn search_attributes() -> SearchAttributes {
    SearchAttributes(BTreeMap::from([(
        "keyword".into(),
        SearchAttrValue::Keyword("value".into()),
    )]))
}

fn retry_policy() -> RetryPolicy {
    RetryPolicy {
        initial_interval: Duration::seconds(1),
        backoff_coefficient: 2.0,
        maximum_interval: Some(Duration::seconds(10)),
        maximum_attempts: 5,
        non_retryable_error_types: vec!["fatal".into()],
    }
}

fn request_context(id: &str) -> RequestContext {
    RequestContext {
        request_id: RequestId(id.into()),
        caller_identity: Some("tester".into()),
        received_at: now(),
    }
}

fn make_start_request() -> StartRequest {
    let run_id = RunId::new();
    StartRequest {
        run_key: RunKey::new(),
        namespace_id: NamespaceId::new(),
        workflow_id: WorkflowId("workflow".into()),
        run_id,
        workflow_type: WorkflowType("wf".into()),
        task_queue: TaskQueueName("queue".into()),
        input: payloads("start-input"),
        memo: memo(),
        search_attributes: search_attributes(),
        workflow_execution_timeout: Some(Duration::minutes(10)),
        workflow_run_timeout: Some(Duration::minutes(5)),
        workflow_task_timeout: Duration::seconds(10),
        retry_policy: Some(retry_policy()),
        attempt: 1,
        continued_execution_run_id: None,
        first_execution_run_id: Some(run_id),
        request: request_context("start-req"),
        now: now(),
    }
}

fn make_open_state() -> WorkflowState {
    WorkflowState {
        run_key: RunKey::new(),
        namespace_id: NamespaceId::new(),
        workflow_id: WorkflowId("workflow".into()),
        run_id: RunId::new(),
        workflow_type: WorkflowType("wf".into()),
        task_queue: TaskQueueName("queue".into()),
        status: ExecutionStatus::Running,
        transition_seq: TransitionSeq(5),
        last_event_id: 9,
        next_workflow_task_seq: LogicalTaskSeq(4),
        pending_workflow_task: None,
        sticky: None,
        memo: memo(),
        search_attributes: search_attributes(),
        workflow_execution_timeout: Some(Duration::minutes(10)),
        workflow_run_timeout: Some(Duration::minutes(5)),
        workflow_task_timeout: Duration::seconds(10),
        retry_policy: Some(retry_policy()),
        attempt: 1,
        activities: BTreeMap::new(),
        timers: BTreeMap::new(),
        started_at: now() - Duration::minutes(3),
        closed_at: None,
    }
}

fn make_open_state_with_pending_wft() -> WorkflowState {
    let mut state = make_open_state();
    state.pending_workflow_task = Some(PendingWorkflowTask {
        logical_seq: LogicalTaskSeq(3),
        scheduled_event_id: 8,
        started_event_id: None,
        attempt: 0,
    });
    state
}

fn make_open_state_with_started_wft() -> WorkflowState {
    let mut state = make_open_state();
    state.pending_workflow_task = Some(PendingWorkflowTask {
        logical_seq: LogicalTaskSeq(3),
        scheduled_event_id: 8,
        started_event_id: Some(9),
        attempt: 1,
    });
    state
}

fn make_open_state_with_activity(id: &str) -> WorkflowState {
    let mut state = make_open_state();
    state.activities.insert(
        id.into(),
        ActivityState {
            activity_id: id.into(),
            schedule_event_id: 7,
            task_queue: TaskQueueName("activity-q".into()),
            attempt: 1,
            schedule_to_close_timeout: Some(Duration::minutes(2)),
            schedule_to_start_timeout: Some(Duration::seconds(30)),
            start_to_close_timeout: Some(Duration::minutes(1)),
            heartbeat_timeout: Some(Duration::seconds(20)),
        },
    );
    state
}

fn make_open_state_with_timer(id: &str) -> WorkflowState {
    let mut state = make_open_state();
    state.timers.insert(
        id.into(),
        TimerState {
            timer_id: id.into(),
            started_event_id: 7,
            fire_at: now(),
        },
    );
    state
}

fn make_closed_state() -> WorkflowState {
    let mut state = make_open_state();
    state.status = ExecutionStatus::Completed;
    state.closed_at = Some(now());
    state
}

fn kernel() -> BasicKernel {
    BasicKernel
}

#[test]
fn start_from_absent() {
    let req = make_start_request();
    let transition = kernel().apply(LoadedRun::Absent, Command::Start(req.clone())).unwrap();

    assert_eq!(transition.expected_seq, TransitionSeq::ZERO);
    assert_eq!(transition.next_state.status, ExecutionStatus::Running);
    assert_eq!(transition.next_state.transition_seq, TransitionSeq(1));
    assert_eq!(transition.next_state.last_event_id, 2);
    assert_eq!(transition.next_state.workflow_execution_timeout, req.workflow_execution_timeout);
    assert_eq!(transition.next_state.workflow_run_timeout, req.workflow_run_timeout);
    assert_eq!(transition.next_state.workflow_task_timeout, req.workflow_task_timeout);
    assert_eq!(transition.next_state.retry_policy, req.retry_policy);
    assert_eq!(transition.next_state.attempt, req.attempt);
    assert_eq!(transition.history_events.len(), 2);
    assert!(matches!(transition.history_events[0].kind, HistoryEventKind::WorkflowExecutionStarted { .. }));
    assert!(matches!(transition.history_events[1].kind, HistoryEventKind::WorkflowTaskScheduled { .. }));
    assert_eq!(transition.request_dedupe_ops.len(), 1);
    assert_eq!(transition.projection_ops.len(), 1);
    assert_eq!(transition.projection_ops[0], ProjectionOp::UpsertExecution {
        status: ExecutionStatus::Running,
        memo_patch: req.memo,
        search_attr_patch: req.search_attributes,
    });
    assert_eq!(transition.dispatch_ops.len(), 1);
    assert!(transition.next_state.pending_workflow_task.is_some());
}

#[test]
fn signal_with_no_pending_wft() {
    let state = make_open_state();
    let transition = kernel().apply(
        LoadedRun::Existing(state),
        Command::Signal(SignalRequest {
            signal_name: "sig".into(),
            input: payloads("signal"),
            request: request_context("signal-req"),
            now: now(),
        }),
    ).unwrap();

    assert!(matches!(transition.history_events[0].kind, HistoryEventKind::WorkflowExecutionSignaled { .. }));
    assert!(matches!(transition.history_events[1].kind, HistoryEventKind::WorkflowTaskScheduled { .. }));
    assert_eq!(transition.request_dedupe_ops.len(), 1);
    assert!(transition.dispatch_ops.iter().any(|op| matches!(op, DispatchOp::EnqueueWorkflowTask { .. })));
    assert!(transition.next_state.pending_workflow_task.is_some());
}

#[test]
fn signal_with_pending_wft() {
    let state = make_open_state_with_pending_wft();
    let pending = state.pending_workflow_task.clone().unwrap();
    let transition = kernel().apply(
        LoadedRun::Existing(state),
        Command::Signal(SignalRequest {
            signal_name: "sig".into(),
            input: payloads("signal"),
            request: request_context("signal-req"),
            now: now(),
        }),
    ).unwrap();

    assert_eq!(transition.history_events.len(), 1);
    assert!(matches!(transition.history_events[0].kind, HistoryEventKind::WorkflowExecutionSignaled { .. }));
    assert_eq!(transition.request_dedupe_ops.len(), 1);
    assert!(transition.dispatch_ops.is_empty());
    assert_eq!(transition.next_state.pending_workflow_task.unwrap().logical_seq, pending.logical_seq);
}

#[test]
fn workflow_task_started_with_sticky() {
    let state = make_open_state_with_pending_wft();
    let transition = kernel().apply(
        LoadedRun::Existing(state),
        Command::WorkflowTaskStarted(StartWorkflowTaskRequest {
            logical_seq: LogicalTaskSeq(3),
            worker_identity: WorkerIdentity("worker-a".into()),
            sticky_ttl: Some(Duration::seconds(30)),
            now: now(),
        }),
    ).unwrap();

    assert!(matches!(transition.history_events[0].kind, HistoryEventKind::WorkflowTaskStarted { attempt: 1, .. }));
    let pending = transition.next_state.pending_workflow_task.unwrap();
    assert_eq!(pending.started_event_id, Some(10));
    assert_eq!(pending.attempt, 1);
    let sticky = transition.next_state.sticky.unwrap();
    assert_eq!(sticky.worker_identity, WorkerIdentity("worker-a".into()));
    assert_eq!(sticky.expires_at, now() + Duration::seconds(30));
}

#[test]
fn workflow_task_completed_with_activity_and_timer() {
    let state = make_open_state_with_started_wft();
    let transition = kernel().apply(
        LoadedRun::Existing(state.clone()),
        Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
            token: WorkflowTaskToken {
                run_key: state.run_key,
                logical_seq: LogicalTaskSeq(3),
                started_event_id: 9,
                attempt: 1,
                shard_epoch: ShardEpoch::ZERO,
            },
            identity: WorkerIdentity("worker".into()),
            commands: vec![
                WorkflowCommand::ScheduleActivity {
                    activity_id: "activity-1".into(),
                    task_queue: TaskQueueName("activity-q".into()),
                    input: payloads("act"),
                    schedule_to_close_timeout: Some(Duration::minutes(2)),
                    schedule_to_start_timeout: Some(Duration::seconds(30)),
                    start_to_close_timeout: Some(Duration::minutes(1)),
                    heartbeat_timeout: Some(Duration::seconds(20)),
                },
                WorkflowCommand::StartTimer {
                    timer_id: "timer-1".into(),
                    fire_at: now() + Duration::seconds(45),
                },
            ],
            force_new_workflow_task: false,
            now: now(),
        }),
    ).unwrap();

    assert!(matches!(transition.history_events[0].kind, HistoryEventKind::WorkflowTaskCompleted { .. }));
    assert!(transition.history_events.iter().any(|event| matches!(event.kind, HistoryEventKind::ActivityTaskScheduled { .. })));
    assert!(transition.history_events.iter().any(|event| matches!(event.kind, HistoryEventKind::TimerStarted { .. })));
    assert!(transition.activity_ops.iter().any(|op| matches!(op, tokeira_kernel::ActivityOp::Upsert(_))));
    assert!(transition.timer_ops.iter().any(|op| matches!(op, tokeira_kernel::TimerOp::Upsert(_))));
    assert!(transition.dispatch_ops.iter().any(|op| matches!(op, DispatchOp::EnqueueActivityTask { .. })));
    assert!(transition.next_state.activities.contains_key("activity-1"));
    assert!(transition.next_state.timers.contains_key("timer-1"));
    assert!(transition.next_state.pending_workflow_task.is_none());
}

#[test]
fn workflow_task_completed_with_complete_workflow() {
    let state = make_open_state_with_started_wft();
    let transition = kernel().apply(
        LoadedRun::Existing(state.clone()),
        Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
            token: WorkflowTaskToken {
                run_key: state.run_key,
                logical_seq: LogicalTaskSeq(3),
                started_event_id: 9,
                attempt: 1,
                shard_epoch: ShardEpoch::ZERO,
            },
            identity: WorkerIdentity("worker".into()),
            commands: vec![WorkflowCommand::CompleteWorkflow { result: payloads("done") }],
            force_new_workflow_task: false,
            now: now(),
        }),
    ).unwrap();

    assert!(transition.history_events.iter().any(|event| matches!(event.kind, HistoryEventKind::WorkflowExecutionCompleted { .. })));
    assert_eq!(transition.projection_ops.last(), Some(&ProjectionOp::CloseExecution { status: ExecutionStatus::Completed, closed_at: now() }));
    assert_eq!(transition.next_state.status, ExecutionStatus::Completed);
    assert!(transition.next_state.closed_at.is_some());
    assert!(transition.next_state.pending_workflow_task.is_none());
    assert!(transition.next_state.sticky.is_none());
}

#[test]
fn workflow_task_completed_with_fail_workflow() {
    let state = make_open_state_with_started_wft();
    let transition = kernel().apply(
        LoadedRun::Existing(state.clone()),
        Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
            token: WorkflowTaskToken {
                run_key: state.run_key,
                logical_seq: LogicalTaskSeq(3),
                started_event_id: 9,
                attempt: 1,
                shard_epoch: ShardEpoch::ZERO,
            },
            identity: WorkerIdentity("worker".into()),
            commands: vec![WorkflowCommand::FailWorkflow { message: "nope".into(), details: Some(payload("details")) }],
            force_new_workflow_task: false,
            now: now(),
        }),
    ).unwrap();

    assert!(transition.history_events.iter().any(|event| matches!(event.kind, HistoryEventKind::WorkflowExecutionFailed { .. })));
    assert_eq!(transition.projection_ops.last(), Some(&ProjectionOp::CloseExecution { status: ExecutionStatus::Failed, closed_at: now() }));
    assert_eq!(transition.next_state.status, ExecutionStatus::Failed);
    assert!(transition.next_state.closed_at.is_some());
    assert!(transition.next_state.pending_workflow_task.is_none());
    assert!(transition.next_state.sticky.is_none());
}

#[test]
fn activity_resolved_completed_schedules_wft() {
    let state = make_open_state_with_activity("activity-1");
    let transition = kernel().apply(
        LoadedRun::Existing(state),
        Command::ActivityResolved(ActivityResolvedRequest {
            activity_id: "activity-1".into(),
            resolution: tokeira_kernel::ActivityResolution::Completed { result: payloads("done") },
            worker_identity: None,
            now: now(),
        }),
    ).unwrap();

    assert!(transition.history_events.iter().any(|event| matches!(event.kind, HistoryEventKind::ActivityTaskCompleted { .. })));
    assert!(transition.activity_ops.iter().any(|op| matches!(op, tokeira_kernel::ActivityOp::Delete { activity_id } if activity_id == "activity-1")));
    assert!(transition.history_events.iter().any(|event| matches!(event.kind, HistoryEventKind::WorkflowTaskScheduled { .. })));
    assert!(transition.dispatch_ops.iter().any(|op| matches!(op, DispatchOp::EnqueueWorkflowTask { .. })));
    assert!(!transition.next_state.activities.contains_key("activity-1"));
    assert!(transition.next_state.pending_workflow_task.is_some());
}

#[test]
fn activity_resolved_timed_out_schedules_wft() {
    let state = make_open_state_with_activity("activity-1");
    let transition = kernel().apply(
        LoadedRun::Existing(state),
        Command::ActivityResolved(ActivityResolvedRequest {
            activity_id: "activity-1".into(),
            resolution: tokeira_kernel::ActivityResolution::TimedOut { timeout_type: "HEARTBEAT".into() },
            worker_identity: None,
            now: now(),
        }),
    ).unwrap();

    assert!(transition.history_events.iter().any(|event| matches!(event.kind, HistoryEventKind::ActivityTaskTimedOut { .. })));
    assert!(transition.next_state.pending_workflow_task.is_some());
}

#[test]
fn activity_resolved_canceled_schedules_wft() {
    let state = make_open_state_with_activity("activity-1");
    let transition = kernel().apply(
        LoadedRun::Existing(state),
        Command::ActivityResolved(ActivityResolvedRequest {
            activity_id: "activity-1".into(),
            resolution: tokeira_kernel::ActivityResolution::Canceled { details: Some(payloads("cancel")) },
            worker_identity: None,
            now: now(),
        }),
    ).unwrap();

    assert!(transition.history_events.iter().any(|event| matches!(event.kind, HistoryEventKind::ActivityTaskCanceled { .. })));
    assert!(transition.next_state.pending_workflow_task.is_some());
}

#[test]
fn timer_due_schedules_wft() {
    let state = make_open_state_with_timer("timer-1");
    let transition = kernel().apply(
        LoadedRun::Existing(state),
        Command::TimerDue(TimerDueRequest {
            timer_id: "timer-1".into(),
            fired_at: now(),
        }),
    ).unwrap();

    assert!(transition.history_events.iter().any(|event| matches!(event.kind, HistoryEventKind::TimerFired { .. })));
    assert!(transition.dispatch_ops.iter().any(|op| matches!(op, DispatchOp::EnqueueWorkflowTask { .. })));
    assert!(!transition.next_state.timers.contains_key("timer-1"));
    assert!(transition.next_state.pending_workflow_task.is_some());
}

#[test]
fn reject_start_on_existing_run() {
    assert_eq!(
        kernel().apply(LoadedRun::Existing(make_open_state()), Command::Start(make_start_request())),
        Err(Reject::RunAlreadyExists)
    );
}

#[test]
fn reject_signal_on_absent_run() {
    assert_eq!(
        kernel().apply(LoadedRun::Absent, Command::Signal(SignalRequest {
            signal_name: "sig".into(),
            input: payloads("signal"),
            request: request_context("signal"),
            now: now(),
        })),
        Err(Reject::MissingRun)
    );
}

#[test]
fn reject_signal_on_closed_run() {
    assert_eq!(
        kernel().apply(LoadedRun::Existing(make_closed_state()), Command::Signal(SignalRequest {
            signal_name: "sig".into(),
            input: payloads("signal"),
            request: request_context("signal"),
            now: now(),
        })),
        Err(Reject::RunClosed(ExecutionStatus::Completed))
    );
}

#[test]
fn reject_wft_started_no_pending() {
    assert_eq!(
        kernel().apply(LoadedRun::Existing(make_open_state()), Command::WorkflowTaskStarted(StartWorkflowTaskRequest {
            logical_seq: LogicalTaskSeq(3),
            worker_identity: WorkerIdentity("worker".into()),
            sticky_ttl: None,
            now: now(),
        })),
        Err(Reject::NoPendingWorkflowTask)
    );
}

#[test]
fn reject_wft_started_seq_mismatch() {
    assert_eq!(
        kernel().apply(LoadedRun::Existing(make_open_state_with_pending_wft()), Command::WorkflowTaskStarted(StartWorkflowTaskRequest {
            logical_seq: LogicalTaskSeq(4),
            worker_identity: WorkerIdentity("worker".into()),
            sticky_ttl: None,
            now: now(),
        })),
        Err(Reject::WorkflowTaskSeqMismatch { expected: 3, got: 4 })
    );
}

#[test]
fn reject_wft_started_already_started() {
    let mut started = make_open_state_with_pending_wft();
    started.pending_workflow_task.as_mut().unwrap().started_event_id = Some(9);
    assert_eq!(
        kernel().apply(LoadedRun::Existing(started), Command::WorkflowTaskStarted(StartWorkflowTaskRequest {
            logical_seq: LogicalTaskSeq(3),
            worker_identity: WorkerIdentity("worker".into()),
            sticky_ttl: None,
            now: now(),
        })),
        Err(Reject::WorkflowTaskAlreadyStarted { logical_seq: 3 })
    );
}

#[test]
fn reject_wft_completed_no_pending() {
    assert_eq!(
        kernel().apply(LoadedRun::Existing(make_open_state()), Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
            token: WorkflowTaskToken { run_key: RunKey::new(), logical_seq: LogicalTaskSeq(3), started_event_id: 9, attempt: 1, shard_epoch: ShardEpoch::ZERO },
            identity: WorkerIdentity("worker".into()),
            commands: vec![],
            force_new_workflow_task: false,
            now: now(),
        })),
        Err(Reject::NoPendingWorkflowTask)
    );
}

#[test]
fn reject_wft_completed_not_started() {
    assert_eq!(
        kernel().apply(LoadedRun::Existing(make_open_state_with_pending_wft()), Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
            token: WorkflowTaskToken { run_key: RunKey::new(), logical_seq: LogicalTaskSeq(3), started_event_id: 9, attempt: 1, shard_epoch: ShardEpoch::ZERO },
            identity: WorkerIdentity("worker".into()),
            commands: vec![],
            force_new_workflow_task: false,
            now: now(),
        })),
        Err(Reject::WorkflowTaskNotStarted { logical_seq: 3 })
    );
}

#[test]
fn reject_wft_completed_seq_mismatch() {
    let state = make_open_state_with_started_wft();
    assert_eq!(
        kernel().apply(LoadedRun::Existing(state.clone()), Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
            token: WorkflowTaskToken { run_key: state.run_key, logical_seq: LogicalTaskSeq(4), started_event_id: 9, attempt: 1, shard_epoch: ShardEpoch::ZERO },
            identity: WorkerIdentity("worker".into()),
            commands: vec![],
            force_new_workflow_task: false,
            now: now(),
        })),
        Err(Reject::WorkflowTaskSeqMismatch { expected: 3, got: 4 })
    );
}

#[test]
fn reject_wft_completed_token_mismatch() {
    let state = make_open_state_with_started_wft();
    assert_eq!(
        kernel().apply(LoadedRun::Existing(state.clone()), Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
            token: WorkflowTaskToken { run_key: state.run_key, logical_seq: LogicalTaskSeq(3), started_event_id: 10, attempt: 2, shard_epoch: ShardEpoch::ZERO },
            identity: WorkerIdentity("worker".into()),
            commands: vec![],
            force_new_workflow_task: false,
            now: now(),
        })),
        Err(Reject::WorkflowTaskTokenMismatch)
    );
}

#[test]
fn reject_duplicate_activity_id() {
    let state = make_open_state_with_started_wft();
    let mut with_activity = state.clone();
    with_activity.activities.insert("dup".into(), ActivityState {
        activity_id: "dup".into(),
        schedule_event_id: 1,
        task_queue: TaskQueueName("activity-q".into()),
        attempt: 1,
        schedule_to_close_timeout: None,
        schedule_to_start_timeout: None,
        start_to_close_timeout: None,
        heartbeat_timeout: None,
    });
    assert_eq!(
        kernel().apply(LoadedRun::Existing(with_activity), Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
            token: WorkflowTaskToken { run_key: state.run_key, logical_seq: LogicalTaskSeq(3), started_event_id: 9, attempt: 1, shard_epoch: ShardEpoch::ZERO },
            identity: WorkerIdentity("worker".into()),
            commands: vec![WorkflowCommand::ScheduleActivity {
                activity_id: "dup".into(),
                task_queue: TaskQueueName("activity-q".into()),
                input: payloads("a"),
                schedule_to_close_timeout: None,
                schedule_to_start_timeout: None,
                start_to_close_timeout: None,
                heartbeat_timeout: None,
            }],
            force_new_workflow_task: false,
            now: now(),
        })),
        Err(Reject::DuplicateActivityId("dup".into()))
    );
}

#[test]
fn reject_duplicate_timer_id() {
    let state = make_open_state_with_started_wft();
    let mut with_timer = state.clone();
    with_timer.timers.insert("dup".into(), TimerState { timer_id: "dup".into(), started_event_id: 1, fire_at: now() });
    assert_eq!(
        kernel().apply(LoadedRun::Existing(with_timer), Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
            token: WorkflowTaskToken { run_key: state.run_key, logical_seq: LogicalTaskSeq(3), started_event_id: 9, attempt: 1, shard_epoch: ShardEpoch::ZERO },
            identity: WorkerIdentity("worker".into()),
            commands: vec![WorkflowCommand::StartTimer { timer_id: "dup".into(), fire_at: now() }],
            force_new_workflow_task: false,
            now: now(),
        })),
        Err(Reject::DuplicateTimerId("dup".into()))
    );
}

#[test]
fn reject_unknown_activity() {
    assert_eq!(
        kernel().apply(LoadedRun::Existing(make_open_state()), Command::ActivityResolved(ActivityResolvedRequest {
            activity_id: "missing".into(),
            resolution: tokeira_kernel::ActivityResolution::Completed { result: payloads("done") },
            worker_identity: None,
            now: now(),
        })),
        Err(Reject::UnknownActivity("missing".into()))
    );
}

#[test]
fn reject_unknown_timer() {
    assert_eq!(
        kernel().apply(LoadedRun::Existing(make_open_state()), Command::TimerDue(TimerDueRequest {
            timer_id: "missing".into(),
            fired_at: now(),
        })),
        Err(Reject::UnknownTimer("missing".into()))
    );
}

#[test]
fn reject_commands_after_close() {
    let state = make_open_state_with_started_wft();
    assert_eq!(
        kernel().apply(LoadedRun::Existing(state.clone()), Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
            token: WorkflowTaskToken { run_key: state.run_key, logical_seq: LogicalTaskSeq(3), started_event_id: 9, attempt: 1, shard_epoch: ShardEpoch::ZERO },
            identity: WorkerIdentity("worker".into()),
            commands: vec![
                WorkflowCommand::CompleteWorkflow { result: payloads("done") },
                WorkflowCommand::RequestNewWorkflowTask,
            ],
            force_new_workflow_task: false,
            now: now(),
        })),
        Err(Reject::CommandsAfterClose { index: 1 })
    );
}
