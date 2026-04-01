use std::collections::BTreeMap;

use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    event::HistoryEventKind, kernel::Kernel, ActivityResolvedRequest, ActivityState, BasicKernel,
    CancelRequest, Command, DispatchOp, ExternalWorkflowExecution, LoadedRun, PendingWorkflowTask,
    ProjectionOp, Reject, SignalRequest, StartRequest, StartWorkflowTaskRequest,
    TerminateRequest, TimerDueRequest, TimerState, WorkflowCommand, WorkflowExecutionTimedOutRequest,
    WorkflowState, WorkflowTaskCompletedRequest, WorkflowTaskFailedCause,
    WorkflowTaskFailedRequest, WorkflowTaskTimedOutRequest, WorkflowTaskTimeoutType,
    WorkflowTimeoutType, RetryState,
};
use tokeira_types::{
    ExecutionStatus, LogicalTaskSeq, Memo, NamespaceId, Payload, Payloads, RequestContext,
    RequestId, RetryPolicy, RunId, RunKey, SearchAttrValue, SearchAttributes, ShardEpoch,
    StickyAffinity, TaskQueueName, TransitionSeq, WorkerIdentity, WorkflowId, WorkflowTaskToken,
    WorkflowType,
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

fn make_open_state_with_started_wft_and_sticky() -> WorkflowState {
    let mut state = make_open_state_with_started_wft();
    state.sticky = Some(StickyAffinity {
        worker_identity: WorkerIdentity("sticky-worker".into()),
        expires_at: now() + Duration::seconds(30),
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

fn external_workflow_execution() -> ExternalWorkflowExecution {
    ExternalWorkflowExecution {
        namespace_id: NamespaceId::new(),
        workflow_id: WorkflowId("parent".into()),
        run_id: RunId::new(),
    }
}

fn make_cancel_request() -> CancelRequest {
    CancelRequest {
        reason: "cancel requested".into(),
        external_initiator: None,
        request: request_context("cancel-req"),
        now: now(),
    }
}

fn make_terminate_request() -> TerminateRequest {
    TerminateRequest {
        reason: "terminated".into(),
        details: Some(payloads("term-details")),
        identity: "operator".into(),
        request: request_context("terminate-req"),
        now: now(),
    }
}

fn make_timeout_request() -> WorkflowExecutionTimedOutRequest {
    WorkflowExecutionTimedOutRequest {
        timeout_type: WorkflowTimeoutType::RunTimeout,
        retry_state: RetryState::Timeout,
        now: now(),
    }
}

fn make_continue_as_new_command() -> WorkflowCommand {
    WorkflowCommand::ContinueAsNew {
        new_run_id: RunId::new(),
        workflow_type: WorkflowType("wf-next".into()),
        task_queue: TaskQueueName("queue-next".into()),
        input: payloads("continue-input"),
        memo: memo(),
        search_attributes: search_attributes(),
        workflow_execution_timeout: Some(Duration::minutes(12)),
        workflow_run_timeout: Some(Duration::minutes(6)),
        workflow_task_timeout: Duration::seconds(11),
    }
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
fn cancel_with_no_pending_wft() {
    let state = make_open_state();
    let req = make_cancel_request();
    let transition = kernel()
        .apply(LoadedRun::Existing(state), Command::Cancel(req.clone()))
        .unwrap();

    assert_eq!(transition.history_events.len(), 2);
    assert!(matches!(
        &transition.history_events[0].kind,
        HistoryEventKind::WorkflowExecutionCancelRequested {
            reason,
            external_workflow_execution,
            request_id,
        } if reason == &req.reason
            && external_workflow_execution == &None
            && request_id == "cancel-req"
    ));
    assert!(matches!(
        transition.history_events[1].kind,
        HistoryEventKind::WorkflowTaskScheduled { .. }
    ));
    assert_eq!(transition.request_dedupe_ops.len(), 1);
    assert_eq!(transition.activity_ops.len(), 0);
    assert_eq!(transition.timer_ops.len(), 0);
    assert_eq!(transition.projection_ops.len(), 0);
    assert_eq!(transition.dispatch_ops.len(), 1);
    assert!(transition.next_state.pending_workflow_task.is_some());
    assert_eq!(transition.next_state.status, ExecutionStatus::Running);
}

#[test]
fn cancel_with_pending_wft() {
    let state = make_open_state_with_pending_wft();
    let pending = state.pending_workflow_task.clone();
    let transition = kernel()
        .apply(LoadedRun::Existing(state), Command::Cancel(make_cancel_request()))
        .unwrap();

    assert_eq!(transition.history_events.len(), 1);
    assert!(matches!(
        transition.history_events[0].kind,
        HistoryEventKind::WorkflowExecutionCancelRequested { .. }
    ));
    assert_eq!(transition.request_dedupe_ops.len(), 1);
    assert!(transition.dispatch_ops.is_empty());
    assert_eq!(transition.activity_ops.len(), 0);
    assert_eq!(transition.timer_ops.len(), 0);
    assert_eq!(transition.projection_ops.len(), 0);
    assert_eq!(transition.next_state.pending_workflow_task, pending);
}

#[test]
fn cancel_with_external_initiator() {
    let state = make_open_state();
    let mut req = make_cancel_request();
    req.external_initiator = Some(external_workflow_execution());

    let transition = kernel()
        .apply(LoadedRun::Existing(state), Command::Cancel(req.clone()))
        .unwrap();

    assert!(matches!(
        &transition.history_events[0].kind,
        HistoryEventKind::WorkflowExecutionCancelRequested {
            external_workflow_execution,
            ..
        } if *external_workflow_execution == req.external_initiator
    ));
}

#[test]
fn terminate_no_open_entities() {
    let state = make_open_state();
    let req = make_terminate_request();
    let transition = kernel()
        .apply(LoadedRun::Existing(state), Command::Terminate(req.clone()))
        .unwrap();

    assert_eq!(transition.history_events.len(), 1);
    assert!(matches!(
        &transition.history_events[0].kind,
        HistoryEventKind::WorkflowExecutionTerminated { reason, details, identity }
        if reason == &req.reason && details == &req.details && identity == &req.identity
    ));
    assert_eq!(transition.next_state.status, ExecutionStatus::Terminated);
    assert!(transition.next_state.closed_at.is_some());
    assert!(transition.next_state.pending_workflow_task.is_none());
    assert!(transition.next_state.sticky.is_none());
    assert!(transition.next_state.activities.is_empty());
    assert!(transition.next_state.timers.is_empty());
    assert_eq!(transition.request_dedupe_ops.len(), 1);
    assert_eq!(
        transition.projection_ops.last(),
        Some(&ProjectionOp::CloseExecution {
            status: ExecutionStatus::Terminated,
            closed_at: now(),
        })
    );
    assert!(transition.dispatch_ops.is_empty());
    assert!(transition.activity_ops.is_empty());
    assert!(transition.timer_ops.is_empty());
}

#[test]
fn terminate_with_activities_and_timers() {
    let mut state = make_open_state_with_activity("activity-1");
    state.activities.insert(
        "activity-2".into(),
        ActivityState {
            activity_id: "activity-2".into(),
            schedule_event_id: 6,
            task_queue: TaskQueueName("activity-q".into()),
            attempt: 1,
            schedule_to_close_timeout: Some(Duration::minutes(2)),
            schedule_to_start_timeout: Some(Duration::seconds(30)),
            start_to_close_timeout: Some(Duration::minutes(1)),
            heartbeat_timeout: Some(Duration::seconds(20)),
        },
    );
    state.timers.insert(
        "timer-1".into(),
        TimerState {
            timer_id: "timer-1".into(),
            started_event_id: 7,
            fire_at: now(),
        },
    );
    let transition = kernel()
        .apply(LoadedRun::Existing(state), Command::Terminate(make_terminate_request()))
        .unwrap();

    assert!(transition.next_state.activities.is_empty());
    assert!(transition.next_state.timers.is_empty());
    assert_eq!(transition.activity_ops.len(), 2);
    assert_eq!(transition.timer_ops.len(), 1);
    assert!(matches!(
        transition.activity_ops[0],
        tokeira_kernel::ActivityOp::Delete { .. }
    ));
    assert!(matches!(transition.timer_ops[0], tokeira_kernel::TimerOp::Delete { .. }));
}

#[test]
fn terminate_with_pending_wft() {
    let state = make_open_state_with_pending_wft();
    let transition = kernel()
        .apply(LoadedRun::Existing(state), Command::Terminate(make_terminate_request()))
        .unwrap();

    assert!(transition.next_state.pending_workflow_task.is_none());
    assert!(transition.dispatch_ops.is_empty());
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

    assert!(transition.history_events.iter().any(|event| matches!(
        event.kind,
        HistoryEventKind::WorkflowExecutionFailed {
            retry_state: RetryState::InProgress,
            attempt: 1,
            ..
        }
    )));
    assert_eq!(transition.projection_ops.last(), Some(&ProjectionOp::CloseExecution { status: ExecutionStatus::Failed, closed_at: now() }));
    assert_eq!(transition.next_state.status, ExecutionStatus::Failed);
    assert!(transition.next_state.closed_at.is_some());
    assert!(transition.next_state.pending_workflow_task.is_none());
    assert!(transition.next_state.sticky.is_none());
}

#[test]
fn continue_as_new_closes_run() {
    let state = make_open_state_with_started_wft_and_sticky();
    let command = make_continue_as_new_command();
    let (
        expected_new_run_id,
        expected_workflow_type,
        expected_task_queue,
        expected_input,
        expected_memo,
        expected_search_attributes,
        expected_execution_timeout,
        expected_run_timeout,
        expected_task_timeout,
    ) = match &command {
        WorkflowCommand::ContinueAsNew {
            new_run_id,
            workflow_type,
            task_queue,
            input,
            memo,
            search_attributes,
            workflow_execution_timeout,
            workflow_run_timeout,
            workflow_task_timeout,
        } => (
            new_run_id.clone(),
            workflow_type.clone(),
            task_queue.clone(),
            input.clone(),
            memo.clone(),
            search_attributes.clone(),
            *workflow_execution_timeout,
            *workflow_run_timeout,
            *workflow_task_timeout,
        ),
        _ => unreachable!(),
    };
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
            commands: vec![command.clone()],
            force_new_workflow_task: false,
            now: now(),
        }),
    ).unwrap();

    assert_eq!(transition.next_state.status, ExecutionStatus::ContinuedAsNew);
    assert!(transition.next_state.closed_at.is_some());
    assert!(transition.next_state.pending_workflow_task.is_none());
    assert!(transition.next_state.sticky.is_none());
    assert!(transition.dispatch_ops.is_empty());
    assert!(matches!(
        &transition.history_events[1].kind,
        HistoryEventKind::WorkflowExecutionContinuedAsNew {
            new_run_id,
            workflow_type,
            task_queue,
            input,
            memo,
            search_attributes,
            workflow_execution_timeout,
            workflow_run_timeout,
            workflow_task_timeout,
        } if *new_run_id == expected_new_run_id
            && *workflow_type == expected_workflow_type
            && *task_queue == expected_task_queue
            && *input == expected_input
            && *memo == expected_memo
            && *search_attributes == expected_search_attributes
            && *workflow_execution_timeout == expected_execution_timeout
            && *workflow_run_timeout == expected_run_timeout
            && *workflow_task_timeout == expected_task_timeout
    ));
}

#[test]
fn continue_as_new_then_another_command() {
    let state = make_open_state_with_started_wft();
    assert_eq!(
        kernel().apply(
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
                    make_continue_as_new_command(),
                    WorkflowCommand::RequestNewWorkflowTask,
                ],
                force_new_workflow_task: false,
                now: now(),
            }),
        ),
        Err(Reject::CommandsAfterClose { index: 1 })
    );
}

#[test]
fn workflow_execution_timed_out_no_entities() {
    let transition = kernel().apply(
        LoadedRun::Existing(make_open_state()),
        Command::WorkflowExecutionTimedOut(make_timeout_request()),
    ).unwrap();

    assert_eq!(transition.next_state.status, ExecutionStatus::TimedOut);
    assert!(transition.next_state.closed_at.is_some());
    assert!(transition.next_state.pending_workflow_task.is_none());
    assert!(transition.next_state.sticky.is_none());
    assert!(transition.next_state.activities.is_empty());
    assert!(transition.next_state.timers.is_empty());
    assert!(transition.dispatch_ops.is_empty());
    assert!(transition.request_dedupe_ops.is_empty());
    assert!(matches!(
        transition.history_events[0].kind,
        HistoryEventKind::WorkflowExecutionTimedOut {
            timeout_type: WorkflowTimeoutType::RunTimeout,
            retry_state: RetryState::Timeout
        }
    ));
}

#[test]
fn workflow_execution_timed_out_with_entities() {
    let mut state = make_open_state_with_activity("activity-1");
    state.activities.insert(
        "activity-2".into(),
        ActivityState {
            activity_id: "activity-2".into(),
            schedule_event_id: 6,
            task_queue: TaskQueueName("activity-q".into()),
            attempt: 1,
            schedule_to_close_timeout: Some(Duration::minutes(2)),
            schedule_to_start_timeout: Some(Duration::seconds(30)),
            start_to_close_timeout: Some(Duration::minutes(1)),
            heartbeat_timeout: Some(Duration::seconds(20)),
        },
    );
    state.timers.insert(
        "timer-1".into(),
        TimerState {
            timer_id: "timer-1".into(),
            started_event_id: 7,
            fire_at: now(),
        },
    );
    let transition = kernel().apply(
        LoadedRun::Existing(state),
        Command::WorkflowExecutionTimedOut(make_timeout_request()),
    ).unwrap();

    assert!(transition.next_state.activities.is_empty());
    assert!(transition.next_state.timers.is_empty());
    assert_eq!(transition.activity_ops.len(), 2);
    assert_eq!(transition.timer_ops.len(), 1);
}

#[test]
fn workflow_execution_timed_out_with_pending_wft() {
    let mut state = make_open_state_with_pending_wft();
    state.sticky = Some(StickyAffinity {
        worker_identity: WorkerIdentity("sticky-worker".into()),
        expires_at: now() + Duration::seconds(30),
    });
    let transition = kernel().apply(
        LoadedRun::Existing(state),
        Command::WorkflowExecutionTimedOut(make_timeout_request()),
    ).unwrap();

    assert!(transition.next_state.pending_workflow_task.is_none());
    assert!(transition.next_state.sticky.is_none());
    assert!(transition.dispatch_ops.is_empty());
}

#[test]
fn reject_timeout_absent_run() {
    assert_eq!(
        kernel().apply(
            LoadedRun::Absent,
            Command::WorkflowExecutionTimedOut(make_timeout_request()),
        ),
        Err(Reject::MissingRun)
    );
}

#[test]
fn reject_timeout_closed_run() {
    assert_eq!(
        kernel().apply(
            LoadedRun::Existing(make_closed_state()),
            Command::WorkflowExecutionTimedOut(make_timeout_request()),
        ),
        Err(Reject::RunClosed(ExecutionStatus::Completed))
    );
}

#[test]
fn fail_workflow_with_retry_policy() {
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
            commands: vec![WorkflowCommand::FailWorkflow {
                message: "nope".into(),
                details: Some(payload("details")),
            }],
            force_new_workflow_task: false,
            now: now(),
        }),
    ).unwrap();

    assert!(transition.history_events.iter().any(|event| matches!(
        event.kind,
        HistoryEventKind::WorkflowExecutionFailed {
            retry_state: RetryState::InProgress,
            attempt: 1,
            ..
        }
    )));
}

#[test]
fn fail_workflow_without_retry_policy() {
    let mut state = make_open_state_with_started_wft();
    state.retry_policy = None;
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
            commands: vec![WorkflowCommand::FailWorkflow {
                message: "nope".into(),
                details: Some(payload("details")),
            }],
            force_new_workflow_task: false,
            now: now(),
        }),
    ).unwrap();

    assert!(transition.history_events.iter().any(|event| matches!(
        event.kind,
        HistoryEventKind::WorkflowExecutionFailed {
            retry_state: RetryState::RetryPolicyNotSet,
            attempt: 1,
            ..
        }
    )));
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
fn wft_failed_with_started_wft() {
    let state = make_open_state_with_started_wft_and_sticky();
    let transition = kernel().apply(
        LoadedRun::Existing(state.clone()),
        Command::WorkflowTaskFailed(WorkflowTaskFailedRequest {
            logical_seq: LogicalTaskSeq(3),
            started_event_id: 9,
            failure_cause: WorkflowTaskFailedCause::NonDeterminismError,
            failure_details: Some(payload("details")),
            worker_identity: WorkerIdentity("worker".into()),
            now: now(),
        }),
    ).unwrap();

    assert_eq!(transition.history_events.len(), 1);
    assert!(matches!(
        &transition.history_events[0].kind,
        HistoryEventKind::WorkflowTaskFailed {
            logical_seq,
            scheduled_event_id,
            started_event_id,
            failure_cause,
            failure_details,
            identity,
        }
        if *logical_seq == LogicalTaskSeq(3)
            && *scheduled_event_id == 8
            && *started_event_id == 9
            && *failure_cause == WorkflowTaskFailedCause::NonDeterminismError
            && *failure_details == Some(payload("details"))
            && *identity == WorkerIdentity("worker".into())
    ));
    let pending = transition.next_state.pending_workflow_task.unwrap();
    assert_eq!(pending.logical_seq, LogicalTaskSeq(3));
    assert_eq!(pending.scheduled_event_id, 8);
    assert_eq!(pending.started_event_id, None);
    assert_eq!(transition.next_state.sticky, state.sticky);
    assert_eq!(transition.dispatch_ops.len(), 1);
    assert!(matches!(
        &transition.dispatch_ops[0],
        DispatchOp::EnqueueWorkflowTask {
            logical_seq,
            sticky_preferred,
            ..
        } if *logical_seq == LogicalTaskSeq(3)
            && *sticky_preferred == Some(WorkerIdentity("sticky-worker".into()))
    ));
    assert!(transition.request_dedupe_ops.is_empty());
    assert!(transition.activity_ops.is_empty());
    assert!(transition.timer_ops.is_empty());
    assert!(transition.projection_ops.is_empty());
}

#[test]
fn wft_timed_out_with_started_wft() {
    let state = make_open_state_with_started_wft_and_sticky();
    let transition = kernel().apply(
        LoadedRun::Existing(state),
        Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest {
            logical_seq: LogicalTaskSeq(3),
            started_event_id: 9,
            timeout_type: WorkflowTaskTimeoutType::StartToClose,
            now: now(),
        }),
    ).unwrap();

    assert_eq!(transition.history_events.len(), 1);
    assert!(matches!(
        &transition.history_events[0].kind,
        HistoryEventKind::WorkflowTaskTimedOut {
            logical_seq,
            scheduled_event_id,
            started_event_id,
            timeout_type,
        }
        if *logical_seq == LogicalTaskSeq(3)
            && *scheduled_event_id == 8
            && *started_event_id == 9
            && *timeout_type == WorkflowTaskTimeoutType::StartToClose
    ));
    let pending = transition.next_state.pending_workflow_task.unwrap();
    assert_eq!(pending.logical_seq, LogicalTaskSeq(3));
    assert_eq!(pending.scheduled_event_id, 8);
    assert_eq!(pending.started_event_id, None);
    assert!(transition.next_state.sticky.is_none());
    assert_eq!(transition.dispatch_ops.len(), 1);
    assert!(matches!(
        &transition.dispatch_ops[0],
        DispatchOp::EnqueueWorkflowTask {
            logical_seq,
            sticky_preferred,
            ..
        } if *logical_seq == LogicalTaskSeq(3) && sticky_preferred.is_none()
    ));
    assert!(transition.request_dedupe_ops.is_empty());
    assert!(transition.activity_ops.is_empty());
    assert!(transition.timer_ops.is_empty());
    assert!(transition.projection_ops.is_empty());
}

#[test]
fn wft_failed_no_sticky() {
    let state = make_open_state_with_started_wft();
    let transition = kernel().apply(
        LoadedRun::Existing(state),
        Command::WorkflowTaskFailed(WorkflowTaskFailedRequest {
            logical_seq: LogicalTaskSeq(3),
            started_event_id: 9,
            failure_cause: WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure,
            failure_details: None,
            worker_identity: WorkerIdentity("worker".into()),
            now: now(),
        }),
    ).unwrap();

    assert!(transition.next_state.sticky.is_none());
    assert!(matches!(
        &transition.dispatch_ops[0],
        DispatchOp::EnqueueWorkflowTask {
            sticky_preferred,
            ..
        } if sticky_preferred.is_none()
    ));
}

#[test]
fn wft_timed_out_no_sticky() {
    let state = make_open_state_with_started_wft();
    let transition = kernel().apply(
        LoadedRun::Existing(state),
        Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest {
            logical_seq: LogicalTaskSeq(3),
            started_event_id: 9,
            timeout_type: WorkflowTaskTimeoutType::StartToClose,
            now: now(),
        }),
    ).unwrap();

    assert!(transition.next_state.sticky.is_none());
    assert!(matches!(
        &transition.dispatch_ops[0],
        DispatchOp::EnqueueWorkflowTask {
            sticky_preferred,
            ..
        } if sticky_preferred.is_none()
    ));
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
fn reject_cancel_absent_run() {
    assert_eq!(
        kernel().apply(LoadedRun::Absent, Command::Cancel(make_cancel_request())),
        Err(Reject::MissingRun)
    );
}

#[test]
fn reject_cancel_closed_run() {
    assert_eq!(
        kernel().apply(
            LoadedRun::Existing(make_closed_state()),
            Command::Cancel(make_cancel_request()),
        ),
        Err(Reject::RunClosed(ExecutionStatus::Completed))
    );
}

#[test]
fn reject_terminate_absent_run() {
    assert_eq!(
        kernel().apply(LoadedRun::Absent, Command::Terminate(make_terminate_request())),
        Err(Reject::MissingRun)
    );
}

#[test]
fn reject_terminate_closed_run() {
    assert_eq!(
        kernel().apply(
            LoadedRun::Existing(make_closed_state()),
            Command::Terminate(make_terminate_request()),
        ),
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
fn reject_wft_failed_absent_run() {
    assert_eq!(
        kernel().apply(LoadedRun::Absent, Command::WorkflowTaskFailed(WorkflowTaskFailedRequest {
            logical_seq: LogicalTaskSeq(3),
            started_event_id: 9,
            failure_cause: WorkflowTaskFailedCause::UnhandledCommand,
            failure_details: None,
            worker_identity: WorkerIdentity("worker".into()),
            now: now(),
        })),
        Err(Reject::MissingRun)
    );
}

#[test]
fn reject_wft_failed_closed_run() {
    assert_eq!(
        kernel().apply(LoadedRun::Existing(make_closed_state()), Command::WorkflowTaskFailed(WorkflowTaskFailedRequest {
            logical_seq: LogicalTaskSeq(3),
            started_event_id: 9,
            failure_cause: WorkflowTaskFailedCause::UnhandledCommand,
            failure_details: None,
            worker_identity: WorkerIdentity("worker".into()),
            now: now(),
        })),
        Err(Reject::RunClosed(ExecutionStatus::Completed))
    );
}

#[test]
fn reject_wft_failed_no_pending() {
    assert_eq!(
        kernel().apply(LoadedRun::Existing(make_open_state()), Command::WorkflowTaskFailed(WorkflowTaskFailedRequest {
            logical_seq: LogicalTaskSeq(3),
            started_event_id: 9,
            failure_cause: WorkflowTaskFailedCause::UnhandledCommand,
            failure_details: None,
            worker_identity: WorkerIdentity("worker".into()),
            now: now(),
        })),
        Err(Reject::NoPendingWorkflowTask)
    );
}

#[test]
fn reject_wft_failed_not_started() {
    assert_eq!(
        kernel().apply(LoadedRun::Existing(make_open_state_with_pending_wft()), Command::WorkflowTaskFailed(WorkflowTaskFailedRequest {
            logical_seq: LogicalTaskSeq(3),
            started_event_id: 9,
            failure_cause: WorkflowTaskFailedCause::UnhandledCommand,
            failure_details: None,
            worker_identity: WorkerIdentity("worker".into()),
            now: now(),
        })),
        Err(Reject::WorkflowTaskNotStarted { logical_seq: 3 })
    );
}

#[test]
fn reject_wft_failed_seq_mismatch() {
    assert_eq!(
        kernel().apply(LoadedRun::Existing(make_open_state_with_started_wft()), Command::WorkflowTaskFailed(WorkflowTaskFailedRequest {
            logical_seq: LogicalTaskSeq(4),
            started_event_id: 9,
            failure_cause: WorkflowTaskFailedCause::UnhandledCommand,
            failure_details: None,
            worker_identity: WorkerIdentity("worker".into()),
            now: now(),
        })),
        Err(Reject::WorkflowTaskSeqMismatch { expected: 3, got: 4 })
    );
}

#[test]
fn reject_wft_failed_started_event_mismatch() {
    assert_eq!(
        kernel().apply(LoadedRun::Existing(make_open_state_with_started_wft()), Command::WorkflowTaskFailed(WorkflowTaskFailedRequest {
            logical_seq: LogicalTaskSeq(3),
            started_event_id: 10,
            failure_cause: WorkflowTaskFailedCause::UnhandledCommand,
            failure_details: None,
            worker_identity: WorkerIdentity("worker".into()),
            now: now(),
        })),
        Err(Reject::WorkflowTaskTokenMismatch)
    );
}

#[test]
fn reject_wft_timed_out_absent_run() {
    assert_eq!(
        kernel().apply(LoadedRun::Absent, Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest {
            logical_seq: LogicalTaskSeq(3),
            started_event_id: 9,
            timeout_type: WorkflowTaskTimeoutType::StartToClose,
            now: now(),
        })),
        Err(Reject::MissingRun)
    );
}

#[test]
fn reject_wft_timed_out_closed_run() {
    assert_eq!(
        kernel().apply(LoadedRun::Existing(make_closed_state()), Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest {
            logical_seq: LogicalTaskSeq(3),
            started_event_id: 9,
            timeout_type: WorkflowTaskTimeoutType::StartToClose,
            now: now(),
        })),
        Err(Reject::RunClosed(ExecutionStatus::Completed))
    );
}

#[test]
fn reject_wft_timed_out_no_pending() {
    assert_eq!(
        kernel().apply(LoadedRun::Existing(make_open_state()), Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest {
            logical_seq: LogicalTaskSeq(3),
            started_event_id: 9,
            timeout_type: WorkflowTaskTimeoutType::StartToClose,
            now: now(),
        })),
        Err(Reject::NoPendingWorkflowTask)
    );
}

#[test]
fn reject_wft_timed_out_not_started() {
    assert_eq!(
        kernel().apply(LoadedRun::Existing(make_open_state_with_pending_wft()), Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest {
            logical_seq: LogicalTaskSeq(3),
            started_event_id: 9,
            timeout_type: WorkflowTaskTimeoutType::StartToClose,
            now: now(),
        })),
        Err(Reject::WorkflowTaskNotStarted { logical_seq: 3 })
    );
}

#[test]
fn reject_wft_timed_out_seq_mismatch() {
    assert_eq!(
        kernel().apply(LoadedRun::Existing(make_open_state_with_started_wft()), Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest {
            logical_seq: LogicalTaskSeq(4),
            started_event_id: 9,
            timeout_type: WorkflowTaskTimeoutType::StartToClose,
            now: now(),
        })),
        Err(Reject::WorkflowTaskSeqMismatch { expected: 3, got: 4 })
    );
}

#[test]
fn reject_wft_timed_out_started_event_mismatch() {
    assert_eq!(
        kernel().apply(LoadedRun::Existing(make_open_state_with_started_wft()), Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest {
            logical_seq: LogicalTaskSeq(3),
            started_event_id: 10,
            timeout_type: WorkflowTaskTimeoutType::StartToClose,
            now: now(),
        })),
        Err(Reject::WorkflowTaskTokenMismatch)
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

#[test]
fn cancel_workflow_command() {
    let state = make_open_state_with_started_wft_and_sticky();
    let transition = kernel()
        .apply(
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
                commands: vec![WorkflowCommand::CancelWorkflow],
                force_new_workflow_task: false,
                now: now(),
            }),
        )
        .unwrap();

    assert!(matches!(
        transition.history_events[0].kind,
        HistoryEventKind::WorkflowTaskCompleted { .. }
    ));
    assert!(matches!(
        transition.history_events[1].kind,
        HistoryEventKind::WorkflowExecutionCanceled
    ));
    assert_eq!(transition.next_state.status, ExecutionStatus::Cancelled);
    assert!(transition.next_state.closed_at.is_some());
    assert!(transition.next_state.pending_workflow_task.is_none());
    assert!(transition.next_state.sticky.is_none());
    assert_eq!(
        transition.projection_ops.last(),
        Some(&ProjectionOp::CloseExecution {
            status: ExecutionStatus::Cancelled,
            closed_at: now(),
        })
    );
    assert!(transition.request_dedupe_ops.is_empty());
    assert!(transition.activity_ops.is_empty());
    assert!(transition.timer_ops.is_empty());
}

#[test]
fn cancel_workflow_then_another_command() {
    let state = make_open_state_with_started_wft();
    assert_eq!(
        kernel().apply(
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
                    WorkflowCommand::CancelWorkflow,
                    WorkflowCommand::RequestNewWorkflowTask,
                ],
                force_new_workflow_task: false,
                now: now(),
            }),
        ),
        Err(Reject::CommandsAfterClose { index: 1 })
    );
}

#[test]
fn request_cancel_activity() {
    let state = with_pending_activity_started_wft();
    let transition = kernel()
        .apply(
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
                commands: vec![WorkflowCommand::RequestCancelActivity {
                    activity_id: "activity-1".into(),
                }],
                force_new_workflow_task: false,
                now: now(),
            }),
        )
        .unwrap();

    assert!(transition.history_events.iter().any(|event| matches!(
        &event.kind,
        HistoryEventKind::ActivityTaskCancelRequested { activity_id } if activity_id == "activity-1"
    )));
    assert!(transition.next_state.activities.contains_key("activity-1"));
    assert!(transition.activity_ops.is_empty());
}

#[test]
fn request_cancel_activity_unknown() {
    let state = make_open_state_with_started_wft();
    assert_eq!(
        kernel().apply(
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
                commands: vec![WorkflowCommand::RequestCancelActivity {
                    activity_id: "missing".into(),
                }],
                force_new_workflow_task: false,
                now: now(),
            }),
        ),
        Err(Reject::UnknownActivity("missing".into()))
    );
}

#[test]
fn cancel_timer() {
    let state = with_pending_timer_started_wft();
    let transition = kernel()
        .apply(
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
                commands: vec![WorkflowCommand::CancelTimer {
                    timer_id: "timer-1".into(),
                }],
                force_new_workflow_task: false,
                now: now(),
            }),
        )
        .unwrap();

    assert!(transition.history_events.iter().any(|event| matches!(
        &event.kind,
        HistoryEventKind::TimerCanceled { timer_id } if timer_id == "timer-1"
    )));
    assert!(!transition.next_state.timers.contains_key("timer-1"));
    assert!(matches!(
        transition.timer_ops[0],
        tokeira_kernel::TimerOp::Delete { ref timer_id } if timer_id == "timer-1"
    ));
}

#[test]
fn cancel_timer_unknown() {
    let state = make_open_state_with_started_wft();
    assert_eq!(
        kernel().apply(
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
                commands: vec![WorkflowCommand::CancelTimer {
                    timer_id: "missing".into(),
                }],
                force_new_workflow_task: false,
                now: now(),
            }),
        ),
        Err(Reject::UnknownTimer("missing".into()))
    );
}

#[test]
fn request_cancel_activity_then_resolved_canceled() {
    let state = with_pending_activity_started_wft();
    let first = kernel()
        .apply(
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
                commands: vec![WorkflowCommand::RequestCancelActivity {
                    activity_id: "activity-1".into(),
                }],
                force_new_workflow_task: false,
                now: now(),
            }),
        )
        .unwrap();
    assert!(first.next_state.activities.contains_key("activity-1"));
    assert!(first.history_events.iter().any(|event| matches!(
        event.kind,
        HistoryEventKind::ActivityTaskCancelRequested { .. }
    )));

    let second = kernel()
        .apply(
            LoadedRun::Existing(first.next_state),
            Command::ActivityResolved(ActivityResolvedRequest {
                activity_id: "activity-1".into(),
                resolution: tokeira_kernel::ActivityResolution::Canceled {
                    details: Some(payloads("cancel")),
                },
                worker_identity: None,
                now: now(),
            }),
        )
        .unwrap();
    assert!(second.history_events.iter().any(|event| matches!(
        event.kind,
        HistoryEventKind::ActivityTaskCanceled { .. }
    )));
    assert!(!second.next_state.activities.contains_key("activity-1"));
    assert!(second.activity_ops.iter().any(|op| matches!(
        op,
        tokeira_kernel::ActivityOp::Delete { activity_id } if activity_id == "activity-1"
    )));
}

#[test]
fn cancel_then_cancel_workflow_e2e() {
    let cancel = kernel()
        .apply(
            LoadedRun::Existing(make_open_state()),
            Command::Cancel(make_cancel_request()),
        )
        .unwrap();
    let started = kernel()
        .apply(
            LoadedRun::Existing(cancel.next_state.clone()),
            Command::WorkflowTaskStarted(StartWorkflowTaskRequest {
                logical_seq: cancel.next_state.pending_workflow_task.unwrap().logical_seq,
                worker_identity: WorkerIdentity("worker".into()),
                sticky_ttl: None,
                now: now(),
            }),
        )
        .unwrap();
    let state = started.next_state.clone();
    let pending = state.pending_workflow_task.clone().unwrap_or(PendingWorkflowTask {
        logical_seq: LogicalTaskSeq(4),
        scheduled_event_id: 11,
        started_event_id: Some(12),
        attempt: 1,
    });
    let final_transition = kernel()
        .apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: pending.logical_seq,
                    started_event_id: pending.started_event_id.unwrap(),
                    attempt: pending.attempt,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                commands: vec![WorkflowCommand::CancelWorkflow],
                force_new_workflow_task: false,
                now: now(),
            }),
        )
        .unwrap();
    assert_eq!(final_transition.next_state.status, ExecutionStatus::Cancelled);
}

fn with_pending_activity_started_wft() -> WorkflowState {
    let state = make_open_state_with_started_wft();
    let mut state = state;
    state.activities.insert(
        "activity-1".into(),
        ActivityState {
            activity_id: "activity-1".into(),
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

fn with_pending_timer_started_wft() -> WorkflowState {
    let state = make_open_state_with_started_wft();
    let mut state = state;
    state.timers.insert(
        "timer-1".into(),
        TimerState {
            timer_id: "timer-1".into(),
            started_event_id: 7,
            fire_at: now(),
        },
    );
    state
}
