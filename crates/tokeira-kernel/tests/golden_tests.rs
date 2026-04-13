use std::collections::BTreeMap;

use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    ActivityOp, ActivityPauseInfo, ActivityResolvedRequest, ActivityState, BasicKernel,
    CancelRequest, ChildResolution, ChildResolvedRequest, ChildStartConfirmedRequest,
    ChildStartResult, ChildWorkflowState, Command, CompletionCallback, DispatchOp,
    ExternalCancelResolvedRequest, ExternalCancelResult, ExternalSignalResolvedRequest,
    ExternalSignalResult, ExternalWorkflowExecution, FieldChange, LoadedRun,
    NexusOperationResolvedRequest, NexusResolution, ParentClosePolicy,
    PauseActivityRequest, PauseInfo, PauseWorkflowRequest, PendingExternalCancel,
    PendingExternalSignal, PendingNexusOperation, PendingUpdate, PendingWorkflowTask,
    ProjectionOp, Reject, ReplayContext, ResetActivityRequest, ResetRequest,
    RetryState, SignalRequest, StartRequest, StartWorkflowTaskRequest,
    TerminateRequest, TimerDueRequest, TimerState, UnpauseActivityRequest,
    UnpauseWorkflowRequest,
    UpdateActivityOptionsRequest, UpdateExecutionOptionsRequest, UpdateProtocolBody,
    UpdateRequest, VersioningOverride, WorkflowCommand, WorkflowExecutionTimedOutRequest,
    WorkflowState, WorkflowTaskCompletedRequest, WorkflowTaskFailedCause,
    WorkflowTaskFailedRequest, WorkflowTaskTimedOutRequest, WorkflowTaskTimeoutType,
    WorkflowTimeoutType, event::{HistoryEvent, HistoryEventKind}, kernel::Kernel,
};
use tokeira_types::{
    ExecutionStatus, LogicalTaskSeq, Memo, NamespaceId, Payload, Payloads,
    RequestContext, RequestId, RetryPolicy, RunId, RunKey, SearchAttrValue,
    SearchAttributes, ShardEpoch, StickyAffinity, TaskQueueName, TransitionSeq,
    WorkerIdentity, WorkflowId, WorkflowTaskToken, WorkflowType,
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
        deployment: None,
        build_id: None,
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
        parent_run_key: None,
        parent_workflow_id: None,
        first_run_started_at: Some(now()),
        request: request_context("start-req"),
        now: now(),
    }
}

fn replay_context_from_start(start: &StartRequest) -> ReplayContext {
    ReplayContext {
        run_key: start.run_key,
        namespace_id: start.namespace_id,
        workflow_id: start.workflow_id.clone(),
        run_id: start.run_id,
        deployment: start.deployment.clone(),
        build_id: start.build_id.clone(),
        parent_run_key: start.parent_run_key,
        parent_workflow_id: start.parent_workflow_id.clone(),
        first_run_started_at: start.first_run_started_at,
    }
}

fn history_event(
    event_id: i64,
    happened_at: OffsetDateTime,
    kind: HistoryEventKind,
) -> HistoryEvent {
    HistoryEvent {
        event_id,
        happened_at,
        kind,
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
        deployment: None,
        build_id: None,
        status: ExecutionStatus::Running,
        transition_seq: TransitionSeq(5),
        last_event_id: 9,
        next_workflow_task_seq: LogicalTaskSeq(4),
        pending_workflow_task: None,
        sticky: None,
        pause_info: None,
        wft_stamp: 0,
        memo: memo(),
        search_attributes: search_attributes(),
        workflow_execution_timeout: Some(Duration::minutes(10)),
        workflow_run_timeout: Some(Duration::minutes(5)),
        workflow_task_timeout: Duration::seconds(10),
        retry_policy: Some(retry_policy()),
        attempt: 1,
        first_execution_run_id: Some(RunId::new()),
        parent_run_key: None,
        parent_workflow_id: None,
        activities: BTreeMap::new(),
        timers: BTreeMap::new(),
        children: BTreeMap::new(),
        pending_external_signals: BTreeMap::new(),
        pending_external_cancels: BTreeMap::new(),
        pending_updates: BTreeMap::new(),
        pending_nexus_operations: BTreeMap::new(),
        versioning_override: None,
        completion_callbacks: Vec::new(),
        started_at: now() - Duration::minutes(3),
        first_run_started_at: Some(now() - Duration::minutes(3)),
        closed_at: None,
        close_result: None,
        close_failure: None,
    }
}

#[test]
fn replay_history_reconstructs_workflow_task_lifecycle() {
    let kernel = BasicKernel;
    let start = make_start_request();
    let ctx = replay_context_from_start(&start);
    let worker = WorkerIdentity("worker".into());
    let started_at = now();
    let events = vec![
        history_event(
            1,
            started_at,
            HistoryEventKind::WorkflowExecutionStarted {
                workflow_type: start.workflow_type.clone(),
                task_queue: start.task_queue.clone(),
                input: start.input.clone(),
                memo: start.memo.clone(),
                search_attributes: start.search_attributes.clone(),
                request_id: start.request.request_id.0.clone(),
                continued_execution_run_id: start.continued_execution_run_id,
                first_execution_run_id: start.first_execution_run_id,
                retry_policy: start.retry_policy.clone(),
                attempt: start.attempt,
                workflow_execution_timeout: start.workflow_execution_timeout,
                workflow_run_timeout: start.workflow_run_timeout,
                workflow_task_timeout: start.workflow_task_timeout,
            },
        ),
        history_event(
            2,
            started_at,
            HistoryEventKind::WorkflowTaskScheduled {
                logical_seq: LogicalTaskSeq::ONE,
            },
        ),
        history_event(
            3,
            started_at,
            HistoryEventKind::WorkflowTaskStarted {
                logical_seq: LogicalTaskSeq::ONE,
                scheduled_event_id: 2,
                attempt: 1,
                identity: worker.clone(),
            },
        ),
        history_event(
            4,
            started_at,
            HistoryEventKind::WorkflowTaskCompleted {
                logical_seq: LogicalTaskSeq::ONE,
                scheduled_event_id: 2,
                started_event_id: 3,
                identity: worker,
            },
        ),
    ];

    let state = kernel.replay_history_prefix(ctx, &events).unwrap();

    assert_eq!(state.pending_workflow_task, None);
    assert_eq!(state.next_workflow_task_seq, LogicalTaskSeq(2));
    assert_eq!(state.transition_seq, TransitionSeq::ZERO);
    assert_eq!(state.last_event_id, 4);
    assert_eq!(state.started_at, started_at);
}

#[test]
fn replay_history_reconstructs_activity_and_timer_state() {
    let kernel = BasicKernel;
    let start = make_start_request();
    let ctx = replay_context_from_start(&start);
    let t0 = now();
    let events = vec![
        history_event(
            1,
            t0,
            HistoryEventKind::WorkflowExecutionStarted {
                workflow_type: start.workflow_type.clone(),
                task_queue: start.task_queue.clone(),
                input: start.input.clone(),
                memo: start.memo.clone(),
                search_attributes: start.search_attributes.clone(),
                request_id: start.request.request_id.0.clone(),
                continued_execution_run_id: start.continued_execution_run_id,
                first_execution_run_id: start.first_execution_run_id,
                retry_policy: start.retry_policy.clone(),
                attempt: start.attempt,
                workflow_execution_timeout: start.workflow_execution_timeout,
                workflow_run_timeout: start.workflow_run_timeout,
                workflow_task_timeout: start.workflow_task_timeout,
            },
        ),
        history_event(
            2,
            t0,
            HistoryEventKind::ActivityTaskScheduled {
                activity_id: "a1".into(),
                activity_type: "activity".into(),
                task_queue: TaskQueueName("activity-q".into()),
                input: payloads("activity-input"),
                header: None,
                retry_policy: Some(retry_policy()),
                schedule_to_close_timeout: Some(Duration::minutes(2)),
                schedule_to_start_timeout: Some(Duration::seconds(30)),
                start_to_close_timeout: Some(Duration::minutes(1)),
                heartbeat_timeout: Some(Duration::seconds(20)),
            },
        ),
        history_event(
            3,
            t0 + Duration::seconds(1),
            HistoryEventKind::ActivityTaskStarted {
                activity_id: "a1".into(),
                scheduled_event_id: 2,
                attempt: 1,
                identity: WorkerIdentity("activity-worker".into()),
            },
        ),
        history_event(
            4,
            t0,
            HistoryEventKind::TimerStarted {
                timer_id: "t1".into(),
                fire_at: t0 + Duration::minutes(1),
            },
        ),
    ];

    let state = kernel.replay_history_prefix(ctx, &events).unwrap();

    let activity = state.activities.get("a1").unwrap();
    assert_eq!(activity.started_event_id, Some(3));
    assert_eq!(activity.started_at, Some(t0 + Duration::seconds(1)));
    let timer = state.timers.get("t1").unwrap();
    assert_eq!(timer.started_event_id, 4);
}

#[test]
fn replay_history_reconstructs_historical_execution_options_and_pause() {
    let kernel = BasicKernel;
    let start = make_start_request();
    let ctx = replay_context_from_start(&start);
    let t0 = now();
    let events = vec![
        history_event(
            1,
            t0,
            HistoryEventKind::WorkflowExecutionStarted {
                workflow_type: start.workflow_type.clone(),
                task_queue: start.task_queue.clone(),
                input: start.input.clone(),
                memo: start.memo.clone(),
                search_attributes: start.search_attributes.clone(),
                request_id: start.request.request_id.0.clone(),
                continued_execution_run_id: start.continued_execution_run_id,
                first_execution_run_id: start.first_execution_run_id,
                retry_policy: start.retry_policy.clone(),
                attempt: start.attempt,
                workflow_execution_timeout: start.workflow_execution_timeout,
                workflow_run_timeout: start.workflow_run_timeout,
                workflow_task_timeout: start.workflow_task_timeout,
            },
        ),
        history_event(
            2,
            t0,
            HistoryEventKind::WorkflowExecutionOptionsUpdated {
                versioning_override: FieldChange::Set(VersioningOverride),
                completion_callbacks: FieldChange::Set(vec![CompletionCallback]),
                attached_request_id: Some("options-req".into()),
            },
        ),
        history_event(
            3,
            t0 + Duration::seconds(1),
            HistoryEventKind::WorkflowExecutionPaused {
                identity: "operator".into(),
                reason: "paused".into(),
                request_id: "pause-req".into(),
            },
        ),
    ];

    let state = kernel.replay_history_prefix(ctx, &events).unwrap();

    assert_eq!(state.status, ExecutionStatus::Paused);
    assert!(state.pause_info.is_some());
    assert_eq!(state.versioning_override, Some(VersioningOverride));
    assert_eq!(state.completion_callbacks, vec![CompletionCallback]);
    assert_eq!(state.sticky, None);
    assert_eq!(state.wft_stamp, 0);
}

#[test]
fn replay_history_rejects_empty_or_non_started_sequences() {
    let kernel = BasicKernel;
    let start = make_start_request();
    let ctx = replay_context_from_start(&start);

    let empty = kernel.replay_history_prefix(ctx.clone(), &[]);
    assert_eq!(empty, Err(Reject::InvalidReplayHistory));

    let invalid = kernel.replay_history_prefix(
        ctx,
        &[history_event(
            1,
            now(),
            HistoryEventKind::WorkflowTaskScheduled {
                logical_seq: LogicalTaskSeq::ONE,
            },
        )],
    );
    assert_eq!(invalid, Err(Reject::InvalidReplayHistory));
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

fn make_paused_state() -> WorkflowState {
    let mut state = make_open_state();
    state.status = ExecutionStatus::Paused;
    state.pause_info = Some(PauseInfo {
        pause_time: now(),
        identity: "operator".into(),
        reason: "paused".into(),
        request_id: "pause-req".into(),
    });
    state.wft_stamp = 1;
    state
}

fn make_paused_state_with_activity(id: &str) -> WorkflowState {
    let mut state = make_paused_state();
    state.activities.insert(
        id.into(),
        ActivityState {
            activity_id: id.into(),
            activity_type: "activity-type".into(),
            schedule_event_id: 7,
            task_queue: TaskQueueName("activity-q".into()),
            deployment: None,
            build_id: None,
            input: Payloads::default(),
            header: None,
            attempt: 1,
            retry_policy: None,
            schedule_to_close_timeout: Some(Duration::minutes(2)),
            schedule_to_start_timeout: Some(Duration::seconds(30)),
            start_to_close_timeout: Some(Duration::minutes(1)),
            heartbeat_timeout: Some(Duration::seconds(20)),
            scheduled_at: OffsetDateTime::UNIX_EPOCH,
            started_at: None,
            started_event_id: None,
            pause_info: None,
            stamp: 0,
        },
    );
    state
}

fn make_open_state_with_activity(id: &str) -> WorkflowState {
    let mut state = make_open_state();
    state.activities.insert(
        id.into(),
        ActivityState {
            activity_id: id.into(),
            activity_type: "activity-type".into(),
            schedule_event_id: 7,
            task_queue: TaskQueueName("activity-q".into()),
            deployment: None,
            build_id: None,
            input: Payloads::default(),
            header: None,
            attempt: 1,
            retry_policy: None,
            schedule_to_close_timeout: Some(Duration::minutes(2)),
            schedule_to_start_timeout: Some(Duration::seconds(30)),
            start_to_close_timeout: Some(Duration::minutes(1)),
            heartbeat_timeout: Some(Duration::seconds(20)),
            scheduled_at: OffsetDateTime::UNIX_EPOCH,
            started_at: None,
            started_event_id: None,
            pause_info: None,
            stamp: 0,
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

fn make_reset_request() -> ResetRequest {
    ResetRequest {
        fork_event_id: 5,
        new_run_id: RunId::new(),
        reason: "operator reset".into(),
        request: request_context("reset-req"),
        now: now(),
    }
}

fn make_pause_workflow_request() -> PauseWorkflowRequest {
    PauseWorkflowRequest {
        identity: "operator".into(),
        reason: "paused for maintenance".into(),
        request: request_context("pause-req"),
        now: now(),
    }
}

fn make_unpause_workflow_request() -> UnpauseWorkflowRequest {
    UnpauseWorkflowRequest {
        identity: "operator".into(),
        reason: "resume".into(),
        request: request_context("unpause-req"),
        now: now(),
    }
}

fn make_update_activity_options_request(
    activity_id: &str,
) -> UpdateActivityOptionsRequest {
    UpdateActivityOptionsRequest {
        activity_id: activity_id.into(),
        task_queue: FieldChange::Set(TaskQueueName("activity-updated".into())),
        schedule_to_close_timeout: FieldChange::Set(Some(Duration::minutes(3))),
        schedule_to_start_timeout: FieldChange::Set(Some(Duration::seconds(45))),
        start_to_close_timeout: FieldChange::Set(Some(Duration::minutes(2))),
        heartbeat_timeout: FieldChange::Set(Some(Duration::seconds(30))),
        request: request_context("update-activity-req"),
        now: now(),
    }
}

fn make_pause_activity_request(activity_id: &str) -> PauseActivityRequest {
    PauseActivityRequest {
        activity_id: activity_id.into(),
        identity: "operator".into(),
        reason: "pause activity".into(),
        request: request_context("pause-activity-req"),
        now: now(),
    }
}

fn make_unpause_activity_request(activity_id: &str) -> UnpauseActivityRequest {
    UnpauseActivityRequest {
        activity_id: activity_id.into(),
        request: request_context("unpause-activity-req"),
        now: now(),
    }
}

fn make_reset_activity_request(activity_id: &str) -> ResetActivityRequest {
    ResetActivityRequest {
        activity_id: activity_id.into(),
        reset_heartbeat: true,
        request: request_context("reset-activity-req"),
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

fn with_execution_options(mut state: WorkflowState) -> WorkflowState {
    state.versioning_override = Some(VersioningOverride);
    state.completion_callbacks = vec![CompletionCallback];
    state
}

fn with_pending_nexus_operation(
    mut state: WorkflowState,
    operation_id: &str,
) -> WorkflowState {
    state.pending_nexus_operations.insert(
        operation_id.into(),
        PendingNexusOperation {
            operation_id: operation_id.into(),
            scheduled_event_id: 12,
            endpoint: "endpoint".into(),
            service: "service".into(),
            operation: "operation".into(),
            schedule_to_close_timeout: None,
            scheduled_at: OffsetDateTime::UNIX_EPOCH,
            started: false,
        },
    );
    state
}

fn with_started_nexus_operation(
    mut state: WorkflowState,
    operation_id: &str,
) -> WorkflowState {
    state = with_pending_nexus_operation(state, operation_id);
    if let Some(pending) = state.pending_nexus_operations.get_mut(operation_id) {
        pending.started = true;
    }
    state
}

fn kernel() -> BasicKernel {
    BasicKernel
}

#[test]
fn start_from_absent() {
    let req = make_start_request();
    let transition = kernel()
        .apply(LoadedRun::Absent, Command::Start(req.clone()))
        .unwrap();

    assert_eq!(transition.expected_seq, TransitionSeq::ZERO);
    assert_eq!(transition.next_state.status, ExecutionStatus::Running);
    assert_eq!(transition.next_state.transition_seq, TransitionSeq(1));
    assert_eq!(transition.next_state.last_event_id, 2);
    assert_eq!(
        transition.next_state.workflow_execution_timeout,
        req.workflow_execution_timeout
    );
    assert_eq!(
        transition.next_state.workflow_run_timeout,
        req.workflow_run_timeout
    );
    assert_eq!(
        transition.next_state.workflow_task_timeout,
        req.workflow_task_timeout
    );
    assert_eq!(transition.next_state.retry_policy, req.retry_policy);
    assert_eq!(transition.next_state.attempt, req.attempt);
    assert_eq!(transition.history_events.len(), 2);
    assert!(matches!(
        transition.history_events[0].kind,
        HistoryEventKind::WorkflowExecutionStarted { .. }
    ));
    assert!(matches!(
        transition.history_events[1].kind,
        HistoryEventKind::WorkflowTaskScheduled { .. }
    ));
    assert_eq!(transition.request_dedupe_ops.len(), 1);
    assert_eq!(transition.projection_ops.len(), 1);
    assert_eq!(
        transition.projection_ops[0],
        ProjectionOp::UpsertExecution {
            status: ExecutionStatus::Running,
            memo_patch: req.memo,
            search_attr_patch: req.search_attributes,
        }
    );
    assert_eq!(transition.dispatch_ops.len(), 1);
    assert!(transition.next_state.pending_workflow_task.is_some());
}

#[test]
fn signal_with_no_pending_wft() {
    let state = make_open_state();
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::Signal(SignalRequest {
                signal_name: "sig".into(),
                input: payloads("signal"),
                request: request_context("signal-req"),
                now: now(),
            }),
        )
        .unwrap();

    assert!(matches!(
        transition.history_events[0].kind,
        HistoryEventKind::WorkflowExecutionSignaled { .. }
    ));
    assert!(matches!(
        transition.history_events[1].kind,
        HistoryEventKind::WorkflowTaskScheduled { .. }
    ));
    assert_eq!(transition.request_dedupe_ops.len(), 1);
    assert!(
        transition
            .dispatch_ops
            .iter()
            .any(|op| matches!(op, DispatchOp::EnqueueWorkflowTask { .. }))
    );
    assert!(transition.next_state.pending_workflow_task.is_some());
}

#[test]
fn signal_with_pending_wft() {
    let state = make_open_state_with_pending_wft();
    let pending = state.pending_workflow_task.clone().unwrap();
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::Signal(SignalRequest {
                signal_name: "sig".into(),
                input: payloads("signal"),
                request: request_context("signal-req"),
                now: now(),
            }),
        )
        .unwrap();

    assert_eq!(transition.history_events.len(), 1);
    assert!(matches!(
        transition.history_events[0].kind,
        HistoryEventKind::WorkflowExecutionSignaled { .. }
    ));
    assert_eq!(transition.request_dedupe_ops.len(), 1);
    assert!(transition.dispatch_ops.is_empty());
    assert_eq!(
        transition
            .next_state
            .pending_workflow_task
            .unwrap()
            .logical_seq,
        pending.logical_seq
    );
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
        .apply(
            LoadedRun::Existing(state),
            Command::Cancel(make_cancel_request()),
        )
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
            activity_type: "activity-type".into(),
            schedule_event_id: 6,
            task_queue: TaskQueueName("activity-q".into()),
            deployment: None,
            build_id: None,
            input: Payloads::default(),
            header: None,
            attempt: 1,
            retry_policy: None,
            schedule_to_close_timeout: Some(Duration::minutes(2)),
            schedule_to_start_timeout: Some(Duration::seconds(30)),
            start_to_close_timeout: Some(Duration::minutes(1)),
            heartbeat_timeout: Some(Duration::seconds(20)),
            scheduled_at: OffsetDateTime::UNIX_EPOCH,
            started_at: None,
            started_event_id: None,
            pause_info: None,
            stamp: 0,
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
        .apply(
            LoadedRun::Existing(state),
            Command::Terminate(make_terminate_request()),
        )
        .unwrap();

    assert!(transition.next_state.activities.is_empty());
    assert!(transition.next_state.timers.is_empty());
    assert_eq!(transition.activity_ops.len(), 2);
    assert_eq!(transition.timer_ops.len(), 1);
    assert!(matches!(
        transition.activity_ops[0],
        tokeira_kernel::ActivityOp::Delete { .. }
    ));
    assert!(matches!(
        transition.timer_ops[0],
        tokeira_kernel::TimerOp::Delete { .. }
    ));
}

#[test]
fn terminate_with_pending_wft() {
    let state = make_open_state_with_pending_wft();
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::Terminate(make_terminate_request()),
        )
        .unwrap();

    assert!(transition.next_state.pending_workflow_task.is_none());
    assert!(transition.dispatch_ops.is_empty());
}

#[test]
fn reset_happy_path_no_pending_wft() {
    let state = make_open_state();
    let req = make_reset_request();
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state.clone()),
            Command::Reset(req.clone()),
        )
        .unwrap();

    assert_eq!(transition.history_events.len(), 1);
    assert!(matches!(
        &transition.history_events[0].kind,
        HistoryEventKind::WorkflowTaskFailed {
            scheduled_event_id,
            started_event_id,
            failure_cause,
            failure_details,
            identity,
            base_run_id,
            new_run_id,
            fork_event_version,
            fork_event_id,
            ..
        }
        if *scheduled_event_id == 0
            && *started_event_id == 0
            && *failure_cause == WorkflowTaskFailedCause::ResetWorkflow
            && failure_details.is_none()
            && *identity == WorkerIdentity("reset".into())
            && *base_run_id == Some(state.run_id)
            && *new_run_id == Some(req.new_run_id)
            && fork_event_version.is_none()
            && *fork_event_id == Some(req.fork_event_id)
    ));
    assert_eq!(transition.request_dedupe_ops.len(), 1);
    assert_eq!(transition.next_state.status, ExecutionStatus::Terminated);
    assert!(transition.next_state.closed_at.is_some());
    assert!(transition.next_state.pending_workflow_task.is_none());
    assert!(transition.next_state.sticky.is_none());
    assert!(
        transition
            .dispatch_ops
            .iter()
            .all(|op| !matches!(op, DispatchOp::EnqueueWorkflowTask { .. }))
    );
}

#[test]
fn reset_happy_path_with_started_wft() {
    let state = make_open_state_with_started_wft();
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::Reset(make_reset_request()),
        )
        .unwrap();

    assert!(matches!(
        &transition.history_events[0].kind,
        HistoryEventKind::WorkflowTaskFailed {
            scheduled_event_id,
            started_event_id,
            failure_cause,
            ..
        } if *scheduled_event_id == 8
            && *started_event_id == 9
            && *failure_cause == WorkflowTaskFailedCause::ResetWorkflow
    ));
}

#[test]
fn reset_happy_path_with_scheduled_wft() {
    let state = make_open_state_with_pending_wft();
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::Reset(make_reset_request()),
        )
        .unwrap();

    assert!(matches!(
        &transition.history_events[0].kind,
        HistoryEventKind::WorkflowTaskFailed {
            scheduled_event_id,
            started_event_id,
            failure_cause,
            ..
        } if *scheduled_event_id == 8
            && *started_event_id == 0
            && *failure_cause == WorkflowTaskFailedCause::ResetWorkflow
    ));
}

#[test]
fn reset_cleans_up_activities_and_timers() {
    let mut state = make_open_state_with_activity("activity-1");
    state.activities.insert(
        "activity-2".into(),
        ActivityState {
            activity_id: "activity-2".into(),
            activity_type: "activity-type".into(),
            schedule_event_id: 6,
            task_queue: TaskQueueName("activity-q".into()),
            deployment: None,
            build_id: None,
            input: Payloads::default(),
            header: None,
            attempt: 1,
            retry_policy: None,
            schedule_to_close_timeout: Some(Duration::minutes(2)),
            schedule_to_start_timeout: Some(Duration::seconds(30)),
            start_to_close_timeout: Some(Duration::minutes(1)),
            heartbeat_timeout: Some(Duration::seconds(20)),
            scheduled_at: OffsetDateTime::UNIX_EPOCH,
            started_at: None,
            started_event_id: None,
            pause_info: None,
            stamp: 0,
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
        .apply(
            LoadedRun::Existing(state),
            Command::Reset(make_reset_request()),
        )
        .unwrap();

    assert!(transition.next_state.activities.is_empty());
    assert!(transition.next_state.timers.is_empty());
    assert_eq!(transition.activity_ops.len(), 2);
    assert_eq!(transition.timer_ops.len(), 1);
    assert!(
        transition
            .activity_ops
            .iter()
            .all(|op| matches!(op, tokeira_kernel::ActivityOp::Delete { .. }))
    );
    assert!(
        transition
            .timer_ops
            .iter()
            .all(|op| matches!(op, tokeira_kernel::TimerOp::Delete { .. }))
    );
}

#[test]
fn reset_applies_parent_close_policy() {
    let state = with_child(
        make_open_state(),
        "child-1",
        10,
        ParentClosePolicy::Terminate,
        true,
    );
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::Reset(make_reset_request()),
        )
        .unwrap();

    assert!(transition.next_state.children.is_empty());
    assert!(transition.dispatch_ops.iter().any(|op| matches!(
        op,
        DispatchOp::TerminateChild { child_workflow_id, .. }
            if child_workflow_id == &WorkflowId("child-1".into())
    )));
}

#[test]
fn reset_rejects_fork_event_id_zero() {
    let mut req = make_reset_request();
    req.fork_event_id = 0;
    let reject = kernel()
        .apply(LoadedRun::Existing(make_open_state()), Command::Reset(req))
        .unwrap_err();
    assert!(matches!(reject, Reject::ResetConstraintViolation { .. }));
}

#[test]
fn reset_rejects_fork_event_id_negative() {
    let mut req = make_reset_request();
    req.fork_event_id = -1;
    let reject = kernel()
        .apply(LoadedRun::Existing(make_open_state()), Command::Reset(req))
        .unwrap_err();
    assert!(matches!(reject, Reject::ResetConstraintViolation { .. }));
}

#[test]
fn reset_rejects_fork_event_id_exceeds_last() {
    let mut req = make_reset_request();
    req.fork_event_id = 10;
    let reject = kernel()
        .apply(LoadedRun::Existing(make_open_state()), Command::Reset(req))
        .unwrap_err();
    assert!(matches!(reject, Reject::ResetConstraintViolation { .. }));
}

#[test]
fn reset_accepts_fork_event_id_one() {
    let mut req = make_reset_request();
    req.fork_event_id = 1;
    let transition = kernel()
        .apply(LoadedRun::Existing(make_open_state()), Command::Reset(req))
        .unwrap();
    assert_eq!(transition.next_state.status, ExecutionStatus::Terminated);
}

#[test]
fn reset_accepts_fork_event_id_equals_last() {
    let mut req = make_reset_request();
    req.fork_event_id = 9;
    let transition = kernel()
        .apply(LoadedRun::Existing(make_open_state()), Command::Reset(req))
        .unwrap();
    assert_eq!(transition.next_state.status, ExecutionStatus::Terminated);
}

#[test]
fn reset_rejects_absent_run() {
    let reject = kernel()
        .apply(LoadedRun::Absent, Command::Reset(make_reset_request()))
        .unwrap_err();
    assert_eq!(reject, Reject::MissingRun);
}

#[test]
fn reset_rejects_closed_run() {
    let reject = kernel()
        .apply(
            LoadedRun::Existing(make_closed_state()),
            Command::Reset(make_reset_request()),
        )
        .unwrap_err();
    assert_eq!(reject, Reject::RunClosed(ExecutionStatus::Completed));
}

#[test]
fn pause_workflow_happy_path() {
    let mut state = make_open_state_with_activity("activity-1");
    state.activities.insert(
        "activity-2".into(),
        ActivityState {
            activity_id: "activity-2".into(),
            activity_type: "activity-type".into(),
            schedule_event_id: 8,
            task_queue: TaskQueueName("activity-q".into()),
            deployment: None,
            build_id: None,
            input: Payloads::default(),
            header: None,
            attempt: 1,
            retry_policy: None,
            schedule_to_close_timeout: Some(Duration::minutes(2)),
            schedule_to_start_timeout: Some(Duration::seconds(30)),
            start_to_close_timeout: Some(Duration::minutes(1)),
            heartbeat_timeout: Some(Duration::seconds(20)),
            scheduled_at: OffsetDateTime::UNIX_EPOCH,
            started_at: None,
            started_event_id: None,
            pause_info: None,
            stamp: 0,
        },
    );

    let req = make_pause_workflow_request();
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::PauseWorkflow(req.clone()),
        )
        .unwrap();

    assert!(matches!(
        &transition.history_events[0].kind,
        HistoryEventKind::WorkflowExecutionPaused { identity, reason, request_id }
            if identity == &req.identity && reason == &req.reason && request_id == "pause-req"
    ));
    assert_eq!(transition.next_state.status, ExecutionStatus::Paused);
    assert_eq!(transition.next_state.wft_stamp, 1);
    assert!(matches!(
        &transition.next_state.pause_info,
        Some(PauseInfo { identity, reason, request_id, .. })
            if identity == &req.identity && reason == &req.reason && request_id == "pause-req"
    ));
    assert_eq!(transition.activity_ops.len(), 2);
    assert!(
        transition
            .activity_ops
            .iter()
            .all(|op| matches!(op, ActivityOp::Upsert(ActivityState { stamp: 1, .. })))
    );
    assert!(transition.dispatch_ops.is_empty());
    assert_eq!(
        transition.projection_ops.last(),
        Some(&ProjectionOp::UpsertExecution {
            status: ExecutionStatus::Paused,
            memo_patch: transition.next_state.memo.clone(),
            search_attr_patch: transition.next_state.search_attributes.clone(),
        })
    );
}

#[test]
fn pause_workflow_idempotent_same_request_id() {
    let state = make_paused_state();
    let mut req = make_pause_workflow_request();
    req.request = request_context("pause-req");
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state.clone()),
            Command::PauseWorkflow(req),
        )
        .unwrap();

    assert!(transition.history_events.is_empty());
    assert!(transition.activity_ops.is_empty());
    assert!(transition.dispatch_ops.is_empty());
    assert!(transition.projection_ops.is_empty());
    assert_eq!(
        transition.next_state.transition_seq,
        state.transition_seq.next()
    );
    assert_eq!(transition.next_state.status, ExecutionStatus::Paused);
    assert_eq!(transition.next_state.pause_info, state.pause_info);
}

#[test]
fn pause_workflow_rejects_different_request_id() {
    let mut req = make_pause_workflow_request();
    req.request = request_context("different-pause-req");
    let reject = kernel()
        .apply(
            LoadedRun::Existing(make_paused_state()),
            Command::PauseWorkflow(req),
        )
        .unwrap_err();
    assert_eq!(reject, Reject::AlreadyPaused);
}

#[test]
fn pause_workflow_rejects_absent_run() {
    let reject = kernel()
        .apply(
            LoadedRun::Absent,
            Command::PauseWorkflow(make_pause_workflow_request()),
        )
        .unwrap_err();
    assert_eq!(reject, Reject::MissingRun);
}

#[test]
fn pause_workflow_rejects_closed_run() {
    let reject = kernel()
        .apply(
            LoadedRun::Existing(make_closed_state()),
            Command::PauseWorkflow(make_pause_workflow_request()),
        )
        .unwrap_err();
    assert_eq!(reject, Reject::RunClosed(ExecutionStatus::Completed));
}

#[test]
fn unpause_workflow_happy_path() {
    let mut state = make_paused_state_with_activity("activity-1");
    state.activities.insert(
        "activity-2".into(),
        ActivityState {
            activity_id: "activity-2".into(),
            activity_type: "activity-type".into(),
            schedule_event_id: 8,
            task_queue: TaskQueueName("activity-q".into()),
            deployment: None,
            build_id: None,
            input: Payloads::default(),
            header: None,
            attempt: 1,
            retry_policy: None,
            schedule_to_close_timeout: Some(Duration::minutes(2)),
            schedule_to_start_timeout: Some(Duration::seconds(30)),
            start_to_close_timeout: Some(Duration::minutes(1)),
            heartbeat_timeout: Some(Duration::seconds(20)),
            scheduled_at: OffsetDateTime::UNIX_EPOCH,
            started_at: None,
            started_event_id: None,
            pause_info: None,
            stamp: 1,
        },
    );
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::UnpauseWorkflow(make_unpause_workflow_request()),
        )
        .unwrap();

    assert!(matches!(
        transition.history_events[0].kind,
        HistoryEventKind::WorkflowExecutionUnpaused { .. }
    ));
    assert!(matches!(
        transition.history_events[1].kind,
        HistoryEventKind::WorkflowTaskScheduled { .. }
    ));
    assert_eq!(transition.next_state.status, ExecutionStatus::Running);
    assert!(transition.next_state.pause_info.is_none());
    assert_eq!(transition.activity_ops.len(), 2);
    assert_eq!(
        transition
            .dispatch_ops
            .iter()
            .filter(|op| matches!(op, DispatchOp::EnqueueActivityTask { .. }))
            .count(),
        2
    );
    assert_eq!(
        transition
            .dispatch_ops
            .iter()
            .filter(|op| matches!(op, DispatchOp::EnqueueWorkflowTask { .. }))
            .count(),
        1
    );
}

#[test]
fn unpause_workflow_no_activities() {
    let transition = kernel()
        .apply(
            LoadedRun::Existing(make_paused_state()),
            Command::UnpauseWorkflow(make_unpause_workflow_request()),
        )
        .unwrap();
    assert_eq!(transition.activity_ops.len(), 0);
    assert!(matches!(
        transition.history_events[1].kind,
        HistoryEventKind::WorkflowTaskScheduled { .. }
    ));
}

#[test]
fn unpause_workflow_rejects_running() {
    let reject = kernel()
        .apply(
            LoadedRun::Existing(make_open_state()),
            Command::UnpauseWorkflow(make_unpause_workflow_request()),
        )
        .unwrap_err();
    assert_eq!(reject, Reject::NotPaused);
}

#[test]
fn signal_paused_workflow_no_wft() {
    let transition = kernel()
        .apply(
            LoadedRun::Existing(make_paused_state()),
            Command::Signal(SignalRequest {
                signal_name: "sig".into(),
                input: payloads("signal"),
                request: request_context("paused-signal"),
                now: now(),
            }),
        )
        .unwrap();
    assert_eq!(transition.history_events.len(), 1);
    assert!(matches!(
        transition.history_events[0].kind,
        HistoryEventKind::WorkflowExecutionSignaled { .. }
    ));
    assert!(transition.dispatch_ops.is_empty());
}

#[test]
fn cancel_paused_workflow_no_wft() {
    let transition = kernel()
        .apply(
            LoadedRun::Existing(make_paused_state()),
            Command::Cancel(make_cancel_request()),
        )
        .unwrap();
    assert_eq!(transition.history_events.len(), 1);
    assert!(matches!(
        transition.history_events[0].kind,
        HistoryEventKind::WorkflowExecutionCancelRequested { .. }
    ));
    assert!(transition.dispatch_ops.is_empty());
}

#[test]
fn update_rejects_paused_workflow() {
    let reject = kernel()
        .apply(
            LoadedRun::Existing(make_paused_state()),
            Command::Update(UpdateRequest {
                update_id: "update-1".into(),
                update_name: "handler".into(),
                input: payloads("input"),
                request: request_context("paused-update"),
                now: now(),
            }),
        )
        .unwrap_err();
    assert_eq!(reject, Reject::WorkflowPaused);
}

#[test]
fn terminate_paused_workflow() {
    let transition = kernel()
        .apply(
            LoadedRun::Existing(make_paused_state()),
            Command::Terminate(make_terminate_request()),
        )
        .unwrap();
    assert_eq!(transition.next_state.status, ExecutionStatus::Terminated);
    assert!(transition.next_state.pause_info.is_none());
}

#[test]
fn activity_resolved_paused_workflow_no_wft() {
    let transition = kernel()
        .apply(
            LoadedRun::Existing(make_paused_state_with_activity("activity-1")),
            Command::ActivityResolved(ActivityResolvedRequest {
                activity_id: "activity-1".into(),
                resolution: tokeira_kernel::event::ActivityResolution::Completed {
                    result: payloads("done"),
                },
                worker_identity: None,
                now: now(),
            }),
        )
        .unwrap();
    assert!(matches!(
        transition.history_events[0].kind,
        HistoryEventKind::ActivityTaskCompleted { .. }
    ));
    assert!(transition.next_state.activities.is_empty());
    assert!(transition.dispatch_ops.is_empty());
}

#[test]
fn wft_failed_paused_workflow_no_redispatch() {
    let mut state = make_paused_state();
    state.pending_workflow_task = Some(PendingWorkflowTask {
        logical_seq: LogicalTaskSeq(3),
        scheduled_event_id: 8,
        started_event_id: Some(9),
        attempt: 1,
    });
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::WorkflowTaskFailed(WorkflowTaskFailedRequest {
                logical_seq: LogicalTaskSeq(3),
                started_event_id: 9,
                failure_cause: WorkflowTaskFailedCause::UnhandledCommand,
                failure_details: None,
                worker_identity: WorkerIdentity("worker".into()),
                now: now(),
            }),
        )
        .unwrap();
    assert!(matches!(
        transition.history_events[0].kind,
        HistoryEventKind::WorkflowTaskFailed { .. }
    ));
    assert!(transition.dispatch_ops.is_empty());
    assert_eq!(
        transition
            .next_state
            .pending_workflow_task
            .unwrap()
            .started_event_id,
        None
    );
}

#[test]
fn wft_timed_out_paused_workflow_no_redispatch() {
    let mut state = make_paused_state();
    state.pending_workflow_task = Some(PendingWorkflowTask {
        logical_seq: LogicalTaskSeq(3),
        scheduled_event_id: 8,
        started_event_id: Some(9),
        attempt: 1,
    });
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest {
                logical_seq: LogicalTaskSeq(3),
                started_event_id: 9,
                timeout_type: WorkflowTaskTimeoutType::StartToClose,
                now: now(),
            }),
        )
        .unwrap();
    assert!(matches!(
        transition.history_events[0].kind,
        HistoryEventKind::WorkflowTaskTimedOut { .. }
    ));
    assert!(transition.dispatch_ops.is_empty());
    assert_eq!(
        transition
            .next_state
            .pending_workflow_task
            .unwrap()
            .started_event_id,
        None
    );
}

#[test]
fn wft_completed_paused_workflow_no_force_wft() {
    let mut state = make_paused_state();
    state.pending_workflow_task = Some(PendingWorkflowTask {
        logical_seq: LogicalTaskSeq(3),
        scheduled_event_id: 8,
        started_event_id: Some(9),
        attempt: 1,
    });
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
                commands: vec![],
                force_new_workflow_task: true,
                now: now(),
            }),
        )
        .unwrap();
    assert!(transition.dispatch_ops.is_empty());
    assert!(transition.next_state.pending_workflow_task.is_none());
}

#[test]
fn update_activity_options_happy_path() {
    let transition = kernel()
        .apply(
            LoadedRun::Existing(make_open_state_with_activity("activity-1")),
            Command::UpdateActivityOptions(make_update_activity_options_request(
                "activity-1",
            )),
        )
        .unwrap();
    assert!(transition.history_events.is_empty());
    assert_eq!(transition.request_dedupe_ops.len(), 1);
    assert!(
        matches!(&transition.activity_ops[0], ActivityOp::Upsert(ActivityState { stamp: 1, task_queue, .. }) if *task_queue == TaskQueueName("activity-updated".into()))
    );
    let activity = transition.next_state.activities.get("activity-1").unwrap();
    assert_eq!(activity.stamp, 1);
    assert_eq!(activity.heartbeat_timeout, Some(Duration::seconds(30)));
}

#[test]
fn update_activity_options_unknown_activity() {
    let reject = kernel()
        .apply(
            LoadedRun::Existing(make_open_state()),
            Command::UpdateActivityOptions(make_update_activity_options_request(
                "missing",
            )),
        )
        .unwrap_err();
    assert_eq!(reject, Reject::UnknownActivity("missing".into()));
}

#[test]
fn pause_activity_happy_path() {
    let req = make_pause_activity_request("activity-1");
    let transition = kernel()
        .apply(
            LoadedRun::Existing(make_open_state_with_activity("activity-1")),
            Command::PauseActivity(req.clone()),
        )
        .unwrap();
    assert!(transition.history_events.is_empty());
    let activity = transition.next_state.activities.get("activity-1").unwrap();
    assert_eq!(activity.stamp, 1);
    assert!(
        matches!(&activity.pause_info, Some(ActivityPauseInfo { identity, reason, .. }) if identity == &req.identity && reason == &req.reason)
    );
}

#[test]
fn pause_activity_unknown_activity() {
    let reject = kernel()
        .apply(
            LoadedRun::Existing(make_open_state()),
            Command::PauseActivity(make_pause_activity_request("missing")),
        )
        .unwrap_err();
    assert_eq!(reject, Reject::UnknownActivity("missing".into()));
}

#[test]
fn unpause_activity_happy_path() {
    let mut state = make_open_state_with_activity("activity-1");
    if let Some(activity) = state.activities.get_mut("activity-1") {
        activity.pause_info = Some(ActivityPauseInfo {
            pause_time: now(),
            identity: "operator".into(),
            reason: "pause".into(),
        });
        activity.stamp = 1;
    }
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::UnpauseActivity(make_unpause_activity_request("activity-1")),
        )
        .unwrap();
    let activity = transition.next_state.activities.get("activity-1").unwrap();
    assert!(activity.pause_info.is_none());
    assert_eq!(activity.stamp, 2);
    assert_eq!(transition.dispatch_ops.len(), 1);
    assert!(matches!(
        transition.dispatch_ops[0],
        DispatchOp::EnqueueActivityTask { .. }
    ));
}

#[test]
fn unpause_activity_not_paused() {
    let reject = kernel()
        .apply(
            LoadedRun::Existing(make_open_state_with_activity("activity-1")),
            Command::UnpauseActivity(make_unpause_activity_request("activity-1")),
        )
        .unwrap_err();
    assert_eq!(reject, Reject::ActivityNotPaused("activity-1".into()));
}

#[test]
fn unpause_activity_unknown_activity() {
    let reject = kernel()
        .apply(
            LoadedRun::Existing(make_open_state()),
            Command::UnpauseActivity(make_unpause_activity_request("missing")),
        )
        .unwrap_err();
    assert_eq!(reject, Reject::UnknownActivity("missing".into()));
}

#[test]
fn reset_activity_happy_path() {
    let mut state = make_open_state_with_activity("activity-1");
    if let Some(activity) = state.activities.get_mut("activity-1") {
        activity.attempt = 5;
    }
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::ResetActivity(make_reset_activity_request("activity-1")),
        )
        .unwrap();
    let activity = transition.next_state.activities.get("activity-1").unwrap();
    assert_eq!(activity.attempt, 1);
    assert_eq!(activity.stamp, 1);
    assert_eq!(transition.dispatch_ops.len(), 1);
    assert!(transition.history_events.is_empty());
}

#[test]
fn reset_activity_unknown_activity() {
    let reject = kernel()
        .apply(
            LoadedRun::Existing(make_open_state()),
            Command::ResetActivity(make_reset_activity_request("missing")),
        )
        .unwrap_err();
    assert_eq!(reject, Reject::UnknownActivity("missing".into()));
}

#[test]
fn workflow_task_started_with_sticky() {
    let state = make_open_state_with_pending_wft();
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::WorkflowTaskStarted(StartWorkflowTaskRequest {
                logical_seq: LogicalTaskSeq(3),
                worker_identity: WorkerIdentity("worker-a".into()),
                sticky_ttl: Some(Duration::seconds(30)),
                now: now(),
            }),
        )
        .unwrap();

    assert!(matches!(
        transition.history_events[0].kind,
        HistoryEventKind::WorkflowTaskStarted { attempt: 1, .. }
    ));
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
                commands: vec![
                    WorkflowCommand::ScheduleActivity {
                        activity_id: "activity-1".into(),
                        activity_type: "activity-type".into(),
                        task_queue: TaskQueueName("activity-q".into()),
                        input: payloads("act"),
                        header: None,
                        retry_policy: None,
                        deployment: None,
                        build_id: None,
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
        )
        .unwrap();

    assert!(matches!(
        transition.history_events[0].kind,
        HistoryEventKind::WorkflowTaskCompleted { .. }
    ));
    assert!(transition.history_events.iter().any(|event| matches!(
        event.kind,
        HistoryEventKind::ActivityTaskScheduled { .. }
    )));
    assert!(
        transition
            .history_events
            .iter()
            .any(|event| matches!(event.kind, HistoryEventKind::TimerStarted { .. }))
    );
    assert!(
        transition
            .activity_ops
            .iter()
            .any(|op| matches!(op, tokeira_kernel::ActivityOp::Upsert(_)))
    );
    assert!(
        transition
            .timer_ops
            .iter()
            .any(|op| matches!(op, tokeira_kernel::TimerOp::Upsert(_)))
    );
    assert!(
        transition
            .dispatch_ops
            .iter()
            .any(|op| matches!(op, DispatchOp::EnqueueActivityTask { .. }))
    );
    assert!(transition.next_state.activities.contains_key("activity-1"));
    assert!(transition.next_state.timers.contains_key("timer-1"));
    assert!(transition.next_state.pending_workflow_task.is_none());
}

#[test]
fn workflow_task_completed_with_complete_workflow() {
    let state = make_open_state_with_started_wft();
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
                commands: vec![WorkflowCommand::CompleteWorkflow {
                    result: payloads("done"),
                }],
                force_new_workflow_task: false,
                now: now(),
            }),
        )
        .unwrap();

    assert!(transition.history_events.iter().any(|event| matches!(
        event.kind,
        HistoryEventKind::WorkflowExecutionCompleted { .. }
    )));
    assert_eq!(
        transition.projection_ops.last(),
        Some(&ProjectionOp::CloseExecution {
            status: ExecutionStatus::Completed,
            closed_at: now()
        })
    );
    assert_eq!(transition.next_state.status, ExecutionStatus::Completed);
    assert!(transition.next_state.closed_at.is_some());
    assert!(transition.next_state.pending_workflow_task.is_none());
    assert!(transition.next_state.sticky.is_none());
}

#[test]
fn workflow_task_completed_with_fail_workflow() {
    let state = make_open_state_with_started_wft();
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
                commands: vec![WorkflowCommand::FailWorkflow {
                    message: "nope".into(),
                    details: Some(payload("details")),
                }],
                force_new_workflow_task: false,
                now: now(),
            }),
        )
        .unwrap();

    assert!(transition.history_events.iter().any(|event| matches!(
        event.kind,
        HistoryEventKind::WorkflowExecutionFailed {
            retry_state: RetryState::InProgress,
            attempt: 1,
            ..
        }
    )));
    assert_eq!(
        transition.projection_ops.last(),
        Some(&ProjectionOp::CloseExecution {
            status: ExecutionStatus::Failed,
            closed_at: now()
        })
    );
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
                commands: vec![command.clone()],
                force_new_workflow_task: false,
                now: now(),
            }),
        )
        .unwrap();

    assert_eq!(
        transition.next_state.status,
        ExecutionStatus::ContinuedAsNew
    );
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
    let transition = kernel()
        .apply(
            LoadedRun::Existing(make_open_state()),
            Command::WorkflowExecutionTimedOut(make_timeout_request()),
        )
        .unwrap();

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
            activity_type: "activity-type".into(),
            schedule_event_id: 6,
            task_queue: TaskQueueName("activity-q".into()),
            deployment: None,
            build_id: None,
            input: Payloads::default(),
            header: None,
            attempt: 1,
            retry_policy: None,
            schedule_to_close_timeout: Some(Duration::minutes(2)),
            schedule_to_start_timeout: Some(Duration::seconds(30)),
            start_to_close_timeout: Some(Duration::minutes(1)),
            heartbeat_timeout: Some(Duration::seconds(20)),
            scheduled_at: OffsetDateTime::UNIX_EPOCH,
            started_at: None,
            started_event_id: None,
            pause_info: None,
            stamp: 0,
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
        .apply(
            LoadedRun::Existing(state),
            Command::WorkflowExecutionTimedOut(make_timeout_request()),
        )
        .unwrap();

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
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::WorkflowExecutionTimedOut(make_timeout_request()),
        )
        .unwrap();

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
                commands: vec![WorkflowCommand::FailWorkflow {
                    message: "nope".into(),
                    details: Some(payload("details")),
                }],
                force_new_workflow_task: false,
                now: now(),
            }),
        )
        .unwrap();

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
                commands: vec![WorkflowCommand::FailWorkflow {
                    message: "nope".into(),
                    details: Some(payload("details")),
                }],
                force_new_workflow_task: false,
                now: now(),
            }),
        )
        .unwrap();

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
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::ActivityResolved(ActivityResolvedRequest {
                activity_id: "activity-1".into(),
                resolution: tokeira_kernel::ActivityResolution::Completed {
                    result: payloads("done"),
                },
                worker_identity: None,
                now: now(),
            }),
        )
        .unwrap();

    assert!(transition.history_events.iter().any(|event| matches!(
        event.kind,
        HistoryEventKind::ActivityTaskCompleted { .. }
    )));
    assert!(transition.activity_ops.iter().any(|op| matches!(op, tokeira_kernel::ActivityOp::Delete { activity_id } if activity_id == "activity-1")));
    assert!(transition.history_events.iter().any(|event| matches!(
        event.kind,
        HistoryEventKind::WorkflowTaskScheduled { .. }
    )));
    assert!(
        transition
            .dispatch_ops
            .iter()
            .any(|op| matches!(op, DispatchOp::EnqueueWorkflowTask { .. }))
    );
    assert!(!transition.next_state.activities.contains_key("activity-1"));
    assert!(transition.next_state.pending_workflow_task.is_some());
}

#[test]
fn activity_resolved_timed_out_schedules_wft() {
    let state = make_open_state_with_activity("activity-1");
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::ActivityResolved(ActivityResolvedRequest {
                activity_id: "activity-1".into(),
                resolution: tokeira_kernel::ActivityResolution::TimedOut {
                    timeout_type: "HEARTBEAT".into(),
                },
                worker_identity: None,
                now: now(),
            }),
        )
        .unwrap();

    assert!(transition.history_events.iter().any(|event| matches!(
        event.kind,
        HistoryEventKind::ActivityTaskTimedOut { .. }
    )));
    assert!(transition.next_state.pending_workflow_task.is_some());
}

#[test]
fn activity_resolved_canceled_schedules_wft() {
    let state = make_open_state_with_activity("activity-1");
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
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

    assert!(transition.history_events.iter().any(|event| matches!(
        event.kind,
        HistoryEventKind::ActivityTaskCanceled { .. }
    )));
    assert!(transition.next_state.pending_workflow_task.is_some());
}

#[test]
fn timer_due_schedules_wft() {
    let state = make_open_state_with_timer("timer-1");
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::TimerDue(TimerDueRequest {
                timer_id: "timer-1".into(),
                fired_at: now(),
            }),
        )
        .unwrap();

    assert!(
        transition
            .history_events
            .iter()
            .any(|event| matches!(event.kind, HistoryEventKind::TimerFired { .. }))
    );
    assert!(
        transition
            .dispatch_ops
            .iter()
            .any(|op| matches!(op, DispatchOp::EnqueueWorkflowTask { .. }))
    );
    assert!(!transition.next_state.timers.contains_key("timer-1"));
    assert!(transition.next_state.pending_workflow_task.is_some());
}

#[test]
fn wft_failed_with_started_wft() {
    let state = make_open_state_with_started_wft_and_sticky();
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskFailed(WorkflowTaskFailedRequest {
                logical_seq: LogicalTaskSeq(3),
                started_event_id: 9,
                failure_cause: WorkflowTaskFailedCause::NonDeterminismError,
                failure_details: Some(payload("details")),
                worker_identity: WorkerIdentity("worker".into()),
                now: now(),
            }),
        )
        .unwrap();

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
            base_run_id,
            new_run_id,
            fork_event_version,
            fork_event_id,
        }
        if *logical_seq == LogicalTaskSeq(3)
            && *scheduled_event_id == 8
            && *started_event_id == 9
            && *failure_cause == WorkflowTaskFailedCause::NonDeterminismError
            && *failure_details == Some(payload("details"))
            && *identity == WorkerIdentity("worker".into())
            && base_run_id.is_none()
            && new_run_id.is_none()
            && fork_event_version.is_none()
            && fork_event_id.is_none()
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
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest {
                logical_seq: LogicalTaskSeq(3),
                started_event_id: 9,
                timeout_type: WorkflowTaskTimeoutType::StartToClose,
                now: now(),
            }),
        )
        .unwrap();

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
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::WorkflowTaskFailed(WorkflowTaskFailedRequest {
                logical_seq: LogicalTaskSeq(3),
                started_event_id: 9,
                failure_cause: WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure,
                failure_details: None,
                worker_identity: WorkerIdentity("worker".into()),
                now: now(),
            }),
        )
        .unwrap();

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
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest {
                logical_seq: LogicalTaskSeq(3),
                started_event_id: 9,
                timeout_type: WorkflowTaskTimeoutType::StartToClose,
                now: now(),
            }),
        )
        .unwrap();

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
        kernel().apply(
            LoadedRun::Existing(make_open_state()),
            Command::Start(make_start_request())
        ),
        Err(Reject::RunAlreadyExists)
    );
}

#[test]
fn reject_signal_on_absent_run() {
    assert_eq!(
        kernel().apply(
            LoadedRun::Absent,
            Command::Signal(SignalRequest {
                signal_name: "sig".into(),
                input: payloads("signal"),
                request: request_context("signal"),
                now: now(),
            })
        ),
        Err(Reject::MissingRun)
    );
}

#[test]
fn reject_signal_on_closed_run() {
    assert_eq!(
        kernel().apply(
            LoadedRun::Existing(make_closed_state()),
            Command::Signal(SignalRequest {
                signal_name: "sig".into(),
                input: payloads("signal"),
                request: request_context("signal"),
                now: now(),
            })
        ),
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
        kernel().apply(
            LoadedRun::Absent,
            Command::Terminate(make_terminate_request())
        ),
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
        kernel().apply(
            LoadedRun::Existing(make_open_state()),
            Command::WorkflowTaskStarted(StartWorkflowTaskRequest {
                logical_seq: LogicalTaskSeq(3),
                worker_identity: WorkerIdentity("worker".into()),
                sticky_ttl: None,
                now: now(),
            })
        ),
        Err(Reject::NoPendingWorkflowTask)
    );
}

#[test]
fn reject_wft_started_seq_mismatch() {
    assert_eq!(
        kernel().apply(
            LoadedRun::Existing(make_open_state_with_pending_wft()),
            Command::WorkflowTaskStarted(StartWorkflowTaskRequest {
                logical_seq: LogicalTaskSeq(4),
                worker_identity: WorkerIdentity("worker".into()),
                sticky_ttl: None,
                now: now(),
            })
        ),
        Err(Reject::WorkflowTaskSeqMismatch {
            expected: 3,
            got: 4
        })
    );
}

#[test]
fn reject_wft_started_already_started() {
    let mut started = make_open_state_with_pending_wft();
    started
        .pending_workflow_task
        .as_mut()
        .unwrap()
        .started_event_id = Some(9);
    assert_eq!(
        kernel().apply(
            LoadedRun::Existing(started),
            Command::WorkflowTaskStarted(StartWorkflowTaskRequest {
                logical_seq: LogicalTaskSeq(3),
                worker_identity: WorkerIdentity("worker".into()),
                sticky_ttl: None,
                now: now(),
            })
        ),
        Err(Reject::WorkflowTaskAlreadyStarted { logical_seq: 3 })
    );
}

#[test]
fn reject_wft_completed_no_pending() {
    assert_eq!(
        kernel().apply(
            LoadedRun::Existing(make_open_state()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                token: WorkflowTaskToken {
                    run_key: RunKey::new(),
                    logical_seq: LogicalTaskSeq(3),
                    started_event_id: 9,
                    attempt: 1,
                    shard_epoch: ShardEpoch::ZERO
                },
                identity: WorkerIdentity("worker".into()),
                commands: vec![],
                force_new_workflow_task: false,
                now: now(),
            })
        ),
        Err(Reject::NoPendingWorkflowTask)
    );
}

#[test]
fn reject_wft_completed_not_started() {
    assert_eq!(
        kernel().apply(
            LoadedRun::Existing(make_open_state_with_pending_wft()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                token: WorkflowTaskToken {
                    run_key: RunKey::new(),
                    logical_seq: LogicalTaskSeq(3),
                    started_event_id: 9,
                    attempt: 1,
                    shard_epoch: ShardEpoch::ZERO
                },
                identity: WorkerIdentity("worker".into()),
                commands: vec![],
                force_new_workflow_task: false,
                now: now(),
            })
        ),
        Err(Reject::WorkflowTaskNotStarted { logical_seq: 3 })
    );
}

#[test]
fn reject_wft_completed_seq_mismatch() {
    let state = make_open_state_with_started_wft();
    assert_eq!(
        kernel().apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(4),
                    started_event_id: 9,
                    attempt: 1,
                    shard_epoch: ShardEpoch::ZERO
                },
                identity: WorkerIdentity("worker".into()),
                commands: vec![],
                force_new_workflow_task: false,
                now: now(),
            })
        ),
        Err(Reject::WorkflowTaskSeqMismatch {
            expected: 3,
            got: 4
        })
    );
}

#[test]
fn reject_wft_completed_token_mismatch() {
    let state = make_open_state_with_started_wft();
    assert_eq!(
        kernel().apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(3),
                    started_event_id: 10,
                    attempt: 2,
                    shard_epoch: ShardEpoch::ZERO
                },
                identity: WorkerIdentity("worker".into()),
                commands: vec![],
                force_new_workflow_task: false,
                now: now(),
            })
        ),
        Err(Reject::WorkflowTaskTokenMismatch)
    );
}

#[test]
fn reject_duplicate_activity_id() {
    let state = make_open_state_with_started_wft();
    let mut with_activity = state.clone();
    with_activity.activities.insert(
        "dup".into(),
        ActivityState {
            activity_id: "dup".into(),
            activity_type: "activity-type".into(),
            schedule_event_id: 1,
            task_queue: TaskQueueName("activity-q".into()),
            deployment: None,
            build_id: None,
            input: Payloads::default(),
            header: None,
            attempt: 1,
            retry_policy: None,
            schedule_to_close_timeout: None,
            schedule_to_start_timeout: None,
            start_to_close_timeout: None,
            heartbeat_timeout: None,
            scheduled_at: OffsetDateTime::UNIX_EPOCH,
            started_at: None,
            started_event_id: None,
            pause_info: None,
            stamp: 0,
        },
    );
    assert_eq!(
        kernel().apply(
            LoadedRun::Existing(with_activity),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(3),
                    started_event_id: 9,
                    attempt: 1,
                    shard_epoch: ShardEpoch::ZERO
                },
                identity: WorkerIdentity("worker".into()),
                commands: vec![WorkflowCommand::ScheduleActivity {
                    activity_id: "dup".into(),
                    activity_type: "activity-type".into(),
                    task_queue: TaskQueueName("activity-q".into()),
                    input: payloads("a"),
                    header: None,
                    retry_policy: None,
                    deployment: None,
                    build_id: None,
                    schedule_to_close_timeout: None,
                    schedule_to_start_timeout: None,
                    start_to_close_timeout: None,
                    heartbeat_timeout: None,
                }],
                force_new_workflow_task: false,
                now: now(),
            })
        ),
        Err(Reject::DuplicateActivityId("dup".into()))
    );
}

#[test]
fn reject_duplicate_timer_id() {
    let state = make_open_state_with_started_wft();
    let mut with_timer = state.clone();
    with_timer.timers.insert(
        "dup".into(),
        TimerState {
            timer_id: "dup".into(),
            started_event_id: 1,
            fire_at: now(),
        },
    );
    assert_eq!(
        kernel().apply(
            LoadedRun::Existing(with_timer),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(3),
                    started_event_id: 9,
                    attempt: 1,
                    shard_epoch: ShardEpoch::ZERO
                },
                identity: WorkerIdentity("worker".into()),
                commands: vec![WorkflowCommand::StartTimer {
                    timer_id: "dup".into(),
                    fire_at: now()
                }],
                force_new_workflow_task: false,
                now: now(),
            })
        ),
        Err(Reject::DuplicateTimerId("dup".into()))
    );
}

#[test]
fn reject_wft_failed_absent_run() {
    assert_eq!(
        kernel().apply(
            LoadedRun::Absent,
            Command::WorkflowTaskFailed(WorkflowTaskFailedRequest {
                logical_seq: LogicalTaskSeq(3),
                started_event_id: 9,
                failure_cause: WorkflowTaskFailedCause::UnhandledCommand,
                failure_details: None,
                worker_identity: WorkerIdentity("worker".into()),
                now: now(),
            })
        ),
        Err(Reject::MissingRun)
    );
}

#[test]
fn reject_wft_failed_closed_run() {
    assert_eq!(
        kernel().apply(
            LoadedRun::Existing(make_closed_state()),
            Command::WorkflowTaskFailed(WorkflowTaskFailedRequest {
                logical_seq: LogicalTaskSeq(3),
                started_event_id: 9,
                failure_cause: WorkflowTaskFailedCause::UnhandledCommand,
                failure_details: None,
                worker_identity: WorkerIdentity("worker".into()),
                now: now(),
            })
        ),
        Err(Reject::RunClosed(ExecutionStatus::Completed))
    );
}

#[test]
fn reject_wft_failed_no_pending() {
    assert_eq!(
        kernel().apply(
            LoadedRun::Existing(make_open_state()),
            Command::WorkflowTaskFailed(WorkflowTaskFailedRequest {
                logical_seq: LogicalTaskSeq(3),
                started_event_id: 9,
                failure_cause: WorkflowTaskFailedCause::UnhandledCommand,
                failure_details: None,
                worker_identity: WorkerIdentity("worker".into()),
                now: now(),
            })
        ),
        Err(Reject::NoPendingWorkflowTask)
    );
}

#[test]
fn reject_wft_failed_not_started() {
    assert_eq!(
        kernel().apply(
            LoadedRun::Existing(make_open_state_with_pending_wft()),
            Command::WorkflowTaskFailed(WorkflowTaskFailedRequest {
                logical_seq: LogicalTaskSeq(3),
                started_event_id: 9,
                failure_cause: WorkflowTaskFailedCause::UnhandledCommand,
                failure_details: None,
                worker_identity: WorkerIdentity("worker".into()),
                now: now(),
            })
        ),
        Err(Reject::WorkflowTaskNotStarted { logical_seq: 3 })
    );
}

#[test]
fn reject_wft_failed_seq_mismatch() {
    assert_eq!(
        kernel().apply(
            LoadedRun::Existing(make_open_state_with_started_wft()),
            Command::WorkflowTaskFailed(WorkflowTaskFailedRequest {
                logical_seq: LogicalTaskSeq(4),
                started_event_id: 9,
                failure_cause: WorkflowTaskFailedCause::UnhandledCommand,
                failure_details: None,
                worker_identity: WorkerIdentity("worker".into()),
                now: now(),
            })
        ),
        Err(Reject::WorkflowTaskSeqMismatch {
            expected: 3,
            got: 4
        })
    );
}

#[test]
fn reject_wft_failed_started_event_mismatch() {
    assert_eq!(
        kernel().apply(
            LoadedRun::Existing(make_open_state_with_started_wft()),
            Command::WorkflowTaskFailed(WorkflowTaskFailedRequest {
                logical_seq: LogicalTaskSeq(3),
                started_event_id: 10,
                failure_cause: WorkflowTaskFailedCause::UnhandledCommand,
                failure_details: None,
                worker_identity: WorkerIdentity("worker".into()),
                now: now(),
            })
        ),
        Err(Reject::WorkflowTaskTokenMismatch)
    );
}

#[test]
fn reject_wft_timed_out_absent_run() {
    assert_eq!(
        kernel().apply(
            LoadedRun::Absent,
            Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest {
                logical_seq: LogicalTaskSeq(3),
                started_event_id: 9,
                timeout_type: WorkflowTaskTimeoutType::StartToClose,
                now: now(),
            })
        ),
        Err(Reject::MissingRun)
    );
}

#[test]
fn reject_wft_timed_out_closed_run() {
    assert_eq!(
        kernel().apply(
            LoadedRun::Existing(make_closed_state()),
            Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest {
                logical_seq: LogicalTaskSeq(3),
                started_event_id: 9,
                timeout_type: WorkflowTaskTimeoutType::StartToClose,
                now: now(),
            })
        ),
        Err(Reject::RunClosed(ExecutionStatus::Completed))
    );
}

#[test]
fn reject_wft_timed_out_no_pending() {
    assert_eq!(
        kernel().apply(
            LoadedRun::Existing(make_open_state()),
            Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest {
                logical_seq: LogicalTaskSeq(3),
                started_event_id: 9,
                timeout_type: WorkflowTaskTimeoutType::StartToClose,
                now: now(),
            })
        ),
        Err(Reject::NoPendingWorkflowTask)
    );
}

#[test]
fn reject_wft_timed_out_not_started() {
    assert_eq!(
        kernel().apply(
            LoadedRun::Existing(make_open_state_with_pending_wft()),
            Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest {
                logical_seq: LogicalTaskSeq(3),
                started_event_id: 9,
                timeout_type: WorkflowTaskTimeoutType::StartToClose,
                now: now(),
            })
        ),
        Err(Reject::WorkflowTaskNotStarted { logical_seq: 3 })
    );
}

#[test]
fn reject_wft_timed_out_seq_mismatch() {
    assert_eq!(
        kernel().apply(
            LoadedRun::Existing(make_open_state_with_started_wft()),
            Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest {
                logical_seq: LogicalTaskSeq(4),
                started_event_id: 9,
                timeout_type: WorkflowTaskTimeoutType::StartToClose,
                now: now(),
            })
        ),
        Err(Reject::WorkflowTaskSeqMismatch {
            expected: 3,
            got: 4
        })
    );
}

#[test]
fn reject_wft_timed_out_started_event_mismatch() {
    assert_eq!(
        kernel().apply(
            LoadedRun::Existing(make_open_state_with_started_wft()),
            Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest {
                logical_seq: LogicalTaskSeq(3),
                started_event_id: 10,
                timeout_type: WorkflowTaskTimeoutType::StartToClose,
                now: now(),
            })
        ),
        Err(Reject::WorkflowTaskTokenMismatch)
    );
}

#[test]
fn reject_unknown_activity() {
    assert_eq!(
        kernel().apply(
            LoadedRun::Existing(make_open_state()),
            Command::ActivityResolved(ActivityResolvedRequest {
                activity_id: "missing".into(),
                resolution: tokeira_kernel::ActivityResolution::Completed {
                    result: payloads("done")
                },
                worker_identity: None,
                now: now(),
            })
        ),
        Err(Reject::UnknownActivity("missing".into()))
    );
}

#[test]
fn reject_unknown_timer() {
    assert_eq!(
        kernel().apply(
            LoadedRun::Existing(make_open_state()),
            Command::TimerDue(TimerDueRequest {
                timer_id: "missing".into(),
                fired_at: now(),
            })
        ),
        Err(Reject::UnknownTimer("missing".into()))
    );
}

#[test]
fn reject_commands_after_close() {
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
                    shard_epoch: ShardEpoch::ZERO
                },
                identity: WorkerIdentity("worker".into()),
                commands: vec![
                    WorkflowCommand::CompleteWorkflow {
                        result: payloads("done")
                    },
                    WorkflowCommand::RequestNewWorkflowTask,
                ],
                force_new_workflow_task: false,
                now: now(),
            })
        ),
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
    let pending = state
        .pending_workflow_task
        .clone()
        .unwrap_or(PendingWorkflowTask {
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
    assert_eq!(
        final_transition.next_state.status,
        ExecutionStatus::Cancelled
    );
}

fn with_pending_activity_started_wft() -> WorkflowState {
    let state = make_open_state_with_started_wft();
    let mut state = state;
    state.activities.insert(
        "activity-1".into(),
        ActivityState {
            activity_id: "activity-1".into(),
            activity_type: "activity-type".into(),
            schedule_event_id: 7,
            task_queue: TaskQueueName("activity-q".into()),
            deployment: None,
            build_id: None,
            input: Payloads::default(),
            header: None,
            attempt: 1,
            retry_policy: None,
            schedule_to_close_timeout: Some(Duration::minutes(2)),
            schedule_to_start_timeout: Some(Duration::seconds(30)),
            start_to_close_timeout: Some(Duration::minutes(1)),
            heartbeat_timeout: Some(Duration::seconds(20)),
            scheduled_at: OffsetDateTime::UNIX_EPOCH,
            started_at: None,
            started_event_id: None,
            pause_info: None,
            stamp: 0,
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

fn with_child(
    mut state: WorkflowState,
    child_workflow_id: &str,
    initiated_event_id: i64,
    parent_close_policy: ParentClosePolicy,
    started: bool,
) -> WorkflowState {
    state.children.insert(
        WorkflowId(child_workflow_id.into()),
        ChildWorkflowState {
            child_workflow_id: WorkflowId(child_workflow_id.into()),
            namespace_id: state.namespace_id,
            child_run_id: started.then(RunId::new),
            initiated_event_id,
            started_event_id: started.then_some(initiated_event_id + 1),
            parent_close_policy,
        },
    );
    state
}

fn with_pending_external_signal(
    mut state: WorkflowState,
    initiated_event_id: i64,
    workflow_id: &str,
) -> WorkflowState {
    state.pending_external_signals.insert(
        initiated_event_id,
        PendingExternalSignal {
            initiated_event_id,
            target_workflow_id: WorkflowId(workflow_id.into()),
            target_run_id: Some(RunId::new()),
            signal_name: "sig".into(),
        },
    );
    state
}

fn with_pending_external_cancel(
    mut state: WorkflowState,
    initiated_event_id: i64,
    workflow_id: &str,
) -> WorkflowState {
    state.pending_external_cancels.insert(
        initiated_event_id,
        PendingExternalCancel {
            initiated_event_id,
            target_workflow_id: WorkflowId(workflow_id.into()),
            target_run_id: Some(RunId::new()),
        },
    );
    state
}

fn with_pending_update(mut state: WorkflowState, update_id: &str) -> WorkflowState {
    state.pending_updates.insert(
        update_id.into(),
        PendingUpdate {
            update_id: update_id.into(),
            accepted_event_id: 10,
            name: "handler".into(),
        },
    );
    state
}

#[test]
fn start_child_workflow_happy_path() {
    let state = make_open_state_with_started_wft();
    let pending = state.pending_workflow_task.clone().unwrap();
    let child_workflow_id = WorkflowId("child-1".into());
    let transition = kernel()
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
                commands: vec![WorkflowCommand::StartChildWorkflow {
                    child_workflow_id: child_workflow_id.clone(),
                    namespace_id: NamespaceId::new(),
                    workflow_type: WorkflowType("child-wf".into()),
                    task_queue: TaskQueueName("child-q".into()),
                    input: payloads("child-input"),
                    parent_close_policy: ParentClosePolicy::Terminate,
                }],
                force_new_workflow_task: false,
                now: now(),
            }),
        )
        .unwrap();

    assert!(matches!(
        transition.history_events[1].kind,
        HistoryEventKind::StartChildWorkflowExecutionInitiated { .. }
    ));
    assert!(matches!(
        transition.dispatch_ops[0],
        DispatchOp::StartChildWorkflow { .. }
    ));
    let child = transition
        .next_state
        .children
        .get(&child_workflow_id)
        .unwrap();
    assert_eq!(child.child_run_id, None);
    assert_eq!(child.started_event_id, None);
    assert_eq!(child.parent_close_policy, ParentClosePolicy::Terminate);
}

#[test]
fn child_start_confirmed_started_no_wft() {
    let state = with_child(
        make_open_state(),
        "child-1",
        10,
        ParentClosePolicy::Terminate,
        false,
    );
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::ChildStartConfirmed(ChildStartConfirmedRequest {
                child_workflow_id: WorkflowId("child-1".into()),
                initiated_event_id: 10,
                result: ChildStartResult::Started {
                    child_run_id: RunId::new(),
                    workflow_type: WorkflowType("child-wf".into()),
                },
                now: now(),
            }),
        )
        .unwrap();

    assert!(matches!(
        transition.history_events[0].kind,
        HistoryEventKind::ChildWorkflowExecutionStarted { .. }
    ));
    assert!(matches!(
        transition.history_events[1].kind,
        HistoryEventKind::WorkflowTaskScheduled { .. }
    ));
    assert!(
        transition
            .next_state
            .children
            .get(&WorkflowId("child-1".into()))
            .unwrap()
            .child_run_id
            .is_some()
    );
}

#[test]
fn child_resolved_completed() {
    let state = with_child(
        make_open_state(),
        "child-1",
        10,
        ParentClosePolicy::Terminate,
        true,
    );
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::ChildResolved(ChildResolvedRequest {
                child_workflow_id: WorkflowId("child-1".into()),
                resolution: ChildResolution::Completed {
                    result: payloads("done"),
                },
                now: now(),
            }),
        )
        .unwrap();

    assert!(matches!(
        transition.history_events[0].kind,
        HistoryEventKind::ChildWorkflowExecutionCompleted { .. }
    ));
    assert!(
        !transition
            .next_state
            .children
            .contains_key(&WorkflowId("child-1".into()))
    );
    assert!(matches!(
        transition.history_events[1].kind,
        HistoryEventKind::WorkflowTaskScheduled { .. }
    ));
}

#[test]
fn terminate_applies_parent_close_policy_to_started_children() {
    let state = with_child(
        make_open_state(),
        "child-1",
        10,
        ParentClosePolicy::Terminate,
        true,
    );
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::Terminate(make_terminate_request()),
        )
        .unwrap();

    assert!(transition.next_state.children.is_empty());
    assert!(transition.dispatch_ops.iter().any(|op| matches!(
        op,
        DispatchOp::TerminateChild {
            child_workflow_id,
            ..
        } if child_workflow_id == &WorkflowId("child-1".into())
    )));
}

#[test]
fn signal_external_workflow_happy_path() {
    let state = make_open_state_with_started_wft();
    let pending = state.pending_workflow_task.clone().unwrap();
    let transition = kernel()
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
                commands: vec![WorkflowCommand::SignalExternalWorkflowExecution {
                    target_namespace_id: state.namespace_id,
                    target_workflow_id: WorkflowId("target".into()),
                    target_run_id: Some(RunId::new()),
                    signal_name: "sig".into(),
                    input: payloads("payload"),
                }],
                force_new_workflow_task: false,
                now: now(),
            }),
        )
        .unwrap();

    assert!(matches!(
        transition.history_events[1].kind,
        HistoryEventKind::SignalExternalWorkflowExecutionInitiated { .. }
    ));
    assert!(matches!(
        transition.dispatch_ops[0],
        DispatchOp::SignalExternalWorkflow { .. }
    ));
    assert_eq!(transition.next_state.pending_external_signals.len(), 1);
}

#[test]
fn external_signal_resolved_signaled_no_wft() {
    let state = with_pending_external_signal(make_open_state(), 10, "target");
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::ExternalSignalResolved(ExternalSignalResolvedRequest {
                initiated_event_id: 10,
                result: ExternalSignalResult::Signaled,
                now: now(),
            }),
        )
        .unwrap();

    assert!(matches!(
        transition.history_events[0].kind,
        HistoryEventKind::ExternalWorkflowExecutionSignaled { .. }
    ));
    assert!(transition.next_state.pending_external_signals.is_empty());
    assert!(matches!(
        transition.history_events[1].kind,
        HistoryEventKind::WorkflowTaskScheduled { .. }
    ));
}

#[test]
fn request_cancel_external_workflow_happy_path() {
    let state = make_open_state_with_started_wft();
    let pending = state.pending_workflow_task.clone().unwrap();
    let transition = kernel()
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
                commands: vec![WorkflowCommand::RequestCancelExternalWorkflowExecution {
                    target_namespace_id: state.namespace_id,
                    target_workflow_id: WorkflowId("target".into()),
                    target_run_id: Some(RunId::new()),
                }],
                force_new_workflow_task: false,
                now: now(),
            }),
        )
        .unwrap();

    assert!(matches!(
        transition.history_events[1].kind,
        HistoryEventKind::RequestCancelExternalWorkflowExecutionInitiated { .. }
    ));
    assert!(matches!(
        transition.dispatch_ops[0],
        DispatchOp::RequestCancelExternalWorkflow { .. }
    ));
    assert_eq!(transition.next_state.pending_external_cancels.len(), 1);
}

#[test]
fn external_cancel_resolved_success_no_wft() {
    let state = with_pending_external_cancel(make_open_state(), 10, "target");
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::ExternalCancelResolved(ExternalCancelResolvedRequest {
                initiated_event_id: 10,
                result: ExternalCancelResult::CancelRequested,
                now: now(),
            }),
        )
        .unwrap();

    assert!(matches!(
        transition.history_events[0].kind,
        HistoryEventKind::ExternalWorkflowExecutionCancelRequested { .. }
    ));
    assert!(transition.next_state.pending_external_cancels.is_empty());
    assert!(matches!(
        transition.history_events[1].kind,
        HistoryEventKind::WorkflowTaskScheduled { .. }
    ));
}

#[test]
fn terminate_clears_pending_externals() {
    let state = with_pending_external_cancel(
        with_pending_external_signal(make_open_state(), 10, "target-sig"),
        11,
        "target-cancel",
    );
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::Terminate(make_terminate_request()),
        )
        .unwrap();

    assert!(transition.next_state.pending_external_signals.is_empty());
    assert!(transition.next_state.pending_external_cancels.is_empty());
    assert!(transition.dispatch_ops.iter().all(|op| !matches!(
        op,
        DispatchOp::SignalExternalWorkflow { .. }
            | DispatchOp::RequestCancelExternalWorkflow { .. }
    )));
}

#[test]
fn update_with_no_pending_wft() {
    let transition = kernel()
        .apply(
            LoadedRun::Existing(make_open_state()),
            Command::Update(UpdateRequest {
                update_id: "update-1".into(),
                update_name: "handler".into(),
                input: payloads("input"),
                request: request_context("update-req"),
                now: now(),
            }),
        )
        .unwrap();

    assert!(matches!(
        transition.history_events[0].kind,
        HistoryEventKind::WorkflowExecutionUpdateAccepted { .. }
    ));
    assert_eq!(transition.request_dedupe_ops.len(), 1);
    assert!(
        transition
            .next_state
            .pending_updates
            .contains_key("update-1")
    );
    assert!(matches!(
        transition.history_events[1].kind,
        HistoryEventKind::WorkflowTaskScheduled { .. }
    ));
}

#[test]
fn update_with_pending_wft() {
    let state = make_open_state_with_pending_wft();
    let pending = state.pending_workflow_task.clone();
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::Update(UpdateRequest {
                update_id: "update-1".into(),
                update_name: "handler".into(),
                input: payloads("input"),
                request: request_context("update-req"),
                now: now(),
            }),
        )
        .unwrap();

    assert!(matches!(
        transition.history_events[0].kind,
        HistoryEventKind::WorkflowExecutionUpdateAccepted { .. }
    ));
    assert_eq!(transition.dispatch_ops.len(), 0);
    assert_eq!(transition.next_state.pending_workflow_task, pending);
}

#[test]
fn update_rejected_missing_run() {
    let reject = kernel()
        .apply(
            LoadedRun::Absent,
            Command::Update(UpdateRequest {
                update_id: "update-1".into(),
                update_name: "handler".into(),
                input: payloads("input"),
                request: request_context("missing"),
                now: now(),
            }),
        )
        .unwrap_err();
    assert_eq!(reject, Reject::MissingRun);
}

#[test]
fn update_rejected_closed_run() {
    let reject = kernel()
        .apply(
            LoadedRun::Existing(make_closed_state()),
            Command::Update(UpdateRequest {
                update_id: "update-1".into(),
                update_name: "handler".into(),
                input: payloads("input"),
                request: request_context("closed"),
                now: now(),
            }),
        )
        .unwrap_err();
    assert_eq!(reject, Reject::RunClosed(ExecutionStatus::Completed));
}

#[test]
fn update_duplicate_update_id() {
    let state = with_pending_update(make_open_state(), "update-1");
    let reject = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::Update(UpdateRequest {
                update_id: "update-1".into(),
                update_name: "handler".into(),
                input: payloads("input"),
                request: request_context("dup-update"),
                now: now(),
            }),
        )
        .unwrap_err();
    assert_eq!(reject, Reject::DuplicateUpdateId("update-1".into()));
}

#[test]
fn update_completed_happy_path() {
    let state = with_pending_update(make_open_state_with_started_wft(), "update-1");
    let pending = state.pending_workflow_task.clone().unwrap();
    let transition = kernel()
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
                commands: vec![WorkflowCommand::UpdateCompleted {
                    update_id: "update-1".into(),
                    result: payloads("done"),
                }],
                force_new_workflow_task: false,
                now: now(),
            }),
        )
        .unwrap();

    assert!(matches!(
        transition.history_events[1].kind,
        HistoryEventKind::WorkflowExecutionUpdateCompleted { .. }
    ));
    assert!(
        !transition
            .next_state
            .pending_updates
            .contains_key("update-1")
    );
}

#[test]
fn update_rejected_happy_path() {
    let state = with_pending_update(make_open_state_with_started_wft(), "update-1");
    let pending = state.pending_workflow_task.clone().unwrap();
    let transition = kernel()
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
                commands: vec![WorkflowCommand::UpdateRejected {
                    update_id: "update-1".into(),
                    failure: "nope".into(),
                }],
                force_new_workflow_task: false,
                now: now(),
            }),
        )
        .unwrap();

    assert!(matches!(
        transition.history_events[1].kind,
        HistoryEventKind::WorkflowExecutionUpdateRejected { .. }
    ));
    assert!(
        !transition
            .next_state
            .pending_updates
            .contains_key("update-1")
    );
}

#[test]
fn update_completed_unknown_update() {
    let state = make_open_state_with_started_wft();
    let pending = state.pending_workflow_task.clone().unwrap();
    let reject = kernel()
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
                commands: vec![WorkflowCommand::UpdateCompleted {
                    update_id: "missing".into(),
                    result: payloads("done"),
                }],
                force_new_workflow_task: false,
                now: now(),
            }),
        )
        .unwrap_err();
    assert_eq!(reject, Reject::UnknownUpdate("missing".into()));
}

#[test]
fn update_rejected_unknown_update() {
    let state = make_open_state_with_started_wft();
    let pending = state.pending_workflow_task.clone().unwrap();
    let reject = kernel()
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
                commands: vec![WorkflowCommand::UpdateRejected {
                    update_id: "missing".into(),
                    failure: "nope".into(),
                }],
                force_new_workflow_task: false,
                now: now(),
            }),
        )
        .unwrap_err();
    assert_eq!(reject, Reject::UnknownUpdate("missing".into()));
}

#[test]
fn protocol_message_accepted_body() {
    let state = make_open_state_with_started_wft();
    let pending = state.pending_workflow_task.clone().unwrap();
    let transition = kernel()
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
                commands: vec![WorkflowCommand::ProtocolMessage {
                    message_id: "msg-1".into(),
                    body: UpdateProtocolBody::Accepted {
                        update_id: "update-1".into(),
                        update_name: "handler".into(),
                        input: payloads("input"),
                    },
                }],
                force_new_workflow_task: false,
                now: now(),
            }),
        )
        .unwrap();

    assert!(matches!(
        transition.history_events[1].kind,
        HistoryEventKind::WorkflowExecutionUpdateAccepted { .. }
    ));
    assert!(
        transition
            .next_state
            .pending_updates
            .contains_key("update-1")
    );
}

#[test]
fn protocol_message_completed_body() {
    let state = with_pending_update(make_open_state_with_started_wft(), "update-1");
    let pending = state.pending_workflow_task.clone().unwrap();
    let transition = kernel()
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
                commands: vec![WorkflowCommand::ProtocolMessage {
                    message_id: "msg-2".into(),
                    body: UpdateProtocolBody::Completed {
                        update_id: "update-1".into(),
                        result: payloads("done"),
                    },
                }],
                force_new_workflow_task: false,
                now: now(),
            }),
        )
        .unwrap();
    assert!(matches!(
        transition.history_events[1].kind,
        HistoryEventKind::WorkflowExecutionUpdateCompleted { .. }
    ));
    assert!(
        !transition
            .next_state
            .pending_updates
            .contains_key("update-1")
    );
}

#[test]
fn protocol_message_rejected_body() {
    let state = with_pending_update(make_open_state_with_started_wft(), "update-1");
    let pending = state.pending_workflow_task.clone().unwrap();
    let transition = kernel()
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
                commands: vec![WorkflowCommand::ProtocolMessage {
                    message_id: "msg-3".into(),
                    body: UpdateProtocolBody::Rejected {
                        update_id: "update-1".into(),
                        failure: "nope".into(),
                    },
                }],
                force_new_workflow_task: false,
                now: now(),
            }),
        )
        .unwrap();
    assert!(matches!(
        transition.history_events[1].kind,
        HistoryEventKind::WorkflowExecutionUpdateRejected { .. }
    ));
    assert!(
        !transition
            .next_state
            .pending_updates
            .contains_key("update-1")
    );
}

#[test]
fn terminate_clears_pending_updates() {
    let state = with_pending_update(make_open_state(), "update-1");
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::Terminate(make_terminate_request()),
        )
        .unwrap();
    assert!(transition.next_state.pending_updates.is_empty());
}

#[test]
fn complete_workflow_clears_pending_updates() {
    let state = with_pending_update(make_open_state_with_started_wft(), "update-1");
    let pending = state.pending_workflow_task.clone().unwrap();
    let transition = kernel()
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
                commands: vec![WorkflowCommand::CompleteWorkflow {
                    result: payloads("done"),
                }],
                force_new_workflow_task: false,
                now: now(),
            }),
        )
        .unwrap();
    assert!(transition.next_state.pending_updates.is_empty());
}

#[test]
fn record_marker_happy_path() {
    let state = make_open_state_with_started_wft();
    let pending = state.pending_workflow_task.clone().unwrap();
    let details = BTreeMap::from([("side-effect".into(), payloads("value"))]);
    let header = Some(BTreeMap::from([("encoding".into(), payload("json"))]));
    let transition = kernel()
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
                commands: vec![WorkflowCommand::RecordMarker {
                    marker_name: "marker".into(),
                    details: details.clone(),
                    failure: Some(payload("failure")),
                    header: header.clone(),
                }],
                force_new_workflow_task: false,
                now: now(),
            }),
        )
        .unwrap();

    assert_eq!(transition.history_events.len(), 2);
    assert!(matches!(
        &transition.history_events[1].kind,
        HistoryEventKind::MarkerRecorded {
            marker_name,
            details: event_details,
            failure: Some(_),
            header: event_header,
        } if marker_name == "marker" && event_details == &details && event_header == &header
    ));
    assert!(transition.next_state.is_open());
    assert!(transition.dispatch_ops.is_empty());
    assert!(transition.projection_ops.is_empty());
    assert!(transition.request_dedupe_ops.is_empty());
}

#[test]
fn record_marker_after_close_rejected() {
    let state = make_open_state_with_started_wft();
    let pending = state.pending_workflow_task.clone().unwrap();
    let reject = kernel()
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
                commands: vec![
                    WorkflowCommand::CompleteWorkflow {
                        result: payloads("done"),
                    },
                    WorkflowCommand::RecordMarker {
                        marker_name: "marker".into(),
                        details: BTreeMap::new(),
                        failure: None,
                        header: None,
                    },
                ],
                force_new_workflow_task: false,
                now: now(),
            }),
        )
        .unwrap_err();

    assert_eq!(reject, Reject::CommandsAfterClose { index: 1 });
}

#[test]
fn update_execution_options_happy_path() {
    let state = make_open_state();
    let req = UpdateExecutionOptionsRequest {
        versioning_override: FieldChange::Set(VersioningOverride),
        completion_callbacks: FieldChange::Set(vec![CompletionCallback]),
        attached_request_id: Some("attached-1".into()),
        request: request_context("options-req"),
        now: now(),
    };
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::UpdateExecutionOptions(req.clone()),
        )
        .unwrap();

    assert_eq!(transition.request_dedupe_ops.len(), 1);
    assert!(matches!(
        &transition.history_events[0].kind,
        HistoryEventKind::WorkflowExecutionOptionsUpdated {
            versioning_override,
            completion_callbacks,
            attached_request_id,
        } if versioning_override == &req.versioning_override
            && completion_callbacks == &req.completion_callbacks
            && attached_request_id == &req.attached_request_id
    ));
    assert_eq!(
        transition.next_state.versioning_override,
        Some(VersioningOverride)
    );
    assert_eq!(
        transition.next_state.completion_callbacks,
        vec![CompletionCallback]
    );
    assert!(transition.dispatch_ops.is_empty());
    assert!(transition.next_state.is_open());
}

#[test]
fn update_execution_options_clear_versioning() {
    let state = with_execution_options(make_open_state());
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::UpdateExecutionOptions(UpdateExecutionOptionsRequest {
                versioning_override: FieldChange::Clear,
                completion_callbacks: FieldChange::Unchanged,
                attached_request_id: None,
                request: request_context("options-clear"),
                now: now(),
            }),
        )
        .unwrap();

    assert_eq!(transition.next_state.versioning_override, None);
    assert_eq!(
        transition.next_state.completion_callbacks,
        vec![CompletionCallback]
    );
}

#[test]
fn update_execution_options_missing_run() {
    let reject = kernel()
        .apply(
            LoadedRun::Absent,
            Command::UpdateExecutionOptions(UpdateExecutionOptionsRequest {
                versioning_override: FieldChange::Unchanged,
                completion_callbacks: FieldChange::Unchanged,
                attached_request_id: None,
                request: request_context("options-missing"),
                now: now(),
            }),
        )
        .unwrap_err();

    assert_eq!(reject, Reject::MissingRun);
}

#[test]
fn update_execution_options_closed_run() {
    let reject = kernel()
        .apply(
            LoadedRun::Existing(make_closed_state()),
            Command::UpdateExecutionOptions(UpdateExecutionOptionsRequest {
                versioning_override: FieldChange::Unchanged,
                completion_callbacks: FieldChange::Unchanged,
                attached_request_id: None,
                request: request_context("options-closed"),
                now: now(),
            }),
        )
        .unwrap_err();

    assert_eq!(reject, Reject::RunClosed(ExecutionStatus::Completed));
}

#[test]
fn close_preserves_execution_options() {
    let state = with_execution_options(make_open_state());
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::Terminate(make_terminate_request()),
        )
        .unwrap();

    assert_eq!(
        transition.next_state.versioning_override,
        Some(VersioningOverride)
    );
    assert_eq!(
        transition.next_state.completion_callbacks,
        vec![CompletionCallback]
    );
}

#[test]
fn schedule_nexus_operation_happy_path() {
    let state = make_open_state_with_started_wft();
    let pending = state.pending_workflow_task.clone().unwrap();
    let transition = kernel()
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
                commands: vec![WorkflowCommand::ScheduleNexusOperation {
                    operation_id: "op-1".into(),
                    endpoint: "endpoint".into(),
                    service: "service".into(),
                    operation: "method".into(),
                    input: payloads("input"),
                    schedule_to_close_timeout: Some(Duration::seconds(30)),
                }],
                force_new_workflow_task: false,
                now: now(),
            }),
        )
        .unwrap();

    assert!(matches!(
        &transition.history_events[1].kind,
        HistoryEventKind::NexusOperationScheduled { operation_id, endpoint, service, operation, .. }
            if operation_id == "op-1" && endpoint == "endpoint" && service == "service" && operation == "method"
    ));
    let pending_nexus = transition
        .next_state
        .pending_nexus_operations
        .get("op-1")
        .unwrap();
    assert_eq!(pending_nexus.scheduled_event_id, 11);
    assert_eq!(pending_nexus.endpoint, "endpoint");
    assert_eq!(pending_nexus.service, "service");
    assert_eq!(pending_nexus.operation, "method");
    assert!(!pending_nexus.started);
    assert!(transition.dispatch_ops.iter().any(|op| matches!(op, DispatchOp::ScheduleNexusOperation { operation_id, .. } if operation_id == "op-1")));
}

#[test]
fn schedule_nexus_operation_duplicate_rejected() {
    let state = with_pending_nexus_operation(make_open_state_with_started_wft(), "op-1");
    let pending = state.pending_workflow_task.clone().unwrap();
    let reject = kernel()
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
                commands: vec![WorkflowCommand::ScheduleNexusOperation {
                    operation_id: "op-1".into(),
                    endpoint: "endpoint".into(),
                    service: "service".into(),
                    operation: "method".into(),
                    input: payloads("input"),
                    schedule_to_close_timeout: None,
                }],
                force_new_workflow_task: false,
                now: now(),
            }),
        )
        .unwrap_err();

    assert_eq!(reject, Reject::DuplicateNexusOperationId("op-1".into()));
}

#[test]
fn cancel_nexus_operation_happy_path() {
    let state = with_pending_nexus_operation(make_open_state_with_started_wft(), "op-1");
    let pending = state.pending_workflow_task.clone().unwrap();
    let transition = kernel()
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
                commands: vec![WorkflowCommand::CancelNexusOperation {
                    scheduled_event_id: 12,
                }],
                force_new_workflow_task: false,
                now: now(),
            }),
        )
        .unwrap();

    assert!(matches!(
        transition.history_events[1].kind,
        HistoryEventKind::NexusOperationCancelRequested {
            scheduled_event_id: 12
        }
    ));
    assert!(transition.dispatch_ops.iter().any(|op| matches!(
        op,
        DispatchOp::CancelNexusOperation {
            scheduled_event_id: 12,
            originator_run_key,
            operation_id,
            endpoint,
            service,
        } if originator_run_key == &state.run_key
            && operation_id == "op-1"
            && endpoint == "endpoint"
            && service == "service"
    )));
    assert!(
        transition
            .next_state
            .pending_nexus_operations
            .contains_key("op-1")
    );
}

#[test]
fn cancel_nexus_operation_unknown() {
    let state = make_open_state_with_started_wft();
    let pending = state.pending_workflow_task.clone().unwrap();
    let reject = kernel()
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
                commands: vec![WorkflowCommand::CancelNexusOperation {
                    scheduled_event_id: 12,
                }],
                force_new_workflow_task: false,
                now: now(),
            }),
        )
        .unwrap_err();

    assert_eq!(
        reject,
        Reject::UnknownNexusOperation("scheduled_event_id=12".into())
    );
}

#[test]
fn nexus_operation_resolved_started() {
    let state = with_pending_nexus_operation(make_open_state(), "op-1");
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::NexusOperationResolved(NexusOperationResolvedRequest {
                operation_id: "op-1".into(),
                scheduled_event_id: 12,
                resolution: NexusResolution::Started,
                now: now(),
            }),
        )
        .unwrap();

    assert!(matches!(
        transition.history_events[0].kind,
        HistoryEventKind::NexusOperationStarted {
            scheduled_event_id: 12,
            ..
        }
    ));
    assert!(
        transition
            .next_state
            .pending_nexus_operations
            .get("op-1")
            .unwrap()
            .started
    );
    assert!(transition.next_state.pending_workflow_task.is_none());
    assert!(transition.request_dedupe_ops.is_empty());
}

#[test]
fn nexus_operation_resolved_started_duplicate() {
    let state = with_started_nexus_operation(
        with_pending_nexus_operation(make_open_state(), "op-1"),
        "op-1",
    );
    let reject = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::NexusOperationResolved(NexusOperationResolvedRequest {
                operation_id: "op-1".into(),
                scheduled_event_id: 12,
                resolution: NexusResolution::Started,
                now: now(),
            }),
        )
        .unwrap_err();

    assert_eq!(reject, Reject::NexusOperationAlreadyStarted("op-1".into()));
}

#[test]
fn nexus_operation_resolved_completed() {
    let state = with_pending_nexus_operation(make_open_state(), "op-1");
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::NexusOperationResolved(NexusOperationResolvedRequest {
                operation_id: "op-1".into(),
                scheduled_event_id: 12,
                resolution: NexusResolution::Completed {
                    result: payloads("done"),
                },
                now: now(),
            }),
        )
        .unwrap();

    assert!(matches!(
        transition.history_events[0].kind,
        HistoryEventKind::NexusOperationCompleted {
            scheduled_event_id: 12,
            ..
        }
    ));
    assert!(
        !transition
            .next_state
            .pending_nexus_operations
            .contains_key("op-1")
    );
    assert!(transition.next_state.pending_workflow_task.is_some());
}

#[test]
fn nexus_operation_resolved_completed_with_pending_wft() {
    let state = with_pending_nexus_operation(make_open_state_with_pending_wft(), "op-1");
    let existing = state.pending_workflow_task.clone();
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::NexusOperationResolved(NexusOperationResolvedRequest {
                operation_id: "op-1".into(),
                scheduled_event_id: 12,
                resolution: NexusResolution::Completed {
                    result: payloads("done"),
                },
                now: now(),
            }),
        )
        .unwrap();
    assert_eq!(transition.next_state.pending_workflow_task, existing);
    assert_eq!(
        transition
            .dispatch_ops
            .iter()
            .filter(|op| matches!(op, DispatchOp::EnqueueWorkflowTask { .. }))
            .count(),
        0
    );
}

#[test]
fn nexus_operation_resolved_failed() {
    let state = with_pending_nexus_operation(make_open_state(), "op-1");
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::NexusOperationResolved(NexusOperationResolvedRequest {
                operation_id: "op-1".into(),
                scheduled_event_id: 12,
                resolution: NexusResolution::Failed {
                    failure: "nope".into(),
                },
                now: now(),
            }),
        )
        .unwrap();
    assert!(matches!(
        transition.history_events[0].kind,
        HistoryEventKind::NexusOperationFailed {
            scheduled_event_id: 12,
            ..
        }
    ));
    assert!(transition.next_state.pending_workflow_task.is_some());
}

#[test]
fn nexus_operation_resolved_canceled() {
    let state = with_pending_nexus_operation(make_open_state(), "op-1");
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::NexusOperationResolved(NexusOperationResolvedRequest {
                operation_id: "op-1".into(),
                scheduled_event_id: 12,
                resolution: NexusResolution::Canceled,
                now: now(),
            }),
        )
        .unwrap();
    assert!(matches!(
        transition.history_events[0].kind,
        HistoryEventKind::NexusOperationCanceled {
            scheduled_event_id: 12,
            ..
        }
    ));
    assert!(transition.next_state.pending_workflow_task.is_some());
}

#[test]
fn nexus_operation_resolved_timed_out() {
    let state = with_pending_nexus_operation(make_open_state(), "op-1");
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::NexusOperationResolved(NexusOperationResolvedRequest {
                operation_id: "op-1".into(),
                scheduled_event_id: 12,
                resolution: NexusResolution::TimedOut,
                now: now(),
            }),
        )
        .unwrap();
    assert!(matches!(
        transition.history_events[0].kind,
        HistoryEventKind::NexusOperationTimedOut {
            scheduled_event_id: 12,
            ..
        }
    ));
    assert!(transition.next_state.pending_workflow_task.is_some());
}

#[test]
fn nexus_operation_resolved_unknown_operation() {
    let reject = kernel()
        .apply(
            LoadedRun::Existing(make_open_state()),
            Command::NexusOperationResolved(NexusOperationResolvedRequest {
                operation_id: "missing".into(),
                scheduled_event_id: 12,
                resolution: NexusResolution::Started,
                now: now(),
            }),
        )
        .unwrap_err();
    assert_eq!(reject, Reject::UnknownNexusOperation("missing".into()));
}

#[test]
fn nexus_operation_resolved_stale() {
    let state = with_pending_nexus_operation(make_open_state(), "op-1");
    let reject = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::NexusOperationResolved(NexusOperationResolvedRequest {
                operation_id: "op-1".into(),
                scheduled_event_id: 99,
                resolution: NexusResolution::Started,
                now: now(),
            }),
        )
        .unwrap_err();
    assert_eq!(
        reject,
        Reject::StaleNexusResolution {
            operation_id: "op-1".into(),
            expected_scheduled_event_id: 12,
        }
    );
}

#[test]
fn nexus_operation_resolved_absent_run() {
    let reject = kernel()
        .apply(
            LoadedRun::Absent,
            Command::NexusOperationResolved(NexusOperationResolvedRequest {
                operation_id: "op-1".into(),
                scheduled_event_id: 12,
                resolution: NexusResolution::Started,
                now: now(),
            }),
        )
        .unwrap_err();
    assert_eq!(reject, Reject::MissingRun);
}

#[test]
fn nexus_operation_resolved_closed_run() {
    let reject = kernel()
        .apply(
            LoadedRun::Existing(make_closed_state()),
            Command::NexusOperationResolved(NexusOperationResolvedRequest {
                operation_id: "op-1".into(),
                scheduled_event_id: 12,
                resolution: NexusResolution::Started,
                now: now(),
            }),
        )
        .unwrap_err();
    assert_eq!(reject, Reject::RunClosed(ExecutionStatus::Completed));
}

#[test]
fn terminate_clears_pending_nexus_operations() {
    let state = with_pending_nexus_operation(make_open_state(), "op-1");
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::Terminate(make_terminate_request()),
        )
        .unwrap();
    assert!(transition.next_state.pending_nexus_operations.is_empty());
    assert_eq!(
        transition
            .dispatch_ops
            .iter()
            .filter(|op| matches!(
                op,
                DispatchOp::ScheduleNexusOperation { .. }
                    | DispatchOp::CancelNexusOperation { .. }
            ))
            .count(),
        0
    );
}

#[test]
fn close_via_complete_clears_pending_nexus_operations() {
    let state = with_pending_nexus_operation(make_open_state_with_started_wft(), "op-1");
    let pending = state.pending_workflow_task.clone().unwrap();
    let transition = kernel()
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
                commands: vec![WorkflowCommand::CompleteWorkflow {
                    result: payloads("done"),
                }],
                force_new_workflow_task: false,
                now: now(),
            }),
        )
        .unwrap();
    assert!(transition.next_state.pending_nexus_operations.is_empty());
    assert_eq!(
        transition
            .dispatch_ops
            .iter()
            .filter(|op| matches!(
                op,
                DispatchOp::ScheduleNexusOperation { .. }
                    | DispatchOp::CancelNexusOperation { .. }
            ))
            .count(),
        0
    );
}
