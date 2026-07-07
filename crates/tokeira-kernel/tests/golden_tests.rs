use std::collections::BTreeMap;

use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    ActivityOp, ActivityPauseInfo, ActivityResolvedRequest, ActivityState, BasicKernel,
    CallbackAttemptOutcome, CallbackCompletionOutcome, CallbackSpec, CallbackState,
    CallbackTrigger, CancelRequest, ChildResolution, ChildResolvedRequest,
    ChildStartConfirmedRequest, ChildStartResult, ChildWorkflowState, Command, CompletionCallback,
    CompletionCallbackAttemptedRequest, DispatchOp, ExternalCancelResolvedRequest,
    ExternalCancelResult, ExternalSignalResolvedRequest, ExternalSignalResult,
    ExternalWorkflowExecution, FieldChange, LoadedRun, NexusOperationResolvedRequest,
    NexusOperationRetryRequest, NexusResolution, NexusTimeoutType, ParentClosePolicy,
    PauseActivityRequest, PauseInfo, PauseWorkflowRequest, PendingExternalCancel,
    PendingExternalSignal, PendingNexusOperation, PendingUpdate, PendingWorkflowTask, ProjectionOp,
    Reject, ReplayContext, ResetActivityRequest, ResetRequest, RetryState, SignalRequest,
    SignalWithStartRequest, StartDeploymentTransitionRequest, StartRequest,
    StartWorkflowTaskRequest, TerminateOnWorkflowTaskFailedRequest, TerminateRequest,
    TimerDueRequest, TimerState, Transition, UnpauseActivityRequest, UnpauseWorkflowRequest,
    UpdateActivityOptionsRequest, UpdateExecutionOptionsRequest, UpdateProtocolBody, UpdateRequest,
    VersioningBehavior, VersioningOverride, WORKFLOW_START_DELAY_TIMER_ID,
    WorkerDeploymentVersionRef, WorkflowCommand, WorkflowExecutionTimedOutRequest,
    WorkflowStartDelayElapsedRequest, WorkflowState, WorkflowTaskCompletedRequest,
    WorkflowTaskFailedCause, WorkflowTaskFailedRequest, WorkflowTaskTimedOutRequest,
    WorkflowTaskTimeoutType, WorkflowTimeoutType,
    event::{HistoryEvent, HistoryEventKind},
    kernel::Kernel,
};
use tokeira_types::{
    ExecutionStatus, Headers, LogicalTaskSeq, Memo, NamespaceId, Payload, Payloads, RequestContext,
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

fn completion_callback() -> CompletionCallback {
    CompletionCallback {
        spec: CallbackSpec::Nexus {
            url: "https://callback.example/run".into(),
            header: BTreeMap::new(),
        },
        links: Vec::new(),
        trigger: CallbackTrigger::WorkflowClosed,
        registration_time: None,
        state: CallbackState::Standby,
        attempt: 0,
        last_attempt_failure: None,
        next_attempt_at: None,
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
        versioning_override: None,
        workflow_start_delay: None,
        completion_callbacks: Vec::new(),
        user_metadata: None,
        links: Vec::new(),
        on_conflict_options: None,
        priority: None,
        input: payloads("start-input"),
        header: None,
        memo: memo(),
        search_attributes: search_attributes(),
        workflow_execution_timeout: Some(Duration::minutes(10)),
        workflow_run_timeout: Some(Duration::minutes(5)),
        workflow_task_timeout: Duration::seconds(10),
        retry_policy: Some(retry_policy()),
        conflict_policy: tokeira_kernel::WorkflowIdConflictPolicy::Fail,
        reuse_policy: tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
        attempt: 1,
        continued_execution_run_id: None,
        first_execution_run_id: Some(run_id),
        parent_run_key: None,
        parent_workflow_id: None,
        parent_run_id: None,
        parent_namespace_id: None,
        parent_namespace_name: None,
        parent_initiated_event_id: 0,
        root_workflow_id: None,
        root_run_id: None,
        original_execution_run_id: Some(run_id),
        continued_failure: None,
        last_completion_result: None,
        first_run_started_at: Some(now()),
        request: request_context("start-req"),
        now: now(),
        client_cron_schedule: None,
        cron_schedule: None,
        reserved_poller_identity: None,
    }
}

fn make_signal_with_start_request() -> SignalWithStartRequest {
    let start = make_start_request();
    SignalWithStartRequest {
        run_key: start.run_key,
        namespace_id: start.namespace_id,
        workflow_id: start.workflow_id,
        run_id: start.run_id,
        workflow_type: start.workflow_type,
        task_queue: start.task_queue,
        deployment: start.deployment,
        build_id: start.build_id,
        versioning_override: start.versioning_override,
        header: start.header,
        workflow_start_delay: start.workflow_start_delay,
        user_metadata: start.user_metadata,
        links: start.links,
        priority: start.priority,
        client_cron_schedule: start.client_cron_schedule,
        cron_schedule: start.cron_schedule,
        input: start.input,
        memo: start.memo,
        search_attributes: start.search_attributes,
        workflow_execution_timeout: start.workflow_execution_timeout,
        workflow_run_timeout: start.workflow_run_timeout,
        workflow_task_timeout: start.workflow_task_timeout,
        retry_policy: start.retry_policy,
        conflict_policy: start.conflict_policy,
        reuse_policy: start.reuse_policy,
        attempt: start.attempt,
        continued_execution_run_id: start.continued_execution_run_id,
        first_execution_run_id: start.first_execution_run_id,
        parent_run_key: start.parent_run_key,
        parent_workflow_id: start.parent_workflow_id,
        parent_run_id: start.parent_run_id,
        parent_namespace_id: start.parent_namespace_id,
        parent_namespace_name: None,
        parent_initiated_event_id: start.parent_initiated_event_id,
        root_workflow_id: start.root_workflow_id,
        root_run_id: start.root_run_id,
        original_execution_run_id: start.original_execution_run_id,
        continued_failure: start.continued_failure,
        last_completion_result: start.last_completion_result,
        first_run_started_at: start.first_run_started_at,
        request: start.request,
        now: start.now,
        signal_name: "sig".into(),
        signal_input: payloads("signal-input"),
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
        versioning_info: None,
        worker_deployment_name: None,
        status: ExecutionStatus::Running,
        transition_seq: TransitionSeq(5),
        last_event_id: 9,
        next_workflow_task_seq: LogicalTaskSeq(4),
        pending_workflow_task: None,
        previous_started_event_id: 0,
        workflow_task_attempt: 1,
        sticky: None,
        pause_info: None,
        cancel_requested: false,
        wft_stamp: 0,
        memo: memo(),
        search_attributes: search_attributes(),
        workflow_execution_timeout: Some(Duration::minutes(10)),
        workflow_run_timeout: Some(Duration::minutes(5)),
        workflow_task_timeout: Duration::seconds(10),
        retry_policy: Some(retry_policy()),
        attempt: 1,
        first_execution_run_id: Some(RunId::new()),
        original_execution_run_id: None,
        parent_run_key: None,
        parent_workflow_id: None,
        parent_run_id: None,
        parent_namespace_id: None,
        parent_namespace_name: None,
        parent_initiated_event_id: 0,
        root_workflow_id: None,
        root_run_id: None,
        last_completion_result: None,
        activities: BTreeMap::new(),
        timers: BTreeMap::new(),
        children: BTreeMap::new(),
        pending_external_signals: BTreeMap::new(),
        pending_external_cancels: BTreeMap::new(),
        pending_updates: BTreeMap::new(),
        admitted_updates: std::collections::HashSet::new(),
        pending_nexus_operations: BTreeMap::new(),
        completion_callbacks: Vec::new(),
        user_metadata: None,
        links: Vec::new(),
        workflow_start_delay: None,
        priority: None,
        started_at: now() - Duration::minutes(3),
        first_run_started_at: Some(now() - Duration::minutes(3)),
        closed_at: None,
        close_result: None,
        close_failure: None,
        request_id_infos: std::collections::BTreeMap::new(),
        buffered_events: Vec::new(),
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
                header: start.header.clone(),
                workflow_start_delay: start.workflow_start_delay,
                completion_callbacks: start.completion_callbacks.clone(),
                user_metadata: start.user_metadata.clone(),
                links: start.links.clone(),
                identity: start.request.caller_identity.clone().unwrap_or_default(),
                continued_execution_run_id: start.continued_execution_run_id,
                first_execution_run_id: start.first_execution_run_id,
                retry_policy: start.retry_policy.clone(),
                attempt: start.attempt,
                workflow_execution_timeout: start.workflow_execution_timeout,
                workflow_run_timeout: start.workflow_run_timeout,
                workflow_task_timeout: start.workflow_task_timeout,
                parent_workflow_id: start.parent_workflow_id.clone(),
                parent_run_id: start.parent_run_id,
                parent_namespace_id: start.parent_namespace_id,
                parent_namespace_name: None,
                parent_initiated_event_id: start.parent_initiated_event_id,
                root_workflow_id: start.root_workflow_id.clone(),
                root_run_id: start.root_run_id,
                original_execution_run_id: start.original_execution_run_id,
                continued_failure: start.continued_failure.clone(),
                last_completion_result: start.last_completion_result.clone(),
                cron_schedule: start.cron_schedule.clone(),
                versioning_info: None,
                worker_deployment_name: None,
                priority: start.priority.clone(),
            },
        ),
        history_event(
            2,
            started_at,
            HistoryEventKind::WorkflowTaskScheduled {
                logical_seq: LogicalTaskSeq::ONE,
                task_queue: start.task_queue.clone(),
                workflow_task_timeout: start.workflow_task_timeout,
                attempt: 1,
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
                request_id: "wft-start".into(),
                history_size_bytes: 0,
                suggest_continue_as_new: false,
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
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
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
                header: start.header.clone(),
                workflow_start_delay: start.workflow_start_delay,
                completion_callbacks: start.completion_callbacks.clone(),
                user_metadata: start.user_metadata.clone(),
                links: start.links.clone(),
                identity: start.request.caller_identity.clone().unwrap_or_default(),
                continued_execution_run_id: start.continued_execution_run_id,
                first_execution_run_id: start.first_execution_run_id,
                retry_policy: start.retry_policy.clone(),
                attempt: start.attempt,
                workflow_execution_timeout: start.workflow_execution_timeout,
                workflow_run_timeout: start.workflow_run_timeout,
                workflow_task_timeout: start.workflow_task_timeout,
                parent_workflow_id: start.parent_workflow_id.clone(),
                parent_run_id: start.parent_run_id,
                parent_namespace_id: start.parent_namespace_id,
                parent_namespace_name: None,
                parent_initiated_event_id: start.parent_initiated_event_id,
                root_workflow_id: start.root_workflow_id.clone(),
                root_run_id: start.root_run_id,
                original_execution_run_id: start.original_execution_run_id,
                continued_failure: start.continued_failure.clone(),
                last_completion_result: start.last_completion_result.clone(),
                cron_schedule: start.cron_schedule.clone(),
                versioning_info: None,
                worker_deployment_name: None,
                priority: start.priority.clone(),
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
                workflow_task_completed_event_id: 4,
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
                request_id: "activity-start".into(),
                last_failure: None,
            },
        ),
        history_event(
            4,
            t0,
            HistoryEventKind::TimerStarted {
                timer_id: "t1".into(),
                fire_at: t0 + Duration::minutes(1),
                workflow_task_completed_event_id: 4,
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
                header: start.header.clone(),
                workflow_start_delay: start.workflow_start_delay,
                completion_callbacks: start.completion_callbacks.clone(),
                user_metadata: start.user_metadata.clone(),
                links: start.links.clone(),
                identity: start.request.caller_identity.clone().unwrap_or_default(),
                continued_execution_run_id: start.continued_execution_run_id,
                first_execution_run_id: start.first_execution_run_id,
                retry_policy: start.retry_policy.clone(),
                attempt: start.attempt,
                workflow_execution_timeout: start.workflow_execution_timeout,
                workflow_run_timeout: start.workflow_run_timeout,
                workflow_task_timeout: start.workflow_task_timeout,
                parent_workflow_id: start.parent_workflow_id.clone(),
                parent_run_id: start.parent_run_id,
                parent_namespace_id: start.parent_namespace_id,
                parent_namespace_name: None,
                parent_initiated_event_id: start.parent_initiated_event_id,
                root_workflow_id: start.root_workflow_id.clone(),
                root_run_id: start.root_run_id,
                original_execution_run_id: start.original_execution_run_id,
                continued_failure: start.continued_failure.clone(),
                last_completion_result: start.last_completion_result.clone(),
                cron_schedule: start.cron_schedule.clone(),
                versioning_info: None,
                worker_deployment_name: None,
                priority: start.priority.clone(),
            },
        ),
        history_event(
            2,
            t0,
            HistoryEventKind::WorkflowExecutionOptionsUpdated {
                versioning_override: FieldChange::Set(VersioningOverride::AutoUpgrade),
                completion_callbacks: FieldChange::Set(vec![completion_callback()]),
                attached_completion_callbacks: Vec::new(),
                attached_links: Vec::new(),
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
    assert_eq!(
        state.versioning_override().cloned(),
        Some(VersioningOverride::AutoUpgrade)
    );
    assert_eq!(state.completion_callbacks, vec![completion_callback()]);
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
                task_queue: start.task_queue.clone(),
                workflow_task_timeout: start.workflow_task_timeout,
                attempt: 1,
            },
        )],
    );
    assert_eq!(invalid, Err(Reject::InvalidReplayHistory));
}

fn make_open_state_with_pending_wft() -> WorkflowState {
    let mut state = make_open_state();
    state.pending_workflow_task = Some(PendingWorkflowTask {
        task_type: tokeira_kernel::WorkflowTaskType::Normal,
        schedule_to_start_deadline: None,
        logical_seq: LogicalTaskSeq(3),
        scheduled_event_id: 8,
        scheduled_at: now(),
        started_event_id: None,
        started_at: None,
        attempt: state.workflow_task_attempt,
    });
    state
}

fn make_open_state_with_started_wft() -> WorkflowState {
    let mut state = make_open_state();
    state.pending_workflow_task = Some(PendingWorkflowTask {
        task_type: tokeira_kernel::WorkflowTaskType::Normal,
        schedule_to_start_deadline: None,
        logical_seq: LogicalTaskSeq(3),
        scheduled_event_id: 8,
        scheduled_at: now(),
        started_event_id: Some(9),
        started_at: Some(now()),
        attempt: 1,
    });
    state
}

fn make_open_state_with_started_wft_and_sticky() -> WorkflowState {
    let mut state = make_open_state_with_started_wft();
    state.sticky = Some(StickyAffinity {
        sticky_queue: tokeira_types::TaskQueueName(String::new()),
        schedule_to_start_timeout: time::Duration::ZERO,
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
            cancel_requested: false,
            started_identity: None,
            retry_last_worker_identity: None,
            activity_id: id.into(),
            activity_type: "activity-type".into(),
            schedule_event_id: 7,
            task_queue: TaskQueueName("activity-q".into()),
            deployment: None,
            build_id: None,
            input: Payloads::default(),
            header: None,
            last_failure: None,
            heartbeat_details: None,
            attempt: 1,
            retry_policy: None,
            schedule_to_close_timeout: Some(Duration::minutes(2)),
            schedule_to_start_timeout: Some(Duration::seconds(30)),
            start_to_close_timeout: Some(Duration::minutes(1)),
            heartbeat_timeout: Some(Duration::seconds(20)),
            scheduled_at: OffsetDateTime::UNIX_EPOCH,
            current_attempt_scheduled_at: None,
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
            cancel_requested: false,
            started_identity: None,
            retry_last_worker_identity: None,
            activity_id: id.into(),
            activity_type: "activity-type".into(),
            schedule_event_id: 7,
            task_queue: TaskQueueName("activity-q".into()),
            deployment: None,
            build_id: None,
            input: Payloads::default(),
            header: None,
            last_failure: None,
            heartbeat_details: None,
            attempt: 1,
            retry_policy: None,
            schedule_to_close_timeout: Some(Duration::minutes(2)),
            schedule_to_start_timeout: Some(Duration::seconds(30)),
            start_to_close_timeout: Some(Duration::minutes(1)),
            heartbeat_timeout: Some(Duration::seconds(20)),
            scheduled_at: OffsetDateTime::UNIX_EPOCH,
            current_attempt_scheduled_at: None,
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
        external_initiated_event_id: 0,
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

fn make_update_activity_options_request(activity_id: &str) -> UpdateActivityOptionsRequest {
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

fn make_reset_activity_request_with_heartbeat_policy(
    activity_id: &str,
    reset_heartbeat: bool,
) -> ResetActivityRequest {
    ResetActivityRequest {
        activity_id: activity_id.into(),
        reset_heartbeat,
        request: request_context(if reset_heartbeat {
            "reset-activity-clear-heartbeat"
        } else {
            "reset-activity-keep-heartbeat"
        }),
        now: now(),
    }
}

fn make_timeout_request() -> WorkflowExecutionTimedOutRequest {
    WorkflowExecutionTimedOutRequest {
        timeout_type: WorkflowTimeoutType::RunTimeout,
        retry_state: RetryState::Timeout,
        new_execution_run_id: None,
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
        retry_policy: Some(retry_policy()),
    }
}

fn make_closed_state() -> WorkflowState {
    let mut state = make_open_state();
    state.status = ExecutionStatus::Completed;
    state.closed_at = Some(now());
    state
}

fn with_execution_options(mut state: WorkflowState) -> WorkflowState {
    state.set_versioning_override(Some(VersioningOverride::AutoUpgrade));
    state.completion_callbacks = vec![completion_callback()];
    state
}

fn with_pending_nexus_operation(mut state: WorkflowState, operation_id: &str) -> WorkflowState {
    state.pending_nexus_operations.insert(
        operation_id.into(),
        PendingNexusOperation {
            operation_id: operation_id.into(),
            scheduled_event_id: 12,
            endpoint: "endpoint".into(),
            service: "service".into(),
            operation: "operation".into(),
            schedule_to_close_timeout: None,
            schedule_to_start_timeout: None,
            start_to_close_timeout: None,
            scheduled_at: OffsetDateTime::UNIX_EPOCH,
            started: false,
            started_at: None,
            attempt: 0,
            last_attempt_failure: None,
            next_attempt_at: None,
            operation_token: String::new(),
            input: Default::default(),
        },
    );
    state
}

fn with_started_nexus_operation(mut state: WorkflowState, operation_id: &str) -> WorkflowState {
    state = with_pending_nexus_operation(state, operation_id);
    if let Some(pending) = state.pending_nexus_operations.get_mut(operation_id) {
        pending.started = true;
        pending.started_at = Some(OffsetDateTime::UNIX_EPOCH);
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
fn delayed_start_commits_without_initial_wft_and_records_internal_timer() {
    let mut req = make_start_request();
    req.workflow_start_delay = Some(Duration::seconds(30));
    let transition = kernel()
        .apply(LoadedRun::Absent, Command::Start(req.clone()))
        .unwrap();

    assert_eq!(transition.history_events.len(), 1);
    assert!(matches!(
        transition.history_events[0].kind,
        HistoryEventKind::WorkflowExecutionStarted { .. }
    ));
    assert!(transition.next_state.pending_workflow_task.is_none());
    assert!(transition.dispatch_ops.is_empty());
    assert_eq!(transition.timer_ops.len(), 1);
    match &transition.timer_ops[0] {
        tokeira_kernel::TimerOp::Upsert(timer) => {
            assert_eq!(timer.timer_id, WORKFLOW_START_DELAY_TIMER_ID);
            assert_eq!(timer.started_event_id, 0);
            assert_eq!(timer.fire_at, req.now + Duration::seconds(30));
        }
        other => panic!("unexpected timer op: {other:?}"),
    }
}

#[test]
fn start_delay_elapsed_schedules_first_wft_without_timer_fired_history() {
    let mut req = make_start_request();
    req.workflow_start_delay = Some(Duration::seconds(30));
    let start = kernel()
        .apply(LoadedRun::Absent, Command::Start(req))
        .unwrap();

    let transition = kernel()
        .apply(
            LoadedRun::Existing(start.next_state),
            Command::WorkflowStartDelayElapsed(WorkflowStartDelayElapsedRequest {
                fired_at: now(),
            }),
        )
        .unwrap();

    assert!(transition.next_state.pending_workflow_task.is_some());
    assert!(
        transition
            .history_events
            .iter()
            .any(|event| matches!(event.kind, HistoryEventKind::WorkflowTaskScheduled { .. }))
    );
    assert!(
        !transition
            .history_events
            .iter()
            .any(|event| matches!(event.kind, HistoryEventKind::TimerFired { .. }))
    );
    assert!(matches!(
        transition.timer_ops.as_slice(),
        [tokeira_kernel::TimerOp::Delete { timer_id }] if timer_id == WORKFLOW_START_DELAY_TIMER_ID
    ));
}

#[test]
fn duplicate_start_delay_elapsed_without_timer_is_noop() {
    let req = make_start_request();
    let start = kernel()
        .apply(LoadedRun::Absent, Command::Start(req))
        .unwrap();

    let transition = kernel()
        .apply(
            LoadedRun::Existing(start.next_state),
            Command::WorkflowStartDelayElapsed(WorkflowStartDelayElapsedRequest {
                fired_at: now(),
            }),
        )
        .unwrap();

    assert!(transition.history_events.is_empty());
    assert!(transition.dispatch_ops.is_empty());
    assert!(transition.timer_ops.is_empty());
}

#[test]
fn delayed_start_replay_reconstructs_timer_until_wft_is_scheduled() {
    let kernel = kernel();
    let mut req = make_start_request();
    req.workflow_start_delay = Some(Duration::seconds(30));
    let start = kernel
        .apply(LoadedRun::Absent, Command::Start(req.clone()))
        .unwrap();
    let ctx = replay_context_from_start(&req);

    let replayed_start = kernel
        .replay_history_prefix(ctx.clone(), &start.history_events)
        .unwrap();
    assert!(
        replayed_start
            .timers
            .contains_key(WORKFLOW_START_DELAY_TIMER_ID)
    );
    assert!(replayed_start.pending_workflow_task.is_none());

    let elapsed = kernel
        .apply(
            LoadedRun::Existing(start.next_state),
            Command::WorkflowStartDelayElapsed(WorkflowStartDelayElapsedRequest {
                fired_at: now(),
            }),
        )
        .unwrap();
    let history = start
        .history_events
        .iter()
        .chain(elapsed.history_events.iter())
        .cloned()
        .collect::<Vec<_>>();
    let replayed_elapsed = kernel.replay_history_prefix(ctx, &history).unwrap();
    assert!(
        !replayed_elapsed
            .timers
            .contains_key(WORKFLOW_START_DELAY_TIMER_ID)
    );
    assert!(replayed_elapsed.pending_workflow_task.is_some());
}

#[test]
fn start_and_update_from_absent_is_one_transition() {
    // Update-with-Start's fresh-start leg: ONE transition creates the run,
    // admits the update, and schedules the first WFT (the delivery vehicle).
    // NO update event is emitted — v1.31.0 writes none on this path and the
    // corpus pins UWS histories starting exactly
    // `1 WorkflowExecutionStarted / 2 WorkflowTaskScheduled`
    // (multioperation/api.go @ v1.31.0; spec api-conformance-multi-operation).
    let transition = kernel()
        .apply(
            LoadedRun::Absent,
            Command::StartAndUpdate(tokeira_kernel::StartAndUpdateRequest {
                start: make_start_request(),
                update_id: "update-1".to_string(),
            }),
        )
        .unwrap();

    assert_eq!(transition.history_events.len(), 2);
    assert!(matches!(
        transition.history_events[0].kind,
        HistoryEventKind::WorkflowExecutionStarted { .. }
    ));
    assert!(matches!(
        transition.history_events[1].kind,
        HistoryEventKind::WorkflowTaskScheduled { .. }
    ));
    assert!(transition.next_state.admitted_updates.contains("update-1"));
    assert!(transition.next_state.pending_workflow_task.is_some());
    assert!(
        transition
            .dispatch_ops
            .iter()
            .any(|op| matches!(op, DispatchOp::EnqueueWorkflowTask { .. }))
    );
}

#[test]
fn start_and_update_rejects_existing_run() {
    let reject = kernel()
        .apply(
            LoadedRun::Existing(make_open_state()),
            Command::StartAndUpdate(tokeira_kernel::StartAndUpdateRequest {
                start: make_start_request(),
                update_id: "update-1".to_string(),
            }),
        )
        .unwrap_err();
    assert_eq!(reject, Reject::RunAlreadyExists);
}

#[test]
fn signal_with_start_from_absent() {
    let mut req = make_signal_with_start_request();
    let mut header = BTreeMap::new();
    header.insert("x-signal".to_string(), Payload::new(b"metadata".to_vec()));
    let links = vec![tokeira_kernel::state::Link::BatchJob {
        job_id: "batch-1".to_string(),
    }];
    req.header = Some(Headers(header.clone()));
    req.links = links.clone();
    let transition = kernel()
        .apply(LoadedRun::Absent, Command::SignalWithStart(req.clone()))
        .unwrap();

    assert_eq!(transition.history_events.len(), 3);
    assert!(matches!(
        transition.history_events[0].kind,
        HistoryEventKind::WorkflowExecutionStarted { .. }
    ));
    assert!(matches!(
        transition.history_events[1].kind,
        HistoryEventKind::WorkflowExecutionSignaled { .. }
    ));
    assert!(matches!(
        transition.history_events[2].kind,
        HistoryEventKind::WorkflowTaskScheduled { .. }
    ));
    match &transition.history_events[0].kind {
        HistoryEventKind::WorkflowExecutionStarted {
            header: actual_header,
            links: actual_links,
            ..
        } => {
            assert_eq!(actual_header, &Some(Headers(header.clone())));
            assert_eq!(actual_links, &links);
        }
        other => panic!("expected started event, got {other:?}"),
    }
    match &transition.history_events[1].kind {
        HistoryEventKind::WorkflowExecutionSignaled {
            header: actual_header,
            links: actual_links,
            ..
        } => {
            assert_eq!(actual_header, &Some(Headers(header)));
            assert_eq!(actual_links, &links);
        }
        other => panic!("expected signaled event, got {other:?}"),
    }
    assert_eq!(transition.request_dedupe_ops.len(), 1);
    assert_eq!(transition.projection_ops.len(), 1);
    assert!(
        transition
            .dispatch_ops
            .iter()
            .any(|op| matches!(op, DispatchOp::EnqueueWorkflowTask { .. }))
    );
    assert!(transition.next_state.pending_workflow_task.is_some());
}

#[test]
fn signal_with_start_rejects_existing_run() {
    let reject = kernel()
        .apply(
            LoadedRun::Existing(make_open_state()),
            Command::SignalWithStart(make_signal_with_start_request()),
        )
        .unwrap_err();
    assert_eq!(reject, Reject::RunAlreadyExists);
}

#[test]
fn sticky_completion_dispatches_next_wft_sticky_then_s2s_times_out() {
    // The stickytq leaves' spine (sticky raise S1–S3): complete with sticky
    // attributes → a signal's WFT dispatches on the STICKY queue with a
    // schedule-to-start deadline → the S2S timeout writes a real
    // WorkflowTaskTimedOut(ScheduleToStart), clears sticky, keeps attempt 1,
    // and reschedules on the NORMAL queue.
    let state = make_open_state_with_started_wft();
    let completed = kernel()
        .apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(3),
                    started_event_id: 9,
                    attempt: 1,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker1".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: Some(tokeira_kernel::StickySpec {
                    queue: tokeira_types::TaskQueueName("queue-sticky".into()),
                    schedule_to_start_timeout: Duration::seconds(2),
                }),
                commands: vec![],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now: now(),
            }),
        )
        .unwrap();
    let sticky = completed
        .next_state
        .sticky
        .as_ref()
        .expect("completion with sticky attributes records the affinity");
    assert_eq!(sticky.sticky_queue.0, "queue-sticky");
    assert_eq!(sticky.schedule_to_start_timeout, Duration::seconds(2));

    let signaled = kernel()
        .apply(
            LoadedRun::Existing(completed.next_state),
            Command::Signal(SignalRequest {
                signal_name: "sigA".into(),
                input: payloads("signal"),
                header: None,
                links: Vec::new(),
                request: request_context("signal-sticky"),
                now: now(),
            }),
        )
        .unwrap();
    // The scheduled event and the dispatch op both name the STICKY queue.
    assert!(signaled.history_events.iter().any(|event| matches!(
        &event.kind,
        HistoryEventKind::WorkflowTaskScheduled { task_queue, .. } if task_queue.0 == "queue-sticky"
    )));
    assert!(signaled.dispatch_ops.iter().any(|op| matches!(
        op,
        tokeira_kernel::DispatchOp::EnqueueWorkflowTask { queue, .. }
            if queue.task_queue.0 == "queue-sticky"
    )));
    let pending = signaled
        .next_state
        .pending_workflow_task
        .clone()
        .expect("signal schedules a WFT");
    let deadline = pending
        .schedule_to_start_deadline
        .expect("sticky dispatch carries a schedule-to-start deadline");
    assert_eq!(deadline, now() + Duration::seconds(2));
    assert!(pending.started_event_id.is_none());

    let timed_out = kernel()
        .apply(
            LoadedRun::Existing(signaled.next_state),
            Command::WorkflowTaskTimedOut(tokeira_kernel::WorkflowTaskTimedOutRequest {
                logical_seq: pending.logical_seq,
                started_event_id: 0,
                timeout_type: tokeira_kernel::WorkflowTaskTimeoutType::ScheduleToStart,
                now: now() + Duration::seconds(3),
            }),
        )
        .unwrap();
    // Real WorkflowTaskTimedOut(SCHEDULE_TO_START) + fresh attempt-1
    // WorkflowTaskScheduled on the NORMAL queue; sticky cleared.
    assert!(timed_out.history_events.iter().any(|event| matches!(
        &event.kind,
        HistoryEventKind::WorkflowTaskTimedOut {
            timeout_type: tokeira_kernel::WorkflowTaskTimeoutType::ScheduleToStart,
            started_event_id: 0,
            ..
        }
    )));
    assert!(timed_out.history_events.iter().any(|event| matches!(
        &event.kind,
        HistoryEventKind::WorkflowTaskScheduled { task_queue, attempt: 1, .. }
            if task_queue.0 != "queue-sticky"
    )));
    assert!(timed_out.next_state.sticky.is_none());
    let rescheduled = timed_out
        .next_state
        .pending_workflow_task
        .expect("S2S timeout reschedules");
    assert_eq!(rescheduled.attempt, 1);
    assert!(rescheduled.schedule_to_start_deadline.is_none());
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
                header: None,
                links: Vec::new(),
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
fn signal_preserves_header_and_links_in_history() {
    let mut header = BTreeMap::new();
    header.insert("x-signal".to_string(), Payload::new(b"metadata".to_vec()));
    let links = vec![tokeira_kernel::state::Link::BatchJob {
        job_id: "batch-1".to_string(),
    }];
    let transition = kernel()
        .apply(
            LoadedRun::Existing(make_open_state()),
            Command::Signal(SignalRequest {
                signal_name: "sig".into(),
                input: payloads("signal"),
                header: Some(Headers(header.clone())),
                links: links.clone(),
                request: request_context("signal-req"),
                now: now(),
            }),
        )
        .unwrap();

    match &transition.history_events[0].kind {
        HistoryEventKind::WorkflowExecutionSignaled {
            header: actual_header,
            links: actual_links,
            ..
        } => {
            assert_eq!(actual_header, &Some(Headers(header)));
            assert_eq!(actual_links, &links);
        }
        other => panic!("expected signal event, got {other:?}"),
    }
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
                header: None,
                links: Vec::new(),
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
            ..
        } if reason == &req.reason
            && external_workflow_execution.is_none()
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
            cancel_requested: false,
            started_identity: None,
            retry_last_worker_identity: None,
            activity_id: "activity-2".into(),
            activity_type: "activity-type".into(),
            schedule_event_id: 6,
            task_queue: TaskQueueName("activity-q".into()),
            deployment: None,
            build_id: None,
            input: Payloads::default(),
            header: None,
            last_failure: None,
            heartbeat_details: None,
            attempt: 1,
            retry_policy: None,
            schedule_to_close_timeout: Some(Duration::minutes(2)),
            schedule_to_start_timeout: Some(Duration::seconds(30)),
            start_to_close_timeout: Some(Duration::minutes(1)),
            heartbeat_timeout: Some(Duration::seconds(20)),
            scheduled_at: OffsetDateTime::UNIX_EPOCH,
            current_attempt_scheduled_at: None,
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
            cancel_requested: false,
            started_identity: None,
            retry_last_worker_identity: None,
            activity_id: "activity-2".into(),
            activity_type: "activity-type".into(),
            schedule_event_id: 6,
            task_queue: TaskQueueName("activity-q".into()),
            deployment: None,
            build_id: None,
            input: Payloads::default(),
            header: None,
            last_failure: None,
            heartbeat_details: None,
            attempt: 1,
            retry_policy: None,
            schedule_to_close_timeout: Some(Duration::minutes(2)),
            schedule_to_start_timeout: Some(Duration::seconds(30)),
            start_to_close_timeout: Some(Duration::minutes(1)),
            heartbeat_timeout: Some(Duration::seconds(20)),
            scheduled_at: OffsetDateTime::UNIX_EPOCH,
            current_attempt_scheduled_at: None,
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
            cancel_requested: false,
            started_identity: None,
            retry_last_worker_identity: None,
            activity_id: "activity-2".into(),
            activity_type: "activity-type".into(),
            schedule_event_id: 8,
            task_queue: TaskQueueName("activity-q".into()),
            deployment: None,
            build_id: None,
            input: Payloads::default(),
            header: None,
            last_failure: None,
            heartbeat_details: None,
            attempt: 1,
            retry_policy: None,
            schedule_to_close_timeout: Some(Duration::minutes(2)),
            schedule_to_start_timeout: Some(Duration::seconds(30)),
            start_to_close_timeout: Some(Duration::minutes(1)),
            heartbeat_timeout: Some(Duration::seconds(20)),
            scheduled_at: OffsetDateTime::UNIX_EPOCH,
            current_attempt_scheduled_at: None,
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
            cancel_requested: false,
            started_identity: None,
            retry_last_worker_identity: None,
            activity_id: "activity-2".into(),
            activity_type: "activity-type".into(),
            schedule_event_id: 8,
            task_queue: TaskQueueName("activity-q".into()),
            deployment: None,
            build_id: None,
            input: Payloads::default(),
            header: None,
            last_failure: None,
            heartbeat_details: None,
            attempt: 1,
            retry_policy: None,
            schedule_to_close_timeout: Some(Duration::minutes(2)),
            schedule_to_start_timeout: Some(Duration::seconds(30)),
            start_to_close_timeout: Some(Duration::minutes(1)),
            heartbeat_timeout: Some(Duration::seconds(20)),
            scheduled_at: OffsetDateTime::UNIX_EPOCH,
            current_attempt_scheduled_at: None,
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
                header: None,
                links: Vec::new(),
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
        task_type: tokeira_kernel::WorkflowTaskType::Normal,
        schedule_to_start_deadline: None,
        logical_seq: LogicalTaskSeq(3),
        scheduled_event_id: 8,
        scheduled_at: now(),
        started_event_id: Some(9),
        started_at: Some(now()),
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
        task_type: tokeira_kernel::WorkflowTaskType::Normal,
        schedule_to_start_deadline: None,
        logical_seq: LogicalTaskSeq(3),
        scheduled_event_id: 8,
        scheduled_at: now(),
        started_event_id: Some(9),
        started_at: Some(now()),
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
        task_type: tokeira_kernel::WorkflowTaskType::Normal,
        schedule_to_start_deadline: None,
        logical_seq: LogicalTaskSeq(3),
        scheduled_event_id: 8,
        scheduled_at: now(),
        started_event_id: Some(9),
        started_at: Some(now()),
        attempt: 1,
    });
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(3),
                    started_event_id: 9,
                    attempt: 1,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![],
                force_new_workflow_task: true,
                delivered_update_ids: Vec::new(),
                now: now(),
            }),
        )
        .unwrap();
    assert!(transition.dispatch_ops.is_empty());
    assert!(transition.next_state.pending_workflow_task.is_none());
}

#[test]
fn wft_completion_tracks_previous_started_event_id() {
    let state = make_open_state_with_started_wft();
    assert_eq!(state.previous_started_event_id, 0);

    let transition = kernel()
        .apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(3),
                    started_event_id: 9,
                    attempt: 1,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now: now(),
            }),
        )
        .unwrap();

    assert_eq!(transition.next_state.previous_started_event_id, 9);
    assert_eq!(transition.next_state.workflow_task_attempt, 1);
}

#[test]
fn update_activity_options_happy_path() {
    let transition = kernel()
        .apply(
            LoadedRun::Existing(make_open_state_with_activity("activity-1")),
            Command::UpdateActivityOptions(make_update_activity_options_request("activity-1")),
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
            Command::UpdateActivityOptions(make_update_activity_options_request("missing")),
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
        activity.heartbeat_details = Some(Payloads::default());
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
    assert!(activity.heartbeat_details.is_none());
    assert_eq!(transition.dispatch_ops.len(), 1);
    assert!(transition.history_events.is_empty());
}

#[test]
fn reset_activity_preserves_heartbeat_without_reset_flag() {
    let mut state = make_open_state_with_activity("activity-1");
    let heartbeat = Payloads(vec![Payload {
        data: b"progress".to_vec(),
        metadata: BTreeMap::new(),
        external_payloads: Vec::new(),
    }]);
    if let Some(activity) = state.activities.get_mut("activity-1") {
        activity.attempt = 5;
        activity.heartbeat_details = Some(heartbeat.clone());
    }
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::ResetActivity(make_reset_activity_request_with_heartbeat_policy(
                "activity-1",
                false,
            )),
        )
        .unwrap();
    let activity = transition.next_state.activities.get("activity-1").unwrap();
    assert_eq!(activity.attempt, 1);
    assert_eq!(activity.heartbeat_details, Some(heartbeat));
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
                request_id: "start-wft".into(),
                history_size_bytes: 0,
                suggest_continue_as_new: false,
                deployment_transition: None,
                deployment_transition_revision_number: None,
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
fn workflow_task_started_with_deployment_transition_keeps_started_wft_running() {
    let target = WorkerDeploymentVersionRef {
        deployment_name: "deployment".into(),
        build_id: "build-a".into(),
    };
    let state = make_open_state_with_pending_wft();
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::WorkflowTaskStarted(StartWorkflowTaskRequest {
                logical_seq: LogicalTaskSeq(3),
                worker_identity: WorkerIdentity("worker-a".into()),
                request_id: "start-wft".into(),
                history_size_bytes: 0,
                suggest_continue_as_new: false,
                deployment_transition: Some(target.clone()),
                deployment_transition_revision_number: Some(42),
                sticky_ttl: Some(Duration::seconds(30)),
                now: now(),
            }),
        )
        .unwrap();

    let pending = transition.next_state.pending_workflow_task.unwrap();
    assert_eq!(pending.started_event_id, Some(10));
    assert_eq!(pending.started_at, Some(now()));
    assert_eq!(transition.next_state.sticky, None);
    let info = transition.next_state.versioning_info.unwrap();
    assert_eq!(info.version_transition, Some(target));
    assert_eq!(info.revision_number, 42);
}

#[test]
fn start_deployment_transition_schedules_wft_when_missing() {
    let target = WorkerDeploymentVersionRef {
        deployment_name: "deployment".into(),
        build_id: "build-a".into(),
    };
    let transition = kernel()
        .apply(
            LoadedRun::Existing(make_open_state()),
            Command::StartDeploymentTransition(StartDeploymentTransitionRequest {
                target: target.clone(),
                revision_number: 42,
                now: now(),
            }),
        )
        .unwrap();

    assert!(matches!(
        transition.history_events[0].kind,
        HistoryEventKind::WorkflowTaskScheduled { .. }
    ));
    assert_eq!(transition.dispatch_ops.len(), 1);
    let pending = transition
        .next_state
        .pending_workflow_task
        .expect("transition schedules a workflow task");
    assert_eq!(pending.logical_seq, LogicalTaskSeq(4));
    let info = transition.next_state.versioning_info.unwrap();
    assert_eq!(info.version_transition, Some(target));
    assert_eq!(info.revision_number, 42);
}

#[test]
fn start_deployment_transition_reuses_pending_wft_without_double_schedule() {
    let target = WorkerDeploymentVersionRef {
        deployment_name: "deployment".into(),
        build_id: "build-a".into(),
    };
    let mut state = make_open_state_with_pending_wft();
    state.sticky = Some(StickyAffinity {
        sticky_queue: tokeira_types::TaskQueueName(String::new()),
        schedule_to_start_timeout: time::Duration::ZERO,
        worker_identity: WorkerIdentity("sticky".into()),
        expires_at: now() + Duration::seconds(30),
    });

    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::StartDeploymentTransition(StartDeploymentTransitionRequest {
                target: target.clone(),
                revision_number: 43,
                now: now(),
            }),
        )
        .unwrap();

    assert!(transition.history_events.is_empty());
    assert!(transition.dispatch_ops.is_empty());
    assert_eq!(transition.next_state.sticky, None);
    let pending = transition.next_state.pending_workflow_task.unwrap();
    assert_eq!(pending.logical_seq, LogicalTaskSeq(3));
    assert_eq!(pending.started_event_id, None);
    let info = transition.next_state.versioning_info.unwrap();
    assert_eq!(info.version_transition, Some(target));
    assert_eq!(info.revision_number, 43);
}

#[test]
fn start_deployment_transition_rejects_pinned_workflow() {
    let target = WorkerDeploymentVersionRef {
        deployment_name: "deployment".into(),
        build_id: "build-a".into(),
    };
    let mut state = make_open_state();
    state.versioning_info = Some(tokeira_kernel::WorkflowVersioningInfo {
        behavior: VersioningBehavior::Pinned,
        deployment_version: Some(WorkerDeploymentVersionRef {
            deployment_name: "deployment".into(),
            build_id: "pinned".into(),
        }),
        versioning_override: None,
        version_transition: None,
        revision_number: 7,
        continue_as_new_initial_versioning_behavior:
            tokeira_kernel::ContinueAsNewVersioningBehavior::Unspecified,
    });
    let before = state.clone();

    let reject = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::StartDeploymentTransition(StartDeploymentTransitionRequest {
                target,
                revision_number: 44,
                now: now(),
            }),
        )
        .unwrap_err();

    assert_eq!(reject, Reject::PinnedWorkflowCannotTransition);
    assert_eq!(before.versioning_info.unwrap().revision_number, 7);
}

#[test]
fn workflow_task_completed_with_activity_and_timer() {
    let state = make_open_state_with_started_wft();
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(3),
                    started_event_id: 9,
                    attempt: 1,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![
                    WorkflowCommand::ScheduleActivity {
                        activity_id: "activity-1".into(),
                        activity_type: "activity-type".into(),
                        task_queue: TaskQueueName("activity-q".into()),
                        input: payloads("act"),
                        header: None,
                        request_eager_execution: false,
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
                delivered_update_ids: Vec::new(),
                now: now(),
            }),
        )
        .unwrap();

    assert!(matches!(
        transition.history_events[0].kind,
        HistoryEventKind::WorkflowTaskCompleted { .. }
    ));
    assert!(
        transition
            .history_events
            .iter()
            .any(|event| matches!(event.kind, HistoryEventKind::ActivityTaskScheduled { .. }))
    );
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
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(3),
                    started_event_id: 9,
                    attempt: 1,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![WorkflowCommand::CompleteWorkflow {
                    result: payloads("done"),
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
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
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(3),
                    started_event_id: 9,
                    attempt: 1,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![WorkflowCommand::FailWorkflow {
                    failure: payload("nope"),
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
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
            ..
        } => (
            *new_run_id,
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
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(3),
                    started_event_id: 9,
                    attempt: 1,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![command.clone()],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
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
            ..
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
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(3),
                    started_event_id: 9,
                    attempt: 1,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![
                    make_continue_as_new_command(),
                    WorkflowCommand::RequestNewWorkflowTask,
                ],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
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
            retry_state: RetryState::Timeout,
            ..
        }
    ));
}

#[test]
fn workflow_execution_timed_out_with_entities() {
    let mut state = make_open_state_with_activity("activity-1");
    state.activities.insert(
        "activity-2".into(),
        ActivityState {
            cancel_requested: false,
            started_identity: None,
            retry_last_worker_identity: None,
            activity_id: "activity-2".into(),
            activity_type: "activity-type".into(),
            schedule_event_id: 6,
            task_queue: TaskQueueName("activity-q".into()),
            deployment: None,
            build_id: None,
            input: Payloads::default(),
            header: None,
            last_failure: None,
            heartbeat_details: None,
            attempt: 1,
            retry_policy: None,
            schedule_to_close_timeout: Some(Duration::minutes(2)),
            schedule_to_start_timeout: Some(Duration::seconds(30)),
            start_to_close_timeout: Some(Duration::minutes(1)),
            heartbeat_timeout: Some(Duration::seconds(20)),
            scheduled_at: OffsetDateTime::UNIX_EPOCH,
            current_attempt_scheduled_at: None,
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
        sticky_queue: tokeira_types::TaskQueueName(String::new()),
        schedule_to_start_timeout: time::Duration::ZERO,
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
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(3),
                    started_event_id: 9,
                    attempt: 1,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![WorkflowCommand::FailWorkflow {
                    failure: payload("nope"),
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
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
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(3),
                    started_event_id: 9,
                    attempt: 1,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![WorkflowCommand::FailWorkflow {
                    failure: payload("nope"),
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
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

    assert!(
        transition
            .history_events
            .iter()
            .any(|event| matches!(event.kind, HistoryEventKind::ActivityTaskCompleted { .. }))
    );
    assert!(transition.activity_ops.iter().any(|op| matches!(op, tokeira_kernel::ActivityOp::Delete { activity_id } if activity_id == "activity-1")));
    assert!(
        transition
            .history_events
            .iter()
            .any(|event| matches!(event.kind, HistoryEventKind::WorkflowTaskScheduled { .. }))
    );
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
                    retry_state: tokeira_kernel::RetryState::Timeout,
                    failure: None,
                },
                worker_identity: None,
                now: now(),
            }),
        )
        .unwrap();

    assert!(
        transition
            .history_events
            .iter()
            .any(|event| matches!(event.kind, HistoryEventKind::ActivityTaskTimedOut { .. }))
    );
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

    assert!(
        transition
            .history_events
            .iter()
            .any(|event| matches!(event.kind, HistoryEventKind::ActivityTaskCanceled { .. }))
    );
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
    // The retry is TRANSIENT (spec transient-wft Req B.1/B.3): its scheduled id
    // becomes the virtual next-event id (WorkflowTaskFailed landed as event 10,
    // so virtual = 11) and no WorkflowTaskScheduled event is persisted
    // (workflow_task_state_machine.go:376-379 @ v1.31.0).
    assert_eq!(pending.scheduled_event_id, 11);
    assert_eq!(pending.started_event_id, None);
    assert_eq!(pending.attempt, 2);
    assert_eq!(transition.next_state.last_event_id, 10);
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

    // After an attempt-1 timeout: WorkflowTaskTimedOut only — the retry is
    // TRANSIENT (spec transient-wft Req B.1/B.3): no fresh WorkflowTaskScheduled
    // is persisted; the reschedule carries the virtual next-event id
    // (TimedOut landed as event 10 → virtual scheduled = 11;
    // workflow_task_state_machine.go:376-379 @ v1.31.0).
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
    assert_eq!(pending.logical_seq, LogicalTaskSeq(4));
    assert_eq!(pending.scheduled_event_id, 11);
    assert_eq!(pending.attempt, 2);
    assert_eq!(transition.next_state.last_event_id, 10);
    assert_eq!(pending.started_event_id, None);
    assert!(transition.next_state.sticky.is_none());
    assert_eq!(transition.dispatch_ops.len(), 1);
    assert!(matches!(
        &transition.dispatch_ops[0],
        DispatchOp::EnqueueWorkflowTask {
            logical_seq,
            sticky_preferred,
            ..
        } if *logical_seq == LogicalTaskSeq(4) && sticky_preferred.is_none()
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
                header: None,
                links: Vec::new(),
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
                header: None,
                links: Vec::new(),
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
fn cancel_closed_run_is_success_noop() {
    // "the request to cancel this workflow is a success even if the target
    // workflow has already finished" — no event, no WFT
    // (requestcancelworkflow/api.go:44-53 @ v1.31.0).
    let transition = kernel()
        .apply(
            LoadedRun::Existing(make_closed_state()),
            Command::Cancel(make_cancel_request()),
        )
        .unwrap();
    assert!(transition.history_events.is_empty());
    assert!(transition.dispatch_ops.is_empty());
    assert!(!transition.next_state.cancel_requested);
}

#[test]
fn cancel_already_cancel_requested_run_is_success_noop() {
    // Repeat cancel requests short-circuit on `IsCancelRequested` — no
    // second WorkflowExecutionCancelRequested event
    // (requestcancelworkflow/api.go:74-81 @ v1.31.0).
    let first = kernel()
        .apply(
            LoadedRun::Existing(make_open_state()),
            Command::Cancel(make_cancel_request()),
        )
        .unwrap();
    assert_eq!(
        first
            .history_events
            .iter()
            .filter(|e| matches!(
                e.kind,
                HistoryEventKind::WorkflowExecutionCancelRequested { .. }
            ))
            .count(),
        1
    );
    let mut repeat = make_cancel_request();
    repeat.request = request_context("cancel-req-2");
    let second = kernel()
        .apply(
            LoadedRun::Existing(first.next_state),
            Command::Cancel(repeat),
        )
        .unwrap();
    assert!(second.history_events.is_empty());
    assert!(second.next_state.cancel_requested);
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
                request_id: "start-wft".into(),
                history_size_bytes: 0,
                suggest_continue_as_new: false,
                deployment_transition: None,
                deployment_transition_revision_number: None,
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
                request_id: "start-wft".into(),
                history_size_bytes: 0,
                suggest_continue_as_new: false,
                deployment_transition: None,
                deployment_transition_revision_number: None,
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
                request_id: "start-wft".into(),
                history_size_bytes: 0,
                suggest_continue_as_new: false,
                deployment_transition: None,
                deployment_transition_revision_number: None,
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
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: RunKey::new(),
                    logical_seq: LogicalTaskSeq(3),
                    started_event_id: 9,
                    attempt: 1,
                    shard_epoch: ShardEpoch::ZERO
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
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
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: RunKey::new(),
                    logical_seq: LogicalTaskSeq(3),
                    started_event_id: 9,
                    attempt: 1,
                    shard_epoch: ShardEpoch::ZERO
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
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
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(4),
                    started_event_id: 9,
                    attempt: 1,
                    shard_epoch: ShardEpoch::ZERO
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
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
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(3),
                    started_event_id: 10,
                    attempt: 2,
                    shard_epoch: ShardEpoch::ZERO
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
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
            cancel_requested: false,
            started_identity: None,
            retry_last_worker_identity: None,
            activity_id: "dup".into(),
            activity_type: "activity-type".into(),
            schedule_event_id: 1,
            task_queue: TaskQueueName("activity-q".into()),
            deployment: None,
            build_id: None,
            input: Payloads::default(),
            header: None,
            last_failure: None,
            heartbeat_details: None,
            attempt: 1,
            retry_policy: None,
            schedule_to_close_timeout: None,
            schedule_to_start_timeout: None,
            start_to_close_timeout: None,
            heartbeat_timeout: None,
            scheduled_at: OffsetDateTime::UNIX_EPOCH,
            current_attempt_scheduled_at: None,
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
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(3),
                    started_event_id: 9,
                    attempt: 1,
                    shard_epoch: ShardEpoch::ZERO
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![WorkflowCommand::ScheduleActivity {
                    activity_id: "dup".into(),
                    activity_type: "activity-type".into(),
                    task_queue: TaskQueueName("activity-q".into()),
                    input: payloads("a"),
                    header: None,
                    request_eager_execution: false,
                    retry_policy: None,
                    deployment: None,
                    build_id: None,
                    schedule_to_close_timeout: None,
                    schedule_to_start_timeout: None,
                    start_to_close_timeout: None,
                    heartbeat_timeout: None,
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
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
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(3),
                    started_event_id: 9,
                    attempt: 1,
                    shard_epoch: ShardEpoch::ZERO
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![WorkflowCommand::StartTimer {
                    timer_id: "dup".into(),
                    fire_at: now()
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
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
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(3),
                    started_event_id: 9,
                    attempt: 1,
                    shard_epoch: ShardEpoch::ZERO
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![
                    WorkflowCommand::CompleteWorkflow {
                        result: payloads("done")
                    },
                    WorkflowCommand::RequestNewWorkflowTask,
                ],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
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
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(3),
                    started_event_id: 9,
                    attempt: 1,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![WorkflowCommand::CancelWorkflow { details: None }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
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
        HistoryEventKind::WorkflowExecutionCanceled { .. }
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
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(3),
                    started_event_id: 9,
                    attempt: 1,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![
                    WorkflowCommand::CancelWorkflow { details: None },
                    WorkflowCommand::RequestNewWorkflowTask,
                ],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
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
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(3),
                    started_event_id: 9,
                    attempt: 1,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![WorkflowCommand::RequestCancelActivity {
                    scheduled_event_id: 7,
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now: now(),
            }),
        )
        .unwrap();

    // K4: an UNSTARTED activity cancels immediately — CancelRequested +
    // ActivityTaskCanceled with the canonical "ACTIVITY_ID_NOT_STARTED"
    // details, removal, and a fresh WFT so the workflow observes it
    // (workflow_task_completed_handler.go:651-665 @ v1.31.0).
    assert!(transition.history_events.iter().any(|event| matches!(
        &event.kind,
        HistoryEventKind::ActivityTaskCancelRequested { activity_id, scheduled_event_id: 7, .. } if activity_id == "activity-1"
    )));
    assert!(transition.history_events.iter().any(|event| matches!(
        &event.kind,
        HistoryEventKind::ActivityTaskCanceled { activity_id, details: Some(details), .. }
            if activity_id == "activity-1"
                && details.0[0].data == b"\"ACTIVITY_ID_NOT_STARTED\"".to_vec()
    )));
    assert!(!transition.next_state.activities.contains_key("activity-1"));
    assert!(transition.activity_ops.iter().any(|op| matches!(
        op,
        tokeira_kernel::ActivityOp::Delete { activity_id } if activity_id == "activity-1"
    )));
    assert!(transition.next_state.pending_workflow_task.is_some());
}

#[test]
fn request_cancel_activity_started_sets_durable_cancel_requested() {
    let state = with_started_activity_started_wft();
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(3),
                    started_event_id: 9,
                    attempt: 1,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![WorkflowCommand::RequestCancelActivity {
                    scheduled_event_id: 7,
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now: now(),
            }),
        )
        .unwrap();

    // K4: a STARTED activity stays pending with the durable cancel bit set;
    // the worker learns via its next heartbeat response
    // (`ai.CancelRequested = true` @ v1.31.0).
    assert!(transition.history_events.iter().any(|event| matches!(
        &event.kind,
        HistoryEventKind::ActivityTaskCancelRequested { activity_id, .. } if activity_id == "activity-1"
    )));
    let activity = transition
        .next_state
        .activities
        .get("activity-1")
        .expect("started activity stays pending");
    assert!(activity.cancel_requested);
    assert!(transition.activity_ops.iter().any(|op| matches!(
        op,
        tokeira_kernel::ActivityOp::Upsert(state) if state.cancel_requested
    )));
}

#[test]
fn request_cancel_activity_unknown() {
    let state = make_open_state_with_started_wft();
    let transition = kernel().apply(
        LoadedRun::Existing(state.clone()),
        Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
            client_discards_speculative_with_events: false,
            token: WorkflowTaskToken {
                run_key: state.run_key,
                logical_seq: LogicalTaskSeq(3),
                started_event_id: 9,
                attempt: 1,
                shard_epoch: ShardEpoch::ZERO,
            },
            identity: WorkerIdentity("worker".into()),
            sdk_metadata: None,
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: VersioningBehavior::Unspecified,
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            commands: vec![WorkflowCommand::RequestCancelActivity {
                scheduled_event_id: 999,
            }],
            force_new_workflow_task: false,
            delivered_update_ids: Vec::new(),
            now: now(),
        }),
    );

    // K4 (revised in Tier 1.6): an unknown scheduled_event_id REJECTS the
    // completion with the WFT-failure cause; the RUNTIME then persists the
    // WorkflowTaskFailed transition and errors the call with
    // INVALID_ARGUMENT — v1.31.0's handler/API split
    // (respondworkflowtaskcompleted/api.go:455-485,739-742).
    assert_eq!(
        transition,
        Err(Reject::InvalidCommandAttributes {
            cause: tokeira_kernel::WorkflowTaskFailedCause::BadRequestCancelActivityAttributes,
            message: Some("ScheduledEventID: 999".to_string()),
        })
    );
}

#[test]
fn close_command_with_buffered_events_is_unhandled_command() {
    // TestNoTransientWorkflowTaskAfterFlushBufferedEvents' spine: an event
    // buffered during the started WFT makes any CLOSE command an
    // UnhandledCommand — the completion rejects with the exact cause string
    // the corpus asserts as err.Error() ("UnhandledCommand"); the runtime
    // fails the WFT and the buffered event flushes onto a fresh attempt-1
    // task (`hasBufferedEventsOrMessages` close guards @ v1.31.0).
    let mut state = make_open_state_with_started_wft();
    state
        .buffered_events
        .push(tokeira_kernel::state::BufferedEvent {
            admitted_at: now(),
            kind: HistoryEventKind::WorkflowExecutionSignaled {
                signal_name: "buffered-signal-1".into(),
                input: payloads("buffered-signal-input"),
                header: None,
                links: Vec::new(),
                request_id: "buffered-signal-req".into(),
                identity: Some("worker1".into()),
            },
        });
    let transition = kernel().apply(
        LoadedRun::Existing(state.clone()),
        Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
            client_discards_speculative_with_events: false,
            token: WorkflowTaskToken {
                run_key: state.run_key,
                logical_seq: LogicalTaskSeq(3),
                started_event_id: 9,
                attempt: 1,
                shard_epoch: ShardEpoch::ZERO,
            },
            identity: WorkerIdentity("worker".into()),
            sdk_metadata: None,
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: VersioningBehavior::Unspecified,
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            commands: vec![WorkflowCommand::CompleteWorkflow {
                result: payloads("done"),
            }],
            force_new_workflow_task: false,
            delivered_update_ids: Vec::new(),
            now: now(),
        }),
    );
    assert_eq!(
        transition,
        Err(Reject::InvalidCommandAttributes {
            cause: tokeira_kernel::WorkflowTaskFailedCause::UnhandledCommand,
            message: None,
        })
    );
}

#[test]
fn cancel_timer() {
    let state = with_pending_timer_started_wft();
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(3),
                    started_event_id: 9,
                    attempt: 1,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![WorkflowCommand::CancelTimer {
                    timer_id: "timer-1".into(),
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now: now(),
            }),
        )
        .unwrap();

    assert!(transition.history_events.iter().any(|event| matches!(
        &event.kind,
        HistoryEventKind::TimerCanceled { timer_id, .. } if timer_id == "timer-1"
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
    let transition = kernel().apply(
        LoadedRun::Existing(state.clone()),
        Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
            client_discards_speculative_with_events: false,
            token: WorkflowTaskToken {
                run_key: state.run_key,
                logical_seq: LogicalTaskSeq(3),
                started_event_id: 9,
                attempt: 1,
                shard_epoch: ShardEpoch::ZERO,
            },
            identity: WorkerIdentity("worker".into()),
            sdk_metadata: None,
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: VersioningBehavior::Unspecified,
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            commands: vec![WorkflowCommand::CancelTimer {
                timer_id: "missing".into(),
            }],
            force_new_workflow_task: false,
            delivered_update_ids: Vec::new(),
            now: now(),
        }),
    );

    // A cancel of a timer that is neither running nor fired-and-buffered
    // REJECTS the completion with the WFT-failure cause; the RUNTIME then
    // persists the WorkflowTaskFailed transition and errors the call with
    // INVALID_ARGUMENT — v1.31.0's handler/API split
    // (respondworkflowtaskcompleted/api.go:455-485,739-742).
    assert_eq!(
        transition,
        Err(Reject::InvalidCommandAttributes {
            cause: tokeira_kernel::WorkflowTaskFailedCause::BadCancelTimerAttributes,
            message: Some("TimerID: missing".to_string()),
        })
    );
}

#[test]
fn record_marker_missing_name_fails_wft() {
    // TestRespondWorkflowTaskCompletedReturnsErrorIfInvalidArgument's spine
    // (Tier 1.7): an empty MarkerName REJECTS the completion with
    // BadRecordMarkerAttributes and the validator's exact causeErr
    // (`ValidateRecordMarkerAttributes`, command_attr_validator.go:194-211
    // @ v1.31.0); the runtime persists the WFT failure and renders the wire
    // message "BadRecordMarkerAttributes: MarkerName is not set on
    // RecordMarkerCommand." — asserted verbatim by the corpus.
    let state = make_open_state_with_started_wft();
    let transition = kernel().apply(
        LoadedRun::Existing(state.clone()),
        Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
            client_discards_speculative_with_events: false,
            token: WorkflowTaskToken {
                run_key: state.run_key,
                logical_seq: LogicalTaskSeq(3),
                started_event_id: 9,
                attempt: 1,
                shard_epoch: ShardEpoch::ZERO,
            },
            identity: WorkerIdentity("worker".into()),
            sdk_metadata: None,
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: VersioningBehavior::Unspecified,
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            commands: vec![WorkflowCommand::RecordMarker {
                marker_name: String::new(),
                details: std::collections::BTreeMap::new(),
                failure: None,
                header: None,
            }],
            force_new_workflow_task: false,
            delivered_update_ids: Vec::new(),
            now: now(),
        }),
    );
    assert_eq!(
        transition,
        Err(Reject::InvalidCommandAttributes {
            cause: tokeira_kernel::WorkflowTaskFailedCause::BadRecordMarkerAttributes,
            message: Some("MarkerName is not set on RecordMarkerCommand.".to_string()),
        })
    );
}

#[test]
fn transient_completion_materializes_scheduled_started_with_task_times() {
    // Req B.6 timestamp fidelity (Tier 1.7): the late-materialized
    // Scheduled/Started pair carries the transient task's ACTUAL
    // schedule/start times — the worker observed them on its poll response,
    // and TestWorkflowTaskFailed asserts the persisted started time equals
    // the observed one, ≥1s before the close event
    // (`workflowTask.ScheduledTime` / `workflowTask.StartedTime` in
    // `AddWorkflowTaskCompletedEvent`,
    // workflow_task_state_machine.go:768-800 @ v1.31.0).
    let mut state = make_open_state_with_started_wft();
    let scheduled_at = now() - Duration::seconds(10);
    let started_at = now() - Duration::seconds(5);
    state.workflow_task_attempt = 3;
    state.pending_workflow_task = Some(PendingWorkflowTask {
        task_type: tokeira_kernel::WorkflowTaskType::Normal,
        schedule_to_start_deadline: None,
        logical_seq: LogicalTaskSeq(3),
        scheduled_event_id: 10, // virtual: last_event_id (9) + 1
        scheduled_at,
        started_event_id: Some(11),
        started_at: Some(started_at),
        attempt: 3,
    });
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(3),
                    started_event_id: 11,
                    attempt: 3,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now: now(),
            }),
        )
        .unwrap();

    let scheduled = transition
        .history_events
        .iter()
        .find(|event| {
            matches!(
                event.kind,
                HistoryEventKind::WorkflowTaskScheduled { attempt: 3, .. }
            )
        })
        .expect("materialized WorkflowTaskScheduled");
    assert_eq!(scheduled.event_id, 10);
    assert_eq!(scheduled.happened_at, scheduled_at);
    let started = transition
        .history_events
        .iter()
        .find(|event| {
            matches!(
                event.kind,
                HistoryEventKind::WorkflowTaskStarted { attempt: 3, .. }
            )
        })
        .expect("materialized WorkflowTaskStarted");
    assert_eq!(started.event_id, 11);
    assert_eq!(started.happened_at, started_at);
    let completed = transition
        .history_events
        .iter()
        .find(|event| matches!(event.kind, HistoryEventKind::WorkflowTaskCompleted { .. }))
        .expect("WorkflowTaskCompleted");
    assert_eq!(completed.event_id, 12);
    assert_eq!(completed.happened_at, now());
}

#[test]
fn cancel_timer_fired_and_buffered_deletes_buffered_fired_event() {
    // TestCancelTimer_CancelFiredAndBuffered's spine: the timer fires DURING
    // the started WFT (fired event buffered, Phase 2 timer slice), then the
    // same WFT's completion cancels it — the buffered TimerFired is deleted
    // and only TimerCanceled reaches history
    // (`GetAndRemoveTimerFireEvent` in `AddTimerCanceledEvent` @ v1.31.0).
    let state = with_pending_timer_started_wft();
    let fired = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::TimerDue(TimerDueRequest {
                timer_id: "timer-1".into(),
                fired_at: now(),
            }),
        )
        .unwrap();
    // Buffered, not appended: no TimerFired in history, timer gone from state.
    assert!(
        !fired
            .history_events
            .iter()
            .any(|event| matches!(&event.kind, HistoryEventKind::TimerFired { .. }))
    );
    assert_eq!(fired.next_state.buffered_events.len(), 1);
    assert!(!fired.next_state.timers.contains_key("timer-1"));

    let cancelled = kernel()
        .apply(
            LoadedRun::Existing(fired.next_state),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: RunKey::new(),
                    logical_seq: LogicalTaskSeq(3),
                    started_event_id: 9,
                    attempt: 1,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![WorkflowCommand::CancelTimer {
                    timer_id: "timer-1".into(),
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now: now(),
            }),
        )
        .unwrap();
    // TimerCanceled wins; the buffered TimerFired is deleted (flush emits
    // nothing), so no follow-up WFT is scheduled for it.
    assert!(cancelled.history_events.iter().any(|event| matches!(
        &event.kind,
        HistoryEventKind::TimerCanceled { timer_id, .. } if timer_id == "timer-1"
    )));
    assert!(
        !cancelled
            .history_events
            .iter()
            .any(|event| matches!(&event.kind, HistoryEventKind::TimerFired { .. }))
    );
    assert!(cancelled.next_state.buffered_events.is_empty());
}

#[test]
fn request_cancel_activity_then_resolved_canceled() {
    let state = with_started_activity_started_wft();
    let first = kernel()
        .apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(3),
                    started_event_id: 9,
                    attempt: 1,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![WorkflowCommand::RequestCancelActivity {
                    scheduled_event_id: 7,
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
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
    assert!(
        second
            .history_events
            .iter()
            .any(|event| matches!(event.kind, HistoryEventKind::ActivityTaskCanceled { .. }))
    );
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
                request_id: "start-wft".into(),
                history_size_bytes: 0,
                suggest_continue_as_new: false,
                deployment_transition: None,
                deployment_transition_revision_number: None,
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
            task_type: tokeira_kernel::WorkflowTaskType::Normal,
            schedule_to_start_deadline: None,
            logical_seq: LogicalTaskSeq(4),
            scheduled_event_id: 11,
            scheduled_at: now(),
            started_event_id: Some(12),
            started_at: Some(now()),
            attempt: 1,
        });
    let final_transition = kernel()
        .apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: pending.logical_seq,
                    started_event_id: pending.started_event_id.unwrap(),
                    attempt: pending.attempt,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![WorkflowCommand::CancelWorkflow { details: None }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
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
            cancel_requested: false,
            started_identity: None,
            retry_last_worker_identity: None,
            activity_id: "activity-1".into(),
            activity_type: "activity-type".into(),
            schedule_event_id: 7,
            task_queue: TaskQueueName("activity-q".into()),
            deployment: None,
            build_id: None,
            input: Payloads::default(),
            header: None,
            last_failure: None,
            heartbeat_details: None,
            attempt: 1,
            retry_policy: None,
            schedule_to_close_timeout: Some(Duration::minutes(2)),
            schedule_to_start_timeout: Some(Duration::seconds(30)),
            start_to_close_timeout: Some(Duration::minutes(1)),
            heartbeat_timeout: Some(Duration::seconds(20)),
            scheduled_at: OffsetDateTime::UNIX_EPOCH,
            current_attempt_scheduled_at: None,
            started_at: None,
            started_event_id: None,
            pause_info: None,
            stamp: 0,
        },
    );
    state
}

fn with_started_activity_started_wft() -> WorkflowState {
    let mut state = with_pending_activity_started_wft();
    let activity = state
        .activities
        .get_mut("activity-1")
        .expect("fixture activity");
    activity.started_event_id = Some(8);
    activity.started_at = Some(OffsetDateTime::UNIX_EPOCH);
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
            namespace: None,
            workflow_type: WorkflowType("child-workflow".into()),
            header: None,
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
            target_namespace_id: state.namespace_id,
            target_namespace: None,
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
            target_namespace_id: state.namespace_id,
            target_namespace: None,
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
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: pending.logical_seq,
                    started_event_id: pending.started_event_id.unwrap(),
                    attempt: pending.attempt,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![WorkflowCommand::StartChildWorkflow {
                    child_workflow_id: child_workflow_id.clone(),
                    namespace_id: NamespaceId::new(),
                    namespace: None,
                    workflow_type: WorkflowType("child-wf".into()),
                    task_queue: TaskQueueName("child-q".into()),
                    input: payloads("child-input"),
                    header: None,
                    memo: Memo::default(),
                    search_attributes: SearchAttributes::default(),
                    workflow_execution_timeout: None,
                    workflow_run_timeout: None,
                    workflow_task_timeout: Duration::seconds(10),
                    retry_policy: None,
                    cron_schedule: None,
                    parent_close_policy: ParentClosePolicy::Terminate,
                    reuse_policy: tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
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
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: pending.logical_seq,
                    started_event_id: pending.started_event_id.unwrap(),
                    attempt: pending.attempt,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![WorkflowCommand::SignalExternalWorkflowExecution {
                    target_namespace_id: state.namespace_id,
                    target_namespace: None,
                    target_workflow_id: WorkflowId("target".into()),
                    target_run_id: Some(RunId::new()),
                    signal_name: "sig".into(),
                    input: payloads("payload"),
                    header: None,
                    control: "ctl".into(),
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
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
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: pending.logical_seq,
                    started_event_id: pending.started_event_id.unwrap(),
                    attempt: pending.attempt,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![WorkflowCommand::RequestCancelExternalWorkflowExecution {
                    target_namespace_id: state.namespace_id,
                    target_namespace: None,
                    target_workflow_id: WorkflowId("target".into()),
                    target_run_id: Some(RunId::new()),
                    control: "ctl".into(),
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
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

// Feature: speculative-wft, Golden (a) / Property P1 — an update against a
// quiescent run (no pending WFT, no buffered events) schedules a SPECULATIVE
// task: NO events persist, `last_event_id` is untouched, ids are virtual
// (scheduled = last_event_id + 1), and the dispatch op is marked speculative
// (`AddWorkflowTaskScheduledEvent(false, SPECULATIVE)`,
// updateworkflow/api.go:171-186 + workflow_task_state_machine.go:309-410
// @ v1.31.0).
#[test]
fn update_with_no_pending_wft() {
    let state = make_open_state();
    let last_event_id = state.last_event_id;
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

    assert_eq!(transition.request_dedupe_ops.len(), 1);
    assert!(transition.next_state.admitted_updates.contains("update-1"));
    assert!(transition.history_events.is_empty());
    assert_eq!(transition.next_state.last_event_id, last_event_id);
    let pending = transition
        .next_state
        .pending_workflow_task
        .as_ref()
        .expect("speculative task must be pending");
    assert_eq!(
        pending.task_type,
        tokeira_kernel::WorkflowTaskType::Speculative
    );
    assert_eq!(pending.attempt, 1);
    assert_eq!(pending.scheduled_event_id, last_event_id + 1);
    assert!(matches!(
        transition.dispatch_ops[0],
        DispatchOp::EnqueueWorkflowTask {
            speculative: true,
            ..
        }
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

    // No events emitted, no new WFT (one already pending).
    assert!(transition.history_events.is_empty());
    assert_eq!(transition.dispatch_ops.len(), 0);
    assert_eq!(transition.next_state.pending_workflow_task, pending);
    assert!(transition.next_state.admitted_updates.contains("update-1"));
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
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: pending.logical_seq,
                    started_event_id: pending.started_event_id.unwrap(),
                    attempt: pending.attempt,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![WorkflowCommand::UpdateCompleted {
                    update_id: "update-1".into(),
                    result: payloads("done"),
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now: now(),
            }),
        )
        .unwrap();

    // The kernel emits the failure-capable V2 completed shape; the original
    // variant is decode-only (spec speculative-wft K6).
    assert!(matches!(
        transition.history_events[1].kind,
        HistoryEventKind::WorkflowExecutionUpdateCompletedV2 {
            outcome: tokeira_kernel::UpdateEventOutcome::Success(..),
            ..
        }
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
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: pending.logical_seq,
                    started_event_id: pending.started_event_id.unwrap(),
                    attempt: pending.attempt,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![WorkflowCommand::UpdateRejected {
                    update_id: "update-1".into(),
                    failure: payload("nope"),
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now: now(),
            }),
        )
        .unwrap();

    // A rejection writes NO history event — v1.31.0's
    // RejectWorkflowExecutionUpdate is a documented no-op, and the Go SDK
    // panics TMPRL1100-nondeterministic replaying any rejection event.
    assert!(!transition.history_events.iter().any(|event| matches!(
        event.kind,
        HistoryEventKind::WorkflowExecutionUpdateRejected { .. }
    )));
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
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: pending.logical_seq,
                    started_event_id: pending.started_event_id.unwrap(),
                    attempt: pending.attempt,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![WorkflowCommand::UpdateCompleted {
                    update_id: "missing".into(),
                    result: payloads("done"),
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now: now(),
            }),
        )
        .unwrap_err();
    assert_eq!(reject, Reject::UnknownUpdate("missing".into()));
}

#[test]
fn update_rejected_unknown_update_is_tolerated() {
    let state = make_open_state_with_started_wft();
    let pending = state.pending_workflow_task.clone().unwrap();
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: pending.logical_seq,
                    started_event_id: pending.started_event_id.unwrap(),
                    attempt: pending.attempt,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![WorkflowCommand::UpdateRejected {
                    update_id: "missing".into(),
                    failure: payload("nope"),
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now: now(),
            }),
        )
        .unwrap();
    // A rejection for an unknown/already-resolved update is a no-op:
    // v1.31.0 resurrects from the rejection's embedded request and resolves
    // without error (TryResurrect, registry.go:455-484) — observably
    // identical to ignoring it. Erroring here poisoned SDK retries that
    // re-send a rejection for an update the server already dropped.
    assert!(!transition.history_events.iter().any(|event| matches!(
        event.kind,
        HistoryEventKind::WorkflowExecutionUpdateRejected { .. }
    )));
}

#[test]
fn protocol_message_accepted_body() {
    let state = make_open_state_with_started_wft();
    let pending = state.pending_workflow_task.clone().unwrap();
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: pending.logical_seq,
                    started_event_id: pending.started_event_id.unwrap(),
                    attempt: pending.attempt,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![WorkflowCommand::ProtocolMessage {
                    message_id: "msg-1".into(),
                    body: UpdateProtocolBody::Accepted {
                        update_id: "update-1".into(),
                        update_name: "handler".into(),
                        input: payloads("input"),
                        sequencing_event_id: 1,
                    },
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
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
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: pending.logical_seq,
                    started_event_id: pending.started_event_id.unwrap(),
                    attempt: pending.attempt,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![WorkflowCommand::ProtocolMessage {
                    message_id: "msg-2".into(),
                    body: UpdateProtocolBody::Completed {
                        update_id: "update-1".into(),
                        result: payloads("done"),
                        failure: None,
                    },
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now: now(),
            }),
        )
        .unwrap();
    // Completions emit the failure-capable V2 shape; the original variant is
    // decode-only (spec speculative-wft K6).
    assert!(matches!(
        transition.history_events[1].kind,
        HistoryEventKind::WorkflowExecutionUpdateCompletedV2 {
            outcome: tokeira_kernel::UpdateEventOutcome::Success(..),
            ..
        }
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
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: pending.logical_seq,
                    started_event_id: pending.started_event_id.unwrap(),
                    attempt: pending.attempt,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![WorkflowCommand::ProtocolMessage {
                    message_id: "msg-3".into(),
                    body: UpdateProtocolBody::Rejected {
                        update_id: "update-1".into(),
                        failure: payload("nope"),
                    },
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now: now(),
            }),
        )
        .unwrap();
    // A rejection writes NO history event — v1.31.0's
    // RejectWorkflowExecutionUpdate is a documented no-op, and the Go SDK
    // panics TMPRL1100-nondeterministic replaying any rejection event. The
    // update just leaves the admitted/pending sets (re-admittable).
    assert!(!transition.history_events.iter().any(|event| matches!(
        event.kind,
        HistoryEventKind::WorkflowExecutionUpdateRejected { .. }
    )));
    assert!(
        !transition
            .next_state
            .pending_updates
            .contains_key("update-1")
    );
    assert!(!transition.next_state.admitted_updates.contains("update-1"));
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
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: pending.logical_seq,
                    started_event_id: pending.started_event_id.unwrap(),
                    attempt: pending.attempt,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![WorkflowCommand::CompleteWorkflow {
                    result: payloads("done"),
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
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
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: pending.logical_seq,
                    started_event_id: pending.started_event_id.unwrap(),
                    attempt: pending.attempt,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![WorkflowCommand::RecordMarker {
                    marker_name: "marker".into(),
                    details: details.clone(),
                    failure: Some(payload("failure")),
                    header: header.clone(),
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
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
            ..
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
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: pending.logical_seq,
                    started_event_id: pending.started_event_id.unwrap(),
                    attempt: pending.attempt,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
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
                delivered_update_ids: Vec::new(),
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
        versioning_override: FieldChange::Set(VersioningOverride::AutoUpgrade),
        completion_callbacks: FieldChange::Set(vec![completion_callback()]),
        attached_completion_callbacks: Vec::new(),
        attached_links: Vec::new(),
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
    let mut expected_callback = completion_callback();
    expected_callback.registration_time = Some(req.now);
    let expected_callbacks = vec![expected_callback];

    assert_eq!(transition.request_dedupe_ops.len(), 1);
    assert!(matches!(
        &transition.history_events[0].kind,
        HistoryEventKind::WorkflowExecutionOptionsUpdated {
            versioning_override,
            completion_callbacks,
            attached_completion_callbacks,
            attached_links,
            attached_request_id,
        } if versioning_override == &req.versioning_override
            && completion_callbacks == &FieldChange::Set(expected_callbacks.clone())
            && attached_completion_callbacks == &req.attached_completion_callbacks
            && attached_links == &req.attached_links
            && attached_request_id == &req.attached_request_id
    ));
    assert_eq!(
        transition.next_state.versioning_override().cloned(),
        Some(VersioningOverride::AutoUpgrade)
    );
    assert_eq!(
        transition.next_state.completion_callbacks,
        expected_callbacks
    );
    // The attached request id authors this options-updated event and is recorded
    // in request_id_infos (UseExisting attach surfaces it on Describe, Req 5.3).
    let info = transition
        .next_state
        .request_id_infos
        .get("attached-1")
        .expect("attached request id recorded");
    assert_eq!(info.event_id, transition.next_state.last_event_id);
    assert_eq!(
        info.event_type,
        tokeira_kernel::EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED
    );
    assert!(!info.buffered);
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
                attached_completion_callbacks: Vec::new(),
                attached_links: Vec::new(),
                attached_request_id: None,
                request: request_context("options-clear"),
                now: now(),
            }),
        )
        .unwrap();

    assert_eq!(transition.next_state.versioning_override().cloned(), None);
    assert_eq!(
        transition.next_state.completion_callbacks,
        vec![completion_callback()]
    );
    assert!(transition.dispatch_ops.is_empty());
}

#[test]
fn update_execution_options_missing_run() {
    let reject = kernel()
        .apply(
            LoadedRun::Absent,
            Command::UpdateExecutionOptions(UpdateExecutionOptionsRequest {
                versioning_override: FieldChange::Unchanged,
                completion_callbacks: FieldChange::Unchanged,
                attached_completion_callbacks: Vec::new(),
                attached_links: Vec::new(),
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
                attached_completion_callbacks: Vec::new(),
                attached_links: Vec::new(),
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
        transition.next_state.versioning_override().cloned(),
        Some(VersioningOverride::AutoUpgrade)
    );
    let mut scheduled_callback = completion_callback();
    scheduled_callback.state = CallbackState::Scheduled;
    assert_eq!(
        transition.next_state.completion_callbacks,
        vec![scheduled_callback]
    );
    assert_eq!(
        transition
            .dispatch_ops
            .iter()
            .filter(|op| matches!(op, DispatchOp::DispatchCompletionCallback { .. }))
            .count(),
        1
    );
}

// ---- nexus-async-completion Wave 1: completion-callback outcome + lifecycle ----

/// An open run with a started WFT and a single Standby `WorkflowClosed` Nexus
/// completion callback, ready to be driven to a terminal state.
fn make_started_wft_with_standby_callback() -> WorkflowState {
    let mut state = make_open_state_with_started_wft();
    state.completion_callbacks = vec![completion_callback()];
    state
}

/// Token matching `make_open_state_with_started_wft`'s pending WFT.
fn started_wft_token(state: &WorkflowState) -> WorkflowTaskToken {
    WorkflowTaskToken {
        run_key: state.run_key,
        logical_seq: LogicalTaskSeq(3),
        started_event_id: 9,
        attempt: 1,
        shard_epoch: ShardEpoch::ZERO,
    }
}

/// Drive `state` to close by completing its started WFT with `commands`.
fn close_via_wft(state: WorkflowState, commands: Vec<WorkflowCommand>) -> Transition {
    let token = started_wft_token(&state);
    kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token,
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands,
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now: now(),
            }),
        )
        .unwrap()
}

/// Extract the single `DispatchCompletionCallback` outcome from a transition,
/// asserting exactly one callback was dispatched.
fn single_dispatched_outcome(transition: &Transition) -> CallbackCompletionOutcome {
    let mut dispatched = transition.dispatch_ops.iter().filter_map(|op| match op {
        DispatchOp::DispatchCompletionCallback { outcome, .. } => Some(outcome.clone()),
        _ => None,
    });
    let outcome = dispatched
        .next()
        .expect("a completion callback was dispatched on close");
    assert!(
        dispatched.next().is_none(),
        "exactly one completion callback dispatched"
    );
    outcome
}

#[test]
fn dispatch_completion_callback_outcome_completed() {
    let transition = close_via_wft(
        make_started_wft_with_standby_callback(),
        vec![WorkflowCommand::CompleteWorkflow {
            result: payloads("done"),
        }],
    );
    assert_eq!(
        single_dispatched_outcome(&transition),
        CallbackCompletionOutcome::Success {
            result: Some(payload("done")),
        }
    );
}

#[test]
fn dispatch_completion_callback_outcome_failed() {
    let transition = close_via_wft(
        make_started_wft_with_standby_callback(),
        vec![WorkflowCommand::FailWorkflow {
            failure: payload("nope"),
        }],
    );
    assert_eq!(
        single_dispatched_outcome(&transition),
        CallbackCompletionOutcome::Failed {
            failure: payload("nope"),
        }
    );
}

#[test]
fn dispatch_completion_callback_outcome_canceled() {
    let transition = close_via_wft(
        make_started_wft_with_standby_callback(),
        vec![WorkflowCommand::CancelWorkflow { details: None }],
    );
    assert_eq!(
        single_dispatched_outcome(&transition),
        CallbackCompletionOutcome::Canceled { details: None }
    );
}

#[test]
fn dispatch_completion_callback_outcome_continued_as_new() {
    let WorkflowCommand::ContinueAsNew { .. } = make_continue_as_new_command() else {
        unreachable!("make_continue_as_new_command builds a ContinueAsNew command");
    };
    let transition = close_via_wft(
        make_started_wft_with_standby_callback(),
        vec![make_continue_as_new_command()],
    );
    assert_eq!(
        single_dispatched_outcome(&transition),
        CallbackCompletionOutcome::ContinuedAsNew
    );
}

#[test]
fn dispatch_completion_callback_outcome_terminated() {
    let mut state = make_open_state();
    state.completion_callbacks = vec![completion_callback()];
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::Terminate(make_terminate_request()),
        )
        .unwrap();
    assert_eq!(
        single_dispatched_outcome(&transition),
        CallbackCompletionOutcome::Terminated
    );
}

#[test]
fn dispatch_completion_callback_outcome_timed_out() {
    let mut state = make_open_state();
    state.completion_callbacks = vec![completion_callback()];
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::WorkflowExecutionTimedOut(WorkflowExecutionTimedOutRequest {
                timeout_type: WorkflowTimeoutType::RunTimeout,
                retry_state: RetryState::Timeout,
                new_execution_run_id: None,
                now: now(),
            }),
        )
        .unwrap();
    assert_eq!(
        single_dispatched_outcome(&transition),
        CallbackCompletionOutcome::TimedOut
    );
}

/// A closed run carrying a single `Scheduled` completion callback — the state a
/// `CompletionCallbackAttempted` command targets (callbacks fire post-close).
fn make_closed_state_with_scheduled_callback() -> WorkflowState {
    let mut state = make_open_state();
    state.status = ExecutionStatus::Completed;
    state.closed_at = Some(now());
    let mut callback = completion_callback();
    callback.state = CallbackState::Scheduled;
    callback.registration_time = Some(now());
    state.completion_callbacks = vec![callback];
    state
}

fn attempt(outcome: CallbackAttemptOutcome) -> Command {
    Command::CompletionCallbackAttempted(CompletionCallbackAttemptedRequest {
        callback_index: 0,
        outcome,
        now: now(),
    })
}

#[test]
fn completion_callback_attempted_succeeded_is_terminal() {
    let state = make_closed_state_with_scheduled_callback();
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state.clone()),
            attempt(CallbackAttemptOutcome::Succeeded),
        )
        .unwrap();

    let callback = &transition.next_state.completion_callbacks[0];
    assert_eq!(callback.state, CallbackState::Succeeded);
    assert_eq!(callback.attempt, 0);
    assert_eq!(callback.next_attempt_at, None);
    assert_eq!(callback.last_attempt_failure, None);
    // No history event and no dispatch op: callback lifecycle is mutable state only.
    assert!(transition.history_events.is_empty());
    assert!(transition.dispatch_ops.is_empty());
    // The state-only commit still fences via transition_seq.
    assert_eq!(transition.expected_seq, state.transition_seq);
    assert_eq!(
        transition.next_state.transition_seq,
        state.transition_seq.next()
    );
}

#[test]
fn completion_callback_attempted_retryable_backs_off() {
    let state = make_closed_state_with_scheduled_callback();
    let retry_at = now() + Duration::seconds(1);
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            attempt(CallbackAttemptOutcome::RetryableFailure {
                failure: payload("503"),
                next_attempt_at: retry_at,
            }),
        )
        .unwrap();

    let callback = &transition.next_state.completion_callbacks[0];
    assert_eq!(callback.state, CallbackState::BackingOff);
    assert_eq!(callback.attempt, 1);
    assert_eq!(callback.next_attempt_at, Some(retry_at));
    assert_eq!(callback.last_attempt_failure, Some(payload("503")));
    assert!(transition.history_events.is_empty());
    assert!(transition.dispatch_ops.is_empty());
}

#[test]
fn completion_callback_attempted_non_retryable_is_terminal() {
    let state = make_closed_state_with_scheduled_callback();
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            attempt(CallbackAttemptOutcome::NonRetryableFailure {
                failure: payload("400"),
            }),
        )
        .unwrap();

    let callback = &transition.next_state.completion_callbacks[0];
    assert_eq!(callback.state, CallbackState::Failed);
    assert_eq!(callback.next_attempt_at, None);
    assert_eq!(callback.last_attempt_failure, Some(payload("400")));
    assert!(transition.history_events.is_empty());
    assert!(transition.dispatch_ops.is_empty());
}

#[test]
fn completion_callback_attempted_rejects_out_of_range_index() {
    let state = make_closed_state_with_scheduled_callback();
    let reject = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::CompletionCallbackAttempted(CompletionCallbackAttemptedRequest {
                callback_index: 7,
                outcome: CallbackAttemptOutcome::Succeeded,
                now: now(),
            }),
        )
        .unwrap_err();
    assert_eq!(reject, Reject::UnknownCompletionCallback(7));
}

#[test]
fn completion_callback_attempted_rejects_already_terminal() {
    let mut state = make_closed_state_with_scheduled_callback();
    state.completion_callbacks[0].state = CallbackState::Succeeded;
    let reject = kernel()
        .apply(
            LoadedRun::Existing(state),
            attempt(CallbackAttemptOutcome::Succeeded),
        )
        .unwrap_err();
    assert_eq!(reject, Reject::CompletionCallbackAlreadyTerminal(0));
}

#[test]
fn completion_callback_attempted_rejects_absent_run() {
    let reject = kernel()
        .apply(
            LoadedRun::Absent,
            attempt(CallbackAttemptOutcome::Succeeded),
        )
        .unwrap_err();
    assert_eq!(reject, Reject::MissingRun);
}

#[test]
fn schedule_nexus_operation_happy_path() {
    let state = make_open_state_with_started_wft();
    let pending = state.pending_workflow_task.clone().unwrap();
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: pending.logical_seq,
                    started_event_id: pending.started_event_id.unwrap(),
                    attempt: pending.attempt,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![WorkflowCommand::ScheduleNexusOperation {
                    operation_id: "op-1".into(),
                    endpoint: "endpoint".into(),
                    service: "service".into(),
                    operation: "method".into(),
                    input: payloads("input"),
                    schedule_to_close_timeout: Some(Duration::seconds(30)),
                    schedule_to_start_timeout: None,
                    start_to_close_timeout: None,
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
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
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: pending.logical_seq,
                    started_event_id: pending.started_event_id.unwrap(),
                    attempt: pending.attempt,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![WorkflowCommand::ScheduleNexusOperation {
                    operation_id: "op-1".into(),
                    endpoint: "endpoint".into(),
                    service: "service".into(),
                    operation: "method".into(),
                    input: payloads("input"),
                    schedule_to_close_timeout: None,
                    schedule_to_start_timeout: None,
                    start_to_close_timeout: None,
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
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
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: pending.logical_seq,
                    started_event_id: pending.started_event_id.unwrap(),
                    attempt: pending.attempt,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![WorkflowCommand::CancelNexusOperation {
                    scheduled_event_id: 12,
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
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
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: pending.logical_seq,
                    started_event_id: pending.started_event_id.unwrap(),
                    attempt: pending.attempt,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![WorkflowCommand::CancelNexusOperation {
                    scheduled_event_id: 12,
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
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
                resolution: NexusResolution::Started {
                    operation_token: "handler-token-xyz".into(),
                    links: Vec::new(),
                },
                now: now(),
            }),
        )
        .unwrap();

    // The handler-issued token is recorded verbatim on the started event so the
    // caller can read it as NexusOperationExecution.OperationToken and a later
    // cancel can send it back (`operation_token` field 5 @ v1.31.0).
    assert!(matches!(
        &transition.history_events[0].kind,
        HistoryEventKind::NexusOperationStarted {
            scheduled_event_id: 12,
            operation_token,
            ..
        } if operation_token == "handler-token-xyz"
    ));
    assert!(
        transition
            .next_state
            .pending_nexus_operations
            .get("op-1")
            .unwrap()
            .started
    );
    // NexusOperationStarted is a workflow-task trigger, so the started
    // transition schedules a WFT to deliver the event to the worker
    // (`StartedEventDefinition.IsWorkflowTaskTrigger() -> true`, v1.31.0).
    assert!(transition.next_state.pending_workflow_task.is_some());
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
                resolution: NexusResolution::Started {
                    operation_token: String::new(),
                    links: Vec::new(),
                },
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
                    links: Vec::new(),
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
                    links: Vec::new(),
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
                    failure: payload("nope"),
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
fn nexus_operation_resolved_attempt_failed_stays_pending() {
    // A retryable attempt failure backs the op off: it stays pending, records the
    // handler failure + next-attempt time + bumps the attempt count, and emits NO
    // history event and NO workflow task (v1.31.0 EventAttemptFailed is internal HSM
    // state — Invariant 1).
    let state = with_pending_nexus_operation(make_open_state(), "op-1");
    let next_attempt_at = now() + time::Duration::seconds(1);
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::NexusOperationResolved(NexusOperationResolvedRequest {
                operation_id: "op-1".into(),
                scheduled_event_id: 12,
                resolution: NexusResolution::AttemptFailed {
                    failure: payload("intentional internal error"),
                    next_attempt_at,
                },
                now: now(),
            }),
        )
        .unwrap();
    // No history event, no workflow task scheduled.
    assert!(transition.history_events.is_empty());
    assert!(transition.next_state.pending_workflow_task.is_none());
    // The operation is still pending, now carrying the recorded attempt state.
    let pending = transition
        .next_state
        .pending_nexus_operations
        .get("op-1")
        .expect("op stays pending after a retryable attempt failure");
    assert_eq!(pending.attempt, 1);
    assert_eq!(
        pending.last_attempt_failure,
        Some(payload("intentional internal error"))
    );
    assert_eq!(pending.next_attempt_at, Some(next_attempt_at));
}

#[test]
fn nexus_operation_resolved_attempt_failed_stale_rejected() {
    // A stale AttemptFailed (wrong scheduled_event_id) is rejected like any other
    // resolution — the shared fencing check guards re-dispatch races.
    let state = with_pending_nexus_operation(make_open_state(), "op-1");
    let result = kernel().apply(
        LoadedRun::Existing(state),
        Command::NexusOperationResolved(NexusOperationResolvedRequest {
            operation_id: "op-1".into(),
            scheduled_event_id: 999,
            resolution: NexusResolution::AttemptFailed {
                failure: payload("nope"),
                next_attempt_at: now(),
            },
            now: now(),
        }),
    );
    assert!(result.is_err());
}

#[test]
fn nexus_operation_retry_redispatches_and_clears_backoff() {
    // A backing-off op (as AttemptFailed leaves it) whose next attempt is due
    // re-dispatches: one ScheduleNexusOperation dispatch op, NO history event, and the
    // backoff state cleared (Invariant 4) while the attempt count is preserved.
    let mut state = with_pending_nexus_operation(make_open_state(), "op-1");
    if let Some(op) = state.pending_nexus_operations.get_mut("op-1") {
        op.attempt = 1;
        op.last_attempt_failure = Some(payload("intentional internal error"));
        op.next_attempt_at = Some(now());
    }
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::NexusOperationRetry(NexusOperationRetryRequest {
                operation_id: "op-1".into(),
                scheduled_event_id: 12,
                now: now(),
            }),
        )
        .unwrap();
    assert!(transition.history_events.is_empty());
    assert_eq!(transition.dispatch_ops.len(), 1);
    assert!(matches!(
        transition.dispatch_ops[0],
        DispatchOp::ScheduleNexusOperation { .. }
    ));
    let op = transition
        .next_state
        .pending_nexus_operations
        .get("op-1")
        .expect("op stays pending across a re-dispatch");
    assert!(op.last_attempt_failure.is_none());
    assert!(op.next_attempt_at.is_none());
    assert_eq!(op.attempt, 1);
}

#[test]
fn nexus_operation_retry_not_backing_off_rejected() {
    // A pending op that is not backing off (next_attempt_at None) rejects the retry —
    // the fence against a double re-dispatch from a concurrent scanner tick.
    let state = with_pending_nexus_operation(make_open_state(), "op-1");
    let result = kernel().apply(
        LoadedRun::Existing(state),
        Command::NexusOperationRetry(NexusOperationRetryRequest {
            operation_id: "op-1".into(),
            scheduled_event_id: 12,
            now: now(),
        }),
    );
    assert!(result.is_err());
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
                resolution: NexusResolution::TimedOut {
                    timeout_type: NexusTimeoutType::ScheduleToClose,
                },
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
                resolution: NexusResolution::Started {
                    operation_token: String::new(),
                    links: Vec::new(),
                },
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
                resolution: NexusResolution::Started {
                    operation_token: String::new(),
                    links: Vec::new(),
                },
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
                resolution: NexusResolution::Started {
                    operation_token: String::new(),
                    links: Vec::new(),
                },
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
                resolution: NexusResolution::Started {
                    operation_token: String::new(),
                    links: Vec::new(),
                },
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
                DispatchOp::ScheduleNexusOperation { .. } | DispatchOp::CancelNexusOperation { .. }
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
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: pending.logical_seq,
                    started_event_id: pending.started_event_id.unwrap(),
                    attempt: pending.attempt,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![WorkflowCommand::CompleteWorkflow {
                    result: payloads("done"),
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
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
                DispatchOp::ScheduleNexusOperation { .. } | DispatchOp::CancelNexusOperation { .. }
            ))
            .count(),
        0
    );
}

/// Feature: kernel-event-buffering, Golden G1 — message-too-large terminate.
///
/// Drives the full corpus lifecycle: start → WFT started → signal buffered
/// while the WFT is started → `RespondWorkflowTaskFailed(GRPC_MESSAGE_TOO_LARGE)`
/// force-close-terminate. The assembled history must exactly match the
/// v1.31.0 corpus assertion (`tests/workflow_test.go:993 @ v1.31.0`):
/// WorkflowExecutionStarted, WorkflowTaskScheduled, WorkflowTaskStarted,
/// WorkflowTaskFailed, WorkflowExecutionSignaled, WorkflowExecutionTerminated.
#[test]
fn golden_message_too_large_terminate_history() {
    let kernel = kernel();
    let start = make_start_request();

    // 1-2: WorkflowExecutionStarted + WorkflowTaskScheduled.
    let started = kernel
        .apply(LoadedRun::Absent, Command::Start(start))
        .unwrap();
    let pending = started
        .next_state
        .pending_workflow_task
        .clone()
        .expect("start schedules the first WFT");
    let mut history: Vec<HistoryEvent> = started.history_events.to_vec();

    // 3: WorkflowTaskStarted (worker polls, never responds successfully).
    let wft_started = kernel
        .apply(
            LoadedRun::Existing(started.next_state),
            Command::WorkflowTaskStarted(StartWorkflowTaskRequest {
                logical_seq: pending.logical_seq,
                worker_identity: WorkerIdentity("worker".into()),
                request_id: "g1-wft-start".into(),
                history_size_bytes: 0,
                suggest_continue_as_new: false,
                deployment_transition: None,
                deployment_transition_revision_number: None,
                sticky_ttl: None,
                now: now(),
            }),
        )
        .unwrap();
    let started_event_id = wft_started
        .next_state
        .pending_workflow_task
        .as_ref()
        .and_then(|wft| wft.started_event_id)
        .expect("WFT is started");
    history.extend(wft_started.history_events.iter().cloned());

    // Signal while the WFT is started: buffered, no history event.
    let signaled = kernel
        .apply(
            LoadedRun::Existing(wft_started.next_state),
            Command::Signal(SignalRequest {
                signal_name: "buffered-signal".into(),
                input: Payloads::default(),
                header: None,
                links: Vec::new(),
                request: request_context("g1-signal"),
                now: now(),
            }),
        )
        .unwrap();
    assert!(signaled.history_events.is_empty());
    assert_eq!(signaled.next_state.buffered_events.len(), 1);
    history.extend(signaled.history_events.iter().cloned());

    // 4-6: RespondWorkflowTaskFailed(GRPC_MESSAGE_TOO_LARGE) →
    // WorkflowTaskFailed(ForceCloseCommand), flushed WorkflowExecutionSignaled,
    // WorkflowExecutionTerminated. Reason/identity per v1.31.0:
    // `request.GetCause().String()` = "GrpcMessageTooLarge" and
    // `consts.IdentityHistoryService` = "history-service".
    let terminated = kernel
        .apply(
            LoadedRun::Existing(signaled.next_state),
            Command::TerminateOnWorkflowTaskFailed(TerminateOnWorkflowTaskFailedRequest {
                logical_seq: pending.logical_seq,
                started_event_id,
                reason: "GrpcMessageTooLarge".into(),
                identity: "history-service".into(),
                request: request_context("g1-terminate"),
                now: now(),
            }),
        )
        .unwrap();
    history.extend(terminated.history_events.iter().cloned());

    let kinds: Vec<&'static str> = history
        .iter()
        .map(|event| match &event.kind {
            HistoryEventKind::WorkflowExecutionStarted { .. } => "WorkflowExecutionStarted",
            HistoryEventKind::WorkflowTaskScheduled { .. } => "WorkflowTaskScheduled",
            HistoryEventKind::WorkflowTaskStarted { .. } => "WorkflowTaskStarted",
            HistoryEventKind::WorkflowTaskFailed { .. } => "WorkflowTaskFailed",
            HistoryEventKind::WorkflowExecutionSignaled { .. } => "WorkflowExecutionSignaled",
            HistoryEventKind::WorkflowExecutionTerminated { .. } => "WorkflowExecutionTerminated",
            other => panic!("unexpected event kind in G1 history: {other:?}"),
        })
        .collect();
    assert_eq!(
        kinds,
        vec![
            "WorkflowExecutionStarted",
            "WorkflowTaskScheduled",
            "WorkflowTaskStarted",
            "WorkflowTaskFailed",
            "WorkflowExecutionSignaled",
            "WorkflowExecutionTerminated",
        ],
        "history must match tests/workflow_test.go:993 @ v1.31.0"
    );
    // Event ids are the contiguous corpus ids 1..=6.
    let ids: Vec<i64> = history.iter().map(|event| event.event_id).collect();
    assert_eq!(ids, vec![1, 2, 3, 4, 5, 6]);

    // The force-close WFT-failed event carries ForceCloseCommand (the inbound
    // GrpcMessageTooLarge only selects the terminate route, Req 4.2.2).
    match &history[3].kind {
        HistoryEventKind::WorkflowTaskFailed { failure_cause, .. } => {
            assert_eq!(failure_cause, &WorkflowTaskFailedCause::ForceCloseCommand);
        }
        other => panic!("expected WorkflowTaskFailed, got {other:?}"),
    }
    match &history[5].kind {
        HistoryEventKind::WorkflowExecutionTerminated {
            reason, identity, ..
        } => {
            assert_eq!(reason, "GrpcMessageTooLarge");
            assert_eq!(identity, "history-service");
        }
        other => panic!("expected WorkflowExecutionTerminated, got {other:?}"),
    }
    assert_eq!(terminated.next_state.status, ExecutionStatus::Terminated);
    assert!(terminated.next_state.buffered_events.is_empty());

    // Stale-token fencing: a second message-too-large for the same (now
    // closed) run rejects rather than mutating (Req 4.2.4 / 5.1).
    let reject = kernel
        .apply(
            LoadedRun::Existing(terminated.next_state),
            Command::TerminateOnWorkflowTaskFailed(TerminateOnWorkflowTaskFailedRequest {
                logical_seq: pending.logical_seq,
                started_event_id,
                reason: "GrpcMessageTooLarge".into(),
                identity: "history-service".into(),
                request: request_context("g1-terminate-stale"),
                now: now(),
            }),
        )
        .unwrap_err();
    assert_eq!(reject, Reject::RunClosed(ExecutionStatus::Terminated));
}

// ─── Feature: speculative-wft — kernel goldens (spec speculative-wft K.4-K.6) ───
//
// All goldens assume the standard corpus 4-event lead-in (1
// WorkflowExecutionStarted, 2 WorkflowTaskScheduled, 3 WorkflowTaskStarted,
// 4 WorkflowTaskCompleted): last_event_id = 4, previous_started_event_id = 3,
// no pending WFT.

fn make_quiescent_state_after_first_wft() -> WorkflowState {
    let mut state = make_open_state();
    state.last_event_id = 4;
    state.previous_started_event_id = 3;
    state
}

/// Admit an update via the kernel, returning the post-admission state (a
/// pending SPECULATIVE task when the run was quiescent, per K2).
fn admit_update(state: WorkflowState, update_id: &str) -> WorkflowState {
    kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::Update(UpdateRequest {
                update_id: update_id.into(),
                update_name: "handler".into(),
                input: payloads("input"),
                request: request_context(&format!("admit-{update_id}")),
                now: now(),
            }),
        )
        .unwrap()
        .next_state
}

/// Start the pending workflow task through the kernel command path.
fn start_pending_task(state: WorkflowState) -> Transition {
    let pending = state.pending_workflow_task.clone().unwrap();
    kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::WorkflowTaskStarted(StartWorkflowTaskRequest {
                logical_seq: pending.logical_seq,
                worker_identity: WorkerIdentity("worker".into()),
                request_id: "speculative-start".into(),
                history_size_bytes: 0,
                suggest_continue_as_new: false,
                deployment_transition: None,
                deployment_transition_revision_number: None,
                sticky_ttl: None,
                now: now(),
            }),
        )
        .unwrap()
}

fn speculative_completion_request(
    state: &WorkflowState,
    commands: Vec<WorkflowCommand>,
) -> WorkflowTaskCompletedRequest {
    let pending = state.pending_workflow_task.clone().unwrap();
    WorkflowTaskCompletedRequest {
        client_discards_speculative_with_events: false,
        token: WorkflowTaskToken {
            run_key: state.run_key,
            logical_seq: pending.logical_seq,
            started_event_id: pending.started_event_id.unwrap(),
            attempt: pending.attempt,
            shard_epoch: ShardEpoch::ZERO,
        },
        identity: WorkerIdentity("worker".into()),
        sdk_metadata: None,
        metering_metadata: None,
        worker_version: None,
        versioning_behavior: VersioningBehavior::Unspecified,
        deployment_version: None,
        worker_deployment_name: None,
        sticky: None,
        commands,
        force_new_workflow_task: false,
        delivered_update_ids: Vec::new(),
        now: now(),
    }
}

fn accepted_body(update_id: &str, sequencing_event_id: i64) -> UpdateProtocolBody {
    UpdateProtocolBody::Accepted {
        update_id: update_id.into(),
        update_name: "handler".into(),
        input: payloads("input"),
        sequencing_event_id,
    }
}

// Feature: speculative-wft, Golden (a) / P1 — schedule + start persist
// nothing: virtual ids scheduled = last_event_id + 1, started = scheduled + 1
// (workflow_task_state_machine.go:309-410, 556-627 @ v1.31.0; Req 1.1, 2.1).
#[test]
fn speculative_start_persists_nothing() {
    let state = admit_update(make_quiescent_state_after_first_wft(), "update-1");
    let started = start_pending_task(state);
    assert!(started.history_events.is_empty());
    assert_eq!(started.next_state.last_event_id, 4);
    let pending = started.next_state.pending_workflow_task.as_ref().unwrap();
    assert_eq!(
        pending.task_type,
        tokeira_kernel::WorkflowTaskType::Speculative
    );
    assert_eq!(pending.scheduled_event_id, 5);
    assert_eq!(pending.started_event_id, Some(6));
}

// Feature: speculative-wft, Golden (b) / P2 — a rejection-only completion of
// a started speculative task DROPS without trace: zero history events,
// last_event_id unchanged, pending task cleared, the rejected update gone
// from the admitted set (`skipWorkflowTaskCompletedEvent`,
// workflow_task_state_machine.go:676-748 @ v1.31.0; Req 3.1). The follow-up
// rule (K7) fires when OTHER admitted updates remain: a fresh SPECULATIVE
// task is scheduled (respondworkflowtaskcompleted/api.go:512-541 @ v1.31.0;
// Req 8).
#[test]
fn speculative_rejection_only_completion_drops_without_trace() {
    let state = admit_update(make_quiescent_state_after_first_wft(), "update-1");
    let started = start_pending_task(state).next_state;

    // Plain drop: nothing left admitted, no successor task.
    let dropped = kernel()
        .apply(
            LoadedRun::Existing(started.clone()),
            Command::WorkflowTaskCompleted(speculative_completion_request(
                &started,
                vec![WorkflowCommand::ProtocolMessage {
                    message_id: "msg-reject".into(),
                    body: UpdateProtocolBody::Rejected {
                        update_id: "update-1".into(),
                        failure: payload("rejected"),
                    },
                }],
            )),
        )
        .unwrap();
    assert!(dropped.history_events.is_empty());
    assert_eq!(dropped.next_state.last_event_id, 4);
    assert_eq!(dropped.next_state.previous_started_event_id, 3);
    assert!(dropped.next_state.pending_workflow_task.is_none());
    assert!(!dropped.next_state.admitted_updates.contains("update-1"));
    // The drop carries no dispatch except the M.1 rollback metric op.
    assert_eq!(dropped.dispatch_ops.len(), 1);
    assert!(matches!(
        dropped.dispatch_ops[0],
        DispatchOp::RecordSpeculativeOutcome {
            committed: false,
            ..
        }
    ));

    // K7 follow-up: a second update admitted while the task ran keeps a
    // delivery vehicle — the drop schedules a fresh speculative task.
    let with_second = admit_update(started.clone(), "update-2");
    let dropped = kernel()
        .apply(
            LoadedRun::Existing(with_second.clone()),
            Command::WorkflowTaskCompleted(speculative_completion_request(
                &with_second,
                vec![WorkflowCommand::ProtocolMessage {
                    message_id: "msg-reject".into(),
                    body: UpdateProtocolBody::Rejected {
                        update_id: "update-1".into(),
                        failure: payload("rejected"),
                    },
                }],
            )),
        )
        .unwrap();
    assert!(dropped.history_events.is_empty());
    assert_eq!(dropped.next_state.last_event_id, 4);
    let follow_up = dropped.next_state.pending_workflow_task.as_ref().unwrap();
    assert_eq!(
        follow_up.task_type,
        tokeira_kernel::WorkflowTaskType::Speculative
    );
    assert_eq!(follow_up.scheduled_event_id, 5);
    assert!(dropped.next_state.admitted_updates.contains("update-2"));
    // The drop still rolls back (M.1) and, because another update remains
    // admitted, schedules a fresh speculative follow-up (K7).
    assert!(dropped.dispatch_ops.iter().any(|op| matches!(
        op,
        DispatchOp::EnqueueWorkflowTask {
            speculative: true,
            ..
        }
    )));
    assert!(dropped.dispatch_ops.iter().any(|op| matches!(
        op,
        DispatchOp::RecordSpeculativeOutcome {
            committed: false,
            ..
        }
    )));
}

// Feature: speculative-wft, Golden (c) / P3 / design G1 — an accept+complete
// completion MATERIALIZES the task late: Scheduled(5), Started(6),
// Completed(7), then UpdateAccepted(8) carrying the worker sequencing id (5 =
// the delivering virtual scheduled id) and the failure-capable V2
// Completed(9) anchored at accepted_event_id 8
// (`AddWorkflowTaskCompletedEvent`, workflow_task_state_machine.go:750-819;
// event_factory.go:424-456 @ v1.31.0; Req 3.4, 7.1, 7.2).
#[test]
fn speculative_accept_complete_materializes_late() {
    let state = admit_update(make_quiescent_state_after_first_wft(), "update-1");
    let started = start_pending_task(state).next_state;

    let transition = kernel()
        .apply(
            LoadedRun::Existing(started.clone()),
            Command::WorkflowTaskCompleted(speculative_completion_request(
                &started,
                vec![
                    WorkflowCommand::ProtocolMessage {
                        message_id: "msg-accept".into(),
                        body: accepted_body("update-1", 5),
                    },
                    WorkflowCommand::ProtocolMessage {
                        message_id: "msg-complete".into(),
                        body: UpdateProtocolBody::Completed {
                            update_id: "update-1".into(),
                            result: payloads("done"),
                            failure: None,
                        },
                    },
                ],
            )),
        )
        .unwrap();

    let ids: Vec<i64> = transition
        .history_events
        .iter()
        .map(|event| event.event_id)
        .collect();
    assert_eq!(ids, vec![5, 6, 7, 8, 9]);
    assert!(matches!(
        transition.history_events[0].kind,
        HistoryEventKind::WorkflowTaskScheduled { attempt: 1, .. }
    ));
    assert!(matches!(
        transition.history_events[1].kind,
        HistoryEventKind::WorkflowTaskStarted {
            scheduled_event_id: 5,
            ..
        }
    ));
    assert!(matches!(
        transition.history_events[2].kind,
        HistoryEventKind::WorkflowTaskCompleted {
            scheduled_event_id: 5,
            started_event_id: 6,
            ..
        }
    ));
    match &transition.history_events[3].kind {
        HistoryEventKind::WorkflowExecutionUpdateAccepted {
            update_id,
            accepted_request_sequencing_event_id,
            ..
        } => {
            assert_eq!(update_id, "update-1");
            // Worker-provided anchor recorded verbatim (Req 7.1).
            assert_eq!(*accepted_request_sequencing_event_id, 5);
        }
        other => panic!("expected UpdateAccepted, got {other:?}"),
    }
    match &transition.history_events[4].kind {
        HistoryEventKind::WorkflowExecutionUpdateCompletedV2 {
            update_id,
            accepted_event_id,
            outcome,
            ..
        } => {
            assert_eq!(update_id, "update-1");
            assert_eq!(*accepted_event_id, 8);
            assert_eq!(
                outcome,
                &tokeira_kernel::UpdateEventOutcome::Success(payloads("done"))
            );
        }
        other => panic!("expected V2 UpdateCompleted, got {other:?}"),
    }
    assert_eq!(transition.next_state.last_event_id, 9);
    assert_eq!(transition.next_state.previous_started_event_id, 6);
    assert!(transition.next_state.pending_workflow_task.is_none());
}

// Feature: speculative-wft, Golden (d) / P4 — a signal while the speculative
// task is SCHEDULED converts it in place with the Scheduled event BEFORE the
// signal: Scheduled(5), Signaled(6), then the start persists Started(7)
// (`convertSpeculativeWorkflowTaskToNormal`,
// workflow_task_state_machine.go:1466-1530 @ v1.31.0; corpus
// `TestScheduledSpeculativeWorkflowTask_ConvertToNormalBecauseOfSignal`;
// Req 4.1).
#[test]
fn speculative_signal_while_scheduled_converts_scheduled_before_signaled() {
    let state = admit_update(make_quiescent_state_after_first_wft(), "update-1");
    let scheduled_at = state.pending_workflow_task.as_ref().unwrap().scheduled_at;

    let signaled = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::Signal(SignalRequest {
                signal_name: "sig".into(),
                input: payloads("signal"),
                header: None,
                links: Vec::new(),
                request: request_context("signal-convert"),
                now: now() + Duration::seconds(1),
            }),
        )
        .unwrap();

    assert_eq!(signaled.history_events.len(), 2);
    match &signaled.history_events[0] {
        HistoryEvent {
            event_id,
            happened_at,
            kind: HistoryEventKind::WorkflowTaskScheduled { attempt, .. },
        } => {
            assert_eq!(*event_id, 5);
            // The materialized Scheduled event keeps the task's original
            // schedule time (`wt.ScheduledTime` @ v1.31.0).
            assert_eq!(*happened_at, scheduled_at);
            assert_eq!(*attempt, 1);
        }
        other => panic!("expected WorkflowTaskScheduled, got {other:?}"),
    }
    assert!(matches!(
        signaled.history_events[1].kind,
        HistoryEventKind::WorkflowExecutionSignaled { .. }
    ));
    assert_eq!(signaled.history_events[1].event_id, 6);
    let pending = signaled.next_state.pending_workflow_task.as_ref().unwrap();
    assert_eq!(pending.task_type, tokeira_kernel::WorkflowTaskType::Normal);
    assert_eq!(pending.scheduled_event_id, 5);
    assert!(pending.started_event_id.is_none());

    // The converted (now normal) task starts with a REAL Started event at 7.
    let started = start_pending_task(signaled.next_state);
    assert_eq!(started.history_events.len(), 1);
    assert!(matches!(
        started.history_events[0].kind,
        HistoryEventKind::WorkflowTaskStarted {
            scheduled_event_id: 5,
            ..
        }
    ));
    assert_eq!(started.history_events[0].event_id, 7);
}

// Feature: speculative-wft, Golden (K4.2) / P4 — a signal BUFFERED while the
// speculative task is STARTED blocks the drop: the rejection-only completion
// MATERIALIZES and the buffered signal flushes AFTER the completed event,
// then the flushed event forces a real follow-up task — corpus shape
// 5 WTScheduled, 6 WTStarted, 7 WTCompleted, 8 Signaled, 9 WTScheduled
// (`TestStartedSpeculativeWorkflowTask_ConvertToNormalBecauseOfBufferedSignal`;
// Req 4.2).
#[test]
fn speculative_buffered_signal_blocks_drop_and_flushes_after_completed() {
    let state = admit_update(make_quiescent_state_after_first_wft(), "update-1");
    let started = start_pending_task(state).next_state;

    let buffered = kernel()
        .apply(
            LoadedRun::Existing(started),
            Command::Signal(SignalRequest {
                signal_name: "sig".into(),
                input: payloads("signal"),
                header: None,
                links: Vec::new(),
                request: request_context("signal-buffer"),
                now: now() + Duration::seconds(1),
            }),
        )
        .unwrap();
    assert!(buffered.history_events.is_empty());
    assert_eq!(buffered.next_state.buffered_events.len(), 1);

    let completed = kernel()
        .apply(
            LoadedRun::Existing(buffered.next_state.clone()),
            Command::WorkflowTaskCompleted(speculative_completion_request(
                &buffered.next_state,
                vec![WorkflowCommand::ProtocolMessage {
                    message_id: "msg-reject".into(),
                    body: UpdateProtocolBody::Rejected {
                        update_id: "update-1".into(),
                        failure: payload("rejected"),
                    },
                }],
            )),
        )
        .unwrap();

    let kinds: Vec<&HistoryEventKind> = completed
        .history_events
        .iter()
        .map(|event| &event.kind)
        .collect();
    assert_eq!(completed.history_events.len(), 5);
    assert!(matches!(
        kinds[0],
        HistoryEventKind::WorkflowTaskScheduled { .. }
    ));
    assert!(matches!(
        kinds[1],
        HistoryEventKind::WorkflowTaskStarted { .. }
    ));
    assert!(matches!(
        kinds[2],
        HistoryEventKind::WorkflowTaskCompleted { .. }
    ));
    assert!(matches!(
        kinds[3],
        HistoryEventKind::WorkflowExecutionSignaled { .. }
    ));
    assert!(matches!(
        kinds[4],
        HistoryEventKind::WorkflowTaskScheduled { .. }
    ));
    assert_eq!(completed.history_events[3].event_id, 8);
    // The flushed-signal follow-up is a NORMAL task with a real Scheduled
    // event at 9.
    assert_eq!(completed.history_events[4].event_id, 9);
    let follow_up = completed.next_state.pending_workflow_task.as_ref().unwrap();
    assert_eq!(
        follow_up.task_type,
        tokeira_kernel::WorkflowTaskType::Normal
    );
    assert_eq!(follow_up.scheduled_event_id, 9);
}

// Feature: speculative-wft, Golden (K4.3) / P5 — START_TO_CLOSE on a started
// speculative task persists Scheduled(5) + Started(6) late, then
// WorkflowTaskTimedOut(7), and the attempt-2 retry stays virtual (transient)
// (`AddWorkflowTaskTimedOutEvent`, workflow_task_state_machine.go:934-960;
// timer_queue_active_task_executor.go:425-440 @ v1.31.0; Req 5.1; corpus
// `TestSpeculativeWorkflowTask_StartToCloseTimeout`).
#[test]
fn speculative_start_to_close_timeout_persists_and_retries_transient() {
    let state = admit_update(make_quiescent_state_after_first_wft(), "update-1");
    let started = start_pending_task(state).next_state;
    let pending = started.pending_workflow_task.clone().unwrap();

    let timed_out = kernel()
        .apply(
            LoadedRun::Existing(started),
            Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest {
                logical_seq: pending.logical_seq,
                started_event_id: pending.started_event_id.unwrap(),
                timeout_type: WorkflowTaskTimeoutType::StartToClose,
                now: now() + Duration::seconds(10),
            }),
        )
        .unwrap();

    assert_eq!(timed_out.history_events.len(), 3);
    assert!(matches!(
        timed_out.history_events[0].kind,
        HistoryEventKind::WorkflowTaskScheduled { attempt: 1, .. }
    ));
    assert!(matches!(
        timed_out.history_events[1].kind,
        HistoryEventKind::WorkflowTaskStarted {
            scheduled_event_id: 5,
            ..
        }
    ));
    assert!(matches!(
        timed_out.history_events[2].kind,
        HistoryEventKind::WorkflowTaskTimedOut {
            scheduled_event_id: 5,
            started_event_id: 6,
            timeout_type: WorkflowTaskTimeoutType::StartToClose,
            ..
        }
    ));
    assert_eq!(timed_out.next_state.last_event_id, 7);
    // Attempt-2 retry: transient — no persisted Scheduled event, virtual id 8.
    let retry = timed_out.next_state.pending_workflow_task.as_ref().unwrap();
    assert_eq!(retry.attempt, 2);
    assert_eq!(retry.task_type, tokeira_kernel::WorkflowTaskType::Normal);
    assert_eq!(retry.scheduled_event_id, 8);
    // The still-admitted update survives for redelivery on the retry.
    assert!(timed_out.next_state.admitted_updates.contains("update-1"));
}

// Feature: speculative-wft, Golden (K4.3) / P5 — SCHEDULE_TO_START on a
// scheduled speculative task persists Scheduled(5) +
// WorkflowTaskTimedOut(6, SCHEDULE_TO_START), resets the attempt to 1, and
// reschedules a REAL normal-queue task with Scheduled(7)
// (`AddWorkflowTaskScheduleToStartTimeoutEvent`,
// workflow_task_state_machine.go:270-305 + the 5s in-memory timer,
// workflow_task_timer.go:15-19 @ v1.31.0; Req 5.2/5.3; corpus
// `TestSpeculativeWorkflowTask_ScheduleToStartTimeout*`).
#[test]
fn speculative_schedule_to_start_timeout_persists_and_reschedules_real() {
    let state = admit_update(make_quiescent_state_after_first_wft(), "update-1");
    let pending = state.pending_workflow_task.clone().unwrap();

    let timed_out = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest {
                logical_seq: pending.logical_seq,
                started_event_id: 0,
                timeout_type: WorkflowTaskTimeoutType::ScheduleToStart,
                now: now() + Duration::seconds(5),
            }),
        )
        .unwrap();

    assert_eq!(timed_out.history_events.len(), 3);
    assert!(matches!(
        timed_out.history_events[0].kind,
        HistoryEventKind::WorkflowTaskScheduled { attempt: 1, .. }
    ));
    assert_eq!(timed_out.history_events[0].event_id, 5);
    assert!(matches!(
        timed_out.history_events[1].kind,
        HistoryEventKind::WorkflowTaskTimedOut {
            scheduled_event_id: 5,
            started_event_id: 0,
            timeout_type: WorkflowTaskTimeoutType::ScheduleToStart,
            ..
        }
    ));
    assert!(matches!(
        timed_out.history_events[2].kind,
        HistoryEventKind::WorkflowTaskScheduled { attempt: 1, .. }
    ));
    assert_eq!(timed_out.history_events[2].event_id, 7);
    let retry = timed_out.next_state.pending_workflow_task.as_ref().unwrap();
    assert_eq!(retry.attempt, 1);
    assert_eq!(retry.task_type, tokeira_kernel::WorkflowTaskType::Normal);
    assert_eq!(retry.scheduled_event_id, 7);
    assert!(matches!(
        timed_out.dispatch_ops[0],
        DispatchOp::EnqueueWorkflowTask {
            speculative: false,
            ..
        }
    ));
}

// Feature: speculative-wft, Golden (e) — the bad-update-message taxonomy
// (Req 6.1/6.2 + owner amendment F5): double-accept and zero sequencing id
// reject with `BadUpdateMessage` (InvalidArgument flavor); an unknown accept
// without the echoed request is NotFound; an unknown accept WITH the echoed
// request RESURRECTS (`checkStateSet`, update/update.go:648-656;
// `validateAcceptanceMsg`, update/validation.go:62-68; `TryResurrect`,
// update/registry.go:238-281;
// workflow_task_completed_handler.go:375-425 @ v1.31.0).
#[test]
fn bad_update_message_taxonomy() {
    let state = admit_update(make_quiescent_state_after_first_wft(), "update-1");
    let started = start_pending_task(state).next_state;

    // Double accept in one completion → invalid state transition, rendered at
    // the in-flight state ProvisionallyAccepted.
    let reject = kernel()
        .apply(
            LoadedRun::Existing(started.clone()),
            Command::WorkflowTaskCompleted(speculative_completion_request(
                &started,
                vec![
                    WorkflowCommand::ProtocolMessage {
                        message_id: "msg-accept-1".into(),
                        body: accepted_body("update-1", 5),
                    },
                    WorkflowCommand::ProtocolMessage {
                        message_id: "msg-accept-2".into(),
                        body: accepted_body("update-1", 5),
                    },
                ],
            )),
        )
        .unwrap_err();
    assert_eq!(
        reject,
        Reject::BadUpdateMessage {
            message: "invalid state transition attempted for Update update-1: received \
                      *update.Acceptance message while in state ProvisionallyAccepted"
                .into(),
            not_found: false,
        }
    );

    // Accept for an update accepted by a PREVIOUS task → state Accepted.
    let previously_accepted = with_pending_update(started.clone(), "update-2");
    let reject = kernel()
        .apply(
            LoadedRun::Existing(previously_accepted.clone()),
            Command::WorkflowTaskCompleted(speculative_completion_request(
                &previously_accepted,
                vec![WorkflowCommand::ProtocolMessage {
                    message_id: "msg-accept".into(),
                    body: accepted_body("update-2", 5),
                }],
            )),
        )
        .unwrap_err();
    assert_eq!(
        reject,
        Reject::BadUpdateMessage {
            message: "invalid state transition attempted for Update update-2: received \
                      *update.Acceptance message while in state Accepted"
                .into(),
            not_found: false,
        }
    );

    // Unknown update, no echoed request → NotFound, exact v1.31.0 message
    // (workflow_task_completed_handler.go:381).
    let reject = kernel()
        .apply(
            LoadedRun::Existing(started.clone()),
            Command::WorkflowTaskCompleted(speculative_completion_request(
                &started,
                vec![WorkflowCommand::ProtocolMessage {
                    message_id: "msg-accept".into(),
                    body: UpdateProtocolBody::Accepted {
                        update_id: "update-ghost".into(),
                        update_name: String::new(),
                        input: Payloads::default(),
                        sequencing_event_id: 5,
                    },
                }],
            )),
        )
        .unwrap_err();
    assert_eq!(
        reject,
        Reject::BadUpdateMessage {
            message: "update update-ghost wasn't found on the server. This is most likely a \
                      transient error which will be resolved automatically by retries"
                .into(),
            not_found: true,
        }
    );

    // Unknown update WITH the echoed request → resurrect and accept.
    let resurrected = kernel()
        .apply(
            LoadedRun::Existing(started.clone()),
            Command::WorkflowTaskCompleted(speculative_completion_request(
                &started,
                vec![WorkflowCommand::ProtocolMessage {
                    message_id: "msg-accept".into(),
                    body: accepted_body("update-ghost", 5),
                }],
            )),
        )
        .unwrap();
    assert!(
        resurrected
            .next_state
            .pending_updates
            .contains_key("update-ghost")
    );
    assert!(resurrected.history_events.iter().any(|event| matches!(
        &event.kind,
        HistoryEventKind::WorkflowExecutionUpdateAccepted { update_id, .. }
            if update_id == "update-ghost"
    )));

    // Zero sequencing id → exact validateAcceptanceMsg rendering
    // (update/validation.go:62-68 @ v1.31.0).
    let reject = kernel()
        .apply(
            LoadedRun::Existing(started.clone()),
            Command::WorkflowTaskCompleted(speculative_completion_request(
                &started,
                vec![WorkflowCommand::ProtocolMessage {
                    message_id: "msg-accept".into(),
                    body: accepted_body("update-1", 0),
                }],
            )),
        )
        .unwrap_err();
    assert_eq!(
        reject,
        Reject::BadUpdateMessage {
            message: "invalid *update.Acceptance: accepted_request_sequencing_event_id is not set"
                .into(),
            not_found: false,
        }
    );

    // Out-of-range sequencing id → owner amendment F5 bounds check.
    let reject = kernel()
        .apply(
            LoadedRun::Existing(started.clone()),
            Command::WorkflowTaskCompleted(speculative_completion_request(
                &started,
                vec![WorkflowCommand::ProtocolMessage {
                    message_id: "msg-accept".into(),
                    body: accepted_body("update-1", 999),
                }],
            )),
        )
        .unwrap_err();
    assert!(matches!(
        reject,
        Reject::BadUpdateMessage {
            not_found: false,
            ..
        }
    ));

    // Complete-before-accept: the update the task delivered is in state Sent.
    let reject = kernel()
        .apply(
            LoadedRun::Existing(started.clone()),
            Command::WorkflowTaskCompleted(speculative_completion_request(
                &started,
                vec![WorkflowCommand::ProtocolMessage {
                    message_id: "msg-complete".into(),
                    body: UpdateProtocolBody::Completed {
                        update_id: "update-1".into(),
                        result: payloads("done"),
                        failure: None,
                    },
                }],
            )),
        )
        .unwrap_err();
    assert_eq!(
        reject,
        Reject::BadUpdateMessage {
            message: "invalid state transition attempted for Update update-1: received \
                      *update.Response message while in state Sent"
                .into(),
            not_found: false,
        }
    );
}

// Feature: speculative-wft — a Response with a Failure outcome is a COMPLETED
// update whose handler failed post-acceptance: it materializes the task and
// persists the failure-capable V2 event, NOT a rejection (Req 7.2;
// mutable_state_impl.go:5288-5378 @ v1.31.0).
#[test]
fn update_completed_with_failure_outcome_persists_v2_failure() {
    let state = admit_update(make_quiescent_state_after_first_wft(), "update-1");
    let started = start_pending_task(state).next_state;

    let transition = kernel()
        .apply(
            LoadedRun::Existing(started.clone()),
            Command::WorkflowTaskCompleted(speculative_completion_request(
                &started,
                vec![
                    WorkflowCommand::ProtocolMessage {
                        message_id: "msg-accept".into(),
                        body: accepted_body("update-1", 5),
                    },
                    WorkflowCommand::ProtocolMessage {
                        message_id: "msg-complete".into(),
                        body: UpdateProtocolBody::Completed {
                            update_id: "update-1".into(),
                            result: Payloads::default(),
                            failure: Some(payload("handler-failed")),
                        },
                    },
                ],
            )),
        )
        .unwrap();

    match &transition.history_events[4].kind {
        HistoryEventKind::WorkflowExecutionUpdateCompletedV2 {
            accepted_event_id,
            outcome,
            ..
        } => {
            assert_eq!(*accepted_event_id, 8);
            assert_eq!(
                outcome,
                &tokeira_kernel::UpdateEventOutcome::Failure(payload("handler-failed"))
            );
        }
        other => panic!("expected V2 UpdateCompleted, got {other:?}"),
    }
    assert!(
        !transition
            .next_state
            .pending_updates
            .contains_key("update-1")
    );
}

// Feature: speculative-wft — an explicit WFT failure (RespondWorkflowTaskFailed
// or the bad-update-message seam) on a STARTED speculative task persists
// Scheduled(5) + Started(6) late, then WorkflowTaskFailed(7); the retry is an
// ordinary transient attempt-2 and the update stays admitted for redelivery
// (`AddWorkflowTaskFailedEvent`, workflow_task_state_machine.go:865-891
// @ v1.31.0; corpus `TestSpeculativeWorkflowTask_Fail` distinction, Req 6.3).
#[test]
fn speculative_explicit_fail_materializes_and_keeps_update_admitted() {
    let state = admit_update(make_quiescent_state_after_first_wft(), "update-1");
    let started = start_pending_task(state).next_state;
    let pending = started.pending_workflow_task.clone().unwrap();

    let failed = kernel()
        .apply(
            LoadedRun::Existing(started),
            Command::WorkflowTaskFailed(WorkflowTaskFailedRequest {
                logical_seq: pending.logical_seq,
                started_event_id: pending.started_event_id.unwrap(),
                failure_cause: WorkflowTaskFailedCause::BadUpdateWorkflowExecutionMessage,
                failure_details: None,
                worker_identity: WorkerIdentity("worker".into()),
                now: now() + Duration::seconds(1),
            }),
        )
        .unwrap();

    assert_eq!(failed.history_events.len(), 3);
    assert!(matches!(
        failed.history_events[0].kind,
        HistoryEventKind::WorkflowTaskScheduled { attempt: 1, .. }
    ));
    assert!(matches!(
        failed.history_events[1].kind,
        HistoryEventKind::WorkflowTaskStarted {
            scheduled_event_id: 5,
            ..
        }
    ));
    match &failed.history_events[2].kind {
        HistoryEventKind::WorkflowTaskFailed {
            scheduled_event_id,
            started_event_id,
            failure_cause,
            ..
        } => {
            assert_eq!(*scheduled_event_id, 5);
            assert_eq!(*started_event_id, 6);
            assert_eq!(
                failure_cause,
                &WorkflowTaskFailedCause::BadUpdateWorkflowExecutionMessage
            );
        }
        other => panic!("expected WorkflowTaskFailed, got {other:?}"),
    }
    // Retry is transient attempt-2 with a virtual scheduled id; the update
    // stays admitted for redelivery.
    let retry = failed.next_state.pending_workflow_task.as_ref().unwrap();
    assert_eq!(retry.attempt, 2);
    assert_eq!(retry.task_type, tokeira_kernel::WorkflowTaskType::Normal);
    assert_eq!(retry.scheduled_event_id, 8);
    assert!(failed.next_state.admitted_updates.contains("update-1"));
}
