use std::collections::BTreeMap;

use proptest::prelude::*;
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    event::HistoryEventKind, kernel::Kernel, ActivityOp, ActivityResolution,
    ActivityResolvedRequest, ActivityState, BasicKernel, Command, DispatchOp, LoadedRun,
    PendingWorkflowTask, RequestDedupeOp, SignalRequest, StartRequest, StartWorkflowTaskRequest,
    TimerDueRequest, TimerOp, TimerState, WorkflowCommand, WorkflowState,
    WorkflowTaskCompletedRequest, WorkflowTaskFailedCause, WorkflowTaskFailedRequest,
    WorkflowTaskTimedOutRequest, WorkflowTaskTimeoutType,
};
use tokeira_types::{
    ExecutionStatus, LogicalTaskSeq, Memo, NamespaceId, Payload, Payloads, RequestContext,
    RequestId, RetryPolicy, RunId, RunKey, SearchAttrValue, SearchAttributes, ShardEpoch,
    StickyAffinity, TaskQueueName, TransitionSeq, WorkerIdentity, WorkflowId, WorkflowTaskToken,
    WorkflowType,
};

fn fixed_now() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()
}

fn default_workflow_task_timeout() -> Duration {
    Duration::seconds(10)
}

fn payload(data: &str) -> Payload {
    Payload::new(data.as_bytes().to_vec())
}

fn payloads(data: &str) -> Payloads {
    Payloads(vec![payload(data)])
}

fn memo_with(value: &str) -> Memo {
    Memo(BTreeMap::from([("k".into(), payload(value))]))
}

fn search_attrs_with(value: &str) -> SearchAttributes {
    SearchAttributes(BTreeMap::from([(
        "keyword".into(),
        SearchAttrValue::Keyword(value.into()),
    )]))
}

fn request_context(id: &str, now: OffsetDateTime) -> RequestContext {
    RequestContext {
        request_id: RequestId(id.into()),
        caller_identity: Some("caller".into()),
        received_at: now,
    }
}

fn sample_retry_policy() -> RetryPolicy {
    RetryPolicy {
        initial_interval: Duration::seconds(1),
        backoff_coefficient: 2.0,
        maximum_interval: Some(Duration::seconds(100)),
        maximum_attempts: 5,
        non_retryable_error_types: vec!["fatal".into()],
    }
}

fn make_open_state(now: OffsetDateTime) -> WorkflowState {
    WorkflowState {
        run_key: RunKey::new(),
        namespace_id: NamespaceId::new(),
        workflow_id: WorkflowId("workflow".into()),
        run_id: RunId::new(),
        workflow_type: WorkflowType("wf".into()),
        task_queue: TaskQueueName("queue".into()),
        status: ExecutionStatus::Running,
        transition_seq: TransitionSeq(7),
        last_event_id: 14,
        next_workflow_task_seq: LogicalTaskSeq(4),
        pending_workflow_task: None,
        sticky: None,
        memo: memo_with("memo"),
        search_attributes: search_attrs_with("search"),
        workflow_execution_timeout: Some(Duration::minutes(5)),
        workflow_run_timeout: Some(Duration::minutes(1)),
        workflow_task_timeout: default_workflow_task_timeout(),
        retry_policy: Some(sample_retry_policy()),
        attempt: 2,
        activities: BTreeMap::new(),
        timers: BTreeMap::new(),
        started_at: now - Duration::minutes(10),
        closed_at: None,
    }
}

fn with_pending_wft(mut state: WorkflowState, logical_seq: u64, started_event_id: Option<i64>, attempt: u32) -> WorkflowState {
    state.pending_workflow_task = Some(PendingWorkflowTask {
        logical_seq: LogicalTaskSeq(logical_seq),
        scheduled_event_id: state.last_event_id - 1,
        started_event_id,
        attempt,
    });
    state
}

fn with_sticky(mut state: WorkflowState, worker_identity: &str, now: OffsetDateTime) -> WorkflowState {
    state.sticky = Some(StickyAffinity {
        worker_identity: WorkerIdentity(worker_identity.into()),
        expires_at: now + Duration::seconds(30),
    });
    state
}

fn with_activity(mut state: WorkflowState, activity_id: &str) -> WorkflowState {
    state.activities.insert(
        activity_id.into(),
        ActivityState {
            activity_id: activity_id.into(),
            schedule_event_id: state.last_event_id - 2,
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

fn with_timer(mut state: WorkflowState, timer_id: &str, now: OffsetDateTime) -> WorkflowState {
    state.timers.insert(
        timer_id.into(),
        TimerState {
            timer_id: timer_id.into(),
            started_event_id: state.last_event_id - 2,
            fire_at: now,
        },
    );
    state
}

// --- Arbitrary strategies ---

fn arb_duration() -> impl Strategy<Value = Duration> {
    (1i64..600).prop_map(Duration::seconds)
}

fn arb_small_string() -> impl Strategy<Value = String> {
    "[a-z0-9]{1,8}".prop_map(|s| s)
}

fn arb_payload() -> impl Strategy<Value = Payload> {
    prop::collection::vec(any::<u8>(), 0..8).prop_map(Payload::new)
}

fn arb_payloads() -> impl Strategy<Value = Payloads> {
    prop::collection::vec(arb_payload(), 0..3).prop_map(Payloads)
}

fn arb_memo() -> impl Strategy<Value = Memo> {
    prop::collection::btree_map(arb_small_string(), arb_payload(), 0..3).prop_map(Memo)
}

fn arb_search_attributes() -> impl Strategy<Value = SearchAttributes> {
    prop::collection::btree_map(arb_small_string(), arb_small_string(), 0..3).prop_map(|m| {
        SearchAttributes(
            m.into_iter()
                .map(|(k, v)| (k, SearchAttrValue::Keyword(v)))
                .collect(),
        )
    })
}

fn arb_retry_policy() -> impl Strategy<Value = RetryPolicy> {
    (
        arb_duration(),
        1.0f64..5.0f64,
        prop::option::of(arb_duration()),
        1u32..10u32,
        prop::collection::vec(arb_small_string(), 0..3),
    )
        .prop_map(
            |(initial_interval, backoff_coefficient, maximum_interval, maximum_attempts, non_retryable_error_types)| RetryPolicy {
                initial_interval,
                backoff_coefficient,
                maximum_interval,
                maximum_attempts,
                non_retryable_error_types,
            },
        )
}

fn arb_start_request() -> impl Strategy<Value = StartRequest> {
    (
        arb_payloads(),
        arb_memo(),
        arb_search_attributes(),
        prop::option::of(arb_duration()),
        prop::option::of(arb_duration()),
        arb_duration(),
        prop::option::of(arb_retry_policy()),
        1u32..10u32,
    )
        .prop_map(|(input, memo, search_attributes, workflow_execution_timeout, workflow_run_timeout, workflow_task_timeout, retry_policy, attempt)| {
            let now = fixed_now();
            let run_id = RunId::new();
            StartRequest {
                run_key: RunKey::new(),
                namespace_id: NamespaceId::new(),
                workflow_id: WorkflowId("workflow".into()),
                run_id,
                workflow_type: WorkflowType("wf".into()),
                task_queue: TaskQueueName("queue".into()),
                input,
                memo,
                search_attributes,
                workflow_execution_timeout,
                workflow_run_timeout,
                workflow_task_timeout,
                retry_policy,
                attempt,
                continued_execution_run_id: None,
                first_execution_run_id: Some(run_id),
                request: request_context("prop-start", now),
                now,
            }
        })
}

fn arb_activity_resolution() -> impl Strategy<Value = ActivityResolution> {
    prop_oneof![
        arb_payloads().prop_map(|result| ActivityResolution::Completed { result }),
        arb_small_string().prop_map(|message| ActivityResolution::Failed { message }),
        arb_small_string().prop_map(|timeout_type| ActivityResolution::TimedOut { timeout_type }),
        prop::option::of(arb_payloads()).prop_map(|details| ActivityResolution::Canceled { details }),
    ]
}

fn arb_schedule_activity_command() -> impl Strategy<Value = WorkflowCommand> {
    (
        arb_small_string(),
        arb_small_string(),
        arb_payloads(),
        prop::option::of(arb_duration()),
        prop::option::of(arb_duration()),
        prop::option::of(arb_duration()),
        prop::option::of(arb_duration()),
    )
        .prop_map(
            |(activity_id, task_queue, input, schedule_to_close_timeout, schedule_to_start_timeout, start_to_close_timeout, heartbeat_timeout)| WorkflowCommand::ScheduleActivity {
                activity_id,
                task_queue: TaskQueueName(task_queue),
                input,
                schedule_to_close_timeout,
                schedule_to_start_timeout,
                start_to_close_timeout,
                heartbeat_timeout,
            },
        )
}

fn arb_wft_failed_cause() -> impl Strategy<Value = WorkflowTaskFailedCause> {
    prop_oneof![
        Just(WorkflowTaskFailedCause::NonDeterminismError),
        Just(WorkflowTaskFailedCause::BadScheduleActivityAttributes),
        Just(WorkflowTaskFailedCause::BadStartTimerAttributes),
        Just(WorkflowTaskFailedCause::UnhandledCommand),
        Just(WorkflowTaskFailedCause::BadRequestCancelActivityAttributes),
        Just(WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure),
        Just(WorkflowTaskFailedCause::BadSignalWorkflowExecutionAttributes),
    ]
}

fn arb_wft_failed_request(
    logical_seq: LogicalTaskSeq,
    started_event_id: i64,
    now: OffsetDateTime,
) -> impl Strategy<Value = WorkflowTaskFailedRequest> {
    (
        arb_wft_failed_cause(),
        prop::option::of(arb_payload()),
        arb_small_string(),
    ).prop_map(move |(failure_cause, failure_details, worker_identity)| WorkflowTaskFailedRequest {
        logical_seq,
        started_event_id,
        failure_cause,
        failure_details,
        worker_identity: WorkerIdentity(worker_identity),
        now,
    })
}

fn arb_wft_timed_out_request(
    logical_seq: LogicalTaskSeq,
    started_event_id: i64,
    now: OffsetDateTime,
) -> impl Strategy<Value = WorkflowTaskTimedOutRequest> {
    Just(WorkflowTaskTimedOutRequest {
        logical_seq,
        started_event_id,
        timeout_type: WorkflowTaskTimeoutType::StartToClose,
        now,
    })
}

fn arb_valid_pair() -> impl Strategy<Value = (LoadedRun, Command)> {
    let now = fixed_now();
    prop_oneof![
        arb_start_request().prop_map(|req| (LoadedRun::Absent, Command::Start(req))),
        arb_payloads().prop_map(move |input| {
            let state = make_open_state(now);
            let req = SignalRequest {
                signal_name: "sig".into(),
                input,
                request: request_context("prop-signal", now),
                now,
            };
            (LoadedRun::Existing(state), Command::Signal(req))
        }),
        (0u64..10u64).prop_map(move |offset| {
            let logical_seq = 20 + offset;
            let state = with_pending_wft(make_open_state(now), logical_seq, None, 0);
            let req = StartWorkflowTaskRequest {
                logical_seq: LogicalTaskSeq(logical_seq),
                worker_identity: WorkerIdentity("worker".into()),
                sticky_ttl: Some(Duration::seconds(30)),
                now,
            };
            (LoadedRun::Existing(state), Command::WorkflowTaskStarted(req))
        }),
        prop_oneof![
            Just(vec![WorkflowCommand::RequestNewWorkflowTask]),
            arb_schedule_activity_command().prop_map(|cmd| vec![cmd]),
            arb_payloads().prop_map(|result| vec![WorkflowCommand::CompleteWorkflow { result }]),
            (arb_small_string(), prop::option::of(arb_payload())).prop_map(|(message, details)| vec![WorkflowCommand::FailWorkflow { message, details }]),
        ]
        .prop_map(move |commands| {
            let state = with_pending_wft(make_open_state(now), 30, Some(13), 1);
            let req = WorkflowTaskCompletedRequest {
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(30),
                    started_event_id: 13,
                    attempt: 1,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                commands,
                force_new_workflow_task: false,
                now,
            };
            (LoadedRun::Existing(state), Command::WorkflowTaskCompleted(req))
        }),
        arb_activity_resolution().prop_map(move |resolution| {
            let state = with_activity(make_open_state(now), "activity-1");
            let req = ActivityResolvedRequest {
                activity_id: "activity-1".into(),
                resolution,
                worker_identity: Some(WorkerIdentity("worker".into())),
                now,
            };
            (LoadedRun::Existing(state), Command::ActivityResolved(req))
        }),
        Just(()).prop_map(move |_| {
            let state = with_timer(make_open_state(now), "timer-1", now);
            let req = TimerDueRequest {
                timer_id: "timer-1".into(),
                fired_at: now,
            };
            (LoadedRun::Existing(state), Command::TimerDue(req))
        }),
        prop::bool::ANY.prop_flat_map(move |sticky| {
            let logical_seq = LogicalTaskSeq(40);
            let started_event_id = 15;
            let mut state = with_pending_wft(make_open_state(now), logical_seq.0, Some(started_event_id), 1);
            if sticky {
                state = with_sticky(state, "sticky-worker", now);
            }
            arb_wft_failed_request(logical_seq, started_event_id, now)
                .prop_map(move |req| (LoadedRun::Existing(state.clone()), Command::WorkflowTaskFailed(req)))
        }),
        prop::bool::ANY.prop_flat_map(move |sticky| {
            let logical_seq = LogicalTaskSeq(41);
            let started_event_id = 16;
            let mut state = with_pending_wft(make_open_state(now), logical_seq.0, Some(started_event_id), 1);
            if sticky {
                state = with_sticky(state, "sticky-worker", now);
            }
            arb_wft_timed_out_request(logical_seq, started_event_id, now)
                .prop_map(move |req| (LoadedRun::Existing(state.clone()), Command::WorkflowTaskTimedOut(req)))
        }),
    ]
}

fn kernel() -> BasicKernel {
    BasicKernel
}

proptest! {
    #[test]
    fn property_1_start_field_pass_through(req in arb_start_request()) {
        let transition = kernel().apply(LoadedRun::Absent, Command::Start(req.clone())).unwrap();
        let started = match &transition.history_events[0].kind {
            HistoryEventKind::WorkflowExecutionStarted {
                continued_execution_run_id,
                first_execution_run_id,
                retry_policy,
                attempt,
                workflow_execution_timeout,
                workflow_run_timeout,
                workflow_task_timeout,
                ..
            } => (
                *continued_execution_run_id,
                *first_execution_run_id,
                retry_policy.clone(),
                *attempt,
                *workflow_execution_timeout,
                *workflow_run_timeout,
                *workflow_task_timeout,
            ),
            other => panic!("unexpected first event: {other:?}"),
        };

        prop_assert_eq!(transition.next_state.workflow_execution_timeout, req.workflow_execution_timeout);
        prop_assert_eq!(transition.next_state.workflow_run_timeout, req.workflow_run_timeout);
        prop_assert_eq!(transition.next_state.workflow_task_timeout, req.workflow_task_timeout);
        prop_assert_eq!(transition.next_state.retry_policy.clone(), req.retry_policy);
        prop_assert_eq!(transition.next_state.attempt, req.attempt);
        prop_assert_eq!(started.0, req.continued_execution_run_id);
        prop_assert_eq!(started.1, req.first_execution_run_id);
        prop_assert_eq!(started.2, transition.next_state.retry_policy.clone());
        prop_assert_eq!(started.3, transition.next_state.attempt);
        prop_assert_eq!(started.4, transition.next_state.workflow_execution_timeout);
        prop_assert_eq!(started.5, transition.next_state.workflow_run_timeout);
        prop_assert_eq!(started.6, transition.next_state.workflow_task_timeout);
    }

    #[test]
    fn property_2_activity_resolution_event_matches_variant(resolution in arb_activity_resolution()) {
        let now = fixed_now();
        let transition = kernel().apply(
            LoadedRun::Existing(with_activity(make_open_state(now), "activity-1")),
            Command::ActivityResolved(ActivityResolvedRequest {
                activity_id: "activity-1".into(),
                resolution: resolution.clone(),
                worker_identity: Some(WorkerIdentity("worker".into())),
                now,
            }),
        ).unwrap();

        let terminal = &transition.history_events[0].kind;
        match (terminal, resolution) {
            (HistoryEventKind::ActivityTaskCompleted { activity_id, result }, ActivityResolution::Completed { result: expected }) => {
                prop_assert_eq!(activity_id, "activity-1");
                prop_assert_eq!(result, &expected);
            }
            (HistoryEventKind::ActivityTaskFailed { activity_id, message }, ActivityResolution::Failed { message: expected }) => {
                prop_assert_eq!(activity_id, "activity-1");
                prop_assert_eq!(message, &expected);
            }
            (HistoryEventKind::ActivityTaskTimedOut { activity_id, timeout_type }, ActivityResolution::TimedOut { timeout_type: expected }) => {
                prop_assert_eq!(activity_id, "activity-1");
                prop_assert_eq!(timeout_type, &expected);
            }
            (HistoryEventKind::ActivityTaskCanceled { activity_id, details }, ActivityResolution::Canceled { details: expected }) => {
                prop_assert_eq!(activity_id, "activity-1");
                prop_assert_eq!(details, &expected);
            }
            other => panic!("mismatched terminal event: {other:?}"),
        }
    }

    #[test]
    fn property_3_schedule_activity_timeout_pass_through(cmd in arb_schedule_activity_command()) {
        let now = fixed_now();
        let state = with_pending_wft(make_open_state(now), 30, Some(13), 1);
        let transition = kernel().apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(30),
                    started_event_id: 13,
                    attempt: 1,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                commands: vec![cmd.clone()],
                force_new_workflow_task: false,
                now,
            }),
        ).unwrap();

        let (expected_id, expected_s2c, expected_s2s, expected_stc, expected_hb) = match cmd {
            WorkflowCommand::ScheduleActivity {
                activity_id, schedule_to_close_timeout, schedule_to_start_timeout,
                start_to_close_timeout, heartbeat_timeout, ..
            } => (activity_id, schedule_to_close_timeout, schedule_to_start_timeout, start_to_close_timeout, heartbeat_timeout),
            _ => unreachable!(),
        };

        let scheduled = transition.history_events.iter().find_map(|event| match &event.kind {
            HistoryEventKind::ActivityTaskScheduled {
                activity_id, schedule_to_close_timeout, schedule_to_start_timeout,
                start_to_close_timeout, heartbeat_timeout, ..
            } => Some((activity_id.clone(), *schedule_to_close_timeout, *schedule_to_start_timeout, *start_to_close_timeout, *heartbeat_timeout)),
            _ => None,
        }).unwrap();
        let activity = transition.next_state.activities.get(&expected_id).unwrap();
        let dispatch = transition.dispatch_ops.iter().find_map(|op| match op {
            DispatchOp::EnqueueActivityTask {
                activity_id, schedule_to_close_timeout, schedule_to_start_timeout,
                start_to_close_timeout, heartbeat_timeout, ..
            } => Some((activity_id.clone(), *schedule_to_close_timeout, *schedule_to_start_timeout, *start_to_close_timeout, *heartbeat_timeout)),
            _ => None,
        }).unwrap();

        prop_assert_eq!(scheduled.0, expected_id.clone());
        prop_assert_eq!(scheduled.1, expected_s2c);
        prop_assert_eq!(scheduled.2, expected_s2s);
        prop_assert_eq!(scheduled.3, expected_stc);
        prop_assert_eq!(scheduled.4, expected_hb);
        prop_assert_eq!(activity.schedule_to_close_timeout, expected_s2c);
        prop_assert_eq!(activity.schedule_to_start_timeout, expected_s2s);
        prop_assert_eq!(activity.start_to_close_timeout, expected_stc);
        prop_assert_eq!(activity.heartbeat_timeout, expected_hb);
        prop_assert_eq!(dispatch.0, expected_id);
        prop_assert_eq!(dispatch.1, expected_s2c);
        prop_assert_eq!(dispatch.2, expected_s2s);
        prop_assert_eq!(dispatch.3, expected_stc);
        prop_assert_eq!(dispatch.4, expected_hb);
    }

    #[test]
    fn property_4_event_id_contiguity((loaded, command) in arb_valid_pair()) {
        let last_event_id = match &loaded {
            LoadedRun::Absent => 0,
            LoadedRun::Existing(state) => state.last_event_id,
        };
        let transition = kernel().apply(loaded, command).unwrap();
        for (index, event) in transition.history_events.iter().enumerate() {
            prop_assert_eq!(event.event_id, last_event_id + index as i64 + 1);
        }
    }

    #[test]
    fn property_5_transition_sequence_increment((loaded, command) in arb_valid_pair()) {
        let input_seq = match &loaded {
            LoadedRun::Absent => TransitionSeq::ZERO,
            LoadedRun::Existing(state) => state.transition_seq,
        };
        let transition = kernel().apply(loaded, command).unwrap();
        prop_assert_eq!(transition.expected_seq, input_seq);
        prop_assert_eq!(transition.next_state.transition_seq, input_seq.next());
    }

    #[test]
    fn property_6_pending_wft_identity_preservation(signal_input in arb_payloads()) {
        let now = fixed_now();
        let base = with_pending_wft(make_open_state(now), 99, None, 0);

        let signal_transition = kernel().apply(
            LoadedRun::Existing(base.clone()),
            Command::Signal(SignalRequest {
                signal_name: "sig".into(),
                input: signal_input,
                request: request_context("signal", now),
                now,
            }),
        ).unwrap();
        prop_assert_eq!(signal_transition.next_state.pending_workflow_task.as_ref().unwrap().logical_seq, LogicalTaskSeq(99));
        prop_assert_eq!(
            signal_transition.dispatch_ops.iter().all(|op| !matches!(op, DispatchOp::EnqueueWorkflowTask { .. })),
            true
        );

        let activity_transition = kernel().apply(
            LoadedRun::Existing(with_activity(base.clone(), "activity-1")),
            Command::ActivityResolved(ActivityResolvedRequest {
                activity_id: "activity-1".into(),
                resolution: ActivityResolution::Completed { result: payloads("done") },
                worker_identity: None,
                now,
            }),
        ).unwrap();
        prop_assert_eq!(activity_transition.next_state.pending_workflow_task.as_ref().unwrap().logical_seq, LogicalTaskSeq(99));
        prop_assert_eq!(
            activity_transition.dispatch_ops.iter().all(|op| !matches!(op, DispatchOp::EnqueueWorkflowTask { .. })),
            true
        );

        let timer_transition = kernel().apply(
            LoadedRun::Existing(with_timer(base, "timer-1", now)),
            Command::TimerDue(TimerDueRequest {
                timer_id: "timer-1".into(),
                fired_at: now,
            }),
        ).unwrap();
        prop_assert_eq!(timer_transition.next_state.pending_workflow_task.as_ref().unwrap().logical_seq, LogicalTaskSeq(99));
        prop_assert_eq!(
            timer_transition.dispatch_ops.iter().all(|op| !matches!(op, DispatchOp::EnqueueWorkflowTask { .. })),
            true
        );
    }

    #[test]
    fn property_7_closed_workflow_no_schedule((loaded, command) in arb_valid_pair()) {
        let transition = kernel().apply(loaded, command).unwrap();
        if transition.next_state.status != ExecutionStatus::Running {
            prop_assert_eq!(
                transition.dispatch_ops.iter().all(|op| !matches!(op, DispatchOp::EnqueueWorkflowTask { .. } | DispatchOp::EnqueueActivityTask { .. })),
                true
            );
            prop_assert!(transition.next_state.pending_workflow_task.is_none());
            prop_assert!(transition.next_state.closed_at.is_some());
        }
    }

    #[test]
    fn property_8_last_event_id_consistency((loaded, command) in arb_valid_pair()) {
        let input_last_event_id = match &loaded {
            LoadedRun::Absent => 0,
            LoadedRun::Existing(state) => state.last_event_id,
        };
        let transition = kernel().apply(loaded, command).unwrap();
        if let Some(last) = transition.history_events.last() {
            prop_assert_eq!(transition.next_state.last_event_id, last.event_id);
        } else {
            prop_assert_eq!(transition.next_state.last_event_id, input_last_event_id);
        }
    }

    #[test]
    fn property_9_activity_and_timer_op_consistency((loaded, command) in arb_valid_pair()) {
        let transition = kernel().apply(loaded, command).unwrap();
        for op in &transition.activity_ops {
            match op {
                ActivityOp::Upsert(activity) => prop_assert_eq!(transition.next_state.activities.get(&activity.activity_id), Some(activity)),
                ActivityOp::Delete { activity_id } => prop_assert!(!transition.next_state.activities.contains_key(activity_id)),
            }
        }
        for op in &transition.timer_ops {
            match op {
                TimerOp::Upsert(timer) => prop_assert_eq!(transition.next_state.timers.get(&timer.timer_id), Some(timer)),
                TimerOp::Delete { timer_id } => prop_assert!(!transition.next_state.timers.contains_key(timer_id)),
            }
        }
    }

    #[test]
    fn property_10_request_dedup_correctness((loaded, command) in arb_valid_pair()) {
        let transition = kernel().apply(loaded, command.clone()).unwrap();
        match command {
            Command::Start(req) => {
                prop_assert_eq!(transition.request_dedupe_ops.len(), 1);
                prop_assert_eq!(transition.request_dedupe_ops[0].clone(), RequestDedupeOp { request_id: req.request.request_id });
            }
            Command::Signal(req) => {
                prop_assert_eq!(transition.request_dedupe_ops.len(), 1);
                prop_assert_eq!(transition.request_dedupe_ops[0].clone(), RequestDedupeOp { request_id: req.request.request_id });
            }
            _ => prop_assert!(transition.request_dedupe_ops.is_empty()),
        }
    }

    #[test]
    fn property_11_wft_failed_event_field_pass_through(req in arb_wft_failed_request(LogicalTaskSeq(50), 21, fixed_now())) {
        let now = fixed_now();
        let state = with_pending_wft(make_open_state(now), 50, Some(21), 1);
        let transition = kernel().apply(LoadedRun::Existing(state), Command::WorkflowTaskFailed(req.clone())).unwrap();
        match &transition.history_events[0].kind {
            HistoryEventKind::WorkflowTaskFailed { logical_seq, scheduled_event_id, started_event_id, failure_cause, failure_details, identity } => {
                prop_assert_eq!(*logical_seq, LogicalTaskSeq(50));
                prop_assert_eq!(*scheduled_event_id, 13);
                prop_assert_eq!(*started_event_id, 21);
                prop_assert_eq!(failure_cause, &req.failure_cause);
                prop_assert_eq!(failure_details, &req.failure_details);
                prop_assert_eq!(identity, &req.worker_identity);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn property_12_wft_timed_out_event_field_pass_through(req in arb_wft_timed_out_request(LogicalTaskSeq(51), 22, fixed_now())) {
        let now = fixed_now();
        let state = with_pending_wft(make_open_state(now), 51, Some(22), 1);
        let transition = kernel().apply(LoadedRun::Existing(state), Command::WorkflowTaskTimedOut(req.clone())).unwrap();
        match &transition.history_events[0].kind {
            HistoryEventKind::WorkflowTaskTimedOut { logical_seq, scheduled_event_id, started_event_id, timeout_type } => {
                prop_assert_eq!(*logical_seq, LogicalTaskSeq(51));
                prop_assert_eq!(*scheduled_event_id, 13);
                prop_assert_eq!(*started_event_id, 22);
                prop_assert_eq!(timeout_type, &req.timeout_type);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn property_13_failure_timeout_preserve_pending_wft_identity(req in arb_wft_failed_request(LogicalTaskSeq(60), 30, fixed_now())) {
        let now = fixed_now();
        let failed_transition = kernel().apply(
            LoadedRun::Existing(with_pending_wft(make_open_state(now), 60, Some(30), 1)),
            Command::WorkflowTaskFailed(req),
        ).unwrap();
        let failed_pending = failed_transition.next_state.pending_workflow_task.unwrap();
        prop_assert_eq!(failed_pending.logical_seq, LogicalTaskSeq(60));
        prop_assert_eq!(failed_pending.scheduled_event_id, 13);
        prop_assert_eq!(failed_pending.started_event_id, None);

        let timed_out_transition = kernel().apply(
            LoadedRun::Existing(with_pending_wft(make_open_state(now), 61, Some(31), 1)),
            Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest {
                logical_seq: LogicalTaskSeq(61),
                started_event_id: 31,
                timeout_type: WorkflowTaskTimeoutType::StartToClose,
                now,
            }),
        ).unwrap();
        let timed_out_pending = timed_out_transition.next_state.pending_workflow_task.unwrap();
        prop_assert_eq!(timed_out_pending.logical_seq, LogicalTaskSeq(61));
        prop_assert_eq!(timed_out_pending.scheduled_event_id, 13);
        prop_assert_eq!(timed_out_pending.started_event_id, None);
    }

    #[test]
    fn property_14_wft_failed_preserves_sticky(req in arb_wft_failed_request(LogicalTaskSeq(70), 40, fixed_now())) {
        let now = fixed_now();
        let state = with_sticky(with_pending_wft(make_open_state(now), 70, Some(40), 1), "sticky-worker", now);
        let transition = kernel().apply(LoadedRun::Existing(state.clone()), Command::WorkflowTaskFailed(req)).unwrap();
        prop_assert_eq!(transition.next_state.sticky, state.sticky);
        match &transition.dispatch_ops[0] {
            DispatchOp::EnqueueWorkflowTask { sticky_preferred, .. } => {
                prop_assert_eq!(sticky_preferred, &Some(WorkerIdentity("sticky-worker".into())));
            }
            other => panic!("unexpected dispatch op: {other:?}"),
        }
    }

    #[test]
    fn property_16_failure_timeout_minimal_side_effects(req in arb_wft_failed_request(LogicalTaskSeq(80), 50, fixed_now())) {
        let now = fixed_now();
        let failed_transition = kernel().apply(
            LoadedRun::Existing(with_pending_wft(make_open_state(now), 80, Some(50), 1)),
            Command::WorkflowTaskFailed(req),
        ).unwrap();
        prop_assert_eq!(failed_transition.history_events.len(), 1);
        prop_assert_eq!(failed_transition.dispatch_ops.len(), 1);
        prop_assert_eq!(matches!(failed_transition.history_events[0].kind, HistoryEventKind::WorkflowTaskFailed { .. }), true);
        prop_assert_eq!(matches!(failed_transition.dispatch_ops[0], DispatchOp::EnqueueWorkflowTask { logical_seq: LogicalTaskSeq(80), .. }), true);
        prop_assert!(failed_transition.request_dedupe_ops.is_empty());
        prop_assert!(failed_transition.activity_ops.is_empty());
        prop_assert!(failed_transition.timer_ops.is_empty());
        prop_assert!(failed_transition.projection_ops.is_empty());
        prop_assert_eq!(failed_transition.next_state.status, ExecutionStatus::Running);

        let timed_out_transition = kernel().apply(
            LoadedRun::Existing(with_pending_wft(make_open_state(now), 81, Some(51), 1)),
            Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest {
                logical_seq: LogicalTaskSeq(81),
                started_event_id: 51,
                timeout_type: WorkflowTaskTimeoutType::StartToClose,
                now,
            }),
        ).unwrap();
        prop_assert_eq!(timed_out_transition.history_events.len(), 1);
        prop_assert_eq!(timed_out_transition.dispatch_ops.len(), 1);
        prop_assert_eq!(matches!(timed_out_transition.history_events[0].kind, HistoryEventKind::WorkflowTaskTimedOut { .. }), true);
        prop_assert_eq!(matches!(timed_out_transition.dispatch_ops[0], DispatchOp::EnqueueWorkflowTask { logical_seq: LogicalTaskSeq(81), .. }), true);
        prop_assert!(timed_out_transition.request_dedupe_ops.is_empty());
        prop_assert!(timed_out_transition.activity_ops.is_empty());
        prop_assert!(timed_out_transition.timer_ops.is_empty());
        prop_assert!(timed_out_transition.projection_ops.is_empty());
        prop_assert_eq!(timed_out_transition.next_state.status, ExecutionStatus::Running);
    }
}

// Property 15 is not property-based (deterministic single case), so it lives outside the proptest! block.
#[test]
fn property_15_wft_timed_out_clears_sticky() {
    let now = fixed_now();
    let state = with_sticky(with_pending_wft(make_open_state(now), 71, Some(41), 1), "sticky-worker", now);
    let transition = kernel().apply(
        LoadedRun::Existing(state),
        Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest {
            logical_seq: LogicalTaskSeq(71),
            started_event_id: 41,
            timeout_type: WorkflowTaskTimeoutType::StartToClose,
            now,
        }),
    ).unwrap();
    assert_eq!(transition.next_state.sticky, None);
    match &transition.dispatch_ops[0] {
        DispatchOp::EnqueueWorkflowTask { sticky_preferred, .. } => {
            assert_eq!(sticky_preferred, &None);
        }
        other => panic!("unexpected dispatch op: {other:?}"),
    }
}
