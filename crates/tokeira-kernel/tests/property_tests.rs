use std::collections::BTreeMap;

use proptest::prelude::*;
use prost::Message;
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    ActivityOp, ActivityPauseInfo, ActivityResolution, ActivityResolvedRequest, ActivityState,
    BasicKernel, CallbackAttemptOutcome, CallbackCompletionOutcome, CallbackSpec, CallbackState,
    CallbackTrigger, CancelRequest, ChildResolution, ChildResolvedRequest,
    ChildStartConfirmedRequest, ChildStartResult, ChildWorkflowState, Command, CompletionCallback,
    CompletionCallbackAttemptedRequest, ContinueAsNewVersioningBehavior, DispatchOp,
    ExternalCancelResolvedRequest, ExternalCancelResult, ExternalSignalResolvedRequest,
    ExternalSignalResult, ExternalWorkflowExecution, FieldChange, Link, LoadedRun,
    NexusOperationResolvedRequest, NexusResolution, NexusTimeoutType, ParentClosePolicy,
    PauseActivityRequest, PauseInfo, PauseWorkflowRequest, PendingExternalCancel,
    PendingExternalSignal, PendingNexusOperation, PendingUpdate, PendingWorkflowTask, Priority,
    Reject, ReplayContext, RequestDedupeOp, ResetActivityRequest, ResetRequest, RetryContinuation,
    RetryState, SignalRequest, StartRequest, StartWorkflowTaskRequest, TerminateRequest,
    TimerDueRequest, TimerOp, TimerState, Transition, UnpauseActivityRequest,
    UnpauseWorkflowRequest, UpdateActivityOptionsRequest, UpdateExecutionOptionsRequest,
    UpdateProtocolBody, UpdateRequest, UserMetadata, VersioningBehavior, VersioningOverride,
    WorkerDeploymentVersionRef, WorkflowCommand, WorkflowExecutionTimedOutRequest, WorkflowState,
    WorkflowTaskCompletedRequest, WorkflowTaskFailedCause, WorkflowTaskFailedRequest,
    WorkflowTaskTimedOutRequest, WorkflowTaskTimeoutType, WorkflowTimeoutType,
    WorkflowVersioningInfo,
    event::{HistoryEvent, HistoryEventKind},
    kernel::Kernel,
};
use tokeira_types::{
    BuildId, DeploymentId, ExecutionStatus, LogicalTaskSeq, Memo, NamespaceId, Payload, Payloads,
    RequestContext, RequestId, RetryPolicy, RunId, RunKey, SearchAttrValue, SearchAttributes,
    ShardEpoch, StickyAffinity, TaskQueueName, TransitionSeq, WorkerIdentity, WorkflowId,
    WorkflowTaskToken, WorkflowType,
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

fn stamp_callbacks(
    callbacks: impl IntoIterator<Item = CompletionCallback>,
    now: OffsetDateTime,
) -> Vec<CompletionCallback> {
    callbacks
        .into_iter()
        .map(|mut callback| {
            if callback.registration_time.is_none() {
                callback.registration_time = Some(now);
            }
            callback
        })
        .collect()
}

fn stamp_callback_field_change(
    change: &FieldChange<Vec<CompletionCallback>>,
    now: OffsetDateTime,
) -> FieldChange<Vec<CompletionCallback>> {
    match change {
        FieldChange::Unchanged => FieldChange::Unchanged,
        FieldChange::Clear => FieldChange::Clear,
        FieldChange::Set(callbacks) => FieldChange::Set(stamp_callbacks(callbacks.clone(), now)),
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
        deployment: None,
        build_id: None,
        versioning_info: None,
        worker_deployment_name: None,
        status: ExecutionStatus::Running,
        transition_seq: TransitionSeq(7),
        last_event_id: 14,
        next_workflow_task_seq: LogicalTaskSeq(4),
        pending_workflow_task: None,
        previous_started_event_id: 0,
        workflow_task_attempt: 1,
        sticky: None,
        pause_info: None,
        cancel_requested: false,
        wft_stamp: 0,
        memo: memo_with("memo"),
        search_attributes: search_attrs_with("search"),
        workflow_execution_timeout: Some(Duration::minutes(5)),
        workflow_run_timeout: Some(Duration::minutes(1)),
        workflow_task_timeout: default_workflow_task_timeout(),
        retry_policy: Some(sample_retry_policy()),
        attempt: 2,
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
        started_at: now - Duration::minutes(10),
        first_run_started_at: Some(now - Duration::minutes(10)),
        closed_at: None,
        close_result: None,
        close_failure: None,
        request_id_infos: std::collections::BTreeMap::new(),
        buffered_events: Vec::new(),
    }
}

fn with_pending_wft(
    mut state: WorkflowState,
    logical_seq: u64,
    started_event_id: Option<i64>,
    attempt: u32,
) -> WorkflowState {
    state.pending_workflow_task = Some(PendingWorkflowTask {
        task_type: tokeira_kernel::WorkflowTaskType::Normal,
        schedule_to_start_deadline: None,
        logical_seq: LogicalTaskSeq(logical_seq),
        scheduled_event_id: state.last_event_id - 1,
        scheduled_at: state.started_at,
        started_event_id,
        started_at: started_event_id.map(|_| state.started_at + Duration::seconds(1)),
        attempt,
    });
    state.next_workflow_task_seq = LogicalTaskSeq(logical_seq).next();
    state
}

fn with_sticky(
    mut state: WorkflowState,
    worker_identity: &str,
    now: OffsetDateTime,
) -> WorkflowState {
    state.sticky = Some(StickyAffinity {
        sticky_queue: tokeira_types::TaskQueueName(String::new()),
        schedule_to_start_timeout: time::Duration::ZERO,
        worker_identity: WorkerIdentity(worker_identity.into()),
        expires_at: now + Duration::seconds(30),
    });
    state
}

fn with_paused_status(
    mut state: WorkflowState,
    now: OffsetDateTime,
    request_id: &str,
) -> WorkflowState {
    state.status = ExecutionStatus::Paused;
    state.pause_info = Some(PauseInfo {
        pause_time: now,
        identity: "operator".into(),
        reason: "paused".into(),
        request_id: request_id.into(),
    });
    state.wft_stamp = 1;
    state
}

fn with_activity(mut state: WorkflowState, activity_id: &str) -> WorkflowState {
    state.activities.insert(
        activity_id.into(),
        ActivityState {
            cancel_requested: false,
            started_identity: None,
            retry_last_worker_identity: None,
            activity_id: activity_id.into(),
            activity_type: "activity-type".into(),
            schedule_event_id: state.last_event_id - 2,
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
) -> WorkflowState {
    state.pending_external_signals.insert(
        initiated_event_id,
        PendingExternalSignal {
            initiated_event_id,
            target_namespace_id: state.namespace_id,
            target_namespace: None,
            target_workflow_id: WorkflowId("target-signal".into()),
            target_run_id: Some(RunId::new()),
            signal_name: "sig".into(),
        },
    );
    state
}

fn with_pending_external_cancel(
    mut state: WorkflowState,
    initiated_event_id: i64,
) -> WorkflowState {
    state.pending_external_cancels.insert(
        initiated_event_id,
        PendingExternalCancel {
            initiated_event_id,
            target_namespace_id: state.namespace_id,
            target_namespace: None,
            target_workflow_id: WorkflowId("target-cancel".into()),
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

fn with_execution_options(mut state: WorkflowState, callbacks: usize) -> WorkflowState {
    state.set_versioning_override(Some(VersioningOverride::AutoUpgrade));
    state.completion_callbacks = vec![completion_callback(); callbacks];
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

#[test]
fn workflow_state_deserialize_defaults_describe_metadata() {
    let mut value = serde_json::to_value(make_open_state(fixed_now())).unwrap();
    let object = value.as_object_mut().unwrap();
    object.remove("cancel_requested");
    object.remove("root_workflow_id");
    object.remove("root_run_id");

    let restored: WorkflowState = serde_json::from_value(value).unwrap();
    assert!(!restored.cancel_requested);
    assert_eq!(restored.root_workflow_id, None);
    assert_eq!(restored.root_run_id, None);
}

fn arb_open_state_for_reset(now: OffsetDateTime) -> impl Strategy<Value = WorkflowState> {
    (
        any::<bool>(),
        any::<bool>(),
        any::<bool>(),
        any::<bool>(),
        any::<bool>(),
        any::<bool>(),
        any::<bool>(),
        any::<bool>(),
        any::<bool>(),
    )
        .prop_map(
            move |(
                started_wft,
                sticky,
                activity,
                timer,
                child,
                ext_signal,
                ext_cancel,
                update,
                nexus,
            )| {
                let mut state = make_open_state(now);
                if started_wft {
                    state = with_pending_wft(state, 90, Some(30), 1);
                }
                if sticky {
                    state = with_sticky(state, "sticky-worker", now);
                }
                if activity {
                    state = with_activity(state, "activity-1");
                }
                if timer {
                    state = with_timer(state, "timer-1", now);
                }
                if child {
                    state = with_child(state, "child-1", 22, ParentClosePolicy::Terminate, true);
                }
                if ext_signal {
                    state = with_pending_external_signal(state, 31);
                }
                if ext_cancel {
                    state = with_pending_external_cancel(state, 32);
                }
                if update {
                    state = with_pending_update(state, "update-1");
                }
                if nexus {
                    state = with_pending_nexus_operation(state, "op-1");
                }
                state
            },
        )
}

fn arb_reset_request(
    state: WorkflowState,
    now: OffsetDateTime,
) -> impl Strategy<Value = ResetRequest> {
    (
        1i64..=state.last_event_id,
        arb_small_string(),
        arb_small_string(),
    )
        .prop_map(move |(fork_event_id, reason, request_id)| ResetRequest {
            fork_event_id,
            new_run_id: RunId::new(),
            reason,
            request: request_context(&request_id, now),
            now,
        })
}

fn arb_pause_workflow_request(now: OffsetDateTime) -> impl Strategy<Value = PauseWorkflowRequest> {
    (arb_small_string(), arb_small_string(), arb_small_string()).prop_map(
        move |(identity, reason, request_id)| PauseWorkflowRequest {
            identity,
            reason,
            request: request_context(&request_id, now),
            now,
        },
    )
}

fn arb_unpause_workflow_request(
    now: OffsetDateTime,
) -> impl Strategy<Value = UnpauseWorkflowRequest> {
    (arb_small_string(), arb_small_string(), arb_small_string()).prop_map(
        move |(identity, reason, request_id)| UnpauseWorkflowRequest {
            identity,
            reason,
            request: request_context(&request_id, now),
            now,
        },
    )
}

fn arb_update_activity_options_request(
    activity_id: String,
    now: OffsetDateTime,
) -> impl Strategy<Value = UpdateActivityOptionsRequest> {
    (
        arb_field_change(arb_small_string().prop_map(TaskQueueName)),
        arb_field_change(prop::option::of(arb_duration())),
        arb_field_change(prop::option::of(arb_duration())),
        arb_field_change(prop::option::of(arb_duration())),
        arb_field_change(prop::option::of(arb_duration())),
        arb_small_string(),
    )
        .prop_map(move |(task_queue, s2c, s2s, stc, hb, request_id)| {
            UpdateActivityOptionsRequest {
                activity_id: activity_id.clone(),
                task_queue,
                schedule_to_close_timeout: s2c,
                schedule_to_start_timeout: s2s,
                start_to_close_timeout: stc,
                heartbeat_timeout: hb,
                request: request_context(&request_id, now),
                now,
            }
        })
}

fn arb_pause_activity_request(
    activity_id: String,
    now: OffsetDateTime,
) -> impl Strategy<Value = PauseActivityRequest> {
    (arb_small_string(), arb_small_string(), arb_small_string()).prop_map(
        move |(identity, reason, request_id)| PauseActivityRequest {
            activity_id: activity_id.clone(),
            identity,
            reason,
            request: request_context(&request_id, now),
            now,
        },
    )
}

fn arb_unpause_activity_request(
    activity_id: String,
    now: OffsetDateTime,
) -> impl Strategy<Value = UnpauseActivityRequest> {
    arb_small_string().prop_map(move |request_id| UnpauseActivityRequest {
        activity_id: activity_id.clone(),
        request: request_context(&request_id, now),
        now,
    })
}

fn arb_reset_activity_request(
    activity_id: String,
    now: OffsetDateTime,
) -> impl Strategy<Value = ResetActivityRequest> {
    (any::<bool>(), arb_small_string()).prop_map(move |(reset_heartbeat, request_id)| {
        ResetActivityRequest {
            activity_id: activity_id.clone(),
            reset_heartbeat,
            request: request_context(&request_id, now),
            now,
        }
    })
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

fn arb_failure_payload() -> impl Strategy<Value = Payload> {
    "[a-z ]{0,20}".prop_map(|msg| {
        let failure = tokeira_proto::public::temporal::api::failure::v1::Failure {
            message: msg,
            ..Default::default()
        };
        let mut data = Vec::new();
        failure.encode(&mut data).unwrap();
        let mut metadata = BTreeMap::new();
        metadata.insert("encoding".to_string(), "temporal/failure+proto".to_string());
        Payload {
            data,
            metadata,
            external_payloads: Vec::new(),
        }
    })
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

fn arb_record_marker_command() -> impl Strategy<Value = WorkflowCommand> {
    (
        arb_small_string(),
        prop::collection::btree_map(arb_small_string(), arb_payloads(), 0..3),
        prop::option::of(arb_payload()),
        prop::option::of(prop::collection::btree_map(
            arb_small_string(),
            arb_payload(),
            0..3,
        )),
    )
        .prop_map(
            |(marker_name, details, failure, header)| WorkflowCommand::RecordMarker {
                marker_name,
                details,
                failure,
                header,
            },
        )
}

fn arb_schedule_nexus_operation_command() -> impl Strategy<Value = WorkflowCommand> {
    (
        arb_small_string(),
        arb_small_string(),
        arb_small_string(),
        arb_small_string(),
        arb_payloads(),
        prop::option::of(arb_duration()),
    )
        .prop_map(
            |(operation_id, endpoint, service, operation, input, schedule_to_close_timeout)| {
                WorkflowCommand::ScheduleNexusOperation {
                    operation_id,
                    endpoint,
                    service,
                    operation,
                    input,
                    schedule_to_close_timeout,
                    schedule_to_start_timeout: None,
                    start_to_close_timeout: None,
                }
            },
        )
}

fn arb_field_change<T: Strategy>(strategy: T) -> impl Strategy<Value = FieldChange<T::Value>>
where
    T::Value: Clone + std::fmt::Debug,
{
    prop_oneof![
        Just(FieldChange::Unchanged),
        strategy.prop_map(FieldChange::Set),
        Just(FieldChange::Clear),
    ]
}

fn arb_update_execution_options_request(
    now: OffsetDateTime,
) -> impl Strategy<Value = UpdateExecutionOptionsRequest> {
    (
        arb_field_change(Just(VersioningOverride::AutoUpgrade)),
        arb_field_change(prop::collection::vec(Just(completion_callback()), 0..3)),
        prop::option::of(arb_small_string()),
        arb_small_string(),
    )
        .prop_map(
            move |(versioning_override, completion_callbacks, attached_request_id, request_id)| {
                UpdateExecutionOptionsRequest {
                    versioning_override,
                    completion_callbacks,
                    attached_completion_callbacks: Vec::new(),
                    attached_links: Vec::new(),
                    attached_request_id,
                    request: request_context(&request_id, now),
                    now,
                }
            },
        )
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
            |(
                initial_interval,
                backoff_coefficient,
                maximum_interval,
                maximum_attempts,
                non_retryable_error_types,
            )| RetryPolicy {
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
        prop::option::of(any::<i64>().prop_map(|secs| {
            OffsetDateTime::from_unix_timestamp(secs.clamp(-2_000_000_000, 4_000_000_000)).unwrap()
        })),
    )
        .prop_map(
            |(
                input,
                memo,
                search_attributes,
                workflow_execution_timeout,
                workflow_run_timeout,
                workflow_task_timeout,
                retry_policy,
                attempt,
                first_run_started_at,
            )| {
                let now = fixed_now();
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
                    input,
                    header: None,
                    memo,
                    search_attributes,
                    workflow_execution_timeout,
                    workflow_run_timeout,
                    workflow_task_timeout,
                    retry_policy,
                    conflict_policy: tokeira_kernel::WorkflowIdConflictPolicy::Fail,
                    reuse_policy: tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
                    attempt,
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
                    first_run_started_at,
                    request: request_context("prop-start", now),
                    now,
                    client_cron_schedule: None,
                    cron_schedule: None,
                    reserved_poller_identity: None,
                }
            },
        )
}

fn arb_worker_deployment_version_ref() -> impl Strategy<Value = WorkerDeploymentVersionRef> {
    ("[a-z][a-z0-9-]{0,16}", "[a-z][a-z0-9-]{0,16}").prop_map(|(deployment_name, build_id)| {
        WorkerDeploymentVersionRef {
            deployment_name,
            build_id,
        }
    })
}

fn arb_versioning_behavior() -> impl Strategy<Value = VersioningBehavior> {
    prop_oneof![
        Just(VersioningBehavior::Unspecified),
        Just(VersioningBehavior::Pinned),
        Just(VersioningBehavior::AutoUpgrade),
    ]
}

fn arb_continue_as_new_versioning_behavior()
-> impl Strategy<Value = ContinueAsNewVersioningBehavior> {
    prop_oneof![
        Just(ContinueAsNewVersioningBehavior::Unspecified),
        Just(ContinueAsNewVersioningBehavior::AutoUpgrade),
        Just(ContinueAsNewVersioningBehavior::UseRampingVersion),
    ]
}

fn arb_versioning_override() -> impl Strategy<Value = VersioningOverride> {
    prop_oneof![
        arb_worker_deployment_version_ref()
            .prop_map(|version| VersioningOverride::Pinned { version }),
        Just(VersioningOverride::AutoUpgrade),
    ]
}

fn arb_user_metadata() -> impl Strategy<Value = Option<UserMetadata>> {
    prop::option::of(
        (
            prop::option::of(arb_payload()),
            prop::option::of(arb_payload()),
        )
            .prop_map(|(summary, details)| UserMetadata { summary, details }),
    )
}

fn arb_links() -> impl Strategy<Value = Vec<Link>> {
    prop::collection::vec(
        arb_small_string().prop_map(|job_id| Link::BatchJob { job_id }),
        0..3,
    )
}

fn arb_priority() -> impl Strategy<Value = Option<Priority>> {
    prop::option::of((0i32..100, arb_small_string(), 0.1f32..10.0).prop_map(
        |(priority_key, fairness_key, fairness_weight)| Priority {
            priority_key,
            fairness_key,
            fairness_weight,
        },
    ))
}

fn arb_workflow_versioning_info() -> impl Strategy<Value = WorkflowVersioningInfo> {
    (
        arb_versioning_behavior(),
        prop::option::of(arb_worker_deployment_version_ref()),
        prop::option::of(arb_versioning_override()),
        prop::option::of(arb_worker_deployment_version_ref()),
        -1000i64..1000i64,
        arb_continue_as_new_versioning_behavior(),
    )
        .prop_map(
            |(
                behavior,
                deployment_version,
                versioning_override,
                version_transition,
                revision_number,
                continue_as_new_initial_versioning_behavior,
            )| WorkflowVersioningInfo {
                behavior,
                deployment_version,
                versioning_override,
                version_transition,
                revision_number,
                continue_as_new_initial_versioning_behavior,
            },
        )
}

fn arb_wft_versioning_completion() -> impl Strategy<
    Value = (
        VersioningBehavior,
        Option<WorkerDeploymentVersionRef>,
        Option<String>,
    ),
> {
    (
        arb_versioning_behavior(),
        prop::option::of(arb_worker_deployment_version_ref()),
        prop::option::of("[a-z][a-z0-9-]{0,16}"),
    )
}

fn arb_activity_resolution() -> impl Strategy<Value = ActivityResolution> {
    prop_oneof![
        arb_payloads().prop_map(|result| ActivityResolution::Completed { result }),
        (arb_failure_payload(), arb_retry_state()).prop_map(|(failure, retry_state)| {
            ActivityResolution::Failed {
                failure,
                retry_state,
            }
        }),
        (
            arb_small_string(),
            arb_retry_state(),
            prop::option::of(arb_failure_payload()),
        )
            .prop_map(|(timeout_type, retry_state, failure)| {
                ActivityResolution::TimedOut {
                    timeout_type,
                    retry_state,
                    failure,
                }
            }),
        prop::option::of(arb_payloads())
            .prop_map(|details| ActivityResolution::Canceled { details }),
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
            |(
                activity_id,
                task_queue,
                input,
                schedule_to_close_timeout,
                schedule_to_start_timeout,
                start_to_close_timeout,
                heartbeat_timeout,
            )| WorkflowCommand::ScheduleActivity {
                activity_id,
                activity_type: "activity-type".into(),
                task_queue: TaskQueueName(task_queue),
                input,
                header: None,
                request_eager_execution: false,
                retry_policy: None,
                deployment: None,
                build_id: None,
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
        Just(WorkflowTaskFailedCause::ResetWorkflow),
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
    )
        .prop_map(move |(failure_cause, failure_details, worker_identity)| {
            WorkflowTaskFailedRequest {
                logical_seq,
                started_event_id,
                failure_cause,
                failure_details,
                worker_identity: WorkerIdentity(worker_identity),
                now,
            }
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

fn arb_external_workflow_execution() -> impl Strategy<Value = ExternalWorkflowExecution> {
    arb_small_string().prop_map(|workflow_id| ExternalWorkflowExecution {
        namespace_id: NamespaceId::new(),
        workflow_id: WorkflowId(workflow_id),
        run_id: RunId::new(),
    })
}

fn arb_cancel_request(now: OffsetDateTime) -> impl Strategy<Value = CancelRequest> {
    (
        arb_small_string(),
        prop::option::of(arb_external_workflow_execution()),
        arb_small_string(),
    )
        .prop_map(
            move |(reason, external_initiator, request_id)| CancelRequest {
                reason,
                external_initiator,
                external_initiated_event_id: 0,
                request: request_context(&request_id, now),
                now,
            },
        )
}

fn arb_terminate_request(now: OffsetDateTime) -> impl Strategy<Value = TerminateRequest> {
    (
        arb_small_string(),
        prop::option::of(arb_payloads()),
        arb_small_string(),
        arb_small_string(),
    )
        .prop_map(
            move |(reason, details, identity, request_id)| TerminateRequest {
                reason,
                details,
                identity,
                request: request_context(&request_id, now),
                now,
            },
        )
}

fn arb_workflow_timeout_type() -> impl Strategy<Value = WorkflowTimeoutType> {
    prop_oneof![
        Just(WorkflowTimeoutType::ExecutionTimeout),
        Just(WorkflowTimeoutType::RunTimeout),
    ]
}

fn arb_retry_state() -> impl Strategy<Value = RetryState> {
    prop_oneof![
        Just(RetryState::InProgress),
        Just(RetryState::NonRetryableFailure),
        Just(RetryState::Timeout),
        Just(RetryState::MaximumAttemptsReached),
        Just(RetryState::RetryPolicyNotSet),
        Just(RetryState::InternalServerError),
        Just(RetryState::CancelRequested),
    ]
}

fn arb_continue_as_new_command() -> impl Strategy<Value = WorkflowCommand> {
    (
        arb_small_string(),
        arb_small_string(),
        arb_payloads(),
        arb_memo(),
        arb_search_attributes(),
        prop::option::of(arb_duration()),
        prop::option::of(arb_duration()),
        arb_duration(),
    )
        .prop_map(
            |(
                workflow_type,
                task_queue,
                input,
                memo,
                search_attributes,
                workflow_execution_timeout,
                workflow_run_timeout,
                workflow_task_timeout,
            )| WorkflowCommand::ContinueAsNew {
                header: None,
                new_run_id: RunId::new(),
                workflow_type: WorkflowType(workflow_type),
                task_queue: TaskQueueName(task_queue),
                input,
                memo,
                search_attributes,
                workflow_execution_timeout,
                workflow_run_timeout,
                workflow_task_timeout,
                retry_policy: None,
            },
        )
}

fn arb_parent_close_policy() -> impl Strategy<Value = ParentClosePolicy> {
    prop_oneof![
        Just(ParentClosePolicy::Terminate),
        Just(ParentClosePolicy::RequestCancel),
        Just(ParentClosePolicy::Abandon),
    ]
}

fn arb_child_start_result() -> impl Strategy<Value = ChildStartResult> {
    prop_oneof![
        arb_small_string().prop_map(|workflow_type| ChildStartResult::Started {
            child_run_id: RunId::new(),
            workflow_type: WorkflowType(workflow_type),
        }),
        arb_small_string().prop_map(|cause| ChildStartResult::Failed { cause }),
    ]
}

fn arb_child_resolution() -> impl Strategy<Value = ChildResolution> {
    prop_oneof![
        arb_payloads().prop_map(|result| ChildResolution::Completed { result }),
        arb_failure_payload().prop_map(|failure| ChildResolution::Failed { failure }),
        Just(ChildResolution::Canceled),
        Just(ChildResolution::Terminated),
        Just(ChildResolution::TimedOut),
    ]
}

fn arb_external_signal_result() -> impl Strategy<Value = ExternalSignalResult> {
    prop_oneof![
        Just(ExternalSignalResult::Signaled),
        arb_small_string().prop_map(|cause| ExternalSignalResult::Failed { cause }),
    ]
}

fn arb_external_cancel_result() -> impl Strategy<Value = ExternalCancelResult> {
    prop_oneof![
        Just(ExternalCancelResult::CancelRequested),
        arb_small_string().prop_map(|cause| ExternalCancelResult::Failed { cause }),
    ]
}

fn arb_update_request(now: OffsetDateTime) -> impl Strategy<Value = UpdateRequest> {
    (
        arb_small_string(),
        arb_small_string(),
        arb_payloads(),
        arb_small_string(),
    )
        .prop_map(
            move |(update_id, update_name, input, request_id)| UpdateRequest {
                update_id,
                update_name,
                input,
                request: request_context(&request_id, now),
                now,
            },
        )
}

fn arb_update_completed_command() -> impl Strategy<Value = WorkflowCommand> {
    (arb_small_string(), arb_payloads())
        .prop_map(|(update_id, result)| WorkflowCommand::UpdateCompleted { update_id, result })
}

fn arb_update_rejected_command() -> impl Strategy<Value = WorkflowCommand> {
    (arb_small_string(), arb_failure_payload())
        .prop_map(|(update_id, failure)| WorkflowCommand::UpdateRejected { update_id, failure })
}

fn arb_workflow_execution_timed_out_request(
    now: OffsetDateTime,
) -> impl Strategy<Value = WorkflowExecutionTimedOutRequest> {
    (arb_workflow_timeout_type(), arb_retry_state()).prop_map(move |(timeout_type, retry_state)| {
        WorkflowExecutionTimedOutRequest {
            timeout_type,
            retry_state,
            new_execution_run_id: None,
            now,
        }
    })
}

fn arb_nexus_resolution() -> impl Strategy<Value = NexusResolution> {
    prop_oneof![
        Just(NexusResolution::Started {
            operation_token: String::new(),
            links: Vec::new()
        }),
        arb_payloads().prop_map(|result| NexusResolution::Completed {
            result,
            links: Vec::new(),
        }),
        arb_failure_payload().prop_map(|failure| NexusResolution::Failed { failure }),
        Just(NexusResolution::Canceled),
        Just(NexusResolution::TimedOut {
            timeout_type: NexusTimeoutType::ScheduleToClose,
        }),
    ]
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
                header: None,
                links: Vec::new(),
                request: request_context("prop-signal", now),
                now,
            };
            (LoadedRun::Existing(state), Command::Signal(req))
        }),
        arb_cancel_request(now).prop_map(move |req| {
            let state = make_open_state(now);
            (LoadedRun::Existing(state), Command::Cancel(req))
        }),
        arb_terminate_request(now).prop_map(move |req| {
            let state = make_open_state(now);
            (LoadedRun::Existing(state), Command::Terminate(req))
        }),
        arb_pause_workflow_request(now).prop_map(move |req| {
            let state = with_activity(make_open_state(now), "activity-1");
            (LoadedRun::Existing(state), Command::PauseWorkflow(req))
        }),
        arb_unpause_workflow_request(now).prop_map(move |req| {
            let state = with_activity(
                with_paused_status(make_open_state(now), now, "pause-req"),
                "activity-1",
            );
            (LoadedRun::Existing(state), Command::UnpauseWorkflow(req))
        }),
        arb_open_state_for_reset(now).prop_flat_map(move |state| {
            arb_reset_request(state.clone(), now)
                .prop_map(move |req| (LoadedRun::Existing(state.clone()), Command::Reset(req)))
        }),
        arb_update_activity_options_request("activity-1".into(), now).prop_map(move |req| {
            let state = with_activity(make_open_state(now), "activity-1");
            (
                LoadedRun::Existing(state),
                Command::UpdateActivityOptions(req),
            )
        }),
        arb_pause_activity_request("activity-1".into(), now).prop_map(move |req| {
            let state = with_activity(make_open_state(now), "activity-1");
            (LoadedRun::Existing(state), Command::PauseActivity(req))
        }),
        arb_unpause_activity_request("activity-1".into(), now).prop_map(move |req| {
            let mut state = with_activity(make_open_state(now), "activity-1");
            if let Some(activity) = state.activities.get_mut("activity-1") {
                activity.pause_info = Some(ActivityPauseInfo {
                    pause_time: now,
                    identity: "operator".into(),
                    reason: "pause".into(),
                });
                activity.stamp = 1;
            }
            (LoadedRun::Existing(state), Command::UnpauseActivity(req))
        }),
        arb_reset_activity_request("activity-1".into(), now).prop_map(move |req| {
            let state = with_activity(make_open_state(now), "activity-1");
            (LoadedRun::Existing(state), Command::ResetActivity(req))
        }),
        arb_update_execution_options_request(now).prop_map(move |req| {
            let state = make_open_state(now);
            (
                LoadedRun::Existing(state),
                Command::UpdateExecutionOptions(req),
            )
        }),
        arb_workflow_execution_timed_out_request(now).prop_map(move |req| {
            let state = with_sticky(
                with_timer(
                    with_activity(make_open_state(now), "activity-1"),
                    "timer-1",
                    now,
                ),
                "sticky-worker",
                now,
            );
            (
                LoadedRun::Existing(state),
                Command::WorkflowExecutionTimedOut(req),
            )
        }),
        (0u64..10u64).prop_map(move |offset| {
            let logical_seq = 20 + offset;
            let state = with_pending_wft(make_open_state(now), logical_seq, None, 0);
            let req = StartWorkflowTaskRequest {
                logical_seq: LogicalTaskSeq(logical_seq),
                worker_identity: WorkerIdentity("worker".into()),
                request_id: format!("start-wft-{logical_seq}"),
                history_size_bytes: 0,
                suggest_continue_as_new: false,
                deployment_transition: None,
                deployment_transition_revision_number: None,
                sticky_ttl: Some(Duration::seconds(30)),
                now,
            };
            (
                LoadedRun::Existing(state),
                Command::WorkflowTaskStarted(req),
            )
        }),
        prop_oneof![
            Just(vec![WorkflowCommand::RequestNewWorkflowTask]),
            arb_schedule_activity_command().prop_map(|cmd| vec![cmd]),
            arb_record_marker_command().prop_map(|cmd| vec![cmd]),
            arb_schedule_nexus_operation_command().prop_map(|cmd| vec![cmd]),
            arb_payloads().prop_map(|result| vec![WorkflowCommand::CompleteWorkflow { result }]),
            arb_failure_payload()
                .prop_map(|failure| vec![WorkflowCommand::FailWorkflow { failure }]),
            arb_continue_as_new_command().prop_map(|cmd| vec![cmd]),
            (
                arb_small_string(),
                arb_small_string(),
                arb_small_string(),
                arb_payloads(),
                arb_parent_close_policy()
            )
                .prop_map(
                    |(child_workflow_id, workflow_type, task_queue, input, parent_close_policy)| {
                        vec![WorkflowCommand::StartChildWorkflow {
                            child_workflow_id: WorkflowId(child_workflow_id),
                            namespace_id: NamespaceId::new(),
                            namespace: None,
                            workflow_type: WorkflowType(workflow_type),
                            task_queue: TaskQueueName(task_queue),
                            input,
                            header: None,
                            memo: Memo::default(),
                            search_attributes: SearchAttributes::default(),
                            workflow_execution_timeout: None,
                            workflow_run_timeout: None,
                            workflow_task_timeout: Duration::seconds(10),
                            retry_policy: None,
                            cron_schedule: None,
                            parent_close_policy,
                            reuse_policy: tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
                        }]
                    }
                ),
            (
                arb_small_string(),
                any::<bool>(),
                arb_small_string(),
                arb_payloads()
            )
                .prop_map(|(target_workflow_id, with_run_id, signal_name, input)| {
                    vec![WorkflowCommand::SignalExternalWorkflowExecution {
                        target_namespace_id: NamespaceId::new(),
                        target_namespace: None,
                        target_workflow_id: WorkflowId(target_workflow_id),
                        target_run_id: with_run_id.then(RunId::new),
                        signal_name,
                        input,
                        header: None,
                        control: "ctl".into(),
                    }]
                }),
            (arb_small_string(), any::<bool>()).prop_map(|(target_workflow_id, with_run_id)| {
                vec![WorkflowCommand::RequestCancelExternalWorkflowExecution {
                    target_namespace_id: NamespaceId::new(),
                    target_namespace: None,
                    target_workflow_id: WorkflowId(target_workflow_id),
                    target_run_id: with_run_id.then(RunId::new),
                    control: "ctl".into(),
                }]
            }),
        ]
        .prop_map(move |commands| {
            let state = with_pending_wft(make_open_state(now), 30, Some(13), 1);
            let req = WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(30),
                    started_event_id: 13,
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
                commands,
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now,
            };
            (
                LoadedRun::Existing(state),
                Command::WorkflowTaskCompleted(req),
            )
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
            let mut state = with_pending_wft(
                make_open_state(now),
                logical_seq.0,
                Some(started_event_id),
                1,
            );
            if sticky {
                state = with_sticky(state, "sticky-worker", now);
            }
            arb_wft_failed_request(logical_seq, started_event_id, now).prop_map(move |req| {
                (
                    LoadedRun::Existing(state.clone()),
                    Command::WorkflowTaskFailed(req),
                )
            })
        }),
        prop::bool::ANY.prop_flat_map(move |sticky| {
            let logical_seq = LogicalTaskSeq(41);
            let started_event_id = 16;
            let mut state = with_pending_wft(
                make_open_state(now),
                logical_seq.0,
                Some(started_event_id),
                1,
            );
            if sticky {
                state = with_sticky(state, "sticky-worker", now);
            }
            arb_wft_timed_out_request(logical_seq, started_event_id, now).prop_map(move |req| {
                (
                    LoadedRun::Existing(state.clone()),
                    Command::WorkflowTaskTimedOut(req),
                )
            })
        }),
        Just(()).prop_map(move |_| {
            let state = with_pending_wft(make_open_state(now), 42, Some(17), 1);
            let req = WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(42),
                    started_event_id: 17,
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
                now,
            };
            (
                LoadedRun::Existing(state),
                Command::WorkflowTaskCompleted(req),
            )
        }),
        Just(()).prop_map(move |_| {
            let state = with_activity(
                with_pending_wft(make_open_state(now), 43, Some(18), 1),
                "activity-1",
            );
            let cancel_scheduled_event_id = state.activities["activity-1"].schedule_event_id;
            let req = WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(43),
                    started_event_id: 18,
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
                    scheduled_event_id: cancel_scheduled_event_id,
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now,
            };
            (
                LoadedRun::Existing(state),
                Command::WorkflowTaskCompleted(req),
            )
        }),
        Just(()).prop_map(move |_| {
            let state = with_timer(
                with_pending_wft(make_open_state(now), 44, Some(19), 1),
                "timer-1",
                now,
            );
            let req = WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(44),
                    started_event_id: 19,
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
                now,
            };
            (
                LoadedRun::Existing(state),
                Command::WorkflowTaskCompleted(req),
            )
        }),
        arb_small_string().prop_map(move |operation_id| {
            let state = with_pending_nexus_operation(
                with_pending_wft(make_open_state(now), 45, Some(20), 1),
                &operation_id,
            );
            let req = WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(45),
                    started_event_id: 20,
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
                commands: vec![WorkflowCommand::CancelNexusOperation {
                    scheduled_event_id: 12,
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now,
            };
            (
                LoadedRun::Existing(state),
                Command::WorkflowTaskCompleted(req),
            )
        }),
        (arb_small_string(), arb_child_start_result()).prop_map(
            move |(child_workflow_id, result)| {
                let state = with_child(
                    make_open_state(now),
                    &child_workflow_id,
                    21,
                    ParentClosePolicy::Terminate,
                    false,
                );
                let req = ChildStartConfirmedRequest {
                    child_workflow_id: WorkflowId(child_workflow_id),
                    initiated_event_id: 21,
                    result,
                    now,
                };
                (
                    LoadedRun::Existing(state),
                    Command::ChildStartConfirmed(req),
                )
            }
        ),
        (arb_small_string(), arb_child_resolution()).prop_map(
            move |(child_workflow_id, resolution)| {
                let state = with_child(
                    make_open_state(now),
                    &child_workflow_id,
                    21,
                    ParentClosePolicy::Terminate,
                    true,
                );
                let req = ChildResolvedRequest {
                    resolved_run_id: None,
                    child_workflow_id: WorkflowId(child_workflow_id),
                    resolution,
                    now,
                };
                (LoadedRun::Existing(state), Command::ChildResolved(req))
            }
        ),
        arb_external_signal_result().prop_map(move |result| {
            let state = with_pending_external_signal(make_open_state(now), 55);
            let req = ExternalSignalResolvedRequest {
                initiated_event_id: 55,
                result,
                now,
            };
            (
                LoadedRun::Existing(state),
                Command::ExternalSignalResolved(req),
            )
        }),
        arb_external_cancel_result().prop_map(move |result| {
            let state = with_pending_external_cancel(make_open_state(now), 56);
            let req = ExternalCancelResolvedRequest {
                initiated_event_id: 56,
                result,
                now,
            };
            (
                LoadedRun::Existing(state),
                Command::ExternalCancelResolved(req),
            )
        }),
        (arb_small_string(), arb_nexus_resolution()).prop_map(move |(operation_id, resolution)| {
            let state = with_pending_nexus_operation(make_open_state(now), &operation_id);
            let req = NexusOperationResolvedRequest {
                operation_id,
                scheduled_event_id: 12,
                resolution,
                now,
            };
            (
                LoadedRun::Existing(state),
                Command::NexusOperationResolved(req),
            )
        }),
        arb_update_request(now).prop_map(move |req| {
            (
                LoadedRun::Existing(make_open_state(now)),
                Command::Update(req),
            )
        }),
        arb_update_request(now).prop_map(move |req| {
            (
                LoadedRun::Existing(with_pending_wft(make_open_state(now), 58, None, 0)),
                Command::Update(req),
            )
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
        prop_assert_eq!(transition.next_state.first_execution_run_id, req.first_execution_run_id);
        prop_assert_eq!(transition.next_state.first_run_started_at, req.first_run_started_at);
        prop_assert_eq!(transition.next_state.status, ExecutionStatus::Running);
        prop_assert_eq!(transition.next_state.last_event_id, transition.history_events.last().unwrap().event_id);
        prop_assert_eq!(transition.next_state.transition_seq, transition.expected_seq.next());
        prop_assert!(transition.next_state.pending_updates.is_empty());
        prop_assert!(transition.next_state.pending_nexus_operations.is_empty());
        prop_assert_eq!(transition.next_state.versioning_override().cloned(), None);
        prop_assert!(transition.next_state.completion_callbacks.is_empty());
        prop_assert_eq!(started.0, req.continued_execution_run_id);
        prop_assert_eq!(started.1, req.first_execution_run_id);
        prop_assert_eq!(started.2, transition.next_state.retry_policy.clone());
        prop_assert_eq!(started.3, transition.next_state.attempt);
        prop_assert_eq!(started.4, transition.next_state.workflow_execution_timeout);
        prop_assert_eq!(started.5, transition.next_state.workflow_run_timeout);
        prop_assert_eq!(started.6, transition.next_state.workflow_task_timeout);
    }

    #[test]
    fn property_durable_start_metadata(
        mut req in arb_start_request(),
        workflow_start_delay in prop::option::of(arb_duration()),
        callbacks in prop::collection::vec(Just(completion_callback()), 0..3),
        user_metadata in arb_user_metadata(),
        links in arb_links(),
        versioning_override in prop::option::of(arb_versioning_override()),
        priority in arb_priority(),
        cron_schedule in prop::option::of(arb_small_string()),
    ) {
        // Feature: api-conformance-start-fields, Property 3: Durable Start Metadata.
        // **Validates: Requirements 3.1, 3.2**
        req.workflow_start_delay = workflow_start_delay;
        req.completion_callbacks = callbacks;
        req.user_metadata = user_metadata;
        req.links = links;
        req.versioning_override = versioning_override;
        req.priority = priority;
        req.cron_schedule = cron_schedule;

        let expected_callbacks = stamp_callbacks(req.completion_callbacks.clone(), req.now);
        let transition = kernel().apply(LoadedRun::Absent, Command::Start(req.clone())).unwrap();

        let (
            event_workflow_start_delay,
            event_completion_callbacks,
            event_user_metadata,
            event_links,
            event_priority,
            event_cron_schedule,
            event_versioning_override,
        ) = match &transition.history_events[0].kind {
            HistoryEventKind::WorkflowExecutionStarted {
                workflow_start_delay,
                completion_callbacks,
                user_metadata,
                links,
                priority,
                cron_schedule,
                versioning_info,
                ..
            } => (
                *workflow_start_delay,
                completion_callbacks.clone(),
                user_metadata.clone(),
                links.clone(),
                priority.clone(),
                cron_schedule.clone(),
                versioning_info
                    .as_ref()
                    .and_then(|info| info.versioning_override.clone()),
            ),
            other => panic!("unexpected first event: {other:?}"),
        };

        prop_assert_eq!(transition.next_state.workflow_start_delay, req.workflow_start_delay);
        prop_assert_eq!(event_workflow_start_delay, req.workflow_start_delay);
        prop_assert_eq!(&transition.next_state.completion_callbacks, &expected_callbacks);
        prop_assert_eq!(&event_completion_callbacks, &transition.next_state.completion_callbacks);
        prop_assert_eq!(&transition.next_state.user_metadata, &req.user_metadata);
        prop_assert_eq!(&event_user_metadata, &req.user_metadata);
        prop_assert_eq!(&transition.next_state.links, &req.links);
        prop_assert_eq!(&event_links, &req.links);
        prop_assert_eq!(&transition.next_state.priority, &req.priority);
        prop_assert_eq!(&event_priority, &req.priority);
        prop_assert_eq!(&event_cron_schedule, &req.cron_schedule);
        let state_versioning_override = transition.next_state.versioning_override().cloned();
        prop_assert_eq!(&state_versioning_override, &req.versioning_override);
        prop_assert_eq!(&event_versioning_override, &req.versioning_override);

        if let Some(delay) = req.workflow_start_delay.filter(|delay| delay > &Duration::ZERO) {
            let timer = transition
                .next_state
                .timers
                .get(tokeira_kernel::WORKFLOW_START_DELAY_TIMER_ID)
                .expect("delayed starts must persist the first-WFT timer");
            prop_assert_eq!(timer.fire_at, req.now + delay);
            prop_assert!(transition
                .timer_ops
                .iter()
                .any(|op| matches!(
                    op,
                    TimerOp::Upsert(timer) if timer.timer_id == tokeira_kernel::WORKFLOW_START_DELAY_TIMER_ID
                )));
        }
    }

    #[test]
    fn reserved_start_combines_schedule_and_started_events(mut req in arb_start_request(), worker in "\\PC{1,64}") {
        req.reserved_poller_identity = Some(WorkerIdentity(worker.clone()));

        let transition = kernel().apply(LoadedRun::Absent, Command::Start(req)).unwrap();
        let event_kinds = transition
            .history_events
            .iter()
            .map(|event| &event.kind)
            .collect::<Vec<_>>();

        prop_assert_eq!(matches!(event_kinds[0], HistoryEventKind::WorkflowExecutionStarted { .. }), true);
        prop_assert_eq!(matches!(event_kinds[1], HistoryEventKind::WorkflowTaskScheduled { .. }), true);
        prop_assert_eq!(
            matches!(
                event_kinds[2],
                HistoryEventKind::WorkflowTaskStarted {
                    identity,
                    ..
                } if identity == &WorkerIdentity(worker)
            ),
            true,
        );
        prop_assert_eq!(transition
            .dispatch_ops
            .iter()
            .any(|op| matches!(op, DispatchOp::EnqueueWorkflowTask { .. })), true);
        let pending = transition.next_state.pending_workflow_task.as_ref().unwrap();
        prop_assert_eq!(pending.scheduled_event_id, 2);
        prop_assert_eq!(pending.started_event_id, Some(3));
        prop_assert_eq!(transition.next_state.previous_started_event_id, 0);
    }

    #[test]
    fn start_without_reserved_poller_never_emits_workflow_task_started(req in arb_start_request()) {
        prop_assume!(req.reserved_poller_identity.is_none());

        let transition = kernel().apply(LoadedRun::Absent, Command::Start(req)).unwrap();

        prop_assert_eq!(transition.history_events.iter().any(|event| {
            matches!(event.kind, HistoryEventKind::WorkflowTaskScheduled { .. })
        }), true);
        prop_assert_eq!(transition.history_events.iter().all(|event| {
            !matches!(event.kind, HistoryEventKind::WorkflowTaskStarted { .. })
        }), true);
        let pending = transition.next_state.pending_workflow_task.as_ref().unwrap();
        prop_assert_eq!(pending.started_event_id, None);
        prop_assert_eq!(transition.next_state.previous_started_event_id, 0);
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
            (HistoryEventKind::ActivityTaskCompleted { activity_id, result, .. }, ActivityResolution::Completed { result: expected }) => {
                prop_assert_eq!(activity_id, "activity-1");
                prop_assert_eq!(result, &expected);
            }
            (HistoryEventKind::ActivityTaskFailed { activity_id, failure, retry_state, .. }, ActivityResolution::Failed { failure: expected, retry_state: expected_retry_state }) => {
                prop_assert_eq!(activity_id, "activity-1");
                prop_assert_eq!(failure, &expected);
                // K1: the event carries the caller-computed state verbatim.
                prop_assert_eq!(retry_state, &expected_retry_state);
            }
            (HistoryEventKind::ActivityTaskTimedOut { activity_id, timeout_type, retry_state, failure, .. }, ActivityResolution::TimedOut { timeout_type: expected, retry_state: expected_retry_state, failure: expected_failure }) => {
                prop_assert_eq!(activity_id, "activity-1");
                prop_assert_eq!(timeout_type, &expected);
                // K1: the event carries the caller-computed state verbatim.
                prop_assert_eq!(retry_state, &expected_retry_state);
                // K2: the caller-built timeout failure carries through verbatim.
                prop_assert_eq!(failure, &expected_failure);
            }
            (HistoryEventKind::ActivityTaskCanceled { activity_id, details, .. }, ActivityResolution::Canceled { details: expected }) => {
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
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(30),
                    started_event_id: 13,
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
                commands: vec![cmd.clone()],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
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
    fn property_wft_completion_preserves_metering_metadata_and_sticky_ttl(
        metering_metadata in prop::collection::vec(any::<u8>(), 0..32),
        sticky_ttl_secs in 1i64..120,
    ) {
        // Feature: api-conformance-wft-completion, Property: completion metadata and sticky routing.
        // Temporal records completion metering metadata on the completed event and applies sticky
        // attrs during the same transition; see event_factory.go:150-180 and api.go:200-345 @ v1.31.0.
        // **Validates: Requirements 1.2, 2.1**
        let now = fixed_now();
        let state = with_pending_wft(make_open_state(now), 30, Some(13), 1);
        let sticky_ttl = Duration::seconds(sticky_ttl_secs);
        let transition = kernel().apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(30),
                    started_event_id: 13,
                    attempt: 1,
                    shard_epoch: ShardEpoch::ZERO,
                },
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: Some(metering_metadata.clone()),
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: Some(tokeira_kernel::StickySpec { queue: tokeira_types::TaskQueueName(String::new()), schedule_to_start_timeout: sticky_ttl }),
                commands: Vec::new(),
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now,
            }),
        ).unwrap();

        let completed = transition.history_events.iter().find_map(|event| match &event.kind {
            HistoryEventKind::WorkflowTaskCompleted {
                metering_metadata, ..
            } => Some(metering_metadata),
            _ => None,
        }).expect("workflow task completed event");
        prop_assert_eq!(completed, &Some(metering_metadata));

        let sticky = transition.next_state.sticky.as_ref().expect("sticky affinity");
        prop_assert_eq!(sticky.worker_identity.0.as_str(), "worker");
        prop_assert_eq!(sticky.expires_at, now + sticky_ttl);
    }

    #[test]
    fn property_3a_versioned_dispatch_queue_propagation(
        workflow_deployment in prop::option::of(arb_small_string()),
        workflow_build_id in prop::option::of(arb_small_string()),
        activity_deployment in prop::option::of(arb_small_string()),
        activity_build_id in prop::option::of(arb_small_string()),
    ) {
        let now = fixed_now();
        let workflow_deployment = workflow_deployment.map(DeploymentId);
        let workflow_build_id = workflow_build_id.map(BuildId);
        let activity_deployment = activity_deployment.map(DeploymentId);
        let activity_build_id = activity_build_id.map(BuildId);

        let mut signal_state = make_open_state(now);
        signal_state.deployment = workflow_deployment.clone();
        signal_state.build_id = workflow_build_id.clone();
        let signal_transition = kernel().apply(
            LoadedRun::Existing(signal_state),
            Command::Signal(SignalRequest {
                signal_name: "sig".into(),
                input: Payloads::default(),
                header: None,
                links: Vec::new(),
                request: request_context("versioned-signal", now),
                now,
            }),
        ).unwrap();
        prop_assert_eq!(
            signal_transition.dispatch_ops.iter().any(|op| matches!(
                op,
                DispatchOp::EnqueueWorkflowTask { queue, .. }
                    if queue.deployment == workflow_deployment && queue.build_id == workflow_build_id
            )),
            true
        );

        let mut activity_state = with_pending_wft(make_open_state(now), 84, Some(33), 1);
        activity_state.deployment = workflow_deployment.clone();
        activity_state.build_id = workflow_build_id.clone();
        let activity_transition = kernel().apply(
            LoadedRun::Existing(activity_state.clone()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: activity_state.run_key,
                    logical_seq: LogicalTaskSeq(84),
                    started_event_id: 33,
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
                commands: vec![WorkflowCommand::ScheduleActivity {
                    activity_id: "activity-1".into(),
                    activity_type: "activity-type".into(),
                    task_queue: TaskQueueName("activity-q".into()),
                    input: Payloads::default(),
                    header: None,
                    request_eager_execution: false,
                    retry_policy: None,
                    deployment: activity_deployment.clone(),
                    build_id: activity_build_id.clone(),
                    schedule_to_close_timeout: None,
                    schedule_to_start_timeout: None,
                    start_to_close_timeout: None,
                    heartbeat_timeout: None,
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now,
            }),
        ).unwrap();
        let expected_deployment = activity_deployment.clone().or_else(|| workflow_deployment.clone());
        let expected_build_id = activity_build_id.clone().or_else(|| workflow_build_id.clone());
        prop_assert_eq!(
            activity_transition.dispatch_ops.iter().any(|op| matches!(
                op,
                DispatchOp::EnqueueActivityTask { queue, .. }
                    if queue.deployment == expected_deployment && queue.build_id == expected_build_id
            )),
            true
        );
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
                header: None,
                links: Vec::new(),
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
        if !transition.next_state.status.is_open() {
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
            Command::Update(req) => {
                prop_assert_eq!(transition.request_dedupe_ops.len(), 1);
                prop_assert_eq!(transition.request_dedupe_ops[0].clone(), RequestDedupeOp { request_id: req.request.request_id });
            }
            Command::Signal(req) => {
                prop_assert_eq!(transition.request_dedupe_ops.len(), 1);
                prop_assert_eq!(transition.request_dedupe_ops[0].clone(), RequestDedupeOp { request_id: req.request.request_id });
            }
            Command::Cancel(req) => {
                prop_assert_eq!(transition.request_dedupe_ops.len(), 1);
                prop_assert_eq!(transition.request_dedupe_ops[0].clone(), RequestDedupeOp { request_id: req.request.request_id });
            }
            Command::Terminate(req) => {
                prop_assert_eq!(transition.request_dedupe_ops.len(), 1);
                prop_assert_eq!(transition.request_dedupe_ops[0].clone(), RequestDedupeOp { request_id: req.request.request_id });
            }
            Command::Reset(req) => {
                prop_assert_eq!(transition.request_dedupe_ops.len(), 1);
                prop_assert_eq!(transition.request_dedupe_ops[0].clone(), RequestDedupeOp { request_id: req.request.request_id });
            }
            Command::PauseWorkflow(req) => {
                prop_assert_eq!(transition.request_dedupe_ops.len(), 1);
                prop_assert_eq!(transition.request_dedupe_ops[0].clone(), RequestDedupeOp { request_id: req.request.request_id });
            }
            Command::UnpauseWorkflow(req) => {
                prop_assert_eq!(transition.request_dedupe_ops.len(), 1);
                prop_assert_eq!(transition.request_dedupe_ops[0].clone(), RequestDedupeOp { request_id: req.request.request_id });
            }
            Command::UpdateActivityOptions(req) => {
                prop_assert_eq!(transition.request_dedupe_ops.len(), 1);
                prop_assert_eq!(transition.request_dedupe_ops[0].clone(), RequestDedupeOp { request_id: req.request.request_id });
            }
            Command::PauseActivity(req) => {
                prop_assert_eq!(transition.request_dedupe_ops.len(), 1);
                prop_assert_eq!(transition.request_dedupe_ops[0].clone(), RequestDedupeOp { request_id: req.request.request_id });
            }
            Command::UnpauseActivity(req) => {
                prop_assert_eq!(transition.request_dedupe_ops.len(), 1);
                prop_assert_eq!(transition.request_dedupe_ops[0].clone(), RequestDedupeOp { request_id: req.request.request_id });
            }
            Command::ResetActivity(req) => {
                prop_assert_eq!(transition.request_dedupe_ops.len(), 1);
                prop_assert_eq!(transition.request_dedupe_ops[0].clone(), RequestDedupeOp { request_id: req.request.request_id });
            }
            Command::UpdateExecutionOptions(req) => {
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
            HistoryEventKind::WorkflowTaskFailed { logical_seq, scheduled_event_id, started_event_id, failure_cause, failure_details, identity, base_run_id, new_run_id, fork_event_version, fork_event_id } => {
                prop_assert_eq!(*logical_seq, LogicalTaskSeq(50));
                prop_assert_eq!(*scheduled_event_id, 13);
                prop_assert_eq!(*started_event_id, 21);
                prop_assert_eq!(failure_cause, &req.failure_cause);
                prop_assert_eq!(failure_details, &req.failure_details);
                prop_assert_eq!(identity, &req.worker_identity);
                prop_assert!(base_run_id.is_none());
                prop_assert!(new_run_id.is_none());
                prop_assert!(fork_event_version.is_none());
                prop_assert!(fork_event_id.is_none());
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
    fn property_12b_previous_started_event_id_tracks_last_completion(
        started_event_ids in prop::collection::vec(1i64..1_000i64, 1..8)
    ) {
        let now = fixed_now();
        let mut state = make_open_state(now);

        for (idx, started_event_id) in started_event_ids.iter().enumerate() {
            let logical_seq = 100 + idx as u64;
            state = with_pending_wft(state, logical_seq, Some(*started_event_id), 1);
            let run_key = state.run_key;
            let transition = kernel().apply(
                LoadedRun::Existing(state),
                Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                    client_discards_speculative_with_events: false,
                    token: WorkflowTaskToken {
                        run_key,
                        logical_seq: LogicalTaskSeq(logical_seq),
                        started_event_id: *started_event_id,
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
                    now,
                }),
            ).unwrap();
            state = transition.next_state;
        }

        prop_assert_eq!(
            state.previous_started_event_id,
            *started_event_ids.last().expect("non-empty sequence"),
        );
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
        // The retry is TRANSIENT (spec transient-wft Req B.1/B.3): its scheduled
        // id is the virtual next-event id (WorkflowTaskFailed landed as event 15
        // -> virtual 16), not the original real Scheduled id
        // (workflow_task_state_machine.go:376-379 @ v1.31.0).
        prop_assert_eq!(failed_pending.scheduled_event_id, 16);
        prop_assert_eq!(failed_pending.started_event_id, None);
        prop_assert_eq!(failed_pending.attempt, 2);

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
        prop_assert_eq!(timed_out_pending.logical_seq, LogicalTaskSeq(62));
        // Virtual scheduled id: TimedOut landed as event 15 and the transient
        // reschedule persists nothing, so 16 is virtual (last_event_id stays 15).
        prop_assert_eq!(timed_out_pending.scheduled_event_id, 16);
        prop_assert_eq!(timed_out_transition.next_state.last_event_id, 15);
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
        // Transient model (spec transient-wft Req B.1/B.3): the attempt-1 timeout
        // persists WorkflowTaskTimedOut only; the retry's Scheduled event is
        // virtual (workflow_task_state_machine.go:376-379 @ v1.31.0).
        prop_assert_eq!(timed_out_transition.history_events.len(), 1);
        prop_assert_eq!(timed_out_transition.dispatch_ops.len(), 1);
        prop_assert_eq!(matches!(timed_out_transition.history_events[0].kind, HistoryEventKind::WorkflowTaskTimedOut { .. }), true);
        prop_assert_eq!(matches!(timed_out_transition.dispatch_ops[0], DispatchOp::EnqueueWorkflowTask { logical_seq: LogicalTaskSeq(82), .. }), true);
        prop_assert!(timed_out_transition.request_dedupe_ops.is_empty());
        prop_assert!(timed_out_transition.activity_ops.is_empty());
        prop_assert!(timed_out_transition.timer_ops.is_empty());
        prop_assert!(timed_out_transition.projection_ops.is_empty());
        prop_assert_eq!(timed_out_transition.next_state.status, ExecutionStatus::Running);
    }

    #[test]
    fn property_17_cancel_event_field_pass_through(req in arb_cancel_request(fixed_now())) {
        let now = fixed_now();
        let transition = kernel().apply(
            LoadedRun::Existing(make_open_state(now)),
            Command::Cancel(req.clone()),
        ).unwrap();
        match &transition.history_events[0].kind {
            HistoryEventKind::WorkflowExecutionCancelRequested {
                reason,
                external_workflow_execution,
                request_id,
                ..
            } => {
                prop_assert_eq!(reason, &req.reason);
                prop_assert_eq!(external_workflow_execution, &req.external_initiator);
                prop_assert_eq!(request_id, &req.request.request_id.0);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn property_18_cancel_does_not_close(req in arb_cancel_request(fixed_now())) {
        let now = fixed_now();
        let transition = kernel().apply(
            LoadedRun::Existing(with_activity(with_timer(make_open_state(now), "timer-1", now), "activity-1")),
            Command::Cancel(req),
        ).unwrap();
        prop_assert_eq!(transition.next_state.status, ExecutionStatus::Running);
        prop_assert!(transition.next_state.cancel_requested);
        prop_assert_eq!(transition.next_state.closed_at, None);
        prop_assert!(transition.projection_ops.is_empty());
        prop_assert!(transition.activity_ops.is_empty());
        prop_assert!(transition.timer_ops.is_empty());
    }

    #[test]
    fn property_describe_cancel_requested_survives_live_apply_and_replay(
        start in arb_start_request(),
        cancel in arb_cancel_request(fixed_now()),
    ) {
        let kernel = kernel();
        let start_transition = kernel
            .apply(LoadedRun::Absent, Command::Start(start.clone()))
            .unwrap();
        prop_assert!(!start_transition.next_state.cancel_requested);

        let cancel_transition = kernel
            .apply(
                LoadedRun::Existing(start_transition.next_state.clone()),
                Command::Cancel(cancel),
            )
            .unwrap();
        prop_assert!(cancel_transition.next_state.cancel_requested);

        let mut history = start_transition.history_events;
        history.extend(cancel_transition.history_events);
        let replayed = kernel
            .replay_history_prefix(
                ReplayContext {
                    run_key: start.run_key,
                    namespace_id: start.namespace_id,
                    workflow_id: start.workflow_id,
                    run_id: start.run_id,
                    deployment: start.deployment,
                    build_id: start.build_id,
                    parent_run_key: start.parent_run_key,
                    parent_workflow_id: start.parent_workflow_id,
                    first_run_started_at: start.first_run_started_at,
                },
                &history,
            )
            .unwrap();
        prop_assert!(replayed.cancel_requested);
    }

    #[test]
    fn property_18_per_run_versioning_replay_round_trip(
        initial_info in arb_workflow_versioning_info(),
        initial_worker_deployment_name in prop::option::of("[a-z][a-z0-9-]{0,16}"),
        completions in prop::collection::vec(arb_wft_versioning_completion(), 0..8),
    ) {
        let kernel = kernel();
        let now = fixed_now();
        let run_key = RunKey::new();
        let namespace_id = NamespaceId::new();
        let workflow_id = WorkflowId("workflow".to_string());
        let run_id = RunId::new();
        let ctx = ReplayContext {
            run_key,
            namespace_id,
            workflow_id: workflow_id.clone(),
            run_id,
            deployment: None,
            build_id: None,
            parent_run_key: None,
            parent_workflow_id: None,
            first_run_started_at: Some(now),
        };
        let start_event = HistoryEvent {
            event_id: 1,
            happened_at: now,
            kind: HistoryEventKind::WorkflowExecutionStarted {
                workflow_type: WorkflowType("wf".to_string()),
                task_queue: TaskQueueName("queue".to_string()),
                input: Payloads::default(),
                header: None,
                workflow_start_delay: None,
                completion_callbacks: Vec::new(),
                user_metadata: None,
                links: Vec::new(),
                memo: Memo::default(),
                search_attributes: SearchAttributes::default(),
                request_id: "versioning-start".to_string(),
                identity: "tester".to_string(),
                continued_execution_run_id: None,
                first_execution_run_id: Some(run_id),
                retry_policy: None,
                attempt: 1,
                workflow_execution_timeout: None,
                workflow_run_timeout: None,
                workflow_task_timeout: default_workflow_task_timeout(),
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
                cron_schedule: None,
                versioning_info: Some(initial_info),
                worker_deployment_name: initial_worker_deployment_name,
                priority: None,
            },
        };
        let mut history = vec![start_event.clone()];
        let mut reference = kernel
            .replay_history_prefix(ctx.clone(), &[start_event])
            .unwrap();
        let mut next_event_id = 2;

        for (index, (behavior, deployment_version, worker_deployment_name)) in completions.into_iter().enumerate() {
            let logical_seq = LogicalTaskSeq(index as u64 + 1);
            let scheduled_event_id = next_event_id;
            history.push(HistoryEvent {
                event_id: scheduled_event_id,
                happened_at: now,
                kind: HistoryEventKind::WorkflowTaskScheduled {
                    logical_seq,
                    task_queue: TaskQueueName("queue".to_string()),
                    workflow_task_timeout: default_workflow_task_timeout(),
                    attempt: 1,
                },
            });
            next_event_id += 1;

            let started_event_id = next_event_id;
            history.push(HistoryEvent {
                event_id: started_event_id,
                happened_at: now,
                kind: HistoryEventKind::WorkflowTaskStarted {
                    logical_seq,
                    scheduled_event_id,
                    attempt: 1,
                    identity: WorkerIdentity("worker".to_string()),
                    request_id: format!("wft-start-{index}"),
                    history_size_bytes: 0,
                    suggest_continue_as_new: false,
                },
            });
            next_event_id += 1;

            history.push(HistoryEvent {
                event_id: next_event_id,
                happened_at: now,
                kind: HistoryEventKind::WorkflowTaskCompleted {
                    logical_seq,
                    scheduled_event_id,
                    started_event_id,
                    identity: WorkerIdentity("worker".to_string()),
                    sdk_metadata: None,
                metering_metadata: None,
                    worker_version: None,
                    versioning_behavior: behavior,
                    deployment_version: deployment_version.clone(),
                    worker_deployment_name: worker_deployment_name.clone(),
                },
            });
            next_event_id += 1;

            reference.apply_wft_versioning(behavior, deployment_version, worker_deployment_name);
            reference.previous_started_event_id = started_event_id;
            reference.workflow_task_attempt = 1;
            reference.pending_workflow_task = None;
        }

        let replayed = kernel.replay_history_prefix(ctx, &history).unwrap();

        prop_assert_eq!(&replayed.versioning_info, &reference.versioning_info);
        prop_assert_eq!(
            &replayed.worker_deployment_name,
            &reference.worker_deployment_name
        );
        prop_assert_eq!(
            replayed.effective_deployment().cloned(),
            reference.effective_deployment().cloned()
        );
        prop_assert_eq!(replayed.effective_behavior(), reference.effective_behavior());
    }

    #[test]
    fn property_describe_root_execution_event_or_self_survives_replay(
        mut start in arb_start_request(),
        authored_root in any::<bool>(),
        parented in any::<bool>(),
    ) {
        let root_workflow_id = WorkflowId("root-workflow".to_string());
        let root_run_id = RunId::new();
        if authored_root {
            start.root_workflow_id = Some(root_workflow_id.clone());
            start.root_run_id = Some(root_run_id);
        }
        if parented {
            start.parent_run_key = Some(RunKey::new());
            start.parent_workflow_id = Some(WorkflowId("parent-workflow".to_string()));
            start.parent_run_id = Some(RunId::new());
            start.parent_namespace_id = Some(start.namespace_id);
            start.parent_initiated_event_id = 7;
        }

        let kernel = kernel();
        let transition = kernel
            .apply(LoadedRun::Absent, Command::Start(start.clone()))
            .unwrap();
        let expected_workflow_id = start
            .root_workflow_id
            .clone()
            .unwrap_or_else(|| start.workflow_id.clone());
        let expected_run_id = start.root_run_id.unwrap_or(start.run_id);
        prop_assert_eq!(
            transition.next_state.root_workflow_id.as_ref(),
            Some(&expected_workflow_id)
        );
        prop_assert_eq!(transition.next_state.root_run_id, Some(expected_run_id));

        match &transition.history_events[0].kind {
            HistoryEventKind::WorkflowExecutionStarted {
                root_workflow_id,
                root_run_id,
                ..
            } => {
                prop_assert_eq!(root_workflow_id, &start.root_workflow_id);
                prop_assert_eq!(root_run_id, &start.root_run_id);
            }
            other => panic!("unexpected event: {other:?}"),
        }

        let replayed = kernel
            .replay_history_prefix(
                ReplayContext {
                    run_key: start.run_key,
                    namespace_id: start.namespace_id,
                    workflow_id: start.workflow_id,
                    run_id: start.run_id,
                    deployment: start.deployment,
                    build_id: start.build_id,
                    parent_run_key: start.parent_run_key,
                    parent_workflow_id: start.parent_workflow_id,
                    first_run_started_at: start.first_run_started_at,
                },
                &transition.history_events,
            )
            .unwrap();
        prop_assert_eq!(replayed.root_workflow_id, Some(expected_workflow_id));
        prop_assert_eq!(replayed.root_run_id, Some(expected_run_id));
    }

    #[test]
    fn property_19_cancel_wft_coalescing(no_pending in prop::bool::ANY, req in arb_cancel_request(fixed_now())) {
        let now = fixed_now();
        let state = if no_pending {
            make_open_state(now)
        } else {
            with_pending_wft(make_open_state(now), 90, None, 0)
        };
        let transition = kernel().apply(LoadedRun::Existing(state), Command::Cancel(req)).unwrap();
        if no_pending {
            prop_assert!(transition.next_state.pending_workflow_task.is_some());
            prop_assert_eq!(transition.dispatch_ops.len(), 1);
        } else {
            prop_assert!(transition.dispatch_ops.is_empty());
            prop_assert_eq!(
                transition.next_state.pending_workflow_task.as_ref().unwrap().logical_seq,
                LogicalTaskSeq(90)
            );
        }
    }

    #[test]
    fn property_20_terminate_event_field_pass_through(req in arb_terminate_request(fixed_now())) {
        let now = fixed_now();
        let transition = kernel().apply(
            LoadedRun::Existing(make_open_state(now)),
            Command::Terminate(req.clone()),
        ).unwrap();
        match &transition.history_events[0].kind {
            HistoryEventKind::WorkflowExecutionTerminated { reason, details, identity } => {
                prop_assert_eq!(reason, &req.reason);
                prop_assert_eq!(details, &req.details);
                prop_assert_eq!(identity, &req.identity);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn property_21_terminate_closes_with_terminal_invariants(req in arb_terminate_request(fixed_now())) {
        let now = fixed_now();
        let state = with_sticky(
            with_pending_wft(with_activity(with_timer(make_open_state(now), "timer-1", now), "activity-1"), 91, None, 0),
            "sticky-worker",
            now,
        );
        let transition = kernel().apply(LoadedRun::Existing(state), Command::Terminate(req)).unwrap();
        prop_assert_eq!(transition.next_state.status, ExecutionStatus::Terminated);
        prop_assert!(transition.next_state.closed_at.is_some());
        prop_assert!(transition.next_state.pending_workflow_task.is_none());
        prop_assert!(transition.next_state.sticky.is_none());
        prop_assert!(transition.next_state.activities.is_empty());
        prop_assert!(transition.next_state.timers.is_empty());
        prop_assert!(transition.dispatch_ops.is_empty());
    }

    #[test]
    fn property_22_terminate_entity_cleanup(req in arb_terminate_request(fixed_now())) {
        let now = fixed_now();
        let mut state = with_activity(make_open_state(now), "activity-1");
        state.activities.insert(
            "activity-2".into(),
            ActivityState {
                cancel_requested: false,
                started_identity: None,
                retry_last_worker_identity: None,
                activity_id: "activity-2".into(),
                activity_type: "activity-type".into(),
                schedule_event_id: 11,
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
        state = with_timer(state, "timer-1", now);
        let transition = kernel().apply(LoadedRun::Existing(state), Command::Terminate(req)).unwrap();
        prop_assert_eq!(transition.activity_ops.len(), 2);
        prop_assert_eq!(transition.timer_ops.len(), 1);
        for op in &transition.activity_ops {
            match op {
                ActivityOp::Delete { activity_id } => prop_assert!(activity_id == "activity-1" || activity_id == "activity-2"),
                _ => panic!("unexpected activity op"),
            }
        }
        match &transition.timer_ops[0] {
            TimerOp::Delete { timer_id } => prop_assert_eq!(timer_id, "timer-1"),
            _ => panic!("unexpected timer op"),
        }
    }

    #[test]
    fn property_25_continue_as_new_closes_with_terminal_invariants(cmd in arb_continue_as_new_command()) {
        let now = fixed_now();
        let state = with_sticky(with_pending_wft(make_open_state(now), 94, Some(22), 1), "sticky-worker", now);
        let transition = kernel().apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(94),
                    started_event_id: 22,
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
                commands: vec![cmd],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now,
            }),
        ).unwrap();
        prop_assert_eq!(transition.next_state.status, ExecutionStatus::ContinuedAsNew);
        prop_assert!(transition.next_state.closed_at.is_some());
        prop_assert!(transition.next_state.pending_workflow_task.is_none());
        prop_assert!(transition.next_state.sticky.is_none());
        prop_assert!(transition.dispatch_ops.is_empty());
    }

    #[test]
    fn property_26_continue_as_new_field_pass_through(cmd in arb_continue_as_new_command()) {
        let now = fixed_now();
        let state = with_pending_wft(make_open_state(now), 95, Some(23), 1);
        let transition = kernel().apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(95),
                    started_event_id: 23,
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
                commands: vec![cmd.clone()],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now,
            }),
        ).unwrap();
        match (&transition.history_events[1].kind, &cmd) {
            (
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
                },
                WorkflowCommand::ContinueAsNew {
                    header: None,
                    new_run_id: expected_new_run_id,
                    workflow_type: expected_workflow_type,
                    task_queue: expected_task_queue,
                    input: expected_input,
                    memo: expected_memo,
                    search_attributes: expected_search_attributes,
                    // Inherited from the run, not the command — see below.
                    workflow_execution_timeout: _expected_execution_timeout,
                    workflow_run_timeout: expected_run_timeout,
                    workflow_task_timeout: expected_task_timeout,
                    ..
                },
            ) => {
                prop_assert_eq!(new_run_id, expected_new_run_id);
                prop_assert_eq!(workflow_type, expected_workflow_type);
                prop_assert_eq!(task_queue, expected_task_queue);
                prop_assert_eq!(input, expected_input);
                prop_assert_eq!(memo, expected_memo);
                prop_assert_eq!(search_attributes, expected_search_attributes);
                // The execution timeout is inherited from the run (the chain's
                // first-run deadline), not carried on the CaN command.
                prop_assert_eq!(workflow_execution_timeout, &state.workflow_execution_timeout);
                prop_assert_eq!(workflow_run_timeout, expected_run_timeout);
                prop_assert_eq!(workflow_task_timeout, expected_task_timeout);
            }
            other => panic!("unexpected continue-as-new event: {other:?}"),
        }
    }

    #[test]
    fn property_27_continue_as_new_is_terminal(cmd in arb_continue_as_new_command()) {
        let now = fixed_now();
        let state = with_pending_wft(make_open_state(now), 96, Some(24), 1);
        prop_assert_eq!(
            kernel().apply(
                LoadedRun::Existing(state.clone()),
                Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                    client_discards_speculative_with_events: false,
                    token: WorkflowTaskToken {
                        run_key: state.run_key,
                        logical_seq: LogicalTaskSeq(96),
                        started_event_id: 24,
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
                    commands: vec![cmd, WorkflowCommand::RequestNewWorkflowTask],
                    force_new_workflow_task: false,
                    delivered_update_ids: Vec::new(),
                    now,
                }),
            ),
            Err(tokeira_kernel::Reject::CommandsAfterClose { index: 1 })
        );
    }

    #[test]
    fn property_28_timeout_closes_with_terminal_invariants(req in arb_workflow_execution_timed_out_request(fixed_now())) {
        let now = fixed_now();
        let state = with_sticky(
            with_pending_wft(with_timer(with_activity(make_open_state(now), "activity-1"), "timer-1", now), 97, None, 0),
            "sticky-worker",
            now,
        );
        let transition = kernel().apply(LoadedRun::Existing(state), Command::WorkflowExecutionTimedOut(req)).unwrap();
        prop_assert_eq!(transition.next_state.status, ExecutionStatus::TimedOut);
        prop_assert!(transition.next_state.closed_at.is_some());
        prop_assert!(transition.next_state.pending_workflow_task.is_none());
        prop_assert!(transition.next_state.sticky.is_none());
        prop_assert!(transition.next_state.activities.is_empty());
        prop_assert!(transition.next_state.timers.is_empty());
        prop_assert!(transition.dispatch_ops.is_empty());
    }

    #[test]
    fn property_29_timeout_entity_cleanup(req in arb_workflow_execution_timed_out_request(fixed_now())) {
        let now = fixed_now();
        let mut state = with_activity(make_open_state(now), "activity-1");
        state.activities.insert(
            "activity-2".into(),
            ActivityState {
                cancel_requested: false,
                started_identity: None,
                retry_last_worker_identity: None,
                activity_id: "activity-2".into(),
                activity_type: "activity-type".into(),
                schedule_event_id: 11,
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
        state = with_timer(state, "timer-1", now);
        let transition = kernel().apply(LoadedRun::Existing(state), Command::WorkflowExecutionTimedOut(req)).unwrap();
        prop_assert_eq!(transition.activity_ops.len(), 2);
        prop_assert_eq!(transition.timer_ops.len(), 1);
        prop_assert!(transition.next_state.activities.is_empty());
        prop_assert!(transition.next_state.timers.is_empty());
    }

    #[test]
    fn property_30_timeout_event_field_pass_through(req in arb_workflow_execution_timed_out_request(fixed_now())) {
        let now = fixed_now();
        let transition = kernel().apply(
            LoadedRun::Existing(make_open_state(now)),
            Command::WorkflowExecutionTimedOut(req.clone()),
        ).unwrap();
        match &transition.history_events[0].kind {
            HistoryEventKind::WorkflowExecutionTimedOut { timeout_type, retry_state, .. } => {
                prop_assert_eq!(timeout_type, &req.timeout_type);
                prop_assert_eq!(retry_state, &req.retry_state);
            }
            other => panic!("unexpected timeout event: {other:?}"),
        }
    }

    #[test]
    fn property_31_timeout_no_request_dedupe(req in arb_workflow_execution_timed_out_request(fixed_now())) {
        let now = fixed_now();
        let transition = kernel().apply(
            LoadedRun::Existing(make_open_state(now)),
            Command::WorkflowExecutionTimedOut(req),
        ).unwrap();
        prop_assert!(transition.request_dedupe_ops.is_empty());
    }

    #[test]
    fn property_32_fail_workflow_retry_metadata(has_retry_policy in prop::bool::ANY) {
        let now = fixed_now();
        let mut state = with_pending_wft(make_open_state(now), 98, Some(25), 1);
        state.attempt = 7;
        if !has_retry_policy {
            state.retry_policy = None;
        }
        let transition = kernel().apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(98),
                    started_event_id: 25,
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
                    failure: payload("failed"),
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now,
            }),
        ).unwrap();
        match &transition.history_events[1].kind {
            HistoryEventKind::WorkflowExecutionFailed { retry_state, attempt, .. } => {
                prop_assert_eq!(*attempt, 7);
                prop_assert_eq!(
                    retry_state,
                    &(if has_retry_policy {
                        RetryState::InProgress
                    } else {
                        RetryState::RetryPolicyNotSet
                    })
                );
            }
            other => panic!("unexpected failure event: {other:?}"),
        }
    }

}

proptest! {
    #[test]
    fn property_58_pause_workflow_produces_correct_state_and_event(
        req in arb_pause_workflow_request(fixed_now()),
        extra_activities in 0usize..3usize,
    ) {
        let now = fixed_now();
        let mut state = make_open_state(now);
        for idx in 0..extra_activities {
            state = with_activity(state, &format!("activity-{idx}"));
        }

        let transition = kernel().apply(
            LoadedRun::Existing(state.clone()),
            Command::PauseWorkflow(req.clone()),
        ).unwrap();

        prop_assert_eq!(transition.next_state.status, ExecutionStatus::Paused);
        prop_assert_eq!(transition.next_state.wft_stamp, state.wft_stamp + 1);
        let pause_info = transition.next_state.pause_info.clone().unwrap();
        prop_assert_eq!(pause_info.pause_time, req.now);
        prop_assert_eq!(pause_info.identity, req.identity);
        prop_assert_eq!(pause_info.reason, req.reason);
        prop_assert_eq!(pause_info.request_id, req.request.request_id.0);
        prop_assert_eq!(transition.history_events.len(), 1);
        prop_assert_eq!(matches!(transition.history_events[0].kind, HistoryEventKind::WorkflowExecutionPaused { .. }), true);
        prop_assert_eq!(transition.request_dedupe_ops.len(), 1);
        prop_assert_eq!(transition.activity_ops.len(), state.activities.len());
        prop_assert_eq!(transition.dispatch_ops.iter().all(|op| !matches!(op, DispatchOp::EnqueueWorkflowTask { .. })), true);
    }

    #[test]
    fn property_59_unpause_workflow_produces_correct_state_and_dispatches(
        req in arb_unpause_workflow_request(fixed_now()),
        extra_activities in 0usize..3usize,
        has_pending_wft in any::<bool>(),
    ) {
        let now = fixed_now();
        let mut state = with_paused_status(make_open_state(now), now, "pause-req");
        for idx in 0..extra_activities {
            state = with_activity(state, &format!("activity-{idx}"));
        }
        if has_pending_wft {
            state = with_pending_wft(state, 77, None, 0);
        }

        let transition = kernel().apply(
            LoadedRun::Existing(state.clone()),
            Command::UnpauseWorkflow(req),
        ).unwrap();

        prop_assert_eq!(transition.next_state.status, ExecutionStatus::Running);
        prop_assert_eq!(transition.next_state.pause_info, None);
        prop_assert_eq!(transition.next_state.wft_stamp, state.wft_stamp + 1);
        prop_assert_eq!(matches!(transition.history_events[0].kind, HistoryEventKind::WorkflowExecutionUnpaused { .. }), true);
        prop_assert_eq!(
            transition.dispatch_ops.iter().filter(|op| matches!(op, DispatchOp::EnqueueActivityTask { .. })).count(),
            state.activities.len()
        );
        let workflow_task_dispatches = transition.dispatch_ops.iter().filter(|op| matches!(op, DispatchOp::EnqueueWorkflowTask { .. })).count();
        prop_assert_eq!(workflow_task_dispatches, if has_pending_wft { 0 } else { 1 });
    }

    #[test]
    fn property_60_pause_workflow_idempotency(req in arb_pause_workflow_request(fixed_now())) {
        let now = fixed_now();
        let paused = with_paused_status(make_open_state(now), now, &req.request.request_id.0);
        let idempotent = kernel().apply(
            LoadedRun::Existing(paused.clone()),
            Command::PauseWorkflow(req.clone()),
        ).unwrap();
        prop_assert!(idempotent.history_events.is_empty());
        prop_assert!(idempotent.request_dedupe_ops.is_empty());
        prop_assert!(idempotent.activity_ops.is_empty());
        prop_assert!(idempotent.dispatch_ops.is_empty());
        prop_assert!(idempotent.projection_ops.is_empty());

        let conflicting = kernel().apply(
            LoadedRun::Existing(paused),
            Command::PauseWorkflow(PauseWorkflowRequest {
                request: request_context("different-request", now),
                ..req
            }),
        );
        prop_assert_eq!(conflicting, Err(tokeira_kernel::Reject::AlreadyPaused));
    }

    // Paused workflows must suppress WFT scheduling on every kernel wakeup path,
    // not just the signal/cancel/activity paths. The WFT-suppression invariant is
    // enforced structurally at the `schedule_workflow_task()` chokepoint and the
    // single direct `EnqueueWorkflowTask` site, both of which guard on paused
    // state. This test asserts the invariant explicitly across the full set of
    // wakeup-producing commands so a future refactor that bypasses the chokepoint
    // cannot silently wake a paused workflow.
    //
    // Covered command variants (every path that can schedule a WFT, excluding
    // `UnpauseWorkflow`, which intentionally transitions to `Running` first):
    //   - Signal
    //   - Cancel
    //   - ActivityResolved
    //   - TimerDue
    //   - ChildResolved
    //   - ExternalSignalResolved
    //   - ExternalCancelResolved
    //   - NexusOperationResolved (terminal)
    #[test]
    fn property_61_paused_workflows_suppress_wft_scheduling(
        signal_input in arb_payloads(),
        completed in arb_payloads(),
    ) {
        let now = fixed_now();
        let paused = with_paused_status(make_open_state(now), now, "pause-req");

        let no_wft = |transition: &tokeira_kernel::Transition| {
            transition
                .dispatch_ops
                .iter()
                .all(|op| !matches!(op, DispatchOp::EnqueueWorkflowTask { .. }))
        };

        let signal_transition = kernel().apply(
            LoadedRun::Existing(paused.clone()),
            Command::Signal(SignalRequest {
                signal_name: "sig".into(),
                input: signal_input,
                header: None,
                links: Vec::new(),
                request: request_context("sig-req", now),
                now,
            }),
        ).unwrap();
        prop_assert_eq!(no_wft(&signal_transition), true);

        let cancel_transition = kernel().apply(
            LoadedRun::Existing(paused.clone()),
            Command::Cancel(CancelRequest {
                reason: "cancel".into(),
                external_initiator: None,
                external_initiated_event_id: 0,
                request: request_context("cancel-req", now),
                now,
            }),
        ).unwrap();
        prop_assert_eq!(no_wft(&cancel_transition), true);

        let activity_transition = kernel().apply(
            LoadedRun::Existing(with_activity(paused.clone(), "activity-1")),
            Command::ActivityResolved(ActivityResolvedRequest {
                activity_id: "activity-1".into(),
                resolution: ActivityResolution::Completed { result: completed },
                worker_identity: None,
                now,
            }),
        ).unwrap();
        prop_assert_eq!(no_wft(&activity_transition), true);

        let timer_transition = kernel().apply(
            LoadedRun::Existing(with_timer(paused.clone(), "timer-1", now)),
            Command::TimerDue(TimerDueRequest {
                timer_id: "timer-1".into(),
                fired_at: now,
            }),
        ).unwrap();
        prop_assert_eq!(no_wft(&timer_transition), true);

        let child_transition = kernel().apply(
            LoadedRun::Existing(with_child(
                paused.clone(),
                "child-1",
                21,
                ParentClosePolicy::Terminate,
                true,
            )),
            Command::ChildResolved(ChildResolvedRequest {
                resolved_run_id: None,
                child_workflow_id: WorkflowId("child-1".into()),
                resolution: ChildResolution::Completed {
                    result: Payloads::default(),
                },
                now,
            }),
        ).unwrap();
        prop_assert_eq!(no_wft(&child_transition), true);

        let external_signal_transition = kernel().apply(
            LoadedRun::Existing(with_pending_external_signal(paused.clone(), 60)),
            Command::ExternalSignalResolved(ExternalSignalResolvedRequest {
                initiated_event_id: 60,
                result: ExternalSignalResult::Signaled,
                now,
            }),
        ).unwrap();
        prop_assert_eq!(no_wft(&external_signal_transition), true);

        let external_cancel_transition = kernel().apply(
            LoadedRun::Existing(with_pending_external_cancel(paused.clone(), 61)),
            Command::ExternalCancelResolved(ExternalCancelResolvedRequest {
                initiated_event_id: 61,
                result: ExternalCancelResult::CancelRequested,
                now,
            }),
        ).unwrap();
        prop_assert_eq!(no_wft(&external_cancel_transition), true);

        let nexus_transition = kernel().apply(
            LoadedRun::Existing(with_pending_nexus_operation(paused, "nexus-op-1")),
            Command::NexusOperationResolved(NexusOperationResolvedRequest {
                operation_id: "nexus-op-1".into(),
                scheduled_event_id: 12,
                resolution: NexusResolution::Completed {
                    result: Payloads::default(),
                    links: Vec::new(),
                },
                now,
            }),
        ).unwrap();
        prop_assert_eq!(no_wft(&nexus_transition), true);
    }

    #[test]
    fn property_62_activity_management_emits_no_history_and_no_wft(
        update_req in arb_update_activity_options_request("activity-1".into(), fixed_now()),
        pause_req in arb_pause_activity_request("activity-1".into(), fixed_now()),
        reset_req in arb_reset_activity_request("activity-1".into(), fixed_now()),
    ) {
        let now = fixed_now();
        let base = with_activity(make_open_state(now), "activity-1");

        let update_transition = kernel().apply(
            LoadedRun::Existing(base.clone()),
            Command::UpdateActivityOptions(update_req),
        ).unwrap();
        let pause_transition = kernel().apply(
            LoadedRun::Existing(base.clone()),
            Command::PauseActivity(pause_req),
        ).unwrap();
        let reset_transition = kernel().apply(
            LoadedRun::Existing(base),
            Command::ResetActivity(reset_req),
        ).unwrap();

        for transition in [update_transition, pause_transition, reset_transition] {
            prop_assert!(transition.history_events.is_empty());
            prop_assert_eq!(transition.dispatch_ops.iter().all(|op| !matches!(op, DispatchOp::EnqueueWorkflowTask { .. })), true);
            prop_assert_eq!(transition.request_dedupe_ops.len(), 1);
            prop_assert_eq!(transition.activity_ops.len(), 1);
        }
    }

    #[test]
    fn property_63_update_activity_options_mutates_specified_fields_correctly(
        req in arb_update_activity_options_request("activity-1".into(), fixed_now())
    ) {
        let now = fixed_now();
        let state = with_activity(make_open_state(now), "activity-1");
        let before = state.activities.get("activity-1").unwrap().clone();
        let transition = kernel().apply(
            LoadedRun::Existing(state),
            Command::UpdateActivityOptions(req.clone()),
        ).unwrap();
        let after = transition.next_state.activities.get("activity-1").unwrap();

        match req.task_queue {
            FieldChange::Set(ref v) => prop_assert_eq!(&after.task_queue, v),
            FieldChange::Unchanged | FieldChange::Clear => prop_assert_eq!(&after.task_queue, &before.task_queue),
        }
        match req.schedule_to_close_timeout {
            FieldChange::Set(v) => prop_assert_eq!(after.schedule_to_close_timeout, v),
            FieldChange::Clear => prop_assert_eq!(after.schedule_to_close_timeout, None),
            FieldChange::Unchanged => prop_assert_eq!(after.schedule_to_close_timeout, before.schedule_to_close_timeout),
        }
        match req.schedule_to_start_timeout {
            FieldChange::Set(v) => prop_assert_eq!(after.schedule_to_start_timeout, v),
            FieldChange::Clear => prop_assert_eq!(after.schedule_to_start_timeout, None),
            FieldChange::Unchanged => prop_assert_eq!(after.schedule_to_start_timeout, before.schedule_to_start_timeout),
        }
        match req.start_to_close_timeout {
            FieldChange::Set(v) => prop_assert_eq!(after.start_to_close_timeout, v),
            FieldChange::Clear => prop_assert_eq!(after.start_to_close_timeout, None),
            FieldChange::Unchanged => prop_assert_eq!(after.start_to_close_timeout, before.start_to_close_timeout),
        }
        match req.heartbeat_timeout {
            FieldChange::Set(v) => prop_assert_eq!(after.heartbeat_timeout, v),
            FieldChange::Clear => prop_assert_eq!(after.heartbeat_timeout, None),
            FieldChange::Unchanged => prop_assert_eq!(after.heartbeat_timeout, before.heartbeat_timeout),
        }
    }

    #[test]
    fn property_64_pause_and_unpause_activity_manage_pause_info(
        pause_req in arb_pause_activity_request("activity-1".into(), fixed_now()),
        unpause_req in arb_unpause_activity_request("activity-1".into(), fixed_now()),
        workflow_paused in any::<bool>(),
    ) {
        let now = fixed_now();
        let mut state = with_activity(make_open_state(now), "activity-1");
        if workflow_paused {
            state = with_paused_status(state, now, "pause-req");
        }

        let paused = kernel().apply(
            LoadedRun::Existing(state.clone()),
            Command::PauseActivity(pause_req.clone()),
        ).unwrap();
        let paused_activity = paused.next_state.activities.get("activity-1").unwrap();
        prop_assert_eq!(paused_activity.pause_info.clone(), Some(ActivityPauseInfo {
            pause_time: pause_req.now,
            identity: pause_req.identity,
            reason: pause_req.reason,
        }));

        let unpaused = kernel().apply(
            LoadedRun::Existing(paused.next_state),
            Command::UnpauseActivity(unpause_req),
        ).unwrap();
        let unpaused_activity = unpaused.next_state.activities.get("activity-1").unwrap();
        prop_assert_eq!(unpaused_activity.pause_info.clone(), None);
        let activity_dispatches = unpaused.dispatch_ops.iter().filter(|op| matches!(op, DispatchOp::EnqueueActivityTask { .. })).count();
        prop_assert_eq!(activity_dispatches, if workflow_paused { 0 } else { 1 });
    }

    #[test]
    fn property_65_unpause_activity_rejects_non_paused_activity(req in arb_unpause_activity_request("activity-1".into(), fixed_now())) {
        let now = fixed_now();
        let result = kernel().apply(
            LoadedRun::Existing(with_activity(make_open_state(now), "activity-1")),
            Command::UnpauseActivity(req),
        );
        prop_assert_eq!(result, Err(tokeira_kernel::Reject::ActivityNotPaused("activity-1".into())));
    }

    #[test]
    fn property_66_reset_activity_resets_attempt_and_dispatches_conditionally(
        req in arb_reset_activity_request("activity-1".into(), fixed_now()),
        workflow_paused in any::<bool>(),
        attempt in 2u32..10u32,
    ) {
        let now = fixed_now();
        let mut state = with_activity(make_open_state(now), "activity-1");
        if workflow_paused {
            state = with_paused_status(state, now, "pause-req");
        }
        state.activities.get_mut("activity-1").unwrap().attempt = attempt;

        let transition = kernel().apply(
            LoadedRun::Existing(state),
            Command::ResetActivity(req),
        ).unwrap();
        let activity = transition.next_state.activities.get("activity-1").unwrap();
        prop_assert_eq!(activity.attempt, 1);
        let activity_dispatches = transition.dispatch_ops.iter().filter(|op| matches!(op, DispatchOp::EnqueueActivityTask { .. })).count();
        prop_assert_eq!(activity_dispatches, if workflow_paused { 0 } else { 1 });
    }

    #[test]
    fn property_67_schedule_activity_initializes_pause_fields(cmd in arb_schedule_activity_command()) {
        let now = fixed_now();
        let state = with_pending_wft(make_open_state(now), 101, Some(27), 1);
        let transition = kernel().apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(101),
                    started_event_id: 27,
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
                commands: vec![cmd.clone()],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now,
            }),
        ).unwrap();

        let expected_activity_id = match cmd {
            WorkflowCommand::ScheduleActivity { activity_id, .. } => activity_id,
            _ => unreachable!(),
        };
        let activity = transition.next_state.activities.get(&expected_activity_id).unwrap();
        prop_assert_eq!(activity.pause_info.clone(), None);
        prop_assert_eq!(activity.stamp, 0);
    }

    #[test]
    fn property_68_unpause_workflow_rejects_non_paused(req in arb_unpause_workflow_request(fixed_now())) {
        let now = fixed_now();
        let result = kernel().apply(
            LoadedRun::Existing(make_open_state(now)),
            Command::UnpauseWorkflow(req),
        );
        prop_assert_eq!(result, Err(tokeira_kernel::Reject::NotPaused));
    }
}

// Properties 15, 23, and 24 are deterministic single-case checks, so they live
// outside the proptest! block.
#[test]
fn property_15_wft_timed_out_clears_sticky() {
    let now = fixed_now();
    let state = with_sticky(
        with_pending_wft(make_open_state(now), 71, Some(41), 1),
        "sticky-worker",
        now,
    );
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest {
                logical_seq: LogicalTaskSeq(71),
                started_event_id: 41,
                timeout_type: WorkflowTaskTimeoutType::StartToClose,
                now,
            }),
        )
        .unwrap();
    assert_eq!(transition.next_state.sticky, None);
    match &transition.dispatch_ops[0] {
        DispatchOp::EnqueueWorkflowTask {
            sticky_preferred, ..
        } => {
            assert_eq!(sticky_preferred, &None);
        }
        other => panic!("unexpected dispatch op: {other:?}"),
    }
}

#[test]
fn property_23_request_cancel_activity_preserves_activity() {
    let now = fixed_now();
    let mut state = with_activity(
        with_pending_wft(make_open_state(now), 92, Some(20), 1),
        "activity-1",
    );
    // K4: only a STARTED activity survives a cancel request (with the durable
    // cancel bit set); an unstarted one is cancelled immediately and removed
    // (workflow_task_completed_handler.go:651-665 @ v1.31.0).
    let scheduled_event_id = {
        let activity = state.activities.get_mut("activity-1").expect("fixture");
        activity.started_event_id = Some(activity.schedule_event_id + 1);
        activity.started_at = Some(now);
        activity.schedule_event_id
    };
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: RunKey::new(),
                    logical_seq: LogicalTaskSeq(92),
                    started_event_id: 20,
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
                commands: vec![WorkflowCommand::RequestCancelActivity { scheduled_event_id }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now,
            }),
        )
        .unwrap();
    let activity = transition
        .next_state
        .activities
        .get("activity-1")
        .expect("started activity survives the cancel request");
    assert!(activity.cancel_requested);
    assert!(
        transition
            .activity_ops
            .iter()
            .all(|op| !matches!(op, ActivityOp::Delete { .. }))
    );
}

#[test]
fn property_24_cancel_timer_removes_timer() {
    let now = fixed_now();
    let state = with_timer(
        with_pending_wft(make_open_state(now), 93, Some(21), 1),
        "timer-1",
        now,
    );
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: RunKey::new(),
                    logical_seq: LogicalTaskSeq(93),
                    started_event_id: 21,
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
                now,
            }),
        )
        .unwrap();
    assert!(!transition.next_state.timers.contains_key("timer-1"));
    assert!(
        transition
            .timer_ops
            .iter()
            .any(|op| matches!(op, TimerOp::Delete { timer_id } if timer_id == "timer-1"))
    );
}

proptest! {
    #[test]
    fn property_33_start_child_workflow_happy_path(
        child_workflow_id in arb_small_string(),
        workflow_type in arb_small_string(),
        task_queue in arb_small_string(),
        input in arb_payloads(),
        parent_close_policy in arb_parent_close_policy(),
    ) {
        let now = fixed_now();
        let state = with_pending_wft(make_open_state(now), 30, Some(13), 1);
        let transition = kernel().apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(30),
                    started_event_id: 13,
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
                commands: vec![WorkflowCommand::StartChildWorkflow {
                    child_workflow_id: WorkflowId(child_workflow_id.clone()),
                    namespace_id: NamespaceId::new(),
                    namespace: None,
                    workflow_type: WorkflowType(workflow_type),
                    task_queue: TaskQueueName(task_queue),
                    input,
                    header: None,
                    memo: Memo::default(),
                    search_attributes: SearchAttributes::default(),
                    workflow_execution_timeout: None,
                    workflow_run_timeout: None,
                    workflow_task_timeout: Duration::seconds(10),
                    retry_policy: None,
                    cron_schedule: None,
                    parent_close_policy,
                    reuse_policy: tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now,
            }),
        ).unwrap();

        let child = transition.next_state.children.get(&WorkflowId(child_workflow_id)).unwrap();
        prop_assert_eq!(child.child_run_id, None);
        prop_assert_eq!(child.started_event_id, None);
        prop_assert_eq!(child.parent_close_policy, parent_close_policy);
        prop_assert_eq!(transition.history_events.iter().any(|event| matches!(
            event.kind,
            HistoryEventKind::StartChildWorkflowExecutionInitiated { .. }
        )), true);
        prop_assert_eq!(transition.dispatch_ops.iter().any(|op| matches!(
            op,
            DispatchOp::StartChildWorkflow { .. }
        )), true);
    }

    #[test]
    fn property_40_child_resolved_removes_child(
        child_workflow_id in arb_small_string(),
        resolution in arb_child_resolution(),
    ) {
        let now = fixed_now();
        let state = with_child(make_open_state(now), &child_workflow_id, 22, ParentClosePolicy::Terminate, true);
        let transition = kernel().apply(
            LoadedRun::Existing(state),
            Command::ChildResolved(ChildResolvedRequest {
                resolved_run_id: None,
                child_workflow_id: WorkflowId(child_workflow_id.clone()),
                resolution,
                now,
            }),
        ).unwrap();

        prop_assert!(!transition.next_state.children.contains_key(&WorkflowId(child_workflow_id)));
    }

    #[test]
    fn property_44_signal_external_workflow_happy_path(
        target_workflow_id in arb_small_string(),
        signal_name in arb_small_string(),
        input in arb_payloads(),
    ) {
        let now = fixed_now();
        let state = with_pending_wft(make_open_state(now), 32, Some(15), 1);
        let transition = kernel().apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(32),
                    started_event_id: 15,
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
                commands: vec![WorkflowCommand::SignalExternalWorkflowExecution {
                    target_namespace_id: state.namespace_id,
                    target_namespace: None,
                    target_workflow_id: WorkflowId(target_workflow_id.clone()),
                    target_run_id: Some(RunId::new()),
                    signal_name: signal_name.clone(),
                    input,
                    header: None,
                    control: "ctl".into(),
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now,
            }),
        ).unwrap();

        let pending = transition.next_state.pending_external_signals.values().next().unwrap();
        prop_assert_eq!(&pending.target_workflow_id, &WorkflowId(target_workflow_id));
        prop_assert_eq!(&pending.signal_name, &signal_name);
        prop_assert_eq!(transition.dispatch_ops.iter().any(|op| matches!(op, DispatchOp::SignalExternalWorkflow { .. })), true);
    }

    #[test]
    fn property_46_external_signal_resolved_event_and_removal(result in arb_external_signal_result()) {
        let now = fixed_now();
        let transition = kernel().apply(
            LoadedRun::Existing(with_pending_external_signal(make_open_state(now), 60)),
            Command::ExternalSignalResolved(ExternalSignalResolvedRequest {
                initiated_event_id: 60,
                result: result.clone(),
                now,
            }),
        ).unwrap();

        match (&transition.history_events[0].kind, result) {
            (HistoryEventKind::ExternalWorkflowExecutionSignaled { .. }, ExternalSignalResult::Signaled) => {}
            (HistoryEventKind::SignalExternalWorkflowExecutionFailed { cause, .. }, ExternalSignalResult::Failed { cause: expected }) => {
                prop_assert_eq!(cause, &expected);
            }
            other => panic!("unexpected event/result pair: {other:?}"),
        }
        prop_assert!(transition.next_state.pending_external_signals.is_empty());
    }

    #[test]
    fn property_47_external_cancel_resolved_event_and_removal(result in arb_external_cancel_result()) {
        let now = fixed_now();
        let transition = kernel().apply(
            LoadedRun::Existing(with_pending_external_cancel(make_open_state(now), 61)),
            Command::ExternalCancelResolved(ExternalCancelResolvedRequest {
                initiated_event_id: 61,
                result: result.clone(),
                now,
            }),
        ).unwrap();

        match (&transition.history_events[0].kind, result) {
            (HistoryEventKind::ExternalWorkflowExecutionCancelRequested { .. }, ExternalCancelResult::CancelRequested) => {}
            (HistoryEventKind::RequestCancelExternalWorkflowExecutionFailed { cause, .. }, ExternalCancelResult::Failed { cause: expected }) => {
                prop_assert_eq!(cause, &expected);
            }
            other => panic!("unexpected event/result pair: {other:?}"),
        }
        prop_assert!(transition.next_state.pending_external_cancels.is_empty());
    }

    #[test]
    fn property_50_no_dedup_for_resolution(
        signal_result in arb_external_signal_result(),
        cancel_result in arb_external_cancel_result(),
    ) {
        let now = fixed_now();
        let signal_transition = kernel().apply(
            LoadedRun::Existing(with_pending_external_signal(make_open_state(now), 70)),
            Command::ExternalSignalResolved(ExternalSignalResolvedRequest {
                initiated_event_id: 70,
                result: signal_result,
                now,
            }),
        ).unwrap();
        prop_assert!(signal_transition.request_dedupe_ops.is_empty());

        let cancel_transition = kernel().apply(
            LoadedRun::Existing(with_pending_external_cancel(make_open_state(now), 71)),
            Command::ExternalCancelResolved(ExternalCancelResolvedRequest {
                initiated_event_id: 71,
                result: cancel_result,
                now,
            }),
        ).unwrap();
        prop_assert!(cancel_transition.request_dedupe_ops.is_empty());
    }
}

#[test]
fn property_42_parent_close_policy_all_paths() {
    let now = fixed_now();
    for policy in [
        ParentClosePolicy::Terminate,
        ParentClosePolicy::RequestCancel,
        ParentClosePolicy::Abandon,
    ] {
        let direct_close = |command| {
            kernel()
                .apply(
                    LoadedRun::Existing(with_child(
                        make_open_state(now),
                        "child-1",
                        10,
                        policy,
                        true,
                    )),
                    command,
                )
                .unwrap()
        };

        let wf_close = |command| {
            let started = with_pending_wft(
                with_child(make_open_state(now), "child-1", 10, policy, true),
                31,
                Some(14),
                1,
            );
            kernel()
                .apply(
                    LoadedRun::Existing(started.clone()),
                    Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                        client_discards_speculative_with_events: false,
                        token: WorkflowTaskToken {
                            run_key: started.run_key,
                            logical_seq: LogicalTaskSeq(31),
                            started_event_id: 14,
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
                        commands: vec![command],
                        force_new_workflow_task: false,
                        delivered_update_ids: Vec::new(),
                        now,
                    }),
                )
                .unwrap()
        };

        let transitions = vec![
            direct_close(Command::Terminate(TerminateRequest {
                reason: "reason".into(),
                details: None,
                identity: "tester".into(),
                request: request_context("term", now),
                now,
            })),
            direct_close(Command::WorkflowExecutionTimedOut(
                WorkflowExecutionTimedOutRequest {
                    timeout_type: WorkflowTimeoutType::RunTimeout,
                    retry_state: RetryState::Timeout,
                    new_execution_run_id: None,
                    now,
                },
            )),
            wf_close(WorkflowCommand::CompleteWorkflow {
                result: payloads("done"),
            }),
            wf_close(WorkflowCommand::FailWorkflow {
                failure: payload("fail"),
            }),
            wf_close(WorkflowCommand::CancelWorkflow { details: None }),
            wf_close(WorkflowCommand::ContinueAsNew {
                header: None,
                new_run_id: RunId::new(),
                workflow_type: WorkflowType("next".into()),
                task_queue: TaskQueueName("queue".into()),
                input: payloads("input"),
                memo: memo_with("memo"),
                search_attributes: search_attrs_with("search"),
                workflow_execution_timeout: None,
                workflow_run_timeout: None,
                workflow_task_timeout: default_workflow_task_timeout(),
                retry_policy: None,
            }),
        ];

        for transition in transitions {
            assert!(transition.next_state.children.is_empty());
            let terminate_count = transition
                .dispatch_ops
                .iter()
                .filter(|op| matches!(op, DispatchOp::TerminateChild { .. }))
                .count();
            let cancel_count = transition
                .dispatch_ops
                .iter()
                .filter(|op| matches!(op, DispatchOp::CancelChild { .. }))
                .count();
            match policy {
                ParentClosePolicy::Terminate => {
                    assert_eq!(terminate_count, 1);
                    assert_eq!(cancel_count, 0);
                }
                ParentClosePolicy::RequestCancel => {
                    assert_eq!(terminate_count, 0);
                    assert_eq!(cancel_count, 1);
                }
                ParentClosePolicy::Abandon => {
                    assert_eq!(terminate_count, 0);
                    assert_eq!(cancel_count, 0);
                }
            }
        }
    }
}

proptest! {
    #[test]
    fn property_53_update_acceptance(req in arb_update_request(fixed_now())) {
        let transition = kernel().apply(
            LoadedRun::Existing(make_open_state(fixed_now())),
            Command::Update(req.clone()),
        ).unwrap();

        prop_assert_eq!(transition.request_dedupe_ops.len(), 1);
        // apply_update no longer emits UpdateAccepted — it only
        // tracks the update as admitted and schedules a WFT.
        prop_assert!(transition.next_state.admitted_updates.contains(&req.update_id));
        prop_assert!(!transition.next_state.pending_updates.contains_key(&req.update_id));
        // Feature: speculative-wft, Property P1 — with no pending WFT the
        // delivery task is SPECULATIVE: no events persist and the ids are
        // virtual (updateworkflow/api.go:171-186 @ v1.31.0).
        prop_assert!(transition.history_events.is_empty());
        let pending = transition.next_state.pending_workflow_task.as_ref().unwrap();
        prop_assert_eq!(pending.task_type, tokeira_kernel::WorkflowTaskType::Speculative);
        prop_assert_eq!(pending.scheduled_event_id, transition.next_state.last_event_id + 1);
    }

    #[test]
    fn property_54_update_wft_coalescing(req in arb_update_request(fixed_now()), with_wft in any::<bool>()) {
        let now = fixed_now();
        let state = if with_wft {
            with_pending_wft(make_open_state(now), 59, None, 0)
        } else {
            make_open_state(now)
        };
        let transition = kernel().apply(LoadedRun::Existing(state), Command::Update(req)).unwrap();
        let enqueued = transition.dispatch_ops.iter().filter(|op| matches!(op, DispatchOp::EnqueueWorkflowTask { .. })).count();
        if with_wft {
            prop_assert_eq!(enqueued, 0);
        } else {
            prop_assert_eq!(enqueued, 1);
        }
    }

    #[test]
    fn property_55_update_completion_and_rejection_remove_pending(
        completed_cmd in arb_update_completed_command(),
        rejected_cmd in arb_update_rejected_command(),
    ) {
        let now = fixed_now();
        let update_id = match &completed_cmd {
            WorkflowCommand::UpdateCompleted { update_id, .. } => update_id.clone(),
            _ => unreachable!(),
        };
        let started = with_pending_wft(with_pending_update(make_open_state(now), &update_id), 60, Some(20), 1);
        let token = WorkflowTaskToken {
            run_key: started.run_key,
            logical_seq: LogicalTaskSeq(60),
            started_event_id: 20,
            attempt: 1,
            shard_epoch: ShardEpoch::ZERO,
        };

        let completed = kernel().apply(
            LoadedRun::Existing(started.clone()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: token.clone(),
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands: vec![completed_cmd],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now,
            }),
        ).unwrap();
        prop_assert!(!completed.next_state.pending_updates.contains_key(&update_id));

        let rejected_update_id = match &rejected_cmd {
            WorkflowCommand::UpdateRejected { update_id, .. } => update_id.clone(),
            _ => unreachable!(),
        };
        let started = with_pending_wft(with_pending_update(make_open_state(now), &rejected_update_id), 60, Some(20), 1);
        let rejected = kernel().apply(
            LoadedRun::Existing(started),
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
                commands: vec![rejected_cmd],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now,
            }),
        ).unwrap();
        prop_assert!(!rejected.next_state.pending_updates.contains_key(&rejected_update_id));
    }

    #[test]
    fn property_56_protocol_message_bodies(
        input in arb_payloads(),
        result in arb_payloads(),
        failure in arb_failure_payload(),
    ) {
        let now = fixed_now();
        let base = with_pending_wft(make_open_state(now), 61, Some(21), 1);
        let accepted = kernel().apply(
            LoadedRun::Existing(base.clone()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: base.run_key,
                    logical_seq: LogicalTaskSeq(61),
                    started_event_id: 21,
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
                commands: vec![WorkflowCommand::ProtocolMessage {
                    message_id: "msg-1".into(),
                    body: UpdateProtocolBody::Accepted {
                        update_id: "update-1".into(),
                        update_name: "handler".into(),
                        input,
                        sequencing_event_id: 1,
                    },
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now,
            }),
        ).unwrap();
        prop_assert!(accepted.next_state.pending_updates.contains_key("update-1"));

        let started = with_pending_wft(with_pending_update(make_open_state(now), "update-1"), 62, Some(22), 1);
        let completed = kernel().apply(
            LoadedRun::Existing(started.clone()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: started.run_key,
                    logical_seq: LogicalTaskSeq(62),
                    started_event_id: 22,
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
                commands: vec![WorkflowCommand::ProtocolMessage {
                    message_id: "msg-2".into(),
                    body: UpdateProtocolBody::Completed {
                        update_id: "update-1".into(),
                        result,
                        failure: None,
                    },
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now,
            }),
        ).unwrap();
        prop_assert!(!completed.next_state.pending_updates.contains_key("update-1"));

        let started = with_pending_wft(with_pending_update(make_open_state(now), "update-1"), 63, Some(23), 1);
        let rejected = kernel().apply(
            LoadedRun::Existing(started.clone()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: started.run_key,
                    logical_seq: LogicalTaskSeq(63),
                    started_event_id: 23,
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
                commands: vec![WorkflowCommand::ProtocolMessage {
                    message_id: "msg-3".into(),
                    body: UpdateProtocolBody::Rejected {
                        update_id: "update-1".into(),
                        failure,
                    },
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now,
            }),
        ).unwrap();
        prop_assert!(!rejected.next_state.pending_updates.contains_key("update-1"));
    }
}

#[test]
fn property_57_close_clears_pending_updates() {
    let now = fixed_now();
    let direct_close = |command| {
        kernel()
            .apply(
                LoadedRun::Existing(with_pending_update(make_open_state(now), "update-1")),
                command,
            )
            .unwrap()
    };

    let wf_close = |command| {
        let started = with_pending_wft(
            with_pending_update(make_open_state(now), "update-1"),
            64,
            Some(24),
            1,
        );
        kernel()
            .apply(
                LoadedRun::Existing(started.clone()),
                Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                    client_discards_speculative_with_events: false,
                    token: WorkflowTaskToken {
                        run_key: started.run_key,
                        logical_seq: LogicalTaskSeq(64),
                        started_event_id: 24,
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
                    commands: vec![command],
                    force_new_workflow_task: false,
                    delivered_update_ids: Vec::new(),
                    now,
                }),
            )
            .unwrap()
    };

    let transitions = vec![
        direct_close(Command::Terminate(TerminateRequest {
            reason: "reason".into(),
            details: None,
            identity: "tester".into(),
            request: request_context("term", now),
            now,
        })),
        direct_close(Command::WorkflowExecutionTimedOut(
            WorkflowExecutionTimedOutRequest {
                timeout_type: WorkflowTimeoutType::RunTimeout,
                retry_state: RetryState::Timeout,
                new_execution_run_id: None,
                now,
            },
        )),
        wf_close(WorkflowCommand::CompleteWorkflow {
            result: payloads("done"),
        }),
        wf_close(WorkflowCommand::FailWorkflow {
            failure: payload("fail"),
        }),
        wf_close(WorkflowCommand::CancelWorkflow { details: None }),
        wf_close(WorkflowCommand::ContinueAsNew {
            header: None,
            new_run_id: RunId::new(),
            workflow_type: WorkflowType("next".into()),
            task_queue: TaskQueueName("queue".into()),
            input: payloads("input"),
            memo: memo_with("memo"),
            search_attributes: search_attrs_with("search"),
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: default_workflow_task_timeout(),
            retry_policy: None,
        }),
    ];

    for transition in transitions {
        assert!(transition.next_state.pending_updates.is_empty());
    }
}

proptest! {
    #[test]
    fn property_58_record_marker_event_field_pass_through(cmd in arb_record_marker_command()) {
        let now = fixed_now();
        let state = with_pending_wft(make_open_state(now), 80, Some(30), 1);
        let transition = kernel().apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(80),
                    started_event_id: 30,
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
                commands: vec![cmd.clone()],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now,
            }),
        ).unwrap();

        let marker = transition.history_events.iter().find_map(|event| match &event.kind {
            HistoryEventKind::MarkerRecorded { marker_name, details, failure, header, .. } => {
                Some((marker_name.clone(), details.clone(), failure.clone(), header.clone()))
            }
            _ => None,
        }).unwrap();

        match cmd {
            WorkflowCommand::RecordMarker { marker_name, details, failure, header } => {
                prop_assert_eq!(marker.0, marker_name);
                prop_assert_eq!(marker.1, details);
                prop_assert_eq!(marker.2, failure);
                prop_assert_eq!(marker.3, header);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn property_59_record_marker_is_pure_event_emission(cmd in arb_record_marker_command()) {
        let now = fixed_now();
        let state = with_pending_wft(make_open_state(now), 81, Some(31), 1);
        let transition = kernel().apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(81),
                    started_event_id: 31,
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
                commands: vec![cmd],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now,
            }),
        ).unwrap();

        prop_assert_eq!(transition.dispatch_ops.len(), 0);
        prop_assert_eq!(transition.projection_ops.len(), 0);
        prop_assert_eq!(transition.request_dedupe_ops.len(), 0);
        prop_assert!(transition.next_state.is_open());
        prop_assert_eq!(&transition.next_state.memo, &state.memo);
        prop_assert_eq!(&transition.next_state.search_attributes, &state.search_attributes);
        prop_assert_eq!(&transition.next_state.activities, &state.activities);
        prop_assert_eq!(&transition.next_state.timers, &state.timers);
        prop_assert_eq!(&transition.next_state.children, &state.children);
        prop_assert_eq!(
            &transition.next_state.pending_external_signals,
            &state.pending_external_signals
        );
        prop_assert_eq!(
            &transition.next_state.pending_external_cancels,
            &state.pending_external_cancels
        );
        prop_assert_eq!(&transition.next_state.pending_updates, &state.pending_updates);
        prop_assert_eq!(
            transition.next_state.versioning_override().cloned(),
            state.versioning_override().cloned()
        );
        prop_assert_eq!(
            &transition.next_state.completion_callbacks,
            &state.completion_callbacks
        );
    }

    #[test]
    fn property_60_update_execution_options_event_and_dedup(req in arb_update_execution_options_request(fixed_now())) {
        let now = fixed_now();
        let transition = kernel().apply(
            LoadedRun::Existing(make_open_state(now)),
            Command::UpdateExecutionOptions(req.clone()),
        ).unwrap();

        prop_assert_eq!(transition.request_dedupe_ops.len(), 1);
        prop_assert_eq!(transition.request_dedupe_ops[0].clone(), RequestDedupeOp { request_id: req.request.request_id.clone() });

        match &transition.history_events[0].kind {
            HistoryEventKind::WorkflowExecutionOptionsUpdated {
                versioning_override,
                completion_callbacks,
                attached_completion_callbacks,
                attached_links,
                attached_request_id,
            } => {
                prop_assert_eq!(versioning_override, &req.versioning_override);
                prop_assert_eq!(
                    completion_callbacks,
                    &stamp_callback_field_change(&req.completion_callbacks, req.now)
                );
                prop_assert_eq!(
                    attached_completion_callbacks,
                    &stamp_callbacks(req.attached_completion_callbacks.clone(), req.now)
                );
                prop_assert_eq!(attached_links, &req.attached_links);
                prop_assert_eq!(attached_request_id, &req.attached_request_id);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn property_61_update_execution_options_state_mutation(req in arb_update_execution_options_request(fixed_now()), callback_count in 0usize..3usize) {
        let now = fixed_now();
        let base = with_execution_options(make_open_state(now), callback_count);
        let transition = kernel().apply(
            LoadedRun::Existing(base.clone()),
            Command::UpdateExecutionOptions(req.clone()),
        ).unwrap();

        let expected_versioning_override = match req.versioning_override {
            FieldChange::Unchanged => base.versioning_override().cloned(),
            FieldChange::Set(versioning_override) => Some(versioning_override),
            FieldChange::Clear => None,
        };
        let expected_completion_callbacks = match req.completion_callbacks {
            FieldChange::Unchanged => base.completion_callbacks,
            FieldChange::Set(completion_callbacks) => stamp_callbacks(completion_callbacks, req.now),
            FieldChange::Clear => Vec::new(),
        };
        let expected_completion_callbacks = expected_completion_callbacks
            .into_iter()
            .chain(stamp_callbacks(req.attached_completion_callbacks, req.now))
            .collect::<Vec<_>>();

        prop_assert_eq!(transition.next_state.versioning_override().cloned(), expected_versioning_override);
        prop_assert_eq!(transition.next_state.completion_callbacks, expected_completion_callbacks);
    }

    #[test]
    fn property_62_update_execution_options_does_not_schedule_wft_or_close(
        req in arb_update_execution_options_request(fixed_now()),
        with_pending in any::<bool>(),
    ) {
        let now = fixed_now();
        let state = if with_pending {
            with_pending_wft(make_open_state(now), 82, None, 0)
        } else {
            make_open_state(now)
        };
        let pending = state.pending_workflow_task.clone();
        let transition = kernel().apply(
            LoadedRun::Existing(state),
            Command::UpdateExecutionOptions(req),
        ).unwrap();

        prop_assert_eq!(
            transition
                .dispatch_ops
                .iter()
                .all(|op| !matches!(op, DispatchOp::EnqueueWorkflowTask { .. })),
            true
        );
        prop_assert_eq!(transition.next_state.pending_workflow_task, pending);
        prop_assert_eq!(transition.next_state.status, ExecutionStatus::Running);
    }
}

#[test]
fn property_63_close_preserves_execution_options() {
    let now = fixed_now();
    let direct_close = |command| {
        kernel()
            .apply(
                LoadedRun::Existing(with_execution_options(make_open_state(now), 2)),
                command,
            )
            .unwrap()
    };

    let wf_close = |command| {
        let started = with_pending_wft(
            with_execution_options(make_open_state(now), 2),
            83,
            Some(32),
            1,
        );
        kernel()
            .apply(
                LoadedRun::Existing(started.clone()),
                Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                    client_discards_speculative_with_events: false,
                    token: WorkflowTaskToken {
                        run_key: started.run_key,
                        logical_seq: LogicalTaskSeq(83),
                        started_event_id: 32,
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
                    commands: vec![command],
                    force_new_workflow_task: false,
                    delivered_update_ids: Vec::new(),
                    now,
                }),
            )
            .unwrap()
    };

    let transitions = vec![
        direct_close(Command::Terminate(TerminateRequest {
            reason: "reason".into(),
            details: None,
            identity: "tester".into(),
            request: request_context("term-options", now),
            now,
        })),
        direct_close(Command::WorkflowExecutionTimedOut(
            WorkflowExecutionTimedOutRequest {
                timeout_type: WorkflowTimeoutType::RunTimeout,
                retry_state: RetryState::Timeout,
                new_execution_run_id: None,
                now,
            },
        )),
        wf_close(WorkflowCommand::CompleteWorkflow {
            result: payloads("done"),
        }),
        wf_close(WorkflowCommand::FailWorkflow {
            failure: payload("fail"),
        }),
        wf_close(WorkflowCommand::CancelWorkflow { details: None }),
        wf_close(WorkflowCommand::ContinueAsNew {
            header: None,
            new_run_id: RunId::new(),
            workflow_type: WorkflowType("next".into()),
            task_queue: TaskQueueName("queue".into()),
            input: payloads("input"),
            memo: memo_with("memo"),
            search_attributes: search_attrs_with("search"),
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: default_workflow_task_timeout(),
            retry_policy: None,
        }),
    ];

    for transition in transitions {
        assert_eq!(
            transition.next_state.versioning_override().cloned(),
            Some(VersioningOverride::AutoUpgrade)
        );
        let mut scheduled_callback = completion_callback();
        scheduled_callback.state = CallbackState::Scheduled;
        assert_eq!(
            transition.next_state.completion_callbacks,
            vec![scheduled_callback.clone(), scheduled_callback]
        );
        assert_eq!(
            transition
                .dispatch_ops
                .iter()
                .filter(|op| matches!(op, DispatchOp::DispatchCompletionCallback { .. }))
                .count(),
            2
        );
    }
}

// ---- nexus-async-completion Wave 1 property helpers (P2, P4) ----

/// One terminal close mechanism, paired with the `CallbackCompletionOutcome`
/// variant the kernel must derive for it (mirrors `GetNexusCompletion @ v1.31.0`).
#[derive(Clone, Debug)]
enum CloseKind {
    Completed(Vec<u8>),
    Failed(Vec<u8>),
    Canceled,
    ContinuedAsNew,
    Terminated,
    TimedOut,
}

fn arb_close_kind() -> impl Strategy<Value = CloseKind> {
    prop_oneof![
        prop::collection::vec(any::<u8>(), 0..16).prop_map(CloseKind::Completed),
        prop::collection::vec(any::<u8>(), 0..16).prop_map(CloseKind::Failed),
        Just(CloseKind::Canceled),
        Just(CloseKind::ContinuedAsNew),
        Just(CloseKind::Terminated),
        Just(CloseKind::TimedOut),
    ]
}

/// Drive an open run carrying a single Standby `WorkflowClosed` completion callback
/// to the terminal state described by `kind`, returning the resulting transition.
fn drive_close(kind: &CloseKind, now: OffsetDateTime) -> Transition {
    let mut state = with_pending_wft(make_open_state(now), 90, Some(40), 1);
    let mut callback = completion_callback();
    callback.registration_time = Some(now);
    state.completion_callbacks = vec![callback];

    let token = WorkflowTaskToken {
        run_key: state.run_key,
        logical_seq: LogicalTaskSeq(90),
        started_event_id: 40,
        attempt: 1,
        shard_epoch: ShardEpoch::ZERO,
    };
    let wft = |commands: Vec<WorkflowCommand>| {
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
            now,
        })
    };
    let command = match kind {
        CloseKind::Completed(bytes) => wft(vec![WorkflowCommand::CompleteWorkflow {
            result: Payloads(vec![Payload::new(bytes.clone())]),
        }]),
        CloseKind::Failed(bytes) => wft(vec![WorkflowCommand::FailWorkflow {
            failure: Payload::new(bytes.clone()),
        }]),
        CloseKind::Canceled => wft(vec![WorkflowCommand::CancelWorkflow { details: None }]),
        CloseKind::ContinuedAsNew => wft(vec![WorkflowCommand::ContinueAsNew {
            header: None,
            new_run_id: RunId::new(),
            workflow_type: WorkflowType("wf".into()),
            task_queue: TaskQueueName("queue".into()),
            input: payloads("can-input"),
            memo: memo_with("memo"),
            search_attributes: search_attrs_with("search"),
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: default_workflow_task_timeout(),
            retry_policy: None,
        }]),
        CloseKind::Terminated => Command::Terminate(TerminateRequest {
            reason: "terminated".into(),
            details: None,
            identity: "operator".into(),
            request: request_context("terminate-req", now),
            now,
        }),
        CloseKind::TimedOut => {
            Command::WorkflowExecutionTimedOut(WorkflowExecutionTimedOutRequest {
                timeout_type: WorkflowTimeoutType::RunTimeout,
                retry_state: RetryState::Timeout,
                new_execution_run_id: None,
                now,
            })
        }
    };
    kernel().apply(LoadedRun::Existing(state), command).unwrap()
}

/// All `DispatchCompletionCallback` outcomes carried on a transition.
fn dispatched_outcomes(transition: &Transition) -> Vec<CallbackCompletionOutcome> {
    transition
        .dispatch_ops
        .iter()
        .filter_map(|op| match op {
            DispatchOp::DispatchCompletionCallback { outcome, .. } => Some(outcome.clone()),
            _ => None,
        })
        .collect()
}

/// A closed run carrying a single non-terminal (`start_state`) completion callback,
/// the durable shape a `CompletionCallbackAttempted` targets.
fn closed_state_with_callback(
    start_state: CallbackState,
    start_attempt: u32,
    now: OffsetDateTime,
) -> WorkflowState {
    let mut state = make_open_state(now);
    state.status = ExecutionStatus::Completed;
    state.closed_at = Some(now);
    let mut callback = completion_callback();
    callback.state = start_state;
    callback.attempt = start_attempt;
    callback.registration_time = Some(now);
    state.completion_callbacks = vec![callback];
    state
}

fn arb_attempt_outcome() -> impl Strategy<Value = CallbackAttemptOutcome> {
    prop_oneof![
        Just(CallbackAttemptOutcome::Succeeded),
        (prop::collection::vec(any::<u8>(), 0..16), 1i64..3600).prop_map(|(bytes, secs)| {
            CallbackAttemptOutcome::RetryableFailure {
                failure: Payload::new(bytes),
                next_attempt_at: fixed_now() + Duration::seconds(secs),
            }
        }),
        prop::collection::vec(any::<u8>(), 0..16).prop_map(|bytes| {
            CallbackAttemptOutcome::NonRetryableFailure {
                failure: Payload::new(bytes),
            }
        }),
    ]
}

proptest! {
    #[test]
    fn property_completion_callbacks_schedule_once_on_terminal_close(
        standby_flags in prop::collection::vec(any::<bool>(), 0..5)
    ) {
        // Feature: api-conformance-start-fields, Property: terminal callbacks dispatch exactly once.
        // **Validates: Requirements 1.4, 3.4, 4.1**
        let now = fixed_now();
        let mut state = with_pending_wft(make_open_state(now), 90, Some(40), 1);
        state.completion_callbacks = standby_flags
            .iter()
            .map(|standby| {
                let mut callback = completion_callback();
                callback.registration_time = Some(now);
                if !standby {
                    callback.state = CallbackState::Scheduled;
                }
                callback
            })
            .collect();

        let transition = kernel().apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(90),
                    started_event_id: 40,
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
                now,
            }),
        ).unwrap();

        let callback_dispatches: Vec<_> = transition
            .dispatch_ops
            .iter()
            .filter_map(|op| match op {
                DispatchOp::DispatchCompletionCallback {
                    callback_index,
                    callback,
                    ..
                } => Some((*callback_index, callback.clone())),
                _ => None,
            })
            .collect();
        let expected_dispatch_count = standby_flags.iter().filter(|standby| **standby).count();
        prop_assert_eq!(callback_dispatches.len(), expected_dispatch_count);

        for (index, callback) in transition.next_state.completion_callbacks.iter().enumerate() {
            if standby_flags[index] {
                prop_assert_eq!(&callback.state, &CallbackState::Scheduled);
                prop_assert!(callback_dispatches.iter().any(
                    |(callback_index, dispatched)| *callback_index == index
                        && dispatched.state == CallbackState::Scheduled
                ));
            } else {
                prop_assert_eq!(&callback.state, &CallbackState::Scheduled);
                prop_assert!(!callback_dispatches
                    .iter()
                    .any(|(callback_index, _)| *callback_index == index));
            }
        }
    }

    /// Feature: nexus-async-completion, Property 2 (kernel half).
    /// A closing workflow carrying a Standby completion callback dispatches exactly
    /// one callback whose `outcome` matches the close kind — the variant the runtime
    /// maps to a `NexusResolution`. **Validates: Requirements 2.2, 2.3, 4.1, 4.2, 4.3**
    #[test]
    fn property_p2_close_kind_yields_matching_outcome(kind in arb_close_kind()) {
        let now = fixed_now();
        let transition = drive_close(&kind, now);
        let outcomes = dispatched_outcomes(&transition);
        prop_assert_eq!(outcomes.len(), 1);
        let expected = match &kind {
            CloseKind::Completed(bytes) => CallbackCompletionOutcome::Success {
                result: Some(Payload::new(bytes.clone())),
            },
            CloseKind::Failed(bytes) => CallbackCompletionOutcome::Failed {
                failure: Payload::new(bytes.clone()),
            },
            CloseKind::Canceled => CallbackCompletionOutcome::Canceled { details: None },
            CloseKind::ContinuedAsNew => CallbackCompletionOutcome::ContinuedAsNew,
            CloseKind::Terminated => CallbackCompletionOutcome::Terminated,
            CloseKind::TimedOut => CallbackCompletionOutcome::TimedOut,
        };
        prop_assert_eq!(&outcomes[0], &expected);
    }

    /// Feature: nexus-async-completion, Property 4.
    /// A delivery attempt against a non-terminal callback advances its lifecycle to
    /// exactly one well-formed state: `Succeeded`/`Failed` are terminal with no
    /// `next_attempt_at`; `RetryableFailure` backs off with `attempt` incremented and
    /// a future `next_attempt_at`. No history event or dispatch op is emitted, and the
    /// state-only commit still bumps `transition_seq`. **Validates: Requirements 2.1, 2.4, 2.5**
    #[test]
    fn property_p4_attempt_advances_lifecycle_well_formed(
        backing_off in any::<bool>(),
        start_attempt in 0u32..10,
        outcome in arb_attempt_outcome(),
    ) {
        let now = fixed_now();
        let start_state = if backing_off {
            CallbackState::BackingOff
        } else {
            CallbackState::Scheduled
        };
        let state = closed_state_with_callback(start_state, start_attempt, now);
        let expected_seq = state.transition_seq;
        let transition = kernel()
            .apply(
                LoadedRun::Existing(state),
                Command::CompletionCallbackAttempted(CompletionCallbackAttemptedRequest {
                    callback_index: 0,
                    outcome: outcome.clone(),
                    now,
                }),
            )
            .unwrap();

        // Lifecycle advances are mutable-state-only: no history, no dispatch.
        prop_assert!(transition.history_events.is_empty());
        prop_assert!(transition.dispatch_ops.is_empty());
        prop_assert_eq!(transition.expected_seq, expected_seq);
        prop_assert_eq!(transition.next_state.transition_seq, expected_seq.next());

        let callback = &transition.next_state.completion_callbacks[0];
        match outcome {
            CallbackAttemptOutcome::Succeeded => {
                prop_assert_eq!(&callback.state, &CallbackState::Succeeded);
                prop_assert_eq!(callback.attempt, start_attempt);
                prop_assert_eq!(callback.next_attempt_at, None);
            }
            CallbackAttemptOutcome::RetryableFailure { failure, next_attempt_at } => {
                prop_assert_eq!(&callback.state, &CallbackState::BackingOff);
                prop_assert_eq!(callback.attempt, start_attempt + 1);
                prop_assert_eq!(callback.next_attempt_at, Some(next_attempt_at));
                prop_assert!(next_attempt_at > now);
                prop_assert_eq!(callback.last_attempt_failure.as_ref(), Some(&failure));
            }
            CallbackAttemptOutcome::NonRetryableFailure { failure } => {
                prop_assert_eq!(&callback.state, &CallbackState::Failed);
                prop_assert_eq!(callback.next_attempt_at, None);
                prop_assert_eq!(callback.last_attempt_failure.as_ref(), Some(&failure));
            }
        }
    }

    /// Feature: nexus-async-completion, Property 4 (boundedness).
    /// A terminal callback (`Succeeded`/`Failed`) is never re-attempted: a late
    /// attempt is rejected rather than mutating durable state. **Validates: Requirement 2.5**
    #[test]
    fn property_p4_terminal_callback_never_reattempted(
        succeeded in any::<bool>(),
        outcome in arb_attempt_outcome(),
    ) {
        let now = fixed_now();
        let terminal = if succeeded {
            CallbackState::Succeeded
        } else {
            CallbackState::Failed
        };
        let state = closed_state_with_callback(terminal, 3, now);
        let reject = kernel()
            .apply(
                LoadedRun::Existing(state),
                Command::CompletionCallbackAttempted(CompletionCallbackAttemptedRequest {
                    callback_index: 0,
                    outcome,
                    now,
                }),
            )
            .unwrap_err();
        prop_assert_eq!(reject, tokeira_kernel::Reject::CompletionCallbackAlreadyTerminal(0));
    }

    #[test]
    fn property_64_schedule_nexus_operation_event_and_state_pass_through(cmd in arb_schedule_nexus_operation_command()) {
        let now = fixed_now();
        let state = with_pending_wft(make_open_state(now), 84, Some(33), 1);
        let transition = kernel().apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(84),
                    started_event_id: 33,
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
                commands: vec![cmd.clone()],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now,
            }),
        ).unwrap();

        match cmd {
            WorkflowCommand::ScheduleNexusOperation { operation_id, endpoint, service, operation, input, schedule_to_close_timeout, schedule_to_start_timeout: _, start_to_close_timeout: _ } => {
                let pending = transition.next_state.pending_nexus_operations.get(&operation_id).unwrap();
                prop_assert_eq!(&pending.endpoint, &endpoint);
                prop_assert_eq!(&pending.service, &service);
                prop_assert_eq!(&pending.operation, &operation);
                prop_assert!(!pending.started);
                prop_assert_eq!(
                    transition.dispatch_ops.iter().any(|op| matches!(op, DispatchOp::ScheduleNexusOperation { operation_id: id, endpoint: ep, service: svc, operation: opn, input: inp, schedule_to_close_timeout: sto, originator_run_key, scheduled_event_id, scheduled_at, .. } if id == &operation_id && ep == &endpoint && svc == &service && opn == &operation && inp == &input && sto == &schedule_to_close_timeout && originator_run_key == &state.run_key && *scheduled_event_id == pending.scheduled_event_id && *scheduled_at == now)),
                    true
                );
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn property_65_schedule_nexus_operation_duplicate_rejection(operation_id in arb_small_string()) {
        let now = fixed_now();
        let state = with_pending_nexus_operation(with_pending_wft(make_open_state(now), 85, Some(34), 1), &operation_id);
        let result = kernel().apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(85),
                    started_event_id: 34,
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
                commands: vec![WorkflowCommand::ScheduleNexusOperation {
                    operation_id: operation_id.clone(),
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
                now,
            }),
        );
        prop_assert_eq!(result, Err(tokeira_kernel::Reject::DuplicateNexusOperationId(operation_id)));
    }

    #[test]
    fn property_66_cancel_nexus_operation_event_and_dispatch(operation_id in arb_small_string()) {
        let now = fixed_now();
        let state = with_pending_nexus_operation(with_pending_wft(make_open_state(now), 86, Some(35), 1), &operation_id);
        let transition = kernel().apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: WorkflowTaskToken {
                    run_key: state.run_key,
                    logical_seq: LogicalTaskSeq(86),
                    started_event_id: 35,
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
                commands: vec![WorkflowCommand::CancelNexusOperation {
                    scheduled_event_id: 12,
                }],
                force_new_workflow_task: false,
                delivered_update_ids: Vec::new(),
                now,
            }),
        ).unwrap();
        prop_assert_eq!(
            matches!(transition.history_events[1].kind, HistoryEventKind::NexusOperationCancelRequested { scheduled_event_id: 12 }),
            true
        );
        prop_assert_eq!(
            transition.dispatch_ops.iter().any(|op| matches!(op, DispatchOp::CancelNexusOperation { scheduled_event_id: 12, originator_run_key, operation_id: id, endpoint, service } if originator_run_key == &state.run_key && id == &operation_id && endpoint == "endpoint" && service == "service")),
            true
        );
        prop_assert!(transition.next_state.pending_nexus_operations.contains_key(&operation_id));
    }

    #[test]
    fn property_67_started_resolution_is_non_terminal(operation_id in arb_small_string()) {
        let now = fixed_now();
        let transition = kernel().apply(
            LoadedRun::Existing(with_pending_nexus_operation(make_open_state(now), &operation_id)),
            Command::NexusOperationResolved(NexusOperationResolvedRequest {
                operation_id: operation_id.clone(),
                scheduled_event_id: 12,
                resolution: NexusResolution::Started { operation_token: String::new(), links: Vec::new() },
                now,
            }),
        ).unwrap();
        prop_assert_eq!(
            matches!(transition.history_events[0].kind, HistoryEventKind::NexusOperationStarted { scheduled_event_id: 12, .. }),
            true
        );
        prop_assert!(transition.next_state.pending_nexus_operations.contains_key(&operation_id));
        // Started is non-terminal (the operation stays pending) but IS a
        // workflow-task trigger, so it schedules a WFT to deliver the started
        // event to the worker (`StartedEventDefinition.IsWorkflowTaskTrigger()
        // -> true`, components/nexusoperations/events.go @ v1.31.0).
        prop_assert!(transition.next_state.pending_workflow_task.is_some());
        prop_assert_eq!(
            transition.dispatch_ops.iter().any(|op| matches!(op, DispatchOp::EnqueueWorkflowTask { .. })),
            true
        );
        prop_assert!(transition.request_dedupe_ops.is_empty());
    }

    #[test]
    fn property_68_terminal_resolution_removes_from_pending_and_schedules_wft(operation_id in arb_small_string(), resolution in prop_oneof![
        arb_payloads().prop_map(|result| NexusResolution::Completed { result, links: Vec::new() }),
        arb_payload().prop_map(|failure| NexusResolution::Failed { failure }),
        Just(NexusResolution::Canceled),
        Just(NexusResolution::TimedOut {
            timeout_type: NexusTimeoutType::ScheduleToClose,
        }),
    ]) {
        let now = fixed_now();
        let transition = kernel().apply(
            LoadedRun::Existing(with_pending_nexus_operation(make_open_state(now), &operation_id)),
            Command::NexusOperationResolved(NexusOperationResolvedRequest {
                operation_id: operation_id.clone(),
                scheduled_event_id: 12,
                resolution,
                now,
            }),
        ).unwrap();
        prop_assert!(!transition.next_state.pending_nexus_operations.contains_key(&operation_id));
        prop_assert!(transition.next_state.pending_workflow_task.is_some());
        prop_assert!(transition.request_dedupe_ops.is_empty());
    }

    #[test]
    fn property_69_nexus_resolution_rejection_paths(operation_id in arb_small_string()) {
        let now = fixed_now();
        let unknown = kernel().apply(
            LoadedRun::Existing(make_open_state(now)),
            Command::NexusOperationResolved(NexusOperationResolvedRequest {
                operation_id: operation_id.clone(),
                scheduled_event_id: 12,
                resolution: NexusResolution::Started { operation_token: String::new(), links: Vec::new() },
                now,
            }),
        );
        prop_assert_eq!(unknown, Err(tokeira_kernel::Reject::UnknownNexusOperation(operation_id.clone())));

        let stale = kernel().apply(
            LoadedRun::Existing(with_pending_nexus_operation(make_open_state(now), &operation_id)),
            Command::NexusOperationResolved(NexusOperationResolvedRequest {
                operation_id: operation_id.clone(),
                scheduled_event_id: 99,
                resolution: NexusResolution::Started { operation_token: String::new(), links: Vec::new() },
                now,
            }),
        );
        prop_assert_eq!(stale, Err(tokeira_kernel::Reject::StaleNexusResolution {
            operation_id,
            expected_scheduled_event_id: 12,
        }));
    }
}

#[test]
fn property_70_close_clears_pending_nexus_operations_without_dispatch_ops() {
    let now = fixed_now();
    let direct_close = |command| {
        kernel()
            .apply(
                LoadedRun::Existing(with_pending_nexus_operation(make_open_state(now), "op-1")),
                command,
            )
            .unwrap()
    };

    let wf_close = |command| {
        let started = with_pending_wft(
            with_pending_nexus_operation(make_open_state(now), "op-1"),
            87,
            Some(36),
            1,
        );
        kernel()
            .apply(
                LoadedRun::Existing(started.clone()),
                Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                    client_discards_speculative_with_events: false,
                    token: WorkflowTaskToken {
                        run_key: started.run_key,
                        logical_seq: LogicalTaskSeq(87),
                        started_event_id: 36,
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
                    commands: vec![command],
                    force_new_workflow_task: false,
                    delivered_update_ids: Vec::new(),
                    now,
                }),
            )
            .unwrap()
    };

    let transitions = vec![
        direct_close(Command::Terminate(TerminateRequest {
            reason: "reason".into(),
            details: None,
            identity: "tester".into(),
            request: request_context("term-nexus", now),
            now,
        })),
        direct_close(Command::WorkflowExecutionTimedOut(
            WorkflowExecutionTimedOutRequest {
                timeout_type: WorkflowTimeoutType::RunTimeout,
                retry_state: RetryState::Timeout,
                new_execution_run_id: None,
                now,
            },
        )),
        wf_close(WorkflowCommand::CompleteWorkflow {
            result: payloads("done"),
        }),
        wf_close(WorkflowCommand::FailWorkflow {
            failure: payload("fail"),
        }),
        wf_close(WorkflowCommand::CancelWorkflow { details: None }),
        wf_close(WorkflowCommand::ContinueAsNew {
            header: None,
            new_run_id: RunId::new(),
            workflow_type: WorkflowType("next".into()),
            task_queue: TaskQueueName("queue".into()),
            input: payloads("input"),
            memo: memo_with("memo"),
            search_attributes: search_attrs_with("search"),
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: default_workflow_task_timeout(),
            retry_policy: None,
        }),
    ];

    for transition in transitions {
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
}

// ─── Feature: kernel-event-buffering (Phase 1) ───
//
// Buffered-event model + terminate force-close ordering, ground-truthed to
// v1.31.0 (`bufferEvent` event_store.go:263; `failWorkflowTask` util.go:26;
// `TerminateWorkflow` util.go:115). Spec: .kiro/specs/kernel-event-buffering.

fn signal_request(name: &str, now: OffsetDateTime) -> SignalRequest {
    SignalRequest {
        signal_name: name.into(),
        input: payloads("signal-input"),
        header: None,
        links: Vec::new(),
        request: request_context(&format!("req-{name}"), now),
        now,
    }
}

proptest! {
    // Feature: kernel-event-buffering, Property 1
    // Signal during a started WFT buffers, not appends: no history event, no
    // consumed event id, no new WFT dispatch; the dedupe op still lands at
    // admission. (Req 2.1.1, 2.1.3, 2.2, 6.1.1)
    #[test]
    fn property_buffering_1_signal_during_started_wft_buffers(input in arb_payloads()) {
        let now = fixed_now();
        let state = with_pending_wft(make_open_state(now), 30, Some(13), 1);
        let last_event_id = state.last_event_id;
        let mut req = signal_request("buffered", now);
        req.input = input;
        let transition = kernel()
            .apply(LoadedRun::Existing(state), Command::Signal(req))
            .unwrap();
        prop_assert!(transition.history_events.is_empty());
        prop_assert_eq!(transition.next_state.last_event_id, last_event_id);
        prop_assert_eq!(transition.next_state.buffered_events.len(), 1);
        let buffered_is_signal = matches!(
            transition.next_state.buffered_events[0].kind,
            HistoryEventKind::WorkflowExecutionSignaled { .. }
        );
        prop_assert!(buffered_is_signal);
        prop_assert_eq!(transition.request_dedupe_ops.len(), 1);
        let enqueues_wft = transition
            .dispatch_ops
            .iter()
            .any(|op| matches!(op, DispatchOp::EnqueueWorkflowTask { .. }));
        prop_assert!(!enqueues_wft);
        // At-most-one-WFT preserved (Req 6.2.1).
        prop_assert!(transition.next_state.pending_workflow_task.is_some());
    }

    // Feature: kernel-event-buffering, Property 2
    // Signal without a started WFT appends immediately: exactly one
    // WorkflowExecutionSignaled, nothing buffered. Covers both no-pending-WFT
    // and scheduled-but-not-started. (Req 2.1.2)
    #[test]
    fn property_buffering_2_signal_without_started_wft_appends(
        input in arb_payloads(),
        scheduled_not_started in any::<bool>(),
    ) {
        let now = fixed_now();
        let state = if scheduled_not_started {
            with_pending_wft(make_open_state(now), 30, None, 1)
        } else {
            make_open_state(now)
        };
        let mut req = signal_request("immediate", now);
        req.input = input;
        let transition = kernel()
            .apply(LoadedRun::Existing(state), Command::Signal(req))
            .unwrap();
        let signaled = transition
            .history_events
            .iter()
            .filter(|event| {
                matches!(event.kind, HistoryEventKind::WorkflowExecutionSignaled { .. })
            })
            .count();
        prop_assert_eq!(signaled, 1);
        prop_assert!(transition.next_state.buffered_events.is_empty());
    }

    // Feature: kernel-event-buffering, Property 3
    // Flush on WFT completion preserves admission order and id contiguity:
    // N buffered signals flush after WorkflowTaskCompleted in admission
    // order with contiguous ids, the buffer empties, and a follow-up WFT is
    // scheduled. (Req 3.1, 6.1.2)
    #[test]
    fn property_buffering_3_flush_on_completion_order_and_contiguity(count in 1usize..5) {
        let now = fixed_now();
        let mut state = with_pending_wft(make_open_state(now), 30, Some(13), 1);
        let kernel = kernel();
        // Buffer `count` signals through the real Signal path.
        for index in 0..count {
            let transition = kernel
                .apply(
                    LoadedRun::Existing(state),
                    Command::Signal(signal_request(&format!("sig-{index}"), now)),
                )
                .unwrap();
            state = transition.next_state;
        }
        prop_assert_eq!(state.buffered_events.len(), count);

        let run_key = state.run_key;
        let req = WorkflowTaskCompletedRequest {
            client_discards_speculative_with_events: false,
            token: WorkflowTaskToken {
                run_key,
                logical_seq: LogicalTaskSeq(30),
                started_event_id: 13,
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
            commands: Vec::new(),
            force_new_workflow_task: false,
            delivered_update_ids: Vec::new(),
            now,
        };
        let transition = kernel
            .apply(LoadedRun::Existing(state), Command::WorkflowTaskCompleted(req))
            .unwrap();

        // Events: WorkflowTaskCompleted, then the flushed signals in
        // admission order, then the follow-up WorkflowTaskScheduled.
        let first_is_completed = matches!(
            transition.history_events[0].kind,
            HistoryEventKind::WorkflowTaskCompleted { .. }
        );
        prop_assert!(first_is_completed);
        for index in 0..count {
            match &transition.history_events[1 + index].kind {
                HistoryEventKind::WorkflowExecutionSignaled { signal_name, .. } => {
                    prop_assert_eq!(signal_name, &format!("sig-{index}"));
                }
                other => panic!("expected flushed signal at {index}, got {other:?}"),
            }
        }
        let follow_up_is_scheduled = matches!(
            transition.history_events[1 + count].kind,
            HistoryEventKind::WorkflowTaskScheduled { .. }
        );
        prop_assert!(follow_up_is_scheduled);
        // Contiguous ids across close event + flushed events (Req 6.1.2).
        for pair in transition.history_events.windows(2) {
            prop_assert_eq!(pair[1].event_id, pair[0].event_id + 1);
        }
        prop_assert!(transition.next_state.buffered_events.is_empty());
        prop_assert!(transition.next_state.pending_workflow_task.is_some());
    }

    // Feature: kernel-event-buffering, Property 4
    // Terminate force-close ordering: started WFT + one buffered signal
    // terminates as WorkflowTaskFailed(ForceCloseCommand),
    // WorkflowExecutionSignaled, WorkflowExecutionTerminated, contiguous,
    // status Terminated. (Req 4.1)
    #[test]
    fn property_buffering_4_terminate_force_close_ordering(req in arb_terminate_request(fixed_now())) {
        let now = fixed_now();
        let state = with_pending_wft(make_open_state(now), 30, Some(13), 1);
        let kernel = kernel();
        let buffered = kernel
            .apply(
                LoadedRun::Existing(state),
                Command::Signal(signal_request("buffered", now)),
            )
            .unwrap()
            .next_state;
        let transition = kernel
            .apply(LoadedRun::Existing(buffered), Command::Terminate(req))
            .unwrap();

        prop_assert_eq!(transition.history_events.len(), 3);
        match &transition.history_events[0].kind {
            HistoryEventKind::WorkflowTaskFailed { failure_cause, .. } => {
                prop_assert_eq!(failure_cause, &WorkflowTaskFailedCause::ForceCloseCommand);
            }
            other => panic!("expected force-close WorkflowTaskFailed, got {other:?}"),
        }
        let second_is_signal = matches!(
            transition.history_events[1].kind,
            HistoryEventKind::WorkflowExecutionSignaled { .. }
        );
        prop_assert!(second_is_signal);
        let third_is_terminated = matches!(
            transition.history_events[2].kind,
            HistoryEventKind::WorkflowExecutionTerminated { .. }
        );
        prop_assert!(third_is_terminated);
        for pair in transition.history_events.windows(2) {
            prop_assert_eq!(pair[1].event_id, pair[0].event_id + 1);
        }
        prop_assert_eq!(transition.next_state.status, ExecutionStatus::Terminated);
        prop_assert!(transition.next_state.pending_workflow_task.is_none());
    }

    // Feature: kernel-event-buffering, Property 5
    // Terminal cleanliness: closing transitions leave no buffered events —
    // terminate flushes before the terminal event; a worker close command
    // overtaking a buffered signal drops it, matching v1.31.0
    // (`FlushBufferToCurrentBatch` workflowFinished branch,
    // event_store.go:139). (Req 6.3)
    #[test]
    fn property_buffering_5_closed_runs_carry_no_buffered_events(close_via_terminate in any::<bool>()) {
        let now = fixed_now();
        let state = with_pending_wft(make_open_state(now), 30, Some(13), 1);
        let kernel = kernel();
        let buffered = kernel
            .apply(
                LoadedRun::Existing(state),
                Command::Signal(signal_request("buffered", now)),
            )
            .unwrap()
            .next_state;
        let run_key = buffered.run_key;

        let transition = if close_via_terminate {
            kernel
                .apply(
                    LoadedRun::Existing(buffered),
                    Command::Terminate(TerminateRequest {
                        reason: "p5".into(),
                        details: None,
                        identity: "tester".into(),
                        request: request_context("p5-terminate", now),
                        now,
                    }),
                )
                .unwrap()
        } else {
            // A worker CANNOT close past buffered events — the close command
            // is an UnhandledCommand (Tier 1.6; `hasBufferedEventsOrMessages`
            // close guards @ v1.31.0). The run stays open with its buffer;
            // only force-closes (terminate branch above) drop it.
            let rejected = kernel.apply(
                LoadedRun::Existing(buffered.clone()),
                Command::WorkflowTaskCompleted(WorkflowTaskCompletedRequest {
                    client_discards_speculative_with_events: false,
                    token: WorkflowTaskToken {
                        run_key,
                        logical_seq: LogicalTaskSeq(30),
                        started_event_id: 13,
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
                    now,
                }),
            );
            prop_assert_eq!(
                rejected,
                Err(Reject::InvalidCommandAttributes {
                    cause: WorkflowTaskFailedCause::UnhandledCommand,
                    message: None,
                })
            );
            // The runtime then fails the WFT; the failure flushes the buffer
            // onto a fresh attempt-1 task, after which a clean close drops
            // nothing. Model that here to keep the closed-run invariant.
            let failed = kernel
                .apply(
                    LoadedRun::Existing(buffered),
                    Command::WorkflowTaskFailed(tokeira_kernel::WorkflowTaskFailedRequest {
                        logical_seq: LogicalTaskSeq(30),
                        started_event_id: 13,
                        failure_cause: WorkflowTaskFailedCause::UnhandledCommand,
                        failure_details: None,
                        worker_identity: WorkerIdentity("worker".into()),
                        now,
                    }),
                )
                .unwrap();
            prop_assert!(failed.next_state.buffered_events.is_empty());
            kernel
                .apply(
                    LoadedRun::Existing(failed.next_state),
                    Command::Terminate(TerminateRequest {
                        reason: "p5-after-fail".into(),
                        details: None,
                        identity: "tester".into(),
                        request: request_context("p5-terminate-2", now),
                        now,
                    }),
                )
                .unwrap()
        };
        prop_assert!(!transition.next_state.status.is_open());
        prop_assert!(transition.next_state.buffered_events.is_empty());
    }

    // Feature: kernel-event-buffering, Req 1.1.4 — buffered events survive a
    // serde round-trip without loss.
    #[test]
    fn property_buffering_serde_round_trip(count in 0usize..4) {
        let now = fixed_now();
        let mut state = with_pending_wft(make_open_state(now), 30, Some(13), 1);
        let kernel = kernel();
        for index in 0..count {
            state = kernel
                .apply(
                    LoadedRun::Existing(state),
                    Command::Signal(signal_request(&format!("rt-{index}"), now)),
                )
                .unwrap()
                .next_state;
        }
        let encoded = serde_json::to_vec(&state).unwrap();
        let decoded: WorkflowState = serde_json::from_slice(&encoded).unwrap();
        prop_assert_eq!(decoded, state);
    }
}

// ── workflow-retry-chain: FailWorkflow retry-continuation recording ──
//
// These drive a started WFT to completion with a single FailWorkflow command and
// the runtime-supplied RetryContinuation, asserting the kernel records the
// decision on WorkflowExecutionFailed (Req 1.2). They are example-based single
// transitions: the retry *evaluation* the continuation stands in for is a runtime
// concern (wall-clock/policy dependent), out of the pure kernel.

fn fail_workflow_completion_request(state: &WorkflowState) -> WorkflowTaskCompletedRequest {
    WorkflowTaskCompletedRequest {
        client_discards_speculative_with_events: false,
        token: WorkflowTaskToken {
            run_key: state.run_key,
            logical_seq: LogicalTaskSeq(30),
            started_event_id: 13,
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
            failure: payload("boom"),
        }],
        force_new_workflow_task: false,
        delivered_update_ids: Vec::new(),
        now: fixed_now(),
    }
}

fn fail_with_retry_continuation(
    state: &WorkflowState,
    retry_continuation: RetryContinuation,
) -> Transition {
    kernel()
        .apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskCompletedWithRetry {
                request: fail_workflow_completion_request(state),
                retry_continuation,
            },
        )
        .unwrap()
}

fn failed_event_outcome(transition: &Transition) -> Option<(RetryState, Option<RunId>)> {
    transition
        .history_events
        .iter()
        .find_map(|event| match &event.kind {
            HistoryEventKind::WorkflowExecutionFailed {
                retry_state,
                new_execution_run_id,
                ..
            } => Some((retry_state.clone(), *new_execution_run_id)),
            _ => None,
        })
}

#[test]
fn retry_continuation_links_successor() {
    // Feature: workflow-retry-chain, Property 1: a retry-eligible FailWorkflow
    // records retry_state=InProgress and new_execution_run_id=Some(successor).
    let state = with_pending_wft(make_open_state(fixed_now()), 30, Some(13), 1);
    let successor = RunId::new();
    let transition = fail_with_retry_continuation(
        &state,
        RetryContinuation::Retry {
            new_run_id: successor,
        },
    );
    assert_eq!(
        failed_event_outcome(&transition),
        Some((RetryState::InProgress, Some(successor)))
    );
    assert_eq!(transition.next_state.status, ExecutionStatus::Failed);
}

#[test]
fn retry_continuation_terminal_max_attempts_has_no_successor() {
    // Feature: workflow-retry-chain, Property 2: Terminal(MaximumAttemptsReached)
    // records that retry_state and no successor.
    let state = with_pending_wft(make_open_state(fixed_now()), 30, Some(13), 1);
    let transition = fail_with_retry_continuation(
        &state,
        RetryContinuation::Terminal {
            retry_state: RetryState::MaximumAttemptsReached,
        },
    );
    assert_eq!(
        failed_event_outcome(&transition),
        Some((RetryState::MaximumAttemptsReached, None))
    );
}

#[test]
fn retry_continuation_terminal_non_retryable_has_no_successor() {
    // Feature: workflow-retry-chain, Property 3: Terminal(NonRetryableFailure)
    // records that retry_state and no successor.
    let state = with_pending_wft(make_open_state(fixed_now()), 30, Some(13), 1);
    let transition = fail_with_retry_continuation(
        &state,
        RetryContinuation::Terminal {
            retry_state: RetryState::NonRetryableFailure,
        },
    );
    assert_eq!(
        failed_event_outcome(&transition),
        Some((RetryState::NonRetryableFailure, None))
    );
}

#[test]
fn fail_workflow_without_retry_policy_is_terminal() {
    // Feature: workflow-retry-chain, Property 4: a run with no retry policy fails
    // terminally with RetryPolicyNotSet and no successor (behaviour unchanged from
    // before the retry chain existed; no continuation is supplied).
    let mut state = with_pending_wft(make_open_state(fixed_now()), 30, Some(13), 1);
    state.retry_policy = None;
    let transition = kernel()
        .apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskCompleted(fail_workflow_completion_request(&state)),
        )
        .unwrap();
    assert_eq!(
        failed_event_outcome(&transition),
        Some((RetryState::RetryPolicyNotSet, None))
    );
}

#[test]
fn workflow_execution_failed_new_run_id_round_trips() {
    // Feature: workflow-retry-chain, Property 6: WorkflowExecutionFailed round-trips
    // with and without new_execution_run_id, and a record written before the field
    // existed (key absent) decodes to None via #[serde(default)].
    for new_execution_run_id in [None, Some(RunId::new())] {
        let event = HistoryEventKind::WorkflowExecutionFailed {
            workflow_task_completed_event_id: 4,
            failure: payload("boom"),
            retry_state: RetryState::InProgress,
            attempt: 1,
            new_execution_run_id,
        };
        let encoded = serde_json::to_vec(&event).unwrap();
        let decoded: HistoryEventKind = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, event);
    }

    let event = HistoryEventKind::WorkflowExecutionFailed {
        workflow_task_completed_event_id: 4,
        failure: payload("boom"),
        retry_state: RetryState::InProgress,
        attempt: 1,
        new_execution_run_id: None,
    };
    let mut value = serde_json::to_value(&event).unwrap();
    value["WorkflowExecutionFailed"]
        .as_object_mut()
        .unwrap()
        .remove("new_execution_run_id");
    let decoded: HistoryEventKind = serde_json::from_value(value).unwrap();
    assert_eq!(decoded, event);
}

#[test]
fn wft_failed_with_buffered_events_schedules_fresh_normal_task() {
    // Feature: transient-wft, Property 1: a WFT that fails after events buffered
    // during it flushes them and schedules a fresh NORMAL (attempt-1) task with a
    // real WorkflowTaskScheduled — not a re-dispatch of the failed task
    // (workflow_task_state_machine.go:329-334 @ v1.31.0).
    let now = fixed_now();
    let state = with_pending_wft(make_open_state(now), 30, Some(13), 1);
    let kernel = kernel();

    // Buffer a signal while the WFT is started.
    let buffered = kernel
        .apply(
            LoadedRun::Existing(state),
            Command::Signal(signal_request("sig", now)),
        )
        .unwrap()
        .next_state;
    assert_eq!(buffered.buffered_events.len(), 1);

    // Fail the WFT — the buffered signal flushes and a fresh normal task schedules.
    let transition = kernel
        .apply(
            LoadedRun::Existing(buffered),
            Command::WorkflowTaskFailed(WorkflowTaskFailedRequest {
                logical_seq: LogicalTaskSeq(30),
                started_event_id: 13,
                failure_cause: WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure,
                failure_details: None,
                worker_identity: WorkerIdentity("worker".into()),
                now,
            }),
        )
        .unwrap();

    assert_eq!(transition.history_events.len(), 3);
    assert!(matches!(
        transition.history_events[0].kind,
        HistoryEventKind::WorkflowTaskFailed { .. }
    ));
    assert!(matches!(
        transition.history_events[1].kind,
        HistoryEventKind::WorkflowExecutionSignaled { .. }
    ));
    assert!(matches!(
        transition.history_events[2].kind,
        HistoryEventKind::WorkflowTaskScheduled { .. }
    ));
    assert_eq!(transition.next_state.workflow_task_attempt, 1);
    let pending = transition
        .next_state
        .pending_workflow_task
        .expect("a fresh workflow task must be scheduled");
    assert_eq!(pending.attempt, 1);
    assert!(pending.started_event_id.is_none());
    assert!(transition.next_state.buffered_events.is_empty());
}
