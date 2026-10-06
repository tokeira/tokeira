//! `workflow-task-command-limits`: the kernel checks what the edge measured on
//! each command of a workflow task completion where Temporal v1.31.0 checks
//! it, measures the run's merged search attributes and memo, and terminates
//! the workflow for a command over a size limit.
// Integration test: unwrap is idiomatic in test code (root AGENTS.md §1).
#![allow(clippy::unwrap_used)]

use std::collections::{BTreeMap, HashSet};

use proptest::prelude::*;
use prost::Message;
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    BasicKernel, Command, CommandPayloadSizes, ContinueAsNewVersioningBehavior, FieldChange,
    LoadedRun, MemoPatch, ParentClosePolicy, PayloadSize, PendingNexusOperation,
    PendingWorkflowTask, ProtocolMessageSize, Reject, SearchAttributeSizes, SearchAttributesPatch,
    SignalRequest, Transition, UpdateProtocolBody, UpsertedFieldSizes, WorkflowCommand,
    WorkflowIdReusePolicy, WorkflowState, WorkflowTaskCompletedRequest,
    WorkflowTaskCompletionLimits, WorkflowTaskFailedCause, WorkflowTaskFailedRequest,
    event::HistoryEventKind,
    kernel::Kernel,
    payload_size::{
        memo_encoded_len, merged_memo_encoded_len, merged_search_attribute_sizes,
        payload_encoded_len, search_attribute_payload_size,
    },
};
use tokeira_proto::conversions::common::{
    memo_from_domain, payload_from_domain, search_attr_value_to_payload,
};
use tokeira_types::{
    ExecutionStatus, ExternalPayloadDetail, LogicalTaskSeq, Memo, NamespaceId, Payload, Payloads,
    RequestContext, RunId, RunKey, SearchAttrValue, SearchAttributes, ShardEpoch, TaskQueueName,
    TransitionSeq, WorkerIdentity, WorkflowId, WorkflowTaskToken, WorkflowType,
};

const BLOB: usize = 2 * 1024 * 1024;
const MEMO: usize = 2 * 1024 * 1024;

fn now() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()
}

fn payloads(data: &str) -> Payloads {
    Payloads(vec![Payload::new(data.as_bytes().to_vec())])
}

fn open_state() -> WorkflowState {
    let now = now();
    WorkflowState {
        completed_update_count: 0,
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
        external_payload_count: 0,
        external_payload_size_bytes: 0,
        next_workflow_task_seq: LogicalTaskSeq(4),
        pending_workflow_task: None,
        previous_started_event_id: 0,
        workflow_task_attempt: 1,
        workflow_task_attempts_since_last_success: 0,
        last_workflow_task_problem: None,
        sticky: None,
        pause_info: None,
        cancel_requested: false,
        wft_stamp: 0,
        memo: Memo(BTreeMap::new()),
        search_attributes: SearchAttributes(BTreeMap::new()),
        workflow_execution_timeout: Some(Duration::minutes(5)),
        workflow_run_timeout: Some(Duration::minutes(1)),
        workflow_task_timeout: Duration::seconds(10),
        retry_policy: None,
        attempt: 1,
        first_execution_run_id: Some(RunId::new()),
        original_execution_run_id: None,
        reset_run_id: None,
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
        admitted_updates: HashSet::new(),
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
        request_id_infos: BTreeMap::new(),
        buffered_events: Vec::new(),
        auto_reset_points: Vec::new(),
    }
}

/// A run whose attempt-1 workflow task is started (Scheduled 13, Started 14).
fn started(mut state: WorkflowState) -> WorkflowState {
    state.pending_workflow_task = Some(PendingWorkflowTask {
        advice: Default::default(),
        task_type: tokeira_kernel::WorkflowTaskType::Normal,
        schedule_to_start_deadline: None,
        target_worker_deployment_version_changed: false,
        target_version_changed_enabled: false,
        target_deployment_version: None,
        logical_seq: LogicalTaskSeq(30),
        scheduled_event_id: 13,
        scheduled_at: state.started_at,
        started_event_id: Some(14),
        started_at: Some(state.started_at + Duration::seconds(1)),
        attempt: 1,
    });
    state.next_workflow_task_seq = LogicalTaskSeq(31);
    state
}

fn completion(
    state: &WorkflowState,
    commands: Vec<WorkflowCommand>,
    command_sizes: Vec<CommandPayloadSizes>,
) -> WorkflowTaskCompletedRequest {
    let pending = state.pending_workflow_task.as_ref().unwrap();
    WorkflowTaskCompletedRequest {
        token: WorkflowTaskToken {
            run_key: state.run_key,
            logical_seq: pending.logical_seq,
            started_event_id: pending.started_event_id.unwrap(),
            attempt: pending.attempt,
            shard_epoch: ShardEpoch::ZERO,
        },
        client_discards_speculative_with_events: false,
        identity: WorkerIdentity("worker".into()),
        sdk_metadata: None,
        metering_metadata: None,
        worker_version: None,
        versioning_behavior: Default::default(),
        deployment_version: None,
        worker_deployment_name: None,
        sticky: None,
        commands,
        command_sizes,
        force_new_workflow_task: false,
        limits: WorkflowTaskCompletionLimits::default(),
        delivered_update_ids: Vec::new(),
        request: RequestContext::unattributed(OffsetDateTime::UNIX_EPOCH),
        now: now(),
    }
}

fn apply(
    state: &WorkflowState,
    commands: Vec<WorkflowCommand>,
    command_sizes: Vec<CommandPayloadSizes>,
) -> Result<Transition, Reject> {
    BasicKernel.apply(
        LoadedRun::Existing(state.clone()),
        Command::WorkflowTaskCompleted(completion(state, commands, command_sizes)),
    )
}

fn schedule_activity(index: usize) -> WorkflowCommand {
    WorkflowCommand::ScheduleActivity {
        activity_id: format!("activity-{index}"),
        activity_type: "work".into(),
        task_queue: TaskQueueName(String::new()),
        input: payloads("input"),
        header: None,
        request_eager_execution: false,
        retry_policy: None,
        deployment: None,
        build_id: None,
        schedule_to_close_timeout: None,
        schedule_to_start_timeout: None,
        start_to_close_timeout: Some(Duration::seconds(10)),
        heartbeat_timeout: None,
        priority: None,
    }
}

fn record_marker() -> WorkflowCommand {
    WorkflowCommand::RecordMarker {
        marker_name: "marker".into(),
        details: BTreeMap::new(),
        failure: None,
        header: None,
    }
}

fn signal_external(index: usize) -> WorkflowCommand {
    WorkflowCommand::SignalExternalWorkflowExecution {
        target_namespace_id: NamespaceId::new(),
        target_namespace: Some("default".into()),
        target_workflow_id: WorkflowId(format!("target-{index}")),
        target_run_id: None,
        signal_name: "signal".into(),
        input: payloads("input"),
        header: None,
        control: String::new(),
    }
}

fn start_child(index: usize) -> WorkflowCommand {
    WorkflowCommand::StartChildWorkflow {
        child_workflow_id: WorkflowId(format!("child-{index}")),
        namespace_id: NamespaceId::new(),
        namespace: Some("default".into()),
        workflow_type: WorkflowType("child-type".into()),
        task_queue: TaskQueueName(String::new()),
        input: payloads("input"),
        header: None,
        memo: Memo(BTreeMap::new()),
        search_attributes: SearchAttributes(BTreeMap::new()),
        workflow_execution_timeout: None,
        workflow_run_timeout: None,
        workflow_task_timeout: Duration::seconds(10),
        retry_policy: None,
        cron_schedule: None,
        parent_close_policy: ParentClosePolicy::Abandon,
        reuse_policy: WorkflowIdReusePolicy::default(),
        priority: None,
    }
}

fn upsert(index: usize) -> WorkflowCommand {
    WorkflowCommand::UpsertSearchAttributesPatch(SearchAttributesPatch(BTreeMap::from([(
        format!("Key{index}"),
        FieldChange::Set(SearchAttrValue::Keyword("value".into())),
    )])))
}

fn modify_memo(index: usize) -> WorkflowCommand {
    WorkflowCommand::UpsertMemoPatch(MemoPatch(BTreeMap::from([(
        format!("memo-{index}"),
        FieldChange::Set(Payload::new(b"value".to_vec())),
    )])))
}

fn schedule_nexus(index: usize, endpoint: &str) -> WorkflowCommand {
    WorkflowCommand::ScheduleNexusOperation {
        operation_id: format!("operation-{index}"),
        endpoint: endpoint.into(),
        service: "service".into(),
        operation: "operation".into(),
        input: payloads("input"),
        schedule_to_close_timeout: None,
        schedule_to_start_timeout: None,
        start_to_close_timeout: None,
    }
}

fn continue_as_new() -> WorkflowCommand {
    WorkflowCommand::ContinueAsNew {
        new_run_id: RunId::new(),
        workflow_type: WorkflowType(String::new()),
        task_queue: TaskQueueName(String::new()),
        input: payloads("input"),
        memo: Memo(BTreeMap::new()),
        search_attributes: SearchAttributes(BTreeMap::new()),
        workflow_execution_timeout: None,
        workflow_run_timeout: None,
        workflow_task_timeout: Duration::seconds(10),
        retry_policy: None,
        header: None,
        initial_versioning_behavior: ContinueAsNewVersioningBehavior::default(),
        successor_versioning_info: None,
    }
}

fn payload_size(over: bool) -> Option<usize> {
    Some(if over { BLOB + 1 } else { BLOB })
}

/// One small set field, as the edge measures it.
fn small_upsert(fields_over: bool, key: &str) -> CommandPayloadSizes {
    CommandPayloadSizes {
        upserted_fields: Some(UpsertedFieldSizes {
            fields_size: if fields_over { BLOB + 1 } else { BLOB },
            set_fields: BTreeMap::from([(
                key.to_owned(),
                PayloadSize {
                    data: 7,
                    encoded: 9,
                },
            )]),
        }),
        ..CommandPayloadSizes::default()
    }
}

fn start_sizes(over: [bool; 4]) -> CommandPayloadSizes {
    CommandPayloadSizes {
        payload: payload_size(over[0]),
        memo: Some(if over[1] { MEMO + 1 } else { MEMO }),
        search_attributes: Some(SearchAttributeSizes {
            keys: if over[2] { 101 } else { 100 },
            value_sizes: BTreeMap::from([("Key".to_owned(), if over[3] { 2049 } else { 2048 })]),
            total: 2100,
        }),
        ..CommandPayloadSizes::default()
    }
}

/// What a command may carry, each field within or one byte over its limit.
#[derive(Clone, Copy, Debug)]
enum Kind {
    ScheduleActivity,
    RecordMarker,
    SignalExternal,
    StartChild,
    Upsert,
    ModifyMemo,
    Nexus { system: bool },
}

#[derive(Clone, Copy, Debug)]
enum Close {
    Complete,
    Fail,
    ContinueAsNew,
}

/// The first failure v1.31.0 reports for a command, or none.
#[derive(Clone, Debug, PartialEq)]
enum Expected {
    Fail(WorkflowTaskFailedCause, String),
    Terminate(WorkflowTaskFailedCause, String),
}

fn build(
    kind: Kind,
    over: [bool; 4],
    index: usize,
) -> (WorkflowCommand, CommandPayloadSizes, Option<Expected>) {
    use WorkflowTaskFailedCause as C;
    let terminate = |cause: C, message: &str| Some(Expected::Terminate(cause, message.to_owned()));
    match kind {
        Kind::ScheduleActivity => (
            schedule_activity(index),
            CommandPayloadSizes {
                payload: payload_size(over[0]),
                ..Default::default()
            },
            if over[0] {
                terminate(
                    C::BadScheduleActivityAttributes,
                    "ScheduleActivityTaskCommandAttributes.Input exceeds size limit.",
                )
            } else {
                None
            },
        ),
        Kind::RecordMarker => (
            record_marker(),
            CommandPayloadSizes {
                payload: payload_size(over[0]),
                ..Default::default()
            },
            if over[0] {
                terminate(
                    C::BadRecordMarkerAttributes,
                    "RecordMarkerCommandAttributes.Details exceeds size limit.",
                )
            } else {
                None
            },
        ),
        Kind::SignalExternal => (
            signal_external(index),
            CommandPayloadSizes {
                payload: payload_size(over[0]),
                ..Default::default()
            },
            if over[0] {
                terminate(
                    C::BadSignalWorkflowExecutionAttributes,
                    "SignalExternalWorkflowExecutionCommandAttributes.Input exceeds size limit.",
                )
            } else {
                None
            },
        ),
        Kind::StartChild => {
            let expected = if over[2] {
                Some(Expected::Fail(
                    C::BadSearchAttributes,
                    format!(
                        "invalid SearchAttributes on StartChildWorkflowCommand: number of search \
                         attributes 101 exceeds limit 100. WorkflowId=child-{index} \
                         WorkflowType=child-type Namespace=default"
                    ),
                ))
            } else if over[0] {
                terminate(
                    C::BadStartChildExecutionAttributes,
                    "StartChildWorkflowExecutionCommandAttributes. Input exceeds size limit.",
                )
            } else if over[1] {
                terminate(
                    C::BadStartChildExecutionAttributes,
                    "StartChildWorkflowExecutionCommandAttributes.Memo exceeds size limit.",
                )
            } else if over[3] {
                terminate(
                    C::BadStartChildExecutionAttributes,
                    "search attribute Key value size 2049 exceeds size limit 2048",
                )
            } else {
                None
            };
            (start_child(index), start_sizes(over), expected)
        }
        Kind::Upsert => (
            upsert(index),
            small_upsert(over[0], &format!("Key{index}")),
            if over[0] {
                terminate(
                    C::BadSearchAttributes,
                    "UpsertWorkflowSearchAttributesCommandAttributes exceeds size limit.",
                )
            } else {
                None
            },
        ),
        Kind::ModifyMemo => (
            modify_memo(index),
            small_upsert(over[0], &format!("memo-{index}")),
            if over[0] {
                terminate(
                    C::BadModifyWorkflowPropertiesAttributes,
                    "ModifyWorkflowPropertiesCommandAttributes exceeds size limit.",
                )
            } else {
                None
            },
        ),
        Kind::Nexus { system } => (
            schedule_nexus(
                index,
                if system {
                    "__temporal_system"
                } else {
                    "endpoint"
                },
            ),
            // The edge doesn't measure the system endpoint's input.
            CommandPayloadSizes {
                payload: if system { None } else { payload_size(over[0]) },
                ..Default::default()
            },
            if over[0] && !system {
                terminate(
                    C::BadScheduleNexusOperationAttributes,
                    "ScheduleNexusOperationCommandAttributes.Input exceeds size limit",
                )
            } else {
                None
            },
        ),
    }
}

fn build_close(
    close: Close,
    over: [bool; 4],
) -> (WorkflowCommand, CommandPayloadSizes, Option<Expected>) {
    use WorkflowTaskFailedCause as C;
    let terminate = |cause: C, message: &str| Some(Expected::Terminate(cause, message.to_owned()));
    match close {
        Close::Complete => (
            WorkflowCommand::CompleteWorkflow {
                result: payloads("result"),
            },
            CommandPayloadSizes {
                payload: payload_size(over[0]),
                ..Default::default()
            },
            if over[0] {
                // v1.31.0's own choice of cause.
                terminate(
                    C::BadScheduleActivityAttributes,
                    "CompleteWorkflowExecutionCommandAttributes.Result exceeds size limit.",
                )
            } else {
                None
            },
        ),
        Close::Fail => (
            WorkflowCommand::FailWorkflow {
                failure: Payload::new(b"failure".to_vec()),
            },
            CommandPayloadSizes {
                payload: payload_size(over[0]),
                ..Default::default()
            },
            if over[0] {
                terminate(
                    C::BadFailWorkflowExecutionAttributes,
                    "FailWorkflowExecutionCommandAttributes.Failure exceeds size limit.",
                )
            } else {
                None
            },
        ),
        Close::ContinueAsNew => {
            let expected = if over[2] {
                Some(Expected::Fail(
                    C::BadSearchAttributes,
                    "invalid SearchAttributes on ContinueAsNewWorkflowExecutionCommand: number of \
                     search attributes 101 exceeds limit 100. WorkflowType=wf \
                     TaskQueue=name:\"queue\" kind:TASK_QUEUE_KIND_NORMAL"
                        .to_owned(),
                ))
            } else if over[0] {
                terminate(
                    C::BadContinueAsNewAttributes,
                    "ContinueAsNewWorkflowExecutionCommandAttributes. Input exceeds size limit.",
                )
            } else if over[1] {
                terminate(
                    C::BadContinueAsNewAttributes,
                    "ContinueAsNewWorkflowExecutionCommandAttributes. Memo exceeds size limit.",
                )
            } else if over[3] {
                terminate(
                    C::BadContinueAsNewAttributes,
                    "search attribute Key value size 2049 exceeds size limit 2048",
                )
            } else {
                None
            };
            (continue_as_new(), start_sizes(over), expected)
        }
    }
}

fn arb_kind() -> impl Strategy<Value = Kind> {
    prop_oneof![
        Just(Kind::ScheduleActivity),
        Just(Kind::RecordMarker),
        Just(Kind::SignalExternal),
        Just(Kind::StartChild),
        Just(Kind::Upsert),
        Just(Kind::ModifyMemo),
        any::<bool>().prop_map(|system| Kind::Nexus { system }),
    ]
}

fn arb_close() -> impl Strategy<Value = Option<Close>> {
    prop_oneof![
        Just(None),
        Just(Some(Close::Complete)),
        Just(Some(Close::Fail)),
        Just(Some(Close::ContinueAsNew)),
    ]
}

/// Mostly within the limits, so that later commands get their turn.
fn arb_over() -> impl Strategy<Value = [bool; 4]> {
    prop::array::uniform4(prop::bool::weighted(0.15))
}

// Feature: workflow-task-command-limits, Property 1: Command limits match v1.31.0's
proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn property_command_limits_match_v1_31(
        commands in prop::collection::vec((arb_kind(), arb_over()), 0..7),
        close in (arb_close(), arb_over()),
    ) {
        let state = started(open_state());
        let mut built: Vec<_> = commands
            .iter()
            .enumerate()
            .map(|(index, (kind, over))| build(*kind, *over, index))
            .collect();
        if let (Some(close), over) = close {
            built.push(build_close(close, over));
        }
        let expected = built.iter().find_map(|(_, _, expected)| expected.clone());
        let (commands, sizes): (Vec<_>, Vec<_>) =
            built.into_iter().map(|(command, sizes, _)| (command, sizes)).unzip();
        let outcome = apply(&state, commands, sizes);
        match expected {
            None => prop_assert!(outcome.is_ok(), "expected the completion to apply: {outcome:?}"),
            Some(Expected::Fail(cause, message)) => prop_assert_eq!(
                outcome.unwrap_err(),
                Reject::InvalidCommandAttributes { cause, message: Some(message) }
            ),
            Some(Expected::Terminate(cause, message)) => prop_assert_eq!(
                outcome.unwrap_err(),
                Reject::CommandExceedsLimit { cause, message }
            ),
        }
    }
}

#[test]
fn unmeasured_commands_check_no_size() {
    let mut state = started(open_state());
    // Stored search attributes far over the total limit.
    for index in 0..30 {
        state.search_attributes.0.insert(
            format!("Key{index:02}"),
            SearchAttrValue::Text("x".repeat(2000)),
        );
    }
    assert!(apply(&state, vec![upsert(99), schedule_activity(1)], Vec::new()).is_ok());
}

#[test]
fn the_merged_search_attributes_are_checked_after_an_upsert() {
    let mut state = started(open_state());
    for index in 0..20 {
        state.search_attributes.0.insert(
            format!("Key{index:02}"),
            SearchAttrValue::Text("x".repeat(2000)),
        );
    }
    let (_, total) = merged_search_attribute_sizes(
        &state.search_attributes,
        &SearchAttributesPatch::default(),
        &BTreeMap::new(),
    );
    assert!(total < 40 * 1024);
    // Twenty 2,000-byte values fit; a twenty-first takes the map over 40 KiB.
    let patch = SearchAttributesPatch(BTreeMap::from([(
        "Key20".to_owned(),
        FieldChange::Set(SearchAttrValue::Text("x".repeat(1998))),
    )]));
    let sizes = CommandPayloadSizes {
        upserted_fields: Some(UpsertedFieldSizes {
            fields_size: 5 + 2000,
            set_fields: BTreeMap::from([(
                "Key20".to_owned(),
                PayloadSize {
                    data: 2000,
                    encoded: 2000 + 3 + 22,
                },
            )]),
        }),
        ..CommandPayloadSizes::default()
    };
    let rejected = apply(
        &state,
        vec![WorkflowCommand::UpsertSearchAttributesPatch(patch.clone())],
        vec![sizes.clone()],
    )
    .unwrap_err();
    let (_, merged_total) = merged_search_attribute_sizes(
        &state.search_attributes,
        &patch,
        &sizes.upserted_fields.as_ref().unwrap().set_fields,
    );
    assert!(merged_total > 40 * 1024);
    assert_eq!(
        rejected,
        Reject::CommandExceedsLimit {
            cause: WorkflowTaskFailedCause::BadSearchAttributes,
            message: format!(
                "total size of search attributes {merged_total} exceeds size limit 40960"
            ),
        }
    );
    // Removing a key instead brings the map back within the limit.
    let clear = SearchAttributesPatch(BTreeMap::from([("Key00".to_owned(), FieldChange::Clear)]));
    let clear_sizes = CommandPayloadSizes {
        upserted_fields: Some(UpsertedFieldSizes {
            fields_size: 9,
            set_fields: BTreeMap::new(),
        }),
        ..CommandPayloadSizes::default()
    };
    assert!(
        apply(
            &state,
            vec![WorkflowCommand::UpsertSearchAttributesPatch(clear)],
            vec![clear_sizes]
        )
        .is_ok()
    );
}

#[test]
fn the_merged_memo_is_checked_after_modify_workflow_properties() {
    let mut state = started(open_state());
    state
        .memo
        .0
        .insert("big".into(), Payload::new(vec![b'x'; MEMO - 64]));
    let patch = MemoPatch(BTreeMap::from([(
        "more".to_owned(),
        FieldChange::Set(Payload::new(vec![b'y'; 100])),
    )]));
    let sizes = CommandPayloadSizes {
        upserted_fields: Some(UpsertedFieldSizes {
            fields_size: 4 + 100,
            set_fields: BTreeMap::from([(
                "more".to_owned(),
                PayloadSize {
                    data: 100,
                    encoded: 102,
                },
            )]),
        }),
        ..CommandPayloadSizes::default()
    };
    assert_eq!(
        apply(
            &state,
            vec![WorkflowCommand::UpsertMemoPatch(patch)],
            vec![sizes]
        )
        .unwrap_err(),
        Reject::CommandExceedsLimit {
            cause: WorkflowTaskFailedCause::BadModifyWorkflowPropertiesAttributes,
            message: "ModifyWorkflowPropertiesCommandAttributes. Memo exceeds size limit.".into(),
        }
    );
}

#[test]
fn an_oversized_protocol_message_terminates_before_it_is_processed() {
    let state = started(open_state());
    let command = WorkflowCommand::ProtocolMessage {
        message_id: "message".into(),
        // Unknown to the run: processing it would fail the task instead.
        body: UpdateProtocolBody::Rejected {
            update_id: "unknown".into(),
            failure: Payload::new(b"failure".to_vec()),
        },
    };
    let sizes = CommandPayloadSizes {
        protocol_message: Some(ProtocolMessageSize {
            body: BLOB + 1,
            type_name: "temporal.api.update.v1.Rejection".into(),
        }),
        ..CommandPayloadSizes::default()
    };
    assert_eq!(
        apply(&state, vec![command], vec![sizes]).unwrap_err(),
        Reject::CommandExceedsLimit {
            cause: WorkflowTaskFailedCause::BadUpdateWorkflowExecutionMessage,
            message: "Message type temporal.api.update.v1.Rejection exceeds size limit.".into(),
        }
    );
}

fn pending_nexus_operation(index: usize) -> PendingNexusOperation {
    PendingNexusOperation {
        operation_id: format!("pending-{index}"),
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
        cancellation: None,
    }
}

#[test]
fn the_thirty_first_pending_nexus_operation_fails_the_task() {
    let mut state = started(open_state());
    for index in 0..29 {
        state
            .pending_nexus_operations
            .insert(format!("pending-{index}"), pending_nexus_operation(index));
    }
    let expected = Reject::InvalidCommandAttributes {
        cause: WorkflowTaskFailedCause::PendingNexusOperationsLimitExceeded,
        message: Some(
            "workflow has reached the pending nexus operation limit of 30 for this namespace"
                .into(),
        ),
    };
    // The 30th fits; one scheduled earlier in the same completion counts.
    assert!(apply(&state, vec![schedule_nexus(1, "endpoint")], Vec::new()).is_ok());
    assert_eq!(
        apply(
            &state,
            vec![schedule_nexus(1, "endpoint"), schedule_nexus(2, "endpoint")],
            Vec::new()
        )
        .unwrap_err(),
        expected
    );
}

#[test]
fn a_terminating_failure_records_the_failure_the_buffer_and_the_termination() {
    let state = started(open_state());
    // A signal while the task is started is buffered.
    let buffered = BasicKernel
        .apply(
            LoadedRun::Existing(state),
            Command::Signal(SignalRequest {
                signal_name: "signal".into(),
                input: Payloads::default(),
                header: None,
                links: Vec::new(),
                request: RequestContext::unattributed(OffsetDateTime::UNIX_EPOCH),
                now: now(),
            }),
        )
        .unwrap()
        .next_state;
    assert_eq!(buffered.buffered_events.len(), 1);
    let reason =
        "BadScheduleActivityAttributes: ScheduleActivityTaskCommandAttributes.Input exceeds size limit."
            .to_owned();
    let transition = BasicKernel
        .apply(
            LoadedRun::Existing(buffered),
            Command::WorkflowTaskFailed(WorkflowTaskFailedRequest {
                logical_seq: LogicalTaskSeq(30),
                started_event_id: 14,
                failure_cause: WorkflowTaskFailedCause::BadScheduleActivityAttributes,
                failure_details: Some(Payload::new(b"server failure".to_vec())),
                worker_identity: WorkerIdentity("worker".into()),
                request: RequestContext::unattributed(OffsetDateTime::UNIX_EPOCH),
                now: now(),
                reset_reapply: Vec::new(),
                history_size_bytes: 0,
                advice_policy: tokeira_kernel::ContinueAsNewAdvicePolicy::V1_31_0,
                terminate_reason: Some(reason.clone()),
            }),
        )
        .unwrap();
    let kinds: Vec<_> = transition
        .history_events
        .iter()
        .map(|event| &event.kind)
        .collect();
    assert_eq!(kinds.len(), 3, "{kinds:?}");
    assert!(matches!(
        kinds[0],
        HistoryEventKind::WorkflowTaskFailed { failure_cause: WorkflowTaskFailedCause::BadScheduleActivityAttributes, identity, .. }
            if identity.0 == "worker"
    ));
    assert!(matches!(
        kinds[1],
        HistoryEventKind::WorkflowExecutionSignaled { .. }
    ));
    assert!(matches!(
        kinds[2],
        HistoryEventKind::WorkflowExecutionTerminated { reason: recorded, details: None, identity, .. }
            if *recorded == reason && identity == "history-service"
    ));
    assert_eq!(transition.next_state.status, ExecutionStatus::Terminated);
    assert!(transition.next_state.pending_workflow_task.is_none());
    assert!(transition.next_state.buffered_events.is_empty());
}

fn arb_text() -> impl Strategy<Value = String> {
    "[a-zA-Z0-9 é€😀<>&\"\\\\]{0,40}"
}

fn arb_domain_payload() -> impl Strategy<Value = Payload> {
    (
        prop::collection::btree_map(arb_text(), arb_text(), 0..4),
        prop::collection::vec(any::<u8>(), 0..300),
        prop::collection::vec(any::<i64>(), 0..3),
    )
        .prop_map(|(metadata, data, external)| Payload {
            metadata,
            data,
            external_payloads: external
                .into_iter()
                .map(|size_bytes| ExternalPayloadDetail { size_bytes })
                .collect(),
        })
}

fn arb_search_attr_value() -> impl Strategy<Value = SearchAttrValue> {
    prop_oneof![
        arb_text().prop_map(SearchAttrValue::Keyword),
        arb_text().prop_map(SearchAttrValue::Text),
        prop::collection::vec(arb_text(), 0..4).prop_map(SearchAttrValue::KeywordList),
        any::<i64>().prop_map(SearchAttrValue::Int),
        (-1.0e12f64..1.0e12).prop_map(SearchAttrValue::Double),
        any::<bool>().prop_map(SearchAttrValue::Bool),
        (0i64..4_000_000_000, 0u32..1_000_000_000).prop_map(|(seconds, nanos)| {
            SearchAttrValue::Datetime(
                OffsetDateTime::from_unix_timestamp(seconds).unwrap()
                    + Duration::nanoseconds(i64::from(nanos)),
            )
        }),
    ]
}

/// The payload Tokeira sends for a stored value, without its `type` metadata.
fn wire_payload(value: &SearchAttrValue) -> tokeira_proto::common::Payload {
    let mut payload = search_attr_value_to_payload(value);
    payload.metadata.remove("type");
    payload
}

// Feature: workflow-task-command-limits, Property 3: Sizes match v1.31.0's measurements
proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn property_stored_sizes_match_protobuf(
        payload in arb_domain_payload(),
        memo in prop::collection::btree_map(arb_text(), arb_domain_payload(), 0..5),
        value in arb_search_attr_value(),
    ) {
        prop_assert_eq!(payload_encoded_len(&payload), payload_from_domain(&payload).encoded_len());
        let memo = Memo(memo);
        prop_assert_eq!(memo_encoded_len(&memo), memo_from_domain(&memo).encoded_len());
        let wire = wire_payload(&value);
        let size = search_attribute_payload_size(&value);
        prop_assert_eq!(size.data, wire.data.len());
        prop_assert_eq!(size.encoded, wire.encoded_len());
    }

    #[test]
    fn property_merged_sizes_match_protobuf(
        stored in prop::collection::btree_map("[A-Za-z]{1,6}", arb_search_attr_value(), 0..6),
        changes in prop::collection::btree_map(
            "[A-Za-z]{1,6}",
            prop::option::of(arb_search_attr_value()),
            0..6,
        ),
        stored_memo in prop::collection::btree_map("[a-z]{1,6}", arb_domain_payload(), 0..5),
        memo_changes in prop::collection::btree_map(
            "[a-z]{1,6}",
            prop::option::of(arb_domain_payload()),
            0..5,
        ),
    ) {
        // Search attributes: set fields measured as the edge measures the
        // payloads the SDK sent, stored values as Tokeira encodes them.
        let patch = SearchAttributesPatch(
            changes
                .iter()
                .map(|(key, value)| {
                    let change = value.clone().map_or(FieldChange::Clear, FieldChange::Set);
                    (key.clone(), change)
                })
                .collect(),
        );
        let set_fields = changes
            .iter()
            .filter_map(|(key, value)| {
                value.as_ref().map(|value| {
                    let wire = wire_payload(value);
                    (key.clone(), PayloadSize { data: wire.data.len(), encoded: wire.encoded_len() })
                })
            })
            .collect();
        let mut merged: BTreeMap<String, SearchAttrValue> = stored.clone();
        for (key, value) in &changes {
            match value {
                Some(value) => { merged.insert(key.clone(), value.clone()); }
                None => { merged.remove(key); }
            }
        }
        let expected = tokeira_proto::common::SearchAttributes {
            indexed_fields: merged.iter().map(|(key, value)| (key.clone(), wire_payload(value))).collect(),
        };
        let (values, total) = merged_search_attribute_sizes(
            &SearchAttributes(stored),
            &patch,
            &set_fields,
        );
        prop_assert_eq!(total, expected.encoded_len());
        let expected_values: BTreeMap<String, usize> = expected
            .indexed_fields
            .iter()
            .map(|(key, payload)| (key.clone(), payload.data.len()))
            .collect();
        prop_assert_eq!(values, expected_values);

        // Memo.
        let memo_patch = MemoPatch(
            memo_changes
                .iter()
                .map(|(key, value)| {
                    let change = value.clone().map_or(FieldChange::Clear, FieldChange::Set);
                    (key.clone(), change)
                })
                .collect(),
        );
        let mut merged_memo = stored_memo.clone();
        for (key, value) in &memo_changes {
            match value {
                Some(value) => { merged_memo.insert(key.clone(), value.clone()); }
                None => { merged_memo.remove(key); }
            }
        }
        prop_assert_eq!(
            merged_memo_encoded_len(&Memo(stored_memo), &memo_patch, &BTreeMap::new()),
            memo_from_domain(&Memo(merged_memo)).encoded_len()
        );
    }
}
