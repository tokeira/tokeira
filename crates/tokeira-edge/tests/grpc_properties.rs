// Property tests construct prost messages with `..Default::default()` for
// forward-compat against upstream proto field additions.
#![allow(clippy::needless_update)]

use std::{collections::BTreeMap, time::Duration};

use http::header::HeaderValue;
use proptest::prelude::*;
use prost::Message;
use time::OffsetDateTime;
use tokeira_edge::{
    errors::EdgeError,
    grpc::{
        errors::proto_conversion_status,
        metadata::metadata_to_header_map,
        translate::{
            completed_response_to_proto, count_request_to_edge, count_response_to_proto,
            describe_request_to_edge, describe_response_to_proto, list_request_to_edge,
            list_response_to_proto, poll_request_to_edge, poll_response_to_proto,
            proto_command_to_workflow_command, signal_request_to_edge, start_request_to_edge,
            start_response_to_proto, workflow_command_to_proto,
        },
    },
    translate::{
        CountWorkflowExecutionsRequest, CountWorkflowExecutionsResponse,
        DescribeWorkflowExecutionRequest, ExecutionConfigDescription, GroupCount, Link,
        ListWorkflowExecutionsRequest, ListWorkflowExecutionsResponse, PendingActivityDescription,
        PendingChildDescription, PendingWorkflowTaskDescription, PollWorkflowTaskQueueRequest,
        PollWorkflowTaskQueueResponse, RespondWorkflowTaskCompletedResponse,
        SignalWorkflowExecutionRequest, StartWorkflowExecutionRequest,
        StartWorkflowExecutionResponse, WorkflowExecutionDescription, WorkflowExecutionSummary,
        WorkflowTaskPayloadDto,
    },
};
use tokeira_kernel::{
    WorkflowCommand, WorkflowIdConflictPolicy, WorkflowIdReusePolicy, state::ParentClosePolicy,
};
use tokeira_proto::{
    conversions::common::{
        failure_to_payload, headers_from_domain, memo_from_domain, payload_to_failure,
        payloads_from_domain, search_attributes_from_domain,
    },
    enums::WorkflowExecutionStatus,
    public::temporal::api::failure::v1 as failure_proto,
    workflowservice,
};
use tokeira_types::{
    ExecutionStatus, Headers, Memo, Payload, Payloads, RunId, RunKey, SearchAttrValue,
    SearchAttributes, WorkflowId,
};
use tonic::{Code, metadata::MetadataMap};
use uuid::Uuid;

proptest! {
    #[test]
    fn property_start_request_roundtrip(edge in arb_start_request()) {
        let proto = start_request_to_proto(&edge);
        let roundtrip = start_request_to_edge(proto).unwrap();
        prop_assert_eq!(roundtrip, edge);
    }

    #[test]
    fn property_start_request_preserves_request_eager_execution(edge in arb_start_request()) {
        let expected = edge.request_eager_execution;
        let proto = start_request_to_proto(&edge);
        let roundtrip = start_request_to_edge(proto).unwrap();
        prop_assert_eq!(roundtrip.request_eager_execution, expected);
    }

    #[test]
    fn property_signal_request_roundtrip(edge in arb_signal_request()) {
        let proto = signal_request_to_proto(&edge);
        let roundtrip = signal_request_to_edge(proto).unwrap();
        prop_assert_eq!(roundtrip, edge);
    }

    #[test]
    fn property_poll_request_roundtrip(edge in arb_poll_request()) {
        let proto = poll_request_to_proto(&edge);
        let roundtrip = poll_request_to_edge(proto).unwrap();
        prop_assert_eq!(roundtrip, edge);
    }

    #[test]
    fn property_describe_request_roundtrip(edge in arb_describe_request()) {
        let proto = workflowservice::DescribeWorkflowExecutionRequest {
            namespace: edge.namespace.clone(),
            execution: Some(tokeira_proto::common::WorkflowExecution {
                workflow_id: edge.workflow_id.clone(),
                run_id: edge.run_id.clone().unwrap_or_default(),
                ..Default::default()
            }),
        };
        let roundtrip = describe_request_to_edge(proto).unwrap();
        prop_assert_eq!(roundtrip, edge);
    }

    #[test]
    fn property_list_request_roundtrip(edge in arb_list_request()) {
        let proto = workflowservice::ListWorkflowExecutionsRequest {
            namespace: edge.namespace.clone(),
            query: edge.query.clone().unwrap_or_default(),
            page_size: edge.page_size as i32,
            next_page_token: edge.next_page_token.clone().map(String::into_bytes).unwrap_or_default(),
        };
        let roundtrip = list_request_to_edge(proto).unwrap();
        prop_assert_eq!(roundtrip, edge);
    }

    #[test]
    fn property_count_request_roundtrip(edge in arb_count_request()) {
        let proto = workflowservice::CountWorkflowExecutionsRequest {
            namespace: edge.namespace.clone(),
            query: edge.query.clone().unwrap_or_default(),
        };
        let roundtrip = count_request_to_edge(proto).unwrap();
        prop_assert_eq!(roundtrip, edge);
    }

    #[test]
    fn property_start_response_projection(edge in arb_start_response()) {
        let proto = start_response_to_proto(edge.clone(), "ns".to_string(), "wf".to_string());
        prop_assert_eq!(proto.run_id, edge.run_id.0.to_string());
        prop_assert_eq!(proto.started, edge.started);
        prop_assert_eq!(
            proto.eager_workflow_task.is_some(),
            edge.eager_workflow_task.is_some()
        );
    }

    #[test]
    fn property_poll_response_projection(edge in arb_poll_response()) {
        let proto = poll_response_to_proto(edge.clone());
        prop_assert_eq!(proto.task_token, edge.task_token);
        prop_assert_eq!(proto.started_event_id, edge.started_event_id);
        prop_assert_eq!(
            proto.previous_started_event_id,
            edge.previous_started_event_id
        );
        prop_assert_eq!(
            proto.scheduled_time.map(|ts| (ts.seconds, ts.nanos)),
            edge.scheduled_time.map(|t| (t.unix_timestamp(), t.nanosecond() as i32))
        );
        prop_assert_eq!(
            proto.started_time.map(|ts| (ts.seconds, ts.nanos)),
            edge.started_time.map(|t| (t.unix_timestamp(), t.nanosecond() as i32))
        );
        let execution = proto.workflow_execution.expect("workflow_execution");
        prop_assert_eq!(execution.workflow_id, edge.payload.workflow_id);
        prop_assert_eq!(execution.run_id, edge.payload.run_key.0.to_string());
    }

    #[test]
    fn property_describe_response_projection(edge in arb_description()) {
        let proto = describe_response_to_proto(edge.clone());
        let info = proto.workflow_execution_info.expect("workflow_execution_info");
        let exec = info.execution.expect("execution");
        prop_assert_eq!(exec.workflow_id, edge.workflow_id);
        prop_assert_eq!(exec.run_id, edge.run_id.0.to_string());
        prop_assert_eq!(
            info.r#type.expect("workflow_type").name,
            edge.workflow_type
        );
        prop_assert_eq!(info.task_queue, edge.task_queue);
        prop_assert_eq!(info.status, execution_status_to_proto(edge.status));
        prop_assert_eq!(
            info.start_time.map(|ts| (ts.seconds, ts.nanos)),
            edge.start_time.map(|t| {
                let nanos = t.unix_timestamp_nanos();
                (
                    (nanos / 1_000_000_000) as i64,
                    (nanos % 1_000_000_000) as i32,
                )
            })
        );
        prop_assert_eq!(
            info.close_time.map(|ts| (ts.seconds, ts.nanos)),
            edge.close_time.map(|t| {
                let nanos = t.unix_timestamp_nanos();
                (
                    (nanos / 1_000_000_000) as i64,
                    (nanos % 1_000_000_000) as i32,
                )
            })
        );
        prop_assert_eq!(info.history_length, edge.history_length);
        prop_assert_eq!(info.state_transition_count, edge.state_transition_count);
        prop_assert_eq!(
            info.memo.expect("memo").fields.len(),
            edge.memo.0.len()
        );
        prop_assert_eq!(
            info.search_attributes.expect("search attributes").indexed_fields.len(),
            edge.search_attributes.0.len()
        );
    }

    #[test]
    fn property_pending_activities_count_and_fields(edge in arb_description()) {
        let proto = describe_response_to_proto(edge.clone());
        prop_assert_eq!(proto.pending_activities.len(), edge.pending_activities.len());
        for (actual, expected) in proto.pending_activities.iter().zip(edge.pending_activities.iter()) {
            prop_assert_eq!(&actual.activity_id, &expected.activity_id);
            prop_assert_eq!(
                actual.activity_type.as_ref().map(|t| t.name.as_str()).unwrap_or_default(),
                expected.activity_type.as_str()
            );
            prop_assert_eq!(actual.attempt, expected.attempt as i32);
            prop_assert_eq!(
                actual.state,
                if expected.is_started {
                    tokeira_proto::enums::PendingActivityState::Started as i32
                } else {
                    tokeira_proto::enums::PendingActivityState::Scheduled as i32
                }
            );
        }
    }

    #[test]
    fn property_pending_children_count_and_fields(edge in arb_description()) {
        let proto = describe_response_to_proto(edge.clone());
        prop_assert_eq!(proto.pending_children.len(), edge.pending_children.len());
        for (actual, expected) in proto.pending_children.iter().zip(edge.pending_children.iter()) {
            prop_assert_eq!(&actual.workflow_id, &expected.workflow_id);
            prop_assert_eq!(actual.initiated_id, expected.initiated_event_id);
            prop_assert_eq!(
                actual.parent_close_policy,
                match expected.parent_close_policy {
                    ParentClosePolicy::Terminate => 1,
                    ParentClosePolicy::Abandon => 2,
                    ParentClosePolicy::RequestCancel => 3,
                }
            );
        }
    }

    #[test]
    fn property_pending_wft_presence_and_fields(edge in arb_description()) {
        let proto = describe_response_to_proto(edge.clone());
        prop_assert_eq!(
            proto.pending_workflow_task.is_some(),
            edge.pending_workflow_task.is_some()
        );
        if let (Some(actual), Some(expected)) =
            (proto.pending_workflow_task.as_ref(), edge.pending_workflow_task.as_ref())
        {
            prop_assert_eq!(actual.attempt, expected.attempt as i32);
            prop_assert_eq!(
                actual.state,
                if expected.is_started {
                    tokeira_proto::enums::PendingWorkflowTaskState::Started as i32
                } else {
                    tokeira_proto::enums::PendingWorkflowTaskState::Scheduled as i32
                }
            );
            prop_assert_eq!(actual.started_time.is_some(), expected.started_at.is_some());
            prop_assert_eq!(actual.scheduled_time.is_some(), true);
        }
    }

    #[test]
    fn property_list_response_projection(edge in arb_list_response()) {
        let proto = list_response_to_proto(edge.clone());
        prop_assert_eq!(proto.executions.len(), edge.executions.len());
        prop_assert_eq!(proto.next_page_token, edge.next_page_token.unwrap_or_default().into_bytes());
    }

    #[test]
    fn property_count_response_projection(edge in arb_count_response()) {
        let proto = count_response_to_proto(edge.clone());
        prop_assert_eq!(proto.count, edge.total_count);
        prop_assert_eq!(proto.groups.len(), edge.groups.len());
    }

    #[test]
    fn property_workflow_command_roundtrip(cmd in arb_workflow_command()) {
        match &cmd {
            WorkflowCommand::ScheduleActivity { .. } => {
                let proto = workflow_command_to_proto(&cmd).unwrap();
                let roundtrip = proto_command_to_workflow_command(proto).unwrap();
                // v1.31.0 ensure-defaults the activity retry policy on decode —
                // activities always carry one (`EnsureDefaults`; see
                // `grpc/translate.rs` `activity_retry_policy_with_defaults`). The
                // generator authors `retry_policy: None`, so the round-trip is
                // identity on every field except the now-ensured retry policy.
                let mut expected = cmd.clone();
                if let WorkflowCommand::ScheduleActivity { retry_policy, .. } = &mut expected {
                    *retry_policy = Some(ensured_activity_retry_policy());
                }
                prop_assert_eq!(roundtrip, expected);
            }
            WorkflowCommand::StartTimer { .. }
            | WorkflowCommand::UpsertMemo(_)
            | WorkflowCommand::UpsertSearchAttributes(_)
            | WorkflowCommand::CompleteWorkflow { .. }
            | WorkflowCommand::FailWorkflow { .. }
            | WorkflowCommand::CancelWorkflow
            | WorkflowCommand::CancelTimer { .. }
            | WorkflowCommand::StartChildWorkflow { .. }
            | WorkflowCommand::SignalExternalWorkflowExecution { .. }
            | WorkflowCommand::RequestCancelExternalWorkflowExecution { .. }
            | WorkflowCommand::CancelNexusOperation { .. } => {
                let proto = workflow_command_to_proto(&cmd).unwrap();
                let roundtrip = proto_command_to_workflow_command(proto).unwrap();
                match (&roundtrip, &cmd) {
                    (
                        WorkflowCommand::FailWorkflow { failure: actual },
                        WorkflowCommand::FailWorkflow { failure: expected },
                    ) => {
                        prop_assert_eq!(
                            payload_to_failure(actual).message,
                            payload_to_failure(expected).message
                        );
                    }
                    _ => prop_assert_eq!(roundtrip, cmd),
                }
            }
            WorkflowCommand::RecordMarker { .. }
            | WorkflowCommand::ContinueAsNew { .. }
            | WorkflowCommand::RequestCancelActivity { .. }
            | WorkflowCommand::ScheduleNexusOperation { .. } => {
                prop_assert!(workflow_command_to_proto(&cmd).is_ok());
            }
            WorkflowCommand::ProtocolMessage { .. } => {
                let proto = workflow_command_to_proto(&cmd).unwrap();
                // ProtocolMessage commands produce a placeholder that
                // the edge layer resolves from the messages field.
                let result = proto_command_to_workflow_command(proto);
                prop_assert!(result.is_ok());
            }
            WorkflowCommand::UpdateCompleted { .. }
            | WorkflowCommand::UpdateRejected { .. }
            | WorkflowCommand::RequestNewWorkflowTask => {
                prop_assert!(workflow_command_to_proto(&cmd).is_err());
            }
        }
    }

    #[test]
    fn property_forward_translation_supported_for_added_commands(cmd in arb_workflow_command()) {
        match &cmd {
            WorkflowCommand::ContinueAsNew { .. }
            | WorkflowCommand::RecordMarker { .. }
            | WorkflowCommand::RequestCancelActivity { .. }
            | WorkflowCommand::ScheduleNexusOperation { .. }
            | WorkflowCommand::ProtocolMessage { .. } => {
                prop_assert!(workflow_command_to_proto(&cmd).is_ok());
            }
            WorkflowCommand::UpdateCompleted { .. }
            | WorkflowCommand::UpdateRejected { .. }
            | WorkflowCommand::RequestNewWorkflowTask => {
                prop_assert!(workflow_command_to_proto(&cmd).is_err());
            }
            _ => {}
        }
    }

    #[test]
    fn property_edge_error_to_grpc_status(err in arb_edge_error()) {
        let code = expected_code(&err);
        let status: tonic::Status = err.into();
        prop_assert_eq!(status.code(), code);
    }

    #[test]
    fn property_grpc_metadata_to_header_map(pairs in prop::collection::vec((arb_header_key(), arb_header_value()), 0..8)) {
        let mut metadata = MetadataMap::new();
        for (k, v) in &pairs {
            metadata.append(k.parse::<tonic::metadata::MetadataKey<tonic::metadata::Ascii>>().unwrap(), v.parse().unwrap());
        }

        let headers = metadata_to_header_map(&metadata);
        let mut expected = BTreeMap::<String, Vec<HeaderValue>>::new();
        for (k, v) in pairs {
            expected
                .entry(k)
                .or_default()
                .push(HeaderValue::from_str(&v).unwrap());
        }

        for (k, values) in expected {
            let actual: Vec<_> = headers.get_all(k.as_str()).iter().cloned().collect();
            prop_assert_eq!(actual, values);
        }
    }

    #[test]
    fn property_schedule_activity_preserves_request_eager_execution(
        cmd in arb_schedule_activity_command()
    ) {
        let proto = workflow_command_to_proto(&cmd).unwrap();
        let roundtrip = proto_command_to_workflow_command(proto).unwrap();
        // The eager-execution flag must survive the round-trip. The retry policy
        // is ensure-defaulted on decode (v1.31.0: activities always carry one),
        // so compare against the authored command with that default filled in.
        let mut expected = cmd.clone();
        if let WorkflowCommand::ScheduleActivity { retry_policy, .. } = &mut expected {
            *retry_policy = Some(ensured_activity_retry_policy());
        }
        prop_assert_eq!(roundtrip, expected);
    }
}

#[test]
fn property_proto_conversion_status_maps_to_invalid_argument() {
    let status = proto_conversion_status(
        tokeira_proto::conversions::ProtoConversionError::MissingField("field"),
    );
    assert_eq!(status.code(), Code::InvalidArgument);
}

#[test]
fn signal_request_rejects_link_without_variant() {
    let mut proto = signal_request_to_proto(&SignalWorkflowExecutionRequest {
        namespace: "default".to_string(),
        workflow_id: "workflow-a".to_string(),
        run_id: None,
        signal_name: "poke".to_string(),
        input: Payloads::default(),
        header: None,
        links: Vec::new(),
        request_id: Some("signal-1".to_string()),
        identity: Some("tester".to_string()),
        now: None,
    });
    proto
        .links
        .push(tokeira_proto::common::Link { variant: None });

    let error = signal_request_to_edge(proto).expect_err("unset Link.variant is invalid");
    let status = proto_conversion_status(error);
    assert_eq!(status.code(), Code::InvalidArgument);
}

fn start_request_to_proto(
    edge: &StartWorkflowExecutionRequest,
) -> workflowservice::StartWorkflowExecutionRequest {
    workflowservice::StartWorkflowExecutionRequest {
        namespace: edge.namespace.clone(),
        workflow_id: edge.workflow_id.clone(),
        workflow_type: Some(tokeira_proto::common::WorkflowType {
            name: edge.workflow_type.clone(),
        }),
        task_queue: Some(tokeira_proto::taskqueue::TaskQueue {
            name: edge.task_queue.clone(),
            ..Default::default()
        }),
        input: Some(payloads_from_domain(&edge.input)),
        request_id: edge.request_id.clone().unwrap_or_default(),
        request_eager_execution: edge.request_eager_execution,
        memo: Some(memo_from_domain(&edge.memo)),
        search_attributes: Some(search_attributes_from_domain(&edge.search_attributes)),
        ..Default::default()
    }
}

fn signal_request_to_proto(
    edge: &SignalWorkflowExecutionRequest,
) -> workflowservice::SignalWorkflowExecutionRequest {
    workflowservice::SignalWorkflowExecutionRequest {
        namespace: edge.namespace.clone(),
        workflow_execution: Some(tokeira_proto::common::WorkflowExecution {
            workflow_id: edge.workflow_id.clone(),
            run_id: edge.run_id.clone().unwrap_or_default(),
            ..Default::default()
        }),
        signal_name: edge.signal_name.clone(),
        input: Some(payloads_from_domain(&edge.input)),
        header: edge.header.as_ref().map(headers_from_domain),
        links: edge.links.iter().map(edge_link_to_proto).collect(),
        request_id: edge.request_id.clone().unwrap_or_default(),
        identity: edge.identity.clone().unwrap_or_default(),
        ..Default::default()
    }
}

fn edge_link_to_proto(link: &Link) -> tokeira_proto::common::Link {
    use tokeira_proto::common::link::Variant;

    match link {
        Link::BatchJob { job_id } => tokeira_proto::common::Link {
            variant: Some(Variant::BatchJob(tokeira_proto::common::link::BatchJob {
                job_id: job_id.clone(),
            })),
        },
        Link::WorkflowEvent { .. } | Link::Activity { .. } | Link::NexusOperation { .. } => {
            // The signal property generator currently uses batch links because
            // this test only needs to prove the gRPC signal path does not drop
            // the repeated `links` field.
            unreachable!("signal property generator only emits batch links")
        }
    }
}

fn poll_request_to_proto(
    edge: &PollWorkflowTaskQueueRequest,
) -> workflowservice::PollWorkflowTaskQueueRequest {
    workflowservice::PollWorkflowTaskQueueRequest {
        namespace: edge.namespace.clone(),
        task_queue: Some(tokeira_proto::taskqueue::TaskQueue {
            name: edge.task_queue.clone(),
            ..Default::default()
        }),
        identity: edge.worker_identity.clone(),
        ..Default::default()
    }
}

fn execution_status_to_proto(status: ExecutionStatus) -> i32 {
    match status {
        ExecutionStatus::Running => WorkflowExecutionStatus::Running as i32,
        ExecutionStatus::Paused => WorkflowExecutionStatus::Paused as i32,
        ExecutionStatus::Completed => WorkflowExecutionStatus::Completed as i32,
        ExecutionStatus::Failed => WorkflowExecutionStatus::Failed as i32,
        ExecutionStatus::Cancelled => WorkflowExecutionStatus::Canceled as i32,
        ExecutionStatus::Terminated => WorkflowExecutionStatus::Terminated as i32,
        ExecutionStatus::ContinuedAsNew => WorkflowExecutionStatus::ContinuedAsNew as i32,
        ExecutionStatus::TimedOut => WorkflowExecutionStatus::TimedOut as i32,
    }
}

fn expected_code(err: &EdgeError) -> Code {
    match err {
        EdgeError::BadRequest(_) => Code::InvalidArgument,
        EdgeError::Unimplemented(_) => Code::Unimplemented,
        EdgeError::NotFound(_) => Code::NotFound,
        EdgeError::AlreadyExists(_) => Code::AlreadyExists,
        EdgeError::ResourceExhausted(_) => Code::ResourceExhausted,
        EdgeError::WorkflowClosing => Code::ResourceExhausted,
        EdgeError::WorkflowNotReady(_) => Code::FailedPrecondition,
        EdgeError::QueryFailed { .. } => Code::InvalidArgument,
        EdgeError::QueryTimedOut => Code::DeadlineExceeded,
        EdgeError::Unauthorized(_) => Code::Unauthenticated,
        EdgeError::Forbidden { .. } => Code::PermissionDenied,
        EdgeError::NamespaceNotFound(_)
        | EdgeError::WorkflowNotFound { .. }
        | EdgeError::ActivityNotFound { .. }
        | EdgeError::BatchOperationNotFound { .. } => Code::NotFound,
        EdgeError::ActivityNotStarted { .. } => Code::FailedPrecondition,
        EdgeError::WorkflowAlreadyStarted { .. }
        | EdgeError::WorkflowStartRejected { .. }
        | EdgeError::BatchOperationAlreadyExists { .. } => Code::AlreadyExists,
        EdgeError::ActivityExecutionAlreadyStarted { .. } => Code::AlreadyExists,
        EdgeError::NamespaceDeleted(_) => Code::FailedPrecondition,
        EdgeError::NamespaceAlreadyExists(_) => Code::AlreadyExists,
        EdgeError::TooManyLongPolls => Code::ResourceExhausted,
        EdgeError::LongPollAdmissionTimeout => Code::DeadlineExceeded,
        EdgeError::RemoteRouteUnsupported { .. } => Code::Unavailable,
        EdgeError::NotShardOwner { .. } => Code::Aborted,
        EdgeError::FailedPrecondition(_) => Code::FailedPrecondition,
        EdgeError::Internal(_) => Code::Internal,
    }
}

#[test]
fn describe_request_rejects_malformed_run_id() {
    let status = proto_conversion_status(
        describe_request_to_edge(workflowservice::DescribeWorkflowExecutionRequest {
            namespace: "default".to_string(),
            execution: Some(tokeira_proto::common::WorkflowExecution {
                workflow_id: "workflow".to_string(),
                run_id: "not-a-run-id".to_string(),
            }),
        })
        .unwrap_err(),
    );
    assert_eq!(status.code(), Code::InvalidArgument);
}

fn arb_small_string() -> impl Strategy<Value = String> {
    prop::collection::vec(prop::char::range('a', 'z'), 1..8)
        .prop_map(|chars| chars.into_iter().collect())
}

fn arb_header_key() -> impl Strategy<Value = String> {
    prop::collection::vec(prop::char::range('a', 'z'), 1..8).prop_filter_map(
        "avoid grpc binary metadata suffix",
        |chars| {
            let suffix: String = chars.into_iter().collect();
            (!suffix.ends_with("bin")).then(|| format!("x-{suffix}"))
        },
    )
}

fn arb_header_value() -> impl Strategy<Value = String> {
    prop::collection::vec(prop::char::range('a', 'z'), 1..12)
        .prop_map(|chars| chars.into_iter().collect())
}

fn arb_payload() -> impl Strategy<Value = Payload> {
    (
        prop::collection::btree_map(arb_small_string(), arb_small_string(), 0..3),
        prop::collection::vec(any::<u8>(), 0..8),
    )
        .prop_map(|(metadata, data)| Payload {
            metadata,
            data,
            external_payloads: Vec::new(),
        })
}

fn arb_payloads() -> impl Strategy<Value = Payloads> {
    prop::collection::vec(arb_payload(), 0..4).prop_map(Payloads)
}

fn arb_headers() -> impl Strategy<Value = Headers> {
    prop::collection::btree_map(arb_small_string(), arb_payload(), 0..3).prop_map(Headers)
}

fn arb_signal_links() -> impl Strategy<Value = Vec<Link>> {
    prop::collection::vec(
        arb_small_string().prop_map(|job_id| Link::BatchJob { job_id }),
        0..3,
    )
}

fn arb_memo() -> impl Strategy<Value = Memo> {
    prop::collection::btree_map(arb_small_string(), arb_payload(), 0..3).prop_map(Memo)
}

fn arb_search_attr_value() -> impl Strategy<Value = SearchAttrValue> {
    prop_oneof![
        arb_small_string().prop_map(SearchAttrValue::Keyword),
        // Non-empty only: an empty keyword list encodes as bare JSON `[]`,
        // which the admission nil-filter drops as an unset marker
        // (`common/payload/payload.go:94 @ v1.31.0`), so it cannot round-trip.
        prop::collection::vec(arb_small_string(), 1..4).prop_map(SearchAttrValue::KeywordList),
        any::<i64>().prop_map(SearchAttrValue::Int),
        any::<bool>().prop_map(SearchAttrValue::Bool),
        (-1_000_000i64..1_000_000i64).prop_map(|v| SearchAttrValue::Double(v as f64 / 10.0)),
        (0i64..4_000_000_000i64).prop_map(|secs| SearchAttrValue::Datetime(
            OffsetDateTime::from_unix_timestamp(secs).unwrap()
        )),
        arb_small_string().prop_map(SearchAttrValue::Text),
    ]
}

fn arb_search_attributes() -> impl Strategy<Value = SearchAttributes> {
    prop::collection::btree_map(arb_small_string(), arb_search_attr_value(), 0..4)
        .prop_map(SearchAttributes)
}

fn arb_start_request() -> impl Strategy<Value = StartWorkflowExecutionRequest> {
    (
        arb_small_string(),
        arb_small_string(),
        arb_small_string(),
        arb_small_string(),
        arb_payloads(),
        prop::option::of(arb_small_string()),
        arb_memo(),
        arb_search_attributes(),
        any::<bool>(),
    )
        .prop_map(
            |(
                namespace,
                workflow_id,
                workflow_type,
                task_queue,
                input,
                request_id,
                memo,
                search_attributes,
                request_eager_execution,
            )| StartWorkflowExecutionRequest {
                namespace,
                workflow_id,
                workflow_type,
                task_queue,
                input,
                request_id,
                memo,
                search_attributes,
                identity: None,
                request_eager_execution,
                workflow_start_delay: None,
                completion_callbacks: Vec::new(),
                user_metadata: None,
                links: Vec::new(),
                eager_worker_deployment_options: None,
                workflow_execution_timeout: None,
                workflow_run_timeout: None,
                workflow_task_timeout: None,
                retry_policy: None,
                conflict_policy: WorkflowIdConflictPolicy::Fail,
                reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
                header: None,
                versioning_override: None,
                on_conflict_options: None,
                priority: None,
                cron_schedule: None,
                run_key: None,
                run_id: None,
                now: None,
            },
        )
}

fn arb_signal_request() -> impl Strategy<Value = SignalWorkflowExecutionRequest> {
    (
        arb_small_string(),
        arb_small_string(),
        arb_small_string(),
        prop::option::of(
            "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}".prop_map(|s| s),
        ),
        arb_payloads(),
        prop::option::of(arb_headers()),
        arb_signal_links(),
        prop::option::of(arb_small_string()),
        prop::option::of(arb_small_string()),
    )
        .prop_map(
            |(
                namespace,
                workflow_id,
                signal_name,
                run_id,
                input,
                header,
                links,
                request_id,
                identity,
            )| {
                SignalWorkflowExecutionRequest {
                    namespace,
                    workflow_id,
                    run_id,
                    signal_name,
                    input,
                    header,
                    links,
                    request_id,
                    identity,
                    now: None,
                }
            },
        )
}

fn arb_poll_request() -> impl Strategy<Value = PollWorkflowTaskQueueRequest> {
    (arb_small_string(), arb_small_string(), arb_small_string()).prop_map(
        |(namespace, task_queue, worker_identity)| PollWorkflowTaskQueueRequest {
            namespace,
            task_queue,
            worker_identity,
            deployment: None,
            build_id: None,
            sticky_run: None,
            timeout: Duration::from_secs(60),
            sticky_ttl: Duration::from_secs(30),
        },
    )
}

fn arb_describe_request() -> impl Strategy<Value = DescribeWorkflowExecutionRequest> {
    (
        arb_small_string(),
        arb_small_string(),
        prop::option::of(any::<u128>()),
    )
        .prop_map(
            |(namespace, workflow_id, run_id)| DescribeWorkflowExecutionRequest {
                namespace,
                workflow_id,
                run_id: run_id.map(|value| Uuid::from_u128(value).to_string()),
            },
        )
}

fn arb_list_request() -> impl Strategy<Value = ListWorkflowExecutionsRequest> {
    (
        arb_small_string(),
        prop::option::of(arb_small_string()),
        0usize..32usize,
        prop::option::of(arb_small_string()),
    )
        .prop_map(|(namespace, query, page_size, next_page_token)| {
            ListWorkflowExecutionsRequest {
                namespace,
                query,
                page_size,
                next_page_token,
            }
        })
}

fn arb_count_request() -> impl Strategy<Value = CountWorkflowExecutionsRequest> {
    (arb_small_string(), prop::option::of(arb_small_string())).prop_map(|(namespace, query)| {
        CountWorkflowExecutionsRequest {
            namespace,
            query,
            group_by: None,
        }
    })
}

fn arb_start_response() -> impl Strategy<Value = StartWorkflowExecutionResponse> {
    (
        any::<u128>(),
        any::<u128>(),
        any::<u64>(),
        any::<i64>(),
        any::<bool>(),
        prop::option::of(arb_poll_response()),
    )
        .prop_map(
            |(run_key, run_id, transition_seq, last_event_id, started, eager_workflow_task)| {
                StartWorkflowExecutionResponse {
                    run_key: RunKey(Uuid::from_u128(run_key)),
                    run_id: RunId(Uuid::from_u128(run_id)),
                    transition_seq,
                    last_event_id,
                    started,
                    status: ExecutionStatus::Running,
                    attached_request_id: None,
                    eager_workflow_task,
                }
            },
        )
}

fn arb_completed_response() -> impl Strategy<Value = RespondWorkflowTaskCompletedResponse> {
    prop::collection::vec(arb_poll_activity_response(), 0..6).prop_map(|activity_tasks| {
        RespondWorkflowTaskCompletedResponse {
            transition_seq: 0,
            last_event_id: 0,
            execution_status: ExecutionStatus::Running,
            new_run_id: None,
            was_duplicate: false,
            workflow_task: None,
            activity_tasks,
        }
    })
}

fn arb_poll_response() -> impl Strategy<Value = PollWorkflowTaskQueueResponse> {
    (
        prop::collection::vec(any::<u8>(), 0..16),
        any::<i64>(),
        any::<i64>(),
        any::<u32>(),
        prop::option::of(0i64..4_000_000_000i64),
        prop::option::of(0i64..4_000_000_000i64),
        arb_small_string(),
        any::<u128>(),
        arb_small_string(),
    )
        .prop_map(
            |(
                task_token,
                started_event_id,
                previous_started_event_id,
                attempt,
                scheduled_time,
                started_time,
                workflow_id,
                run_key,
                task_queue,
            )| PollWorkflowTaskQueueResponse {
                task_token,
                started_event_id,
                previous_started_event_id,
                attempt,
                scheduled_time: scheduled_time
                    .map(OffsetDateTime::from_unix_timestamp)
                    .transpose()
                    .expect("valid timestamp"),
                started_time: started_time
                    .map(OffsetDateTime::from_unix_timestamp)
                    .transpose()
                    .expect("valid timestamp"),
                payload: WorkflowTaskPayloadDto {
                    workflow_id,
                    run_key: RunKey(Uuid::from_u128(run_key)),
                    run_id: RunId(Uuid::from_u128(run_key)),
                    task_queue,
                    history: Vec::new(),
                },
                query: None,
                queries: Default::default(),
                messages: Vec::new(),
            },
        )
}

fn arb_description() -> impl Strategy<Value = WorkflowExecutionDescription> {
    (
        (
            arb_small_string(),
            arb_small_string(),
            any::<u128>(),
            any::<u128>(),
            arb_small_string(),
            arb_small_string(),
            arb_execution_status(),
        ),
        (
            prop::option::of(0i64..4_000_000_000i64),
            prop::option::of(0i64..4_000_000_000i64),
            0i64..1000i64,
            0i64..1000i64,
            arb_memo(),
            arb_search_attributes(),
            prop::collection::vec(arb_pending_activity(), 0..10),
            prop::collection::vec(arb_pending_child(), 0..5),
            prop::option::of(arb_pending_wft()),
        ),
    )
        .prop_map(
            |(
                (namespace, workflow_id, run_key, run_id, workflow_type, task_queue, status),
                (
                    start_time,
                    close_time,
                    history_length,
                    state_transition_count,
                    memo,
                    search_attributes,
                    pending_activities,
                    pending_children,
                    pending_workflow_task,
                ),
            )| WorkflowExecutionDescription {
                namespace,
                workflow_id: workflow_id.clone(),
                run_key: RunKey(Uuid::from_u128(run_key)),
                run_id: RunId(Uuid::from_u128(run_id)),
                workflow_type,
                task_queue: task_queue.clone(),
                status,
                start_time: start_time
                    .map(|secs| OffsetDateTime::from_unix_timestamp(secs).unwrap()),
                close_time: close_time
                    .map(|secs| OffsetDateTime::from_unix_timestamp(secs).unwrap()),
                execution_time: start_time
                    .map(|secs| OffsetDateTime::from_unix_timestamp(secs).unwrap())
                    .unwrap_or(OffsetDateTime::UNIX_EPOCH),
                execution_config: ExecutionConfigDescription {
                    task_queue: task_queue.clone(),
                    workflow_execution_timeout: None,
                    workflow_run_timeout: None,
                    default_workflow_task_timeout: time::Duration::seconds(10),
                    user_metadata: None,
                },
                history_length,
                state_transition_count,
                parent_namespace_id: None,
                parent_workflow_id: None,
                parent_run_id: None,
                root_workflow_id: Some(WorkflowId(workflow_id.clone())),
                root_run_id: Some(RunId(Uuid::from_u128(run_id))),
                first_run_id: Some(RunId(Uuid::from_u128(run_id))),
                memo,
                search_attributes,
                pending_activities,
                pending_children,
                pending_workflow_task,
                callbacks: Vec::new(),
                pending_nexus_operations: Vec::new(),
                pause_info: None,
                execution_expiration_time: None,
                run_expiration_time: None,
                cancel_requested: false,
                original_start_time: start_time
                    .map(|secs| OffsetDateTime::from_unix_timestamp(secs).unwrap())
                    .unwrap_or(OffsetDateTime::UNIX_EPOCH),
                versioning_info: None,
                worker_deployment_name: None,
                request_id_infos: std::collections::BTreeMap::new(),
                external_payload_count: 0,
                external_payload_size_bytes: 0,
            },
        )
}

fn arb_pending_activity() -> impl Strategy<Value = PendingActivityDescription> {
    (
        arb_small_string(),
        arb_small_string(),
        any::<bool>(),
        1u32..5,
        0u32..10,
        0i64..4_000_000_000i64,
        prop::option::of(0i64..4_000_000_000i64),
    )
        .prop_map(
            |(
                activity_id,
                activity_type,
                is_started,
                attempt,
                maximum_attempts,
                scheduled_at,
                started_at,
            )| PendingActivityDescription {
                activity_id,
                activity_type,
                is_started,
                attempt,
                maximum_attempts,
                scheduled_at: OffsetDateTime::from_unix_timestamp(scheduled_at).unwrap(),
                started_at: started_at
                    .map(|secs| OffsetDateTime::from_unix_timestamp(secs).unwrap()),
                last_failure: None,
                heartbeat_details: None,
                last_worker_identity: String::new(),
                paused: false,
                pause_info: None,
            },
        )
}

fn arb_pending_child() -> impl Strategy<Value = PendingChildDescription> {
    (
        arb_small_string(),
        prop::option::of(arb_small_string()),
        arb_small_string(),
        1i64..1000,
        prop_oneof![
            Just(ParentClosePolicy::Terminate),
            Just(ParentClosePolicy::Abandon),
            Just(ParentClosePolicy::RequestCancel),
        ],
    )
        .prop_map(
            |(workflow_id, run_id, workflow_type, initiated_event_id, parent_close_policy)| {
                PendingChildDescription {
                    workflow_id,
                    run_id,
                    workflow_type,
                    initiated_event_id,
                    parent_close_policy,
                }
            },
        )
}

fn arb_pending_wft() -> impl Strategy<Value = PendingWorkflowTaskDescription> {
    (
        any::<bool>(),
        0i64..4_000_000_000i64,
        prop::option::of(0i64..4_000_000_000i64),
        1u32..5,
    )
        .prop_map(|(is_started, scheduled_at, started_at, attempt)| {
            PendingWorkflowTaskDescription {
                is_started,
                scheduled_at: OffsetDateTime::from_unix_timestamp(scheduled_at).unwrap(),
                started_at: started_at
                    .map(|secs| OffsetDateTime::from_unix_timestamp(secs).unwrap()),
                attempt,
            }
        })
}

fn arb_list_response() -> impl Strategy<Value = ListWorkflowExecutionsResponse> {
    (
        prop::collection::vec(arb_summary(), 0..4),
        prop::option::of(arb_small_string()),
    )
        .prop_map(
            |(executions, next_page_token)| ListWorkflowExecutionsResponse {
                executions,
                next_page_token,
            },
        )
}

fn arb_summary() -> impl Strategy<Value = WorkflowExecutionSummary> {
    (
        arb_small_string(),
        arb_small_string(),
        any::<u128>(),
        arb_small_string(),
        arb_small_string(),
        arb_execution_status(),
        prop::option::of(0i64..4_000_000_000i64),
        prop::option::of(0i64..4_000_000_000i64),
    )
        .prop_map(
            |(
                namespace,
                workflow_id,
                run_id,
                workflow_type,
                task_queue,
                status,
                start_time,
                close_time,
            )| WorkflowExecutionSummary {
                namespace,
                workflow_id,
                run_id: RunId(Uuid::from_u128(run_id)),
                workflow_type,
                task_queue,
                status,
                start_time: start_time
                    .map(|secs| OffsetDateTime::from_unix_timestamp(secs).unwrap()),
                close_time: close_time
                    .map(|secs| OffsetDateTime::from_unix_timestamp(secs).unwrap()),
                history_length: 0,
                state_transition_count: 0,
                memo: Default::default(),
                search_attributes: Default::default(),
            },
        )
}

fn arb_count_response() -> impl Strategy<Value = CountWorkflowExecutionsResponse> {
    (
        any::<i64>(),
        prop::collection::vec((arb_small_string(), any::<i64>()), 0..4),
    )
        .prop_map(|(total_count, groups)| CountWorkflowExecutionsResponse {
            total_count,
            groups: groups
                .into_iter()
                .map(|(value, count)| GroupCount { value, count })
                .collect(),
        })
}

fn arb_execution_status() -> impl Strategy<Value = ExecutionStatus> {
    prop_oneof![
        Just(ExecutionStatus::Running),
        Just(ExecutionStatus::Paused),
        Just(ExecutionStatus::Completed),
        Just(ExecutionStatus::Failed),
        Just(ExecutionStatus::Cancelled),
        Just(ExecutionStatus::Terminated),
        Just(ExecutionStatus::ContinuedAsNew),
        Just(ExecutionStatus::TimedOut),
    ]
}

fn arb_workflow_command() -> impl Strategy<Value = WorkflowCommand> {
    prop_oneof![
        (arb_small_string(), arb_small_string(), arb_payloads()).prop_map(
            |(activity_id, task_queue, input)| WorkflowCommand::ScheduleActivity {
                activity_id,
                activity_type: "activity-type".into(),
                task_queue: tokeira_types::TaskQueueName(task_queue),
                input,
                header: None,
                request_eager_execution: false,
                retry_policy: None,
                deployment: None,
                build_id: None,
                schedule_to_close_timeout: None,
                schedule_to_start_timeout: None,
                start_to_close_timeout: None,
                heartbeat_timeout: None,
            }
        ),
        arb_memo().prop_map(WorkflowCommand::UpsertMemo),
        arb_search_attributes().prop_map(WorkflowCommand::UpsertSearchAttributes),
        (
            arb_small_string(),
            prop::collection::btree_map(arb_small_string(), arb_payloads(), 0..3),
            prop::option::of(arb_payload()),
            prop::option::of(prop::collection::btree_map(
                arb_small_string(),
                arb_payload(),
                0..3
            )),
        )
            .prop_map(|(marker_name, details, failure, header)| {
                WorkflowCommand::RecordMarker {
                    marker_name,
                    details,
                    failure,
                    header,
                }
            }),
        arb_payloads().prop_map(|result| WorkflowCommand::CompleteWorkflow { result }),
        arb_small_string().prop_map(|message| WorkflowCommand::FailWorkflow {
            failure: Payload::new(message.into_bytes()),
        }),
        (
            arb_small_string(),
            arb_small_string(),
            arb_payloads(),
            arb_memo(),
            arb_search_attributes(),
        )
            .prop_map(
                |(workflow_type, task_queue, input, memo, search_attributes)| {
                    WorkflowCommand::ContinueAsNew {
                        new_run_id: tokeira_types::RunId::new(),
                        workflow_type: tokeira_types::WorkflowType(workflow_type),
                        task_queue: tokeira_types::TaskQueueName(task_queue),
                        input,
                        memo,
                        search_attributes,
                        workflow_execution_timeout: None,
                        workflow_run_timeout: None,
                        workflow_task_timeout: time::Duration::seconds(10),
                        retry_policy: None,
                    }
                }
            ),
        Just(WorkflowCommand::CancelWorkflow),
        (1i64..10_000).prop_map(|scheduled_event_id| {
            WorkflowCommand::RequestCancelActivity { scheduled_event_id }
        }),
        arb_small_string().prop_map(|timer_id| WorkflowCommand::CancelTimer { timer_id }),
        (
            arb_small_string(),
            arb_small_string(),
            arb_small_string(),
            arb_payloads(),
        )
            .prop_map(|(child_workflow_id, workflow_type, task_queue, input)| {
                let namespace_id = tokeira_types::NamespaceId::new();
                WorkflowCommand::StartChildWorkflow {
                    child_workflow_id: tokeira_types::WorkflowId(child_workflow_id),
                    namespace: Some(namespace_id.0.to_string()),
                    namespace_id,
                    workflow_type: tokeira_types::WorkflowType(workflow_type),
                    task_queue: tokeira_types::TaskQueueName(task_queue),
                    input,
                    header: None,
                    memo: tokeira_types::Memo::default(),
                    search_attributes: tokeira_types::SearchAttributes::default(),
                    workflow_execution_timeout: None,
                    workflow_run_timeout: None,
                    workflow_task_timeout: time::Duration::seconds(10),
                    retry_policy: None,
                    cron_schedule: None,
                    parent_close_policy: tokeira_kernel::ParentClosePolicy::Terminate,
                }
            }),
        (arb_small_string(), arb_payloads(),).prop_map(|(target_workflow_id, input)| {
            let target_namespace_id = tokeira_types::NamespaceId::new();
            WorkflowCommand::SignalExternalWorkflowExecution {
                target_namespace: Some(target_namespace_id.0.to_string()),
                target_namespace_id,
                target_workflow_id: tokeira_types::WorkflowId(target_workflow_id),
                target_run_id: Some(tokeira_types::RunId::new()),
                signal_name: "sig".into(),
                input,
                header: None,
                control: "ctl".into(),
            }
        }),
        arb_small_string().prop_map(|target_workflow_id| {
            let target_namespace_id = tokeira_types::NamespaceId::new();
            WorkflowCommand::RequestCancelExternalWorkflowExecution {
                target_namespace: Some(target_namespace_id.0.to_string()),
                target_namespace_id,
                target_workflow_id: tokeira_types::WorkflowId(target_workflow_id),
                target_run_id: Some(tokeira_types::RunId::new()),
                control: "ctl".into(),
            }
        }),
        (
            arb_small_string(),
            arb_small_string(),
            arb_small_string(),
            arb_small_string(),
            arb_payloads(),
        )
            .prop_map(|(operation_id, endpoint, service, operation, input)| {
                WorkflowCommand::ScheduleNexusOperation {
                    operation_id,
                    endpoint,
                    service,
                    operation,
                    input,
                    schedule_to_close_timeout: None,
                    schedule_to_start_timeout: None,
                    start_to_close_timeout: None,
                }
            }),
        (1i64..100i64).prop_map(|scheduled_event_id| {
            WorkflowCommand::CancelNexusOperation { scheduled_event_id }
        }),
        arb_payloads().prop_map(|result| WorkflowCommand::UpdateCompleted {
            update_id: "update-1".into(),
            result,
        }),
        arb_small_string().prop_map(|failure| WorkflowCommand::UpdateRejected {
            update_id: "update-1".into(),
            failure: Payload::new(failure.into_bytes()),
        }),
        arb_payloads().prop_map(|input| WorkflowCommand::ProtocolMessage {
            message_id: "msg-1".into(),
            body: tokeira_kernel::UpdateProtocolBody::Accepted {
                update_id: "update-1".into(),
                update_name: "handler".into(),
                input,
            },
        }),
    ]
}

fn ensured_activity_retry_policy() -> tokeira_types::RetryPolicy {
    // v1.31.0 `EnsureDefaults` for an omitted activity retry policy
    // (`common/retrypolicy/retry_policy.go`): InitialInterval 1s,
    // BackoffCoefficient 2.0, MaximumInterval 100×InitialInterval, MaximumAttempts
    // 0 (unlimited). Mirrors `grpc/translate.rs` `activity_retry_policy_with_defaults(None)`.
    tokeira_types::RetryPolicy {
        initial_interval: time::Duration::seconds(1),
        backoff_coefficient: 2.0,
        maximum_interval: Some(time::Duration::seconds(100)),
        maximum_attempts: 0,
        non_retryable_error_types: Vec::new(),
    }
}

fn arb_schedule_activity_command() -> impl Strategy<Value = WorkflowCommand> {
    (
        arb_small_string(),
        arb_small_string(),
        arb_payloads(),
        any::<bool>(),
    )
        .prop_map(
            |(activity_id, task_queue, input, request_eager_execution)| {
                WorkflowCommand::ScheduleActivity {
                    activity_id,
                    activity_type: "activity-type".into(),
                    task_queue: tokeira_types::TaskQueueName(task_queue),
                    input,
                    header: None,
                    request_eager_execution,
                    retry_policy: None,
                    deployment: None,
                    build_id: None,
                    schedule_to_close_timeout: None,
                    schedule_to_start_timeout: None,
                    start_to_close_timeout: None,
                    heartbeat_timeout: None,
                }
            },
        )
}

fn arb_edge_error() -> impl Strategy<Value = EdgeError> {
    prop_oneof![
        arb_small_string().prop_map(EdgeError::BadRequest),
        arb_small_string().prop_map(EdgeError::Unauthorized),
        (
            prop_oneof![
                Just("operator_read"),
                Just("operator_write"),
                Just("start_workflow_execution")
            ],
            prop::option::of(arb_small_string())
        )
            .prop_map(|(action, namespace)| EdgeError::Forbidden { action, namespace }),
        arb_small_string().prop_map(EdgeError::NamespaceNotFound),
        arb_small_string().prop_map(EdgeError::NamespaceDeleted),
        (arb_small_string(), arb_small_string()).prop_map(|(namespace, workflow_id)| {
            EdgeError::WorkflowNotFound {
                namespace,
                workflow_id,
            }
        }),
        any::<bool>().prop_map(|_| EdgeError::TooManyLongPolls),
        any::<bool>().prop_map(|_| EdgeError::LongPollAdmissionTimeout),
        arb_small_string().prop_map(|target| EdgeError::RemoteRouteUnsupported { target }),
        arb_small_string().prop_map(EdgeError::Internal),
    ]
}

// ── Property 5: Proto-to-edge DTO round-trip for new endpoints ──
// **Validates: Requirements 12.1, 12.2, 12.3, 12.4, 12.5, 12.6,
//   12.7, 12.8, 12.9, 12.10, 12.11, 12.12, 12.15**

use tokeira_edge::{
    grpc::translate::{
        cancel_request_to_edge, deserialize_activity_token, poll_activity_request_to_edge,
        poll_activity_response_to_proto, query_request_to_edge, query_response_to_proto,
        respond_activity_completed_to_edge, serialize_activity_token, terminate_request_to_edge,
        update_request_to_edge, update_response_to_proto,
    },
    translate::{
        PollActivityTaskQueueRequest, PollActivityTaskQueueResponse, QueryWorkflowRequest,
        QueryWorkflowResponse, RequestCancelWorkflowExecutionRequest,
        TerminateWorkflowExecutionRequest, UpdateLifecycleStageDto, UpdateOutcomeDto, UpdateRefDto,
        UpdateWaitPolicyDto, UpdateWorkflowExecutionRequest, UpdateWorkflowExecutionResponse,
    },
};
use tokeira_types::{ActivityTaskToken, ShardEpoch};

fn arb_activity_task_token() -> impl Strategy<Value = ActivityTaskToken> {
    (
        any::<u128>(),
        arb_small_string(),
        any::<i64>(),
        1u32..100u32,
    )
        .prop_map(
            |(run_key, activity_id, schedule_event_id, attempt)| ActivityTaskToken {
                run_key: RunKey(Uuid::from_u128(run_key)),
                activity_id,
                schedule_event_id,
                attempt,
                shard_epoch: ShardEpoch::ZERO,
            },
        )
}

fn arb_poll_activity_request() -> impl Strategy<Value = PollActivityTaskQueueRequest> {
    (arb_small_string(), arb_small_string(), arb_small_string()).prop_map(
        |(namespace, task_queue, worker_identity)| PollActivityTaskQueueRequest {
            namespace,
            task_queue,
            worker_identity,
            timeout: Duration::from_secs(60),
        },
    )
}

fn arb_poll_activity_response() -> impl Strategy<Value = PollActivityTaskQueueResponse> {
    (
        arb_activity_task_token(),
        arb_small_string(),
        arb_payloads(),
        1u32..100u32,
        arb_small_string(),
        any::<u128>(),
    )
        .prop_map(
            |(token, activity_id, input, attempt, workflow_id, run_key)| {
                let token_bytes = serialize_activity_token(&token);
                PollActivityTaskQueueResponse {
                    task_token: token_bytes,
                    activity_id,
                    run_id: RunId(Uuid::from_u128(run_key)),
                    activity_type: String::new(),
                    input,
                    attempt,
                    workflow_id,
                    workflow_type: String::new(),
                    workflow_namespace: String::new(),
                    run_key: RunKey(Uuid::from_u128(run_key)),
                    header: None,
                    retry_policy: None,
                    heartbeat_details: None,
                    scheduled_time: None,
                    current_attempt_scheduled_time: None,
                    started_time: None,
                    schedule_to_close_timeout: None,
                    start_to_close_timeout: None,
                    heartbeat_timeout: None,
                }
            },
        )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Property 5a: PollActivityTaskQueue request round-trip
    #[test]
    fn property_poll_activity_request_roundtrip(
        edge in arb_poll_activity_request()
    ) {
        let proto =
            workflowservice::PollActivityTaskQueueRequest {
                namespace: edge.namespace.clone(),
                task_queue: Some(
                    tokeira_proto::taskqueue::TaskQueue {
                        name: edge.task_queue.clone(),
                        ..Default::default()
                    },
                ),
                identity: edge.worker_identity.clone(),
                ..Default::default()
            };
        let roundtrip =
            poll_activity_request_to_edge(proto).unwrap();
        prop_assert_eq!(roundtrip, edge);
    }

    // Property 5b: PollActivityTaskQueue response projection
    #[test]
    fn property_poll_activity_response_projection(
        edge in arb_poll_activity_response()
    ) {
        let proto =
            poll_activity_response_to_proto(edge.clone());
        prop_assert_eq!(proto.task_token, edge.task_token);
        prop_assert_eq!(proto.activity_id, edge.activity_id);
        prop_assert_eq!(
            proto.attempt, edge.attempt as i32
        );
    }

    #[test]
    fn property_completed_response_projection(
        edge in arb_completed_response()
    ) {
        let proto = completed_response_to_proto(edge.clone());
        prop_assert_eq!(proto.activity_tasks.len(), edge.activity_tasks.len());
    }

    // Property 5c: Terminate request round-trip
    #[test]
    fn property_terminate_request_roundtrip(
        namespace in arb_small_string(),
        workflow_id in arb_small_string(),
        reason in arb_small_string(),
        identity in arb_small_string(),
    ) {
        let edge = TerminateWorkflowExecutionRequest {
            namespace: namespace.clone(),
            workflow_id: workflow_id.clone(),
            run_id: None,
            reason: reason.clone(),
            details: None,
            identity: identity.clone(),
        };
        let proto =
            workflowservice::TerminateWorkflowExecutionRequest {
                namespace,
                workflow_execution: Some(tokeira_proto::common::WorkflowExecution {
                    workflow_id,
                    run_id: String::new(),
                    ..Default::default()
                }),
                reason,
                details: None,
                identity,
                ..Default::default()
            };
        let roundtrip =
            terminate_request_to_edge(proto).unwrap();
        prop_assert_eq!(roundtrip, edge);
    }

    // Property 5d: Cancel request round-trip
    #[test]
    fn property_cancel_request_roundtrip(
        namespace in arb_small_string(),
        workflow_id in arb_small_string(),
        reason in arb_small_string(),
        identity in arb_small_string(),
    ) {
        let edge = RequestCancelWorkflowExecutionRequest {
            namespace: namespace.clone(),
            workflow_id: workflow_id.clone(),
            run_id: None,
            reason: reason.clone(),
            identity: identity.clone(),
        };
        let proto =
            workflowservice::RequestCancelWorkflowExecutionRequest {
                namespace,
                workflow_execution: Some(tokeira_proto::common::WorkflowExecution {
                    workflow_id,
                    run_id: String::new(),
                    ..Default::default()
                }),
                reason,
                identity,
                ..Default::default()
            };
        let roundtrip =
            cancel_request_to_edge(proto).unwrap();
        prop_assert_eq!(roundtrip, edge);
    }

    // Property 5e: Query request round-trip
    #[test]
    fn property_query_request_roundtrip(
        namespace in arb_small_string(),
        workflow_id in arb_small_string(),
        query_type in arb_small_string(),
        query_args in arb_payloads(),
    ) {
        let edge = QueryWorkflowRequest {
            namespace: namespace.clone(),
            workflow_id: workflow_id.clone(),
            run_id: None,
            query_type: query_type.clone(),
            query_args: query_args.clone(),
            timeout: Duration::from_secs(10),
        };
        let proto = workflowservice::QueryWorkflowRequest {
            namespace,
            execution: Some(
                tokeira_proto::common::WorkflowExecution {
                    workflow_id,
                    run_id: String::new(),
                    ..Default::default()
                },
            ),
            query: Some(tokeira_proto::public::temporal::api::query::v1::WorkflowQuery {
                query_type,
                query_args: Some(payloads_from_domain(
                    &query_args,
                )),
                ..Default::default()
            }),
            ..Default::default()
        };
        let roundtrip =
            query_request_to_edge(proto).unwrap();
        prop_assert_eq!(roundtrip, edge);
    }

    // Property 5f: Query response projection
    #[test]
    fn property_query_response_projection(
        result in prop::option::of(arb_payloads()),
    ) {
        let edge = QueryWorkflowResponse {
            result: result.clone(),
            rejected_status: None,
        };
        let proto = query_response_to_proto(edge);
        match result {
            Some(payloads) => {
                let proto_payloads =
                    proto.query_result.unwrap();
                prop_assert_eq!(
                    proto_payloads.payloads.len(),
                    payloads.0.len()
                );
            }
            None => {
                prop_assert!(
                    proto.query_result.is_none()
                );
            }
        }
        prop_assert!(proto.query_rejected.is_none());
    }

    // Property 5g: Update request round-trip
    #[test]
    fn property_update_request_roundtrip(
        namespace in arb_small_string(),
        workflow_id in arb_small_string(),
        update_id in arb_small_string(),
        update_name in arb_small_string(),
        input in arb_payloads(),
        use_completed in any::<bool>(),
    ) {
        let wait_policy = if use_completed {
            UpdateWaitPolicyDto::Completed
        } else {
            UpdateWaitPolicyDto::Accepted
        };
        let stage = if use_completed { 3 } else { 2 };

        let edge = UpdateWorkflowExecutionRequest {
            namespace: namespace.clone(),
            workflow_id: workflow_id.clone(),
            run_id: None,
            first_execution_run_id: None,
            update_id: update_id.clone(),
            update_name: update_name.clone(),
            input: input.clone(),
            wait_policy,
            timeout: Duration::from_secs(30),
        };
        let proto =
            workflowservice::UpdateWorkflowExecutionRequest {
                namespace,
                workflow_execution: Some(
                    tokeira_proto::common::WorkflowExecution {
                        workflow_id,
                        run_id: String::new(),
                        ..Default::default()
                    },
                ),
                request: Some(tokeira_proto::public::temporal::api::update::v1::Request {
                    meta: Some(tokeira_proto::public::temporal::api::update::v1::Meta {
                        update_id,
                        identity: String::new(),
                    }),
                    input: Some(tokeira_proto::public::temporal::api::update::v1::Input {
                        name: update_name,
                        args: Some(payloads_from_domain(&input)),
                        ..Default::default()
                    }),
                }),
                wait_policy: Some(
                    tokeira_proto::public::temporal::api::update::v1::WaitPolicy {
                        lifecycle_stage: stage,
                    },
                ),
                ..Default::default()
            };
        let roundtrip =
            update_request_to_edge(proto).unwrap();
        prop_assert_eq!(roundtrip, edge);
    }

    // Property 5h: Update response projection
    #[test]
    fn property_update_response_projection(
        accepted_event_id in 1i64..1000i64,
        result in arb_payloads(),
        failure in arb_small_string(),
        rejected in any::<bool>(),
    ) {
        let outcome = if rejected {
            UpdateOutcomeDto::Rejected {
                accepted_event_id,
                failure: Payload::new(failure.clone().into_bytes()),
            }
        } else {
            UpdateOutcomeDto::Completed {
                accepted_event_id,
                result: result.clone(),
            }
        };
        let edge = UpdateWorkflowExecutionResponse {
            update_ref: UpdateRefDto {
                workflow_id: "workflow".to_string(),
                run_id: "00000000-0000-0000-0000-000000000001".to_string(),
                update_id: "update".to_string(),
            },
            stage: UpdateLifecycleStageDto::Completed,
            outcome: Some(outcome),
        };
        let _proto = update_response_to_proto(edge);
        // The upstream response type has different fields (update_ref, outcome, stage)
        // so we just verify the conversion doesn't panic.
    }
}

// ── Property 6: ActivityTaskToken serialization round-trip ──
// **Validates: Requirements 12.3, 12.4, 12.5, 12.13**

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn property_activity_token_roundtrip(
        token in arb_activity_task_token()
    ) {
        let bytes = serialize_activity_token(&token);
        let roundtrip =
            deserialize_activity_token(&bytes).unwrap();
        prop_assert_eq!(roundtrip, token);
    }

    #[test]
    fn property_activity_token_in_completed_request(
        token in arb_activity_task_token(),
        result in arb_payloads(),
    ) {
        let bytes = serialize_activity_token(&token);
        let proto =
            workflowservice::RespondActivityTaskCompletedRequest {
                task_token: bytes,
                result: Some(payloads_from_domain(&result)),
                identity: "worker".to_string(),
                ..Default::default()
            };
        let edge =
            respond_activity_completed_to_edge(proto)
                .unwrap();
        prop_assert_eq!(edge.token, token);
    }
}

// ── Property 8: failure_to_payload / payload_to_failure round-trip ──
// **Validates: Requirement 8 (AC 8.1)**

fn arb_proto_failure() -> impl Strategy<Value = failure_proto::Failure> {
    (
        "[a-z ]{0,20}",
        "[a-z]{0,10}",
        "[a-z\n]{0,30}",
        arb_failure_info(),
    )
        .prop_map(
            |(msg, source, stack, failure_info)| failure_proto::Failure {
                message: msg,
                source,
                stack_trace: stack,
                failure_info,
                ..Default::default()
            },
        )
}

fn arb_failure_info() -> impl Strategy<Value = Option<failure_proto::failure::FailureInfo>> {
    prop_oneof![
        Just(None),
        "[a-z]{0,10}".prop_map(|t| Some(
            failure_proto::failure::FailureInfo::ApplicationFailureInfo(
                failure_proto::ApplicationFailureInfo {
                    r#type: t,
                    non_retryable: false,
                    ..Default::default()
                },
            )
        )),
        Just(Some(
            failure_proto::failure::FailureInfo::TimeoutFailureInfo(
                failure_proto::TimeoutFailureInfo {
                    timeout_type: 3,
                    ..Default::default()
                },
            )
        )),
        Just(Some(
            failure_proto::failure::FailureInfo::CanceledFailureInfo(
                failure_proto::CanceledFailureInfo {
                    ..Default::default()
                },
            )
        )),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn prop_failure_payload_round_trip(failure in arb_proto_failure()) {
        let original_bytes = failure.encode_to_vec();
        let payload = failure_to_payload(&failure);
        let decoded = payload_to_failure(&payload);
        let re_encoded = decoded.encode_to_vec();
        prop_assert_eq!(original_bytes, re_encoded);
    }
}
