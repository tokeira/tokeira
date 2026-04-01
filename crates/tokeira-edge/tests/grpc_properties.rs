use std::{collections::BTreeMap, time::Duration};

use http::header::HeaderValue;
use proptest::prelude::*;
use time::OffsetDateTime;
use tokeira_edge::{
    errors::EdgeError,
    grpc::{
        errors::proto_conversion_status,
        metadata::metadata_to_header_map,
        translate::{
            count_request_to_edge, count_response_to_proto, describe_request_to_edge,
            describe_response_to_proto, list_request_to_edge, list_response_to_proto,
            poll_request_to_edge, poll_response_to_proto, proto_command_to_workflow_command,
            signal_request_to_edge, start_request_to_edge, start_response_to_proto,
            workflow_command_to_proto,
        },
    },
    translate::{
        CountWorkflowExecutionsRequest, CountWorkflowExecutionsResponse,
        DescribeWorkflowExecutionRequest, GroupCount, ListWorkflowExecutionsRequest,
        ListWorkflowExecutionsResponse, PollWorkflowTaskQueueRequest, PollWorkflowTaskQueueResponse,
        SignalWorkflowExecutionRequest, StartWorkflowExecutionRequest,
        StartWorkflowExecutionResponse, WorkflowExecutionDescription, WorkflowExecutionSummary,
        WorkflowTaskPayloadDto,
    },
};
use tokeira_kernel::WorkflowCommand;
use tokeira_proto::{
    conversions::common::{memo_from_domain, payloads_from_domain, search_attributes_from_domain},
    enums::WorkflowExecutionStatus,
    workflowservice,
};
use tokeira_types::{ExecutionStatus, Memo, Payload, Payloads, RunId, RunKey, SearchAttrValue, SearchAttributes};
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
            workflow_id: edge.workflow_id.clone(),
            run_id: String::new(),
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
            group_by: edge.group_by.clone().into_iter().collect(),
        };
        let roundtrip = count_request_to_edge(proto).unwrap();
        prop_assert_eq!(roundtrip, edge);
    }

    #[test]
    fn property_start_response_projection(edge in arb_start_response()) {
        let proto = start_response_to_proto(edge.clone());
        prop_assert_eq!(proto.run_id, edge.run_id.0.to_string());
    }

    #[test]
    fn property_poll_response_projection(edge in arb_poll_response()) {
        let proto = poll_response_to_proto(edge.clone());
        prop_assert_eq!(proto.task_token, edge.task_token);
        prop_assert_eq!(proto.started_event_id, edge.started_event_id);
        prop_assert_eq!(proto.sticky, false);
        prop_assert_eq!(proto.history_blob, Vec::<u8>::new());
        let execution = proto.workflow_execution.expect("workflow_execution");
        prop_assert_eq!(execution.workflow_id, edge.payload.workflow_id);
        prop_assert_eq!(execution.run_id, edge.payload.run_key.0.to_string());
    }

    #[test]
    fn property_describe_response_projection(edge in arb_description()) {
        let proto = describe_response_to_proto(edge.clone());
        let info = proto.execution.expect("execution");
        prop_assert_eq!(info.namespace, edge.namespace);
        prop_assert_eq!(info.workflow_id, edge.workflow_id);
        prop_assert_eq!(info.run_id, edge.run_id.0.to_string());
        prop_assert_eq!(info.workflow_type, edge.workflow_type);
        prop_assert_eq!(info.task_queue.expect("task_queue").name, edge.task_queue);
        prop_assert_eq!(info.status, execution_status_to_proto(edge.status));
        prop_assert_eq!(info.history_length, edge.history_length);
        prop_assert_eq!(info.state_transition_count, edge.state_transition_count);
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
            WorkflowCommand::ScheduleActivity { .. }
            | WorkflowCommand::StartTimer { .. }
            | WorkflowCommand::UpsertMemo(_)
            | WorkflowCommand::UpsertSearchAttributes(_)
            | WorkflowCommand::CompleteWorkflow { .. }
            | WorkflowCommand::FailWorkflow { .. } => {
                let proto = workflow_command_to_proto(&cmd).unwrap();
                let roundtrip = proto_command_to_workflow_command(proto).unwrap();
                prop_assert_eq!(roundtrip, cmd);
            }
            WorkflowCommand::CancelWorkflow
            | WorkflowCommand::RequestCancelActivity { .. }
            | WorkflowCommand::CancelTimer { .. }
            | WorkflowCommand::RequestNewWorkflowTask => {
                prop_assert!(workflow_command_to_proto(&cmd).is_err());
            }
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
}

#[test]
fn property_proto_conversion_status_maps_to_invalid_argument() {
    let status = proto_conversion_status(tokeira_proto::conversions::ProtoConversionError::MissingField("field"));
    assert_eq!(status.code(), Code::InvalidArgument);
}

fn start_request_to_proto(edge: &StartWorkflowExecutionRequest) -> workflowservice::StartWorkflowExecutionRequest {
    workflowservice::StartWorkflowExecutionRequest {
        namespace: edge.namespace.clone(),
        workflow_id: edge.workflow_id.clone(),
        workflow_type: edge.workflow_type.clone(),
        task_queue: Some(tokeira_proto::common::TaskQueue {
            name: edge.task_queue.clone(),
        }),
        input: Some(payloads_from_domain(&edge.input)),
        request_id: edge.request_id.clone().unwrap_or_default(),
        memo: Some(memo_from_domain(&edge.memo)),
        search_attributes: Some(search_attributes_from_domain(&edge.search_attributes)),
    }
}

fn signal_request_to_proto(edge: &SignalWorkflowExecutionRequest) -> workflowservice::SignalWorkflowExecutionRequest {
    workflowservice::SignalWorkflowExecutionRequest {
        namespace: edge.namespace.clone(),
        workflow_id: edge.workflow_id.clone(),
        run_id: String::new(),
        signal_name: edge.signal_name.clone(),
        input: Some(payloads_from_domain(&edge.input)),
        request_id: edge.request_id.clone().unwrap_or_default(),
        identity: edge.identity.clone().unwrap_or_default(),
    }
}

fn poll_request_to_proto(edge: &PollWorkflowTaskQueueRequest) -> workflowservice::PollWorkflowTaskQueueRequest {
    workflowservice::PollWorkflowTaskQueueRequest {
        namespace: edge.namespace.clone(),
        task_queue: Some(tokeira_proto::common::TaskQueue {
            name: edge.task_queue.clone(),
        }),
        identity: edge.worker_identity.clone(),
        deployment: String::new(),
        build_id: String::new(),
    }
}

fn execution_status_to_proto(status: ExecutionStatus) -> i32 {
    match status {
        ExecutionStatus::Running => WorkflowExecutionStatus::Running as i32,
        ExecutionStatus::Completed => WorkflowExecutionStatus::Completed as i32,
        ExecutionStatus::Failed => WorkflowExecutionStatus::Failed as i32,
        ExecutionStatus::Cancelled => WorkflowExecutionStatus::Canceled as i32,
        ExecutionStatus::Terminated => WorkflowExecutionStatus::Terminated as i32,
    }
}

fn expected_code(err: &EdgeError) -> Code {
    match err {
        EdgeError::BadRequest(_) => Code::InvalidArgument,
        EdgeError::Unauthorized(_) => Code::Unauthenticated,
        EdgeError::Forbidden { .. } => Code::PermissionDenied,
        EdgeError::NamespaceNotFound(_) | EdgeError::WorkflowNotFound { .. } => Code::NotFound,
        EdgeError::NamespaceDeleted(_) => Code::FailedPrecondition,
        EdgeError::TooManyLongPolls => Code::ResourceExhausted,
        EdgeError::LongPollAdmissionTimeout => Code::DeadlineExceeded,
        EdgeError::RemoteRouteUnsupported { .. } => Code::Unavailable,
        EdgeError::Internal(_) => Code::Internal,
    }
}

fn arb_small_string() -> impl Strategy<Value = String> {
    prop::collection::vec(prop::char::range('a', 'z'), 1..8)
        .prop_map(|chars| chars.into_iter().collect())
}

fn arb_header_key() -> impl Strategy<Value = String> {
    prop::collection::vec(prop::char::range('a', 'z'), 1..8)
        .prop_map(|chars| format!("x-{}", chars.into_iter().collect::<String>()))
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
        .prop_map(|(metadata, data)| Payload { metadata, data })
}

fn arb_payloads() -> impl Strategy<Value = Payloads> {
    prop::collection::vec(arb_payload(), 0..4).prop_map(Payloads)
}

fn arb_memo() -> impl Strategy<Value = Memo> {
    prop::collection::btree_map(arb_small_string(), arb_payload(), 0..3).prop_map(Memo)
}

fn arb_search_attr_value() -> impl Strategy<Value = SearchAttrValue> {
    prop_oneof![
        arb_small_string().prop_map(SearchAttrValue::Keyword),
        prop::collection::vec(arb_small_string(), 0..4).prop_map(SearchAttrValue::KeywordList),
        any::<i64>().prop_map(SearchAttrValue::Int),
        any::<bool>().prop_map(SearchAttrValue::Bool),
        (-1_000_000i64..1_000_000i64).prop_map(|v| SearchAttrValue::Double(v as f64 / 10.0)),
        (0i64..4_000_000_000i64).prop_map(|secs| SearchAttrValue::Datetime(OffsetDateTime::from_unix_timestamp(secs).unwrap())),
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
    )
        .prop_map(|(namespace, workflow_id, workflow_type, task_queue, input, request_id, memo, search_attributes)| StartWorkflowExecutionRequest {
            namespace,
            workflow_id,
            workflow_type,
            task_queue,
            input,
            request_id,
            memo,
            search_attributes,
            identity: None,
            run_key: None,
            run_id: None,
            now: None,
        })
}

fn arb_signal_request() -> impl Strategy<Value = SignalWorkflowExecutionRequest> {
    (
        arb_small_string(),
        arb_small_string(),
        arb_small_string(),
        arb_payloads(),
        prop::option::of(arb_small_string()),
        prop::option::of(arb_small_string()),
    )
        .prop_map(|(namespace, workflow_id, signal_name, input, request_id, identity)| SignalWorkflowExecutionRequest {
            namespace,
            workflow_id,
            signal_name,
            input,
            request_id,
            identity,
            now: None,
        })
}

fn arb_poll_request() -> impl Strategy<Value = PollWorkflowTaskQueueRequest> {
    (arb_small_string(), arb_small_string(), arb_small_string())
        .prop_map(|(namespace, task_queue, worker_identity)| PollWorkflowTaskQueueRequest {
            namespace,
            task_queue,
            worker_identity,
            sticky_run: None,
            timeout: Duration::from_secs(60),
            sticky_ttl: Duration::from_secs(30),
        })
}

fn arb_describe_request() -> impl Strategy<Value = DescribeWorkflowExecutionRequest> {
    (arb_small_string(), arb_small_string()).prop_map(|(namespace, workflow_id)| DescribeWorkflowExecutionRequest {
        namespace,
        workflow_id,
    })
}

fn arb_list_request() -> impl Strategy<Value = ListWorkflowExecutionsRequest> {
    (
        arb_small_string(),
        prop::option::of(arb_small_string()),
        0usize..32usize,
        prop::option::of(arb_small_string()),
    )
        .prop_map(|(namespace, query, page_size, next_page_token)| ListWorkflowExecutionsRequest {
            namespace,
            query,
            page_size,
            next_page_token,
        })
}

fn arb_count_request() -> impl Strategy<Value = CountWorkflowExecutionsRequest> {
    (
        arb_small_string(),
        prop::option::of(arb_small_string()),
        prop::option::of(arb_small_string()),
    )
        .prop_map(|(namespace, query, group_by)| CountWorkflowExecutionsRequest {
            namespace,
            query,
            group_by,
        })
}

fn arb_start_response() -> impl Strategy<Value = StartWorkflowExecutionResponse> {
    (any::<u128>(), any::<u128>(), any::<u64>(), any::<i64>()).prop_map(|(run_key, run_id, transition_seq, last_event_id)| StartWorkflowExecutionResponse {
        run_key: RunKey(Uuid::from_u128(run_key)),
        run_id: RunId(Uuid::from_u128(run_id)),
        transition_seq,
        last_event_id,
    })
}

fn arb_poll_response() -> impl Strategy<Value = PollWorkflowTaskQueueResponse> {
    (
        prop::collection::vec(any::<u8>(), 0..16),
        any::<i64>(),
        any::<u32>(),
        arb_small_string(),
        any::<u128>(),
        arb_small_string(),
    )
        .prop_map(|(task_token, started_event_id, attempt, workflow_id, run_key, task_queue)| PollWorkflowTaskQueueResponse {
            task_token,
            started_event_id,
            attempt,
            payload: WorkflowTaskPayloadDto {
                workflow_id,
                run_key: RunKey(Uuid::from_u128(run_key)),
                task_queue,
                history: Vec::new(),
            },
        })
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
        ),
    )
        .prop_map(|((namespace, workflow_id, run_key, run_id, workflow_type, task_queue, status), (start_time, close_time, history_length, state_transition_count, memo, search_attributes))| WorkflowExecutionDescription {
            namespace,
            workflow_id,
            run_key: RunKey(Uuid::from_u128(run_key)),
            run_id: RunId(Uuid::from_u128(run_id)),
            workflow_type,
            task_queue,
            status,
            start_time: start_time.map(|secs| OffsetDateTime::from_unix_timestamp(secs).unwrap()),
            close_time: close_time.map(|secs| OffsetDateTime::from_unix_timestamp(secs).unwrap()),
            history_length,
            state_transition_count,
            memo,
            search_attributes,
        })
}

fn arb_list_response() -> impl Strategy<Value = ListWorkflowExecutionsResponse> {
    (
        prop::collection::vec(arb_summary(), 0..4),
        prop::option::of(arb_small_string()),
    )
        .prop_map(|(executions, next_page_token)| ListWorkflowExecutionsResponse {
            executions,
            next_page_token,
        })
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
        .prop_map(|(namespace, workflow_id, run_id, workflow_type, task_queue, status, start_time, close_time)| WorkflowExecutionSummary {
            namespace,
            workflow_id,
            run_id: RunId(Uuid::from_u128(run_id)),
            workflow_type,
            task_queue,
            status,
            start_time: start_time.map(|secs| OffsetDateTime::from_unix_timestamp(secs).unwrap()),
            close_time: close_time.map(|secs| OffsetDateTime::from_unix_timestamp(secs).unwrap()),
        })
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
        Just(ExecutionStatus::Completed),
        Just(ExecutionStatus::Failed),
        Just(ExecutionStatus::Cancelled),
        Just(ExecutionStatus::Terminated),
    ]
}

fn arb_workflow_command() -> impl Strategy<Value = WorkflowCommand> {
    prop_oneof![
        (arb_small_string(), arb_small_string(), arb_payloads()).prop_map(|(activity_id, task_queue, input)| WorkflowCommand::ScheduleActivity {
            activity_id,
            task_queue: tokeira_types::TaskQueueName(task_queue),
            input,
            schedule_to_close_timeout: None,
            schedule_to_start_timeout: None,
            start_to_close_timeout: None,
            heartbeat_timeout: None,
        }),
        arb_memo().prop_map(WorkflowCommand::UpsertMemo),
        arb_search_attributes().prop_map(WorkflowCommand::UpsertSearchAttributes),
        arb_payloads().prop_map(|result| WorkflowCommand::CompleteWorkflow { result }),
        (arb_small_string(), prop::option::of(arb_payload())).prop_map(|(message, details)| WorkflowCommand::FailWorkflow { message, details }),
        Just(WorkflowCommand::CancelWorkflow),
        arb_small_string().prop_map(|activity_id| WorkflowCommand::RequestCancelActivity { activity_id }),
        arb_small_string().prop_map(|timer_id| WorkflowCommand::CancelTimer { timer_id }),
    ]
}

fn arb_edge_error() -> impl Strategy<Value = EdgeError> {
    prop_oneof![
        arb_small_string().prop_map(EdgeError::BadRequest),
        arb_small_string().prop_map(EdgeError::Unauthorized),
        (prop_oneof![Just("operator_read"), Just("operator_write"), Just("start_workflow_execution")], prop::option::of(arb_small_string()))
            .prop_map(|(action, namespace)| EdgeError::Forbidden { action, namespace }),
        arb_small_string().prop_map(EdgeError::NamespaceNotFound),
        arb_small_string().prop_map(EdgeError::NamespaceDeleted),
        (arb_small_string(), arb_small_string()).prop_map(|(namespace, workflow_id)| EdgeError::WorkflowNotFound { namespace, workflow_id }),
        any::<bool>().prop_map(|_| EdgeError::TooManyLongPolls),
        any::<bool>().prop_map(|_| EdgeError::LongPollAdmissionTimeout),
        arb_small_string().prop_map(|target| EdgeError::RemoteRouteUnsupported { target }),
        arb_small_string().prop_map(EdgeError::Internal),
    ]
}
