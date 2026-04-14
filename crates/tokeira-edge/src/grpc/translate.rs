use std::time::Duration;

use prost::Message as _;
use time::OffsetDateTime;
use tokeira_kernel::WorkflowCommand;
use tokeira_proto::{
    conversions::{
        ProtoConversionError,
        common::{
            headers_from_domain, headers_to_domain, memo_from_domain,
            memo_to_domain, payload_from_domain, payload_to_domain,
            payloads_from_domain, payloads_to_domain,
            search_attributes_from_domain, search_attributes_to_domain,
            task_queue_to_domain, to_proto_duration, to_proto_timestamp,
            workflow_execution_from_ids,
        },
    },
    enums, workflowservice,
    public::temporal::api::{
        command::v1 as command,
        namespace::v1 as namespace_proto,
        replication::v1 as replication_proto,
        workflow::v1 as workflow,
        failure::v1 as failure_proto,
    },
};
use tokeira_types::{
    ActivityTaskToken, ExecutionStatus, NamespaceId, Payload, Payloads,
    RetryPolicy, RunId, RunKey, TaskKind, WorkflowId, WorkflowType,
};
use uuid::Uuid;

use crate::translate::to_internal::namespace_id_for;
use crate::translate::{
    CountWorkflowExecutionsRequest, CountWorkflowExecutionsResponse,
    DeleteWorkflowExecutionRequest as EdgeDeleteWorkflowExecutionRequest,
    DescribeTaskQueueRequest as EdgeDescribeTaskQueueRequest,
    DescribeTaskQueueResponse as EdgeDescribeTaskQueueResponse,
    DescribeWorkflowExecutionRequest, ListNamespacesResponse as EdgeListNamespacesResponse,
    ListWorkflowExecutionsRequest, ListWorkflowExecutionsResponse,
    NamespaceDescription, PollWorkflowTaskQueueRequest,
    PollWorkflowTaskQueueResponse, ProtocolMessageDto, QueryResultDto,
    RespondWorkflowTaskCompletedRequest,
    ResetWorkflowExecutionRequest as EdgeResetWorkflowExecutionRequest,
    ResetWorkflowExecutionResponse as EdgeResetWorkflowExecutionResponse,
    RegisterNamespaceRequest as EdgeRegisterNamespaceRequest,
    RespondWorkflowTaskCompletedResponse, SignalWorkflowExecutionRequest,
    SignalWorkflowExecutionResponse, SignalWithStartWorkflowExecutionRequest as EdgeSignalWithStartWorkflowExecutionRequest,
    SignalWithStartWorkflowExecutionResponse as EdgeSignalWithStartWorkflowExecutionResponse, StartWorkflowExecutionRequest,
    StartWorkflowExecutionResponse, SystemInfo, WorkflowExecutionDescription,
    WorkflowExecutionSummary,
};
use tokeira_kernel::state::ParentClosePolicy;

const DEFAULT_POLL_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_STICKY_TTL: Duration = Duration::from_secs(30);

fn proto_duration_to_time(
    value: Option<&prost_types::Duration>,
) -> Option<time::Duration> {
    value.map(|duration| {
        time::Duration::seconds(duration.seconds)
            + time::Duration::nanoseconds(i64::from(duration.nanos))
    })
}

fn retry_policy_to_domain(
    value: &tokeira_proto::common::RetryPolicy,
) -> RetryPolicy {
    RetryPolicy {
        initial_interval: proto_duration_to_time(value.initial_interval.as_ref())
            .unwrap_or(time::Duration::ZERO),
        backoff_coefficient: if value.backoff_coefficient > 0.0 {
            value.backoff_coefficient
        } else {
            1.0
        },
        maximum_interval: proto_duration_to_time(value.maximum_interval.as_ref()),
        maximum_attempts: value.maximum_attempts.max(0) as u32,
        non_retryable_error_types: value.non_retryable_error_types.clone(),
    }
}

fn retry_policy_from_domain(
    value: &RetryPolicy,
) -> tokeira_proto::common::RetryPolicy {
    tokeira_proto::common::RetryPolicy {
        initial_interval: Some(to_proto_duration(value.initial_interval)),
        backoff_coefficient: value.backoff_coefficient,
        maximum_interval: value.maximum_interval.map(to_proto_duration),
        maximum_attempts: value.maximum_attempts as i32,
        non_retryable_error_types: value.non_retryable_error_types.clone(),
    }
}

fn failure_to_payload(value: &failure_proto::Failure) -> Payload {
    let mut metadata = std::collections::BTreeMap::new();
    metadata.insert(
        "encoding".to_string(),
        "temporal/failure+proto".to_string(),
    );
    Payload {
        data: value.encode_to_vec(),
        metadata,
    }
}

fn payload_to_failure(value: &Payload) -> failure_proto::Failure {
    failure_proto::Failure::decode(value.data.as_slice()).unwrap_or_else(|_| {
        failure_proto::Failure {
            message: String::from_utf8_lossy(&value.data).into_owned(),
            ..Default::default()
        }
    })
}

fn parent_close_policy_to_domain(value: i32) -> ParentClosePolicy {
    match value {
        2 => ParentClosePolicy::Abandon,
        3 => ParentClosePolicy::RequestCancel,
        _ => ParentClosePolicy::Terminate,
    }
}

fn parent_close_policy_from_domain(value: ParentClosePolicy) -> i32 {
    match value {
        ParentClosePolicy::Terminate => 1,
        ParentClosePolicy::Abandon => 2,
        ParentClosePolicy::RequestCancel => 3,
    }
}

fn namespace_name_to_domain(value: &str) -> NamespaceId {
    if value.is_empty() {
        NamespaceId(Uuid::nil())
    } else if let Ok(uuid) = Uuid::parse_str(value) {
        NamespaceId(uuid)
    } else {
        namespace_id_for(value)
    }
}

fn parse_run_id(value: &str) -> Result<RunId, ProtoConversionError> {
    Ok(RunId(Uuid::parse_str(value)?))
}

fn extract_conflict_policy(value: i32) -> tokeira_kernel::WorkflowIdConflictPolicy {
    match enums::WorkflowIdConflictPolicy::try_from(value).ok() {
        Some(enums::WorkflowIdConflictPolicy::UseExisting) => {
            tokeira_kernel::WorkflowIdConflictPolicy::UseExisting
        }
        Some(enums::WorkflowIdConflictPolicy::TerminateExisting) => {
            tokeira_kernel::WorkflowIdConflictPolicy::TerminateExisting
        }
        _ => tokeira_kernel::WorkflowIdConflictPolicy::Fail,
    }
}

fn extract_reuse_policy(value: i32) -> tokeira_kernel::WorkflowIdReusePolicy {
    match enums::WorkflowIdReusePolicy::try_from(value).ok() {
        Some(enums::WorkflowIdReusePolicy::AllowDuplicateFailedOnly) => {
            tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicateFailedOnly
        }
        Some(enums::WorkflowIdReusePolicy::RejectDuplicate) => {
            tokeira_kernel::WorkflowIdReusePolicy::RejectDuplicate
        }
        Some(enums::WorkflowIdReusePolicy::TerminateIfRunning) => {
            tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate
        }
        _ => tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
    }
}

fn migrate_reuse_policy(
    reuse: &mut tokeira_kernel::WorkflowIdReusePolicy,
    conflict: &mut tokeira_kernel::WorkflowIdConflictPolicy,
    raw_reuse_value: i32,
) {
    if matches!(
        enums::WorkflowIdReusePolicy::try_from(raw_reuse_value).ok(),
        Some(enums::WorkflowIdReusePolicy::TerminateIfRunning)
    ) {
        *reuse = tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate;
        *conflict = tokeira_kernel::WorkflowIdConflictPolicy::TerminateExisting;
    }
}

pub fn start_request_to_edge(
    req: workflowservice::StartWorkflowExecutionRequest,
) -> Result<StartWorkflowExecutionRequest, ProtoConversionError> {
    let task_queue =
        req.task_queue
            .as_ref()
            .ok_or(ProtoConversionError::MissingField(
                "StartWorkflowExecutionRequest.task_queue",
            ))?;

    let mut conflict_policy =
        extract_conflict_policy(req.workflow_id_conflict_policy);
    let mut reuse_policy = extract_reuse_policy(req.workflow_id_reuse_policy);
    migrate_reuse_policy(
        &mut reuse_policy,
        &mut conflict_policy,
        req.workflow_id_reuse_policy,
    );

    Ok(StartWorkflowExecutionRequest {
        namespace: req.namespace,
        workflow_id: req.workflow_id,
        workflow_type: req.workflow_type.map(|wt| wt.name).unwrap_or_default(),
        task_queue: task_queue.name.clone(),
        input: req
            .input
            .as_ref()
            .map(payloads_to_domain)
            .unwrap_or_default(),
        request_id: non_empty(req.request_id),
        memo: req.memo.as_ref().map(memo_to_domain).unwrap_or_default(),
        search_attributes: req
            .search_attributes
            .as_ref()
            .map(search_attributes_to_domain)
            .transpose()?
            .unwrap_or_default(),
        identity: non_empty(req.identity),
        workflow_execution_timeout: proto_duration_to_time(
            req.workflow_execution_timeout.as_ref(),
        ),
        workflow_run_timeout: proto_duration_to_time(req.workflow_run_timeout.as_ref()),
        workflow_task_timeout: proto_duration_to_time(req.workflow_task_timeout.as_ref()),
        retry_policy: req.retry_policy.as_ref().map(retry_policy_to_domain),
        conflict_policy,
        reuse_policy,
        header: req.header.as_ref().map(headers_to_domain),
        run_key: None,
        run_id: None,
        now: None,
    })
}

pub fn signal_request_to_edge(
    req: workflowservice::SignalWorkflowExecutionRequest,
) -> Result<SignalWorkflowExecutionRequest, ProtoConversionError> {
    let execution = req.workflow_execution.as_ref().ok_or(
        ProtoConversionError::MissingField("SignalWorkflowExecutionRequest.workflow_execution"),
    )?;
    Ok(SignalWorkflowExecutionRequest {
        namespace: req.namespace,
        workflow_id: execution.workflow_id.clone(),
        signal_name: req.signal_name,
        input: req
            .input
            .as_ref()
            .map(payloads_to_domain)
            .unwrap_or_default(),
        request_id: non_empty(req.request_id),
        identity: non_empty(req.identity),
        now: None,
    })
}

pub fn poll_request_to_edge(
    req: workflowservice::PollWorkflowTaskQueueRequest,
) -> Result<PollWorkflowTaskQueueRequest, ProtoConversionError> {
    let task_queue =
        req.task_queue
            .as_ref()
            .ok_or(ProtoConversionError::MissingField(
                "PollWorkflowTaskQueueRequest.task_queue",
            ))?;

    Ok(PollWorkflowTaskQueueRequest {
        namespace: req.namespace,
        task_queue: task_queue.name.clone(),
        worker_identity: req.identity,
        deployment: None,
        build_id: None,
        sticky_run: None,
        timeout: DEFAULT_POLL_TIMEOUT,
        sticky_ttl: DEFAULT_STICKY_TTL,
    })
}

pub fn respond_completed_request_to_edge(
    req: workflowservice::RespondWorkflowTaskCompletedRequest,
) -> Result<RespondWorkflowTaskCompletedRequest, ProtoConversionError> {
    Ok(RespondWorkflowTaskCompletedRequest {
        task_token: req.task_token,
        identity: req.identity,
        commands: req
            .commands
            .into_iter()
            .map(proto_command_to_workflow_command)
            .collect::<Result<Vec<_>, _>>()?,
        force_new_workflow_task: false,
        query_results: req
            .query_results
            .into_iter()
            .map(|(id, result)| {
                let dto = match enums::QueryResultType::try_from(result.result_type).unwrap_or(
                    enums::QueryResultType::Failed,
                ) {
                    enums::QueryResultType::Answered => QueryResultDto::Answered {
                        result: result.answer.as_ref().map(payloads_to_domain).unwrap_or_default(),
                    },
                    enums::QueryResultType::Failed
                    | enums::QueryResultType::Unspecified => QueryResultDto::Failed {
                        error_message: result.error_message,
                    },
                };
                Ok((id, dto))
            })
            .collect::<Result<_, ProtoConversionError>>()?,
        messages: req
            .messages
            .into_iter()
            .map(|message| {
                let body = message
                    .body
                    .map(|body| body.encode_to_vec())
                    .unwrap_or_default();
                Ok(ProtocolMessageDto {
                    id: message.id,
                    protocol_instance_id: message.protocol_instance_id,
                    body,
                    sequencing_event_id: match message.sequencing_id {
                        Some(
                            tokeira_proto::public::temporal::api::protocol::v1::message::SequencingId::EventId(event_id),
                        ) => Some(event_id),
                        Some(
                            tokeira_proto::public::temporal::api::protocol::v1::message::SequencingId::CommandIndex(command_index),
                        ) => Some(command_index),
                        None => None,
                    },
                })
            })
            .collect::<Result<Vec<_>, ProtoConversionError>>()?,
    })
}

pub fn describe_request_to_edge(
    req: workflowservice::DescribeWorkflowExecutionRequest,
) -> Result<DescribeWorkflowExecutionRequest, ProtoConversionError> {
    let execution = req.execution.as_ref().ok_or(
        ProtoConversionError::MissingField("DescribeWorkflowExecutionRequest.execution"),
    )?;
    Ok(DescribeWorkflowExecutionRequest {
        namespace: req.namespace,
        workflow_id: execution.workflow_id.clone(),
    })
}

pub fn list_request_to_edge(
    req: workflowservice::ListWorkflowExecutionsRequest,
) -> Result<ListWorkflowExecutionsRequest, ProtoConversionError> {
    Ok(ListWorkflowExecutionsRequest {
        namespace: req.namespace,
        query: non_empty(req.query),
        page_size: req.page_size.max(0) as usize,
        next_page_token: if req.next_page_token.is_empty() {
            None
        } else {
            Some(String::from_utf8_lossy(&req.next_page_token).into_owned())
        },
    })
}

pub fn count_request_to_edge(
    req: workflowservice::CountWorkflowExecutionsRequest,
) -> Result<CountWorkflowExecutionsRequest, ProtoConversionError> {
    Ok(CountWorkflowExecutionsRequest {
        namespace: req.namespace,
        query: non_empty(req.query),
        group_by: None,
    })
}

pub fn register_namespace_request_to_edge(
    req: workflowservice::RegisterNamespaceRequest,
) -> Result<EdgeRegisterNamespaceRequest, ProtoConversionError> {
    Ok(EdgeRegisterNamespaceRequest {
        namespace: req.namespace,
    })
}

pub fn start_response_to_proto(
    resp: StartWorkflowExecutionResponse,
) -> workflowservice::StartWorkflowExecutionResponse {
    workflowservice::StartWorkflowExecutionResponse {
        run_id: resp.run_id.0.to_string(),
        ..Default::default()
    }
}

pub fn signal_response_to_proto(
    _resp: SignalWorkflowExecutionResponse,
) -> workflowservice::SignalWorkflowExecutionResponse {
    workflowservice::SignalWorkflowExecutionResponse {}
}

pub fn poll_response_to_proto(
    resp: PollWorkflowTaskQueueResponse,
) -> workflowservice::PollWorkflowTaskQueueResponse {
    let workflow_execution = Some(workflow_execution_from_ids(
        &WorkflowId(resp.payload.workflow_id),
        run_id_from_run_key(resp.payload.run_key),
    ));

    let history_bytes =
        crate::translate::history_serializer::serialize_history(
            &resp.payload.history,
        );
    let history =
        tokeira_proto::history::History::decode(
            &history_bytes[..],
        )
        .ok();

    // Extract workflow_type from the first history event (WorkflowExecutionStarted)
    let workflow_type_name = resp.payload.history.first().and_then(|ev| {
        if let tokeira_kernel::event::HistoryEventKind::WorkflowExecutionStarted {
            ref workflow_type, ..
        } = ev.kind
        {
            Some(workflow_type.0.clone())
        } else {
            None
        }
    }).unwrap_or_default();

    workflowservice::PollWorkflowTaskQueueResponse {
        task_token: resp.task_token,
        workflow_execution,
        workflow_type: Some(tokeira_proto::common::WorkflowType {
            name: workflow_type_name,
        }),
        started_event_id: resp.started_event_id,
        attempt: resp.attempt as i32,
        history,
        workflow_execution_task_queue: Some(
            tokeira_proto::conversions::common::task_queue_from_domain(
                &tokeira_types::TaskQueueName(resp.payload.task_queue),
            ),
        ),
        queries: resp
            .queries
            .into_iter()
            .map(|(id, query)| {
                (
                    id,
                    tokeira_proto::public::temporal::api::query::v1::WorkflowQuery {
                        query_type: query.query_type,
                        query_args: Some(payloads_from_domain(&query.query_args)),
                        header: None,
                    },
                )
            })
            .collect(),
        messages: resp
            .messages
            .into_iter()
            .map(|message| {
                Ok(tokeira_proto::public::temporal::api::protocol::v1::Message {
                    id: message.id,
                    protocol_instance_id: message.protocol_instance_id,
                    body: prost_types::Any::decode(message.body.as_slice()).ok(),
                    sequencing_id: message.sequencing_event_id.map(
                        tokeira_proto::public::temporal::api::protocol::v1::message::SequencingId::EventId,
                    ),
                })
            })
            .collect::<Result<Vec<_>, ProtoConversionError>>()
            .unwrap_or_default(),
        ..Default::default()
    }
}

pub fn completed_response_to_proto(
    _resp: RespondWorkflowTaskCompletedResponse,
) -> workflowservice::RespondWorkflowTaskCompletedResponse {
    workflowservice::RespondWorkflowTaskCompletedResponse {
        ..Default::default()
    }
}

pub fn describe_response_to_proto(
    resp: WorkflowExecutionDescription,
) -> workflowservice::DescribeWorkflowExecutionResponse {
    workflowservice::DescribeWorkflowExecutionResponse {
        workflow_execution_info: Some(workflow_execution_info_from_description(resp)),
        ..Default::default()
    }
}

pub fn list_response_to_proto(
    resp: ListWorkflowExecutionsResponse,
) -> workflowservice::ListWorkflowExecutionsResponse {
    workflowservice::ListWorkflowExecutionsResponse {
        executions: resp
            .executions
            .into_iter()
            .map(workflow_execution_info_from_summary)
            .collect(),
        next_page_token: resp
            .next_page_token
            .map(|token| token.into_bytes())
            .unwrap_or_default(),
    }
}

pub fn count_response_to_proto(
    resp: CountWorkflowExecutionsResponse,
) -> workflowservice::CountWorkflowExecutionsResponse {
    use workflowservice::count_workflow_executions_response::AggregationGroup;
    workflowservice::CountWorkflowExecutionsResponse {
        count: resp.total_count,
        groups: resp
            .groups
            .into_iter()
            .map(|group| AggregationGroup {
                group_values: vec![tokeira_proto::common::Payload {
                    data: group.value.into_bytes(),
                    ..Default::default()
                }],
                count: group.count,
            })
            .collect(),
    }
}

pub fn cluster_info_to_proto(
    resp: crate::operator_service::ClusterInfo,
) -> workflowservice::GetClusterInfoResponse {
    workflowservice::GetClusterInfoResponse {
        supported_clients: std::collections::BTreeMap::new(),
        server_version: resp.version.clone(),
        cluster_id: resp.cluster_name.clone(),
        version_info: None,
        cluster_name: resp.cluster_name,
        history_shard_count: 0,
        persistence_store: "in-memory".to_string(),
        visibility_store: "in-memory".to_string(),
    }
}

pub fn system_info_to_proto(
    resp: SystemInfo,
) -> workflowservice::GetSystemInfoResponse {
    workflowservice::GetSystemInfoResponse {
        server_version: resp.server_version,
        capabilities: Some(
            workflowservice::get_system_info_response::Capabilities {
                signal_and_query_header: resp
                    .capabilities
                    .signal_and_query_header,
                internal_error_differentiation: resp
                    .capabilities
                    .internal_error_differentiation,
                activity_failure_include_heartbeat: resp
                    .capabilities
                    .activity_failure_include_heartbeat,
                supports_schedules: resp.capabilities.supports_schedules,
                encoded_failure_attributes: resp
                    .capabilities
                    .encoded_failure_attributes,
                build_id_based_versioning: resp
                    .capabilities
                    .build_id_based_versioning,
                upsert_memo: resp.capabilities.upsert_memo,
                eager_workflow_start: resp.capabilities.eager_workflow_start,
                sdk_metadata: resp.capabilities.sdk_metadata,
                count_group_by_execution_status: resp
                    .capabilities
                    .count_group_by_execution_status,
                nexus: resp.capabilities.nexus,
            },
        ),
    }
}

pub fn namespace_to_proto(
    namespace: NamespaceDescription,
) -> workflowservice::DescribeNamespaceResponse {
    workflowservice::DescribeNamespaceResponse {
        namespace_info: Some(namespace_proto::NamespaceInfo {
            name: namespace.name,
            state: if namespace.deleted {
                enums::NamespaceState::Deleted as i32
            } else {
                enums::NamespaceState::Registered as i32
            },
            description: String::new(),
            owner_email: String::new(),
            data: std::collections::BTreeMap::new(),
            id: namespace.namespace_id.unwrap_or_default(),
            capabilities: Some(
                namespace_proto::namespace_info::Capabilities {
                    eager_workflow_start: false,
                    sync_update: true,
                    async_update: true,
                },
            ),
            supports_schedules: false,
        }),
        config: Some(namespace_proto::NamespaceConfig {
            workflow_execution_retention_ttl: None,
            bad_binaries: None,
            history_archival_state: 0,
            history_archival_uri: String::new(),
            visibility_archival_state: 0,
            visibility_archival_uri: String::new(),
            custom_search_attribute_aliases: std::collections::BTreeMap::new(),
        }),
        replication_config: Some(
            replication_proto::NamespaceReplicationConfig {
                active_cluster_name: "local".to_string(),
                clusters: Vec::new(),
                state: 0,
            },
        ),
        failover_version: 0,
        is_global_namespace: namespace.is_global,
        failover_history: Vec::new(),
    }
}

pub fn list_namespaces_to_proto(
    resp: EdgeListNamespacesResponse,
) -> workflowservice::ListNamespacesResponse {
    workflowservice::ListNamespacesResponse {
        namespaces: resp
            .namespaces
            .into_iter()
            .map(namespace_to_proto)
            .collect(),
        next_page_token: resp
            .next_page_token
            .map(|token| token.into_bytes())
            .unwrap_or_default(),
    }
}

pub fn get_history_request_to_edge(
    req: workflowservice::GetWorkflowExecutionHistoryRequest,
) -> Result<
    crate::translate::GetWorkflowExecutionHistoryRequest,
    ProtoConversionError,
> {
    let execution = req.execution.as_ref().ok_or(
        ProtoConversionError::MissingField(
            "GetWorkflowExecutionHistoryRequest.execution",
        ),
    )?;
    Ok(
        crate::translate::GetWorkflowExecutionHistoryRequest {
            namespace: req.namespace,
            workflow_id: execution
                .workflow_id
                .clone(),
            run_id: non_empty(
                execution.run_id.clone(),
            ),
            maximum_page_size: req
                .maximum_page_size
                .max(0)
                as usize,
            wait_new_event: req.wait_new_event,
            history_event_filter_type: req.history_event_filter_type,
            next_page_token: req.next_page_token,
        },
    )
}

pub fn get_history_reverse_request_to_edge(
    req: workflowservice::GetWorkflowExecutionHistoryReverseRequest,
) -> Result<
    crate::translate::GetWorkflowExecutionHistoryReverseRequest,
    ProtoConversionError,
> {
    let execution = req.execution.as_ref().ok_or(
        ProtoConversionError::MissingField(
            "GetWorkflowExecutionHistoryReverseRequest.execution",
        ),
    )?;
    Ok(crate::translate::GetWorkflowExecutionHistoryReverseRequest {
        namespace: req.namespace,
        workflow_id: execution.workflow_id.clone(),
        run_id: non_empty(execution.run_id.clone()),
        maximum_page_size: req.maximum_page_size.max(0) as usize,
        next_page_token: req.next_page_token,
    })
}

pub fn get_history_response_to_proto(
    resp: crate::translate::GetWorkflowExecutionHistoryResponse,
    filter_type: i32,
) -> workflowservice::GetWorkflowExecutionHistoryResponse {
    use prost::Message;
    let history_bytes =
        crate::translate::history_serializer::serialize_history(
            &resp.history,
        );
    let mut history =
        tokeira_proto::history::History::decode(
            &history_bytes[..],
        )
        .ok();

    // HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT = 2
    if filter_type == 2
        && let Some(ref mut h) = history
    {
        h.events.retain(|event| is_close_event(event.event_type));
    }

    workflowservice::GetWorkflowExecutionHistoryResponse {
        history,
        // Only set next_page_token when there are genuinely more events to
        // paginate. For close-event filtered responses or complete histories,
        // an empty token tells the SDK "you have everything."
        next_page_token: if filter_type == 2 {
            // Close-event filter: the SDK only needs the close event(s).
            // Never paginate — return empty token so the SDK stops.
            vec![]
        } else {
            resp.next_page_token
        },
        ..Default::default()
    }
}

pub fn get_history_reverse_response_to_proto(
    resp: crate::translate::GetWorkflowExecutionHistoryReverseResponse,
) -> workflowservice::GetWorkflowExecutionHistoryReverseResponse {
    use prost::Message;
    let history_bytes =
        crate::translate::history_serializer::serialize_history(&resp.history);
    let history =
        tokeira_proto::history::History::decode(&history_bytes[..]).ok();

    workflowservice::GetWorkflowExecutionHistoryReverseResponse {
        history,
        next_page_token: resp.next_page_token,
        ..Default::default()
    }
}

pub fn describe_task_queue_request_to_edge(
    req: workflowservice::DescribeTaskQueueRequest,
) -> Result<EdgeDescribeTaskQueueRequest, ProtoConversionError> {
    let task_queue = req.task_queue.as_ref().ok_or(
        ProtoConversionError::MissingField(
            "DescribeTaskQueueRequest.task_queue",
        ),
    )?;

    let task_kind = match req.task_queue_type {
        x if x == enums::TaskQueueType::Activity as i32 => TaskKind::Activity,
        _ => TaskKind::Workflow,
    };

    Ok(EdgeDescribeTaskQueueRequest {
        namespace: req.namespace,
        task_queue: task_queue.name.clone(),
        task_kind,
        include_status: req.include_task_queue_status,
    })
}

pub fn describe_task_queue_response_to_proto(
    resp: EdgeDescribeTaskQueueResponse,
) -> workflowservice::DescribeTaskQueueResponse {
    workflowservice::DescribeTaskQueueResponse {
        pollers: resp
            .pollers
            .into_iter()
            .map(|poller| {
                tokeira_proto::public::temporal::api::taskqueue::v1::PollerInfo {
                    last_access_time: poller
                        .last_access_time
                        .map(to_proto_timestamp),
                    identity: poller.identity,
                    rate_per_second: poller.rate_per_second,
                    worker_version_capabilities: None,
                }
            })
            .collect(),
        task_queue_status: resp.backlog_count_hint.map(|backlog_count_hint| {
            tokeira_proto::public::temporal::api::taskqueue::v1::TaskQueueStatus {
                backlog_count_hint,
                ..Default::default()
            }
        }),
        versions_info: Default::default(),
    }
}

pub fn delete_request_to_edge(
    req: workflowservice::DeleteWorkflowExecutionRequest,
) -> Result<EdgeDeleteWorkflowExecutionRequest, ProtoConversionError> {
    let execution = req.workflow_execution.as_ref().ok_or(
        ProtoConversionError::MissingField(
            "DeleteWorkflowExecutionRequest.workflow_execution",
        ),
    )?;

    Ok(EdgeDeleteWorkflowExecutionRequest {
        namespace: req.namespace,
        workflow_id: execution.workflow_id.clone(),
        run_id: non_empty(execution.run_id.clone()),
    })
}

pub fn reset_request_to_edge(
    req: workflowservice::ResetWorkflowExecutionRequest,
) -> Result<EdgeResetWorkflowExecutionRequest, ProtoConversionError> {
    let execution = req.workflow_execution.as_ref().ok_or(
        ProtoConversionError::MissingField(
            "ResetWorkflowExecutionRequest.workflow_execution",
        ),
    )?;

    Ok(EdgeResetWorkflowExecutionRequest {
        namespace: req.namespace,
        workflow_id: execution.workflow_id.clone(),
        run_id: non_empty(execution.run_id.clone()),
        reason: req.reason,
        workflow_task_finish_event_id: req.workflow_task_finish_event_id,
        request_id: non_empty(req.request_id),
    })
}

pub fn reset_response_to_proto(
    resp: EdgeResetWorkflowExecutionResponse,
) -> workflowservice::ResetWorkflowExecutionResponse {
    workflowservice::ResetWorkflowExecutionResponse {
        run_id: resp.run_id.0.to_string(),
    }
}

pub fn signal_with_start_request_to_edge(
    req: workflowservice::SignalWithStartWorkflowExecutionRequest,
) -> Result<EdgeSignalWithStartWorkflowExecutionRequest, ProtoConversionError> {
    let task_queue =
        req.task_queue
            .as_ref()
            .ok_or(ProtoConversionError::MissingField(
                "SignalWithStartWorkflowExecutionRequest.task_queue",
            ))?;

    let mut conflict_policy = if matches!(
        enums::WorkflowIdConflictPolicy::try_from(req.workflow_id_conflict_policy).ok(),
        None | Some(enums::WorkflowIdConflictPolicy::Unspecified)
    ) {
        tokeira_kernel::WorkflowIdConflictPolicy::UseExisting
    } else {
        extract_conflict_policy(req.workflow_id_conflict_policy)
    };
    let mut reuse_policy = extract_reuse_policy(req.workflow_id_reuse_policy);
    migrate_reuse_policy(
        &mut reuse_policy,
        &mut conflict_policy,
        req.workflow_id_reuse_policy,
    );

    Ok(EdgeSignalWithStartWorkflowExecutionRequest {
        namespace: req.namespace,
        workflow_id: req.workflow_id,
        workflow_type: req.workflow_type.map(|wt| wt.name).unwrap_or_default(),
        task_queue: task_queue.name.clone(),
        input: req
            .input
            .as_ref()
            .map(payloads_to_domain)
            .unwrap_or_default(),
        request_id: non_empty(req.request_id),
        memo: req.memo.as_ref().map(memo_to_domain).unwrap_or_default(),
        search_attributes: req
            .search_attributes
            .as_ref()
            .map(search_attributes_to_domain)
            .transpose()?
            .unwrap_or_default(),
        identity: non_empty(req.identity),
        workflow_execution_timeout: proto_duration_to_time(
            req.workflow_execution_timeout.as_ref(),
        ),
        workflow_run_timeout: proto_duration_to_time(req.workflow_run_timeout.as_ref()),
        workflow_task_timeout: proto_duration_to_time(req.workflow_task_timeout.as_ref()),
        retry_policy: req.retry_policy.as_ref().map(retry_policy_to_domain),
        conflict_policy,
        reuse_policy,
        header: req.header.as_ref().map(headers_to_domain),
        signal_name: req.signal_name,
        signal_input: req
            .signal_input
            .as_ref()
            .map(payloads_to_domain)
            .unwrap_or_default(),
    })
}

pub fn signal_with_start_response_to_proto(
    resp: EdgeSignalWithStartWorkflowExecutionResponse,
) -> workflowservice::SignalWithStartWorkflowExecutionResponse {
    workflowservice::SignalWithStartWorkflowExecutionResponse {
        run_id: resp.run_id.0.to_string(),
        started: resp.started,
    }
}

fn is_close_event(event_type: i32) -> bool {
    use tokeira_proto::enums::EventType;
    matches!(
        EventType::try_from(event_type),
        Ok(EventType::WorkflowExecutionCompleted
            | EventType::WorkflowExecutionFailed
            | EventType::WorkflowExecutionTimedOut
            | EventType::WorkflowExecutionCanceled
            | EventType::WorkflowExecutionTerminated
            | EventType::WorkflowExecutionContinuedAsNew)
    )
}

pub fn proto_command_to_workflow_command(
    cmd: command::Command,
) -> Result<WorkflowCommand, ProtoConversionError> {
    use command::command::Attributes;

    match cmd.attributes {
        Some(Attributes::ScheduleActivityTaskCommandAttributes(attrs)) => {
            let task_queue =
                attrs
                    .task_queue
                    .as_ref()
                    .ok_or(ProtoConversionError::MissingField(
                        "ScheduleActivityCommandAttributes.task_queue",
                    ))?;
            Ok(WorkflowCommand::ScheduleActivity {
                activity_id: attrs.activity_id,
                activity_type: attrs
                    .activity_type
                    .as_ref()
                    .map(|activity_type| activity_type.name.clone())
                    .unwrap_or_default(),
                task_queue: task_queue_to_domain(task_queue),
                input: attrs
                    .input
                    .as_ref()
                    .map(payloads_to_domain)
                    .unwrap_or_default(),
                header: attrs.header.as_ref().map(headers_to_domain),
                retry_policy: attrs.retry_policy.as_ref().map(retry_policy_to_domain),
                deployment: None,
                build_id: None,
                schedule_to_close_timeout: proto_duration_to_time(
                    attrs.schedule_to_close_timeout.as_ref(),
                ),
                schedule_to_start_timeout: proto_duration_to_time(
                    attrs.schedule_to_start_timeout.as_ref(),
                ),
                start_to_close_timeout: proto_duration_to_time(
                    attrs.start_to_close_timeout.as_ref(),
                ),
                heartbeat_timeout: proto_duration_to_time(
                    attrs.heartbeat_timeout.as_ref(),
                ),
            })
        }
        Some(Attributes::StartTimerCommandAttributes(attrs)) => {
            let delay = attrs.start_to_fire_timeout
                .map(|d| time::Duration::new(d.seconds, d.nanos))
                .unwrap_or(time::Duration::ZERO);
            Ok(WorkflowCommand::StartTimer {
                timer_id: attrs.timer_id,
                fire_at: OffsetDateTime::now_utc() + delay,
            })
        }
        Some(Attributes::UpsertWorkflowSearchAttributesCommandAttributes(attrs)) => {
            Ok(WorkflowCommand::UpsertSearchAttributes(
                attrs
                    .search_attributes
                    .as_ref()
                    .map(search_attributes_to_domain)
                    .transpose()?
                    .unwrap_or_default(),
            ))
        }
        Some(Attributes::ModifyWorkflowPropertiesCommandAttributes(attrs)) => {
            Ok(WorkflowCommand::UpsertMemo(
                attrs.upserted_memo.as_ref().map(memo_to_domain).unwrap_or_default(),
            ))
        }
        Some(Attributes::CompleteWorkflowExecutionCommandAttributes(attrs)) => {
            Ok(WorkflowCommand::CompleteWorkflow {
                result: attrs
                    .result
                    .as_ref()
                    .map(payloads_to_domain)
                    .unwrap_or_default(),
            })
        }
        Some(Attributes::FailWorkflowExecutionCommandAttributes(attrs)) => {
            let message = attrs.failure.as_ref().map(|f| f.message.clone()).unwrap_or_default();
            Ok(WorkflowCommand::FailWorkflow { message, details: None })
        }
        Some(Attributes::RequestCancelActivityTaskCommandAttributes(attrs)) => {
            Ok(WorkflowCommand::RequestCancelActivity {
                activity_id: attrs.scheduled_event_id.to_string(),
            })
        }
        Some(Attributes::CancelTimerCommandAttributes(attrs)) => {
            Ok(WorkflowCommand::CancelTimer {
                timer_id: attrs.timer_id,
            })
        }
        Some(Attributes::CancelWorkflowExecutionCommandAttributes(_attrs)) => {
            Ok(WorkflowCommand::CancelWorkflow)
        }
        Some(Attributes::RequestCancelExternalWorkflowExecutionCommandAttributes(attrs)) => {
            Ok(WorkflowCommand::RequestCancelExternalWorkflowExecution {
                target_namespace_id: namespace_name_to_domain(&attrs.namespace),
                target_workflow_id: WorkflowId(attrs.workflow_id),
                target_run_id: non_empty(attrs.run_id)
                    .map(|run_id| parse_run_id(&run_id))
                    .transpose()?,
            })
        }
        Some(Attributes::RecordMarkerCommandAttributes(attrs)) => {
            Ok(WorkflowCommand::RecordMarker {
                marker_name: attrs.marker_name,
                details: attrs
                    .details
                    .iter()
                    .map(|(key, payloads)| (key.clone(), payloads_to_domain(payloads)))
                    .collect(),
                failure: attrs.failure.as_ref().map(failure_to_payload),
                header: attrs.header.as_ref().map(|header| headers_to_domain(header).0),
            })
        }
        Some(Attributes::ContinueAsNewWorkflowExecutionCommandAttributes(attrs)) => {
            let task_queue = attrs.task_queue.as_ref().ok_or(
                ProtoConversionError::MissingField(
                    "ContinueAsNewWorkflowExecutionCommandAttributes.task_queue",
                ),
            )?;
            Ok(WorkflowCommand::ContinueAsNew {
                new_run_id: RunId::new(),
                workflow_type: WorkflowType(
                    attrs
                        .workflow_type
                        .as_ref()
                        .map(|workflow_type| workflow_type.name.clone())
                        .unwrap_or_default(),
                ),
                task_queue: task_queue_to_domain(task_queue),
                input: attrs.input.as_ref().map(payloads_to_domain).unwrap_or_default(),
                memo: attrs.memo.as_ref().map(memo_to_domain).unwrap_or_default(),
                search_attributes: attrs
                    .search_attributes
                    .as_ref()
                    .map(search_attributes_to_domain)
                    .transpose()?
                    .unwrap_or_default(),
                workflow_execution_timeout: None,
                workflow_run_timeout: proto_duration_to_time(
                    attrs.workflow_run_timeout.as_ref(),
                ),
                workflow_task_timeout: proto_duration_to_time(
                    attrs.workflow_task_timeout.as_ref(),
                )
                .unwrap_or(time::Duration::seconds(10)),
            })
        }
        Some(Attributes::StartChildWorkflowExecutionCommandAttributes(attrs)) => {
            let task_queue = attrs.task_queue.as_ref().ok_or(
                ProtoConversionError::MissingField(
                    "StartChildWorkflowExecutionCommandAttributes.task_queue",
                ),
            )?;
            Ok(WorkflowCommand::StartChildWorkflow {
                child_workflow_id: WorkflowId(attrs.workflow_id),
                namespace_id: namespace_name_to_domain(&attrs.namespace),
                workflow_type: WorkflowType(
                    attrs
                        .workflow_type
                        .as_ref()
                        .map(|workflow_type| workflow_type.name.clone())
                        .unwrap_or_default(),
                ),
                task_queue: task_queue_to_domain(task_queue),
                input: attrs.input.as_ref().map(payloads_to_domain).unwrap_or_default(),
                parent_close_policy: parent_close_policy_to_domain(
                    attrs.parent_close_policy,
                ),
            })
        }
        Some(Attributes::SignalExternalWorkflowExecutionCommandAttributes(attrs)) => {
            let execution = attrs.execution.as_ref().ok_or(
                ProtoConversionError::MissingField(
                    "SignalExternalWorkflowExecutionCommandAttributes.execution",
                ),
            )?;
            Ok(WorkflowCommand::SignalExternalWorkflowExecution {
                target_namespace_id: namespace_name_to_domain(&attrs.namespace),
                target_workflow_id: WorkflowId(execution.workflow_id.clone()),
                target_run_id: non_empty(execution.run_id.clone())
                    .map(|run_id| parse_run_id(&run_id))
                    .transpose()?,
                signal_name: attrs.signal_name,
                input: attrs.input.as_ref().map(payloads_to_domain).unwrap_or_default(),
            })
        }
        Some(Attributes::ProtocolMessageCommandAttributes(_attrs)) => Err(
            ProtoConversionError::MissingField(
                "ProtocolMessage body is carried out-of-band from Command.attributes",
            ),
        ),
        Some(Attributes::ScheduleNexusOperationCommandAttributes(attrs)) => {
            Ok(WorkflowCommand::ScheduleNexusOperation {
                operation_id: Uuid::new_v4().to_string(),
                endpoint: attrs.endpoint,
                service: attrs.service,
                operation: attrs.operation,
                input: attrs
                    .input
                    .as_ref()
                    .map(|payload| Payloads(vec![payload_to_domain(payload)]))
                    .unwrap_or_default(),
                schedule_to_close_timeout: proto_duration_to_time(
                    attrs.schedule_to_close_timeout.as_ref(),
                ),
            })
        }
        Some(Attributes::RequestCancelNexusOperationCommandAttributes(attrs)) => {
            Ok(WorkflowCommand::CancelNexusOperation {
                scheduled_event_id: attrs.scheduled_event_id,
            })
        }
        None => Err(ProtoConversionError::MissingField("Command.attributes")),
    }
}

pub fn workflow_command_to_proto(
    cmd: &WorkflowCommand,
) -> Result<command::Command, ProtoConversionError> {
    use command::command::Attributes;
    let attributes = match cmd {
        WorkflowCommand::ScheduleActivity {
            activity_id,
            activity_type,
            task_queue,
            input,
            header,
            retry_policy,
            schedule_to_close_timeout,
            schedule_to_start_timeout,
            start_to_close_timeout,
            heartbeat_timeout,
            ..
        } => Some(Attributes::ScheduleActivityTaskCommandAttributes(
            command::ScheduleActivityTaskCommandAttributes {
                activity_id: activity_id.clone(),
                activity_type: Some(tokeira_proto::common::ActivityType {
                    name: activity_type.clone(),
                }),
                task_queue: Some(
                    tokeira_proto::conversions::common::task_queue_from_domain(
                        task_queue,
                    ),
                ),
                header: header.as_ref().map(headers_from_domain),
                input: Some(payloads_from_domain(input)),
                schedule_to_close_timeout: schedule_to_close_timeout
                    .map(to_proto_duration),
                schedule_to_start_timeout: schedule_to_start_timeout
                    .map(to_proto_duration),
                start_to_close_timeout: start_to_close_timeout
                    .map(to_proto_duration),
                heartbeat_timeout: heartbeat_timeout.map(to_proto_duration),
                retry_policy: retry_policy.as_ref().map(retry_policy_from_domain),
                ..Default::default()
            },
        )),
        WorkflowCommand::StartTimer { timer_id, fire_at } => {
            let now = OffsetDateTime::now_utc();
            let delay = *fire_at - now;
            let delay = if delay.is_negative() { time::Duration::ZERO } else { delay };
            Some(Attributes::StartTimerCommandAttributes(
                command::StartTimerCommandAttributes {
                    timer_id: timer_id.clone(),
                    start_to_fire_timeout: Some(to_proto_duration(delay)),
                },
            ))
        }
        WorkflowCommand::UpsertSearchAttributes(search_attributes) => {
            Some(Attributes::UpsertWorkflowSearchAttributesCommandAttributes(
                command::UpsertWorkflowSearchAttributesCommandAttributes {
                    search_attributes: Some(search_attributes_from_domain(
                        search_attributes,
                    )),
                },
            ))
        }
        WorkflowCommand::UpsertMemo(memo) => Some(Attributes::ModifyWorkflowPropertiesCommandAttributes(
            command::ModifyWorkflowPropertiesCommandAttributes {
                upserted_memo: Some(memo_from_domain(memo)),
            },
        )),
        WorkflowCommand::CompleteWorkflow { result } => {
            Some(Attributes::CompleteWorkflowExecutionCommandAttributes(
                command::CompleteWorkflowExecutionCommandAttributes {
                    result: Some(payloads_from_domain(result)),
                },
            ))
        }
        WorkflowCommand::FailWorkflow { message, details: _ } => {
            Some(Attributes::FailWorkflowExecutionCommandAttributes(
                command::FailWorkflowExecutionCommandAttributes {
                    failure: Some(failure_proto::Failure {
                        message: message.clone(),
                        ..Default::default()
                    }),
                },
            ))
        }
        WorkflowCommand::RequestCancelActivity { activity_id } => {
            Some(Attributes::RequestCancelActivityTaskCommandAttributes(
                command::RequestCancelActivityTaskCommandAttributes {
                    scheduled_event_id: activity_id.parse::<i64>().unwrap_or_default(),
                },
            ))
        }
        WorkflowCommand::CancelTimer { timer_id } => {
            Some(Attributes::CancelTimerCommandAttributes(
                command::CancelTimerCommandAttributes {
                    timer_id: timer_id.clone(),
                },
            ))
        }
        WorkflowCommand::CancelWorkflow => {
            Some(Attributes::CancelWorkflowExecutionCommandAttributes(
                command::CancelWorkflowExecutionCommandAttributes::default(),
            ))
        }
        WorkflowCommand::RecordMarker {
            marker_name,
            details,
            failure,
            header,
        } => Some(Attributes::RecordMarkerCommandAttributes(
            command::RecordMarkerCommandAttributes {
                marker_name: marker_name.clone(),
                details: details
                    .iter()
                    .map(|(key, payloads)| (key.clone(), payloads_from_domain(payloads)))
                    .collect(),
                header: header.as_ref().map(|header| {
                    headers_from_domain(&tokeira_types::Headers(header.clone()))
                }),
                failure: failure.as_ref().map(payload_to_failure),
            },
        )),
        WorkflowCommand::ContinueAsNew {
            workflow_type,
            task_queue,
            input,
            memo,
            search_attributes,
            workflow_run_timeout,
            workflow_task_timeout,
            ..
        } => Some(Attributes::ContinueAsNewWorkflowExecutionCommandAttributes(
            command::ContinueAsNewWorkflowExecutionCommandAttributes {
                workflow_type: Some(tokeira_proto::common::WorkflowType {
                    name: workflow_type.0.clone(),
                }),
                task_queue: Some(
                    tokeira_proto::conversions::common::task_queue_from_domain(
                        task_queue,
                    ),
                ),
                input: Some(payloads_from_domain(input)),
                workflow_run_timeout: workflow_run_timeout.map(to_proto_duration),
                workflow_task_timeout: Some(to_proto_duration(*workflow_task_timeout)),
                memo: Some(memo_from_domain(memo)),
                search_attributes: Some(search_attributes_from_domain(
                    search_attributes,
                )),
                ..Default::default()
            },
        )),
        WorkflowCommand::StartChildWorkflow {
            child_workflow_id,
            namespace_id,
            workflow_type,
            task_queue,
            input,
            parent_close_policy,
        } => Some(Attributes::StartChildWorkflowExecutionCommandAttributes(
            command::StartChildWorkflowExecutionCommandAttributes {
                namespace: namespace_id.0.to_string(),
                workflow_id: child_workflow_id.0.clone(),
                workflow_type: Some(tokeira_proto::common::WorkflowType {
                    name: workflow_type.0.clone(),
                }),
                task_queue: Some(
                    tokeira_proto::conversions::common::task_queue_from_domain(
                        task_queue,
                    ),
                ),
                input: Some(payloads_from_domain(input)),
                parent_close_policy: parent_close_policy_from_domain(
                    *parent_close_policy,
                ),
                ..Default::default()
            },
        )),
        WorkflowCommand::SignalExternalWorkflowExecution {
            target_namespace_id,
            target_workflow_id,
            target_run_id,
            signal_name,
            input,
        } => Some(Attributes::SignalExternalWorkflowExecutionCommandAttributes(
            command::SignalExternalWorkflowExecutionCommandAttributes {
                namespace: target_namespace_id.0.to_string(),
                execution: Some(workflow_execution_from_ids(
                    target_workflow_id,
                    target_run_id.unwrap_or(RunId(Uuid::nil())),
                )),
                signal_name: signal_name.clone(),
                input: Some(payloads_from_domain(input)),
                ..Default::default()
            },
        )),
        WorkflowCommand::RequestCancelExternalWorkflowExecution {
            target_namespace_id,
            target_workflow_id,
            target_run_id,
        } => Some(Attributes::RequestCancelExternalWorkflowExecutionCommandAttributes(
            command::RequestCancelExternalWorkflowExecutionCommandAttributes {
                namespace: target_namespace_id.0.to_string(),
                workflow_id: target_workflow_id.0.clone(),
                run_id: target_run_id
                    .map(|run_id| run_id.0.to_string())
                    .unwrap_or_default(),
                ..Default::default()
            },
        )),
        WorkflowCommand::ScheduleNexusOperation {
            endpoint,
            service,
            operation,
            input,
            schedule_to_close_timeout,
            ..
        } => Some(Attributes::ScheduleNexusOperationCommandAttributes(
            command::ScheduleNexusOperationCommandAttributes {
                endpoint: endpoint.clone(),
                service: service.clone(),
                operation: operation.clone(),
                input: input.0.first().map(payload_from_domain),
                schedule_to_close_timeout: schedule_to_close_timeout
                    .map(to_proto_duration),
                ..Default::default()
            },
        )),
        WorkflowCommand::CancelNexusOperation { scheduled_event_id } => {
            Some(Attributes::RequestCancelNexusOperationCommandAttributes(
                command::RequestCancelNexusOperationCommandAttributes {
                    scheduled_event_id: *scheduled_event_id,
                },
            ))
        }
        WorkflowCommand::ProtocolMessage { message_id, body: _ } => {
            Some(Attributes::ProtocolMessageCommandAttributes(
                command::ProtocolMessageCommandAttributes {
                    message_id: message_id.clone(),
                },
            ))
        }
        WorkflowCommand::UpdateCompleted { .. }
        | WorkflowCommand::UpdateRejected { .. }
        | WorkflowCommand::RequestNewWorkflowTask => {
            return Err(ProtoConversionError::MissingField(
                "WorkflowCommand has no proto Command equivalent",
            ));
        }
    };

    Ok(command::Command { attributes, ..Default::default() })
}

fn workflow_execution_info_from_description(
    value: WorkflowExecutionDescription,
) -> workflow::WorkflowExecutionInfo {
    workflow::WorkflowExecutionInfo {
        execution: Some(workflow_execution_from_ids(
            &WorkflowId(value.workflow_id),
            value.run_id,
        )),
        r#type: Some(tokeira_proto::common::WorkflowType {
            name: value.workflow_type,
        }),
        task_queue: value.task_queue,
        status: execution_status_to_proto(value.status),
        start_time: value.start_time.map(to_proto_timestamp),
        execution_time: None,
        close_time: value.close_time.map(to_proto_timestamp),
        history_length: value.history_length,
        state_transition_count: value.state_transition_count,
        memo: Some(memo_from_domain(&value.memo)),
        search_attributes: Some(search_attributes_from_domain(&value.search_attributes)),
        ..Default::default()
    }
}

fn workflow_execution_info_from_summary(
    value: WorkflowExecutionSummary,
) -> workflow::WorkflowExecutionInfo {
    workflow::WorkflowExecutionInfo {
        execution: Some(workflow_execution_from_ids(
            &WorkflowId(value.workflow_id),
            value.run_id,
        )),
        r#type: Some(tokeira_proto::common::WorkflowType {
            name: value.workflow_type,
        }),
        task_queue: value.task_queue,
        status: execution_status_to_proto(value.status),
        start_time: value.start_time.map(to_proto_timestamp),
        execution_time: None,
        close_time: value.close_time.map(to_proto_timestamp),
        history_length: value.history_length,
        state_transition_count: value.state_transition_count,
        memo: Some(memo_from_domain(&value.memo)),
        search_attributes: Some(search_attributes_from_domain(&value.search_attributes)),
        ..Default::default()
    }
}

fn execution_status_to_proto(value: ExecutionStatus) -> i32 {
    use enums::WorkflowExecutionStatus as Proto;

    match value {
        ExecutionStatus::Running => Proto::Running as i32,
        ExecutionStatus::Paused => Proto::Unspecified as i32,
        ExecutionStatus::Completed => Proto::Completed as i32,
        ExecutionStatus::Failed => Proto::Failed as i32,
        ExecutionStatus::Cancelled => Proto::Canceled as i32,
        ExecutionStatus::Terminated => Proto::Terminated as i32,
        ExecutionStatus::ContinuedAsNew => Proto::ContinuedAsNew as i32,
        ExecutionStatus::TimedOut => Proto::TimedOut as i32,
    }
}

fn _first_payload(payloads: Payloads) -> Option<tokeira_types::Payload> {
    payloads.0.into_iter().next()
}

fn non_empty(value: String) -> Option<String> {
    if value.is_empty() { None } else { Some(value) }
}

fn run_id_from_run_key(run_key: RunKey) -> RunId {
    RunId(run_key.0)
}

// ── Activity endpoint translations ──

const DEFAULT_QUERY_TIMEOUT: Duration =
    Duration::from_secs(10);
const DEFAULT_UPDATE_TIMEOUT: Duration =
    Duration::from_secs(30);

pub fn serialize_activity_token(
    token: &ActivityTaskToken,
) -> Vec<u8> {
    serde_json::to_vec(token).unwrap_or_default()
}

pub fn deserialize_activity_token(
    bytes: &[u8],
) -> Result<ActivityTaskToken, ProtoConversionError> {
    if bytes.is_empty() {
        return Err(ProtoConversionError::InvalidTaskToken(
            "task_token is empty".to_string(),
        ));
    }
    serde_json::from_slice(bytes).map_err(|e| {
        ProtoConversionError::InvalidTaskToken(e.to_string())
    })
}

pub fn poll_activity_request_to_edge(
    req: workflowservice::PollActivityTaskQueueRequest,
) -> Result<
    crate::translate::PollActivityTaskQueueRequest,
    ProtoConversionError,
> {
    let task_queue = req.task_queue.as_ref().ok_or(
        ProtoConversionError::MissingField(
            "PollActivityTaskQueueRequest.task_queue",
        ),
    )?;

    Ok(crate::translate::PollActivityTaskQueueRequest {
        namespace: req.namespace,
        task_queue: task_queue.name.clone(),
        worker_identity: req.identity,
        timeout: DEFAULT_POLL_TIMEOUT,
    })
}

pub fn poll_activity_response_to_proto(
    resp: crate::translate::PollActivityTaskQueueResponse,
) -> workflowservice::PollActivityTaskQueueResponse {
    let workflow_execution =
        Some(workflow_execution_from_ids(
            &WorkflowId(resp.workflow_id),
            run_id_from_run_key(resp.run_key),
        ));

    workflowservice::PollActivityTaskQueueResponse {
        task_token: resp.task_token,
        workflow_namespace: resp.workflow_namespace,
        workflow_type: Some(tokeira_proto::common::WorkflowType {
            name: resp.workflow_type,
        }),
        activity_id: resp.activity_id,
        activity_type: Some(tokeira_proto::common::ActivityType {
            name: resp.activity_type,
        }),
        header: resp.header.as_ref().map(headers_from_domain),
        input: Some(payloads_from_domain(&resp.input)),
        attempt: resp.attempt as i32,
        workflow_execution,
        retry_policy: resp.retry_policy.as_ref().map(retry_policy_from_domain),
        schedule_to_close_timeout: resp
            .schedule_to_close_timeout
            .and_then(|d| time::Duration::try_from(d).ok())
            .map(to_proto_duration),
        start_to_close_timeout: resp
            .start_to_close_timeout
            .and_then(|d| time::Duration::try_from(d).ok())
            .map(to_proto_duration),
        heartbeat_timeout: resp
            .heartbeat_timeout
            .and_then(|d| time::Duration::try_from(d).ok())
            .map(to_proto_duration),
        ..Default::default()
    }
}

pub fn respond_activity_completed_to_edge(
    req: workflowservice::RespondActivityTaskCompletedRequest,
) -> Result<
    crate::translate::RespondActivityTaskCompletedRequest,
    ProtoConversionError,
> {
    let token =
        deserialize_activity_token(&req.task_token)?;
    Ok(
        crate::translate::RespondActivityTaskCompletedRequest {
            token,
            result: req
                .result
                .as_ref()
                .map(payloads_to_domain)
                .unwrap_or_default(),
            identity: req.identity,
        },
    )
}

pub fn respond_activity_completed_to_proto(
) -> workflowservice::RespondActivityTaskCompletedResponse {
    workflowservice::RespondActivityTaskCompletedResponse {}
}

pub fn respond_activity_failed_to_edge(
    req: workflowservice::RespondActivityTaskFailedRequest,
) -> Result<
    crate::translate::RespondActivityTaskFailedRequest,
    ProtoConversionError,
> {
    let token =
        deserialize_activity_token(&req.task_token)?;
    let (failure_message, failure_error_type) = match req.failure {
        Some(f) => (f.message, non_empty(f.source)),
        None => (String::new(), None),
    };
    Ok(
        crate::translate::RespondActivityTaskFailedRequest {
            token,
            failure_message,
            failure_error_type,
            identity: req.identity,
        },
    )
}

pub fn respond_activity_failed_to_proto(
) -> workflowservice::RespondActivityTaskFailedResponse {
    workflowservice::RespondActivityTaskFailedResponse {
        ..Default::default()
    }
}

pub fn record_heartbeat_to_edge(
    req: workflowservice::RecordActivityTaskHeartbeatRequest,
) -> Result<
    crate::translate::RecordActivityTaskHeartbeatRequest,
    ProtoConversionError,
> {
    let token =
        deserialize_activity_token(&req.task_token)?;
    Ok(
        crate::translate::RecordActivityTaskHeartbeatRequest {
            token,
            identity: req.identity,
        },
    )
}

pub fn record_heartbeat_to_proto(
    resp: crate::translate::RecordActivityTaskHeartbeatResponse,
) -> workflowservice::RecordActivityTaskHeartbeatResponse {
    workflowservice::RecordActivityTaskHeartbeatResponse {
        cancel_requested: resp.cancel_requested,
        activity_paused: false,
    }
}

// ── Advanced workflow endpoint translations ──

pub fn terminate_request_to_edge(
    req: workflowservice::TerminateWorkflowExecutionRequest,
) -> Result<
    crate::translate::TerminateWorkflowExecutionRequest,
    ProtoConversionError,
> {
    let execution = req.workflow_execution.as_ref().ok_or(
        ProtoConversionError::MissingField("TerminateWorkflowExecutionRequest.workflow_execution"),
    )?;
    Ok(
        crate::translate::TerminateWorkflowExecutionRequest {
            namespace: req.namespace,
            workflow_id: execution.workflow_id.clone(),
            run_id: non_empty(execution.run_id.clone()),
            reason: req.reason,
            details: req
                .details
                .as_ref()
                .map(payloads_to_domain),
            identity: req.identity,
        },
    )
}

pub fn terminate_response_to_proto(
) -> workflowservice::TerminateWorkflowExecutionResponse {
    workflowservice::TerminateWorkflowExecutionResponse {}
}

pub fn cancel_request_to_edge(
    req: workflowservice::RequestCancelWorkflowExecutionRequest,
) -> Result<
    crate::translate::RequestCancelWorkflowExecutionRequest,
    ProtoConversionError,
> {
    let execution = req.workflow_execution.as_ref().ok_or(
        ProtoConversionError::MissingField("RequestCancelWorkflowExecutionRequest.workflow_execution"),
    )?;
    Ok(
        crate::translate::RequestCancelWorkflowExecutionRequest {
            namespace: req.namespace,
            workflow_id: execution.workflow_id.clone(),
            run_id: non_empty(execution.run_id.clone()),
            reason: req.reason,
            identity: req.identity,
        },
    )
}

pub fn cancel_response_to_proto(
) -> workflowservice::RequestCancelWorkflowExecutionResponse
{
    workflowservice::RequestCancelWorkflowExecutionResponse {}
}

pub fn query_request_to_edge(
    req: workflowservice::QueryWorkflowRequest,
) -> Result<
    crate::translate::QueryWorkflowRequest,
    ProtoConversionError,
> {
    let execution = req.execution.as_ref().ok_or(
        ProtoConversionError::MissingField(
            "QueryWorkflowRequest.execution",
        ),
    )?;
    let query = req.query.as_ref().ok_or(
        ProtoConversionError::MissingField(
            "QueryWorkflowRequest.query",
        ),
    )?;

    Ok(crate::translate::QueryWorkflowRequest {
        namespace: req.namespace,
        workflow_id: execution.workflow_id.clone(),
        run_id: non_empty(execution.run_id.clone()),
        query_type: query.query_type.clone(),
        query_args: query
            .query_args
            .as_ref()
            .map(payloads_to_domain)
            .unwrap_or_default(),
        timeout: DEFAULT_QUERY_TIMEOUT,
    })
}

pub fn query_response_to_proto(
    resp: crate::translate::QueryWorkflowResponse,
) -> workflowservice::QueryWorkflowResponse {
    workflowservice::QueryWorkflowResponse {
        query_result: resp
            .result
            .map(|p| payloads_from_domain(&p)),
        query_rejected: resp.rejected_status.map(|status| {
            tokeira_proto::public::temporal::api::query::v1::QueryRejected {
                status: execution_status_to_proto(status),
            }
        }),
    }
}

pub fn update_request_to_edge(
    req: workflowservice::UpdateWorkflowExecutionRequest,
) -> Result<
    crate::translate::UpdateWorkflowExecutionRequest,
    ProtoConversionError,
> {
    let execution = req.workflow_execution.as_ref().ok_or(
        ProtoConversionError::MissingField(
            "UpdateWorkflowExecutionRequest.workflow_execution",
        ),
    )?;

    let request = req.request.as_ref().ok_or(
        ProtoConversionError::MissingField(
            "UpdateWorkflowExecutionRequest.request",
        ),
    )?;
    let meta = request.meta.as_ref();
    let input_msg = request.input.as_ref();

    let wait_policy = match req.wait_policy {
        Some(wp) => match wp.lifecycle_stage {
            2 => crate::translate::UpdateWaitPolicyDto::Completed,
            _ => crate::translate::UpdateWaitPolicyDto::Accepted,
        },
        None => {
            crate::translate::UpdateWaitPolicyDto::Accepted
        }
    };

    Ok(crate::translate::UpdateWorkflowExecutionRequest {
        namespace: req.namespace,
        workflow_id: execution.workflow_id.clone(),
        run_id: non_empty(execution.run_id.clone()),
        update_id: meta.map(|m| m.update_id.clone()).unwrap_or_default(),
        update_name: input_msg.map(|i| i.name.clone()).unwrap_or_default(),
        input: input_msg
            .and_then(|i| i.args.as_ref())
            .map(payloads_to_domain)
            .unwrap_or_default(),
        wait_policy,
        timeout: DEFAULT_UPDATE_TIMEOUT,
    })
}

pub fn update_response_to_proto(
    resp: crate::translate::UpdateWorkflowExecutionResponse,
) -> workflowservice::UpdateWorkflowExecutionResponse {
    use tokeira_proto::public::temporal::api::update::v1 as update;

    match resp.outcome {
        crate::translate::UpdateOutcomeDto::Accepted {
            ..
        } => workflowservice::UpdateWorkflowExecutionResponse {
            outcome: None,
            ..Default::default()
        },
        crate::translate::UpdateOutcomeDto::Completed {
            result,
            ..
        } => workflowservice::UpdateWorkflowExecutionResponse {
            outcome: Some(update::Outcome {
                value: Some(update::outcome::Value::Success(
                    payloads_from_domain(&result),
                )),
            }),
            ..Default::default()
        },
        crate::translate::UpdateOutcomeDto::Rejected {
            failure,
            ..
        } => workflowservice::UpdateWorkflowExecutionResponse {
            outcome: Some(update::Outcome {
                value: Some(update::outcome::Value::Failure(
                    failure_proto::Failure {
                        message: failure,
                        ..Default::default()
                    },
                )),
            }),
            ..Default::default()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_proto::public::temporal::api::taskqueue::v1 as taskqueue;

    #[test]
    fn poll_request_applies_default_timeout_and_sticky_ttl() {
        let req = workflowservice::PollWorkflowTaskQueueRequest {
            namespace: "default".to_string(),
            task_queue: Some(taskqueue::TaskQueue {
                name: "queue".to_string(),
                ..Default::default()
            }),
            identity: "worker-1".to_string(),
            ..Default::default()
        };

        let edge = poll_request_to_edge(req).expect("poll request should convert");
        assert_eq!(edge.timeout, Duration::from_secs(60));
        assert_eq!(edge.sticky_ttl, Duration::from_secs(30));
    }

    #[test]
    fn poll_request_maps_empty_version_fields_to_none() {
        let edge = poll_request_to_edge(workflowservice::PollWorkflowTaskQueueRequest {
            namespace: "default".to_string(),
            task_queue: Some(taskqueue::TaskQueue {
                name: "queue".to_string(),
                ..Default::default()
            }),
            identity: "worker-1".to_string(),
            ..Default::default()
        })
        .expect("poll request should convert");

        assert_eq!(edge.deployment, None);
        assert_eq!(edge.build_id, None);
    }

    #[test]
    fn command_without_attributes_returns_missing_field() {
        let err = proto_command_to_workflow_command(command::Command {
            attributes: None,
            ..Default::default()
        })
        .expect_err("missing attributes should fail");

        match err {
            ProtoConversionError::MissingField(field) => {
                assert_eq!(field, "Command.attributes");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn empty_activity_poll_returns_default_response() {
        let default =
            workflowservice::PollActivityTaskQueueResponse::default();
        assert!(default.task_token.is_empty());
        assert!(default.activity_id.is_empty());
        assert_eq!(default.attempt, 0);
    }

    #[test]
    fn invalid_task_token_returns_error() {
        let err = deserialize_activity_token(b"not-json")
            .expect_err("should fail on invalid bytes");
        match err {
            ProtoConversionError::InvalidTaskToken(_) => {}
            other => {
                panic!("unexpected error: {other:?}")
            }
        }
    }

    #[test]
    fn empty_task_token_returns_error() {
        let err = deserialize_activity_token(b"")
            .expect_err("should fail on empty bytes");
        match err {
            ProtoConversionError::InvalidTaskToken(_) => {}
            other => {
                panic!("unexpected error: {other:?}")
            }
        }
    }

    #[test]
    fn heartbeat_cancel_requested_propagation() {
        let resp =
            crate::translate::RecordActivityTaskHeartbeatResponse {
                cancel_requested: true,
            };
        let proto = record_heartbeat_to_proto(resp);
        assert!(proto.cancel_requested);

        let resp =
            crate::translate::RecordActivityTaskHeartbeatResponse {
                cancel_requested: false,
            };
        let proto = record_heartbeat_to_proto(resp);
        assert!(!proto.cancel_requested);
    }

    #[test]
    fn activity_poll_default_timeout() {
        let req =
            workflowservice::PollActivityTaskQueueRequest {
                namespace: "default".to_string(),
                task_queue: Some(
                    taskqueue::TaskQueue {
                        name: "queue".to_string(),
                        ..Default::default()
                    },
                ),
                identity: "worker-1".to_string(),
                ..Default::default()
            };
        let edge =
            poll_activity_request_to_edge(req).unwrap();
        assert_eq!(
            edge.timeout,
            Duration::from_secs(60)
        );
    }

    #[test]
    fn terminate_with_details_translates() {
        use tokeira_proto::conversions::common::payloads_from_domain;
        let details = Payloads(vec![tokeira_types::Payload {
            data: b"stack-trace".to_vec(),
            metadata: Default::default(),
        }]);
        let req =
            workflowservice::TerminateWorkflowExecutionRequest {
                namespace: "ns".to_string(),
                workflow_execution: Some(tokeira_proto::common::WorkflowExecution {
                    workflow_id: "wf".to_string(),
                    run_id: String::new(),
                    ..Default::default()
                }),
                reason: "test".to_string(),
                details: Some(payloads_from_domain(&details)),
                identity: "admin".to_string(),
                ..Default::default()
            };
        let edge =
            terminate_request_to_edge(req).unwrap();
        assert!(edge.details.is_some());
        assert_eq!(
            edge.details.unwrap().0.len(),
            1
        );
    }

    #[test]
    fn cancel_with_empty_reason() {
        let req =
            workflowservice::RequestCancelWorkflowExecutionRequest {
                namespace: "ns".to_string(),
                workflow_execution: Some(tokeira_proto::common::WorkflowExecution {
                    workflow_id: "wf".to_string(),
                    run_id: String::new(),
                    ..Default::default()
                }),
                reason: String::new(),
                identity: "admin".to_string(),
                ..Default::default()
            };
        let edge =
            cancel_request_to_edge(req).unwrap();
        assert_eq!(edge.reason, "");
    }

    #[test]
    fn update_wait_policy_mapping() {
        use tokeira_proto::public::temporal::api::update::v1 as update;
        let wf_exec = || tokeira_proto::common::WorkflowExecution {
            workflow_id: "wf".to_string(),
            run_id: String::new(),
            ..Default::default()
        };
        let update_request = |name: &str, id: &str| update::Request {
            meta: Some(update::Meta {
                update_id: id.to_string(),
                identity: String::new(),
            }),
            input: Some(update::Input {
                name: name.to_string(),
                ..Default::default()
            }),
        };

        // lifecycle_stage 2 → Completed
        let req =
            workflowservice::UpdateWorkflowExecutionRequest {
                namespace: "ns".to_string(),
                workflow_execution: Some(wf_exec()),
                request: Some(update_request("handler", "u1")),
                wait_policy: Some(
                    update::WaitPolicy {
                        lifecycle_stage: 2,
                    },
                ),
                ..Default::default()
            };
        let edge = update_request_to_edge(req).unwrap();
        assert_eq!(
            edge.wait_policy,
            crate::translate::UpdateWaitPolicyDto::Completed
        );

        // lifecycle_stage 1 → Accepted
        let req =
            workflowservice::UpdateWorkflowExecutionRequest {
                namespace: "ns".to_string(),
                workflow_execution: Some(wf_exec()),
                request: Some(update_request("handler", "u2")),
                wait_policy: Some(
                    update::WaitPolicy {
                        lifecycle_stage: 1,
                    },
                ),
                ..Default::default()
            };
        let edge = update_request_to_edge(req).unwrap();
        assert_eq!(
            edge.wait_policy,
            crate::translate::UpdateWaitPolicyDto::Accepted
        );
    }

    #[test]
    fn query_default_timeout() {
        use tokeira_proto::public::temporal::api::query::v1 as query;
        let req = workflowservice::QueryWorkflowRequest {
            namespace: "ns".to_string(),
            execution: Some(
                tokeira_proto::common::WorkflowExecution {
                    workflow_id: "wf".to_string(),
                    run_id: String::new(),
                    ..Default::default()
                },
            ),
            query: Some(query::WorkflowQuery {
                query_type: "check".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let edge = query_request_to_edge(req).unwrap();
        assert_eq!(
            edge.timeout,
            Duration::from_secs(10)
        );
    }

    #[test]
    fn update_default_timeout() {
        use tokeira_proto::public::temporal::api::update::v1 as update;
        let req =
            workflowservice::UpdateWorkflowExecutionRequest {
                namespace: "ns".to_string(),
                workflow_execution: Some(
                    tokeira_proto::common::WorkflowExecution {
                        workflow_id: "wf".to_string(),
                        run_id: String::new(),
                        ..Default::default()
                    },
                ),
                request: Some(update::Request {
                    meta: Some(update::Meta {
                        update_id: "u1".to_string(),
                        identity: String::new(),
                    }),
                    input: Some(update::Input {
                        name: "handler".to_string(),
                        ..Default::default()
                    }),
                }),
                wait_policy: None,
                ..Default::default()
            };
        let edge = update_request_to_edge(req).unwrap();
        assert_eq!(
            edge.timeout,
            Duration::from_secs(30)
        );
    }

    // The upstream RespondWorkflowTaskCompletedResponse no longer has
    // workflow_completed/new_run_id fields, so the old property test
    // and related tests are removed.
}
