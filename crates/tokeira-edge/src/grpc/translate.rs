//! gRPC <-> edge DTO translation.
//!
//! This module is where we normalize the upstream Temporal proto surface into
//! the smaller edge-facing DTOs used by the rest of the crate. It is allowed to
//! carry compatibility policy: proto enums are migrated into kernel/runtime
//! policies here, missing transport fields receive edge defaults here, and
//! version-specific transport quirks are collapsed before they leak deeper into
//! the system.
//!
//! That also means defaults here must be treated carefully. A default is only
//! acceptable when the upstream API truly omits the concept and the edge needs
//! an internal policy value. If upstream already carries a field, translation
//! should preserve it rather than silently inventing a replacement.
//!
//! The update protocol is the most complex translation path: SDK completions
//! carry `ProtocolMessage` commands that reference entries in the `messages`
//! list by `message_id`. This module resolves those references and decodes
//! the `Any`-typed bodies into kernel `UpdateProtocolBody` variants before
//! the workflow service layer ever sees them.

use std::time::Duration;

use prost::Message as _;
use time::OffsetDateTime;
use tokeira_kernel::WorkflowCommand;
use tokeira_proto::{
    conversions::{
        ProtoConversionError,
        common::{
            failure_to_payload, headers_from_domain, headers_to_domain, memo_from_domain,
            memo_to_domain, payload_from_domain, payload_to_domain, payload_to_failure,
            payloads_from_domain, payloads_to_domain, search_attributes_from_domain,
            search_attributes_to_domain, task_queue_to_domain, to_proto_duration,
            to_proto_timestamp, workflow_execution_from_ids,
        },
    },
    enums,
    public::temporal::api::{
        command::v1 as command, common::v1 as proto_common, failure::v1 as failure_proto,
        namespace::v1 as namespace_proto, replication::v1 as replication_proto,
        taskqueue::v1 as taskqueue_proto, version::v1 as version_proto, workflow::v1 as workflow,
    },
    workflowservice,
};
use tokeira_runtime::{
    AssignmentRule, RedirectRule, TaskReachabilityType, VersioningMutation, VersioningRules,
};
use tokeira_types::{
    ActivityTaskToken, BuildId, DeploymentId, ExecutionStatus, NamespaceId, Payloads, RetryPolicy,
    RunId, RunKey, TaskKind, WorkflowId, WorkflowType,
};
use uuid::Uuid;

use crate::translate::{
    CountWorkflowExecutionsRequest, CountWorkflowExecutionsResponse,
    DeleteWorkflowExecutionRequest as EdgeDeleteWorkflowExecutionRequest,
    DescribeTaskQueueRequest as EdgeDescribeTaskQueueRequest,
    DescribeTaskQueueResponse as EdgeDescribeTaskQueueResponse, DescribeWorkflowExecutionRequest,
    ListNamespacesResponse as EdgeListNamespacesResponse, ListWorkflowExecutionsRequest,
    ListWorkflowExecutionsResponse, NamespaceDescription, PollWorkflowTaskQueueRequest,
    PollWorkflowTaskQueueResponse, ProtocolMessageDto, QueryResultDto,
    RegisterNamespaceRequest as EdgeRegisterNamespaceRequest,
    ResetWorkflowExecutionRequest as EdgeResetWorkflowExecutionRequest,
    ResetWorkflowExecutionResponse as EdgeResetWorkflowExecutionResponse,
    RespondWorkflowTaskCompletedRequest, RespondWorkflowTaskCompletedResponse,
    SignalWithStartWorkflowExecutionRequest as EdgeSignalWithStartWorkflowExecutionRequest,
    SignalWithStartWorkflowExecutionResponse as EdgeSignalWithStartWorkflowExecutionResponse,
    SignalWorkflowExecutionRequest, SignalWorkflowExecutionResponse, StartWorkflowExecutionRequest,
    StartWorkflowExecutionResponse, SystemInfo, TaskQueueConfig, VersioningOverride,
    WorkflowExecutionDescription, WorkflowExecutionSummary, to_internal::namespace_id_for,
};
use tokeira_kernel::state::ParentClosePolicy;

const DEFAULT_POLL_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_STICKY_TTL: Duration = Duration::from_secs(30);
const NON_RETRYABLE_ACTIVITY_SENTINEL: &str = "__tokeira_non_retryable__";

fn proto_duration_to_time(value: Option<&prost_types::Duration>) -> Option<time::Duration> {
    value.map(|duration| {
        time::Duration::seconds(duration.seconds)
            + time::Duration::nanoseconds(i64::from(duration.nanos))
    })
}

fn activity_retry_classification(failure: &failure_proto::Failure) -> (Option<String>, bool) {
    let mut cursor = Some(failure);
    while let Some(current) = cursor {
        if let Some(failure_proto::failure::FailureInfo::ApplicationFailureInfo(info)) =
            &current.failure_info
        {
            let error_type = non_empty(info.r#type.clone())
                .or_else(|| Some(NON_RETRYABLE_ACTIVITY_SENTINEL.to_string()));
            return (error_type, info.non_retryable);
        }
        cursor = current.cause.as_deref();
    }
    (None, false)
}

pub struct ParsedVersioningMutation {
    pub mutation: VersioningMutation,
    pub commit_build_id: Option<String>,
    pub commit_force: bool,
}

fn retry_policy_to_domain(value: &tokeira_proto::common::RetryPolicy) -> RetryPolicy {
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

fn retry_policy_from_domain(value: &RetryPolicy) -> tokeira_proto::common::RetryPolicy {
    tokeira_proto::common::RetryPolicy {
        initial_interval: Some(to_proto_duration(value.initial_interval)),
        backoff_coefficient: value.backoff_coefficient,
        maximum_interval: value.maximum_interval.map(to_proto_duration),
        maximum_attempts: value.maximum_attempts as i32,
        non_retryable_error_types: value.non_retryable_error_types.clone(),
    }
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

// v1.62-sync: reads deprecated `VersioningOverride.behavior` and `.deployment`
// fields for wire-compat with v0.4-era SDK clients. v1.62 replaces the flat
// shape with a `override` oneof; migration to the new shape is tracked under
// `runtime-worker-versioning`.
#[allow(deprecated)]
fn versioning_override_to_edge(
    override_: Option<workflow::VersioningOverride>,
) -> Result<Option<VersioningOverride>, ProtoConversionError> {
    let Some(override_) = override_ else {
        return Ok(None);
    };
    match enums::VersioningBehavior::try_from(override_.behavior).ok() {
        Some(enums::VersioningBehavior::Pinned) => {
            let deployment = override_
                .deployment
                .ok_or(ProtoConversionError::MissingField(
                    "VersioningOverride.deployment",
                ))?;
            if deployment.series_name.is_empty() || deployment.build_id.is_empty() {
                return Err(ProtoConversionError::MissingField(
                    "VersioningOverride.deployment.series_name/build_id",
                ));
            }
            Ok(Some(VersioningOverride::Pinned {
                deployment_series: deployment.series_name,
                build_id: deployment.build_id,
            }))
        }
        Some(enums::VersioningBehavior::AutoUpgrade) => Ok(Some(VersioningOverride::AutoUpgrade)),
        _ => Ok(None),
    }
}

pub fn start_request_to_edge(
    req: workflowservice::StartWorkflowExecutionRequest,
) -> Result<StartWorkflowExecutionRequest, ProtoConversionError> {
    let task_queue = req
        .task_queue
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "StartWorkflowExecutionRequest.task_queue",
        ))?;

    let mut conflict_policy = extract_conflict_policy(req.workflow_id_conflict_policy);
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
        request_eager_execution: req.request_eager_execution,
        workflow_execution_timeout: proto_duration_to_time(req.workflow_execution_timeout.as_ref()),
        workflow_run_timeout: proto_duration_to_time(req.workflow_run_timeout.as_ref()),
        workflow_task_timeout: proto_duration_to_time(req.workflow_task_timeout.as_ref()),
        retry_policy: req.retry_policy.as_ref().map(retry_policy_to_domain),
        conflict_policy,
        reuse_policy,
        header: req.header.as_ref().map(headers_to_domain),
        versioning_override: versioning_override_to_edge(req.versioning_override)?,
        run_key: None,
        run_id: None,
        now: None,
    })
}

pub fn signal_request_to_edge(
    req: workflowservice::SignalWorkflowExecutionRequest,
) -> Result<SignalWorkflowExecutionRequest, ProtoConversionError> {
    let execution = req
        .workflow_execution
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "SignalWorkflowExecutionRequest.workflow_execution",
        ))?;
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

// v1.62-sync: reads deprecated `PollWorkflowTaskQueueRequest.worker_version_capabilities`
// for wire-compat with v0.4-era SDK workers. v1.62 replaces it with
// `deployment_options`; migration is tracked under `runtime-worker-versioning`.
#[allow(deprecated)]
pub fn poll_request_to_edge(
    req: workflowservice::PollWorkflowTaskQueueRequest,
) -> Result<PollWorkflowTaskQueueRequest, ProtoConversionError> {
    let task_queue = req
        .task_queue
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "PollWorkflowTaskQueueRequest.task_queue",
        ))?;

    let (deployment, build_id) = req
        .worker_version_capabilities
        .as_ref()
        .filter(|caps| caps.use_versioning)
        .map(|caps| {
            let deployment = non_empty(caps.deployment_series_name.clone()).map(DeploymentId);
            let build_id = non_empty(caps.build_id.clone()).map(BuildId);
            (deployment, build_id)
        })
        .unwrap_or((None, None));

    Ok(PollWorkflowTaskQueueRequest {
        namespace: req.namespace,
        task_queue: task_queue.name.clone(),
        worker_identity: req.identity,
        deployment,
        build_id,
        sticky_run: None,
        timeout: DEFAULT_POLL_TIMEOUT,
        sticky_ttl: DEFAULT_STICKY_TTL,
    })
}

/// Translate a WFT completion from the proto wire format into the edge DTO.
///
/// The critical step here is resolving `ProtocolMessage` commands. The SDK
/// sends update protocol responses as two correlated pieces: a
/// `ProtocolMessageCommandAttributes` (carrying only a `message_id`) and a
/// corresponding entry in the top-level `messages` list (carrying the actual
/// `Any`-typed body). We index the messages by ID, then for each
/// `ProtocolMessage` command we pop the matching message and decode its body
/// via `resolve_protocol_message_body`. Any messages not claimed by a command
/// are passed through as-is for edge-layer processing.
pub fn respond_completed_request_to_edge(
    req: workflowservice::RespondWorkflowTaskCompletedRequest,
) -> Result<RespondWorkflowTaskCompletedRequest, ProtoConversionError> {
    // Index messages by ID so ProtocolMessage commands can
    // look up their corresponding message body.
    let mut messages_by_id: std::collections::HashMap<String, _> = req
        .messages
        .into_iter()
        .map(|m| (m.id.clone(), m))
        .collect();

    let mut commands = Vec::new();
    for cmd in req.commands {
        match proto_command_to_workflow_command(cmd) {
            Ok(WorkflowCommand::ProtocolMessage { message_id, .. }) => {
                // Resolve the body from the messages index.
                if let Some(msg) = messages_by_id.remove(&message_id) {
                    let body = msg
                        .body
                        .map(|body| body.encode_to_vec())
                        .unwrap_or_default();
                    commands.push(WorkflowCommand::ProtocolMessage {
                        message_id,
                        body: resolve_protocol_message_body(&body, msg.protocol_instance_id)?,
                    });
                }
            }
            Ok(cmd) => commands.push(cmd),
            Err(e) => return Err(e),
        }
    }

    // Remaining messages not referenced by commands go into
    // the DTO's messages field for edge-layer processing.
    let remaining_messages = messages_by_id
        .into_values()
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
        .collect::<Result<Vec<_>, ProtoConversionError>>()?;

    Ok(RespondWorkflowTaskCompletedRequest {
        task_token: req.task_token,
        identity: req.identity,
        sdk_metadata: req.sdk_metadata.map(|metadata| metadata.encode_to_vec()),
        worker_version: req.worker_version_stamp.map(|stamp| stamp.build_id),
        client_discards_speculative_with_events: req
            .capabilities
            .is_some_and(|capabilities| capabilities.discard_speculative_workflow_task_with_events),
        commands,
        force_create_new_workflow_task: req.force_create_new_workflow_task,
        return_new_workflow_task: req.return_new_workflow_task,
        query_results: req
            .query_results
            .into_iter()
            .map(|(id, result)| {
                let dto = match enums::QueryResultType::try_from(result.result_type)
                    .unwrap_or(enums::QueryResultType::Failed)
                {
                    enums::QueryResultType::Answered => QueryResultDto::Answered {
                        result: result
                            .answer
                            .as_ref()
                            .map(payloads_to_domain)
                            .unwrap_or_default(),
                    },
                    enums::QueryResultType::Failed | enums::QueryResultType::Unspecified => {
                        QueryResultDto::Failed {
                            error_message: result.error_message,
                        }
                    }
                };
                Ok((id, dto))
            })
            .collect::<Result<_, ProtoConversionError>>()?,
        messages: remaining_messages,
    })
}

/// Decode the `Any`-typed body of a protocol message into a kernel
/// `UpdateProtocolBody` variant.
///
/// The Temporal update protocol uses three message types — `Acceptance`,
/// `Response`, and `Rejection` — each wrapped in a `prost_types::Any`.
/// We match on the `type_url` suffix to determine which variant to decode.
/// `Response` bodies carry an `Outcome` that can be either `Success` or
/// `Failure`; a `Failure` outcome is mapped to `Rejected` because from the
/// kernel's perspective both represent terminal negative outcomes.
fn resolve_protocol_message_body(
    body_bytes: &[u8],
    protocol_instance_id: String,
) -> Result<tokeira_kernel::UpdateProtocolBody, ProtoConversionError> {
    use prost::Message as _;
    let any = prost_types::Any::decode(body_bytes)
        .map_err(|_| ProtoConversionError::MissingField("ProtocolMessage body decode failed"))?;
    match any.type_url.as_str() {
        url if url.ends_with("update.v1.Acceptance") => {
            Ok(tokeira_kernel::UpdateProtocolBody::Accepted {
                update_id: protocol_instance_id,
                update_name: String::new(),
                input: Payloads::default(),
            })
        }
        url if url.ends_with("update.v1.Response") => {
            let response = tokeira_proto::public::temporal::api::update::v1::Response::decode(
                any.value.as_slice(),
            )
            .map_err(|_| ProtoConversionError::MissingField("update.v1.Response decode failed"))?;
            match response.outcome.and_then(|o| o.value) {
                Some(
                    tokeira_proto::public::temporal::api::update::v1::outcome::Value::Success(
                        payloads,
                    ),
                ) => Ok(tokeira_kernel::UpdateProtocolBody::Completed {
                    update_id: protocol_instance_id,
                    result: payloads_to_domain(&payloads),
                }),
                Some(
                    tokeira_proto::public::temporal::api::update::v1::outcome::Value::Failure(
                        failure,
                    ),
                ) => Ok(tokeira_kernel::UpdateProtocolBody::Rejected {
                    update_id: protocol_instance_id,
                    failure: failure_to_payload(&failure),
                }),
                None => Err(ProtoConversionError::MissingField(
                    "update.v1.Response missing outcome",
                )),
            }
        }
        url if url.ends_with("update.v1.Rejection") => {
            let rejection = tokeira_proto::public::temporal::api::update::v1::Rejection::decode(
                any.value.as_slice(),
            )
            .map_err(|_| ProtoConversionError::MissingField("update.v1.Rejection decode failed"))?;
            Ok(tokeira_kernel::UpdateProtocolBody::Rejected {
                update_id: protocol_instance_id,
                failure: rejection
                    .failure
                    .as_ref()
                    .map(failure_to_payload)
                    .unwrap_or_else(|| {
                        failure_to_payload(&failure_proto::Failure {
                            message: "update rejected".to_string(),
                            ..Default::default()
                        })
                    }),
            })
        }
        _ => Err(ProtoConversionError::MissingField(
            "unknown protocol message type_url",
        )),
    }
}

pub fn describe_request_to_edge(
    req: workflowservice::DescribeWorkflowExecutionRequest,
) -> Result<DescribeWorkflowExecutionRequest, ProtoConversionError> {
    let execution = req
        .execution
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "DescribeWorkflowExecutionRequest.execution",
        ))?;
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
        started: resp.started,
        eager_workflow_task: resp.eager_workflow_task.map(poll_response_to_proto),
        ..Default::default()
    }
}

pub fn signal_response_to_proto(
    _resp: SignalWorkflowExecutionResponse,
) -> workflowservice::SignalWorkflowExecutionResponse {
    workflowservice::SignalWorkflowExecutionResponse { link: None }
}

/// Build the proto poll response from the edge DTO.
pub fn poll_response_to_proto(
    resp: PollWorkflowTaskQueueResponse,
) -> workflowservice::PollWorkflowTaskQueueResponse {
    let workflow_execution = Some(workflow_execution_from_ids(
        &WorkflowId(resp.payload.workflow_id),
        run_id_from_run_key(resp.payload.run_key),
    ));

    let history_bytes =
        crate::translate::history_serializer::serialize_history(&resp.payload.history);
    let history = tokeira_proto::history::History::decode(&history_bytes[..]).ok();

    // Extract workflow_type from the first history event (WorkflowExecutionStarted)
    let workflow_type_name = resp
        .payload
        .history
        .first()
        .and_then(|ev| {
            if let tokeira_kernel::event::HistoryEventKind::WorkflowExecutionStarted {
                ref workflow_type,
                ..
            } = ev.kind
            {
                Some(workflow_type.0.clone())
            } else {
                None
            }
        })
        .unwrap_or_default();

    workflowservice::PollWorkflowTaskQueueResponse {
        task_token: resp.task_token,
        workflow_execution,
        workflow_type: Some(tokeira_proto::common::WorkflowType {
            name: workflow_type_name,
        }),
        previous_started_event_id: resp.previous_started_event_id,
        started_event_id: resp.started_event_id,
        attempt: resp.attempt as i32,
        history,
        scheduled_time: resp.scheduled_time.map(to_proto_timestamp),
        started_time: resp.started_time.map(to_proto_timestamp),
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
                // The body is already an encoded prost_types::Any.
                // Decode it back to set on the proto Message.
                let body = match prost_types::Any::decode(message.body.as_slice()) {
                    Ok(any) => Some(any),
                    Err(e) => {
                        tracing::warn!("Failed to decode protocol message body: {e}");
                        None
                    }
                };
                Ok(tokeira_proto::public::temporal::api::protocol::v1::Message {
                    id: message.id,
                    protocol_instance_id: message.protocol_instance_id,
                    body,
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

/// Build the proto WFT completion response.
///
/// The `workflow_task` field carries an optional inline poll response for
/// "eager return": when the SDK sets `return_new_workflow_task = true` and
/// the edge has a query-only WFT ready, it piggybacks the next task on the
/// completion response to avoid an extra poll round-trip.
pub fn completed_response_to_proto(
    resp: RespondWorkflowTaskCompletedResponse,
) -> workflowservice::RespondWorkflowTaskCompletedResponse {
    workflowservice::RespondWorkflowTaskCompletedResponse {
        workflow_task: resp.workflow_task.map(poll_response_to_proto),
        activity_tasks: resp
            .activity_tasks
            .into_iter()
            .map(poll_activity_response_to_proto)
            .collect(),
        ..Default::default()
    }
}

pub fn describe_response_to_proto(
    resp: WorkflowExecutionDescription,
) -> workflowservice::DescribeWorkflowExecutionResponse {
    let pending_activities = resp
        .pending_activities
        .iter()
        .map(pending_activity_to_proto)
        .collect();
    let pending_children = resp
        .pending_children
        .iter()
        .map(pending_child_to_proto)
        .collect();
    let pending_workflow_task = resp
        .pending_workflow_task
        .as_ref()
        .map(pending_wft_to_proto);

    workflowservice::DescribeWorkflowExecutionResponse {
        workflow_execution_info: Some(workflow_execution_info_from_description(resp)),
        pending_activities,
        pending_children,
        pending_workflow_task,
        ..Default::default()
    }
}

fn pending_activity_to_proto(
    act: &crate::translate::PendingActivityDescription,
) -> workflow::PendingActivityInfo {
    workflow::PendingActivityInfo {
        activity_id: act.activity_id.clone(),
        activity_type: Some(proto_common::ActivityType {
            name: act.activity_type.clone(),
        }),
        state: if act.is_started {
            enums::PendingActivityState::Started as i32
        } else {
            enums::PendingActivityState::Scheduled as i32
        },
        attempt: act.attempt as i32,
        maximum_attempts: act.maximum_attempts as i32,
        scheduled_time: Some(to_proto_timestamp(act.scheduled_at)),
        last_started_time: act.started_at.map(to_proto_timestamp),
        ..Default::default()
    }
}

fn pending_child_to_proto(
    child: &crate::translate::PendingChildDescription,
) -> workflow::PendingChildExecutionInfo {
    workflow::PendingChildExecutionInfo {
        workflow_id: child.workflow_id.clone(),
        run_id: child.run_id.clone().unwrap_or_default(),
        workflow_type_name: child.workflow_type.clone(),
        initiated_id: child.initiated_event_id,
        parent_close_policy: parent_close_policy_from_domain(child.parent_close_policy),
    }
}

fn pending_wft_to_proto(
    wft: &crate::translate::PendingWorkflowTaskDescription,
) -> workflow::PendingWorkflowTaskInfo {
    workflow::PendingWorkflowTaskInfo {
        state: if wft.is_started {
            enums::PendingWorkflowTaskState::Started as i32
        } else {
            enums::PendingWorkflowTaskState::Scheduled as i32
        },
        scheduled_time: Some(to_proto_timestamp(wft.scheduled_at)),
        started_time: wft.started_at.map(to_proto_timestamp),
        attempt: wft.attempt as i32,
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
        supported_clients: resp.supported_clients,
        server_version: resp.version.clone(),
        cluster_id: resp.cluster_name.clone(),
        version_info: Some(version_proto::VersionInfo {
            current: Some(version_proto::ReleaseInfo {
                version: resp.version.clone(),
                release_time: None,
                notes: resp.notes.join("\n"),
            }),
            ..Default::default()
        }),
        cluster_name: resp.cluster_name,
        history_shard_count: resp.shard_count.max(1),
        persistence_store: "in-memory".to_string(),
        visibility_store: "in-memory".to_string(),
        initial_failover_version: 0,
        failover_version_increment: 0,
    }
}

pub fn system_info_to_proto(resp: SystemInfo) -> workflowservice::GetSystemInfoResponse {
    workflowservice::GetSystemInfoResponse {
        server_version: resp.server_version,
        capabilities: Some(workflowservice::get_system_info_response::Capabilities {
            signal_and_query_header: resp.capabilities.signal_and_query_header,
            internal_error_differentiation: resp.capabilities.internal_error_differentiation,
            activity_failure_include_heartbeat: resp
                .capabilities
                .activity_failure_include_heartbeat,
            supports_schedules: resp.capabilities.supports_schedules,
            encoded_failure_attributes: resp.capabilities.encoded_failure_attributes,
            build_id_based_versioning: resp.capabilities.build_id_based_versioning,
            upsert_memo: resp.capabilities.upsert_memo,
            eager_workflow_start: resp.capabilities.eager_workflow_start,
            sdk_metadata: resp.capabilities.sdk_metadata,
            count_group_by_execution_status: resp.capabilities.count_group_by_execution_status,
            nexus: resp.capabilities.nexus,
            server_scaled_deployments: resp.capabilities.server_scaled_deployments,
        }),
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
            description: namespace.description,
            owner_email: namespace.owner_email,
            data: std::collections::BTreeMap::new(),
            id: namespace.namespace_id.unwrap_or_default(),
            capabilities: Some(namespace_proto::namespace_info::Capabilities {
                eager_workflow_start: false,
                sync_update: true,
                async_update: true,
                // temporal-api-v1.62-sync accepts RecordWorkerHeartbeat as a
                // no-op; worker-heartbeat-observability owns persistence.
                // Advertising `true` keeps v0.4+ SDK workers running.
                worker_heartbeats: namespace.capabilities.worker_heartbeats,
                reported_problems_search_attribute: namespace
                    .capabilities
                    .reported_problems_search_attribute,
                workflow_pause: false,
                standalone_activities: false,
                worker_poll_complete_on_shutdown: false,
                poller_autoscaling: false,
            }),
            limits: None,
            supports_schedules: false,
        }),
        config: Some(namespace_proto::NamespaceConfig {
            workflow_execution_retention_ttl: None,
            bad_binaries: None,
            history_archival_state: enums::ArchivalState::Disabled as i32,
            history_archival_uri: String::new(),
            visibility_archival_state: enums::ArchivalState::Disabled as i32,
            visibility_archival_uri: String::new(),
            custom_search_attribute_aliases: namespace.custom_search_attribute_aliases,
        }),
        replication_config: Some(replication_proto::NamespaceReplicationConfig {
            active_cluster_name: namespace.cluster_name.clone(),
            clusters: vec![replication_proto::ClusterReplicationConfig {
                cluster_name: namespace.cluster_name,
            }],
            state: 0,
        }),
        failover_version: 1,
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
) -> Result<crate::translate::GetWorkflowExecutionHistoryRequest, ProtoConversionError> {
    let execution = req
        .execution
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "GetWorkflowExecutionHistoryRequest.execution",
        ))?;
    Ok(crate::translate::GetWorkflowExecutionHistoryRequest {
        namespace: req.namespace,
        workflow_id: execution.workflow_id.clone(),
        run_id: non_empty(execution.run_id.clone()),
        maximum_page_size: req.maximum_page_size.max(0) as usize,
        wait_new_event: req.wait_new_event,
        history_event_filter_type: req.history_event_filter_type,
        next_page_token: req.next_page_token,
    })
}

pub fn get_history_reverse_request_to_edge(
    req: workflowservice::GetWorkflowExecutionHistoryReverseRequest,
) -> Result<crate::translate::GetWorkflowExecutionHistoryReverseRequest, ProtoConversionError> {
    let execution = req
        .execution
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "GetWorkflowExecutionHistoryReverseRequest.execution",
        ))?;
    Ok(
        crate::translate::GetWorkflowExecutionHistoryReverseRequest {
            namespace: req.namespace,
            workflow_id: execution.workflow_id.clone(),
            run_id: non_empty(execution.run_id.clone()),
            maximum_page_size: req.maximum_page_size.max(0) as usize,
            next_page_token: req.next_page_token,
        },
    )
}

pub fn get_history_response_to_proto(
    resp: crate::translate::GetWorkflowExecutionHistoryResponse,
    filter_type: i32,
) -> workflowservice::GetWorkflowExecutionHistoryResponse {
    use prost::Message;
    let history_bytes = crate::translate::history_serializer::serialize_history(&resp.history);
    let mut history = tokeira_proto::history::History::decode(&history_bytes[..]).ok();

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
    let history_bytes = crate::translate::history_serializer::serialize_history(&resp.history);
    let history = tokeira_proto::history::History::decode(&history_bytes[..]).ok();

    workflowservice::GetWorkflowExecutionHistoryReverseResponse {
        history,
        next_page_token: resp.next_page_token,
    }
}

// v1.62-sync: reads deprecated `DescribeTaskQueueRequest.include_task_queue_status`
// for wire-compat with v0.4-era SDK clients. v1.62 replaces it with explicit
// stats request fields; v0.4 callers still set the boolean so edge preserves the read.
#[allow(deprecated)]
pub fn describe_task_queue_request_to_edge(
    req: workflowservice::DescribeTaskQueueRequest,
) -> Result<EdgeDescribeTaskQueueRequest, ProtoConversionError> {
    let task_queue = req
        .task_queue
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "DescribeTaskQueueRequest.task_queue",
        ))?;

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

// v1.62-sync: writes deprecated `PollerInfo.worker_version_capabilities` and
// `DescribeTaskQueueResponse.task_queue_status` for wire-compat with v0.4-era
// SDK readers. v1.62 replaces the former with `deployment_options` and the
// latter with `stats` on `DescribeTaskQueueResponse`; migration is owned by
// task 4.9 (wire-through additions) and `runtime-worker-versioning`.
#[allow(deprecated)]
pub fn describe_task_queue_response_to_proto(
    resp: EdgeDescribeTaskQueueResponse,
) -> workflowservice::DescribeTaskQueueResponse {
    // Tokeira does not yet publish worker-version capabilities or queue-level
    // versions info on the DescribeTaskQueue surface, so those remain absent.
    workflowservice::DescribeTaskQueueResponse {
        pollers: resp
            .pollers
            .into_iter()
            .map(
                |poller| tokeira_proto::public::temporal::api::taskqueue::v1::PollerInfo {
                    last_access_time: poller.last_access_time.map(to_proto_timestamp),
                    identity: poller.identity,
                    rate_per_second: poller.rate_per_second,
                    worker_version_capabilities: None,
                    deployment_options: None,
                },
            )
            .collect(),
        stats: None,
        stats_by_priority_key: Default::default(),
        versioning_info: None,
        config: Some(task_queue_config_to_proto(resp.config)),
        effective_rate_limit: None,
        task_queue_status: resp.backlog_count_hint.map(|backlog_count_hint| {
            tokeira_proto::public::temporal::api::taskqueue::v1::TaskQueueStatus {
                backlog_count_hint,
                ..Default::default()
            }
        }),
        versions_info: Default::default(),
    }
}

pub fn task_queue_config_from_update_request(
    req: &workflowservice::UpdateTaskQueueConfigRequest,
) -> TaskQueueConfig {
    TaskQueueConfig {
        queue_rate_limit: req
            .update_queue_rate_limit
            .as_ref()
            .and_then(|update| update.rate_limit.as_ref())
            .map(|rate_limit| rate_limit.requests_per_second),
        fairness_key_rate_limit_default: req
            .update_fairness_key_rate_limit_default
            .as_ref()
            .and_then(|update| update.rate_limit.as_ref())
            .map(|rate_limit| rate_limit.requests_per_second),
        fairness_weight_overrides: req.set_fairness_weight_overrides.clone(),
    }
}

pub fn task_queue_config_to_proto(config: TaskQueueConfig) -> taskqueue_proto::TaskQueueConfig {
    taskqueue_proto::TaskQueueConfig {
        queue_rate_limit: config.queue_rate_limit.map(rate_limit_config_to_proto),
        fairness_keys_rate_limit_default: config
            .fairness_key_rate_limit_default
            .map(rate_limit_config_to_proto),
        fairness_weight_overrides: config.fairness_weight_overrides,
    }
}

fn rate_limit_config_to_proto(requests_per_second: f32) -> taskqueue_proto::RateLimitConfig {
    taskqueue_proto::RateLimitConfig {
        rate_limit: Some(taskqueue_proto::RateLimit {
            requests_per_second,
        }),
        metadata: None,
    }
}

pub fn delete_request_to_edge(
    req: workflowservice::DeleteWorkflowExecutionRequest,
) -> Result<EdgeDeleteWorkflowExecutionRequest, ProtoConversionError> {
    let execution = req
        .workflow_execution
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "DeleteWorkflowExecutionRequest.workflow_execution",
        ))?;

    Ok(EdgeDeleteWorkflowExecutionRequest {
        namespace: req.namespace,
        workflow_id: execution.workflow_id.clone(),
        run_id: non_empty(execution.run_id.clone()),
    })
}

pub fn reset_request_to_edge(
    req: workflowservice::ResetWorkflowExecutionRequest,
) -> Result<EdgeResetWorkflowExecutionRequest, ProtoConversionError> {
    let execution = req
        .workflow_execution
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "ResetWorkflowExecutionRequest.workflow_execution",
        ))?;

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
    let task_queue = req
        .task_queue
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
        workflow_execution_timeout: proto_duration_to_time(req.workflow_execution_timeout.as_ref()),
        workflow_run_timeout: proto_duration_to_time(req.workflow_run_timeout.as_ref()),
        workflow_task_timeout: proto_duration_to_time(req.workflow_task_timeout.as_ref()),
        retry_policy: req.retry_policy.as_ref().map(retry_policy_to_domain),
        conflict_policy,
        reuse_policy,
        header: req.header.as_ref().map(headers_to_domain),
        versioning_override: versioning_override_to_edge(req.versioning_override)?,
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
        signal_link: None,
    }
}

pub fn versioning_mutation_from_proto(
    operation: Option<workflowservice::update_worker_versioning_rules_request::Operation>,
    now: OffsetDateTime,
) -> Result<ParsedVersioningMutation, ProtoConversionError> {
    use workflowservice::update_worker_versioning_rules_request::Operation;

    let operation = operation.ok_or(ProtoConversionError::MissingField(
        "UpdateWorkerVersioningRulesRequest.operation",
    ))?;
    let parsed = match operation {
        Operation::InsertAssignmentRule(op) => ParsedVersioningMutation {
            mutation: VersioningMutation::InsertAssignmentRule {
                rule: assignment_rule_from_proto(op.rule, now)?,
                index: op.rule_index.max(0) as usize,
            },
            commit_build_id: None,
            commit_force: false,
        },
        Operation::ReplaceAssignmentRule(op) => ParsedVersioningMutation {
            mutation: VersioningMutation::ReplaceAssignmentRule {
                rule: assignment_rule_from_proto(op.rule, now)?,
                index: op.rule_index.max(0) as usize,
                force: op.force,
            },
            commit_build_id: None,
            commit_force: false,
        },
        Operation::DeleteAssignmentRule(op) => ParsedVersioningMutation {
            mutation: VersioningMutation::DeleteAssignmentRule {
                index: op.rule_index.max(0) as usize,
                force: op.force,
            },
            commit_build_id: None,
            commit_force: false,
        },
        Operation::AddCompatibleRedirectRule(op) => ParsedVersioningMutation {
            mutation: VersioningMutation::AddRedirectRule {
                rule: redirect_rule_from_proto(op.rule, now)?,
            },
            commit_build_id: None,
            commit_force: false,
        },
        Operation::ReplaceCompatibleRedirectRule(op) => {
            let rule = redirect_rule_from_proto(op.rule, now)?;
            ParsedVersioningMutation {
                mutation: VersioningMutation::ReplaceRedirectRule {
                    source_build_id: rule.source_build_id.clone(),
                    rule,
                },
                commit_build_id: None,
                commit_force: false,
            }
        }
        Operation::DeleteCompatibleRedirectRule(op) => ParsedVersioningMutation {
            mutation: VersioningMutation::DeleteRedirectRule {
                source_build_id: op.source_build_id,
            },
            commit_build_id: None,
            commit_force: false,
        },
        Operation::CommitBuildId(op) => ParsedVersioningMutation {
            mutation: VersioningMutation::CommitBuildId {
                build_id: op.target_build_id.clone(),
            },
            commit_build_id: Some(op.target_build_id),
            commit_force: op.force,
        },
    };
    Ok(parsed)
}

fn assignment_rule_from_proto(
    rule: Option<taskqueue_proto::BuildIdAssignmentRule>,
    now: OffsetDateTime,
) -> Result<AssignmentRule, ProtoConversionError> {
    let rule = rule.ok_or(ProtoConversionError::MissingField(
        "BuildIdAssignmentRule.rule",
    ))?;
    let percentage_ramp = rule.ramp.map(|ramp| match ramp {
        taskqueue_proto::build_id_assignment_rule::Ramp::PercentageRamp(ramp) => {
            ramp.ramp_percentage
        }
    });
    Ok(AssignmentRule {
        target_build_id: rule.target_build_id,
        percentage_ramp,
        create_time: now,
    })
}

fn redirect_rule_from_proto(
    rule: Option<taskqueue_proto::CompatibleBuildIdRedirectRule>,
    now: OffsetDateTime,
) -> Result<RedirectRule, ProtoConversionError> {
    let rule = rule.ok_or(ProtoConversionError::MissingField(
        "CompatibleBuildIdRedirectRule.rule",
    ))?;
    Ok(RedirectRule {
        source_build_id: rule.source_build_id,
        target_build_id: rule.target_build_id,
        create_time: now,
    })
}

pub fn versioning_rules_to_update_proto(
    rules: VersioningRules,
) -> workflowservice::UpdateWorkerVersioningRulesResponse {
    workflowservice::UpdateWorkerVersioningRulesResponse {
        assignment_rules: assignment_rules_to_proto(&rules.assignment_rules),
        compatible_redirect_rules: redirect_rules_to_proto(&rules.redirect_rules),
        conflict_token: rules.conflict_token,
    }
}

pub fn versioning_rules_to_get_proto(
    rules: VersioningRules,
) -> workflowservice::GetWorkerVersioningRulesResponse {
    workflowservice::GetWorkerVersioningRulesResponse {
        assignment_rules: assignment_rules_to_proto(&rules.assignment_rules),
        compatible_redirect_rules: redirect_rules_to_proto(&rules.redirect_rules),
        conflict_token: rules.conflict_token,
    }
}

fn assignment_rules_to_proto(
    rules: &[AssignmentRule],
) -> Vec<taskqueue_proto::TimestampedBuildIdAssignmentRule> {
    rules
        .iter()
        .map(|rule| taskqueue_proto::TimestampedBuildIdAssignmentRule {
            rule: Some(taskqueue_proto::BuildIdAssignmentRule {
                target_build_id: rule.target_build_id.clone(),
                ramp: rule.percentage_ramp.map(|percentage| {
                    taskqueue_proto::build_id_assignment_rule::Ramp::PercentageRamp(
                        taskqueue_proto::RampByPercentage {
                            ramp_percentage: percentage,
                        },
                    )
                }),
            }),
            create_time: Some(to_proto_timestamp(rule.create_time)),
        })
        .collect()
}

fn redirect_rules_to_proto(
    rules: &[RedirectRule],
) -> Vec<taskqueue_proto::TimestampedCompatibleBuildIdRedirectRule> {
    rules
        .iter()
        .map(
            |rule| taskqueue_proto::TimestampedCompatibleBuildIdRedirectRule {
                rule: Some(taskqueue_proto::CompatibleBuildIdRedirectRule {
                    source_build_id: rule.source_build_id.clone(),
                    target_build_id: rule.target_build_id.clone(),
                }),
                create_time: Some(to_proto_timestamp(rule.create_time)),
            },
        )
        .collect()
}

pub fn reachability_to_proto(
    results: Vec<tokeira_runtime::BuildIdReachabilityResult>,
) -> workflowservice::GetWorkerTaskReachabilityResponse {
    workflowservice::GetWorkerTaskReachabilityResponse {
        build_id_reachability: results
            .into_iter()
            .map(|result| taskqueue_proto::BuildIdReachability {
                build_id: result.build_id,
                task_queue_reachability: result
                    .task_queue_reachability
                    .into_iter()
                    .map(|queue| taskqueue_proto::TaskQueueReachability {
                        task_queue: queue.task_queue.0,
                        reachability: queue
                            .reachability
                            .into_iter()
                            .map(|reachability| match reachability {
                                TaskReachabilityType::NewWorkflows => {
                                    enums::TaskReachability::NewWorkflows as i32
                                }
                                TaskReachabilityType::ExistingWorkflows => {
                                    enums::TaskReachability::ExistingWorkflows as i32
                                }
                            })
                            .collect(),
                    })
                    .collect(),
            })
            .collect(),
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

// v1.62-sync: reads deprecated `namespace` and `control` fields on
// `RequestCancelExternalWorkflowExecutionCommandAttributes`,
// `StartChildWorkflowExecutionCommandAttributes`, and
// `SignalExternalWorkflowExecutionCommandAttributes`. v1.62 replaces `namespace`
// with `namespace_id` (equivalent semantics) and moves `control` to a separate
// `input_payload` shape; edge preserves both reads so v0.4-era SDK completions
// that still send the old shape continue to dispatch correctly.
#[allow(deprecated)]
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
                request_eager_execution: attrs.request_eager_execution,
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
                heartbeat_timeout: proto_duration_to_time(attrs.heartbeat_timeout.as_ref()),
            })
        }
        Some(Attributes::StartTimerCommandAttributes(attrs)) => {
            let delay = attrs
                .start_to_fire_timeout
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
                attrs
                    .upserted_memo
                    .as_ref()
                    .map(memo_to_domain)
                    .unwrap_or_default(),
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
            Ok(WorkflowCommand::FailWorkflow {
                failure: attrs
                    .failure
                    .as_ref()
                    .map(failure_to_payload)
                    .unwrap_or_else(|| failure_to_payload(&failure_proto::Failure::default())),
            })
        }
        Some(Attributes::RequestCancelActivityTaskCommandAttributes(attrs)) => {
            Ok(WorkflowCommand::RequestCancelActivity {
                activity_id: attrs.scheduled_event_id.to_string(),
            })
        }
        Some(Attributes::CancelTimerCommandAttributes(attrs)) => Ok(WorkflowCommand::CancelTimer {
            timer_id: attrs.timer_id,
        }),
        Some(Attributes::CancelWorkflowExecutionCommandAttributes(_attrs)) => {
            Ok(WorkflowCommand::CancelWorkflow)
        }
        Some(Attributes::RequestCancelExternalWorkflowExecutionCommandAttributes(attrs)) => {
            Ok(WorkflowCommand::RequestCancelExternalWorkflowExecution {
                target_namespace_id: namespace_name_to_domain(&attrs.namespace),
                target_namespace: non_empty(attrs.namespace),
                target_workflow_id: WorkflowId(attrs.workflow_id),
                target_run_id: non_empty(attrs.run_id)
                    .map(|run_id| parse_run_id(&run_id))
                    .transpose()?,
                control: attrs.control,
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
                header: attrs
                    .header
                    .as_ref()
                    .map(|header| headers_to_domain(header).0),
            })
        }
        Some(Attributes::ContinueAsNewWorkflowExecutionCommandAttributes(attrs)) => {
            let task_queue =
                attrs
                    .task_queue
                    .as_ref()
                    .ok_or(ProtoConversionError::MissingField(
                        "ContinueAsNewWorkflowExecutionCommandAttributes.task_queue",
                    ))?;
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
                input: attrs
                    .input
                    .as_ref()
                    .map(payloads_to_domain)
                    .unwrap_or_default(),
                memo: attrs.memo.as_ref().map(memo_to_domain).unwrap_or_default(),
                search_attributes: attrs
                    .search_attributes
                    .as_ref()
                    .map(search_attributes_to_domain)
                    .transpose()?
                    .unwrap_or_default(),
                workflow_execution_timeout: None,
                workflow_run_timeout: proto_duration_to_time(attrs.workflow_run_timeout.as_ref()),
                workflow_task_timeout: proto_duration_to_time(attrs.workflow_task_timeout.as_ref())
                    .unwrap_or(time::Duration::seconds(10)),
                retry_policy: attrs.retry_policy.as_ref().map(retry_policy_to_domain),
            })
        }
        Some(Attributes::StartChildWorkflowExecutionCommandAttributes(attrs)) => {
            let task_queue =
                attrs
                    .task_queue
                    .as_ref()
                    .ok_or(ProtoConversionError::MissingField(
                        "StartChildWorkflowExecutionCommandAttributes.task_queue",
                    ))?;
            Ok(WorkflowCommand::StartChildWorkflow {
                child_workflow_id: WorkflowId(attrs.workflow_id),
                namespace_id: namespace_name_to_domain(&attrs.namespace),
                namespace: non_empty(attrs.namespace),
                workflow_type: WorkflowType(
                    attrs
                        .workflow_type
                        .as_ref()
                        .map(|workflow_type| workflow_type.name.clone())
                        .unwrap_or_default(),
                ),
                task_queue: task_queue_to_domain(task_queue),
                input: attrs
                    .input
                    .as_ref()
                    .map(payloads_to_domain)
                    .unwrap_or_default(),
                header: attrs.header.as_ref().map(headers_to_domain),
                memo: attrs.memo.as_ref().map(memo_to_domain).unwrap_or_default(),
                search_attributes: attrs
                    .search_attributes
                    .as_ref()
                    .map(search_attributes_to_domain)
                    .transpose()?
                    .unwrap_or_default(),
                workflow_execution_timeout: proto_duration_to_time(
                    attrs.workflow_execution_timeout.as_ref(),
                ),
                workflow_run_timeout: proto_duration_to_time(attrs.workflow_run_timeout.as_ref()),
                workflow_task_timeout: proto_duration_to_time(attrs.workflow_task_timeout.as_ref())
                    .unwrap_or(time::Duration::seconds(10)),
                retry_policy: attrs.retry_policy.as_ref().map(retry_policy_to_domain),
                cron_schedule: non_empty(attrs.cron_schedule),
                parent_close_policy: parent_close_policy_to_domain(attrs.parent_close_policy),
            })
        }
        Some(Attributes::SignalExternalWorkflowExecutionCommandAttributes(attrs)) => {
            let execution = attrs
                .execution
                .as_ref()
                .ok_or(ProtoConversionError::MissingField(
                    "SignalExternalWorkflowExecutionCommandAttributes.execution",
                ))?;
            Ok(WorkflowCommand::SignalExternalWorkflowExecution {
                target_namespace_id: namespace_name_to_domain(&attrs.namespace),
                target_namespace: non_empty(attrs.namespace),
                target_workflow_id: WorkflowId(execution.workflow_id.clone()),
                target_run_id: non_empty(execution.run_id.clone())
                    .map(|run_id| parse_run_id(&run_id))
                    .transpose()?,
                signal_name: attrs.signal_name,
                input: attrs
                    .input
                    .as_ref()
                    .map(payloads_to_domain)
                    .unwrap_or_default(),
                header: attrs.header.as_ref().map(headers_to_domain),
                control: attrs.control,
            })
        }
        Some(Attributes::ProtocolMessageCommandAttributes(attrs)) => {
            // ProtocolMessage commands reference a message in the
            // completion's `messages` field by message_id. The body
            // is resolved by the caller which has access to the
            // messages list. Return a placeholder that the caller
            // will resolve.
            Ok(WorkflowCommand::ProtocolMessage {
                message_id: attrs.message_id,
                body: tokeira_kernel::UpdateProtocolBody::Accepted {
                    update_id: String::new(),
                    update_name: String::new(),
                    input: Payloads::default(),
                },
            })
        }
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

// v1.62-sync: writes deprecated `namespace` and `control` fields on outbound
// `StartChildWorkflowExecutionCommandAttributes`,
// `SignalExternalWorkflowExecutionCommandAttributes`, and
// `RequestCancelExternalWorkflowExecutionCommandAttributes` for wire-compat
// with v0.4-era SDK readers. v1.62 introduces `namespace_id` and an
// `input_payload`-based control shape; the edge will emit both once the
// follow-up spec lands (see `runtime-worker-versioning` for namespace id
// unification; `control` retirement has no named follow-up and should track
// with the next command-attributes cleanup spec).
#[allow(deprecated)]
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
            request_eager_execution,
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
                task_queue: Some(tokeira_proto::conversions::common::task_queue_from_domain(
                    task_queue,
                )),
                header: header.as_ref().map(headers_from_domain),
                input: Some(payloads_from_domain(input)),
                request_eager_execution: *request_eager_execution,
                schedule_to_close_timeout: schedule_to_close_timeout.map(to_proto_duration),
                schedule_to_start_timeout: schedule_to_start_timeout.map(to_proto_duration),
                start_to_close_timeout: start_to_close_timeout.map(to_proto_duration),
                heartbeat_timeout: heartbeat_timeout.map(to_proto_duration),
                retry_policy: retry_policy.as_ref().map(retry_policy_from_domain),
                ..Default::default()
            },
        )),
        WorkflowCommand::StartTimer { timer_id, fire_at } => {
            let now = OffsetDateTime::now_utc();
            let delay = *fire_at - now;
            let delay = if delay.is_negative() {
                time::Duration::ZERO
            } else {
                delay
            };
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
                    search_attributes: Some(search_attributes_from_domain(search_attributes)),
                },
            ))
        }
        WorkflowCommand::UpsertMemo(memo) => {
            Some(Attributes::ModifyWorkflowPropertiesCommandAttributes(
                command::ModifyWorkflowPropertiesCommandAttributes {
                    upserted_memo: Some(memo_from_domain(memo)),
                },
            ))
        }
        WorkflowCommand::CompleteWorkflow { result } => {
            Some(Attributes::CompleteWorkflowExecutionCommandAttributes(
                command::CompleteWorkflowExecutionCommandAttributes {
                    result: Some(payloads_from_domain(result)),
                },
            ))
        }
        WorkflowCommand::FailWorkflow { failure } => {
            Some(Attributes::FailWorkflowExecutionCommandAttributes(
                command::FailWorkflowExecutionCommandAttributes {
                    failure: Some(payload_to_failure(failure)),
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
        WorkflowCommand::CancelTimer { timer_id } => Some(
            Attributes::CancelTimerCommandAttributes(command::CancelTimerCommandAttributes {
                timer_id: timer_id.clone(),
            }),
        ),
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
                header: header
                    .as_ref()
                    .map(|header| headers_from_domain(&tokeira_types::Headers(header.clone()))),
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
            retry_policy,
            ..
        } => Some(Attributes::ContinueAsNewWorkflowExecutionCommandAttributes(
            command::ContinueAsNewWorkflowExecutionCommandAttributes {
                workflow_type: Some(tokeira_proto::common::WorkflowType {
                    name: workflow_type.0.clone(),
                }),
                task_queue: Some(tokeira_proto::conversions::common::task_queue_from_domain(
                    task_queue,
                )),
                input: Some(payloads_from_domain(input)),
                workflow_run_timeout: workflow_run_timeout.map(to_proto_duration),
                workflow_task_timeout: Some(to_proto_duration(*workflow_task_timeout)),
                retry_policy: retry_policy.as_ref().map(retry_policy_from_domain),
                memo: Some(memo_from_domain(memo)),
                search_attributes: Some(search_attributes_from_domain(search_attributes)),
                ..Default::default()
            },
        )),
        WorkflowCommand::StartChildWorkflow {
            child_workflow_id,
            namespace_id,
            namespace,
            workflow_type,
            task_queue,
            input,
            header,
            memo,
            search_attributes,
            workflow_execution_timeout,
            workflow_run_timeout,
            workflow_task_timeout,
            retry_policy,
            cron_schedule,
            parent_close_policy,
        } => Some(Attributes::StartChildWorkflowExecutionCommandAttributes(
            command::StartChildWorkflowExecutionCommandAttributes {
                namespace: namespace
                    .clone()
                    .unwrap_or_else(|| namespace_id.0.to_string()),
                workflow_id: child_workflow_id.0.clone(),
                workflow_type: Some(tokeira_proto::common::WorkflowType {
                    name: workflow_type.0.clone(),
                }),
                task_queue: Some(tokeira_proto::conversions::common::task_queue_from_domain(
                    task_queue,
                )),
                input: Some(payloads_from_domain(input)),
                header: header.as_ref().map(headers_from_domain),
                memo: Some(memo_from_domain(memo)),
                search_attributes: Some(search_attributes_from_domain(search_attributes)),
                workflow_execution_timeout: workflow_execution_timeout.map(to_proto_duration),
                workflow_run_timeout: workflow_run_timeout.map(to_proto_duration),
                workflow_task_timeout: Some(to_proto_duration(*workflow_task_timeout)),
                retry_policy: retry_policy.as_ref().map(retry_policy_from_domain),
                cron_schedule: cron_schedule.clone().unwrap_or_default(),
                parent_close_policy: parent_close_policy_from_domain(*parent_close_policy),
                ..Default::default()
            },
        )),
        WorkflowCommand::SignalExternalWorkflowExecution {
            target_namespace_id,
            target_namespace,
            target_workflow_id,
            target_run_id,
            signal_name,
            input,
            header,
            control,
        } => Some(
            Attributes::SignalExternalWorkflowExecutionCommandAttributes(
                command::SignalExternalWorkflowExecutionCommandAttributes {
                    namespace: target_namespace
                        .clone()
                        .unwrap_or_else(|| target_namespace_id.0.to_string()),
                    execution: Some(workflow_execution_from_ids(
                        target_workflow_id,
                        target_run_id.unwrap_or(RunId(Uuid::nil())),
                    )),
                    signal_name: signal_name.clone(),
                    input: Some(payloads_from_domain(input)),
                    header: header.as_ref().map(headers_from_domain),
                    control: control.clone(),
                    ..Default::default()
                },
            ),
        ),
        WorkflowCommand::RequestCancelExternalWorkflowExecution {
            target_namespace_id,
            target_namespace,
            target_workflow_id,
            target_run_id,
            control,
        } => Some(
            Attributes::RequestCancelExternalWorkflowExecutionCommandAttributes(
                command::RequestCancelExternalWorkflowExecutionCommandAttributes {
                    namespace: target_namespace
                        .clone()
                        .unwrap_or_else(|| target_namespace_id.0.to_string()),
                    workflow_id: target_workflow_id.0.clone(),
                    run_id: target_run_id
                        .map(|run_id| run_id.0.to_string())
                        .unwrap_or_default(),
                    control: control.clone(),
                    ..Default::default()
                },
            ),
        ),
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
                schedule_to_close_timeout: schedule_to_close_timeout.map(to_proto_duration),
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
        WorkflowCommand::ProtocolMessage {
            message_id,
            body: _,
        } => Some(Attributes::ProtocolMessageCommandAttributes(
            command::ProtocolMessageCommandAttributes {
                message_id: message_id.clone(),
            },
        )),
        WorkflowCommand::UpdateCompleted { .. }
        | WorkflowCommand::UpdateRejected { .. }
        | WorkflowCommand::RequestNewWorkflowTask => {
            return Err(ProtoConversionError::MissingField(
                "WorkflowCommand has no proto Command equivalent",
            ));
        }
    };

    Ok(command::Command {
        attributes,
        ..Default::default()
    })
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

const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_UPDATE_TIMEOUT: Duration = Duration::from_secs(30);

pub fn serialize_activity_token(token: &ActivityTaskToken) -> Vec<u8> {
    serde_json::to_vec(token).unwrap_or_default()
}

pub fn deserialize_activity_token(bytes: &[u8]) -> Result<ActivityTaskToken, ProtoConversionError> {
    if bytes.is_empty() {
        return Err(ProtoConversionError::InvalidTaskToken(
            "task_token is empty".to_string(),
        ));
    }
    serde_json::from_slice(bytes).map_err(|e| ProtoConversionError::InvalidTaskToken(e.to_string()))
}

pub fn poll_activity_request_to_edge(
    req: workflowservice::PollActivityTaskQueueRequest,
) -> Result<crate::translate::PollActivityTaskQueueRequest, ProtoConversionError> {
    let task_queue = req
        .task_queue
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "PollActivityTaskQueueRequest.task_queue",
        ))?;

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
    let workflow_execution = Some(workflow_execution_from_ids(
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
) -> Result<crate::translate::RespondActivityTaskCompletedRequest, ProtoConversionError> {
    let token = deserialize_activity_token(&req.task_token)?;
    Ok(crate::translate::RespondActivityTaskCompletedRequest {
        token,
        result: req
            .result
            .as_ref()
            .map(payloads_to_domain)
            .unwrap_or_default(),
        identity: req.identity,
    })
}

pub fn respond_activity_completed_to_proto() -> workflowservice::RespondActivityTaskCompletedResponse
{
    workflowservice::RespondActivityTaskCompletedResponse {}
}

pub fn respond_activity_failed_to_edge(
    req: workflowservice::RespondActivityTaskFailedRequest,
) -> Result<crate::translate::RespondActivityTaskFailedRequest, ProtoConversionError> {
    let token = deserialize_activity_token(&req.task_token)?;
    let (failure, failure_error_type, is_non_retryable) = match req.failure {
        Some(f) => {
            let (error_type, non_retryable) = activity_retry_classification(&f);
            (failure_to_payload(&f), error_type, non_retryable)
        }
        None => (
            failure_to_payload(&failure_proto::Failure::default()),
            None,
            false,
        ),
    };
    Ok(crate::translate::RespondActivityTaskFailedRequest {
        token,
        failure,
        failure_error_type,
        is_non_retryable,
        identity: req.identity,
    })
}

pub fn respond_activity_failed_to_proto() -> workflowservice::RespondActivityTaskFailedResponse {
    workflowservice::RespondActivityTaskFailedResponse {
        ..Default::default()
    }
}

pub fn record_heartbeat_to_edge(
    req: workflowservice::RecordActivityTaskHeartbeatRequest,
) -> Result<crate::translate::RecordActivityTaskHeartbeatRequest, ProtoConversionError> {
    let token = deserialize_activity_token(&req.task_token)?;
    Ok(crate::translate::RecordActivityTaskHeartbeatRequest {
        token,
        identity: req.identity,
    })
}

pub fn record_heartbeat_to_proto(
    resp: crate::translate::RecordActivityTaskHeartbeatResponse,
) -> workflowservice::RecordActivityTaskHeartbeatResponse {
    workflowservice::RecordActivityTaskHeartbeatResponse {
        cancel_requested: resp.cancel_requested,
        activity_paused: false,
        activity_reset: false,
    }
}

// ── Advanced workflow endpoint translations ──

pub fn terminate_request_to_edge(
    req: workflowservice::TerminateWorkflowExecutionRequest,
) -> Result<crate::translate::TerminateWorkflowExecutionRequest, ProtoConversionError> {
    let execution = req
        .workflow_execution
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "TerminateWorkflowExecutionRequest.workflow_execution",
        ))?;
    Ok(crate::translate::TerminateWorkflowExecutionRequest {
        namespace: req.namespace,
        workflow_id: execution.workflow_id.clone(),
        run_id: non_empty(execution.run_id.clone()),
        reason: req.reason,
        details: req.details.as_ref().map(payloads_to_domain),
        identity: req.identity,
    })
}

pub fn terminate_response_to_proto() -> workflowservice::TerminateWorkflowExecutionResponse {
    workflowservice::TerminateWorkflowExecutionResponse {}
}

pub fn cancel_request_to_edge(
    req: workflowservice::RequestCancelWorkflowExecutionRequest,
) -> Result<crate::translate::RequestCancelWorkflowExecutionRequest, ProtoConversionError> {
    let execution = req
        .workflow_execution
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "RequestCancelWorkflowExecutionRequest.workflow_execution",
        ))?;
    Ok(crate::translate::RequestCancelWorkflowExecutionRequest {
        namespace: req.namespace,
        workflow_id: execution.workflow_id.clone(),
        run_id: non_empty(execution.run_id.clone()),
        reason: req.reason,
        identity: req.identity,
    })
}

pub fn cancel_response_to_proto() -> workflowservice::RequestCancelWorkflowExecutionResponse {
    workflowservice::RequestCancelWorkflowExecutionResponse {}
}

pub fn query_request_to_edge(
    req: workflowservice::QueryWorkflowRequest,
) -> Result<crate::translate::QueryWorkflowRequest, ProtoConversionError> {
    let execution = req
        .execution
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "QueryWorkflowRequest.execution",
        ))?;
    let query = req
        .query
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "QueryWorkflowRequest.query",
        ))?;

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
        query_result: resp.result.map(|p| payloads_from_domain(&p)),
        query_rejected: resp.rejected_status.map(|status| {
            tokeira_proto::public::temporal::api::query::v1::QueryRejected {
                status: execution_status_to_proto(status),
            }
        }),
    }
}

pub fn update_request_to_edge(
    req: workflowservice::UpdateWorkflowExecutionRequest,
) -> Result<crate::translate::UpdateWorkflowExecutionRequest, ProtoConversionError> {
    let execution = req
        .workflow_execution
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "UpdateWorkflowExecutionRequest.workflow_execution",
        ))?;

    let request = req
        .request
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "UpdateWorkflowExecutionRequest.request",
        ))?;
    let meta = request.meta.as_ref();
    let input_msg = request.input.as_ref();

    let wait_policy = match req.wait_policy {
        Some(wp) => match wp.lifecycle_stage {
            3 => crate::translate::UpdateWaitPolicyDto::Completed,
            _ => crate::translate::UpdateWaitPolicyDto::Accepted,
        },
        None => crate::translate::UpdateWaitPolicyDto::Accepted,
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
        crate::translate::UpdateOutcomeDto::Accepted { .. } => {
            workflowservice::UpdateWorkflowExecutionResponse {
                outcome: None,
                ..Default::default()
            }
        }
        crate::translate::UpdateOutcomeDto::Completed { result, .. } => {
            workflowservice::UpdateWorkflowExecutionResponse {
                outcome: Some(update::Outcome {
                    value: Some(update::outcome::Value::Success(payloads_from_domain(
                        &result,
                    ))),
                }),
                ..Default::default()
            }
        }
        crate::translate::UpdateOutcomeDto::Rejected { failure, .. } => {
            workflowservice::UpdateWorkflowExecutionResponse {
                outcome: Some(update::Outcome {
                    value: Some(update::outcome::Value::Failure(payload_to_failure(
                        &failure,
                    ))),
                }),
                ..Default::default()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{operator_service::ClusterInfo, translate::NamespaceDescription};
    use tokeira_proto::public::temporal::api::taskqueue::v1 as taskqueue;
    use tokeira_runtime::{RedirectRule, VersioningRules};

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
    fn poll_request_preserves_worker_version_capabilities() {
        let edge = poll_request_to_edge(workflowservice::PollWorkflowTaskQueueRequest {
            namespace: "default".to_string(),
            task_queue: Some(taskqueue::TaskQueue {
                name: "queue".to_string(),
                ..Default::default()
            }),
            identity: "worker-1".to_string(),
            worker_version_capabilities: Some(
                tokeira_proto::public::temporal::api::common::v1::WorkerVersionCapabilities {
                    build_id: "build-a".to_string(),
                    use_versioning: true,
                    deployment_series_name: "deploy-a".to_string(),
                },
            ),
            ..Default::default()
        })
        .expect("poll request should convert");

        assert_eq!(edge.deployment, Some(DeploymentId("deploy-a".to_string())));
        assert_eq!(edge.build_id, Some(BuildId("build-a".to_string())));
    }

    #[test]
    fn respond_completed_request_decodes_speculative_capability() {
        let edge = respond_completed_request_to_edge(
            workflowservice::RespondWorkflowTaskCompletedRequest {
                capabilities: Some(
                    workflowservice::respond_workflow_task_completed_request::Capabilities {
                        discard_speculative_workflow_task_with_events: true,
                    },
                ),
                ..Default::default()
            },
        )
        .expect("respond completed request should convert");

        assert!(edge.client_discards_speculative_with_events);
    }

    #[test]
    fn versioning_rules_proto_emits_redirect_create_time() {
        let create_time = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(42);
        let proto = versioning_rules_to_get_proto(VersioningRules {
            assignment_rules: Vec::new(),
            redirect_rules: vec![RedirectRule {
                source_build_id: "old".to_string(),
                target_build_id: "new".to_string(),
                create_time,
            }],
            conflict_token: 7_u64.to_be_bytes().to_vec(),
        });

        assert_eq!(proto.compatible_redirect_rules.len(), 1);
        assert_eq!(
            proto.compatible_redirect_rules[0].create_time,
            Some(to_proto_timestamp(create_time))
        );
    }

    #[test]
    fn namespace_archival_disabled() {
        let proto = namespace_to_proto(NamespaceDescription {
            name: "default".to_string(),
            namespace_id: Some("ns-1".to_string()),
            is_global: false,
            visibility_enabled: true,
            deleted: false,
            description: String::new(),
            owner_email: String::new(),
            cluster_name: "local".to_string(),
            custom_search_attribute_aliases: std::collections::BTreeMap::new(),
            capabilities: crate::translate::NamespaceCapabilities {
                worker_heartbeats: true,
                reported_problems_search_attribute: false,
            },
        });

        let config = proto.config.expect("config");
        assert_eq!(
            config.history_archival_state,
            enums::ArchivalState::Disabled as i32
        );
        assert_eq!(
            config.visibility_archival_state,
            enums::ArchivalState::Disabled as i32
        );
    }

    #[test]
    fn namespace_clusters_populated() {
        let proto = namespace_to_proto(NamespaceDescription {
            name: "default".to_string(),
            namespace_id: Some("ns-1".to_string()),
            is_global: false,
            visibility_enabled: true,
            deleted: false,
            description: String::new(),
            owner_email: String::new(),
            cluster_name: "local".to_string(),
            custom_search_attribute_aliases: std::collections::BTreeMap::new(),
            capabilities: crate::translate::NamespaceCapabilities {
                worker_heartbeats: true,
                reported_problems_search_attribute: false,
            },
        });

        let replication = proto.replication_config.expect("replication");
        assert_eq!(replication.active_cluster_name, "local");
        assert_eq!(replication.clusters.len(), 1);
        assert_eq!(replication.clusters[0].cluster_name, "local");
    }

    #[test]
    fn cluster_info_populated() {
        let proto = cluster_info_to_proto(ClusterInfo {
            cluster_name: "tokeira-local".to_string(),
            version: "0.1.0-dev".to_string(),
            notes: vec!["in-memory operator api".to_string()],
            shard_count: 1,
            supported_clients: std::collections::BTreeMap::from([(
                "temporal-go".to_string(),
                ">=1.26.0".to_string(),
            )]),
        });

        assert!(!proto.supported_clients.is_empty());
        assert!(proto.version_info.is_some());
        assert!(proto.history_shard_count >= 1);
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
        let default = workflowservice::PollActivityTaskQueueResponse::default();
        assert!(default.task_token.is_empty());
        assert!(default.activity_id.is_empty());
        assert_eq!(default.attempt, 0);
    }

    #[test]
    fn invalid_task_token_returns_error() {
        let err =
            deserialize_activity_token(b"not-json").expect_err("should fail on invalid bytes");
        match err {
            ProtoConversionError::InvalidTaskToken(_) => {}
            other => {
                panic!("unexpected error: {other:?}")
            }
        }
    }

    #[test]
    fn empty_task_token_returns_error() {
        let err = deserialize_activity_token(b"").expect_err("should fail on empty bytes");
        match err {
            ProtoConversionError::InvalidTaskToken(_) => {}
            other => {
                panic!("unexpected error: {other:?}")
            }
        }
    }

    #[test]
    fn heartbeat_cancel_requested_propagation() {
        let resp = crate::translate::RecordActivityTaskHeartbeatResponse {
            cancel_requested: true,
        };
        let proto = record_heartbeat_to_proto(resp);
        assert!(proto.cancel_requested);

        let resp = crate::translate::RecordActivityTaskHeartbeatResponse {
            cancel_requested: false,
        };
        let proto = record_heartbeat_to_proto(resp);
        assert!(!proto.cancel_requested);
    }

    #[test]
    fn activity_poll_default_timeout() {
        let req = workflowservice::PollActivityTaskQueueRequest {
            namespace: "default".to_string(),
            task_queue: Some(taskqueue::TaskQueue {
                name: "queue".to_string(),
                ..Default::default()
            }),
            identity: "worker-1".to_string(),
            ..Default::default()
        };
        let edge = poll_activity_request_to_edge(req).unwrap();
        assert_eq!(edge.timeout, Duration::from_secs(60));
    }

    #[test]
    fn terminate_with_details_translates() {
        use tokeira_proto::conversions::common::payloads_from_domain;
        let details = Payloads(vec![tokeira_types::Payload {
            data: b"stack-trace".to_vec(),
            metadata: Default::default(),
        }]);
        let req = workflowservice::TerminateWorkflowExecutionRequest {
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
        let edge = terminate_request_to_edge(req).unwrap();
        assert!(edge.details.is_some());
        assert_eq!(edge.details.unwrap().0.len(), 1);
    }

    #[test]
    fn cancel_with_empty_reason() {
        let req = workflowservice::RequestCancelWorkflowExecutionRequest {
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
        let edge = cancel_request_to_edge(req).unwrap();
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

        // lifecycle_stage 3 → Completed (COMPLETED = 3 in the proto enum)
        let req = workflowservice::UpdateWorkflowExecutionRequest {
            namespace: "ns".to_string(),
            workflow_execution: Some(wf_exec()),
            request: Some(update_request("handler", "u1")),
            wait_policy: Some(update::WaitPolicy { lifecycle_stage: 3 }),
            ..Default::default()
        };
        let edge = update_request_to_edge(req).unwrap();
        assert_eq!(
            edge.wait_policy,
            crate::translate::UpdateWaitPolicyDto::Completed
        );

        // lifecycle_stage 1 → Accepted
        let req = workflowservice::UpdateWorkflowExecutionRequest {
            namespace: "ns".to_string(),
            workflow_execution: Some(wf_exec()),
            request: Some(update_request("handler", "u2")),
            wait_policy: Some(update::WaitPolicy { lifecycle_stage: 1 }),
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
            execution: Some(tokeira_proto::common::WorkflowExecution {
                workflow_id: "wf".to_string(),
                run_id: String::new(),
                ..Default::default()
            }),
            query: Some(query::WorkflowQuery {
                query_type: "check".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let edge = query_request_to_edge(req).unwrap();
        assert_eq!(edge.timeout, Duration::from_secs(10));
    }

    #[test]
    fn update_default_timeout() {
        use tokeira_proto::public::temporal::api::update::v1 as update;
        let req = workflowservice::UpdateWorkflowExecutionRequest {
            namespace: "ns".to_string(),
            workflow_execution: Some(tokeira_proto::common::WorkflowExecution {
                workflow_id: "wf".to_string(),
                run_id: String::new(),
                ..Default::default()
            }),
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
        assert_eq!(edge.timeout, Duration::from_secs(30));
    }

    // The upstream RespondWorkflowTaskCompletedResponse no longer has
    // workflow_completed/new_run_id fields, so the old property test
    // and related tests are removed.

    #[test]
    fn fail_workflow_command_produces_failure_proto_encoding() {
        let failure = failure_proto::Failure {
            message: "app error".to_string(),
            failure_info: Some(failure_proto::failure::FailureInfo::ApplicationFailureInfo(
                failure_proto::ApplicationFailureInfo {
                    r#type: "AppError".to_string(),
                    non_retryable: false,
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        let proto_cmd = command::Command {
            attributes: Some(
                command::command::Attributes::FailWorkflowExecutionCommandAttributes(
                    command::FailWorkflowExecutionCommandAttributes {
                        failure: Some(failure),
                    },
                ),
            ),
            ..Default::default()
        };
        let edge = proto_command_to_workflow_command(proto_cmd).unwrap();
        match edge {
            WorkflowCommand::FailWorkflow { failure } => {
                assert_eq!(
                    failure.metadata.get("encoding").map(|s| s.as_str()),
                    Some("temporal/failure+proto")
                );
                let decoded = payload_to_failure(&failure);
                assert_eq!(decoded.message, "app error");
                match decoded.failure_info.unwrap() {
                    failure_proto::failure::FailureInfo::ApplicationFailureInfo(info) => {
                        assert_eq!(info.r#type, "AppError");
                    }
                    other => panic!("unexpected failure_info: {other:?}"),
                }
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn respond_activity_failed_extracts_type_from_application_failure_info() {
        let failure = failure_proto::Failure {
            message: "activity error".to_string(),
            source: "GoSDK".to_string(),
            failure_info: Some(failure_proto::failure::FailureInfo::ApplicationFailureInfo(
                failure_proto::ApplicationFailureInfo {
                    r#type: "CustomActivityError".to_string(),
                    non_retryable: true,
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        let token = tokeira_types::ActivityTaskToken {
            run_key: tokeira_types::RunKey::new(),
            activity_id: "act-1".to_string(),
            schedule_event_id: 5,
            attempt: 1,
            shard_epoch: tokeira_types::ShardEpoch::ZERO,
        };
        let token_bytes = serialize_activity_token(&token);
        let req = workflowservice::RespondActivityTaskFailedRequest {
            task_token: token_bytes,
            failure: Some(failure),
            identity: "worker".to_string(),
            ..Default::default()
        };
        let edge = respond_activity_failed_to_edge(req).unwrap();
        assert_eq!(
            edge.failure.metadata.get("encoding").map(|s| s.as_str()),
            Some("temporal/failure+proto")
        );
        assert_eq!(
            edge.failure_error_type.as_deref(),
            Some("CustomActivityError")
        );
        assert!(edge.is_non_retryable);
    }

    #[test]
    fn corrupted_payload_produces_fallback_failure() {
        let corrupted = tokeira_types::Payload {
            data: b"garbage bytes".to_vec(),
            metadata: Default::default(),
        };
        let decoded = payload_to_failure(&corrupted);
        assert_eq!(decoded.message, "garbage bytes");
        assert!(decoded.failure_info.is_none());
    }
}
