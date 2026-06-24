// Nexus translation mirrors upstream's deprecated-but-still-on-wire `operation_id`
// fields, required for v1.31.0 wire compatibility.
#![allow(deprecated)]

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokeira_kernel::NexusResolution;
use tokeira_proto::{
    conversions::common::{
        failure_to_payload, payload_from_domain, payload_to_domain, to_proto_timestamp,
    },
    public::temporal::api::{failure::v1 as failure_proto, nexus::v1 as nexus_v1},
    workflowservice,
};
use tokeira_runtime::NexusTaskRequest;
use tokeira_types::{Payload, Payloads, TaskQueueName};

#[derive(Debug, Error)]
pub enum NexusTranslateError {
    #[error("missing required field: {0}")]
    MissingField(&'static str),
    #[error("invalid nexus transport request: {0}")]
    InvalidArgument(String),
}

#[derive(Clone, Debug, PartialEq)]
pub struct PollNexusTaskQueueRequest {
    pub namespace: String,
    pub worker_identity: String,
    pub task_queue: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PollNexusTaskQueueResponse {
    pub task_token: Vec<u8>,
    pub request: NexusTaskRequest,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RespondNexusTaskCompletedRequest {
    pub namespace: String,
    pub identity: String,
    pub task_token: Vec<u8>,
    pub response: Option<nexus_v1::Response>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RespondNexusTaskFailedRequest {
    pub namespace: String,
    pub identity: String,
    pub task_token: Vec<u8>,
    /// Deprecated v0.4-era handler error (`RespondNexusTaskFailedRequest.error`,
    /// field 4). Read for wire-compat with old SDKs.
    pub error: Option<nexus_v1::HandlerError>,
    /// v1.62 structured handler failure (`RespondNexusTaskFailedRequest.failure`,
    /// field 5). A `temporal.api.failure.v1.Failure` that MUST contain a
    /// `NexusHandlerFailureInfo` (`workflow_handler.go:6096 @ v1.31.0`). This is
    /// what modern SDKs send; preferred over `error` when present.
    pub failure: Option<failure_proto::Failure>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct NexusFailureEnvelope {
    error_type: String,
    message: String,
    metadata: BTreeMap<String, String>,
    details_hex: String,
}

pub fn poll_request_to_edge(
    req: workflowservice::PollNexusTaskQueueRequest,
) -> Result<PollNexusTaskQueueRequest, NexusTranslateError> {
    if req.namespace.trim().is_empty() {
        return Err(NexusTranslateError::MissingField("namespace"));
    }
    let task_queue = req
        .task_queue
        .ok_or(NexusTranslateError::MissingField("task_queue"))?;
    if task_queue.name.trim().is_empty() {
        return Err(NexusTranslateError::InvalidArgument(
            "task_queue.name must not be empty".to_string(),
        ));
    }
    Ok(PollNexusTaskQueueRequest {
        namespace: req.namespace,
        worker_identity: req.identity,
        task_queue: task_queue.name,
    })
}

pub fn poll_response_to_proto(
    resp: PollNexusTaskQueueResponse,
) -> Result<workflowservice::PollNexusTaskQueueResponse, NexusTranslateError> {
    Ok(workflowservice::PollNexusTaskQueueResponse {
        task_token: resp.task_token,
        request: Some(nexus_task_to_proto_request(&resp.request)?),
        poller_group_id: String::new(),
        poller_group_infos: Vec::new(),
        poller_scaling_decision: None,
    })
}

pub fn completed_request_to_edge(
    req: workflowservice::RespondNexusTaskCompletedRequest,
) -> Result<RespondNexusTaskCompletedRequest, NexusTranslateError> {
    if req.namespace.trim().is_empty() {
        return Err(NexusTranslateError::MissingField("namespace"));
    }
    Ok(RespondNexusTaskCompletedRequest {
        namespace: req.namespace,
        identity: req.identity,
        task_token: req.task_token,
        response: req.response,
    })
}

// v1.62-sync: prefers the structured `failure` field (5) that modern SDKs send;
// falls back to the deprecated `error` field (4) for v0.4-era handlers. v1.31.0's
// frontend requires one of them and that `failure`, if set, carries a
// `NexusHandlerFailureInfo` (`workflow_handler.go:6096 @ v1.31.0`); that check is
// enforced in `respond_nexus_task_failed`, not here, so the DTO stays neutral.
#[allow(deprecated)]
pub fn failed_request_to_edge(
    req: workflowservice::RespondNexusTaskFailedRequest,
) -> Result<RespondNexusTaskFailedRequest, NexusTranslateError> {
    if req.namespace.trim().is_empty() {
        return Err(NexusTranslateError::MissingField("namespace"));
    }
    Ok(RespondNexusTaskFailedRequest {
        namespace: req.namespace,
        identity: req.identity,
        task_token: req.task_token,
        error: req.error,
        failure: req.failure,
    })
}

// v1.62-sync: writes deprecated `CancelOperationRequest.operation_id` for
// wire-compat with v0.4-era Nexus clients. v1.62 renames to `operation_token`;
// the code above populates both fields so new and old readers both work.
// Migration to `operation_token`-only is task 4.7 / 8.1.
#[allow(deprecated)]
pub fn nexus_task_to_proto_request(
    task_request: &NexusTaskRequest,
) -> Result<nexus_v1::Request, NexusTranslateError> {
    match task_request {
        NexusTaskRequest::StartOperation {
            service,
            operation,
            request_id,
            payload,
            scheduled_time,
            callback_url,
            callback_token,
        } => Ok(nexus_v1::Request {
            header: BTreeMap::new(),
            scheduled_time: scheduled_time.map(to_proto_timestamp),
            capabilities: Some(nexus_v1::request::Capabilities {
                temporal_failure_responses: true,
            }),
            endpoint: String::new(),
            variant: Some(nexus_v1::request::Variant::StartOperation(
                nexus_v1::StartOperationRequest {
                    service: service.clone(),
                    operation: operation.clone(),
                    request_id: request_id.clone(),
                    // Emit the completion callback so the handler SDK's
                    // WorkflowRunOperation reads `nexusOptions.CallbackURL` /
                    // `CallbackHeader` and registers the callback on its backing
                    // workflow (nexus-async-completion task 5.1). Absent when no
                    // callback was attached (e.g. an External-target dispatch).
                    callback: callback_url.clone().unwrap_or_default(),
                    payload: payload.as_ref().map(payload_from_domain),
                    callback_header: callback_token
                        .as_ref()
                        .map(|token| {
                            BTreeMap::from([(
                                tokeira_runtime::TEMPORAL_CALLBACK_TOKEN_HEADER.to_string(),
                                token.clone(),
                            )])
                        })
                        .unwrap_or_default(),
                    links: Vec::new(),
                },
            )),
        }),
        NexusTaskRequest::CancelOperation {
            service,
            operation,
            operation_id,
        } => Ok(nexus_v1::Request {
            header: BTreeMap::new(),
            scheduled_time: None,
            capabilities: Some(nexus_v1::request::Capabilities {
                temporal_failure_responses: true,
            }),
            endpoint: String::new(),
            variant: Some(nexus_v1::request::Variant::CancelOperation(
                nexus_v1::CancelOperationRequest {
                    service: service.clone(),
                    operation: operation.clone(),
                    operation_id: operation_id.clone(),
                    operation_token: operation_id.clone(),
                },
            )),
        }),
    }
}

pub fn proto_response_to_resolution(
    response: nexus_v1::Response,
    expected_operation_id: &str,
) -> Result<NexusResolution, NexusTranslateError> {
    match response.variant {
        Some(nexus_v1::response::Variant::StartOperation(start)) => {
            proto_start_response_to_resolution(start, expected_operation_id)
        }
        Some(nexus_v1::response::Variant::CancelOperation(_)) => Ok(NexusResolution::Canceled),
        None => Err(NexusTranslateError::MissingField("response.variant")),
    }
}

// v1.62-sync: reads deprecated `start_operation_response::Async::operation_id`
// for wire-compat with v0.4-era Nexus clients. v1.62 renames to
// `operation_token`; the mismatch-check above still uses the deprecated
// field because the expected_operation_id callers supply is the v0.4 shape.
// Migration to `operation_token` is task 4.7 / 8.1.
#[allow(deprecated)]
pub fn proto_start_response_to_resolution(
    response: nexus_v1::StartOperationResponse,
    expected_operation_id: &str,
) -> Result<NexusResolution, NexusTranslateError> {
    match response.variant {
        Some(nexus_v1::start_operation_response::Variant::SyncSuccess(sync)) => {
            // Worker-handler (gRPC) path: the SDK's `Sync.links` are Nexus links
            // (url+type), distinct from the kernel's structured `common.v1.Link`.
            // Converting them requires parsing the `temporal://` link scheme, which
            // is out of scope here — this spec (runtime-nexus-http-client) covers the
            // External HTTP path only. Pre-Wave-1 this path carried no links at all,
            // so emitting empty preserves behaviour rather than regressing it; the
            // worker-path link conversion is tracked separately.
            Ok(NexusResolution::Completed {
                result: single_payload_to_payloads(sync.payload),
                links: Vec::new(),
            })
        }
        Some(nexus_v1::start_operation_response::Variant::AsyncSuccess(async_success)) => {
            if async_success.operation_id != expected_operation_id {
                tracing::warn!(
                    expected_operation_id,
                    returned_operation_id = async_success.operation_id,
                    "nexus async start returned mismatched operation_id"
                );
            }
            // The handler's async token: v1.62 carries it in `operation_token`;
            // fall back to the deprecated `operation_id` for v0.4-era handlers
            // that only set the old field. This becomes the started event's
            // operation_token (the caller's NexusOperationExecution.OperationToken).
            let operation_token = if async_success.operation_token.is_empty() {
                async_success.operation_id.clone()
            } else {
                async_success.operation_token.clone()
            };
            // See the SyncSuccess arm: worker-path Nexus links are not converted here.
            Ok(NexusResolution::Started {
                operation_token,
                links: Vec::new(),
            })
        }
        Some(nexus_v1::start_operation_response::Variant::OperationError(error)) => {
            Ok(NexusResolution::Failed {
                failure: nexus_failure_to_kernel_payload(
                    error.operation_state,
                    error.failure.as_ref(),
                )?,
            })
        }
        Some(nexus_v1::start_operation_response::Variant::Failure(failure)) => {
            Ok(NexusResolution::Failed {
                failure: failure_to_payload(&failure),
            })
        }
        None => Err(NexusTranslateError::MissingField(
            "start_operation_response.variant",
        )),
    }
}

pub fn proto_handler_error_to_resolution(
    error: nexus_v1::HandlerError,
) -> Result<NexusResolution, NexusTranslateError> {
    Ok(NexusResolution::Failed {
        failure: nexus_failure_to_kernel_payload(error.error_type, error.failure.as_ref())?,
    })
}

/// True when a `temporal.api.failure.v1.Failure` carries a `NexusHandlerFailureInfo`.
///
/// v1.31.0's frontend rejects a `RespondNexusTaskFailedRequest.failure` that does
/// not contain one (`workflow_handler.go:6096 @ v1.31.0`).
pub fn failure_has_nexus_handler_info(failure: &failure_proto::Failure) -> bool {
    matches!(
        failure.failure_info,
        Some(failure_proto::failure::FailureInfo::NexusHandlerFailureInfo(_))
    )
}

/// Build the caller-facing `NexusResolution::Failed` for a worker handler failure,
/// wrapping the handler's `failure` (the cause) in a `NexusOperationFailureInfo`,
/// exactly as v1.31.0 records it on the `NexusOperationFailed` event
/// (`createNexusOperationFailure`, `components/nexusoperations/executors.go @
/// v1.31.0`: outer message "nexus operation completed unsuccessfully", a
/// `NexusOperationFailureInfo{endpoint, service, operation, scheduled_event_id}`,
/// and `cause` = the handler failure). The result is stored as the opaque
/// `temporal/failure+proto` payload the history serializer round-trips verbatim,
/// so the SDK caller decodes the full chain (NexusOperationError → HandlerError →
/// ApplicationError). `operation_token` is empty: a start that failed never
/// produced one.
pub fn wrap_handler_failure_as_resolution(
    cause: failure_proto::Failure,
    endpoint: String,
    service: String,
    operation: String,
    scheduled_event_id: i64,
) -> NexusResolution {
    let wrapped = failure_proto::Failure {
        message: "nexus operation completed unsuccessfully".to_string(),
        failure_info: Some(
            failure_proto::failure::FailureInfo::NexusOperationExecutionFailureInfo(
                failure_proto::NexusOperationFailureInfo {
                    scheduled_event_id,
                    endpoint,
                    service,
                    operation,
                    operation_id: String::new(),
                    operation_token: String::new(),
                },
            ),
        ),
        cause: Some(Box::new(cause)),
        ..Default::default()
    };
    NexusResolution::Failed {
        failure: failure_to_payload(&wrapped),
    }
}

pub fn nexus_failure_to_kernel_payload(
    error_type: String,
    failure: Option<&nexus_v1::Failure>,
) -> Result<Payload, NexusTranslateError> {
    let envelope = NexusFailureEnvelope {
        error_type,
        message: failure
            .map(|value| value.message.clone())
            .unwrap_or_default(),
        metadata: failure
            .map(|value| value.metadata.clone())
            .unwrap_or_default(),
        details_hex: failure
            .map(|value| hex_encode(&value.details))
            .unwrap_or_default(),
    };
    let data = serde_json::to_vec(&envelope).map_err(|error| {
        NexusTranslateError::InvalidArgument(format!(
            "failed to encode nexus failure envelope: {error}"
        ))
    })?;
    let mut metadata = BTreeMap::new();
    metadata.insert("encoding".to_string(), "json/plain".to_string());
    Ok(Payload { data, metadata })
}

fn single_payload_to_payloads(payload: Option<tokeira_proto::public::common::Payload>) -> Payloads {
    match payload {
        Some(payload) => Payloads(vec![payload_to_domain(&payload)]),
        None => Payloads::default(),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

pub fn broker_queue(
    namespace: &str,
    task_queue: &str,
) -> (tokeira_types::NamespaceId, TaskQueueName) {
    (
        crate::translate::to_internal::namespace_id_for(namespace),
        TaskQueueName(task_queue.to_string()),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use proptest::prelude::*;
    use time::OffsetDateTime;
    use tokeira_kernel::NexusResolution;
    use tokeira_proto::public::common::Payload as ProtoPayload;

    use super::*;

    fn payload_with_bytes(bytes: Vec<u8>) -> Payload {
        Payload {
            data: bytes,
            metadata: BTreeMap::new(),
        }
    }

    #[test]
    fn failure_with_nexus_handler_info_is_recognized() {
        let with = failure_proto::Failure {
            failure_info: Some(
                failure_proto::failure::FailureInfo::NexusHandlerFailureInfo(
                    failure_proto::NexusHandlerFailureInfo {
                        r#type: "INTERNAL".to_string(),
                        retry_behavior: 0,
                    },
                ),
            ),
            ..Default::default()
        };
        assert!(failure_has_nexus_handler_info(&with));
        // A bare failure (no NexusHandlerFailureInfo) must be rejected, matching
        // v1.31.0's frontend validation.
        let without = failure_proto::Failure {
            message: "boom".to_string(),
            ..Default::default()
        };
        assert!(!failure_has_nexus_handler_info(&without));
    }

    #[test]
    fn handler_failure_wraps_into_nexus_operation_failure_info() {
        use tokeira_proto::conversions::common::payload_to_failure;
        // The handler's failure: NexusHandlerFailureInfo with an ApplicationFailureInfo
        // cause typed WorkflowExecutionAlreadyStarted (the conflict-policy case).
        let cause = failure_proto::Failure {
            message: "already started".to_string(),
            failure_info: Some(
                failure_proto::failure::FailureInfo::NexusHandlerFailureInfo(
                    failure_proto::NexusHandlerFailureInfo {
                        r#type: "INTERNAL".to_string(),
                        retry_behavior: 0,
                    },
                ),
            ),
            cause: Some(Box::new(failure_proto::Failure {
                message: "already started".to_string(),
                failure_info: Some(failure_proto::failure::FailureInfo::ApplicationFailureInfo(
                    failure_proto::ApplicationFailureInfo {
                        r#type: "WorkflowExecutionAlreadyStarted".to_string(),
                        ..Default::default()
                    },
                )),
                ..Default::default()
            })),
            ..Default::default()
        };
        let NexusResolution::Failed { failure } = wrap_handler_failure_as_resolution(
            cause,
            "endpoint-1".to_string(),
            "service".to_string(),
            "op".to_string(),
            42,
        ) else {
            panic!("expected Failed");
        };
        // The stored payload round-trips (via payload_to_failure, as the history
        // serializer does) to a NexusOperationFailureInfo wrapper carrying the
        // operation metadata, whose cause preserves the handler chain so the SDK
        // caller decodes ApplicationError "WorkflowExecutionAlreadyStarted".
        let decoded = payload_to_failure(&failure);
        let Some(failure_proto::failure::FailureInfo::NexusOperationExecutionFailureInfo(info)) =
            &decoded.failure_info
        else {
            panic!(
                "expected NexusOperationFailureInfo wrapper, got {:?}",
                decoded.failure_info
            );
        };
        assert_eq!(info.endpoint, "endpoint-1");
        assert_eq!(info.service, "service");
        assert_eq!(info.operation, "op");
        assert_eq!(info.scheduled_event_id, 42);
        let handler = decoded.cause.expect("handler cause present");
        assert!(matches!(
            handler.failure_info,
            Some(failure_proto::failure::FailureInfo::NexusHandlerFailureInfo(_))
        ));
        let app = handler.cause.expect("application cause present");
        match app.failure_info {
            Some(failure_proto::failure::FailureInfo::ApplicationFailureInfo(a)) => {
                assert_eq!(a.r#type, "WorkflowExecutionAlreadyStarted");
            }
            other => panic!("expected ApplicationFailureInfo, got {other:?}"),
        }
    }

    // Feature: edge-nexus-task-transport, Property 3: Request translation preserves stored fields
    proptest! {
        #[test]
        fn property_request_translation_preserves_start_fields(
            service in "[a-z]{1,12}",
            operation in "[a-z]{1,12}",
            request_id in "[a-z0-9_-]{1,16}",
            payload_bytes in proptest::collection::vec(any::<u8>(), 0..32),
            seconds in 0i64..10_000,
        ) {
            let request = NexusTaskRequest::StartOperation {
                service: service.clone(),
                operation: operation.clone(),
                request_id: request_id.clone(),
                payload: Some(payload_with_bytes(payload_bytes.clone())),
                scheduled_time: Some(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(seconds)),
                callback_url: None,
                callback_token: None,
            };

            let proto = nexus_task_to_proto_request(&request).expect("translation should succeed");
            let start = match proto.variant.expect("variant") {
                nexus_v1::request::Variant::StartOperation(start) => start,
                other => panic!("unexpected variant: {other:?}"),
            };

            prop_assert_eq!(proto.header, BTreeMap::new());
            prop_assert!(proto.scheduled_time.is_some());
            prop_assert_eq!(start.service, service);
            prop_assert_eq!(start.operation, operation);
            prop_assert_eq!(start.request_id, request_id);
            prop_assert_eq!(start.payload.expect("payload").data, payload_bytes);
            prop_assert_eq!(start.callback, "");
            prop_assert!(start.callback_header.is_empty());
            prop_assert!(start.links.is_empty());
        }

        #[test]
        fn property_request_translation_preserves_cancel_fields(
            service in "[a-z]{1,12}",
            operation in "[a-z]{1,12}",
            operation_id in "[a-z0-9_-]{1,16}",
        ) {
            let request = NexusTaskRequest::CancelOperation {
                service: service.clone(),
                operation: operation.clone(),
                operation_id: operation_id.clone(),
            };

            let proto = nexus_task_to_proto_request(&request).expect("translation should succeed");
            let cancel = match proto.variant.expect("variant") {
                nexus_v1::request::Variant::CancelOperation(cancel) => cancel,
                other => panic!("unexpected variant: {other:?}"),
            };

            prop_assert!(proto.header.is_empty());
            prop_assert!(proto.scheduled_time.is_none());
            prop_assert_eq!(cancel.service, service);
            prop_assert_eq!(cancel.operation, operation);
            prop_assert_eq!(cancel.operation_id, operation_id.clone());
            prop_assert_eq!(cancel.operation_token, operation_id);
        }
    }

    // Feature: edge-nexus-task-transport, Property 4: Response translation correctness
    proptest! {
        #[test]
        fn property_response_translation_correctness(
            operation_id in "[a-z0-9_-]{1,16}",
            payload_bytes in proptest::collection::vec(any::<u8>(), 0..32),
            error_type in "[A-Za-z]{1,16}",
            message in ".*",
        ) {
            let sync = nexus_v1::Response {
                variant: Some(nexus_v1::response::Variant::StartOperation(
                    nexus_v1::StartOperationResponse {
                        variant: Some(nexus_v1::start_operation_response::Variant::SyncSuccess(
                            nexus_v1::start_operation_response::Sync {
                                payload: Some(ProtoPayload {
                                    metadata: BTreeMap::new(),
                                    data: payload_bytes.clone(),
                                    external_payloads: Vec::new(),
                                }),
                                links: Vec::new(),
                            },
                        )),
                    },
                )),
            };
            match proto_response_to_resolution(sync, &operation_id).expect("sync success") {
                NexusResolution::Completed { result, .. } => {
                    prop_assert_eq!(result.0.len(), 1);
                    prop_assert_eq!(result.0[0].data.clone(), payload_bytes.clone());
                }
                other => panic!("unexpected sync resolution: {other:?}"),
            }

            let async_response = nexus_v1::Response {
                variant: Some(nexus_v1::response::Variant::StartOperation(
                    nexus_v1::StartOperationResponse {
                        variant: Some(nexus_v1::start_operation_response::Variant::AsyncSuccess(
                            nexus_v1::start_operation_response::Async {
                                operation_id: operation_id.clone(),
                                links: Vec::new(),
                                operation_token: operation_id.clone(),
                            },
                        )),
                    },
                )),
            };
            prop_assert_eq!(
                proto_response_to_resolution(async_response, &operation_id).expect("async success"),
                NexusResolution::Started {
                    operation_token: operation_id.clone(),
                    links: Vec::new()
                }
            );

            let cancel = nexus_v1::Response {
                variant: Some(nexus_v1::response::Variant::CancelOperation(
                    nexus_v1::CancelOperationResponse {},
                )),
            };
            prop_assert_eq!(
                proto_response_to_resolution(cancel, &operation_id).expect("cancel"),
                NexusResolution::Canceled
            );

            let handler_error = nexus_v1::HandlerError {
                error_type: error_type.clone(),
                failure: Some(nexus_v1::Failure {
                    message: message.clone(),
                    stack_trace: String::new(),
                    metadata: BTreeMap::new(),
                    details: payload_bytes.clone(),
                    cause: None,
                }),
                retry_behavior: 0,
            };
            match proto_handler_error_to_resolution(handler_error).expect("handler error") {
                NexusResolution::Failed { failure } => {
                    let envelope: NexusFailureEnvelope =
                        serde_json::from_slice(&failure.data).expect("envelope should decode");
                    prop_assert_eq!(envelope.error_type, error_type);
                    prop_assert_eq!(envelope.message, message);
                    prop_assert_eq!(envelope.details_hex, hex_encode(&payload_bytes));
                }
                other => panic!("unexpected handler resolution: {other:?}"),
            }
        }
    }
}
