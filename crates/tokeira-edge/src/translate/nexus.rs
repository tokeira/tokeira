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
    /// SDK poller-count hint derived from remaining Nexus queue pressure.
    pub poller_scaling_decision: Option<i32>,
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
        poller_scaling_decision: resp.poller_scaling_decision.map(|delta| {
            tokeira_proto::public::temporal::api::taskqueue::v1::PollerScalingDecision {
                poll_request_delta_suggestion: delta,
            }
        }),
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
            operation_token,
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
                    // Deprecated wire field keeps tokeira's operation id; the modern
                    // operation_token carries the handler-issued async token so a
                    // WorkflowRunOperation handler can unmarshal it.
                    operation_id: operation_id.clone(),
                    operation_token: operation_token.clone(),
                },
            )),
        }),
    }
}

/// Translate a worker's `RespondNexusTaskCompleted` response into a caller-op resolution.
///
/// Returns `None` for a `CancelOperation` response: a cancel-ack does NOT resolve the
/// operation. v1.31.0 decouples the two — `EventCancelationSucceeded`
/// (`components/nexusoperations/statemachine.go:671 @ v1.31.0`) only advances the cancelation
/// sub-machine; the operation resolves solely via its completion when the backing workflow
/// closes (`GetNexusCompletion`), and a completion that already resolved the op wins over a
/// later cancel (`statemachine.go:424`). Resolving on cancel-ack would race the completion
/// and emit a caller resolution the SDK does not expect.
pub fn proto_response_to_resolution(
    response: nexus_v1::Response,
    expected_operation_id: &str,
    op: &NexusOperationContext,
) -> Result<Option<NexusResolution>, NexusTranslateError> {
    match response.variant {
        Some(nexus_v1::response::Variant::StartOperation(start)) => {
            proto_start_response_to_resolution(start, expected_operation_id, op).map(Some)
        }
        Some(nexus_v1::response::Variant::CancelOperation(_)) => Ok(None),
        None => Err(NexusTranslateError::MissingField("response.variant")),
    }
}

/// The caller's pending Nexus operation identity, needed to wrap a worker-reported
/// operation failure in a `NexusOperationFailureInfo` (so the SDK decodes a
/// `NexusOperationError`). Empty fields are tolerated (a missing/raced pending op) — the
/// inner cause chain still decodes.
#[derive(Clone, Debug, Default)]
pub struct NexusOperationContext {
    pub endpoint: String,
    pub service: String,
    pub operation: String,
    pub scheduled_event_id: i64,
}

/// Build a terminal `NexusResolution::Failed` for a worker operation-unsuccessful response,
/// wrapping the operation failure in `NexusOperationFailureInfo` exactly as
/// `wrap_handler_failure_as_resolution` does for handler errors, so the caller decodes a
/// `NexusOperationError`. Mirrors v1.31.0's `createNexusOperationFailure` +
/// `NexusFailureToTemporalFailure` `nexus.OperationError` case (`common/nexus/failure.go @
/// v1.31.0`): a failed operation becomes a non-retryable `ApplicationFailureInfo{Type:
/// "OperationError"}` carrying the failure message; a canceled one becomes a
/// `CanceledFailureInfo`.
fn operation_error_to_resolution(
    operation_state: &str,
    failure: Option<&nexus_v1::Failure>,
    op: &NexusOperationContext,
) -> NexusResolution {
    let cause_info = if operation_state == "canceled" {
        failure_proto::failure::FailureInfo::CanceledFailureInfo(
            failure_proto::CanceledFailureInfo::default(),
        )
    } else {
        failure_proto::failure::FailureInfo::ApplicationFailureInfo(
            failure_proto::ApplicationFailureInfo {
                r#type: "OperationError".to_string(),
                non_retryable: true,
                ..Default::default()
            },
        )
    };
    let cause = failure_proto::Failure {
        message: failure.map(|f| f.message.clone()).unwrap_or_default(),
        failure_info: Some(cause_info),
        ..Default::default()
    };
    wrap_handler_failure_as_resolution(
        cause,
        op.endpoint.clone(),
        op.service.clone(),
        op.operation.clone(),
        op.scheduled_event_id,
    )
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
    op: &NexusOperationContext,
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
            // Operation-unsuccessful: wrap in NexusOperationFailureInfo so the caller
            // decodes a NexusOperationError (v1.31.0 handleOperationError /
            // createNexusOperationFailure), rather than the bare json envelope.
            Ok(operation_error_to_resolution(
                &error.operation_state,
                error.failure.as_ref(),
                op,
            ))
        }
        Some(nexus_v1::start_operation_response::Variant::Failure(failure)) => {
            // A bare `Failure` start response is an operation-unsuccessful outcome carrying
            // the handler's already-built temporal failure as the cause (e.g. an SDK that
            // sends `nexus.NewOperationFailedError` as an ApplicationFailure here rather than
            // via the OperationError variant). Wrap it in NexusOperationFailureInfo so the
            // caller decodes a NexusOperationError, exactly like the OperationError arm and
            // the worker handler-error path; storing it raw would surface only the cause.
            Ok(wrap_handler_failure_as_resolution(
                failure,
                op.endpoint.clone(),
                op.service.clone(),
                op.operation.clone(),
                op.scheduled_event_id,
            ))
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

/// `failure_source` metric-tag values for an outbound Nexus request. `worker` marks a
/// failure the handler/worker reported; otherwise the tag defaults to `_unknown_`
/// (`common/nexus/failure.go:25-26`, `common/metrics/tags.go:66,264-268 @ v1.31.0`).
pub const FAILURE_SOURCE_WORKER: &str = "worker";
pub const FAILURE_SOURCE_UNKNOWN: &str = "_unknown_";

/// The `nexus_outbound_requests` / `nexus_outbound_latency` tag triple for one caller-side
/// Nexus request, mirroring v1.31.0's `metrics.NexusMethodTag` + `startCallOutcomeTag` +
/// `metrics.FailureSourceTag` (`components/nexusoperations/executors.go:320-331,899-933 @
/// v1.31.0`).
#[derive(Clone, Debug, PartialEq)]
pub struct NexusOutboundTags {
    /// `StartOperation` or `CancelOperation`.
    pub method: &'static str,
    /// `startCallOutcomeTag` value: `successful` / `pending` / `operation-unsuccessful:<state>`
    /// / `handler-error:<TYPE>`.
    pub outcome: String,
    /// [`FAILURE_SOURCE_WORKER`] when the worker reported the failure, else
    /// [`FAILURE_SOURCE_UNKNOWN`].
    pub failure_source: &'static str,
}

/// Derive the outbound-metric tags for a worker's `RespondNexusTaskCompleted` response —
/// the terminal outcome of a dispatched StartOperation/CancelOperation. `None` when the
/// response variant is absent (the handler rejects that as `BadRequest` before resolving,
/// so no outbound outcome is recorded). Mirrors `startCallOutcomeTag`'s non-error arms plus
/// the `OperationError` → `operation-unsuccessful:<state>` mapping (`executors.go:920-922,
/// 930-933 @ v1.31.0`).
pub fn nexus_completed_outbound_tags(response: &nexus_v1::Response) -> Option<NexusOutboundTags> {
    match response.variant.as_ref()? {
        nexus_v1::response::Variant::CancelOperation(_) => Some(NexusOutboundTags {
            method: "CancelOperation",
            outcome: "successful".to_string(),
            failure_source: FAILURE_SOURCE_UNKNOWN,
        }),
        nexus_v1::response::Variant::StartOperation(start) => {
            let tags = match start.variant.as_ref()? {
                nexus_v1::start_operation_response::Variant::SyncSuccess(_) => NexusOutboundTags {
                    method: "StartOperation",
                    outcome: "successful".to_string(),
                    failure_source: FAILURE_SOURCE_UNKNOWN,
                },
                nexus_v1::start_operation_response::Variant::AsyncSuccess(_) => NexusOutboundTags {
                    method: "StartOperation",
                    outcome: "pending".to_string(),
                    failure_source: FAILURE_SOURCE_UNKNOWN,
                },
                nexus_v1::start_operation_response::Variant::OperationError(error) => {
                    NexusOutboundTags {
                        method: "StartOperation",
                        outcome: format!(
                            "operation-unsuccessful:{}",
                            outbound_operation_state(&error.operation_state)
                        ),
                        failure_source: FAILURE_SOURCE_WORKER,
                    }
                }
                // A bare `Failure` start response (no Nexus operation state) is still an
                // unsuccessful operation; v1.31.0's `nexus.OperationError` carries a state,
                // so default to `failed` when none is present.
                nexus_v1::start_operation_response::Variant::Failure(_) => NexusOutboundTags {
                    method: "StartOperation",
                    outcome: "operation-unsuccessful:failed".to_string(),
                    failure_source: FAILURE_SOURCE_WORKER,
                },
            };
            Some(tags)
        }
    }
}

/// Derive the outbound-metric tags for a worker's `RespondNexusTaskFailed` — always a
/// handler error (`handler-error:<TYPE>`, `executors.go:925-927 @ v1.31.0`), with the
/// failure attributed to the `worker`. The type comes from the modern `failure`'s
/// `NexusHandlerFailureInfo` or the deprecated `error.error_type`.
///
/// The dispatched task's method (Start vs Cancel) is not carried on the task token, and a
/// worker failure response is a StartOperation in every path the corpus exercises; a
/// CancelOperation handler-failure (untested) would be tagged `StartOperation` until the
/// task kind is threaded onto the token.
pub fn nexus_failed_outbound_tags(
    failure: Option<&failure_proto::Failure>,
    error: Option<&nexus_v1::HandlerError>,
) -> NexusOutboundTags {
    NexusOutboundTags {
        method: "StartOperation",
        outcome: format!("handler-error:{}", handler_error_type(failure, error)),
        failure_source: FAILURE_SOURCE_WORKER,
    }
}

/// The Nexus operation state for an `operation-unsuccessful:<state>` outcome, defaulting to
/// `failed` when the worker left it empty (the only terminal-unsuccessful state the corpus
/// asserts).
fn outbound_operation_state(state: &str) -> &str {
    if state.is_empty() { "failed" } else { state }
}

/// The handler-error type string for an outbound `handler-error:<TYPE>` outcome, read from
/// the modern `failure`'s `NexusHandlerFailureInfo.type` first, then the deprecated
/// `HandlerError.error_type`, defaulting to `INTERNAL` (v1.31.0 treats an unclassified
/// handler failure as internal).
fn handler_error_type(
    failure: Option<&failure_proto::Failure>,
    error: Option<&nexus_v1::HandlerError>,
) -> String {
    if let Some(failure_proto::failure::FailureInfo::NexusHandlerFailureInfo(info)) =
        failure.and_then(|failure| failure.failure_info.as_ref())
        && !info.r#type.is_empty()
    {
        return info.r#type.clone();
    }
    if let Some(error) = error
        && !error.error_type.is_empty()
    {
        return error.error_type.clone();
    }
    "INTERNAL".to_string()
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

/// Whether a worker-reported handler failure is retryable, mirroring v1.31.0
/// `HandlerError.Retryable()` (`github.com/nexus-rpc/sdk-go@v0.6.0/nexus/errors.go:255-279`,
/// pinned at `v1.31.0:go.mod:40`): an explicit `retry_behavior` overrides, else the per-type
/// default — `BAD_REQUEST / UNAUTHENTICATED / UNAUTHORIZED / NOT_FOUND / NOT_IMPLEMENTED /
/// CONFLICT` are terminal, everything else retryable. This drives `BACKING_OFF` vs terminal on
/// `StartOperation` (`components/nexusoperations/executors.go:499-532 @ v1.31.0`). A failure
/// without a `NexusHandlerFailureInfo` is treated as retryable (v1.31.0's non-`HandlerError`
/// default).
pub fn nexus_handler_failure_retryable(failure: &failure_proto::Failure) -> bool {
    use tokeira_proto::public::temporal::api::enums::v1::NexusHandlerErrorRetryBehavior;
    let Some(failure_proto::failure::FailureInfo::NexusHandlerFailureInfo(info)) =
        failure.failure_info.as_ref()
    else {
        return true;
    };
    match info.retry_behavior() {
        NexusHandlerErrorRetryBehavior::Retryable => true,
        NexusHandlerErrorRetryBehavior::NonRetryable => false,
        NexusHandlerErrorRetryBehavior::Unspecified => !matches!(
            info.r#type.as_str(),
            "BAD_REQUEST"
                | "UNAUTHENTICATED"
                | "UNAUTHORIZED"
                | "NOT_FOUND"
                | "NOT_IMPLEMENTED"
                | "CONFLICT"
        ),
    }
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
    Ok(Payload {
        data,
        metadata,
        external_payloads: Vec::new(),
    })
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
            external_payloads: Vec::new(),
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
    fn outbound_tags_map_v1_31_0_outcome_taxonomy() {
        // RespondNexusTaskCompleted: async start → `pending`, no failure source.
        let async_started = nexus_v1::Response {
            variant: Some(nexus_v1::response::Variant::StartOperation(
                nexus_v1::StartOperationResponse {
                    variant: Some(nexus_v1::start_operation_response::Variant::AsyncSuccess(
                        nexus_v1::start_operation_response::Async {
                            operation_id: "op".to_string(),
                            operation_token: "op".to_string(),
                            links: Vec::new(),
                        },
                    )),
                },
            )),
        };
        let tags = nexus_completed_outbound_tags(&async_started).expect("variant present");
        assert_eq!(tags.method, "StartOperation");
        assert_eq!(tags.outcome, "pending");
        assert_eq!(tags.failure_source, FAILURE_SOURCE_UNKNOWN);

        // A sync operation failure → `operation-unsuccessful:<state>`, attributed to the worker.
        let op_error = nexus_v1::Response {
            variant: Some(nexus_v1::response::Variant::StartOperation(
                nexus_v1::StartOperationResponse {
                    variant: Some(nexus_v1::start_operation_response::Variant::OperationError(
                        nexus_v1::UnsuccessfulOperationError {
                            operation_state: "failed".to_string(),
                            failure: None,
                        },
                    )),
                },
            )),
        };
        let tags = nexus_completed_outbound_tags(&op_error).expect("variant present");
        assert_eq!(tags.method, "StartOperation");
        assert_eq!(tags.outcome, "operation-unsuccessful:failed");
        assert_eq!(tags.failure_source, FAILURE_SOURCE_WORKER);

        // CancelOperation completes successfully.
        let cancel = nexus_v1::Response {
            variant: Some(nexus_v1::response::Variant::CancelOperation(
                nexus_v1::CancelOperationResponse {},
            )),
        };
        let tags = nexus_completed_outbound_tags(&cancel).expect("variant present");
        assert_eq!(tags.method, "CancelOperation");
        assert_eq!(tags.outcome, "successful");
        assert_eq!(tags.failure_source, FAILURE_SOURCE_UNKNOWN);

        // A missing response variant carries no outbound outcome (rejected as BadRequest
        // before resolution, so nothing is recorded).
        assert!(nexus_completed_outbound_tags(&nexus_v1::Response { variant: None }).is_none());

        // RespondNexusTaskFailed: handler-error:<TYPE> from the modern failure's
        // NexusHandlerFailureInfo, attributed to the worker.
        let failure = failure_proto::Failure {
            failure_info: Some(
                failure_proto::failure::FailureInfo::NexusHandlerFailureInfo(
                    failure_proto::NexusHandlerFailureInfo {
                        r#type: "BAD_REQUEST".to_string(),
                        retry_behavior: 0,
                    },
                ),
            ),
            ..Default::default()
        };
        let tags = nexus_failed_outbound_tags(Some(&failure), None);
        assert_eq!(tags.method, "StartOperation");
        assert_eq!(tags.outcome, "handler-error:BAD_REQUEST");
        assert_eq!(tags.failure_source, FAILURE_SOURCE_WORKER);

        // The deprecated `error` field is the fallback type source.
        let handler_error = nexus_v1::HandlerError {
            error_type: "NOT_FOUND".to_string(),
            failure: None,
            retry_behavior: 0,
        };
        assert_eq!(
            nexus_failed_outbound_tags(None, Some(&handler_error)).outcome,
            "handler-error:NOT_FOUND"
        );

        // Neither present → defaults to INTERNAL (v1.31.0 treats an unclassified handler
        // failure as internal).
        assert_eq!(
            nexus_failed_outbound_tags(None, None).outcome,
            "handler-error:INTERNAL"
        );
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
                operation_token: operation_id.clone(),
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
            match proto_response_to_resolution(sync, &operation_id, &NexusOperationContext::default())
                .expect("sync success")
                .expect("sync success resolves the op")
            {
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
                proto_response_to_resolution(async_response, &operation_id, &NexusOperationContext::default()).expect("async success"),
                Some(NexusResolution::Started {
                    operation_token: operation_id.clone(),
                    links: Vec::new()
                })
            );

            let cancel = nexus_v1::Response {
                variant: Some(nexus_v1::response::Variant::CancelOperation(
                    nexus_v1::CancelOperationResponse {},
                )),
            };
            // A cancel-ack does not resolve the operation (v1.31.0 decouples cancel-ack from
            // resolution; the op resolves via its completion).
            prop_assert_eq!(
                proto_response_to_resolution(cancel, &operation_id, &NexusOperationContext::default()).expect("cancel"),
                None
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
