use tokeira_proto::conversions::ProtoConversionError;
use tonic::{
    Code, Status,
    metadata::{MetadataMap, MetadataValue},
};

use crate::errors::EdgeError;

impl From<EdgeError> for Status {
    fn from(err: EdgeError) -> Self {
        match err {
            EdgeError::BadRequest(message) => Status::invalid_argument(message),
            EdgeError::Unimplemented(message) => Status::unimplemented(message),
            EdgeError::NotFound(message) => Status::not_found(message),
            EdgeError::AlreadyExists(message) => Status::already_exists(message),
            EdgeError::ResourceExhausted(message) => Status::resource_exhausted(message),
            EdgeError::WorkflowClosing => workflow_closing_status(),
            EdgeError::ConsistentQueryBufferExceeded => busy_workflow_resource_exhausted_status(
                "consistent query buffer is full, this may be caused by too many queries and \
                 workflow not able to process query fast enough"
                    .to_owned(),
            ),
            EdgeError::WorkflowNotReady(message) => workflow_not_ready_status(message),
            EdgeError::QueryFailed { message, failure } => query_failed_status(message, failure),
            EdgeError::QueryTimedOut => Status::deadline_exceeded(err_static(
                "query timed out before a worker could process it",
            )),
            EdgeError::Unauthorized(message) => Status::unauthenticated(message),
            EdgeError::Forbidden { action, namespace } => {
                let message = match namespace {
                    Some(namespace) => {
                        format!("forbidden action `{action}` for namespace `{namespace}`")
                    }
                    None => format!("forbidden action `{action}`"),
                };
                Status::permission_denied(message)
            }
            EdgeError::NamespaceNotFound(namespace) => namespace_not_found_status(namespace),
            EdgeError::NamespaceDeleted(namespace) => Status::failed_precondition(namespace),
            EdgeError::NamespaceAlreadyExists(namespace) => Status::already_exists(namespace),
            EdgeError::WorkflowNotFound {
                namespace: _,
                workflow_id,
            } => {
                // v1.31.0's current-execution lookup rewrites any missing-
                // workflow NotFound to this exact message — workflow id only,
                // no namespace, no run id (`GetCurrentExecution`,
                // common/persistence/execution_manager.go:855-859 @ v1.31.0).
                Status::not_found(format!("workflow not found for ID: {workflow_id}"))
            }
            EdgeError::ActivityNotFound {
                namespace,
                workflow_id,
                activity_id,
            } => Status::not_found(format!("{namespace}/{workflow_id}/{activity_id}")),
            EdgeError::ActivityNotStarted {
                namespace,
                workflow_id,
                activity_id,
            } => Status::failed_precondition(format!(
                "{namespace}/{workflow_id}/{activity_id} has not started"
            )),
            EdgeError::WorkflowAlreadyStarted {
                namespace,
                workflow_id,
                run_id,
            } => workflow_already_started_status(
                format!("{namespace}/{workflow_id} already started as {run_id}"),
                run_id,
            ),
            EdgeError::WorkflowStartRejected { message, run_id } => {
                workflow_already_started_status(message, run_id)
            }
            EdgeError::BatchOperationAlreadyExists { namespace, job_id } => {
                Status::already_exists(format!("{namespace}/{job_id}"))
            }
            EdgeError::BatchOperationNotFound { namespace, job_id } => {
                Status::not_found(format!("{namespace}/{job_id}"))
            }
            EdgeError::TooManyLongPolls => {
                Status::resource_exhausted(err_static("too many concurrent long polls"))
            }
            EdgeError::LongPollAdmissionTimeout => Status::deadline_exceeded(err_static(
                "long poll timed out while waiting for admission",
            )),
            EdgeError::RemoteRouteUnsupported { target } => Status::unavailable(format!(
                "request routed to remote target `{target}` but forwarding is not wired yet"
            )),
            EdgeError::NotShardOwner {
                bundle_id,
                current_epoch,
                current_owner_node_id,
            } => {
                let mut status = Status::aborted(format!(
                    "not shard owner for bundle {:?} at epoch {:?}",
                    bundle_id, current_epoch
                ));
                insert_metadata_value(
                    &mut status,
                    "tokeira-bundle-id",
                    bundle_id.0.to_string().as_str(),
                );
                insert_metadata_value(
                    &mut status,
                    "tokeira-current-epoch",
                    current_epoch.0.to_string().as_str(),
                );
                if let Some(owner) = current_owner_node_id {
                    insert_metadata_value(
                        &mut status,
                        "tokeira-current-owner",
                        owner.to_string().as_str(),
                    );
                }
                status
            }
            EdgeError::ActivityExecutionAlreadyStarted {
                message,
                run_id,
                start_request_id,
            } => activity_already_started_status(message, run_id, start_request_id),
            EdgeError::FailedPrecondition(message) => Status::failed_precondition(message),
            EdgeError::Internal(message) => Status::internal(message),
        }
    }
}

/// Build the typed `serviceerror.NamespaceNotFound` status expected by Temporal
/// clients. A bare gRPC `NOT_FOUND` is reconstructed as a generic service error;
/// the `NamespaceNotFoundFailure` detail is what makes `errors.As` succeed
/// (`serviceerror/namespace_not_found.go @ go.temporal.io/api v1.62`).
pub(crate) fn namespace_not_found_status(namespace: String) -> Status {
    use prost::Message as _;
    use tokeira_proto::public::temporal::api::errordetails::v1::NamespaceNotFoundFailure;

    let message = format!("Namespace {namespace} is not found.");
    let detail = ProtoAny {
        type_url: "type.googleapis.com/temporal.api.errordetails.v1.NamespaceNotFoundFailure"
            .to_owned(),
        value: NamespaceNotFoundFailure {
            namespace: namespace.clone(),
        }
        .encode_to_vec(),
    };
    let rpc_status = RpcStatus {
        code: Code::NotFound as i32,
        message: message.clone(),
        details: vec![detail],
    };
    Status::with_details_and_metadata(
        Code::NotFound,
        message,
        rpc_status.encode_to_vec().into(),
        MetadataMap::new(),
    )
}

/// Build the typed `WorkflowExecutionAlreadyStarted` gRPC status.
///
/// v1.31.0 returns an id-conflict (`WORKFLOW_ID_CONFLICT_POLICY_FAIL` / a non-reusable
/// id-reuse policy) as code `AlreadyExists` carrying a `WorkflowExecutionAlreadyStartedFailure`
/// detail (`go.temporal.io/api serviceerror/workflow_execution_already_started.go`). The
/// client reconstructs the typed `serviceerror.WorkflowExecutionAlreadyStarted` by reading the
/// `google.rpc.Status` from the `grpc-status-details-bin` trailer and matching code ==
/// AlreadyExists AND the first detail's type (`serviceerror/convert.go`). That typed error is
/// what lets a Nexus `WorkflowRunOperation` handler surface a `WorkflowExecutionAlreadyStarted`
/// application error to the caller (`TestNexusAsyncOperationWithMultipleCallers`); a bare
/// `AlreadyExists` would reconstruct as a generic error and lose the type. See
/// [`activity_already_started_status`] for the hand-encoded `Status`/`Any` wrapper rationale.
///
/// `start_request_id` is left empty: the `WorkflowAlreadyStarted` edge error does not carry
/// it, and SDK type reconstruction keys on the detail's *type*, not its fields.
/// Build v1.31.0's `consts.ErrWorkflowClosing` gRPC status: code
/// RESOURCE_EXHAUSTED carrying a `ResourceExhaustedFailure` detail with cause
/// BUSY_WORKFLOW and scope NAMESPACE (consts/const.go:62-67 @ v1.31.0). The Go
/// SDK reconstructs `serviceerror.ResourceExhausted` — including the Cause the
/// corpus asserts — from the `google.rpc.Status` detail in the trailer.
/// Build v1.31.0's `serviceerror.WorkflowNotReady` status: FAILED_PRECONDITION
/// with an empty `WorkflowNotReadyFailure` detail so the SDK reconstructs the
/// typed error (`serviceerror/workflow_not_ready.go` @ go.temporal.io/api v1.62).
fn workflow_not_ready_status(message: String) -> Status {
    use prost::Message as _;
    use tokeira_proto::public::temporal::api::errordetails::v1::WorkflowNotReadyFailure;

    let detail = ProtoAny {
        type_url: "type.googleapis.com/temporal.api.errordetails.v1.WorkflowNotReadyFailure"
            .to_owned(),
        value: WorkflowNotReadyFailure {}.encode_to_vec(),
    };
    let rpc_status = RpcStatus {
        code: Code::FailedPrecondition as i32,
        message: message.clone(),
        details: vec![detail],
    };
    Status::with_details_and_metadata(
        Code::FailedPrecondition,
        message,
        rpc_status.encode_to_vec().into(),
        MetadataMap::new(),
    )
}

/// Build v1.31.0's `serviceerror.QueryFailed` status: INVALID_ARGUMENT with a
/// `QueryFailedFailure` detail carrying the worker's structured failure
/// (`serviceerror/query_failed.go` @ go.temporal.io/api v1.62).
fn query_failed_status(message: String, failure: Option<tokeira_types::Payload>) -> Status {
    use prost::Message as _;
    use tokeira_proto::public::temporal::api::errordetails::v1::QueryFailedFailure;

    let failure_proto = failure
        .as_ref()
        .map(tokeira_proto::conversions::common::payload_to_failure);
    let detail = ProtoAny {
        type_url: "type.googleapis.com/temporal.api.errordetails.v1.QueryFailedFailure".to_owned(),
        value: QueryFailedFailure {
            failure: failure_proto,
        }
        .encode_to_vec(),
    };
    let rpc_status = RpcStatus {
        code: Code::InvalidArgument as i32,
        message: message.clone(),
        details: vec![detail],
    };
    Status::with_details_and_metadata(
        Code::InvalidArgument,
        message,
        rpc_status.encode_to_vec().into(),
        MetadataMap::new(),
    )
}

fn workflow_closing_status() -> Status {
    busy_workflow_resource_exhausted_status(
        "workflow operation can not be applied because workflow is closing".to_owned(),
    )
}

/// Build a RESOURCE_EXHAUSTED status carrying a `ResourceExhaustedFailure`
/// detail with cause BUSY_WORKFLOW and scope NAMESPACE — the shape shared by
/// v1.31.0's `ErrWorkflowClosing` and `ErrConsistentQueryBufferExceeded`
/// (consts/const.go:62-77 @ v1.31.0). The Go SDK reconstructs
/// `serviceerror.ResourceExhausted` — including the Cause the corpus asserts —
/// from the `google.rpc.Status` detail in the trailer.
fn busy_workflow_resource_exhausted_status(message: String) -> Status {
    use prost::Message as _;
    use tokeira_proto::public::temporal::api::{
        enums::v1::{ResourceExhaustedCause, ResourceExhaustedScope},
        errordetails::v1::ResourceExhaustedFailure,
    };

    let failure = ResourceExhaustedFailure {
        cause: ResourceExhaustedCause::BusyWorkflow as i32,
        scope: ResourceExhaustedScope::Namespace as i32,
    };
    let detail = ProtoAny {
        type_url: "type.googleapis.com/temporal.api.errordetails.v1.ResourceExhaustedFailure"
            .to_owned(),
        value: failure.encode_to_vec(),
    };
    let rpc_status = RpcStatus {
        code: Code::ResourceExhausted as i32,
        message: message.clone(),
        details: vec![detail],
    };
    Status::with_details_and_metadata(
        Code::ResourceExhausted,
        message,
        rpc_status.encode_to_vec().into(),
        MetadataMap::new(),
    )
}

fn workflow_already_started_status(message: String, run_id: String) -> Status {
    use prost::Message as _;
    use tokeira_proto::public::temporal::api::errordetails::v1::WorkflowExecutionAlreadyStartedFailure;

    let failure = WorkflowExecutionAlreadyStartedFailure {
        start_request_id: String::new(),
        run_id,
    };
    let detail = ProtoAny {
        type_url:
            "type.googleapis.com/temporal.api.errordetails.v1.WorkflowExecutionAlreadyStartedFailure"
                .to_owned(),
        value: failure.encode_to_vec(),
    };
    let rpc_status = RpcStatus {
        code: Code::AlreadyExists as i32,
        message: message.clone(),
        details: vec![detail],
    };
    Status::with_details_and_metadata(
        Code::AlreadyExists,
        message,
        rpc_status.encode_to_vec().into(),
        MetadataMap::new(),
    )
}

/// Build the typed `ActivityExecutionAlreadyStarted` gRPC status.
///
/// The targeted release returns this as code `AlreadyExists` carrying an
/// `ActivityExecutionAlreadyStartedFailure` detail (`StartRequestId`/`RunId`)
/// (`go.temporal.io/api serviceerror/activity_execution_already_started.go`). The
/// client recovers the typed error by reading the `google.rpc.Status` from the
/// `grpc-status-details-bin` trailer and matching code == AlreadyExists AND the
/// first detail's type (`serviceerror/convert.go`). tonic 0.11 has no Temporal
/// detail support, so we encode the `google.rpc.Status` wrapper by hand: the detail
/// is wrapped in a protobuf `Any` whose `type_url` is the fully-qualified message
/// name Go's `anypb.New` emits.
fn activity_already_started_status(
    message: String,
    run_id: String,
    start_request_id: String,
) -> Status {
    use prost::Message as _;
    use tokeira_proto::public::temporal::api::errordetails::v1::ActivityExecutionAlreadyStartedFailure;

    let failure = ActivityExecutionAlreadyStartedFailure {
        start_request_id,
        run_id,
    };
    let detail = ProtoAny {
        type_url:
            "type.googleapis.com/temporal.api.errordetails.v1.ActivityExecutionAlreadyStartedFailure"
                .to_owned(),
        value: failure.encode_to_vec(),
    };
    let rpc_status = RpcStatus {
        code: Code::AlreadyExists as i32,
        message: message.clone(),
        details: vec![detail],
    };
    Status::with_details_and_metadata(
        Code::AlreadyExists,
        message,
        rpc_status.encode_to_vec().into(),
        MetadataMap::new(),
    )
}

/// Build v1.31.0's stock-default rejection for the deprecated Worker
/// Versioning v0.1 (Version Set-based) data APIs. The gate
/// (`frontend.workerVersioningDataAPIs`) defaults to **false**
/// (`common/dynamicconfig/constants.go:1054-1058` @ v1.31.0), so an
/// out-of-the-box server refuses `UpdateWorkerBuildIdCompatibility` and
/// `GetWorkerBuildIdCompatibility` with this exact PermissionDenied — checked
/// first within the handler, before any request-field validation
/// (`service/frontend/workflow_handler.go:5330-5391`,
/// `service/frontend/errors.go:131`; upstream's namespace-validator
/// interceptor still runs ahead of the handler, a layer tokeira does not
/// model). tokeira has no dynamic config, so the gate is permanently off and
/// this rejection *is* the conformant behaviour.
pub(crate) fn worker_versioning_v1_disabled_status() -> Status {
    worker_versioning_disabled_status(
        "Worker versioning v0.1 (Version Set-based, deprecated) is disabled on this namespace.",
    )
}

/// Build v1.31.0's stock-default rejection for the deprecated Worker
/// Versioning v0.2 (Rules-based) APIs. The rules gate
/// (`frontend.workerVersioningRuleAPIs`) defaults to **false**
/// (`common/dynamicconfig/constants.go:1064-1068` @ v1.31.0), refusing
/// `UpdateWorkerVersioningRules` / `GetWorkerVersioningRules` with this exact
/// PermissionDenied before any request-field validation.
/// `GetWorkerTaskReachability` is gated by the **v0.1 data flag** but returns
/// this **v0.2 message** — a deliberate upstream cross-wiring
/// (`service/frontend/workflow_handler.go:5491-5493`); with both gates
/// permanently off in tokeira the observable result is this same status.
pub(crate) fn worker_versioning_v2_disabled_status() -> Status {
    worker_versioning_disabled_status(
        "Worker versioning v0.2 (Rules-based, deprecated) is disabled on this namespace.",
    )
}

/// PERMISSION_DENIED carrying a `PermissionDeniedFailure` detail with an empty
/// `reason`, matching `serviceerror.NewPermissionDenied(message, "")`
/// (`go.temporal.io/api serviceerror/permission_denied.go` @ v1.62.8) so SDK
/// clients reconstruct the typed `serviceerror.PermissionDenied` from the
/// `grpc-status-details-bin` trailer.
fn worker_versioning_disabled_status(message: &'static str) -> Status {
    use prost::Message as _;
    use tokeira_proto::public::temporal::api::errordetails::v1::PermissionDeniedFailure;

    let detail = ProtoAny {
        type_url: "type.googleapis.com/temporal.api.errordetails.v1.PermissionDeniedFailure"
            .to_owned(),
        value: PermissionDeniedFailure {
            reason: String::new(),
        }
        .encode_to_vec(),
    };
    let rpc_status = RpcStatus {
        code: Code::PermissionDenied as i32,
        message: message.to_owned(),
        details: vec![detail],
    };
    Status::with_details_and_metadata(
        Code::PermissionDenied,
        message.to_owned(),
        rpc_status.encode_to_vec().into(),
        MetadataMap::new(),
    )
}

/// Minimal `google.rpc.Status` mirror for the `grpc-status-details-bin` trailer.
/// Not generated (no temporal proto pulls `google/rpc/status.proto` into our
/// build), so it is hand-defined to the stable field numbers.
#[derive(Clone, PartialEq, ::prost::Message)]
struct RpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<ProtoAny>,
}

/// Minimal `google.protobuf.Any` mirror (hand-defined to avoid a `prost-types`
/// dependency for this single call site).
#[derive(Clone, PartialEq, ::prost::Message)]
struct ProtoAny {
    #[prost(string, tag = "1")]
    type_url: String,
    #[prost(bytes = "vec", tag = "2")]
    value: Vec<u8>,
}

fn err_static(message: &'static str) -> &'static str {
    message
}

fn insert_metadata_value(status: &mut Status, key: &'static str, value: &str) {
    if let Ok(value) = MetadataValue::try_from(value) {
        status.metadata_mut().insert(key, value);
    }
}

pub fn proto_conversion_status(err: ProtoConversionError) -> Status {
    Status::invalid_argument(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Code;

    #[test]
    fn edge_errors_map_to_expected_grpc_codes() {
        let cases = [
            (
                EdgeError::BadRequest("bad".to_string()),
                Code::InvalidArgument,
            ),
            (
                EdgeError::Unimplemented("todo".to_string()),
                Code::Unimplemented,
            ),
            (EdgeError::NotFound("missing".to_string()), Code::NotFound),
            (
                EdgeError::AlreadyExists("duplicate".to_string()),
                Code::AlreadyExists,
            ),
            (
                EdgeError::ResourceExhausted("full".to_string()),
                Code::ResourceExhausted,
            ),
            (
                EdgeError::Unauthorized("nope".to_string()),
                Code::Unauthenticated,
            ),
            (
                EdgeError::Forbidden {
                    action: "operator_read",
                    namespace: Some("default".to_string()),
                },
                Code::PermissionDenied,
            ),
            (
                EdgeError::NamespaceNotFound("missing".to_string()),
                Code::NotFound,
            ),
            (
                EdgeError::WorkflowNotFound {
                    namespace: "default".to_string(),
                    workflow_id: "wf".to_string(),
                },
                Code::NotFound,
            ),
            (
                EdgeError::ActivityNotFound {
                    namespace: "default".to_string(),
                    workflow_id: "wf".to_string(),
                    activity_id: "act".to_string(),
                },
                Code::NotFound,
            ),
            (
                EdgeError::ActivityNotStarted {
                    namespace: "default".to_string(),
                    workflow_id: "wf".to_string(),
                    activity_id: "act".to_string(),
                },
                Code::FailedPrecondition,
            ),
            (
                EdgeError::WorkflowAlreadyStarted {
                    namespace: "default".to_string(),
                    workflow_id: "wf".to_string(),
                    run_id: "run".to_string(),
                },
                Code::AlreadyExists,
            ),
            (
                EdgeError::NamespaceDeleted("default".to_string()),
                Code::FailedPrecondition,
            ),
            (
                EdgeError::NamespaceAlreadyExists("default".to_string()),
                Code::AlreadyExists,
            ),
            (EdgeError::TooManyLongPolls, Code::ResourceExhausted),
            (EdgeError::LongPollAdmissionTimeout, Code::DeadlineExceeded),
            (
                EdgeError::RemoteRouteUnsupported {
                    target: "other".to_string(),
                },
                Code::Unavailable,
            ),
            (
                EdgeError::NotShardOwner {
                    bundle_id: tokeira_types::ShardId(3),
                    current_epoch: tokeira_types::ShardEpoch(7),
                    current_owner_node_id: None,
                },
                Code::Aborted,
            ),
            (
                EdgeError::FailedPrecondition("workflow is already paused".to_string()),
                Code::FailedPrecondition,
            ),
            (EdgeError::Internal("boom".to_string()), Code::Internal),
        ];

        for (err, code) in cases {
            let status: Status = err.into();
            assert_eq!(status.code(), code);
        }
    }

    #[test]
    fn not_shard_owner_status_carries_routing_hints() {
        let status: Status = EdgeError::NotShardOwner {
            bundle_id: tokeira_types::ShardId(4),
            current_epoch: tokeira_types::ShardEpoch(11),
            current_owner_node_id: None,
        }
        .into();

        assert_eq!(status.code(), Code::Aborted);
        assert_eq!(status.metadata().get("tokeira-bundle-id").unwrap(), "4");
        assert_eq!(
            status.metadata().get("tokeira-current-epoch").unwrap(),
            "11"
        );
    }

    #[test]
    fn activity_already_started_status_carries_typed_detail() {
        use prost::Message as _;
        use tokeira_proto::public::temporal::api::errordetails::v1::ActivityExecutionAlreadyStartedFailure;

        // The SDK's serviceerror decode (convert.go) keys off code == AlreadyExists
        // AND the first google.rpc.Status detail being an
        // ActivityExecutionAlreadyStartedFailure. Assert both round-trip on the wire.
        let status: Status = EdgeError::ActivityExecutionAlreadyStarted {
            message: "activity execution already started".to_string(),
            run_id: "run-123".to_string(),
            start_request_id: "req-abc".to_string(),
        }
        .into();

        assert_eq!(status.code(), Code::AlreadyExists);
        assert_eq!(status.message(), "activity execution already started");

        let rpc_status = RpcStatus::decode(status.details()).expect("decode google.rpc.Status");
        assert_eq!(rpc_status.code, Code::AlreadyExists as i32);
        let detail = &rpc_status.details[0];
        assert_eq!(
            detail.type_url,
            "type.googleapis.com/temporal.api.errordetails.v1.ActivityExecutionAlreadyStartedFailure"
        );
        let failure = ActivityExecutionAlreadyStartedFailure::decode(detail.value.as_slice())
            .expect("decode failure detail");
        assert_eq!(failure.run_id, "run-123");
        assert_eq!(failure.start_request_id, "req-abc");
    }

    #[test]
    fn namespace_not_found_status_carries_typed_detail() {
        use prost::Message as _;
        use tokeira_proto::public::temporal::api::errordetails::v1::NamespaceNotFoundFailure;

        let status: Status = EdgeError::NamespaceNotFound("namespace-id".to_owned()).into();
        assert_eq!(status.code(), Code::NotFound);

        let rpc_status = RpcStatus::decode(status.details()).expect("decode google.rpc.Status");
        let detail = &rpc_status.details[0];
        assert_eq!(
            detail.type_url,
            "type.googleapis.com/temporal.api.errordetails.v1.NamespaceNotFoundFailure"
        );
        let failure = NamespaceNotFoundFailure::decode(detail.value.as_slice())
            .expect("decode namespace-not-found detail");
        assert_eq!(failure.namespace, "namespace-id");
    }

    #[test]
    fn workflow_already_started_status_carries_typed_detail() {
        use prost::Message as _;
        use tokeira_proto::public::temporal::api::errordetails::v1::WorkflowExecutionAlreadyStartedFailure;

        // A Nexus `WorkflowRunOperation` handler relies on the SDK reconstructing
        // `serviceerror.WorkflowExecutionAlreadyStarted` from this status (code ==
        // AlreadyExists AND the first google.rpc.Status detail being a
        // WorkflowExecutionAlreadyStartedFailure) so the caller observes a
        // `WorkflowExecutionAlreadyStarted` application error. A bare AlreadyExists would
        // lose the type. Assert both round-trip on the wire.
        let status: Status = EdgeError::WorkflowAlreadyStarted {
            namespace: "ns".to_string(),
            workflow_id: "wf-1".to_string(),
            run_id: "run-xyz".to_string(),
        }
        .into();

        assert_eq!(status.code(), Code::AlreadyExists);

        let rpc_status = RpcStatus::decode(status.details()).expect("decode google.rpc.Status");
        assert_eq!(rpc_status.code, Code::AlreadyExists as i32);
        let detail = &rpc_status.details[0];
        assert_eq!(
            detail.type_url,
            "type.googleapis.com/temporal.api.errordetails.v1.WorkflowExecutionAlreadyStartedFailure"
        );
        let failure = WorkflowExecutionAlreadyStartedFailure::decode(detail.value.as_slice())
            .expect("decode failure detail");
        assert_eq!(failure.run_id, "run-xyz");
    }
}
