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
            EdgeError::NamespaceNotFound(namespace) => Status::not_found(namespace),
            EdgeError::NamespaceDeleted(namespace) => Status::failed_precondition(namespace),
            EdgeError::NamespaceAlreadyExists(namespace) => Status::already_exists(namespace),
            EdgeError::WorkflowNotFound {
                namespace,
                workflow_id,
            } => Status::not_found(format!("{namespace}/{workflow_id}")),
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
            } => Status::already_exists(format!(
                "{namespace}/{workflow_id} already started as {run_id}"
            )),
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
}
