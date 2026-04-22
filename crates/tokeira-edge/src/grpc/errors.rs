use tokeira_proto::conversions::ProtoConversionError;
use tonic::Status;

use crate::errors::EdgeError;

impl From<EdgeError> for Status {
    fn from(err: EdgeError) -> Self {
        match err {
            EdgeError::BadRequest(message) => Status::invalid_argument(message),
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
            EdgeError::NamespaceDeleted(namespace) => {
                Status::failed_precondition(namespace)
            }
            EdgeError::NamespaceAlreadyExists(namespace) => {
                Status::already_exists(namespace)
            }
            EdgeError::WorkflowNotFound {
                namespace,
                workflow_id,
            } => Status::not_found(format!("{namespace}/{workflow_id}")),
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
            EdgeError::Internal(message) => Status::internal(message),
        }
    }
}

fn err_static(message: &'static str) -> &'static str {
    message
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
            (EdgeError::Internal("boom".to_string()), Code::Internal),
        ];

        for (err, code) in cases {
            let status: Status = err.into();
            assert_eq!(status.code(), code);
        }
    }
}
