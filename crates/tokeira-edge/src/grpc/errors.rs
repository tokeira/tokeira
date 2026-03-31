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
            EdgeError::NamespaceDeleted(namespace) => Status::failed_precondition(namespace),
            EdgeError::WorkflowNotFound {
                namespace,
                workflow_id,
            } => Status::not_found(format!("{namespace}/{workflow_id}")),
            EdgeError::TooManyLongPolls => Status::resource_exhausted(err_static("too many concurrent long polls")),
            EdgeError::LongPollAdmissionTimeout => {
                Status::deadline_exceeded(err_static("long poll timed out while waiting for admission"))
            }
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
