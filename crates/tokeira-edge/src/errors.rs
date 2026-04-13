use http::StatusCode;
use thiserror::Error;

/// Common error surface for edge handlers.
///
/// The edge should classify errors into request/protocol failures, admission/routing
/// failures, and internal failures. Doing this at the boundary gives callers a
/// stable contract even if the internal implementation changes.
#[derive(Debug, Error)]
pub enum EdgeError {
    #[error("bad request: {0}")]
    BadRequest(String),

    #[error("unauthorized: {0}")]
    Unauthorized(String),

    #[error("forbidden action `{action}` for namespace {namespace:?}")]
    Forbidden {
        action: &'static str,
        namespace: Option<String>,
    },

    #[error("namespace not found: {0}")]
    NamespaceNotFound(String),

    #[error("namespace is deleted: {0}")]
    NamespaceDeleted(String),

    #[error("namespace already exists: {0}")]
    NamespaceAlreadyExists(String),

    #[error("workflow not found: {namespace}/{workflow_id}")]
    WorkflowNotFound {
        namespace: String,
        workflow_id: String,
    },

    #[error("workflow already started: {namespace}/{workflow_id} ({run_id})")]
    WorkflowAlreadyStarted {
        namespace: String,
        workflow_id: String,
        run_id: String,
    },

    #[error("too many concurrent long polls")]
    TooManyLongPolls,

    #[error("long poll timed out while waiting for admission")]
    LongPollAdmissionTimeout,

    #[error("request routed to remote target `{target}` but forwarding is not wired yet")]
    RemoteRouteUnsupported { target: String },

    #[error("internal error: {0}")]
    Internal(String),
}

pub type EdgeResult<T> = Result<T, EdgeError>;

impl EdgeError {
    /// Map edge errors to HTTP status codes.
    ///
    /// These status codes are also useful for gRPC translation later because they
    /// make the high-level class of failure explicit.
    pub fn status_code(&self) -> StatusCode {
        match self {
            EdgeError::BadRequest(_) => StatusCode::BAD_REQUEST,
            EdgeError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            EdgeError::Forbidden { .. } => StatusCode::FORBIDDEN,
            EdgeError::NamespaceNotFound(_) | EdgeError::WorkflowNotFound { .. } => {
                StatusCode::NOT_FOUND
            }
            EdgeError::WorkflowAlreadyStarted { .. } => StatusCode::CONFLICT,
            EdgeError::NamespaceDeleted(_) => StatusCode::GONE,
            EdgeError::NamespaceAlreadyExists(_) => StatusCode::CONFLICT,
            EdgeError::TooManyLongPolls => StatusCode::TOO_MANY_REQUESTS,
            EdgeError::LongPollAdmissionTimeout => StatusCode::REQUEST_TIMEOUT,
            EdgeError::RemoteRouteUnsupported { .. } => StatusCode::BAD_GATEWAY,
            EdgeError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn action_name(&self) -> &'static str {
        match self {
            EdgeError::BadRequest(_) => "bad_request",
            EdgeError::Unauthorized(_) => "unauthorized",
            EdgeError::Forbidden { .. } => "forbidden",
            EdgeError::NamespaceNotFound(_) => "namespace_not_found",
            EdgeError::NamespaceDeleted(_) => "namespace_deleted",
            EdgeError::NamespaceAlreadyExists(_) => "namespace_already_exists",
            EdgeError::WorkflowNotFound { .. } => "workflow_not_found",
            EdgeError::WorkflowAlreadyStarted { .. } => "workflow_already_started",
            EdgeError::TooManyLongPolls => "too_many_long_polls",
            EdgeError::LongPollAdmissionTimeout => "long_poll_admission_timeout",
            EdgeError::RemoteRouteUnsupported { .. } => "remote_route_unsupported",
            EdgeError::Internal(_) => "internal",
        }
    }
}

impl From<anyhow::Error> for EdgeError {
    fn from(value: anyhow::Error) -> Self {
        Self::Internal(value.to_string())
    }
}
