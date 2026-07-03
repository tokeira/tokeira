//! Typed error surface for the edge crate.
//!
//! Every variant of [`EdgeError`] maps to a specific HTTP/gRPC status code so
//! that the gRPC translation layer can produce correct status responses without
//! inspecting error messages. Keeping the classification here (rather than in
//! the gRPC handlers) means the business-logic layer in `workflow_service`
//! can return rich, testable errors while the transport layer stays thin.

use http::StatusCode;
use thiserror::Error;
use tokeira_types::{BundleId, IncarnationId, ShardEpoch};

/// Common error surface for edge handlers.
///
/// The edge should classify errors into request/protocol failures, admission/routing
/// failures, and internal failures. Doing this at the boundary gives callers a
/// stable contract even if the internal implementation changes.
#[derive(Debug, Error)]
pub enum EdgeError {
    #[error("bad request: {0}")]
    BadRequest(String),

    #[error("unimplemented: {0}")]
    Unimplemented(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("already exists: {0}")]
    AlreadyExists(String),

    #[error("resource exhausted: {0}")]
    ResourceExhausted(String),

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

    #[error("activity not found: {namespace}/{workflow_id}/{activity_id}")]
    ActivityNotFound {
        namespace: String,
        workflow_id: String,
        activity_id: String,
    },

    #[error("activity has not started: {namespace}/{workflow_id}/{activity_id}")]
    ActivityNotStarted {
        namespace: String,
        workflow_id: String,
        activity_id: String,
    },

    #[error("workflow already started: {namespace}/{workflow_id} ({run_id})")]
    WorkflowAlreadyStarted {
        namespace: String,
        workflow_id: String,
        run_id: String,
    },

    /// A standalone-activity Start was rejected by the id reuse/conflict policy
    /// against an existing run. Carries the current run's id and create request id
    /// so the gRPC layer can emit the typed `ActivityExecutionAlreadyStarted`
    /// serviceerror (code `AlreadyExists`, with an
    /// `ActivityExecutionAlreadyStartedFailure` detail) the SDK decodes via
    /// `ErrorAs` — `serviceerror/activity_execution_already_started.go` +
    /// `serviceerror/convert.go` (go.temporal.io/api @ v1.62.x).
    #[error("{message}")]
    ActivityExecutionAlreadyStarted {
        message: String,
        run_id: String,
        start_request_id: String,
    },

    #[error("batch operation already exists: {namespace}/{job_id}")]
    BatchOperationAlreadyExists { namespace: String, job_id: String },

    #[error("batch operation not found: {namespace}/{job_id}")]
    BatchOperationNotFound { namespace: String, job_id: String },

    #[error("too many concurrent long polls")]
    TooManyLongPolls,

    #[error("long poll timed out while waiting for admission")]
    LongPollAdmissionTimeout,

    #[error("request routed to remote target `{target}` but forwarding is not wired yet")]
    RemoteRouteUnsupported { target: String },

    #[error(
        "not shard owner for bundle {bundle_id:?}: current_epoch={current_epoch:?}, current_owner={current_owner_node_id:?}"
    )]
    NotShardOwner {
        bundle_id: BundleId,
        current_epoch: ShardEpoch,
        current_owner_node_id: Option<IncarnationId>,
    },

    #[error("failed precondition: {0}")]
    FailedPrecondition(String),

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
            EdgeError::Unimplemented(_) => StatusCode::NOT_IMPLEMENTED,
            EdgeError::NotFound(_) => StatusCode::NOT_FOUND,
            EdgeError::AlreadyExists(_) => StatusCode::CONFLICT,
            EdgeError::ResourceExhausted(_) => StatusCode::TOO_MANY_REQUESTS,
            EdgeError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            EdgeError::Forbidden { .. } => StatusCode::FORBIDDEN,
            EdgeError::NamespaceNotFound(_)
            | EdgeError::WorkflowNotFound { .. }
            | EdgeError::ActivityNotFound { .. }
            | EdgeError::BatchOperationNotFound { .. } => StatusCode::NOT_FOUND,
            EdgeError::ActivityNotStarted { .. } => StatusCode::PRECONDITION_FAILED,
            EdgeError::WorkflowAlreadyStarted { .. }
            | EdgeError::BatchOperationAlreadyExists { .. } => StatusCode::CONFLICT,
            EdgeError::ActivityExecutionAlreadyStarted { .. } => StatusCode::CONFLICT,
            EdgeError::NamespaceDeleted(_) => StatusCode::GONE,
            EdgeError::NamespaceAlreadyExists(_) => StatusCode::CONFLICT,
            EdgeError::TooManyLongPolls => StatusCode::TOO_MANY_REQUESTS,
            EdgeError::LongPollAdmissionTimeout => StatusCode::REQUEST_TIMEOUT,
            EdgeError::RemoteRouteUnsupported { .. } => StatusCode::BAD_GATEWAY,
            EdgeError::NotShardOwner { .. } => StatusCode::CONFLICT,
            EdgeError::FailedPrecondition(_) => StatusCode::PRECONDITION_FAILED,
            EdgeError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn action_name(&self) -> &'static str {
        match self {
            EdgeError::BadRequest(_) => "bad_request",
            EdgeError::Unimplemented(_) => "unimplemented",
            EdgeError::NotFound(_) => "not_found",
            EdgeError::AlreadyExists(_) => "already_exists",
            EdgeError::ResourceExhausted(_) => "resource_exhausted",
            EdgeError::Unauthorized(_) => "unauthorized",
            EdgeError::Forbidden { .. } => "forbidden",
            EdgeError::NamespaceNotFound(_) => "namespace_not_found",
            EdgeError::NamespaceDeleted(_) => "namespace_deleted",
            EdgeError::NamespaceAlreadyExists(_) => "namespace_already_exists",
            EdgeError::WorkflowNotFound { .. } => "workflow_not_found",
            EdgeError::ActivityNotFound { .. } => "activity_not_found",
            EdgeError::ActivityNotStarted { .. } => "activity_not_started",
            EdgeError::WorkflowAlreadyStarted { .. } => "workflow_already_started",
            EdgeError::ActivityExecutionAlreadyStarted { .. } => {
                "activity_execution_already_started"
            }
            EdgeError::BatchOperationAlreadyExists { .. } => "batch_operation_already_exists",
            EdgeError::BatchOperationNotFound { .. } => "batch_operation_not_found",
            EdgeError::TooManyLongPolls => "too_many_long_polls",
            EdgeError::LongPollAdmissionTimeout => "long_poll_admission_timeout",
            EdgeError::RemoteRouteUnsupported { .. } => "remote_route_unsupported",
            EdgeError::NotShardOwner { .. } => "not_shard_owner",
            EdgeError::FailedPrecondition(_) => "failed_precondition",
            EdgeError::Internal(_) => "internal",
        }
    }
}

impl From<anyhow::Error> for EdgeError {
    fn from(value: anyhow::Error) -> Self {
        // A failed activity-token revalidation surfaces as v1.31.0's
        // `ErrActivityTaskNotFound`: code NotFound with this exact message,
        // asserted verbatim by clients (`service/history/consts/const.go:44-45
        // @ v1.31.0`). The typed error's own reason is diagnostic only.
        if value
            .downcast_ref::<tokeira_runtime::ActivityTaskNotFound>()
            .is_some()
        {
            return Self::NotFound(
                "invalid activityID or activity already timed out or invoking workflow is completed"
                    .to_string(),
            );
        }
        match value.downcast::<tokeira_runtime::NotShardOwner>() {
            Ok(not_owner) => Self::NotShardOwner {
                bundle_id: not_owner.bundle_id,
                current_epoch: not_owner.current_epoch,
                current_owner_node_id: not_owner.current_owner_node_id,
            },
            Err(value) => {
                // Kernel rejections surface as stringified anyhow errors
                // (`kernel rejected command: <Display of Reject>`) because the
                // lane boundary does not preserve the typed `Reject`. Pause and
                // unpause precondition failures must map to FAILED_PRECONDITION
                // rather than the default INTERNAL classification.
                let message = value.to_string();
                if message.contains("workflow is already paused")
                    || message.contains("workflow is not paused")
                {
                    Self::FailedPrecondition(message)
                } else {
                    Self::Internal(message)
                }
            }
        }
    }
}
