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

    /// v1.31.0's `consts.ErrWorkflowClosing`: a signal (or update) arrived
    /// while the workflow has a close attempt bouncing off buffered events
    /// with a started WFT retrying it. RESOURCE_EXHAUSTED with cause
    /// BUSY_WORKFLOW, scope NAMESPACE, and this exact message — the corpus
    /// asserts cause and message verbatim (consts/const.go:62-67 @ v1.31.0).
    #[error("workflow operation can not be applied because workflow is closing")]
    WorkflowClosing,

    /// v1.31.0's `consts.ErrConsistentQueryBufferExceeded`: a query overflowed
    /// the run's consistent-query buffer (capacity 1). RESOURCE_EXHAUSTED with
    /// cause BUSY_WORKFLOW, scope NAMESPACE, and this exact message — the
    /// corpus asserts type and message (consts/const.go:72-77 +
    /// queryworkflow/api.go:191-194 @ v1.31.0).
    #[error(
        "consistent query buffer is full, this may be caused by too many queries and workflow \
         not able to process query fast enough"
    )]
    ConsistentQueryBufferExceeded,

    /// v1.31.0's `serviceerror.WorkflowNotReady`: a query arrived while the
    /// workflow cannot serve one (closed before any WFT started, first WFT
    /// still pending on backoff past the query deadline, or the WFT stuck
    /// failing). gRPC FAILED_PRECONDITION with a `WorkflowNotReadyFailure`
    /// detail; the guard messages come verbatim from
    /// queryworkflow/api.go:116-143 @ v1.31.0.
    #[error("{0}")]
    WorkflowNotReady(String),

    /// The worker answered a query with a failure: the typed `QueryFailed`
    /// serviceerror (gRPC INVALID_ARGUMENT + `QueryFailedFailure` detail
    /// carrying the worker's structured failure,
    /// serviceerror/query_failed.go @ go.temporal.io/api v1.62).
    #[error("{message}")]
    QueryFailed {
        message: String,
        failure: Option<tokeira_types::Payload>,
    },

    /// A query waited out its deadline with no worker answering — v1.31.0's
    /// frontend rewrite `DeadlineExceeded("query timed out before a worker
    /// could process it")` (workflow_handler.go:3178-3180).
    #[error("query timed out before a worker could process it")]
    QueryTimedOut,

    #[error("unauthorized: {0}")]
    Unauthorized(String),

    /// Temporal authorization denial. The transport always emits a typed
    /// `PermissionDeniedFailure`; `reason` is the authorizer-supplied detail.
    #[error("{message}")]
    PermissionDenied {
        message: String,
        reason: Option<String>,
    },

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

    /// A start (or signal-with-start) rejected by the workflow-id conflict or
    /// reuse policy against the incumbent run. Carries v1.31.0's exact
    /// policy-specific message (`workflow_id_dedup.go:95-129 @ v1.31.0` —
    /// the corpus asserts the suffixes verbatim) and maps to the typed
    /// `WorkflowExecutionAlreadyStarted` serviceerror like
    /// [`EdgeError::WorkflowAlreadyStarted`].
    #[error("{message}")]
    WorkflowStartRejected { message: String, run_id: String },

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
            EdgeError::WorkflowClosing => StatusCode::TOO_MANY_REQUESTS,
            EdgeError::ConsistentQueryBufferExceeded => StatusCode::TOO_MANY_REQUESTS,
            EdgeError::WorkflowNotReady(_) => StatusCode::PRECONDITION_FAILED,
            EdgeError::QueryFailed { .. } => StatusCode::BAD_REQUEST,
            EdgeError::QueryTimedOut => StatusCode::REQUEST_TIMEOUT,
            EdgeError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            EdgeError::PermissionDenied { .. } => StatusCode::FORBIDDEN,
            EdgeError::Forbidden { .. } => StatusCode::FORBIDDEN,
            EdgeError::NamespaceNotFound(_)
            | EdgeError::WorkflowNotFound { .. }
            | EdgeError::ActivityNotFound { .. }
            | EdgeError::BatchOperationNotFound { .. } => StatusCode::NOT_FOUND,
            EdgeError::ActivityNotStarted { .. } => StatusCode::PRECONDITION_FAILED,
            EdgeError::WorkflowAlreadyStarted { .. }
            | EdgeError::WorkflowStartRejected { .. }
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
            EdgeError::WorkflowClosing => "workflow_closing",
            EdgeError::ConsistentQueryBufferExceeded => "consistent_query_buffer_exceeded",
            EdgeError::WorkflowNotReady(_) => "workflow_not_ready",
            EdgeError::QueryFailed { .. } => "query_failed",
            EdgeError::QueryTimedOut => "query_timed_out",
            EdgeError::Unauthorized(_) => "unauthorized",
            EdgeError::PermissionDenied { .. } => "permission_denied",
            EdgeError::Forbidden { .. } => "forbidden",
            EdgeError::NamespaceNotFound(_) => "namespace_not_found",
            EdgeError::NamespaceDeleted(_) => "namespace_deleted",
            EdgeError::NamespaceAlreadyExists(_) => "namespace_already_exists",
            EdgeError::WorkflowNotFound { .. } => "workflow_not_found",
            EdgeError::ActivityNotFound { .. } => "activity_not_found",
            EdgeError::ActivityNotStarted { .. } => "activity_not_started",
            EdgeError::WorkflowAlreadyStarted { .. } => "workflow_already_started",
            EdgeError::WorkflowStartRejected { .. } => "workflow_already_started",
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
        if let Some(tokeira_runtime::KernelRejected(tokeira_kernel::Reject::UnknownActivity(
            activity,
        ))) = value.downcast_ref::<tokeira_runtime::KernelRejected>()
        {
            return Self::NotFound(format!("activity not found for ID: {activity}"));
        }
        if let Some(tokeira_runtime::KernelRejected(
            tokeira_kernel::Reject::ResetConstraintViolation { reason },
        )) = value.downcast_ref::<tokeira_runtime::KernelRejected>()
        {
            return Self::BadRequest(reason.clone());
        }
        // A rejected activity-options patch (an unmergeable retry-policy subfield,
        // or — under a rare race — a type/all match set that grew a member with no
        // resolved original options) is a client-argument error, matching
        // v1.31.0's `INVALID_ARGUMENT` for these cases, not an internal fault.
        if let Some(tokeira_runtime::KernelRejected(
            tokeira_kernel::Reject::InvalidActivityOptions(reason),
        )) = value.downcast_ref::<tokeira_runtime::KernelRejected>()
        {
            return Self::BadRequest(reason.clone());
        }
        // Implied pinned overrides are structurally valid; availability is a
        // state-dependent precondition resolved inside the serialized kernel
        // transition (`updateworkflowoptions/api.go @ v1.31.0`). Preserve that
        // distinction instead of collapsing the typed reject into INTERNAL.
        if let Some(tokeira_runtime::KernelRejected(
            tokeira_kernel::Reject::InvalidVersioningOverride(reason),
        )) = value.downcast_ref::<tokeira_runtime::KernelRejected>()
        {
            return Self::FailedPrecondition(reason.clone());
        }
        // An invalid workflow command fails the WFT server-side and errors the
        // completion with INVALID_ARGUMENT carrying the cause message — the
        // corpus asserts err.Error() == "UnhandledCommand" exactly
        // (respondworkflowtaskcompleted/api.go:739-742 @ v1.31.0).
        if let Some(invalid) = value.downcast_ref::<tokeira_runtime::InvalidWorkflowCommand>() {
            return Self::BadRequest(invalid.message.clone());
        }
        // Query pre-dispatch guards surface v1.31.0's WorkflowNotReady
        // (queryworkflow/api.go:116-143).
        if let Some(not_ready) = value.downcast_ref::<tokeira_runtime::WorkflowNotReady>() {
            return Self::WorkflowNotReady(not_ready.message.to_string());
        }
        // Visibility syntax/type/unknown-attribute failures are client query
        // errors, not projection failures (`List/CountActivityExecutions @ v1.31.0`).
        if let Some(invalid) = value.downcast_ref::<tokeira_projection::InvalidVisibilityQuery>() {
            return Self::BadRequest(invalid.to_string());
        }
        // A query that outlived its deadline surfaces v1.31.0's frontend
        // rewrite (workflow_handler.go:3178-3180).
        if value
            .downcast_ref::<tokeira_runtime::QueryTimedOut>()
            .is_some()
        {
            return Self::QueryTimedOut;
        }
        // A query overflowing the run's consistent-query buffer surfaces
        // v1.31.0's `ErrConsistentQueryBufferExceeded` (queryworkflow/api.go:
        // 191-194 + consts/const.go:72-77).
        if value
            .downcast_ref::<tokeira_runtime::ConsistentQueryBufferExceeded>()
            .is_some()
        {
            return Self::ConsistentQueryBufferExceeded;
        }
        // The close-attempted signal gate surfaces v1.31.0's
        // `ErrWorkflowClosing` (signal_workflow_util.go:63-70).
        if value
            .downcast_ref::<tokeira_runtime::WorkflowClosing>()
            .is_some()
        {
            return Self::WorkflowClosing;
        }
        // A pre-accepted update aborted by its run closing with no successor
        // surfaces v1.31.0's `update.AbortedByWorkflowClosingErr` — NotFound
        // with this exact message (errors_failures.go:10-35 @ v1.31.0).
        if value
            .downcast_ref::<tokeira_runtime::UpdateAbortedByClosingWorkflow>()
            .is_some()
        {
            return Self::NotFound("workflow update was aborted by closing workflow".to_string());
        }
        // A completion referencing an unknown update with no resurrect
        // payload already failed the WFT server-side; the call surfaces
        // NotFound "update {id} wasn't found on the server…"
        // (workflow_task_completed_handler.go:381 @ v1.31.0; spec
        // speculative-wft Req 6.2 / K5). Wrong-state siblings ride the
        // InvalidWorkflowCommand → BadRequest arm above.
        if let Some(not_found) = value.downcast_ref::<tokeira_runtime::UpdateMessageNotFound>() {
            return Self::NotFound(not_found.message.clone());
        }
        // Any mutation against an already-closed run surfaces v1.31.0's
        // `ErrWorkflowCompleted`: NOT_FOUND with this exact message
        // (`consts/const.go:51 @ v1.31.0`) — asserted by type on
        // signal-after-terminate (tests/signal_workflow_test.go:231).
        if value
            .downcast_ref::<tokeira_runtime::KernelRejected>()
            .is_some_and(|rejected| matches!(rejected.0, tokeira_kernel::Reject::RunClosed(_)))
        {
            return Self::NotFound("workflow execution already completed".to_string());
        }
        // A workflow-task mutation whose token no longer matches the run's
        // current task — the task was completed, timed out, or superseded — is
        // v1.31.0's `ErrWorkflowTaskNotFound`: NOT_FOUND "Workflow task not
        // found." (consts/const.go @ v1.31.0). Asserted verbatim by the
        // speculative start-to-close timeout leaf, whose late completion races
        // the in-memory timeout (`TestSpeculativeWorkflowTask_StartToCloseTimeout`).
        if value
            .downcast_ref::<tokeira_runtime::KernelRejected>()
            .is_some_and(|rejected| {
                matches!(
                    rejected.0,
                    tokeira_kernel::Reject::WorkflowTaskSeqMismatch { .. }
                        | tokeira_kernel::Reject::WorkflowTaskTokenMismatch
                        | tokeira_kernel::Reject::WorkflowTaskNotStarted { .. }
                )
            })
        {
            return Self::NotFound("Workflow task not found.".to_string());
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
