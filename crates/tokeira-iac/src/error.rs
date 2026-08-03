//! Error types for the infrastructure lifecycle engine.

use thiserror::Error;
use tokeira_state::StateError;

use crate::PlatformIssue;

#[derive(Debug, Error)]
pub enum IacError {
    /// A provider SDK seam classified a reachability failure while describing
    /// live state. Plan paths transport this as a no-change outcome; mutation
    /// paths keep it as a hard, actionable error.
    #[error("Platform issue: {0}")]
    PlatformIssue(PlatformIssue),
    /// Provider-owned runtime preparation failed before resource execution.
    #[error("Provider error: {0}")]
    Provider(String),
    #[error("Provider SDK error: {0}")]
    AwsSdk(String),
    #[error("State not found: {0}")]
    StateNotFound(String),
    #[error(transparent)]
    State(#[from] StateError),
    #[error("Dependency resolution failed: {0}")]
    DependencyResolution(String),
    #[error("Resource not found: {resource_type} {resource_id}")]
    ResourceNotFound {
        resource_type: String,
        resource_id: String,
    },
    #[error("Resource creation failed: {resource_type} {resource_id}: {details}")]
    ResourceCreationFailed {
        resource_type: String,
        resource_id: String,
        details: String,
    },
    #[error("Resource wait timed out: {resource_type} {resource_id}: {details}")]
    ResourceWaitTimedOut {
        resource_type: String,
        resource_id: String,
        details: String,
    },
    /// A Delete targets a resource that is recorded in state but absent from the
    /// `known` managed set, so the engine has no `Resource` to delete the live
    /// object with. Failing closed here prevents dropping the state entry while
    /// leaving the live resource orphaned. (Property 10.)
    #[error(
        "fail-closed delete: resource '{resource_id}' is in state but not in the known managed set — \
         refusing to remove it from state without deleting the live resource"
    )]
    UnknownResourceDelete { resource_id: String },
    /// The composition failed pre-flight validation (duplicate module/resource id,
    /// a desired module absent from `known`, or a dependency cycle). Refusing
    /// before any Delta keeps the resource map unambiguous. (Property 12.)
    #[error("invalid composition: {0}")]
    CompositionInvalid(String),
    #[error("{0}")]
    Other(#[from] anyhow::Error),
}
