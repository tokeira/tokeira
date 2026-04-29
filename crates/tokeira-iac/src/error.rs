//! Error types for the infrastructure lifecycle engine.

use thiserror::Error;
use tokeira_state::StateError;

#[derive(Debug, Error)]
pub enum IacError {
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
    #[error("{0}")]
    Other(#[from] anyhow::Error),
}
