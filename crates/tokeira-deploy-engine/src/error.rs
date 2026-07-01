//! Runtime lifecycle error types.

use thiserror::Error;
use tokeira_state::StateError;

/// Errors raised during image or service lifecycle operations.
#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("Image error: {0}")]
    Image(String),
    #[error("Service error: {0}")]
    Service(String),
    #[error("Platform error: {0}")]
    Platform(String),
    #[error("State error: {0}")]
    State(#[from] StateError),
    #[error("Kubernetes error: {0}")]
    Kubernetes(String),
    #[error("{0}")]
    Other(#[from] anyhow::Error),
}
