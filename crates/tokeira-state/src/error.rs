use thiserror::Error;

/// Errors raised by state persistence operations.
#[derive(Debug, Error)]
pub enum StateError {
    #[error("State conflict: {0}")]
    Conflict(String),
    #[error("State is locked: {0}")]
    Locked(String),
    #[error("State lock lost: {0}")]
    LockLost(String),
    #[error("State corrupted: {0}")]
    Corrupted(String),
    #[error("State not found: {0}")]
    NotFound(String),
    #[error("S3 error: {0}")]
    S3(String),
    #[error("Backend error: {0}")]
    Backend(String),
    #[error("{0}")]
    Other(#[from] anyhow::Error),
}
