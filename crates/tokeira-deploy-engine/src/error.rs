//! Runtime lifecycle error types.

use thiserror::Error;
use tokeira_state::StateError;

/// The platform-owned class of a failure resolving one service image.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceImageIssueKind {
    /// Docker or another runtime refused or interrupted an image pull.
    Pull,
    /// The runtime could not inspect its local image cache.
    Inspect,
    /// Manifest policy forbids pulling an image that is absent locally.
    Unavailable,
}

/// A service-image failure transported as reportable data rather than a
/// nested string error.
///
/// The platform owns the evidence and any grounded direction. The deploy
/// shell owns presentation, so the same value renders once in Markdown or
/// JSON without reformatting the registry error at every layer.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, Error)]
#[error("service '{service}' image '{image}' could not be resolved: {evidence}")]
pub struct ServiceImageIssue {
    /// Service whose manifest selected the image.
    pub service: String,
    /// Exact image reference from the service manifest.
    pub image: String,
    /// Operation that failed while resolving the image.
    pub kind: ServiceImageIssueKind,
    /// Runtime or registry evidence, preserved verbatim.
    pub evidence: String,
    /// Platform-owned next step when the evidence establishes one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub direction: Option<String>,
}

/// Errors raised during image or service lifecycle operations.
#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("Image error: {0}")]
    Image(String),
    #[error("Service error: {0}")]
    Service(String),
    #[error(transparent)]
    ServiceImage(#[from] ServiceImageIssue),
    #[error("Platform error: {0}")]
    Platform(String),
    #[error("State error: {0}")]
    State(#[from] StateError),
    #[error("Kubernetes error: {0}")]
    Kubernetes(String),
    #[error("{0}")]
    Other(#[from] anyhow::Error),
}

impl RuntimeError {
    /// Return structured service-image evidence when this error carries it.
    pub fn service_image_issue(&self) -> Option<&ServiceImageIssue> {
        match self {
            Self::ServiceImage(issue) => Some(issue),
            _ => None,
        }
    }
}
