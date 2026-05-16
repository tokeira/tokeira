//! Platform abstraction for applying service manifests.
//!
//! The engine uses this trait to apply manifests without knowing the
//! concrete platform. Implementations (e.g. a Kubernetes platform) are
//! registered on [`ServiceContext`] by the CLI at runtime.

use crate::RuntimeError;

/// A platform that can apply deployment manifests.
///
/// Implement this trait for the concrete runtime target. The engine gives the
/// platform only the manifests for services that changed, so
/// `apply_manifests` should be idempotent for each manifest and return the
/// number of manifests it accepted. Platform implementations own the mechanics:
/// Docker API calls, `docker compose` reconciliation, Kubernetes apply, ECS
/// service updates, and any provider-specific validation.
#[async_trait::async_trait]
pub trait Platform: Send + Sync {
    /// Apply a set of manifests and return the number applied.
    async fn apply_manifests(&self, manifests: &[serde_json::Value])
    -> Result<usize, RuntimeError>;

    /// Check whether a service's running state matches the desired manifests.
    ///
    /// Returns `false` if the running container's image has been rebuilt behind
    /// the same tag, or if the container is missing. The engine uses this to
    /// force recreation even when the desired manifest hash hasn't changed.
    ///
    /// Default implementation returns `true` (assume current) for platforms
    /// that don't support live drift detection.
    async fn is_service_current(&self, _service_name: &str, _manifests: &[serde_json::Value]) -> bool {
        true
    }
}
