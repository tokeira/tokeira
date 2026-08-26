//! Platform abstraction for applying service manifests.
//!
//! The engine uses this trait to apply manifests without knowing the
//! concrete platform. Implementations (e.g. a Kubernetes platform) are
//! registered on [`ServiceContext`] by the host application at runtime.

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
    /// Resolve any platform-owned prerequisites for one service manifest.
    ///
    /// The engine invokes this immediately before classifying the service in
    /// a platform-aware plan and immediately before applying that same
    /// service. Implementations may populate a local image cache according to
    /// manifest policy, but must not mutate the running workload. Returning an
    /// error aborts at this service, so earlier services may already have been
    /// applied and persisted while later services remain untouched.
    async fn prepare_service(
        &self,
        _service_name: &str,
        _manifests: &[serde_json::Value],
    ) -> Result<(), RuntimeError> {
        Ok(())
    }

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
    async fn is_service_current(
        &self,
        _service_name: &str,
        _manifests: &[serde_json::Value],
    ) -> bool {
        true
    }

    /// Whether this platform can tear down a running service workload.
    ///
    /// The engine checks this before a delete-only pass so it can refuse the
    /// whole operation up front rather than fail partway. Default `false`.
    fn supports_delete(&self) -> bool {
        false
    }

    /// Tear down a service's running workload — the inverse of an apply. Used by
    /// definition-driven rollback: the superseded binary B deletes
    /// the services it created before the binding re-pins to A. `manifests` are
    /// the service's last-applied manifests, so the platform knows what to remove.
    ///
    /// Must be idempotent: an already-absent workload is success. The default
    /// **fail-closes** — a platform that cannot delete refuses rollback rather
    /// than leaving a workload running.
    async fn delete_service(
        &self,
        service_name: &str,
        _manifests: &[serde_json::Value],
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::Platform(format!(
            "fail-closed: platform does not support service deletion; refusing to tear down '{service_name}'"
        )))
    }
}
