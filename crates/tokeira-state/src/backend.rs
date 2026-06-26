use async_trait::async_trait;

use crate::StateError;

/// Object-safe storage backend for [`crate::StateStore`].
///
/// Implement this trait to persist state through a custom medium. The backend
/// owns all storage-specific
/// details: paths, object keys, version tags, encryption, and retry behavior.
/// The facade only relies on the manifest version returned by
/// [`read_manifest`](Self::read_manifest) and passed back to
/// [`write_manifest`](Self::write_manifest).
///
/// All operations return the unified [`StateError`] so backends can be used as
/// `Box<dyn StateBackend>`. Preserve backend-specific detail inside
/// `StateError::Backend` or `StateError::Other` when no narrower variant fits.
#[async_trait]
pub trait StateBackend: Send + Sync {
    /// Read the current serialized state document and backend version tag.
    ///
    /// Return `Ok(None)` when no state has been committed yet. The version tag
    /// is opaque to callers; the same value must be accepted by
    /// [`write_manifest`](Self::write_manifest) as the expected version.
    async fn read_manifest(&self, key: &str) -> Result<Option<(Vec<u8>, String)>, StateError>;

    /// Commit a new serialized state document if the expected version matches.
    ///
    /// `expected_version` is empty for first write. Implementations must return
    /// [`StateError::Conflict`] when an existing document has changed since it
    /// was read.
    async fn write_manifest(
        &self,
        key: &str,
        data: &[u8],
        expected_version: &str,
    ) -> Result<(), StateError>;

    /// Read an immutable snapshot payload.
    ///
    /// The generic facade does not currently use snapshots, but this method is
    /// part of the backend contract so adapters can expose the same storage
    /// primitives as the S3 source store.
    async fn read_snapshot(&self, key: &str) -> Result<Vec<u8>, StateError>;

    /// Write an immutable snapshot payload.
    ///
    /// Backends should treat duplicate snapshot writes as idempotent when the
    /// existing payload is the same logical object.
    async fn write_snapshot(&self, key: &str, data: &[u8]) -> Result<(), StateError>;

    /// List snapshot keys under the supplied prefix.
    async fn list_snapshots(&self, prefix: &str) -> Result<Vec<String>, StateError>;
}
