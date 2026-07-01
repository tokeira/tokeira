//! Optional binary retention (task 6, Property 3).
//!
//! A provisioner binary blob is persisted keyed by `version`+`target` alongside
//! the deployment's state documents, and retrieved + **checksum-verified** against
//! the integrity manifest before execution — a binary whose `sha256` does not
//! match its manifest descriptor is never handed back for execution (the caller
//! aborts). Built over any [`StateBackend`]'s immutable snapshot I/O, so one store
//! serves both the cloud (`S3Backend`) and local dev (`LocalBackend`).

use tokeira_state::{StateBackend, StateError};

use crate::{IntegrityError, IntegrityManifest, Target};

/// Failure retrieving-and-verifying a binary.
#[derive(Debug, thiserror::Error)]
pub enum BinaryError {
    #[error(transparent)]
    Store(#[from] StateError),
    #[error(transparent)]
    Integrity(#[from] IntegrityError),
}

/// An immutable store for provisioner binary blobs, over a [`StateBackend`].
pub struct BinaryStore {
    backend: Box<dyn StateBackend>,
    prefix: String,
}

impl BinaryStore {
    /// Construct a store rooted at `prefix` (e.g. `"binaries"`), alongside the
    /// deployment's state documents.
    pub fn new(backend: Box<dyn StateBackend>, prefix: impl Into<String>) -> Self {
        Self {
            backend,
            prefix: prefix.into(),
        }
    }

    fn key(&self, version: &str, target: &Target) -> String {
        format!("{}/{}-{}", self.prefix, version, target.0)
    }

    /// Persist a binary blob keyed by `version`+`target`. Immutable and idempotent
    /// (re-persisting the same key is a no-op). Returns the retrieval key, suitable
    /// for a [`BinaryArtifactDescriptor::retrieval_ref`](crate::BinaryArtifactDescriptor::retrieval_ref).
    pub async fn persist(
        &self,
        version: &str,
        target: &Target,
        bytes: &[u8],
    ) -> Result<String, StateError> {
        let key = self.key(version, target);
        self.backend.write_snapshot(&key, bytes).await?;
        Ok(key)
    }

    /// Retrieve a binary blob (unverified — prefer [`retrieve_verified`](Self::retrieve_verified)).
    pub async fn retrieve(&self, version: &str, target: &Target) -> Result<Vec<u8>, StateError> {
        let key = self.key(version, target);
        self.backend.read_snapshot(&key).await
    }

    /// Retrieve a binary blob **and checksum-verify** it against `manifest` before
    /// use (task 6.2, Property 3). A blob whose `sha256` does not match its
    /// manifest descriptor — or a target absent from the manifest — is an error;
    /// the caller must not execute it.
    pub async fn retrieve_verified(
        &self,
        version: &str,
        target: &Target,
        manifest: &IntegrityManifest,
    ) -> Result<Vec<u8>, BinaryError> {
        let bytes = self.retrieve(version, target).await?;
        manifest.verify_artifact(&bytes, target)?;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BinaryArtifactDescriptor, sha256_hex};
    use tokeira_state::LocalBackend;

    fn store(root: &std::path::Path) -> BinaryStore {
        BinaryStore::new(Box::new(LocalBackend::new(root)), "binaries")
    }

    fn manifest_for(bytes: &[u8], target: &Target) -> IntegrityManifest {
        IntegrityManifest {
            provisioner_version: "1.0.0".to_string(),
            artifacts: vec![BinaryArtifactDescriptor {
                version: "1.0.0".to_string(),
                target: target.clone(),
                sha256: sha256_hex(bytes),
                retrieval_ref: None,
                size_bytes: bytes.len() as u64,
            }],
        }
    }

    #[tokio::test]
    async fn persist_then_retrieve_round_trips() {
        let tmp = tempfile::tempdir().unwrap();
        let store = store(tmp.path());
        let target = Target("aarch64-unknown-linux-musl".to_string());

        let key = store.persist("1.0.0", &target, b"binary-bytes").await.unwrap();
        assert!(key.starts_with("binaries/1.0.0-"));
        let back = store.retrieve("1.0.0", &target).await.unwrap();
        assert_eq!(back, b"binary-bytes");
    }

    #[tokio::test]
    async fn retrieve_verified_returns_matching_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        let store = store(tmp.path());
        let target = Target("aarch64-apple-darwin".to_string());
        let bytes = b"the-real-provisioner";

        store.persist("1.0.0", &target, bytes).await.unwrap();
        let manifest = manifest_for(bytes, &target);
        let verified = store
            .retrieve_verified("1.0.0", &target, &manifest)
            .await
            .expect("matching bytes verify");
        assert_eq!(verified, bytes);
    }

    #[tokio::test]
    async fn retrieve_verified_rejects_a_checksum_mismatch() {
        // The stored blob differs from what the manifest records → refuse.
        let tmp = tempfile::tempdir().unwrap();
        let store = store(tmp.path());
        let target = Target("aarch64-apple-darwin".to_string());

        store.persist("1.0.0", &target, b"tampered-bytes").await.unwrap();
        let manifest = manifest_for(b"the-expected-bytes", &target);
        let err = store
            .retrieve_verified("1.0.0", &target, &manifest)
            .await
            .expect_err("checksum mismatch is refused");
        assert!(matches!(err, BinaryError::Integrity(IntegrityError::ChecksumMismatch { .. })));
    }

    #[tokio::test]
    async fn retrieve_missing_blob_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let store = store(tmp.path());
        let target = Target("aarch64-apple-darwin".to_string());
        assert!(store.retrieve("9.9.9", &target).await.is_err());
    }
}
