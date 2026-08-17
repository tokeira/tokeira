//! Optional binary retention (task 6, re-keyed by task 16.2; Property 3).
//!
//! A provisioner binary blob is persisted keyed by **`EngineIdentity` +
//! `target`** — the identity digest addresses the blob, so two deployments on
//! the same engine share one retained artifact and a semver label can never
//! alias two different builds. Retrieval **checksum-verifies** against the
//! integrity manifest before execution — a blob whose `sha256` does not match
//! its manifest descriptor is never handed back for execution (the caller
//! aborts). Built over any [`StateBackend`]'s immutable snapshot I/O, so one
//! store serves both the cloud (`S3Backend`) and local dev (`LocalBackend`).
//!
//! Only identity-keyed bundles are retainable: a pre-identity (native dev)
//! manifest has no address here, which is correct — the dev loop re-builds
//! rather than retains (Proposal 005).

use tokeira_state::{StateBackend, StateError};

use crate::{EngineIdentity, IntegrityError, IntegrityManifest, Target};

/// Failure retrieving-and-verifying a binary.
#[derive(Debug, thiserror::Error)]
pub enum BinaryError {
    #[error(transparent)]
    Store(#[from] StateError),
    #[error(transparent)]
    Integrity(#[from] IntegrityError),
    /// The manifest does not describe the engine identity the caller asked
    /// for — verifying bytes for identity X against identity Y's manifest (or
    /// a pre-identity manifest) is a category error, refused outright.
    #[error(
        "integrity manifest does not describe engine identity {requested} \
         (manifest records {recorded})"
    )]
    IdentityMismatch { requested: String, recorded: String },
}

/// An immutable store for provisioner binary blobs, over a [`StateBackend`],
/// addressed by `EngineIdentity × target`.
pub struct BinaryStore {
    backend: Box<dyn StateBackend>,
    prefix: String,
}

// Manual impl: `backend` is a trait object without a `Debug` bound.
impl std::fmt::Debug for BinaryStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BinaryStore")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
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

    fn key(&self, identity: &EngineIdentity, target: &Target) -> String {
        format!(
            "{}/{}/{}",
            self.prefix,
            identity.digest().to_hex(),
            target.0
        )
    }

    /// Persist a binary blob keyed by `identity`+`target`. Immutable and
    /// idempotent (re-persisting the same key is a no-op). Returns the retrieval
    /// key, suitable for a
    /// [`BinaryArtifactDescriptor::retrieval_ref`](crate::BinaryArtifactDescriptor::retrieval_ref).
    pub async fn persist(
        &self,
        identity: &EngineIdentity,
        target: &Target,
        bytes: &[u8],
    ) -> Result<String, StateError> {
        let key = self.key(identity, target);
        self.backend.write_snapshot(&key, bytes).await?;
        Ok(key)
    }

    /// Retrieve a binary blob (unverified — prefer [`retrieve_verified`](Self::retrieve_verified)).
    pub async fn retrieve(
        &self,
        identity: &EngineIdentity,
        target: &Target,
    ) -> Result<Vec<u8>, StateError> {
        let key = self.key(identity, target);
        self.backend.read_snapshot(&key).await
    }

    /// Retrieve a binary blob **and checksum-verify** it against `manifest`
    /// before use (task 6.2, Property 3). The manifest must describe the
    /// requested `identity` — a manifest for a different (or no) identity is
    /// refused before any byte check — and a blob whose `sha256` does not match
    /// its descriptor, or a target absent from the manifest, is an error; the
    /// caller must not execute it.
    pub async fn retrieve_verified(
        &self,
        identity: &EngineIdentity,
        target: &Target,
        manifest: &IntegrityManifest,
    ) -> Result<Vec<u8>, BinaryError> {
        let requested = identity.digest();
        let recorded = manifest
            .engine_identity
            .as_ref()
            .map(EngineIdentity::digest);
        if recorded != Some(requested) {
            return Err(BinaryError::IdentityMismatch {
                requested: requested.to_hex(),
                recorded: recorded
                    .map(|d| d.to_hex())
                    .unwrap_or_else(|| "no identity (pre-identity manifest)".to_string()),
            });
        }
        let bytes = self.retrieve(identity, target).await?;
        manifest.verify_artifact(&bytes, target)?;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BinaryArtifactDescriptor, BuildProfile, Sha256Digest, sha256_hex};
    use tokeira_state::LocalBackend;

    fn store(root: &std::path::Path) -> BinaryStore {
        BinaryStore::new(Box::new(LocalBackend::new(root)), "binaries")
    }

    fn identity(marker: &[u8]) -> EngineIdentity {
        EngineIdentity {
            source_closure: Sha256Digest::from_bytes(marker),
            lock_closure: Sha256Digest::from_bytes(b"lock"),
            toolchain: "rustc 1.88.0".to_string(),
            build_container: None,
            features: ["provisioner".to_string()].into(),
            profile: BuildProfile::Dist,
        }
    }

    fn manifest_for(bytes: &[u8], target: &Target, identity: &EngineIdentity) -> IntegrityManifest {
        IntegrityManifest {
            engine_identity: Some(identity.clone()),
            provisioner_version: "1.0.0".to_string(),
            artifacts: vec![BinaryArtifactDescriptor {
                target: target.clone(),
                sha256: sha256_hex(bytes),
                retrieval_ref: None,
                size_bytes: bytes.len() as u64,
            }],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn persist_then_retrieve_round_trips_under_the_identity_key() {
        let tmp = tempfile::tempdir().unwrap();
        let store = store(tmp.path());
        let id = identity(b"engine-1");
        let target = Target("aarch64-unknown-linux-musl".to_string());

        let key = store.persist(&id, &target, b"binary-bytes").await.unwrap();
        assert_eq!(
            key,
            format!("binaries/{}/{}", id.digest().to_hex(), target.0),
            "the retrieval key is identity-digest addressed"
        );
        let back = store.retrieve(&id, &target).await.unwrap();
        assert_eq!(back, b"binary-bytes");
    }

    #[tokio::test]
    async fn distinct_identities_do_not_collide() {
        // Same target, same version label in life — different engines must
        // land under different keys (the aliasing the version key allowed).
        let tmp = tempfile::tempdir().unwrap();
        let store = store(tmp.path());
        let target = Target("t".to_string());
        let (a, b) = (identity(b"engine-a"), identity(b"engine-b"));

        store.persist(&a, &target, b"bytes-of-A").await.unwrap();
        store.persist(&b, &target, b"bytes-of-B").await.unwrap();
        assert_eq!(store.retrieve(&a, &target).await.unwrap(), b"bytes-of-A");
        assert_eq!(store.retrieve(&b, &target).await.unwrap(), b"bytes-of-B");
    }

    #[tokio::test]
    async fn retrieve_verified_returns_matching_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        let store = store(tmp.path());
        let id = identity(b"engine-1");
        let target = Target("aarch64-apple-darwin".to_string());
        let bytes = b"the-real-provisioner";

        store.persist(&id, &target, bytes).await.unwrap();
        let manifest = manifest_for(bytes, &target, &id);
        let verified = store
            .retrieve_verified(&id, &target, &manifest)
            .await
            .expect("matching bytes verify");
        assert_eq!(verified, bytes);
    }

    #[tokio::test]
    async fn retrieve_verified_rejects_a_checksum_mismatch() {
        // The stored blob differs from what the manifest records → refuse.
        let tmp = tempfile::tempdir().unwrap();
        let store = store(tmp.path());
        let id = identity(b"engine-1");
        let target = Target("aarch64-apple-darwin".to_string());

        store
            .persist(&id, &target, b"the-tampered-bytes")
            .await
            .unwrap();
        let manifest = manifest_for(b"the-expected-bytes", &target, &id);
        let err = store
            .retrieve_verified(&id, &target, &manifest)
            .await
            .expect_err("checksum mismatch is refused");
        assert!(matches!(
            err,
            BinaryError::Integrity(IntegrityError::ChecksumMismatch { .. })
        ));
    }

    #[tokio::test]
    async fn retrieve_verified_refuses_a_manifest_for_another_identity() {
        let tmp = tempfile::tempdir().unwrap();
        let store = store(tmp.path());
        let (id, other) = (identity(b"engine-1"), identity(b"engine-2"));
        let target = Target("t".to_string());
        let bytes = b"bytes";

        store.persist(&id, &target, bytes).await.unwrap();
        // Manifest describes a different engine — refused before any byte check.
        let manifest = manifest_for(bytes, &target, &other);
        let err = store
            .retrieve_verified(&id, &target, &manifest)
            .await
            .expect_err("identity mismatch is refused");
        assert!(matches!(err, BinaryError::IdentityMismatch { .. }));
    }

    #[tokio::test]
    async fn retrieve_verified_refuses_a_pre_identity_manifest() {
        let tmp = tempfile::tempdir().unwrap();
        let store = store(tmp.path());
        let id = identity(b"engine-1");
        let target = Target("t".to_string());
        let bytes = b"bytes";

        store.persist(&id, &target, bytes).await.unwrap();
        let manifest = IntegrityManifest {
            engine_identity: None,
            ..manifest_for(bytes, &target, &id)
        };
        let err = store
            .retrieve_verified(&id, &target, &manifest)
            .await
            .expect_err("a pre-identity manifest cannot verify an identity-keyed blob");
        assert!(matches!(err, BinaryError::IdentityMismatch { .. }));
    }

    #[tokio::test]
    async fn retrieve_missing_blob_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let store = store(tmp.path());
        let target = Target("aarch64-apple-darwin".to_string());
        assert!(store.retrieve(&identity(b"absent"), &target).await.is_err());
    }
}
