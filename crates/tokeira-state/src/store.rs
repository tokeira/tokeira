//! The store seam: one abstraction over both state-store implementations so a
//! deployment can select its *store*, not just a backend.
//!
//! [`CasStore`] (single-document CAS over a [`StateBackend`](crate::StateBackend))
//! and [`S3StateStore`] (S3-native manifest + immutable snapshots + lease lock)
//! have different native APIs. [`DeploymentStore`] unifies them behind a
//! `load`/`save` pair that threads an opaque version tag, so the engines can hold
//! a `Box<dyn DeploymentStore<T>>` and each platform decides which store to
//! construct.

use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};

use crate::{CasStore, S3StateStore, StateError, Validate};

/// A deployment state store, abstracting over [`CasStore`] and [`S3StateStore`].
///
/// `load` returns the current document plus an opaque version tag; pass that tag
/// back to `save` (empty string on first write). The CAS store uses the tag for
/// optimistic compare-and-swap; the S3-native store self-manages CAS through its
/// lease and manifest ETag, so for it the tag is advisory (higher-level mutual
/// exclusion is the remote operation lock).
#[async_trait]
pub trait DeploymentStore<T>: Send + Sync {
    /// Load the current document and an opaque version tag for CAS on save.
    async fn load(&self) -> Result<(T, String), StateError>;

    /// Save the document. `expected_version` is the tag from the load that
    /// produced `doc` (empty for the first write). Returns the new version tag.
    async fn save(&self, doc: &T, expected_version: &str) -> Result<String, StateError>;
}

#[async_trait]
impl<T> DeploymentStore<T> for CasStore<T>
where
    T: Serialize + DeserializeOwned + Default + Validate + Send + Sync,
{
    async fn load(&self) -> Result<(T, String), StateError> {
        CasStore::load(self).await
    }

    async fn save(&self, doc: &T, expected_version: &str) -> Result<String, StateError> {
        CasStore::save(self, doc, expected_version).await
    }
}

#[async_trait]
impl<T> DeploymentStore<T> for S3StateStore<T>
where
    T: Serialize + DeserializeOwned + Default + Validate + Send + Sync,
{
    async fn load(&self) -> Result<(T, String), StateError> {
        S3StateStore::load_with_version(self).await
    }

    async fn save(&self, doc: &T, _expected_version: &str) -> Result<String, StateError> {
        // The S3-native store self-manages CAS via its lease + manifest ETag, so
        // the caller's expected version is advisory. The returned manifest ETag
        // is the new version tag.
        S3StateStore::save(self, doc).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LocalBackend;
    use serde::Deserialize;

    #[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
    struct Doc {
        value: u32,
    }

    impl Validate for Doc {
        fn validate(&self) -> Result<(), StateError> {
            Ok(())
        }
    }

    fn cas_store(root: &std::path::Path) -> Box<dyn DeploymentStore<Doc>> {
        Box::new(CasStore::new(
            Box::new(LocalBackend::new(root)),
            "doc".to_string(),
        ))
    }

    #[tokio::test]
    async fn cas_store_as_deployment_store_round_trips() {
        let tmp = tempfile::tempdir().unwrap();
        let store = cas_store(tmp.path());

        // Empty load returns default + empty version.
        let (doc, version) = store.load().await.unwrap();
        assert_eq!(doc, Doc::default());
        assert_eq!(version, "");

        // Save then load round-trips through the trait object.
        let new_version = store.save(&Doc { value: 42 }, &version).await.unwrap();
        assert!(!new_version.is_empty());
        let (loaded, _) = store.load().await.unwrap();
        assert_eq!(loaded, Doc { value: 42 });
    }

    #[tokio::test]
    async fn cas_store_as_deployment_store_rejects_stale_version() {
        let tmp = tempfile::tempdir().unwrap();
        let store = cas_store(tmp.path());

        let (_, v0) = store.load().await.unwrap();
        store.save(&Doc { value: 1 }, &v0).await.unwrap();

        // Reusing the stale (empty) version conflicts — the CAS is enforced
        // through the trait object.
        let err = store.save(&Doc { value: 2 }, &v0).await.unwrap_err();
        assert!(matches!(err, StateError::Conflict(_)));
    }
}
