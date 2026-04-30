use std::marker::PhantomData;

use serde::{Serialize, de::DeserializeOwned};

use crate::{Validate, backend::StateBackend, error::StateError};

/// Single-document compare-and-swap state store.
///
/// Delegates all I/O to a [`StateBackend`] trait object, enabling
/// local-filesystem, S3 adapter, or any other backend. Unlike
/// [`S3StateStore`](crate::S3StateStore) which uses a manifest pointer plus
/// immutable snapshots, this store writes the full serialized document
/// directly through the backend's manifest API.
pub struct CasStore<T> {
    backend: Box<dyn StateBackend>,
    key_prefix: String,
    _phantom: PhantomData<T>,
}

impl<T> CasStore<T>
where
    T: Serialize + DeserializeOwned + Default + Validate,
{
    pub fn new(backend: Box<dyn StateBackend>, key_prefix: String) -> Self {
        Self {
            backend,
            key_prefix,
            _phantom: PhantomData,
        }
    }

    /// Load the current state document, or Default if none exists.
    /// Returns `(document, version_tag)` for CAS on subsequent save.
    pub async fn load(&self) -> Result<(T, String), StateError> {
        match self.backend.read_manifest(&self.key_prefix).await? {
            Some((bytes, version)) => {
                let doc: T = serde_json::from_slice(&bytes).map_err(|e| {
                    StateError::Corrupted(format!("failed to deserialize state: {e}"))
                })?;
                doc.validate()?;
                Ok((doc, version))
            }
            None => Ok((T::default(), String::new())),
        }
    }

    /// Save a state document with CAS semantics.
    /// Returns the new version tag on success.
    pub async fn save(&self, doc: &T, expected_version: &str) -> Result<String, StateError> {
        doc.validate()?;
        let bytes = serde_json::to_vec_pretty(doc)
            .map_err(|e| StateError::Corrupted(format!("failed to serialize state: {e}")))?;
        self.backend
            .write_manifest(&self.key_prefix, &bytes, expected_version)
            .await?;
        // Re-read to get the new version tag
        match self.backend.read_manifest(&self.key_prefix).await? {
            Some((_, new_version)) => Ok(new_version),
            None => Ok(String::new()),
        }
    }
}
