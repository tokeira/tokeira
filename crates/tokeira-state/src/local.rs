use std::path::PathBuf;

use async_trait::async_trait;

use crate::{StateError, backend::StateBackend};

/// Filesystem-backed state storage for local development and tests.
///
/// Each logical state document is stored as `{root}/{key}/manifest.json`.
///
/// ## CAS semantics
///
/// Version tags are content-addressed SHA-256 hashes of the manifest bytes.
/// This gives reliable compare-and-swap: two writes from the same loaded
/// version will race, and at most one succeeds.
///
/// - `expected_version == ""` means "create only if no manifest exists."
///   If a manifest already exists, the write returns `StateError::Conflict`.
/// - `expected_version != ""` means "update only if the current content hash
///   matches." If it doesn't, the write returns `StateError::Conflict`.
///
/// ## Atomicity
///
/// Writes use a unique temp file (PID + random suffix) and `rename()` for
/// atomic replacement. Concurrent writers cannot clobber each other's temp
/// files.
///
/// ## Limitations
///
/// This is a single-host backend. It does not provide distributed locking.
/// Cloud deployments should use the S3 state path.
#[derive(Debug)]
pub struct LocalBackend {
    root: PathBuf,
}

impl LocalBackend {
    /// Create a backend rooted at the supplied directory.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

// Content hashing for the persisted manifest. The on-disk format is
// `{ "version": "<sha256>", "data": <raw bytes as base64> }`; the version returned
// to callers is the SHA-256 hex digest of the raw data bytes.
fn content_hash(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

fn unique_tmp_name() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let pid = std::process::id();
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("manifest.json.{pid}.{seq}.tmp")
}

#[async_trait]
impl StateBackend for LocalBackend {
    async fn read_manifest(&self, key: &str) -> Result<Option<(Vec<u8>, String)>, StateError> {
        let path = self.root.join(key).join("manifest.json");
        match tokio::fs::read(&path).await {
            Ok(data) => {
                let version = content_hash(&data);
                Ok(Some((data, version)))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StateError::Backend(format!("read {}: {e}", path.display()))),
        }
    }

    async fn write_manifest(
        &self,
        key: &str,
        data: &[u8],
        expected_version: &str,
    ) -> Result<(), StateError> {
        let dir = self.root.join(key);
        tokio::fs::create_dir_all(&dir)
            .await
            .map_err(|e| StateError::Backend(format!("mkdir {}: {e}", dir.display())))?;

        let path = dir.join("manifest.json");

        if expected_version.is_empty() {
            // Create-only: fail if manifest already exists
            if path.exists() {
                let existing = tokio::fs::read(&path)
                    .await
                    .map_err(|e| StateError::Backend(format!("read {}: {e}", path.display())))?;
                let current_version = content_hash(&existing);
                return Err(StateError::Conflict(format!(
                    "manifest already exists (version {current_version}); \
                     expected empty version for first write"
                )));
            }
        } else {
            // Update: verify current content hash matches expected
            match tokio::fs::read(&path).await {
                Ok(existing) => {
                    let current_version = content_hash(&existing);
                    if current_version != expected_version {
                        return Err(StateError::Conflict(format!(
                            "manifest version mismatch: expected {expected_version}, \
                             got {current_version}"
                        )));
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    return Err(StateError::Conflict(format!(
                        "manifest does not exist but expected version {expected_version}"
                    )));
                }
                Err(e) => {
                    return Err(StateError::Backend(format!("read {}: {e}", path.display())));
                }
            }
        }

        // Atomic write: unique temp file + rename
        let tmp = dir.join(unique_tmp_name());
        tokio::fs::write(&tmp, data)
            .await
            .map_err(|e| StateError::Backend(format!("write {}: {e}", tmp.display())))?;
        tokio::fs::rename(&tmp, &path).await.map_err(|e| {
            // Clean up temp file on rename failure
            let _ = std::fs::remove_file(&tmp);
            StateError::Backend(format!("rename {}: {e}", path.display()))
        })?;

        Ok(())
    }

    async fn read_snapshot(&self, key: &str) -> Result<Vec<u8>, StateError> {
        let path = self.root.join(key);
        tokio::fs::read(&path).await.map_err(|e| {
            // An absent key is `NotFound` — content-addressed consumers (the
            // binary/bundle stores) discriminate an honest miss from a
            // backend failure.
            if e.kind() == std::io::ErrorKind::NotFound {
                StateError::NotFound(format!("snapshot {key}"))
            } else {
                StateError::Backend(format!("read snapshot {}: {e}", path.display()))
            }
        })
    }

    async fn write_snapshot(&self, key: &str, data: &[u8]) -> Result<(), StateError> {
        let path = self.root.join(key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| StateError::Backend(format!("mkdir {}: {e}", parent.display())))?;
        }
        if path.exists() {
            return Ok(());
        }
        tokio::fs::write(&path, data)
            .await
            .map_err(|e| StateError::Backend(format!("write snapshot {}: {e}", path.display())))
    }

    async fn list_snapshots(&self, prefix: &str) -> Result<Vec<String>, StateError> {
        let dir = self.root.join(prefix);
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let mut entries = Vec::new();
        let mut read_dir = tokio::fs::read_dir(&dir)
            .await
            .map_err(|e| StateError::Backend(format!("readdir {}: {e}", dir.display())))?;
        while let Some(entry) = read_dir
            .next_entry()
            .await
            .map_err(|e| StateError::Backend(format!("readdir entry: {e}")))?
        {
            if let Some(name) = entry.file_name().to_str() {
                entries.push(format!("{prefix}/{name}"));
            }
        }
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The read_snapshot contract: an absent key is NotFound (an honest miss),
    // never a generic Backend failure — the content-addressed stores
    // discriminate on it.
    #[tokio::test]
    async fn missing_snapshot_reads_as_not_found() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(tmp.path());
        let err = backend.read_snapshot("absent/key").await.unwrap_err();
        assert!(matches!(err, StateError::NotFound(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn empty_version_creates_new_manifest() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(tmp.path());

        backend.write_manifest("test", b"hello", "").await.unwrap();

        let (data, version) = backend
            .read_manifest("test")
            .await
            .unwrap()
            .expect("manifest should exist");
        assert_eq!(data, b"hello");
        assert_eq!(version, content_hash(b"hello"));
    }

    #[tokio::test]
    async fn empty_version_rejects_existing_manifest() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(tmp.path());

        backend.write_manifest("test", b"first", "").await.unwrap();

        let err = backend
            .write_manifest("test", b"second", "")
            .await
            .unwrap_err();
        assert!(
            matches!(err, StateError::Conflict(_)),
            "expected Conflict, got {err:?}"
        );
    }

    #[tokio::test]
    async fn correct_version_allows_update() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(tmp.path());

        backend.write_manifest("test", b"v1", "").await.unwrap();
        let (_, version) = backend.read_manifest("test").await.unwrap().unwrap();

        backend
            .write_manifest("test", b"v2", &version)
            .await
            .unwrap();

        let (data, _) = backend.read_manifest("test").await.unwrap().unwrap();
        assert_eq!(data, b"v2");
    }

    #[tokio::test]
    async fn stale_version_rejects_update() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(tmp.path());

        backend.write_manifest("test", b"v1", "").await.unwrap();
        let (_, v1) = backend.read_manifest("test").await.unwrap().unwrap();

        // Another writer updates
        backend.write_manifest("test", b"v2", &v1).await.unwrap();

        // First writer tries with stale version
        let err = backend
            .write_manifest("test", b"v3", &v1)
            .await
            .unwrap_err();
        assert!(
            matches!(err, StateError::Conflict(_)),
            "expected Conflict, got {err:?}"
        );
    }

    #[tokio::test]
    async fn nonexistent_version_rejects_update() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(tmp.path());

        let err = backend
            .write_manifest("test", b"data", "some-version")
            .await
            .unwrap_err();
        assert!(
            matches!(err, StateError::Conflict(_)),
            "expected Conflict, got {err:?}"
        );
    }

    #[tokio::test]
    async fn read_missing_manifest_returns_none() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(tmp.path());

        assert!(backend.read_manifest("test").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn version_is_content_hash() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(tmp.path());

        backend
            .write_manifest("test", b"deterministic", "")
            .await
            .unwrap();

        let (_, version) = backend.read_manifest("test").await.unwrap().unwrap();
        assert_eq!(version, content_hash(b"deterministic"));

        // Same content always produces same version
        let tmp2 = tempfile::tempdir().unwrap();
        let backend2 = LocalBackend::new(tmp2.path());
        backend2
            .write_manifest("test", b"deterministic", "")
            .await
            .unwrap();
        let (_, version2) = backend2.read_manifest("test").await.unwrap().unwrap();
        assert_eq!(version, version2);
    }
}
