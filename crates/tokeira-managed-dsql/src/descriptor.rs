//! Durable managed-cluster descriptor and local compare-and-swap store.

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::{
    fmt,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::identity::{CanonicalClusterIdentity, IdentityError};

const FORMAT_VERSION: u32 = 1;

/// An explicit AWS DSQL control-plane idempotency token.
///
/// The managed lifecycle serializes the creation token into the durable descriptor.
/// Formatting always redacts token values so diagnostics cannot leak them.
#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct DsqlClientToken(String);

impl DsqlClientToken {
    /// Constructs a token satisfying the AWS 1–128 printable-ASCII contract.
    pub fn new(value: impl Into<String>) -> Result<Self, DescriptorError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || !value.bytes().all(|byte| (b'!'..=b'~').contains(&byte))
        {
            return Err(DescriptorError::InvalidClientToken);
        }
        Ok(Self(value))
    }

    /// Returns the value for the AWS request boundary.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for DsqlClientToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DsqlClientToken([REDACTED])")
    }
}

impl fmt::Display for DsqlClientToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

/// Version 1 of the durable cluster descriptor.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClusterDescriptorV1 {
    /// On-disk schema version; must be exactly one.
    pub format_version: u32,
    /// Store-controlled monotonic compare-and-swap revision.
    pub revision: u64,
    /// Region in which the dedicated cluster is managed.
    pub region: String,
    /// Explicit token persisted before the first create request.
    pub creation_client_token: DsqlClientToken,
    /// Durable lifecycle phase.
    pub state: ClusterDescriptorState,
}

impl ClusterDescriptorV1 {
    /// Constructs an uncommitted pending descriptor.
    pub fn pending(region: impl Into<String>, token: DsqlClientToken) -> Self {
        Self {
            format_version: FORMAT_VERSION,
            revision: 0,
            region: region.into(),
            creation_client_token: token,
            state: ClusterDescriptorState::PendingCreate,
        }
    }

    /// Validates format, token, and any persisted canonical identity.
    pub fn validate(&self) -> Result<(), DescriptorError> {
        if self.format_version != FORMAT_VERSION {
            return Err(DescriptorError::UnsupportedFormat(self.format_version));
        }
        DsqlClientToken::new(self.creation_client_token.0.clone())?;
        if self.region.is_empty() {
            return Err(DescriptorError::InvalidIdentity(
                IdentityError::InvalidRegion,
            ));
        }
        match &self.state {
            ClusterDescriptorState::PendingCreate => Ok(()),
            ClusterDescriptorState::Ready {
                cluster_id,
                cluster_arn,
                endpoint,
            }
            | ClusterDescriptorState::Destroyed {
                cluster_id,
                cluster_arn,
                endpoint,
                ..
            } => {
                CanonicalClusterIdentity::new(&self.region, cluster_id, cluster_arn)?;
                if endpoint.is_empty() {
                    return Err(DescriptorError::EmptyEndpoint);
                }
                Ok(())
            }
        }
    }
}

/// Durable managed-cluster phase.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClusterDescriptorState {
    /// The client token is durable but canonical identity is not yet recorded.
    PendingCreate,
    /// The AWS create result has been durably bound to canonical identity.
    Ready {
        /// Canonical cluster ID.
        cluster_id: String,
        /// Canonical cluster ARN.
        cluster_arn: String,
        /// Refreshable connection locator.
        endpoint: String,
    },
    /// Explicit destruction completed; ordinary startup must not recreate.
    Destroyed {
        /// Former canonical cluster ID.
        cluster_id: String,
        /// Former canonical cluster ARN.
        cluster_arn: String,
        /// Last observed connection locator.
        endpoint: String,
        /// Time at which destruction was observed complete.
        destroyed_at: OffsetDateTime,
    },
}

/// A parsed descriptor whose on-disk version has been recognized.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VersionedClusterDescriptor {
    /// Current descriptor version.
    V1(ClusterDescriptorV1),
}

impl VersionedClusterDescriptor {
    /// Returns the current descriptor representation.
    pub fn into_v1(self) -> ClusterDescriptorV1 {
        match self {
            Self::V1(descriptor) => descriptor,
        }
    }

    /// Borrows the current descriptor representation.
    pub fn as_v1(&self) -> &ClusterDescriptorV1 {
        match self {
            Self::V1(descriptor) => descriptor,
        }
    }
}

/// Durable descriptor operations used by lifecycle recovery.
#[async_trait]
pub trait ClusterDescriptorStore: Send + Sync + fmt::Debug {
    /// Loads the recognized descriptor, or `None` when no create decision is durable.
    async fn load(&self) -> Result<Option<VersionedClusterDescriptor>, DescriptorError>;

    /// Writes `next` only if the current revision matches `expected_revision`.
    ///
    /// `None` means that the descriptor must not yet exist. The store assigns and
    /// returns the next monotonic revision, ignoring `next.revision`.
    async fn compare_and_swap(
        &self,
        expected_revision: Option<u64>,
        next: &ClusterDescriptorV1,
    ) -> Result<u64, DescriptorError>;
}

/// Crash-safe JSON descriptor store for an exclusive embedded-engine process.
#[derive(Clone, Debug)]
pub struct LocalClusterDescriptorStore {
    path: PathBuf,
}

impl LocalClusterDescriptorStore {
    /// Creates a store at `path`; its sidecar lock is `path` plus `.lock`.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Returns the descriptor path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[async_trait]
impl ClusterDescriptorStore for LocalClusterDescriptorStore {
    async fn load(&self) -> Result<Option<VersionedClusterDescriptor>, DescriptorError> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || with_lock(&path, || read_descriptor(&path)))
            .await
            .map_err(|error| DescriptorError::Join(error.to_string()))?
    }

    async fn compare_and_swap(
        &self,
        expected_revision: Option<u64>,
        next: &ClusterDescriptorV1,
    ) -> Result<u64, DescriptorError> {
        let path = self.path.clone();
        let next = next.clone();
        tokio::task::spawn_blocking(move || {
            with_lock(&path, || {
                let current = read_descriptor(&path)?;
                let actual = current.as_ref().map(|value| value.as_v1().revision);
                if actual != expected_revision {
                    return Err(DescriptorError::CasConflict {
                        expected: expected_revision,
                        actual,
                    });
                }
                let revision = actual
                    .unwrap_or(0)
                    .checked_add(1)
                    .ok_or(DescriptorError::RevisionExhausted)?;
                let mut committed = next;
                committed.format_version = FORMAT_VERSION;
                committed.revision = revision;
                committed.validate()?;
                persist_atomically(&path, &committed)?;
                Ok(revision)
            })
        })
        .await
        .map_err(|error| DescriptorError::Join(error.to_string()))?
    }
}

fn lock_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(".lock");
    PathBuf::from(value)
}

fn with_lock<T>(
    path: &Path,
    operation: impl FnOnce() -> Result<T, DescriptorError>,
) -> Result<T, DescriptorError> {
    let parent = parent_directory(path)?;
    fs::create_dir_all(parent).map_err(DescriptorError::io)?;
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true);
    #[cfg(unix)]
    options.mode(0o600);
    let lock = options.open(lock_path(path)).map_err(DescriptorError::io)?;
    lock.lock().map_err(DescriptorError::io)?;
    let result = operation();
    let unlock = lock.unlock().map_err(DescriptorError::io);
    match (result, unlock) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), _) | (Ok(_), Err(error)) => Err(error),
    }
}

fn read_descriptor(path: &Path) -> Result<Option<VersionedClusterDescriptor>, DescriptorError> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(DescriptorError::io(error)),
    };
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(DescriptorError::io)?;
    let descriptor: ClusterDescriptorV1 = serde_json::from_slice(&bytes)
        .map_err(|error| DescriptorError::Corrupt(error.to_string()))?;
    descriptor.validate()?;
    Ok(Some(VersionedClusterDescriptor::V1(descriptor)))
}

fn persist_atomically(
    path: &Path,
    descriptor: &ClusterDescriptorV1,
) -> Result<(), DescriptorError> {
    persist_atomically_with(
        path,
        descriptor,
        File::sync_all,
        |from, to| fs::rename(from, to),
        |parent| File::open(parent).and_then(|directory| directory.sync_all()),
    )
}

fn persist_atomically_with<SyncFile, Rename, SyncParent>(
    path: &Path,
    descriptor: &ClusterDescriptorV1,
    sync_file: SyncFile,
    rename: Rename,
    sync_parent: SyncParent,
) -> Result<(), DescriptorError>
where
    SyncFile: FnOnce(&File) -> std::io::Result<()>,
    Rename: FnOnce(&Path, &Path) -> std::io::Result<()>,
    SyncParent: FnOnce(&Path) -> std::io::Result<()>,
{
    let parent = parent_directory(path)?;
    let file_name = path
        .file_name()
        .ok_or_else(|| DescriptorError::InvalidPath(path.to_path_buf()))?
        .to_string_lossy();
    let temporary = parent.join(format!(".{file_name}.{}.tmp", Uuid::new_v4()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let result = (|| {
        let mut file = options.open(&temporary).map_err(DescriptorError::io)?;
        let bytes = serde_json::to_vec_pretty(descriptor)
            .map_err(|error| DescriptorError::Corrupt(error.to_string()))?;
        file.write_all(&bytes).map_err(DescriptorError::io)?;
        file.write_all(b"\n").map_err(DescriptorError::io)?;
        sync_file(&file).map_err(DescriptorError::io)?;
        rename(&temporary, path).map_err(DescriptorError::io)?;
        sync_parent(parent).map_err(DescriptorError::io)
    })();
    if result.is_err() {
        let _ignored = fs::remove_file(&temporary);
    }
    result
}

fn parent_directory(path: &Path) -> Result<&Path, DescriptorError> {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| DescriptorError::InvalidPath(path.to_path_buf()))
}

/// Descriptor validation, persistence, or compare-and-swap failure.
#[derive(Debug, Error)]
pub enum DescriptorError {
    /// The client token violates AWS length or character constraints.
    #[error("creation client token is invalid")]
    InvalidClientToken,
    /// A descriptor format is corrupt JSON or otherwise undecodable.
    #[error("cluster descriptor is corrupt: {0}")]
    Corrupt(String),
    /// The descriptor was written by an unsupported future format.
    #[error("unsupported cluster descriptor format version {0}")]
    UnsupportedFormat(u32),
    /// The descriptor contains inconsistent canonical identity.
    #[error(transparent)]
    InvalidIdentity(#[from] IdentityError),
    /// A ready/destroyed descriptor has no connection locator.
    #[error("cluster descriptor endpoint is empty")]
    EmptyEndpoint,
    /// The descriptor path has no usable parent or filename.
    #[error("invalid cluster descriptor path: {0}")]
    InvalidPath(PathBuf),
    /// Another process committed a different revision first.
    #[error(
        "cluster descriptor compare-and-swap conflict (expected {expected:?}, actual {actual:?})"
    )]
    CasConflict {
        /// Revision supplied by the caller.
        expected: Option<u64>,
        /// Revision found under the exclusive lock.
        actual: Option<u64>,
    },
    /// The revision counter cannot advance safely.
    #[error("cluster descriptor revision is exhausted")]
    RevisionExhausted,
    /// A filesystem durability operation failed.
    #[error("cluster descriptor I/O failed: {0}")]
    Io(String),
    /// A blocking filesystem operation could not complete.
    #[error("cluster descriptor worker failed: {0}")]
    Join(String),
}

impl DescriptorError {
    fn io(error: std::io::Error) -> Self {
        Self::Io(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    use proptest::prelude::*;
    use tempfile::tempdir;

    use super::{
        ClusterDescriptorState, ClusterDescriptorStore, ClusterDescriptorV1, DescriptorError,
        DsqlClientToken, LocalClusterDescriptorStore, persist_atomically_with,
    };

    fn pending(region: &str, token: &str) -> ClusterDescriptorV1 {
        ClusterDescriptorV1::pending(
            region,
            DsqlClientToken::new(token).expect("fixture token is valid"),
        )
    }

    #[tokio::test]
    async fn persists_before_loading_and_redacts_token() {
        let directory = tempdir().expect("temporary directory is available");
        let path = directory.path().join("cluster.json");
        let store = LocalClusterDescriptorStore::new(&path);
        let descriptor = pending("eu-west-2", "secret-client-token");
        assert_eq!(
            store
                .compare_and_swap(None, &descriptor)
                .await
                .expect("CAS succeeds"),
            1
        );
        let loaded = store
            .load()
            .await
            .expect("load succeeds")
            .expect("descriptor exists");
        assert_eq!(loaded.as_v1().revision, 1);
        assert!(!format!("{loaded:?}").contains("secret-client-token"));
        assert!(!format!("{}", descriptor.creation_client_token).contains("secret-client-token"));
        #[cfg(unix)]
        assert_eq!(
            fs::metadata(path)
                .expect("descriptor exists")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }

    #[tokio::test]
    async fn rejects_corrupt_future_and_unknown_descriptors() {
        let directory = tempdir().expect("temporary directory is available");
        let path = directory.path().join("cluster.json");
        fs::write(&path, b"not json").expect("fixture write succeeds");
        let store = LocalClusterDescriptorStore::new(&path);
        assert!(matches!(
            store.load().await,
            Err(DescriptorError::Corrupt(_))
        ));
        let mut future = pending("eu-west-2", "token");
        future.format_version = 2;
        fs::write(
            &path,
            serde_json::to_vec(&future).expect("serialize fixture"),
        )
        .expect("fixture write succeeds");
        assert!(matches!(
            store.load().await,
            Err(DescriptorError::UnsupportedFormat(2))
        ));
        fs::write(
            &path,
            br#"{"format_version":1,"revision":1,"region":"eu-west-2","creation_client_token":"token","state":{"phase":"pending_create"},"extra":true}"#,
        )
        .expect("fixture write succeeds");
        assert!(matches!(
            store.load().await,
            Err(DescriptorError::Corrupt(_))
        ));
    }

    #[test]
    fn file_sync_and_rename_failures_leave_no_partial_descriptor() {
        let directory = tempdir().expect("temporary directory is available");
        let path = directory.path().join("cluster.json");
        let descriptor = pending("eu-west-2", "token");
        let sync_error = persist_atomically_with(
            &path,
            &descriptor,
            |_| Err(std::io::Error::other("injected file sync failure")),
            |from, to| fs::rename(from, to),
            |_| Ok(()),
        );
        assert!(matches!(sync_error, Err(DescriptorError::Io(_))));
        assert!(!path.exists());

        let rename_error = persist_atomically_with(
            &path,
            &descriptor,
            |_| Ok(()),
            |_, _| Err(std::io::Error::other("injected rename failure")),
            |_| Ok(()),
        );
        assert!(matches!(rename_error, Err(DescriptorError::Io(_))));
        assert!(!path.exists());

        let entries = fs::read_dir(directory.path())
            .expect("temporary directory is readable")
            .collect::<Result<Vec<_>, _>>()
            .expect("directory entries are readable");
        assert!(
            entries.is_empty(),
            "failed writes must clean temporary files"
        );
    }

    #[tokio::test]
    async fn destroyed_descriptor_remains_a_tombstone() {
        let directory = tempdir().expect("temporary directory is available");
        let store = LocalClusterDescriptorStore::new(directory.path().join("cluster.json"));
        let mut descriptor = pending("eu-west-2", "token");
        descriptor.state = ClusterDescriptorState::Destroyed {
            cluster_id: "abcdefghijklmnopqrstuv1234".to_owned(),
            cluster_arn: "arn:aws:dsql:eu-west-2:123456789012:cluster/abcdefghijklmnopqrstuv1234"
                .to_owned(),
            endpoint: "example.dsql.eu-west-2.on.aws".to_owned(),
            destroyed_at: time::OffsetDateTime::UNIX_EPOCH,
        };
        store
            .compare_and_swap(None, &descriptor)
            .await
            .expect("CAS succeeds");
        assert!(matches!(
            store
                .load()
                .await
                .expect("load succeeds")
                .expect("descriptor exists")
                .as_v1()
                .state,
            ClusterDescriptorState::Destroyed { .. }
        ));
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        // Feature: managed-embedded-dsql, Property 2: descriptor CAS admits one canonical history
        #[test]
        fn descriptor_cas_admits_one_canonical_history(
            first_token in "[A-Za-z0-9-]{1,64}",
            second_token in "[A-Za-z0-9-]{1,64}"
        ) {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime builds");
            runtime.block_on(async {
                let directory = tempdir().expect("temporary directory is available");
                let store = LocalClusterDescriptorStore::new(directory.path().join("cluster.json"));
                let first = pending("eu-west-2", &first_token);
                let second = pending("eu-west-2", &second_token);
                let first_result = store.compare_and_swap(None, &first).await;
                let second_result = store.compare_and_swap(None, &second).await;
                prop_assert!(first_result.is_ok());
                let second_conflicted =
                    matches!(second_result, Err(DescriptorError::CasConflict { .. }));
                prop_assert!(second_conflicted);
                let loaded = store.load().await.expect("load succeeds").expect("winner is durable");
                prop_assert_eq!(loaded.as_v1().creation_client_token.expose(), first_token);
                Ok(())
            })?;
        }
    }
}
