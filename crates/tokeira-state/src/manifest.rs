//! Manifest and lease types for the S3 state backend.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Mutable head pointer stored in S3.
///
/// Updated via `If-None-Match: *` on creation and `If-Match: <etag>` for
/// subsequent writes. Actual document bytes live in immutable snapshots.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateManifest {
    pub schema_version: u32,
    pub revision: u64,
    pub head: Option<SnapshotRef>,
    pub lock: Option<StateLeaseLock>,
}

impl StateManifest {
    /// Construct an empty manifest for first-time bootstrap.
    pub fn empty() -> Self {
        Self {
            schema_version: 1,
            revision: 0,
            head: None,
            lock: None,
        }
    }
}

/// Immutable snapshot metadata referenced by the manifest head.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotRef {
    pub snapshot_key: String,
    pub snapshot_version_id: Option<String>,
    pub snapshot_etag: String,
    pub sha256_hex: String,
    pub size_bytes: u64,
    pub commit_id: String,
    pub committed_at: DateTime<Utc>,
    pub committed_by: String,
}

/// Time-based lease stored inside the manifest.
///
/// Lock expiry is evaluated from client time with a small skew tolerance.
/// Operators need reasonably synchronized clocks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateLeaseLock {
    pub owner_id: String,
    pub token: String,
    pub acquired_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

/// Manifest bytes plus the ETag required for CAS updates.
#[derive(Debug, Clone)]
pub struct ManifestState {
    pub manifest: StateManifest,
    pub etag: String,
    pub version_id: Option<String>,
}

/// In-memory representation of a held lease.
#[derive(Debug, Clone)]
pub struct LockGuard {
    pub owner_id: String,
    pub token: String,
    pub expires_at: DateTime<Utc>,
}
