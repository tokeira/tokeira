//! S3-backed remote state storage using a single mutable manifest and
//! immutable snapshot objects.
//!
//! Design:
//! - `manifest.json` is the only mutable object
//! - `snapshots/<timestamp>-<uuid>.json` stores full immutable state payloads
//! - manifest creation uses `If-None-Match: *`
//! - manifest updates use `If-Match: <etag>`
//! - snapshot writes use `If-None-Match: *`
//!
//! Correctness model:
//! - The manifest is the single serialization point for writers. A state update
//!   is committed only when the manifest CAS succeeds.
//! - Snapshot objects are never overwritten, so failed writers can leave orphan
//!   snapshots but cannot corrupt the committed head.
//! - `save()` acquires a short lease stored inside the manifest, uploads a new
//!   snapshot, then commits `head + unlock` in one CAS update.
//! - Reads follow the manifest head, fetch the referenced snapshot, and verify
//!   its SHA-256 digest against the checksum recorded in the manifest.

use std::{marker::PhantomData, process, time::Duration};

use aws_sdk_s3::{
    error::ProvideErrorMetadata,
    primitives::ByteStream,
    types::{ChecksumAlgorithm, ChecksumMode, ServerSideEncryption},
};
use chrono::{Duration as ChronoDuration, Utc};
use serde::{Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    Validate,
    error::StateError,
    manifest::{LockGuard, ManifestState, SnapshotRef, StateLeaseLock, StateManifest},
};

/// Generic S3-backed state store with CAS-updated manifest and immutable snapshots.
///
/// Layout:
/// - `{key_prefix}/manifest.json` — mutable compare-and-swap object
/// - `{key_prefix}/snapshots/<timestamp>-<uuid>.json` — immutable snapshots
pub struct S3StateStore<T> {
    client: aws_sdk_s3::Client,
    bucket: String,
    key_prefix: String,
    manifest_key: String,
    snapshot_prefix: String,
    kms_key_id: Option<String>,
    owner_id: String,
    _marker: PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned + Default + Validate> S3StateStore<T> {
    const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(60);
    const LOCK_SKEW_TOLERANCE: Duration = Duration::from_secs(5);

    /// Construct a new state store rooted at the given key prefix.
    pub fn new(client: aws_sdk_s3::Client, bucket: String, key_prefix: String) -> Self {
        let (trimmed, manifest_key, snapshot_prefix) = Self::layout_for_prefix(&key_prefix);
        let owner_id = format!("pid-{}-{}", process::id(), Uuid::new_v4());
        Self {
            client,
            bucket,
            manifest_key,
            snapshot_prefix,
            key_prefix: trimmed,
            kms_key_id: None,
            owner_id,
            _marker: PhantomData,
        }
    }

    pub fn layout_for_prefix(key_prefix: &str) -> (String, String, String) {
        let trimmed = key_prefix.trim_end_matches('/').to_string();
        let manifest_key = format!("{trimmed}/manifest.json");
        let snapshot_prefix = format!("{trimmed}/snapshots");
        (trimmed, manifest_key, snapshot_prefix)
    }

    /// Load the current state snapshot referenced by the manifest head.
    pub async fn load(&self) -> Result<T, StateError> {
        match self.get_manifest().await? {
            Some(manifest_state) => {
                let state = self.load_snapshot(&manifest_state).await?;
                state.validate()?;
                Ok(state)
            }
            None => Ok(T::default()),
        }
    }

    /// Persist a new state snapshot and atomically move the manifest head.
    ///
    /// Returns the manifest ETag after the successful commit so callers that
    /// coordinate multiple writes can carry the latest version forward.
    pub async fn save(&self, state: &T) -> Result<String, StateError> {
        let bytes = serde_json::to_vec_pretty(state)
            .map_err(|e| StateError::Corrupted(format!("failed to serialize state: {e}")))?;
        let guard = self
            .acquire_lock(self.owner_id.clone(), Self::DEFAULT_LEASE_DURATION)
            .await?;

        match self.write_snapshot(&guard, &bytes).await {
            Ok(_) => self
                .get_manifest()
                .await?
                .map(|manifest| manifest.etag)
                .ok_or_else(|| StateError::Corrupted("manifest missing after save".into())),
            Err(err @ StateError::Conflict(_))
            | Err(err @ StateError::LockLost(_))
            | Err(err @ StateError::Locked(_)) => Err(err),
            Err(err) => {
                if let Err(unlock_err) = self.unlock(&guard).await {
                    tracing::warn!(error = %unlock_err, "failed to release state lease after write error");
                }
                Err(err)
            }
        }
    }

    /// Like [`load`](Self::load) but also returns the manifest ETag as an opaque
    /// version tag (empty when no manifest exists yet). Used by the
    /// [`DeploymentStore`](crate::DeploymentStore) seam so a caller that threads a
    /// version between load and save can treat this store like the CAS store.
    pub async fn load_with_version(&self) -> Result<(T, String), StateError> {
        match self.get_manifest().await? {
            Some(manifest_state) => {
                let state = self.load_snapshot(&manifest_state).await?;
                state.validate()?;
                Ok((state, manifest_state.etag))
            }
            None => Ok((T::default(), String::new())),
        }
    }

    /// Load the manifest without loading the snapshot.
    pub async fn load_manifest(&self) -> Result<Option<ManifestState>, StateError> {
        self.get_manifest().await
    }

    /// Acquire a manifest lease using CAS on the manifest object.
    pub async fn acquire_lock(
        &self,
        owner_id: impl Into<String>,
        lease_duration: Duration,
    ) -> Result<LockGuard, StateError> {
        let owner_id = owner_id.into();
        let lease_delta = ChronoDuration::from_std(lease_duration)
            .map_err(|e| StateError::Other(anyhow::anyhow!("invalid lease duration: {e}")))?;

        loop {
            let current = self.ensure_manifest().await?;
            let now = Utc::now();

            if let Some(existing) = &current.manifest.lock
                && !Self::lock_has_expired(existing.expires_at, now)
            {
                return Err(StateError::Locked(format!(
                    "state is locked by {} until {}",
                    existing.owner_id, existing.expires_at
                )));
            }

            let lock = StateLeaseLock {
                owner_id: owner_id.clone(),
                token: Uuid::new_v4().to_string(),
                acquired_at: now,
                expires_at: now + lease_delta,
            };

            let mut next = current.manifest.clone();
            next.lock = Some(lock.clone());

            match self.put_manifest_if_match(&current.etag, &next).await {
                Ok(_) => {
                    return Ok(LockGuard {
                        owner_id: lock.owner_id,
                        token: lock.token,
                        expires_at: lock.expires_at,
                    });
                }
                Err(StateError::Conflict(_)) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Renew a held manifest lease.
    pub async fn renew_lock(
        &self,
        guard: &mut LockGuard,
        lease_duration: Duration,
    ) -> Result<(), StateError> {
        let lease_delta = ChronoDuration::from_std(lease_duration)
            .map_err(|e| StateError::Other(anyhow::anyhow!("invalid lease duration: {e}")))?;
        let current = self.ensure_manifest().await?;
        let current_lock = current
            .manifest
            .lock
            .as_ref()
            .ok_or_else(|| StateError::LockLost("manifest has no active lock".into()))?;

        if current_lock.token != guard.token || current_lock.owner_id != guard.owner_id {
            return Err(StateError::LockLost(
                "manifest lock belongs to another writer".into(),
            ));
        }

        let mut next = current.manifest.clone();
        let new_expiry = Utc::now() + lease_delta;
        if let Some(lock) = next.lock.as_mut() {
            lock.expires_at = new_expiry;
        }

        self.put_manifest_if_match(&current.etag, &next).await?;
        guard.expires_at = new_expiry;
        Ok(())
    }

    /// Upload a new immutable snapshot and CAS-update the manifest head.
    pub async fn write_snapshot(
        &self,
        guard: &LockGuard,
        state_bytes: &[u8],
    ) -> Result<SnapshotRef, StateError> {
        let current = self.ensure_manifest().await?;
        let current_lock = current
            .manifest
            .lock
            .as_ref()
            .ok_or_else(|| StateError::LockLost("manifest has no active lock".into()))?;

        if current_lock.token != guard.token || current_lock.owner_id != guard.owner_id {
            return Err(StateError::LockLost(
                "manifest lock belongs to another writer".into(),
            ));
        }

        if Self::lock_has_expired(current_lock.expires_at, Utc::now()) {
            return Err(StateError::LockLost(
                "lease expired before snapshot commit".into(),
            ));
        }

        let snapshot = self.put_snapshot(state_bytes, &guard.owner_id).await?;

        let mut next = current.manifest.clone();
        next.revision += 1;
        next.head = Some(snapshot.clone());
        next.lock = None;

        match self.put_manifest_if_match(&current.etag, &next).await {
            Ok(_) => Ok(snapshot),
            Err(StateError::Conflict(_)) => Err(StateError::Conflict(
                "manifest changed during commit; snapshot was uploaded but not committed".into(),
            )),
            Err(err) => Err(err),
        }
    }

    /// Release a held manifest lease without moving the head pointer.
    pub async fn unlock(&self, guard: &LockGuard) -> Result<(), StateError> {
        let current = self.ensure_manifest().await?;
        let current_lock = current
            .manifest
            .lock
            .as_ref()
            .ok_or_else(|| StateError::LockLost("manifest has no active lock".into()))?;

        if current_lock.token != guard.token || current_lock.owner_id != guard.owner_id {
            return Err(StateError::LockLost(
                "manifest lock belongs to another writer".into(),
            ));
        }

        let mut next = current.manifest.clone();
        next.lock = None;
        self.put_manifest_if_match(&current.etag, &next).await?;
        Ok(())
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn key_prefix(&self) -> &str {
        &self.key_prefix
    }

    pub fn manifest_key(&self) -> &str {
        &self.manifest_key
    }

    pub fn snapshot_prefix(&self) -> &str {
        &self.snapshot_prefix
    }

    // ── Private helpers ──────────────────────────────────────────

    async fn load_snapshot(&self, manifest_state: &ManifestState) -> Result<T, StateError> {
        let head = match &manifest_state.manifest.head {
            Some(head) => head,
            None => return Ok(T::default()),
        };

        let mut req = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&head.snapshot_key)
            .checksum_mode(ChecksumMode::Enabled);

        if let Some(version_id) = &head.snapshot_version_id {
            req = req.version_id(version_id);
        }

        let output = req.send().await.map_err(|e| {
            StateError::S3(format!(
                "failed to load snapshot {}: {}",
                head.snapshot_key, e
            ))
        })?;

        let bytes = output
            .body
            .collect()
            .await
            .map_err(|e| StateError::S3(format!("failed to read snapshot body: {e}")))?
            .into_bytes();

        let actual_sha256 = sha256_hex(&bytes);
        if actual_sha256 != head.sha256_hex {
            return Err(StateError::Corrupted(format!(
                "snapshot checksum mismatch for {}: manifest={}, actual={}",
                head.snapshot_key, head.sha256_hex, actual_sha256
            )));
        }

        serde_json::from_slice(&bytes)
            .map_err(|e| StateError::Corrupted(format!("invalid snapshot JSON: {e}")))
    }

    async fn ensure_manifest(&self) -> Result<ManifestState, StateError> {
        if let Some(existing) = self.get_manifest().await? {
            return Ok(existing);
        }

        let empty = StateManifest::empty();
        match self.put_manifest_if_absent(&empty).await {
            Ok(created) => Ok(created),
            Err(StateError::Conflict(_)) => self.get_manifest().await?.ok_or_else(|| {
                StateError::Conflict(
                    "manifest still missing after concurrent creation attempt".into(),
                )
            }),
            Err(err) => Err(err),
        }
    }

    async fn get_manifest(&self) -> Result<Option<ManifestState>, StateError> {
        let output = match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&self.manifest_key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
        {
            Ok(output) => output,
            Err(err) => {
                if is_missing(&err) || has_missing_status(&err) {
                    return Ok(None);
                }
                return Err(StateError::S3(format!(
                    "failed to load manifest: {}",
                    format_sdk_error(&err)
                )));
            }
        };

        let etag = output
            .e_tag()
            .ok_or_else(|| StateError::Corrupted("manifest missing ETag".into()))?
            .to_string();
        let version_id = output.version_id().map(ToOwned::to_owned);
        let bytes = output
            .body
            .collect()
            .await
            .map_err(|e| StateError::S3(format!("failed to read manifest body: {e}")))?
            .into_bytes();
        let manifest: StateManifest = serde_json::from_slice(&bytes)
            .map_err(|e| StateError::Corrupted(format!("invalid manifest JSON: {e}")))?;

        Ok(Some(ManifestState {
            manifest,
            etag,
            version_id,
        }))
    }

    async fn put_manifest_if_absent(
        &self,
        manifest: &StateManifest,
    ) -> Result<ManifestState, StateError> {
        let body = serde_json::to_vec_pretty(manifest)
            .map_err(|e| StateError::Corrupted(format!("failed to serialize manifest: {e}")))?;
        let (etag, version_id) = self
            .put_bytes(
                &self.manifest_key,
                body,
                "application/json",
                None,
                Some("*"),
            )
            .await?;

        Ok(ManifestState {
            manifest: manifest.clone(),
            etag,
            version_id,
        })
    }

    async fn put_manifest_if_match(
        &self,
        expected_etag: &str,
        manifest: &StateManifest,
    ) -> Result<ManifestState, StateError> {
        let body = serde_json::to_vec_pretty(manifest)
            .map_err(|e| StateError::Corrupted(format!("failed to serialize manifest: {e}")))?;
        let (etag, version_id) = self
            .put_bytes(
                &self.manifest_key,
                body,
                "application/json",
                Some(expected_etag),
                None,
            )
            .await?;

        Ok(ManifestState {
            manifest: manifest.clone(),
            etag,
            version_id,
        })
    }

    async fn put_snapshot(
        &self,
        state_bytes: &[u8],
        owner_id: &str,
    ) -> Result<SnapshotRef, StateError> {
        let sha256 = sha256_hex(state_bytes);
        let snapshot_key = format!(
            "{}/{}-{}.json",
            self.snapshot_prefix,
            Utc::now().format("%Y%m%dT%H%M%S%.3fZ"),
            Uuid::new_v4()
        );
        let (etag, version_id) = self
            .put_bytes(
                &snapshot_key,
                state_bytes.to_vec(),
                "application/json",
                None,
                Some("*"),
            )
            .await?;

        Ok(SnapshotRef {
            snapshot_key,
            snapshot_version_id: version_id,
            snapshot_etag: etag,
            sha256_hex: sha256,
            size_bytes: state_bytes.len() as u64,
            commit_id: Uuid::new_v4().to_string(),
            committed_at: Utc::now(),
            committed_by: owner_id.to_string(),
        })
    }

    async fn put_bytes(
        &self,
        key: &str,
        body: Vec<u8>,
        content_type: &str,
        if_match: Option<&str>,
        if_none_match: Option<&str>,
    ) -> Result<(String, Option<String>), StateError> {
        let checksum_b64 = sha256_base64(&body);

        let mut req = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .content_type(content_type)
            .body(ByteStream::from(body))
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .checksum_sha256(checksum_b64);

        if let Some(etag) = if_match {
            req = req.if_match(etag);
        }
        if let Some(value) = if_none_match {
            req = req.if_none_match(value);
        }

        req = match &self.kms_key_id {
            Some(kms_key_id) => req
                .server_side_encryption(ServerSideEncryption::AwsKms)
                .ssekms_key_id(kms_key_id),
            None => req.server_side_encryption(ServerSideEncryption::Aes256),
        };

        let output = req.send().await.map_err(|err| {
            if is_cas_conflict(&err) {
                StateError::Conflict(format!(
                    "conditional write failed for '{}': {}",
                    key,
                    format_sdk_error(&err)
                ))
            } else {
                StateError::S3(format!(
                    "failed to write '{}': {}",
                    key,
                    format_sdk_error(&err)
                ))
            }
        })?;

        let etag = output
            .e_tag()
            .ok_or_else(|| StateError::Corrupted(format!("PUT {key} missing ETag")))?;
        Ok((etag.to_string(), output.version_id().map(ToOwned::to_owned)))
    }

    fn lock_has_expired(expires_at: chrono::DateTime<Utc>, now: chrono::DateTime<Utc>) -> bool {
        let tolerance = ChronoDuration::from_std(Self::LOCK_SKEW_TOLERANCE)
            .expect("lock skew tolerance should be a valid chrono duration");
        expires_at + tolerance <= now
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    hex::encode(digest)
}

fn sha256_base64(bytes: &[u8]) -> String {
    use base64::Engine;
    let digest = Sha256::digest(bytes);
    base64::engine::general_purpose::STANDARD.encode(digest)
}

fn is_missing<E: ProvideErrorMetadata>(err: &E) -> bool {
    matches!(
        err.code(),
        Some("NoSuchKey") | Some("NotFound") | Some("NoSuchBucket")
    )
}

fn has_missing_status(
    err: &aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::get_object::GetObjectError>,
) -> bool {
    err.raw_response()
        .map(|response| u16::from(response.status()) == 404)
        .unwrap_or(false)
}

fn format_sdk_error<E: ProvideErrorMetadata>(err: &aws_sdk_s3::error::SdkError<E>) -> String {
    let mut details = Vec::new();

    details.push(format!("kind={err}"));

    if let Some(status) = err
        .raw_response()
        .map(|response| u16::from(response.status()))
    {
        details.push(format!("http_status={status}"));
    }
    if let Some(code) = err.code() {
        details.push(format!("code={code}"));
    }
    if let Some(message) = err.message() {
        details.push(format!("message={message}"));
    }

    details.join(", ")
}

fn is_cas_conflict<E: ProvideErrorMetadata>(err: &E) -> bool {
    matches!(
        err.code(),
        Some("PreconditionFailed") | Some("ConditionalRequestConflict")
    )
}
