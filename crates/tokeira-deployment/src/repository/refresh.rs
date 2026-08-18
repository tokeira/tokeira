//! Freshness refresh: re-sign the freshness statement for the current
//! publication (deployment-repository spec, Requirement 9.3).
//!
//! Refresh never alters targets or their claim. `timestamp.json` is the
//! only object it rewrites (the Repository Object Contract's one mutable
//! verification head); when the snapshot itself would expire inside the new
//! freshness window, a re-signed snapshot lands as a NEW versioned object —
//! create-only like every versioned metadata — referencing the same
//! targets. The next `publish_transition` may then collide with that
//! advanced snapshot version; the publication-conflict repair contract
//! (retry at the version the conflict names) recovers it.

use std::num::NonZeroU64;

use aws_lc_rs::rand::SystemRandom;
use jiff::Timestamp;
use tough::{
    editor::signed::SignedRole,
    schema::{Hashes, KeyHolder, Metafile, Timestamp as TimestampRole},
};

use super::{
    config::RepositoryConfig,
    error::PublishError,
    open::{Freshness, open},
    writer::{WriteSource, writer_for},
};

/// What a refresh signed, for the operator report.
#[derive(Debug)]
pub struct RefreshedFreshness {
    /// The publication (targets) version the freshness now vouches for.
    pub publication_version: u64,
    /// The new freshness expiration.
    pub timestamp_expires: Timestamp,
    /// The snapshot expiration now in force.
    pub snapshot_expires: Timestamp,
    /// Whether the snapshot itself was re-signed (as a new versioned
    /// object) because its expiry fell inside the new freshness window.
    pub snapshot_resigned: bool,
}

/// Re-sign the freshness statement (and, if its expiry requires, the
/// snapshot) for the current publication.
///
/// The load deliberately ignores expiration — restoring freshness to an
/// expired repository is this operation's purpose — while every signature
/// and rollback check still holds.
pub async fn refresh_freshness(
    config: &RepositoryConfig,
    trusted_root: &[u8],
    s3: Option<aws_sdk_s3::Client>,
) -> Result<RefreshedFreshness, PublishError> {
    let opened = open(
        &config.locator,
        trusted_root,
        None,
        Freshness::BreakGlass,
        s3.clone(),
    )
    .await
    .map_err(|error| PublishError::Other(format!("repository load refused: {error}")))?;
    let repo = opened.repository();
    let root = repo.root().signed.clone();
    let now = Timestamp::now();
    let timestamp_expires = now
        .checked_add(config.lifetimes.timestamp())
        .map_err(|error| PublishError::Other(error.to_string()))?;
    let rng = SystemRandom::new();

    // The snapshot must outlive the freshness window it anchors; when it
    // would not, re-sign it at the next version over the same targets.
    let snapshot = repo.snapshot();
    let resign_snapshot = snapshot.signed.expires < timestamp_expires;
    let (snapshot_version, snapshot_expires, resigned_snapshot) = if resign_snapshot {
        let mut fresh = snapshot.signed.clone();
        fresh.version = NonZeroU64::new(fresh.version.get().wrapping_add(1))
            .ok_or_else(|| PublishError::Other("snapshot version overflow".to_string()))?;
        fresh.expires = now
            .checked_add(config.lifetimes.metadata())
            .map_err(|error| PublishError::Other(error.to_string()))?;
        let version = fresh.version;
        let expires = fresh.expires;
        let signed = SignedRole::new(
            fresh,
            &KeyHolder::Root(root.clone()),
            &[config.keys.snapshot.source().boxed()],
            &rng,
        )
        .await
        .map_err(|error| PublishError::Signing {
            role: "snapshot",
            error: error.to_string(),
        })?;
        (version, expires, Some(signed.buffer().clone()))
    } else {
        (snapshot.signed.version, snapshot.signed.expires, None)
    };

    // The fresh statement: its own version advances; the snapshot is
    // referenced by version (hashes ride along when we hold the bytes;
    // version alone identifies a consistent-snapshot object otherwise).
    let mut statement = TimestampRole::new(
        "1.0.0".to_string(),
        NonZeroU64::new(repo.timestamp().signed.version.get().wrapping_add(1))
            .ok_or_else(|| PublishError::Other("timestamp version overflow".to_string()))?,
        timestamp_expires,
    );
    let meta = match &resigned_snapshot {
        Some(bytes) => Metafile {
            length: Some(bytes.len() as u64),
            hashes: Some(Hashes {
                sha256: aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, bytes)
                    .as_ref()
                    .to_vec()
                    .into(),
                _extra: std::collections::HashMap::new(),
            }),
            version: snapshot_version,
            _extra: std::collections::HashMap::new(),
        },
        None => Metafile {
            length: None,
            hashes: None,
            version: snapshot_version,
            _extra: std::collections::HashMap::new(),
        },
    };
    statement.meta.insert("snapshot.json".to_string(), meta);
    let signed_statement = SignedRole::new(
        statement,
        &KeyHolder::Root(root),
        &[config.keys.timestamp.source().boxed()],
        &rng,
    )
    .await
    .map_err(|error| PublishError::Signing {
        role: "timestamp",
        error: error.to_string(),
    })?;

    // Create-only object first, the mutable head last — a torn refresh
    // leaves the old freshness in force, never a mixture.
    let writer =
        writer_for(&config.locator, s3).map_err(|error| PublishError::Other(error.to_string()))?;
    if let Some(bytes) = &resigned_snapshot {
        writer
            .put_create_only(
                &format!("metadata/{snapshot_version}.snapshot.json"),
                WriteSource::Bytes(bytes),
            )
            .await
            .map_err(|error| PublishError::Other(error.to_string()))?;
    }
    writer
        .put_mutable_head("metadata/timestamp.json", signed_statement.buffer())
        .await
        .map_err(|error| PublishError::Other(error.to_string()))?;

    Ok(RefreshedFreshness {
        publication_version: opened.version(),
        timestamp_expires,
        snapshot_expires,
        snapshot_resigned: resign_snapshot,
    })
}
