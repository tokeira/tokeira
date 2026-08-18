//! Publication: root authoring and `publish_transition`.
//!
//! Publication always follows the authoritative commit (deployment-dir
//! rename at create, envelope CAS at transitions) — it is a derived, signed
//! projection, and a publication failure never unwinds a commit; callers
//! report "publication pending" with `tkr deployment publish` as the remedy.
//!
//! The write order realizes the Repository Object Contract: every
//! create-only object (versioned metadata, content-named targets) is
//! admitted before the mutable heads move, so a torn publish is invisible —
//! a reader either resolves the old publication or the new one, never a
//! mixture. `expected_version` is the caller's last-observed publication
//! version; a concurrent publication surfaces as a typed conflict at the
//! first colliding create-only key, never as an overwrite. (The
//! operation-lease spec makes the race impossible; this makes it loud.)

use std::{collections::HashMap, num::NonZeroU64, path::PathBuf};

use aws_lc_rs::rand::SystemRandom;
use jiff::Timestamp;
use tough::{
    TargetName,
    editor::{RepositoryEditor, signed::SignedRole},
    schema::{KeyHolder, RoleKeys, RoleType, Root, Target},
};

use super::{
    claim::{CLAIM_KEY, COMPANION_KEY, CONFIG_KEY, DeploymentClaim, ENGINE_ARTIFACT_KEY},
    config::RepositoryConfig,
    error::{PublishError, WriteError},
    writer::{UploadOutcome, WriteSource, writer_for},
};
use crate::ProvisionerBundle;

/// One document of the publication: target name + bytes (definition root,
/// companions, config-tree files — all small).
pub type Document = (String, Vec<u8>);

/// Everything one publication is assembled from. Callers collect this from
/// the committed deployment dir — publish never reads mutable state itself.
#[derive(Debug)]
pub struct PublicationInput {
    /// The signed statement binding both halves.
    pub claim: DeploymentClaim,
    /// Definition root + companions + config-tree files, by target name.
    /// The root document MUST be present under `claim.definition.root`;
    /// config-tree files use their relative paths as target names.
    pub documents: Vec<Document>,
    /// Which documents are config-tree files (by target name); the rest are
    /// definition documents.
    pub config_tree: Vec<String>,
    /// The committed bundle manifest. Its artifact `retrieval_ref`s are
    /// rewritten here to name the engine binary targets before serialization.
    pub bundle_manifest: ProvisionerBundle,
    /// Per-target engine binaries: (target triple, path to the bytes).
    /// Streamed, never buffered whole.
    pub bundle_artifacts: Vec<(String, PathBuf)>,
}

/// What a completed publication looked like.
#[derive(Debug)]
pub struct PublicationReceipt {
    /// The publication version written (`expected_version + 1`).
    pub version: u64,
    /// Trust-anchor bytes: the signed root this publication verifies under
    /// (authored fresh at version 1, otherwise the caller's own).
    pub trusted_root: Vec<u8>,
    /// Per-object outcomes, in write order.
    pub outcomes: Vec<(String, UploadOutcome)>,
}

/// Write the next publication of this deployment's repository.
///
/// `trusted_root` carries the existing chain for `expected_version > 0`;
/// at version 0 (the birth publication) a fresh root v1 is authored from
/// the configured role keys. S3 residency needs `s3` supplied.
pub async fn publish_transition(
    config: &RepositoryConfig,
    input: PublicationInput,
    expected_version: u64,
    trusted_root: Option<&[u8]>,
    s3: Option<aws_sdk_s3::Client>,
) -> Result<PublicationReceipt, PublishError> {
    let version = expected_version
        .checked_add(1)
        .ok_or_else(|| PublishError::Other("publication version overflow".to_string()))?;
    let now = Timestamp::now();
    let lifetimes = &config.lifetimes;

    // 1. The trust anchor: authored at birth, carried thereafter.
    let (root_bytes, authored_root) = match trusted_root {
        Some(bytes) => (bytes.to_vec(), None),
        None => {
            if expected_version != 0 {
                return Err(PublishError::Other(
                    "a non-birth publication needs the existing trust anchor".to_string(),
                ));
            }
            let signed = author_root(
                config,
                1,
                now.checked_add(lifetimes.root())
                    .map_err(|error| PublishError::Other(error.to_string()))?,
            )
            .await?;
            (signed.buffer().clone(), Some(signed))
        }
    };

    // 2. Stage: the editor needs an on-disk root and real target files.
    let staging = tempfile::tempdir()
        .map_err(|error| PublishError::Other(format!("staging dir: {error}")))?;
    let root_path = staging.path().join("root.json");
    std::fs::write(&root_path, &root_bytes)
        .map_err(|error| PublishError::Other(format!("staging root.json: {error}")))?;
    let documents_dir = staging.path().join("documents");
    std::fs::create_dir_all(&documents_dir)
        .map_err(|error| PublishError::Other(error.to_string()))?;
    for (name, bytes) in &input.documents {
        let path = documents_dir.join(name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| PublishError::Other(error.to_string()))?;
        }
        std::fs::write(&path, bytes).map_err(|error| PublishError::Other(error.to_string()))?;
    }

    // 3. The manifest's artifact descriptors point at their binary targets
    //    (the concrete meaning of `retrieval_ref`), then the manifest itself
    //    becomes a target.
    let mut manifest = input.bundle_manifest;
    for artifact in &mut manifest.artifacts {
        artifact.retrieval_ref = Some(engine_target_name(&artifact.target.0));
    }
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)
        .map_err(|error| PublishError::Other(format!("serializing bundle manifest: {error}")))?;
    let manifest_path = staging.path().join(&input.claim.engine.manifest);
    std::fs::write(&manifest_path, &manifest_bytes)
        .map_err(|error| PublishError::Other(error.to_string()))?;

    // 4. Build the target inventory with claim metadata.
    let mut editor = RepositoryEditor::new(&root_path)
        .await
        .map_err(|error| PublishError::Other(format!("opening editor: {error}")))?;
    let claim_value = serde_json::to_value(&input.claim)
        .map_err(|error| PublishError::Other(format!("serializing claim: {error}")))?;
    let config_tree: std::collections::BTreeSet<&str> =
        input.config_tree.iter().map(String::as_str).collect();

    let mut planned: Vec<(String, PathBuf)> = Vec::new();
    for (name, _) in &input.documents {
        let path = documents_dir.join(name);
        let mut target = Target::from_path(&path)
            .await
            .map_err(|error| PublishError::Other(format!("hashing `{name}`: {error}")))?;
        if name == &input.claim.definition.root {
            target
                .custom
                .insert(CLAIM_KEY.to_string(), claim_value.clone());
        } else if config_tree.contains(name.as_str()) {
            target
                .custom
                .insert(CONFIG_KEY.to_string(), serde_json::json!({}));
        } else {
            target.custom.insert(
                COMPANION_KEY.to_string(),
                serde_json::json!({ "format": input.claim.format.as_str() }),
            );
        }
        let hash = hex::encode(&target.hashes.sha256);
        editor
            .add_target(
                TargetName::new(name.clone())
                    .map_err(|error| PublishError::Other(error.to_string()))?,
                target,
            )
            .map_err(|error| PublishError::Other(error.to_string()))?;
        planned.push((format!("targets/{hash}.{name}"), path));
    }
    {
        let mut target = Target::from_path(&manifest_path)
            .await
            .map_err(|error| PublishError::Other(format!("hashing manifest: {error}")))?;
        target
            .custom
            .insert(CONFIG_KEY.to_string(), serde_json::json!({}));
        let hash = hex::encode(&target.hashes.sha256);
        editor
            .add_target(
                TargetName::new(input.claim.engine.manifest.clone())
                    .map_err(|error| PublishError::Other(error.to_string()))?,
                target,
            )
            .map_err(|error| PublishError::Other(error.to_string()))?;
        planned.push((
            format!("targets/{hash}.{}", input.claim.engine.manifest),
            manifest_path.clone(),
        ));
    }
    for (triple, path) in &input.bundle_artifacts {
        let name = engine_target_name(triple);
        let mut target = Target::from_path(path)
            .await
            .map_err(|error| PublishError::Other(format!("hashing `{name}`: {error}")))?;
        target.custom.insert(
            ENGINE_ARTIFACT_KEY.to_string(),
            serde_json::json!({ "target": triple }),
        );
        let hash = hex::encode(&target.hashes.sha256);
        editor
            .add_target(
                TargetName::new(name.clone())
                    .map_err(|error| PublishError::Other(error.to_string()))?,
                target,
            )
            .map_err(|error| PublishError::Other(error.to_string()))?;
        planned.push((format!("targets/{hash}.{name}"), path.clone()));
    }

    // 5. One publication version across the online roles.
    let non_zero = NonZeroU64::new(version)
        .ok_or_else(|| PublishError::Other("publication version is zero".to_string()))?;
    let metadata_expires = now
        .checked_add(lifetimes.metadata())
        .map_err(|error| PublishError::Other(error.to_string()))?;
    let timestamp_expires = now
        .checked_add(lifetimes.timestamp())
        .map_err(|error| PublishError::Other(error.to_string()))?;
    editor
        .targets_version(non_zero)
        .map_err(|error| PublishError::Other(error.to_string()))?
        .targets_expires(metadata_expires)
        .map_err(|error| PublishError::Other(error.to_string()))?
        .snapshot_version(non_zero)
        .snapshot_expires(metadata_expires)
        .timestamp_version(non_zero)
        .timestamp_expires(timestamp_expires);

    let signed = editor
        .sign(&config.keys.online_sources())
        .await
        .map_err(|error| PublishError::Signing {
            role: "online roles",
            error: error.to_string(),
        })?;
    let metadata_dir = staging.path().join("metadata");
    std::fs::create_dir_all(&metadata_dir)
        .map_err(|error| PublishError::Other(error.to_string()))?;
    if let Some(root) = &authored_root {
        root.write(&metadata_dir, true)
            .await
            .map_err(|error| PublishError::Other(format!("writing versioned root: {error}")))?;
    }
    signed
        .write(&metadata_dir)
        .await
        .map_err(|error| PublishError::Other(format!("writing metadata: {error}")))?;

    // 6. Upload under the write policy: create-only objects first, mutable
    //    heads last.
    let writer = writer_for(&config.locator, s3).map_err(io_to_publish)?;
    let mut outcomes = Vec::new();
    let mut timestamp_bytes: Option<Vec<u8>> = None;
    let mut entries: Vec<_> = std::fs::read_dir(&metadata_dir)
        .map_err(|error| PublishError::Other(error.to_string()))?
        .collect::<Result<_, _>>()
        .map_err(|error| PublishError::Other(error.to_string()))?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let name = entry.file_name().to_string_lossy().into_owned();
        let bytes =
            std::fs::read(entry.path()).map_err(|error| PublishError::Other(error.to_string()))?;
        if name == "timestamp.json" {
            timestamp_bytes = Some(bytes);
            continue;
        }
        let key = format!("metadata/{name}");
        let outcome = writer
            .put_create_only(&key, WriteSource::Bytes(&bytes))
            .await
            .map_err(|error| conflict_or_publish(error, version))?;
        outcomes.push((key, outcome));
    }
    for (key, path) in &planned {
        let outcome = writer
            .put_create_only(key, WriteSource::File(path))
            .await
            .map_err(|error| conflict_or_publish(error, version))?;
        outcomes.push((key.clone(), outcome));
    }
    let timestamp_bytes = timestamp_bytes
        .ok_or_else(|| PublishError::Other("editor produced no timestamp.json".to_string()))?;
    writer
        .put_mutable_head("metadata/timestamp.json", &timestamp_bytes)
        .await
        .map_err(io_to_publish)?;
    // The convenience trust-anchor copy for operator pinning; never fetched
    // by the verification chain.
    writer
        .put_mutable_head("metadata/root.json", &root_bytes)
        .await
        .map_err(io_to_publish)?;
    outcomes.push((
        "metadata/timestamp.json".to_string(),
        UploadOutcome::Created,
    ));

    Ok(PublicationReceipt {
        version,
        trusted_root: root_bytes,
        outcomes,
    })
}

/// Author a root at `version` with the configured keys — the online-rotation
/// shape (same keys, advanced version) for the P9 root-walk property.
#[cfg(test)]
pub(crate) async fn author_root_for_tests(
    config: &RepositoryConfig,
    version: u64,
) -> Result<SignedRole<Root>, PublishError> {
    let now = Timestamp::now();
    let expires = now
        .checked_add(config.lifetimes.root())
        .map_err(|error| PublishError::Other(error.to_string()))?;
    author_root(config, version, expires).await
}

/// The engine binary target name for one target triple.
pub fn engine_target_name(triple: &str) -> String {
    format!("tkp-{triple}")
}

fn io_to_publish(error: WriteError) -> PublishError {
    PublishError::Other(error.to_string())
}

fn conflict_or_publish(error: WriteError, attempted: u64) -> PublishError {
    match error {
        WriteError::Conflict { key } => {
            // Metadata collisions are version races; target collisions with
            // differing bytes are conflicting publications of "identical"
            // content — both refuse, distinctly named.
            if key.starts_with("metadata/") {
                PublishError::Conflict { key, attempted }
            } else {
                PublishError::ImmutableDivergence { key }
            }
        }
        other => io_to_publish(other),
    }
}

/// Author and sign root.json `version` with the configured role keys.
/// Thresholds are 1 (single-operator repositories; raising is
/// configuration); `consistent_snapshot` is on — the property that makes
/// the layout create-only.
async fn author_root(
    config: &RepositoryConfig,
    version: u64,
    expires: Timestamp,
) -> Result<SignedRole<Root>, PublishError> {
    let mut root = Root {
        spec_version: "1.0.0".to_string(),
        consistent_snapshot: true,
        version: NonZeroU64::new(version)
            .ok_or_else(|| PublishError::Other("root version is zero".to_string()))?,
        expires,
        keys: HashMap::new(),
        roles: HashMap::new(),
        _extra: HashMap::new(),
    };
    let one = NonZeroU64::new(1).expect("one is non-zero");
    for role in [
        RoleType::Root,
        RoleType::Targets,
        RoleType::Snapshot,
        RoleType::Timestamp,
    ] {
        root.roles.insert(
            role,
            RoleKeys {
                keyids: Vec::new(),
                threshold: one,
                _extra: HashMap::new(),
            },
        );
    }
    for (role, source) in [
        (RoleType::Root, config.keys.root.source()),
        (RoleType::Targets, config.keys.targets.source()),
        (RoleType::Snapshot, config.keys.snapshot.source()),
        (RoleType::Timestamp, config.keys.timestamp.source()),
    ] {
        let key = tough::key_source::KeySource::as_sign(&source)
            .await
            .map_err(|error| PublishError::Signing {
                role: "root authoring",
                error: error.to_string(),
            })?
            .tuf_key();
        let key_id = key.key_id().map_err(|error| PublishError::Signing {
            role: "root authoring",
            error: error.to_string(),
        })?;
        // One key may back several roles (e.g. one KMS key for the online
        // set); the keys map is id-keyed so re-insertion is benign.
        root.keys.insert(key_id.clone(), key);
        root.roles
            .get_mut(&role)
            .expect("role inserted above")
            .keyids
            .push(key_id);
    }
    let rng = SystemRandom::new();
    let holder = KeyHolder::Root(root.clone());
    let signing = [config.keys.root.source().boxed()];
    SignedRole::new(root, &holder, &signing, &rng)
        .await
        .map_err(|error| PublishError::Signing {
            role: "root",
            error: error.to_string(),
        })
}
