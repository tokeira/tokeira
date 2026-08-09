//! Publishing a definition set as a signed TUF repository.
//!
//! The mapping under test:
//!
//! - **Targets** are the set's files under their sibling file names — the
//!   root document (`deployment.tkd`) and each part (`platform.tkd`, …) —
//!   exactly the names `DirectoryPartSources` would serve from disk.
//! - **The set claim** rides as `custom` metadata on the root document's
//!   target: format, root name, served-part order, and the `sha256-set-v1`
//!   identity. TUF authenticates per-target bytes; the claim carries the two
//!   things TUF does not define — evaluation order and the product's
//!   composite identity — inside the signed targets role, so they enjoy the
//!   same signatures.
//! - **Consistent snapshots** are enabled: every metadata version and target
//!   is written under a digest/version-prefixed name, so a repository in S3
//!   is create-only for everything except `timestamp.json` — the same
//!   write policy the platform-source-set spec demands of its blob store.
//!
//! Role separation: root (offline trust anchor) signs only `root.json`;
//! targets/snapshot/timestamp are the online publishing roles. Each role's
//! key arrives as a `tough::KeySource`, which is the whole KMS story — see
//! `kms.rs`.

use std::{
    collections::HashMap,
    num::NonZeroU64,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Context as _;
use async_trait::async_trait;
use aws_lc_rs::rand::SystemRandom;
use jiff::{Span, Timestamp};
use tough::{
    TargetName,
    editor::{
        RepositoryEditor,
        signed::{PathExists, SignedRole},
    },
    key_source::KeySource,
    schema::{KeyHolder, RoleKeys, RoleType, Root, Target},
    sign::Sign,
};

use crate::set::{DefinitionSet, SetIdentity};

/// The `custom` key carrying the set claim on the root document's target.
pub const SET_CLAIM_KEY: &str = "tokeira:definition-set";
/// The `custom` key marking part targets.
pub const PART_CLAIM_KEY: &str = "tokeira:definition-part";

/// The signed set claim: what the publisher asserts about the set beyond
/// per-target bytes. Serialized into targets metadata, so it is covered by
/// the targets role's signature (and, transitively, snapshot + timestamp).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SetClaim {
    /// Definition format id, e.g. `tkd`.
    pub format: String,
    /// The root document's target name.
    pub root: String,
    /// Bare part names in first-request (served) order.
    pub parts: Vec<String>,
    /// The product's composite configuration identity.
    pub identity: SetIdentity,
}

/// A shareable `KeySource`: `tough` APIs take `Box<dyn KeySource>` slices by
/// reference in some places and by value in others, and `KeySource` is not
/// clonable — an `Arc` shim lets one configured source (file or KMS) be
/// handed to every call site that needs it.
#[derive(Debug, Clone)]
pub struct SharedKeySource(Arc<dyn KeySource>);

impl SharedKeySource {
    /// Wrap a concrete key source.
    pub fn new(source: impl KeySource + 'static) -> Self {
        Self(Arc::new(source))
    }

    /// Wrap an already-boxed key source.
    pub fn from_box(source: Box<dyn KeySource>) -> Self {
        Self(Arc::from(source))
    }

    /// Mint a boxed clone for APIs that want ownership.
    pub fn boxed(&self) -> Box<dyn KeySource> {
        Box::new(self.clone())
    }
}

#[async_trait]
impl KeySource for SharedKeySource {
    async fn as_sign(
        &self,
    ) -> Result<Box<dyn Sign>, Box<dyn std::error::Error + Send + Sync + 'static>> {
        self.0.as_sign().await
    }

    async fn write(
        &self,
        value: &str,
        key_id_hex: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        self.0.write(value, key_id_hex).await
    }
}

/// One `KeySource` per TUF role. Any entry can be a local file or KMS; the
/// publisher does not know the difference.
#[derive(Debug, Clone)]
pub struct RoleSources {
    /// Offline trust anchor; used only when authoring/rotating root.json.
    pub root: SharedKeySource,
    /// Online role: signs the target inventory.
    pub targets: SharedKeySource,
    /// Online role: signs the metadata snapshot.
    pub snapshot: SharedKeySource,
    /// Online role: signs the freshness statement.
    pub timestamp: SharedKeySource,
}

impl RoleSources {
    /// The online-role key boxes handed to `RepositoryEditor::sign`.
    pub fn online(&self) -> Vec<Box<dyn KeySource>> {
        vec![
            self.targets.boxed(),
            self.snapshot.boxed(),
            self.timestamp.boxed(),
        ]
    }
}

/// Versions and lifetimes for one publication.
#[derive(Debug, Clone)]
pub struct PublishOptions {
    /// root.json version (rotates rarely).
    pub root_version: u64,
    /// Version shared by targets/snapshot/timestamp for this publication.
    pub repo_version: u64,
    /// root.json lifetime.
    pub root_lifetime: Span,
    /// targets.json / snapshot.json lifetime.
    pub metadata_lifetime: Span,
    /// timestamp.json lifetime — the freshness window; the shortest by design.
    pub timestamp_lifetime: Span,
}

impl Default for PublishOptions {
    fn default() -> Self {
        Self {
            root_version: 1,
            repo_version: 1,
            // Timestamp arithmetic in jiff takes absolute units only
            // (hours or smaller): 365d / 90d / 14d.
            root_lifetime: Span::new().hours(365 * 24),
            metadata_lifetime: Span::new().hours(90 * 24),
            timestamp_lifetime: Span::new().hours(14 * 24),
        }
    }
}

/// A published repository on disk, laid out for direct upload.
#[derive(Debug)]
pub struct PublishedRepo {
    /// `<out>/metadata` — versioned metadata plus mutable `timestamp.json`.
    pub metadata_dir: PathBuf,
    /// `<out>/targets` — digest-prefixed target files.
    pub targets_dir: PathBuf,
    /// The trusted root bytes a consumer pins out-of-band.
    pub trusted_root: Vec<u8>,
    /// The published set claim (for assertions and reporting).
    pub claim: SetClaim,
}

fn nonzero(v: u64) -> anyhow::Result<NonZeroU64> {
    NonZeroU64::new(v).context("version must be non-zero")
}

/// Author and sign `root.json` version `version` with the four role keys.
///
/// Thresholds are all 1 in the spike; production would raise the root
/// threshold. `consistent_snapshot` is on — that is the property that makes
/// the S3 layout create-only.
pub async fn author_root(
    keys: &RoleSources,
    version: u64,
    expires: Timestamp,
) -> anyhow::Result<SignedRole<Root>> {
    let mut root = Root {
        spec_version: "1.0.0".to_owned(),
        consistent_snapshot: true,
        version: nonzero(version)?,
        expires,
        keys: HashMap::new(),
        roles: HashMap::new(),
        _extra: HashMap::new(),
    };
    let one = nonzero(1)?;
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
        (RoleType::Root, &keys.root),
        (RoleType::Targets, &keys.targets),
        (RoleType::Snapshot, &keys.snapshot),
        (RoleType::Timestamp, &keys.timestamp),
    ] {
        let key = source
            .as_sign()
            .await
            .map_err(|e| anyhow::anyhow!("loading {role:?} key: {e}"))?
            .tuf_key();
        let key_id = key.key_id().context("computing key id")?;
        // The same key may back several roles (e.g. one KMS key for all
        // online roles); the keys map is id-keyed so re-insertion is benign.
        root.keys.insert(key_id.clone(), key);
        root.roles
            .get_mut(&role)
            .expect("role inserted above")
            .keyids
            .push(key_id);
    }

    // Root signs itself: the KeyHolder is the very root being authored.
    let rng = SystemRandom::new();
    let holder = KeyHolder::Root(root.clone());
    let signing = [keys.root.boxed()];
    let signed = SignedRole::new(root, &holder, &signing, &rng)
        .await
        .context("signing root.json")?;
    Ok(signed)
}

/// Publish `set` as a complete signed repository under `out`.
///
/// Layout produced:
///
/// ```text
/// out/metadata/{N.root.json, root.json, N.targets.json, N.snapshot.json, timestamp.json}
/// out/targets/<sha256>.<file-name>
/// ```
pub async fn publish_set(
    set: &DefinitionSet,
    keys: &RoleSources,
    out: &Path,
    opts: &PublishOptions,
) -> anyhow::Result<PublishedRepo> {
    let now = Timestamp::now();
    let metadata_dir = out.join("metadata");
    let targets_dir = out.join("targets");
    std::fs::create_dir_all(&metadata_dir)?;
    std::fs::create_dir_all(&targets_dir)?;

    // 1. Author the trust anchor.
    let signed_root = author_root(
        keys,
        opts.root_version,
        now.checked_add(opts.root_lifetime)?,
    )
    .await?;
    signed_root
        .write(&metadata_dir, true)
        .await
        .context("writing versioned root.json")?;
    // The un-versioned copy is the out-of-band trust anchor a consumer pins;
    // it is not part of the TUF fetch protocol.
    let trusted_root = signed_root.buffer().clone();
    let root_path = metadata_dir.join("root.json");
    std::fs::write(&root_path, &trusted_root)?;

    // 2. Stage the set as plain files (the editor hashes real files).
    let staging = out.join("staging");
    std::fs::create_dir_all(&staging)?;
    std::fs::write(staging.join(&set.root_name), &set.root)?;
    for (name, bytes) in &set.parts {
        std::fs::write(staging.join(set.part_file_name(name)), bytes)?;
    }

    // 3. Build the target inventory with the set claim in custom metadata.
    let claim = SetClaim {
        format: set.format.clone(),
        root: set.root_name.clone(),
        parts: set.parts.iter().map(|(n, _)| n.clone()).collect(),
        identity: set.identity(),
    };

    let mut editor = RepositoryEditor::new(&root_path)
        .await
        .context("opening editor against authored root")?;

    let mut root_target = Target::from_path(staging.join(&set.root_name))
        .await
        .context("hashing root document")?;
    root_target.custom.insert(
        SET_CLAIM_KEY.to_owned(),
        serde_json::to_value(&claim).context("serializing set claim")?,
    );
    editor.add_target(
        TargetName::new(set.root_name.clone()).context("root target name")?,
        root_target,
    )?;

    for (name, _) in &set.parts {
        let file_name = set.part_file_name(name);
        let mut target = Target::from_path(staging.join(&file_name))
            .await
            .with_context(|| format!("hashing part `{name}`"))?;
        target.custom.insert(
            PART_CLAIM_KEY.to_owned(),
            serde_json::json!({ "format": set.format }),
        );
        editor.add_target(
            TargetName::new(file_name).context("part target name")?,
            target,
        )?;
    }

    // 4. Versions and lifetimes for the online roles.
    let repo_version = nonzero(opts.repo_version)?;
    let metadata_expires = now.checked_add(opts.metadata_lifetime)?;
    editor
        .targets_version(repo_version)?
        .targets_expires(metadata_expires)?
        .snapshot_version(repo_version)
        .snapshot_expires(metadata_expires)
        .timestamp_version(repo_version)
        .timestamp_expires(now.checked_add(opts.timestamp_lifetime)?);

    // 5. Sign with the online role keys and lay the repository out.
    let signed = editor
        .sign(&keys.online())
        .await
        .context("signing repository")?;
    signed
        .write(&metadata_dir)
        .await
        .context("writing metadata")?;
    signed
        .copy_targets(&staging, &targets_dir, PathExists::Skip)
        .await
        .context("copying digest-named targets")?;

    Ok(PublishedRepo {
        metadata_dir,
        targets_dir,
        trusted_root,
        claim,
    })
}
