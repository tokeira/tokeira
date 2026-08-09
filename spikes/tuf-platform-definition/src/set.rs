//! Mirrored product shapes: the definition set, the part-resolver seam, and
//! the `sha256-set-v1` configuration identity.
//!
//! The spike is standalone by contract, so the three shapes it must stay
//! faithful to are copied here with citations rather than imported:
//!
//! - `SourceResolver` / `PartResolveError` mirror
//!   `crates/tokeira-platform/src/definition.rs:64` — the seam a verified
//!   TUF repository must be able to stand behind.
//! - `ConfigurationIdentity::{compute, compute_set}` mirror
//!   `definition.rs:237` / `definition.rs:255` byte-for-byte (same domain
//!   strings, same length-prefixed layout, same lowercase-hex encoding), so
//!   an identity computed from TUF-fetched bytes is comparable with one the
//!   product recorded at publish time.
//! - `DefinitionSet` carries what `evaluate_definition` derives live: the
//!   root document plus the served parts in first-request order. The spike
//!   models `.tkd` request order as `mod` declaration order, which is how
//!   `tokeira-tkd/src/parts.rs` loads them.

use std::sync::Arc;

use sha2::{Digest, Sha256};

/// Mirror of `tokeira_platform::definition::SourceResolver` (definition.rs:64).
///
/// `name` is a bare identifier — no extension, no separators; the resolver
/// owns the `<name>.<ext>` mapping.
pub trait SourceResolver {
    /// Serve the bytes of the named part, or explain why it cannot be served.
    fn resolve(&self, name: &str) -> Result<Arc<[u8]>, PartResolveError>;
}

/// Mirror of `tokeira_platform::definition::PartResolveError` (definition.rs:70).
#[derive(Debug, thiserror::Error)]
#[error("part `{name}` cannot be served: {reason}")]
pub struct PartResolveError {
    /// The part name that was requested.
    pub name: String,
    /// Why the resolver refused it.
    pub reason: String,
}

/// A complete platform definition set: the root document plus every part the
/// evaluation would serve, in first-request order.
///
/// This is the publisher-side view. The product derives the same information
/// live (`RecordingResolver` inside `evaluate_definition`); the spike derives
/// it from the root's `mod` declarations because it deliberately cannot run
/// the real frontends.
#[derive(Debug, Clone)]
pub struct DefinitionSet {
    /// Definition format id, e.g. `tkd`.
    pub format: String,
    /// The root document's file name, e.g. `deployment.tkd`.
    pub root_name: String,
    /// The root document bytes.
    pub root: Vec<u8>,
    /// `(bare part name, bytes)` in first-request order.
    pub parts: Vec<(String, Arc<[u8]>)>,
}

impl DefinitionSet {
    /// The set's configuration identity: `sha256-set-v1` when parts were
    /// served, `sha256-v1` for a single-document set — the same branch the
    /// product takes in `evaluate_definition` (definition.rs:339).
    pub fn identity(&self) -> SetIdentity {
        if self.parts.is_empty() {
            SetIdentity::compute_single(&self.format, &self.root)
        } else {
            SetIdentity::compute_set(&self.format, &self.root, &self.parts)
        }
    }

    /// File name a part is stored under next to the root, e.g. `platform.tkd`
    /// — the `DirectoryPartSources` mapping (definition.rs:99).
    pub fn part_file_name(&self, part: &str) -> String {
        format!("{part}.{}", self.format)
    }
}

/// Mirror of `ConfigurationIdentity` (definition.rs:220): algorithm label
/// plus lowercase-hex SHA-256 digest.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SetIdentity {
    /// `sha256-v1` or `sha256-set-v1`.
    pub algorithm: String,
    /// Lowercase SHA-256 digest.
    pub digest: String,
}

impl SetIdentity {
    /// Byte-exact mirror of `ConfigurationIdentity::compute` (definition.rs:237).
    pub fn compute_single(format: &str, bytes: &[u8]) -> Self {
        let mut digest = Sha256::new();
        digest.update(b"tokeira.configuration.v1\0");
        digest.update((format.len() as u64).to_be_bytes());
        digest.update(format.as_bytes());
        digest.update((bytes.len() as u64).to_be_bytes());
        digest.update(bytes);
        Self {
            algorithm: "sha256-v1".to_owned(),
            digest: hex::encode(digest.finalize()),
        }
    }

    /// Byte-exact mirror of `ConfigurationIdentity::compute_set`
    /// (definition.rs:255): domain-separated, length-prefixed, order-sensitive.
    pub fn compute_set(format: &str, root: &[u8], parts: &[(String, Arc<[u8]>)]) -> Self {
        let mut digest = Sha256::new();
        digest.update(b"tokeira.configuration.set.v1\0");
        digest.update((format.len() as u64).to_be_bytes());
        digest.update(format.as_bytes());
        digest.update((root.len() as u64).to_be_bytes());
        digest.update(root);
        for (name, bytes) in parts {
            digest.update((name.len() as u64).to_be_bytes());
            digest.update(name.as_bytes());
            digest.update((bytes.len() as u64).to_be_bytes());
            digest.update(bytes.as_ref());
        }
        Self {
            algorithm: "sha256-set-v1".to_owned(),
            digest: hex::encode(digest.finalize()),
        }
    }
}

/// Load a definition set from a directory, deriving part order from the
/// root's `mod <name>;` declarations.
///
/// This models the shipped `.tkd` behaviour — parts live as sibling
/// `<name>.<ext>` files and are requested in declaration order
/// (`tokeira-tkd/src/parts.rs`). It is a publisher-side stand-in for running
/// the real frontend, which the spike cannot do without a crate dependency.
pub fn load_set_from_dir(
    dir: &std::path::Path,
    root_name: &str,
    format: &str,
) -> anyhow::Result<DefinitionSet> {
    let root = std::fs::read(dir.join(root_name))?;
    let mut parts = Vec::new();
    for name in declared_parts(&root) {
        let path = dir.join(format!("{name}.{format}"));
        let bytes = std::fs::read(&path)
            .map_err(|e| anyhow::anyhow!("part `{name}` at {}: {e}", path.display()))?;
        parts.push((name, Arc::from(bytes.into_boxed_slice())));
    }
    Ok(DefinitionSet {
        format: format.to_owned(),
        root_name: root_name.to_owned(),
        root,
        parts,
    })
}

/// Bare part names from `mod <name>;` declarations, in declaration order.
fn declared_parts(root: &[u8]) -> Vec<String> {
    let text = String::from_utf8_lossy(root);
    let mut names = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("mod ")
            && let Some(name) = rest.strip_suffix(';')
        {
            let name = name.trim();
            if !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                names.push(name.to_owned());
            }
        }
    }
    names
}

/// A `SourceResolver` over an in-memory set of verified parts — what a
/// TUF-backed part source looks like behind the product seam.
#[derive(Debug)]
pub struct VerifiedPartSources {
    parts: std::collections::HashMap<String, Arc<[u8]>>,
}

impl VerifiedPartSources {
    /// Build from `(bare name, bytes)` pairs that already passed TUF target
    /// verification.
    pub fn new(parts: impl IntoIterator<Item = (String, Arc<[u8]>)>) -> Self {
        Self {
            parts: parts.into_iter().collect(),
        }
    }
}

impl SourceResolver for VerifiedPartSources {
    fn resolve(&self, name: &str) -> Result<Arc<[u8]>, PartResolveError> {
        self.parts
            .get(name)
            .cloned()
            .ok_or_else(|| PartResolveError {
                name: name.to_owned(),
                reason: "not a target of the verified definition set".to_owned(),
            })
    }
}
