//! Provisioner source-closure resolution (task 17.1's "the closure").
//!
//! The closure is the set of workspace crates **reachable from a provisioner
//! seed package** (a platform crate carrying a `tkp-*` bin target), resolved
//! from `cargo metadata` so it cannot rot as crates move — plus the workspace
//! files that shape every build (`Cargo.toml`, `Cargo.lock`,
//! `rust-toolchain.toml`, `.cargo/config.toml`).
//!
//! Resolution runs with **all features enabled**, which over-approximates the
//! closure (an optional dep never built may still join it). Over-approximation
//! is the safe direction for an identity input: it can only re-key more often
//! than strictly necessary, never reuse wrong bytes. Feature-exact narrowing
//! via the unit graph is a later refinement.
//!
//! The same walk yields the **locked third-party set** reachable from the
//! seed — the `lock_closure` digest input of `EngineIdentity` (task 16.1):
//! name, version, source, and the `Cargo.lock` checksum, canonically
//! serialized by [`ProvisionerClosure::canonical_lock_bytes`].

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    path::{Path, PathBuf},
};

use cargo_metadata::{CargoOpt, MetadataCommand};

/// Workspace files outside any crate directory that are inputs to every
/// build of the closure.
const WORKSPACE_BUILD_FILES: &[&str] = &[
    "Cargo.toml",
    "Cargo.lock",
    "rust-toolchain.toml",
    ".cargo/config.toml",
];

/// The resolved provisioner closure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProvisionerClosure {
    /// Workspace-relative crate directories reachable from the seed, sorted.
    /// These (plus [`workspace_files`](Self::workspace_files)) are the
    /// snapshot's closure paths.
    pub crate_dirs: Vec<PathBuf>,
    /// Package names of the reachable workspace members, sorted — the `-p`
    /// set a hermetic build tests (task 18.1).
    pub crate_names: Vec<String>,
    /// Workspace-relative build-shaping files that exist in this workspace.
    pub workspace_files: Vec<PathBuf>,
    /// The locked third-party dependencies reachable from the seed, sorted by
    /// (name, version).
    pub locked: Vec<LockedDependency>,
}

/// One locked third-party dependency reachable from the seed.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct LockedDependency {
    pub name: String,
    pub version: String,
    /// The dependency source (registry/git url), when not a path dep.
    pub source: Option<String>,
    /// The `Cargo.lock` checksum, when the lock records one.
    pub checksum: Option<String>,
}

/// Failure resolving the closure.
#[derive(Debug, thiserror::Error)]
pub enum ClosureError {
    #[error("cargo metadata failed: {0}")]
    Metadata(#[from] cargo_metadata::Error),
    #[error("seed package '{0}' is not in the workspace dependency graph")]
    SeedNotFound(String),
    #[error("cargo metadata returned no resolve graph")]
    NoResolveGraph,
    #[error("failed to read {path}: {source}")]
    ReadFile {
        path: String,
        source: std::io::Error,
    },
    #[error("failed to parse Cargo.lock: {0}")]
    LockParse(#[from] toml::de::Error),
}

impl ProvisionerClosure {
    /// Every snapshot path: the crate directories plus the workspace build
    /// files, in one sorted list for
    /// [`SnapshotRequest::closure_paths`](crate::SnapshotRequest).
    pub fn closure_paths(&self) -> Vec<PathBuf> {
        let mut paths: Vec<PathBuf> = self
            .crate_dirs
            .iter()
            .chain(self.workspace_files.iter())
            .cloned()
            .collect();
        paths.sort();
        paths
    }

    /// Canonical serialization of the locked set — the `lock_closure` digest
    /// input (task 16.1): one `name<SP>version<SP>source<SP>checksum` line per
    /// dependency, sorted, `-` for an absent field. Deterministic for a given
    /// resolved set.
    pub fn canonical_lock_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"tokeira-lock-closure/v1\n");
        for dep in &self.locked {
            buf.extend_from_slice(dep.name.as_bytes());
            buf.push(b' ');
            buf.extend_from_slice(dep.version.as_bytes());
            buf.push(b' ');
            buf.extend_from_slice(dep.source.as_deref().unwrap_or("-").as_bytes());
            buf.push(b' ');
            buf.extend_from_slice(dep.checksum.as_deref().unwrap_or("-").as_bytes());
            buf.push(b'\n');
        }
        buf
    }
}

/// Resolve the source closure reachable from `seed_package` (e.g.
/// `tokeira-compose-syn`) in the workspace at `workspace_root`.
pub fn resolve_source_closure(
    workspace_root: &Path,
    seed_package: &str,
) -> Result<ProvisionerClosure, ClosureError> {
    let metadata = MetadataCommand::new()
        .manifest_path(workspace_root.join("Cargo.toml"))
        .features(CargoOpt::AllFeatures)
        .exec()?;
    let resolve = metadata
        .resolve
        .as_ref()
        .ok_or(ClosureError::NoResolveGraph)?;

    // BFS over the resolve graph from the seed.
    let nodes: BTreeMap<&cargo_metadata::PackageId, &cargo_metadata::Node> =
        resolve.nodes.iter().map(|n| (&n.id, n)).collect();
    let seed_id = metadata
        .packages
        .iter()
        .find(|p| p.name.as_str() == seed_package && metadata.workspace_members.contains(&p.id))
        .map(|p| &p.id)
        .ok_or_else(|| ClosureError::SeedNotFound(seed_package.to_string()))?;
    let mut reachable: BTreeSet<&cargo_metadata::PackageId> = BTreeSet::new();
    let mut queue = VecDeque::from([seed_id]);
    while let Some(id) = queue.pop_front() {
        if !reachable.insert(id) {
            continue;
        }
        if let Some(node) = nodes.get(id) {
            queue.extend(node.dependencies.iter());
        }
    }

    // Partition: workspace members → crate dirs; the rest → the locked set.
    let members: BTreeSet<&cargo_metadata::PackageId> = metadata.workspace_members.iter().collect();
    let checksums = lock_checksums(workspace_root)?;
    let mut crate_dirs = Vec::new();
    let mut crate_names = Vec::new();
    let mut locked = Vec::new();
    for package in &metadata.packages {
        if !reachable.contains(&package.id) {
            continue;
        }
        if members.contains(&package.id) {
            let dir = package
                .manifest_path
                .parent()
                .expect("a manifest path always has a parent directory");
            let relative = dir
                .strip_prefix(workspace_root.to_string_lossy().as_ref())
                .unwrap_or(dir);
            crate_dirs.push(PathBuf::from(relative.as_str().trim_start_matches('/')));
            crate_names.push(package.name.as_str().to_owned());
        } else {
            let version = package.version.to_string();
            let checksum = checksums
                .get(&(package.name.as_str().to_owned(), version.clone()))
                .cloned();
            locked.push(LockedDependency {
                name: package.name.as_str().to_owned(),
                version,
                source: package.source.as_ref().map(|s| s.repr.clone()),
                checksum,
            });
        }
    }
    crate_dirs.sort();
    crate_names.sort();
    locked.sort();

    let workspace_files = WORKSPACE_BUILD_FILES
        .iter()
        .map(PathBuf::from)
        .filter(|f| workspace_root.join(f).exists())
        .collect();

    Ok(ProvisionerClosure {
        crate_dirs,
        crate_names,
        workspace_files,
        locked,
    })
}

/// `(name, version) → checksum` from the workspace `Cargo.lock`.
fn lock_checksums(
    workspace_root: &Path,
) -> Result<BTreeMap<(String, String), String>, ClosureError> {
    #[derive(serde::Deserialize)]
    struct Lockfile {
        #[serde(default)]
        package: Vec<LockPackage>,
    }
    #[derive(serde::Deserialize)]
    struct LockPackage {
        name: String,
        version: String,
        #[serde(default)]
        checksum: Option<String>,
    }

    let path = workspace_root.join("Cargo.lock");
    let content = std::fs::read_to_string(&path).map_err(|source| ClosureError::ReadFile {
        path: path.display().to_string(),
        source,
    })?;
    let lockfile: Lockfile = toml::from_str(&content)?;
    Ok(lockfile
        .package
        .into_iter()
        .filter_map(|p| p.checksum.map(|c| ((p.name, p.version), c)))
        .collect())
}
