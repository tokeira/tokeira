//! Provisioner source-closure resolution.
//!
//! The closure is the union of workspace crates reachable from one or more
//! provisioner roots, resolved from `cargo metadata` so it cannot rot as
//! crates move — plus the workspace files that shape every build
//! (`Cargo.toml`, `Cargo.lock`, `rust-toolchain.toml`, `.cargo/config.toml`).
//! Legacy callers still resolve one platform seed; statically assembled
//! provisioners resolve exactly the shell, selected platform, and selected
//! frontend roots.
//!
//! Resolution runs with **all features enabled**, which over-approximates the
//! closure (an optional dep never built may still join it). Over-approximation
//! is the safe direction for an identity input: it can only re-key more often
//! than strictly necessary, never reuse wrong bytes. Feature-exact narrowing
//! via the unit graph is a later refinement.
//!
//! The same walk yields the **locked third-party set** reachable from the
//! seed — the `lock_closure` digest input of `EngineIdentity`: name, version,
//! source, and the `Cargo.lock` checksum, canonically serialized by
//! [`ProvisionerClosure::canonical_lock_bytes`].

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
    /// set a hermetic build tests.
    pub crate_names: Vec<String>,
    /// Workspace-relative build-shaping files that exist in this workspace.
    pub workspace_files: Vec<PathBuf>,
    /// Workspace-relative directories of **non-member path dependencies** the
    /// scoped workspace must carry, sorted: every `[patch.*]` target (patches
    /// resolve eagerly, so a dangling patch path fails resolution outright)
    /// and every reachable source-less non-member package (a vendored crate a
    /// closure member consumes). These stage and freeze like crate dirs but
    /// are never workspace members.
    pub path_dependency_dirs: Vec<PathBuf>,
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
    #[error("root package '{0}' is not in the workspace dependency graph")]
    SeedNotFound(String),
    /// A composition root without dependency roots cannot produce an engine.
    #[error("at least one source-closure root package is required")]
    NoRoots,
    #[error("cargo metadata returned no resolve graph")]
    NoResolveGraph,
    #[error("failed to read {path}: {source}")]
    ReadFile {
        path: String,
        source: std::io::Error,
    },
    #[error("failed to parse Cargo.lock: {0}")]
    LockParse(#[from] toml::de::Error),
    #[error("failed to parse the workspace manifest: {0}")]
    ManifestParse(toml::de::Error),
    /// A path dependency the closure must freeze lies outside the workspace
    /// — it cannot travel with the frozen source.
    #[error("path dependency `{package}` at {path} lies outside the workspace")]
    PathDependencyOutsideWorkspace { package: String, path: String },
}

impl ProvisionerClosure {
    /// Every snapshot path: the crate directories, the workspace build
    /// files, and the non-member path-dependency directories, in one sorted
    /// list for [`SnapshotRequest::closure_paths`](crate::SnapshotRequest).
    pub fn closure_paths(&self) -> Vec<PathBuf> {
        let mut paths: Vec<PathBuf> = self
            .crate_dirs
            .iter()
            .chain(self.workspace_files.iter())
            .chain(self.path_dependency_dirs.iter())
            .cloned()
            .collect();
        paths.sort();
        paths
    }

    /// Canonical serialization of the locked set — the `lock_closure` digest
    /// input: one `name<SP>version<SP>source<SP>checksum` line per
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
/// `tokeira-compose-deployment`) in the workspace at `workspace_root`.
pub fn resolve_source_closure(
    workspace_root: &Path,
    seed_package: &str,
) -> Result<ProvisionerClosure, ClosureError> {
    resolve_source_closure_for_packages(workspace_root, &[seed_package])
}

/// Resolve the union source closure reachable from the named workspace roots.
///
/// Root order and duplicates do not affect the result. Every named root must
/// be a workspace member; silently dropping one could build a provisioner
/// whose recorded closure omits executable source.
pub fn resolve_source_closure_for_packages(
    workspace_root: &Path,
    root_packages: &[&str],
) -> Result<ProvisionerClosure, ClosureError> {
    if root_packages.is_empty() {
        return Err(ClosureError::NoRoots);
    }
    let metadata = MetadataCommand::new()
        .manifest_path(workspace_root.join("Cargo.toml"))
        .features(CargoOpt::AllFeatures)
        .exec()?;
    let resolve = metadata
        .resolve
        .as_ref()
        .ok_or(ClosureError::NoResolveGraph)?;

    // BFS over the resolve graph from every admitted root.
    let nodes: BTreeMap<&cargo_metadata::PackageId, &cargo_metadata::Node> =
        resolve.nodes.iter().map(|n| (&n.id, n)).collect();
    let mut reachable: BTreeSet<&cargo_metadata::PackageId> = BTreeSet::new();
    let mut queue = VecDeque::new();
    for root in root_packages.iter().copied().collect::<BTreeSet<_>>() {
        let root_id = metadata
            .packages
            .iter()
            .find(|package| {
                package.name.as_str() == root && metadata.workspace_members.contains(&package.id)
            })
            .map(|package| &package.id)
            .ok_or_else(|| ClosureError::SeedNotFound(root.to_string()))?;
        queue.push_back(root_id);
    }
    while let Some(id) = queue.pop_front() {
        if !reachable.insert(id) {
            continue;
        }
        if let Some(node) = nodes.get(id) {
            queue.extend(node.dependencies.iter());
        }
    }

    // Partition: workspace members → crate dirs; the rest → the locked set,
    // with source-less (path-dependency) packages also recorded as
    // directories the scoped workspace must carry.
    let members: BTreeSet<&cargo_metadata::PackageId> = metadata.workspace_members.iter().collect();
    let checksums = lock_checksums(workspace_root)?;
    // `AsRef::<str>` spelled out: `typed_path` (via `tough`) adds a
    // blanket `AsRef<Utf8Path<_>> for Cow<'_, str>` that makes a bare
    // `as_ref()` ambiguous.
    let root_prefix: &str = &workspace_root.to_string_lossy();
    let workspace_relative_dir =
        |package: &cargo_metadata::Package| -> Result<PathBuf, ClosureError> {
            let dir = package
                .manifest_path
                .parent()
                .expect("a manifest path always has a parent directory");
            let relative = dir.strip_prefix(root_prefix).map_err(|_| {
                ClosureError::PathDependencyOutsideWorkspace {
                    package: package.name.as_str().to_owned(),
                    path: dir.as_str().to_owned(),
                }
            })?;
            Ok(PathBuf::from(relative.as_str().trim_start_matches('/')))
        };
    let mut crate_dirs = Vec::new();
    let mut crate_names = Vec::new();
    let mut path_dependency_dirs: BTreeSet<PathBuf> = BTreeSet::new();
    let mut locked = Vec::new();
    for package in &metadata.packages {
        if !reachable.contains(&package.id) {
            continue;
        }
        if members.contains(&package.id) {
            crate_dirs.push(workspace_relative_dir(package)?);
            crate_names.push(package.name.as_str().to_owned());
        } else {
            if package.source.is_none() {
                // A reachable non-member without a registry/git source is a
                // path dependency (a vendored crate): its directory must
                // stage and freeze with the closure or the scoped workspace
                // cannot resolve.
                path_dependency_dirs.insert(workspace_relative_dir(package)?);
            }
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
    // `[patch.*]` targets resolve eagerly — a dangling patch path fails the
    // scoped workspace outright, reachable or not — so every patch path
    // stages unconditionally.
    for patch_dir in manifest_patch_paths(workspace_root)? {
        path_dependency_dirs.insert(patch_dir);
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
        path_dependency_dirs: path_dependency_dirs.into_iter().collect(),
        locked,
    })
}

/// Workspace-relative `path` targets of every `[patch.*]` entry in the root
/// manifest. Patch paths are authored workspace-relative; an absolute or
/// escaping path is refused — it could not be frozen with the source.
fn manifest_patch_paths(workspace_root: &Path) -> Result<Vec<PathBuf>, ClosureError> {
    let path = workspace_root.join("Cargo.toml");
    let content = std::fs::read_to_string(&path).map_err(|source| ClosureError::ReadFile {
        path: path.display().to_string(),
        source,
    })?;
    let manifest: toml::Value = content.parse().map_err(ClosureError::ManifestParse)?;
    let mut paths = Vec::new();
    let Some(patches) = manifest.get("patch").and_then(toml::Value::as_table) else {
        return Ok(paths);
    };
    for registry in patches.values() {
        let Some(entries) = registry.as_table() else {
            continue;
        };
        for (name, entry) in entries {
            let Some(patch_path) = entry.get("path").and_then(toml::Value::as_str) else {
                continue;
            };
            let relative = Path::new(patch_path);
            if relative.is_absolute() || patch_path.contains("..") {
                return Err(ClosureError::PathDependencyOutsideWorkspace {
                    package: name.clone(),
                    path: patch_path.to_owned(),
                });
            }
            paths.push(relative.to_path_buf());
        }
    }
    Ok(paths)
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
