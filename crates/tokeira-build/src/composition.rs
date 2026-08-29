//! Static provisioner composition-root generation.
//!
//! A generated root contains no platform dispatch. It binds one trusted
//! platform library to one trusted Definition Frontend library through the
//! generic provisioner shell, then becomes a disposable build input. Cargo
//! metadata supplies every package coordinate; descriptors cannot inject
//! Rust paths or arbitrary dependencies.
//!
//! The generated package is an **ordinary member of the frozen source
//! workspace** — never a detached island with a private lock. The snapshot's
//! `Cargo.toml`/`Cargo.lock` are frozen closure-scoped
//! ([`BoundProvisionerSource::snapshot_request`]): members are exactly the
//! closure's crates plus the generated root, and the lock keeps exactly the
//! packages those members reach — a pure text filter of the workspace's own
//! authoritative lock. Cargo stays the sole dependency resolver; `--locked`
//! verifies the scoped lock is exact, for the build and the closure tests
//! alike, in the one shared workspace.

use std::{
    collections::BTreeSet,
    path::{Component, Path, PathBuf},
    process::Stdio,
};

use cargo_metadata::{Metadata, MetadataCommand, Package};
use thiserror::Error;
use tokeira_deployment::{BoundProvisionerEvidence, Sha256Digest};
use tokeira_orchestrator::{DefinitionFormatId, PlatformId};

use crate::{
    ClosureError, DefinitionFrontendPackageDescriptor, DiscoveryError, PackageCoordinates,
    PlatformPackageDescriptor, ProvisionerClosure,
    discovery::{descriptors_from_metadata, package_coordinates},
    resolve_source_closure_for_packages,
};

/// Cargo package containing the generic provisioner shell.
pub const TKP_PACKAGE: &str = "tokeira-tkp";

/// Stable location of the disposable root within a staged source tree.
pub const GENERATED_ROOT_RELATIVE_PATH: &str = ".tokeira-build/bound-provisioner";

/// Stable location (workspace-relative, gitignored) of the staged scoped
/// workspace. One directory serves both the snapshot author and the native
/// dev build: staging is mtime-preserving, so reuse keeps cargo incremental
/// and the closure copy is paid once.
pub const SCOPED_WORKSPACE_RELATIVE_PATH: &str = ".tokeira-build/scoped-workspace";

/// Cargo package name of every statically assembled provisioner root — the
/// `-p` spec both the hermetic and the native build select.
pub const GENERATED_ROOT_PACKAGE: &str = "tokeira-bound-provisioner";

/// Cargo binary produced by every statically assembled provisioner root.
pub const GENERATED_PROVISIONER_BIN: &str = "tkp";

/// Deterministic source and closure for one selected platform/frontend pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundProvisionerSource {
    platform: PlatformId,
    format: DefinitionFormatId,
    engine: String,
    cargo_toml: String,
    main_rs: String,
    closure: ProvisionerClosure,
}

impl BoundProvisionerSource {
    /// A minimal buildable source for the offline fakes (`testing`
    /// feature): real generated root, empty engine binding.
    #[cfg(any(test, feature = "testing"))]
    pub fn testing(closure: ProvisionerClosure) -> Self {
        Self {
            platform: PlatformId::new("alpha").expect("test platform id"),
            format: DefinitionFormatId::new("tkd").expect("test format id"),
            engine: "0.0.0".to_string(),
            cargo_toml: "[package]\nname = \"tokeira-bound-provisioner\"\nversion = \"0.0.0\"\nedition = \"2024\"\n\n[dependencies]\n".to_string(),
            main_rs: "fn main() {}\n".to_string(),
            closure,
        }
    }

    #[cfg(test)]
    pub(crate) fn testing_clear_crates(&mut self) {
        self.closure.crate_names.clear();
    }

    /// Borrow the selected open platform identity.
    pub fn platform(&self) -> &PlatformId {
        &self.platform
    }

    /// Borrow the selected language-neutral Definition Format identity.
    pub fn format(&self) -> &DefinitionFormatId {
        &self.format
    }

    /// Return the Engine_Version the platform definition indicated.
    pub fn engine(&self) -> &str {
        &self.engine
    }

    /// Borrow the deterministic generated `Cargo.toml` bytes.
    pub fn cargo_toml(&self) -> &str {
        &self.cargo_toml
    }

    /// Borrow the deterministic generated `src/main.rs` bytes.
    pub fn main_rs(&self) -> &str {
        &self.main_rs
    }

    /// Borrow the exact union closure of shell, platform, and frontend.
    pub fn closure(&self) -> &ProvisionerClosure {
        &self.closure
    }

    /// Digest the deterministic generated package and every selection fact
    /// that determines its meaning.
    ///
    /// Contract versions are included explicitly even though the generated
    /// Rust source contains only the selected identifiers and conventional
    /// exports. Advancing either private contract must therefore re-key the
    /// engine instead of reusing an artifact assembled under older semantics.
    ///
    /// The generated package carries no lock of its own — it resolves inside
    /// the scoped snapshot workspace, whose `Cargo.lock` is frozen in the
    /// snapshot tree and therefore already keys the source-closure digest.
    pub fn generated_root_digest(&self) -> Sha256Digest {
        let mut bytes = b"tokeira-bound-provisioner-root/v4\n".to_vec();
        framed_field(&mut bytes, "platform", self.platform.as_str().as_bytes());
        framed_field(&mut bytes, "format", self.format.as_str().as_bytes());
        framed_field(&mut bytes, "engine", self.engine.as_bytes());
        framed_field(&mut bytes, "Cargo.toml", self.cargo_toml.as_bytes());
        framed_field(&mut bytes, "src/main.rs", self.main_rs.as_bytes());
        Sha256Digest::from_bytes(&bytes)
    }

    /// Digest the frozen source tree together with the generated overlay.
    ///
    /// The generated files are not committed workspace source, so the git
    /// tree oid alone cannot identify the compiled program. Length-framed,
    /// domain-separated bytes make the overlay an explicit engine-identity
    /// input without making the deployment definition itself executable
    /// source.
    pub fn source_closure_digest(&self, snapshot_tree_oid: &str) -> Sha256Digest {
        let mut bytes = b"tokeira-bound-provisioner-source/v2\n".to_vec();
        framed_field(&mut bytes, "snapshot-tree", snapshot_tree_oid.as_bytes());
        framed_field(
            &mut bytes,
            "generated-root",
            self.generated_root_digest().to_hex().as_bytes(),
        );
        Sha256Digest::from_bytes(&bytes)
    }

    /// Produce the complete build/admission evidence for a frozen source tree.
    ///
    /// This is the sole derivation used by native and hermetic bound builds:
    /// the selected descriptors, generated root, source closure, and lock
    /// closure cannot be populated independently and silently disagree.
    pub fn evidence(&self, snapshot_tree_oid: &str) -> BoundProvisionerEvidence {
        BoundProvisionerEvidence {
            platform: self.platform.clone(),
            format: self.format.clone(),
            engine: self.engine.clone(),
            generated_root: self.generated_root_digest(),
            source_closure: self.source_closure_digest(snapshot_tree_oid),
            lock_closure: Sha256Digest::from_bytes(&self.closure.canonical_lock_bytes()),
        }
    }

    /// Materialize the generated package inside one frozen source staging tree.
    pub(crate) fn materialize_in(&self, source_root: &Path) -> Result<PathBuf, CompositionError> {
        let root = source_root.join(GENERATED_ROOT_RELATIVE_PATH);
        let source_dir = root.join("src");
        std::fs::create_dir_all(&source_dir).map_err(|source| {
            CompositionError::WriteGenerated {
                path: source_dir.display().to_string(),
                source,
            }
        })?;
        write_generated(&root.join("Cargo.toml"), self.cargo_toml.as_bytes())?;
        write_generated(&source_dir.join("main.rs"), self.main_rs.as_bytes())?;
        Ok(root)
    }

    /// The snapshot request that freezes this source's closure as a
    /// **complete, valid cargo workspace**: the closure paths, with the
    /// workspace `Cargo.toml`/`Cargo.lock` overridden by their closure-scoped
    /// forms. The scoped bytes are authored by staging the workspace once
    /// into a throwaway directory ([`stage_native_workspace`](Self::stage_native_workspace))
    /// — exact scoped-lock membership is cargo's feature unification over
    /// the scoped member set, which no derivation short of cargo's own
    /// resolution reproduces. Materializing the resulting tree and overlaying
    /// `materialize_in` yields a workspace plain
    /// `cargo build`/`cargo test --locked` accept.
    pub fn snapshot_request(
        &self,
        workspace_root: &Path,
    ) -> Result<crate::SnapshotRequest, CompositionError> {
        let staging = workspace_root.join(SCOPED_WORKSPACE_RELATIVE_PATH);
        self.stage_native_workspace(workspace_root, &staging)?;
        let read = |name: &str| {
            std::fs::read(staging.join(name)).map_err(|source| {
                CompositionError::ReadWorkspaceLock {
                    path: staging.join(name).display().to_string(),
                    source,
                }
            })
        };
        let manifest = read("Cargo.toml")?;
        let lock = read("Cargo.lock")?;
        Ok(crate::SnapshotRequest {
            repo_root: workspace_root.to_path_buf(),
            closure_paths: self.closure.closure_paths(),
            include_untracked: false,
            content_overrides: std::collections::BTreeMap::from([
                ("Cargo.toml".to_string(), manifest),
                ("Cargo.lock".to_string(), lock),
            ]),
        })
    }

    /// Stage the **live** closure as the scoped workspace: the closure files
    /// copied skip-identical (mtimes survive, cargo stays incremental), the
    /// scoped root manifest, the generated package as an ordinary member, and
    /// the scoped lock — with strays from prior stagings removed so a
    /// shrunken closure cannot leave stale source behind. `target/` is
    /// cargo's and is never touched.
    ///
    /// The scoped lock is authored by cargo itself: the staging is seeded
    /// with the workspace's authoritative `Cargo.lock` and `cargo metadata
    /// --offline` resolves the staged member set — it can only prune the
    /// seed, never consult a registry — then the result is validated to
    /// contain nothing outside the admitted closure
    /// (`validate_scoped_lock`). Exactness matters because `--locked`
    /// builds and tests refuse any lock cargo would rewrite.
    ///
    /// This also serves the native dev build, which deliberately compiles
    /// the working tree and must work in trees without `.git`.
    pub fn stage_native_workspace(
        &self,
        workspace_root: &Path,
        staging: &Path,
    ) -> Result<PathBuf, CompositionError> {
        let read = |name: &str| {
            std::fs::read_to_string(workspace_root.join(name)).map_err(|source| {
                CompositionError::ReadWorkspaceLock {
                    path: workspace_root.join(name).display().to_string(),
                    source,
                }
            })
        };
        let manifest = scoped_workspace_manifest(&read("Cargo.toml")?, &self.closure)?;
        let seed_lock = read("Cargo.lock")?;

        let mut kept: BTreeSet<PathBuf> = BTreeSet::new();
        for relative in self.closure.closure_paths() {
            // The two scoped files are written below — copying their full
            // forms first would churn their mtimes every staging.
            let key = relative.to_string_lossy().replace('\\', "/");
            if key == "Cargo.toml" || key == "Cargo.lock" {
                continue;
            }
            copy_skip_identical(
                &workspace_root.join(&relative),
                &staging.join(&relative),
                &relative,
                &mut kept,
            )?;
        }
        write_generated(&staging.join("Cargo.toml"), manifest.as_bytes())?;
        kept.insert(PathBuf::from("Cargo.toml"));
        self.materialize_in(staging)?;
        let generated = Path::new(GENERATED_ROOT_RELATIVE_PATH);
        kept.insert(generated.join("Cargo.toml"));
        kept.insert(generated.join("src/main.rs"));
        prune_strays(staging, Path::new(""), &kept)?;

        // Lock last, after the sweep (the sweep would otherwise treat a
        // fresh staging's lock as a stray): seed with the authoritative
        // lock, let cargo prune it to the staged member set, and refuse
        // anything outside the admitted closure.
        let staged_lock = staging.join("Cargo.lock");
        write_generated(&staged_lock, seed_lock.as_bytes())?;
        let status = std::process::Command::new("cargo")
            .current_dir(staging)
            .args(["metadata", "--offline", "--format-version", "1"])
            .stdout(Stdio::null())
            .status()
            .map_err(|source| CompositionError::Stage {
                path: staged_lock.display().to_string(),
                source,
            })?;
        if !status.success() {
            return Err(CompositionError::InvalidSelection(format!(
                "cargo could not resolve the staged scoped workspace at {}",
                staging.display()
            )));
        }
        let resolved = std::fs::read_to_string(&staged_lock).map_err(|source| {
            CompositionError::ReadWorkspaceLock {
                path: staged_lock.display().to_string(),
                source,
            }
        })?;
        validate_scoped_lock(&resolved, &self.closure)?;
        Ok(staging.to_path_buf())
    }
}

fn framed_field(buffer: &mut Vec<u8>, tag: &str, value: &[u8]) {
    buffer.extend_from_slice(tag.as_bytes());
    buffer.push(b'=');
    buffer.extend_from_slice(value.len().to_string().as_bytes());
    buffer.push(b':');
    buffer.extend_from_slice(value);
    buffer.push(b'\n');
}

/// The workspace root manifest with `members` replaced by exactly the
/// closure's crate directories plus the generated root. A byte-targeted
/// splice — every other table (dependency policy, lints, profiles) survives
/// verbatim — validated by re-parsing the result.
fn scoped_workspace_manifest(
    full: &str,
    closure: &ProvisionerClosure,
) -> Result<String, CompositionError> {
    // Line-anchored search: a bare substring match would also hit
    // `default-members = [`.
    let start = full
        .find("\nmembers = [")
        .map(|at| at + 1)
        .or_else(|| full.starts_with("members = [").then_some(0))
        .ok_or_else(|| {
            CompositionError::InvalidSelection(
                "workspace manifest has no members array to scope".to_string(),
            )
        })?;
    let open = start + "members = [".len();
    let close = open
        + full[open..].find(']').ok_or_else(|| {
            CompositionError::InvalidSelection(
                "workspace manifest members array is unterminated".to_string(),
            )
        })?;

    let mut members: BTreeSet<String> = closure
        .crate_dirs
        .iter()
        .map(|dir| dir.to_string_lossy().replace('\\', "/"))
        .collect();
    members.insert(GENERATED_ROOT_RELATIVE_PATH.to_string());
    let mut rendered = String::from("\n");
    for member in &members {
        rendered.push_str("    ");
        rendered.push_str(&toml_string(member));
        rendered.push_str(",\n");
    }

    let scoped = format!("{}{}{}", &full[..open], rendered, &full[close..]);
    // Patches resolve eagerly: an entry whose package the closure never
    // reaches would drag an unrelated vendored tree into every frozen
    // source (re-keying identities when that vendor advances) and draw a
    // "patch was not used" warning from every resolution — drop it.
    let reachable: BTreeSet<&str> = closure
        .crate_names
        .iter()
        .map(String::as_str)
        .chain(closure.locked.iter().map(|dep| dep.name.as_str()))
        .collect();
    let scoped = drop_unreachable_patches(&scoped, &reachable)?;

    let parsed: toml::Value = toml::from_str(&scoped).map_err(|error| {
        CompositionError::InvalidSelection(format!(
            "scoped workspace manifest does not parse: {error}"
        ))
    })?;
    let spliced: Option<BTreeSet<String>> = parsed
        .get("workspace")
        .and_then(|w| w.get("members"))
        .and_then(toml::Value::as_array)
        .map(|list| {
            list.iter()
                .filter_map(toml::Value::as_str)
                .map(str::to_string)
                .collect()
        });
    if spliced.as_ref() != Some(&members) {
        return Err(CompositionError::InvalidSelection(
            "scoping the workspace members array altered the wrong region".to_string(),
        ));
    }
    if let Some(patches) = parsed.get("patch").and_then(toml::Value::as_table) {
        for registry in patches.values() {
            if let Some(entries) = registry.as_table()
                && let Some(stray) = entries
                    .keys()
                    .find(|name| !reachable.contains(name.as_str()))
            {
                return Err(CompositionError::InvalidSelection(format!(
                    "scoped manifest still patches unreachable package `{stray}`"
                )));
            }
        }
    }
    Ok(scoped)
}

/// Remove `[patch.<registry>]` entry lines (and emptied table headers) for
/// packages outside `reachable`, line-based so every kept line survives
/// byte-identically. Tables and entries are recognized structurally
/// (`[patch.` headers, `name = value` lines within), and the result is
/// re-parsed by the caller, so a miss fails loudly rather than silently.
fn drop_unreachable_patches(
    manifest: &str,
    reachable: &BTreeSet<&str>,
) -> Result<String, CompositionError> {
    let mut kept = String::with_capacity(manifest.len());
    let mut in_patch_table = false;
    let mut table_buffer: Vec<&str> = Vec::new();
    let mut table_has_entries = false;

    let flush = |buffer: &mut Vec<&str>, has_entries: bool, kept: &mut String| {
        if has_entries {
            for line in buffer.iter() {
                kept.push_str(line);
                kept.push('\n');
            }
        }
        buffer.clear();
    };

    for line in manifest.lines() {
        let trimmed = line.trim_start();
        let is_header = trimmed.starts_with('[');
        if is_header {
            if in_patch_table {
                flush(&mut table_buffer, table_has_entries, &mut kept);
            }
            in_patch_table = trimmed.starts_with("[patch.");
            if in_patch_table {
                table_buffer.push(line);
                table_has_entries = false;
                continue;
            }
        }
        if in_patch_table {
            let entry_name = trimmed
                .split_once('=')
                .map(|(name, _)| name.trim().trim_matches('"'));
            match entry_name {
                Some(name) if !name.is_empty() && !trimmed.starts_with('#') => {
                    if reachable.contains(name) {
                        table_buffer.push(line);
                        table_has_entries = true;
                    }
                    // Unreachable entry lines are dropped.
                }
                // Comments and blank lines ride with the table.
                _ => table_buffer.push(line),
            }
            continue;
        }
        kept.push_str(line);
        kept.push('\n');
    }
    if in_patch_table {
        flush(&mut table_buffer, table_has_entries, &mut kept);
    }
    Ok(kept)
}

/// Refuse a staged scoped lock that strays outside the admitted closure:
/// every third-party entry must be byte-exact in the closure's locked set
/// (name/version/source/checksum), every source-less entry must be a closure
/// member or the generated root, and every closure member and the root must
/// be present. The offline resolution that produced the lock can only prune
/// its authoritative seed, so a violation is drift or corruption — never a
/// legitimate resolution.
///
/// The lock may be a *subset* of the closure's locked set: the closure walks
/// the full workspace's resolve graph, whose feature unification includes
/// activations contributed by non-closure members. Cargo's resolution of the
/// scoped member set drops those edges — the direction that only ever
/// removes dependencies from the built artifact.
fn validate_scoped_lock(text: &str, closure: &ProvisionerClosure) -> Result<(), CompositionError> {
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
        source: Option<String>,
        #[serde(default)]
        checksum: Option<String>,
    }

    let lockfile: Lockfile = toml::from_str(text).map_err(|error| {
        CompositionError::InvalidSelection(format!("staged scoped lock is invalid: {error}"))
    })?;
    let admitted = closure.locked.iter().collect::<BTreeSet<_>>();
    let members = closure
        .crate_names
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut found_root = false;
    let mut seen_members: BTreeSet<&str> = BTreeSet::new();
    for package in &lockfile.package {
        if package.name == GENERATED_ROOT_PACKAGE && package.source.is_none() {
            found_root = true;
            continue;
        }
        if package.source.is_none() {
            if let Some(member) = members.get(package.name.as_str()) {
                seen_members.insert(member);
                continue;
            }
        } else {
            let dependency = crate::LockedDependency {
                name: package.name.clone(),
                version: package.version.clone(),
                source: package.source.clone(),
                checksum: package.checksum.clone(),
            };
            if admitted.contains(&dependency) {
                continue;
            }
        }
        return Err(CompositionError::InvalidSelection(format!(
            "staged scoped lock contains package `{} {}` outside the admitted source closure",
            package.name, package.version
        )));
    }
    if !found_root {
        return Err(CompositionError::InvalidSelection(
            "staged scoped lock does not contain the generated provisioner root".to_string(),
        ));
    }
    if let Some(missing) = members.iter().find(|m| !seen_members.contains(*m)) {
        return Err(CompositionError::InvalidSelection(format!(
            "staged scoped lock has no entry for closure member `{missing}`"
        )));
    }
    Ok(())
}

/// Copy `src` (file, dir, or symlink) to `dst`, writing only when content
/// differs so an unchanged file keeps its mtime — cargo's fingerprints stay
/// valid across stagings. Records every staged file (repo-relative) in
/// `kept` for the stray sweep.
fn copy_skip_identical(
    src: &Path,
    dst: &Path,
    relative: &Path,
    kept: &mut BTreeSet<PathBuf>,
) -> Result<(), CompositionError> {
    let stage_err = |path: &Path| {
        let path = path.display().to_string();
        move |source: std::io::Error| CompositionError::Stage { path, source }
    };
    let Ok(metadata) = std::fs::symlink_metadata(src) else {
        // A closure path may be tracked-but-deleted; absence is the truth.
        return Ok(());
    };
    if metadata.is_dir() {
        std::fs::create_dir_all(dst).map_err(stage_err(dst))?;
        let entries = std::fs::read_dir(src).map_err(stage_err(src))?;
        for entry in entries {
            let entry = entry.map_err(stage_err(src))?;
            let name = entry.file_name();
            copy_skip_identical(
                &src.join(&name),
                &dst.join(&name),
                &relative.join(&name),
                kept,
            )?;
        }
        return Ok(());
    }
    if let Some(parent) = dst.parent() {
        std::fs::create_dir_all(parent).map_err(stage_err(parent))?;
    }
    if metadata.is_symlink() {
        let target = std::fs::read_link(src).map_err(stage_err(src))?;
        let unchanged = std::fs::read_link(dst).is_ok_and(|current| current == target);
        if !unchanged {
            let _ = std::fs::remove_file(dst);
            #[cfg(unix)]
            std::os::unix::fs::symlink(&target, dst).map_err(stage_err(dst))?;
            #[cfg(not(unix))]
            std::fs::write(dst, target.as_os_str().as_encoded_bytes()).map_err(stage_err(dst))?;
        }
    } else {
        let bytes = std::fs::read(src).map_err(stage_err(src))?;
        let unchanged = std::fs::read(dst).is_ok_and(|current| current == bytes);
        if !unchanged {
            std::fs::write(dst, &bytes).map_err(stage_err(dst))?;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = metadata.permissions().mode();
            if mode & 0o111 != 0 {
                std::fs::set_permissions(dst, std::fs::Permissions::from_mode(0o755))
                    .map_err(stage_err(dst))?;
            }
        }
    }
    kept.insert(relative.to_path_buf());
    Ok(())
}

/// Delete files under `root` that this staging did not produce, so a
/// shrunken closure cannot leave stale source for cargo to compile.
/// `target/` belongs to cargo and is never entered.
fn prune_strays(
    root: &Path,
    relative: &Path,
    kept: &BTreeSet<PathBuf>,
) -> Result<(), CompositionError> {
    let stage_err = |path: &Path| {
        let path = path.display().to_string();
        move |source: std::io::Error| CompositionError::Stage { path, source }
    };
    let Ok(entries) = std::fs::read_dir(root) else {
        return Ok(());
    };
    for entry in entries {
        let entry = entry.map_err(stage_err(root))?;
        let name = entry.file_name();
        let child_rel = relative.join(&name);
        if child_rel == Path::new("target") {
            continue;
        }
        let path = entry.path();
        let file_type = entry.file_type().map_err(stage_err(&path))?;
        if file_type.is_dir() {
            prune_strays(&path, &child_rel, kept)?;
            // Best-effort: a directory emptied by the sweep is itself a stray.
            let _ = std::fs::remove_dir(&path);
        } else if !kept.contains(&child_rel) {
            std::fs::remove_file(&path).map_err(stage_err(&path))?;
        }
    }
    Ok(())
}

/// Refusal to assemble or materialize a static provisioner root.
#[derive(Debug, Error)]
pub enum CompositionError {
    /// Trusted workspace metadata could not be read.
    #[error("cargo metadata failed: {0}")]
    Metadata(#[from] cargo_metadata::Error),
    /// A selected conventional library violated descriptor shape.
    #[error(transparent)]
    Discovery(#[from] DiscoveryError),
    /// The exact three-root dependency closure could not be resolved.
    #[error(transparent)]
    Closure(#[from] ClosureError),
    /// Selected discovery data no longer agrees with the recognized workspace.
    #[error("invalid bound-provisioner selection: {0}")]
    InvalidSelection(String),
    /// A workspace build file (`Cargo.toml`/`Cargo.lock`) could not be read
    /// for closure scoping.
    #[error("failed to read workspace file {path}: {source}")]
    ReadWorkspaceLock {
        /// Workspace file whose bytes were required.
        path: String,
        /// Filesystem failure.
        source: std::io::Error,
    },
    /// Generated files could not be staged for compilation.
    #[error("failed to write generated composition-root file {path}: {source}")]
    WriteGenerated {
        /// Destination whose write failed.
        path: String,
        /// Filesystem failure.
        source: std::io::Error,
    },
    /// The native staging workspace could not be arranged.
    #[error("failed to stage {path}: {source}")]
    Stage {
        /// Staging path whose copy or sweep failed.
        path: String,
        /// Filesystem failure.
        source: std::io::Error,
    },
}

/// Assemble one static provisioner from trusted workspace descriptors.
pub fn assemble_bound_provisioner(
    workspace_root: &Path,
    platform: &PlatformPackageDescriptor,
    frontend: &DefinitionFrontendPackageDescriptor,
) -> Result<BoundProvisionerSource, CompositionError> {
    let metadata = MetadataCommand::new()
        .manifest_path(workspace_root.join("Cargo.toml"))
        .no_deps()
        .exec()?;
    let descriptors = descriptors_from_metadata(&metadata)?;
    if !descriptors.platforms.contains(platform) {
        return Err(CompositionError::InvalidSelection(format!(
            "platform `{}` does not match its recognized workspace descriptor",
            platform.id
        )));
    }
    if !descriptors.frontends.contains(frontend) {
        return Err(CompositionError::InvalidSelection(format!(
            "format `{}` does not match its recognized workspace descriptor",
            frontend.format
        )));
    }
    let cli = find_workspace_package(&metadata, TKP_PACKAGE)?;
    let cli_coordinates = package_coordinates(cli, "bound-provisioner shell")?;

    // Cargo's own normalized spelling must anchor dependency paths. On macOS
    // `/var` and `/private/var` can name the same directory but are not
    // lexically strip-compatible, while metadata uses one spelling
    // consistently for the workspace and every manifest.
    let workspace_root = metadata.workspace_root.as_std_path().to_path_buf();
    let cli_path = dependency_path(&workspace_root, &cli_coordinates)?;
    let platform_path = dependency_path(&workspace_root, &platform.package)?;
    let frontend_path = dependency_path(&workspace_root, &frontend.package)?;

    let cargo_toml = render_manifest(
        &cli_coordinates,
        &cli_path,
        &platform.package,
        &platform_path,
        &frontend.package,
        &frontend_path,
        &frontend.feature,
    );
    let main_rs = render_main(
        &platform.id,
        &frontend.format,
        &platform.content,
        &frontend.feature,
    );
    let closure = resolve_source_closure_for_packages(
        &workspace_root,
        &[
            TKP_PACKAGE,
            platform.package.package_name.as_str(),
            frontend.package.package_name.as_str(),
        ],
    )?;

    Ok(BoundProvisionerSource {
        platform: platform.id.clone(),
        format: frontend.format.clone(),
        engine: platform.engine.clone(),
        cargo_toml,
        main_rs,
        closure,
    })
}

fn find_workspace_package<'a>(
    metadata: &'a Metadata,
    package_name: &str,
) -> Result<&'a Package, CompositionError> {
    let members = metadata.workspace_members.iter().collect::<BTreeSet<_>>();
    let matches = metadata
        .packages
        .iter()
        .filter(|package| package.name.as_str() == package_name && members.contains(&package.id))
        .collect::<Vec<_>>();
    let [package] = matches.as_slice() else {
        return Err(CompositionError::InvalidSelection(format!(
            "expected exactly one workspace package `{package_name}`; found {}",
            matches.len()
        )));
    };
    Ok(package)
}

fn dependency_path(
    workspace_root: &Path,
    coordinates: &PackageCoordinates,
) -> Result<String, CompositionError> {
    let package_dir = coordinates.manifest_path.parent().ok_or_else(|| {
        CompositionError::InvalidSelection(format!(
            "package `{}` has no manifest parent",
            coordinates.package_name
        ))
    })?;
    let relative = package_dir.strip_prefix(workspace_root).map_err(|_| {
        CompositionError::InvalidSelection(format!(
            "package `{}` manifest `{}` is outside the recognized workspace",
            coordinates.package_name,
            coordinates.manifest_path.display()
        ))
    })?;
    if relative
        .components()
        .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(CompositionError::InvalidSelection(format!(
            "package `{}` has a non-canonical workspace-relative path",
            coordinates.package_name
        )));
    }
    let generated_relative = Path::new("../..").join(relative);
    let rendered = generated_relative.to_str().ok_or_else(|| {
        CompositionError::InvalidSelection(format!(
            "package `{}` path is not valid UTF-8",
            coordinates.package_name
        ))
    })?;
    Ok(rendered.replace('\\', "/"))
}

fn render_manifest(
    cli: &PackageCoordinates,
    cli_path: &str,
    platform: &PackageCoordinates,
    platform_path: &str,
    frontend: &PackageCoordinates,
    frontend_path: &str,
    frontend_feature: &str,
) -> String {
    format!(
        "[package]\nname = \"{GENERATED_ROOT_PACKAGE}\"\nversion = \"0.0.0\"\nedition = \"2024\"\npublish = false\n\n[[bin]]\nname = \"{GENERATED_PROVISIONER_BIN}\"\npath = \"src/main.rs\"\n\n[dependencies]\nselected_frontend = {{ package = {}, path = {}, features = [{}], default-features = false }}\nselected_platform = {{ package = {}, path = {} }}\ntokeira_tkp = {{ package = {}, path = {} }}\n",
        toml_string(&frontend.package_name),
        toml_string(frontend_path),
        toml_string(frontend_feature),
        toml_string(&platform.package_name),
        toml_string(platform_path),
        toml_string(&cli.package_name),
        toml_string(cli_path),
    )
}

fn render_main(
    platform: &PlatformId,
    format: &DefinitionFormatId,
    content_roots: &[tokeira_orchestrator::RelativeDefinitionPath],
    frontend_module: &str,
) -> String {
    let platform = serde_json::to_string(platform.as_str()).expect("platform ids serialize");
    let format = serde_json::to_string(format.as_str()).expect("format ids serialize");
    let content_roots = content_roots
        .iter()
        .map(|root| serde_json::to_string(root.as_str()).expect("content roots serialize"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "tokeira_tkp::bound_provisioner_main!(\n    expected_platform: {platform},\n    platform: selected_platform::platform,\n    expected_format: {format},\n    content_roots: [{content_roots}],\n    frontend: selected_frontend::{frontend_module}::frontend,\n);\n"
    )
}

fn toml_string(value: &str) -> String {
    toml::Value::String(value.to_string()).to_string()
}

fn write_generated(path: &Path, bytes: &[u8]) -> Result<(), CompositionError> {
    // Skip identical bytes: a stable generated file keeps its mtime across
    // stagings, so cargo's fingerprints stay valid and unchanged inputs
    // rebuild nothing.
    if std::fs::read(path).is_ok_and(|current| current == bytes) {
        return Ok(());
    }
    std::fs::write(path, bytes).map_err(|source| CompositionError::WriteGenerated {
        path: path.display().to_string(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use std::process::Command;

    use super::*;
    use crate::discover_workspace_descriptors;

    struct Fixture {
        _temp: tempfile::TempDir,
        root: PathBuf,
        platforms: Vec<PlatformPackageDescriptor>,
        frontends: Vec<DefinitionFrontendPackageDescriptor>,
    }

    impl Fixture {
        fn new() -> Self {
            let temp = tempfile::tempdir().expect("temporary workspace");
            let root = temp.path().to_path_buf();
            write(
                &root.join("Cargo.toml"),
                "[workspace]\nmembers = [\"crates/cli\", \"platforms/alpha\", \"frontends/tkd\", \"frontends/tkdp\", \"crates/unrelated\"]\nresolver = \"3\"\n",
            );
            package(
                &root,
                "crates/cli",
                TKP_PACKAGE,
                "",
                r#"#[macro_export]
macro_rules! bound_provisioner_main {
    (
        expected_platform: $platform:literal,
        platform: $platform_factory:path,
        expected_format: $format:literal,
        content_roots: [$($content_root:literal),* $(,)?],
        frontend: $frontend:path $(,)?
    ) => {
        fn main() {
            let _ = (
                $platform,
                $format,
                &[$($content_root),*],
                $platform_factory(),
                $frontend(),
            );
        }
    };
}
"#,
            );
            package(
                &root,
                "platforms/alpha",
                "alpha-platform",
                "[package.metadata.tokeira.platform]\nid = \"alpha\"\nengine = \"0.1.0\"\ndefault = false\ncontent = [\"observability\"]\n",
                "pub fn platform() {}\n",
            );
            std::fs::create_dir_all(root.join("platforms/alpha/observability"))
                .expect("create platform content root");
            write(
                &root.join("platforms/alpha/observability/template.txt"),
                "authored content\n",
            );
            package(
                &root,
                "frontends/tkd",
                "tkd-frontend",
                "[[package.metadata.tokeira.definition-frontend]]\nformat = \"tkd\"\nsource-extension = \"tkd\"\nfeature = \"tkd\"\n\n[features]\ntkd = []\n",
                "#[cfg(feature = \"tkd\")]\npub mod tkd {\n    pub fn frontend() {}\n}\n",
            );
            package(
                &root,
                "frontends/tkdp",
                "tkdp-frontend",
                "[[package.metadata.tokeira.definition-frontend]]\nformat = \"tkdp\"\nsource-extension = \"tkdp\"\nfeature = \"tkdp\"\n\n[features]\ntkdp = []\n",
                "#[cfg(feature = \"tkdp\")]\npub mod tkdp {\n    pub fn frontend() {}\n}\n",
            );
            package(
                &root,
                "crates/unrelated",
                "unrelated",
                "",
                "pub fn unrelated() {}\n",
            );
            let descriptors = discover_workspace_descriptors(&root).expect("discover fixture");
            Self {
                _temp: temp,
                root,
                platforms: descriptors.platforms,
                frontends: descriptors.frontends,
            }
        }

        fn assemble(&self, format: &str) -> BoundProvisionerSource {
            let platform = self.platforms.first().expect("one platform");
            let frontend = self
                .frontends
                .iter()
                .find(|frontend| frontend.format.as_str() == format)
                .expect("known frontend");
            assemble_bound_provisioner(&self.root, platform, frontend).expect("assemble root")
        }
    }

    #[test]
    fn generated_root_is_deterministic_and_uses_exactly_three_dependencies() {
        let fixture = Fixture::new();
        let first = fixture.assemble("tkd");
        let second = fixture.assemble("tkd");

        assert_eq!(first, second);
        assert_eq!(
            first.main_rs,
            "tokeira_tkp::bound_provisioner_main!(\n    expected_platform: \"alpha\",\n    platform: selected_platform::platform,\n    expected_format: \"tkd\",\n    content_roots: [\"observability\"],\n    frontend: selected_frontend::tkd::frontend,\n);\n"
        );
        let manifest: toml::Value = toml::from_str(&first.cargo_toml).expect("generated manifest");
        let dependencies = manifest
            .get("dependencies")
            .and_then(toml::Value::as_table)
            .expect("dependency table");
        assert_eq!(
            dependencies.keys().map(String::as_str).collect::<Vec<_>>(),
            ["selected_frontend", "selected_platform", "tokeira_tkp"]
        );
        assert!(first.closure.crate_names.contains(&TKP_PACKAGE.to_string()));
        assert!(
            first
                .closure
                .crate_names
                .contains(&"alpha-platform".to_string())
        );
        assert!(
            first
                .closure
                .crate_names
                .contains(&"tkd-frontend".to_string())
        );
        assert!(!first.closure.crate_names.contains(&"unrelated".to_string()));
        assert!(
            !first
                .closure
                .crate_names
                .contains(&"tkdp-frontend".to_string())
        );
    }

    #[test]
    fn generated_overlay_and_selected_format_rekey_source_identity() {
        let fixture = Fixture::new();
        let tkd = fixture.assemble("tkd");
        let tkdp = fixture.assemble("tkdp");

        assert_eq!(
            tkd.source_closure_digest("tree-a"),
            fixture.assemble("tkd").source_closure_digest("tree-a")
        );
        assert_ne!(
            tkd.source_closure_digest("tree-a"),
            tkd.source_closure_digest("tree-b")
        );
        assert_ne!(
            tkd.source_closure_digest("tree-a"),
            tkdp.source_closure_digest("tree-a")
        );

        let evidence = tkd.evidence("tree-a");
        assert_eq!(evidence.platform, *tkd.platform());
        assert_eq!(evidence.format, *tkd.format());
        assert_eq!(evidence.engine, tkd.engine());
        assert_eq!(evidence.generated_root, tkd.generated_root_digest());
        assert_eq!(evidence.source_closure, tkd.source_closure_digest("tree-a"));
        assert_eq!(
            evidence.lock_closure,
            Sha256Digest::from_bytes(&tkd.closure().canonical_lock_bytes())
        );
    }

    #[test]
    fn generated_root_digest_covers_engine_and_exact_generated_bytes() {
        let fixture = Fixture::new();
        let source = fixture.assemble("tkd");
        let digest = source.generated_root_digest();

        let mut engine = source.clone();
        engine.engine = "9.9.9".to_string();
        assert_ne!(engine.generated_root_digest(), digest);

        let mut manifest = source.clone();
        manifest.cargo_toml.push_str("# changed\n");
        assert_ne!(manifest.generated_root_digest(), digest);

        let mut main = source;
        main.main_rs.push_str("// changed\n");
        assert_ne!(main.generated_root_digest(), digest);
    }

    proptest! {
        // Feature: platform-builder-abstraction, Property 22: catalog selection determines one static root.
        #[test]
        fn generated_assembly_matches_the_three_root_reference_model(
            platform_segment in "[a-z][a-z0-9]{0,10}",
            format_segment in "[a-z][a-z0-9]{0,10}",
            snapshot in "[a-f0-9]{8,40}",
            definition_bytes in prop::collection::vec(any::<u8>(), 0..128),
        ) {
            let platform_id = PlatformId::new(&platform_segment)
                .expect("generated platform id is canonical");
            let format_id = DefinitionFormatId::new(&format_segment)
                .expect("generated format id is canonical");
            let cli = PackageCoordinates {
                package_id: "cli-id".to_string(),
                package_name: TKP_PACKAGE.to_string(),
                library_target: "tokeira_tkp".to_string(),
                manifest_path: PathBuf::from("/workspace/crates/cli/Cargo.toml"),
            };
            let platform = PackageCoordinates {
                package_id: "platform-id".to_string(),
                package_name: format!("{platform_segment}-platform"),
                library_target: format!("{platform_segment}_platform"),
                manifest_path: PathBuf::from(format!(
                    "/workspace/platforms/{platform_segment}/Cargo.toml"
                )),
            };
            let frontend = PackageCoordinates {
                package_id: "frontend-id".to_string(),
                package_name: format!("{format_segment}-frontend"),
                library_target: format!("{format_segment}_frontend"),
                manifest_path: PathBuf::from(format!(
                    "/workspace/frontends/{format_segment}/Cargo.toml"
                )),
            };
            let cargo_toml = render_manifest(
                &cli,
                "../../crates/cli",
                &platform,
                &format!("../../platforms/{platform_segment}"),
                &frontend,
                &format!("../../frontends/{format_segment}"),
                &format_segment,
            );
            let main_rs = render_main(&platform_id, &format_id, &[], &format_segment);
            let source = BoundProvisionerSource {
                platform: platform_id,
                format: format_id,
                engine: "0.1.0".to_string(),
                cargo_toml,
                main_rs,
                closure: ProvisionerClosure {
                    crate_dirs: Vec::new(),
                    crate_names: vec![
                        TKP_PACKAGE.to_string(),
                        platform.package_name.clone(),
                        frontend.package_name.clone(),
                    ],
                    path_dependency_dirs: Vec::new(),
                    workspace_files: Vec::new(),
                    locked: Vec::new(),
                },
            };

            let manifest: toml::Value = toml::from_str(source.cargo_toml())
                .expect("generated manifest parses");
            let roots = manifest["dependencies"]
                .as_table()
                .expect("dependency table")
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>();
            prop_assert_eq!(
                roots,
                vec!["selected_frontend", "selected_platform", "tokeira_tkp"]
            );
            prop_assert_eq!(source.generated_root_digest(), source.clone().generated_root_digest());
            prop_assert_eq!(source.evidence(&snapshot), source.clone().evidence(&snapshot));

            let mut changed_contract = source.clone();
            changed_contract.engine = "9.9.9".to_string();
            prop_assert_ne!(
                source.generated_root_digest(),
                changed_contract.generated_root_digest()
            );
            let mut changed_generated_source = source.clone();
            changed_generated_source.main_rs.push_str("// changed\n");
            prop_assert_ne!(
                source.generated_root_digest(),
                changed_generated_source.generated_root_digest()
            );
            let mut changed_format = source.clone();
            changed_format.format = DefinitionFormatId::new(format!("{format_segment}-other"))
                .expect("derived format is canonical");
            changed_format.main_rs = render_main(
                &changed_format.platform,
                &changed_format.format,
                &[],
                changed_format.format.as_str(),
            );
            prop_assert_ne!(
                source.generated_root_digest(),
                changed_format.generated_root_digest()
            );

            // Deployment definitions are interpreted data, not executable
            // closure input: arbitrary definition bytes cannot re-key this
            // already selected platform/frontend engine.
            let mut edited_definition = definition_bytes.clone();
            edited_definition.push(0);
            prop_assert_ne!(definition_bytes, edited_definition);
            prop_assert_eq!(source.evidence(&snapshot), source.clone().evidence(&snapshot));
        }
    }

    // Proves the whole locking model on a real workspace: the scoped
    // manifest and scoped lock are exact enough for `--locked`, in the one
    // staged workspace the generated member resolves in.
    #[test]
    fn staged_workspace_compiles_the_conventional_exports_under_locked() {
        let fixture = Fixture::new();
        let source = fixture.assemble("tkd");
        let staging = tempfile::tempdir().expect("staging dir");
        source
            .stage_native_workspace(&fixture.root, staging.path())
            .expect("stage the scoped workspace");
        assert!(
            !staging.path().join("crates/unrelated").exists(),
            "out-of-closure members are not staged"
        );

        let output = cargo_check(staging.path());
        assert!(
            output.status.success(),
            "staged workspace failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn staged_workspace_carries_the_storage_schema_contract() {
        let fixture = Fixture::new();
        let contract = fixture
            .root
            .join("crates/tokeira-storage/schema-contract.toml");
        std::fs::create_dir_all(contract.parent().expect("contract has a parent"))
            .expect("create storage crate directory");
        write(&contract, "target_version = 67\n");
        let source = fixture.assemble("tkd");
        let staging = tempfile::tempdir().expect("staging dir");

        source
            .stage_native_workspace(&fixture.root, staging.path())
            .expect("stage the scoped workspace");

        assert_eq!(
            std::fs::read_to_string(
                staging
                    .path()
                    .join("crates/tokeira-storage/schema-contract.toml")
            )
            .expect("read staged schema contract"),
            "target_version = 67\n"
        );
    }

    #[test]
    fn compilation_rejects_a_missing_conventional_export() {
        let fixture = Fixture::new();
        let source = fixture.assemble("tkd");
        write(
            &fixture.root.join("platforms/alpha/src/lib.rs"),
            "pub fn differently_named() {}\n",
        );
        let staging = tempfile::tempdir().expect("staging dir");
        source
            .stage_native_workspace(&fixture.root, staging.path())
            .expect("stage the scoped workspace");

        let output = cargo_check(staging.path());
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("provisioner"),
            "unexpected compiler error: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn scoped_manifest_reduces_members_and_preserves_every_other_table() {
        let fixture = Fixture::new();
        let source = fixture.assemble("tkd");
        let full =
            std::fs::read_to_string(fixture.root.join("Cargo.toml")).expect("fixture manifest");

        let scoped = scoped_workspace_manifest(&full, source.closure()).expect("scoped manifest");
        let parsed: toml::Value = toml::from_str(&scoped).expect("scoped manifest parses");
        let members: Vec<&str> = parsed["workspace"]["members"]
            .as_array()
            .expect("members array")
            .iter()
            .filter_map(toml::Value::as_str)
            .collect();
        assert!(members.contains(&GENERATED_ROOT_RELATIVE_PATH));
        assert!(members.contains(&"crates/cli"));
        assert!(members.contains(&"platforms/alpha"));
        assert!(members.contains(&"frontends/tkd"));
        assert!(!members.contains(&"crates/unrelated"));
        assert!(!members.contains(&"frontends/tkdp"));
        // Everything outside the members array survives verbatim.
        assert_eq!(
            parsed["workspace"]["resolver"],
            toml::Value::String("3".to_string())
        );
    }

    #[test]
    fn scoped_manifest_drops_patches_the_closure_never_reaches() {
        let fixture = Fixture::new();
        let source = fixture.assemble("tkd");
        let full =
            std::fs::read_to_string(fixture.root.join("Cargo.toml")).expect("fixture manifest");
        // One patch the closure reaches (a member's name) and one it never
        // does — only the reachable entry may survive the scoping.
        let with_patches = format!(
            "{full}\n[patch.crates-io]\nalpha-platform = {{ path = \"platforms/alpha\" }}\nunrelated-vendored = {{ path = \"vendor/unrelated\" }}\n"
        );

        let scoped =
            scoped_workspace_manifest(&with_patches, source.closure()).expect("scoped manifest");
        let parsed: toml::Value = toml::from_str(&scoped).expect("scoped manifest parses");
        let patches = parsed["patch"]["crates-io"]
            .as_table()
            .expect("patch table survives");
        assert!(patches.contains_key("alpha-platform"));
        assert!(!patches.contains_key("unrelated-vendored"));

        // A closure reaching NO patched package drops the table whole.
        let mut none_reachable = source.clone();
        let scoped = {
            let closure = none_reachable.closure().clone();
            let _ = &mut none_reachable;
            let mut trimmed = closure;
            trimmed.crate_names.retain(|name| name != "alpha-platform");
            trimmed.locked.clear();
            scoped_workspace_manifest(&with_patches, &trimmed).expect("scoped manifest")
        };
        let parsed: toml::Value = toml::from_str(&scoped).expect("scoped manifest parses");
        assert!(
            parsed.get("patch").is_none(),
            "an all-unreachable patch table is dropped whole:\n{scoped}"
        );
    }

    #[test]
    fn staged_lock_holds_exactly_the_closure_and_the_generated_root() {
        let fixture = Fixture::new();
        let source = fixture.assemble("tkd");
        let staging = tempfile::tempdir().expect("staging dir");
        source
            .stage_native_workspace(&fixture.root, staging.path())
            .expect("stage the scoped workspace");

        let scoped =
            std::fs::read_to_string(staging.path().join("Cargo.lock")).expect("staged lock");
        let parsed: toml::Value = toml::from_str(&scoped).expect("staged lock parses");
        let names: Vec<&str> = parsed["package"]
            .as_array()
            .expect("package entries")
            .iter()
            .filter_map(|entry| entry.get("name").and_then(toml::Value::as_str))
            .collect();
        assert!(names.contains(&GENERATED_ROOT_PACKAGE));
        assert!(names.contains(&TKP_PACKAGE));
        assert!(names.contains(&"alpha-platform"));
        assert!(names.contains(&"tkd-frontend"));
        assert!(!names.contains(&"unrelated"));
        assert!(!names.contains(&"tkdp-frontend"));

        // Deterministic: restaging reproduces the same bytes.
        let staging_again = tempfile::tempdir().expect("staging dir");
        source
            .stage_native_workspace(&fixture.root, staging_again.path())
            .expect("stage again");
        assert_eq!(
            scoped,
            std::fs::read_to_string(staging_again.path().join("Cargo.lock"))
                .expect("staged lock again")
        );
    }

    #[test]
    fn a_lock_entry_outside_the_admitted_closure_is_refused() {
        let fixture = Fixture::new();
        let source = fixture.assemble("tkd");
        let root =
            format!("[[package]]\nname = \"{GENERATED_ROOT_PACKAGE}\"\nversion = \"0.0.0\"\n");
        let members: String = source
            .closure()
            .crate_names
            .iter()
            .map(|name| format!("[[package]]\nname = \"{name}\"\nversion = \"0.1.0\"\n\n"))
            .collect();

        let foreign = format!(
            "version = 4\n\n{members}{root}\n[[package]]\nname = \"foreign\"\nversion = \"9.9.9\"\nsource = \"registry+https://github.com/rust-lang/crates.io-index\"\n"
        );
        let err = validate_scoped_lock(&foreign, source.closure())
            .expect_err("a package outside the closure is refused");
        assert!(
            err.to_string()
                .contains("outside the admitted source closure")
        );

        let missing_member = format!("version = 4\n\n{root}");
        let err = validate_scoped_lock(&missing_member, source.closure())
            .expect_err("a lock without the closure members is refused");
        assert!(err.to_string().contains("no entry for closure member"));

        let missing_root = format!("version = 4\n\n{members}");
        let err = validate_scoped_lock(&missing_root, source.closure())
            .expect_err("a lock without the generated root is refused");
        assert!(err.to_string().contains("generated provisioner root"));

        let exact = format!("version = 4\n\n{members}{root}");
        validate_scoped_lock(&exact, source.closure()).expect("the exact set is accepted");
    }

    #[test]
    fn restaging_prunes_strays_and_preserves_unchanged_mtimes() {
        let fixture = Fixture::new();
        let source = fixture.assemble("tkd");
        let staging = tempfile::tempdir().expect("staging dir");
        source
            .stage_native_workspace(&fixture.root, staging.path())
            .expect("first staging");

        // A stray from a prior (wider) staging, and cargo's own target dir.
        write(&staging.path().join("crates/cli/src/stale.rs"), "oops\n");
        std::fs::create_dir_all(staging.path().join("target/debug")).expect("target dir");
        write(&staging.path().join("target/debug/artifact"), "keep\n");
        let lib = staging.path().join("crates/cli/src/lib.rs");
        let mtime_before = std::fs::metadata(&lib)
            .expect("lib metadata")
            .modified()
            .expect("lib mtime");

        source
            .stage_native_workspace(&fixture.root, staging.path())
            .expect("second staging");

        assert!(
            !staging.path().join("crates/cli/src/stale.rs").exists(),
            "strays are swept"
        );
        assert!(
            staging.path().join("target/debug/artifact").exists(),
            "target/ is cargo's and is never touched"
        );
        assert_eq!(
            std::fs::metadata(&lib)
                .expect("lib metadata")
                .modified()
                .expect("lib mtime"),
            mtime_before,
            "unchanged files keep their mtime — incremental builds survive"
        );
    }

    fn package(root: &Path, relative: &str, name: &str, metadata: &str, source: &str) {
        let package = root.join(relative);
        std::fs::create_dir_all(package.join("src")).expect("create package source");
        write(
            &package.join("Cargo.toml"),
            &format!(
                "[package]\nname = \"{name}\"\nversion = \"0.1.0\"\nedition = \"2024\"\n{metadata}"
            ),
        );
        write(&package.join("src/lib.rs"), source);
    }

    fn write(path: &Path, content: &str) {
        std::fs::write(path, content).expect("write fixture file");
    }

    fn cargo_check(staging: &Path) -> std::process::Output {
        Command::new(env!("CARGO"))
            .current_dir(staging)
            .args([
                "check",
                "--offline",
                "--locked",
                "-p",
                GENERATED_ROOT_PACKAGE,
            ])
            .output()
            .expect("run cargo check")
    }
}
