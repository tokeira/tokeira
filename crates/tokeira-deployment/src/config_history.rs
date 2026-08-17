//! Format-bearing per-revision desired-source snapshots.
//!
//! Each revision retains the exact source bytes plus an identity sidecar. A
//! restore compares format and safe relative path before replacing the live
//! source, so equal filenames under different frontends cannot cross formats.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tokeira_orchestrator::{DefinitionFormatId, RelativeDefinitionPath};

use crate::deployment::ConfigSource;

const SOURCE_METADATA: &str = "source.json";

/// The server configuration retained beside the definition source. A revision
/// folder holds the whole desired-source set: a baseline realization that
/// resolves companions against it (rather than the live deployment dir) can
/// then attribute a `tokeirad.toml` edit to the operator instead of
/// misreading it as a provisioner advance.
pub const SERVER_CONFIG: &str = "tokeirad.toml";

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetainedSourceIdentity {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    format: Option<DefinitionFormatId>,
    path: RelativeDefinitionPath,
    /// File names of the definition parts retained beside the root — the
    /// sibling `.{format}` files present when the revision was taken. The
    /// whole set is retained (not just the parts the evaluation served):
    /// the revision folder is a snapshot of the authored source set, the
    /// shape the platform-source-set work will formalize.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    parts: Vec<String>,
}

impl RetainedSourceIdentity {
    fn new(source: &ConfigSource, parts: Vec<String>) -> Self {
        Self {
            format: source.format.clone(),
            path: source.path.clone(),
            parts,
        }
    }
}

fn revisions_root(deployment_dir: &Path) -> PathBuf {
    deployment_dir.join("state").join("config-revisions")
}

fn revision_root(deployment_dir: &Path, revision: u64) -> PathBuf {
    revisions_root(deployment_dir).join(revision.to_string())
}

/// The sole live source recorded for this deployment.
pub fn config_file(deployment_dir: &Path, source: &ConfigSource) -> PathBuf {
    deployment_dir.join(source.path.as_path())
}

/// Retained source bytes for one revision.
pub fn snapshot_path(deployment_dir: &Path, source: &ConfigSource, revision: u64) -> PathBuf {
    revision_root(deployment_dir, revision).join(source.path.as_path())
}

fn identity_path(deployment_dir: &Path, revision: u64) -> PathBuf {
    revision_root(deployment_dir, revision).join(SOURCE_METADATA)
}

/// Retain exact live bytes and their independently admitted format/path,
/// plus every sibling definition part (`*.{format}` beside the root).
pub fn snapshot(deployment_dir: &Path, source: &ConfigSource, revision: u64) -> Result<()> {
    let live = config_file(deployment_dir, source);
    let bytes = match std::fs::read(&live) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && source.format.is_none() => {
            return Ok(());
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to read recorded source {}", live.display()));
        }
    };
    let destination = snapshot_path(deployment_dir, source, revision);
    let parent = destination
        .parent()
        .expect("a retained deployment-relative path has a revision parent");
    std::fs::create_dir_all(parent)
        .with_context(|| format!("failed to create {}", parent.display()))?;
    std::fs::write(&destination, bytes)
        .with_context(|| format!("failed to write {}", destination.display()))?;
    let parts = live_part_names(&live, source)?;
    for part in &parts {
        let live_part = live
            .parent()
            .expect("a live source has a parent")
            .join(part);
        let retained_part = parent.join(part);
        let bytes = std::fs::read(&live_part)
            .with_context(|| format!("failed to read part {}", live_part.display()))?;
        std::fs::write(&retained_part, bytes)
            .with_context(|| format!("failed to write {}", retained_part.display()))?;
    }
    let identity = serde_json::to_vec_pretty(&RetainedSourceIdentity::new(source, parts))?;
    let identity_path = identity_path(deployment_dir, revision);
    std::fs::write(&identity_path, identity)
        .with_context(|| format!("failed to write {}", identity_path.display()))?;
    // The desired-source companion, when the deployment carries one. Retained
    // only — `restore` deliberately leaves it alone: reverting the definition
    // does not rewrite the live server configuration.
    let server_config = deployment_dir.join(SERVER_CONFIG);
    match std::fs::read(&server_config) {
        Ok(bytes) => {
            let retained = revision_root(deployment_dir, revision).join(SERVER_CONFIG);
            std::fs::write(&retained, bytes)
                .with_context(|| format!("failed to write {}", retained.display()))?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| {
                format!("failed to read server config {}", server_config.display())
            });
        }
    }
    Ok(())
}

/// Retain an applied revision's explanation beside its retained sources.
/// The revision folder is the deployment's own record of what each apply
/// meant — `state/config-revisions/{n}/explanation.json` is readable by
/// operators (`jq`) and agents alike, with no serving process in between.
pub fn retain_explanation(
    deployment_dir: &Path,
    revision: u64,
    explanation: &tokeira_explain::DeploymentExplanation,
) -> Result<()> {
    let root = revision_root(deployment_dir, revision);
    std::fs::create_dir_all(&root)
        .with_context(|| format!("failed to create {}", root.display()))?;
    tokeira_explain::artifact::write(&root.join("explanation.json"), explanation)?;
    Ok(())
}

/// The sibling definition parts beside the live root: every `.{format}`
/// file in the root's directory except the root itself, ascending by name.
/// A format-less source has no part convention and yields none.
fn live_part_names(live_root: &Path, source: &ConfigSource) -> Result<Vec<String>> {
    let Some(format) = &source.format else {
        return Ok(Vec::new());
    };
    let dir = live_root.parent().expect("a live source has a parent");
    let root_name = live_root.file_name();
    let mut names = Vec::new();
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(error).with_context(|| format!("failed to list {}", dir.display()));
        }
    };
    for entry in entries {
        let entry = entry.with_context(|| format!("failed to list {}", dir.display()))?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some(format.as_str())
            && path.file_name() != root_name
            && path.is_file()
            && let Some(name) = path.file_name().and_then(|n| n.to_str())
        {
            names.push(name.to_string());
        }
    }
    names.sort_unstable();
    Ok(names)
}

/// Directory the retarget gate resolves a retained revision's parts from —
/// the revision folder itself, which holds the part files flat beside the
/// retained root.
pub fn retained_parts_dir(deployment_dir: &Path, source: &ConfigSource, revision: u64) -> PathBuf {
    snapshot_path(deployment_dir, source, revision)
        .parent()
        .expect("a retained deployment-relative path has a revision parent")
        .to_path_buf()
}

fn retained_identity(
    deployment_dir: &Path,
    revision: u64,
) -> Result<Option<RetainedSourceIdentity>> {
    let path = identity_path(deployment_dir, revision);
    match std::fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .with_context(|| format!("failed to decode {}", path.display()))
            .map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("failed to read {}", path.display())),
    }
}

fn identity_matches(retained: &RetainedSourceIdentity, source: &ConfigSource) -> bool {
    retained.format == source.format && retained.path == source.path
}

/// Whether a revision carries bytes under the exact current format/path.
/// The retained source text for `revision`, when that revision was retained.
/// Absence is a fact, not an error: a deployment that has never applied has
/// nothing to compare against.
pub fn retained_source(
    deployment_dir: &Path,
    source: &ConfigSource,
    revision: u64,
) -> anyhow::Result<Option<String>> {
    let path = snapshot_path(deployment_dir, source, revision);
    if !path.exists() {
        return Ok(None);
    }
    std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read the retained revision at {}", path.display()))
        .map(Some)
}

pub fn is_retained(deployment_dir: &Path, source: &ConfigSource, revision: u64) -> bool {
    let bytes_exist = snapshot_path(deployment_dir, source, revision).is_file();
    if !bytes_exist {
        return false;
    }
    match retained_identity(deployment_dir, revision) {
        Ok(Some(retained)) => identity_matches(&retained, source),
        // Pre-sidecar history is admitted only for legacy, format-less paths.
        Ok(None) => source.format.is_none(),
        Err(_) => false,
    }
}

/// Every revision retained for the exact current format/path, ascending.
pub fn retained_revisions(deployment_dir: &Path, source: &ConfigSource) -> Vec<u64> {
    let Ok(entries) = std::fs::read_dir(revisions_root(deployment_dir)) else {
        return Vec::new();
    };
    let mut revisions = entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            entry
                .file_name()
                .to_str()
                .and_then(|name| name.parse().ok())
        })
        .filter(|revision| is_retained(deployment_dir, source, *revision))
        .collect::<Vec<_>>();
    revisions.sort_unstable();
    revisions
}

/// Restore one same-format, same-path revision into the recorded live
/// source, definition parts included. Parts the revision retained are
/// written back over their live counterparts; a live part file the revision
/// never knew is left in place — the restored root decides what it imports,
/// and an unimported file is inert.
pub fn restore(deployment_dir: &Path, source: &ConfigSource, revision: u64) -> Result<()> {
    let retained = retained_identity(deployment_dir, revision)?;
    let parts = match retained {
        Some(retained) if identity_matches(&retained, source) => retained.parts,
        Some(retained) => bail!(
            "config revision {revision} records format/path {:?}/{}; current source is {:?}/{}",
            retained.format,
            retained.path.as_str(),
            source.format,
            source.path.as_str()
        ),
        None if source.format.is_none() => Vec::new(),
        None => bail!(
            "config revision {revision} has no format-bearing source identity; refusing restore"
        ),
    };

    let snapshot = snapshot_path(deployment_dir, source, revision);
    let bytes = std::fs::read(&snapshot).with_context(|| {
        format!(
            "config revision {revision} has no retained {} snapshot ({})",
            source.path.as_str(),
            snapshot.display()
        )
    })?;
    let live = config_file(deployment_dir, source);
    replace_file(&live, bytes)?;
    let retained_dir = retained_parts_dir(deployment_dir, source, revision);
    let live_dir = live.parent().expect("a live source has a parent");
    for part in &parts {
        let bytes = std::fs::read(retained_dir.join(part))
            .with_context(|| format!("config revision {revision} has no retained part {part}"))?;
        replace_file(&live_dir.join(part), bytes)?;
    }
    Ok(())
}

/// Writes bytes next to the target and renames into place, so a torn write
/// never leaves a half-restored file.
fn replace_file(live: &Path, bytes: Vec<u8>) -> Result<()> {
    let temporary = live.with_extension(format!("restore-{}", std::process::id()));
    std::fs::write(&temporary, bytes)
        .with_context(|| format!("failed to write {}", temporary.display()))?;
    if let Err(error) = std::fs::rename(&temporary, live) {
        let _ = std::fs::remove_file(&temporary);
        return Err(error).with_context(|| format!("failed to replace {}", live.display()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tkd() -> ConfigSource {
        ConfigSource::recorded(
            tokeira_orchestrator::DefinitionFormatId::new("tkd").expect("format"),
            tokeira_orchestrator::RelativeDefinitionPath::new("definition.tkd").expect("path"),
        )
    }

    #[test]
    fn snapshot_and_restore_round_trip_format_bearing_source() {
        let temp = tempfile::tempdir().expect("tempdir");
        let source = tkd();
        std::fs::write(config_file(temp.path(), &source), b"one").expect("write live");
        snapshot(temp.path(), &source, 1).expect("snapshot");
        std::fs::write(config_file(temp.path(), &source), b"two").expect("edit live");
        restore(temp.path(), &source, 1).expect("restore");
        assert_eq!(
            std::fs::read(config_file(temp.path(), &source)).expect("read live"),
            b"one"
        );
    }

    // The revision retains the whole definition set: the root plus every
    // sibling part, and restore brings the parts back with it.
    #[test]
    fn snapshot_and_restore_carry_the_definition_parts() {
        let temp = tempfile::tempdir().expect("tempdir");
        let source = tkd();
        std::fs::write(config_file(temp.path(), &source), b"root-one").expect("write live");
        std::fs::write(temp.path().join("networking.tkd"), b"part-one").expect("write part");
        std::fs::write(temp.path().join("notes.txt"), b"not a part").expect("write bystander");
        snapshot(temp.path(), &source, 1).expect("snapshot");

        let revision = revision_root(temp.path(), 1);
        assert_eq!(
            std::fs::read(revision.join("networking.tkd")).expect("retained part"),
            b"part-one"
        );
        assert!(
            !revision.join("notes.txt").exists(),
            "only part-extension files retain"
        );

        std::fs::write(config_file(temp.path(), &source), b"root-two").expect("edit root");
        std::fs::write(temp.path().join("networking.tkd"), b"part-two").expect("edit part");
        restore(temp.path(), &source, 1).expect("restore");
        assert_eq!(
            std::fs::read(config_file(temp.path(), &source)).expect("live root"),
            b"root-one"
        );
        assert_eq!(
            std::fs::read(temp.path().join("networking.tkd")).expect("live part"),
            b"part-one"
        );
    }

    #[test]
    fn snapshot_retains_the_server_config_beside_the_definition() {
        let temp = tempfile::tempdir().expect("tempdir");
        let source = tkd();
        std::fs::write(config_file(temp.path(), &source), b"def").expect("write live");
        std::fs::write(temp.path().join(SERVER_CONFIG), b"port = 1\n").expect("write toml");
        snapshot(temp.path(), &source, 4).expect("snapshot");
        assert_eq!(
            std::fs::read(revision_root(temp.path(), 4).join(SERVER_CONFIG)).expect("retained"),
            b"port = 1\n"
        );
        // Restore rewrites only the definition source; the live server
        // config is the operator's file and stays theirs.
        std::fs::write(temp.path().join(SERVER_CONFIG), b"port = 2\n").expect("edit toml");
        restore(temp.path(), &source, 4).expect("restore");
        assert_eq!(
            std::fs::read(temp.path().join(SERVER_CONFIG)).expect("live toml"),
            b"port = 2\n"
        );
    }

    #[test]
    fn equal_path_under_another_format_cannot_restore() {
        let temp = tempfile::tempdir().expect("tempdir");
        let source = tkd();
        std::fs::write(config_file(temp.path(), &source), b"tkd").expect("write live");
        snapshot(temp.path(), &source, 2).expect("snapshot");
        let other = ConfigSource::recorded(
            tokeira_orchestrator::DefinitionFormatId::new("tkdp").expect("format"),
            tokeira_orchestrator::RelativeDefinitionPath::new("definition.tkd").expect("path"),
        );
        assert!(!is_retained(temp.path(), &other, 2));
        let error = restore(temp.path(), &other, 2).expect_err("cross-format restore");
        assert!(error.to_string().contains("current source"));
    }
}
