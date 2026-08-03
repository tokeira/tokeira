//! Format-bearing per-revision desired-source snapshots.
//!
//! Each revision retains the exact source bytes plus an identity sidecar. A
//! restore compares format and safe relative path before replacing the live
//! source, so equal filenames under different frontends cannot cross formats.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tokeira_orchestrator::{DefinitionFormatId, RelativeDefinitionPath};

use crate::ConfigSource;

const SOURCE_METADATA: &str = "source.json";

/// The server configuration retained beside the definition source. A revision
/// folder holds the whole desired-source set: a baseline realization that
/// resolves companions against it (rather than the live deployment dir) can
/// then attribute a `tokeirad.toml` edit to the operator instead of
/// misreading it as a provisioner advance.
const SERVER_CONFIG: &str = "tokeirad.toml";

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetainedSourceIdentity {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    format: Option<DefinitionFormatId>,
    path: RelativeDefinitionPath,
}

impl From<&ConfigSource> for RetainedSourceIdentity {
    fn from(source: &ConfigSource) -> Self {
        Self {
            format: source.format.clone(),
            path: source.path.clone(),
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
pub(crate) fn config_file(deployment_dir: &Path, source: &ConfigSource) -> PathBuf {
    deployment_dir.join(source.path.as_path())
}

/// Retained source bytes for one revision.
pub(crate) fn snapshot_path(
    deployment_dir: &Path,
    source: &ConfigSource,
    revision: u64,
) -> PathBuf {
    revision_root(deployment_dir, revision).join(source.path.as_path())
}

fn identity_path(deployment_dir: &Path, revision: u64) -> PathBuf {
    revision_root(deployment_dir, revision).join(SOURCE_METADATA)
}

/// Retain exact live bytes and their independently admitted format/path.
pub(crate) fn snapshot(deployment_dir: &Path, source: &ConfigSource, revision: u64) -> Result<()> {
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
    let identity = serde_json::to_vec_pretty(&RetainedSourceIdentity::from(source))?;
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
pub(crate) fn retain_explanation(
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
pub(crate) fn is_retained(deployment_dir: &Path, source: &ConfigSource, revision: u64) -> bool {
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
pub(crate) fn retained_revisions(deployment_dir: &Path, source: &ConfigSource) -> Vec<u64> {
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

/// Restore one same-format, same-path revision into the recorded live source.
pub(crate) fn restore(deployment_dir: &Path, source: &ConfigSource, revision: u64) -> Result<()> {
    let retained = retained_identity(deployment_dir, revision)?;
    match retained {
        Some(retained) if identity_matches(&retained, source) => {}
        Some(retained) => bail!(
            "config revision {revision} records format/path {:?}/{}; current source is {:?}/{}",
            retained.format,
            retained.path.as_str(),
            source.format,
            source.path.as_str()
        ),
        None if source.format.is_none() => {}
        None => bail!(
            "config revision {revision} has no format-bearing source identity; refusing restore"
        ),
    }

    let snapshot = snapshot_path(deployment_dir, source, revision);
    let bytes = std::fs::read(&snapshot).with_context(|| {
        format!(
            "config revision {revision} has no retained {} snapshot ({})",
            source.path.as_str(),
            snapshot.display()
        )
    })?;
    let live = config_file(deployment_dir, source);
    let temporary = live.with_extension(format!("restore-{}", std::process::id()));
    std::fs::write(&temporary, bytes)
        .with_context(|| format!("failed to write {}", temporary.display()))?;
    if let Err(error) = std::fs::rename(&temporary, &live) {
        let _ = std::fs::remove_file(&temporary);
        return Err(error).with_context(|| format!("failed to replace {}", live.display()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tkd() -> ConfigSource {
        ConfigSource::definition("tkd", "definition.tkd").expect("source")
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
        let other = ConfigSource::definition("tkdp", "definition.tkd").expect("source");
        assert!(!is_retained(temp.path(), &other, 2));
        let error = restore(temp.path(), &other, 2).expect_err("cross-format restore");
        assert!(error.to_string().contains("current source"));
    }
}
