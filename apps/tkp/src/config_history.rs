//! Per-revision configuration snapshots (task 14.3).
//!
//! Config-revision revert is a *same-engine* apply of a **prior recorded config
//! revision** (Req 13.3) — not an `upgrade` (the engine identity is unchanged),
//! not a two-binary `rollback` (Proposal 002). For a prior revision to be
//! re-applicable, its config **source** must have been retained: each applying
//! verb snapshots the live config source keyed by the `config_revision` it
//! produced, and `revert` restores that snapshot into the live config file
//! before the ordinary gated apply reconciles toward it.
//!
//! Each snapshot is stored **under the config file's basename** for the platform
//! it came from — `{dir}/state/config-revisions/{n}/{basename}` (a `.tkd` for
//! compose-syn, `deployment.toml` for local). Keying by basename makes a
//! cross-platform revert *refuse* rather than clobber: a revision retained as a
//! `deployment.toml` is simply not present under the current `definition.tkd`
//! basename, so `restore` errors instead of overwriting the live `.tkd` with the
//! wrong format. The config source *is* the desired-state definition, so
//! retaining the source is sufficient — no engine rebuild, no before-images.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::platform;

fn revisions_root(deployment_dir: &Path) -> PathBuf {
    deployment_dir.join("state").join("config-revisions")
}

/// The config source file's basename for the deployment's resolved platform.
fn config_basename(deployment_dir: &Path) -> &'static str {
    match platform::detect(deployment_dir) {
        platform::Platform::ComposeSyn => "definition.tkd",
        platform::Platform::Local => "deployment.toml",
    }
}

/// The live config source file for the deployment's resolved platform.
pub(crate) fn config_file(deployment_dir: &Path) -> PathBuf {
    deployment_dir.join(config_basename(deployment_dir))
}

/// Where a given revision's snapshot lives — under the *current platform's*
/// config basename, so it is found only when reverting within the same platform.
fn snapshot_path(deployment_dir: &Path, revision: u64) -> PathBuf {
    revisions_root(deployment_dir)
        .join(revision.to_string())
        .join(config_basename(deployment_dir))
}

/// Retain the current config source as `revision`. Idempotent (a re-snapshot of
/// the same revision overwrites). A deployment with no config file yet (local
/// defaults) has nothing to retain — that is not an error.
pub(crate) fn snapshot(deployment_dir: &Path, revision: u64) -> Result<()> {
    let src = config_file(deployment_dir);
    let bytes = match std::fs::read(&src) {
        Ok(bytes) => bytes,
        Err(_) => return Ok(()),
    };
    let dst = snapshot_path(deployment_dir, revision);
    if let Some(parent) = dst.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    std::fs::write(&dst, &bytes).with_context(|| format!("failed to write {}", dst.display()))?;
    Ok(())
}

/// Whether `revision`'s config source was retained **for the current platform**
/// (revertable). A revision snapshotted under a different platform is not
/// revertable here and reports `false`.
pub(crate) fn is_retained(deployment_dir: &Path, revision: u64) -> bool {
    snapshot_path(deployment_dir, revision).exists()
}

/// Restore a retained revision's config source into the live config file. Errors
/// if the revision was never snapshotted for this platform — never overwriting
/// the live config with a foreign-format or absent snapshot.
pub(crate) fn restore(deployment_dir: &Path, revision: u64) -> Result<()> {
    let snap = snapshot_path(deployment_dir, revision);
    let bytes = std::fs::read(&snap).with_context(|| {
        format!(
            "config revision {revision} has no retained {} snapshot ({}); only same-platform \
             revisions produced by a prior `init`/`apply` can be reverted to",
            config_basename(deployment_dir),
            snap.display()
        )
    })?;
    let dst = config_file(deployment_dir);
    std::fs::write(&dst, &bytes).with_context(|| format!("failed to write {}", dst.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_then_restore_round_trips_the_config_source() {
        let tmp = tempfile::tempdir().unwrap();
        // No `.tkd` → local config file.
        assert!(config_file(tmp.path()).ends_with("deployment.toml"));

        // A `definition.tkd` resolves the deployment to compose-syn.
        std::fs::write(tmp.path().join("definition.tkd"), b"REV-ONE").unwrap();
        let cfg = config_file(tmp.path());
        assert!(cfg.ends_with("definition.tkd"));

        snapshot(tmp.path(), 1).unwrap();
        assert!(is_retained(tmp.path(), 1));
        assert!(!is_retained(tmp.path(), 2));

        // Advance the live config, then revert to revision 1's retained source.
        std::fs::write(&cfg, b"REV-TWO").unwrap();
        restore(tmp.path(), 1).unwrap();
        assert_eq!(std::fs::read(&cfg).unwrap(), b"REV-ONE");
    }

    #[test]
    fn restore_of_an_unretained_revision_errors() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("definition.tkd"), b"x").unwrap();
        let err = restore(tmp.path(), 7).expect_err("no snapshot → error");
        assert!(err.to_string().contains("no retained"), "unexpected: {err}");
    }

    // Regression for the cross-platform clobber: a revision retained while the
    // deployment was local must NOT overwrite a later `definition.tkd`.
    #[test]
    fn cross_platform_revision_is_refused_not_clobbered() {
        let tmp = tempfile::tempdir().unwrap();
        // Snapshot revision 1 as a LOCAL deployment (deployment.toml).
        std::fs::write(tmp.path().join("deployment.toml"), b"LOCAL-TOML").unwrap();
        snapshot(tmp.path(), 1).unwrap();
        assert!(is_retained(tmp.path(), 1), "retained under the local basename");

        // The deployment becomes compose-syn (a `.tkd` appears with real content).
        std::fs::write(tmp.path().join("definition.tkd"), b"REAL-TKD").unwrap();
        assert!(
            !is_retained(tmp.path(), 1),
            "the local revision is not retained under the compose-syn basename"
        );

        let err = restore(tmp.path(), 1).expect_err("cross-platform restore refuses");
        assert!(err.to_string().contains("no retained"), "unexpected: {err}");
        // The live `.tkd` is untouched — no clobber.
        assert_eq!(
            std::fs::read(tmp.path().join("definition.tkd")).unwrap(),
            b"REAL-TKD"
        );
    }
}
