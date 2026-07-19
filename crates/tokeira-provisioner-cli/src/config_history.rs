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
//! compose-syn, `deployment.toml` for local); the basename comes from the
//! injected platform's `config_basename`. Keying by basename makes a
//! cross-platform revert *refuse* rather than clobber: a revision retained as a
//! `deployment.toml` is simply not present under the current `definition.tkd`
//! basename, so `restore` errors instead of overwriting the live `.tkd` with the
//! wrong format. The config source *is* the desired-state definition, so
//! retaining the source is sufficient — no engine rebuild, no before-images.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

fn revisions_root(deployment_dir: &Path) -> PathBuf {
    deployment_dir.join("state").join("config-revisions")
}

/// The live config source file for the platform's `config_basename`.
pub(crate) fn config_file(deployment_dir: &Path, config_basename: &str) -> PathBuf {
    deployment_dir.join(config_basename)
}

/// Where a given revision's snapshot lives — under the *current platform's*
/// config basename, so it is found only when reverting within the same platform.
fn snapshot_path(deployment_dir: &Path, config_basename: &str, revision: u64) -> PathBuf {
    revisions_root(deployment_dir)
        .join(revision.to_string())
        .join(config_basename)
}

/// Retain the current config source as `revision`. Idempotent (a re-snapshot of
/// the same revision overwrites). A deployment with no config file yet (local
/// defaults) has nothing to retain — that is not an error.
pub(crate) fn snapshot(deployment_dir: &Path, config_basename: &str, revision: u64) -> Result<()> {
    let src = config_file(deployment_dir, config_basename);
    let bytes = match std::fs::read(&src) {
        Ok(bytes) => bytes,
        Err(_) => return Ok(()),
    };
    let dst = snapshot_path(deployment_dir, config_basename, revision);
    if let Some(parent) = dst.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    std::fs::write(&dst, &bytes).with_context(|| format!("failed to write {}", dst.display()))?;
    Ok(())
}

/// Whether `revision`'s config source was retained **for the current platform**
/// (revertable). A revision snapshotted under a different platform is not
/// retained under this basename and reports `false`.
pub(crate) fn is_retained(deployment_dir: &Path, config_basename: &str, revision: u64) -> bool {
    snapshot_path(deployment_dir, config_basename, revision).exists()
}

/// Every revision retained **for the current platform** (its basename), sorted
/// ascending — the revertable set `describe` reports.
pub(crate) fn retained_revisions(deployment_dir: &Path, config_basename: &str) -> Vec<u64> {
    let Ok(entries) = std::fs::read_dir(revisions_root(deployment_dir)) else {
        return Vec::new();
    };
    let mut revisions: Vec<u64> = entries
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| entry.file_name().to_str().and_then(|n| n.parse().ok()))
        .filter(|revision| is_retained(deployment_dir, config_basename, *revision))
        .collect();
    revisions.sort_unstable();
    revisions
}

/// Restore a retained revision's config source into the live config file. Errors
/// if the revision was never snapshotted for this platform — never overwriting
/// the live config with a foreign-format or absent snapshot.
pub(crate) fn restore(deployment_dir: &Path, config_basename: &str, revision: u64) -> Result<()> {
    let snap = snapshot_path(deployment_dir, config_basename, revision);
    let bytes = std::fs::read(&snap).with_context(|| {
        format!(
            "config revision {revision} has no retained {config_basename} snapshot ({}); only \
             same-platform revisions produced by a prior `init`/`apply` can be reverted to",
            snap.display()
        )
    })?;
    let dst = config_file(deployment_dir, config_basename);
    std::fs::write(&dst, &bytes).with_context(|| format!("failed to write {}", dst.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const TKD: &str = "definition.tkd";
    const TOML: &str = "deployment.toml";

    #[test]
    fn snapshot_then_restore_round_trips_the_config_source() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join(TKD), b"REV-ONE").unwrap();
        let cfg = config_file(tmp.path(), TKD);
        assert!(cfg.ends_with(TKD));

        snapshot(tmp.path(), TKD, 1).unwrap();
        assert!(is_retained(tmp.path(), TKD, 1));
        assert!(!is_retained(tmp.path(), TKD, 2));

        // Advance the live config, then revert to revision 1's retained source.
        std::fs::write(&cfg, b"REV-TWO").unwrap();
        restore(tmp.path(), TKD, 1).unwrap();
        assert_eq!(std::fs::read(&cfg).unwrap(), b"REV-ONE");
    }

    #[test]
    fn restore_of_an_unretained_revision_errors() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join(TKD), b"x").unwrap();
        let err = restore(tmp.path(), TKD, 7).expect_err("no snapshot → error");
        assert!(err.to_string().contains("no retained"), "unexpected: {err}");
    }

    #[test]
    fn retained_revisions_lists_the_platform_keyed_set_sorted() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join(TKD), b"cfg").unwrap();
        snapshot(tmp.path(), TKD, 2).unwrap();
        snapshot(tmp.path(), TKD, 0).unwrap();
        // A revision retained under the OTHER platform's basename is not listed.
        std::fs::write(tmp.path().join(TOML), b"toml").unwrap();
        snapshot(tmp.path(), TOML, 1).unwrap();

        assert_eq!(retained_revisions(tmp.path(), TKD), vec![0, 2]);
        assert_eq!(retained_revisions(tmp.path(), TOML), vec![1]);
        // No history at all → empty, not an error.
        let empty = tempfile::tempdir().unwrap();
        assert!(retained_revisions(empty.path(), TKD).is_empty());
    }

    // Regression for the cross-platform clobber: a revision retained while the
    // deployment was local (keyed `deployment.toml`) must NOT overwrite a later
    // `definition.tkd`.
    #[test]
    fn cross_platform_revision_is_refused_not_clobbered() {
        let tmp = tempfile::tempdir().unwrap();
        // Snapshot revision 1 as a LOCAL deployment (deployment.toml).
        std::fs::write(tmp.path().join(TOML), b"LOCAL-TOML").unwrap();
        snapshot(tmp.path(), TOML, 1).unwrap();
        assert!(
            is_retained(tmp.path(), TOML, 1),
            "retained under the local basename"
        );

        // The deployment becomes compose-syn (a `.tkd` appears with real content).
        std::fs::write(tmp.path().join(TKD), b"REAL-TKD").unwrap();
        assert!(
            !is_retained(tmp.path(), TKD, 1),
            "the local revision is not retained under the compose-syn basename"
        );

        let err = restore(tmp.path(), TKD, 1).expect_err("cross-platform restore refuses");
        assert!(err.to_string().contains("no retained"), "unexpected: {err}");
        // The live `.tkd` is untouched — no clobber.
        assert_eq!(std::fs::read(tmp.path().join(TKD)).unwrap(), b"REAL-TKD");
    }
}
