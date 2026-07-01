//! Deployment lock — the mis-apply guard (Requirement 8, task 10).
//!
//! `tkr` separates a **soft** default (`tkr deployment use`, the `.latest`
//! sentinel) from a **hard** guard (`tkr deployment lock`). While a lock is
//! active, every *mutating* command may target only the locked deployment; a
//! command aimed elsewhere refuses *before the launcher runs*, with no
//! per-command override. Read-only commands are never blocked.
//!
//! The lock is a durable `lock.toml` in the deployments registry root, read at
//! the start of every mutating command so it survives process and session
//! restarts. It records the locked name + a stable **identity fingerprint**
//! (deployment id + platform + storage). The fingerprint deliberately **excludes**
//! `source_tree_hash` (which changes on every upgrade), so the lock survives a
//! versioned upgrade (Property 13). A locked deployment that no longer exists, or
//! whose fingerprint no longer matches, **fails closed** — mutations refuse and
//! the discrepancy is surfaced rather than silently transferred.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::deployment_dir::{DeploymentResolver, METADATA_JSON};
use crate::metadata::{self, DeploymentMetadata};

const LOCK_FILE: &str = "lock.toml";

/// The durable lock record persisted at `{registry_root}/lock.toml`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentLock {
    /// The locked deployment's normalized name.
    pub name: String,
    /// A stable identity fingerprint (see [`fingerprint`]).
    pub fingerprint: String,
}

fn lock_path(root: &Path) -> PathBuf {
    root.join(LOCK_FILE)
}

/// A stable identity fingerprint for a deployment: deployment id + platform +
/// storage. Excludes `source_tree_hash` so the lock survives a versioned upgrade
/// (Property 13); a recreated deployment reusing the same name gets a fresh `id`
/// and therefore a different fingerprint, so the lock fails closed rather than
/// silently transferring.
pub fn fingerprint(meta: &DeploymentMetadata) -> String {
    let canonical = format!("{}|{:?}|{:?}", meta.id, meta.platform, meta.storage);
    format!("sha256:{}", tokeira_provisioner::sha256_hex(canonical.as_bytes()))
}

/// Read the active lock, if any.
pub fn read(resolver: &DeploymentResolver) -> Result<Option<DeploymentLock>> {
    let path = lock_path(resolver.root());
    match std::fs::read_to_string(&path) {
        Ok(text) => {
            let lock = toml::from_str(&text)
                .with_context(|| format!("invalid deployment lock file {}", path.display()))?;
            Ok(Some(lock))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err).with_context(|| format!("failed to read {}", path.display())),
    }
}

/// Lock the named (or currently-selected) deployment, and make it the soft
/// selection too so the happy path (`--deployment` omitted) targets it.
pub fn lock(resolver: &DeploymentResolver, name: Option<&str>) -> Result<DeploymentLock> {
    let name = resolver.resolve_name(name)?;
    let path = resolver.path(&name);
    if !path.join(METADATA_JSON).exists() {
        bail!("{}", resolver.not_found_message(&name)?);
    }
    let meta = metadata::read(&path)?;
    let lock = DeploymentLock {
        name: name.clone(),
        fingerprint: fingerprint(&meta),
    };
    std::fs::create_dir_all(resolver.root())?;
    std::fs::write(
        lock_path(resolver.root()),
        toml::to_string_pretty(&lock).context("failed to serialize the deployment lock")?,
    )
    .context("failed to write the deployment lock")?;
    // Align the soft selection with the hard lock.
    resolver.mark_latest(&name)?;
    Ok(lock)
}

/// Clear the lock. Returns the lock that was cleared, or `None` if none was set.
///
/// Deliberately tolerant of a corrupt `lock.toml`: `unlock --yes` is the
/// documented recovery command, so it must clear the file even when the record
/// no longer parses (while `enforce_mutation` keeps failing closed on it).
pub fn unlock(resolver: &DeploymentResolver) -> Result<Option<DeploymentLock>> {
    let existing = read(resolver).unwrap_or(None);
    let path = lock_path(resolver.root());
    if path.exists() {
        std::fs::remove_file(&path)
            .with_context(|| format!("failed to remove {}", path.display()))?;
    }
    Ok(existing)
}

/// Enforce the lock ahead of a *mutating* command targeting `target_name` (its
/// resolved deployment, or `None` to fall back to the soft selection). Read-only
/// commands must not call this. Fails closed on a wrong target, a vanished locked
/// deployment, or a changed identity fingerprint (a corrupt `lock.toml` also
/// refuses, via the `read` error).
///
/// Returns the **validated target name** when a lock is active so the caller can
/// pin dispatch to exactly the name this check approved — re-resolving the
/// `.latest` sentinel later would race a concurrent `deployment use`.
pub fn enforce_mutation(
    resolver: &DeploymentResolver,
    target_name: Option<&str>,
) -> Result<Option<String>> {
    let Some(lock) = read(resolver)? else {
        return Ok(None);
    };

    // The locked deployment must still exist with a matching fingerprint.
    let locked_path = resolver.path(&lock.name);
    if !locked_path.join(METADATA_JSON).exists() {
        bail!(
            "the deployment lock names '{}', which no longer exists — refusing all mutations \
             (fail closed). Run `tkr deployment unlock --yes` to clear it.",
            lock.name
        );
    }
    let meta = metadata::read(&locked_path)?;
    if fingerprint(&meta) != lock.fingerprint {
        bail!(
            "the identity fingerprint of the locked deployment '{}' no longer matches the lock — \
             refusing all mutations (fail closed). Re-lock explicitly with `tkr deployment lock`.",
            lock.name
        );
    }

    // The command must target the locked deployment.
    let target = resolver.resolve_name(target_name)?;
    if target != lock.name {
        bail!(
            "deployment '{}' is locked; refusing to mutate '{}'. There is no per-command override — \
             run `tkr deployment unlock --yes`, or re-lock with `tkr deployment lock {}`.",
            lock.name,
            target,
            target
        );
    }
    Ok(Some(target))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_orchestrator::{PlatformKind, StorageKind};

    fn seed(resolver: &DeploymentResolver, name: &str) -> DeploymentMetadata {
        resolver
            .create(name, PlatformKind::Local, StorageKind::InMemory, None)
            .unwrap()
    }

    fn resolver() -> (tempfile::TempDir, DeploymentResolver) {
        let tmp = tempfile::tempdir().unwrap();
        let resolver = DeploymentResolver::with_root(tmp.path().to_path_buf());
        (tmp, resolver)
    }

    #[test]
    fn fingerprint_is_stable_and_identity_scoped() {
        let (_t, r) = resolver();
        let a = seed(&r, "a");
        // Stable across reads.
        assert_eq!(fingerprint(&a), fingerprint(&a));
        // A different deployment identity (fresh id) yields a different fingerprint.
        let b = seed(&r, "b");
        assert_ne!(fingerprint(&a), fingerprint(&b));
    }

    #[test]
    fn lock_then_read_round_trips_and_unlock_clears() {
        let (_t, r) = resolver();
        seed(&r, "prod");
        let written = lock(&r, Some("prod")).unwrap();
        assert_eq!(read(&r).unwrap().as_ref(), Some(&written));
        let cleared = unlock(&r).unwrap();
        assert_eq!(cleared.as_ref(), Some(&written));
        assert!(read(&r).unwrap().is_none());
    }

    #[test]
    fn enforce_allows_the_locked_target_and_refuses_others() {
        let (_t, r) = resolver();
        seed(&r, "prod");
        seed(&r, "staging");
        lock(&r, Some("prod")).unwrap();

        // The locked deployment is allowed (explicit or via the soft selection lock
        // set), and the validated target is returned so dispatch can pin to it.
        assert_eq!(
            enforce_mutation(&r, Some("prod")).expect("locked target allowed"),
            Some("prod".to_string())
        );
        assert_eq!(
            enforce_mutation(&r, None).expect("soft selection is the locked deployment"),
            Some("prod".to_string())
        );

        // Any other deployment refuses.
        let err = enforce_mutation(&r, Some("staging")).expect_err("other target refused");
        assert!(err.to_string().contains("is locked"), "unexpected: {err}");
    }

    #[test]
    fn enforce_is_a_noop_without_a_lock() {
        let (_t, r) = resolver();
        seed(&r, "prod");
        assert_eq!(
            enforce_mutation(&r, Some("prod")).expect("no lock → allowed"),
            None,
            "no lock → nothing pinned"
        );
    }

    #[test]
    fn corrupt_lock_file_fails_closed_but_unlock_clears_it() {
        let (_t, r) = resolver();
        seed(&r, "prod");
        std::fs::create_dir_all(r.root()).unwrap();
        std::fs::write(r.root().join("lock.toml"), "not { valid toml").unwrap();

        // Mutations refuse (fail closed) while the record is unreadable...
        let err = enforce_mutation(&r, Some("prod")).expect_err("corrupt lock fails closed");
        assert!(err.to_string().contains("invalid deployment lock"), "unexpected: {err}");

        // ...but the documented recovery command still works.
        let cleared = unlock(&r).expect("unlock clears a corrupt lock file");
        assert!(cleared.is_none(), "nothing parseable to report");
        assert!(read(&r).unwrap().is_none(), "lock file removed");
        enforce_mutation(&r, Some("prod")).expect("mutations allowed again");
    }

    #[test]
    fn vanished_locked_deployment_fails_closed() {
        let (_t, r) = resolver();
        seed(&r, "prod");
        lock(&r, Some("prod")).unwrap();
        r.remove("prod").unwrap();
        let err = enforce_mutation(&r, Some("prod")).expect_err("vanished lock fails closed");
        assert!(err.to_string().contains("no longer exists"), "unexpected: {err}");
    }

    #[test]
    fn changed_identity_fingerprint_fails_closed() {
        let (_t, r) = resolver();
        seed(&r, "prod");
        lock(&r, Some("prod")).unwrap();
        // Recreate 'prod' with a fresh id (destroy + create) → fingerprint changes.
        r.remove("prod").unwrap();
        seed(&r, "prod");
        let err = enforce_mutation(&r, Some("prod")).expect_err("changed fingerprint fails closed");
        assert!(err.to_string().contains("fingerprint"), "unexpected: {err}");
    }
}
