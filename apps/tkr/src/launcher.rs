//! The launch seam (Requirement 7, task 9.1).
//!
//! `tkr` never mutates a deployment itself — for a lifecycle verb it resolves the
//! deployment's provisioner binary **by launch class**, checksum-verifies it, and
//! executes it, forwarding the verb to the bound `tkp`. The mutating binary is
//! therefore always the exact stamped binary married to the deployment.
//!
//! | Class | When | Binary | Verified against |
//! |-------|------|--------|------------------|
//! | **Bound** | normal versioned mutation | the recorded binary | the recorded integrity manifest |
//! | **Candidate-upgrade** | `upgrade` | operator/release-resolved B (manifest still records A) | external release metadata (follow-on) |
//! | **Dev-candidate** | apply to a `dev` deployment | the current local dev build | gate permits `DevIterate` |
//! | **Rollback** | `rollback` | bound B (undo), then retained A (reconcile) | B from the manifest; A from the checkpoint |
//!
//! This first increment resolves `tkp` (an installed binary on `PATH`, else a
//! `cargo run --bin tkp` dev build), enforces the **Bound** class's checksum
//! against the recorded manifest (abort on mismatch, Req 7.2), and execs
//! `tkp <verb> --deployment-dir <dir>`. The candidate-upgrade external-metadata
//! verification and the two-binary rollback re-exec are follow-ons.

use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{Context, Result, bail};
use tokeira_provisioner::{BuildMode, DeploymentStateEnvelope, sha256_hex};
use tokeira_state::{CasStore, DeploymentStore, LocalBackend};

use crate::deployment_dir::DeploymentContext;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaunchClass {
    Bound,
    CandidateUpgrade,
    DevCandidate,
    Rollback,
    /// A read-only verb (`describe`/`plan`/`status`). Never verified and never
    /// refused: `tkp describe` itself deliberately never gates, precisely so it
    /// can diagnose the mismatches that block the mutating classes.
    ReadOnly,
}

impl LaunchClass {
    pub fn label(self) -> &'static str {
        match self {
            LaunchClass::Bound => "bound",
            LaunchClass::CandidateUpgrade => "candidate-upgrade",
            LaunchClass::DevCandidate => "dev-candidate",
            LaunchClass::Rollback => "rollback",
            LaunchClass::ReadOnly => "read-only",
        }
    }
}

/// Resolve the launch class for `verb` from the deployment's recorded binding.
///
/// Read-only verbs are never gated (Req 6.1/8.3 — they must work precisely when
/// the mutating classes would refuse). `upgrade` cannot run the recorded binary
/// (A cannot know how to advance to B) → candidate-upgrade; `rollback` runs
/// B-then-A. Otherwise a versioned binding is **bound** (run exactly the recorded
/// binary) and a dev/unstamped binding is a **dev-candidate** (the current local
/// build; a fresh deployment we are about to `init` is treated the same).
pub fn resolve_class(verb: &str, envelope: &DeploymentStateEnvelope) -> LaunchClass {
    match verb {
        "describe" | "plan" | "status" => LaunchClass::ReadOnly,
        "upgrade" => LaunchClass::CandidateUpgrade,
        "rollback" => LaunchClass::Rollback,
        _ => match envelope.binding.as_ref().map(|b| b.build_mode) {
            Some(BuildMode::Versioned) => LaunchClass::Bound,
            _ => LaunchClass::DevCandidate,
        },
    }
}

/// The resolved provisioner binary.
enum TkpBinary {
    /// A concrete installed `tkp` (checksum-verifiable).
    Installed(PathBuf),
    /// `cargo run --bin tkp` — a dev build; not checksum-verifiable, so it can
    /// only serve dev/candidate classes, never **bound**.
    Cargo,
}

impl TkpBinary {
    fn resolve() -> Self {
        match which::which("tkp") {
            Ok(path) => TkpBinary::Installed(path),
            Err(_) => TkpBinary::Cargo,
        }
    }

    fn command(&self) -> (String, Vec<String>) {
        match self {
            TkpBinary::Installed(path) => (path.display().to_string(), Vec::new()),
            TkpBinary::Cargo => (
                "cargo".to_string(),
                vec![
                    "run".to_string(),
                    "--quiet".to_string(),
                    "--bin".to_string(),
                    "tkp".to_string(),
                    "--".to_string(),
                ],
            ),
        }
    }
}

/// Load the deployment's provisioner envelope (a `Default` envelope when the
/// deployment has never been `tkp init`-stamped). Mirrors `tkp`'s own store seam.
async fn load_envelope(deployment_dir: &Path) -> Result<DeploymentStateEnvelope> {
    let store: Box<dyn DeploymentStore<DeploymentStateEnvelope>> = Box::new(CasStore::new(
        Box::new(LocalBackend::new(deployment_dir.join("state/envelope"))),
        "envelope".to_string(),
    ));
    let (envelope, _version) = store
        .load()
        .await
        .context("failed to read the deployment's provisioner state")?;
    Ok(envelope)
}

/// Verify a concrete `tkp` binary against the recorded integrity manifest: its
/// SHA-256 must match one of the recorded artifacts. Abort otherwise (Req 7.2).
fn verify_against_manifest(path: &Path, envelope: &DeploymentStateEnvelope) -> Result<()> {
    let manifest = envelope.integrity.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "deployment is bound but records no integrity manifest; cannot verify the bound `tkp` \
             (was it initialized by `tkp init`?)"
        )
    })?;
    let bytes = std::fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    let actual = sha256_hex(&bytes);
    if !manifest.artifacts.iter().any(|a| a.sha256 == actual) {
        bail!(
            "integrity check failed: the `tkp` at {} (sha256 {}) matches no recorded artifact — \
             refusing to launch the bound binary (Req 7.2)",
            path.display(),
            actual
        );
    }
    Ok(())
}

/// Forward a lifecycle `verb` to the deployment's bound `tkp`: resolve the launch
/// class, checksum-verify (bound), then exec `tkp <verb> --deployment-dir <dir>
/// [extra_args]`, inheriting stdio and propagating the exit status.
pub async fn launch(ctx: &DeploymentContext, verb: &str, extra_args: &[String]) -> Result<()> {
    let deployment_dir = &ctx.path;
    let envelope = load_envelope(deployment_dir).await?;
    let class = resolve_class(verb, &envelope);
    let binary = TkpBinary::resolve();

    // The bound class must run the exact recorded binary — a concrete path that
    // checksum-matches the manifest. Dev/candidate/rollback verify differently
    // (dev = the current build; candidate/rollback = follow-ons).
    if class == LaunchClass::Bound {
        match &binary {
            TkpBinary::Installed(path) => verify_against_manifest(path, &envelope)?,
            TkpBinary::Cargo => bail!(
                "deployment is bound to a versioned engine but no installed `tkp` was found on PATH; \
                 refusing to drive it with a `cargo run` dev build — install the recorded `tkp`"
            ),
        }
    }

    let (program, mut args) = binary.command();
    args.push(verb.to_string());
    args.push("--deployment-dir".to_string());
    args.push(deployment_dir.display().to_string());
    args.extend(extra_args.iter().cloned());

    eprintln!(
        "launcher: {} class → forwarding `{verb}` to tkp",
        class.label()
    );
    let status = tokio::process::Command::new(&program)
        .args(&args)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .await
        .with_context(|| format!("failed to launch `{program}` (the deployment's provisioner)"))?;
    if !status.success() {
        // Propagate the provisioner's actual exit status (Req 7) — tkp already
        // reported its error on the inherited stderr, so exit with its code
        // rather than wrapping it into a fresh (always-1) error of our own.
        std::process::exit(status.code().unwrap_or(1));
    }
    Ok(())
}

/// Forward `apply`, first forwarding `init` when the deployment has never been
/// stamped — so `tkr deployment apply` is a coherent one-command flow.
pub async fn launch_apply(ctx: &DeploymentContext) -> Result<()> {
    if load_envelope(&ctx.path).await?.binding.is_none() {
        launch(ctx, "init", &[]).await?;
    }
    launch(ctx, "apply", &[]).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use tokeira_provisioner::{
        BinaryArtifactDescriptor, IntegrityManifest, ProvenanceStamp, Target,
    };

    fn envelope_with(build_mode: Option<BuildMode>) -> DeploymentStateEnvelope {
        DeploymentStateEnvelope {
            binding: build_mode.map(|mode| ProvenanceStamp {
                build_mode: mode,
                ..ProvenanceStamp::current(Utc::now())
            }),
            ..Default::default()
        }
    }

    #[test]
    fn resolve_class_maps_verb_and_binding() {
        let versioned = envelope_with(Some(BuildMode::Versioned));
        let dev = envelope_with(Some(BuildMode::Dev));
        let unstamped = envelope_with(None);

        assert_eq!(resolve_class("upgrade", &versioned), LaunchClass::CandidateUpgrade);
        assert_eq!(resolve_class("rollback", &versioned), LaunchClass::Rollback);
        assert_eq!(resolve_class("apply", &versioned), LaunchClass::Bound);
        assert_eq!(resolve_class("apply", &dev), LaunchClass::DevCandidate);
        assert_eq!(resolve_class("apply", &unstamped), LaunchClass::DevCandidate);

        // Read-only verbs are never gated — regardless of the binding, so a
        // versioned-bound deployment with a missing/mismatched tkp can still be
        // described (that is exactly when the operator needs diagnostics).
        for envelope in [&versioned, &dev, &unstamped] {
            assert_eq!(resolve_class("describe", envelope), LaunchClass::ReadOnly);
            assert_eq!(resolve_class("plan", envelope), LaunchClass::ReadOnly);
            assert_eq!(resolve_class("status", envelope), LaunchClass::ReadOnly);
        }
    }

    fn manifest_with(sha: &str) -> IntegrityManifest {
        IntegrityManifest {
            provisioner_version: "1.0.0".to_string(),
            artifacts: vec![BinaryArtifactDescriptor {
                version: "1.0.0".to_string(),
                target: Target("x".to_string()),
                sha256: sha.to_string(),
                retrieval_ref: None,
                size_bytes: 0,
            }],
        }
    }

    #[test]
    fn verify_matches_recorded_checksum_and_rejects_mismatch() {
        let tmp = tempfile::tempdir().unwrap();
        let bin = tmp.path().join("tkp");
        std::fs::write(&bin, b"the-binary-bytes").unwrap();
        let sha = sha256_hex(b"the-binary-bytes");

        let mut env = envelope_with(Some(BuildMode::Versioned));
        env.integrity = Some(manifest_with(&sha));
        verify_against_manifest(&bin, &env).expect("matching checksum passes");

        env.integrity = Some(manifest_with("deadbeef"));
        let err = verify_against_manifest(&bin, &env).expect_err("mismatch aborts");
        assert!(err.to_string().contains("integrity check failed"), "unexpected: {err}");
    }

    #[test]
    fn verify_requires_a_manifest() {
        let tmp = tempfile::tempdir().unwrap();
        let bin = tmp.path().join("tkp");
        std::fs::write(&bin, b"x").unwrap();
        let env = envelope_with(Some(BuildMode::Versioned)); // no integrity
        let err = verify_against_manifest(&bin, &env).expect_err("no manifest aborts");
        assert!(err.to_string().contains("no integrity manifest"), "unexpected: {err}");
    }
}
