//! `tkp apply` — apply the deployment, gated on the binding (task 8.3).
//!
//! The binding gate runs *before* any provider mutation: a versioned deployment
//! refuses on any non-`Match` verdict; a dev deployment takes the permissive
//! `DevIterate` path with a warning. On success the deployment envelope is
//! re-stamped with the running binding and its `config_revision` advances.

use std::path::Path;

use anyhow::{Context, Result};
use chrono::Utc;
use tokeira_provisioner::ProvenanceStamp;

use crate::{
    ProvisionerPlatform, config_history, envelope_store,
    gate::{GateOutcome, evaluate_gate},
};

pub(crate) async fn apply<P: ProvisionerPlatform>(
    platform: &P,
    deployment_dir: &Path,
) -> Result<()> {
    let running = ProvenanceStamp::current(Utc::now());
    let store = envelope_store(deployment_dir);
    let (mut envelope, version) = store
        .load()
        .await
        .context("failed to load the deployment envelope")?;

    // ── Operation-marker gate (task 19.4): an interrupted upgrade/rollback
    // is recovered by re-running THAT verb; everything else refuses. ──
    crate::marker::refuse_if_marked(&envelope, "apply")?;

    // ── Gate before any mutation ──
    match evaluate_gate(envelope.binding.as_ref(), &running) {
        GateOutcome::Refuse { verdict, reason } => {
            anyhow::bail!("binding gate refuses `apply` ({verdict:?}): {reason}");
        }
        GateOutcome::Proceed {
            verdict,
            authoritative: true,
        } => {
            println!("binding: {verdict:?} (authoritative) — proceeding");
        }
        GateOutcome::Proceed {
            verdict,
            authoritative: false,
        } => {
            eprintln!("warning: {verdict:?} — dev iteration, advisory (not authoritative)");
        }
    }

    // ── Engine apply (realized by the injected platform) ──
    // The deployment identity seeds the platform context; it was set at `init`.
    let project_name = deployment_identity(&envelope.deployment_id);
    let applied = platform.infra_apply(deployment_dir).await?;
    // Under an open rollback checkpoint, creations join keys(S_B) − keys(S_A)
    // — the set the rollback B-delete pass consumes (task 19.3).
    envelope.record_post_checkpoint_changes(&applied);
    println!(
        "[{}] infra apply: {} change(s)",
        applied.len(),
        platform.label(deployment_dir)
    );

    // ── Re-stamp the envelope ──
    // A config apply keeps the engine identity and advances the config revision
    // (task 14.2): record the effective config ref and bump `config_revision`.
    if envelope.deployment_id.is_empty() {
        envelope.deployment_id = project_name;
    }
    restamp_applied_revision(
        &mut envelope,
        running,
        deployment_dir,
        platform.config_basename(deployment_dir),
    )?;
    envelope.stamp_current_schema();
    store
        .save(&envelope, &version)
        .await
        .context("failed to persist the deployment envelope after apply")?;
    println!(
        "envelope: config_revision now {} (config {})",
        envelope.config_revision,
        envelope
            .effective_config_ref
            .as_deref()
            .unwrap_or("default")
    );
    Ok(())
}

/// Advance the envelope to a new applied config revision: re-stamp the binding,
/// bump `config_revision`, record the effective-config ref, and retain the
/// revision's config source (task 14.2/14.3). Shared by every verb that applies
/// configuration (`apply`, `deploy apply`, `scale`); the caller persists.
pub(crate) fn restamp_applied_revision(
    envelope: &mut tokeira_provisioner::DeploymentStateEnvelope,
    running: ProvenanceStamp,
    deployment_dir: &Path,
    config_basename: &str,
) -> Result<()> {
    envelope.binding = Some(running);
    envelope.config_revision += 1;
    envelope.effective_config_ref = Some(config_ref(deployment_dir, config_basename));
    // Best-effort retention: a config-less local deployment has nothing to keep.
    config_history::snapshot(deployment_dir, config_basename, envelope.config_revision)
        .context("failed to retain the applied config revision")
}

/// The deployment identity used to seed the platform context, defaulting when the
/// envelope has not recorded one yet.
pub(crate) fn deployment_identity(recorded: &str) -> String {
    if recorded.is_empty() {
        "tokeira".to_string()
    } else {
        recorded.to_string()
    }
}

/// A content ref for the effective configuration — a SHA-256 of the deployment's
/// config source (the platform's `config_basename`), so a given config revision
/// is identifiable (and revertable to; task 14.3). Absent config falls back to
/// `"default"`.
pub(crate) fn config_ref(deployment_dir: &Path, config_basename: &str) -> String {
    let config_file = deployment_dir.join(config_basename);
    match std::fs::read(&config_file) {
        Ok(bytes) => format!("sha256:{}", tokeira_provisioner::sha256_hex(&bytes)),
        Err(_) => "default".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TestPlatform;
    use tokeira_provisioner::DeploymentStateEnvelope;

    // Task 19.4: while an upgrade/rollback marker is open, `apply` refuses —
    // recovery goes through the interrupted verb, never around it.
    #[tokio::test]
    async fn apply_refuses_while_an_operation_marker_is_open() {
        use chrono::Utc;
        use tokeira_provisioner::ProvenanceStamp;

        let tmp = tempfile::tempdir().unwrap();
        let store = crate::envelope_store(tmp.path());
        // A dev binding that would otherwise DevIterate straight through.
        let running = ProvenanceStamp::current(Utc::now());
        let mut env = DeploymentStateEnvelope {
            binding: Some(running.clone()),
            ..Default::default()
        };
        env.begin_upgrade(running, "op-open", Utc::now());
        let (_, v) = store.load().await.unwrap();
        store.save(&env, &v).await.unwrap();

        let err = apply(&TestPlatform, tmp.path())
            .await
            .expect_err("the open marker gates apply");
        assert!(err.to_string().contains("in flight"), "unexpected: {err}");
    }

    #[tokio::test]
    async fn apply_refuses_an_unstamped_deployment() {
        // No envelope → binding Unknown → refuse before any mutation (Day-0
        // stamping happens at `create`, so an unstamped deployment at apply time
        // is unverifiable).
        let tmp = tempfile::tempdir().unwrap();
        let err = apply(&TestPlatform, tmp.path())
            .await
            .expect_err("an unstamped deployment refuses");
        assert!(
            err.to_string().contains("binding gate refuses"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn apply_proceeds_on_dev_iterate_and_restamps() {
        let tmp = tempfile::tempdir().unwrap();
        let store = envelope_store(tmp.path());

        // Pre-stamp (simulating `create`) with the running dev identity. Dev +
        // dev binary → DevIterate → proceeds.
        let recorded = ProvenanceStamp::current(Utc::now());
        let env = DeploymentStateEnvelope {
            binding: Some(recorded),
            config_revision: 4,
            ..Default::default()
        };
        let (_, v) = store.load().await.unwrap();
        store.save(&env, &v).await.unwrap();

        apply(&TestPlatform, tmp.path())
            .await
            .expect("apply proceeds under DevIterate");

        let (after, _) = store.load().await.unwrap();
        assert_eq!(after.config_revision, 5, "config_revision advanced by one");
        assert!(after.binding.is_some(), "envelope re-stamped");
        assert_eq!(after.deployment_id, "tokeira", "id defaulted from config");
    }
}
