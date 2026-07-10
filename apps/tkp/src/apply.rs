//! `tkp apply` — apply the deployment, gated on the binding (task 8.3).
//!
//! The binding gate runs *before* any provider mutation: a versioned deployment
//! refuses on any non-`Match` verdict; a dev deployment takes the permissive
//! `DevIterate` path with a warning. On success the deployment envelope is
//! re-stamped with the running binding and its `config_revision` advances.
//!
//! This first increment wires the **local** platform (no infra modules → an
//! empty, no-op plan that still exercises the real `InfraEngine` and the
//! `DeploymentStore` state seam). The full multi-platform dispatch and the
//! service/runtime apply land next.

use std::path::Path;

use anyhow::{Context, Result};
use chrono::Utc;
use tokeira_local_deployment::LocalConfig;
use tokeira_provisioner::ProvenanceStamp;

use crate::{
    config_history, envelope_store,
    gate::{GateOutcome, evaluate_gate},
    platform,
};

pub async fn apply(deployment_dir: &Path) -> Result<()> {
    let running = ProvenanceStamp::current(Utc::now());
    let store = envelope_store(deployment_dir);
    let (mut envelope, version) = store
        .load()
        .await
        .context("failed to load the deployment envelope")?;

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

    // ── Engine apply (dispatched by platform: local | compose-syn) ──
    let resolved = platform::detect(deployment_dir);
    // The deployment identity seeds the compose-syn `Cx`; it was set at `init`.
    let project_name = deployment_identity(&envelope.deployment_id);
    let change_count = platform::infra_apply(deployment_dir, &project_name).await?;
    println!(
        "[{}] infra apply: {change_count} change(s)",
        resolved.label()
    );

    // ── Re-stamp the envelope ──
    // A config apply keeps the engine identity and advances the config revision
    // (task 14.2): record the effective config ref and bump `config_revision`.
    if envelope.deployment_id.is_empty() {
        envelope.deployment_id = project_name;
    }
    envelope.binding = Some(running);
    envelope.config_revision += 1;
    envelope.effective_config_ref = Some(config_ref(deployment_dir));
    // Retain this revision's config source so a later `revert` can re-apply it
    // (task 14.3). Best-effort: a config-less local deployment has nothing to keep.
    config_history::snapshot(deployment_dir, envelope.config_revision)
        .context("failed to retain the applied config revision")?;
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

/// The deployment identity used to seed the compose-syn `Cx`, defaulting when the
/// envelope has not recorded one yet.
pub(crate) fn deployment_identity(recorded: &str) -> String {
    if recorded.is_empty() {
        "tokeira".to_string()
    } else {
        recorded.to_string()
    }
}

/// A content ref for the effective configuration — a SHA-256 of the deployment's
/// config source (the `.tkd` for compose-syn, `deployment.toml` for local), so a
/// given config revision is identifiable (and revertable to; task 14.3). Absent
/// config falls back to `"default"`.
pub(crate) fn config_ref(deployment_dir: &Path) -> String {
    let config_file = match platform::detect(deployment_dir) {
        platform::Platform::ComposeSyn => deployment_dir.join("definition.tkd"),
        platform::Platform::Local => deployment_dir.join("deployment.toml"),
    };
    match std::fs::read(&config_file) {
        Ok(bytes) => format!("sha256:{}", tokeira_provisioner::sha256_hex(&bytes)),
        Err(_) => "default".to_string(),
    }
}

pub(crate) fn load_local_config(deployment_dir: &Path) -> Result<LocalConfig> {
    let path = deployment_dir.join("deployment.toml");
    if path.exists() {
        tokeira_config::load_config(&path, None)
            .with_context(|| format!("failed to load {}", path.display()))
    } else {
        // First increment: a missing config falls back to defaults so the flow
        // is exercisable; a real deployment carries a `deployment.toml`.
        Ok(LocalConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_provisioner::DeploymentStateEnvelope;

    #[tokio::test]
    async fn apply_refuses_an_unstamped_deployment() {
        // No envelope → binding Unknown → refuse before any mutation (Day-0
        // stamping happens at `create`, so an unstamped deployment at apply time
        // is unverifiable).
        let tmp = tempfile::tempdir().unwrap();
        let err = apply(tmp.path())
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

        apply(tmp.path())
            .await
            .expect("apply proceeds under DevIterate");

        let (after, _) = store.load().await.unwrap();
        assert_eq!(after.config_revision, 5, "config_revision advanced by one");
        assert!(after.binding.is_some(), "envelope re-stamped");
        assert_eq!(after.deployment_id, "tokeira", "id defaulted from config");
    }
}
