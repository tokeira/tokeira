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
use tokeira_iac::ModuleSelection;
use tokeira_local_deployment::{LocalConfig, LocalDeployment};
use tokeira_orchestrator::InfraEngine;
use tokeira_provisioner::ProvenanceStamp;

use crate::envelope_store;
use crate::gate::{GateOutcome, evaluate_gate};

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

    // ── Engine apply ──
    let (change_count, config) = run_local_infra_apply(deployment_dir).await?;
    println!("infra apply: {change_count} change(s)");

    // ── Re-stamp the envelope ──
    // A config apply keeps the engine identity and advances the config revision
    // (task 14.2): record the effective config ref and bump `config_revision`.
    if envelope.deployment_id.is_empty() {
        envelope.deployment_id = config.project_name.clone();
    }
    envelope.binding = Some(running);
    envelope.config_revision += 1;
    envelope.effective_config_ref = Some(config_ref(deployment_dir));
    store
        .save(&envelope, &version)
        .await
        .context("failed to persist the deployment envelope after apply")?;
    println!(
        "envelope: config_revision now {} (config {})",
        envelope.config_revision,
        envelope.effective_config_ref.as_deref().unwrap_or("default")
    );
    Ok(())
}

/// A content ref for the effective configuration — a SHA-256 of the deployment's
/// config file, so a given config revision is identifiable (and revertable to;
/// task 14.3). Absent config falls back to `"default"`.
pub(crate) fn config_ref(deployment_dir: &Path) -> String {
    match std::fs::read(deployment_dir.join("deployment.toml")) {
        Ok(bytes) => format!("sha256:{}", tokeira_provisioner::sha256_hex(&bytes)),
        Err(_) => "default".to_string(),
    }
}

/// Run the local platform's infrastructure apply and return `(change_count,
/// config)`. Local has no infra modules, so this is an empty plan that still
/// exercises the real `InfraEngine` and the `DeploymentStore` state seam. Shared
/// by `apply` and `upgrade`.
pub(crate) async fn run_local_infra_apply(
    deployment_dir: &Path,
) -> Result<(usize, LocalConfig)> {
    let config = load_local_config(deployment_dir)?;
    let mut engine = InfraEngine::new(LocalDeployment, &config, deployment_dir)
        .await
        .context("failed to open the infrastructure engine")?;
    let composition = engine.compose(ModuleSelection::All)?;
    let changes = engine
        .apply(&composition, ModuleSelection::All)
        .await
        .context("infrastructure apply failed")?;
    Ok((changes.len(), config))
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

        apply(tmp.path()).await.expect("apply proceeds under DevIterate");

        let (after, _) = store.load().await.unwrap();
        assert_eq!(after.config_revision, 5, "config_revision advanced by one");
        assert!(after.binding.is_some(), "envelope re-stamped");
        assert_eq!(after.deployment_id, "tokeira", "id defaulted from config");
    }
}
