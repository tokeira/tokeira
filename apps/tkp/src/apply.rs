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
    let config = load_local_config(deployment_dir)?;
    let mut engine = InfraEngine::new(LocalDeployment, &config, deployment_dir)
        .await
        .context("failed to open the infrastructure engine")?;
    let composition = engine.compose(ModuleSelection::All)?;
    let changes = engine
        .apply(&composition, ModuleSelection::All)
        .await
        .context("infrastructure apply failed")?;
    println!("infra apply: {} change(s)", changes.len());

    // ── Re-stamp the envelope ──
    // A config apply keeps the engine identity and advances the revision.
    if envelope.deployment_id.is_empty() {
        envelope.deployment_id = config.project_name.clone();
    }
    envelope.binding = Some(running);
    envelope.config_revision += 1;
    store
        .save(&envelope, &version)
        .await
        .context("failed to persist the deployment envelope after apply")?;
    println!("envelope: config_revision now {}", envelope.config_revision);
    Ok(())
}

fn load_local_config(deployment_dir: &Path) -> Result<LocalConfig> {
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
