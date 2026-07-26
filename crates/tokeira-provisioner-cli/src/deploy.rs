//! `tkp deploy plan|apply` — the workload universe (design §Command behaviour
//! and outputs).
//!
//! The workload verbs are **conditionally realized**: a platform whose workload
//! rides the infra universe (compose-syn models its tokeirad containers as
//! infra resources) realizes them as the infra verbs; a platform with no
//! workload notion answers [`Realization::NotApplicable`], which the shell turns
//! into a typed non-zero refusal. `deploy apply` follows the same mutating-verb
//! contract as `infra apply` — gate before any mutation, then a config-revision
//! advance on success.

use std::path::Path;

use anyhow::{Context, Result};
use chrono::Utc;
use tokeira_provisioner::{ProvenanceStamp, check_binding};

use crate::{
    ProvisionerPlatform, Realization,
    apply::restamp_applied_revision,
    envelope_store,
    gate::{GateOutcome, evaluate_gate},
};

/// Read-only workload plan: binding verdict (annotates, never refuses) + the
/// workload Delta.
pub(crate) async fn deploy_plan<P: ProvisionerPlatform>(
    platform: &P,
    deployment_dir: &Path,
    mode: tokeira_report::Mode,
) -> Result<()> {
    let running = ProvenanceStamp::current(Utc::now());
    let (envelope, _) = envelope_store(deployment_dir)
        .load()
        .await
        .context("failed to load the deployment envelope")?;

    let verdict = check_binding(envelope.binding.as_ref(), &running);
    match platform.deploy_plan(deployment_dir).await? {
        Realization::NotApplicable { reason } => {
            anyhow::bail!("not applicable: {reason}");
        }
        Realization::Realized(outcome) => {
            let report = crate::render::PlanReport::new(
                platform.label(deployment_dir).to_string(),
                "deploy plan",
                &envelope,
                verdict,
                outcome.changes,
            );
            print!("{}", tokeira_report::render(&report, mode)?);
        }
    }
    Ok(())
}

/// Reconcile the workload to desired under the mutating-verb contract.
pub(crate) async fn deploy_apply<P: ProvisionerPlatform>(
    platform: &P,
    deployment_dir: &Path,
    yes: bool,
) -> Result<()> {
    let running = ProvenanceStamp::current(Utc::now());
    let store = envelope_store(deployment_dir);
    let (mut envelope, version) = store
        .load()
        .await
        .context("failed to load the deployment envelope")?;

    // ── Operation-marker gate (task 19.4): an interrupted upgrade/rollback
    // is recovered by re-running THAT verb; everything else refuses. ──
    crate::marker::refuse_if_marked(&envelope, "deploy apply")?;

    // ── Gate before any mutation ──
    match evaluate_gate(envelope.binding.as_ref(), &running) {
        GateOutcome::Refuse { verdict, reason } => {
            anyhow::bail!("binding gate refuses `deploy apply` ({verdict:?}): {reason}");
        }
        // Proceeding verdicts are silent: the gate regime is a standing fact
        // of the deployment (describe's story), not news on every verb. Only
        // a refusal earns narration — and it is the error above.
        GateOutcome::Proceed { .. } => {}
    }

    // ── Destructive gate (§4): identical contract to `infra apply`. ──
    if !yes && let Realization::Realized(planned) = platform.deploy_plan(deployment_dir).await? {
        crate::apply::refuse_destructive_without_yes("deploy apply", &planned.changes)?;
    }

    // ── Workload apply (realized by the injected platform) ──
    let applied = match platform.deploy_apply(deployment_dir).await? {
        Realization::NotApplicable { reason } => {
            anyhow::bail!("not applicable: {reason}");
        }
        Realization::Realized(entries) => entries,
    };
    // Under an open rollback checkpoint, creations join keys(S_B) − keys(S_A)
    // — the set the rollback B-delete pass consumes (task 19.3).
    envelope.record_post_checkpoint_changes(&applied);
    println!(
        "[{}] deploy apply: {}",
        platform.label(deployment_dir),
        tokeira_report::counted(applied.len(), "change")
    );

    // ── Re-stamp: a workload apply advances the config revision like any apply ──
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
        .context("failed to persist the deployment envelope after deploy apply")?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TestPlatform;
    use tokeira_provisioner::DeploymentStateEnvelope;

    #[tokio::test]
    async fn deploy_apply_refuses_an_unstamped_deployment() {
        let tmp = tempfile::tempdir().unwrap();
        let err = deploy_apply(&TestPlatform, tmp.path(), false)
            .await
            .expect_err("an unstamped deployment refuses");
        assert!(
            err.to_string().contains("binding gate refuses"),
            "unexpected: {err}"
        );
    }

    #[tokio::test]
    async fn deploy_apply_advances_the_config_revision() {
        let tmp = tempfile::tempdir().unwrap();
        let store = envelope_store(tmp.path());
        let env = DeploymentStateEnvelope {
            binding: Some(ProvenanceStamp::current(Utc::now())),
            config_revision: 1,
            ..Default::default()
        };
        let (_, v) = store.load().await.unwrap();
        store.save(&env, &v).await.unwrap();

        deploy_apply(&TestPlatform, tmp.path(), false)
            .await
            .expect("deploy apply proceeds under DevIterate");

        let (after, _) = store.load().await.unwrap();
        assert_eq!(after.config_revision, 2, "workload apply is a config apply");
    }
}
