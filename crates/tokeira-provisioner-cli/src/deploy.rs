//! `tkp deploy plan|apply` — the workload universe (design §Command behaviour
//! and outputs).
//!
//! The workload verbs are **conditionally realized**: a platform whose workload
//! rides the infra universe (compose models its tokeirad containers as
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
    explanation_path: Option<&Path>,
) -> Result<()> {
    let running = ProvenanceStamp::current(Utc::now());
    let (envelope, _) = envelope_store(deployment_dir)
        .load()
        .await
        .context("failed to load the deployment envelope")?;

    let verdict = check_binding(envelope.binding.as_ref(), &running);
    // Causality's S is gathered before the plan runs its refresh
    // (Requirement 2.3) — see the causality module's isolation rule.
    let gathered = crate::causality::gather_causality(platform, deployment_dir, &envelope).await?;
    match platform.deploy_plan(deployment_dir).await? {
        Realization::NotApplicable { reason } => {
            anyhow::bail!("not applicable: {reason}");
        }
        Realization::Realized(outcome) => {
            let mut explanation = tokeira_explain::explain_plan(
                crate::explain_context(platform, deployment_dir, &envelope, "deploy plan"),
                &outcome,
            );
            tokeira_explain::apply_causality(
                &mut explanation,
                &crate::causality::causality_view(gathered, &outcome),
            );
            let report = crate::render::ExplanationReport {
                initialized: envelope.binding.is_some(),
                binding: verdict,
                explanation,
            };
            // Artifact before report (Req 7.6): the verb fails before
            // claiming anything if the requested artifact cannot be written.
            if let Some(path) = explanation_path {
                tokeira_explain::artifact::write(path, &report.explanation)?;
            }
            crate::emit_report(&tokeira_report::render(&report, mode)?, mode);
            // Non-zero on a platform issue, like `infra plan` — the document
            // is the whole report.
            if !report.explanation.platform_issues.is_empty() {
                return Err(crate::PlatformBlocked.into());
            }
        }
    }
    Ok(())
}

/// Reconcile the workload to desired under the mutating-verb contract.
pub(crate) async fn deploy_apply<P: ProvisionerPlatform>(
    platform: &P,
    deployment_dir: &Path,
    yes: bool,
    mode: tokeira_report::Mode,
    explanation_path: Option<&Path>,
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
    let verdict = match evaluate_gate(envelope.binding.as_ref(), &running) {
        GateOutcome::Refuse { verdict, reason } => {
            anyhow::bail!("binding gate refuses `deploy apply` ({verdict:?}): {reason}");
        }
        // Proceeding verdicts are silent: the gate regime is a standing fact
        // of the deployment (describe's story), not news on every verb. Only
        // a refusal earns narration — and it is the error above.
        GateOutcome::Proceed { verdict, .. } => verdict,
    };

    // ── Destructive gate (§4): identical contract to `infra apply`. The
    // gate's plan doubles as the applied explanation's field evidence
    // (Property 9: never invented, only reused). ──
    let preceding = if yes {
        None
    } else {
        match platform.deploy_plan(deployment_dir).await? {
            Realization::Realized(planned) => {
                crate::apply::refuse_destructive_without_yes("deploy apply", &planned.changes)?;
                Some(planned)
            }
            Realization::NotApplicable { .. } => None,
        }
    };

    // ── Workload apply (realized by the injected platform) ──
    let applied = match platform.deploy_apply(deployment_dir).await? {
        Realization::NotApplicable { reason } => {
            anyhow::bail!("not applicable: {reason}");
        }
        Realization::Realized(outcome) => outcome,
    };
    // Under an open rollback checkpoint, creations join keys(S_B) − keys(S_A)
    // — the set the rollback B-delete pass consumes (task 19.3). The
    // checkpoint consumes the audit vocabulary, not the report identity.
    envelope.record_post_checkpoint_changes(&crate::change_log_entries(&applied.changes));

    // ── Re-stamp: a workload apply advances the config revision like any apply ──
    let from_revision = envelope.config_revision;
    let config_source = platform.config_source(deployment_dir)?;
    restamp_applied_revision(&mut envelope, running, deployment_dir, &config_source)?;
    envelope.stamp_current_schema();
    store
        .save(&envelope, &version)
        .await
        .context("failed to persist the deployment envelope after deploy apply")?;
    let mut context = crate::explain_context(platform, deployment_dir, &envelope, "deploy apply");
    context.current_revision = from_revision;
    context.proposed_revision = Some(envelope.config_revision);
    let explanation = tokeira_explain::explain_applied(
        context,
        &crate::committed_changes(&applied),
        preceding.as_ref(),
    );
    // Artifact after the state record is safe, before the verb claims
    // success — same contract as `infra apply` (Req 7.6).
    if let Some(path) = explanation_path {
        tokeira_explain::artifact::write(path, &explanation)
            .context("the apply is committed and recorded; only the explanation artifact failed")?;
    }
    let report = crate::render::ExplanationReport {
        initialized: true,
        binding: verdict,
        explanation,
    };
    crate::emit_report(&tokeira_report::render(&report, mode)?, mode);
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
        let err = deploy_apply(
            &TestPlatform,
            tmp.path(),
            false,
            tokeira_report::Mode::resolve(false, false),
            None,
        )
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

        deploy_apply(
            &TestPlatform,
            tmp.path(),
            false,
            tokeira_report::Mode::resolve(false, false),
            None,
        )
        .await
        .expect("deploy apply proceeds under DevIterate");

        let (after, _) = store.load().await.unwrap();
        assert_eq!(after.config_revision, 2, "workload apply is a config apply");
    }
}
