//! `tkp deploy plan|apply` — the workload universe.
//!
//! The verbs drive the deploy engine over the definition's service plane:
//! desired manifests hashed against recorded runtime state, reconciled per
//! service. The plane is the provider's projection of the realized
//! definition — the workload partition computed at realization — so the
//! verbs manage exactly the services the definition describes, through the
//! provider's declared applier. `deploy apply` follows the same
//! mutating-verb contract as `infra apply` — gate before any mutation,
//! then a config-revision advance on success.
//!
//! The service plane has no explanation model yet (that machinery is the
//! infra plane's); the verbs render the typed service changes directly and
//! refuse an `--explanation` request rather than mislabel an infra model.

use std::path::Path;

use anyhow::{Context, Result};
use chrono::Utc;
use tokeira_orchestrator::{ServiceChange, ServiceChangeKind};
use tokeira_platform::definition::DefinitionFrontend;
use tokeira_provisioner::ProvenanceStamp;

use crate::{
    apply::restamp_applied_revision,
    engine::Engine,
    envelope_store,
    gate::{GateOutcome, evaluate_gate},
    platform::Admitted,
};

/// Read-only workload plan: the per-service Delta, by manifest hash against
/// recorded runtime state.
pub(crate) async fn deploy_plan<F: DefinitionFrontend>(
    engine: &Engine<F>,
    admitted: &Admitted,
    _mode: tokeira_report::Mode,
    explanation_path: Option<&Path>,
) -> Result<()> {
    refuse_explanation(explanation_path)?;
    let changes = engine.deploy_plan(admitted).await?;
    render_service_changes("deploy plan", &changes);
    Ok(())
}

/// Reconcile the workload to desired under the mutating-verb contract.
pub(crate) async fn deploy_apply<F: DefinitionFrontend>(
    engine: &Engine<F>,
    admitted: &Admitted,
    yes: bool,
    _mode: tokeira_report::Mode,
    explanation_path: Option<&Path>,
) -> Result<()> {
    refuse_explanation(explanation_path)?;
    let deployment_dir = admitted.deployment_ref.dir.as_path();
    let running = ProvenanceStamp::current(Utc::now());
    let store = envelope_store(deployment_dir);
    let (mut envelope, version) = store
        .load()
        .await
        .context("failed to load the deployment envelope")?;

    // ── Operation-marker gate: an interrupted upgrade/rollback
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

    // ── Retarget gate: identical contract to `infra apply`. ──
    crate::retarget_gate(engine, admitted, &envelope).await?;

    // ── Destructive gate (§4), plane-correct: a torn-down service is the
    // destructive class here. ──
    if !yes {
        let planned = engine.deploy_plan(admitted).await?;
        let destructive: Vec<&ServiceChange> = planned
            .iter()
            .filter(|change| matches!(change.kind, ServiceChangeKind::Delete))
            .collect();
        if !destructive.is_empty() {
            let mut lines = String::new();
            for change in &destructive {
                lines.push_str(&format!("\n  - {}", change.service));
            }
            anyhow::bail!(
                "deploy apply: refusing — the plan tears down {}:{lines}\nre-run with `--yes` to proceed",
                tokeira_report::counted(destructive.len(), "service"),
            );
        }
    }

    // ── Service apply, through the deploy engine ──
    let changes = engine.deploy_apply(admitted).await?;
    render_service_changes("deploy apply", &changes);

    // ── Re-stamp: a workload apply advances the config revision like any apply ──
    let from_revision = envelope.config_revision;
    let config_source = admitted.config_source();
    restamp_applied_revision(&mut envelope, running, deployment_dir, &config_source)?;
    envelope.stamp_current_schema();
    store
        .save(&envelope, &version)
        .await
        .context("failed to persist the deployment envelope after deploy apply")?;
    println!("revision {} → {}", from_revision, envelope.config_revision);
    Ok(())
}

/// The service plane's explanation model arrives with its realizers; until
/// then the request is refused rather than answered with a mislabeled infra
/// model.
fn refuse_explanation(explanation_path: Option<&Path>) -> Result<()> {
    if explanation_path.is_some() {
        anyhow::bail!(
            "the workload verbs carry no explanation model yet; run without `--explanation`"
        );
    }
    Ok(())
}

/// The typed service Delta, rendered directly: one line per changed
/// service, and an honest "nothing" when the plane is empty or steady.
fn render_service_changes(verb: &str, changes: &[ServiceChange]) {
    let changed: Vec<&ServiceChange> = changes
        .iter()
        .filter(|change| !matches!(change.kind, ServiceChangeKind::NoChange))
        .collect();
    if changed.is_empty() {
        println!("{verb}: no service changes (the declared service set is steady)");
        return;
    }
    for change in &changed {
        let glyph = match change.kind {
            ServiceChangeKind::Create => "+",
            ServiceChangeKind::Update => "~",
            ServiceChangeKind::Delete => "-",
            ServiceChangeKind::NoChange => unreachable!("filtered above"),
        };
        println!("  {glyph} {}", change.service);
    }
    println!(
        "{verb}: {}",
        tokeira_report::counted(changed.len(), "service change")
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_provisioner::DeploymentStateEnvelope;

    #[tokio::test]
    async fn deploy_apply_refuses_an_unstamped_deployment() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, admitted) = crate::testkit::engine(tmp.path());
        let err = deploy_apply(
            &engine,
            &admitted,
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

        let (engine, admitted) = crate::testkit::engine(tmp.path());
        deploy_apply(
            &engine,
            &admitted,
            false,
            tokeira_report::Mode::resolve(false, false),
            None,
        )
        .await
        .expect("deploy apply proceeds under DevIterate");

        let (after, _) = store.load().await.unwrap();
        assert_eq!(after.config_revision, 2, "workload apply is a config apply");
    }

    // The explanation model belongs to the plane's realizers; until they
    // arrive the request refuses rather than mislabeling an infra model.
    #[tokio::test]
    async fn an_explanation_request_refuses_honestly() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, admitted) = crate::testkit::engine(tmp.path());
        let err = deploy_plan(
            &engine,
            &admitted,
            tokeira_report::Mode::resolve(false, false),
            Some(std::path::Path::new("/tmp/x.json")),
        )
        .await
        .expect_err("no explanation model on the service plane yet");
        assert!(err.to_string().contains("no explanation model"), "{err}");
    }
}
