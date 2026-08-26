//! `tkp deploy plan|apply|destroy` — the workload universe.
//!
//! The verbs drive the deploy engine over the definition's service plane:
//! desired manifests hashed against recorded runtime state, reconciled per
//! service. The plane is empty until the service split realizes `.service(`
//! nodes, so today the verbs honestly reconcile nothing — and become real
//! the moment the set fills. `deploy apply` follows the same mutating-verb
//! contract as `infra apply` — gate before any mutation, then a
//! config-revision advance on success.
//!
//! The service plane has no field-level explanation model yet (that machinery
//! is the infra plane's), but its typed Delta still renders through the shared
//! operator report contract. An `--explanation` artifact request is refused
//! rather than populated with a mislabeled infra model.

use std::path::Path;

use anyhow::{Context, Result};
use chrono::Utc;
use tokeira_deployment::ProvenanceStamp;
use tokeira_orchestrator::{ServiceChange, ServiceChangeKind};
use tokeira_platform::definition::DefinitionFrontend;

use crate::{
    apply::restamp_applied_revision,
    engine::{Engine, ServiceOperationError},
    envelope_store,
    gate::{GateOutcome, evaluate_gate},
    platform::Admitted,
    render::{ServiceFailureReport, ServiceReport},
};

/// Workload plan: the per-service Delta after resolving manifest-directed
/// platform prerequisites. Image cache population may occur, but running
/// workloads and deployment state remain untouched.
pub(crate) async fn deploy_plan<F: DefinitionFrontend>(
    engine: &Engine<F>,
    admitted: &Admitted,
    mode: tokeira_report::Mode,
    explanation_path: Option<&Path>,
) -> Result<()> {
    refuse_explanation(explanation_path)?;
    let (envelope, _) = envelope_store(&admitted.deployment_ref.dir)
        .load()
        .await
        .context("failed to load the deployment envelope")?;
    let changes = match engine.deploy_plan(admitted).await {
        Ok(changes) => changes,
        Err(error) => {
            return emit_service_failure(
                "deploy plan",
                &admitted.deployment_ref.name,
                envelope.config_revision,
                mode,
                error,
            );
        }
    };
    let report = ServiceReport::plan(
        &admitted.deployment_ref.name,
        envelope.config_revision,
        &changes,
    );
    crate::emit_report(&tokeira_report::render(&report, mode)?, mode);
    Ok(())
}

/// Reconcile the workload to desired under the mutating-verb contract.
pub(crate) async fn deploy_apply<F: DefinitionFrontend>(
    engine: &Engine<F>,
    admitted: &Admitted,
    yes: bool,
    mode: tokeira_report::Mode,
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
        let planned = match engine.deploy_plan(admitted).await {
            Ok(changes) => changes,
            Err(error) => {
                return emit_service_failure(
                    "deploy apply",
                    &admitted.deployment_ref.name,
                    envelope.config_revision,
                    mode,
                    error,
                );
            }
        };
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
    let changes = match engine.deploy_apply(admitted).await {
        Ok(changes) => changes,
        Err(error) => {
            return emit_service_failure(
                "deploy apply",
                &admitted.deployment_ref.name,
                envelope.config_revision,
                mode,
                error,
            );
        }
    };

    // ── Re-stamp: a workload apply advances the config revision like any apply ──
    let from_revision = envelope.config_revision;
    let config_source = admitted.config_source();
    restamp_applied_revision(&mut envelope, running, deployment_dir, &config_source)?;
    envelope.stamp_current_schema();
    store
        .save(&envelope, &version)
        .await
        .context("failed to persist the deployment envelope after deploy apply")?;
    let report = ServiceReport::applied(
        &admitted.deployment_ref.name,
        from_revision,
        envelope.config_revision,
        &changes,
    );
    crate::emit_report(&tokeira_report::render(&report, mode)?, mode);
    Ok(())
}

/// Remove the workload plane without touching its substrate or advancing the
/// configuration revision.
pub(crate) async fn deploy_destroy<F: DefinitionFrontend>(
    engine: &Engine<F>,
    admitted: &Admitted,
    yes: bool,
    mode: tokeira_report::Mode,
) -> Result<()> {
    let deployment_dir = admitted.deployment_ref.dir.as_path();
    let running = ProvenanceStamp::current(Utc::now());
    let (envelope, _) = envelope_store(deployment_dir)
        .load()
        .await
        .context("failed to load the deployment envelope")?;

    crate::marker::refuse_if_marked(&envelope, "deploy destroy")?;
    match evaluate_gate(envelope.binding.as_ref(), &running) {
        GateOutcome::Refuse { verdict, reason } => {
            anyhow::bail!("binding gate refuses `deploy destroy` ({verdict:?}): {reason}");
        }
        GateOutcome::Proceed { .. } => {}
    }
    if !yes {
        anyhow::bail!(
            "`deploy destroy` removes all of the deployment's services and is irreversible; \
             re-run with `--yes` to confirm"
        );
    }

    let changes = match engine.deploy_destroy(admitted).await {
        Ok(changes) => changes,
        Err(error) => {
            return emit_service_failure(
                "deploy destroy",
                &admitted.deployment_ref.name,
                envelope.config_revision,
                mode,
                error,
            );
        }
    };
    let report = ServiceReport::destroyed(
        &admitted.deployment_ref.name,
        envelope.config_revision,
        &changes,
    );
    crate::emit_report(&tokeira_report::render(&report, mode)?, mode);
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

/// Render the typed platform evidence once and replace the ordinary error
/// chain with the post-report marker. Non-image failures retain their
/// existing propagation until their owning layers define equivalent models.
fn emit_service_failure(
    operation: &'static str,
    deployment: &str,
    current_revision: u64,
    mode: tokeira_report::Mode,
    error: ServiceOperationError,
) -> Result<()> {
    let Some(failure) = error.service_image_issue().cloned() else {
        let context = match operation {
            "deploy plan" => "service plan failed",
            "deploy apply" => "service apply failed",
            "deploy destroy" => "service destroy failed",
            _ => "service operation failed",
        };
        return Err(error.into_anyhow(context));
    };
    let report = ServiceFailureReport::new(operation, deployment, current_revision, failure);
    crate::emit_report(&tokeira_report::render(&report, mode)?, mode);
    Err(crate::ReportEmitted.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_deployment::DeploymentStateEnvelope;

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

    #[test]
    fn image_failure_remains_typed_at_the_service_operation_boundary() {
        let issue = tokeira_deploy_engine::ServiceImageIssue {
            service: "grafana".to_string(),
            image: "missing:1".to_string(),
            kind: tokeira_deploy_engine::ServiceImageIssueKind::Pull,
            evidence: "manifest unknown".to_string(),
            direction: None,
        };
        let error = ServiceOperationError::from(tokeira_orchestrator::OrchestratorError::Deploy(
            tokeira_deploy_engine::RuntimeError::from(issue),
        ));

        let failure = error
            .service_image_issue()
            .expect("typed evidence crosses the orchestrator unchanged");
        assert_eq!(failure.service, "grafana");
        assert_eq!(failure.evidence, "manifest unknown");
    }
}
