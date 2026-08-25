//! `tkp apply` — apply the deployment, gated on the binding.
//!
//! The binding gate runs *before* any provider mutation: a versioned deployment
//! refuses on any non-`Match` verdict; a dev deployment takes the permissive
//! `DevIterate` path with a warning. On success the deployment envelope is
//! re-stamped with the running binding and its `config_revision` advances.

use std::path::Path;

use anyhow::{Context, Result};
use chrono::Utc;
use tokeira_deployment::ProvenanceStamp;
use tokeira_report::Mode;

use tokeira_platform::definition::DefinitionFrontend;

use crate::{
    config_history,
    engine::Engine,
    envelope_store,
    gate::{GateOutcome, evaluate_gate},
    platform::Admitted,
    render::ExplanationReport,
};

pub(crate) async fn apply<F: DefinitionFrontend>(
    engine: &Engine<F>,
    admitted: &Admitted,
    module: Option<&str>,
    yes: bool,
    mode: Mode,
    explanation_path: Option<&Path>,
) -> Result<()> {
    let deployment_dir = admitted.deployment_ref.dir.as_path();
    let running = ProvenanceStamp::current(Utc::now());
    let store = envelope_store(deployment_dir);
    let (mut envelope, version) = store
        .load()
        .await
        .context("failed to load the deployment envelope")?;

    // ── Operation-marker gate: an interrupted upgrade/rollback
    // is recovered by re-running THAT verb; everything else refuses. ──
    crate::marker::refuse_if_marked(&envelope, "apply")?;

    // ── Gate before any mutation ──
    let verdict = match evaluate_gate(envelope.binding.as_ref(), &running) {
        GateOutcome::Refuse { verdict, reason } => {
            anyhow::bail!("binding gate refuses `apply` ({verdict:?}): {reason}");
        }
        // Proceeding verdicts are silent: the gate regime is a standing fact
        // of the deployment (describe's story), not news on every verb. Only
        // a refusal earns narration — and it is the error above.
        GateOutcome::Proceed { verdict, .. } => verdict,
    };

    // ── Retarget gate: a `#[create]` change is a new deployment, not an
    // apply — refused absolutely, before even the destructive-plan review. ──
    crate::retarget_gate(engine, admitted, &envelope).await?;

    // ── Destructive gate (§4): the engine classifies, the
    // shell confirms. Skipped under `--yes` — the operator has already
    // reviewed — so the confirmed path pays no extra plan pass. The gate's
    // plan doubles as the applied explanation's field evidence; under
    // `--yes` there is none, and the explanation says so rather than
    // inventing before-images: field evidence is only ever reused from a
    // real plan. ──
    let preceding = if yes {
        None
    } else {
        let planned = engine.plan(admitted, module).await?;
        refuse_destructive_without_yes("infra apply", &planned.changes)?;
        Some(planned)
    };

    // ── Engine apply ──
    // The deployment identity seeds the context; creation recorded it before
    // the deployment directory became visible.
    let project_name = deployment_identity(&envelope.deployment_id);
    let applied = engine.apply(admitted, module).await?;
    // Under an open rollback checkpoint, creations join keys(S_B) − keys(S_A)
    // — the set the rollback B-delete pass consumes. The
    // checkpoint consumes the audit vocabulary, not the report identity.
    envelope.record_post_checkpoint_changes(&crate::change_log_entries(&applied.changes));
    // The definitive story first: resolved writeback lands in tokeirad.toml
    // before the re-stamp, so the retained revision snapshots the
    // post-writeback document.
    crate::persist_writeback(deployment_dir, &applied.writeback)?;

    // ── Re-stamp the envelope ──
    // A config apply keeps the engine identity and advances the config revision
    //: record the effective config ref and bump `config_revision`.
    if envelope.deployment_id.is_empty() {
        envelope.deployment_id = project_name;
    }
    // The advance the report states: the revision the verb started from and
    // the one the re-stamp commits.
    let from_revision = envelope.config_revision;
    let config_source = admitted.config_source();
    restamp_applied_revision(&mut envelope, running, deployment_dir, &config_source)?;
    envelope.stamp_current_schema();
    store
        .save(&envelope, &version)
        .await
        .context("failed to persist the deployment envelope after apply")?;
    // ── Post-commit publication: a derived projection of the committed
    // state; its failure reports a remedy and never alters the outcome. ──
    crate::publication::publish_committed_transition(
        engine,
        admitted,
        tokeira_deployment::repository::claim::Transition::Apply,
        envelope.config_revision,
    )
    .await;
    let mut context = crate::explain_context(engine, admitted, &envelope, "infra apply");
    context.current_revision = from_revision;
    context.proposed_revision = Some(envelope.config_revision);
    let explanation = tokeira_explain::explain_applied(
        context,
        &crate::committed_changes(&applied),
        preceding.as_ref(),
    );
    // The artifact is written only after the state record is safe: a failed
    // write must fail the verb without ever costing the envelope
    // its revision advance. The context states the one fact the operator
    // must not misread — the apply itself is committed and recorded.
    config_history::retain_explanation(deployment_dir, envelope.config_revision, &explanation)
        .context("the apply is committed and recorded; only explanation retention failed")?;
    if let Some(path) = explanation_path {
        tokeira_explain::artifact::write(path, &explanation)
            .context("the apply is committed and recorded; only the explanation artifact failed")?;
    }
    let report = ExplanationReport {
        initialized: true,
        binding: verdict,
        explanation,
    };
    crate::emit_report(&tokeira_report::render(&report, mode)?, mode);
    Ok(())
}

/// Refuse a destructive plan without `--yes`: name the destructive changes
/// (the evidence) and the remedy. Shared by `infra apply` and `deploy apply`.
pub(crate) fn refuse_destructive_without_yes(
    verb: &str,
    planned: &[tokeira_iac::Change],
) -> Result<()> {
    let destructive = tokeira_iac::destructive_changes(planned);
    if destructive.is_empty() {
        return Ok(());
    }
    let mut lines = String::new();
    for change in &destructive {
        let glyph = match change.kind {
            tokeira_iac::ChangeKind::Replace => tokeira_report::symbol::REPLACE,
            _ => tokeira_report::symbol::DELETE,
        };
        lines.push_str(&format!(
            "\n  {glyph} {}::{}  ({})",
            change.module, change.resource, change.resource_type
        ));
    }
    anyhow::bail!(
        "{verb}: refusing — the plan is destructive: {}{lines}\nre-run with `--yes` to proceed",
        tokeira_report::counted(destructive.len(), "destructive change"),
    );
}

/// Advance the envelope to a new applied config revision: re-stamp the binding,
/// bump `config_revision`, record the effective-config ref, and retain the
/// revision's config source. Shared by every verb that applies
/// configuration (`apply`, `deploy apply`, `scale`); the caller persists.
pub(crate) fn restamp_applied_revision(
    envelope: &mut tokeira_deployment::DeploymentStateEnvelope,
    running: ProvenanceStamp,
    deployment_dir: &Path,
    config_source: &crate::ConfigSource,
) -> Result<()> {
    envelope.binding = Some(running);
    envelope.config_revision += 1;
    envelope.effective_config_ref = Some(config_ref(deployment_dir, config_source));
    // Best-effort retention: a config-less local deployment has nothing to keep.
    config_history::snapshot(deployment_dir, config_source, envelope.config_revision)
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
/// recorded config source (format plus safe deployment-relative path), so a given config revision
/// is identifiable (and revertable to). Absent config falls back to
/// `"default"`.
pub(crate) fn config_ref(deployment_dir: &Path, config_source: &crate::ConfigSource) -> String {
    let config_file = config_history::config_file(deployment_dir, config_source);
    match std::fs::read(&config_file) {
        Ok(bytes) => format!("sha256:{}", tokeira_deployment::sha256_hex(&bytes)),
        Err(_) => "default".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_deployment::DeploymentStateEnvelope;

    #[tokio::test]
    async fn a_retained_revision_and_a_refusing_frontend_stop_apply_before_mutation() {
        let tmp = tempfile::tempdir().unwrap();
        let store = envelope_store(tmp.path());
        let env = DeploymentStateEnvelope {
            binding: Some(ProvenanceStamp::current(Utc::now())),
            config_revision: 1,
            ..Default::default()
        };
        let (_, v) = store.load().await.unwrap();
        store.save(&env, &v).await.unwrap();

        let (engine, admitted) = crate::testkit::engine_over(
            tmp.path(),
            crate::testkit::FixedProbe(None),
            crate::testkit::StubFrontend::refusing_retarget(vec![
                "Config.storage: dsql -> in-memory".to_string(),
            ]),
        );
        // Retain revision 1 so the gate has a prior to compare against —
        // keyed by the recorded source the admitted metadata names.
        let source = admitted.config_source();
        config_history::snapshot(tmp.path(), &source, 1).unwrap();

        let err = apply(
            &engine,
            &admitted,
            None,
            false,
            Mode::resolve(false, false),
            None,
        )
        .await
        .expect_err("the retarget gate refuses");
        assert!(err.to_string().contains("retarget"), "unexpected: {err}");

        // Refused before any mutation: the revision did not advance.
        let (after, _) = store.load().await.unwrap();
        assert_eq!(after.config_revision, 1, "no revision advance on refusal");
    }

    // Task 19.4: while an upgrade/rollback marker is open, `apply` refuses —
    // recovery goes through the interrupted verb, never around it.
    #[tokio::test]
    async fn apply_refuses_while_an_operation_marker_is_open() {
        use chrono::Utc;
        use tokeira_deployment::ProvenanceStamp;

        let tmp = tempfile::tempdir().unwrap();
        let (engine, admitted) = crate::testkit::engine(tmp.path());
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

        let err = apply(
            &engine,
            &admitted,
            None,
            false,
            Mode::resolve(false, false),
            None,
        )
        .await
        .expect_err("the open marker gates apply");
        assert!(err.to_string().contains("in flight"), "unexpected: {err}");
    }

    // §4: the engine classifies destructive changes; the shell
    // refuses them without `--yes`, naming the evidence and the remedy.
    #[test]
    fn destructive_plans_refuse_without_yes() {
        let destructive = vec![tokeira_iac::Change {
            kind: tokeira_iac::ChangeKind::Delete,
            resource_type: "Service".into(),
            module: "grafana".into(),
            resource: "compose/grafana".into(),
            details: Vec::new(),
        }];
        let err = refuse_destructive_without_yes("infra apply", &destructive)
            .expect_err("destructive without --yes refuses");
        let message = err.to_string();
        assert!(
            message.contains("1 destructive change"),
            "counted: {message}"
        );
        assert!(message.contains("compose/grafana"), "evidence: {message}");
        assert!(message.contains("--yes"), "remedy: {message}");

        let benign = vec![tokeira_iac::Change {
            kind: tokeira_iac::ChangeKind::Update,
            resource_type: "Service".into(),
            module: "tokeirad".into(),
            resource: "compose/tokeirad".into(),
            details: Vec::new(),
        }];
        refuse_destructive_without_yes("infra apply", &benign)
            .expect("a non-destructive plan needs no confirmation");
    }

    #[tokio::test]
    async fn apply_refuses_an_unstamped_deployment() {
        // No envelope → binding Unknown → refuse before any mutation (Day-0
        // stamping happens at `create`, so an unstamped deployment at apply time
        // is unverifiable).
        let tmp = tempfile::tempdir().unwrap();
        let (engine, admitted) = crate::testkit::engine(tmp.path());
        let err = apply(
            &engine,
            &admitted,
            None,
            false,
            Mode::resolve(false, false),
            None,
        )
        .await
        .expect_err("an unstamped deployment refuses");
        assert!(
            err.to_string().contains("binding gate refuses"),
            "unexpected error: {err}"
        );
    }

    // The artifact is the applied explanation,
    // revision advance recorded as the field policy states it (current = the
    // revision the verb started from, proposed = the one it committed),
    // field evidence from the gate's own plan pass (never invented —
    // never invented from state.
    #[tokio::test]
    async fn apply_writes_the_applied_explanation_artifact() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, admitted) = crate::testkit::engine(tmp.path());
        let store = envelope_store(tmp.path());
        let env = DeploymentStateEnvelope {
            binding: Some(ProvenanceStamp::current(Utc::now())),
            config_revision: 4,
            ..Default::default()
        };
        let (_, v) = store.load().await.unwrap();
        store.save(&env, &v).await.unwrap();

        let path = tmp.path().join("explanation.json");
        apply(
            &engine,
            &admitted,
            None,
            false,
            Mode::resolve(false, false),
            Some(&path),
        )
        .await
        .expect("apply succeeds and writes the artifact");

        let model = tokeira_explain::artifact::read(&path).expect("artifact parses alone");
        assert_eq!(model.operation, "infra apply");
        assert_eq!(
            model.current_revision, 4,
            "the revision the verb started from"
        );
        assert_eq!(
            model.proposed_revision,
            Some(5),
            "the artifact records the committed advance"
        );

        // Retention needs no flag: the committed revision's folder holds the
        // same explanation, readable after the fact with no serving process.
        let retained = tokeira_explain::artifact::read(
            &tmp.path().join("state/config-revisions/5/explanation.json"),
        )
        .expect("the revision retains its explanation");
        assert_eq!(retained, model);
    }

    // A failed artifact write fails the verb without state damage:
    // but the apply's record — the revision advance — is already safe. The
    // artifact must never cost the envelope its commit.
    #[tokio::test]
    async fn a_failed_artifact_write_fails_the_verb_but_keeps_the_record() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, admitted) = crate::testkit::engine(tmp.path());
        let store = envelope_store(tmp.path());
        let env = DeploymentStateEnvelope {
            binding: Some(ProvenanceStamp::current(Utc::now())),
            config_revision: 4,
            ..Default::default()
        };
        let (_, v) = store.load().await.unwrap();
        store.save(&env, &v).await.unwrap();

        let path = tmp.path().join("no-such-dir").join("explanation.json");
        let err = apply(
            &engine,
            &admitted,
            None,
            false,
            Mode::resolve(false, false),
            Some(&path),
        )
        .await
        .expect_err("an unwritable artifact path fails the verb");
        let message = format!("{err:#}");
        assert!(
            message.contains("committed and recorded"),
            "the operator is told the apply itself is safe: {message}"
        );

        let (after, _) = store.load().await.unwrap();
        assert_eq!(
            after.config_revision, 5,
            "the artifact failure never costs the revision advance"
        );
    }

    #[tokio::test]
    async fn apply_proceeds_on_dev_iterate_and_restamps() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, admitted) = crate::testkit::engine(tmp.path());
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

        apply(
            &engine,
            &admitted,
            None,
            false,
            Mode::resolve(false, false),
            None,
        )
        .await
        .expect("apply proceeds under DevIterate");

        let (after, _) = store.load().await.unwrap();
        assert_eq!(after.config_revision, 5, "config_revision advanced by one");
        assert!(after.binding.is_some(), "envelope re-stamped");
        assert_eq!(after.deployment_id, "tokeira", "id defaulted from config");
    }
}
