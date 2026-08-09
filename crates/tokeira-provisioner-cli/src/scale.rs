//! `tkp scale <dim>=<n> …` — change workload capacity: the platform's ops surface realizes the change,
//! then the shell folds it into a config revision.
//!
//! Conditionally realized through presence: a platform without an ops
//! surface refuses with a typed non-zero exit **before any mutation** —
//! after the binding and retarget gates, so the gate ordering stays
//! observable — and a provider whose ops surface has no scale dimension
//! states its own refusal as the error, in its own words.

use anyhow::{Context, Result};
use chrono::Utc;
use tokeira_platform::definition::DefinitionFrontend;
use tokeira_provisioner::ProvenanceStamp;

use crate::{
    apply::restamp_applied_revision,
    engine::Engine,
    envelope_store,
    gate::{GateOutcome, evaluate_gate},
    platform::Admitted,
};

pub(crate) async fn scale<F: DefinitionFrontend>(
    engine: &Engine<F>,
    admitted: &Admitted,
    specs: &[String],
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
    crate::marker::refuse_if_marked(&envelope, "scale")?;

    // ── Gate before any mutation ──
    match evaluate_gate(envelope.binding.as_ref(), &running) {
        GateOutcome::Refuse { verdict, reason } => {
            anyhow::bail!("binding gate refuses `scale` ({verdict:?}): {reason}");
        }
        // Proceeding verdicts are silent: the gate regime is a standing fact
        // of the deployment (describe's story), not news on every verb. Only
        // a refusal earns narration — and it is the error above.
        GateOutcome::Proceed { .. } => {}
    }

    // ── Retarget gate: a capacity change is a config apply, gated the same. ──
    crate::retarget_gate(engine, admitted, &envelope).await?;

    // ── Capacity change, through the platform's ops surface ──
    let Some(ops) = engine.platform().ops() else {
        anyhow::bail!("not applicable: this platform declares no ops surface");
    };
    let change_count = ops.scale(&admitted.deployment_ref, specs).await?;
    println!(
        "[{}] scale {}: {}",
        engine.platform().id(),
        specs.join(" "),
        tokeira_report::counted(change_count, "change")
    );

    // ── Re-stamp: a capacity change is a config revision ──
    let from_revision = envelope.config_revision;
    let config_source = admitted.config_source();
    restamp_applied_revision(&mut envelope, running, deployment_dir, &config_source)?;
    envelope.stamp_current_schema();
    store
        .save(&envelope, &version)
        .await
        .context("failed to persist the deployment envelope after scale")?;
    println!("revision {} → {}", from_revision, envelope.config_revision);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_provisioner::DeploymentStateEnvelope;

    #[tokio::test]
    async fn scale_reports_not_applicable_without_mutating() {
        let tmp = tempfile::tempdir().unwrap();
        let store = envelope_store(tmp.path());
        let env = DeploymentStateEnvelope {
            binding: Some(ProvenanceStamp::current(Utc::now())),
            config_revision: 3,
            ..Default::default()
        };
        let (_, v) = store.load().await.unwrap();
        store.save(&env, &v).await.unwrap();

        let (engine, admitted) = crate::testkit::engine(tmp.path());
        let err = scale(&engine, &admitted, &["web=3".to_string()])
            .await
            .expect_err("no ops surface → typed refusal");
        assert!(
            err.to_string().contains("not applicable"),
            "unexpected: {err}"
        );

        // Refused before any mutation: the revision did not advance.
        let (after, _) = store.load().await.unwrap();
        assert_eq!(after.config_revision, 3, "no revision advance on refusal");
    }

    #[tokio::test]
    async fn scale_gates_before_capability() {
        // An unstamped deployment refuses at the gate, before the ops
        // surface is even consulted.
        let tmp = tempfile::tempdir().unwrap();
        let (engine, admitted) = crate::testkit::engine(tmp.path());
        let err = scale(&engine, &admitted, &["web=3".to_string()])
            .await
            .expect_err("unstamped refuses at the gate");
        assert!(
            err.to_string().contains("binding gate refuses"),
            "unexpected: {err}"
        );
    }
}
