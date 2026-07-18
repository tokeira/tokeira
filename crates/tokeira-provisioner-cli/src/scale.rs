//! `tkp scale <dim>=<n> …` — change workload capacity (design §Command
//! behaviour and outputs): fold the specs into a config revision, then a
//! workload apply.
//!
//! Conditionally realized: neither current platform has a scale dimension, so
//! both answer [`Realization::NotApplicable`] and the shell refuses with a
//! typed non-zero exit **before any mutation**. When a platform (ECS) realizes
//! scaling, the realized path advances the config revision under the same
//! mutating-verb contract as every apply.

use std::path::Path;

use anyhow::{Context, Result};
use chrono::Utc;
use tokeira_provisioner::ProvenanceStamp;

use crate::{
    ProvisionerPlatform, Realization,
    apply::restamp_applied_revision,
    envelope_store,
    gate::{GateOutcome, evaluate_gate},
};

pub(crate) async fn scale<P: ProvisionerPlatform>(
    platform: &P,
    deployment_dir: &Path,
    specs: &[String],
) -> Result<()> {
    let running = ProvenanceStamp::current(Utc::now());
    let store = envelope_store(deployment_dir);
    let (mut envelope, version) = store
        .load()
        .await
        .context("failed to load the deployment envelope")?;

    // ── Gate before any mutation ──
    match evaluate_gate(envelope.binding.as_ref(), &running) {
        GateOutcome::Refuse { verdict, reason } => {
            anyhow::bail!("binding gate refuses `scale` ({verdict:?}): {reason}");
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

    // ── Capacity change (realized by the injected platform) ──
    let change_count = match platform.scale(deployment_dir, specs).await? {
        Realization::NotApplicable { reason } => {
            anyhow::bail!("not applicable: {reason}");
        }
        Realization::Realized(count) => count,
    };
    println!(
        "[{}] scale {}: {change_count} change(s)",
        platform.label(deployment_dir),
        specs.join(" ")
    );

    // ── Re-stamp: a capacity change is a config revision ──
    restamp_applied_revision(
        &mut envelope,
        running,
        deployment_dir,
        platform.config_basename(deployment_dir),
    )?;
    store
        .save(&envelope, &version)
        .await
        .context("failed to persist the deployment envelope after scale")?;
    println!("envelope: config_revision now {}", envelope.config_revision);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TestPlatform;
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

        let err = scale(&TestPlatform, tmp.path(), &["web=3".to_string()])
            .await
            .expect_err("no scale dimension → typed refusal");
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
        // An unstamped deployment refuses at the gate, before the platform's
        // capability answer is even consulted.
        let tmp = tempfile::tempdir().unwrap();
        let err = scale(&TestPlatform, tmp.path(), &["web=3".to_string()])
            .await
            .expect_err("unstamped refuses at the gate");
        assert!(
            err.to_string().contains("binding gate refuses"),
            "unexpected: {err}"
        );
    }
}
