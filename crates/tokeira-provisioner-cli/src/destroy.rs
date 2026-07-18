//! `tkp destroy` — gated teardown of the deployment's infrastructure (task 8.3).
//!
//! `destroy` is a mutating verb, so the binding gate runs **before any provider
//! mutation** (versioned deployments refuse on a non-`Match` verdict; dev
//! deployments take the permissive `DevIterate` warn-and-proceed path — the same
//! policy as `apply`). Because a teardown is irreversible, it additionally
//! requires an explicit `--yes` confirmation.
//!
//! The engine identity binding is retained (the running binary is still the
//! authority over the now-empty state) and `effective_config_ref` is cleared to
//! record that nothing is currently applied; `config_revision` is a property of
//! the *configuration*, which the teardown does not change, so it is left as-is.

use std::path::Path;

use anyhow::{Context, Result};
use chrono::Utc;
use tokeira_provisioner::ProvenanceStamp;

use crate::{
    ProvisionerPlatform, envelope_store,
    gate::{GateOutcome, evaluate_gate},
};

pub(crate) async fn destroy<P: ProvisionerPlatform>(
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

    // ── Gate before any mutation ──
    match evaluate_gate(envelope.binding.as_ref(), &running) {
        GateOutcome::Refuse { verdict, reason } => {
            anyhow::bail!("binding gate refuses `destroy` ({verdict:?}): {reason}");
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

    // ── Irreversible → require explicit confirmation (after the gate) ──
    if !yes {
        anyhow::bail!(
            "`destroy` tears down all of the deployment's infrastructure and is irreversible; \
             re-run with `--yes` to confirm"
        );
    }

    // ── Engine destroy (realized by the injected platform) ──
    let removed = platform.infra_destroy(deployment_dir).await?;
    println!(
        "[{}] infra destroy: {removed} resource(s) removed",
        platform.label(deployment_dir)
    );

    // ── Record the teardown ──
    // Retain the engine identity (it authored the empty state); clear the
    // effective config ref (nothing applied). config_revision is unchanged — the
    // configuration itself did not change, only the live footprint.
    envelope.binding = Some(running);
    envelope.effective_config_ref = None;
    store
        .save(&envelope, &version)
        .await
        .context("failed to persist the deployment envelope after destroy")?;
    println!(
        "envelope: torn down (config_revision {} retained)",
        envelope.config_revision
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TestPlatform;
    use tokeira_provisioner::DeploymentStateEnvelope;

    #[tokio::test]
    async fn destroy_requires_confirmation() {
        let tmp = tempfile::tempdir().unwrap();
        let store = envelope_store(tmp.path());
        // Dev-stamped (DevIterate proceeds through the gate) so we reach the
        // confirmation guard rather than a gate refusal.
        let env = DeploymentStateEnvelope {
            binding: Some(ProvenanceStamp::current(Utc::now())),
            ..Default::default()
        };
        let (_, v) = store.load().await.unwrap();
        store.save(&env, &v).await.unwrap();

        let err = destroy(&TestPlatform, tmp.path(), false)
            .await
            .expect_err("destroy without --yes refuses");
        assert!(
            err.to_string().contains("irreversible"),
            "unexpected: {err}"
        );
    }

    #[tokio::test]
    async fn destroy_refuses_an_unstamped_deployment() {
        // No binding → Unknown → gate refuses before the confirmation guard.
        let tmp = tempfile::tempdir().unwrap();
        let err = destroy(&TestPlatform, tmp.path(), true)
            .await
            .expect_err("an unstamped deployment refuses");
        assert!(
            err.to_string().contains("binding gate refuses"),
            "unexpected: {err}"
        );
    }

    #[tokio::test]
    async fn destroy_tears_down_local_and_clears_the_config_ref() {
        let tmp = tempfile::tempdir().unwrap();
        let store = envelope_store(tmp.path());
        let env = DeploymentStateEnvelope {
            binding: Some(ProvenanceStamp::current(Utc::now())),
            config_revision: 3,
            effective_config_ref: Some("sha256:deadbeef".to_string()),
            ..Default::default()
        };
        let (_, v) = store.load().await.unwrap();
        store.save(&env, &v).await.unwrap();

        destroy(&TestPlatform, tmp.path(), true)
            .await
            .expect("local destroy succeeds");

        let (after, _) = store.load().await.unwrap();
        assert_eq!(after.config_revision, 3, "config_revision retained");
        assert!(
            after.effective_config_ref.is_none(),
            "effective config ref cleared"
        );
        assert!(after.binding.is_some(), "engine identity retained");
    }
}
