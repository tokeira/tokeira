//! `tkp upgrade` — atomic ownership transfer, then apply B's plan (tasks 5.3 / 8.4).
//!
//! `upgrade` is the only verb that authoritatively advances the recorded engine
//! identity. Its **first act is one atomic CAS commit** — flip the binding to the
//! running engine `B`, capture the `[A final]` `RollbackCheckpoint`, and open the
//! `UpgradeInFlight` operation marker — **before any provider mutation**, so a
//! crash always recovers as `B` with an open marker. It then applies `B`'s plan
//! and closes the marker.
//!
//! This first increment wires the local platform's (empty) apply between the
//! transfer and the close; state-schema migrations and multi-platform apply are
//! follow-ons.

use std::path::Path;

use anyhow::{Context, Result};
use chrono::Utc;
use tokeira_provisioner::{ProvenanceStamp, UpgradeDecision, evaluate_upgrade};

use crate::apply::run_local_infra_apply;
use crate::envelope_store;

pub async fn upgrade(deployment_dir: &Path) -> Result<()> {
    let running = ProvenanceStamp::current(Utc::now()); // B
    let store = envelope_store(deployment_dir);
    let (mut envelope, mut version) = store
        .load()
        .await
        .context("failed to load the deployment envelope")?;

    // Upgrade advances *from* a recorded engine A.
    let Some(recorded) = envelope.binding.clone() else {
        anyhow::bail!(
            "cannot upgrade an unstamped deployment (no recorded engine to upgrade from — \
             `create` stamps it first)"
        );
    };

    match evaluate_upgrade(&recorded, &running) {
        UpgradeDecision::Refuse(reason) => anyhow::bail!("`upgrade` refused: {reason}"),
        UpgradeDecision::VersionedAdvance => println!(
            "upgrade: versioned advance {} → {}",
            recorded.version, running.version
        ),
        UpgradeDecision::Promotion => println!(
            "upgrade: dev → versioned promotion (now version {})",
            running.version
        ),
    }

    // ── Atomic ownership transfer — one CAS commit, BEFORE any provider mutation ──
    let operation_id = format!("upgrade-{}", Utc::now().timestamp_millis());
    envelope.begin_upgrade(running.clone(), operation_id, Utc::now());
    version = store
        .save(&envelope, &version)
        .await
        .context("failed to commit the atomic ownership transfer")?;
    println!(
        "ownership transferred — [A final] checkpoint captured, operation marker open"
    );

    // ── Apply B's plan (local: empty). Migrations would run here on a schema change. ──
    let (change_count, _) = run_local_infra_apply(deployment_dir).await?;
    println!("infra apply under the new engine: {change_count} change(s)");

    // ── Close the operation marker ──
    envelope.close_operation();
    store
        .save(&envelope, &version)
        .await
        .context("failed to close the operation marker")?;
    println!("upgrade complete — now bound to version {}", running.version);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_provisioner::{BuildMode, DeploymentStateEnvelope};

    #[tokio::test]
    async fn upgrade_refuses_an_unstamped_deployment() {
        let tmp = tempfile::tempdir().unwrap();
        let err = upgrade(tmp.path())
            .await
            .expect_err("an unstamped deployment refuses");
        assert!(err.to_string().contains("unstamped"), "unexpected: {err}");
    }

    #[tokio::test]
    async fn upgrade_refuses_versioned_to_dev_restamp() {
        // Deployment recorded as Versioned; the running test binary is Dev
        // (a cargo build) → re-stamp-to-dev is refused.
        let tmp = tempfile::tempdir().unwrap();
        let store = envelope_store(tmp.path());
        let versioned = ProvenanceStamp {
            version: "1.0.0".to_string(),
            git_sha: "s".to_string(),
            source_tree_hash: "h".to_string(),
            build_mode: BuildMode::Versioned,
            recorded_at: Utc::now(),
        };
        let env = DeploymentStateEnvelope {
            binding: Some(versioned),
            ..Default::default()
        };
        let (_, v) = store.load().await.unwrap();
        store.save(&env, &v).await.unwrap();

        let err = upgrade(tmp.path())
            .await
            .expect_err("versioned → dev refuses");
        assert!(err.to_string().contains("refused"), "unexpected: {err}");
    }
}
