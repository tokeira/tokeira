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
use tokeira_provisioner::{
    DeploymentStateEnvelope, ENVELOPE_SCHEMA_VERSION, MigrationRegistry, ProvenanceStamp,
    UpgradeDecision, evaluate_upgrade,
};

use crate::{ProvisionerPlatform, envelope_store, init::running_integrity_manifest};

pub(crate) async fn upgrade<P: ProvisionerPlatform>(
    platform: &P,
    deployment_dir: &Path,
) -> Result<()> {
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

    // ── State-schema migration boundary (before any mutation) ──
    // Refuse an unbridged schema migration up front; run the forward migration
    // only when the schema changes (a new source_tree_hash at the same schema is
    // a re-stamp, not a migration).
    let migrations = MigrationRegistry::new();
    let from_schema = envelope.schema_version;
    let to_schema = ENVELOPE_SCHEMA_VERSION;
    migrations
        .check_path(from_schema, to_schema)
        .map_err(|e| anyhow::anyhow!("`upgrade` refused: {e}"))?;
    if MigrationRegistry::needs_migration(from_schema, to_schema) {
        println!("state-schema migration {from_schema} → {to_schema}");
        // (Envelope + state-doc migrations run here when transitions are registered.)
        envelope.schema_version = to_schema;
    }

    // ── Atomic ownership transfer — one CAS commit, BEFORE any provider mutation ──
    transfer_ownership(&mut envelope, running.clone())?;
    version = store
        .save(&envelope, &version)
        .await
        .context("failed to commit the atomic ownership transfer")?;
    println!("ownership transferred — [A final] checkpoint captured, operation marker open");

    // ── Apply B's plan (realized by the injected platform). Migrations would run here on a schema change. ──
    let change_count = platform.infra_apply(deployment_dir).await?;
    println!("infra apply under the new engine: {change_count} change(s)");

    // ── Close the operation marker ──
    envelope.close_operation();
    store
        .save(&envelope, &version)
        .await
        .context("failed to close the operation marker")?;
    println!(
        "upgrade complete — now bound to version {}",
        running.version
    );
    Ok(())
}

/// The in-memory ownership transfer: flip the binding to the running engine `B`
/// (capturing the `[A final]` checkpoint — including A's integrity manifest,
/// which rollback restores) and **re-record the integrity manifest for B**. The
/// envelope's manifest must always describe the engine the binding names: the
/// launcher's bound-class verification (`tkr`, task 9.1) checks the installed
/// `tkp` against it, so a stale Day-0 manifest would permanently fail every
/// post-upgrade bound launch.
fn transfer_ownership(envelope: &mut DeploymentStateEnvelope, to: ProvenanceStamp) -> Result<()> {
    let operation_id = format!("upgrade-{}", Utc::now().timestamp_millis());
    envelope.begin_upgrade(to, operation_id, Utc::now());
    envelope.integrity = Some(
        running_integrity_manifest()
            .context("failed to record the new engine's integrity manifest")?,
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TestPlatform;
    use tokeira_provisioner::{BuildMode, DeploymentStateEnvelope};

    #[tokio::test]
    async fn upgrade_refuses_an_unstamped_deployment() {
        let tmp = tempfile::tempdir().unwrap();
        let err = upgrade(&TestPlatform, tmp.path())
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

        let err = upgrade(&TestPlatform, tmp.path())
            .await
            .expect_err("versioned → dev refuses");
        assert!(err.to_string().contains("refused"), "unexpected: {err}");
    }

    // Regression for the stale-manifest wedge: the ownership transfer must
    // re-record the integrity manifest for the NEW engine (B), retaining A's in
    // the checkpoint for rollback — otherwise every post-upgrade bound-class
    // launch fails verification against the Day-0 (A) manifest forever.
    #[test]
    fn ownership_transfer_rerecords_integrity_for_the_new_engine() {
        use tokeira_provisioner::{BinaryArtifactDescriptor, IntegrityManifest, Target};

        let a_manifest = IntegrityManifest {
            provisioner_version: "1.0.0".to_string(),
            artifacts: vec![BinaryArtifactDescriptor {
                version: "1.0.0".to_string(),
                target: Target("x".to_string()),
                sha256: "sha-of-A".to_string(),
                retrieval_ref: None,
                size_bytes: 1,
            }],
        };
        let mut env = DeploymentStateEnvelope {
            binding: Some(ProvenanceStamp::current(Utc::now())),
            integrity: Some(a_manifest),
            ..Default::default()
        };

        transfer_ownership(&mut env, ProvenanceStamp::current(Utc::now())).unwrap();

        // A's manifest is retained in the checkpoint (for rollback)...
        let checkpoint = env.checkpoint.as_ref().expect("[A final] captured");
        assert_eq!(checkpoint.from_integrity.artifacts[0].sha256, "sha-of-A");
        // ...and the envelope now records a fresh manifest of the running binary.
        let refreshed = env.integrity.as_ref().expect("integrity re-recorded");
        assert_ne!(refreshed.artifacts[0].sha256, "sha-of-A");
        assert!(!refreshed.artifacts[0].sha256.is_empty());
    }
}
