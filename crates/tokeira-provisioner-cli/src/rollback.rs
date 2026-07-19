//! `tkp rollback` — definition-driven rollback (task 8.5, Proposal 002).
//!
//! Rollback restores the retained prior configuration revision and lets the
//! forward engine reconcile toward it — no recorded before-images. The sequence:
//! the superseded binary `B` deletes what it created (a delete-only pass over
//! `keys(S_B) − keys(S_A)`), the binding re-pins to `A`, and `A` forward-applies
//! its retained prior revision.
//!
//! This first increment runs the local platform (no infra resources → the
//! delete-only pass is empty; the reconcile is an empty apply) in a single
//! process. The two-binary orchestration — `tkr` relaunches `A` to perform the
//! reconcile after `B`'s re-pin — and the real `destroy_selected` delete-only
//! pass over live resources are follow-ons.

use std::path::Path;

use anyhow::{Context, Result};
use chrono::Utc;
use tokeira_provisioner::ProvenanceStamp;

use crate::{
    ProvisionerPlatform, envelope_store,
    gate::{GateOutcome, evaluate_gate},
};

pub(crate) async fn rollback<P: ProvisionerPlatform>(
    platform: &P,
    deployment_dir: &Path,
) -> Result<()> {
    let running = ProvenanceStamp::current(Utc::now());
    let store = envelope_store(deployment_dir);
    let (mut envelope, mut version) = store
        .load()
        .await
        .context("failed to load the deployment envelope")?;

    // ── Binding gate, before any mutation ──
    // Like every other mutating verb, `rollback` gates the running binary against
    // the deployment's *current* recorded engine (B, post-upgrade): only the
    // engine that owns the deployment may initiate its rollback. This closes the
    // window where an unrelated binary could re-pin the binding to A and reconcile
    // the provider without ever being verified. (The two-binary orchestration —
    // `tkr` relaunches A for the reconcile — is a follow-on; today the running
    // binary must be B.)
    match evaluate_gate(envelope.binding.as_ref(), &running) {
        GateOutcome::Refuse { verdict, reason } => {
            anyhow::bail!("binding gate refuses `rollback` ({verdict:?}): {reason}");
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

    // ── Preconditions (fail-closed, before any destructive work) ──
    let checkpoint = envelope.checkpoint.clone().ok_or_else(|| {
        anyhow::anyhow!(
            "nothing to roll back: no [A final] checkpoint recorded (rollback follows an `upgrade`)"
        )
    })?;
    let to_a = checkpoint.from_provenance;
    println!(
        "rollback: reverting to the recorded prior engine (version {})",
        to_a.version
    );

    // ── B deletes what it created (keys(S_B) − keys(S_A)) ──
    // The real delete-only pass (`Engine::destroy_selected` over live resources)
    // lands with the two-binary orchestration (task 19.3); today it is empty.
    println!("B delete-only: 0 resource(s) (delete-only pass lands with task 19.3)");

    // ── Re-pin to A — one CAS commit ──
    // `begin_rollback` restores A's binding, integrity manifest (it must always
    // describe the engine the binding names), state heads, and config ref.
    let operation_id = format!("rollback-{}", Utc::now().timestamp_millis());
    envelope
        .begin_rollback(operation_id, Utc::now())
        .map_err(|e| anyhow::anyhow!(e))?;
    envelope.stamp_current_schema();
    version = store
        .save(&envelope, &version)
        .await
        .context("failed to commit the re-pin to A")?;
    println!(
        "re-pinned to A (version {}); rollback marker open",
        to_a.version
    );

    // ── A forward-reconciles toward its retained prior configuration revision ──
    let change_count = platform.infra_apply(deployment_dir).await?;
    println!("A reconcile (re-apply retained revision): {change_count} change(s)");

    // ── Complete: clear the marker and consume the checkpoint ──
    envelope.complete_rollback();
    store
        .save(&envelope, &version)
        .await
        .context("failed to complete the rollback")?;
    println!("rollback complete — bound to version {}", to_a.version);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TestPlatform;
    use tokeira_provisioner::{BuildMode, DeploymentStateEnvelope, ProvenanceStamp};

    #[tokio::test]
    async fn rollback_refuses_a_mismatched_running_binary() {
        // Post-upgrade shape bound to a *versioned* engine B; the running test
        // binary is a dev build (different identity) → the gate refuses before any
        // re-pin or provider reconcile, and the state is left untouched.
        let tmp = tempfile::tempdir().unwrap();
        let store = envelope_store(tmp.path());
        let b = ProvenanceStamp {
            version: "2.0.0".to_string(),
            git_sha: "sB".to_string(),
            source_tree_hash: "hB".to_string(),
            build_mode: BuildMode::Versioned,
            recorded_at: Utc::now(),
        };
        let a = ProvenanceStamp {
            source_tree_hash: "hA".to_string(),
            ..b.clone()
        };
        let mut env = DeploymentStateEnvelope {
            binding: Some(a),
            config_revision: 2,
            ..Default::default()
        };
        env.begin_upgrade(b, "op-up", Utc::now());
        env.close_operation(); // binding now B, checkpoint from A
        let (_, v) = store.load().await.unwrap();
        store.save(&env, &v).await.unwrap();

        let err = rollback(&TestPlatform, tmp.path())
            .await
            .expect_err("a mismatched running binary is refused");
        assert!(
            err.to_string().contains("binding gate refuses"),
            "unexpected: {err}"
        );

        // The re-pin/reconcile never ran: still bound to B, checkpoint intact.
        let (after, _) = store.load().await.unwrap();
        assert_eq!(
            after.binding.as_ref().unwrap().source_tree_hash,
            "hB",
            "no re-pin happened"
        );
        assert!(after.checkpoint.is_some(), "checkpoint not consumed");
    }

    #[tokio::test]
    async fn rollback_refuses_without_a_checkpoint() {
        let tmp = tempfile::tempdir().unwrap();
        let store = envelope_store(tmp.path());
        let env = DeploymentStateEnvelope {
            binding: Some(ProvenanceStamp::current(Utc::now())),
            ..Default::default()
        };
        let (_, v) = store.load().await.unwrap();
        store.save(&env, &v).await.unwrap();

        let err = rollback(&TestPlatform, tmp.path())
            .await
            .expect_err("no checkpoint refuses");
        assert!(
            err.to_string().contains("nothing to roll back"),
            "unexpected: {err}"
        );
    }

    #[tokio::test]
    async fn rollback_repins_to_the_checkpoint_engine() {
        use tokeira_provisioner::{BinaryArtifactDescriptor, IntegrityManifest, Target};

        fn manifest(sha: &str) -> IntegrityManifest {
            IntegrityManifest {
                provisioner_version: "1.0.0".to_string(),
                artifacts: vec![BinaryArtifactDescriptor {
                    target: Target("x".to_string()),
                    sha256: sha.to_string(),
                    retrieval_ref: None,
                    size_bytes: 1,
                }],
                ..Default::default()
            }
        }

        let tmp = tempfile::tempdir().unwrap();
        let store = envelope_store(tmp.path());

        // Build a post-upgrade envelope: A (binding + integrity) captured in the
        // checkpoint, bound to B with B's re-recorded manifest.
        let a = ProvenanceStamp::current(Utc::now());
        let b = ProvenanceStamp {
            source_tree_hash: "hB".to_string(),
            ..a.clone()
        };
        let mut env = DeploymentStateEnvelope {
            binding: Some(a.clone()),
            integrity: Some(manifest("sha-of-A")),
            config_revision: 2,
            ..Default::default()
        };
        env.begin_upgrade(b, "op-up", Utc::now());
        env.integrity = Some(manifest("sha-of-B")); // what `upgrade` re-records
        env.close_operation();
        let (_, v) = store.load().await.unwrap();
        store.save(&env, &v).await.unwrap();

        rollback(&TestPlatform, tmp.path())
            .await
            .expect("rollback succeeds");

        let (after, _) = store.load().await.unwrap();
        assert_eq!(
            after.binding.as_ref().unwrap().source_tree_hash,
            a.source_tree_hash,
            "re-pinned to A"
        );
        assert_eq!(
            after.integrity.as_ref().unwrap().artifacts[0].sha256,
            "sha-of-A",
            "A's integrity manifest restored with A's binding"
        );
        assert!(after.checkpoint.is_none(), "checkpoint consumed");
        assert!(after.operation.is_none(), "marker closed");
    }
}
