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

use crate::apply::run_local_infra_apply;
use crate::envelope_store;

pub async fn rollback(deployment_dir: &Path) -> Result<()> {
    let store = envelope_store(deployment_dir);
    let (mut envelope, mut version) = store
        .load()
        .await
        .context("failed to load the deployment envelope")?;

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
    // The local platform has no infra resources, so the delete-only pass is
    // empty; `Engine::destroy_selected` wires here for real platforms.
    println!("B delete-only: 0 resource(s) (local platform has no infra resources)");

    // ── Re-pin to A — one CAS commit ──
    let operation_id = format!("rollback-{}", Utc::now().timestamp_millis());
    envelope
        .begin_rollback(operation_id, Utc::now())
        .map_err(|e| anyhow::anyhow!(e))?;
    version = store
        .save(&envelope, &version)
        .await
        .context("failed to commit the re-pin to A")?;
    println!("re-pinned to A (version {}); rollback marker open", to_a.version);

    // ── A forward-reconciles toward its retained prior configuration revision ──
    let (change_count, _) = run_local_infra_apply(deployment_dir).await?;
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
    use tokeira_provisioner::{DeploymentStateEnvelope, ProvenanceStamp};

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

        let err = rollback(tmp.path())
            .await
            .expect_err("no checkpoint refuses");
        assert!(
            err.to_string().contains("nothing to roll back"),
            "unexpected: {err}"
        );
    }

    #[tokio::test]
    async fn rollback_repins_to_the_checkpoint_engine() {
        let tmp = tempfile::tempdir().unwrap();
        let store = envelope_store(tmp.path());

        // Build a post-upgrade envelope: A captured in the checkpoint, bound to B.
        let a = ProvenanceStamp::current(Utc::now());
        let b = ProvenanceStamp {
            source_tree_hash: "hB".to_string(),
            ..a.clone()
        };
        let mut env = DeploymentStateEnvelope {
            binding: Some(a.clone()),
            config_revision: 2,
            ..Default::default()
        };
        env.begin_upgrade(b, "op-up", Utc::now());
        env.close_operation();
        let (_, v) = store.load().await.unwrap();
        store.save(&env, &v).await.unwrap();

        rollback(tmp.path()).await.expect("rollback succeeds");

        let (after, _) = store.load().await.unwrap();
        assert_eq!(
            after.binding.as_ref().unwrap().source_tree_hash,
            a.source_tree_hash,
            "re-pinned to A"
        );
        assert!(after.checkpoint.is_none(), "checkpoint consumed");
        assert!(after.operation.is_none(), "marker closed");
    }
}
