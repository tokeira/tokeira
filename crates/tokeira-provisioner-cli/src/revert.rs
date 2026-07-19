//! `tkp revert --to <revision>` — config-revision revert (task 14.3, Req 13.3).
//!
//! Reverting a configuration is a **same-engine `apply` of a prior recorded
//! config revision** — deliberately *not* an `upgrade` (the engine identity is
//! unchanged; `source_tree_hash` does not move) and *not* a two-binary
//! `rollback` (Proposal 002; no `[A final]` checkpoint, no delete-only pass). The
//! retained revision's config source is restored into the live config file and
//! the ordinary gated apply reconciles the live footprint toward it.
//!
//! Revisions are monotonic (forward-only, like a commit log): reverting to
//! revision `N` produces a *new* revision whose content equals `N`'s, rather than
//! rewinding the counter. So `describe`'s history stays append-only and a revert
//! is itself revertable.

use std::path::Path;

use anyhow::{Context, Result};
use chrono::Utc;
use tokeira_provisioner::ProvenanceStamp;

use crate::{
    ProvisionerPlatform,
    apply::config_ref,
    config_history, envelope_store,
    gate::{GateOutcome, evaluate_gate},
};

pub(crate) async fn revert<P: ProvisionerPlatform>(
    platform: &P,
    deployment_dir: &Path,
    to_revision: u64,
) -> Result<()> {
    let running = ProvenanceStamp::current(Utc::now());
    let store = envelope_store(deployment_dir);
    let (mut envelope, version) = store
        .load()
        .await
        .context("failed to load the deployment envelope")?;

    // ── Gate before any mutation (a revert is a config apply) ──
    match evaluate_gate(envelope.binding.as_ref(), &running) {
        GateOutcome::Refuse { verdict, reason } => {
            anyhow::bail!("binding gate refuses `revert` ({verdict:?}): {reason}");
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

    // ── Target must be a prior, retained revision ──
    let config_basename = platform.config_basename(deployment_dir);
    if to_revision >= envelope.config_revision {
        anyhow::bail!(
            "cannot revert to config revision {to_revision}: the current revision is {} \
             (revert restores a *prior* revision)",
            envelope.config_revision
        );
    }
    if !config_history::is_retained(deployment_dir, config_basename, to_revision) {
        anyhow::bail!(
            "config revision {to_revision} was not retained; only revisions produced by a prior \
             `init`/`apply` can be reverted to"
        );
    }

    // ── Restore the retained revision's config source, then reconcile ──
    config_history::restore(deployment_dir, config_basename, to_revision)
        .context("failed to restore the target config revision")?;
    println!(
        "restored config revision {to_revision} → {}",
        config_history::config_file(deployment_dir, config_basename).display()
    );

    let change_count = platform.infra_apply(deployment_dir).await?;
    println!(
        "[{}] revert reconcile: {change_count} change(s)",
        platform.label(deployment_dir)
    );

    // ── Re-stamp: a forward config revision whose content equals `to_revision` ──
    envelope.binding = Some(running);
    envelope.config_revision += 1;
    envelope.effective_config_ref = Some(config_ref(deployment_dir, config_basename));
    config_history::snapshot(deployment_dir, config_basename, envelope.config_revision)
        .context("failed to retain the reverted config revision")?;
    envelope.stamp_current_schema();
    store
        .save(&envelope, &version)
        .await
        .context("failed to persist the deployment envelope after revert")?;
    println!(
        "envelope: config_revision now {} (content of revision {to_revision})",
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
    async fn revert_refuses_a_non_prior_revision() {
        let tmp = tempfile::tempdir().unwrap();
        let store = envelope_store(tmp.path());
        let env = DeploymentStateEnvelope {
            binding: Some(ProvenanceStamp::current(Utc::now())),
            config_revision: 2,
            ..Default::default()
        };
        let (_, v) = store.load().await.unwrap();
        store.save(&env, &v).await.unwrap();

        // Reverting to the current (or a future) revision is rejected.
        let err = revert(&TestPlatform, tmp.path(), 2)
            .await
            .expect_err("revert to current revision refuses");
        assert!(err.to_string().contains("prior"), "unexpected: {err}");
    }

    #[tokio::test]
    async fn revert_refuses_an_unretained_revision() {
        let tmp = tempfile::tempdir().unwrap();
        let store = envelope_store(tmp.path());
        let env = DeploymentStateEnvelope {
            binding: Some(ProvenanceStamp::current(Utc::now())),
            config_revision: 5,
            ..Default::default()
        };
        let (_, v) = store.load().await.unwrap();
        store.save(&env, &v).await.unwrap();

        let err = revert(&TestPlatform, tmp.path(), 1)
            .await
            .expect_err("no snapshot for revision 1 → refuse");
        assert!(
            err.to_string().contains("not retained"),
            "unexpected: {err}"
        );
    }

    #[tokio::test]
    async fn revert_restores_a_prior_local_revision_and_advances_forward() {
        // Full flow through the shell (empty test-platform apply): init → apply →
        // apply builds two retained revisions with distinct config, then revert to
        // the first restores its config and advances the counter forward.
        let tmp = tempfile::tempdir().unwrap();
        crate::init::init(&TestPlatform, tmp.path())
            .await
            .expect("init");

        // Revision 1: a deployment.toml with project "one".
        std::fs::write(
            tmp.path().join("deployment.toml"),
            "project_name = \"one\"\n",
        )
        .unwrap();
        crate::apply::apply(&TestPlatform, tmp.path())
            .await
            .expect("apply rev 1");

        // Revision 2: change the config source.
        std::fs::write(
            tmp.path().join("deployment.toml"),
            "project_name = \"two\"\n",
        )
        .unwrap();
        crate::apply::apply(&TestPlatform, tmp.path())
            .await
            .expect("apply rev 2");

        let (before, _) = envelope_store(tmp.path()).load().await.unwrap();
        assert_eq!(before.config_revision, 2);

        revert(&TestPlatform, tmp.path(), 1)
            .await
            .expect("revert to revision 1");

        // The live config source now holds revision 1's content...
        let restored = std::fs::read_to_string(tmp.path().join("deployment.toml")).unwrap();
        assert!(
            restored.contains("one"),
            "reverted to revision 1's config: {restored}"
        );
        // ...and the counter advanced forward (monotonic).
        let (after, _) = envelope_store(tmp.path()).load().await.unwrap();
        assert_eq!(after.config_revision, 3, "revert is a forward revision");
    }
}
