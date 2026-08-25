//! `tkp revert --to <revision>` — config-revision revert.
//!
//! Reverting a configuration is a **same-engine `apply` of a prior recorded
//! config revision** — deliberately *not* an `upgrade` (the engine identity is
//! unchanged; `source_tree_hash` does not move) and *not* a two-binary
//! `rollback` (no `[A final]` checkpoint, no delete-only pass). The
//! retained revision's config source is restored into the live config file and
//! the ordinary gated apply reconciles the live footprint toward it.
//!
//! Revisions are monotonic (forward-only, like a commit log): reverting to
//! revision `N` produces a *new* revision whose content equals `N`'s, rather than
//! rewinding the counter. So `describe`'s history stays append-only and a revert
//! is itself revertable.

use anyhow::{Context, Result};
use chrono::Utc;
use tokeira_deployment::ProvenanceStamp;

use tokeira_platform::definition::DefinitionFrontend;

use crate::{
    apply::config_ref,
    config_history,
    engine::Engine,
    envelope_store,
    gate::{GateOutcome, evaluate_gate},
    platform::Admitted,
};

pub(crate) async fn revert<F: DefinitionFrontend>(
    engine: &Engine<F>,
    admitted: &Admitted,
    to_revision: u64,
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
    crate::marker::refuse_if_marked(&envelope, "revert")?;

    // ── Gate before any mutation (a revert is a config apply) ──
    match evaluate_gate(envelope.binding.as_ref(), &running) {
        GateOutcome::Refuse { verdict, reason } => {
            anyhow::bail!("binding gate refuses `revert` ({verdict:?}): {reason}");
        }
        // Proceeding verdicts are silent: the gate regime is a standing fact
        // of the deployment (describe's story), not news on every verb. Only
        // a refusal earns narration — and it is the error above.
        GateOutcome::Proceed { .. } => {}
    }

    // ── Target must be a prior, retained revision ──
    let config_source = admitted.config_source();
    if to_revision >= envelope.config_revision {
        anyhow::bail!(
            "cannot revert to config revision {to_revision}: the current revision is {} \
             (revert restores a *prior* revision)",
            envelope.config_revision
        );
    }
    if !config_history::is_retained(deployment_dir, &config_source, to_revision) {
        anyhow::bail!(
            "config revision {to_revision} was not retained; only revisions produced by a prior \
             creation/apply can be reverted to"
        );
    }

    // ── Restore the retained revision's config source, then reconcile ──
    config_history::restore(deployment_dir, &config_source, to_revision)
        .context("failed to restore the target config revision")?;
    println!(
        "restored config revision {to_revision} → {}",
        config_history::config_file(deployment_dir, &config_source).display()
    );

    let outcome = engine.apply(admitted, None).await?;
    crate::persist_writeback(deployment_dir, &outcome.writeback)?;
    let applied = crate::change_log_entries(&outcome.changes);
    // Under an open rollback checkpoint, creations join keys(S_B) − keys(S_A)
    // — the set the rollback B-delete pass consumes.
    envelope.record_post_checkpoint_changes(&applied);
    println!(
        "[{}] revert reconcile: {}",
        engine.platform().id(),
        tokeira_report::counted(applied.len(), "change")
    );

    // ── Re-stamp: a forward config revision whose content equals `to_revision` ──
    envelope.binding = Some(running);
    envelope.config_revision += 1;
    envelope.effective_config_ref = Some(config_ref(deployment_dir, &config_source));
    config_history::snapshot(deployment_dir, &config_source, envelope.config_revision)
        .context("failed to retain the reverted config revision")?;
    envelope.stamp_current_schema();
    store
        .save(&envelope, &version)
        .await
        .context("failed to persist the deployment envelope after revert")?;
    // ── Post-commit publication: a NEW, higher publication whose content
    // is the reverted-to state (Req 4.3); failure never alters the
    // committed revert. ──
    crate::publication::publish_committed_transition(
        engine,
        admitted,
        tokeira_deployment::repository::claim::Transition::Revert,
        envelope.config_revision,
    )
    .await;
    println!(
        "envelope: config_revision now {} (content of revision {to_revision})",
        envelope.config_revision
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_deployment::DeploymentStateEnvelope;

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
        let (engine, admitted) = crate::testkit::engine(tmp.path());
        let err = revert(&engine, &admitted, 2)
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

        let (engine, admitted) = crate::testkit::engine(tmp.path());
        let err = revert(&engine, &admitted, 1)
            .await
            .expect_err("no snapshot for revision 1 → refuse");
        assert!(
            err.to_string().contains("not retained"),
            "unexpected: {err}"
        );
    }

    #[tokio::test]
    async fn revert_restores_a_prior_local_revision_and_advances_forward() {
        // Full flow through the shell (empty test-platform apply): creation → apply →
        // apply builds two retained revisions with distinct config, then revert to
        // the first restores its config and advances the counter forward.
        let tmp = tempfile::tempdir().unwrap();
        let (engine, admitted) = crate::testkit::engine(tmp.path());
        crate::testkit::realize_creation(&admitted).await;

        // Revision 1: a definition with marker content "one" (the stub
        // frontend ignores the bytes; retention copies them).
        std::fs::write(tmp.path().join("definition.tkd"), "// one\n").unwrap();
        crate::apply::apply(
            &engine,
            &admitted,
            None,
            false,
            tokeira_report::Mode::resolve(false, false),
            None,
        )
        .await
        .expect("apply rev 1");

        // Revision 2: change the recorded source.
        std::fs::write(tmp.path().join("definition.tkd"), "// two\n").unwrap();
        crate::apply::apply(
            &engine,
            &admitted,
            None,
            false,
            tokeira_report::Mode::resolve(false, false),
            None,
        )
        .await
        .expect("apply rev 2");

        let (before, _) = envelope_store(tmp.path()).load().await.unwrap();
        assert_eq!(before.config_revision, 2);

        revert(&engine, &admitted, 1)
            .await
            .expect("revert to revision 1");

        // The live config source now holds revision 1's content...
        let restored = std::fs::read_to_string(tmp.path().join("definition.tkd")).unwrap();
        assert!(
            restored.contains("one"),
            "reverted to revision 1's config: {restored}"
        );
        // ...and the counter advanced forward (monotonic).
        let (after, _) = envelope_store(tmp.path()).load().await.unwrap();
        assert_eq!(after.config_revision, 3, "revert is a forward revision");
    }
}
