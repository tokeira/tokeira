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
    ChangeLog, ChangeLogEntry, DeploymentStateEnvelope, ENVELOPE_SCHEMA_VERSION, MigrationRegistry,
    OperationKind, ProvenanceStamp, UpgradeDecision, envelope_migrations, evaluate_upgrade,
};
use tokeira_state::DeploymentStore;

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

    // ── Operation-marker gate (task 19.4): a re-run of an interrupted
    // upgrade RESUMES from the recorded phase — the transfer already
    // happened, the binding already names B, so the decision gate below
    // (which would now see B == B and refuse) is exactly what resume skips.
    // An open ROLLBACK marker refuses: it is finished by `rollback`.
    if let crate::marker::MarkerDisposition::Resume(_) =
        crate::marker::check_marker(&envelope, "upgrade", Some(OperationKind::UpgradeInFlight))?
    {
        // The marker's phase and operation id are evidence, not answer —
        // detail-tier once mutating verbs carry the output contract's flags.
        println!("upgrade: finishing an upgrade interrupted earlier");
        // The binding must name the engine resuming it — a DIFFERENT binary
        // than the one that transferred ownership must not finish its upgrade.
        let bound = envelope
            .binding
            .as_ref()
            .map(|b| b.source_tree_hash.as_str());
        if bound != Some(running.source_tree_hash.as_str()) {
            anyhow::bail!(
                "the open upgrade marker belongs to another engine (bound {}, running {}) — \
                 run that binary to resume, or `rollback` to abort forward to A",
                bound.unwrap_or("unstamped"),
                running.source_tree_hash
            );
        }
        // Idempotent remainder: drift gate, apply B's plan, record the audit
        // log, close the marker — the same tail the fresh path runs.
        check_baseline_drift(&envelope)?;
        let applied = crate::change_log_entries(
            &platform
                .infra_apply_with_artifacts(deployment_dir)
                .await?
                .changes,
        );
        println!(
            "infra apply: {}",
            tokeira_report::counted(applied.len(), "change")
        );
        crate::render::print_applied(&applied);
        // B's creations join keys(S_B) − keys(S_A) — the rollback delete-set
        // (task 19.3) — in the same save as the audit log.
        envelope.record_post_checkpoint_changes(&applied);
        version = record_audit_log(store.as_ref(), &mut envelope, version, &applied).await?;
        envelope.close_operation();
        envelope.stamp_current_schema();
        store
            .save(&envelope, &version)
            .await
            .context("failed to close the operation marker")?;
        println!("upgrade complete — the deployment runs the new provisioner");
        return Ok(());
    }

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
            "upgrade: advancing the deployment's provisioner {} → {}",
            recorded.version, running.version
        ),
        UpgradeDecision::Promotion => println!(
            "upgrade: promoting the deployment's provisioner from dev to version {}",
            running.version
        ),
        // The dev-loop refresh: same ceremony (transfer, migrations, apply,
        // audit), advisory stamp; the re-recorded integrity manifest is what
        // actually changes — the envelope describes the new binary. The
        // caller (`tkr`) has already established the bytes differ. The gate
        // regime (advisory vs authoritative) is describe's story, not this
        // line's.
        UpgradeDecision::DevRefresh => {
            println!("upgrade: replacing the deployment's provisioner with the current dev build")
        }
    }

    // ── State-schema migration boundary (before any mutation) ──
    // Refuse an unbridged schema migration up front; run the forward migration
    // only when the schema changes (a new source_tree_hash at the same schema is
    // a re-stamp, not a migration).
    let migrations = envelope_migrations();
    let from_schema = envelope.schema_version;
    let to_schema = ENVELOPE_SCHEMA_VERSION;
    migrations
        .check_path(from_schema, to_schema)
        .map_err(|e| anyhow::anyhow!("`upgrade` refused: {e}"))?;
    if MigrationRegistry::needs_migration(from_schema, to_schema) {
        println!("state-schema migration {from_schema} → {to_schema}");
        let migrated = migrations
            .migrate(
                serde_json::to_value(&envelope).context("failed to serialize the envelope")?,
                from_schema,
                to_schema,
            )
            .map_err(|e| anyhow::anyhow!("`upgrade` refused: {e}"))?;
        envelope = serde_json::from_value(migrated)
            .context("the migrated envelope does not parse as the current schema")?;
    }

    // ── Atomic ownership transfer — one CAS commit, BEFORE any provider mutation ──
    transfer_ownership(&mut envelope, running.clone())?;
    version = store
        .save(&envelope, &version)
        .await
        .context("failed to commit the atomic ownership transfer")?;
    // Silent at summary depth: the checkpoint + open marker are crash-safety
    // evidence ("an interrupted upgrade resumes by running `upgrade` again"),
    // surfaced on the detail/JSON surface when `upgrade` migrates onto the
    // output contract — not narrated mid-ceremony.

    // ── Advisory baseline gate (Req 4.7, task 19.2): the envelope heads must
    // still be exactly [A final]'s — drift between the transfer and B's apply
    // means something else wrote state mid-upgrade. Refuse and surface;
    // `rollback` is the way out. (Provider-level live drift detection rides
    // the 19.3 relaunch machinery — this gate covers the recorded baseline.) ──
    check_baseline_drift(&envelope)?;

    // ── Apply B's plan (realized by the injected platform) ──
    let applied = crate::change_log_entries(
        &platform
            .infra_apply_with_artifacts(deployment_dir)
            .await?
            .changes,
    );
    println!(
        "infra apply: {}",
        tokeira_report::counted(applied.len(), "change")
    );
    crate::render::print_applied(&applied);

    // ── Record the ids-only audit change log in the open marker (19.2) and
    // fold B's creations into the rollback delete-set (19.3), persisted
    // BEFORE the close: an interruption here leaves both the evidence and
    // the undo-set durable. ──
    envelope.record_post_checkpoint_changes(&applied);
    version = record_audit_log(store.as_ref(), &mut envelope, version, &applied).await?;

    // ── Close the operation marker ──
    envelope.close_operation();
    store
        .save(&envelope, &version)
        .await
        .context("failed to close the operation marker")?;
    println!("upgrade complete — the deployment runs the new provisioner");
    Ok(())
}

/// The advisory baseline gate (Req 4.7, task 19.2): between the ownership
/// transfer and B's apply, the envelope heads must still be exactly what
/// `[A final]` recorded — divergence means another writer touched state
/// mid-upgrade (tampering, or an ungated older binary). Refuse and surface;
/// `rollback` aborts forward to A. Advisory means exactly this recorded-
/// baseline check — there is no cross-version authoritative reconcile, and
/// provider-level live drift detection rides the 19.3 relaunch machinery.
fn check_baseline_drift(envelope: &DeploymentStateEnvelope) -> Result<()> {
    let Some(checkpoint) = &envelope.checkpoint else {
        return Ok(());
    };
    let infra_drifted = envelope.infra_head != checkpoint.from_infra_head;
    let runtime_drifted = envelope.runtime_head != checkpoint.from_runtime_head;
    if infra_drifted || runtime_drifted {
        anyhow::bail!(
            "baseline drift from [A final]: the deployment's {} advanced since the ownership \
             transfer — another writer touched state mid-upgrade. Refusing to apply; `rollback` \
             aborts forward to A",
            match (infra_drifted, runtime_drifted) {
                (true, true) => "infra and runtime heads",
                (true, false) => "infra head",
                _ => "runtime head",
            }
        );
    }
    Ok(())
}

/// Persist the ids-only audit change log into the still-open marker
/// (task 19.2): one save between B's apply and the close, so the evidence of
/// what B committed survives an interruption — visible to `describe` and the
/// resume. Ids only, never before-images (Proposal 002); an empty apply
/// records no log, but the phase still advances to `applied` (the resume
/// waypoint).
async fn record_audit_log(
    store: &dyn DeploymentStore<DeploymentStateEnvelope>,
    envelope: &mut DeploymentStateEnvelope,
    version: String,
    applied: &[ChangeLogEntry],
) -> Result<String> {
    if let Some(operation) = envelope.operation.as_mut() {
        operation.phase = "applied".to_string();
        operation.audit_log = if applied.is_empty() {
            None
        } else {
            Some(ChangeLog {
                entries: applied.to_vec(),
            })
        };
    }
    store
        .save(envelope, &version)
        .await
        .context("failed to record the upgrade audit log")
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
                target: Target("x".to_string()),
                sha256: "sha-of-A".to_string(),
                retrieval_ref: None,
                size_bytes: 1,
            }],
            ..Default::default()
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

    // Task 19.4: an interrupted upgrade is recovered by RE-RUNNING `upgrade` —
    // the marker's phase says the transfer already committed, so the re-run
    // skips straight to B's apply and the close.
    #[tokio::test]
    async fn rerun_resumes_an_interrupted_upgrade_and_completes() {
        let tmp = tempfile::tempdir().unwrap();
        let store = envelope_store(tmp.path());
        // Mid-upgrade state: binding already names B (the RUNNING binary),
        // checkpoint holds [A final], marker open at ownership-transferred.
        let running = ProvenanceStamp::current(Utc::now());
        let mut env = DeploymentStateEnvelope {
            binding: Some(ProvenanceStamp {
                source_tree_hash: "hash-of-A".into(),
                ..running.clone()
            }),
            effective_config_ref: Some("cfg-A".into()),
            ..Default::default()
        };
        env.begin_upgrade(running.clone(), "op-interrupted", Utc::now());
        let (_, v) = store.load().await.unwrap();
        store.save(&env, &v).await.unwrap();

        upgrade(&TestPlatform, tmp.path())
            .await
            .expect("the re-run resumes and completes");

        let (after, _) = store.load().await.unwrap();
        assert!(after.operation.is_none(), "marker closed by the resume");
        assert_eq!(
            after.binding.as_ref().unwrap().source_tree_hash,
            running.source_tree_hash,
            "still bound to B"
        );
        assert!(
            after.checkpoint.is_some(),
            "[A final] stays retained — the rollback window survives the resume"
        );
        // A second run — nothing in flight, dev binding, dev binary — now
        // proceeds as a DEV REFRESH (the sanctioned dev-loop re-marry): the
        // tkp-side ceremony is unconditional; byte-level idempotency is the
        // launcher's gate (`tkr` compares candidate vs bound bytes and skips
        // the ceremony entirely when they match).
        upgrade(&TestPlatform, tmp.path())
            .await
            .expect("dev → dev proceeds as a refresh");
        let (after_refresh, _) = store.load().await.unwrap();
        assert!(after_refresh.operation.is_none(), "refresh closed cleanly");
    }

    // Task 19.2: the audit save between apply and close — the marker carries
    // the ids-only log at phase 'applied', durably, before the close wipes it.
    #[tokio::test]
    async fn record_audit_log_persists_entries_in_the_open_marker() {
        use tokeira_provisioner::ChangeOp;

        let tmp = tempfile::tempdir().unwrap();
        let store = envelope_store(tmp.path());
        let running = ProvenanceStamp::current(Utc::now());
        let mut env = DeploymentStateEnvelope {
            binding: Some(running.clone()),
            ..Default::default()
        };
        env.begin_upgrade(running, "op-audit", Utc::now());
        let (_, v0) = store.load().await.unwrap();
        let v1 = store.save(&env, &v0).await.unwrap();

        let applied = vec![
            ChangeLogEntry {
                id: "vpc/main".into(),
                op: ChangeOp::Created,
            },
            ChangeLogEntry {
                id: "svc/web".into(),
                op: ChangeOp::Updated,
            },
        ];
        record_audit_log(store.as_ref(), &mut env, v1, &applied)
            .await
            .expect("audit save");

        let (saved, _) = store.load().await.unwrap();
        let mut empty_env = saved.clone();
        let operation = saved.operation.expect("marker still open");
        assert_eq!(operation.phase, "applied", "the resume waypoint advanced");
        let log = operation.audit_log.expect("log recorded");
        assert_eq!(
            log.entries, applied,
            "ids-only evidence of what B committed"
        );

        // An empty apply advances the phase but records no log.
        let (_, v) = store.load().await.unwrap();
        record_audit_log(store.as_ref(), &mut empty_env, v, &[])
            .await
            .expect("audit save");
        assert!(empty_env.operation.expect("open").audit_log.is_none());
    }

    // Task 19.2: the advisory baseline gate — heads that moved since the
    // transfer refuse B's apply; rollback is the way out.
    #[tokio::test]
    async fn drifted_heads_refuse_the_upgrade_apply() {
        use tokeira_state::SnapshotRef;

        let tmp = tempfile::tempdir().unwrap();
        let store = envelope_store(tmp.path());
        let running = ProvenanceStamp::current(Utc::now());
        let mut env = DeploymentStateEnvelope {
            binding: Some(ProvenanceStamp {
                source_tree_hash: "hash-of-A".into(),
                ..running.clone()
            }),
            ..Default::default()
        };
        env.begin_upgrade(running, "op-drift", Utc::now());
        // Someone advances the infra head AFTER the transfer captured [A final].
        env.infra_head = Some(SnapshotRef {
            snapshot_key: "k".into(),
            snapshot_version_id: None,
            snapshot_etag: "e".into(),
            sha256_hex: "deadbeef".into(),
            size_bytes: 1,
            commit_id: "c".into(),
            committed_at: Utc::now(),
            committed_by: "intruder".into(),
        });
        let (_, v) = store.load().await.unwrap();
        store.save(&env, &v).await.unwrap();

        let err = upgrade(&TestPlatform, tmp.path())
            .await
            .expect_err("drift refuses the resume/apply");
        assert!(
            err.to_string().contains("baseline drift"),
            "unexpected: {err}"
        );
    }

    // Only the engine that transferred ownership may finish its upgrade — a
    // different binary re-running `upgrade` against the open marker refuses.
    #[tokio::test]
    async fn a_different_engine_cannot_resume_anothers_upgrade() {
        let tmp = tempfile::tempdir().unwrap();
        let store = envelope_store(tmp.path());
        let running = ProvenanceStamp::current(Utc::now());
        let other_engine = ProvenanceStamp {
            source_tree_hash: "hash-of-someone-else".into(),
            ..running
        };
        let mut env = DeploymentStateEnvelope {
            binding: Some(ProvenanceStamp {
                source_tree_hash: "hash-of-A".into(),
                ..other_engine.clone()
            }),
            ..Default::default()
        };
        env.begin_upgrade(other_engine, "op-foreign", Utc::now());
        let (_, v) = store.load().await.unwrap();
        store.save(&env, &v).await.unwrap();

        let err = upgrade(&TestPlatform, tmp.path())
            .await
            .expect_err("a foreign marker refuses");
        assert!(
            err.to_string().contains("another engine"),
            "unexpected: {err}"
        );
    }
}
