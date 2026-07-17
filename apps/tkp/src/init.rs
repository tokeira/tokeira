//! `tkp init` — Day-0 mandatory versioning (task 2.2), recording the integrity
//! manifest at stamp time (task 4.1).
//!
//! Versioning is mandatory from Day 0: initialization writes the deployment
//! envelope's first provenance stamp **before any resource create**, so there is
//! no create path that leaves state unstamped. After this, every applying verb
//! finds a recorded binding and the gate can run.
//!
//! ## Engine identity vs configuration revision (task 14.1, Req 13)
//!
//! The binding's `source_tree_hash` (from `tokeira-build-info`) is the engine /
//! resource-implementation identity — a digest of the workspace **code**. A
//! deployment's desired-state configuration is operator **data** (its
//! `deployment.toml` / `.tkd`), which lives in the deployment dir, not the
//! workspace source, and so is **never** part of `source_tree_hash`. Therefore a
//! configuration change cannot change the engine identity and cannot gate: the
//! binding keys only on `source_tree_hash`, while config is tracked by the
//! separate `config_revision` (see `apply`). (Narrowing the digest to only the
//! engine crates — versus the whole workspace — is a build-system refinement;
//! Property 14 holds either way because config is excluded.)

use std::path::Path;

use anyhow::{Context, Result};
use chrono::Utc;
use tokeira_provisioner::{
    BinaryArtifactDescriptor, DeploymentStateEnvelope, IntegrityManifest, ProvenanceStamp, Target,
    sha256_hex,
};

use crate::{apply::load_local_config, config_history, envelope_store};

pub(crate) async fn init(deployment_dir: &Path) -> Result<()> {
    let running = ProvenanceStamp::current(Utc::now());
    let store = envelope_store(deployment_dir);
    let (existing, version) = store
        .load()
        .await
        .context("failed to load the deployment envelope")?;

    if existing.binding.is_some() {
        anyhow::bail!(
            "deployment is already initialized (a binding is recorded); `init` is Day-0 only"
        );
    }

    let config = load_local_config(deployment_dir)?;
    let integrity = running_integrity_manifest()?;

    let envelope = DeploymentStateEnvelope {
        deployment_id: config.project_name.clone(),
        binding: Some(running.clone()),
        integrity: Some(integrity),
        config_revision: 0,
        ..Default::default()
    };
    store
        .save(&envelope, &version)
        .await
        .context("failed to write the Day-0 stamp")?;
    // Retain the Day-0 config source as revision 0 so it is revertable (task 14.3).
    config_history::snapshot(deployment_dir, 0)
        .context("failed to retain the Day-0 config revision")?;

    println!(
        "initialized deployment '{}' — stamped with provisioner {} ({:?}), source_tree_hash {}",
        config.project_name, running.version, running.build_mode, running.source_tree_hash
    );
    Ok(())
}

/// Build the integrity manifest for the **running** binary (task 4.1): its
/// version, target triple, SHA-256, and size. Recorded at stamp time; the
/// launcher verifies a retrieved binary against it before execution (task 4.2).
pub(crate) fn running_integrity_manifest() -> Result<IntegrityManifest> {
    let exe = std::env::current_exe().context("failed to locate the running binary")?;
    let bytes = std::fs::read(&exe).with_context(|| format!("failed to read {}", exe.display()))?;
    let version = tokeira_build_info::TOKEIRA_VERSION.to_string();
    Ok(IntegrityManifest {
        provisioner_version: version.clone(),
        artifacts: vec![BinaryArtifactDescriptor {
            version,
            target: Target(env!("TKP_TARGET").to_string()),
            sha256: sha256_hex(&bytes),
            retrieval_ref: None,
            size_bytes: bytes.len() as u64,
        }],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn init_stamps_the_envelope_with_binding_and_integrity() {
        let tmp = tempfile::tempdir().unwrap();
        init(tmp.path()).await.expect("init succeeds");

        let (env, _) = envelope_store(tmp.path()).load().await.unwrap();
        assert!(env.binding.is_some(), "Day-0 binding recorded");
        let integrity = env.integrity.expect("integrity recorded at stamp time");
        assert_eq!(integrity.artifacts.len(), 1);
        assert!(!integrity.artifacts[0].sha256.is_empty());
        assert_eq!(env.config_revision, 0);
    }

    #[tokio::test]
    async fn init_refuses_an_already_initialized_deployment() {
        let tmp = tempfile::tempdir().unwrap();
        init(tmp.path()).await.expect("first init succeeds");
        let err = init(tmp.path()).await.expect_err("second init refuses");
        assert!(
            err.to_string().contains("already initialized"),
            "unexpected: {err}"
        );
    }

    // Property 14 (task 14.1): configuration refinement (repeated same-engine
    // applies) keeps the engine binding — the recorded source_tree_hash is
    // unchanged and the verdict stays proceeding — while config_revision advances.
    #[tokio::test]
    async fn config_refinement_keeps_the_engine_binding_and_advances_revision() {
        let tmp = tempfile::tempdir().unwrap();
        init(tmp.path()).await.expect("init");
        let (after_init, _) = envelope_store(tmp.path()).load().await.unwrap();
        let engine_hash = after_init
            .binding
            .as_ref()
            .unwrap()
            .source_tree_hash
            .clone();

        // Two successive config applies (same binary → same engine).
        crate::apply::apply(tmp.path()).await.expect("apply 1");
        crate::apply::apply(tmp.path()).await.expect("apply 2");

        let (after, _) = envelope_store(tmp.path()).load().await.unwrap();
        assert_eq!(
            after.binding.as_ref().unwrap().source_tree_hash,
            engine_hash,
            "the engine source_tree_hash is unchanged by config refinement"
        );
        assert_eq!(
            after.config_revision, 2,
            "config_revision advanced per apply"
        );
        assert!(
            after.effective_config_ref.is_some(),
            "apply records the effective config ref (task 14.2)"
        );
    }
}
