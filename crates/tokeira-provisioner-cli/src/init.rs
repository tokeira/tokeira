//! Day-0 mandatory versioning (task 2.2), recording the integrity manifest at
//! stamp time (task 4.1). Not an operator verb: `tkr deployment create` runs
//! this as an internal inception step (Req 6.5).
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
    BinaryArtifactDescriptor, BuildAuthority, DeploymentStateEnvelope, IntegrityManifest,
    ProvenanceStamp, Target, sha256_hex,
};

use crate::{ProvisionerPlatform, config_history, envelope_store};

pub(crate) async fn init<P: ProvisionerPlatform>(
    platform: &P,
    deployment_dir: &Path,
) -> Result<()> {
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

    let deployment_id = platform.deployment_id(deployment_dir)?;
    let integrity = day_zero_manifest(deployment_dir)?;

    let envelope = DeploymentStateEnvelope {
        deployment_id: deployment_id.clone(),
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
    config_history::snapshot(deployment_dir, platform.config_basename(deployment_dir), 0)
        .context("failed to retain the Day-0 config revision")?;

    println!(
        "initialized deployment '{}' — stamped with provisioner {} ({:?}), source_tree_hash {}",
        deployment_id, running.version, running.build_mode, running.source_tree_hash
    );
    Ok(())
}

/// The manifest the Day-0 stamp records (tasks 18.3/19.1): extracted from the
/// **bundle sidecar** (`tkp.manifest.json` — the full [`ProvisionerBundle`],
/// whose build provenance `describe` also surfaces) when `tkr deployment
/// create` placed one next to the deployment's `tkp`, else the pre-identity
/// self-stamp.
///
/// A placed sidecar is a create-time contract, so a corrupt one **refuses
/// init** — it is never silently downgraded to a self-stamp. And it is only
/// recorded after the running binary proves it *is* one of the sidecar's
/// artifacts (its own bytes verify against the manifest for its own target):
/// the manifest the envelope records always describes the engine that
/// stamped it.
fn day_zero_manifest(deployment_dir: &Path) -> Result<IntegrityManifest> {
    let sidecar = deployment_dir.join(tokeira_provisioner::BUNDLE_MANIFEST_BASENAME);
    if !sidecar.exists() {
        return running_integrity_manifest();
    }
    let raw = std::fs::read(&sidecar)
        .with_context(|| format!("failed to read the bundle sidecar {}", sidecar.display()))?;
    let bundle: tokeira_provisioner::ProvisionerBundle = serde_json::from_slice(&raw)
        .with_context(|| {
            format!(
                "the placed bundle sidecar {} does not parse — refusing to stamp from it",
                sidecar.display()
            )
        })?;
    let manifest = bundle.integrity_manifest();
    manifest
        .validate()
        .map_err(|e| anyhow::anyhow!("the placed bundle manifest is malformed: {e}"))?;

    let exe = std::env::current_exe().context("failed to locate the running binary")?;
    let bytes = std::fs::read(&exe).with_context(|| format!("failed to read {}", exe.display()))?;
    manifest
        .verify_artifact(&bytes, &Target(env!("TKP_TARGET").to_string()))
        .map_err(|e| {
            anyhow::anyhow!(
                "the running provisioner is not an artifact of the placed bundle manifest \
                 ({e}) — the deployment's tkp and its manifest disagree; re-run \
                 `tkr deployment create`"
            )
        })?;
    Ok(manifest)
}

/// Build the integrity manifest for the **running** binary (task 4.1): its
/// target triple, SHA-256, and size, with the semver as a human label.
/// Recorded at stamp time; the launcher verifies a retrieved binary against it
/// before execution (task 4.2).
///
/// A self-stamped native build is a **pre-identity** manifest: it carries no
/// [`EngineIdentity`](tokeira_provisioner::EngineIdentity) (the closure inputs
/// exist only once the source snapshot supplies them — task 17) and attests
/// nothing beyond `LocalDeveloper` authority. The build pipeline (task 18) is
/// what records more.
pub(crate) fn running_integrity_manifest() -> Result<IntegrityManifest> {
    let exe = std::env::current_exe().context("failed to locate the running binary")?;
    let bytes = std::fs::read(&exe).with_context(|| format!("failed to read {}", exe.display()))?;
    Ok(IntegrityManifest {
        engine_identity: None,
        authority: BuildAuthority::LocalDeveloper,
        provisioner_version: tokeira_build_info::TOKEIRA_VERSION.to_string(),
        artifacts: vec![BinaryArtifactDescriptor {
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
    use crate::TestPlatform;

    #[tokio::test]
    async fn init_stamps_the_envelope_with_binding_and_integrity() {
        let tmp = tempfile::tempdir().unwrap();
        init(&TestPlatform, tmp.path())
            .await
            .expect("init succeeds");

        let (env, _) = envelope_store(tmp.path()).load().await.unwrap();
        assert!(env.binding.is_some(), "Day-0 binding recorded");
        let integrity = env.integrity.expect("integrity recorded at stamp time");
        assert_eq!(integrity.artifacts.len(), 1);
        assert!(!integrity.artifacts[0].sha256.is_empty());
        assert_eq!(env.config_revision, 0);
    }

    /// A full bundle sidecar (what `tkr deployment create --bundle` places)
    /// whose sole artifact has the given bytes' checksum.
    fn bundle_sidecar_for(bytes: &[u8]) -> tokeira_provisioner::ProvisionerBundle {
        use tokeira_provisioner::{
            BuildProfile, EngineIdentity, ProvisionerBundle, Sha256Digest,
            bundle::{BuildManifest, TestEvidence},
        };
        ProvisionerBundle {
            identity: EngineIdentity {
                source_closure: Sha256Digest::from_bytes(b"src"),
                lock_closure: Sha256Digest::from_bytes(b"lock"),
                toolchain: "rustc 1.88.0".into(),
                build_container: Some(Sha256Digest::from_bytes(b"img")),
                features: ["provisioner".to_string()].into(),
                profile: BuildProfile::Dist,
            },
            bound: None,
            authority: BuildAuthority::LocalDeveloper,
            provisioner_version: "0.1.0".into(),
            artifacts: vec![BinaryArtifactDescriptor {
                target: Target(env!("TKP_TARGET").to_string()),
                sha256: sha256_hex(bytes),
                retrieval_ref: None,
                size_bytes: bytes.len() as u64,
            }],
            tests: TestEvidence {
                command: "cargo test --locked".into(),
                passed: true,
            },
            build: BuildManifest {
                request_id: "req-1".into(),
                source_tree_oid: "tree-oid".into(),
                snapshot_commit_oid: "commit-oid".into(),
                toolchain: "1.88.0".into(),
                builder: "dagger-local".into(),
            },
        }
    }

    // Tasks 18.3/19.1: a placed bundle sidecar's integrity manifest is
    // recorded as the Day-0 manifest — identity-keyed, unlike the
    // pre-identity self-stamp — after the running binary proves it is one of
    // its artifacts.
    #[tokio::test]
    async fn init_records_a_placed_bundle_manifest_that_describes_the_running_binary() {
        let tmp = tempfile::tempdir().unwrap();
        // A sidecar whose artifact IS the running (test) binary.
        let exe = std::env::current_exe().unwrap();
        let bytes = std::fs::read(&exe).unwrap();
        let bundle = bundle_sidecar_for(&bytes);
        std::fs::write(
            tmp.path()
                .join(tokeira_provisioner::BUNDLE_MANIFEST_BASENAME),
            serde_json::to_vec(&bundle).unwrap(),
        )
        .unwrap();

        init(&TestPlatform, tmp.path())
            .await
            .expect("init succeeds");

        let (env, _) = envelope_store(tmp.path()).load().await.unwrap();
        let recorded = env.integrity.expect("integrity recorded");
        assert_eq!(
            recorded,
            bundle.integrity_manifest(),
            "the sidecar bundle's manifest is the record"
        );
        assert!(
            recorded.engine_identity.is_some(),
            "the Day-0 stamp is identity-keyed"
        );
    }

    #[tokio::test]
    async fn init_refuses_a_sidecar_that_does_not_describe_the_running_binary() {
        let tmp = tempfile::tempdir().unwrap();
        let bundle = bundle_sidecar_for(b"some other binary entirely");
        std::fs::write(
            tmp.path()
                .join(tokeira_provisioner::BUNDLE_MANIFEST_BASENAME),
            serde_json::to_vec(&bundle).unwrap(),
        )
        .unwrap();

        let err = init(&TestPlatform, tmp.path())
            .await
            .expect_err("a disagreeing sidecar refuses init");
        assert!(
            err.to_string().contains("not an artifact"),
            "unexpected: {err}"
        );
        // Nothing was stamped.
        let (env, _) = envelope_store(tmp.path()).load().await.unwrap();
        assert!(env.binding.is_none());
    }

    #[tokio::test]
    async fn init_refuses_an_already_initialized_deployment() {
        let tmp = tempfile::tempdir().unwrap();
        init(&TestPlatform, tmp.path())
            .await
            .expect("first init succeeds");
        let err = init(&TestPlatform, tmp.path())
            .await
            .expect_err("second init refuses");
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
        init(&TestPlatform, tmp.path()).await.expect("init");
        let (after_init, _) = envelope_store(tmp.path()).load().await.unwrap();
        let engine_hash = after_init
            .binding
            .as_ref()
            .unwrap()
            .source_tree_hash
            .clone();

        // Two successive config applies (same binary → same engine).
        crate::apply::apply(
            &TestPlatform,
            tmp.path(),
            false,
            tokeira_report::Mode::resolve(false, false),
            None,
        )
        .await
        .expect("apply 1");
        crate::apply::apply(
            &TestPlatform,
            tmp.path(),
            false,
            tokeira_report::Mode::resolve(false, false),
            None,
        )
        .await
        .expect("apply 2");

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
