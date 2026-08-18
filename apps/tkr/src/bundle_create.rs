//! Optional hermetic bundle placement for a discovery-selected provisioner.
//!
//! The flow snapshots the selected source closure, resolves `EngineIdentity`,
//! obtains a verified bundle (CAS hit or one Dagger build), retains it in the
//! deployment, and places `tkp` with its manifest sidecar. The native generated
//! build remains the default development path.
//!
//! The CAS lives under the deployments root (`.bundle-cas/`, a
//! `LocalBackend`) — the `LocalDeveloper` tier's store. Trusted tiers ride
//! the same [`BundleStore`] over S3 when CI flows land (18.4 skipped by
//! owner decision; Phase 3).

use std::{collections::BTreeSet, path::PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use tokeira_build::{
    DefaultDaggerClient, DefinitionFrontendPackageDescriptor, PlatformPackageDescriptor,
    ProvisionerBuildRequest, assemble_bound_provisioner, obtain_provisioner,
    rust_toolchain_version, snapshot_source_closure,
};
use tokeira_deployment::{
    AuthorityTier, BUNDLE_MANIFEST_BASENAME, BinaryArtifactDescriptor, BinaryStore, BuildAuthority,
    BuildManifest, BuildProfile, BundleStore, EngineIdentity, ProvisionerBundle, Sha256Digest,
    Target, TestEvidence,
};
use tokeira_state::LocalBackend;

use crate::deployment_dir::{DeploymentResolver, PROVISIONER_BIN};

/// Obtain a verified bundle for the host target and marry it to the
/// deployment: `<deployment>/tkp` + the manifest sidecar, with the bytes
/// retained under the deployment's state for self-contained rollback.
pub(crate) async fn place_bundle_provisioner_at(
    deployments: &DeploymentResolver,
    deployment_dir: &std::path::Path,
    build_image: &str,
    workspace_root: &std::path::Path,
    platform: &PlatformPackageDescriptor,
    frontend: &DefinitionFrontendPackageDescriptor,
) -> Result<()> {
    let dagger = DefaultDaggerClient::from_env()
        .map_err(|e| anyhow!("`--bundle` needs a running Dagger engine: {e}"))?;
    let bound_source = assemble_bound_provisioner(workspace_root, platform, frontend)
        .context("failed to assemble the bound provisioner source")?;
    let snapshot = snapshot_source_closure(
        &bound_source
            .snapshot_request(workspace_root)
            .context("failed to scope the snapshot request to the closure")?,
    )
    .context("failed to freeze the source snapshot")?;

    let host_target = Target(env!("TKR_TARGET").to_string());
    let request_id = uuid::Uuid::new_v4().to_string();
    let request = ProvisionerBuildRequest {
        workspace_root: workspace_root.to_path_buf(),
        bound_source,
        targets: vec![host_target.clone()],
        profile: BuildProfile::Dist,
        authority: BuildAuthority::LocalDeveloper,
        build_image: build_image.to_string(),
        snapshot,
        version: tokeira_build_info::TOKEIRA_VERSION.to_string(),
        request_id: request_id.clone(),
        output_dir: deployments.root().join(".bundle-work").join(&request_id),
    };

    let cas = BundleStore::new(
        Box::new(LocalBackend::new(deployments.root().join(".bundle-cas"))),
        "bundles",
    );
    let obtained =
        obtain_provisioner(&request, &dagger, &cas, AuthorityTier::LocalDeveloper).await?;
    let bytes = obtained.bytes_by_target.get(&host_target).ok_or_else(|| {
        anyhow!(
            "the bundle carries no artifact for host target {}",
            host_target.0
        )
    })?;

    let (bundle, retrieval_ref) =
        marry_bundle_at(deployment_dir, obtained.bundle, &host_target, bytes).await?;

    println!(
        "provisioner: bundle {} ({}) placed as `tkp` — retained at {}",
        &bundle.identity_digest().to_hex()[..12],
        if obtained.cache_hit {
            "CAS hit, re-verified"
        } else {
            "built hermetically, published"
        },
        retrieval_ref
    );
    Ok(())
}

/// Marry an obtained bundle to the deployment: retain the host artifact
/// under the deployment's own state (self-contained rollback, Proposal
/// 002/005), record the retention ref inside the bundle — the sidecar
/// carries the FULL bundle (describe surfaces identity fields, build
/// provenance, and test evidence from it; init extracts the integrity
/// manifest after self-verifying) — then place `tkp` and the sidecar
/// `tkp init` records.
pub(crate) async fn marry_bundle_at(
    deployment_dir: &std::path::Path,
    mut bundle: tokeira_deployment::ProvisionerBundle,
    host_target: &Target,
    bytes: &[u8],
) -> Result<(tokeira_deployment::ProvisionerBundle, String)> {
    let retention = BinaryStore::new(
        Box::new(LocalBackend::new(deployment_dir.join("state"))),
        "binaries",
    );
    let retrieval_ref = retention
        .persist(&bundle.identity, host_target, bytes)
        .await
        .context("failed to retain the bundle in the deployment")?;
    for artifact in &mut bundle.artifacts {
        if artifact.target == *host_target {
            artifact.retrieval_ref = Some(retrieval_ref.clone());
        }
    }

    let dest = deployment_dir.join(PROVISIONER_BIN);
    std::fs::write(&dest, bytes).with_context(|| format!("failed to place {}", dest.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&dest, std::fs::Permissions::from_mode(0o755))?;
    }
    let sidecar = deployment_dir.join(BUNDLE_MANIFEST_BASENAME);
    std::fs::write(&sidecar, serde_json::to_vec_pretty(&bundle)?)
        .with_context(|| format!("failed to write {}", sidecar.display()))?;
    Ok((bundle, retrieval_ref))
}

/// Place the native workspace build as the deployment's engine, with a
/// synthesized dev-tier bundle manifest so publication is uniform across
/// engine kinds.
///
/// The identity is honest about what this tier is: closures computed from
/// the tracked source snapshot, `build_container: None` (the native dev
/// loop is non-hermetic by construction — an S3 publication of this engine
/// refuses on exactly that), `LocalDeveloper` authority, and one artifact
/// for the host target — fetching a dev publication onto another
/// architecture refuses with `host_target_unsupported`, which is correct
/// for a dev artifact.
pub(crate) async fn place_dev_provisioner_at(
    deployment_dir: &std::path::Path,
    workspace_root: &std::path::Path,
    platform: &PlatformPackageDescriptor,
    frontend: &DefinitionFrontendPackageDescriptor,
) -> Result<()> {
    // The native build assembles the bound source internally; the assembly
    // below re-derives the same deterministic source, so the evidence and
    // the placed binary describe the same composition.
    let artifact =
        DeploymentResolver::build_provisioner_from_workspace(workspace_root, platform, frontend)?;
    let bytes = std::fs::read(&artifact)
        .with_context(|| format!("failed to read {}", artifact.display()))?;

    let bound_source = assemble_bound_provisioner(workspace_root, platform, frontend)
        .context("failed to assemble the bound provisioner source")?;
    let snapshot = snapshot_source_closure(
        &bound_source
            .snapshot_request(workspace_root)
            .context("failed to scope the snapshot request to the closure")?,
    )
    .context("failed to freeze the source snapshot")?;
    let toolchain = rust_toolchain_version(workspace_root)
        .context("failed to resolve the workspace toolchain")?;
    let identity = EngineIdentity {
        source_closure: bound_source.source_closure_digest(&snapshot.tree_oid),
        lock_closure: Sha256Digest::from_bytes(&bound_source.closure().canonical_lock_bytes()),
        toolchain: toolchain.clone(),
        build_container: None,
        features: BTreeSet::new(),
        profile: BuildProfile::Dev,
    };

    let host_target = Target(env!("TKR_TARGET").to_string());
    let synthesized = ProvisionerBundle {
        identity,
        bound: None,
        authority: BuildAuthority::LocalDeveloper,
        provisioner_version: tokeira_build_info::TOKEIRA_VERSION.to_string(),
        artifacts: vec![BinaryArtifactDescriptor {
            target: host_target.clone(),
            sha256: tokeira_deployment::sha256_hex(&bytes),
            retrieval_ref: None,
            size_bytes: bytes.len() as u64,
        }],
        // No test step runs at this tier; recording `passed: false` with the
        // reason keeps the evidence honest rather than claiming a pass that
        // never happened. The dev bundle never enters the CAS, whose publish
        // gate is where `passed` is enforced.
        tests: TestEvidence {
            command: "dev engine: native workspace build".to_string(),
            passed: false,
        },
        build: BuildManifest {
            request_id: uuid::Uuid::new_v4().to_string(),
            source_tree_oid: snapshot.tree_oid.clone(),
            snapshot_commit_oid: snapshot.commit_oid.clone(),
            toolchain,
            builder: "native-dev".to_string(),
        },
    }
    .with_bound_evidence(bound_source.evidence(&snapshot.tree_oid))
    .context("dev engine evidence disagrees with its own identity")?;

    let (bundle, _retrieval_ref) =
        marry_bundle_at(deployment_dir, synthesized, &host_target, &bytes).await?;

    println!(
        "provisioner: dev engine {} placed as `tkp` (native workspace build; dev tier)",
        &bundle.identity_digest().to_hex()[..12],
    );
    Ok(())
}

/// Walk up from `start` to the workspace root (`Cargo.lock` and
/// `rust-toolchain.toml` together mark it).
pub(crate) fn workspace_root_from(start: &std::path::Path) -> Result<PathBuf> {
    let mut dir = start.to_path_buf();
    loop {
        if dir.join("Cargo.lock").exists() && dir.join("rust-toolchain.toml").exists() {
            return Ok(dir);
        }
        if !dir.pop() {
            bail!(
                "`--bundle` builds the provisioner from source — run it from inside the \
                 tokeira workspace (no Cargo.lock + rust-toolchain.toml found walking up \
                 from {})",
                start.display()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_root_walks_up_to_the_marker_pair() {
        let tmp = tempfile::tempdir().expect("tempdir");
        std::fs::write(tmp.path().join("Cargo.lock"), "").unwrap();
        std::fs::write(tmp.path().join("rust-toolchain.toml"), "").unwrap();
        let nested = tmp.path().join("crates/somewhere/deep");
        std::fs::create_dir_all(&nested).unwrap();

        let found = workspace_root_from(&nested).expect("found");
        assert_eq!(
            std::fs::canonicalize(found).unwrap(),
            std::fs::canonicalize(tmp.path()).unwrap()
        );
    }

    #[test]
    fn outside_a_workspace_is_a_clear_refusal() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let err = workspace_root_from(tmp.path()).expect_err("refuses");
        assert!(err.to_string().contains("--bundle"), "unexpected: {err}");
    }
}
