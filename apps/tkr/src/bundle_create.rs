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

use std::path::PathBuf;

use anyhow::{Context, Result, anyhow, bail};
use tokeira_build::{
    DefaultDaggerClient, DefinitionFrontendPackageDescriptor, PlatformPackageDescriptor,
    ProvisionerBuildRequest, SnapshotRequest, assemble_bound_provisioner, obtain_provisioner,
    snapshot_source_closure,
};
use tokeira_deployment::{
    AuthorityTier, BUNDLE_MANIFEST_BASENAME, BinaryStore, BuildAuthority, BuildProfile,
    BundleStore, Target,
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
    let snapshot = snapshot_source_closure(&SnapshotRequest {
        repo_root: workspace_root.to_path_buf(),
        closure_paths: bound_source.closure().closure_paths(),
        include_untracked: false,
    })
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

    // Retain under the deployment's own state (self-contained rollback,
    // Proposal 002/005), and record where in the manifest we place.
    let retention = BinaryStore::new(
        Box::new(LocalBackend::new(deployment_dir.join("state"))),
        "binaries",
    );
    let retrieval_ref = retention
        .persist(&obtained.bundle.identity, &host_target, bytes)
        .await
        .context("failed to retain the bundle in the deployment")?;
    // Record the retention ref inside the bundle itself — the sidecar carries
    // the FULL bundle (task 19.1: describe surfaces identity fields, build
    // provenance, and test evidence from it; init extracts the integrity
    // manifest after self-verifying).
    let mut bundle = obtained.bundle;
    for artifact in &mut bundle.artifacts {
        if artifact.target == host_target {
            artifact.retrieval_ref = Some(retrieval_ref.clone());
        }
    }

    // Place the married provisioner + the sidecar `tkp init` records (18.3c).
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
