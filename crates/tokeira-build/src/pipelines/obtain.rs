//! Resolve-or-build (task 18.3; Proposal 005 §Creation flow, Phase 2).
//!
//! The canonical obtain step of `tkr deployment create`:
//!
//! > resolve `EngineIdentity` → CAS: verified bundle? **hit** → re-verify →
//! > reuse; **miss** → produce via Dagger → publish → use.
//!
//! Identity is computed **without building** ([`engine_identity_for`]) — the
//! store is consulted first, and a hit means zero compilation. The hit is
//! re-verified by the store's own admission gate (bytes + authority floor +
//! deny-list; caching never grants admission), and a *present but
//! inadmissible* bundle surfaces as an error rather than triggering a quiet
//! rebuild — an integrity event the operator must see.
//!
//! On a miss the freshly built bundle is published back to the CAS before
//! use, so the next create of the same identity (any deployment, any agent)
//! hits.

use std::collections::BTreeMap;

use tokeira_provisioner::{AuthorityTier, BundleStore, ProvisionerBundle, Target};

use crate::{BuildError, DaggerClient};

use super::provisioner::{ProvisionerBuildRequest, build_provisioner, engine_identity_for};

/// The outcome of the obtain step: a verified bundle and the bytes for every
/// requested target, ready to place and retain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObtainedProvisioner {
    pub bundle: ProvisionerBundle,
    pub bytes_by_target: BTreeMap<Target, Vec<u8>>,
    /// Whether the CAS satisfied the request without a build — a performance
    /// fact for the operator report, never a trust statement (trust came
    /// from the admission gate either way).
    pub cache_hit: bool,
}

/// Resolve a verified bundle for the request's identity from `store`, or
/// build one via `dagger` and publish it. `floor` is the deployment's
/// required authority tier; the request's own authority must satisfy it
/// (building bytes the deployment would immediately refuse is a request
/// error, surfaced before any work).
pub async fn obtain_provisioner(
    request: &ProvisionerBuildRequest,
    dagger: &dyn DaggerClient,
    store: &BundleStore,
    floor: AuthorityTier,
) -> Result<ObtainedProvisioner, BuildError> {
    if !request.authority.satisfies(floor) {
        return Err(BuildError::Validation {
            reason: format!(
                "the requested build authority {:?} cannot satisfy the deployment's required \
                 tier {floor:?} — a fresh build would be refused at bind",
                request.authority.tier(),
            ),
        });
    }
    let identity = engine_identity_for(request)?;

    // Hit path: every requested target must resolve (a bundle built for
    // other targets is a miss for this request as a whole).
    let mut resolved = BTreeMap::new();
    let mut bundle: Option<ProvisionerBundle> = None;
    for target in &request.targets {
        match store.resolve(&identity, floor, target).await? {
            Some(hit) => {
                bundle.get_or_insert(hit.bundle);
                resolved.insert(target.clone(), hit.bytes);
            }
            None => {
                resolved.clear();
                bundle = None;
                break;
            }
        }
    }
    if let Some(bundle) = bundle {
        return Ok(ObtainedProvisioner {
            bundle,
            bytes_by_target: resolved,
            cache_hit: true,
        });
    }

    // Miss: one canonical build, published before use so the next create of
    // this identity — any deployment, any agent — hits.
    let bundle = build_provisioner(request, dagger)?;
    let mut bytes_by_target = BTreeMap::new();
    for target in &request.targets {
        let path = request.output_dir.join(&target.0).join("tkp");
        let bytes = std::fs::read(&path).map_err(|e| BuildError::Validation {
            reason: format!("built artifact missing at {}: {e}", path.display()),
        })?;
        bytes_by_target.insert(target.clone(), bytes);
    }
    store.publish(&bundle, &bytes_by_target).await?;
    Ok(ObtainedProvisioner {
        bundle,
        bytes_by_target,
        cache_hit: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ProvisionerClosure, SnapshotRequest, snapshot_source_closure, testing::MockDaggerClient,
    };
    use std::{
        path::{Path, PathBuf},
        process::Command,
    };
    use tokeira_provisioner::{BuildAuthority, BuildProfile, BundleStoreError};
    use tokeira_state::LocalBackend;

    fn git(dir: &Path, args: &[&str]) {
        let output = Command::new("git")
            .current_dir(dir)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_AUTHOR_NAME", "t")
            .env("GIT_AUTHOR_EMAIL", "t@t.invalid")
            .env("GIT_COMMITTER_NAME", "t")
            .env("GIT_COMMITTER_EMAIL", "t@t.invalid")
            .args(args)
            .output()
            .expect("git runs");
        assert!(output.status.success(), "git {args:?} failed");
    }

    fn request(repo: &Path, out: &Path) -> ProvisionerBuildRequest {
        git(repo, &["init", "-q", "-b", "main"]);
        std::fs::create_dir_all(repo.join("platforms/alpha/src")).expect("mkdir");
        std::fs::write(
            repo.join("rust-toolchain.toml"),
            "[toolchain]\nchannel = \"1.95\"\n",
        )
        .expect("write");
        std::fs::write(repo.join("platforms/alpha/src/lib.rs"), "pub fn a() {}\n").expect("write");
        git(repo, &["add", "."]);
        git(repo, &["commit", "-q", "-m", "seed"]);
        let snapshot = snapshot_source_closure(&SnapshotRequest {
            repo_root: repo.to_path_buf(),
            closure_paths: vec![
                PathBuf::from("platforms/alpha"),
                PathBuf::from("rust-toolchain.toml"),
            ],
            include_untracked: false,
        })
        .expect("snapshot");
        ProvisionerBuildRequest {
            workspace_root: repo.to_path_buf(),
            seed_package: "tokeira-alpha".into(),
            bin_name: "tkp-alpha".into(),
            features: vec!["provisioner".into()],
            targets: vec![Target("aarch64-unknown-linux-gnu".into())],
            profile: BuildProfile::Dist,
            authority: BuildAuthority::LocalDeveloper,
            build_image: format!("rust:1.95-slim-bookworm@sha256:{}", "ab".repeat(32)),
            snapshot,
            closure: ProvisionerClosure {
                crate_dirs: vec![PathBuf::from("platforms/alpha")],
                crate_names: vec!["tokeira-alpha".into()],
                workspace_files: vec![PathBuf::from("rust-toolchain.toml")],
                locked: vec![],
            },
            version: "0.1.0".into(),
            request_id: "req-1".into(),
            output_dir: out.to_path_buf(),
        }
    }

    fn store(root: &Path) -> BundleStore {
        BundleStore::new(Box::new(LocalBackend::new(root)), "bundles")
    }

    #[tokio::test]
    async fn miss_builds_publishes_then_the_next_obtain_hits() {
        let repo = tempfile::tempdir().expect("tempdir");
        let out = tempfile::tempdir().expect("tempdir");
        let cas = tempfile::tempdir().expect("tempdir");
        let store = store(cas.path());
        let request = request(repo.path(), out.path());

        // First obtain: an honest miss → build → publish.
        let mock = MockDaggerClient::default();
        let first = obtain_provisioner(&request, &mock, &store, AuthorityTier::LocalDeveloper)
            .await
            .expect("obtain builds");
        assert!(!first.cache_hit);
        assert!(!mock.calls().is_empty(), "the engine was exercised");

        // Second obtain, same identity: a verified CAS hit — ZERO engine calls.
        let mock2 = MockDaggerClient::default();
        let out2 = tempfile::tempdir().expect("tempdir");
        let mut second_request = request.clone();
        second_request.output_dir = out2.path().to_path_buf();
        let second = obtain_provisioner(
            &second_request,
            &mock2,
            &store,
            AuthorityTier::LocalDeveloper,
        )
        .await
        .expect("obtain hits");
        assert!(second.cache_hit);
        assert!(mock2.calls().is_empty(), "a hit builds nothing");
        assert_eq!(second.bundle, first.bundle);
        assert_eq!(second.bytes_by_target, first.bytes_by_target);
    }

    #[tokio::test]
    async fn an_insufficient_authority_request_is_refused_before_any_work() {
        let repo = tempfile::tempdir().expect("tempdir");
        let out = tempfile::tempdir().expect("tempdir");
        let cas = tempfile::tempdir().expect("tempdir");
        let mock = MockDaggerClient::default();
        let request = request(repo.path(), out.path());

        let err = obtain_provisioner(
            &request,
            &mock,
            &store(cas.path()),
            AuthorityTier::TrustedCi,
        )
        .await
        .expect_err("a LocalDeveloper build cannot satisfy a TrustedCi floor");
        assert!(matches!(err, BuildError::Validation { .. }));
        assert!(mock.calls().is_empty());
    }

    #[tokio::test]
    async fn a_tampered_cas_entry_surfaces_instead_of_rebuilding() {
        let repo = tempfile::tempdir().expect("tempdir");
        let out = tempfile::tempdir().expect("tempdir");
        let cas = tempfile::tempdir().expect("tempdir");
        let store = store(cas.path());
        let request = request(repo.path(), out.path());

        let mock = MockDaggerClient::default();
        let built = obtain_provisioner(&request, &mock, &store, AuthorityTier::LocalDeveloper)
            .await
            .expect("obtain builds");
        // Corrupt the stored artifact behind the store's back.
        std::fs::write(
            cas.path().join(format!(
                "bundles/local_developer/{}/{}",
                built.bundle.identity_digest().to_hex(),
                request.targets[0].0
            )),
            b"tampered!",
        )
        .expect("tamper");

        let err = obtain_provisioner(&request, &mock, &store, AuthorityTier::LocalDeveloper)
            .await
            .expect_err("tampering surfaces — no quiet rebuild");
        assert!(matches!(
            err,
            BuildError::BundleStore(BundleStoreError::Admission(_))
        ));
    }
}
