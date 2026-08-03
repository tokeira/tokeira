//! The provisioner bundle build (task 18.1; Proposal 005 §Build boundary).
//!
//! [`build_provisioner`] is the **sole** implementation of building, testing,
//! hashing, and packaging `tkp` — the same function whether a laptop's local
//! Dagger engine runs it or a Buildkite agent does (who *invoked* it is the
//! [`BuildAuthority`], not a different pipeline).
//!
//! The build is **hermetic and cold**: it consumes the materialized source
//! *snapshot* (task 17 — never the live tree), inside a build container
//! pinned **by digest** (a floating tag would poison the identity), with
//! `--locked` cargo invocations. No incremental `target/` is mounted — a
//! build that mounts one is not pure w.r.t. its declared inputs, and only a
//! pure build may be memoised by identity (Proposal 005, guardrail 3; the
//! dev loop uses native cargo instead and never comes through here).
//!
//! Artifacts are exported and then hashed/measured **host-side**: the
//! integrity manifest must attest the bytes that actually left the engine.
//! Tests run inside the same container, on the same frozen source; reaching
//! the packaging step at all means they passed ([`TestEvidence`]).

use std::{collections::BTreeSet, path::PathBuf};

use tokeira_provisioner::{
    BinaryArtifactDescriptor, BuildAuthority, BuildProfile, EngineIdentity, ProvisionerBundle,
    Sha256Digest, Target,
    bundle::{BuildManifest, TestEvidence},
    sha256_hex,
};

use crate::{
    BoundProvisionerSource, BuildError, DaggerClient, SourceSnapshot, materialize_snapshot,
    rust_toolchain_version,
};

/// System packages the build container needs on top of the pinned rust image
/// (identical to the tokeirad image build — the provisioner links the same
/// TLS/proto stack).
const APT_LINE: &str = "apt-get update && apt-get install -y --no-install-recommends pkg-config libssl-dev protobuf-compiler libprotobuf-dev ca-certificates && rm -rf /var/lib/apt/lists/*";

/// Everything [`build_provisioner`] consumes. The snapshot and closure come
/// from tasks 17's freeze/resolve; the request only carries decisions, never
/// live source.
#[derive(Debug, Clone)]
pub struct ProvisionerBuildRequest {
    /// The repository the snapshot's objects live in (also supplies
    /// `rust-toolchain.toml` for the toolchain identity input).
    pub workspace_root: PathBuf,
    /// Exact generated composition root and its three-root source closure.
    pub bound_source: BoundProvisionerSource,
    /// Target triples to build. Every successfully built target joins the
    /// bundle's manifest.
    pub targets: Vec<Target>,
    pub profile: BuildProfile,
    /// Who this build is for — recorded in the bundle; never changes *how*
    /// the function builds (that is the point).
    pub authority: BuildAuthority,
    /// The build container, pinned by digest (`…@sha256:…`). A floating tag
    /// is refused: the container is an identity input (guardrail 2).
    pub build_image: String,
    /// The frozen source (task 17). The build reads its tree, never the
    /// worktree.
    pub snapshot: SourceSnapshot,
    /// The resolved closure (crate names for the test set; canonical lock
    /// bytes for the identity's lock digest).
    /// Human-facing version label for the bundle (the workspace semver).
    pub version: String,
    /// Correlation id, recorded in the bundle's build manifest.
    pub request_id: String,
    /// Host directory the pipeline populates: `source/` (the materialized
    /// snapshot) and one `<target-triple>/tkp` per built target.
    pub output_dir: PathBuf,
}

/// Build, test, checksum, and package the provisioner. On success the
/// artifacts are on disk under `output_dir` and the returned bundle's
/// descriptors attest their exact bytes — ready for
/// [`BundleStore::publish`](tokeira_provisioner::BundleStore::publish).
pub fn build_provisioner(
    request: &ProvisionerBuildRequest,
    dagger: &dyn DaggerClient,
) -> Result<ProvisionerBundle, BuildError> {
    let identity = engine_identity_for(request)?;
    let toolchain = identity.toolchain.clone();

    // Feed the snapshot to the build — materialize the frozen tree and upload
    // that, never the live worktree.
    let source_dir = request.output_dir.join("source");
    materialize_snapshot(
        &request.workspace_root,
        &request.snapshot.tree_oid,
        &source_dir,
    )
    .map_err(|e| BuildError::Validation {
        reason: format!("failed to materialize the source snapshot: {e}"),
    })?;
    request
        .bound_source
        .materialize_in(&source_dir)
        .map_err(|error| BuildError::Validation {
            reason: format!("failed to materialize the generated composition root: {error}"),
        })?;
    let source = dagger.host_directory(&source_dir)?;

    // The base build environment: pinned image + system deps + the frozen
    // source. `--locked` everywhere — the lock is an identity input; drift
    // fails the build rather than silently re-resolving.
    let mut base = dagger.container_from(&request.build_image)?;
    base = base.with_exec(&["sh", "-c", APT_LINE])?;
    base = base.with_env("CARGO_TERM_COLOR", "never")?;
    base = base.with_env("RUSTUP_TOOLCHAIN", &toolchain)?;
    base = base.with_workdir("/src")?;
    base = base.with_directory("/src", &*source)?;
    // Tests: the closure's workspace crates, once (target-independent), on
    // the frozen source. A failure aborts the pipeline — no bundle exists.
    let mut test_args: Vec<String> = vec!["cargo".into(), "test".into(), "--locked".into()];
    for name in &request.bound_source.closure().crate_names {
        test_args.push("-p".into());
        test_args.push(name.clone());
    }
    let tester = base.clone_ref()?;
    tester.with_exec(&as_strs(&test_args))?;
    let tests = TestEvidence {
        command: test_args.join(" "),
        passed: true,
    };

    // Per-target: cross-build, strip, export, and attest host-side.
    let profile_flag = profile_name(request.profile);
    let mut artifacts = Vec::with_capacity(request.targets.len());
    for target in &request.targets {
        let mut builder = base.clone_ref()?;
        builder = builder.with_exec(&["rustup", "target", "add", &target.0])?;
        let build_args: Vec<String> = vec![
            "cargo".into(),
            "build".into(),
            "--locked".into(),
            "--profile".into(),
            profile_flag.to_string(),
            "--target".into(),
            target.0.clone(),
            "--manifest-path".into(),
            format!("/src/{}/Cargo.toml", crate::GENERATED_ROOT_RELATIVE_PATH),
        ];
        builder = builder.with_exec(&as_strs(&build_args))?;

        let built = format!(
            "/src/{}/target/{}/{}/{}",
            crate::GENERATED_ROOT_RELATIVE_PATH,
            target.0,
            profile_dir(request.profile),
            crate::GENERATED_PROVISIONER_BIN
        );
        builder = builder.with_exec(&["strip", &built])?;

        // Export, then hash and measure the exported bytes — the manifest
        // attests what left the engine, not what a container step claimed.
        let out_path = request.output_dir.join(&target.0).join("tkp");
        builder.file(&built)?.export(&out_path)?;
        let bytes = std::fs::read(&out_path).map_err(|e| BuildError::Validation {
            reason: format!(
                "failed to read exported artifact {}: {e}",
                out_path.display()
            ),
        })?;
        artifacts.push(BinaryArtifactDescriptor {
            target: target.clone(),
            sha256: sha256_hex(&bytes),
            retrieval_ref: None,
            size_bytes: bytes.len() as u64,
        });
    }

    Ok(ProvisionerBundle {
        identity,
        bound: Some(request.bound_source.evidence(&request.snapshot.tree_oid)),
        authority: request.authority.clone(),
        provisioner_version: request.version.clone(),
        artifacts,
        tests,
        build: BuildManifest {
            request_id: request.request_id.clone(),
            source_tree_oid: request.snapshot.tree_oid.clone(),
            snapshot_commit_oid: request.snapshot.commit_oid.clone(),
            toolchain,
            builder: builder_label(&request.authority),
        },
    })
}

/// The engine identity a request resolves to — computable **without
/// building** (task 18.3): the CAS is consulted by identity before any build
/// starts. Validates the request (including the digest-pin) on the way.
pub fn engine_identity_for(
    request: &ProvisionerBuildRequest,
) -> Result<EngineIdentity, BuildError> {
    let build_container = validate(request)?;
    let toolchain = rust_toolchain_version(&request.workspace_root)?;
    Ok(EngineIdentity {
        source_closure: request
            .bound_source
            .source_closure_digest(&request.snapshot.tree_oid),
        lock_closure: Sha256Digest::from_bytes(
            &request.bound_source.closure().canonical_lock_bytes(),
        ),
        toolchain,
        build_container: Some(build_container),
        features: BTreeSet::new(),
        profile: request.profile,
    })
}

/// Validate the request and extract the pinned build-container digest.
fn validate(request: &ProvisionerBuildRequest) -> Result<Sha256Digest, BuildError> {
    let refuse = |reason: String| Err(BuildError::Validation { reason });
    if request.targets.is_empty() {
        return refuse("no build targets requested".into());
    }
    if request.bound_source.closure().crate_names.is_empty() {
        return refuse("the resolved closure contains no workspace crates".into());
    }
    if request.snapshot.tree_oid.is_empty() {
        return refuse("the source snapshot carries no tree oid".into());
    }
    let Some((_, digest)) = request.build_image.split_once("@sha256:") else {
        return refuse(format!(
            "build image '{}' is not digest-pinned — a floating tag cannot be an \
             engine-identity input; pin it as <image>@sha256:<digest>",
            request.build_image
        ));
    };
    Sha256Digest::parse_manifest_hex(digest).map_or_else(
        |e| {
            refuse(format!(
                "build image '{}' carries a malformed digest: {e}",
                request.build_image
            ))
        },
        Ok,
    )
}

fn builder_label(authority: &BuildAuthority) -> String {
    match authority {
        BuildAuthority::LocalDeveloper => "dagger-local".to_string(),
        BuildAuthority::TrustedCi {
            provider, build_id, ..
        } => format!("{provider}:{build_id}"),
    }
}

fn profile_name(profile: BuildProfile) -> &'static str {
    match profile {
        BuildProfile::Dev => "dev",
        BuildProfile::Release => "release",
        BuildProfile::Dist => "dist",
    }
}

/// The `target/<triple>/<dir>` cargo writes a profile's artifacts to (`dev`
/// is the odd one out: its directory is `debug`).
fn profile_dir(profile: BuildProfile) -> &'static str {
    match profile {
        BuildProfile::Dev => "debug",
        BuildProfile::Release => "release",
        BuildProfile::Dist => "dist",
    }
}

fn as_strs(args: &[String]) -> Vec<&str> {
    args.iter().map(String::as_str).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ProvisionerClosure,
        testing::{MockCall, MockDaggerClient, mock_artifact_bytes},
    };
    use std::{path::Path, process::Command};

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
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    /// A committed workspace-shaped repo plus its frozen snapshot.
    fn snapshot_fixture(dir: &Path) -> SourceSnapshot {
        git(dir, &["init", "-q", "-b", "main"]);
        std::fs::create_dir_all(dir.join("platforms/alpha/src")).expect("mkdir");
        std::fs::write(
            dir.join("rust-toolchain.toml"),
            "[toolchain]\nchannel = \"1.95\"\n",
        )
        .expect("write");
        std::fs::write(dir.join("Cargo.lock"), "version = 4\n").expect("write");
        std::fs::write(dir.join("platforms/alpha/src/lib.rs"), "pub fn a() {}\n").expect("write");
        git(dir, &["add", "."]);
        git(dir, &["commit", "-q", "-m", "seed"]);
        crate::snapshot_source_closure(&crate::SnapshotRequest {
            repo_root: dir.to_path_buf(),
            closure_paths: vec![
                PathBuf::from("platforms/alpha"),
                PathBuf::from("rust-toolchain.toml"),
                PathBuf::from("Cargo.lock"),
            ],
            include_untracked: false,
        })
        .expect("snapshot")
    }

    fn closure() -> ProvisionerClosure {
        ProvisionerClosure {
            crate_dirs: vec![PathBuf::from("platforms/alpha")],
            crate_names: vec!["tokeira-alpha".into()],
            workspace_files: vec![
                PathBuf::from("rust-toolchain.toml"),
                PathBuf::from("Cargo.lock"),
            ],
            locked: vec![],
        }
    }

    fn request(repo: &Path, out: &Path) -> ProvisionerBuildRequest {
        ProvisionerBuildRequest {
            workspace_root: repo.to_path_buf(),
            bound_source: BoundProvisionerSource::testing(closure()),
            targets: vec![Target("aarch64-unknown-linux-gnu".into())],
            profile: BuildProfile::Dist,
            authority: BuildAuthority::LocalDeveloper,
            build_image: format!("rust:1.95-slim-bookworm@sha256:{}", "ab".repeat(32)),
            snapshot: snapshot_fixture(repo),
            version: "0.1.0".into(),
            request_id: "req-1".into(),
            output_dir: out.to_path_buf(),
        }
    }

    #[test]
    fn builds_tests_exports_and_attests_the_exported_bytes() {
        let repo = tempfile::tempdir().expect("tempdir");
        let out = tempfile::tempdir().expect("tempdir");
        let mock = MockDaggerClient::default();
        let request = request(repo.path(), out.path());

        let bundle = build_provisioner(&request, &mock).expect("pipeline");

        // Identity: closure-scoped inputs, pinned container, dist profile.
        assert_eq!(
            bundle.identity.source_closure,
            request
                .bound_source
                .source_closure_digest(&request.snapshot.tree_oid)
        );
        assert_eq!(bundle.identity.toolchain, "1.95");
        assert_eq!(
            bundle.identity.build_container,
            Some(Sha256Digest::parse_manifest_hex(&"ab".repeat(32)).unwrap())
        );

        // The artifact attests the EXPORTED bytes (the mock writes
        // deterministic bytes derived from the container path).
        let built =
            "/src/.tokeira-build/bound-provisioner/target/aarch64-unknown-linux-gnu/dist/tkp";
        assert_eq!(bundle.artifacts.len(), 1);
        assert_eq!(
            bundle.artifacts[0].sha256,
            sha256_hex(&mock_artifact_bytes(built))
        );
        let on_disk = std::fs::read(out.path().join("aarch64-unknown-linux-gnu/tkp"))
            .expect("exported artifact exists");
        assert_eq!(on_disk, mock_artifact_bytes(built));

        // The source fed to the engine is the MATERIALIZED snapshot dir.
        let calls = mock.calls();
        assert!(calls.iter().any(|c| matches!(
            c,
            MockCall::HostDirectory(p) if p.ends_with("/source")
        )));
        // Tests ran (once), with --locked and the closure's -p set.
        assert!(calls.contains(&MockCall::WithExec(vec![
            "cargo".into(),
            "test".into(),
            "--locked".into(),
            "-p".into(),
            "tokeira-alpha".into(),
        ])));
        // The cross-build targets the generated composition root.
        assert!(calls.contains(&MockCall::WithExec(vec![
            "cargo".into(),
            "build".into(),
            "--locked".into(),
            "--profile".into(),
            "dist".into(),
            "--target".into(),
            "aarch64-unknown-linux-gnu".into(),
            "--manifest-path".into(),
            "/src/.tokeira-build/bound-provisioner/Cargo.toml".into(),
        ])));
        assert!(bundle.tests.passed);
        assert_eq!(bundle.build.source_tree_oid, request.snapshot.tree_oid);

        // The bundle publishes as-is: descriptors match the on-disk bytes.
        bundle
            .integrity_manifest()
            .verify_artifact(&on_disk, &bundle.artifacts[0].target)
            .expect("exported bytes verify against the bundle manifest");
    }

    #[test]
    fn a_floating_tag_build_image_is_refused() {
        let repo = tempfile::tempdir().expect("tempdir");
        let out = tempfile::tempdir().expect("tempdir");
        let mock = MockDaggerClient::default();
        let mut request = request(repo.path(), out.path());
        request.build_image = "rust:1.95-slim-bookworm".into();

        let err = build_provisioner(&request, &mock).expect_err("floating tag refuses");
        assert!(
            err.to_string().contains("digest-pinned"),
            "unexpected: {err}"
        );
        assert!(mock.calls().is_empty(), "refused before any engine call");
    }

    #[test]
    fn empty_targets_and_empty_closure_are_refused() {
        let repo = tempfile::tempdir().expect("tempdir");
        let out = tempfile::tempdir().expect("tempdir");
        let mock = MockDaggerClient::default();

        let base = request(repo.path(), out.path());
        let mut no_targets = base.clone();
        no_targets.targets.clear();
        assert!(build_provisioner(&no_targets, &mock).is_err());

        let mut no_crates = base;
        no_crates.bound_source.testing_clear_crates();
        assert!(build_provisioner(&no_crates, &mock).is_err());
    }

    #[test]
    fn every_requested_target_is_built_and_attested() {
        let repo = tempfile::tempdir().expect("tempdir");
        let out = tempfile::tempdir().expect("tempdir");
        let mock = MockDaggerClient::default();
        let mut request = request(repo.path(), out.path());
        request.targets = vec![
            Target("aarch64-unknown-linux-gnu".into()),
            Target("x86_64-unknown-linux-gnu".into()),
        ];

        let bundle = build_provisioner(&request, &mock).expect("pipeline");
        assert_eq!(bundle.artifacts.len(), 2);
        for target in &request.targets {
            assert!(
                out.path().join(&target.0).join("tkp").exists(),
                "artifact for {} exported",
                target.0
            );
        }
    }
}
