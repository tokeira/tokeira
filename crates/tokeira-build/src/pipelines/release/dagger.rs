//! The Dagger executor for release trains: observation, preparation, publication.
//!
//! Invariants this module upholds:
//!
//! - **The operator's checkout is never mutated.** Every Git command runs against an
//!   engine-side snapshot of the repository (`/repo.git` plus a rewritten worktree
//!   pointer). The host receives nothing back; after a train the operator fetches the
//!   release branch and tag from the remote like anyone else.
//! - **Two credential phases never meet.** The registry token enters only the publish
//!   container. `GH_TOKEN` enters only the release-note container, in a later session,
//!   after parity. The operator's SSH agent is exposed only to the Git steps that need
//!   it and is removed before `cargo package` or `cargo publish` run dependency build
//!   scripts.
//! - **Rust decides, shell observes.** Each container step reports facts on stdout;
//!   the admission, the resume decision, parity, and the report classification are
//!   pure functions in `apply.rs` and `scripts.rs` with offline proofs.
//! - **Nothing outward is replayed.** Every container that observes or mutates the
//!   remote, the registry, or the release API carries a per-invocation nonce, so a
//!   rerun re-observes instead of reusing a recorded exec result.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use cargo_metadata::Metadata;
use dagger_sdk::{
    Client, Container, ContainerWithExecOpts, Directory, HostDirectoryOpts, ReturnType,
};

use crate::{BuildError, CHANGIE_RELEASE, DAGGER_RELEASE, rust_toolchain_version};

use super::{
    GitObservation, PackageIdentity, PackageOutcome, PackageResult, PlannedArtifact,
    PublishParityReport, RELEASE_SCHEMA_VERSION, RegistryObservation, ReleaseAdmission,
    ReleaseConfig, ReleaseDaggerClient, ReleaseDiagnostic, ReleaseError, ReleaseNotesOutcome,
    ReleaseNotesRequest, ReleaseNotesResult, ReleaseObservations, ReleasePlan, ReleasePlanRequest,
    ReleasePublishRequest, ReleaseReport, ReleaseVerifyRequest, RepositoryIdentity, TagResult,
    ToolchainIdentity, TrainIdentity, TrainPhaseFacts, admit_changelog_config, admit_fragments,
    admit_release_refs,
    apply::SUCCESS_COOLDOWN,
    atomic_git_push_arguments, cargo_package_arguments, cargo_package_arguments_for_names,
    classify_train_state, external_publish_dependencies, generate_release_notes,
    gh_release_create_arguments, plan_release, publishable_packages, registry_observation_delays,
    rewrite_workspace_manifests,
    scripts::{
        HERMETIC_CHECKSUM_SCRIPT, OBSERVATION_DELAYS_ENV, REGISTRY_PUBLISH_SCRIPT,
        REGISTRY_VERIFY_SCRIPT, RELEASE_PREPARE_FRESH_SCRIPT, RELEASE_PREPARE_RESUME_SCRIPT,
        RegistryStop, SUCCESS_COOLDOWN_ENV, VALIDATE_PREPARED_SOURCE_SCRIPT, parse_checksum_lines,
        parse_commit_line, parse_refs_line, parse_registry_output, release_observe_script,
    },
    sha256_hex, verify_published_refs, verify_resume_refs,
};
use crate::pipelines::{build::builder_toolchain, ci::GitLayout};

const RELEASE_SOURCE_EXCLUDES: &[&str] = &[
    "target",
    "**/target",
    ".git",
    ".tokeira-build",
    ".env*",
    "**/.env*",
    "artifacts",
    "**/*.log",
];

/// Environment variable carrying the per-invocation nonce.
const NONCE_ENV: &str = "TOKEIRA_RELEASE_INVOCATION";
/// Where the operator's SSH agent socket is mounted inside Git-facing steps.
const SSH_SOCKET_PATH: &str = "/run/tokeira-release-ssh-agent";
const RELEASE_API_IMAGE: &str = "ghcr.io/cli/cli:2.65.0";
const OBSERVATION_IMAGE: &str = "debian:bookworm-slim";
const OBSERVATION_APT: &str = "apt-get update && apt-get install -y --no-install-recommends git curl jq ca-certificates && rm -rf /var/lib/apt/lists/*";

/// Exact target-version planning evidence produced from one isolated source graph.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlannedReleaseArtifacts {
    /// Hermetic archive checksum and README facts by package name.
    pub packages: BTreeMap<String, PlannedArtifact>,
    /// Exact changie version file generated for the target version.
    pub version_body: String,
}

/// Public release-object facts used to avoid resolving `GH_TOKEN` for idempotent reruns.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReleaseObjectObservation {
    /// SHA-256 of the exact public notes body.
    pub notes_sha256: String,
    /// GitHub target commit-ish recorded on the release object.
    pub target: String,
}

/// A value no two invocations share, stamped onto every outward-facing step.
///
/// Dagger caches an exec by the digest of its inputs. Observations and mutations must
/// never be served from that cache, or a resume would trust last run's `ls-remote`
/// instead of looking again; the nonce makes every such step unique.
fn invocation_nonce() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or_default();
    format!("{nanos}-{}", std::process::id())
}

/// The operator's SSH agent socket, when one is advertised.
fn ssh_agent_socket() -> Option<PathBuf> {
    std::env::var_os("SSH_AUTH_SOCK")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
}

/// Forward the agent socket, never private-key bytes, into a Git-facing step.
fn with_agent(client: &Client, container: Container) -> Container {
    match ssh_agent_socket() {
        Some(path) => {
            let socket = client
                .query()
                .host()
                .unix_socket(path.display().to_string());
            container
                .with_unix_socket(SSH_SOCKET_PATH, socket)
                .with_env_variable("SSH_AUTH_SOCK", SSH_SOCKET_PATH)
        }
        None => container,
    }
}

/// Remove the agent before anything that compiles dependency build scripts runs.
fn without_agent(container: Container) -> Container {
    match ssh_agent_socket() {
        Some(_) => container
            .without_unix_socket(SSH_SOCKET_PATH)
            .without_env_variable("SSH_AUTH_SOCK"),
        None => container,
    }
}

fn expect_any() -> ContainerWithExecOpts {
    ContainerWithExecOpts::default().with_expect(ReturnType::Any)
}

/// What to tell an operator whose Git observation failed without an agent.
fn agent_hint() -> &'static str {
    if ssh_agent_socket().is_none() {
        "; an SSH origin needs the operator's agent (SSH_AUTH_SOCK is not set)"
    } else {
        ""
    }
}

/// Observe a public GitHub release without a credential.
pub async fn observe_release_object(
    repository: &str,
    tag: &str,
    client: &Client,
) -> Result<Option<ReleaseObjectObservation>, BuildError> {
    let output = client
        .query()
        .container()
        .from(OBSERVATION_IMAGE)
        .with_exec(vec!["sh", "-c", OBSERVATION_APT])
        .with_env_variable(NONCE_ENV, invocation_nonce())
        .with_env_variable("REPOSITORY", repository)
        .with_env_variable("RELEASE_TAG", tag)
        .with_exec(vec![
            "sh",
            "-c",
            r#"set -eu
code=$(curl --silent --show-error --location --output /tmp/release.json --write-out '%{http_code}' "https://api.github.com/repos/$REPOSITORY/releases/tags/$RELEASE_TAG")
case "$code" in
  200)
    jq -j '.body' /tmp/release.json >/tmp/notes
    digest=$(sha256sum /tmp/notes | cut -d' ' -f1)
    target=$(jq -r '.target_commitish' /tmp/release.json)
    printf 'existing\t%s\t%s\n' "$digest" "$target"
    ;;
  404) printf 'absent\n' ;;
  *) echo "release observation failed with HTTP $code" >&2; exit 1 ;;
esac"#,
        ])
        .stdout()
        .await?;
    let trimmed = output.trim();
    if trimmed == "absent" {
        return Ok(None);
    }
    let fields = trimmed.split('\t').collect::<Vec<_>>();
    if fields.len() != 3 || fields[0] != "existing" {
        return Err(BuildError::Validation {
            reason: format!("release API observation was malformed: {trimmed}"),
        });
    }
    Ok(Some(ReleaseObjectObservation {
        notes_sha256: fields[1].to_owned(),
        target: fields[2].to_owned(),
    }))
}

#[async_trait]
impl ReleaseDaggerClient for Client {
    async fn execute_publish_and_parity(
        &self,
        request: &ReleasePublishRequest,
    ) -> Result<PublishParityReport, ReleaseError> {
        execute_publish_and_parity(self, request).await
    }

    async fn execute_release_notes(
        &self,
        request: &ReleaseNotesRequest,
    ) -> Result<ReleaseReport, ReleaseError> {
        execute_release_notes(self, request).await
    }

    async fn execute_verify(
        &self,
        request: &ReleaseVerifyRequest,
    ) -> Result<ReleaseReport, ReleaseError> {
        execute_verify(self, request).await
    }
}

/// The first invocation: admission, preparation, atomic Git publication, registry
/// publication in dependency order, and parity — with only the registry token.
async fn execute_publish_and_parity(
    client: &Client,
    request: &ReleasePublishRequest,
) -> Result<PublishParityReport, ReleaseError> {
    let plan = &request.plan;
    let nonce = invocation_nonce();
    let metadata = cargo_metadata::MetadataCommand::new()
        .manifest_path(plan.workspace_root.join("Cargo.toml"))
        .other_options(["--locked".to_owned()])
        .exec()
        .map_err(|source| {
            executor_error(format!("could not read locked Cargo metadata: {source}"))
        })?;
    let layout = GitLayout::resolve(&plan.workspace_root)
        .map_err(|source| executor_error(source.to_string()))?;
    let config = ReleaseConfig::load(&plan.workspace_root, &plan.repository)?;
    let prepared = rewritten_source_directory(client, plan, &metadata, &config)?;
    let query = client.query();
    let git = query
        .host()
        .directory(layout.common_dir.display().to_string());
    let registry_cache = query.cache_volume("tokeira-release-registry");
    let target_cache = query.cache_volume(format!("tokeira-release-target-{}", plan.digest));
    let acquisition =
        changie_acquisition_script().map_err(|source| executor_error(source.to_string()))?;
    let author_name = git_config(&plan.workspace_root, "user.name")?;
    let author_email = git_config(&plan.workspace_root, "user.email")?;

    // Preparation: pinned changie batch and merge over the rewritten source, then a
    // proof that only release-owned paths changed. Local work, no agent, no secret.
    let workspace = builder_toolchain(&query, &plan.toolchain.rust)
        .with_mounted_cache(registry_cache, "/usr/local/cargo/registry")
        .with_directory("/workspace", prepared)
        .with_directory("/repo.git", git)
        .with_new_file(&layout.worktree_pointer, "/workspace/.git")
        .with_workdir("/workspace")
        .with_env_variable(NONCE_ENV, &nonce)
        .with_env_variable("RELEASE_TAG", &plan.tag)
        .with_env_variable("RELEASE_BRANCH", &config.release_branch)
        .with_env_variable("RELEASE_VERSION", &plan.target_version)
        .with_env_variable("PLAN_DIGEST", &plan.digest)
        .with_env_variable("GIT_AUTHOR_NAME", &author_name)
        .with_env_variable("GIT_AUTHOR_EMAIL", &author_email)
        .with_env_variable("GIT_COMMITTER_NAME", &author_name)
        .with_env_variable("GIT_COMMITTER_EMAIL", &author_email)
        .with_new_file(allowed_paths(plan, &config), "/tmp/release-allowed-paths")
        .with_new_file(fragment_paths(plan), "/tmp/release-fragments")
        .with_exec(vec!["sh", "-c", &acquisition])
        .with_exec(vec![
            "/tmp/changie",
            "batch",
            &plan.target_version,
            "--allow-no-changes=false",
        ])
        .with_exec(vec!["/tmp/changie", "merge"])
        // Only the isolated prepared source owns the Cargo-generated lock rewrite.
        .with_exec(vec![
            "cargo",
            "metadata",
            "--offline",
            "--format-version",
            "1",
        ])
        .with_exec(vec!["sh", "-c", VALIDATE_PREPARED_SOURCE_SCRIPT]);

    // Admission: one observation of both refs and the push target, then a pure
    // decision. The agent is present only for `ls-remote`/`fetch`.
    let observed =
        with_agent(client, workspace).with_exec(vec!["sh", "-c", &release_observe_script()]);
    let refs_output = observed.stdout().await.map_err(|source| {
        executor_error(format!(
            "release ref observation failed: {source}{}",
            agent_hint()
        ))
    })?;
    let observation = parse_refs_line(&refs_output)?;
    admit_push_target(&plan.repository, &observation.push_url)?;
    let admission = admit_release_refs(
        &plan.base_commit,
        &plan.tag,
        &plan.digest,
        &observation.refs,
    )?;
    let prepared_source = match &admission {
        ReleaseAdmission::Fresh => {
            observed.with_exec(vec!["sh", "-c", RELEASE_PREPARE_FRESH_SCRIPT])
        }
        ReleaseAdmission::Resume { .. } => {
            observed.with_exec(vec!["sh", "-c", RELEASE_PREPARE_RESUME_SCRIPT])
        }
    };
    let commit_output = prepared_source
        .stdout()
        .await
        .map_err(|source| executor_error(format!("release preparation failed: {source}")))?;
    let commit = parse_commit_line(&commit_output)?;
    if let ReleaseAdmission::Resume { commit: published } = &admission
        && published != &commit
    {
        return Err(executor_error(format!(
            "resumed tag extracted {commit} but the remote peels to {published}"
        )));
    }
    let git_published = matches!(admission, ReleaseAdmission::Resume { .. });

    // Hermetic Tag Build: the exact tagged tree, packaged in one invocation, with
    // the agent removed before any dependency build script can run.
    let packaged = without_agent(
        prepared_source
            .with_mounted_cache(target_cache, "/release-source/target")
            .with_workdir("/release-source"),
    )
    .with_exec_opts(cargo_argv(cargo_package_arguments(plan)), &expect_any());
    let package_code = packaged
        .exit_code()
        .await
        .map_err(|source| executor_error(format!("Hermetic Tag Build graph failed: {source}")))?;
    if package_code != 0 {
        let stderr = packaged.stderr().await.unwrap_or_default();
        return Err(stopped(
            plan,
            git_published,
            &commit,
            ReleaseError::PackageDryRun {
                reason: format!(
                    "Hermetic Tag Build exited with status {package_code}: {}",
                    tail(&stderr)
                ),
            },
            Vec::new(),
            Vec::new(),
        ));
    }
    // The bytes the operator confirmed are the only bytes this train may publish.
    let checksum_output = packaged
        .with_exec(vec!["sh", "-c", HERMETIC_CHECKSUM_SCRIPT])
        .stdout()
        .await
        .map_err(|source| executor_error(format!("hermetic checksum step failed: {source}")))?;
    let checksums = parse_checksum_lines(&checksum_output);
    for package in &plan.packages {
        let archive = format!("{}-{}.crate", package.name, package.target_version);
        let condition = match checksums.get(&archive) {
            Some(observed) if *observed == package.hermetic_sha256 => continue,
            Some(observed) => ReleaseError::HermeticBuildDrift {
                package: package.name.clone(),
                version: package.target_version.clone(),
                planned: package.hermetic_sha256.clone(),
                observed: observed.clone(),
            },
            None => ReleaseError::PackageDryRun {
                reason: format!("Hermetic Tag Build omitted {archive}"),
            },
        };
        return Err(stopped(
            plan,
            git_published,
            &commit,
            condition,
            Vec::new(),
            Vec::new(),
        ));
    }

    // Atomic Git publication, fresh trains only. The push's own status is not the
    // evidence: the remote is re-observed and both refs must identify the train.
    let published_commit = match &admission {
        ReleaseAdmission::Fresh => {
            let pushed = with_agent(client, prepared_source.with_workdir("/workspace"))
                .with_exec_opts(
                    git_argv(atomic_git_push_arguments(
                        "origin",
                        &config.release_branch,
                        &plan.tag,
                    )),
                    &expect_any(),
                );
            let push_code = pushed.exit_code().await.map_err(|source| {
                executor_error(format!(
                    "atomic release ref publication graph failed: {source}"
                ))
            })?;
            if push_code != 0 {
                let stderr = pushed.stderr().await.unwrap_or_default();
                return Err(stopped(
                    plan,
                    false,
                    &commit,
                    parse_push_failure(plan, &stderr),
                    Vec::new(),
                    Vec::new(),
                ));
            }
            let after = pushed
                .with_exec(vec!["sh", "-c", &release_observe_script()])
                .stdout()
                .await
                .map_err(|source| {
                    executor_error(format!("post-push ref observation failed: {source}"))
                })?;
            let after = parse_refs_line(&after)?;
            verify_resume_refs(&plan.tag, &commit, &plan.digest, &after.refs)?;
            commit.clone()
        }
        ReleaseAdmission::Resume { commit } => commit.clone(),
    };

    // Registry publication and parity, with the registry token as the only secret
    // and the Rust-owned observation schedule handed to the script verbatim.
    let delays = registry_observation_delays()
        .iter()
        .map(|delay| delay.as_secs().to_string())
        .collect::<Vec<_>>()
        .join(" ");
    // `packaged` already runs without the agent; publication inherits that.
    let mut publish = packaged
        .with_workdir("/release-source")
        .with_env_variable(OBSERVATION_DELAYS_ENV, delays)
        .with_env_variable(SUCCESS_COOLDOWN_ENV, SUCCESS_COOLDOWN.as_secs().to_string());
    if let Some(credential) = &request.registry_credential {
        let secret = query.set_secret("cargo_registry_token", credential.expose());
        publish = publish.with_secret_variable("CARGO_REGISTRY_TOKEN", secret);
    }
    let mut publish_arguments = vec![
        "sh".to_owned(),
        "-c".to_owned(),
        REGISTRY_PUBLISH_SCRIPT.to_owned(),
        "--".to_owned(),
    ];
    publish_arguments.extend(package_specs(
        plan.packages
            .iter()
            .map(|package| (package.name.as_str(), package.target_version.as_str())),
    ));
    let execution = publish.with_exec_opts(publish_arguments, &expect_any());
    let exit_code = execution.exit_code().await.map_err(|source| {
        executor_error(format!("publish-and-parity Dagger graph failed: {source}"))
    })?;
    let output = execution.stdout().await.map_err(|source| {
        executor_error(format!(
            "publish-and-parity output was unavailable: {source}"
        ))
    })?;
    let registry = parse_registry_output(&output)?;
    if let Some(stop) = registry.stop {
        return Err(stopped(
            plan,
            true,
            &published_commit,
            stop.into_error(),
            registry.packages,
            registry.diagnostics,
        ));
    }
    if exit_code != 0 {
        return Err(stopped(
            plan,
            true,
            &published_commit,
            ReleaseError::RegistryPublish {
                package: "unknown".to_owned(),
                version: plan.target_version.clone(),
                reason: format!("Dagger registry process exited with status {exit_code}"),
            },
            registry.packages,
            registry.diagnostics,
        ));
    }
    let version_body = execution
        .file(format!(
            "/release-source/.changes/{}.md",
            plan.target_version
        ))
        .contents()
        .await
        .map_err(|source| {
            executor_error(format!(
                "could not read the tagged changie version body: {source}"
            ))
        })?;
    let notes = generate_release_notes(&version_body, &registry.packages)?;
    Ok(PublishParityReport {
        schema_version: RELEASE_SCHEMA_VERSION,
        train: TrainIdentity::from(plan),
        packages: registry.packages,
        tag: TagResult {
            tag: plan.tag.clone(),
            commit: published_commit,
            published: true,
        },
        release_notes_sha256: sha256_hex(&notes),
        diagnostics: registry_diagnostics(registry.diagnostics),
    })
}

/// The second invocation: immutable release notes, with `GH_TOKEN` as the only secret.
///
/// The notes are generated here from the tagged version file and the parity report,
/// so the executed bytes are exactly what `generate_release_notes` proves offline.
async fn execute_release_notes(
    client: &Client,
    request: &ReleaseNotesRequest,
) -> Result<ReleaseReport, ReleaseError> {
    let parity = &request.parity;
    let tag = &parity.tag.tag;
    let repository = &parity.train.repository.slug;
    let query = client.query();
    let secret = query.set_secret(
        "github_release_token",
        request.release_api_credential.expose(),
    );
    let fetched = query
        .container()
        .from(RELEASE_API_IMAGE)
        .with_entrypoint(Vec::<String>::new())
        .with_secret_variable("GH_TOKEN", secret)
        .with_env_variable(NONCE_ENV, invocation_nonce())
        .with_env_variable("RELEASE_TAG", tag)
        .with_env_variable("REPOSITORY", repository)
        .with_env_variable("RELEASE_VERSION", &parity.train.target_version)
        .with_exec(vec![
            "sh",
            "-c",
            r#"set -eu
gh api -H 'Accept: application/vnd.github.raw+json' "repos/$REPOSITORY/contents/.changes/$RELEASE_VERSION.md?ref=$RELEASE_TAG" >/tmp/version-body"#,
        ]);
    let version_body = fetched
        .file("/tmp/version-body")
        .contents()
        .await
        .map_err(|source| {
            executor_error(format!("could not read the tagged version file: {source}"))
        })?;
    let notes = generate_release_notes(&version_body, &parity.packages)?;
    let observed_digest = sha256_hex(&notes);
    if observed_digest != parity.release_notes_sha256 {
        return Err(ReleaseError::Plan {
            reason: format!(
                "tagged release notes digest {observed_digest} differs from the parity report's {}",
                parity.release_notes_sha256
            ),
        });
    }
    let notes_text = String::from_utf8(notes)
        .map_err(|source| executor_error(format!("release notes are not UTF-8: {source}")))?;

    let viewed = fetched.with_exec_opts(
        vec![
            "gh",
            "release",
            "view",
            tag,
            "--repo",
            repository,
            "--json",
            "body,tagName,targetCommitish",
        ],
        &expect_any(),
    );
    let view_code = viewed
        .exit_code()
        .await
        .map_err(|source| executor_error(format!("release view graph failed: {source}")))?;
    let outcome = if view_code == 0 {
        let body = viewed.stdout().await.map_err(|source| {
            executor_error(format!("release view output was unavailable: {source}"))
        })?;
        let existing: serde_json::Value = serde_json::from_str(&body).map_err(|source| {
            executor_error(format!("release view output was malformed: {source}"))
        })?;
        let existing_digest = sha256_hex(existing["body"].as_str().unwrap_or_default().as_bytes());
        if existing["tagName"].as_str() != Some(tag.as_str())
            || existing["targetCommitish"].as_str() != Some(parity.tag.commit.as_str())
            || existing_digest != observed_digest
        {
            return Err(ReleaseError::ReleaseConflict {
                tag: tag.clone(),
                reason: "existing release differs from the immutable train".to_owned(),
            });
        }
        ReleaseNotesOutcome::ExistingVerified
    } else {
        let created = fetched
            .with_new_file(&notes_text, "/release/notes.md")
            .with_exec_opts(
                gh_argv(gh_release_create_arguments(
                    repository,
                    tag,
                    &parity.tag.commit,
                    "/release/notes.md",
                )),
                &expect_any(),
            );
        let create_code = created
            .exit_code()
            .await
            .map_err(|source| executor_error(format!("release create graph failed: {source}")))?;
        if create_code != 0 {
            let stderr = created.stderr().await.unwrap_or_default();
            return Err(ReleaseError::ReleaseConflict {
                tag: tag.clone(),
                reason: format!(
                    "gh release create exited with status {create_code}: {}",
                    tail(&stderr)
                ),
            });
        }
        ReleaseNotesOutcome::Created
    };
    Ok(ReleaseReport {
        schema_version: RELEASE_SCHEMA_VERSION,
        train: parity.train.clone(),
        state: super::TrainState::Complete,
        packages: parity.packages.clone(),
        tag: parity.tag.clone(),
        release_notes: ReleaseNotesResult {
            outcome,
            sha256: observed_digest,
        },
        diagnostics: parity.diagnostics.clone(),
    })
}

/// Read-only verification of a published or partially published train.
async fn execute_verify(
    client: &Client,
    request: &ReleaseVerifyRequest,
) -> Result<ReleaseReport, ReleaseError> {
    let layout = GitLayout::resolve(&request.workspace_root)
        .map_err(|source| executor_error(source.to_string()))?;
    let query = client.query();
    let source = query.host().directory_opts(
        request.workspace_root.display().to_string(),
        &HostDirectoryOpts::default().with_exclude(RELEASE_SOURCE_EXCLUDES.to_vec()),
    );
    let git = query
        .host()
        .directory(layout.common_dir.display().to_string());
    let base = builder_toolchain(
        &query,
        &rust_toolchain_version(&request.workspace_root)
            .map_err(|source| executor_error(source.to_string()))?,
    )
    .with_directory("/workspace", source)
    .with_directory("/repo.git", git)
    .with_new_file(&layout.worktree_pointer, "/workspace/.git")
    .with_workdir("/workspace")
    .with_env_variable(NONCE_ENV, invocation_nonce())
    .with_env_variable("RELEASE_TAG", &request.tag)
    .with_env_variable("RELEASE_BRANCH", &request.release_branch);
    let observed = with_agent(client, base).with_exec(vec!["sh", "-c", &release_observe_script()]);
    let refs_output = observed.stdout().await.map_err(|source| {
        executor_error(format!("release ref observation graph failed: {source}"))
    })?;
    let observation = parse_refs_line(&refs_output)?;
    let tag = verify_published_refs(
        &request.tag,
        &observation.refs,
        observation.tag_commit_digest.as_deref(),
        observation.branch_contains_tag,
        request.expected_plan_digest.as_deref(),
    )?;
    let plan_digest = observation
        .refs
        .tag
        .as_ref()
        .map(|observed| observed.plan_digest.clone())
        .unwrap_or_default();
    let train = TrainIdentity {
        repository: request.repository.clone(),
        base_commit: observation.tag_parent.clone().unwrap_or_default(),
        target_version: request.version.clone(),
        plan_digest,
    };

    let tagged = without_agent(observed.with_exec(vec!["sh", "-c", RELEASE_PREPARE_RESUME_SCRIPT]))
        .with_workdir("/release-source")
        .with_exec(vec![
            "cargo",
            "metadata",
            "--locked",
            "--format-version",
            "1",
        ]);
    let tagged_metadata = tagged.stdout().await.map_err(|source| {
        executor_error(format!("tagged Cargo metadata graph failed: {source}"))
    })?;
    let metadata: Metadata = serde_json::from_str(&tagged_metadata).map_err(|source| {
        executor_error(format!("tagged Cargo metadata was malformed: {source}"))
    })?;
    let packages = publishable_packages(&metadata)?;
    let names = packages
        .iter()
        .map(|package| package.name.as_str())
        .collect::<Vec<_>>();
    let mut verify_arguments = vec![
        "sh".to_owned(),
        "-c".to_owned(),
        REGISTRY_VERIFY_SCRIPT.to_owned(),
        "--".to_owned(),
    ];
    verify_arguments.extend(package_specs(
        names.iter().map(|name| (*name, request.version.as_str())),
    ));
    let version_body = tagged
        .file(format!("/release-source/.changes/{}.md", request.version))
        .contents()
        .await
        .map_err(|source| {
            executor_error(format!(
                "could not read the tagged changie version file: {source}"
            ))
        })?;
    let execution = tagged
        .with_exec(cargo_argv(cargo_package_arguments_for_names(
            names.iter().copied(),
        )))
        .with_exec_opts(verify_arguments, &expect_any());
    let exit_code = execution.exit_code().await.map_err(|source| {
        executor_error(format!(
            "release verification Dagger graph failed: {source}"
        ))
    })?;
    let output = execution.stdout().await.map_err(|source| {
        executor_error(format!(
            "release verification output was unavailable: {source}"
        ))
    })?;
    let registry = parse_registry_output(&output)?;
    if let Some(stop) = registry.stop {
        return Err(incomplete(
            train,
            true,
            tag,
            stop,
            registry.packages,
            registry.diagnostics,
        ));
    }
    if exit_code != 0 {
        return Err(executor_error(format!(
            "registry verification exited with status {exit_code}"
        )));
    }
    let expected_notes = generate_release_notes(&version_body, &registry.packages)?;
    let expected_notes_sha256 = sha256_hex(&expected_notes);
    let release = observe_release_object(&request.repository.slug, &request.tag, client)
        .await
        .map_err(|source| executor_error(format!("release API observation failed: {source}")))?;
    let (state, release_notes) = match release {
        Some(existing)
            if existing.notes_sha256 == expected_notes_sha256 && existing.target == tag.commit =>
        {
            (
                super::TrainState::Complete,
                ReleaseNotesResult {
                    outcome: ReleaseNotesOutcome::ExistingVerified,
                    sha256: expected_notes_sha256,
                },
            )
        }
        Some(existing) => {
            return Err(ReleaseError::ReleaseConflict {
                tag: request.tag.clone(),
                reason: format!(
                    "observed target {} and notes digest {}; expected target {} and notes digest {}",
                    existing.target, existing.notes_sha256, tag.commit, expected_notes_sha256
                ),
            });
        }
        None => (
            super::TrainState::PartiallyPublished,
            ReleaseNotesResult {
                outcome: ReleaseNotesOutcome::Absent,
                sha256: expected_notes_sha256,
            },
        ),
    };
    Ok(ReleaseReport {
        schema_version: RELEASE_SCHEMA_VERSION,
        train,
        state,
        packages: registry.packages,
        tag,
        release_notes,
        diagnostics: registry_diagnostics(registry.diagnostics),
    })
}

/// Wrap a stopped publish invocation in the report that classifies it.
fn stopped(
    plan: &ReleasePlan,
    git_published: bool,
    commit: &str,
    condition: ReleaseError,
    packages: Vec<PackageResult>,
    diagnostics: Vec<String>,
) -> ReleaseError {
    let tag = TagResult {
        tag: plan.tag.clone(),
        commit: commit.to_owned(),
        published: git_published,
    };
    let stop = matches!(
        condition,
        ReleaseError::ArtifactMismatch { .. } | ReleaseError::ReleaseConflict { .. }
    );
    let facts = TrainPhaseFacts {
        pre_publication_failure: !git_published,
        git_published,
        any_package_public: !packages.is_empty(),
        all_packages_verified: packages.len() == plan.packages.len()
            && packages.iter().all(|package| {
                matches!(
                    package.outcome,
                    PackageOutcome::Published | PackageOutcome::ExistingVerified
                )
            }),
        terminal_mismatch: stop,
        release_notes_complete: false,
    };
    let state = classify_train_state(facts);
    let mut report_diagnostics = vec![ReleaseDiagnostic {
        code: condition.code().to_owned(),
        summary: condition.to_string(),
        details: None,
    }];
    report_diagnostics.extend(registry_diagnostics(diagnostics));
    let report = ReleaseReport {
        schema_version: RELEASE_SCHEMA_VERSION,
        train: TrainIdentity::from(plan),
        state,
        packages,
        tag,
        release_notes: ReleaseNotesResult {
            outcome: ReleaseNotesOutcome::Absent,
            sha256: String::new(),
        },
        diagnostics: report_diagnostics,
    };
    ReleaseError::Incomplete {
        state,
        condition: Box::new(condition),
        report: Box::new(report),
    }
}

/// Wrap a verification that found a registry stop in the report that classifies it.
fn incomplete(
    train: TrainIdentity,
    git_published: bool,
    tag: TagResult,
    stop: RegistryStop,
    packages: Vec<PackageResult>,
    diagnostics: Vec<String>,
) -> ReleaseError {
    let terminal_mismatch = matches!(stop, RegistryStop::Mismatch { .. });
    let condition = stop.into_error();
    let facts = TrainPhaseFacts {
        pre_publication_failure: !git_published,
        git_published,
        any_package_public: !packages.is_empty(),
        all_packages_verified: false,
        terminal_mismatch,
        release_notes_complete: false,
    };
    let state = classify_train_state(facts);
    let mut report_diagnostics = vec![ReleaseDiagnostic {
        code: condition.code().to_owned(),
        summary: condition.to_string(),
        details: None,
    }];
    report_diagnostics.extend(registry_diagnostics(diagnostics));
    let report = ReleaseReport {
        schema_version: RELEASE_SCHEMA_VERSION,
        train,
        state,
        packages,
        tag,
        release_notes: ReleaseNotesResult {
            outcome: ReleaseNotesOutcome::Absent,
            sha256: String::new(),
        },
        diagnostics: report_diagnostics,
    };
    ReleaseError::Incomplete {
        state,
        condition: Box::new(condition),
        report: Box::new(report),
    }
}

fn registry_diagnostics(diagnostics: Vec<String>) -> Vec<ReleaseDiagnostic> {
    diagnostics
        .into_iter()
        .map(|summary| ReleaseDiagnostic {
            code: "registry_evidence".to_owned(),
            summary,
            details: None,
        })
        .collect()
}

/// Refuse to publish anywhere but the Plan's repository.
///
/// The container resolves `origin` from the snapshot of the repository's own config,
/// so a repo-local `pushurl` or `insteadOf` could otherwise redirect the release. The
/// push URL is normalized exactly as the Plan's identity was and must match it.
fn admit_push_target(repository: &RepositoryIdentity, push_url: &str) -> Result<(), ReleaseError> {
    let observed = RepositoryIdentity::from_remote(push_url).map_err(|_| {
        ReleaseError::RepositoryMismatch {
            expected: repository.remote.clone(),
            observed: push_url.to_owned(),
        }
    })?;
    if observed.remote != repository.remote {
        return Err(ReleaseError::RepositoryMismatch {
            expected: repository.remote.clone(),
            observed: observed.remote,
        });
    }
    Ok(())
}

fn allowed_paths(plan: &ReleasePlan, config: &ReleaseConfig) -> String {
    let mut allowed = plan
        .packages
        .iter()
        .map(|package| package.manifest_path.display().to_string())
        .collect::<BTreeSet<_>>();
    allowed.extend(
        config
            .extra_version_fields
            .iter()
            .map(|field| field.path.display().to_string()),
    );
    allowed.extend([
        "Cargo.toml".to_owned(),
        "Cargo.lock".to_owned(),
        "CHANGELOG.md".to_owned(),
        format!(".changes/{}.md", plan.target_version),
    ]);
    allowed.extend(
        plan.fragments
            .iter()
            .map(|fragment| fragment.path.display().to_string()),
    );
    allowed.into_iter().collect::<Vec<_>>().join("\n") + "\n"
}

fn fragment_paths(plan: &ReleasePlan) -> String {
    plan.fragments
        .iter()
        .map(|fragment| fragment.path.display().to_string())
        .collect::<Vec<_>>()
        .join("\n")
        + "\n"
}

fn package_specs<'a>(packages: impl Iterator<Item = (&'a str, &'a str)>) -> Vec<String> {
    packages
        .map(|(name, version)| format!("{name}={version}"))
        .collect()
}

fn cargo_argv(arguments: Vec<String>) -> Vec<String> {
    let mut argv = vec!["cargo".to_owned()];
    argv.extend(arguments);
    argv
}

fn git_argv(arguments: Vec<String>) -> Vec<String> {
    let mut argv = vec!["git".to_owned()];
    argv.extend(arguments);
    argv
}

fn gh_argv(arguments: Vec<String>) -> Vec<String> {
    let mut argv = vec!["gh".to_owned()];
    argv.extend(arguments);
    argv
}

/// The last few lines of a process stream, on one line, for a diagnostic.
fn tail(stream: &str) -> String {
    let lines = stream.lines().collect::<Vec<_>>();
    lines[lines.len().saturating_sub(5)..]
        .join(" | ")
        .trim()
        .to_owned()
}

fn rewritten_source_directory(
    client: &Client,
    plan: &ReleasePlan,
    metadata: &Metadata,
    config: &ReleaseConfig,
) -> Result<Directory, ReleaseError> {
    let internal = plan
        .packages
        .iter()
        .map(|package| package.name.clone())
        .collect::<BTreeSet<_>>();
    let current_version = plan
        .packages
        .first()
        .map(|package| package.from_version.as_str())
        .ok_or_else(|| ReleaseError::Workspace {
            reason: "release Plan has no publishable package".to_owned(),
        })?;
    let manifests = plan
        .packages
        .iter()
        .map(|package_plan| {
            let package = metadata
                .packages
                .iter()
                .find(|package| package.name.as_ref() == package_plan.name)
                .ok_or_else(|| ReleaseError::Workspace {
                    reason: format!("Cargo metadata omitted {}", package_plan.name),
                })?;
            let relative = package
                .manifest_path
                .as_std_path()
                .strip_prefix(&plan.workspace_root)
                .map_err(|_| ReleaseError::Workspace {
                    reason: format!(
                        "manifest for {} is outside the workspace",
                        package_plan.name
                    ),
                })?;
            Ok((package_plan.name.clone(), relative.to_path_buf()))
        })
        .collect::<Result<Vec<_>, ReleaseError>>()?;
    let rewritten_files = rewrite_workspace_manifests(
        &plan.workspace_root,
        &manifests,
        &internal,
        current_version,
        &plan.target_version,
        &config.extra_version_fields,
    )?;
    let query = client.query();
    let source = query.host().directory_opts(
        plan.workspace_root.display().to_string(),
        &HostDirectoryOpts::default().with_exclude(RELEASE_SOURCE_EXCLUDES.to_vec()),
    );
    let mut prepared = source;
    for (path, text) in rewritten_files {
        prepared = prepared.with_new_file(text, path.display().to_string());
    }
    Ok(prepared)
}

fn git_config(root: &Path, key: &str) -> Result<String, ReleaseError> {
    let output = Command::new("git")
        .args(["-C"])
        .arg(root)
        .args(["config", "--get", key])
        .output()
        .map_err(|source| executor_error(format!("could not read Git {key}: {source}")))?;
    let value = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if !output.status.success() || value.is_empty() {
        return Err(executor_error(format!(
            "Git {key} is required to author the Release Commit"
        )));
    }
    Ok(value)
}

/// Classify a failed atomic push from Git's stderr.
///
/// A `[rejected]` line naming the tag means the remote refused the transaction as a
/// whole; Git's `--atomic` contract guarantees the branch was not moved either.
fn parse_push_failure(plan: &ReleasePlan, stderr: &str) -> ReleaseError {
    if stderr.contains("[rejected]") && stderr.contains(&plan.tag) {
        return ReleaseError::TagConflict {
            tag: plan.tag.clone(),
            expected: format!("sha256:{}", plan.digest),
            observed: "remote rejected the atomic tag update".to_owned(),
        };
    }
    if stderr.contains("[rejected]") || stderr.contains("[remote rejected]") {
        return ReleaseError::GitRefConflict {
            branch_observed: "remote rejected the atomic branch update".to_owned(),
            tag_observed: "absent".to_owned(),
        };
    }
    executor_error(format!(
        "atomic release ref publication failed: {}",
        tail(stderr)
    ))
}

fn executor_error(reason: String) -> ReleaseError {
    ReleaseError::Executor { reason }
}

/// Execute the complete read-only Dagger observation and seal a canonical Plan.
pub async fn plan_release_with_dagger(
    workspace_root: &Path,
    target_version: &str,
    base_ref: Option<&str>,
    repository: RepositoryIdentity,
    client: &Client,
) -> Result<ReleasePlan, ReleaseError> {
    let metadata = cargo_metadata::MetadataCommand::new()
        .manifest_path(workspace_root.join("Cargo.toml"))
        .other_options(["--locked".to_owned()])
        .exec()
        .map_err(|source| ReleaseError::Workspace {
            reason: format!("could not read locked Cargo metadata: {source}"),
        })?;
    let graph = publishable_packages(&metadata)?;
    let target = cargo_metadata::semver::Version::parse(target_version).map_err(|source| {
        ReleaseError::TargetVersion {
            reason: source.to_string(),
        }
    })?;
    if !target.pre.is_empty() || !target.build.is_empty() {
        return Err(ReleaseError::TargetVersion {
            reason: "release target must be stable SemVer without pre-release/build metadata"
                .to_owned(),
        });
    }
    let versions = graph
        .iter()
        .filter_map(|node| {
            metadata
                .packages
                .iter()
                .find(|package| package.id.repr == node.package_id)
        })
        .map(|package| package.version.clone())
        .collect::<BTreeSet<_>>();
    if versions.len() != 1 {
        return Err(ReleaseError::NonUnifiedVersion {
            versions: versions.iter().map(ToString::to_string).collect(),
        });
    }
    let current = versions
        .first()
        .expect("non-empty publish graph has one unified version");
    if target <= *current {
        return Err(ReleaseError::TargetVersion {
            reason: format!("target {target} must be greater than unified version {current}"),
        });
    }
    let package_identities = graph
        .iter()
        .map(|node| PackageIdentity {
            name: node.name.clone(),
            version: target_version.to_owned(),
        })
        .collect::<Vec<_>>();
    let external_dependencies = external_publish_dependencies(&metadata)?
        .into_iter()
        .map(|(name, version)| PackageIdentity { name, version })
        .collect::<Vec<_>>();
    let mut observed_packages = package_identities.clone();
    observed_packages.extend(external_dependencies.iter().cloned());
    let config = ReleaseConfig::load(workspace_root, &repository)?;
    let selected_base = base_ref
        .map(str::to_owned)
        .unwrap_or_else(|| format!("origin/{}", config.release_branch));
    let tag = format!("v{target}");
    admit_changelog_config(workspace_root)?;
    if admit_fragments(workspace_root)?.is_empty() {
        return Err(ReleaseError::Changelog {
            path: PathBuf::from(".changes/unreleased"),
            reason: "a release requires at least one admitted fragment".to_owned(),
        });
    }
    // Source admission precedes the expensive target-version package graph so
    // a dirty or stale tree cannot be masked by a later packaging diagnostic.
    let source_observation =
        observe_release_inputs(workspace_root, &selected_base, &tag, &[], client)
            .await
            .map_err(|source| ReleaseError::Executor {
                reason: source.to_string(),
            })?;
    let git = source_observation.git_observation();
    if !git.clean {
        return Err(ReleaseError::DirtyWorkspace {
            commit: git.head_commit.clone(),
        });
    }
    if !git.up_to_date || git.head_commit != git.base_commit {
        return Err(ReleaseError::StaleWorkspace {
            head: git.head_commit.clone(),
            base: git.base_commit.clone(),
        });
    }
    let planned_artifacts =
        observe_planned_artifacts(workspace_root, target_version, &metadata, &config, client)
            .await
            .map_err(|source| ReleaseError::PackageDryRun {
                reason: source.to_string(),
            })?;
    let observations = observe_release_inputs(
        workspace_root,
        &selected_base,
        &tag,
        &observed_packages,
        client,
    )
    .await
    .map_err(|source| ReleaseError::Executor {
        reason: source.to_string(),
    })?;
    let changie_platform = match std::env::consts::ARCH {
        "aarch64" => "linux-aarch64",
        "x86_64" => "linux-x86_64",
        architecture => {
            return Err(ReleaseError::UnsupportedToolPlatform {
                platform: format!("linux-{architecture}"),
                remediation: "run the release train on a supported x86_64 or aarch64 executor"
                    .to_owned(),
            });
        }
    };
    let request = ReleasePlanRequest {
        workspace_root: workspace_root.to_path_buf(),
        target_version: target_version.to_owned(),
        base_ref: Some(selected_base),
        repository,
        metadata: &metadata,
        external_dependencies,
        planned_artifacts: planned_artifacts.packages,
        version_body: planned_artifacts.version_body,
        changie_platform: changie_platform.to_owned(),
        toolchain: ToolchainIdentity {
            rust: rust_toolchain_version(workspace_root).map_err(|source| {
                ReleaseError::Workspace {
                    reason: source.to_string(),
                }
            })?,
            dagger: DAGGER_RELEASE.engine_version.to_owned(),
        },
    };
    plan_release(&request, &observations)
}

/// Materialized read-only Git and crates.io facts produced by one Dagger session.
#[derive(Clone, Debug)]
pub struct ObservedReleaseInputs {
    git: GitObservation,
    tag_commit: Option<String>,
    registry: BTreeMap<(String, String), RegistryObservation>,
}

impl ReleaseObservations for ObservedReleaseInputs {
    fn git(&self, _request: &ReleasePlanRequest<'_>) -> Result<GitObservation, ReleaseError> {
        Ok(self.git.clone())
    }

    fn registry(&self, package: &PackageIdentity) -> Result<RegistryObservation, ReleaseError> {
        self.registry
            .get(&(package.name.clone(), package.version.clone()))
            .cloned()
            .ok_or_else(|| ReleaseError::Executor {
                reason: format!(
                    "Dagger observations omitted {} {}",
                    package.name, package.version
                ),
            })
    }

    fn release_tag(&self, _tag: &str) -> Result<Option<String>, ReleaseError> {
        Ok(self.tag_commit.clone())
    }
}

impl ObservedReleaseInputs {
    fn git_observation(&self) -> &GitObservation {
        &self.git
    }
}

/// Observe clean Git identity, the release tag, and public registry checksums.
///
/// No credential is involved: the registry is read anonymously and the remote is
/// read through the operator's own SSH agent, which is the only way `ls-remote` can
/// reach an SSH `origin` from inside the container.
pub async fn observe_release_inputs(
    workspace_root: &Path,
    base_ref: &str,
    tag: &str,
    packages: &[PackageIdentity],
    client: &Client,
) -> Result<ObservedReleaseInputs, BuildError> {
    let layout = GitLayout::resolve(workspace_root)?;
    let query = client.query();
    let source = query.host().directory_opts(
        workspace_root.display().to_string(),
        &HostDirectoryOpts::default().with_exclude(RELEASE_SOURCE_EXCLUDES.to_vec()),
    );
    let git = query
        .host()
        .directory(layout.common_dir.display().to_string());
    let container = query
        .container()
        .from(OBSERVATION_IMAGE)
        .with_exec(vec!["sh", "-c", OBSERVATION_APT])
        .with_directory("/workspace", source)
        .with_directory("/repo.git", git)
        .with_new_file(&layout.worktree_pointer, "/workspace/.git")
        .with_workdir("/workspace")
        .with_env_variable(NONCE_ENV, invocation_nonce());
    let git_output = with_agent(client, container.with_env_variable("BASE_REF", base_ref))
        .with_env_variable("RELEASE_TAG", tag)
        .with_exec(vec![
            "sh",
            "-c",
            r#"set -eu
head=$(git rev-parse HEAD)
case "$BASE_REF" in
  origin/*)
    branch=${BASE_REF#origin/}
    base=$(git ls-remote --heads origin "refs/heads/$branch" | awk '{print $1}')
    if [ -z "$base" ]; then
      echo "remote base branch $branch is absent" >&2
      exit 2
    fi
    ;;
  *) base=$(git rev-parse "$BASE_REF") ;;
esac
tag_commit=$(git ls-remote origin "refs/tags/$RELEASE_TAG^{}" | awk '{print $1}')
if [ -z "$(git status --porcelain --untracked-files=normal)" ]; then clean=1; else clean=0; fi
if [ "$head" = "$base" ]; then current=1; else current=0; fi
printf '%s\t%s\t%s\t%s\t%s\n' "$head" "$base" "$clean" "$current" "${tag_commit:-absent}""#,
        ])
        .stdout()
        .await?;
    let fields = git_output.trim().split('\t').collect::<Vec<_>>();
    if fields.len() != 5 {
        return Err(BuildError::Validation {
            reason: format!("Dagger Git observation was malformed: {git_output}"),
        });
    }
    let git = GitObservation {
        head_commit: fields[0].to_owned(),
        base_commit: fields[1].to_owned(),
        clean: fields[2] == "1",
        up_to_date: fields[3] == "1",
    };
    let tag_commit = (fields[4] != "absent").then(|| fields[4].to_owned());

    let registry_script = r#"set -eu
for spec in "$@"; do
  name=${spec%%=*}
  version=${spec#*=}
  url="https://crates.io/api/v1/crates/$name/$version"
  code=$(curl --silent --show-error --location --output /tmp/crate.json --write-out '%{http_code}' "$url")
  case "$code" in
    200) checksum=$(jq -er '.version.checksum' /tmp/crate.json); printf '%s\t%s\t%s\n' "$name" "$version" "$checksum" ;;
    404) printf '%s\t%s\tabsent\n' "$name" "$version" ;;
    *) echo "registry observation failed for $name $version with HTTP $code" >&2; exit 1 ;;
  esac
done"#;
    let mut arguments = vec![
        "sh".to_owned(),
        "-c".to_owned(),
        registry_script.to_owned(),
        "--".to_owned(),
    ];
    arguments.extend(package_specs(
        packages
            .iter()
            .map(|package| (package.name.as_str(), package.version.as_str())),
    ));
    let registry_output = container.with_exec(arguments).stdout().await?;
    let mut registry = BTreeMap::new();
    for line in registry_output.lines() {
        let fields = line.split('\t').collect::<Vec<_>>();
        if fields.len() != 3 {
            return Err(BuildError::Validation {
                reason: format!("Dagger registry observation was malformed: {line}"),
            });
        }
        let observation = if fields[2] == "absent" {
            RegistryObservation::Absent
        } else {
            RegistryObservation::Existing {
                checksum: fields[2].to_owned(),
            }
        };
        registry.insert((fields[0].to_owned(), fields[1].to_owned()), observation);
    }
    Ok(ObservedReleaseInputs {
        git,
        tag_commit,
        registry,
    })
}

/// Hermetically prepare and package target-version source without mutating the host.
pub async fn observe_planned_artifacts(
    workspace_root: &Path,
    target_version: &str,
    metadata: &Metadata,
    config: &ReleaseConfig,
    client: &Client,
) -> Result<PlannedReleaseArtifacts, BuildError> {
    let graph = publishable_packages(metadata).map_err(|source| BuildError::Validation {
        reason: source.to_string(),
    })?;
    let internal = graph
        .iter()
        .map(|node| node.name.clone())
        .collect::<BTreeSet<_>>();
    let versions = graph
        .iter()
        .filter_map(|node| {
            metadata
                .packages
                .iter()
                .find(|package| package.id.repr == node.package_id)
        })
        .map(|package| package.version.to_string())
        .collect::<BTreeSet<_>>();
    if versions.len() != 1 {
        return Err(BuildError::Validation {
            reason: format!("release planning requires one unified version, observed {versions:?}"),
        });
    }
    let current_version = versions
        .first()
        .expect("non-empty publish graph has one version");
    let manifests = graph
        .iter()
        .map(|node| {
            let package = metadata
                .packages
                .iter()
                .find(|package| package.id.repr == node.package_id)
                .expect("publish graph nodes originate in this metadata");
            let relative = package
                .manifest_path
                .as_std_path()
                .strip_prefix(workspace_root)
                .map_err(|_| BuildError::Validation {
                    reason: format!("manifest for {} is outside the workspace", node.name),
                })?;
            Ok((node.name.clone(), relative.to_path_buf()))
        })
        .collect::<Result<Vec<_>, BuildError>>()?;
    let rewritten_files = rewrite_workspace_manifests(
        workspace_root,
        &manifests,
        &internal,
        current_version,
        target_version,
        &config.extra_version_fields,
    )
    .map_err(|source| BuildError::Validation {
        reason: source.to_string(),
    })?;

    let query = client.query();
    let source = query.host().directory_opts(
        workspace_root.display().to_string(),
        &HostDirectoryOpts::default().with_exclude(RELEASE_SOURCE_EXCLUDES.to_vec()),
    );
    let mut prepared = source;
    for (path, text) in rewritten_files {
        prepared = prepared.with_new_file(text, path.display().to_string());
    }

    let toolchain = rust_toolchain_version(workspace_root)?;
    let acquisition = changie_acquisition_script()?;
    let registry = query.cache_volume("tokeira-release-planning-registry");
    let target = query.cache_volume("tokeira-release-planning-target");
    let names = graph
        .iter()
        .map(|node| node.name.as_str())
        .collect::<Vec<_>>();
    let execution = builder_toolchain(&query, &toolchain)
        .with_mounted_cache(registry, "/usr/local/cargo/registry")
        .with_mounted_cache(target, "/workspace/target")
        .with_directory("/workspace", prepared)
        .with_workdir("/workspace")
        .with_exec(vec!["sh", "-c", &acquisition])
        .with_exec(vec![
            "/tmp/changie",
            "batch",
            target_version,
            "--allow-no-changes=false",
        ])
        .with_exec(vec!["/tmp/changie", "merge"])
        // The isolated source owns this lock update; the host lockfile remains byte-identical.
        .with_exec(vec![
            "cargo",
            "metadata",
            "--offline",
            "--format-version",
            "1",
        ])
        .with_exec(cargo_argv(cargo_package_arguments_for_names(
            names.iter().copied(),
        )))
        .with_exec(vec!["sh", "-c", HERMETIC_CHECKSUM_SCRIPT]);
    let version_body = execution
        .file(format!(".changes/{target_version}.md"))
        .contents()
        .await?;
    let output = execution.stdout().await?;
    let checksums = parse_checksum_lines(&output);
    let packages = graph
        .into_iter()
        .map(|node| {
            let archive = format!("{}-{target_version}.crate", node.name);
            let sha256 =
                checksums
                    .get(&archive)
                    .cloned()
                    .ok_or_else(|| BuildError::Validation {
                        reason: format!("Hermetic planning build omitted {archive}"),
                    })?;
            let name = node.name;
            Ok((
                name.clone(),
                PlannedArtifact {
                    sha256,
                    readme_url: format!(
                        "https://crates.io/api/v1/crates/{name}/{target_version}/readme"
                    ),
                },
            ))
        })
        .collect::<Result<BTreeMap<_, _>, BuildError>>()?;
    Ok(PlannedReleaseArtifacts {
        packages,
        version_body,
    })
}

fn changie_acquisition_script() -> Result<String, BuildError> {
    let x86 = CHANGIE_RELEASE
        .asset("linux-x86_64")
        .ok_or_else(|| BuildError::Validation {
            reason: "changie pin omitted linux-x86_64".to_owned(),
        })?;
    let arm = CHANGIE_RELEASE
        .asset("linux-aarch64")
        .ok_or_else(|| BuildError::Validation {
            reason: "changie pin omitted linux-aarch64".to_owned(),
        })?;
    Ok(format!(
        r#"set -eu
case "$(uname -m)" in
  x86_64) url='{x86_url}'; sha='{x86_sha}' ;;
  aarch64|arm64) url='{arm_url}'; sha='{arm_sha}' ;;
  *) echo 'unsupported changie executor architecture' >&2; exit 3 ;;
esac
curl --fail --location --silent --show-error --proto '=https' --proto-redir '=https' --output /tmp/changie.tar.gz "$url"
printf '%s  %s\n' "$sha" /tmp/changie.tar.gz | sha256sum --check --strict
tar -xzf /tmp/changie.tar.gz -C /tmp changie
/tmp/changie --version | grep -F '{version}' >/dev/null
"#,
        x86_url = x86.url,
        x86_sha = x86.sha256,
        arm_url = arm.url,
        arm_sha = arm.sha256,
        version = CHANGIE_RELEASE.version,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_target_must_be_the_plans_repository() {
        let repository = RepositoryIdentity {
            slug: "tokeira/tokeira".to_owned(),
            remote: "https://github.com/tokeira/tokeira".to_owned(),
        };
        assert!(admit_push_target(&repository, "git@github.com:tokeira/tokeira.git").is_ok());
        assert!(admit_push_target(&repository, "https://github.com/tokeira/tokeira").is_ok());
        assert!(matches!(
            admit_push_target(&repository, "git@github.com:someone/tokeira.git"),
            Err(ReleaseError::RepositoryMismatch { .. })
        ));
        assert!(matches!(
            admit_push_target(&repository, "ssh://git@mirror.invalid/tokeira/tokeira.git"),
            Err(ReleaseError::RepositoryMismatch { .. })
        ));
    }

    #[test]
    fn stopped_trains_classify_what_is_durable() {
        let plan = crate::pipelines::release::apply::tests::sample_plan("a");
        let before_push = stopped(
            &plan,
            false,
            "c".repeat(40).as_str(),
            ReleaseError::PackageDryRun {
                reason: "build failed".to_owned(),
            },
            Vec::new(),
            Vec::new(),
        );
        assert!(matches!(
            before_push,
            ReleaseError::Incomplete {
                state: super::super::TrainState::PrePublicationFailed,
                ..
            }
        ));
        assert_eq!(before_push.exit_code(), 4);

        let after_push = stopped(
            &plan,
            true,
            "c".repeat(40).as_str(),
            RegistryStop::Pending {
                package: "crate-a".to_owned(),
                version: "1.0.0".to_owned(),
            }
            .into_error(),
            Vec::new(),
            vec!["crate-a 1.0.0: timed out".to_owned()],
        );
        let report = after_push
            .report()
            .expect("a stopped train carries its report");
        assert_eq!(report.state, super::super::TrainState::PartiallyPublished);
        assert!(report.tag.published);
        assert_eq!(report.diagnostics.len(), 2);
        assert_eq!(after_push.code(), "registry_state_pending");
        assert_eq!(after_push.exit_code(), 6);

        let mismatch = stopped(
            &plan,
            true,
            "c".repeat(40).as_str(),
            RegistryStop::Mismatch {
                package: "crate-a".to_owned(),
                version: "1.0.0".to_owned(),
                hermetic: "a".repeat(64),
                downloaded: "b".repeat(64),
                registry: "b".repeat(64),
            }
            .into_error(),
            Vec::new(),
            Vec::new(),
        );
        assert!(matches!(
            mismatch,
            ReleaseError::Incomplete {
                state: super::super::TrainState::TerminalMismatch,
                ..
            }
        ));
    }

    #[test]
    fn push_failures_classify_by_gits_rejection_line() {
        let plan = crate::pipelines::release::apply::tests::sample_plan("a");
        assert!(matches!(
            parse_push_failure(&plan, " ! [rejected] v1.0.0 -> v1.0.0 (already exists)"),
            ReleaseError::TagConflict { .. }
        ));
        assert!(matches!(
            parse_push_failure(&plan, " ! [rejected] HEAD -> main (fetch first)"),
            ReleaseError::GitRefConflict { .. }
        ));
        assert!(matches!(
            parse_push_failure(&plan, "ssh: connect to host github.com port 22: timed out"),
            ReleaseError::Executor { .. }
        ));
    }

    #[test]
    fn nonces_differ_between_invocations() {
        assert_ne!(invocation_nonce(), invocation_nonce());
    }

    #[test]
    fn registry_output_feeds_reports_without_losing_evidence() {
        let output = "DIAG\ta\t1.0.0\ttimed out\nPACKAGE\ta\t1.0.0\tpublished\tx\tx\tx\turl\nPENDING\tb\t1.0.0\n";
        let parsed = parse_registry_output(output).expect("parseable");
        assert_eq!(parsed.packages.len(), 1);
        assert_eq!(parsed.diagnostics, vec!["a 1.0.0: timed out".to_owned()]);
        assert!(matches!(parsed.stop, Some(RegistryStop::Pending { .. })));
    }
}
