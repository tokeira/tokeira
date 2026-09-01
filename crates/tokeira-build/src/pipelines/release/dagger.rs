//! Dagger-side pinned changie acquisition and read-only target-artifact planning.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    path::{Path, PathBuf},
    process::Command,
};

use async_trait::async_trait;
use cargo_metadata::Metadata;
use dagger_sdk::{Client, ContainerWithExecOpts, Directory, HostDirectoryOpts, ReturnType};

use crate::{BuildError, CHANGIE_RELEASE, DAGGER_RELEASE, rust_toolchain_version};

use super::{
    GitObservation, ObservedGitRef, PackageIdentity, PackageOutcome, PackageResult,
    PlannedArtifact, PublishParityReport, RELEASE_SCHEMA_VERSION, RegistryObservation,
    ReleaseConfig, ReleaseDaggerClient, ReleaseError, ReleaseNotesOutcome, ReleaseNotesRequest,
    ReleaseNotesResult, ReleaseObservations, ReleasePlan, ReleasePlanRequest,
    ReleasePublishRequest, ReleaseReport, ReleaseVerifyRequest, RemoteGitObservation,
    RepositoryIdentity, TagResult, ToolchainIdentity, TrainIdentity, TrainState,
    admit_changelog_config, admit_fragments, external_publish_dependencies, generate_release_notes,
    plan_release, publishable_packages, rewrite_extra_version_field, rewrite_manifest, sha256_hex,
    verify_resume_refs,
};
use crate::pipelines::build::builder_toolchain;

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

/// Observe a public GitHub release without a credential.
pub async fn observe_release_object(
    repository: &str,
    tag: &str,
    client: &Client,
) -> Result<Option<ReleaseObjectObservation>, BuildError> {
    let output = client
        .query()
        .container()
        .from("debian:bookworm-slim")
        .with_exec(vec![
            "sh",
            "-c",
            "apt-get update && apt-get install -y --no-install-recommends curl jq ca-certificates && rm -rf /var/lib/apt/lists/*",
        ])
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

async fn execute_publish_and_parity(
    client: &Client,
    request: &ReleasePublishRequest,
) -> Result<PublishParityReport, ReleaseError> {
    let plan = &request.plan;
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
    let mut allowed_paths = plan
        .packages
        .iter()
        .map(|package| package.manifest_path.display().to_string())
        .collect::<BTreeSet<_>>();
    allowed_paths.extend(
        config
            .extra_version_fields
            .iter()
            .map(|field| field.path.display().to_string()),
    );
    allowed_paths.extend([
        "Cargo.toml".to_owned(),
        "Cargo.lock".to_owned(),
        "CHANGELOG.md".to_owned(),
        format!(".changes/{}.md", plan.target_version),
    ]);
    allowed_paths.extend(
        plan.fragments
            .iter()
            .map(|fragment| fragment.path.display().to_string()),
    );
    let allowed_paths = allowed_paths.into_iter().collect::<Vec<_>>().join("\n") + "\n";
    let fragments = plan
        .fragments
        .iter()
        .map(|fragment| fragment.path.display().to_string())
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    let mut container = builder_toolchain(&query, &plan.toolchain.rust)
        .with_mounted_cache(registry_cache, "/usr/local/cargo/registry")
        .with_directory("/workspace", prepared)
        .with_directory("/repo.git", git)
        .with_new_file(&layout.worktree_pointer, "/workspace/.git")
        .with_workdir("/workspace")
        .with_env_variable("RELEASE_TAG", &plan.tag)
        .with_env_variable("RELEASE_BRANCH", &config.release_branch)
        .with_env_variable("RELEASE_BASE", &plan.base_commit)
        .with_env_variable("PLAN_DIGEST", &plan.digest)
        .with_env_variable("RELEASE_VERSION", &plan.target_version)
        .with_new_file(&allowed_paths, "/tmp/release-allowed-paths")
        .with_new_file(&fragments, "/tmp/release-fragments")
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
        .with_exec(vec!["sh", "-c", validate_prepared_source_script()]);
    // Forward the agent socket, never private-key bytes: the isolated Git
    // transaction needs the operator's existing push authority without
    // inventing another serializable credential surface.
    if let Some(socket_path) = std::env::var_os("SSH_AUTH_SOCK") {
        let socket_path = PathBuf::from(socket_path);
        if socket_path.is_absolute() {
            let socket = query.host().unix_socket(socket_path.display().to_string());
            container = container
                .with_unix_socket("/run/tokeira-release-ssh-agent", socket)
                .with_env_variable("SSH_AUTH_SOCK", "/run/tokeira-release-ssh-agent");
        }
    }
    let author_name = git_config(&plan.workspace_root, "user.name")?;
    let author_email = git_config(&plan.workspace_root, "user.email")?;
    let git_admission = container
        .with_env_variable("GIT_AUTHOR_NAME", &author_name)
        .with_env_variable("GIT_AUTHOR_EMAIL", &author_email)
        .with_env_variable("GIT_COMMITTER_NAME", &author_name)
        .with_env_variable("GIT_COMMITTER_EMAIL", &author_email)
        .with_new_file(fresh_or_resume_script(), "/tmp/release-git-admit.sh")
        .with_exec(vec!["chmod", "+x", "/tmp/release-git-admit.sh"])
        .with_exec_opts(
            vec!["/tmp/release-git-admit.sh"],
            &ContainerWithExecOpts::default().with_expect(ReturnType::Any),
        );
    let admission_code = git_admission.exit_code().await.map_err(|source| {
        executor_error(format!("release Git admission graph failed: {source}"))
    })?;
    if admission_code != 0 {
        let stderr = git_admission.stderr().await.unwrap_or_default();
        return Err(parse_git_failure(plan, &stderr));
    }
    container = git_admission
        .with_mounted_cache(target_cache, "/release-source/target")
        .with_workdir("/release-source");
    let mut package_arguments = vec![
        "cargo".to_owned(),
        "package".to_owned(),
        "--locked".to_owned(),
        "--allow-dirty".to_owned(),
    ];
    for package in &plan.packages {
        package_arguments.push("--package".to_owned());
        package_arguments.push(package.name.clone());
    }
    let packaged = container.with_exec_opts(
        package_arguments,
        &ContainerWithExecOpts::default().with_expect(ReturnType::Any),
    );
    let package_code = packaged
        .exit_code()
        .await
        .map_err(|source| executor_error(format!("Hermetic Tag Build graph failed: {source}")))?;
    if package_code != 0 {
        return Err(ReleaseError::PackageDryRun {
            reason: format!("Hermetic Tag Build exited with status {package_code}"),
        });
    }
    // The release branch and annotated tag cross the remote boundary together,
    // after every archive has been built from the exact tag source.
    let pushed = packaged
        .with_workdir("/workspace")
        .with_new_file(atomic_push_script(), "/tmp/release-git-push.sh")
        .with_exec(vec!["chmod", "+x", "/tmp/release-git-push.sh"])
        .with_exec_opts(
            vec!["/tmp/release-git-push.sh"],
            &ContainerWithExecOpts::default().with_expect(ReturnType::Any),
        );
    let push_code = pushed.exit_code().await.map_err(|source| {
        executor_error(format!(
            "atomic release ref publication graph failed: {source}"
        ))
    })?;
    if push_code != 0 {
        let stderr = pushed.stderr().await.unwrap_or_default();
        return Err(parse_git_failure(plan, &stderr));
    }
    container = pushed.with_workdir("/release-source");
    if let Some(credential) = &request.registry_credential {
        let secret = query.set_secret("cargo_registry_token", credential.expose());
        container = container.with_secret_variable("CARGO_REGISTRY_TOKEN", secret);
    }
    let mut publish_arguments = vec![
        "sh".to_owned(),
        "-c".to_owned(),
        registry_publish_script().to_owned(),
        "--".to_owned(),
    ];
    publish_arguments.extend(
        plan.packages
            .iter()
            .map(|package| format!("{}={}", package.name, package.target_version)),
    );
    let execution = container.with_exec_opts(
        publish_arguments,
        &ContainerWithExecOpts::default().with_expect(ReturnType::Any),
    );
    let exit_code = execution.exit_code().await.map_err(|source| {
        executor_error(format!("publish-and-parity Dagger graph failed: {source}"))
    })?;
    let output = execution.stdout().await.map_err(|source| {
        executor_error(format!(
            "publish-and-parity output was unavailable: {source}"
        ))
    })?;
    if exit_code != 0 {
        if let Some(error) = parse_pending_or_mismatch(&output) {
            return Err(error);
        }
        return Err(ReleaseError::RegistryPublish {
            package: "unknown".to_owned(),
            version: plan.target_version.clone(),
            reason: format!("Dagger registry process exited with status {exit_code}"),
        });
    }
    let report = parse_publish_report(plan, &output)?;
    let version_body = execution
        .file(format!(
            "/release-source/.changes/{}.md",
            plan.target_version
        ))
        .contents()
        .await
        .map_err(|source| {
            executor_error(format!(
                "could not read tagged changie version body: {source}"
            ))
        })?;
    let observed_notes = generate_release_notes(&version_body, &report.packages)?;
    let observed_notes_sha256 = sha256_hex(&observed_notes);
    if observed_notes_sha256 != plan.release_notes_sha256 {
        return Err(ReleaseError::Plan {
            reason: format!(
                "release notes drifted after tagged artifact parity: planned {}, observed {}",
                plan.release_notes_sha256, observed_notes_sha256
            ),
        });
    }
    Ok(report)
}

async fn execute_release_notes(
    client: &Client,
    request: &ReleaseNotesRequest,
) -> Result<ReleaseReport, ReleaseError> {
    let query = client.query();
    let secret = query.set_secret(
        "github_release_token",
        request.release_api_credential.expose(),
    );
    let mut packages = request.parity.packages.iter().collect::<Vec<_>>();
    packages.sort_by(|left, right| left.name.cmp(&right.name));
    let mut inventory = String::new();
    for package in packages {
        let checksum = package.registry_sha256.as_deref().ok_or_else(|| {
            executor_error(format!(
                "release-note package {} lacks registry checksum evidence",
                package.name
            ))
        })?;
        let readme = package.readme_url.as_deref().ok_or_else(|| {
            executor_error(format!(
                "release-note package {} lacks README evidence",
                package.name
            ))
        })?;
        writeln!(
            inventory,
            "{}\t{}\t{}\t{}",
            package.name, package.version, checksum, readme
        )
        .expect("writing to a String cannot fail");
    }
    let files = query.directory().with_new_file("inventory.tsv", inventory);
    let tag = &request.parity.tag.tag;
    let repository = &request.parity.train.repository.slug;
    let expected_digest = &request.parity.release_notes_sha256;
    let execution = query
        .container()
        .from("ghcr.io/cli/cli:2.65.0")
        .with_entrypoint(Vec::<String>::new())
        .with_directory("/release", files)
        .with_secret_variable("GH_TOKEN", secret)
        .with_env_variable("RELEASE_TAG", tag)
        .with_env_variable("REPOSITORY", repository)
        .with_env_variable("RELEASE_VERSION", &request.parity.train.target_version)
        .with_env_variable("EXPECTED_NOTES_DIGEST", expected_digest)
        .with_env_variable("EXPECTED_TARGET", &request.parity.tag.commit)
        .with_exec_opts(
            vec!["sh", "-c",
            r#"set -eu
gh api -H 'Accept: application/vnd.github.raw+json' "repos/$REPOSITORY/contents/.changes/$RELEASE_VERSION.md?ref=$RELEASE_TAG" >/tmp/notes
version_body=$(cat /tmp/notes)
: >/tmp/notes
if [ -n "$version_body" ]; then printf '%s\n\n' "$version_body" >>/tmp/notes; fi
printf 'Requires Rust 1.97 or newer.\n\n' >>/tmp/notes
printf '| Package | Version | SHA-256 | crates.io | README |\n' >>/tmp/notes
printf '|---|---:|---|---|---|\n' >>/tmp/notes
tab=$(printf '\t')
while IFS="$tab" read -r name version checksum readme; do
  printf '| `%s` | `%s` | `%s` | [package](https://crates.io/crates/%s/%s) | [README](%s) |\n' "$name" "$version" "$checksum" "$name" "$version" "$readme" >>/tmp/notes
done </release/inventory.tsv
observed=$(sha256sum /tmp/notes | cut -d' ' -f1)
if [ "$observed" != "$EXPECTED_NOTES_DIGEST" ]; then
  echo "tagged release notes digest $observed differs from planned $EXPECTED_NOTES_DIGEST" >&2
  exit 2
fi
if gh release view "$RELEASE_TAG" --repo "$REPOSITORY" --json body,tagName,targetCommitish >/tmp/release.json 2>/dev/null; then
  jq -j '.body' /tmp/release.json >/tmp/existing-notes
  existing_digest=$(sha256sum /tmp/existing-notes | cut -d' ' -f1)
  tag=$(jq -r '.tagName' /tmp/release.json)
  target=$(jq -r '.targetCommitish' /tmp/release.json)
  if [ "$tag" != "$RELEASE_TAG" ] || [ "$target" != "$EXPECTED_TARGET" ] || [ "$existing_digest" != "$EXPECTED_NOTES_DIGEST" ]; then
    echo 'existing release differs from the immutable train' >&2
    exit 5
  fi
  printf 'existing\n'
else
  gh release create "$RELEASE_TAG" --repo "$REPOSITORY" --verify-tag --target "$EXPECTED_TARGET" --title "$RELEASE_TAG" --notes-file /tmp/notes
  printf 'created\n'
fi"#,
            ],
            &ContainerWithExecOpts::default().with_expect(ReturnType::Any),
        );
    let exit_code = execution
        .exit_code()
        .await
        .map_err(|source| executor_error(format!("release-note Dagger graph failed: {source}")))?;
    let output = execution.stdout().await.map_err(|source| {
        executor_error(format!("release-note output was unavailable: {source}"))
    })?;
    if exit_code == 5 {
        return Err(ReleaseError::ReleaseConflict {
            tag: tag.clone(),
            reason: "existing release differs from the immutable train".to_owned(),
        });
    }
    if exit_code != 0 {
        return Err(ReleaseError::Plan {
            reason: format!("tagged release-note generation exited with status {exit_code}"),
        });
    }
    let outcome = if output.lines().any(|line| line == "existing") {
        ReleaseNotesOutcome::ExistingVerified
    } else {
        ReleaseNotesOutcome::Created
    };
    Ok(ReleaseReport {
        schema_version: RELEASE_SCHEMA_VERSION,
        train: request.parity.train.clone(),
        state: TrainState::Complete,
        packages: request.parity.packages.clone(),
        tag: request.parity.tag.clone(),
        release_notes: ReleaseNotesResult {
            outcome,
            sha256: request.parity.release_notes_sha256.clone(),
        },
        diagnostics: Vec::new(),
    })
}

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
    .with_env_variable("RELEASE_TAG", &request.tag)
    .with_env_variable("RELEASE_BRANCH", &request.release_branch)
    .with_exec(vec![
        "sh",
        "-c",
        r#"set -eu
branch=$(git ls-remote --heads origin "refs/heads/$RELEASE_BRANCH" | awk '{print $1}')
tag_object=$(git ls-remote origin "refs/tags/$RELEASE_TAG" | awk '{print $1}')
tag_commit=$(git ls-remote origin "refs/tags/$RELEASE_TAG^{}" | awk '{print $1}')
branch_digest=absent
tag_digest=absent
base_commit=absent
mkdir -p /tag-source
if [ -n "$branch" ]; then
  git fetch --no-tags --force origin "refs/heads/$RELEASE_BRANCH:refs/tokeira-release/branch"
  branch_digest=$(git log -1 --format=%B refs/tokeira-release/branch | sed -n 's/^Release-Plan-Digest: sha256:\([0-9a-f]\{64\}\)$/\1/p' | tail -1)
fi
if [ -n "$tag_object" ] && [ -n "$tag_commit" ]; then
  git fetch --no-tags --force origin "refs/tags/$RELEASE_TAG:refs/tokeira-release/tag"
  if [ "$(git cat-file -t refs/tokeira-release/tag)" = tag ]; then
    tag_digest=$(git for-each-ref --format='%(contents)' refs/tokeira-release/tag | sed -n 's/^Release-Plan-Digest: sha256:\([0-9a-f]\{64\}\)$/\1/p' | tail -1)
  fi
  base_commit=$(git rev-parse "$tag_commit^")
  git archive refs/tokeira-release/tag | tar -x -C /tag-source
fi
printf 'REFS\t%s\t%s\t%s\t%s\t%s\t%s\n' "${branch:-absent}" "${tag_object:-absent}" "${tag_commit:-absent}" "${branch_digest:-absent}" "${tag_digest:-absent}" "$base_commit"
"#,
    ]);
    let refs_output = base.stdout().await.map_err(|source| {
        executor_error(format!("release ref observation graph failed: {source}"))
    })?;
    let fields = refs_output
        .lines()
        .find(|line| line.starts_with("REFS\t"))
        .map(|line| line.split('\t').collect::<Vec<_>>())
        .ok_or_else(|| executor_error("release ref observation omitted evidence".to_owned()))?;
    if fields.len() != 7 {
        return Err(executor_error(format!(
            "release ref observation was malformed: {refs_output}"
        )));
    }
    let expected_digest = request.expected_plan_digest.as_deref().unwrap_or(fields[5]);
    let refs = RemoteGitObservation {
        branch: (fields[1] != "absent").then(|| ObservedGitRef {
            object_id: fields[1].to_owned(),
            commit: fields[1].to_owned(),
            plan_digest: fields[4].to_owned(),
        }),
        tag: (fields[2] != "absent").then(|| ObservedGitRef {
            object_id: fields[2].to_owned(),
            commit: fields[3].to_owned(),
            plan_digest: fields[5].to_owned(),
        }),
    };
    let tag = verify_resume_refs(&request.tag, fields[3], expected_digest, &refs)?;
    let tagged = base.with_workdir("/tag-source").with_exec(vec![
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
    let mut package_arguments = vec![
        "cargo".to_owned(),
        "package".to_owned(),
        "--locked".to_owned(),
        "--allow-dirty".to_owned(),
    ];
    for package in &packages {
        package_arguments.push("--package".to_owned());
        package_arguments.push(package.name.clone());
    }
    let mut verify_arguments = vec![
        "sh".to_owned(),
        "-c".to_owned(),
        registry_verify_script().to_owned(),
        "--".to_owned(),
    ];
    verify_arguments.extend(
        packages
            .iter()
            .map(|package| format!("{}={}", package.name, request.version)),
    );
    let version_body = tagged
        .file(format!("/tag-source/.changes/{}.md", request.version))
        .contents()
        .await
        .map_err(|source| {
            executor_error(format!(
                "could not read the tagged changie version file: {source}"
            ))
        })?;
    let execution = tagged
        .with_workdir("/tag-source")
        .with_exec(package_arguments)
        .with_exec_opts(
            verify_arguments,
            &ContainerWithExecOpts::default().with_expect(ReturnType::Any),
        );
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
    if exit_code != 0 {
        if let Some(error) = parse_pending_or_mismatch(&output) {
            return Err(error);
        }
        return Err(ReleaseError::RegistryPending {
            package: "unknown".to_owned(),
            version: request.version.clone(),
        });
    }
    let package_results = parse_package_lines(&output)?;
    let expected_notes = generate_release_notes(&version_body, &package_results)?;
    let expected_notes_sha256 = sha256_hex(&expected_notes);
    let release = observe_release_object(&request.repository.slug, &request.tag, client)
        .await
        .map_err(|source| executor_error(format!("release API observation failed: {source}")))?;
    let (state, release_notes) = match release {
        Some(existing)
            if existing.notes_sha256 == expected_notes_sha256 && existing.target == tag.commit =>
        {
            (
                TrainState::Complete,
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
            TrainState::PartiallyPublished,
            ReleaseNotesResult {
                outcome: ReleaseNotesOutcome::Absent,
                sha256: expected_notes_sha256,
            },
        ),
    };
    Ok(ReleaseReport {
        schema_version: RELEASE_SCHEMA_VERSION,
        train: TrainIdentity {
            repository: request.repository.clone(),
            base_commit: fields[6].to_owned(),
            target_version: request.version.clone(),
            plan_digest: expected_digest.to_owned(),
        },
        state,
        packages: package_results,
        tag,
        release_notes,
        diagnostics: Vec::new(),
    })
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
    let root_path = plan.workspace_root.join("Cargo.toml");
    let root = std::fs::read_to_string(&root_path).map_err(|source| {
        executor_error(format!("could not read {}: {source}", root_path.display()))
    })?;
    let root = rewrite_manifest(
        &root,
        current_version,
        &plan.target_version,
        &internal,
        true,
    )?;
    let mut rewritten_files = BTreeMap::from([(PathBuf::from("Cargo.toml"), root)]);
    for package_plan in &plan.packages {
        if package_plan.manifest_path == Path::new("Cargo.toml") {
            continue;
        }
        let package = metadata
            .packages
            .iter()
            .find(|package| package.name.as_ref() == package_plan.name)
            .ok_or_else(|| ReleaseError::Workspace {
                reason: format!("Cargo metadata omitted {}", package_plan.name),
            })?;
        let text =
            std::fs::read_to_string(package.manifest_path.as_std_path()).map_err(|source| {
                executor_error(format!(
                    "could not read {}: {source}",
                    package.manifest_path
                ))
            })?;
        let rewritten = rewrite_manifest(
            &text,
            current_version,
            &plan.target_version,
            &internal,
            true,
        )?;
        rewritten_files.insert(package_plan.manifest_path.clone(), rewritten);
    }
    for field in &config.extra_version_fields {
        let input = match rewritten_files.get(&field.path) {
            Some(rewritten) => rewritten.clone(),
            None => {
                let path = plan.workspace_root.join(&field.path);
                std::fs::read_to_string(&path).map_err(|source| {
                    executor_error(format!("could not read {}: {source}", path.display()))
                })?
            }
        };
        let rewritten =
            rewrite_extra_version_field(&input, &field.key, current_version, &plan.target_version)?;
        rewritten_files.insert(field.path.clone(), rewritten);
    }
    let query = client.query();
    let source = query.host().directory_opts(
        plan.workspace_root.display().to_string(),
        &HostDirectoryOpts::default().with_exclude(RELEASE_SOURCE_EXCLUDES.to_vec()),
    );
    let mut prepared = source;
    for (path, text) in rewritten_files {
        prepared = prepared.with_new_file(path.display().to_string(), text);
    }
    Ok(prepared)
}

fn fresh_or_resume_script() -> String {
    r#"#!/bin/sh
set -eu
branch=$(git ls-remote --heads origin "refs/heads/$RELEASE_BRANCH" | awk '{print $1}')
tag=$(git ls-remote origin "refs/tags/$RELEASE_TAG^{}" | awk '{print $1}')
mkdir -p /release-source
if [ -n "$tag" ]; then
  if [ -z "$branch" ] || [ "$branch" != "$tag" ]; then
    if [ "${branch:-absent}" = "$RELEASE_BASE" ]; then
      printf 'remote release tag conflict: expected train %s; observed %s\n' "$PLAN_DIGEST" "$tag" >&2
    else
      printf 'remote release refs conflict: branch observed %s; tag observed %s\n' "${branch:-absent}" "$tag" >&2
    fi
    exit 5
  fi
  git fetch --no-tags origin "refs/tags/$RELEASE_TAG:refs/tokeira-release/$RELEASE_TAG"
  annotation=$(git for-each-ref --format='%(contents)' "refs/tokeira-release/$RELEASE_TAG")
  if ! printf '%s\n' "$annotation" | grep -F "Release-Plan-Digest: sha256:$PLAN_DIGEST" >/dev/null; then
    printf 'remote release refs conflict: branch observed %s; tag observed %s\n' "$branch" "$tag" >&2
    exit 5
  fi
  commit_message=$(git log -1 --format=%B "refs/tokeira-release/$RELEASE_TAG^{}")
  if ! printf '%s\n' "$commit_message" | grep -F "Release-Plan-Digest: sha256:$PLAN_DIGEST" >/dev/null; then
    printf 'remote release refs conflict: branch observed %s; tag observed %s\n' "$branch" "$tag" >&2
    exit 5
  fi
  git archive "refs/tokeira-release/$RELEASE_TAG" | tar -x -C /release-source
  printf '%s\n' "$tag" >/tmp/release-commit
  printf 'resume\n' >/tmp/release-mode
else
  if [ -z "$branch" ] || [ "$branch" != "$RELEASE_BASE" ]; then
    printf 'remote release refs conflict: branch observed %s; tag observed absent\n' "${branch:-absent}" >&2
    exit 5
  fi
  git add -A
  git commit -m "release: prepare $RELEASE_TAG" -m "Release-Plan-Digest: sha256:$PLAN_DIGEST"
  git tag -a "$RELEASE_TAG" -m "$RELEASE_TAG" -m "Release-Plan-Digest: sha256:$PLAN_DIGEST"
  commit=$(git rev-parse HEAD)
  git archive "$RELEASE_TAG" | tar -x -C /release-source
  printf '%s\n' "$commit" >/tmp/release-commit
  printf 'fresh\n' >/tmp/release-mode
fi
"#
    .to_owned()
}

fn validate_prepared_source_script() -> &'static str {
    r#"set -eu
git status --porcelain --untracked-files=normal | cut -c4- | sort -u >/tmp/release-actual-paths
while IFS= read -r path; do
  [ -n "$path" ] || continue
  if ! grep -Fx "$path" /tmp/release-allowed-paths >/dev/null; then
    echo "release preparation changed unowned path $path" >&2
    exit 2
  fi
done </tmp/release-actual-paths
while IFS= read -r fragment; do
  [ -n "$fragment" ] || continue
  if [ -e "$fragment" ]; then
    echo "release preparation did not consume fragment $fragment" >&2
    exit 2
  fi
done </tmp/release-fragments
test -f ".changes/$RELEASE_VERSION.md"
if git diff --quiet -- CHANGELOG.md; then
  echo 'release preparation did not merge CHANGELOG.md' >&2
  exit 2
fi
"#
}

fn atomic_push_script() -> String {
    r#"#!/bin/sh
set -eu
if [ "$(cat /tmp/release-mode)" = fresh ]; then
  git push --atomic origin "HEAD:refs/heads/$RELEASE_BRANCH" "refs/tags/$RELEASE_TAG:refs/tags/$RELEASE_TAG"
fi
branch=$(git ls-remote --heads origin "refs/heads/$RELEASE_BRANCH" | awk '{print $1}')
tag=$(git ls-remote origin "refs/tags/$RELEASE_TAG^{}" | awk '{print $1}')
if [ -z "$branch" ] || [ -z "$tag" ] || [ "$branch" != "$tag" ]; then
  printf 'remote release refs conflict: branch observed %s; tag observed %s\n' "${branch:-absent}" "${tag:-absent}" >&2
  exit 5
fi
"#
    .to_owned()
}

fn registry_publish_script() -> &'static str {
    r#"set -eu
printf 'COMMIT\t%s\n' "$(cat /tmp/release-commit)"
last_success=0
retry_ready=0
for spec in "$@"; do
  name=${spec%%=*}
  version=${spec#*=}
  archive="target/package/$name-$version.crate"
  api="https://crates.io/api/v1/crates/$name/$version"
  code=$(curl --silent --show-error --location --output /tmp/crate.json --write-out '%{http_code}' "$api")
  outcome=existing
  if [ "$code" = 404 ]; then
    while [ "$code" = 404 ]; do
      now=$(date +%s)
      ready=$((last_success + 600))
      if [ "$retry_ready" -gt "$ready" ]; then ready=$retry_ready; fi
      if [ "$now" -lt "$ready" ]; then sleep $((ready - now)); fi
      # A longer registry deadline may outlive the cooldown. Re-observe before
      # issuing another irreversible upload request in case the prior response
      # was ambiguous but eventually became visible.
      code=$(curl --silent --show-error --location --output /tmp/crate.json --write-out '%{http_code}' "$api")
      if [ "$code" = 200 ]; then break; fi
      if [ "$code" != 404 ]; then
        printf 'FAILED\t%s\t%s\n' "$name" "$version"
        exit 6
      fi
      set +e
      cargo publish --locked --package "$name" >/tmp/publish.out 2>/tmp/publish.err
      publish_status=$?
      publish_started=$(date +%s)
      set -e
      if [ "$publish_status" -ne 0 ] && grep -Eqi '403 Forbidden|not an owner|not allowed to upload' /tmp/publish.err; then
        printf 'FAILED\t%s\t%s\n' "$name" "$version"
        exit 6
      fi
      retry_seconds=$(sed -n -E 's/.*[Rr]etry-?[Aa]fter[^0-9]*([0-9]+).*/\1/p' /tmp/publish.err | tail -1)
      if [ "$publish_status" -ne 0 ] && [ -n "$retry_seconds" ]; then
        retry_ready=$((publish_started + retry_seconds))
        continue
      fi
      elapsed=0
      delay=5
      while [ "$elapsed" -lt 600 ]; do
        remaining=$((600 - elapsed))
        if [ "$delay" -gt "$remaining" ]; then delay=$remaining; fi
        sleep "$delay"
        elapsed=$((elapsed + delay))
        code=$(curl --silent --show-error --location --output /tmp/crate.json --write-out '%{http_code}' "$api")
        if [ "$code" = 200 ]; then break; fi
        delay=$((delay * 2))
      done
      if [ "$code" != 200 ]; then
        printf 'PENDING\t%s\t%s\n' "$name" "$version"
        exit 6
      fi
      # Cargo can time out after the registry accepted the upload. Public
      # visibility plus parity, not the process status, is authoritative.
      last_success=$publish_started
      outcome=published
    done
  elif [ "$code" != 200 ]; then
    printf 'FAILED\t%s\t%s\n' "$name" "$version"
    exit 6
  fi
  registry=$(jq -er '.version.checksum' /tmp/crate.json)
  curl --fail --silent --show-error --location --output /tmp/download.crate "https://crates.io/api/v1/crates/$name/$version/download"
  local_sha=$(sha256sum "$archive" | cut -d' ' -f1)
  downloaded_sha=$(sha256sum /tmp/download.crate | cut -d' ' -f1)
  if [ "$local_sha" != "$downloaded_sha" ] || [ "$downloaded_sha" != "$registry" ]; then
    printf 'MISMATCH\t%s\t%s\t%s\t%s\t%s\n' "$name" "$version" "$local_sha" "$downloaded_sha" "$registry"
    exit 7
  fi
  readme="https://crates.io/api/v1/crates/$name/$version/readme"
  printf 'PACKAGE\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$name" "$version" "$outcome" "$local_sha" "$downloaded_sha" "$registry" "$readme"
done"#
}

fn registry_verify_script() -> &'static str {
    r#"set -eu
for spec in "$@"; do
  name=${spec%%=*}
  version=${spec#*=}
  archive="target/package/$name-$version.crate"
  api="https://crates.io/api/v1/crates/$name/$version"
  code=$(curl --silent --show-error --location --output /tmp/crate.json --write-out '%{http_code}' "$api")
  if [ "$code" != 200 ]; then printf 'PENDING\t%s\t%s\n' "$name" "$version"; exit 6; fi
  registry=$(jq -er '.version.checksum' /tmp/crate.json)
  curl --fail --silent --show-error --location --output /tmp/download.crate "https://crates.io/api/v1/crates/$name/$version/download"
  local_sha=$(sha256sum "$archive" | cut -d' ' -f1)
  downloaded_sha=$(sha256sum /tmp/download.crate | cut -d' ' -f1)
  if [ "$local_sha" != "$downloaded_sha" ] || [ "$downloaded_sha" != "$registry" ]; then
    printf 'MISMATCH\t%s\t%s\t%s\t%s\t%s\n' "$name" "$version" "$local_sha" "$downloaded_sha" "$registry"
    exit 7
  fi
  readme="https://crates.io/api/v1/crates/$name/$version/readme"
  printf 'PACKAGE\t%s\t%s\texisting\t%s\t%s\t%s\t%s\n' "$name" "$version" "$local_sha" "$downloaded_sha" "$registry" "$readme"
done"#
}

fn parse_publish_report(
    plan: &ReleasePlan,
    output: &str,
) -> Result<PublishParityReport, ReleaseError> {
    if let Some(error) = parse_pending_or_mismatch(output) {
        return Err(error);
    }
    let commit = output
        .lines()
        .find_map(|line| line.strip_prefix("COMMIT\t"))
        .ok_or_else(|| executor_error("publish report omitted the release commit".to_owned()))?;
    let packages = parse_package_lines(output)?;
    Ok(PublishParityReport {
        schema_version: RELEASE_SCHEMA_VERSION,
        train: TrainIdentity::from(plan),
        packages,
        tag: TagResult {
            tag: plan.tag.clone(),
            commit: commit.to_owned(),
            published: true,
        },
        release_notes_sha256: plan.release_notes_sha256.clone(),
        diagnostics: Vec::new(),
    })
}

fn parse_pending_or_mismatch(output: &str) -> Option<ReleaseError> {
    if let Some(line) = output.lines().find(|line| line.starts_with("PENDING\t")) {
        let fields = line.split('\t').collect::<Vec<_>>();
        return Some(ReleaseError::RegistryPending {
            package: fields.get(1).copied().unwrap_or("unknown").to_owned(),
            version: fields.get(2).copied().unwrap_or("unknown").to_owned(),
        });
    }
    if let Some(line) = output.lines().find(|line| line.starts_with("FAILED\t")) {
        let fields = line.split('\t').collect::<Vec<_>>();
        return Some(ReleaseError::RegistryPublish {
            package: fields.get(1).copied().unwrap_or("unknown").to_owned(),
            version: fields.get(2).copied().unwrap_or("unknown").to_owned(),
            reason: "the registry conclusively rejected publication".to_owned(),
        });
    }
    if let Some(line) = output.lines().find(|line| line.starts_with("MISMATCH\t")) {
        let fields = line.split('\t').collect::<Vec<_>>();
        return Some(ReleaseError::ArtifactMismatch {
            package: fields.get(1).copied().unwrap_or("unknown").to_owned(),
            version: fields.get(2).copied().unwrap_or("unknown").to_owned(),
            hermetic: fields.get(3).copied().unwrap_or("absent").to_owned(),
            downloaded: fields.get(4).copied().unwrap_or("absent").to_owned(),
            registry: fields.get(5).copied().unwrap_or("absent").to_owned(),
        });
    }
    None
}

fn parse_package_lines(output: &str) -> Result<Vec<PackageResult>, ReleaseError> {
    output
        .lines()
        .filter(|line| line.starts_with("PACKAGE\t"))
        .map(|line| {
            let fields = line.split('\t').collect::<Vec<_>>();
            if fields.len() != 8 {
                return Err(executor_error(format!(
                    "malformed package evidence: {line}"
                )));
            }
            let outcome = match fields[3] {
                "published" => PackageOutcome::Published,
                "existing" => PackageOutcome::ExistingVerified,
                value => return Err(executor_error(format!("unknown package outcome {value}"))),
            };
            Ok(PackageResult {
                name: fields[1].to_owned(),
                version: fields[2].to_owned(),
                outcome,
                hermetic_sha256: Some(fields[4].to_owned()),
                downloaded_sha256: Some(fields[5].to_owned()),
                registry_sha256: Some(fields[6].to_owned()),
                readme_url: Some(fields[7].to_owned()),
            })
        })
        .collect()
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

fn parse_git_failure(plan: &ReleasePlan, stderr: &str) -> ReleaseError {
    if let Some(observed) = stderr
        .lines()
        .find_map(|line| line.strip_prefix("remote release tag conflict: "))
        .and_then(|line| line.rsplit_once("observed ").map(|(_, observed)| observed))
    {
        return ReleaseError::TagConflict {
            tag: plan.tag.clone(),
            expected: format!("sha256:{}", plan.digest),
            observed: observed.to_owned(),
        };
    }
    if let Some(line) = stderr
        .lines()
        .find_map(|line| line.strip_prefix("remote release refs conflict: branch observed "))
        && let Some((branch, tag)) = line.split_once("; tag observed ")
    {
        return ReleaseError::GitRefConflict {
            branch_observed: branch.to_owned(),
            tag_observed: tag.to_owned(),
        };
    }
    if stderr.contains("[rejected]") && stderr.contains(&plan.tag) {
        return ReleaseError::TagConflict {
            tag: plan.tag.clone(),
            expected: format!("sha256:{}", plan.digest),
            observed: "remote rejected the atomic tag update".to_owned(),
        };
    }
    executor_error("release Git operation failed before registry publication".to_owned())
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
    admit_changelog_config(workspace_root)?;
    if admit_fragments(workspace_root)?.is_empty() {
        return Err(ReleaseError::Changelog {
            path: PathBuf::from(".changes/unreleased"),
            reason: "a release requires at least one admitted fragment".to_owned(),
        });
    }
    // Source admission precedes the expensive target-version package graph so
    // a dirty or stale tree cannot be masked by a later packaging diagnostic.
    let source_observation = observe_release_inputs(workspace_root, &selected_base, &[], client)
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
    let observations =
        observe_release_inputs(workspace_root, &selected_base, &observed_packages, client)
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
}

impl ObservedReleaseInputs {
    fn git_observation(&self) -> &GitObservation {
        &self.git
    }
}

/// Observe clean Git identity and public registry checksums with no credential.
pub async fn observe_release_inputs(
    workspace_root: &Path,
    base_ref: &str,
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
        .from("debian:bookworm-slim")
        .with_exec(vec![
            "sh",
            "-c",
            "apt-get update && apt-get install -y --no-install-recommends git curl jq ca-certificates && rm -rf /var/lib/apt/lists/*",
        ])
        .with_directory("/workspace", source)
        .with_directory("/repo.git", git)
        .with_new_file(&layout.worktree_pointer, "/workspace/.git")
        .with_workdir("/workspace");
    let git_output = container
        .with_env_variable("BASE_REF", base_ref)
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
if [ -z "$(git status --porcelain --untracked-files=normal)" ]; then clean=1; else clean=0; fi
if [ "$head" = "$base" ]; then current=1; else current=0; fi
printf '%s\t%s\t%s\t%s\n' "$head" "$base" "$clean" "$current""#,
        ])
        .stdout()
        .await?;
    let fields = git_output.trim().split('\t').collect::<Vec<_>>();
    if fields.len() != 4 {
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
    arguments.extend(
        packages
            .iter()
            .map(|package| format!("{}={}", package.name, package.version)),
    );
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
    Ok(ObservedReleaseInputs { git, registry })
}

#[derive(Debug)]
struct GitLayout {
    common_dir: PathBuf,
    worktree_pointer: String,
}

impl GitLayout {
    fn resolve(root: &Path) -> Result<Self, BuildError> {
        let common_dir = git_path(root, "--git-common-dir")?;
        let git_dir = git_path(root, "--absolute-git-dir")?;
        let relative = git_dir
            .strip_prefix(&common_dir)
            .map_err(|_| BuildError::Validation {
                reason: format!(
                    "worktree Git directory {} is outside common directory {}",
                    git_dir.display(),
                    common_dir.display()
                ),
            })?;
        let engine_path = if relative.as_os_str().is_empty() {
            "/repo.git".to_owned()
        } else {
            format!("/repo.git/{}", relative.display())
        };
        Ok(Self {
            common_dir,
            worktree_pointer: format!("gitdir: {engine_path}\n"),
        })
    }
}

fn git_path(root: &Path, argument: &str) -> Result<PathBuf, BuildError> {
    let output = Command::new("git")
        .args(["-C"])
        .arg(root)
        .args(["rev-parse", "--path-format=absolute", argument])
        .output()
        .map_err(|source| BuildError::Validation {
            reason: format!("could not inspect Git layout: {source}"),
        })?;
    if !output.status.success() {
        return Err(BuildError::Validation {
            reason: format!(
                "Git layout inspection failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ),
        });
    }
    Ok(PathBuf::from(
        String::from_utf8_lossy(&output.stdout).trim(),
    ))
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
    let root_manifest = workspace_root.join("Cargo.toml");
    let root_text =
        std::fs::read_to_string(&root_manifest).map_err(|source| BuildError::Validation {
            reason: format!("could not read {}: {source}", root_manifest.display()),
        })?;
    let rewritten_root =
        rewrite_manifest(&root_text, current_version, target_version, &internal, true).map_err(
            |source| BuildError::Validation {
                reason: source.to_string(),
            },
        )?;
    let mut rewritten_files = BTreeMap::from([(PathBuf::from("Cargo.toml"), rewritten_root)]);
    for node in &graph {
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
        if relative == Path::new("Cargo.toml") {
            continue;
        }
        let text =
            std::fs::read_to_string(package.manifest_path.as_std_path()).map_err(|source| {
                BuildError::Validation {
                    reason: format!("could not read {}: {source}", package.manifest_path),
                }
            })?;
        let rewritten = rewrite_manifest(&text, current_version, target_version, &internal, true)
            .map_err(|source| BuildError::Validation {
            reason: source.to_string(),
        })?;
        rewritten_files.insert(relative.to_path_buf(), rewritten);
    }
    for field in &config.extra_version_fields {
        let input = match rewritten_files.get(&field.path) {
            Some(rewritten) => rewritten.clone(),
            None => {
                let path = workspace_root.join(&field.path);
                std::fs::read_to_string(&path).map_err(|source| BuildError::Validation {
                    reason: format!("could not read {}: {source}", path.display()),
                })?
            }
        };
        let rewritten =
            rewrite_extra_version_field(&input, &field.key, current_version, target_version)
                .map_err(|source| BuildError::Validation {
                    reason: source.to_string(),
                })?;
        rewritten_files.insert(field.path.clone(), rewritten);
    }

    let query = client.query();
    let source = query.host().directory_opts(
        workspace_root.display().to_string(),
        &HostDirectoryOpts::default().with_exclude(RELEASE_SOURCE_EXCLUDES.to_vec()),
    );
    let mut prepared = source;
    for (path, text) in rewritten_files {
        prepared = prepared.with_new_file(path.display().to_string(), text);
    }

    let toolchain = rust_toolchain_version(workspace_root)?;
    let acquisition = changie_acquisition_script()?;
    let registry = query.cache_volume("tokeira-release-planning-registry");
    let target = query.cache_volume("tokeira-release-planning-target");
    let mut package_arguments = vec![
        "cargo".to_owned(),
        "package".to_owned(),
        "--locked".to_owned(),
        "--allow-dirty".to_owned(),
    ];
    for node in &graph {
        package_arguments.push("--package".to_owned());
        package_arguments.push(node.name.clone());
    }
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
        .with_exec(package_arguments)
        .with_exec(vec!["sh", "-c", "sha256sum target/package/*.crate"]);
    let version_body = execution
        .file(format!(".changes/{target_version}.md"))
        .contents()
        .await?;
    let output = execution.stdout().await?;
    let checksums = output
        .lines()
        .filter_map(|line| line.split_once("  "))
        .map(|(digest, path)| {
            (
                Path::new(path)
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or_default()
                    .to_owned(),
                digest.to_owned(),
            )
        })
        .collect::<BTreeMap<_, _>>();
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
