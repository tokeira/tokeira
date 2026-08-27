//! Dagger-backed local CI, compatibility governance, and binary builds.
//!
//! This module owns the reusable report-producing pipeline; command-line policy
//! (human rendering, JSON selection, and lock-update delegation) remains in `tkr`.
//! Every workspace input is copied into the engine, and every Git observation is
//! made there, so a future remote runner can reuse the verdict without a host-shell
//! approximation.

use std::{
    path::{Path, PathBuf},
    process::Command,
};

use async_trait::async_trait;
use dagger_sdk::{Client, Container, ContainerWithExecOpts, HostDirectoryOpts, ReturnType};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::build::{builder_definition, builder_toolchain};
use crate::{
    BuildError,
    compat_bump::{BumpTrailer, CompatibilityVersion},
    rust_toolchain_version,
};

const GOVERNANCE_IMAGE: &str = "debian:bookworm-slim";
const GOVERNANCE_APT: &str = "apt-get update && apt-get install -y --no-install-recommends git ripgrep ca-certificates && rm -rf /var/lib/apt/lists/*";
const NEXTTEST_VERSION: &str = "0.9.143";
const DENY_VERSION: &str = "0.19.9";
const LYCHEE_VERSION: &str = "0.24.2";
const CI_CARGO_BUILD_JOBS: &str = "1";
const PINNED_RS: &str = "crates/tokeira-build-info/src/pinned.rs";
const CI_WORKSPACE_EXCLUDES: &[&str] = &[
    "target",
    "**/target",
    ".git",
    ".tokeira-build",
    ".env*",
    "**/.env*",
    "artifacts",
    "**/*.log",
];

/// One independently reported local-CI verdict.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CiCheck {
    /// The vendored Temporal proto pin never regresses without an override trailer.
    ProtoMonotonicity,
    /// The Temporal server compatibility claim never regresses without an override.
    ServerCompatMonotonicity,
    /// Every server-compat change has an exact transition trailer.
    BumpTrailer,
    /// Pinned-nightly rustfmt parity.
    Fmt,
    /// Workspace lint wall.
    Lint,
    /// Workspace compilation check.
    Check,
    /// Workspace nextest suite.
    Nextest,
    /// Workspace rustdoc tests.
    Doctests,
    /// Warning-free workspace documentation.
    Rustdoc,
    /// Cargo source, license, and duplicate-version policy.
    Deny,
    /// Offline Markdown link integrity.
    Links,
}

impl CiCheck {
    /// Registry order used by local and future remote CI.
    pub const ALL: [Self; 11] = [
        Self::ProtoMonotonicity,
        Self::ServerCompatMonotonicity,
        Self::BumpTrailer,
        Self::Fmt,
        Self::Lint,
        Self::Check,
        Self::Nextest,
        Self::Doctests,
        Self::Rustdoc,
        Self::Deny,
        Self::Links,
    ];

    /// Stable CLI/report name.
    pub const fn name(self) -> &'static str {
        match self {
            Self::ProtoMonotonicity => "proto-monotonicity",
            Self::ServerCompatMonotonicity => "server-compat-monotonicity",
            Self::BumpTrailer => "bump-trailer",
            Self::Fmt => "fmt",
            Self::Lint => "lint",
            Self::Check => "check",
            Self::Nextest => "nextest",
            Self::Doctests => "doctests",
            Self::Rustdoc => "rustdoc",
            Self::Deny => "deny",
            Self::Links => "links",
        }
    }

    const fn is_governance(self) -> bool {
        matches!(
            self,
            Self::ProtoMonotonicity | Self::ServerCompatMonotonicity | Self::BumpTrailer
        )
    }
}

/// Inputs for a reusable CI run. An empty selection means the full registry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CiCheckRequest {
    /// Host workspace copied into Dagger.
    pub workspace_root: PathBuf,
    /// Selected checks, or all checks when empty.
    pub checks: Vec<CiCheck>,
}

/// Serializable result for one check.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CiCheckResult {
    /// Check identity.
    pub check: CiCheck,
    /// Whether the check admitted the source tree.
    pub passed: bool,
    /// Concise operator-facing verdict.
    pub summary: String,
    /// Failure output or other optional diagnostic evidence.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
}

/// Stable, transportable CI evidence.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CiCheckReport {
    /// Results in registry/request order.
    pub results: Vec<CiCheckResult>,
}

impl CiCheckReport {
    /// Return true only when every selected check passed.
    pub fn passed(&self) -> bool {
        self.results.iter().all(|result| result.passed)
    }
}

/// Client seam that lets remote orchestration supply its own Dagger connection.
#[async_trait]
pub trait DaggerClient: Send + Sync {
    /// Execute one selected check using the request's mounted workspace.
    async fn execute_ci_check(
        &self,
        request: &CiCheckRequest,
        check: CiCheck,
    ) -> Result<CiCheckResult, BuildError>;
}

/// Run the selected checks and return their unmodified reusable report.
pub async fn run_ci_checks(
    request: &CiCheckRequest,
    dagger: &dyn DaggerClient,
) -> Result<CiCheckReport, BuildError> {
    let checks = if request.checks.is_empty() {
        CiCheck::ALL.as_slice()
    } else {
        request.checks.as_slice()
    };
    let mut results = Vec::with_capacity(checks.len());
    for &check in checks {
        results.push(dagger.execute_ci_check(request, check).await?);
    }
    Ok(CiCheckReport { results })
}

#[async_trait]
impl DaggerClient for Client {
    async fn execute_ci_check(
        &self,
        request: &CiCheckRequest,
        check: CiCheck,
    ) -> Result<CiCheckResult, BuildError> {
        let layout = GitLayout::resolve(&request.workspace_root)?;
        let result: Result<CiCheckResult, BuildError> = if check.is_governance() {
            execute_governance(self, &request.workspace_root, &layout, check)
                .await
                .map_err(BuildError::from)
        } else {
            execute_bar(self, &request.workspace_root, &layout, check).await
        };
        Ok(match result {
            Ok(result) => result,
            Err(error) => CiCheckResult {
                check,
                passed: false,
                summary: format!("{} failed", check.name()),
                details: Some(format!("{error:#?}")),
            },
        })
    }
}

async fn execute_governance(
    client: &Client,
    root: &Path,
    layout: &GitLayout,
    check: CiCheck,
) -> Result<CiCheckResult, BuildError> {
    let base = attach_workspace(
        client
            .query()
            .container()
            .from(GOVERNANCE_IMAGE)
            .with_exec(vec!["sh", "-c", GOVERNANCE_APT])
            .with_env_variable("CI", "1"),
        client,
        root,
        layout,
    );
    match check {
        CiCheck::ProtoMonotonicity => {
            let output = base
                .with_exec(vec![
                    "sh",
                    "-c",
                    &monotonicity_probe("TEMPORAL_PROTO_VERSION", "Proto-Downgrade"),
                ])
                .stdout()
                .await?;
            Ok(evaluate_monotonicity(check, &output))
        }
        CiCheck::ServerCompatMonotonicity => {
            let output = base
                .with_exec(vec![
                    "sh",
                    "-c",
                    &monotonicity_probe("TEMPORAL_SERVER_COMPAT", "Server-Compat-Downgrade"),
                ])
                .stdout()
                .await?;
            Ok(evaluate_monotonicity(check, &output))
        }
        CiCheck::BumpTrailer => {
            let output = base
                .with_exec(vec!["sh", "-c", BUMP_TRAILER_PROBE])
                .stdout()
                .await?;
            Ok(evaluate_bump_trailers(&output))
        }
        _ => unreachable!("governance dispatch excludes bar checks"),
    }
}

fn monotonicity_probe(constant: &str, trailer: &str) -> String {
    format!(
        r#"set -eu
tag=$(git tag --list 'v[0-9]*' --sort=-v:refname | head -n 1)
if [ -z "$tag" ]; then
  printf 'epoch\n'
  exit 0
fi
base=$(git show "$tag:{PINNED_RS}" | sed -n 's/^pub const {constant}: &str = "\([^"]*\)";.*/\1/p')
tip=$(sed -n 's/^pub const {constant}: &str = "\([^"]*\)";.*/\1/p' {PINNED_RS})
reason=$(git log -1 --format=%B | git interpret-trailers --parse | sed -n 's/^{trailer}: //p' | head -n 1)
printf 'compare\t%s\t%s\t%s\t%s\n' "$tag" "$base" "$tip" "$reason""#
    )
}

const BUMP_TRAILER_PROBE: &str = r#"set -eu
tag=$(git tag --list 'v[0-9]*' --sort=-v:refname | head -n 1)
if [ -z "$tag" ]; then
  printf 'epoch\n'
  exit 0
fi
commits=$(git rev-list --reverse "$tag..HEAD" -- crates/tokeira-build-info/src/pinned.rs)
if [ -z "$commits" ]; then
  printf 'unchanged\n'
  exit 0
fi
for commit in $commits; do
  parent=$(git rev-parse "$commit^")
  old=$(git show "$parent:crates/tokeira-build-info/src/pinned.rs" | sed -n 's/^pub const TEMPORAL_SERVER_COMPAT: &str = "\([^"]*\)";.*/\1/p')
  new=$(git show "$commit:crates/tokeira-build-info/src/pinned.rs" | sed -n 's/^pub const TEMPORAL_SERVER_COMPAT: &str = "\([^"]*\)";.*/\1/p')
  if [ "$old" = "$new" ]; then
    continue
  fi
  trailer=$(git show -s --format=%B "$commit" | git interpret-trailers --parse | sed -n 's/^Server-Compat-Bump: /Server-Compat-Bump: /p' | head -n 1)
  printf 'commit\t%s\t%s\t%s\t%s\n' "$commit" "$old" "$new" "$trailer"
done"#;

fn evaluate_monotonicity(check: CiCheck, output: &str) -> CiCheckResult {
    let trimmed = output.trim();
    if trimmed == "epoch" {
        return passing(
            check,
            "no earlier Tokeira release tag; this tree defines the monotonicity epoch",
        );
    }
    let fields = trimmed.splitn(5, '\t').collect::<Vec<_>>();
    if fields.len() != 5 || fields[0] != "compare" {
        return failing(
            check,
            "monotonicity probe returned malformed evidence",
            trimmed,
        );
    }
    let tag = fields[1];
    let base = parse_pin(fields[2]);
    let tip = parse_pin(fields[3]);
    let (Ok(base), Ok(tip)) = (base, tip) else {
        return failing(
            check,
            "compatibility pin is not semantic versioning",
            trimmed,
        );
    };
    if tip >= base {
        return passing(
            check,
            format!("pin {tip} is monotonic from {base} at {tag}"),
        );
    }
    if !fields[4].trim().is_empty() {
        return passing(
            check,
            format!(
                "pin regression {base} -> {tip} is explicitly overridden: {}",
                fields[4]
            ),
        );
    }
    failing(
        check,
        format!("pin regressed from {base} at {tag} to {tip} without an override trailer"),
        trimmed,
    )
}

fn parse_pin(value: &str) -> Result<CompatibilityVersion, crate::compat_bump::BumpTrailerError> {
    value.strip_prefix('v').unwrap_or(value).parse()
}

fn evaluate_bump_trailers(output: &str) -> CiCheckResult {
    let trimmed = output.trim();
    if trimmed == "epoch" {
        return passing(
            CiCheck::BumpTrailer,
            "no earlier Tokeira release tag; no post-epoch bump commits exist",
        );
    }
    if trimmed.is_empty() || trimmed == "unchanged" {
        return passing(
            CiCheck::BumpTrailer,
            "server compatibility pin is unchanged since the last release",
        );
    }
    for line in trimmed.lines() {
        let fields = line.splitn(5, '\t').collect::<Vec<_>>();
        if fields.len() != 5 || fields[0] != "commit" {
            return failing(
                CiCheck::BumpTrailer,
                "bump-trailer probe returned malformed evidence",
                line,
            );
        }
        let parsed = fields[4].parse::<BumpTrailer>();
        let (Ok(old), Ok(new), Ok(trailer)) = (parse_pin(fields[2]), parse_pin(fields[3]), parsed)
        else {
            return failing(
                CiCheck::BumpTrailer,
                format!(
                    "commit {} has a missing or malformed bump trailer",
                    fields[1]
                ),
                line,
            );
        };
        if trailer.old != old || trailer.new != new {
            return failing(
                CiCheck::BumpTrailer,
                format!(
                    "commit {} bump trailer does not match its pin diff",
                    fields[1]
                ),
                line,
            );
        }
    }
    passing(
        CiCheck::BumpTrailer,
        "every server compatibility change since the last release has an exact bump trailer",
    )
}

#[derive(Clone, Copy)]
struct BarCommand {
    check: CiCheck,
    rendered: &'static str,
    shell: &'static str,
}

const BAR_COMMANDS: [BarCommand; 8] = [
    BarCommand {
        check: CiCheck::Fmt,
        rendered: "cargo +nightly fmt --all",
        // Run the fleet's exact command in the copied workspace, then reject any
        // byte change. This preserves §10.4 command parity without mutating the host.
        shell: "find . -type f -name '*.rs' -not -path './target/*' -print0 | sort -z | xargs -0 sha256sum > /tmp/rust-before && cargo +\"$NIGHTLY_FMT_TOOLCHAIN\" fmt --all && find . -type f -name '*.rs' -not -path './target/*' -print0 | sort -z | xargs -0 sha256sum > /tmp/rust-after && cmp /tmp/rust-before /tmp/rust-after",
    },
    BarCommand {
        check: CiCheck::Lint,
        rendered: "cargo lint --locked",
        shell: "cargo lint --locked",
    },
    BarCommand {
        check: CiCheck::Check,
        rendered: "cargo check --workspace --locked",
        shell: "cargo check --workspace --locked",
    },
    BarCommand {
        check: CiCheck::Nextest,
        rendered: "cargo nextest run --workspace --locked",
        shell: "cargo nextest run --workspace --locked",
    },
    BarCommand {
        check: CiCheck::Doctests,
        rendered: "cargo test --workspace --doc --locked",
        shell: "cargo test --workspace --doc --locked",
    },
    BarCommand {
        check: CiCheck::Rustdoc,
        rendered: "RUSTDOCFLAGS=\"-D warnings\" cargo doc --workspace --no-deps --locked",
        shell: "RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps --locked",
    },
    BarCommand {
        check: CiCheck::Deny,
        rendered: "cargo deny check bans licenses sources",
        shell: "cargo deny check bans licenses sources",
    },
    BarCommand {
        check: CiCheck::Links,
        rendered: "lychee --offline --no-progress --hidden --exclude-path .git --exclude-path target --exclude-path spikes/dagger-rust-sdk/vendor --exclude-path vendor/dagger-sdk --exclude-path vendor/dagger-sdk-macros './**/*.md'",
        shell: "lychee --offline --no-progress --hidden --exclude-path .git --exclude-path target --exclude-path spikes/dagger-rust-sdk/vendor --exclude-path vendor/dagger-sdk --exclude-path vendor/dagger-sdk-macros './**/*.md'",
    },
];

/// Return the eight stable command lines shown in CI evidence.
pub fn workspace_bar_commands() -> Vec<&'static str> {
    BAR_COMMANDS.iter().map(|entry| entry.rendered).collect()
}

async fn execute_bar(
    client: &Client,
    root: &Path,
    layout: &GitLayout,
    check: CiCheck,
) -> Result<CiCheckResult, BuildError> {
    let command = BAR_COMMANDS
        .iter()
        .find(|candidate| candidate.check == check)
        .expect("bar dispatch only receives registered bar checks");
    let base = ci_builder(client, root, layout)?;
    let execution = base.with_exec_opts(
        vec!["sh", "-c", command.shell],
        &ContainerWithExecOpts::default().with_expect(ReturnType::Any),
    );
    let exit_code = execution.exit_code().await?;
    let stdout = execution.stdout().await?;
    let stderr = execution.stderr().await?;
    Ok(if exit_code == 0 {
        CiCheckResult {
            check,
            passed: true,
            summary: format!("{} passed", command.rendered),
            // Successful compiler output is both noisy and redundant with the
            // stable command summary. Preserve the streams only when they are
            // needed to diagnose a failure.
            details: None,
        }
    } else {
        CiCheckResult {
            check,
            passed: false,
            summary: format!("{} failed with exit code {exit_code}", command.rendered),
            details: command_details(&stdout, &stderr),
        }
    })
}

fn command_details(stdout: &str, stderr: &str) -> Option<String> {
    let mut streams = Vec::new();
    if !stdout.trim().is_empty() {
        streams.push(format!("stdout:\n{}", stdout.trim()));
    }
    if !stderr.trim().is_empty() {
        streams.push(format!("stderr:\n{}", stderr.trim()));
    }
    (!streams.is_empty()).then(|| streams.join("\n\n"))
}

fn ci_builder(client: &Client, root: &Path, layout: &GitLayout) -> Result<Container, BuildError> {
    let toolchain = rust_toolchain_version(root)?;
    let definition = format!(
        "{}\nnextest:{NEXTTEST_VERSION}\ndeny:{DENY_VERSION}\nlychee:{LYCHEE_VERSION}\nbuild-jobs:{CI_CARGO_BUILD_JOBS}",
        builder_definition(&toolchain)
    );
    let key = &tokeira_deployment::sha256_hex(definition.as_bytes())[..12];
    let query = client.query();
    let registry = query.cache_volume(format!("tokeira-ci-registry-{key}"));
    let target = query.cache_volume(format!("tokeira-ci-target-{key}"));
    let container = builder_toolchain(&query, &toolchain)
        .with_env_variable("CI", "1")
        // The workspace links several AWS SDK test graphs. Serial linking
        // keeps the fixed-size runner inside its memory limit without changing
        // any command in the bar evidence.
        .with_env_variable("CARGO_BUILD_JOBS", CI_CARGO_BUILD_JOBS)
        .with_mounted_cache(registry, "/usr/local/cargo/registry")
        .with_exec(vec![
            "sh",
            "-c",
            &format!(
                "cargo install --locked cargo-nextest --version {NEXTTEST_VERSION} && cargo install --locked cargo-deny --version {DENY_VERSION} && cargo install --locked lychee --version {LYCHEE_VERSION}"
            ),
        ])
        .with_mounted_cache(target, "/workspace/target");
    Ok(attach_workspace(container, client, root, layout))
}

fn attach_workspace(
    container: Container,
    client: &Client,
    root: &Path,
    layout: &GitLayout,
) -> Container {
    let query = client.query();
    let options = HostDirectoryOpts::default().with_exclude(CI_WORKSPACE_EXCLUDES.to_vec());
    let source = query
        .host()
        .directory_opts(root.display().to_string(), &options);
    let git = query
        .host()
        .directory(layout.common_dir.display().to_string());
    container
        .with_directory("/workspace", source)
        .with_directory("/repo.git", git)
        .with_new_file(&layout.worktree_pointer, "/workspace/.git")
        .with_workdir("/workspace")
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
                    "worktree git directory {} is outside common directory {}",
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
        .map_err(|error| BuildError::Validation {
            reason: format!("could not inspect the workspace Git layout: {error}"),
        })?;
    if !output.status.success() {
        return Err(BuildError::Validation {
            reason: format!(
                "workspace Git layout inspection failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ),
        });
    }
    Ok(PathBuf::from(
        String::from_utf8_lossy(&output.stdout).trim(),
    ))
}

fn passing(check: CiCheck, summary: impl Into<String>) -> CiCheckResult {
    CiCheckResult {
        check,
        passed: true,
        summary: summary.into(),
        details: None,
    }
}

fn failing(
    check: CiCheck,
    summary: impl Into<String>,
    details: impl Into<String>,
) -> CiCheckResult {
    CiCheckResult {
        check,
        passed: false,
        summary: summary.into(),
        details: Some(details.into()),
    }
}

/// Dagger build flavor selected by `tkr ci build`.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CiBuildMode {
    /// Fast local build with explicitly non-authoritative metadata.
    Dev,
    /// Clean-tree build with a validated deterministic metadata manifest.
    Versioned,
}

/// Inputs for a Dagger binary build.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CiBuildRequest {
    /// Host workspace copied into Dagger.
    pub workspace_root: PathBuf,
    /// Metadata and validation policy.
    pub mode: CiBuildMode,
}

/// Serializable build evidence returned to the CLI.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CiBuildReport {
    /// Build flavor.
    pub mode: CiBuildMode,
    /// Host path receiving the exported binary.
    pub artifact: PathBuf,
    /// JSON emitted by the built binary's `--version --json` path.
    pub build_info: Value,
}

/// Build `tokeirad` in Dagger and export the validated artifact.
pub async fn run_ci_build(
    request: &CiBuildRequest,
    client: &Client,
) -> Result<CiBuildReport, BuildError> {
    let layout = GitLayout::resolve(&request.workspace_root)?;
    let builder = ci_builder(client, &request.workspace_root, &layout)?;
    let (builder, binary) = match request.mode {
        CiBuildMode::Dev => (
            builder
                .with_exec(vec![
                    "cargo", "build", "--locked", "-p", "tokeirad", "--bin", "tokeirad",
                ])
                .with_exec(vec![
                    "sh",
                    "-c",
                    "target/debug/tokeirad --version --json > /tmp/tokeirad-build-info.json && cp target/debug/tokeirad /tokeirad",
                ]),
            "/tokeirad",
        ),
        CiBuildMode::Versioned => (
            builder.with_exec(vec!["sh", "-c", VERSIONED_BUILD_SCRIPT]),
            "/tokeirad",
        ),
    };
    let build_info = builder
        .file("/tmp/tokeirad-build-info.json")
        .contents()
        .await?;
    let build_info = serde_json::from_str(&build_info).map_err(|error| BuildError::Validation {
        reason: format!("built tokeirad returned invalid build-info JSON: {error}"),
    })?;
    let flavor = match request.mode {
        CiBuildMode::Dev => "dev",
        CiBuildMode::Versioned => "versioned",
    };
    let artifact = request
        .workspace_root
        .join("target")
        .join("tkr-ci")
        .join(flavor)
        .join("tokeirad");
    builder
        .file(binary)
        .export(artifact.display().to_string())
        .await?;
    Ok(CiBuildReport {
        mode: request.mode,
        artifact,
        build_info,
    })
}

const VERSIONED_BUILD_SCRIPT: &str = r#"set -eu
if [ -n "$(git status --porcelain --untracked-files=normal)" ]; then
  echo 'versioned build requires a clean repository' >&2
  exit 1
fi
derive_manifest() {
  output=$1
  cargo run --locked --quiet -p tkr -- compat show --json > /tmp/compat.json
  version=$(cargo metadata --locked --no-deps --format-version=1 | jq -r '.packages[] | select(.name == "tokeirad") | .version')
  git_sha=$(git rev-parse --short=8 HEAD)
  proto=$(sed -n 's/^pub const TEMPORAL_PROTO_VERSION: &str = "\([^"]*\)";.*/\1/p' crates/tokeira-build-info/src/pinned.rs)
  server=$(sed -n 's/^pub const TEMPORAL_SERVER_COMPAT: &str = "\([^"]*\)";.*/\1/p' crates/tokeira-build-info/src/pinned.rs)
  rust=$(sed -n 's/^channel = "\([^"]*\)".*/\1/p' rust-toolchain.toml)
  source_hash=$(git archive --format=tar HEAD | sha256sum | cut -d' ' -f1)
  feature=$(jq -r '.feature_matrix_digest' /tmp/compat.json)
  sdk=$(jq -r '.sdk_matrix_digest' /tmp/compat.json)
  printf 'TOKEIRA_VERSION=%s\nTOKEIRA_GIT_SHA=%s\nTEMPORAL_PROTO_VERSION=%s\nTEMPORAL_SERVER_COMPAT=%s\nRUST_TOOLCHAIN=%s\nSOURCE_TREE_HASH=%s\nFEATURE_MATRIX_DIGEST=%s\nSDK_MATRIX_DIGEST=%s\nBUILD_MODE=versioned\n' "$version" "$git_sha" "$proto" "$server" "$rust" "$source_hash" "$feature" "$sdk" > "$output"
}
derive_manifest /tmp/build-manifest-1
derive_manifest /tmp/build-manifest-2
cmp /tmp/build-manifest-1 /tmp/build-manifest-2
TOKEIRA_BUILD_MANIFEST_PATH=/tmp/build-manifest-1 cargo build --locked --release -p tokeirad --bin tokeirad
target/release/tokeirad --version --json > /tmp/tokeirad-build-info.json
value() { sed -n "s/^$1=//p" /tmp/build-manifest-1; }
test "$(jq -r '.tokeira_version' /tmp/tokeirad-build-info.json)" = "$(value TOKEIRA_VERSION)"
test "$(jq -r '.tokeira_git_sha' /tmp/tokeirad-build-info.json)" = "$(value TOKEIRA_GIT_SHA)"
test "$(jq -r '.temporal_proto_version' /tmp/tokeirad-build-info.json)" = "$(value TEMPORAL_PROTO_VERSION)"
test "$(jq -r '.temporal_server_compat' /tmp/tokeirad-build-info.json)" = "$(value TEMPORAL_SERVER_COMPAT)"
test "$(jq -r '.rust_toolchain' /tmp/tokeirad-build-info.json)" = "$(value RUST_TOOLCHAIN)"
test "$(jq -r '.source_tree_hash' /tmp/tokeirad-build-info.json)" = "$(value SOURCE_TREE_HASH)"
test "$(jq -r '.feature_matrix_digest' /tmp/tokeirad-build-info.json)" = "$(value FEATURE_MATRIX_DIGEST)"
test "$(jq -r '.sdk_matrix_digest' /tmp/tokeirad-build-info.json)" = "$(value SDK_MATRIX_DIGEST)"
test "$(jq -r '.build_mode' /tmp/tokeirad-build-info.json)" = 'versioned'
cp target/release/tokeirad /tokeirad"#;

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use proptest::prelude::*;

    use super::*;

    struct FakeDagger {
        seen: Mutex<Vec<CiCheck>>,
    }

    #[async_trait]
    impl DaggerClient for FakeDagger {
        async fn execute_ci_check(
            &self,
            _request: &CiCheckRequest,
            check: CiCheck,
        ) -> Result<CiCheckResult, BuildError> {
            self.seen.lock().expect("fake lock").push(check);
            Ok(passing(check, format!("{} passed", check.name())))
        }
    }

    #[tokio::test]
    async fn empty_selection_runs_the_complete_registry() {
        let fake = FakeDagger {
            seen: Mutex::new(Vec::new()),
        };
        let report = run_ci_checks(
            &CiCheckRequest {
                workspace_root: PathBuf::from("/unused"),
                checks: Vec::new(),
            },
            &fake,
        )
        .await
        .expect("fake CI run");
        assert!(report.passed());
        assert_eq!(*fake.seen.lock().expect("fake lock"), CiCheck::ALL);
    }

    #[test]
    fn workspace_bar_matches_the_fleet_finishing_bar() {
        let expected = vec![
            "cargo +nightly fmt --all",
            "cargo lint --locked",
            "cargo check --workspace --locked",
            "cargo nextest run --workspace --locked",
            "cargo test --workspace --doc --locked",
            "RUSTDOCFLAGS=\"-D warnings\" cargo doc --workspace --no-deps --locked",
            "cargo deny check bans licenses sources",
            "lychee --offline --no-progress --hidden --exclude-path .git --exclude-path target --exclude-path spikes/dagger-rust-sdk/vendor --exclude-path vendor/dagger-sdk --exclude-path vendor/dagger-sdk-macros './**/*.md'",
        ];
        assert_eq!(workspace_bar_commands(), expected);
    }

    #[test]
    fn failed_command_evidence_keeps_both_output_streams() {
        let details = command_details("partial output\n", "the compiler error\n")
            .expect("non-empty streams produce details");
        assert_eq!(
            details,
            "stdout:\npartial output\n\nstderr:\nthe compiler error"
        );
    }

    #[test]
    fn no_release_tag_is_an_explicit_epoch() {
        for check in [
            CiCheck::ProtoMonotonicity,
            CiCheck::ServerCompatMonotonicity,
        ] {
            let result = evaluate_monotonicity(check, "epoch\n");
            assert!(result.passed);
            assert!(result.summary.contains("monotonicity epoch"));
        }
    }

    proptest! {
        // Feature: release-process, Property 1: regressions require a non-empty override.
        #[test]
        fn pin_regression_detection_and_override(
            base_major in 1_u64..20,
            tip_major in 0_u64..20,
            reason in "[a-zA-Z0-9 ]{0,30}",
        ) {
            prop_assume!(tip_major < base_major);
            let output = format!("compare\tv0.1.0\t{base_major}.0.0\t{tip_major}.0.0\t{reason}\n");
            let result = evaluate_monotonicity(CiCheck::ServerCompatMonotonicity, &output);
            prop_assert_eq!(result.passed, !reason.trim().is_empty());
        }

        // Feature: release-process, Property 2: trailer values must equal the observed diff.
        #[test]
        fn bump_trailer_must_match_diff(
            old_major in 0_u64..20,
            new_major in 0_u64..20,
            trailer_old in 0_u64..20,
        ) {
            let output = format!(
                "commit\tabc\t{old_major}.0.0\t{new_major}.0.0\tServer-Compat-Bump: {trailer_old}.0.0 -> {new_major}.0.0, trigger: 1\n"
            );
            let result = evaluate_bump_trailers(&output);
            prop_assert_eq!(result.passed, old_major == trailer_old);
        }

        // Feature: release-process, Property 6: CI evidence round-trips unchanged.
        #[test]
        fn report_json_round_trips(
            passed in any::<bool>(),
            summary in ".{0,80}",
            details in proptest::option::of(".{0,80}"),
        ) {
            let report = CiCheckReport {
                results: vec![CiCheckResult {
                    check: CiCheck::Check,
                    passed,
                    summary,
                    details,
                }],
            };
            let encoded = serde_json::to_string(&report).expect("serialize report");
            let decoded = serde_json::from_str::<CiCheckReport>(&encoded).expect("deserialize report");
            prop_assert_eq!(decoded, report);
        }
    }
}
