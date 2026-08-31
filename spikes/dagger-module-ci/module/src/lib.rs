//! Tokeira CI as a Dagger module — the live rust.4 surface of the
//! dagger-module-ci spike (`../README.md`; the engine-free authored shape it
//! grew from is `../shape/`).
//!
//! Check bodies mirror client-mode `tkr ci check`
//! (`crates/tokeira-build/src/pipelines/ci.rs`) — same builder image line, same
//! pinned fmt nightly, same parity shell — so the module-vs-client comparison
//! measures dispatch and wire behaviour, not differing check content.

pub mod dagger_generated;

use dagger_generated::ModuleContext;
use dagger_sdk as sdk;
use sdk::{ContainerWithExecOpts, ReturnType};

// Mirrored verbatim from the client-mode pipeline so both interiors build the
// same container. The values are duplicated (not imported) because a module
// crate is a standalone project: it cannot depend on tokeira-build by design.
const RUST_TOOLCHAIN: &str = "1.97.1";
const CI_FMT_NIGHTLY: &str = "nightly-2026-06-16";
const BUILDER_APT_LINE: &str = "apt-get update && apt-get install -y --no-install-recommends pkg-config libssl-dev protobuf-compiler libprotobuf-dev ca-certificates cmake clang git curl jq && rm -rf /var/lib/apt/lists/*";
const NEXTTEST_VERSION: &str = "0.9.143";
// Run the fleet's exact fmt command in the mounted source, then reject any byte
// change — command parity with AGENTS.md §10.4 without mutating the host.
const FMT_PARITY_SHELL: &str = "find . -type f -name '*.rs' -not -path './target/*' -print0 | sort -z | xargs -0 sha256sum > /tmp/rust-before && cargo +\"$NIGHTLY_FMT_TOOLCHAIN\" fmt --all && find . -type f -name '*.rs' -not -path './target/*' -print0 | sort -z | xargs -0 sha256sum > /tmp/rust-after && cmp /tmp/rust-before /tmp/rust-after";

/// One finished check's outcome — the wire projection of the report row that
/// client-mode `run_ci_checks` returns in-process (`CiCheckResult` in
/// `tokeira-build`). Crossing the module boundary as a typed Dagger object is
/// the seam the rust.4 release unblocked for real round-trip measurement.
#[sdk::object]
pub struct CiCheckOutcome {
    /// Bar-check name exactly as the finishing bar spells it.
    #[dagger(field)]
    check: String,
    /// Verdict, preserved for aggregation; `dagger check` itself reads the
    /// function's success or failure.
    #[dagger(field)]
    passed: bool,
    /// One-line operator-facing summary.
    #[dagger(field)]
    summary: String,
}

/// Root object of the Tokeira CI module: the module-mode counterpart of
/// `run_ci_checks`. Each bar check is one `role = "check"` function so
/// `dagger check` (and patterns like `ci:fmt`) enumerate it.
#[sdk::object(root, rename = "tokeiraCi")]
pub struct TokeiraCi {}

#[sdk::methods]
impl TokeiraCi {
    /// Modules are constructed per call; the CI module carries no
    /// configuration — the workspace source arrives per-function.
    #[dagger(constructor)]
    pub fn new() -> TokeiraCi {
        TokeiraCi {}
    }

    /// The cheap bar check: rustfmt under the pinned nightly, byte-parity.
    ///
    /// The contextual default supplies the Dagger workspace source with build
    /// output excluded at the mount (the filter client-mode applies by hand);
    /// pass `--source` explicitly to check another tree.
    #[dagger(function, role = "check")]
    pub async fn fmt(
        &self,
        #[dagger(context)] ctx: ModuleContext,
        #[dagger(default_path = "/", ignore = ["target", "**/target", ".git", ".tokeira-build", ".env*", "**/.env*", "artifacts", "**/*.log"])]
        source: sdk::Directory,
    ) -> Result<CiCheckOutcome, sdk::ModuleError> {
        let execution = fmt_builder(&ctx)
            .with_directory("/workspace", source)
            .with_workdir("/workspace")
            .with_exec_opts(
                vec!["sh", "-c", FMT_PARITY_SHELL],
                &ContainerWithExecOpts::default().with_expect(ReturnType::Any),
            );
        let exit_code = execution.exit_code().await?;
        if exit_code == 0 {
            Ok(outcome("fmt", true, "cargo +nightly fmt --all passed"))
        } else {
            let stderr = execution.stderr().await?;
            Err(sdk::ModuleError::new(format!(
                "cargo +nightly fmt --all failed with exit code {exit_code}"
            ))
            .with_detail("stderr", stderr.trim().into())
            .map_err(|_| sdk::ModuleError::new("fmt failure detail could not be attached"))?)
        }
    }

    /// The expensive bar check: `cargo nextest run --workspace --locked` at the
    /// source root — the minutes-long single engine query that finding F3 must
    /// clear before module-mode can carry real CI.
    #[dagger(function, role = "check")]
    pub async fn nextest(
        &self,
        #[dagger(context)] ctx: ModuleContext,
        #[dagger(default_path = "/", ignore = ["target", "**/target", ".git", ".tokeira-build", ".env*", "**/.env*", "artifacts", "**/*.log"])]
        source: sdk::Directory,
    ) -> Result<CiCheckOutcome, sdk::ModuleError> {
        let query = ctx.query();
        // Cache volumes keyed like client-mode: stable toolchain inputs only,
        // so editing Rust code reuses warm registry and target volumes.
        let registry = query.cache_volume(format!("tokeira-ci-module-registry-{RUST_TOOLCHAIN}"));
        let target = query.cache_volume(format!("tokeira-ci-module-target-{RUST_TOOLCHAIN}"));
        let execution = fmt_builder(&ctx)
            .with_mounted_cache(registry, "/usr/local/cargo/registry")
            .with_exec(vec![
                "sh",
                "-c",
                &format!("cargo install --locked cargo-nextest --version {NEXTTEST_VERSION}"),
            ])
            .with_directory("/workspace", source)
            .with_workdir("/workspace")
            .with_mounted_cache(target, "/workspace/target")
            .with_exec_opts(
                vec!["sh", "-c", "cargo nextest run --workspace --locked"],
                &ContainerWithExecOpts::default().with_expect(ReturnType::Any),
            );
        let exit_code = execution.exit_code().await?;
        if exit_code == 0 {
            Ok(outcome(
                "nextest",
                true,
                "cargo nextest run --workspace --locked passed",
            ))
        } else {
            let stderr = execution.stderr().await?;
            Err(sdk::ModuleError::new(format!(
                "cargo nextest run --workspace --locked failed with exit code {exit_code}"
            ))
            .with_detail("stderr", stderr.trim().into())
            .map_err(|_| sdk::ModuleError::new("nextest failure detail could not be attached"))?)
        }
    }

    /// Finding-F3 probe: holds one module→engine query open for `seconds` by
    /// awaiting a single long container exec. Bisecting `seconds` across the
    /// documented ~30s boundary answers whether module-mode can carry real CI
    /// checks. Distinct `seconds` values defeat the exec cache; repeat a value
    /// only knowing the repeat returns cached instantly.
    #[dagger(function)]
    pub async fn probe_long_call(
        &self,
        #[dagger(context)] ctx: ModuleContext,
        #[dagger(default = 45)] seconds: i64,
    ) -> Result<String, sdk::ModuleError> {
        let output = ctx
            .query()
            .container()
            .from("docker.io/library/alpine:3.20")
            .with_exec(vec![
                "sh",
                "-c",
                &format!("sleep {seconds} && echo probe-held-{seconds}s"),
            ])
            .stdout()
            .await?;
        Ok(output.trim().to_owned())
    }
}

/// The shared builder: client-mode's `builder_toolchain` mirrored — pinned
/// stable base, one apt line, the dated fmt nightly with rustfmt only.
fn fmt_builder(ctx: &ModuleContext) -> sdk::Container {
    ctx.query()
        .container()
        .from(format!("rust:{RUST_TOOLCHAIN}-slim-bookworm"))
        .with_exec(vec!["sh", "-c", BUILDER_APT_LINE])
        .with_exec(vec![
            "rustup",
            "toolchain",
            "install",
            CI_FMT_NIGHTLY,
            "--profile",
            "minimal",
            "--component",
            "rustfmt",
        ])
        .with_env_variable("CARGO_TERM_COLOR", "never")
        .with_env_variable("NIGHTLY_FMT_TOOLCHAIN", CI_FMT_NIGHTLY)
        .with_env_variable("RUSTUP_TOOLCHAIN", RUST_TOOLCHAIN)
}

fn outcome(check: &str, passed: bool, summary: &str) -> CiCheckOutcome {
    CiCheckOutcome {
        check: check.to_owned(),
        passed,
        summary: summary.to_owned(),
    }
}
