//! The tokeirad image build, SDK-idiomatically.
//!
//! Parity target: `tokeira-build/src/pipelines/build.rs`. Differences under
//! evaluation, each impossible with the hand-rolled client:
//! - **Cache volumes** for the cargo registry and the build `target/`
//!   replace the three-stage cargo-chef choreography outright — the whole
//!   planner/cacher dance existed to fake incremental builds with layer
//!   caching.
//! - **`export_image`** loads the result into the host image store from the
//!   engine side — no tarball, no `docker load` subprocess, no double
//!   export for a second tag.
//! - **Captured stdout** — the old client could not read any output.

use std::time::Instant;

use anyhow::{Context, Result};
use dagger_sdk::HostDirectoryOpts;

/// The same exclude list `build.rs` codifies as a survival invariant (the
/// 7-minute-upload lesson).
const WORKSPACE_EXCLUDES: &[&str] = &[
    "target",
    "**/target",
    ".git",
    ".github",
    ".vscode",
    ".idea",
    ".kiro",
    ".DS_Store",
    ".claude",
    "artifacts",
    "dev",
    "docs",
    "fixtures",
    "ops",
    "schemas",
    "spec",
    "spikes",
    "tokeira.code-workspace",
    "tokeirad.log",
    "**/*.log",
];

pub(crate) async fn build(tag: Option<&str>) -> Result<()> {
    let workspace = crate::workspace_root()?;
    let toolchain = crate::toolchain(&workspace)?;
    let tag = tag.unwrap_or("tokeirad:sdk-spike");
    let total = Instant::now();

    let client = dagger_sdk::connect().await.context("connect")?;
    let query = client.query();

    let upload = Instant::now();
    let opts = HostDirectoryOpts::default().with_exclude(WORKSPACE_EXCLUDES.to_vec());
    let source = query
        .host()
        .directory_opts(workspace.display().to_string(), &opts);
    // Force the upload so its cost is visible in isolation.
    let entries = source.entries().await.context("upload workspace")?;
    println!(
        "workspace uploaded ({} top-level entries) in {:?}",
        entries.len(),
        upload.elapsed()
    );

    // Two cache volumes replace cargo-chef entirely.
    let registry_cache = query.cache_volume("tokeira-cargo-registry");
    let target_cache = query.cache_volume("tokeira-build-target");

    let builder = query
        .container()
        .from(format!("rust:{toolchain}-slim-bookworm"))
        .with_exec(vec![
            "sh",
            "-c",
            "apt-get update && apt-get install -y --no-install-recommends \
             pkg-config libssl-dev protobuf-compiler libprotobuf-dev ca-certificates \
             && rm -rf /var/lib/apt/lists/*",
        ])
        .with_env_variable("CARGO_TERM_COLOR", "never")
        .with_env_variable("RUSTUP_TOOLCHAIN", &toolchain)
        .with_mounted_cache(registry_cache, "/usr/local/cargo/registry")
        .with_mounted_cache(target_cache, "/src/target")
        .with_directory("/src", source)
        .with_workdir("/src");

    let compile = Instant::now();
    let built = builder
        .with_exec(vec![
            "cargo",
            "build",
            "--release",
            "--locked",
            "--bin",
            "tokeirad",
            "-p",
            "tokeirad",
        ])
        // The binary must leave the cache mount to survive into the layer.
        .with_exec(vec!["cp", "target/release/tokeirad", "/tokeirad"])
        .with_exec(vec!["strip", "/tokeirad"]);
    // Output capture — the old client had no way to read any of this.
    // (cargo reports on stderr; stdout of a build is rightly quiet.)
    let build_output = built.stderr().await.context("cargo build")?;
    println!(
        "compiled in {:?} (captured {} bytes of build output)",
        compile.elapsed(),
        build_output.len()
    );

    let runtime = query
        .container()
        .from("cgr.dev/chainguard/glibc-dynamic:latest")
        .with_file("/usr/local/bin/tokeirad", built.file("/tokeirad"))
        .with_user("nonroot")
        .with_entrypoint(vec!["/usr/local/bin/tokeirad"]);

    let export = Instant::now();
    // Engine-side load into the host image store: no tarball, no
    // `docker load`, one call per tag. `exportImage` is Void-typed; the
    // rust.2 pair (engine encodes Void as JSON null, SDK decodes strictly
    // null) makes this the whole-stack proof of that fix — a failure here
    // is a probe failure, never a fallback.
    runtime
        .export_image(tag)
        .await
        .context("export_image (Void-typed; exercises the rust.2 null encoding end to end)")?;
    println!("exported image `{tag}` in {:?}", export.elapsed());

    client.close().await.context("close")?;
    println!("tokeirad image build complete in {:?}", total.elapsed());
    Ok(())
}
