//! Spike: the first-party Dagger Rust SDK driving tokeira's two real builds.
//!
//! Three probes, in ascending weight:
//! - `probe`    — session lifecycle only: connect, version, close. The
//!                envar-magic replacement test.
//! - `tokeirad` — the tokeirad image build (build.rs parity) rebuilt
//!                SDK-idiomatically: cache volumes instead of cargo-chef,
//!                engine-side `export_image`, captured stdout.
//! - `tkp`      — the bound-provisioner build (provisioner.rs parity):
//!                real assembled source + snapshot, deliberately cold,
//!                per-target build → strip → export → host-side hash.
//!
//! Session recipe (the fork's engine + its embedded CLI):
//!   _EXPERIMENTAL_DAGGER_RUNNER_HOST=docker-container://<engine-container>
//!   _EXPERIMENTAL_DAGGER_CLI_BIN=<cli extracted from the engine image>
//! See README.md for the full engine bring-up.

mod tkp;
mod tokeirad;

use std::time::Instant;

use anyhow::{Context, Result, bail};

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let command = args.first().map(String::as_str).unwrap_or("probe");
    match command {
        "probe" => probe().await,
        "tokeirad" => tokeirad::build(args.get(1).map(String::as_str)).await,
        "tkp" => tkp::build(args.get(1).map(String::as_str)).await,
        other => bail!("unknown probe `{other}` (probe|tokeirad|tkp)"),
    }
}

/// The session-lifecycle probe: everything the old plumbing needed a
/// re-exec dance for, in three lines.
async fn probe() -> Result<()> {
    let started = Instant::now();
    let client = dagger_sdk::connect()
        .await
        .context("dagger_sdk::connect()")?;
    let version = client.query().version().await.context("query version")?;
    println!(
        "connected to Dagger {version} in {:?} — no env vars set by us, no re-exec",
        started.elapsed()
    );
    client.close().await.context("close")?;
    println!("closed cleanly in {:?} total", started.elapsed());
    Ok(())
}

/// Walk up from the spike to the tokeira workspace root.
pub(crate) fn workspace_root() -> Result<std::path::PathBuf> {
    let mut dir = std::env::current_dir()?;
    loop {
        if dir.join("Cargo.lock").exists() && dir.join("rust-toolchain.toml").exists() {
            return Ok(dir);
        }
        if !dir.pop() {
            bail!("run from inside the tokeira workspace");
        }
    }
}

/// The toolchain channel pinned by the workspace.
pub(crate) fn toolchain(workspace: &std::path::Path) -> Result<String> {
    tokeira_build::rust_toolchain_version(workspace).map_err(Into::into)
}
