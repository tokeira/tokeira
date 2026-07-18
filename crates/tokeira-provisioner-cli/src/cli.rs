//! The `tkp` command surface and dispatch.
//!
//! [`run`] is the whole binary: a per-platform `tkp` parses the CLI here and
//! dispatches every verb through the shell, injecting its
//! [`ProvisionerPlatform`] for the resource realization. Read-only verbs never
//! gate or lock; mutating verbs run under the deployment's operation lock
//! (Req 11), with `rollback` holding one continuous lock across its whole
//! sequence (12.2).

use std::path::PathBuf;

use anyhow::Result;
use clap::{Args, Parser, Subcommand};

use crate::{
    ProvisionerPlatform, apply, describe, destroy, init, lock, plan, revert, rollback, upgrade,
};

#[derive(Parser)]
#[command(
    name = "tkp",
    version,
    about = "Tokeira platform provisioner — deployment lifecycle"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Day-0 mandatory versioning: write the first provenance stamp + integrity
    /// manifest before any resource create.
    Init(LifecycleArgs),
    /// Read-only report of identity, recorded provenance, binding verdict, and
    /// state facts. Never gates.
    Describe(DescribeArgs),
    /// Show the binding verdict + the infrastructure plan. Read-only; never gates.
    Plan(LifecycleArgs),
    /// Plan and apply the deployment, gated on the binding.
    Apply(LifecycleArgs),
    /// Tear down the deployment's infrastructure, gated on the binding. Irreversible.
    Destroy(DestroyArgs),
    /// Revert to a prior config revision — a same-engine apply, gated on the binding.
    Revert(RevertArgs),
    /// Upgrade to a new engine identity.
    Upgrade(LifecycleArgs),
    /// Roll back to the retained prior configuration revision.
    Rollback(LifecycleArgs),
}

#[derive(Args)]
struct DescribeArgs {
    /// Deployment directory holding the state envelope.
    #[arg(long)]
    deployment_dir: PathBuf,
    /// Emit JSON instead of human-readable text.
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
struct LifecycleArgs {
    /// Deployment directory holding the state envelope.
    #[arg(long)]
    deployment_dir: PathBuf,
}

#[derive(Args)]
struct DestroyArgs {
    /// Deployment directory holding the state envelope.
    #[arg(long)]
    deployment_dir: PathBuf,
    /// Confirm the irreversible teardown (required).
    #[arg(long)]
    yes: bool,
}

#[derive(Args)]
struct RevertArgs {
    /// Deployment directory holding the state envelope.
    #[arg(long)]
    deployment_dir: PathBuf,
    /// The prior config revision to revert to (a same-engine re-apply of its config).
    #[arg(long)]
    to: u64,
}

/// Parse the CLI and run the selected verb with `platform` supplying the
/// resource realization. This is the per-platform binary's entire `main`.
pub async fn run<P: ProvisionerPlatform>(platform: P) -> Result<()> {
    match Cli::parse().command {
        // Read-only: never gates, never locks.
        Command::Describe(args) => describe::describe(&args.deployment_dir, args.json).await,
        Command::Plan(args) => plan::plan(&platform, &args.deployment_dir).await,
        // Mutating verbs run under the deployment's operation lock (Req 11).
        // `rollback` holds one continuous lock across its whole sequence (12.2).
        Command::Init(args) => {
            let dir = args.deployment_dir;
            lock::with_operation_lock(&dir, "init", || init::init(&platform, &dir)).await
        }
        Command::Apply(args) => {
            let dir = args.deployment_dir;
            lock::with_operation_lock(&dir, "apply", || apply::apply(&platform, &dir)).await
        }
        Command::Destroy(args) => {
            let dir = args.deployment_dir;
            let yes = args.yes;
            lock::with_operation_lock(&dir, "destroy", || destroy::destroy(&platform, &dir, yes))
                .await
        }
        Command::Revert(args) => {
            let dir = args.deployment_dir;
            let to = args.to;
            lock::with_operation_lock(&dir, "revert", || revert::revert(&platform, &dir, to)).await
        }
        Command::Upgrade(args) => {
            let dir = args.deployment_dir;
            lock::with_operation_lock(&dir, "upgrade", || upgrade::upgrade(&platform, &dir)).await
        }
        Command::Rollback(args) => {
            let dir = args.deployment_dir;
            lock::with_operation_lock(&dir, "rollback", || rollback::rollback(&platform, &dir))
                .await
        }
    }
}
