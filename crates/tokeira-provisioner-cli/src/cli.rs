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
    ProvisionerPlatform, apply, deploy, describe, destroy, init, lock, plan, revert, rollback,
    scale, upgrade,
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
    /// manifest before any resource create. Internal — an inception step of
    /// `tkr deployment create` (Req 6.5), not an operator verb, so it is hidden
    /// from help.
    #[command(hide = true)]
    Init(LifecycleArgs),
    /// Read-only report of identity, recorded provenance, binding verdict, and
    /// state facts. Never gates.
    Describe(DescribeArgs),
    /// Substrate — the infrastructure the deployment stands on. Namespaced to
    /// mirror `tkr` so forwarding is a transparent pass-through (Req 7.3).
    #[command(subcommand)]
    Infra(InfraCommand),
    /// Workload — the services that run on the substrate. Conditionally
    /// realized: a platform whose workload rides the infra universe realizes
    /// these as the infra verbs.
    #[command(subcommand)]
    Deploy(DeployCommand),
    /// Change workload capacity (`<dim>=<n>` specs); a config revision + a
    /// workload apply. `NotApplicable` where the platform has no scale dimension.
    Scale(ScaleArgs),
    /// Revert to a prior config revision — a same-engine apply, gated on the binding.
    Revert(RevertArgs),
    /// Upgrade to a new engine identity.
    Upgrade(LifecycleArgs),
    /// Roll back to the retained prior configuration revision.
    Rollback(LifecycleArgs),
}

#[derive(Subcommand)]
enum InfraCommand {
    /// Show the binding verdict + the infrastructure plan. Read-only; never gates.
    Plan(LifecycleArgs),
    /// Reconcile infrastructure to desired, gated on the binding.
    Apply(LifecycleArgs),
    /// Tear down the deployment's infrastructure, gated on the binding. Irreversible.
    Destroy(DestroyArgs),
}

#[derive(Subcommand)]
enum DeployCommand {
    /// Show the binding verdict + the workload plan. Read-only; never gates.
    Plan(LifecycleArgs),
    /// Reconcile the workload to desired, gated on the binding.
    Apply(LifecycleArgs),
}

#[derive(Args)]
struct ScaleArgs {
    /// Deployment directory holding the state envelope.
    #[arg(long)]
    deployment_dir: PathBuf,
    /// Capacity specs (`<dim>=<n>`), platform-interpreted.
    #[arg(required = true)]
    specs: Vec<String>,
}

#[derive(Args)]
struct DescribeArgs {
    /// Deployment directory holding the state envelope.
    #[arg(long)]
    deployment_dir: PathBuf,
    /// Emit the full verification record as JSON (stable, machine-parseable).
    #[arg(long)]
    json: bool,
    /// Human-readable verification/debug view: the complete per-artifact
    /// manifest, retained revisions, state heads. Default is the short operator
    /// view.
    #[arg(long, conflicts_with = "json")]
    verbose: bool,
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
        Command::Describe(args) => {
            describe::describe(&platform, &args.deployment_dir, args.json, args.verbose).await
        }
        Command::Infra(InfraCommand::Plan(args)) => {
            plan::plan(&platform, &args.deployment_dir).await
        }
        Command::Deploy(DeployCommand::Plan(args)) => {
            deploy::deploy_plan(&platform, &args.deployment_dir).await
        }
        // Mutating verbs run under the deployment's operation lock (Req 11).
        // `rollback` holds one continuous lock across its whole sequence (12.2).
        Command::Init(args) => {
            let dir = args.deployment_dir;
            lock::with_operation_lock(&dir, "init", || init::init(&platform, &dir)).await
        }
        Command::Infra(InfraCommand::Apply(args)) => {
            let dir = args.deployment_dir;
            lock::with_operation_lock(&dir, "apply", || apply::apply(&platform, &dir)).await
        }
        Command::Infra(InfraCommand::Destroy(args)) => {
            let dir = args.deployment_dir;
            let yes = args.yes;
            lock::with_operation_lock(&dir, "destroy", || destroy::destroy(&platform, &dir, yes))
                .await
        }
        Command::Deploy(DeployCommand::Apply(args)) => {
            let dir = args.deployment_dir;
            lock::with_operation_lock(&dir, "deploy-apply", || {
                deploy::deploy_apply(&platform, &dir)
            })
            .await
        }
        Command::Scale(args) => {
            let dir = args.deployment_dir;
            let specs = args.specs;
            lock::with_operation_lock(&dir, "scale", || scale::scale(&platform, &dir, &specs)).await
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
