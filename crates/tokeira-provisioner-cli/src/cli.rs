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
    ProvisionerPlatform, apply, definition, deploy, describe, destroy, init, lock, plan, revert,
    rollback, scale, upgrade,
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
    /// Emit the complete structured result as JSON. Depth-blind: the model is
    /// always whole, whatever `--detail` says (the contract's collapse rule).
    #[arg(long, global = true)]
    json: bool,
    /// Show the evidence behind the summary — per-resource lines, field
    /// diffs, digests, provenance.
    #[arg(long, global = true)]
    detail: bool,
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
    /// The deployment definition — the interpreted `.tkd`.
    #[command(subcommand)]
    Definition(DefinitionCommand),
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
    Rollback(RollbackArgs),
}

#[derive(Args)]
struct RollbackArgs {
    /// Deployment directory holding the state envelope.
    #[arg(long)]
    deployment_dir: PathBuf,
    /// Two-binary orchestration (task 19.3): stop after B's delete-only pass
    /// and the re-pin commit, leaving the rollback marker open — the
    /// orchestrator relaunches the retained A, whose `rollback` re-run
    /// resumes at the reconcile. Internal — set by `tkr`, hidden from help.
    #[arg(long, hide = true)]
    handoff: bool,
}

#[derive(Subcommand)]
enum DefinitionCommand {
    /// Parse + interpret the definition in memory — no providers touched,
    /// nothing changes. Read-only; never gates.
    Check(CheckArgs),
}

#[derive(Args)]
struct CheckArgs {
    /// Deployment directory holding the definition (and naming the check's
    /// deployment context).
    #[arg(long)]
    deployment_dir: PathBuf,
    /// Check this definition file instead of the deployment's own — authoring
    /// mode: the report carries no deployment context, only the path.
    #[arg(long)]
    definition: Option<PathBuf>,
}

#[derive(Subcommand)]
enum InfraCommand {
    /// Show the binding verdict + the infrastructure plan. Read-only; never gates.
    Plan(PlanArgs),
    /// Reconcile infrastructure to desired, gated on the binding.
    Apply(ApplyArgs),
    /// Tear down the deployment's infrastructure, gated on the binding. Irreversible.
    Destroy(DestroyArgs),
}

#[derive(Subcommand)]
enum DeployCommand {
    /// Show the binding verdict + the workload plan. Read-only; never gates.
    Plan(PlanArgs),
    /// Reconcile the workload to desired, gated on the binding.
    Apply(ApplyArgs),
}

#[derive(Args)]
struct PlanArgs {
    /// Deployment directory holding the state envelope.
    #[arg(long)]
    deployment_dir: PathBuf,
    /// Also write the complete explanation model as JSON to this path.
    /// Orthogonal to `--json`: the report still renders to stdout.
    #[arg(long, value_name = "PATH")]
    explanation: Option<PathBuf>,
}

#[derive(Args)]
struct ApplyArgs {
    /// Deployment directory holding the state envelope.
    #[arg(long)]
    deployment_dir: PathBuf,
    /// Confirm a destructive plan (deletes or replacements). An apply whose
    /// plan is destructive refuses without it (review before action, §4).
    #[arg(long)]
    yes: bool,
    /// Also write the complete explanation model as JSON to this path.
    /// Orthogonal to `--json`: the report still renders to stdout.
    #[arg(long, value_name = "PATH")]
    explanation: Option<PathBuf>,
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
    let cli = Cli::parse();
    // One resolution of the output contract's global flags; the collapse rule
    // (`--json` is depth-blind) is enforced inside `Mode::resolve`.
    let mode = tokeira_report::Mode::resolve(cli.json, cli.detail);
    match cli.command {
        // Read-only: never gates, never locks.
        Command::Describe(args) => {
            describe::describe(&platform, &args.deployment_dir, cli.json, cli.detail).await
        }
        Command::Definition(DefinitionCommand::Check(args)) => {
            definition::check(
                &platform,
                &args.deployment_dir,
                args.definition.as_deref(),
                mode,
            )
            .await
        }
        Command::Infra(InfraCommand::Plan(args)) => {
            plan::plan(
                &platform,
                &args.deployment_dir,
                mode,
                args.explanation.as_deref(),
            )
            .await
        }
        Command::Deploy(DeployCommand::Plan(args)) => {
            deploy::deploy_plan(
                &platform,
                &args.deployment_dir,
                mode,
                args.explanation.as_deref(),
            )
            .await
        }
        // Mutating verbs run under the deployment's operation lock (Req 11).
        // `rollback` holds one continuous lock across its whole sequence (12.2).
        Command::Init(args) => {
            let dir = args.deployment_dir;
            lock::with_operation_lock(&dir, "init", || init::init(&platform, &dir)).await
        }
        Command::Infra(InfraCommand::Apply(args)) => {
            let dir = args.deployment_dir;
            let yes = args.yes;
            let explanation = args.explanation;
            lock::with_operation_lock(&dir, "apply", || {
                apply::apply(&platform, &dir, yes, mode, explanation.as_deref())
            })
            .await
        }
        Command::Infra(InfraCommand::Destroy(args)) => {
            let dir = args.deployment_dir;
            let yes = args.yes;
            lock::with_operation_lock(&dir, "destroy", || destroy::destroy(&platform, &dir, yes))
                .await
        }
        Command::Deploy(DeployCommand::Apply(args)) => {
            let dir = args.deployment_dir;
            let yes = args.yes;
            let explanation = args.explanation;
            lock::with_operation_lock(&dir, "deploy-apply", || {
                deploy::deploy_apply(&platform, &dir, yes, mode, explanation.as_deref())
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
            let handoff = args.handoff;
            lock::with_operation_lock(&dir, "rollback", || {
                rollback::rollback(&platform, &dir, handoff)
            })
            .await
        }
    }
}
