//! The `tkp` command surface and dispatch.
//!
//! [`run`] is the whole binary: a per-platform `tkp` parses the CLI here,
//! admits the deployment once at this boundary, and dispatches every verb
//! through the shell over the bound [`Engine`]. Read-only verbs never gate
//! or lock; mutating verbs run under the deployment's operation lock
//!, with `rollback` holding one continuous lock across its whole
//! sequence.

use std::path::PathBuf;

use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use futures_util::StreamExt;
use tokeira_orchestrator::DefinitionFormatId;

use tokeira_platform::definition::DefinitionFrontend;

use crate::{
    apply, definition, deploy, describe, destroy, engine::Engine, lock, plan, platform::Admitted,
    revert, rollback, scale, upgrade,
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
    /// Read-only report of identity, recorded provenance, binding verdict, and
    /// state facts. Never gates.
    Describe(DescribeArgs),
    /// The deployment definition — the interpreted `.tkd`.
    #[command(subcommand)]
    Definition(DefinitionCommand),
    /// Server configuration — the rendered document the platform seeds for
    /// the deployment.
    #[command(subcommand)]
    Config(ConfigCommand),
    /// Substrate — the infrastructure the deployment stands on. Namespaced to
    /// mirror `tkr` so forwarding is a transparent pass-through.
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
    /// Stream logs for one logical service.
    Logs(LogsArgs),
    /// Print live published port mappings for one logical service.
    PortMappings(ServiceArgs),
    /// Revert to a prior config revision — a same-engine apply, gated on the binding.
    Revert(RevertArgs),
    /// Upgrade to a new engine identity.
    Upgrade(LifecycleArgs),
    /// Roll back to the retained prior configuration revision.
    Rollback(RollbackArgs),
}

impl Command {
    /// Deployment identity admission precedes even the operation lock. A
    /// standalone authored source has no deployment metadata to admit.
    fn admission_dir(&self) -> Option<&std::path::Path> {
        match self {
            Self::Upgrade(args) => Some(&args.deployment_dir),
            Self::Config(ConfigCommand::Seed(args)) => Some(&args.deployment_dir),
            Self::Describe(args) => Some(&args.deployment_dir),
            Self::Definition(DefinitionCommand::Check(args)) => args
                .definition
                .is_none()
                .then_some(args.deployment_dir.as_path()),
            Self::Infra(InfraCommand::Plan(args)) | Self::Deploy(DeployCommand::Plan(args)) => {
                Some(&args.deployment_dir)
            }
            Self::Infra(InfraCommand::Apply(args)) | Self::Deploy(DeployCommand::Apply(args)) => {
                Some(&args.deployment_dir)
            }
            Self::Infra(InfraCommand::Destroy(args)) => Some(&args.deployment_dir),
            Self::Scale(args) => Some(&args.deployment_dir),
            Self::Logs(args) => Some(&args.deployment_dir),
            Self::PortMappings(args) => Some(&args.deployment_dir),
            Self::Revert(args) => Some(&args.deployment_dir),
            Self::Rollback(args) => Some(&args.deployment_dir),
        }
    }
}

#[derive(Subcommand)]
enum ConfigCommand {
    /// Seed the deployment's server configuration: the config-document render
    /// with provider-derived fields left blank. Internal — invoked by `tkr
    /// deployment create` against the staging directory before the deployment
    /// is published, so it is hidden from help.
    #[command(hide = true)]
    Seed(LifecycleArgs),
}

#[derive(Args)]
struct LogsArgs {
    /// Logical service name owned by the platform.
    service: String,
    /// Continue streaming new output when supported.
    #[arg(long)]
    follow: bool,
    /// Number of recent lines requested from the provider.
    #[arg(long)]
    tail: Option<u32>,
    /// Deployment directory holding platform state.
    #[arg(long)]
    deployment_dir: PathBuf,
}

#[derive(Args)]
struct ServiceArgs {
    /// Logical service name owned by the platform.
    service: String,
    /// Deployment directory holding platform state.
    #[arg(long)]
    deployment_dir: PathBuf,
}

#[derive(Args)]
struct RollbackArgs {
    /// Deployment directory holding the state envelope.
    #[arg(long)]
    deployment_dir: PathBuf,
    /// Two-binary orchestration: stop after B's delete-only pass
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
    /// Definition format selected through the trusted frontend catalog.
    /// Required with `--definition`; deployment mode reads the recorded value.
    #[arg(long, requires = "definition")]
    format: Option<DefinitionFormatId>,
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
    /// Restrict the operation to one module and what it stands on (infra
    /// verbs only; the platform expands prerequisites, and destroy expands
    /// dependants instead).
    #[arg(long)]
    module: Option<String>,
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
    /// Restrict the operation to one module and what it stands on (infra
    /// verbs only; the platform expands prerequisites).
    #[arg(long)]
    module: Option<String>,
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
    /// Restrict the teardown to one module and everything standing on it
    /// (the platform expands dependants, never prerequisites).
    #[arg(long)]
    module: Option<String>,
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

/// Parse the CLI and run the selected verb over the bound engine. This is
/// the per-platform binary's entire `main`.
///
/// The exit code is part of the output contract: a verb refused by a
/// platform issue has already emitted the document that says everything, so
/// that refusal becomes a bare non-zero exit — never an error line
/// restating the report. Every other error propagates for the binary's
/// error reporting.
pub async fn run<F: DefinitionFrontend>(engine: Engine<F>) -> Result<std::process::ExitCode> {
    let cli = Cli::parse();
    // Admission once per command, at this boundary: identity is never
    // re-derived, metadata never re-read, the executable never re-verified
    // between the verbs one command drives. The authoring-mode definition
    // check is the one admission-free path — no deployment exists to admit.
    let admitted = match cli.command.admission_dir() {
        Some(dir) => Some(engine.platform().admit_deployment(dir)?),
        None => None,
    };
    let admitted = admitted.as_ref();
    // One resolution of the output contract's global flags; the collapse rule
    // (`--json` is depth-blind) is enforced inside `Mode::resolve`.
    let mode = tokeira_report::Mode::resolve(cli.json, cli.detail);
    let outcome: Result<()> = match cli.command {
        // Read-only: never gates, never locks.
        Command::Describe(_) => {
            describe::describe(&engine, require(admitted), cli.json, cli.detail).await
        }
        Command::Definition(DefinitionCommand::Check(args)) => {
            definition::check(
                &engine,
                admitted,
                args.definition.as_deref(),
                args.format.as_ref(),
                mode,
            )
            .await
        }
        Command::Config(ConfigCommand::Seed(_)) => crate::config_seed::seed(require(admitted)),
        Command::Logs(args) => {
            let Some(ops) = engine.platform().ops() else {
                anyhow::bail!("not applicable: this platform declares no ops surface");
            };
            let mut stream = ops
                .log_stream(
                    &require(admitted).deployment_ref,
                    &args.service,
                    args.follow,
                    args.tail,
                )
                .await?;
            while let Some(line) = stream.next().await {
                println!("{}", line?);
            }
            Ok(())
        }
        Command::PortMappings(args) => {
            let Some(ops) = engine.platform().ops() else {
                anyhow::bail!("not applicable: this platform declares no ops surface");
            };
            let mappings = ops
                .port_mappings(&require(admitted).deployment_ref, &args.service)
                .await?;
            if mappings.is_empty() {
                println!("no port mappings for service {}", args.service);
            } else {
                for mapping in mappings {
                    println!(
                        "{}:{} -> {}:{}/{}",
                        mapping.host_addr,
                        mapping.host_port,
                        args.service,
                        mapping.container_port,
                        mapping.protocol
                    );
                }
            }
            Ok(())
        }
        Command::Infra(InfraCommand::Plan(args)) => {
            plan::plan(
                &engine,
                require(admitted),
                args.module.as_deref(),
                mode,
                args.explanation.as_deref(),
            )
            .await
        }
        Command::Deploy(DeployCommand::Plan(args)) => {
            // Refused, never silently dropped: the workload verbs take no
            // module filter — that is the infra verbs' vocabulary.
            if args.module.is_some() {
                anyhow::bail!("`deploy plan` takes no `--module`; module filters are infra verbs");
            }
            deploy::deploy_plan(
                &engine,
                require(admitted),
                mode,
                args.explanation.as_deref(),
            )
            .await
        }
        // Mutating verbs run under the deployment's operation lock.
        // `rollback` holds one continuous lock across its whole sequence.
        Command::Infra(InfraCommand::Apply(args)) => {
            let admitted = require(admitted);
            let yes = args.yes;
            let module = args.module;
            let explanation = args.explanation;
            lock::with_operation_lock(&admitted.deployment_ref.dir, "apply", || {
                apply::apply(
                    &engine,
                    admitted,
                    module.as_deref(),
                    yes,
                    mode,
                    explanation.as_deref(),
                )
            })
            .await
        }
        Command::Infra(InfraCommand::Destroy(args)) => {
            let admitted = require(admitted);
            let yes = args.yes;
            let module = args.module;
            lock::with_operation_lock(&admitted.deployment_ref.dir, "destroy", || {
                destroy::destroy(&engine, admitted, module.as_deref(), yes)
            })
            .await
        }
        Command::Deploy(DeployCommand::Apply(args)) => {
            // Refused, never silently dropped — as with `deploy plan`.
            if args.module.is_some() {
                anyhow::bail!("`deploy apply` takes no `--module`; module filters are infra verbs");
            }
            let admitted = require(admitted);
            let yes = args.yes;
            let explanation = args.explanation;
            lock::with_operation_lock(&admitted.deployment_ref.dir, "deploy-apply", || {
                deploy::deploy_apply(&engine, admitted, yes, mode, explanation.as_deref())
            })
            .await
        }
        Command::Scale(args) => {
            let admitted = require(admitted);
            let specs = args.specs;
            lock::with_operation_lock(&admitted.deployment_ref.dir, "scale", || {
                scale::scale(&engine, admitted, &specs)
            })
            .await
        }
        Command::Revert(args) => {
            let admitted = require(admitted);
            let to = args.to;
            lock::with_operation_lock(&admitted.deployment_ref.dir, "revert", || {
                revert::revert(&engine, admitted, to)
            })
            .await
        }
        Command::Upgrade(_) => {
            let admitted = require(admitted);
            lock::with_operation_lock(&admitted.deployment_ref.dir, "upgrade", || {
                upgrade::upgrade(&engine, admitted, mode)
            })
            .await
        }
        Command::Rollback(args) => {
            let admitted = require(admitted);
            let handoff = args.handoff;
            lock::with_operation_lock(&admitted.deployment_ref.dir, "rollback", || {
                rollback::rollback(&engine, admitted, handoff)
            })
            .await
        }
    };
    exit_status(outcome)
}

/// The admission invariant, stated once: every deployment verb's
/// `admission_dir` arm covers it, so absence here is a programming error,
/// never an operator input.
fn require(admitted: Option<&Admitted>) -> &Admitted {
    admitted.expect("admission precedes every deployment verb")
}

/// Collapse a typed post-report failure to a bare process status. Every other
/// failure remains an error so the entrypoint can render it once.
pub(crate) fn exit_status(outcome: Result<()>) -> Result<std::process::ExitCode> {
    match outcome {
        Ok(()) => Ok(std::process::ExitCode::SUCCESS),
        Err(err) => match err.downcast::<crate::ReportEmitted>() {
            Ok(_) => Ok(std::process::ExitCode::FAILURE),
            Err(err) => Err(err),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_is_not_a_provisioner_command() {
        let Err(error) = Cli::try_parse_from(["tkp", "init", "--deployment-dir", "/tmp/d"]) else {
            panic!("creation has no provisioner inception verb");
        };
        assert_eq!(error.kind(), clap::error::ErrorKind::InvalidSubcommand);
    }
}
