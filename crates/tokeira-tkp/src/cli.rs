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
use clap::{Args, Parser, Subcommand, ValueEnum};
use futures_util::StreamExt;
use tokeira_orchestrator::DefinitionFormatId;

use tokeira_platform::{definition::DefinitionFrontend, ops::PortForwardOutcome};

use crate::{
    apply, definition, deploy, describe, destroy, engine::Engine, lock, observability, plan,
    platform::Admitted, revert, rollback, scale, upgrade,
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
    /// Container images declared and published by this platform.
    #[command(subcommand)]
    Image(ImageCommand),
    /// Tear down workloads and then infrastructure. The owning `tkr`
    /// removes deployment records only after this command succeeds.
    Destroy(DeploymentDestroyArgs),
    /// Change workload capacity (`<dim>=<n>` specs); a config revision + a
    /// workload apply. `NotApplicable` where the platform has no scale dimension.
    Scale(ScaleArgs),
    /// Stream logs for one logical service.
    Logs(LogsArgs),
    /// Print live published port mappings for one logical service.
    PortMappings(ServiceArgs),
    /// Reach one logical service using the bound platform's forwarding mode.
    PortForward(PortForwardArgs),
    /// Execute an interactive command in one live service container.
    Exec(ExecArgs),
    /// Run one command through the platform's on-demand admin workload.
    Admin(AdminArgs),
    /// Validate the deployment's realized observability configuration.
    #[command(subcommand)]
    Observability(ObservabilityCommand),
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
            Self::Deploy(DeployCommand::Destroy(args)) => Some(&args.deployment_dir),
            Self::Image(ImageCommand::List(args)) => Some(&args.deployment_dir),
            Self::Image(ImageCommand::Push(args)) => Some(&args.deployment_dir),
            Self::Image(ImageCommand::Mirror(args)) => Some(&args.deployment_dir),
            Self::Destroy(args) => Some(&args.deployment_dir),
            Self::Scale(args) => Some(&args.deployment_dir),
            Self::Logs(args) => Some(&args.deployment_dir),
            Self::PortMappings(args) => Some(&args.deployment_dir),
            Self::PortForward(args) => Some(&args.deployment_dir),
            Self::Exec(args) => Some(&args.deployment_dir),
            Self::Admin(args) => Some(&args.deployment_dir),
            Self::Observability(ObservabilityCommand::Check(args)) => Some(&args.deployment_dir),
            Self::Revert(args) => Some(&args.deployment_dir),
            Self::Rollback(args) => Some(&args.deployment_dir),
        }
    }
}

#[derive(Subcommand)]
enum ImageCommand {
    /// List the images declared by the bound platform.
    List(ImageListArgs),
    /// Publish a locally built image to the platform registry.
    Push(ImagePushArgs),
    /// Mirror authored upstream images into the platform registry.
    Mirror(ImageMirrorArgs),
}

#[derive(Args)]
struct ImageListArgs {
    /// Deployment directory holding the definition and runtime state.
    #[arg(long)]
    deployment_dir: PathBuf,
    /// Limit the inventory to one source class.
    #[arg(long)]
    source_type: Option<ImageSourceFilter>,
}

#[derive(Args)]
struct ImagePushArgs {
    /// Deployment directory holding the definition and runtime state.
    #[arg(long)]
    deployment_dir: PathBuf,
    /// Publish only this logical build image.
    #[arg(long)]
    image: Option<String>,
    /// Additional deployment tag; `latest` is always published.
    #[arg(long, default_value = "latest")]
    tag: String,
    /// Confirm ECR mutation.
    #[arg(long)]
    yes: bool,
}

#[derive(Args)]
struct ImageMirrorArgs {
    /// Deployment directory holding the definition and runtime state.
    #[arg(long)]
    deployment_dir: PathBuf,
    /// Mirror only this logical upstream image.
    #[arg(long)]
    image: Option<String>,
    /// Confirm ECR mutation.
    #[arg(long)]
    yes: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ImageSourceFilter {
    Build,
    Mirror,
    Registry,
}

impl From<ImageSourceFilter> for tokeira_deploy_engine::ImageSourceType {
    fn from(value: ImageSourceFilter) -> Self {
        match value {
            ImageSourceFilter::Build => Self::Build,
            ImageSourceFilter::Mirror => Self::Mirror,
            ImageSourceFilter::Registry => Self::Registry,
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

#[derive(Subcommand)]
enum ObservabilityCommand {
    /// Run the platform-declared observability checks. Read-only; never gates
    /// or acquires the operation lock.
    Check(ObservabilityCheckArgs),
}

#[derive(Args)]
struct ObservabilityCheckArgs {
    /// Deployment directory holding the definition and its companion content.
    #[arg(long)]
    deployment_dir: PathBuf,
    /// Maximum duration available to platform-defined read-only reachability
    /// checks. A platform with only static checks may ignore it.
    #[arg(long, default_value = "30")]
    timeout_seconds: u64,
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
struct PortForwardArgs {
    /// Logical service name owned by the platform.
    service: String,
    /// Local port to bind when the platform opens a tunnel.
    #[arg(long)]
    local_port: Option<u16>,
    /// Deployment directory holding platform state.
    #[arg(long)]
    deployment_dir: PathBuf,
}

#[derive(Args)]
struct ExecArgs {
    /// Logical service name owned by the platform.
    service: String,
    /// Container name; the platform defaults to the service's primary container.
    #[arg(long)]
    container: Option<String>,
    /// Command and arguments to execute remotely.
    #[arg(last = true, required = true)]
    command: Vec<String>,
    /// Deployment directory holding platform identity and the admitted definition.
    #[arg(long)]
    deployment_dir: PathBuf,
}

#[derive(Args)]
struct AdminArgs {
    /// Command and arguments passed to the platform's admin workload.
    #[arg(last = true, required = true)]
    command: Vec<String>,
    /// Deployment directory holding platform identity and the admitted definition.
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
    /// Tear down every workload in reverse dependency order. Irreversible.
    Destroy(DeployDestroyArgs),
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
struct DeployDestroyArgs {
    /// Deployment directory holding the state envelope.
    #[arg(long)]
    deployment_dir: PathBuf,
    /// Confirm the irreversible workload teardown (required).
    #[arg(long)]
    yes: bool,
}

#[derive(Args)]
struct DeploymentDestroyArgs {
    /// Deployment directory retained until both live planes are empty.
    #[arg(long)]
    deployment_dir: PathBuf,
    /// Confirm the complete workload and infrastructure teardown (required).
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
        Some(dir) => Some(engine.platform().admit_deployment(dir).await?),
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
        Command::Image(ImageCommand::List(args)) => crate::image::list(
            &engine,
            require(admitted),
            args.source_type.map(Into::into),
            cli.json,
        ),
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
        Command::PortForward(args) => {
            let Some(ops) = engine.platform().ops() else {
                anyhow::bail!("not applicable: this platform declares no ops surface");
            };
            match ops
                .port_forward(
                    &require(admitted).deployment_ref,
                    &args.service,
                    args.local_port,
                )
                .await?
            {
                PortForwardOutcome::Mappings(mappings) if mappings.is_empty() => {
                    println!("no port mappings for service {}", args.service);
                }
                PortForwardOutcome::Mappings(mappings) => {
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
                PortForwardOutcome::SessionClosed => {}
            }
            Ok(())
        }
        Command::Exec(args) => {
            let Some(ops) = engine.platform().ops() else {
                anyhow::bail!("not applicable: this platform declares no ops surface");
            };
            ops.exec(
                &require(admitted).deployment_ref,
                &args.service,
                args.container.as_deref(),
                &args.command,
            )
            .await
        }
        Command::Observability(ObservabilityCommand::Check(args)) => {
            observability::check(&engine, require(admitted), args.timeout_seconds)
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
            lock::with_operation_lock(&admitted.state, "apply", || {
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
            lock::with_operation_lock(&admitted.state, "destroy", || {
                destroy::destroy(&engine, admitted, module.as_deref(), yes)
            })
            .await
        }
        Command::Deploy(DeployCommand::Destroy(args)) => {
            let admitted = require(admitted);
            let yes = args.yes;
            lock::with_operation_lock(&admitted.state, "deploy-destroy", || {
                deploy::deploy_destroy(&engine, admitted, yes, mode)
            })
            .await
        }
        Command::Destroy(args) => {
            let admitted = require(admitted);
            let yes = args.yes;
            lock::with_operation_lock(&admitted.state, "deployment-destroy", || {
                destroy::destroy_deployment(&engine, admitted, yes, mode)
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
            lock::with_operation_lock(&admitted.state, "deploy-apply", || {
                deploy::deploy_apply(&engine, admitted, yes, mode, explanation.as_deref())
            })
            .await
        }
        Command::Image(ImageCommand::Push(args)) => {
            if !args.yes {
                anyhow::bail!("image push changes ECR; re-run with `--yes`");
            }
            let admitted = require(admitted);
            let image = args.image;
            let tag = args.tag;
            lock::with_operation_lock(&admitted.state, "image-push", || {
                crate::image::push(&engine, admitted, image.as_deref(), &tag, cli.json)
            })
            .await
        }
        Command::Image(ImageCommand::Mirror(args)) => {
            if !args.yes {
                anyhow::bail!("image mirror changes ECR; re-run with `--yes`");
            }
            let admitted = require(admitted);
            let image = args.image;
            lock::with_operation_lock(&admitted.state, "image-mirror", || {
                crate::image::mirror(&engine, admitted, image.as_deref(), cli.json)
            })
            .await
        }
        Command::Scale(args) => {
            let admitted = require(admitted);
            let specs = args.specs;
            lock::with_operation_lock(&admitted.state, "scale", || {
                scale::scale(&engine, admitted, &specs)
            })
            .await
        }
        Command::Admin(args) => {
            let admitted = require(admitted);
            let command = args.command;
            lock::with_operation_lock(&admitted.state, "admin", || async {
                let Some(ops) = engine.platform().ops() else {
                    anyhow::bail!("not applicable: this platform declares no ops surface");
                };
                ops.admin(&admitted.deployment_ref, &command).await
            })
            .await
        }
        Command::Revert(args) => {
            let admitted = require(admitted);
            let to = args.to;
            lock::with_operation_lock(&admitted.state, "revert", || {
                revert::revert(&engine, admitted, to)
            })
            .await
        }
        Command::Upgrade(_) => {
            let admitted = require(admitted);
            lock::with_operation_lock(&admitted.state, "upgrade", || {
                upgrade::upgrade(&engine, admitted, mode)
            })
            .await
        }
        Command::Rollback(args) => {
            let admitted = require(admitted);
            let handoff = args.handoff;
            lock::with_operation_lock(&admitted.state, "rollback", || {
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

    #[test]
    fn parses_granular_and_complete_destroy_commands() {
        let deploy = Cli::try_parse_from([
            "tkp",
            "deploy",
            "destroy",
            "--deployment-dir",
            "/tmp/d",
            "--yes",
        ])
        .unwrap();
        assert!(matches!(
            deploy.command,
            Command::Deploy(DeployCommand::Destroy(DeployDestroyArgs { yes: true, .. }))
        ));

        let complete =
            Cli::try_parse_from(["tkp", "destroy", "--deployment-dir", "/tmp/d", "--yes"]).unwrap();
        assert!(matches!(
            complete.command,
            Command::Destroy(DeploymentDestroyArgs { yes: true, .. })
        ));
    }

    #[test]
    fn parses_read_only_observability_check() {
        let parsed = Cli::try_parse_from([
            "tkp",
            "observability",
            "check",
            "--deployment-dir",
            "/tmp/d",
            "--timeout-seconds",
            "15",
        ])
        .unwrap();
        assert!(matches!(
            parsed.command,
            Command::Observability(ObservabilityCommand::Check(ObservabilityCheckArgs {
                timeout_seconds: 15,
                ..
            }))
        ));
    }

    #[test]
    fn parses_definition_bound_image_commands() {
        let list = Cli::try_parse_from([
            "tkp",
            "image",
            "list",
            "--deployment-dir",
            "/tmp/d",
            "--source-type",
            "mirror",
        ])
        .unwrap();
        assert!(matches!(
            list.command,
            Command::Image(ImageCommand::List(ImageListArgs {
                source_type: Some(ImageSourceFilter::Mirror),
                ..
            }))
        ));

        assert!(
            Cli::try_parse_from([
                "tkp",
                "image",
                "push",
                "--deployment-dir",
                "/tmp/d",
                "--tag",
                "v1",
                "--yes",
            ])
            .is_ok()
        );
        assert!(
            Cli::try_parse_from([
                "tkp",
                "image",
                "mirror",
                "--deployment-dir",
                "/tmp/d",
                "--image",
                "grafana",
                "--yes",
            ])
            .is_ok()
        );
    }

    #[test]
    fn parses_platform_owned_port_forward() {
        let parsed = Cli::try_parse_from([
            "tkp",
            "port-forward",
            "grafana",
            "--local-port",
            "33000",
            "--deployment-dir",
            "/tmp/d",
        ])
        .unwrap();
        assert!(matches!(
            parsed.command,
            Command::PortForward(PortForwardArgs {
                service,
                local_port: Some(33000),
                ..
            }) if service == "grafana"
        ));
    }

    #[test]
    fn parses_platform_owned_exec() {
        let parsed = Cli::try_parse_from([
            "tkp",
            "exec",
            "--deployment-dir",
            "/tmp/d",
            "runtime",
            "--container",
            "tokeira-runtime",
            "--",
            "sh",
            "-c",
            "echo ready",
        ])
        .unwrap();
        assert!(matches!(
            parsed.command,
            Command::Exec(ExecArgs {
                service,
                container: Some(container),
                command,
                ..
            }) if service == "runtime"
                && container == "tokeira-runtime"
                && command == ["sh", "-c", "echo ready"]
        ));
        assert!(
            Cli::try_parse_from(["tkp", "exec", "runtime", "--deployment-dir", "/tmp/d"]).is_err(),
            "the remote command is mandatory"
        );
    }

    #[test]
    fn parses_platform_owned_admin() {
        let parsed = Cli::try_parse_from([
            "tkp",
            "admin",
            "--deployment-dir",
            "/tmp/d",
            "--",
            "schema",
            "migrate",
            "--target",
            "5",
        ])
        .unwrap();
        assert!(matches!(
            parsed.command,
            Command::Admin(AdminArgs { command, .. })
                if command == ["schema", "migrate", "--target", "5"]
        ));
        assert!(
            Cli::try_parse_from(["tkp", "admin", "--deployment-dir", "/tmp/d"]).is_err(),
            "the admin command is mandatory"
        );
    }
}
