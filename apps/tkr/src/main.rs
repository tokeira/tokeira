//! `tkr` — the Tokeira operator CLI.
//!
//! `tkr` is a single binary that drives every stage of a Tokeira deployment
//! against any supported platform (local process, Docker Compose, AWS ECS).
//! It replaces Terraform, kubectl, and shell scripts with one `plan → confirm
//! → apply` workflow per concern.
//!
//! # Command map
//!
//! | Concern           | Command          | What it does                                   |
//! |-------------------|------------------|------------------------------------------------|
//! | Workspace dev     | `tkr dev ...`    | Thin shims over `cargo build/test/clippy/fmt`. |
//! | Deployment CRUD   | `tkr deployment` | Create/list/use/destroy named deployments.     |
//! | Container images  | `tkr image`      | Build, push, and mirror runtime images.        |
//! | Release train     | `tkr release`    | Plan, apply, resume, and verify crate releases.|
//! | Cloud infra       | `tkr infra`      | Plan / apply / destroy declared resources.     |
//! | Service deploy    | `tkr deploy`     | Plan / apply / destroy service manifests.      |
//! | DSQL schema       | `tkr schema`     | Schema setup for DSQL-backed deployments.      |
//! | Scaling + ops     | `tkr scale`, `tkr logs`, `tkr port-forward`, `tkr exec`, `tkr admin` | Day-2 operator loops. |
//! | Inspection        | `tkr config show`, `tkr version`            | Debugging aids.       |
//!
//! # Architecture pointers
//!
//! - **`cli`** defines the clap surface. Adding a subcommand starts here.
//! - **`deployment_dir`** owns how deployments are resolved, loaded, and persisted.
//! - **`commands`** contains one module per top-level subcommand. Definition-bound
//!   deployments, including ECS deployments, execute through their married
//!   provisioner; Local retains the in-process handlers.
//! - **`tui`** wires engine progress events to spinners (human mode) or JSON
//!   lines (`--json` mode).
//! - **`platform_discovery`** resolves platform/front-end source used to create bound deployments.
//!
//! # Working assumptions
//!
//! - All deployments live under a platform-specific state dir
//!   (typically `~/Library/Application Support/tokeira/tkr/` on macOS);
//!   see `DeploymentResolver::default` for the lookup rules.
//! - The selected deployment is tracked in a `.latest` sentinel file so
//!   operators can omit `--deployment` for the happy path.
//! - Destructive operations require `--yes` (see [`commands::require_confirmation`]).

// CLI: stdout/stderr are the user interface.
#![allow(clippy::print_stdout, clippy::print_stderr)]
use anyhow::Result;
use clap::Parser;

mod bundle_create;
mod cli;
mod commands;
mod definition_check;
mod deployment_dir;
mod deployment_lock;
mod launcher;
mod legacy;
mod metadata;
mod output;
pub mod platform_discovery;
mod process;
mod repository_setup;
mod tui;

use cli::{
    Cli, Command, ConfigAction, DeployAction, DeploymentAction, InfraAction, ObservabilityAction,
    ScaleAction, SchemaAction,
};
use deployment_dir::{DeploymentResolver, load_context, load_record_context};

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    let json = cli.json;
    match run(cli).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            let exit_code = error
                .downcast_ref::<tokeira_build::ReleaseError>()
                .map_or(1, tokeira_build::ReleaseError::exit_code);
            output::render_refusal(&error, json);
            std::process::ExitCode::from(exit_code)
        }
    }
}

async fn run(cli: Cli) -> Result<()> {
    let deployments = DeploymentResolver::default()?;

    // Deployment lock (mis-apply guard, task 10.2): before dispatching, a mutating
    // command must target the locked deployment (if any). Read-only commands and
    // registry/global commands are never blocked. Fail closed on a vanished lock
    // or a changed identity fingerprint. When the lock validated a target, that
    // *pinned* name is what dispatch uses — the handler must not independently
    // re-read the `.latest` sentinel (which could change between the check and
    // the mutation).
    let mut selected = cli.deployment.clone();
    if let Some(target) = mutation_target(&cli.command, selected.as_deref())
        && let Some(pinned) = deployment_lock::enforce_mutation(&deployments, target.as_deref())?
    {
        selected = Some(pinned);
    }
    let selected = selected.as_deref();

    // Each arm resolves the deployment context it needs before dispatching.
    // Image building operates on workspace sources and needs no deployment.
    match cli.command {
        Command::Dev { action } => commands::dev::run(action),
        Command::Deployment { action } => {
            commands::deployment::run(action, &deployments, selected, cli.json, cli.detail).await
        }
        Command::Image(args) => {
            let format = if cli.json {
                tui::OutputFormat::Json
            } else {
                tui::OutputFormat::Human
            };
            match args.command {
                cli::ImageCommand::Build { arch, tag } => {
                    commands::image::run(cli::ImageCommand::Build { arch, tag }, format).await
                }
                command => {
                    if !deployments.uses_bound_provisioner(selected)? {
                        anyhow::bail!(
                            "the selected deployment has no definition-bound image lifecycle"
                        );
                    }
                    let dir = deployments.resolve_dir(selected)?;
                    let (verb, mut extra) = forwarded_image_verb(&command);
                    extra.extend(output_flags(cli.json, cli.detail));
                    launcher::launch(&dir, verb, &extra).await
                }
            }
        }
        Command::Definition { action } => {
            let cli::DefinitionAction::Check { definition, format } = action;
            if let Some(path) = definition {
                // Authoring mode: the definition needs no deployment. One
                // subject per check — a named path and a named deployment
                // cannot both be it.
                if selected.is_some() {
                    anyhow::bail!("pass either `--definition` or `--deployment`, not both");
                }
                // The syntax tier: in-process, against the frontends linked
                // into `tkr`. Engine interpretation belongs to the
                // deployment-bound arm below.
                definition_check::check_at_path(&path, format.as_ref(), cli.json)
            } else if deployments.uses_bound_provisioner(selected)? {
                let dir = deployments.resolve_dir(selected)?;
                launcher::launch(
                    &dir,
                    &["definition", "check"],
                    &output_flags(cli.json, cli.detail),
                )
                .await
            } else {
                // In-process platforms are configured, not defined: there is
                // no interpreted definition to check.
                let name = deployments.resolve_name(selected)?;
                anyhow::bail!(
                    "deployment '{name}' is configured by `deployment.toml`; there is no \
                     interpreted definition to check"
                );
            }
        }
        Command::Infra { action } => {
            // A definition deployment is forwarded to its bound `tkp`; only
            // Local runs through `commands::infra`.
            if deployments.uses_bound_provisioner(selected)? {
                let dir = deployments.resolve_dir(selected)?;
                // `infra apply` is the same forwarded operation as
                // `deployment apply` (Req 6.5); creation has already committed
                // the binding and initial revision.
                if let InfraAction::Apply {
                    yes,
                    explanation,
                    module,
                    ..
                } = &action
                {
                    launcher::launch_apply(&dir, *yes, module.as_deref(), explanation.as_deref())
                        .await
                } else {
                    let (verb, mut extra) = forwarded_infra_verb(&action);
                    // The output contract's global flags travel with the
                    // read-only verbs, so the operator cannot tell which
                    // binary rendered the report.
                    if matches!(action, InfraAction::Plan { .. } | InfraAction::Status) {
                        extra.extend(output_flags(cli.json, cli.detail));
                    }
                    launcher::launch(&dir, verb, &extra).await
                }
            } else {
                let ctx = load_context(&deployments, selected)?;
                let format = if cli.json {
                    tui::OutputFormat::Json
                } else {
                    tui::OutputFormat::Human
                };
                commands::infra::run(action, &deployments, ctx, format).await
            }
        }
        Command::Deploy { action } => {
            if deployments.uses_bound_provisioner(selected)? {
                let dir = deployments.resolve_dir(selected)?;
                let (verb, mut extra) = forwarded_deploy_verb(&action);
                // Both workload verbs now produce reports, so their global
                // output mode crosses the provisioner boundary unchanged.
                extra.extend(output_flags(cli.json, cli.detail));
                launcher::launch(&dir, verb, &extra).await
            } else {
                let ctx = load_context(&deployments, selected)?;
                commands::deploy::run(action, &deployments, ctx).await
            }
        }
        Command::Schema { action } => {
            let ctx = load_record_context(&deployments, selected)?;
            commands::schema::run(action, ctx).await
        }
        Command::Scale { action } => {
            if deployments.uses_bound_provisioner(selected)? {
                let dir = deployments.resolve_dir(selected)?;
                match forwarded_scale_verb(&action) {
                    Some(specs) => launcher::launch(&dir, &["scale"], &specs).await,
                    None => {
                        launcher::launch(&dir, &["describe"], &output_flags(cli.json, cli.detail))
                            .await
                    }
                }
            } else {
                let ctx = load_context(&deployments, selected)?;
                commands::scale::run(action, &deployments, ctx).await
            }
        }
        Command::Logs {
            service,
            follow,
            tail,
        } => {
            if deployments.uses_bound_provisioner(selected)? {
                let dir = deployments.resolve_dir(selected)?;
                let mut extra = vec![service];
                if follow {
                    extra.push("--follow".to_string());
                }
                if let Some(tail) = tail {
                    extra.push("--tail".to_string());
                    extra.push(tail.to_string());
                }
                launcher::launch(&dir, &["logs"], &extra).await
            } else {
                let ctx = load_context(&deployments, selected)?;
                commands::logs::run(&service, follow, tail, ctx).await
            }
        }
        Command::PortForward {
            service,
            local_port,
        } => {
            if deployments.uses_bound_provisioner(selected)? {
                let dir = deployments.resolve_dir(selected)?;
                let mut extra = vec![service];
                if let Some(local_port) = local_port {
                    extra.push("--local-port".to_owned());
                    extra.push(local_port.to_string());
                }
                launcher::launch(&dir, &["port-forward"], &extra).await
            } else {
                let ctx = load_context(&deployments, selected)?;
                commands::port_forward::run(&service, local_port, ctx).await
            }
        }
        Command::Exec {
            service,
            container,
            command,
        } => {
            if !deployments.uses_bound_provisioner(selected)? {
                anyhow::bail!(
                    "interactive exec is not supported by the selected in-process platform"
                );
            }
            let dir = deployments.resolve_dir(selected)?;
            launcher::launch(
                &dir,
                &["exec"],
                &forwarded_exec_args(&service, container.as_deref(), &command),
            )
            .await
        }
        Command::Admin { command } => {
            if !deployments.uses_bound_provisioner(selected)? {
                anyhow::bail!(
                    "on-demand administration is not supported by the selected in-process platform"
                );
            }
            let dir = deployments.resolve_dir(selected)?;
            launcher::launch(&dir, &["admin"], &forwarded_admin_args(&command)).await
        }
        Command::Config {
            action: ConfigAction::Show,
        } => commands::config::run_show(&deployments, selected),
        Command::Compat(args) => commands::compat::run(args.command, cli.json),
        Command::Ci(args) => commands::ci::run(args.command, cli.json).await,
        Command::Release(args) => commands::release::run(args.command, cli.json).await,
        Command::Observability { action } => {
            let ObservabilityAction::Check {
                path,
                grafana,
                timeout_seconds,
            } = action;
            if grafana {
                if selected.is_some() {
                    anyhow::bail!("pass either `--path` or `--deployment`, not both");
                }
                let path = path.ok_or_else(|| anyhow::anyhow!("`--grafana` requires `--path`"))?;
                commands::observability::run_grafana(&path)
            } else if deployments.uses_bound_provisioner(selected)? {
                let dir = deployments.resolve_dir(selected)?;
                let (verb, extra) = forwarded_observability_verb(timeout_seconds);
                launcher::launch(&dir, verb, &extra).await
            } else {
                let ctx = load_context(&deployments, selected)?;
                commands::observability::run_selected(timeout_seconds, ctx)
            }
        }
        Command::Diagnostics { action } => {
            let ctx = load_record_context(&deployments, selected)?;
            commands::diagnostics::run(action, ctx, cli.json).await
        }
        Command::Version { verbose, json } => {
            commands::version::run(verbose, cli.json || json);
            Ok(())
        }
    }
}

/// Classify a command for the deployment lock (task 10.2). Returns `Some(target)`
/// for a *mutating* deployment-scoped command — `target` is its resolved
/// deployment name, or `None` to fall back to the soft selection — and `None`
/// for read-only, registry, or global commands (which the lock never blocks).
///
/// The guarded set is the lifecycle mutations that change a deployment's
/// infrastructure/services or its registry entry: `infra apply|destroy`,
/// `deploy apply|destroy`, `image push|mirror`, `schema setup`, `scale
/// up|down`, `deployment destroy`, and the forwarded `deployment
/// apply|upgrade|rollback`. Plans, statuses, `describe`, `list`, `version`,
/// image builds, and registry/selection verbs (`create`/`use`/`lock`/`unlock`)
/// are never blocked.
fn mutation_target(command: &Command, selected: Option<&str>) -> Option<Option<String>> {
    let selected = || selected.map(str::to_string);
    match command {
        Command::Infra {
            action: InfraAction::Apply { .. } | InfraAction::Destroy { .. },
        }
        | Command::Deploy {
            action: DeployAction::Apply { .. } | DeployAction::Destroy { .. },
        }
        | Command::Schema {
            action: SchemaAction::Setup { .. },
        }
        | Command::Scale {
            action: ScaleAction::Up { .. } | ScaleAction::Down { .. },
        }
        | Command::Admin { .. }
        | Command::Image(cli::ImageArgs {
            command: cli::ImageCommand::Push { .. } | cli::ImageCommand::Mirror { .. },
        }) => Some(selected()),
        Command::Deployment { action } => match action {
            DeploymentAction::Destroy { name, .. } => Some(Some(name.clone())),
            DeploymentAction::Apply { .. }
            | DeploymentAction::Upgrade
            | DeploymentAction::Rollback => Some(selected()),
            _ => None,
        },
        _ => None,
    }
}

/// Map definition-bound image commands onto the married provisioner's
/// namespace. Build is deliberately absent: it remains deployment-independent
/// and runs in `tkr` against the current workspace source.
fn forwarded_image_verb(command: &cli::ImageCommand) -> (&'static [&'static str], Vec<String>) {
    match command {
        cli::ImageCommand::List { source_type } => {
            let extra = source_type.map_or_else(Vec::new, |source| {
                vec![
                    "--source-type".to_string(),
                    match source {
                        cli::CliImageSource::Build => "build",
                        cli::CliImageSource::Mirror => "mirror",
                        cli::CliImageSource::Registry => "registry",
                    }
                    .to_string(),
                ]
            });
            (&["image", "list"], extra)
        }
        cli::ImageCommand::Push { tag, image, yes } => {
            let mut extra = vec!["--tag".to_string(), tag.clone()];
            if let Some(image) = image {
                extra.extend(["--image".to_string(), image.clone()]);
            }
            if *yes {
                extra.push("--yes".to_string());
            }
            (&["image", "push"], extra)
        }
        cli::ImageCommand::Mirror { image, yes } => {
            let mut extra = Vec::new();
            if let Some(image) = image {
                extra.extend(["--image".to_string(), image.clone()]);
            }
            if *yes {
                extra.push("--yes".to_string());
            }
            (&["image", "mirror"], extra)
        }
        cli::ImageCommand::Build { .. } => {
            unreachable!("deployment-independent builds are handled in-process")
        }
    }
}

/// Map a `tkr infra` action to the forwarded `tkp` verb tokens — the same
/// namespaced words the operator typed (`tkr infra plan` → `tkp infra plan`),
/// so forwarding is a transparent pass-through (Req 7.3).
fn forwarded_infra_verb(action: &InfraAction) -> (&'static [&'static str], Vec<String>) {
    match action {
        InfraAction::Plan {
            explanation,
            module,
            ..
        } => {
            let mut extra = explanation_flag(explanation.as_deref());
            extra.extend(module_flag(module.as_deref()));
            (&["infra", "plan"], extra)
        }
        InfraAction::Apply { .. } => (&["infra", "apply"], Vec::new()),
        InfraAction::Destroy { yes, module, .. } => {
            let mut extra = if *yes {
                vec!["--yes".to_string()]
            } else {
                Vec::new()
            };
            extra.extend(module_flag(module.as_deref()));
            (&["infra", "destroy"], extra)
        }
        InfraAction::Status => (&["describe"], Vec::new()),
    }
}

/// Forward the read-only observability verb to the provisioner bound to the
/// admitted deployment. The timeout remains explicit at the boundary so both
/// shells preserve the same operator request even while live reachability is
/// reported as a warning.
fn forwarded_observability_verb(timeout_seconds: u64) -> (&'static [&'static str], Vec<String>) {
    (
        &["observability", "check"],
        vec!["--timeout-seconds".to_string(), timeout_seconds.to_string()],
    )
}

/// Preserve remote argv boundaries across the `tkr` → married `tkp` process
/// boundary. The separator prevents a remote flag from being interpreted as
/// a provisioner flag; the platform performs the final provider encoding.
fn forwarded_exec_args(service: &str, container: Option<&str>, command: &[String]) -> Vec<String> {
    let mut args = vec![service.to_owned()];
    if let Some(container) = container {
        args.extend(["--container".to_owned(), container.to_owned()]);
    }
    args.push("--".to_owned());
    args.extend(command.iter().cloned());
    args
}

/// Preserve the admin command as argv across the `tkr` → `tkp` boundary.
/// The separator keeps a command-local flag from becoming a provisioner flag.
fn forwarded_admin_args(command: &[String]) -> Vec<String> {
    let mut args = vec!["--".to_owned()];
    args.extend(command.iter().cloned());
    args
}

/// `--module` crosses to the bound `tkp` verbatim — the platform owns its
/// meaning; `tkr` no longer drops it silently on the forwarded path.
fn module_flag(module: Option<&str>) -> Vec<String> {
    match module {
        Some(name) => vec!["--module".to_string(), name.to_string()],
        None => Vec::new(),
    }
}

/// Map a `tkr deploy` action to the forwarded `tkp` verb tokens — the same
/// namespaced words (`tkr deploy plan` → `tkp deploy plan`). The platform
/// decides realization: the compose platform's workload rides its infra universe.
fn forwarded_deploy_verb(action: &DeployAction) -> (&'static [&'static str], Vec<String>) {
    match action {
        DeployAction::Plan { explanation } => (
            &["deploy", "plan"],
            explanation_flag(explanation.as_deref()),
        ),
        DeployAction::Apply {
            yes, explanation, ..
        } => {
            // `--yes` crosses the forwarding boundary: the destructive gate
            // lives in `tkp`, and the operator's confirmation must reach it —
            // dropping the flag left destructive workload applies with a
            // refusal and no remedy. `--force` stays in-process-only: `tkp`
            // has no such flag.
            let mut extra = Vec::new();
            if *yes {
                extra.push("--yes".to_string());
            }
            extra.extend(explanation_flag(explanation.as_deref()));
            (&["deploy", "apply"], extra)
        }
        DeployAction::Destroy { yes } => {
            let extra = if *yes {
                vec!["--yes".to_string()]
            } else {
                Vec::new()
            };
            (&["deploy", "destroy"], extra)
        }
        DeployAction::Status => (&["describe"], Vec::new()),
    }
}

/// Re-spell a requested explanation-artifact path for a forwarded `tkp`
/// invocation. The path crosses as given: `tkp` owns the write and the
/// failure report (operator-explanation Req 3.1).
fn explanation_flag(explanation: Option<&std::path::Path>) -> Vec<String> {
    explanation
        .map(|path| vec!["--explanation".to_string(), path.display().to_string()])
        .unwrap_or_default()
}

/// The operator output contract's global flags (`--json`, `--detail`),
/// re-spelled for a forwarded `tkp` invocation. Attached to read-only verbs so
/// the report renders identically whichever binary produced it; mutating verbs
/// join as their reports migrate onto the contract.
fn output_flags(json: bool, detail: bool) -> Vec<String> {
    let mut flags = Vec::new();
    if json {
        flags.push("--json".to_string());
    }
    if detail {
        flags.push("--detail".to_string());
    }
    flags
}

/// Map a `tkr scale` action to the forwarded `tkp scale` specs. The spec grammar
/// is platform-interpreted; both current platforms answer `NotApplicable`.
fn forwarded_scale_verb(action: &ScaleAction) -> Option<Vec<String>> {
    let spec = |direction: &str, service: &Option<String>, replicas: &Option<u32>| {
        let mut specs = vec![direction.to_string()];
        if let Some(service) = service {
            specs.push(service.clone());
        }
        if let Some(replicas) = replicas {
            specs.push(replicas.to_string());
        }
        specs
    };
    match action {
        ScaleAction::Up { service, replicas } => Some(spec("up", service, replicas)),
        ScaleAction::Down { service, replicas } => Some(spec("down", service, replicas)),
        // `scale status` is a read; the deployment describe covers it.
        ScaleAction::Status => None,
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fs, path::PathBuf, sync::Arc};

    use super::*;
    use crate::{
        cli::{
            CliStorageKind, ConfigAction, DeployAction, DeploymentAction, DevAction,
            DiagnosticsAction, InfraAction, ObservabilityAction, ScaleAction,
        },
        deployment_dir::{DEPLOYMENT_TOML, METADATA_JSON, TOKEIRAD_TOML},
    };
    use proptest::prelude::*;
    use serde::Serialize;
    use serde_json::json;
    use tokeira_local_deployment::LocalConfig;
    use tokeira_orchestrator::{PlatformConfig, PlatformId, StorageKind};
    use uuid::Uuid;

    fn platform(value: &str) -> PlatformId {
        PlatformId::new(value).expect("test platform id")
    }

    #[test]
    fn parses_create_with_cli_enums() {
        let cli = Cli::try_parse_from([
            "tkr",
            "deployment",
            "create",
            "--name",
            "dev",
            "--platform",
            "local",
            "--storage",
            "in-memory",
        ])
        .unwrap();
        assert!(matches!(
            cli.command,
            Command::Deployment {
                action: DeploymentAction::Create { .. }
            }
        ));
    }

    // `tkr deployment create` alone must always work on a fresh machine: the
    // platform/storage default to the zero-dependency dev pairing.
    #[test]
    fn create_defaults_to_local_platform_with_in_memory_storage() {
        let cli = Cli::try_parse_from(["tkr", "deployment", "create", "--name", "dev"]).unwrap();
        let Command::Deployment {
            action:
                DeploymentAction::Create {
                    platform: selected_platform,
                    storage,
                    ..
                },
        } = cli.command
        else {
            panic!("expected a create action");
        };
        assert_eq!(selected_platform, platform("local"));
        assert!(matches!(storage, CliStorageKind::InMemory));
    }

    #[test]
    fn create_parses_a_complete_remote_state_locator() {
        let cli = Cli::try_parse_from([
            "tkr",
            "deployment",
            "create",
            "--name",
            "dev",
            "--state-bucket",
            "shared-state",
            "--state-region",
            "eu-west-2",
            "--state-prefix",
            "deployments/dev",
        ])
        .unwrap();
        let Command::Deployment {
            action:
                DeploymentAction::Create {
                    state_bucket,
                    state_region,
                    state_prefix,
                    ..
                },
        } = cli.command
        else {
            panic!("expected a create action");
        };
        assert_eq!(state_bucket.as_deref(), Some("shared-state"));
        assert_eq!(state_region.as_deref(), Some("eu-west-2"));
        assert_eq!(state_prefix.as_deref(), Some("deployments/dev"));
    }

    #[test]
    fn create_refuses_a_partial_remote_state_locator() {
        let error = match Cli::try_parse_from([
            "tkr",
            "deployment",
            "create",
            "--name",
            "dev",
            "--state-bucket",
            "shared-state",
        ]) {
            Ok(_) => panic!("bucket, region, and prefix are one choice"),
            Err(error) => error,
        };
        let rendered = error.to_string();
        assert!(rendered.contains("--state-region"), "{rendered}");
        assert!(rendered.contains("--state-prefix"), "{rendered}");
    }

    #[test]
    fn parses_dev_commands() {
        assert!(matches!(
            Cli::try_parse_from(["tkr", "dev", "build"])
                .unwrap()
                .command,
            Command::Dev {
                action: DevAction::Build
            }
        ));
        assert!(matches!(
            Cli::try_parse_from(["tkr", "dev", "test", "--crate", "tkr"])
                .unwrap()
                .command,
            Command::Dev {
                action: DevAction::Test {
                    crate_name: Some(crate_name)
                }
            } if crate_name == "tkr"
        ));
        assert!(matches!(
            Cli::try_parse_from(["tkr", "dev", "lint"]).unwrap().command,
            Command::Dev {
                action: DevAction::Lint
            }
        ));
        assert!(matches!(
            Cli::try_parse_from(["tkr", "dev", "fmt"]).unwrap().command,
            Command::Dev {
                action: DevAction::Fmt
            }
        ));
        assert!(matches!(
            Cli::try_parse_from(["tkr", "dev", "check"])
                .unwrap()
                .command,
            Command::Dev {
                action: DevAction::Check
            }
        ));
    }

    #[test]
    fn parses_worker_compute_diagnostics_as_read_only() {
        let command = Cli::try_parse_from([
            "tkr",
            "diagnostics",
            "worker-compute",
            "--namespace",
            "payments",
        ])
        .expect("diagnostics command")
        .command;
        assert!(matches!(
            &command,
            Command::Diagnostics {
                action: DiagnosticsAction::WorkerCompute { namespace }
            } if namespace == "payments"
        ));
        assert!(mutation_target(&command, Some("prod")).is_none());
    }

    #[test]
    fn parses_deployment_commands() {
        assert!(matches!(
            Cli::try_parse_from(["tkr", "deployment", "list"])
                .unwrap()
                .command,
            Command::Deployment {
                action: DeploymentAction::List { repositories: None }
            }
        ));
        assert!(matches!(
            Cli::try_parse_from(["tkr", "deployment", "use", "dev"])
                .unwrap()
                .command,
            Command::Deployment {
                action: DeploymentAction::Use { name }
            } if name == "dev"
        ));
        assert!(matches!(
            Cli::try_parse_from(["tkr", "deployment", "destroy", "--name", "dev", "--yes"])
                .unwrap()
                .command,
            Command::Deployment {
                action: DeploymentAction::Destroy { name, yes: true }
            } if name == "dev"
        ));
    }

    #[test]
    fn parses_lock_and_forwarded_verbs() {
        assert!(matches!(
            Cli::try_parse_from(["tkr", "deployment", "lock"])
                .unwrap()
                .command,
            Command::Deployment {
                action: DeploymentAction::Lock { name: None }
            }
        ));
        assert!(matches!(
            Cli::try_parse_from(["tkr", "deployment", "lock", "prod"])
                .unwrap()
                .command,
            Command::Deployment {
                action: DeploymentAction::Lock { name: Some(n) }
            } if n == "prod"
        ));
        assert!(matches!(
            Cli::try_parse_from(["tkr", "deployment", "unlock", "--yes"])
                .unwrap()
                .command,
            Command::Deployment {
                action: DeploymentAction::Unlock { yes: true }
            }
        ));
        for (args, ok) in [
            (
                ["tkr", "deployment", "describe"],
                DeploymentAction::Describe,
            ),
            (
                ["tkr", "deployment", "apply"],
                DeploymentAction::Apply { yes: false },
            ),
            (["tkr", "deployment", "upgrade"], DeploymentAction::Upgrade),
            (
                ["tkr", "deployment", "rollback"],
                DeploymentAction::Rollback,
            ),
        ] {
            let parsed = Cli::try_parse_from(args).unwrap().command;
            assert!(
                matches!(parsed, Command::Deployment { action } if std::mem::discriminant(&action) == std::mem::discriminant(&ok)),
                "unexpected parse for {args:?}"
            );
        }
    }

    #[test]
    fn mutation_target_classifies_mutating_vs_readonly() {
        let parse = |args: &[&str]| Cli::try_parse_from(args).unwrap().command;

        // Mutating, deployment-scoped → guarded.
        for args in [
            vec!["tkr", "infra", "apply", "--yes"],
            vec!["tkr", "infra", "destroy", "--yes"],
            vec!["tkr", "deploy", "apply", "--yes"],
            vec!["tkr", "deploy", "destroy", "--yes"],
            vec!["tkr", "schema", "setup", "--yes"],
            vec!["tkr", "scale", "up"],
            vec!["tkr", "scale", "down"],
            vec!["tkr", "deployment", "apply"],
            vec!["tkr", "deployment", "upgrade"],
            vec!["tkr", "deployment", "rollback"],
            vec!["tkr", "image", "push", "--yes"],
            vec!["tkr", "image", "mirror", "--yes"],
        ] {
            assert!(
                mutation_target(&parse(&args), Some("prod")).is_some(),
                "{args:?} should be guarded"
            );
        }

        // `deployment destroy` guards its explicit positional target.
        assert_eq!(
            mutation_target(
                &parse(&["tkr", "deployment", "destroy", "--name", "staging", "--yes"]),
                Some("prod")
            ),
            Some(Some("staging".to_string()))
        );

        // Read-only / registry / global → never guarded.
        for args in [
            vec!["tkr", "infra", "plan"],
            vec!["tkr", "infra", "status"],
            vec!["tkr", "deploy", "status"],
            vec!["tkr", "scale", "status"],
            vec!["tkr", "schema", "status"],
            vec!["tkr", "image", "build"],
            vec!["tkr", "image", "list"],
            vec!["tkr", "observability", "check"],
            vec!["tkr", "deployment", "describe"],
            vec!["tkr", "deployment", "list"],
            vec!["tkr", "deployment", "lock"],
            vec!["tkr", "deployment", "unlock", "--yes"],
            vec!["tkr", "version"],
        ] {
            assert!(
                mutation_target(&parse(&args), None).is_none(),
                "{args:?} should never be guarded"
            );
        }
    }

    // The explanation-artifact path crosses the forwarding boundary verbatim
    // (operator-explanation Req 3.1): `tkp` owns the write and the failure report,
    // so the token mapping is the whole of `tkr`'s responsibility here.
    #[test]
    fn forwarding_carries_the_explanation_path() {
        let plan = InfraAction::Plan {
            module: None,
            explanation: Some(PathBuf::from("/tmp/out.json")),
        };
        let (verb, extra) = forwarded_infra_verb(&plan);
        assert_eq!(verb, &["infra", "plan"]);
        assert_eq!(extra, vec!["--explanation", "/tmp/out.json"]);

        let deploy = DeployAction::Apply {
            yes: false,
            force: false,
            explanation: Some(PathBuf::from("/tmp/out.json")),
        };
        let (verb, extra) = forwarded_deploy_verb(&deploy);
        assert_eq!(verb, &["deploy", "apply"]);
        assert_eq!(extra, vec!["--explanation", "/tmp/out.json"]);

        let bare = InfraAction::Plan {
            module: None,
            explanation: None,
        };
        let (_, extra) = forwarded_infra_verb(&bare);
        assert!(extra.is_empty(), "no flag without a request");
    }

    #[test]
    fn forwarding_preserves_exec_argument_boundaries() {
        assert_eq!(
            forwarded_exec_args(
                "runtime",
                Some("tokeira-runtime"),
                &["sh".to_owned(), "-c".to_owned(), "echo ready".to_owned()],
            ),
            [
                "runtime",
                "--container",
                "tokeira-runtime",
                "--",
                "sh",
                "-c",
                "echo ready",
            ]
        );
    }

    #[test]
    fn forwarding_preserves_admin_argument_boundaries() {
        assert_eq!(
            forwarded_admin_args(&[
                "schema".to_owned(),
                "migrate".to_owned(),
                "--target".to_owned(),
                "5".to_owned(),
            ]),
            ["--", "schema", "migrate", "--target", "5"]
        );
    }

    #[test]
    fn forwarding_preserves_the_observability_check_timeout() {
        let (verb, extra) = forwarded_observability_verb(15);
        assert_eq!(verb, &["observability", "check"]);
        assert_eq!(
            extra,
            vec!["--timeout-seconds".to_string(), "15".to_string()]
        );
    }

    // The operator's confirmation crosses to the binary that gates on it:
    // `tkp` refuses destructive plans without `--yes`, so a forwarded
    // `deploy apply --yes` must carry the flag.
    #[test]
    fn forwarded_deploy_apply_carries_yes() {
        let confirmed = DeployAction::Apply {
            yes: true,
            force: false,
            explanation: None,
        };
        let (verb, extra) = forwarded_deploy_verb(&confirmed);
        assert_eq!(verb, &["deploy", "apply"]);
        assert_eq!(extra, vec!["--yes"]);

        let unconfirmed = DeployAction::Apply {
            yes: false,
            force: false,
            explanation: None,
        };
        let (_, extra) = forwarded_deploy_verb(&unconfirmed);
        assert!(extra.is_empty(), "no confirmation is forwarded unasked");

        let destroy = DeployAction::Destroy { yes: true };
        let (verb, extra) = forwarded_deploy_verb(&destroy);
        assert_eq!(verb, &["deploy", "destroy"]);
        assert_eq!(extra, vec!["--yes"]);
    }

    #[test]
    fn parses_infra_commands() {
        assert!(matches!(
            Cli::try_parse_from(["tkr", "infra", "plan"])
                .unwrap()
                .command,
            Command::Infra {
                action: InfraAction::Plan {
                    module: None,
                    explanation: None
                }
            }
        ));
        assert!(matches!(
            Cli::try_parse_from(["tkr", "infra", "apply", "--yes", "--module", "mimir"])
                .unwrap()
                .command,
            Command::Infra {
                action: InfraAction::Apply {
                    yes: true,
                    module: Some(module),
                    explanation: None
                }
            } if module == "mimir"
        ));
        assert!(matches!(
            Cli::try_parse_from(["tkr", "infra", "destroy", "--yes"])
                .unwrap()
                .command,
            Command::Infra {
                action: InfraAction::Destroy {
                    yes: true,
                    module: None
                }
            }
        ));
        assert!(matches!(
            Cli::try_parse_from(["tkr", "infra", "status"])
                .unwrap()
                .command,
            Command::Infra {
                action: InfraAction::Status
            }
        ));
    }

    #[test]
    fn parses_deploy_commands() {
        assert!(matches!(
            Cli::try_parse_from(["tkr", "deploy", "plan"])
                .unwrap()
                .command,
            Command::Deploy {
                action: DeployAction::Plan { explanation: None }
            }
        ));
        assert!(matches!(
            Cli::try_parse_from(["tkr", "deploy", "apply", "--yes"])
                .unwrap()
                .command,
            Command::Deploy {
                action: DeployAction::Apply { yes: true, .. }
            }
        ));
        assert!(matches!(
            Cli::try_parse_from(["tkr", "deploy", "destroy", "--yes"])
                .unwrap()
                .command,
            Command::Deploy {
                action: DeployAction::Destroy { yes: true }
            }
        ));
        assert!(matches!(
            Cli::try_parse_from(["tkr", "deploy", "status"])
                .unwrap()
                .command,
            Command::Deploy {
                action: DeployAction::Status
            }
        ));
    }

    #[test]
    fn parses_operational_commands() {
        assert!(matches!(
            Cli::try_parse_from(["tkr", "scale", "up"]).unwrap().command,
            Command::Scale {
                action: ScaleAction::Up {
                    service: None,
                    replicas: None
                }
            }
        ));
        assert!(matches!(
            Cli::try_parse_from(["tkr", "definition", "check"])
                .unwrap()
                .command,
            Command::Definition {
                action: cli::DefinitionAction::Check { .. }
            }
        ));
        // Authoring mode: source + explicit format parse; global --deployment parses after the
        // subcommand (Ian-grammar: flags where the operator's hands already are).
        assert!(matches!(
            Cli::try_parse_from([
                "tkr",
                "definition",
                "check",
                "--definition",
                "defs/staging.tkd",
                "--format",
                "tkd"
            ])
            .unwrap()
            .command,
            Command::Definition {
                action: cli::DefinitionAction::Check {
                    definition: Some(_),
                    format: Some(_)
                }
            }
        ));
        let parsed =
            Cli::try_parse_from(["tkr", "definition", "check", "--deployment", "prod"]).unwrap();
        assert_eq!(parsed.deployment.as_deref(), Some("prod"));
        assert!(matches!(
            Cli::try_parse_from(["tkr", "scale", "down", "tokeirad", "2"])
                .unwrap()
                .command,
            Command::Scale {
                action: ScaleAction::Down {
                    service: Some(service),
                    replicas: Some(2)
                }
            } if service == "tokeirad"
        ));
        assert!(matches!(
            Cli::try_parse_from(["tkr", "scale", "status"])
                .unwrap()
                .command,
            Command::Scale {
                action: ScaleAction::Status
            }
        ));
        assert!(matches!(
            Cli::try_parse_from(["tkr", "logs", "tokeirad", "--follow", "--tail", "20"])
                .unwrap()
                .command,
            Command::Logs {
                service,
                follow: true,
                tail: Some(20)
            } if service == "tokeirad"
        ));
        assert!(matches!(
            Cli::try_parse_from(["tkr", "port-forward", "grafana"])
                .unwrap()
                .command,
            Command::PortForward {
                service,
                local_port: None
            } if service == "grafana"
        ));
        assert!(matches!(
            Cli::try_parse_from(["tkr", "admin", "schema", "setup"])
                .unwrap()
                .command,
            Command::Admin { command } if command == ["schema", "setup"]
        ));
        assert!(matches!(
            Cli::try_parse_from([
                "tkr",
                "exec",
                "runtime",
                "--container",
                "tokeira-runtime",
                "--",
                "sh",
                "-c",
                "echo ready",
            ])
            .unwrap()
            .command,
            Command::Exec {
                service,
                container: Some(container),
                command,
            } if service == "runtime"
                && container == "tokeira-runtime"
                && command == ["sh", "-c", "echo ready"]
        ));
        assert!(matches!(
            Cli::try_parse_from(["tkr", "port-forward", "grafana", "--local-port", "33000"])
                .unwrap()
                .command,
            Command::PortForward {
                service,
                local_port: Some(33000)
            } if service == "grafana"
        ));
        assert!(matches!(
            Cli::try_parse_from(["tkr", "config", "show"])
                .unwrap()
                .command,
            Command::Config {
                action: ConfigAction::Show
            }
        ));
        assert!(matches!(
            Cli::try_parse_from(["tkr", "compat", "show", "--verbose"])
                .unwrap()
                .command,
            Command::Compat(crate::cli::CompatArgs {
                command: crate::cli::CompatCommand::Show {
                    remote: None,
                    json: false,
                    verbose: true
                }
            })
        ));
        assert!(matches!(
            Cli::try_parse_from(["tkr", "observability", "check", "--timeout-seconds", "15"])
                .unwrap()
                .command,
            Command::Observability {
                action: ObservabilityAction::Check {
                    path: None,
                    grafana: false,
                    timeout_seconds: 15
                }
            }
        ));
        assert!(matches!(
            Cli::try_parse_from([
                "tkr",
                "observability",
                "check",
                "--grafana",
                "--path",
                "/tmp/dashboard.json"
            ])
            .unwrap()
            .command,
            Command::Observability {
                action: ObservabilityAction::Check {
                    path: Some(path),
                    grafana: true,
                    timeout_seconds: 30
                }
            } if path == std::path::Path::new("/tmp/dashboard.json")
        ));
        assert!(Cli::try_parse_from(["tkr", "observability", "check", "--grafana"]).is_err());
        assert!(
            Cli::try_parse_from([
                "tkr",
                "observability",
                "check",
                "--path",
                "/tmp/rendered/config"
            ])
            .is_err()
        );
        assert!(matches!(
            Cli::try_parse_from(["tkr", "version"]).unwrap().command,
            Command::Version {
                verbose: false,
                json: false
            }
        ));
        assert!(matches!(
            Cli::try_parse_from(["tkr", "version", "--verbose"])
                .unwrap()
                .command,
            Command::Version {
                verbose: true,
                json: false
            }
        ));
    }

    #[test]
    fn creates_separate_deployment_and_server_configs() {
        let temp = tempfile::tempdir().unwrap();
        let deployments = DeploymentResolver::with_root(temp.path().to_path_buf());
        deployments
            .create("dev", platform("local"), StorageKind::InMemory, None)
            .unwrap();
        let deployment_path = deployments.path("dev");
        let _: LocalConfig =
            tokeira_config::load_config(&deployment_path.join(DEPLOYMENT_TOML), None).unwrap();
        let _: tokeira_config::TokeiraConfig =
            toml::from_str(&fs::read_to_string(deployment_path.join(TOKEIRAD_TOML)).unwrap())
                .unwrap();
    }

    #[test]
    fn deployment_create_round_trip_updates_latest() {
        let temp = tempfile::tempdir().unwrap();
        let deployments = DeploymentResolver::with_root(temp.path().to_path_buf());
        deployments
            .create("My Dev", platform("local"), StorageKind::InMemory, None)
            .unwrap();
        let deployment_path = deployments.path("my-dev");
        assert!(deployment_path.join(DEPLOYMENT_TOML).exists());
        assert!(deployment_path.join(TOKEIRAD_TOML).exists());
        assert!(deployment_path.join(METADATA_JSON).exists());
        assert!(deployment_path.join("state").exists());
        assert_eq!(deployments.resolve_name(None).unwrap(), "my-dev");
    }

    proptest! {
        // Feature: platform-builder-abstraction, Property 23: deployment publication is all-or-nothing.
        #[test]
        fn creation_transaction_hides_staging_and_rolls_back_latest_failure(
            suffix in "[a-z0-9]{1,12}",
            failure_point in 0_u8..3,
        ) {
            let temp = tempfile::tempdir().expect("tempdir");
            let deployments = DeploymentResolver::with_root(temp.path().to_path_buf());
            let name = format!("txn-{suffix}");
            let pending = deployments
                .begin_create(
                    &name,
                    platform("local"),
                    StorageKind::InMemory,
                    None,
                    tokeira_deployment::DeploymentStateLocation::Local,
                    None,
                )
                .expect("stage");

            match failure_point {
                0 => drop(pending),
                1 => {
                    fs::create_dir(deployments.latest_path()).expect("block latest rename");
                    let error = pending.publish().expect_err("publication must fail");
                    prop_assert!(error.to_string().contains(".latest"));
                }
                _ => {
                    let metadata = pending.publish().expect("publish");
                    prop_assert_eq!(metadata.name, name.clone());
                }
            }

            let final_path = deployments.path(&name);
            if failure_point < 2 {
                prop_assert!(!final_path.exists());
                prop_assert!(!deployments.latest_path().is_file());
            } else {
                prop_assert!(final_path.join(METADATA_JSON).is_file());
                prop_assert!(final_path.join(DEPLOYMENT_TOML).is_file());
                prop_assert!(final_path.join(TOKEIRAD_TOML).is_file());
                prop_assert_eq!(
                    fs::read_to_string(deployments.latest_path()).expect("latest"),
                    name
                );
            }
            let staging_remains = fs::read_dir(deployments.root())
                .expect("root")
                .filter_map(Result::ok)
                .any(|entry| entry.file_name().to_string_lossy().contains(".create-"));
            prop_assert!(!staging_remains);
        }
    }

    #[test]
    fn launch_routing_uses_metadata_not_definition_file_presence() {
        let temp = tempfile::tempdir().unwrap();
        let deployments = DeploymentResolver::with_root(temp.path().to_path_buf());
        deployments
            .create(
                "compose-route",
                platform("compose"),
                StorageKind::InMemory,
                None,
            )
            .unwrap();
        fs::remove_file(deployments.path("compose-route").join("deployment.tkd")).unwrap();
        assert!(
            deployments
                .uses_bound_provisioner(Some("compose-route"))
                .unwrap()
        );

        deployments
            .create(
                "local-route",
                platform("local"),
                StorageKind::InMemory,
                None,
            )
            .unwrap();
        fs::write(
            deployments.path("local-route").join("definition.tkd"),
            "not routing metadata",
        )
        .unwrap();
        assert!(
            !deployments
                .uses_bound_provisioner(Some("local-route"))
                .unwrap()
        );
    }

    #[test]
    fn deployment_name_normalization_is_lowercase_and_space_free() {
        assert_eq!(deployment_dir::normalize_name(" My Dev Env "), "my-dev-env");
        let normalized = deployment_dir::normalize_name("Mixed CASE Name");
        assert_eq!(
            normalized,
            deployment_dir::normalize_name("Mixed CASE Name")
        );
        assert!(normalized.chars().all(|ch| !ch.is_ascii_uppercase()));
        assert!(!normalized.contains(' '));
    }

    #[test]
    fn duplicate_deployment_name_is_rejected() {
        let temp = tempfile::tempdir().unwrap();
        let deployments = DeploymentResolver::with_root(temp.path().to_path_buf());
        deployments
            .create("dev", platform("local"), StorageKind::InMemory, None)
            .unwrap();
        let err = deployments
            .create("DEV", platform("local"), StorageKind::InMemory, None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("already exists"));
    }

    #[test]
    fn deployment_destroy_removes_directory_and_clears_latest() {
        let temp = tempfile::tempdir().unwrap();
        let deployments = DeploymentResolver::with_root(temp.path().to_path_buf());
        deployments
            .create("dev", platform("local"), StorageKind::InMemory, None)
            .unwrap();
        let path = deployments.path("dev");
        assert!(path.exists());
        deployments.remove("dev").unwrap();
        assert!(!path.exists());
        assert!(!deployments.latest_path().exists());
    }

    #[test]
    fn missing_deployment_error_lists_available_deployments() {
        let temp = tempfile::tempdir().unwrap();
        let deployments = DeploymentResolver::with_root(temp.path().to_path_buf());
        deployments
            .create("alpha", platform("local"), StorageKind::InMemory, None)
            .unwrap();
        deployments
            .create("beta", platform("local"), StorageKind::InMemory, None)
            .unwrap();
        let error = deployments.not_found_message("gamma").unwrap();
        assert!(error.contains("alpha"));
        assert!(error.contains("beta"));
    }

    // The in-process context loader refuses a forwarded deployment in domain
    // terms — what the deployment is and what operates it — never as a raw
    // missing-file error, and never as an implementation-status report.
    #[test]
    fn in_process_context_refuses_a_forwarded_deployment_in_domain_terms() {
        let temp = tempfile::tempdir().unwrap();
        let deployments = DeploymentResolver::with_root(temp.path().to_path_buf());
        deployments
            .create("fwd", platform("compose"), StorageKind::InMemory, None)
            .unwrap();
        let Err(err) = load_context(&deployments, Some("fwd")) else {
            panic!("a forwarded deployment must refuse the in-process context");
        };
        let message = err.to_string();
        assert!(message.contains("deployment.tkd"), "unexpected: {message}");
        assert!(message.contains("tkp"), "unexpected: {message}");
        assert!(
            !message.contains("No such file") && !message.contains("yet"),
            "reads as ENOENT or a roadmap apology: {message}"
        );
    }

    #[test]
    fn legacy_config_seeding_accepts_local_storage_modes() {
        let temp = tempfile::tempdir().unwrap();
        let deployments = DeploymentResolver::with_root(temp.path().to_path_buf());
        deployments
            .create("local-mem", platform("local"), StorageKind::InMemory, None)
            .unwrap();
        deployments
            .create("local-dsql", platform("local"), StorageKind::Dsql, None)
            .unwrap();
    }

    #[test]
    fn new_ecs_deployments_record_the_shipped_definition() {
        let temp = tempfile::tempdir().expect("deployment root");
        let deployments = DeploymentResolver::with_root(temp.path().to_path_buf());
        let metadata = deployments
            .create("ecs-defined", platform("ecs"), StorageKind::InMemory, None)
            .expect("ECS definition stages");
        let dir = deployments.path("ecs-defined");
        let definition = metadata
            .definition
            .expect("new ECS deployment records its definition");

        assert_eq!(definition.format.as_str(), "tkd");
        assert!(dir.join(definition.path.as_path()).is_file());
        assert!(dir.join("helpers.tkd").is_file());
        assert!(
            dir.join("observability/dashboards/broker-runtime-health.json")
                .is_file()
        );
        assert!(!dir.join(DEPLOYMENT_TOML).exists());
        assert!(dir.join(TOKEIRAD_TOML).exists());
    }

    #[test]
    fn writeback_sets_dotted_server_config_key() {
        use tokeira_local_deployment::LocalDeployment;

        let temp = tempfile::tempdir().unwrap();
        fs::write(
            temp.path().join(TOKEIRAD_TOML),
            LocalDeployment::prototypical_server_config(StorageKind::InMemory),
        )
        .unwrap();
        commands::infra::write_tokeirad_writeback(
            temp.path(),
            vec![(
                "infrastructure.dsql.endpoint".to_string(),
                "https://example.test".to_string(),
            )],
        )
        .unwrap();
        let config =
            commands::infra::read_tokeirad_config(&temp.path().join(TOKEIRAD_TOML)).unwrap();
        assert_eq!(
            config.infrastructure.dsql.endpoint.as_deref(),
            Some("https://example.test")
        );
    }

    #[test]
    fn json_metadata_is_stable() {
        let metadata = metadata::DeploymentMetadata {
            name: "dev".into(),
            id: Uuid::nil(),
            platform: platform("local"),
            state: Default::default(),
            definition: None,
            deployment_repository: None,
            storage: StorageKind::InMemory,
            status: metadata::DeploymentStatus::Created,
            created_at: "now".into(),
            updated_at: "now".into(),
        };
        assert_eq!(
            serde_json::to_value(&metadata).unwrap()["storage"],
            json!("in-memory")
        );
    }

    #[test]
    fn metadata_json_round_trip_preserves_values() {
        let metadata = metadata::DeploymentMetadata {
            name: "dev".into(),
            id: Uuid::nil(),
            platform: platform("compose"),
            state: tokeira_deployment::DeploymentStateLocation::S3 {
                bucket: "shared-state".into(),
                region: "eu-west-2".into(),
                prefix: "deployments/dev".into(),
            },
            definition: Some(tokeira_deployment::RecordedDefinition {
                format: tokeira_orchestrator::DefinitionFormatId::new("tkd").unwrap(),
                path: tokeira_orchestrator::RelativeDefinitionPath::new("definition.tkd").unwrap(),
            }),
            deployment_repository: None,
            storage: StorageKind::Dsql,
            status: metadata::DeploymentStatus::Running,
            created_at: "2026-04-24T00:00:00Z".into(),
            updated_at: "2026-04-24T00:00:01Z".into(),
        };
        let json = serde_json::to_string(&metadata).unwrap();
        let decoded: metadata::DeploymentMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.name, metadata.name);
        assert_eq!(decoded.id, metadata.id);
        assert_eq!(decoded.state, metadata.state);
        assert_eq!(decoded.created_at, metadata.created_at);
        assert!(matches!(
            decoded.status,
            metadata::DeploymentStatus::Running
        ));
    }

    #[test]
    fn local_deployment_config_has_no_compose_fields() {
        let temp = tempfile::tempdir().unwrap();
        let deployments = DeploymentResolver::with_root(temp.path().to_path_buf());
        deployments
            .create("test-local", platform("local"), StorageKind::InMemory, None)
            .unwrap();
        let toml_content =
            fs::read_to_string(deployments.path("test-local").join(DEPLOYMENT_TOML)).unwrap();
        assert!(!toml_content.contains("compose_file"));
        assert!(!toml_content.contains("observability"));
        assert!(!toml_content.contains("mimir"));
        assert!(toml_content.contains("project_name"));
        // state_dir removed — derived from deployment directory
        assert!(!toml_content.contains("state_dir"));
    }

    #[test]
    fn compose_deployment_seeds_the_tkd_definition() {
        // Compose is `.tkd`-defined + tkp-provisioned (forwarded): `create`
        // seeds the declared root and its parts, not a legacy in-process
        // `deployment.toml`.
        let temp = tempfile::tempdir().unwrap();
        let deployments = DeploymentResolver::with_root(temp.path().to_path_buf());
        deployments
            .create(
                "test-compose",
                platform("compose"),
                StorageKind::InMemory,
                None,
            )
            .unwrap();
        let dir = deployments.path("test-compose");

        let tkd = fs::read_to_string(dir.join("deployment.tkd")).unwrap();
        assert!(
            tkd.contains("fn deployment"),
            "seeds the compose `.tkd` structure"
        );
        // The definition set stages whole: the split root's parts land
        // beside it.
        assert!(dir.join("platform.tkd").is_file(), "stages the model part");
        assert!(
            dir.join("observability.tkd").is_file(),
            "stages the observability part"
        );
        assert!(
            dir.join("observability/dashboards/broker-runtime-health.json")
                .is_file(),
            "stages platform-owned companion content"
        );
        assert!(
            tkd.contains("Storage::InMemory"),
            "in-memory keeps the shipped config()"
        );
        assert!(tkd.contains("Observability"), "and the platform's kinds");
        // Server config is seeded (prototypical, editable pre-apply); no legacy
        // in-process `deployment.toml`.
        assert!(
            dir.join(TOKEIRAD_TOML).exists(),
            "seeds a prototypical tokeirad.toml"
        );
        assert!(
            !dir.join(DEPLOYMENT_TOML).exists(),
            "no legacy deployment.toml"
        );
        assert!(dir.join("state").exists(), "state dir created");
    }

    #[derive(Serialize)]
    struct DefinitionContext {
        project_name: String,
    }

    fn assert_created_eks_plan_inputs(
        dir: &std::path::Path,
        definition: &tokeira_deployment::RecordedDefinition,
    ) {
        use tokeira_platform::definition::{
            DefinitionSource, DefinitionSourceName, DirectoryPartSources, evaluate_definition,
            verify_definition,
        };

        let definition_path = dir.join(definition.path.as_path());
        let source = DefinitionSource {
            format: definition.format.clone(),
            source_name: DefinitionSourceName::DeploymentRelative(definition.path.clone()),
            bytes: Arc::from(fs::read(&definition_path).expect("created root is readable")),
        };
        let parts = DirectoryPartSources::new(dir, definition.format.as_str());
        let context = DefinitionContext {
            project_name: "created-eks".to_string(),
        };
        let declaration = tokeira_eks_deployment::platform();
        let evaluated = match definition.format.as_str() {
            "tkd" => evaluate_definition(
                &tokeira_platform_definition::tkd::frontend(),
                source,
                &context,
                &declaration.namespaces,
                &parts,
            ),
            "tkdp" => evaluate_definition(
                &tokeira_platform_definition::tkdp::frontend(),
                source,
                &context,
                &declaration.namespaces,
                &parts,
            ),
            format => panic!("unexpected EKS definition format {format}"),
        }
        .expect("created definition evaluates from its staged companions");
        let realized = verify_definition(&evaluated)
            .realize("created-eks", dir, dir, &BTreeMap::new())
            .expect("created definition realizes with staged companion content");
        let resources = realized.iter().collect::<Vec<_>>();
        assert!(
            tokeira_iac::verify_resources(&resources).is_empty(),
            "created definition produces valid provider-independent plan inputs"
        );
        assert_eq!(
            realized.manifests().len(),
            resources.len(),
            "every realized infrastructure resource contributes its desired plan manifest"
        );
    }

    #[test]
    fn eks_create_stages_full_sets_and_plan_inputs_for_both_formats() {
        let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let discovery = platform_discovery::PlatformDiscovery::from_workspace(&workspace)
            .expect("workspace discovery");
        let descriptor = discovery
            .platform(&platform("eks"))
            .expect("EKS descriptor");

        for format in ["tkd", "tkdp"] {
            let format_id = tokeira_orchestrator::DefinitionFormatId::new(format)
                .expect("built-in definition format");
            let (frontend, definition_path, seed_path) = discovery
                .workspace_frontend(descriptor, Some(&format_id))
                .expect("EKS frontend seed");
            let sources =
                tokeira_platform_definition::read_source_set(&seed_path, Some(&frontend.format))
                    .expect("complete EKS source set");
            let expected_root = sources.root.clone();
            let expected_parts = sources.parts.clone();
            let expected_content = discovery
                .workspace_content(descriptor)
                .expect("EKS companion content");
            let temp = tempfile::tempdir().expect("deployment root");
            let deployments = DeploymentResolver::with_root(temp.path().to_path_buf());
            let recorded = tokeira_deployment::RecordedDefinition {
                format: frontend.format.clone(),
                path: definition_path,
            };
            let pending = deployments
                .begin_create(
                    "created-eks",
                    platform("eks"),
                    StorageKind::InMemory,
                    None,
                    tokeira_deployment::DeploymentStateLocation::Local,
                    Some(deployment_dir::DefinitionSeed {
                        definition: recorded.clone(),
                        bytes: sources.root,
                        parts: sources.parts,
                        content: expected_content
                            .iter()
                            .map(|file| (file.relative_path.clone(), file.bytes.clone()))
                            .collect(),
                    }),
                )
                .expect("tkr create stages EKS");
            assert_eq!(
                fs::read(pending.path().join(recorded.path.as_path())).expect("staged root"),
                expected_root
            );
            for (name, expected) in &expected_parts {
                assert_eq!(
                    fs::read(pending.path().join(name)).expect("staged definition part"),
                    *expected,
                    "{format} part {name} stages byte-for-byte"
                );
            }
            for file in &expected_content {
                assert_eq!(
                    fs::read(pending.path().join(file.relative_path.as_path()))
                        .expect("staged companion content"),
                    file.bytes,
                    "{format} content {} stages byte-for-byte",
                    file.relative_path
                );
            }

            let metadata = pending.publish().expect("created deployment publishes");
            let dir = deployments.path(&metadata.name);
            assert_created_eks_plan_inputs(&dir, &recorded);
        }
    }

    #[tokio::test]
    // Feature: platform-builder-abstraction, Property 23: deployment publication is all-or-nothing.
    async fn discovered_selection_creates_and_checks_with_the_generated_compose_provisioner() {
        let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let discovery = platform_discovery::PlatformDiscovery::from_workspace(&workspace)
            .expect("workspace discovery");
        let descriptor = discovery
            .platform(&platform("compose"))
            .expect("Compose descriptor");
        let (frontend, definition_path, seed) = discovery
            .workspace_frontend(descriptor, None)
            .expect("Compose seed frontend");
        let platform_package = &descriptor.package;
        let frontend_package = &frontend.package;
        let sources = tokeira_platform_definition::read_source_set(&seed, Some(&frontend.format))
            .expect("frontend-owned sources");
        let content = discovery
            .workspace_content(descriptor)
            .expect("platform-owned content")
            .into_iter()
            .map(|file| (file.relative_path, file.bytes))
            .collect();
        let temp = tempfile::tempdir().expect("deployment root");
        let deployments = DeploymentResolver::with_root(temp.path().to_path_buf());
        let pending = deployments
            .begin_create(
                "generated-compose",
                platform("compose"),
                StorageKind::InMemory,
                None,
                tokeira_deployment::DeploymentStateLocation::Local,
                Some(deployment_dir::DefinitionSeed {
                    definition: tokeira_deployment::RecordedDefinition {
                        format: frontend.format.clone(),
                        path: definition_path,
                    },
                    parts: sources.parts,
                    bytes: sources.root,
                    content,
                }),
            )
            .expect("stage deployment");
        // Native build, then marry the real binary under a fixture identity
        // so the sidecar honestly describes the placed bytes. (The full
        // `--dev-engine` synthesis derives its identity from a git source
        // snapshot; the workspace-bar copy of this tree carries no .git, so
        // the test supplies the identity the snapshot would.)
        deployments
            .place_provisioner_at(
                pending.path(),
                &workspace,
                platform_package,
                frontend_package,
            )
            .expect("generated provisioner compiles and is placed");
        let engine_bytes = fs::read(pending.path().join("tkp")).expect("placed engine bytes");
        // Evidence over the real assembly with a fixture tree oid: the
        // identity's closures are taken FROM the evidence, so the pair
        // agrees by construction and admission's cross-checks all hold.
        let bound_source = tokeira_build::assemble_bound_provisioner(
            &workspace,
            platform_package,
            frontend_package,
        )
        .expect("bound source assembles");
        let evidence = bound_source.evidence("test-fixture-tree");
        let dev_identity = tokeira_deployment::EngineIdentity {
            source_closure: evidence.source_closure,
            lock_closure: evidence.lock_closure,
            toolchain: "rustc test".to_string(),
            build_container: None,
            features: std::collections::BTreeSet::new(),
            profile: tokeira_deployment::BuildProfile::Dev,
        };
        let synthesized = tokeira_deployment::ProvisionerBundle {
            identity: dev_identity,
            bound: None,
            authority: tokeira_deployment::BuildAuthority::LocalDeveloper,
            provisioner_version: "0.0.0-test".to_string(),
            artifacts: vec![tokeira_deployment::BinaryArtifactDescriptor {
                target: tokeira_deployment::Target(env!("TKR_TARGET").to_string()),
                sha256: tokeira_deployment::sha256_hex(&engine_bytes),
                retrieval_ref: None,
                size_bytes: engine_bytes.len() as u64,
            }],
            tests: tokeira_deployment::TestEvidence {
                command: "not run (test fixture)".to_string(),
                passed: false,
            },
            build: tokeira_deployment::BuildManifest {
                request_id: "req-test".to_string(),
                source_tree_oid: "t".to_string(),
                snapshot_commit_oid: "c".to_string(),
                toolchain: "rustc test".to_string(),
                builder: "test".to_string(),
            },
        }
        .with_bound_evidence(evidence)
        .expect("fixture evidence agrees with its identity");
        bundle_create::marry_bundle_at(
            pending.path(),
            synthesized,
            &tokeira_deployment::Target(env!("TKR_TARGET").to_string()),
            &engine_bytes,
        )
        .await
        .expect("real binary marries under the fixture identity");
        let facts = launcher::validate_staged_definition(pending.path())
            .await
            .expect("generated provisioner validates its seed");
        launcher::realize_staged_deployment(pending.path(), &facts, 0)
            .await
            .expect("creation realizes the Day-0 binding");
        let identity = facts.identity.expect("the check reports the identity");
        let companions = facts.companions.unwrap_or_default();
        let staged_repository = repository_setup::provision_staged(
            deployments.root(),
            pending.path(),
            "generated-compose",
        )
        .await
        .expect("staged repository state provisions");
        let metadata = pending.publish().expect("publish checked deployment");
        assert_eq!(metadata.platform.as_str(), "compose");
        let dir = deployments.path("generated-compose");
        assert!(dir.join("tkp").is_file());
        let envelope_store: Box<
            dyn tokeira_state::DeploymentStore<tokeira_deployment::DeploymentStateEnvelope>,
        > = Box::new(tokeira_state::CasStore::new(
            Box::new(tokeira_state::LocalBackend::new(dir.join("state/envelope"))),
            "envelope".to_string(),
        ));
        let (envelope, _) = envelope_store
            .load()
            .await
            .expect("created deployment envelope loads");
        assert!(
            envelope.binding.is_some(),
            "creation records the Day-0 provisioner binding"
        );
        assert!(
            envelope.integrity.is_some(),
            "creation records the placed provisioner's integrity manifest"
        );
        let retained_zero = dir.join("state/config-revisions/0");
        assert!(
            retained_zero
                .join("observability/templates/alloy.alloy")
                .is_file(),
            "revision zero retains the platform-declared authored content tree"
        );
        assert!(
            !retained_zero.join("config").exists(),
            "revision zero does not invent rendered apply output"
        );
        repository_setup::publish_birth(&dir, &staged_repository, identity, companions)
            .await
            .expect("the birth publication uploads");

        // Fetch onto a second root and prove the fetched seat's admission:
        // `tkp describe` on the placed bundle succeeds exactly as it does
        // for a workspace create (Requirements 5.5 / 12.4).
        let second = tempfile::tempdir().expect("second root");
        let fetched_resolver = DeploymentResolver::with_root(second.path().to_path_buf());
        repository_setup::fetch(
            &fetched_resolver,
            "generated-compose",
            &repository_setup::local_repository_home(deployments.root(), "generated-compose")
                .display()
                .to_string(),
            &dir.join("state/repository/root.json"),
        )
        .await
        .expect("fetch materializes the publication");
        let fetched_dir = fetched_resolver.path("generated-compose");
        let fetched_envelope_store: Box<
            dyn tokeira_state::DeploymentStore<tokeira_deployment::DeploymentStateEnvelope>,
        > = Box::new(tokeira_state::CasStore::new(
            Box::new(tokeira_state::LocalBackend::new(
                fetched_dir.join("state/envelope"),
            )),
            "envelope".to_string(),
        ));
        let (fetched_envelope, _) = fetched_envelope_store
            .load()
            .await
            .expect("fetched creation envelope loads");
        assert!(
            fetched_envelope.binding.is_some(),
            "fetch realizes the verified creation binding before publication"
        );
        assert!(
            fetched_dir
                .join("state/config-revisions/0/observability/templates/alloy.alloy")
                .is_file(),
            "fetch retains the materialized authored source at the claim's revision"
        );
        let fetched_metadata = metadata::read(&fetched_dir).expect("fetched metadata");
        let fetched_binding = fetched_metadata
            .deployment_repository
            .expect("fetched metadata carries the repository binding");
        assert_eq!(
            fetched_binding.trusted_root_digest,
            tokeira_deployment::sha256_hex(&staged_repository.trusted_root),
            "fetch re-pins the accepted trust anchor"
        );
        launcher::launch(&fetched_dir, &["describe"], &[])
            .await
            .expect("post-fetch describe admission is green");
    }

    /// Task 15.3: local create → local repository, no Dagger. The engine is
    /// a fake bundle through the REAL obtain pipeline (mock engine); the
    /// staged self-check is exercised by the discovered-selection test above
    /// (fake engine bytes cannot exec), so its identity facts are computed
    /// here with the sole implementation, exactly as the check would.
    #[tokio::test]
    async fn create_flow_publishes_a_verifiable_birth_publication_locally() {
        use tokeira_deployment::repository::{
            claim::Transition,
            open::{Freshness, open},
            publish::engine_target_name,
        };

        let temp = tempfile::tempdir().expect("deployment root");
        let deployments = DeploymentResolver::with_root(temp.path().join("deployments"));
        let format = tokeira_orchestrator::DefinitionFormatId::new("tkd").expect("format");
        let pending = deployments
            .begin_create(
                "dev",
                platform("compose"),
                StorageKind::InMemory,
                None,
                tokeira_deployment::DeploymentStateLocation::Local,
                Some(deployment_dir::DefinitionSeed {
                    definition: tokeira_deployment::RecordedDefinition {
                        format: format.clone(),
                        path: tokeira_orchestrator::RelativeDefinitionPath::new("definition.tkd")
                            .expect("path"),
                    },
                    bytes: b"root-definition".to_vec(),
                    parts: vec![("platform.tkd".to_string(), b"companion-part".to_vec())],
                    content: vec![(
                        tokeira_orchestrator::RelativeDefinitionPath::new(
                            "observability/templates/alloy.alloy",
                        )
                        .expect("content path"),
                        b"template-content".to_vec(),
                    )],
                }),
            )
            .expect("stage deployment");

        // A fake hermetic bundle through the real obtain pipeline.
        let repo = tempfile::tempdir().expect("fixture repo");
        let out = tempfile::tempdir().expect("obtain output");
        let host = tokeira_deployment::Target(env!("TKR_TARGET").to_string());
        let request = fake_bundle_request(repo.path(), out.path(), &host);
        let cas = tokeira_deployment::BundleStore::new(
            Box::new(tokeira_state::LocalBackend::new(temp.path().join("cas"))),
            "bundles",
        );
        let (client, _wire) = tokeira_build::testing::canned_client().await;
        let obtained = tokeira_build::obtain_provisioner(
            &request,
            &client,
            &cas,
            tokeira_deployment::AuthorityTier::LocalDeveloper,
        )
        .await
        .expect("obtains the fake bundle");
        let engine_bytes = obtained
            .bytes_by_target
            .get(&host)
            .expect("host artifact")
            .clone();
        bundle_create::marry_bundle_at(pending.path(), obtained.bundle, &host, &engine_bytes)
            .await
            .expect("marries the bundle");

        let identity = tokeira_platform::definition::ConfigurationIdentity::compute_set(
            &format,
            b"root-definition",
            &[(
                "platform".to_string(),
                std::sync::Arc::from(&b"companion-part"[..]),
            )],
        );

        // Repository state lands in the STAGED dir, before the rename.
        let staged = repository_setup::provision_staged(deployments.root(), pending.path(), "dev")
            .await
            .expect("provisions the staged repository state");
        for wired in [
            "state/repository/publisher.json",
            "state/repository/root.json",
            "state/repository/datastore",
        ] {
            assert!(pending.path().join(wired).exists(), "{wired} staged");
        }
        let staged_metadata = metadata::read(pending.path()).expect("staged metadata");
        let binding = staged_metadata
            .deployment_repository
            .expect("staged metadata carries the repository binding");
        assert_eq!(
            binding.trusted_root_digest,
            tokeira_deployment::sha256_hex(&staged.trusted_root)
        );

        let committed = pending.publish().expect("atomic rename commits");
        let dir = deployments.path(&committed.name);
        let receipt = repository_setup::publish_birth(
            &dir,
            &staged,
            identity.clone(),
            vec!["platform".to_string()],
        )
        .await
        .expect("the birth publication uploads");
        assert_eq!(receipt.version, 1);

        // Verify exactly as a fetch would: from the pinned anchor.
        let anchor = fs::read(dir.join("state/repository/root.json")).expect("pinned anchor");
        let publication = open(
            &staged.config.locator,
            &anchor,
            None,
            Freshness::Enforced,
            None,
        )
        .await
        .expect("repository opens from the pinned anchor")
        .verified_publication()
        .await
        .expect("the claim contract holds");
        let claim = publication.claim();
        assert_eq!(claim.deployment.name, "dev");
        assert_eq!(claim.transition, Transition::Create);
        assert_eq!(claim.config_revision, 0);
        assert_eq!(claim.definition.identity, identity);
        assert_eq!(claim.definition.companions, vec!["platform".to_string()]);
        assert_eq!(claim.engine.build_authority, "local-developer");

        // Byte-identity with the created dir (Requirement 2.2): definition
        // documents, every config target, and the engine binary.
        for target in ["definition.tkd", "platform.tkd"] {
            assert_eq!(
                publication.read(target).await.expect("published document"),
                fs::read(dir.join(target)).expect("local copy"),
                "{target} byte-identical"
            );
        }
        assert!(
            publication
                .config_targets()
                .iter()
                .any(|target| target == TOKEIRAD_TOML),
            "the templated config tree is published"
        );
        assert!(
            publication
                .config_targets()
                .iter()
                .any(|target| target == "observability/templates/alloy.alloy"),
            "platform-owned content is published"
        );
        for target in publication.config_targets() {
            assert_eq!(
                publication.read(target).await.expect("published config"),
                fs::read(dir.join(target)).expect("local copy"),
                "{target} byte-identical"
            );
        }
        assert_eq!(
            publication
                .read(&engine_target_name(&host.0))
                .await
                .expect("published engine"),
            fs::read(dir.join("tkp")).expect("placed tkp"),
            "the engine binary is the placed tkp"
        );

        // Exercise the verified materialization plan directly: the public
        // fetch path additionally executes the staged provisioner, which the
        // real generated-provisioner test above covers. These deterministic
        // mock artifact bytes deliberately are not an executable.
        let second = tempfile::tempdir().expect("second root");
        let fetched_resolver = DeploymentResolver::with_root(second.path().to_path_buf());
        let fetched_dir = fetched_resolver.path("dev");
        let plan = tokeira_deployment::repository::fetch::MaterializePlan::new(
            &publication,
            env!("TKR_TARGET"),
        )
        .expect("verified publication has a host materialization plan");
        plan.materialize_into(&publication, &fetched_dir)
            .await
            .expect("verified bytes materialize");
        for file in [
            "definition.tkd",
            "platform.tkd",
            TOKEIRAD_TOML,
            "tkp",
            tokeira_deployment::BUNDLE_MANIFEST_BASENAME,
        ] {
            assert_eq!(
                fs::read(fetched_dir.join(file)).expect("fetched file"),
                fs::read(dir.join(file)).expect("created file"),
                "{file} byte-identical across create and fetch"
            );
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = fs::metadata(fetched_dir.join("tkp"))
                .expect("fetched tkp")
                .permissions()
                .mode();
            assert_eq!(mode & 0o111, 0o111, "fetched tkp is executable");
        }
    }

    /// A buildable fake-bundle request over a throwaway git fixture — the
    /// mock engine exports deterministic bytes for `host`.
    fn fake_bundle_request(
        repo: &std::path::Path,
        out: &std::path::Path,
        host: &tokeira_deployment::Target,
    ) -> tokeira_build::ProvisionerBuildRequest {
        let git = |args: &[&str]| {
            let output = std::process::Command::new("git")
                .current_dir(repo)
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .env("GIT_AUTHOR_NAME", "t")
                .env("GIT_AUTHOR_EMAIL", "t@t.invalid")
                .env("GIT_COMMITTER_NAME", "t")
                .env("GIT_COMMITTER_EMAIL", "t@t.invalid")
                .args(args)
                .output()
                .expect("git runs");
            assert!(output.status.success(), "git {args:?} failed");
        };
        git(&["init", "-q", "-b", "main"]);
        fs::create_dir_all(repo.join("platforms/alpha/src")).expect("mkdir");
        fs::write(
            repo.join("rust-toolchain.toml"),
            "[toolchain]\nchannel = \"1.95\"\n",
        )
        .expect("write");
        fs::write(repo.join("Cargo.lock"), "version = 4\n").expect("write");
        fs::write(repo.join("platforms/alpha/src/lib.rs"), "pub fn a() {}\n").expect("write");
        git(&["add", "."]);
        git(&["commit", "-q", "-m", "seed"]);
        let snapshot = tokeira_build::snapshot_source_closure(&tokeira_build::SnapshotRequest {
            repo_root: repo.to_path_buf(),
            closure_paths: vec![
                PathBuf::from("platforms/alpha"),
                PathBuf::from("rust-toolchain.toml"),
                PathBuf::from("Cargo.lock"),
            ],
            include_untracked: false,
            content_overrides: std::collections::BTreeMap::new(),
        })
        .expect("snapshot");
        tokeira_build::ProvisionerBuildRequest {
            workspace_root: repo.to_path_buf(),
            bound_source: tokeira_build::BoundProvisionerSource::testing(
                tokeira_build::ProvisionerClosure {
                    crate_dirs: vec![PathBuf::from("platforms/alpha")],
                    crate_names: vec!["tokeira-alpha".into()],
                    path_dependency_dirs: Vec::new(),
                    workspace_files: vec![
                        PathBuf::from("rust-toolchain.toml"),
                        PathBuf::from("Cargo.lock"),
                    ],
                    locked: vec![],
                },
            ),
            targets: vec![host.clone()],
            profile: tokeira_deployment::BuildProfile::Dist,
            authority: tokeira_deployment::BuildAuthority::LocalDeveloper,
            build_image: format!("rust:1.95-slim-bookworm@sha256:{}", "ab".repeat(32)),
            snapshot,
            version: "0.1.0".into(),
            request_id: "req-repo-test".into(),
            output_dir: out.to_path_buf(),
        }
    }
}
