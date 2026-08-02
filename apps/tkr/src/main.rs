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
//! | Cloud infra       | `tkr infra`      | Plan / apply / destroy declared resources.     |
//! | Service deploy    | `tkr deploy`     | Plan / apply service manifests (0-replica).    |
//! | DSQL schema       | `tkr schema`     | Schema setup for DSQL-backed deployments.      |
//! | Scaling + ops     | `tkr scale`, `tkr logs`, `tkr port-forward` | Day-2 operator loops. |
//! | Inspection        | `tkr config show`, `tkr version`            | Debugging aids.       |
//!
//! # Architecture pointers
//!
//! - **`cli`** defines the clap surface. Adding a subcommand starts here.
//! - **`deployment_dir`** owns how deployments are resolved, loaded, and persisted.
//! - **`commands`** contains one module per top-level subcommand; each module
//!   receives a `DeploymentContext` (when needed) and dispatches through
//!   `commands::PlatformOps` for platform-agnostic operations.
//! - **`tui`** wires engine progress events to spinners (human mode) or JSON
//!   lines (`--json` mode).
//! - **`prototypical`** generates the template TOML written when a deployment
//!   is created.
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
pub mod catalog;
mod cli;
mod commands;
mod deployment_dir;
mod deployment_lock;
mod launcher;
mod metadata;
mod output;
mod process;
mod prototypical;
mod tui;

use cli::{
    Cli, Command, ConfigAction, DeployAction, DeploymentAction, ImageCommand, InfraAction,
    ScaleAction, SchemaAction,
};
use deployment_dir::{DeploymentResolver, load_context};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
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
    // `Image` is the odd one out: `image build` works without any deployment
    // (it operates on the workspace sources directly), while `image
    // push/mirror/list` need a deployment to look up the platform's image
    // catalog. The subcommand module handles that split internally; we just
    // opportunistically load a context when one could apply.
    match cli.command {
        Command::Dev { action } => commands::dev::run(action),
        Command::Deployment { action } => {
            commands::deployment::run(action, &deployments, selected, cli.json, cli.detail).await
        }
        Command::Image(args) => {
            let deployment = match args.command {
                cli::ImageCommand::Build { .. } => None,
                _ => Some(load_context(&deployments, selected)?),
            };
            let format = if cli.json {
                tui::OutputFormat::Json
            } else {
                tui::OutputFormat::Human
            };
            commands::image::run(args.command, deployment, format).await
        }
        Command::Definition { action } => {
            let cli::DefinitionAction::Check { path } = action;
            if let Some(path) = path {
                // Authoring mode: the definition needs no deployment. One
                // subject per check — a named path and a named deployment
                // cannot both be it.
                if selected.is_some() {
                    anyhow::bail!("pass either `--path` or `--deployment`, not both");
                }
                launcher::launch_definition_check_at_path(&path, cli.json, cli.detail).await
            } else if deployments.is_forwarded(selected)? {
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
            // A `.tkd` deployment is forwarded to its bound `tkp`; only the legacy
            // in-process platforms run through `commands::infra`.
            if deployments.is_forwarded(selected)? {
                let dir = deployments.resolve_dir(selected)?;
                // `infra apply` is the same coherent one-command flow as
                // `deployment apply` (Req 6.5): a never-stamped deployment is
                // initialized first, then applied — the operator's first
                // apply works whichever spelling they reach for.
                if let InfraAction::Apply {
                    yes, explanation, ..
                } = &action
                {
                    launcher::launch_apply(&dir, *yes, explanation.as_deref()).await
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
            if deployments.is_forwarded(selected)? {
                let dir = deployments.resolve_dir(selected)?;
                let (verb, mut extra) = forwarded_deploy_verb(&action);
                if matches!(action, DeployAction::Plan { .. } | DeployAction::Status) {
                    extra.extend(output_flags(cli.json, cli.detail));
                }
                launcher::launch(&dir, verb, &extra).await
            } else {
                let ctx = load_context(&deployments, selected)?;
                commands::deploy::run(action, &deployments, ctx).await
            }
        }
        Command::Schema { action } => {
            let ctx = load_context(&deployments, selected)?;
            commands::schema::run(action, ctx).await
        }
        Command::Scale { action } => {
            if deployments.is_forwarded(selected)? {
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
            let ctx = load_context(&deployments, selected)?;
            commands::logs::run(&service, follow, tail, ctx).await
        }
        Command::PortForward {
            service,
            local_port,
        } => {
            let ctx = load_context(&deployments, selected)?;
            commands::port_forward::run(&service, local_port, ctx).await
        }
        Command::Exec {
            service,
            container,
            cmd,
        } => {
            let ctx = load_context(&deployments, selected)?;
            commands::exec::run(&service, container.as_deref(), &cmd, ctx).await
        }
        Command::Config {
            action: ConfigAction::Show,
        } => commands::config::run_show(&deployments, selected),
        Command::Compat(args) => commands::compat::run(args.command, cli.json),
        Command::Ci(args) => commands::ci::run(args.command, cli.json).await,
        Command::Observability { action } => {
            let ctx = load_context(&deployments, selected)?;
            commands::observability::run(action, ctx)
        }
        Command::Diagnostics { action } => {
            let ctx = load_context(&deployments, selected)?;
            commands::diagnostics::run(action, ctx, cli.json).await
        }
        Command::Workstation { action } => commands::workstation::run(action, cli.json).await,
        Command::Admin { command } => {
            let ctx = load_context(&deployments, selected)?;
            commands::admin::run(command, ctx).await
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
/// `deploy apply`, `schema setup`, `scale up|down`, `image push|mirror`,
/// `admin`, `exec` (arbitrary in-container commands can mutate the deployment,
/// exactly like `admin`), `deployment destroy`, and the forwarded `deployment
/// apply|upgrade|rollback`. Plans, statuses, `describe`, `list`, `version`,
/// `image list|build`, and registry/selection verbs (`create`/`use`/`lock`/
/// `unlock`) are never blocked.
fn mutation_target(command: &Command, selected: Option<&str>) -> Option<Option<String>> {
    let selected = || selected.map(str::to_string);
    match command {
        Command::Infra {
            action: InfraAction::Apply { .. } | InfraAction::Destroy { .. },
        }
        | Command::Deploy {
            action: DeployAction::Apply { .. },
        }
        | Command::Schema {
            action: SchemaAction::Setup { .. },
        }
        | Command::Scale {
            action: ScaleAction::Up { .. } | ScaleAction::Down { .. },
        }
        | Command::Admin { .. }
        | Command::Exec { .. } => Some(selected()),
        Command::Image(args) => match &args.command {
            ImageCommand::Push { .. } | ImageCommand::Mirror { .. } => Some(selected()),
            ImageCommand::List { .. } | ImageCommand::Build { .. } => None,
        },
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

/// Map a `tkr infra` action to the `tkp` verb (+ args) the launcher forwards for a
/// `.tkd`/forwarded deployment. `tkp`'s surface is currently flat (`tkp plan`);
/// once it is namespaced (`tkp infra plan`) this maps to the namespaced form.
/// Map a `tkr infra` action to the forwarded `tkp` verb tokens — the same
/// namespaced words the operator typed (`tkr infra plan` → `tkp infra plan`),
/// so forwarding is a transparent pass-through (Req 7.3).
fn forwarded_infra_verb(action: &InfraAction) -> (&'static [&'static str], Vec<String>) {
    match action {
        InfraAction::Plan { explanation, .. } => {
            (&["infra", "plan"], explanation_flag(explanation.as_deref()))
        }
        InfraAction::Apply { .. } => (&["infra", "apply"], Vec::new()),
        InfraAction::Destroy { yes, .. } => (
            &["infra", "destroy"],
            if *yes {
                vec!["--yes".to_string()]
            } else {
                Vec::new()
            },
        ),
        InfraAction::Status => (&["describe"], Vec::new()),
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
        DeployAction::Status => (&["describe"], Vec::new()),
    }
}

/// Re-spell a requested explanation-artifact path for a forwarded `tkp`
/// invocation. The path crosses as given: `tkp` owns the write and the
/// failure report (evidence-model Req 7.6).
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
    use std::{fs, path::PathBuf};

    use super::*;
    use crate::{
        cli::{
            CliPlatformKind, CliStorageKind, ConfigAction, DeployAction, DeploymentAction,
            DevAction, DiagnosticsAction, InfraAction, ObservabilityAction, ScaleAction,
        },
        deployment_dir::{DEPLOYMENT_TOML, METADATA_JSON, TOKEIRAD_TOML},
    };
    use serde_json::json;
    use tokeira_local_deployment::LocalConfig;
    use tokeira_orchestrator::{PlatformConfig, PlatformKind, StorageKind};
    use uuid::Uuid;

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
            action: DeploymentAction::Create {
                platform, storage, ..
            },
        } = cli.command
        else {
            panic!("expected a create action");
        };
        assert!(matches!(platform, CliPlatformKind::Local));
        assert!(matches!(storage, CliStorageKind::InMemory));
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
                action: DeploymentAction::List
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
            vec!["tkr", "schema", "setup", "--yes"],
            vec!["tkr", "scale", "up"],
            vec!["tkr", "scale", "down"],
            vec!["tkr", "image", "push"],
            vec!["tkr", "image", "mirror"],
            vec!["tkr", "admin", "schema", "setup"],
            vec!["tkr", "exec", "runtime", "--", "sh"],
            vec!["tkr", "deployment", "apply"],
            vec!["tkr", "deployment", "upgrade"],
            vec!["tkr", "deployment", "rollback"],
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
            vec!["tkr", "image", "list"],
            vec!["tkr", "image", "build"],
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
    // (evidence-model Req 7.1): `tkp` owns the write and the failure report,
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
        // Authoring mode: --path parses; global --deployment parses after the
        // subcommand (Ian-grammar: flags where the operator's hands already are).
        assert!(matches!(
            Cli::try_parse_from(["tkr", "definition", "check", "--path", "defs/staging.tkd"])
                .unwrap()
                .command,
            Command::Definition {
                action: cli::DefinitionAction::Check { path: Some(_) }
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
            Command::PortForward { service, local_port: None } if service == "grafana"
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
                    timeout_seconds: 15
                }
            }
        ));
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
            .create("dev", PlatformKind::Local, StorageKind::InMemory, None)
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
            .create("My Dev", PlatformKind::Local, StorageKind::InMemory, None)
            .unwrap();
        let deployment_path = deployments.path("my-dev");
        assert!(deployment_path.join(DEPLOYMENT_TOML).exists());
        assert!(deployment_path.join(TOKEIRAD_TOML).exists());
        assert!(deployment_path.join(METADATA_JSON).exists());
        assert!(deployment_path.join("state").exists());
        assert_eq!(deployments.resolve_name(None).unwrap(), "my-dev");
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
            .create("dev", PlatformKind::Local, StorageKind::InMemory, None)
            .unwrap();
        let err = deployments
            .create("DEV", PlatformKind::Local, StorageKind::InMemory, None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("already exists"));
    }

    #[test]
    fn deployment_destroy_removes_directory_and_clears_latest() {
        let temp = tempfile::tempdir().unwrap();
        let deployments = DeploymentResolver::with_root(temp.path().to_path_buf());
        deployments
            .create("dev", PlatformKind::Local, StorageKind::InMemory, None)
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
            .create("alpha", PlatformKind::Local, StorageKind::InMemory, None)
            .unwrap();
        deployments
            .create("beta", PlatformKind::Local, StorageKind::InMemory, None)
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
            .create("fwd", PlatformKind::Compose, StorageKind::InMemory, None)
            .unwrap();
        let Err(err) = load_context(&deployments, Some("fwd")) else {
            panic!("a forwarded deployment must refuse the in-process context");
        };
        let message = err.to_string();
        assert!(message.contains("definition.tkd"), "unexpected: {message}");
        assert!(message.contains("tkp"), "unexpected: {message}");
        assert!(
            !message.contains("No such file") && !message.contains("yet"),
            "reads as ENOENT or a roadmap apology: {message}"
        );
    }

    #[test]
    fn rejects_invalid_platform_storage_combination() {
        // Currently all platform × storage combinations are valid.
        // This test documents that deployment creation succeeds for
        // each supported combination.
        let temp = tempfile::tempdir().unwrap();
        let deployments = DeploymentResolver::with_root(temp.path().to_path_buf());
        deployments
            .create(
                "local-mem",
                PlatformKind::Local,
                StorageKind::InMemory,
                None,
            )
            .unwrap();
        deployments
            .create("local-dsql", PlatformKind::Local, StorageKind::Dsql, None)
            .unwrap();
        deployments
            .create(
                "compose-mem",
                PlatformKind::Compose,
                StorageKind::InMemory,
                None,
            )
            .unwrap();
        deployments
            .create(
                "compose-dsql",
                PlatformKind::Compose,
                StorageKind::Dsql,
                None,
            )
            .unwrap();
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
            platform: PlatformKind::Local,
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
            platform: PlatformKind::Compose,
            storage: StorageKind::Dsql,
            status: metadata::DeploymentStatus::Running,
            created_at: "2026-04-24T00:00:00Z".into(),
            updated_at: "2026-04-24T00:00:01Z".into(),
        };
        let json = serde_json::to_string(&metadata).unwrap();
        let decoded: metadata::DeploymentMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.name, metadata.name);
        assert_eq!(decoded.id, metadata.id);
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
            .create(
                "test-local",
                PlatformKind::Local,
                StorageKind::InMemory,
                None,
            )
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
        // Compose is now `.tkd`-defined + tkp-provisioned (forwarded): `create`
        // seeds `definition.tkd`, not a legacy in-process `deployment.toml`.
        let temp = tempfile::tempdir().unwrap();
        let deployments = DeploymentResolver::with_root(temp.path().to_path_buf());
        deployments
            .create(
                "test-compose",
                PlatformKind::Compose,
                StorageKind::InMemory,
                None,
            )
            .unwrap();
        let dir = deployments.path("test-compose");

        let tkd = fs::read_to_string(dir.join("definition.tkd")).unwrap();
        assert!(
            tkd.contains("fn deployment"),
            "seeds the compose `.tkd` structure"
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
}
