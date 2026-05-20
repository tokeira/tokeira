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

use anyhow::Result;
use clap::Parser;

mod cli;
mod commands;
mod deployment_dir;
mod metadata;
mod output;
mod process;
mod prototypical;
mod tui;

use cli::{Cli, Command, ConfigAction};
use deployment_dir::{DeploymentResolver, load_context};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let deployments = DeploymentResolver::default()?;

    // Each arm resolves the deployment context it needs before dispatching.
    // `Image` is the odd one out: `image build` works without any deployment
    // (it operates on the workspace sources directly), while `image
    // push/mirror/list` need a deployment to look up the platform's image
    // catalog. The subcommand module handles that split internally; we just
    // opportunistically load a context when one could apply.
    match cli.command {
        Command::Dev { action } => commands::dev::run(action),
        Command::Deployment { action } => commands::deployment::run(action, &deployments, cli.json),
        Command::Image(args) => {
            let deployment = match args.command {
                cli::ImageCommand::Build { .. } => None,
                _ => Some(load_context(&deployments, cli.deployment.as_deref())?),
            };
            let format = if cli.json {
                tui::OutputFormat::Json
            } else {
                tui::OutputFormat::Human
            };
            commands::image::run(args.command, deployment, format).await
        }
        Command::Infra { action } => {
            let ctx = load_context(&deployments, cli.deployment.as_deref())?;
            let format = if cli.json {
                tui::OutputFormat::Json
            } else {
                tui::OutputFormat::Human
            };
            commands::infra::run(action, &deployments, ctx, format).await
        }
        Command::Deploy { action } => {
            let ctx = load_context(&deployments, cli.deployment.as_deref())?;
            commands::deploy::run(action, &deployments, ctx).await
        }
        Command::Schema { action } => {
            let ctx = load_context(&deployments, cli.deployment.as_deref())?;
            commands::schema::run(action, ctx).await
        }
        Command::Scale { action } => {
            let ctx = load_context(&deployments, cli.deployment.as_deref())?;
            commands::scale::run(action, &deployments, ctx).await
        }
        Command::Logs {
            service,
            follow,
            tail,
        } => {
            let ctx = load_context(&deployments, cli.deployment.as_deref())?;
            commands::logs::run(&service, follow, tail, ctx).await
        }
        Command::PortForward { service, local_port } => {
            let ctx = load_context(&deployments, cli.deployment.as_deref())?;
            commands::port_forward::run(&service, local_port, ctx).await
        }
        Command::Exec {
            service,
            container,
            cmd,
        } => {
            let ctx = load_context(&deployments, cli.deployment.as_deref())?;
            commands::exec::run(&service, container.as_deref(), &cmd, ctx).await
        }
        Command::Config {
            action: ConfigAction::Show,
        } => {
            let ctx = load_context(&deployments, cli.deployment.as_deref())?;
            commands::config::run_show(ctx)
        }
        Command::Workstation { action } => commands::workstation::run(action, cli.json).await,
        Command::Admin { command } => {
            let ctx = load_context(&deployments, cli.deployment.as_deref())?;
            commands::admin::run(command, ctx).await
        }
        Command::Version => {
            commands::version::run();
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use crate::{
        cli::{ConfigAction, DeployAction, DeploymentAction, DevAction, InfraAction, ScaleAction},
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
            Cli::try_parse_from(["tkr", "deployment", "destroy", "dev", "--yes"])
                .unwrap()
                .command,
            Command::Deployment {
                action: DeploymentAction::Destroy { name, yes: true }
            } if name == "dev"
        ));
    }

    #[test]
    fn parses_infra_commands() {
        assert!(matches!(
            Cli::try_parse_from(["tkr", "infra", "plan"])
                .unwrap()
                .command,
            Command::Infra {
                action: InfraAction::Plan { module: None }
            }
        ));
        assert!(matches!(
            Cli::try_parse_from(["tkr", "infra", "apply", "--yes", "--module", "mimir"])
                .unwrap()
                .command,
            Command::Infra {
                action: InfraAction::Apply {
                    yes: true,
                    module: Some(module)
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
                action: DeployAction::Plan
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
            Cli::try_parse_from(["tkr", "version"]).unwrap().command,
            Command::Version
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
    fn compose_deployment_config_has_compose_fields() {
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
        let toml_content =
            fs::read_to_string(deployments.path("test-compose").join(DEPLOYMENT_TOML)).unwrap();
        // compose_file removed — derived from deployment directory
        assert!(!toml_content.contains("compose_file"));
        assert!(toml_content.contains("observability"));
        assert!(toml_content.contains("mimir"));
        assert!(toml_content.contains("grafana"));
    }
}
