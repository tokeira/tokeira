//! Command handlers, one module per top-level `tkr` subcommand.
//!
//! Each submodule owns its own `run(...)` entry point, called from the
//! dispatcher in `main.rs`. Handlers receive a `DeploymentContext` (when
//! applicable) and use [`PlatformOps::from_context`] to get a
//! platform-agnostic interface to day-2 operations (scale, logs,
//! port-forward). Subcommand-specific flows (`infra`, `deploy`, `image`,
//! `schema`) are more bespoke because their engine boundaries differ.
//!
//! # Adding a new subcommand
//!
//! 1. Add a new module file here.
//! 2. Add the clap variant in `cli::Command`.
//! 3. Wire up the dispatch arm in `main::main`.
//! 4. Re-use `require_confirmation` for any destructive operation.

pub mod admin;
pub mod config;
pub mod deploy;
pub mod deployment;
pub mod dev;
pub mod exec;
pub mod image;
pub mod infra;
pub mod logs;
pub mod observability;
pub mod port_forward;
pub mod scale;
pub mod schema;
pub mod version;
pub mod workstation;

use std::path::PathBuf;

use anyhow::{Result, bail};
use tokeira_compose_deployment::{ComposeConfig, ComposeDeployment};
use tokeira_ecs_deployment::{EcsConfig, EcsDeployment};
use tokeira_local_deployment::{LocalConfig, LocalDeployment};
use tokeira_orchestrator::Ops;

use crate::deployment_dir::{DeploymentContext, PlatformDeploymentConfig};

/// Platform-agnostic facade over the three concrete deployment types.
///
/// Each variant carries the platform's `Deployment` implementor together
/// with the config it needs. The `Deployment` trait is object-unsafe
/// (it's generic over `Config`), so a plain `Box<dyn Deployment>` won't
/// work — this enum threads the three platforms explicitly instead.
///
/// Day-2 handlers (`scale`, `logs`, `port_forward`) go through this facade
/// so they stay platform-neutral. Handlers that need to reach into
/// platform-specific APIs (the engine-based `infra` and `deploy`
/// handlers, or the ECS-only `image push/mirror`) match on
/// [`PlatformDeploymentConfig`] directly.
///
/// The Compose variant also carries the deployment directory because the
/// Compose ops need it to locate the generated `docker-compose.yml`.
pub enum PlatformOps {
    Local(LocalDeployment, LocalConfig),
    Compose(ComposeDeployment, Box<ComposeConfig>, PathBuf),
    Ecs(EcsDeployment, Box<EcsConfig>),
}

impl PlatformOps {
    pub fn from_context(ctx: &DeploymentContext) -> Result<Self> {
        match &ctx.platform_config {
            PlatformDeploymentConfig::Local(config) => {
                Ok(Self::Local(LocalDeployment, config.clone()))
            }
            PlatformDeploymentConfig::Compose(config) => Ok(Self::Compose(
                ComposeDeployment,
                config.clone(),
                ctx.path.clone(),
            )),
            PlatformDeploymentConfig::Ecs(config) => {
                Ok(Self::Ecs(EcsDeployment::new(), config.clone()))
            }
        }
    }

    pub fn desired_replicas(&self) -> Vec<tokeira_orchestrator::ServiceReplicas> {
        match self {
            Self::Local(d, c) => d.desired_replicas(c),
            Self::Compose(d, c, _) => d.desired_replicas(c),
            Self::Ecs(d, c) => d.desired_replicas(c),
        }
    }

    pub async fn scale_up(&self, service: &str, replicas: u32) -> tokeira_orchestrator::Result<()> {
        match self {
            Self::Local(d, c) => d.scale_up(service, replicas, c).await,
            Self::Compose(d, c, dir) => d.scale_up_with_dir(service, replicas, c, dir).await,
            Self::Ecs(d, c) => d.scale_up(service, replicas, c).await,
        }
    }

    pub async fn scale_down(
        &self,
        service: &str,
        replicas: u32,
    ) -> tokeira_orchestrator::Result<()> {
        match self {
            Self::Local(d, c) => d.scale_down(service, replicas, c).await,
            Self::Compose(d, c, dir) => d.scale_down_with_dir(service, replicas, c, dir).await,
            Self::Ecs(d, c) => d.scale_down(service, replicas, c).await,
        }
    }

    pub async fn logs(&self, service: &str) -> tokeira_orchestrator::Result<Vec<String>> {
        match self {
            Self::Local(d, c) => d.logs(service, c).await,
            Self::Compose(d, c, dir) => d.logs_with_dir(service, c, dir).await,
            Self::Ecs(d, c) => d.logs(service, c).await,
        }
    }

    pub async fn port_mappings(
        &self,
        service: &str,
    ) -> tokeira_orchestrator::Result<Vec<tokeira_orchestrator::PortMapping>> {
        match self {
            Self::Local(d, c) => d.port_mappings(service, c).await,
            Self::Compose(d, c, dir) => d.port_mappings_with_dir(service, c, dir).await,
            Self::Ecs(d, c) => d.port_mappings(service, c).await,
        }
    }
}

/// Gate a destructive action behind an explicit `--yes` flag.
///
/// Every `apply`, `destroy`, or `remove` path routes through this helper
/// so confirmation behaviour stays consistent across subcommands. The
/// `action` argument is echoed in the error text so operators know
/// exactly which invocation was refused.
pub fn require_confirmation(yes: bool, action: &str) -> Result<()> {
    if yes {
        Ok(())
    } else {
        bail!("refusing to run {action} without --yes")
    }
}
