//! Command handlers, one module per top-level `tkr` subcommand.
//!
//! Each submodule owns its own `run(...)` entry point, called from the
//! dispatcher in `main.rs`. Definition-bound deployments are forwarded to their
//! married provisioner. These handlers retain the Local in-process path.
//!
//! # Adding a new subcommand
//!
//! 1. Add a new module file here.
//! 2. Add the clap variant in `cli::Command`.
//! 3. Wire up the dispatch arm in `main::main`.
//! 4. Re-use `require_confirmation` for any destructive operation.

pub(crate) mod ci;
pub(crate) mod compat;
pub(crate) mod config;
pub(crate) mod deploy;
pub(crate) mod deployment;
pub(crate) mod dev;
pub(crate) mod diagnostics;
pub(crate) mod image;
pub(crate) mod infra;
pub(crate) mod logs;
pub(crate) mod observability;
pub(crate) mod port_forward;
pub(crate) mod release;
pub(crate) mod scale;
pub(crate) mod schema;
pub(crate) mod version;

use anyhow::{Result, bail};
use tokeira_local_deployment::{LocalConfig, LocalDeployment};
use tokeira_orchestrator::Ops;

use crate::deployment_dir::{DeploymentContext, PlatformDeploymentConfig};

/// Facade over the local in-process deployment type.
///
/// Each variant carries the platform's `Deployment` implementor together
/// with the config it needs. The `Deployment` trait is object-unsafe
/// (it's generic over `Config`), so a plain `Box<dyn Deployment>` won't
/// work — this enum keeps that boundary explicit for the shared handlers.
///
/// Day-2 handlers (`scale`, `logs`, `port_forward`) go through this facade
/// so they stay platform-neutral. Handlers that need to reach into
/// platform-specific APIs match on [`PlatformDeploymentConfig`] directly.
pub(crate) enum PlatformOps {
    Local(LocalDeployment, LocalConfig),
}

impl PlatformOps {
    pub(crate) fn from_context(ctx: &DeploymentContext) -> Result<Self> {
        match &ctx.platform_config {
            PlatformDeploymentConfig::Local(config) => {
                Ok(Self::Local(LocalDeployment, config.clone()))
            }
        }
    }

    pub(crate) fn desired_replicas(&self) -> Vec<tokeira_orchestrator::ServiceReplicas> {
        match self {
            Self::Local(d, c) => d.desired_replicas(c),
        }
    }

    pub(crate) async fn scale_up(
        &self,
        service: &str,
        replicas: u32,
    ) -> tokeira_orchestrator::Result<()> {
        match self {
            Self::Local(d, c) => d.scale_up(service, replicas, c).await,
        }
    }

    pub(crate) async fn scale_down(
        &self,
        service: &str,
        replicas: u32,
    ) -> tokeira_orchestrator::Result<()> {
        match self {
            Self::Local(d, c) => d.scale_down(service, replicas, c).await,
        }
    }

    pub(crate) async fn logs(&self, service: &str) -> tokeira_orchestrator::Result<Vec<String>> {
        match self {
            Self::Local(d, c) => d.logs(service, c).await,
        }
    }

    pub(crate) async fn port_mappings(
        &self,
        service: &str,
    ) -> tokeira_orchestrator::Result<Vec<tokeira_orchestrator::PortMapping>> {
        match self {
            Self::Local(d, c) => d.port_mappings(service, c).await,
        }
    }
}

/// Gate a destructive action behind an explicit `--yes` flag.
///
/// Every `apply`, `destroy`, or `remove` path routes through this helper
/// so confirmation behaviour stays consistent across subcommands. The
/// `action` argument is echoed in the error text so operators know
/// exactly which invocation was refused.
pub(crate) fn require_confirmation(yes: bool, action: &str) -> Result<()> {
    if yes {
        Ok(())
    } else {
        bail!("refusing to run {action} without --yes")
    }
}

/// Refuse `--explanation` on the in-process platforms. The explanation
/// artifact is produced by a deployment's own provisioner (operator-explanation
/// Req 3); a `deployment.toml`-configured deployment runs in-process and has
/// no explanation model to write — stated as the contract, not a roadmap.
pub(crate) fn refuse_explanation(explanation: Option<&std::path::Path>) -> Result<()> {
    if explanation.is_some() {
        bail!(
            "`--explanation` is not available here: this deployment's platform does not \
             produce an explanation model"
        );
    }
    Ok(())
}
