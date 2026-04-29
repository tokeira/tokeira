pub mod config;
pub mod deploy;
pub mod deployment;
pub mod dev;
pub mod infra;
pub mod logs;
pub mod port_forward;
pub mod scale;
pub mod schema;
pub mod version;

use std::path::PathBuf;

use anyhow::{Result, bail};
use tokeira_compose_deployment::{ComposeConfig, ComposeDeployment};
use tokeira_local_deployment::{LocalConfig, LocalDeployment};
use tokeira_orchestrator::Ops;

use crate::deployment_dir::{DeploymentContext, PlatformDeploymentConfig};

/// Dispatch helper: returns a boxed Ops trait object and the matching config
/// for the deployment's platform kind.
pub enum PlatformOps {
    Local(LocalDeployment, LocalConfig),
    Compose(ComposeDeployment, ComposeConfig, PathBuf),
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
        }
    }

    pub fn desired_replicas(&self) -> Vec<tokeira_orchestrator::ServiceReplicas> {
        match self {
            Self::Local(d, c) => d.desired_replicas(c),
            Self::Compose(d, c, _) => d.desired_replicas(c),
        }
    }

    pub async fn scale_up(
        &self,
        service: &str,
        replicas: u32,
    ) -> tokeira_orchestrator::Result<()> {
        match self {
            Self::Local(d, c) => d.scale_up(service, replicas, c).await,
            Self::Compose(d, c, dir) => {
                d.scale_up_with_dir(service, replicas, c, dir).await
            }
        }
    }

    pub async fn scale_down(
        &self,
        service: &str,
        replicas: u32,
    ) -> tokeira_orchestrator::Result<()> {
        match self {
            Self::Local(d, c) => d.scale_down(service, replicas, c).await,
            Self::Compose(d, c, dir) => {
                d.scale_down_with_dir(service, replicas, c, dir).await
            }
        }
    }

    pub async fn logs(&self, service: &str) -> tokeira_orchestrator::Result<Vec<String>> {
        match self {
            Self::Local(d, c) => d.logs(service, c).await,
            Self::Compose(d, c, dir) => d.logs_with_dir(service, c, dir).await,
        }
    }

    pub async fn port_mappings(
        &self,
        service: &str,
    ) -> tokeira_orchestrator::Result<Vec<tokeira_orchestrator::PortMapping>> {
        match self {
            Self::Local(d, c) => d.port_mappings(service, c).await,
            Self::Compose(d, c, dir) => d.port_mappings_with_dir(service, c, dir).await,
        }
    }
}

pub fn require_confirmation(yes: bool, action: &str) -> Result<()> {
    if yes {
        Ok(())
    } else {
        bail!("refusing to run {action} without --yes")
    }
}
