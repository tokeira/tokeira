//! The Compose ops surface over running deployments: logs and port
//! mappings.
//!
//! These answer from the Docker daemon alone — label-filtered container
//! lookup plus inspect — scoped by the deployment name the framework already
//! admitted. No compose-file ledger and no recorded state is consulted: an
//! ops question is about live containers, so the handle behind it is the
//! ledger-free [`ComposePlatform::ops`] constructor.

use tokeira_platform::declaration::{DeploymentRef, LogStream, Ops, PortMapping};

use crate::ComposePlatform;

/// Compose's ops surface, answered by the local Docker daemon.
#[derive(Debug)]
pub struct DockerOps;

#[async_trait::async_trait]
impl Ops for DockerOps {
    async fn log_stream(
        &self,
        deployment: &DeploymentRef,
        service: &str,
        follow: bool,
        tail: Option<u32>,
    ) -> anyhow::Result<LogStream> {
        use futures_util::StreamExt;
        let platform = ComposePlatform::ops(&deployment.name)?;
        let stream = platform.log_stream(service, follow, tail).await?;
        Ok(Box::pin(
            stream.map(|item| item.map_err(anyhow::Error::from)),
        ))
    }

    async fn port_mappings(
        &self,
        deployment: &DeploymentRef,
        service: &str,
    ) -> anyhow::Result<Vec<PortMapping>> {
        let platform = ComposePlatform::ops(&deployment.name)?;
        Ok(platform
            .port_mappings(service)
            .await?
            .into_iter()
            .map(
                |(host_addr, host_port, container_port, protocol)| PortMapping {
                    host_addr,
                    host_port,
                    container_port,
                    protocol,
                },
            )
            .collect())
    }
}
