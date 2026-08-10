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

    /// Scale one or more services by replicating their recorded container
    /// specs (`<service>=<n>`). Drives the platform's additive scale-up —
    /// each spec creates `n` suffixed replica containers from the service's
    /// recorded configuration; reducing capacity is not a Compose ops verb
    /// (the deploy plane's reconcile owns desired-replica convergence).
    /// Returns the number of containers created.
    async fn scale(&self, deployment: &DeploymentRef, specs: &[String]) -> anyhow::Result<usize> {
        let ledger = deployment.dir.join("state/compose-services.yaml");
        let platform = ComposePlatform::connect(ledger, &deployment.dir, &deployment.name)?;
        let mut created = 0usize;
        for spec in specs {
            let (service, replicas) = spec
                .split_once('=')
                .and_then(|(name, count)| Some((name.trim(), count.trim().parse::<u32>().ok()?)))
                .ok_or_else(|| {
                    anyhow::anyhow!("scale spec `{spec}` is not `<service>=<replicas>`")
                })?;
            let recorded = platform.recorded_service(service)?.ok_or_else(|| {
                anyhow::anyhow!(
                    "service `{service}` is not recorded in the deployment's compose state; \
                     apply the deployment before scaling it"
                )
            })?;
            platform.scale_service(&recorded, replicas).await?;
            created += replicas as usize;
        }
        Ok(created)
    }
}
