//! Compose-owned logs and port-forwarding support.

use std::path::Path;

use futures_util::TryStreamExt;
use tokeira_compose::{ComposePlatform, LogStream};
use tokeira_orchestrator::{PortMapping, Result, ServiceReplicas};
use tokeira_provisioner::DeploymentBindingMetadata;

use crate::config::ComposeConfig;

/// Services in the reference Compose deployment.
pub const VALID_SERVICES: [&str; 5] = ["mimir", "loki", "tokeirad", "grafana", "alloy"];

fn platform(deployment_dir: &Path) -> Result<ComposePlatform> {
    let metadata_path = deployment_dir.join("metadata.json");
    let metadata: DeploymentBindingMetadata =
        serde_json::from_slice(&std::fs::read(&metadata_path).map_err(anyhow::Error::from)?)
            .map_err(anyhow::Error::from)?;
    ComposePlatform::connect(
        deployment_dir.join("state/compose-services.yaml"),
        &metadata.name,
    )
    .map_err(anyhow::Error::from)
    .map_err(Into::into)
}

fn known_service(service: &str) -> Result<()> {
    if VALID_SERVICES.contains(&service) {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "unknown service '{service}'. valid services: {}",
            VALID_SERVICES.join(", ")
        )
        .into())
    }
}

/// Replica intent in platform startup order.
pub fn desired_replicas(config: &ComposeConfig) -> Vec<ServiceReplicas> {
    vec![
        ServiceReplicas {
            service: "mimir".into(),
            replicas: config.observability.mimir.replicas,
        },
        ServiceReplicas {
            service: "loki".into(),
            replicas: config.observability.loki.replicas,
        },
        ServiceReplicas {
            service: "tokeirad".into(),
            replicas: config.tokeirad.replicas,
        },
        ServiceReplicas {
            service: "grafana".into(),
            replicas: config.observability.grafana.replicas,
        },
        ServiceReplicas {
            service: "alloy".into(),
            replicas: config.observability.alloy.replicas,
        },
    ]
}

/// Open incremental logs for one Compose service.
pub async fn log_stream(service: &str, deployment_dir: &Path) -> Result<LogStream> {
    known_service(service)?;
    platform(deployment_dir)?
        .log_stream(service)
        .await
        .map_err(anyhow::Error::from)
        .map_err(Into::into)
}

/// Collect logs for the retained in-process `tkr` route until catalog routing lands.
pub async fn logs(
    service: &str,
    _config: &ComposeConfig,
    deployment_dir: &Path,
) -> Result<Vec<String>> {
    log_stream(service, deployment_dir)
        .await?
        .map_err(|error| anyhow::Error::from(error).into())
        .try_collect()
        .await
}

/// Resolve the service's live host/container port mappings.
pub async fn port_mappings(
    service: &str,
    _config: &ComposeConfig,
    deployment_dir: &Path,
) -> Result<Vec<PortMapping>> {
    known_service(service)?;
    platform(deployment_dir)?
        .port_mappings(service)
        .await
        .map(|mappings| {
            mappings
                .into_iter()
                .map(
                    |(host_addr, host_port, container_port, protocol)| PortMapping {
                        host_addr,
                        host_port,
                        container_port,
                        protocol,
                    },
                )
                .collect()
        })
        .map_err(anyhow::Error::from)
        .map_err(Into::into)
}
