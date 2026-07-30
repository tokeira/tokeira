//! Day-2 operator helpers over a running compose deployment — log and
//! port-mapping lookups plus declared replicas, keyed on the deployment
//! directory and the `deployment.toml` config. Read-only by construction:
//! the mutating verbs reach the deployment through its married provisioner
//! and are deliberately not here.

use std::path::{Path, PathBuf};

use tokeira_compose::ComposePlatform;
use tokeira_orchestrator::{PortMapping, Result, ServiceReplicas};

use crate::config::ComposeConfig;

/// The services a compose deployment runs. Fixed by the reference definition;
/// used to answer an unknown service name with the valid set.
pub const VALID_SERVICES: [&str; 5] = ["mimir", "loki", "tokeirad", "grafana", "alloy"];

/// Compose file is always at `<deployment_dir>/docker-compose.yml`.
fn compose_file_for(deployment_dir: &Path) -> PathBuf {
    deployment_dir.join("docker-compose.yml")
}

fn platform(deployment_dir: &Path, config: &ComposeConfig) -> Result<ComposePlatform> {
    ComposePlatform::connect(compose_file_for(deployment_dir), &config.project_name)
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

/// The replica counts the deployment's config declares.
pub fn desired_replicas(config: &ComposeConfig) -> Vec<ServiceReplicas> {
    vec![
        ServiceReplicas {
            service: "mimir".into(),
            replicas: config.observability.mimir_replicas,
        },
        ServiceReplicas {
            service: "loki".into(),
            replicas: config.observability.loki_replicas,
        },
        ServiceReplicas {
            service: "tokeirad".into(),
            replicas: config.tokeirad.replicas,
        },
        ServiceReplicas {
            service: "grafana".into(),
            replicas: config.observability.grafana_replicas,
        },
        ServiceReplicas {
            service: "alloy".into(),
            replicas: config.observability.alloy_replicas,
        },
    ]
}

/// Tail a service's container logs.
pub async fn logs(
    service: &str,
    config: &ComposeConfig,
    deployment_dir: &Path,
) -> Result<Vec<String>> {
    known_service(service)?;
    platform(deployment_dir, config)?
        .logs(service)
        .await
        .map_err(anyhow::Error::from)
        .map_err(Into::into)
}

/// The service's live host-port mappings.
pub async fn port_mappings(
    service: &str,
    config: &ComposeConfig,
    deployment_dir: &Path,
) -> Result<Vec<PortMapping>> {
    known_service(service)?;
    platform(deployment_dir, config)?
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
