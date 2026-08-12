//! Typed author input for one ECS deploy-plane workload.

use serde::Deserialize;
use tokeira_platform::{
    error::KindError,
    kind::{Kind, PlacementContext},
};

use crate::{EcsConfig, services::EcsWorkload};

/// Author-visible name of the realized service type.
pub const TYPE: &str = "EcsWorkload";

/// Reusable author input for one ECS workload, selected by canonical
/// service name from the platform's derived workload set.
///
/// The authored surface is exactly the operator-policy slice — identity
/// coordinates plus image, capacity, and replica policy. Everything else a
/// workload carries (container wiring, sidecars, Service Connect ports,
/// capacity-provider assignment) is derived by the platform's builders from
/// its default model, the same derivation the legacy path performs; this
/// kind applies the authored values onto that model and never re-derives.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Workload {
    /// Canonical workload name (e.g. `tokeira-runtime`, `tokeira-grafana`).
    pub service: String,
    /// Deployment environment (e.g. `dev`, `prod`).
    pub environment: String,
    /// AWS region the workload deploys into.
    pub region: String,
    /// ECS cluster name.
    pub cluster: String,
    /// Service Connect namespace.
    pub service_connect_namespace: String,
    /// Container image for the workload's primary container.
    pub image: String,
    /// Desired replicas; `None` keeps the service's own scheduling policy
    /// (daemon services stay daemons, replica services keep their default).
    #[serde(default)]
    pub replicas: Option<u32>,
    /// Task CPU units.
    pub cpu: u32,
    /// Task memory in MiB.
    pub memory_mb: u32,
}

impl Workload {
    /// Apply the authored values onto the default model at the slot the
    /// canonical service owns. Refuses unknown names with the buildable set.
    fn configured(&self) -> Result<EcsConfig, KindError> {
        let mut config = EcsConfig::default();
        config.environment = self.environment.clone();
        config.region = self.region.clone();
        config.cluster.name = self.cluster.clone();
        config.cluster.service_connect_namespace = self.service_connect_namespace.clone();

        let replicas = self.replicas;
        match self.service.as_str() {
            "tokeira-edge-api" => {
                let service = &mut config.services.edge_api;
                service.image = self.image.clone();
                service.cpu = self.cpu;
                service.memory_mb = self.memory_mb;
                if let Some(replicas) = replicas {
                    service.desired_count = replicas;
                }
            }
            "tokeira-edge-poll" => {
                let service = &mut config.services.edge_poll;
                service.image = self.image.clone();
                service.cpu = self.cpu;
                service.memory_mb = self.memory_mb;
                if let Some(replicas) = replicas {
                    service.desired_count = replicas;
                }
            }
            "tokeira-runtime" => {
                let service = &mut config.services.runtime;
                service.image = self.image.clone();
                service.cpu = self.cpu;
                service.memory_mb = self.memory_mb;
            }
            "tokeira-projection" => {
                let service = &mut config.services.projection;
                service.image = self.image.clone();
                service.cpu = self.cpu;
                service.memory_mb = self.memory_mb;
                if let Some(replicas) = replicas {
                    service.desired_count = replicas;
                }
            }
            "tokeira-controller" => {
                let service = &mut config.services.controller;
                service.image = self.image.clone();
                service.cpu = self.cpu;
                service.memory_mb = self.memory_mb;
                if let Some(replicas) = replicas {
                    service.desired_count = replicas;
                }
            }
            "tokeira-admin" => {
                let service = &mut config.services.admin;
                service.image = self.image.clone();
                service.cpu = self.cpu;
                service.memory_mb = self.memory_mb;
                if let Some(replicas) = replicas {
                    service.desired_count = replicas;
                }
            }
            "tokeira-autoscaler" => {
                config.autoscaler.image = self.image.clone();
            }
            "tokeira-mimir" => {
                config.observability.mimir_image = self.image.clone();
                config.observability.mimir_cpu = self.cpu;
                config.observability.mimir_memory_mb = self.memory_mb;
            }
            "tokeira-loki" => {
                config.observability.loki_image = self.image.clone();
                config.observability.loki_cpu = self.cpu;
                config.observability.loki_memory_mb = self.memory_mb;
            }
            "tokeira-grafana" => {
                config.observability.grafana_image = self.image.clone();
                config.observability.grafana_cpu = self.cpu;
                config.observability.grafana_memory_mb = self.memory_mb;
            }
            unknown => {
                let mut workloads = EcsWorkload::build_all(&config);
                workloads.extend(EcsWorkload::build_observability(&config));
                let names: Vec<&str> = workloads
                    .iter()
                    .map(|workload| workload.name.as_str())
                    .collect();
                return Err(KindError::new(format!(
                    "unknown ECS workload `{unknown}`; the platform builds: {}",
                    names.join(", ")
                )));
            }
        }
        Ok(config)
    }
}

impl Kind<EcsWorkload> for Workload {
    fn realize(&self, _placement: &PlacementContext) -> Result<EcsWorkload, KindError> {
        let config = self.configured()?;
        let mut workloads = EcsWorkload::build_all(&config);
        workloads.extend(EcsWorkload::build_observability(&config));
        workloads
            .iter()
            .position(|workload| workload.name == self.service)
            .map(|index| workloads.swap_remove(index))
            .ok_or_else(|| {
                KindError::new(format!(
                    "workload `{}` did not derive from the configured model",
                    self.service
                ))
            })
    }
}
