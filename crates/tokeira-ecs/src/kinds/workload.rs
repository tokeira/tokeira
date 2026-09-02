//! Typed author input for one ECS deploy-plane workload.

use serde::Deserialize;
use tokeira_platform::{
    error::KindError,
    kind::{Kind, PlacementContext},
};

use crate::{
    EcsConfig,
    modules::services::task_definition_needs_execution_role,
    services::{EcsScheduling, EcsWorkload},
};

/// Author-visible name of the realized service type.
pub(crate) const TYPE: &str = "EcsWorkload";

/// Reusable author input for one ECS workload, selected by canonical
/// service name from the platform's derived workload set.
///
/// The authored surface is exactly the operator-policy slice — identity
/// coordinates plus image, capacity, and replica policy. Everything else a
/// workload carries (container wiring, sidecars, Service Connect ports,
/// capacity-provider assignment) is derived by the platform's builders from
/// its default model; this kind applies authored values onto that model and
/// never re-derives them afterward.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Workload {
    /// Canonical workload name (e.g. `tokeira-runtime`, `tokeira-grafana`).
    pub(crate) service: String,
    /// Deployment environment (e.g. `dev`, `prod`).
    pub(crate) environment: String,
    /// AWS region the workload deploys into.
    pub(crate) region: String,
    /// ECS cluster name.
    pub(crate) cluster: String,
    /// Service Connect namespace. This is authored independently from the
    /// networking private DNS zone and must survive workload realization.
    pub(crate) service_connect_namespace: String,
    /// Container image for the workload's primary container.
    pub(crate) image: String,
    /// Desired replicas; `None` keeps the service's own scheduling policy
    /// (daemon services stay daemons, replica services keep their default).
    #[serde(default)]
    pub(crate) replicas: Option<u32>,
    /// Task CPU units.
    pub(crate) cpu: u32,
    /// Task memory in MiB.
    pub(crate) memory_mb: u32,
    /// Alloy image shared by the workload's collection sidecar.
    pub(crate) alloy_image: String,
    /// AWS CLI image used by the Alloy configuration init container.
    pub(crate) aws_cli_image: String,
    /// BusyBox image used by dependency-readiness init containers.
    pub(crate) busybox_image: String,
}

impl Workload {
    /// Apply the authored values onto the default model at the slot the
    /// canonical service owns. Refuses unknown names with the buildable set.
    fn configured(&self, deployment_id: &str) -> Result<EcsConfig, KindError> {
        let mut config = EcsConfig {
            project_name: deployment_id.to_owned(),
            environment: self.environment.clone(),
            region: self.region.clone(),
            ..EcsConfig::default()
        };
        config.cluster.name = self.cluster.clone();
        config.cluster.service_connect_namespace = self.service_connect_namespace.clone();
        config.observability.alloy_image = self.alloy_image.clone();
        config.observability.aws_cli_image = self.aws_cli_image.clone();
        config.observability.busybox_image = self.busybox_image.clone();

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
                let service = &mut config.services.autoscaler;
                service.image = self.image.clone();
                service.cpu = self.cpu;
                service.memory_mb = self.memory_mb;
                if let Some(replicas) = replicas {
                    service.desired_count = replicas;
                }
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
        // The builder subtracts fixed init/sidecar reservations from the task
        // total. Admission here is the last operator-facing boundary before
        // those unsigned calculations and the ECS task-definition manifest.
        config.validate().map_err(|error| {
            KindError::new(format!("invalid ECS workload `{}`: {error}", self.service))
        })?;
        Ok(config)
    }

    fn role_dependency(
        &self,
        placement: &PlacementContext,
        role_class: &str,
    ) -> Option<tokeira_iac::ResourceId> {
        let expected = format!(
            "iam-role-{}-{}-{role_class}",
            placement.deployment_id, self.service
        );
        placement
            .dependencies
            .iter()
            .find(|dependency| dependency.0 == expected)
            .cloned()
    }

    fn dependency(placement: &PlacementContext, expected: &str) -> Option<tokeira_iac::ResourceId> {
        placement
            .dependencies
            .iter()
            .find(|dependency| dependency.0 == expected)
            .cloned()
    }

    fn security_group_name(&self) -> &str {
        match self.service.as_str() {
            "tokeira-edge-api" | "tokeira-edge-poll" => "edge",
            "tokeira-runtime" => "runtime",
            "tokeira-projection" => "projection",
            "tokeira-controller" | "tokeira-autoscaler" | "tokeira-admin" => "control",
            "tokeira-mimir" => "mimir",
            "tokeira-loki" => "loki",
            "tokeira-grafana" => "grafana",
            _ => "unknown",
        }
    }

    fn uses_server_config(&self) -> bool {
        matches!(
            self.service.as_str(),
            "tokeira-edge-api"
                | "tokeira-edge-poll"
                | "tokeira-runtime"
                | "tokeira-projection"
                | "tokeira-admin"
        )
    }
}

impl Kind<EcsWorkload> for Workload {
    fn realize(&self, placement: &PlacementContext) -> Result<EcsWorkload, KindError> {
        let config = self.configured(&placement.deployment_id)?;
        let mut workloads = EcsWorkload::build_all(&config);
        workloads.extend(EcsWorkload::build_observability(&config));
        let mut workload = workloads
            .iter()
            .position(|workload| workload.name == self.service)
            .map(|index| workloads.swap_remove(index))
            .ok_or_else(|| {
                KindError::new(format!(
                    "workload `{}` did not derive from the configured model",
                    self.service
                ))
            })?;
        // Observability defaults are built as fixed replicas rather than
        // carrying a second desired-count config graph. Apply the common
        // authored replica field after selection so every replica workload
        // has the same definition contract.
        if let (Some(replicas), EcsScheduling::Replica { desired_count }) =
            (self.replicas, &mut workload.scheduling)
        {
            *desired_count = replicas;
        }
        let task_role = self.role_dependency(placement, "task").ok_or_else(|| {
            KindError::new(format!(
                "ECS workload `{}` needs its EcsTaskRole declared as a dependency",
                self.service
            ))
        })?;
        let execution_role = self.role_dependency(placement, "execution");
        if task_definition_needs_execution_role(&workload.task_definition)
            && execution_role.is_none()
        {
            return Err(KindError::new(format!(
                "ECS workload `{}` uses ECR or Secrets Manager and needs its EcsExecutionRole declared as a dependency",
                self.service
            )));
        }
        let vpc = Self::dependency(placement, &format!("{}-vpc", placement.deployment_id))
            .ok_or_else(|| {
                KindError::new(format!(
                    "ECS workload `{}` needs its Vpc declared as a dependency",
                    self.service
                ))
            })?;
        let security_group_id = format!("sg-{}", self.security_group_name());
        let security_group = Self::dependency(placement, &security_group_id).ok_or_else(|| {
            KindError::new(format!(
                "ECS workload `{}` needs security group `{security_group_id}` declared as a dependency",
                self.service
            ))
        })?;
        let target_group_id = format!("alb-tg-{}", self.service);
        let target_group = self
            .service
            .starts_with("tokeira-edge-")
            .then(|| {
                Self::dependency(placement, &target_group_id).ok_or_else(|| {
                    KindError::new(format!(
                        "ECS workload `{}` needs target group `{target_group_id}` declared as a dependency",
                        self.service
                    ))
                })
            })
            .transpose()?;
        let server_config = self
            .uses_server_config()
            .then(|| {
                let id = tokeira_deployment::server_config::resource_id();
                Self::dependency(placement, &id.0).ok_or_else(|| {
                    KindError::new(format!(
                        "ECS workload `{}` needs ServerConfig declared as a dependency",
                        self.service
                    ))
                })
            })
            .transpose()?;
        Ok(workload
            .with_role_dependencies(task_role, execution_role)
            .with_infrastructure_dependencies(vpc, security_group, target_group, server_config))
    }
}
