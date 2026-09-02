//! Definition-derived coordinates for ECS live operations.
//!
//! Workload kinds own these facts because they have already applied authored
//! values and platform derivation. The framework transports only a sanitized
//! descriptor; this module closes that opaque value back into a strict ECS
//! schema and rejects mixed deployment coordinates before any AWS client can
//! be selected. Provider state is deliberately absent: task ARNs, running
//! counts, and network attachments must always be queried live.

use std::collections::BTreeMap;

use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Serialize};
use tokeira_platform::declaration::{DefinitionOperationsContext, DeploymentRef};

use crate::{
    kinds::workload,
    services::{EcsScheduling, EcsWorkload},
};

/// Closed, non-secret descriptor emitted by one realized [`EcsWorkload`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkloadDescriptor {
    region: String,
    cluster: String,
    service_connect_namespace: String,
    container: String,
    scheduling: EcsScheduling,
    capacity_provider: String,
    ports: Vec<PortDescriptor>,
}

/// Primary-container port available to definition-bound operator commands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PortDescriptor {
    name: String,
    container_port: u16,
    protocol: String,
}

/// Validated coordinates shared by every service in one ECS deployment.
#[derive(Debug, Clone)]
pub struct EcsOperationsContext {
    deployment: DeploymentRef,
    region: String,
    cluster: String,
    service_connect_namespace: String,
    services: BTreeMap<String, EcsServiceOperations>,
}

/// Validated coordinates owned by one ECS service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcsServiceOperations {
    container: String,
    scheduling: EcsScheduling,
    capacity_provider: String,
    ports: Vec<PortDescriptor>,
}

/// Produce the only data allowed to cross from desired workload realization
/// into live operations.
///
/// This allowlist is a security boundary. In particular, it does not reuse a
/// task-definition or service manifest: those carry images, secret references,
/// IAM role ARNs, and resolved network state that generic operator diagnostics
/// must never retain or print.
pub(crate) fn workload_descriptor(workload: &EcsWorkload) -> serde_json::Value {
    let primary = workload
        .task_definition
        .containers
        .iter()
        .find(|container| container.name == workload.name)
        .expect("ECS builders always name the primary container after its service");
    let descriptor = WorkloadDescriptor {
        region: workload.region.clone(),
        cluster: workload.cluster.clone(),
        service_connect_namespace: workload.service_connect_namespace.clone(),
        container: primary.name.clone(),
        scheduling: workload.scheduling.clone(),
        capacity_provider: workload.capacity_provider.clone(),
        ports: primary
            .port_mappings
            .iter()
            .map(|port| PortDescriptor {
                name: port.name.clone(),
                container_port: port.container_port,
                protocol: port.protocol.clone(),
            })
            .collect(),
    };
    serde_json::to_value(descriptor)
        .expect("the closed ECS operations descriptor contains only JSON-compatible values")
}

impl EcsOperationsContext {
    /// Close and validate all ECS workload descriptors for one admitted definition.
    pub fn from_definition(context: &DefinitionOperationsContext) -> Result<Self> {
        let mut region: Option<String> = None;
        let mut cluster: Option<String> = None;
        let mut service_connect_namespace: Option<String> = None;
        let mut services = BTreeMap::new();

        for service in context
            .services()
            .iter()
            .filter(|service| service.resource_type() == workload::TYPE)
        {
            let descriptor: WorkloadDescriptor =
                serde_json::from_value(service.attributes().clone()).with_context(|| {
                    format!(
                        "ECS workload `{}` emitted an invalid operations descriptor",
                        service.name()
                    )
                })?;
            validate_descriptor(service.name(), &descriptor)?;
            admit_shared_coordinate("region", service.name(), &descriptor.region, &mut region)?;
            admit_shared_coordinate("cluster", service.name(), &descriptor.cluster, &mut cluster)?;
            admit_shared_coordinate(
                "Service Connect namespace",
                service.name(),
                &descriptor.service_connect_namespace,
                &mut service_connect_namespace,
            )?;
            let prior = services.insert(
                service.name().to_owned(),
                EcsServiceOperations {
                    container: descriptor.container,
                    scheduling: descriptor.scheduling,
                    capacity_provider: descriptor.capacity_provider,
                    ports: descriptor.ports,
                },
            );
            if prior.is_some() {
                bail!(
                    "ECS operations context contains duplicate service `{}`",
                    service.name()
                );
            }
        }

        if services.is_empty() {
            bail!("ECS operations context contains no realized EcsWorkload services");
        }

        Ok(Self {
            deployment: context.deployment().clone(),
            region: region.expect("a non-empty ECS service set admits a region"),
            cluster: cluster.expect("a non-empty ECS service set admits a cluster"),
            service_connect_namespace: service_connect_namespace
                .expect("a non-empty ECS service set admits a Service Connect namespace"),
            services,
        })
    }

    /// Admitted deployment identity associated with these coordinates.
    pub fn deployment(&self) -> &DeploymentRef {
        &self.deployment
    }

    /// Single AWS region admitted across all realized ECS workloads.
    pub fn region(&self) -> &str {
        &self.region
    }

    /// Single ECS cluster admitted across all realized ECS workloads.
    pub fn cluster(&self) -> &str {
        &self.cluster
    }

    /// Authored Service Connect namespace, independent of private DNS.
    pub fn service_connect_namespace(&self) -> &str {
        &self.service_connect_namespace
    }

    /// Look up one realized service's live-operation coordinates.
    pub fn service(&self, name: &str) -> Option<&EcsServiceOperations> {
        self.services.get(name)
    }

    /// Number of realized ECS workload services in this context.
    pub fn service_count(&self) -> usize {
        self.services.len()
    }
}

impl EcsServiceOperations {
    /// Primary container selected for logs, exec, and port forwarding.
    pub fn container(&self) -> &str {
        &self.container
    }

    /// Definition-authored scheduling policy used to validate scale dimensions.
    pub fn scheduling(&self) -> &EcsScheduling {
        &self.scheduling
    }

    /// Exact capacity-provider name derived for this workload.
    pub fn capacity_provider(&self) -> &str {
        &self.capacity_provider
    }

    /// Declared primary-container ports as `(name, port, protocol)` tuples.
    pub fn ports(&self) -> impl Iterator<Item = (&str, u16, &str)> {
        self.ports.iter().map(|port| {
            (
                port.name.as_str(),
                port.container_port,
                port.protocol.as_str(),
            )
        })
    }
}

fn validate_descriptor(service: &str, descriptor: &WorkloadDescriptor) -> Result<()> {
    for (field, value) in [
        ("region", descriptor.region.as_str()),
        ("cluster", descriptor.cluster.as_str()),
        (
            "service_connect_namespace",
            descriptor.service_connect_namespace.as_str(),
        ),
        ("container", descriptor.container.as_str()),
        ("capacity_provider", descriptor.capacity_provider.as_str()),
    ] {
        if value.trim().is_empty() {
            bail!("ECS workload `{service}` has an empty operations `{field}`");
        }
    }
    let mut port_names = std::collections::BTreeSet::new();
    for port in &descriptor.ports {
        if port.name.trim().is_empty()
            || port.protocol.trim().is_empty()
            || port.container_port == 0
        {
            bail!("ECS workload `{service}` has an invalid operations port descriptor");
        }
        if !port_names.insert(&port.name) {
            bail!(
                "ECS workload `{service}` has duplicate operations port `{}`",
                port.name
            );
        }
    }
    Ok(())
}

fn admit_shared_coordinate(
    field: &str,
    service: &str,
    candidate: &str,
    admitted: &mut Option<String>,
) -> Result<()> {
    if let Some(existing) = admitted {
        if existing != candidate {
            bail!(
                "ECS workload `{service}` uses {field} `{candidate}` but this operations context already admitted `{existing}`"
            );
        }
    } else {
        *admitted = Some(candidate.to_owned());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use tokeira_deploy_engine::Service as _;
    use tokeira_platform::declaration::OperationalService;

    use super::*;
    use crate::EcsConfig;

    fn definition_context() -> DefinitionOperationsContext {
        let mut config = EcsConfig {
            region: "eu-north-1".to_owned(),
            ..EcsConfig::default()
        };
        config.cluster.name = "authored-cluster".to_owned();
        config.cluster.service_connect_namespace = "mesh.example".to_owned();
        let services = EcsWorkload::build_all(&config)
            .into_iter()
            .take(2)
            .map(|service| {
                OperationalService::new(
                    service.resource_type(),
                    service.name(),
                    service
                        .operations_metadata()
                        .expect("ECS workloads publish operations metadata"),
                )
            })
            .collect();
        DefinitionOperationsContext::new(
            DeploymentRef {
                name: "demo".to_owned(),
                dir: "/deployments/demo".into(),
            },
            services,
        )
    }

    #[test]
    fn authored_shared_coordinates_survive_workload_realization() {
        let context = EcsOperationsContext::from_definition(&definition_context())
            .expect("consistent ECS descriptors admit");

        assert_eq!(context.deployment.name, "demo");
        assert_eq!(context.region, "eu-north-1");
        assert_eq!(context.cluster, "authored-cluster");
        assert_eq!(context.service_connect_namespace, "mesh.example");
        assert_eq!(context.services.len(), 2);
        assert_eq!(
            context.services["tokeira-edge-api"].container,
            "tokeira-edge-api"
        );
        assert_eq!(
            context.services["tokeira-edge-api"].capacity_provider,
            "cp-edge-api"
        );
        assert!(matches!(
            context.services["tokeira-edge-api"].scheduling,
            EcsScheduling::Replica { .. }
        ));
        assert!(
            context.services["tokeira-edge-api"]
                .ports
                .iter()
                .any(|port| port.name == "grpc" && port.container_port == 7233)
        );
    }

    #[test]
    fn descriptor_is_an_explicit_non_secret_allowlist() {
        let context = definition_context();
        let descriptor = context.services()[0]
            .attributes()
            .as_object()
            .expect("descriptor is an object");

        assert_eq!(
            descriptor
                .keys()
                .map(String::as_str)
                .collect::<std::collections::BTreeSet<_>>(),
            std::collections::BTreeSet::from([
                "capacity_provider",
                "cluster",
                "container",
                "ports",
                "region",
                "scheduling",
                "service_connect_namespace",
            ])
        );
    }

    #[test]
    fn mixed_service_connect_namespaces_are_refused() {
        let context = definition_context();
        let mut services = context.services().to_vec();
        let mut attributes = services[1].attributes().clone();
        attributes["service_connect_namespace"] = "other.example".into();
        services[1] =
            OperationalService::new(services[1].resource_type(), services[1].name(), attributes);
        let mixed = DefinitionOperationsContext::new(context.deployment().clone(), services);

        let error = EcsOperationsContext::from_definition(&mixed)
            .expect_err("one operation cannot span namespaces");
        assert!(error.to_string().contains("Service Connect namespace"));
    }

    #[test]
    fn unknown_descriptor_fields_are_refused() {
        let context = definition_context();
        let mut services = context.services().to_vec();
        let mut attributes = services[0].attributes().clone();
        attributes["secret"] = "must-not-cross".into();
        services[0] =
            OperationalService::new(services[0].resource_type(), services[0].name(), attributes);
        let unsafe_context =
            DefinitionOperationsContext::new(context.deployment().clone(), services);

        let error = EcsOperationsContext::from_definition(&unsafe_context)
            .expect_err("the closed descriptor rejects additions");
        assert!(error.to_string().contains("invalid operations descriptor"));
    }
}
