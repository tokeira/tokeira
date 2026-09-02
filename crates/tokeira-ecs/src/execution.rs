//! Platform execution seams for the definition-driven ECS platform:
//! substrate reachability, framework integration, and the deploy-plane
//! manifest applier.
//!
//! Ownership split (single-owner rule): the infrastructure engine realizes
//! everything up to and including cluster capacity and observability
//! artifacts; this module's [`EcsPlatform`] owns the deploy plane — task
//! definition registration, service create/rollout, drift, and deletion —
//! driven entirely by the self-describing manifests [`crate::services`]
//! emits. Infrastructure apply never deploys a workload.

use std::sync::Arc;

use serde::Deserialize;
use tokio::sync::OnceCell;

use tokeira_aws::resources::ecs_service as aws_ecs;
use tokeira_deploy_engine as deploy_engine;
use tokeira_platform::declaration::{DeploymentRef, PlatformExecution, PlatformIntegration};

use crate::services::{
    EcsScheduling, LoadBalancerSpec, NetworkSpec, PlacementConstraint, ServiceConnectSpec,
    TaskDefinitionSpec,
};

/// Substrate reachability for ECS.
#[derive(Debug)]
pub struct EcsExecution;

#[async_trait::async_trait]
impl PlatformExecution for EcsExecution {
    /// Deliberately `Ok(None)`: ECS has no single authoritative substrate
    /// probe — reachability is per-operation (every AWS call carries its own
    /// provider failure with the operation's own region). Probing here would
    /// require loading a client bundle before the definition's region is
    /// known, and a wrong-region probe would report a fact about the wrong
    /// substrate.
    async fn probe(
        &self,
        _deployment: &DeploymentRef,
    ) -> anyhow::Result<Option<tokeira_iac::PlatformIssue>> {
        Ok(None)
    }
}

/// Framework integration for the ECS declaration.
#[derive(Debug)]
pub struct EcsIntegration;

#[async_trait::async_trait]
impl PlatformIntegration for EcsIntegration {
    /// No platform extensions: the declaration includes the `tokeira_aws`
    /// namespace, which is the framework's signal to install the standard
    /// deployment-scoped `AwsClients` bundle (with the authored `aws.region`)
    /// before this delegation runs. Registering another bundle here would
    /// duplicate the framework's region hierarchy.
    async fn register_infra_extensions(
        &self,
        _deployment: &DeploymentRef,
        _ctx: &mut tokeira_iac::ProvisionContext,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn register_deploy_extensions(
        &self,
        _deployment: &DeploymentRef,
        _ctx: &mut deploy_engine::ServiceContext,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn register_image_extensions(
        &self,
        _deployment: &DeploymentRef,
        _ctx: &mut deploy_engine::ImageContext,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn service_platform(
        &self,
        _deployment: &DeploymentRef,
    ) -> anyhow::Result<Box<dyn deploy_engine::Platform>> {
        Ok(Box::new(EcsPlatform::new()))
    }
}

/// The deploy-plane manifest applier.
///
/// Region discipline: the platform instance admits exactly one region, taken
/// from the manifests themselves (every manifest carries `region`), and
/// builds one lazy `AwsClients` bundle for it. Manifests naming a different
/// region — within one call or across calls on the same instance — are
/// refused before any client exists, so an operation can never run against
/// the wrong region.
#[derive(Debug)]
pub struct EcsPlatform {
    /// Admitted region and its lazily-built clients; set exactly once.
    clients: Arc<OnceCell<(String, tokeira_aws::AwsClients)>>,
}

impl Default for EcsPlatform {
    fn default() -> Self {
        Self::new()
    }
}

/// The parsed, validated shape of one `ecs-task-definition` manifest.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskDefinitionManifest {
    #[allow(dead_code, reason = "the discriminant is consumed by classification")]
    kind: String,
    service: String,
    region: String,
    spec: TaskDefinitionSpec,
    #[serde(default)]
    task_role_arn: Option<String>,
    #[serde(default)]
    execution_role_arn: Option<String>,
}

/// The parsed, validated shape of one `ecs-service` manifest.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServiceManifest {
    #[allow(dead_code, reason = "the discriminant is consumed by classification")]
    kind: String,
    service: String,
    region: String,
    cluster: String,
    scheduling: EcsScheduling,
    capacity_provider: String,
    service_connect: ServiceConnectSpec,
    #[serde(default)]
    placement_constraints: Vec<PlacementConstraint>,
    #[serde(default)]
    enable_execute_command: bool,
    #[serde(default)]
    network: Option<NetworkSpec>,
    #[serde(default)]
    load_balancer: Option<LoadBalancerSpec>,
}

/// One manifest, classified.
#[derive(Debug)]
enum Manifest {
    TaskDefinition(TaskDefinitionManifest),
    Service(Box<ServiceManifest>),
}

impl Manifest {
    fn region(&self) -> &str {
        match self {
            Manifest::TaskDefinition(manifest) => &manifest.region,
            Manifest::Service(manifest) => &manifest.region,
        }
    }
}

fn runtime_error(message: impl Into<String>) -> deploy_engine::RuntimeError {
    deploy_engine::RuntimeError::Platform(message.into())
}

impl EcsPlatform {
    pub fn new() -> Self {
        Self {
            clients: Arc::new(OnceCell::new()),
        }
    }

    /// Parse and classify one manifest document, refusing unknown kinds.
    fn parse(manifest: &serde_json::Value) -> Result<Manifest, deploy_engine::RuntimeError> {
        let kind = manifest
            .get("kind")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| runtime_error("ECS manifest carries no `kind`"))?;
        match kind {
            "ecs-task-definition" => serde_json::from_value(manifest.clone())
                .map(Manifest::TaskDefinition)
                .map_err(|error| runtime_error(format!("ecs-task-definition manifest: {error}"))),
            "ecs-service" => serde_json::from_value(manifest.clone())
                .map(Box::new)
                .map(Manifest::Service)
                .map_err(|error| runtime_error(format!("ecs-service manifest: {error}"))),
            other => Err(runtime_error(format!(
                "unknown ECS manifest kind `{other}`; this platform applies \
                 ecs-task-definition and ecs-service"
            ))),
        }
    }

    /// Parse every manifest and admit their single region.
    ///
    /// Mixed regions in one call are refused before client construction. The
    /// existing-cell check is an early refusal; [`Self::clients`] repeats it
    /// after one concurrent initializer wins, which is the authoritative
    /// admission point before any service operation reaches AWS.
    fn parse_and_admit(
        &self,
        manifests: &[serde_json::Value],
    ) -> Result<(Vec<Manifest>, String), deploy_engine::RuntimeError> {
        let parsed = manifests
            .iter()
            .map(Self::parse)
            .collect::<Result<Vec<_>, _>>()?;
        let mut regions: Vec<&str> = parsed.iter().map(Manifest::region).collect();
        regions.sort_unstable();
        regions.dedup();
        let region = match regions.as_slice() {
            [] => return Err(runtime_error("no ECS manifests to apply")),
            [single] => (*single).to_string(),
            several => {
                return Err(runtime_error(format!(
                    "ECS manifests disagree on the region: {}; one deployment \
                     admits one region",
                    several.join(", ")
                )));
            }
        };
        if let Some((admitted, _)) = self.clients.get()
            && *admitted != region
        {
            return Err(runtime_error(format!(
                "this deployment's ECS platform is bound to region {admitted}; \
                 manifests for {region} are refused"
            )));
        }
        Ok((parsed, region))
    }

    /// Return the admitted region's client bundle, built once on first use.
    ///
    /// Checking the winning region after `get_or_init` closes the first-use
    /// race: two callers may both observe an empty cell, but only the caller
    /// whose region initialized it can receive the shared bundle.
    async fn clients(
        &self,
        region: &str,
    ) -> Result<&tokeira_aws::AwsClients, deploy_engine::RuntimeError> {
        let (admitted, clients) = self
            .clients
            .get_or_init(|| async {
                let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                    .region(aws_config::Region::new(region.to_string()))
                    .load()
                    .await;
                (
                    region.to_string(),
                    tokeira_aws::AwsClients::new(&sdk_config),
                )
            })
            .await;
        if admitted != region {
            return Err(runtime_error(format!(
                "this deployment's ECS platform is bound to region {admitted}; \
                 manifests for {region} are refused"
            )));
        }
        Ok(clients)
    }

    async fn ensure_service(
        &self,
        manifest: &ServiceManifest,
        clients: &tokeira_aws::AwsClients,
    ) -> Result<(), deploy_engine::RuntimeError> {
        let existing = self.describe_service(manifest, clients).await?;
        match existing {
            Some(_) => {
                let capacity_provider = capacity_provider_strategy(manifest)?;
                let mut update = clients
                    .ecs
                    .update_service()
                    .cluster(&manifest.cluster)
                    .service(&manifest.service)
                    .task_definition(&manifest.service)
                    .force_new_deployment(true)
                    .enable_execute_command(manifest.enable_execute_command)
                    .set_capacity_provider_strategy(Some(vec![capacity_provider]))
                    .set_network_configuration(network_configuration(manifest.network.as_ref())?)
                    .set_load_balancers(load_balancers(manifest.load_balancer.as_ref()))
                    .service_connect_configuration(service_connect_configuration(
                        &manifest.service_connect,
                    )?);
                for constraint in &manifest.placement_constraints {
                    update = update.placement_constraints(placement_constraint(constraint));
                }
                if let EcsScheduling::Replica { desired_count } = manifest.scheduling {
                    update = update.desired_count(desired_count as i32);
                }
                update.send().await.map_err(|error| {
                    runtime_error(format!(
                        "updating service {}: {}",
                        manifest.service,
                        error.into_service_error()
                    ))
                })?;
            }
            None => {
                let capacity_provider = capacity_provider_strategy(manifest)?;
                let mut create = clients
                    .ecs
                    .create_service()
                    .cluster(&manifest.cluster)
                    .service_name(&manifest.service)
                    .task_definition(&manifest.service)
                    .enable_execute_command(manifest.enable_execute_command)
                    .service_connect_configuration(service_connect_configuration(
                        &manifest.service_connect,
                    )?)
                    .capacity_provider_strategy(capacity_provider)
                    .set_network_configuration(network_configuration(manifest.network.as_ref())?)
                    .set_load_balancers(load_balancers(manifest.load_balancer.as_ref()));
                for constraint in &manifest.placement_constraints {
                    create = create.placement_constraints(placement_constraint(constraint));
                }
                create = match manifest.scheduling {
                    EcsScheduling::Replica { desired_count } => create
                        .scheduling_strategy(aws_sdk_ecs::types::SchedulingStrategy::Replica)
                        .desired_count(desired_count as i32),
                    EcsScheduling::Daemon => {
                        create.scheduling_strategy(aws_sdk_ecs::types::SchedulingStrategy::Daemon)
                    }
                };
                create.send().await.map_err(|error| {
                    runtime_error(format!(
                        "creating service {}: {}",
                        manifest.service,
                        error.into_service_error()
                    ))
                })?;
            }
        }
        Ok(())
    }

    async fn describe_service(
        &self,
        manifest: &ServiceManifest,
        clients: &tokeira_aws::AwsClients,
    ) -> Result<Option<aws_sdk_ecs::types::Service>, deploy_engine::RuntimeError> {
        let described = clients
            .ecs
            .describe_services()
            .cluster(&manifest.cluster)
            .services(&manifest.service)
            .send()
            .await
            .map_err(|error| {
                runtime_error(format!(
                    "describing service {}: {}",
                    manifest.service,
                    error.into_service_error()
                ))
            })?;
        Ok(described
            .services
            .unwrap_or_default()
            .into_iter()
            .find(|service| {
                // ECS reports INACTIVE records for deleted services; absent
                // and inactive are the same fact at this boundary.
                service.status().is_none_or(|status| status != "INACTIVE")
            }))
    }
}

fn capacity_provider_strategy(
    manifest: &ServiceManifest,
) -> Result<aws_sdk_ecs::types::CapacityProviderStrategyItem, deploy_engine::RuntimeError> {
    aws_sdk_ecs::types::CapacityProviderStrategyItem::builder()
        .capacity_provider(&manifest.capacity_provider)
        .weight(1)
        .build()
        .map_err(|error| runtime_error(format!("ecs:CapacityProviderStrategyItem build: {error}")))
}

fn placement_constraint(
    constraint: &PlacementConstraint,
) -> aws_sdk_ecs::types::PlacementConstraint {
    aws_sdk_ecs::types::PlacementConstraint::builder()
        .r#type(aws_sdk_ecs::types::PlacementConstraintType::MemberOf)
        .expression(&constraint.expression)
        .build()
}

fn network_configuration(
    spec: Option<&NetworkSpec>,
) -> Result<Option<aws_sdk_ecs::types::NetworkConfiguration>, deploy_engine::RuntimeError> {
    let Some(spec) = spec else {
        return Ok(None);
    };
    let awsvpc = aws_sdk_ecs::types::AwsVpcConfiguration::builder()
        .set_subnets(Some(spec.subnets.clone()))
        .set_security_groups(Some(spec.security_groups.clone()))
        .assign_public_ip(if spec.assign_public_ip {
            aws_sdk_ecs::types::AssignPublicIp::Enabled
        } else {
            aws_sdk_ecs::types::AssignPublicIp::Disabled
        })
        .build()
        .map_err(|error| runtime_error(format!("ecs:AwsVpcConfiguration build: {error}")))?;
    Ok(Some(
        aws_sdk_ecs::types::NetworkConfiguration::builder()
            .awsvpc_configuration(awsvpc)
            .build(),
    ))
}

fn load_balancers(
    spec: Option<&LoadBalancerSpec>,
) -> Option<Vec<aws_sdk_ecs::types::LoadBalancer>> {
    spec.map(|spec| {
        vec![
            aws_sdk_ecs::types::LoadBalancer::builder()
                .target_group_arn(&spec.target_group_arn)
                .container_name(&spec.container_name)
                .container_port(spec.container_port as i32)
                .build(),
        ]
    })
}

fn service_matches_manifest(
    live: &aws_sdk_ecs::types::Service,
    desired: &ServiceManifest,
    latest_task_definition: &str,
) -> bool {
    if live.task_definition() != Some(latest_task_definition)
        || live.enable_execute_command() != desired.enable_execute_command
    {
        return false;
    }
    let scheduling_matches = match desired.scheduling {
        EcsScheduling::Replica { desired_count } => {
            live.scheduling_strategy() == Some(&aws_sdk_ecs::types::SchedulingStrategy::Replica)
                && live.desired_count() == desired_count as i32
        }
        EcsScheduling::Daemon => {
            live.scheduling_strategy() == Some(&aws_sdk_ecs::types::SchedulingStrategy::Daemon)
        }
    };
    if !scheduling_matches {
        return false;
    }
    let capacity = live.capacity_provider_strategy();
    if capacity.len() != 1
        || capacity[0].capacity_provider() != desired.capacity_provider
        || capacity[0].weight() != 1
    {
        return false;
    }
    if !network_matches(live.network_configuration(), desired.network.as_ref())
        || !load_balancers_match(live.load_balancers(), desired.load_balancer.as_ref())
        || !placement_constraints_match(
            live.placement_constraints(),
            &desired.placement_constraints,
        )
    {
        return false;
    }
    let service_connect = live
        .deployments()
        .iter()
        .find(|deployment| deployment.task_definition() == Some(latest_task_definition))
        .and_then(aws_sdk_ecs::types::Deployment::service_connect_configuration);
    service_connect_matches(service_connect, &desired.service_connect)
}

fn network_matches(
    live: Option<&aws_sdk_ecs::types::NetworkConfiguration>,
    desired: Option<&NetworkSpec>,
) -> bool {
    match (
        live.and_then(|network| network.awsvpc_configuration()),
        desired,
    ) {
        (None, None) => true,
        (Some(live), Some(desired)) => {
            string_sets_match(live.subnets(), &desired.subnets)
                && string_sets_match(live.security_groups(), &desired.security_groups)
                && live.assign_public_ip()
                    == Some(if desired.assign_public_ip {
                        &aws_sdk_ecs::types::AssignPublicIp::Enabled
                    } else {
                        &aws_sdk_ecs::types::AssignPublicIp::Disabled
                    })
        }
        _ => false,
    }
}

fn string_sets_match(live: &[String], desired: &[String]) -> bool {
    let mut live = live.to_vec();
    live.sort_unstable();
    let mut desired = desired.to_vec();
    desired.sort_unstable();
    live == desired
}

fn load_balancers_match(
    live: &[aws_sdk_ecs::types::LoadBalancer],
    desired: Option<&LoadBalancerSpec>,
) -> bool {
    match desired {
        None => live.is_empty(),
        Some(desired) => {
            live.len() == 1
                && live[0].target_group_arn() == Some(desired.target_group_arn.as_str())
                && live[0].container_name() == Some(desired.container_name.as_str())
                && live[0].container_port() == Some(desired.container_port as i32)
        }
    }
}

fn placement_constraints_match(
    live: &[aws_sdk_ecs::types::PlacementConstraint],
    desired: &[PlacementConstraint],
) -> bool {
    let mut live = live
        .iter()
        .map(|constraint| {
            (
                constraint.r#type().map(ToString::to_string),
                constraint.expression().map(str::to_owned),
            )
        })
        .collect::<Vec<_>>();
    live.sort_unstable();
    let mut desired = desired
        .iter()
        .map(|constraint| {
            (
                Some(aws_sdk_ecs::types::PlacementConstraintType::MemberOf.to_string()),
                Some(constraint.expression.clone()),
            )
        })
        .collect::<Vec<_>>();
    desired.sort_unstable();
    live == desired
}

fn service_connect_matches(
    live: Option<&aws_sdk_ecs::types::ServiceConnectConfiguration>,
    desired: &ServiceConnectSpec,
) -> bool {
    let Some(live) = live else {
        return false;
    };
    let mut desired_ports = Vec::new();
    if let Some(grpc) = &desired.grpc {
        desired_ports.push(grpc);
    }
    desired_ports.push(&desired.metrics);
    live.enabled()
        && live.services().len() == desired_ports.len()
        && desired_ports.iter().all(|desired| {
            live.services().iter().any(|service| {
                service.port_name() == desired.port_name
                    && service.discovery_name() == Some(desired.discovery_name.as_str())
                    && service.client_aliases().len() == 1
                    && service.client_aliases()[0].port() == desired.container_port as i32
                    && service.client_aliases()[0].dns_name() == Some(desired.dns_name.as_str())
            })
        })
}

#[async_trait::async_trait]
impl deploy_engine::Platform for EcsPlatform {
    async fn apply_manifests(
        &self,
        manifests: &[serde_json::Value],
    ) -> Result<usize, deploy_engine::RuntimeError> {
        let (parsed, region) = self.parse_and_admit(manifests)?;
        let clients = self.clients(&region).await?;
        let mut applied = 0;
        // Task definitions first: a service manifest references its family
        // by name and must land on the freshest revision.
        for manifest in &parsed {
            if let Manifest::TaskDefinition(task_definition) = manifest {
                register_task_definition(clients, task_definition).await?;
                applied += 1;
            }
        }
        for manifest in &parsed {
            if let Manifest::Service(service) = manifest {
                self.ensure_service(service, clients).await?;
                applied += 1;
            }
        }
        Ok(applied)
    }

    async fn is_service_current(
        &self,
        service_name: &str,
        manifests: &[serde_json::Value],
    ) -> bool {
        let (parsed, region) = match self.parse_and_admit(manifests) {
            Ok(admitted) => admitted,
            Err(error) => {
                tracing::warn!(service = service_name, %error, "ECS drift check refused manifests");
                return false;
            }
        };
        let Some(desired) = parsed.iter().find_map(|manifest| match manifest {
            Manifest::Service(service) if service.service == service_name => Some(service),
            _ => None,
        }) else {
            return false;
        };
        let clients = match self.clients(&region).await {
            Ok(clients) => clients,
            Err(error) => {
                tracing::warn!(service = service_name, %error, "ECS drift check refused region");
                return false;
            }
        };
        let live = match self.describe_service(desired, clients).await {
            Ok(Some(live)) => live,
            Ok(None) => return false,
            Err(error) => {
                tracing::warn!(service = service_name, %error, "ECS drift check could not describe service");
                return false;
            }
        };
        let latest = match clients
            .ecs
            .describe_task_definition()
            .task_definition(service_name)
            .send()
            .await
        {
            Ok(output) => output
                .task_definition()
                .and_then(|task| task.task_definition_arn())
                .map(str::to_owned),
            Err(error) => {
                tracing::warn!(
                    service = service_name,
                    error = %error.into_service_error(),
                    "ECS drift check could not describe the latest task definition"
                );
                return false;
            }
        };
        latest
            .as_deref()
            .is_some_and(|latest| service_matches_manifest(&live, desired, latest))
    }

    fn supports_delete(&self) -> bool {
        true
    }

    async fn delete_service(
        &self,
        name: &str,
        manifests: &[serde_json::Value],
    ) -> Result<(), deploy_engine::RuntimeError> {
        let (parsed, region) = self.parse_and_admit(manifests)?;
        let Some(service) = parsed.iter().find_map(|manifest| match manifest {
            Manifest::Service(service) if service.service == name => Some(service),
            _ => None,
        }) else {
            // Nothing describes this service — deletion of the undescribed
            // is complete by definition (idempotent boundary).
            return Ok(());
        };
        let clients = self.clients(&region).await?;
        if self.describe_service(service, clients).await?.is_none() {
            // Already gone: deletion is idempotent at this boundary.
            return Ok(());
        }
        clients
            .ecs
            .delete_service()
            .cluster(&service.cluster)
            .service(name)
            .force(true)
            .send()
            .await
            .map_err(|error| {
                runtime_error(format!(
                    "deleting service {name}: {}",
                    error.into_service_error()
                ))
            })?;
        Ok(())
    }
}

/// Register one task-definition revision from a deploy manifest.
///
/// Role ARNs were resolved from infrastructure state when the workload
/// produced this self-describing manifest. SDK translation remains owned by
/// `tokeira-aws`, so infrastructure and deployment cannot drift apart.
async fn register_task_definition(
    clients: &tokeira_aws::AwsClients,
    manifest: &TaskDefinitionManifest,
) -> Result<(), deploy_engine::RuntimeError> {
    let spec = crate::modules::services::to_aws_task_definition(&manifest.spec, None, None);
    aws_ecs::register_task_definition(
        clients,
        &spec,
        manifest.task_role_arn.as_deref(),
        manifest.execution_role_arn.as_deref(),
    )
    .await
    .map_err(|error| {
        runtime_error(format!(
            "registering task definition for service {} (family {}): {error}",
            manifest.service, spec.family
        ))
    })?;
    Ok(())
}

fn service_connect_configuration(
    spec: &ServiceConnectSpec,
) -> Result<aws_sdk_ecs::types::ServiceConnectConfiguration, deploy_engine::RuntimeError> {
    let mut services = Vec::new();
    if let Some(grpc) = &spec.grpc {
        services.push(service_connect_service(grpc)?);
    }
    services.push(service_connect_service(&spec.metrics)?);
    Ok(aws_sdk_ecs::types::ServiceConnectConfiguration::builder()
        .enabled(true)
        .set_services(Some(services))
        .build())
}

fn service_connect_service(
    port: &crate::services::ServiceConnectPort,
) -> Result<aws_sdk_ecs::types::ServiceConnectService, deploy_engine::RuntimeError> {
    aws_sdk_ecs::types::ServiceConnectService::builder()
        .port_name(&port.port_name)
        .discovery_name(&port.discovery_name)
        .client_aliases(
            aws_sdk_ecs::types::ServiceConnectClientAlias::builder()
                .port(port.container_port as i32)
                .dns_name(&port.dns_name)
                .build()
                .map_err(|error| {
                    runtime_error(format!("ecs:ServiceConnectClientAlias build: {error}"))
                })?,
        )
        .build()
        .map_err(|error| runtime_error(format!("ecs:ServiceConnectService build: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task_definition_manifest(region: &str) -> serde_json::Value {
        serde_json::json!({
            "kind": "ecs-task-definition",
            "service": "tokeira-runtime",
            "region": region,
            "spec": {
                "family": "tokeira-runtime",
                "cpu": 1024,
                "memory_mb": 2048,
                "containers": [],
                "volumes": [],
            },
        })
    }

    fn desired_service() -> ServiceManifest {
        serde_json::from_value(serde_json::json!({
            "kind": "ecs-service",
            "service": "tokeira-edge-api",
            "region": "eu-west-2",
            "cluster": "tokeira",
            "scheduling": { "replica": { "desired_count": 2 } },
            "capacity_provider": "cp-edge-api",
            "service_connect": {
                "grpc": {
                    "port_name": "grpc",
                    "container_port": 7233,
                    "discovery_name": "tokeira-edge-api",
                    "dns_name": "tokeira-edge-api"
                },
                "metrics": {
                    "port_name": "metrics",
                    "container_port": 9090,
                    "discovery_name": "tokeira-edge-api-metrics",
                    "dns_name": "tokeira-edge-api-metrics"
                }
            },
            "placement_constraints": [{
                "type": "memberOf",
                "expression": "attribute:workload == edge-api"
            }],
            "enable_execute_command": true,
            "network": {
                "subnets": ["subnet-b", "subnet-a"],
                "security_groups": ["sg-edge"],
                "assign_public_ip": false
            },
            "load_balancer": {
                "target_group_arn": "arn:aws:elasticloadbalancing:tg/edge",
                "container_name": "tokeira-edge-api",
                "container_port": 7233
            }
        }))
        .expect("service manifest")
    }

    fn live_service(
        desired: &ServiceManifest,
        task_definition: &str,
    ) -> aws_sdk_ecs::types::Service {
        let service_connect = service_connect_configuration(&desired.service_connect)
            .expect("service connect configuration");
        let deployment = aws_sdk_ecs::types::Deployment::builder()
            .task_definition(task_definition)
            .service_connect_configuration(service_connect)
            .build();
        let mut service = aws_sdk_ecs::types::Service::builder()
            .status("ACTIVE")
            .task_definition(task_definition)
            .enable_execute_command(desired.enable_execute_command)
            .scheduling_strategy(aws_sdk_ecs::types::SchedulingStrategy::Replica)
            .desired_count(2)
            .capacity_provider_strategy(
                capacity_provider_strategy(desired).expect("capacity strategy"),
            )
            .network_configuration(
                network_configuration(desired.network.as_ref())
                    .expect("network builds")
                    .expect("network present"),
            )
            .deployments(deployment);
        for load_balancer in
            load_balancers(desired.load_balancer.as_ref()).expect("load balancer present")
        {
            service = service.load_balancers(load_balancer);
        }
        for constraint in &desired.placement_constraints {
            service = service.placement_constraints(placement_constraint(constraint));
        }
        service.build()
    }

    // Mixed regions are refused before any client could exist, naming both.
    #[tokio::test]
    async fn mixed_regions_are_refused() {
        let platform = EcsPlatform::new();
        let error = platform
            .parse_and_admit(&[
                task_definition_manifest("eu-west-2"),
                task_definition_manifest("us-east-1"),
            ])
            .expect_err("mixed regions");
        let message = error.to_string();
        assert!(message.contains("eu-west-2"), "{message}");
        assert!(message.contains("us-east-1"), "{message}");
    }

    // A consistent set admits its single region.
    #[tokio::test]
    async fn consistent_region_is_admitted() {
        let platform = EcsPlatform::new();
        let (parsed, region) = platform
            .parse_and_admit(&[task_definition_manifest("eu-west-2")])
            .expect("consistent set");
        assert_eq!(region, "eu-west-2");
        assert_eq!(parsed.len(), 1);
    }

    #[test]
    fn task_definition_manifest_carries_resolved_role_arns() {
        let mut source = task_definition_manifest("eu-west-2");
        source["task_role_arn"] = serde_json::json!("arn:aws:iam::1:role/runtime-task");
        source["execution_role_arn"] = serde_json::json!("arn:aws:iam::1:role/runtime-execution");

        let Manifest::TaskDefinition(manifest) = EcsPlatform::parse(&source).expect("manifest")
        else {
            panic!("task definition classified as a service")
        };

        assert_eq!(
            manifest.task_role_arn.as_deref(),
            Some("arn:aws:iam::1:role/runtime-task")
        );
        assert_eq!(
            manifest.execution_role_arn.as_deref(),
            Some("arn:aws:iam::1:role/runtime-execution")
        );
    }

    // Drift comparison covers the complete service-owned topology and ignores
    // provider ordering of subnets.
    #[test]
    fn live_service_matches_network_load_balancer_and_rollout_contract() {
        let desired = desired_service();
        let latest = "arn:aws:ecs:eu-west-2:1:task-definition/tokeira-edge-api:7";
        let live = live_service(&desired, latest);

        assert!(service_matches_manifest(&live, &desired, latest));
        assert!(!service_matches_manifest(
            &live,
            &desired,
            "arn:aws:ecs:eu-west-2:1:task-definition/tokeira-edge-api:8"
        ));
    }

    #[test]
    fn live_service_with_wrong_target_group_is_drifted() {
        let desired = desired_service();
        let latest = "arn:aws:ecs:eu-west-2:1:task-definition/tokeira-edge-api:7";
        let mut live = live_service(&desired, latest);
        live.load_balancers = Some(vec![
            aws_sdk_ecs::types::LoadBalancer::builder()
                .target_group_arn("arn:aws:elasticloadbalancing:tg/wrong")
                .container_name("tokeira-edge-api")
                .container_port(7233)
                .build(),
        ]);

        assert!(!service_matches_manifest(&live, &desired, latest));
    }

    // Unknown manifest kinds are refused by name.
    #[tokio::test]
    async fn unknown_kind_is_refused() {
        let platform = EcsPlatform::new();
        let error = platform
            .parse_and_admit(&[serde_json::json!({"kind": "helm-chart", "region": "eu-west-2"})])
            .expect_err("unknown kind");
        assert!(error.to_string().contains("helm-chart"), "{error}");
    }

    #[tokio::test]
    async fn initialized_client_bundle_refuses_a_different_region() {
        let platform = EcsPlatform::new();
        let sdk_config = aws_config::SdkConfig::builder()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new("eu-west-2"))
            .build();
        assert!(
            platform
                .clients
                .set((
                    "eu-west-2".to_owned(),
                    tokeira_aws::AwsClients::new(&sdk_config),
                ))
                .is_ok()
        );

        let Err(error) = platform.clients("us-east-1").await else {
            panic!("different region must be refused after initialization");
        };
        assert!(error.to_string().contains("eu-west-2"), "{error}");
        assert!(error.to_string().contains("us-east-1"), "{error}");
    }

    // Deleting a service no manifest describes is complete by definition —
    // the idempotent boundary needs no AWS call and therefore no region.
    #[tokio::test]
    async fn deleting_the_undescribed_is_idempotent() {
        use tokeira_deploy_engine::Platform as _;

        let platform = EcsPlatform::new();
        platform
            .delete_service("tokeira-unknown", &[task_definition_manifest("eu-west-2")])
            .await
            .expect("no-op delete");
    }
}
