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

use crate::services::{EcsScheduling, PlacementConstraint, ServiceConnectSpec, TaskDefinitionSpec};

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
}

/// One manifest, classified.
#[derive(Debug)]
enum Manifest {
    TaskDefinition(TaskDefinitionManifest),
    Service(ServiceManifest),
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
    /// The refusal comes before any client construction: mixed regions in
    /// one call, or a region differing from the instance's admitted one,
    /// never reach AWS.
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

    /// The admitted region's client bundle, built once on first use.
    async fn clients(&self, region: &str) -> &tokeira_aws::AwsClients {
        let (_, clients) = self
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
        clients
    }

    async fn ensure_service(
        &self,
        manifest: &ServiceManifest,
        clients: &tokeira_aws::AwsClients,
    ) -> Result<(), deploy_engine::RuntimeError> {
        let existing = self.describe_service(manifest, clients).await?;
        match existing {
            Some(_) => {
                let mut update = clients
                    .ecs
                    .update_service()
                    .cluster(&manifest.cluster)
                    .service(&manifest.service)
                    .task_definition(&manifest.service);
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
                    .capacity_provider_strategy(
                        aws_sdk_ecs::types::CapacityProviderStrategyItem::builder()
                            .capacity_provider(&manifest.capacity_provider)
                            .weight(1)
                            .build()
                            .map_err(|error| {
                                runtime_error(format!(
                                    "ecs:CapacityProviderStrategyItem build: {error}"
                                ))
                            })?,
                    );
                for constraint in &manifest.placement_constraints {
                    create = create.placement_constraints(
                        aws_sdk_ecs::types::PlacementConstraint::builder()
                            .r#type(aws_sdk_ecs::types::PlacementConstraintType::MemberOf)
                            .expression(&constraint.expression)
                            .build(),
                    );
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

#[async_trait::async_trait]
impl deploy_engine::Platform for EcsPlatform {
    async fn apply_manifests(
        &self,
        manifests: &[serde_json::Value],
    ) -> Result<usize, deploy_engine::RuntimeError> {
        let (parsed, region) = self.parse_and_admit(manifests)?;
        let clients = self.clients(&region).await;
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
        let clients = self.clients(&region).await;
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
/// The SDK call mirrors `TaskDefinitionResource::create` in
/// `tokeira-aws/src/resources/ecs_service.rs` — the correctness reference —
/// with role ARNs already resolved from the infrastructure state while the
/// workload produced this self-describing manifest. The builder helpers below
/// remain mirrored from the same file because they are private there.
async fn register_task_definition(
    clients: &tokeira_aws::AwsClients,
    manifest: &TaskDefinitionManifest,
) -> Result<(), deploy_engine::RuntimeError> {
    let spec = crate::modules::services::to_aws_task_definition(&manifest.spec, None, None);
    if requires_execution_role(&spec) && manifest.execution_role_arn.is_none() {
        tracing::warn!(
            service = %manifest.service,
            task_definition = %spec.family,
            "task definition uses ECS-agent-side features but its deploy \
             manifest carries no execution role"
        );
    }
    let containers = spec
        .containers
        .iter()
        .map(container_definition)
        .collect::<Result<Vec<_>, _>>()?;
    let volumes = spec.volumes.iter().map(volume).collect::<Vec<_>>();
    let mut request = clients
        .ecs
        .register_task_definition()
        .family(&spec.family)
        .network_mode(aws_sdk_ecs::types::NetworkMode::Awsvpc)
        .requires_compatibilities(aws_sdk_ecs::types::Compatibility::Ec2)
        .cpu(spec.cpu.to_string())
        .memory(spec.memory_mb.to_string())
        .set_container_definitions(Some(containers))
        .set_volumes(Some(volumes));
    if let Some(task_role_arn) = &manifest.task_role_arn {
        request = request.task_role_arn(task_role_arn);
    }
    if let Some(execution_role_arn) = &manifest.execution_role_arn {
        request = request.execution_role_arn(execution_role_arn);
    }
    request.send().await.map_err(|error| {
        runtime_error(format!(
            "ecs:RegisterTaskDefinition for service {} (family {}): {}",
            manifest.service,
            spec.family,
            error.into_service_error()
        ))
    })?;
    Ok(())
}

fn requires_execution_role(spec: &aws_ecs::TaskDefinitionSpec) -> bool {
    spec.containers
        .iter()
        .any(|container| !container.secrets.is_empty() || container.image.contains(".dkr.ecr."))
}

fn container_definition(
    spec: &aws_ecs::ContainerSpec,
) -> Result<aws_sdk_ecs::types::ContainerDefinition, deploy_engine::RuntimeError> {
    let mut builder = aws_sdk_ecs::types::ContainerDefinition::builder()
        .name(&spec.name)
        .image(&spec.image)
        .essential(spec.essential)
        .cpu(spec.cpu as i32)
        .memory(spec.memory_mb as i32)
        .set_command((!spec.command.is_empty()).then_some(spec.command.clone()))
        .set_port_mappings(Some(
            spec.port_mappings
                .iter()
                .map(port_mapping)
                .collect::<Vec<_>>(),
        ))
        .set_mount_points(Some(
            spec.mount_points
                .iter()
                .map(mount_point)
                .collect::<Vec<_>>(),
        ))
        .set_environment(Some(
            spec.environment
                .iter()
                .map(|environment| {
                    aws_sdk_ecs::types::KeyValuePair::builder()
                        .name(&environment.name)
                        .value(&environment.value)
                        .build()
                })
                .collect(),
        ))
        .set_secrets(Some(
            spec.secrets
                .iter()
                .map(|secret| {
                    aws_sdk_ecs::types::Secret::builder()
                        .name(&secret.name)
                        .value_from(&secret.value_from)
                        .build()
                        .map_err(|error| runtime_error(format!("ecs:Secret build: {error}")))
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))
        .set_depends_on(Some(
            spec.depends_on
                .iter()
                .map(container_dependency)
                .collect::<Result<Vec<_>, _>>()?,
        ));
    if spec.init_process_enabled {
        builder = builder.linux_parameters(
            aws_sdk_ecs::types::LinuxParameters::builder()
                .init_process_enabled(true)
                .build(),
        );
    }
    Ok(builder.build())
}

fn port_mapping(spec: &aws_ecs::PortMappingSpec) -> aws_sdk_ecs::types::PortMapping {
    aws_sdk_ecs::types::PortMapping::builder()
        .name(&spec.name)
        .container_port(spec.container_port as i32)
        .protocol(aws_sdk_ecs::types::TransportProtocol::Tcp)
        .build()
}

fn mount_point(spec: &aws_ecs::MountPointSpec) -> aws_sdk_ecs::types::MountPoint {
    aws_sdk_ecs::types::MountPoint::builder()
        .source_volume(&spec.source_volume)
        .container_path(&spec.container_path)
        .read_only(spec.read_only)
        .build()
}

fn container_dependency(
    spec: &aws_ecs::ContainerDependencySpec,
) -> Result<aws_sdk_ecs::types::ContainerDependency, deploy_engine::RuntimeError> {
    aws_sdk_ecs::types::ContainerDependency::builder()
        .container_name(&spec.container_name)
        .condition(match spec.condition.as_str() {
            "SUCCESS" => aws_sdk_ecs::types::ContainerCondition::Success,
            "HEALTHY" => aws_sdk_ecs::types::ContainerCondition::Healthy,
            "COMPLETE" => aws_sdk_ecs::types::ContainerCondition::Complete,
            _ => aws_sdk_ecs::types::ContainerCondition::Start,
        })
        .build()
        .map_err(|error| runtime_error(format!("ecs:ContainerDependency build: {error}")))
}

fn volume(spec: &aws_ecs::VolumeSpec) -> aws_sdk_ecs::types::Volume {
    let mut builder = aws_sdk_ecs::types::Volume::builder().name(&spec.name);
    if let Some(host_path) = &spec.host_path {
        builder = builder.host(
            aws_sdk_ecs::types::HostVolumeProperties::builder()
                .source_path(host_path)
                .build(),
        );
    }
    builder.build()
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

    // Unknown manifest kinds are refused by name.
    #[tokio::test]
    async fn unknown_kind_is_refused() {
        let platform = EcsPlatform::new();
        let error = platform
            .parse_and_admit(&[serde_json::json!({"kind": "helm-chart", "region": "eu-west-2"})])
            .expect_err("unknown kind");
        assert!(error.to_string().contains("helm-chart"), "{error}");
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
