use serde::{Deserialize, Serialize};
use tokeira_iac::{
    InternalChange, ProvisionContext, Resource, ResourceId, ResourceState, ResourceType,
    error::IacError,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EcsScheduling {
    Replica { desired_count: u32 },
    Daemon,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskDefinitionSpec {
    pub family: String,
    pub cpu: u32,
    pub memory_mb: u32,
    pub containers: Vec<ContainerSpec>,
    pub volumes: Vec<VolumeSpec>,
    pub task_role_dependency: Option<ResourceId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerSpec {
    pub name: String,
    pub image: String,
    pub essential: bool,
    pub cpu: u32,
    pub memory_mb: u32,
    pub command: Vec<String>,
    pub port_mappings: Vec<PortMappingSpec>,
    pub mount_points: Vec<MountPointSpec>,
    pub depends_on: Vec<ContainerDependencySpec>,
    pub init_process_enabled: bool,
    pub secrets: Vec<SecretSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretSpec {
    pub name: String,
    pub value_from: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortMappingSpec {
    pub name: String,
    pub container_port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MountPointSpec {
    pub source_volume: String,
    pub container_path: String,
    pub read_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerDependencySpec {
    pub container_name: String,
    pub condition: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VolumeSpec {
    pub name: String,
    pub host_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceConnectSpec {
    pub grpc: Option<ServiceConnectPort>,
    pub metrics: ServiceConnectPort,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceConnectPort {
    pub port_name: String,
    pub container_port: u16,
    pub discovery_name: String,
    pub dns_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementConstraintSpec {
    pub expression: String,
}

#[derive(Debug)]
pub struct TaskDefinitionResource {
    pub spec: TaskDefinitionSpec,
    pub module: String,
}

#[async_trait::async_trait]
impl Resource for TaskDefinitionResource {
    fn resource_type(&self) -> ResourceType {
        ResourceType::new("EcsTaskDefinition")
    }

    fn resource_id(&self) -> ResourceId {
        ResourceId(format!("task-definition:{}", self.spec.family))
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        self.spec.task_role_dependency.clone().into_iter().collect()
    }

    fn module(&self) -> &str {
        &self.module
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        let containers = self
            .spec
            .containers
            .iter()
            .map(container_definition)
            .collect::<Result<Vec<_>, _>>()?;
        let volumes = self
            .spec
            .volumes
            .iter()
            .map(volume)
            .collect::<Result<Vec<_>, _>>()?;
        let mut request = ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ecs
            .register_task_definition()
            .family(&self.spec.family)
            .network_mode(aws_sdk_ecs::types::NetworkMode::Awsvpc)
            .requires_compatibilities(aws_sdk_ecs::types::Compatibility::Ec2)
            .cpu(self.spec.cpu.to_string())
            .memory(self.spec.memory_mb.to_string())
            .set_container_definitions(Some(containers))
            .set_volumes(Some(volumes));
        if let Some(role_dependency) = &self.spec.task_role_dependency {
            let role_state = ctx.get_resource_state(role_dependency)?;
            let role_arn = role_state
                .properties
                .get("role_arn")
                .and_then(|value| value.as_str())
                .ok_or_else(|| IacError::StateNotFound("role_arn missing".into()))?;
            // ECS Exec and the Alloy config init container both use task-role
            // credentials from inside the running container, not the execution role.
            request = request.task_role_arn(role_arn);
        }
        let output = request.send().await.map_err(|e| {
            IacError::AwsSdk(format!(
                "ecs:RegisterTaskDefinition: {}",
                e.into_service_error()
            ))
        })?;
        let arn = output
            .task_definition()
            .and_then(|definition| definition.task_definition_arn())
            .unwrap_or(&self.spec.family)
            .to_owned();
        Ok(task_definition_state(self, arn))
    }

    async fn update(
        &self,
        _current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<ResourceState, IacError> {
        self.create(ctx).await
    }

    async fn delete(
        &self,
        current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<(), IacError> {
        ctx.extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ecs
            .deregister_task_definition()
            .task_definition(&current.physical_id)
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!(
                    "ecs:DeregisterTaskDefinition: {}",
                    e.into_service_error()
                ))
            })?;
        Ok(())
    }

    async fn describe(&self, _ctx: &ProvisionContext) -> Result<Option<ResourceState>, IacError> {
        Ok(None)
    }

    fn diff(&self, _current: &ResourceState, _ctx: &ProvisionContext) -> InternalChange {
        InternalChange::Update {
            resource_id: self.resource_id(),
            resource_type: self.resource_type(),
            details: "task definition revisions are immutable".into(),
        }
    }
}

#[derive(Debug)]
pub struct EcsServiceResource {
    pub service_name: String,
    pub scheduling: EcsScheduling,
    pub capacity_provider: String,
    pub service_connect: ServiceConnectSpec,
    pub placement_constraints: Vec<PlacementConstraintSpec>,
    pub cluster_dependency: ResourceId,
    pub task_definition_dependency: ResourceId,
    pub vpc_dependency: ResourceId,
    pub security_group_dependency: ResourceId,
    pub module: String,
}

#[async_trait::async_trait]
impl Resource for EcsServiceResource {
    fn resource_type(&self) -> ResourceType {
        ResourceType::new("EcsService")
    }

    fn resource_id(&self) -> ResourceId {
        ResourceId(format!("ecs-service:{}", self.service_name))
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        vec![
            self.cluster_dependency.clone(),
            self.task_definition_dependency.clone(),
            self.vpc_dependency.clone(),
            self.security_group_dependency.clone(),
        ]
    }

    fn module(&self) -> &str {
        &self.module
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        let cluster = ctx.get_resource_state(&self.cluster_dependency)?;
        let task_definition = ctx.get_resource_state(&self.task_definition_dependency)?;
        let vpc = ctx.get_resource_state(&self.vpc_dependency)?;
        let security_group = ctx.get_resource_state(&self.security_group_dependency)?;
        let subnet_ids: Vec<String> = vpc
            .properties
            .get("subnet_ids")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();
        let security_group_id = security_group
            .properties
            .get("security_group_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| IacError::StateNotFound("security_group_id missing".into()))?;
        let capacity_provider_strategy =
            aws_sdk_ecs::types::CapacityProviderStrategyItem::builder()
                .capacity_provider(&self.capacity_provider)
                .weight(1)
                .build()
                .map_err(|e| {
                    IacError::AwsSdk(format!("ecs:CapacityProviderStrategyItem build: {e}"))
                })?;
        let awsvpc_configuration = aws_sdk_ecs::types::AwsVpcConfiguration::builder()
            .set_subnets(Some(subnet_ids))
            .security_groups(security_group_id)
            .assign_public_ip(aws_sdk_ecs::types::AssignPublicIp::Disabled)
            .build()
            .map_err(|e| IacError::AwsSdk(format!("ecs:AwsVpcConfiguration build: {e}")))?;
        let mut builder = ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ecs
            .create_service()
            .cluster(&cluster.physical_id)
            .service_name(&self.service_name)
            .task_definition(&task_definition.physical_id)
            .enable_execute_command(true)
            .capacity_provider_strategy(capacity_provider_strategy)
            .network_configuration(
                aws_sdk_ecs::types::NetworkConfiguration::builder()
                    .awsvpc_configuration(awsvpc_configuration)
                    .build(),
            )
            .service_connect_configuration(service_connect_configuration(&self.service_connect)?)
            .set_placement_constraints(Some(
                self.placement_constraints
                    .iter()
                    .map(|constraint| {
                        aws_sdk_ecs::types::PlacementConstraint::builder()
                            .r#type(aws_sdk_ecs::types::PlacementConstraintType::MemberOf)
                            .expression(&constraint.expression)
                            .build()
                    })
                    .collect(),
            ));
        match self.scheduling {
            EcsScheduling::Replica { desired_count } => {
                builder = builder
                    .scheduling_strategy(aws_sdk_ecs::types::SchedulingStrategy::Replica)
                    .desired_count(desired_count as i32);
            }
            EcsScheduling::Daemon => {
                builder =
                    builder.scheduling_strategy(aws_sdk_ecs::types::SchedulingStrategy::Daemon);
            }
        }
        let output = builder.send().await.map_err(|e| {
            IacError::AwsSdk(format!("ecs:CreateService: {}", e.into_service_error()))
        })?;
        let arn = output
            .service()
            .and_then(|service| service.service_arn())
            .unwrap_or(&self.service_name)
            .to_owned();
        Ok(service_state(self, arn))
    }

    async fn update(
        &self,
        current: &ResourceState,
        _ctx: &ProvisionContext,
    ) -> Result<ResourceState, IacError> {
        Ok(current.clone())
    }

    async fn delete(
        &self,
        current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<(), IacError> {
        let cluster = ctx.get_resource_state(&self.cluster_dependency)?;
        ctx.extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ecs
            .delete_service()
            .cluster(&cluster.physical_id)
            .service(&current.physical_id)
            .force(true)
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!("ecs:DeleteService: {}", e.into_service_error()))
            })?;
        Ok(())
    }

    async fn describe(&self, _ctx: &ProvisionContext) -> Result<Option<ResourceState>, IacError> {
        Ok(None)
    }

    fn diff(&self, _current: &ResourceState, _ctx: &ProvisionContext) -> InternalChange {
        InternalChange::NoChange {
            resource_id: self.resource_id(),
        }
    }
}

#[derive(Debug)]
pub struct CloudMapNamespaceResource {
    pub name: String,
    pub vpc_dependency: ResourceId,
    pub module: String,
}

#[async_trait::async_trait]
impl Resource for CloudMapNamespaceResource {
    fn resource_type(&self) -> ResourceType {
        ResourceType::new("CloudMapNamespace")
    }

    fn resource_id(&self) -> ResourceId {
        ResourceId("cloudmap:namespace".to_owned())
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        vec![self.vpc_dependency.clone()]
    }

    fn module(&self) -> &str {
        &self.module
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        let vpc = ctx.get_resource_state(&self.vpc_dependency)?;
        let vpc_id = vpc
            .properties
            .get("vpc_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| IacError::StateNotFound("vpc_id missing".into()))?;
        let output = ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .servicediscovery
            .create_private_dns_namespace()
            .name(&self.name)
            .vpc(vpc_id)
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!(
                    "servicediscovery:CreatePrivateDnsNamespace: {}",
                    e.into_service_error()
                ))
            })?;
        let operation_id = output.operation_id().unwrap_or(&self.name).to_owned();
        Ok(namespace_state(self, operation_id))
    }

    async fn update(
        &self,
        current: &ResourceState,
        _ctx: &ProvisionContext,
    ) -> Result<ResourceState, IacError> {
        Ok(current.clone())
    }

    async fn delete(
        &self,
        _current: &ResourceState,
        _ctx: &ProvisionContext,
    ) -> Result<(), IacError> {
        Ok(())
    }

    async fn describe(&self, _ctx: &ProvisionContext) -> Result<Option<ResourceState>, IacError> {
        Ok(None)
    }

    fn diff(&self, _current: &ResourceState, _ctx: &ProvisionContext) -> InternalChange {
        InternalChange::NoChange {
            resource_id: self.resource_id(),
        }
    }
}

fn container_definition(
    spec: &ContainerSpec,
) -> Result<aws_sdk_ecs::types::ContainerDefinition, IacError> {
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
        .set_secrets(Some(
            spec.secrets
                .iter()
                .map(container_secret)
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

fn container_secret(spec: &SecretSpec) -> Result<aws_sdk_ecs::types::Secret, IacError> {
    aws_sdk_ecs::types::Secret::builder()
        .name(&spec.name)
        .value_from(&spec.value_from)
        .build()
        .map_err(|e| IacError::AwsSdk(format!("ecs:Secret build: {e}")))
}

fn port_mapping(spec: &PortMappingSpec) -> aws_sdk_ecs::types::PortMapping {
    aws_sdk_ecs::types::PortMapping::builder()
        .name(&spec.name)
        .container_port(spec.container_port as i32)
        .protocol(aws_sdk_ecs::types::TransportProtocol::Tcp)
        .build()
}

fn mount_point(spec: &MountPointSpec) -> aws_sdk_ecs::types::MountPoint {
    aws_sdk_ecs::types::MountPoint::builder()
        .source_volume(&spec.source_volume)
        .container_path(&spec.container_path)
        .read_only(spec.read_only)
        .build()
}

fn container_dependency(
    spec: &ContainerDependencySpec,
) -> Result<aws_sdk_ecs::types::ContainerDependency, IacError> {
    aws_sdk_ecs::types::ContainerDependency::builder()
        .container_name(&spec.container_name)
        .condition(match spec.condition.as_str() {
            "SUCCESS" => aws_sdk_ecs::types::ContainerCondition::Success,
            "HEALTHY" => aws_sdk_ecs::types::ContainerCondition::Healthy,
            "COMPLETE" => aws_sdk_ecs::types::ContainerCondition::Complete,
            _ => aws_sdk_ecs::types::ContainerCondition::Start,
        })
        .build()
        .map_err(|e| IacError::AwsSdk(format!("ecs:ContainerDependency build: {e}")))
}

fn volume(spec: &VolumeSpec) -> Result<aws_sdk_ecs::types::Volume, IacError> {
    let mut builder = aws_sdk_ecs::types::Volume::builder().name(&spec.name);
    if let Some(host_path) = &spec.host_path {
        builder = builder.host(
            aws_sdk_ecs::types::HostVolumeProperties::builder()
                .source_path(host_path)
                .build(),
        );
    }
    Ok(builder.build())
}

fn service_connect_configuration(
    spec: &ServiceConnectSpec,
) -> Result<aws_sdk_ecs::types::ServiceConnectConfiguration, IacError> {
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
    port: &ServiceConnectPort,
) -> Result<aws_sdk_ecs::types::ServiceConnectService, IacError> {
    let alias = aws_sdk_ecs::types::ServiceConnectClientAlias::builder()
        .dns_name(&port.dns_name)
        .port(port.container_port as i32)
        .build()
        .map_err(|e| IacError::AwsSdk(format!("ecs:ServiceConnectClientAlias build: {e}")))?;
    aws_sdk_ecs::types::ServiceConnectService::builder()
        .port_name(&port.port_name)
        .discovery_name(&port.discovery_name)
        .client_aliases(alias)
        .build()
        .map_err(|e| IacError::AwsSdk(format!("ecs:ServiceConnectService build: {e}")))
}

fn task_definition_state(resource: &TaskDefinitionResource, arn: String) -> ResourceState {
    let now = chrono::Utc::now().to_rfc3339();
    ResourceState {
        resource_type: resource.resource_type(),
        physical_id: arn.clone(),
        properties: serde_json::json!({
            "task_definition_arn": arn,
            "family": resource.spec.family,
            "manifest": resource.spec,
        }),
        dependencies: resource.dependencies(),
        created_at: now.clone(),
        updated_at: now,
        module: resource.module.clone(),
    }
}

fn service_state(resource: &EcsServiceResource, arn: String) -> ResourceState {
    let now = chrono::Utc::now().to_rfc3339();
    ResourceState {
        resource_type: resource.resource_type(),
        physical_id: arn.clone(),
        properties: serde_json::json!({
            "service_arn": arn,
            "service_name": resource.service_name,
            "capacity_provider": resource.capacity_provider,
            "enable_execute_command": true,
        }),
        dependencies: resource.dependencies(),
        created_at: now.clone(),
        updated_at: now,
        module: resource.module.clone(),
    }
}

fn namespace_state(resource: &CloudMapNamespaceResource, operation_id: String) -> ResourceState {
    let now = chrono::Utc::now().to_rfc3339();
    ResourceState {
        resource_type: resource.resource_type(),
        physical_id: operation_id.clone(),
        properties: serde_json::json!({
            "operation_id": operation_id,
            "namespace": resource.name,
        }),
        dependencies: resource.dependencies(),
        created_at: now.clone(),
        updated_at: now,
        module: resource.module.clone(),
    }
}
