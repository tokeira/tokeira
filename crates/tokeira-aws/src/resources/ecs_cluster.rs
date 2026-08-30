use base64::{Engine as _, engine::general_purpose::STANDARD};
use tokeira_iac::{
    ChangeKind, ChangeSemantics, Citation, Confidence, DescribeResult, Disruption, InternalChange,
    ProvisionContext, Resource, ResourceId, ResourceState, ResourceType, SemanticsContext,
    error::IacError,
};

/// ECS cluster with ECS Exec configured for private Session Manager access.
#[derive(Debug)]
pub struct EcsClusterResource {
    pub(crate) cluster_name: String,
    pub(crate) service_connect_namespace: String,
    pub(crate) module: String,
}

impl EcsClusterResource {
    pub fn new(
        cluster_name: String,
        service_connect_namespace: String,
        module: impl Into<String>,
    ) -> Self {
        Self {
            cluster_name,
            service_connect_namespace,
            module: module.into(),
        }
    }

    fn state(&self, cluster_arn: String) -> ResourceState {
        let now = chrono::Utc::now().to_rfc3339();
        ResourceState {
            resource_type: self.resource_type(),
            physical_id: cluster_arn.clone(),
            properties: serde_json::json!({
                "cluster_name": self.cluster_name,
                "cluster_arn": cluster_arn,
                "execute_command_logging": "NONE",
                "service_connect_namespace": self.service_connect_namespace,
            }),
            dependencies: self.dependencies(),
            created_at: now.clone(),
            updated_at: now,
            module: self.module.clone(),
        }
    }
}

#[async_trait::async_trait]
impl Resource for EcsClusterResource {
    fn change_semantics(&self, ctx: &SemanticsContext<'_>) -> ChangeSemantics {
        const CREATE: Citation = Citation::code(concat!(
            module_path!(),
            "::create — ecs:CreateCluster with container-insights and tags"
        ));
        const UPDATE: Citation = Citation::code(concat!(
            module_path!(),
            "::update — a recorded no-op: cluster settings are fixed at create \
             and `diff` answers NoChange"
        ));
        const DELETE: Citation = Citation::code(concat!(
            module_path!(),
            "::delete — ecs:DeleteCluster (services and capacity are separate \
             resources, deleted first by dependency order)"
        ));
        super::control_plane_semantics(ctx.kind, CREATE, UPDATE, DELETE)
    }

    fn resource_type(&self) -> ResourceType {
        ResourceType::new("EcsCluster")
    }

    fn resource_id(&self) -> ResourceId {
        ResourceId("ecs:cluster".to_owned())
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        vec![]
    }

    fn module(&self) -> &str {
        &self.module
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        if let DescribeResult::Present(existing) = self.describe(ctx).await? {
            return Ok(existing);
        }
        let service_connect_defaults =
            aws_sdk_ecs::types::ClusterServiceConnectDefaultsRequest::builder()
                .namespace(&self.service_connect_namespace)
                .build()
                .map_err(|e| {
                    IacError::AwsSdk(format!("ecs:ClusterServiceConnectDefaults build: {e}"))
                })?;
        let output = ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ecs
            .create_cluster()
            .cluster_name(&self.cluster_name)
            .configuration(
                aws_sdk_ecs::types::ClusterConfiguration::builder()
                    .execute_command_configuration(
                        aws_sdk_ecs::types::ExecuteCommandConfiguration::builder()
                            .logging(aws_sdk_ecs::types::ExecuteCommandLogging::None)
                            .build(),
                    )
                    .build(),
            )
            .service_connect_defaults(service_connect_defaults)
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!("ecs:CreateCluster: {}", e.into_service_error()))
            })?;
        let cluster_arn = output
            .cluster()
            .and_then(|cluster| cluster.cluster_arn())
            .unwrap_or(&self.cluster_name)
            .to_owned();
        Ok(self.state(cluster_arn))
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
        ctx.extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ecs
            .delete_cluster()
            .cluster(&current.physical_id)
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!("ecs:DeleteCluster: {}", e.into_service_error()))
            })?;
        Ok(())
    }

    async fn describe(&self, ctx: &ProvisionContext) -> Result<DescribeResult, IacError> {
        let output = ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ecs
            .describe_clusters()
            .clusters(&self.cluster_name)
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!("ecs:DescribeClusters: {}", e.into_service_error()))
            })?;
        Ok(output
            .clusters()
            .first()
            .and_then(|cluster| cluster.cluster_arn())
            .map(|arn| DescribeResult::Present(self.state(arn.to_owned())))
            .unwrap_or(DescribeResult::Absent))
    }

    fn diff(&self, _current: &ResourceState, _ctx: &ProvisionContext) -> InternalChange {
        InternalChange::NoChange {
            resource_id: self.resource_id(),
        }
    }
}

/// EC2 launch template for one ECS capacity provider plane.
#[derive(Debug)]
pub struct LaunchTemplateResource {
    pub name: String,
    pub cluster_name: String,
    pub instance_type: String,
    pub workload: String,
    pub instance_profile_name: String,
    pub security_group_dependency: ResourceId,
    pub module: String,
}

impl LaunchTemplateResource {
    pub fn user_data(&self) -> String {
        format!(
            "#!/bin/bash\n\
             echo \"ECS_CLUSTER={}\" >> /etc/ecs/ecs.config\n\
             echo \"ECS_ENABLE_CONTAINER_METADATA=true\" >> /etc/ecs/ecs.config\n\
             echo \"ECS_ENABLE_SPOT_INSTANCE_DRAINING=true\" >> /etc/ecs/ecs.config\n\
             echo \"ECS_IMAGE_PULL_BEHAVIOR=always\" >> /etc/ecs/ecs.config\n\
             echo 'ECS_INSTANCE_ATTRIBUTES={{\"workload\":\"{}\"}}' >> /etc/ecs/ecs.config\n",
            self.cluster_name, self.workload
        )
    }

    async fn resolve_ami(&self, ctx: &ProvisionContext) -> Result<String, IacError> {
        let parameter =
            "/aws/service/ecs/optimized-ami/amazon-linux-2023/arm64/recommended/image_id";
        let output = ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ssm
            .get_parameter()
            .name(parameter)
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!("ssm:GetParameter: {}", e.into_service_error()))
            })?;
        output
            .parameter()
            .and_then(|parameter| parameter.value())
            .map(str::to_owned)
            .ok_or_else(|| IacError::AwsSdk(format!("{parameter} missing value")))
    }

    fn state(&self, template_id: String, ami_id: String) -> ResourceState {
        let now = chrono::Utc::now().to_rfc3339();
        ResourceState {
            resource_type: self.resource_type(),
            physical_id: template_id.clone(),
            properties: serde_json::json!({
                "launch_template_id": template_id,
                "name": self.name,
                "ami_id": ami_id,
                "instance_type": self.instance_type,
                "workload": self.workload,
                "user_data": self.user_data(),
            }),
            dependencies: self.dependencies(),
            created_at: now.clone(),
            updated_at: now,
            module: self.module.clone(),
        }
    }
}

#[async_trait::async_trait]
impl Resource for LaunchTemplateResource {
    fn change_semantics(&self, ctx: &SemanticsContext<'_>) -> ChangeSemantics {
        const CREATE: Citation = Citation::code(concat!(
            module_path!(),
            "::create — ec2:CreateLaunchTemplate with the state-read AMI, \
             instance profile, and security group"
        ));
        const UPDATE: Citation = Citation::code(concat!(
            module_path!(),
            "::update — a recorded no-op: template content is fixed at create \
             and `diff` answers NoChange"
        ));
        const DELETE: Citation = Citation::code(concat!(
            module_path!(),
            "::delete — ec2:DeleteLaunchTemplate by recorded template id"
        ));
        let mut semantics = super::control_plane_semantics(ctx.kind, CREATE, UPDATE, DELETE);
        if matches!(ctx.kind, ChangeKind::Create) {
            semantics.provider_assigned = vec!["launch_template_id".into()];
        }
        semantics
    }

    fn resource_type(&self) -> ResourceType {
        ResourceType::new("LaunchTemplate")
    }

    fn resource_id(&self) -> ResourceId {
        ResourceId(format!("lt-{}", self.name))
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        vec![self.security_group_dependency.clone()]
    }

    fn module(&self) -> &str {
        &self.module
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        if let DescribeResult::Present(existing) = self.describe(ctx).await? {
            return Ok(existing);
        }
        let ami_id = self.resolve_ami(ctx).await?;
        let sg_state = ctx.get_resource_state(&self.security_group_dependency)?;
        let security_group_id = sg_state
            .properties
            .get("security_group_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| IacError::StateNotFound("security_group_id missing".into()))?;
        let output = ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ec2
            .create_launch_template()
            .launch_template_name(&self.name)
            .launch_template_data(
                aws_sdk_ec2::types::RequestLaunchTemplateData::builder()
                    .image_id(&ami_id)
                    .instance_type(aws_sdk_ec2::types::InstanceType::from(self.instance_type.as_str()))
                    .iam_instance_profile(
                        aws_sdk_ec2::types::LaunchTemplateIamInstanceProfileSpecificationRequest::builder()
                            .name(&self.instance_profile_name)
                            .build(),
                    )
                    .security_group_ids(security_group_id)
                    .user_data(STANDARD.encode(self.user_data()))
                    .build(),
            )
            .send()
            .await
            .map_err(|e| IacError::AwsSdk(format!("ec2:CreateLaunchTemplate: {}", e.into_service_error())))?;
        let template_id = output
            .launch_template()
            .and_then(|template| template.launch_template_id())
            .unwrap_or(&self.name)
            .to_owned();
        Ok(self.state(template_id, ami_id))
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
        ctx.extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ec2
            .delete_launch_template()
            .launch_template_id(&current.physical_id)
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!(
                    "ec2:DeleteLaunchTemplate: {}",
                    e.into_service_error()
                ))
            })?;
        Ok(())
    }

    async fn describe(&self, ctx: &ProvisionContext) -> Result<DescribeResult, IacError> {
        let output = match ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ec2
            .describe_launch_templates()
            .launch_template_names(&self.name)
            .send()
            .await
        {
            Ok(output) => output,
            Err(e) => {
                let svc_err = e.into_service_error();
                let message = format!("{svc_err}");
                if message.contains("InvalidLaunchTemplateName.NotFound") {
                    return Ok(DescribeResult::Absent);
                }
                return Err(IacError::AwsSdk(format!(
                    "ec2:DescribeLaunchTemplates: {svc_err}"
                )));
            }
        };
        Ok(output
            .launch_templates()
            .first()
            .and_then(|template| template.launch_template_id())
            .map(|template_id| {
                DescribeResult::Present(self.state(template_id.to_owned(), String::new()))
            })
            .unwrap_or(DescribeResult::Absent))
    }

    fn diff(&self, _current: &ResourceState, _ctx: &ProvisionContext) -> InternalChange {
        InternalChange::NoChange {
            resource_id: self.resource_id(),
        }
    }
}

/// Auto Scaling group backing one ECS capacity provider.
#[derive(Debug)]
pub struct AsgResource {
    pub name: String,
    pub min_size: u32,
    pub desired_capacity: u32,
    pub max_size: u32,
    pub new_instances_protected_from_scale_in: bool,
    pub launch_template_dependency: ResourceId,
    pub vpc_dependency: ResourceId,
    pub module: String,
}

impl AsgResource {
    fn state(&self, arn: String) -> ResourceState {
        let now = chrono::Utc::now().to_rfc3339();
        ResourceState {
            resource_type: self.resource_type(),
            physical_id: arn.clone(),
            properties: serde_json::json!({
                "auto_scaling_group_name": self.name,
                "auto_scaling_group_arn": arn,
                "min_size": self.min_size,
                "desired_capacity": self.desired_capacity,
                "max_size": self.max_size,
                "new_instances_protected_from_scale_in": self.new_instances_protected_from_scale_in,
            }),
            dependencies: self.dependencies(),
            created_at: now.clone(),
            updated_at: now,
            module: self.module.clone(),
        }
    }
}

#[async_trait::async_trait]
impl Resource for AsgResource {
    fn change_semantics(&self, ctx: &SemanticsContext<'_>) -> ChangeSemantics {
        const CREATE: Citation = Citation::code(concat!(
            module_path!(),
            "::create — autoscaling:CreateAutoScalingGroup from the state-read \
             launch template; the group launches its instances"
        ));
        const UPDATE: Citation = Citation::code(concat!(
            module_path!(),
            "::update — a recorded no-op: group shape is fixed at create and \
             `diff` answers NoChange"
        ));
        const DELETE: Citation = Citation::code(concat!(
            module_path!(),
            "::delete — autoscaling:DeleteAutoScalingGroup with \
             force_delete(true): the group's instances are terminated with it, \
             by our own parameter choice"
        ));
        let mut semantics = super::control_plane_semantics(ctx.kind, CREATE, UPDATE, DELETE);
        if matches!(ctx.kind, ChangeKind::Delete) {
            semantics.disruption = Confidence::EngineFact {
                value: Disruption::UnavailableDuringChange,
                citation: DELETE,
            };
        }
        semantics
    }

    fn resource_type(&self) -> ResourceType {
        ResourceType::new("AutoScalingGroup")
    }

    fn resource_id(&self) -> ResourceId {
        ResourceId(format!("asg-{}", self.name))
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        vec![
            self.launch_template_dependency.clone(),
            self.vpc_dependency.clone(),
        ]
    }

    fn module(&self) -> &str {
        &self.module
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        if let DescribeResult::Present(existing) = self.describe(ctx).await? {
            return Ok(existing);
        }
        let lt_state = ctx.get_resource_state(&self.launch_template_dependency)?;
        let vpc_state = ctx.get_resource_state(&self.vpc_dependency)?;
        let subnet_ids: Vec<String> = vpc_state
            .properties
            .get("subnet_ids")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();
        ctx.extension::<crate::AwsClients>()
            .expect("AwsClients")
            .autoscaling
            .create_auto_scaling_group()
            .auto_scaling_group_name(&self.name)
            .min_size(self.min_size as i32)
            .desired_capacity(self.desired_capacity as i32)
            .max_size(self.max_size as i32)
            .new_instances_protected_from_scale_in(self.new_instances_protected_from_scale_in)
            .launch_template(
                aws_sdk_autoscaling::types::LaunchTemplateSpecification::builder()
                    .launch_template_id(&lt_state.physical_id)
                    .version("$Latest")
                    .build(),
            )
            .vpc_zone_identifier(subnet_ids.join(","))
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!(
                    "autoscaling:CreateAutoScalingGroup: {}",
                    e.into_service_error()
                ))
            })?;
        Ok(self.state(self.name.clone()))
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
        ctx: &ProvisionContext,
    ) -> Result<(), IacError> {
        ctx.extension::<crate::AwsClients>()
            .expect("AwsClients")
            .autoscaling
            .delete_auto_scaling_group()
            .auto_scaling_group_name(&self.name)
            .force_delete(true)
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!(
                    "autoscaling:DeleteAutoScalingGroup: {}",
                    e.into_service_error()
                ))
            })?;
        Ok(())
    }

    async fn describe(&self, ctx: &ProvisionContext) -> Result<DescribeResult, IacError> {
        let output = ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .autoscaling
            .describe_auto_scaling_groups()
            .auto_scaling_group_names(&self.name)
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!(
                    "autoscaling:DescribeAutoScalingGroups: {}",
                    e.into_service_error()
                ))
            })?;
        Ok(output
            .auto_scaling_groups()
            .first()
            .map(|asg| {
                DescribeResult::Present(
                    self.state(
                        asg.auto_scaling_group_arn()
                            .unwrap_or(&self.name)
                            .to_owned(),
                    ),
                )
            })
            .unwrap_or(DescribeResult::Absent))
    }

    fn diff(&self, _current: &ResourceState, _ctx: &ProvisionContext) -> InternalChange {
        InternalChange::NoChange {
            resource_id: self.resource_id(),
        }
    }
}

/// ECS capacity provider backed by an Auto Scaling group.
#[derive(Debug)]
pub struct CapacityProviderResource {
    pub name: String,
    pub cluster_dependency: ResourceId,
    pub asg_dependency: ResourceId,
    pub module: String,
}

impl CapacityProviderResource {
    fn state(&self, capacity_provider_arn: String) -> ResourceState {
        let now = chrono::Utc::now().to_rfc3339();
        ResourceState {
            resource_type: self.resource_type(),
            physical_id: capacity_provider_arn.clone(),
            properties: serde_json::json!({
                "capacity_provider_name": self.name,
                "capacity_provider_arn": capacity_provider_arn,
                "managed_termination_protection": "DISABLED",
            }),
            dependencies: self.dependencies(),
            created_at: now.clone(),
            updated_at: now,
            module: self.module.clone(),
        }
    }
}

#[async_trait::async_trait]
impl Resource for CapacityProviderResource {
    fn change_semantics(&self, ctx: &SemanticsContext<'_>) -> ChangeSemantics {
        const CREATE: Citation = Citation::code(concat!(
            module_path!(),
            "::create — ecs:CreateCapacityProvider over the state-read ASG, \
             then ecs:PutClusterCapacityProviders to attach it"
        ));
        const UPDATE: Citation = Citation::code(concat!(
            module_path!(),
            "::update — a recorded no-op: provider settings are fixed at \
             create and `diff` answers NoChange"
        ));
        const DELETE: Citation = Citation::code(concat!(
            module_path!(),
            "::delete — ecs:DeleteCapacityProvider after detaching it from \
             the cluster"
        ));
        super::control_plane_semantics(ctx.kind, CREATE, UPDATE, DELETE)
    }

    fn resource_type(&self) -> ResourceType {
        ResourceType::new("EcsCapacityProvider")
    }

    fn resource_id(&self) -> ResourceId {
        ResourceId(self.name.clone())
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        vec![self.cluster_dependency.clone(), self.asg_dependency.clone()]
    }

    fn module(&self) -> &str {
        &self.module
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        if let DescribeResult::Present(existing) = self.describe(ctx).await? {
            return Ok(existing);
        }
        let asg_state = ctx.get_resource_state(&self.asg_dependency)?;
        let auto_scaling_group_provider = aws_sdk_ecs::types::AutoScalingGroupProvider::builder()
            .auto_scaling_group_arn(&asg_state.physical_id)
            .managed_termination_protection(
                aws_sdk_ecs::types::ManagedTerminationProtection::Disabled,
            )
            .build()
            .map_err(|e| IacError::AwsSdk(format!("ecs:AutoScalingGroupProvider build: {e}")))?;
        let output = ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ecs
            .create_capacity_provider()
            .name(&self.name)
            .auto_scaling_group_provider(auto_scaling_group_provider)
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!(
                    "ecs:CreateCapacityProvider: {}",
                    e.into_service_error()
                ))
            })?;
        let arn = output
            .capacity_provider()
            .and_then(|provider| provider.capacity_provider_arn())
            .unwrap_or(&self.name)
            .to_owned();
        Ok(self.state(arn))
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
        ctx: &ProvisionContext,
    ) -> Result<(), IacError> {
        ctx.extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ecs
            .delete_capacity_provider()
            .capacity_provider(&self.name)
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!(
                    "ecs:DeleteCapacityProvider: {}",
                    e.into_service_error()
                ))
            })?;
        Ok(())
    }

    async fn describe(&self, ctx: &ProvisionContext) -> Result<DescribeResult, IacError> {
        let output = ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ecs
            .describe_capacity_providers()
            .capacity_providers(&self.name)
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!(
                    "ecs:DescribeCapacityProviders: {}",
                    e.into_service_error()
                ))
            })?;
        Ok(output
            .capacity_providers()
            .first()
            .and_then(|provider| provider.capacity_provider_arn())
            .map(|arn| DescribeResult::Present(self.state(arn.to_owned())))
            .unwrap_or(DescribeResult::Absent))
    }

    fn diff(&self, _current: &ResourceState, _ctx: &ProvisionContext) -> InternalChange {
        InternalChange::NoChange {
            resource_id: self.resource_id(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    // All four capacity-plane resources are fixed at create (update is a
    // recorded no-op), so diff answers NoChange even when the recorded state
    // diverges from the desired shape.

    #[test]
    fn cluster_diff_noops_regardless_of_recorded_state() {
        let resource = EcsClusterResource::new("tok-cluster".into(), "tok.internal".into(), "ecs");
        let stale = EcsClusterResource::new("tok-cluster".into(), "stale.internal".into(), "ecs");
        let current = stale.state("arn:cluster".into());

        let change = resource.diff(&current, &ProvisionContext::new("test", HashMap::new()));

        assert!(matches!(
            change,
            InternalChange::NoChange { resource_id } if resource_id == resource.resource_id()
        ));
    }

    #[test]
    fn launch_template_diff_noops_regardless_of_recorded_state() {
        let resource = LaunchTemplateResource {
            name: "tok-lt".into(),
            cluster_name: "tok-cluster".into(),
            instance_type: "m7g.large".into(),
            workload: "engine".into(),
            instance_profile_name: "tok-profile".into(),
            security_group_dependency: ResourceId("sg".into()),
            module: "ecs".into(),
        };
        let stale = LaunchTemplateResource {
            instance_type: "m7g.xlarge".into(),
            workload: "stale".into(),
            ..resource
        };
        let current = stale.state("lt-0abc".into(), "ami-stale".into());
        let resource = LaunchTemplateResource {
            instance_type: "m7g.large".into(),
            workload: "engine".into(),
            ..stale
        };

        let change = resource.diff(&current, &ProvisionContext::new("test", HashMap::new()));

        assert!(matches!(
            change,
            InternalChange::NoChange { resource_id } if resource_id == resource.resource_id()
        ));
    }

    #[test]
    fn asg_diff_noops_regardless_of_recorded_state() {
        let resource = AsgResource {
            name: "tok-asg".into(),
            min_size: 1,
            desired_capacity: 2,
            max_size: 3,
            new_instances_protected_from_scale_in: false,
            launch_template_dependency: ResourceId("lt".into()),
            vpc_dependency: ResourceId("vpc".into()),
            module: "ecs".into(),
        };
        let stale = AsgResource {
            desired_capacity: 9,
            ..resource
        };
        let current = stale.state("arn:asg".into());
        let resource = AsgResource {
            desired_capacity: 2,
            ..stale
        };

        let change = resource.diff(&current, &ProvisionContext::new("test", HashMap::new()));

        assert!(matches!(
            change,
            InternalChange::NoChange { resource_id } if resource_id == resource.resource_id()
        ));
    }

    #[test]
    fn capacity_provider_diff_noops_regardless_of_recorded_state() {
        let resource = CapacityProviderResource {
            name: "tok-cp".into(),
            cluster_dependency: ResourceId("cluster".into()),
            asg_dependency: ResourceId("asg".into()),
            module: "ecs".into(),
        };
        let current = resource.state("arn:cp".into());

        let change = resource.diff(&current, &ProvisionContext::new("test", HashMap::new()));

        assert!(matches!(
            change,
            InternalChange::NoChange { resource_id } if resource_id == resource.resource_id()
        ));
    }
}
