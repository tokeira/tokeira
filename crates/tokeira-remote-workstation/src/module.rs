//! Workstation IaC module — composes generic AWS resources into the
//! workstation topology.
//!
//! The module enumerates: IAM role, instance profile, security group,
//! two EBS volumes (cache + repo), and one EC2 instance. Dependencies
//! are declared so the IaC engine creates them in the correct order and
//! destroys them in reverse.
//!
//! Note: We use the generic `EbsVolume` and `Ec2Instance` resources from
//! `tokeira-aws`, but for the security group we use our own implementation
//! because the generic `SecurityGroup` resolves VPC ID from a VPC resource
//! dependency — and the workstation uses a pre-existing default VPC
//! discovered at pre-flight time.

use std::collections::HashMap;
use std::fmt::Debug;

use tokeira_aws::ResourceContext;
use tokeira_aws::resources::ebs_volume::{EbsVolume, EbsVolumeConfig};
use tokeira_aws::resources::ec2_instance::{Ec2Instance, Ec2InstanceConfig, VolumeAttachment};
use tokeira_aws::resources::iam_instance_profile::{IamInstanceProfile, IamInstanceProfileConfig};
use tokeira_aws::resources::iam_role::{IamRole, IamRoleConfig};
use tokeira_iac::{Module, ModuleContext, Resource, ResourceId, error::IacError};

const MODULE_NAME: &str = "workstation";
const MANAGED_POLICY_ARN: &str = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore";

/// Configuration for the workstation module, derived from `WorkstationProfile`
/// plus pre-flight discovery results (subnet, VPC, AZ, AMI).
#[derive(Debug, Clone)]
pub struct WorkstationModuleConfig {
    pub workstation_id: String,
    pub instance_type: String,
    pub ami_id: String,
    pub subnet_id: String,
    pub vpc_id: String,
    pub availability_zone: String,
    pub root_volume_gib: u32,
    pub cache_volume_gib: u32,
    pub repo_volume_gib: u32,
    pub user_data_base64: String,
    pub region: String,
}

/// The workstation IaC module.
#[derive(Debug)]
pub struct WorkstationModule {
    pub config: WorkstationModuleConfig,
    pub rctx: ResourceContext,
}

impl WorkstationModule {
    pub fn new(config: WorkstationModuleConfig, rctx: ResourceContext) -> Self {
        Self { config, rctx }
    }
}

impl Module for WorkstationModule {
    fn name(&self) -> &str {
        MODULE_NAME
    }

    fn dependencies(&self) -> &[&str] {
        &[]
    }

    fn resources(&self, _ctx: &ModuleContext) -> Result<Vec<Box<dyn Resource>>, IacError> {
        let ws_id = &self.config.workstation_id;
        let role_name = format!("tokeira-workstation-{ws_id}-role");
        let profile_name = format!("tokeira-workstation-{ws_id}-profile");
        let sg_name = format!("tokeira-workstation-{ws_id}-sg");
        let cache_vol_name = format!("tokeira-ws-{ws_id}-cache");
        let repo_vol_name = format!("tokeira-ws-{ws_id}-repo");
        let instance_name = format!("tokeira-ws-{ws_id}");

        let trust_policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": { "Service": "ec2.amazonaws.com" },
                "Action": "sts:AssumeRole"
            }]
        })
        .to_string();

        // 1. IAM Role
        let role = IamRole::new(
            role_name.clone(),
            IamRoleConfig {
                trust_policy,
                inline_policies: HashMap::new(),
                managed_policy_arns: vec![MANAGED_POLICY_ARN.to_string()],
                module: MODULE_NAME.to_string(),
            },
            &self.rctx,
        );
        let role_resource_id = role.resource_id();

        // 2. IAM Instance Profile
        let instance_profile = IamInstanceProfile::new(
            profile_name.clone(),
            IamInstanceProfileConfig {
                role_name: role_name.clone(),
                role_dependency: role_resource_id.clone(),
                module: MODULE_NAME.to_string(),
            },
            &self.rctx,
        );

        // 3. Security Group — we use the generic SecurityGroup but need to
        //    handle the VPC dependency. Since we don't create a VPC resource,
        //    we'll use a synthetic "no-dependency" approach by creating a
        //    minimal EbsVolume-style SG resource. For now, use the generic
        //    SecurityGroup with an empty vpc_dependency and override the VPC
        //    resolution in a workstation-specific way.
        //
        //    Actually: the simplest correct approach is to use our own
        //    Ec2Instance-style resource that takes vpc_id directly. But we
        //    already have the generic SecurityGroup. Let's just create a
        //    synthetic VPC state entry — no, that's fragile.
        //
        //    Decision: use the generic SecurityGroup with a dummy dependency
        //    that we pre-seed. The engine won't try to create it because
        //    describe() will find it in state.
        //
        //    Simplest correct path: the workstation module pre-seeds a VPC
        //    state entry. But Module::resources() doesn't have write access
        //    to state.
        //
        //    Final decision: we need a workstation-specific SG resource that
        //    takes vpc_id directly. This is the same pattern as Ec2Instance
        //    taking subnet_id directly. Let's define it inline here.
        let sg = WorkstationSecurityGroup {
            name: sg_name,
            vpc_id: self.config.vpc_id.clone(),
            module: MODULE_NAME.to_string(),
            rctx: self.rctx.clone(),
        };
        let sg_resource_id = sg.resource_id();

        // 4. Cache EBS Volume
        let cache_vol = EbsVolume::new(
            cache_vol_name.clone(),
            EbsVolumeConfig {
                size_gib: self.config.cache_volume_gib,
                availability_zone: self.config.availability_zone.clone(),
                volume_type: "gp3".to_string(),
                encrypted: true,
                module: MODULE_NAME.to_string(),
            },
            &self.rctx,
        );
        let cache_vol_resource_id = cache_vol.resource_id();

        // 5. Repo EBS Volume
        let repo_vol = EbsVolume::new(
            repo_vol_name.clone(),
            EbsVolumeConfig {
                size_gib: self.config.repo_volume_gib,
                availability_zone: self.config.availability_zone.clone(),
                volume_type: "gp3".to_string(),
                encrypted: true,
                module: MODULE_NAME.to_string(),
            },
            &self.rctx,
        );
        let repo_vol_resource_id = repo_vol.resource_id();

        // 6. EC2 Instance
        let instance_profile_resource_id = instance_profile.resource_id();
        let instance = Ec2Instance::new(
            instance_name,
            Ec2InstanceConfig {
                instance_type: self.config.instance_type.clone(),
                ami_id: self.config.ami_id.clone(),
                subnet_id: self.config.subnet_id.clone(),
                security_group_resource_id: sg_resource_id,
                instance_profile_resource_id,
                instance_profile_name: profile_name,
                root_volume_gib: self.config.root_volume_gib,
                user_data_base64: self.config.user_data_base64.clone(),
                associate_public_ip: true,
                volume_attachments: vec![
                    VolumeAttachment {
                        volume_resource_id: cache_vol_resource_id,
                        device: "/dev/sdf".to_string(),
                    },
                    VolumeAttachment {
                        volume_resource_id: repo_vol_resource_id,
                        device: "/dev/sdg".to_string(),
                    },
                ],
                module: MODULE_NAME.to_string(),
            },
            &self.rctx,
        );

        Ok(vec![
            Box::new(role),
            Box::new(instance_profile),
            Box::new(sg),
            Box::new(cache_vol),
            Box::new(repo_vol),
            Box::new(instance),
        ])
    }
}

// ── Workstation-specific security group ──────────────────────────────────────
//
// Takes vpc_id directly rather than resolving from a VPC resource dependency.
// This is necessary because the workstation uses a pre-existing default VPC
// that is not managed by the IaC engine.

#[derive(Debug)]
struct WorkstationSecurityGroup {
    name: String,
    vpc_id: String,
    module: String,
    rctx: ResourceContext,
}

#[async_trait::async_trait]
impl Resource for WorkstationSecurityGroup {
    fn resource_type(&self) -> tokeira_iac::ResourceType {
        tokeira_iac::ResourceType::new("SecurityGroup")
    }

    fn resource_id(&self) -> ResourceId {
        ResourceId(format!("sg-{}", self.name))
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        vec![]
    }

    fn module(&self) -> &str {
        &self.module
    }

    async fn create(
        &self,
        ctx: &tokeira_iac::ProvisionContext,
    ) -> Result<tokeira_iac::ResourceState, IacError> {
        let clients = ctx
            .extension::<tokeira_aws::AwsClients>()
            .expect("AwsClients");
        let tags = ctx.resource_tags(&self.name);

        let output = clients
            .ec2
            .create_security_group()
            .group_name(&self.name)
            .description("Tokeira workstation - zero inbound, all egress, SSM only")
            .vpc_id(&self.vpc_id)
            .tag_specifications(
                aws_sdk_ec2::types::TagSpecification::builder()
                    .resource_type(aws_sdk_ec2::types::ResourceType::SecurityGroup)
                    .set_tags(Some(tokeira_aws::resources::ec2_tags(&tags)))
                    .build(),
            )
            .send()
            .await
            .map_err(|e| {
                IacError::ProviderError(format!(
                    "failed to create security group {}: {}",
                    self.name,
                    e.into_service_error()
                ))
            })?;

        let sg_id = output.group_id().unwrap_or_default().to_string();

        Ok(tokeira_iac::ResourceState {
            resource_type: self.resource_type(),
            physical_id: sg_id,
            properties: serde_json::json!({
                "vpc_id": self.vpc_id,
                "ingress_rules": 0,
            }),
            dependencies: self.dependencies(),
            module: self.module.clone(),
            created_at: chrono::Utc::now().to_rfc3339(),
            updated_at: chrono::Utc::now().to_rfc3339(),
        })
    }

    async fn update(
        &self,
        current: &tokeira_iac::ResourceState,
        _ctx: &tokeira_iac::ProvisionContext,
    ) -> Result<tokeira_iac::ResourceState, IacError> {
        Ok(current.clone())
    }

    async fn delete(
        &self,
        current: &tokeira_iac::ResourceState,
        ctx: &tokeira_iac::ProvisionContext,
    ) -> Result<(), IacError> {
        let clients = ctx
            .extension::<tokeira_aws::AwsClients>()
            .expect("AwsClients");
        let sg_id = &current.physical_id;

        clients
            .ec2
            .delete_security_group()
            .group_id(sg_id)
            .send()
            .await
            .map_err(|e| {
                IacError::ProviderError(format!(
                    "failed to delete security group {sg_id}: {}",
                    e.into_service_error()
                ))
            })?;

        Ok(())
    }

    async fn describe(
        &self,
        ctx: &tokeira_iac::ProvisionContext,
    ) -> Result<Option<tokeira_iac::ResourceState>, IacError> {
        let physical_id = ctx
            .get_resource_state(&self.resource_id())
            .ok()
            .map(|s| s.physical_id.clone());

        let Some(sg_id) = physical_id else {
            return Ok(None);
        };

        let clients = ctx
            .extension::<tokeira_aws::AwsClients>()
            .expect("AwsClients");

        match clients
            .ec2
            .describe_security_groups()
            .group_ids(&sg_id)
            .send()
            .await
        {
            Ok(resp) => {
                if resp.security_groups().is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(tokeira_iac::ResourceState {
                        resource_type: self.resource_type(),
                        physical_id: sg_id,
                        properties: serde_json::json!({
                            "vpc_id": self.vpc_id,
                            "ingress_rules": 0,
                        }),
                        dependencies: self.dependencies(),
                        module: self.module.clone(),
                        created_at: String::new(),
                        updated_at: chrono::Utc::now().to_rfc3339(),
                    }))
                }
            }
            Err(e) => {
                let msg = format!("{}", e.into_service_error());
                if msg.contains("InvalidGroup.NotFound") {
                    Ok(None)
                } else {
                    Err(IacError::ProviderError(format!(
                        "failed to describe security group {sg_id}: {msg}"
                    )))
                }
            }
        }
    }

    fn diff(
        &self,
        _current: &tokeira_iac::ResourceState,
        _ctx: &tokeira_iac::ProvisionContext,
    ) -> tokeira_iac::InternalChange {
        tokeira_iac::InternalChange::NoChange {
            resource_id: self.resource_id(),
        }
    }
}
