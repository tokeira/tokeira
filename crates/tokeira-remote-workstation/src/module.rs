//! Workstation IaC modules — four modules composing generic AWS resources
//! into the workstation topology.
//!
//! Module dependency graph:
//!   identity (no deps)     → IAM Role + Instance Profile
//!   network  (no deps)     → Security Group
//!   storage  (no deps)     → EBS Volume (cache) + EBS Volume (repo)
//!   compute  (depends on: identity, network, storage) → EC2 Instance
//!
//! The engine processes modules in dependency order: identity, network, and
//! storage are created first (in parallel if the engine supports it), then
//! compute. On destroy, compute is deleted first, then the others.

use std::collections::HashMap;
use std::fmt::Debug;

use tokeira_aws::ResourceContext;
use tokeira_aws::resources::ebs_volume::{EbsVolume, EbsVolumeConfig};
use tokeira_aws::resources::iam_instance_profile::{IamInstanceProfile, IamInstanceProfileConfig};
use tokeira_aws::resources::iam_role::{IamRole, IamRoleConfig};
use tokeira_iac::{Module, ModuleContext, Resource, ResourceId, error::IacError};

const MANAGED_POLICY_ARN: &str = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore";

/// Configuration for the workstation modules, derived from `WorkstationProfile`
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

// ── Identity Module ──────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct IdentityModule {
    pub workstation_id: String,
    pub rctx: ResourceContext,
}

impl Module for IdentityModule {
    fn name(&self) -> &str {
        "identity"
    }

    fn dependencies(&self) -> &[&str] {
        &[]
    }

    fn resources(&self, _ctx: &ModuleContext) -> Result<Vec<Box<dyn Resource>>, IacError> {
        let ws_id = &self.workstation_id;
        let role_name = format!("tokeira-workstation-{ws_id}-role");
        let profile_name = format!("tokeira-workstation-{ws_id}-profile");

        let trust_policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": { "Service": "ec2.amazonaws.com" },
                "Action": "sts:AssumeRole"
            }]
        })
        .to_string();

        let role = IamRole::new(
            role_name.clone(),
            IamRoleConfig {
                trust_policy,
                inline_policies: HashMap::new(),
                managed_policy_arns: vec![MANAGED_POLICY_ARN.to_string()],
                module: "identity".to_string(),
            },
            &self.rctx,
        );
        let role_resource_id = role.resource_id();

        let instance_profile = IamInstanceProfile::new(
            profile_name,
            IamInstanceProfileConfig {
                role_name,
                role_dependency: role_resource_id,
                module: "identity".to_string(),
            },
            &self.rctx,
        );

        Ok(vec![Box::new(role), Box::new(instance_profile)])
    }
}

// ── Network Module ───────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct NetworkModule {
    pub workstation_id: String,
    pub vpc_id: String,
    pub rctx: ResourceContext,
}

impl Module for NetworkModule {
    fn name(&self) -> &str {
        "network"
    }

    fn dependencies(&self) -> &[&str] {
        &[]
    }

    fn resources(&self, _ctx: &ModuleContext) -> Result<Vec<Box<dyn Resource>>, IacError> {
        let sg_name = format!(
            "tokeira-workstation-{}-sg",
            self.workstation_id
        );

        let sg = WorkstationSecurityGroup {
            name: sg_name,
            vpc_id: self.vpc_id.clone(),
            module: "network".to_string(),
        };

        Ok(vec![Box::new(sg)])
    }
}

// ── Storage Module ───────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct StorageModule {
    pub workstation_id: String,
    pub cache_volume_gib: u32,
    pub repo_volume_gib: u32,
    pub availability_zone: String,
    pub rctx: ResourceContext,
}

impl StorageModule {
    pub fn cache_volume_resource_id(&self) -> ResourceId {
        ResourceId(format!(
            "ebs-volume-tokeira-ws-{}-cache",
            self.workstation_id
        ))
    }

    pub fn repo_volume_resource_id(&self) -> ResourceId {
        ResourceId(format!(
            "ebs-volume-tokeira-ws-{}-repo",
            self.workstation_id
        ))
    }
}

impl Module for StorageModule {
    fn name(&self) -> &str {
        "storage"
    }

    fn dependencies(&self) -> &[&str] {
        &[]
    }

    fn resources(&self, _ctx: &ModuleContext) -> Result<Vec<Box<dyn Resource>>, IacError> {
        let ws_id = &self.workstation_id;

        let cache_vol = EbsVolume::new(
            format!("tokeira-ws-{ws_id}-cache"),
            EbsVolumeConfig {
                size_gib: self.cache_volume_gib,
                availability_zone: self.availability_zone.clone(),
                volume_type: "gp3".to_string(),
                encrypted: true,
                module: "storage".to_string(),
            },
            &self.rctx,
        );

        let repo_vol = EbsVolume::new(
            format!("tokeira-ws-{ws_id}-repo"),
            EbsVolumeConfig {
                size_gib: self.repo_volume_gib,
                availability_zone: self.availability_zone.clone(),
                volume_type: "gp3".to_string(),
                encrypted: true,
                module: "storage".to_string(),
            },
            &self.rctx,
        );

        Ok(vec![Box::new(cache_vol), Box::new(repo_vol)])
    }
}

// ── Compute Module ───────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct ComputeModule {
    pub config: WorkstationModuleConfig,
    pub rctx: ResourceContext,
}

impl Module for ComputeModule {
    fn name(&self) -> &str {
        "compute"
    }

    fn dependencies(&self) -> &[&str] {
        &["identity", "network", "storage"]
    }

    fn resources(&self, _ctx: &ModuleContext) -> Result<Vec<Box<dyn Resource>>, IacError> {
        let ws_id = &self.config.workstation_id;
        let profile_name = format!("tokeira-workstation-{ws_id}-profile");
        let sg_name = format!("tokeira-workstation-{ws_id}-sg");
        let instance_name = format!("tokeira-ws-{ws_id}");

        let instance_profile_resource_id =
            ResourceId(format!("iam-instance-profile-{profile_name}"));
        let sg_resource_id = ResourceId(format!("sg-{sg_name}"));
        let cache_vol_resource_id =
            ResourceId(format!("ebs-volume-tokeira-ws-{ws_id}-cache"));
        let repo_vol_resource_id =
            ResourceId(format!("ebs-volume-tokeira-ws-{ws_id}-repo"));

        let instance = crate::instance::WorkstationInstance {
            name: instance_name,
            config: crate::instance::WorkstationInstanceConfig {
                workstation_id: ws_id.clone(),
                instance_type: self.config.instance_type.clone(),
                ami_id: self.config.ami_id.clone(),
                subnet_id: self.config.subnet_id.clone(),
                security_group_resource_id: sg_resource_id,
                instance_profile_resource_id,
                instance_profile_name: profile_name,
                root_volume_gib: self.config.root_volume_gib,
                cache_volume_resource_id: cache_vol_resource_id,
                repo_volume_resource_id: repo_vol_resource_id,
                module: "compute".to_string(),
            },
            rctx: self.rctx.clone(),
        };

        Ok(vec![Box::new(instance)])
    }
}

// ── Workstation Security Group (takes vpc_id directly) ───────────────────────

#[derive(Debug)]
struct WorkstationSecurityGroup {
    name: String,
    vpc_id: String,
    module: String,
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

        let sg_id = match clients
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
        {
            Ok(output) => output.group_id().unwrap_or_default().to_string(),
            Err(e) => {
                let svc_err = e.into_service_error();
                let msg = format!("{svc_err}");
                if msg.contains("InvalidGroup.Duplicate") {
                    tracing::warn!(sg = %self.name, "security group already exists, adopting");
                    let desc = clients
                        .ec2
                        .describe_security_groups()
                        .group_names(&self.name)
                        .send()
                        .await
                        .map_err(|e2| {
                            IacError::AwsSdk(format!(
                                "failed to describe existing security group {}: {}",
                                self.name,
                                e2.into_service_error()
                            ))
                        })?;
                    desc.security_groups()
                        .first()
                        .and_then(|sg| sg.group_id())
                        .unwrap_or_default()
                        .to_string()
                } else {
                    return Err(IacError::AwsSdk(format!(
                        "failed to create security group {}: {svc_err}",
                        self.name
                    )));
                }
            }
        };

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
                IacError::AwsSdk(format!(
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
                    Err(IacError::AwsSdk(format!(
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
