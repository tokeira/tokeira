//! EC2 instance resource.
//!
//! Provisions a single EC2 instance with user-data, waits for it to reach
//! `running` using the SDK waiter, and optionally attaches EBS volumes.

use std::{collections::HashMap, time::Duration};

use aws_sdk_ec2::{
    client::Waiters as Ec2Waiters,
    types::{
        BlockDeviceMapping, EbsBlockDevice, IamInstanceProfileSpecification,
        InstanceNetworkInterfaceSpecification, InstanceType, ResourceType as Ec2ResourceType,
        VolumeType,
    },
};
use tokeira_iac::{
    InternalChange, ProvisionContext, Resource, ResourceId, ResourceState, ResourceType,
    error::IacError,
};

use crate::ResourceContext;

/// Volume attachment specification.
#[derive(Debug, Clone)]
pub struct VolumeAttachment {
    /// Resource ID of the EBS volume to attach.
    pub volume_resource_id: ResourceId,
    /// Device name (e.g. "/dev/sdf").
    pub device: String,
}

/// Configuration for a single EC2 instance.
#[derive(Debug)]
pub struct Ec2InstanceConfig {
    pub instance_type: String,
    pub ami_id: String,
    pub subnet_id: String,
    pub security_group_resource_id: ResourceId,
    pub instance_profile_resource_id: ResourceId,
    pub instance_profile_name: String,
    pub root_volume_gib: u32,
    pub user_data_base64: String,
    pub associate_public_ip: bool,
    /// EBS volumes to attach after the instance is running.
    pub volume_attachments: Vec<VolumeAttachment>,
    pub module: String,
}

/// Generic provider resource that provisions one EC2 instance.
#[derive(Debug)]
pub struct Ec2Instance {
    pub name: String,
    pub config: Ec2InstanceConfig,
    pub project: String,
    pub region: String,
    pub tags: HashMap<String, String>,
}

impl Ec2Instance {
    pub fn new(name: String, config: Ec2InstanceConfig, rctx: &ResourceContext) -> Self {
        Self {
            name,
            config,
            project: rctx.project.clone(),
            region: rctx.region.clone(),
            tags: rctx.tags.clone(),
        }
    }
}

#[async_trait::async_trait]
impl Resource for Ec2Instance {
    fn resource_type(&self) -> ResourceType {
        ResourceType::new("Ec2Instance")
    }

    fn resource_id(&self) -> ResourceId {
        ResourceId(format!("ec2-instance-{}", self.name))
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        let mut deps = vec![
            self.config.instance_profile_resource_id.clone(),
            self.config.security_group_resource_id.clone(),
        ];
        for attachment in &self.config.volume_attachments {
            deps.push(attachment.volume_resource_id.clone());
        }
        deps
    }

    fn module(&self) -> &str {
        &self.config.module
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        let clients = ctx.extension::<crate::AwsClients>().expect("AwsClients");
        let tags = ctx.resource_tags(&self.name);

        // Resolve security group physical ID from state
        let sg_id = ctx
            .get_resource_state(&self.config.security_group_resource_id)
            .map(|s| s.physical_id.clone())
            .map_err(|_| {
                IacError::AwsSdk("security group not found in state — was it created?".to_string())
            })?;

        let run_output = clients
            .ec2
            .run_instances()
            .image_id(&self.config.ami_id)
            .instance_type(InstanceType::from(self.config.instance_type.as_str()))
            .min_count(1)
            .max_count(1)
            .iam_instance_profile(
                IamInstanceProfileSpecification::builder()
                    .name(&self.config.instance_profile_name)
                    .build(),
            )
            .network_interfaces(
                InstanceNetworkInterfaceSpecification::builder()
                    .device_index(0)
                    .subnet_id(&self.config.subnet_id)
                    .groups(&sg_id)
                    .associate_public_ip_address(self.config.associate_public_ip)
                    .build(),
            )
            .block_device_mappings(
                BlockDeviceMapping::builder()
                    .device_name("/dev/sda1")
                    .ebs(
                        EbsBlockDevice::builder()
                            .volume_size(self.config.root_volume_gib as i32)
                            .volume_type(VolumeType::Gp3)
                            .encrypted(true)
                            .delete_on_termination(true)
                            .build(),
                    )
                    .build(),
            )
            .tag_specifications(
                aws_sdk_ec2::types::TagSpecification::builder()
                    .resource_type(Ec2ResourceType::Instance)
                    .set_tags(Some(super::ec2_tags(&tags)))
                    .build(),
            )
            .user_data(&self.config.user_data_base64)
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!(
                    "failed to launch instance: {:?}",
                    e.into_service_error()
                ))
            })?;

        let instance_id = run_output
            .instances()
            .first()
            .and_then(|i| i.instance_id())
            .ok_or_else(|| IacError::AwsSdk("RunInstances did not return instance ID".to_string()))?
            .to_string();

        // Wait for instance to reach running
        clients
            .ec2
            .wait_until_instance_running()
            .instance_ids(&instance_id)
            .wait(Duration::from_secs(300))
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!("instance {instance_id} did not reach running: {e}"))
            })?;

        // Attach EBS volumes
        for attachment in &self.config.volume_attachments {
            let vol_id = ctx
                .get_resource_state(&attachment.volume_resource_id)
                .map(|s| s.physical_id.clone())
                .unwrap_or_default();

            if !vol_id.is_empty() {
                clients
                    .ec2
                    .attach_volume()
                    .instance_id(&instance_id)
                    .volume_id(&vol_id)
                    .device(&attachment.device)
                    .send()
                    .await
                    .map_err(|e| {
                        IacError::AwsSdk(format!(
                            "failed to attach volume {vol_id} to {instance_id}: {}",
                            e.into_service_error()
                        ))
                    })?;
            }
        }

        Ok(ResourceState {
            resource_type: self.resource_type(),
            physical_id: instance_id,
            properties: serde_json::json!({
                "instance_type": self.config.instance_type,
                "ami_id": self.config.ami_id,
                "subnet_id": self.config.subnet_id,
                "security_group_id": sg_id,
            }),
            dependencies: self.dependencies(),
            module: self.config.module.clone(),
            created_at: chrono::Utc::now().to_rfc3339(),
            updated_at: chrono::Utc::now().to_rfc3339(),
        })
    }

    async fn update(
        &self,
        current: &ResourceState,
        _ctx: &ProvisionContext,
    ) -> Result<ResourceState, IacError> {
        // Instance properties are immutable — destroy + recreate for changes
        Ok(current.clone())
    }

    async fn delete(
        &self,
        current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<(), IacError> {
        let clients = ctx.extension::<crate::AwsClients>().expect("AwsClients");
        let instance_id = &current.physical_id;

        clients
            .ec2
            .terminate_instances()
            .instance_ids(instance_id)
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!(
                    "failed to terminate instance {instance_id}: {}",
                    e.into_service_error()
                ))
            })?;

        // Wait for termination
        clients
            .ec2
            .wait_until_instance_terminated()
            .instance_ids(instance_id)
            .wait(Duration::from_secs(300))
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!("instance {instance_id} did not terminate: {e}"))
            })?;

        Ok(())
    }

    async fn describe(&self, ctx: &ProvisionContext) -> Result<Option<ResourceState>, IacError> {
        let physical_id = ctx
            .get_resource_state(&self.resource_id())
            .ok()
            .map(|s| s.physical_id.clone());

        let Some(instance_id) = physical_id else {
            return Ok(None);
        };

        let clients = ctx.extension::<crate::AwsClients>().expect("AwsClients");

        match clients
            .ec2
            .describe_instances()
            .instance_ids(&instance_id)
            .send()
            .await
        {
            Ok(resp) => {
                let instance = resp
                    .reservations()
                    .iter()
                    .flat_map(|r| r.instances())
                    .next();

                match instance {
                    Some(i) => {
                        let state = i
                            .state()
                            .and_then(|s| s.name())
                            .map(|n| n.as_str().to_string())
                            .unwrap_or_default();

                        if state == "terminated" {
                            return Ok(None);
                        }

                        Ok(Some(ResourceState {
                            resource_type: self.resource_type(),
                            physical_id: instance_id,
                            properties: serde_json::json!({
                                "instance_type": i.instance_type().map(|t| t.as_str()).unwrap_or_default(),
                                "state": state,
                            }),
                            dependencies: self.dependencies(),
                            module: self.config.module.clone(),
                            created_at: String::new(),
                            updated_at: chrono::Utc::now().to_rfc3339(),
                        }))
                    }
                    None => Ok(None),
                }
            }
            Err(e) => {
                let msg = format!("{}", e.into_service_error());
                if msg.contains("InvalidInstanceID.NotFound") {
                    Ok(None)
                } else {
                    Err(IacError::AwsSdk(format!(
                        "failed to describe instance {instance_id}: {msg}"
                    )))
                }
            }
        }
    }

    fn diff(&self, _current: &ResourceState, _ctx: &ProvisionContext) -> InternalChange {
        InternalChange::NoChange {
            resource_id: self.resource_id(),
        }
    }
}
