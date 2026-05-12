//! Workstation-specific EC2 instance resource.
//!
//! Wraps the generic `Ec2Instance` resource but renders user-data at
//! `create()` time using volume IDs from state. This solves the
//! chicken-and-egg problem: user-data needs volume IDs, but volumes are
//! created by the IaC engine before the instance.

use std::collections::HashMap;
use std::time::Duration;

use aws_sdk_ec2::client::Waiters as Ec2Waiters;
use aws_sdk_ec2::types::{
    BlockDeviceMapping, EbsBlockDevice, IamInstanceProfileSpecification, InstanceType,
    InstanceNetworkInterfaceSpecification, ResourceType as Ec2ResourceType, VolumeType,
};
use base64::Engine as _;
use tokeira_aws::ResourceContext;
use tokeira_iac::{
    InternalChange, ProvisionContext, Resource, ResourceId, ResourceState, ResourceType,
    error::IacError,
};

use crate::bootstrap::{self, BootstrapContext};
use crate::engine::WorkstationProfile;

/// Configuration for the workstation instance.
#[derive(Debug)]
pub struct WorkstationInstanceConfig {
    pub workstation_id: String,
    pub instance_type: String,
    pub ami_id: String,
    pub subnet_id: String,
    pub security_group_resource_id: ResourceId,
    pub instance_profile_resource_id: ResourceId,
    pub instance_profile_name: String,
    pub root_volume_gib: u32,
    pub cache_volume_resource_id: ResourceId,
    pub repo_volume_resource_id: ResourceId,
    pub module: String,
}

#[derive(Debug)]
pub struct WorkstationInstance {
    pub name: String,
    pub config: WorkstationInstanceConfig,
    pub rctx: ResourceContext,
}

#[async_trait::async_trait]
impl Resource for WorkstationInstance {
    fn resource_type(&self) -> ResourceType {
        ResourceType::new("Ec2Instance")
    }

    fn resource_id(&self) -> ResourceId {
        ResourceId(format!("ec2-instance-{}", self.name))
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        vec![
            self.config.instance_profile_resource_id.clone(),
            self.config.security_group_resource_id.clone(),
            self.config.cache_volume_resource_id.clone(),
            self.config.repo_volume_resource_id.clone(),
        ]
    }

    fn module(&self) -> &str {
        &self.config.module
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        let clients = ctx.extension::<tokeira_aws::AwsClients>().expect("AwsClients");
        let tags = ctx.resource_tags(&self.name);

        // Resolve physical IDs from state (volumes and SG created before us)
        let sg_id = ctx
            .get_resource_state(&self.config.security_group_resource_id)
            .map(|s| s.physical_id.clone())
            .map_err(|_| IacError::AwsSdk("security group not in state".to_string()))?;

        let cache_vol_id = ctx
            .get_resource_state(&self.config.cache_volume_resource_id)
            .map(|s| s.physical_id.clone())
            .map_err(|_| IacError::AwsSdk("cache volume not in state".to_string()))?;

        let repo_vol_id = ctx
            .get_resource_state(&self.config.repo_volume_resource_id)
            .map(|s| s.physical_id.clone())
            .map_err(|_| IacError::AwsSdk("repo volume not in state".to_string()))?;

        // Render bootstrap user-data with real volume IDs
        let profile = WorkstationProfile::c8gd_rust();
        let toolchain_toml = crate::engine::read_rust_toolchain_toml();
        let fingerprint = bootstrap::fingerprint(&profile, &toolchain_toml);
        let user_data = bootstrap::render(&BootstrapContext {
            workstation_id: self.config.workstation_id.clone(),
            bootstrap_fingerprint: fingerprint,
            profile,
            cache_volume_id: cache_vol_id.clone(),
            repo_volume_id: repo_vol_id.clone(),
            rust_toolchain_toml: toolchain_toml,
        });
        let user_data_base64 =
            base64::engine::general_purpose::STANDARD.encode(user_data.as_bytes());

        // Launch instance
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
                    .associate_public_ip_address(true)
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
                    .set_tags(Some(tokeira_aws::resources::ec2_tags(&tags)))
                    .build(),
            )
            .user_data(&user_data_base64)
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
            .ok_or_else(|| {
                IacError::AwsSdk("RunInstances did not return instance ID".to_string())
            })?
            .to_string();

        // Wait for running
        clients
            .ec2
            .wait_until_instance_running()
            .instance_ids(&instance_id)
            .wait(Duration::from_secs(300))
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!(
                    "instance {instance_id} did not reach running: {e}"
                ))
            })?;

        // Attach volumes
        clients
            .ec2
            .attach_volume()
            .instance_id(&instance_id)
            .volume_id(&cache_vol_id)
            .device("/dev/sdf")
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!(
                    "failed to attach cache volume: {:?}",
                    e.into_service_error()
                ))
            })?;

        clients
            .ec2
            .attach_volume()
            .instance_id(&instance_id)
            .volume_id(&repo_vol_id)
            .device("/dev/sdg")
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!(
                    "failed to attach repo volume: {:?}",
                    e.into_service_error()
                ))
            })?;

        Ok(ResourceState {
            resource_type: self.resource_type(),
            physical_id: instance_id,
            properties: serde_json::json!({
                "instance_type": self.config.instance_type,
                "ami_id": self.config.ami_id,
                "subnet_id": self.config.subnet_id,
                "security_group_id": sg_id,
                "cache_volume_id": cache_vol_id,
                "repo_volume_id": repo_vol_id,
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
        Ok(current.clone())
    }

    async fn delete(
        &self,
        current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<(), IacError> {
        let clients = ctx.extension::<tokeira_aws::AwsClients>().expect("AwsClients");
        let instance_id = &current.physical_id;

        clients
            .ec2
            .terminate_instances()
            .instance_ids(instance_id)
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!(
                    "failed to terminate instance {instance_id}: {:?}",
                    e.into_service_error()
                ))
            })?;

        clients
            .ec2
            .wait_until_instance_terminated()
            .instance_ids(instance_id)
            .wait(Duration::from_secs(300))
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!(
                    "instance {instance_id} did not terminate: {e}"
                ))
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

        let clients = ctx.extension::<tokeira_aws::AwsClients>().expect("AwsClients");

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
                let msg = format!("{:?}", e.into_service_error());
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
