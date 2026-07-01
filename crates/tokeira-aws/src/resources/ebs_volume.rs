//! EBS volume resource.
//!
//! Provisions an encrypted gp3 EBS volume and waits for it to reach the
//! `available` state using the SDK's built-in waiter.

use std::{collections::HashMap, time::Duration};

use aws_sdk_ec2::client::Waiters as Ec2Waiters;
use tokeira_iac::{
    DescribeResult, InternalChange, ProvisionContext, Resource, ResourceId, ResourceState,
    ResourceType,
    error::IacError,
};

use crate::ResourceContext;

/// Configuration for a single EBS volume.
#[derive(Debug)]
pub struct EbsVolumeConfig {
    pub size_gib: u32,
    pub availability_zone: String,
    pub volume_type: String,
    pub encrypted: bool,
    pub module: String,
}

/// Generic provider resource that provisions one EBS volume.
#[derive(Debug)]
pub struct EbsVolume {
    /// Logical name for resource ID derivation (e.g. "workstation-cache").
    pub name: String,
    pub config: EbsVolumeConfig,
    pub project: String,
    pub region: String,
    pub tags: HashMap<String, String>,
}

impl EbsVolume {
    pub fn new(name: String, config: EbsVolumeConfig, rctx: &ResourceContext) -> Self {
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
impl Resource for EbsVolume {
    fn resource_type(&self) -> ResourceType {
        ResourceType::new("EbsVolume")
    }

    fn resource_id(&self) -> ResourceId {
        ResourceId(format!("ebs-volume-{}", self.name))
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        vec![]
    }

    fn module(&self) -> &str {
        &self.config.module
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        let clients = ctx.extension::<crate::AwsClients>().expect("AwsClients");
        let tags = ctx.resource_tags(&self.name);

        let volume_type = match self.config.volume_type.as_str() {
            "gp3" => aws_sdk_ec2::types::VolumeType::Gp3,
            "gp2" => aws_sdk_ec2::types::VolumeType::Gp2,
            "io1" => aws_sdk_ec2::types::VolumeType::Io1,
            "io2" => aws_sdk_ec2::types::VolumeType::Io2,
            _ => aws_sdk_ec2::types::VolumeType::Gp3,
        };

        let output = clients
            .ec2
            .create_volume()
            .availability_zone(&self.config.availability_zone)
            .size(self.config.size_gib as i32)
            .volume_type(volume_type)
            .encrypted(self.config.encrypted)
            .tag_specifications(
                aws_sdk_ec2::types::TagSpecification::builder()
                    .resource_type(aws_sdk_ec2::types::ResourceType::Volume)
                    .set_tags(Some(super::ec2_tags(&tags)))
                    .build(),
            )
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!(
                    "failed to create EBS volume {}: {}",
                    self.name,
                    e.into_service_error()
                ))
            })?;

        let volume_id = output
            .volume_id()
            .ok_or_else(|| IacError::AwsSdk("CreateVolume did not return volume ID".to_string()))?
            .to_string();

        // Wait for volume to become available using SDK waiter
        clients
            .ec2
            .wait_until_volume_available()
            .volume_ids(&volume_id)
            .wait(Duration::from_secs(120))
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!(
                    "volume {volume_id} did not reach available state: {e}"
                ))
            })?;

        Ok(ResourceState {
            resource_type: self.resource_type(),
            physical_id: volume_id,
            properties: serde_json::json!({
                "size_gib": self.config.size_gib,
                "availability_zone": self.config.availability_zone,
                "volume_type": self.config.volume_type,
                "encrypted": self.config.encrypted,
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
        // Volume properties (size, type) are immutable — destroy + recreate for changes
        Ok(current.clone())
    }

    async fn delete(
        &self,
        current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<(), IacError> {
        let clients = ctx.extension::<crate::AwsClients>().expect("AwsClients");
        let volume_id = &current.physical_id;

        clients
            .ec2
            .delete_volume()
            .volume_id(volume_id)
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!(
                    "failed to delete volume {volume_id}: {}",
                    e.into_service_error()
                ))
            })?;

        Ok(())
    }

    async fn describe(&self, ctx: &ProvisionContext) -> Result<DescribeResult, IacError> {
        // Look up physical_id from persisted state
        let physical_id = ctx
            .get_resource_state(&self.resource_id())
            .ok()
            .map(|s| s.physical_id.clone());

        let Some(volume_id) = physical_id else {
            return Ok(DescribeResult::Unsupported);
        };

        let clients = ctx.extension::<crate::AwsClients>().expect("AwsClients");

        match clients
            .ec2
            .describe_volumes()
            .volume_ids(&volume_id)
            .send()
            .await
        {
            Ok(resp) => match resp.volumes().first() {
                Some(v) => {
                    let state = v.state().map(|s| s.as_str()).unwrap_or("unknown");
                    if state == "deleted" {
                        return Ok(DescribeResult::Absent);
                    }
                    Ok(DescribeResult::Present(ResourceState {
                        resource_type: self.resource_type(),
                        physical_id: volume_id,
                        properties: serde_json::json!({
                            "size_gib": v.size().unwrap_or(0),
                            "availability_zone": v.availability_zone().unwrap_or_default(),
                            "encrypted": v.encrypted().unwrap_or(false),
                            "state": state,
                        }),
                        dependencies: self.dependencies(),
                        module: self.config.module.clone(),
                        created_at: String::new(),
                        updated_at: chrono::Utc::now().to_rfc3339(),
                    }))
                }
                None => Ok(DescribeResult::Absent),
            },
            Err(e) => {
                let msg = format!("{}", e.into_service_error());
                if msg.contains("InvalidVolume.NotFound") {
                    Ok(DescribeResult::Absent)
                } else {
                    Err(IacError::AwsSdk(format!(
                        "failed to describe volume {volume_id}: {msg}"
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
