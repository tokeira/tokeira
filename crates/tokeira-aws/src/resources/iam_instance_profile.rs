//! IAM instance profile resource.
//!
//! Provisions an instance profile, adds a role to it, and waits for IAM
//! eventual consistency before reporting completion. EC2 cannot reference
//! a newly-created profile for up to 10-15 seconds.

use std::{collections::HashMap, time::Duration};

use tokeira_iac::{
    DescribeResult, InternalChange, ProvisionContext, Resource, ResourceId, ResourceState,
    ResourceType,
    error::IacError,
};
use tokio::time::sleep;

use crate::ResourceContext;

/// Configuration for a single IAM instance profile.
#[derive(Debug)]
pub struct IamInstanceProfileConfig {
    /// The IAM role to attach to this profile.
    pub role_name: String,
    /// Resource ID of the role dependency (for ordering).
    pub role_dependency: ResourceId,
    pub module: String,
}

/// Generic provider resource that provisions one IAM instance profile.
#[derive(Debug)]
pub struct IamInstanceProfile {
    pub profile_name: String,
    pub config: IamInstanceProfileConfig,
    pub project: String,
    pub region: String,
    pub tags: HashMap<String, String>,
}

impl IamInstanceProfile {
    pub fn new(
        profile_name: String,
        config: IamInstanceProfileConfig,
        rctx: &ResourceContext,
    ) -> Self {
        Self {
            profile_name,
            config,
            project: rctx.project.clone(),
            region: rctx.region.clone(),
            tags: rctx.tags.clone(),
        }
    }
}

#[async_trait::async_trait]
impl Resource for IamInstanceProfile {
    fn resource_type(&self) -> ResourceType {
        ResourceType::new("IamInstanceProfile")
    }

    fn resource_id(&self) -> ResourceId {
        ResourceId(format!("iam-instance-profile-{}", self.profile_name))
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        vec![self.config.role_dependency.clone()]
    }

    fn module(&self) -> &str {
        &self.config.module
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        let clients = ctx.extension::<crate::AwsClients>().expect("AwsClients");
        let tags = ctx.resource_tags(&self.profile_name);
        let iam_tags = super::iam_tags(&tags);

        // Create the instance profile
        match clients
            .iam
            .create_instance_profile()
            .instance_profile_name(&self.profile_name)
            .set_tags(Some(iam_tags))
            .send()
            .await
        {
            Ok(_) => {}
            Err(e) => {
                let svc_err = e.into_service_error();
                if svc_err.is_entity_already_exists_exception() {
                    tracing::warn!(
                        profile = %self.profile_name,
                        "instance profile already exists, adopting"
                    );
                } else {
                    return Err(IacError::AwsSdk(format!(
                        "failed to create instance profile {}: {svc_err}",
                        self.profile_name
                    )));
                }
            }
        }

        // Add role to profile
        match clients
            .iam
            .add_role_to_instance_profile()
            .instance_profile_name(&self.profile_name)
            .role_name(&self.config.role_name)
            .send()
            .await
        {
            Ok(_) => {}
            Err(e) => {
                let svc_err = e.into_service_error();
                let msg = format!("{svc_err}");
                if msg.contains("LimitExceeded") || msg.contains("Cannot exceed quota") {
                    tracing::warn!(
                        profile = %self.profile_name,
                        role = %self.config.role_name,
                        "role already in instance profile"
                    );
                } else {
                    return Err(IacError::AwsSdk(format!(
                        "failed to add role {} to instance profile {}: {svc_err}",
                        self.config.role_name, self.profile_name
                    )));
                }
            }
        }

        // Wait for IAM propagation — poll until GetInstanceProfile returns
        // the profile with the role attached.
        for _ in 0..30 {
            if let Ok(output) = clients
                .iam
                .get_instance_profile()
                .instance_profile_name(&self.profile_name)
                .send()
                .await
            {
                let has_role = output
                    .instance_profile()
                    .map(|p| !p.roles().is_empty())
                    .unwrap_or(false);
                if has_role {
                    return Ok(ResourceState {
                        resource_type: self.resource_type(),
                        physical_id: self.profile_name.clone(),
                        properties: serde_json::json!({
                            "role_name": self.config.role_name,
                        }),
                        dependencies: self.dependencies(),
                        module: self.config.module.clone(),
                        created_at: chrono::Utc::now().to_rfc3339(),
                        updated_at: chrono::Utc::now().to_rfc3339(),
                    });
                }
            }
            sleep(Duration::from_secs(2)).await;
        }

        Err(IacError::AwsSdk(format!(
            "instance profile {} did not become available within 60 seconds",
            self.profile_name
        )))
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
        let clients = ctx.extension::<crate::AwsClients>().expect("AwsClients");

        // Remove role from profile first
        let _ = clients
            .iam
            .remove_role_from_instance_profile()
            .instance_profile_name(&self.profile_name)
            .role_name(&self.config.role_name)
            .send()
            .await;

        // Delete the profile
        clients
            .iam
            .delete_instance_profile()
            .instance_profile_name(&self.profile_name)
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!(
                    "failed to delete instance profile {}: {}",
                    self.profile_name,
                    e.into_service_error()
                ))
            })?;

        Ok(())
    }

    async fn describe(&self, ctx: &ProvisionContext) -> Result<DescribeResult, IacError> {
        let clients = ctx.extension::<crate::AwsClients>().expect("AwsClients");

        match clients
            .iam
            .get_instance_profile()
            .instance_profile_name(&self.profile_name)
            .send()
            .await
        {
            Ok(output) => {
                let role_name = output
                    .instance_profile()
                    .and_then(|p| p.roles().first())
                    .map(|r| r.role_name().to_string())
                    .unwrap_or_default();
                Ok(DescribeResult::Present(ResourceState {
                    resource_type: self.resource_type(),
                    physical_id: self.profile_name.clone(),
                    properties: serde_json::json!({ "role_name": role_name }),
                    dependencies: self.dependencies(),
                    module: self.config.module.clone(),
                    created_at: String::new(),
                    updated_at: chrono::Utc::now().to_rfc3339(),
                }))
            }
            Err(e) => {
                let svc_err = e.into_service_error();
                if svc_err.is_no_such_entity_exception() {
                    Ok(DescribeResult::Absent)
                } else {
                    Err(IacError::AwsSdk(format!(
                        "failed to describe instance profile {}: {svc_err}",
                        self.profile_name
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
