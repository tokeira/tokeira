use std::{collections::HashMap, time::Duration};

use tokeira_iac::{
    InternalChange, ProvisionContext, Resource, ResourceId, ResourceState, ResourceType,
    error::IacError,
};

/// Configuration for a single Secrets Manager secret provider resource.
#[derive(Debug)]
pub struct SecretsManagerSecretConfig {
    pub initial_value: String,
    pub module: String,
}

/// Generic provider resource that provisions exactly one Secrets Manager secret.
#[derive(Debug)]
pub struct SecretsManagerSecret {
    pub secret_name: String,
    pub config: SecretsManagerSecretConfig,
    pub project: String,
    pub region: String,
    pub tags: HashMap<String, String>,
}

impl SecretsManagerSecret {
    pub fn new(
        secret_name: String,
        config: SecretsManagerSecretConfig,
        rctx: &crate::ResourceContext,
    ) -> Self {
        Self {
            secret_name,
            config,
            project: rctx.project.clone(),
            region: rctx.region.clone(),
            tags: rctx.tags.clone(),
        }
    }

    async fn wait_until_deleted(&self, ctx: &ProvisionContext) -> Result<(), IacError> {
        let name = self.secret_name.clone();
        let secretsmanager_client = &ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .secretsmanager;
        super::poll_until(
            Duration::from_secs(2),
            Duration::from_secs(120),
            ctx,
            super::PollTarget {
                resource_desc: "Secrets Manager secret deletion",
                resource_id: &self.resource_id(),
                resource_type: self.resource_type(),
                phase: "waiting for secret deletion",
            },
            || async {
                match secretsmanager_client
                    .describe_secret()
                    .secret_id(&name)
                    .send()
                    .await
                {
                    Ok(output) => Ok(output.deleted_date().is_some()),
                    Err(e) => {
                        let svc_err = e.into_service_error();
                        if svc_err.is_resource_not_found_exception() {
                            Ok(true)
                        } else {
                            Err(IacError::AwsSdk(format!(
                                "secretsmanager:DescribeSecret: {svc_err}"
                            )))
                        }
                    }
                }
            },
        )
        .await
    }
}

#[async_trait::async_trait]
impl Resource for SecretsManagerSecret {
    fn resource_type(&self) -> ResourceType {
        ResourceType::new("SecretsManagerSecret")
    }

    fn resource_id(&self) -> ResourceId {
        ResourceId(format!("secret-{}", self.secret_name))
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        vec![]
    }

    fn module(&self) -> &str {
        &self.config.module
    }

    fn diff(&self, _current: &ResourceState, _ctx: &ProvisionContext) -> InternalChange {
        // Secrets are static — no mutable properties to diff.
        InternalChange::NoChange {
            resource_id: self.resource_id(),
        }
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        let name = &self.secret_name;
        let tags = ctx.resource_tags(name);
        let sm_tag_list = super::secretsmanager_tags(&tags);

        let secret_arn;

        // secretsmanager:CreateSecret with secret string and tags
        match ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .secretsmanager
            .create_secret()
            .name(name)
            .secret_string(&self.config.initial_value)
            .set_tags(Some(sm_tag_list))
            .send()
            .await
        {
            Ok(output) => {
                secret_arn = output.arn().unwrap_or_default().to_string();
            }
            Err(e) => {
                let svc_err = e.into_service_error();
                if svc_err.is_resource_exists_exception() {
                    tracing::warn!(secret = %name, "secret already exists, adopting");
                    // secretsmanager:DescribeSecret to get ARN of existing secret
                    let desc = ctx
                        .extension::<crate::AwsClients>()
                        .expect("AwsClients")
                        .secretsmanager
                        .describe_secret()
                        .secret_id(name)
                        .send()
                        .await
                        .map_err(|e| {
                            IacError::AwsSdk(format!(
                                "secretsmanager:DescribeSecret: {}",
                                e.into_service_error()
                            ))
                        })?;
                    secret_arn = desc.arn().unwrap_or_default().to_string();
                } else {
                    return Err(IacError::AwsSdk(format!(
                        "secretsmanager:CreateSecret: {svc_err}"
                    )));
                }
            }
        }

        let now = chrono::Utc::now().to_rfc3339();
        Ok(ResourceState {
            resource_type: ResourceType::new("SecretsManagerSecret"),
            physical_id: secret_arn,
            properties: serde_json::json!({
                "secret_name": self.secret_name,
                "tags": tags,
            }),
            dependencies: vec![],
            created_at: now.clone(),
            updated_at: now,
            module: self.module().to_owned(),
        })
    }

    async fn update(
        &self,
        current: &ResourceState,
        _ctx: &ProvisionContext,
    ) -> Result<ResourceState, IacError> {
        // Secrets are static — update is a no-op that preserves current state.
        Ok(ResourceState {
            resource_type: current.resource_type.clone(),
            physical_id: current.physical_id.clone(),
            properties: current.properties.clone(),
            dependencies: vec![],
            created_at: current.created_at.clone(),
            updated_at: chrono::Utc::now().to_rfc3339(),
            module: self.module().to_owned(),
        })
    }

    async fn delete(
        &self,
        _current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<(), IacError> {
        let name = &self.secret_name;

        // secretsmanager:DeleteSecret with force (no recovery window)
        match ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .secretsmanager
            .delete_secret()
            .secret_id(name)
            .force_delete_without_recovery(true)
            .send()
            .await
        {
            Ok(_) => {}
            Err(e) => {
                let svc_err = e.into_service_error();
                if svc_err.is_resource_not_found_exception() {
                    tracing::warn!(secret = %name, "secret does not exist, skipping deletion");
                } else {
                    return Err(IacError::AwsSdk(format!(
                        "secretsmanager:DeleteSecret: {svc_err}"
                    )));
                }
            }
        }

        self.wait_until_deleted(ctx).await
    }

    async fn describe(&self, ctx: &ProvisionContext) -> Result<Option<ResourceState>, IacError> {
        let name = &self.secret_name;

        // secretsmanager:DescribeSecret for this single secret
        match ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .secretsmanager
            .describe_secret()
            .secret_id(name)
            .send()
            .await
        {
            Ok(output) => {
                let now = chrono::Utc::now().to_rfc3339();
                Ok(Some(ResourceState {
                    resource_type: ResourceType::new("SecretsManagerSecret"),
                    physical_id: output.arn().unwrap_or_default().to_string(),
                    properties: serde_json::json!({
                        "secret_name": self.secret_name,
                    }),
                    dependencies: vec![],
                    created_at: now.clone(),
                    updated_at: now,
                    module: self.module().to_owned(),
                }))
            }
            Err(e) => {
                let svc_err = e.into_service_error();
                if svc_err.is_resource_not_found_exception() {
                    Ok(None)
                } else {
                    Err(IacError::AwsSdk(format!(
                        "secretsmanager:DescribeSecret: {svc_err}"
                    )))
                }
            }
        }
    }
}
