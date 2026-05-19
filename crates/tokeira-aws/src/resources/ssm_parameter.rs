use tokeira_iac::{
    InternalChange, ProvisionContext, Resource, ResourceId, ResourceState, ResourceType,
    error::IacError,
};

/// Secure SSM parameter used for generated task configuration payloads.
#[derive(Debug)]
pub struct SsmParameterResource {
    pub name: String,
    pub value: String,
    pub secure: bool,
    pub module: String,
}

#[async_trait::async_trait]
impl Resource for SsmParameterResource {
    fn resource_type(&self) -> ResourceType {
        ResourceType::new("SsmParameter")
    }

    fn resource_id(&self) -> ResourceId {
        ResourceId(format!("ssm-parameter:{}", self.name))
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        vec![]
    }

    fn module(&self) -> &str {
        &self.module
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        ctx.extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ssm
            .put_parameter()
            .name(&self.name)
            .value(&self.value)
            .r#type(if self.secure {
                aws_sdk_ssm::types::ParameterType::SecureString
            } else {
                aws_sdk_ssm::types::ParameterType::String
            })
            .overwrite(true)
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!("ssm:PutParameter: {}", e.into_service_error()))
            })?;
        Ok(self.state())
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
        _current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<(), IacError> {
        ctx.extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ssm
            .delete_parameter()
            .name(&self.name)
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!("ssm:DeleteParameter: {}", e.into_service_error()))
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
            details: "SSM parameter content is managed by generated config".into(),
        }
    }
}

impl SsmParameterResource {
    fn state(&self) -> ResourceState {
        let now = chrono::Utc::now().to_rfc3339();
        ResourceState {
            resource_type: self.resource_type(),
            physical_id: self.name.clone(),
            properties: serde_json::json!({
                "name": self.name,
                "secure": self.secure,
            }),
            dependencies: self.dependencies(),
            created_at: now.clone(),
            updated_at: now,
            module: self.module.clone(),
        }
    }
}
