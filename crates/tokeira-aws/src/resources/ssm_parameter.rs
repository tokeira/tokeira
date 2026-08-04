use tokeira_iac::{
    ChangeSemantics, Citation, DescribeResult, InternalChange, ProvisionContext, Resource,
    ResourceId, ResourceState, ResourceType, SemanticsContext, error::IacError,
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

    fn change_semantics(&self, ctx: &SemanticsContext<'_>) -> ChangeSemantics {
        // Generated task configuration: the value regenerates from the
        // definition on the next apply, so the parameter holds no data of
        // record.
        super::generated_content_semantics(
            ctx.kind,
            Citation::code(concat!(
                module_path!(),
                "::{create,update} — ssm:PutParameter with overwrite(true)"
            )),
            Citation::code(concat!(
                module_path!(),
                "::delete — ssm:DeleteParameter, ParameterNotFound tolerated (idempotent)"
            )),
        )
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
        match ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ssm
            .delete_parameter()
            .name(&self.name)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                let svc_err = e.into_service_error();
                // Idempotent delete: an already-absent parameter is success. The
                // engine may drive this from persisted state when `describe` is
                // Unsupported, so the live object may already be gone.
                if svc_err.is_parameter_not_found() {
                    Ok(())
                } else {
                    Err(IacError::AwsSdk(format!("ssm:DeleteParameter: {svc_err}")))
                }
            }
        }
    }

    async fn describe(&self, _ctx: &ProvisionContext) -> Result<DescribeResult, IacError> {
        Ok(DescribeResult::Unsupported)
    }

    fn diff(&self, _current: &ResourceState, _ctx: &ProvisionContext) -> InternalChange {
        InternalChange::Update {
            resource_id: self.resource_id(),
            resource_type: self.resource_type(),
            details: vec![tokeira_iac::FieldDiff::observation(
                "SSM parameter content is managed by generated config",
            )],
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
