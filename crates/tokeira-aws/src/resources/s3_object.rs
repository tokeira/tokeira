use aws_sdk_s3::primitives::ByteStream;
use sha2::{Digest, Sha256};
use tokeira_iac::{
    DescribeResult, InternalChange, ProvisionContext, Resource, ResourceId, ResourceState,
    ResourceType, error::IacError,
};

/// Provider resource for generated observability artifacts stored in S3.
#[derive(Debug)]
pub struct S3Object {
    pub bucket_dependency: ResourceId,
    pub key: String,
    pub content: String,
    pub content_type: String,
    pub module: String,
}

#[async_trait::async_trait]
impl Resource for S3Object {
    fn resource_type(&self) -> ResourceType {
        ResourceType::new("S3Object")
    }

    fn resource_id(&self) -> ResourceId {
        ResourceId(format!(
            "s3-object:{}:{}",
            self.bucket_dependency.0, self.key
        ))
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        vec![self.bucket_dependency.clone()]
    }

    fn module(&self) -> &str {
        &self.module
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        let bucket = bucket_name(ctx, &self.bucket_dependency)?;
        ctx.extension::<crate::AwsClients>()
            .expect("AwsClients")
            .s3
            .put_object()
            .bucket(&bucket)
            .key(&self.key)
            .content_type(&self.content_type)
            .body(ByteStream::from(self.content.clone().into_bytes()))
            .send()
            .await
            .map_err(|error| {
                IacError::AwsSdk(format!(
                    "s3:PutObject {}: {}",
                    self.key,
                    error.into_service_error()
                ))
            })?;
        Ok(self.state(bucket))
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
        current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<(), IacError> {
        let bucket = current
            .properties
            .get("bucket_name")
            .and_then(|value| value.as_str())
            .ok_or_else(|| IacError::StateNotFound("bucket_name missing".into()))?;
        ctx.extension::<crate::AwsClients>()
            .expect("AwsClients")
            .s3
            .delete_object()
            .bucket(bucket)
            .key(&self.key)
            .send()
            .await
            .map_err(|error| {
                IacError::AwsSdk(format!(
                    "s3:DeleteObject {}: {}",
                    self.key,
                    error.into_service_error()
                ))
            })?;
        Ok(())
    }

    async fn describe(&self, _ctx: &ProvisionContext) -> Result<DescribeResult, IacError> {
        Ok(DescribeResult::Unsupported)
    }

    fn diff(&self, current: &ResourceState, _ctx: &ProvisionContext) -> InternalChange {
        let checksum = current
            .properties
            .get("checksum")
            .and_then(|value| value.as_str());
        if checksum == Some(self.checksum().as_str()) {
            InternalChange::NoChange {
                resource_id: self.resource_id(),
            }
        } else {
            InternalChange::Update {
                resource_id: self.resource_id(),
                resource_type: self.resource_type(),
                details: vec![tokeira_iac::FieldDiff::observation(
                    "S3 object content changed",
                )],
            }
        }
    }
}

impl S3Object {
    fn checksum(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.content.as_bytes());
        hex::encode(hasher.finalize())
    }

    fn state(&self, bucket_name: String) -> ResourceState {
        let now = chrono::Utc::now().to_rfc3339();
        ResourceState {
            resource_type: self.resource_type(),
            physical_id: format!("s3://{bucket_name}/{}", self.key),
            properties: serde_json::json!({
                "bucket_name": bucket_name,
                "key": self.key,
                "content_type": self.content_type,
                "checksum": self.checksum(),
            }),
            dependencies: self.dependencies(),
            created_at: now.clone(),
            updated_at: now,
            module: self.module.clone(),
        }
    }
}

fn bucket_name(ctx: &ProvisionContext, dependency: &ResourceId) -> Result<String, IacError> {
    let bucket_state = ctx.get_resource_state(dependency)?;
    bucket_state
        .properties
        .get("bucket_name")
        .and_then(|value| value.as_str())
        .map(ToOwned::to_owned)
        .ok_or_else(|| IacError::StateNotFound("bucket_name missing".into()))
}
