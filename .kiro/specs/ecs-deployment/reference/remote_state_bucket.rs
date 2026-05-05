//! Project-owned remote-state bucket resource.
//!
//! The remote-state bucket is shared across deployments in a region. This
//! resource manages only the state-specific safety settings that must hold for
//! this project's prefix and deliberately does not treat bucket tags as desired
//! state.

use std::collections::HashMap;

use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::error::SdkError;
use tokeira_iac::error::IacError;
use tokeira_iac::{InternalChange, ProvisionContext, Resource, ResourceId, ResourceState, ResourceType};

/// Shared remote-state bucket with per-project snapshot protection.
///
/// Lives in `platforms/` (shared across all AWS-backed platforms) because
/// any AWS-backed platform (ECS, EKS, future platforms) needs the same
/// shared-bucket lifecycle semantics.
#[derive(Debug)]
pub struct RemoteStateBucket {
    bucket_name: String,
    region: String,
    key_prefix: String,
    module: String,
}

impl RemoteStateBucket {
    pub fn new(
        bucket_name: impl Into<String>,
        region: impl Into<String>,
        key_prefix: impl Into<String>,
        module: impl Into<String>,
    ) -> Self {
        Self {
            bucket_name: bucket_name.into(),
            region: region.into(),
            key_prefix: key_prefix.into(),
            module: module.into(),
        }
    }

    fn snapshot_delete_prevention_policy(&self) -> Option<String> {
        let key_prefix = self.key_prefix.trim_matches('/');
        if key_prefix.is_empty() {
            return None;
        }

        Some(
            serde_json::json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Sid": "DenySnapshotDeletes",
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": ["s3:DeleteObject", "s3:DeleteObjectVersion"],
                    "Resource": format!("arn:aws:s3:::{}/{}/snapshots/*", self.bucket_name, key_prefix)
                }]
            })
            .to_string(),
        )
    }

    fn snapshot_delete_prevention_prefix(&self) -> Option<String> {
        let key_prefix = self.key_prefix.trim_matches('/');
        if key_prefix.is_empty() {
            return None;
        }
        Some(format!("{key_prefix}/snapshots/"))
    }

    fn aws<'a>(&self, ctx: &'a ProvisionContext) -> &'a tokeira_aws::AwsClients {
        ctx.extension::<tokeira_aws::AwsClients>()
            .expect("AwsClients extension must be registered in ProvisionContext")
    }

    fn managed_snapshot_policy(&self, current: &ResourceState) -> bool {
        current
            .properties
            .get("managed_snapshot_policy")
            .and_then(|value| value.as_bool())
            .unwrap_or(false)
    }

    fn persisted_managed_snapshot_policy(&self, ctx: &ProvisionContext) -> bool {
        ctx.state
            .resources
            .get(&self.resource_id())
            .map(|current| self.managed_snapshot_policy(current))
            .unwrap_or(false)
    }

    fn s3_error<E>(&self, operation: &str, err: &SdkError<E>) -> IacError
    where
        E: ProvideErrorMetadata,
    {
        let mut parts = vec![format!("{operation}: {err}")];
        if let Some(code) = err.code() {
            parts.push(format!("code={code}"));
        }
        if let Some(message) = err.message() {
            parts.push(format!("message={message}"));
        }
        IacError::AwsSdk(parts.join(", "))
    }

    fn is_error_code<E>(&self, err: &SdkError<E>, expected: &str) -> bool
    where
        E: ProvideErrorMetadata,
    {
        err.code() == Some(expected)
    }

    async fn ensure_public_access_block(&self, ctx: &ProvisionContext) -> Result<(), IacError> {
        self.aws(ctx)
            .s3
            .put_public_access_block()
            .bucket(&self.bucket_name)
            .public_access_block_configuration(
                aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
                    .block_public_acls(true)
                    .ignore_public_acls(true)
                    .block_public_policy(true)
                    .restrict_public_buckets(true)
                    .build(),
            )
            .send()
            .await
            .map_err(|e| self.s3_error("s3:PutPublicAccessBlock", &e))?;
        Ok(())
    }

    async fn ensure_versioning(&self, ctx: &ProvisionContext) -> Result<(), IacError> {
        self.aws(ctx)
            .s3
            .put_bucket_versioning()
            .bucket(&self.bucket_name)
            .versioning_configuration(
                aws_sdk_s3::types::VersioningConfiguration::builder()
                    .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send()
            .await
            .map_err(|e| self.s3_error("s3:PutBucketVersioning", &e))?;
        Ok(())
    }

    async fn put_snapshot_policy(&self, ctx: &ProvisionContext) -> Result<Option<String>, IacError> {
        let Some(policy) = self.snapshot_delete_prevention_policy() else {
            return Ok(None);
        };

        self.aws(ctx)
            .s3
            .put_bucket_policy()
            .bucket(&self.bucket_name)
            .policy(policy)
            .send()
            .await
            .map_err(|e| self.s3_error("s3:PutBucketPolicy", &e))?;

        Ok(self.snapshot_delete_prevention_prefix())
    }

    async fn observed_tags(
        &self,
        ctx: &ProvisionContext,
    ) -> Result<HashMap<String, String>, IacError> {
        match self
            .aws(ctx)
            .s3
            .get_bucket_tagging()
            .bucket(&self.bucket_name)
            .send()
            .await
        {
            Ok(output) => Ok(output
                .tag_set()
                .iter()
                .map(|tag| (tag.key().to_string(), tag.value().to_string()))
                .collect()),
            Err(e) => {
                if self.is_error_code(&e, "NoSuchTagSet") {
                    Ok(HashMap::new())
                } else {
                    Err(self.s3_error("s3:GetBucketTagging", &e))
                }
            }
        }
    }

    async fn observed_versioning(&self, ctx: &ProvisionContext) -> Result<bool, IacError> {
        let versioning = self
            .aws(ctx)
            .s3
            .get_bucket_versioning()
            .bucket(&self.bucket_name)
            .send()
            .await
            .map_err(|e| self.s3_error("s3:GetBucketVersioning", &e))?;

        Ok(versioning.status() == Some(&aws_sdk_s3::types::BucketVersioningStatus::Enabled))
    }

    async fn observed_snapshot_policy_prefix(
        &self,
        ctx: &ProvisionContext,
    ) -> Result<Option<String>, IacError> {
        match self
            .aws(ctx)
            .s3
            .get_bucket_policy()
            .bucket(&self.bucket_name)
            .send()
            .await
        {
            Ok(output) => extract_snapshot_policy_prefix(
                output.policy().unwrap_or_default(),
                &self.bucket_name,
            ),
            Err(e) => {
                if self.is_error_code(&e, "NoSuchBucketPolicy") {
                    Ok(None)
                } else {
                    Err(self.s3_error("s3:GetBucketPolicy", &e))
                }
            }
        }
    }

    async fn observe_state(
        &self,
        ctx: &ProvisionContext,
        created_at: String,
        managed_snapshot_policy: bool,
    ) -> Result<ResourceState, IacError> {
        let versioning = self.observed_versioning(ctx).await?;
        let snapshot_delete_prevention_prefix = self.observed_snapshot_policy_prefix(ctx).await?;
        let tags = self.observed_tags(ctx).await?;
        let now = chrono::Utc::now().to_rfc3339();

        Ok(ResourceState {
            resource_type: self.resource_type(),
            physical_id: self.bucket_name.clone(),
            properties: serde_json::json!({
                "bucket_name": self.bucket_name,
                "versioning": versioning,
                "snapshot_delete_prevention_prefix": snapshot_delete_prevention_prefix,
                "managed_snapshot_policy": managed_snapshot_policy,
                "tags": tags,
            }),
            dependencies: vec![],
            created_at,
            updated_at: now,
            module: self.module().to_owned(),
        })
    }
}

fn extract_snapshot_policy_prefix(
    policy: &str,
    bucket_name: &str,
) -> Result<Option<String>, IacError> {
    let value: serde_json::Value = serde_json::from_str(policy).map_err(|e| {
        IacError::AwsSdk(format!(
            "s3:GetBucketPolicy: invalid bucket policy JSON: {e}"
        ))
    })?;
    let Some(statements) = value.get("Statement").and_then(|value| value.as_array()) else {
        return Ok(None);
    };

    for statement in statements {
        if statement.get("Sid").and_then(|value| value.as_str()) != Some("DenySnapshotDeletes") {
            continue;
        }
        let resource = statement
            .get("Resource")
            .and_then(|value| value.as_str())
            .unwrap_or_default();
        let expected_prefix = format!("arn:aws:s3:::{bucket_name}/");
        if let Some(remainder) = resource.strip_prefix(&expected_prefix)
            && let Some(prefix) = remainder.strip_suffix("snapshots/*")
        {
            return Ok(Some(format!("{prefix}snapshots/")));
        }
    }

    Ok(None)
}

#[async_trait::async_trait]
impl Resource for RemoteStateBucket {
    fn resource_type(&self) -> ResourceType {
        ResourceType::new("S3Bucket")
    }

    fn resource_id(&self) -> ResourceId {
        ResourceId(format!("s3-{}", self.bucket_name))
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        vec![]
    }

    fn module(&self) -> &str {
        &self.module
    }

    fn diff(&self, current: &ResourceState, _ctx: &ProvisionContext) -> InternalChange {
        let current_versioning = current
            .properties
            .get("versioning")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let current_snapshot_policy = current
            .properties
            .get("snapshot_delete_prevention_prefix")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let managed_snapshot_policy = self.managed_snapshot_policy(current);
        let desired_snapshot_policy = self.snapshot_delete_prevention_prefix().unwrap_or_default();

        if !current_versioning {
            InternalChange::Update {
                resource_id: self.resource_id(),
            }
        } else if managed_snapshot_policy && current_snapshot_policy != desired_snapshot_policy {
            InternalChange::Update {
                resource_id: self.resource_id(),
            }
        } else {
            InternalChange::NoChange {
                resource_id: self.resource_id(),
            }
        }
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        let name = &self.bucket_name;
        let region = &self.region;
        let now = chrono::Utc::now().to_rfc3339();

        let mut req = self.aws(ctx).s3.create_bucket().bucket(name);

        if region != "us-east-1" {
            req = req.create_bucket_configuration(
                aws_sdk_s3::types::CreateBucketConfiguration::builder()
                    .location_constraint(aws_sdk_s3::types::BucketLocationConstraint::from(
                        region.as_str(),
                    ))
                    .build(),
            );
        }

        let mut managed_snapshot_policy = true;
        match req.send().await {
            Ok(_) => {}
            Err(e) => {
                if self.is_error_code(&e, "BucketAlreadyOwnedByYou") {
                    managed_snapshot_policy = false;
                    tracing::warn!(bucket = %name, "bucket already owned by us, adopting");
                } else {
                    return Err(self.s3_error("s3:CreateBucket", &e));
                }
            }
        }

        self.ensure_public_access_block(ctx).await?;
        self.ensure_versioning(ctx).await?;
        if managed_snapshot_policy {
            self.put_snapshot_policy(ctx).await?;
        }

        self.observe_state(ctx, now, managed_snapshot_policy).await
    }

    async fn update(
        &self,
        current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<ResourceState, IacError> {
        self.ensure_versioning(ctx).await?;

        let managed_snapshot_policy = self.managed_snapshot_policy(current);
        if managed_snapshot_policy {
            self.put_snapshot_policy(ctx).await?;
        }

        self.observe_state(ctx, current.created_at.clone(), managed_snapshot_policy)
            .await
    }

    async fn delete(
        &self,
        _current: &ResourceState,
        _ctx: &ProvisionContext,
    ) -> Result<(), IacError> {
        tracing::info!(bucket = %self.bucket_name, "shared remote-state bucket is not deleted");
        Ok(())
    }

    async fn describe(&self, ctx: &ProvisionContext) -> Result<Option<ResourceState>, IacError> {
        let name = &self.bucket_name;

        match self
            .aws(ctx)
            .s3
            .head_bucket()
            .bucket(name)
            .send()
            .await
        {
            Ok(_) => {
                let managed_snapshot_policy = self.persisted_managed_snapshot_policy(ctx);
                let now = chrono::Utc::now().to_rfc3339();
                Ok(Some(
                    self.observe_state(ctx, now, managed_snapshot_policy).await?,
                ))
            }
            Err(e) => {
                if self.is_error_code(&e, "NotFound") || self.is_error_code(&e, "NoSuchBucket") {
                    Ok(None)
                } else {
                    Err(self.s3_error("s3:HeadBucket", &e))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tokeira_iac::{InternalChange, ProvisionContext, Resource, ResourceState, ResourceType};

    use super::RemoteStateBucket;

    fn state(
        tags: serde_json::Value,
        prefix: &str,
        managed_snapshot_policy: bool,
    ) -> ResourceState {
        ResourceState {
            resource_type: ResourceType::new("S3Bucket"),
            physical_id: "tokeira-state-us-east-1".into(),
            properties: serde_json::json!({
                "bucket_name": "tokeira-state-us-east-1",
                "versioning": true,
                "snapshot_delete_prevention_prefix": prefix,
                "managed_snapshot_policy": managed_snapshot_policy,
                "tags": tags,
            }),
            dependencies: vec![],
            created_at: "2026-01-01T00:00:00Z".into(),
            updated_at: "2026-01-01T00:00:00Z".into(),
            module: "remote-state".into(),
        }
    }

    #[test]
    fn ignores_tag_drift_for_shared_bucket() {
        let bucket = RemoteStateBucket::new(
            "tokeira-state-us-east-1",
            "us-east-1",
            "tokeira/dev",
            "remote-state",
        );
        let ctx = ProvisionContext::new("tokeira", HashMap::new());

        let change = bucket.diff(
            &state(
                serde_json::json!({ "Name": "shared-state-bucket", "Project": "other" }),
                "tokeira/dev/snapshots/",
                false,
            ),
            &ctx,
        );

        assert!(matches!(change, InternalChange::NoChange { .. }));
    }

    #[test]
    fn still_detects_snapshot_policy_drift() {
        let bucket = RemoteStateBucket::new(
            "tokeira-state-us-east-1",
            "us-east-1",
            "tokeira/dev",
            "remote-state",
        );
        let ctx = ProvisionContext::new("tokeira", HashMap::new());

        let change = bucket.diff(
            &state(serde_json::json!({}), "other-project/snapshots/", true),
            &ctx,
        );

        assert!(matches!(change, InternalChange::Update { .. }));
    }

    #[test]
    fn ignores_snapshot_policy_drift_for_adopted_bucket() {
        let bucket = RemoteStateBucket::new(
            "tokeira-state-us-east-1",
            "us-east-1",
            "tokeira/dev",
            "remote-state",
        );
        let ctx = ProvisionContext::new("tokeira", HashMap::new());

        let change = bucket.diff(
            &state(serde_json::json!({}), "other-project/snapshots/", false),
            &ctx,
        );

        assert!(matches!(change, InternalChange::NoChange { .. }));
    }
}
