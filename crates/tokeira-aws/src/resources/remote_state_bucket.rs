//! AWS-backed storage for Tokeira's durable infrastructure state.
//!
//! This is a foundation resource rather than an ECS resource: Compose, ECS,
//! EKS, and future AWS-backed platforms may all use the same retention policy.
//! Unlike a generic S3 bucket, it owns only safety settings that must hold for
//! one deployment's state prefix. It never owns shared tags, never overwrites
//! an adopted bucket's policy, and never deletes the provider object.

use std::collections::HashMap;

use aws_sdk_s3::error::ProvideErrorMetadata;
use tokeira_iac::{
    ChangeKind, ChangeSemantics, Citation, Confidence, DataEffect, DescribeResult, Disruption,
    IacError, InternalChange, LifecycleOperation, ProvisionContext, ReplacementPolicy, Resource,
    ResourceId, ResourceState, ResourceType, Reversibility, SemanticsContext,
};

/// Shared remote-state bucket with per-deployment snapshot protection.
#[derive(Debug)]
pub struct RemoteStateBucket {
    bucket_name: String,
    region: String,
    key_prefix: Option<String>,
    module: String,
}

impl RemoteStateBucket {
    /// Author-visible and persisted resource type.
    pub const TYPE: &'static str = "RemoteStateBucket";

    /// Construct the foundation resource for one state prefix.
    pub fn new(
        bucket_name: impl Into<String>,
        region: impl Into<String>,
        key_prefix: Option<String>,
        module: impl Into<String>,
    ) -> Self {
        Self {
            bucket_name: bucket_name.into(),
            region: region.into(),
            key_prefix,
            module: module.into(),
        }
    }

    fn aws<'a>(&self, ctx: &'a ProvisionContext) -> &'a crate::AwsClients {
        ctx.extension::<crate::AwsClients>().expect(
            "AwsClients extension must be registered before provisioning a remote-state bucket",
        )
    }

    fn snapshot_delete_prevention_policy(&self) -> Option<String> {
        let key_prefix = self.key_prefix.as_deref()?.trim_matches('/');
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
                    "Resource": format!(
                        "arn:aws:s3:::{}/{key_prefix}/snapshots/*",
                        self.bucket_name
                    )
                }]
            })
            .to_string(),
        )
    }

    fn snapshot_delete_prevention_prefix(&self) -> Option<String> {
        let key_prefix = self.key_prefix.as_deref()?.trim_matches('/');
        if key_prefix.is_empty() {
            return None;
        }
        Some(format!("{key_prefix}/snapshots/"))
    }

    fn managed_snapshot_policy(current: &ResourceState) -> bool {
        current
            .properties
            .get("managed_snapshot_policy")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    }

    fn persisted_managed_snapshot_policy(&self, ctx: &ProvisionContext) -> bool {
        ctx.state
            .resources
            .get(&self.resource_id())
            .is_some_and(Self::managed_snapshot_policy)
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
            .map_err(|error| {
                IacError::AwsSdk(format!(
                    "s3:PutPublicAccessBlock: {}",
                    error.into_service_error()
                ))
            })?;
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
            .map_err(|error| {
                IacError::AwsSdk(format!(
                    "s3:PutBucketVersioning: {}",
                    error.into_service_error()
                ))
            })?;
        Ok(())
    }

    async fn put_snapshot_policy(&self, ctx: &ProvisionContext) -> Result<(), IacError> {
        let Some(policy) = self.snapshot_delete_prevention_policy() else {
            return Ok(());
        };
        self.aws(ctx)
            .s3
            .put_bucket_policy()
            .bucket(&self.bucket_name)
            .policy(policy)
            .send()
            .await
            .map_err(|error| {
                IacError::AwsSdk(format!(
                    "s3:PutBucketPolicy: {}",
                    error.into_service_error()
                ))
            })?;
        Ok(())
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
            Err(error) => {
                let service_error = error.into_service_error();
                if service_error.code() == Some("NoSuchTagSet") {
                    Ok(HashMap::new())
                } else {
                    Err(IacError::AwsSdk(format!(
                        "s3:GetBucketTagging: {service_error}"
                    )))
                }
            }
        }
    }

    async fn observed_versioning(&self, ctx: &ProvisionContext) -> Result<bool, IacError> {
        let output = self
            .aws(ctx)
            .s3
            .get_bucket_versioning()
            .bucket(&self.bucket_name)
            .send()
            .await
            .map_err(|error| {
                IacError::AwsSdk(format!(
                    "s3:GetBucketVersioning: {}",
                    error.into_service_error()
                ))
            })?;
        Ok(output.status() == Some(&aws_sdk_s3::types::BucketVersioningStatus::Enabled))
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
            Err(error) => {
                let service_error = error.into_service_error();
                if service_error.code() == Some("NoSuchBucketPolicy") {
                    Ok(None)
                } else {
                    Err(IacError::AwsSdk(format!(
                        "s3:GetBucketPolicy: {service_error}"
                    )))
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
            dependencies: Vec::new(),
            created_at,
            updated_at: chrono::Utc::now().to_rfc3339(),
            module: self.module.clone(),
        })
    }
}

fn extract_snapshot_policy_prefix(
    policy: &str,
    bucket_name: &str,
) -> Result<Option<String>, IacError> {
    let value: serde_json::Value = serde_json::from_str(policy).map_err(|error| {
        IacError::AwsSdk(format!(
            "s3:GetBucketPolicy: invalid bucket policy JSON: {error}"
        ))
    })?;
    let Some(statements) = value.get("Statement").and_then(serde_json::Value::as_array) else {
        return Ok(None);
    };
    for statement in statements {
        if statement.get("Sid").and_then(serde_json::Value::as_str) != Some("DenySnapshotDeletes") {
            continue;
        }
        let resource = statement
            .get("Resource")
            .and_then(serde_json::Value::as_str)
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
        ResourceType::new(Self::TYPE)
    }

    fn validate_input(&self) -> Result<(), String> {
        if self.bucket_name.trim().is_empty() {
            return Err("remote-state bucket name must not be empty".into());
        }
        if self.region.trim().is_empty() {
            return Err("remote-state bucket region must not be empty".into());
        }
        Ok(())
    }

    fn desired_manifest(&self) -> serde_json::Value {
        serde_json::json!({
            "bucket_name": self.bucket_name,
            "region": self.region,
            "key_prefix": self.key_prefix,
            "versioning": true,
            "preserve_on_delete": true,
        })
    }

    fn resource_id(&self) -> ResourceId {
        ResourceId(format!("s3-{}", self.bucket_name))
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        Vec::new()
    }

    fn module(&self) -> &str {
        &self.module
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        let mut request = self.aws(ctx).s3.create_bucket().bucket(&self.bucket_name);
        if self.region != "us-east-1" {
            request = request.create_bucket_configuration(
                aws_sdk_s3::types::CreateBucketConfiguration::builder()
                    .location_constraint(aws_sdk_s3::types::BucketLocationConstraint::from(
                        self.region.as_str(),
                    ))
                    .build(),
            );
        }

        let mut managed_snapshot_policy = true;
        match request.send().await {
            Ok(_) => {}
            Err(error) => {
                let service_error = error.into_service_error();
                if service_error.is_bucket_already_owned_by_you() {
                    managed_snapshot_policy = false;
                    tracing::warn!(
                        bucket = %self.bucket_name,
                        "bucket already owned by this account; adopting without taking policy ownership"
                    );
                } else {
                    return Err(IacError::AwsSdk(format!(
                        "s3:CreateBucket: {service_error}"
                    )));
                }
            }
        }

        self.ensure_public_access_block(ctx).await?;
        self.ensure_versioning(ctx).await?;
        if managed_snapshot_policy {
            self.put_snapshot_policy(ctx).await?;
        }
        self.observe_state(
            ctx,
            chrono::Utc::now().to_rfc3339(),
            managed_snapshot_policy,
        )
        .await
    }

    async fn update(
        &self,
        current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<ResourceState, IacError> {
        self.ensure_versioning(ctx).await?;
        let managed_snapshot_policy = Self::managed_snapshot_policy(current);
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
        tracing::info!(
            bucket = %self.bucket_name,
            "shared remote-state bucket is not deleted"
        );
        Ok(())
    }

    async fn describe(&self, ctx: &ProvisionContext) -> Result<DescribeResult, IacError> {
        match self
            .aws(ctx)
            .s3
            .head_bucket()
            .bucket(&self.bucket_name)
            .send()
            .await
        {
            Ok(_) => {
                let managed_snapshot_policy = self.persisted_managed_snapshot_policy(ctx);
                let created_at = ctx
                    .state
                    .resources
                    .get(&self.resource_id())
                    .map(|state| state.created_at.clone())
                    .unwrap_or_else(|| chrono::Utc::now().to_rfc3339());
                self.observe_state(ctx, created_at, managed_snapshot_policy)
                    .await
                    .map(DescribeResult::Present)
            }
            Err(error) => {
                let service_error = error.into_service_error();
                if service_error.is_not_found() {
                    Ok(DescribeResult::Absent)
                } else {
                    Err(IacError::AwsSdk(format!("s3:HeadBucket: {service_error}")))
                }
            }
        }
    }

    fn diff(&self, current: &ResourceState, _ctx: &ProvisionContext) -> InternalChange {
        let current_versioning = current
            .properties
            .get("versioning")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let current_snapshot_policy = current
            .properties
            .get("snapshot_delete_prevention_prefix")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let desired_snapshot_policy = self.snapshot_delete_prevention_prefix().unwrap_or_default();

        let detail = if !current_versioning {
            Some("versioning is not enabled")
        } else if Self::managed_snapshot_policy(current)
            && current_snapshot_policy != desired_snapshot_policy
        {
            Some("managed snapshot protection changed")
        } else {
            None
        };
        match detail {
            Some(detail) => InternalChange::Update {
                resource_id: self.resource_id(),
                resource_type: self.resource_type(),
                details: vec![tokeira_iac::FieldDiff::observation(detail)],
            },
            None => InternalChange::NoChange {
                resource_id: self.resource_id(),
            },
        }
    }

    fn change_semantics(&self, ctx: &SemanticsContext<'_>) -> ChangeSemantics {
        const CREATE: Citation = Citation::code(concat!(
            module_path!(),
            "::create — creates or adopts S3 storage, enforces public blocking and versioning, \
             and writes prefix-scoped snapshot protection only for a newly created bucket"
        ));
        const UPDATE: Citation = Citation::code(concat!(
            module_path!(),
            "::update — reconciles versioning and only the snapshot policy recorded as managed; \
             objects and adopted policies are untouched"
        ));
        const DELETE: Citation = Citation::code(concat!(
            module_path!(),
            "::delete — deliberately a no-op: only the engine record is retired"
        ));
        let claims = |operation, data_effect, citation: Citation| ChangeSemantics {
            operation: Confidence::EngineFact {
                value: operation,
                citation: citation.clone(),
            },
            replacement: Confidence::EngineFact {
                value: ReplacementPolicy::NotRequired,
                citation: citation.clone(),
            },
            disruption: Confidence::EngineFact {
                value: Disruption::None,
                citation: citation.clone(),
            },
            data_effect: Confidence::EngineFact {
                value: data_effect,
                citation: citation.clone(),
            },
            reversibility: Confidence::EngineFact {
                value: Reversibility::Reversible,
                citation,
            },
            statement: None,
            provider_assigned: Vec::new(),
        };
        match ctx.kind {
            ChangeKind::Create => {
                claims(LifecycleOperation::Created, DataEffect::NoDataHeld, CREATE)
            }
            ChangeKind::Update | ChangeKind::Replace => claims(
                LifecycleOperation::UpdatedInPlace,
                DataEffect::Preserved,
                UPDATE,
            ),
            ChangeKind::Delete => {
                claims(LifecycleOperation::Deleted, DataEffect::Preserved, DELETE)
            }
            ChangeKind::NoChange => ChangeSemantics::default(),
        }
    }

    fn display_kind(&self) -> Option<&'static str> {
        Some("remote-state bucket")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bucket() -> RemoteStateBucket {
        RemoteStateBucket::new(
            "demo-state-eu-west-2",
            "eu-west-2",
            Some("demo/dev".into()),
            "remote-state",
        )
    }

    fn state(
        tags: serde_json::Value,
        prefix: &str,
        managed_snapshot_policy: bool,
        versioning: bool,
    ) -> ResourceState {
        ResourceState {
            resource_type: ResourceType::new(RemoteStateBucket::TYPE),
            physical_id: "demo-state-eu-west-2".into(),
            properties: serde_json::json!({
                "bucket_name": "demo-state-eu-west-2",
                "versioning": versioning,
                "snapshot_delete_prevention_prefix": prefix,
                "managed_snapshot_policy": managed_snapshot_policy,
                "tags": tags,
            }),
            dependencies: Vec::new(),
            created_at: String::new(),
            updated_at: String::new(),
            module: "remote-state".into(),
        }
    }

    #[test]
    fn shared_tag_drift_is_not_desired_state() {
        let change = bucket().diff(
            &state(
                serde_json::json!({ "Project": "another-deployment" }),
                "demo/dev/snapshots/",
                true,
                true,
            ),
            &ProvisionContext::default(),
        );
        assert!(matches!(change, InternalChange::NoChange { .. }));
    }

    #[test]
    fn managed_snapshot_policy_drift_is_reconciled() {
        let change = bucket().diff(
            &state(serde_json::json!({}), "another/snapshots/", true, true),
            &ProvisionContext::default(),
        );
        assert!(matches!(change, InternalChange::Update { .. }));
    }

    #[test]
    fn adopted_snapshot_policy_is_not_owned() {
        let change = bucket().diff(
            &state(serde_json::json!({}), "another/snapshots/", false, true),
            &ProvisionContext::default(),
        );
        assert!(matches!(change, InternalChange::NoChange { .. }));
    }

    #[test]
    fn versioning_is_required_even_for_an_adopted_bucket() {
        let change = bucket().diff(
            &state(serde_json::json!({}), "another/snapshots/", false, false),
            &ProvisionContext::default(),
        );
        assert!(matches!(change, InternalChange::Update { .. }));
    }

    #[test]
    fn extracted_policy_prefix_is_scoped_to_the_named_statement() {
        let policy = bucket()
            .snapshot_delete_prevention_policy()
            .expect("non-empty prefix has a policy");
        assert_eq!(
            extract_snapshot_policy_prefix(&policy, "demo-state-eu-west-2")
                .expect("policy parses")
                .as_deref(),
            Some("demo/dev/snapshots/")
        );
    }
}
