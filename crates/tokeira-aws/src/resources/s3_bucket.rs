use std::collections::HashMap;

use tokeira_iac::{
    ChangeKind, ChangeSemantics, Citation, Confidence, DataEffect, DescribeResult, Disruption,
    InternalChange, LifecycleOperation, ProvisionContext, ReplacementPolicy, Resource, ResourceId,
    ResourceState, ResourceType, Reversibility, SemanticsContext, error::IacError,
};

/// Configuration for a single S3 bucket provider resource.
#[derive(Debug, Clone)]
pub struct S3BucketConfig {
    pub versioning: bool,
    pub module: String,
    /// Optional key prefix for snapshot delete-prevention bucket policy.
    pub key_prefix: Option<String>,
}

/// Generic provider resource that provisions exactly one S3 bucket.
#[derive(Debug)]
pub struct S3Bucket {
    pub(crate) bucket_name: String,
    pub(crate) config: S3BucketConfig,
    pub project: String,
    pub(crate) region: String,
    pub tags: HashMap<String, String>,
    pub(crate) key_prefix: Option<String>,
}

impl S3Bucket {
    pub fn new(bucket_name: String, config: S3BucketConfig, rctx: &crate::ResourceContext) -> Self {
        let key_prefix = config.key_prefix.clone();
        Self {
            bucket_name,
            config,
            project: rctx.project.clone(),
            region: rctx.region.clone(),
            tags: rctx.tags.clone(),
            key_prefix,
        }
    }

    fn snapshot_delete_prevention_policy(&self, _ctx: &ProvisionContext) -> Option<String> {
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
                    "Resource": format!("arn:aws:s3:::{}/{}/snapshots/*", self.bucket_name, key_prefix)
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
}

fn normalize_s3_tags(tag_set: &[aws_sdk_s3::types::Tag]) -> HashMap<String, String> {
    tag_set
        .iter()
        .map(|tag| (tag.key().to_string(), tag.value().to_string()))
        .collect()
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
impl Resource for S3Bucket {
    fn change_semantics(&self, ctx: &SemanticsContext<'_>) -> ChangeSemantics {
        const CREATE: Citation = Citation::code(concat!(
            module_path!(),
            "::create — s3:CreateBucket (region-constrained), then \
             s3:PutBucketTagging / PutBucketVersioning / PutBucketPolicy per \
             config"
        ));
        const UPDATE: Citation = Citation::code(concat!(
            module_path!(),
            "::update — in-place reconcile of the same bucket-level Puts \
             (tagging, versioning, policy); objects are never touched"
        ));
        const DELETE: Citation = Citation::code(concat!(
            module_path!(),
            "::delete — s3:DeleteBucket only (NoSuchBucket tolerated); this \
             module never deletes objects, and S3 refuses to remove a \
             non-empty bucket, so stored data cannot be destroyed here"
        ));
        let claims =
            |operation, data_effect: Confidence<DataEffect>, citation: Citation| ChangeSemantics {
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
                data_effect,
                reversibility: Confidence::EngineFact {
                    value: Reversibility::Reversible,
                    citation,
                },
                statement: None,
                provider_assigned: Vec::new(),
            };
        match ctx.kind {
            ChangeKind::Create => claims(
                LifecycleOperation::Created,
                Confidence::EngineFact {
                    value: DataEffect::NoDataHeld,
                    citation: CREATE,
                },
                CREATE,
            ),
            ChangeKind::Update | ChangeKind::Replace => claims(
                LifecycleOperation::UpdatedInPlace,
                Confidence::EngineFact {
                    value: DataEffect::Preserved,
                    citation: UPDATE,
                },
                UPDATE,
            ),
            // Preserved is an inference: our fact is that only DeleteBucket
            // is issued; that a non-empty bucket refuses (so objects survive
            // any outcome) is S3's behaviour, not a call we make.
            ChangeKind::Delete => claims(
                LifecycleOperation::Deleted,
                Confidence::Inference {
                    value: DataEffect::Preserved,
                    citation: DELETE,
                },
                DELETE,
            ),
            ChangeKind::NoChange => ChangeSemantics::default(),
        }
    }

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
        &self.config.module
    }

    fn diff(&self, current: &ResourceState, ctx: &ProvisionContext) -> InternalChange {
        let current_tags: HashMap<String, String> = current
            .properties
            .get("tags")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();
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

        // Build the desired tag set via ProvisionContext.
        let desired_tags = ctx.resource_tags(&self.bucket_name);
        let desired_snapshot_policy = self.snapshot_delete_prevention_prefix().unwrap_or_default();

        if current_tags != desired_tags {
            InternalChange::Update {
                resource_id: self.resource_id(),
                resource_type: self.resource_type(),
                details: vec![tokeira_iac::FieldDiff::observation("tags changed")],
            }
        } else if current_versioning != self.config.versioning {
            InternalChange::Update {
                resource_id: self.resource_id(),
                resource_type: self.resource_type(),
                details: vec![tokeira_iac::FieldDiff::observation("versioning changed")],
            }
        } else if current_snapshot_policy != desired_snapshot_policy {
            InternalChange::Update {
                resource_id: self.resource_id(),
                resource_type: self.resource_type(),
                details: vec![tokeira_iac::FieldDiff::observation(
                    "remote state snapshot protection changed",
                )],
            }
        } else {
            InternalChange::NoChange {
                resource_id: self.resource_id(),
            }
        }
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        let name = &self.bucket_name;
        let tags = ctx.resource_tags(name);
        let region = &self.region;

        // s3:CreateBucket
        let mut req = ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .s3
            .create_bucket()
            .bucket(name);

        // us-east-1 is the default region and must NOT use CreateBucketConfiguration
        if region != "us-east-1" {
            req = req.create_bucket_configuration(
                aws_sdk_s3::types::CreateBucketConfiguration::builder()
                    .location_constraint(aws_sdk_s3::types::BucketLocationConstraint::from(
                        region.as_str(),
                    ))
                    .build(),
            );
        }

        match req.send().await {
            Ok(_) => {}
            Err(e) => {
                let svc_err = e.into_service_error();
                if svc_err.is_bucket_already_owned_by_you() {
                    tracing::warn!(bucket = %name, "bucket already owned by us, adopting");
                } else {
                    return Err(IacError::AwsSdk(format!("s3:CreateBucket: {svc_err}")));
                }
            }
        }

        // s3:PutPublicAccessBlock — block all public access
        ctx.extension::<crate::AwsClients>()
            .expect("AwsClients")
            .s3
            .put_public_access_block()
            .bucket(name)
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
            .map_err(|e| {
                IacError::AwsSdk(format!(
                    "s3:PutPublicAccessBlock: {}",
                    e.into_service_error()
                ))
            })?;

        let s3_tag_set = super::s3_tags(&tags);
        let tagging = aws_sdk_s3::types::Tagging::builder()
            .set_tag_set(Some(s3_tag_set))
            .build()
            .map_err(|e| IacError::AwsSdk(format!("s3:PutBucketTagging build: {e}")))?;
        ctx.extension::<crate::AwsClients>()
            .expect("AwsClients")
            .s3
            .put_bucket_tagging()
            .bucket(name)
            .tagging(tagging)
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!("s3:PutBucketTagging: {}", e.into_service_error()))
            })?;

        // s3:PutBucketVersioning — only when versioning is enabled
        if self.config.versioning {
            ctx.extension::<crate::AwsClients>()
                .expect("AwsClients")
                .s3
                .put_bucket_versioning()
                .bucket(name)
                .versioning_configuration(
                    aws_sdk_s3::types::VersioningConfiguration::builder()
                        .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                        .build(),
                )
                .send()
                .await
                .map_err(|e| {
                    IacError::AwsSdk(format!(
                        "s3:PutBucketVersioning: {}",
                        e.into_service_error()
                    ))
                })?;
        }

        let snapshot_delete_prevention_prefix =
            if let Some(policy) = self.snapshot_delete_prevention_policy(ctx) {
                ctx.extension::<crate::AwsClients>()
                    .expect("AwsClients")
                    .s3
                    .put_bucket_policy()
                    .bucket(name)
                    .policy(policy)
                    .send()
                    .await
                    .map_err(|e| {
                        IacError::AwsSdk(format!("s3:PutBucketPolicy: {}", e.into_service_error()))
                    })?;
                self.snapshot_delete_prevention_prefix()
            } else {
                None
            };

        let now = chrono::Utc::now().to_rfc3339();
        Ok(ResourceState {
            resource_type: ResourceType::new("S3Bucket"),
            physical_id: self.bucket_name.clone(),
            properties: serde_json::json!({
                "bucket_name": self.bucket_name,
                "versioning": self.config.versioning,
                "snapshot_delete_prevention_prefix": snapshot_delete_prevention_prefix,
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
        ctx: &ProvisionContext,
    ) -> Result<ResourceState, IacError> {
        let name = &self.bucket_name;
        let tags = ctx.resource_tags(name);

        let s3_tag_set = super::s3_tags(&tags);
        let tagging = aws_sdk_s3::types::Tagging::builder()
            .set_tag_set(Some(s3_tag_set))
            .build()
            .map_err(|e| IacError::AwsSdk(format!("s3:PutBucketTagging build: {e}")))?;
        ctx.extension::<crate::AwsClients>()
            .expect("AwsClients")
            .s3
            .put_bucket_tagging()
            .bucket(name)
            .tagging(tagging)
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!("s3:PutBucketTagging: {}", e.into_service_error()))
            })?;

        if self.config.versioning {
            ctx.extension::<crate::AwsClients>()
                .expect("AwsClients")
                .s3
                .put_bucket_versioning()
                .bucket(name)
                .versioning_configuration(
                    aws_sdk_s3::types::VersioningConfiguration::builder()
                        .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                        .build(),
                )
                .send()
                .await
                .map_err(|e| {
                    IacError::AwsSdk(format!(
                        "s3:PutBucketVersioning: {}",
                        e.into_service_error()
                    ))
                })?;
        }

        let snapshot_delete_prevention_prefix =
            if let Some(policy) = self.snapshot_delete_prevention_policy(ctx) {
                ctx.extension::<crate::AwsClients>()
                    .expect("AwsClients")
                    .s3
                    .put_bucket_policy()
                    .bucket(name)
                    .policy(policy)
                    .send()
                    .await
                    .map_err(|e| {
                        IacError::AwsSdk(format!("s3:PutBucketPolicy: {}", e.into_service_error()))
                    })?;
                self.snapshot_delete_prevention_prefix()
            } else {
                None
            };

        Ok(ResourceState {
            resource_type: current.resource_type.clone(),
            physical_id: current.physical_id.clone(),
            properties: serde_json::json!({
                "bucket_name": self.bucket_name,
                "versioning": self.config.versioning,
                "snapshot_delete_prevention_prefix": snapshot_delete_prevention_prefix,
                "tags": tags,
            }),
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
        let name = &self.bucket_name;

        match ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .s3
            .delete_bucket()
            .bucket(name)
            .send()
            .await
        {
            Ok(_) => {}
            Err(e) => {
                let svc_err = e.into_service_error();
                let err_str = format!("{svc_err}");
                let err_code = format!("{svc_err:?}");
                if err_str.contains("NoSuchBucket") || err_code.contains("NoSuchBucket") {
                    tracing::warn!(bucket = %name, "bucket does not exist, skipping deletion");
                } else {
                    return Err(IacError::AwsSdk(format!("s3:DeleteBucket: {svc_err}")));
                }
            }
        }

        Ok(())
    }

    async fn describe(&self, ctx: &ProvisionContext) -> Result<DescribeResult, IacError> {
        let name = &self.bucket_name;

        match ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .s3
            .head_bucket()
            .bucket(name)
            .send()
            .await
        {
            Ok(_) => {
                let tags = match ctx
                    .extension::<crate::AwsClients>()
                    .expect("AwsClients")
                    .s3
                    .get_bucket_tagging()
                    .bucket(name)
                    .send()
                    .await
                {
                    Ok(output) => normalize_s3_tags(output.tag_set()),
                    Err(e) => {
                        let svc_err = e.into_service_error();
                        let message = format!("{svc_err}");
                        if message.contains("NoSuchTagSet") {
                            HashMap::new()
                        } else {
                            return Err(IacError::AwsSdk(format!(
                                "s3:GetBucketTagging: {svc_err}"
                            )));
                        }
                    }
                };
                let versioning = ctx
                    .extension::<crate::AwsClients>()
                    .expect("AwsClients")
                    .s3
                    .get_bucket_versioning()
                    .bucket(name)
                    .send()
                    .await
                    .map_err(|e| {
                        IacError::AwsSdk(format!(
                            "s3:GetBucketVersioning: {}",
                            e.into_service_error()
                        ))
                    })?;
                let snapshot_delete_prevention_prefix = match ctx
                    .extension::<crate::AwsClients>()
                    .expect("AwsClients")
                    .s3
                    .get_bucket_policy()
                    .bucket(name)
                    .send()
                    .await
                {
                    Ok(output) => {
                        extract_snapshot_policy_prefix(output.policy().unwrap_or_default(), name)?
                    }
                    Err(e) => {
                        let svc_err = e.into_service_error();
                        let message = format!("{svc_err}");
                        if message.contains("NoSuchBucketPolicy") {
                            None
                        } else {
                            return Err(IacError::AwsSdk(format!("s3:GetBucketPolicy: {svc_err}")));
                        }
                    }
                };
                let now = chrono::Utc::now().to_rfc3339();
                Ok(DescribeResult::Present(ResourceState {
                    resource_type: ResourceType::new("S3Bucket"),
                    physical_id: self.bucket_name.clone(),
                    properties: serde_json::json!({
                        "bucket_name": self.bucket_name,
                        "versioning": versioning.status()
                            == Some(&aws_sdk_s3::types::BucketVersioningStatus::Enabled),
                        "snapshot_delete_prevention_prefix": snapshot_delete_prevention_prefix,
                        "tags": tags,
                    }),
                    dependencies: vec![],
                    created_at: now.clone(),
                    updated_at: now,
                    module: self.module().to_owned(),
                }))
            }
            Err(e) => {
                let svc_err = e.into_service_error();
                if svc_err.is_not_found() {
                    Ok(DescribeResult::Absent)
                } else {
                    Err(IacError::AwsSdk(format!("s3:HeadBucket: {svc_err}")))
                }
            }
        }
    }
}
