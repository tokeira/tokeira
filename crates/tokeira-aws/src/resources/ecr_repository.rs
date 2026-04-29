use std::collections::HashMap;

use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use tokeira_iac::error::IacError;
use tokeira_iac::{
    InternalChange, ProvisionContext, Resource, ResourceId, ResourceState, ResourceType,
};

/// Configuration for a single ECR repository provider resource.
#[derive(Debug)]
pub struct EcrRepositoryConfig {
    pub lifecycle_policy: String,
    pub module: String,
}

/// Generic provider resource that provisions exactly one ECR repository.
#[derive(Debug)]
pub struct EcrRepository {
    pub repo_name: String,
    pub config: EcrRepositoryConfig,
    pub project: String,
    pub region: String,
    pub tags: HashMap<String, String>,
}

impl EcrRepository {
    pub fn new(
        repo_name: String,
        config: EcrRepositoryConfig,
        rctx: &crate::ResourceContext,
    ) -> Self {
        Self {
            repo_name,
            config,
            project: rctx.project.clone(),
            region: rctx.region.clone(),
            tags: rctx.tags.clone(),
        }
    }

    fn lifecycle_policy_change_detail(&self, current: &str) -> String {
        let current_norm = normalize_lifecycle_policy(current);
        let desired_norm = normalize_lifecycle_policy(&self.config.lifecycle_policy);
        format!(
            "lifecycle policy changed (rules {} -> {}, current_sha={}, desired_sha={})",
            lifecycle_rule_count(&current_norm),
            lifecycle_rule_count(&desired_norm),
            short_sha(&current_norm),
            short_sha(&desired_norm),
        )
    }
}

fn normalize_lifecycle_policy(policy: &str) -> String {
    match serde_json::from_str::<Value>(policy) {
        Ok(value) => canonical_json_string(&normalize_lifecycle_value(value)),
        Err(_) => policy.trim().to_string(),
    }
}

fn normalize_lifecycle_value(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut normalized = Map::new();
            for (key, value) in map {
                let normalized_value = if key == "rules" {
                    normalize_rules(value)
                } else {
                    normalize_lifecycle_value(value)
                };
                normalized.insert(key, normalized_value);
            }
            Value::Object(normalized)
        }
        Value::Array(values) => {
            Value::Array(values.into_iter().map(normalize_lifecycle_value).collect())
        }
        other => other,
    }
}

fn normalize_rules(value: Value) -> Value {
    let mut rules = match value {
        Value::Array(values) => values
            .into_iter()
            .map(normalize_lifecycle_value)
            .collect::<Vec<_>>(),
        other => vec![normalize_lifecycle_value(other)],
    };
    rules.sort_by_key(|rule| {
        rule.get("rulePriority")
            .and_then(Value::as_i64)
            .unwrap_or(i64::MAX)
    });
    Value::Array(rules)
}

fn canonical_json_string(value: &Value) -> String {
    serde_json::to_string(value).expect("value should serialize")
}

fn lifecycle_rule_count(policy: &str) -> usize {
    serde_json::from_str::<Value>(policy)
        .ok()
        .and_then(|value| value.get("rules").and_then(Value::as_array).map(Vec::len))
        .unwrap_or(0)
}

fn short_sha(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    hex::encode(&digest[..4])
}

#[async_trait::async_trait]
impl Resource for EcrRepository {
    fn resource_type(&self) -> ResourceType {
        ResourceType::new("EcrRepository")
    }

    fn resource_id(&self) -> ResourceId {
        ResourceId(format!("ecr-{}", self.repo_name))
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        vec![]
    }

    fn module(&self) -> &str {
        &self.config.module
    }

    fn diff(&self, current: &ResourceState, _ctx: &ProvisionContext) -> InternalChange {
        let current_tags: HashMap<String, String> = current
            .properties
            .get("tags")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();
        let current_lifecycle_policy = current
            .properties
            .get("lifecycle_policy")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let current_lifecycle_policy_normalized =
            normalize_lifecycle_policy(current_lifecycle_policy);
        let desired_lifecycle_policy_normalized =
            normalize_lifecycle_policy(&self.config.lifecycle_policy);
        let desired_tags = {
            let mut tags = self.tags.clone();
            tags.insert("Name".into(), self.repo_name.clone());
            tags.insert("Project".into(), self.project.clone());
            tags.insert("ManagedBy".into(), "tokeira-cli".into());
            tags
        };

        if current_tags != desired_tags {
            InternalChange::Update {
                resource_id: self.resource_id(),
                resource_type: self.resource_type(),
                details: "tags changed".into(),
            }
        } else if current_lifecycle_policy_normalized
            != desired_lifecycle_policy_normalized
        {
            InternalChange::Update {
                resource_id: self.resource_id(),
                resource_type: self.resource_type(),
                details: self.lifecycle_policy_change_detail(current_lifecycle_policy),
            }
        } else {
            InternalChange::NoChange {
                resource_id: self.resource_id(),
            }
        }
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        let name = &self.repo_name;
        let tags = ctx.resource_tags(name);
        let ecr_tag_list = super::ecr_tags(&tags);

        let mut repository_uri = String::new();
        let mut repository_arn = String::new();

        // ecr:CreateRepository with scanOnPush=true + tags
        match ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ecr
            .create_repository()
            .repository_name(name)
            .image_scanning_configuration(
                aws_sdk_ecr::types::ImageScanningConfiguration::builder()
                    .scan_on_push(true)
                    .build(),
            )
            .set_tags(Some(ecr_tag_list.clone()))
            .send()
            .await
        {
            Ok(output) => {
                if let Some(r) = output.repository() {
                    repository_uri = r.repository_uri().unwrap_or_default().to_string();
                    repository_arn = r.repository_arn().unwrap_or_default().to_string();
                }
            }
            Err(e) => {
                let svc_err = e.into_service_error();
                if svc_err.is_repository_already_exists_exception() {
                    tracing::warn!(repo = %name, "repository already exists, adopting");
                    // ecr:DescribeRepositories to get URI/ARN of existing repo
                    let desc = ctx
                        .extension::<crate::AwsClients>()
                        .expect("AwsClients")
                        .ecr
                        .describe_repositories()
                        .repository_names(name)
                        .send()
                        .await
                        .map_err(|e| {
                            IacError::AwsSdk(format!(
                                "ecr:DescribeRepositories: {}",
                                e.into_service_error()
                            ))
                        })?;
                    if let Some(r) = desc.repositories().first() {
                        repository_uri =
                            r.repository_uri().unwrap_or_default().to_string();
                        repository_arn =
                            r.repository_arn().unwrap_or_default().to_string();
                    }

                    // ecr:TagResource to ensure tags are up-to-date on adopted repo
                    if !repository_arn.is_empty() {
                        ctx.extension::<crate::AwsClients>()
                            .expect("AwsClients")
                            .ecr
                            .tag_resource()
                            .resource_arn(&repository_arn)
                            .set_tags(Some(ecr_tag_list))
                            .send()
                            .await
                            .map_err(|e| {
                                IacError::AwsSdk(format!(
                                    "ecr:TagResource: {}",
                                    e.into_service_error()
                                ))
                            })?;
                    }
                } else {
                    return Err(IacError::AwsSdk(format!(
                        "ecr:CreateRepository: {svc_err}"
                    )));
                }
            }
        }

        // ecr:PutLifecyclePolicy
        ctx.extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ecr
            .put_lifecycle_policy()
            .repository_name(name)
            .lifecycle_policy_text(&self.config.lifecycle_policy)
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!(
                    "ecr:PutLifecyclePolicy: {}",
                    e.into_service_error()
                ))
            })?;

        let now = chrono::Utc::now().to_rfc3339();
        Ok(ResourceState {
            resource_type: ResourceType::new("EcrRepository"),
            physical_id: repository_arn,
            properties: serde_json::json!({
                "repository_name": self.repo_name,
                "repository_uri": repository_uri,
                "scan_on_push": true,
                "lifecycle_policy": self.config.lifecycle_policy,
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
        let name = &self.repo_name;
        let tags = ctx.resource_tags(name);
        let ecr_tag_list = super::ecr_tags(&tags);

        // ecr:TagResource to update tags
        if !current.physical_id.is_empty() {
            ctx.extension::<crate::AwsClients>()
                .expect("AwsClients")
                .ecr
                .tag_resource()
                .resource_arn(&current.physical_id)
                .set_tags(Some(ecr_tag_list))
                .send()
                .await
                .map_err(|e| {
                    IacError::AwsSdk(format!(
                        "ecr:TagResource: {}",
                        e.into_service_error()
                    ))
                })?;
        }

        // ecr:PutLifecyclePolicy to ensure policy is current
        ctx.extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ecr
            .put_lifecycle_policy()
            .repository_name(name)
            .lifecycle_policy_text(&self.config.lifecycle_policy)
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!(
                    "ecr:PutLifecyclePolicy: {}",
                    e.into_service_error()
                ))
            })?;

        Ok(ResourceState {
            resource_type: current.resource_type.clone(),
            physical_id: current.physical_id.clone(),
            properties: serde_json::json!({
                "repository_name": self.repo_name,
                "repository_uri": current.properties.get("repository_uri")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default(),
                "scan_on_push": true,
                "lifecycle_policy": self.config.lifecycle_policy,
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
        let name = &self.repo_name;

        // ecr:DeleteRepository with force=true to delete images
        match ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ecr
            .delete_repository()
            .repository_name(name)
            .force(true)
            .send()
            .await
        {
            Ok(_) => {}
            Err(e) => {
                let svc_err = e.into_service_error();
                if svc_err.is_repository_not_found_exception() {
                    tracing::warn!(repo = %name, "repository does not exist, skipping deletion");
                } else {
                    return Err(IacError::AwsSdk(format!(
                        "ecr:DeleteRepository: {svc_err}"
                    )));
                }
            }
        }

        Ok(())
    }

    async fn describe(
        &self,
        ctx: &ProvisionContext,
    ) -> Result<Option<ResourceState>, IacError> {
        let name = &self.repo_name;

        // ecr:DescribeRepositories for this single repo
        match ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ecr
            .describe_repositories()
            .repository_names(name)
            .send()
            .await
        {
            Ok(output) => {
                if let Some(r) = output.repositories().first() {
                    let repository_arn =
                        r.repository_arn().unwrap_or_default().to_string();
                    let tags = if repository_arn.is_empty() {
                        HashMap::new()
                    } else {
                        ctx.extension::<crate::AwsClients>()
                            .expect("AwsClients")
                            .ecr
                            .list_tags_for_resource()
                            .resource_arn(&repository_arn)
                            .send()
                            .await
                            .map_err(|e| {
                                IacError::AwsSdk(format!(
                                    "ecr:ListTagsForResource: {}",
                                    e.into_service_error()
                                ))
                            })?
                            .tags()
                            .iter()
                            .map(|tag| (tag.key().to_string(), tag.value().to_string()))
                            .collect()
                    };
                    let lifecycle_policy = match ctx
                        .extension::<crate::AwsClients>()
                        .expect("AwsClients")
                        .ecr
                        .get_lifecycle_policy()
                        .repository_name(name)
                        .send()
                        .await
                    {
                        Ok(output) => output
                            .lifecycle_policy_text()
                            .unwrap_or_default()
                            .to_string(),
                        Err(e) => {
                            let svc_err = e.into_service_error();
                            if svc_err.is_lifecycle_policy_not_found_exception() {
                                String::new()
                            } else {
                                return Err(IacError::AwsSdk(format!(
                                    "ecr:GetLifecyclePolicy: {svc_err}"
                                )));
                            }
                        }
                    };
                    let now = chrono::Utc::now().to_rfc3339();
                    Ok(Some(ResourceState {
                        resource_type: ResourceType::new("EcrRepository"),
                        physical_id: repository_arn,
                        properties: serde_json::json!({
                            "repository_name": self.repo_name,
                            "repository_uri": r.repository_uri().unwrap_or_default(),
                            "lifecycle_policy": lifecycle_policy,
                            "tags": tags,
                        }),
                        dependencies: vec![],
                        created_at: now.clone(),
                        updated_at: now,
                        module: self.module().to_owned(),
                    }))
                } else {
                    Ok(None)
                }
            }
            Err(e) => {
                let svc_err = e.into_service_error();
                if svc_err.is_repository_not_found_exception() {
                    Ok(None)
                } else {
                    Err(IacError::AwsSdk(format!(
                        "ecr:DescribeRepositories: {svc_err}"
                    )))
                }
            }
        }
    }
}
