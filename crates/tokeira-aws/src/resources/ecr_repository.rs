use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokeira_iac::{
    ChangeKind, ChangeSemantics, Citation, Confidence, DataEffect, DescribeResult, Disruption,
    InternalChange, LifecycleOperation, ProvisionContext, ReplacementPolicy, Resource, ResourceId,
    ResourceState, ResourceType, Reversibility, SemanticsContext, error::IacError,
};

use crate::{EcrError, ImageTagMutability, RepositoryDescription, clients::ecr::ecr_client};

pub(crate) const ECR_LIFECYCLE_POLICY: &str = r#"{
  "rules": [
    {
      "rulePriority": 1,
      "description": "Keep last 10 untagged images",
      "selection": {
        "tagStatus": "untagged",
        "countType": "imageCountMoreThan",
        "countNumber": 10
      },
      "action": { "type": "expire" }
    }
  ]
}"#;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EcrRepository {
    name: String,
    module: String,
}

impl EcrRepository {
    pub fn new(name: String, module: String) -> Result<Self, EcrError> {
        validate_repository_name(&name)?;
        Ok(Self { name, module })
    }
}

#[async_trait::async_trait]
impl Resource for EcrRepository {
    fn change_semantics(&self, ctx: &SemanticsContext<'_>) -> ChangeSemantics {
        const CREATE: Citation = Citation::code(concat!(
            module_path!(),
            "::create — ecr:CreateRepository (mutable tags), then \
             ecr:PutLifecyclePolicy"
        ));
        const UPDATE: Citation = Citation::code(concat!(
            module_path!(),
            "::update — in-place reconcile: ecr:PutLifecyclePolicy and \
             ecr:TagResource; images are never touched"
        ));
        const DELETE: Citation = Citation::code(concat!(
            module_path!(),
            "::delete — ecr:DeleteRepository with force(true): the repository \
             AND every image stored in it are deleted by our own parameter \
             choice"
        ));
        let claims = |operation,
                      data_effect: Confidence<DataEffect>,
                      reversibility: Confidence<Reversibility>,
                      citation: Citation| ChangeSemantics {
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
                citation,
            },
            data_effect,
            reversibility,
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
                Confidence::EngineFact {
                    value: Reversibility::Reversible,
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
                Confidence::EngineFact {
                    value: Reversibility::Reversible,
                    citation: UPDATE,
                },
                UPDATE,
            ),
            // force(true) destroying the stored images is our parameter — an
            // engine fact. That the loss is recoverable only by re-pushing
            // (images are build artifacts, not unique data) is derived.
            ChangeKind::Delete => claims(
                LifecycleOperation::Deleted,
                Confidence::EngineFact {
                    value: DataEffect::Destroyed,
                    citation: DELETE,
                },
                Confidence::Inference {
                    value: Reversibility::ReversibleWithDataLoss,
                    citation: DELETE,
                },
                DELETE,
            ),
            ChangeKind::NoChange => ChangeSemantics::default(),
        }
    }

    fn resource_type(&self) -> ResourceType {
        ResourceType::new("EcrRepository")
    }

    fn resource_id(&self) -> ResourceId {
        ResourceId(format!("ecr-{}", self.name))
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        Vec::new()
    }

    fn module(&self) -> &str {
        &self.module
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        let ecr = ecr_client(ctx)?;
        let tags = ctx.resource_tags(&self.name);
        let desc = ecr
            .create_repository(&self.name, ImageTagMutability::Mutable, &tags)
            .await
            .map_err(IacError::from)?;
        ecr.put_lifecycle_policy(&self.name, ECR_LIFECYCLE_POLICY)
            .await
            .map_err(IacError::from)?;
        let live_tags = ecr
            .list_tags_for_resource(&desc.arn)
            .await
            .map_err(IacError::from)?;
        let live_policy = ecr
            .get_lifecycle_policy(&self.name)
            .await
            .map_err(IacError::from)?;
        Ok(state_from_live(
            &self.name,
            &desc,
            live_tags,
            live_policy.unwrap_or_default(),
            &self.module,
            None,
        ))
    }

    async fn update(
        &self,
        current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<ResourceState, IacError> {
        let ecr = ecr_client(ctx)?;
        let tags = ctx.resource_tags(&self.name);
        ecr.put_lifecycle_policy(&self.name, ECR_LIFECYCLE_POLICY)
            .await
            .map_err(IacError::from)?;
        ecr.tag_resource(&current.physical_id, &tags)
            .await
            .map_err(IacError::from)?;
        let desc = ecr
            .describe_repository(&self.name)
            .await
            .map_err(IacError::from)?;
        let live_tags = ecr
            .list_tags_for_resource(&desc.arn)
            .await
            .map_err(IacError::from)?;
        let live_policy = ecr
            .get_lifecycle_policy(&self.name)
            .await
            .map_err(IacError::from)?;
        Ok(state_from_live(
            &self.name,
            &desc,
            live_tags,
            live_policy.unwrap_or_default(),
            &self.module,
            Some(current.created_at.clone()),
        ))
    }

    async fn delete(
        &self,
        _current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<(), IacError> {
        let ecr = ecr_client(ctx)?;
        ecr.delete_repository(&self.name, true)
            .await
            .map_err(IacError::from)
    }

    async fn describe(&self, ctx: &ProvisionContext) -> Result<DescribeResult, IacError> {
        let ecr = ecr_client(ctx)?;
        let desc = match ecr.describe_repository(&self.name).await {
            Ok(desc) => desc,
            Err(EcrError::NotFound { .. }) => return Ok(DescribeResult::Absent),
            Err(err) => return Err(IacError::from(err)),
        };
        let live_tags = ecr
            .list_tags_for_resource(&desc.arn)
            .await
            .map_err(IacError::from)?;
        let live_policy = ecr
            .get_lifecycle_policy(&self.name)
            .await
            .map_err(IacError::from)?;
        Ok(DescribeResult::Present(state_from_live(
            &self.name,
            &desc,
            live_tags,
            live_policy.unwrap_or_default(),
            &self.module,
            None,
        )))
    }

    fn diff(&self, current: &ResourceState, ctx: &ProvisionContext) -> InternalChange {
        let current_tags: std::collections::HashMap<String, String> = current
            .properties
            .get("tags")
            .and_then(|value| serde_json::from_value(value.clone()).ok())
            .unwrap_or_default();
        let desired_tags = ctx.resource_tags(&self.name);
        if current_tags != desired_tags {
            return InternalChange::Update {
                resource_id: self.resource_id(),
                resource_type: self.resource_type(),
                details: vec![tokeira_iac::FieldDiff::observation("tags changed")],
            };
        }

        let current_policy = current
            .properties
            .get("lifecycle_policy")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if normalize_lifecycle_policy(current_policy)
            != normalize_lifecycle_policy(ECR_LIFECYCLE_POLICY)
        {
            return InternalChange::Update {
                resource_id: self.resource_id(),
                resource_type: self.resource_type(),
                details: vec![tokeira_iac::FieldDiff::observation(
                    "lifecycle policy changed",
                )],
            };
        }

        InternalChange::NoChange {
            resource_id: self.resource_id(),
        }
    }
}

impl From<EcrError> for IacError {
    fn from(value: EcrError) -> Self {
        IacError::Other(anyhow::anyhow!(value))
    }
}

pub(crate) fn state_from_live(
    name: &str,
    desc: &RepositoryDescription,
    live_tags: std::collections::HashMap<String, String>,
    live_policy: String,
    module: &str,
    created_at: Option<String>,
) -> ResourceState {
    let now = chrono::Utc::now().to_rfc3339();
    ResourceState {
        resource_type: ResourceType::new("EcrRepository"),
        physical_id: desc.arn.clone(),
        properties: serde_json::json!({
            "repository_name": name,
            "repository_uri": desc.uri,
            "lifecycle_policy": live_policy,
            "tags": live_tags,
        }),
        dependencies: Vec::new(),
        created_at: created_at.unwrap_or_else(|| now.clone()),
        updated_at: now,
        module: module.to_owned(),
    }
}

pub(crate) fn normalize_lifecycle_policy(policy: &str) -> String {
    match serde_json::from_str::<Value>(policy) {
        Ok(value) => canonical_json_string(&normalize_lifecycle_value(value)),
        Err(_) => policy.trim().to_owned(),
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
    serde_json::to_string(value).unwrap_or_else(|_| value.to_string())
}

fn validate_repository_name(name: &str) -> Result<(), EcrError> {
    if !(2..=256).contains(&name.len()) {
        return Err(EcrError::Validation(format!(
            "repository name '{name}' must be 2..=256 characters"
        )));
    }
    if name.starts_with('/') || name.starts_with('.') {
        return Err(EcrError::Validation(format!(
            "repository name '{name}' must not start with '/' or '.'"
        )));
    }
    if !name
        .bytes()
        .all(|byte| matches!(byte, b'a'..=b'z' | b'0'..=b'9' | b'.' | b'_' | b'-' | b'/'))
    {
        return Err(EcrError::Validation(format!(
            "repository name '{name}' contains invalid characters"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_policy_json_round_trips() {
        let parsed: Value = serde_json::from_str(ECR_LIFECYCLE_POLICY).expect("policy JSON");
        let serialized = serde_json::to_string(&parsed).expect("serialize policy");
        let reparsed: Value = serde_json::from_str(&serialized).expect("reparse policy");

        assert_eq!(parsed, reparsed);
    }

    #[test]
    fn repository_name_validation_rejects_invalid_names() {
        for name in ["a", "/repo", ".repo", "Upper"] {
            assert!(EcrRepository::new(name.to_owned(), "images".to_owned()).is_err());
        }
    }
}
