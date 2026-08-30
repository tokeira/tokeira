use std::collections::{HashMap, HashSet};

use serde::Serialize;
use tokeira_iac::{
    ChangeKind, ChangeSemantics, Citation, DescribeResult, InternalChange, ProvisionContext,
    Resource, ResourceId, ResourceState, ResourceType, SemanticsContext, error::IacError,
};

// ── Config and resource structs ──────────────────────────────────────────────

/// One inline IAM policy whose resource ARN comes from a declared dependency.
///
/// Provider-assigned identities such as a DSQL cluster ARN are unavailable
/// when a definition is realized. Keeping the source resource and property in
/// the desired model lets the engine order the role after that resource and
/// resolve the exact ARN at lifecycle time instead of widening to `*`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DependentInlinePolicy {
    /// Inline policy name on the role.
    pub name: String,
    /// Exact IAM actions granted by the statement.
    pub actions: Vec<String>,
    /// Declared resource supplying the provider-assigned identity.
    pub dependency: ResourceId,
    /// Property read from the dependency's applied state.
    pub property: String,
    /// Suffixes appended to the resolved identity; an empty suffix means the
    /// identity itself. Multiple suffixes support pairs such as bucket and
    /// bucket objects without reconstructing the base ARN.
    pub resource_suffixes: Vec<String>,
}

/// Configuration for a single IAM role provider resource.
#[derive(Debug)]
pub struct IamRoleConfig {
    pub trust_policy: String,
    pub inline_policies: HashMap<String, String>,
    pub dependent_inline_policies: Vec<DependentInlinePolicy>,
    pub managed_policy_arns: Vec<String>,
    pub module: String,
}

/// Generic provider resource that provisions exactly one IAM role.
#[derive(Debug)]
pub struct IamRole {
    pub role_name: String,
    pub config: IamRoleConfig,
    pub project: String,
    pub region: String,
    pub tags: HashMap<String, String>,
}

impl IamRole {
    pub fn new(role_name: String, config: IamRoleConfig, rctx: &crate::ResourceContext) -> Self {
        Self {
            role_name,
            config,
            project: rctx.project.clone(),
            region: rctx.region.clone(),
            tags: rctx.tags.clone(),
        }
    }

    fn policy_dependencies(&self) -> Vec<ResourceId> {
        let mut dependencies = self
            .config
            .dependent_inline_policies
            .iter()
            .map(|policy| policy.dependency.clone())
            .collect::<Vec<_>>();
        dependencies.sort();
        dependencies.dedup();
        dependencies
    }

    fn resolved_inline_policies(
        &self,
        ctx: &ProvisionContext,
    ) -> Result<HashMap<String, String>, IacError> {
        let mut policies = self.config.inline_policies.clone();
        for policy in &self.config.dependent_inline_policies {
            let state = ctx.get_resource_state(&policy.dependency)?;
            let resource = state
                .properties
                .get(&policy.property)
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    IacError::DependencyResolution(format!(
                        "IAM role `{}` policy `{}` needs non-empty output `{}.{}`",
                        self.role_name, policy.name, policy.dependency.0, policy.property
                    ))
                })?;
            let resources = policy
                .resource_suffixes
                .iter()
                .map(|suffix| format!("{resource}{suffix}"))
                .collect::<Vec<_>>();
            policies.insert(
                policy.name.clone(),
                serde_json::json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Action": policy.actions,
                        "Resource": resources,
                    }]
                })
                .to_string(),
            );
        }
        Ok(policies)
    }
}

fn normalize_string_map(mut map: HashMap<String, String>) -> HashMap<String, String> {
    map.retain(|key, _| !key.is_empty());
    map
}

fn normalize_sorted_strings(mut values: Vec<String>) -> Vec<String> {
    values.sort();
    values.dedup();
    values
}

#[async_trait::async_trait]
impl Resource for IamRole {
    fn validate_input(&self) -> Result<(), String> {
        let mut policy_names = HashSet::new();
        for policy in &self.config.dependent_inline_policies {
            if policy.name.is_empty()
                || policy.actions.is_empty()
                || policy.actions.iter().any(String::is_empty)
                || policy.property.is_empty()
                || policy.resource_suffixes.is_empty()
            {
                return Err(format!(
                    "IAM role `{}` has an incomplete dependency-backed policy",
                    self.role_name
                ));
            }
            if self.config.inline_policies.contains_key(&policy.name)
                || !policy_names.insert(&policy.name)
            {
                return Err(format!(
                    "IAM role `{}` declares inline policy `{}` twice",
                    self.role_name, policy.name
                ));
            }
        }
        Ok(())
    }

    fn desired_manifest(&self) -> serde_json::Value {
        serde_json::json!({
            "role_name": self.role_name,
            "region": self.region,
            "trust_policy": self.config.trust_policy,
            "inline_policies": self.config.inline_policies,
            "dependent_inline_policies": self.config.dependent_inline_policies,
            "managed_policy_arns": self.config.managed_policy_arns,
            "module": self.config.module,
        })
    }

    fn change_semantics(&self, ctx: &SemanticsContext<'_>) -> ChangeSemantics {
        const CREATE: Citation = Citation::code(concat!(
            module_path!(),
            "::create — iam:CreateRole (EntityAlreadyExists adopted), then \
             iam:TagRole, iam:PutRolePolicy per inline policy, \
             iam:AttachRolePolicy per managed ARN"
        ));
        const UPDATE: Citation = Citation::code(concat!(
            module_path!(),
            "::update — in-place reconcile: iam:UntagRole/TagRole, stale inline \
             policies deleted, iam:PutRolePolicy upserts, managed attachments \
             detached/attached to the desired set; principals using the role \
             are never interrupted"
        ));
        const DELETE: Citation = Citation::code(concat!(
            module_path!(),
            "::delete — detach managed policies, delete inline policies, then \
             iam:DeleteRole (NoSuchEntity tolerated at each step)"
        ));
        let mut semantics = super::control_plane_semantics(ctx.kind, CREATE, UPDATE, DELETE);
        if matches!(ctx.kind, ChangeKind::Create) {
            semantics.provider_assigned = vec!["role_arn".into()];
        }
        semantics
    }

    fn resource_type(&self) -> ResourceType {
        ResourceType::new("IamRole")
    }

    fn resource_id(&self) -> ResourceId {
        ResourceId(format!("iam-role-{}", self.role_name))
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        self.policy_dependencies()
    }

    fn module(&self) -> &str {
        &self.config.module
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        let name = &self.role_name;
        let tags = ctx.resource_tags(name);
        let iam_tags = super::iam_tags(&tags);
        let inline_policies = self.resolved_inline_policies(ctx)?;

        // iam:CreateRole with trust policy and tags
        let role_arn = match ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .iam
            .create_role()
            .role_name(name)
            .assume_role_policy_document(&self.config.trust_policy)
            .set_tags(Some(iam_tags))
            .send()
            .await
        {
            Ok(output) => output
                .role()
                .map(|r| r.arn().to_string())
                .unwrap_or_default(),
            Err(e) => {
                let svc_err = e.into_service_error();
                if svc_err.is_entity_already_exists_exception() {
                    tracing::warn!(role = %name, "role already exists, adopting");
                    let get_output = ctx
                        .extension::<crate::AwsClients>()
                        .expect("AwsClients")
                        .iam
                        .get_role()
                        .role_name(name)
                        .send()
                        .await
                        .map_err(|e| {
                            IacError::AwsSdk(format!("iam:GetRole: {}", e.into_service_error()))
                        })?;
                    get_output
                        .role()
                        .map(|r| r.arn().to_string())
                        .unwrap_or_default()
                } else {
                    return Err(IacError::AwsSdk(format!("iam:CreateRole: {svc_err}")));
                }
            }
        };

        // iam:TagRole — explicitly apply desired tags on create/adopt.
        ctx.extension::<crate::AwsClients>()
            .expect("AwsClients")
            .iam
            .tag_role()
            .role_name(name)
            .set_tags(Some(super::iam_tags(&tags)))
            .send()
            .await
            .map_err(|e| IacError::AwsSdk(format!("iam:TagRole: {}", e.into_service_error())))?;

        // iam:PutRolePolicy per inline policy
        for (policy_name, policy_document) in &inline_policies {
            ctx.extension::<crate::AwsClients>()
                .expect("AwsClients")
                .iam
                .put_role_policy()
                .role_name(name)
                .policy_name(policy_name)
                .policy_document(policy_document)
                .send()
                .await
                .map_err(|e| {
                    IacError::AwsSdk(format!("iam:PutRolePolicy: {}", e.into_service_error()))
                })?;
        }

        // iam:AttachRolePolicy per managed ARN
        for policy_arn in &self.config.managed_policy_arns {
            ctx.extension::<crate::AwsClients>()
                .expect("AwsClients")
                .iam
                .attach_role_policy()
                .role_name(name)
                .policy_arn(policy_arn)
                .send()
                .await
                .map_err(|e| {
                    IacError::AwsSdk(format!("iam:AttachRolePolicy: {}", e.into_service_error()))
                })?;
        }

        let now = chrono::Utc::now().to_rfc3339();
        Ok(ResourceState {
            resource_type: ResourceType::new("IamRole"),
            physical_id: role_arn.clone(),
            properties: serde_json::json!({
                "role_name": self.role_name,
                "role_arn": role_arn,
                "inline_policies": inline_policies,
                "managed_policy_arns": normalize_sorted_strings(self.config.managed_policy_arns.clone()),
                "tags": tags,
            }),
            dependencies: self.policy_dependencies(),
            created_at: now.clone(),
            updated_at: now,
            module: self.module().to_owned(),
        })
    }

    async fn delete(
        &self,
        _current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<(), IacError> {
        let name = &self.role_name;

        // iam:ListAttachedRolePolicies → iam:DetachRolePolicy
        match ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .iam
            .list_attached_role_policies()
            .role_name(name)
            .send()
            .await
        {
            Ok(output) => {
                for policy in output.attached_policies() {
                    if let Some(arn) = policy.policy_arn() {
                        ctx.extension::<crate::AwsClients>()
                            .expect("AwsClients")
                            .iam
                            .detach_role_policy()
                            .role_name(name)
                            .policy_arn(arn)
                            .send()
                            .await
                            .map_err(|e| {
                                IacError::AwsSdk(format!(
                                    "iam:DetachRolePolicy: {}",
                                    e.into_service_error()
                                ))
                            })?;
                    }
                }
            }
            Err(e) => {
                let svc_err = e.into_service_error();
                if svc_err.is_no_such_entity_exception() {
                    tracing::warn!(role = %name, "role not found, skipping deletion");
                    return Ok(());
                }
                return Err(IacError::AwsSdk(format!(
                    "iam:ListAttachedRolePolicies: {svc_err}"
                )));
            }
        }

        // iam:ListRolePolicies → iam:DeleteRolePolicy
        match ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .iam
            .list_role_policies()
            .role_name(name)
            .send()
            .await
        {
            Ok(output) => {
                for policy_name in output.policy_names() {
                    ctx.extension::<crate::AwsClients>()
                        .expect("AwsClients")
                        .iam
                        .delete_role_policy()
                        .role_name(name)
                        .policy_name(policy_name)
                        .send()
                        .await
                        .map_err(|e| {
                            IacError::AwsSdk(format!(
                                "iam:DeleteRolePolicy: {}",
                                e.into_service_error()
                            ))
                        })?;
                }
            }
            Err(e) => {
                let svc_err = e.into_service_error();
                if !svc_err.is_no_such_entity_exception() {
                    return Err(IacError::AwsSdk(format!("iam:ListRolePolicies: {svc_err}")));
                }
            }
        }

        // iam:DeleteRole
        match ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .iam
            .delete_role()
            .role_name(name)
            .send()
            .await
        {
            Ok(_) => {}
            Err(e) => {
                let svc_err = e.into_service_error();
                if svc_err.is_no_such_entity_exception() {
                    tracing::warn!(role = %name, "role not found during delete, skipping");
                } else {
                    return Err(IacError::AwsSdk(format!("iam:DeleteRole: {svc_err}")));
                }
            }
        }

        Ok(())
    }

    async fn describe(&self, ctx: &ProvisionContext) -> Result<DescribeResult, IacError> {
        let name = &self.role_name;

        match ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .iam
            .get_role()
            .role_name(name)
            .send()
            .await
        {
            Ok(output) => {
                let role_arn = output
                    .role()
                    .map(|r| r.arn().to_string())
                    .unwrap_or_default();
                let tags = ctx
                    .extension::<crate::AwsClients>()
                    .expect("AwsClients")
                    .iam
                    .list_role_tags()
                    .role_name(name)
                    .send()
                    .await
                    .map_err(|e| {
                        IacError::AwsSdk(format!("iam:ListRoleTags: {}", e.into_service_error()))
                    })?;
                let inline_policies = ctx
                    .extension::<crate::AwsClients>()
                    .expect("AwsClients")
                    .iam
                    .list_role_policies()
                    .role_name(name)
                    .send()
                    .await
                    .map_err(|e| {
                        IacError::AwsSdk(format!(
                            "iam:ListRolePolicies: {}",
                            e.into_service_error()
                        ))
                    })?;
                let managed_policies = ctx
                    .extension::<crate::AwsClients>()
                    .expect("AwsClients")
                    .iam
                    .list_attached_role_policies()
                    .role_name(name)
                    .send()
                    .await
                    .map_err(|e| {
                        IacError::AwsSdk(format!(
                            "iam:ListAttachedRolePolicies: {}",
                            e.into_service_error()
                        ))
                    })?;

                let mut inline_policy_documents = HashMap::new();
                for policy_name in inline_policies.policy_names() {
                    let response = ctx
                        .extension::<crate::AwsClients>()
                        .expect("AwsClients")
                        .iam
                        .get_role_policy()
                        .role_name(name)
                        .policy_name(policy_name)
                        .send()
                        .await
                        .map_err(|e| {
                            IacError::AwsSdk(format!(
                                "iam:GetRolePolicy: {}",
                                e.into_service_error()
                            ))
                        })?;
                    let raw_policy_document = response.policy_document().to_string();
                    let parsed_policy_document =
                        crate::iam_policy::canonical_policy_string(&raw_policy_document);
                    inline_policy_documents.insert(policy_name.to_string(), parsed_policy_document);
                }

                let live_tags = normalize_string_map(
                    tags.tags()
                        .iter()
                        .map(|tag| (tag.key().to_string(), tag.value().to_string()))
                        .collect(),
                );
                let managed_policy_arns = normalize_sorted_strings(
                    managed_policies
                        .attached_policies()
                        .iter()
                        .filter_map(|policy| policy.policy_arn().map(ToString::to_string))
                        .collect(),
                );
                let now = chrono::Utc::now().to_rfc3339();
                Ok(DescribeResult::Present(ResourceState {
                    resource_type: ResourceType::new("IamRole"),
                    physical_id: role_arn.clone(),
                    properties: serde_json::json!({
                        "role_name": self.role_name,
                        "role_arn": role_arn,
                        "inline_policies": inline_policy_documents,
                        "managed_policy_arns": managed_policy_arns,
                        "tags": live_tags,
                    }),
                    dependencies: self.policy_dependencies(),
                    created_at: now.clone(),
                    updated_at: now,
                    module: self.module().to_owned(),
                }))
            }
            Err(e) => {
                let svc_err = e.into_service_error();
                if svc_err.is_no_such_entity_exception() {
                    Ok(DescribeResult::Absent)
                } else {
                    Err(IacError::AwsSdk(format!("iam:GetRole: {svc_err}")))
                }
            }
        }
    }

    async fn update(
        &self,
        current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<ResourceState, IacError> {
        let name = &self.role_name;
        let tags = ctx.resource_tags(name);
        let iam_tags = super::iam_tags(&tags);
        let inline_policies = self.resolved_inline_policies(ctx)?;

        let current_tags: HashMap<String, String> = current
            .properties
            .get("tags")
            .and_then(|value| serde_json::from_value(value.clone()).ok())
            .unwrap_or_default();
        let current_inline_policies: HashMap<String, String> = current
            .properties
            .get("inline_policies")
            .and_then(|value| serde_json::from_value(value.clone()).ok())
            .unwrap_or_default();
        let current_managed_policy_arns: Vec<String> = current
            .properties
            .get("managed_policy_arns")
            .and_then(|value| serde_json::from_value(value.clone()).ok())
            .unwrap_or_default();

        let stale_tag_keys: Vec<String> = current_tags
            .keys()
            .filter(|key| !tags.contains_key(*key))
            .cloned()
            .collect();
        if !stale_tag_keys.is_empty() {
            ctx.extension::<crate::AwsClients>()
                .expect("AwsClients")
                .iam
                .untag_role()
                .role_name(name)
                .set_tag_keys(Some(stale_tag_keys))
                .send()
                .await
                .map_err(|e| {
                    IacError::AwsSdk(format!("iam:UntagRole: {}", e.into_service_error()))
                })?;
        }

        // iam:TagRole — update tags
        ctx.extension::<crate::AwsClients>()
            .expect("AwsClients")
            .iam
            .tag_role()
            .role_name(name)
            .set_tags(Some(iam_tags))
            .send()
            .await
            .map_err(|e| IacError::AwsSdk(format!("iam:TagRole: {}", e.into_service_error())))?;

        let stale_inline_policy_names: Vec<String> = current_inline_policies
            .keys()
            .filter(|policy_name| !inline_policies.contains_key(*policy_name))
            .cloned()
            .collect();
        for policy_name in stale_inline_policy_names {
            ctx.extension::<crate::AwsClients>()
                .expect("AwsClients")
                .iam
                .delete_role_policy()
                .role_name(name)
                .policy_name(policy_name)
                .send()
                .await
                .map_err(|e| {
                    IacError::AwsSdk(format!("iam:DeleteRolePolicy: {}", e.into_service_error()))
                })?;
        }

        // iam:PutRolePolicy per inline policy — ensures policies are up to date
        for (policy_name, policy_document) in &inline_policies {
            ctx.extension::<crate::AwsClients>()
                .expect("AwsClients")
                .iam
                .put_role_policy()
                .role_name(name)
                .policy_name(policy_name)
                .policy_document(policy_document)
                .send()
                .await
                .map_err(|e| {
                    IacError::AwsSdk(format!("iam:PutRolePolicy: {}", e.into_service_error()))
                })?;
        }

        let desired_managed_policy_arns =
            normalize_sorted_strings(self.config.managed_policy_arns.clone());
        for policy_arn in current_managed_policy_arns
            .iter()
            .filter(|policy_arn| !desired_managed_policy_arns.contains(policy_arn))
        {
            ctx.extension::<crate::AwsClients>()
                .expect("AwsClients")
                .iam
                .detach_role_policy()
                .role_name(name)
                .policy_arn(policy_arn)
                .send()
                .await
                .map_err(|e| {
                    IacError::AwsSdk(format!("iam:DetachRolePolicy: {}", e.into_service_error()))
                })?;
        }
        for policy_arn in desired_managed_policy_arns
            .iter()
            .filter(|policy_arn| !current_managed_policy_arns.contains(policy_arn))
        {
            ctx.extension::<crate::AwsClients>()
                .expect("AwsClients")
                .iam
                .attach_role_policy()
                .role_name(name)
                .policy_arn(policy_arn)
                .send()
                .await
                .map_err(|e| {
                    IacError::AwsSdk(format!("iam:AttachRolePolicy: {}", e.into_service_error()))
                })?;
        }

        let role_arn = current
            .properties
            .get("role_arn")
            .and_then(|v| v.as_str())
            .unwrap_or(&current.physical_id)
            .to_string();

        Ok(ResourceState {
            resource_type: current.resource_type.clone(),
            physical_id: current.physical_id.clone(),
            properties: serde_json::json!({
                "role_name": self.role_name,
                "role_arn": role_arn,
                "inline_policies": inline_policies,
                "managed_policy_arns": desired_managed_policy_arns,
                "tags": tags,
            }),
            dependencies: self.policy_dependencies(),
            created_at: current.created_at.clone(),
            updated_at: chrono::Utc::now().to_rfc3339(),
            module: self.module().to_owned(),
        })
    }

    fn diff(&self, current: &ResourceState, _ctx: &ProvisionContext) -> InternalChange {
        let current_tags: HashMap<String, String> = current
            .properties
            .get("tags")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();
        let current_inline_policies: HashMap<String, String> = current
            .properties
            .get("inline_policies")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();
        let current_managed_policy_arns: Vec<String> = current
            .properties
            .get("managed_policy_arns")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();

        // Build the desired tag set the same way create()/update() would.
        let mut desired_tags = self.tags.clone();
        desired_tags.insert("Name".into(), self.role_name.clone());
        desired_tags.insert("Project".into(), self.project.clone());
        desired_tags.insert("ManagedBy".into(), "tokeira-cli".into());
        let desired_inline_policies = match self.resolved_inline_policies(_ctx) {
            Ok(policies) => policies,
            Err(error) => {
                return InternalChange::Update {
                    resource_id: self.resource_id(),
                    resource_type: self.resource_type(),
                    details: vec![tokeira_iac::FieldDiff::observation(format!(
                        "dependency-backed inline policy is unresolved: {error}"
                    ))],
                };
            }
        };
        let desired_managed_policy_arns =
            normalize_sorted_strings(self.config.managed_policy_arns.clone());

        if current_tags != desired_tags {
            InternalChange::Update {
                resource_id: self.resource_id(),
                resource_type: self.resource_type(),
                details: vec![tokeira_iac::FieldDiff::observation("tags changed")],
            }
        } else if !crate::iam_policy::inline_policies_equal(
            &current_inline_policies,
            &desired_inline_policies,
        ) {
            let summary = crate::iam_policy::inline_policies_diff_summary(
                &current_inline_policies,
                &desired_inline_policies,
            );
            InternalChange::Update {
                resource_id: self.resource_id(),
                resource_type: self.resource_type(),
                details: vec![tokeira_iac::FieldDiff::observation(format!(
                    "inline policies changed ({summary})"
                ))],
            }
        } else if normalize_sorted_strings(current_managed_policy_arns)
            != desired_managed_policy_arns
        {
            InternalChange::Update {
                resource_id: self.resource_id(),
                resource_type: self.resource_type(),
                details: vec![tokeira_iac::FieldDiff::observation(
                    "managed policies changed",
                )],
            }
        } else {
            InternalChange::NoChange {
                resource_id: self.resource_id(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn role(inline_policies: &[(&str, &str)], managed_policy_arns: &[&str]) -> IamRole {
        IamRole {
            role_name: "tok-role".into(),
            config: IamRoleConfig {
                trust_policy: "{}".into(),
                inline_policies: inline_policies
                    .iter()
                    .map(|(name, document)| (name.to_string(), document.to_string()))
                    .collect(),
                dependent_inline_policies: Vec::new(),
                managed_policy_arns: managed_policy_arns
                    .iter()
                    .map(|arn| arn.to_string())
                    .collect(),
                module: "iam".into(),
            },
            project: "tok".into(),
            region: "us-east-1".into(),
            tags: HashMap::new(),
        }
    }

    // Mirrors the tag set diff() derives from role_name/project (create() and
    // update() derive the same set via ctx.resource_tags).
    fn matching_tags() -> serde_json::Value {
        serde_json::json!({
            "Name": "tok-role",
            "Project": "tok",
            "ManagedBy": "tokeira-cli",
        })
    }

    fn role_state(
        tags: serde_json::Value,
        inline_policies: serde_json::Value,
        managed_policy_arns: serde_json::Value,
    ) -> ResourceState {
        let now = chrono::Utc::now().to_rfc3339();
        ResourceState {
            resource_type: ResourceType::new("IamRole"),
            physical_id: "arn:role".into(),
            properties: serde_json::json!({
                "role_name": "tok-role",
                "role_arn": "arn:role",
                "tags": tags,
                "inline_policies": inline_policies,
                "managed_policy_arns": managed_policy_arns,
            }),
            dependencies: Vec::new(),
            created_at: now.clone(),
            updated_at: now,
            module: "iam".into(),
        }
    }

    const POLICY: &str = r#"{"Version":"2012-10-17","Statement":[]}"#;

    #[test]
    fn diff_noops_when_tags_policies_and_attachments_match() {
        let resource = role(&[("inline", POLICY)], &["arn:policy:a"]);
        let current = role_state(
            matching_tags(),
            serde_json::json!({ "inline": POLICY }),
            serde_json::json!(["arn:policy:a"]),
        );

        let change = resource.diff(&current, &ProvisionContext::new("test", HashMap::new()));

        assert!(matches!(change, InternalChange::NoChange { .. }));
    }

    #[test]
    fn diff_updates_when_tags_differ() {
        let resource = role(&[], &[]);
        let current = role_state(
            serde_json::json!({ "Name": "tok-role" }),
            serde_json::json!({}),
            serde_json::json!([]),
        );

        let change = resource.diff(&current, &ProvisionContext::new("test", HashMap::new()));

        assert!(matches!(change, InternalChange::Update { .. }));
    }

    #[test]
    fn diff_updates_when_inline_policy_document_changes() {
        let resource = role(
            &[(
                "inline",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow"}]}"#,
            )],
            &[],
        );
        let current = role_state(
            matching_tags(),
            serde_json::json!({ "inline": POLICY }),
            serde_json::json!([]),
        );

        let change = resource.diff(&current, &ProvisionContext::new("test", HashMap::new()));

        assert!(matches!(change, InternalChange::Update { .. }));
    }

    #[test]
    fn diff_noops_when_inline_policies_differ_only_in_formatting() {
        let resource = role(&[("inline", POLICY)], &[]);
        let reformatted = r#"{ "Version" : "2012-10-17" , "Statement" : [ ] }"#;
        let current = role_state(
            matching_tags(),
            serde_json::json!({ "inline": reformatted }),
            serde_json::json!([]),
        );

        let change = resource.diff(&current, &ProvisionContext::new("test", HashMap::new()));

        assert!(matches!(change, InternalChange::NoChange { .. }));
    }

    #[test]
    fn diff_updates_when_managed_policy_set_changes() {
        let resource = role(&[], &["arn:policy:a", "arn:policy:b"]);
        let current = role_state(
            matching_tags(),
            serde_json::json!({}),
            serde_json::json!(["arn:policy:a"]),
        );

        let change = resource.diff(&current, &ProvisionContext::new("test", HashMap::new()));

        assert!(matches!(change, InternalChange::Update { .. }));
    }

    #[test]
    fn diff_noops_when_managed_policies_merely_reordered() {
        let resource = role(&[], &["arn:policy:a", "arn:policy:b"]);
        let current = role_state(
            matching_tags(),
            serde_json::json!({}),
            serde_json::json!(["arn:policy:b", "arn:policy:a", "arn:policy:a"]),
        );

        let change = resource.diff(&current, &ProvisionContext::new("test", HashMap::new()));

        assert!(matches!(change, InternalChange::NoChange { .. }));
    }

    #[test]
    fn dependency_backed_policy_refuses_a_missing_provider_output() {
        let dependency = ResourceId("dsql:cluster".into());
        let mut resource = role(&[], &[]);
        resource.config.dependent_inline_policies = vec![DependentInlinePolicy {
            name: "dsql-connect".into(),
            actions: vec!["dsql:DbConnect".into()],
            dependency: dependency.clone(),
            property: "cluster_arn".into(),
            resource_suffixes: vec![String::new()],
        }];
        let mut ctx = ProvisionContext::new("test", HashMap::new());
        ctx.state.resources.insert(
            dependency,
            role_state(
                serde_json::json!({}),
                serde_json::json!({}),
                serde_json::json!([]),
            ),
        );

        let error = resource
            .resolved_inline_policies(&ctx)
            .expect_err("missing provider identity must fail closed");

        assert!(error.to_string().contains("dsql:cluster.cluster_arn"));
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        // Exact provider outputs remain exact IAM resources; resolution must
        // never replace an unknown-at-realization identity with a wildcard.
        #[test]
        fn dependency_backed_policy_preserves_exact_provider_arns(
            account in "[0-9]{12}",
            cluster in "[a-z0-9-]{1,24}",
        ) {
            let arn = format!("arn:aws:dsql:eu-west-2:{account}:cluster/{cluster}");
            let dependency = ResourceId("dsql:cluster".into());
            let mut resource = role(&[], &[]);
            resource.config.dependent_inline_policies = vec![DependentInlinePolicy {
                name: "dsql-connect".into(),
                actions: vec!["dsql:DbConnect".into()],
                dependency: dependency.clone(),
                property: "cluster_arn".into(),
                resource_suffixes: vec![String::new()],
            }];
            let mut ctx = ProvisionContext::new("test", HashMap::new());
            let mut state = role_state(
                serde_json::json!({}),
                serde_json::json!({}),
                serde_json::json!([]),
            );
            state.properties = serde_json::json!({ "cluster_arn": arn });
            ctx.state.resources.insert(dependency, state);

            let policies = resource
                .resolved_inline_policies(&ctx)
                .expect("declared output resolves");
            let document: serde_json::Value = serde_json::from_str(&policies["dsql-connect"])
                .expect("generated policy is JSON");

            prop_assert_eq!(&document["Statement"][0]["Resource"], &serde_json::json!([arn]));
            prop_assert_eq!(&document["Statement"][0]["Action"], &serde_json::json!(["dsql:DbConnect"]));
            prop_assert!(!document.to_string().contains("\"*\""));
        }
    }
}
