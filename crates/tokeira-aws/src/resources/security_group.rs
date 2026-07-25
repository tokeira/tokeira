use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tokeira_iac::{
    DescribeResult, InternalChange, ProvisionContext, Resource, ResourceId, ResourceState,
    ResourceType, error::IacError,
};

/// Security group rule descriptor for testing and diffing.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SecurityRule {
    pub description: String,
    pub protocol: String,
    pub from_port: u16,
    pub to_port: u16,
    pub source: String,
}

/// Compute the diff between two rule sets.
/// Returns (to_add, to_remove) where:
/// - to_add: rules in `desired` but not in `current`
/// - to_remove: rules in `current` but not in `desired`
pub fn diff_rules(
    current: &[SecurityRule],
    desired: &[SecurityRule],
) -> (Vec<SecurityRule>, Vec<SecurityRule>) {
    let current_set: std::collections::HashSet<&SecurityRule> = current.iter().collect();
    let desired_set: std::collections::HashSet<&SecurityRule> = desired.iter().collect();

    let to_add: Vec<SecurityRule> = desired_set
        .difference(&current_set)
        .map(|r| (*r).clone())
        .collect();
    let to_remove: Vec<SecurityRule> = current_set
        .difference(&desired_set)
        .map(|r| (*r).clone())
        .collect();

    (to_add, to_remove)
}

fn normalize_rules(mut rules: Vec<SecurityRule>) -> Vec<SecurityRule> {
    rules.sort_by(|a, b| {
        (
            a.protocol.as_str(),
            a.from_port,
            a.to_port,
            a.source.as_str(),
            a.description.as_str(),
        )
            .cmp(&(
                b.protocol.as_str(),
                b.from_port,
                b.to_port,
                b.source.as_str(),
                b.description.as_str(),
            ))
    });
    rules
}

fn collect_live_rules(
    sg: &aws_sdk_ec2::types::SecurityGroup,
    sg_id: &str,
) -> Result<Vec<SecurityRule>, IacError> {
    let mut rules = Vec::new();

    for permission in sg.ip_permissions() {
        let protocol = permission.ip_protocol().unwrap_or_default().to_string();
        let from_port = permission.from_port().unwrap_or_default() as u16;
        let to_port = permission.to_port().unwrap_or_default() as u16;

        if !permission.ipv6_ranges().is_empty() {
            return Err(IacError::AwsSdk(format!(
                "ec2:DescribeSecurityGroups: security group '{}' has unmanaged IPv6 ingress rules",
                sg_id
            )));
        }
        if !permission.prefix_list_ids().is_empty() {
            return Err(IacError::AwsSdk(format!(
                "ec2:DescribeSecurityGroups: security group '{}' has unmanaged prefix-list ingress rules",
                sg_id
            )));
        }

        for ip_range in permission.ip_ranges() {
            let source = ip_range.cidr_ip().ok_or_else(|| {
                IacError::AwsSdk(format!(
                    "ec2:DescribeSecurityGroups: security group '{}' contains an ingress rule without a CIDR",
                    sg_id
                ))
            })?;
            rules.push(SecurityRule {
                description: ip_range.description().unwrap_or_default().to_string(),
                protocol: protocol.clone(),
                from_port,
                to_port,
                source: source.to_string(),
            });
        }

        for pair in permission.user_id_group_pairs() {
            let pair_group_id = pair.group_id().ok_or_else(|| {
                IacError::AwsSdk(format!(
                    "ec2:DescribeSecurityGroups: security group '{}' contains an ingress rule without a source group ID",
                    sg_id
                ))
            })?;
            if pair_group_id != sg_id {
                return Err(IacError::AwsSdk(format!(
                    "ec2:DescribeSecurityGroups: security group '{}' has unmanaged ingress from security group '{}'",
                    sg_id, pair_group_id
                )));
            }
            rules.push(SecurityRule {
                description: pair.description().unwrap_or_default().to_string(),
                protocol: protocol.clone(),
                from_port,
                to_port,
                source: "self".into(),
            });
        }
    }

    Ok(normalize_rules(rules))
}

fn collect_live_tags(sg: &aws_sdk_ec2::types::SecurityGroup) -> HashMap<String, String> {
    let mut tags = HashMap::new();
    for tag in sg.tags() {
        if let (Some(key), Some(value)) = (tag.key(), tag.value()) {
            tags.insert(key.to_string(), value.to_string());
        }
    }
    tags
}

/// Build an EC2 IpPermission from a SecurityRule.
/// `sg_id` is used when source="self" to reference the SG's own ID.
fn build_ip_permission(rule: &SecurityRule, sg_id: &str) -> aws_sdk_ec2::types::IpPermission {
    let mut builder = aws_sdk_ec2::types::IpPermission::builder()
        .ip_protocol(&rule.protocol)
        .from_port(rule.from_port as i32)
        .to_port(rule.to_port as i32);

    if rule.source == "self" {
        builder = builder.user_id_group_pairs(
            aws_sdk_ec2::types::UserIdGroupPair::builder()
                .group_id(sg_id)
                .description(&rule.description)
                .build(),
        );
    } else {
        builder = builder.ip_ranges(
            aws_sdk_ec2::types::IpRange::builder()
                .cidr_ip(&rule.source)
                .description(&rule.description)
                .build(),
        );
    }

    builder.build()
}

/// Configuration for a single EC2 security group provider resource.
#[derive(Debug)]
pub struct SecurityGroupConfig {
    pub vpc_dependency: ResourceId,
    pub description: String,
    pub ingress_rules: Vec<SecurityRule>,
    pub module: String,
}

/// Generic provider resource that provisions exactly one EC2 security group.
#[derive(Debug)]
pub struct SecurityGroup {
    pub name: String,
    pub config: SecurityGroupConfig,
    pub project: String,
    pub region: String,
    pub tags: HashMap<String, String>,
}

impl SecurityGroup {
    pub fn new(name: String, config: SecurityGroupConfig, rctx: &crate::ResourceContext) -> Self {
        Self {
            name,
            config,
            project: rctx.project.clone(),
            region: rctx.region.clone(),
            tags: rctx.tags.clone(),
        }
    }
}

#[async_trait::async_trait]
impl Resource for SecurityGroup {
    fn resource_type(&self) -> ResourceType {
        ResourceType::new("SecurityGroup")
    }

    fn resource_id(&self) -> ResourceId {
        ResourceId(format!("sg-{}", self.name))
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        vec![self.config.vpc_dependency.clone()]
    }

    fn module(&self) -> &str {
        &self.config.module
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        let tags = ctx.resource_tags(&self.name);
        let ec2_tags = super::ec2_tags(&tags);
        let rules = &self.config.ingress_rules;

        // Read VPC state for vpc_id
        let vpc_state = ctx.get_resource_state(&self.config.vpc_dependency)?;
        let vpc_id = vpc_state
            .properties
            .get("vpc_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| IacError::StateNotFound("vpc_id not found in VPC state".into()))?
            .to_string();

        // ec2:CreateSecurityGroup
        let sg_tag_spec = aws_sdk_ec2::types::TagSpecification::builder()
            .resource_type(aws_sdk_ec2::types::ResourceType::SecurityGroup)
            .set_tags(Some(ec2_tags.clone()))
            .build();

        let sg_output = ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ec2
            .create_security_group()
            .group_name(&self.name)
            .description(&self.config.description)
            .vpc_id(&vpc_id)
            .tag_specifications(sg_tag_spec)
            .send()
            .await
            .map_err(|e| {
                let msg = format!("{}", e.into_service_error());
                if msg.contains("InvalidGroup.Duplicate") {
                    return IacError::AwsSdk(format!(
                        "ec2:CreateSecurityGroup: group already exists: {msg}"
                    ));
                }
                IacError::AwsSdk(format!("ec2:CreateSecurityGroup: {msg}"))
            })?;

        let sg_id = sg_output
            .group_id()
            .ok_or_else(|| {
                IacError::AwsSdk("ec2:CreateSecurityGroup: no group ID in response".into())
            })?
            .to_string();

        // ec2:AuthorizeSecurityGroupIngress per rule
        for rule in rules {
            let ip_perm = build_ip_permission(rule, &sg_id);
            match ctx
                .extension::<crate::AwsClients>()
                .expect("AwsClients")
                .ec2
                .authorize_security_group_ingress()
                .group_id(&sg_id)
                .ip_permissions(ip_perm)
                .send()
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    let msg = format!("{}", e.into_service_error());
                    if msg.contains("InvalidPermission.Duplicate") {
                        tracing::warn!(rule = %rule.description, "ingress rule already exists, skipping");
                    } else {
                        return Err(IacError::AwsSdk(format!(
                            "ec2:AuthorizeSecurityGroupIngress: {msg}"
                        )));
                    }
                }
            }
        }

        let now = chrono::Utc::now().to_rfc3339();
        Ok(ResourceState {
            resource_type: ResourceType::new("SecurityGroup"),
            physical_id: sg_id.clone(),
            properties: serde_json::json!({
                "security_group_id": sg_id,
                "rule_count": rules.len(),
                "rules": serde_json::to_value(rules).unwrap_or_default(),
                "tags": tags,
            }),
            dependencies: self.dependencies(),
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
        let tags = ctx.resource_tags(&self.name);
        let desired_rules = &self.config.ingress_rules;

        let sg_id = current
            .properties
            .get("security_group_id")
            .and_then(|v| v.as_str())
            .unwrap_or(&current.physical_id)
            .to_string();

        // Deserialize current rules from state (if available).
        let current_rules: Vec<SecurityRule> = current
            .properties
            .get("rules")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();

        // Compute diff between current and desired rule sets.
        let (to_add, to_remove) = diff_rules(&current_rules, desired_rules);

        // Revoke stale rules first.
        for rule in &to_remove {
            let ip_perm = build_ip_permission(rule, &sg_id);
            match ctx
                .extension::<crate::AwsClients>()
                .expect("AwsClients")
                .ec2
                .revoke_security_group_ingress()
                .group_id(&sg_id)
                .ip_permissions(ip_perm)
                .send()
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    let msg = format!("{}", e.into_service_error());
                    if msg.contains("InvalidPermission.NotFound") {
                        tracing::warn!(rule = %rule.description, "ingress rule not found during revoke, skipping");
                    } else {
                        return Err(IacError::AwsSdk(format!(
                            "ec2:RevokeSecurityGroupIngress: {msg}"
                        )));
                    }
                }
            }
        }

        // Authorize new rules.
        for rule in &to_add {
            let ip_perm = build_ip_permission(rule, &sg_id);
            match ctx
                .extension::<crate::AwsClients>()
                .expect("AwsClients")
                .ec2
                .authorize_security_group_ingress()
                .group_id(&sg_id)
                .ip_permissions(ip_perm)
                .send()
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    let msg = format!("{}", e.into_service_error());
                    if msg.contains("InvalidPermission.Duplicate") {
                        // Rule already exists — expected during update
                    } else {
                        return Err(IacError::AwsSdk(format!(
                            "ec2:AuthorizeSecurityGroupIngress: {msg}"
                        )));
                    }
                }
            }
        }

        // Update tags
        let ec2_tags = super::ec2_tags(&tags);
        ctx.extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ec2
            .create_tags()
            .resources(&sg_id)
            .set_tags(Some(ec2_tags))
            .send()
            .await
            .map_err(|e| IacError::AwsSdk(format!("ec2:CreateTags: {}", e.into_service_error())))?;

        Ok(ResourceState {
            resource_type: current.resource_type.clone(),
            physical_id: current.physical_id.clone(),
            properties: serde_json::json!({
                "security_group_id": sg_id,
                "rule_count": desired_rules.len(),
                "rules": serde_json::to_value(desired_rules).unwrap_or_default(),
                "tags": tags,
            }),
            dependencies: self.dependencies(),
            created_at: current.created_at.clone(),
            updated_at: chrono::Utc::now().to_rfc3339(),
            module: self.module().to_owned(),
        })
    }

    async fn delete(
        &self,
        current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<(), IacError> {
        let sg_id = current
            .properties
            .get("security_group_id")
            .and_then(|v| v.as_str())
            .unwrap_or(&current.physical_id)
            .to_string();

        if sg_id.is_empty() {
            return Ok(());
        }

        match ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ec2
            .delete_security_group()
            .group_id(&sg_id)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                let msg = format!("{}", e.into_service_error());
                if msg.contains("InvalidGroup.NotFound") {
                    tracing::warn!(sg = %sg_id, "security group not found, skipping");
                    Ok(())
                } else if msg.contains("DependencyViolation") {
                    Err(IacError::AwsSdk(format!(
                        "ec2:DeleteSecurityGroup: security group '{sg_id}' still has dependent attachments: {msg}"
                    )))
                } else {
                    Err(IacError::AwsSdk(format!("ec2:DeleteSecurityGroup: {msg}")))
                }
            }
        }
    }

    async fn describe(&self, ctx: &ProvisionContext) -> Result<DescribeResult, IacError> {
        let desc_output = ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ec2
            .describe_security_groups()
            .filters(
                aws_sdk_ec2::types::Filter::builder()
                    .name("tag:Name")
                    .values(&self.name)
                    .build(),
            )
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!(
                    "ec2:DescribeSecurityGroups: {}",
                    e.into_service_error()
                ))
            })?;

        let groups = desc_output.security_groups();
        if groups.is_empty() {
            return Ok(DescribeResult::Absent);
        }

        let sg = &groups[0];
        let sg_id = sg.group_id().unwrap_or_default().to_string();
        let rules = collect_live_rules(sg, &sg_id)?;
        let tags = collect_live_tags(sg);
        let now = chrono::Utc::now().to_rfc3339();

        Ok(DescribeResult::Present(ResourceState {
            resource_type: ResourceType::new("SecurityGroup"),
            physical_id: sg_id.clone(),
            properties: serde_json::json!({
                "security_group_id": sg_id,
                "rule_count": rules.len(),
                "rules": serde_json::to_value(&rules).unwrap_or_default(),
                "tags": tags,
            }),
            dependencies: self.dependencies(),
            created_at: now.clone(),
            updated_at: now,
            module: self.module().to_owned(),
        }))
    }

    fn diff(&self, current: &ResourceState, _ctx: &ProvisionContext) -> InternalChange {
        let current_rules: Vec<SecurityRule> = current
            .properties
            .get("rules")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();

        let desired_rules = normalize_rules(self.config.ingress_rules.clone());

        if current_rules != desired_rules {
            return InternalChange::Update {
                resource_id: self.resource_id(),
                resource_type: self.resource_type(),
                details: vec![tokeira_iac::FieldDiff::observation("ingress rules changed")],
            };
        }

        // Compare tags
        let current_tags: HashMap<String, String> = current
            .properties
            .get("tags")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();

        let mut desired_tags = self.tags.clone();
        desired_tags.insert("Name".into(), self.name.clone());
        desired_tags.insert("Project".into(), self.project.clone());
        desired_tags.insert("ManagedBy".into(), "tokeira-cli".into());

        if current_tags != desired_tags {
            return InternalChange::Update {
                resource_id: self.resource_id(),
                resource_type: self.resource_type(),
                details: vec![tokeira_iac::FieldDiff::observation("tags changed")],
            };
        }

        InternalChange::NoChange {
            resource_id: self.resource_id(),
        }
    }
}
