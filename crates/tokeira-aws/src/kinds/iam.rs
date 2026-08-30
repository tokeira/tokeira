//! Typed author inputs for IAM: role and instance profile.

use std::collections::HashMap;

use serde::Deserialize;
use tokeira_platform::{
    error::KindError,
    kind::{Kind, PlacementContext},
};

use crate::resources::{
    iam_instance_profile::{IamInstanceProfile as ProfileResource, IamInstanceProfileConfig},
    iam_role::{
        DependentInlinePolicy as ResourceDependentInlinePolicy, IamRole as RoleResource,
        IamRoleConfig,
    },
};

/// One inline policy resolved from an explicitly declared dependency.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DependentInlinePolicy {
    /// Inline policy name.
    pub(crate) name: String,
    /// Exact IAM actions granted.
    pub(crate) actions: Vec<String>,
    /// Logical resource id supplying the provider-assigned ARN.
    pub(crate) dependency: String,
    /// State property containing that ARN.
    pub(crate) property: String,
    /// Suffixes appended to the ARN; use `[""]` for the ARN itself.
    pub(crate) resource_suffixes: Vec<String>,
}

/// Author-visible name of the realized role resource type.
pub(crate) const ROLE_TYPE: &str = "IamRole";
/// Author-visible name of the realized instance-profile resource type.
pub(crate) const INSTANCE_PROFILE_TYPE: &str = "IamInstanceProfile";

/// Reusable author input for one IAM role.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IamRole {
    /// AWS region.
    pub(crate) region: String,
    /// Role name (resource id `iam-role-<name>`).
    pub(crate) name: String,
    /// Trust policy document (JSON).
    pub(crate) trust_policy: String,
    /// Inline policies by name (JSON documents).
    #[serde(default)]
    pub(crate) inline_policies: HashMap<String, String>,
    /// Policies whose resource identity is supplied after a dependency applies.
    #[serde(default)]
    pub(crate) dependent_inline_policies: Vec<DependentInlinePolicy>,
    /// Managed policy ARNs to attach.
    #[serde(default)]
    pub(crate) managed_policy_arns: Vec<String>,
}

impl Kind<RoleResource> for IamRole {
    fn realize(&self, placement: &PlacementContext) -> Result<RoleResource, KindError> {
        let dependent_inline_policies = self
            .dependent_inline_policies
            .iter()
            .map(|policy| {
                let dependency = tokeira_iac::ResourceId(policy.dependency.clone());
                if !placement.dependencies.contains(&dependency) {
                    return Err(KindError::new(format!(
                        "IamRole policy `{}` needs `{}` declared as a dependency",
                        policy.name, policy.dependency
                    )));
                }
                Ok(ResourceDependentInlinePolicy {
                    name: policy.name.clone(),
                    actions: policy.actions.clone(),
                    dependency,
                    property: policy.property.clone(),
                    resource_suffixes: policy.resource_suffixes.clone(),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let rctx = super::resource_context(&self.region, placement);
        Ok(RoleResource::new(
            self.name.clone(),
            IamRoleConfig {
                trust_policy: self.trust_policy.clone(),
                inline_policies: self.inline_policies.clone(),
                dependent_inline_policies,
                managed_policy_arns: self.managed_policy_arns.clone(),
                module: placement.module.clone(),
            },
            &rctx,
        ))
    }
}

/// Reusable author input for an instance profile over one declared role.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IamInstanceProfile {
    /// AWS region.
    pub(crate) region: String,
    /// Profile name.
    pub(crate) profile_name: String,
    /// Name of the role this profile wraps (a declared dependency).
    pub(crate) role_name: String,
}

impl Kind<ProfileResource> for IamInstanceProfile {
    fn realize(&self, placement: &PlacementContext) -> Result<ProfileResource, KindError> {
        let role_id = format!("iam-role-{}", self.role_name);
        let role = super::required_dependency(placement, "IamRole", |id| id == role_id)?;
        let rctx = super::resource_context(&self.region, placement);
        Ok(ProfileResource::new(
            self.profile_name.clone(),
            IamInstanceProfileConfig {
                role_name: self.role_name.clone(),
                role_dependency: role,
                module: placement.module.clone(),
            },
            &rctx,
        ))
    }
}
