//! Typed author inputs for IAM: role and instance profile.

use std::collections::HashMap;

use serde::Deserialize;
use tokeira_platform::{
    error::KindError,
    kind::{Kind, PlacementContext},
};

use crate::resources::{
    iam_instance_profile::{IamInstanceProfile as ProfileResource, IamInstanceProfileConfig},
    iam_role::{IamRole as RoleResource, IamRoleConfig},
};

/// Author-visible name of the realized role resource type.
pub const ROLE_TYPE: &str = "IamRole";
/// Author-visible name of the realized instance-profile resource type.
pub const INSTANCE_PROFILE_TYPE: &str = "IamInstanceProfile";

/// Reusable author input for one IAM role.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IamRole {
    /// AWS region.
    pub region: String,
    /// Role name (resource id `iam-role-<name>`).
    pub name: String,
    /// Trust policy document (JSON).
    pub trust_policy: String,
    /// Inline policies by name (JSON documents).
    #[serde(default)]
    pub inline_policies: HashMap<String, String>,
    /// Managed policy ARNs to attach.
    #[serde(default)]
    pub managed_policy_arns: Vec<String>,
}

impl Kind<RoleResource> for IamRole {
    fn realize(&self, placement: &PlacementContext) -> Result<RoleResource, KindError> {
        let rctx = super::resource_context(&self.region, placement);
        Ok(RoleResource::new(
            self.name.clone(),
            IamRoleConfig {
                trust_policy: self.trust_policy.clone(),
                inline_policies: self.inline_policies.clone(),
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
    pub region: String,
    /// Profile name.
    pub profile_name: String,
    /// Name of the role this profile wraps (a declared dependency).
    pub role_name: String,
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
