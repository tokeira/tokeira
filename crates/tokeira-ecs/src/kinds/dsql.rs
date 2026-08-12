//! Typed author inputs for the ECS platform's DSQL adopters and IAM roles.

use serde::Deserialize;
use tokeira_iac::ResourceId;
use tokeira_platform::{
    error::KindError,
    kind::{Kind, PlacementContext},
};

use crate::modules::dsql::{AdoptedDsqlResource, DsqlIamRoleResource};

/// Author-visible name of the realized adopted-endpoint resource type.
pub const ENDPOINT_TYPE: &str = "DsqlEndpoint";
/// Author-visible name of the realized DSQL IAM role resource type.
pub const ROLE_TYPE: &str = "DsqlIamRole";

/// Reusable author input for a record-only adopted DSQL endpoint
/// (preexisting mode): the endpoint is referenced, never managed.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdoptedDsqlEndpoint {
    /// Well-known resource id (`dsql:management-endpoint` or
    /// `dsql:connection-endpoint`).
    pub id: String,
    /// The preexisting endpoint's provider id.
    pub endpoint_id: String,
}

impl Kind<AdoptedDsqlResource> for AdoptedDsqlEndpoint {
    fn realize(&self, placement: &PlacementContext) -> Result<AdoptedDsqlResource, KindError> {
        Ok(AdoptedDsqlResource::endpoint(
            ResourceId(self.id.clone()),
            self.endpoint_id.clone(),
            &placement.module,
        ))
    }
}

/// Role provenance: the shape makes the invalid states unrepresentable —
/// managed roles carry policy facts and need the cluster dependency;
/// preexisting roles carry exactly an ARN.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoleMode {
    /// The platform creates and manages the role; its DSQL policy is scoped
    /// to the declared cluster's ARN at create time.
    Managed {
        /// AWS region.
        region: String,
        /// Role name.
        role_name: String,
        /// Inline policy name.
        policy_name: String,
        /// DSQL action the policy grants (e.g. `dsql:DbConnect`).
        action: String,
    },
    /// A preexisting role is referenced, never managed.
    Preexisting {
        /// The adopted role's ARN.
        role_arn: String,
    },
}

/// Reusable author input for one of the platform's DSQL IAM roles.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DsqlIamRole {
    /// Well-known resource id (`dsql:runtime-role` or `dsql:admin-role`).
    pub id: String,
    /// Role provenance and its mode-specific facts.
    pub mode: RoleMode,
}

impl Kind<DsqlIamRoleResource> for DsqlIamRole {
    fn realize(&self, placement: &PlacementContext) -> Result<DsqlIamRoleResource, KindError> {
        Ok(match &self.mode {
            RoleMode::Managed {
                region,
                role_name,
                policy_name,
                action,
            } => {
                let cluster = placement
                    .dependencies
                    .iter()
                    .find(|id| id.0 == "dsql:cluster")
                    .cloned()
                    .ok_or_else(|| {
                        KindError::new(
                            "a managed DsqlIamRole needs the DSQL cluster declared as a dependency",
                        )
                    })?;
                DsqlIamRoleResource::managed(
                    ResourceId(self.id.clone()),
                    role_name.clone(),
                    policy_name,
                    action,
                    cluster,
                    placement.module.clone(),
                    tokeira_aws::ResourceContext {
                        project: placement.deployment_id.clone(),
                        region: region.clone(),
                        tags: placement.tags.clone().into_iter().collect(),
                    },
                )
            }
            RoleMode::Preexisting { role_arn } => DsqlIamRoleResource::preexisting(
                ResourceId(self.id.clone()),
                role_arn.clone(),
                placement.module.clone(),
            ),
        })
    }
}
