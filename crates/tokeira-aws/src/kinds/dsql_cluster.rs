//! Typed author input for an Aurora DSQL cluster resource.

use serde::Deserialize;
use tokeira_platform::{
    error::KindError,
    kind::{Kind, PlacementContext},
};

use crate::resources::dsql_cluster::{DsqlCluster as Resource, DsqlClusterMode};

/// Reusable author input for [`Resource`]. Realizes the resource directly:
/// authored fields carry over flat, placement supplies project scope and
/// tags.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DsqlCluster {
    /// Stable AWS cluster identity.
    pub identity: String,
    /// AWS region.
    pub region: String,
    /// Managed or adopted lifecycle.
    pub mode: DsqlClusterMode,
    /// Required endpoint for an adopted cluster.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Required ARN for an adopted cluster.
    #[serde(default)]
    pub arn: Option<String>,
    /// Stable resource-id override for clusters that carry a well-known
    /// identity (e.g. the ECS platform's `dsql:cluster`); defaults to the
    /// resource's own convention.
    #[serde(default)]
    pub id: Option<String>,
}

impl Kind<Resource> for DsqlCluster {
    fn realize(&self, placement: &PlacementContext) -> Result<Resource, KindError> {
        Ok(Resource {
            identity: self.identity.clone(),
            region: self.region.clone(),
            mode: self.mode,
            endpoint: self.endpoint.clone(),
            arn: self.arn.clone(),
            fallback_identifier: None,
            resource_id: self.id.clone().map(tokeira_iac::ResourceId),
            module: placement.module.clone(),
            project: placement.deployment_id.clone(),
            tags: placement.tags.clone().into_iter().collect(),
        })
    }
}
