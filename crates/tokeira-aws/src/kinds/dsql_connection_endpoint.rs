//! Typed author input for a DSQL VPC connection endpoint.

use serde::Deserialize;
use tokeira_iac::ResourceId;
use tokeira_platform::{
    error::KindError,
    kind::{Kind, PlacementContext},
};

use crate::resources::dsql_connection_endpoint::{
    DsqlConnectionEndpoint as Resource, DsqlConnectionEndpointConfig,
};

/// Author-visible name of the realized resource type.
pub const TYPE: &str = "DsqlConnectionEndpoint";

/// Reusable author input for the DSQL connection endpoint. Declares the
/// VPC, an endpoint security group, and the DSQL cluster as dependencies.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DsqlConnectionEndpoint {
    /// AWS region.
    pub region: String,
    /// Endpoint identity used in provider naming.
    pub identity: String,
    /// Stable resource-id override for the well-known
    /// `dsql:connection-endpoint` identity; defaults to the resource's own
    /// convention.
    #[serde(default)]
    pub id: Option<String>,
}

impl Kind<Resource> for DsqlConnectionEndpoint {
    fn realize(&self, placement: &PlacementContext) -> Result<Resource, KindError> {
        let vpc = super::required_dependency(placement, "Vpc", |id| id.ends_with("-vpc"))?;
        let security_group =
            super::required_dependency(placement, "SecurityGroup", |id| id.starts_with("sg-"))?;
        let cluster =
            super::required_dependency(placement, "DsqlCluster", |id| id == "dsql:cluster")?;
        let rctx = super::resource_context(&self.region, placement);
        Ok(Resource::new(
            self.identity.clone(),
            DsqlConnectionEndpointConfig {
                vpc_dependency: vpc,
                security_group_dependency: security_group,
                dsql_cluster_dependency: cluster,
                resource_id: self.id.clone().map(ResourceId),
                module: placement.module.clone(),
            },
            &rctx,
        ))
    }
}
