//! Typed author input for a VPC endpoint.

use serde::Deserialize;
use tokeira_iac::ResourceId;
use tokeira_platform::{
    error::KindError,
    kind::{Kind, PlacementContext},
};

use crate::resources::vpc_endpoint::{EndpointType, VpcEndpoint as Resource, VpcEndpointConfig};

/// Author-visible name of the realized resource type.
pub(crate) const TYPE: &str = "VpcEndpoint";

/// Authored endpoint flavour, mirroring the resource's [`EndpointType`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum EndpointKind {
    /// Interface endpoint (ENI-backed).
    Interface,
    /// Gateway endpoint (route-table entry).
    Gateway,
}

/// Reusable author input for a VPC endpoint. The owning VPC is a declared
/// dependency; a declared security group (id `sg-…`) is attached when
/// present, matching interface-endpoint usage.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VpcEndpoint {
    /// AWS region.
    pub(crate) region: String,
    /// Short name used in the default resource id (`vpce-<short>`).
    pub(crate) short_name: String,
    /// Full provider service name (`com.amazonaws.<region>.<service>`).
    pub(crate) service_name: String,
    /// Endpoint flavour.
    pub(crate) endpoint_type: EndpointKind,
    /// Stable resource-id override for endpoints that carry a well-known
    /// identity (e.g. the DSQL management endpoint); defaults to the
    /// resource's own `vpce-<short>` convention.
    #[serde(default)]
    pub(crate) id: Option<String>,
}

impl Kind<Resource> for VpcEndpoint {
    fn realize(&self, placement: &PlacementContext) -> Result<Resource, KindError> {
        let vpc = super::required_dependency(placement, "Vpc", |id| id.ends_with("-vpc"))?;
        let security_group = super::optional_dependency(placement, |id| id.starts_with("sg-"));
        let rctx = super::resource_context(&self.region, placement);
        Ok(Resource::new(
            self.short_name.clone(),
            VpcEndpointConfig {
                service_name: self.service_name.clone(),
                endpoint_type: match self.endpoint_type {
                    EndpointKind::Interface => EndpointType::Interface,
                    EndpointKind::Gateway => EndpointType::Gateway,
                },
                vpc_dependency: vpc,
                security_group_dependency: security_group,
                resource_id: self.id.clone().map(ResourceId),
                module: placement.module.clone(),
            },
            &rctx,
        ))
    }
}
