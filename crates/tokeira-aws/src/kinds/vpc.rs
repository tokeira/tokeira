//! Typed author input for the private VPC resource.

use serde::Deserialize;
use tokeira_platform::{
    error::KindError,
    kind::{Kind, PlacementContext},
};

use crate::resources::vpc::{VpcConfig, VpcResource as Resource};

/// Author-visible name of the realized resource type. Lives on the kind
/// because `crates/tokeira-aws/src/kinds` is the authoring surface; the
/// namespace self-test pins it to the realized `resource_type()`.
pub(crate) const TYPE: &str = "Vpc";

/// Reusable author input for the project's private VPC.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Vpc {
    /// AWS region.
    pub(crate) region: String,
    /// VPC CIDR block.
    pub(crate) cidr: String,
    /// Availability zones the private subnets spread across.
    pub(crate) availability_zones: Vec<String>,
}

impl Kind<Resource> for Vpc {
    fn realize(&self, placement: &PlacementContext) -> Result<Resource, KindError> {
        let rctx = super::resource_context(&self.region, placement);
        Ok(Resource::new(
            &rctx,
            VpcConfig {
                cidr: self.cidr.clone(),
                availability_zones: self.availability_zones.clone(),
            },
            placement.module.clone(),
        ))
    }
}
