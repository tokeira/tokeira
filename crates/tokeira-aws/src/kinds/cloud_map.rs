//! Typed author input for a Cloud Map private DNS namespace.

use serde::Deserialize;
use tokeira_platform::{
    error::KindError,
    kind::{Kind, PlacementContext},
};

use crate::resources::ecs_service::CloudMapNamespaceResource as Resource;

/// Author-visible name of the realized resource type.
pub const TYPE: &str = "CloudMapNamespace";

/// Reusable author input for the Service Connect private DNS namespace.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloudMapNamespace {
    /// Namespace name.
    pub name: String,
}

impl Kind<Resource> for CloudMapNamespace {
    fn realize(&self, placement: &PlacementContext) -> Result<Resource, KindError> {
        let vpc = super::required_dependency(placement, "Vpc", |id| id.ends_with("-vpc"))?;
        Ok(Resource {
            name: self.name.clone(),
            vpc_dependency: vpc,
            module: placement.module.clone(),
        })
    }
}
