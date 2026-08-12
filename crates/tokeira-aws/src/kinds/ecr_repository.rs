//! Typed author input for an ECR repository.

use serde::Deserialize;
use tokeira_platform::{
    error::KindError,
    kind::{Kind, PlacementContext},
};

use crate::resources::ecr_repository::EcrRepository as Resource;

/// Author-visible name of the realized resource type.
pub const TYPE: &str = "EcrRepository";

/// Reusable author input for an ECR repository.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EcrRepository {
    /// Full repository name.
    pub repository: String,
}

impl Kind<Resource> for EcrRepository {
    fn realize(&self, placement: &PlacementContext) -> Result<Resource, KindError> {
        // The resource validates repository naming itself; its refusal is the
        // authored error, located by the framework.
        Resource::new(self.repository.clone(), placement.module.clone())
            .map_err(|error| KindError::new(error.to_string()))
    }
}
