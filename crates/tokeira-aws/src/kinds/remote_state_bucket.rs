//! Typed author input for Tokeira's AWS-backed remote-state bucket.

use serde::Deserialize;
use tokeira_platform::{
    error::KindError,
    kind::{Kind, PlacementContext},
};

use crate::resources::remote_state_bucket::RemoteStateBucket as Resource;

/// Author-visible name of the realized resource type.
pub const TYPE: &str = Resource::TYPE;

/// Reusable author input for the shared remote-state bucket. Versioning is
/// always on and deployment destruction never removes the bucket; those are
/// foundation policy rather than authorable choices.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteStateBucket {
    /// AWS region.
    pub region: String,
    /// Full bucket name.
    pub bucket: String,
    /// Key prefix recorded for state consumers.
    #[serde(default)]
    pub key_prefix: Option<String>,
}

impl Kind<Resource> for RemoteStateBucket {
    fn realize(&self, placement: &PlacementContext) -> Result<Resource, KindError> {
        Ok(Resource::new(
            self.bucket.clone(),
            self.region.clone(),
            self.key_prefix.clone(),
            placement.module.clone(),
        ))
    }
}
