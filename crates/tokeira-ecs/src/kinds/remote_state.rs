//! Typed author input for the remote-state bucket.

use serde::Deserialize;
use tokeira_aws::resources::s3_bucket::{S3Bucket, S3BucketConfig};
use tokeira_platform::{
    error::KindError,
    kind::{Kind, PlacementContext},
};

use crate::modules::remote_state::RemoteStateBucket as Resource;

/// Author-visible name of the realized resource type.
pub const TYPE: &str = "RemoteStateBucket";

/// Reusable author input for the shared remote-state bucket. Versioning is
/// always on and delete never removes the bucket — those are the wrapper's
/// policy, not authorable choices.
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
        let rctx = tokeira_aws::ResourceContext {
            project: placement.deployment_id.clone(),
            region: self.region.clone(),
            tags: placement.tags.clone().into_iter().collect(),
        };
        Ok(Resource::wrap(S3Bucket::new(
            self.bucket.clone(),
            S3BucketConfig {
                versioning: true,
                module: placement.module.clone(),
                key_prefix: self.key_prefix.clone(),
            },
            &rctx,
        )))
    }
}
