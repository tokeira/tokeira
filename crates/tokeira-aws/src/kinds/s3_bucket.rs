//! Typed author input for an S3 bucket.

use serde::Deserialize;
use tokeira_platform::{
    error::KindError,
    kind::{Kind, PlacementContext},
};

use crate::resources::s3_bucket::{S3Bucket as Resource, S3BucketConfig};

/// Author-visible name of the realized resource type.
pub const TYPE: &str = "S3Bucket";

/// Reusable author input for an S3 bucket.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3Bucket {
    /// AWS region.
    pub region: String,
    /// Full bucket name.
    pub bucket: String,
    /// Enable object versioning.
    #[serde(default)]
    pub versioning: bool,
    /// Optional key prefix recorded for consumers of the bucket.
    #[serde(default)]
    pub key_prefix: Option<String>,
}

impl Kind<Resource> for S3Bucket {
    fn realize(&self, placement: &PlacementContext) -> Result<Resource, KindError> {
        let rctx = super::resource_context(&self.region, placement);
        Ok(Resource::new(
            self.bucket.clone(),
            S3BucketConfig {
                versioning: self.versioning,
                module: placement.module.clone(),
                key_prefix: self.key_prefix.clone(),
            },
            &rctx,
        ))
    }
}
