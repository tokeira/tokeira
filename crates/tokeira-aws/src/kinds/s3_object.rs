//! Typed author input for a single S3 object.

use serde::Deserialize;
use tokeira_platform::{
    error::KindError,
    kind::{Kind, PlacementContext},
};

use crate::resources::s3_object::S3Object as Resource;

/// Author-visible name of the realized resource type.
pub const TYPE: &str = "S3Object";

/// Reusable author input for one S3 object in a declared bucket.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3Object {
    /// Object key within the bucket.
    pub key: String,
    /// Object content.
    pub content: String,
    /// MIME content type.
    pub content_type: String,
}

impl Kind<Resource> for S3Object {
    fn realize(&self, placement: &PlacementContext) -> Result<Resource, KindError> {
        let bucket = super::required_dependency(placement, "S3Bucket", |id| id.starts_with("s3-"))?;
        Ok(Resource {
            bucket_dependency: bucket,
            key: self.key.clone(),
            content: self.content.clone(),
            content_type: self.content_type.clone(),
            module: placement.module.clone(),
        })
    }
}
