//! Test utilities for observability contracts.

use crate::{ManifestError, MetricManifest, validate_manifests};

/// Validate manifests from tests without installing global recorders.
pub fn assert_manifests_valid(manifests: &[&MetricManifest]) -> Result<(), ManifestError> {
    validate_manifests(manifests)
}
