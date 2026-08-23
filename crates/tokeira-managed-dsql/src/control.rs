//! Contract-shaped Aurora DSQL control-plane interface.

use std::{collections::BTreeMap, fmt, time::Duration};

use async_trait::async_trait;
use thiserror::Error;

use crate::descriptor::DsqlClientToken;

/// Closed create request for single-Region managed embedded clusters.
///
/// The type intentionally has no multi-Region, KMS, resource-policy, or policy-bypass
/// fields. That makes the approved AWS field policy a compile-time boundary.
#[derive(Clone, PartialEq, Eq)]
pub struct CreateClusterRequest {
    /// Region in which the adapter must issue the request.
    pub region: String,
    /// Explicit descriptor-backed idempotency token.
    pub client_token: DsqlClientToken,
    /// Deletion protection; managed startup requires this to be true.
    pub deletion_protection_enabled: bool,
    /// Optional metadata. Tags are never used for identity or recovery.
    pub tags: BTreeMap<String, String>,
}

impl fmt::Debug for CreateClusterRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CreateClusterRequest")
            .field("region", &self.region)
            .field("client_token", &"[REDACTED]")
            .field(
                "deletion_protection_enabled",
                &self.deletion_protection_enabled,
            )
            .field("tags", &self.tags)
            .finish()
    }
}

impl CreateClusterRequest {
    /// Validates managed-mode constraints and AWS tag limits.
    pub fn validate(&self) -> Result<(), DsqlControlError> {
        if self.region.is_empty() {
            return Err(DsqlControlError::Validation {
                field: "region",
                reason: "must not be empty",
            });
        }
        if !self.deletion_protection_enabled {
            return Err(DsqlControlError::Validation {
                field: "deletion_protection_enabled",
                reason: "must be true for managed embedded creation",
            });
        }
        if self.tags.len() > 200 {
            return Err(DsqlControlError::Validation {
                field: "tags",
                reason: "must contain at most 200 entries",
            });
        }
        if self.tags.iter().any(|(key, value)| {
            key.is_empty()
                || key.len() > 128
                || value.len() > 256
                || !key.is_ascii()
                || !value.is_ascii()
        }) {
            return Err(DsqlControlError::Validation {
                field: "tags",
                reason: "contains a key or value outside AWS length/character limits",
            });
        }
        Ok(())
    }
}

/// Administrative request to change deletion protection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetDeletionProtectionRequest {
    /// Adapter Region.
    pub region: String,
    /// Canonical cluster ID; never an endpoint or tag selector.
    pub cluster_id: String,
    /// Desired protection state.
    pub enabled: bool,
    /// Operation-specific idempotency token.
    pub client_token: DsqlClientToken,
}

/// Administrative request to delete one canonical cluster.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteClusterRequest {
    /// Adapter Region.
    pub region: String,
    /// Canonical cluster ID; never an endpoint or tag selector.
    pub cluster_id: String,
    /// Operation-specific idempotency token.
    pub client_token: DsqlClientToken,
}

/// Contract-shaped result of an AWS create/get/update operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterObservation {
    /// Region used for the AWS operation.
    pub region: String,
    /// Canonical AWS DSQL cluster ID.
    pub identifier: String,
    /// Canonical cluster ARN.
    pub arn: String,
    /// Current refreshable connection locator.
    pub endpoint: String,
    /// Current AWS lifecycle status.
    pub status: ClusterStatus,
    /// Whether AWS currently protects the cluster from deletion.
    pub deletion_protection_enabled: bool,
    /// Whether AWS reported multi-Region properties.
    pub multi_region: bool,
}

/// Aurora DSQL status, including future SDK values without silently defaulting.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClusterStatus {
    /// Cluster provisioning is in progress.
    Creating,
    /// Cluster is ready for connections.
    Active,
    /// Scale-to-zero cluster must be woken by a connection.
    Idle,
    /// Long-idle cluster must be woken by a connection.
    Inactive,
    /// Cluster update is in progress.
    Updating,
    /// Deletion is in progress.
    Deleting,
    /// Cluster has been deleted.
    Deleted,
    /// Cluster entered a terminal failure.
    Failed,
    /// Multi-Region setup state, unsupported by managed embedded mode.
    PendingSetup,
    /// Multi-Region deletion state, unsupported by managed embedded mode.
    PendingDelete,
    /// A future AWS value unknown to this release.
    Unknown(String),
}

/// Control-plane operations; generated AWS SDK types never escape this trait.
#[async_trait]
pub trait DsqlControlPlane: Send + Sync + fmt::Debug {
    /// Creates or recovers the idempotent cluster associated with the explicit token.
    async fn create_cluster(
        &self,
        request: CreateClusterRequest,
    ) -> Result<ClusterObservation, DsqlControlError>;

    /// Gets exactly one cluster by canonical ID in the named Region.
    async fn get_cluster(
        &self,
        region: &str,
        cluster_id: &str,
    ) -> Result<ClusterObservation, DsqlControlError>;

    /// Changes deletion protection for an explicit administrative workflow.
    async fn set_deletion_protection(
        &self,
        request: SetDeletionProtectionRequest,
    ) -> Result<ClusterObservation, DsqlControlError>;

    /// Deletes a cluster for an explicit administrative workflow.
    async fn delete_cluster(
        &self,
        request: DeleteClusterRequest,
    ) -> Result<ClusterStatus, DsqlControlError>;
}

/// Class of retryable AWS failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetryableErrorKind {
    /// Request throttling.
    Throttling,
    /// Transient internal service failure.
    Internal,
    /// Create request conflicts while the idempotent operation converges.
    Conflict,
    /// SDK dispatch or response failure whose retry is bounded by startup.
    Transport,
}

/// Redacted, policy-shaped control-plane failure.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum DsqlControlError {
    /// The adapter Region does not match the requested Region.
    #[error("control-plane Region mismatch (adapter {adapter}, request {request})")]
    RegionMismatch {
        /// Region bound to the adapter.
        adapter: String,
        /// Region requested by the lifecycle.
        request: String,
    },
    /// Caller input violates the feature or AWS request contract.
    #[error("invalid {field}: {reason}")]
    Validation {
        /// Invalid logical field.
        field: &'static str,
        /// Stable remediation-oriented reason.
        reason: &'static str,
    },
    /// AWS denied the operation.
    #[error("AWS denied the Aurora DSQL operation")]
    AccessDenied,
    /// AWS could not find the canonical cluster ID.
    #[error("Aurora DSQL cluster was not found")]
    NotFound,
    /// AWS rejected creation because a service quota is exhausted.
    #[error(
        "Aurora DSQL quota exceeded (service {service_code}, quota {quota_code}); request a quota increase or remove an unused cluster"
    )]
    QuotaExceeded {
        /// AWS service-code dimension.
        service_code: String,
        /// AWS quota-code dimension.
        quota_code: String,
    },
    /// The operation may be retried within the caller's absolute deadline.
    #[error("retryable Aurora DSQL control-plane failure: {kind:?}")]
    Retryable {
        /// Stable failure class.
        kind: RetryableErrorKind,
        /// AWS-supplied minimum delay, when present.
        retry_after: Option<Duration>,
    },
    /// A future or malformed SDK response cannot be handled safely.
    #[error("unexpected Aurora DSQL response ({code})")]
    Unexpected {
        /// AWS error code or stable local classification.
        code: String,
    },
}
