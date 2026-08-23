//! Canonical Aurora DSQL identity validation.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// An immutable AWS DSQL cluster identity.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CanonicalClusterIdentity {
    /// AWS Region containing the cluster.
    pub region: String,
    /// AWS DSQL cluster identifier.
    pub cluster_id: String,
    /// Full AWS ARN for the cluster.
    pub cluster_arn: String,
}

impl CanonicalClusterIdentity {
    /// Validates that Region, ID, and ARN describe exactly the same DSQL cluster.
    pub fn new(
        region: impl Into<String>,
        cluster_id: impl Into<String>,
        cluster_arn: impl Into<String>,
    ) -> Result<Self, IdentityError> {
        let identity = Self {
            region: region.into(),
            cluster_id: cluster_id.into(),
            cluster_arn: cluster_arn.into(),
        };
        identity.validate()?;
        Ok(identity)
    }

    /// Revalidates a deserialized identity.
    pub fn validate(&self) -> Result<(), IdentityError> {
        validate_region(&self.region)?;
        if self.cluster_id.len() != 26
            || !self
                .cluster_id
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        {
            return Err(IdentityError::InvalidClusterId);
        }

        let mut parts = self.cluster_arn.splitn(6, ':');
        let prefix = parts.next();
        let partition = parts.next();
        let service = parts.next();
        let arn_region = parts.next();
        let account = parts.next();
        let resource = parts.next();
        if prefix != Some("arn")
            || !partition.is_some_and(valid_partition)
            || service != Some("dsql")
            || account.is_none_or(|value| {
                value.len() != 12 || !value.bytes().all(|byte| byte.is_ascii_digit())
            })
        {
            return Err(IdentityError::InvalidArn);
        }
        let arn_region = arn_region.ok_or(IdentityError::InvalidArn)?;
        validate_region(arn_region)?;
        let arn_id = resource
            .and_then(|value| value.strip_prefix("cluster/"))
            .ok_or(IdentityError::InvalidArn)?;
        if arn_region != self.region {
            return Err(IdentityError::RegionMismatch);
        }
        if arn_id != self.cluster_id {
            return Err(IdentityError::ClusterIdMismatch);
        }
        Ok(())
    }
}

fn valid_partition(partition: &str) -> bool {
    partition.starts_with("aws")
        && partition
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

fn validate_region(region: &str) -> Result<(), IdentityError> {
    if region.is_empty()
        || region.starts_with('-')
        || region.ends_with('-')
        || !region
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err(IdentityError::InvalidRegion);
    }
    Ok(())
}

/// Why a purported canonical identity was rejected.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum IdentityError {
    /// The Region is missing or has invalid syntax.
    #[error("invalid AWS Region")]
    InvalidRegion,
    /// The DSQL identifier is not 26 lowercase alphanumeric characters.
    #[error("invalid Aurora DSQL cluster ID")]
    InvalidClusterId,
    /// The ARN is not a syntactically valid DSQL cluster ARN.
    #[error("invalid Aurora DSQL cluster ARN")]
    InvalidArn,
    /// The ARN Region differs from the configured Region.
    #[error("cluster ARN Region does not match configured Region")]
    RegionMismatch,
    /// The ARN resource differs from the configured cluster ID.
    #[error("cluster ARN resource does not match cluster ID")]
    ClusterIdMismatch,
}

#[cfg(test)]
mod tests {
    use super::{CanonicalClusterIdentity, IdentityError};

    const ID: &str = "abcdefghijklmnopqrstuv1234";

    #[test]
    fn accepts_matching_identity() {
        let identity = CanonicalClusterIdentity::new(
            "eu-west-2",
            ID,
            format!("arn:aws:dsql:eu-west-2:123456789012:cluster/{ID}"),
        );
        assert!(identity.is_ok());
    }

    #[test]
    fn rejects_every_partial_or_conflicting_identity() {
        let cases = [
            ("", ID, format!("arn:aws:dsql::123456789012:cluster/{ID}")),
            (
                "eu-west-2",
                "short",
                format!("arn:aws:dsql:eu-west-2:123456789012:cluster/{ID}"),
            ),
            (
                "eu-west-2",
                ID,
                format!("arn:aws:s3:eu-west-2:123456789012:cluster/{ID}"),
            ),
            (
                "eu-west-2",
                ID,
                format!("arn:aws:dsql:us-east-1:123456789012:cluster/{ID}"),
            ),
            (
                "eu-west-2",
                ID,
                "arn:aws:dsql:eu-west-2:123456789012:cluster/zzzzzzzzzzzzzzzzzzzzzzzzzz".to_owned(),
            ),
        ];
        for (region, id, arn) in cases {
            assert!(CanonicalClusterIdentity::new(region, id, arn).is_err());
        }
    }

    #[test]
    fn reports_region_disagreement() {
        let error = CanonicalClusterIdentity::new(
            "eu-west-2",
            ID,
            format!("arn:aws:dsql:us-east-1:123456789012:cluster/{ID}"),
        )
        .expect_err("mismatched Region must be rejected");
        assert_eq!(error, IdentityError::RegionMismatch);
    }
}
