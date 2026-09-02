//! Deployment-state placement and construction.
//!
//! A command prepares one [`DeploymentStateStores`] value after admitting the
//! deployment, then uses that value for the envelope, infrastructure state,
//! deploy state, and operation lock. Keeping those four handles behind one
//! value prevents a deployment from accidentally splitting its authoritative
//! documents between local disk and remote storage.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use tokeira_iac::{InfraState, RuntimeState};
use tokeira_state::{
    CasStore, DeploymentStore, LocalBackend, OperationLock, S3Backend, S3StateStore,
};

use crate::DeploymentStateEnvelope;

const ENVELOPE_STATE: &str = "envelope";
const INFRA_STATE: &str = "infra";
const DEPLOY_STATE: &str = "deploy";
const LOCK_STATE: &str = "lock";
const OPERATION_LOCK: &str = "operation";

/// Durable placement recorded for all authoritative state of one deployment.
///
/// Existing metadata defaults to [`Local`](Self::Local). An S3 prefix is the
/// deployment-scoped root below which the envelope, infrastructure state,
/// deploy state, and operation lock receive separate namespaces.
///
/// S3 placement deliberately does not imply bucket ownership. The bucket and
/// prefix must already exist/be writable under ambient AWS credentials, and
/// Tokeira neither changes bucket policy/lifecycle nor removes the prefix at
/// deployment destroy. That keeps shared-bucket governance with the operator
/// and preserves the immutable snapshots as recovery/audit material.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "backend", rename_all = "snake_case")]
pub enum DeploymentStateLocation {
    /// Persist state below the deployment directory on the current machine.
    #[default]
    Local,
    /// Persist state in an operator-owned S3 bucket.
    S3 {
        /// Bucket containing the deployment's state.
        bucket: String,
        /// AWS region in which the bucket resides.
        region: String,
        /// Deployment-scoped key prefix within the bucket.
        prefix: String,
    },
}

impl DeploymentStateLocation {
    /// Validate the recorded location before constructing any SDK client or
    /// opening state.
    pub fn validate(&self) -> Result<(), StateLocationError> {
        let Self::S3 {
            bucket,
            region,
            prefix,
        } = self
        else {
            return Ok(());
        };

        validate_bucket(bucket)?;
        validate_region(region)?;
        validate_component("prefix", prefix)?;
        if prefix.starts_with('/')
            || prefix.ends_with('/')
            || prefix.contains("//")
            || prefix.contains('\\')
            || prefix
                .split('/')
                .any(|segment| matches!(segment, "." | ".."))
        {
            return Err(StateLocationError::InvalidPrefix(prefix.clone()));
        }
        Ok(())
    }

    /// Return the operator-facing S3 URI for remote placement.
    ///
    /// This is intentionally only a locator. It contains no credentials and
    /// therefore remains safe to persist in signed deployment claims and to
    /// display after the local deployment record has been removed.
    pub fn remote_uri(&self) -> Option<String> {
        match self {
            Self::Local => None,
            Self::S3 { bucket, prefix, .. } => Some(format!("s3://{bucket}/{prefix}")),
        }
    }
}

fn validate_bucket(bucket: &str) -> Result<(), StateLocationError> {
    let valid_length = (3..=63).contains(&bucket.len());
    let valid_characters = bucket.bytes().all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-')
    });
    let bounded_by_alphanumeric = bucket
        .as_bytes()
        .first()
        .is_some_and(u8::is_ascii_alphanumeric)
        && bucket
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric);
    if !valid_length
        || !valid_characters
        || !bounded_by_alphanumeric
        || bucket.contains("..")
        || bucket.contains(".-")
        || bucket.contains("-.")
    {
        return Err(StateLocationError::InvalidBucket(bucket.to_owned()));
    }
    Ok(())
}

fn validate_region(region: &str) -> Result<(), StateLocationError> {
    let valid = !region.is_empty()
        && region
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && region
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && region
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric);
    if !valid {
        return Err(StateLocationError::InvalidRegion(region.to_owned()));
    }
    Ok(())
}

fn validate_component(name: &'static str, value: &str) -> Result<(), StateLocationError> {
    if value.trim().is_empty() || value.trim() != value || value.chars().any(char::is_control) {
        return Err(StateLocationError::InvalidComponent {
            name,
            value: value.to_owned(),
        });
    }
    Ok(())
}

/// A malformed deployment-state location.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum StateLocationError {
    /// The bucket does not use the canonical S3 general-purpose name shape.
    #[error("remote-state bucket must be a 3-63 character lowercase S3 bucket name (got {0:?})")]
    InvalidBucket(String),
    /// The region cannot be used as an AWS SDK region identifier.
    #[error("remote-state region must contain lowercase letters, digits, and hyphens (got {0:?})")]
    InvalidRegion(String),
    /// A required S3 locator component is empty or contains unsafe whitespace.
    #[error(
        "remote-state {name} must be non-empty and contain no surrounding or control whitespace (got {value:?})"
    )]
    InvalidComponent {
        /// Component name used in the operator-facing error.
        name: &'static str,
        /// Rejected value.
        value: String,
    },
    /// The deployment prefix is not canonical.
    #[error(
        "remote-state prefix must be relative, use forward slashes, have no trailing slash, and contain no empty, `.` or `..` segment (got {0:?})"
    )]
    InvalidPrefix(String),
}

/// Prepared constructors for every authoritative state handle of a deployment.
///
/// The value is created once at command admission. Store construction itself is
/// synchronous thereafter, so individual verbs do not need to load AWS SDK
/// configuration or independently decide where a document lives.
#[derive(Debug, Clone)]
pub enum DeploymentStateStores {
    /// Filesystem-backed stores below one deployment root.
    Local {
        /// Deployment directory containing the `state` directory.
        deployment_dir: PathBuf,
    },
    /// S3-backed stores below one deployment-scoped prefix.
    S3 {
        /// Prepared client for the bucket's recorded region.
        client: aws_sdk_s3::Client,
        /// Bucket containing the state hierarchy.
        bucket: String,
        /// Deployment-scoped root shared by all state documents.
        prefix: String,
    },
}

impl DeploymentStateStores {
    /// Prepare local stores below `deployment_dir`.
    pub fn local(deployment_dir: impl Into<PathBuf>) -> Self {
        Self::Local {
            deployment_dir: deployment_dir.into(),
        }
    }

    /// Prepare S3 stores below one deployment prefix using a region-specific
    /// client. Callers validate the persisted [`DeploymentStateLocation`]
    /// before loading that client.
    pub fn s3(
        client: aws_sdk_s3::Client,
        bucket: impl Into<String>,
        prefix: impl Into<String>,
    ) -> Self {
        Self::S3 {
            client,
            bucket: bucket.into(),
            prefix: prefix.into(),
        }
    }

    /// Open the deployment envelope store.
    pub fn envelope_store(&self) -> Box<dyn DeploymentStore<DeploymentStateEnvelope>> {
        self.store(ENVELOPE_STATE)
    }

    /// Open the infrastructure-state store.
    pub fn infra_store(&self) -> Box<dyn DeploymentStore<InfraState>> {
        self.store(INFRA_STATE)
    }

    /// Open the runtime/deploy-state store.
    pub fn deploy_store(&self) -> Box<dyn DeploymentStore<RuntimeState>> {
        self.store(DEPLOY_STATE)
    }

    fn store<T>(&self, name: &str) -> Box<dyn DeploymentStore<T>>
    where
        T: Serialize
            + serde::de::DeserializeOwned
            + Default
            + tokeira_state::Validate
            + Send
            + Sync
            + 'static,
    {
        match self {
            Self::Local { deployment_dir } => Box::new(CasStore::new(
                Box::new(LocalBackend::new(deployment_dir.join("state").join(name))),
                name.to_owned(),
            )),
            Self::S3 {
                client,
                bucket,
                prefix,
            } => Box::new(S3StateStore::new(
                client.clone(),
                bucket.clone(),
                join_prefix(prefix, name),
            )),
        }
    }

    /// Open the cross-process operation lock in the same placement as state.
    pub fn operation_lock(&self) -> OperationLock {
        match self {
            Self::Local { deployment_dir } => OperationLock::new(
                Box::new(LocalBackend::new(
                    deployment_dir.join("state").join(LOCK_STATE),
                )),
                OPERATION_LOCK,
            ),
            Self::S3 {
                client,
                bucket,
                prefix,
            } => OperationLock::new(
                Box::new(S3Backend::new(
                    client.clone(),
                    bucket.clone(),
                    join_prefix(prefix, LOCK_STATE),
                )),
                OPERATION_LOCK,
            ),
        }
    }
}

fn join_prefix(prefix: &str, suffix: &str) -> String {
    format!("{prefix}/{suffix}")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn local_location_uses_the_stable_tagged_shape() {
        let location: DeploymentStateLocation =
            serde_json::from_value(serde_json::json!({ "backend": "local" })).unwrap();
        assert_eq!(location, DeploymentStateLocation::Local);
    }

    #[test]
    fn remote_prefix_must_be_canonical() {
        let location = DeploymentStateLocation::S3 {
            bucket: "shared-state".to_owned(),
            region: "eu-west-2".to_owned(),
            prefix: "/deployments/example/".to_owned(),
        };
        assert!(matches!(
            location.validate(),
            Err(StateLocationError::InvalidPrefix(_))
        ));
    }

    #[test]
    fn remote_bucket_and_region_are_validated_before_sdk_loading() {
        for location in [
            DeploymentStateLocation::S3 {
                bucket: "Not A Bucket".to_owned(),
                region: "eu-west-2".to_owned(),
                prefix: "deployments/example".to_owned(),
            },
            DeploymentStateLocation::S3 {
                bucket: "shared-state".to_owned(),
                region: "EU West 2".to_owned(),
                prefix: "deployments/example".to_owned(),
            },
        ] {
            assert!(location.validate().is_err());
        }
    }

    #[tokio::test]
    async fn local_bundle_keeps_documents_in_separate_namespaces() {
        let temp = tempfile::tempdir().unwrap();
        let stores = DeploymentStateStores::local(temp.path());

        stores
            .infra_store()
            .save(&InfraState::default(), "")
            .await
            .unwrap();
        stores
            .deploy_store()
            .save(&RuntimeState::default(), "")
            .await
            .unwrap();

        assert!(
            temp.path()
                .join("state/infra/infra/manifest.json")
                .is_file()
        );
        assert!(
            temp.path()
                .join("state/deploy/deploy/manifest.json")
                .is_file()
        );
    }

    #[tokio::test]
    async fn remote_bundle_round_trips_every_document_and_operation_lease_offline() {
        let bucket: crate::repository::testkit::Bucket = Default::default();
        let stores = DeploymentStateStores::s3(
            crate::repository::testkit::s3_client(bucket.clone()),
            "shared-state",
            "deployments/example",
        );

        stores
            .envelope_store()
            .save(&DeploymentStateEnvelope::default(), "")
            .await
            .unwrap();
        stores
            .infra_store()
            .save(&InfraState::default(), "")
            .await
            .unwrap();
        stores
            .deploy_store()
            .save(&RuntimeState::default(), "")
            .await
            .unwrap();
        let lock = stores.operation_lock();
        let guard = lock
            .acquire("offline-seat", Duration::from_secs(60))
            .await
            .unwrap();
        lock.release(guard).await.unwrap();

        let (envelope, _) = stores.envelope_store().load().await.unwrap();
        assert_eq!(envelope, DeploymentStateEnvelope::default());

        let objects = bucket.lock().unwrap();
        for namespace in ["envelope", "infra", "deploy"] {
            assert!(
                objects.contains_key(&format!(
                    "shared-state/deployments/example/{namespace}/manifest.json"
                )),
                "{namespace} manifest must share the deployment prefix"
            );
        }
        assert!(
            objects.contains_key("shared-state/deployments/example/lock/operation/manifest.json")
        );
    }
}
