//! The publisher's configuration, persisted at create into
//! `state/repository/publisher.json` and read by lifecycle publication
//! hooks. A fetched seat has trust state but no publisher configuration
//! until keys are supplied — read-only by default, ahead of the
//! operation-lease spec.

use std::path::{Path, PathBuf};

use jiff::Span;
use serde::{Deserialize, Serialize};

use super::{error::PublishError, keys::RoleKeyConfig, locator::RepositoryLocator};

/// The publisher configuration, relative to a deployment dir.
pub(crate) const PUBLISHER_JSON: &str = "state/repository/publisher.json";

/// The pinned trust anchor, relative to a deployment dir.
pub const TRUST_ANCHOR: &str = "state/repository/root.json";

/// The TUF client datastore, relative to a deployment dir — what makes
/// rollback detection hold across separate loads.
pub const DATASTORE_DIR: &str = "state/repository/datastore";

/// Everything publish needs: where, signed by what, living how long.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepositoryConfig {
    /// The repository's home.
    pub locator: RepositoryLocator,
    /// One key source per role.
    pub keys: RoleKeyConfig,
    /// Role lifetimes; the timestamp lifetime is the freshness window.
    #[serde(default)]
    pub lifetimes: RoleLifetimes,
}

impl RepositoryConfig {
    /// The publisher.json path inside `deployment_dir`.
    pub(crate) fn path_in(deployment_dir: &Path) -> PathBuf {
        deployment_dir.join(PUBLISHER_JSON)
    }

    /// Load the publisher configuration from a deployment dir; `Ok(None)`
    /// when the seat has none (fetched read-only seats until keys arrive).
    pub fn load(deployment_dir: &Path) -> Result<Option<Self>, PublishError> {
        let path = Self::path_in(deployment_dir);
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(PublishError::Other(format!(
                    "reading {}: {error}",
                    path.display()
                )));
            }
        };
        serde_json::from_slice(&bytes).map(Some).map_err(|error| {
            PublishError::Other(format!("{} does not decode: {error}", path.display()))
        })
    }

    /// Persist the publisher configuration into a deployment dir (staged or
    /// committed), creating `state/repository/` as needed.
    pub fn store(&self, deployment_dir: &Path) -> Result<(), PublishError> {
        let path = Self::path_in(deployment_dir);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                PublishError::Other(format!("creating {}: {error}", parent.display()))
            })?;
        }
        let bytes = serde_json::to_vec_pretty(self)
            .map_err(|error| PublishError::Other(format!("serializing publisher.json: {error}")))?;
        std::fs::write(&path, bytes)
            .map_err(|error| PublishError::Other(format!("writing {}: {error}", path.display())))
    }
}

/// Role lifetimes in whole hours (jiff timestamp arithmetic takes absolute
/// units only; hours are the natural grain here).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoleLifetimes {
    /// root.json lifetime.
    pub(crate) root_hours: u32,
    /// targets.json / snapshot.json lifetime.
    pub(crate) metadata_hours: u32,
    /// timestamp.json lifetime — the freshness window; shortest by design.
    pub(crate) timestamp_hours: u32,
}

impl Default for RoleLifetimes {
    fn default() -> Self {
        Self {
            root_hours: 365 * 24,
            metadata_hours: 90 * 24,
            timestamp_hours: 14 * 24,
        }
    }
}

impl RoleLifetimes {
    /// root lifetime as a span.
    pub(crate) fn root(&self) -> Span {
        Span::new().hours(i64::from(self.root_hours))
    }

    /// metadata lifetime as a span.
    pub(crate) fn metadata(&self) -> Span {
        Span::new().hours(i64::from(self.metadata_hours))
    }

    /// freshness window as a span.
    pub(crate) fn timestamp(&self) -> Span {
        Span::new().hours(i64::from(self.timestamp_hours))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::keys::KeySourceConfig;

    #[test]
    fn config_round_trips_and_defaults_make_freshness_shortest() {
        let config = RepositoryConfig {
            locator: RepositoryLocator::S3 {
                bucket: "b".to_string(),
                prefix: "deployments/dev".to_string(),
            },
            keys: RoleKeyConfig {
                root: KeySourceConfig::File {
                    path: "/k/root.der".into(),
                },
                targets: KeySourceConfig::File {
                    path: "/k/targets.der".into(),
                },
                snapshot: KeySourceConfig::File {
                    path: "/k/snapshot.der".into(),
                },
                timestamp: KeySourceConfig::File {
                    path: "/k/timestamp.der".into(),
                },
            },
            lifetimes: RoleLifetimes::default(),
        };
        let json = serde_json::to_string(&config).unwrap();
        let back: RepositoryConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back, config);

        let defaults = RoleLifetimes::default();
        assert!(defaults.timestamp_hours < defaults.metadata_hours);
        assert!(defaults.metadata_hours < defaults.root_hours);

        let unknown = serde_json::json!({
            "locator": {"kind": "local", "path": "/r"},
            "keys": serde_json::to_value(&config.keys).unwrap(),
            "surprise": 1
        });
        assert!(serde_json::from_value::<RepositoryConfig>(unknown).is_err());
    }
}
