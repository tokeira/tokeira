//! Where one Deployment's repository lives.
//!
//! The locator is serialized into `metadata.json`
//! (`deployment_repository.locator`) and displayed by every repository verb;
//! it is identity-free — physical location never participates in any digest.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::error::LocatorError;

/// One repository's home: a local filesystem directory for a local
/// deployment, an S3 base for remote state. Same object contract in both.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum RepositoryLocator {
    /// A directory under the deployments root
    /// (`<deployments-root>/repositories/<name>/`).
    Local {
        /// Absolute repository directory.
        path: PathBuf,
    },
    /// `s3://{bucket}/{prefix}/` — the deployment's home under the remote
    /// deployments base.
    S3 {
        /// Bucket name.
        bucket: String,
        /// Key prefix without leading or trailing slash.
        prefix: String,
    },
}

impl RepositoryLocator {
    /// The TUF metadata base URL (`…/metadata/`).
    pub(crate) fn metadata_url(&self) -> Result<url::Url, LocatorError> {
        self.join("metadata/")
    }

    /// The TUF targets base URL (`…/targets/`).
    pub(crate) fn targets_url(&self) -> Result<url::Url, LocatorError> {
        self.join("targets/")
    }

    fn join(&self, sub: &str) -> Result<url::Url, LocatorError> {
        let base = match self {
            Self::Local { path } => {
                url::Url::from_directory_path(path).map_err(|()| LocatorError::LocalPath {
                    path: path.display().to_string(),
                })?
            }
            Self::S3 { bucket, prefix } => {
                if bucket.is_empty()
                    || prefix.is_empty()
                    || prefix.starts_with('/')
                    || prefix.ends_with('/')
                    || bucket.chars().any(char::is_control)
                    || prefix.chars().any(char::is_control)
                {
                    return Err(LocatorError::S3Shape {
                        bucket: bucket.clone(),
                        prefix: prefix.clone(),
                    });
                }
                url::Url::parse(&format!("s3://{bucket}/{prefix}/")).map_err(|error| {
                    LocatorError::S3Parse {
                        bucket: bucket.clone(),
                        prefix: prefix.clone(),
                        error: error.to_string(),
                    }
                })?
            }
        };
        base.join(sub).map_err(|error| LocatorError::Join {
            sub: sub.to_string(),
            error: error.to_string(),
        })
    }

    /// Operator-facing rendering (`file:///…` or `s3://…`).
    pub fn display(&self) -> String {
        match self {
            Self::Local { path } => format!("file://{}", path.display()),
            Self::S3 { bucket, prefix } => format!("s3://{bucket}/{prefix}/"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn locator_serde_round_trips_and_rejects_unknown_fields() {
        let local = RepositoryLocator::Local {
            path: PathBuf::from("/var/deployments/repositories/dev"),
        };
        let json = serde_json::to_value(&local).unwrap();
        assert_eq!(json["kind"], "local");
        let back: RepositoryLocator = serde_json::from_value(json).unwrap();
        assert_eq!(back, local);

        let s3 = RepositoryLocator::S3 {
            bucket: "deployments".to_string(),
            prefix: "prod/api".to_string(),
        };
        let json = serde_json::to_value(&s3).unwrap();
        assert_eq!(json["kind"], "s3");
        let back: RepositoryLocator = serde_json::from_value(json).unwrap();
        assert_eq!(back, s3);

        let unknown = serde_json::json!({"kind": "s3", "bucket": "b", "prefix": "p", "extra": 1});
        assert!(serde_json::from_value::<RepositoryLocator>(unknown).is_err());
    }

    #[test]
    fn urls_join_and_bad_shapes_refuse() {
        let s3 = RepositoryLocator::S3 {
            bucket: "b".to_string(),
            prefix: "deployments/dev".to_string(),
        };
        assert_eq!(
            s3.metadata_url().unwrap().as_str(),
            "s3://b/deployments/dev/metadata/"
        );
        assert_eq!(
            s3.targets_url().unwrap().as_str(),
            "s3://b/deployments/dev/targets/"
        );

        for bad in ["/lead", "trail/", ""] {
            let locator = RepositoryLocator::S3 {
                bucket: "b".to_string(),
                prefix: bad.to_string(),
            };
            assert!(
                locator.metadata_url().is_err(),
                "prefix `{bad}` must refuse"
            );
        }
    }
}
