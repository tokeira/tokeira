//! Deployment listing: enumerate the two repository homes.
//!
//! Listing reports names and locators; it never verifies — `inspect` is the
//! verifying verb. Local deployments enumerate from the deployments root's
//! `repositories/` directory; remote-state deployments from the configured
//! remote deployments base, where each repository lives under its name.

use std::path::Path;

use super::{error::LocatorError, locator::RepositoryLocator};

/// One listed deployment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentEntry {
    /// The deployment name (repository directory / prefix segment).
    pub name: String,
    /// The repository's locator, ready for `open`/`fetch`/`inspect`.
    pub locator: RepositoryLocator,
}

/// Enumerate local repositories under `<deployments_root>/repositories/`.
pub fn list_local(deployments_root: &Path) -> Result<Vec<DeploymentEntry>, LocatorError> {
    let repositories = deployments_root.join("repositories");
    let mut entries = Vec::new();
    let read = match std::fs::read_dir(&repositories) {
        Ok(read) => read,
        // No repositories yet is an empty listing, not an error.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(entries),
        Err(error) => {
            return Err(LocatorError::LocalPath {
                path: format!("{}: {error}", repositories.display()),
            });
        }
    };
    for entry in read {
        let entry = entry.map_err(|error| LocatorError::LocalPath {
            path: format!("{}: {error}", repositories.display()),
        })?;
        if entry.path().is_dir()
            && let Some(name) = entry.file_name().to_str()
        {
            entries.push(DeploymentEntry {
                name: name.to_string(),
                locator: RepositoryLocator::Local { path: entry.path() },
            });
        }
    }
    entries.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(entries)
}

/// Enumerate remote-state repositories under the remote deployments base:
/// each common prefix directly under `{prefix}/` is one deployment name.
pub async fn list_remote(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
) -> Result<Vec<DeploymentEntry>, LocatorError> {
    let mut entries = Vec::new();
    let mut continuation: Option<String> = None;
    loop {
        let mut request = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(format!("{prefix}/"))
            .delimiter("/");
        if let Some(token) = &continuation {
            request = request.continuation_token(token);
        }
        let response = request
            .send()
            .await
            .map_err(|error| LocatorError::S3Parse {
                bucket: bucket.to_string(),
                prefix: prefix.to_string(),
                error: error.to_string(),
            })?;
        for common in response.common_prefixes() {
            if let Some(full) = common.prefix() {
                let name = full
                    .trim_start_matches(&format!("{prefix}/"))
                    .trim_end_matches('/');
                if !name.is_empty() {
                    entries.push(DeploymentEntry {
                        name: name.to_string(),
                        locator: RepositoryLocator::S3 {
                            bucket: bucket.to_string(),
                            prefix: format!("{prefix}/{name}"),
                        },
                    });
                }
            }
        }
        match response.next_continuation_token() {
            Some(token) => continuation = Some(token.to_string()),
            None => break,
        }
    }
    entries.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_listing_enumerates_repository_dirs_sorted_and_tolerates_absence() {
        let root = tempfile::tempdir().unwrap();
        assert!(list_local(root.path()).unwrap().is_empty());

        let repositories = root.path().join("repositories");
        std::fs::create_dir_all(repositories.join("zeta")).unwrap();
        std::fs::create_dir_all(repositories.join("alpha")).unwrap();
        std::fs::write(repositories.join("not-a-repo.txt"), b"x").unwrap();

        let listed = list_local(root.path()).unwrap();
        let names: Vec<&str> = listed.iter().map(|entry| entry.name.as_str()).collect();
        assert_eq!(names, ["alpha", "zeta"]);
        assert!(matches!(
            &listed[0].locator,
            RepositoryLocator::Local { path } if path.ends_with("repositories/alpha")
        ));
    }
}
