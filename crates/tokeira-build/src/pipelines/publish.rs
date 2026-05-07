use std::fmt;

use crate::{BuildError, DaggerClient};

#[derive(Clone, PartialEq, Eq)]
pub struct RegistryPassword(String);

impl RegistryPassword {
    pub fn new(password: impl Into<String>) -> Self {
        Self(password.into())
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for RegistryPassword {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RegistryPassword(***)")
    }
}

#[derive(Debug, Clone)]
pub struct PublishRequest {
    pub local_image: String,
    pub remote_refs: Vec<String>,
    pub registry_host: String,
    pub username: String,
    pub password: RegistryPassword,
}

#[derive(Debug, Clone)]
pub struct PublishResult {
    pub published: Vec<PublishedReference>,
}

#[derive(Debug, Clone)]
pub struct PublishedReference {
    pub remote_ref: String,
    pub published_ref: String,
}

pub fn publish_image(
    request: &PublishRequest,
    dagger: &dyn DaggerClient,
) -> Result<PublishResult, BuildError> {
    if request.remote_refs.is_empty() {
        return Err(BuildError::Validation {
            reason: "remote_refs cannot be empty".to_owned(),
        });
    }

    let secret = dagger.set_secret("registry_password", request.password.expose())?;
    let mut container = dagger.container_from(&request.local_image)?;
    container =
        container.with_registry_auth(&request.registry_host, &request.username, &*secret)?;

    let mut published = Vec::with_capacity(request.remote_refs.len());
    for remote_ref in &request.remote_refs {
        let published_ref =
            container
                .publish(remote_ref)
                .map_err(|source| BuildError::Publish {
                    remote_ref: remote_ref.clone(),
                    source: eyre::eyre!("{source}"),
                })?;
        published.push(PublishedReference {
            remote_ref: remote_ref.clone(),
            published_ref,
        });
    }

    Ok(PublishResult { published })
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::testing::MockDaggerClient;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn publish_reference_count(refs in proptest::collection::vec("[a-z0-9./_-]+:[a-z0-9._-]+", 1..16)) {
            let mock = MockDaggerClient::default();
            let request = PublishRequest {
                local_image: "tokeirad:latest".to_owned(),
                remote_refs: refs.clone(),
                registry_host: "example.invalid".to_owned(),
                username: "AWS".to_owned(),
                password: RegistryPassword::new("secret"),
            };

            let result = publish_image(&request, &mock).expect("publish");

            prop_assert_eq!(result.published.len(), refs.len());
            for (actual, expected) in result.published.iter().zip(refs.iter()) {
                prop_assert_eq!(&actual.remote_ref, expected);
            }
        }
    }

    #[test]
    fn publish_rejects_empty_remote_refs() {
        let mock = MockDaggerClient::default();
        let request = PublishRequest {
            local_image: "tokeirad:latest".to_owned(),
            remote_refs: Vec::new(),
            registry_host: "example.invalid".to_owned(),
            username: "AWS".to_owned(),
            password: RegistryPassword::new("secret"),
        };

        let err = publish_image(&request, &mock).expect_err("empty refs must fail");

        assert!(matches!(err, BuildError::Validation { .. }));
    }

    #[test]
    fn registry_password_debug_is_redacted() {
        let password = RegistryPassword::new("very-secret");

        assert_eq!(format!("{password:?}"), "RegistryPassword(***)");
    }
}
