use std::fmt;

use dagger_sdk::Client;

use crate::BuildError;

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

pub async fn publish_image(
    request: &PublishRequest,
    client: &Client,
) -> Result<PublishResult, BuildError> {
    if request.remote_refs.is_empty() {
        return Err(BuildError::Validation {
            reason: "remote_refs cannot be empty".to_owned(),
        });
    }

    let query = client.query();
    // The password rides a Dagger secret: it never appears in the query
    // graph or in engine logs, only the secret's handle does.
    let secret = query.set_secret("registry_password", request.password.expose());
    let container = query
        .container()
        .from(&request.local_image)
        .with_registry_auth(&request.registry_host, secret, &request.username);

    let mut published = Vec::with_capacity(request.remote_refs.len());
    for remote_ref in &request.remote_refs {
        let published_ref =
            container
                .publish(remote_ref)
                .await
                .map_err(|source| BuildError::Publish {
                    remote_ref: remote_ref.clone(),
                    source: eyre::eyre!("{source:#}"),
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
    use crate::testing::canned_client;

    fn request_with_refs(refs: Vec<String>) -> PublishRequest {
        PublishRequest {
            local_image: "tokeirad:latest".to_owned(),
            remote_refs: refs,
            registry_host: "example.invalid".to_owned(),
            username: "AWS".to_owned(),
            password: RegistryPassword::new("secret"),
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(20))]

        #[test]
        fn publish_reference_count(refs in proptest::collection::vec("[a-z0-9./_-]+:[a-z0-9._-]+", 1..8)) {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime");
            runtime.block_on(async {
                let (client, wire) = canned_client().await;
                let result = publish_image(&request_with_refs(refs.clone()), &client)
                    .await
                    .expect("publish");

                prop_assert_eq!(result.published.len(), refs.len());
                for (actual, expected) in result.published.iter().zip(refs.iter()) {
                    prop_assert_eq!(&actual.remote_ref, expected);
                }
                let publishes = wire
                    .requests()
                    .into_iter()
                    .filter(|query| query.contains("publish"))
                    .count();
                prop_assert_eq!(publishes, refs.len());
                Ok(())
            })?;
        }
    }

    #[tokio::test]
    async fn publish_rejects_empty_remote_refs() {
        let (client, wire) = canned_client().await;

        let err = publish_image(&request_with_refs(Vec::new()), &client)
            .await
            .expect_err("empty refs must fail");

        assert!(matches!(err, BuildError::Validation { .. }));
        assert!(wire.requests().is_empty(), "refused before any engine call");
    }

    #[tokio::test]
    async fn publish_rides_a_secret_id_never_the_plaintext() {
        let (client, wire) = canned_client().await;
        let mut request = request_with_refs(vec!["example.invalid/t:1".into()]);
        request.password = RegistryPassword::new("plaintext-shibboleth");

        publish_image(&request, &client).await.expect("publish");

        // The plaintext exists only in the setSecret registration; the
        // authenticated publish chain references the secret by id.
        let publish = wire
            .requests()
            .into_iter()
            .find(|query| query.contains("withRegistryAuth"))
            .expect("an authenticated publish execution");
        assert!(publish.contains("example.invalid"));
        assert!(publish.contains("AWS"));
        assert!(
            !publish.contains("plaintext-shibboleth"),
            "the publish chain must carry the secret's id, not its plaintext:\n{publish}"
        );
        assert!(
            wire.transcript().contains("plaintext-shibboleth"),
            "the secret registration itself carries the plaintext to the engine"
        );
    }

    #[test]
    fn registry_password_debug_is_redacted() {
        let password = RegistryPassword::new("very-secret");

        assert_eq!(format!("{password:?}"), "RegistryPassword(***)");
    }
}
