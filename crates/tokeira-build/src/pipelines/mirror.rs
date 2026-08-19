use dagger_sdk::Client;

use crate::{BuildError, pipelines::publish::RegistryPassword};

#[derive(Debug, Clone)]
pub struct MirrorRequest {
    pub source_ref: String,
    pub remote_ref: String,
    pub registry_host: String,
    pub username: String,
    pub password: RegistryPassword,
}

#[derive(Debug, Clone)]
pub struct MirroredReference {
    pub source_ref: String,
    pub remote_ref: String,
    pub published_ref: String,
}

pub async fn mirror_image(
    request: &MirrorRequest,
    client: &Client,
) -> Result<MirroredReference, BuildError> {
    let query = client.query();
    let secret = query.set_secret("registry_password", request.password.expose());
    let published_ref = query
        .container()
        .from(&request.source_ref)
        .with_registry_auth(&request.registry_host, secret, &request.username)
        .publish(&request.remote_ref)
        .await
        .map_err(|source| BuildError::Mirror {
            source_ref: request.source_ref.clone(),
            remote_ref: request.remote_ref.clone(),
            source: eyre::eyre!("{source:#}"),
        })?;

    Ok(MirroredReference {
        source_ref: request.source_ref.clone(),
        remote_ref: request.remote_ref.clone(),
        published_ref,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::canned_client;

    #[tokio::test]
    async fn mirror_issues_an_authenticated_publish() {
        let (client, wire) = canned_client().await;
        let request = request();

        let result = mirror_image(&request, &client).await.expect("mirror");

        assert_eq!(result.source_ref, request.source_ref);
        assert_eq!(result.remote_ref, request.remote_ref);
        let publish = wire
            .requests()
            .into_iter()
            .find(|query| query.contains("publish"))
            .expect("a publish execution");
        for fragment in [
            "docker.io/library/busybox:latest",
            "withRegistryAuth",
            "example.invalid",
            "AWS",
            "example.invalid/tokeira/busybox:latest",
        ] {
            assert!(
                publish.contains(fragment),
                "publish chain missing `{fragment}`:\n{publish}"
            );
        }
    }

    #[tokio::test]
    async fn mirror_maps_publish_error() {
        let (client, wire) = canned_client().await;
        wire.fail_next("registry said no");
        let request = request();

        let err = mirror_image(&request, &client)
            .await
            .expect_err("publish error");

        match err {
            BuildError::Mirror {
                source_ref,
                remote_ref,
                ..
            } => {
                assert_eq!(source_ref, request.source_ref);
                assert_eq!(remote_ref, request.remote_ref);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    fn request() -> MirrorRequest {
        MirrorRequest {
            source_ref: "docker.io/library/busybox:latest".to_owned(),
            remote_ref: "example.invalid/tokeira/busybox:latest".to_owned(),
            registry_host: "example.invalid".to_owned(),
            username: "AWS".to_owned(),
            password: RegistryPassword::new("secret"),
        }
    }
}
