use crate::{BuildError, DaggerClient, pipelines::publish::RegistryPassword};

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

pub fn mirror_image(
    request: &MirrorRequest,
    dagger: &dyn DaggerClient,
) -> Result<MirroredReference, BuildError> {
    let secret = dagger.set_secret("registry_password", request.password.expose())?;
    let mut container = dagger
        .container_from(&request.source_ref)
        .map_err(|source| mirror_error(request, source))?;
    container = container
        .with_registry_auth(&request.registry_host, &request.username, &*secret)
        .map_err(|source| mirror_error(request, source))?;
    let published_ref = container
        .publish(&request.remote_ref)
        .map_err(|source| mirror_error(request, source))?;

    Ok(MirroredReference {
        source_ref: request.source_ref.clone(),
        remote_ref: request.remote_ref.clone(),
        published_ref,
    })
}

fn mirror_error(request: &MirrorRequest, source: BuildError) -> BuildError {
    BuildError::Mirror {
        source_ref: request.source_ref.clone(),
        remote_ref: request.remote_ref.clone(),
        source: eyre::eyre!("{source}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{MockCall, MockDaggerClient};

    #[test]
    fn mirror_records_expected_sequence() {
        let mock = MockDaggerClient::default();
        let request = request();

        let result = mirror_image(&request, &mock).expect("mirror");

        assert_eq!(result.source_ref, request.source_ref);
        assert_eq!(result.remote_ref, request.remote_ref);
        assert_eq!(
            mock.calls(),
            vec![
                MockCall::SetSecret("registry_password".to_owned()),
                MockCall::ContainerFrom("docker.io/library/busybox:latest".to_owned()),
                MockCall::WithRegistryAuth {
                    registry: "example.invalid".to_owned(),
                    username: "AWS".to_owned(),
                },
                MockCall::Publish("example.invalid/tokeira/busybox:latest".to_owned()),
            ]
        );
    }

    #[test]
    fn mirror_maps_publish_error() {
        let mock = MockDaggerClient::default().with_publish_error();
        let request = request();

        let err = mirror_image(&request, &mock).expect_err("publish error");

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
