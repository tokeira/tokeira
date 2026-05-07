use tokeira_compose::ComposeError;

use crate::ComposeConfig;

#[async_trait::async_trait]
pub trait DockerImageInspector: Send + Sync {
    async fn image_exists(&self, image: &str) -> Result<bool, ComposeError>;
}

#[derive(Debug, Clone)]
pub struct BollardInspector(pub bollard::Docker);

#[async_trait::async_trait]
impl DockerImageInspector for BollardInspector {
    async fn image_exists(&self, image: &str) -> Result<bool, ComposeError> {
        match self.0.inspect_image(image).await {
            Ok(_) => Ok(true),
            Err(bollard::errors::Error::DockerResponseServerError {
                status_code: 404, ..
            }) => Ok(false),
            Err(error) => Err(ComposeError::DockerIo(error)),
        }
    }
}

pub async fn validate_local_build<I>(cfg: &ComposeConfig, inspector: &I) -> Result<(), ComposeError>
where
    I: DockerImageInspector + ?Sized,
{
    if cfg.tokeirad.image != "tokeirad:latest" {
        return Ok(());
    }
    if inspector.image_exists("tokeirad:latest").await? {
        Ok(())
    } else {
        Err(ComposeError::LocalBuildMissing {
            image: "tokeirad:latest".into(),
            remediation: "run `tkr image build` first".into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    #[tokio::test]
    async fn missing_local_build_returns_remediation() {
        let inspector = MockDockerImageInspector::new(false);

        let err = validate_local_build(&ComposeConfig::default(), &inspector)
            .await
            .expect_err("missing local image");

        assert!(err.to_string().contains("tkr image build"));
        assert_eq!(inspector.calls(), vec!["tokeirad:latest"]);
    }

    #[tokio::test]
    async fn existing_local_build_passes() {
        let inspector = MockDockerImageInspector::new(true);

        validate_local_build(&ComposeConfig::default(), &inspector)
            .await
            .expect("local image exists");

        assert_eq!(inspector.calls(), vec!["tokeirad:latest"]);
    }

    #[tokio::test]
    async fn remote_image_skips_local_inspection() {
        let inspector = MockDockerImageInspector::new(false);
        let mut config = ComposeConfig::default();
        config.tokeirad.image = "example.invalid/tokeirad:custom".into();

        validate_local_build(&config, &inspector)
            .await
            .expect("remote image should not require local build");

        assert!(inspector.calls().is_empty());
    }

    #[derive(Debug, Clone)]
    struct MockDockerImageInspector {
        exists: bool,
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl MockDockerImageInspector {
        fn new(exists: bool) -> Self {
            Self {
                exists,
                calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().expect("calls lock").clone()
        }
    }

    #[async_trait::async_trait]
    impl DockerImageInspector for MockDockerImageInspector {
        async fn image_exists(&self, image: &str) -> Result<bool, ComposeError> {
            self.calls
                .lock()
                .expect("calls lock")
                .push(image.to_owned());
            Ok(self.exists)
        }
    }
}
