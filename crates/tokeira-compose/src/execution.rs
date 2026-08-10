//! The Compose provider's reachability seam.
//!
//! [`ComposeExecution::probe`] answers whether the local Docker daemon can
//! be reached for this deployment — the degradable answer stated as data:
//! the framework's plan blocks on it and apply/destroy refuse. The answer
//! is point-in-time: a daemon dying after a passing probe surfaces through
//! the operation's own error path, where
//! [`ComposeError::DockerNotAvailable`] already maps to the same typed
//! platform issue (`From<ComposeError> for IacError`).

use tokeira_platform::declaration::{DeploymentRef, PlatformExecution, PlatformIntegration};

use crate::{ComposeError, ComposePlatform, docker_unreachable_issue};

/// Compose's execution seam implementation.
#[derive(Debug)]
pub struct ComposeExecution;

#[async_trait::async_trait]
impl PlatformExecution for ComposeExecution {
    async fn probe(
        &self,
        deployment: &DeploymentRef,
    ) -> anyhow::Result<Option<tokeira_iac::PlatformIssue>> {
        // The ledger-free ops handle suffices: reachability is a question
        // about the daemon, not about any operation artifact.
        let platform = match ComposePlatform::ops(&deployment.name) {
            Ok(platform) => platform,
            Err(error) => return degrade(error),
        };
        match platform.ensure_reachable().await {
            Ok(()) => Ok(None),
            Err(error) => degrade(error),
        }
    }
}

/// Compose's platform implementation as integrated by a platform definition.
///
/// Standard AWS clients are registered by the AWS-first framework before this
/// seam. Compose needs no additional context extension: its desired resources
/// and services are complete. Runtime service application is nevertheless
/// Compose-owned, so this constructs the deployment-scoped [`ComposePlatform`]
/// that reconciles their manifests against Docker.
#[derive(Debug)]
pub struct ComposeIntegration;

#[async_trait::async_trait]
impl PlatformIntegration for ComposeIntegration {
    async fn register_infra_extensions(
        &self,
        _deployment: &DeploymentRef,
        _ctx: &mut tokeira_iac::ProvisionContext,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn register_deploy_extensions(
        &self,
        _deployment: &DeploymentRef,
        _ctx: &mut tokeira_deploy_engine::ServiceContext,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn register_image_extensions(
        &self,
        _deployment: &DeploymentRef,
        _ctx: &mut tokeira_deploy_engine::ImageContext,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn service_platform(
        &self,
        deployment: &DeploymentRef,
    ) -> anyhow::Result<Box<dyn tokeira_deploy_engine::Platform>> {
        let ledger = deployment.dir.join("state/compose-services.yaml");
        Ok(Box::new(ComposePlatform::connect(
            ledger,
            &deployment.dir,
            &deployment.name,
        )?))
    }
}

/// Map a connection failure to the operator-facing issue when the error
/// class is the unreachable daemon; anything else is a real failure.
fn degrade(error: ComposeError) -> anyhow::Result<Option<tokeira_iac::PlatformIssue>> {
    match error {
        ComposeError::DockerNotAvailable {
            socket_path,
            evidence,
        } => Ok(Some(docker_unreachable_issue(&socket_path, &evidence))),
        other => Err(anyhow::Error::from(other)),
    }
}
