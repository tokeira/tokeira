//! The Compose provider's execution seam and registration ingredient.
//!
//! [`ComposeExecution::probe`] answers whether the local Docker daemon can
//! be reached for this deployment — the degradable answer stated as data:
//! the framework's plan blocks on it and apply/destroy refuse. The answer
//! is point-in-time: a daemon dying after a passing probe surfaces through
//! the operation's own error path, where
//! [`ComposeError::DockerNotAvailable`] already maps to the same typed
//! platform issue (`From<ComposeError> for IacError`).
//!
//! [`ComposeInfraConstructor`] is the selection's infra-phase constructor,
//! run by the deployment's registration seam: it registers what Compose
//! resources read from the provision context — the Docker-backed
//! [`ComposePlatform`] (the compose-file ledger lives under the
//! framework-owned `state/` directory) and the compose-service recovery
//! hook. Its failures are errors — real ones, and equally a daemon dying
//! after a passing probe: the error root stays [`ComposeError`], so the
//! unreachable class renders as the same platform issue rather than a
//! blended message.

use tokeira_platform::declaration::{DeploymentRef, InfraConstructor, ProviderExecution};

use crate::{ComposeError, ComposePlatform, docker_unreachable_issue, service_from_manifest};

/// Compose's execution seam implementation.
#[derive(Debug)]
pub struct ComposeExecution;

#[async_trait::async_trait]
impl ProviderExecution for ComposeExecution {
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

/// Compose's infra-phase extension constructor. The provider's own
/// namespace carries no attribute block — everything it needs is the
/// deployment's coordinates.
#[derive(Debug)]
pub struct ComposeInfraConstructor;

#[async_trait::async_trait]
impl InfraConstructor for ComposeInfraConstructor {
    async fn construct(
        &self,
        deployment: &DeploymentRef,
        _attributes: Option<&serde_json::Value>,
        ctx: &mut tokeira_iac::ProvisionContext,
    ) -> anyhow::Result<()> {
        // Recovered resources let refresh/destroy reconstruct a live service
        // from recorded state without the definition in hand.
        ctx.set_extension(tokeira_iac::ResourceRecovery::new(|state| {
            (state.resource_type.0 == "compose_service")
                .then(|| service_from_manifest(state.properties.clone()).ok())
                .flatten()
                .map(|service| Box::new(service) as Box<dyn tokeira_iac::Resource>)
        }));

        let ledger = deployment.dir.join("state/compose-services.yaml");
        let platform = ComposePlatform::connect(ledger, &deployment.name)?;
        ctx.set_extension(platform);
        Ok(())
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
