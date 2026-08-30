//! EKS reachability and framework integration seams.

use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use tokeira_k8s::{K8sError, KubePlatform};
use tokeira_platform::declaration::{DeploymentRef, PlatformExecution, PlatformIntegration};

use crate::service::EksServicePlatform;

/// EKS substrate reachability.
#[derive(Debug)]
pub struct EksExecution;

#[async_trait::async_trait]
impl PlatformExecution for EksExecution {
    /// The standard AWS extension owns SDK configuration and operation-local
    /// AWS errors. A cluster-wide probe would be false for a valid fresh
    /// deployment before the EKS control plane exists, so Kubernetes
    /// reachability is discovered when its resources are described/applied.
    async fn probe(
        &self,
        _deployment: &DeploymentRef,
    ) -> anyhow::Result<Option<tokeira_iac::PlatformIssue>> {
        Ok(None)
    }
}

/// Registers one live Kubernetes handle per deployment.
#[derive(Debug, Default)]
pub struct EksIntegration {
    platforms: Arc<RwLock<BTreeMap<String, KubePlatform>>>,
}

impl EksIntegration {
    async fn reachable_platform() -> Result<Option<KubePlatform>, K8sError> {
        let platform = match KubePlatform::connect().await {
            Ok(platform) => platform,
            Err(K8sError::Unreachable(_)) => return Ok(None),
            Err(error) => return Err(error),
        };
        match platform.ensure_reachable().await {
            Ok(()) => Ok(Some(platform)),
            Err(K8sError::Unreachable(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn remember(&self, deployment: &str, platform: KubePlatform) -> anyhow::Result<()> {
        self.platforms
            .write()
            .map_err(|_| anyhow::anyhow!("EKS platform registry lock is poisoned"))?
            .insert(deployment.to_string(), platform);
        Ok(())
    }
}

#[async_trait::async_trait]
impl PlatformIntegration for EksIntegration {
    async fn register_infra_extensions(
        &self,
        deployment: &DeploymentRef,
        ctx: &mut tokeira_iac::ProvisionContext,
    ) -> anyhow::Result<()> {
        if let Some(platform) = Self::reachable_platform().await? {
            ctx.set_extension(platform.clone());
            self.remember(&deployment.name, platform)?;
        }
        Ok(())
    }

    async fn register_deploy_extensions(
        &self,
        deployment: &DeploymentRef,
        ctx: &mut tokeira_deploy_engine::ServiceContext,
    ) -> anyhow::Result<()> {
        if let Some(platform) = Self::reachable_platform().await? {
            ctx.set_extension(platform.clone());
            self.remember(&deployment.name, platform)?;
        }
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
        Ok(Box::new(EksServicePlatform::new(
            deployment.name.clone(),
            Arc::clone(&self.platforms),
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn deployment() -> DeploymentRef {
        DeploymentRef {
            name: "demo".into(),
            dir: PathBuf::from("demo"),
        }
    }

    #[tokio::test]
    async fn fresh_deployment_probe_defers_to_operation_local_provider_checks() {
        let issue = EksExecution
            .probe(&deployment())
            .await
            .expect("probe is provider-pure");

        assert!(issue.is_none());
    }

    #[tokio::test]
    async fn service_platform_is_deployment_scoped_and_fails_without_a_registered_handle() {
        let integration = EksIntegration::default();
        let platform = integration
            .service_platform(&deployment())
            .expect("standard integration supplies the service applier");

        let error = platform
            .apply_manifests(&[])
            .await
            .expect_err("an unregistered deployment must fail closed");

        assert!(error.to_string().contains("deployment `demo`"));
    }
}
