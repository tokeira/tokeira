//! Deploy-engine service and image adapters for compose.

use tokeira_compose::ComposeService;
use tokeira_deploy_engine as deploy_engine;

use crate::compose::module_for_service;

/// Wraps a [`ComposeService`] as a deploy-engine [`Service`](deploy_engine::Service).
#[derive(Debug)]
pub struct ComposeWorkload {
    pub service: ComposeService,
}

impl deploy_engine::Service for ComposeWorkload {
    fn name(&self) -> &str {
        &self.service.name
    }

    fn module(&self) -> &str {
        module_for_service(&self.service.name)
    }

    fn dependencies(&self) -> &[&str] {
        match self.service.name.as_str() {
            "grafana" => &["mimir", "loki"],
            "alloy" => &["tokeirad", "mimir", "loki"],
            _ => &[],
        }
    }

    fn manifests(
        &self,
        _ctx: &deploy_engine::ServiceContext,
    ) -> Result<Vec<serde_json::Value>, deploy_engine::DeployError> {
        Ok(vec![self.service.to_manifest()])
    }
}

/// Wraps a compose service as a deploy-engine [`Image`](deploy_engine::Image).
#[derive(Debug)]
pub struct ComposeImage {
    pub name: String,
    pub image: String,
}

impl deploy_engine::Image for ComposeImage {
    fn name(&self) -> &str {
        &self.name
    }

    fn source_type(&self) -> deploy_engine::ImageSourceType {
        deploy_engine::ImageSourceType::Registry
    }

    fn desired_ref(
        &self,
        _ctx: &deploy_engine::ImageContext,
    ) -> Result<String, deploy_engine::DeployError> {
        Ok(self.image.clone())
    }
}
