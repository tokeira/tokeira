use tokeira_aws::resources::ecr_repository::EcrRepository;
use tokeira_deploy_engine::ImageContext;
use tokeira_iac::{IacError, Module, ModuleContext, Resource};

use crate::config::EcsConfig;

#[derive(Debug, Clone)]
pub struct ImagesModule {
    config: EcsConfig,
}

impl ImagesModule {
    pub(crate) fn new(config: EcsConfig) -> Self {
        Self { config }
    }
}

impl Module for ImagesModule {
    fn name(&self) -> &str {
        "images"
    }

    fn dependencies(&self) -> Vec<&str> {
        Vec::new()
    }

    fn resources(&self, _ctx: &ModuleContext) -> Result<Vec<Box<dyn Resource>>, IacError> {
        let mut image_ctx = ImageContext::default();
        image_ctx.set_extension(self.config.clone());
        let images =
            crate::images::all(&image_ctx).map_err(|err| IacError::Other(anyhow::anyhow!(err)))?;
        let mut resources = Vec::with_capacity(images.len());
        for image in images {
            let desired = image
                .desired_ref(&image_ctx)
                .map_err(|err| IacError::Other(anyhow::anyhow!(err)))?;
            let resource = EcrRepository::new(desired.repository, "images".into())
                .map_err(|err| IacError::Other(anyhow::anyhow!(err)))?;
            resources.push(Box::new(resource) as Box<dyn Resource>);
        }
        Ok(resources)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_iac::InfraState;

    #[test]
    fn images_module_contains_one_repository_per_image() {
        let config = EcsConfig::default();
        let module = ImagesModule::new(config.clone());
        let state = InfraState::default();
        let extensions = std::collections::HashMap::new();
        let ctx = ModuleContext::new(&state, &extensions);
        let resources = module.resources(&ctx).expect("resources");

        let mut image_ctx = ImageContext::default();
        image_ctx.set_extension(config);
        let images = crate::images::all(&image_ctx).expect("images");

        assert_eq!(resources.len(), images.len());
    }
}
