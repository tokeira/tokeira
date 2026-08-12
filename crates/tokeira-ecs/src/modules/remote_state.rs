//! Legacy ECS module adapter for the shared AWS-backed foundation resource.

use tokeira_aws::resources::remote_state_bucket::RemoteStateBucket;
use tokeira_iac::{IacError, Module, ModuleContext, Resource};

use crate::{config::EcsConfig, state_bucket_name, state_key_prefix};

#[derive(Debug, Clone)]
pub struct RemoteStateModule {
    config: EcsConfig,
}

impl RemoteStateModule {
    pub fn new(config: EcsConfig) -> Self {
        Self { config }
    }
}

impl Module for RemoteStateModule {
    fn name(&self) -> &str {
        "remote-state"
    }

    fn dependencies(&self) -> Vec<&str> {
        Vec::new()
    }

    fn resources(&self, _ctx: &ModuleContext) -> Result<Vec<Box<dyn Resource>>, IacError> {
        let bucket = RemoteStateBucket::new(
            state_bucket_name(&self.config),
            self.config.region.clone(),
            Some(state_key_prefix(&self.config)),
            "remote-state",
        );
        Ok(vec![Box::new(bucket)])
    }
}
