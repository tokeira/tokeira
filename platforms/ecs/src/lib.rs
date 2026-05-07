//! ECS deployment platform scaffold.

pub mod config;
pub mod gates;
pub mod images;
pub mod modules;

use std::path::Path;

use async_trait::async_trait;
use tokeira_config::TokeiraConfig;
use tokeira_deploy_engine as deploy_engine;
use tokeira_iac as iac;
use tokeira_iac::Module;
use tokeira_orchestrator::{
    Ops, PlatformConfig, PortMapping, Result, ServiceReplicas, StorageKind,
};
use tokeira_state::{LocalBackend, StateBackend};

pub use config::EcsConfig;

#[derive(Debug, Clone, Default)]
pub struct EcsDeployment;

impl EcsDeployment {
    pub fn default_config_toml() -> String {
        annotate_image_lifecycle_fields(
            tokeira_config::write_config_toml(&EcsConfig::default()).expect("serializes"),
        )
    }
}

fn annotate_image_lifecycle_fields(toml: String) -> String {
    toml.replace(
        "image = \"tokeirad:latest\"",
        "# populated by `tkr image push`\nimage = \"tokeirad:latest\"",
    )
    .replace(
        "mimir_image = ",
        "# populated by `tkr image mirror`\nmimir_image = ",
    )
    .replace(
        "grafana_image = ",
        "# populated by `tkr image mirror`\ngrafana_image = ",
    )
    .replace(
        "loki_image = ",
        "# populated by `tkr image mirror`\nloki_image = ",
    )
    .replace(
        "alloy_image = ",
        "# populated by `tkr image mirror`\nalloy_image = ",
    )
    .replace(
        "aws_cli_image = ",
        "# populated by `tkr image mirror`\naws_cli_image = ",
    )
    .replace(
        "busybox_image = ",
        "# populated by `tkr image mirror`\nbusybox_image = ",
    )
}

impl PlatformConfig for EcsDeployment {
    fn prototypical_config(_storage: StorageKind) -> String {
        Self::default_config_toml()
    }

    fn prototypical_server_config(storage: StorageKind) -> String {
        let mut config = TokeiraConfig::default();
        if storage == StorageKind::Dsql {
            config.infrastructure.dsql.endpoint = Some("replace-with-dsql-endpoint".to_string());
        }
        config.to_toml().expect("server config serializes")
    }
}

#[async_trait]
impl tokeira_orchestrator::Deployment for EcsDeployment {
    type Config = EcsConfig;

    fn remote_state_module(
        &self,
        _config: &Self::Config,
        _deployment_dir: &Path,
    ) -> Box<dyn iac::Module> {
        Box::new(EmptyModule)
    }

    fn infra_modules(
        &self,
        config: &Self::Config,
        selection: &iac::ModuleSelection,
    ) -> Vec<Box<dyn iac::Module>> {
        let mut modules: Vec<Box<dyn iac::Module>> = Vec::new();
        let images = modules::ImagesModule::new(config.clone());
        if selection.includes(images.name()) {
            modules.push(Box::new(images));
        }
        modules
    }

    fn services(&self, _config: &Self::Config) -> Vec<Box<dyn deploy_engine::Service>> {
        Vec::new()
    }

    fn images(&self, _config: &Self::Config) -> Vec<Box<dyn deploy_engine::Image>> {
        crate::images::construct()
    }

    fn required_namespaces(&self, _config: &Self::Config) -> Vec<String> {
        vec!["default".into()]
    }

    async fn register_infra_extensions(
        &self,
        _config: &Self::Config,
        _ctx: &mut iac::ProvisionContext,
    ) -> Result<()> {
        Ok(())
    }

    async fn register_deploy_extensions(
        &self,
        _config: &Self::Config,
        _ctx: &mut deploy_engine::ServiceContext,
    ) -> Result<()> {
        Ok(())
    }

    async fn register_image_extensions(
        &self,
        config: &Self::Config,
        ctx: &mut deploy_engine::ImageContext,
    ) -> Result<()> {
        ctx.set_extension(config.clone());
        Ok(())
    }

    fn create_infra_store(
        &self,
        _config: &Self::Config,
        deployment_dir: &Path,
    ) -> Box<dyn StateBackend> {
        Box::new(LocalBackend::new(deployment_dir.join("state/infra")))
    }

    fn create_deploy_store(
        &self,
        _config: &Self::Config,
        deployment_dir: &Path,
    ) -> Box<dyn StateBackend> {
        Box::new(LocalBackend::new(deployment_dir.join("state/deploy")))
    }

    fn hydrate_config(&self, config: &Self::Config, _state: &iac::InfraState) -> Self::Config {
        config.clone()
    }

    fn collect_writeback(
        &self,
        _config: &Self::Config,
        _state: &iac::InfraState,
    ) -> Vec<(String, String)> {
        Vec::new()
    }
}

#[async_trait]
impl Ops for EcsDeployment {
    type Config = EcsConfig;

    fn valid_services(&self) -> &[&str] {
        static VALID: [&str; 7] = [
            "edge-api",
            "edge-poll",
            "runtime",
            "projection",
            "controller",
            "autoscaler",
            "admin",
        ];
        &VALID
    }

    fn desired_replicas(&self, _config: &Self::Config) -> Vec<ServiceReplicas> {
        Vec::new()
    }

    async fn scale_up(&self, _service: &str, _replicas: u32, _config: &Self::Config) -> Result<()> {
        Err(anyhow::anyhow!("ECS scale operations are not implemented yet").into())
    }

    async fn scale_down(
        &self,
        _service: &str,
        _replicas: u32,
        _config: &Self::Config,
    ) -> Result<()> {
        Err(anyhow::anyhow!("ECS scale operations are not implemented yet").into())
    }

    async fn logs(&self, _service: &str, _config: &Self::Config) -> Result<Vec<String>> {
        Err(anyhow::anyhow!("ECS logs are not implemented yet").into())
    }

    async fn port_mappings(
        &self,
        _service: &str,
        _config: &Self::Config,
    ) -> Result<Vec<PortMapping>> {
        Err(anyhow::anyhow!("ECS port mappings are not implemented yet").into())
    }
}

#[derive(Debug)]
struct EmptyModule;

impl iac::Module for EmptyModule {
    fn name(&self) -> &str {
        "remote-state"
    }

    fn dependencies(&self) -> &[&str] {
        &[]
    }

    fn resources(
        &self,
        _ctx: &iac::ModuleContext,
    ) -> std::result::Result<Vec<Box<dyn iac::Resource>>, iac::IacError> {
        Ok(Vec::new())
    }
}
