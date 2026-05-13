//! Compose platform — Docker Compose stack with observability services.
//!
//! Structure follows the deploy-eks `project` pattern:
//! - `config` — platform configuration model
//! - `compose` — service descriptor construction
//! - `modules` — IaC modules (remote-state, runtime, observability)
//! - `services` — deploy-engine service and image adapters

pub mod compose;
pub mod config;
pub mod gates;
pub mod images;
pub mod modules;
pub mod observability_config;
pub mod services;

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tokeira_compose::ComposePlatform;
use tokeira_config::TokeiraConfig;
use tokeira_deploy_engine as deploy_engine;
use tokeira_iac as iac;
use tokeira_iac::Module;
use tokeira_orchestrator::{
    Ops, PlatformConfig, PortMapping, Result, ServiceReplicas, StorageKind,
};
use tokeira_state::{LocalBackend, StateBackend};

pub use config::ComposeConfig;

use compose::compose_services;
use modules::{ComposeModule, LocalStateModule};
use services::ComposeWorkload;

#[derive(Clone, Default)]
pub struct ComposeDeployment;

impl ComposeDeployment {
    pub fn compose_platform(compose_file: &Path, project_name: &str) -> Result<ComposePlatform> {
        ComposePlatform::connect(compose_file, project_name)
            .map_err(anyhow::Error::from)
            .map_err(Into::into)
    }

    pub fn default_config_toml() -> String {
        annotate_image_lifecycle_fields(
            tokeira_config::write_config_toml(&ComposeConfig::default()).expect("serializes"),
        )
    }

    pub async fn validate_for_deploy_apply(
        &self,
        config: &ComposeConfig,
        platform: &ComposePlatform,
    ) -> Result<()> {
        gates::validate_local_build(config, &gates::BollardInspector(platform.docker_client()))
            .await
            .map_err(anyhow::Error::from)
            .map_err(Into::into)
    }

    fn desired_service_replicas(config: &ComposeConfig) -> Vec<ServiceReplicas> {
        vec![
            ServiceReplicas {
                service: "mimir".into(),
                replicas: config.observability.mimir_replicas,
            },
            ServiceReplicas {
                service: "loki".into(),
                replicas: config.observability.loki_replicas,
            },
            ServiceReplicas {
                service: "tokeirad".into(),
                replicas: config.tokeirad.replicas,
            },
            ServiceReplicas {
                service: "grafana".into(),
                replicas: config.observability.grafana_replicas,
            },
            ServiceReplicas {
                service: "alloy".into(),
                replicas: config.observability.alloy_replicas,
            },
        ]
    }
}

fn annotate_image_lifecycle_fields(toml: String) -> String {
    toml.replace(
        "image = \"tokeirad:latest\"",
        "# run `tkr image build` before `tkr deploy apply`\nimage = \"tokeirad:latest\"",
    )
    .replace(
        "mimir_image = ",
        "# populated by `tkr image mirror` for ECS deployments\nmimir_image = ",
    )
    .replace(
        "grafana_image = ",
        "# populated by `tkr image mirror` for ECS deployments\ngrafana_image = ",
    )
    .replace(
        "loki_image = ",
        "# populated by `tkr image mirror` for ECS deployments\nloki_image = ",
    )
    .replace(
        "alloy_image = ",
        "# populated by `tkr image mirror` for ECS deployments\nalloy_image = ",
    )
    .replace(
        "aws_cli_image = ",
        "# populated by `tkr image mirror` for ECS deployments\naws_cli_image = ",
    )
    .replace(
        "busybox_image = ",
        "# populated by `tkr image mirror` for ECS deployments\nbusybox_image = ",
    )
}

impl PlatformConfig for ComposeDeployment {
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
impl tokeira_orchestrator::Deployment for ComposeDeployment {
    type Config = ComposeConfig;

    fn remote_state_module(
        &self,
        _config: &Self::Config,
        deployment_dir: &Path,
    ) -> Box<dyn iac::Module> {
        Box::new(LocalStateModule {
            state_dir: deployment_dir.join("state"),
        })
    }

    fn infra_modules(
        &self,
        config: &Self::Config,
        selection: &iac::ModuleSelection,
    ) -> Vec<Box<dyn iac::Module>> {
        let mut modules: Vec<Box<dyn iac::Module>> = Vec::new();
        let runtime = ComposeModule::runtime(config);
        let observability = ComposeModule::observability(config);
        if selection.includes(runtime.name()) {
            modules.push(Box::new(runtime));
        }
        if selection.includes(observability.name()) {
            modules.push(Box::new(observability));
        }
        modules
    }

    fn services(&self, config: &Self::Config) -> Vec<Box<dyn deploy_engine::Service>> {
        compose_services(config)
            .into_iter()
            .map(|service| Box::new(ComposeWorkload { service }) as Box<dyn deploy_engine::Service>)
            .collect()
    }

    fn images(&self, _config: &Self::Config) -> Vec<Box<dyn deploy_engine::Image>> {
        crate::images::construct()
    }

    fn required_namespaces(&self, _config: &Self::Config) -> Vec<String> {
        vec!["default".into()]
    }

    async fn register_infra_extensions(
        &self,
        config: &Self::Config,
        ctx: &mut iac::ProvisionContext,
    ) -> Result<()> {
        let compose_file = config.deployment_dir.join("docker-compose.yml");
        let platform = Self::compose_platform(&compose_file, &config.project_name)?;
        ctx.set_extension(platform);
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
impl Ops for ComposeDeployment {
    type Config = ComposeConfig;

    fn valid_services(&self) -> &[&str] {
        static VALID: [&str; 5] = ["mimir", "loki", "tokeirad", "grafana", "alloy"];
        &VALID
    }

    fn desired_replicas(&self, config: &Self::Config) -> Vec<ServiceReplicas> {
        Self::desired_service_replicas(config)
    }

    async fn scale_up(&self, _service: &str, _replicas: u32, _config: &Self::Config) -> Result<()> {
        Err(anyhow::anyhow!("use scale_up_with_dir for compose platform").into())
    }

    async fn scale_down(
        &self,
        _service: &str,
        _replicas: u32,
        _config: &Self::Config,
    ) -> Result<()> {
        Err(anyhow::anyhow!("use scale_down_with_dir for compose platform").into())
    }

    async fn logs(&self, _service: &str, _config: &Self::Config) -> Result<Vec<String>> {
        Err(anyhow::anyhow!("use logs_with_dir for compose platform").into())
    }

    async fn port_mappings(
        &self,
        _service: &str,
        _config: &Self::Config,
    ) -> Result<Vec<PortMapping>> {
        Err(anyhow::anyhow!("use port_mappings_with_dir for compose platform").into())
    }
}

/// Compose file is always at `<deployment_dir>/docker-compose.yml`.
fn compose_file_for(deployment_dir: &Path) -> PathBuf {
    deployment_dir.join("docker-compose.yml")
}

impl ComposeDeployment {
    pub async fn scale_up_with_dir(
        &self,
        service: &str,
        replicas: u32,
        config: &ComposeConfig,
        deployment_dir: &Path,
    ) -> Result<()> {
        let compose =
            Self::compose_platform(&compose_file_for(deployment_dir), &config.project_name)?;
        let desired = compose_services(config)
            .into_iter()
            .find(|candidate| candidate.name == service)
            .ok_or_else(|| {
                anyhow::anyhow!(invalid_service_message(self.valid_services(), service))
            })?;
        compose
            .scale_service(&desired, replicas)
            .await
            .map_err(anyhow::Error::from)?;
        Ok(())
    }

    pub async fn scale_down_with_dir(
        &self,
        service: &str,
        replicas: u32,
        config: &ComposeConfig,
        deployment_dir: &Path,
    ) -> Result<()> {
        let compose =
            Self::compose_platform(&compose_file_for(deployment_dir), &config.project_name)?;
        for replica in 0..replicas {
            compose
                .delete_service(&format!("{service}-{replica}"))
                .await
                .map_err(anyhow::Error::from)?;
        }
        Ok(())
    }

    pub async fn logs_with_dir(
        &self,
        service: &str,
        config: &ComposeConfig,
        deployment_dir: &Path,
    ) -> Result<Vec<String>> {
        if !self.valid_services().contains(&service) {
            return Err(
                anyhow::anyhow!(invalid_service_message(self.valid_services(), service)).into(),
            );
        }
        Self::compose_platform(&compose_file_for(deployment_dir), &config.project_name)?
            .logs(service)
            .await
            .map_err(anyhow::Error::from)
            .map_err(Into::into)
    }

    pub async fn port_mappings_with_dir(
        &self,
        service: &str,
        config: &ComposeConfig,
        deployment_dir: &Path,
    ) -> Result<Vec<PortMapping>> {
        if !self.valid_services().contains(&service) {
            return Err(
                anyhow::anyhow!(invalid_service_message(self.valid_services(), service)).into(),
            );
        }
        Self::compose_platform(&compose_file_for(deployment_dir), &config.project_name)?
            .port_mappings(service)
            .await
            .map(|mappings| {
                mappings
                    .into_iter()
                    .map(
                        |(host_addr, host_port, container_port, protocol)| PortMapping {
                            host_addr,
                            host_port,
                            container_port,
                            protocol,
                        },
                    )
                    .collect()
            })
            .map_err(anyhow::Error::from)
            .map_err(Into::into)
    }
}

fn invalid_service_message(valid: &[&str], actual: &str) -> String {
    format!(
        "unknown service '{actual}'. valid services: {}",
        valid.join(", ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use tokeira_iac::Module;
    use tokeira_orchestrator::Deployment;

    fn arb_invalid_service() -> impl Strategy<Value = String> {
        "[a-z][a-z0-9_-]{0,15}"
            .prop_filter("must not collide with a valid service", |candidate| {
                !["tokeirad", "mimir", "grafana", "loki", "alloy"].contains(&candidate.as_str())
            })
            .prop_map(|s| s.to_string())
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn p14_invalid_service_error_lists_valid_alternatives(service in arb_invalid_service()) {
            let message = invalid_service_message(ComposeDeployment.valid_services(), &service);
            prop_assert!(message.contains(&service));
            for valid in ComposeDeployment.valid_services() {
                prop_assert!(message.contains(valid));
            }
        }
    }

    #[test]
    fn infra_modules_use_logical_names() {
        let config = ComposeConfig::default();
        let modules = ComposeDeployment.infra_modules(&config, &iac::ModuleSelection::All);
        let names: Vec<String> = modules
            .iter()
            .map(|module| module.name().to_string())
            .collect();
        assert_eq!(names, vec!["runtime", "observability"]);
    }

    #[test]
    fn module_selection_filters_correctly() {
        let config = ComposeConfig::default();
        let modules = ComposeDeployment.infra_modules(
            &config,
            &iac::ModuleSelection::Only(vec!["observability".into()]),
        );
        assert_eq!(modules.len(), 1);
        assert_eq!(modules[0].name(), "observability");
    }

    #[test]
    fn observability_module_contains_expected_resources() {
        let config = ComposeConfig::default();
        let module = ComposeModule::observability(&config);
        let state = iac::InfraState::default();
        let extensions = std::collections::HashMap::new();
        let ctx = iac::ModuleContext::new(&state, &extensions);
        let resources = module.resources(&ctx).unwrap();
        let names: Vec<String> = resources
            .iter()
            .map(|r| r.resource_id().0.clone())
            .collect();
        assert_eq!(names[0], "compose/observability-config-files");
        assert!(names.iter().any(|n| n.contains("mimir")));
        assert!(names.iter().any(|n| n.contains("grafana")));
        assert!(names.iter().any(|n| n.contains("loki")));
        assert!(names.iter().any(|n| n.contains("alloy")));
        // All resources report the observability module
        for r in &resources {
            assert_eq!(r.module(), "observability");
        }
        for resource in resources.iter().skip(1) {
            let deps: Vec<String> = resource
                .dependencies()
                .into_iter()
                .map(|dep| dep.0)
                .collect();
            assert!(
                deps.iter()
                    .any(|dep| dep == "compose/observability-config-files")
            );
        }
    }

    #[test]
    fn runtime_module_contains_tokeirad() {
        let config = ComposeConfig::default();
        let module = ComposeModule::runtime(&config);
        let state = iac::InfraState::default();
        let extensions = std::collections::HashMap::new();
        let ctx = iac::ModuleContext::new(&state, &extensions);
        let resources = module.resources(&ctx).unwrap();
        assert_eq!(resources.len(), 1);
        assert!(resources[0].resource_id().0.contains("tokeirad"));
        assert_eq!(resources[0].module(), "runtime");
        let deps: Vec<String> = resources[0]
            .dependencies()
            .into_iter()
            .map(|dep| dep.0)
            .collect();
        assert!(
            !deps
                .iter()
                .any(|dep| dep == "compose/observability-config-files")
        );
    }

    #[tokio::test]
    async fn invalid_service_lists_valid_names() {
        let temp = tempfile::tempdir().unwrap();
        let config = ComposeConfig::default();
        let err = ComposeDeployment
            .logs_with_dir("not-a-service", &config, temp.path())
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("tokeirad"));
        assert!(err.contains("grafana"));
    }
}
