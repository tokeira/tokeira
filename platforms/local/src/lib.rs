use std::path::{Path, PathBuf};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokeira_config::TokeiraConfig;
use tokeira_deploy_engine as deploy_engine;
use tokeira_iac as iac;
use tokeira_orchestrator::{
    Ops, PlatformConfig, PortMapping, Result, ServiceReplicas, StorageKind,
};
use tokeira_state::{CasStore, DeploymentStore, LocalBackend};

/// Minimal config for bare-process local execution.
/// No compose file, no observability images, no replicas.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalConfig {
    pub(crate) project_name: String,
}

impl Default for LocalConfig {
    fn default() -> Self {
        Self {
            project_name: "tokeira".into(),
        }
    }
}

#[derive(Clone, Default, Debug)]
pub struct LocalDeployment;

impl PlatformConfig for LocalDeployment {
    fn prototypical_config(_storage: StorageKind) -> String {
        tokeira_config::write_config_toml(&LocalConfig::default()).expect("serializes")
    }

    fn prototypical_server_config(storage: StorageKind) -> String {
        let mut config = TokeiraConfig::default();
        if storage == StorageKind::Dsql {
            config.infrastructure.dsql.endpoint = Some("replace-with-dsql-endpoint".to_string());
        }
        config.to_toml().expect("server config serializes")
    }
}

#[derive(Debug)]
struct LocalStateModule {
    state_dir: PathBuf,
}

impl iac::Module for LocalStateModule {
    fn name(&self) -> &str {
        "remote-state"
    }

    fn dependencies(&self) -> Vec<&str> {
        Vec::new()
    }

    fn resources(
        &self,
        _ctx: &iac::ModuleContext<'_>,
    ) -> std::result::Result<Vec<Box<dyn iac::Resource>>, iac::IacError> {
        Ok(vec![Box::new(LocalStateResource {
            state_dir: self.state_dir.clone(),
        })])
    }
}

#[derive(Debug)]
struct LocalStateResource {
    state_dir: PathBuf,
}

#[async_trait]
impl iac::Resource for LocalStateResource {
    fn change_semantics(&self, ctx: &iac::SemanticsContext<'_>) -> iac::ChangeSemantics {
        const CREATE: iac::Citation = iac::Citation::code(concat!(
            module_path!(),
            "::create — std::fs::create_dir_all on the deployment's state dir \
             (idempotent; an existing dir is success)"
        ));
        const UPDATE: iac::Citation = iac::Citation::code(concat!(
            module_path!(),
            "::update — a recorded no-op: the directory has no engine-tracked \
             mutable fields and `diff` answers NoChange"
        ));
        const DELETE: iac::Citation = iac::Citation::code(concat!(
            module_path!(),
            "::delete — deliberately a no-op: the directory and everything in \
             it are left in place, and only the engine's record is retired"
        ));
        let claims = |operation, data_effect, citation: iac::Citation| iac::ChangeSemantics {
            operation: iac::Confidence::EngineFact {
                value: operation,
                citation: citation.clone(),
            },
            replacement: iac::Confidence::EngineFact {
                value: iac::ReplacementPolicy::NotRequired,
                citation: citation.clone(),
            },
            disruption: iac::Confidence::EngineFact {
                value: iac::Disruption::None,
                citation: citation.clone(),
            },
            data_effect: iac::Confidence::EngineFact {
                value: data_effect,
                citation: citation.clone(),
            },
            reversibility: iac::Confidence::EngineFact {
                value: iac::Reversibility::Reversible,
                citation,
            },
            statement: None,
            provider_assigned: Vec::new(),
        };
        match ctx.kind {
            iac::ChangeKind::Create => claims(
                iac::LifecycleOperation::Created,
                iac::DataEffect::NoDataHeld,
                CREATE,
            ),
            iac::ChangeKind::Update | iac::ChangeKind::Replace => claims(
                iac::LifecycleOperation::UpdatedInPlace,
                iac::DataEffect::Preserved,
                UPDATE,
            ),
            iac::ChangeKind::Delete => claims(
                iac::LifecycleOperation::Deleted,
                iac::DataEffect::Preserved,
                DELETE,
            ),
            iac::ChangeKind::NoChange => iac::ChangeSemantics::default(),
        }
    }

    fn resource_type(&self) -> iac::ResourceType {
        iac::ResourceType::new("local_state_dir")
    }

    fn resource_id(&self) -> iac::ResourceId {
        iac::ResourceId("state-dir".into())
    }

    fn dependencies(&self) -> Vec<iac::ResourceId> {
        Vec::new()
    }

    fn module(&self) -> &str {
        "remote-state"
    }

    async fn create(
        &self,
        _ctx: &iac::ProvisionContext,
    ) -> std::result::Result<iac::ResourceState, iac::IacError> {
        std::fs::create_dir_all(&self.state_dir)
            .map_err(|error| iac::IacError::Other(error.into()))?;
        Ok(iac::ResourceState {
            resource_type: iac::ResourceType::new("local_state_dir"),
            physical_id: self.state_dir.display().to_string(),
            properties: serde_json::json!({ "path": self.state_dir }),
            dependencies: Vec::new(),
            created_at: String::new(),
            updated_at: String::new(),
            module: "remote-state".into(),
        })
    }

    async fn update(
        &self,
        current: &iac::ResourceState,
        _ctx: &iac::ProvisionContext,
    ) -> std::result::Result<iac::ResourceState, iac::IacError> {
        Ok(current.clone())
    }

    async fn delete(
        &self,
        _current: &iac::ResourceState,
        _ctx: &iac::ProvisionContext,
    ) -> std::result::Result<(), iac::IacError> {
        Ok(())
    }

    async fn describe(
        &self,
        _ctx: &iac::ProvisionContext,
    ) -> std::result::Result<iac::DescribeResult, iac::IacError> {
        if self.state_dir.exists() {
            Ok(iac::DescribeResult::Present(iac::ResourceState {
                resource_type: iac::ResourceType::new("local_state_dir"),
                physical_id: self.state_dir.display().to_string(),
                properties: serde_json::json!({ "path": self.state_dir }),
                dependencies: Vec::new(),
                created_at: String::new(),
                updated_at: String::new(),
                module: "remote-state".into(),
            }))
        } else {
            // `exists()` is a real check of the managed state dir → confirmed Absent.
            Ok(iac::DescribeResult::Absent)
        }
    }

    fn diff(
        &self,
        _current: &iac::ResourceState,
        _ctx: &iac::ProvisionContext,
    ) -> iac::InternalChange {
        iac::InternalChange::NoChange {
            resource_id: self.resource_id(),
        }
    }
}

#[async_trait]
impl tokeira_orchestrator::Deployment for LocalDeployment {
    type Config = LocalConfig;

    fn remote_state_module(
        &self,
        _config: &Self::Config,
        deployment_dir: &Path,
    ) -> Box<dyn iac::Module> {
        Box::new(LocalStateModule {
            state_dir: deployment_dir.join("state"),
        })
    }

    /// No infrastructure modules — local platform spawns tokeirad directly.
    fn infra_modules(
        &self,
        _config: &Self::Config,
        _selection: &iac::ModuleSelection,
    ) -> Vec<Box<dyn iac::Module>> {
        Vec::new()
    }

    /// No services to deploy — tokeirad is spawned as a bare process.
    fn services(&self, _config: &Self::Config) -> Vec<Box<dyn deploy_engine::Service>> {
        Vec::new()
    }

    /// No container images needed for bare-process execution.
    fn images(&self, _config: &Self::Config) -> Vec<Box<dyn deploy_engine::Image>> {
        Vec::new()
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

    fn create_infra_store(
        &self,
        _config: &Self::Config,
        deployment_dir: &Path,
    ) -> Box<dyn DeploymentStore<iac::InfraState>> {
        Box::new(CasStore::new(
            Box::new(LocalBackend::new(deployment_dir.join("state/infra"))),
            "infra".to_string(),
        ))
    }

    fn create_deploy_store(
        &self,
        _config: &Self::Config,
        deployment_dir: &Path,
    ) -> Box<dyn DeploymentStore<iac::RuntimeState>> {
        Box::new(CasStore::new(
            Box::new(LocalBackend::new(deployment_dir.join("state/deploy"))),
            "deploy".to_string(),
        ))
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
impl Ops for LocalDeployment {
    type Config = LocalConfig;

    fn valid_services(&self) -> &[&str] {
        static VALID: [&str; 1] = ["tokeirad"];
        &VALID
    }

    fn desired_replicas(&self, _config: &Self::Config) -> Vec<ServiceReplicas> {
        vec![ServiceReplicas {
            service: "tokeirad".into(),
            replicas: 1,
        }]
    }

    async fn scale_up(&self, _service: &str, _replicas: u32, _config: &Self::Config) -> Result<()> {
        Err(anyhow::anyhow!("not supported for local platform").into())
    }

    async fn scale_down(
        &self,
        _service: &str,
        _replicas: u32,
        _config: &Self::Config,
    ) -> Result<()> {
        Err(anyhow::anyhow!("not supported for local platform").into())
    }

    async fn logs(&self, _service: &str, _config: &Self::Config) -> Result<Vec<String>> {
        Err(anyhow::anyhow!("not supported for local platform").into())
    }

    async fn port_mappings(
        &self,
        _service: &str,
        _config: &Self::Config,
    ) -> Result<Vec<PortMapping>> {
        Err(anyhow::anyhow!("not supported for local platform").into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_orchestrator::Deployment;

    #[test]
    fn local_config_has_no_compose_fields() {
        let toml = LocalDeployment::prototypical_config(StorageKind::InMemory);
        assert!(!toml.contains("compose_file"));
        assert!(!toml.contains("observability"));
        assert!(!toml.contains("mimir"));
        assert!(!toml.contains("grafana"));
        assert!(toml.contains("project_name"));
        // state_dir is no longer in the config — it comes from the deployment directory
        assert!(!toml.contains("state_dir"));
    }

    #[test]
    fn infra_modules_returns_empty() {
        let config = LocalConfig::default();
        let modules = LocalDeployment.infra_modules(&config, &iac::ModuleSelection::All);
        assert!(modules.is_empty());
    }

    #[test]
    fn services_returns_empty() {
        let config = LocalConfig::default();
        let services = LocalDeployment.services(&config);
        assert!(services.is_empty());
    }

    #[test]
    fn images_returns_empty() {
        let config = LocalConfig::default();
        let images = LocalDeployment.images(&config);
        assert!(images.is_empty());
    }

    #[test]
    fn valid_services_only_tokeirad() {
        assert_eq!(LocalDeployment.valid_services(), &["tokeirad"]);
    }

    #[tokio::test]
    async fn scale_up_returns_not_supported() {
        let config = LocalConfig::default();
        let err = LocalDeployment
            .scale_up("tokeirad", 1, &config)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("not supported"));
    }

    #[tokio::test]
    async fn logs_returns_not_supported() {
        let config = LocalConfig::default();
        let err = LocalDeployment
            .logs("tokeirad", &config)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("not supported"));
    }
}
