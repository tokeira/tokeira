//! `Deployment` trait implementation for the remote workstation.
//!
//! Follows the same pattern as `platforms/compose/src/lib.rs`:
//! - `remote_state_module()` returns a `LocalStateModule` that ensures the
//!   state directory exists
//! - `infra_modules()` returns the `WorkstationModule`
//! - `register_infra_extensions()` registers `AwsClients` on the context
//! - `create_infra_store()` returns a `LocalBackend`
//!
//! The `tkr workstation up` handler uses `InfraEngine<WorkstationDeployment>`
//! exactly like `tkr infra apply` uses `InfraEngine<ComposeDeployment>`.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tokeira_aws::AwsClients;
use tokeira_iac::{self as iac, Module, ModuleContext, Resource};
use tokeira_orchestrator::Deployment;
use tokeira_state::{LocalBackend, StateBackend};

use crate::module::WorkstationModuleConfig;

/// Configuration for a workstation deployment.
///
/// Combines the module config (what resources to create) with the region
/// (for AWS client construction).
#[derive(Debug, Clone)]
pub struct WorkstationConfig {
    pub module_config: WorkstationModuleConfig,
    pub region: String,
}

/// The workstation deployment. Implements `tokeira_orchestrator::Deployment`
/// so it can be driven by `InfraEngine`.
#[derive(Debug)]
pub struct WorkstationDeployment;

#[async_trait]
impl Deployment for WorkstationDeployment {
    type Config = WorkstationConfig;

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
        let rctx = tokeira_aws::ResourceContext {
            project: format!(
                "tokeira-workstation-{}",
                config.module_config.workstation_id
            ),
            region: config.region.clone(),
            tags: workstation_tags(&config.module_config.workstation_id),
        };

        let identity = crate::module::IdentityModule {
            workstation_id: config.module_config.workstation_id.clone(),
            rctx: rctx.clone(),
        };
        let network = crate::module::NetworkModule {
            workstation_id: config.module_config.workstation_id.clone(),
            vpc_id: config.module_config.vpc_id.clone(),
            rctx: rctx.clone(),
        };
        let storage = crate::module::StorageModule {
            workstation_id: config.module_config.workstation_id.clone(),
            cache_volume_gib: config.module_config.cache_volume_gib,
            repo_volume_gib: config.module_config.repo_volume_gib,
            availability_zone: config.module_config.availability_zone.clone(),
            rctx: rctx.clone(),
        };
        let compute = crate::module::ComputeModule {
            config: config.module_config.clone(),
            rctx,
        };

        let mut modules: Vec<Box<dyn iac::Module>> = Vec::new();
        if selection.includes(identity.name()) {
            modules.push(Box::new(identity));
        }
        if selection.includes(network.name()) {
            modules.push(Box::new(network));
        }
        if selection.includes(storage.name()) {
            modules.push(Box::new(storage));
        }
        if selection.includes(compute.name()) {
            modules.push(Box::new(compute));
        }
        modules
    }

    fn services(&self, _config: &Self::Config) -> Vec<Box<dyn tokeira_deploy_engine::Service>> {
        // Workstation has no runtime services managed by the deploy engine
        vec![]
    }

    fn images(&self, _config: &Self::Config) -> Vec<Box<dyn tokeira_deploy_engine::Image>> {
        vec![]
    }

    fn required_namespaces(&self, _config: &Self::Config) -> Vec<String> {
        vec![]
    }

    async fn register_infra_extensions(
        &self,
        config: &Self::Config,
        ctx: &mut iac::ProvisionContext,
    ) -> tokeira_orchestrator::Result<()> {
        let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(config.region.clone()))
            .load()
            .await;
        ctx.set_extension(AwsClients::new(&aws_config));
        Ok(())
    }

    async fn register_deploy_extensions(
        &self,
        _config: &Self::Config,
        _ctx: &mut tokeira_deploy_engine::ServiceContext,
    ) -> tokeira_orchestrator::Result<()> {
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

/// Resolve the deployment directory for a workstation.
///
/// This is the directory where `InfraEngine` stores state, equivalent to
/// the deployment directory used by `tkr deployment create`.
pub fn deployment_dir_for(workstation_id: &str) -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".tokeira")
        .join("workstations")
        .join(workstation_id)
}

fn workstation_tags(workstation_id: &str) -> std::collections::HashMap<String, String> {
    let mut tags = std::collections::HashMap::new();
    tags.insert("tokeira-workstation".into(), "true".into());
    tags.insert("workstation-id".into(), workstation_id.into());
    tags.insert("ManagedBy".into(), "tokeira-cli".into());
    tags
}

// ── LocalStateModule ─────────────────────────────────────────────────────────
//
// Ensures the local state directory exists. Same pattern as
// `platforms/compose/src/modules.rs::LocalStateModule`.

#[derive(Debug)]
struct LocalStateModule {
    state_dir: PathBuf,
}

impl iac::Module for LocalStateModule {
    fn name(&self) -> &str {
        "remote-state"
    }

    fn dependencies(&self) -> &[&str] {
        &[]
    }

    fn resources(&self, _ctx: &ModuleContext<'_>) -> Result<Vec<Box<dyn Resource>>, iac::IacError> {
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
impl Resource for LocalStateResource {
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
    ) -> Result<iac::ResourceState, iac::IacError> {
        std::fs::create_dir_all(&self.state_dir).map_err(|e| {
            iac::IacError::AwsSdk(format!(
                "failed to create state directory {}: {e}",
                self.state_dir.display()
            ))
        })?;
        Ok(iac::ResourceState {
            resource_type: self.resource_type(),
            physical_id: self.state_dir.display().to_string(),
            properties: serde_json::json!({}),
            dependencies: vec![],
            module: "remote-state".into(),
            created_at: chrono::Utc::now().to_rfc3339(),
            updated_at: chrono::Utc::now().to_rfc3339(),
        })
    }

    async fn update(
        &self,
        current: &iac::ResourceState,
        _ctx: &iac::ProvisionContext,
    ) -> Result<iac::ResourceState, iac::IacError> {
        Ok(current.clone())
    }

    async fn delete(
        &self,
        _current: &iac::ResourceState,
        _ctx: &iac::ProvisionContext,
    ) -> Result<(), iac::IacError> {
        // Don't delete the state directory on destroy — it holds the state
        // that records the destroy itself completed.
        Ok(())
    }

    async fn describe(
        &self,
        _ctx: &iac::ProvisionContext,
    ) -> Result<iac::DescribeResult, iac::IacError> {
        if self.state_dir.exists() {
            Ok(iac::DescribeResult::Present(iac::ResourceState {
                resource_type: self.resource_type(),
                physical_id: self.state_dir.display().to_string(),
                properties: serde_json::json!({}),
                dependencies: vec![],
                module: "remote-state".into(),
                created_at: String::new(),
                updated_at: chrono::Utc::now().to_rfc3339(),
            }))
        } else {
            // `exists()` is a real existence check of the managed state dir, so
            // a negative result is a confirmed Absent.
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
