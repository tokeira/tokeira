//! IaC modules for the compose platform.
//!
//! Two logical modules group compose services:
//! - `runtime` — the tokeirad service
//! - `observability` — mimir, loki, grafana, alloy

use std::path::PathBuf;

use async_trait::async_trait;
use tokeira_compose::ComposeService;
use tokeira_iac as iac;

use crate::{
    compose::{MODULE_OBSERVABILITY, MODULE_RUNTIME, compose_services, module_for_service},
    config::ComposeConfig,
    observability_config::{ObservabilityConfigFilesResource, ObservabilityParams},
};

// ── Local state module (remote-state bootstrap) ───────────────────

#[derive(Debug)]
pub struct LocalStateModule {
    pub state_dir: PathBuf,
}

impl iac::Module for LocalStateModule {
    fn name(&self) -> &str {
        "remote-state"
    }

    fn dependencies(&self) -> &[&str] {
        &[]
    }

    fn resources(
        &self,
        _ctx: &iac::ModuleContext<'_>,
    ) -> Result<Vec<Box<dyn iac::Resource>>, iac::IacError> {
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
    ) -> Result<iac::ResourceState, iac::IacError> {
        Ok(current.clone())
    }

    async fn delete(
        &self,
        _current: &iac::ResourceState,
        _ctx: &iac::ProvisionContext,
    ) -> Result<(), iac::IacError> {
        Ok(())
    }

    async fn describe(
        &self,
        _ctx: &iac::ProvisionContext,
    ) -> Result<Option<iac::ResourceState>, iac::IacError> {
        if self.state_dir.exists() {
            Ok(Some(iac::ResourceState {
                resource_type: iac::ResourceType::new("local_state_dir"),
                physical_id: self.state_dir.display().to_string(),
                properties: serde_json::json!({ "path": self.state_dir }),
                dependencies: Vec::new(),
                created_at: String::new(),
                updated_at: String::new(),
                module: "remote-state".into(),
            }))
        } else {
            Ok(None)
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

// ── Compose service modules ───────────────────────────────────────

/// Groups compose services under a logical module name.
///
/// Each compose service resource reports its owning module via
/// [`module_for_service`], so `infra destroy --module observability`
/// correctly targets mimir, loki, grafana, and alloy.
#[derive(Debug)]
pub struct ComposeModule {
    module_name: String,
    config_files: Option<ObservabilityConfigFilesResource>,
    services: Vec<ComposeService>,
}

impl ComposeModule {
    /// Build the runtime module (tokeirad).
    pub fn runtime(config: &ComposeConfig) -> Self {
        let services: Vec<ComposeService> = compose_services(config)
            .into_iter()
            .filter(|s| module_for_service(&s.name) == MODULE_RUNTIME)
            .collect();
        Self {
            module_name: MODULE_RUNTIME.into(),
            config_files: None,
            services,
        }
    }

    /// Build the observability module (mimir, loki, grafana, alloy).
    pub fn observability(config: &ComposeConfig) -> Self {
        let services: Vec<ComposeService> = compose_services(config)
            .into_iter()
            .filter(|s| module_for_service(&s.name) == MODULE_OBSERVABILITY)
            .collect();
        Self {
            module_name: MODULE_OBSERVABILITY.into(),
            config_files: Some(ObservabilityConfigFilesResource::new(
                config.deployment_dir.clone(),
                ObservabilityParams::from_config(config),
            )),
            services,
        }
    }
}

impl iac::Module for ComposeModule {
    fn name(&self) -> &str {
        &self.module_name
    }

    fn dependencies(&self) -> &[&str] {
        match self.module_name.as_str() {
            MODULE_RUNTIME => &["remote-state"],
            MODULE_OBSERVABILITY => &["remote-state", "runtime"],
            _ => &[],
        }
    }

    fn resources(
        &self,
        _ctx: &iac::ModuleContext<'_>,
    ) -> Result<Vec<Box<dyn iac::Resource>>, iac::IacError> {
        let mut resources: Vec<Box<dyn iac::Resource>> = Vec::new();
        let config_resource_id = self
            .config_files
            .as_ref()
            .map(|_| ObservabilityConfigFilesResource::resource_id_value());
        if let Some(config_files) = &self.config_files {
            resources.push(Box::new(config_files.clone()));
        }
        resources.extend(self.services.iter().map(|service| {
            let config_resource_id = if module_for_service(&service.name) == MODULE_OBSERVABILITY {
                config_resource_id.clone()
            } else {
                None
            };
            Box::new(OwnedComposeResource {
                service: service.clone(),
                module_name: self.module_name.clone(),
                config_resource_id,
            }) as Box<dyn iac::Resource>
        }));
        Ok(resources)
    }
}

/// A compose service resource that reports the correct owning module.
#[derive(Debug)]
struct OwnedComposeResource {
    service: ComposeService,
    module_name: String,
    config_resource_id: Option<iac::ResourceId>,
}

#[async_trait]
impl iac::Resource for OwnedComposeResource {
    fn resource_type(&self) -> iac::ResourceType {
        self.service.resource_type()
    }

    fn resource_id(&self) -> iac::ResourceId {
        self.service.resource_id()
    }

    fn dependencies(&self) -> Vec<iac::ResourceId> {
        let mut dependencies = self.service.dependencies();
        if let Some(config_resource_id) = &self.config_resource_id {
            dependencies.push(config_resource_id.clone());
        }
        dependencies
    }

    fn module(&self) -> &str {
        &self.module_name
    }

    async fn create(
        &self,
        ctx: &iac::ProvisionContext,
    ) -> Result<iac::ResourceState, iac::IacError> {
        let mut state = self.service.create(ctx).await?;
        state.module = self.module_name.clone();
        state.dependencies = self.dependencies();
        Ok(state)
    }

    async fn update(
        &self,
        current: &iac::ResourceState,
        ctx: &iac::ProvisionContext,
    ) -> Result<iac::ResourceState, iac::IacError> {
        let mut state = self.service.update(current, ctx).await?;
        state.module = self.module_name.clone();
        state.dependencies = self.dependencies();
        Ok(state)
    }

    async fn delete(
        &self,
        current: &iac::ResourceState,
        ctx: &iac::ProvisionContext,
    ) -> Result<(), iac::IacError> {
        self.service.delete(current, ctx).await
    }

    async fn describe(
        &self,
        ctx: &iac::ProvisionContext,
    ) -> Result<Option<iac::ResourceState>, iac::IacError> {
        match self.service.describe(ctx).await? {
            Some(mut state) => {
                state.module = self.module_name.clone();
                state.dependencies = self.dependencies();
                Ok(Some(state))
            }
            None => Ok(None),
        }
    }

    fn diff(
        &self,
        current: &iac::ResourceState,
        ctx: &iac::ProvisionContext,
    ) -> iac::InternalChange {
        self.service.diff(current, ctx)
    }
}
