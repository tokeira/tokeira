//! Deployment orchestration facade.
//!
//! This crate connects the generic framework crates to a concrete deployment
//! specialization. It does not know about any specific provider. Instead, a
//! specialization implements [`Deployment`] to supply config, state backends,
//! infrastructure modules, service definitions, images, and provider handles.
//!
//! A typical deployment wires the layers as follows:
//!
//! - [`Deployment::create_infra_store`] and
//!   [`Deployment::create_deploy_store`] choose where state is persisted.
//! - [`Deployment::remote_state_module`] provisions the backing state resource
//!   before normal infrastructure modules are planned or applied.
//! - [`Deployment::infra_modules`] returns provider-specific
//!   [`iac::Module`] objects.
//! - [`Deployment::images`] and [`Deployment::services`] describe runtime
//!   artifacts and workloads for the deploy engine.
//! - [`Deployment::register_infra_extensions`] and
//!   [`Deployment::register_deploy_extensions`] attach clients, platforms, and
//!   other provider handles to the execution contexts.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokeira_deploy_engine as deploy_engine;
use tokeira_iac as iac;
use tokeira_state::{StateBackend, StateError, StateStore};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PlatformKind {
    Local,
    Compose,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StorageKind {
    InMemory,
    Dsql,
}

pub trait PlatformConfig {
    fn prototypical_config(storage: StorageKind) -> String;

    fn prototypical_server_config(storage: StorageKind) -> String;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortMapping {
    pub host_addr: String,
    pub host_port: u16,
    pub container_port: u16,
    pub protocol: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceReplicas {
    pub service: String,
    pub replicas: u32,
}

#[derive(Debug, Error)]
pub enum OrchestratorError {
    #[error(transparent)]
    Iac(#[from] iac::IacError),
    #[error(transparent)]
    Deploy(#[from] deploy_engine::DeployError),
    #[error(transparent)]
    ConfigLoader(#[from] tokeira_config::ConfigLoaderError),
    #[error(transparent)]
    State(#[from] StateError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, OrchestratorError>;

#[async_trait]
pub trait Deployment: Send + Sync {
    /// Parsed and validated configuration for this deployment.
    type Config: Send + Sync + Clone + 'static;

    /// Return the module that provisions the state backend itself.
    ///
    /// [`InfraEngine::compose`] always prepends this module before selected
    /// infrastructure modules. Implementations should keep it small and
    /// dependency-free so state can be bootstrapped before other resources.
    fn remote_state_module(
        &self,
        config: &Self::Config,
        deployment_dir: &Path,
    ) -> Box<dyn iac::Module>;

    /// Return infrastructure modules selected for the current operation.
    ///
    /// The returned modules should describe desired infrastructure only. Side
    /// effects belong in their resources' lifecycle methods.
    fn infra_modules(
        &self,
        config: &Self::Config,
        selection: &iac::ModuleSelection,
    ) -> Vec<Box<dyn iac::Module>>;

    /// Return runtime services that should be planned or applied.
    fn services(&self, config: &Self::Config) -> Vec<Box<dyn deploy_engine::Service>>;

    /// Return images that should be resolved before service apply.
    fn images(&self, config: &Self::Config) -> Vec<Box<dyn deploy_engine::Image>>;

    /// Return namespaces or equivalent tenancy units required by this deployment.
    ///
    /// This is an operator-facing validation hook. Implementations can return an
    /// empty list when the target platform has no namespace concept.
    fn required_namespaces(&self, config: &Self::Config) -> Vec<String>;

    /// Register provider-specific infrastructure handles on the provision context.
    ///
    /// Examples include AWS clients, Docker Compose platforms, Kubernetes
    /// clients, project metadata, and validated config fragments. Resources and
    /// modules retrieve these values through `ctx.extension::<T>()`.
    async fn register_infra_extensions(
        &self,
        config: &Self::Config,
        ctx: &mut iac::ProvisionContext,
    ) -> Result<()>;

    /// Register provider-specific runtime handles on the service context.
    ///
    /// Services and platforms retrieve these values through
    /// `ctx.extension::<T>()` while generating or applying manifests.
    async fn register_deploy_extensions(
        &self,
        config: &Self::Config,
        ctx: &mut deploy_engine::ServiceContext,
    ) -> Result<()>;

    /// Create the backend used for infrastructure state.
    ///
    /// **Bootstrap invariant:** the returned backend must tolerate a missing
    /// backing store on `load()` by returning empty/default state. This allows
    /// [`remote_state_module`](Self::remote_state_module) to create the backing
    /// store (S3 bucket, local directory, etc.) as a normal resource during the
    /// first `apply`. After that apply, the backend can read and write normally.
    ///
    /// For the S3 backend, `load()` treats `NoSuchBucket` as "no state yet"
    /// and returns `InfraState::default()`. For the local backend, `load()`
    /// returns default when the directory doesn't exist, and `save()` creates
    /// the directory on first write.
    fn create_infra_store(
        &self,
        config: &Self::Config,
        deployment_dir: &Path,
    ) -> Box<dyn StateBackend>;

    /// Create the backend used for runtime deployment state.
    fn create_deploy_store(
        &self,
        config: &Self::Config,
        deployment_dir: &Path,
    ) -> Box<dyn StateBackend>;

    /// Project infrastructure outputs back into deployment config.
    ///
    /// This lets later deployment phases consume values discovered during
    /// infrastructure apply without coupling them to a concrete state layout.
    fn hydrate_config(&self, config: &Self::Config, state: &iac::InfraState) -> Self::Config;

    /// Return config-file updates derived from the applied infrastructure state.
    ///
    /// The CLI layer can pass these `(dotted_key, value)` pairs to the config
    /// writer when an apply should persist discovered values.
    fn collect_writeback(
        &self,
        config: &Self::Config,
        state: &iac::InfraState,
    ) -> Vec<(String, String)>;
}

#[async_trait]
pub trait Ops: Send + Sync {
    type Config;

    /// Services accepted by the operator commands.
    fn valid_services(&self) -> &[&str];

    /// Desired replica counts in platform-defined startup order.
    fn desired_replicas(&self, config: &Self::Config) -> Vec<ServiceReplicas>;

    /// Increase the number of service instances by the requested amount.
    async fn scale_up(&self, service: &str, replicas: u32, config: &Self::Config) -> Result<()>;

    /// Decrease the number of service instances by the requested amount.
    async fn scale_down(&self, service: &str, replicas: u32, config: &Self::Config) -> Result<()>;

    /// Return recent logs for the named service.
    async fn logs(&self, service: &str, config: &Self::Config) -> Result<Vec<String>>;

    /// Return all host/container port mappings for the named service.
    async fn port_mappings(&self, service: &str, config: &Self::Config)
    -> Result<Vec<PortMapping>>;
}

pub struct InfraEngine<D: Deployment> {
    deployment: D,
    engine: iac::Engine,
    ctx: iac::ProvisionContext,
    config: D::Config,
    deployment_dir: PathBuf,
    state_store: Arc<StateStore<iac::InfraState>>,
}

impl<D: Deployment> InfraEngine<D> {
    /// Create an infrastructure facade for one deployment config.
    ///
    /// The facade loads persisted infrastructure state, installs deployment
    /// extensions, and keeps the hydrated config after apply. Custom
    /// deployments do not normally implement their own engine; they implement
    /// [`Deployment`] and let this facade coordinate state and ordering.
    pub async fn new(deployment: D, config: &D::Config, deployment_dir: &Path) -> Result<Self> {
        let mut ctx = iac::ProvisionContext::default();
        let state_store = Arc::new(StateStore::new(
            deployment.create_infra_store(config, deployment_dir),
            "infra".to_string(),
        ));
        let (state, _) = state_store.load().await?;
        ctx.state = state;
        deployment
            .register_infra_extensions(config, &mut ctx)
            .await?;
        Ok(Self {
            deployment,
            engine: iac::Engine::new(),
            ctx,
            config: config.clone(),
            deployment_dir: deployment_dir.to_path_buf(),
            state_store,
        })
    }

    pub fn compose(&self, selection: iac::ModuleSelection) -> Result<iac::InfraComposition> {
        let remote_state = self
            .deployment
            .remote_state_module(&self.config, &self.deployment_dir);

        let infra_modules = self.deployment.infra_modules(&self.config, &selection);

        // Desired: remote-state + selected infra modules
        let mut desired_modules: Vec<Box<dyn iac::Module>> = Vec::new();
        desired_modules.push(
            self.deployment
                .remote_state_module(&self.config, &self.deployment_dir),
        );
        desired_modules.extend(self.deployment.infra_modules(&self.config, &selection));

        // Known: remote-state + all infra modules (superset of desired,
        // so removed modules can still be deleted)
        let mut known_modules: Vec<Box<dyn iac::Module>> = Vec::new();
        known_modules.push(remote_state);
        known_modules.extend(infra_modules);

        // Active: module names in scope for this operation
        let active_modules = desired_modules
            .iter()
            .map(|m| m.name().to_string())
            .collect();

        Ok(iac::InfraComposition {
            desired_modules,
            known_modules,
            active_modules,
        })
    }

    /// Plan infrastructure changes without calling resource lifecycle methods
    /// that mutate provider state.
    pub async fn plan(&mut self, composition: &iac::InfraComposition) -> Result<Vec<iac::Change>> {
        let (state, _) = self.state_store.load().await?;
        self.ctx.state = state;
        Ok(self.engine.plan(composition, &mut self.ctx).await?)
    }

    /// Apply infrastructure changes and persist the resulting state document.
    pub async fn apply(&mut self, composition: &iac::InfraComposition) -> Result<Vec<iac::Change>> {
        let (state, version) = self.state_store.load().await?;
        self.ctx.state = state;
        let saver = self.make_saver(version.clone());
        let changes = self
            .engine
            .apply(composition, &mut self.ctx, Some(&saver))
            .await?;
        let _ = self.state_store.save(&self.ctx.state, &version).await?;
        self.config = self
            .deployment
            .hydrate_config(&self.config, &self.ctx.state);
        Ok(changes)
    }

    /// Destroy the resources in the supplied composition and persist state.
    pub async fn destroy(
        &mut self,
        composition: &iac::InfraComposition,
    ) -> Result<Vec<iac::Change>> {
        let (state, version) = self.state_store.load().await?;
        self.ctx.state = state;
        let saver = self.make_saver(version.clone());
        let changes = self
            .engine
            .destroy(composition, &mut self.ctx, Some(&saver))
            .await?;
        let _ = self.state_store.save(&self.ctx.state, &version).await?;
        Ok(changes)
    }

    /// Return writeback values derived from the latest in-memory state.
    pub fn collect_writeback(&self) -> Vec<(String, String)> {
        self.deployment
            .collect_writeback(&self.config, &self.ctx.state)
    }

    fn make_saver(&self, version: String) -> iac::StateSaver {
        let store = Arc::clone(&self.state_store);
        Box::new(move |state: &crate::iac::InfraState| {
            let store = Arc::clone(&store);
            let version = version.clone();
            let state = state.clone();
            Box::pin(async move {
                store
                    .save(&state, &version)
                    .await
                    .map_err(iac::IacError::State)?;
                Ok(())
            })
        })
    }
}

pub struct DeployEngine<D: Deployment> {
    deployment: D,
    engine: deploy_engine::ServiceEngine,
    service_ctx: deploy_engine::ServiceContext,
    image_ctx: deploy_engine::ImageContext,
    config: D::Config,
    state_store: StateStore<iac::RuntimeState>,
}

impl<D: Deployment> DeployEngine<D> {
    /// Create a deployment facade for one deployment config.
    ///
    /// The facade loads runtime state on each plan/apply call and delegates
    /// provider-specific manifest application to the supplied platform.
    pub async fn new(deployment: D, config: &D::Config, deployment_dir: &Path) -> Result<Self> {
        let mut service_ctx = deploy_engine::ServiceContext::default();
        let image_ctx = deploy_engine::ImageContext::default();
        deployment
            .register_deploy_extensions(config, &mut service_ctx)
            .await?;
        let state_store = StateStore::new(
            deployment.create_deploy_store(config, deployment_dir),
            "deploy".to_string(),
        );
        Ok(Self {
            deployment,
            engine: deploy_engine::ServiceEngine::new(),
            service_ctx,
            image_ctx,
            config: config.clone(),
            state_store,
        })
    }

    /// Plan service changes from desired manifests and persisted runtime state.
    pub async fn plan(&mut self) -> Result<Vec<deploy_engine::ServiceChange>> {
        let (state, _) = self.state_store.load().await?;
        let services = self.deployment.services(&self.config);
        Ok(self
            .engine
            .plan_services(&services, &mut self.service_ctx, &state)
            .await?)
    }

    /// Resolve images, apply changed service manifests, and persist runtime state.
    pub async fn apply(
        &mut self,
        platform: &dyn deploy_engine::Platform,
    ) -> Result<Vec<deploy_engine::ServiceChange>> {
        let (mut state, version) = self.state_store.load().await?;
        let images = self.deployment.images(&self.config);
        self.engine
            .record_images(&images, &self.image_ctx, &mut state)
            .await?;
        let services = self.deployment.services(&self.config);
        let changes = self
            .engine
            .apply_services(&services, platform, &mut self.service_ctx, &mut state)
            .await?;
        let _ = self.state_store.save(&state, &version).await?;
        Ok(changes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use tokeira_state::LocalBackend;

    #[derive(Clone)]
    struct TestConfig;

    #[derive(Debug)]
    struct TestResource;

    #[async_trait]
    impl iac::Resource for TestResource {
        fn resource_type(&self) -> iac::ResourceType {
            iac::ResourceType::new("test")
        }

        fn resource_id(&self) -> iac::ResourceId {
            iac::ResourceId("test-resource".into())
        }

        fn dependencies(&self) -> Vec<iac::ResourceId> {
            Vec::new()
        }

        fn module(&self) -> &str {
            "test"
        }

        async fn create(
            &self,
            _ctx: &iac::ProvisionContext,
        ) -> std::result::Result<iac::ResourceState, iac::IacError> {
            Ok(iac::ResourceState {
                resource_type: iac::ResourceType::new("test"),
                physical_id: "phys-test".into(),
                properties: serde_json::json!({"ok": true}),
                dependencies: Vec::new(),
                created_at: String::new(),
                updated_at: String::new(),
                module: "test".into(),
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
        ) -> std::result::Result<Option<iac::ResourceState>, iac::IacError> {
            Ok(None)
        }

        fn diff(
            &self,
            _current: &iac::ResourceState,
            _ctx: &iac::ProvisionContext,
        ) -> iac::InternalChange {
            iac::InternalChange::NoChange {
                resource_id: iac::ResourceId("test-resource".into()),
            }
        }
    }

    #[derive(Debug)]
    struct TestModule(&'static str);

    impl iac::Module for TestModule {
        fn name(&self) -> &str {
            self.0
        }

        fn dependencies(&self) -> &[&str] {
            &[]
        }

        fn resources(
            &self,
            _ctx: &iac::ModuleContext<'_>,
        ) -> std::result::Result<Vec<Box<dyn iac::Resource>>, iac::IacError> {
            Ok(vec![Box::new(TestResource)])
        }
    }

    #[derive(Debug)]
    struct TestImage;

    impl deploy_engine::Image for TestImage {
        fn name(&self) -> &str {
            "image"
        }

        fn source_type(&self) -> deploy_engine::ImageSourceType {
            deploy_engine::ImageSourceType::Registry
        }

        fn desired_ref(
            &self,
            _ctx: &deploy_engine::ImageContext,
        ) -> std::result::Result<String, deploy_engine::DeployError> {
            Ok("example:latest".into())
        }
    }

    #[derive(Debug)]
    struct TestService;

    impl deploy_engine::Service for TestService {
        fn name(&self) -> &str {
            "service"
        }

        fn module(&self) -> &str {
            "module"
        }

        fn dependencies(&self) -> &[&str] {
            &[]
        }

        fn manifests(
            &self,
            _ctx: &deploy_engine::ServiceContext,
        ) -> std::result::Result<Vec<serde_json::Value>, deploy_engine::DeployError> {
            Ok(vec![serde_json::json!({"service": "ok"})])
        }
    }

    struct TestPlatform;

    #[async_trait]
    impl deploy_engine::Platform for TestPlatform {
        async fn apply_manifests(
            &self,
            _manifests: &[serde_json::Value],
        ) -> std::result::Result<usize, deploy_engine::DeployError> {
            Ok(1)
        }
    }

    #[derive(Clone)]
    struct TestDeployment;

    #[async_trait]
    impl Deployment for TestDeployment {
        type Config = TestConfig;

        fn remote_state_module(
            &self,
            _config: &Self::Config,
            _deployment_dir: &Path,
        ) -> Box<dyn iac::Module> {
            Box::new(TestModule("remote-state"))
        }

        fn infra_modules(
            &self,
            _config: &Self::Config,
            _selection: &iac::ModuleSelection,
        ) -> Vec<Box<dyn iac::Module>> {
            vec![Box::new(TestModule("app"))]
        }

        fn services(&self, _config: &Self::Config) -> Vec<Box<dyn deploy_engine::Service>> {
            vec![Box::new(TestService)]
        }

        fn images(&self, _config: &Self::Config) -> Vec<Box<dyn deploy_engine::Image>> {
            vec![Box::new(TestImage)]
        }

        fn required_namespaces(&self, _config: &Self::Config) -> Vec<String> {
            Vec::new()
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

    #[tokio::test]
    async fn infra_engine_prepends_remote_state_module() {
        let temp = tempfile::tempdir().unwrap();
        let config = TestConfig;
        let engine = InfraEngine::new(TestDeployment, &config, temp.path())
            .await
            .unwrap();
        let composition = engine.compose(iac::ModuleSelection::All).unwrap();
        assert_eq!(composition.desired_modules.len(), 2);
        assert_eq!(composition.desired_modules[0].name(), "remote-state");
        assert_eq!(composition.known_modules.len(), 2);
        assert_eq!(composition.known_modules[0].name(), "remote-state");
        assert_eq!(composition.active_modules.len(), 2);
    }

    #[tokio::test]
    async fn deploy_engine_applies_services() {
        let temp = tempfile::tempdir().unwrap();
        let config = TestConfig;
        let mut engine = DeployEngine::new(TestDeployment, &config, temp.path())
            .await
            .unwrap();
        let changes = engine.apply(&TestPlatform).await.unwrap();
        assert_eq!(changes.len(), 1);
    }
}
