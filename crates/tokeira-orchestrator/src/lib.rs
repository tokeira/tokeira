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
//!   artifacts and workloads for the deploy/apply phase.
//! - [`Deployment::register_infra_extensions`] and
//!   [`Deployment::register_deploy_extensions`] attach clients, platforms, and
//!   other provider handles to the execution contexts.

use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokeira_deploy_engine as deploy_engine;
use tokeira_iac as iac;
use tokeira_state::{DeploymentStore, StateError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PlatformKind {
    Local,
    Compose,
    Ecs,
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

    /// Register provider-specific image handles and config on the image context.
    ///
    /// Images retrieve these values through `ctx.extension::<T>()` while
    /// resolving desired refs and writeback targets.
    async fn register_image_extensions(
        &self,
        _config: &Self::Config,
        _ctx: &mut deploy_engine::ImageContext,
    ) -> Result<()> {
        Ok(())
    }

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
    /// Select the **store** (not just a backend) for infrastructure state.
    ///
    /// A platform returns the concrete store it wants: `CasStore` over a
    /// `LocalBackend` for local/compose dev, or the S3-native `S3StateStore`
    /// (snapshot/lease) for cloud platforms. The engine holds it as a
    /// `Box<dyn DeploymentStore<InfraState>>`.
    fn create_infra_store(
        &self,
        config: &Self::Config,
        deployment_dir: &Path,
    ) -> Box<dyn DeploymentStore<iac::InfraState>>;

    /// Select the store used for runtime deployment state (see
    /// [`create_infra_store`](Self::create_infra_store)).
    fn create_deploy_store(
        &self,
        config: &Self::Config,
        deployment_dir: &Path,
    ) -> Box<dyn DeploymentStore<iac::RuntimeState>>;

    /// Project infrastructure outputs back into deployment config.
    ///
    /// This lets later deployment phases consume values discovered during
    /// infrastructure apply without coupling them to a concrete state layout.
    fn hydrate_config(&self, config: &Self::Config, state: &iac::InfraState) -> Self::Config;

    /// Return config-file updates derived from the applied infrastructure state.
    ///
    /// The host application can pass these `(dotted_key, value)` pairs to the
    /// config writer when an apply should persist discovered values.
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
    state_store: Arc<dyn DeploymentStore<iac::InfraState>>,
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
        deployment
            .register_infra_extensions(config, &mut ctx)
            .await?;
        let state_store: Arc<dyn DeploymentStore<iac::InfraState>> =
            Arc::from(deployment.create_infra_store(config, deployment_dir));
        let (state, _) = state_store.load().await?;
        ctx.state = state;
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

        let selected_infra_modules = self.deployment.infra_modules(&self.config, &selection);
        let known_infra_modules = self
            .deployment
            .infra_modules(&self.config, &iac::ModuleSelection::All);

        // Desired: remote-state + selected infra modules
        let mut desired_modules: Vec<Box<dyn iac::Module>> = Vec::new();
        desired_modules.push(
            self.deployment
                .remote_state_module(&self.config, &self.deployment_dir),
        );
        desired_modules.extend(selected_infra_modules);

        // Known: remote-state + all infra modules (superset of desired,
        // so removed modules can still be deleted)
        let mut known_modules: Vec<Box<dyn iac::Module>> = Vec::new();
        known_modules.push(remote_state);
        known_modules.extend(known_infra_modules);

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
    pub async fn plan(
        &mut self,
        composition: &iac::InfraComposition,
        selection: iac::ModuleSelection,
    ) -> Result<Vec<iac::Change>> {
        let (state, _) = self.state_store.load().await?;
        self.ctx.state = state;
        let changes = match selection {
            iac::ModuleSelection::All => self.engine.plan(composition, &mut self.ctx).await,
            iac::ModuleSelection::Only(_) | iac::ModuleSelection::Except(_) => {
                self.engine
                    .plan_for_modules(composition, &mut self.ctx)
                    .await
            }
        }?;
        Ok(changes)
    }

    /// Plan infrastructure destruction without mutating provider state.
    pub async fn plan_destroy(
        &mut self,
        composition: &iac::InfraComposition,
        selection: iac::ModuleSelection,
    ) -> Result<Vec<iac::Change>> {
        let (state, _) = self.state_store.load().await?;
        self.ctx.state = state;
        self.ctx.set_extension(iac::DestroyMode);
        let result = match selection {
            iac::ModuleSelection::All => self.engine.plan_destroy(composition, &mut self.ctx).await,
            iac::ModuleSelection::Only(_) | iac::ModuleSelection::Except(_) => {
                self.engine
                    .plan_destroy_for_modules(composition, &mut self.ctx)
                    .await
            }
        };
        self.ctx.remove_extension::<iac::DestroyMode>();
        Ok(result?)
    }

    /// Apply infrastructure changes and persist the resulting state document.
    pub async fn apply(
        &mut self,
        composition: &iac::InfraComposition,
        selection: iac::ModuleSelection,
    ) -> Result<Vec<iac::Change>> {
        let (state, version) = self.state_store.load().await?;
        self.ctx.state = state;
        let saver = self.make_saver(version);
        let changes = match selection {
            iac::ModuleSelection::All => {
                self.engine
                    .apply(composition, &mut self.ctx, Some(&saver))
                    .await
            }
            iac::ModuleSelection::Only(_) | iac::ModuleSelection::Except(_) => {
                self.engine
                    .apply_for_modules(composition, &mut self.ctx, Some(&saver))
                    .await
            }
        }?;
        self.config = self
            .deployment
            .hydrate_config(&self.config, &self.ctx.state);
        Ok(changes)
    }

    /// Destroy the resources in the supplied composition and persist state.
    pub async fn destroy(
        &mut self,
        composition: &iac::InfraComposition,
        selection: iac::ModuleSelection,
    ) -> Result<Vec<iac::Change>> {
        let (state, version) = self.state_store.load().await?;
        self.ctx.state = state;
        self.ctx.set_extension(iac::DestroyMode);
        let saver = self.make_saver(version);
        let result = match selection {
            iac::ModuleSelection::All => {
                self.engine
                    .destroy(composition, &mut self.ctx, Some(&saver))
                    .await
            }
            iac::ModuleSelection::Only(_) | iac::ModuleSelection::Except(_) => {
                self.engine
                    .destroy_for_modules(composition, &mut self.ctx, Some(&saver))
                    .await
            }
        };
        self.ctx.remove_extension::<iac::DestroyMode>();
        Ok(result?)
    }

    /// Return writeback values derived from the latest in-memory state.
    pub fn collect_writeback(&self) -> Vec<(String, String)> {
        self.deployment
            .collect_writeback(&self.config, &self.ctx.state)
    }

    /// Access the provision context for presentation-layer hooks such as progress reporting.
    pub fn provision_context_mut(&mut self) -> &mut iac::ProvisionContext {
        &mut self.ctx
    }

    fn make_saver(&self, initial_version: String) -> iac::StateSaver {
        let store = Arc::clone(&self.state_store);
        let version = Arc::new(Mutex::new(initial_version));
        Box::new(move |state: &crate::iac::InfraState| {
            let store = Arc::clone(&store);
            let version = Arc::clone(&version);
            let state = state.clone();
            Box::pin(async move {
                let current = version
                    .lock()
                    .map_err(|_| {
                        iac::IacError::Other(anyhow::anyhow!("state version mutex poisoned"))
                    })?
                    .clone();
                let next = store
                    .save(&state, &current)
                    .await
                    .map_err(iac::IacError::State)?;
                *version.lock().map_err(|_| {
                    iac::IacError::Other(anyhow::anyhow!("state version mutex poisoned"))
                })? = next;
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
    state_store: Box<dyn DeploymentStore<iac::RuntimeState>>,
}

impl<D: Deployment> DeployEngine<D> {
    /// Create a deployment facade for one deployment config.
    ///
    /// The facade loads runtime state on each plan/apply call and delegates
    /// provider-specific manifest application to the supplied platform.
    pub async fn new(deployment: D, config: &D::Config, deployment_dir: &Path) -> Result<Self> {
        let mut service_ctx = deploy_engine::ServiceContext::default();
        let mut image_ctx = deploy_engine::ImageContext::default();
        deployment
            .register_deploy_extensions(config, &mut service_ctx)
            .await?;
        deployment
            .register_image_extensions(config, &mut image_ctx)
            .await?;
        let state_store = deployment.create_deploy_store(config, deployment_dir);
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

    /// Clear persisted service state so the next apply treats all services as
    /// new. Use after rebuilding a local image behind the same tag.
    pub async fn clear_service_state(&mut self) -> Result<()> {
        let (mut state, version) = self.state_store.load().await?;
        state.services.clear();
        let _ = self.state_store.save(&state, &version).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};
    use tokeira_state::{CasStore, LocalBackend, StateBackend, StateError};

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
        ) -> std::result::Result<iac::DescribeResult, iac::IacError> {
            Ok(iac::DescribeResult::Absent)
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
    struct DestroyModeAssertingModule {
        name: &'static str,
        expected: bool,
    }

    impl iac::Module for DestroyModeAssertingModule {
        fn name(&self) -> &str {
            self.name
        }

        fn dependencies(&self) -> &[&str] {
            &[]
        }

        fn resources(
            &self,
            ctx: &iac::ModuleContext<'_>,
        ) -> std::result::Result<Vec<Box<dyn iac::Resource>>, iac::IacError> {
            assert_eq!(
                ctx.extension::<iac::DestroyMode>().is_some(),
                self.expected,
                "DestroyMode visibility did not match expectation"
            );
            Ok(Vec::new())
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
        ) -> std::result::Result<deploy_engine::DesiredImageRef, deploy_engine::DeployError>
        {
            Ok(deploy_engine::DesiredImageRef {
                repository: "example".into(),
                tag: "latest".into(),
                upstream_ref: Some("example:latest".into()),
            })
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

    #[derive(Clone)]
    struct VersionedDeployment {
        backend: Arc<VersionedBackend>,
    }

    #[async_trait]
    impl Deployment for VersionedDeployment {
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
            vec![Box::new(ThreeResourceModule)]
        }

        fn services(&self, _config: &Self::Config) -> Vec<Box<dyn deploy_engine::Service>> {
            Vec::new()
        }

        fn images(&self, _config: &Self::Config) -> Vec<Box<dyn deploy_engine::Image>> {
            Vec::new()
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
            _deployment_dir: &Path,
        ) -> Box<dyn DeploymentStore<iac::InfraState>> {
            Box::new(CasStore::new(
                Box::new(SharedBackend(Arc::clone(&self.backend))),
                "infra".to_string(),
            ))
        }

        fn create_deploy_store(
            &self,
            _config: &Self::Config,
            _deployment_dir: &Path,
        ) -> Box<dyn DeploymentStore<iac::RuntimeState>> {
            Box::new(CasStore::new(
                Box::new(SharedBackend(Arc::clone(&self.backend))),
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

    #[derive(Debug)]
    struct ThreeResourceModule;

    impl iac::Module for ThreeResourceModule {
        fn name(&self) -> &str {
            "three"
        }

        fn dependencies(&self) -> &[&str] {
            &[]
        }

        fn resources(
            &self,
            _ctx: &iac::ModuleContext<'_>,
        ) -> std::result::Result<Vec<Box<dyn iac::Resource>>, iac::IacError> {
            Ok((0..3)
                .map(|idx| Box::new(NumberedResource(idx)) as Box<dyn iac::Resource>)
                .collect())
        }
    }

    #[derive(Debug)]
    struct NumberedResource(usize);

    #[async_trait]
    impl iac::Resource for NumberedResource {
        fn resource_type(&self) -> iac::ResourceType {
            iac::ResourceType::new("numbered")
        }

        fn resource_id(&self) -> iac::ResourceId {
            iac::ResourceId(format!("numbered-{}", self.0))
        }

        fn dependencies(&self) -> Vec<iac::ResourceId> {
            Vec::new()
        }

        fn module(&self) -> &str {
            "three"
        }

        async fn create(
            &self,
            _ctx: &iac::ProvisionContext,
        ) -> std::result::Result<iac::ResourceState, iac::IacError> {
            Ok(numbered_state(self.0))
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
            Ok(iac::DescribeResult::Absent)
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

    fn numbered_state(idx: usize) -> iac::ResourceState {
        iac::ResourceState {
            resource_type: iac::ResourceType::new("numbered"),
            physical_id: format!("phys-{idx}"),
            properties: serde_json::json!({}),
            dependencies: Vec::new(),
            created_at: String::new(),
            updated_at: String::new(),
            module: "three".into(),
        }
    }

    #[derive(Debug, Default)]
    struct VersionedBackend {
        inner: Mutex<VersionedBackendState>,
    }

    #[derive(Debug, Default)]
    struct VersionedBackendState {
        data: Option<Vec<u8>>,
        version: String,
        writes: usize,
        expected_versions: Vec<String>,
    }

    #[derive(Debug)]
    struct SharedBackend(Arc<VersionedBackend>);

    #[async_trait]
    impl StateBackend for SharedBackend {
        async fn read_manifest(
            &self,
            _key: &str,
        ) -> std::result::Result<Option<(Vec<u8>, String)>, StateError> {
            let state = self.0.inner.lock().expect("backend mutex");
            Ok(state
                .data
                .as_ref()
                .map(|data| (data.clone(), state.version.clone())))
        }

        async fn write_manifest(
            &self,
            _key: &str,
            data: &[u8],
            expected_version: &str,
        ) -> std::result::Result<(), StateError> {
            let mut state = self.0.inner.lock().expect("backend mutex");
            if state.version != expected_version {
                return Err(StateError::Conflict(format!(
                    "expected {}, got {expected_version}",
                    state.version
                )));
            }
            state.expected_versions.push(expected_version.to_string());
            state.writes += 1;
            state.version = format!("v{}", state.writes);
            state.data = Some(data.to_vec());
            Ok(())
        }

        async fn read_snapshot(&self, _key: &str) -> std::result::Result<Vec<u8>, StateError> {
            Err(StateError::NotFound("unused".into()))
        }

        async fn write_snapshot(
            &self,
            _key: &str,
            _data: &[u8],
        ) -> std::result::Result<(), StateError> {
            Ok(())
        }

        async fn list_snapshots(
            &self,
            _prefix: &str,
        ) -> std::result::Result<Vec<String>, StateError> {
            Ok(Vec::new())
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

    #[tokio::test]
    async fn destroy_mode_is_scoped_to_destroy_operations() {
        let temp = tempfile::tempdir().unwrap();
        let config = TestConfig;
        let mut engine = InfraEngine::new(TestDeployment, &config, temp.path())
            .await
            .unwrap();

        let plan_composition = iac::InfraComposition {
            desired_modules: vec![Box::new(DestroyModeAssertingModule {
                name: "plan",
                expected: false,
            })],
            known_modules: vec![Box::new(DestroyModeAssertingModule {
                name: "plan",
                expected: false,
            })],
            active_modules: vec!["plan".to_string()],
        };
        engine
            .plan(&plan_composition, iac::ModuleSelection::All)
            .await
            .unwrap();
        assert!(
            engine
                .provision_context_mut()
                .extension::<iac::DestroyMode>()
                .is_none()
        );

        let destroy_composition = iac::InfraComposition {
            desired_modules: Vec::new(),
            known_modules: vec![Box::new(DestroyModeAssertingModule {
                name: "destroy",
                expected: true,
            })],
            active_modules: vec!["destroy".to_string()],
        };
        engine
            .plan_destroy(&destroy_composition, iac::ModuleSelection::All)
            .await
            .unwrap();
        assert!(
            engine
                .provision_context_mut()
                .extension::<iac::DestroyMode>()
                .is_none()
        );
        engine
            .destroy(&destroy_composition, iac::ModuleSelection::All)
            .await
            .unwrap();
        assert!(
            engine
                .provision_context_mut()
                .extension::<iac::DestroyMode>()
                .is_none()
        );
    }

    #[tokio::test]
    async fn cas_saver_tracks_latest_version_across_incremental_saves() {
        let temp = tempfile::tempdir().unwrap();
        let backend = Arc::new(VersionedBackend::default());
        let config = TestConfig;
        let deployment = VersionedDeployment {
            backend: Arc::clone(&backend),
        };
        let mut engine = InfraEngine::new(deployment, &config, temp.path())
            .await
            .unwrap();
        let composition = iac::InfraComposition {
            desired_modules: vec![Box::new(ThreeResourceModule)],
            known_modules: vec![Box::new(ThreeResourceModule)],
            active_modules: vec!["three".to_string()],
        };

        let changes = engine
            .apply(&composition, iac::ModuleSelection::All)
            .await
            .unwrap();

        assert_eq!(
            changes
                .iter()
                .filter(|change| change.kind == iac::ChangeKind::Create)
                .count(),
            3
        );
        let state = backend.inner.lock().expect("backend mutex");
        assert_eq!(state.expected_versions, ["", "v1", "v2"]);
        assert_eq!(state.version, "v3");
    }
}
