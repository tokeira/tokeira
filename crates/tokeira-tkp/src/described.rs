//! The framework's one `orchestrator::Deployment` on the bound path.
//!
//! [`DescribedDeployment`] derives every answer from its inputs — the
//! execution state (the realized definition), the admitted deployment
//! coordinates, and the declaration's platform implementation. It owns no
//! platform knowledge beyond the framework's standard AWS execution context:
//! no module names and no platform-specific attribute meanings. Registration
//! happens inside `register_infra_extensions` and nowhere else — when the
//! declaration includes AWS, the framework installs its clients with the
//! authored platform default, then delegates additional handles to the
//! platform implementation. Resources read those handles from the context at
//! the mechanics moment.

use std::{path::Path, sync::Arc};

use async_trait::async_trait;
use tokeira_deploy_engine as deploy_engine;
use tokeira_iac::{self as iac, ResourceId};
use tokeira_orchestrator as orchestrator;
use tokeira_platform::{
    declaration::{DeploymentRef, PlatformIntegration},
    graph::WritebackValue,
};
use tokeira_state::{CasStore, DeploymentStore, LocalBackend};

use crate::engine::{ExecutionState, ModuleSpec};

/// The bound path's deployment: graph answers from the execution state,
/// registration from the platform implementation, coordinates from the
/// admitted value.
#[derive(Debug)]
pub(crate) struct DescribedDeployment {
    deployment: DeploymentRef,
    implementation: Arc<dyn PlatformIntegration>,
}

impl DescribedDeployment {
    pub(crate) fn new(
        deployment: DeploymentRef,
        implementation: Arc<dyn PlatformIntegration>,
    ) -> Self {
        Self {
            deployment,
            implementation,
        }
    }

    fn module(config: &ExecutionState, spec: &ModuleSpec) -> Box<dyn iac::Module> {
        Box::new(ConcreteModule {
            spec: spec.clone(),
            resources: config
                .resources
                .get(&spec.name)
                .cloned()
                .unwrap_or_default(),
        })
    }

    fn all_infra(config: &ExecutionState) -> Vec<Box<dyn iac::Module>> {
        config
            .modules
            .iter()
            .filter(|module| module.name != config.bootstrap)
            .map(|module| Self::module(config, module))
            .collect()
    }
}

#[async_trait]
impl orchestrator::Deployment for DescribedDeployment {
    type Config = ExecutionState;

    fn remote_state_module(
        &self,
        config: &Self::Config,
        _deployment_dir: &Path,
    ) -> Box<dyn iac::Module> {
        let bootstrap = config
            .modules
            .iter()
            .find(|module| module.name == config.bootstrap)
            .expect("the execution state names a bootstrap module it contains");
        Self::module(config, bootstrap)
    }

    fn infra_modules(
        &self,
        config: &Self::Config,
        selection: &iac::ModuleSelection,
    ) -> Vec<Box<dyn iac::Module>> {
        let all = Self::all_infra(config);
        // Verb entries validate and expand the operator's filter before any
        // selection reaches here, so this re-expansion is idempotent for
        // valid input; the fallback exists only because this trait method
        // cannot return an error.
        let expanded =
            iac::expand_module_selection(&all, selection, iac::SelectionDirection::Prerequisites)
                .unwrap_or_else(|_| selection.clone());
        all.into_iter()
            .filter(|module| expanded.includes(module.name()))
            .collect()
    }

    fn services(&self, config: &Self::Config) -> Vec<Box<dyn deploy_engine::Service>> {
        config
            .services
            .iter()
            .map(|service| {
                Box::new(ConcreteService {
                    service: Arc::clone(service),
                }) as Box<dyn deploy_engine::Service>
            })
            .collect()
    }

    fn images(&self, _config: &Self::Config) -> Vec<Box<dyn deploy_engine::Image>> {
        Vec::new()
    }

    fn required_namespaces(&self, config: &Self::Config) -> Vec<String> {
        config.namespaces.clone()
    }

    /// THE registration seam: when the declaration includes AWS, installs
    /// its standard clients with the authored platform default, then delegates
    /// platform-specific extensions. An integration failure is an error —
    /// real, or the unreachable class arriving after a passing probe, typed at
    /// the error root.
    async fn register_infra_extensions(
        &self,
        config: &Self::Config,
        ctx: &mut iac::ProvisionContext,
    ) -> orchestrator::Result<()> {
        ctx.project_name = self.deployment.name.clone();
        // ctx.tags stays default: the definition graph carries no authored
        // tags, and nothing may invent them here.
        if config.uses_aws {
            tokeira_aws::register_infra_extensions(config.aws_region.as_deref(), ctx).await;
        }
        self.implementation
            .register_infra_extensions(&self.deployment, ctx)
            .await?;
        Ok(())
    }

    async fn register_deploy_extensions(
        &self,
        _config: &Self::Config,
        ctx: &mut deploy_engine::ServiceContext,
    ) -> orchestrator::Result<()> {
        self.implementation
            .register_deploy_extensions(&self.deployment, ctx)
            .await?;
        Ok(())
    }

    async fn register_image_extensions(
        &self,
        _config: &Self::Config,
        ctx: &mut deploy_engine::ImageContext,
    ) -> orchestrator::Result<()> {
        self.implementation
            .register_image_extensions(&self.deployment, ctx)
            .await?;
        Ok(())
    }

    fn create_infra_store(
        &self,
        _config: &Self::Config,
        deployment_dir: &Path,
    ) -> Box<dyn DeploymentStore<iac::InfraState>> {
        infra_store(deployment_dir)
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

    // Identity: config projection is owned by the declared writeback and
    // its output references, not by hydration.
    fn hydrate_config(&self, config: &Self::Config, _state: &iac::InfraState) -> Self::Config {
        config.clone()
    }

    /// Declared writeback resolved against recorded state: literals pass
    /// through as written; output references resolve through the realized
    /// index into the applied resources' recorded outputs.
    fn collect_writeback(
        &self,
        config: &Self::Config,
        state: &iac::InfraState,
    ) -> Vec<(String, String)> {
        config
            .writeback
            .iter()
            .filter_map(|entry| {
                let value = match entry.value() {
                    WritebackValue::Literal(value) => Some(value.clone()),
                    WritebackValue::Output(output) => {
                        let resource = output.resource();
                        let id = config.index.get(resource.module(), resource.logical_id())?;
                        state
                            .resources
                            .get(id)?
                            .properties
                            .get(output.output())?
                            .as_str()
                            .map(str::to_string)
                    }
                }?;
                Some((entry.key().to_string(), value))
            })
            .collect()
    }
}

/// Owned deployment hand-off over a shared definition-realized service.
///
/// `Deployment::services` returns owned trait objects while `ExecutionState`
/// is cloneable and may be opened independently by plan and apply. This
/// adapter changes neither identity nor lifecycle; it only delegates the
/// service contract to the exact realized value retained by the definition.
#[derive(Debug)]
struct ConcreteService {
    service: Arc<dyn deploy_engine::Service>,
}

impl deploy_engine::Service for ConcreteService {
    fn resource_type(&self) -> &'static str {
        self.service.resource_type()
    }

    fn validate_input(&self) -> Result<(), String> {
        self.service.validate_input()
    }

    fn declared_outputs(&self) -> &'static [&'static str] {
        self.service.declared_outputs()
    }

    fn name(&self) -> &str {
        self.service.name()
    }

    fn module(&self) -> &str {
        self.service.module()
    }

    fn dependencies(&self) -> Vec<&str> {
        self.service.dependencies()
    }

    fn manifests(
        &self,
        ctx: &deploy_engine::ServiceContext,
    ) -> Result<Vec<serde_json::Value>, deploy_engine::RuntimeError> {
        self.service.manifests(ctx)
    }
}

/// The framework-standard infra state store: CAS over the deployment's
/// `state/infra` directory.
pub(crate) fn infra_store(deployment_dir: &Path) -> Box<dyn DeploymentStore<iac::InfraState>> {
    Box::new(CasStore::new(
        Box::new(LocalBackend::new(deployment_dir.join("state/infra"))),
        "infra".to_string(),
    ))
}

/// A graph module realized as an engine module: the spec's identity and
/// edges, the realized resources by ownership.
#[derive(Clone)]
struct ConcreteModule {
    spec: ModuleSpec,
    resources: Vec<Arc<dyn iac::Resource>>,
}

impl std::fmt::Debug for ConcreteModule {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConcreteModule")
            .field("name", &self.spec.name)
            .field("dependencies", &self.spec.dependencies)
            .field("resource_count", &self.resources.len())
            .finish()
    }
}

impl iac::Module for ConcreteModule {
    fn name(&self) -> &str {
        &self.spec.name
    }

    fn dependencies(&self) -> Vec<&str> {
        self.spec.dependencies.iter().map(String::as_str).collect()
    }

    fn resources(
        &self,
        _context: &iac::ModuleContext<'_>,
    ) -> Result<Vec<Box<dyn iac::Resource>>, iac::IacError> {
        Ok(self
            .resources
            .iter()
            .cloned()
            .map(|resource| Box::new(SharedResource(resource)) as Box<dyn iac::Resource>)
            .collect())
    }
}

/// Delegating wrapper: the engine takes owned resource boxes while the
/// execution state keeps shared realized resources, so each operation hands
/// out delegates rather than cloning realizations.
struct SharedResource(Arc<dyn iac::Resource>);

impl std::fmt::Debug for SharedResource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("SharedResource")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl iac::Resource for SharedResource {
    fn resource_type(&self) -> iac::ResourceType {
        self.0.resource_type()
    }

    fn resource_id(&self) -> ResourceId {
        self.0.resource_id()
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        self.0.dependencies()
    }

    fn describes(&self) -> bool {
        self.0.describes()
    }

    fn module(&self) -> &str {
        self.0.module()
    }

    async fn create(
        &self,
        context: &iac::ProvisionContext,
    ) -> Result<iac::ResourceState, iac::IacError> {
        self.0.create(context).await
    }

    async fn update(
        &self,
        current: &iac::ResourceState,
        context: &iac::ProvisionContext,
    ) -> Result<iac::ResourceState, iac::IacError> {
        self.0.update(current, context).await
    }

    async fn delete(
        &self,
        current: &iac::ResourceState,
        context: &iac::ProvisionContext,
    ) -> Result<(), iac::IacError> {
        self.0.delete(current, context).await
    }

    async fn describe(
        &self,
        context: &iac::ProvisionContext,
    ) -> Result<iac::DescribeResult, iac::IacError> {
        self.0.describe(context).await
    }

    fn diff(
        &self,
        current: &iac::ResourceState,
        context: &iac::ProvisionContext,
    ) -> iac::InternalChange {
        self.0.diff(current, context)
    }

    fn change_semantics(&self, context: &iac::SemanticsContext<'_>) -> iac::ChangeSemantics {
        self.0.change_semantics(context)
    }

    fn display_kind(&self) -> Option<&'static str> {
        self.0.display_kind()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };

    use tokeira_orchestrator::Deployment as _;

    use super::*;

    /// A platform implementation that records registration and drops a marker
    /// into the context without receiving frontend namespace data.
    #[derive(Debug)]
    struct Recording {
        namespace: &'static str,
        log: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl PlatformIntegration for Recording {
        async fn register_infra_extensions(
            &self,
            deployment: &DeploymentRef,
            ctx: &mut iac::ProvisionContext,
        ) -> anyhow::Result<()> {
            self.log
                .lock()
                .unwrap()
                .push(format!("{}:{}", self.namespace, deployment.name));
            ctx.set_extension(Marker);
            Ok(())
        }

        async fn register_deploy_extensions(
            &self,
            _deployment: &DeploymentRef,
            _ctx: &mut deploy_engine::ServiceContext,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        async fn register_image_extensions(
            &self,
            _deployment: &DeploymentRef,
            _ctx: &mut deploy_engine::ImageContext,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        fn service_platform(
            &self,
            _deployment: &DeploymentRef,
        ) -> anyhow::Result<Box<dyn deploy_engine::Platform>> {
            Ok(Box::new(NoServices))
        }
    }

    struct Marker;

    #[derive(Debug)]
    struct NoServices;

    #[async_trait]
    impl deploy_engine::Platform for NoServices {
        async fn apply_manifests(
            &self,
            _manifests: &[serde_json::Value],
        ) -> Result<usize, deploy_engine::RuntimeError> {
            unreachable!("the registration test declares no services")
        }
    }

    fn execution_state() -> ExecutionState {
        ExecutionState {
            uses_aws: true,
            aws_region: Some("eu-west-2".to_string()),
            modules: Vec::new(),
            bootstrap: String::new(),
            resources: BTreeMap::new(),
            services: Vec::new(),
            namespaces: Vec::new(),
            writeback: Vec::new(),
            index: Default::default(),
            manifests: BTreeMap::new(),
            configuration_identity: tokeira_platform::definition::ConfigurationIdentity::compute(
                &tokeira_orchestrator::DefinitionFormatId::new("tkd").expect("format"),
                b"probe",
            ),
            served_companions: Vec::new(),
        }
    }

    // The registration contract: standard AWS clients and project identity
    // arrive before platform-specific registration, without frontend
    // namespace data crossing the seam.
    #[tokio::test]
    async fn register_infra_delegates_without_namespace_data() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let described = DescribedDeployment::new(
            DeploymentRef {
                name: "demo".to_string(),
                dir: "/tmp/demo".into(),
            },
            Arc::new(Recording {
                namespace: "compose",
                log: log.clone(),
            }),
        );
        let config = execution_state();
        let mut ctx = iac::ProvisionContext::default();
        described
            .register_infra_extensions(&config, &mut ctx)
            .await
            .unwrap();
        assert_eq!(ctx.project_name, "demo");
        assert_eq!(
            ctx.extension::<tokeira_aws::AwsClients>()
                .unwrap()
                .dsql
                .config()
                .region()
                .map(|region| region.as_ref()),
            Some("eu-west-2")
        );
        assert!(ctx.extension::<Marker>().is_some());
        assert_eq!(*log.lock().unwrap(), vec!["compose:demo".to_string()]);
    }
}
