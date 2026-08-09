//! The framework's one `orchestrator::Deployment` on the bound path.
//!
//! [`DescribedDeployment`] derives every answer from its inputs — the
//! execution state (the realized definition), the admitted deployment
//! coordinates, and the declaration's extension constructors. It owns no
//! platform knowledge: no module names, no provider handles, no attribute
//! meanings. Registration happens inside `register_infra_extensions` and
//! nowhere else — the deployment runs each selection's constructor with
//! its namespace block, the constructors put handles into the context, and
//! resources read the context at the mechanics moment.

use std::{path::Path, sync::Arc};

use async_trait::async_trait;
use tokeira_deploy_engine as deploy_engine;
use tokeira_iac::{self as iac, ResourceId};
use tokeira_orchestrator as orchestrator;
use tokeira_platform::{
    declaration::{DeploymentRef, InfraConstructor},
    graph::WritebackValue,
};
use tokeira_state::{CasStore, DeploymentStore, LocalBackend};

use crate::engine::{ExecutionState, ModuleSpec};

/// The bound path's deployment: graph answers from the execution state,
/// registration from the declaration's constructors, coordinates from the
/// admitted value.
#[derive(Debug)]
pub(crate) struct DescribedDeployment {
    deployment: DeploymentRef,
    constructors: Vec<(&'static str, Arc<dyn InfraConstructor>)>,
}

impl DescribedDeployment {
    pub(crate) fn new(
        deployment: DeploymentRef,
        constructors: Vec<(&'static str, Arc<dyn InfraConstructor>)>,
    ) -> Self {
        Self {
            deployment,
            constructors,
        }
    }

    fn module(config: &ExecutionState, spec: &ModuleSpec) -> Box<dyn iac::Module> {
        Box::new(ConcreteModule {
            spec: spec.clone(),
            // Entries the workload plane claimed leave the infra plane at
            // this derivation — one engine owns each workload, never both.
            // The state keeps them so recorded-state paths still see them.
            resources: config
                .resources
                .get(&spec.name)
                .map(|resources| {
                    resources
                        .iter()
                        .filter(|resource| !config.claimed.contains(&resource.resource_id()))
                        .cloned()
                        .collect()
                })
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

    /// The workload plane, modelled on `all_infra`: the provider's
    /// projected service models from the execution state, handed to the
    /// engine as delegating boxes over the shared projections.
    fn all_services(config: &ExecutionState) -> Vec<Box<dyn deploy_engine::Service>> {
        config
            .services
            .iter()
            .cloned()
            .map(|service| Box::new(SharedService(service)) as Box<dyn deploy_engine::Service>)
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
        Self::all_services(config)
    }

    fn images(&self, _config: &Self::Config) -> Vec<Box<dyn deploy_engine::Image>> {
        Vec::new()
    }

    fn required_namespaces(&self, config: &Self::Config) -> Vec<String> {
        config.namespaces.clone()
    }

    /// THE registration seam: sets the context's standard fields, then runs
    /// every declared selection's infra constructor with its namespace
    /// block. A constructor failure is an error — real, or the unreachable
    /// class arriving after a passing probe, typed at the error root.
    async fn register_infra_extensions(
        &self,
        config: &Self::Config,
        ctx: &mut iac::ProvisionContext,
    ) -> orchestrator::Result<()> {
        ctx.project_name = self.deployment.name.clone();
        // ctx.tags stays default: the definition graph carries no authored
        // tags, and nothing may invent them here.
        for (namespace, constructor) in &self.constructors {
            constructor
                .construct(&self.deployment, config.attributes.get(*namespace), ctx)
                .await?;
        }
        Ok(())
    }

    // No declared selection carries a deploy-phase constructor.
    async fn register_deploy_extensions(
        &self,
        _config: &Self::Config,
        _ctx: &mut deploy_engine::ServiceContext,
    ) -> orchestrator::Result<()> {
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

/// Delegating wrapper: the deploy engine takes owned service boxes while
/// the execution state keeps the shared projected models, so each operation
/// hands out delegates rather than re-projecting.
struct SharedService(Arc<dyn deploy_engine::Service>);

impl std::fmt::Debug for SharedService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("SharedService")
            .finish_non_exhaustive()
    }
}

impl deploy_engine::Service for SharedService {
    fn name(&self) -> &str {
        self.0.name()
    }

    fn module(&self) -> &str {
        self.0.module()
    }

    fn dependencies(&self) -> Vec<&str> {
        self.0.dependencies()
    }

    fn manifests(
        &self,
        ctx: &deploy_engine::ServiceContext,
    ) -> Result<Vec<serde_json::Value>, deploy_engine::RuntimeError> {
        self.0.manifests(ctx)
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

    /// A constructor that records its invocation and drops a marker into
    /// the context, so the test can see both the call order and the block
    /// each selection received.
    #[derive(Debug)]
    struct Recording {
        namespace: &'static str,
        log: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl InfraConstructor for Recording {
        async fn construct(
            &self,
            deployment: &DeploymentRef,
            attributes: Option<&serde_json::Value>,
            ctx: &mut iac::ProvisionContext,
        ) -> anyhow::Result<()> {
            self.log.lock().unwrap().push(format!(
                "{}:{}:{}",
                self.namespace,
                deployment.name,
                attributes
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "none".to_string())
            ));
            ctx.set_extension(Marker);
            Ok(())
        }
    }

    struct Marker;

    fn execution_state(attributes: BTreeMap<String, serde_json::Value>) -> ExecutionState {
        ExecutionState {
            modules: Vec::new(),
            bootstrap: String::new(),
            resources: BTreeMap::new(),
            namespaces: Vec::new(),
            writeback: Vec::new(),
            index: Default::default(),
            manifests: BTreeMap::new(),
            attributes,
            claimed: Default::default(),
            services: Vec::new(),
        }
    }

    #[derive(Debug)]
    struct FixedResource {
        id: &'static str,
        module: &'static str,
    }

    #[async_trait]
    impl iac::Resource for FixedResource {
        fn resource_type(&self) -> iac::ResourceType {
            iac::ResourceType::new("fixed")
        }

        fn resource_id(&self) -> ResourceId {
            ResourceId(self.id.to_string())
        }

        fn dependencies(&self) -> Vec<ResourceId> {
            Vec::new()
        }

        fn module(&self) -> &str {
            self.module
        }

        async fn create(
            &self,
            _context: &iac::ProvisionContext,
        ) -> Result<iac::ResourceState, iac::IacError> {
            unreachable!("the partition test never provisions")
        }

        async fn update(
            &self,
            _current: &iac::ResourceState,
            _context: &iac::ProvisionContext,
        ) -> Result<iac::ResourceState, iac::IacError> {
            unreachable!("the partition test never provisions")
        }

        async fn delete(
            &self,
            _current: &iac::ResourceState,
            _context: &iac::ProvisionContext,
        ) -> Result<(), iac::IacError> {
            unreachable!("the partition test never provisions")
        }

        async fn describe(
            &self,
            _context: &iac::ProvisionContext,
        ) -> Result<iac::DescribeResult, iac::IacError> {
            unreachable!("the partition test never provisions")
        }

        fn diff(
            &self,
            _current: &iac::ResourceState,
            _context: &iac::ProvisionContext,
        ) -> iac::InternalChange {
            iac::InternalChange::NoChange {
                resource_id: self.resource_id(),
            }
        }

        fn change_semantics(&self, _context: &iac::SemanticsContext<'_>) -> iac::ChangeSemantics {
            iac::ChangeSemantics::default()
        }
    }

    #[derive(Debug)]
    struct FixedWorkload;

    impl deploy_engine::Service for FixedWorkload {
        fn name(&self) -> &str {
            "tokeirad"
        }

        fn module(&self) -> &str {
            "runtime"
        }

        fn dependencies(&self) -> Vec<&str> {
            Vec::new()
        }

        fn manifests(
            &self,
            _ctx: &deploy_engine::ServiceContext,
        ) -> Result<Vec<serde_json::Value>, deploy_engine::RuntimeError> {
            Ok(vec![serde_json::json!({"name": "tokeirad"})])
        }
    }

    // The partition at derivation: claimed entries leave the infra modules,
    // the projected services answer `services()` — one engine owns each
    // workload, never both, while the state keeps every realized resource.
    #[test]
    fn the_partition_splits_the_planes() {
        let mut state = execution_state(BTreeMap::new());
        state.modules = vec![
            crate::engine::ModuleSpec {
                name: "local_state".to_string(),
                dependencies: Vec::new(),
            },
            crate::engine::ModuleSpec {
                name: "runtime".to_string(),
                dependencies: vec!["local_state".to_string()],
            },
        ];
        state.bootstrap = "local_state".to_string();
        state.resources.insert(
            "runtime".to_string(),
            vec![
                Arc::new(FixedResource {
                    id: "runtime/config",
                    module: "runtime",
                }) as Arc<dyn iac::Resource>,
                Arc::new(FixedResource {
                    id: "compose/tokeirad",
                    module: "runtime",
                }),
            ],
        );
        state
            .claimed
            .insert(ResourceId("compose/tokeirad".to_string()));
        state.services = vec![Arc::new(FixedWorkload) as Arc<dyn deploy_engine::Service>];

        let described = DescribedDeployment::new(
            DeploymentRef {
                name: "demo".to_string(),
                dir: "/tmp/demo".into(),
            },
            Vec::new(),
        );

        let modules = described.infra_modules(&state, &iac::ModuleSelection::All);
        assert_eq!(modules.len(), 1, "the bootstrap is excluded as ever");
        let infra_state = iac::InfraState::default();
        let extensions = std::collections::HashMap::new();
        let ctx = iac::ModuleContext::new(&infra_state, &extensions);
        let remaining: Vec<ResourceId> = modules[0]
            .resources(&ctx)
            .unwrap()
            .iter()
            .map(|resource| resource.resource_id())
            .collect();
        assert_eq!(
            remaining,
            vec![ResourceId("runtime/config".to_string())],
            "the claimed entry left the infra plane"
        );

        let services = described.services(&state);
        assert_eq!(services.len(), 1);
        assert_eq!(services[0].name(), "tokeirad");
        assert_eq!(services[0].module(), "runtime");
    }

    // The registration contract: project_name from the admitted
    // coordinates, every constructor run in declaration order, each
    // receiving exactly its own namespace block, handles landing in the
    // extension bag.
    #[tokio::test]
    async fn register_infra_runs_each_constructor_with_its_block() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let described = DescribedDeployment::new(
            DeploymentRef {
                name: "demo".to_string(),
                dir: "/tmp/demo".into(),
            },
            vec![
                (
                    "compose",
                    Arc::new(Recording {
                        namespace: "compose",
                        log: log.clone(),
                    }) as Arc<dyn InfraConstructor>,
                ),
                (
                    "aws",
                    Arc::new(Recording {
                        namespace: "aws",
                        log: log.clone(),
                    }),
                ),
            ],
        );
        let config = execution_state(BTreeMap::from([(
            "aws".to_string(),
            serde_json::json!({"region": "eu-west-2"}),
        )]));
        let mut ctx = iac::ProvisionContext::default();
        described
            .register_infra_extensions(&config, &mut ctx)
            .await
            .unwrap();
        assert_eq!(ctx.project_name, "demo");
        assert!(ctx.extension::<Marker>().is_some());
        assert_eq!(
            *log.lock().unwrap(),
            vec![
                "compose:demo:none".to_string(),
                r#"aws:demo:{"region":"eu-west-2"}"#.to_string(),
            ]
        );
    }
}
