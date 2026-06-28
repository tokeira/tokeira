//! The `Deployment`/`Ops` implementation that lets the orchestrator drive a
//! compose-dsl deployment.
//!
//! The DSL [`Composition`] is translated into a flat **plan** ([`ComposeDslConfig`])
//! once at load, where translation errors surface cleanly. The trait methods —
//! which return `Vec`, not `Result` — then read the plan infallibly and build
//! the concrete `iac` modules/resources and deploy-engine services, reusing the
//! real [`ComposeService`]/[`ComposePlatform`] kinds unchanged.
//!
//! Scope (in-memory compose case): compose services + the bootstrap local-state
//! module. AWS/templated infra resources (DSQL, DynamoDB, observability config
//! files), image declarations, and writeback resolution are honest follow-ups
//! and are skipped here (a definition using them lowers, but those items are not
//! yet realized into engine resources).

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tokeira_compose::{ComposePlatform, ComposeService};
use tokeira_compose_deployment::observability_config::{
    ObservabilityConfigFilesResource, ObservabilityParams,
};
use tokeira_deploy_engine::{self as deploy_engine, DeployError};
use tokeira_iac::{
    self as iac, IacError, ProvisionContext, ResourceId, ResourceState, ResourceType,
};
use tokeira_orchestrator::{Deployment, Ops, PortMapping, Result, ServiceReplicas};
use tokeira_platform_dsl::{Composition, ItemRole, RuntimeContext, value::LoweredItem};
use tokeira_state::{LocalBackend, StateBackend};

use crate::{
    DslError, compile_deployment, compose_service_from, optional_str_field, replicas_of,
    required_str_field, required_u16_field,
};

/// Module name of the bootstrap local-state module, prepended by the engine and
/// referenced from definitions via `depends_on [ local_state ]`.
const LOCAL_STATE_MODULE: &str = "local_state";

/// One realized item in a module's plan.
///
/// Built once at config load (where translation errors surface), then rebuilt
/// into concrete `iac` resources by the trait methods. `ObservabilityParams`
/// and `ComposeService` are both `Clone`, so the plan is `Clone` as the
/// `Deployment::Config` bound requires.
#[derive(Debug, Clone)]
enum ItemPlan {
    /// A compose service (→ infra resource + deploy-engine service).
    Service {
        service: ComposeService,
        replicas: u32,
    },
    /// The observability config-files resource (renders mimir/loki/grafana/alloy
    /// config from typed params). Realized via the compose platform's existing
    /// `ObservabilityConfigFilesResource`.
    Observability {
        id: String,
        params: ObservabilityParams,
        depends_on: Vec<String>,
    },
}

/// One module in the plan: its name, dependency edges, and realized items.
#[derive(Debug, Clone)]
struct ModulePlan {
    name: String,
    depends_on: Vec<String>,
    items: Vec<ItemPlan>,
}

/// The loaded, translated configuration for a compose-dsl deployment.
///
/// Built once from the compiled [`Composition`]; the trait methods read it.
#[derive(Debug, Clone)]
pub struct ComposeDslConfig {
    /// The deployment's on-disk root.
    pub deployment_dir: PathBuf,
    /// The compose project name (container/label prefix).
    pub project_name: String,
    modules: Vec<ModulePlan>,
}

impl ComposeDslConfig {
    /// Compile and translate `<deployment_dir>/compose.platform` into a plan.
    ///
    /// Translation errors (a service field of the wrong shape) surface here,
    /// where a `Result` is available, rather than inside the infallible trait
    /// methods.
    pub fn load(
        deployment_dir: &Path,
        project_name: impl Into<String>,
        ctx: &RuntimeContext,
    ) -> std::result::Result<Self, DslError> {
        let composition = compile_deployment(deployment_dir, ctx)?;
        Self::from_composition(deployment_dir, project_name, &composition)
    }

    /// Build a plan from an already-compiled composition (used by tests).
    pub fn from_composition(
        deployment_dir: &Path,
        project_name: impl Into<String>,
        composition: &Composition,
    ) -> std::result::Result<Self, DslError> {
        let project_name = project_name.into();
        let mut modules = Vec::new();
        for module in &composition.modules {
            // The bootstrap local-state module is provided by the engine; skip a
            // same-named module from the definition to keep module names unique.
            if module.name == LOCAL_STATE_MODULE {
                continue;
            }
            let mut items = Vec::new();
            for item in &module.items {
                match (item.role, item.kind.as_str()) {
                    (ItemRole::Service, "ComposeService") => items.push(ItemPlan::Service {
                        service: compose_service_from(item)?,
                        replicas: replicas_of(item),
                    }),
                    (ItemRole::Resource, "ObservabilityConfigFiles") => {
                        items.push(ItemPlan::Observability {
                            id: item.id.clone(),
                            params: observability_params_from(item, &project_name)?,
                            depends_on: item.depends_on.clone(),
                        });
                    }
                    // DSQL / DynamoDB / image kinds are not yet realized (tasks 9.2–9.3).
                    _ => {}
                }
            }
            modules.push(ModulePlan {
                name: module.name.clone(),
                depends_on: module.depends_on.clone(),
                items,
            });
        }
        Ok(Self {
            deployment_dir: deployment_dir.to_path_buf(),
            project_name,
            modules,
        })
    }

    fn all_service_names(&self) -> Vec<String> {
        self.modules
            .iter()
            .flat_map(|m| m.items.iter())
            .filter_map(|item| match item {
                ItemPlan::Service { service, .. } => Some(service.name.clone()),
                _ => None,
            })
            .collect()
    }
}

/// The compose-dsl platform.
///
/// Holds the deployment's valid service names (leaked to `'static` to satisfy
/// the `Ops::valid_services` signature), derived from the plan at construction.
#[derive(Debug, Default)]
pub struct ComposeDslDeployment {
    valid_services: Vec<&'static str>,
}

impl ComposeDslDeployment {
    /// Construct from a loaded config, capturing its service names for `Ops`.
    pub fn new(config: &ComposeDslConfig) -> Self {
        Self {
            valid_services: leak_strs(config.all_service_names()),
        }
    }
}

#[async_trait]
impl Deployment for ComposeDslDeployment {
    type Config = ComposeDslConfig;

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
        config
            .modules
            .iter()
            .filter(|module| selection.includes(&module.name))
            .map(|module| {
                Box::new(DslModule {
                    name: module.name.clone(),
                    depends_on: leak_strs(module.depends_on.clone()),
                    deployment_dir: config.deployment_dir.clone(),
                    items: module.items.clone(),
                }) as Box<dyn iac::Module>
            })
            .collect()
    }

    fn services(&self, config: &Self::Config) -> Vec<Box<dyn deploy_engine::Service>> {
        config
            .modules
            .iter()
            .flat_map(|module| {
                module.items.iter().filter_map(move |item| match item {
                    ItemPlan::Service { service, .. } => Some(Box::new(DslComposeWorkload {
                        service: service.clone(),
                        module: leak_str(&module.name),
                        deps: leak_strs(service.depends_on.clone()),
                    })
                        as Box<dyn deploy_engine::Service>),
                    ItemPlan::Observability { .. } => None,
                })
            })
            .collect()
    }

    fn images(&self, _config: &Self::Config) -> Vec<Box<dyn deploy_engine::Image>> {
        // Image declarations (Build/Mirror) are not yet realized — follow-up.
        Vec::new()
    }

    fn required_namespaces(&self, _config: &Self::Config) -> Vec<String> {
        vec!["default".into()]
    }

    async fn register_infra_extensions(
        &self,
        config: &Self::Config,
        ctx: &mut ProvisionContext,
    ) -> Result<()> {
        // ComposeService::create reads the ComposePlatform handle from the
        // context; register it here (the host-edge effect).
        let compose_file = config.deployment_dir.join("docker-compose.yml");
        let platform = ComposePlatform::connect(&compose_file, &config.project_name)
            .map_err(|err| anyhow::anyhow!("failed to connect compose platform: {err}"))?;
        ctx.project_name = config.project_name.clone();
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
        // In-memory compose has no state→config hydration (no DSQL endpoint
        // writeback yet); return the config unchanged.
        config.clone()
    }

    fn collect_writeback(
        &self,
        _config: &Self::Config,
        _state: &iac::InfraState,
    ) -> Vec<(String, String)> {
        // Writeback resolution from provisioned state is a follow-up.
        Vec::new()
    }
}

#[async_trait]
impl Ops for ComposeDslDeployment {
    type Config = ComposeDslConfig;

    fn valid_services(&self) -> &[&str] {
        &self.valid_services
    }

    fn desired_replicas(&self, config: &Self::Config) -> Vec<ServiceReplicas> {
        config
            .modules
            .iter()
            .flat_map(|module| module.items.iter())
            .filter_map(|item| match item {
                ItemPlan::Service { service, replicas } => Some(ServiceReplicas {
                    service: service.name.clone(),
                    replicas: *replicas,
                }),
                ItemPlan::Observability { .. } => None,
            })
            .collect()
    }

    async fn scale_up(&self, _service: &str, _replicas: u32, _config: &Self::Config) -> Result<()> {
        Err(anyhow::anyhow!("scale is not yet implemented for compose-dsl").into())
    }

    async fn scale_down(
        &self,
        _service: &str,
        _replicas: u32,
        _config: &Self::Config,
    ) -> Result<()> {
        Err(anyhow::anyhow!("scale is not yet implemented for compose-dsl").into())
    }

    async fn logs(&self, _service: &str, _config: &Self::Config) -> Result<Vec<String>> {
        Err(anyhow::anyhow!("logs are not yet implemented for compose-dsl").into())
    }

    async fn port_mappings(
        &self,
        _service: &str,
        _config: &Self::Config,
    ) -> Result<Vec<PortMapping>> {
        Err(anyhow::anyhow!("port mappings are not yet implemented for compose-dsl").into())
    }
}

// ── iac modules / resources ───────────────────────────────────────────

/// The bootstrap module that creates the deployment's local state directory.
#[derive(Debug)]
struct LocalStateModule {
    state_dir: PathBuf,
}

impl iac::Module for LocalStateModule {
    fn name(&self) -> &str {
        LOCAL_STATE_MODULE
    }

    fn dependencies(&self) -> &[&str] {
        &[]
    }

    fn resources(
        &self,
        _ctx: &iac::ModuleContext<'_>,
    ) -> std::result::Result<Vec<Box<dyn iac::Resource>>, IacError> {
        Ok(vec![Box::new(LocalStateDirResource {
            dir: self.state_dir.clone(),
        })])
    }
}

/// A DSL module: rebuilds its items into concrete `iac` resources, each wrapped
/// to carry the DSL-declared id, owning module, and dependency edges.
#[derive(Debug)]
struct DslModule {
    name: String,
    depends_on: Vec<&'static str>,
    deployment_dir: PathBuf,
    items: Vec<ItemPlan>,
}

impl iac::Module for DslModule {
    fn name(&self) -> &str {
        &self.name
    }

    fn dependencies(&self) -> &[&str] {
        &self.depends_on
    }

    fn resources(
        &self,
        _ctx: &iac::ModuleContext<'_>,
    ) -> std::result::Result<Vec<Box<dyn iac::Resource>>, IacError> {
        Ok(self
            .items
            .iter()
            .map(|item| {
                let (inner, id, deps): (Box<dyn iac::Resource>, String, Vec<String>) = match item {
                    ItemPlan::Service { service, .. } => (
                        Box::new(service.clone()),
                        service.name.clone(),
                        service.depends_on.clone(),
                    ),
                    ItemPlan::Observability {
                        id,
                        params,
                        depends_on,
                    } => (
                        Box::new(ObservabilityConfigFilesResource::new(
                            self.deployment_dir.clone(),
                            params.clone(),
                        )),
                        id.clone(),
                        depends_on.clone(),
                    ),
                };
                Box::new(DslOwnedResource {
                    inner,
                    id: ResourceId(format!("compose/{id}")),
                    module: self.name.clone(),
                    deps: deps
                        .into_iter()
                        .map(|dep| ResourceId(format!("compose/{dep}")))
                        .collect(),
                }) as Box<dyn iac::Resource>
            })
            .collect())
    }
}

/// Wraps a concrete compose resource, imposing the DSL-declared resource id,
/// owning module, and dependency edges so that ids and `depends_on` references
/// across kinds key consistently (`compose/<declared-id>`). Delegates the
/// provider lifecycle to the inner resource, stamping the module + deps onto the
/// persisted state.
struct DslOwnedResource {
    inner: Box<dyn iac::Resource>,
    id: ResourceId,
    module: String,
    deps: Vec<ResourceId>,
}

#[async_trait]
impl iac::Resource for DslOwnedResource {
    fn resource_type(&self) -> ResourceType {
        self.inner.resource_type()
    }

    fn resource_id(&self) -> ResourceId {
        self.id.clone()
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        self.deps.clone()
    }

    fn module(&self) -> &str {
        &self.module
    }

    async fn create(&self, ctx: &ProvisionContext) -> std::result::Result<ResourceState, IacError> {
        let mut state = self.inner.create(ctx).await?;
        state.module = self.module.clone();
        state.dependencies = self.deps.clone();
        Ok(state)
    }

    async fn update(
        &self,
        current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> std::result::Result<ResourceState, IacError> {
        let mut state = self.inner.update(current, ctx).await?;
        state.module = self.module.clone();
        state.dependencies = self.deps.clone();
        Ok(state)
    }

    async fn delete(
        &self,
        current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> std::result::Result<(), IacError> {
        self.inner.delete(current, ctx).await
    }

    async fn describe(
        &self,
        ctx: &ProvisionContext,
    ) -> std::result::Result<Option<ResourceState>, IacError> {
        match self.inner.describe(ctx).await? {
            Some(mut state) => {
                state.module = self.module.clone();
                state.dependencies = self.deps.clone();
                Ok(Some(state))
            }
            None => Ok(None),
        }
    }

    fn diff(&self, current: &ResourceState, ctx: &ProvisionContext) -> iac::InternalChange {
        self.inner.diff(current, ctx)
    }
}

/// Creates (and reports) the deployment's local state directory.
#[derive(Debug)]
struct LocalStateDirResource {
    dir: PathBuf,
}

impl LocalStateDirResource {
    fn state(&self) -> ResourceState {
        ResourceState {
            resource_type: ResourceType::new("local_state_dir"),
            physical_id: self.dir.display().to_string(),
            properties: serde_json::json!({ "path": self.dir.display().to_string() }),
            dependencies: Vec::new(),
            created_at: String::new(),
            updated_at: String::new(),
            module: LOCAL_STATE_MODULE.into(),
        }
    }
}

#[async_trait]
impl iac::Resource for LocalStateDirResource {
    fn resource_type(&self) -> ResourceType {
        ResourceType::new("local_state_dir")
    }

    fn resource_id(&self) -> ResourceId {
        ResourceId("state-dir".into())
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        Vec::new()
    }

    fn module(&self) -> &str {
        LOCAL_STATE_MODULE
    }

    async fn create(
        &self,
        _ctx: &ProvisionContext,
    ) -> std::result::Result<ResourceState, IacError> {
        std::fs::create_dir_all(&self.dir).map_err(|err| IacError::Other(err.into()))?;
        Ok(self.state())
    }

    async fn update(
        &self,
        current: &ResourceState,
        _ctx: &ProvisionContext,
    ) -> std::result::Result<ResourceState, IacError> {
        Ok(current.clone())
    }

    async fn delete(
        &self,
        _current: &ResourceState,
        _ctx: &ProvisionContext,
    ) -> std::result::Result<(), IacError> {
        Ok(())
    }

    async fn describe(
        &self,
        _ctx: &ProvisionContext,
    ) -> std::result::Result<Option<ResourceState>, IacError> {
        Ok(self.dir.exists().then(|| self.state()))
    }

    fn diff(&self, _current: &ResourceState, _ctx: &ProvisionContext) -> iac::InternalChange {
        iac::InternalChange::NoChange {
            resource_id: self.resource_id(),
        }
    }
}

// ── deploy-engine service ─────────────────────────────────────────────

/// A compose service as a deploy-engine workload.
#[derive(Debug)]
struct DslComposeWorkload {
    service: ComposeService,
    module: &'static str,
    deps: Vec<&'static str>,
}

impl deploy_engine::Service for DslComposeWorkload {
    fn name(&self) -> &str {
        &self.service.name
    }

    fn module(&self) -> &str {
        self.module
    }

    fn dependencies(&self) -> &[&str] {
        &self.deps
    }

    fn manifests(
        &self,
        _ctx: &deploy_engine::ServiceContext,
    ) -> std::result::Result<Vec<serde_json::Value>, DeployError> {
        Ok(vec![self.service.to_manifest()])
    }
}

// ── helpers ───────────────────────────────────────────────────────────

/// Leak a string to `'static`. The `iac::Module`/`deploy_engine::Service`
/// `dependencies()` signatures return `&[&str]`, which cannot borrow from owned
/// dynamic names; a CLI process builds these once and exits, so leaking the
/// handful of module/dependency names is bounded and acceptable.
fn leak_str(value: &str) -> &'static str {
    Box::leak(value.to_owned().into_boxed_str())
}

fn leak_strs(values: Vec<String>) -> Vec<&'static str> {
    values
        .into_iter()
        .map(|value| &*Box::leak(value.into_boxed_str()))
        .collect()
}

/// Build [`ObservabilityParams`] from an `ObservabilityConfigFiles` item's
/// fields, defaulting `cluster`/`deployment` to the project name and the
/// transport URLs/ports/retention to the compose conventions (matching the
/// compose platform's `ObservabilityParams::from_config`).
fn observability_params_from(
    item: &LoweredItem,
    project_name: &str,
) -> std::result::Result<ObservabilityParams, DslError> {
    Ok(ObservabilityParams {
        metrics_target_host: required_str_field(item, "metrics_target_host")?,
        metrics_target_port: required_u16_field(item, "metrics_target_port")?,
        cluster: optional_str_field(item, "cluster").unwrap_or_else(|| project_name.to_string()),
        deployment: optional_str_field(item, "deployment")
            .unwrap_or_else(|| project_name.to_string()),
        mimir_remote_write_url: "http://mimir:9009/api/v1/push".into(),
        loki_push_url: "http://loki:3100/loki/api/v1/push".into(),
        mimir_http_port: 9009,
        loki_http_port: 3100,
        loki_retention_hours: 168,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_iac::{InfraState, ModuleContext, ModuleSelection};

    fn ctx() -> RuntimeContext {
        RuntimeContext {
            deployment_dir: "/dep".into(),
            home: "/home/u".into(),
            region: "eu-west-2".into(),
        }
    }

    const DEFINITION: &str = r#"platform compose {
        let state_dir = ctx.deployment_dir / ".tokeira-state"
        module local_state { resource state = LocalStateDir { } }
        module observability {
            depends_on [ local_state ]
            resource observability_config = ObservabilityConfigFiles {
                metrics_target_host: "tokeirad",
                metrics_target_port: 9090,
            }
            service mimir = ComposeService {
                image: "grafana/mimir:3.0.6",
                ports: [ "9009:9009" ],
                depends_on: [ observability_config ],
            }
            service grafana = ComposeService {
                image: "grafana/grafana-oss:12.4.3",
                ports: [ port(3000) ],
                depends_on: [ mimir ],
            }
        }
        module runtime {
            depends_on [ observability ]
            service tokeirad = ComposeService {
                image: "tokeirad:latest",
                ports: [ port(7233) ],
                replicas: 2,
            }
        }
    }"#;

    fn config() -> ComposeDslConfig {
        let composition = crate::compile_source(DEFINITION, &ctx()).expect("compiles");
        ComposeDslConfig::from_composition(Path::new("/dep"), "tokeira", &composition)
            .expect("plan")
    }

    #[test]
    fn infra_modules_and_services_derive_from_the_composition() {
        let config = config();
        let deployment = ComposeDslDeployment::new(&config);

        let modules = deployment.infra_modules(&config, &ModuleSelection::All);
        let names: Vec<&str> = modules.iter().map(|m| m.name()).collect();
        assert_eq!(names, vec!["observability", "runtime"]);

        let runtime = modules.iter().find(|m| m.name() == "runtime").unwrap();
        assert_eq!(runtime.dependencies(), &["observability"]);

        let services = deployment.services(&config);
        let service_names: Vec<&str> = services.iter().map(|s| s.name()).collect();
        assert_eq!(service_names, vec!["mimir", "grafana", "tokeirad"]);
    }

    #[test]
    fn remote_state_module_is_the_local_state_bootstrap() {
        let config = config();
        let deployment = ComposeDslDeployment::new(&config);
        let module = deployment.remote_state_module(&config, Path::new("/dep"));
        assert_eq!(module.name(), "local_state");
    }

    #[test]
    fn ops_reports_valid_services_and_replicas() {
        let config = config();
        let deployment = ComposeDslDeployment::new(&config);

        let mut valid = deployment.valid_services().to_vec();
        valid.sort_unstable();
        assert_eq!(valid, vec!["grafana", "mimir", "tokeirad"]);

        let replicas = deployment.desired_replicas(&config);
        let tokeirad = replicas.iter().find(|r| r.service == "tokeirad").unwrap();
        assert_eq!(tokeirad.replicas, 2);
    }

    #[test]
    fn observability_config_resource_is_realized_with_dsl_id_and_module() {
        let config = config();
        let deployment = ComposeDslDeployment::new(&config);
        let modules = deployment.infra_modules(&config, &ModuleSelection::All);
        let observability = modules
            .iter()
            .find(|m| m.name() == "observability")
            .unwrap();

        let state = InfraState::default();
        let extensions = std::collections::HashMap::new();
        let ctx = ModuleContext::new(&state, &extensions);
        let resources = observability.resources(&ctx).expect("resources");

        let ids: Vec<String> = resources.iter().map(|r| r.resource_id().0).collect();
        // The config-files resource keys under its DSL-declared id so that the
        // services' `depends_on: [ observability_config ]` edge resolves.
        assert!(
            ids.contains(&"compose/observability_config".to_string()),
            "got: {ids:?}"
        );
        let config_resource = resources
            .iter()
            .find(|r| r.resource_id().0 == "compose/observability_config")
            .unwrap();
        assert_eq!(config_resource.module(), "observability");
        assert_eq!(
            config_resource.resource_type().0,
            "observability_config_files"
        );

        // mimir depends on the config resource by the same id.
        let mimir = resources
            .iter()
            .find(|r| r.resource_id().0 == "compose/mimir")
            .unwrap();
        assert!(
            mimir
                .dependencies()
                .iter()
                .any(|d| d.0 == "compose/observability_config"),
            "mimir should depend on the observability config resource"
        );
    }
}
