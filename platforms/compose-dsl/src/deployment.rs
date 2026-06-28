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

use anyhow::Context as _;
use async_trait::async_trait;
use tokeira_aws::{
    AwsClients, ResourceContext,
    resources::{
        dsql_cluster::{DsqlCluster, DsqlClusterConfig, DsqlClusterMode},
        dynamodb_table::{
            AttributeType, BillingMode, DynamoDbTable, DynamoDbTableConfig, KeyAttribute, KeyType,
        },
    },
};
use tokeira_compose::{ComposePlatform, ComposeService};
use tokeira_compose_deployment::observability_config::{
    ObservabilityConfigFilesResource, ObservabilityParams,
};
use tokeira_deploy_engine::{
    self as deploy_engine, DeployError, DesiredImageRef, ImageContext, ImageSourceType,
    RuntimeError,
};
use tokeira_iac::{
    self as iac, IacError, InfraState, ProvisionContext, ResourceId, ResourceState, ResourceType,
};
use tokeira_orchestrator::{Deployment, Ops, PortMapping, Result, ServiceReplicas};
use tokeira_platform_dsl::{
    Composition, ItemRole, RuntimeContext,
    value::{CompositionImage, CompositionItem, CompositionWriteback, OutputRef, Value},
};
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
/// into concrete `iac` resources by the trait methods. Every payload is `Clone`
/// and `Debug`, so the plan is `Clone` as the `Deployment::Config` bound needs;
/// the concrete `iac` resources (which are not `Clone`) are reconstructed from
/// these primitives in `resources()`, not stored.
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
    /// An Aurora DSQL cluster, realized via the compiled `tokeira-aws`
    /// `DsqlCluster` resource — the DSL declares intent, not provider behaviour
    /// (Req 2). Fields carry the evaluated DSL values.
    DsqlCluster {
        id: String,
        region: String,
        mode: String,
        endpoint: Option<String>,
        arn: Option<String>,
        depends_on: Vec<String>,
    },
    /// A DynamoDB coordination table (rate-limiter or connection-lease), realized
    /// via the compiled `tokeira-aws` `DynamoDbTable` resource.
    DynamoDbTable {
        id: String,
        table_name: String,
        hash_key: String,
        ttl: Option<String>,
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
    /// The AWS region for DSQL, when the composition selected DSQL storage
    /// (taken from the `DsqlCluster` item). `None` for in-memory deployments;
    /// gates AWS client registration.
    dsql_region: Option<String>,
    /// Declarative writeback targets (dotted config key → resource output) the
    /// composition produced; resolved from provisioned state in `collect_writeback`.
    writeback: Vec<CompositionWriteback>,
    /// Declared images (Build/Mirror), realized into deploy-engine images.
    images: Vec<DslImagePlan>,
}

/// A realized image declaration: name, how it is produced, and the refs.
#[derive(Debug, Clone)]
struct DslImagePlan {
    name: String,
    /// `Build` or `Mirror` (the DSL image kind).
    build: bool,
    repository: String,
    /// Upstream ref for a mirror; `None` for a local build.
    upstream: Option<String>,
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
        let mut dsql_region = None;
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
                    (ItemRole::Resource, "DsqlCluster") => {
                        let region = required_str_field(item, "region")?;
                        // The region the cluster declares is also the region the
                        // AWS clients authenticate against (register_infra_extensions).
                        dsql_region = Some(region.clone());
                        items.push(ItemPlan::DsqlCluster {
                            id: item.id.clone(),
                            region,
                            mode: optional_str_field(item, "mode")
                                .unwrap_or_else(|| "Managed".to_string()),
                            endpoint: optional_str_field(item, "endpoint"),
                            arn: optional_str_field(item, "arn"),
                            depends_on: item.depends_on.clone(),
                        });
                    }
                    (ItemRole::Resource, "DynamoDbTable") => {
                        // Table name mirrors the compose convention
                        // `<project>-dsql-<id>` (dashes), so a DSQL deployment's
                        // coordination tables are named consistently.
                        items.push(ItemPlan::DynamoDbTable {
                            table_name: format!(
                                "{project_name}-dsql-{}",
                                item.id.replace('_', "-")
                            ),
                            id: item.id.clone(),
                            hash_key: required_str_field(item, "hash_key")?,
                            ttl: optional_str_field(item, "ttl"),
                            depends_on: item.depends_on.clone(),
                        });
                    }
                    // Image kinds are realized in `images()`, not as module resources.
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
            dsql_region,
            writeback: composition.writeback.clone(),
            images: composition
                .images
                .iter()
                .map(|image| DslImagePlan {
                    name: image.name.clone(),
                    build: image.kind == "Build",
                    repository: image_str_field(image, "repository").unwrap_or_default(),
                    upstream: image_str_field(image, "upstream"),
                })
                .collect(),
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

    /// Find a declared compose service by name (used by `Ops`).
    fn service(&self, name: &str) -> Option<&ComposeService> {
        self.modules
            .iter()
            .flat_map(|m| m.items.iter())
            .find_map(|item| match item {
                ItemPlan::Service { service, .. } if service.name == name => Some(service),
                _ => None,
            })
    }

    /// Connect a [`ComposePlatform`] against this deployment's compose file.
    ///
    /// Unlike the base compose platform — whose `Ops` trait lacks the deployment
    /// directory and so needs `*_with_dir` helpers — the DSL config carries
    /// `deployment_dir`, so the `Ops` verbs resolve the compose file directly.
    fn compose_platform(&self) -> Result<ComposePlatform> {
        let compose_file = self.deployment_dir.join("docker-compose.yml");
        ComposePlatform::connect(&compose_file, &self.project_name)
            .map_err(|err| anyhow::anyhow!("failed to connect compose platform: {err}").into())
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
                    project_name: config.project_name.clone(),
                    region: config.dsql_region.clone(),
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
                    // Only services become deploy-engine workloads; infra
                    // resources (observability config, DSQL, tables) do not.
                    _ => None,
                })
            })
            .collect()
    }

    fn images(&self, config: &Self::Config) -> Vec<Box<dyn deploy_engine::Image>> {
        config
            .images
            .iter()
            .map(|image| {
                Box::new(DslImage {
                    name: image.name.clone(),
                    build: image.build,
                    repository: image.repository.clone(),
                    upstream: image.upstream.clone(),
                }) as Box<dyn deploy_engine::Image>
            })
            .collect()
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

        // Under DSQL storage the AWS resources read an `AwsClients` handle from
        // the context. Verifying caller identity here fails fast with a clear
        // remediation rather than deep inside a resource create (mirrors the
        // compose platform's behaviour).
        if let Some(region) = &config.dsql_region {
            let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(aws_config::Region::new(region.clone()))
                .load()
                .await;
            let clients = AwsClients::new(&aws_config);
            clients
                .sts
                .get_caller_identity()
                .send()
                .await
                .context("AWS credentials required for DSQL storage; check `aws configure` or environment variables")?;
            ctx.set_extension(clients);
        }
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
        config: &Self::Config,
        state: &iac::InfraState,
    ) -> Vec<(String, String)> {
        // Resolve each declarative writeback target from provisioned state. The
        // realized resource is keyed `compose/<declared-id>` (DslOwnedResource
        // imposes that id), so the output reference's resource name maps there
        // regardless of its owning module. A target whose resource or output is
        // not yet present is skipped (it has nothing to write).
        config
            .writeback
            .iter()
            .filter_map(|wb| resolve_output(state, &wb.source).map(|value| (wb.key.clone(), value)))
            .collect()
    }
}

/// Resolve a writeback output reference to a concrete string from infra state.
fn resolve_output(state: &InfraState, source: &OutputRef) -> Option<String> {
    let id = ResourceId(format!("compose/{}", source.resource));
    state
        .resources
        .get(&id)?
        .properties
        .get(&source.output)?
        .as_str()
        .map(str::to_string)
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
                _ => None,
            })
            .collect()
    }

    async fn scale_up(&self, service: &str, replicas: u32, config: &Self::Config) -> Result<()> {
        let desired = config
            .service(service)
            .ok_or_else(|| anyhow::anyhow!(invalid_service_message(&self.valid_services, service)))?
            .clone();
        config
            .compose_platform()?
            .scale_service(&desired, replicas)
            .await
            .map_err(anyhow::Error::from)?;
        Ok(())
    }

    async fn scale_down(&self, service: &str, replicas: u32, config: &Self::Config) -> Result<()> {
        let platform = config.compose_platform()?;
        // Replicas are numbered containers `<service>-<n>` (the compose
        // platform's scale convention); remove each in turn.
        for replica in 0..replicas {
            platform
                .delete_service(&format!("{service}-{replica}"))
                .await
                .map_err(anyhow::Error::from)?;
        }
        Ok(())
    }

    async fn logs(&self, service: &str, config: &Self::Config) -> Result<Vec<String>> {
        if config.service(service).is_none() {
            return Err(
                anyhow::anyhow!(invalid_service_message(&self.valid_services, service)).into(),
            );
        }
        config
            .compose_platform()?
            .logs(service)
            .await
            .map_err(anyhow::Error::from)
            .map_err(Into::into)
    }

    async fn port_mappings(
        &self,
        service: &str,
        config: &Self::Config,
    ) -> Result<Vec<PortMapping>> {
        if config.service(service).is_none() {
            return Err(
                anyhow::anyhow!(invalid_service_message(&self.valid_services, service)).into(),
            );
        }
        let mappings = config
            .compose_platform()?
            .port_mappings(service)
            .await
            .map_err(anyhow::Error::from)?;
        Ok(mappings
            .into_iter()
            .map(
                |(host_addr, host_port, container_port, protocol)| PortMapping {
                    host_addr,
                    host_port,
                    container_port,
                    protocol,
                },
            )
            .collect())
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
    /// Compose project name — the AWS resource tag/identity prefix for DSQL.
    project_name: String,
    /// The DSQL region (when this module realizes AWS resources); the AWS
    /// `ResourceContext` requires it for both the cluster and its tables.
    region: Option<String>,
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
                    ItemPlan::DsqlCluster {
                        id,
                        region,
                        mode,
                        endpoint,
                        arn,
                        depends_on,
                    } => (
                        Box::new(dsql_cluster_resource(
                            &self.project_name,
                            &self.name,
                            region,
                            mode,
                            endpoint.clone(),
                            arn.clone(),
                        )),
                        id.clone(),
                        depends_on.clone(),
                    ),
                    ItemPlan::DynamoDbTable {
                        id,
                        table_name,
                        hash_key,
                        ttl,
                        depends_on,
                    } => (
                        Box::new(dynamodb_table_resource(
                            &self.project_name,
                            self.region.as_deref().unwrap_or("us-east-1"),
                            &self.name,
                            table_name,
                            hash_key,
                            ttl.clone(),
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

// ── deploy-engine image ───────────────────────────────────────────────

/// A DSL image declaration (`Build`/`Mirror`) as a deploy-engine image.
///
/// `desired_ref` is computed from the DSL fields (repository + upstream), so the
/// image refs are DSL-native and do not read a `ComposeConfig` extension the way
/// the compiled compose images do. Writeback of a built ref into config is a
/// host-runtime concern not modelled by the DSL image kinds, so
/// `writeback_targets` is empty (the default).
#[derive(Debug)]
struct DslImage {
    name: String,
    build: bool,
    repository: String,
    upstream: Option<String>,
}

impl deploy_engine::Image for DslImage {
    fn name(&self) -> &str {
        &self.name
    }

    fn source_type(&self) -> ImageSourceType {
        if self.build {
            ImageSourceType::Build
        } else {
            ImageSourceType::Mirror
        }
    }

    fn desired_ref(
        &self,
        _ctx: &ImageContext,
    ) -> std::result::Result<DesiredImageRef, RuntimeError> {
        Ok(DesiredImageRef {
            repository: self.repository.clone(),
            // A build resolves to `latest`; a mirror takes the upstream's tag.
            tag: self
                .upstream
                .as_deref()
                .map(image_tag)
                .unwrap_or_else(|| "latest".to_owned()),
            upstream_ref: self.upstream.clone(),
        })
    }
}

/// Derive the tag from an upstream image ref, mirroring the compose platform's
/// `image_tag`: strip any `@digest`, then take the segment after the last `:`
/// unless that colon belongs to a registry-host port (before the last `/`).
fn image_tag(upstream: &str) -> String {
    let without_digest = upstream.split('@').next().unwrap_or(upstream);
    let last_slash = without_digest.rfind('/');
    let last_colon = without_digest.rfind(':');
    match last_colon {
        Some(colon) if last_slash.is_none_or(|slash| colon > slash) => {
            without_digest[colon + 1..].to_owned()
        }
        _ => "latest".to_owned(),
    }
}

/// A required string field on a top-level image declaration.
fn image_str_field(image: &CompositionImage, field: &str) -> Option<String> {
    match image.fields.get(field) {
        Some(Value::Str(s)) | Some(Value::Path(s)) => Some(s.clone()),
        _ => None,
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

/// An "unknown service" message listing the valid alternatives, for `Ops`.
fn invalid_service_message(valid: &[&str], actual: &str) -> String {
    format!(
        "unknown service '{actual}'. valid services: {}",
        valid.join(", ")
    )
}

/// Build the AWS `ResourceContext` shared by the DSQL resources in a module.
fn aws_resource_context(project_name: &str, region: &str) -> ResourceContext {
    ResourceContext {
        project: project_name.to_owned(),
        region: region.to_owned(),
        tags: std::collections::HashMap::from([("ManagedBy".to_owned(), "tkr".to_owned())]),
    }
}

/// Realize a DSL `DsqlCluster` item as the compiled `tokeira-aws` resource.
///
/// The cluster identity mirrors the compose convention (`<project>-compose`) so
/// a DSQL deployment authored via the DSL and one via `ComposeConfig` name the
/// same physical cluster. `mode` is the evaluated DSL string; an unrecognised
/// value defaults to `Managed` (the type checker constrains the surface form).
fn dsql_cluster_resource(
    project_name: &str,
    module: &str,
    region: &str,
    mode: &str,
    endpoint: Option<String>,
    arn: Option<String>,
) -> DsqlCluster {
    let mode = match mode {
        "Preexisting" | "preexisting" => DsqlClusterMode::Preexisting,
        _ => DsqlClusterMode::Managed,
    };
    DsqlCluster::new(
        format!("{project_name}-compose"),
        DsqlClusterConfig {
            mode,
            preexisting_endpoint: endpoint,
            preexisting_arn: arn,
            fallback_identifier: None,
            resource_id: None,
            module: module.to_owned(),
        },
        &aws_resource_context(project_name, region),
    )
}

/// Realize a DSL `DynamoDbTable` item as the compiled `tokeira-aws` resource,
/// matching the on-demand, TTL-enabled coordination-table shape the DSQL
/// connection management expects.
fn dynamodb_table_resource(
    project_name: &str,
    region: &str,
    module: &str,
    table_name: &str,
    hash_key: &str,
    ttl: Option<String>,
) -> DynamoDbTable {
    DynamoDbTable::new(
        table_name.to_owned(),
        DynamoDbTableConfig {
            key_schema: vec![KeyAttribute {
                name: hash_key.to_owned(),
                key_type: KeyType::Hash,
                attribute_type: AttributeType::String,
            }],
            billing_mode: BillingMode::OnDemand,
            ttl_attribute: ttl,
            module: module.to_owned(),
        },
        &aws_resource_context(project_name, region),
    )
}

/// Build [`ObservabilityParams`] from an `ObservabilityConfigFiles` item's
/// fields, defaulting `cluster`/`deployment` to the project name and the
/// transport URLs/ports/retention to the compose conventions (matching the
/// compose platform's `ObservabilityParams::from_config`).
fn observability_params_from(
    item: &CompositionItem,
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

    /// Build a DSQL composition by evaluating a definition with a `storage =
    /// Dsql{..}` input override (compile_source uses defaults, so drive the dsl
    /// pipeline directly for the Dsql arm).
    fn dsql_config() -> ComposeDslConfig {
        use tokeira_platform_dsl::{
            KindLibrary, Value, evaluate_with_inputs, lex, parse, resolve, typeck,
        };

        let src = r#"platform compose {
            input storage: Storage = InMemory
            module local_state { resource state = LocalStateDir { } }
            module dsql when storage is Dsql {
                depends_on [ local_state ]
                match storage {
                    Dsql(d) => {
                        resource cluster = DsqlCluster { mode: d.mode, region: d.region }
                        resource rate_limiter = DynamoDbTable { hash_key: "pk", ttl: "ttl_epoch" }
                        resource conn_lease = DynamoDbTable { hash_key: "pk", ttl: "ttl_epoch" }
                    }
                    _ => { }
                }
            }
            writeback when storage is Dsql {
                "infrastructure.dsql.endpoint" : dsql.cluster.cluster_endpoint,
                "infrastructure.dsql.rate_limiter_table" : dsql.rate_limiter.table_name,
            }
        }"#;
        let (tokens, _) = lex(src);
        let (program, _) = parse(&tokens, src.len());
        let program = program.expect("program");
        let kinds = KindLibrary::compose();
        assert!(resolve(&program, &kinds).is_empty());
        assert!(typeck(&program, &kinds).is_empty());

        let mut payload = std::collections::BTreeMap::new();
        payload.insert("mode".to_string(), Value::Str("Managed".into()));
        payload.insert("region".to_string(), Value::Str("eu-west-2".into()));
        let mut inputs = std::collections::HashMap::new();
        inputs.insert(
            "storage".to_string(),
            Value::Variant {
                name: "Dsql".into(),
                payload: Some(Box::new(Value::Record(payload))),
            },
        );
        let composition = evaluate_with_inputs(&program, &ctx(), &inputs).expect("composition");
        ComposeDslConfig::from_composition(Path::new("/dep"), "tokeira", &composition)
            .expect("plan")
    }

    #[test]
    fn dsql_module_realizes_aws_resources_with_dsl_ids() {
        let config = dsql_config();
        assert_eq!(config.dsql_region.as_deref(), Some("eu-west-2"));

        let deployment = ComposeDslDeployment::new(&config);
        let modules = deployment.infra_modules(&config, &ModuleSelection::All);
        let dsql = modules
            .iter()
            .find(|m| m.name() == "dsql")
            .expect("dsql module");
        assert_eq!(dsql.dependencies(), &["local_state"]);

        let state = InfraState::default();
        let extensions = std::collections::HashMap::new();
        let mctx = ModuleContext::new(&state, &extensions);
        let ids: Vec<String> = dsql
            .resources(&mctx)
            .expect("resources")
            .iter()
            .map(|r| r.resource_id().0.clone())
            .collect();
        // The DSL ids carry through to the realized AWS resources, so writeback
        // output references resolve against them (Req 15 / task 9.4).
        assert!(ids.contains(&"compose/cluster".to_string()), "got: {ids:?}");
        assert!(
            ids.contains(&"compose/rate_limiter".to_string()),
            "got: {ids:?}"
        );
        assert!(
            ids.contains(&"compose/conn_lease".to_string()),
            "got: {ids:?}"
        );
    }

    #[test]
    fn collect_writeback_resolves_dsql_outputs_from_state() {
        let config = dsql_config();
        let deployment = ComposeDslDeployment::new(&config);

        // Synthesize the provisioned state the engine would produce.
        let mut state = InfraState::default();
        state.resources.insert(
            ResourceId("compose/cluster".into()),
            ResourceState {
                resource_type: ResourceType::new("dsql_cluster"),
                physical_id: "cluster-xyz".into(),
                properties: serde_json::json!({ "cluster_endpoint": "abc.dsql.eu-west-2.on.aws" }),
                dependencies: Vec::new(),
                created_at: String::new(),
                updated_at: String::new(),
                module: "dsql".into(),
            },
        );
        state.resources.insert(
            ResourceId("compose/rate_limiter".into()),
            ResourceState {
                resource_type: ResourceType::new("dynamodb_table"),
                physical_id: "tokeira-dsql-rate-limiter".into(),
                properties: serde_json::json!({ "table_name": "tokeira-dsql-rate-limiter" }),
                dependencies: Vec::new(),
                created_at: String::new(),
                updated_at: String::new(),
                module: "dsql".into(),
            },
        );

        let writeback = deployment.collect_writeback(&config, &state);
        assert!(writeback.contains(&(
            "infrastructure.dsql.endpoint".into(),
            "abc.dsql.eu-west-2.on.aws".into()
        )));
        assert!(writeback.contains(&(
            "infrastructure.dsql.rate_limiter_table".into(),
            "tokeira-dsql-rate-limiter".into()
        )));
    }

    #[test]
    fn images_are_realized_from_declarations() {
        use tokeira_deploy_engine::ImageContext;

        let src = r#"platform compose {
            image tokeirad = Build { repository: "tokeira/tokeirad" }
            image mimir = Mirror { repository: "tokeira/grafana-mimir", upstream: "grafana/mimir:3.0.6" }
        }"#;
        let composition = crate::compile_source(src, &ctx()).expect("compiles");
        let config = ComposeDslConfig::from_composition(Path::new("/dep"), "tokeira", &composition)
            .expect("plan");
        let deployment = ComposeDslDeployment::new(&config);
        let images = deployment.images(&config);
        let names: Vec<&str> = images.iter().map(|i| i.name()).collect();
        assert_eq!(names, vec!["tokeirad", "mimir"]);

        let image_ctx = ImageContext::default();
        let mimir = images.iter().find(|i| i.name() == "mimir").unwrap();
        assert_eq!(mimir.source_type(), ImageSourceType::Mirror);
        let mirror_ref = mimir.desired_ref(&image_ctx).unwrap();
        assert_eq!(mirror_ref.tag, "3.0.6");
        assert_eq!(
            mirror_ref.upstream_ref.as_deref(),
            Some("grafana/mimir:3.0.6")
        );

        let tokeirad = images.iter().find(|i| i.name() == "tokeirad").unwrap();
        assert_eq!(tokeirad.source_type(), ImageSourceType::Build);
        let build_ref = tokeirad.desired_ref(&image_ctx).unwrap();
        assert_eq!(build_ref.tag, "latest");
        assert_eq!(build_ref.upstream_ref, None);
    }
}
