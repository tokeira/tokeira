//! Docker Compose platform and its concrete bound-provisioner implementation.
//!
//! The selected definition frontend returns a transient structural graph. This crate
//! admits Compose config, realizes its closed provider-kind set once per invocation,
//! and passes concrete resources directly to the infrastructure engine.

use std::{
    collections::{BTreeMap, HashSet},
    fmt,
    path::Path,
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use futures_util::TryStreamExt;
use tokeira_compose::{ComposeError, ComposePlatform};
use tokeira_deploy_engine as deploy_engine;
use tokeira_iac::{self as iac, ModuleSelection, ResourceId, SelectionDirection};
use tokeira_orchestrator::{self as orchestrator, InfraEngine};
use tokeira_platform::{
    context::InvocationContext,
    definition::{
        DefinitionFrontend, DefinitionSource, DefinitionSourceName, EvaluatedDefinition,
        RealizedResourceIndex, evaluate_definition, verify_definition,
    },
    graph::{ModuleNode, WritebackEntry, WritebackValue},
};
use tokeira_provisioner::{DeploymentBindingMetadata, RecordedDefinition};
use tokeira_provisioner_cli::{
    AppliedOutcome, ChangeLogEntry, ConfigSource, ProvisionerPlatform, Realization,
    change_log_entries,
};
use tokeira_state::{CasStore, DeploymentStore, LocalBackend};

pub mod config;
pub mod context;
pub mod images;
pub mod observability;
pub mod ops;
pub mod services;

pub use config::ComposeConfig;

const BOOTSTRAP_MODULE: &str = "local_state";
const METADATA_JSON: &str = "metadata.json";

/// Construct the conventional platform export used by generated composition roots.
pub fn provisioner<F: DefinitionFrontend>(frontend: F) -> ComposeProvisioner<F> {
    ComposeProvisioner { frontend }
}

/// Concrete Compose provisioner assembled with one selected definition frontend.
#[derive(Debug, Clone)]
pub struct ComposeProvisioner<F> {
    frontend: F,
}

#[derive(Clone)]
struct ExecutionConfig {
    config: ComposeConfig,
    modules: Vec<ModuleSpec>,
    resources: BTreeMap<String, Vec<Arc<dyn iac::Resource>>>,
    namespaces: Vec<String>,
    writeback: Vec<WritebackEntry>,
    index: RealizedResourceIndex,
    manifests: BTreeMap<ResourceId, serde_json::Value>,
}

impl fmt::Debug for ExecutionConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExecutionConfig")
            .field("modules", &self.modules)
            .field("resource_modules", &self.resources.keys())
            .field("namespaces", &self.namespaces)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
struct ModuleSpec {
    name: String,
    dependencies: Vec<String>,
}

impl From<&ModuleNode> for ModuleSpec {
    fn from(node: &ModuleNode) -> Self {
        Self {
            name: node.name().to_string(),
            dependencies: node.dependencies().to_vec(),
        }
    }
}

#[derive(Clone)]
struct ConcreteDeployment;

impl fmt::Debug for ConcreteDeployment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ConcreteDeployment")
    }
}

#[derive(Clone)]
struct ConcreteModule {
    spec: ModuleSpec,
    resources: Vec<Arc<dyn iac::Resource>>,
}

impl fmt::Debug for ConcreteModule {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
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

struct SharedResource(Arc<dyn iac::Resource>);

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

impl ConcreteDeployment {
    fn module(config: &ExecutionConfig, spec: &ModuleSpec) -> Box<dyn iac::Module> {
        Box::new(ConcreteModule {
            spec: spec.clone(),
            resources: config
                .resources
                .get(&spec.name)
                .cloned()
                .unwrap_or_default(),
        })
    }

    fn all_infra(config: &ExecutionConfig) -> Vec<Box<dyn iac::Module>> {
        config
            .modules
            .iter()
            .filter(|module| module.name != BOOTSTRAP_MODULE)
            .map(|module| Self::module(config, module))
            .collect()
    }
}

#[async_trait]
impl orchestrator::Deployment for ConcreteDeployment {
    type Config = ExecutionConfig;

    fn remote_state_module(
        &self,
        config: &Self::Config,
        _deployment_dir: &Path,
    ) -> Box<dyn iac::Module> {
        let bootstrap = config
            .modules
            .iter()
            .find(|module| module.name == BOOTSTRAP_MODULE)
            .expect("a verified Compose definition contains local_state");
        Self::module(config, bootstrap)
    }

    fn infra_modules(
        &self,
        config: &Self::Config,
        selection: &ModuleSelection,
    ) -> Vec<Box<dyn iac::Module>> {
        let all = Self::all_infra(config);
        let expanded =
            iac::expand_module_selection(&all, selection, SelectionDirection::Prerequisites)
                .unwrap_or_else(|_| selection.clone());
        all.into_iter()
            .filter(|module| expanded.includes(module.name()))
            .collect()
    }

    fn services(&self, _config: &Self::Config) -> Vec<Box<dyn deploy_engine::Service>> {
        Vec::new()
    }

    fn images(&self, _config: &Self::Config) -> Vec<Box<dyn deploy_engine::Image>> {
        Vec::new()
    }

    fn required_namespaces(&self, config: &Self::Config) -> Vec<String> {
        config.namespaces.clone()
    }

    async fn register_infra_extensions(
        &self,
        config: &Self::Config,
        context: &mut iac::ProvisionContext,
    ) -> orchestrator::Result<()> {
        if let config::Storage::Dsql(config::DsqlStorage { region, .. }) = &config.config.storage {
            let sdk = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(aws_config::Region::new(region.clone()))
                .load()
                .await;
            context.set_extension(tokeira_aws::AwsClients::new(&sdk));
        }
        Ok(())
    }

    async fn register_deploy_extensions(
        &self,
        _config: &Self::Config,
        _context: &mut deploy_engine::ServiceContext,
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

    fn hydrate_config(&self, config: &Self::Config, _state: &iac::InfraState) -> Self::Config {
        config.clone()
    }

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

impl<F: DefinitionFrontend> ComposeProvisioner<F> {
    fn metadata(deployment_dir: &Path) -> Result<DeploymentBindingMetadata> {
        let path = deployment_dir.join(METADATA_JSON);
        let bytes =
            std::fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
        serde_json::from_slice(&bytes)
            .with_context(|| format!("failed to decode {}", path.display()))
    }

    fn invocation(
        deployment_dir: &Path,
        metadata: &DeploymentBindingMetadata,
    ) -> InvocationContext {
        InvocationContext {
            deployment_id: metadata.name.clone(),
            deployment_uuid: metadata.id,
            deployment_dir: deployment_dir.to_path_buf(),
        }
    }

    fn definition(metadata: &DeploymentBindingMetadata) -> Result<&RecordedDefinition> {
        metadata
            .definition
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Compose deployment metadata records no definition"))
    }

    fn source(
        &self,
        path: &Path,
        format: orchestrator::DefinitionFormatId,
        source_name: DefinitionSourceName,
    ) -> Result<DefinitionSource> {
        let bytes = std::fs::read(path)
            .with_context(|| format!("failed to read definition {}", path.display()))?;
        Ok(DefinitionSource {
            format,
            source_name,
            bytes: Arc::from(bytes),
        })
    }

    fn evaluate_with_context(
        &self,
        source: DefinitionSource,
        context: &context::ComposeContext,
    ) -> Result<EvaluatedDefinition<ComposeConfig, services::ComposeKind>> {
        evaluate_definition(
            &self.frontend,
            source,
            context,
            services::kind_functions(),
            config::validate,
        )
        .map_err(anyhow::Error::from)
    }

    fn evaluate(
        &self,
        source: DefinitionSource,
        invocation: &InvocationContext,
    ) -> Result<EvaluatedDefinition<ComposeConfig, services::ComposeKind>> {
        self.evaluate_with_context(
            source,
            &context::ComposeContext::from_invocation(invocation),
        )
    }

    fn evaluate_recorded(
        &self,
        deployment_dir: &Path,
        definition_path: Option<&Path>,
    ) -> Result<(
        InvocationContext,
        EvaluatedDefinition<ComposeConfig, services::ComposeKind>,
    )> {
        let metadata = Self::metadata(deployment_dir)?;
        let definition = Self::definition(&metadata)?;
        let invocation = Self::invocation(deployment_dir, &metadata);
        let path = definition_path
            .map(Path::to_path_buf)
            .unwrap_or_else(|| deployment_dir.join(definition.path.as_path()));
        let source = self.source(
            &path,
            definition.format.clone(),
            DefinitionSourceName::DeploymentRelative(definition.path.clone()),
        )?;
        let evaluated = self.evaluate(source, &invocation)?;
        Ok((invocation, evaluated))
    }

    fn execution(
        &self,
        deployment_dir: &Path,
        definition_path: Option<&Path>,
    ) -> Result<ExecutionConfig> {
        let (invocation, evaluated) = self.evaluate_recorded(deployment_dir, definition_path)?;
        let verified = verify_definition(&evaluated).map_err(anyhow::Error::from)?;
        let realized = verified.realize(
            &invocation.deployment_id,
            &invocation.deployment_dir,
            &BTreeMap::new(),
        )?;
        let resource_refs = realized.iter().collect::<Vec<_>>();
        let findings = iac::verify_resources(&resource_refs);
        if !findings.is_empty() {
            bail!("the definition does not verify: {}", findings.join("; "));
        }
        let manifests = realized.manifests().clone();
        let index = realized.index().clone();
        let mut resources = BTreeMap::<String, Vec<Arc<dyn iac::Resource>>>::new();
        for resource in realized.into_resources() {
            resources
                .entry(resource.module().to_string())
                .or_default()
                .push(Arc::from(resource));
        }
        Ok(ExecutionConfig {
            config: evaluated.config,
            modules: evaluated
                .graph
                .modules()
                .iter()
                .map(ModuleSpec::from)
                .collect(),
            resources,
            namespaces: evaluated.graph.namespaces().to_vec(),
            writeback: evaluated.graph.writeback().to_vec(),
            index,
            manifests,
        })
    }

    async fn engine(
        &self,
        deployment_dir: &Path,
        require_docker: bool,
    ) -> Result<(InfraEngine<ConcreteDeployment>, Option<iac::PlatformIssue>)> {
        let execution = self.execution(deployment_dir, None)?;
        let project_name = Self::metadata(deployment_dir)?.name;
        let mut engine = InfraEngine::new(ConcreteDeployment, &execution, deployment_dir)
            .await
            .context("failed to open the Compose infrastructure engine")?;
        let ledger = deployment_dir.join("state/compose-services.yaml");
        let (platform, issue) = match ComposePlatform::connect(ledger, &project_name) {
            Ok(platform) => match platform.ensure_reachable().await {
                Ok(()) => (Some(platform), None),
                Err(error) => (None, Some(docker_issue(error, require_docker)?)),
            },
            Err(error) => (None, Some(docker_issue(error, require_docker)?)),
        };
        if let Some(platform) = platform {
            engine.provision_context_mut().set_extension(platform);
        }
        engine
            .provision_context_mut()
            .set_extension(iac::ResourceRecovery::new(|state| {
                (state.resource_type.0 == "compose_service")
                    .then(|| tokeira_compose::service_from_manifest(state.properties.clone()).ok())
                    .flatten()
                    .map(|service| Box::new(service) as Box<dyn iac::Resource>)
            }));
        Ok((engine, issue))
    }
}

impl<F: DefinitionFrontend> ProvisionerPlatform for ComposeProvisioner<F> {
    fn label(&self, _deployment_dir: &Path) -> &'static str {
        "compose"
    }

    fn config_source(&self, deployment_dir: &Path) -> Result<ConfigSource> {
        let metadata = Self::metadata(deployment_dir)?;
        let definition = Self::definition(&metadata)?;
        ConfigSource::definition(definition.format.as_str(), definition.path.as_str())
    }

    fn definition_format(&self) -> Option<&str> {
        Some(self.frontend.format().as_str())
    }

    fn deployment_id(&self, deployment_dir: &Path) -> Result<String> {
        Ok(Self::metadata(deployment_dir)?.name)
    }

    async fn definition_check(
        &self,
        deployment_dir: &Path,
        source: Option<&Path>,
    ) -> Result<Realization<()>> {
        let evaluated = if let Some(path) = source {
            let source = self.source(
                path,
                self.frontend.format().clone(),
                DefinitionSourceName::AuthoringPath(path.to_path_buf()),
            )?;
            let project_name = path
                .file_stem()
                .and_then(|value| value.to_str())
                .unwrap_or("definition")
                .to_string();
            self.evaluate_with_context(source, &context::ComposeContext { project_name })?
        } else {
            let metadata = Self::metadata(deployment_dir)?;
            let definition = Self::definition(&metadata)?;
            let invocation = Self::invocation(deployment_dir, &metadata);
            let path = deployment_dir.join(definition.path.as_path());
            let source = self.source(
                &path,
                definition.format.clone(),
                DefinitionSourceName::DeploymentRelative(definition.path.clone()),
            )?;
            self.evaluate(source, &invocation)?
        };
        verify_definition(&evaluated).map_err(anyhow::Error::from)?;
        Ok(Realization::Realized(()))
    }

    async fn log_stream(
        &self,
        deployment_dir: &Path,
        service: &str,
        follow: bool,
        tail: Option<u32>,
    ) -> Result<Realization<tokeira_provisioner_cli::LogStream>> {
        let stream = ops::log_stream(service, deployment_dir, follow, tail).await?;
        Ok(Realization::Realized(Box::pin(
            stream.map_err(anyhow::Error::from),
        )))
    }

    async fn port_mappings(
        &self,
        deployment_dir: &Path,
        service: &str,
    ) -> Result<Realization<Vec<orchestrator::PortMapping>>> {
        Ok(Realization::Realized(
            ops::port_mappings(service, deployment_dir).await?,
        ))
    }

    async fn desired_snapshot(
        &self,
        deployment_dir: &Path,
        definition: &Path,
    ) -> Result<Realization<tokeira_provisioner_cli::DesiredSnapshot>> {
        Ok(Realization::Realized(
            self.execution(deployment_dir, Some(definition))?.manifests,
        ))
    }

    async fn recorded_state(&self, deployment_dir: &Path) -> Result<iac::InfraState> {
        let (state, _) = infra_store(deployment_dir)
            .load()
            .await
            .context("loading recorded Compose infrastructure state")?;
        Ok(state)
    }

    async fn infra_plan(&self, deployment_dir: &Path) -> Result<iac::PlanOutcome> {
        let (mut engine, issue) = self.engine(deployment_dir, false).await?;
        if let Some(issue) = issue {
            return Ok(iac::PlanOutcome {
                platform_issues: vec![issue],
                ..Default::default()
            });
        }
        let composition = engine.compose(ModuleSelection::All)?;
        engine
            .plan(&composition, ModuleSelection::All)
            .await
            .context("Compose infrastructure plan failed")
    }

    async fn infra_apply(&self, deployment_dir: &Path) -> Result<AppliedOutcome> {
        let (mut engine, _) = self.engine(deployment_dir, true).await?;
        let composition = engine.compose(ModuleSelection::All)?;
        let changes = engine
            .apply(&composition, ModuleSelection::All)
            .await
            .context("Compose infrastructure apply failed")?;
        let display_by_id = engine.display_map(&composition)?;
        Ok(AppliedOutcome {
            changes,
            display_by_id,
        })
    }

    async fn publish_inspection(&self, deployment_dir: &Path) -> Result<usize> {
        let execution = self.execution(deployment_dir, None)?;
        let bytes = services::inspection_bytes(&execution.manifests)?;
        let path = orchestrator::RelativeDefinitionPath::new("docker-compose.yml")?;
        tokeira_platform::inspection::publish_inspection(deployment_dir, path, &bytes)?;
        Ok(1)
    }

    async fn infra_destroy(&self, deployment_dir: &Path) -> Result<usize> {
        let (mut engine, _) = self.engine(deployment_dir, true).await?;
        let composition = engine.compose(ModuleSelection::All)?;
        Ok(engine
            .destroy(&composition, ModuleSelection::All)
            .await
            .context("Compose infrastructure destroy failed")?
            .len())
    }

    async fn infra_destroy_selected(
        &self,
        deployment_dir: &Path,
        ids: &[String],
    ) -> Result<Vec<ChangeLogEntry>> {
        let (mut engine, _) = self.engine(deployment_dir, true).await?;
        let composition = engine.compose(ModuleSelection::All)?;
        let ids = ids.iter().cloned().map(ResourceId).collect::<HashSet<_>>();
        let changes = engine
            .destroy_selected(&composition, &ids)
            .await
            .context("Compose selected-resource destroy failed")?;
        Ok(change_log_entries(&changes))
    }

    async fn deploy_plan(&self, deployment_dir: &Path) -> Result<Realization<iac::PlanOutcome>> {
        Ok(Realization::Realized(
            self.infra_plan(deployment_dir).await?,
        ))
    }

    async fn deploy_apply(&self, deployment_dir: &Path) -> Result<Realization<AppliedOutcome>> {
        Ok(Realization::Realized(
            self.infra_apply(deployment_dir).await?,
        ))
    }

    async fn scale(&self, _deployment_dir: &Path, _specs: &[String]) -> Result<Realization<usize>> {
        Ok(Realization::NotApplicable {
            reason: "the Compose platform has no independent scale operation",
        })
    }
}

fn infra_store(deployment_dir: &Path) -> Box<dyn DeploymentStore<iac::InfraState>> {
    Box::new(CasStore::new(
        Box::new(LocalBackend::new(deployment_dir.join("state/infra"))),
        "infra".to_string(),
    ))
}

fn docker_issue(error: ComposeError, required: bool) -> Result<iac::PlatformIssue> {
    if required {
        return Err(anyhow::Error::from(error))
            .context("Compose apply/destroy needs a reachable Docker daemon");
    }
    match error {
        ComposeError::DockerNotAvailable {
            socket_path,
            evidence,
        } => Ok(tokeira_compose::docker_unreachable_issue(
            &socket_path,
            &evidence,
        )),
        other => Err(anyhow::Error::from(other)).context("the Compose platform failed to open"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        Backend, DsqlMode, DsqlStorage, Grafana, Observability, Storage, Tokeirad,
    };

    pub(crate) fn reference_config() -> ComposeConfig {
        ComposeConfig {
            storage: Storage::InMemory,
            tokeirad: Tokeirad {
                image: "tokeirad:latest".to_string(),
                replicas: 1,
                grpc_port: 7233,
                metrics_port: 9090,
            },
            observability: Observability {
                mimir: Backend {
                    image: "grafana/mimir:3.0.6".to_string(),
                    replicas: 1,
                },
                loki: Backend {
                    image: "grafana/loki:3.7.1".to_string(),
                    replicas: 1,
                },
                grafana: Grafana {
                    image: "grafana/grafana-oss:12.4.3".to_string(),
                    replicas: 1,
                    port: 3000,
                    admin_password: "admin".to_string(),
                },
                alloy: Backend {
                    image: "grafana/alloy:v1.16.0".to_string(),
                    replicas: 1,
                },
            },
        }
    }

    fn deployment(source: &str) -> tempfile::TempDir {
        let directory = tempfile::tempdir().expect("temporary deployment");
        std::fs::write(directory.path().join("definition.tkd"), source).expect("write definition");
        let metadata = serde_json::json!({
            "name": "compose-test",
            "id": "7698ae09-197e-4325-9f77-256dac98f23a",
            "platform": "compose",
            "definition": { "format": "tkd", "path": "definition.tkd" }
        });
        std::fs::write(
            directory.path().join(METADATA_JSON),
            serde_json::to_vec_pretty(&metadata).expect("encode metadata"),
        )
        .expect("write metadata");
        directory
    }

    fn reference_source() -> &'static str {
        include_str!("../definition.tkd")
    }

    fn provisioner() -> ComposeProvisioner<tokeira_tkd::TkdFrontend> {
        super::provisioner(tokeira_tkd::frontend())
    }

    fn managed_source() -> String {
        reference_source().replace(
            "storage: Storage::InMemory,",
            "storage: Storage::Dsql(DsqlStorage { region: \"eu-west-2\".into(), mode: DsqlMode::Managed, endpoint: None, arn: None }),",
        )
    }

    fn preexisting_source() -> String {
        reference_source().replace(
            "storage: Storage::InMemory,",
            "storage: Storage::Dsql(DsqlStorage { region: \"eu-west-2\".into(), mode: DsqlMode::Preexisting, endpoint: Some(\"cluster.dsql.eu-west-2.on.aws\".into()), arn: Some(\"arn:aws:dsql:eu-west-2:123456789012:cluster/example\".into()) }),",
        )
    }

    #[test]
    // Feature: platform-builder-abstraction, Property 16: Compose storage modes preserve graph parity.
    fn storage_modes_preserve_the_reference_graph_shape() {
        for (source, has_dsql) in [
            (reference_source().to_string(), false),
            (managed_source(), true),
            (preexisting_source(), true),
        ] {
            let directory = deployment(&source);
            let execution = provisioner()
                .execution(directory.path(), None)
                .expect("realize reference graph");
            let module_names = execution
                .modules
                .iter()
                .map(|module| module.name.as_str())
                .collect::<Vec<_>>();
            assert_eq!(module_names.contains(&"dsql"), has_dsql);
            assert!(module_names.contains(&"local_state"));
            assert!(module_names.contains(&"runtime"));
            assert!(module_names.contains(&"observability"));
            assert_eq!(
                execution
                    .manifests
                    .keys()
                    .filter(|id| id.0.starts_with("compose/"))
                    .count(),
                6
            );
        }
    }

    #[tokio::test]
    // Feature: platform-builder-abstraction, Property 8: verification is pure and execution uses the verified set.
    async fn definition_check_is_provider_and_state_free() {
        let directory = deployment(reference_source());
        assert!(matches!(
            provisioner()
                .definition_check(directory.path(), None)
                .await
                .expect("check reference definition"),
            Realization::Realized(())
        ));
        assert!(!directory.path().join("state").exists());
        assert!(!directory.path().join("config").exists());
    }

    #[test]
    // Feature: platform-builder-abstraction, Property 11: content coupling is deterministic and sensitive.
    fn configuration_content_is_coupled_to_every_consumer() {
        let directory = deployment(reference_source());
        let first = provisioner()
            .execution(directory.path(), None)
            .expect("realize reference definition");
        let config_id = observability::configuration_resource_id();
        let consumers = ["mimir", "loki", "grafana", "alloy"];
        let first_digests = consumers
            .iter()
            .map(|name| {
                let id = ResourceId(format!("compose/{name}"));
                let resource = first
                    .resources
                    .values()
                    .flatten()
                    .find(|resource| resource.resource_id() == id)
                    .expect("configuration consumer");
                assert!(resource.dependencies().contains(&config_id));
                first.manifests[&id]["environment"]["TOKEIRA_CONFIG_DIGEST"]
                    .as_str()
                    .expect("configuration digest")
                    .to_string()
            })
            .collect::<Vec<_>>();

        let edited = reference_source().replace("retention_hours: 168", "retention_hours: 24");
        std::fs::write(directory.path().join("definition.tkd"), edited).expect("edit definition");
        let second = provisioner()
            .execution(directory.path(), None)
            .expect("realize edited definition");
        for (name, first_digest) in consumers.iter().zip(first_digests) {
            assert_ne!(
                first_digest,
                second.manifests[&ResourceId(format!("compose/{name}"))]["environment"]["TOKEIRA_CONFIG_DIGEST"]
            );
        }
    }

    #[tokio::test]
    // Feature: platform-builder-abstraction, Property 17: inspection is deterministic and non-authoritative.
    async fn inspection_projection_is_deterministic_and_non_authoritative() {
        let directory = deployment(reference_source());
        let provisioner = provisioner();
        let execution = provisioner
            .execution(directory.path(), None)
            .expect("realize definition");
        let first = services::inspection_bytes(&execution.manifests).expect("render projection");
        let second = services::inspection_bytes(&execution.manifests).expect("render projection");
        assert_eq!(first, second);
        assert!(
            !directory
                .path()
                .join("state/compose-services.yaml")
                .exists()
        );
        assert!(
            String::from_utf8(first.clone())
                .expect("UTF-8 YAML")
                .contains("tokeirad:")
        );

        assert_eq!(
            provisioner
                .publish_inspection(directory.path())
                .await
                .expect("publish projection"),
            1
        );
        let path = directory.path().join("docker-compose.yml");
        assert_eq!(std::fs::read(&path).expect("read projection"), first);
        std::fs::write(&path, "operator edit\n").expect("edit projection");
        let after_edit = provisioner
            .execution(directory.path(), None)
            .expect("inspection output is not a definition input");
        assert_eq!(execution.manifests, after_edit.manifests);
        assert!(
            !directory
                .path()
                .join("state/compose-services.yaml")
                .exists()
        );
    }

    #[test]
    fn config_types_cover_all_storage_modes() {
        let mut config = reference_config();
        config.storage = Storage::Dsql(DsqlStorage {
            region: "eu-west-2".to_string(),
            mode: DsqlMode::Managed,
            endpoint: None,
            arn: None,
        });
        config::validate(&config).expect("managed config");
    }
}
