//! The engine: lifecycle over one bound platform and one frontend.
//!
//! [`Engine`] owns change on the bound path: evaluation, verification, and
//! realization into one operation's [`ExecutionState`]; the lifecycle verbs
//! (plan, apply, destroy — probe-first over the infrastructure engine); and
//! the reads the shell's verbs and causality machinery consume. It holds
//! the [`BoundPlatform`] and asks it for identity, namespaces, and
//! capabilities; it never reads deployment metadata itself and never
//! answers a platform-identity question.

use std::{
    collections::{BTreeMap, HashSet},
    fmt,
    path::Path,
    sync::Arc,
};

use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Serialize};
use tokeira_iac::{self as iac, ResourceId};
use tokeira_orchestrator::InfraEngine;
use tokeira_platform::{
    author::{LocatedValue, from_located_value},
    definition::{
        DefinitionFrontend, DefinitionSource, DefinitionSourceName, DirectoryPartSources,
        EvaluatedDefinition, FrontendSource, RealizedResourceIndex, evaluate_definition,
        verify_definition,
    },
    graph::{ModuleNode, WritebackEntry},
    kind::DecodedKind,
};

use crate::{
    AppliedOutcome, ChangeLogEntry, DesiredSnapshot, change_log_entries,
    described::DescribedDeployment,
    platform::{Admitted, BoundPlatform},
};

/// The evaluation context every definition receives: the deployment's name,
/// scoping container names and provider resource identities.
#[derive(Debug, Clone, Serialize)]
struct EvaluationContext {
    project_name: String,
}

/// AWS-first configuration projected from the definition only when the
/// platform declaration includes the AWS namespace.
#[derive(Debug, Deserialize)]
struct StandardConfiguration {
    #[serde(default)]
    aws: Option<StandardAwsConfiguration>,
}

#[derive(Debug, Deserialize)]
struct StandardAwsConfiguration {
    #[serde(default)]
    region: Option<String>,
}

/// The lifecycle engine for one bound platform.
pub struct Engine<F> {
    platform: BoundPlatform,
    frontend: F,
}

/// Failure from the service-operation boundary before presentation erases
/// the deploy engine's typed runtime evidence.
///
/// Setup failures retain their ordinary contextual chain. Runtime failures
/// remain typed until `deploy.rs` either renders a specific report or elects
/// to propagate them as ordinary process errors.
#[derive(Debug)]
pub(crate) enum ServiceOperationError {
    Runtime(tokeira_deploy_engine::RuntimeError),
    Other(anyhow::Error),
}

impl ServiceOperationError {
    pub(crate) fn service_image_issue(
        &self,
    ) -> Option<&tokeira_deploy_engine::ServiceImageIssue> {
        match self {
            Self::Runtime(error) => error.service_image_issue(),
            Self::Other(_) => None,
        }
    }

    pub(crate) fn into_anyhow(self, context: &'static str) -> anyhow::Error {
        match self {
            Self::Runtime(error) => anyhow::Error::new(error).context(context),
            Self::Other(error) => error.context(context),
        }
    }
}

impl From<tokeira_orchestrator::OrchestratorError> for ServiceOperationError {
    fn from(error: tokeira_orchestrator::OrchestratorError) -> Self {
        match error {
            tokeira_orchestrator::OrchestratorError::Deploy(error) => Self::Runtime(error),
            error => Self::Other(anyhow::Error::new(error)),
        }
    }
}

impl<F> fmt::Debug for Engine<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Engine")
            .field("platform", &self.platform)
            .finish_non_exhaustive()
    }
}

impl<F: DefinitionFrontend> Engine<F> {
    /// Marry platform and frontend. The one agreement checked here: the
    /// frontend evaluates the format the platform was bound as.
    pub fn new(platform: BoundPlatform, frontend: F) -> Result<Self> {
        if platform.format() != frontend.format() {
            bail!(
                "the bound platform records format `{}` but the assembled frontend evaluates `{}`",
                platform.format(),
                frontend.format()
            );
        }
        Ok(Self { platform, frontend })
    }

    /// The platform, for the shell's identity and capability reads.
    pub fn platform(&self) -> &BoundPlatform {
        &self.platform
    }

    // ------------------------------------------------------------------
    // Evaluation: definition bytes -> evaluated definition. Pure — no
    // provider access, no state reads, no writes.
    // ------------------------------------------------------------------

    fn source(&self, path: &Path, source_name: DefinitionSourceName) -> Result<DefinitionSource> {
        let bytes = std::fs::read(path)
            .with_context(|| format!("failed to read definition {}", path.display()))?;
        Ok(DefinitionSource {
            format: self.platform.format().clone(),
            source_name,
            bytes: Arc::from(bytes),
        })
    }

    /// Evaluate the admitted deployment's recorded definition against the
    /// platform's namespaces. `definition_path` overrides only the file
    /// read — a retained revision's copy of the same recorded document —
    /// never the recorded identity.
    pub fn evaluate_recorded(
        &self,
        admitted: &Admitted,
        definition_path: Option<&Path>,
    ) -> Result<EvaluatedDefinition<DecodedKind>> {
        let definition = admitted.metadata.definition.as_ref().ok_or_else(|| {
            anyhow::anyhow!("bound deployment metadata records no definition format/path")
        })?;
        let path = definition_path
            .map(Path::to_path_buf)
            .unwrap_or_else(|| admitted.deployment_ref.dir.join(definition.path.as_path()));
        let source = self.source(
            &path,
            DefinitionSourceName::DeploymentRelative(definition.path.clone()),
        )?;
        // Parts resolve beside the interpreted document: a retained
        // revision's root loads that revision's parts, never the live ones.
        let parts_dir = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or(admitted.deployment_ref.dir.as_path());
        let parts = DirectoryPartSources::new(parts_dir, self.platform.format().as_str());
        evaluate_definition(
            &self.frontend,
            source,
            &EvaluationContext {
                project_name: admitted.deployment_ref.name.clone(),
            },
            self.platform.namespaces(),
            &parts,
        )
        .map_err(anyhow::Error::from)
    }

    /// Evaluate a standalone authored source: pre-create checking, where no
    /// deployment exists to admit. The project name is the file stem —
    /// placement-bound values never escape a check.
    pub fn evaluate_authoring(&self, path: &Path) -> Result<EvaluatedDefinition<DecodedKind>> {
        let source = self.source(
            path,
            DefinitionSourceName::AuthoringPath(path.to_path_buf()),
        )?;
        let project_name = path
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or("definition")
            .to_string();
        let parts_dir = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty());
        let parts = DirectoryPartSources::new(
            parts_dir.unwrap_or_else(|| Path::new(".")),
            self.platform.format().as_str(),
        );
        evaluate_definition(
            &self.frontend,
            source,
            &EvaluationContext { project_name },
            self.platform.namespaces(),
            &parts,
        )
        .map_err(anyhow::Error::from)
    }

    // ------------------------------------------------------------------
    // Realization: evaluated definition -> one operation's execution state.
    // ------------------------------------------------------------------

    /// Verify and realize one operation's execution state.
    ///
    /// Desired-source companions resolve against the interpreted source's
    /// own directory: a baseline realization from a retained revision folder
    /// digests that folder's companions, not the live ones.
    pub fn execution(
        &self,
        admitted: &Admitted,
        definition_path: Option<&Path>,
    ) -> Result<ExecutionState> {
        let deployment_dir = admitted.deployment_ref.dir.as_path();
        let definition_dir = definition_path
            .and_then(Path::parent)
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or(deployment_dir)
            .to_path_buf();
        let evaluated = self.evaluate_recorded(admitted, definition_path)?;
        let verified = verify_definition(&evaluated);
        let realized = verified.realize(
            &admitted.deployment_ref.name,
            deployment_dir,
            &definition_dir,
            &BTreeMap::new(),
        )?;
        let resource_refs = realized.iter().collect::<Vec<_>>();
        let findings = iac::verify_resources(&resource_refs);
        if !findings.is_empty() {
            bail!("the definition does not verify: {}", findings.join("; "));
        }
        let manifests = realized.manifests().clone();
        let index = realized.index().clone();
        let (infra_resources, services) = realized.into_parts();
        let mut resources = BTreeMap::<String, Vec<Arc<dyn iac::Resource>>>::new();
        for resource in infra_resources {
            resources
                .entry(resource.module().to_string())
                .or_default()
                .push(Arc::from(resource));
        }
        let services = services.into_iter().map(Arc::from).collect();
        let modules: Vec<ModuleSpec> = evaluated
            .graph
            .modules()
            .iter()
            .map(ModuleSpec::from)
            .collect();
        let bootstrap = nominate_bootstrap(&modules)?;
        let uses_aws = self
            .platform
            .namespaces()
            .iter()
            .any(|namespace| namespace.name == tokeira_aws::kinds::NAMESPACE);
        let aws_region = if uses_aws {
            authored_platform_aws_region(&evaluated.config)?
        } else {
            None
        };
        Ok(ExecutionState {
            uses_aws,
            aws_region,
            modules,
            bootstrap,
            resources,
            services,
            namespaces: evaluated.graph.namespaces().to_vec(),
            writeback: evaluated.graph.writeback().to_vec(),
            index,
            manifests,
            configuration_identity: evaluated.configuration_identity.clone(),
            served_companions: evaluated
                .served_companions
                .iter()
                .map(|(name, _)| name.clone())
                .collect(),
        })
    }

    // ------------------------------------------------------------------
    // Lifecycle verbs, probe-first. `--module` selection expands
    // prerequisites on plan and apply, dependants on destroy; an unknown
    // module is refused with the graph's modules listed.
    // ------------------------------------------------------------------

    /// Read-only plan. An unreachable provider endpoint does not error the
    /// verb: the outcome blocks — no planned changes, the platform issue
    /// as its only content — because describing the live substrate is a
    /// precondition of comparing against the record.
    pub async fn plan(
        &self,
        admitted: &Admitted,
        module: Option<&str>,
    ) -> Result<iac::PlanOutcome> {
        let execution = self.execution(admitted, None)?;
        if let Some(issue) = self
            .platform
            .execution()
            .probe(&admitted.deployment_ref)
            .await?
        {
            return Ok(iac::PlanOutcome {
                platform_issues: vec![issue],
                ..Default::default()
            });
        }
        let mut engine = self.open(admitted, &execution).await?;
        let selection =
            operation_selection(&engine, module, iac::SelectionDirection::Prerequisites)?;
        let composition = engine.compose(selection.clone())?;
        engine
            .plan(&composition, selection)
            .await
            .context("infrastructure plan failed")
    }

    /// Reconcile to desired. Refuses on a provider issue.
    pub async fn apply(&self, admitted: &Admitted, module: Option<&str>) -> Result<AppliedOutcome> {
        let execution = self.execution(admitted, None)?;
        self.refuse_on_issue(admitted).await?;
        let mut engine = self.open(admitted, &execution).await?;
        let selection =
            operation_selection(&engine, module, iac::SelectionDirection::Prerequisites)?;
        let composition = engine.compose(selection.clone())?;
        let changes = engine
            .apply(&composition, selection)
            .await
            .context("infrastructure apply failed")?;
        let display_by_id = engine.display_map(&composition)?;
        // Writeback resolves against the state the apply just committed;
        // the shell persists it (the engine returns, never writes files
        // outside its stores).
        let writeback = engine.collect_writeback();
        Ok(AppliedOutcome {
            changes,
            display_by_id,
            writeback,
        })
    }

    /// Tear down. Refuses on a provider issue.
    pub async fn destroy(&self, admitted: &Admitted, module: Option<&str>) -> Result<usize> {
        let execution = self.execution(admitted, None)?;
        self.refuse_on_issue(admitted).await?;
        let mut engine = self.open(admitted, &execution).await?;
        // Destroy expands dependants: taking a module down takes down what
        // stands on it, never what it stands on.
        let selection = operation_selection(&engine, module, iac::SelectionDirection::Dependants)?;
        let composition = engine.compose(selection.clone())?;
        Ok(engine
            .destroy(&composition, selection)
            .await
            .context("infrastructure destroy failed")?
            .len())
    }

    /// Delete exactly the named resource ids (the rollback delete pass):
    /// fail-closed, reverse-dependency-ordered, idempotent. Refuses on a
    /// provider issue.
    pub async fn destroy_selected(
        &self,
        admitted: &Admitted,
        ids: &[String],
    ) -> Result<Vec<ChangeLogEntry>> {
        let execution = self.execution(admitted, None)?;
        self.refuse_on_issue(admitted).await?;
        let mut engine = self.open(admitted, &execution).await?;
        let composition = engine.compose(iac::ModuleSelection::All)?;
        let ids = ids.iter().cloned().map(ResourceId).collect::<HashSet<_>>();
        let changes = engine
            .destroy_selected(&composition, &ids)
            .await
            .context("selected-resource destroy failed")?;
        Ok(change_log_entries(&changes))
    }

    // ------------------------------------------------------------------
    // Reads the shell's verbs and the causality machinery consume.
    // ------------------------------------------------------------------

    /// Canonical desired manifests for one definition source, in memory —
    /// no provider access, no state.
    pub fn desired_snapshot(
        &self,
        admitted: &Admitted,
        definition: &Path,
    ) -> Result<DesiredSnapshot> {
        Ok(self.execution(admitted, Some(definition))?.manifests)
    }

    /// The recorded infrastructure state as the last apply persisted it.
    pub async fn recorded_state(&self, admitted: &Admitted) -> Result<iac::InfraState> {
        let (state, _) = crate::described::infra_store(&admitted.deployment_ref.dir)
            .load()
            .await
            .context("loading recorded infrastructure state")?;
        Ok(state)
    }

    /// Retarget gate: refuse a create-time-immutable change between the
    /// retained prior source and the live one. Delegates to the frontend
    /// with the platform's namespaces. Both revisions carry the recorded
    /// definition identity — the compare is between two states of the same
    /// definition set, each side resolving its parts through its own
    /// resolver: the retained revision's set for the prior, the live
    /// directory's for the current.
    pub fn retarget_check(
        &self,
        admitted: &Admitted,
        prior_source: &str,
        current_source: &str,
        prior_parts: &dyn tokeira_platform::definition::SourceResolver,
        current_parts: &dyn tokeira_platform::definition::SourceResolver,
    ) -> Result<()> {
        let definition = admitted.metadata.definition.as_ref().ok_or_else(|| {
            anyhow::anyhow!("bound deployment metadata records no definition format/path")
        })?;
        let source_name = DefinitionSourceName::DeploymentRelative(definition.path.clone());
        let prior = FrontendSource {
            source_name: &source_name,
            bytes: prior_source.as_bytes(),
        };
        let current = FrontendSource {
            source_name: &source_name,
            bytes: current_source.as_bytes(),
        };
        match self.frontend.retarget_check(
            prior,
            current,
            &EvaluationContext {
                project_name: admitted.deployment_ref.name.clone(),
            },
            self.platform.namespaces(),
            prior_parts,
            current_parts,
        ) {
            Ok(()) => Ok(()),
            Err(messages) => bail!(
                "retarget refused — a create-time value changed; create a new deployment \
                 instead:\n  - {}",
                messages.join("\n  - ")
            ),
        }
    }

    // ------------------------------------------------------------------
    // Deploy verbs: the sibling family, reconciling the service set
    // through the deploy engine against state/deploy. The set is the
    // definition's service plane — empty until the plane split realizes
    // service nodes — so today these verbs honestly reconcile nothing,
    // and become real the moment `services()` fills.
    // ------------------------------------------------------------------

    /// Service plan: which services would change after each manifest's
    /// platform-owned prerequisites have been resolved. Compose image policy
    /// may populate Docker's image cache here so a registry failure is
    /// reported before apply, but no running workload is mutated.
    pub(crate) async fn deploy_plan(
        &self,
        admitted: &Admitted,
    ) -> std::result::Result<Vec<tokeira_orchestrator::ServiceChange>, ServiceOperationError> {
        let execution = self
            .execution(admitted, None)
            .map_err(ServiceOperationError::Other)?;
        let platform = self
            .platform
            .service_platform(&admitted.deployment_ref)
            .context("failed to construct the service platform")
            .map_err(ServiceOperationError::Other)?;
        let mut deploy = self
            .open_deploy(admitted, &execution)
            .await
            .map_err(ServiceOperationError::Other)?;
        deploy
            .plan_with_platform(platform.as_ref())
            .await
            .map_err(ServiceOperationError::from)
    }

    /// Reconcile the service set to desired. Refuses on a provider issue —
    /// applying manifests is substrate work.
    pub(crate) async fn deploy_apply(
        &self,
        admitted: &Admitted,
    ) -> std::result::Result<Vec<tokeira_orchestrator::ServiceChange>, ServiceOperationError> {
        let execution = self
            .execution(admitted, None)
            .map_err(ServiceOperationError::Other)?;
        self.refuse_on_issue(admitted)
            .await
            .map_err(ServiceOperationError::Other)?;
        let platform = self
            .platform
            .service_platform(&admitted.deployment_ref)
            .context("failed to construct the service platform")
            .map_err(ServiceOperationError::Other)?;
        let mut deploy = self
            .open_deploy(admitted, &execution)
            .await
            .map_err(ServiceOperationError::Other)?;
        deploy
            .apply(platform.as_ref())
            .await
            .map_err(ServiceOperationError::from)
    }

    async fn open_deploy(
        &self,
        admitted: &Admitted,
        execution: &ExecutionState,
    ) -> Result<tokeira_orchestrator::DeployEngine<DescribedDeployment>> {
        let described = DescribedDeployment::new(
            admitted.deployment_ref.clone(),
            self.platform.implementation(),
        );
        tokeira_orchestrator::DeployEngine::new(described, execution, &admitted.deployment_ref.dir)
            .await
            .context("failed to open the deploy engine")
    }

    /// The mutating verbs' probe gate: a provider issue refuses the
    /// operation outright, in the platform's own words. The shell owns
    /// document rendering.
    async fn refuse_on_issue(&self, admitted: &Admitted) -> Result<()> {
        if let Some(issue) = self
            .platform
            .execution()
            .probe(&admitted.deployment_ref)
            .await?
        {
            bail!("{issue}");
        }
        Ok(())
    }

    /// Open the infrastructure engine for one operation: the deployment
    /// built from the admitted coordinates and the platform implementation —
    /// registration runs inside `register_infra_extensions`, nowhere else. A
    /// failure here after a passing probe is real, or the unreachable class
    /// arriving in the window; either way it carries the platform's typed
    /// root.
    async fn open(
        &self,
        admitted: &Admitted,
        execution: &ExecutionState,
    ) -> Result<InfraEngine<DescribedDeployment>> {
        let described = DescribedDeployment::new(
            admitted.deployment_ref.clone(),
            self.platform.implementation(),
        );
        InfraEngine::new(described, execution, &admitted.deployment_ref.dir)
            .await
            .context("failed to open the infrastructure engine")
    }
}

/// The operation's module selection from the operator's `--module`: `None`
/// is the whole graph; a name is validated against the definition's modules
/// and expanded along `direction` by the engine, so an unknown module is
/// refused with the known modules listed — never silently widened. The same
/// expanded selection drives both `compose` and the operation, so the two
/// cannot disagree.
fn operation_selection(
    engine: &InfraEngine<DescribedDeployment>,
    module: Option<&str>,
    direction: iac::SelectionDirection,
) -> Result<iac::ModuleSelection> {
    match module {
        None => Ok(iac::ModuleSelection::All),
        Some(name) => Ok(engine.expand_selection(
            &iac::ModuleSelection::Only(vec![name.to_string()]),
            direction,
        )?),
    }
}

/// The bootstrap is nominated by shape, not name: the engine stands it up
/// before every other module (it provisions the state backend itself), and
/// the framework knows no module names — so the graph must contain exactly
/// one module with no dependencies.
fn nominate_bootstrap(modules: &[ModuleSpec]) -> Result<String> {
    let mut roots = modules
        .iter()
        .filter(|module| module.dependencies.is_empty());
    match (roots.next(), roots.next()) {
        (Some(root), None) => Ok(root.name.clone()),
        (None, _) => {
            bail!("the definition has no bootstrap module: every module has dependencies")
        }
        (Some(first), Some(second)) => bail!(
            "the definition must have exactly one bootstrap module (a module with no \
             dependencies); `{}` and `{}` both qualify",
            first.name,
            second.name
        ),
    }
}

fn authored_platform_aws_region(configuration: &LocatedValue) -> Result<Option<String>> {
    let standard: StandardConfiguration = from_located_value(configuration.clone())
        .context("failed to read the definition's standard AWS configuration")?;
    match standard.aws.and_then(|aws| aws.region) {
        Some(region) if region.is_empty() => {
            bail!("definition configuration `aws.region` is empty")
        }
        region => Ok(region),
    }
}

/// One operation's realized execution: the graph's modules (with the
/// shape-nominated bootstrap), the realized resources grouped by module,
/// namespaces, writeback declarations, the logical-to-engine identity
/// index, and desired manifests.
#[derive(Clone)]
pub struct ExecutionState {
    /// Whether the platform declaration includes the AWS authoring namespace.
    pub(crate) uses_aws: bool,
    /// Definition-authored platform default; absence preserves the SDK chain.
    pub(crate) aws_region: Option<String>,
    pub(crate) modules: Vec<ModuleSpec>,
    /// The unique dependency-free module: the engine stands it up before
    /// every other module (it provisions the state backend itself), so the
    /// graph must nominate exactly one.
    pub(crate) bootstrap: String,
    pub(crate) resources: BTreeMap<String, Vec<Arc<dyn iac::Resource>>>,
    /// Realized service resources in definition declaration order.
    pub(crate) services: Vec<Arc<dyn tokeira_deploy_engine::Service>>,
    pub(crate) namespaces: Vec<String>,
    pub(crate) writeback: Vec<WritebackEntry>,
    pub(crate) index: RealizedResourceIndex,
    pub(crate) manifests: BTreeMap<ResourceId, serde_json::Value>,
    /// The evaluated definition's configuration identity — surfaced so the
    /// check report can state what evaluation actually covered.
    pub(crate) configuration_identity: tokeira_platform::definition::ConfigurationIdentity,
    /// Served companion names in first-request order (empty when the
    /// definition is a single document).
    pub(crate) served_companions: Vec<String>,
}

impl fmt::Debug for ExecutionState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExecutionState")
            .field("modules", &self.modules)
            .field("resource_modules", &self.resources.keys())
            .field("service_count", &self.services.len())
            .field("namespaces", &self.namespaces)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ModuleSpec {
    pub(crate) name: String,
    pub(crate) dependencies: Vec<String>,
}

impl From<&ModuleNode> for ModuleSpec {
    fn from(node: &ModuleNode) -> Self {
        Self {
            name: node.name().to_string(),
            dependencies: node.dependencies().to_vec(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(name: &str, dependencies: &[&str]) -> ModuleSpec {
        ModuleSpec {
            name: name.to_string(),
            dependencies: dependencies.iter().map(|name| name.to_string()).collect(),
        }
    }

    #[test]
    fn the_bootstrap_is_the_unique_dependency_free_module() {
        let modules = [
            spec("local_state", &[]),
            spec("runtime", &["local_state"]),
            spec("observability", &["local_state"]),
        ];
        assert_eq!(nominate_bootstrap(&modules).unwrap(), "local_state");
    }

    #[test]
    fn a_graph_where_every_module_has_dependencies_is_refused() {
        let modules = [spec("a", &["b"]), spec("b", &["a"])];
        let error = nominate_bootstrap(&modules).unwrap_err().to_string();
        assert!(error.contains("no bootstrap module"), "{error}");
    }

    #[test]
    fn two_dependency_free_modules_are_refused_by_name() {
        let modules = [spec("first", &[]), spec("second", &[])];
        let error = nominate_bootstrap(&modules).unwrap_err().to_string();
        assert!(
            error.contains("`first`") && error.contains("`second`"),
            "{error}"
        );
    }

    #[test]
    fn the_standard_aws_projection_preserves_the_authored_platform_default() {
        let configuration = LocatedValue::new(tokeira_platform::author::ValueShape::Struct {
            name: "Compose".to_string(),
            fields: vec![(
                "aws".to_string(),
                LocatedValue::new(tokeira_platform::author::ValueShape::Struct {
                    name: "Aws".to_string(),
                    fields: vec![("region".to_string(), LocatedValue::string("eu-west-2"))],
                }),
            )],
        });

        assert_eq!(
            authored_platform_aws_region(&configuration).unwrap(),
            Some("eu-west-2".to_string())
        );
    }

    /// A frontend whose only behaviour is refusing the retarget: the gate
    /// test needs nothing else.
    #[derive(Clone)]
    struct RefusingFrontend(tokeira_orchestrator::DefinitionFormatId);

    impl DefinitionFrontend for RefusingFrontend {
        fn format(&self) -> &tokeira_orchestrator::DefinitionFormatId {
            &self.0
        }

        fn evaluate<C: serde::Serialize>(
            &self,
            _source: FrontendSource<'_>,
            _context: &C,
            _namespaces: &[tokeira_platform::definition::Namespace],
            _parts: &dyn tokeira_platform::definition::SourceResolver,
        ) -> std::result::Result<
            tokeira_platform::definition::FrontendOutput,
            tokeira_platform::error::FrontendDiagnostic,
        > {
            unreachable!("the retarget gate never evaluates")
        }

        fn retarget_check<C: serde::Serialize>(
            &self,
            _prior: FrontendSource<'_>,
            _current: FrontendSource<'_>,
            _context: &C,
            _namespaces: &[tokeira_platform::definition::Namespace],
            _prior_parts: &dyn tokeira_platform::definition::SourceResolver,
            _current_parts: &dyn tokeira_platform::definition::SourceResolver,
        ) -> std::result::Result<(), Vec<String>> {
            Err(vec![
                "storage.region: eu-west-2 -> us-east-1".to_string(),
                "storage.mode: guarded -> open".to_string(),
            ])
        }
    }

    #[derive(Debug)]
    struct NoProbe;

    #[async_trait::async_trait]
    impl tokeira_platform::declaration::PlatformExecution for NoProbe {
        async fn probe(
            &self,
            _deployment: &tokeira_platform::declaration::DeploymentRef,
        ) -> anyhow::Result<Option<iac::PlatformIssue>> {
            Ok(None)
        }
    }

    #[test]
    fn the_retarget_gate_refuses_naming_every_changed_field() {
        let declaration = tokeira_platform::declaration::PlatformDeclaration {
            namespaces: Vec::new(),
            ops: None,
            execution: Box::new(NoProbe),
            implementation: std::sync::Arc::new(crate::testkit::TestIntegration),
        };
        let platform = BoundPlatform::bind("test", "tkd", declaration).unwrap();
        let frontend =
            RefusingFrontend(tokeira_orchestrator::DefinitionFormatId::new("tkd").unwrap());
        let engine = Engine::new(platform, frontend).unwrap();

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("metadata.json"),
            serde_json::json!({
                "name": "demo",
                "id": "00000000-0000-0000-0000-000000000000",
                "platform": "test",
                "definition": {"format": "tkd", "path": "definition.tkd"}
            })
            .to_string(),
        )
        .unwrap();
        let admitted = engine.platform().admit_deployment(dir.path()).unwrap();

        let error = engine
            .retarget_check(
                &admitted,
                "prior",
                "current",
                &tokeira_platform::definition::NoPartSources,
                &tokeira_platform::definition::NoPartSources,
            )
            .unwrap_err()
            .to_string();
        assert!(error.contains("retarget refused"), "{error}");
        assert!(error.contains("storage.region"), "{error}");
        assert!(error.contains("storage.mode"), "{error}");
    }
}
