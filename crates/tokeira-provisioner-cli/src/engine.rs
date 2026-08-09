//! The engine: lifecycle over one bound platform and one frontend.
//!
//! [`Engine`] owns change on the bound path: evaluation, verification, and
//! realization into one operation's [`ExecutionState`]; the lifecycle verbs
//! (plan, apply, destroy — probe-first over the infrastructure engine); and
//! the reads the shell's verbs and causality machinery consume. It holds
//! the [`BoundPlatform`] and asks it for identity, vocabulary, and
//! capabilities; it never reads deployment metadata itself and never
//! answers a platform-identity question.

use std::{
    collections::{BTreeMap, HashSet},
    fmt,
    path::Path,
    sync::Arc,
};

use anyhow::{Context as _, Result, bail};
use serde::Serialize;
use tokeira_iac::{self as iac, ResourceId};
use tokeira_orchestrator::InfraEngine;
use tokeira_platform::{
    author::{LocatedValue, ValueShape, from_located_value},
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

/// The lifecycle engine for one bound platform.
pub struct Engine<F> {
    platform: BoundPlatform,
    frontend: F,
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
    /// platform's vocabulary. `definition_path` overrides only the file
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
            self.platform.vocabulary(),
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
            self.platform.vocabulary(),
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
        let verified = verify_definition(&evaluated).map_err(anyhow::Error::from)?;
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
        let mut resources = BTreeMap::<String, Vec<Arc<dyn iac::Resource>>>::new();
        for resource in realized.into_resources() {
            resources
                .entry(resource.module().to_string())
                .or_default()
                .push(Arc::from(resource));
        }
        let modules: Vec<ModuleSpec> = evaluated
            .graph
            .modules()
            .iter()
            .map(ModuleSpec::from)
            .collect();
        let bootstrap = nominate_bootstrap(&modules)?;
        let attributes = selection_attributes(
            &evaluated.config,
            self.platform
                .infra_constructors()
                .iter()
                .map(|(namespace, _)| *namespace),
        )?;
        Ok(ExecutionState {
            modules,
            bootstrap,
            resources,
            namespaces: evaluated.graph.namespaces().to_vec(),
            writeback: evaluated.graph.writeback().to_vec(),
            index,
            manifests,
            attributes,
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
    /// with the platform's vocabulary. Both revisions carry the recorded
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
            self.platform.vocabulary(),
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

    /// Read-only service plan: which services would change, by desired
    /// manifest hash against recorded runtime state. No substrate access —
    /// the comparison is manifests against the recorded state — so no
    /// probe gates it.
    pub async fn deploy_plan(
        &self,
        admitted: &Admitted,
    ) -> Result<Vec<tokeira_orchestrator::ServiceChange>> {
        let execution = self.execution(admitted, None)?;
        let mut deploy = self.open_deploy(admitted, &execution).await?;
        deploy.plan().await.context("service plan failed")
    }

    /// Reconcile the service set to desired. Refuses on a provider issue —
    /// applying manifests is substrate work.
    pub async fn deploy_apply(
        &self,
        admitted: &Admitted,
    ) -> Result<Vec<tokeira_orchestrator::ServiceChange>> {
        let execution = self.execution(admitted, None)?;
        self.refuse_on_issue(admitted).await?;
        let mut deploy = self.open_deploy(admitted, &execution).await?;
        deploy
            .apply(&NoWorkloadApplier)
            .await
            .context("service apply failed")
    }

    async fn open_deploy(
        &self,
        admitted: &Admitted,
        execution: &ExecutionState,
    ) -> Result<tokeira_orchestrator::DeployEngine<DescribedDeployment>> {
        let described = DescribedDeployment::new(
            admitted.deployment_ref.clone(),
            self.platform.infra_constructors(),
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
    /// built from the admitted coordinates and the declaration's
    /// constructors — registration runs inside `register_infra_extensions`,
    /// nowhere else. A failure here after a passing probe is real, or the
    /// unreachable class arriving in the window; either way it carries the
    /// provider's typed root.
    async fn open(
        &self,
        admitted: &Admitted,
        execution: &ExecutionState,
    ) -> Result<InfraEngine<DescribedDeployment>> {
        let described = DescribedDeployment::new(
            admitted.deployment_ref.clone(),
            self.platform.infra_constructors(),
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

/// The fail-closed workload applier: the declaration exports no service
/// applier yet (the plane split delivers it beside the service realizers),
/// and the service set is empty on this path — so a manifest reaching this
/// impl is a programming error surfaced loudly, never a silent no-op.
#[derive(Debug)]
struct NoWorkloadApplier;

#[async_trait::async_trait]
impl tokeira_deploy_engine::Platform for NoWorkloadApplier {
    async fn apply_manifests(
        &self,
        _manifests: &[serde_json::Value],
    ) -> std::result::Result<usize, tokeira_deploy_engine::RuntimeError> {
        Err(tokeira_deploy_engine::RuntimeError::Platform(
            "this platform declares no workload applier; the service plane arrives with its \
             realizers"
                .to_string(),
        ))
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

/// Extract the declared selections' namespace blocks from the evaluated
/// configuration, as plain JSON: the framework transports them to the
/// constructors without interpreting a single field.
fn selection_attributes(
    config: &LocatedValue,
    namespaces: impl Iterator<Item = &'static str>,
) -> Result<BTreeMap<String, serde_json::Value>> {
    let ValueShape::Struct { fields, .. } = &config.value else {
        return Ok(BTreeMap::new());
    };
    let mut attributes = BTreeMap::new();
    for namespace in namespaces {
        if let Some((_, value)) = fields.iter().find(|(name, _)| name == namespace) {
            let value =
                from_located_value::<serde_json::Value>(value.clone()).with_context(|| {
                    format!("the `{namespace}` attribute block does not convert to plain values")
                })?;
            attributes.insert(namespace.to_string(), value);
        }
    }
    Ok(attributes)
}

/// One operation's realized execution: the graph's modules (with the
/// shape-nominated bootstrap), the realized resources grouped by module,
/// namespaces, writeback declarations, the logical-to-engine identity
/// index, desired manifests, and the declared selections' attribute
/// blocks.
#[derive(Clone)]
pub struct ExecutionState {
    pub(crate) modules: Vec<ModuleSpec>,
    /// The unique dependency-free module: the engine stands it up before
    /// every other module (it provisions the state backend itself), so the
    /// graph must nominate exactly one.
    pub(crate) bootstrap: String,
    pub(crate) resources: BTreeMap<String, Vec<Arc<dyn iac::Resource>>>,
    pub(crate) namespaces: Vec<String>,
    pub(crate) writeback: Vec<WritebackEntry>,
    pub(crate) index: RealizedResourceIndex,
    pub(crate) manifests: BTreeMap<ResourceId, serde_json::Value>,
    /// Namespace → plain-JSON attribute block, transported to the
    /// selections' constructors uninterpreted.
    pub(crate) attributes: BTreeMap<String, serde_json::Value>,
}

impl fmt::Debug for ExecutionState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExecutionState")
            .field("modules", &self.modules)
            .field("resource_modules", &self.resources.keys())
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

    fn unlocated(value: ValueShape) -> LocatedValue {
        LocatedValue { value, range: None }
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
    fn selection_attributes_extracts_declared_namespace_blocks_as_json() {
        let config = unlocated(ValueShape::Struct {
            name: "Compose".to_string(),
            fields: vec![
                (
                    "aws".to_string(),
                    unlocated(ValueShape::Struct {
                        name: "Aws".to_string(),
                        fields: vec![(
                            "region".to_string(),
                            unlocated(ValueShape::String("eu-west-2".to_string())),
                        )],
                    }),
                ),
                ("other".to_string(), unlocated(ValueShape::Bool(true))),
            ],
        });
        let attributes = selection_attributes(&config, ["aws", "compose"].into_iter()).unwrap();
        assert_eq!(attributes.len(), 1);
        assert_eq!(attributes["aws"]["region"], "eu-west-2");
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
            _vocabulary: &tokeira_platform::declaration::Vocabulary,
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
            _vocabulary: &tokeira_platform::declaration::Vocabulary,
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
    impl tokeira_platform::declaration::ProviderExecution for NoProbe {
        async fn probe(
            &self,
            _deployment: &tokeira_platform::declaration::DeploymentRef,
        ) -> anyhow::Result<Option<iac::PlatformIssue>> {
            Ok(None)
        }
    }

    #[test]
    fn the_retarget_gate_refuses_naming_every_changed_field() {
        let declaration = tokeira_platform::declaration::PlatformDeclaration::on(
            tokeira_platform::declaration::ProviderExport {
                kinds: tokeira_platform::declaration::KindSet::new("test", Vec::new()),
                ops: None,
                execution: Box::new(NoProbe),
                infra: None,
            },
        );
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

    #[test]
    fn a_non_struct_configuration_carries_no_attribute_blocks() {
        let config = unlocated(ValueShape::Unit);
        let attributes = selection_attributes(&config, ["aws"].into_iter()).unwrap();
        assert!(attributes.is_empty());
    }
}
