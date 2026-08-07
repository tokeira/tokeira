//! The engine: lifecycle over one bound platform and one frontend.
//!
//! [`Engine`] owns change on both planes: evaluation, verification,
//! realization, infra planning/apply/destroy, deploy planning/apply,
//! module selection, state stores, and writeback resolution. It holds the
//! [`BoundPlatform`] and asks it for identity, vocabulary, and
//! capabilities; it never reads deployment metadata itself and never
//! answers a platform-identity question. The shell drives these inherent
//! methods — there is no verb trait between them — and calls the
//! platform's ops surface directly for live substrate questions (logs,
//! ports; scale as local and ECS onboard), which never enter the engine.

use std::path::Path;

use anyhow::{Result, bail};
use tokeira_iac::{self as iac, ModuleSelection, SelectionDirection};
use tokeira_platform::definition::{DefinitionFrontend, EvaluatedDefinition};

use crate::platform::{Admitted, BoundPlatform};

/// The lifecycle engine for one bound platform.
pub struct Engine<F> {
    platform: BoundPlatform,
    frontend: F,
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
    // Evaluation: definition bytes -> verified, realized execution state.
    // One implementation; every verb below shares it. Admission is NOT
    // here: the shell admits once per command and threads `&Admitted`
    // through every verb it drives.
    // ------------------------------------------------------------------

    /// Evaluate the recorded definition (or an authoring-mode override)
    /// against the platform's vocabulary. Pure: no provider access, no
    /// state, no writes.
    pub fn evaluate(
        &self,
        admitted: &Admitted,
        source_override: Option<&Path>,
    ) -> Result<EvaluatedDefinition> {
        // read the recorded {format, path} from admitted.metadata;
        // evaluate_definition(&self.frontend, source, &context, vocabulary)
        unimplemented!("sketch")
    }

    /// Verify and realize one operation's execution state — both planes:
    /// infra nodes into resources/manifests/index, service nodes into the
    /// deploy engine's service set. Desired-source companions resolve
    /// against the interpreted source's own directory — a baseline
    /// realization from a retained revision digests that revision's
    /// companions, never the live ones. The graph's tags feed every
    /// placement; nothing passes an empty map.
    fn execution(
        &self,
        admitted: &Admitted,
        definition_path: Option<&Path>,
    ) -> Result<ExecutionState> {
        unimplemented!("sketch")
    }

    // ------------------------------------------------------------------
    // Infra verbs. `--module` selection expands prerequisites on plan and
    // apply, dependants on destroy; an unknown module is refused with the
    // graph's modules listed.
    // ------------------------------------------------------------------

    /// Read-only plan. An unreachable provider endpoint does not error the
    /// verb: the outcome blocks — no planned changes, the platform issue as
    /// its only content — because describing the live substrate is a
    /// precondition of comparing against the record.
    pub async fn plan(
        &self,
        admitted: &Admitted,
        module: Option<&str>,
    ) -> Result<iac::PlanOutcome> {
        unimplemented!("sketch")
    }

    /// Reconcile to desired. Refuses on a provider issue.
    pub async fn apply(&self, admitted: &Admitted, module: Option<&str>) -> Result<AppliedOutcome> {
        unimplemented!("sketch")
    }

    /// Tear down, dependants-expanded. Refuses on a provider issue.
    pub async fn destroy(&self, admitted: &Admitted, module: Option<&str>) -> Result<usize> {
        unimplemented!("sketch")
    }

    /// Delete exactly the named resource ids (the rollback delete pass):
    /// fail-closed, reverse-dependency-ordered, idempotent.
    pub async fn destroy_selected(
        &self,
        admitted: &Admitted,
        ids: &[String],
    ) -> Result<Vec<ChangeLogEntry>> {
        unimplemented!("sketch")
    }

    // ------------------------------------------------------------------
    // Deploy verbs: the sibling family, reconciling the service set
    // through the deploy engine against state/deploy. Manifest hashes
    // decide what re-applies. Compose derives docker-compose.yml at
    // deploy apply — a provider-owned artifact, not an engine capability.
    // ------------------------------------------------------------------

    /// Read-only service plan: which services would apply, by manifest
    /// hash against recorded runtime state.
    pub async fn deploy_plan(&self, admitted: &Admitted) -> Result<iac::PlanOutcome> {
        unimplemented!("sketch")
    }

    /// Reconcile the service set to desired. Refuses on a provider issue.
    pub async fn deploy_apply(&self, admitted: &Admitted) -> Result<AppliedOutcome> {
        unimplemented!("sketch")
    }

    // ------------------------------------------------------------------
    // Reads the shell's verbs and the causality machinery consume.
    // ------------------------------------------------------------------

    /// Canonical desired manifests for one definition source, in memory.
    pub fn desired_snapshot(
        &self,
        admitted: &Admitted,
        definition: &Path,
    ) -> Result<DesiredSnapshot> {
        unimplemented!("sketch")
    }

    /// The recorded infrastructure state as the last apply persisted it.
    pub async fn recorded_state(&self, admitted: &Admitted) -> Result<iac::InfraState> {
        unimplemented!("sketch")
    }

    /// Retarget gate: refuse a create-time-immutable change between the
    /// retained prior source and the live one. Delegates to the frontend
    /// with the platform's vocabulary.
    pub fn retarget_check(
        &self,
        admitted: &Admitted,
        prior_source: &str,
        current_source: &str,
    ) -> Result<()> {
        unimplemented!("sketch")
    }
}

/// One operation's realized execution, both planes: the graph's modules,
/// the realized resources grouped by module, namespaces, writeback
/// declarations, the logical-to-engine identity index, desired manifests,
/// the deploy engine's service set and images, and the graph's tags.
pub struct ExecutionState;

/// What an apply committed, with engine identity and operator nouns.
pub struct AppliedOutcome;
pub struct DesiredSnapshot;
pub struct ChangeLogEntry;

// ----------------------------------------------------------------------
// Below the engine: the orchestrator's Deployment, derived once.
// ----------------------------------------------------------------------
//
// DECIDED: `Deployment` and its three registration seams stay exactly as
// they are — signatures, siblings, implementors — and the
// `ProvisionContext` resources read stays intact with them.
// `DescribedDeployment` is the one value that implements
// `orchestrator::Deployment` on the bound path, deriving every answer
// from its three inputs (instantiation.rs, the L5 answer): the execution
// state, the admitted deployment ref, and the declaration's extension
// constructors with each selection's attribute block.

/// The framework's one `orchestrator::Deployment`.
pub struct DescribedDeployment {
    execution_state: ExecutionState,
    deployment: DeploymentRef,
    constructors: ExtensionConstructors, // from the declaration, per selection
}

#[async_trait]
impl orchestrator::Deployment for DescribedDeployment {
    type Config = ExecutionState;

    /// THE seam, unchanged: registration flows through
    /// `register_infra_extensions` exactly as `Deployment` intends. The
    /// body sets ctx.project_name (from the admitted ref) and ctx.tags
    /// (from the graph), then runs every selection's infra constructor —
    /// compose's builds the connected ComposePlatform and the recovery
    /// hook; the aws selection's builds AwsClients from its `aws.region`
    /// block. Reachability is not this method's question — the engine
    /// probed before opening — and a failure here is an error (real, or
    /// the unreachable class arriving in the probe-to-open window, typed
    /// at the error root).
    async fn register_infra_extensions(
        &self,
        config: &Self::Config,
        ctx: &mut iac::ProvisionContext,
    ) -> orchestrator::Result<()>;

    /// The same contract on the deploy plane: runs the selections' deploy
    /// constructors. Compose's deploy plane needs none; ECS's and EKS's
    /// arrive with their onboarding.
    fn register_deploy_extensions(/* … */) -> orchestrator::Result<()>;

    /// The same contract on the image plane, via the trait default until a
    /// selection carries an image constructor (ECS's live inventory, at
    /// its onboarding).
    // register_image_extensions: trait default

    /// The bootstrap is the graph's root module (a verified DAG has one);
    /// the framework knows no module names.
    fn remote_state_module(&self, config: &Self::Config, _dir: &Path) -> Box<dyn iac::Module>;

    /// All non-bootstrap modules, filtered by the operator's expanded
    /// selection. Verbs validate + expand before any selection reaches
    /// here, so re-expansion is idempotent; the fallback exists only
    /// because this method cannot error.
    fn infra_modules(
        &self,
        config: &Self::Config,
        selection: &ModuleSelection,
    ) -> Vec<Box<dyn iac::Module>>;

    /// The service plane's derivations — from the execution state's
    /// service half, no longer empty: the definition's service nodes,
    /// realized. Images likewise, where the definition declares them.
    fn services(&self, config: &Self::Config) -> Vec<Box<dyn deploy_engine::Service>>;
    fn images(&self, config: &Self::Config) -> Vec<Box<dyn deploy_engine::Image>>;

    fn required_namespaces(&self, config: &Self::Config) -> Vec<String>; // config.namespaces

    /// Identity clone: config projection is owned by the writeback /
    /// output-reference story, not by hydration.
    fn hydrate_config(&self, config: &Self::Config, _: &iac::InfraState) -> Self::Config;

    /// CAS stores at state/infra and state/deploy, constructed from the
    /// deployment's RECORDED state-backend option — an operator choice tkr
    /// surfaces at create, never platform data.
    fn create_infra_store(/* … */) -> Box<dyn DeploymentStore<iac::InfraState>>;
    fn create_deploy_store(/* … */) -> Box<dyn DeploymentStore<iac::RuntimeState>>;

    /// Declared writeback resolved against recorded state: literals pass
    /// through; output references resolve via the realized index. Feeds
    /// tokeirad.toml — the definitive story — through the shell's existing
    /// persistence flow.
    fn collect_writeback(
        &self,
        config: &Self::Config,
        state: &iac::InfraState,
    ) -> Vec<(String, String)>;
}

// The verb sequencing, probe-first, on `Engine`:
//
//     plan(admitted, module):
//         execution = self.execution(admitted, None)?      // pure half
//         match self.platform.execution().probe(&admitted.deployment_ref)? {
//             Some(issue) => PlanOutcome { platform_issues: vec![issue], .. }
//             None => open (seam → constructors) → compose → plan
//         }
//
//     apply / destroy / destroy_selected / deploy_apply:
//         probe → Some(issue) => refuse, fact + evidence
//               → None       => open → compose → apply/destroy
//
// Selection directions unchanged: prerequisites on plan/apply, dependants
// on destroy; an unknown module refused with the graph's modules listed.
// The probe is a point-in-time answer, never a guarantee: failures after a
// passing probe surface through the operation's own error path, carrying
// the same platform-issue evidence.
//
// Also absent by construction, not by deferral: `ProvisionerPlatform` (the
// shell drives `Engine` directly), `Realization<T>` (capability is presence
// on `BoundPlatform`; the shell renders refusals), `publish_inspection`
// (compose derives docker-compose.yml itself at deploy apply), and the
// shell's `TestPlatform` (tests bind a declaration with an empty kind set,
// no ops surface, and no constructors).
