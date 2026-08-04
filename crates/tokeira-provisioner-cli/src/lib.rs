//! `tokeira-provisioner-cli` — the platform-agnostic `tkp` shell (Req 14.2).
//!
//! Every platform's provisioner binary (each constructed as `tkp`) is this
//! library plus one [`ProvisionerPlatform`] implementation: the shell owns the
//! lifecycle verbs, the binding-gate orchestration, the operation-lock wrapper,
//! the deployment state envelope, `describe`, the Day-0 stamp, and the
//! config-revision history; the platform supplies only the resource realization.
//! This is the clean split of design §"Per-platform provisioner and three-part
//! provenance": the shell is a distinct layer over `tokeira-provisioner` (the
//! domain library — stamps, binding, integrity), never folded into it.

// CLI shell: stdout/stderr are the operator interface.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::{path::Path, pin::Pin};

use anyhow::{Context, Result};
use tokeira_iac::{Change, ChangeKind, PlanOutcome};
use tokeira_orchestrator::{DefinitionFormatId, RelativeDefinitionPath};
use tokeira_provisioner::DeploymentStateEnvelope;
use tokeira_state::{CasStore, DeploymentStore, LocalBackend};

mod apply;
mod bound;
mod causality;
mod cli;
mod config_history;
mod config_seed;
mod definition;
mod deploy;
mod describe;
mod destroy;
mod gate;
mod init;
mod lock;
mod marker;
mod plan;
mod render;
mod revert;
mod rollback;
mod scale;
mod upgrade;

pub use bound::BoundPlatform;
pub use cli::run;
// The seam's audit vocabulary travels with the seam: platform realizations
// return these from their applying verbs (task 19.2).
pub use tokeira_provisioner::{ChangeLogEntry, ChangeOp};

/// Generate the disposable `tkp` entrypoint for one statically selected
/// concrete platform provisioner and one definition frontend.
#[macro_export]
macro_rules! bound_provisioner_main {
    (
        expected_platform: $platform:literal,
        platform: $platform_factory:path,
        expected_format: $format:literal,
        frontend: $frontend:path $(,)?
    ) => {
        fn main() -> std::process::ExitCode {
            $crate::run_bound_provisioner($platform, $format, $platform_factory($frontend()))
        }
    };
}

/// Synchronous process boundary used only by generated composition roots.
pub fn run_bound_provisioner<P>(
    expected_platform: &'static str,
    expected_format: &'static str,
    platform: P,
) -> std::process::ExitCode
where
    P: ProvisionerPlatform,
{
    let platform = match BoundPlatform::new(expected_platform, expected_format, platform) {
        Ok(platform) => platform,
        Err(error) => {
            eprintln!("{error:#}");
            return std::process::ExitCode::FAILURE;
        }
    };
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("failed to start provisioner runtime: {error}");
            return std::process::ExitCode::FAILURE;
        }
    };
    match runtime.block_on(run(platform)) {
        Ok(code) => code,
        Err(error) => {
            eprintln!("{error:#}");
            std::process::ExitCode::FAILURE
        }
    }
}

/// The outcome of a **conditionally realized** platform verb (design §"Command
/// behaviour and outputs"): the surface is the same for every platform, and
/// where a platform cannot honor a verb it answers `NotApplicable` — a
/// first-class result the shell turns into a typed non-zero refusal, never a
/// missing subcommand and never a crash.
#[derive(Debug)]
pub enum Realization<T> {
    /// The platform realized the verb.
    Realized(T),
    /// This platform cannot honor the verb; `reason` is operator-facing.
    NotApplicable { reason: &'static str },
}

/// Canonical per-resource desired manifests realized from one definition
/// source, in memory: the comparison value revision-level causality is built
/// on. Keys are the engine's own resource identities, so a snapshot joins the
/// plan's changes without translation.
pub type DesiredSnapshot = std::collections::BTreeMap<tokeira_iac::ResourceId, serde_json::Value>;

/// Incremental operator log output. Providers retain their native streaming
/// behavior; the shell never collects logs into an in-memory snapshot.
pub type LogStream = Pin<Box<dyn futures_util::Stream<Item = Result<String>> + Send>>;

/// The typed refusal a verb returns after emitting a platform-issue
/// document: the document already said everything (output-templates §The
/// platform cannot be reached — "The verb exits non-zero"), so [`cli::run`]
/// turns this into a bare non-zero exit instead of an error line that would
/// restate the report. The message exists for any other caller.
#[derive(Debug)]
pub(crate) struct PlatformBlocked;

impl std::fmt::Display for PlatformBlocked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the platform reported an issue; the report carries it")
    }
}

impl std::error::Error for PlatformBlocked {}

/// What an apply committed, under the identity the engine executed it with.
///
/// The persisted audit vocabulary stays ids-only (Proposal 002) —
/// [`change_log_entries`] distills `changes` wherever an audit record is
/// needed. The full engine identity (module, resource type, a `Replace`
/// kind the audit log folds away) and the operator nouns exist here because
/// the applied report states what committed in the operator's language, and
/// reading them back from anywhere else would be re-derivation or invention.
#[derive(Debug, Clone, Default)]
pub struct AppliedOutcome {
    pub changes: Vec<tokeira_iac::Change>,
    pub display_by_id: std::collections::BTreeMap<tokeira_iac::ResourceId, String>,
}

/// Recorded desired-source identity used by shell history and evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigSource {
    /// Definition format for interpreted sources; absent only for retained
    /// legacy configuration files outside the bound-provisioner path.
    pub format: Option<DefinitionFormatId>,
    /// Canonical path relative to the deployment root.
    pub path: RelativeDefinitionPath,
}

impl ConfigSource {
    /// Construct a recorded interpreted definition source.
    pub fn definition(format: &str, path: &str) -> Result<Self> {
        Ok(Self::recorded(
            DefinitionFormatId::new(format)?,
            RelativeDefinitionPath::new(path)?,
        ))
    }

    pub(crate) fn recorded(format: DefinitionFormatId, path: RelativeDefinitionPath) -> Self {
        Self {
            format: Some(format),
            path,
        }
    }

    /// Construct a retained legacy configuration source.
    pub fn legacy(path: &str) -> Result<Self> {
        Ok(Self {
            format: None,
            path: RelativeDefinitionPath::new(path)?,
        })
    }
}

/// The seam a per-platform provisioner binary implements: the platform-realized
/// verbs plus the platform properties the shell genuinely needs. Everything else
/// — gating, locking, the envelope, revisions, describe — is the shell's.
///
/// Methods take the deployment directory because a provisioner is married to one
/// deployment and every verb operates on it; a single-platform implementation is
/// free to ignore it for the property methods.
#[allow(async_fn_in_trait)] // implementations are workspace-internal and monomorphized; no Send bound needed
pub trait ProvisionerPlatform {
    /// Admit the deployment's immutable platform/frontend/bundle identity
    /// before the shell acquires a lock or reads any state. Legacy in-process
    /// adapters have no static assembly evidence and retain the no-op default.
    fn admit_deployment(&self, _deployment_dir: &Path) -> Result<()> {
        Ok(())
    }

    /// Human label for reports (e.g. `"compose"`).
    fn label(&self, deployment_dir: &Path) -> &'static str;

    /// Recorded desired source used for evaluation, identity, and revisions.
    fn config_source(&self, deployment_dir: &Path) -> Result<ConfigSource>;

    /// Definition format compiled into this provisioner, when interpreted.
    fn definition_format(&self) -> Option<&str> {
        None
    }

    /// The deployment identity recorded at Day-0 stamp time.
    fn deployment_id(&self, deployment_dir: &Path) -> Result<String>;

    /// Preview the infrastructure plan (read-only, no mutation): the changes
    /// plus the refresh coverage behind them. The infra verbs are universal —
    /// every provisioner provisions infrastructure — so they are
    /// unconditional, unlike the workload verbs below.
    /// `module` is the operator's `--module` filter; the platform owns its
    /// meaning — validating the name and expanding along the module DAG
    /// (prerequisites here, dependants for destroy) — and `None` is the whole
    /// graph.
    async fn infra_plan(&self, deployment_dir: &Path, module: Option<&str>) -> Result<PlanOutcome>;

    /// Reconcile infrastructure to desired. Returns the committed changes
    /// with their engine identity and operator nouns — never before-images
    /// (Proposal 002): the applied report renders identity, and `upgrade`
    /// records the [`change_log_entries`] distillation as the audit change
    /// log (task 19.2).
    async fn infra_apply(
        &self,
        deployment_dir: &Path,
        module: Option<&str>,
    ) -> Result<AppliedOutcome>;

    /// Reconcile infrastructure after publishing declared operational artifacts.
    ///
    /// Ordinary apply, upgrade, and explicit definition reversion use this
    /// entrypoint. Automated engine rollback deliberately calls
    /// [`infra_apply`](Self::infra_apply) so it cannot publish artifacts while
    /// restoring the retained engine. Legacy adapters have no artifact catalog
    /// and retain their existing apply behavior through this default.
    async fn infra_apply_with_artifacts(
        &self,
        deployment_dir: &Path,
        module: Option<&str>,
    ) -> Result<AppliedOutcome> {
        self.infra_apply(deployment_dir, module).await
    }

    /// Publish declared non-authoritative inspection artifacts after apply state commits.
    ///
    /// The shell calls this only after its configuration revision and engine
    /// state are durable. Legacy adapters declare no inspection artifacts and
    /// retain the no-op default.
    async fn publish_inspection(&self, _deployment_dir: &Path) -> Result<usize> {
        Ok(0)
    }

    /// Tear down the deployment's infrastructure. Returns the removed count.
    /// A `module` filter expands along dependants, never prerequisites.
    async fn infra_destroy(&self, deployment_dir: &Path, module: Option<&str>) -> Result<usize>;

    /// Delete **exactly** the named resource ids — the rollback B-delete pass
    /// (task 19.3, Proposal 002): fail-closed (an id the platform does not
    /// know errors rather than orphaning), reverse-dependency-ordered,
    /// idempotent (an id absent from state is already done); every other
    /// resource untouched. Returns the deleted entries.
    async fn infra_destroy_selected(
        &self,
        deployment_dir: &Path,
        ids: &[String],
    ) -> Result<Vec<ChangeLogEntry>>;

    /// Check a definition: parse + interpret in memory — no provider calls,
    /// no state writes (`definition check`; read-only, never gates). `source`
    /// overrides the deployment's own definition file (authoring mode). `Err`
    /// is the located verdict ("parse error at line 112, …"), which the shell
    /// renders as a report rather than a crash. The default serves platforms
    /// with no interpreted definition.
    async fn definition_check(
        &self,
        deployment_dir: &Path,
        source: Option<&Path>,
    ) -> Result<Realization<()>> {
        let _ = (deployment_dir, source);
        Ok(Realization::NotApplicable {
            reason: "this platform has no interpreted definition",
        })
    }

    /// Seed the deployment's server configuration at create time: realize the
    /// definition's config-document render with provider-derived fields left
    /// blank and write the result into the deployment directory. Evaluation
    /// only — no provider access, no engine state. `tkr deployment create`
    /// invokes this against the staging directory after the staged definition
    /// check; `NotApplicable` keeps the generic seeded document, so create
    /// never fails for a platform that has not adopted the render.
    async fn config_seed(&self, deployment_dir: &Path) -> Result<Realization<()>> {
        let _ = deployment_dir;
        Ok(Realization::NotApplicable {
            reason: "this platform does not seed server configuration",
        })
    }

    /// The retarget gate: compare the retained prior configuration against
    /// the live source and refuse a create-time-immutable change before any
    /// mutation. The shell supplies both sources; the platform evaluates
    /// them through its own frontend. An `Err` is the refusal, naming the
    /// changed fields; `NotApplicable` serves platforms with no interpreted
    /// definition, which gate nothing.
    async fn retarget_check(
        &self,
        deployment_dir: &Path,
        prior_source: &str,
        current_source: &str,
    ) -> Result<Realization<()>> {
        let _ = (deployment_dir, prior_source, current_source);
        Ok(Realization::NotApplicable {
            reason: "this platform has no interpreted definition",
        })
    }

    /// Open a platform-native stream for one logical service.
    async fn log_stream(
        &self,
        _deployment_dir: &Path,
        _service: &str,
        _follow: bool,
        _tail: Option<u32>,
    ) -> Result<Realization<LogStream>> {
        Ok(Realization::NotApplicable {
            reason: "this platform exposes no service logs",
        })
    }

    /// Resolve live host/container mappings for one logical service.
    async fn port_mappings(
        &self,
        _deployment_dir: &Path,
        _service: &str,
    ) -> Result<Realization<Vec<tokeira_orchestrator::PortMapping>>> {
        Ok(Realization::NotApplicable {
            reason: "this platform exposes no published port mappings",
        })
    }

    /// Realize `definition` (a path to a definition source — the working
    /// definition or a retained revision) into canonical per-resource desired
    /// manifests (operator-explanation Req 5.1). MUST NOT contact providers, read
    /// live state, or write state: the snapshot is a value computed entirely
    /// in memory, and two snapshots of semantically identical definitions
    /// MUST be equal (canonical set-valued fields). A source that does not
    /// interpret returns the located verdict as `Err`; the default serves
    /// platforms with no interpreted definition, whose causality classifies
    /// per the algebra's A10.
    async fn desired_snapshot(
        &self,
        deployment_dir: &Path,
        definition: &Path,
    ) -> Result<Realization<DesiredSnapshot>> {
        let _ = (deployment_dir, definition);
        Ok(Realization::NotApplicable {
            reason: "this platform has no interpreted definition to snapshot",
        })
    }

    /// The recorded infrastructure state as persisted by the last apply —
    /// **S** in the causality algebra — loaded from the platform's own state
    /// store, never from a planning context. Refresh overwrites in-context
    /// resource properties with live observations before diffing, so state
    /// obtained any other way is contaminated: drift detection over it would
    /// silently compare live against live (operator-explanation Req 5.1). A
    /// platform with no persisted engine state answers the empty state, which
    /// is the honest S for it (nothing was ever recorded).
    async fn recorded_state(&self, deployment_dir: &Path) -> Result<tokeira_iac::InfraState> {
        let _ = deployment_dir;
        Ok(tokeira_iac::InfraState::default())
    }

    /// Preview the workload Delta (read-only). A platform whose workload rides
    /// the infra universe (compose models its containers as infra
    /// resources) realizes this as the infra plan.
    async fn deploy_plan(&self, deployment_dir: &Path) -> Result<Realization<PlanOutcome>>;

    /// Reconcile the workload to desired. Returns the committed changes
    /// with their identity, like [`infra_apply`](Self::infra_apply).
    async fn deploy_apply(&self, deployment_dir: &Path) -> Result<Realization<AppliedOutcome>>;

    /// Change workload capacity (`<dim>=<n>` specs). `NotApplicable` where the
    /// platform has no scale dimension. Returns the change count.
    async fn scale(&self, deployment_dir: &Path, specs: &[String]) -> Result<Realization<usize>>;
}

/// The explanation's deployment context, assembled from what the envelope
/// records and the platform knows (operator-explanation Req 1). Read-only
/// verbs pass no proposed revision — the plan proposes nothing by itself.
pub(crate) fn explain_context<P: ProvisionerPlatform>(
    platform: &P,
    deployment_dir: &Path,
    envelope: &DeploymentStateEnvelope,
    operation: &str,
) -> tokeira_explain::DeploymentContext {
    tokeira_explain::DeploymentContext {
        deployment: crate::apply::deployment_identity(&envelope.deployment_id),
        platform: platform.label(deployment_dir).to_string(),
        operation: operation.to_string(),
        current_revision: envelope.config_revision,
        proposed_revision: None,
        definition_ref: envelope.effective_config_ref.clone(),
    }
}

/// Map an applied engine Delta to the ids-only audit entries (task 19.2).
///
/// The id is the engine's **`ResourceId`** (`Change::resource`) — the exact
/// address `destroy_selected` keys on, so the audit entries double as the
/// rollback delete-set's feed (task 19.3); the module is grouping metadata
/// the ids-only log deliberately drops. Kinds map onto the audit vocabulary:
/// `Replace` records as `Updated` (the id survives a delete-then-recreate;
/// the audit log tracks identities, not mechanics), and `NoChange` records
/// nothing.
pub fn change_log_entries(changes: &[Change]) -> Vec<ChangeLogEntry> {
    changes
        .iter()
        .filter_map(|change| {
            let op = match change.kind {
                ChangeKind::Create => ChangeOp::Created,
                ChangeKind::Update | ChangeKind::Replace => ChangeOp::Updated,
                ChangeKind::Delete => ChangeOp::Deleted,
                ChangeKind::NoChange => return None,
            };
            Some(ChangeLogEntry {
                id: change.resource.clone(),
                op,
            })
        })
        .collect()
}

/// Emit a rendered report: Markdown narrative is skinned for a terminal via
/// termimad and emitted raw everywhere else — a pipe, a redirect, or an
/// agent receives the deterministic Markdown itself (operator-explanation
/// D8). `--json` output is never skinned.
pub(crate) fn emit_report(text: &str, mode: tokeira_report::Mode) {
    use std::io::IsTerminal;
    if matches!(mode.form, tokeira_report::Form::Narrative) && std::io::stdout().is_terminal() {
        termimad::print_text(text);
    } else {
        print!("{text}");
    }
}

/// Map an apply's committed changes into the explanation's vocabulary at the
/// shell boundary (operator-explanation Req 1): the model crate must not depend on
/// the engine types (Req 9.1), so identity crosses here — ids, operations,
/// the module/type halves of the natural key, and the operator noun. Never
/// before-images (Proposal 002). `NoChange` rows cross as the unchanged
/// census — they render nothing, but they carry the noun ambiguity the
/// renderer needs.
pub(crate) fn committed_changes(applied: &AppliedOutcome) -> Vec<tokeira_explain::CommittedChange> {
    applied
        .changes
        .iter()
        .map(|change| {
            let op = match change.kind {
                tokeira_iac::ChangeKind::Create => tokeira_explain::CommittedOp::Created,
                tokeira_iac::ChangeKind::Update => tokeira_explain::CommittedOp::Updated,
                tokeira_iac::ChangeKind::Replace => tokeira_explain::CommittedOp::Replaced,
                tokeira_iac::ChangeKind::Delete => tokeira_explain::CommittedOp::Deleted,
                tokeira_iac::ChangeKind::NoChange => tokeira_explain::CommittedOp::Unchanged,
            };
            tokeira_explain::CommittedChange {
                id: change.resource.clone(),
                op,
                module: change.module.clone(),
                resource_type: change.resource_type.clone(),
                display: applied
                    .display_by_id
                    .get(&tokeira_iac::ResourceId(change.resource.clone()))
                    .cloned(),
            }
        })
        .collect()
}

/// The deployment-level envelope store.
///
/// For now a local CAS store under `{deployment_dir}/state/envelope`; cloud
/// deployments will select an `S3StateStore` through the platform store seam
/// (task 13.2), just like the infra/runtime state.
/// The retarget gate, run by every config-applying verb after the binding
/// gate and before any mutation: a `#[create]` change is a new deployment,
/// not an apply. No retained prior revision means nothing has ever applied,
/// so there is nothing to gate — Day-0 `init` retains revision 0 from the
/// operator's already-edited definition, which is exactly why choosing a
/// `#[create]` value *before* the first apply is legitimate and changing it
/// *after* is refused.
pub(crate) async fn retarget_gate<P: ProvisionerPlatform>(
    platform: &P,
    deployment_dir: &Path,
    envelope: &DeploymentStateEnvelope,
) -> Result<()> {
    let config_source = platform.config_source(deployment_dir)?;
    let Some(prior) =
        config_history::retained_source(deployment_dir, &config_source, envelope.config_revision)?
    else {
        return Ok(());
    };
    let live_path = config_history::config_file(deployment_dir, &config_source);
    let current = std::fs::read_to_string(&live_path).with_context(|| {
        format!(
            "failed to read the live definition at {}",
            live_path.display()
        )
    })?;
    match platform
        .retarget_check(deployment_dir, &prior, &current)
        .await?
    {
        Realization::Realized(()) | Realization::NotApplicable { .. } => Ok(()),
    }
}

pub(crate) fn envelope_store(
    deployment_dir: &Path,
) -> Box<dyn DeploymentStore<DeploymentStateEnvelope>> {
    Box::new(CasStore::new(
        Box::new(LocalBackend::new(deployment_dir.join("state/envelope"))),
        "envelope".to_string(),
    ))
}

/// A no-op platform for exercising the shell in tests: empty plans, zero-change
/// applies, `deployment.toml` config keying, and the historical `"tokeira"`
/// default identity — the same observable behavior the `local` platform's empty
/// apply gave the shell's tests before the extraction.
#[cfg(test)]
pub(crate) struct TestPlatform;

#[cfg(test)]
impl ProvisionerPlatform for TestPlatform {
    fn label(&self, _deployment_dir: &Path) -> &'static str {
        "test"
    }

    fn config_source(&self, _deployment_dir: &Path) -> Result<ConfigSource> {
        ConfigSource::legacy("deployment.toml")
    }

    fn deployment_id(&self, _deployment_dir: &Path) -> Result<String> {
        Ok("tokeira".to_string())
    }

    async fn infra_plan(
        &self,
        _deployment_dir: &Path,
        _module: Option<&str>,
    ) -> Result<PlanOutcome> {
        Ok(PlanOutcome::default())
    }

    async fn infra_apply(
        &self,
        _deployment_dir: &Path,
        _module: Option<&str>,
    ) -> Result<AppliedOutcome> {
        Ok(AppliedOutcome::default())
    }

    async fn infra_destroy(&self, _deployment_dir: &Path, _module: Option<&str>) -> Result<usize> {
        Ok(0)
    }

    async fn infra_destroy_selected(
        &self,
        _deployment_dir: &Path,
        ids: &[String],
    ) -> Result<Vec<ChangeLogEntry>> {
        // The no-op platform "deletes" whatever it is asked to: the shell's
        // rollback tests observe the pass through the returned entries.
        Ok(ids
            .iter()
            .map(|id| ChangeLogEntry {
                id: id.clone(),
                op: ChangeOp::Deleted,
            })
            .collect())
    }

    async fn deploy_plan(&self, _deployment_dir: &Path) -> Result<Realization<PlanOutcome>> {
        Ok(Realization::Realized(PlanOutcome::default()))
    }

    async fn deploy_apply(&self, _deployment_dir: &Path) -> Result<Realization<AppliedOutcome>> {
        Ok(Realization::Realized(AppliedOutcome::default()))
    }

    async fn scale(&self, _deployment_dir: &Path, _specs: &[String]) -> Result<Realization<usize>> {
        Ok(Realization::NotApplicable {
            reason: "the test platform has no scale dimension",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_iac::ChangeKind;

    fn change(kind: ChangeKind, module: &str, resource: &str) -> Change {
        Change {
            kind,
            resource_type: "t".to_string(),
            module: module.to_string(),
            resource: resource.to_string(),
            details: Vec::new(),
        }
    }

    // Causality Requirement 1.5: a platform with no interpreted definition
    // answers the snapshot with the typed refusal, so the classifier's A10
    // branch has a value, never a panic or a missing method.
    #[tokio::test]
    async fn desired_snapshot_defaults_to_not_applicable() {
        let answer = TestPlatform
            .desired_snapshot(Path::new("/tmp/x"), Path::new("/tmp/x/def"))
            .await
            .expect("the default never errors");
        assert!(matches!(answer, Realization::NotApplicable { .. }));
    }

    // Task 19.2/19.3: the audit mapping — ids are the engine's ResourceId
    // (the destroy_selected key), Replace folds to Updated (the id survives
    // the delete-then-recreate), NoChange vanishes.
    #[test]
    fn change_log_entries_map_ids_and_ops() {
        let entries = change_log_entries(&[
            change(ChangeKind::Create, "vpc", "main-vpc"),
            change(ChangeKind::Update, "svc", "web"),
            change(ChangeKind::Replace, "db", "primary"),
            change(ChangeKind::Delete, "svc", "old"),
            change(ChangeKind::NoChange, "svc", "steady"),
        ]);
        assert_eq!(entries.len(), 4, "NoChange records nothing");
        assert_eq!(
            entries[0].id, "main-vpc",
            "the id IS the engine ResourceId — the delete-set key"
        );
        assert_eq!(entries[0].op, ChangeOp::Created);
        assert_eq!(entries[1].op, ChangeOp::Updated);
        assert_eq!(
            entries[2].op,
            ChangeOp::Updated,
            "Replace records as Updated — identities, not mechanics"
        );
        assert_eq!(entries[3].op, ChangeOp::Deleted);
    }
}
