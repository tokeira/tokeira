//! The explanation model's types, mirroring the field policy in
//! `.kiro/specs/explanation-evidence-model/requirements.md`: every field
//! names its source of truth there, and its documented fallback when the
//! source cannot supply it. No field is silently omitted — the policy is the
//! schema's contract, and construction (`build`) honors it.

use serde::{Deserialize, Serialize};
use tokeira_iac::{ChangeKind, ChangeSemantics, Confidence, FieldDiff, RefreshStatus};

use crate::evidence::{EvidenceId, EvidenceIndex};

/// The artifact schema version. Bumped only for breaking shape changes;
/// slot population (Features 2–4) is additive by design (Requirement 8.3).
pub const EXPLANATION_SCHEMA_VERSION: u32 = 1;

/// One operation's complete explanation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeploymentExplanation {
    pub schema_version: u32,
    /// The deployment's identity (envelope `deployment_id`).
    pub deployment: String,
    /// The platform label (e.g. `compose`).
    pub platform: String,
    /// The verb this explains: `"infra plan"`, `"infra apply"`, ….
    pub operation: String,
    /// The last applied revision (`0` for a never-applied deployment).
    pub current_revision: u64,
    /// The revision a mutating verb would produce; absent for read-only verbs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proposed_revision: Option<u64>,
    /// The effective definition digest (`None` renders as "default").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub definition_ref: Option<String>,
    /// Exactly one entry per engine change, `NoChange` included (Req 1.3).
    pub changes: Vec<ExplainedChange>,
    /// Derived from declared semantics — empty until Feature 2 supplies them.
    pub impacts: Vec<OperationalImpact>,
    /// References into `changes`, derived from the engine's own
    /// classification (`ChangeKind::is_destructive`) — never from
    /// declarations (Feature 2, Requirement 4).
    pub destructive: Vec<EvidenceId>,
    pub uncertainties: Vec<Uncertainty>,
    /// Causal groups over the non-`NoChange` changes (Feature 3,
    /// Requirement 4): a partition, each group hanging from its bounded
    /// ultimate root. `serde(default)` keeps pre-causality artifacts
    /// readable — the slot pattern, applied at the document level.
    #[serde(default)]
    pub causal_groups: Vec<CausalGroup>,
    /// Platform issues (output-templates §Platform Issue lines): the fact,
    /// the SDK error verbatim as evidence, and a direction only where the
    /// error itself establishes one. When present, no change sections render
    /// and the verb exits non-zero — a plan only renders what could execute.
    /// Populated by the platform-issue seam at the describe boundary.
    #[serde(default)]
    pub platform_issues: Vec<PlatformIssue>,
    pub evidence: EvidenceIndex,
}

/// One platform issue: the fact, the platform's own answer, and — only
/// where the error itself establishes one — a factually grounded direction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlatformIssue {
    pub evidence_id: EvidenceId,
    /// The fact, naming the platform component ("Unable to connect to
    /// Docker").
    pub fact: String,
    /// The SDK error verbatim — the platform's words, rendered as evidence,
    /// never blended into Tokeira's own sentence.
    pub evidence: String,
    /// Direction the error establishes; `None` when it establishes none —
    /// direction is never assumed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub direction: Option<String>,
}

/// One causal story: the changes sharing a root, ordered along the
/// dependency path from the root outward (Requirement 4.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CausalGroup {
    pub evidence_id: EvidenceId,
    pub root: CausalRoot,
    /// Member change ids, dependency-path order from the root, ties and
    /// unconnected members ordered by id.
    pub members: Vec<EvidenceId>,
}

/// What a causal group hangs from (Requirement 4.3). Cascade chains walk to
/// their first non-cascade cause and take *that* cause's root — one story —
/// and the walk is bounded by the engine-version and baseline-revision
/// boundaries (Requirement 4.6): an engine-advance origin roots here at
/// `ProvisionerAdvance` and reaches no further back.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CausalRoot {
    /// Definition edits: the baseline-vs-working comparison itself.
    RevisionComparison { baseline: u64 },
    /// A resource's change: drift roots, output-trace dependencies (their
    /// change may be `NoChange` — it still carries an id), unknown causes
    /// (each its own root).
    Resource(EvidenceId),
    /// The provisioner advance: the engine-version boundary, and terminal.
    ProvisionerAdvance,
}

/// One resource change, enriched with everything the layer knows about it.
/// The `semantics`, `cause`, `dependants`, and `source` fields are slots:
/// present from schema v1, populated by Features 2–4, rendered as nothing
/// while not determined (Requirement 8).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExplainedChange {
    pub evidence_id: EvidenceId,
    pub resource_id: String,
    /// The kind's operator-facing noun ("service", "Aurora DSQL cluster"),
    /// composed into prose by renderers; `None` falls back to identifiers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display: Option<String>,
    pub module: String,
    pub resource_type: String,
    pub kind: ChangeKind,
    pub field_diffs: Vec<FieldDiff>,
    /// The refresh confirmation behind this change; `None` means the verb
    /// performed no live-state examination at all.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_status: Option<RefreshStatus>,
    #[serde(default)]
    pub semantics: ChangeSemantics,
    #[serde(default)]
    pub cause: Confidence<Cause>,
    #[serde(default)]
    pub dependants: Vec<String>,
    /// Fields of this change whose live value departed from the record —
    /// the per-diff drift annotation on an edited-and-drifted resource (the
    /// edit owns the change; the drift fact stays visible per field).
    /// Computed by causality against the store-read record; empty when live
    /// was not confirmed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub departed_fields: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceLocation>,
}

/// Why a change is present. Populated by Feature 3's classification algebra;
/// until then every change's cause is `Confidence::Unknown` (the
/// `Undetermined` variant the original design carried was removed by the
/// Feature 3 amendment — it duplicated what `Confidence::Unknown` says).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Cause {
    DefinitionEdit {
        #[serde(skip_serializing_if = "Option::is_none")]
        source: Option<SourceLocation>,
    },
    DependencyOutputChanged {
        dependency: String,
    },
    ProviderDrift,
    ReplacementCascade {
        root: String,
    },
    EngineAdvance,
}

/// A definition location, in the working definition's coordinates.
/// Feature 4 populates it; Feature 4's amendment adds the attribution basis.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceLocation {
    pub file: String,
    pub line: u32,
    pub column: u32,
}

/// An operational consequence of the plan as a whole, derived
/// deterministically from declared semantics (Feature 2 owns the
/// derivation; the type exists from v1 so the field does).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationalImpact {
    pub evidence_id: EvidenceId,
    pub class: ImpactClass,
    /// The changes that justify the impact.
    pub subjects: Vec<EvidenceId>,
    /// A deterministic template rendering — never free prose.
    pub statement: String,
    /// For [`ImpactClass::DependencyLoss`]: the deleted dependency's change,
    /// so a renderer names what is lost by resolving evidence — never by
    /// echoing free prose.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lost: Option<EvidenceId>,
}

/// Impact classes, ordered most consequential first; the ordering is the
/// rendering order (Feature 2, Requirement 5.7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ImpactClass {
    DataDestroyed,
    /// A resource continues without a dependency this plan deletes — the
    /// desired graph dropped a recorded edge whose target is being removed.
    /// Permanent, so it outranks the transient classes below (the enum order
    /// is the severity order).
    DependencyLoss,
    Unavailability,
    Replacement,
    BriefInterruption,
    RollingReplacement,
}

impl ImpactClass {
    /// The stable kebab tag used in evidence identity (`impact:{tag}:…`).
    pub fn tag(&self) -> &'static str {
        match self {
            ImpactClass::DataDestroyed => "data-destroyed",
            ImpactClass::DependencyLoss => "dependency-loss",
            ImpactClass::Unavailability => "unavailability",
            ImpactClass::Replacement => "replacement",
            ImpactClass::BriefInterruption => "brief-interruption",
            ImpactClass::RollingReplacement => "rolling-replacement",
        }
    }
}

/// A modelled statement that something could not be determined — the
/// feature that makes a quiet plan distinguishable from an uninformed one
/// (Requirement 4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Uncertainty {
    pub evidence_id: EvidenceId,
    /// What the uncertainty qualifies; always resolvable in the index
    /// (Property 3).
    pub subject: EvidenceId,
    pub reason: UncertaintyReason,
    /// What the operator cannot rely on as a result.
    pub consequence: String,
    /// The action that would resolve it, where one exists.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolvable_by: Option<String>,
}

/// Why something could not be determined.
///
/// `SemanticsUndeclared` and `ProviderAssignedAtApply` are defined here and
/// **not constructed by Feature 1**: with every semantics slot
/// not-determined, the renderer states no semantics, so nothing is left
/// unqualified — emitting them now would attach an uncertainty to every
/// change in every plan and teach operators to skip the section. They
/// activate with Feature 2.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum UncertaintyReason {
    /// Refresh could not confirm live state (`RefreshStatus::Unknown`).
    LiveStateUnconfirmed,
    /// The verb performed no live-state check at all.
    LiveStateNotExamined,
    /// An apply with no preceding plan in the same invocation: committed ids
    /// are known, field-level evidence is not (Proposal 002's ids-only
    /// audit).
    FieldEvidenceUnavailable,
    /// Reserved for Feature 2's Phase 5 (activation is deliberate — emitting
    /// it before any kind can declare would attach an uncertainty to every
    /// change in every plan).
    SemanticsUndeclared { field: String },
    /// Reserved for Feature 2.
    ProviderAssignedAtApply { field: String },
    /// A deletion whose resource type no recoverer claims: the kind cannot
    /// be reached to declare what the delete does, and that absence is
    /// stated rather than silent (change-semantics Requirement 3.4).
    KindUnavailableForRemovedResource,
    /// The cause algebra could not decide why this resource changed —
    /// typically a drift-shaped condition over an unconfirmed live read,
    /// which the algebra refuses to call drift (causality A9). Constructed
    /// only by the cause classifier.
    CauseUndecidable { resource: String },
    /// The baseline revision's retained definition could not be realized
    /// (missing or does not interpret), so revision comparison — and with it
    /// most of the cause algebra — is unavailable (causality A10). One per
    /// plan, naming the revision. Constructed only by the cause classifier.
    BaselineUnavailable { revision: u64 },
}

impl UncertaintyReason {
    /// The stable tag used in evidence ids.
    pub fn tag(&self) -> &'static str {
        match self {
            UncertaintyReason::LiveStateUnconfirmed => "live-state-unconfirmed",
            UncertaintyReason::LiveStateNotExamined => "live-state-not-examined",
            UncertaintyReason::FieldEvidenceUnavailable => "field-evidence-unavailable",
            UncertaintyReason::SemanticsUndeclared { .. } => "semantics-undeclared",
            UncertaintyReason::ProviderAssignedAtApply { .. } => "provider-assigned-at-apply",
            UncertaintyReason::KindUnavailableForRemovedResource => {
                "kind-unavailable-for-removed-resource"
            }
            UncertaintyReason::CauseUndecidable { .. } => "cause-undecidable",
            UncertaintyReason::BaselineUnavailable { .. } => "baseline-unavailable",
        }
    }
}

/// One committed change from an apply, in the model's own vocabulary so
/// this crate depends on no engine or provisioner types (Requirement 9.1);
/// the shell maps at its boundary. Identity only — id, operation, the
/// module/type halves of the natural key, and the operator noun — never
/// before-images (Proposal 002).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedChange {
    pub id: String,
    pub op: CommittedOp,
    /// The definition-side module name; empty when the apply's source could
    /// not name one (the evidence id then omits the module half rather than
    /// inventing it).
    #[serde(default)]
    pub module: String,
    #[serde(default)]
    pub resource_type: String,
    /// The operator noun (`"service"`, `"Aurora DSQL cluster"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display: Option<String>,
}

/// What an apply did to one resource. `Replaced` is the engine's own
/// classification — the persisted audit log folds it to updated (identities,
/// not mechanics), but the applied report states the mechanics. `Unchanged`
/// rows are the census: they render no line, and they are why a noun like
/// "service" can be told ambiguous without re-reading the definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CommittedOp {
    Created,
    Updated,
    Replaced,
    Deleted,
    Unchanged,
}
