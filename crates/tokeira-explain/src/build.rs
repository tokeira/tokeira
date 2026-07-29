//! Construction: engine outputs in, explanation model out.
//!
//! Pure functions of their inputs — no I/O, no clock, no provider, so two
//! constructions from identical inputs serialize byte-identically (Property
//! 2; the `BTreeMap`s everywhere upstream make "identical inputs" a
//! well-defined phrase). Construction adds structure and honesty, never
//! facts: every change comes from the engine, every uncertainty from a
//! source the engine reported.

use tokeira_iac::{ChangeKind, PlanOutcome, RefreshStatus, ResourceId};

use crate::{
    evidence::{EvidenceId, EvidenceIndex, EvidenceKind},
    model::{
        CommittedChange, CommittedOp, DeploymentExplanation, EXPLANATION_SCHEMA_VERSION,
        ExplainedChange, Uncertainty, UncertaintyReason,
    },
};

/// The deployment facts the engine does not know; the shell supplies them
/// from the envelope and the platform (evidence-model field policy).
#[derive(Debug, Clone)]
pub struct DeploymentContext {
    pub deployment: String,
    pub platform: String,
    pub operation: String,
    pub current_revision: u64,
    pub proposed_revision: Option<u64>,
    pub definition_ref: Option<String>,
}

/// Explain a plan: one explained change per engine change (`NoChange`
/// included), the destructive set from the engine's own classification, and
/// uncertainty derived from refresh coverage.
pub fn explain_plan(context: DeploymentContext, outcome: &PlanOutcome) -> DeploymentExplanation {
    let mut explanation = base(context);

    for change in &outcome.changes {
        let evidence_id = EvidenceId::change(&change.module, &change.resource);
        let refresh_status = if outcome.refresh.examined {
            outcome
                .refresh
                .status_by_id
                .get(&ResourceId(change.resource.clone()))
                .copied()
        } else {
            None
        };
        explanation.changes.push(ExplainedChange {
            evidence_id: evidence_id.clone(),
            resource_id: change.resource.clone(),
            module: change.module.clone(),
            resource_type: change.resource_type.clone(),
            kind: change.kind,
            field_diffs: change.details.clone(),
            refresh_status,
            // Verbatim from the kind's declaration (change-semantics
            // Property 3): the explanation transports, it never amends. An
            // absent id declares nothing — all fields Unknown.
            semantics: outcome
                .semantics_by_id
                .get(&ResourceId(change.resource.clone()))
                .cloned()
                .unwrap_or_default(),
            cause: Default::default(),
            dependants: Vec::new(),
            source: None,
        });
        explanation
            .evidence
            .insert(evidence_id.clone(), EvidenceKind::Change);
        if change.kind.is_destructive() {
            explanation.destructive.push(evidence_id);
        }
    }

    explanation.impacts = crate::impacts::derive_impacts(
        &EvidenceId::deployment(&explanation.deployment),
        &explanation.changes,
    );
    for impact in &explanation.impacts {
        explanation
            .evidence
            .insert(impact.evidence_id.clone(), EvidenceKind::Impact);
    }

    derive_refresh_uncertainties(&mut explanation, outcome);
    derive_semantics_uncertainties(&mut explanation, outcome);
    derive_semantics_undeclared(&mut explanation);
    explanation
}

/// Explain an apply from its committed entries, within Proposal 002's
/// ids-only constraint: field evidence comes only from a preceding plan of
/// the same invocation, and its absence is an uncertainty, never a
/// fabricated before-image (Requirement 2).
pub fn explain_applied(
    context: DeploymentContext,
    committed: &[CommittedChange],
    preceding: Option<&PlanOutcome>,
) -> DeploymentExplanation {
    let mut explanation = base(context);

    for entry in committed {
        // The audit log records bare engine ResourceIds; the module half of
        // the natural key is unknown here, and inventing one would collide
        // with plan-side ids dishonestly. The id form is
        // `change:{resource_id}` alone — stable, and distinct by
        // construction from plan-side ids.
        let evidence_id = EvidenceId::change("", &entry.id);
        let kind = match entry.op {
            CommittedOp::Created => ChangeKind::Create,
            CommittedOp::Updated => ChangeKind::Update,
            CommittedOp::Deleted => ChangeKind::Delete,
        };
        let planned = preceding.and_then(|plan| {
            plan.changes
                .iter()
                .find(|change| change.resource == entry.id)
        });
        let field_diffs = planned.map(|p| p.details.clone()).unwrap_or_default();
        let refresh_status = preceding.and_then(|plan| {
            plan.refresh
                .examined
                .then(|| {
                    plan.refresh
                        .status_by_id
                        .get(&ResourceId(entry.id.clone()))
                        .copied()
                })
                .flatten()
        });

        explanation.changes.push(ExplainedChange {
            evidence_id: evidence_id.clone(),
            resource_id: entry.id.clone(),
            module: String::new(),
            resource_type: String::new(),
            kind,
            field_diffs,
            refresh_status,
            // Reused from the preceding plan's declarations when one exists
            // — the same reuse-never-invent rule as field evidence (Req 2).
            semantics: preceding
                .and_then(|plan| plan.semantics_by_id.get(&ResourceId(entry.id.clone())))
                .cloned()
                .unwrap_or_default(),
            cause: Default::default(),
            dependants: Vec::new(),
            source: None,
        });
        explanation
            .evidence
            .insert(evidence_id.clone(), EvidenceKind::Change);
        if kind.is_destructive() {
            explanation.destructive.push(evidence_id.clone());
        }

        if preceding.is_none() {
            push_uncertainty(
                &mut explanation,
                evidence_id,
                UncertaintyReason::FieldEvidenceUnavailable,
                "the committed change's field-level evidence was not captured (the audit log \
                 records identities only)"
                    .to_string(),
                Some("run the plan and apply in one invocation".to_string()),
            );
        }
    }

    // Impacts derive from the reused declarations — a pure function of the
    // changes, identical rules to the plan side. Undeclared-semantics
    // uncertainty stays plan-scoped (Requirement 6 speaks of the plan; an
    // apply reports what happened, and gap-hunting a done thing is noise).
    explanation.impacts = crate::impacts::derive_impacts(
        &EvidenceId::deployment(&explanation.deployment),
        &explanation.changes,
    );
    for impact in &explanation.impacts {
        explanation
            .evidence
            .insert(impact.evidence_id.clone(), EvidenceKind::Impact);
    }

    explanation
}

fn base(context: DeploymentContext) -> DeploymentExplanation {
    let mut evidence = EvidenceIndex::default();
    evidence.insert(
        EvidenceId::deployment(&context.deployment),
        EvidenceKind::Deployment,
    );
    DeploymentExplanation {
        schema_version: EXPLANATION_SCHEMA_VERSION,
        deployment: context.deployment,
        platform: context.platform,
        operation: context.operation,
        current_revision: context.current_revision,
        proposed_revision: context.proposed_revision,
        definition_ref: context.definition_ref,
        changes: Vec::new(),
        impacts: Vec::new(),
        destructive: Vec::new(),
        uncertainties: Vec::new(),
        evidence,
    }
}

/// Requirement 4: `RefreshStatus::Unknown` on a planned resource is an
/// uncertainty; an unexamined verb is exactly one plan-level uncertainty —
/// "no check happened" is a different statement from "everything confirmed",
/// and both differ from silence.
fn derive_refresh_uncertainties(explanation: &mut DeploymentExplanation, outcome: &PlanOutcome) {
    if !outcome.refresh.examined {
        let subject = EvidenceId::deployment(&explanation.deployment);
        push_uncertainty(
            explanation,
            subject,
            UncertaintyReason::LiveStateNotExamined,
            "this operation performed no live-state check; every claim compares desired \
             state against recorded state only"
                .to_string(),
            Some("run `infra plan` to examine live state".to_string()),
        );
        return;
    }
    let unconfirmed: Vec<EvidenceId> = explanation
        .changes
        .iter()
        .filter(|change| matches!(change.refresh_status, Some(RefreshStatus::Unknown)))
        .map(|change| change.evidence_id.clone())
        .collect();
    for subject in unconfirmed {
        push_uncertainty(
            explanation,
            subject,
            UncertaintyReason::LiveStateUnconfirmed,
            "live state could not be confirmed for this resource; the plan compares \
             desired state against records, not observations"
                .to_string(),
            Some("make the platform reachable and re-run the plan".to_string()),
        );
    }
}

/// Change-semantics Requirement 3.4: a deletion whose id is absent from the
/// declaration map means no recoverer claimed the resource's type — the kind
/// could not be reached to say what the delete does. Stated as uncertainty,
/// never silence. Non-deletions are always reachable (they sit in the
/// desired set), so absence there carries no such meaning.
fn derive_semantics_uncertainties(explanation: &mut DeploymentExplanation, outcome: &PlanOutcome) {
    let undeclared: Vec<(EvidenceId, String)> = explanation
        .changes
        .iter()
        .filter(|change| {
            change.kind == ChangeKind::Delete
                && !outcome
                    .semantics_by_id
                    .contains_key(&ResourceId(change.resource_id.clone()))
        })
        .map(|change| (change.evidence_id.clone(), change.resource_type.clone()))
        .collect();
    for (subject, resource_type) in undeclared {
        push_uncertainty(
            explanation,
            subject,
            UncertaintyReason::KindUnavailableForRemovedResource,
            "no declaration can state what this deletion does; the resource's type is \
             not registered for recovery"
                .to_string(),
            Some(format!(
                "register a `ResourceRecovery` for `{resource_type}`"
            )),
        );
    }
}

/// Requirement 6: undeclared semantics become uncertainty — a gap, never
/// silence. Per field: a change whose field is `Unknown` while the field is
/// stated for another change in the plan gets its own uncertainty; a field
/// `Unknown` across every applicable change collapses to one plan-level
/// uncertainty (the noise control — an undeclared world yields at most one
/// line per field, not one per change); a kind the field does not apply to
/// is never named.
fn derive_semantics_undeclared(explanation: &mut DeploymentExplanation) {
    // Applicability is a fixed property of the change kind, not of what a
    // declaration happens to state: replacement is about how an update is
    // effected, and a creation acts on no existing data.
    fn applies(field: SemanticField, kind: ChangeKind) -> bool {
        match field {
            SemanticField::Replacement => matches!(kind, ChangeKind::Update | ChangeKind::Replace),
            SemanticField::DataEffect => {
                matches!(
                    kind,
                    ChangeKind::Update | ChangeKind::Replace | ChangeKind::Delete
                )
            }
            SemanticField::Operation | SemanticField::Disruption | SemanticField::Reversibility => {
                kind != ChangeKind::NoChange
            }
        }
    }

    let deployment = EvidenceId::deployment(&explanation.deployment);
    for field in SemanticField::ALL {
        let applicable: Vec<(EvidenceId, String, String)> = explanation
            .changes
            .iter()
            .filter(|change| change.kind != ChangeKind::NoChange && applies(field, change.kind))
            .map(|change| {
                (
                    change.evidence_id.clone(),
                    change.resource_type.clone(),
                    if field.is_known(&change.semantics) {
                        String::new()
                    } else {
                        change.resource_id.clone()
                    },
                )
            })
            .collect();
        if applicable.is_empty() {
            continue;
        }
        let unknowns: Vec<&(EvidenceId, String, String)> = applicable
            .iter()
            .filter(|(_, _, gap)| !gap.is_empty())
            .collect();
        if unknowns.is_empty() {
            continue;
        }
        if unknowns.len() == applicable.len() {
            push_uncertainty(
                explanation,
                deployment.clone(),
                UncertaintyReason::SemanticsUndeclared {
                    field: field.wire_name().to_string(),
                },
                format!("no declaration states any change's {}", field.noun()),
                Some("declare `change_semantics` for this plan's resource types".to_string()),
            );
        } else {
            for (subject, resource_type, _) in unknowns {
                push_uncertainty(
                    explanation,
                    subject.clone(),
                    UncertaintyReason::SemanticsUndeclared {
                        field: field.wire_name().to_string(),
                    },
                    format!("no declaration states this change's {}", field.noun()),
                    Some(format!(
                        "declare `{}` in `{resource_type}`'s `change_semantics`",
                        field.wire_name(),
                    )),
                );
            }
        }
    }
}

/// The five declared fields, iterated in one fixed order so uncertainty
/// output is deterministic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SemanticField {
    Operation,
    Replacement,
    Disruption,
    DataEffect,
    Reversibility,
}

impl SemanticField {
    const ALL: [SemanticField; 5] = [
        SemanticField::Operation,
        SemanticField::Replacement,
        SemanticField::Disruption,
        SemanticField::DataEffect,
        SemanticField::Reversibility,
    ];

    /// The wire name carried in `SemanticsUndeclared { field }` — the
    /// model's own serialized field names, stable for `--json` consumers.
    fn wire_name(self) -> &'static str {
        match self {
            SemanticField::Operation => "operation",
            SemanticField::Replacement => "replacement",
            SemanticField::Disruption => "disruption",
            SemanticField::DataEffect => "data_effect",
            SemanticField::Reversibility => "reversibility",
        }
    }

    /// The operator noun used in consequence prose.
    fn noun(self) -> &'static str {
        match self {
            SemanticField::Operation => "operation",
            SemanticField::Replacement => "replacement",
            SemanticField::Disruption => "disruption",
            SemanticField::DataEffect => "data effect",
            SemanticField::Reversibility => "reversibility",
        }
    }

    fn is_known(self, semantics: &tokeira_iac::ChangeSemantics) -> bool {
        match self {
            SemanticField::Operation => semantics.operation.is_known(),
            SemanticField::Replacement => semantics.replacement.is_known(),
            SemanticField::Disruption => semantics.disruption.is_known(),
            SemanticField::DataEffect => semantics.data_effect.is_known(),
            SemanticField::Reversibility => semantics.reversibility.is_known(),
        }
    }
}

fn push_uncertainty(
    explanation: &mut DeploymentExplanation,
    subject: EvidenceId,
    reason: UncertaintyReason,
    consequence: String,
    resolvable_by: Option<String>,
) {
    // The natural key must distinguish facts: five plan-level undeclared
    // fields on one subject are five uncertainties, so the field joins the
    // tag for that reason.
    let evidence_id = match &reason {
        UncertaintyReason::SemanticsUndeclared { field } => {
            EvidenceId::uncertainty(&format!("{}:{field}", reason.tag()), &subject)
        }
        _ => EvidenceId::uncertainty(reason.tag(), &subject),
    };
    explanation
        .evidence
        .insert(evidence_id.clone(), EvidenceKind::Uncertainty);
    explanation.uncertainties.push(Uncertainty {
        evidence_id,
        subject,
        reason,
        consequence,
        resolvable_by,
    });
}
