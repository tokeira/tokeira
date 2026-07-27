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
            kind: change.kind.clone(),
            field_diffs: change.details.clone(),
            refresh_status,
            semantics: Default::default(),
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

    derive_refresh_uncertainties(&mut explanation, outcome);
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
            kind: kind.clone(),
            field_diffs,
            refresh_status,
            semantics: Default::default(),
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

fn push_uncertainty(
    explanation: &mut DeploymentExplanation,
    subject: EvidenceId,
    reason: UncertaintyReason,
    consequence: String,
    resolvable_by: Option<String>,
) {
    let evidence_id = EvidenceId::uncertainty(reason.tag(), &subject);
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
