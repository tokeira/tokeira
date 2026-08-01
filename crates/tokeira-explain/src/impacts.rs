//! Deterministic impact derivation: declared semantics in, operational
//! consequences out (change-semantics Requirement 5).
//!
//! A pure function of the declarations: two plans with identical
//! declarations produce identical impacts, in identical order — most
//! consequential first (the [`ImpactClass`] declaration order). `Inference`
//! confidence contributes — a renderer marks such impacts as derived by
//! inspecting the subjects' declarations — while `Unknown` contributes
//! nothing: an unknown never becomes a claim, it becomes uncertainty
//! (Requirement 6, activated in [`crate::build`]).

use tokeira_iac::{ChangeKind, DataEffect, Disruption, ReplacementPolicy};

use crate::{
    evidence::EvidenceId,
    model::{ExplainedChange, ImpactClass, OperationalImpact},
};

/// Derive the plan's operational impacts from its changes' declared
/// semantics, severity-first.
///
/// `deployment` anchors each impact's evidence identity — one impact per
/// class per plan, so the natural key is `impact:{class}:{deployment}` (a
/// deviation from the design sketch's changes-only signature, recorded
/// here: the id must resolve in the evidence index, and the plan is the
/// impact's subject-of-record; the justifying changes are its `subjects`).
pub fn derive_impacts(
    deployment: &EvidenceId,
    changes: &[ExplainedChange],
) -> Vec<OperationalImpact> {
    let mut impacts = Vec::new();
    for class in [
        ImpactClass::DataDestroyed,
        ImpactClass::Unavailability,
        ImpactClass::Replacement,
        ImpactClass::BriefInterruption,
        ImpactClass::RollingReplacement,
    ] {
        // DependencyLoss is derived by causality (it needs the graph delta),
        // never here.
        let mut subjects: Vec<&ExplainedChange> =
            changes.iter().filter(|c| triggers(c, class)).collect();
        if subjects.is_empty() {
            continue;
        }
        subjects.sort_by(|a, b| a.evidence_id.cmp(&b.evidence_id));
        let resources = subjects
            .iter()
            .map(|c| c.resource_id.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        impacts.push(OperationalImpact {
            evidence_id: EvidenceId::impact(class.tag(), deployment),
            class,
            subjects: subjects.iter().map(|c| c.evidence_id.clone()).collect(),
            statement: statement(class, &resources),
            lost: None,
        });
    }
    impacts
}

/// Whether a change's declarations justify membership in `class`. A
/// declaration counts at any confidence above `Unknown` — `value()` is
/// `None` exactly for `Unknown`.
fn triggers(change: &ExplainedChange, class: ImpactClass) -> bool {
    let semantics = &change.semantics;
    match class {
        ImpactClass::DataDestroyed => semantics.data_effect.value() == Some(&DataEffect::Destroyed),
        ImpactClass::Unavailability => {
            // Two trigger families (Requirement 5.2/5.5): the declared
            // disruption, and the engine's own classification — a delete
            // removes the resource, and the engine executes a replace as
            // delete-then-create, so unavailability is the floor with no
            // declaration required. A declared create-before-destroy (above
            // Unknown) lifts the replace window; nothing lifts a delete.
            let declared =
                semantics.disruption.value() == Some(&Disruption::UnavailableDuringChange);
            let engine_floor = match change.kind {
                ChangeKind::Delete => true,
                ChangeKind::Replace => {
                    semantics.replacement.value() != Some(&ReplacementPolicy::CreateBeforeDestroy)
                }
                _ => false,
            };
            declared || engine_floor
        }
        ImpactClass::Replacement => {
            // Declared, or the engine's own Replace classification
            // (Requirement 5.5) — the floor renders undeclared.
            matches!(
                semantics.replacement.value(),
                Some(policy) if *policy != ReplacementPolicy::NotRequired
            ) || change.kind == ChangeKind::Replace
        }
        ImpactClass::BriefInterruption => {
            semantics.disruption.value() == Some(&Disruption::BriefInterruption)
        }
        ImpactClass::RollingReplacement => {
            semantics.disruption.value() == Some(&Disruption::Rolling)
        }
        // Never triggered here: causality derives it from the graph delta.
        ImpactClass::DependencyLoss => false,
    }
}

/// The deterministic statement template per class — never free prose. The
/// data-destruction statement names the irreversibility of the loss
/// unconditionally: `Destroyed` means the data does not come back (a
/// recoverable effect would have been declared `Preserved` or `Migrated`) —
/// distinct from the change's own reversibility, which speaks about the
/// resource.
fn statement(class: ImpactClass, resources: &str) -> String {
    match class {
        ImpactClass::DataDestroyed => {
            format!("destroys the data held by {resources}; this loss is not reversible")
        }
        ImpactClass::Unavailability => {
            format!("makes {resources} unavailable while the change applies")
        }
        ImpactClass::Replacement => format!("replaces {resources}"),
        ImpactClass::DependencyLoss => {
            format!("{resources} continues without a dependency this plan deletes")
        }
        ImpactClass::BriefInterruption => format!("briefly interrupts {resources}"),
        ImpactClass::RollingReplacement => format!("replaces {resources} one at a time"),
    }
}
