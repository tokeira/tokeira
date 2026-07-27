//! The evidence-model correctness properties (Feature 1, Properties 1–4, 9):
//! construction is total over changes, deterministic, closed over evidence,
//! exhaustive over unconfirmed state, and incapable of inventing apply-side
//! evidence.

use std::collections::BTreeMap;

use proptest::prelude::*;
use tokeira_explain::{
    CommittedChange, CommittedOp, DeploymentContext, UncertaintyReason, explain_applied,
    explain_plan,
};
use tokeira_iac::{
    Change, ChangeKind, FieldDiff, PlanOutcome, RefreshCoverage, RefreshStatus, ResourceId,
};

fn context() -> DeploymentContext {
    DeploymentContext {
        deployment: "prop".to_string(),
        platform: "test".to_string(),
        operation: "infra plan".to_string(),
        current_revision: 3,
        proposed_revision: Some(4),
        definition_ref: Some("sha256:abc".to_string()),
    }
}

fn arb_kind() -> impl Strategy<Value = ChangeKind> {
    prop_oneof![
        Just(ChangeKind::Create),
        Just(ChangeKind::Update),
        Just(ChangeKind::Replace),
        Just(ChangeKind::Delete),
        Just(ChangeKind::NoChange),
    ]
}

fn arb_status() -> impl Strategy<Value = RefreshStatus> {
    prop_oneof![
        Just(RefreshStatus::DesiredLive),
        Just(RefreshStatus::DesiredMissing),
        Just(RefreshStatus::ManagedLive),
        Just(RefreshStatus::ManagedMissing),
        Just(RefreshStatus::Unknown),
    ]
}

fn arb_diffs() -> impl Strategy<Value = Vec<FieldDiff>> {
    proptest::collection::vec(
        prop_oneof![
            ("[a-z]{1,6}", "[a-z]{0,8}", "[a-z]{0,8}").prop_map(|(f, b, a)| FieldDiff {
                field: f,
                before: Some(b),
                after: Some(a),
            }),
            "[a-z ]{1,16}".prop_map(FieldDiff::observation),
        ],
        0..3,
    )
}

/// A generated plan outcome: changes with unique resource ids (an engine
/// invariant — one change per id), and coverage over a subset of them plus
/// occasionally an extra examined-but-unplanned id.
fn arb_outcome() -> impl Strategy<Value = PlanOutcome> {
    (
        proptest::collection::vec((arb_kind(), arb_diffs(), any::<bool>(), arb_status()), 0..8),
        any::<bool>(),
    )
        .prop_map(|(rows, examined)| {
            let mut changes = Vec::new();
            let mut status_by_id = BTreeMap::new();
            for (index, (kind, details, covered, status)) in rows.into_iter().enumerate() {
                let resource = format!("compose/r{index}");
                changes.push(Change {
                    kind,
                    resource_type: "compose_service".to_string(),
                    module: format!("m{}", index % 3),
                    resource: resource.clone(),
                    details,
                });
                if covered {
                    status_by_id.insert(ResourceId(resource), status);
                }
            }
            PlanOutcome {
                changes,
                refresh: RefreshCoverage {
                    status_by_id,
                    examined,
                },
            }
        })
}

proptest! {
    // Property 1 — change coverage is total: exactly one explained change per
    // engine change, preserving kind, module, resource id, and type, and
    // introducing none of its own.
    #[test]
    fn property_1_change_coverage_is_total(outcome in arb_outcome()) {
        let explanation = explain_plan(context(), &outcome);
        prop_assert_eq!(explanation.changes.len(), outcome.changes.len());
        for (explained, engine) in explanation.changes.iter().zip(&outcome.changes) {
            prop_assert_eq!(&explained.resource_id, &engine.resource);
            prop_assert_eq!(&explained.module, &engine.module);
            prop_assert_eq!(&explained.resource_type, &engine.resource_type);
            prop_assert_eq!(&explained.kind, &engine.kind);
            prop_assert_eq!(&explained.field_diffs, &engine.details);
        }
    }

    // Property 2 — construction is deterministic: two constructions from one
    // input serialize byte-identically.
    #[test]
    fn property_2_construction_is_deterministic(outcome in arb_outcome()) {
        let first = serde_json::to_string(&explain_plan(context(), &outcome)).unwrap();
        let second = serde_json::to_string(&explain_plan(context(), &outcome)).unwrap();
        prop_assert_eq!(first, second);
    }

    // Property 3 — evidence closure: every id referenced anywhere resolves in
    // the index to exactly one fact.
    #[test]
    fn property_3_evidence_closure_holds(outcome in arb_outcome()) {
        let explanation = explain_plan(context(), &outcome);
        for change in &explanation.changes {
            prop_assert!(explanation.evidence.resolve(&change.evidence_id).is_some());
        }
        for id in &explanation.destructive {
            prop_assert!(explanation.evidence.resolve(id).is_some());
        }
        for uncertainty in &explanation.uncertainties {
            prop_assert!(explanation.evidence.resolve(&uncertainty.evidence_id).is_some());
            prop_assert!(
                explanation.evidence.resolve(&uncertainty.subject).is_some(),
                "dangling uncertainty subject: {:?}",
                uncertainty.subject
            );
        }
    }

    // Property 4 — uncertainty is exhaustive over unconfirmed state: planned
    // resources with Unknown coverage each yield exactly one
    // LiveStateUnconfirmed; an unexamined verb yields exactly one
    // LiveStateNotExamined for the plan; a fully-confirmed plan yields none.
    #[test]
    fn property_4_uncertainty_is_exhaustive(outcome in arb_outcome()) {
        let explanation = explain_plan(context(), &outcome);
        if !outcome.refresh.examined {
            let not_examined: Vec<_> = explanation
                .uncertainties
                .iter()
                .filter(|u| matches!(u.reason, UncertaintyReason::LiveStateNotExamined))
                .collect();
            prop_assert_eq!(not_examined.len(), 1);
        } else {
            let expected: Vec<_> = explanation
                .changes
                .iter()
                .filter(|c| matches!(c.refresh_status, Some(RefreshStatus::Unknown)))
                .map(|c| c.evidence_id.clone())
                .collect();
            let actual: Vec<_> = explanation
                .uncertainties
                .iter()
                .filter(|u| matches!(u.reason, UncertaintyReason::LiveStateUnconfirmed))
                .map(|u| u.subject.clone())
                .collect();
            prop_assert_eq!(actual, expected);
            prop_assert!(
                explanation
                    .uncertainties
                    .iter()
                    .all(|u| !matches!(u.reason, UncertaintyReason::LiveStateNotExamined))
            );
        }
    }

    // Property 9 — apply-side explanation invents nothing: every field diff
    // in the model appears in the preceding plan for the same id; with no
    // preceding plan there are no field diffs and exactly one
    // FieldEvidenceUnavailable per committed change.
    #[test]
    fn property_9_apply_invents_nothing(
        outcome in arb_outcome(),
        with_preceding in any::<bool>(),
        ops in proptest::collection::vec(0u8..3, 0..6),
    ) {
        let committed: Vec<CommittedChange> = ops
            .iter()
            .enumerate()
            .map(|(index, op)| CommittedChange {
                id: format!("compose/r{index}"),
                op: match op {
                    0 => CommittedOp::Created,
                    1 => CommittedOp::Updated,
                    _ => CommittedOp::Deleted,
                },
            })
            .collect();
        let preceding = with_preceding.then_some(&outcome);
        let explanation = explain_applied(context(), &committed, preceding);

        prop_assert_eq!(explanation.changes.len(), committed.len());
        for change in &explanation.changes {
            match preceding {
                Some(plan) => {
                    let planned = plan.changes.iter().find(|c| c.resource == change.resource_id);
                    match planned {
                        Some(p) => prop_assert_eq!(&change.field_diffs, &p.details),
                        None => prop_assert!(change.field_diffs.is_empty()),
                    }
                }
                None => prop_assert!(change.field_diffs.is_empty()),
            }
        }
        let unavailable = explanation
            .uncertainties
            .iter()
            .filter(|u| matches!(u.reason, UncertaintyReason::FieldEvidenceUnavailable))
            .count();
        if with_preceding {
            prop_assert_eq!(unavailable, 0);
        } else {
            prop_assert_eq!(unavailable, committed.len());
        }
    }
}
