//! The evidence-model correctness properties (Feature 1, Properties 1–4, 9,
//! 10): construction is total over changes, deterministic, closed over
//! evidence, exhaustive over unconfirmed state, incapable of inventing
//! apply-side evidence — and the written artifact stands alone within the
//! field policy. Plus the change-semantics transport properties (Feature 2,
//! Properties 3–4): declarations cross verbatim, and no declaration can move
//! the destructive set.

use std::collections::BTreeMap;

use proptest::prelude::*;
use tokeira_explain::{
    CommittedChange, CommittedOp, DeploymentContext, UncertaintyReason, explain_applied,
    explain_plan,
};
use tokeira_iac::{
    Change, ChangeKind, ChangeSemantics, Citation, Confidence, DataEffect, Disruption, FieldDiff,
    LifecycleOperation, PlanOutcome, RefreshCoverage, RefreshStatus, ReplacementPolicy, ResourceId,
    Reversibility,
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

/// Declarations at every confidence tier. Citations come from a const pool —
/// content is irrelevant to the transport properties; presence is not.
fn arb_semantics() -> impl Strategy<Value = ChangeSemantics> {
    const CITE: Citation = Citation::code("test/citation");
    const DOC: Citation = Citation::doc_quoted("A page", "https://example.test/page", "A quote.");
    let operation = prop_oneof![
        Just(Confidence::Unknown),
        Just(Confidence::Inference {
            value: LifecycleOperation::Replaced,
            citation: CITE,
        }),
        Just(Confidence::EngineFact {
            value: LifecycleOperation::UpdatedInPlace,
            citation: CITE,
        }),
        Just(Confidence::ProviderGuarantee {
            value: LifecycleOperation::Deleted,
            citation: DOC,
        }),
    ];
    let replacement = prop_oneof![
        Just(Confidence::Unknown),
        Just(Confidence::Inference {
            value: ReplacementPolicy::NotRequired,
            citation: CITE,
        }),
        Just(Confidence::EngineFact {
            value: ReplacementPolicy::DestroyBeforeCreate,
            citation: CITE,
        }),
    ];
    let disruption = prop_oneof![
        Just(Confidence::Unknown),
        Just(Confidence::ProviderGuarantee {
            value: Disruption::UnavailableDuringChange,
            citation: DOC,
        }),
    ];
    let data_effect = prop_oneof![
        Just(Confidence::Unknown),
        Just(Confidence::EngineFact {
            value: DataEffect::Destroyed,
            citation: CITE,
        }),
        Just(Confidence::Inference {
            value: DataEffect::Preserved,
            citation: DOC,
        }),
        Just(Confidence::ProviderGuarantee {
            value: DataEffect::Policy,
            citation: DOC,
        }),
    ];
    let reversibility = prop_oneof![
        Just(Confidence::Unknown),
        Just(Confidence::Inference {
            value: Reversibility::Irreversible,
            citation: CITE,
        }),
    ];
    (
        operation,
        replacement,
        disruption,
        data_effect,
        reversibility,
    )
        .prop_map(
            |(operation, replacement, disruption, data_effect, reversibility)| ChangeSemantics {
                operation,
                replacement,
                disruption,
                data_effect,
                reversibility,
                statement: None,
            },
        )
}

/// A generated plan outcome: changes with unique resource ids (an engine
/// invariant — one change per id), coverage over a subset of them, and
/// declarations honouring the engine's own invariants — `NoChange` never
/// declares, and a declaration-less deletion models an unclaimed recoverer.
fn arb_outcome() -> impl Strategy<Value = PlanOutcome> {
    (
        proptest::collection::vec(
            (
                arb_kind(),
                arb_diffs(),
                any::<bool>(),
                arb_status(),
                proptest::option::of(arb_semantics()),
            ),
            0..8,
        ),
        any::<bool>(),
    )
        .prop_map(|(rows, examined)| {
            let mut changes = Vec::new();
            let mut status_by_id = BTreeMap::new();
            let mut semantics_by_id = BTreeMap::new();
            for (index, (kind, details, covered, status, declared)) in rows.into_iter().enumerate()
            {
                let resource = format!("compose/r{index}");
                changes.push(Change {
                    kind,
                    resource_type: "compose_service".to_string(),
                    module: format!("m{}", index % 3),
                    resource: resource.clone(),
                    details,
                });
                if covered {
                    status_by_id.insert(ResourceId(resource.clone()), status);
                }
                if kind != ChangeKind::NoChange
                    && let Some(semantics) = declared
                {
                    semantics_by_id.insert(ResourceId(resource), semantics);
                }
            }
            PlanOutcome {
                changes,
                refresh: RefreshCoverage {
                    status_by_id,
                    examined,
                    ..Default::default()
                },
                semantics_by_id,
                ..Default::default()
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
        for impact in &explanation.impacts {
            prop_assert!(explanation.evidence.resolve(&impact.evidence_id).is_some());
            for subject in &impact.subjects {
                prop_assert!(
                    explanation.evidence.resolve(subject).is_some(),
                    "dangling impact subject: {:?}",
                    subject
                );
            }
        }
        // Causal groups (Feature 3) are closure-bearing: the group id, every
        // member, and a resource root must all resolve.
        for group in &explanation.causal_groups {
            prop_assert!(explanation.evidence.resolve(&group.evidence_id).is_some());
            for member in &group.members {
                prop_assert!(
                    explanation.evidence.resolve(member).is_some(),
                    "dangling group member: {:?}",
                    member
                );
            }
            if let tokeira_explain::CausalRoot::Resource(root) = &group.root {
                prop_assert!(
                    explanation.evidence.resolve(root).is_some(),
                    "dangling group root: {:?}",
                    root
                );
            }
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
    // FieldEvidenceUnavailable per committed *acting* change — the unchanged
    // census makes no claims, so it opens no gaps.
    #[test]
    fn property_9_apply_invents_nothing(
        outcome in arb_outcome(),
        with_preceding in any::<bool>(),
        ops in proptest::collection::vec(0u8..5, 0..6),
    ) {
        let committed: Vec<CommittedChange> = ops
            .iter()
            .enumerate()
            .map(|(index, op)| CommittedChange {
                id: format!("compose/r{index}"),
                op: match op {
                    0 => CommittedOp::Created,
                    1 => CommittedOp::Updated,
                    2 => CommittedOp::Replaced,
                    3 => CommittedOp::Deleted,
                    _ => CommittedOp::Unchanged,
                },
                module: format!("m{index}"),
                resource_type: "compose_service".to_string(),
                display: None,
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
        let acting = committed
            .iter()
            .filter(|c| c.op != CommittedOp::Unchanged)
            .count();
        if with_preceding {
            prop_assert_eq!(unavailable, 0);
        } else {
            prop_assert_eq!(unavailable, acting);
        }
    }

    // Change-semantics Property 3 — transport is verbatim: every explained
    // change carries exactly the declaration the map holds for its id (or
    // declares nothing when absent), and a declaration-less deletion is
    // stated as uncertainty, never silence (Req 3.4/3.5/3.6).
    #[test]
    fn semantics_property_3_transport_is_verbatim(outcome in arb_outcome()) {
        let explanation = explain_plan(context(), &outcome);
        for change in &explanation.changes {
            let declared = outcome
                .semantics_by_id
                .get(&ResourceId(change.resource_id.clone()));
            match declared {
                Some(semantics) => prop_assert_eq!(&change.semantics, semantics),
                None => prop_assert_eq!(&change.semantics, &ChangeSemantics::default()),
            }
        }
        let expected_undeclared: Vec<_> = explanation
            .changes
            .iter()
            .filter(|c| {
                c.kind == ChangeKind::Delete
                    && !outcome
                        .semantics_by_id
                        .contains_key(&ResourceId(c.resource_id.clone()))
            })
            .map(|c| c.evidence_id.clone())
            .collect();
        let actual_undeclared: Vec<_> = explanation
            .uncertainties
            .iter()
            .filter(|u| matches!(u.reason, UncertaintyReason::KindUnavailableForRemovedResource))
            .map(|u| u.subject.clone())
            .collect();
        prop_assert_eq!(actual_undeclared, expected_undeclared);
    }

    // Change-semantics Property 4 — declarations cannot move the destructive
    // set: whatever any kind declares (fuzzed across every confidence tier),
    // the destructive set equals the engine's own classification. The safety
    // property of the feature: a wrong declaration can mislead a reader, but
    // it can never re-gate `apply --yes`.
    #[test]
    fn semantics_property_4_declarations_cannot_move_the_destructive_set(
        outcome in arb_outcome(),
    ) {
        let explanation = explain_plan(context(), &outcome);
        let engine_classification: Vec<_> = explanation
            .changes
            .iter()
            .filter(|c| matches!(c.kind, ChangeKind::Replace | ChangeKind::Delete))
            .map(|c| c.evidence_id.clone())
            .collect();
        prop_assert_eq!(&explanation.destructive, &engine_classification);
    }

    // Change-semantics Property 5 — impact derivation is a pure function of
    // the declarations: identical declarations yield byte-identical impacts,
    // whatever the refresh coverage said.
    #[test]
    fn semantics_property_5_impacts_are_pure(outcome in arb_outcome()) {
        let first = explain_plan(context(), &outcome);
        let second = explain_plan(context(), &outcome);
        prop_assert_eq!(&first.impacts, &second.impacts);

        let mut refreshless = outcome.clone();
        refreshless.refresh = RefreshCoverage::default();
        let third = explain_plan(context(), &refreshless);
        prop_assert_eq!(&first.impacts, &third.impacts,
            "impacts are a function of declarations, never of coverage");
    }

    // Change-semantics Property 6 — every impact is grounded, both ways:
    // each subject's declaration justifies the class, and every qualifying
    // change appears in its class's impact.
    #[test]
    fn semantics_property_6_every_impact_is_grounded(outcome in arb_outcome()) {
        use tokeira_explain::ImpactClass;
        use tokeira_iac::{ChangeKind, DataEffect, Disruption, ReplacementPolicy};

        let explanation = explain_plan(context(), &outcome);
        // Mirrors the two-source contract (Requirements 5.4/5.5): a class is
        // justified by the declared semantics or by the engine-fact floor.
        let qualifies = |change: &tokeira_explain::ExplainedChange, class: ImpactClass| {
            let s = &change.semantics;
            match class {
                ImpactClass::DataDestroyed =>
                    s.data_effect.value() == Some(&DataEffect::Destroyed),
                ImpactClass::Unavailability => {
                    let declared =
                        s.disruption.value() == Some(&Disruption::UnavailableDuringChange);
                    let floor = match change.kind {
                        ChangeKind::Delete => true,
                        ChangeKind::Replace =>
                            s.replacement.value() != Some(&ReplacementPolicy::CreateBeforeDestroy),
                        _ => false,
                    };
                    declared || floor
                }
                ImpactClass::Replacement => matches!(
                    s.replacement.value(),
                    Some(policy) if *policy != ReplacementPolicy::NotRequired
                ) || change.kind == ChangeKind::Replace,
                // Derived from the graph by causality, never by explain_plan.
                ImpactClass::DependencyLoss => false,
                ImpactClass::BriefInterruption =>
                    s.disruption.value() == Some(&Disruption::BriefInterruption),
                ImpactClass::RollingReplacement =>
                    s.disruption.value() == Some(&Disruption::Rolling),
            }
        };

        for impact in &explanation.impacts {
            prop_assert!(!impact.subjects.is_empty());
            for subject in &impact.subjects {
                let change = explanation
                    .changes
                    .iter()
                    .find(|c| &c.evidence_id == subject)
                    .expect("impact subject is a change");
                prop_assert!(
                    qualifies(change, impact.class),
                    "unjustified subject {:?} in {:?}",
                    subject,
                    impact.class
                );
            }
        }
        for change in &explanation.changes {
            for class in [
                ImpactClass::DataDestroyed,
                ImpactClass::Unavailability,
                ImpactClass::Replacement,
                ImpactClass::DependencyLoss,
                ImpactClass::BriefInterruption,
                ImpactClass::RollingReplacement,
            ] {
                if qualifies(change, class) {
                    let listed = explanation
                        .impacts
                        .iter()
                        .find(|i| i.class == class)
                        .is_some_and(|i| i.subjects.contains(&change.evidence_id));
                    prop_assert!(
                        listed,
                        "qualifying change {:?} missing from {:?}",
                        change.resource_id,
                        class
                    );
                }
            }
        }
    }

    // Change-semantics Property 7 — Unknown never becomes a claim
    // (Requirement 5.7): a change declaring nothing joins no impact except
    // one the engine floor grounds in its kind — a Replace floors
    // replacement and unavailability (the all-Unknown policy cannot lift
    // it), a Delete floors unavailability. Declarations are the only other
    // path into an impact.
    #[test]
    fn semantics_property_7_unknown_never_becomes_a_claim(outcome in arb_outcome()) {
        use tokeira_explain::ImpactClass;

        let explanation = explain_plan(context(), &outcome);
        let floor_grounds = |kind: ChangeKind, class: ImpactClass| match class {
            ImpactClass::Unavailability => {
                matches!(kind, ChangeKind::Delete | ChangeKind::Replace)
            }
            ImpactClass::Replacement => kind == ChangeKind::Replace,
            _ => false,
        };
        for change in &explanation.changes {
            if change.semantics == ChangeSemantics::default() {
                for impact in &explanation.impacts {
                    prop_assert!(
                        !impact.subjects.contains(&change.evidence_id)
                            || floor_grounds(change.kind, impact.class),
                        "an all-Unknown change joined {:?} without an engine fact",
                        impact.class
                    );
                }
            }
        }
        // With NO declarations anywhere, only the engine floor may speak —
        // and where even it is silent, the absence is recorded as
        // uncertainty, never as consequence-free.
        if outcome.semantics_by_id.is_empty() {
            for impact in &explanation.impacts {
                for subject in &impact.subjects {
                    let change = explanation
                        .changes
                        .iter()
                        .find(|c| &c.evidence_id == subject)
                        .expect("impact subject is a change");
                    prop_assert!(
                        floor_grounds(change.kind, impact.class),
                        "declaration-free impact {:?} lacks an engine fact",
                        impact.class
                    );
                }
            }
            let acting = explanation
                .changes
                .iter()
                .any(|c| c.kind != ChangeKind::NoChange);
            if acting {
                prop_assert!(
                    explanation.uncertainties.iter().any(|u| matches!(
                        u.reason,
                        UncertaintyReason::SemanticsUndeclared { .. }
                    )),
                    "undeclared world must state the gap"
                );
            }
        }
    }

    // Change-semantics Property 8 — uncertainty activation is exact, both
    // directions: per-change iff Unknown-while-stated-elsewhere, one
    // plan-level entry iff Unknown everywhere applicable, and never for a
    // kind the field does not apply to.
    #[test]
    fn semantics_property_8_uncertainty_activation_is_exact(outcome in arb_outcome()) {
        let explanation = explain_plan(context(), &outcome);
        // (wire name, field-is-declared, field-applies-to-change-kind).
        type FieldProbe = (&'static str, fn(&ChangeSemantics) -> bool, fn(ChangeKind) -> bool);
        let fields: [FieldProbe; 5] = [
            ("operation", |s| s.operation.is_known(), |k| k != ChangeKind::NoChange),
            ("replacement", |s| s.replacement.is_known(),
             |k| matches!(k, ChangeKind::Update | ChangeKind::Replace)),
            ("disruption", |s| s.disruption.is_known(), |k| k != ChangeKind::NoChange),
            ("data_effect", |s| s.data_effect.is_known(),
             |k| matches!(k, ChangeKind::Update | ChangeKind::Replace | ChangeKind::Delete)),
            ("reversibility", |s| s.reversibility.is_known(), |k| k != ChangeKind::NoChange),
        ];
        for (wire, known, applies) in fields {
            let applicable: Vec<_> = explanation
                .changes
                .iter()
                .filter(|c| c.kind != ChangeKind::NoChange && applies(c.kind))
                .collect();
            let unknowns: Vec<_> = applicable
                .iter()
                .filter(|c| !known(&c.semantics))
                .collect();
            let expected_subjects: Vec<_> = if applicable.is_empty() || unknowns.is_empty() {
                Vec::new()
            } else if unknowns.len() == applicable.len() {
                vec![tokeira_explain::EvidenceId::deployment("prop")]
            } else {
                unknowns.iter().map(|c| c.evidence_id.clone()).collect()
            };
            let actual_subjects: Vec<_> = explanation
                .uncertainties
                .iter()
                .filter(|u| matches!(
                    &u.reason,
                    UncertaintyReason::SemanticsUndeclared { field } if field == wire
                ))
                .map(|u| u.subject.clone())
                .collect();
            prop_assert_eq!(actual_subjects, expected_subjects, "field {}", wire);
        }
    }

    // Property 10 — the artifact is self-contained and bounded: a written
    // artifact parses back — from the file alone, no deployment directory —
    // to an equal, evidence-closed model, and serializes no key outside the
    // field policy (design §Data models). A field reaching the artifact
    // unreviewed fails the subset check.
    #[test]
    fn property_10_artifact_is_self_contained_and_bounded(outcome in arb_outcome()) {
        let explanation = explain_plan(context(), &outcome);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("explanation.json");
        tokeira_explain::artifact::write(&path, &explanation).unwrap();

        let back = tokeira_explain::artifact::read(&path).unwrap();
        prop_assert_eq!(&back, &explanation);
        for change in &back.changes {
            prop_assert!(back.evidence.resolve(&change.evidence_id).is_some());
        }
        for uncertainty in &back.uncertainties {
            prop_assert!(back.evidence.resolve(&uncertainty.evidence_id).is_some());
            prop_assert!(back.evidence.resolve(&uncertainty.subject).is_some());
        }
        for id in &back.destructive {
            prop_assert!(back.evidence.resolve(id).is_some());
        }

        let value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        keys_within(&value, &[
            "schema_version", "deployment", "platform", "operation",
            "current_revision", "proposed_revision", "definition_ref",
            "changes", "impacts", "destructive", "uncertainties",
            "causal_groups", "platform_issues", "evidence",
        ])?;
        for change in value["changes"].as_array().into_iter().flatten() {
            keys_within(change, &[
                "evidence_id", "resource_id", "display", "module", "resource_type",
                "kind", "field_diffs", "refresh_status", "semantics", "cause",
                "dependants", "departed_fields", "source",
            ])?;
            for diff in change["field_diffs"].as_array().into_iter().flatten() {
                keys_within(diff, &["field", "before", "after"])?;
            }
        }
        for uncertainty in value["uncertainties"].as_array().into_iter().flatten() {
            keys_within(uncertainty, &[
                "evidence_id", "subject", "reason", "consequence", "resolvable_by",
            ])?;
        }
        for impact in value["impacts"].as_array().into_iter().flatten() {
            keys_within(impact, &["evidence_id", "class", "subjects", "statement", "lost"])?;
        }
    }
}

/// Assert every key of a JSON object is in the field policy's allowed set.
/// Slot interiors (`semantics`, `cause`) are deliberately unbounded here —
/// their shape belongs to the change-semantics and causality specs.
fn keys_within(
    value: &serde_json::Value,
    allowed: &[&str],
) -> Result<(), proptest::test_runner::TestCaseError> {
    let object = value.as_object().expect("policy level is a JSON object");
    for key in object.keys() {
        prop_assert!(
            allowed.contains(&key.as_str()),
            "key {key:?} is outside the artifact field policy"
        );
    }
    Ok(())
}
