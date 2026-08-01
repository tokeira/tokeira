//! Causality properties (explanation-causality Phases 4–5) plus the named
//! example scenarios. The load-bearing test is Property 2: the shipping
//! classifier is checked against an independent, table-literal oracle of the
//! requirements' algebra (A1–A10 with A3b), so the implementation is verified
//! against the specification's own shape, not against itself.

use std::collections::{BTreeMap, BTreeSet};

use proptest::prelude::*;
use serde_json::json;
use tokeira_explain::{
    BaselineView, CausalRoot, CausalityView, Cause, Confidence, DeploymentContext,
    DeploymentExplanation, UncertaintyReason, apply_causality, explain_plan,
};
use tokeira_iac::{
    Change, ChangeKind, PlanOutcome, RefreshCoverage, RefreshStatus, ResourceId, ResourceState,
    ResourceType,
};

// ── World generation ─────────────────────────────────────────────────────

const IDS: [&str; 5] = ["m/alpha", "m/beta", "m/gamma", "m/delta", "m/epsilon"];

/// One resource's membership and observation flags — the generator's row of
/// the (D, P, S, L) tuple.
#[derive(Debug, Clone)]
struct Row {
    in_d: bool,
    in_p: bool,
    /// When present on both sides: whether D(R) == P(R).
    same_manifest: bool,
    in_s: bool,
    status: Option<RefreshStatus>,
    departed: bool,
    kind: ChangeKind,
}

#[derive(Debug, Clone)]
enum BaselineMode {
    Realized,
    NeverApplied,
    Missing,
    DoesNotInterpret,
    NotInterpreted,
}

#[derive(Debug, Clone)]
struct World {
    rows: Vec<Row>,
    /// Edges from a resource to strictly earlier indices — a DAG by
    /// construction (the standing workspace property).
    deps: Vec<Vec<usize>>,
    baseline: BaselineMode,
    examined: bool,
    desired_available: bool,
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

fn arb_status() -> impl Strategy<Value = Option<RefreshStatus>> {
    prop_oneof![
        Just(None),
        Just(Some(RefreshStatus::DesiredLive)),
        Just(Some(RefreshStatus::DesiredMissing)),
        Just(Some(RefreshStatus::Unknown)),
    ]
}

fn arb_row() -> impl Strategy<Value = Row> {
    (
        any::<bool>(),
        any::<bool>(),
        any::<bool>(),
        any::<bool>(),
        arb_status(),
        any::<bool>(),
        arb_kind(),
    )
        .prop_map(
            |(in_d, in_p, same_manifest, in_s, status, departed, kind)| Row {
                in_d,
                in_p,
                same_manifest,
                in_s,
                status,
                departed,
                kind,
            },
        )
}

fn arb_world() -> impl Strategy<Value = World> {
    let rows = proptest::collection::vec(arb_row(), IDS.len());
    let deps = (0..IDS.len())
        .map(|i| {
            proptest::collection::vec(0..IDS.len().max(1), 0..3)
                .prop_map(move |raw| raw.into_iter().filter(|d| *d < i).collect::<Vec<_>>())
        })
        .collect::<Vec<_>>();
    let baseline = prop_oneof![
        4 => Just(BaselineMode::Realized),
        1 => Just(BaselineMode::NeverApplied),
        1 => Just(BaselineMode::Missing),
        1 => Just(BaselineMode::DoesNotInterpret),
        1 => Just(BaselineMode::NotInterpreted),
    ];
    (
        rows,
        deps,
        baseline,
        any::<bool>(),
        prop_oneof![9 => Just(true), 1 => Just(false)],
    )
        .prop_map(
            |(rows, deps, baseline, examined, desired_available)| World {
                rows,
                deps,
                baseline,
                examined,
                desired_available,
            },
        )
}

fn rid(i: usize) -> ResourceId {
    ResourceId(IDS[i].to_string())
}

fn state_entry(i: usize) -> ResourceState {
    ResourceState {
        resource_type: ResourceType("test_kind".to_string()),
        physical_id: format!("phys-{i}"),
        // Distinct per resource so property values never collide with the
        // generated manifests by accident (manifest values are objects).
        properties: json!({ "endpoint": format!("recorded-{i}") }),
        dependencies: Vec::new(),
        created_at: "t0".to_string(),
        updated_at: "t0".to_string(),
        module: "m".to_string(),
    }
}

/// Build the (view, explanation) pair a plan verb would hand to
/// `apply_causality`, from a generated world.
fn build(world: &World) -> (CausalityView, DeploymentExplanation) {
    let mut desired = BTreeMap::new();
    let mut baseline_snapshot = BTreeMap::new();
    let mut recorded = BTreeMap::new();
    let mut status_by_id = BTreeMap::new();
    let mut live_departed = BTreeSet::new();
    let mut changes = Vec::new();
    let mut edges: BTreeMap<ResourceId, Vec<ResourceId>> = BTreeMap::new();

    for (i, row) in world.rows.iter().enumerate() {
        let id = rid(i);
        // Manifests: same or differing under one key, canonical either way.
        if row.in_d {
            desired.insert(id.clone(), json!({ "field": format!("d-{i}") }));
        }
        if row.in_p {
            let value = if row.same_manifest && row.in_d {
                format!("d-{i}")
            } else {
                format!("p-{i}")
            };
            baseline_snapshot.insert(id.clone(), json!({ "field": value }));
        }
        if row.in_s {
            recorded.insert(id.clone(), state_entry(i));
        }
        if let Some(status) = row.status {
            status_by_id.insert(id.clone(), status);
        }
        if row.departed {
            live_departed.insert(id.clone());
        }
        edges.insert(id.clone(), world.deps[i].iter().map(|d| rid(*d)).collect());

        changes.push(Change {
            module: "m".to_string(),
            resource: id.0.clone(),
            resource_type: "test_kind".to_string(),
            kind: row.kind,
            details: Vec::new(),
        });
    }

    let outcome = PlanOutcome {
        changes,
        refresh: RefreshCoverage {
            status_by_id,
            examined: world.examined,
            live_departed,
        },
        ..Default::default()
    };
    let explanation = explain_plan(context(), &outcome);

    let view = CausalityView {
        desired: world.desired_available.then_some(desired),
        baseline: match world.baseline {
            BaselineMode::Realized => BaselineView::Realized(baseline_snapshot),
            BaselineMode::NeverApplied => BaselineView::NeverApplied,
            BaselineMode::Missing => BaselineView::Missing { revision: 4 },
            BaselineMode::DoesNotInterpret => BaselineView::DoesNotInterpret {
                revision: 4,
                verdict: "the definition does not verify: test".to_string(),
            },
            BaselineMode::NotInterpreted => BaselineView::NotInterpreted {
                reason: "this platform has no interpreted definition".to_string(),
            },
        },
        baseline_revision: match world.baseline {
            BaselineMode::NeverApplied => 0,
            _ => 4,
        },
        recorded,
        edges: edges.clone(),
        // The union graph doubles as the desired graph here: these fixtures
        // exercise classification, not dependency loss, and a desired graph
        // covering every recorded edge derives none.
        desired_edges: edges,
        refresh: outcome.refresh.clone(),
    };
    (view, explanation)
}

fn context() -> DeploymentContext {
    DeploymentContext {
        deployment: "causality-test".to_string(),
        platform: "test".to_string(),
        operation: "infra plan".to_string(),
        current_revision: 4,
        proposed_revision: None,
        definition_ref: None,
    }
}

// ── The oracle: the requirements table, row by row ───────────────────────

/// What a row of the algebra concludes, shape-compared against the
/// classifier's `Confidence<Cause>`.
#[derive(Debug, Clone, PartialEq)]
enum Verdict {
    EditEngineFact,
    OutputTracedInference(String),
    CascadeInference(String),
    DriftEngineFact,
    AdvanceEngineFact,
    Unknown,
}

/// A literal walk of the table: A10's baseline branch, then A1, A2, A3, A3b,
/// A4, A5, A6, A9, A7, A8, in the requirements' own precedence (A9 is
/// written before A7/A8 because it guards them). Deliberately structured as
/// sequential row checks — the *shape of the specification*, not the
/// classifier's nested flow.
fn oracle(world: &World, view: &CausalityView, i: usize) -> Verdict {
    let row = &world.rows[i];

    if !world.desired_available {
        return Verdict::Unknown;
    }
    match world.baseline {
        BaselineMode::NeverApplied => {
            return if row.kind == ChangeKind::Create {
                Verdict::EditEngineFact // A10, creates rule
            } else {
                Verdict::Unknown // A10
            };
        }
        BaselineMode::Missing | BaselineMode::DoesNotInterpret | BaselineMode::NotInterpreted => {
            return Verdict::Unknown;
        } // A10
        BaselineMode::Realized => {}
    }

    // A1: R ∈ D and R ∉ P.
    if row.in_d && !row.in_p {
        return Verdict::EditEngineFact;
    }
    // A2: R ∉ D and R ∈ P.
    if !row.in_d && row.in_p {
        return Verdict::EditEngineFact;
    }
    // A3: R ∉ D and R ∉ P and R ∈ S.
    if !row.in_d && !row.in_p && row.in_s {
        return Verdict::EditEngineFact;
    }
    // Off-table: in no source at all.
    if !row.in_d && !row.in_p {
        return Verdict::Unknown;
    }
    // A3b: R ∈ D and R ∈ P and D = P and R ∉ S.
    if row.same_manifest && !row.in_s {
        return Verdict::EditEngineFact;
    }
    // A4: D ≠ P, traced unambiguously to a dependency's changed output.
    if !row.same_manifest {
        if let Some(dep) = oracle_trace(world, view, i) {
            return Verdict::OutputTracedInference(dep);
        }
        // A5: D ≠ P otherwise.
        return Verdict::EditEngineFact;
    }
    // A6: D = P and a direct dependency is planned as Replace.
    let mut replaced_deps: Vec<usize> = world.deps[i]
        .iter()
        .copied()
        .filter(|d| world.rows[*d].kind == ChangeKind::Replace)
        .collect();
    replaced_deps.sort_by_key(|d| rid(*d));
    if let Some(root) = replaced_deps.first() {
        return Verdict::CascadeInference(IDS[*root].to_string());
    }
    // A9: L unconfirmed or unexamined (guards A7/A8).
    let confirmed = world.examined
        && matches!(
            row.status,
            Some(RefreshStatus::DesiredLive) | Some(RefreshStatus::DesiredMissing)
        );
    if !confirmed {
        return Verdict::Unknown;
    }
    // A7: L confirmed and L ≠ S.
    if row.departed || row.status == Some(RefreshStatus::DesiredMissing) {
        return Verdict::DriftEngineFact;
    }
    // A8: L confirmed, L = S, and the engine still computed a change.
    Verdict::AdvanceEngineFact
}

/// The oracle's A4 predicate, evaluated independently: every differing leaf
/// (here: the single generated `field`) matches exactly one recorded scalar
/// of exactly one dependency, with the recorded value departing from the
/// baseline side.
fn oracle_trace(world: &World, view: &CausalityView, i: usize) -> Option<String> {
    let id = rid(i);
    let (Some(desired), BaselineView::Realized(baseline)) = (view.desired.as_ref(), &view.baseline)
    else {
        return None;
    };
    let after = desired.get(&id)?.get("field")?.clone();
    let before = baseline.get(&id)?.get("field")?.clone();
    let mut candidates = Vec::new();
    for dep_index in &world.deps[i] {
        let dep_id = rid(*dep_index);
        let Some(state) = view.recorded.get(&dep_id) else {
            continue;
        };
        if let Some(value) = state.properties.get("endpoint")
            && *value == after
            && *value != before
        {
            candidates.push(dep_id.0.clone());
        }
    }
    if candidates.len() == 1 {
        Some(candidates.remove(0))
    } else {
        None
    }
}

fn verdict_of(cause: &Confidence<Cause>) -> Verdict {
    match cause {
        Confidence::Unknown => Verdict::Unknown,
        Confidence::EngineFact { value, .. } => match value {
            Cause::DefinitionEdit { .. } => Verdict::EditEngineFact,
            Cause::ProviderDrift => Verdict::DriftEngineFact,
            Cause::EngineAdvance => Verdict::AdvanceEngineFact,
            other => panic!("engine-fact confidence on {other:?} is not in the algebra"),
        },
        Confidence::Inference { value, .. } => match value {
            Cause::DependencyOutputChanged { dependency } => {
                Verdict::OutputTracedInference(dependency.clone())
            }
            Cause::ReplacementCascade { root } => Verdict::CascadeInference(root.clone()),
            other => panic!("inference confidence on {other:?} is not in the algebra"),
        },
        Confidence::ProviderGuarantee { .. } => {
            panic!("no algebra row yields a provider guarantee")
        }
    }
}

// ── Properties ───────────────────────────────────────────────────────────

proptest! {
    // Feature: explanation-causality, Property 2 — the algebra is followed
    // exactly: the classifier equals the table-literal oracle on every
    // generated tuple, including precedence collisions.
    #[test]
    fn property_2_the_algebra_is_followed_exactly(world in arb_world()) {
        let (view, mut explanation) = build(&world);
        apply_causality(&mut explanation, &view);
        for (i, change) in explanation.changes.iter().enumerate() {
            if change.kind == ChangeKind::NoChange {
                continue;
            }
            prop_assert_eq!(
                verdict_of(&change.cause),
                oracle(&world, &view, i),
                "row {} ({:?})", i, world.rows[i]
            );
        }
    }

    // Feature: explanation-causality, Property 1 — assessment is total and
    // unique: every non-NoChange change is assessed (a known cause, or an
    // uncertainty naming it), and no NoChange change carries either.
    #[test]
    fn property_1_assessment_is_total_and_unique(world in arb_world()) {
        let (view, mut explanation) = build(&world);
        apply_causality(&mut explanation, &view);
        for change in &explanation.changes {
            let undecided_for_it = explanation.uncertainties.iter().filter(|u| {
                matches!(&u.reason, UncertaintyReason::CauseUndecidable { resource }
                    if *resource == change.resource_id)
            }).count();
            if change.kind == ChangeKind::NoChange {
                prop_assert!(matches!(change.cause, Confidence::Unknown));
                prop_assert_eq!(undecided_for_it, 0);
            } else if change.cause.is_known() {
                prop_assert_eq!(undecided_for_it, 0);
            } else {
                prop_assert_eq!(undecided_for_it, 1);
            }
        }
    }

    // Feature: explanation-causality, Property 3 — classification is
    // deterministic and pure: two applications from one input serialize
    // byte-identically.
    #[test]
    fn property_3_classification_is_deterministic(world in arb_world()) {
        let (view, base) = build(&world);
        let mut first = base.clone();
        let mut second = base;
        apply_causality(&mut first, &view);
        apply_causality(&mut second, &view);
        prop_assert_eq!(
            serde_json::to_string(&first).unwrap(),
            serde_json::to_string(&second).unwrap()
        );
    }

    // Feature: explanation-causality, Property 4 — no drift claim without a
    // confirmed live read: unconfirmed or unexamined L never classifies
    // ProviderDrift or EngineAdvance, and an uncertainty exists for it.
    #[test]
    fn property_4_no_drift_claim_without_confirmed_live(world in arb_world()) {
        let (view, mut explanation) = build(&world);
        apply_causality(&mut explanation, &view);
        for (i, change) in explanation.changes.iter().enumerate() {
            if change.kind == ChangeKind::NoChange {
                continue;
            }
            let confirmed = world.examined && matches!(
                world.rows[i].status,
                Some(RefreshStatus::DesiredLive) | Some(RefreshStatus::DesiredMissing)
            );
            if !confirmed {
                let claims_live = matches!(
                    change.cause.value(),
                    Some(Cause::ProviderDrift) | Some(Cause::EngineAdvance)
                );
                prop_assert!(!claims_live, "unconfirmed L classified {:?}", change.cause);
            }
        }
    }

    // Feature: explanation-causality, Property 10 — unknown causes and
    // cause-undecidable uncertainties correspond one-to-one; the plan-level
    // BaselineUnavailable exists exactly when a retained baseline is missing
    // or broken.
    #[test]
    fn property_10_unknown_causes_surface_one_to_one(world in arb_world()) {
        let (view, mut explanation) = build(&world);
        apply_causality(&mut explanation, &view);
        let unknown = explanation.changes.iter()
            .filter(|c| c.kind != ChangeKind::NoChange && !c.cause.is_known())
            .count();
        let undecidable = explanation.uncertainties.iter()
            .filter(|u| matches!(u.reason, UncertaintyReason::CauseUndecidable { .. }))
            .count();
        prop_assert_eq!(unknown, undecidable);
        let baseline_unavailable = explanation.uncertainties.iter()
            .filter(|u| matches!(u.reason, UncertaintyReason::BaselineUnavailable { .. }))
            .count();
        let expected = matches!(
            world.baseline,
            BaselineMode::Missing | BaselineMode::DoesNotInterpret
        ) as usize;
        prop_assert_eq!(baseline_unavailable, expected);
    }

    // Feature: explanation-causality, Property 8 — groups partition the
    // non-NoChange changes under bounded ultimate roots: every such change
    // in exactly one group, no group empty, and no group rooted at a cascade
    // (the walk always terminates at a non-cascade cause's root).
    #[test]
    fn property_8_groups_partition_under_bounded_roots(world in arb_world()) {
        let (view, mut explanation) = build(&world);
        apply_causality(&mut explanation, &view);
        let mut seen = BTreeSet::new();
        for group in &explanation.causal_groups {
            prop_assert!(!group.members.is_empty(), "empty group");
            for member in &group.members {
                prop_assert!(seen.insert(member.clone()), "member in two groups");
            }
            // The bounded walk: a group rooted at a resource whose own cause
            // is a cascade would mean the walk stopped early.
            if let CausalRoot::Resource(root_id) = &group.root {
                let root_change = explanation.changes.iter()
                    .find(|c| &c.evidence_id == root_id);
                if let Some(root_change) = root_change {
                    prop_assert!(
                        !matches!(root_change.cause.value(), Some(Cause::ReplacementCascade { .. })),
                        "group rooted at a cascade member"
                    );
                }
            }
            // Every group id resolves in the evidence index.
            prop_assert!(explanation.evidence.resolve(&group.evidence_id).is_some());
        }
        let expected: BTreeSet<_> = explanation.changes.iter()
            .filter(|c| c.kind != ChangeKind::NoChange)
            .map(|c| c.evidence_id.clone())
            .collect();
        prop_assert_eq!(seen, expected, "groups do not partition the changes");
    }

    // Feature: explanation-causality, Property 9 — dependants are the
    // reverse edges of the union graph, exactly: no heuristics, no misses.
    #[test]
    fn property_9_dependants_are_the_reverse_graph(world in arb_world()) {
        let (view, mut explanation) = build(&world);
        apply_causality(&mut explanation, &view);
        for change in &explanation.changes {
            if change.kind == ChangeKind::NoChange {
                continue;
            }
            let expected: BTreeSet<String> = view.edges.iter()
                .filter(|(_, deps)| deps.iter().any(|d| d.0 == change.resource_id))
                .map(|(from, _)| from.0.clone())
                .collect();
            let actual: BTreeSet<String> = change.dependants.iter().cloned().collect();
            prop_assert_eq!(actual, expected);
        }
    }
}

// ── Property 6: the A4 gates, constructed adversarially ──────────────────

/// A hand-built world: `beta` depends on `alpha`; `beta`'s field changed
/// between P and D; whether A4 fires depends on `alpha`'s recorded output.
fn traced_world(
    endpoint: &str,
    with_edge: bool,
    p_field: &str,
    d_field: &str,
) -> (CausalityView, DeploymentExplanation) {
    let alpha = ResourceId("m/alpha".to_string());
    let beta = ResourceId("m/beta".to_string());
    let mut desired = BTreeMap::new();
    desired.insert(alpha.clone(), json!({ "field": "same" }));
    desired.insert(beta.clone(), json!({ "field": d_field }));
    let mut baseline = BTreeMap::new();
    baseline.insert(alpha.clone(), json!({ "field": "same" }));
    baseline.insert(beta.clone(), json!({ "field": p_field }));
    let mut recorded = BTreeMap::new();
    let mut alpha_state = state_entry(0);
    alpha_state.properties = json!({ "endpoint": endpoint });
    recorded.insert(alpha.clone(), alpha_state);
    recorded.insert(beta.clone(), state_entry(1));
    let mut edges = BTreeMap::new();
    edges.insert(
        beta.clone(),
        if with_edge {
            vec![alpha.clone()]
        } else {
            Vec::new()
        },
    );
    edges.insert(alpha.clone(), Vec::new());

    let outcome = PlanOutcome {
        changes: vec![
            Change {
                module: "m".into(),
                resource: alpha.0.clone(),
                resource_type: "test_kind".into(),
                kind: ChangeKind::NoChange,
                details: Vec::new(),
            },
            Change {
                module: "m".into(),
                resource: beta.0.clone(),
                resource_type: "test_kind".into(),
                kind: ChangeKind::Update,
                details: Vec::new(),
            },
        ],
        refresh: RefreshCoverage {
            examined: true,
            ..Default::default()
        },
        ..Default::default()
    };
    let explanation = explain_plan(context(), &outcome);
    let view = CausalityView {
        desired: Some(desired),
        baseline: BaselineView::Realized(baseline),
        baseline_revision: 4,
        recorded,
        edges: edges.clone(),
        // The union graph doubles as the desired graph here: these fixtures
        // exercise classification, not dependency loss, and a desired graph
        // covering every recorded edge derives none.
        desired_edges: edges,
        refresh: outcome.refresh.clone(),
    };
    (view, explanation)
}

fn beta_cause(explanation: &DeploymentExplanation) -> Confidence<Cause> {
    explanation
        .changes
        .iter()
        .find(|c| c.resource_id == "m/beta")
        .expect("beta present")
        .cause
        .clone()
}

// Feature: explanation-causality, Property 6 — output tracing rides identity
// and the state diff, or does not fire at all.
#[test]
fn property_6_trace_fires_only_through_identity_and_state_diff() {
    // The clean trace: edge present, recorded value == D-side, != P-side.
    let (view, mut explanation) =
        traced_world("new-endpoint", true, "old-endpoint", "new-endpoint");
    apply_causality(&mut explanation, &view);
    assert!(
        matches!(
            beta_cause(&explanation).value(),
            Some(Cause::DependencyOutputChanged { dependency }) if dependency == "m/alpha"
        ),
        "clean trace classifies A4: {:?}",
        beta_cause(&explanation)
    );

    // Value match without the edge → A5, never A4 (gate i: identity).
    let (view, mut explanation) =
        traced_world("new-endpoint", false, "old-endpoint", "new-endpoint");
    apply_causality(&mut explanation, &view);
    assert!(
        matches!(
            beta_cause(&explanation).value(),
            Some(Cause::DefinitionEdit { .. })
        ),
        "no edge → A5"
    );

    // No state departure (recorded equals the baseline side too) → A5
    // (gate ii: the diff between states). The D-side matching alone is the
    // one-ended match the predicate rejects.
    let (view, mut explanation) =
        traced_world("same-endpoint", true, "same-endpoint", "same-endpoint");
    apply_causality(&mut explanation, &view);
    // D == P here means no content row at all; construct the departure-less
    // case with a real D≠P instead:
    let (view2, mut explanation2) =
        traced_world("old-endpoint", true, "old-endpoint", "new-endpoint");
    apply_causality(&mut explanation2, &view2);
    assert!(
        matches!(
            beta_cause(&explanation2).value(),
            Some(Cause::DefinitionEdit { .. })
        ),
        "recorded value equal to the baseline side is not a changed output → A5"
    );
    let _ = (view, explanation);

    // Ambiguity: two recorded outputs carrying the matching value → A5
    // (gate iii: exactly one pair).
    let (mut view3, mut explanation3) =
        traced_world("new-endpoint", true, "old-endpoint", "new-endpoint");
    let alpha = ResourceId("m/alpha".to_string());
    view3.recorded.get_mut(&alpha).unwrap().properties =
        json!({ "endpoint": "new-endpoint", "alias": "new-endpoint" });
    apply_causality(&mut explanation3, &view3);
    assert!(
        matches!(
            beta_cause(&explanation3).value(),
            Some(Cause::DefinitionEdit { .. })
        ),
        "two candidate outputs → A5"
    );
}

// ── The named example scenarios (regression memory) ──────────────────────

fn uniform_world(kind: ChangeKind, mutate: impl Fn(&mut World)) -> World {
    let mut world = World {
        rows: (0..IDS.len())
            .map(|_| Row {
                in_d: true,
                in_p: true,
                same_manifest: true,
                in_s: true,
                status: Some(RefreshStatus::DesiredLive),
                departed: false,
                kind,
            })
            .collect(),
        deps: vec![Vec::new(); IDS.len()],
        baseline: BaselineMode::Realized,
        examined: true,
        desired_available: true,
    };
    mutate(&mut world);
    world
}

// The demon test: five services, D = P, live departed from recorded on all
// five → five ProviderDrift assessments. Named for the day-long hunt that
// motivated the feature; if this fails, the plan has started lying about
// "why" again.
#[test]
fn the_demon_test_five_drifts() {
    let world = uniform_world(ChangeKind::Update, |w| {
        for row in &mut w.rows {
            row.departed = true;
        }
    });
    let (view, mut explanation) = build(&world);
    apply_causality(&mut explanation, &view);
    for change in &explanation.changes {
        assert!(
            matches!(change.cause.value(), Some(Cause::ProviderDrift)),
            "{}: {:?}",
            change.resource_id,
            change.cause
        );
    }
}

// The label migration: D = P, L = S, and the plan still computed changes —
// the new provisioner realizes what the old one did not. EngineAdvance,
// never drift and never a definition edit.
#[test]
fn the_label_migration_is_an_engine_advance() {
    let world = uniform_world(ChangeKind::Update, |_| {});
    let (view, mut explanation) = build(&world);
    apply_causality(&mut explanation, &view);
    for change in &explanation.changes {
        assert!(
            matches!(change.cause.value(), Some(Cause::EngineAdvance)),
            "{}: {:?}",
            change.resource_id,
            change.cause
        );
    }
}

// Grafana's removal: in recorded state only (removed from the definition at
// or before the baseline) → A3, an engine fact.
#[test]
fn a_resource_in_state_only_classifies_a3() {
    let world = uniform_world(ChangeKind::Delete, |w| {
        w.rows[0].in_d = false;
        w.rows[0].in_p = false;
    });
    let (view, mut explanation) = build(&world);
    apply_causality(&mut explanation, &view);
    let change = &explanation.changes[0];
    assert!(matches!(
        change.cause,
        Confidence::EngineFact {
            value: Cause::DefinitionEdit { .. },
            ..
        }
    ));
}

// The interrupted apply (A3b): desired,
// unchanged since the baseline, never recorded → the create-side reconcile
// completion, an engine fact — NEVER a generic could-not-establish.
#[test]
fn the_interrupted_apply_classifies_a3b_never_unknown() {
    let world = uniform_world(ChangeKind::Create, |w| {
        w.rows[0].in_s = false;
        w.rows[0].status = None;
    });
    let (view, mut explanation) = build(&world);
    apply_causality(&mut explanation, &view);
    let change = &explanation.changes[0];
    assert!(
        matches!(
            change.cause,
            Confidence::EngineFact {
                value: Cause::DefinitionEdit { .. },
                ..
            }
        ),
        "A3b is an engine fact, got {:?}",
        change.cause
    );
    assert!(
        !explanation.uncertainties.iter().any(|u| matches!(
            &u.reason,
            UncertaintyReason::CauseUndecidable { resource } if resource == IDS[0]
        )),
        "A3b never surfaces as could-not-establish"
    );
}

// The never-applied deployment: every create classifies as the definition
// being applied for the first time (the A10 creates rule).
#[test]
fn the_never_applied_deployment_creates_are_definition_edits() {
    let world = uniform_world(ChangeKind::Create, |w| {
        w.baseline = BaselineMode::NeverApplied;
        for row in &mut w.rows {
            row.in_s = false;
            row.status = None;
        }
    });
    let (view, mut explanation) = build(&world);
    apply_causality(&mut explanation, &view);
    for change in &explanation.changes {
        assert!(matches!(
            change.cause,
            Confidence::EngineFact {
                value: Cause::DefinitionEdit { .. },
                ..
            }
        ));
    }
}

// The transitive cascade: an edit replaces alpha; beta (depends on alpha)
// and gamma (depends on beta) cascade. One story: all three group under the
// revision comparison — the terminal cause's root — with per-change causes
// still naming the nearest dependency (Requirements 4.3/4.6).
#[test]
fn a_transitive_cascade_is_one_story() {
    let world = uniform_world(ChangeKind::Replace, |w| {
        // alpha: the definition changed (D≠P). beta, gamma: unchanged
        // definitions, cascading replaces. delta/epsilon: quiet.
        w.rows[0].same_manifest = false;
        w.deps[1] = vec![0];
        w.deps[2] = vec![1];
        w.rows[3].kind = ChangeKind::NoChange;
        w.rows[4].kind = ChangeKind::NoChange;
    });
    let (view, mut explanation) = build(&world);
    apply_causality(&mut explanation, &view);

    let cause_of = |id: &str| {
        explanation
            .changes
            .iter()
            .find(|c| c.resource_id == id)
            .unwrap()
            .cause
            .clone()
    };
    assert!(matches!(
        cause_of("m/alpha").value(),
        Some(Cause::DefinitionEdit { .. })
    ));
    assert!(matches!(
        cause_of("m/beta").value(),
        Some(Cause::ReplacementCascade { root }) if root == "m/alpha"
    ));
    assert!(
        matches!(
            cause_of("m/gamma").value(),
            Some(Cause::ReplacementCascade { root }) if root == "m/beta"
        ),
        "per-change cause names the NEAREST replaced dependency"
    );

    // One group, rooted at the revision comparison, ordered root-outward.
    assert_eq!(
        explanation.causal_groups.len(),
        1,
        "{:?}",
        explanation.causal_groups
    );
    let group = &explanation.causal_groups[0];
    assert!(matches!(
        group.root,
        CausalRoot::RevisionComparison { baseline: 4 }
    ));
    let order: Vec<&str> = group
        .members
        .iter()
        .map(|m| {
            explanation
                .changes
                .iter()
                .find(|c| &c.evidence_id == m)
                .unwrap()
                .resource_id
                .as_str()
        })
        .collect();
    assert_eq!(order, vec!["m/alpha", "m/beta", "m/gamma"]);
}

// Feature: explanation-causality, Property 5 (S-isolation), explain-side
// half: the same scenario classifies drift with S from the store and would
// classify clean with S contaminated by the live overwrite — the exact
// reclassification hazard Requirement 2.3 exists to prevent. (The
// shell-side half — gather reads the store, not the planning context — is
// structural: `gather_causality` takes no context at all.)
#[test]
fn property_5_a_contaminated_s_would_reclassify_drift_as_clean() {
    let world = uniform_world(ChangeKind::Update, |w| {
        w.rows[0].departed = true;
    });
    let (view, mut proper) = build(&world);
    apply_causality(&mut proper, &view);
    assert!(matches!(
        proper.changes[0].cause.value(),
        Some(Cause::ProviderDrift)
    ));

    // Contamination: the live observation overwrote recorded properties, so
    // the departure signal vanishes — the view a context-read S would give.
    let mut contaminated_view = view.clone();
    contaminated_view.refresh.live_departed.clear();
    let (_, mut contaminated) = build(&world);
    apply_causality(&mut contaminated, &contaminated_view);
    assert!(
        matches!(
            contaminated.changes[0].cause.value(),
            Some(Cause::EngineAdvance)
        ),
        "live-vs-live silently reads as an engine advance — the trap, demonstrated"
    );
}
