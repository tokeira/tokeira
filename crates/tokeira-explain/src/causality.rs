//! Cause classification: the (D, P, S, L) algebra, grouping, and dependants
//! (explanation-causality Requirements 2–5).
//!
//! The classifier is a **pure function over four snapshots and a graph**. It
//! decides why each change is present — definition edit, dependency output
//! change, provider drift, replacement cascade, engine advance — or says
//! `Unknown` with an uncertainty, never a guess. It annotates the plan and
//! can never edit it: causes, groups, and dependants are joined onto an
//! already-built [`DeploymentExplanation`], and nothing here inspects
//! declared semantics (causality is independent of Feature 2 by
//! construction).
//!
//! The algebra's row order is normative (requirements §The Classification
//! Algebra): existence rows A1–A3b before content rows A4–A5 before state
//! rows A6–A9, A6 before A7 (a cascade would otherwise misread as drift),
//! A9 guarding A7/A8 (no drift claim over an unconfirmed live read), A10
//! branching from the baseline's availability.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use tokeira_iac::{
    ChangeKind, Citation, Confidence, RefreshCoverage, RefreshStatus, ResourceId, ResourceState,
};

use crate::{
    evidence::{EvidenceId, EvidenceKind},
    model::{
        CausalGroup, CausalRoot, Cause, DeploymentExplanation, ExplainedChange, UncertaintyReason,
    },
};

/// Canonical per-resource desired manifests from one definition source,
/// keyed by the engine's resource identity.
pub type Snapshot = BTreeMap<ResourceId, serde_json::Value>;

/// The baseline (P) as the shell resolved it. Mirrors the shell's typed
/// resolution so the classifier owns the A10 branching, not the caller.
#[derive(Debug, Clone)]
pub enum BaselineView {
    /// The retained baseline definition, realized into a snapshot.
    Realized(Snapshot),
    /// `config_revision == 0`: nothing was ever applied. Creates classify as
    /// definition edits (the A10 creates rule); anything else is off-table.
    NeverApplied,
    /// The baseline revision's retained definition file is absent.
    Missing { revision: u64 },
    /// The retained file exists but no longer interprets; `verdict` is the
    /// located error. Carries the revision so the plan-level
    /// `BaselineUnavailable` uncertainty can name it, exactly as `Missing`.
    DoesNotInterpret { revision: u64, verdict: String },
    /// The platform holds no interpreted definition at all (the snapshot
    /// seam answered NotApplicable): D and P are both structurally
    /// unavailable, and causality classifies per A10 with per-change
    /// uncertainties — but no `BaselineUnavailable`, because no retained
    /// revision is missing.
    NotInterpreted { reason: String },
}

/// Everything the algebra consumes. Owned values, assembled by the shell
/// from sources gathered under its isolation rules — most importantly S
/// read from the state store as persisted, never from a planning context
/// (Requirement 2.3: refresh overwrites in-context properties with live
/// observations, and a contaminated S turns drift detection into
/// live-vs-live).
#[derive(Debug, Clone)]
pub struct CausalityView {
    /// D — the working definition realized. `None` means it could not be
    /// snapshotted (the verb normally fails first; if explanation is reached
    /// anyway, every cause is honestly undecidable).
    pub desired: Option<Snapshot>,
    /// P — the baseline, typed.
    pub baseline: BaselineView,
    /// The baseline revision number (`envelope.config_revision`; `0` for a
    /// never-applied deployment) — the revision the definition comparison is
    /// against, named by revision-comparison roots and rendering.
    pub baseline_revision: u64,
    /// S — recorded resources from the store as persisted.
    pub recorded: BTreeMap<ResourceId, ResourceState>,
    /// The dependency graph: resource → its dependencies, over the union of
    /// the desired (engine-declared) and recorded sides, so dependants of a
    /// change include unchanged resources (Requirement 5.1).
    pub edges: BTreeMap<ResourceId, Vec<ResourceId>>,
    /// L — per-resource confirmation statuses plus the live-departure set.
    pub refresh: RefreshCoverage,
}

/// The classifier's verdict for one change: the assessment, plus the
/// uncertainty text when the cause is `Unknown` (Requirement 2.7 — an
/// unknown cause always explains itself).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Assessment {
    pub cause: Confidence<Cause>,
    pub undecidable: Option<Undecidable>,
}

/// Why a cause could not be established, pre-worded for the uncertainty.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Undecidable {
    pub consequence: String,
    pub resolvable_by: Option<String>,
}

fn engine_fact(cause: Cause) -> Confidence<Cause> {
    Confidence::EngineFact {
        value: cause,
        citation: Citation::code(module_path!()),
    }
}

fn inference(cause: Cause) -> Confidence<Cause> {
    Confidence::Inference {
        value: cause,
        citation: Citation::code(module_path!()),
    }
}

fn definition_edit() -> Assessment {
    Assessment {
        cause: engine_fact(Cause::DefinitionEdit { source: None }),
        undecidable: None,
    }
}

fn undecided(consequence: String, resolvable_by: Option<String>) -> Assessment {
    Assessment {
        cause: Confidence::Unknown,
        undecidable: Some(Undecidable {
            consequence,
            resolvable_by,
        }),
    }
}

/// Classify one non-`NoChange` change. Row order is the algebra's, exactly.
/// `replaced` is the plan's `Replace` set — A6's trigger.
pub(crate) fn assess(
    rid: &ResourceId,
    kind: ChangeKind,
    view: &CausalityView,
    replaced: &BTreeSet<ResourceId>,
) -> Assessment {
    let Some(desired) = view.desired.as_ref() else {
        return undecided(
            "the change's origin could not be established: the working definition could \
             not be interpreted for comparison"
                .to_string(),
            Some("repair the definition (definition check locates the failure)".to_string()),
        );
    };

    let baseline = match &view.baseline {
        BaselineView::Realized(p) => p,
        BaselineView::NeverApplied => {
            // A10, creates rule: on a never-applied deployment every create
            // is the definition being applied for the first time — an engine
            // fact. Anything else has state without a baseline, which is
            // off-table and says so.
            return if kind == ChangeKind::Create {
                definition_edit()
            } else {
                undecided(
                    "the change's origin could not be established: the deployment was \
                     never applied, yet the change is not a create"
                        .to_string(),
                    None,
                )
            };
        }
        BaselineView::Missing { revision } => {
            return undecided(
                "the change's origin could not be established: the baseline revision's \
                 definition is not retained, so no definition comparison exists"
                    .to_string(),
                Some(format!(
                    "restore revision {revision}'s retained definition, or apply to \
                     establish a fresh baseline"
                )),
            );
        }
        BaselineView::DoesNotInterpret { verdict, .. } => {
            return undecided(
                format!(
                    "the change's origin could not be established: the baseline revision's \
                     definition no longer interprets ({verdict})"
                ),
                Some("apply to establish a fresh baseline".to_string()),
            );
        }
        BaselineView::NotInterpreted { .. } => {
            return undecided(
                "the change's origin could not be established: this platform has no \
                 interpreted definition to compare"
                    .to_string(),
                None,
            );
        }
    };

    let in_d = desired.contains_key(rid);
    let in_p = baseline.contains_key(rid);
    let in_s = view.recorded.contains_key(rid);

    // ── Existence rows ────────────────────────────────────────────────
    // A1 (introduced) / A2 (removed) / A3 (removed at or before the
    // baseline) / A3b (introduced at or before the baseline — an
    // interrupted or partially recorded apply). A3b sits here, before the
    // state rows, deliberately: its change is a create, so cascade and
    // drift cannot meaningfully apply — and it SHALL NOT surface as a
    // generic could-not-establish.
    if in_d && !in_p {
        return definition_edit(); // A1
    }
    if !in_d && in_p {
        return definition_edit(); // A2
    }
    if !in_d && !in_p {
        return if in_s {
            definition_edit() // A3
        } else {
            undecided(
                "the change's origin could not be established: the resource appears in \
                 no definition and no recorded state"
                    .to_string(),
                None,
            )
        };
    }
    let d_eq_p = desired.get(rid) == baseline.get(rid);
    if d_eq_p && !in_s {
        return definition_edit(); // A3b
    }

    // ── Content rows (D(R) ≠ P(R)) ────────────────────────────────────
    if !d_eq_p {
        // A4: the desired difference traces to a dependency's changed
        // recorded output — identity + state diff, never bare value
        // equality (Requirement 3.5). Ambiguous or partial → A5.
        if let Some(dependency) = trace_outputs(rid, desired, baseline, view) {
            return Assessment {
                cause: inference(Cause::DependencyOutputChanged {
                    dependency: dependency.0,
                }),
                undecidable: None,
            }; // A4
        }
        return definition_edit(); // A5
    }

    // ── State rows (D(R) = P(R), R ∈ S) ───────────────────────────────
    // A6 before A7: a change forced by a replacing dependency would
    // otherwise misread as drift. The per-change cause names the *nearest*
    // replaced dependency; the group root walks to the ultimate root
    // (Requirement 4.6) at grouping time.
    if let Some(root) = nearest_replaced_dependency(rid, view, replaced) {
        return Assessment {
            cause: inference(Cause::ReplacementCascade { root: root.0 }),
            undecidable: None,
        }; // A6
    }

    // A9 guards A7/A8: no drift claim over an unconfirmed live read.
    let status = view.refresh.status_by_id.get(rid).copied();
    let confirmed = view.refresh.examined
        && matches!(
            status,
            Some(RefreshStatus::DesiredLive) | Some(RefreshStatus::DesiredMissing)
        );
    if !confirmed {
        return undecided(
            "the change's origin could not be established: live state was not confirmed, \
             so a departure cannot be told from the provisioner realizing the definition \
             differently"
                .to_string(),
            Some("confirm live state for this resource and plan again".to_string()),
        ); // A9
    }

    // A7/A8 turn on whether the confirmed live read departed from recorded
    // state — computed by the refresh pass at the only moment both sides
    // exist. A confirmed absence (`DesiredMissing`) with the resource still
    // recorded is a departure by definition.
    let departed =
        view.refresh.live_departed.contains(rid) || status == Some(RefreshStatus::DesiredMissing);
    if departed {
        return Assessment {
            cause: engine_fact(Cause::ProviderDrift),
            undecidable: None,
        }; // A7
    }
    // A8: live matches recorded, the definition matches the baseline, and
    // yet the engine computed a change — the change's existence IS the
    // D ≠ S evidence, in the engine's own diff semantics; no second,
    // generically-agreeing manifest-vs-state comparison is invented here.
    Assessment {
        cause: engine_fact(Cause::EngineAdvance),
        undecidable: None,
    } // A8
}

/// A4's trace, all three gates (design C3):
/// (i) a recorded dependency edge R → dep — identity, never coincidence;
/// (ii) a recorded output of dep whose value equals the leaf's working side
/// **and** differs from its baseline side — "changed" as a P-vs-S state
/// diff, not a one-ended match;
/// (iii) every differing leaf traces to exactly one (dependency, output)
/// pair, and all leaves agree on one dependency.
fn trace_outputs(
    rid: &ResourceId,
    desired: &Snapshot,
    baseline: &Snapshot,
    view: &CausalityView,
) -> Option<ResourceId> {
    let deps = view.edges.get(rid)?;
    if deps.is_empty() {
        return None;
    }
    let mut leaves = Vec::new();
    differing_leaves(baseline.get(rid)?, desired.get(rid)?, &mut leaves);
    if leaves.is_empty() {
        return None;
    }

    let mut traced_dep: Option<ResourceId> = None;
    for (before, after) in &leaves {
        // Candidate (dependency, output) pairs for this leaf under gate
        // (ii). Two matching outputs are ambiguity even within one
        // dependency, so pairs are counted, not dependencies.
        let mut pair_count = 0usize;
        let mut leaf_dep: Option<&ResourceId> = None;
        for dep in deps {
            let Some(state) = view.recorded.get(dep) else {
                continue;
            };
            let mut outputs = Vec::new();
            scalar_properties(&state.properties, &mut outputs);
            for value in outputs {
                if value == *after && value != *before {
                    pair_count += 1;
                    leaf_dep = Some(dep);
                }
            }
        }
        if pair_count != 1 {
            return None; // ambiguous or untraced leaf → A5 (Requirement 3.3)
        }
        let dep = leaf_dep
            .expect("pair_count == 1 implies a candidate")
            .clone();
        match &traced_dep {
            None => traced_dep = Some(dep),
            Some(prior) if *prior == dep => {}
            Some(_) => return None, // leaves disagree on the dependency
        }
    }
    traced_dep
}

/// Leaf-level differences between two canonical manifests. Objects recurse
/// per key; anything else that differs (scalars, arrays, type mismatches,
/// one-sided keys) is one leaf. Arrays are deliberately one leaf: matching
/// an array against a scalar output can never succeed, which is the
/// conservative (A5) direction.
fn differing_leaves<'a>(
    before: &'a serde_json::Value,
    after: &'a serde_json::Value,
    out: &mut Vec<(&'a serde_json::Value, &'a serde_json::Value)>,
) {
    use serde_json::Value;
    static NULL: Value = Value::Null;
    if before == after {
        return;
    }
    match (before, after) {
        (Value::Object(b), Value::Object(a)) => {
            let keys: BTreeSet<&String> = b.keys().chain(a.keys()).collect();
            for key in keys {
                match (b.get(key), a.get(key)) {
                    (Some(bv), Some(av)) => differing_leaves(bv, av, out),
                    (Some(bv), None) => out.push((bv, &NULL)),
                    (None, Some(av)) => out.push((&NULL, av)),
                    (None, None) => unreachable!("key came from one of the two maps"),
                }
            }
        }
        _ => out.push((before, after)),
    }
}

/// Every scalar value reachable in a recorded properties document — the
/// output vocabulary A4 matches against.
fn scalar_properties<'a>(value: &'a serde_json::Value, out: &mut Vec<&'a serde_json::Value>) {
    use serde_json::Value;
    match value {
        Value::Object(map) => {
            for v in map.values() {
                scalar_properties(v, out);
            }
        }
        Value::Array(items) => {
            for v in items {
                scalar_properties(v, out);
            }
        }
        Value::Null => {}
        _ => out.push(value),
    }
}

/// A6's trigger: the nearest direct dependency with a `Replace` change in
/// this plan. Deterministic on ties (sorted resource ids).
fn nearest_replaced_dependency(
    rid: &ResourceId,
    view: &CausalityView,
    replaced: &BTreeSet<ResourceId>,
) -> Option<ResourceId> {
    let deps = view.edges.get(rid)?;
    let mut hits: Vec<&ResourceId> = deps.iter().filter(|dep| replaced.contains(*dep)).collect();
    hits.sort();
    hits.first().map(|dep| (*dep).clone())
}

/// Join causality onto a built explanation: per-change causes, dependants,
/// uncertainties for every unknown cause (one-to-one with the undecided
/// changes — Requirement 2.7 / Property 10), a plan-level
/// `BaselineUnavailable` when a retained baseline is missing or broken, and
/// the causal groups (a partition of the non-`NoChange` changes under
/// bounded ultimate roots).
pub fn apply_causality(explanation: &mut DeploymentExplanation, view: &CausalityView) {
    // A6's trigger set is derived from the explanation itself, so the
    // classifier and the plan can never disagree about what replaces.
    let replaced: BTreeSet<ResourceId> = explanation
        .changes
        .iter()
        .filter(|c| c.kind == ChangeKind::Replace)
        .map(|c| ResourceId(c.resource_id.clone()))
        .collect();

    // ── Causes, and one uncertainty per undecided change ──────────────
    let mut undecidables: Vec<(EvidenceId, String, Undecidable)> = Vec::new();
    for change in &mut explanation.changes {
        if change.kind == ChangeKind::NoChange {
            continue;
        }
        let rid = ResourceId(change.resource_id.clone());
        let assessment = assess(&rid, change.kind, view, &replaced);
        change.cause = assessment.cause;
        if let Some(undecidable) = assessment.undecidable {
            undecidables.push((
                change.evidence_id.clone(),
                change.resource_id.clone(),
                undecidable,
            ));
        }
    }
    for (subject, resource, undecidable) in undecidables {
        crate::build::push_uncertainty(
            explanation,
            subject,
            UncertaintyReason::CauseUndecidable { resource },
            undecidable.consequence,
            undecidable.resolvable_by,
        );
    }
    if let Some(revision) = match &view.baseline {
        BaselineView::Missing { revision } | BaselineView::DoesNotInterpret { revision, .. } => {
            Some(*revision)
        }
        _ => None,
    } {
        let subject = EvidenceId::deployment(&explanation.deployment);
        crate::build::push_uncertainty(
            explanation,
            subject,
            UncertaintyReason::BaselineUnavailable { revision },
            format!(
                "the definition applied as revision {revision} is unavailable for \
                 comparison, so no change in this plan can be attributed to a \
                 definition edit"
            ),
            Some("apply to establish a fresh baseline".to_string()),
        );
    }

    // ── Dependants: the reverse edges of the union graph ──────────────
    let mut dependants_of: BTreeMap<ResourceId, BTreeSet<ResourceId>> = BTreeMap::new();
    for (from, deps) in &view.edges {
        for dep in deps {
            dependants_of
                .entry(dep.clone())
                .or_default()
                .insert(from.clone());
        }
    }
    for change in &mut explanation.changes {
        if change.kind == ChangeKind::NoChange {
            continue;
        }
        let rid = ResourceId(change.resource_id.clone());
        change.dependants = dependants_of
            .get(&rid)
            .map(|set| set.iter().map(|id| id.0.clone()).collect())
            .unwrap_or_default();
    }

    // ── Causal groups ─────────────────────────────────────────────────
    explanation.causal_groups = group(explanation, view);
    let group_ids: Vec<EvidenceId> = explanation
        .causal_groups
        .iter()
        .map(|g| g.evidence_id.clone())
        .collect();
    for id in group_ids {
        explanation.evidence.insert(id, EvidenceKind::CausalGroup);
    }
}

/// Grouping: every non-`NoChange` change lands in exactly one group
/// (Requirement 4.5). Cascade chains walk `ReplacementCascade` causes to
/// the first non-cascade cause — bounded by the engine-version and
/// baseline-revision boundaries (Requirement 4.6) — and take *that* cause's
/// root, so a definition edit that replaces A and drags B and C reads as
/// one story rather than a chain of linked groups. Termination: dependency
/// graphs are DAGs, and a visited set guards the walk against malformed
/// input anyway.
fn group(explanation: &DeploymentExplanation, view: &CausalityView) -> Vec<CausalGroup> {
    let by_resource: BTreeMap<&str, &ExplainedChange> = explanation
        .changes
        .iter()
        .map(|c| (c.resource_id.as_str(), c))
        .collect();

    let mut members_by_root: BTreeMap<String, (CausalRoot, Vec<EvidenceId>)> = BTreeMap::new();
    for change in &explanation.changes {
        if change.kind == ChangeKind::NoChange {
            continue;
        }
        let root = terminal_root(change, &by_resource, view.baseline_revision);
        let key = root_key(&root);
        members_by_root
            .entry(key)
            .or_insert_with(|| (root, Vec::new()))
            .1
            .push(change.evidence_id.clone());
    }

    members_by_root
        .into_values()
        .map(|(root, members)| CausalGroup {
            evidence_id: EvidenceId::group(&root_key(&root)),
            root,
            members: order_members(members, explanation, view),
        })
        .collect()
}

/// Walk a change's cause to its terminal (non-cascade) cause and answer
/// that cause's root per Requirement 4.3. The walk is bounded exactly as
/// Requirement 4.6 demands: `EngineAdvance` terminates at
/// `ProvisionerAdvance` (the engine-version boundary), and `DefinitionEdit`
/// terminates at the baseline comparison (the revision boundary) — nothing
/// attributes past either.
fn terminal_root(
    change: &ExplainedChange,
    by_resource: &BTreeMap<&str, &ExplainedChange>,
    baseline_revision: u64,
) -> CausalRoot {
    let mut current = change;
    let mut visited: BTreeSet<&str> = BTreeSet::new();
    loop {
        if !visited.insert(current.resource_id.as_str()) {
            // A cycle cannot arise from a DAG; if malformed input produces
            // one, the change roots at itself rather than looping.
            return CausalRoot::Resource(current.evidence_id.clone());
        }
        match current.cause.value() {
            Some(Cause::ReplacementCascade { root }) => match by_resource.get(root.as_str()) {
                Some(next) => current = next,
                // The named root has no change in this plan (malformed
                // input); stop at the member itself rather than invent one.
                None => return CausalRoot::Resource(current.evidence_id.clone()),
            },
            Some(Cause::DefinitionEdit { .. }) => {
                return CausalRoot::RevisionComparison {
                    baseline: baseline_revision,
                };
            }
            Some(Cause::EngineAdvance) => return CausalRoot::ProvisionerAdvance,
            Some(Cause::ProviderDrift) => {
                return CausalRoot::Resource(current.evidence_id.clone());
            }
            Some(Cause::DependencyOutputChanged { dependency }) => {
                // The named dependency's change (possibly `NoChange`) is the
                // root; every engine change carries an id, so this resolves
                // in the evidence index.
                return match by_resource.get(dependency.as_str()) {
                    Some(dep) => CausalRoot::Resource(dep.evidence_id.clone()),
                    None => CausalRoot::Resource(current.evidence_id.clone()),
                };
            }
            // Unknown: each undecided change is its own root; it renders
            // through its uncertainty, never as a cause line.
            None => return CausalRoot::Resource(current.evidence_id.clone()),
        }
    }
}

fn root_key(root: &CausalRoot) -> String {
    match root {
        CausalRoot::RevisionComparison { baseline } => {
            format!("revision-comparison:{baseline}")
        }
        CausalRoot::Resource(id) => format!("resource:{}", id.as_str()),
        CausalRoot::ProvisionerAdvance => "provisioner-advance".to_string(),
    }
}

/// Requirement 4.2: members ordered along the dependency path from the root
/// outward — in-group BFS layering over the dependency edges (a dependency
/// precedes its dependants), ties and path-unconnected members by id.
fn order_members(
    members: Vec<EvidenceId>,
    explanation: &DeploymentExplanation,
    view: &CausalityView,
) -> Vec<EvidenceId> {
    let rid_of: BTreeMap<&EvidenceId, ResourceId> = explanation
        .changes
        .iter()
        .map(|c| (&c.evidence_id, ResourceId(c.resource_id.clone())))
        .collect();
    let member_rids: BTreeMap<ResourceId, EvidenceId> = members
        .iter()
        .filter_map(|m| rid_of.get(m).map(|rid| (rid.clone(), m.clone())))
        .collect();
    let in_group = |rid: &ResourceId| member_rids.contains_key(rid);

    // Layer 0: members none of whose dependencies are group members (the
    // heads nearest the root); each further layer depends on an earlier one.
    let mut depth: BTreeMap<EvidenceId, usize> = BTreeMap::new();
    let mut queue: VecDeque<ResourceId> = VecDeque::new();
    for (rid, member) in &member_rids {
        let parents = view
            .edges
            .get(rid)
            .map(|deps| deps.iter().filter(|d| in_group(d)).count())
            .unwrap_or(0);
        if parents == 0 {
            depth.insert(member.clone(), 0);
            queue.push_back(rid.clone());
        }
    }
    while let Some(current) = queue.pop_front() {
        let current_depth = depth[&member_rids[&current]];
        for (rid, member) in &member_rids {
            if depth.contains_key(member) {
                continue;
            }
            let depends_on_current = view
                .edges
                .get(rid)
                .map(|deps| deps.contains(&current))
                .unwrap_or(false);
            if depends_on_current {
                depth.insert(member.clone(), current_depth + 1);
                queue.push_back(rid.clone());
            }
        }
    }

    let mut ordered = members;
    ordered.sort_by(|a, b| match (depth.get(a), depth.get(b)) {
        (Some(x), Some(y)) => x.cmp(y).then_with(|| a.cmp(b)),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => a.cmp(b),
    });
    ordered
}
