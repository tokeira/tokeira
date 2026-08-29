//! Gathering the causality sources: D, P, S,
//! and the union dependency graph, assembled for the classifier's
//! [`CausalityView`].
//!
//! The isolation rule this module exists to enforce: **S is
//! read from the state store as persisted, never from a planning context.**
//! The engine's refresh overwrites in-context resource properties with live
//! observations before diffing, so any state that has passed through a
//! planning context is contaminated — drift detection over it silently
//! degenerates to live-vs-live and every drift reclassifies as clean. The
//! shell therefore loads S through the engine's own store read
//! ([`Engine::recorded_state`]) before the verb's plan (and its refresh)
//! runs; plan verbs never write the store today, and gathering first keeps
//! the isolation independent of that staying true.

use anyhow::Result;
use tokeira_deployment::DeploymentStateEnvelope;
use tokeira_explain::{BaselineView, CausalityView};
use tokeira_iac::{PlanOutcome, ResourceId};

use tokeira_platform::definition::DefinitionFrontend;

use crate::{DesiredSnapshot, config_history, engine::Engine, platform::Admitted};

/// The gathered sources, before the plan outcome joins them into a view.
///
/// The baseline is typed with the classifier's own [`BaselineView`] rather
/// than a shell-local mirror: one typed resolution, not two agreeing ones
/// (recorded deviation from the design's `BaselineSnapshot` sketch).
pub(crate) struct GatheredCausality {
    pub(crate) desired: Option<DesiredSnapshot>,
    pub(crate) baseline: BaselineView,
    pub(crate) baseline_revision: u64,
    pub(crate) recorded: tokeira_iac::InfraState,
}

/// Gather D, P, and S for one verb. Never fails on causality's own account:
/// a working definition that cannot be snapshotted or a baseline that is
/// missing/broken becomes a typed, honest input the classifier turns into
/// uncertainty. The one propagated failure is the state-store read — the
/// error-handling table's rule: explanation is never built from a partial S.
pub(crate) async fn gather_causality<F: DefinitionFrontend>(
    engine: &Engine<F>,
    admitted: &Admitted,
    envelope: &DeploymentStateEnvelope,
) -> Result<GatheredCausality> {
    let deployment_dir = admitted.deployment_ref.dir.as_path();
    // S first, from the store as persisted.
    let recorded = engine.recorded_state(admitted).await?;

    let source = admitted.config_source();
    let working = config_history::config_file(deployment_dir, &source);
    let baseline_revision = envelope.config_revision;

    // D — the working definition, realized. A bound platform always
    // interprets, so D is absent only when the definition itself is broken.
    // The verb interprets the same file and fails first on a broken
    // definition; if this races an edit, the classifier answers
    // Unknown-with-uncertainty rather than the verb failing twice.
    let desired = engine.desired_snapshot(admitted, &working).ok();

    // P — the baseline revision's retained definition, realized through the
    // same engine path as D.
    let baseline = match baseline_revision {
        0 => BaselineView::NeverApplied,
        revision => {
            let retained = config_history::snapshot_path(deployment_dir, &source, revision);
            if !retained.exists() {
                BaselineView::Missing { revision }
            } else {
                match engine.desired_snapshot(admitted, &retained) {
                    Ok(snapshot) => BaselineView::Realized(snapshot),
                    Err(err) => BaselineView::DoesNotInterpret {
                        revision,
                        verdict: format!("{err:#}"),
                    },
                }
            }
        }
    };

    Ok(GatheredCausality {
        desired,
        baseline,
        baseline_revision,
        recorded,
    })
}

/// Join the gathered sources with the plan outcome into the classifier's
/// view. The graph is the union of the engine-declared desired edges
/// (`PlanOutcome::edges_by_id`) and the recorded edges
/// (`ResourceState::dependencies`), per resource, sorted for determinism —
/// dependants of a change must include unchanged and no-longer-desired
/// resources alike.
pub(crate) fn causality_view(gathered: GatheredCausality, outcome: &PlanOutcome) -> CausalityView {
    let mut edges: std::collections::BTreeMap<ResourceId, Vec<ResourceId>> = outcome
        .edges_by_id
        .iter()
        .map(|(id, deps)| (id.clone(), deps.clone()))
        .collect();
    for (id, state) in &gathered.recorded.resources {
        let deps = edges.entry(id.clone()).or_default();
        for dep in &state.dependencies {
            if !deps.contains(dep) {
                deps.push(dep.clone());
            }
        }
    }
    for deps in edges.values_mut() {
        deps.sort();
        deps.dedup();
    }

    CausalityView {
        desired: gathered.desired,
        baseline: gathered.baseline,
        baseline_revision: gathered.baseline_revision,
        recorded: gathered.recorded.resources,
        edges,
        desired_edges: outcome
            .edges_by_id
            .iter()
            .map(|(id, deps)| (id.clone(), deps.clone()))
            .collect(),
        refresh: outcome.refresh.clone(),
    }
}
