//! Gathering the causality sources (explanation-causality Phase 3): D, P, S,
//! and the union dependency graph, assembled for the classifier's
//! [`CausalityView`].
//!
//! The isolation rule this module exists to enforce (Requirement 2.3): **S is
//! read from the state store as persisted, never from a planning context.**
//! The engine's refresh overwrites in-context resource properties with live
//! observations before diffing, so any state that has passed through a
//! planning context is contaminated — drift detection over it silently
//! degenerates to live-vs-live and every drift reclassifies as clean. The
//! shell therefore loads S through the platform's own store seam
//! ([`ProvisionerPlatform::recorded_state`]) before the verb's plan (and its
//! refresh) runs; plan verbs never write the store today, and gathering
//! first keeps the isolation independent of that staying true.

use std::path::Path;

use anyhow::Result;
use tokeira_explain::{BaselineView, CausalityView};
use tokeira_iac::{PlanOutcome, ResourceId};
use tokeira_provisioner::DeploymentStateEnvelope;

use crate::{DesiredSnapshot, ProvisionerPlatform, Realization, config_history};

/// The gathered sources, before the plan outcome joins them into a view.
///
/// The baseline is typed with the classifier's own [`BaselineView`] rather
/// than a shell-local mirror: one typed resolution, not two agreeing ones
/// (recorded deviation from the design's `BaselineSnapshot` sketch).
pub(crate) struct GatheredCausality {
    pub desired: Option<DesiredSnapshot>,
    pub baseline: BaselineView,
    pub baseline_revision: u64,
    pub recorded: tokeira_iac::InfraState,
}

/// Gather D, P, and S for one verb. Never fails on causality's own account:
/// a working definition that cannot be snapshotted or a baseline that is
/// missing/broken becomes a typed, honest input the classifier turns into
/// uncertainty. The one propagated failure is the state-store read — the
/// error-handling table's rule: explanation is never built from a partial S.
pub(crate) async fn gather_causality<P: ProvisionerPlatform>(
    platform: &P,
    deployment_dir: &Path,
    envelope: &DeploymentStateEnvelope,
) -> Result<GatheredCausality> {
    // S first, from the store as persisted (Requirement 2.3).
    let recorded = platform.recorded_state(deployment_dir).await?;

    let basename = platform.config_basename(deployment_dir);
    let working = config_history::config_file(deployment_dir, basename);
    let baseline_revision = envelope.config_revision;

    // D — the working definition, realized. A platform with no interpreted
    // definition makes both D and P structurally unavailable (A10).
    let desired = match platform.desired_snapshot(deployment_dir, &working).await {
        Ok(Realization::Realized(snapshot)) => Some(snapshot),
        Ok(Realization::NotApplicable { reason }) => {
            return Ok(GatheredCausality {
                desired: None,
                baseline: BaselineView::NotInterpreted {
                    reason: reason.to_string(),
                },
                baseline_revision,
                recorded,
            });
        }
        // The verb interprets the same file and fails first on a broken
        // definition; if this races an edit, the classifier answers
        // Unknown-with-uncertainty rather than the verb failing twice.
        Err(_) => None,
    };

    // P — the baseline revision's retained definition, realized through the
    // same platform path as D (Requirement 1.6: one code path).
    let baseline = match baseline_revision {
        0 => BaselineView::NeverApplied,
        revision => {
            let retained = config_history::snapshot_path(deployment_dir, basename, revision);
            if !retained.exists() {
                BaselineView::Missing { revision }
            } else {
                match platform.desired_snapshot(deployment_dir, &retained).await {
                    Ok(Realization::Realized(snapshot)) => BaselineView::Realized(snapshot),
                    Ok(Realization::NotApplicable { reason }) => BaselineView::NotInterpreted {
                        reason: reason.to_string(),
                    },
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
/// resources alike (Requirement 5.1).
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
