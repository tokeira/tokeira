//! Stateless infrastructure lifecycle engine.
//!
//! [`Engine`] coordinates plan/apply/destroy operations over a set of
//! [`Resource`] objects. It is stateless — the caller (typically the
//! orchestrator) owns persistence. The engine reads and mutates state
//! through [`ProvisionContext`].
//!
//! The engine distinguishes two resource sets:
//!
//! - **desired**: resources that should exist after apply.
//! - **known**: all resources the deployment knows how to manage, including
//!   resources that are no longer desired but may still exist in state and
//!   need to be deleted.
//!
//! When `known` and `desired` are the same (the common case), the engine
//! can delete any resource that disappears from config. When they differ,
//! the engine uses `known` to look up `Resource` objects for deletion and
//! `describe()` calls, ensuring provider resources are properly cleaned up
//! rather than silently orphaned from state.
//!
//! ## State persistence
//!
//! The engine does not own a state store. Instead, callers may supply an
//! optional [`StateSaver`] callback that is invoked after each individual
//! create/update/delete operation. This gives the orchestrator incremental
//! crash-safety without coupling the engine to a specific store type.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    pin::Pin,
};

use tracing::{info, warn};

use crate::{
    InternalChange, ProvisionContext, Resource, ResourceId,
    error::IacError,
    module::ModuleContext,
    types::{Change, ChangeKind, FieldDiff, InfraComposition},
};

/// Callback invoked after each mutating operation so the orchestrator can
/// persist state incrementally. Receives the current state snapshot.
///
/// If not provided, state is only mutated in memory and the caller is
/// responsible for persisting after the engine returns.
pub type StateSaver = Box<
    dyn Fn(
            &crate::document::InfraState,
        ) -> Pin<Box<dyn Future<Output = Result<(), IacError>> + Send + '_>>
        + Send
        + Sync,
>;

/// Stateless IaC engine.
///
/// The orchestrator manages state persistence. This engine computes plans
/// and applies changes using the `known` vs `desired` resource model to
/// ensure removed resources are properly deleted through their `Resource`
/// implementation rather than silently dropped from state.
pub struct Engine;

impl Engine {
    pub fn new() -> Self {
        Self
    }

    /// Compute a plan showing what would change, without side effects.
    ///
    /// Calls `describe()` on all known resources to refresh live state before
    /// diffing. Resources in state but not in the desired set are planned for
    /// deletion.
    pub async fn plan(
        &self,
        composition: &InfraComposition,
        ctx: &mut ProvisionContext,
    ) -> Result<Vec<Change>, IacError> {
        let desired = collect_resources_from(&composition.desired_modules, ctx)?;
        let known = collect_resources_from(&composition.known_modules, ctx)?;
        let desired_refs: Vec<&dyn Resource> = desired.iter().map(|r| r.as_ref()).collect();
        let known_refs: Vec<&dyn Resource> = known.iter().map(|r| r.as_ref()).collect();
        self.plan_with_known(&desired_refs, &known_refs, ctx).await
    }

    /// Plan using a separate known-managed resource set.
    pub async fn plan_with_known(
        &self,
        desired: &[&dyn Resource],
        known: &[&dyn Resource],
        ctx: &mut ProvisionContext,
    ) -> Result<Vec<Change>, IacError> {
        let refreshed = refresh_state(known, desired, ctx).await?;
        ctx.state = refreshed.state;
        Ok(compute_changes(desired, &ctx.state, ctx))
    }

    /// Plan restricted to the provided modules.
    pub async fn plan_for_modules(
        &self,
        composition: &InfraComposition,
        ctx: &mut ProvisionContext,
    ) -> Result<Vec<Change>, IacError> {
        let desired = collect_resources_from(&composition.desired_modules, ctx)?;
        let known = collect_resources_from(&composition.known_modules, ctx)?;
        let desired_refs: Vec<&dyn Resource> = desired.iter().map(|r| r.as_ref()).collect();
        let known_refs: Vec<&dyn Resource> = known.iter().map(|r| r.as_ref()).collect();
        let refreshed = refresh_state(&known_refs, &desired_refs, ctx).await?;
        ctx.state = refreshed.state;
        let changes = compute_changes(&desired_refs, &ctx.state, ctx);
        let active: Vec<&str> = composition
            .active_modules
            .iter()
            .map(|s| s.as_str())
            .collect();
        Ok(filter_changes_by_modules(&changes, &ctx.state, &active))
    }

    /// Apply changes: create/update desired resources, delete removed resources.
    pub async fn apply(
        &self,
        composition: &InfraComposition,
        ctx: &mut ProvisionContext,
        saver: Option<&StateSaver>,
    ) -> Result<Vec<Change>, IacError> {
        let desired = collect_resources_from(&composition.desired_modules, ctx)?;
        let known = collect_resources_from(&composition.known_modules, ctx)?;
        let desired_refs: Vec<&dyn Resource> = desired.iter().map(|r| r.as_ref()).collect();
        let known_refs: Vec<&dyn Resource> = known.iter().map(|r| r.as_ref()).collect();
        self.apply_with_known(&desired_refs, &known_refs, ctx, saver)
            .await
    }

    /// Apply using a separate known-managed resource set.
    ///
    /// `known` must be a superset of `desired`. Resources in `known` but not
    /// in `desired` that exist in state will be deleted through their
    /// `Resource::delete()` implementation.
    pub async fn apply_with_known(
        &self,
        desired: &[&dyn Resource],
        known: &[&dyn Resource],
        ctx: &mut ProvisionContext,
        saver: Option<&StateSaver>,
    ) -> Result<Vec<Change>, IacError> {
        let refreshed = refresh_state(known, desired, ctx).await?;
        if refreshed.has_managed_missing {
            ctx.state = refreshed.state.clone();
            if let Some(save) = saver {
                save(&ctx.state).await?;
            }
        }
        ctx.state = refreshed.state;
        let changes = compute_changes(desired, &ctx.state, ctx);
        apply_changes(known, ctx, changes, saver).await
    }

    /// Apply restricted to the active modules in the composition.
    pub async fn apply_for_modules(
        &self,
        composition: &InfraComposition,
        ctx: &mut ProvisionContext,
        saver: Option<&StateSaver>,
    ) -> Result<Vec<Change>, IacError> {
        let desired = collect_resources_from(&composition.desired_modules, ctx)?;
        let known = collect_resources_from(&composition.known_modules, ctx)?;
        let desired_refs: Vec<&dyn Resource> = desired.iter().map(|r| r.as_ref()).collect();
        let known_refs: Vec<&dyn Resource> = known.iter().map(|r| r.as_ref()).collect();
        let refreshed = refresh_state(&known_refs, &desired_refs, ctx).await?;
        if refreshed.has_managed_missing {
            ctx.state = refreshed.state.clone();
            if let Some(save) = saver {
                save(&ctx.state).await?;
            }
        }
        ctx.state = refreshed.state;
        let changes = compute_changes(&desired_refs, &ctx.state, ctx);
        let active: Vec<&str> = composition
            .active_modules
            .iter()
            .map(|s| s.as_str())
            .collect();
        let filtered = filter_changes_by_modules(&changes, &ctx.state, &active);
        apply_changes(&known_refs, ctx, filtered, saver).await
    }

    /// Destroy resources in reverse dependency order.
    pub async fn destroy(
        &self,
        composition: &InfraComposition,
        ctx: &mut ProvisionContext,
        saver: Option<&StateSaver>,
    ) -> Result<Vec<Change>, IacError> {
        let known = collect_resources_from(&composition.known_modules, ctx)?;
        let known_refs: Vec<&dyn Resource> = known.iter().map(|r| r.as_ref()).collect();
        self.destroy_known(&known_refs, ctx, saver).await
    }

    /// Destroy using a known-managed resource set.
    pub async fn destroy_known(
        &self,
        known: &[&dyn Resource],
        ctx: &mut ProvisionContext,
        saver: Option<&StateSaver>,
    ) -> Result<Vec<Change>, IacError> {
        let desired: [&dyn Resource; 0] = [];
        let refreshed = refresh_state(known, &desired, ctx).await?;
        if refreshed.has_managed_missing {
            ctx.state = refreshed.state.clone();
            if let Some(save) = saver {
                save(&ctx.state).await?;
            }
        }
        ctx.state = refreshed.state;
        let changes = compute_changes(&desired, &ctx.state, ctx);
        destroy_changes(known, ctx, changes, saver).await
    }

    /// Destroy restricted to the active modules in the composition.
    pub async fn destroy_for_modules(
        &self,
        composition: &InfraComposition,
        ctx: &mut ProvisionContext,
        saver: Option<&StateSaver>,
    ) -> Result<Vec<Change>, IacError> {
        let known = collect_resources_from(&composition.known_modules, ctx)?;
        let known_refs: Vec<&dyn Resource> = known.iter().map(|r| r.as_ref()).collect();
        let desired: [&dyn Resource; 0] = [];
        let refreshed = refresh_state(&known_refs, &desired, ctx).await?;
        if refreshed.has_managed_missing {
            ctx.state = refreshed.state.clone();
            if let Some(save) = saver {
                save(&ctx.state).await?;
            }
        }
        ctx.state = refreshed.state;
        let changes = compute_changes(&desired, &ctx.state, ctx);
        let active: Vec<&str> = composition
            .active_modules
            .iter()
            .map(|s| s.as_str())
            .collect();
        let filtered = filter_changes_by_modules(&changes, &ctx.state, &active);
        destroy_changes(&known_refs, ctx, filtered, saver).await
    }
}

// ── Resource collection ───────────────────────────────────────────────

/// Collect resources from modules in dependency order.
///
/// Modules are topologically sorted by `Module::dependencies()` before
/// resource expansion. This ensures that a module's resources are collected
/// after the modules it depends on, so that any state lookups or extension
/// reads during `Module::resources()` see the correct ordering context.
///
/// Returns `IacError::DependencyResolution` if the module dependency graph
/// contains a cycle.
fn collect_resources_from(
    modules: &[Box<dyn crate::Module>],
    ctx: &ProvisionContext,
) -> Result<Vec<Box<dyn Resource>>, IacError> {
    let sorted = topological_sort_modules(modules)?;
    let module_map: HashMap<&str, &Box<dyn crate::Module>> =
        modules.iter().map(|m| (m.name(), m)).collect();
    let module_ctx = ModuleContext::new(&ctx.state, ctx.extensions());
    let mut all = Vec::new();
    for name in &sorted {
        if let Some(module) = module_map.get(name.as_str()) {
            all.extend(module.resources(&module_ctx)?);
        }
    }
    Ok(all)
}

/// Topological sort of modules by `Module::dependencies()`.
///
/// Returns module names in dependency order (dependencies first).
/// Only considers dependencies that are present in the input set —
/// external dependencies are silently ignored.
fn topological_sort_modules(modules: &[Box<dyn crate::Module>]) -> Result<Vec<String>, IacError> {
    let names: HashSet<&str> = modules.iter().map(|m| m.name()).collect();

    let mut in_degree: HashMap<&str, usize> = HashMap::new();
    let mut dependents: HashMap<&str, Vec<&str>> = HashMap::new();

    for module in modules {
        let name = module.name();
        in_degree.entry(name).or_insert(0);
        for dep in module.dependencies() {
            if names.contains(dep) {
                *in_degree.entry(name).or_insert(0) += 1;
                dependents.entry(dep).or_default().push(name);
            }
        }
    }

    let mut queue: VecDeque<&str> = VecDeque::new();
    let mut initial: Vec<&str> = in_degree
        .iter()
        .filter(|&(_, deg)| *deg == 0)
        .map(|(&name, _)| name)
        .collect();
    initial.sort();
    queue.extend(initial);

    let mut result = Vec::new();

    while let Some(name) = queue.pop_front() {
        result.push(name.to_string());
        if let Some(deps) = dependents.get(name) {
            let mut next_batch: Vec<&str> = Vec::new();
            for dep in deps {
                if let Some(deg) = in_degree.get_mut(dep) {
                    *deg -= 1;
                    if *deg == 0 {
                        next_batch.push(dep);
                    }
                }
            }
            next_batch.sort();
            queue.extend(next_batch);
        }
    }

    if result.len() != names.len() {
        let in_result: HashSet<&str> = result.iter().map(|s| s.as_str()).collect();
        let cycle_members: Vec<&str> = names.difference(&in_result).copied().collect();
        return Err(IacError::DependencyResolution(format!(
            "cycle detected in module dependencies: {:?}",
            cycle_members
        )));
    }

    Ok(result)
}

// ── Live state refresh ────────────────────────────────────────────────

/// Per-resource refresh outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RefreshStatus {
    /// Resource is desired and exists in the provider.
    DesiredLive,
    /// Resource is desired but absent from the provider.
    DesiredMissing,
    /// Resource is known-managed (not desired) and exists in the provider.
    ManagedLive,
    /// Resource is known-managed (not desired) and absent from the provider.
    /// State should be pruned.
    ManagedMissing,
}

#[derive(Debug)]
#[allow(dead_code)] // status_by_id retained for diagnostic reporting
struct RefreshReport {
    state: crate::document::InfraState,
    status_by_id: HashMap<ResourceId, RefreshStatus>,
    /// True if any managed-but-not-desired resource was absent from the
    /// provider, indicating stale state entries should be pruned.
    has_managed_missing: bool,
}

/// Refresh persisted state against live provider state for all known resources.
///
/// This ensures the engine operates on real provider state rather than
/// potentially stale persisted state. Resources that exist in the provider
/// but not in persisted state are added. Resources that are absent from the
/// provider but present in persisted state are removed.
async fn refresh_state(
    known: &[&dyn Resource],
    desired: &[&dyn Resource],
    ctx: &mut ProvisionContext,
) -> Result<RefreshReport, IacError> {
    let desired_ids: HashSet<ResourceId> = desired.iter().map(|r| r.resource_id()).collect();
    let sorted = topological_sort(known)?;
    let resource_map: HashMap<ResourceId, &dyn Resource> =
        known.iter().map(|r| (r.resource_id(), *r)).collect();

    let mut refreshed = ctx.state.clone();
    let mut status_by_id = HashMap::new();
    let mut has_managed_missing = false;

    for resource_id in sorted {
        let resource = resource_map.get(&resource_id).ok_or_else(|| {
            IacError::DependencyResolution(format!(
                "resource {resource_id:?} not found in known managed set"
            ))
        })?;
        let is_desired = desired_ids.contains(&resource_id);

        match resource.describe(ctx).await.map_err(|err| {
            IacError::Other(anyhow::anyhow!(
                "failed to describe resource '{}' during refresh: {err}",
                resource_id.0
            ))
        })? {
            Some(live_state) => {
                refreshed
                    .resources
                    .insert(resource_id.clone(), live_state.clone());
                ctx.state.resources.insert(resource_id.clone(), live_state);
                status_by_id.insert(
                    resource_id,
                    if is_desired {
                        RefreshStatus::DesiredLive
                    } else {
                        RefreshStatus::ManagedLive
                    },
                );
            }
            None => {
                refreshed.resources.remove(&resource_id);
                ctx.state.resources.remove(&resource_id);
                let status = if is_desired {
                    RefreshStatus::DesiredMissing
                } else {
                    has_managed_missing = true;
                    info!(
                        ?resource_id,
                        "managed resource absent from provider, pruning from state"
                    );
                    RefreshStatus::ManagedMissing
                };
                status_by_id.insert(resource_id, status);
            }
        }
    }

    Ok(RefreshReport {
        state: refreshed,
        status_by_id,
        has_managed_missing,
    })
}

// ── Change computation ────────────────────────────────────────────────

fn compute_changes(
    desired: &[&dyn Resource],
    state: &crate::document::InfraState,
    ctx: &ProvisionContext,
) -> Vec<Change> {
    let desired_ids: HashSet<ResourceId> = desired.iter().map(|r| r.resource_id()).collect();
    let mut changes = Vec::new();

    for resource in desired {
        let rid = resource.resource_id();
        match state.resources.get(&rid) {
            Some(current) => {
                let diff = resource.diff(current, ctx);
                changes.push(internal_change_to_flat(
                    &diff,
                    resource.module(),
                    &rid.0,
                    &resource.resource_type().0,
                ));
            }
            None => {
                changes.push(Change {
                    kind: ChangeKind::Create,
                    resource_type: resource.resource_type().0.clone(),
                    module: resource.module().to_string(),
                    resource: rid.0.clone(),
                    details: Vec::new(),
                });
            }
        }
    }

    // Resources in state but not desired → delete
    for (rid, rs) in &state.resources {
        if !desired_ids.contains(rid) {
            changes.push(Change {
                kind: ChangeKind::Delete,
                resource_type: rs.resource_type.0.clone(),
                module: rs.module.clone(),
                resource: rid.0.clone(),
                details: Vec::new(),
            });
        }
    }

    changes
}

// ── Apply ─────────────────────────────────────────────────────────────

async fn apply_changes(
    known: &[&dyn Resource],
    ctx: &mut ProvisionContext,
    changes: Vec<Change>,
    saver: Option<&StateSaver>,
) -> Result<Vec<Change>, IacError> {
    let resource_map: HashMap<ResourceId, &dyn Resource> =
        known.iter().map(|r| (r.resource_id(), *r)).collect();

    let sorted = topological_sort(known)?;
    let total_operations = changes
        .iter()
        .filter(|c| c.kind != ChangeKind::NoChange)
        .count();
    let mut operation_index = 0usize;

    // Create and update in topological order
    for rid in &sorted {
        let change = changes.iter().find(|c| c.resource == rid.0);
        match change {
            Some(c) if c.kind == ChangeKind::Create => {
                let resource = resource_map.get(rid).ok_or_else(|| {
                    IacError::DependencyResolution(format!(
                        "resource {rid:?} not found in known set"
                    ))
                })?;
                operation_index += 1;
                ctx.emit_apply_progress(
                    "create",
                    rid,
                    &resource.resource_type(),
                    operation_index,
                    total_operations,
                );
                info!(?rid, "creating resource");
                let rs = resource.create(ctx).await?;
                ctx.state.resources.insert(rid.clone(), rs);
                if let Some(save) = saver {
                    save(&ctx.state).await?;
                }
            }
            Some(c) if c.kind == ChangeKind::Update => {
                let resource = resource_map.get(rid).ok_or_else(|| {
                    IacError::DependencyResolution(format!(
                        "resource {rid:?} not found in known set"
                    ))
                })?;
                let current = ctx.state.resources.get(rid).ok_or_else(|| {
                    IacError::StateNotFound(format!(
                        "resource {rid:?} missing from state during update"
                    ))
                })?;
                operation_index += 1;
                ctx.emit_apply_progress(
                    "update",
                    rid,
                    &resource.resource_type(),
                    operation_index,
                    total_operations,
                );
                info!(?rid, "updating resource");
                let rs = resource.update(current, ctx).await?;
                ctx.state.resources.insert(rid.clone(), rs);
                if let Some(save) = saver {
                    save(&ctx.state).await?;
                }
            }
            _ => {}
        }
    }

    // Delete in reverse dependency order
    let delete_ids: HashSet<ResourceId> = changes
        .iter()
        .filter(|c| c.kind == ChangeKind::Delete)
        .map(|c| ResourceId(c.resource.clone()))
        .collect();

    if !delete_ids.is_empty() {
        let sorted_deletes = topological_sort_from_state(&delete_ids, &ctx.state);
        let reversed: Vec<ResourceId> = sorted_deletes.into_iter().rev().collect();

        for rid in &reversed {
            if let Some(current) = ctx.state.resources.get(rid).cloned() {
                if let Some(resource) = resource_map.get(rid) {
                    operation_index += 1;
                    ctx.emit_apply_progress(
                        "delete",
                        rid,
                        &current.resource_type,
                        operation_index,
                        total_operations,
                    );
                    info!(?rid, "deleting resource");
                    resource.delete(&current, ctx).await?;
                } else {
                    warn!(
                        ?rid,
                        "resource in state but not in known managed set — removing from state only"
                    );
                }
                ctx.state.resources.remove(rid);
                if let Some(save) = saver {
                    save(&ctx.state).await?;
                }
            }
        }
    }

    Ok(changes)
}

// ── Destroy ───────────────────────────────────────────────────────────

async fn destroy_changes(
    known: &[&dyn Resource],
    ctx: &mut ProvisionContext,
    changes: Vec<Change>,
    saver: Option<&StateSaver>,
) -> Result<Vec<Change>, IacError> {
    let resource_map: HashMap<ResourceId, &dyn Resource> =
        known.iter().map(|r| (r.resource_id(), *r)).collect();

    let delete_ids: HashSet<ResourceId> = changes
        .iter()
        .filter(|c| c.kind == ChangeKind::Delete)
        .map(|c| ResourceId(c.resource.clone()))
        .collect();

    let sorted_deletes = topological_sort_from_state(&delete_ids, &ctx.state);
    let reversed: Vec<ResourceId> = sorted_deletes.into_iter().rev().collect();
    let total_operations = reversed.len();
    let mut operation_index = 0usize;

    for rid in &reversed {
        if let Some(_current) = ctx.state.resources.get(rid).cloned() {
            if let Some(resource) = resource_map.get(rid) {
                // Re-describe to get live state before deleting
                match resource.describe(ctx).await.map_err(|err| {
                    IacError::Other(anyhow::anyhow!(
                        "failed to describe resource '{}' during destroy: {err}",
                        rid.0
                    ))
                })? {
                    Some(live_state) => {
                        operation_index += 1;
                        ctx.emit_apply_progress(
                            "delete",
                            rid,
                            &live_state.resource_type,
                            operation_index,
                            total_operations,
                        );
                        info!(?rid, "destroying resource");
                        resource.delete(&live_state, ctx).await?;
                    }
                    None => {
                        warn!(
                            ?rid,
                            "resource already absent from provider, pruning from state"
                        );
                    }
                }
            } else {
                warn!(
                    ?rid,
                    "resource in state but not in known managed set — skipping delete"
                );
            }
            ctx.state.resources.remove(rid);
            if let Some(save) = saver {
                save(&ctx.state).await?;
            }
        }
    }

    Ok(changes)
}

// ── Module filtering ──────────────────────────────────────────────────

fn filter_changes_by_modules(
    changes: &[Change],
    state: &crate::document::InfraState,
    active_modules: &[&str],
) -> Vec<Change> {
    let active: HashSet<&str> = active_modules.iter().copied().collect();
    changes
        .iter()
        .filter(|change| match change.kind {
            ChangeKind::Delete => state
                .resources
                .get(&ResourceId(change.resource.clone()))
                .map(|rs| active.contains(rs.module.as_str()))
                .unwrap_or(false),
            _ => true,
        })
        .cloned()
        .collect()
}

// ── Topological sort ──────────────────────────────────────────────────

/// Kahn's algorithm for topological sort on resource dependencies.
/// Returns resource IDs in creation order (dependencies first).
pub fn topological_sort(resources: &[&dyn Resource]) -> Result<Vec<ResourceId>, IacError> {
    let ids: HashSet<ResourceId> = resources.iter().map(|r| r.resource_id()).collect();

    let mut in_degree: HashMap<ResourceId, usize> = HashMap::new();
    let mut dependents: HashMap<ResourceId, Vec<ResourceId>> = HashMap::new();

    for r in resources {
        let rid = r.resource_id();
        in_degree.entry(rid.clone()).or_insert(0);
        for dep in r.dependencies() {
            if ids.contains(&dep) {
                *in_degree.entry(rid.clone()).or_insert(0) += 1;
                dependents.entry(dep).or_default().push(rid.clone());
            }
        }
    }

    let mut queue: VecDeque<ResourceId> = VecDeque::new();
    let mut initial: Vec<ResourceId> = in_degree
        .iter()
        .filter(|&(_, deg)| *deg == 0)
        .map(|(id, _)| id.clone())
        .collect();
    initial.sort();
    queue.extend(initial);

    let mut result = Vec::new();

    while let Some(rid) = queue.pop_front() {
        result.push(rid.clone());
        if let Some(deps) = dependents.get(&rid) {
            let mut next_batch = Vec::new();
            for dep in deps {
                if let Some(deg) = in_degree.get_mut(dep) {
                    *deg -= 1;
                    if *deg == 0 {
                        next_batch.push(dep.clone());
                    }
                }
            }
            next_batch.sort();
            queue.extend(next_batch);
        }
    }

    if result.len() != ids.len() {
        return Err(IacError::DependencyResolution(
            "cycle detected in resource dependencies".into(),
        ));
    }

    Ok(result)
}

/// Topological sort using dependency info from persisted state.
///
/// Used for ordering deletes when the `Resource` object may not be in the
/// desired set. Falls back to alphabetical order for resources without
/// dependency info in state.
fn topological_sort_from_state(
    ids: &HashSet<ResourceId>,
    state: &crate::document::InfraState,
) -> Vec<ResourceId> {
    let mut in_degree: HashMap<ResourceId, usize> = HashMap::new();
    let mut dependents: HashMap<ResourceId, Vec<ResourceId>> = HashMap::new();

    for rid in ids {
        in_degree.entry(rid.clone()).or_insert(0);
        if let Some(rs) = state.resources.get(rid) {
            for dep in &rs.dependencies {
                if ids.contains(dep) {
                    *in_degree.entry(rid.clone()).or_insert(0) += 1;
                    dependents.entry(dep.clone()).or_default().push(rid.clone());
                }
            }
        }
    }

    let mut queue: VecDeque<ResourceId> = VecDeque::new();
    let mut initial: Vec<ResourceId> = in_degree
        .iter()
        .filter(|&(_, deg)| *deg == 0)
        .map(|(id, _)| id.clone())
        .collect();
    initial.sort();
    queue.extend(initial);

    let mut result = Vec::new();

    while let Some(rid) = queue.pop_front() {
        result.push(rid.clone());
        if let Some(deps) = dependents.get(&rid) {
            let mut next_batch = Vec::new();
            for dep in deps {
                if let Some(deg) = in_degree.get_mut(dep) {
                    *deg -= 1;
                    if *deg == 0 {
                        next_batch.push(dep.clone());
                    }
                }
            }
            next_batch.sort();
            queue.extend(next_batch);
        }
    }

    // Append remaining IDs in sorted order if cycle or missing deps
    if result.len() < ids.len() {
        let in_result: HashSet<_> = result.iter().cloned().collect();
        let mut remaining: Vec<_> = ids.difference(&in_result).cloned().collect();
        remaining.sort();
        result.extend(remaining);
    }

    result
}

// ── Change conversion ─────────────────────────────────────────────────

fn internal_change_to_flat(
    change: &InternalChange,
    module: &str,
    resource_name: &str,
    resource_type: &str,
) -> Change {
    match change {
        InternalChange::Create {
            resource_id,
            resource_type: rt,
        } => Change {
            kind: ChangeKind::Create,
            resource_type: rt.0.clone(),
            module: module.to_string(),
            resource: resource_id.0.clone(),
            details: Vec::new(),
        },
        InternalChange::Update {
            resource_id,
            resource_type: rt,
            details,
        } => Change {
            kind: ChangeKind::Update,
            resource_type: rt.0.clone(),
            module: module.to_string(),
            resource: resource_id.0.clone(),
            details: vec![FieldDiff {
                field: "config".to_string(),
                before: None,
                after: Some(details.clone()),
            }],
        },
        InternalChange::Delete {
            resource_id,
            resource_type: rt,
        } => Change {
            kind: ChangeKind::Delete,
            resource_type: rt.0.clone(),
            module: module.to_string(),
            resource: resource_id.0.clone(),
            details: Vec::new(),
        },
        InternalChange::NoChange { resource_id: _ } => Change {
            kind: ChangeKind::NoChange,
            resource_type: resource_type.to_string(),
            module: module.to_string(),
            resource: resource_name.to_string(),
            details: Vec::new(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Module, Resource};

    #[derive(Debug)]
    struct StubModule {
        name: &'static str,
        deps: &'static [&'static str],
    }

    impl Module for StubModule {
        fn name(&self) -> &str {
            self.name
        }
        fn dependencies(&self) -> &[&str] {
            self.deps
        }
        fn resources(
            &self,
            _ctx: &crate::ModuleContext,
        ) -> Result<Vec<Box<dyn Resource>>, IacError> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn modules_sorted_by_dependencies() {
        let modules: Vec<Box<dyn Module>> = vec![
            Box::new(StubModule {
                name: "temporal",
                deps: &["foundation", "visibility"],
            }),
            Box::new(StubModule {
                name: "foundation",
                deps: &["remotestate"],
            }),
            Box::new(StubModule {
                name: "remotestate",
                deps: &[],
            }),
            Box::new(StubModule {
                name: "visibility",
                deps: &["foundation"],
            }),
        ];

        let sorted = topological_sort_modules(&modules).unwrap();

        let pos = |name: &str| sorted.iter().position(|n| n == name).unwrap();
        assert!(pos("remotestate") < pos("foundation"));
        assert!(pos("foundation") < pos("visibility"));
        assert!(pos("foundation") < pos("temporal"));
        assert!(pos("visibility") < pos("temporal"));
    }

    #[test]
    fn module_cycle_detected() {
        let modules: Vec<Box<dyn Module>> = vec![
            Box::new(StubModule {
                name: "a",
                deps: &["b"],
            }),
            Box::new(StubModule {
                name: "b",
                deps: &["a"],
            }),
        ];

        let err = topological_sort_modules(&modules).unwrap_err();
        assert!(
            matches!(err, IacError::DependencyResolution(ref msg) if msg.contains("cycle")),
            "expected cycle error, got {err:?}"
        );
    }

    #[test]
    fn external_dependencies_are_ignored() {
        // "foundation" depends on "remotestate" which is not in the set
        let modules: Vec<Box<dyn Module>> = vec![Box::new(StubModule {
            name: "foundation",
            deps: &["remotestate"],
        })];

        let sorted = topological_sort_modules(&modules).unwrap();
        assert_eq!(sorted, vec!["foundation"]);
    }

    #[test]
    fn empty_module_list_succeeds() {
        let modules: Vec<Box<dyn Module>> = vec![];
        let sorted = topological_sort_modules(&modules).unwrap();
        assert!(sorted.is_empty());
    }

    #[test]
    fn independent_modules_sorted_deterministically() {
        let modules: Vec<Box<dyn Module>> = vec![
            Box::new(StubModule {
                name: "zebra",
                deps: &[],
            }),
            Box::new(StubModule {
                name: "alpha",
                deps: &[],
            }),
            Box::new(StubModule {
                name: "middle",
                deps: &[],
            }),
        ];

        let sorted = topological_sort_modules(&modules).unwrap();
        // Independent modules should be in alphabetical order for determinism
        assert_eq!(sorted, vec!["alpha", "middle", "zebra"]);
    }
}
