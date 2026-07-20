//! Stateless infrastructure lifecycle engine.
//!
//! [`Engine`] coordinates plan/apply/destroy operations over a set of
//! [`Resource`] objects. It is stateless — the caller owns persistence. The
//! engine reads and mutates state
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
//! create/update/delete operation. This gives the caller incremental
//! crash-safety without coupling the engine to a specific store type.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    pin::Pin,
    time::Instant,
};

use tracing::{info, warn};

use crate::{
    DescribeResult, InternalChange, ProvisionContext, Resource, ResourceId,
    error::IacError,
    module::ModuleContext,
    types::{Change, ChangeKind, FieldDiff, InfraComposition},
};

/// Callback invoked after each mutating operation so the caller can
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
/// The caller manages state persistence. This engine computes plans
/// and applies changes using the `known` vs `desired` resource model to
/// ensure removed resources are properly deleted through their `Resource`
/// implementation rather than silently dropped from state.
#[derive(Debug)]
pub struct Engine;

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}

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
        validate_composition(composition, ctx)?;
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
        let refreshed = refresh_state(known, desired, ctx, None).await?;
        ctx.state = refreshed.state;
        Ok(compute_changes(desired, &ctx.state, ctx))
    }

    /// Plan restricted to the provided modules.
    pub async fn plan_for_modules(
        &self,
        composition: &InfraComposition,
        ctx: &mut ProvisionContext,
    ) -> Result<Vec<Change>, IacError> {
        validate_composition(composition, ctx)?;
        let desired = collect_resources_from(&composition.desired_modules, ctx)?;
        let known = collect_resources_from(&composition.known_modules, ctx)?;
        let desired_refs: Vec<&dyn Resource> = desired.iter().map(|r| r.as_ref()).collect();
        let known_refs: Vec<&dyn Resource> = known.iter().map(|r| r.as_ref()).collect();
        let refreshed = refresh_state(&known_refs, &desired_refs, ctx, None).await?;
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
        validate_composition(composition, ctx)?;
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
        let refreshed = refresh_state(known, desired, ctx, saver).await?;
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
        validate_composition(composition, ctx)?;
        let desired = collect_resources_from(&composition.desired_modules, ctx)?;
        let known = collect_resources_from(&composition.known_modules, ctx)?;
        let desired_refs: Vec<&dyn Resource> = desired.iter().map(|r| r.as_ref()).collect();
        let known_refs: Vec<&dyn Resource> = known.iter().map(|r| r.as_ref()).collect();
        let refreshed = refresh_state(&known_refs, &desired_refs, ctx, saver).await?;
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
        validate_composition(composition, ctx)?;
        let known = collect_resources_from(&composition.known_modules, ctx)?;
        let known_refs: Vec<&dyn Resource> = known.iter().map(|r| r.as_ref()).collect();
        self.destroy_known(&known_refs, ctx, saver).await
    }

    /// Compute the destroy change set without calling provider delete methods.
    pub async fn plan_destroy(
        &self,
        composition: &InfraComposition,
        ctx: &mut ProvisionContext,
    ) -> Result<Vec<Change>, IacError> {
        validate_composition(composition, ctx)?;
        let known = collect_resources_from(&composition.known_modules, ctx)?;
        let known_refs: Vec<&dyn Resource> = known.iter().map(|r| r.as_ref()).collect();
        let desired: [&dyn Resource; 0] = [];
        let refreshed = refresh_state(&known_refs, &desired, ctx, None).await?;
        ctx.state = refreshed.state;
        Ok(compute_changes(&desired, &ctx.state, ctx))
    }

    /// Compute the destroy change set restricted to the active modules.
    pub async fn plan_destroy_for_modules(
        &self,
        composition: &InfraComposition,
        ctx: &mut ProvisionContext,
    ) -> Result<Vec<Change>, IacError> {
        let changes = self.plan_destroy(composition, ctx).await?;
        let active: Vec<&str> = composition
            .active_modules
            .iter()
            .map(|s| s.as_str())
            .collect();
        Ok(filter_changes_by_modules(&changes, &ctx.state, &active))
    }

    /// Destroy using a known-managed resource set.
    pub async fn destroy_known(
        &self,
        known: &[&dyn Resource],
        ctx: &mut ProvisionContext,
        saver: Option<&StateSaver>,
    ) -> Result<Vec<Change>, IacError> {
        let desired: [&dyn Resource; 0] = [];
        let refreshed = refresh_state(known, &desired, ctx, saver).await?;
        ctx.state = refreshed.state;
        let changes = compute_changes(&desired, &ctx.state, ctx);
        destroy_changes(known, ctx, changes, saver).await
    }

    /// Delete exactly the resources named in `ids`, over the current
    /// `ctx.state`, leaving every other resource untouched.
    ///
    /// This is the delete-only primitive used by definition-driven rollback
    /// (Proposal 002): the superseded binary B removes the resources it created
    /// (`keys(S_B) − keys(S_A)`) before the binding re-pins to A. It runs the
    /// shared fail-closed, reverse-dependency-ordered delete path — an id in
    /// `ids` but absent from `known` errors ([`IacError::UnknownResourceDelete`],
    /// Property 10) rather than orphaning the live resource, and an id absent
    /// from state is skipped (idempotent). Unlike
    /// [`destroy_known`](Self::destroy_known), it does **not** refresh or
    /// reconcile the full known set — only the named ids.
    pub async fn destroy_selected(
        &self,
        known: &[&dyn Resource],
        ids: &HashSet<ResourceId>,
        ctx: &mut ProvisionContext,
        saver: Option<&StateSaver>,
    ) -> Result<Vec<Change>, IacError> {
        let changes: Vec<Change> = ids
            .iter()
            .filter_map(|rid| {
                ctx.state.resources.get(rid).map(|rs| Change {
                    kind: ChangeKind::Delete,
                    resource_type: rs.resource_type.0.clone(),
                    module: rs.module.clone(),
                    resource: rid.0.clone(),
                    details: Vec::new(),
                })
            })
            .collect();
        destroy_changes(known, ctx, changes, saver).await
    }

    /// Destroy restricted to the active modules in the composition.
    pub async fn destroy_for_modules(
        &self,
        composition: &InfraComposition,
        ctx: &mut ProvisionContext,
        saver: Option<&StateSaver>,
    ) -> Result<Vec<Change>, IacError> {
        validate_composition(composition, ctx)?;
        let known = collect_resources_from(&composition.known_modules, ctx)?;
        let known_refs: Vec<&dyn Resource> = known.iter().map(|r| r.as_ref()).collect();
        let desired: [&dyn Resource; 0] = [];
        let refreshed = refresh_state(&known_refs, &desired, ctx, saver).await?;
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
    /// `describe` could not determine the resource's existence
    /// ([`DescribeResult::Unsupported`]). State is left untouched — never
    /// pruned on an unconfirmed describe.
    Unknown,
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
/// Validate a composition before any Delta (Property 12): unique module ids,
/// every desired module present in `known`, no module dependency cycle, and
/// unique resource ids across the known set. A duplicate resource id would let
/// the engine's resource map silently route a delete to the wrong resource.
fn validate_composition(
    composition: &InfraComposition,
    ctx: &ProvisionContext,
) -> Result<(), IacError> {
    let mut known_module_names = HashSet::new();
    for m in &composition.known_modules {
        if !known_module_names.insert(m.name().to_string()) {
            return Err(IacError::CompositionInvalid(format!(
                "duplicate module id '{}'",
                m.name()
            )));
        }
    }
    for m in &composition.desired_modules {
        if !known_module_names.contains(m.name()) {
            return Err(IacError::CompositionInvalid(format!(
                "desired module '{}' is not in the known managed set",
                m.name()
            )));
        }
    }
    // Collecting also validates the module dependency graph (cycle → error).
    let resources = collect_resources_from(&composition.known_modules, ctx)?;
    let mut seen = HashSet::new();
    for r in &resources {
        let id = r.resource_id();
        if !seen.insert(id.clone()) {
            return Err(IacError::CompositionInvalid(format!(
                "duplicate resource id '{}'",
                id.0
            )));
        }
    }
    Ok(())
}

async fn refresh_state(
    known: &[&dyn Resource],
    desired: &[&dyn Resource],
    ctx: &mut ProvisionContext,
    saver: Option<&StateSaver>,
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
            DescribeResult::Present(live_state) => {
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
            DescribeResult::Absent => {
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
                    if let Some(save) = saver {
                        save(&ctx.state).await?;
                    }
                    RefreshStatus::ManagedMissing
                };
                status_by_id.insert(resource_id, status);
            }
            DescribeResult::Unsupported => {
                // Existence could not be confirmed — never prune on an
                // unconfirmed describe. Persisted state is authoritative, so
                // leave both `refreshed` and `ctx.state` untouched for this id.
                status_by_id.insert(resource_id, RefreshStatus::Unknown);
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
                let started = Instant::now();
                let rs = match resource.create(ctx).await {
                    Ok(rs) => {
                        ctx.emit_complete_progress(
                            "create",
                            rid,
                            &resource.resource_type(),
                            started.elapsed(),
                        );
                        rs
                    }
                    Err(err) => {
                        ctx.emit_failed_progress(
                            "create",
                            rid,
                            &resource.resource_type(),
                            started.elapsed(),
                            &err,
                        );
                        return Err(err);
                    }
                };
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
                let current = current.clone();
                let started = Instant::now();
                let rs = match resource.update(&current, ctx).await {
                    Ok(rs) => {
                        ctx.emit_complete_progress(
                            "update",
                            rid,
                            &resource.resource_type(),
                            started.elapsed(),
                        );
                        rs
                    }
                    Err(err) => {
                        ctx.emit_failed_progress(
                            "update",
                            rid,
                            &resource.resource_type(),
                            started.elapsed(),
                            &err,
                        );
                        return Err(err);
                    }
                };
                ctx.state.resources.insert(rid.clone(), rs);
                if let Some(save) = saver {
                    save(&ctx.state).await?;
                }
            }
            Some(c) if c.kind == ChangeKind::Replace => {
                // Immutable-field change: delete the current resource, then
                // re-create it from config — ordered at the resource's forward
                // topo slot, so its dependencies already exist and its dependents
                // (later in the order) reconcile against the new resource.
                let resource = resource_map.get(rid).ok_or_else(|| {
                    IacError::DependencyResolution(format!(
                        "resource {rid:?} not found in known set"
                    ))
                })?;
                let current = ctx
                    .state
                    .resources
                    .get(rid)
                    .ok_or_else(|| {
                        IacError::StateNotFound(format!(
                            "resource {rid:?} missing from state during replace"
                        ))
                    })?
                    .clone();
                operation_index += 1;
                ctx.emit_apply_progress(
                    "replace",
                    rid,
                    &resource.resource_type(),
                    operation_index,
                    total_operations,
                );
                info!(?rid, "replacing resource (delete + recreate)");
                let started = Instant::now();
                if let Err(err) = resource.delete(&current, ctx).await {
                    ctx.emit_failed_progress(
                        "replace",
                        rid,
                        &resource.resource_type(),
                        started.elapsed(),
                        &err,
                    );
                    return Err(err);
                }
                // Old resource is gone; reflect that before the recreate so an
                // interrupted replace re-creates rather than trying to update a
                // resource that no longer exists.
                ctx.state.resources.remove(rid);
                if let Some(save) = saver {
                    save(&ctx.state).await?;
                }
                let rs = match resource.create(ctx).await {
                    Ok(rs) => {
                        ctx.emit_complete_progress(
                            "replace",
                            rid,
                            &resource.resource_type(),
                            started.elapsed(),
                        );
                        rs
                    }
                    Err(err) => {
                        ctx.emit_failed_progress(
                            "replace",
                            rid,
                            &resource.resource_type(),
                            started.elapsed(),
                            &err,
                        );
                        return Err(err);
                    }
                };
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
            let Some(current) = ctx.state.resources.get(rid).cloned() else {
                continue;
            };
            // Fail-closed: a Delete must have a known `Resource` to delete the live
            // object with. Dropping the state entry without deleting would orphan
            // the live resource (Property 10).
            let Some(resource) = resource_map.get(rid) else {
                return Err(IacError::UnknownResourceDelete {
                    resource_id: rid.0.clone(),
                });
            };
            operation_index += 1;
            ctx.emit_apply_progress(
                "delete",
                rid,
                &current.resource_type,
                operation_index,
                total_operations,
            );
            info!(?rid, "deleting resource");
            let started = Instant::now();
            if let Err(err) = resource.delete(&current, ctx).await {
                ctx.emit_failed_progress(
                    "delete",
                    rid,
                    &current.resource_type,
                    started.elapsed(),
                    &err,
                );
                return Err(err);
            }
            ctx.emit_complete_progress("delete", rid, &current.resource_type, started.elapsed());
            ctx.state.resources.remove(rid);
            if let Some(save) = saver {
                save(&ctx.state).await?;
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
        let Some(current) = ctx.state.resources.get(rid).cloned() else {
            continue;
        };
        // Fail-closed: a Delete must have a known `Resource` to delete the live
        // object with (Property 10).
        let Some(resource) = resource_map.get(rid) else {
            return Err(IacError::UnknownResourceDelete {
                resource_id: rid.0.clone(),
            });
        };
        // Re-describe to get live state before deleting
        match resource.describe(ctx).await.map_err(|err| {
            IacError::Other(anyhow::anyhow!(
                "failed to describe resource '{}' during destroy: {err}",
                rid.0
            ))
        })? {
            DescribeResult::Present(live_state) => {
                operation_index += 1;
                ctx.emit_apply_progress(
                    "delete",
                    rid,
                    &live_state.resource_type,
                    operation_index,
                    total_operations,
                );
                info!(?rid, "destroying resource");
                let started = Instant::now();
                if let Err(err) = resource.delete(&live_state, ctx).await {
                    ctx.emit_failed_progress(
                        "delete",
                        rid,
                        &live_state.resource_type,
                        started.elapsed(),
                        &err,
                    );
                    return Err(err);
                }
                ctx.emit_complete_progress(
                    "delete",
                    rid,
                    &live_state.resource_type,
                    started.elapsed(),
                );
            }
            DescribeResult::Absent => {
                warn!(
                    ?rid,
                    "resource already absent from provider, pruning from state"
                );
                ctx.emit_note_progress(
                    rid,
                    &current.resource_type,
                    &format!("pruned absent: {}", rid.0),
                );
            }
            DescribeResult::Unsupported => {
                // `describe` cannot confirm presence, so absence must not be
                // assumed — drive the delete from persisted state rather than
                // pruning blind. The resource's `delete` must treat a provider
                // not-found as success for this to remain idempotent.
                operation_index += 1;
                ctx.emit_apply_progress(
                    "delete",
                    rid,
                    &current.resource_type,
                    operation_index,
                    total_operations,
                );
                info!(
                    ?rid,
                    "destroying resource from persisted state (describe unsupported)"
                );
                let started = Instant::now();
                if let Err(err) = resource.delete(&current, ctx).await {
                    ctx.emit_failed_progress(
                        "delete",
                        rid,
                        &current.resource_type,
                        started.elapsed(),
                        &err,
                    );
                    return Err(err);
                }
                ctx.emit_complete_progress(
                    "delete",
                    rid,
                    &current.resource_type,
                    started.elapsed(),
                );
            }
        }
        ctx.state.resources.remove(rid);
        if let Some(save) = saver {
            save(&ctx.state).await?;
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
        InternalChange::Replace {
            resource_id,
            resource_type: rt,
            details,
        } => Change {
            kind: ChangeKind::Replace,
            resource_type: rt.0.clone(),
            module: module.to_string(),
            resource: resource_id.0.clone(),
            details: vec![FieldDiff {
                field: "immutable".to_string(),
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
    use proptest::prelude::*;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

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

    #[derive(Debug)]
    struct StubResource {
        id: ResourceId,
        module: &'static str,
        describe_state: Option<crate::ResourceState>,
        /// When true, `describe` returns [`DescribeResult::Unsupported`]
        /// regardless of `describe_state` — models a resource whose existence
        /// cannot be confirmed from the provider.
        describe_unsupported: bool,
        /// When true, `diff` returns [`InternalChange::Replace`] — models an
        /// immutable-field change requiring delete-then-recreate.
        diff_replace: bool,
        create_fails: bool,
        delete_counter: Option<Arc<AtomicUsize>>,
        delete_capture: Option<Arc<Mutex<Vec<String>>>>,
    }

    impl StubResource {
        fn missing(id: &str, module: &'static str) -> Self {
            Self {
                id: ResourceId(id.to_string()),
                module,
                describe_state: None,
                describe_unsupported: false,
                diff_replace: false,
                create_fails: false,
                delete_counter: None,
                delete_capture: None,
            }
        }

        fn live(id: &str, module: &'static str) -> Self {
            Self {
                id: ResourceId(id.to_string()),
                module,
                describe_state: Some(stub_state(id, module)),
                describe_unsupported: false,
                diff_replace: false,
                create_fails: false,
                delete_counter: None,
                delete_capture: None,
            }
        }

        fn create_fails(id: &str, module: &'static str) -> Self {
            Self {
                id: ResourceId(id.to_string()),
                module,
                describe_state: None,
                describe_unsupported: false,
                diff_replace: false,
                create_fails: true,
                delete_counter: None,
                delete_capture: None,
            }
        }

        /// `describe` cannot confirm existence ([`DescribeResult::Unsupported`]).
        fn with_describe_unsupported(mut self) -> Self {
            self.describe_unsupported = true;
            self
        }

        /// `diff` reports an immutable-field change ([`InternalChange::Replace`]).
        fn with_replace(mut self) -> Self {
            self.diff_replace = true;
            self
        }

        fn with_delete_counter(mut self, counter: Arc<AtomicUsize>) -> Self {
            self.delete_counter = Some(counter);
            self
        }

        fn with_delete_capture(mut self, capture: Arc<Mutex<Vec<String>>>) -> Self {
            self.delete_capture = Some(capture);
            self
        }
    }

    #[async_trait::async_trait]
    impl Resource for StubResource {
        fn resource_type(&self) -> crate::ResourceType {
            crate::ResourceType::new("Stub")
        }

        fn resource_id(&self) -> ResourceId {
            self.id.clone()
        }

        fn dependencies(&self) -> Vec<ResourceId> {
            Vec::new()
        }

        fn module(&self) -> &str {
            self.module
        }

        async fn create(&self, _ctx: &ProvisionContext) -> Result<crate::ResourceState, IacError> {
            if self.create_fails {
                return Err(IacError::Other(anyhow::anyhow!("create failed")));
            }
            Ok(stub_state(&self.id.0, self.module))
        }

        async fn update(
            &self,
            _current: &crate::ResourceState,
            _ctx: &ProvisionContext,
        ) -> Result<crate::ResourceState, IacError> {
            Ok(stub_state(&self.id.0, self.module))
        }

        async fn delete(
            &self,
            current: &crate::ResourceState,
            _ctx: &ProvisionContext,
        ) -> Result<(), IacError> {
            if let Some(counter) = &self.delete_counter {
                counter.fetch_add(1, Ordering::Relaxed);
            }
            if let Some(capture) = &self.delete_capture {
                capture
                    .lock()
                    .expect("delete capture mutex")
                    .push(current.physical_id.clone());
            }
            Ok(())
        }

        async fn describe(&self, _ctx: &ProvisionContext) -> Result<DescribeResult, IacError> {
            if self.describe_unsupported {
                return Ok(DescribeResult::Unsupported);
            }
            Ok(match self.describe_state.clone() {
                Some(state) => DescribeResult::Present(state),
                None => DescribeResult::Absent,
            })
        }

        fn diff(&self, _current: &crate::ResourceState, _ctx: &ProvisionContext) -> InternalChange {
            if self.diff_replace {
                return InternalChange::Replace {
                    resource_id: self.resource_id(),
                    resource_type: self.resource_type(),
                    details: "immutable field changed".to_string(),
                };
            }
            InternalChange::NoChange {
                resource_id: self.resource_id(),
            }
        }
    }

    fn stub_state(id: &str, module: &str) -> crate::ResourceState {
        crate::ResourceState {
            resource_type: crate::ResourceType::new("Stub"),
            physical_id: id.to_string(),
            properties: serde_json::json!({}),
            dependencies: Vec::new(),
            created_at: "now".to_string(),
            updated_at: "now".to_string(),
            module: module.to_string(),
        }
    }

    fn boxed_resources(resources: Vec<StubResource>) -> Vec<Box<dyn Resource>> {
        resources
            .into_iter()
            .map(|resource| Box::new(resource) as Box<dyn Resource>)
            .collect()
    }

    fn refs(resources: &[Box<dyn Resource>]) -> Vec<&dyn Resource> {
        resources.iter().map(|resource| resource.as_ref()).collect()
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

    // Property 10: a Delete never silently orphans a live resource.
    #[tokio::test]
    async fn fail_closed_delete_refuses_unknown_resource() {
        // A resource recorded in state but absent from the known managed set: the
        // engine has no `Resource` to delete the live object with, so it must
        // refuse — never drop the state entry while leaving the live one orphaned.
        let engine = Engine::new();
        let mut ctx = ProvisionContext::default();
        ctx.state
            .resources
            .insert(ResourceId("orphan".into()), stub_state("orphan", "ghost"));

        let none: Vec<Box<dyn Resource>> = boxed_resources(vec![]);
        let err = engine
            .apply_with_known(&refs(&none), &refs(&none), &mut ctx, None)
            .await
            .unwrap_err();

        assert!(
            matches!(err, IacError::UnknownResourceDelete { ref resource_id } if resource_id == "orphan"),
            "expected fail-closed delete error, got {err:?}"
        );
        assert!(
            ctx.state
                .resources
                .contains_key(&ResourceId("orphan".into())),
            "fail-closed must NOT drop the orphan from state"
        );
    }

    /// A test module that yields stub resources for the given ids.
    #[derive(Debug)]
    struct ResourceModule {
        name: &'static str,
        ids: &'static [&'static str],
    }

    impl Module for ResourceModule {
        fn name(&self) -> &str {
            self.name
        }
        fn dependencies(&self) -> &[&str] {
            &[]
        }
        fn resources(
            &self,
            _ctx: &crate::ModuleContext,
        ) -> Result<Vec<Box<dyn Resource>>, IacError> {
            Ok(self
                .ids
                .iter()
                .map(|id| Box::new(StubResource::missing(id, self.name)) as Box<dyn Resource>)
                .collect())
        }
    }

    // Property 12: composition validation refuses before any Delta.
    #[tokio::test]
    async fn composition_refuses_duplicate_resource_id() {
        let engine = Engine::new();
        let mut ctx = ProvisionContext::default();
        let composition = InfraComposition {
            known_modules: vec![
                Box::new(ResourceModule {
                    name: "a",
                    ids: &["dup"],
                }),
                Box::new(ResourceModule {
                    name: "b",
                    ids: &["dup"],
                }),
            ],
            desired_modules: vec![
                Box::new(ResourceModule {
                    name: "a",
                    ids: &["dup"],
                }),
                Box::new(ResourceModule {
                    name: "b",
                    ids: &["dup"],
                }),
            ],
            active_modules: vec!["a".into(), "b".into()],
        };
        let err = engine.plan(&composition, &mut ctx).await.unwrap_err();
        assert!(
            matches!(err, IacError::CompositionInvalid(ref m) if m.contains("duplicate resource id")),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn composition_refuses_desired_module_not_in_known() {
        let engine = Engine::new();
        let mut ctx = ProvisionContext::default();
        let composition = InfraComposition {
            known_modules: vec![Box::new(StubModule {
                name: "a",
                deps: &[],
            })],
            desired_modules: vec![Box::new(StubModule {
                name: "ghost",
                deps: &[],
            })],
            active_modules: vec!["ghost".into()],
        };
        let err = engine.plan(&composition, &mut ctx).await.unwrap_err();
        assert!(
            matches!(err, IacError::CompositionInvalid(ref m) if m.contains("not in the known managed set")),
            "got {err:?}"
        );
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

    #[tokio::test]
    async fn refresh_prunes_save_once_per_missing_managed_resource() {
        let engine = Engine::new();
        let mut ctx = ProvisionContext::default();
        let first = ResourceId("first".to_string());
        let second = ResourceId("second".to_string());
        ctx.state
            .resources
            .insert(first.clone(), stub_state(&first.0, "module"));
        ctx.state
            .resources
            .insert(second.clone(), stub_state(&second.0, "module"));

        let resources: Vec<Box<dyn Resource>> = vec![
            Box::new(StubResource::missing(&first.0, "module")),
            Box::new(StubResource::missing(&second.0, "module")),
        ];
        let known: Vec<&dyn Resource> =
            resources.iter().map(|resource| resource.as_ref()).collect();
        let save_count = Arc::new(AtomicUsize::new(0));
        let save_count_for_saver = Arc::clone(&save_count);
        let saver: StateSaver = Box::new(move |_state| {
            save_count_for_saver.fetch_add(1, Ordering::Relaxed);
            Box::pin(async { Ok(()) })
        });

        let changes = engine
            .destroy_known(&known, &mut ctx, Some(&saver))
            .await
            .unwrap();

        assert!(changes.is_empty());
        assert_eq!(save_count.load(Ordering::Relaxed), 2);
        assert!(ctx.state.resources.is_empty());
    }

    proptest! {
        #[test]
        fn state_saver_invocation_count(create_count in 0usize..20, prune_count in 0usize..20) {
            let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
            runtime.block_on(async move {
                let engine = Engine::new();
                let mut ctx = ProvisionContext::default();

                let desired_resources = (0..create_count)
                    .map(|idx| StubResource::missing(&format!("create-{idx}"), "desired"))
                    .collect::<Vec<_>>();
                let prune_resources = (0..prune_count)
                    .map(|idx| {
                        let id = ResourceId(format!("prune-{idx}"));
                        ctx.state.resources.insert(id.clone(), stub_state(&id.0, "stale"));
                        StubResource::missing(&id.0, "stale")
                    })
                    .collect::<Vec<_>>();

                let desired = boxed_resources(desired_resources);
                let mut known = desired
                    .iter()
                    .map(|resource| {
                        StubResource::missing(&resource.resource_id().0, "desired")
                    })
                    .collect::<Vec<_>>();
                known.extend(prune_resources);
                let known = boxed_resources(known);
                let desired_refs = refs(&desired);
                let known_refs = refs(&known);

                let save_count = Arc::new(AtomicUsize::new(0));
                let save_count_for_saver = Arc::clone(&save_count);
                let saver: StateSaver = Box::new(move |_state| {
                    save_count_for_saver.fetch_add(1, Ordering::Relaxed);
                    Box::pin(async { Ok(()) })
                });

                engine
                    .apply_with_known(&desired_refs, &known_refs, &mut ctx, Some(&saver))
                    .await
                    .expect("apply succeeds");

                prop_assert_eq!(
                    save_count.load(Ordering::Relaxed),
                    create_count + prune_count
                );
                Ok(())
            })?;
        }

        #[test]
        fn state_saver_error_aborts_engine(total in 1usize..20, fail_at in 1usize..20) {
            let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
            runtime.block_on(async move {
                let fail_at = fail_at.min(total);
                let engine = Engine::new();
                let mut ctx = ProvisionContext::default();
                let resources = boxed_resources(
                    (0..total)
                        .map(|idx| StubResource::missing(&format!("resource-{idx}"), "module"))
                        .collect()
                );
                let resource_refs = refs(&resources);
                let save_count = Arc::new(AtomicUsize::new(0));
                let save_count_for_saver = Arc::clone(&save_count);
                let saver: StateSaver = Box::new(move |_state| {
                    let invocation = save_count_for_saver.fetch_add(1, Ordering::Relaxed) + 1;
                    Box::pin(async move {
                        if invocation == fail_at {
                            Err(IacError::Other(anyhow::anyhow!("save failed")))
                        } else {
                            Ok(())
                        }
                    })
                });

                let result = engine
                    .apply_with_known(&resource_refs, &resource_refs, &mut ctx, Some(&saver))
                    .await;

                prop_assert!(result.is_err());
                prop_assert!(ctx.state.resources.len() <= fail_at);
                prop_assert_eq!(save_count.load(Ordering::Relaxed), fail_at);
                Ok(())
            })?;
        }

        #[test]
        fn module_scoped_delete_filtering(
            active_flags in prop::collection::vec(any::<bool>(), 1..8),
            delete_flags in prop::collection::vec(any::<bool>(), 1..8)
        ) {
            let modules = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"];
            let len = active_flags.len().min(delete_flags.len());
            let active = modules
                .iter()
                .zip(active_flags.iter())
                .filter_map(|(module, active)| active.then_some(*module))
                .collect::<Vec<_>>();
            let mut state = crate::InfraState::default();
            let mut changes = Vec::new();

            for idx in 0..len {
                let id = ResourceId(format!("resource-{idx}"));
                let module = modules[idx];
                state.resources.insert(id.clone(), stub_state(&id.0, module));
                changes.push(Change {
                    kind: if delete_flags[idx] {
                        ChangeKind::Delete
                    } else {
                        ChangeKind::NoChange
                    },
                    resource_type: "Stub".to_string(),
                    module: module.to_string(),
                    resource: id.0,
                    details: Vec::new(),
                });
            }

            let filtered = filter_changes_by_modules(&changes, &state, &active);

            for change in &filtered {
                if change.kind == ChangeKind::Delete {
                    prop_assert!(active.iter().any(|module| *module == change.module));
                }
            }
            for change in changes.iter().filter(|change| change.kind != ChangeKind::Delete) {
                prop_assert!(filtered
                    .iter()
                    .any(|filtered| filtered.resource == change.resource && filtered.kind == change.kind));
            }
        }

        #[test]
        fn describe_before_delete_count(total in 1usize..20, missing_count in 0usize..20) {
            let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
            runtime.block_on(async move {
                let missing_count = missing_count.min(total);
                let engine = Engine::new();
                let mut ctx = ProvisionContext::default();
                let delete_count = Arc::new(AtomicUsize::new(0));
                let resources = boxed_resources(
                    (0..total)
                        .map(|idx| {
                            let id = format!("resource-{idx}");
                            ctx.state.resources.insert(ResourceId(id.clone()), stub_state(&id, "module"));
                            let resource = if idx < missing_count {
                                StubResource::missing(&id, "module")
                            } else {
                                StubResource::live(&id, "module")
                            };
                            resource.with_delete_counter(Arc::clone(&delete_count))
                        })
                        .collect()
                );
                let resource_refs = refs(&resources);

                engine
                    .destroy_known(&resource_refs, &mut ctx, None)
                    .await
                    .expect("destroy succeeds");

                prop_assert_eq!(delete_count.load(Ordering::Relaxed), total - missing_count);
                Ok(())
            })?;
        }
    }

    #[tokio::test]
    async fn describe_before_delete_uses_live_state() {
        let engine = Engine::new();
        let mut ctx = ProvisionContext::default();
        let id = ResourceId("resource".to_string());
        ctx.state
            .resources
            .insert(id.clone(), stub_state(&id.0, "module"));
        let captured = Arc::new(Mutex::new(Vec::new()));
        let mut live_state = stub_state(&id.0, "module");
        live_state.physical_id = "live-physical".to_string();
        let resource = StubResource {
            id: id.clone(),
            module: "module",
            describe_state: Some(live_state),
            describe_unsupported: false,
            diff_replace: false,
            create_fails: false,
            delete_counter: None,
            delete_capture: None,
        }
        .with_delete_capture(Arc::clone(&captured));
        let resources = boxed_resources(vec![resource]);
        let resource_refs = refs(&resources);

        engine
            .destroy_known(&resource_refs, &mut ctx, None)
            .await
            .unwrap();

        assert_eq!(
            captured.lock().expect("delete capture mutex").as_slice(),
            ["live-physical"]
        );
    }

    #[tokio::test]
    async fn unsupported_describe_deletes_from_persisted_state() {
        // A resource whose `describe` cannot confirm existence
        // (`DescribeResult::Unsupported`) must still be deleted from persisted
        // state during destroy — never silently pruned (Property 10 / Req 10.3).
        let engine = Engine::new();
        let mut ctx = ProvisionContext::default();
        let id = ResourceId("resource".to_string());
        let mut persisted = stub_state(&id.0, "module");
        persisted.physical_id = "persisted-physical".to_string();
        ctx.state.resources.insert(id.clone(), persisted);
        let captured = Arc::new(Mutex::new(Vec::new()));
        let resource = StubResource::live(&id.0, "module")
            .with_describe_unsupported()
            .with_delete_capture(Arc::clone(&captured));
        let resources = boxed_resources(vec![resource]);
        let resource_refs = refs(&resources);

        engine
            .destroy_known(&resource_refs, &mut ctx, None)
            .await
            .unwrap();

        // delete() was driven from the persisted state, not skipped.
        assert_eq!(
            captured.lock().expect("delete capture mutex").as_slice(),
            ["persisted-physical"]
        );
        assert!(
            !ctx.state.resources.contains_key(&id),
            "state entry removed after successful delete"
        );
    }

    #[tokio::test]
    async fn unsupported_describe_is_not_pruned_on_refresh() {
        // refresh_state must NOT prune a managed resource whose describe is
        // Unsupported — only a provider-confirmed Absent prunes. A `missing`
        // (Absent) resource in the same run is pruned, proving the contrast.
        let mut ctx = ProvisionContext::default();
        let keep = ResourceId("keep".to_string());
        let prune = ResourceId("prune".to_string());
        ctx.state
            .resources
            .insert(keep.clone(), stub_state(&keep.0, "module"));
        ctx.state
            .resources
            .insert(prune.clone(), stub_state(&prune.0, "module"));
        let resources = boxed_resources(vec![
            StubResource::live(&keep.0, "module").with_describe_unsupported(),
            StubResource::missing(&prune.0, "module"),
        ]);
        let resource_refs = refs(&resources);

        // desired empty → both are managed-but-not-desired.
        let report = refresh_state(&resource_refs, &[], &mut ctx, None)
            .await
            .expect("refresh succeeds");

        assert!(
            ctx.state.resources.contains_key(&keep),
            "unsupported describe must not prune managed state"
        );
        assert!(report.state.resources.contains_key(&keep));
        assert!(
            !ctx.state.resources.contains_key(&prune),
            "absent describe prunes managed state"
        );
    }

    #[tokio::test]
    async fn replace_deletes_then_recreates() {
        // 11.3a: an immutable-field change (diff -> Replace) deletes the current
        // resource and re-creates it at the resource's forward-topo slot.
        let engine = Engine::new();
        let mut ctx = ProvisionContext::default();
        let id = ResourceId("resource".to_string());
        ctx.state
            .resources
            .insert(id.clone(), stub_state(&id.0, "module"));
        let delete_count = Arc::new(AtomicUsize::new(0));
        let resource = StubResource::live(&id.0, "module")
            .with_replace()
            .with_delete_counter(Arc::clone(&delete_count));
        let resources = boxed_resources(vec![resource]);
        let resource_refs = refs(&resources);

        let changes = engine
            .apply_with_known(&resource_refs, &resource_refs, &mut ctx, None)
            .await
            .expect("apply succeeds");

        assert!(
            changes.iter().any(|c| c.kind == ChangeKind::Replace),
            "plan classifies the change as Replace"
        );
        assert_eq!(
            delete_count.load(Ordering::Relaxed),
            1,
            "replace deletes the current resource once"
        );
        assert!(
            ctx.state.resources.contains_key(&id),
            "replace recreates the resource"
        );
    }

    #[tokio::test]
    async fn destroy_selected_deletes_only_named_ids() {
        // 11.3c: delete only the named ids over the current state; others kept.
        let engine = Engine::new();
        let mut ctx = ProvisionContext::default();
        let keep = ResourceId("keep".to_string());
        let drop_id = ResourceId("drop".to_string());
        ctx.state
            .resources
            .insert(keep.clone(), stub_state(&keep.0, "m"));
        ctx.state
            .resources
            .insert(drop_id.clone(), stub_state(&drop_id.0, "m"));
        let resources = boxed_resources(vec![
            StubResource::live(&keep.0, "m"),
            StubResource::live(&drop_id.0, "m"),
        ]);
        let resource_refs = refs(&resources);
        let ids: std::collections::HashSet<ResourceId> = [drop_id.clone()].into_iter().collect();

        engine
            .destroy_selected(&resource_refs, &ids, &mut ctx, None)
            .await
            .expect("destroy_selected succeeds");

        assert!(ctx.state.resources.contains_key(&keep), "unnamed id kept");
        assert!(
            !ctx.state.resources.contains_key(&drop_id),
            "named id deleted"
        );
    }

    #[tokio::test]
    async fn destroy_selected_fails_closed_on_unknown_id() {
        // 11.3c: an id to delete that is absent from `known` refuses to drop the
        // state entry rather than orphaning the live resource (Property 10).
        let engine = Engine::new();
        let mut ctx = ProvisionContext::default();
        let id = ResourceId("orphan".to_string());
        ctx.state
            .resources
            .insert(id.clone(), stub_state(&id.0, "m"));
        let resources: Vec<Box<dyn Resource>> = Vec::new();
        let resource_refs = refs(&resources);
        let ids: std::collections::HashSet<ResourceId> = [id.clone()].into_iter().collect();

        let err = engine
            .destroy_selected(&resource_refs, &ids, &mut ctx, None)
            .await
            .expect_err("unknown id refuses");
        assert!(matches!(err, IacError::UnknownResourceDelete { .. }));
    }

    #[tokio::test]
    async fn progress_callbacks_report_success_and_failure() {
        let engine = Engine::new();
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut success_ctx = ProvisionContext::default();
        let success_events = Arc::clone(&events);
        success_ctx.set_apply_progress(move |action, id, _, _, _| {
            success_events
                .lock()
                .expect("event mutex")
                .push(format!("start:{action}:{}", id.0));
        });
        let success_events = Arc::clone(&events);
        success_ctx.set_complete_progress(move |action, id, _, elapsed| {
            assert!(elapsed >= std::time::Duration::ZERO);
            success_events
                .lock()
                .expect("event mutex")
                .push(format!("complete:{action}:{}", id.0));
        });
        let resources = boxed_resources(vec![StubResource::missing("resource", "module")]);
        let resource_refs = refs(&resources);

        engine
            .apply_with_known(&resource_refs, &resource_refs, &mut success_ctx, None)
            .await
            .unwrap();

        assert_eq!(
            events.lock().expect("event mutex").as_slice(),
            ["start:create:resource", "complete:create:resource"]
        );

        let mut failure_ctx = ProvisionContext::default();
        let failures = Arc::new(Mutex::new(Vec::new()));
        let failure_events = Arc::clone(&failures);
        failure_ctx.set_apply_progress(move |action, id, _, _, _| {
            failure_events
                .lock()
                .expect("failure mutex")
                .push(format!("start:{action}:{}", id.0));
        });
        let failure_events = Arc::clone(&failures);
        failure_ctx.set_failed_progress(move |action, id, _, elapsed, err| {
            assert!(elapsed >= std::time::Duration::ZERO);
            failure_events
                .lock()
                .expect("failure mutex")
                .push(format!("failed:{action}:{}:{err}", id.0));
        });
        let failing = boxed_resources(vec![StubResource::create_fails("failing", "module")]);
        let failing_refs = refs(&failing);

        let result = engine
            .apply_with_known(&failing_refs, &failing_refs, &mut failure_ctx, None)
            .await;

        assert!(result.is_err());
        let failure_log = failures.lock().expect("failure mutex");
        assert_eq!(failure_log[0], "start:create:failing");
        assert!(failure_log[1].starts_with("failed:create:failing:"));
    }
}
