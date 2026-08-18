//! Generic infrastructure lifecycle engine.
//!
//! Provides the [`Resource`] and [`Module`] trait abstractions, the plan/apply/
//! destroy [`Engine`], dependency ordering, and typed state
//! documents. Provider-agnostic — concrete resource types and module names
//! are defined by consumer crates.
//!
//! Deployment specializations implement this crate at two levels:
//!
//! - A [`Resource`] models one provisioned thing, such as a bucket, database,
//!   namespace, local directory, or compose service. It owns provider calls and
//!   translates provider responses into a persisted [`ResourceState`].
//! - A [`Module`] groups resources into an ordered deployment unit. Modules
//!   make a deployment selectable and express coarse dependencies between
//!   resource groups.
//!
//! The engine is deliberately unaware of AWS, Docker, Kubernetes, or local
//! filesystem details. Specializations pass provider clients and config through
//! [`ProvisionContext::set_extension`] and recover them in resources or module
//! assembly with [`ProvisionContext::extension`].

pub mod diff;
pub mod document;
pub mod engine;
pub mod error;
pub mod module;
pub mod resource_modes;
pub mod semantics;
pub mod types;
pub mod writeback;

pub use document::{
    ImageSource, ImageState, InfraState, InfraStateStore, RuntimeState, RuntimeStateStore,
    ServiceState,
};
pub use engine::{
    Engine, PlanOutcome, PlatformIssue, RefreshCoverage, RefreshStatus, StateSaver,
    verify_resources,
};
pub use error::IacError;
pub use module::{Module, ModuleContext};
pub use semantics::{
    ChangeSemantics, Citation, Confidence, DataEffect, Disruption, LifecycleOperation,
    ReplacementPolicy, Reversibility, SemanticsContext,
};
pub use types::{
    Change, ChangeKind, FieldDiff, InfraComposition, ModuleSelection, ResourceDiff,
    SelectionDirection, destructive_changes, expand_module_selection, plan_is_destructive,
};
pub use writeback::{WritebackError, write_config_values};

use std::{
    any::{Any, TypeId},
    collections::HashMap,
    fmt,
    sync::Arc,
    time::Duration,
};

use serde::{Deserialize, Serialize};

/// Opaque resource type identifier.
///
/// Provider crates define constants for their supported types.
/// The engine treats this as an opaque string for display, serialization,
/// and change reporting.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ResourceType(pub String);

impl fmt::Display for ResourceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl ResourceType {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
}

/// Logical identifier for a managed resource.
///
/// This identifier is the stable key in [`InfraState`]. It must not depend on
/// provider-assigned physical IDs that are unknown before creation. A custom
/// deployment should derive it from the deployment model, for example
/// `vpc/main`, `service/api`, or `namespace/default`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct ResourceId(pub String);

/// Persisted state for a single resource after creation or update.
///
/// Resource implementations are responsible for preserving enough data here to
/// make later `diff`, `update`, and `delete` calls deterministic. Put the
/// provider identifier in [`physical_id`](Self::physical_id), and put any
/// provider-specific shape needed for comparison in
/// [`properties`](Self::properties).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceState {
    pub resource_type: ResourceType,
    pub physical_id: String,
    pub properties: serde_json::Value,
    pub dependencies: Vec<ResourceId>,
    pub created_at: String,
    pub updated_at: String,
    /// Module that owns this resource.
    pub module: String,
}

/// Outcome of a [`Resource::describe`] call.
///
/// `describe` must distinguish "I queried the provider and the resource is
/// genuinely absent" from "I could not determine the resource's existence".
/// The engine treats these very differently:
///
/// - On [`Absent`](Self::Absent) the engine prunes the resource from persisted
///   state (refresh) or treats a pending delete as already done.
/// - On [`Unsupported`](Self::Unsupported) the engine **refuses to prune** and,
///   when deleting, drives [`Resource::delete`] from the persisted state rather
///   than skipping it. A resource whose `describe` cannot confirm absence is
///   therefore never silently orphaned.
///
/// Return [`Unsupported`](Self::Unsupported) from any path that yields "not
/// found" without a real provider query confirming absence — an
/// unimplemented/stub `describe`, or an early return because a prerequisite
/// (physical id, dependency) is missing from local state. Reserve
/// [`Absent`](Self::Absent) for a provider query that positively reported the
/// resource does not exist.
// `Present` carries `ResourceState` by value: every caller consumes the state
// immediately, and boxing it would ripple through every provider match. The
// size gap only crosses clippy's threshold in `--all-targets` builds, where a
// downstream dev-dependency's feature unification (`serde_json/preserve_order`)
// enlarges `serde_json::Value`.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum DescribeResult {
    /// The provider returned live state for the resource.
    Present(ResourceState),
    /// A provider query positively confirmed the resource does not exist.
    Absent,
    /// The resource's existence could not be determined (no describe support,
    /// or a prerequisite for the provider query was unavailable). Persisted
    /// state is authoritative; the engine must not prune on this outcome.
    Unsupported,
}

/// Marker extension registered in [`ProvisionContext`] during destroy operations.
///
/// Modules can inspect this via [`ModuleContext::extension`] to include
/// resources recoverable from persisted state, not just resources present in
/// current config.
#[derive(Debug, Clone, Copy)]
pub struct DestroyMode;

/// Reconstruct a live-deletable [`Resource`] from its recorded state — the
/// seam behind the fail-closed delete guard (Property 10).
///
/// A resource removed from the deployment definition is still in state and
/// still live, but no module realizes it any more, so the known managed set
/// cannot supply a `Resource` to delete it with. The platform registers a
/// recoverer in the [`ProvisionContext`] extension bag (beside its provider
/// handle); the delete passes consult it exactly where the guard would
/// otherwise refuse. Recovery must yield a resource whose `delete()` acts on
/// the live object; a recoverer that cannot claim the recorded type returns
/// `None` and the guard refuses as before — recovery widens what is
/// deletable, never what is droppable.
pub struct ResourceRecovery(
    #[allow(clippy::type_complexity)] // one seam, named once; an alias would just move the shape
    Box<dyn Fn(&ResourceState) -> Option<Box<dyn Resource>> + Send + Sync>,
);

impl ResourceRecovery {
    pub fn new(
        recover: impl Fn(&ResourceState) -> Option<Box<dyn Resource>> + Send + Sync + 'static,
    ) -> Self {
        Self(Box::new(recover))
    }

    /// Attempt to reconstruct the recorded resource. `None` means this
    /// recoverer does not claim the type — the caller stays fail-closed.
    pub fn recover(&self, state: &ResourceState) -> Option<Box<dyn Resource>> {
        (self.0)(state)
    }
}

impl std::fmt::Debug for ResourceRecovery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ResourceRecovery(..)")
    }
}

/// A change detected by the internal diff engine.
///
/// Used by the low-level engine; the higher-level API uses
/// [`types::Change`] (a flat struct) instead.
///
/// `details` is the field-level evidence behind an update/replace — the plan
/// renderer's `field: before → after` lines. A resource that detects a change
/// without holding the value pair reports a [`FieldDiff::observation`]
/// (named, valueless); a bare count with no evidence is what blinded the
/// compose phantom-drift hunt, so the evidence rides in the type.
#[derive(Debug, Clone)]
pub enum InternalChange {
    Create {
        resource_id: ResourceId,
        resource_type: ResourceType,
    },
    Update {
        resource_id: ResourceId,
        resource_type: ResourceType,
        details: Vec<FieldDiff>,
    },
    /// An immutable-field change that `update` cannot apply in place: the engine
    /// deletes the current resource and re-creates it. A resource returns this
    /// from [`Resource::diff`] when it detects a changed field it declares
    /// immutable. Destructive — the live resource (and its data) is replaced.
    Replace {
        resource_id: ResourceId,
        resource_type: ResourceType,
        details: Vec<FieldDiff>,
    },
    Delete {
        resource_id: ResourceId,
        resource_type: ResourceType,
    },
    NoChange {
        resource_id: ResourceId,
    },
}

impl InternalChange {
    pub fn resource_id(&self) -> &ResourceId {
        match self {
            InternalChange::Create { resource_id, .. }
            | InternalChange::Update { resource_id, .. }
            | InternalChange::Replace { resource_id, .. }
            | InternalChange::Delete { resource_id, .. }
            | InternalChange::NoChange { resource_id } => resource_id,
        }
    }
}

/// Context passed to resource operations (create/update/delete/describe).
///
/// Carries project identity, tags, persisted state for dependency lookups,
/// progress reporters, and a typed extension map for provider-specific handles.
///
/// Deployment code should register long-lived provider handles here before
/// calling the engine. Resources should read those handles with
/// [`extension`](Self::extension) instead of storing global clients or adding
/// provider-specific fields to this framework crate.
pub struct ProvisionContext {
    pub project_name: String,
    pub tags: HashMap<String, String>,
    pub state: document::InfraState,
    apply_progress: Option<Arc<ApplyProgressReporter>>,
    complete_progress: Option<Arc<CompleteProgressReporter>>,
    failed_progress: Option<Arc<FailedProgressReporter>>,
    wait_progress: Option<Arc<WaitProgressReporter>>,
    note_progress: Option<Arc<NoteProgressReporter>>,
    extensions: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

// Manual impl: the progress reporters are bare `dyn Fn` values and the
// extension bag is type-erased; neither can carry `Debug`.
impl std::fmt::Debug for ProvisionContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProvisionContext")
            .field("project_name", &self.project_name)
            .field("tags", &self.tags)
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

type ApplyProgressReporter = dyn Fn(&str, &ResourceId, &ResourceType, usize, usize) + Send + Sync;
type CompleteProgressReporter = dyn Fn(&str, &ResourceId, &ResourceType, Duration) + Send + Sync;
type FailedProgressReporter =
    dyn Fn(&str, &ResourceId, &ResourceType, Duration, &IacError) + Send + Sync;
type WaitProgressReporter =
    dyn Fn(&ResourceId, &ResourceType, &str, Duration, Duration) + Send + Sync;
type NoteProgressReporter = dyn Fn(&ResourceId, &ResourceType, &str) + Send + Sync;

impl ProvisionContext {
    pub fn new(project_name: impl Into<String>, tags: HashMap<String, String>) -> Self {
        Self {
            project_name: project_name.into(),
            tags,
            state: document::InfraState::default(),
            apply_progress: None,
            complete_progress: None,
            failed_progress: None,
            wait_progress: None,
            note_progress: None,
            extensions: HashMap::new(),
        }
    }

    /// Retrieve a typed extension by type.
    pub fn extension<T: 'static + Send + Sync>(&self) -> Option<&T> {
        self.extensions
            .get(&TypeId::of::<T>())
            .and_then(|boxed| boxed.downcast_ref::<T>())
    }

    /// Register a typed extension.
    pub fn set_extension<T: 'static + Send + Sync>(&mut self, value: T) {
        self.extensions.insert(TypeId::of::<T>(), Box::new(value));
    }

    /// Remove a typed extension, returning whether one was present.
    pub fn remove_extension<T: 'static + Send + Sync>(&mut self) -> bool {
        self.extensions.remove(&TypeId::of::<T>()).is_some()
    }

    /// Access the extensions map (used by `ModuleContext`).
    pub fn extensions(&self) -> &HashMap<TypeId, Box<dyn Any + Send + Sync>> {
        &self.extensions
    }

    pub fn set_apply_progress<F>(&mut self, reporter: F)
    where
        F: Fn(&str, &ResourceId, &ResourceType, usize, usize) + Send + Sync + 'static,
    {
        self.apply_progress = Some(Arc::new(reporter));
    }

    pub fn emit_apply_progress(
        &self,
        action: &str,
        resource_id: &ResourceId,
        resource_type: &ResourceType,
        current: usize,
        total: usize,
    ) {
        if let Some(reporter) = &self.apply_progress {
            reporter(action, resource_id, resource_type, current, total);
        }
    }

    pub fn set_complete_progress<F>(&mut self, reporter: F)
    where
        F: Fn(&str, &ResourceId, &ResourceType, Duration) + Send + Sync + 'static,
    {
        self.complete_progress = Some(Arc::new(reporter));
    }

    pub fn emit_complete_progress(
        &self,
        action: &str,
        resource_id: &ResourceId,
        resource_type: &ResourceType,
        elapsed: Duration,
    ) {
        if let Some(reporter) = &self.complete_progress {
            reporter(action, resource_id, resource_type, elapsed);
        }
    }

    pub fn set_failed_progress<F>(&mut self, reporter: F)
    where
        F: Fn(&str, &ResourceId, &ResourceType, Duration, &IacError) + Send + Sync + 'static,
    {
        self.failed_progress = Some(Arc::new(reporter));
    }

    pub fn emit_failed_progress(
        &self,
        action: &str,
        resource_id: &ResourceId,
        resource_type: &ResourceType,
        elapsed: Duration,
        err: &IacError,
    ) {
        if let Some(reporter) = &self.failed_progress {
            reporter(action, resource_id, resource_type, elapsed, err);
        }
    }

    pub fn set_wait_progress<F>(&mut self, reporter: F)
    where
        F: Fn(&ResourceId, &ResourceType, &str, Duration, Duration) + Send + Sync + 'static,
    {
        self.wait_progress = Some(Arc::new(reporter));
    }

    pub fn emit_wait_progress(
        &self,
        resource_id: &ResourceId,
        resource_type: &ResourceType,
        phase: &str,
        elapsed: Duration,
        timeout: Duration,
    ) {
        if let Some(reporter) = &self.wait_progress {
            reporter(resource_id, resource_type, phase, elapsed, timeout);
        }
    }

    pub fn set_note_progress<F>(&mut self, reporter: F)
    where
        F: Fn(&ResourceId, &ResourceType, &str) + Send + Sync + 'static,
    {
        self.note_progress = Some(Arc::new(reporter));
    }

    pub fn emit_note_progress(
        &self,
        resource_id: &ResourceId,
        resource_type: &ResourceType,
        message: &str,
    ) {
        if let Some(reporter) = &self.note_progress {
            reporter(resource_id, resource_type, message);
        }
    }

    /// Look up a dependent resource's persisted state by ID.
    pub fn get_resource_state(&self, id: &ResourceId) -> Result<&ResourceState, error::IacError> {
        self.state.resources.get(id).ok_or_else(|| {
            error::IacError::StateNotFound(format!("resource {:?} not found in state", id.0))
        })
    }

    /// Build the tag set for a resource, merging user tags with auto-generated ones.
    pub fn resource_tags(&self, resource_name: &str) -> HashMap<String, String> {
        let mut tags = self.tags.clone();
        tags.insert("Name".into(), resource_name.into());
        tags.insert("Project".into(), self.project_name.clone());
        tags.insert("ManagedBy".into(), "tokeira-cli".into());
        tags
    }
}

impl Default for ProvisionContext {
    fn default() -> Self {
        Self::new("", HashMap::new())
    }
}

/// A managed resource with a full lifecycle (create/update/delete/describe).
///
/// Provider crates implement this trait for concrete resource types.
/// The engine is provider-agnostic — it operates solely through this trait.
///
/// Implementation guidance for custom deployments:
///
/// - [`resource_id`](Self::resource_id) must be stable across runs.
/// - [`dependencies`](Self::dependencies) should reference resource IDs, not
///   module names.
/// - [`describe`](Self::describe) returns a [`DescribeResult`]: `Present` with
///   live provider state, `Absent` when a provider query confirms the resource
///   does not exist, or `Unsupported` when existence cannot be determined (stub
///   describe, or a missing prerequisite for the query). Never return `Absent`
///   without a provider query confirming absence — the engine prunes state on
///   `Absent`, so a wrong `Absent` silently orphans a live resource.
/// - [`diff`](Self::diff) is a local comparison against current state. It should
///   avoid side effects and should explain update reasons in the `details`
///   string when possible.
#[async_trait::async_trait]
pub trait Resource: Send + Sync {
    /// Opaque resource type identifier.
    fn resource_type(&self) -> ResourceType;

    /// Validate the complete desired resource before an engine sees it.
    ///
    /// Definition frontends construct resources from authored kinds without
    /// duplicating provider validation. The definition realization boundary
    /// calls this method immediately after construction; lifecycle methods are
    /// never invoked for an invalid resource.
    fn validate_input(&self) -> Result<(), String> {
        Ok(())
    }

    /// Complete output names this resource may record for author references.
    fn declared_outputs(&self) -> &'static [&'static str] {
        &[]
    }

    /// Canonical desired representation used for definition evidence.
    ///
    /// Ordinary module-built resources need not participate in definition
    /// snapshots. Resources introduced through a frontend kind override this
    /// with the desired properties whose movement must be explainable across
    /// retained definition revisions.
    fn desired_manifest(&self) -> serde_json::Value {
        serde_json::Value::Null
    }

    /// Logical resource identifier.
    fn resource_id(&self) -> ResourceId;

    /// IDs of resources this resource depends on.
    fn dependencies(&self) -> Vec<ResourceId>;

    /// Whether this kind's `describe` performs a live provider query when its
    /// prerequisites are present. A kind whose `describe` is a stub — it can
    /// only ever answer [`DescribeResult::Unsupported`] — declares `false`,
    /// and definition verification refuses to admit it: updating a resource
    /// presupposes describing it, so a plan over such a kind could never
    /// confirm live state. Defaulted `true` because every current in-tree
    /// kind queries its provider; a new kind stubbing `describe` MUST
    /// override this alongside the stub.
    fn describes(&self) -> bool {
        true
    }

    /// Module that owns this resource.
    fn module(&self) -> &str;

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, error::IacError>;
    async fn update(
        &self,
        current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<ResourceState, error::IacError>;
    async fn delete(
        &self,
        current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<(), error::IacError>;
    async fn describe(&self, ctx: &ProvisionContext) -> Result<DescribeResult, error::IacError>;
    fn diff(&self, current: &ResourceState, ctx: &ProvisionContext) -> InternalChange;

    /// Declare what this change does to the running resource.
    ///
    /// MUST be pure and total: no I/O, no provider call, no panic, a value
    /// for every input — the engine calls this during change computation,
    /// inside the plan path's tight loop. Deliberately no default: capturing
    /// the properties the deployment engine acts on is the resource + kind
    /// author's responsibility, discharged at authoring time — the compiler
    /// asks alongside the lifecycle itself. Claims cite their sources
    /// (`EngineFact` / `ProviderGuarantee` carry citations structurally);
    /// a single claim nobody can establish is declared `Unknown` per-field,
    /// with intent.
    fn change_semantics(&self, ctx: &SemanticsContext<'_>) -> ChangeSemantics;

    /// A short operator-facing noun for what this resource is — "service",
    /// "DNS zone", "state directory". Consumers compose it into prose
    /// ("the *grafana* service"); `None` falls back to identifiers. Keep it
    /// lowercase unless the noun is a proper name.
    fn display_kind(&self) -> Option<&'static str> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remove_extension_removes_registered_destroy_mode() {
        let mut ctx = ProvisionContext::default();
        ctx.set_extension(DestroyMode);
        assert!(ctx.extension::<DestroyMode>().is_some());
        assert!(ctx.remove_extension::<DestroyMode>());
        assert!(ctx.extension::<DestroyMode>().is_none());
        assert!(!ctx.remove_extension::<DestroyMode>());
    }
}
