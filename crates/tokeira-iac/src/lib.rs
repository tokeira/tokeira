//! Generic infrastructure lifecycle engine.
//!
//! Provides the [`Resource`] and [`Module`] trait abstractions, the plan/apply/
//! destroy [`Engine`](engine::Engine), dependency ordering, and typed state
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
pub mod types;

pub use document::{
    ImageSource, ImageState, InfraState, InfraStateStore, RuntimeState, RuntimeStateStore,
    ServiceState,
};
pub use engine::{Engine, StateSaver};
pub use error::IacError;
pub use module::{Module, ModuleContext};
pub use types::{Change, ChangeKind, FieldDiff, InfraComposition, ModuleSelection, ResourceDiff};

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
/// `vpc/main`, `compose/tokeirad`, or `namespace/default`.
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

/// A change detected by the internal diff engine.
///
/// Used by the low-level engine; the orchestrator-facing API uses
/// [`types::Change`] (a flat struct) instead.
#[derive(Debug, Clone)]
pub enum InternalChange {
    Create {
        resource_id: ResourceId,
        resource_type: ResourceType,
    },
    Update {
        resource_id: ResourceId,
        resource_type: ResourceType,
        details: String,
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
    wait_progress: Option<Arc<WaitProgressReporter>>,
    note_progress: Option<Arc<NoteProgressReporter>>,
    extensions: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

type ApplyProgressReporter = dyn Fn(&str, &ResourceId, &ResourceType, usize, usize) + Send + Sync;
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
/// - [`describe`](Self::describe) should return the live provider state when it
///   can be read cheaply and safely; return `Ok(None)` only when the resource is
///   absent.
/// - [`diff`](Self::diff) is a local comparison against current state. It should
///   avoid side effects and should explain update reasons in the `details`
///   string when possible.
#[async_trait::async_trait]
pub trait Resource: Send + Sync {
    /// Opaque resource type identifier.
    fn resource_type(&self) -> ResourceType;

    /// Logical resource identifier.
    fn resource_id(&self) -> ResourceId;

    /// IDs of resources this resource depends on.
    fn dependencies(&self) -> Vec<ResourceId>;

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
    async fn describe(
        &self,
        ctx: &ProvisionContext,
    ) -> Result<Option<ResourceState>, error::IacError>;
    fn diff(&self, current: &ResourceState, ctx: &ProvisionContext) -> InternalChange;
}
