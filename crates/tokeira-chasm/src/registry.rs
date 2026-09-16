//! The immutable component, typed-handler and search-attribute registry.
//!
//! Task codecs belong to the task's defining library. Registration builds typed
//! decode/call/encode closures, so dispatch needs no reflection. Task FQNs and ids
//! are globally unique; ids below 1024 are explicit built-in identities because
//! existing durable activity outboxes already use them. Sealing ends the built-in
//! registration phase permanently, protecting both those ids and library names.
//!
//! A [`Library`] groups the components a domain registers; the [`Registry`] indexes
//! every registered component three ways — by fully-qualified name (FQN), by a
//! `u32` archetype/type id derived from the FQN, and by Rust [`TypeId`]
//! (Requirement 8.1). An **archetype** is the FQN of a root component and its
//! **archetype id** is the type id of that FQN.
//!
//! Two invariants this module upholds:
//! - Archetype id `0` ([`LEGACY_WORKFLOW_ARCHETYPE_ID`]) is **reserved for legacy
//!   Workflow** and is never assigned to a CHASM archetype, so the workflow engine
//!   and CHASM never collide on identity (Requirement 8.2).
//! - The registry is built **once** via a [`RegistryBuilder`] and is **immutable**
//!   thereafter — no runtime mutation, consistent with the no-reflection rule
//!   (Requirement 8.3).
//!
//! ## Type-id derivation (tokeira-owned)
//!
//! Upstream derives the id from `farm.Fingerprint32(fqn)` (`registry.go @
//! v1.31.0`). tokeira reproduces the *contract* — a deterministic `u32` from the
//! FQN with `0` reserved — but owns the hash: the pure crate's dependency set is
//! confined to value/wire types (Requirement 1.1), so it cannot pull in a
//! FarmHash crate, and these ids are internal to tokeira (never on the wire to an
//! SDK), so byte-compatibility with Temporal's fingerprint is not required (same
//! reasoning as `ComponentRef`'s tokeira-owned encoding). [`archetype_id_for_fqn`]
//! therefore uses FNV-1a/32, which is deterministic and dependency-free. The id `0`
//! is remapped to a fixed non-zero sentinel so the reservation always holds; any
//! resulting collision is caught by [`RegistryBuilder`], which rejects two FQNs
//! that map to the same id.

use std::{
    any::TypeId,
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use prost::Message;
use serde::{Deserialize, Serialize};

use crate::{
    Context, EngineComponent, LifecycleState, MutableContext, PureTaskHandler,
    RESERVED_SYSTEM_FIELDS, RESERVED_TASK_ID_LIMIT, RootComponent, ScheduledTask,
    SearchAttributeProvider, SearchAttributes, SideEffectTaskHandler, Task, TaskKind, TaskOutcome,
    TaskValidity, VisibilityContributor, VisibilitySnapshot, component::Component,
    error::ChasmError, task_type_id_for_fqn,
};

/// The archetype id reserved for the legacy Workflow engine. CHASM never assigns
/// it to one of its archetypes (Requirement 8.2).
pub const LEGACY_WORKFLOW_ARCHETYPE_ID: u32 = 0;

/// FNV-1a/32 offset basis.
const FNV_OFFSET_BASIS: u32 = 0x811c_9dc5;
/// FNV-1a/32 prime.
const FNV_PRIME: u32 = 0x0100_0193;
/// Sentinel the reserved id `0` is remapped to, so a real FQN never receives the
/// legacy-workflow id. Chosen at the top of the `u32` range to keep it clear of
/// the dense low-id space typical FQN hashes land in.
const ZERO_REMAP_SENTINEL: u32 = u32::MAX;

/// Derive a CHASM archetype/type id from a fully-qualified name.
///
/// Deterministic FNV-1a/32 over the FQN's bytes, with the reserved
/// [`LEGACY_WORKFLOW_ARCHETYPE_ID`] (`0`) remapped to a fixed non-zero sentinel so
/// no CHASM component can ever be assigned the legacy-workflow id (Requirement
/// 8.2). See the module doc for why the hash is tokeira-owned rather than FarmHash.
pub fn archetype_id_for_fqn(fqn: &str) -> u32 {
    let mut hash = FNV_OFFSET_BASIS;
    for byte in fqn.as_bytes() {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    if hash == LEGACY_WORKFLOW_ARCHETYPE_ID {
        ZERO_REMAP_SENTINEL
    } else {
        hash
    }
}

/// A registered component's index entry: its FQN, derived archetype id, Rust
/// [`TypeId`], and the name of the [`Library`] that registered it.
#[derive(Debug, Clone)]
pub struct ComponentEntry {
    /// The component's fully-qualified name (its [`Component::FQN`]).
    pub fqn: &'static str,
    /// The derived archetype/type id (never [`LEGACY_WORKFLOW_ARCHETYPE_ID`]).
    pub archetype_id: u32,
    /// The Rust [`TypeId`] of the component type.
    pub type_id: TypeId,
    /// The name of the library that registered the component.
    pub library: &'static str,
    root: Option<RootFunctions>,
}

// Equality remains registration identity, independent of monomorphized function
// addresses (compiler merging/splitting makes address equality unreliable).
impl PartialEq for ComponentEntry {
    fn eq(&self, other: &Self) -> bool {
        (
            self.fqn,
            self.archetype_id,
            self.type_id,
            self.library,
            self.root.is_some(),
        ) == (
            other.fqn,
            other.archetype_id,
            other.type_id,
            other.library,
            other.root.is_some(),
        )
    }
}
impl Eq for ComponentEntry {}

type SnapshotFn = fn(&[u8]) -> Result<Option<VisibilitySnapshot>, ChasmError>;
type SearchFn = fn(&[u8]) -> Result<SearchAttributes, ChasmError>;
type LifecycleFn = fn(&[u8], &dyn Context) -> Result<LifecycleState, ChasmError>;

#[derive(Debug, Clone, Copy)]
struct RootFunctions {
    visibility: SnapshotFn,
    search: SearchFn,
    lifecycle: LifecycleFn,
}

/// A typed handler's immutable dispatch entry. Identity is global across tasks;
/// lookup additionally requires its owning component, preventing cross-root calls.
#[derive(Debug)]
pub struct TaskEntry {
    /// Durable task name, owned by its defining library.
    pub fqn: &'static str,
    /// Derived or explicitly reserved durable id.
    pub task_type_id: u32,
    /// Discipline checked against the handler at registration.
    pub kind: TaskKind,
    /// Archetype whose root data the handler materializes.
    pub component_type_id: u32,
    /// Library that registered the handler.
    pub library: &'static str,
    pub(crate) erased: ErasedTaskHandler,
}

type ValidateFn =
    dyn Fn(&[u8], &[u8], &dyn Context) -> Result<TaskValidity, ChasmError> + Send + Sync;
type ExecuteFn =
    dyn Fn(&[u8], &[u8], &mut dyn MutableContext) -> Result<Vec<u8>, ChasmError> + Send + Sync;
type OutcomeFn = dyn Fn(&[u8], &[u8], &TaskOutcome, &mut dyn MutableContext) -> Result<Vec<u8>, ChasmError>
    + Send
    + Sync;

pub(crate) enum ErasedTaskHandler {
    Pure {
        validate: Box<ValidateFn>,
        execute: Box<ExecuteFn>,
    },
    SideEffect {
        validate: Box<ValidateFn>,
        on_outcome: Box<OutcomeFn>,
    },
}

impl std::fmt::Debug for ErasedTaskHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Pure { .. } => "Pure",
            Self::SideEffect { .. } => "SideEffect",
        })
    }
}

/// Projection-independent search-attribute type. These variants mirror the
/// projection's types; the engine maps them without introducing a dependency here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SearchAttrKind {
    /// Exact string.
    Keyword,
    /// Collection of exact strings.
    KeywordList,
    /// Signed integer.
    Int,
    /// Boolean.
    Bool,
    /// Floating-point number.
    Double,
    /// Timestamp.
    Datetime,
    /// Full-text string.
    Text,
}

/// A component-owned search key declared at startup. Reserved system fields
/// cannot be declared; repeating an identical definition is harmless.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchAttributeDef {
    /// Stable key emitted by the component.
    pub name: &'static str,
    /// Required projection value type.
    pub kind: SearchAttrKind,
}

fn decode_component<C: EngineComponent>(bytes: &[u8]) -> Result<C, ChasmError> {
    C::Data::decode(bytes)
        .map(C::from_data)
        .map_err(|e| ChasmError::Validation(format!("decode component {}: {e}", C::FQN)))
}

fn erase_pure<H: PureTaskHandler>(handler: H) -> ErasedTaskHandler {
    let handler = Arc::new(handler);
    let validator = handler.clone();
    ErasedTaskHandler::Pure {
        validate: Box::new(move |data, task, ctx| {
            Ok(validator.validate(
                &decode_component::<H::Component>(data)?,
                &H::Task::decode(task)?,
                ctx,
            ))
        }),
        execute: Box::new(move |data, task, ctx| {
            let mut component = decode_component::<H::Component>(data)?;
            handler.execute(&mut component, &H::Task::decode(task)?, ctx)?;
            Ok(component.into_data().encode_to_vec())
        }),
    }
}

fn erase_side_effect<H: SideEffectTaskHandler>(handler: H) -> ErasedTaskHandler {
    let handler = Arc::new(handler);
    let validator = handler.clone();
    ErasedTaskHandler::SideEffect {
        validate: Box::new(move |data, task, ctx| {
            Ok(validator.validate(
                &decode_component::<H::Component>(data)?,
                &H::Task::decode(task)?,
                ctx,
            ))
        }),
        on_outcome: Box::new(move |data, task, outcome, ctx| {
            let mut component = decode_component::<H::Component>(data)?;
            handler.on_outcome(&mut component, &H::Task::decode(task)?, outcome, ctx)?;
            Ok(component.into_data().encode_to_vec())
        }),
    }
}

/// A group of components a domain registers into the [`Registry`].
///
/// A library implementation names itself and registers its components against a
/// [`RegistryBuilder`]. This is the unit `tokeira-chasm-activity` (and every future
/// archetype crate) implements to declare "one ASM among many" (foundation §4).
pub trait Library {
    /// The library's stable name (e.g. `"activity"`).
    const NAME: &'static str;

    /// Register the library's components into `builder`.
    ///
    /// # Errors
    ///
    /// Propagates [`RegistryBuilder::register`] errors (FQN/id/type collisions).
    fn register(builder: &mut RegistryBuilder) -> Result<(), ChasmError>;
}

/// Builder for the immutable [`Registry`]. Components are registered once at
/// startup; [`build`](RegistryBuilder::build) freezes the index (Requirement 8.3).
#[derive(Debug, Default)]
pub struct RegistryBuilder {
    entries: Vec<ComponentEntry>,
    tasks: Vec<TaskEntry>,
    attributes: Vec<(usize, SearchAttributeDef)>,
    libraries: BTreeSet<&'static str>,
    sealed: Option<BTreeSet<&'static str>>,
}

impl RegistryBuilder {
    fn check_library(&self, library: &str) -> Result<(), ChasmError> {
        if self
            .sealed
            .as_ref()
            .is_some_and(|names| names.contains(library))
        {
            return Err(ChasmError::ReservedLibraryName {
                name: library.to_owned(),
            });
        }
        Ok(())
    }

    /// Freeze the currently registered library names as built-ins. Repeated
    /// calls are idempotent: extensions can never be promoted into built-ins.
    pub fn seal_built_ins(&mut self) -> &mut Self {
        self.sealed.get_or_insert_with(|| self.libraries.clone());
        self
    }

    /// Register a pure handler using its task FQN hash. Rejects missing components,
    /// wrong task disciplines, sealed library names and duplicate/reserved ids.
    pub fn register_pure_task<H: PureTaskHandler>(
        &mut self,
        library: &'static str,
        handler: H,
    ) -> Result<&mut Self, ChasmError> {
        self.register_task(
            library,
            H::Component::FQN,
            H::Task::FQN,
            H::Task::KIND,
            None,
            erase_pure(handler),
        )
    }

    /// Register an outcome handler using its task FQN hash, with the same identity
    /// checks as [`register_pure_task`](Self::register_pure_task).
    pub fn register_side_effect_task<H: SideEffectTaskHandler>(
        &mut self,
        library: &'static str,
        handler: H,
    ) -> Result<&mut Self, ChasmError> {
        self.register_task(
            library,
            H::Component::FQN,
            H::Task::FQN,
            H::Task::KIND,
            None,
            erase_side_effect(handler),
        )
    }

    /// Preserve a built-in pure task's explicit persisted id. The library must
    /// already be registered, sealing must not have occurred, and `id < 1024`.
    pub fn register_reserved_pure_task<H: PureTaskHandler>(
        &mut self,
        library: &'static str,
        id: u32,
        handler: H,
    ) -> Result<&mut Self, ChasmError> {
        self.register_task(
            library,
            H::Component::FQN,
            H::Task::FQN,
            H::Task::KIND,
            Some(id),
            erase_pure(handler),
        )
    }

    /// Preserve a built-in side-effect task's explicit persisted id. The same
    /// pre-seal and range restrictions as reserved pure registration apply.
    pub fn register_reserved_side_effect_task<H: SideEffectTaskHandler>(
        &mut self,
        library: &'static str,
        id: u32,
        handler: H,
    ) -> Result<&mut Self, ChasmError> {
        self.register_task(
            library,
            H::Component::FQN,
            H::Task::FQN,
            H::Task::KIND,
            Some(id),
            erase_side_effect(handler),
        )
    }

    fn register_task(
        &mut self,
        library: &'static str,
        component: &'static str,
        fqn: &'static str,
        kind: TaskKind,
        reserved_id: Option<u32>,
        erased: ErasedTaskHandler,
    ) -> Result<&mut Self, ChasmError> {
        self.check_library(library)?;
        let owner = self
            .entries
            .iter()
            .find(|entry| entry.fqn == component)
            .ok_or_else(|| {
                ChasmError::Validation(format!(
                    "component `{component}` must be registered before task `{fqn}`"
                ))
            })?;
        let expected_kind = match &erased {
            ErasedTaskHandler::Pure { .. } => TaskKind::Pure,
            ErasedTaskHandler::SideEffect { .. } => TaskKind::SideEffect,
        };
        if expected_kind != kind {
            return Err(ChasmError::Validation(format!(
                "task `{fqn}` is {kind:?}, but its handler is {expected_kind:?}"
            )));
        }
        let id = if let Some(id) = reserved_id {
            if self.sealed.is_some()
                || !self.libraries.contains(library)
                || id >= RESERVED_TASK_ID_LIMIT
            {
                return Err(ChasmError::ReservedLibraryName {
                    name: library.to_owned(),
                });
            }
            id
        } else {
            let id = task_type_id_for_fqn(fqn);
            if id < RESERVED_TASK_ID_LIMIT {
                return Err(ChasmError::TaskTypeCollision {
                    first: fqn.to_owned(),
                    second: "reserved built-in range".to_owned(),
                    id,
                });
            }
            id
        };
        if let Some(prior) = self
            .tasks
            .iter()
            .find(|entry| entry.fqn == fqn || entry.task_type_id == id)
        {
            return Err(ChasmError::TaskTypeCollision {
                first: prior.fqn.to_owned(),
                second: fqn.to_owned(),
                id,
            });
        }
        self.tasks.push(TaskEntry {
            fqn,
            task_type_id: id,
            kind,
            component_type_id: owner.archetype_id,
            library,
            erased,
        });
        self.libraries.insert(library);
        Ok(self)
    }

    /// Declare a registered component's custom search keys. Rejects system keys,
    /// sealed owners and conflicting types before adding any of the definitions.
    pub fn register_search_attributes<C: Component>(
        &mut self,
        defs: &[SearchAttributeDef],
    ) -> Result<&mut Self, ChasmError> {
        let index = self
            .entries
            .iter()
            .position(|entry| entry.fqn == C::FQN)
            .ok_or_else(|| {
                ChasmError::Validation(format!(
                    "component `{}` must be registered before search attributes",
                    C::FQN
                ))
            })?;
        self.check_library(self.entries[index].library)?;
        for (offset, def) in defs.iter().enumerate() {
            if RESERVED_SYSTEM_FIELDS.contains(&def.name) {
                return Err(ChasmError::ReservedSearchAttribute {
                    name: def.name.to_owned(),
                });
            }
            if self
                .attributes
                .iter()
                .filter(|(owner, _)| *owner == index)
                .map(|(_, def)| def)
                .chain(defs[..offset].iter())
                .any(|prior| prior.name == def.name && prior.kind != def.kind)
            {
                return Err(ChasmError::Validation(format!(
                    "search attribute `{}` has conflicting types on `{}`",
                    def.name,
                    C::FQN
                )));
            }
        }
        for def in defs {
            if !self
                .attributes
                .iter()
                .any(|(owner, prior)| *owner == index && prior == def)
            {
                self.attributes.push((index, *def));
            }
        }
        Ok(self)
    }
    /// Construct an empty builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register component `C` as belonging to `library`.
    ///
    /// Derives the archetype id from [`Component::FQN`] and records the Rust
    /// [`TypeId`].
    ///
    /// # Errors
    ///
    /// [`ChasmError::Internal`] if the FQN, the derived archetype id, or the Rust
    /// type is already registered — duplicate identities would make lookup
    /// ambiguous, and an archetype-id collision (including against the
    /// zero-remap sentinel) is rejected here rather than silently shadowing.
    pub fn register<C: Component>(
        &mut self,
        library: &'static str,
    ) -> Result<&mut Self, ChasmError> {
        self.register_component(library, C::FQN, TypeId::of::<C>())
    }

    /// Register a root and its byte-to-component visibility adapters. Generic
    /// transitions and repair can then rebuild derived views without depending on
    /// the library. Plain component registration remains available for children.
    pub fn register_root<C>(&mut self, library: &'static str) -> Result<&mut Self, ChasmError>
    where
        C: EngineComponent + RootComponent + VisibilityContributor + SearchAttributeProvider,
    {
        self.register::<C>(library)?;
        // Successful registration appended exactly this entry; adapters capture no
        // runtime state and use the same prost bridge as task handlers.
        if let Some(entry) = self.entries.last_mut() {
            entry.root = Some(RootFunctions {
                visibility: |bytes| Ok(decode_component::<C>(bytes)?.visibility_snapshot()),
                search: |bytes| Ok(decode_component::<C>(bytes)?.search_attributes()),
                lifecycle: |bytes, ctx| Ok(decode_component::<C>(bytes)?.lifecycle_state(ctx)),
            });
        }
        Ok(self)
    }

    fn register_component(
        &mut self,
        library: &'static str,
        fqn: &'static str,
        type_id: TypeId,
    ) -> Result<&mut Self, ChasmError> {
        self.check_library(library)?;
        let archetype_id = archetype_id_for_fqn(fqn);

        for entry in &self.entries {
            if entry.fqn == fqn {
                return Err(ChasmError::Internal(format!(
                    "registry: FQN `{fqn}` is already registered"
                )));
            }
            if entry.archetype_id == archetype_id {
                return Err(ChasmError::Internal(format!(
                    "registry: archetype id {archetype_id} collides between `{}` and `{fqn}`",
                    entry.fqn
                )));
            }
            if entry.type_id == type_id {
                return Err(ChasmError::Internal(format!(
                    "registry: Rust type for `{fqn}` is already registered as `{}`",
                    entry.fqn
                )));
            }
        }

        self.libraries.insert(library);
        self.entries.push(ComponentEntry {
            fqn,
            archetype_id,
            type_id,
            library,
            root: None,
        });
        Ok(self)
    }

    /// Register all of library `L`'s components.
    ///
    /// # Errors
    ///
    /// Propagates [`register`](RegistryBuilder::register) collision errors.
    pub fn register_library<L: Library>(&mut self) -> Result<&mut Self, ChasmError> {
        self.check_library(L::NAME)?;
        self.libraries.insert(L::NAME);
        L::register(self)?;
        Ok(self)
    }

    /// Freeze the builder into an immutable [`Registry`].
    pub fn build(self) -> Registry {
        let mut by_fqn = HashMap::with_capacity(self.entries.len());
        let mut by_archetype = HashMap::with_capacity(self.entries.len());
        let mut by_type = HashMap::with_capacity(self.entries.len());
        for (index, entry) in self.entries.iter().enumerate() {
            by_fqn.insert(entry.fqn, index);
            by_archetype.insert(entry.archetype_id, index);
            by_type.insert(entry.type_id, index);
        }
        let by_task = self
            .tasks
            .iter()
            .enumerate()
            .map(|(index, entry)| ((entry.component_type_id, entry.task_type_id), index))
            .collect();
        Registry {
            tasks: self.tasks,
            by_task,
            attributes: self.attributes,
            built_ins: self.sealed.unwrap_or(self.libraries),
            entries: self.entries,
            by_fqn,
            by_archetype,
            by_type,
        }
    }
}

/// The immutable component index. Built once via [`RegistryBuilder`] and never
/// mutated thereafter (Requirement 8.3). Lookups by FQN, archetype id, and Rust
/// [`TypeId`] all resolve to the same [`ComponentEntry`].
#[derive(Debug)]
pub struct Registry {
    tasks: Vec<TaskEntry>,
    by_task: HashMap<(u32, u32), usize>,
    attributes: Vec<(usize, SearchAttributeDef)>,
    built_ins: BTreeSet<&'static str>,
    entries: Vec<ComponentEntry>,
    by_fqn: HashMap<&'static str, usize>,
    by_archetype: HashMap<u32, usize>,
    by_type: HashMap<TypeId, usize>,
}

impl Registry {
    /// Resolve a task only for the root archetype that registered its handler.
    pub fn task_for_id(&self, component_type_id: u32, task_type_id: u32) -> Option<&TaskEntry> {
        self.by_task
            .get(&(component_type_id, task_type_id))
            .map(|&i| &self.tasks[i])
    }

    fn checked_task(
        &self,
        component_type_id: u32,
        task: &ScheduledTask,
    ) -> Result<&TaskEntry, ChasmError> {
        self.task_for_id(component_type_id, task.task_type_id)
            .filter(|entry| entry.kind == task.kind)
            .ok_or(ChasmError::UnknownTaskType {
                component_type_id,
                task_type_id: task.task_type_id,
            })
    }

    /// Decode and validate against the live root. Unknown or wrong-kind tasks and
    /// malformed component/task bytes return an error so transition close aborts.
    pub fn validate_task(
        &self,
        component_type_id: u32,
        data: &[u8],
        task: &ScheduledTask,
        ctx: &dyn Context,
    ) -> Result<TaskValidity, ChasmError> {
        match &self.checked_task(component_type_id, task)?.erased {
            ErasedTaskHandler::Pure { validate, .. }
            | ErasedTaskHandler::SideEffect { validate, .. } => validate(data, &task.payload, ctx),
        }
    }

    /// Execute a pure handler and encode the changed root. The caller owns
    /// validation, fencing and persistence; handler errors return no new data.
    pub fn execute_pure(
        &self,
        component_type_id: u32,
        data: &[u8],
        task: &ScheduledTask,
        ctx: &mut dyn MutableContext,
    ) -> Result<Vec<u8>, ChasmError> {
        match &self.checked_task(component_type_id, task)?.erased {
            ErasedTaskHandler::Pure { execute, .. } => execute(data, &task.payload, ctx),
            _ => Err(ChasmError::UnknownTaskType {
                component_type_id,
                task_type_id: task.task_type_id,
            }),
        }
    }

    /// Apply an external outcome through its pure transition handler. Calling a
    /// pure-task entry here is an unknown-handler error, never an implicit no-op.
    pub fn apply_outcome(
        &self,
        component_type_id: u32,
        data: &[u8],
        task: &ScheduledTask,
        outcome: &TaskOutcome,
        ctx: &mut dyn MutableContext,
    ) -> Result<Vec<u8>, ChasmError> {
        match &self.checked_task(component_type_id, task)?.erased {
            ErasedTaskHandler::SideEffect { on_outcome, .. } => {
                on_outcome(data, &task.payload, outcome, ctx)
            }
            _ => Err(ChasmError::UnknownTaskType {
                component_type_id,
                task_type_id: task.task_type_id,
            }),
        }
    }

    /// Read authoritative lifecycle from a registered root, independently of its
    /// optional visibility contribution. Generic handler transitions must not
    /// leave an invisible component running after it has closed.
    pub fn lifecycle_state(
        &self,
        component_type_id: u32,
        data: &[u8],
        ctx: &dyn Context,
    ) -> Result<LifecycleState, ChasmError> {
        let root = self
            .component_for_archetype(component_type_id)
            .and_then(|entry| entry.root)
            .ok_or_else(|| {
                ChasmError::Validation(format!(
                    "root adapters not registered for component {component_type_id}"
                ))
            })?;
        (root.lifecycle)(data, ctx)
    }

    /// Rebuild a registered root's visibility from durable data. A component
    /// registered without root adapters contributes no snapshot; malformed root
    /// data is an error rather than silently disappearing from projection.
    pub fn visibility_snapshot(
        &self,
        component_type_id: u32,
        data: &[u8],
    ) -> Result<Option<VisibilitySnapshot>, ChasmError> {
        match self
            .component_for_archetype(component_type_id)
            .and_then(|entry| entry.root)
        {
            Some(root) => (root.visibility)(data),
            None => Ok(None),
        }
    }

    /// Rebuild a root's search attributes. Components without root adapters
    /// contribute an empty set; decoding errors propagate to the transition.
    pub fn search_attributes(
        &self,
        component_type_id: u32,
        data: &[u8],
    ) -> Result<SearchAttributes, ChasmError> {
        match self
            .component_for_archetype(component_type_id)
            .and_then(|entry| entry.root)
        {
            Some(root) => (root.search)(data),
            None => Ok(Vec::new()),
        }
    }

    /// Custom definitions in registration order, paired with their owning roots.
    pub fn search_attribute_defs(
        &self,
    ) -> impl Iterator<Item = (&ComponentEntry, &SearchAttributeDef)> {
        self.attributes
            .iter()
            .map(|(owner, def)| (&self.entries[*owner], def))
    }

    /// Whether the library was registered during the built-in phase.
    pub fn is_built_in(&self, library: &str) -> bool {
        self.built_ins.contains(library)
    }
    /// Start building a registry.
    pub fn builder() -> RegistryBuilder {
        RegistryBuilder::new()
    }

    /// Number of registered components.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// True iff no components are registered.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The archetype id for `fqn`, if a component with that FQN is registered.
    pub fn archetype_id(&self, fqn: &str) -> Option<u32> {
        self.by_fqn.get(fqn).map(|&i| self.entries[i].archetype_id)
    }

    /// The entry for an archetype id, if registered. Always `None` for
    /// [`LEGACY_WORKFLOW_ARCHETYPE_ID`] (it is never a CHASM archetype).
    pub fn component_for_archetype(&self, id: u32) -> Option<&ComponentEntry> {
        self.by_archetype.get(&id).map(|&i| &self.entries[i])
    }

    /// The entry for a fully-qualified name, if registered.
    pub fn component_for_fqn(&self, fqn: &str) -> Option<&ComponentEntry> {
        self.by_fqn.get(fqn).map(|&i| &self.entries[i])
    }

    /// The entry for component type `C`, if registered.
    pub fn component_for_type<C: Component>(&self) -> Option<&ComponentEntry> {
        self.by_type
            .get(&TypeId::of::<C>())
            .map(|&i| &self.entries[i])
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        StartActivityTask, TaskOutcome,
        test_support::{Data, OutcomeHandler, Root, TestContext, Tick, TickHandler, scheduled},
    };
    use proptest::prelude::*;
    use std::sync::OnceLock;

    use super::*;
    use crate::{
        component::{Component, Lifecycle, LifecycleState},
        context::Context,
        field::FieldRegistry,
    };

    // Two minimal components for index tests. They implement the trait by hand
    // (the derive macro is exercised by its own crate's consumers); only FQN/Data
    // and the field registry matter here.
    struct CompA;
    struct CompB;

    impl Lifecycle for CompA {
        fn lifecycle_state(&self, _ctx: &dyn Context) -> LifecycleState {
            LifecycleState::Running
        }
    }
    impl Component for CompA {
        type Data = ();
        const FQN: &'static str = "test.alpha";
        fn fields(&self) -> FieldRegistry<'_> {
            FieldRegistry::new(&[])
        }
    }

    impl Lifecycle for CompB {
        fn lifecycle_state(&self, _ctx: &dyn Context) -> LifecycleState {
            LifecycleState::Running
        }
    }
    impl Component for CompB {
        type Data = ();
        const FQN: &'static str = "test.beta";
        fn fields(&self) -> FieldRegistry<'_> {
            FieldRegistry::new(&[])
        }
    }

    #[test]
    fn archetype_id_is_deterministic_and_never_zero() {
        let id = archetype_id_for_fqn("activity.activity");
        assert_eq!(id, archetype_id_for_fqn("activity.activity"));
        assert_ne!(id, LEGACY_WORKFLOW_ARCHETYPE_ID);
    }

    #[test]
    fn registry_indexes_three_ways() {
        let mut builder = Registry::builder();
        builder.register::<CompA>("test").expect("register A");
        builder.register::<CompB>("test").expect("register B");
        let registry = builder.build();

        assert_eq!(registry.len(), 2);
        let a_id = registry.archetype_id("test.alpha").expect("A id");
        assert_eq!(
            registry.component_for_archetype(a_id).map(|e| e.fqn),
            Some("test.alpha")
        );
        assert_eq!(
            registry
                .component_for_fqn("test.beta")
                .map(|e| e.archetype_id),
            registry.archetype_id("test.beta")
        );
        assert_eq!(
            registry.component_for_type::<CompA>().map(|e| e.fqn),
            Some("test.alpha")
        );
    }

    #[test]
    fn legacy_workflow_archetype_is_never_registered() {
        let registry = Registry::builder().build();
        assert!(
            registry
                .component_for_archetype(LEGACY_WORKFLOW_ARCHETYPE_ID)
                .is_none()
        );
    }

    #[test]
    fn duplicate_fqn_registration_is_rejected() {
        let mut builder = Registry::builder();
        builder.register::<CompA>("test").expect("first");
        // Re-registering the same type/FQN must fail.
        assert!(matches!(
            builder.register::<CompA>("test"),
            Err(ChasmError::Internal(_))
        ));
    }

    #[test]
    fn library_registration_path() {
        struct TestLib;
        impl Library for TestLib {
            const NAME: &'static str = "test";
            fn register(builder: &mut RegistryBuilder) -> Result<(), ChasmError> {
                builder.register::<CompA>(Self::NAME)?;
                builder.register::<CompB>(Self::NAME)?;
                Ok(())
            }
        }
        let mut builder = Registry::builder();
        builder.register_library::<TestLib>().expect("register lib");
        let registry = builder.build();
        assert_eq!(registry.len(), 2);
        assert_eq!(
            registry.component_for_fqn("test.alpha").map(|e| e.library),
            Some("test")
        );
    }

    #[test]
    fn typed_handlers_decode_validate_mutate_and_reencode() {
        let mut builder = Registry::builder();
        builder
            .register::<Root>("test")
            .unwrap()
            .register_pure_task("test", TickHandler)
            .unwrap()
            .register_side_effect_task("test", OutcomeHandler)
            .unwrap()
            .register_search_attributes::<Root>(&[SearchAttributeDef {
                name: "Generation",
                kind: SearchAttrKind::Int,
            }])
            .unwrap();
        let registry = builder.build();
        let component = archetype_id_for_fqn(Root::<0>::FQN);
        let data = Data { value: 3 }.encode_to_vec();
        let tick = scheduled(&Tick { delta: 4 }, 0);
        let mut ctx = TestContext::default();
        assert_eq!(
            registry
                .validate_task(component, &data, &tick, &ctx)
                .unwrap(),
            TaskValidity::Valid
        );
        let changed = registry
            .execute_pure(component, &data, &tick, &mut ctx)
            .unwrap();
        assert_eq!(Data::decode(changed.as_slice()).unwrap().value, 7);
        assert!(ctx.dirty);
        let effect = scheduled(
            &StartActivityTask {
                activity_id: "work".into(),
                heartbeat_nanos: 5,
                ..Default::default()
            },
            1,
        );
        assert_eq!(
            registry
                .validate_task(component, &data, &effect, &ctx)
                .unwrap(),
            TaskValidity::Valid
        );
        let changed = registry
            .apply_outcome(
                component,
                &data,
                &effect,
                &TaskOutcome::Completed {
                    payload: vec![1, 2],
                },
                &mut ctx,
            )
            .unwrap();
        assert_eq!(Data::decode(changed.as_slice()).unwrap().value, 10);
        assert_eq!(ctx.resolved.len(), 1);
        assert_eq!(
            registry
                .search_attribute_defs()
                .map(|(c, d)| (c.fqn, d.name))
                .collect::<Vec<_>>(),
            vec![(Root::<0>::FQN, "Generation")]
        );
        for error in [
            registry
                .execute_pure(component, &data, &effect, &mut ctx)
                .unwrap_err(),
            registry
                .apply_outcome(component, &data, &tick, &TaskOutcome::Terminated, &mut ctx)
                .unwrap_err(),
            registry
                .validate_task(component.wrapping_add(1), &data, &tick, &ctx)
                .unwrap_err(),
        ] {
            assert!(matches!(error, ChasmError::UnknownTaskType { .. }));
        }
        let mut wrong_kind = tick.clone();
        wrong_kind.kind = TaskKind::SideEffect;
        assert!(matches!(
            registry.validate_task(component, &data, &wrong_kind, &ctx),
            Err(ChasmError::UnknownTaskType { .. })
        ));
        let mut malformed = tick.clone();
        malformed.payload = vec![0xff];
        ctx.dirty = false;
        for error in [
            registry
                .validate_task(component, &[0xff], &tick, &ctx)
                .unwrap_err(),
            registry
                .execute_pure(component, &data, &malformed, &mut ctx)
                .unwrap_err(),
            registry
                .execute_pure(
                    component,
                    &data,
                    &scheduled(&Tick { delta: -1 }, 0),
                    &mut ctx,
                )
                .unwrap_err(),
        ] {
            assert!(matches!(error, ChasmError::Validation(_)));
        }
        assert!(!ctx.dirty);
        assert_eq!(
            registry
                .validate_task(component, &data, &scheduled(&Tick { delta: 101 }, 0), &ctx)
                .unwrap(),
            TaskValidity::Drop
        );
    }

    #[test]
    fn registration_guards_and_seal_cover_every_entry_point() {
        let mut builder = Registry::builder();
        let error = builder
            .register_pure_task("test", TickHandler)
            .unwrap_err()
            .to_string();
        assert!(error.contains(Root::<0>::FQN) && error.contains(Tick::FQN));
        assert!(builder.register_search_attributes::<Root>(&[]).is_err());
        builder.register::<Root>("built-in").unwrap();
        builder
            .register_reserved_pure_task("built-in", 1, TickHandler)
            .unwrap();
        builder
            .register_reserved_side_effect_task("built-in", 2, OutcomeHandler)
            .unwrap();
        assert!(matches!(
            builder.register_reserved_pure_task("absent", 3, TickHandler),
            Err(ChasmError::ReservedLibraryName { .. })
        ));
        assert!(matches!(
            builder.register_reserved_pure_task("built-in", 1024, TickHandler),
            Err(ChasmError::ReservedLibraryName { .. })
        ));
        let defs = [
            SearchAttributeDef {
                name: "Generation",
                kind: SearchAttrKind::Int,
            },
            SearchAttributeDef {
                name: "status",
                kind: SearchAttrKind::Text,
            },
        ];
        assert!(matches!(
            builder.register_search_attributes::<Root>(&defs),
            Err(ChasmError::ReservedSearchAttribute { .. })
        ));
        assert!(builder.attributes.is_empty());
        builder.seal_built_ins();
        assert!(matches!(
            builder.register::<Root<1>>("built-in"),
            Err(ChasmError::ReservedLibraryName { .. })
        ));
        assert!(matches!(
            builder.register_pure_task("built-in", TickHandler),
            Err(ChasmError::ReservedLibraryName { .. })
        ));
        assert!(matches!(
            builder.register_side_effect_task("built-in", OutcomeHandler),
            Err(ChasmError::ReservedLibraryName { .. })
        ));
        assert!(matches!(
            builder.register_search_attributes::<Root>(&[]),
            Err(ChasmError::ReservedLibraryName { .. })
        ));
        builder.register::<Root<1>>("extension").unwrap();
        assert!(matches!(
            builder.register_reserved_side_effect_task("extension", 3, OutcomeHandler),
            Err(ChasmError::ReservedLibraryName { .. })
        ));
        builder.seal_built_ins();
        let registry = builder.build();
        assert!(registry.is_built_in("built-in"));
        assert!(!registry.is_built_in("extension"));
        assert_eq!(
            registry
                .task_for_id(archetype_id_for_fqn(Root::<0>::FQN), 1)
                .unwrap()
                .fqn,
            Tick::FQN
        );
        assert_eq!(
            registry
                .task_for_id(archetype_id_for_fqn(Root::<0>::FQN), 2)
                .unwrap()
                .fqn,
            StartActivityTask::FQN
        );
    }

    fn model_hash(fqn: &str) -> u32 {
        let hash = fqn.bytes().fold(2_166_136_261_u32, |hash, byte| {
            (hash ^ u32::from(byte)).wrapping_mul(16_777_619)
        });
        if hash == 0 { u32::MAX } else { hash }
    }

    fn reserved_fqn() -> &'static str {
        static NAME: OnceLock<String> = OnceLock::new();
        NAME.get_or_init(|| {
            (0_u64..)
                .map(|n| format!("reserved.task{n}"))
                .find(|name| task_type_id_for_fqn(name) < RESERVED_TASK_ID_LIMIT)
                .unwrap()
        })
    }

    #[derive(Debug, Clone)]
    enum Registration {
        Component {
            library: u8,
            fqn: String,
            rust_type: u8,
        },
        Task {
            library: u8,
            fqn: String,
            reserved: Option<u32>,
            side_effect: bool,
            missing_root: bool,
        },
        Attribute {
            name: String,
            kind: bool,
        },
        Seal,
    }

    fn registrations() -> impl Strategy<Value = Vec<Registration>> {
        let names = prop_oneof![
            Just("sample.task".to_owned()),
            Just(reserved_fqn().to_owned()),
            "lib[a-c]{1,2}\\.[a-z]{1,5}"
        ];
        prop::collection::vec(
            prop_oneof![
                (0_u8..3, "lib[a-c]\\.[ab]{1,3}", 0_u8..4).prop_map(|(library, fqn, rust_type)| {
                    Registration::Component {
                        library,
                        fqn,
                        rust_type,
                    }
                }),
                (
                    0_u8..3,
                    names,
                    prop::option::of(prop_oneof![0_u32..6, 1024_u32..1028]),
                    any::<bool>(),
                    any::<bool>()
                )
                    .prop_map(
                        |(library, fqn, reserved, side_effect, missing_root)| Registration::Task {
                            library,
                            fqn,
                            reserved,
                            side_effect,
                            missing_root
                        }
                    ),
                (
                    prop_oneof![
                        prop::sample::select(RESERVED_SYSTEM_FIELDS.to_vec())
                            .prop_map(str::to_owned),
                        "Custom[A-C]{1,2}"
                    ],
                    any::<bool>()
                )
                    .prop_map(|(name, kind)| Registration::Attribute { name, kind }),
                Just(Registration::Seal),
            ],
            0..35,
        )
    }

    fn library(index: u8) -> &'static str {
        ["built-in", "extension", "other"][usize::from(index)]
    }
    fn rust_type(index: u8) -> TypeId {
        match index {
            0 => TypeId::of::<CompA>(),
            1 => TypeId::of::<CompB>(),
            2 => TypeId::of::<Root<1>>(),
            _ => TypeId::of::<Root<2>>(),
        }
    }

    #[derive(Default)]
    struct RegistrationModel {
        components: Vec<(String, u32, TypeId)>,
        tasks: Vec<(String, u32)>,
        libraries: BTreeSet<&'static str>,
        sealed: Option<BTreeSet<&'static str>>,
        attributes: HashMap<String, bool>,
    }

    impl RegistrationModel {
        fn sealed_name(&self, name: &str) -> bool {
            self.sealed
                .as_ref()
                .is_some_and(|names| names.contains(name))
        }

        fn apply(&mut self, operation: &Registration) -> Option<(&'static str, Vec<String>)> {
            match operation {
                Registration::Seal => {
                    self.sealed.get_or_insert_with(|| self.libraries.clone());
                }
                Registration::Component {
                    library: index,
                    fqn,
                    rust_type: slot,
                } => {
                    let name = library(*index);
                    if self.sealed_name(name) {
                        return Some(("library", vec![name.into()]));
                    }
                    let id = model_hash(fqn);
                    if self.components.iter().any(|(prior, hash, ty)| {
                        prior == fqn || *hash == id || *ty == rust_type(*slot)
                    }) {
                        return Some(("component", vec![fqn.clone()]));
                    }
                    self.components.push((fqn.clone(), id, rust_type(*slot)));
                    self.libraries.insert(name);
                }
                Registration::Task {
                    library: index,
                    fqn,
                    reserved,
                    missing_root,
                    ..
                } => {
                    let name = library(*index);
                    if self.sealed_name(name) {
                        return Some(("library", vec![name.into()]));
                    }
                    if *missing_root {
                        return Some(("validation", vec!["missing.root".into(), fqn.clone()]));
                    }
                    let id = reserved.unwrap_or_else(|| model_hash(fqn));
                    if reserved.is_some()
                        && (self.sealed.is_some() || !self.libraries.contains(name) || id >= 1024)
                    {
                        return Some(("library", vec![name.into()]));
                    }
                    if reserved.is_none() && id < 1024 {
                        return Some((
                            "collision",
                            vec![fqn.clone(), "reserved built-in range".into()],
                        ));
                    }
                    if let Some((prior, _)) = self
                        .tasks
                        .iter()
                        .find(|(prior, prior_id)| prior == fqn || *prior_id == id)
                    {
                        return Some(("collision", vec![prior.clone(), fqn.clone()]));
                    }
                    self.tasks.push((fqn.clone(), id));
                    self.libraries.insert(name);
                }
                Registration::Attribute { name, kind } => {
                    if self.sealed_name("built-in") {
                        return Some(("library", vec!["built-in".into()]));
                    }
                    if [
                        "archetype",
                        "status",
                        "lifecycle_state",
                        "namespace",
                        "run_id",
                        "business_id",
                    ]
                    .contains(&name.as_str())
                    {
                        return Some(("attribute", vec![name.clone()]));
                    }
                    if self.attributes.get(name).is_some_and(|prior| prior != kind) {
                        return Some(("validation", vec![name.clone()]));
                    }
                    self.attributes.insert(name.clone(), *kind);
                }
            }
            None
        }
    }

    fn error_kind(error: &ChasmError) -> &'static str {
        match error {
            ChasmError::ReservedLibraryName { .. } => "library",
            ChasmError::TaskTypeCollision { .. } => "collision",
            ChasmError::ReservedSearchAttribute { .. } => "attribute",
            ChasmError::Validation(_) => "validation",
            ChasmError::Internal(_) => "component",
            _ => "unexpected",
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct BuiltIdentities {
        valid: bool,
        components: Vec<(String, u32)>,
        tasks: Vec<(String, u32)>,
    }

    fn build_generated(operations: &[Registration]) -> BuiltIdentities {
        let mut builder = Registry::builder();
        builder.register::<Root>("built-in").unwrap();
        let mut model = RegistrationModel::default();
        model.components.push((
            Root::<0>::FQN.into(),
            model_hash(Root::<0>::FQN),
            TypeId::of::<Root>(),
        ));
        model.libraries.insert("built-in");
        let mut valid = true;
        for operation in operations {
            let expected = model.apply(operation);
            let result = match operation {
                Registration::Seal => {
                    builder.seal_built_ins();
                    Ok(())
                }
                Registration::Component {
                    library: index,
                    fqn,
                    rust_type: slot,
                } => builder
                    .register_component(
                        library(*index),
                        Box::leak(fqn.clone().into_boxed_str()),
                        rust_type(*slot),
                    )
                    .map(|_| ()),
                Registration::Task {
                    library: index,
                    fqn,
                    reserved,
                    side_effect,
                    missing_root,
                } => {
                    let erased = if *side_effect {
                        erase_side_effect(OutcomeHandler)
                    } else {
                        erase_pure(TickHandler)
                    };
                    builder
                        .register_task(
                            library(*index),
                            if *missing_root {
                                "missing.root"
                            } else {
                                Root::<0>::FQN
                            },
                            Box::leak(fqn.clone().into_boxed_str()),
                            if *side_effect {
                                TaskKind::SideEffect
                            } else {
                                TaskKind::Pure
                            },
                            *reserved,
                            erased,
                        )
                        .map(|_| ())
                }
                Registration::Attribute { name, kind } => builder
                    .register_search_attributes::<Root>(&[SearchAttributeDef {
                        name: Box::leak(name.clone().into_boxed_str()),
                        kind: if *kind {
                            SearchAttrKind::Int
                        } else {
                            SearchAttrKind::Keyword
                        },
                    }])
                    .map(|_| ()),
            };
            match (result, expected) {
                (Ok(()), None) => {}
                (Err(error), Some((kind, names))) => {
                    valid = false;
                    assert_eq!(error_kind(&error), kind, "{operation:?}: {error}");
                    for name in names {
                        assert!(error.to_string().contains(&name), "{error}: missing {name}");
                    }
                }
                (result, expected) => panic!("{operation:?}: {result:?} != {expected:?}"),
            }
        }
        let registry = builder.build();
        let components = registry
            .entries
            .iter()
            .map(|entry| (entry.fqn.to_owned(), entry.archetype_id))
            .collect::<Vec<_>>();
        let tasks = registry
            .tasks
            .iter()
            .map(|entry| (entry.fqn.to_owned(), entry.task_type_id))
            .collect::<Vec<_>>();
        assert_eq!(tasks, model.tasks);
        assert_eq!(
            components,
            model
                .components
                .into_iter()
                .map(|(name, id, _)| (name, id))
                .collect::<Vec<_>>()
        );
        for name in ["built-in", "extension", "other"] {
            assert_eq!(
                registry.is_built_in(name),
                model
                    .sealed
                    .as_ref()
                    .unwrap_or(&model.libraries)
                    .contains(name)
            );
        }
        BuiltIdentities {
            valid,
            components,
            tasks,
        }
    }

    // Feature: chasm-extension-archetypes, Property 1: registry validity and id stability
    // Independent registration models agree on every admission and stable durable id.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]
        #[test]
        fn registry_validity_and_id_stability(operations in registrations()) {
            prop_assert_eq!(build_generated(&operations), build_generated(&operations));
        }
    }
}
