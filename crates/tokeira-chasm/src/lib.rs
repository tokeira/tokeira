//! Pure CHASM substrate — the reusable durable-execution framework.
//!
//! CHASM ("Coordinated Heterogeneous Application State Machines") generalizes the
//! durable-execution engine: Workflow stops being *the* engine and becomes one
//! Application State Machine among many, all riding a shared substrate of
//! registry → libraries → typed components → atomic clock-stamped transitions →
//! transactional-outbox tasks → built-in visibility.
//!
//! This crate is that substrate, and it is a **peer of [`tokeira-kernel`], not an
//! extension of it**. Like the kernel, it is a deterministic, side-effect-free
//! library: it owns the component model, the field types, the node tree and its
//! transition-close algorithm, the [`VersionedTransition`](versioned_transition)
//! clock, the registry/library indexing, [`ComponentRef`](component_ref)
//! addressing, and the task model. It computes *what* a transition changes and
//! *what* it schedules; the runtime plane performs the I/O. Keeping that split
//! intact is the whole point of the parallel-substrate design — the workflow
//! kernel is left untouched, and `tokeira-kernel` does not depend on this crate
//! (Requirement 1.5).
//!
//! ## Invariants this crate upholds
//!
//! - **Purity.** No I/O, no async, no storage access, no metrics emission. The
//!   only dependencies are value/wire types (`tokeira-types`, `tokeira-proto`)
//!   plus serde/prost/thiserror (Requirement 1.1, 1.2). A change that would add
//!   async or storage belongs in `tokeira-runtime` or `tokeira-storage` instead.
//! - **No panics as control flow.** Every fallible operation returns
//!   [`Result<_, ChasmError>`]; there is no `unwrap`/`expect` in library code and
//!   no panic/recover framework boundary (Requirement 6.7, 6.8).
//! - **No runtime reflection.** Field discovery is generated at compile time by
//!   `tokeira-chasm-derive`; this crate performs no runtime type inspection.
//!
//! ## Module map
//!
//! The modules below are the substrate's surface; each carries its own contract
//! in its `//!` doc. They are introduced as skeletons here (task 1.1 of the
//! `chasm-foundation` spec) and filled in by later tasks:
//!
//! - [`error`] — the [`ChasmError`] taxonomy returned across the substrate.
//! - [`component`] — the component model and lifecycle.
//! - [`context`] — read (`Context`) vs read-write (`MutableContext`) access.
//! - [`field`] — the persistent field types (`Field`, `Map`, `ParentPtr`).
//! - [`path`] — the prefix-range-scannable node path encoder.
//! - [`node`] — the persisted node, the `ExecutionKey`, and the node tree.
//! - [`versioned_transition`] — the monotonic per-execution logical clock.
//! - [`registry`] — the immutable component/library index.
//! - [`component_ref`] — the addressable, staleness-checked node return-address.
//! - [`task`] — the pure/side-effect task model and validation contract.
//!
//! [`tokeira-kernel`]: https://docs.rs/tokeira-kernel

pub mod component;
pub mod component_ref;
pub mod context;
pub mod error;
pub mod field;
pub mod node;
pub mod path;
pub mod registry;
pub mod task;
pub mod versioned_transition;
pub mod visibility;

pub use component::{
    Component, ContextMetadata, EngineComponent, Lifecycle, LifecycleState, RootComponent,
    TerminateReason,
};
pub use component_ref::ComponentRef;
pub use context::{Context, MutableContext};
pub use error::ChasmError;
pub use field::{Field, FieldDescriptor, FieldKind, FieldRegistry, Map, NodeHandle, ParentPtr};
pub use node::{
    ChasmNode, DispatchableTask, ExecutionInfo, ExecutionKey, NodeMetadata, NodeTree,
    TransitionResult,
};
pub use path::{PathEncoder, PathSegment, SegmentKind};
pub use registry::{
    ComponentEntry, LEGACY_WORKFLOW_ARCHETYPE_ID, Library, Registry, RegistryBuilder,
    archetype_id_for_fqn,
};
pub use task::{
    OutboxValidator, RetainAllValidator, ScheduledTask, Task, TaskId, TaskKind, TaskOutbox,
    TaskValidator, TaskValidity,
};
pub use versioned_transition::{Staleness, VersionedTransition};
pub use visibility::{SearchAttributeProvider, SearchAttributes};
