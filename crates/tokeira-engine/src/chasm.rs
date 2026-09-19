//! Everything an extension library and its embedder name, re-exported so a
//! component library compiles with `tokeira-engine` as its only engine dependency.
//! This opt-in embedder surface is unstable and has no semver promise.
//!
//! ## What a library reaches through here
//!
//! - The component contract: [`Component`] (trait and derive), [`Lifecycle`],
//!   [`RootComponent`], [`EngineComponent`], [`SearchAttributeProvider`],
//!   [`VisibilityContributor`] — the bounds [`Engine::chasm`](crate::Engine::chasm)
//!   places on a root — with the field, metadata and value types their
//!   signatures use.
//! - Task authoring: [`Task`], the two handler traits, the staged
//!   [`StartActivityTask`], the erased [`ScheduledTask`] an executor receives,
//!   and the id derivations [`archetype_id_for_fqn`] / [`task_type_id_for_fqn`]
//!   that registration and `MutableContext::add_task` agree on.
//! - The typed handle [`TypedEngine`] with its outcomes, the
//!   [`SideEffectExecutor`] contract, the visibility query types, and
//!   [`namespace_id_for`], which turns a namespace name into the
//!   [`NamespaceId`] every key and handle for that namespace carries.
//!
//! ## Deriving `Component` through this module
//!
//! The derive generates paths rooted at the substrate crate. A crate that reaches
//! the substrate only through this module names it instead:
//!
//! ```ignore
//! use tokeira_engine::chasm::{Component, ContextMetadata, Field};
//!
//! #[derive(Debug, Component)]
//! #[chasm(fqn = "example.resource", crate = "::tokeira_engine::chasm")]
//! struct Resource {
//!     #[chasm(data)]
//!     state: Field<ResourceState>,
//!     #[chasm(transient)]
//!     meta: ContextMetadata,
//! }
//! ```
//!
//! `SearchAttributeProvider::search_attributes` returns `Vec<(String, String)>`;
//! the [`SearchAttributes`] re-exported here is the typed map a
//! [`VisibilitySnapshot`] carries.
//!
//! ## Wire encodings on the activity boundary
//!
//! The byte fields a library exchanges with the built-in standalone-activity
//! machinery are the Temporal API protobuf messages, encoded as protobuf (the
//! `temporal.api` packages the vendored `proto/upstream/` tree defines, which any
//! Temporal SDK's proto crate can produce and parse):
//!
//! | Field | Message |
//! |---|---|
//! | [`StartActivityTask::input`] | `temporal.api.common.v1.Payloads` |
//! | [`StartActivityTask::header`] | `temporal.api.common.v1.Header` |
//! | [`StartActivityTask::retry_policy`] | `temporal.api.common.v1.RetryPolicy`; empty selects the engine's defaults |
//! | [`TaskOutcome::Completed`] `payload` | `temporal.api.common.v1.Payloads` — the activity result |
//! | [`TaskOutcome::Failed`] `failure` | `temporal.api.failure.v1.Failure` |
//! | [`TaskOutcome::Canceled`] `details` | `temporal.api.common.v1.Payloads` — the worker's cancellation details |
//! | [`TaskOutcome::TimedOut`] `timeout_type` | the numeric `temporal.api.enums.v1.TimeoutType` |
//!
//! A library's own task payloads are its own affair: [`Task::encode`] and
//! [`Task::decode`] own the codec, and the engine never inspects those bytes.

pub use tokeira_chasm::{
    BusinessIdConflictPolicy, BusinessIdPolicy, BusinessIdReusePolicy, ChasmError, Component,
    ComponentRef, Context, ContextMetadata, DeploymentVersionTarget, EngineComponent,
    ExecutionInfo, ExecutionKey, Field, FieldDescriptor, FieldKind, FieldRegistry, Library,
    Lifecycle, LifecycleState, Map, MutableContext, ParentPtr, PureTaskHandler, RegistryBuilder,
    RootComponent, ScheduledTask, SearchAttrKind, SearchAttributeDef, SearchAttributeProvider,
    SideEffectTaskHandler, StartActivityTask, Task, TaskId, TaskKind, TaskOutcome, TaskValidity,
    TerminateReason, VersionedTransition, VisibilityContributor, VisibilitySnapshot,
    archetype_id_for_fqn, task_type_id_for_fqn,
};
pub use tokeira_chasm_derive::Component;
pub use tokeira_edge::translate::to_internal::namespace_id_for;
pub use tokeira_projection::{
    ComponentPage, ComponentQuery, ComponentQueryError, ComponentSummary, ComponentVisibility,
};
pub use tokeira_runtime::chasm::{SideEffectExecutor, StartOutcome, TypedEngine, UpdateOutcome};
pub use tokeira_types::{
    ArchetypeId, Memo, NamespaceId, RunId, SearchAttrValue, SearchAttributes, TransitionSeq,
    VisibilityLifecycleState,
};
