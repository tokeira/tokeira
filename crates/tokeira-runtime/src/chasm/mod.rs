//! The CHASM engine surface — the async, stateful runtime façade for the CHASM
//! substrate (design "The Engine Surface"; Requirement 6).
//!
//! This module is a **new, additive plane** alongside the existing workflow
//! runtime: it shares storage and the fenced-commit posture but no code paths with
//! the workflow engine, which is left untouched (design "Parallel Substrate
//! First"). It uses the pure [`tokeira_chasm`] crate to compute transitions and the
//! [`ChasmNodeRepository`](tokeira_storage::ChasmNodeRepository) to persist them,
//! mirroring how the workflow runtime uses the kernel + `RunRepository`.
//!
//! The load-bearing runtime rule still holds (`crates/tokeira-runtime/AGENTS.md`):
//! every state-changing request is a durable, fenced transition; dispatch, timers,
//! and visibility are **derived effects** computed at transition close and applied
//! post-commit, never the source of truth.
//!
//! ## What this layer is, and what it defers
//!
//! It implements the seven engine operations, monotonic long-poll, post-commit
//! side-effect dispatch, the single tree-wide physical timer, execution close on
//! root lifecycle, and the visibility provider hook — all over the in-memory node
//! store the design names as the verification vehicle (spec task 15.1).
//!
//! **MVP component materialization.** A component is materialized from a single
//! **root node** (encoded path = the empty key) whose `data` is the serialized
//! [`Component::Data`](tokeira_chasm::Component::Data). The full lazy multi-node
//! field tree (child `Field`/`Map` subtrees resolved on access) is a Layer-2+
//! concern the activity archetype does not need for its state machine; the node
//! tree and store already support multi-node trees, but the engine's typed closure
//! operates on the root component's data. The [`EngineComponent`] bridge makes that
//! seam explicit. This is a deliberate, documented MVP boundary (`AGENTS §9`).

mod engine;
mod typed;

pub use engine::{
    ChasmEngine, ChasmEngineConfig, CollectingDispatchSink, CollectingVisibilitySink, DispatchSink,
    NoopVisibilitySink, ROOT_PATH, VisibilitySink,
};
pub use typed::TypedEngine;
// Re-exported from the pure crate: these are component-level contracts, not engine
// internals, so component crates depend on `tokeira-chasm` for them (Requirement 1.4).
pub use tokeira_chasm::{EngineComponent, SearchAttributeProvider, SearchAttributes};

use async_trait::async_trait;
use tokeira_chasm::{ChasmError, ComponentRef, ExecutionKey, LifecycleState, VersionedTransition};

/// A task staged by a transition for persistence into the owning node's outbox.
/// The type-erased shape the engine threads from a typed mutation to the pure
/// node tree (Requirement 7.2).
#[derive(Debug, Clone)]
pub struct StagedTask {
    /// Pure vs side-effect discipline.
    pub kind: tokeira_chasm::TaskKind,
    /// Registry type id of the task type (its validator/executor).
    pub task_type_id: u32,
    /// Serialized task payload.
    pub payload: Vec<u8>,
    /// Pure-task deadline in Unix nanoseconds, or `None`.
    pub fire_at_unix_nanos: Option<i64>,
}

/// Request to create a new execution rooted at an archetype's root component
/// (Requirement 6.1, `StartExecution`).
#[derive(Debug, Clone)]
pub struct StartRequest {
    /// The execution to create.
    pub key: ExecutionKey,
    /// The root component's archetype id (from the registry).
    pub archetype_id: u32,
    /// The serialized initial root-component data.
    pub data: Vec<u8>,
    /// Originating request id, recorded as context metadata.
    pub request_id: Option<String>,
}

/// A precomputed mutation to apply as one fenced transition (Requirement 6.1,
/// `UpdateComponent`).
///
/// The typed wrapper computes this from the loaded state and hands it to the
/// untyped engine; the engine applies it fenced on `expected_execution_vt`. On a
/// fence/CAS conflict the engine returns [`CommitOutcome::Conflict`] and the typed
/// wrapper reloads and recomputes (Requirement 9.5) — the reason the closure-rerun
/// loop lives in [`TypedEngine`], not here.
#[derive(Debug, Clone)]
pub struct UpdateRequest {
    /// The execution to mutate.
    pub key: ExecutionKey,
    /// The execution VT the mutation was computed against; the fence rejects the
    /// commit if the live clock has advanced past it.
    pub expected_execution_vt: VersionedTransition,
    /// The new serialized root-component data.
    pub new_root_data: Vec<u8>,
    /// The root component's lifecycle after the mutation; a closed value closes the
    /// execution (Requirement 2.3, 2.4).
    pub new_lifecycle: LifecycleState,
    /// Tasks scheduled by the mutation.
    pub tasks: Vec<StagedTask>,
    /// Search attributes contributed by the mutation, emitted to visibility on
    /// commit (Requirement 10).
    pub search_attributes: SearchAttributes,
}

/// The outcome of a successful transition (Requirement 6.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateOutcome {
    /// A fresh reference to the root component as of the committed VT.
    pub reference: ComponentRef,
    /// The execution clock after the commit.
    pub execution_vt: VersionedTransition,
    /// Whether this transition closed the execution.
    pub closed: bool,
}

/// The result of an [`Engine::update_component`] attempt: applied, or rejected by
/// the fence/CAS so the caller must reload and retry (Requirement 9.5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitOutcome {
    /// The transition committed.
    Applied(UpdateOutcome),
    /// The fence failed; reload and re-run.
    Conflict,
}

/// Read-only snapshot of a component (Requirement 6.1, `ReadComponent`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadOutcome {
    /// The serialized root-component data, if the execution exists.
    pub data: Option<Vec<u8>>,
    /// The root component's lifecycle, if known.
    pub lifecycle: Option<LifecycleState>,
    /// The execution clock at the time of the read.
    pub execution_vt: VersionedTransition,
}

/// A monotonic long-poll request (Requirement 6.5, 6.6).
#[derive(Debug, Clone)]
pub struct PollRequest {
    /// The execution to poll.
    pub key: ExecutionKey,
    /// The caller's last-seen execution VT; the poll resolves only once the live
    /// clock advances past it.
    pub since: VersionedTransition,
}

/// The result of a long-poll (Requirement 6.6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PollOutcome {
    /// The component advanced past `since`; carries the fresh snapshot.
    Advanced(ReadOutcome),
    /// The deadline (minus the long-poll buffer) elapsed without advancing; the
    /// caller should resubmit.
    Empty,
}

/// An external wake signal for an execution (Requirement 6.1, `NotifyExecution`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotifyEvent {
    /// Re-evaluate pollers/tasks after an external event.
    Generic,
}

/// The untyped CHASM engine core (Requirement 6.1, 6.7).
///
/// Async and stateful; errors are explicit `Result`s — no engine-on-context
/// injection and no panic-as-framework-error (design HP5; `AGENTS §1`). The typed,
/// component-keyed [`TypedEngine`] wraps this for callers that work in terms of a
/// concrete [`Component`](tokeira_chasm::Component).
#[async_trait]
pub trait Engine: Send + Sync {
    /// Create a new execution rooted at an archetype's root component. Fails with
    /// [`ChasmError::BusinessIdConflict`] if the execution already exists.
    async fn start_execution(&self, req: StartRequest) -> Result<ComponentRef, ChasmError>;

    /// Apply one precomputed mutation as a fenced transition. Returns
    /// [`CommitOutcome::Conflict`] (not an error) when the fence fails so the caller
    /// can reload and retry (Requirement 9.5).
    async fn update_component(&self, req: UpdateRequest) -> Result<CommitOutcome, ChasmError>;

    /// Snapshot-read the root component with no dirty nodes and no tasks
    /// (Requirement 6.1, 5.3).
    async fn read_component(&self, key: &ExecutionKey) -> Result<ReadOutcome, ChasmError>;

    /// Monotonic long-poll: resolve when the component VT advances past
    /// `req.since`, or return [`PollOutcome::Empty`] on deadline (Requirement 6.5,
    /// 6.6).
    async fn poll_component(&self, req: PollRequest) -> Result<PollOutcome, ChasmError>;

    /// Range-delete the execution's node subtree (Requirement 6.1).
    async fn delete_execution(&self, key: &ExecutionKey) -> Result<(), ChasmError>;

    /// Wake pollers / re-evaluate tasks after an external event (Requirement 6.1).
    async fn notify_execution(
        &self,
        key: &ExecutionKey,
        event: NotifyEvent,
    ) -> Result<(), ChasmError>;
}
