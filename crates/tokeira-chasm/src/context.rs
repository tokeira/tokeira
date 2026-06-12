//! Read and read-write access capabilities for component code.
//!
//! Two object-safe traits gate what component code may do, mirroring upstream
//! CHASM's `context.go @ v1.31.0`:
//!
//! - [`Context`] (read-only) — read execution metadata and (once the tree lands)
//!   load components and read fields. It exposes neither field mutation nor task
//!   scheduling (Requirement 6.3).
//! - [`MutableContext`] (read-write) — additionally permits field mutation and
//!   [`add_task`](MutableContext::add_task). It is available **only inside a
//!   transition** (`UpdateComponent`/`StartExecution`), never on a read path
//!   (Requirement 6.4).
//!
//! The traits live in this pure crate so component authors (and the
//! [`Task`](crate::task) / [`Component`](crate::component) contracts) can be
//! written against them without depending on the runtime; the runtime supplies the
//! concrete impls over the live node tree. Both are **object-safe** — methods take
//! `&self`/`&mut self` and no generic type parameters — so component code receives
//! them as `&dyn Context` / `&mut dyn MutableContext`, exactly as the engine's
//! typed wrappers hand them out. Fallible operations return
//! [`ChasmError`](crate::ChasmError) rather than panicking (`AGENTS §1`).

use crate::{
    error::ChasmError,
    node::{ExecutionInfo, ExecutionKey},
    task::TaskKind,
};

/// Read-only access available on every path (reads and transitions alike).
///
/// At this layer it exposes the execution's identity, its summary
/// [`ExecutionInfo`], and the engine's logical clock. Component/field *reads*
/// resolve against the live node tree and are added with the runtime engine
/// (Layer 2); the trait is introduced here so the pure contracts that consume it
/// (lifecycle and task validation) have a stable, object-safe surface to name.
pub trait Context {
    /// The key identifying the execution this context operates on.
    fn execution_key(&self) -> &ExecutionKey;

    /// A read-only summary of the execution (transition count, approximate size,
    /// close time).
    fn execution_info(&self) -> ExecutionInfo;

    /// The engine's current logical time, in Unix nanoseconds. Component lifecycle
    /// and task validators read time from here rather than a wall clock so their
    /// decisions stay deterministic with respect to the transition being applied.
    fn now_unix_nanos(&self) -> i64;
}

/// Read-write access, available **only inside a transition**.
///
/// Extends [`Context`] with the two mutating capabilities a transition needs:
/// scheduling a task into the owning node's outbox, and marking the component's
/// node dirty after mutating its in-memory state. Both are absent from [`Context`]
/// so a read path structurally cannot schedule work or mutate state
/// (Requirement 6.3, 6.4).
///
/// `add_task` takes the **pre-serialized** task fields rather than a generic
/// `T: Task` so the trait stays object-safe (`&mut dyn MutableContext`). The
/// engine's typed wrapper serializes a typed [`Task`](crate::task::Task) and calls
/// this; the task receives its stable [`TaskId`](crate::task::TaskId) at transition
/// close, so none is supplied here.
pub trait MutableContext: Context {
    /// Schedule a task into the owning component node's outbox (Requirement 7.2).
    ///
    /// `kind` selects the pure/side-effect outbox; `task_type_id` names the
    /// registry task type (its validator/executor); `payload` is the serialized
    /// task; `fire_at_unix_nanos` is the pure-task deadline (`None` for
    /// side-effect tasks or undeadlined pure tasks). The task's `(VT, offset)`
    /// identity is assigned when the transition closes.
    fn add_task(
        &mut self,
        kind: TaskKind,
        task_type_id: u32,
        payload: Vec<u8>,
        fire_at_unix_nanos: Option<i64>,
    ) -> Result<(), ChasmError>;

    /// Mark the current component's node dirty so the transition close stamps it
    /// with a new [`VersionedTransition`](crate::versioned_transition) and persists
    /// it. Called after mutating the component's in-memory state (Requirement 5.1).
    fn mark_dirty(&mut self) -> Result<(), ChasmError>;
}
