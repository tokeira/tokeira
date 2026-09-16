//! The CHASM error taxonomy.
//!
//! Every fallible operation in the substrate — transition application, task
//! validation, reference resolution, and the engine surface that wraps them —
//! returns [`ChasmError`] rather than panicking. This is the structural reason
//! the substrate satisfies the no-`unwrap`/no-panic-as-control-flow rule
//! (Requirement 6.7, 6.8; `AGENTS §1`): a failed precondition is a typed value
//! that the runtime threads back to the caller and the edge maps to a wire
//! status, never a panic across a recover boundary the way upstream CHASM does.
//!
//! The enum is `#[non_exhaustive]`: the substrate (task 1.1) fixes the core
//! variants the design names, and later layers (engine, storage, edge) add the
//! engine-surfaced variants they need without it being a breaking change.

use thiserror::Error;

/// The error type returned across the CHASM substrate.
///
/// Variants fall into two groups: **component/transition errors** raised by the
/// pure framework and component code (`IllegalTransition`, `StaleStamp`,
/// `StaleReference`, `Validation`), and **engine-surfaced errors** raised by the
/// runtime engine while orchestrating a transition (`ExecutionNotFound`,
/// `ExecutionClosed`, `BusinessIdConflict`, `RetriesExhausted`, `Internal`). The
/// edge bridge maps each to the gRPC status the targeted release returns; see the
/// error-mapping table in the design.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ChasmError {
    /// A component operation failed a lifecycle or capacity precondition. Preserve
    /// the message at the edge while mapping to FAILED_PRECONDITION, not INVALID_ARGUMENT.
    #[error("{0}")]
    FailedPrecondition(String),
    /// No handler of the requested discipline owns this component/task pair.
    /// Transition close must abort rather than silently retaining unknown work.
    #[error("unknown task type {task_type_id} for component {component_type_id}")]
    UnknownTaskType {
        /// Archetype of the component that owns the outbox.
        component_type_id: u32,
        /// Persisted task type whose handler is missing.
        task_type_id: u32,
    },
    /// A sealed built-in library name or the reserved registration phase was
    /// used by an extension; registration leaves the builder unchanged.
    #[error("reserved built-in library name or registration: `{name}`")]
    ReservedLibraryName {
        /// Library whose registration is forbidden.
        name: String,
    },
    /// Duplicate task identity, or a derived id in the built-in reserved range.
    #[error("task type id {id} collides between `{first}` and `{second}`")]
    TaskTypeCollision {
        /// Previously registered FQN, or the rejected FQN for a reserved hash.
        first: String,
        /// Conflicting FQN, or the reserved-range explanation.
        second: String,
        /// Conflicting persisted task id.
        id: u32,
    },
    /// A component tried to declare an attribute supplied by the engine itself.
    #[error("reserved system search attribute `{name}`")]
    ReservedSearchAttribute {
        /// Reserved field that must not be spoofed by a component.
        name: String,
    },
    /// Startup found durable executions whose archetype is absent from the
    /// registry; serving them without their library would strand their work.
    #[error("unregistered archetype {archetype_id} owns {executions} executions")]
    UnregisteredArchetype {
        /// Persisted archetype id without a registered component.
        archetype_id: u32,
        /// Number of affected current execution pointers.
        executions: u64,
    },
    /// An activity/component event was applied from a state in which it is not
    /// legal. The transition is rejected and the component state is left
    /// unchanged (Requirement 11.5). This is a programming/protocol error, not a
    /// race; validated edge input should never produce it.
    #[error("illegal transition: event `{event}` is not legal from state `{from}`")]
    IllegalTransition {
        /// Human-readable name of the state the event was applied from.
        from: String,
        /// Human-readable name of the event that was rejected.
        event: String,
    },

    /// A transition or task was evaluated against a per-attempt fencing stamp
    /// that no longer matches the live attempt, meaning a newer attempt has
    /// superseded it (Requirement 11.6). The work is treated as a no-op rather
    /// than applied.
    #[error("stale stamp: the fencing stamp does not match the live attempt; superseded")]
    StaleStamp,

    /// A [`ComponentRef`](crate::component_ref) whose execution `VersionedTransition`
    /// is behind the live execution clock was used; the referenced node may have
    /// moved or closed, so the reference is reported stale rather than followed
    /// (Requirement 8.6).
    #[error("stale reference: the component reference is behind the live execution clock")]
    StaleReference,

    /// A request failed validation (e.g. missing task queue, over-length id, or a
    /// timeout that cannot be normalized). Carries the targeted-release-aligned
    /// message (Requirement 11.9).
    #[error("validation failed: {0}")]
    Validation(String),

    /// The addressed execution does not exist.
    #[error("execution not found")]
    ExecutionNotFound,

    /// A mutating transition was attempted against an execution whose root
    /// component is already closed; closed executions admit no further mutating
    /// transition (Requirement 2.4).
    #[error("execution is closed: no further mutating transition is admitted")]
    ExecutionClosed,

    /// `UpdateWithStartExecution`/`StartExecution` hit the business-id
    /// reuse/conflict policy and the request was not admitted (Requirement 6.2).
    #[error("business id conflict: {0}")]
    BusinessIdConflict(String),

    /// A `StartExecution` was rejected because a current run for the same business
    /// id already exists and the id reuse/conflict policy did not admit a new run
    /// (Requirement 1, 2). Carries the *current* run's id and its create request id
    /// so the edge can surface the targeted release's typed
    /// `ActivityExecutionAlreadyStarted` error with `RunId`/`StartRequestId` details
    /// (`chasm/lib/activity/handler.go:91`; `service/history/chasm_engine.go` →
    /// `NewExecutionAlreadyStartedErr` @ v1.31.0). Distinct from
    /// [`BusinessIdConflict`](Self::BusinessIdConflict) (an opaque message) because
    /// the conformance contract requires the structured run/request ids, not a
    /// string.
    #[error("{message}")]
    BusinessIdAlreadyStarted {
        /// The current run's id (the run the new Start collided with).
        run_id: String,
        /// The current run's create request id (`StartRequestId` in the detail).
        request_id: String,
        /// The v1.31.0-faithful human-readable message.
        message: String,
    },

    /// The fenced commit was retried after optimistic-concurrency conflicts up to
    /// the bound without succeeding (Requirement 9.5). Surfaced only when retries
    /// are exhausted; ordinary conflicts reload and re-run transparently.
    #[error("transition retries exhausted after {attempts} optimistic-concurrency conflicts")]
    RetriesExhausted {
        /// Number of attempts made before giving up.
        attempts: u32,
    },

    /// An invariant the substrate expects to hold was violated. Indicates a bug
    /// in the substrate or its callers rather than bad input.
    #[error("internal CHASM error: {0}")]
    Internal(String),

    /// A requested behaviour is recognised but not implemented in this release,
    /// mirroring a targeted-release `Unimplemented` (e.g. the `TerminateExisting`
    /// id-conflict policy, which `service/history/chasm_engine.go:1041 @ v1.31.0`
    /// also answers `Unimplemented`). The edge maps this to gRPC `Unimplemented`.
    #[error("unsupported: {0}")]
    Unsupported(String),
}
