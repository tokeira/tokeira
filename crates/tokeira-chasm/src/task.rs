//! The task model and the transactional-outbox close semantics.
//!
//! Tasks are how a transition schedules future work, and they are the substrate's
//! expression of "history is authority; dispatch is a derived effect"
//! (`AGENTS §3`). A task is either:
//!
//! - a **pure task** — runs in-transaction, holds the write lock, performs no
//!   I/O (used for time-driven state changes such as activity timeouts), or
//! - a **side-effect task** — runs post-commit, may do I/O, performs no direct
//!   state mutation (used for dispatch-to-matching) (Requirement 7.1).
//!
//! Tasks persist in the owning component node's outbox as `pure_tasks[]` /
//! `side_effect_tasks[]`, each identified by `(VersionedTransition, offset)`
//! (Requirement 7.2). On every dirty close the outbox is re-validated through each
//! task's validator: a task whose precondition no longer holds is **dropped
//! without executing** (validate-then-drop), and a surviving task keeps its stable
//! `(VT, offset)` identity (Requirement 7.3–7.5).
//!
//! ## Two faces of a task
//!
//! A task has an **author-facing** typed form and a **persisted** type-erased
//! form, and this module owns both:
//!
//! - The [`Task`] trait (its [`KIND`](Task::KIND) and [`fire_at`](Task::fire_at))
//!   and the [`TaskValidator`] gate are what a component library *writes*, generic
//!   over a concrete [`Component`] and task type.
//! - [`ScheduledTask`] is the serialized form that actually lives in a node's
//!   [`TaskOutbox`]: a `kind`, a `task_type_id` naming which validator/executor
//!   applies, the serialized `payload`, the pure-task `fire_at`, and the
//!   `(VT, offset)` [`TaskId`]. The node tree persists and re-validates this
//!   erased form, because at close time it walks a tree of type-erased nodes (it
//!   does not know the concrete component types). The runtime bridges the two by
//!   deserializing a `ScheduledTask` back to its typed `Task` to run the typed
//!   validator/executor.
//!
//! Task FQNs derive stable ids outside the reserved built-in range. Each task
//! owns its wire codec: changing the registry must not change persisted bytes.
//!
//! Purity: these are plain value types and contracts. No I/O, no async, no storage
//! (Requirement 1.1). The single armed physical timer and post-commit dispatch
//! that consume a [transition result](crate::node::TransitionResult) live in the
//! runtime; this module owns the model and the validate-then-drop contract.

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    ChasmError, Registry, component::Component, context::Context,
    versioned_transition::VersionedTransition,
};

/// Persisted built-in task ids occupy `[0, 1024)`. Derived extension ids in this
/// range are rejected, keeping existing activity outboxes (ids 1–5) readable.
pub const RESERVED_TASK_ID_LIMIT: u32 = 1024;

/// Derive a stable task id using exactly the component FQN hash. Registration
/// rejects reserved-range hashes and collisions; this adds no remapping beyond
/// the component hash's existing zero-id reservation.
pub fn task_type_id_for_fqn(fqn: &str) -> u32 {
    crate::registry::archetype_id_for_fqn(fqn)
}

/// Result of external work, applied by a pure outcome handler in a transition.
/// The byte fields are opaque to the substrate; for the built-in standalone
/// activity they are the Temporal API messages named on each variant, encoded as
/// protobuf, and a library's own executor chooses its own encoding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskOutcome {
    /// Successful work with the executor's opaque result bytes.
    Completed {
        /// Result in the executor's wire format; an activity result is an encoded
        /// `temporal.api.common.v1.Payloads`.
        payload: Vec<u8>,
    },
    /// Failed work and the executor's retry classification.
    Failed {
        /// Encoded failure, interpreted by the component; an activity failure is
        /// an encoded `temporal.api.failure.v1.Failure`.
        failure: Vec<u8>,
        /// Whether retrying the external work may succeed.
        retryable: bool,
    },
    /// Canceled work with optional executor-specific detail bytes.
    Canceled {
        /// Encoded cancellation details; an activity's are the worker's encoded
        /// `temporal.api.common.v1.Payloads`.
        details: Vec<u8>,
    },
    /// Work exceeded the named Temporal timeout category.
    TimedOut {
        /// Numeric Temporal timeout enum, preserved without narrowing.
        timeout_type: i32,
    },
    /// Work was terminated explicitly.
    Terminated,
}

/// Exact worker deployment version for an internally staged standalone activity.
/// This is Tokeira-owned routing metadata, not a public start-request extension.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, prost::Message)]
pub struct DeploymentVersionTarget {
    /// Deployment that owns the worker release.
    #[prost(string, tag = "1")]
    pub deployment_name: String,
    /// Exact build within that deployment.
    #[prost(string, tag = "2")]
    pub build_id: String,
}

/// Library-neutral request for runtime work that starts a standalone activity.
/// Payloads remain opaque encoded Temporal messages. This task owns a prost
/// codec; libraries with existing codecs keep theirs, avoiding a substrate codec
/// dependency or an implicit rewrite of durable outboxes.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, prost::Message)]
pub struct StartActivityTask {
    /// Business id of the requested activity execution.
    #[prost(string, tag = "1")]
    pub activity_id: String,
    /// Worker activity type.
    #[prost(string, tag = "2")]
    pub activity_type: String,
    /// Queue receiving the activity attempt.
    #[prost(string, tag = "3")]
    pub task_queue: String,
    /// Encoded `temporal.api.common.v1.Payloads`: the worker's activity input.
    #[prost(bytes = "vec", tag = "4")]
    pub input: Vec<u8>,
    /// Encoded `temporal.api.common.v1.Header`, handed to the worker unchanged.
    #[prost(bytes = "vec", tag = "5")]
    pub header: Vec<u8>,
    /// Encoded `temporal.api.common.v1.RetryPolicy`; empty selects the engine's
    /// defaults.
    #[prost(bytes = "vec", tag = "6")]
    pub retry_policy: Vec<u8>,
    /// Schedule-to-start timeout in nanoseconds.
    #[prost(int64, tag = "7")]
    pub schedule_to_start_nanos: i64,
    /// Schedule-to-close timeout in nanoseconds.
    #[prost(int64, tag = "8")]
    pub schedule_to_close_nanos: i64,
    /// Start-to-close timeout in nanoseconds.
    #[prost(int64, tag = "9")]
    pub start_to_close_nanos: i64,
    /// Heartbeat timeout in nanoseconds.
    #[prost(int64, tag = "10")]
    pub heartbeat_nanos: i64,
    /// Optional exact worker release; absent means unversioned dispatch.
    #[prost(message, optional, tag = "11")]
    pub version_target: Option<DeploymentVersionTarget>,
}

impl Task for StartActivityTask {
    const KIND: TaskKind = TaskKind::SideEffect;
    const FQN: &'static str = "chasm.start_activity";

    fn fire_at(&self) -> Option<i64> {
        None
    }

    fn encode(&self) -> Result<Vec<u8>, ChasmError> {
        Ok(prost::Message::encode_to_vec(self))
    }

    fn decode(bytes: &[u8]) -> Result<Self, ChasmError> {
        <Self as prost::Message>::decode(bytes)
            .map_err(|e| ChasmError::Validation(format!("decode {}: {e}", Self::FQN)))
    }
}

/// Which of the two task disciplines a task obeys (Requirement 7.1; foundation
/// §1, §3). The discipline is a static property of the task *type*, so it is a
/// `const` on [`Task`] rather than a per-instance field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskKind {
    /// Runs *inside* the transition, holds the write lock, performs **no I/O**.
    /// Used for time-driven state changes (activity timeouts); folded into the
    /// commit so the effect costs no extra DSQL transaction (foundation §6).
    Pure,
    /// Runs *post-commit*, may do I/O, performs **no direct state mutation** (it
    /// re-enters the engine to open a new transition if it must change state).
    /// Used for dispatch-to-matching.
    SideEffect,
}

/// The outcome of re-validating a task at transition close (Requirement 7.4).
///
/// This is the validate-then-drop gate: a [`Drop`](TaskValidity::Drop) task is
/// removed from the outbox and never executed; a [`Valid`](TaskValidity::Valid)
/// task is retained with its stable [`TaskId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskValidity {
    /// The task's precondition still holds; keep it (with its `(VT, offset)`).
    Valid,
    /// The task's precondition no longer holds (e.g. its attempt was superseded);
    /// drop it from the outbox without executing it.
    Drop,
}

/// The stable identity of a persisted task within its owning node: the
/// [`VersionedTransition`] at which it was scheduled plus an `offset` unique
/// within that node (Requirement 7.2).
///
/// `offset` is assigned from a per-node monotonic counter at transition close, so
/// it is unique across the node's whole lifetime — a superset of "unique within a
/// VT". Re-validation preserves a surviving task's `TaskId` unchanged, which is
/// what lets the runtime recognise "the same task" across transitions
/// (Requirement 7.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskId {
    /// The VersionedTransition at which the task was scheduled.
    pub versioned_transition: VersionedTransition,
    /// A per-node monotonic offset, unique within the owning node.
    pub offset: u32,
}

impl TaskId {
    /// Construct a task identity from its VT and per-node offset.
    pub const fn new(versioned_transition: VersionedTransition, offset: u32) -> Self {
        Self {
            versioned_transition,
            offset,
        }
    }
}

/// A persisted, type-erased task as it lives in a node's [`TaskOutbox`].
///
/// This is the serialized form the node tree stores in `metadata` and re-validates
/// on dirty close. It is deliberately type-erased: the close algorithm walks a
/// tree of nodes without knowing their concrete component/task Rust types, so it
/// carries `task_type_id` (the registry id of the task type) plus the serialized
/// `payload` and lets the runtime reconstruct the typed [`Task`] to run its typed
/// validator/executor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduledTask {
    /// Pure vs side-effect — which outbox the task lives in and how it is run.
    pub kind: TaskKind,
    /// Registry type id of the task type; names the validator/executor to apply.
    pub task_type_id: u32,
    /// The serialized task value (the runtime deserializes it to the typed form).
    pub payload: Vec<u8>,
    /// For a pure task, the logical time (Unix nanoseconds) at which it becomes
    /// due; drives the single tree-wide physical timer. `None` for side-effect
    /// tasks and for pure tasks with no deadline. Carried as `i64` nanos rather
    /// than a time type to keep the pure crate's dependency set minimal
    /// (Requirement 1.1); the runtime converts at its boundary.
    pub fire_at_unix_nanos: Option<i64>,
    /// The task's stable `(VT, offset)` identity, assigned at transition close.
    pub id: TaskId,
}

impl ScheduledTask {
    /// True iff this is a [`TaskKind::Pure`] task.
    pub fn is_pure(&self) -> bool {
        matches!(self.kind, TaskKind::Pure)
    }

    /// True iff this is a [`TaskKind::SideEffect`] task.
    pub fn is_side_effect(&self) -> bool {
        matches!(self.kind, TaskKind::SideEffect)
    }
}

/// A node's transactional outbox: the pure and side-effect tasks scheduled against
/// it, serialized inside the node's `metadata` (Storage Design). Keeping the
/// outbox *in the node* — not in a separate queue — is the structural reason
/// scheduling a task is part of the same dirty-node write and dispatch is a
/// derived read (`AGENTS §3`; Requirement 7.2).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskOutbox {
    /// Pure tasks scheduled against the owning node, in `(VT, offset)` order.
    pub pure_tasks: Vec<ScheduledTask>,
    /// Side-effect tasks scheduled against the owning node, in `(VT, offset)`
    /// order.
    pub side_effect_tasks: Vec<ScheduledTask>,
}

impl TaskOutbox {
    /// An empty outbox.
    pub fn new() -> Self {
        Self::default()
    }

    /// True iff the outbox holds no tasks of either kind.
    pub fn is_empty(&self) -> bool {
        self.pure_tasks.is_empty() && self.side_effect_tasks.is_empty()
    }

    /// Total number of tasks across both disciplines.
    pub fn len(&self) -> usize {
        self.pure_tasks.len() + self.side_effect_tasks.len()
    }

    /// Append a task to the outbox slot matching its [`kind`](ScheduledTask::kind).
    pub fn push(&mut self, task: ScheduledTask) {
        match task.kind {
            TaskKind::Pure => self.pure_tasks.push(task),
            TaskKind::SideEffect => self.side_effect_tasks.push(task),
        }
    }

    /// Iterate every task in the outbox (pure first, then side-effect), in
    /// `(VT, offset)` order within each discipline.
    pub fn iter(&self) -> impl Iterator<Item = &ScheduledTask> {
        self.pure_tasks.iter().chain(self.side_effect_tasks.iter())
    }

    /// The earliest pure-task [`fire_at`](ScheduledTask::fire_at_unix_nanos) in
    /// this outbox, ignoring pure tasks with no deadline. Used by the tree-wide
    /// "single earliest pure timer" computation (Requirement 7.6).
    pub fn earliest_pure_deadline(&self) -> Option<i64> {
        self.pure_tasks
            .iter()
            .filter_map(|t| t.fire_at_unix_nanos)
            .min()
    }
}

/// A scheduled task in its author-facing typed form (Requirement 7.1, 7.2).
///
/// A component library implements [`Task`] for each task type it schedules. The
/// type's discipline is the static [`KIND`](Task::KIND); a pure task additionally
/// reports when it becomes due via [`fire_at`](Task::fire_at). The runtime
/// serializes a `Task` into a [`ScheduledTask`] for the outbox and deserializes it
/// back to run the typed [`TaskValidator`].
pub trait Task: Serialize + DeserializeOwned + Send + Sync + 'static {
    /// The task's static discipline (pure vs side-effect).
    const KIND: TaskKind;

    /// Stable name (`library.task`), hashed for extension ids. Renaming changes
    /// durable identity, so a library must preserve it across releases.
    const FQN: &'static str;

    /// Encode with this task's durable wire codec. The substrate never chooses
    /// a codec on the library's behalf.
    fn encode(&self) -> Result<Vec<u8>, ChasmError>;

    /// Decode the task's durable bytes; malformed payloads abort the transition.
    fn decode(bytes: &[u8]) -> Result<Self, ChasmError>;

    /// For a pure task, the logical time (Unix nanoseconds) at which it becomes
    /// due. Drives the single tree-wide physical timer (Requirement 7.6). Returns
    /// `None` for side-effect tasks and for pure tasks with no deadline.
    fn fire_at(&self) -> Option<i64>;
}

/// The drop-if-stale gate, re-evaluated on every dirty close (validate-then-drop,
/// Requirement 7.3–7.5).
///
/// A validator compares the task's fencing stamp to the live component state: a
/// task belonging to a superseded attempt, or scheduled against a now-terminal
/// component, returns [`TaskValidity::Drop`] and is reaped without executing. The
/// validator is **pure**: it reads the component and the task through a read-only
/// [`Context`] and returns a verdict; it performs no I/O and no mutation.
pub trait TaskValidator<C: Component, T: Task> {
    /// Decide whether `task` (scheduled against `component`) is still valid.
    fn validate(&self, component: &C, task: &T, ctx: &dyn Context) -> TaskValidity;
}

/// A type-erased re-validation hook the pure [`close_transaction`](crate::node::NodeTree::close_transaction) calls for each
/// persisted task (Requirement 7.3).
///
/// The pure node tree cannot run a typed [`TaskValidator`] — at close it holds
/// type-erased [`ScheduledTask`]s, not concrete components. So the runtime, which
/// *can* deserialize a task and its owning component, supplies an
/// `OutboxValidator`: given the node's encoded path and a persisted task, it
/// returns the [`TaskValidity`]. This keeps the substrate generic while the
/// validate-then-drop *policy* stays in the pure crate.
///
/// A blanket impl is provided for `Fn(&[u8], &ScheduledTask) -> TaskValidity`, so
/// tests and simple callers can pass a closure.
pub trait OutboxValidator {
    /// Decide whether the persisted `task` on the node at `encoded_path` survives.
    /// Errors abort transition close; they must never be treated as a stale-task
    /// drop, which would silently erase work whose handler cannot be resolved.
    fn validate(
        &self,
        encoded_path: &[u8],
        task: &ScheduledTask,
    ) -> Result<TaskValidity, ChasmError>;
}

impl<F> OutboxValidator for F
where
    F: Fn(&[u8], &ScheduledTask) -> TaskValidity,
{
    fn validate(
        &self,
        encoded_path: &[u8],
        task: &ScheduledTask,
    ) -> Result<TaskValidity, ChasmError> {
        Ok(self(encoded_path, task))
    }
}

/// An [`OutboxValidator`] that keeps every task. The default re-validation policy
/// when no component-specific validators are wired (e.g. substrate-level tests and
/// the engine before any library registers validators).
#[derive(Debug, Clone, Copy, Default)]
pub struct RetainAllValidator;

impl OutboxValidator for RetainAllValidator {
    fn validate(
        &self,
        _encoded_path: &[u8],
        _task: &ScheduledTask,
    ) -> Result<TaskValidity, ChasmError> {
        Ok(TaskValidity::Valid)
    }
}

/// Validates one root's outbox through its registered typed handlers. The encoded
/// path is intentionally unused: materialization supports a single root proto
/// (extension-archetypes decision D4), not arbitrary child components.
pub struct RegistryOutboxValidator<'a> {
    /// Frozen startup registry containing the root's handlers.
    pub registry: &'a Registry,
    /// Registered root archetype id.
    pub component_type_id: u32,
    /// Root data after the transition's component mutations.
    pub data: &'a [u8],
    /// Read-only transition context for deterministic validation.
    pub ctx: &'a dyn Context,
}

impl std::fmt::Debug for RegistryOutboxValidator<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistryOutboxValidator")
            .field("component_type_id", &self.component_type_id)
            .finish_non_exhaustive()
    }
}

impl OutboxValidator for RegistryOutboxValidator<'_> {
    fn validate(
        &self,
        _encoded_path: &[u8],
        task: &ScheduledTask,
    ) -> Result<TaskValidity, ChasmError> {
        self.registry
            .validate_task(self.component_type_id, self.data, task, self.ctx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(kind: TaskKind, offset: u32, fire_at: Option<i64>) -> ScheduledTask {
        ScheduledTask {
            kind,
            task_type_id: 1,
            payload: vec![1, 2, 3],
            fire_at_unix_nanos: fire_at,
            id: TaskId::new(VersionedTransition::new(1, offset as i64), offset),
        }
    }

    #[test]
    fn outbox_push_routes_by_kind() {
        let mut outbox = TaskOutbox::new();
        assert!(outbox.is_empty());
        outbox.push(task(TaskKind::Pure, 0, Some(100)));
        outbox.push(task(TaskKind::SideEffect, 1, None));
        assert_eq!(outbox.pure_tasks.len(), 1);
        assert_eq!(outbox.side_effect_tasks.len(), 1);
        assert_eq!(outbox.len(), 2);
        assert!(outbox.pure_tasks[0].is_pure());
        assert!(outbox.side_effect_tasks[0].is_side_effect());
    }

    #[test]
    fn earliest_pure_deadline_ignores_undeadlined_and_side_effect() {
        let mut outbox = TaskOutbox::new();
        outbox.push(task(TaskKind::Pure, 0, Some(300)));
        outbox.push(task(TaskKind::Pure, 1, None));
        outbox.push(task(TaskKind::Pure, 2, Some(150)));
        outbox.push(task(TaskKind::SideEffect, 3, Some(1))); // not a pure deadline
        assert_eq!(outbox.earliest_pure_deadline(), Some(150));
    }

    #[test]
    fn outbox_round_trips_through_serde() {
        let mut outbox = TaskOutbox::new();
        outbox.push(task(TaskKind::Pure, 0, Some(100)));
        outbox.push(task(TaskKind::SideEffect, 1, None));
        let json = serde_json::to_string(&outbox).expect("serialize");
        let back: TaskOutbox = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(outbox, back);
    }

    #[test]
    fn closure_and_retain_all_validators_agree_on_keep() {
        let t = task(TaskKind::Pure, 0, Some(1));
        let keep = |_path: &[u8], _t: &ScheduledTask| TaskValidity::Valid;
        assert_eq!(
            OutboxValidator::validate(&keep, b"$state", &t).unwrap(),
            TaskValidity::Valid
        );
        assert_eq!(
            RetainAllValidator.validate(b"$state", &t).unwrap(),
            TaskValidity::Valid
        );
    }

    #[test]
    fn closure_validator_can_drop() {
        let t = task(TaskKind::Pure, 0, Some(1));
        let drop_pure = |_path: &[u8], t: &ScheduledTask| {
            if t.is_pure() {
                TaskValidity::Drop
            } else {
                TaskValidity::Valid
            }
        };
        assert_eq!(
            OutboxValidator::validate(&drop_pure, b"$state", &t).unwrap(),
            TaskValidity::Drop
        );
    }
    #[test]
    fn start_activity_task_preserves_all_fields_in_its_prost_codec() {
        let task = StartActivityTask {
            activity_id: "job".into(),
            activity_type: "compile".into(),
            task_queue: "queue".into(),
            input: vec![0, 255],
            header: vec![1, 2],
            retry_policy: vec![3, 4],
            schedule_to_start_nanos: 10,
            schedule_to_close_nanos: 20,
            start_to_close_nanos: 30,
            heartbeat_nanos: 40,
            version_target: Some(DeploymentVersionTarget {
                deployment_name: "worker".into(),
                build_id: "release".into(),
            }),
        };
        let bytes = Task::encode(&task).unwrap();
        assert_eq!(<StartActivityTask as Task>::decode(&bytes).unwrap(), task);
        assert_eq!(
            serde_json::from_str::<StartActivityTask>(&serde_json::to_string(&task).unwrap())
                .unwrap(),
            task
        );
        assert!(<StartActivityTask as Task>::decode(&[0xff]).is_err());
        assert_eq!(task.fire_at(), None);
        assert_eq!(
            Task::encode(&StartActivityTask {
                activity_id: "x".into(),
                ..Default::default()
            })
            .unwrap(),
            vec![10, 1, b'x']
        );
        assert_eq!(
            prost::Message::encode_to_vec(&DeploymentVersionTarget {
                deployment_name: "a".into(),
                build_id: "b".into()
            }),
            vec![10, 1, b'a', 18, 1, b'b']
        );
        for outcome in [
            TaskOutcome::Completed { payload: vec![1] },
            TaskOutcome::Failed {
                failure: vec![2],
                retryable: true,
            },
            TaskOutcome::Canceled { details: vec![3] },
            TaskOutcome::TimedOut { timeout_type: 17 },
            TaskOutcome::Terminated,
        ] {
            assert_eq!(
                serde_json::from_str::<TaskOutcome>(&serde_json::to_string(&outcome).unwrap())
                    .unwrap(),
                outcome
            );
        }
    }
}
