//! The standalone-activity edge bridge: public `*ActivityExecution` operations
//! translated to CHASM engine calls, gated by `activity.enableStandalone`.
//!
//! This is the thin translation seam (`crates/tokeira-edge/AGENTS.md`): it admits
//! and validates requests, applies the per-namespace enable gate, drives the CHASM
//! [`ChasmEngine`] (the durable semantics live there and in `tokeira-chasm-activity`),
//! and maps [`ChasmError`] to the edge's [`EdgeError`]. It does **not** implement
//! activity semantics.
//!
//! ## The gate (Requirement 11.10)
//!
//! Standalone activities are ahead of the `v1.31.0` baseline behavioural claim, so
//! they are gated off by default. When disabled, every operation returns
//! [`EdgeError::Unimplemented`] carrying the targeted-release message
//! ("Standalone activity is disabled"), ground-truthed to
//! `chasm/lib/activity/frontend.go:36 @ v1.31.0`
//! (`serviceerror.NewUnimplemented("Standalone activity is disabled")`). With the
//! gate off the wire answer is `UNIMPLEMENTED`, which matches the conformance
//! matrix; the feature only serves once an operator enables it (the
//! conformance-override gate declares that deviation — a separate finish-line item).
//!
//! ## Surface
//!
//! - Public frontend operations: [`start`](ActivityBridge::start),
//!   [`describe`](ActivityBridge::describe), [`poll`](ActivityBridge::poll),
//!   [`request_cancel`](ActivityBridge::request_cancel),
//!   [`terminate`](ActivityBridge::terminate), [`delete`](ActivityBridge::delete).
//! - Worker-facing lifecycle drivers (used by the end-to-end tests and the
//!   `scenarios` exerciser to advance an activity the way a polling worker would):
//!   [`record_started`](ActivityBridge::record_started),
//!   [`record_completed`](ActivityBridge::record_completed),
//!   [`record_failed`](ActivityBridge::record_failed).

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};

use prost::Message as _;
use tokeira_chasm::{
    BusinessIdPolicy, ChasmError, Component as _, ComponentRef, DispatchableTask, ExecutionKey,
    VersionedTransition,
};
use tokeira_chasm_activity::{
    ActivityConfig, ActivityEvent, ActivityExecution, ActivityRequest, ActivityState,
    ActivityStatus, DISPATCH_TASK_ID, DispatchTask, RetryOutcome, TimeoutType, due_timeout,
    next_timeout_deadline, retry_decision, validate_and_normalize,
};
use tokeira_runtime::chasm::{
    ChasmEngine, DispatchSink, Engine, PollOutcome, PollRequest, TimeoutEvaluator, TypedEngine,
};
use tokeira_types::ArchetypeId;

use crate::errors::{EdgeError, EdgeResult};

/// Temporal's namespace-scoped standalone-activity admission setting.
///
/// `chasm/lib/activity/config.go @ v1.31.0` defines the false default and
/// `chasm/lib/activity/frontend.go @ v1.31.0` consults it on every request.
#[cfg(feature = "conformance")]
const STANDALONE_ACTIVITIES_KEY: &str = "activity.enableStandalone";
#[cfg(feature = "conformance")]
const ACTIVITY_LONG_POLL_TIMEOUT_KEY: &str = "activity.longPollTimeout";
#[cfg(feature = "conformance")]
const ACTIVITY_LONG_POLL_BUFFER_KEY: &str = "activity.longPollBuffer";

#[cfg(not(feature = "conformance"))]
fn standalone_activities_enabled(configured: bool) -> bool {
    configured
}

#[cfg(feature = "conformance")]
fn standalone_activities_enabled(configured: bool) -> bool {
    // The override is compiled out of production. Reading it live is required
    // because the functional corpus applies the namespace setting after the
    // out-of-process server has started.
    tokeira_conformance::overrides()
        .get_bool(STANDALONE_ACTIVITIES_KEY)
        .unwrap_or(configured)
}

#[cfg(not(feature = "conformance"))]
fn activity_long_poll_timeout(configured: std::time::Duration) -> std::time::Duration {
    configured
}

#[cfg(feature = "conformance")]
fn activity_long_poll_timeout(configured: std::time::Duration) -> std::time::Duration {
    tokeira_conformance::overrides()
        .get_duration(ACTIVITY_LONG_POLL_TIMEOUT_KEY)
        .unwrap_or(configured)
}

#[cfg(not(feature = "conformance"))]
fn activity_long_poll_buffer(configured: std::time::Duration) -> std::time::Duration {
    configured
}

#[cfg(feature = "conformance")]
fn activity_long_poll_buffer(configured: std::time::Duration) -> std::time::Duration {
    tokeira_conformance::overrides()
        .get_duration(ACTIVITY_LONG_POLL_BUFFER_KEY)
        .unwrap_or(configured)
}

/// Attributes for starting a standalone activity (the edge-domain form the gRPC
/// handler translates the proto request into).
#[derive(Debug, Clone)]
pub struct StartActivity {
    /// Namespace id (UUID string for DSQL; any string for the in-memory store).
    pub namespace_id: String,
    /// Application-level activity id (the execution's business id).
    pub activity_id: String,
    /// Run id naming this instance (UUID string; the handler generates it).
    pub run_id: String,
    /// Activity type name.
    pub activity_type: String,
    /// User-defined task queue (required).
    pub task_queue: String,
    /// Serialized activity input payload.
    pub input: Vec<u8>,
    /// Requested schedule-to-start timeout in nanoseconds (`0` = unset).
    pub schedule_to_start_nanos: i64,
    /// Requested schedule-to-close timeout in nanoseconds (`0` = unset).
    pub schedule_to_close_nanos: i64,
    /// Requested start-to-close timeout in nanoseconds (`0` = unset).
    pub start_to_close_nanos: i64,
    /// Requested heartbeat timeout in nanoseconds (`0` = unset).
    pub heartbeat_nanos: i64,
    /// Enclosing run timeout in nanoseconds (`0` = unset); the cap for the above.
    pub run_timeout_nanos: i64,
    /// Originating request id, if supplied.
    pub request_id: Option<String>,
    /// Business-id reuse/conflict policy mapped from the request's
    /// `ActivityIdReusePolicy`/`ActivityIdConflictPolicy` (`handler.go:19-25 @
    /// v1.31.0`). Governs whether a new Start may supersede the current run.
    pub policy: BusinessIdPolicy,
    /// Encoded `Header` from the Start request (opaque; echoed on describe). Empty
    /// when unset.
    pub header: Vec<u8>,
    /// Encoded `RetryPolicy` from the Start request (opaque; echoed on describe).
    /// Empty when unset.
    pub retry_policy: Vec<u8>,
    /// Retry-policy initial interval in nanoseconds, with Temporal's defaults already
    /// applied at the edge (`retrypolicy.EnsureDefaults @ v1.31.0`: `1s` when unset).
    /// Folded out of `retry_policy` so the pure retry decision stays proto-free.
    pub retry_initial_interval_nanos: i64,
    /// Retry-policy backoff coefficient, defaulted to `2.0` when unset.
    pub retry_backoff_coefficient: f64,
    /// Retry-policy maximum interval cap in nanoseconds, defaulted to
    /// `100 × initial_interval` when unset (`0` would mean "no cap", but the default
    /// is always applied here).
    pub retry_maximum_interval_nanos: i64,
    /// Retry-policy maximum attempts (`0` = unlimited). The retry bound the pure
    /// decision enforces.
    pub maximum_attempts: i32,
    /// Encoded `Priority` from the Start request (opaque; echoed on describe). Empty
    /// when unset.
    pub priority: Vec<u8>,
    /// Encoded `SearchAttributes` from the Start request (opaque; echoed on
    /// describe). Empty when unset.
    pub search_attributes: Vec<u8>,
    /// Encoded `UserMetadata` from the Start request (opaque; echoed on describe).
    /// Empty when unset.
    pub user_metadata: Vec<u8>,
}

/// Outcome of [`ActivityBridge::start`].
///
/// `started` mirrors the targeted release's `StartActivityExecutionResponse.started`
/// (`result.Created` @ v1.31.0): `true` when a fresh run was created and scheduled,
/// `false` when the reuse/conflict policy (`UseExisting`) or request-id idempotency
/// returned an already-live run.
#[derive(Debug, Clone)]
pub struct StartActivityOutcome {
    /// Reference to the created — or existing — activity run.
    pub reference: ComponentRef,
    /// Whether a new run was created (vs an existing run returned).
    pub started: bool,
}

/// A read view of an activity execution (the source for `Describe`/`Poll`
/// responses).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ActivityDescription {
    /// Current status.
    pub status: ActivityStatus,
    /// Current attempt.
    pub attempt: i32,
    /// Activity type name.
    pub activity_type: String,
    /// Task queue the activity is scheduled on.
    pub task_queue: String,
    /// Serialized activity input (the encoded `Payloads` envelope).
    pub input: Vec<u8>,
    /// Result payload (set once completed).
    pub result: Vec<u8>,
    /// Failure message (set on a terminal failure).
    pub failure: String,
    /// Full encoded `Failure` proto recorded on a worker failure (empty unless the
    /// activity failed with a structured failure) — the source for the describe
    /// outcome's `failure`.
    pub failure_payload: Vec<u8>,
    /// Schedule-to-close timeout in nanoseconds (`0` = unset).
    pub schedule_to_close_nanos: i64,
    /// Schedule-to-start timeout in nanoseconds (`0` = unset).
    pub schedule_to_start_nanos: i64,
    /// Start-to-close timeout in nanoseconds (`0` = unset).
    pub start_to_close_nanos: i64,
    /// Heartbeat timeout in nanoseconds (`0` = unset).
    pub heartbeat_nanos: i64,
    /// Last scheduled time in Unix nanoseconds.
    pub scheduled_time_nanos: i64,
    /// Scheduled time of the current attempt, including retry backoff.
    pub attempt_scheduled_time_nanos: i64,
    /// Last started time in Unix nanoseconds (`0` = not started).
    pub started_time_nanos: i64,
    /// Identity of the worker that polled/started the current attempt (empty until
    /// pickup) — `DescribeActivityExecution.info.last_worker_identity`.
    pub worker_identity: String,
    /// Close time in Unix nanoseconds (`0` = not closed) — `info.close_time`.
    pub close_time_nanos: i64,
    /// Encoded `Header` echoed on `info.header` (empty when unset).
    pub header: Vec<u8>,
    /// Encoded `RetryPolicy` echoed on `info.retry_policy` (empty when unset).
    pub retry_policy: Vec<u8>,
    /// Encoded `Priority` echoed on `info.priority` (empty when unset).
    pub priority: Vec<u8>,
    /// Encoded `SearchAttributes` echoed on `info.search_attributes` (empty when
    /// unset).
    pub search_attributes: Vec<u8>,
    /// Encoded `UserMetadata` echoed on `info.user_metadata` (empty when unset).
    pub user_metadata: Vec<u8>,
    /// The execution clock, used as the caller's long-poll token.
    pub execution_vt: VersionedTransition,
    /// Encoded `Payloads` of the worker's last heartbeat details (empty when none) —
    /// echoed on `info.heartbeat_details`.
    pub heartbeat_details: Vec<u8>,
    /// The cancel request's `request_id` (empty until cancel requested) — the
    /// idempotency/conflict key for a repeated `RequestCancelActivityExecution`.
    pub cancel_request_id: String,
    /// The cancel request's reason (empty until cancel requested) — echoed on
    /// `info.canceled_reason`.
    pub cancel_reason: String,
    /// Identity of the client that requested cancellation.
    pub cancel_identity: String,
    /// Encoded cancellation acknowledgement details.
    pub canceled_details: Vec<u8>,
    /// The terminate request's `request_id` (empty until terminated) — the
    /// idempotency/conflict key for a repeated `TerminateActivityExecution`.
    pub terminate_request_id: String,
    /// Identity of the client that terminated the activity.
    pub terminate_identity: String,
    /// Completion time of the previous attempt.
    pub last_attempt_complete_time_nanos: i64,
    /// Backoff interval selected for the current retry.
    pub current_retry_interval_nanos: i64,
}

/// A worker-facing activity task: the dispatched attempt a polling worker receives,
/// already marked `Started` (the bridge records the start before returning, the way
/// matching + `RecordActivityTaskStarted` do server-side).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolledActivityTask {
    /// Opaque token the worker echoes back on completion/failure; encodes the
    /// execution key and the attempt stamp it was issued for.
    pub task_token: Vec<u8>,
    /// Application-level activity id.
    pub activity_id: String,
    /// Run id of the dispatched instance.
    pub run_id: String,
    /// Activity type name.
    pub activity_type: String,
    /// Serialized activity input (the encoded `Payloads` envelope start carried).
    pub input: Vec<u8>,
    /// The attempt number this task is for (1-based).
    pub attempt: i32,
    /// Schedule-to-close timeout in nanoseconds (`0` = unset) — echoed on the poll
    /// response so the worker sees the same timeouts it started with
    /// (`standalone_activity_test.go:326`).
    pub schedule_to_close_nanos: i64,
    /// Start-to-close timeout in nanoseconds (`0` = unset) — `poll.start_to_close_timeout`.
    pub start_to_close_nanos: i64,
    /// Heartbeat timeout in nanoseconds (`0` = unset) — `poll.heartbeat_timeout`.
    pub heartbeat_nanos: i64,
    /// Schedule time in Unix nanoseconds — `poll.scheduled_time`.
    pub scheduled_time_nanos: i64,
    /// Scheduled time of this attempt, including retry backoff.
    pub attempt_scheduled_time_nanos: i64,
    /// Started time in Unix nanoseconds (the pickup time recorded by this poll) —
    /// `poll.started_time`.
    pub started_time_nanos: i64,
    /// Encoded `Priority` from the Start request (empty when unset) — `poll.priority`.
    pub priority: Vec<u8>,
    /// Encoded `Header` from the Start request (empty when unset) — `poll.header`.
    pub header: Vec<u8>,
    /// Encoded `Payloads` of the prior attempt's last heartbeat details (empty when
    /// none) — `poll.heartbeat_details`, so a retried attempt sees them.
    pub heartbeat_details: Vec<u8>,
}

/// The worker task token: a wire-compatible mirror of Temporal's server-internal
/// activity task token (`temporal.server.api.token.v1.Task`,
/// `proto/internal/temporal/server/api/token/v1/message.proto @ v1.31.0`), carrying
/// a serialized `ChasmComponentRef` in `component_ref` (field 14). This is the
/// decoded, validated form; the wire form is the two `Proto*` mirrors below.
///
/// Wire compatibility is load-bearing, not cosmetic. The conformance corpus
/// `Deserialize`s the issued token with Temporal's `tasktoken.Serializer`, swaps its
/// `component_ref` for another namespace's, and re-`Serialize`s it
/// (`MismatchedTokenComponentRef`, `standalone_activity_test.go:734 @ v1.31.0`); an
/// SDK echoes the token verbatim on `RespondActivityTask*`. tokeira does not vendor
/// the server-internal protos, so the subset the standalone path needs is
/// hand-defined to the stable field numbers — the same approach as the typed-error
/// encoding in `grpc/errors.rs`.
#[derive(Debug, Clone)]
struct ActivityTaskToken {
    /// Top-level `Task.namespace_id` (field 1) — the namespace-validator
    /// interceptor's check (`errTaskTokenNamespaceMismatch`,
    /// `common/rpc/interceptor/namespace_validator.go:354 @ v1.31.0`). Issued equal
    /// to the component ref's namespace; the corpus tampers them apart.
    namespace_id: String,
    /// The component ref's namespace (`ChasmComponentRef.namespace_id`) — the
    /// `validateActivityTaskToken` check (`activity.go:804 @ v1.31.0`). The
    /// activity is addressed by the ref, so this is also the addressed namespace.
    ref_namespace_id: String,
    /// The addressed activity id (`ChasmComponentRef.business_id`); also the id in
    /// the `NotFound "activity not found for ID: <id>"` message (the engine's
    /// `convertNotFoundError` uses `ref.BusinessID`, `chasm_engine.go:1320`).
    activity_id: String,
    /// Run id of the dispatched instance (`ChasmComponentRef.run_id`).
    run_id: String,
    /// The attempt the dispatch was issued for (`Task.attempt`, field 5). The
    /// v1.31.0 fence is `token.Attempt != LastAttempt.Count` (`activity.go:790`);
    /// in tokeira `attempt` and `stamp` move together (both bump only on
    /// Scheduled/Rescheduled), so this is the same fence the timers use.
    attempt: i32,
}

impl ActivityTaskToken {
    /// Encode the wire token a poll hands a worker: a marshaled `Task` whose
    /// `component_ref` is a marshaled `ChasmComponentRef`. `namespace_id`,
    /// `activity_id`, and `run_id` are written into both the top-level `Task` and
    /// the embedded ref so the two namespace checks see consistent values until the
    /// corpus deliberately diverges them.
    fn encode(
        namespace_id: &str,
        activity_id: &str,
        run_id: &str,
        attempt: i32,
        archetype_id: u32,
    ) -> EdgeResult<Vec<u8>> {
        let component_ref = ProtoComponentRef {
            namespace_id: namespace_id.to_owned(),
            business_id: activity_id.to_owned(),
            run_id: run_id.to_owned(),
            archetype_id,
        }
        .encode_to_vec();
        Ok(ProtoTaskToken {
            namespace_id: namespace_id.to_owned(),
            run_id: run_id.to_owned(),
            attempt,
            activity_id: activity_id.to_owned(),
            component_ref,
        }
        .encode_to_vec())
    }

    /// Decode a wire token into the validated form. Fails `InvalidArgument` if the
    /// bytes are not a `Task` carrying a well-formed `ChasmComponentRef` — the
    /// `errDeserializingToken` / `malformed token` paths (`activity.go:797`).
    fn decode(bytes: &[u8]) -> EdgeResult<Self> {
        let (task, component_ref) = Self::decode_proto(bytes)
            .ok_or_else(|| EdgeError::BadRequest("malformed activity task token".to_owned()))?;
        Ok(Self {
            namespace_id: task.namespace_id,
            ref_namespace_id: component_ref.namespace_id,
            activity_id: component_ref.business_id,
            run_id: component_ref.run_id,
            attempt: task.attempt,
        })
    }

    /// Parse the wire bytes into the `Task` and its embedded `ChasmComponentRef`,
    /// or `None` if the bytes are not a standalone-activity token. A standalone
    /// token is positively keyed on a present, well-formed `component_ref` with a
    /// namespace and business id (mirrors Temporal's `len(GetComponentRef()) > 0`
    /// routing, `service/frontend/workflow_handler.go:1402 @ v1.31.0`); this is what
    /// distinguishes it from the workflow-activity token sharing the same RPC.
    fn decode_proto(bytes: &[u8]) -> Option<(ProtoTaskToken, ProtoComponentRef)> {
        let task = ProtoTaskToken::decode(bytes).ok()?;
        if task.component_ref.is_empty() {
            return None;
        }
        let component_ref = ProtoComponentRef::decode(task.component_ref.as_slice()).ok()?;
        if component_ref.namespace_id.is_empty() || component_ref.business_id.is_empty() {
            return None;
        }
        Some((task, component_ref))
    }

    fn execution_key(&self) -> ExecutionKey {
        ExecutionKey::new(
            self.ref_namespace_id.clone(),
            self.activity_id.clone(),
            self.run_id.clone(),
        )
    }
}

/// Minimal mirror of `temporal.server.api.token.v1.Task` (`message.proto @
/// v1.31.0`). Only the fields the standalone-activity path reads or sets are
/// declared, at their on-wire tags — protobuf is tag-keyed, so an unset field
/// (e.g. `workflow_id`, `scheduled_event_id`) round-trips through the SDK and
/// Temporal's serializer untouched.
#[derive(Clone, PartialEq, ::prost::Message)]
struct ProtoTaskToken {
    #[prost(string, tag = "1")]
    namespace_id: String,
    #[prost(string, tag = "3")]
    run_id: String,
    #[prost(int32, tag = "5")]
    attempt: i32,
    #[prost(string, tag = "6")]
    activity_id: String,
    #[prost(bytes = "vec", tag = "14")]
    component_ref: Vec<u8>,
}

/// Minimal mirror of `temporal.server.api.persistence.v1.ChasmComponentRef`
/// (`chasm.proto @ v1.31.0`), the value of the task token's `component_ref`. Only
/// the identity fields are carried; the namespace is what `validateActivityTaskToken`
/// checks, and `archetype_id` keeps the ref faithful to what `ComponentRef.Serialize`
/// emits (`chasm/ref.go:99 @ v1.31.0`).
#[derive(Clone, PartialEq, ::prost::Message)]
struct ProtoComponentRef {
    #[prost(string, tag = "1")]
    namespace_id: String,
    #[prost(string, tag = "2")]
    business_id: String,
    #[prost(string, tag = "3")]
    run_id: String,
    #[prost(uint32, tag = "4")]
    archetype_id: u32,
}

/// One queued dispatch: the execution to run, the attempt stamp it was scheduled
/// for, and the time it becomes pollable (`None` = immediately). A worker poll reaps
/// stale entries (stamp/status no longer current) and skips not-yet-due entries
/// (a backoff-delayed retry dispatch) rather than dispatching them.
#[derive(Debug, Clone)]
struct DispatchEntry {
    key: ExecutionKey,
    stamp: i64,
    /// Earliest Unix-nanosecond time this dispatch may be handed to a worker. A
    /// retry stages this at `now + backoff` so the new attempt is not pollable until
    /// its backoff elapses (`statemachine.go:119 @ v1.31.0`); the first attempt
    /// carries `None` (immediate).
    fire_at: Option<i64>,
}

/// The matching-side activity queue: a [`DispatchSink`] the CHASM engine hands
/// committed dispatch tasks, fanned out into per-task-queue FIFOs that a worker
/// drains via [`ActivityBridge::poll_activity_task`].
///
/// It is shared (behind an `Arc`) between the engine — which holds it as its
/// dispatch sink — and the [`ActivityBridge`], which drains it. This is the
/// derived-effect boundary: history is authority; this queue is disposable and can
/// be rebuilt from the durable `Scheduled` state, so losing it costs at most a
/// redispatch, never correctness.
#[derive(Debug, Default)]
pub struct ActivityDispatchQueue {
    queues: Mutex<HashMap<String, VecDeque<DispatchEntry>>>,
    dispatch_available: tokio::sync::Notify,
}

impl ActivityDispatchQueue {
    /// Construct an empty queue.
    pub fn new() -> Self {
        Self::default()
    }

    fn enqueue(&self, task_queue: String, entry: DispatchEntry) {
        if let Ok(mut queues) = self.queues.lock() {
            queues.entry(task_queue).or_default().push_back(entry);
            // A timeout may produce a retry while a worker is long-polling this
            // standalone queue. One committed dispatch wakes one poller.
            self.dispatch_available.notify_one();
        }
    }

    /// Pop the first dispatch on `task_queue` that is due at `now` (its `fire_at` is
    /// unset or `<= now`), preserving order for the rest. A not-yet-due retry
    /// dispatch is left in place (skipped, not removed) so a later poll past its
    /// backoff observes it; this is the pull-side of the backoff-delayed dispatch
    /// (Stage 3.2), so the runtime sweeper does not need to "release" delayed
    /// dispatches separately.
    fn dequeue_due(&self, task_queue: &str, now: i64) -> Option<DispatchEntry> {
        let mut queues = self.queues.lock().ok()?;
        let queue = queues.get_mut(task_queue)?;
        let pos = queue
            .iter()
            .position(|entry| entry.fire_at.is_none_or(|at| at <= now))?;
        queue.remove(pos)
    }

    /// Return the earliest delayed dispatch deadline on `task_queue`.
    fn next_due_at(&self, task_queue: &str) -> Option<i64> {
        let queues = self.queues.lock().ok()?;
        queues
            .get(task_queue)?
            .iter()
            .filter_map(|entry| entry.fire_at)
            .min()
    }

    fn has_seen_queue(&self, task_queue: &str) -> bool {
        self.queues
            .lock()
            .is_ok_and(|queues| queues.contains_key(task_queue))
    }
}

#[async_trait::async_trait]
impl DispatchSink for ActivityDispatchQueue {
    async fn dispatch(
        &self,
        key: &ExecutionKey,
        tasks: Vec<DispatchableTask>,
    ) -> anyhow::Result<()> {
        for task in tasks {
            // Only the activity dispatch side-effect task routes to a worker queue;
            // any other side-effect id is not ours to enqueue.
            if task.task.task_type_id == DISPATCH_TASK_ID {
                let dispatch = DispatchTask::decode(&task.task.payload)
                    .map_err(|e| anyhow::anyhow!("decode dispatch task: {e}"))?;
                self.enqueue(
                    dispatch.task_queue,
                    DispatchEntry {
                        key: key.clone(),
                        stamp: dispatch.stamp,
                        // A retry dispatch carries its backoff release time as the
                        // scheduled task's `fire_at`; the first attempt has none.
                        fire_at: task.task.fire_at_unix_nanos,
                    },
                );
            }
        }
        Ok(())
    }
}

/// The standalone-activity bridge over a CHASM engine.
pub struct ActivityBridge {
    engine: Arc<ChasmEngine>,
    config: ActivityConfig,
    max_id_length: usize,
    archetype_id: u32,
    dispatch_queue: Option<Arc<ActivityDispatchQueue>>,
}

impl std::fmt::Debug for ActivityBridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActivityBridge")
            .field("enabled", &self.config.enable_standalone)
            .field("max_id_length", &self.max_id_length)
            .finish_non_exhaustive()
    }
}

impl ActivityBridge {
    /// Build a bridge over `engine` (whose registry must include the `activity`
    /// library) with the given config and id-length limit.
    pub fn new(engine: Arc<ChasmEngine>, config: ActivityConfig, max_id_length: usize) -> Self {
        let archetype_id = engine
            .registry()
            .archetype_id(ActivityExecution::FQN)
            .unwrap_or(0);
        Self {
            engine,
            config,
            max_id_length,
            archetype_id,
            dispatch_queue: None,
        }
    }

    /// Attach the dispatch queue the engine routes committed dispatch tasks into,
    /// enabling the worker poll path. The same `Arc` must be the engine's
    /// [`DispatchSink`] so that what `start` enqueues is what `poll` drains.
    pub fn with_dispatch_queue(mut self, queue: Arc<ActivityDispatchQueue>) -> Self {
        self.dispatch_queue = Some(queue);
        self
    }

    /// Whether standalone activities are enabled.
    pub fn is_enabled(&self) -> bool {
        standalone_activities_enabled(self.config.enable_standalone)
    }

    /// The configured max length for user-supplied ids (activity id, run id),
    /// used by the edge admission validators. Mirrors v1.31.0's
    /// `MaxIDLengthLimit` (`chasm/lib/activity/validator.go @ v1.31.0`).
    pub fn max_id_length(&self) -> usize {
        self.max_id_length
    }

    /// Server-side describe/poll long-poll timeout (`activity.longPollTimeout`,
    /// default 20s @ v1.31.0). The wait returns an empty response when it elapses.
    pub fn long_poll_timeout(&self) -> std::time::Duration {
        activity_long_poll_timeout(self.config.long_poll_timeout)
    }

    /// Slack subtracted from the caller's deadline so an empty long-poll response
    /// is sent before the caller times out (`activity.longPollBuffer`, default 1s
    /// @ v1.31.0).
    pub fn long_poll_buffer(&self) -> std::time::Duration {
        activity_long_poll_buffer(self.config.long_poll_buffer)
    }

    /// The registry-assigned archetype id for the activity component. The
    /// visibility plane is archetype-neutral, so the edge supplies this to scope
    /// `ListActivityExecutions`/`CountActivityExecutions` to activities (Req 13.1).
    pub fn archetype_id(&self) -> ArchetypeId {
        ArchetypeId(self.archetype_id)
    }

    /// Whether `task_token` is one this bridge issued (a standalone-activity
    /// token). The gRPC layer uses this to route a worker response onto the CHASM
    /// path or fall through to the workflow-activity path — standalone and
    /// workflow activities share the `RespondActivityTask*` RPCs. The discriminator
    /// is a present, well-formed `component_ref` (`len(taskToken.GetComponentRef())
    /// > 0`, `service/frontend/workflow_handler.go:1402 @ v1.31.0`).
    pub fn owns_task_token(&self, task_token: &[u8]) -> bool {
        ActivityTaskToken::decode_proto(task_token).is_some()
    }

    /// The enable gate (Requirement 11.10): when off, every operation is rejected
    /// with the targeted-release `Unimplemented` status.
    fn ensure_enabled(&self) -> EdgeResult<()> {
        if self.is_enabled() {
            Ok(())
        } else {
            Err(EdgeError::Unimplemented(
                "Standalone activity is disabled".to_owned(),
            ))
        }
    }

    /// Start (and schedule) a standalone activity, returning a fresh reference to
    /// the scheduled execution (Requirement 11.8). Validates and normalizes the
    /// request first (Requirement 11.9), then creates the root and runs the initial
    /// `Scheduled` transition (which enqueues the dispatch task and the relevant
    /// timers).
    pub async fn start(&self, req: StartActivity) -> EdgeResult<StartActivityOutcome> {
        self.ensure_enabled()?;

        let normalized = validate_and_normalize(&ActivityRequest {
            activity_id: req.activity_id.clone(),
            activity_type: req.activity_type.clone(),
            task_queue: req.task_queue.clone(),
            schedule_to_start_nanos: req.schedule_to_start_nanos,
            schedule_to_close_nanos: req.schedule_to_close_nanos,
            start_to_close_nanos: req.start_to_close_nanos,
            heartbeat_nanos: req.heartbeat_nanos,
            run_timeout_nanos: req.run_timeout_nanos,
            max_id_length: self.max_id_length,
        })
        .map_err(map_chasm_err)?;

        let key = ExecutionKey::new(
            req.namespace_id.clone(),
            req.activity_id.clone(),
            req.run_id.clone(),
        );
        let state = ActivityState {
            activity_id: req.activity_id,
            activity_type: req.activity_type,
            task_queue: req.task_queue,
            input: req.input,
            schedule_to_start_nanos: normalized.schedule_to_start_nanos,
            schedule_to_close_nanos: normalized.schedule_to_close_nanos,
            start_to_close_nanos: normalized.start_to_close_nanos,
            heartbeat_nanos: normalized.heartbeat_nanos,
            header: req.header,
            retry_policy: req.retry_policy,
            retry_initial_interval_nanos: req.retry_initial_interval_nanos,
            retry_backoff_coefficient: req.retry_backoff_coefficient,
            retry_maximum_interval_nanos: req.retry_maximum_interval_nanos,
            maximum_attempts: req.maximum_attempts,
            priority: req.priority,
            search_attributes: req.search_attributes,
            user_metadata: req.user_metadata,
            ..ActivityState::default()
        };

        let typed = TypedEngine::<ActivityExecution>::new(&self.engine);
        let outcome = typed
            .start(key, state, req.request_id, req.policy)
            .await
            .map_err(map_chasm_err)?;
        if !outcome.created {
            // UseExisting / same-request-id idempotency: the policy returned an
            // existing run, which is already scheduled — do NOT re-run the Scheduled
            // transition (it would illegally re-schedule a live activity). `started`
            // is false, mirroring `result.Created == false` →
            // `StartActivityExecutionResponse.started` (`handler.go:101 @ v1.31.0`).
            return Ok(StartActivityOutcome {
                reference: outcome.reference,
                started: false,
            });
        }
        // A fresh run: the initial Scheduled transition bumps attempt/stamp and
        // schedules the dispatch task + schedule-to-start/close timers.
        let (_, scheduled) = typed
            .update(&outcome.reference, |activity, ctx| {
                activity.apply(ActivityEvent::Scheduled, ctx)
            })
            .await
            .map_err(map_chasm_err)?;
        Ok(StartActivityOutcome {
            reference: scheduled.reference,
            started: true,
        })
    }

    /// Describe an activity execution (Requirement 11.8).
    pub async fn describe(&self, key: ExecutionKey) -> EdgeResult<ActivityDescription> {
        self.ensure_enabled()?;
        let snapshot = self
            .engine
            .read_component(&key)
            .await
            .map_err(|e| map_activity_not_found(e, &key))?;
        description_from(snapshot.data, snapshot.execution_vt)
    }

    /// Monotonic long-poll: resolve when the activity's VT advances past `since`,
    /// else `None` on deadline (Requirement 11.8, 6.5).
    pub async fn poll(
        &self,
        key: ExecutionKey,
        since: VersionedTransition,
    ) -> EdgeResult<Option<ActivityDescription>> {
        self.ensure_enabled()?;
        match self
            .engine
            .poll_component(PollRequest { key, since })
            .await
            .map_err(map_chasm_err)?
        {
            PollOutcome::Advanced(read) => {
                Ok(Some(description_from(read.data, read.execution_vt)?))
            }
            PollOutcome::Empty => Ok(None),
        }
    }

    /// Long-poll for the terminal outcome (`PollActivityExecution`, which
    /// "long-polls for activity outcome" — `chasm/lib/activity/frontend.go @
    /// v1.31.0`). Blocks across VT advances until the activity reaches a terminal
    /// status, then returns its description. If the long-poll budget elapses before
    /// a terminal status is reached, returns the current (non-terminal) description
    /// so the caller re-polls — mirroring the workflow long-poll "empty then
    /// resubmit" contract.
    pub async fn poll_outcome(&self, key: ExecutionKey) -> EdgeResult<ActivityDescription> {
        self.ensure_enabled()?;
        loop {
            let current = self.describe(key.clone()).await?;
            if current.status.is_terminal() {
                return Ok(current);
            }
            match self.poll(key.clone(), current.execution_vt).await? {
                // Advanced to a terminal status: done.
                Some(advanced) if advanced.status.is_terminal() => return Ok(advanced),
                // Advanced but still running (e.g. Scheduled → Started): keep waiting.
                Some(_) => continue,
                // Budget elapsed without a terminal status; let the caller re-poll.
                None => return Ok(current),
            }
        }
    }

    /// Request cancellation of an activity (Requirement 11.8), mirroring v1.31.0's
    /// `handleCancellationRequested` (`chasm/lib/activity/activity.go:395-440`):
    /// - already `CANCEL_REQUESTED`: a different `request_id` is `FailedPrecondition`
    ///   ("cancellation already requested with request ID `<id>`"); the same id is an
    ///   idempotent no-op.
    /// - otherwise mark cancel-requested (storing request_id/reason); and if the
    ///   activity was still `SCHEDULED` (no worker holds it), cancel it immediately
    ///   (Scheduled → CancelRequested → Canceled).
    ///
    /// The status read and the transition are two commits, not one: a benign
    /// TOCTOU window the single-client conformance path never exercises. The
    /// dedup/immediate-cancel decision is correctness-critical and cited above.
    pub async fn request_cancel(
        &self,
        key: ExecutionKey,
        identity: String,
        request_id: String,
        reason: String,
    ) -> EdgeResult<()> {
        self.ensure_enabled()?;
        let description = self.describe(key.clone()).await?;
        if description.status == ActivityStatus::CancelRequested {
            if description.cancel_request_id != request_id {
                return Err(EdgeError::FailedPrecondition(format!(
                    "cancellation already requested with request ID {}",
                    description.cancel_request_id
                )));
            }
            return Ok(());
        }
        let was_scheduled = description.status == ActivityStatus::Scheduled;
        self.apply_event(
            key.clone(),
            ActivityEvent::CancelRequested {
                identity,
                request_id,
                reason,
            },
        )
        .await?;
        if was_scheduled {
            self.apply_event(
                key,
                ActivityEvent::Canceled {
                    details: Vec::new(),
                },
            )
            .await?;
        }
        Ok(())
    }

    /// Terminate an activity (Requirement 11.8), mirroring v1.31.0's `Terminate`
    /// (`chasm/lib/activity/activity.go:359-381`): an already-`TERMINATED` activity
    /// with a different `request_id` is `FailedPrecondition` ("already terminated
    /// with request ID `<id>`"); the same id is an idempotent no-op.
    pub async fn terminate(
        &self,
        key: ExecutionKey,
        reason: String,
        request_id: String,
        identity: String,
    ) -> EdgeResult<()> {
        self.ensure_enabled()?;
        let description = self.describe(key.clone()).await?;
        if description.status == ActivityStatus::Terminated {
            if description.terminate_request_id != request_id {
                return Err(EdgeError::FailedPrecondition(format!(
                    "already terminated with request ID {}",
                    description.terminate_request_id
                )));
            }
            return Ok(());
        }
        self.apply_event(
            key,
            ActivityEvent::Terminated {
                reason,
                identity,
                request_id,
            },
        )
        .await
    }

    /// Resolve the current run for `activity_id` in `namespace_id` — the run a bare-id
    /// (empty `run_id`) request addresses (`activity-executions-first-class` Req 1).
    /// `None` when there is no current run for the id. Authoritative (engine
    /// current-run pointer), never the visibility projection.
    pub async fn current_run(
        &self,
        namespace_id: &str,
        activity_id: &str,
    ) -> EdgeResult<Option<String>> {
        self.ensure_enabled()?;
        Ok(self
            .engine
            .current_run(namespace_id, activity_id)
            .await
            .map_err(map_chasm_err)?
            .map(|current| current.run_id))
    }

    /// Delete an activity execution's node subtree (Requirement 11.8).
    pub async fn delete(&self, key: ExecutionKey) -> EdgeResult<()> {
        self.ensure_enabled()?;
        // A missing activity is a NotFound, not a silent no-op: confirm the activity
        // exists before deleting so DeleteActivityExecution mirrors v1.31.0
        // (`chasm/lib/activity/frontend.go @ v1.31.0`).
        self.engine
            .read_component(&key)
            .await
            .map_err(|e| map_activity_not_found(e, &key))?;
        self.engine
            .delete_execution(&key)
            .await
            .map_err(map_chasm_err)
    }

    /// Worker-facing: record that a worker started the activity attempt. Used by
    /// the end-to-end tests and the `scenarios` exerciser to advance the lifecycle
    /// the way a polling worker's `RecordActivityTaskStarted` would.
    pub async fn record_started(
        &self,
        key: ExecutionKey,
        started_time_nanos: i64,
        identity: String,
    ) -> EdgeResult<()> {
        self.apply_event(
            key,
            ActivityEvent::Started {
                started_time_nanos,
                identity,
            },
        )
        .await
    }

    /// Worker-facing: record successful completion.
    pub async fn record_completed(
        &self,
        key: ExecutionKey,
        result: Vec<u8>,
        identity: String,
    ) -> EdgeResult<()> {
        self.apply_event(key, ActivityEvent::Completed { result, identity })
            .await
    }

    /// Worker-facing: record a terminal failure. `failure` is the message (for
    /// `info.last_failure` and the quick outcome message); `failure_payload` is the
    /// full encoded `Failure` proto so the describe outcome round-trips it exactly;
    /// `last_heartbeat_details` is the encoded `Payloads` of the worker's last
    /// heartbeat (empty when none), recorded so describe echoes them on
    /// `info.heartbeat_details` (`statemachine.go:220 @ v1.31.0`).
    pub async fn record_failed(
        &self,
        key: ExecutionKey,
        failure: String,
        failure_payload: Vec<u8>,
        last_heartbeat_details: Vec<u8>,
        identity: String,
    ) -> EdgeResult<()> {
        self.ensure_enabled()?;
        let reference = self.activity_ref(key);
        let typed = TypedEngine::<ActivityExecution>::new(&self.engine);
        // The retry-vs-terminal decision is made INSIDE the fenced closure so it runs
        // against the committed live state and re-runs on a conflict. A worker failure
        // is retryable iff it carries an `ApplicationFailureInfo` that is not marked
        // non-retryable and whose type is not in the policy's non-retryable list
        // (`HandleFailed @ v1.31.0`); a retryable failure then defers to the pure
        // `retry_decision` (`shouldRetry`), honouring `NextRetryDelay` as the
        // override interval. Non-retryable, or no retry budget, goes terminal.
        typed
            .update(&reference, move |activity, ctx| {
                let state = activity.activity_state().cloned().unwrap_or_default();
                let now = ctx.now_unix_nanos();
                let (retryable, override_nanos) =
                    classify_worker_failure(&failure_payload, &state.retry_policy);
                let event = match retryable.then(|| retry_decision(&state, now, override_nanos)) {
                    Some(RetryOutcome::Reschedule(interval)) => ActivityEvent::Rescheduled {
                        failure: failure.clone(),
                        identity: identity.clone(),
                        last_heartbeat_details: last_heartbeat_details.clone(),
                        interval_nanos: interval,
                    },
                    // Not retryable, or retryable but out of attempts/budget: terminal.
                    _ => ActivityEvent::Failed {
                        failure: failure.clone(),
                        failure_payload: failure_payload.clone(),
                        identity: identity.clone(),
                        last_heartbeat_details: last_heartbeat_details.clone(),
                    },
                };
                activity.apply(event, ctx)
            })
            .await
            .map_err(map_chasm_err)?;
        Ok(())
    }

    /// Worker-facing: record a heartbeat for the attempt named by `task_token`,
    /// returning whether cancellation has been requested
    /// (`RecordActivityTaskHeartbeat.cancel_requested`). The details are recorded
    /// onto the activity status-preservingly so a later describe (and, once retry
    /// re-dispatch lands, the next attempt) observes them. The token is validated
    /// exactly like the terminal responses (`validate_token`): a stale token on a
    /// completed/superseded activity is `NotFound "activity not found for ID: <id>"`,
    /// and a cross-namespace token is the same `InvalidArgument` the responds use.
    pub async fn record_heartbeat(
        &self,
        task_token: &[u8],
        request_namespace_id: &str,
        details: Vec<u8>,
    ) -> EdgeResult<bool> {
        self.ensure_enabled()?;
        let token = ActivityTaskToken::decode(task_token)?;
        self.validate_token(&token, request_namespace_id).await?;
        let key = token.execution_key();
        self.apply_event(key.clone(), ActivityEvent::Heartbeat { details })
            .await?;
        // cancel_requested reflects a pending RequestCancelActivityExecution: the
        // activity is in CANCEL_REQUESTED until the worker acknowledges
        // (`standalone_activity_test.go:4406`).
        let description = self.describe(key).await?;
        Ok(description.status == ActivityStatus::CancelRequested)
    }

    /// Worker poll: hand the next dispatched attempt on `task_queue` to a worker,
    /// recording its start first (the way matching + `RecordActivityTaskStarted`
    /// do server-side). Returns `None` when the queue is empty.
    ///
    /// Stale entries are reaped, not dispatched: a queued dispatch whose attempt
    /// has advanced, that already left `Scheduled`, or whose execution was deleted
    /// is skipped (validate-then-drop, Requirement 11.7). This MVP poll does not
    /// long-poll — it drains what is already enqueued and returns; `start` enqueues
    /// synchronously in its post-commit, so a poll after a start observes the task.
    pub async fn poll_activity_task(
        &self,
        task_queue: &str,
        worker_identity: &str,
    ) -> EdgeResult<Option<PolledActivityTask>> {
        self.ensure_enabled()?;
        let queue = self.dispatch_queue.as_ref().ok_or_else(|| {
            EdgeError::Internal("activity dispatch queue not attached".to_owned())
        })?;
        // Pull only dispatches that are due now: a backoff-delayed retry dispatch is
        // skipped until its release time, so a worker cannot pick up the next attempt
        // before its backoff elapses (Stage 3.2).
        let now = self.engine.now();
        while let Some(entry) = queue.dequeue_due(task_queue, now) {
            let snapshot = match self.engine.read_component(&entry.key).await {
                Ok(snapshot) => snapshot,
                // A deleted execution leaves a dangling dispatch; drop and continue.
                Err(ChasmError::ExecutionNotFound) => continue,
                Err(error) => return Err(map_chasm_err(error)),
            };
            let Some(bytes) = snapshot.data else { continue };
            let state = ActivityState::decode(bytes.as_slice())
                .map_err(|e| EdgeError::Internal(format!("decode activity state: {e}")))?;
            // Only dispatch the exact attempt the task was scheduled for, and only
            // while still awaiting pickup; anything else is a superseded dispatch.
            if state.stamp != entry.stamp || state.status() != ActivityStatus::Scheduled {
                continue;
            }
            // Record the start before returning, so the lifecycle advances on
            // pickup even if the worker never responds (the start-to-close timer
            // then fences the lost attempt).
            let started_at = self.engine.now();
            self.record_started(entry.key.clone(), started_at, worker_identity.to_owned())
                .await?;
            // The token carries the attempt (== stamp here) as the fence and the
            // activity archetype id in the embedded component ref, so it round-trips
            // through the SDK / Temporal's tasktoken serializer (see ActivityTaskToken).
            let task_token = ActivityTaskToken::encode(
                &entry.key.namespace_id,
                &entry.key.business_id,
                &entry.key.run_id,
                state.attempt,
                self.archetype_id,
            )?;
            return Ok(Some(PolledActivityTask {
                task_token,
                activity_id: state.activity_id,
                run_id: entry.key.run_id.clone(),
                activity_type: state.activity_type,
                input: state.input,
                attempt: state.attempt,
                schedule_to_close_nanos: state.schedule_to_close_nanos,
                start_to_close_nanos: state.start_to_close_nanos,
                heartbeat_nanos: state.heartbeat_nanos,
                scheduled_time_nanos: state.scheduled_time_nanos,
                attempt_scheduled_time_nanos: state.attempt_scheduled_time_nanos,
                // The pickup time this poll just recorded (state was read pre-start,
                // so use the value handed to record_started, not state).
                started_time_nanos: started_at,
                priority: state.priority,
                header: state.header,
                heartbeat_details: state.last_heartbeat_details,
            }));
        }
        Ok(None)
    }

    /// Poll a standalone activity, waiting when this queue has a delayed retry or a
    /// started attempt whose timeout may produce one. A never-seen standalone queue
    /// still returns immediately so the shared RPC can fall through to ordinary
    /// workflow activities.
    pub async fn poll_activity_task_waiting(
        &self,
        task_queue: &str,
        worker_identity: &str,
    ) -> EdgeResult<Option<PolledActivityTask>> {
        let queue = self.dispatch_queue.as_ref().ok_or_else(|| {
            EdgeError::Internal("activity dispatch queue not attached".to_owned())
        })?;
        loop {
            // Register before the empty check so a racing enqueue leaves a permit
            // instead of stranding this long poll.
            let dispatch_available = queue.dispatch_available.notified();
            if let Some(task) = self.poll_activity_task(task_queue, worker_identity).await? {
                return Ok(Some(task));
            }
            if let Some(due_at) = queue.next_due_at(task_queue) {
                let remaining = due_at.saturating_sub(self.engine.now());
                if remaining <= 0 {
                    continue;
                }
                // A retry is committed before it is advertised to matching. Waiting
                // for its release instant prevents an early empty response.
                tokio::time::sleep(std::time::Duration::from_nanos(
                    u64::try_from(remaining).unwrap_or(u64::MAX),
                ))
                .await;
            } else if queue.has_seen_queue(task_queue) {
                // A started attempt can time out and enqueue its retry later.
                dispatch_available.await;
            } else {
                return Ok(None);
            }
        }
    }

    /// Worker-facing: complete the activity attempt named by `task_token`.
    pub async fn respond_activity_task_completed(
        &self,
        task_token: &[u8],
        request_namespace_id: &str,
        result: Vec<u8>,
        identity: String,
    ) -> EdgeResult<()> {
        self.ensure_enabled()?;
        let token = ActivityTaskToken::decode(task_token)?;
        self.validate_token(&token, request_namespace_id).await?;
        self.record_completed(token.execution_key(), result, identity)
            .await
    }

    /// Worker-facing: fail the activity attempt named by `task_token`. `failure` is
    /// the message; `failure_payload` is the full encoded `Failure` proto (so the
    /// describe outcome round-trips the structured failure, not just the message);
    /// `last_heartbeat_details` is the encoded `Payloads` of the worker's last
    /// heartbeat (empty when none).
    pub async fn respond_activity_task_failed(
        &self,
        task_token: &[u8],
        request_namespace_id: &str,
        failure: String,
        failure_payload: Vec<u8>,
        last_heartbeat_details: Vec<u8>,
        identity: String,
    ) -> EdgeResult<()> {
        self.ensure_enabled()?;
        let token = ActivityTaskToken::decode(task_token)?;
        self.validate_token(&token, request_namespace_id).await?;
        self.record_failed(
            token.execution_key(),
            failure,
            failure_payload,
            last_heartbeat_details,
            identity,
        )
        .await
    }

    /// Worker-facing: acknowledge cancellation of the activity attempt named by
    /// `task_token`. Like completed/failed, the worker echoes the dispatch token;
    /// it is validated before the terminal transition (`HandleCanceled` validates
    /// the same token and applies `TransitionCanceled`, `chasm/lib/activity/
    /// activity.go:330 @ v1.31.0`). The transition is legal only from
    /// `CANCEL_REQUESTED` (`statemachine.go:307 @ v1.31.0`); a token naming any
    /// other live state surfaces the engine's illegal-transition error, and a stale
    /// or cross-namespace token is rejected by `validate_token` first.
    pub async fn respond_activity_task_canceled(
        &self,
        task_token: &[u8],
        request_namespace_id: &str,
        details: Vec<u8>,
    ) -> EdgeResult<()> {
        self.ensure_enabled()?;
        let token = ActivityTaskToken::decode(task_token)?;
        self.validate_token(&token, request_namespace_id).await?;
        self.apply_event(token.execution_key(), ActivityEvent::Canceled { details })
            .await
    }

    /// Synthesize the worker token a `RespondActivityTask*ById` request implies.
    ///
    /// v1.31.0's by-id handlers do not carry a worker token; for a standalone
    /// activity (empty workflow id) the frontend builds a `ChasmComponentRef` for
    /// `(namespace, activity_id, run_id)`, wraps it in a task token whose attempt is
    /// fixed at `1`, and routes that synthesized token through the normal
    /// `RespondActivityTaskCompleted`/`Failed`/`Canceled` path
    /// (`service/frontend/workflow_handler.go:1671-1702 @ v1.31.0`). We mirror that
    /// exactly so the by-id and by-token paths share one validation/record path.
    ///
    /// Two deliberate consequences of matching v1.31.0:
    /// - The attempt is hardcoded `1`, so by-id only addresses the first attempt;
    ///   once a retry advances the live attempt the synthesized token is fenced
    ///   stale by `validate_token` (this is Temporal's behaviour, not a tokeira
    ///   limitation).
    /// - tokeira addresses executions by explicit key, so a bare (empty) run id is
    ///   resolved to the current run here, where Temporal defers that to the engine
    ///   when it deserializes the ref. A missing current run is the same
    ///   `NotFound "activity not found for ID: <id>"` the token path returns.
    async fn by_id_token(
        &self,
        namespace_id: &str,
        activity_id: &str,
        run_id: &str,
    ) -> EdgeResult<Vec<u8>> {
        self.ensure_enabled()?;
        let resolved_run = if run_id.is_empty() {
            self.current_run(namespace_id, activity_id)
                .await?
                .ok_or_else(|| {
                    EdgeError::NotFound(format!("activity not found for ID: {activity_id}"))
                })?
        } else {
            run_id.to_owned()
        };
        ActivityTaskToken::encode(
            namespace_id,
            activity_id,
            &resolved_run,
            1,
            self.archetype_id,
        )
    }

    /// Complete a standalone activity addressed by id (the `RespondActivityTask
    /// CompletedById` path). Synthesizes the by-id token and reuses the by-token
    /// completion path; see `by_id_token` for the v1.31.0
    /// fidelity notes.
    pub async fn complete_by_id(
        &self,
        namespace_id: &str,
        activity_id: &str,
        run_id: &str,
        result: Vec<u8>,
        identity: String,
    ) -> EdgeResult<()> {
        let token = self.by_id_token(namespace_id, activity_id, run_id).await?;
        self.respond_activity_task_completed(&token, namespace_id, result, identity)
            .await
    }

    /// Fail a standalone activity addressed by id (the `RespondActivityTaskFailed
    /// ById` path). `failure` is the message; `failure_payload` is the full encoded
    /// `Failure` proto so the describe outcome round-trips the structured failure;
    /// `last_heartbeat_details` is the encoded `Payloads` of the worker's last
    /// heartbeat (empty when none).
    pub async fn fail_by_id(
        &self,
        namespace_id: &str,
        activity_id: &str,
        run_id: &str,
        failure: String,
        failure_payload: Vec<u8>,
        last_heartbeat_details: Vec<u8>,
        identity: String,
    ) -> EdgeResult<()> {
        let token = self.by_id_token(namespace_id, activity_id, run_id).await?;
        self.respond_activity_task_failed(
            &token,
            namespace_id,
            failure,
            failure_payload,
            last_heartbeat_details,
            identity,
        )
        .await
    }

    /// Acknowledge cancellation of a standalone activity addressed by id (the
    /// `RespondActivityTaskCanceledById` path).
    pub async fn cancel_by_id(
        &self,
        namespace_id: &str,
        activity_id: &str,
        run_id: &str,
        details: Vec<u8>,
    ) -> EdgeResult<()> {
        let token = self.by_id_token(namespace_id, activity_id, run_id).await?;
        self.respond_activity_task_canceled(&token, namespace_id, details)
            .await
    }

    /// Record a heartbeat for a standalone activity addressed by id (the
    /// `RecordActivityTaskHeartbeatById` path), returning `cancel_requested`.
    /// Synthesizes the by-id token and reuses the by-token heartbeat path; see
    /// `by_id_token` for the v1.31.0 fidelity notes.
    pub async fn heartbeat_by_id(
        &self,
        namespace_id: &str,
        activity_id: &str,
        run_id: &str,
        details: Vec<u8>,
    ) -> EdgeResult<bool> {
        let token = self.by_id_token(namespace_id, activity_id, run_id).await?;
        self.record_heartbeat(&token, namespace_id, details).await
    }

    /// Encode the describe long-poll token: a serialized `ComponentRef` to the
    /// activity root carrying the execution key and the current execution VT — the
    /// shape `ctx.Ref(a)` produces in v1.31.0 (`chasm/lib/activity/activity.go:723`).
    /// The embedded VT is the point a follow-on long-poll resumes waiting from; the
    /// embedded key is what [`decode_describe_token`](Self::decode_describe_token)
    /// validates the request against. This is deliberately NOT a bare
    /// `VersionedTransition`: a bare clock cannot be validated against the requested
    /// execution, which is what `LongPollTokenFromWrongExecution` /
    /// `LongPollTokenFromDifferentNamespace` require (`standalone_activity_test.go:4068,4108`).
    pub fn encode_describe_token(
        &self,
        key: &ExecutionKey,
        execution_vt: VersionedTransition,
    ) -> Vec<u8> {
        // A root-component ref carries an empty `component_path`, the only input that
        // could make `encode` fail (empty path segment). With no segments it cannot
        // fail, so the `unwrap_or_default` is unreachable rather than a swallowed
        // error path.
        ComponentRef::new(
            key.clone(),
            self.archetype_id,
            execution_vt,
            Vec::new(),
            VersionedTransition::default(),
        )
        .encode()
        .unwrap_or_default()
    }

    /// Decode and validate a describe long-poll token against the requested
    /// execution, returning the execution VT to resume the wait from. Mirrors the
    /// two `chasm.ExecutionStateChanged` failure modes
    /// (`chasm/lib/activity/handler.go:147-150 @ v1.31.0`):
    /// - bytes that are not a well-formed `ComponentRef` → `InvalidArgument
    ///   "invalid long poll token"` (`ErrMalformedComponentRef`);
    /// - a ref whose execution key names a different execution than the request →
    ///   `InvalidArgument "long poll token does not match execution"`
    ///   (`ErrInvalidComponentRef`) — the cross-execution / cross-namespace token
    ///   reuse guard.
    pub fn decode_describe_token(
        &self,
        token: &[u8],
        expected: &ExecutionKey,
    ) -> EdgeResult<VersionedTransition> {
        let reference = ComponentRef::decode(token)
            .map_err(|_| EdgeError::BadRequest("invalid long poll token".to_owned()))?;
        if &reference.execution_key != expected {
            return Err(EdgeError::BadRequest(
                "long poll token does not match execution".to_owned(),
            ));
        }
        Ok(reference.execution_versioned_transition)
    }

    /// Validate a worker response token before applying it — shared by
    /// `RespondActivityTaskCompleted`/`Failed`/`Canceled` (token validation,
    /// `validateActivityTaskToken`, `chasm/lib/activity/activity.go:782 @ v1.31.0`):
    ///
    /// - a token whose namespace differs from the request's is rejected
    ///   `InvalidArgument` ("Operation requested with a token from a different
    ///   namespace.");
    /// - a token whose *component ref* namespace differs from the request's — a
    ///   tampered or cross-namespace ref that slipped past the first check — is
    ///   rejected `InvalidArgument` ("token does not match namespace");
    /// - a token naming a superseded attempt (a retry advanced the live attempt), a
    ///   terminal activity (the attempt already resolved), or a missing execution is
    ///   rejected `NotFound "activity not found for ID: <id>"` — the active attempt
    ///   the token named no longer exists.
    async fn validate_token(
        &self,
        token: &ActivityTaskToken,
        request_namespace_id: &str,
    ) -> EdgeResult<()> {
        // Check 1 — the namespace-validator interceptor: the request namespace must
        // match the token's top-level namespace (`errTaskTokenNamespaceMismatch`,
        // `common/rpc/interceptor/namespace_validator.go:354 @ v1.31.0`).
        if token.namespace_id != request_namespace_id {
            return Err(EdgeError::BadRequest(
                "Operation requested with a token from a different namespace.".to_owned(),
            ));
        }
        // Check 2 — `validateActivityTaskToken`: the request namespace must also
        // match the *component ref's* namespace. This is what catches a token whose
        // ref was swapped to another namespace while the top-level namespace still
        // matches (`MismatchedTokenComponentRef`, `activity.go:804 @ v1.31.0`).
        if token.ref_namespace_id != request_namespace_id {
            return Err(EdgeError::BadRequest(
                "token does not match namespace".to_owned(),
            ));
        }
        let not_found =
            || EdgeError::NotFound(format!("activity not found for ID: {}", token.activity_id));
        let snapshot = match self.engine.read_component(&token.execution_key()).await {
            Ok(snapshot) => snapshot,
            Err(ChasmError::ExecutionNotFound) => return Err(not_found()),
            Err(error) => return Err(map_chasm_err(error)),
        };
        let bytes = snapshot.data.ok_or_else(not_found)?;
        let state = ActivityState::decode(bytes.as_slice())
            .map_err(|e| EdgeError::Internal(format!("decode activity state: {e}")))?;
        // Attempt fence (`token.Attempt != LastAttempt.Count`, `activity.go:790`): a
        // retry advanced the live attempt, or the activity is terminal, so the
        // attempt the token named is no longer live. tokeira's `attempt` and `stamp`
        // move together, so this is the same fence the dispatch/timers use.
        if state.attempt != token.attempt || state.status().is_terminal() {
            return Err(not_found());
        }
        Ok(())
    }

    /// Drive one activity event through a fenced transition.
    async fn apply_event(&self, key: ExecutionKey, event: ActivityEvent) -> EdgeResult<()> {
        self.ensure_enabled()?;
        let reference = self.activity_ref(key);
        let typed = TypedEngine::<ActivityExecution>::new(&self.engine);
        // `update` reads the live state by execution key; the closure is the only
        // place the event is applied, and it may re-run on a fenced conflict.
        typed
            .update(&reference, move |activity, ctx| {
                activity.apply(event.clone(), ctx)
            })
            .await
            .map_err(map_chasm_err)?;
        Ok(())
    }

    /// Build a reference addressing an activity execution by key. `update`/`read`
    /// consult only the execution key, so the VT fields are placeholders.
    fn activity_ref(&self, key: ExecutionKey) -> ComponentRef {
        ComponentRef::new(
            key,
            self.archetype_id,
            VersionedTransition::default(),
            Vec::new(),
            VersionedTransition::default(),
        )
    }

    /// Read the live [`ActivityState`] for `key`, or `None` if the execution does not
    /// exist (a deleted/never-created activity). Shared by the timeout sweeper path.
    async fn load_state(&self, key: &ExecutionKey) -> EdgeResult<Option<ActivityState>> {
        match self.engine.read_component(key).await {
            Ok(snapshot) => match snapshot.data {
                Some(bytes) => ActivityState::decode(bytes.as_slice())
                    .map(Some)
                    .map_err(|e| EdgeError::Internal(format!("decode activity state: {e}"))),
                None => Ok(None),
            },
            Err(ChasmError::ExecutionNotFound) => Ok(None),
            Err(error) => Err(map_chasm_err(error)),
        }
    }

    /// Fire any due activity timeout for `key` at `now`, returning the next timeout
    /// deadline to re-arm (`None` when terminal/gone/no timeout). This is the edge
    /// half of the runtime timer sweeper (`chasm-activity-timeouts-and-retry`): it
    /// re-derives the due timeout from durable state (history is authority; the armed
    /// timer is a derived hint), then applies it under one fenced transition with
    /// schedule-to-close precedence:
    ///
    /// - schedule-to-start / schedule-to-close → `TimedOut` directly (these never
    ///   retry — `activity_tasks.go` `scheduleToStart`/`scheduleToClose` `Execute @
    ///   v1.31.0` apply `TransitionTimedOut`);
    /// - start-to-close / heartbeat → `tryReschedule` first, falling back to
    ///   `TimedOut` when no retry is possible (`startToClose`/`heartbeat` `Execute @
    ///   v1.31.0`).
    ///
    /// The decision is recomputed inside the closure against committed state, so a
    /// timeout that is no longer due (a heartbeat raced in, the attempt advanced) is a
    /// validate-then-drop no-op. The fenced update is issued only when a timeout is
    /// due, so a not-due sweep does not churn the execution's VT.
    pub async fn evaluate_timeouts(&self, key: &ExecutionKey, now: i64) -> EdgeResult<Option<i64>> {
        self.ensure_enabled()?;
        let Some(state) = self.load_state(key).await? else {
            return Ok(None);
        };
        if state.status().is_terminal() {
            return Ok(None);
        }
        // Nothing due yet: re-arm to the earliest future deadline without a commit.
        if due_timeout(&state, now).is_none() {
            return Ok(next_timeout_deadline(&state));
        }

        let reference = self.activity_ref(key.clone());
        let typed = TypedEngine::<ActivityExecution>::new(&self.engine);
        typed
            .update(&reference, move |activity, ctx| {
                let state = activity.activity_state().cloned().unwrap_or_default();
                let now = ctx.now_unix_nanos();
                // Re-derive against committed state; a raced advance makes this a
                // no-op (validate-then-drop) rather than a wrong timeout.
                let Some(timeout_type) = due_timeout(&state, now) else {
                    return Ok(());
                };
                let timed_out = |tt: TimeoutType| ActivityEvent::TimedOut {
                    stamp: state.stamp,
                    timeout_type: tt,
                    failure_payload: build_timeout_failure(tt),
                };
                let event = match timeout_type {
                    // schedule-to-start / schedule-to-close never retry.
                    TimeoutType::ScheduleToStart | TimeoutType::ScheduleToClose => {
                        timed_out(timeout_type)
                    }
                    // start-to-close / heartbeat reschedule when the retry budget
                    // allows, else time out (`tryReschedule` @ v1.31.0).
                    TimeoutType::StartToClose | TimeoutType::Heartbeat => {
                        match retry_decision(&state, now, 0) {
                            RetryOutcome::Reschedule(interval) => ActivityEvent::Rescheduled {
                                failure: format!("activity {} timeout", timeout_type.as_str()),
                                identity: state.last_worker_identity.clone(),
                                last_heartbeat_details: Vec::new(),
                                interval_nanos: interval,
                            },
                            RetryOutcome::Terminal => timed_out(timeout_type),
                        }
                    }
                };
                activity.apply(event, ctx)
            })
            .await
            .map_err(map_chasm_err)?;

        // Re-arm to the post-transition next deadline (a retry's new attempt, or
        // `None` once terminal).
        let next = self
            .load_state(key)
            .await?
            .filter(|s| !s.status().is_terminal())
            .and_then(|s| next_timeout_deadline(&s));
        Ok(next)
    }
}

/// Decode an activity snapshot into an [`ActivityDescription`].
fn description_from(
    data: Option<Vec<u8>>,
    execution_vt: VersionedTransition,
) -> EdgeResult<ActivityDescription> {
    let bytes =
        data.ok_or_else(|| EdgeError::NotFound("activity execution not found".to_owned()))?;
    let state = ActivityState::decode(bytes.as_slice())
        .map_err(|e| EdgeError::Internal(format!("decode activity state: {e}")))?;
    Ok(ActivityDescription {
        status: state.status(),
        attempt: state.attempt,
        activity_type: state.activity_type,
        task_queue: state.task_queue,
        input: state.input,
        result: state.result,
        failure: state.failure,
        failure_payload: state.failure_payload,
        schedule_to_close_nanos: state.schedule_to_close_nanos,
        schedule_to_start_nanos: state.schedule_to_start_nanos,
        start_to_close_nanos: state.start_to_close_nanos,
        heartbeat_nanos: state.heartbeat_nanos,
        scheduled_time_nanos: state.scheduled_time_nanos,
        attempt_scheduled_time_nanos: state.attempt_scheduled_time_nanos,
        started_time_nanos: state.started_time_nanos,
        worker_identity: state.last_worker_identity,
        close_time_nanos: state.close_time_nanos,
        header: state.header,
        retry_policy: state.retry_policy,
        priority: state.priority,
        search_attributes: state.search_attributes,
        user_metadata: state.user_metadata,
        execution_vt,
        heartbeat_details: state.last_heartbeat_details,
        cancel_request_id: state.cancel_request_id,
        cancel_reason: state.cancel_reason,
        cancel_identity: state.cancel_identity,
        canceled_details: state.canceled_details,
        terminate_request_id: state.terminate_request_id,
        terminate_identity: state.terminate_identity,
        last_attempt_complete_time_nanos: state.last_attempt_complete_time_nanos,
        current_retry_interval_nanos: state.current_retry_interval_nanos,
    })
}

/// Map a [`ChasmError`] to the edge's [`EdgeError`] (which the gRPC layer maps to a
/// `tonic::Status`).
/// Map an engine error for an activity lookup, rendering a missing activity as the
/// v1.31.0 NotFound message that names the activity id ("activity not found for ID:
/// <id>", `chasm/lib/activity/frontend.go @ v1.31.0`). Other errors fall through to
/// [`map_chasm_err`].
fn map_activity_not_found(error: ChasmError, key: &ExecutionKey) -> EdgeError {
    match error {
        ChasmError::ExecutionNotFound => {
            EdgeError::NotFound(format!("activity not found for ID: {}", key.business_id))
        }
        other => map_chasm_err(other),
    }
}

fn map_chasm_err(error: ChasmError) -> EdgeError {
    match error {
        ChasmError::Validation(message) => EdgeError::BadRequest(message),
        ChasmError::ExecutionNotFound => {
            EdgeError::NotFound("activity execution not found".to_owned())
        }
        ChasmError::ExecutionClosed => {
            EdgeError::FailedPrecondition("activity execution is closed".to_owned())
        }
        ChasmError::BusinessIdConflict(message) => EdgeError::AlreadyExists(message),
        // Surface the targeted release's typed ActivityExecutionAlreadyStarted: the
        // edge encodes code AlreadyExists + the ActivityExecutionAlreadyStartedFailure
        // detail (RunId/StartRequestId) so the SDK's ErrorAs recovers it
        // (`chasm/lib/activity/handler.go:91 @ v1.31.0`).
        ChasmError::BusinessIdAlreadyStarted {
            run_id,
            request_id,
            message,
        } => EdgeError::ActivityExecutionAlreadyStarted {
            message,
            run_id,
            start_request_id: request_id,
        },
        ChasmError::Unsupported(message) => EdgeError::Unimplemented(message),
        ChasmError::IllegalTransition { from, event } => EdgeError::FailedPrecondition(format!(
            "illegal activity transition: {event} from {from}"
        )),
        ChasmError::StaleStamp => {
            EdgeError::FailedPrecondition("activity attempt superseded".to_owned())
        }
        ChasmError::StaleReference => {
            EdgeError::FailedPrecondition("stale activity reference".to_owned())
        }
        ChasmError::RetriesExhausted { attempts } => EdgeError::Internal(format!(
            "activity transition retries exhausted after {attempts} conflicts"
        )),
        ChasmError::Internal(message) => EdgeError::Internal(message),
        // `ChasmError` is `#[non_exhaustive]`; surface any future variant safely.
        other => EdgeError::Internal(format!("unexpected chasm error: {other}")),
    }
}

/// Classify a worker-reported failure for retry, returning `(retryable,
/// override_nanos)`. Mirrors the retryability test in `HandleFailed @ v1.31.0`: a
/// failure is retryable only if it carries an `ApplicationFailureInfo` that is not
/// `non_retryable` and whose `type` is not in the policy's `non_retryable_error_types`.
/// `override_nanos` is the failure's `NextRetryDelay` (0 when unset), which overrides
/// the exponential interval. Decoding the failure and the retry policy is the edge's
/// job (the pure crate stays proto-free), so this lives here.
fn classify_worker_failure(failure_payload: &[u8], retry_policy_bytes: &[u8]) -> (bool, i64) {
    use tokeira_proto::failure::{Failure, failure::FailureInfo};
    if failure_payload.is_empty() {
        // No structured failure → only ApplicationFailureInfo retries, so terminal.
        return (false, 0);
    }
    let Ok(failure) = Failure::decode(failure_payload) else {
        return (false, 0);
    };
    let Some(FailureInfo::ApplicationFailureInfo(app)) = failure.failure_info else {
        // Timeout/cancellation/etc. failures are not application-retryable here.
        return (false, 0);
    };
    if app.non_retryable {
        return (false, 0);
    }
    // An empty policy blob decodes to an empty `RetryPolicy` (no excluded types).
    let excluded = tokeira_proto::common::RetryPolicy::decode(retry_policy_bytes)
        .map(|p| p.non_retryable_error_types)
        .unwrap_or_default();
    if excluded.iter().any(|t| t == &app.r#type) {
        return (false, 0);
    }
    let override_nanos = app
        .next_retry_delay
        .map(|d| {
            d.seconds
                .saturating_mul(1_000_000_000)
                .saturating_add(i64::from(d.nanos))
        })
        .unwrap_or(0);
    (true, override_nanos)
}

/// Build the encoded `Failure` (with `TimeoutFailureInfo`) recorded when a timeout
/// fires, so the describe/poll outcome surfaces the structured timeout type
/// (`createStartToCloseTimeoutFailure` et al. `@ v1.31.0`). Built at the edge because
/// the pure crate is proto-free.
fn build_timeout_failure(timeout_type: TimeoutType) -> Vec<u8> {
    use tokeira_proto::failure::{Failure, TimeoutFailureInfo, failure::FailureInfo};
    Failure {
        message: format!("activity {} timeout", timeout_type.as_str()),
        failure_info: Some(FailureInfo::TimeoutFailureInfo(TimeoutFailureInfo {
            timeout_type: timeout_type_to_proto(timeout_type) as i32,
            ..Default::default()
        })),
        ..Default::default()
    }
    .encode_to_vec()
}

/// Map the pure [`TimeoutType`] to the proto `enums.TimeoutType`.
fn timeout_type_to_proto(timeout_type: TimeoutType) -> tokeira_proto::enums::TimeoutType {
    use tokeira_proto::enums::TimeoutType as T;
    match timeout_type {
        TimeoutType::ScheduleToStart => T::ScheduleToStart,
        TimeoutType::ScheduleToClose => T::ScheduleToClose,
        TimeoutType::StartToClose => T::StartToClose,
        TimeoutType::Heartbeat => T::Heartbeat,
    }
}

/// The bridge is the runtime sweeper's [`TimeoutEvaluator`]: it owns the activity
/// timeout semantics (over the pure crate), so the runtime fires timers without
/// depending on the edge. Delegates to the inherent
/// [`evaluate_timeouts`](ActivityBridge::evaluate_timeouts).
#[async_trait::async_trait]
impl TimeoutEvaluator for ActivityBridge {
    async fn evaluate_timeouts(&self, key: &ExecutionKey, now: i64) -> anyhow::Result<Option<i64>> {
        ActivityBridge::evaluate_timeouts(self, key, now)
            .await
            .map_err(|e| anyhow::anyhow!("evaluate activity timeouts: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_chasm::{Library, Registry};
    use tokeira_chasm_activity::ActivityLibrary;
    use tokeira_runtime::chasm::{CollectingDispatchSink, CollectingVisibilitySink};
    use tokeira_storage::InMemoryChasmNodeStore;

    const SEC: i64 = 1_000_000_000;
    #[cfg(feature = "conformance")]
    static CONFORMANCE_OVERRIDE_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn engine() -> Arc<ChasmEngine> {
        let mut builder = Registry::builder();
        ActivityLibrary::register(&mut builder).expect("register activity library");
        let registry = Arc::new(builder.build());
        Arc::new(ChasmEngine::new(
            Arc::new(InMemoryChasmNodeStore::new()),
            registry,
            Arc::new(CollectingDispatchSink::default()),
            Arc::new(CollectingVisibilitySink::default()),
        ))
    }

    fn bridge(enabled: bool) -> ActivityBridge {
        ActivityBridge::new(
            engine(),
            ActivityConfig {
                enable_standalone: enabled,
                ..ActivityConfig::default()
            },
            1000,
        )
    }

    fn start_request() -> StartActivity {
        StartActivity {
            namespace_id: uuid::Uuid::new_v4().to_string(),
            activity_id: "act-1".to_owned(),
            run_id: uuid::Uuid::new_v4().to_string(),
            activity_type: "PaymentActivity".to_owned(),
            task_queue: "payments".to_owned(),
            input: vec![1, 2, 3],
            schedule_to_start_nanos: 0,
            schedule_to_close_nanos: 0,
            start_to_close_nanos: 10 * SEC,
            heartbeat_nanos: 0,
            run_timeout_nanos: 0,
            request_id: Some("req-1".to_owned()),
            policy: BusinessIdPolicy::default(),
            header: Vec::new(),
            retry_policy: Vec::new(),
            retry_initial_interval_nanos: SEC,
            retry_backoff_coefficient: 2.0,
            retry_maximum_interval_nanos: 100 * SEC,
            maximum_attempts: 0,
            priority: Vec::new(),
            search_attributes: Vec::new(),
            user_metadata: Vec::new(),
        }
    }

    /// Build a Start sharing a business id (`namespace_id` + `activity_id`) with a
    /// fresh run id, so repeated calls collide on the current-run pointer the way
    /// the id reuse/conflict matrix expects.
    fn start_with(
        namespace_id: &str,
        activity_id: &str,
        request_id: &str,
        policy: BusinessIdPolicy,
    ) -> StartActivity {
        StartActivity {
            namespace_id: namespace_id.to_owned(),
            activity_id: activity_id.to_owned(),
            run_id: uuid::Uuid::new_v4().to_string(),
            activity_type: "PaymentActivity".to_owned(),
            task_queue: "payments".to_owned(),
            input: vec![1, 2, 3],
            schedule_to_start_nanos: 0,
            schedule_to_close_nanos: 0,
            start_to_close_nanos: 10 * SEC,
            heartbeat_nanos: 0,
            run_timeout_nanos: 0,
            request_id: Some(request_id.to_owned()),
            policy,
            header: Vec::new(),
            retry_policy: Vec::new(),
            retry_initial_interval_nanos: SEC,
            retry_backoff_coefficient: 2.0,
            retry_maximum_interval_nanos: 100 * SEC,
            maximum_attempts: 0,
            priority: Vec::new(),
            search_attributes: Vec::new(),
            user_metadata: Vec::new(),
        }
    }

    // Feature: activity-executions-first-class, task 1.3 (conflict policy, live run):
    // Fail rejects a second Start with AlreadyStarted naming the current run; the
    // same request id is idempotent; UseExisting returns the live run without
    // creating one. Ground truth: chasm_engine.go:1014-1045 + standalone_activity_test.go
    // TestIDConflictPolicy @ v1.31.0.
    #[tokio::test]
    async fn conflict_policy_against_live_run() {
        use tokeira_chasm::{BusinessIdConflictPolicy, BusinessIdReusePolicy};
        let bridge = bridge(true);
        let ns = uuid::Uuid::new_v4().to_string();
        let first = bridge
            .start(start_with(
                &ns,
                "act-c",
                "req-first",
                BusinessIdPolicy::default(),
            ))
            .await
            .expect("first start");
        assert!(first.started);
        let first_run = first.reference.execution_key.run_id.clone();

        // Default conflict policy is Fail: a different-request-id Start is rejected.
        let err = bridge
            .start(start_with(
                &ns,
                "act-c",
                "req-other",
                BusinessIdPolicy::default(),
            ))
            .await
            .expect_err("Fail must reject a live run");
        assert!(
            matches!(err, EdgeError::ActivityExecutionAlreadyStarted { .. }),
            "got {err:?}"
        );

        // Same request id as the current run is idempotent: existing run, not started.
        let same = bridge
            .start(start_with(
                &ns,
                "act-c",
                "req-first",
                BusinessIdPolicy::default(),
            ))
            .await
            .expect("same request id returns existing");
        assert!(!same.started);
        assert_eq!(same.reference.execution_key.run_id, first_run);

        // UseExisting returns the live run without creating a new one.
        let use_existing = bridge
            .start(start_with(
                &ns,
                "act-c",
                "req-use",
                BusinessIdPolicy {
                    reuse: BusinessIdReusePolicy::AllowDuplicate,
                    conflict: BusinessIdConflictPolicy::UseExisting,
                },
            ))
            .await
            .expect("UseExisting returns existing");
        assert!(!use_existing.started);
        assert_eq!(use_existing.reference.execution_key.run_id, first_run);
    }

    // Feature: activity-executions-first-class, task 1.3 (reuse policy, terminal run):
    // against a completed run, RejectDuplicate rejects while AllowDuplicate creates a
    // fresh run. Ground truth: chasm_engine.go:1063-1090 + TestIDReusePolicy @ v1.31.0.
    #[tokio::test]
    async fn reuse_policy_against_terminal_run() {
        use tokeira_chasm::{BusinessIdConflictPolicy, BusinessIdReusePolicy};
        let bridge = bridge(true);
        let ns = uuid::Uuid::new_v4().to_string();
        let first = start_with(&ns, "act-r", "req-1", BusinessIdPolicy::default());
        let key = key_of(&first);
        let first_run = bridge.start(first).await.expect("start").reference;
        bridge
            .record_started(key.clone(), SEC, "worker-1".to_owned())
            .await
            .expect("started");
        bridge
            .record_completed(key.clone(), vec![9, 9], "worker-1".to_owned())
            .await
            .expect("completed");

        // RejectDuplicate against a terminal run is rejected.
        let err = bridge
            .start(start_with(
                &ns,
                "act-r",
                "req-2",
                BusinessIdPolicy {
                    reuse: BusinessIdReusePolicy::RejectDuplicate,
                    conflict: BusinessIdConflictPolicy::Fail,
                },
            ))
            .await
            .expect_err("RejectDuplicate must reject a terminal run");
        assert!(
            matches!(err, EdgeError::ActivityExecutionAlreadyStarted { .. }),
            "got {err:?}"
        );

        // AllowDuplicate (the default) creates a fresh run.
        let again = bridge
            .start(start_with(
                &ns,
                "act-r",
                "req-3",
                BusinessIdPolicy::default(),
            ))
            .await
            .expect("AllowDuplicate creates a new run");
        assert!(again.started);
        assert_ne!(
            again.reference.execution_key.run_id,
            first_run.execution_key.run_id
        );
    }

    fn key_of(req: &StartActivity) -> ExecutionKey {
        ExecutionKey::new(
            req.namespace_id.clone(),
            req.activity_id.clone(),
            req.run_id.clone(),
        )
    }

    #[tokio::test]
    async fn gate_off_returns_unimplemented() {
        #[cfg(feature = "conformance")]
        let _override_guard = CONFORMANCE_OVERRIDE_TEST_LOCK
            .lock()
            .expect("conformance override test lock");
        #[cfg(feature = "conformance")]
        tokeira_conformance::overrides().clear(STANDALONE_ACTIVITIES_KEY);
        let bridge = bridge(false);
        let err = bridge.start(start_request()).await.unwrap_err();
        assert!(
            matches!(err, EdgeError::Unimplemented(ref m) if m == "Standalone activity is disabled")
        );
    }

    #[cfg(feature = "conformance")]
    #[test]
    fn conformance_overrides_control_activity_policy_live() {
        let _override_guard = CONFORMANCE_OVERRIDE_TEST_LOCK
            .lock()
            .expect("conformance override test lock");
        let overrides = tokeira_conformance::overrides();
        overrides.clear(STANDALONE_ACTIVITIES_KEY);
        overrides.clear(ACTIVITY_LONG_POLL_TIMEOUT_KEY);
        overrides.clear(ACTIVITY_LONG_POLL_BUFFER_KEY);
        let bridge = bridge(false);
        assert!(!bridge.is_enabled());
        assert_eq!(
            bridge.long_poll_timeout(),
            std::time::Duration::from_secs(20)
        );
        assert_eq!(bridge.long_poll_buffer(), std::time::Duration::from_secs(1));

        overrides
            .set(
                STANDALONE_ACTIVITIES_KEY,
                tokeira_conformance::OverrideValue::Bool(true),
            )
            .expect("standalone-activity override is wired");
        overrides
            .set(
                ACTIVITY_LONG_POLL_TIMEOUT_KEY,
                tokeira_conformance::OverrideValue::Duration(std::time::Duration::from_millis(10)),
            )
            .expect("activity long-poll timeout override is wired");
        overrides
            .set(
                ACTIVITY_LONG_POLL_BUFFER_KEY,
                tokeira_conformance::OverrideValue::Duration(std::time::Duration::from_secs(29)),
            )
            .expect("activity long-poll buffer override is wired");
        assert!(bridge.is_enabled());
        assert_eq!(
            bridge.long_poll_timeout(),
            std::time::Duration::from_millis(10)
        );
        assert_eq!(
            bridge.long_poll_buffer(),
            std::time::Duration::from_secs(29)
        );

        overrides.clear(STANDALONE_ACTIVITIES_KEY);
        overrides.clear(ACTIVITY_LONG_POLL_TIMEOUT_KEY);
        overrides.clear(ACTIVITY_LONG_POLL_BUFFER_KEY);
        assert!(!bridge.is_enabled());
    }

    #[tokio::test]
    async fn start_then_describe_is_scheduled() {
        let bridge = bridge(true);
        let req = start_request();
        let key = key_of(&req);
        bridge.start(req).await.expect("start");

        let described = bridge.describe(key).await.expect("describe");
        assert_eq!(described.status, ActivityStatus::Scheduled);
        assert_eq!(described.attempt, 1);
    }

    #[tokio::test]
    async fn start_indexes_activity_for_count_by_activity_id() {
        // Repro for TestCountActivityExecutions/CountByActivityId: a started activity
        // must be queryable by `ActivityId = '<id>'` (the chasm business-id alias,
        // `WithBusinessIDAlias("ActivityId")`, `chasm/lib/activity/library.go:66 @
        // v1.31.0`). Wires the engine's post-commit ProjectionVisibilitySink to the
        // same Arc-shared InMemoryVisibilityStore a VisibilityQueryService reads,
        // mirroring the tokeirad bootstrap (apps/tokeirad/src/lib.rs).
        use tokeira_projection::{
            CountActivityExecutionsRequest, InMemoryVisibilityStore, VisibilityApi,
            VisibilityQueryService, VisibilitySink as ProjStoreSink,
        };
        use tokeira_runtime::chasm::ProjectionVisibilitySink;

        let store = InMemoryVisibilityStore::default();
        let chasm_sink = Arc::new(ProjectionVisibilitySink::new(
            Arc::new(ProjStoreSink::new(store.clone())),
            1,
        ));
        let mut builder = Registry::builder();
        ActivityLibrary::register(&mut builder).expect("register activity library");
        let registry = Arc::new(builder.build());
        let engine = Arc::new(ChasmEngine::new(
            Arc::new(InMemoryChasmNodeStore::new()),
            registry,
            Arc::new(CollectingDispatchSink::default()),
            chasm_sink,
        ));
        let bridge = ActivityBridge::new(
            engine,
            ActivityConfig {
                enable_standalone: true,
                ..ActivityConfig::default()
            },
            1000,
        );

        let namespace_id = uuid::Uuid::new_v4().to_string();
        let mut req = start_request();
        req.namespace_id = namespace_id.clone();
        req.activity_id = "count-act".to_owned();
        bridge.start(req).await.expect("start");

        let query = VisibilityQueryService::new(store.clone());
        let resp = query
            .count_activities(
                bridge.archetype_id(),
                CountActivityExecutionsRequest {
                    namespace: namespace_id,
                    query: Some("ActivityId = 'count-act'".to_owned()),
                    group_by: None,
                },
            )
            .await
            .expect("count");
        assert_eq!(
            resp.total_count, 1,
            "a started activity must be counted by ActivityId"
        );
    }

    #[tokio::test]
    async fn describe_token_round_trips_and_validates_execution() {
        // The describe long-poll token is a serialized ComponentRef (key + VT). A
        // round-trip returns the embedded VT; a token for a different execution is
        // rejected "long poll token does not match execution", and malformed bytes
        // are rejected "invalid long poll token" — the two
        // `chasm.ExecutionStateChanged` failure modes (handler.go:147-150 @ v1.31.0,
        // standalone_activity_test.go:4068/4108/4029).
        let bridge = bridge(true);
        let key = ExecutionKey::new(
            uuid::Uuid::new_v4().to_string(),
            "act-1".to_owned(),
            uuid::Uuid::new_v4().to_string(),
        );
        let vt = tokeira_chasm::VersionedTransition::new(3, 7);
        let token = bridge.encode_describe_token(&key, vt);
        assert!(!token.is_empty(), "token for a real execution is non-empty");
        assert_eq!(
            bridge.decode_describe_token(&token, &key).expect("valid"),
            vt,
            "round-trips the embedded execution VT"
        );

        let other = ExecutionKey::new(
            uuid::Uuid::new_v4().to_string(),
            "act-2".to_owned(),
            uuid::Uuid::new_v4().to_string(),
        );
        match bridge.decode_describe_token(&token, &other) {
            Err(EdgeError::BadRequest(m)) => {
                assert_eq!(m, "long poll token does not match execution")
            }
            other => panic!("expected mismatch BadRequest, got {other:?}"),
        }

        match bridge.decode_describe_token(b"invalid-token", &key) {
            Err(EdgeError::BadRequest(m)) => assert_eq!(m, "invalid long poll token"),
            other => panic!("expected malformed BadRequest, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fail_with_heartbeat_details_is_echoed_on_describe() {
        // RespondActivityTaskFailed.LastHeartbeatDetails is recorded onto the activity
        // and echoed on info.heartbeat_details (statemachine.go:220 / activity.go:215
        // @ v1.31.0; standalone_activity_test.go:4908). A fail without details leaves
        // the field empty (the `details != nil` guard).
        let bridge = bridge(true);
        let req = start_request();
        let key = key_of(&req);
        bridge.start(req).await.expect("start");
        bridge
            .record_started(key.clone(), 5 * SEC, "worker-1".to_owned())
            .await
            .expect("started");

        let details = vec![1, 2, 3, 4];
        bridge
            .record_failed(
                key.clone(),
                "boom".to_owned(),
                Vec::new(),
                details.clone(),
                "worker-1".to_owned(),
            )
            .await
            .expect("failed");

        let described = bridge.describe(key).await.expect("describe");
        assert_eq!(described.status, ActivityStatus::Failed);
        assert_eq!(
            described.heartbeat_details, details,
            "the worker's last heartbeat details are echoed on describe"
        );

        // A separate failure without details must not surface phantom heartbeat data.
        let req2 = start_with(
            &uuid::Uuid::new_v4().to_string(),
            "no-hb",
            "req-no-hb",
            BusinessIdPolicy::default(),
        );
        let key2 = key_of(&req2);
        bridge.start(req2).await.expect("start");
        bridge
            .record_started(key2.clone(), 5 * SEC, "worker-1".to_owned())
            .await
            .expect("started");
        bridge
            .record_failed(
                key2.clone(),
                "boom".to_owned(),
                Vec::new(),
                Vec::new(),
                "worker-1".to_owned(),
            )
            .await
            .expect("failed");
        assert!(
            bridge
                .describe(key2)
                .await
                .expect("describe")
                .heartbeat_details
                .is_empty(),
            "no heartbeat details supplied → field stays empty"
        );
    }

    #[tokio::test]
    async fn start_carries_describe_echo_fields() {
        // The Start request's header / retry policy / priority / search attributes /
        // user metadata are stored opaque and returned verbatim by describe (Req 5):
        // DescribeActivityExecution must echo them (standalone_activity_test.go:3122).
        let bridge = bridge(true);
        let mut req = start_request();
        req.header = vec![1, 2, 3];
        req.retry_policy = vec![4, 5];
        req.priority = vec![6];
        req.search_attributes = vec![7, 8, 9, 10];
        req.user_metadata = vec![11, 12];
        let key = key_of(&req);
        bridge.start(req).await.expect("start");

        let described = bridge.describe(key).await.expect("describe");
        assert_eq!(described.header, vec![1, 2, 3]);
        assert_eq!(described.retry_policy, vec![4, 5]);
        assert_eq!(described.priority, vec![6]);
        assert_eq!(described.search_attributes, vec![7, 8, 9, 10]);
        assert_eq!(described.user_metadata, vec![11, 12]);
    }

    #[tokio::test]
    async fn full_lifecycle_start_started_completed() {
        let bridge = bridge(true);
        let req = start_request();
        let key = key_of(&req);
        bridge.start(req).await.expect("start");

        bridge
            .record_started(key.clone(), 5 * SEC, "worker-1".to_owned())
            .await
            .expect("started");
        assert_eq!(
            bridge.describe(key.clone()).await.unwrap().status,
            ActivityStatus::Started
        );

        bridge
            .record_completed(key.clone(), vec![9, 9], "worker-1".to_owned())
            .await
            .expect("completed");
        let done = bridge.describe(key).await.unwrap();
        assert_eq!(done.status, ActivityStatus::Completed);
        assert_eq!(done.result, vec![9, 9]);
    }

    #[tokio::test]
    async fn terminate_then_describe_is_terminated() {
        let bridge = bridge(true);
        let req = start_request();
        let key = key_of(&req);
        bridge.start(req).await.expect("start");
        bridge
            .terminate(
                key.clone(),
                "operator stop".to_owned(),
                String::new(),
                "operator".to_owned(),
            )
            .await
            .expect("terminate");
        assert_eq!(
            bridge.describe(key).await.unwrap().status,
            ActivityStatus::Terminated
        );
    }

    #[tokio::test]
    async fn poll_resolves_after_advance_and_delete_removes() {
        let bridge = bridge(true);
        let req = start_request();
        let key = key_of(&req);
        let scheduled_ref = bridge.start(req).await.expect("start").reference;
        let since = scheduled_ref.execution_versioned_transition;

        // Advance (start), then a poll since the scheduled VT resolves.
        bridge
            .record_started(key.clone(), SEC, "worker-1".to_owned())
            .await
            .expect("started");
        let polled = bridge.poll(key.clone(), since).await.expect("poll");
        assert!(polled.is_some());
        assert_eq!(polled.unwrap().status, ActivityStatus::Started);

        bridge.delete(key.clone()).await.expect("delete");
        // Describe and a repeat delete on the now-missing activity both surface the
        // v1.31.0 NotFound message that names the activity id (map_activity_not_found),
        // and a delete on a missing activity is a NotFound, not a silent no-op.
        let describe_err = bridge.describe(key.clone()).await.unwrap_err();
        assert!(
            matches!(&describe_err, EdgeError::NotFound(msg) if msg.contains("activity not found for ID")),
            "describe after delete: {describe_err:?}"
        );
        let delete_err = bridge.delete(key).await.unwrap_err();
        assert!(
            matches!(&delete_err, EdgeError::NotFound(msg) if msg.contains("activity not found for ID")),
            "delete on missing: {delete_err:?}"
        );
    }

    #[tokio::test]
    async fn current_run_resolves_bare_id_then_clears_on_delete() {
        // Stage 1 (activity-executions-first-class Req 1): the current-run pointer is
        // set on start, resolves a bare id (empty run_id) to the started run, and is
        // cleared when the current run is deleted (read-your-write).
        let bridge = bridge(true);
        let req = start_request();
        let key = key_of(&req);
        bridge.start(req).await.expect("start");

        let resolved = bridge
            .current_run(&key.namespace_id, &key.business_id)
            .await
            .expect("current_run");
        assert_eq!(resolved.as_deref(), Some(key.run_id.as_str()));

        bridge.delete(key.clone()).await.expect("delete");
        let after = bridge
            .current_run(&key.namespace_id, &key.business_id)
            .await
            .expect("current_run after delete");
        assert_eq!(
            after, None,
            "pointer must clear when the current run is deleted"
        );
    }

    /// A bridge whose engine routes dispatch tasks into a shared
    /// [`ActivityDispatchQueue`], enabling the worker poll/respond path.
    fn worker_bridge() -> ActivityBridge {
        let mut builder = Registry::builder();
        ActivityLibrary::register(&mut builder).expect("register activity library");
        let registry = Arc::new(builder.build());
        let queue = Arc::new(ActivityDispatchQueue::new());
        let engine = Arc::new(ChasmEngine::new(
            Arc::new(InMemoryChasmNodeStore::new()),
            registry,
            queue.clone(),
            Arc::new(CollectingVisibilitySink::default()),
        ));
        ActivityBridge::new(
            engine,
            ActivityConfig {
                enable_standalone: true,
                ..ActivityConfig::default()
            },
            1000,
        )
        .with_dispatch_queue(queue)
    }

    #[tokio::test]
    async fn worker_poll_respond_completes_activity() {
        let bridge = worker_bridge();
        let req = start_request();
        let key = key_of(&req);
        let task_queue = req.task_queue.clone();
        bridge.start(req).await.expect("start");

        // The committed dispatch task is queued; a worker poll picks it up, which
        // records the start (Scheduled → Started) before returning the task.
        let task = bridge
            .poll_activity_task(&task_queue, "worker-1")
            .await
            .expect("poll")
            .expect("a queued task");
        assert_eq!(task.activity_type, "PaymentActivity");
        assert_eq!(task.attempt, 1);
        assert_eq!(
            bridge.describe(key.clone()).await.unwrap().status,
            ActivityStatus::Started
        );

        bridge
            .respond_activity_task_completed(
                &task.task_token,
                &key.namespace_id,
                vec![7, 7],
                "worker-1".to_owned(),
            )
            .await
            .expect("complete");
        let done = bridge.describe(key).await.unwrap();
        assert_eq!(done.status, ActivityStatus::Completed);
        assert_eq!(done.result, vec![7, 7]);

        // The queue is drained — a second poll finds nothing.
        assert!(
            bridge
                .poll_activity_task(&task_queue, "worker-1")
                .await
                .expect("poll empty")
                .is_none()
        );
    }

    #[tokio::test]
    async fn worker_respond_after_terminal_is_rejected() {
        let bridge = worker_bridge();
        let req = start_request();
        let key = key_of(&req);
        let task_queue = req.task_queue.clone();
        bridge.start(req).await.expect("start");

        let task = bridge
            .poll_activity_task(&task_queue, "worker-1")
            .await
            .expect("poll")
            .expect("a queued task");

        // Terminate the activity (now terminal); a late worker completion for the
        // dispatched attempt must be rejected, not applied — `Completed` is illegal
        // from `Terminated`.
        let namespace_id = key.namespace_id.clone();
        bridge
            .terminate(
                key,
                "operator stop".to_owned(),
                String::new(),
                "operator".to_owned(),
            )
            .await
            .expect("terminate");
        // A response to a terminal activity is rejected NotFound — the attempt the
        // token named no longer exists (token validation, v1.31.0).
        let err = bridge
            .respond_activity_task_completed(
                &task.task_token,
                &namespace_id,
                vec![1],
                "worker-1".to_owned(),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, EdgeError::NotFound(_)));
    }

    #[tokio::test]
    async fn worker_poll_empty_queue_returns_none() {
        let bridge = worker_bridge();
        assert!(
            bridge
                .poll_activity_task("idle-queue", "worker-1")
                .await
                .expect("poll")
                .is_none()
        );
    }

    #[tokio::test]
    async fn worker_heartbeat_records_details_and_reports_cancel_requested() {
        // RecordActivityTaskHeartbeat records details (status-preserving) and reports
        // cancel_requested once a cancel is pending (standalone_activity_test.go:4387,
        // 4406). Poll to Started, heartbeat (false + details echoed), request cancel,
        // heartbeat again (true).
        let bridge = worker_bridge();
        let req = start_request();
        let key = key_of(&req);
        let task_queue = req.task_queue.clone();
        bridge.start(req).await.expect("start");

        let task = bridge
            .poll_activity_task(&task_queue, "worker-1")
            .await
            .expect("poll")
            .expect("a queued task");

        let details = vec![5, 6, 7];
        let cancel_requested = bridge
            .record_heartbeat(&task.task_token, &key.namespace_id, details.clone())
            .await
            .expect("heartbeat");
        assert!(!cancel_requested, "no cancel pending before RequestCancel");
        assert_eq!(
            bridge
                .describe(key.clone())
                .await
                .unwrap()
                .heartbeat_details,
            details,
            "heartbeat details are recorded onto the activity"
        );

        bridge
            .request_cancel(
                key.clone(),
                "operator".to_owned(),
                String::new(),
                String::new(),
            )
            .await
            .expect("request cancel");
        let cancel_requested = bridge
            .record_heartbeat(&task.task_token, &key.namespace_id, details.clone())
            .await
            .expect("heartbeat after cancel");
        assert!(
            cancel_requested,
            "cancel_requested is true once a cancel is pending"
        );
    }

    #[tokio::test]
    async fn cancel_while_scheduled_is_immediate() {
        // A cancel on a still-SCHEDULED activity (no worker holds it) drives it
        // straight to CANCELED (`handleCancellationRequested` immediate-cancel,
        // activity.go:413-430 @ v1.31.0; standalone_activity_test.go:1627).
        let bridge = bridge(true);
        let req = start_request();
        let key = key_of(&req);
        bridge.start(req).await.expect("start");
        bridge
            .request_cancel(
                key.clone(),
                "op".to_owned(),
                "rid-1".to_owned(),
                "because".to_owned(),
            )
            .await
            .expect("cancel");
        assert_eq!(
            bridge.describe(key).await.unwrap().status,
            ActivityStatus::Canceled
        );
    }

    #[tokio::test]
    async fn cancel_request_id_dedup() {
        // Repeated cancel: same request_id is an idempotent no-op; a different one is
        // FailedPrecondition (activity.go:402-409 @ v1.31.0;
        // standalone_activity_test.go:1345). Poll to Started so the first cancel marks
        // CANCEL_REQUESTED rather than cancelling immediately.
        let bridge = worker_bridge();
        let req = start_request();
        let key = key_of(&req);
        let task_queue = req.task_queue.clone();
        bridge.start(req).await.expect("start");
        bridge
            .poll_activity_task(&task_queue, "w")
            .await
            .expect("poll")
            .expect("task");
        bridge
            .request_cancel(
                key.clone(),
                "op".to_owned(),
                "rid-1".to_owned(),
                "r".to_owned(),
            )
            .await
            .expect("first cancel");
        bridge
            .request_cancel(
                key.clone(),
                "op".to_owned(),
                "rid-1".to_owned(),
                "r".to_owned(),
            )
            .await
            .expect("same request id is idempotent");
        match bridge
            .request_cancel(
                key.clone(),
                "op".to_owned(),
                "rid-2".to_owned(),
                "r".to_owned(),
            )
            .await
        {
            Err(EdgeError::FailedPrecondition(m)) => {
                assert_eq!(m, "cancellation already requested with request ID rid-1")
            }
            other => panic!("expected FailedPrecondition, got {other:?}"),
        }
        assert_eq!(bridge.describe(key).await.unwrap().cancel_reason, "r");
    }

    #[tokio::test]
    async fn terminate_request_id_dedup() {
        // Repeated terminate: same request_id is an idempotent no-op; a different one
        // is FailedPrecondition (activity.go:359-370 @ v1.31.0;
        // standalone_activity_test.go:1983).
        let bridge = bridge(true);
        let req = start_request();
        let key = key_of(&req);
        bridge.start(req).await.expect("start");
        bridge
            .terminate(
                key.clone(),
                "stop".to_owned(),
                "t-1".to_owned(),
                "operator".to_owned(),
            )
            .await
            .expect("terminate");
        bridge
            .terminate(
                key.clone(),
                "stop".to_owned(),
                "t-1".to_owned(),
                "operator".to_owned(),
            )
            .await
            .expect("same request id is idempotent");
        match bridge
            .terminate(
                key.clone(),
                "stop".to_owned(),
                "t-2".to_owned(),
                "operator".to_owned(),
            )
            .await
        {
            Err(EdgeError::FailedPrecondition(m)) => {
                assert_eq!(m, "already terminated with request ID t-1")
            }
            other => panic!("expected FailedPrecondition, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn heartbeat_by_id_records_details_on_current_run() {
        // RecordActivityTaskHeartbeatById resolves the run by id (attempt-1 token) and
        // records details, returning cancel_requested (standalone_activity_test.go:4716).
        let bridge = worker_bridge();
        let req = start_request();
        let key = key_of(&req);
        let task_queue = req.task_queue.clone();
        bridge.start(req).await.expect("start");
        bridge
            .poll_activity_task(&task_queue, "worker-1")
            .await
            .expect("poll")
            .expect("a queued task");

        let details = vec![8, 9];
        let cancel_requested = bridge
            .heartbeat_by_id(
                &key.namespace_id,
                &key.business_id,
                &key.run_id,
                details.clone(),
            )
            .await
            .expect("heartbeat by id");
        assert!(!cancel_requested);
        assert_eq!(
            bridge.describe(key).await.unwrap().heartbeat_details,
            details
        );
    }

    #[tokio::test]
    async fn polled_task_carries_timeouts_and_times() {
        // The dispatched task echoes the started activity's timeouts and times so the
        // worker can honor them (standalone_activity_test.go:322-329).
        let bridge = worker_bridge();
        let mut req = start_request();
        req.start_to_close_nanos = 7 * SEC;
        req.priority = vec![1, 2];
        req.header = vec![3, 4];
        let task_queue = req.task_queue.clone();
        bridge.start(req).await.expect("start");
        let task = bridge
            .poll_activity_task(&task_queue, "w")
            .await
            .expect("poll")
            .expect("task");
        assert_eq!(task.start_to_close_nanos, 7 * SEC);
        assert!(task.scheduled_time_nanos > 0, "scheduled time is set");
        assert!(
            task.started_time_nanos > 0,
            "started time is the pickup time"
        );
        assert_eq!(task.priority, vec![1, 2]);
        assert_eq!(task.header, vec![3, 4]);
        assert!(
            task.heartbeat_details.is_empty(),
            "no prior heartbeat on the first attempt"
        );
    }

    #[tokio::test]
    async fn worker_heartbeat_on_terminal_activity_is_not_found() {
        // A heartbeat with a stale token (the activity already completed) is the same
        // NotFound the terminal responses give (standalone_activity_test.go:4179).
        let bridge = worker_bridge();
        let req = start_request();
        let key = key_of(&req);
        let task_queue = req.task_queue.clone();
        bridge.start(req).await.expect("start");
        let task = bridge
            .poll_activity_task(&task_queue, "worker-1")
            .await
            .expect("poll")
            .expect("a queued task");
        bridge
            .respond_activity_task_completed(
                &task.task_token,
                &key.namespace_id,
                Vec::new(),
                "worker-1".to_owned(),
            )
            .await
            .expect("complete");
        match bridge
            .record_heartbeat(&task.task_token, &key.namespace_id, vec![1])
            .await
        {
            Err(EdgeError::NotFound(m)) => {
                assert_eq!(m, format!("activity not found for ID: {}", key.business_id))
            }
            other => panic!("expected NotFound, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn worker_respond_canceled_acknowledges_cancel_request() {
        // RespondActivityTaskCanceled drives CANCEL_REQUESTED → CANCELED
        // (`TransitionCanceled`, legal only from CANCEL_REQUESTED, `statemachine.go:307
        // @ v1.31.0`): poll to Started, request cancel, then the worker acknowledges.
        let bridge = worker_bridge();
        let req = start_request();
        let key = key_of(&req);
        let task_queue = req.task_queue.clone();
        bridge.start(req).await.expect("start");

        let task = bridge
            .poll_activity_task(&task_queue, "worker-1")
            .await
            .expect("poll")
            .expect("a queued task");
        bridge
            .request_cancel(
                key.clone(),
                "operator".to_owned(),
                String::new(),
                String::new(),
            )
            .await
            .expect("request cancel");

        bridge
            .respond_activity_task_canceled(&task.task_token, &key.namespace_id, Vec::new())
            .await
            .expect("canceled");
        assert_eq!(
            bridge.describe(key).await.unwrap().status,
            ActivityStatus::Canceled
        );
    }

    #[tokio::test]
    async fn worker_respond_canceled_with_stale_token_is_rejected() {
        // The cancel respond path shares `validate_token`: a token for an attempt
        // that already resolved (here, completed) is NotFound — the attempt the
        // token named no longer exists (`TestCancel/StaleToken @ v1.31.0`).
        let bridge = worker_bridge();
        let req = start_request();
        let key = key_of(&req);
        let task_queue = req.task_queue.clone();
        bridge.start(req).await.expect("start");

        let task = bridge
            .poll_activity_task(&task_queue, "worker-1")
            .await
            .expect("poll")
            .expect("a queued task");
        bridge
            .respond_activity_task_completed(
                &task.task_token,
                &key.namespace_id,
                vec![1],
                "worker-1".to_owned(),
            )
            .await
            .expect("complete");

        let err = bridge
            .respond_activity_task_canceled(&task.task_token, &key.namespace_id, Vec::new())
            .await
            .unwrap_err();
        assert!(matches!(err, EdgeError::NotFound(_)));
    }

    #[tokio::test]
    async fn worker_respond_token_from_other_namespace_is_rejected() {
        // A token whose namespace differs from the request's is rejected before any
        // state change, mirroring the namespace-validator interceptor's
        // `errTaskTokenNamespaceMismatch` (`MismatchedTokenNamespace @ v1.31.0`).
        let bridge = worker_bridge();
        let req = start_request();
        let task_queue = req.task_queue.clone();
        bridge.start(req).await.expect("start");

        let task = bridge
            .poll_activity_task(&task_queue, "worker-1")
            .await
            .expect("poll")
            .expect("a queued task");

        let err = bridge
            .respond_activity_task_completed(
                &task.task_token,
                "some-other-namespace",
                vec![1],
                "worker-1".to_owned(),
            )
            .await
            .unwrap_err();
        match err {
            EdgeError::BadRequest(message) => {
                assert_eq!(
                    message,
                    "Operation requested with a token from a different namespace."
                );
            }
            other => panic!("expected BadRequest, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn worker_token_component_ref_tamper_is_rejected() {
        // Wire-compatibility proof + `MismatchedTokenComponentRef @ v1.31.0`: the
        // issued token decodes as a `tokenspb.Task` carrying a `ChasmComponentRef`
        // (so the corpus's `tasktoken.Serializer` round-trip works). Swapping the
        // ref's namespace while leaving the top-level namespace intact passes the
        // interceptor check but fails `validateActivityTaskToken` ->
        // InvalidArgument "token does not match namespace".
        let bridge = worker_bridge();
        let req = start_request();
        let key = key_of(&req);
        let task_queue = req.task_queue.clone();
        bridge.start(req).await.expect("start");

        let task = bridge
            .poll_activity_task(&task_queue, "worker-1")
            .await
            .expect("poll")
            .expect("a queued task");

        // The token is a real tokenspb.Task with a populated component_ref.
        let mut wire = ProtoTaskToken::decode(task.task_token.as_slice()).expect("decode Task");
        assert_eq!(wire.namespace_id, key.namespace_id);
        let mut component_ref =
            ProtoComponentRef::decode(wire.component_ref.as_slice()).expect("decode ref");
        assert_eq!(component_ref.namespace_id, key.namespace_id);
        assert_eq!(component_ref.business_id, key.business_id);

        // Tamper only the ref's namespace, re-serialize, and respond in the original
        // namespace (so check 1 passes, check 2 fires).
        component_ref.namespace_id = "tampered-namespace".to_owned();
        wire.component_ref = component_ref.encode_to_vec();
        let tampered = wire.encode_to_vec();

        let err = bridge
            .respond_activity_task_completed(
                &tampered,
                &key.namespace_id,
                vec![1],
                "worker-1".to_owned(),
            )
            .await
            .unwrap_err();
        match err {
            EdgeError::BadRequest(message) => {
                assert_eq!(message, "token does not match namespace");
            }
            other => panic!("expected BadRequest, got {other:?}"),
        }
    }
}
