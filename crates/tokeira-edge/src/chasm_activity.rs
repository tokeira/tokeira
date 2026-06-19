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
    ActivityStatus, DISPATCH_TASK_ID, DispatchTask, validate_and_normalize,
};
use tokeira_runtime::chasm::{
    ChasmEngine, DispatchSink, Engine, PollOutcome, PollRequest, TypedEngine,
};
use tokeira_types::ArchetypeId;

use crate::errors::{EdgeError, EdgeResult};

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
#[derive(Debug, Clone, PartialEq, Eq)]
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

/// One queued dispatch: the execution to run and the attempt stamp it was scheduled
/// for. A worker poll reaps stale entries (stamp/status no longer current) rather
/// than dispatching them.
#[derive(Debug, Clone)]
struct DispatchEntry {
    key: ExecutionKey,
    stamp: i64,
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
}

impl ActivityDispatchQueue {
    /// Construct an empty queue.
    pub fn new() -> Self {
        Self::default()
    }

    fn enqueue(&self, task_queue: String, entry: DispatchEntry) {
        if let Ok(mut queues) = self.queues.lock() {
            queues.entry(task_queue).or_default().push_back(entry);
        }
    }

    fn dequeue(&self, task_queue: &str) -> Option<DispatchEntry> {
        self.queues
            .lock()
            .ok()
            .and_then(|mut queues| queues.get_mut(task_queue).and_then(VecDeque::pop_front))
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
        self.config.enable_standalone
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
        self.config.long_poll_timeout
    }

    /// Slack subtracted from the caller's deadline so an empty long-poll response
    /// is sent before the caller times out (`activity.longPollBuffer`, default 1s
    /// @ v1.31.0).
    pub fn long_poll_buffer(&self) -> std::time::Duration {
        self.config.long_poll_buffer
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
        if self.config.enable_standalone {
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

    /// Request cancellation of an activity (Requirement 11.8).
    pub async fn request_cancel(&self, key: ExecutionKey, identity: String) -> EdgeResult<()> {
        self.apply_event(key, ActivityEvent::CancelRequested { identity })
            .await
    }

    /// Terminate an activity (Requirement 11.8).
    pub async fn terminate(&self, key: ExecutionKey, reason: String) -> EdgeResult<()> {
        self.apply_event(key, ActivityEvent::Terminated { reason })
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
    pub async fn record_completed(&self, key: ExecutionKey, result: Vec<u8>) -> EdgeResult<()> {
        self.apply_event(key, ActivityEvent::Completed { result })
            .await
    }

    /// Worker-facing: record a terminal failure.
    pub async fn record_failed(&self, key: ExecutionKey, failure: String) -> EdgeResult<()> {
        self.apply_event(key, ActivityEvent::Failed { failure })
            .await
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
        while let Some(entry) = queue.dequeue(task_queue) {
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
            }));
        }
        Ok(None)
    }

    /// Worker-facing: complete the activity attempt named by `task_token`.
    pub async fn respond_activity_task_completed(
        &self,
        task_token: &[u8],
        request_namespace_id: &str,
        result: Vec<u8>,
    ) -> EdgeResult<()> {
        self.ensure_enabled()?;
        let token = ActivityTaskToken::decode(task_token)?;
        self.validate_token(&token, request_namespace_id).await?;
        self.record_completed(token.execution_key(), result).await
    }

    /// Worker-facing: fail the activity attempt named by `task_token`.
    pub async fn respond_activity_task_failed(
        &self,
        task_token: &[u8],
        request_namespace_id: &str,
        failure: String,
    ) -> EdgeResult<()> {
        self.ensure_enabled()?;
        let token = ActivityTaskToken::decode(task_token)?;
        self.validate_token(&token, request_namespace_id).await?;
        self.record_failed(token.execution_key(), failure).await
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
    ) -> EdgeResult<()> {
        self.ensure_enabled()?;
        let token = ActivityTaskToken::decode(task_token)?;
        self.validate_token(&token, request_namespace_id).await?;
        self.apply_event(token.execution_key(), ActivityEvent::Canceled)
            .await
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
        schedule_to_close_nanos: state.schedule_to_close_nanos,
        schedule_to_start_nanos: state.schedule_to_start_nanos,
        start_to_close_nanos: state.start_to_close_nanos,
        heartbeat_nanos: state.heartbeat_nanos,
        scheduled_time_nanos: state.scheduled_time_nanos,
        started_time_nanos: state.started_time_nanos,
        worker_identity: state.last_worker_identity,
        close_time_nanos: state.close_time_nanos,
        header: state.header,
        retry_policy: state.retry_policy,
        priority: state.priority,
        search_attributes: state.search_attributes,
        user_metadata: state.user_metadata,
        execution_vt,
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_chasm::{Library, Registry};
    use tokeira_chasm_activity::ActivityLibrary;
    use tokeira_runtime::chasm::{CollectingDispatchSink, CollectingVisibilitySink};
    use tokeira_storage::InMemoryChasmNodeStore;

    const SEC: i64 = 1_000_000_000;

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
            .record_completed(key.clone(), vec![9, 9])
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
        let bridge = bridge(false);
        let err = bridge.start(start_request()).await.unwrap_err();
        assert!(
            matches!(err, EdgeError::Unimplemented(ref m) if m == "Standalone activity is disabled")
        );
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
            .record_completed(key.clone(), vec![9, 9])
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
            .terminate(key.clone(), "operator stop".to_owned())
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
            .respond_activity_task_completed(&task.task_token, &key.namespace_id, vec![7, 7])
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
            .terminate(key, "operator stop".to_owned())
            .await
            .expect("terminate");
        // A response to a terminal activity is rejected NotFound — the attempt the
        // token named no longer exists (token validation, v1.31.0).
        let err = bridge
            .respond_activity_task_completed(&task.task_token, &namespace_id, vec![1])
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
            .request_cancel(key.clone(), "operator".to_owned())
            .await
            .expect("request cancel");

        bridge
            .respond_activity_task_canceled(&task.task_token, &key.namespace_id)
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
            .respond_activity_task_completed(&task.task_token, &key.namespace_id, vec![1])
            .await
            .expect("complete");

        let err = bridge
            .respond_activity_task_canceled(&task.task_token, &key.namespace_id)
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
            .respond_activity_task_completed(&task.task_token, "some-other-namespace", vec![1])
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
            .respond_activity_task_completed(&tampered, &key.namespace_id, vec![1])
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
