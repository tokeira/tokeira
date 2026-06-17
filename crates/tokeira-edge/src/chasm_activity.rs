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
use serde::{Deserialize, Serialize};
use tokeira_chasm::{
    ChasmError, Component as _, ComponentRef, DispatchableTask, ExecutionKey, VersionedTransition,
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

/// The opaque worker task token: enough to re-address the execution and fence the
/// completion against the attempt it was issued for. Serialized as JSON (the edge's
/// existing serde stack) — it is internal, never on the public SDK wire as a typed
/// message, so the encoding is ours to choose.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ActivityTaskToken {
    /// Namespace id the execution belongs to.
    namespace_id: String,
    /// Application-level activity id (the execution business id).
    activity_id: String,
    /// Run id of the dispatched instance.
    run_id: String,
    /// The attempt stamp the dispatch was issued for (fences a stale completion).
    stamp: i64,
}

impl ActivityTaskToken {
    fn encode(&self) -> EdgeResult<Vec<u8>> {
        serde_json::to_vec(self)
            .map_err(|e| EdgeError::Internal(format!("encode activity task token: {e}")))
    }

    fn decode(bytes: &[u8]) -> EdgeResult<Self> {
        serde_json::from_slice(bytes)
            .map_err(|_| EdgeError::BadRequest("malformed activity task token".to_owned()))
    }

    fn execution_key(&self) -> ExecutionKey {
        ExecutionKey::new(
            self.namespace_id.clone(),
            self.activity_id.clone(),
            self.run_id.clone(),
        )
    }
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

    /// The registry-assigned archetype id for the activity component. The
    /// visibility plane is archetype-neutral, so the edge supplies this to scope
    /// `ListActivityExecutions`/`CountActivityExecutions` to activities (Req 13.1).
    pub fn archetype_id(&self) -> ArchetypeId {
        ArchetypeId(self.archetype_id)
    }

    /// Whether `task_token` is one this bridge issued (a standalone-activity
    /// token). The gRPC layer uses this to route a worker response onto the CHASM
    /// path or fall through to the workflow-activity path — standalone and
    /// workflow activities share the `RespondActivityTask*` RPCs.
    pub fn owns_task_token(&self, task_token: &[u8]) -> bool {
        ActivityTaskToken::decode(task_token).is_ok()
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
    pub async fn start(&self, req: StartActivity) -> EdgeResult<ComponentRef> {
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
            ..ActivityState::default()
        };

        let typed = TypedEngine::<ActivityExecution>::new(&self.engine);
        let reference = typed
            .start(key, state, req.request_id)
            .await
            .map_err(map_chasm_err)?;
        // The initial Scheduled transition bumps attempt/stamp and schedules the
        // dispatch task + schedule-to-start/close timers.
        let (_, outcome) = typed
            .update(&reference, |activity, ctx| {
                activity.apply(ActivityEvent::Scheduled, ctx)
            })
            .await
            .map_err(map_chasm_err)?;
        Ok(outcome.reference)
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
    ) -> EdgeResult<()> {
        self.apply_event(key, ActivityEvent::Started { started_time_nanos })
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
            self.record_started(entry.key.clone(), started_at).await?;
            let token = ActivityTaskToken {
                namespace_id: entry.key.namespace_id.clone(),
                activity_id: entry.key.business_id.clone(),
                run_id: entry.key.run_id.clone(),
                stamp: entry.stamp,
            };
            return Ok(Some(PolledActivityTask {
                task_token: token.encode()?,
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
        result: Vec<u8>,
    ) -> EdgeResult<()> {
        self.ensure_enabled()?;
        let token = ActivityTaskToken::decode(task_token)?;
        self.fence_token(&token).await?;
        self.record_completed(token.execution_key(), result).await
    }

    /// Worker-facing: fail the activity attempt named by `task_token`.
    pub async fn respond_activity_task_failed(
        &self,
        task_token: &[u8],
        failure: String,
    ) -> EdgeResult<()> {
        self.ensure_enabled()?;
        let token = ActivityTaskToken::decode(task_token)?;
        self.fence_token(&token).await?;
        self.record_failed(token.execution_key(), failure).await
    }

    /// Reject a worker response whose token names a superseded attempt (a retry has
    /// moved the live stamp past the one the dispatch was issued for).
    async fn fence_token(&self, token: &ActivityTaskToken) -> EdgeResult<()> {
        let snapshot = self
            .engine
            .read_component(&token.execution_key())
            .await
            .map_err(map_chasm_err)?;
        let bytes = snapshot
            .data
            .ok_or_else(|| EdgeError::NotFound("activity execution not found".to_owned()))?;
        let state = ActivityState::decode(bytes.as_slice())
            .map_err(|e| EdgeError::Internal(format!("decode activity state: {e}")))?;
        if state.stamp != token.stamp {
            return Err(EdgeError::FailedPrecondition(
                "activity attempt superseded".to_owned(),
            ));
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
        }
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
    async fn full_lifecycle_start_started_completed() {
        let bridge = bridge(true);
        let req = start_request();
        let key = key_of(&req);
        bridge.start(req).await.expect("start");

        bridge
            .record_started(key.clone(), 5 * SEC)
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
        let scheduled_ref = bridge.start(req).await.expect("start");
        let since = scheduled_ref.execution_versioned_transition;

        // Advance (start), then a poll since the scheduled VT resolves.
        bridge
            .record_started(key.clone(), SEC)
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
            .poll_activity_task(&task_queue)
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
            .respond_activity_task_completed(&task.task_token, vec![7, 7])
            .await
            .expect("complete");
        let done = bridge.describe(key).await.unwrap();
        assert_eq!(done.status, ActivityStatus::Completed);
        assert_eq!(done.result, vec![7, 7]);

        // The queue is drained — a second poll finds nothing.
        assert!(
            bridge
                .poll_activity_task(&task_queue)
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
            .poll_activity_task(&task_queue)
            .await
            .expect("poll")
            .expect("a queued task");

        // Terminate the activity (now terminal); a late worker completion for the
        // dispatched attempt must be rejected, not applied — `Completed` is illegal
        // from `Terminated`.
        bridge
            .terminate(key, "operator stop".to_owned())
            .await
            .expect("terminate");
        let err = bridge
            .respond_activity_task_completed(&task.task_token, vec![1])
            .await
            .unwrap_err();
        assert!(matches!(err, EdgeError::FailedPrecondition(_)));
    }

    #[tokio::test]
    async fn worker_poll_empty_queue_returns_none() {
        let bridge = worker_bridge();
        assert!(
            bridge
                .poll_activity_task("idle-queue")
                .await
                .expect("poll")
                .is_none()
        );
    }
}
