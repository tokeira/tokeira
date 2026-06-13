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

use std::sync::Arc;

use prost::Message as _;
use tokeira_chasm::{ChasmError, Component as _, ComponentRef, ExecutionKey, VersionedTransition};
use tokeira_chasm_activity::{
    ActivityConfig, ActivityEvent, ActivityExecution, ActivityRequest, ActivityState,
    ActivityStatus, validate_and_normalize,
};
use tokeira_runtime::chasm::{ChasmEngine, Engine, PollOutcome, PollRequest, TypedEngine};

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
    /// Result payload (set once completed).
    pub result: Vec<u8>,
    /// Failure message (set on a terminal failure).
    pub failure: String,
    /// The execution clock, used as the caller's long-poll token.
    pub execution_vt: VersionedTransition,
}

/// The standalone-activity bridge over a CHASM engine.
pub struct ActivityBridge {
    engine: Arc<ChasmEngine>,
    config: ActivityConfig,
    max_id_length: usize,
    archetype_id: u32,
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
        }
    }

    /// Whether standalone activities are enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enable_standalone
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
            .map_err(map_chasm_err)?;
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
        result: state.result,
        failure: state.failure,
        execution_vt,
    })
}

/// Map a [`ChasmError`] to the edge's [`EdgeError`] (which the gRPC layer maps to a
/// `tonic::Status`).
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
        let err = bridge.describe(key).await.unwrap_err();
        assert!(matches!(err, EdgeError::NotFound(_)));
    }
}
