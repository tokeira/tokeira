use time::Duration;
use smallvec::SmallVec;
use time::OffsetDateTime;
use tokeira_types::{
    ExecutionStatus, Memo, QueueKey, RequestId, SearchAttributes, TransitionSeq,
};

use crate::{event::HistoryEvent, state::{ActivityState, TimerState, WorkflowState}};

/// The full result of one authoritative transition.
///
/// A storage implementation may decompose this into multiple tables or rows,
/// but the semantic contract is that these fields describe *one fenced commit*.
/// If a backend cannot make these changes appear atomically, it is not yet a
/// faithful implementation of the architecture docs.
#[derive(Clone, Debug, PartialEq)]
pub struct Transition {
    pub expected_seq: TransitionSeq,
    pub next_state: WorkflowState,
    pub history_events: SmallVec<[HistoryEvent; 8]>,
    pub request_dedupe_ops: SmallVec<[RequestDedupeOp; 1]>,
    pub activity_ops: SmallVec<[ActivityOp; 4]>,
    pub timer_ops: SmallVec<[TimerOp; 4]>,
    pub dispatch_ops: SmallVec<[DispatchOp; 4]>,
    pub projection_ops: SmallVec<[ProjectionOp; 8]>,
}

/// Request-dedupe is part of the authoritative write set.
///
/// Insight: the request id is intentionally carried beside history rather than
/// being treated as an edge-only concern. A durable execution platform must be
/// able to survive retries and partial failures without "maybe applied"
/// ambiguity. Persisting request identity in the same fenced commit as the
/// history batch is how we keep that story honest.
#[derive(Clone, Debug, PartialEq)]
pub struct RequestDedupeOp {
    pub request_id: RequestId,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ActivityOp {
    Upsert(ActivityState),
    Delete { activity_id: String },
}

#[derive(Clone, Debug, PartialEq)]
pub enum TimerOp {
    Upsert(TimerState),
    Delete { timer_id: String },
}

#[derive(Clone, Debug, PartialEq)]
pub enum DispatchOp {
    EnqueueWorkflowTask {
        queue: QueueKey,
        logical_seq: tokeira_types::LogicalTaskSeq,
        sticky_preferred: Option<tokeira_types::WorkerIdentity>,
    },
    EnqueueActivityTask {
        queue: QueueKey,
        activity_id: String,
        schedule_event_id: i64,
        attempt: u32,
        schedule_to_close_timeout: Option<Duration>,
        schedule_to_start_timeout: Option<Duration>,
        start_to_close_timeout: Option<Duration>,
        heartbeat_timeout: Option<Duration>,
    },
}

/// Projection operations are the contract between the correctness path and the
/// read-model plane. They are intentionally semantic, not SQL-shaped.
#[derive(Clone, Debug, PartialEq)]
pub enum ProjectionOp {
    UpsertExecution {
        status: ExecutionStatus,
        memo_patch: Memo,
        search_attr_patch: SearchAttributes,
    },
    CloseExecution {
        status: ExecutionStatus,
        closed_at: OffsetDateTime,
    },
}
