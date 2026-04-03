use smallvec::SmallVec;
use time::Duration;
use time::OffsetDateTime;
use tokeira_types::{
    ExecutionStatus, Memo, NamespaceId, Payloads, QueueKey, RequestId, RunId,
    SearchAttributes, TaskQueueName, TransitionSeq, WorkflowId, WorkflowType,
};

use crate::{
    event::HistoryEvent,
    state::{ActivityState, TimerState, WorkflowState},
};

/// The full result of one authoritative transition.
///
/// A storage implementation may decompose this into multiple tables or rows,
/// but the semantic contract is that these fields describe *one fenced commit*.
/// If a backend cannot make these changes appear atomically, it is not yet a
/// faithful implementation of the architecture docs.
#[derive(Clone, Debug, PartialEq)]
pub struct Transition {
    /// The `TransitionSeq` that was current when the kernel
    /// began processing. Storage uses this as an optimistic
    /// concurrency fence.
    pub expected_seq: TransitionSeq,
    /// Full next state (not a delta). Storage can do a
    /// complete replacement.
    pub next_state: WorkflowState,
    /// History events appended by this transition.
    pub history_events: SmallVec<[HistoryEvent; 8]>,
    /// Request IDs to persist for idempotent deduplication.
    pub request_dedupe_ops: SmallVec<[RequestDedupeOp; 1]>,
    /// Activity state mutations (upsert or delete).
    pub activity_ops: SmallVec<[ActivityOp; 4]>,
    /// Timer state mutations (upsert or delete).
    pub timer_ops: SmallVec<[TimerOp; 4]>,
    /// Side-effect dispatch operations (task enqueue, child
    /// start, external signal, etc.).
    pub dispatch_ops: SmallVec<[DispatchOp; 4]>,
    /// Projection-plane mutations for the read model.
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
    /// The request ID to persist for idempotent deduplication.
    pub request_id: RequestId,
}

/// Mutation to the activity state table.
#[derive(Clone, Debug, PartialEq)]
pub enum ActivityOp {
    /// Create or update an activity's durable state.
    Upsert(ActivityState),
    /// Remove a resolved activity from durable state.
    Delete { activity_id: String },
}

/// Mutation to the timer state table.
#[derive(Clone, Debug, PartialEq)]
pub enum TimerOp {
    /// Create or update a timer's durable state.
    Upsert(TimerState),
    /// Remove a fired or canceled timer from durable state.
    Delete { timer_id: String },
}

/// Side-effect operations that downstream layers must honour
/// after the transition is committed.
///
/// The kernel produces these but never executes them. The
/// runtime reads them from the committed transition and
/// performs the actual I/O.
#[derive(Clone, Debug, PartialEq)]
pub enum DispatchOp {
    /// Place a workflow task on the task queue for a worker
    /// to pick up.
    EnqueueWorkflowTask {
        queue: QueueKey,
        logical_seq: tokeira_types::LogicalTaskSeq,
        sticky_preferred: Option<tokeira_types::WorkerIdentity>,
    },
    /// Place an activity task on the task queue for a worker
    /// to pick up.
    EnqueueActivityTask {
        queue: QueueKey,
        activity_id: String,
        input: Payloads,
        schedule_event_id: i64,
        attempt: u32,
        schedule_to_close_timeout: Option<Duration>,
        schedule_to_start_timeout: Option<Duration>,
        start_to_close_timeout: Option<Duration>,
        heartbeat_timeout: Option<Duration>,
    },
    /// Initiate a child workflow execution.
    StartChildWorkflow {
        child_workflow_id: WorkflowId,
        namespace_id: NamespaceId,
        workflow_type: WorkflowType,
        task_queue: TaskQueueName,
        input: Payloads,
    },
    /// Forcibly terminate a child workflow (parent close
    /// policy).
    TerminateChild {
        child_workflow_id: WorkflowId,
        child_run_id: RunId,
        reason: String,
    },
    /// Send a cooperative cancel to a child workflow (parent
    /// close policy).
    CancelChild {
        child_workflow_id: WorkflowId,
        child_run_id: RunId,
        reason: String,
    },
    /// Deliver a signal to an external workflow.
    SignalExternalWorkflow {
        target_workflow_id: WorkflowId,
        target_run_id: Option<RunId>,
        signal_name: String,
        input: Payloads,
    },
    /// Request cancellation of an external workflow.
    RequestCancelExternalWorkflow {
        target_workflow_id: WorkflowId,
        target_run_id: Option<RunId>,
    },
    /// Schedule a Nexus operation on an external endpoint.
    ScheduleNexusOperation {
        operation_id: String,
        endpoint: String,
        service: String,
        operation: String,
        input: Payloads,
        schedule_to_close_timeout: Option<Duration>,
    },
    /// Cancel a previously scheduled Nexus operation.
    CancelNexusOperation { scheduled_event_id: i64 },
}

/// Projection operations are the contract between the correctness path and the
/// read-model plane. They are intentionally semantic, not SQL-shaped.
#[derive(Clone, Debug, PartialEq)]
pub enum ProjectionOp {
    /// Update the execution's status and metadata in the
    /// read model.
    UpsertExecution {
        status: ExecutionStatus,
        memo_patch: Memo,
        search_attr_patch: SearchAttributes,
    },
    /// Mark the execution as closed in the read model.
    CloseExecution {
        status: ExecutionStatus,
        closed_at: OffsetDateTime,
    },
}
