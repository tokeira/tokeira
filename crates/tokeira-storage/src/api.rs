use anyhow::Result;
use async_trait::async_trait;
use time::OffsetDateTime;
use tokeira_kernel::{
    ActivityOp, DispatchOp, HistoryEvent, LoadedRun, ProjectionOp, TimerOp, Transition,
    WorkflowState,
};
use tokeira_types::{
    ExecutionRef, NamespaceId, Payloads, ProjectionCursor, QueueKey, RequestId, RunId,
    RunKey, ShardEpoch, ShardId, TransitionSeq, WorkerIdentity, WorkflowId,
};

/// Write result from persisting one authoritative
/// run transition.
///
/// See [`RunRepository::commit_transition`] for the
/// commit protocol and OCC fencing semantics.
#[derive(Clone, Debug, PartialEq)]
pub enum CommitResult {
    /// Transition was durably applied; contains the
    /// new authoritative workflow state.
    Applied { new_state: WorkflowState },
    /// OCC fence check failed — the durable
    /// `transition_seq` has moved past the expected
    /// value. The runtime should reload and retry.
    Conflict { reason: String },
    /// A request with the same dedupe key was already
    /// committed. The caller can short-circuit.
    Duplicate,
}

/// Durable request-dedupe record.
///
/// Insight: the request id is stored at workflow scope here because the minimal
/// workspace has not yet implemented continue-as-new chains or reuse policies.
/// That is intentionally conservative: it prevents ambiguous duplicate handling
/// now, and leaves a clear TODO for future refinement when execution-chain
/// semantics become richer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestRecord {
    /// Namespace that owns this workflow.
    pub namespace_id: NamespaceId,
    /// Workflow-scoped identifier for dedupe lookup.
    pub workflow_id: WorkflowId,
    /// Run that first committed this request.
    pub run_id: RunId,
    /// Durable storage key for the owning run.
    pub run_key: RunKey,
    /// The external request id being deduplicated.
    pub request_id: RequestId,
    /// Transition that first persisted this request.
    pub first_seen_transition_seq: TransitionSeq,
}

/// Development/test view of one persisted transition.
///
/// Production DSQL storage will likely materialize this information across
/// multiple tables. The dev store keeps an audit-style record so semantic tests
/// can verify that history and derived ops are all persisted together.
#[derive(Clone, Debug, PartialEq)]
pub struct TransitionAuditRecord {
    /// Durable storage key for the run.
    pub run_key: RunKey,
    /// Monotonic sequence assigned to this transition.
    pub transition_seq: TransitionSeq,
    /// History events appended by this transition.
    pub history_events: Vec<HistoryEvent>,
    /// Activity side-table mutations.
    pub activity_ops: Vec<ActivityOp>,
    /// Timer bucket mutations.
    pub timer_ops: Vec<TimerOp>,
    /// Dispatch queue operations (enqueue tasks).
    pub dispatch_ops: Vec<DispatchOp>,
    /// Projection log entries for visibility sinks.
    pub projection_ops: Vec<ProjectionOp>,
}

/// Query surface the runtime needs from storage.
///
/// The interface is intentionally shaped around semantics rather than a
/// particular SQL schema. That keeps the rest of the workspace honest: callers
/// ask for "resolve this execution reference" or "read history", not "query
/// table X with join Y".
#[async_trait]
pub trait RunRepository: Send + Sync {
    /// Resolve an execution reference to a concrete durable run key.
    ///
    /// Semantics:
    /// - when `execution.run_id` is `None`, return the current open run if one
    ///   exists;
    /// - when `execution.run_id` is `Some`, return that specific run if known,
    ///   even if it is closed.
    async fn resolve_execution(&self, execution: &ExecutionRef)
    -> Result<Option<RunKey>>;

    /// Load the full durable state for a run, or
    /// [`LoadedRun::Absent`] if the key is unknown.
    async fn load_run(&self, run_key: RunKey) -> Result<LoadedRun>;

    /// Read the authoritative history stream after a known event id.
    async fn read_history(
        &self,
        run_key: RunKey,
        after_event_id: i64,
        limit: usize,
    ) -> Result<Vec<HistoryEvent>>;

    /// Lookup request dedupe state for a workflow execution reference.
    async fn lookup_request_dedupe(
        &self,
        execution: &ExecutionRef,
        request_id: &RequestId,
    ) -> Result<Option<RequestRecord>>;

    /// Read the persisted transition audit log for a run.
    ///
    /// TODO(storage): once a real DSQL backend lands, decide whether this stays
    /// in the main trait, moves behind a test-only feature, or becomes an admin
    /// API. It is extremely useful for semantic tests right now.
    async fn read_transition_audit(
        &self,
        run_key: RunKey,
    ) -> Result<Vec<TransitionAuditRecord>>;

    /// Atomically persist a kernel-produced transition.
    ///
    /// The implementation must check `transition.expected_seq`
    /// against the durable `transition_seq` and return
    /// [`CommitResult::Conflict`] on mismatch (OCC fence).
    /// See the [storage architecture docs](../../docs/crates/storage.md)
    /// for the full fenced-commit model.
    async fn commit_transition(
        &self,
        run_key: RunKey,
        transition: Transition,
    ) -> Result<CommitResult>;

    /// Return workflow tasks that are scheduled but not
    /// yet started for the given queue, up to `limit`.
    async fn list_dispatchable_workflow_tasks(
        &self,
        queue: &QueueKey,
        limit: usize,
    ) -> Result<Vec<DispatchableWorkflowTask>>;

    /// Return activity tasks awaiting dispatch for the
    /// given queue, up to `limit`.
    async fn list_dispatchable_activity_tasks(
        &self,
        queue: &QueueKey,
        limit: usize,
    ) -> Result<Vec<DispatchableActivityTask>>;

    /// Durably persist unmatched tasks to the dispatch
    /// backlog for later retry.
    async fn persist_to_backlog(&self, entries: Vec<BacklogEntry>) -> Result<()>;

    /// Remove and return up to `limit` backlog entries
    /// for the given queue (FIFO order).
    async fn drain_backlog(
        &self,
        queue: &QueueKey,
        limit: usize,
    ) -> Result<Vec<BacklogEntry>>;

    /// Return timers whose `fire_at` is at or before
    /// `now`, up to `limit`.
    async fn list_due_timers(
        &self,
        now: OffsetDateTime,
        limit: usize,
    ) -> Result<Vec<DueTimer>>;

    // TODO(storage): add sweep methods for activity tasks, archival eligibility,
    // namespace-scoped pagination, and explicit current-execution conflict
    // policies (reuse, reject, allow-after-close, continue-as-new chains).
}

/// A workflow task that is ready for dispatch to a
/// worker. Produced by [`RunRepository::list_dispatchable_workflow_tasks`].
#[derive(Clone, Debug, PartialEq)]
pub struct DispatchableWorkflowTask {
    /// Durable storage key for the owning run.
    pub run_key: RunKey,
    /// Task queue this task belongs to.
    pub queue: QueueKey,
    /// Monotonic sequence within the run's workflow
    /// task chain.
    pub logical_seq: tokeira_types::LogicalTaskSeq,
    /// Worker that holds sticky cache affinity, if any.
    pub sticky_preferred: Option<WorkerIdentity>,
    /// When the sticky affinity expires.
    pub sticky_expires_at: Option<OffsetDateTime>,
}

/// An activity task that is ready for dispatch to a
/// worker. Produced by [`RunRepository::list_dispatchable_activity_tasks`].
#[derive(Clone, Debug, PartialEq)]
pub struct DispatchableActivityTask {
    /// Durable storage key for the owning run.
    pub run_key: RunKey,
    /// Task queue this activity is assigned to.
    pub queue: QueueKey,
    /// Application-level activity identifier.
    pub activity_id: String,
    /// Serialized input payloads for the activity.
    pub input: Payloads,
    /// History event id that scheduled this activity.
    pub schedule_event_id: i64,
    /// Current retry attempt (starts at 1).
    pub attempt: u32,
}

/// Discriminant for the type of task stored in the
/// dispatch backlog.
#[derive(Clone, Debug, PartialEq)]
pub enum BacklogTaskKind {
    /// A workflow task awaiting dispatch.
    Workflow,
    /// An activity task awaiting dispatch.
    Activity { activity_id: String },
}

/// A single entry in the durable dispatch backlog.
///
/// Tasks land here when no worker is immediately
/// available. The runtime drains the backlog on the
/// next sweep cycle.
#[derive(Clone, Debug, PartialEq)]
pub struct BacklogEntry {
    /// Durable storage key for the owning run.
    pub run_key: RunKey,
    /// Task queue this entry targets.
    pub queue: QueueKey,
    /// Whether this is a workflow or activity task.
    pub kind: BacklogTaskKind,
    /// Monotonic insertion order within the backlog.
    pub insertion_seq: u64,
}

/// Policy for handling a start-workflow request when
/// a current execution already exists for the same
/// `(namespace, workflow_id)`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CurrentExecutionConflictPolicy {
    /// Reject the new start if an open execution
    /// already exists (default).
    #[default]
    Reject,
    /// Allow the new start only after the existing
    /// execution has closed.
    AllowAfterClose,
}

/// A timer whose `fire_at` deadline has been reached.
#[derive(Clone, Debug, PartialEq)]
pub struct DueTimer {
    /// Durable storage key for the owning run.
    pub run_key: RunKey,
    /// Application-level timer identifier.
    pub timer_id: String,
}

/// Read-only interface for projection workers.
///
/// Projection sinks consume a partitioned log of
/// [`ProjectionOp`](tokeira_kernel::ProjectionOp)s
/// to maintain visibility and search-attribute tables.
#[async_trait]
pub trait ProjectionLog: Send + Sync {
    /// Read projection records from `cursor` forward,
    /// returning at most `limit` records and an
    /// updated cursor for the next call.
    async fn read_from(
        &self,
        cursor: &ProjectionCursor,
        limit: usize,
    ) -> Result<ProjectionBatch>;
}

/// One row in the projection log, grouping all
/// projection ops from a single transition.
#[derive(Clone, Debug, PartialEq)]
pub struct ProjectionRecord {
    /// Hash-based partition for fan-out distribution.
    pub partition_id: u32,
    /// Fanout factor (typically 1 for the dev store).
    pub fanout: u16,
    /// Durable storage key for the source run.
    pub run_key: RunKey,
    /// Transition that produced these ops.
    pub transition_seq: tokeira_types::TransitionSeq,
    /// The projection operations to apply.
    pub ops: Vec<tokeira_kernel::ProjectionOp>,
}

/// A page of projection records returned by
/// [`ProjectionLog::read_from`].
#[derive(Clone, Debug, PartialEq)]
pub struct ProjectionBatch {
    /// The projection records in this page.
    pub records: Vec<ProjectionRecord>,
    /// Cursor to pass into the next `read_from` call.
    pub next_cursor: ProjectionCursor,
}

/// Lease repository for shard or bundle ownership.
///
/// Leases are epoch-fenced: a successful acquire or
/// renew returns the current epoch, and a stale epoch
/// causes rejection. See the
/// [storage docs](../../docs/crates/storage.md) for
/// the fenced-lease model.
#[async_trait]
pub trait LeaseRepository: Send + Sync {
    /// Attempt to acquire ownership of `bundle`.
    ///
    /// Returns [`LeaseOutcome::Acquired`] on success,
    /// or [`LeaseOutcome::Rejected`] if another owner
    /// already holds the lease.
    async fn try_acquire_bundle(
        &self,
        bundle: ShardId,
        owner: String,
    ) -> Result<LeaseOutcome>;
    /// Renew an existing lease for `bundle` at the
    /// given `epoch`. Fails if the epoch is stale.
    async fn renew_bundle(
        &self,
        bundle: ShardId,
        owner: String,
        epoch: ShardEpoch,
    ) -> Result<LeaseOutcome>;

    // TODO(storage): support explicit relinquish, generation-aware placement,
    // and bulk lease observation for the controller.
}

/// Outcome of a lease acquire or renew attempt.
#[derive(Clone, Debug, PartialEq)]
pub enum LeaseOutcome {
    /// Lease successfully acquired at this epoch.
    Acquired { epoch: ShardEpoch },
    /// Lease successfully renewed at this epoch.
    Renewed { epoch: ShardEpoch },
    /// Lease is held by another owner; includes the
    /// current owner and epoch for diagnostics.
    Rejected {
        current_owner: String,
        current_epoch: ShardEpoch,
    },
}

/// Database work classes used by the future connection
/// director.
///
/// Priority order (highest first): `Control` >
/// `Commit` > `Read` > `Projection` > `Maintenance`.
/// See [060-connection-management](../../docs/architecture/060-connection-management.md).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DbClass {
    /// Shard lease and cluster-control operations.
    Control,
    /// State-transition commits (OCC fenced writes).
    Commit,
    /// Read-path queries (load run, read history).
    Read,
    /// Projection sink writes.
    Projection,
    /// Background housekeeping (archival, cleanup).
    Maintenance,
}

/// Small abstraction for future DSQL connection-budget
/// control.
///
/// In the dev store this is a no-op. A production
/// implementation will enforce per-class connection
/// limits and open-rate budgets.
#[async_trait]
pub trait ConnectionDirector: Send + Sync {
    /// Acquire a connection permit for the given work
    /// class. Blocks until a permit is available.
    async fn acquire(&self, class: DbClass) -> Result<DbPermit>;
}

/// A held connection permit, scoped to a [`DbClass`].
///
/// TODO(storage): replace this trivial type with a
/// real session/lease wrapper in the DSQL-backed
/// implementation.
#[derive(Debug)]
pub struct DbPermit {
    /// The work class this permit was issued for.
    pub class: DbClass,
}

#[async_trait]
impl<T> RunRepository for std::sync::Arc<T>
where
    T: RunRepository + ?Sized,
{
    async fn resolve_execution(
        &self,
        execution: &ExecutionRef,
    ) -> Result<Option<RunKey>> {
        (**self).resolve_execution(execution).await
    }

    async fn load_run(&self, run_key: RunKey) -> Result<LoadedRun> {
        (**self).load_run(run_key).await
    }

    async fn read_history(
        &self,
        run_key: RunKey,
        after_event_id: i64,
        limit: usize,
    ) -> Result<Vec<HistoryEvent>> {
        (**self).read_history(run_key, after_event_id, limit).await
    }

    async fn lookup_request_dedupe(
        &self,
        execution: &ExecutionRef,
        request_id: &RequestId,
    ) -> Result<Option<RequestRecord>> {
        (**self).lookup_request_dedupe(execution, request_id).await
    }

    async fn read_transition_audit(
        &self,
        run_key: RunKey,
    ) -> Result<Vec<TransitionAuditRecord>> {
        (**self).read_transition_audit(run_key).await
    }

    async fn commit_transition(
        &self,
        run_key: RunKey,
        transition: Transition,
    ) -> Result<CommitResult> {
        (**self).commit_transition(run_key, transition).await
    }

    async fn list_dispatchable_workflow_tasks(
        &self,
        queue: &QueueKey,
        limit: usize,
    ) -> Result<Vec<DispatchableWorkflowTask>> {
        (**self)
            .list_dispatchable_workflow_tasks(queue, limit)
            .await
    }

    async fn list_dispatchable_activity_tasks(
        &self,
        queue: &QueueKey,
        limit: usize,
    ) -> Result<Vec<DispatchableActivityTask>> {
        (**self)
            .list_dispatchable_activity_tasks(queue, limit)
            .await
    }

    async fn persist_to_backlog(&self, entries: Vec<BacklogEntry>) -> Result<()> {
        (**self).persist_to_backlog(entries).await
    }

    async fn drain_backlog(
        &self,
        queue: &QueueKey,
        limit: usize,
    ) -> Result<Vec<BacklogEntry>> {
        (**self).drain_backlog(queue, limit).await
    }

    async fn list_due_timers(
        &self,
        now: OffsetDateTime,
        limit: usize,
    ) -> Result<Vec<DueTimer>> {
        (**self).list_due_timers(now, limit).await
    }
}

#[async_trait]
impl<T> ProjectionLog for std::sync::Arc<T>
where
    T: ProjectionLog + ?Sized,
{
    async fn read_from(
        &self,
        cursor: &ProjectionCursor,
        limit: usize,
    ) -> Result<ProjectionBatch> {
        (**self).read_from(cursor, limit).await
    }
}

#[async_trait]
impl<T> LeaseRepository for std::sync::Arc<T>
where
    T: LeaseRepository + ?Sized,
{
    async fn try_acquire_bundle(
        &self,
        bundle: ShardId,
        owner: String,
    ) -> Result<LeaseOutcome> {
        (**self).try_acquire_bundle(bundle, owner).await
    }

    async fn renew_bundle(
        &self,
        bundle: ShardId,
        owner: String,
        epoch: ShardEpoch,
    ) -> Result<LeaseOutcome> {
        (**self).renew_bundle(bundle, owner, epoch).await
    }
}

#[async_trait]
impl<T> ConnectionDirector for std::sync::Arc<T>
where
    T: ConnectionDirector + ?Sized,
{
    async fn acquire(&self, class: DbClass) -> Result<DbPermit> {
        (**self).acquire(class).await
    }
}
