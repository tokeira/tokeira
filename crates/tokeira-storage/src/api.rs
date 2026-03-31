use anyhow::Result;
use async_trait::async_trait;
use time::OffsetDateTime;
use tokeira_kernel::{
    ActivityOp, DispatchOp, HistoryEvent, LoadedRun, ProjectionOp, TimerOp, Transition,
    WorkflowState,
};
use tokeira_types::{
    ExecutionRef, NamespaceId, ProjectionCursor, QueueKey, RequestId, RunId, RunKey,
    ShardEpoch, ShardId, TransitionSeq, WorkerIdentity, WorkflowId,
};

/// Write result from persisting one authoritative run transition.
#[derive(Clone, Debug, PartialEq)]
pub enum CommitResult {
    Applied { new_state: WorkflowState },
    Conflict { reason: String },
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
    pub namespace_id: NamespaceId,
    pub workflow_id: WorkflowId,
    pub run_id: RunId,
    pub run_key: RunKey,
    pub request_id: RequestId,
    pub first_seen_transition_seq: TransitionSeq,
}

/// Development/test view of one persisted transition.
///
/// Production DSQL storage will likely materialize this information across
/// multiple tables. The dev store keeps an audit-style record so semantic tests
/// can verify that history and derived ops are all persisted together.
#[derive(Clone, Debug, PartialEq)]
pub struct TransitionAuditRecord {
    pub run_key: RunKey,
    pub transition_seq: TransitionSeq,
    pub history_events: Vec<HistoryEvent>,
    pub activity_ops: Vec<ActivityOp>,
    pub timer_ops: Vec<TimerOp>,
    pub dispatch_ops: Vec<DispatchOp>,
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
    async fn resolve_execution(&self, execution: &ExecutionRef) -> Result<Option<RunKey>>;

    async fn load_run(&self, run_key: RunKey) -> Result<LoadedRun>;

    /// Read the authoritative history stream after a known event id.
    async fn read_history(&self, run_key: RunKey, after_event_id: i64, limit: usize)
        -> Result<Vec<HistoryEvent>>;

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
    async fn read_transition_audit(&self, run_key: RunKey) -> Result<Vec<TransitionAuditRecord>>;

    async fn commit_transition(&self, run_key: RunKey, transition: Transition) -> Result<CommitResult>;

    async fn list_dispatchable_workflow_tasks(
        &self,
        queue: &QueueKey,
        limit: usize,
    ) -> Result<Vec<DispatchableWorkflowTask>>;

    async fn list_due_timers(&self, now: OffsetDateTime, limit: usize) -> Result<Vec<DueTimer>>;

    // TODO(storage): add sweep methods for activity tasks, archival eligibility,
    // namespace-scoped pagination, and explicit current-execution conflict
    // policies (reuse, reject, allow-after-close, continue-as-new chains).
}

#[derive(Clone, Debug, PartialEq)]
pub struct DispatchableWorkflowTask {
    pub run_key: RunKey,
    pub queue: QueueKey,
    pub logical_seq: tokeira_types::LogicalTaskSeq,
    pub sticky_preferred: Option<WorkerIdentity>,
    pub sticky_expires_at: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DueTimer {
    pub run_key: RunKey,
    pub timer_id: String,
}

/// Read-only interface for projection workers.
#[async_trait]
pub trait ProjectionLog: Send + Sync {
    async fn read_from(&self, cursor: &ProjectionCursor, limit: usize) -> Result<ProjectionBatch>;
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProjectionRecord {
    pub partition_id: u32,
    pub fanout: u16,
    pub run_key: RunKey,
    pub transition_seq: tokeira_types::TransitionSeq,
    pub ops: Vec<tokeira_kernel::ProjectionOp>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProjectionBatch {
    pub records: Vec<ProjectionRecord>,
    pub next_cursor: ProjectionCursor,
}

/// Lease repository for shard or bundle ownership.
#[async_trait]
pub trait LeaseRepository: Send + Sync {
    async fn try_acquire_bundle(&self, bundle: ShardId, owner: String) -> Result<LeaseOutcome>;
    async fn renew_bundle(&self, bundle: ShardId, owner: String, epoch: ShardEpoch) -> Result<LeaseOutcome>;

    // TODO(storage): support explicit relinquish, generation-aware placement,
    // and bulk lease observation for the controller.
}

#[derive(Clone, Debug, PartialEq)]
pub enum LeaseOutcome {
    Acquired { epoch: ShardEpoch },
    Renewed { epoch: ShardEpoch },
    Rejected { current_owner: String, current_epoch: ShardEpoch },
}

/// Database work classes used by the future connection director.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DbClass {
    Control,
    Commit,
    Read,
    Projection,
    Maintenance,
}

/// Small abstraction for future DSQL connection-budget control.
#[async_trait]
pub trait ConnectionDirector: Send + Sync {
    async fn acquire(&self, class: DbClass) -> Result<DbPermit>;
}

#[derive(Debug)]
pub struct DbPermit {
    pub class: DbClass,
}

/// TODO(storage): replace this trivial type with a real session/lease wrapper in
/// the DSQL-backed implementation.


#[async_trait]
impl<T> RunRepository for std::sync::Arc<T>
where
    T: RunRepository + ?Sized,
{
    async fn resolve_execution(&self, execution: &ExecutionRef) -> Result<Option<RunKey>> {
        (**self).resolve_execution(execution).await
    }

    async fn load_run(&self, run_key: RunKey) -> Result<LoadedRun> {
        (**self).load_run(run_key).await
    }

    async fn read_history(&self, run_key: RunKey, after_event_id: i64, limit: usize) -> Result<Vec<HistoryEvent>> {
        (**self).read_history(run_key, after_event_id, limit).await
    }

    async fn lookup_request_dedupe(
        &self,
        execution: &ExecutionRef,
        request_id: &RequestId,
    ) -> Result<Option<RequestRecord>> {
        (**self).lookup_request_dedupe(execution, request_id).await
    }

    async fn read_transition_audit(&self, run_key: RunKey) -> Result<Vec<TransitionAuditRecord>> {
        (**self).read_transition_audit(run_key).await
    }

    async fn commit_transition(&self, run_key: RunKey, transition: Transition) -> Result<CommitResult> {
        (**self).commit_transition(run_key, transition).await
    }

    async fn list_dispatchable_workflow_tasks(
        &self,
        queue: &QueueKey,
        limit: usize,
    ) -> Result<Vec<DispatchableWorkflowTask>> {
        (**self).list_dispatchable_workflow_tasks(queue, limit).await
    }

    async fn list_due_timers(&self, now: OffsetDateTime, limit: usize) -> Result<Vec<DueTimer>> {
        (**self).list_due_timers(now, limit).await
    }
}

#[async_trait]
impl<T> ProjectionLog for std::sync::Arc<T>
where
    T: ProjectionLog + ?Sized,
{
    async fn read_from(&self, cursor: &ProjectionCursor, limit: usize) -> Result<ProjectionBatch> {
        (**self).read_from(cursor, limit).await
    }
}

#[async_trait]
impl<T> LeaseRepository for std::sync::Arc<T>
where
    T: LeaseRepository + ?Sized,
{
    async fn try_acquire_bundle(&self, bundle: ShardId, owner: String) -> Result<LeaseOutcome> {
        (**self).try_acquire_bundle(bundle, owner).await
    }

    async fn renew_bundle(&self, bundle: ShardId, owner: String, epoch: ShardEpoch) -> Result<LeaseOutcome> {
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
