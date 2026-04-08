use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use anyhow::Result;
use async_trait::async_trait;
use time::OffsetDateTime;
use tokeira_kernel::{
    ActivityOp, DispatchOp, LoadedRun, TimerOp, Transition, WorkflowState,
};
use tokeira_types::{
    ExecutionRef, NamespaceId, ProjectionCursor, QueueKey, RequestId, RunId, RunKey,
    ShardEpoch, ShardId,
};
use tokio::sync::Mutex;

use crate::api::{
    BacklogEntry, CommitResult, ConnectionDirector, CurrentExecutionConflictPolicy,
    DbClass, DbPermit, DispatchableActivityTask, DispatchableWorkflowTask, DueTimer,
    LeaseOutcome, LeaseRepository, ProjectionBatch, ProjectionLog, ProjectionRecord,
    RequestRecord, RunRepository, TransitionAuditRecord,
};

/// In-memory store intended for local development and semantic tests.
///
/// Insight: this store is not pretending to be DSQL. It exists so the rest of
/// the workspace can be exercised *before* a real storage backend is finished.
/// It deliberately favors readability over durability or lock efficiency.
///
/// Important difference from the earlier version of this store: we now keep the
/// authoritative history stream, request-dedupe records, and a transition audit
/// log instead of silently dropping them. That makes crash/recovery semantics
/// visible in tests and keeps the dev store aligned with the architecture docs.
#[derive(Default, Clone)]
pub struct InMemoryStore {
    inner: Arc<Mutex<StoreState>>,
}

#[derive(Default)]
struct StoreState {
    current_open: HashMap<(NamespaceId, String), RunKey>,
    execution_index: HashMap<(NamespaceId, String, RunId), RunKey>,
    runs: HashMap<RunKey, WorkflowState>,
    history: HashMap<RunKey, Vec<tokeira_kernel::HistoryEvent>>,
    request_dedupe: HashMap<(NamespaceId, String, String), RequestRecord>,
    transition_audit: HashMap<RunKey, Vec<TransitionAuditRecord>>,
    projection_log: Vec<ProjectionRecord>,
    bundle_leases: HashMap<ShardId, (String, ShardEpoch)>,
    activity_dispatch: HashMap<(RunKey, String), ActivityDispatchEntry>,
    dispatch_backlog: VecDeque<BacklogEntry>,
    backlog_next_seq: u64,
    conflict_injections: HashMap<RunKey, usize>,
    conflict_policy: CurrentExecutionConflictPolicy,
    activity_state_table: HashMap<(RunKey, String), tokeira_kernel::ActivityState>,
    timer_bucket: HashMap<(RunKey, String), tokeira_kernel::TimerState>,
}

#[derive(Clone, Debug, PartialEq)]
struct ActivityDispatchEntry {
    task: DispatchableActivityTask,
    schedule_to_close_timeout: Option<time::Duration>,
    schedule_to_start_timeout: Option<time::Duration>,
    start_to_close_timeout: Option<time::Duration>,
    heartbeat_timeout: Option<time::Duration>,
}

impl InMemoryStore {
    /// Inject `count` synthetic OCC conflicts for
    /// `run_key`. Each subsequent `commit_transition`
    /// call for that key will return
    /// [`CommitResult::Conflict`] until the counter
    /// reaches zero. Useful for testing retry logic.
    pub async fn inject_conflict(&self, run_key: RunKey, count: usize) {
        let mut store = self.inner.lock().await;
        store.conflict_injections.insert(run_key, count);
    }

    /// Override the current-execution conflict policy
    /// used by `commit_transition` when a new workflow
    /// start collides with an existing open execution.
    pub async fn set_conflict_policy(&self, policy: CurrentExecutionConflictPolicy) {
        let mut store = self.inner.lock().await;
        store.conflict_policy = policy;
    }
}

#[async_trait]
impl RunRepository for InMemoryStore {
    async fn resolve_execution(
        &self,
        execution: &ExecutionRef,
    ) -> Result<Option<RunKey>> {
        let store = self.inner.lock().await;
        if let Some(run_id) = execution.run_id {
            return Ok(store
                .execution_index
                .get(&(
                    execution.namespace_id,
                    execution.workflow_id.0.clone(),
                    run_id,
                ))
                .copied());
        }

        Ok(store
            .current_open
            .get(&(execution.namespace_id, execution.workflow_id.0.clone()))
            .copied())
    }

    async fn load_run(&self, run_key: RunKey) -> Result<LoadedRun> {
        let mut store = self.inner.lock().await;
        Ok(match store.runs.get_mut(&run_key) {
            Some(state) => {
                clear_expired_sticky_if_needed(state, OffsetDateTime::now_utc());
                LoadedRun::Existing(state.clone())
            }
            None => LoadedRun::Absent,
        })
    }

    async fn read_history(
        &self,
        run_key: RunKey,
        after_event_id: i64,
        limit: usize,
    ) -> Result<Vec<tokeira_kernel::HistoryEvent>> {
        let store = self.inner.lock().await;
        let Some(history) = store.history.get(&run_key) else {
            return Ok(Vec::new());
        };
        Ok(history
            .iter()
            .filter(|event| event.event_id > after_event_id)
            .take(limit)
            .cloned()
            .collect())
    }

    async fn lookup_request_dedupe(
        &self,
        execution: &ExecutionRef,
        request_id: &RequestId,
    ) -> Result<Option<RequestRecord>> {
        let store = self.inner.lock().await;
        let key = (
            execution.namespace_id,
            execution.workflow_id.0.clone(),
            request_id.0.clone(),
        );
        let found = store.request_dedupe.get(&key).cloned();
        Ok(match (found, execution.run_id) {
            (Some(record), Some(run_id)) if record.run_id == run_id => Some(record),
            (Some(record), None) => Some(record),
            _ => None,
        })
    }

    async fn read_transition_audit(
        &self,
        run_key: RunKey,
    ) -> Result<Vec<TransitionAuditRecord>> {
        let store = self.inner.lock().await;
        Ok(store
            .transition_audit
            .get(&run_key)
            .cloned()
            .unwrap_or_default())
    }

    async fn commit_transition(
        &self,
        run_key: RunKey,
        transition: Transition,
    ) -> Result<CommitResult> {
        let mut store = self.inner.lock().await;
        if let Some(remaining) = store.conflict_injections.get_mut(&run_key) {
            if *remaining > 0 {
                *remaining -= 1;
                return Ok(CommitResult::Conflict {
                    reason: format!("injected conflict for run {:?}", run_key),
                });
            }
        }

        let current_seq = store
            .runs
            .get(&run_key)
            .map(|state| state.transition_seq)
            .unwrap_or(tokeira_types::TransitionSeq::ZERO);
        if current_seq != transition.expected_seq {
            return Ok(CommitResult::Conflict {
                reason: format!(
                    "expected seq {:?}, found {:?}",
                    transition.expected_seq, current_seq
                ),
            });
        }

        let state = transition.next_state.clone();
        let workflow_key = (state.namespace_id, state.workflow_id.0.clone());

        for op in &transition.request_dedupe_ops {
            let dedupe_key = (
                workflow_key.0,
                workflow_key.1.clone(),
                op.request_id.0.clone(),
            );
            if store.request_dedupe.contains_key(&dedupe_key) {
                return Ok(CommitResult::Duplicate);
            }
        }

        if transition.expected_seq == tokeira_types::TransitionSeq::ZERO
            && state.status.is_open()
        {
            match store.conflict_policy {
                CurrentExecutionConflictPolicy::Reject
                | CurrentExecutionConflictPolicy::AllowAfterClose => {
                    if let Some(existing_run) = store.current_open.get(&workflow_key) {
                        if *existing_run != run_key {
                            if let Some(existing_state) = store.runs.get(existing_run) {
                                if existing_state.status.is_open() {
                                    return Ok(CommitResult::Conflict {
                                        reason: format!(
                                            "current execution already exists for {}: {:?}",
                                            state.workflow_id.0, existing_run
                                        ),
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }

        store
            .history
            .entry(run_key)
            .or_default()
            .extend(transition.history_events.iter().cloned());

        store
            .transition_audit
            .entry(run_key)
            .or_default()
            .push(TransitionAuditRecord {
                run_key,
                transition_seq: state.transition_seq,
                history_events: transition.history_events.iter().cloned().collect(),
                activity_ops: transition.activity_ops.iter().cloned().collect(),
                timer_ops: transition.timer_ops.iter().cloned().collect(),
                dispatch_ops: transition.dispatch_ops.iter().cloned().collect(),
                projection_ops: transition.projection_ops.iter().cloned().collect(),
            });

        for op in &transition.request_dedupe_ops {
            let key = (
                workflow_key.0,
                workflow_key.1.clone(),
                op.request_id.0.clone(),
            );
            store.request_dedupe.insert(
                key,
                RequestRecord {
                    namespace_id: state.namespace_id,
                    workflow_id: state.workflow_id.clone(),
                    run_id: state.run_id,
                    run_key,
                    request_id: op.request_id.clone(),
                    first_seen_transition_seq: state.transition_seq,
                },
            );
        }

        for op in &transition.activity_ops {
            match op {
                ActivityOp::Upsert(activity) => {
                    store.activity_state_table.insert(
                        (run_key, activity.activity_id.clone()),
                        activity.clone(),
                    );
                }
                ActivityOp::Delete { activity_id } => {
                    store
                        .activity_state_table
                        .remove(&(run_key, activity_id.clone()));
                    store
                        .activity_dispatch
                        .remove(&(run_key, activity_id.clone()));
                }
            }
        }

        for op in &transition.timer_ops {
            match op {
                TimerOp::Upsert(timer) => {
                    store
                        .timer_bucket
                        .insert((run_key, timer.timer_id.clone()), timer.clone());
                }
                TimerOp::Delete { timer_id } => {
                    store.timer_bucket.remove(&(run_key, timer_id.clone()));
                }
            }
        }

        for op in &transition.dispatch_ops {
            if let DispatchOp::EnqueueActivityTask {
                queue,
                activity_id,
                input,
                schedule_event_id,
                attempt,
                schedule_to_close_timeout,
                schedule_to_start_timeout,
                start_to_close_timeout,
                heartbeat_timeout,
            } = op
            {
                store.activity_dispatch.insert(
                    (run_key, activity_id.clone()),
                    ActivityDispatchEntry {
                        task: DispatchableActivityTask {
                            run_key,
                            queue: queue.clone(),
                            activity_id: activity_id.clone(),
                            input: input.clone(),
                            schedule_event_id: *schedule_event_id,
                            attempt: *attempt,
                        },
                        schedule_to_close_timeout: *schedule_to_close_timeout,
                        schedule_to_start_timeout: *schedule_to_start_timeout,
                        start_to_close_timeout: *start_to_close_timeout,
                        heartbeat_timeout: *heartbeat_timeout,
                    },
                );
            }
        }

        store.runs.insert(run_key, state.clone());
        store.execution_index.insert(
            (
                state.namespace_id,
                state.workflow_id.0.clone(),
                state.run_id,
            ),
            run_key,
        );

        if state.status.is_open() {
            store.current_open.insert(workflow_key.clone(), run_key);
        } else if store.current_open.get(&workflow_key) == Some(&run_key) {
            store.current_open.remove(&workflow_key);
        }

        if !transition.projection_ops.is_empty() {
            store.projection_log.push(ProjectionRecord {
                partition_id: partition_for(run_key),
                fanout: 1,
                run_key,
                transition_seq: state.transition_seq,
                ops: transition.projection_ops.iter().cloned().collect(),
            });
        }

        Ok(CommitResult::Applied { new_state: state })
    }

    async fn list_dispatchable_workflow_tasks(
        &self,
        queue: &QueueKey,
        limit: usize,
    ) -> Result<Vec<DispatchableWorkflowTask>> {
        let mut store = self.inner.lock().await;
        let now = OffsetDateTime::now_utc();
        let mut out = Vec::new();
        for state in store.runs.values_mut() {
            clear_expired_sticky_if_needed(state, now);

            let Some(pending) = &state.pending_workflow_task else {
                continue;
            };
            if pending.started_event_id.is_some() {
                continue;
            }

            let candidate = QueueKey {
                namespace_id: state.namespace_id,
                task_queue: state.task_queue.clone(),
                task_kind: tokeira_types::TaskKind::Workflow,
                deployment: None,
                build_id: None,
            };
            if &candidate != queue {
                continue;
            }

            out.push(DispatchableWorkflowTask {
                run_key: state.run_key,
                queue: candidate,
                logical_seq: pending.logical_seq,
                sticky_preferred: state
                    .sticky
                    .as_ref()
                    .map(|s| s.worker_identity.clone()),
                sticky_expires_at: state.sticky.as_ref().map(|s| s.expires_at),
            });
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    async fn list_dispatchable_activity_tasks(
        &self,
        queue: &QueueKey,
        limit: usize,
    ) -> Result<Vec<DispatchableActivityTask>> {
        let store = self.inner.lock().await;
        Ok(store
            .activity_dispatch
            .values()
            .filter(|entry| &entry.task.queue == queue)
            .take(limit)
            .map(|entry| entry.task.clone())
            .collect())
    }

    async fn persist_to_backlog(&self, entries: Vec<BacklogEntry>) -> Result<()> {
        let mut store = self.inner.lock().await;
        for entry in entries {
            let insertion_seq = store.backlog_next_seq;
            store.backlog_next_seq += 1;
            store.dispatch_backlog.push_back(BacklogEntry {
                insertion_seq,
                ..entry
            });
        }
        Ok(())
    }

    async fn drain_backlog(
        &self,
        queue: &QueueKey,
        limit: usize,
    ) -> Result<Vec<BacklogEntry>> {
        let mut store = self.inner.lock().await;
        let mut drained = Vec::new();
        let mut kept = VecDeque::new();
        while let Some(entry) = store.dispatch_backlog.pop_front() {
            if drained.len() < limit && &entry.queue == queue {
                drained.push(entry);
            } else {
                kept.push_back(entry);
            }
        }
        store.dispatch_backlog = kept;
        Ok(drained)
    }

    async fn list_due_timers(
        &self,
        now: OffsetDateTime,
        limit: usize,
    ) -> Result<Vec<DueTimer>> {
        let store = self.inner.lock().await;
        let mut due = Vec::new();
        for ((run_key, _), timer) in &store.timer_bucket {
            if timer.fire_at <= now {
                due.push(DueTimer {
                    run_key: *run_key,
                    timer_id: timer.timer_id.clone(),
                });
                if due.len() >= limit {
                    return Ok(due);
                }
            }
        }
        Ok(due)
    }
}

#[async_trait]
impl ProjectionLog for InMemoryStore {
    async fn read_from(
        &self,
        cursor: &ProjectionCursor,
        limit: usize,
    ) -> Result<ProjectionBatch> {
        let store = self.inner.lock().await;
        let mut started = cursor.last_transition_seq.is_none();
        let mut out = Vec::new();
        for record in store.projection_log.iter() {
            if record.partition_id != cursor.partition_id
                || record.fanout != cursor.fanout
            {
                continue;
            }
            if !started {
                if Some(record.run_key) == cursor.last_run_key
                    && Some(record.transition_seq) == cursor.last_transition_seq
                {
                    started = true;
                }
                continue;
            }
            out.push(record.clone());
            if out.len() >= limit {
                break;
            }
        }
        let next_cursor = match out.last() {
            Some(last) => ProjectionCursor {
                partition_id: cursor.partition_id,
                fanout: cursor.fanout,
                last_run_key: Some(last.run_key),
                last_transition_seq: Some(last.transition_seq),
            },
            None => cursor.clone(),
        };
        Ok(ProjectionBatch {
            records: out,
            next_cursor,
        })
    }
}

#[async_trait]
impl LeaseRepository for InMemoryStore {
    async fn try_acquire_bundle(
        &self,
        bundle: ShardId,
        owner: String,
    ) -> Result<LeaseOutcome> {
        let mut store = self.inner.lock().await;
        match store.bundle_leases.get(&bundle) {
            Some((current_owner, current_epoch)) => Ok(LeaseOutcome::Rejected {
                current_owner: current_owner.clone(),
                current_epoch: *current_epoch,
            }),
            None => {
                let epoch = ShardEpoch(1);
                store.bundle_leases.insert(bundle, (owner, epoch));
                Ok(LeaseOutcome::Acquired { epoch })
            }
        }
    }

    async fn renew_bundle(
        &self,
        bundle: ShardId,
        owner: String,
        epoch: ShardEpoch,
    ) -> Result<LeaseOutcome> {
        let mut store = self.inner.lock().await;
        match store.bundle_leases.get_mut(&bundle) {
            Some((current_owner, current_epoch))
                if *current_owner == owner && *current_epoch == epoch =>
            {
                Ok(LeaseOutcome::Renewed { epoch })
            }
            Some((current_owner, current_epoch)) => Ok(LeaseOutcome::Rejected {
                current_owner: current_owner.clone(),
                current_epoch: *current_epoch,
            }),
            None => {
                store.bundle_leases.insert(bundle, (owner, epoch));
                Ok(LeaseOutcome::Acquired { epoch })
            }
        }
    }
}

#[async_trait]
impl ConnectionDirector for InMemoryStore {
    async fn acquire(&self, class: DbClass) -> Result<DbPermit> {
        Ok(DbPermit { class })
    }
}

fn clear_expired_sticky_if_needed(state: &mut WorkflowState, now: OffsetDateTime) {
    if state
        .sticky
        .as_ref()
        .is_some_and(|sticky| sticky.expires_at <= now)
    {
        state.sticky = None;
    }
}

fn partition_for(run_key: RunKey) -> u32 {
    // TODO(perf): make projection partitioning configurable by the real storage
    // implementation. For the dev store we keep it simple and deterministic.
    let raw = run_key.0.as_u128();
    (raw as u32) % 16
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use time::Duration;
    use tokeira_kernel::{PendingWorkflowTask, RequestDedupeOp};
    use tokeira_types::{
        ExecutionStatus, LogicalTaskSeq, Memo, NamespaceId, QueueKey, RequestId, RunId,
        RunKey, SearchAttributes, TaskKind, TaskQueueName, TransitionSeq, WorkflowId,
        WorkflowType,
    };

    use super::*;
    use crate::api::BacklogTaskKind;

    fn fixed_now() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()
    }

    fn sample_queue(task_kind: TaskKind) -> QueueKey {
        QueueKey {
            namespace_id: NamespaceId::new(),
            task_queue: TaskQueueName("queue".into()),
            task_kind,
            deployment: None,
            build_id: None,
        }
    }

    fn sample_state(run_key: RunKey) -> WorkflowState {
        WorkflowState {
            run_key,
            namespace_id: NamespaceId::new(),
            workflow_id: WorkflowId("workflow".into()),
            run_id: RunId::new(),
            workflow_type: WorkflowType("wf".into()),
            task_queue: TaskQueueName("queue".into()),
            status: ExecutionStatus::Running,
            transition_seq: TransitionSeq(1),
            last_event_id: 0,
            next_workflow_task_seq: LogicalTaskSeq(1),
            pending_workflow_task: Some(PendingWorkflowTask {
                logical_seq: LogicalTaskSeq(1),
                scheduled_event_id: 1,
                started_event_id: None,
                attempt: 0,
            }),
            sticky: None,
            pause_info: None,
            wft_stamp: 0,
            memo: Memo::default(),
            search_attributes: SearchAttributes::default(),
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: Duration::seconds(10),
            retry_policy: None,
            attempt: 1,
            parent_run_key: None,
            parent_workflow_id: None,
            activities: Default::default(),
            timers: Default::default(),
            children: Default::default(),
            pending_external_signals: Default::default(),
            pending_external_cancels: Default::default(),
            pending_updates: Default::default(),
            pending_nexus_operations: Default::default(),
            versioning_override: None,
            completion_callbacks: Vec::new(),
            started_at: fixed_now(),
            closed_at: None,
            close_result: None,
            close_failure: None,
        }
    }

    fn start_transition(run_key: RunKey) -> Transition {
        Transition {
            expected_seq: TransitionSeq::ZERO,
            next_state: sample_state(run_key),
            history_events: Default::default(),
            request_dedupe_ops: Default::default(),
            activity_ops: Default::default(),
            timer_ops: Default::default(),
            dispatch_ops: Default::default(),
            projection_ops: Default::default(),
        }
    }

    fn activity_state(activity_id: &str) -> tokeira_kernel::ActivityState {
        tokeira_kernel::ActivityState {
            activity_id: activity_id.into(),
            schedule_event_id: 7,
            task_queue: TaskQueueName("activity-q".into()),
            input: tokeira_types::Payloads::default(),
            attempt: 2,
            retry_policy: None,
            schedule_to_close_timeout: Some(Duration::seconds(30)),
            schedule_to_start_timeout: Some(Duration::seconds(10)),
            start_to_close_timeout: Some(Duration::seconds(20)),
            heartbeat_timeout: Some(Duration::seconds(5)),
            pause_info: None,
            stamp: 0,
        }
    }

    fn timer_state(
        timer_id: &str,
        fire_at: OffsetDateTime,
    ) -> tokeira_kernel::TimerState {
        tokeira_kernel::TimerState {
            timer_id: timer_id.into(),
            started_event_id: 11,
            fire_at,
        }
    }

    fn arb_activity_id() -> impl Strategy<Value = String> {
        "[a-z0-9]{1,8}".prop_map(|s| s)
    }

    proptest! {
        #[test]
        fn property_activity_dispatch_round_trip_fidelity(activity_id in arb_activity_id()) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let store = InMemoryStore::default();
                let run_key = RunKey::new();
                let mut transition = start_transition(run_key);
                let queue = QueueKey {
                    namespace_id: transition.next_state.namespace_id,
                    task_queue: TaskQueueName("queue".into()),
                    task_kind: TaskKind::Activity,
                    deployment: None,
                    build_id: None,
                };
                transition.dispatch_ops.push(DispatchOp::EnqueueActivityTask {
                    queue: queue.clone(),
                    activity_id: activity_id.clone(),
                    input: tokeira_types::Payloads::default(),
                    schedule_event_id: 42,
                    attempt: 3,
                    schedule_to_close_timeout: Some(Duration::seconds(30)),
                    schedule_to_start_timeout: Some(Duration::seconds(10)),
                    start_to_close_timeout: Some(Duration::seconds(20)),
                    heartbeat_timeout: Some(Duration::seconds(5)),
                });

                let result = store.commit_transition(run_key, transition).await.unwrap();
                assert!(matches!(result, CommitResult::Applied { .. }));
                let tasks = store.list_dispatchable_activity_tasks(&queue, 10).await.unwrap();
                assert_eq!(tasks.len(), 1);
                assert_eq!(tasks[0].run_key, run_key);
                assert_eq!(tasks[0].activity_id, activity_id);
                assert_eq!(tasks[0].schedule_event_id, 42);
                assert_eq!(tasks[0].attempt, 3);
            });
        }

        #[test]
        fn property_activity_dispatch_cleanup_on_delete(activity_id in arb_activity_id()) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let store = InMemoryStore::default();
                let run_key = RunKey::new();
                let namespace_id = NamespaceId::new();
                let workflow_id = WorkflowId("workflow".into());
                let queue = QueueKey {
                    namespace_id,
                    task_queue: TaskQueueName("queue".into()),
                    task_kind: TaskKind::Activity,
                    deployment: None,
                    build_id: None,
                };

                let mut transition = start_transition(run_key);
                transition.next_state.namespace_id = namespace_id;
                transition.next_state.workflow_id = workflow_id.clone();
                transition.dispatch_ops.push(DispatchOp::EnqueueActivityTask {
                    queue: queue.clone(),
                    activity_id: activity_id.clone(),
                    input: tokeira_types::Payloads::default(),
                    schedule_event_id: 42,
                    attempt: 3,
                    schedule_to_close_timeout: None,
                    schedule_to_start_timeout: None,
                    start_to_close_timeout: None,
                    heartbeat_timeout: None,
                });
                transition.activity_ops.push(ActivityOp::Upsert(activity_state(&activity_id)));
                let _ = store.commit_transition(run_key, transition).await.unwrap();

                let mut delete_transition = Transition {
                    expected_seq: TransitionSeq(1),
                    next_state: sample_state(run_key),
                    history_events: Default::default(),
                    request_dedupe_ops: Default::default(),
                    activity_ops: Default::default(),
                    timer_ops: Default::default(),
                    dispatch_ops: Default::default(),
                    projection_ops: Default::default(),
                };
                delete_transition.next_state.transition_seq = TransitionSeq(2);
                delete_transition
                    .activity_ops
                    .push(ActivityOp::Delete { activity_id: activity_id.clone() });
                let _ = store.commit_transition(run_key, delete_transition).await.unwrap();

                let tasks = store.list_dispatchable_activity_tasks(&queue, 10).await.unwrap();
                assert!(tasks.is_empty());
            });
        }

        #[test]
        fn property_failed_commits_leave_new_structures_unchanged(activity_id in arb_activity_id()) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let store = InMemoryStore::default();
                let run_key = RunKey::new();
                let queue = QueueKey {
                    namespace_id: NamespaceId::new(),
                    task_queue: TaskQueueName("queue".into()),
                    task_kind: TaskKind::Activity,
                    deployment: None,
                    build_id: None,
                };

                let mut first = start_transition(run_key);
                first.next_state.namespace_id = queue.namespace_id;
                first.dispatch_ops.push(DispatchOp::EnqueueActivityTask {
                    queue: queue.clone(),
                    activity_id: activity_id.clone(),
                    input: tokeira_types::Payloads::default(),
                    schedule_event_id: 42,
                    attempt: 3,
                    schedule_to_close_timeout: None,
                    schedule_to_start_timeout: None,
                    start_to_close_timeout: None,
                    heartbeat_timeout: None,
                });
                first.activity_ops.push(ActivityOp::Upsert(activity_state(&activity_id)));
                let _ = store.commit_transition(run_key, first).await.unwrap();
                let before = store.list_dispatchable_activity_tasks(&queue, 10).await.unwrap();

                store.inject_conflict(run_key, 1).await;
                let mut conflict = Transition {
                    expected_seq: TransitionSeq(1),
                    next_state: sample_state(run_key),
                    history_events: Default::default(),
                    request_dedupe_ops: Default::default(),
                    activity_ops: Default::default(),
                    timer_ops: Default::default(),
                    dispatch_ops: Default::default(),
                    projection_ops: Default::default(),
                };
                conflict.next_state.namespace_id = queue.namespace_id;
                conflict.next_state.transition_seq = TransitionSeq(2);
                conflict.dispatch_ops.push(DispatchOp::EnqueueActivityTask {
                    queue: queue.clone(),
                    activity_id: "other".into(),
                    input: tokeira_types::Payloads::default(),
                    schedule_event_id: 99,
                    attempt: 1,
                    schedule_to_close_timeout: None,
                    schedule_to_start_timeout: None,
                    start_to_close_timeout: None,
                    heartbeat_timeout: None,
                });
                let result = store.commit_transition(run_key, conflict).await.unwrap();
                assert!(matches!(result, CommitResult::Conflict { .. }));

                let after = store.list_dispatchable_activity_tasks(&queue, 10).await.unwrap();
                assert_eq!(before, after);
            });
        }

        #[test]
        fn property_activity_sweep_returns_matching_tasks_up_to_limit(limit in 1usize..4usize) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let store = InMemoryStore::default();
                let activity_queue = sample_queue(TaskKind::Activity);
                let other_queue = QueueKey { task_queue: TaskQueueName("other".into()), ..activity_queue.clone() };

                for idx in 0..5 {
                    let run_key = RunKey::new();
                    let mut transition = start_transition(run_key);
                    transition.next_state.namespace_id = activity_queue.namespace_id;
                    transition.dispatch_ops.push(DispatchOp::EnqueueActivityTask {
                        queue: if idx % 2 == 0 { activity_queue.clone() } else { other_queue.clone() },
                        activity_id: format!("a{idx}"),
                        input: tokeira_types::Payloads::default(),
                        schedule_event_id: idx as i64,
                        attempt: 1,
                        schedule_to_close_timeout: None,
                        schedule_to_start_timeout: None,
                        start_to_close_timeout: None,
                        heartbeat_timeout: None,
                    });
                    let _ = store.commit_transition(run_key, transition).await.unwrap();
                }

                let tasks = store.list_dispatchable_activity_tasks(&activity_queue, limit).await.unwrap();
                assert!(tasks.len() <= limit);
                assert!(tasks.iter().all(|task| task.queue == activity_queue));
            });
        }

        #[test]
        fn property_backlog_insertion_and_drain_order(limit in 1usize..4usize) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let store = InMemoryStore::default();
                let queue = sample_queue(TaskKind::Workflow);
                let entries: Vec<_> = (0..5)
                    .map(|idx| BacklogEntry {
                        run_key: RunKey::new(),
                        queue: queue.clone(),
                        kind: if idx % 2 == 0 {
                            BacklogTaskKind::Workflow
                        } else {
                            BacklogTaskKind::Activity { activity_id: format!("a{idx}") }
                        },
                        insertion_seq: 999,
                    })
                    .collect();
                store.persist_to_backlog(entries).await.unwrap();
                let drained = store.drain_backlog(&queue, limit).await.unwrap();
                assert!(drained.len() <= limit);
                for pair in drained.windows(2) {
                    assert!(pair[0].insertion_seq < pair[1].insertion_seq);
                }
            });
        }

        #[test]
        fn property_conflict_injection_lifecycle(count in 1usize..4usize) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let store = InMemoryStore::default();
                let run_key = RunKey::new();
                store.inject_conflict(run_key, count).await;
                for _ in 0..count {
                    let result = store.commit_transition(run_key, start_transition(run_key)).await.unwrap();
                    assert!(matches!(result, CommitResult::Conflict { .. }));
                }
                let result = store.commit_transition(run_key, start_transition(run_key)).await.unwrap();
                assert!(matches!(result, CommitResult::Applied { .. }));
            });
        }

        #[test]
        fn property_reject_and_allow_after_close_block_when_open_exists(use_allow_after_close in any::<bool>()) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let store = InMemoryStore::default();
                if use_allow_after_close {
                    store.set_conflict_policy(CurrentExecutionConflictPolicy::AllowAfterClose).await;
                }

                let namespace_id = NamespaceId::new();
                let workflow_id = WorkflowId("same".into());
                let run_key_1 = RunKey::new();
                let mut t1 = start_transition(run_key_1);
                t1.next_state.namespace_id = namespace_id;
                t1.next_state.workflow_id = workflow_id.clone();
                let _ = store.commit_transition(run_key_1, t1).await.unwrap();

                let run_key_2 = RunKey::new();
                let mut t2 = start_transition(run_key_2);
                t2.next_state.namespace_id = namespace_id;
                t2.next_state.workflow_id = workflow_id;
                let result = store.commit_transition(run_key_2, t2).await.unwrap();
                assert!(matches!(result, CommitResult::Conflict { .. }));
            });
        }

        #[test]
        fn property_independent_activity_and_timer_state_upsert_delete(activity_id in arb_activity_id()) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let store = InMemoryStore::default();
                let run_key = RunKey::new();
                let mut upsert = start_transition(run_key);
                upsert.activity_ops.push(ActivityOp::Upsert(activity_state(&activity_id)));
                upsert.timer_ops.push(TimerOp::Upsert(timer_state("timer-1", fixed_now())));
                let _ = store.commit_transition(run_key, upsert).await.unwrap();
                assert_eq!(store.list_due_timers(fixed_now(), 10).await.unwrap().len(), 1);

                let mut delete = Transition {
                    expected_seq: TransitionSeq(1),
                    next_state: sample_state(run_key),
                    history_events: Default::default(),
                    request_dedupe_ops: Default::default(),
                    activity_ops: Default::default(),
                    timer_ops: Default::default(),
                    dispatch_ops: Default::default(),
                    projection_ops: Default::default(),
                };
                delete.next_state.transition_seq = TransitionSeq(2);
                delete.activity_ops.push(ActivityOp::Delete { activity_id });
                delete.timer_ops.push(TimerOp::Delete { timer_id: "timer-1".into() });
                let _ = store.commit_transition(run_key, delete).await.unwrap();
                assert!(store.list_due_timers(fixed_now(), 10).await.unwrap().is_empty());
            });
        }

        // Feature: storage-memory-fidelity, Property 9: AllowAfterClose permits creation after close
        #[test]
        fn property_allow_after_close_permits_creation_after_close(activity_id in arb_activity_id()) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let store = InMemoryStore::default();
                store.set_conflict_policy(CurrentExecutionConflictPolicy::AllowAfterClose).await;

                let namespace_id = NamespaceId::new();
                let workflow_id = WorkflowId(format!("wf-{activity_id}"));

                let run_key_1 = RunKey::new();
                let mut open = start_transition(run_key_1);
                open.next_state.namespace_id = namespace_id;
                open.next_state.workflow_id = workflow_id.clone();
                let _ = store.commit_transition(run_key_1, open).await.unwrap();

                let mut close = Transition {
                    expected_seq: TransitionSeq(1),
                    next_state: sample_state(run_key_1),
                    history_events: Default::default(),
                    request_dedupe_ops: Default::default(),
                    activity_ops: Default::default(),
                    timer_ops: Default::default(),
                    dispatch_ops: Default::default(),
                    projection_ops: Default::default(),
                };
                close.next_state.namespace_id = namespace_id;
                close.next_state.workflow_id = workflow_id.clone();
                close.next_state.status = ExecutionStatus::Completed;
                close.next_state.closed_at = Some(fixed_now());
                close.next_state.pending_workflow_task = None;
                close.next_state.transition_seq = TransitionSeq(2);
                let _ = store.commit_transition(run_key_1, close).await.unwrap();

                let run_key_2 = RunKey::new();
                let mut reopen = start_transition(run_key_2);
                reopen.next_state.namespace_id = namespace_id;
                reopen.next_state.workflow_id = workflow_id;
                let result = store.commit_transition(run_key_2, reopen).await.unwrap();
                assert!(matches!(result, CommitResult::Applied { .. }));
            });
        }

        // Feature: storage-memory-fidelity, Property 11: Independent structures mirror WorkflowState maps
        #[test]
        fn property_independent_structures_mirror_workflow_state(activity_id in arb_activity_id()) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let store = InMemoryStore::default();
                let run_key = RunKey::new();
                let timer_id = format!("t-{activity_id}");

                let mut t1 = start_transition(run_key);
                let act = activity_state(&activity_id);
                let tmr = timer_state(&timer_id, fixed_now());
                t1.activity_ops.push(ActivityOp::Upsert(act.clone()));
                t1.timer_ops.push(TimerOp::Upsert(tmr.clone()));
                t1.next_state.activities.insert(activity_id.clone(), act);
                t1.next_state.timers.insert(timer_id.clone(), tmr);
                let _ = store.commit_transition(run_key, t1).await.unwrap();

                let inner = store.inner.lock().await;
                for (rk, state) in &inner.runs {
                    for (aid, act_state) in &state.activities {
                        assert_eq!(inner.activity_state_table.get(&(*rk, aid.clone())), Some(act_state));
                    }
                    for (tid, tmr_state) in &state.timers {
                        assert_eq!(inner.timer_bucket.get(&(*rk, tid.clone())), Some(tmr_state));
                    }
                }
                assert_eq!(
                    inner.activity_state_table.len(),
                    inner.runs.values().map(|s| s.activities.len()).sum::<usize>()
                );
                assert_eq!(
                    inner.timer_bucket.len(),
                    inner.runs.values().map(|s| s.timers.len()).sum::<usize>()
                );
            });
        }

        #[test]
        fn property_backlog_size_invariant(first in 1usize..4usize, second in 0usize..3usize, drain in 0usize..5usize) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let store = InMemoryStore::default();
                let queue = sample_queue(TaskKind::Workflow);

                let mk_entries = |count: usize| {
                    (0..count)
                        .map(|_| BacklogEntry {
                            run_key: RunKey::new(),
                            queue: queue.clone(),
                            kind: BacklogTaskKind::Workflow,
                            insertion_seq: 123,
                        })
                        .collect::<Vec<_>>()
                };

                store.persist_to_backlog(mk_entries(first)).await.unwrap();
                store.persist_to_backlog(mk_entries(second)).await.unwrap();
                let drained = store.drain_backlog(&queue, drain).await.unwrap();
                let remaining = store.drain_backlog(&queue, usize::MAX).await.unwrap();
                assert_eq!(remaining.len(), first + second - drained.len().min(first + second));
            });
        }
    }

    #[tokio::test]
    async fn default_policy_is_reject() {
        let store = InMemoryStore::default();
        let namespace_id = NamespaceId::new();
        let workflow_id = WorkflowId("same".into());

        let run_key_1 = RunKey::new();
        let mut t1 = start_transition(run_key_1);
        t1.next_state.namespace_id = namespace_id;
        t1.next_state.workflow_id = workflow_id.clone();
        let _ = store.commit_transition(run_key_1, t1).await.unwrap();

        let run_key_2 = RunKey::new();
        let mut t2 = start_transition(run_key_2);
        t2.next_state.namespace_id = namespace_id;
        t2.next_state.workflow_id = workflow_id;
        let result = store.commit_transition(run_key_2, t2).await.unwrap();
        assert!(matches!(result, CommitResult::Conflict { .. }));
    }

    #[tokio::test]
    async fn allow_after_close_permits_new_execution_after_close() {
        let store = InMemoryStore::default();
        store
            .set_conflict_policy(CurrentExecutionConflictPolicy::AllowAfterClose)
            .await;

        let namespace_id = NamespaceId::new();
        let workflow_id = WorkflowId("same".into());

        let run_key_1 = RunKey::new();
        let mut open = start_transition(run_key_1);
        open.next_state.namespace_id = namespace_id;
        open.next_state.workflow_id = workflow_id.clone();
        let _ = store.commit_transition(run_key_1, open).await.unwrap();

        let mut close = Transition {
            expected_seq: TransitionSeq(1),
            next_state: sample_state(run_key_1),
            history_events: Default::default(),
            request_dedupe_ops: Default::default(),
            activity_ops: Default::default(),
            timer_ops: Default::default(),
            dispatch_ops: Default::default(),
            projection_ops: Default::default(),
        };
        close.next_state.namespace_id = namespace_id;
        close.next_state.workflow_id = workflow_id.clone();
        close.next_state.status = ExecutionStatus::Completed;
        close.next_state.closed_at = Some(fixed_now());
        close.next_state.pending_workflow_task = None;
        close.next_state.transition_seq = TransitionSeq(2);
        let _ = store.commit_transition(run_key_1, close).await.unwrap();

        let run_key_2 = RunKey::new();
        let mut reopen = start_transition(run_key_2);
        reopen.next_state.namespace_id = namespace_id;
        reopen.next_state.workflow_id = workflow_id;
        let result = store.commit_transition(run_key_2, reopen).await.unwrap();
        assert!(matches!(result, CommitResult::Applied { .. }));
    }

    #[tokio::test]
    async fn list_due_timers_uses_timer_bucket() {
        let store = InMemoryStore::default();
        let run_key = RunKey::new();
        let mut transition = start_transition(run_key);
        transition
            .timer_ops
            .push(TimerOp::Upsert(timer_state("timer-1", fixed_now())));
        let _ = store.commit_transition(run_key, transition).await.unwrap();
        let due = store.list_due_timers(fixed_now(), 10).await.unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].timer_id, "timer-1");
    }

    #[tokio::test]
    async fn empty_sweep_and_drain_return_empty() {
        let store = InMemoryStore::default();
        assert!(
            store
                .list_dispatchable_activity_tasks(&sample_queue(TaskKind::Activity), 10)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .drain_backlog(&sample_queue(TaskKind::Workflow), 10)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn inject_conflict_replaces_count() {
        let store = InMemoryStore::default();
        let run_key = RunKey::new();
        store.inject_conflict(run_key, 3).await;
        store.inject_conflict(run_key, 1).await;
        let first = store
            .commit_transition(run_key, start_transition(run_key))
            .await
            .unwrap();
        let second = store
            .commit_transition(run_key, start_transition(run_key))
            .await
            .unwrap();
        assert!(matches!(first, CommitResult::Conflict { .. }));
        assert!(matches!(second, CommitResult::Applied { .. }));
    }

    #[tokio::test]
    async fn backlog_insertion_order_matches_input_order() {
        let store = InMemoryStore::default();
        let queue = sample_queue(TaskKind::Workflow);
        store
            .persist_to_backlog(vec![
                BacklogEntry {
                    run_key: RunKey::new(),
                    queue: queue.clone(),
                    kind: BacklogTaskKind::Workflow,
                    insertion_seq: 999,
                },
                BacklogEntry {
                    run_key: RunKey::new(),
                    queue: queue.clone(),
                    kind: BacklogTaskKind::Activity {
                        activity_id: "a1".into(),
                    },
                    insertion_seq: 999,
                },
            ])
            .await
            .unwrap();
        let drained = store.drain_backlog(&queue, 10).await.unwrap();
        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0].insertion_seq, 0);
        assert_eq!(drained[1].insertion_seq, 1);
    }

    #[tokio::test]
    async fn commit_transition_does_not_write_backlog() {
        let store = InMemoryStore::default();
        let run_key = RunKey::new();
        let mut transition = start_transition(run_key);
        transition
            .dispatch_ops
            .push(DispatchOp::EnqueueWorkflowTask {
                queue: sample_queue(TaskKind::Workflow),
                logical_seq: LogicalTaskSeq(1),
                sticky_preferred: None,
            });
        transition
            .dispatch_ops
            .push(DispatchOp::EnqueueActivityTask {
                queue: sample_queue(TaskKind::Activity),
                activity_id: "a1".into(),
                input: tokeira_types::Payloads::default(),
                schedule_event_id: 3,
                attempt: 1,
                schedule_to_close_timeout: None,
                schedule_to_start_timeout: None,
                start_to_close_timeout: None,
                heartbeat_timeout: None,
            });
        let _ = store.commit_transition(run_key, transition).await.unwrap();
        assert!(
            store
                .drain_backlog(&sample_queue(TaskKind::Workflow), 10)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn duplicate_commit_leaves_new_structures_unchanged() {
        let store = InMemoryStore::default();
        let run_key = RunKey::new();
        let mut first = start_transition(run_key);
        let namespace_id = NamespaceId::new();
        let workflow_id = WorkflowId("workflow".into());
        first.next_state.namespace_id = namespace_id;
        first.next_state.workflow_id = workflow_id.clone();
        first.request_dedupe_ops.push(RequestDedupeOp {
            request_id: RequestId("req-1".into()),
        });
        first.dispatch_ops.push(DispatchOp::EnqueueActivityTask {
            queue: QueueKey {
                namespace_id,
                task_queue: TaskQueueName("queue".into()),
                task_kind: TaskKind::Activity,
                deployment: None,
                build_id: None,
            },
            activity_id: "a1".into(),
            input: tokeira_types::Payloads::default(),
            schedule_event_id: 3,
            attempt: 1,
            schedule_to_close_timeout: None,
            schedule_to_start_timeout: None,
            start_to_close_timeout: None,
            heartbeat_timeout: None,
        });
        first
            .activity_ops
            .push(ActivityOp::Upsert(activity_state("a1")));
        let _ = store.commit_transition(run_key, first).await.unwrap();

        let tasks_before = store
            .list_dispatchable_activity_tasks(&sample_queue(TaskKind::Activity), 10)
            .await
            .unwrap();

        let mut duplicate = Transition {
            expected_seq: TransitionSeq(1),
            next_state: sample_state(run_key),
            history_events: Default::default(),
            request_dedupe_ops: Default::default(),
            activity_ops: Default::default(),
            timer_ops: Default::default(),
            dispatch_ops: Default::default(),
            projection_ops: Default::default(),
        };
        duplicate.next_state.namespace_id = namespace_id;
        duplicate.next_state.workflow_id = workflow_id;
        duplicate.next_state.transition_seq = TransitionSeq(2);
        duplicate.request_dedupe_ops.push(RequestDedupeOp {
            request_id: RequestId("req-1".into()),
        });
        let result = store.commit_transition(run_key, duplicate).await.unwrap();
        assert!(matches!(result, CommitResult::Duplicate));

        let tasks_after = store
            .list_dispatchable_activity_tasks(&sample_queue(TaskKind::Activity), 10)
            .await
            .unwrap();
        assert_eq!(tasks_before, tasks_after);
    }
}
