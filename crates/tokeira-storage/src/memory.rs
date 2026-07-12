//! In-memory storage backend for development and tests.
//!
//! This store exercises the full `RunRepository` contract in a single process
//! without external dependencies. It is a reference model for behaviour, not a
//! template for a production engine. Key indexing structures: `execution_index`
//! maps `(namespace, workflow_id, run_id)` → `RunKey`, `current_open` tracks
//! the single open run per workflow, and `current_execution` retains the exact
//! current pointer after close without falling back to an older surviving run.

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Instant,
};

use anyhow::Result;
use async_trait::async_trait;
use time::OffsetDateTime;
use tokeira_kernel::{
    ActivityOp, BasicKernel, CallbackState, DispatchOp, LoadedRun, ReplayContext, TimerOp,
    Transition, WorkflowState,
};
use tokeira_types::{
    ExecutionRef, ExecutionStatus, GenerationCounter, NamespaceId, ProjectionCursor, QueueKey,
    RequestId, RunId, RunKey, ShardEpoch, ShardId, TaskKind, WorkflowId,
};
use tokio::sync::Mutex;

use crate::{
    api::{
        ActivitySweepEntry, BacklogEntry, BudgetAllocationResult, BundleLease, CommitResult,
        CompletionCallbackSweepEntry, ConflictToken, ConnectionDirector, ControlRepository,
        CurrentExecutionConflictPolicy, DbClass, DbPermit, DeleteRunRequest, DeleteRunResult,
        DeploymentCasResult, DeploymentKey, DeploymentName, DispatchableActivityTask,
        DispatchableWorkflowTask, DueTimer, GenerationAdvanceResult, LeaseOutcome, LeaseRepository,
        NexusSweepEntry, ProjectionBatch, ProjectionLog, ProjectionRecord, RequestRecord,
        RunRepository, StoredWorkerDeployment, TransitionAuditRecord, WftTimeoutSweepEntry,
        WorkerDeploymentRepository, WorkerDeploymentVersionKey, WorkflowTimeoutSweepEntry,
        deleted_workflow_projection_context, workflow_is_open_and_pinned_to_version,
        workflow_projection_context,
    },
    metrics as storage_metrics,
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
    /// Current open run per workflow.
    ///
    /// Closed workflows are removed from this map, but remain present in
    /// `latest_run` and `execution_index` equivalents below.
    current_open: HashMap<(NamespaceId, String), RunKey>,
    /// Current run per workflow, retained after close until a successor starts
    /// or the pointed-to run is explicitly deleted.
    ///
    /// DSQL persists the same distinction in `current_execution.is_open`.
    /// Keeping a pointer here prevents deleting the newest run from exposing an
    /// arbitrary older survivor through `find_latest_run`.
    current_execution: HashMap<(NamespaceId, String), RunKey>,
    /// Durable run lookup by full execution identity.
    ///
    /// This mirrors the DSQL explicit-run path: older closed runs remain
    /// addressable even after a newer run becomes current.
    execution_index: HashMap<(NamespaceId, String, RunId), RunKey>,
    /// Materialized hot state by run key.
    runs: HashMap<RunKey, WorkflowState>,
    /// Worker Deployment registry records by namespace/name.
    worker_deployments: HashMap<DeploymentKey, StoredWorkerDeployment>,
    /// Per-deployment conflict-token high-water-mark.
    ///
    /// The conflict token must increase monotonically across the entire
    /// lifetime of a deployment *name*, including delete-then-recreate. In
    /// v1.31.0 the token is `workflow.Now(ctx).MarshalBinary()` of the
    /// deployment entity workflow (`service/worker/workerdeployment/workflow.go:248,502
    /// @ v1.31.0`); a recreated deployment runs a fresh entity workflow and so
    /// observes a strictly later time. We model that with a monotonic
    /// generation that survives record deletion — keyed by `DeploymentKey` and
    /// never reset — so a recreated deployment never reuses a prior token.
    deployment_token_hwm: HashMap<DeploymentKey, u64>,
    /// Authoritative event stream by run key.
    history: HashMap<RunKey, Vec<tokeira_kernel::HistoryEvent>>,
    /// Workflow-scoped request dedupe records.
    request_dedupe: HashMap<(NamespaceId, String, String), RequestRecord>,
    /// Test/admin transition audit records.
    transition_audit: HashMap<RunKey, Vec<TransitionAuditRecord>>,
    /// Projection records awaiting projection workers.
    projection_log: Vec<ProjectionRecord>,
    /// Shard lease state keyed by shard id.
    bundle_leases: HashMap<ShardId, (Option<String>, ShardEpoch, Option<String>)>,
    /// Controller routing generation singleton.
    routing_generation: GenerationCounter,
    /// CAS version for controller connection-budget allocation.
    budget_version: u64,
    /// Durable dispatch source for activity work.
    ///
    /// Do not infer dispatchability from `activity_state_table`: started,
    /// paused, or workflow-paused activities can still have durable activity
    /// state while being intentionally absent from this map.
    activity_dispatch: HashMap<(RunKey, String), ActivityDispatchEntry>,
    /// FIFO backlog for tasks that could not be immediately handed to a worker.
    dispatch_backlog: VecDeque<BacklogEntry>,
    /// Monotonic insertion sequence assigned by `persist_to_backlog`.
    backlog_next_seq: u64,
    /// Test hook for injecting OCC conflicts.
    conflict_injections: HashMap<RunKey, usize>,
    /// Current workflow-id conflict behavior for start transitions.
    conflict_policy: CurrentExecutionConflictPolicy,
    /// Activity timeout/sweep materialization.
    activity_state_table: HashMap<(RunKey, String), tokeira_kernel::ActivityState>,
    /// Timer sweep materialization.
    timer_bucket: HashMap<(RunKey, String), tokeira_kernel::TimerState>,
    /// Deterministic run-to-shard mapping.
    run_shard_map: HashMap<RunKey, ShardId>,
    /// Total shard count for deterministic assignment.
    shard_count: u32,
}

#[derive(Clone, Debug, PartialEq)]
struct ActivityDispatchEntry {
    task: DispatchableActivityTask,
    schedule_to_close_timeout: Option<time::Duration>,
    schedule_to_start_timeout: Option<time::Duration>,
    start_to_close_timeout: Option<time::Duration>,
    heartbeat_timeout: Option<time::Duration>,
}

impl StoreState {
    /// Allocate the next conflict token for `key`, advancing the per-key
    /// high-water-mark. The generation is monotonic for the lifetime of the
    /// deployment name and never resets on delete-then-recreate, mirroring the
    /// strictly-increasing timestamp token of v1.31.0's deployment entity
    /// workflow (`service/worker/workerdeployment/workflow.go:248,502 @ v1.31.0`).
    fn allocate_deployment_token(&mut self, key: &DeploymentKey) -> ConflictToken {
        let generation = self.deployment_token_hwm.entry(key.clone()).or_insert(0);
        *generation += 1;
        ConflictToken::from_generation(*generation)
    }
}

impl InMemoryStore {
    /// Create a store with a configured shard count for
    /// shard-filtered queries.
    pub fn with_shard_count(shard_count: u32) -> Self {
        let state = StoreState {
            shard_count,
            ..StoreState::default()
        };
        Self {
            inner: Arc::new(Mutex::new(state)),
        }
    }

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

    fn effective_shard_count(store: &StoreState) -> u32 {
        store.shard_count.max(1)
    }
}

#[async_trait]
impl RunRepository for InMemoryStore {
    #[tracing::instrument(name = "storage.resolve_execution", skip(self), fields(namespace_id = %execution.namespace_id.0, workflow_id = %execution.workflow_id.0))]
    async fn resolve_execution(&self, execution: &ExecutionRef) -> Result<Option<RunKey>> {
        let _started = Instant::now();
        let store = self.inner.lock().await;
        let result = if let Some(run_id) = execution.run_id {
            Ok(store
                .execution_index
                .get(&(
                    execution.namespace_id,
                    execution.workflow_id.0.clone(),
                    run_id,
                ))
                .copied())
        } else {
            Ok(store
                .current_open
                .get(&(execution.namespace_id, execution.workflow_id.0.clone()))
                .copied())
        };
        storage_metrics::record_storage_operation("resolve_execution", "success");
        result
    }

    async fn find_latest_run(
        &self,
        namespace_id: NamespaceId,
        workflow_id: &WorkflowId,
    ) -> Result<Option<RunKey>> {
        let store = self.inner.lock().await;
        Ok(store
            .current_execution
            .get(&(namespace_id, workflow_id.0.clone()))
            .copied())
    }

    #[tracing::instrument(name = "storage.load_run", skip(self), fields(run_key = %run_key.0))]
    async fn load_run(&self, run_key: RunKey) -> Result<LoadedRun> {
        let started = Instant::now();
        let mut store = self.inner.lock().await;
        let result = Ok(match store.runs.get_mut(&run_key) {
            Some(state) => {
                clear_expired_sticky_if_needed(state, OffsetDateTime::now_utc());
                LoadedRun::Existing(state.clone())
            }
            None => LoadedRun::Absent,
        });
        storage_metrics::record_load_run_duration(started.elapsed());
        storage_metrics::record_storage_operation("load_run", "success");
        result
    }

    #[tracing::instrument(name = "storage.read_history", skip(self), fields(run_key = %run_key.0, after_event_id, limit))]
    async fn read_history(
        &self,
        run_key: RunKey,
        after_event_id: i64,
        limit: usize,
    ) -> Result<Vec<tokeira_kernel::HistoryEvent>> {
        let started = Instant::now();
        let store = self.inner.lock().await;
        let result = if let Some(history) = store.history.get(&run_key) {
            Ok(history
                .iter()
                .filter(|event| event.event_id > after_event_id)
                .take(limit)
                .cloned()
                .collect())
        } else {
            Ok(Vec::new())
        };
        storage_metrics::record_read_history_duration(started.elapsed());
        storage_metrics::record_storage_operation("read_history", "success");
        result
    }

    #[tracing::instrument(name = "storage.lookup_request_dedupe", skip(self), fields(namespace_id = %execution.namespace_id.0, workflow_id = %execution.workflow_id.0, request_id = %request_id.0))]
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

    async fn read_transition_audit(&self, run_key: RunKey) -> Result<Vec<TransitionAuditRecord>> {
        let store = self.inner.lock().await;
        Ok(store
            .transition_audit
            .get(&run_key)
            .cloned()
            .unwrap_or_default())
    }

    async fn has_open_pinned_workflows(
        &self,
        namespace_id: NamespaceId,
        version: &WorkerDeploymentVersionKey,
    ) -> Result<bool> {
        let store = self.inner.lock().await;
        Ok(store
            .runs
            .values()
            .any(|state| workflow_is_open_and_pinned_to_version(state, namespace_id, version)))
    }

    #[tracing::instrument(name = "storage.commit_transition", skip(self, transition), fields(run_key = %run_key.0, expected_seq = transition.expected_seq.0, epoch = epoch.0))]
    async fn commit_transition(
        &self,
        run_key: RunKey,
        transition: Transition,
        epoch: ShardEpoch,
    ) -> Result<CommitResult> {
        let started = Instant::now();
        let state = transition.next_state.clone();
        let namespace = Some(state.namespace_id.0.to_string());
        let mut store = self.inner.lock().await;
        if epoch != ShardEpoch::ZERO {
            let shard_id = tokeira_types::execution_home_bundle(
                state.namespace_id.0.as_bytes(),
                state.workflow_id.0.as_bytes(),
                Self::effective_shard_count(&store),
            );
            match store.bundle_leases.get(&shard_id) {
                Some((_owner, current_epoch, _endpoint)) if *current_epoch == epoch => {}
                Some((_owner, current_epoch, _endpoint)) => {
                    storage_metrics::record_commit_transition_duration(
                        namespace.clone(),
                        "conflict",
                        started.elapsed(),
                    );
                    storage_metrics::record_storage_operation("commit_transition", "conflict");
                    return Ok(CommitResult::Conflict {
                        reason: format!(
                            "stale shard epoch {:?} for shard {:?}; current {:?}",
                            epoch, shard_id, current_epoch
                        ),
                    });
                }
                None => {
                    storage_metrics::record_commit_transition_duration(
                        namespace.clone(),
                        "conflict",
                        started.elapsed(),
                    );
                    storage_metrics::record_storage_operation("commit_transition", "conflict");
                    return Ok(CommitResult::Conflict {
                        reason: format!(
                            "no active lease for shard {:?} at epoch {:?}",
                            shard_id, epoch
                        ),
                    });
                }
            }
        }
        if let Some(remaining) = store.conflict_injections.get_mut(&run_key)
            && *remaining > 0
        {
            *remaining -= 1;
            storage_metrics::record_commit_transition_duration(
                namespace.clone(),
                "conflict",
                started.elapsed(),
            );
            storage_metrics::record_storage_operation("commit_transition", "conflict");
            return Ok(CommitResult::Conflict {
                reason: format!("injected conflict for run {:?}", run_key),
            });
        }

        let current_seq = store
            .runs
            .get(&run_key)
            .map(|state| state.transition_seq)
            .unwrap_or(tokeira_types::TransitionSeq::ZERO);
        if current_seq != transition.expected_seq {
            storage_metrics::record_commit_transition_duration(
                namespace.clone(),
                "conflict",
                started.elapsed(),
            );
            storage_metrics::record_storage_operation("commit_transition", "conflict");
            return Ok(CommitResult::Conflict {
                reason: format!(
                    "expected seq {:?}, found {:?}",
                    transition.expected_seq, current_seq
                ),
            });
        }

        let workflow_key = (state.namespace_id, state.workflow_id.0.clone());

        for op in &transition.request_dedupe_ops {
            let dedupe_key = (
                workflow_key.0,
                workflow_key.1.clone(),
                op.request_id.0.clone(),
            );
            // Request-id dedupe is scoped to the RUN, mirroring v1.31.0 where
            // request ids live in the run's mutable state: a request id reused
            // against a NEW run (e.g. signal-with-start after the prior run
            // closed) is fresh, not a duplicate
            // (`pendingSignalRequestedIDs` / `request_id_infos` are per-run,
            // mutable_state_impl.go:2361-2398 @ v1.31.0; start-retry dedup is
            // the conflict-resolution layer's job, not this table's).
            if store
                .request_dedupe
                .get(&dedupe_key)
                .is_some_and(|record| record.run_id == state.run_id)
            {
                storage_metrics::record_commit_transition_duration(
                    namespace.clone(),
                    "duplicate",
                    started.elapsed(),
                );
                storage_metrics::record_storage_operation("commit_transition", "duplicate");
                return Ok(CommitResult::Duplicate);
            }
        }

        if transition.expected_seq == tokeira_types::TransitionSeq::ZERO && state.status.is_open() {
            match store.conflict_policy {
                CurrentExecutionConflictPolicy::Reject
                | CurrentExecutionConflictPolicy::AllowAfterClose => {
                    if let Some(existing_run) = store.current_open.get(&workflow_key)
                        && *existing_run != run_key
                        && let Some(existing_state) = store.runs.get(existing_run)
                        && existing_state.status.is_open()
                    {
                        storage_metrics::record_commit_transition_duration(
                            namespace.clone(),
                            "conflict",
                            started.elapsed(),
                        );
                        storage_metrics::record_storage_operation("commit_transition", "conflict");
                        // Report the current-execution collision (not deny): the
                        // runtime resolves it by the request's
                        // WorkflowIdConflictPolicy. Distinct from a transient
                        // `Conflict` so the lane does not OCC-retry it.
                        return Ok(CommitResult::CurrentExecutionConflict {
                            existing_run_key: *existing_run,
                            existing_status: existing_state.status,
                            request_ids: existing_state
                                .request_id_infos
                                .iter()
                                .map(|(id, info)| (id.clone(), info.clone()))
                                .collect(),
                        });
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
                    store
                        .activity_state_table
                        .insert((run_key, activity.activity_id.clone()), activity.clone());
                    // Activity state and activity dispatch are intentionally
                    // separate. Upserts always refresh state, but dispatch rows
                    // only survive while the activity remains eligible to be
                    // offered to a worker.
                    if activity.started_at.is_some() || activity.pause_info.is_some() {
                        store
                            .activity_dispatch
                            .remove(&(run_key, activity.activity_id.clone()));
                    } else if let Some(entry) = store
                        .activity_dispatch
                        .get_mut(&(run_key, activity.activity_id.clone()))
                    {
                        entry.task.queue = QueueKey {
                            namespace_id: state.namespace_id,
                            task_queue: activity.task_queue.clone(),
                            task_kind: TaskKind::Activity,
                            deployment: activity
                                .deployment
                                .clone()
                                .or_else(|| state.deployment.clone()),
                            build_id: activity.build_id.clone().or_else(|| state.build_id.clone()),
                        };
                        entry.task.input = activity.input.clone();
                        entry.task.schedule_event_id = activity.schedule_event_id;
                        entry.task.attempt = activity.attempt;
                        entry.schedule_to_close_timeout = activity.schedule_to_close_timeout;
                        entry.schedule_to_start_timeout = activity.schedule_to_start_timeout;
                        entry.start_to_close_timeout = activity.start_to_close_timeout;
                        entry.heartbeat_timeout = activity.heartbeat_timeout;
                    }
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
                dispatch_revision,
                schedule_to_close_timeout,
                schedule_to_start_timeout,
                start_to_close_timeout,
                heartbeat_timeout,
            } = op
            {
                // Enqueue is the only state transition effect that creates an
                // activity dispatch entry. Later ActivityOp::Upsert values may
                // update or remove this entry, but they cannot create it.
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
                            dispatch_revision: *dispatch_revision,
                        },
                        schedule_to_close_timeout: *schedule_to_close_timeout,
                        schedule_to_start_timeout: *schedule_to_start_timeout,
                        start_to_close_timeout: *start_to_close_timeout,
                        heartbeat_timeout: *heartbeat_timeout,
                    },
                );
            }
        }
        if state.status == ExecutionStatus::Paused {
            // Workflow pause suppresses all queued activity dispatch for this
            // run while preserving activity_state_table for later unpause.
            store
                .activity_dispatch
                .retain(|(entry_run_key, _), _| entry_run_key != &run_key);
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

        if transition.expected_seq == tokeira_types::TransitionSeq::ZERO {
            store
                .current_execution
                .insert(workflow_key.clone(), run_key);
        }

        if state.status.is_open() {
            store.current_open.insert(workflow_key.clone(), run_key);
        } else if store.current_open.get(&workflow_key) == Some(&run_key) {
            store.current_open.remove(&workflow_key);
        }

        // The projection log carries a complete post-transition visibility
        // image. Emitting a row for every committed transition keeps the
        // in-memory reference store aligned with DSQL and prevents visibility
        // freshness from depending on whether the kernel emitted a legacy delta.
        store.projection_log.push(ProjectionRecord {
            partition_id: partition_for(run_key),
            fanout: 1,
            run_key,
            transition_seq: state.transition_seq,
            context: workflow_projection_context(&state)?,
        });

        if transition.expected_seq == tokeira_types::TransitionSeq::ZERO {
            let shard_id = shard_for_run_key(run_key, Self::effective_shard_count(&store));
            store.run_shard_map.insert(run_key, shard_id);
        }

        storage_metrics::record_commit_transition_duration(namespace, "applied", started.elapsed());
        storage_metrics::record_storage_operation("commit_transition", "success");
        Ok(CommitResult::Applied { new_state: state })
    }

    async fn commit_transition_for_bundle(
        &self,
        run_key: RunKey,
        _execution_home_bundle: ShardId,
        transition: Transition,
        epoch: ShardEpoch,
    ) -> Result<CommitResult> {
        // Pass the real epoch through to commit_transition so the epoch check
        // and the mutation write execute under the same Mutex lock acquisition.
        // Previously this method acquired the lock for an epoch check, dropped
        // it, then called commit_transition with ShardEpoch::ZERO — creating a
        // TOCTOU race window. commit_transition already performs an atomic epoch
        // check when epoch != ShardEpoch::ZERO, so forwarding the real epoch is
        // sufficient to close the race.
        self.commit_transition(run_key, transition, epoch).await
    }

    #[tracing::instrument(
        name = "storage.delete_run_for_bundle",
        skip(self),
        fields(
            run_key = %run_key.0,
            bundle = execution_home_bundle.0,
            expected_seq = request.expected_seq.0,
            epoch = epoch.0
        )
    )]
    async fn delete_run_for_bundle(
        &self,
        run_key: RunKey,
        execution_home_bundle: ShardId,
        request: DeleteRunRequest,
        epoch: ShardEpoch,
    ) -> Result<DeleteRunResult> {
        let mut store = self.inner.lock().await;
        let Some(state) = store.runs.get(&run_key).cloned() else {
            return Ok(DeleteRunResult::NotFound);
        };

        let derived_bundle = tokeira_types::execution_home_bundle(
            state.namespace_id.0.as_bytes(),
            state.workflow_id.0.as_bytes(),
            Self::effective_shard_count(&store),
        );
        if derived_bundle != execution_home_bundle {
            return Ok(DeleteRunResult::Conflict {
                reason: format!(
                    "execution-home bundle mismatch for {run_key:?}: expected {derived_bundle:?}, got {execution_home_bundle:?}"
                ),
            });
        }

        if epoch != ShardEpoch::ZERO {
            match store.bundle_leases.get(&execution_home_bundle) {
                Some((_owner, current_epoch, _endpoint)) if *current_epoch == epoch => {}
                Some((_owner, current_epoch, _endpoint)) => {
                    return Ok(DeleteRunResult::Conflict {
                        reason: format!(
                            "stale shard epoch {epoch:?} for shard {execution_home_bundle:?}; current {current_epoch:?}"
                        ),
                    });
                }
                None => {
                    return Ok(DeleteRunResult::Conflict {
                        reason: format!(
                            "no active lease for shard {execution_home_bundle:?} at epoch {epoch:?}"
                        ),
                    });
                }
            }
        }

        if state.transition_seq != request.expected_seq {
            return Ok(DeleteRunResult::Conflict {
                reason: format!(
                    "expected seq {:?}, found {:?}",
                    request.expected_seq, state.transition_seq
                ),
            });
        }
        if state.status.is_open() {
            return Ok(DeleteRunResult::Conflict {
                reason: "workflow must be closed before authoritative deletion".to_owned(),
            });
        }

        let tombstone_seq = state.transition_seq.next();
        let mut tombstone_state = state.clone();
        tombstone_state.transition_seq = tombstone_seq;
        let tombstone = ProjectionRecord {
            partition_id: partition_for(run_key),
            fanout: 1,
            run_key,
            transition_seq: tombstone_seq,
            context: deleted_workflow_projection_context(&tombstone_state, request.deleted_at)?,
        };
        // The tombstone and purge share this lock acquisition. No reader can
        // observe the run removed without its anti-resurrection record present.
        store.projection_log.push(tombstone.clone());

        let workflow_key = (state.namespace_id, state.workflow_id.0.clone());
        if store.current_open.get(&workflow_key) == Some(&run_key) {
            store.current_open.remove(&workflow_key);
        }
        if store.current_execution.get(&workflow_key) == Some(&run_key) {
            store.current_execution.remove(&workflow_key);
        }
        store.execution_index.remove(&(
            state.namespace_id,
            state.workflow_id.0.clone(),
            state.run_id,
        ));
        store.runs.remove(&run_key);
        store.history.remove(&run_key);
        store.transition_audit.remove(&run_key);
        store.run_shard_map.remove(&run_key);
        store.conflict_injections.remove(&run_key);
        store
            .request_dedupe
            .retain(|_, record| record.run_key != run_key);
        store
            .activity_state_table
            .retain(|(candidate, _), _| *candidate != run_key);
        store
            .timer_bucket
            .retain(|(candidate, _), _| *candidate != run_key);
        store
            .activity_dispatch
            .retain(|(candidate, _), _| *candidate != run_key);
        store
            .dispatch_backlog
            .retain(|entry| entry.run_key != run_key);

        Ok(DeleteRunResult::Deleted { tombstone })
    }

    async fn materialize_reset_successor(
        &self,
        base_run_key: RunKey,
        fork_event_id: i64,
        successor_run_id: RunId,
    ) -> Result<()> {
        let mut store = self.inner.lock().await;

        let base_state = store
            .runs
            .get(&base_run_key)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("base run not found: {:?}", base_run_key))?;
        let successor_run_key = RunKey::derive(
            base_state.namespace_id,
            &base_state.workflow_id,
            successor_run_id,
        );

        if store.runs.contains_key(&successor_run_key) {
            anyhow::bail!(
                "successor run already exists for {:?}: {:?}",
                successor_run_id,
                successor_run_key
            );
        }

        let base_history = store
            .history
            .get(&base_run_key)
            .ok_or_else(|| anyhow::anyhow!("base history not found: {:?}", base_run_key))?;

        // The fork event is the WFT-FINISH event being reset; the successor
        // branch keeps only the events BEFORE it — v1.31.0 rebuilds mutable
        // state to `WorkflowTaskFinishEventId - 1`
        // (`baseRebuildLastEventID`, resetworkflow/api.go:119 @ v1.31.0). The
        // replayed successor thus ends with that WFT still started; the reset
        // flow then fails it with cause ResetWorkflow and re-dispatches.
        let prefix_len = base_history
            .iter()
            .position(|event| event.event_id == fork_event_id)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "fork_event_id {} outside committed history for {:?}",
                    fork_event_id,
                    base_run_key
                )
            })?;
        let copied_history: Vec<_> = base_history[..prefix_len].to_vec();

        let kernel = BasicKernel;
        let replay_ctx = ReplayContext {
            run_key: successor_run_key,
            namespace_id: base_state.namespace_id,
            workflow_id: base_state.workflow_id.clone(),
            run_id: successor_run_id,
            deployment: base_state.deployment.clone(),
            build_id: base_state.build_id.clone(),
            parent_run_key: base_state.parent_run_key,
            parent_workflow_id: base_state.parent_workflow_id.clone(),
            first_run_started_at: base_state.first_run_started_at,
        };
        let mut successor_state = kernel.replay_history_prefix(replay_ctx, &copied_history)?;
        // The reset run points back to the chain's origin: inherit the base's
        // `OriginalExecutionRunId` (or the base itself when the base is the origin),
        // so repeated resets all reference the same original run.
        successor_state.original_execution_run_id = base_state
            .original_execution_run_id
            .or(Some(base_state.run_id));
        // The successor's run/execution-timeout windows restart at reset time:
        // v1.31.0's resetter calls `RefreshExpirationTimeoutTask`, which
        // re-anchors BOTH expirations at now + timeout
        // (mutable_state_impl.go:8417 @ v1.31.0) — the replayed prefix's
        // original timestamps must not leave the successor born expired
        // (TestResetWorkflowAfterTimeout resets well past the base's 1s
        // window and still expects a usable successor). Tokeira's sweep
        // derives both deadlines from these two anchors.
        let materialized_at = time::OffsetDateTime::now_utc();
        successor_state.started_at = materialized_at;
        successor_state.first_run_started_at = Some(materialized_at);

        store.history.insert(successor_run_key, copied_history);
        store
            .runs
            .insert(successor_run_key, successor_state.clone());
        store.execution_index.insert(
            (
                successor_state.namespace_id,
                successor_state.workflow_id.0.clone(),
                successor_state.run_id,
            ),
            successor_run_key,
        );
        store.current_execution.insert(
            (
                successor_state.namespace_id,
                successor_state.workflow_id.0.clone(),
            ),
            successor_run_key,
        );
        if successor_state.status.is_open() {
            store.current_open.insert(
                (
                    successor_state.namespace_id,
                    successor_state.workflow_id.0.clone(),
                ),
                successor_run_key,
            );
        }

        for activity in successor_state.activities.values() {
            store.activity_state_table.insert(
                (successor_run_key, activity.activity_id.clone()),
                activity.clone(),
            );
        }
        for timer in successor_state.timers.values() {
            store
                .timer_bucket
                .insert((successor_run_key, timer.timer_id.clone()), timer.clone());
        }

        let shard_id = shard_for_run_key(successor_run_key, Self::effective_shard_count(&store));
        store.run_shard_map.insert(successor_run_key, shard_id);

        Ok(())
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
                sticky_preferred: state.sticky.as_ref().map(|s| s.worker_identity.clone()),
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

    async fn drain_backlog(&self, queue: &QueueKey, limit: usize) -> Result<Vec<BacklogEntry>> {
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

    async fn list_due_timers(&self, now: OffsetDateTime, limit: usize) -> Result<Vec<DueTimer>> {
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

    async fn list_dispatchable_workflow_tasks_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<DispatchableWorkflowTask>> {
        let mut store = self.inner.lock().await;
        let now = OffsetDateTime::now_utc();

        // Collect matching run keys first so we can do the
        // mutable sticky cleanup without cloning the shard map.
        let candidates: Vec<RunKey> = store
            .runs
            .keys()
            .filter(|rk| store.run_shard_map.get(rk) == Some(&shard_id))
            .copied()
            .collect();

        let mut out = Vec::new();
        for run_key in candidates {
            let Some(state) = store.runs.get_mut(&run_key) else {
                continue;
            };
            clear_expired_sticky_if_needed(state, now);
            let Some(pending) = &state.pending_workflow_task else {
                continue;
            };
            if pending.started_event_id.is_some() {
                continue;
            }
            out.push(DispatchableWorkflowTask {
                run_key: state.run_key,
                queue: QueueKey {
                    namespace_id: state.namespace_id,
                    task_queue: state.task_queue.clone(),
                    task_kind: tokeira_types::TaskKind::Workflow,
                    deployment: None,
                    build_id: None,
                },
                logical_seq: pending.logical_seq,
                sticky_preferred: state.sticky.as_ref().map(|s| s.worker_identity.clone()),
                sticky_expires_at: state.sticky.as_ref().map(|s| s.expires_at),
            });
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    async fn list_dispatchable_activity_tasks_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<DispatchableActivityTask>> {
        let store = self.inner.lock().await;
        Ok(store
            .activity_dispatch
            .values()
            .filter(|entry| store.run_shard_map.get(&entry.task.run_key) == Some(&shard_id))
            .take(limit)
            .map(|entry| entry.task.clone())
            .collect())
    }

    async fn list_due_timers_for_shard(
        &self,
        shard_id: ShardId,
        now: OffsetDateTime,
        limit: usize,
    ) -> Result<Vec<DueTimer>> {
        let store = self.inner.lock().await;
        let mut due = Vec::new();
        for ((run_key, _), timer) in &store.timer_bucket {
            if store.run_shard_map.get(run_key) != Some(&shard_id) {
                continue;
            }
            if timer.fire_at <= now {
                due.push(DueTimer {
                    run_key: *run_key,
                    timer_id: timer.timer_id.clone(),
                });
                if due.len() >= limit {
                    break;
                }
            }
        }
        Ok(due)
    }

    async fn list_runs_with_workflow_timeouts_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<WorkflowTimeoutSweepEntry>> {
        let store = self.inner.lock().await;
        let mut out = Vec::new();
        for state in store.runs.values() {
            if store.run_shard_map.get(&state.run_key) != Some(&shard_id) {
                continue;
            }
            if !state.is_open() {
                continue;
            }
            if state.workflow_execution_timeout.is_none() && state.workflow_run_timeout.is_none() {
                continue;
            }
            out.push(WorkflowTimeoutSweepEntry {
                run_key: state.run_key,
                workflow_execution_timeout: state.workflow_execution_timeout,
                workflow_run_timeout: state.workflow_run_timeout,
                started_at: state.started_at,
                workflow_start_delay: state.workflow_start_delay,
                first_run_started_at: state.first_run_started_at,
                has_retry_policy: state.retry_policy.is_some(),
            });
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    async fn list_started_workflow_tasks_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<WftTimeoutSweepEntry>> {
        let store = self.inner.lock().await;
        let mut out = Vec::new();
        for state in store.runs.values() {
            if store.run_shard_map.get(&state.run_key) != Some(&shard_id) {
                continue;
            }
            let Some(pending) = state.pending_workflow_task.as_ref() else {
                continue;
            };
            let (Some(started_event_id), Some(started_at)) =
                (pending.started_event_id, pending.started_at)
            else {
                continue;
            };
            out.push(WftTimeoutSweepEntry {
                run_key: state.run_key,
                logical_seq: pending.logical_seq,
                started_event_id,
                started_at,
                workflow_task_timeout: state.workflow_task_timeout,
            });
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    async fn list_open_activities_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<ActivitySweepEntry>> {
        let store = self.inner.lock().await;
        let mut out = Vec::new();
        for ((run_key, _), activity) in &store.activity_state_table {
            if store.run_shard_map.get(run_key) != Some(&shard_id) {
                continue;
            }
            out.push(ActivitySweepEntry {
                run_key: *run_key,
                activity_id: activity.activity_id.clone(),
                schedule_event_id: activity.schedule_event_id,
                attempt: activity.attempt,
                original_scheduled_at: activity.scheduled_at,
                started_at: activity.started_at,
                schedule_to_close_timeout: activity.schedule_to_close_timeout,
                schedule_to_start_timeout: activity.schedule_to_start_timeout,
                start_to_close_timeout: activity.start_to_close_timeout,
                heartbeat_timeout: activity.heartbeat_timeout,
            });
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    async fn list_pending_nexus_operations_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<NexusSweepEntry>> {
        let store = self.inner.lock().await;
        let mut out = Vec::new();
        for state in store.runs.values() {
            if store.run_shard_map.get(&state.run_key) != Some(&shard_id) {
                continue;
            }
            if !state.is_open() {
                continue;
            }
            for op in state.pending_nexus_operations.values() {
                // Only include operations with at least one timeout configured —
                // operations without any timeout don't need tracking reconstruction.
                if op.schedule_to_close_timeout.is_none()
                    && op.schedule_to_start_timeout.is_none()
                    && op.start_to_close_timeout.is_none()
                {
                    continue;
                }
                out.push(NexusSweepEntry {
                    run_key: state.run_key,
                    operation_id: op.operation_id.clone(),
                    scheduled_event_id: op.scheduled_event_id,
                    scheduled_at: op.scheduled_at,
                });
                if out.len() >= limit {
                    return Ok(out);
                }
            }
        }
        Ok(out)
    }

    async fn list_runs_with_pending_completion_callbacks_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<CompletionCallbackSweepEntry>> {
        let store = self.inner.lock().await;
        let mut out = Vec::new();
        for state in store.runs.values() {
            if store.run_shard_map.get(&state.run_key) != Some(&shard_id) {
                continue;
            }
            // A callback is pending delivery once the run closes: `Scheduled` (fired,
            // not yet attempted) or `BackingOff` (attempt failed, awaiting retry). Both
            // must be re-watched so a `Scheduled` callback whose first attempt was lost
            // to a crash is re-driven; terminal/Standby callbacks are not the scanner's.
            for (callback_index, callback) in state.completion_callbacks.iter().enumerate() {
                if !matches!(
                    callback.state,
                    CallbackState::Scheduled | CallbackState::BackingOff
                ) {
                    continue;
                }
                out.push(CompletionCallbackSweepEntry {
                    run_key: state.run_key,
                    callback_index,
                });
                if out.len() >= limit {
                    return Ok(out);
                }
            }
        }
        Ok(out)
    }
}

#[async_trait]
impl WorkerDeploymentRepository for InMemoryStore {
    async fn load_deployment(&self, key: &DeploymentKey) -> Result<Option<StoredWorkerDeployment>> {
        let store = self.inner.lock().await;
        Ok(store.worker_deployments.get(key).cloned())
    }

    async fn put_deployment(
        &self,
        mut record: StoredWorkerDeployment,
        expected: Option<ConflictToken>,
    ) -> Result<DeploymentCasResult> {
        let mut store = self.inner.lock().await;
        let key = DeploymentKey {
            namespace_id: record.namespace_id,
            deployment_name: record.name.clone(),
        };

        match (store.worker_deployments.get(&key), expected) {
            (Some(_), None) => Ok(DeploymentCasResult::AlreadyExists),
            (None, None) => {
                let token = store.allocate_deployment_token(&key);
                record.conflict_token = token;
                store.worker_deployments.insert(key, record);
                Ok(DeploymentCasResult::Applied { token })
            }
            (None, Some(_)) => Ok(DeploymentCasResult::NotFound),
            (Some(current), Some(expected)) if current.conflict_token != expected => {
                Ok(DeploymentCasResult::Conflict)
            }
            (Some(_), Some(_)) => {
                let token = store.allocate_deployment_token(&key);
                record.conflict_token = token;
                store.worker_deployments.insert(key, record);
                Ok(DeploymentCasResult::Applied { token })
            }
        }
    }

    async fn delete_deployment(
        &self,
        key: &DeploymentKey,
        expected: ConflictToken,
    ) -> Result<DeploymentCasResult> {
        let mut store = self.inner.lock().await;
        let Some(current) = store.worker_deployments.get(key) else {
            return Ok(DeploymentCasResult::NotFound);
        };
        if current.conflict_token != expected {
            return Ok(DeploymentCasResult::Conflict);
        }
        let token = store.allocate_deployment_token(key);
        store.worker_deployments.remove(key);
        Ok(DeploymentCasResult::Applied { token })
    }

    async fn list_deployments(
        &self,
        namespace_id: NamespaceId,
        after: Option<&DeploymentName>,
        limit: usize,
    ) -> Result<Vec<StoredWorkerDeployment>> {
        let store = self.inner.lock().await;
        let mut records: Vec<_> = store
            .worker_deployments
            .values()
            .filter(|record| record.namespace_id == namespace_id)
            .filter(|record| after.is_none_or(|after| record.name > *after))
            .cloned()
            .collect();
        records.sort_by(|left, right| left.name.cmp(&right.name));
        records.truncate(limit);
        Ok(records)
    }

    async fn list_all_for_namespace(
        &self,
        namespace_id: NamespaceId,
    ) -> Result<Vec<StoredWorkerDeployment>> {
        let store = self.inner.lock().await;
        let mut records: Vec<_> = store
            .worker_deployments
            .values()
            .filter(|record| record.namespace_id == namespace_id)
            .cloned()
            .collect();
        records.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(records)
    }
}

#[async_trait]
impl ProjectionLog for InMemoryStore {
    async fn read_from(&self, cursor: &ProjectionCursor, limit: usize) -> Result<ProjectionBatch> {
        let store = self.inner.lock().await;
        let mut started = cursor.last_transition_seq.is_none();
        let mut out = Vec::new();
        for record in store.projection_log.iter() {
            if record.partition_id != cursor.partition_id || record.fanout != cursor.fanout {
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
        node_endpoint: String,
    ) -> Result<LeaseOutcome> {
        let mut store = self.inner.lock().await;
        match store.bundle_leases.get_mut(&bundle) {
            Some((current_owner, current_epoch, current_endpoint)) => {
                if current_owner.as_deref() == Some(owner.as_str()) {
                    *current_endpoint = Some(node_endpoint);
                    Ok(LeaseOutcome::Acquired {
                        epoch: *current_epoch,
                    })
                } else if current_owner.is_none() {
                    *current_epoch = current_epoch.next();
                    *current_owner = Some(owner);
                    *current_endpoint = Some(node_endpoint);
                    Ok(LeaseOutcome::Acquired {
                        epoch: *current_epoch,
                    })
                } else {
                    Ok(LeaseOutcome::Rejected {
                        current_owner: current_owner.clone().unwrap_or_default(),
                        current_epoch: *current_epoch,
                    })
                }
            }
            None => {
                let epoch = ShardEpoch(1);
                store
                    .bundle_leases
                    .insert(bundle, (Some(owner), epoch, Some(node_endpoint)));
                Ok(LeaseOutcome::Acquired { epoch })
            }
        }
    }

    async fn renew_bundle(
        &self,
        bundle: ShardId,
        owner: String,
        epoch: ShardEpoch,
        node_endpoint: String,
    ) -> Result<LeaseOutcome> {
        let mut store = self.inner.lock().await;
        match store.bundle_leases.get_mut(&bundle) {
            Some((current_owner, current_epoch, current_endpoint))
                if current_owner.as_deref() == Some(owner.as_str()) && *current_epoch == epoch =>
            {
                *current_endpoint = Some(node_endpoint);
                Ok(LeaseOutcome::Renewed { epoch })
            }
            Some((current_owner, current_epoch, _endpoint)) => Ok(LeaseOutcome::Rejected {
                current_owner: current_owner.clone().unwrap_or_default(),
                current_epoch: *current_epoch,
            }),
            None => {
                store
                    .bundle_leases
                    .insert(bundle, (Some(owner), epoch, Some(node_endpoint)));
                Ok(LeaseOutcome::Acquired { epoch })
            }
        }
    }

    async fn list_bundle_leases(&self) -> Result<Vec<BundleLease>> {
        let store = self.inner.lock().await;
        Ok(store
            .bundle_leases
            .iter()
            .map(|(bundle_id, (owner, epoch, endpoint))| BundleLease {
                bundle_id: *bundle_id,
                owner_node_id: owner.clone(),
                epoch: *epoch,
                lease_until: OffsetDateTime::now_utc(),
                node_endpoint: endpoint.clone(),
            })
            .collect())
    }

    async fn relinquish_bundle(
        &self,
        bundle: ShardId,
        owner: String,
        epoch: ShardEpoch,
    ) -> Result<LeaseOutcome> {
        let mut store = self.inner.lock().await;
        match store.bundle_leases.get_mut(&bundle) {
            Some((current_owner, current_epoch, current_endpoint))
                if current_owner.as_deref() == Some(owner.as_str()) && *current_epoch == epoch =>
            {
                *current_owner = None;
                *current_epoch = current_epoch.next();
                *current_endpoint = None;
                Ok(LeaseOutcome::Acquired {
                    epoch: *current_epoch,
                })
            }
            Some((current_owner, current_epoch, _endpoint)) => Ok(LeaseOutcome::Rejected {
                current_owner: current_owner.clone().unwrap_or_default(),
                current_epoch: *current_epoch,
            }),
            None => Ok(LeaseOutcome::Rejected {
                current_owner: String::new(),
                current_epoch: ShardEpoch::ZERO,
            }),
        }
    }
}

#[async_trait]
impl ControlRepository for InMemoryStore {
    async fn advance_generation(
        &self,
        expected: GenerationCounter,
    ) -> Result<GenerationAdvanceResult> {
        let mut store = self.inner.lock().await;
        if store.routing_generation == expected {
            store.routing_generation = store.routing_generation.next();
            Ok(GenerationAdvanceResult::Advanced(store.routing_generation))
        } else {
            Ok(GenerationAdvanceResult::Conflict(store.routing_generation))
        }
    }

    async fn current_generation(&self) -> Result<GenerationCounter> {
        Ok(self.inner.lock().await.routing_generation)
    }

    async fn allocate_budget(
        &self,
        expected_version: u64,
        _allocator_id: uuid::Uuid,
        _rate_budget: f64,
        _capacity_budget: u64,
    ) -> Result<BudgetAllocationResult> {
        let mut store = self.inner.lock().await;
        if store.budget_version == expected_version {
            store.budget_version += 1;
            Ok(BudgetAllocationResult::Allocated {
                version: store.budget_version,
            })
        } else {
            Ok(BudgetAllocationResult::Conflict {
                current_version: store.budget_version,
            })
        }
    }

    async fn current_budget_version(&self) -> Result<u64> {
        Ok(self.inner.lock().await.budget_version)
    }
}

#[async_trait]
impl ConnectionDirector for InMemoryStore {
    type Permit = DbPermit;

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

fn shard_for_run_key(run_key: RunKey, shard_count: u32) -> ShardId {
    ShardId((run_key.0.as_u128() as u32) % shard_count.max(1))
}

#[cfg(test)]
mod tests {
    // These tests use `ShardEpoch::ZERO` intentionally: they validate storage
    // mechanics (transition_seq OCC, activity side-tables, timer side-tables,
    // workflow-id uniqueness) without a placement controller. Epoch fencing is
    // tested separately in the fencing-specific test suites.
    use std::collections::{BTreeMap, BTreeSet};

    use proptest::prelude::*;
    use time::Duration;
    use tokeira_kernel::{
        ActivityPauseInfo, PauseInfo, PendingWorkflowTask, RequestDedupeOp,
        event::{HistoryEvent, HistoryEventKind},
        state::{VersioningOverride, WorkerDeploymentVersionRef, WorkflowVersioningInfo},
    };
    use tokeira_types::{
        ExecutionRef, ExecutionStatus, LogicalTaskSeq, Memo, NamespaceId, QueueKey, RequestId,
        RunId, RunKey, SearchAttrValue, SearchAttributes, TaskKind, TaskQueueName, TransitionSeq,
        VisibilityLifecycleState, WorkerIdentity, WorkflowId, WorkflowType,
    };
    use tracing::{
        Subscriber,
        span::{Attributes, Id},
    };
    use tracing_subscriber::{
        Layer,
        layer::{Context, SubscriberExt},
        registry::LookupSpan,
    };

    use crate::api::{
        RoutingConfigUpdateState, RunRepository, StoredRoutingConfig, WorkerDeploymentRepository,
    };

    use super::*;

    #[derive(Clone, Default)]
    struct SpanNames(std::sync::Arc<std::sync::Mutex<Vec<String>>>);

    impl<S> Layer<S> for SpanNames
    where
        S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    {
        fn on_new_span(&self, _attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
            if let Some(span) = ctx.span(id) {
                self.0
                    .lock()
                    .unwrap()
                    .push(span.metadata().name().to_string());
            }
        }
    }

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

    fn sample_deployment(namespace_id: NamespaceId, name: &str) -> StoredWorkerDeployment {
        StoredWorkerDeployment {
            namespace_id,
            name: DeploymentName(name.to_owned()),
            create_time: fixed_now(),
            routing_config: StoredRoutingConfig::default(),
            last_modifier_identity: "tester".to_owned(),
            manager_identity: None,
            routing_config_update_state: RoutingConfigUpdateState::default(),
            versions: BTreeMap::new(),
            conflict_token: ConflictToken::default(),
            create_request_ids: BTreeSet::from([format!("create-{name}")]),
        }
    }

    fn deployment_key(namespace_id: NamespaceId, name: &str) -> DeploymentKey {
        DeploymentKey {
            namespace_id,
            deployment_name: DeploymentName(name.to_owned()),
        }
    }

    #[tokio::test]
    async fn worker_deployment_create_on_existing_returns_already_exists_without_mutation() {
        let store = InMemoryStore::default();
        let namespace_id = NamespaceId::new();
        let record = sample_deployment(namespace_id, "alpha");
        let key = deployment_key(namespace_id, "alpha");

        let applied = store.put_deployment(record.clone(), None).await.unwrap();
        let DeploymentCasResult::Applied { token } = applied else {
            panic!("fresh create should apply, got {applied:?}");
        };
        let duplicate = store.put_deployment(record, None).await.unwrap();

        assert_eq!(duplicate, DeploymentCasResult::AlreadyExists);
        let stored = store.load_deployment(&key).await.unwrap().unwrap();
        assert_eq!(stored.conflict_token, token);
        assert_eq!(stored.last_modifier_identity, "tester");
    }

    #[tokio::test]
    async fn worker_deployment_stale_token_write_conflicts_and_preserves_state() {
        let store = InMemoryStore::default();
        let namespace_id = NamespaceId::new();
        let key = deployment_key(namespace_id, "alpha");

        let created = store
            .put_deployment(sample_deployment(namespace_id, "alpha"), None)
            .await
            .unwrap();
        let DeploymentCasResult::Applied {
            token: create_token,
        } = created
        else {
            panic!("fresh create should apply, got {created:?}");
        };

        let mut current_update = sample_deployment(namespace_id, "alpha");
        current_update.last_modifier_identity = "current-writer".to_owned();
        let updated = store
            .put_deployment(current_update.clone(), Some(create_token))
            .await
            .unwrap();
        let DeploymentCasResult::Applied {
            token: update_token,
        } = updated
        else {
            panic!("current-token update should apply, got {updated:?}");
        };
        assert!(update_token.generation() > create_token.generation());
        current_update.conflict_token = update_token;

        let mut stale_update = sample_deployment(namespace_id, "alpha");
        stale_update.last_modifier_identity = "stale-writer".to_owned();
        let stale = store
            .put_deployment(stale_update, Some(create_token))
            .await
            .unwrap();

        assert_eq!(stale, DeploymentCasResult::Conflict);
        assert_eq!(
            store.load_deployment(&key).await.unwrap(),
            Some(current_update)
        );
    }

    #[tokio::test]
    async fn worker_deployment_none_and_current_token_writes_advance_generation() {
        let store = InMemoryStore::default();
        let namespace_id = NamespaceId::new();
        let key = deployment_key(namespace_id, "alpha");

        let created = store
            .put_deployment(sample_deployment(namespace_id, "alpha"), None)
            .await
            .unwrap();
        let DeploymentCasResult::Applied {
            token: create_token,
        } = created
        else {
            panic!("none-token create should apply, got {created:?}");
        };
        assert_eq!(create_token.generation(), 1);

        let mut updated_record = sample_deployment(namespace_id, "alpha");
        updated_record.manager_identity = Some("manager".to_owned());
        let updated = store
            .put_deployment(updated_record.clone(), Some(create_token))
            .await
            .unwrap();
        let DeploymentCasResult::Applied {
            token: update_token,
        } = updated
        else {
            panic!("current-token write should apply, got {updated:?}");
        };

        assert_eq!(update_token.generation(), create_token.generation() + 1);
        updated_record.conflict_token = update_token;
        assert_eq!(
            store.load_deployment(&key).await.unwrap(),
            Some(updated_record)
        );
    }

    #[tokio::test]
    async fn worker_deployment_list_pages_every_record_once_until_empty_page() {
        let store = InMemoryStore::default();
        let namespace_id = NamespaceId::new();
        let other_namespace_id = NamespaceId::new();
        for name in ["gamma", "alpha", "epsilon", "beta", "delta"] {
            let result = store
                .put_deployment(sample_deployment(namespace_id, name), None)
                .await
                .unwrap();
            assert!(matches!(result, DeploymentCasResult::Applied { .. }));
        }
        let other_result = store
            .put_deployment(sample_deployment(other_namespace_id, "alpha"), None)
            .await
            .unwrap();
        assert!(matches!(other_result, DeploymentCasResult::Applied { .. }));

        let mut after = None;
        let mut seen = Vec::new();
        loop {
            let page = store
                .list_deployments(namespace_id, after.as_ref(), 2)
                .await
                .unwrap();
            if page.is_empty() {
                break;
            }
            after = page.last().map(|record| record.name.clone());
            seen.extend(page.into_iter().map(|record| record.name.0));
        }

        assert_eq!(seen, ["alpha", "beta", "delta", "epsilon", "gamma"]);
        assert_eq!(
            store
                .list_deployments(namespace_id, after.as_ref(), 2)
                .await
                .unwrap(),
            Vec::<StoredWorkerDeployment>::new()
        );
    }

    #[tokio::test]
    async fn worker_deployment_current_token_delete_applies_and_stale_delete_conflicts() {
        let store = InMemoryStore::default();
        let namespace_id = NamespaceId::new();
        let key = deployment_key(namespace_id, "alpha");

        let created = store
            .put_deployment(sample_deployment(namespace_id, "alpha"), None)
            .await
            .unwrap();
        let DeploymentCasResult::Applied {
            token: create_token,
        } = created
        else {
            panic!("fresh create should apply, got {created:?}");
        };
        let mut updated_record = sample_deployment(namespace_id, "alpha");
        updated_record.last_modifier_identity = "current-writer".to_owned();
        let updated = store
            .put_deployment(updated_record, Some(create_token))
            .await
            .unwrap();
        let DeploymentCasResult::Applied {
            token: update_token,
        } = updated
        else {
            panic!("current-token update should apply, got {updated:?}");
        };

        assert_eq!(
            store.delete_deployment(&key, create_token).await.unwrap(),
            DeploymentCasResult::Conflict
        );
        let deleted = store.delete_deployment(&key, update_token).await.unwrap();
        let DeploymentCasResult::Applied {
            token: delete_token,
        } = deleted
        else {
            panic!("current-token delete should apply, got {deleted:?}");
        };
        assert_eq!(delete_token.generation(), update_token.generation() + 1);
        assert_eq!(store.load_deployment(&key).await.unwrap(), None);
    }

    #[tokio::test]
    async fn worker_deployment_recreate_after_delete_issues_distinct_token() {
        // v1.31.0 derives the conflict token from the deployment entity
        // workflow's clock (`service/worker/workerdeployment/workflow.go:248,502
        // @ v1.31.0`), so a recreated deployment never reuses a prior token. The
        // per-name high-water-mark must not reset to 1 on delete-then-recreate.
        // Conformance: tests/worker_deployment_test.go
        // TestCreateWorkerDeployment_AfterDelete_CanRecreate @ v1.31.0.
        let store = InMemoryStore::default();
        let namespace_id = NamespaceId::new();
        let key = deployment_key(namespace_id, "alpha");

        let created = store
            .put_deployment(sample_deployment(namespace_id, "alpha"), None)
            .await
            .unwrap();
        let DeploymentCasResult::Applied { token: first_token } = created else {
            panic!("fresh create should apply, got {created:?}");
        };
        store.delete_deployment(&key, first_token).await.unwrap();

        let recreated = store
            .put_deployment(sample_deployment(namespace_id, "alpha"), None)
            .await
            .unwrap();
        let DeploymentCasResult::Applied {
            token: second_token,
        } = recreated
        else {
            panic!("recreate after delete should apply, got {recreated:?}");
        };

        assert_ne!(
            first_token, second_token,
            "recreated deployment must not reuse the prior conflict token"
        );
        assert!(
            second_token.generation() > first_token.generation(),
            "recreated token generation must advance monotonically"
        );
    }

    #[tokio::test]
    async fn lease_repository_lists_relinquishes_and_reacquires_unowned_bundles() {
        let store = InMemoryStore::default();
        let first = store
            .try_acquire_bundle(
                ShardId(1),
                "owner-a".to_owned(),
                "127.0.0.1:7233".to_owned(),
            )
            .await
            .unwrap();
        let LeaseOutcome::Acquired { epoch } = first else {
            panic!("expected initial acquire");
        };
        store
            .try_acquire_bundle(
                ShardId(2),
                "owner-b".to_owned(),
                "127.0.0.1:7234".to_owned(),
            )
            .await
            .unwrap();

        let leases = store.list_bundle_leases().await.unwrap();
        assert_eq!(leases.len(), 2);
        assert!(leases.iter().any(|lease| {
            lease.bundle_id == ShardId(1)
                && lease.owner_node_id.as_deref() == Some("owner-a")
                && lease.node_endpoint.as_deref() == Some("127.0.0.1:7233")
        }));

        let relinquished = store
            .relinquish_bundle(ShardId(1), "owner-a".to_owned(), epoch)
            .await
            .unwrap();
        let LeaseOutcome::Acquired {
            epoch: relinquished_epoch,
        } = relinquished
        else {
            panic!("expected relinquish to advance epoch");
        };
        assert!(relinquished_epoch.0 > epoch.0);

        let leases = store.list_bundle_leases().await.unwrap();
        let unowned = leases
            .iter()
            .find(|lease| lease.bundle_id == ShardId(1))
            .unwrap();
        assert_eq!(unowned.owner_node_id, None);
        assert_eq!(unowned.node_endpoint, None);

        let reacquired = store
            .try_acquire_bundle(
                ShardId(1),
                "owner-c".to_owned(),
                "127.0.0.1:7235".to_owned(),
            )
            .await
            .unwrap();
        assert!(matches!(reacquired, LeaseOutcome::Acquired { .. }));
        let leases = store.list_bundle_leases().await.unwrap();
        assert!(leases.iter().any(|lease| {
            lease.bundle_id == ShardId(1)
                && lease.owner_node_id.as_deref() == Some("owner-c")
                && lease.node_endpoint.as_deref() == Some("127.0.0.1:7235")
        }));
    }

    #[tokio::test]
    async fn relinquish_rejects_stale_epoch_and_renew_updates_endpoint() {
        let store = InMemoryStore::default();
        let acquired = store
            .try_acquire_bundle(ShardId(7), "owner".to_owned(), "127.0.0.1:7233".to_owned())
            .await
            .unwrap();
        let LeaseOutcome::Acquired { epoch } = acquired else {
            panic!("expected acquire");
        };

        let stale = store
            .relinquish_bundle(ShardId(7), "owner".to_owned(), epoch.next())
            .await
            .unwrap();
        assert!(matches!(stale, LeaseOutcome::Rejected { .. }));

        let renewed = store
            .renew_bundle(
                ShardId(7),
                "owner".to_owned(),
                epoch,
                "127.0.0.1:9000".to_owned(),
            )
            .await
            .unwrap();
        assert_eq!(renewed, LeaseOutcome::Renewed { epoch });
        let leases = store.list_bundle_leases().await.unwrap();
        assert!(leases.iter().any(|lease| {
            lease.bundle_id == ShardId(7)
                && lease.node_endpoint.as_deref() == Some("127.0.0.1:9000")
        }));
    }

    #[tokio::test]
    async fn control_repository_generation_and_budget_are_cas_protected() {
        let store = InMemoryStore::default();

        assert_eq!(
            store
                .advance_generation(GenerationCounter::ZERO)
                .await
                .unwrap(),
            GenerationAdvanceResult::Advanced(GenerationCounter(1))
        );
        assert_eq!(
            store
                .advance_generation(GenerationCounter::ZERO)
                .await
                .unwrap(),
            GenerationAdvanceResult::Conflict(GenerationCounter(1))
        );

        let allocator_id = uuid::Uuid::new_v4();
        assert_eq!(
            store
                .allocate_budget(0, allocator_id, 10.0, 100)
                .await
                .unwrap(),
            BudgetAllocationResult::Allocated { version: 1 }
        );
        assert_eq!(
            store
                .allocate_budget(0, allocator_id, 10.0, 100)
                .await
                .unwrap(),
            BudgetAllocationResult::Conflict { current_version: 1 }
        );
    }

    fn sample_state(run_key: RunKey) -> WorkflowState {
        WorkflowState {
            run_key,
            namespace_id: NamespaceId::new(),
            workflow_id: WorkflowId("workflow".into()),
            run_id: RunId::new(),
            workflow_type: WorkflowType("wf".into()),
            task_queue: TaskQueueName("queue".into()),
            deployment: None,
            build_id: None,
            status: ExecutionStatus::Running,
            transition_seq: TransitionSeq(1),
            last_event_id: 0,
            external_payload_count: 0,
            external_payload_size_bytes: 0,
            next_workflow_task_seq: LogicalTaskSeq(1),
            pending_workflow_task: Some(PendingWorkflowTask {
                task_type: tokeira_kernel::WorkflowTaskType::Normal,
                schedule_to_start_deadline: None,
                logical_seq: LogicalTaskSeq(1),
                scheduled_event_id: 1,
                scheduled_at: fixed_now(),
                started_event_id: None,
                started_at: None,
                attempt: 1,
            }),
            previous_started_event_id: 0,
            workflow_task_attempt: 1,
            workflow_task_attempts_since_last_success: 0,
            last_workflow_task_problem: None,
            sticky: None,
            pause_info: None,
            cancel_requested: false,
            wft_stamp: 0,
            memo: Memo::default(),
            search_attributes: SearchAttributes::default(),
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: Duration::seconds(10),
            retry_policy: None,
            attempt: 1,
            first_execution_run_id: None,
            original_execution_run_id: None,
            reset_run_id: None,
            parent_run_key: None,
            parent_workflow_id: None,
            parent_run_id: None,
            parent_namespace_id: None,
            parent_namespace_name: None,
            parent_initiated_event_id: 0,
            root_workflow_id: None,
            root_run_id: None,
            last_completion_result: None,
            activities: Default::default(),
            timers: Default::default(),
            children: Default::default(),
            pending_external_signals: Default::default(),
            pending_external_cancels: Default::default(),
            pending_updates: Default::default(),
            admitted_updates: Default::default(),
            pending_nexus_operations: Default::default(),
            versioning_info: None,
            worker_deployment_name: None,
            completion_callbacks: Vec::new(),
            user_metadata: None,
            links: Vec::new(),
            workflow_start_delay: None,
            priority: None,
            started_at: fixed_now(),
            first_run_started_at: None,
            closed_at: None,
            close_result: None,
            close_failure: None,
            request_id_infos: std::collections::BTreeMap::new(),
            buffered_events: Vec::new(),
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

    #[test]
    fn projection_derives_pinned_worker_deployment_search_attributes() {
        let run_key = RunKey::new();
        let mut state = sample_state(run_key);
        state.versioning_info = Some(WorkflowVersioningInfo {
            versioning_override: Some(VersioningOverride::Pinned {
                version: WorkerDeploymentVersionRef {
                    deployment_name: "deployment".to_owned(),
                    build_id: "build-id".to_owned(),
                },
            }),
            ..WorkflowVersioningInfo::default()
        });

        let projection = workflow_projection_context(&state).unwrap();
        assert_eq!(
            projection
                .search_attributes
                .0
                .get("TemporalWorkflowVersioningBehavior"),
            Some(&SearchAttrValue::Keyword("Pinned".to_owned()))
        );
        assert_eq!(
            projection
                .search_attributes
                .0
                .get("TemporalWorkerDeployment"),
            Some(&SearchAttrValue::Keyword("deployment".to_owned()))
        );
        assert_eq!(
            projection
                .search_attributes
                .0
                .get("TemporalWorkerDeploymentVersion"),
            Some(&SearchAttrValue::Keyword("deployment:build-id".to_owned()))
        );
        assert!(
            state
                .search_attributes
                .0
                .get("TemporalWorkflowVersioningBehavior")
                .is_none(),
            "server-managed visibility attributes must not mutate authoritative history state"
        );
    }

    // The completion-callback sweep query must surface BOTH `Scheduled` (first delivery
    // lost to a crash) and `BackingOff` (awaiting retry) callbacks, and never a terminal
    // one — so the scanner index is fully rebuilt on shard takeover.
    #[tokio::test]
    async fn list_pending_completion_callbacks_includes_scheduled_and_backing_off() {
        use tokeira_kernel::{CallbackSpec, CallbackState, CallbackTrigger, CompletionCallback};
        let store = InMemoryStore::with_shard_count(1);
        let run_key = RunKey::new();
        let callback = |state: CallbackState| CompletionCallback {
            spec: CallbackSpec::Nexus {
                url: "temporal://system".into(),
                header: BTreeMap::new(),
            },
            links: Vec::new(),
            trigger: CallbackTrigger::WorkflowClosed,
            registration_time: None,
            state,
            attempt: 0,
            last_attempt_failure: None,
            next_attempt_at: None,
        };
        let mut transition = start_transition(run_key);
        transition.next_state.status = ExecutionStatus::Completed;
        transition.next_state.closed_at = Some(fixed_now());
        transition.next_state.pending_workflow_task = None;
        transition.next_state.completion_callbacks = vec![
            callback(CallbackState::Scheduled),
            callback(CallbackState::BackingOff),
            callback(CallbackState::Succeeded),
        ];
        store
            .commit_transition(run_key, transition, ShardEpoch::ZERO)
            .await
            .unwrap();

        let entries = store
            .list_runs_with_pending_completion_callbacks_for_shard(ShardId(0), usize::MAX)
            .await
            .unwrap();
        let indices: BTreeSet<usize> = entries
            .iter()
            .filter(|entry| entry.run_key == run_key)
            .map(|entry| entry.callback_index)
            .collect();
        assert_eq!(
            indices,
            BTreeSet::from([0, 1]),
            "Scheduled + BackingOff are pending; Succeeded is terminal"
        );
    }

    fn activity_state(activity_id: &str) -> tokeira_kernel::ActivityState {
        tokeira_kernel::ActivityState {
            cancel_requested: false,
            started_identity: None,
            retry_last_worker_identity: None,
            activity_id: activity_id.into(),
            activity_type: "activity-type".into(),
            schedule_event_id: 7,
            task_queue: TaskQueueName("activity-q".into()),
            deployment: None,
            build_id: None,
            input: tokeira_types::Payloads::default(),
            header: None,
            last_failure: None,
            heartbeat_details: None,
            attempt: 2,
            retry_policy: None,
            schedule_to_close_timeout: Some(Duration::seconds(30)),
            schedule_to_start_timeout: Some(Duration::seconds(10)),
            start_to_close_timeout: Some(Duration::seconds(20)),
            heartbeat_timeout: Some(Duration::seconds(5)),
            scheduled_at: fixed_now(),
            current_attempt_scheduled_at: None,
            started_at: None,
            started_event_id: None,
            pause_info: None,
            stamp: 0,
        }
    }

    #[tokio::test]
    async fn started_activity_upsert_removes_dispatch_entry() {
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
        transition
            .activity_ops
            .push(ActivityOp::Upsert(activity_state("activity-1")));
        transition
            .dispatch_ops
            .push(DispatchOp::EnqueueActivityTask {
                queue: queue.clone(),
                activity_id: "activity-1".to_owned(),
                input: tokeira_types::Payloads::default(),
                schedule_event_id: 7,
                attempt: 1,
                dispatch_revision: 0,
                schedule_to_close_timeout: None,
                schedule_to_start_timeout: None,
                start_to_close_timeout: None,
                heartbeat_timeout: None,
            });
        store
            .commit_transition(run_key, transition, ShardEpoch::ZERO)
            .await
            .unwrap();

        let mut started = activity_state("activity-1");
        started.started_at = Some(fixed_now());
        started.started_event_id = Some(9);
        let mut started_transition = start_transition(run_key);
        started_transition.expected_seq = TransitionSeq(1);
        started_transition.next_state.transition_seq = TransitionSeq(2);
        started_transition.next_state.namespace_id = queue.namespace_id;
        started_transition
            .activity_ops
            .push(ActivityOp::Upsert(started));
        store
            .commit_transition(run_key, started_transition, ShardEpoch::ZERO)
            .await
            .unwrap();

        let tasks = store
            .list_dispatchable_activity_tasks(&queue, 10)
            .await
            .unwrap();
        assert!(tasks.is_empty());
    }

    #[tokio::test]
    async fn still_dispatchable_activity_upsert_updates_existing_dispatch_entry() {
        let store = InMemoryStore::default();
        let run_key = RunKey::new();
        let mut transition = start_transition(run_key);
        let old_queue = QueueKey {
            namespace_id: transition.next_state.namespace_id,
            task_queue: TaskQueueName("old-queue".into()),
            task_kind: TaskKind::Activity,
            deployment: None,
            build_id: None,
        };
        transition
            .activity_ops
            .push(ActivityOp::Upsert(activity_state("activity-1")));
        transition
            .dispatch_ops
            .push(DispatchOp::EnqueueActivityTask {
                queue: old_queue.clone(),
                activity_id: "activity-1".to_owned(),
                input: tokeira_types::Payloads::default(),
                schedule_event_id: 7,
                attempt: 1,
                dispatch_revision: 0,
                schedule_to_close_timeout: None,
                schedule_to_start_timeout: None,
                start_to_close_timeout: None,
                heartbeat_timeout: None,
            });
        store
            .commit_transition(run_key, transition, ShardEpoch::ZERO)
            .await
            .unwrap();

        let mut updated = activity_state("activity-1");
        updated.task_queue = TaskQueueName("new-queue".into());
        updated.attempt = 2;
        let mut update_transition = start_transition(run_key);
        update_transition.expected_seq = TransitionSeq(1);
        update_transition.next_state.transition_seq = TransitionSeq(2);
        update_transition.next_state.namespace_id = old_queue.namespace_id;
        update_transition
            .activity_ops
            .push(ActivityOp::Upsert(updated));
        store
            .commit_transition(run_key, update_transition, ShardEpoch::ZERO)
            .await
            .unwrap();

        let new_queue = QueueKey {
            task_queue: TaskQueueName("new-queue".into()),
            ..old_queue.clone()
        };
        assert!(
            store
                .list_dispatchable_activity_tasks(&old_queue, 10)
                .await
                .unwrap()
                .is_empty()
        );
        let tasks = store
            .list_dispatchable_activity_tasks(&new_queue, 10)
            .await
            .unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].attempt, 2);
    }

    #[tokio::test]
    async fn paused_activity_upsert_removes_dispatch_entry() {
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
        transition
            .activity_ops
            .push(ActivityOp::Upsert(activity_state("activity-1")));
        transition
            .dispatch_ops
            .push(DispatchOp::EnqueueActivityTask {
                queue: queue.clone(),
                activity_id: "activity-1".to_owned(),
                input: tokeira_types::Payloads::default(),
                schedule_event_id: 7,
                attempt: 1,
                dispatch_revision: 0,
                schedule_to_close_timeout: None,
                schedule_to_start_timeout: None,
                start_to_close_timeout: None,
                heartbeat_timeout: None,
            });
        store
            .commit_transition(run_key, transition, ShardEpoch::ZERO)
            .await
            .unwrap();

        let mut paused = activity_state("activity-1");
        paused.pause_info = Some(ActivityPauseInfo {
            pause_time: fixed_now(),
            identity: "tester".to_owned(),
            reason: "maintenance".to_owned(),
        });
        let mut pause_transition = start_transition(run_key);
        pause_transition.expected_seq = TransitionSeq(1);
        pause_transition.next_state.transition_seq = TransitionSeq(2);
        pause_transition.next_state.namespace_id = queue.namespace_id;
        pause_transition
            .activity_ops
            .push(ActivityOp::Upsert(paused));
        store
            .commit_transition(run_key, pause_transition, ShardEpoch::ZERO)
            .await
            .unwrap();

        let tasks = store
            .list_dispatchable_activity_tasks(&queue, 10)
            .await
            .unwrap();
        assert!(tasks.is_empty());
    }

    #[tokio::test]
    async fn workflow_pause_removes_activity_dispatch_entries_for_run() {
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
        for activity_id in ["activity-1", "activity-2"] {
            transition
                .activity_ops
                .push(ActivityOp::Upsert(activity_state(activity_id)));
            transition
                .dispatch_ops
                .push(DispatchOp::EnqueueActivityTask {
                    queue: queue.clone(),
                    activity_id: activity_id.to_owned(),
                    input: tokeira_types::Payloads::default(),
                    schedule_event_id: 7,
                    attempt: 1,
                    dispatch_revision: 0,
                    schedule_to_close_timeout: None,
                    schedule_to_start_timeout: None,
                    start_to_close_timeout: None,
                    heartbeat_timeout: None,
                });
        }
        store
            .commit_transition(run_key, transition, ShardEpoch::ZERO)
            .await
            .unwrap();

        let mut pause_transition = start_transition(run_key);
        pause_transition.expected_seq = TransitionSeq(1);
        pause_transition.next_state.transition_seq = TransitionSeq(2);
        pause_transition.next_state.namespace_id = queue.namespace_id;
        pause_transition.next_state.status = ExecutionStatus::Paused;
        pause_transition.next_state.pause_info = Some(PauseInfo {
            pause_time: fixed_now(),
            identity: "tester".to_owned(),
            reason: "maintenance".to_owned(),
            request_id: "pause-1".to_owned(),
        });
        store
            .commit_transition(run_key, pause_transition, ShardEpoch::ZERO)
            .await
            .unwrap();

        let tasks = store
            .list_dispatchable_activity_tasks(&queue, 10)
            .await
            .unwrap();
        assert!(tasks.is_empty());
    }

    #[tokio::test]
    async fn unpaused_activity_reenqueue_restores_dispatch_entry() {
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
        let mut paused = activity_state("activity-1");
        paused.pause_info = Some(ActivityPauseInfo {
            pause_time: fixed_now(),
            identity: "tester".to_owned(),
            reason: "maintenance".to_owned(),
        });
        transition.activity_ops.push(ActivityOp::Upsert(paused));
        store
            .commit_transition(run_key, transition, ShardEpoch::ZERO)
            .await
            .unwrap();

        let mut unpause_transition = start_transition(run_key);
        unpause_transition.expected_seq = TransitionSeq(1);
        unpause_transition.next_state.transition_seq = TransitionSeq(2);
        unpause_transition.next_state.namespace_id = queue.namespace_id;
        unpause_transition
            .activity_ops
            .push(ActivityOp::Upsert(activity_state("activity-1")));
        unpause_transition
            .dispatch_ops
            .push(DispatchOp::EnqueueActivityTask {
                queue: queue.clone(),
                activity_id: "activity-1".to_owned(),
                input: tokeira_types::Payloads::default(),
                schedule_event_id: 7,
                attempt: 1,
                dispatch_revision: 0,
                schedule_to_close_timeout: None,
                schedule_to_start_timeout: None,
                start_to_close_timeout: None,
                heartbeat_timeout: None,
            });
        store
            .commit_transition(run_key, unpause_transition, ShardEpoch::ZERO)
            .await
            .unwrap();

        let tasks = store
            .list_dispatchable_activity_tasks(&queue, 10)
            .await
            .unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].activity_id, "activity-1");
    }

    fn timer_state(timer_id: &str, fire_at: OffsetDateTime) -> tokeira_kernel::TimerState {
        tokeira_kernel::TimerState {
            timer_id: timer_id.into(),
            started_event_id: 11,
            fire_at,
        }
    }

    fn history_event(
        event_id: i64,
        happened_at: OffsetDateTime,
        kind: HistoryEventKind,
    ) -> HistoryEvent {
        HistoryEvent {
            event_id,
            happened_at,
            kind,
        }
    }

    async fn seed_base_run(
        store: &InMemoryStore,
        run_key: RunKey,
        state: WorkflowState,
        history: Vec<HistoryEvent>,
    ) {
        let mut inner = store.inner.lock().await;
        inner.runs.insert(run_key, state.clone());
        inner.history.insert(run_key, history);
        inner.execution_index.insert(
            (
                state.namespace_id,
                state.workflow_id.0.clone(),
                state.run_id,
            ),
            run_key,
        );
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
                    dispatch_revision: 0,
                    schedule_to_close_timeout: Some(Duration::seconds(30)),
                    schedule_to_start_timeout: Some(Duration::seconds(10)),
                    start_to_close_timeout: Some(Duration::seconds(20)),
                    heartbeat_timeout: Some(Duration::seconds(5)),
                });

                let result = store.commit_transition(run_key, transition, ShardEpoch::ZERO).await.unwrap();
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
                    dispatch_revision: 0,
                    schedule_to_close_timeout: None,
                    schedule_to_start_timeout: None,
                    start_to_close_timeout: None,
                    heartbeat_timeout: None,
                });
                transition.activity_ops.push(ActivityOp::Upsert(activity_state(&activity_id)));
                let _ = store.commit_transition(run_key, transition, ShardEpoch::ZERO).await.unwrap();

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
                let _ = store.commit_transition(run_key, delete_transition, ShardEpoch::ZERO).await.unwrap();

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
                    dispatch_revision: 0,
                    schedule_to_close_timeout: None,
                    schedule_to_start_timeout: None,
                    start_to_close_timeout: None,
                    heartbeat_timeout: None,
                });
                first.activity_ops.push(ActivityOp::Upsert(activity_state(&activity_id)));
                let _ = store.commit_transition(run_key, first, ShardEpoch::ZERO).await.unwrap();
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
                    dispatch_revision: 0,
                    schedule_to_close_timeout: None,
                    schedule_to_start_timeout: None,
                    start_to_close_timeout: None,
                    heartbeat_timeout: None,
                });
                let result = store.commit_transition(run_key, conflict, ShardEpoch::ZERO).await.unwrap();
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
                        dispatch_revision: 0,
                        schedule_to_close_timeout: None,
                        schedule_to_start_timeout: None,
                        start_to_close_timeout: None,
                        heartbeat_timeout: None,
                    });
                    let _ = store.commit_transition(run_key, transition, ShardEpoch::ZERO).await.unwrap();
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
                        payload: if idx % 2 == 0 {
                            crate::api::BacklogPayload::Workflow {
                                logical_seq: LogicalTaskSeq(idx as u64 + 1),
                            }
                        } else {
                            crate::api::BacklogPayload::Activity {
                                activity_id: format!("a{idx}"),
                                input: tokeira_types::Payloads::default(),
                                schedule_event_id: idx as i64,
                                attempt: 1,
                                dispatch_revision: 0,
                            }
                        },
                        scheduled_at: fixed_now(),
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
                    let result = store.commit_transition(run_key, start_transition(run_key), ShardEpoch::ZERO).await.unwrap();
                    assert!(matches!(result, CommitResult::Conflict { .. }));
                }
                let result = store.commit_transition(run_key, start_transition(run_key), ShardEpoch::ZERO).await.unwrap();
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
                let _ = store.commit_transition(run_key_1, t1, ShardEpoch::ZERO).await.unwrap();

                let run_key_2 = RunKey::new();
                let mut t2 = start_transition(run_key_2);
                t2.next_state.namespace_id = namespace_id;
                t2.next_state.workflow_id = workflow_id;
                let result = store.commit_transition(run_key_2, t2, ShardEpoch::ZERO).await.unwrap();
                assert!(matches!(result, CommitResult::CurrentExecutionConflict { .. }));
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
                let _ = store.commit_transition(run_key, upsert, ShardEpoch::ZERO).await.unwrap();
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
                let _ = store.commit_transition(run_key, delete, ShardEpoch::ZERO).await.unwrap();
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
                let _ = store.commit_transition(run_key_1, open, ShardEpoch::ZERO).await.unwrap();

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
                let _ = store.commit_transition(run_key_1, close, ShardEpoch::ZERO).await.unwrap();

                let run_key_2 = RunKey::new();
                let mut reopen = start_transition(run_key_2);
                reopen.next_state.namespace_id = namespace_id;
                reopen.next_state.workflow_id = workflow_id;
                let result = store.commit_transition(run_key_2, reopen, ShardEpoch::ZERO).await.unwrap();
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
                let _ = store.commit_transition(run_key, t1, ShardEpoch::ZERO).await.unwrap();

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
                            payload: crate::api::BacklogPayload::Workflow {
                                logical_seq: LogicalTaskSeq::ONE,
                            },
                            scheduled_at: fixed_now(),
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
        let _ = store
            .commit_transition(run_key_1, t1, ShardEpoch::ZERO)
            .await
            .unwrap();

        let run_key_2 = RunKey::new();
        let mut t2 = start_transition(run_key_2);
        t2.next_state.namespace_id = namespace_id;
        t2.next_state.workflow_id = workflow_id;
        let result = store
            .commit_transition(run_key_2, t2, ShardEpoch::ZERO)
            .await
            .unwrap();
        assert!(matches!(
            result,
            CommitResult::CurrentExecutionConflict { .. }
        ));
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
        let _ = store
            .commit_transition(run_key_1, open, ShardEpoch::ZERO)
            .await
            .unwrap();

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
        let _ = store
            .commit_transition(run_key_1, close, ShardEpoch::ZERO)
            .await
            .unwrap();

        let run_key_2 = RunKey::new();
        let mut reopen = start_transition(run_key_2);
        reopen.next_state.namespace_id = namespace_id;
        reopen.next_state.workflow_id = workflow_id;
        let result = store
            .commit_transition(run_key_2, reopen, ShardEpoch::ZERO)
            .await
            .unwrap();
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
        let _ = store
            .commit_transition(run_key, transition, ShardEpoch::ZERO)
            .await
            .unwrap();
        let due = store.list_due_timers(fixed_now(), 10).await.unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].timer_id, "timer-1");
    }

    #[tokio::test]
    async fn empty_sweep_and_drain_return_empty() {
        let store = InMemoryStore::default();
        assert!(
            store
                .list_dispatchable_activity_tasks(&sample_queue(TaskKind::Activity), 10,)
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
            .commit_transition(run_key, start_transition(run_key), ShardEpoch::ZERO)
            .await
            .unwrap();
        let second = store
            .commit_transition(run_key, start_transition(run_key), ShardEpoch::ZERO)
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
                    payload: crate::api::BacklogPayload::Workflow {
                        logical_seq: LogicalTaskSeq::ONE,
                    },
                    scheduled_at: fixed_now(),
                    insertion_seq: 999,
                },
                BacklogEntry {
                    run_key: RunKey::new(),
                    queue: queue.clone(),
                    payload: crate::api::BacklogPayload::Activity {
                        activity_id: "a1".into(),
                        input: tokeira_types::Payloads::default(),
                        schedule_event_id: 7,
                        attempt: 1,
                        dispatch_revision: 0,
                    },
                    scheduled_at: fixed_now(),
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
                speculative: false,
                queue: sample_queue(TaskKind::Workflow),
                logical_seq: LogicalTaskSeq(1),
                sticky_preferred: None,
                normal_task_queue: None,
            });
        transition
            .dispatch_ops
            .push(DispatchOp::EnqueueActivityTask {
                queue: sample_queue(TaskKind::Activity),
                activity_id: "a1".into(),
                input: tokeira_types::Payloads::default(),
                schedule_event_id: 3,
                attempt: 1,
                dispatch_revision: 0,
                schedule_to_close_timeout: None,
                schedule_to_start_timeout: None,
                start_to_close_timeout: None,
                heartbeat_timeout: None,
            });
        let _ = store
            .commit_transition(run_key, transition, ShardEpoch::ZERO)
            .await
            .unwrap();
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
        let run_id = first.next_state.run_id;
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
            dispatch_revision: 0,
            schedule_to_close_timeout: None,
            schedule_to_start_timeout: None,
            start_to_close_timeout: None,
            heartbeat_timeout: None,
        });
        first
            .activity_ops
            .push(ActivityOp::Upsert(activity_state("a1")));
        let _ = store
            .commit_transition(run_key, first, ShardEpoch::ZERO)
            .await
            .unwrap();

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
        // Same RUN retrying the same request id — dedupe is run-scoped
        // (a different run reusing the id is fresh, per v1.31.0's per-run
        // request-id space).
        duplicate.next_state.run_id = run_id;
        duplicate.next_state.transition_seq = TransitionSeq(2);
        duplicate.request_dedupe_ops.push(RequestDedupeOp {
            request_id: RequestId("req-1".into()),
        });
        let result = store
            .commit_transition(run_key, duplicate, ShardEpoch::ZERO)
            .await
            .unwrap();
        assert!(matches!(result, CommitResult::Duplicate));

        let tasks_after = store
            .list_dispatchable_activity_tasks(&sample_queue(TaskKind::Activity), 10)
            .await
            .unwrap();
        assert_eq!(tasks_before, tasks_after);
    }

    #[tokio::test]
    async fn materialize_reset_successor_replays_prefix_state() {
        let store = InMemoryStore::default();
        let base_run_key = RunKey::new();
        let successor_run_id = RunId::new();
        let mut base_state = sample_state(base_run_key);
        let successor_run_key = RunKey::derive(
            base_state.namespace_id,
            &base_state.workflow_id,
            successor_run_id,
        );
        base_state.status = ExecutionStatus::Terminated;
        base_state.closed_at = Some(fixed_now());
        base_state.pending_workflow_task = None;
        base_state.transition_seq = TransitionSeq(7);

        let history = vec![
            history_event(
                1,
                fixed_now(),
                HistoryEventKind::WorkflowExecutionStarted {
                    initiator: None,
                    workflow_type: base_state.workflow_type.clone(),
                    task_queue: base_state.task_queue.clone(),
                    input: tokeira_types::Payloads::default(),
                    memo: base_state.memo.clone(),
                    search_attributes: base_state.search_attributes.clone(),
                    request_id: "start".into(),
                    header: None,
                    workflow_start_delay: base_state.workflow_start_delay,
                    completion_callbacks: base_state.completion_callbacks.clone(),
                    user_metadata: base_state.user_metadata.clone(),
                    links: base_state.links.clone(),
                    identity: "starter".into(),
                    continued_execution_run_id: None,
                    first_execution_run_id: base_state.first_execution_run_id,
                    retry_policy: base_state.retry_policy.clone(),
                    attempt: base_state.attempt,
                    workflow_execution_timeout: base_state.workflow_execution_timeout,
                    workflow_run_timeout: base_state.workflow_run_timeout,
                    workflow_task_timeout: base_state.workflow_task_timeout,
                    parent_workflow_id: base_state.parent_workflow_id.clone(),
                    parent_run_id: base_state.parent_run_id,
                    parent_namespace_id: base_state.parent_namespace_id,
                    parent_namespace_name: None,
                    parent_initiated_event_id: base_state.parent_initiated_event_id,
                    root_workflow_id: base_state.root_workflow_id.clone(),
                    root_run_id: base_state.root_run_id,
                    original_execution_run_id: base_state.original_execution_run_id,
                    continued_failure: None,
                    cron_schedule: None,
                    last_completion_result: base_state.last_completion_result.clone(),
                    versioning_info: base_state.versioning_info.clone(),
                    worker_deployment_name: base_state.worker_deployment_name.clone(),
                    priority: base_state.priority.clone(),
                },
            ),
            history_event(
                2,
                fixed_now(),
                HistoryEventKind::WorkflowTaskScheduled {
                    logical_seq: LogicalTaskSeq::ONE,
                    task_queue: base_state.task_queue.clone(),
                    workflow_task_timeout: base_state.workflow_task_timeout,
                    attempt: 1,
                },
            ),
            history_event(
                3,
                fixed_now(),
                HistoryEventKind::WorkflowTaskStarted {
                    logical_seq: LogicalTaskSeq::ONE,
                    scheduled_event_id: 2,
                    attempt: 1,
                    identity: WorkerIdentity("worker".into()),
                    request_id: "wft-start".into(),
                    history_size_bytes: 0,
                    suggest_continue_as_new: false,
                },
            ),
            history_event(
                4,
                fixed_now(),
                HistoryEventKind::WorkflowTaskCompleted {
                    logical_seq: LogicalTaskSeq::ONE,
                    scheduled_event_id: 2,
                    started_event_id: 3,
                    identity: WorkerIdentity("worker".into()),
                    sdk_metadata: None,
                    metering_metadata: None,
                    worker_version: None,
                    versioning_behavior: tokeira_kernel::VersioningBehavior::Unspecified,
                    deployment_version: None,
                    worker_deployment_name: None,
                },
            ),
            history_event(
                5,
                fixed_now(),
                HistoryEventKind::ActivityTaskScheduled {
                    activity_id: "a1".into(),
                    activity_type: "activity".into(),
                    task_queue: TaskQueueName("activity-q".into()),
                    input: tokeira_types::Payloads::default(),
                    header: None,
                    workflow_task_completed_event_id: 4,
                    retry_policy: None,
                    schedule_to_close_timeout: Some(Duration::seconds(30)),
                    schedule_to_start_timeout: Some(Duration::seconds(10)),
                    start_to_close_timeout: Some(Duration::seconds(20)),
                    heartbeat_timeout: Some(Duration::seconds(5)),
                },
            ),
            history_event(
                6,
                fixed_now(),
                HistoryEventKind::TimerStarted {
                    timer_id: "t1".into(),
                    fire_at: fixed_now() + Duration::seconds(30),
                    workflow_task_completed_event_id: 4,
                },
            ),
        ];
        seed_base_run(&store, base_run_key, base_state.clone(), history).await;

        // Fork at the WFT-finish event 4: the successor keeps only events
        // 1..3 (v1.31.0 rebuilds to `WorkflowTaskFinishEventId - 1`,
        // resetworkflow/api.go:119) — the reset WFT is still STARTED and the
        // activity/timer it commanded (events 5/6) never happened on this
        // branch.
        RunRepository::materialize_reset_successor(&store, base_run_key, 4, successor_run_id)
            .await
            .unwrap();

        let LoadedRun::Existing(successor) = RunRepository::load_run(&store, successor_run_key)
            .await
            .unwrap()
        else {
            panic!("expected successor run to exist");
        };

        assert_eq!(successor.run_id, successor_run_id);
        assert_eq!(successor.transition_seq, TransitionSeq::ZERO);
        assert_eq!(successor.last_event_id, 3);
        let pending = successor
            .pending_workflow_task
            .as_ref()
            .expect("the reset WFT is still started on the successor branch");
        assert_eq!(pending.scheduled_event_id, 2);
        assert_eq!(pending.started_event_id, Some(3));
        assert!(successor.activities.is_empty());
        assert!(successor.timers.is_empty());
    }

    #[tokio::test]
    async fn materialize_reset_successor_rejects_invalid_fork_event_id() {
        let store = InMemoryStore::default();
        let base_run_key = RunKey::new();
        let mut base_state = sample_state(base_run_key);
        base_state.status = ExecutionStatus::Terminated;
        base_state.closed_at = Some(fixed_now());
        seed_base_run(
            &store,
            base_run_key,
            base_state.clone(),
            vec![history_event(
                1,
                fixed_now(),
                HistoryEventKind::WorkflowExecutionStarted {
                    initiator: None,
                    workflow_type: base_state.workflow_type.clone(),
                    task_queue: base_state.task_queue.clone(),
                    input: tokeira_types::Payloads::default(),
                    memo: base_state.memo.clone(),
                    search_attributes: base_state.search_attributes.clone(),
                    request_id: "start".into(),
                    header: None,
                    workflow_start_delay: base_state.workflow_start_delay,
                    completion_callbacks: base_state.completion_callbacks.clone(),
                    user_metadata: base_state.user_metadata.clone(),
                    links: base_state.links.clone(),
                    identity: "starter".into(),
                    continued_execution_run_id: None,
                    first_execution_run_id: base_state.first_execution_run_id,
                    retry_policy: base_state.retry_policy.clone(),
                    attempt: base_state.attempt,
                    workflow_execution_timeout: base_state.workflow_execution_timeout,
                    workflow_run_timeout: base_state.workflow_run_timeout,
                    workflow_task_timeout: base_state.workflow_task_timeout,
                    parent_workflow_id: base_state.parent_workflow_id.clone(),
                    parent_run_id: base_state.parent_run_id,
                    parent_namespace_id: base_state.parent_namespace_id,
                    parent_namespace_name: None,
                    parent_initiated_event_id: base_state.parent_initiated_event_id,
                    root_workflow_id: base_state.root_workflow_id.clone(),
                    root_run_id: base_state.root_run_id,
                    original_execution_run_id: base_state.original_execution_run_id,
                    continued_failure: None,
                    cron_schedule: None,
                    last_completion_result: base_state.last_completion_result.clone(),
                    versioning_info: base_state.versioning_info.clone(),
                    worker_deployment_name: base_state.worker_deployment_name.clone(),
                    priority: base_state.priority.clone(),
                },
            )],
        )
        .await;

        let err =
            RunRepository::materialize_reset_successor(&store, base_run_key, 99, RunId::new())
                .await
                .unwrap_err();

        assert!(err.to_string().contains("outside committed history"));
    }

    #[tokio::test]
    async fn materialize_reset_successor_is_durably_queryable() {
        let store = InMemoryStore::default();
        let base_run_key = RunKey::new();
        let successor_run_id = RunId::new();
        let mut base_state = sample_state(base_run_key);
        let successor_run_key = RunKey::derive(
            base_state.namespace_id,
            &base_state.workflow_id,
            successor_run_id,
        );
        base_state.status = ExecutionStatus::Terminated;
        base_state.closed_at = Some(fixed_now());
        base_state.pending_workflow_task = None;
        let base_namespace = base_state.namespace_id;
        let base_workflow_id = base_state.workflow_id.clone();
        seed_base_run(
            &store,
            base_run_key,
            base_state.clone(),
            vec![
                history_event(
                    1,
                    fixed_now(),
                    HistoryEventKind::WorkflowExecutionStarted {
                        initiator: None,
                        workflow_type: base_state.workflow_type.clone(),
                        task_queue: base_state.task_queue.clone(),
                        input: tokeira_types::Payloads::default(),
                        memo: base_state.memo.clone(),
                        search_attributes: base_state.search_attributes.clone(),
                        request_id: "start".into(),
                        header: None,
                        workflow_start_delay: base_state.workflow_start_delay,
                        completion_callbacks: base_state.completion_callbacks.clone(),
                        user_metadata: base_state.user_metadata.clone(),
                        links: base_state.links.clone(),
                        identity: "starter".into(),
                        continued_execution_run_id: None,
                        first_execution_run_id: base_state.first_execution_run_id,
                        retry_policy: base_state.retry_policy.clone(),
                        attempt: base_state.attempt,
                        workflow_execution_timeout: base_state.workflow_execution_timeout,
                        workflow_run_timeout: base_state.workflow_run_timeout,
                        workflow_task_timeout: base_state.workflow_task_timeout,
                        parent_workflow_id: base_state.parent_workflow_id.clone(),
                        parent_run_id: base_state.parent_run_id,
                        parent_namespace_id: base_state.parent_namespace_id,
                        parent_namespace_name: None,
                        parent_initiated_event_id: base_state.parent_initiated_event_id,
                        root_workflow_id: base_state.root_workflow_id.clone(),
                        root_run_id: base_state.root_run_id,
                        original_execution_run_id: base_state.original_execution_run_id,
                        continued_failure: None,
                        cron_schedule: None,
                        last_completion_result: base_state.last_completion_result.clone(),
                        versioning_info: base_state.versioning_info.clone(),
                        worker_deployment_name: base_state.worker_deployment_name.clone(),
                        priority: base_state.priority.clone(),
                    },
                ),
                history_event(
                    2,
                    fixed_now(),
                    HistoryEventKind::WorkflowTaskScheduled {
                        logical_seq: LogicalTaskSeq::ONE,
                        task_queue: base_state.task_queue.clone(),
                        workflow_task_timeout: base_state.workflow_task_timeout,
                        attempt: 1,
                    },
                ),
            ],
        )
        .await;

        RunRepository::materialize_reset_successor(&store, base_run_key, 2, successor_run_id)
            .await
            .unwrap();

        let resolved = RunRepository::resolve_execution(
            &store,
            &ExecutionRef {
                namespace_id: base_namespace,
                workflow_id: base_workflow_id,
                run_id: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(resolved, Some(successor_run_key));

        // The cut is EXCLUSIVE of the fork event (v1.31.0 rebuilds to
        // `WorkflowTaskFinishEventId - 1`): only event 1 survives.
        let history = RunRepository::read_history(&store, successor_run_key, 0, 10)
            .await
            .unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].event_id, 1);
    }

    proptest! {
        #[test]
        fn property_reset_successor_key_consistency(
            namespace in any::<u128>(),
            workflow in "[a-z0-9-]{1,64}",
            successor in any::<u128>(),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let store = InMemoryStore::default();
                let base_run_key = RunKey::new();
                let successor_run_id = RunId(uuid::Uuid::from_u128(successor));
                let mut base_state = sample_state(base_run_key);
                base_state.namespace_id = NamespaceId(uuid::Uuid::from_u128(namespace));
                base_state.workflow_id = WorkflowId(workflow);
                base_state.status = ExecutionStatus::Terminated;
                base_state.closed_at = Some(fixed_now());
                base_state.pending_workflow_task = None;
                let expected_successor_key = RunKey::derive(
                    base_state.namespace_id,
                    &base_state.workflow_id,
                    successor_run_id,
                );

                seed_base_run(
                    &store,
                    base_run_key,
                    base_state.clone(),
                    vec![history_event(
                        1,
                        fixed_now(),
                        HistoryEventKind::WorkflowExecutionStarted {
                            initiator: None,
                            workflow_type: base_state.workflow_type.clone(),
                            task_queue: base_state.task_queue.clone(),
                            input: tokeira_types::Payloads::default(),
                            memo: base_state.memo.clone(),
                            search_attributes: base_state.search_attributes.clone(),
                            request_id: "start".into(),
                    header: None,
                    workflow_start_delay: base_state.workflow_start_delay,
                    completion_callbacks: base_state.completion_callbacks.clone(),
                    user_metadata: base_state.user_metadata.clone(),
                    links: base_state.links.clone(),
                    identity: "starter".into(),
                            continued_execution_run_id: None,
                            first_execution_run_id: base_state.first_execution_run_id,
                            retry_policy: base_state.retry_policy.clone(),
                            attempt: base_state.attempt,
                            workflow_execution_timeout: base_state.workflow_execution_timeout,
                            workflow_run_timeout: base_state.workflow_run_timeout,
                            workflow_task_timeout: base_state.workflow_task_timeout,
                            parent_workflow_id: base_state.parent_workflow_id.clone(),
                            parent_run_id: base_state.parent_run_id,
                            parent_namespace_id: base_state.parent_namespace_id,
                            parent_namespace_name: None,
                            parent_initiated_event_id: base_state.parent_initiated_event_id,
                            root_workflow_id: base_state.root_workflow_id.clone(),
                            root_run_id: base_state.root_run_id,
                            original_execution_run_id: base_state.original_execution_run_id,
                            continued_failure: None,
                            cron_schedule: None,
                            last_completion_result: base_state.last_completion_result.clone(),
                    versioning_info: base_state.versioning_info.clone(),
                    worker_deployment_name: base_state.worker_deployment_name.clone(),
                    priority: base_state.priority.clone(),
                        },
                    ),
                    history_event(
                        2,
                        fixed_now(),
                        HistoryEventKind::WorkflowTaskScheduled {
                            logical_seq: LogicalTaskSeq::ONE,
                            task_queue: base_state.task_queue.clone(),
                            workflow_task_timeout: base_state.workflow_task_timeout,
                            attempt: 1,
                        },
                    )],
                )
                .await;

                // Exclusive cut: forking at event 2 keeps only the start event.
                RunRepository::materialize_reset_successor(
                    &store,
                    base_run_key,
                    2,
                    successor_run_id,
                )
                .await
                .unwrap();

                let loaded = RunRepository::load_run(&store, expected_successor_key)
                    .await
                    .unwrap();
                prop_assert!(matches!(loaded, LoadedRun::Existing(_)));
                Ok(())
            })?;
        }
    }

    // ── Property 17: Shard-filtered query correctness ──
    // Feature: runtime-sweeper-recovery
    // **Validates: Requirements 14.3, 14.4, 14.5, 14.6**

    // ── Property 2: Epoch fencing rejects stale commits ─
    // Feature: runtime-sweeper-recovery
    // **Validates: Requirements 1.5**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn property_bundle_ownership_consistency(
            bundle_count in 1u32..16,
            owner_seed in any::<u64>(),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                let store = InMemoryStore::default();
                let owner = format!("owner-{owner_seed}");
                let endpoint = "127.0.0.1:7233".to_owned();
                for bundle in 0..bundle_count {
                    let outcome = store
                        .try_acquire_bundle(ShardId(bundle), owner.clone(), endpoint.clone())
                        .await
                        .unwrap();
                    let acquired = matches!(outcome, LeaseOutcome::Acquired { .. });
                    prop_assert!(acquired);
                }
                let leases = store.list_bundle_leases().await.unwrap();
                prop_assert_eq!(leases.len(), bundle_count as usize);
                for lease in leases {
                    prop_assert_eq!(lease.owner_node_id.as_deref(), Some(owner.as_str()));
                    prop_assert_eq!(lease.node_endpoint.as_deref(), Some(endpoint.as_str()));
                    prop_assert!(lease.epoch.0 > 0);
                }
                Ok(())
            })?;
        }

        #[test]
        fn epoch_fencing_rejects_stale_commits(
            stale_epoch in 2u64..100,
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let store =
                    InMemoryStore::with_shard_count(1);
                let shard_id = ShardId(0);
                let current_epoch = ShardEpoch(1);

                // Acquire a lease so the store knows
                // the current epoch.
                let outcome = store
                    .try_acquire_bundle(
                        shard_id,
                        "owner".into(),
                        "127.0.0.1:7233".into(),
                    )
                    .await
                    .unwrap();
                assert!(matches!(
                    outcome,
                    LeaseOutcome::Acquired { .. }
                ));

                let run_key = RunKey::new();
                // First commit with correct epoch
                let t = start_transition(run_key);
                let result = store
                    .commit_transition(
                        run_key,
                        t,
                        current_epoch,
                    )
                    .await
                    .unwrap();
                assert!(matches!(
                    result,
                    CommitResult::Applied { .. }
                ));

                // Attempt commit with stale epoch
                let stale = ShardEpoch(stale_epoch);
                if stale != current_epoch {
                    let mut t2 = Transition {
                        expected_seq: TransitionSeq(1),
                        next_state: sample_state(run_key),
                        history_events: Default::default(),
                        request_dedupe_ops:
                            Default::default(),
                        activity_ops: Default::default(),
                        timer_ops: Default::default(),
                        dispatch_ops: Default::default(),
                        projection_ops: Default::default(),
                    };
                    t2.next_state.transition_seq =
                        TransitionSeq(2);
                    let result = store
                        .commit_transition(
                            run_key,
                            t2,
                            stale,
                        )
                        .await
                        .unwrap();
                    assert!(matches!(
                        result,
                        CommitResult::Conflict { .. }
                    ));
                }
            });
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn property_shard_filtered_query_correctness(
            run_count in 2usize..6usize,
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let shard_count: u32 = 4;
                let store =
                    InMemoryStore::with_shard_count(shard_count);
                let ns = NamespaceId::new();
                let mut run_shards: Vec<(RunKey, ShardId)> =
                    Vec::new();

                for idx in 0..run_count {
                    let run_key = RunKey::new();
                    let shard_id = ShardId(
                        (run_key.0.as_u128() as u32) % shard_count,
                    );
                    run_shards.push((run_key, shard_id));

                    let mut t = start_transition(run_key);
                    t.next_state.namespace_id = ns;
                    t.next_state.workflow_id =
                        WorkflowId(format!("wf-{idx}"));
                    t.next_state.workflow_execution_timeout =
                        Some(Duration::minutes(5));
                    t.next_state.workflow_run_timeout =
                        Some(Duration::minutes(10));

                    let act_id = format!("act-{idx}");
                    let act = tokeira_kernel::ActivityState {
                        cancel_requested: false,
                        started_identity: None,
                        retry_last_worker_identity: None,
                        activity_id: act_id.clone(),
                        activity_type: "activity-type".into(),
                        schedule_event_id: idx as i64,
                        task_queue: TaskQueueName(
                            "q".into(),
                        ),
                        deployment: None,
                        build_id: None,
                        input:
                            tokeira_types::Payloads::default(),
                        header: None,
                        last_failure: None,
                        heartbeat_details: None,
                        attempt: 1,
                        retry_policy: None,
                        schedule_to_close_timeout: Some(
                            Duration::seconds(30),
                        ),
                        schedule_to_start_timeout: None,
                        start_to_close_timeout: None,
                        heartbeat_timeout: None,
                        scheduled_at: fixed_now(),
                        current_attempt_scheduled_at: None,
                        started_at: None,
                        started_event_id: None,
                        pause_info: None,
                        stamp: 0,
                    };
                    t.activity_ops.push(
                        tokeira_kernel::ActivityOp::Upsert(
                            act.clone(),
                        ),
                    );
                    t.next_state
                        .activities
                        .insert(act_id.clone(), act);

                    let timer_id = format!("tmr-{idx}");
                    let tmr = timer_state(
                        &timer_id,
                        fixed_now(),
                    );
                    t.timer_ops.push(
                        tokeira_kernel::TimerOp::Upsert(
                            tmr.clone(),
                        ),
                    );
                    t.next_state
                        .timers
                        .insert(timer_id, tmr);

                    let queue = QueueKey {
                        namespace_id: ns,
                        task_queue: TaskQueueName(
                            "q".into(),
                        ),
                        task_kind: TaskKind::Activity,
                        deployment: None,
                        build_id: None,
                    };
                    t.dispatch_ops.push(
                        DispatchOp::EnqueueActivityTask {
                            queue,
                            activity_id: act_id,
                            input:
                                tokeira_types::Payloads::default(),
                            schedule_event_id: idx as i64,
                            attempt: 1,
                            dispatch_revision: 0,
                            schedule_to_close_timeout: Some(
                                Duration::seconds(30),
                            ),
                            schedule_to_start_timeout: None,
                            start_to_close_timeout: None,
                            heartbeat_timeout: None,
                        },
                    );

                    let result = store
                        .commit_transition(
                            run_key,
                            t,
                            ShardEpoch::ZERO,
                        )
                        .await
                        .unwrap();
                    assert!(matches!(
                        result,
                        CommitResult::Applied { .. }
                    ));
                }

                for target_shard_id in 0..shard_count {
                    let sid = ShardId(target_shard_id);
                    let expected_runs: Vec<RunKey> =
                        run_shards
                            .iter()
                            .filter(|(_, s)| *s == sid)
                            .map(|(rk, _)| *rk)
                            .collect();

                    let wf_tasks = store
                        .list_dispatchable_workflow_tasks_for_shard(
                            sid,
                            usize::MAX,
                        )
                        .await
                        .unwrap();
                    assert_eq!(
                        wf_tasks.len(),
                        expected_runs.len(),
                    );
                    for task in &wf_tasks {
                        assert!(
                            expected_runs
                                .contains(&task.run_key),
                        );
                    }

                    let act_tasks = store
                        .list_dispatchable_activity_tasks_for_shard(
                            sid,
                            usize::MAX,
                        )
                        .await
                        .unwrap();
                    assert_eq!(
                        act_tasks.len(),
                        expected_runs.len(),
                    );
                    for task in &act_tasks {
                        assert!(
                            expected_runs
                                .contains(&task.run_key),
                        );
                    }

                    let timers = store
                        .list_due_timers_for_shard(
                            sid,
                            fixed_now(),
                            usize::MAX,
                        )
                        .await
                        .unwrap();
                    assert_eq!(
                        timers.len(),
                        expected_runs.len(),
                    );
                    for timer in &timers {
                        assert!(
                            expected_runs
                                .contains(&timer.run_key),
                        );
                    }

                    let wf_timeouts = store
                        .list_runs_with_workflow_timeouts_for_shard(
                            sid,
                            usize::MAX,
                        )
                        .await
                        .unwrap();
                    assert_eq!(
                        wf_timeouts.len(),
                        expected_runs.len(),
                    );
                    for entry in &wf_timeouts {
                        assert!(
                            expected_runs
                                .contains(&entry.run_key),
                        );
                    }

                    let activities = store
                        .list_open_activities_for_shard(
                            sid,
                            usize::MAX,
                        )
                        .await
                        .unwrap();
                    assert_eq!(
                        activities.len(),
                        expected_runs.len(),
                    );
                    for entry in &activities {
                        assert!(
                            expected_runs
                                .contains(&entry.run_key),
                        );
                    }
                }
            });
        }
    }

    async fn commit_closed_lineage_run(
        store: &InMemoryStore,
        namespace_id: NamespaceId,
        workflow_id: &WorkflowId,
        run_key: RunKey,
        run_id: RunId,
    ) {
        let mut start = start_transition(run_key);
        start.next_state.namespace_id = namespace_id;
        start.next_state.workflow_id = workflow_id.clone();
        start.next_state.run_id = run_id;
        let mut closed_state = start.next_state.clone();
        store
            .commit_transition(run_key, start, ShardEpoch::ZERO)
            .await
            .unwrap();

        closed_state.transition_seq = TransitionSeq(2);
        closed_state.status = ExecutionStatus::Completed;
        closed_state.pending_workflow_task = None;
        closed_state.closed_at = Some(fixed_now());
        let close = Transition {
            expected_seq: TransitionSeq(1),
            next_state: closed_state,
            history_events: Default::default(),
            request_dedupe_ops: Default::default(),
            activity_ops: Default::default(),
            timer_ops: Default::default(),
            dispatch_ops: Default::default(),
            projection_ops: Default::default(),
        };
        store
            .commit_transition(run_key, close, ShardEpoch::ZERO)
            .await
            .unwrap();
    }

    // Feature: temporal-ui-support, Property 5: authoritative workflow deletion
    // A successful purge removes every run-owned authoritative/dispatch row and
    // leaves exactly the newer Deleted projection high-water record.
    // **Validates: Requirements 9.1, 9.2, 9.4**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        #[test]
        fn property_authoritative_workflow_deletion(
            seed in any::<u128>(),
            side_row_count in 1usize..6,
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let store = InMemoryStore::with_shard_count(1);
                let run_key = RunKey(uuid::Uuid::from_u128(seed));
                let namespace_id = NamespaceId(uuid::Uuid::from_u128(seed.wrapping_add(1)));
                let workflow_id = WorkflowId(format!("delete-{seed}"));
                let run_id = RunId(uuid::Uuid::from_u128(seed.wrapping_add(2)));
                let activity_queue = QueueKey {
                    namespace_id,
                    task_queue: TaskQueueName("activity-q".to_string()),
                    task_kind: TaskKind::Activity,
                    deployment: None,
                    build_id: None,
                };

                let mut transition = start_transition(run_key);
                transition.next_state.namespace_id = namespace_id;
                transition.next_state.workflow_id = workflow_id.clone();
                transition.next_state.run_id = run_id;
                transition.next_state.status = ExecutionStatus::Terminated;
                transition.next_state.pending_workflow_task = None;
                transition.next_state.closed_at = Some(fixed_now());
                transition.next_state.last_event_id = side_row_count as i64;
                for index in 0..side_row_count {
                    let suffix = index.to_string();
                    transition.history_events.push(history_event(
                        index as i64 + 1,
                        fixed_now(),
                        HistoryEventKind::WorkflowExecutionTerminated {
                            reason: format!("terminated-{suffix}"),
                            details: None,
                            identity: "history-service".to_string(),
                        },
                    ));
                    transition.request_dedupe_ops.push(RequestDedupeOp {
                        request_id: RequestId(format!("request-{suffix}")),
                    });
                    let activity_id = format!("activity-{suffix}");
                    let mut activity = activity_state(&activity_id);
                    activity.schedule_event_id = index as i64 + 1;
                    transition.activity_ops.push(ActivityOp::Upsert(activity));
                    transition.timer_ops.push(TimerOp::Upsert(timer_state(
                        &format!("timer-{suffix}"),
                        fixed_now() + Duration::minutes(1),
                    )));
                    transition.dispatch_ops.push(DispatchOp::EnqueueActivityTask {
                        queue: activity_queue.clone(),
                        activity_id,
                        input: tokeira_types::Payloads::default(),
                        schedule_event_id: index as i64 + 1,
                        attempt: 1,
                        dispatch_revision: 0,
                        schedule_to_close_timeout: None,
                        schedule_to_start_timeout: None,
                        start_to_close_timeout: None,
                        heartbeat_timeout: None,
                    });
                }
                let commit = store
                    .commit_transition(run_key, transition, ShardEpoch::ZERO)
                    .await
                    .unwrap();
                prop_assert!(
                    matches!(commit, CommitResult::Applied { .. }),
                    "seed commit should apply",
                );

                store
                    .persist_to_backlog(
                        (0..side_row_count)
                            .map(|index| BacklogEntry {
                                run_key,
                                queue: activity_queue.clone(),
                                payload: crate::api::BacklogPayload::Activity {
                                    activity_id: format!("backlog-{index}"),
                                    input: tokeira_types::Payloads::default(),
                                    schedule_event_id: index as i64 + 1,
                                    attempt: 1,
                                    dispatch_revision: 0,
                                },
                                scheduled_at: fixed_now(),
                                insertion_seq: index as u64,
                            })
                            .collect(),
                    )
                    .await
                    .unwrap();
                store.inject_conflict(run_key, 1).await;

                {
                    let inner = store.inner.lock().await;
                    prop_assert_eq!(inner.history[&run_key].len(), side_row_count);
                    prop_assert_eq!(
                        inner
                            .request_dedupe
                            .values()
                            .filter(|record| record.run_key == run_key)
                            .count(),
                        side_row_count,
                    );
                    prop_assert_eq!(
                        inner
                            .activity_state_table
                            .keys()
                            .filter(|(candidate, _)| *candidate == run_key)
                            .count(),
                        side_row_count,
                    );
                    prop_assert_eq!(
                        inner
                            .timer_bucket
                            .keys()
                            .filter(|(candidate, _)| *candidate == run_key)
                            .count(),
                        side_row_count,
                    );
                    prop_assert_eq!(
                        inner
                            .activity_dispatch
                            .keys()
                            .filter(|(candidate, _)| *candidate == run_key)
                            .count(),
                        side_row_count,
                    );
                    prop_assert_eq!(
                        inner
                            .dispatch_backlog
                            .iter()
                            .filter(|entry| entry.run_key == run_key)
                            .count(),
                        side_row_count,
                    );
                }

                let result = store
                    .delete_run_for_bundle(
                        run_key,
                        ShardId(0),
                        DeleteRunRequest {
                            expected_seq: TransitionSeq(1),
                            deleted_at: fixed_now() + Duration::seconds(1),
                        },
                        ShardEpoch::ZERO,
                    )
                    .await
                    .unwrap();
                let DeleteRunResult::Deleted { tombstone } = result else {
                    prop_assert!(false, "closed run should be deleted: {result:?}");
                    return Ok(());
                };
                prop_assert_eq!(tombstone.transition_seq, TransitionSeq(2));
                prop_assert_eq!(
                    tombstone.context.lifecycle_state,
                    VisibilityLifecycleState::Deleted,
                );
                prop_assert!(tombstone.context.memo.0.is_empty());
                prop_assert!(tombstone.context.search_attributes.0.is_empty());

                prop_assert!(matches!(
                    store.load_run(run_key).await.unwrap(),
                    LoadedRun::Absent
                ));
                prop_assert!(store.read_history(run_key, 0, usize::MAX).await.unwrap().is_empty());
                prop_assert_eq!(
                    store
                        .resolve_execution(&ExecutionRef {
                            namespace_id,
                            workflow_id: workflow_id.clone(),
                            run_id: Some(run_id),
                        })
                        .await
                        .unwrap(),
                    None,
                );

                let inner = store.inner.lock().await;
                prop_assert!(!inner.runs.contains_key(&run_key));
                prop_assert!(!inner.history.contains_key(&run_key));
                prop_assert!(!inner.transition_audit.contains_key(&run_key));
                prop_assert!(!inner.run_shard_map.contains_key(&run_key));
                prop_assert!(!inner.conflict_injections.contains_key(&run_key));
                prop_assert!(inner.request_dedupe.values().all(|record| record.run_key != run_key));
                prop_assert!(inner.activity_state_table.keys().all(|(candidate, _)| *candidate != run_key));
                prop_assert!(inner.timer_bucket.keys().all(|(candidate, _)| *candidate != run_key));
                prop_assert!(inner.activity_dispatch.keys().all(|(candidate, _)| *candidate != run_key));
                prop_assert!(inner.dispatch_backlog.iter().all(|entry| entry.run_key != run_key));
                prop_assert_eq!(inner.projection_log.last(), Some(&tombstone));
                Ok(())
            })?;
        }
    }

    // Feature: temporal-ui-support, Property 10: current-execution pointer safety
    // Deleting the selected run conditionally clears only its pointer and never
    // exposes an older surviving execution as current.
    // **Validates: Requirements 9.3**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        #[test]
        fn property_current_execution_pointer_safety(
            seed in any::<u128>(),
            older_count in 0usize..5,
            install_replacement in any::<bool>(),
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let store = InMemoryStore::with_shard_count(1);
                store
                    .set_conflict_policy(CurrentExecutionConflictPolicy::AllowAfterClose)
                    .await;
                let namespace_id = NamespaceId(uuid::Uuid::from_u128(seed.wrapping_add(1)));
                let workflow_id = WorkflowId(format!("lineage-{seed}"));
                let mut older = Vec::new();

                for index in 0..older_count {
                    let run_key = RunKey(uuid::Uuid::from_u128(
                        seed.wrapping_add(100 + index as u128),
                    ));
                    let run_id = RunId(uuid::Uuid::from_u128(
                        seed.wrapping_add(200 + index as u128),
                    ));
                    commit_closed_lineage_run(
                        &store,
                        namespace_id,
                        &workflow_id,
                        run_key,
                        run_id,
                    )
                    .await;
                    older.push((run_key, run_id));
                }

                let target_key = RunKey(uuid::Uuid::from_u128(seed.wrapping_add(10_000)));
                let target_id = RunId(uuid::Uuid::from_u128(seed.wrapping_add(20_000)));
                commit_closed_lineage_run(
                    &store,
                    namespace_id,
                    &workflow_id,
                    target_key,
                    target_id,
                )
                .await;

                let replacement = if install_replacement {
                    let run_key = RunKey(uuid::Uuid::from_u128(seed.wrapping_add(30_000)));
                    let run_id = RunId(uuid::Uuid::from_u128(seed.wrapping_add(40_000)));
                    let mut start = start_transition(run_key);
                    start.next_state.namespace_id = namespace_id;
                    start.next_state.workflow_id = workflow_id.clone();
                    start.next_state.run_id = run_id;
                    let result = store
                        .commit_transition(run_key, start, ShardEpoch::ZERO)
                        .await
                        .unwrap();
                    prop_assert!(
                        matches!(result, CommitResult::Applied { .. }),
                        "replacement start should apply",
                    );
                    Some((run_key, run_id))
                } else {
                    None
                };

                let result = store
                    .delete_run_for_bundle(
                        target_key,
                        ShardId(0),
                        DeleteRunRequest {
                            expected_seq: TransitionSeq(2),
                            deleted_at: fixed_now(),
                        },
                        ShardEpoch::ZERO,
                    )
                    .await
                    .unwrap();
                prop_assert!(
                    matches!(result, DeleteRunResult::Deleted { .. }),
                    "target deletion should apply",
                );

                prop_assert_eq!(
                    store.find_latest_run(namespace_id, &workflow_id).await.unwrap(),
                    replacement.map(|(run_key, _)| run_key),
                );
                prop_assert_eq!(
                    store
                        .resolve_execution(&ExecutionRef {
                            namespace_id,
                            workflow_id: workflow_id.clone(),
                            run_id: None,
                        })
                        .await
                        .unwrap(),
                    replacement.map(|(run_key, _)| run_key),
                );
                prop_assert_eq!(
                    store
                        .resolve_execution(&ExecutionRef {
                            namespace_id,
                            workflow_id: workflow_id.clone(),
                            run_id: Some(target_id),
                        })
                        .await
                        .unwrap(),
                    None,
                );
                for (run_key, run_id) in older {
                    prop_assert_eq!(
                        store
                            .resolve_execution(&ExecutionRef {
                                namespace_id,
                                workflow_id: workflow_id.clone(),
                                run_id: Some(run_id),
                            })
                            .await
                            .unwrap(),
                        Some(run_key),
                    );
                }
                Ok(())
            })?;
        }
    }

    #[test]
    fn load_run_emits_storage_load_run_span() {
        // A thread-local tracing dispatch does not follow a future migrated to
        // another worker thread. Keep this instrumentation assertion on one
        // thread so parallel test load cannot make the span nondeterministic.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let buffer = SpanNames::default();
        let subscriber = tracing_subscriber::registry().with(buffer.clone());
        // This test is the crate's sole subscriber installer. A global default
        // avoids the static-callsite interest race that a thread-local dispatch
        // has with parallel tests invoking `load_run` first.
        tracing::subscriber::set_global_default(subscriber)
            .expect("storage tests should install one tracing subscriber");

        rt.block_on(async {
            let store = InMemoryStore::default();
            let run_key = RunKey::new();
            seed_base_run(&store, run_key, sample_state(run_key), Vec::new()).await;

            let _ = store.load_run(run_key).await.unwrap();
        });

        let names = buffer.0.lock().unwrap().clone();
        assert!(names.iter().any(|name| name == "storage.load_run"));
    }
}
