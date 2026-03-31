use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use time::OffsetDateTime;
use tokio::sync::Mutex;
use tokeira_kernel::{LoadedRun, Transition, WorkflowState};
use tokeira_types::{
    ExecutionRef, NamespaceId, ProjectionCursor, QueueKey, RequestId, RunId, RunKey, ShardEpoch,
    ShardId, WorkflowId,
};

use crate::api::{
    CommitResult, ConnectionDirector, DbClass, DbPermit, DispatchableWorkflowTask, DueTimer,
    LeaseOutcome, LeaseRepository, ProjectionBatch, ProjectionLog, ProjectionRecord, RequestRecord,
    RunRepository, TransitionAuditRecord,
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
}

#[async_trait]
impl RunRepository for InMemoryStore {
    async fn resolve_execution(&self, execution: &ExecutionRef) -> Result<Option<RunKey>> {
        let store = self.inner.lock().await;
        if let Some(run_id) = execution.run_id {
            return Ok(store
                .execution_index
                .get(&(execution.namespace_id, execution.workflow_id.0.clone(), run_id))
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

    async fn read_transition_audit(&self, run_key: RunKey) -> Result<Vec<TransitionAuditRecord>> {
        let store = self.inner.lock().await;
        Ok(store
            .transition_audit
            .get(&run_key)
            .cloned()
            .unwrap_or_default())
    }

    async fn commit_transition(&self, run_key: RunKey, transition: Transition) -> Result<CommitResult> {
        let mut store = self.inner.lock().await;
        let current_seq = store
            .runs
            .get(&run_key)
            .map(|state| state.transition_seq)
            .unwrap_or(tokeira_types::TransitionSeq::ZERO);
        if current_seq != transition.expected_seq {
            return Ok(CommitResult::Conflict {
                reason: format!("expected seq {:?}, found {:?}", transition.expected_seq, current_seq),
            });
        }

        let state = transition.next_state.clone();
        let workflow_key = (state.namespace_id, state.workflow_id.0.clone());

        for op in &transition.request_dedupe_ops {
            let dedupe_key = (workflow_key.0, workflow_key.1.clone(), op.request_id.0.clone());
            if store.request_dedupe.contains_key(&dedupe_key) {
                return Ok(CommitResult::Duplicate);
            }
        }

        if transition.expected_seq == tokeira_types::TransitionSeq::ZERO && state.status.is_open() {
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
            let key = (workflow_key.0, workflow_key.1.clone(), op.request_id.0.clone());
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

        store.runs.insert(run_key, state.clone());
        store.execution_index.insert(
            (state.namespace_id, state.workflow_id.0.clone(), state.run_id),
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
                sticky_preferred: state.sticky.as_ref().map(|s| s.worker_identity.clone()),
                sticky_expires_at: state.sticky.as_ref().map(|s| s.expires_at),
            });
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    async fn list_due_timers(&self, now: OffsetDateTime, limit: usize) -> Result<Vec<DueTimer>> {
        let store = self.inner.lock().await;
        let mut due = Vec::new();
        for state in store.runs.values() {
            for timer in state.timers.values() {
                if timer.fire_at <= now {
                    due.push(DueTimer {
                        run_key: state.run_key,
                        timer_id: timer.timer_id.clone(),
                    });
                    if due.len() >= limit {
                        return Ok(due);
                    }
                }
            }
        }
        Ok(due)
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
    async fn try_acquire_bundle(&self, bundle: ShardId, owner: String) -> Result<LeaseOutcome> {
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

    async fn renew_bundle(&self, bundle: ShardId, owner: String, epoch: ShardEpoch) -> Result<LeaseOutcome> {
        let mut store = self.inner.lock().await;
        match store.bundle_leases.get_mut(&bundle) {
            Some((current_owner, current_epoch)) if *current_owner == owner && *current_epoch == epoch => {
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
