use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use anyhow::Result;
use time::OffsetDateTime;
use tokio::{
    sync::{Mutex, Notify},
    time::{timeout, Duration},
};
use tokeira_storage::DispatchableWorkflowTask;
use tokeira_types::{LogicalTaskSeq, QueueKey, RunKey, WorkerIdentity};

/// Lightweight in-memory workflow-task broker.
///
/// The broker exists so pollers do not become durable objects. The interesting
/// thing here is not the data structure sophistication, but the contract:
/// - worker polls stay memory-only,
/// - sticky preference is honored when possible,
/// - stale sticky hints are allowed to decay into general readiness,
/// - duplicate publications are suppressed by logical task identity.
///
/// TODO(perf): split this into explicit sticky/live/backlog tiers once the
/// surrounding runtime grows. This starter keeps the semantic points visible
/// without trying to be production-smart too early.
#[derive(Default, Clone)]
pub struct InMemoryBroker {
    inner: Arc<Mutex<BrokerState>>,
    wake: Arc<Notify>,
}

#[derive(Default)]
struct BrokerState {
    sticky_ready: HashMap<QueueKey, VecDeque<DispatchableWorkflowTask>>,
    general_ready: HashMap<QueueKey, VecDeque<DispatchableWorkflowTask>>,
    enqueued: HashSet<(RunKey, LogicalTaskSeq)>,
}

impl InMemoryBroker {
    pub async fn publish_workflow_task(&self, task: DispatchableWorkflowTask) {
        let mut inner = self.inner.lock().await;
        let dedupe_key = (task.run_key, task.logical_seq);
        if !inner.enqueued.insert(dedupe_key) {
            return;
        }

        if task.sticky_preferred.is_some() {
            inner
                .sticky_ready
                .entry(task.queue.clone())
                .or_default()
                .push_back(task);
        } else {
            inner
                .general_ready
                .entry(task.queue.clone())
                .or_default()
                .push_back(task);
        }
        drop(inner);
        self.wake.notify_waiters();
    }

    pub async fn poll_workflow_task(
        &self,
        queue: &QueueKey,
        worker: &WorkerIdentity,
        wait_for: Duration,
    ) -> Result<Option<DispatchableWorkflowTask>> {
        // TODO(perf): add fairness budgets between sticky/live/backlog sources.
        // TODO(perf): add per-namespace admission and caps.
        if let Some(task) = self.try_take(queue, worker).await? {
            return Ok(Some(task));
        }

        if timeout(wait_for, self.wake.notified()).await.is_err() {
            return Ok(None);
        }

        self.try_take(queue, worker).await
    }

    async fn try_take(
        &self,
        queue: &QueueKey,
        worker: &WorkerIdentity,
    ) -> Result<Option<DispatchableWorkflowTask>> {
        let mut inner = self.inner.lock().await;
        let now = OffsetDateTime::now_utc();
        let mut promote_to_general = Vec::new();
        let mut matched = None;

        if let Some(sticky) = inner.sticky_ready.get_mut(queue) {
            let mut idx = 0;
            while idx < sticky.len() {
                let action = match sticky.get(idx) {
                    Some(task) if task.sticky_preferred.as_ref() == Some(worker) => StickyAction::Take,
                    Some(task) if task.sticky_expires_at.is_some_and(|expires_at| expires_at <= now) => {
                        StickyAction::Promote
                    }
                    Some(task) if task.sticky_preferred.is_none() => StickyAction::Promote,
                    Some(_) => StickyAction::Keep,
                    None => break,
                };

                match action {
                    StickyAction::Take => {
                        matched = sticky.remove(idx);
                        break;
                    }
                    StickyAction::Promote => {
                        if let Some(mut task) = sticky.remove(idx) {
                            task.sticky_preferred = None;
                            task.sticky_expires_at = None;
                            promote_to_general.push(task);
                        }
                    }
                    StickyAction::Keep => {
                        idx += 1;
                    }
                }
            }
        }

        if !promote_to_general.is_empty() {
            let general = inner.general_ready.entry(queue.clone()).or_default();
            for task in promote_to_general {
                general.push_back(task);
            }
        }

        if let Some(task) = matched {
            inner.enqueued.remove(&(task.run_key, task.logical_seq));
            return Ok(Some(task));
        }

        let task = inner
            .general_ready
            .get_mut(queue)
            .and_then(|q| q.pop_front());
        if let Some(task) = &task {
            inner.enqueued.remove(&(task.run_key, task.logical_seq));
        }
        Ok(task)
    }
}

enum StickyAction {
    Keep,
    Promote,
    Take,
}
