use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use anyhow::Result;
use time::OffsetDateTime;
use tokeira_storage::{DispatchableActivityTask, DispatchableWorkflowTask};
use tokeira_types::{LogicalTaskSeq, QueueKey, RunKey, WorkerIdentity};
use tokio::{
    sync::{Mutex, Notify},
    time::{Duration, timeout},
};

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

/// In-memory activity-task broker.
///
/// Mirrors [`InMemoryBroker`] but for activity tasks.
/// Deduplication is keyed on `(run_key, activity_id, attempt)`.
#[derive(Default, Clone)]
pub struct InMemoryActivityBroker {
    inner: Arc<Mutex<ActivityBrokerState>>,
    wake: Arc<Notify>,
}

#[derive(Default)]
struct ActivityBrokerState {
    ready: HashMap<QueueKey, VecDeque<DispatchableActivityTask>>,
    enqueued: HashSet<(RunKey, String, u32)>,
}

#[derive(Default)]
struct BrokerState {
    sticky_ready: HashMap<QueueKey, VecDeque<DispatchableWorkflowTask>>,
    general_ready: HashMap<QueueKey, VecDeque<DispatchableWorkflowTask>>,
    enqueued: HashSet<(RunKey, LogicalTaskSeq)>,
}

impl InMemoryBroker {
    /// Enqueue a workflow task for delivery.
    ///
    /// Duplicate publications (same `run_key` + `logical_seq`)
    /// are silently suppressed. Sticky-preferred tasks are
    /// placed in the sticky tier; all others go to general.
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

    /// Long-poll for a workflow task on `queue`.
    ///
    /// Returns immediately if a task is available, otherwise
    /// blocks up to `wait_for`. Sticky tasks matching `worker`
    /// are preferred; expired sticky hints are promoted to the
    /// general tier.
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
                    Some(task) if task.sticky_preferred.as_ref() == Some(worker) => {
                        StickyAction::Take
                    }
                    Some(task)
                        if task
                            .sticky_expires_at
                            .is_some_and(|expires_at| expires_at <= now) =>
                    {
                        StickyAction::Promote
                    }
                    Some(task) if task.sticky_preferred.is_none() => {
                        StickyAction::Promote
                    }
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

impl InMemoryActivityBroker {
    /// Enqueue an activity task for delivery.
    ///
    /// Duplicate publications (same `run_key` +
    /// `activity_id` + `attempt`) are silently suppressed.
    pub async fn publish_activity_task(
        &self,
        task: DispatchableActivityTask,
    ) -> Result<()> {
        let mut inner = self.inner.lock().await;
        let dedupe_key = (task.run_key, task.activity_id.clone(), task.attempt);
        if !inner.enqueued.insert(dedupe_key) {
            return Ok(());
        }

        inner
            .ready
            .entry(task.queue.clone())
            .or_default()
            .push_back(task);
        drop(inner);
        self.wake.notify_waiters();
        Ok(())
    }

    /// Long-poll for an activity task on `queue`.
    ///
    /// Returns immediately if a task is available,
    /// otherwise blocks up to `wait_for`.
    pub async fn poll_activity_task(
        &self,
        queue: &QueueKey,
        wait_for: Duration,
    ) -> Result<Option<DispatchableActivityTask>> {
        if let Some(task) = self.try_take(queue).await? {
            return Ok(Some(task));
        }

        if timeout(wait_for, self.wake.notified()).await.is_err() {
            return Ok(None);
        }

        self.try_take(queue).await
    }

    async fn try_take(
        &self,
        queue: &QueueKey,
    ) -> Result<Option<DispatchableActivityTask>> {
        let mut inner = self.inner.lock().await;
        let task = inner.ready.get_mut(queue).and_then(|q| q.pop_front());
        if let Some(task) = &task {
            inner.enqueued.remove(&(
                task.run_key,
                task.activity_id.clone(),
                task.attempt,
            ));
        }
        Ok(task)
    }
}

enum StickyAction {
    Keep,
    Promote,
    Take,
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::Duration as TimeDuration;
    use tokeira_types::{NamespaceId, Payloads, TaskKind, TaskQueueName};

    fn activity_queue(name: &str) -> QueueKey {
        QueueKey {
            namespace_id: NamespaceId::new(),
            task_queue: TaskQueueName(name.to_string()),
            task_kind: TaskKind::Activity,
            deployment: None,
            build_id: None,
        }
    }

    fn activity_task(queue: QueueKey) -> DispatchableActivityTask {
        DispatchableActivityTask {
            run_key: RunKey::new(),
            queue,
            activity_id: "activity-1".to_string(),
            input: Payloads::default(),
            schedule_event_id: 7,
            attempt: 1,
        }
    }

    #[tokio::test]
    async fn activity_broker_deduplicates_by_run_activity_attempt() {
        let broker = InMemoryActivityBroker::default();
        let queue = activity_queue("queue-a");
        let task = activity_task(queue.clone());

        broker.publish_activity_task(task.clone()).await.unwrap();
        broker.publish_activity_task(task.clone()).await.unwrap();

        let first = broker
            .poll_activity_task(&queue, std::time::Duration::from_millis(5))
            .await
            .unwrap();
        let second = broker
            .poll_activity_task(&queue, std::time::Duration::from_millis(5))
            .await
            .unwrap();

        assert_eq!(first, Some(task));
        assert_eq!(second, None);
    }

    #[tokio::test]
    async fn activity_broker_isolates_queues() {
        let broker = InMemoryActivityBroker::default();
        let queue_a = activity_queue("queue-a");
        let queue_b = activity_queue("queue-b");
        let task = activity_task(queue_a.clone());

        broker.publish_activity_task(task.clone()).await.unwrap();

        let wrong = broker
            .poll_activity_task(&queue_b, std::time::Duration::from_millis(5))
            .await
            .unwrap();
        let right = broker
            .poll_activity_task(&queue_a, std::time::Duration::from_millis(5))
            .await
            .unwrap();

        assert_eq!(wrong, None);
        assert_eq!(right, Some(task));
        let _ = TimeDuration::ZERO;
    }
}
