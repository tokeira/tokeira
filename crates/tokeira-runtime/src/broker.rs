//! In-memory delivery brokers for workflow, activity, and query work.
//!
//! These brokers are intentionally transport-local. Pollers should not become
//! durable storage objects, and queue fairness/sticky behavior should be
//! understandable without reading storage code. When work must survive process
//! loss or prolonged absence of pollers, that responsibility moves to the
//! durable backlog and scanner paths elsewhere in the runtime.
//!
//! Query delivery is kept separate from workflow-task delivery: queries may
//! need sticky affinity and long-poll wakeups, but they should not advance
//! history or masquerade as durable workflow tasks.
//!
//! ## Sticky / general tier model
//!
//! Workflow tasks enter the *sticky* tier when the run has a preferred worker
//! (set by the sticky TTL during `start_polled_workflow_task`). Only the
//! matching worker may take a sticky task; if the TTL expires before that
//! worker polls, the task is promoted to the *general* tier where any poller
//! can claim it. This avoids full-history replays when the worker's cache is
//! warm, while still guaranteeing progress when a worker disappears.
//!
//! ## Deduplication
//!
//! Each workflow task is keyed by `(RunKey, LogicalTaskSeq)` and each activity
//! task by `(RunKey, activity_id, attempt)`. Duplicate publications are
//! silently suppressed so that scanner sweeps and retry paths can safely
//! re-publish without creating phantom work items.
//!
//! ## Notify-based wake pattern
//!
//! Pollers register a `Notify` future *before* re-checking the queue. This
//! closes the TOCTOU race between `try_take` returning `None` and a concurrent
//! `publish` calling `notify_waiters`: if the publish fires in that gap, the
//! already-registered future still fires and the poller retries.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use anyhow::Result;
use time::OffsetDateTime;
use tokeira_storage::{DispatchableActivityTask, DispatchableWorkflowTask};
use tokeira_types::{
    LogicalTaskSeq, NamespaceId, QueueKey, RunKey, TaskQueueName, WorkerIdentity,
};
use tokio::{
    sync::{Mutex, Notify},
    time::{Duration, Instant, timeout},
};

use crate::{DeliveryMetrics, QueryTask};

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
    query_wake: Arc<Notify>,
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
    ready: HashMap<QueueKey, VecDeque<TimestampedActivityTask>>,
    enqueued: HashSet<(RunKey, String, u32)>,
    waiter_counts: HashMap<QueueKey, usize>,
}

#[derive(Default)]
struct BrokerState {
    sticky_ready: HashMap<QueueKey, VecDeque<TimestampedWorkflowTask>>,
    general_ready: HashMap<QueueKey, VecDeque<TimestampedWorkflowTask>>,
    enqueued: HashSet<(RunKey, LogicalTaskSeq)>,
    waiter_counts: HashMap<QueueKey, usize>,
    query_ready: HashMap<QueueKey, VecDeque<QueryTask>>,
    query_waiter_counts: HashMap<QueueKey, usize>,
    denied_workers: HashSet<(NamespaceId, TaskQueueName, WorkerIdentity)>,
}

#[derive(Clone, Debug)]
pub(crate) struct TimestampedWorkflowTask {
    pub(crate) task: DispatchableWorkflowTask,
    pub(crate) entered_at: Instant,
    pub(crate) scheduled_at: OffsetDateTime,
}

#[derive(Clone, Debug)]
pub(crate) struct TimestampedActivityTask {
    pub(crate) task: DispatchableActivityTask,
    pub(crate) entered_at: Instant,
    pub(crate) scheduled_at: OffsetDateTime,
}

impl InMemoryBroker {
    /// Publish a query task without deduplication or backlog participation.
    pub async fn publish_query_task(&self, task: QueryTask) {
        let mut inner = self.inner.lock().await;
        inner
            .query_ready
            .entry(task.queue.clone())
            .or_default()
            .push_back(task);
        drop(inner);
        self.query_wake.notify_waiters();
    }

    /// Long-poll for a read-only query task on `queue`.
    ///
    /// Sticky-matched queries are preferred. Queries with a
    /// non-matching sticky worker stay queued for the matching
    /// worker rather than being taken by any poller.
    pub async fn poll_query_task(
        &self,
        queue: &QueueKey,
        worker: &WorkerIdentity,
        wait_for: Duration,
    ) -> Option<QueryTask> {
        if let Some(task) = self.try_take_query(queue, worker).await {
            return Some(task);
        }

        self.increment_query_waiter(queue).await;
        let notified = timeout(wait_for, self.query_wake.notified()).await;
        self.decrement_query_waiter(queue).await;

        if notified.is_err() {
            return None;
        }

        self.try_take_query(queue, worker).await
    }

    /// Enqueue a workflow task for delivery.
    ///
    /// Duplicate publications (same `run_key` + `logical_seq`)
    /// are silently suppressed. Sticky-preferred tasks are
    /// placed in the sticky tier; all others go to general.
    pub async fn publish_workflow_task(
        &self,
        task: DispatchableWorkflowTask,
        metrics: Option<&DeliveryMetrics>,
    ) {
        let mut inner = self.inner.lock().await;
        let dedupe_key = (task.run_key, task.logical_seq);
        if !inner.enqueued.insert(dedupe_key) {
            return;
        }
        let has_waiter = inner.waiter_counts.get(&task.queue).copied().unwrap_or(0) > 0;
        if let Some(metrics) = metrics {
            if has_waiter {
                metrics.record_sync_match(&task.queue);
            } else {
                metrics.record_non_sync_match(&task.queue);
            }
        }
        let timestamped = TimestampedWorkflowTask {
            task,
            entered_at: Instant::now(),
            scheduled_at: OffsetDateTime::now_utc(),
        };

        if timestamped.task.sticky_preferred.is_some() {
            inner
                .sticky_ready
                .entry(timestamped.task.queue.clone())
                .or_default()
                .push_back(timestamped);
        } else {
            inner
                .general_ready
                .entry(timestamped.task.queue.clone())
                .or_default()
                .push_back(timestamped);
        }
        drop(inner);
        self.wake.notify_waiters();
    }

    /// Stop future workflow-task deliveries for a worker on one sticky queue.
    ///
    /// The upstream shutdown API identifies a sticky workflow-task queue, not
    /// every activity or non-sticky queue a worker may use, so the deny entry is
    /// scoped to the namespace and task queue observed by workflow-task polls.
    pub async fn deny_worker(
        &self,
        namespace_id: NamespaceId,
        task_queue: TaskQueueName,
        worker: WorkerIdentity,
    ) {
        let mut inner = self.inner.lock().await;
        inner
            .denied_workers
            .insert((namespace_id, task_queue, worker));
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
    ) -> Result<Option<(DispatchableWorkflowTask, Instant)>> {
        if self.is_denied(queue, worker).await {
            return Ok(None);
        }
        if let Some(task) = self.try_take(queue, worker).await? {
            return Ok(Some(task));
        }

        self.increment_waiter(queue).await;

        // Create the notified future first, then re-check. This
        // closes the race between try_take and notify_waiters:
        // if a task was published between our first try_take and
        // registering the waiter, we catch it here.
        let notified = self.wake.notified();
        if let Some(task) = self.try_take(queue, worker).await? {
            self.decrement_waiter(queue).await;
            return Ok(Some(task));
        }

        let _ = timeout(wait_for, notified).await;
        self.decrement_waiter(queue).await;

        if self.is_denied(queue, worker).await {
            return Ok(None);
        }
        self.try_take(queue, worker).await
    }

    pub async fn queues_with_waiters(&self) -> HashSet<QueueKey> {
        self.inner
            .lock()
            .await
            .waiter_counts
            .iter()
            .filter_map(|(queue, count)| (*count > 0).then_some(queue.clone()))
            .collect()
    }

    async fn try_take_query(
        &self,
        queue: &QueueKey,
        worker: &WorkerIdentity,
    ) -> Option<QueryTask> {
        let mut inner = self.inner.lock().await;
        let ready = inner.query_ready.get_mut(queue)?;

        if let Some(idx) = ready
            .iter()
            .position(|task| task.sticky_preferred.as_ref() == Some(worker))
        {
            return ready.remove(idx);
        }

        if let Some(idx) = ready
            .iter()
            .position(|task| task.sticky_preferred.is_none())
        {
            return ready.remove(idx);
        }

        None
    }

    pub(crate) async fn take_expired(
        &self,
        grace_window: Duration,
    ) -> Vec<TimestampedWorkflowTask> {
        let mut inner = self.inner.lock().await;
        let mut expired = Vec::new();
        let mut dedupe_keys = Vec::new();
        Self::drain_expired_workflow_queue(
            &mut inner.sticky_ready,
            grace_window,
            &mut expired,
            &mut dedupe_keys,
        );
        Self::drain_expired_workflow_queue(
            &mut inner.general_ready,
            grace_window,
            &mut expired,
            &mut dedupe_keys,
        );
        for key in dedupe_keys {
            inner.enqueued.remove(&key);
        }
        expired
    }

    async fn try_take(
        &self,
        queue: &QueueKey,
        worker: &WorkerIdentity,
    ) -> Result<Option<(DispatchableWorkflowTask, Instant)>> {
        let mut inner = self.inner.lock().await;
        if inner.denied_workers.contains(&(
            queue.namespace_id,
            queue.task_queue.clone(),
            worker.clone(),
        )) {
            return Ok(None);
        }
        let now = OffsetDateTime::now_utc();
        let mut promote_to_general = Vec::new();
        let mut matched = None;

        if let Some(sticky) = inner.sticky_ready.get_mut(queue) {
            let mut idx = 0;
            while idx < sticky.len() {
                let action = match sticky.get(idx) {
                    Some(task) if task.task.sticky_preferred.as_ref() == Some(worker) => {
                        StickyAction::Take
                    }
                    Some(task)
                        if task
                            .task
                            .sticky_expires_at
                            .is_some_and(|expires_at| expires_at <= now) =>
                    {
                        StickyAction::Promote
                    }
                    Some(task) if task.task.sticky_preferred.is_none() => {
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
                            task.task.sticky_preferred = None;
                            task.task.sticky_expires_at = None;
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
            inner
                .enqueued
                .remove(&(task.task.run_key, task.task.logical_seq));
            return Ok(Some((task.task, task.entered_at)));
        }

        let task = inner
            .general_ready
            .get_mut(queue)
            .and_then(|q| q.pop_front());
        if let Some(task) = &task {
            inner
                .enqueued
                .remove(&(task.task.run_key, task.task.logical_seq));
        }
        Ok(task.map(|task| (task.task, task.entered_at)))
    }

    async fn is_denied(&self, queue: &QueueKey, worker: &WorkerIdentity) -> bool {
        self.inner.lock().await.denied_workers.contains(&(
            queue.namespace_id,
            queue.task_queue.clone(),
            worker.clone(),
        ))
    }

    async fn increment_waiter(&self, queue: &QueueKey) {
        let mut inner = self.inner.lock().await;
        *inner.waiter_counts.entry(queue.clone()).or_default() += 1;
    }

    async fn decrement_waiter(&self, queue: &QueueKey) {
        let mut inner = self.inner.lock().await;
        if let Some(count) = inner.waiter_counts.get_mut(queue) {
            *count -= 1;
            if *count == 0 {
                inner.waiter_counts.remove(queue);
            }
        }
    }

    fn drain_expired_workflow_queue(
        queues: &mut HashMap<QueueKey, VecDeque<TimestampedWorkflowTask>>,
        grace_window: Duration,
        expired: &mut Vec<TimestampedWorkflowTask>,
        dedupe_keys: &mut Vec<(RunKey, LogicalTaskSeq)>,
    ) {
        for ready in queues.values_mut() {
            let mut idx = 0;
            while idx < ready.len() {
                let is_expired = ready
                    .get(idx)
                    .map(|entry| entry.entered_at.elapsed() >= grace_window)
                    .unwrap_or(false);
                if is_expired {
                    if let Some(entry) = ready.remove(idx) {
                        dedupe_keys.push((entry.task.run_key, entry.task.logical_seq));
                        expired.push(entry);
                    }
                } else {
                    idx += 1;
                }
            }
        }
    }

    async fn increment_query_waiter(&self, queue: &QueueKey) {
        let mut inner = self.inner.lock().await;
        *inner.query_waiter_counts.entry(queue.clone()).or_default() += 1;
    }

    async fn decrement_query_waiter(&self, queue: &QueueKey) {
        let mut inner = self.inner.lock().await;
        if let Some(count) = inner.query_waiter_counts.get_mut(queue) {
            *count -= 1;
            if *count == 0 {
                inner.query_waiter_counts.remove(queue);
            }
        }
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
        metrics: Option<&DeliveryMetrics>,
    ) -> Result<()> {
        let mut inner = self.inner.lock().await;
        let dedupe_key = (task.run_key, task.activity_id.clone(), task.attempt);
        if !inner.enqueued.insert(dedupe_key) {
            return Ok(());
        }
        let has_waiter = inner.waiter_counts.get(&task.queue).copied().unwrap_or(0) > 0;
        if let Some(metrics) = metrics {
            if has_waiter {
                metrics.record_sync_match(&task.queue);
            } else {
                metrics.record_non_sync_match(&task.queue);
            }
        }

        inner
            .ready
            .entry(task.queue.clone())
            .or_default()
            .push_back(TimestampedActivityTask {
                task,
                entered_at: Instant::now(),
                scheduled_at: OffsetDateTime::now_utc(),
            });
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
    ) -> Result<Option<(DispatchableActivityTask, Instant)>> {
        if let Some(task) = self.try_take(queue).await? {
            return Ok(Some(task));
        }

        self.increment_waiter(queue).await;

        let notified = timeout(wait_for, self.wake.notified()).await;
        self.decrement_waiter(queue).await;

        if notified.is_err() {
            return Ok(None);
        }

        self.try_take(queue).await
    }

    pub async fn queues_with_waiters(&self) -> HashSet<QueueKey> {
        self.inner
            .lock()
            .await
            .waiter_counts
            .iter()
            .filter_map(|(queue, count)| (*count > 0).then_some(queue.clone()))
            .collect()
    }

    pub(crate) async fn take_expired(
        &self,
        grace_window: Duration,
    ) -> Vec<TimestampedActivityTask> {
        let mut inner = self.inner.lock().await;
        let mut expired = Vec::new();
        let mut dedupe_keys = Vec::new();
        for ready in inner.ready.values_mut() {
            let mut idx = 0;
            while idx < ready.len() {
                let is_expired = ready
                    .get(idx)
                    .map(|entry| entry.entered_at.elapsed() >= grace_window)
                    .unwrap_or(false);
                if is_expired {
                    if let Some(entry) = ready.remove(idx) {
                        dedupe_keys.push((
                            entry.task.run_key,
                            entry.task.activity_id.clone(),
                            entry.task.attempt,
                        ));
                        expired.push(entry);
                    }
                } else {
                    idx += 1;
                }
            }
        }
        for key in dedupe_keys {
            inner.enqueued.remove(&key);
        }
        expired
    }

    async fn try_take(
        &self,
        queue: &QueueKey,
    ) -> Result<Option<(DispatchableActivityTask, Instant)>> {
        let mut inner = self.inner.lock().await;
        let task = inner.ready.get_mut(queue).and_then(|q| q.pop_front());
        if let Some(task) = &task {
            inner.enqueued.remove(&(
                task.task.run_key,
                task.task.activity_id.clone(),
                task.task.attempt,
            ));
        }
        Ok(task.map(|task| (task.task, task.entered_at)))
    }

    async fn increment_waiter(&self, queue: &QueueKey) {
        let mut inner = self.inner.lock().await;
        *inner.waiter_counts.entry(queue.clone()).or_default() += 1;
    }

    async fn decrement_waiter(&self, queue: &QueueKey) {
        let mut inner = self.inner.lock().await;
        if let Some(count) = inner.waiter_counts.get_mut(queue) {
            *count -= 1;
            if *count == 0 {
                inner.waiter_counts.remove(queue);
            }
        }
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
    use crate::QueryTask;
    use proptest::prelude::*;
    use time::Duration as TimeDuration;
    use tokeira_types::{
        BuildId, DeploymentId, NamespaceId, Payloads, TaskKind, TaskQueueName,
    };
    use tokio::sync::oneshot;

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

    fn workflow_queue(
        name: &str,
        deployment: Option<&str>,
        build_id: Option<&str>,
    ) -> QueueKey {
        QueueKey {
            namespace_id: NamespaceId::new(),
            task_queue: TaskQueueName(name.to_string()),
            task_kind: TaskKind::Workflow,
            deployment: deployment.map(|value| DeploymentId(value.to_string())),
            build_id: build_id.map(|value| BuildId(value.to_string())),
        }
    }

    fn workflow_task(queue: QueueKey) -> DispatchableWorkflowTask {
        DispatchableWorkflowTask {
            run_key: RunKey::new(),
            queue,
            logical_seq: LogicalTaskSeq::ONE,
            sticky_preferred: None,
            sticky_expires_at: None,
        }
    }

    #[tokio::test]
    async fn activity_broker_deduplicates_by_run_activity_attempt() {
        let broker = InMemoryActivityBroker::default();
        let queue = activity_queue("queue-a");
        let task = activity_task(queue.clone());

        broker
            .publish_activity_task(task.clone(), None)
            .await
            .unwrap();
        broker
            .publish_activity_task(task.clone(), None)
            .await
            .unwrap();

        let first = broker
            .poll_activity_task(&queue, std::time::Duration::from_millis(5))
            .await
            .unwrap();
        let second = broker
            .poll_activity_task(&queue, std::time::Duration::from_millis(5))
            .await
            .unwrap();

        assert_eq!(first.map(|entry| entry.0), Some(task));
        assert_eq!(second, None);
    }

    #[tokio::test]
    async fn activity_broker_isolates_queues() {
        let broker = InMemoryActivityBroker::default();
        let queue_a = activity_queue("queue-a");
        let queue_b = activity_queue("queue-b");
        let task = activity_task(queue_a.clone());

        broker
            .publish_activity_task(task.clone(), None)
            .await
            .unwrap();

        let wrong = broker
            .poll_activity_task(&queue_b, std::time::Duration::from_millis(5))
            .await
            .unwrap();
        let right = broker
            .poll_activity_task(&queue_a, std::time::Duration::from_millis(5))
            .await
            .unwrap();

        assert_eq!(wrong, None);
        assert_eq!(right.map(|entry| entry.0), Some(task));
        let _ = TimeDuration::ZERO;
    }

    #[tokio::test]
    async fn activity_broker_isolates_versioned_queues() {
        let broker = InMemoryActivityBroker::default();
        let versioned = QueueKey {
            namespace_id: NamespaceId::new(),
            task_queue: TaskQueueName("queue-a".to_string()),
            task_kind: TaskKind::Activity,
            deployment: Some(DeploymentId("deploy-a".to_string())),
            build_id: Some(BuildId("build-a".to_string())),
        };
        let unversioned = QueueKey {
            deployment: None,
            build_id: None,
            ..versioned.clone()
        };
        let task = activity_task(versioned.clone());

        broker
            .publish_activity_task(task.clone(), None)
            .await
            .unwrap();

        let wrong = broker
            .poll_activity_task(&unversioned, std::time::Duration::from_millis(5))
            .await
            .unwrap();
        let right = broker
            .poll_activity_task(&versioned, std::time::Duration::from_millis(5))
            .await
            .unwrap();

        assert_eq!(wrong, None);
        assert_eq!(right.map(|entry| entry.0), Some(task));
    }

    #[tokio::test]
    async fn workflow_broker_isolates_versioned_queues() {
        let broker = InMemoryBroker::default();
        let versioned = workflow_queue("queue-a", Some("deploy-a"), Some("build-a"));
        let unversioned = workflow_queue("queue-a", None, None);
        let task = workflow_task(versioned.clone());

        broker.publish_workflow_task(task.clone(), None).await;

        let wrong = broker
            .poll_workflow_task(
                &unversioned,
                &WorkerIdentity("worker-a".to_string()),
                std::time::Duration::from_millis(5),
            )
            .await
            .unwrap();
        let right = broker
            .poll_workflow_task(
                &versioned,
                &WorkerIdentity("worker-a".to_string()),
                std::time::Duration::from_millis(5),
            )
            .await
            .unwrap();

        assert_eq!(wrong, None);
        assert_eq!(right.map(|entry| entry.0), Some(task));
    }

    #[tokio::test]
    async fn denied_worker_cannot_receive_workflow_tasks() {
        let broker = InMemoryBroker::default();
        let queue = workflow_queue("queue-a", None, None);
        let worker = WorkerIdentity("worker-a".to_string());
        let task = workflow_task(queue.clone());

        broker
            .deny_worker(queue.namespace_id, queue.task_queue.clone(), worker.clone())
            .await;
        broker.publish_workflow_task(task, None).await;

        let denied = broker
            .poll_workflow_task(&queue, &worker, std::time::Duration::from_millis(5))
            .await
            .unwrap();
        let other_worker = broker
            .poll_workflow_task(
                &queue,
                &WorkerIdentity("worker-b".to_string()),
                std::time::Duration::from_millis(5),
            )
            .await
            .unwrap();

        assert!(denied.is_none());
        assert!(other_worker.is_some());
    }

    #[tokio::test]
    async fn denied_workflow_worker_does_not_affect_activity_delivery() {
        let workflow_broker = InMemoryBroker::default();
        let activity_broker = InMemoryActivityBroker::default();
        let workflow_queue = workflow_queue("queue-a", None, None);
        let activity_queue = QueueKey {
            task_kind: TaskKind::Activity,
            ..workflow_queue.clone()
        };
        let activity = activity_task(activity_queue.clone());

        workflow_broker
            .deny_worker(
                workflow_queue.namespace_id,
                workflow_queue.task_queue,
                WorkerIdentity("worker-a".to_string()),
            )
            .await;
        activity_broker
            .publish_activity_task(activity.clone(), None)
            .await
            .unwrap();

        let delivered = activity_broker
            .poll_activity_task(&activity_queue, std::time::Duration::from_millis(5))
            .await
            .unwrap();

        assert_eq!(delivered.map(|entry| entry.0), Some(activity));
    }

    #[tokio::test]
    async fn expired_workflow_tasks_move_out_of_live_ready() {
        let broker = InMemoryBroker::default();
        let queue = workflow_queue("queue-a", None, None);
        let task = workflow_task(queue);

        broker.publish_workflow_task(task.clone(), None).await;
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        let expired = broker
            .take_expired(std::time::Duration::from_millis(1))
            .await;
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].task, task);
    }

    #[tokio::test]
    async fn sticky_promotion_preserves_original_entry_timestamp() {
        let broker = InMemoryBroker::default();
        let queue = workflow_queue("queue-a", None, None);
        let task = DispatchableWorkflowTask {
            sticky_preferred: Some(WorkerIdentity("worker-a".to_string())),
            sticky_expires_at: Some(OffsetDateTime::UNIX_EPOCH),
            ..workflow_task(queue.clone())
        };

        broker.publish_workflow_task(task.clone(), None).await;
        tokio::time::sleep(std::time::Duration::from_millis(3)).await;
        {
            let mut inner = broker.inner.lock().await;
            let sticky = inner.sticky_ready.get_mut(&queue).unwrap();
            let mut entry = sticky.pop_front().unwrap();
            entry.task.sticky_preferred = None;
            entry.task.sticky_expires_at = None;
            inner
                .general_ready
                .entry(queue.clone())
                .or_default()
                .push_back(entry);
        }

        tokio::time::sleep(std::time::Duration::from_millis(3)).await;
        let expired = broker
            .take_expired(std::time::Duration::from_millis(5))
            .await;
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].task.run_key, task.run_key);
        assert_eq!(expired[0].task.logical_seq, task.logical_seq);
    }

    #[tokio::test]
    async fn waiter_visibility_tracks_active_polls() {
        let broker = InMemoryActivityBroker::default();
        let queue = activity_queue("queue-a");
        let broker_clone = broker.clone();
        let queue_clone = queue.clone();
        let handle = tokio::spawn(async move {
            broker_clone
                .poll_activity_task(&queue_clone, std::time::Duration::from_millis(50))
                .await
                .unwrap()
        });

        tokio::task::yield_now().await;
        let waiting = broker.queues_with_waiters().await;
        assert!(waiting.contains(&queue));

        let _ = handle.await.unwrap();
        let waiting = broker.queues_with_waiters().await;
        assert!(!waiting.contains(&queue));
    }

    #[tokio::test]
    async fn query_tasks_bypass_dedup_and_all_deliver() {
        let broker = InMemoryBroker::default();
        let queue = workflow_queue("queue-a", None, None);

        for query_type in ["q1", "q2"] {
            let (tx, _rx) = oneshot::channel();
            broker
                .publish_query_task(QueryTask {
                    run_key: RunKey::new(),
                    query_type: query_type.to_string(),
                    query_args: Payloads::default(),
                    queue: queue.clone(),
                    sticky_preferred: None,
                    response_tx: tx,
                })
                .await;
        }

        let first = broker
            .poll_query_task(
                &queue,
                &WorkerIdentity("worker-a".into()),
                std::time::Duration::from_millis(5),
            )
            .await
            .expect("first query should deliver");
        let second = broker
            .poll_query_task(
                &queue,
                &WorkerIdentity("worker-a".into()),
                std::time::Duration::from_millis(5),
            )
            .await
            .expect("second query should deliver");

        assert_eq!(first.query_type, "q1");
        assert_eq!(second.query_type, "q2");
    }

    #[tokio::test]
    async fn query_poll_prefers_matching_sticky_worker() {
        let broker = InMemoryBroker::default();
        let queue = workflow_queue("queue-a", None, None);
        let (sticky_tx, _sticky_rx) = oneshot::channel();
        broker
            .publish_query_task(QueryTask {
                run_key: RunKey::new(),
                query_type: "sticky".into(),
                query_args: Payloads::default(),
                queue: queue.clone(),
                sticky_preferred: Some(WorkerIdentity("worker-a".into())),
                response_tx: sticky_tx,
            })
            .await;
        let (general_tx, _general_rx) = oneshot::channel();
        broker
            .publish_query_task(QueryTask {
                run_key: RunKey::new(),
                query_type: "general".into(),
                query_args: Payloads::default(),
                queue: queue.clone(),
                sticky_preferred: None,
                response_tx: general_tx,
            })
            .await;

        let wrong_worker = broker
            .poll_query_task(
                &queue,
                &WorkerIdentity("worker-b".into()),
                std::time::Duration::from_millis(5),
            )
            .await
            .expect("general query should still deliver");
        let sticky_worker = broker
            .poll_query_task(
                &queue,
                &WorkerIdentity("worker-a".into()),
                std::time::Duration::from_millis(5),
            )
            .await
            .expect("sticky query should deliver to matching worker");

        assert_eq!(wrong_worker.query_type, "general");
        assert_eq!(sticky_worker.query_type, "sticky");
    }

    proptest! {
        #[test]
        fn property_publish_records_entry_timestamp(is_activity in any::<bool>()) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                let before = Instant::now();
                if is_activity {
                    let broker = InMemoryActivityBroker::default();
                    let queue = activity_queue("queue-a");
                    broker.publish_activity_task(activity_task(queue.clone()), None).await.unwrap();
                    let inner = broker.inner.lock().await;
                    let entry = inner.ready.get(&queue).and_then(|q| q.front()).unwrap();
                    prop_assert!(entry.entered_at >= before);
                } else {
                    let broker = InMemoryBroker::default();
                    let queue = workflow_queue("queue-a", None, None);
                    broker.publish_workflow_task(workflow_task(queue.clone()), None).await;
                    let inner = broker.inner.lock().await;
                    let entry = inner.general_ready.get(&queue).and_then(|q| q.front()).unwrap();
                    prop_assert!(entry.entered_at >= before);
                }
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }

    #[tokio::test]
    async fn property_sticky_promotion_preserves_original_timestamp() {
        let broker = InMemoryBroker::default();
        let queue = workflow_queue("queue-a", None, None);
        let task = DispatchableWorkflowTask {
            sticky_preferred: Some(WorkerIdentity("worker-a".into())),
            sticky_expires_at: Some(OffsetDateTime::UNIX_EPOCH),
            ..workflow_task(queue.clone())
        };

        broker.publish_workflow_task(task, None).await;

        let original = {
            let inner = broker.inner.lock().await;
            inner
                .sticky_ready
                .get(&queue)
                .and_then(|q| q.front())
                .map(|entry| entry.entered_at)
                .unwrap()
        };

        {
            let mut inner = broker.inner.lock().await;
            let sticky = inner.sticky_ready.get_mut(&queue).unwrap();
            let mut entry = sticky.pop_front().unwrap();
            entry.task.sticky_preferred = None;
            entry.task.sticky_expires_at = None;
            inner
                .general_ready
                .entry(queue.clone())
                .or_default()
                .push_back(entry);
        }

        let after = {
            let inner = broker.inner.lock().await;
            inner
                .general_ready
                .get(&queue)
                .and_then(|q| q.front())
                .map(|entry| entry.entered_at)
                .unwrap()
        };

        assert_eq!(after, original);
    }

    proptest! {
        #[test]
        fn property_dedup_prevents_double_dispatch(seq in 1u64..8u64) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                let broker = InMemoryBroker::default();
                let queue = workflow_queue("queue-a", None, None);
                let task = DispatchableWorkflowTask {
                    logical_seq: LogicalTaskSeq(seq),
                    ..workflow_task(queue.clone())
                };

                broker.publish_workflow_task(task.clone(), None).await;
                broker.publish_workflow_task(task.clone(), None).await;

                let first = broker
                    .poll_workflow_task(
                        &queue,
                        &WorkerIdentity("worker-a".into()),
                        std::time::Duration::from_millis(1),
                    )
                    .await
                    .unwrap();
                let second = broker
                    .poll_workflow_task(
                        &queue,
                        &WorkerIdentity("worker-a".into()),
                        std::time::Duration::from_millis(1),
                    )
                    .await
                    .unwrap();

                prop_assert_eq!(first.map(|entry| entry.0), Some(task));
                prop_assert_eq!(second, None);
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }
}
