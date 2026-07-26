//! In-memory delivery brokers for workflow, activity, and query work.
//!
//! These brokers are intentionally transport-local. Pollers should not become
//! durable storage objects, and queue fairness/sticky behavior should be
//! understandable without reading storage code. When work must survive process
//! loss or prolonged absence of pollers, that responsibility moves to the
//! durable backlog and scanner paths elsewhere in the runtime.
//!
//! ## Sticky / live / backlog tier model
//!
//! Workflow tasks and direct query tasks share the same poll wake path because
//! Temporal SDKs only poll `PollWorkflowTaskQueue` for both surfaces
//! (`service/matching/matching_engine.go:1084 @ v1.31.0`). Work enters a
//! *sticky* tier only when that worker has polled recently or is actively
//! parked. Otherwise publication immediately selects the supplied normal-queue
//! fallback. A pending WFT deadline is a timeout fence, not an affinity expiry;
//! durable backlog scanning remains outside this broker.
//!
//! ## Deduplication
//!
//! Each workflow task is keyed by `(RunKey, LogicalTaskSeq)` and each activity
//! task by `(RunKey, activity_id, attempt)`. Duplicate publications are
//! silently suppressed so that scanner sweeps and retry paths can safely
//! re-publish without creating phantom work items.
//!
//! ## Per-queue wake pattern
//!
//! Wakeups are scoped to the **queue**, not the whole broker. Each queue has its
//! own `Notify`; a `publish` wakes only pollers parked on that queue. This is a
//! correctness property, not an optimisation: the broker is a *derived* delivery
//! index over the authoritative transition log (see the type docs below), so a
//! poll must reflect readiness of *its own* queue and nothing else. A global
//! wake would let traffic on one queue end an unrelated poll empty, and would
//! wake every idle poller on every publish.
//!
//! Two invariants the poll loops enforce:
//!
//! 1. **A wake is a hint to re-check, not a result.** On every wake a poller
//!    re-derives readiness via `try_take` and keeps waiting until its deadline if
//!    there is still nothing for it, returning empty only at the deadline. A
//!    spurious wake therefore never produces a premature empty poll.
//! 2. **Register before re-check (TOCTOU).** The per-queue `notified` future is
//!    `enable()`d *before* the `try_take` re-check, so a `publish` racing the
//!    check still wakes the poller. `notify_waiters` only signals already-enabled
//!    waiters, so the `enable()` is what makes the race-close sound.

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    sync::{Arc, RwLock as StdRwLock},
};

use anyhow::Result;
use time::OffsetDateTime;
use tokeira_storage::{DeliveryOrder, DispatchableActivityTask, DispatchableWorkflowTask};
use tokeira_types::{LogicalTaskSeq, NamespaceId, QueueKey, RunKey, TaskQueueName, WorkerIdentity};
use tokio::{
    sync::{Mutex, Notify, oneshot},
    time::{Duration, Instant},
};

use crate::{
    DeliveryMetrics, DeliveryModeProvider, DeliveryOrdering, DispatchEligibility,
    DispatchRateLimits, InMemoryTaskQueueConfigStore, QueryResult, QueryTask, StartedWorkflowTask,
    StockDeliveryModeProvider, TaskQueueConfigEntry, TaskQueueConfigKey, TaskQueueConfigKind,
    TaskQueueConfigStore, effective_priority, metrics as runtime_metrics,
};

const STICKY_POLLER_AVAILABILITY_WINDOW: Duration = Duration::from_secs(10);

/// Lightweight in-memory workflow-task broker.
///
/// The broker exists so pollers do not become durable objects. The interesting
/// thing here is not the data structure sophistication, but the contract:
/// - worker polls stay memory-only,
/// - sticky preference is honored when possible,
/// - unavailable sticky hints fall back atomically at publication,
/// - duplicate publications are suppressed by logical task identity.
#[derive(Clone)]
pub struct InMemoryBroker {
    inner: Arc<Mutex<BrokerState>>,
    policy: Arc<BrokerPolicy>,
}

// Manual impl: summarizes without taking the interior lock — a `Debug` that
// must lock the state invites deadlock from inside failure paths.
impl std::fmt::Debug for InMemoryBroker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryBroker").finish_non_exhaustive()
    }
}

/// In-memory activity-task broker.
///
/// Mirrors [`InMemoryBroker`] but for activity tasks.
/// Deduplication is keyed on `(run_key, activity_id, attempt)`.
#[derive(Clone)]
pub struct InMemoryActivityBroker {
    inner: Arc<Mutex<ActivityBrokerState>>,
    policy: Arc<BrokerPolicy>,
}

// Manual impl: summarizes without taking the interior lock — a `Debug` that
// must lock the state invites deadlock from inside failure paths.
impl std::fmt::Debug for InMemoryActivityBroker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryActivityBroker")
            .finish_non_exhaustive()
    }
}

/// Live backlog shape used by matching diagnostics and poller scaling.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BrokerBacklogStats {
    /// Number of runnable tasks currently held by the disposable broker.
    pub count: usize,
    /// Age of the oldest runnable task, or zero for an empty queue.
    pub oldest_age: Duration,
}

/// Live broker backlog grouped by effective priority band.
pub type PriorityBacklogStats = BTreeMap<i32, BrokerBacklogStats>;

fn add_priority_stat(stats: &mut PriorityBacklogStats, priority_key: i32, age: Duration) {
    stats
        .entry(priority_key)
        .and_modify(|band| {
            band.count += 1;
            band.oldest_age = band.oldest_age.max(age);
        })
        .or_insert(BrokerBacklogStats {
            count: 1,
            oldest_age: age,
        });
}

fn aggregate_priority_stats(stats: &PriorityBacklogStats) -> BrokerBacklogStats {
    stats
        .values()
        .fold(BrokerBacklogStats::default(), |mut aggregate, band| {
            aggregate.count += band.count;
            aggregate.oldest_age = aggregate.oldest_age.max(band.oldest_age);
            aggregate
        })
}

pub(crate) fn merge_priority_stats(
    target: &mut PriorityBacklogStats,
    source: PriorityBacklogStats,
) {
    for (priority_key, band) in source {
        target
            .entry(priority_key)
            .and_modify(|current| {
                current.count += band.count;
                current.oldest_age = current.oldest_age.max(band.oldest_age);
            })
            .or_insert(band);
    }
}

struct ActivityBrokerState {
    ready: HashMap<QueueKey, OrderedReady<TimestampedActivityTask>>,
    enqueued: HashSet<(RunKey, String, u32, u64)>,
    ordering: DeliveryOrdering,
    waiter_counts: HashMap<QueueKey, usize>,
    /// Per-queue wake handles (see the module's "Per-queue wake pattern").
    wakes: HashMap<QueueKey, Arc<Notify>>,
    denied_workers: HashSet<(NamespaceId, TaskQueueName, WorkerIdentity)>,
    rate_origin: Instant,
    rate_limits: DispatchRateLimits,
}

impl Default for ActivityBrokerState {
    fn default() -> Self {
        Self {
            ready: HashMap::new(),
            enqueued: HashSet::new(),
            ordering: DeliveryOrdering::default(),
            waiter_counts: HashMap::new(),
            wakes: HashMap::new(),
            denied_workers: HashSet::new(),
            rate_origin: Instant::now(),
            rate_limits: DispatchRateLimits::default(),
        }
    }
}

#[derive(Default)]
struct BrokerState {
    sticky_ready: HashMap<QueueKey, OrderedReady<TimestampedWorkflowTask>>,
    general_ready: HashMap<QueueKey, OrderedReady<TimestampedWorkflowTask>>,
    enqueued: HashSet<(RunKey, LogicalTaskSeq)>,
    ordering: DeliveryOrdering,
    waiter_counts: HashMap<QueueKey, usize>,
    /// Normal queue → sticky queues whose parked pollers may also consume it.
    normal_alias_wakes: HashMap<QueueKey, HashSet<QueueKey>>,
    /// Parked sticky polls counted as demand on their declared normal queue.
    normal_alias_waiter_counts: HashMap<QueueKey, usize>,
    workflow_waiters: HashMap<QueueKey, VecDeque<WorkflowWaiter>>,
    next_workflow_waiter_id: u64,
    query_ready: HashMap<QueueKey, VecDeque<QueryTask>>,
    query_waiter_counts: HashMap<QueueKey, usize>,
    denied_workers: HashSet<(NamespaceId, TaskQueueName, WorkerIdentity)>,
    poller_observations: HashMap<(QueueKey, WorkerIdentity), Instant>,
    /// Per-queue wake handles, created on first use and shared by workflow and
    /// query pollers on that queue (they share the poll path). Grows with
    /// distinct queues seen, like the ready/waiter maps; the broker is
    /// process-local and disposable, so this is bounded by live queues.
    wakes: HashMap<QueueKey, Arc<Notify>>,
}

struct BrokerPolicy {
    config_store: StdRwLock<Arc<dyn TaskQueueConfigStore>>,
    mode_provider: StdRwLock<Arc<dyn DeliveryModeProvider>>,
}

impl Default for BrokerPolicy {
    fn default() -> Self {
        Self {
            config_store: StdRwLock::new(Arc::new(InMemoryTaskQueueConfigStore::default())),
            mode_provider: StdRwLock::new(Arc::new(StockDeliveryModeProvider)),
        }
    }
}

impl BrokerPolicy {
    fn delivery_inputs(
        &self,
        queue: &QueueKey,
    ) -> (crate::DeliveryMode, u64, Option<TaskQueueConfigEntry>) {
        let provider = self
            .mode_provider
            .read()
            .expect("delivery mode provider lock poisoned");
        let mode = provider.mode_for(queue);
        let scope_generation = provider.scope_generation();
        let config = self
            .config_store
            .read()
            .expect("task queue config store lock poisoned")
            .get(&TaskQueueConfigKey {
                namespace_id: queue.namespace_id,
                task_queue: queue.task_queue.clone(),
                kind: match queue.task_kind {
                    tokeira_types::TaskKind::Workflow => TaskQueueConfigKind::Workflow,
                    tokeira_types::TaskKind::Activity => TaskQueueConfigKind::Activity,
                },
            });
        (mode, scope_generation, config)
    }

    fn set_config_store(&self, store: Arc<dyn TaskQueueConfigStore>) {
        *self
            .config_store
            .write()
            .expect("task queue config store lock poisoned") = store;
    }

    fn config_store(&self) -> Arc<dyn TaskQueueConfigStore> {
        self.config_store
            .read()
            .expect("task queue config store lock poisoned")
            .clone()
    }
}

impl Default for InMemoryBroker {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(BrokerState::default())),
            policy: Arc::new(BrokerPolicy::default()),
        }
    }
}

impl Default for InMemoryActivityBroker {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(ActivityBrokerState::default())),
            policy: Arc::new(BrokerPolicy::default()),
        }
    }
}

/// Ready tasks indexed by runtime-assigned delivery order.
///
/// The collision bucket is a correctness guard for best-effort order rebuilt
/// after independent process loss: equal disposable order values must never
/// cause one logical task to overwrite another.
#[derive(Debug)]
struct OrderedReady<T> {
    entries: BTreeMap<DeliveryOrder, VecDeque<T>>,
    len: usize,
}

fn prefer_sticky_candidate(sticky_priority: Option<i16>, normal_priority: Option<i16>) -> bool {
    sticky_priority.is_some_and(|sticky| normal_priority.is_none_or(|normal| sticky <= normal))
}

impl<T> Default for OrderedReady<T> {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            len: 0,
        }
    }
}

impl<T> OrderedReady<T> {
    fn len(&self) -> usize {
        self.len
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn insert(&mut self, order: DeliveryOrder, value: T) {
        self.entries.entry(order).or_default().push_back(value);
        self.len += 1;
    }

    fn front(&self) -> Option<&T> {
        self.entries
            .first_key_value()
            .and_then(|(_, bucket)| bucket.front())
    }

    fn pop_front(&mut self) -> Option<T> {
        let order = *self.entries.first_key_value()?.0;
        let bucket = self
            .entries
            .get_mut(&order)
            .expect("first delivery-order bucket exists");
        let value = bucket.pop_front();
        if value.is_some() {
            self.len -= 1;
        }
        if bucket.is_empty() {
            self.entries.remove(&order);
        }
        value
    }

    fn iter(&self) -> impl Iterator<Item = &T> {
        self.entries.values().flat_map(VecDeque::iter)
    }

    fn iter_mut(&mut self) -> impl Iterator<Item = &mut T> {
        self.entries.values_mut().flat_map(VecDeque::iter_mut)
    }

    fn remove_where(&mut self, predicate: impl Fn(&T) -> bool) -> Option<T> {
        let order = self
            .entries
            .iter()
            .find_map(|(order, bucket)| bucket.iter().any(&predicate).then_some(*order))?;
        let bucket = self
            .entries
            .get_mut(&order)
            .expect("selected delivery-order bucket exists");
        let index = bucket
            .iter()
            .position(predicate)
            .expect("selected bucket contains matching task");
        let value = bucket.remove(index);
        if value.is_some() {
            self.len -= 1;
        }
        if bucket.is_empty() {
            self.entries.remove(&order);
        }
        value
    }

    fn retain(&mut self, mut keep: impl FnMut(&T) -> bool) {
        let mut retained = 0;
        self.entries.retain(|_, bucket| {
            bucket.retain(|value| {
                let should_keep = keep(value);
                retained += usize::from(should_keep);
                should_keep
            });
            !bucket.is_empty()
        });
        self.len = retained;
    }

    fn append(&mut self, other: &mut Self) {
        for (order, mut bucket) in std::mem::take(&mut other.entries) {
            self.len += bucket.len();
            self.entries.entry(order).or_default().append(&mut bucket);
        }
        other.len = 0;
    }
}

#[derive(Debug)]
struct WorkflowWaiter {
    id: u64,
    worker: WorkerIdentity,
    response_tx: oneshot::Sender<Result<Option<StartedWorkflowTask>>>,
}

/// A workflow waiter that has been pulled off the wait queue so a producer can
/// hand it a task directly (synchronous match), bypassing the ready queues.
///
/// Holding a `ReservedPoller` is a claim on that specific parked poll: if the
/// producer cannot ultimately deliver, it must hand the reservation back via
/// [`InMemoryBroker::return_reserved_poller`] so the poller is not stranded.
#[derive(Debug)]
pub struct ReservedPoller {
    queue: QueueKey,
    worker_identity: WorkerIdentity,
    response_tx: oneshot::Sender<Result<Option<StartedWorkflowTask>>>,
}

impl ReservedPoller {
    pub fn worker_identity(&self) -> &WorkerIdentity {
        &self.worker_identity
    }

    /// Deliver a started task to the reserved poller. Returns `false` if the
    /// poller already went away (timed out or cancelled), so the caller can
    /// re-route the task instead of losing it.
    pub fn deliver(self, task: StartedWorkflowTask) -> bool {
        self.response_tx.send(Ok(Some(task))).is_ok()
    }
}

/// Outcome of a workflow poll.
///
/// `Queued` means a dispatchable task was taken from a ready queue and the
/// caller must drive it to a started state. `Started` means the task was handed
/// over already-started through the synchronous reservation path
/// ([`ReservedPoller`]), so no further start work is needed.
#[derive(Debug)]
pub enum WorkflowPollResult {
    Queued(DispatchableWorkflowTask, Instant),
    Started(StartedWorkflowTask),
    Query(QueryTask),
}

impl PartialEq for WorkflowPollResult {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Queued(left, _), Self::Queued(right, _)) => left == right,
            (Self::Started(left), Self::Started(right)) => left == right,
            (Self::Query(left), Self::Query(right)) => {
                // The waiter channel is transport state, not delivery identity.
                // Equality exists for broker tests that compare poll outcomes;
                // matching correctness is determined by the query metadata.
                left.run_key == right.run_key
                    && left.query_type == right.query_type
                    && left.query_args == right.query_args
                    && left.queue == right.queue
                    && left.sticky_preferred == right.sticky_preferred
                    && left.sticky_deadline == right.sticky_deadline
            }
            _ => false,
        }
    }
}

impl WorkflowPollResult {
    pub fn into_queued(self) -> Option<(DispatchableWorkflowTask, Instant)> {
        match self {
            Self::Queued(task, entered_at) => Some((task, entered_at)),
            Self::Started(_) | Self::Query(_) => None,
        }
    }

    pub fn queued_task(&self) -> Option<&DispatchableWorkflowTask> {
        match self {
            Self::Queued(task, _) => Some(task),
            Self::Started(_) | Self::Query(_) => None,
        }
    }
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

enum ActivityTakeOutcome {
    Ready((DispatchableActivityTask, Instant)),
    WaitUntil(Instant),
    Blocked,
    Empty,
}

impl InMemoryBroker {
    /// Share the runtime's live task-queue configuration store with this broker.
    ///
    /// Existing clones observe the replacement because construction-time
    /// wiring updates the shared policy handle, not one broker facade.
    pub fn set_task_queue_config_store(&self, store: Arc<dyn TaskQueueConfigStore>) {
        self.policy.set_config_store(store);
    }

    fn emit_queue_depths(inner: &BrokerState, queue: &QueueKey) {
        let sticky = inner
            .sticky_ready
            .get(queue)
            .map(|entries| entries.len())
            .unwrap_or(0);
        let general = inner
            .general_ready
            .get(queue)
            .map(|entries| entries.len())
            .unwrap_or(0);
        runtime_metrics::set_queue_depth(queue, "sticky", sticky);
        runtime_metrics::set_queue_depth(queue, "general", general);
    }

    /// Whether a workflow poll that just claimed one task left runnable work
    /// behind on the same physical queue.
    ///
    /// v1.31.0 recommends one additional poll when either aged backlog or the
    /// root queue's add/dispatch-rate pressure is positive
    /// (`physical_task_queue_manager.go:840-889 @ v1.31.0`). Tokeira's
    /// disposable broker has no sampled rate window, so remaining runnable
    /// work is the live pressure signal used for that same `+1` recommendation.
    pub async fn has_runnable_backlog(&self, queue: &QueueKey) -> bool {
        let inner = self.inner.lock().await;
        inner
            .sticky_ready
            .get(queue)
            .is_some_and(|ready| !ready.is_empty())
            || inner
                .general_ready
                .get(queue)
                .is_some_and(|ready| !ready.is_empty())
    }

    /// Snapshot non-sticky workflow backlog for `DescribeTaskQueue`.
    /// Sticky work is intentionally excluded by the public field contract
    /// (`proto/upstream/temporal/api/taskqueue/v1/message.proto:101-115`).
    pub async fn backlog_stats(&self, queue: &QueueKey) -> BrokerBacklogStats {
        aggregate_priority_stats(&self.backlog_stats_by_priority(queue).await)
    }

    /// Snapshot non-sticky workflow backlog by effective priority band.
    pub async fn backlog_stats_by_priority(&self, queue: &QueueKey) -> PriorityBacklogStats {
        let inner = self.inner.lock().await;
        let Some(ready) = inner.general_ready.get(queue) else {
            return PriorityBacklogStats::new();
        };
        let mut stats = PriorityBacklogStats::new();
        for task in ready.iter() {
            add_priority_stat(
                &mut stats,
                task.task
                    .order
                    .map_or(3, |order| i32::from(order.priority_key)),
                task.entered_at.elapsed(),
            );
        }
        stats
    }

    /// Snapshot a directly addressed sticky workflow backlog by priority band.
    ///
    /// Normal queue statistics deliberately exclude sticky work, but Temporal's
    /// internal partition description can address a sticky queue by name. Keeping
    /// this observation separate prevents that conformance/admin read from changing
    /// the public normal-queue scaling statistic.
    pub async fn sticky_backlog_stats_by_priority(&self, queue: &QueueKey) -> PriorityBacklogStats {
        let inner = self.inner.lock().await;
        let Some(ready) = inner.sticky_ready.get(queue) else {
            return PriorityBacklogStats::new();
        };
        let mut stats = PriorityBacklogStats::new();
        for task in ready.iter() {
            add_priority_stat(
                &mut stats,
                task.task
                    .order
                    .map_or(3, |order| i32::from(order.priority_key)),
                task.entered_at.elapsed(),
            );
        }
        stats
    }

    /// Move already-ready unversioned work onto a newly promoted deployment
    /// queue and wake pollers parked there.
    ///
    /// Temporal matching lets a Current (or 100% Ramping) version consume the
    /// unversioned backlog that existed before promotion. Rekeying disposable
    /// broker entries preserves that observable routing without changing the
    /// authoritative pending workflow task.
    pub async fn promote_unversioned_backlog(&self, target: &QueueKey) {
        let source = QueueKey {
            namespace_id: target.namespace_id,
            task_queue: target.task_queue.clone(),
            task_kind: target.task_kind,
            deployment: None,
            build_id: None,
        };
        let mut inner = self.inner.lock().await;
        let mut moved = inner.general_ready.remove(&source).unwrap_or_default();
        if moved.is_empty() {
            return;
        }
        for entry in moved.iter_mut() {
            entry.task.queue = target.clone();
        }
        inner
            .general_ready
            .entry(target.clone())
            .or_default()
            .append(&mut moved);
        Self::emit_queue_depths(&inner, &source);
        Self::emit_queue_depths(&inner, target);
        let wake = inner.wakes.entry(target.clone()).or_default().clone();
        drop(inner);
        wake.notify_waiters();
    }

    /// Take a specific run's task out of the general tier by run key, if present.
    ///
    /// Used to pull a task for direct/eager dispatch to a run the caller is
    /// already handling, rather than letting it flow through the normal poll
    /// path. Only the general tier is searched — sticky tasks are owned by their
    /// preferred worker and must not be claimed out from under it here.
    pub async fn try_claim_workflow_task(
        &self,
        queue: &QueueKey,
        run_key: RunKey,
    ) -> Option<(DispatchableWorkflowTask, Instant)> {
        self.try_claim_workflow_task_for_worker(queue, run_key, None)
            .await
    }

    /// Take a specific run's task for direct/eager dispatch, allowing a sticky
    /// claim only when the caller is the sticky owner.
    ///
    /// Eager WFT return is allowed to bypass a normal poll only when it can
    /// prove the returned task belongs to the completing worker. That keeps
    /// sticky cache ownership intact while still letting non-sticky tasks take
    /// the existing direct-claim path.
    pub async fn try_claim_workflow_task_for_worker(
        &self,
        queue: &QueueKey,
        run_key: RunKey,
        worker: Option<&WorkerIdentity>,
    ) -> Option<(DispatchableWorkflowTask, Instant)> {
        let mut inner = self.inner.lock().await;
        if let Some(worker) = worker
            && inner.denied_workers.contains(&(
                queue.namespace_id,
                queue.task_queue.clone(),
                worker.clone(),
            ))
        {
            return None;
        }

        let matched = inner.sticky_ready.get_mut(queue).and_then(|sticky| {
            sticky.remove_where(|task| {
                task.task.run_key == run_key
                    && worker.is_some()
                    && task.task.sticky_preferred.as_ref() == worker
            })
        });

        let matched = matched.or_else(|| {
            let ready = inner.general_ready.get_mut(queue)?;
            ready.remove_where(|task| task.task.run_key == run_key)
        });

        if let Some(removed) = matched {
            inner
                .enqueued
                .remove(&(removed.task.run_key, removed.task.logical_seq));
            Self::emit_queue_depths(&inner, queue);
            return Some((removed.task, removed.entered_at));
        }

        Self::emit_queue_depths(&inner, queue);
        None
    }

    /// Publish a query task without deduplication or backlog participation.
    pub async fn publish_query_task(&self, mut task: QueryTask) {
        let queue = task.queue.clone();
        let sticky_queue = task
            .sticky_queue
            .as_ref()
            .map(|sticky_queue| QueueKey {
                task_queue: sticky_queue.clone(),
                ..queue.clone()
            })
            .or_else(|| task.sticky_preferred.as_ref().map(|_| queue.clone()));
        let mut inner = self.inner.lock().await;
        // Matching returns StickyWorkerUnavailable immediately when the
        // targeted sticky worker has no active/recent poll, so history can
        // retry the query on the normal queue without consuming the entire
        // sticky S2S window (`matching_engine.go:1093-1099` and
        // `queryworkflow/api.go:350-410 @ v1.31.0`). This is transient routing
        // only: durable affinity is left for the normal fallback start/query
        // lifecycle to resolve.
        if let (Some(sticky_queue), Some(worker)) =
            (sticky_queue.as_ref(), task.sticky_preferred.as_ref())
            && !Self::sticky_worker_available(&inner, sticky_queue, worker)
        {
            task.sticky_preferred = None;
            task.sticky_queue = None;
            task.sticky_deadline = None;
        }
        let wake_sticky = task.sticky_preferred.is_some();
        inner
            .query_ready
            .entry(queue.clone())
            .or_default()
            .push_back(task);
        // The task stays indexed by its normal queue for fallback, but a live
        // sticky poll is parked on the SDK-generated sticky queue. Wake both
        // observations so the sticky-first attempt does not wait for an
        // unrelated event (`queryworkflow/api.go:350-410 @ v1.31.0`).
        let normal_wake = inner.wakes.entry(queue.clone()).or_default().clone();
        let sticky_wake = wake_sticky
            .then_some(())
            .and(sticky_queue.as_ref())
            .cloned()
            .filter(|sticky| sticky != &queue)
            .map(|sticky| inner.wakes.entry(sticky).or_default().clone());
        drop(inner);
        normal_wake.notify_waiters();
        if let Some(wake) = sticky_wake {
            wake.notify_waiters();
        }
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
        let wake = self.queue_wake(queue).await;
        let deadline = Instant::now() + wait_for;

        let result = loop {
            // Enable before the re-check so a publish racing it still wakes us.
            let notified = wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if let Some(task) = self.try_take_query(queue, worker).await {
                break Some(task);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break None;
            }
            // A sticky-only query becomes takeable once its sticky window closes;
            // bound the wait by that so we re-check then even with no new publish.
            let wait = match self.query_fallback_wait(queue).await {
                Some(fallback) if !fallback.is_zero() => fallback.min(remaining),
                _ => remaining,
            };
            tokio::select! {
                _ = notified.as_mut() => {}
                _ = tokio::time::sleep(wait) => {}
            }
        };

        self.decrement_query_waiter(queue).await;
        result
    }

    /// Long-poll for the next workflow activation on `queue`.
    ///
    /// This is the Temporal-compatible poll path: a worker polling
    /// `PollWorkflowTaskQueue` can receive either a history-advancing workflow
    /// task or a legacy direct query task. Workflow tasks still win when both
    /// are ready so state-changing progress is not delayed by read-only work.
    pub async fn poll_workflow_activation(
        &self,
        queue: &QueueKey,
        normal_queue: Option<&QueueKey>,
        worker: &WorkerIdentity,
        wait_for: Duration,
    ) -> Result<Option<WorkflowPollResult>> {
        self.poll_workflow_inner(queue, normal_queue, worker, wait_for, true)
            .await
    }

    /// Shared workflow-task long-poll loop (see the module's per-queue wake
    /// pattern). Holds until a task for `queue` is available or `wait_for`
    /// elapses; a wake on this queue is a hint to re-derive readiness, never a
    /// reason to return empty early. `include_query` mirrors the Temporal
    /// contract that `PollWorkflowTaskQueue` also satisfies a ready direct query
    /// (`service/matching/matching_engine.go:1084 @ v1.31.0`); the workflow-only
    /// internal path passes `false`.
    async fn poll_workflow_inner(
        &self,
        queue: &QueueKey,
        normal_queue: Option<&QueueKey>,
        worker: &WorkerIdentity,
        wait_for: Duration,
        include_query: bool,
    ) -> Result<Option<WorkflowPollResult>> {
        if !self.record_workflow_poller(queue, worker).await {
            return Ok(None);
        }
        if let Some(task) = self.try_take(queue, normal_queue, worker).await? {
            let _ = self.record_workflow_poller(queue, worker).await;
            return Ok(Some(WorkflowPollResult::Queued(task.0, task.1)));
        }
        if include_query && let Some(task) = self.try_take_query(queue, worker).await {
            let _ = self.record_workflow_poller(queue, worker).await;
            return Ok(Some(WorkflowPollResult::Query(task)));
        }

        // The waiter is the sync-match target (reserved-poller / eager hand-off
        // via `response_rx`); it stays registered for the whole call and is
        // removed once on exit.
        let deadline = Instant::now() + wait_for;
        let (response_tx, mut response_rx) = oneshot::channel();
        let waiter_id = self
            .insert_workflow_waiter(queue, normal_queue, worker.clone(), response_tx)
            .await;
        let wake = self.queue_wake(queue).await;

        let result = loop {
            // Enable before the re-check so a publish racing it still wakes us.
            let notified = wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if self.is_denied(queue, worker).await {
                break Ok(None);
            }
            if let Some(task) = self.try_take(queue, normal_queue, worker).await? {
                break Ok(Some(WorkflowPollResult::Queued(task.0, task.1)));
            }
            if include_query && let Some(task) = self.try_take_query(queue, worker).await {
                break Ok(Some(WorkflowPollResult::Query(task)));
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break Ok(None);
            }
            let wait = match (include_query, self.query_fallback_wait(queue).await) {
                (true, Some(fallback)) if !fallback.is_zero() => fallback.min(remaining),
                _ => remaining,
            };

            tokio::select! {
                response = &mut response_rx => {
                    break match response {
                        Ok(result) => result.map(|task| task.map(WorkflowPollResult::Started)),
                        Err(_) => Ok(None),
                    };
                }
                _ = notified.as_mut() => {}
                _ = tokio::time::sleep(wait) => {}
            }
        };

        self.remove_workflow_waiter(queue, normal_queue, waiter_id)
            .await;
        // Reaching this point is a normal completion or timeout. Client
        // cancellation drops the future before here, so it deliberately does
        // not refresh the admission observation
        // (`ctx.Err() != context.Canceled`,
        // task_queue_partition_manager.go:617-621 @ v1.31.0).
        let _ = self.record_workflow_poller(queue, worker).await;
        result
    }

    /// Enqueue a workflow task for delivery.
    ///
    /// Duplicate publications (same `run_key` + `logical_seq`)
    /// are silently suppressed. Sticky-preferred tasks are
    /// placed in the sticky tier; all others go to general.
    pub async fn publish_workflow_task(
        &self,
        mut task: DispatchableWorkflowTask,
        metrics: Option<&DeliveryMetrics>,
    ) {
        let (mode, scope_generation, config) = self.policy.delivery_inputs(&task.queue);
        let mut inner = self.inner.lock().await;
        inner.ordering.enter_scope(scope_generation);
        if task.sticky_preferred.is_some()
            && !Self::sticky_poller_available(&inner, &task)
            && let Some(normal_queue) = task.normal_queue.take()
        {
            task.queue = normal_queue;
            task.sticky_preferred = None;
            task.sticky_deadline = None;
        }
        runtime_metrics::record_broker_publish(&task.queue);
        let queue = task.queue.clone();
        let dedupe_key = (task.run_key, task.logical_seq);
        if !inner.enqueued.insert(dedupe_key) {
            return;
        }
        let is_sticky = task.sticky_preferred.is_some();
        let order = match task.order {
            Some(order) => inner.ordering.preserve(order),
            None => inner.ordering.assign(
                &task.queue,
                task.priority.as_ref(),
                is_sticky,
                config.as_ref(),
                mode,
            ),
        };
        task.order = Some(order);
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
                .insert(order, timestamped);
        } else {
            inner
                .general_ready
                .entry(timestamped.task.queue.clone())
                .or_default()
                .insert(order, timestamped);
        }
        Self::emit_queue_depths(&inner, &queue);
        let wake = inner.wakes.entry(queue.clone()).or_default().clone();
        let alias_wakes = inner
            .normal_alias_wakes
            .get(&queue)
            .into_iter()
            .flatten()
            .filter_map(|sticky| inner.wakes.get(sticky).cloned())
            .collect::<Vec<_>>();
        drop(inner);
        wake.notify_waiters();
        for alias_wake in alias_wakes {
            alias_wake.notify_waiters();
        }
    }

    fn sticky_poller_available(inner: &BrokerState, task: &DispatchableWorkflowTask) -> bool {
        let Some(worker) = task.sticky_preferred.as_ref() else {
            return false;
        };
        Self::sticky_worker_available(inner, &task.queue, worker)
    }

    fn sticky_worker_available(
        inner: &BrokerState,
        queue: &QueueKey,
        worker: &WorkerIdentity,
    ) -> bool {
        if inner.denied_workers.contains(&(
            queue.namespace_id,
            queue.task_queue.clone(),
            worker.clone(),
        )) {
            return false;
        }
        let active = inner.workflow_waiters.get(queue).is_some_and(|waiters| {
            waiters
                .iter()
                .any(|waiter| waiter.worker == *worker && !waiter.response_tx.is_closed())
        });
        active
            || inner
                .poller_observations
                .get(&(queue.clone(), worker.clone()))
                .is_some_and(|observed_at| {
                    observed_at.elapsed() <= STICKY_POLLER_AVAILABILITY_WINDOW
                })
    }

    async fn record_workflow_poller(&self, queue: &QueueKey, worker: &WorkerIdentity) -> bool {
        let mut inner = self.inner.lock().await;
        if inner.denied_workers.contains(&(
            queue.namespace_id,
            queue.task_queue.clone(),
            worker.clone(),
        )) {
            return false;
        }
        let now = Instant::now();
        inner.poller_observations.retain(|_, observed_at| {
            now.duration_since(*observed_at) <= STICKY_POLLER_AVAILABILITY_WINDOW
        });
        inner
            .poller_observations
            .insert((queue.clone(), worker.clone()), now);
        true
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
            .insert((namespace_id, task_queue.clone(), worker.clone()));
        inner.poller_observations.retain(|(queue, identity), _| {
            queue.namespace_id != namespace_id
                || queue.task_queue != task_queue
                || identity != &worker
        });
        // Deny carries only (namespace, task_queue, worker) — not a poller's full
        // QueueKey — and is a rare shutdown path, so wake every queue's waiters;
        // each re-checks `is_denied` on wake and the denied one returns empty.
        let wakes: Vec<Arc<Notify>> = inner.wakes.values().cloned().collect();
        drop(inner);
        for wake in wakes {
            wake.notify_waiters();
        }
    }

    /// Long-poll for a workflow task on `queue`.
    ///
    /// Returns immediately if a task is available, otherwise
    /// blocks up to `wait_for`. Sticky tasks on this queue are delivered only
    /// to their preferred worker; unavailable affinity was already redirected
    /// atomically when the task was published.
    pub async fn poll_workflow_task(
        &self,
        queue: &QueueKey,
        worker: &WorkerIdentity,
        wait_for: Duration,
    ) -> Result<Option<WorkflowPollResult>> {
        self.poll_workflow_inner(queue, None, worker, wait_for, false)
            .await
    }

    /// Pull one waiting poller off `queue`'s wait list for synchronous delivery.
    ///
    /// Skips waiters whose response channel has already closed (the poll timed
    /// out or was cancelled) so a producer never reserves a dead poller. Returns
    /// `None` when no live waiter is parked.
    pub async fn try_reserve_poller(&self, queue: &QueueKey) -> Option<ReservedPoller> {
        let mut inner = self.inner.lock().await;
        loop {
            let waiter = inner
                .workflow_waiters
                .get_mut(queue)
                .and_then(|waiters| waiters.pop_front())?;
            Self::decrement_waiter_count(&mut inner, queue);
            if waiter.response_tx.is_closed() {
                continue;
            }
            return Some(ReservedPoller {
                queue: queue.clone(),
                worker_identity: waiter.worker,
                response_tx: waiter.response_tx,
            });
        }
    }

    /// Return a previously reserved poller to the front of its wait list.
    ///
    /// Re-queued at the front (not the back) so a poller whose reservation could
    /// not be fulfilled keeps its place in line rather than being penalised for
    /// the failed synchronous-match attempt. A closed channel is dropped.
    pub async fn return_reserved_poller(&self, reserved: ReservedPoller) {
        if reserved.response_tx.is_closed() {
            return;
        }
        let mut inner = self.inner.lock().await;
        let id = inner.next_workflow_waiter_id;
        inner.next_workflow_waiter_id = inner.next_workflow_waiter_id.wrapping_add(1);
        inner
            .workflow_waiters
            .entry(reserved.queue.clone())
            .or_default()
            .push_front(WorkflowWaiter {
                id,
                worker: reserved.worker_identity,
                response_tx: reserved.response_tx,
            });
        *inner
            .waiter_counts
            .entry(reserved.queue.clone())
            .or_default() += 1;
        let wake = inner.wakes.entry(reserved.queue).or_default().clone();
        drop(inner);
        wake.notify_waiters();
    }

    /// Queues that currently have at least one parked poller.
    ///
    /// The backlog drain loop uses this to stay demand-driven — it only
    /// re-hydrates queues someone is actually waiting on.
    pub async fn queues_with_waiters(&self) -> HashSet<QueueKey> {
        self.inner
            .lock()
            .await
            .waiter_counts
            .iter()
            .filter_map(|(queue, count)| (*count > 0).then_some(queue.clone()))
            .collect()
    }

    /// Queues with parked pollers, with the parked-poller count for each.
    ///
    /// The drain loop treats each parked poller as one unit of live demand:
    /// the fairness budget is seeded from COMPLETED polls in the metrics
    /// window, so a poller that is still parked contributes nothing to it —
    /// without this count, a queue whose first poller arrived after its tasks
    /// aged into the backlog would sit at budget 0 forever (the poll can never
    /// complete without a drain, and the budget can never re-seed without a
    /// completed poll).
    pub async fn workflow_waiter_counts(&self) -> HashMap<QueueKey, u32> {
        let inner = self.inner.lock().await;
        let mut counts = inner
            .waiter_counts
            .iter()
            .filter(|(_, count)| **count > 0)
            .map(|(queue, count)| (queue.clone(), *count))
            .collect::<HashMap<_, _>>();
        for (queue, alias_count) in &inner.normal_alias_waiter_counts {
            *counts.entry(queue.clone()).or_default() += *alias_count;
        }
        counts
            .into_iter()
            .map(|(queue, count)| (queue, u32::try_from(count).unwrap_or(u32::MAX)))
            .collect()
    }

    async fn try_take_query(&self, queue: &QueueKey, worker: &WorkerIdentity) -> Option<QueryTask> {
        let mut inner = self.inner.lock().await;

        // Expire sticky preferences FIRST: past the sticky deadline v1.31.0
        // abandons the sticky attempt and re-dispatches on the normal queue
        // with full history (queryworkflow/api.go:355-385) — even the
        // preferred worker must then receive the task as a NON-sticky
        // delivery (its cache may be gone; empty history would strand it).
        let now = OffsetDateTime::now_utc();
        for ready in inner.query_ready.values_mut() {
            for task in ready.iter_mut() {
                if task.sticky_deadline.is_some_and(|deadline| deadline <= now) {
                    task.sticky_preferred = None;
                    task.sticky_queue = None;
                    task.sticky_deadline = None;
                }
            }
        }

        if let Some(ready) = inner.query_ready.get_mut(queue) {
            if let Some(idx) = ready.iter().position(|task| {
                task.sticky_preferred.as_ref() == Some(worker)
                    && task
                        .sticky_queue
                        .as_ref()
                        .is_none_or(|sticky| sticky == &queue.task_queue)
            }) {
                return ready.remove(idx);
            }

            if let Some(idx) = ready
                .iter()
                .position(|task| task.sticky_preferred.is_none())
            {
                return ready.remove(idx);
            }
        }

        // Sticky query tasks remain indexed by their normal queue. A poll on
        // the sticky queue may claim exactly the task carrying that sticky
        // queue name and worker identity, but cannot see siblings from another
        // namespace or deployment version.
        for (normal_queue, ready) in &mut inner.query_ready {
            if normal_queue.namespace_id != queue.namespace_id
                || normal_queue.deployment != queue.deployment
                || normal_queue.build_id != queue.build_id
            {
                continue;
            }
            if let Some(idx) = ready.iter().position(|task| {
                task.sticky_preferred.as_ref() == Some(worker)
                    && task.sticky_queue.as_ref() == Some(&queue.task_queue)
            }) {
                return ready.remove(idx);
            }
        }

        None
    }

    /// Return the next sticky-query fallback interval for `queue`.
    ///
    /// A query's sticky deadline is not durable state; it is the in-memory
    /// equivalent of Temporal's sticky query attempt context deadline
    /// (`service/history/api/queryworkflow/api.go:350-410 @ v1.31.0`). Pollers
    /// include this interval in their wait so a live worker can take the same
    /// query as soon as the sticky-only window expires, even if no new work is
    /// published to wake the broker.
    async fn query_fallback_wait(&self, queue: &QueueKey) -> Option<Duration> {
        let inner = self.inner.lock().await;
        let now = OffsetDateTime::now_utc();
        inner
            .query_ready
            .get(queue)?
            .iter()
            .filter(|task| task.sticky_preferred.is_some())
            .filter_map(|task| task.sticky_deadline)
            .min()
            .map(|deadline| {
                if deadline <= now {
                    Duration::ZERO
                } else {
                    (deadline - now).try_into().unwrap_or(Duration::ZERO)
                }
            })
    }

    /// Remove and return tasks that have sat in either tier longer than
    /// `grace_window`, clearing their dedup keys.
    ///
    /// This is the grace scanner's hook: expired tasks leave the in-memory
    /// broker here and are then persisted to the durable backlog. Clearing the
    /// dedup keys is what lets the same logical task be re-published later
    /// (e.g. when drained back from storage) without being suppressed.
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
        normal_queue: Option<&QueueKey>,
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
        let sticky_priority = inner.sticky_ready.get(queue).and_then(|sticky| {
            sticky
                .iter()
                .find(|task| task.task.sticky_preferred.as_ref() == Some(worker))
                .and_then(|task| task.task.order)
                .map(|order| order.priority_key)
        });
        let general_queue = normal_queue.unwrap_or(queue);
        let general_priority = inner
            .general_ready
            .get(general_queue)
            .and_then(OrderedReady::front)
            .and_then(|task| task.task.order)
            .map(|order| order.priority_key);
        let select_sticky = prefer_sticky_candidate(sticky_priority, general_priority);
        let task = if select_sticky {
            inner.sticky_ready.get_mut(queue).and_then(|sticky| {
                sticky.remove_where(|task| task.task.sticky_preferred.as_ref() == Some(worker))
            })
        } else {
            inner
                .general_ready
                .get_mut(general_queue)
                .and_then(OrderedReady::pop_front)
        };

        if let Some(task) = task {
            if let Some(order) = task.task.order {
                inner.ordering.served(&task.task.queue, order);
            }
            inner
                .enqueued
                .remove(&(task.task.run_key, task.task.logical_seq));
            Self::emit_queue_depths(&inner, queue);
            if general_queue != queue {
                Self::emit_queue_depths(&inner, general_queue);
            }
            return Ok(Some((task.task, task.entered_at)));
        }
        Self::emit_queue_depths(&inner, queue);
        if general_queue != queue {
            Self::emit_queue_depths(&inner, general_queue);
        }
        Ok(None)
    }

    async fn is_denied(&self, queue: &QueueKey, worker: &WorkerIdentity) -> bool {
        self.inner.lock().await.denied_workers.contains(&(
            queue.namespace_id,
            queue.task_queue.clone(),
            worker.clone(),
        ))
    }

    /// Whether worker shutdown currently fences this identity from the queue.
    pub async fn is_worker_denied(&self, queue: &QueueKey, worker: &WorkerIdentity) -> bool {
        self.is_denied(queue, worker).await
    }

    /// Per-queue wake handle, created on first use (see the module's "Per-queue
    /// wake pattern"). Wakeups are scoped to the queue so a publish elsewhere
    /// never disturbs — or empties — a poll on this one.
    async fn queue_wake(&self, queue: &QueueKey) -> Arc<Notify> {
        self.inner
            .lock()
            .await
            .wakes
            .entry(queue.clone())
            .or_default()
            .clone()
    }

    async fn insert_workflow_waiter(
        &self,
        queue: &QueueKey,
        normal_queue: Option<&QueueKey>,
        worker: WorkerIdentity,
        response_tx: oneshot::Sender<Result<Option<StartedWorkflowTask>>>,
    ) -> u64 {
        let mut inner = self.inner.lock().await;
        let id = inner.next_workflow_waiter_id;
        inner.next_workflow_waiter_id = inner.next_workflow_waiter_id.wrapping_add(1);
        inner
            .workflow_waiters
            .entry(queue.clone())
            .or_default()
            .push_back(WorkflowWaiter {
                id,
                worker,
                response_tx,
            });
        *inner.waiter_counts.entry(queue.clone()).or_default() += 1;
        if let Some(normal_queue) = normal_queue {
            inner
                .normal_alias_wakes
                .entry(normal_queue.clone())
                .or_default()
                .insert(queue.clone());
            *inner
                .normal_alias_waiter_counts
                .entry(normal_queue.clone())
                .or_default() += 1;
        }
        id
    }

    async fn remove_workflow_waiter(
        &self,
        queue: &QueueKey,
        normal_queue: Option<&QueueKey>,
        waiter_id: u64,
    ) -> bool {
        let mut inner = self.inner.lock().await;
        let removed = inner
            .workflow_waiters
            .get_mut(queue)
            .and_then(|waiters| {
                let index = waiters.iter().position(|waiter| waiter.id == waiter_id)?;
                waiters.remove(index)
            })
            .is_some();
        if removed {
            Self::decrement_waiter_count(&mut inner, queue);
            if let Some(normal_queue) = normal_queue
                && let Some(count) = inner.normal_alias_waiter_counts.get_mut(normal_queue)
            {
                *count -= 1;
                if *count == 0 {
                    inner.normal_alias_waiter_counts.remove(normal_queue);
                }
            }
        }
        removed
    }

    fn decrement_waiter_count(inner: &mut BrokerState, queue: &QueueKey) {
        if let Some(count) = inner.waiter_counts.get_mut(queue) {
            *count -= 1;
            if *count == 0 {
                inner.waiter_counts.remove(queue);
            }
        }
    }

    fn drain_expired_workflow_queue(
        queues: &mut HashMap<QueueKey, OrderedReady<TimestampedWorkflowTask>>,
        grace_window: Duration,
        expired: &mut Vec<TimestampedWorkflowTask>,
        dedupe_keys: &mut Vec<(RunKey, LogicalTaskSeq)>,
    ) {
        for ready in queues.values_mut() {
            while let Some(entry) =
                ready.remove_where(|entry| entry.entered_at.elapsed() >= grace_window)
            {
                dedupe_keys.push((entry.task.run_key, entry.task.logical_seq));
                expired.push(entry);
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

    /// Remove every queued workflow task and direct query for a deleted run.
    ///
    /// Broker state is disposable, but retaining stale entries wastes worker
    /// polls and can strand query callers. Authoritative storage still fences a
    /// task already handed to a worker; this only cleans work not yet delivered.
    pub async fn remove_run(&self, run_key: RunKey) {
        let mut inner = self.inner.lock().await;
        for ready in inner.sticky_ready.values_mut() {
            ready.retain(|entry| entry.task.run_key != run_key);
        }
        for ready in inner.general_ready.values_mut() {
            ready.retain(|entry| entry.task.run_key != run_key);
        }
        inner
            .enqueued
            .retain(|(candidate, _)| *candidate != run_key);

        let mut removed_queries = Vec::new();
        for ready in inner.query_ready.values_mut() {
            let mut retained = VecDeque::with_capacity(ready.len());
            while let Some(query) = ready.pop_front() {
                if query.run_key == run_key {
                    removed_queries.push(query);
                } else {
                    retained.push_back(query);
                }
            }
            *ready = retained;
        }

        let workflow_queues = inner
            .sticky_ready
            .keys()
            .chain(inner.general_ready.keys())
            .cloned()
            .collect::<HashSet<_>>();
        inner.sticky_ready.retain(|_, ready| !ready.is_empty());
        inner.general_ready.retain(|_, ready| !ready.is_empty());
        inner.query_ready.retain(|_, ready| !ready.is_empty());
        for queue in workflow_queues {
            Self::emit_queue_depths(&inner, &queue);
        }
        drop(inner);

        for query in removed_queries {
            let _ = query.response_tx.send(QueryResult::Failed {
                message: "workflow execution was deleted".to_owned(),
                failure: None,
            });
        }
    }
}

impl InMemoryActivityBroker {
    /// Share the runtime's live task-queue configuration store with this broker.
    pub fn set_task_queue_config_store(&self, store: Arc<dyn TaskQueueConfigStore>) {
        self.policy.set_config_store(store);
    }

    fn emit_queue_depth(inner: &ActivityBrokerState, queue: &QueueKey) {
        let depth = inner
            .ready
            .get(queue)
            .map(|entries| entries.len())
            .unwrap_or(0);
        runtime_metrics::set_queue_depth(queue, "general", depth);
    }

    /// Whether an activity poll that just claimed one task left runnable work
    /// behind on the same physical queue.
    pub async fn has_runnable_backlog(&self, queue: &QueueKey) -> bool {
        self.inner
            .lock()
            .await
            .ready
            .get(queue)
            .is_some_and(|ready| !ready.is_empty())
    }

    /// Snapshot activity backlog for `DescribeTaskQueue`.
    pub async fn backlog_stats(&self, queue: &QueueKey) -> BrokerBacklogStats {
        aggregate_priority_stats(&self.backlog_stats_by_priority(queue).await)
    }

    /// Snapshot activity backlog by effective priority band.
    pub async fn backlog_stats_by_priority(&self, queue: &QueueKey) -> PriorityBacklogStats {
        let inner = self.inner.lock().await;
        let Some(ready) = inner.ready.get(queue) else {
            return PriorityBacklogStats::new();
        };
        let mut stats = PriorityBacklogStats::new();
        for task in ready.iter() {
            add_priority_stat(
                &mut stats,
                task.task
                    .order
                    .map_or(3, |order| i32::from(order.priority_key)),
                task.entered_at.elapsed(),
            );
        }
        stats
    }

    /// Move already-ready unversioned activities onto a newly promoted
    /// deployment queue and wake its pollers.
    pub async fn promote_unversioned_backlog(&self, target: &QueueKey) {
        let source = QueueKey {
            namespace_id: target.namespace_id,
            task_queue: target.task_queue.clone(),
            task_kind: target.task_kind,
            deployment: None,
            build_id: None,
        };
        let mut inner = self.inner.lock().await;
        let mut moved = inner.ready.remove(&source).unwrap_or_default();
        if moved.is_empty() {
            return;
        }
        for entry in moved.iter_mut() {
            entry.task.queue = target.clone();
        }
        inner
            .ready
            .entry(target.clone())
            .or_default()
            .append(&mut moved);
        Self::emit_queue_depth(&inner, &source);
        Self::emit_queue_depth(&inner, target);
        let wake = inner.wakes.entry(target.clone()).or_default().clone();
        drop(inner);
        wake.notify_waiters();
    }

    /// Snapshot live-ready unversioned activities for authoritative rerouting.
    ///
    /// The caller resolves each task outside the broker lock because routing
    /// may read durable run and deployment state. The subsequent selective
    /// rekey applies only identities still present, closing the race with a
    /// concurrent poll or grace demotion without making broker state authoritative.
    pub(crate) async fn unversioned_ready_tasks(
        &self,
        target: &QueueKey,
    ) -> Vec<DispatchableActivityTask> {
        let source = QueueKey {
            namespace_id: target.namespace_id,
            task_queue: target.task_queue.clone(),
            task_kind: target.task_kind,
            deployment: None,
            build_id: None,
        };
        self.inner
            .lock()
            .await
            .ready
            .get(&source)
            .map(|ready| ready.iter().map(|entry| entry.task.clone()).collect())
            .unwrap_or_default()
    }

    /// Selectively rekey live-ready unversioned activities after routing was
    /// re-derived from authoritative state.
    ///
    /// Timestamps and deduplication identities are preserved: this is only a
    /// disposable queue-coordinate correction, not a new task publication.
    pub(crate) async fn reroute_unversioned_ready_tasks(
        &self,
        target: &QueueKey,
        routes: &HashMap<(RunKey, String, u32), QueueKey>,
    ) {
        if routes.is_empty() {
            return;
        }
        let source = QueueKey {
            namespace_id: target.namespace_id,
            task_queue: target.task_queue.clone(),
            task_kind: target.task_kind,
            deployment: None,
            build_id: None,
        };
        let mut inner = self.inner.lock().await;
        let Some(mut ready) = inner.ready.remove(&source) else {
            return;
        };
        let mut kept = OrderedReady::default();
        let mut destinations = HashSet::new();
        while let Some(mut entry) = ready.pop_front() {
            let order = entry
                .task
                .order
                .expect("broker publication assigns activity delivery order");
            let identity = (
                entry.task.run_key,
                entry.task.activity_id.clone(),
                entry.task.attempt,
            );
            if let Some(queue) = routes.get(&identity) {
                entry.task.queue = queue.clone();
                destinations.insert(queue.clone());
                inner
                    .ready
                    .entry(queue.clone())
                    .or_default()
                    .insert(order, entry);
            } else {
                kept.insert(order, entry);
            }
        }
        if !kept.is_empty() {
            inner.ready.insert(source.clone(), kept);
        }
        Self::emit_queue_depth(&inner, &source);
        let wakes: Vec<Arc<Notify>> = destinations
            .iter()
            .map(|queue| {
                Self::emit_queue_depth(&inner, queue);
                inner.wakes.entry(queue.clone()).or_default().clone()
            })
            .collect();
        drop(inner);
        for wake in wakes {
            wake.notify_waiters();
        }
    }

    /// Take a specific activity out of the ready queue by run key and activity
    /// id, if present.
    ///
    /// The direct-claim counterpart to [`InMemoryBroker::try_claim_workflow_task`],
    /// for routing a known activity to a caller already handling it instead of
    /// via the normal poll path.
    pub async fn try_claim_activity_task(
        &self,
        queue: &QueueKey,
        run_key: RunKey,
        activity_id: &str,
    ) -> Option<(DispatchableActivityTask, Instant)> {
        let mut inner = self.inner.lock().await;
        let ready = inner.ready.get_mut(queue)?;
        let removed = ready.remove_where(|task| {
            task.task.run_key == run_key && task.task.activity_id == activity_id
        })?;
        if let Some(order) = removed.task.order {
            inner.ordering.served(&removed.task.queue, order);
        }
        inner.enqueued.remove(&(
            removed.task.run_key,
            removed.task.activity_id.clone(),
            removed.task.attempt,
            removed.task.stamp,
        ));
        Self::emit_queue_depth(&inner, queue);
        Some((removed.task, removed.entered_at))
    }

    /// Enqueue an activity task for delivery.
    ///
    /// Duplicate publications (same `run_key` +
    /// `activity_id` + `attempt`) are silently suppressed.
    pub async fn publish_activity_task(
        &self,
        mut task: DispatchableActivityTask,
        metrics: Option<&DeliveryMetrics>,
    ) -> Result<()> {
        runtime_metrics::record_broker_publish(&task.queue);
        let queue = task.queue.clone();
        let (mode, scope_generation, config) = self.policy.delivery_inputs(&task.queue);
        let mut inner = self.inner.lock().await;
        inner.ordering.enter_scope(scope_generation);
        let dedupe_key = (
            task.run_key,
            task.activity_id.clone(),
            task.attempt,
            task.stamp,
        );
        if !inner.enqueued.insert(dedupe_key) {
            return Ok(());
        }
        let order = match task.order {
            Some(order) => inner.ordering.preserve(order),
            None => inner.ordering.assign(
                &task.queue,
                task.priority.as_ref(),
                false,
                config.as_ref(),
                mode,
            ),
        };
        task.order = Some(order);
        let has_waiter = inner.waiter_counts.get(&task.queue).copied().unwrap_or(0) > 0;
        if let Some(metrics) = metrics {
            if has_waiter {
                metrics.record_sync_match(&task.queue);
            } else {
                metrics.record_non_sync_match(&task.queue);
            }
        }

        inner.ready.entry(task.queue.clone()).or_default().insert(
            order,
            TimestampedActivityTask {
                task,
                entered_at: Instant::now(),
                scheduled_at: OffsetDateTime::now_utc(),
            },
        );
        Self::emit_queue_depth(&inner, &queue);
        let wake = inner.wakes.entry(queue).or_default().clone();
        drop(inner);
        wake.notify_waiters();
        Ok(())
    }

    /// Long-poll for an activity task on `queue`.
    ///
    /// Holds until a task for `queue` is available or `wait_for` elapses. As with
    /// the workflow polls, wakeups are per-queue and a wake is a hint to
    /// re-check, not a reason to return empty early (see the module's "Per-queue
    /// wake pattern").
    pub async fn poll_activity_task(
        &self,
        queue: &QueueKey,
        wait_for: Duration,
    ) -> Result<Option<(DispatchableActivityTask, Instant)>> {
        self.poll_activity_task_for_worker(queue, &WorkerIdentity(String::new()), wait_for)
            .await
    }

    /// Long-poll with worker-shutdown fencing.
    pub async fn poll_activity_task_for_worker(
        &self,
        queue: &QueueKey,
        worker: &WorkerIdentity,
        wait_for: Duration,
    ) -> Result<Option<(DispatchableActivityTask, Instant)>> {
        if self.is_denied(queue, worker).await {
            return Ok(None);
        }
        match self.try_take(queue).await? {
            ActivityTakeOutcome::Ready(task) => return Ok(Some(task)),
            ActivityTakeOutcome::WaitUntil(_)
            | ActivityTakeOutcome::Blocked
            | ActivityTakeOutcome::Empty => {}
        }

        self.increment_waiter(queue).await;
        let wake = self.queue_wake(queue).await;
        let config_store = self.policy.config_store();
        let config_changed = config_store.changed(&activity_config_key(queue));
        let deadline = Instant::now() + wait_for;

        let result = loop {
            // Enable before the re-check so a publish racing it still wakes us.
            let notified = wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let changed = config_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();

            if self.is_denied(queue, worker).await {
                break Ok(None);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break Ok(None);
            }
            let rate_wait = match self.try_take(queue).await? {
                ActivityTakeOutcome::Ready(task) => break Ok(Some(task)),
                ActivityTakeOutcome::WaitUntil(eligible_at) => {
                    eligible_at.saturating_duration_since(Instant::now())
                }
                ActivityTakeOutcome::Blocked | ActivityTakeOutcome::Empty => remaining,
            };
            tokio::select! {
                _ = notified.as_mut() => {}
                _ = changed.as_mut() => {}
                _ = tokio::time::sleep(rate_wait.min(remaining)) => {}
            }
        };

        self.decrement_waiter(queue).await;
        result
    }

    /// Cancel current and reject future activity polls for a shutting-down
    /// worker identity on a task-queue family.
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
        let wakes = inner.wakes.values().cloned().collect::<Vec<_>>();
        drop(inner);
        for wake in wakes {
            wake.notify_waiters();
        }
    }

    async fn is_denied(&self, queue: &QueueKey, worker: &WorkerIdentity) -> bool {
        !worker.0.is_empty()
            && self.inner.lock().await.denied_workers.contains(&(
                queue.namespace_id,
                queue.task_queue.clone(),
                worker.clone(),
            ))
    }

    /// Per-queue wake handle, created on first use.
    async fn queue_wake(&self, queue: &QueueKey) -> Arc<Notify> {
        self.inner
            .lock()
            .await
            .wakes
            .entry(queue.clone())
            .or_default()
            .clone()
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
            while let Some(entry) =
                ready.remove_where(|entry| entry.entered_at.elapsed() >= grace_window)
            {
                dedupe_keys.push((
                    entry.task.run_key,
                    entry.task.activity_id.clone(),
                    entry.task.attempt,
                    entry.task.stamp,
                ));
                expired.push(entry);
            }
        }
        for key in dedupe_keys {
            inner.enqueued.remove(&key);
        }
        expired
    }

    async fn try_take(&self, queue: &QueueKey) -> Result<ActivityTakeOutcome> {
        let config_key = activity_config_key(queue);
        let config = self.policy.config_store().get(&config_key);
        let mut inner = self.inner.lock().await;
        let Some(candidate) = inner.ready.get(queue).and_then(OrderedReady::front) else {
            Self::emit_queue_depth(&inner, queue);
            return Ok(ActivityTakeOutcome::Empty);
        };
        let effective = effective_priority(candidate.task.priority.as_ref(), config.as_ref());
        let now = inner.rate_origin.elapsed();
        match inner
            .rate_limits
            .inspect(&config_key, &effective, config.as_ref(), now)
        {
            DispatchEligibility::Blocked => return Ok(ActivityTakeOutcome::Blocked),
            DispatchEligibility::At(offset) => {
                return Ok(ActivityTakeOutcome::WaitUntil(inner.rate_origin + offset));
            }
            DispatchEligibility::Ready => {}
        }
        let task = inner.ready.get_mut(queue).and_then(|q| q.pop_front());
        if let Some(task) = &task {
            if let Some(order) = task.task.order {
                inner.ordering.served(&task.task.queue, order);
            }
            inner
                .rate_limits
                .consume(&config_key, &effective, config.as_ref(), now);
            inner.enqueued.remove(&(
                task.task.run_key,
                task.task.activity_id.clone(),
                task.task.attempt,
                task.task.stamp,
            ));
        }
        Self::emit_queue_depth(&inner, queue);
        Ok(task.map_or(ActivityTakeOutcome::Empty, |task| {
            ActivityTakeOutcome::Ready((task.task, task.entered_at))
        }))
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

    /// Remove all not-yet-delivered activity tasks for a deleted run.
    pub async fn remove_run(&self, run_key: RunKey) {
        let mut inner = self.inner.lock().await;
        for ready in inner.ready.values_mut() {
            ready.retain(|entry| entry.task.run_key != run_key);
        }
        inner
            .enqueued
            .retain(|(candidate, _, _, _)| *candidate != run_key);
        let queues = inner.ready.keys().cloned().collect::<Vec<_>>();
        inner.ready.retain(|_, ready| !ready.is_empty());
        for queue in queues {
            Self::emit_queue_depth(&inner, &queue);
        }
    }
}

fn activity_config_key(queue: &QueueKey) -> TaskQueueConfigKey {
    TaskQueueConfigKey {
        namespace_id: queue.namespace_id,
        task_queue: queue.task_queue.clone(),
        kind: TaskQueueConfigKind::Activity,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DeliveryMode, QueryTask, TaskQueueConfigMetadata};
    use proptest::prelude::*;
    use time::Duration as TimeDuration;
    use tokeira_types::{BuildId, DeploymentId, NamespaceId, Payloads, TaskKind, TaskQueueName};
    use tokio::sync::oneshot;
    use uuid::Uuid;

    fn arb_small_string() -> impl Strategy<Value = String> {
        prop::collection::vec(prop::char::range('a', 'z'), 1..8)
            .prop_map(|chars| chars.into_iter().collect())
    }

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
            dispatch_revision: 0,
            stamp: 0,
            priority: None,
            order: None,
        }
    }

    fn workflow_queue(name: &str, deployment: Option<&str>, build_id: Option<&str>) -> QueueKey {
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
            normal_queue: None,
            sticky_deadline: None,
            priority: None,
            order: None,
        }
    }

    struct FixedDeliveryMode(DeliveryMode);

    impl DeliveryModeProvider for FixedDeliveryMode {
        fn mode_for(&self, _queue: &QueueKey) -> DeliveryMode {
            self.0
        }
    }

    fn set_activity_mode(broker: &InMemoryActivityBroker, mode: DeliveryMode) {
        *broker
            .policy
            .mode_provider
            .write()
            .expect("delivery mode provider lock poisoned") = Arc::new(FixedDeliveryMode(mode));
    }

    fn priority(key: i32, fairness_key: &str, fairness_weight: f32) -> tokeira_kernel::Priority {
        tokeira_kernel::Priority {
            priority_key: key,
            fairness_key: fairness_key.to_string(),
            fairness_weight,
        }
    }

    fn named_activity_task(
        queue: &QueueKey,
        activity_id: impl Into<String>,
        priority: tokeira_kernel::Priority,
    ) -> DispatchableActivityTask {
        DispatchableActivityTask {
            activity_id: activity_id.into(),
            priority: Some(priority),
            ..activity_task(queue.clone())
        }
    }

    #[tokio::test]
    async fn priority_bands_precede_fifo_and_disabled_mode_is_global_fifo() {
        let queue = activity_queue("priority-order");
        let broker = InMemoryActivityBroker::default();
        for task in [
            named_activity_task(&queue, "low", priority(5, "", 0.0)),
            named_activity_task(&queue, "high-1", priority(1, "", 0.0)),
            named_activity_task(&queue, "default", priority(0, "", 0.0)),
            named_activity_task(&queue, "high-2", priority(1, "", 0.0)),
        ] {
            broker
                .publish_activity_task(task, None)
                .await
                .expect("publish");
        }
        let mut ordered = Vec::new();
        for _ in 0..4 {
            ordered.push(
                broker
                    .poll_activity_task(&queue, Duration::ZERO)
                    .await
                    .expect("poll")
                    .expect("ready task")
                    .0
                    .activity_id,
            );
        }
        assert_eq!(ordered, ["high-1", "high-2", "default", "low"]);

        let fifo = InMemoryActivityBroker::default();
        set_activity_mode(
            &fifo,
            DeliveryMode {
                priority_enabled: false,
                fairness_enabled: false,
                auto_enable: false,
            },
        );
        for task in [
            named_activity_task(&queue, "low-first", priority(5, "", 0.0)),
            named_activity_task(&queue, "high-second", priority(1, "", 0.0)),
        ] {
            fifo.publish_activity_task(task, None)
                .await
                .expect("publish");
        }
        let first = fifo
            .poll_activity_task(&queue, Duration::ZERO)
            .await
            .expect("poll")
            .expect("ready task")
            .0;
        let second = fifo
            .poll_activity_task(&queue, Duration::ZERO)
            .await
            .expect("poll")
            .expect("ready task")
            .0;
        assert_eq!(
            (first.activity_id, second.activity_id),
            ("low-first".into(), "high-second".into())
        );
    }

    #[tokio::test]
    async fn sticky_affinity_wins_only_among_equal_or_lower_normal_priority() {
        let broker = InMemoryBroker::default();
        let namespace_id = NamespaceId::new();
        let sticky = QueueKey {
            namespace_id,
            task_queue: TaskQueueName("sticky".to_string()),
            task_kind: TaskKind::Workflow,
            deployment: None,
            build_id: None,
        };
        let normal = QueueKey {
            task_queue: TaskQueueName("normal".to_string()),
            ..sticky.clone()
        };
        let worker = WorkerIdentity("sticky-worker".to_string());
        broker
            .inner
            .lock()
            .await
            .poller_observations
            .insert((sticky.clone(), worker.clone()), Instant::now());

        let sticky_task = DispatchableWorkflowTask {
            sticky_preferred: Some(worker.clone()),
            normal_queue: Some(normal.clone()),
            sticky_deadline: Some(OffsetDateTime::now_utc() + TimeDuration::minutes(1)),
            priority: Some(priority(3, "", 0.0)),
            ..workflow_task(sticky.clone())
        };
        broker.publish_workflow_task(sticky_task, None).await;
        broker
            .publish_workflow_task(
                DispatchableWorkflowTask {
                    priority: Some(priority(1, "", 0.0)),
                    ..workflow_task(normal.clone())
                },
                None,
            )
            .await;
        broker
            .publish_workflow_task(
                DispatchableWorkflowTask {
                    priority: Some(priority(5, "", 0.0)),
                    ..workflow_task(normal.clone())
                },
                None,
            )
            .await;

        assert_eq!(
            broker
                .sticky_backlog_stats_by_priority(&sticky)
                .await
                .get(&3)
                .map(|stats| stats.count),
            Some(1)
        );
        assert_eq!(broker.backlog_stats(&sticky).await.count, 0);
        assert_eq!(broker.backlog_stats(&normal).await.count, 2);

        let first = broker
            .poll_workflow_activation(&sticky, Some(&normal), &worker, Duration::ZERO)
            .await
            .expect("poll")
            .expect("ready task");
        let second = broker
            .poll_workflow_activation(&sticky, Some(&normal), &worker, Duration::ZERO)
            .await
            .expect("poll")
            .expect("ready task");
        let third = broker
            .poll_workflow_activation(&sticky, Some(&normal), &worker, Duration::ZERO)
            .await
            .expect("poll")
            .expect("ready task");
        let priorities = [first, second, third].map(|result| {
            result
                .into_queued()
                .expect("queued workflow task")
                .0
                .priority
                .expect("priority")
                .priority_key
        });
        assert_eq!(priorities, [1, 3, 5]);
    }

    #[tokio::test]
    async fn weighted_fairness_tends_to_one_to_one_and_two_to_one_and_is_work_conserving() {
        let queue = activity_queue("weighted");
        let mode = DeliveryMode {
            priority_enabled: true,
            fairness_enabled: true,
            auto_enable: false,
        };

        let equal = InMemoryActivityBroker::default();
        set_activity_mode(&equal, mode);
        for index in 0..20 {
            equal
                .publish_activity_task(
                    named_activity_task(&queue, format!("a-{index}"), priority(3, "a", 1.0)),
                    None,
                )
                .await
                .expect("publish");
        }
        for index in 0..20 {
            equal
                .publish_activity_task(
                    named_activity_task(&queue, format!("b-{index}"), priority(3, "b", 1.0)),
                    None,
                )
                .await
                .expect("publish");
        }
        let mut equal_counts = (0, 0);
        for _ in 0..20 {
            let id = equal
                .poll_activity_task(&queue, Duration::ZERO)
                .await
                .expect("poll")
                .expect("ready task")
                .0
                .activity_id;
            if id.starts_with("a-") {
                equal_counts.0 += 1;
            } else {
                equal_counts.1 += 1;
            }
        }
        assert_eq!(equal_counts, (10, 10));

        let weighted = InMemoryActivityBroker::default();
        set_activity_mode(&weighted, mode);
        for index in 0..30 {
            weighted
                .publish_activity_task(
                    named_activity_task(
                        &queue,
                        format!("heavy-{index}"),
                        priority(3, "heavy", 2.0),
                    ),
                    None,
                )
                .await
                .expect("publish");
        }
        for index in 0..30 {
            weighted
                .publish_activity_task(
                    named_activity_task(
                        &queue,
                        format!("light-{index}"),
                        priority(3, "light", 1.0),
                    ),
                    None,
                )
                .await
                .expect("publish");
        }
        let mut weighted_counts = (0, 0);
        for _ in 0..30 {
            let id = weighted
                .poll_activity_task(&queue, Duration::ZERO)
                .await
                .expect("poll")
                .expect("ready task")
                .0
                .activity_id;
            if id.starts_with("heavy-") {
                weighted_counts.0 += 1;
            } else {
                weighted_counts.1 += 1;
            }
        }
        assert_eq!(weighted_counts, (20, 10));

        let mut remaining = 0;
        while weighted
            .poll_activity_task(&queue, Duration::ZERO)
            .await
            .expect("poll")
            .is_some()
        {
            remaining += 1;
        }
        assert_eq!(remaining, 30);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        // Feature: task-queue-priority-fairness, Property 5
        #[test]
        fn sticky_and_normal_candidates_are_compared_by_priority(
            sticky_priority in prop::option::of(1i16..=5),
            normal_priority in prop::option::of(1i16..=5),
        ) {
            let expected = match (sticky_priority, normal_priority) {
                (Some(sticky), Some(normal)) => sticky <= normal,
                (Some(_), None) => true,
                (None, _) => false,
            };
            prop_assert_eq!(
                prefer_sticky_candidate(sticky_priority, normal_priority),
                expected
            );
        }

        // Feature: task-queue-priority-fairness, Property 15
        #[test]
        fn per_priority_stats_conserve_live_and_durable_work(
            live_keys in prop::collection::vec(prop::option::of(-10i32..20), 0..64),
            durable_keys in prop::collection::vec(prop::option::of(-10i32..20), 0..64),
        ) {
            let mut live = PriorityBacklogStats::new();
            let mut durable = PriorityBacklogStats::new();
            let mut expected = BTreeMap::<i32, usize>::new();
            for (index, raw_key) in live_keys.iter().enumerate() {
                let raw = raw_key.map(|priority_key| tokeira_kernel::Priority {
                    priority_key,
                    fairness_key: String::new(),
                    fairness_weight: 0.0,
                });
                let band = i32::from(effective_priority(raw.as_ref(), None).priority_key);
                add_priority_stat(&mut live, band, Duration::from_secs(index as u64));
                *expected.entry(band).or_default() += 1;
            }
            for (index, raw_key) in durable_keys.iter().enumerate() {
                let raw = raw_key.map(|priority_key| tokeira_kernel::Priority {
                    priority_key,
                    fairness_key: String::new(),
                    fairness_weight: 0.0,
                });
                let band = i32::from(effective_priority(raw.as_ref(), None).priority_key);
                add_priority_stat(
                    &mut durable,
                    band,
                    Duration::from_secs((index + live_keys.len()) as u64),
                );
                *expected.entry(band).or_default() += 1;
            }

            merge_priority_stats(&mut live, durable);
            prop_assert_eq!(
                live.iter()
                    .map(|(priority_key, stats)| (*priority_key, stats.count))
                    .collect::<BTreeMap<_, _>>(),
                expected.clone()
            );
            let aggregate = aggregate_priority_stats(&live);
            prop_assert_eq!(
                aggregate.count,
                live_keys.len() + durable_keys.len()
            );
            prop_assert_eq!(
                live.keys().copied().collect::<Vec<_>>(),
                expected.keys().copied().collect::<Vec<_>>()
            );
        }

        #[test]
        fn sticky_availability_matches_recent_active_and_denied_model(
            recent_observation in any::<bool>(),
            active_waiter in any::<bool>(),
            closed_waiter in any::<bool>(),
            denied in any::<bool>(),
        ) {
            // Feature: api-conformance-client-misc, Property 5: sticky availability and immediate fallback
            let queue = workflow_queue("sticky", None, None);
            let worker = WorkerIdentity("sticky-worker".into());
            let mut state = BrokerState::default();
            let observed_age = if recent_observation {
                Duration::from_secs(9)
            } else {
                Duration::from_secs(11)
            };
            state.poller_observations.insert(
                (queue.clone(), worker.clone()),
                Instant::now() - observed_age,
            );

            let mut live_receiver = None;
            if active_waiter {
                let (response_tx, response_rx) = oneshot::channel();
                if closed_waiter {
                    drop(response_rx);
                } else {
                    live_receiver = Some(response_rx);
                }
                state
                    .workflow_waiters
                    .entry(queue.clone())
                    .or_default()
                    .push_back(WorkflowWaiter {
                        id: 1,
                        worker: worker.clone(),
                        response_tx,
                    });
            }
            if denied {
                state.denied_workers.insert((
                    queue.namespace_id,
                    queue.task_queue.clone(),
                    worker.clone(),
                ));
            }

            let expected = !denied
                && (recent_observation || (active_waiter && !closed_waiter));
            prop_assert_eq!(
                InMemoryBroker::sticky_worker_available(&state, &queue, &worker),
                expected
            );
            drop(live_receiver);
        }
    }

    /// Spin (cooperatively) until a workflow poller is parked on `queue`, so
    /// tests synchronise on observable broker state instead of a fixed sleep.
    async fn await_workflow_waiter(broker: &InMemoryBroker, queue: &QueueKey) {
        loop {
            if broker
                .inner
                .lock()
                .await
                .waiter_counts
                .get(queue)
                .copied()
                .unwrap_or(0)
                > 0
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    }

    // Property: queue isolation. A publish to queue B must not end an in-flight
    // poll on queue A — wakeups are per-queue, so unrelated traffic neither wakes
    // nor empties the poll. The poll completes only when queue A itself gets a
    // task. (Regression guard for the broker-global-wake spurious-empty bug.)
    #[tokio::test]
    async fn poll_on_one_queue_is_not_ended_by_publish_to_another() {
        let broker = InMemoryBroker::default();
        let queue_a = workflow_queue("queue-a", None, None);
        let queue_b = workflow_queue("queue-b", None, None);
        let worker = WorkerIdentity("w".to_string());
        let task_a = workflow_task(queue_a.clone());

        let poll = {
            let broker = broker.clone();
            let queue_a = queue_a.clone();
            let worker = worker.clone();
            tokio::spawn(async move {
                broker
                    .poll_workflow_activation(
                        &queue_a,
                        None,
                        &worker,
                        std::time::Duration::from_secs(5),
                    )
                    .await
            })
        };

        await_workflow_waiter(&broker, &queue_a).await;

        // A publish to an unrelated queue must not wake or empty the queue-A poll.
        broker
            .publish_workflow_task(workflow_task(queue_b.clone()), None)
            .await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(
            !poll.is_finished(),
            "poll on queue-a was ended by a publish to queue-b"
        );

        // A publish to queue A delivers.
        broker.publish_workflow_task(task_a.clone(), None).await;
        match poll.await.unwrap().unwrap() {
            Some(WorkflowPollResult::Queued(task, _)) => assert_eq!(task, task_a),
            other => panic!("expected queued task from queue-a, got {other:?}"),
        }
    }

    // Property: same-queue delivery. A task published to the polled queue after
    // the poller has parked is delivered within the poll, promptly.
    #[tokio::test]
    async fn parked_poll_receives_a_later_same_queue_publish() {
        let broker = InMemoryBroker::default();
        let queue = workflow_queue("queue-a", None, None);
        let worker = WorkerIdentity("w".to_string());
        let task = workflow_task(queue.clone());

        let poll = {
            let broker = broker.clone();
            let queue = queue.clone();
            let worker = worker.clone();
            tokio::spawn(async move {
                broker
                    .poll_workflow_activation(
                        &queue,
                        None,
                        &worker,
                        std::time::Duration::from_secs(5),
                    )
                    .await
            })
        };

        await_workflow_waiter(&broker, &queue).await;
        broker.publish_workflow_task(task.clone(), None).await;

        match poll.await.unwrap().unwrap() {
            Some(WorkflowPollResult::Queued(delivered, _)) => assert_eq!(delivered, task),
            other => panic!("expected queued task, got {other:?}"),
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
        assert_eq!(
            right.and_then(|entry| entry.into_queued().map(|queued| queued.0)),
            Some(task)
        );
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

    // Tier 4.29 invariant: scaling pressure and Describe depth are two views of
    // the same live workflow backlog, and consuming the last task clears both.
    #[tokio::test]
    async fn workflow_backlog_drives_stats_and_scaling_pressure() {
        let broker = InMemoryBroker::default();
        let queue = workflow_queue("queue-a", None, None);
        let task = workflow_task(queue.clone());
        broker.publish_workflow_task(task.clone(), None).await;

        assert!(broker.has_runnable_backlog(&queue).await);
        assert_eq!(broker.backlog_stats(&queue).await.count, 1);

        assert!(
            broker
                .try_claim_workflow_task(&queue, task.run_key)
                .await
                .is_some()
        );
        assert!(!broker.has_runnable_backlog(&queue).await);
        assert_eq!(
            broker.backlog_stats(&queue).await,
            BrokerBacklogStats::default()
        );
    }

    // Tier 4.29 invariant: promotion rekeys disposable backlog without losing
    // or duplicating authoritative work.
    #[tokio::test]
    async fn promotion_moves_workflow_and_activity_backlog_to_versioned_queues() {
        let workflow_broker = InMemoryBroker::default();
        let unversioned_workflow = workflow_queue("queue-a", None, None);
        let versioned_workflow = QueueKey {
            deployment: Some(DeploymentId("deployment-a".to_string())),
            build_id: Some(BuildId("build-a".to_string())),
            ..unversioned_workflow.clone()
        };
        workflow_broker
            .publish_workflow_task(workflow_task(unversioned_workflow.clone()), None)
            .await;
        workflow_broker
            .promote_unversioned_backlog(&versioned_workflow)
            .await;

        assert_eq!(
            workflow_broker
                .backlog_stats(&unversioned_workflow)
                .await
                .count,
            0
        );
        assert_eq!(
            workflow_broker
                .backlog_stats(&versioned_workflow)
                .await
                .count,
            1
        );

        let activity_broker = InMemoryActivityBroker::default();
        let unversioned_activity = QueueKey {
            task_kind: TaskKind::Activity,
            ..unversioned_workflow
        };
        let versioned_activity = QueueKey {
            deployment: versioned_workflow.deployment.clone(),
            build_id: versioned_workflow.build_id.clone(),
            ..unversioned_activity.clone()
        };
        activity_broker
            .publish_activity_task(activity_task(unversioned_activity.clone()), None)
            .await
            .unwrap();
        activity_broker
            .promote_unversioned_backlog(&versioned_activity)
            .await;

        assert_eq!(
            activity_broker
                .backlog_stats(&unversioned_activity)
                .await
                .count,
            0
        );
        assert_eq!(
            activity_broker
                .backlog_stats(&versioned_activity)
                .await
                .count,
            1
        );
    }

    // Tier 4.29 invariant: the shutdown fence applies to activity polls as well
    // as workflow polls while leaving other worker identities eligible.
    #[tokio::test]
    async fn denied_activity_worker_cannot_receive_activity_tasks() {
        let broker = InMemoryActivityBroker::default();
        let queue = activity_queue("queue-a");
        let denied_worker = WorkerIdentity("worker-a".to_string());
        broker
            .deny_worker(
                queue.namespace_id,
                queue.task_queue.clone(),
                denied_worker.clone(),
            )
            .await;
        broker
            .publish_activity_task(activity_task(queue.clone()), None)
            .await
            .unwrap();

        let denied = broker
            .poll_activity_task_for_worker(
                &queue,
                &denied_worker,
                std::time::Duration::from_millis(5),
            )
            .await
            .unwrap();
        let allowed = broker
            .poll_activity_task_for_worker(
                &queue,
                &WorkerIdentity("worker-b".to_string()),
                std::time::Duration::from_millis(5),
            )
            .await
            .unwrap();

        assert!(denied.is_none());
        assert!(allowed.is_some());
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
    async fn sticky_publication_falls_back_when_worker_is_unavailable() {
        let broker = InMemoryBroker::default();
        let sticky_queue = workflow_queue("sticky", None, None);
        let normal_queue = QueueKey {
            task_queue: TaskQueueName("normal".into()),
            ..sticky_queue.clone()
        };
        let task = DispatchableWorkflowTask {
            sticky_preferred: Some(WorkerIdentity("worker-a".to_string())),
            normal_queue: Some(normal_queue.clone()),
            sticky_deadline: Some(OffsetDateTime::UNIX_EPOCH),
            ..workflow_task(sticky_queue)
        };

        broker.publish_workflow_task(task.clone(), None).await;
        let delivered = broker
            .poll_workflow_task(
                &normal_queue,
                &WorkerIdentity("worker-b".into()),
                std::time::Duration::ZERO,
            )
            .await;
        let delivered = delivered
            .unwrap()
            .expect("normal fallback is immediately ready")
            .into_queued()
            .expect("workflow task");
        assert_eq!(delivered.0.queue, normal_queue);
        assert_eq!(delivered.0.sticky_preferred, None);
        assert_eq!(delivered.0.sticky_deadline, None);
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
    async fn zero_activity_rate_unblocks_on_live_config_change() {
        let store = Arc::new(InMemoryTaskQueueConfigStore::default());
        let broker = InMemoryActivityBroker::default();
        broker.set_task_queue_config_store(store.clone());
        let queue = activity_queue("rate-change");
        let config_key = activity_config_key(&queue);
        let metadata = || TaskQueueConfigMetadata {
            reason: "test".to_string(),
            update_identity: "test".to_string(),
            update_time: OffsetDateTime::UNIX_EPOCH,
        };
        store
            .apply(
                config_key.clone(),
                crate::TaskQueueConfigPatch {
                    queue_rate_limit: crate::TaskQueueConfigFieldPatch::Set((
                        Some(0.0),
                        metadata(),
                    )),
                    ..crate::TaskQueueConfigPatch::default()
                },
                1_000,
            )
            .expect("zero is a valid blocking rate");
        broker
            .publish_activity_task(activity_task(queue.clone()), None)
            .await
            .expect("publish");

        let waiter = {
            let broker = broker.clone();
            let queue = queue.clone();
            tokio::spawn(async move {
                broker
                    .poll_activity_task(&queue, Duration::from_secs(1))
                    .await
                    .expect("poll")
            })
        };
        while !broker.queues_with_waiters().await.contains(&queue) {
            tokio::task::yield_now().await;
        }
        store
            .apply(
                config_key,
                crate::TaskQueueConfigPatch {
                    queue_rate_limit: crate::TaskQueueConfigFieldPatch::Set((None, metadata())),
                    ..crate::TaskQueueConfigPatch::default()
                },
                1_000,
            )
            .expect("unsetting the rate is valid");

        assert!(waiter.await.expect("poll task").is_some());
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
                    sticky_queue: None,
                    sticky_deadline: None,
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
        let sticky_worker = WorkerIdentity("worker-a".into());
        assert!(broker.record_workflow_poller(&queue, &sticky_worker).await);
        let (sticky_tx, _sticky_rx) = oneshot::channel();
        broker
            .publish_query_task(QueryTask {
                run_key: RunKey::new(),
                query_type: "sticky".into(),
                query_args: Payloads::default(),
                queue: queue.clone(),
                sticky_preferred: Some(sticky_worker.clone()),
                sticky_queue: None,
                sticky_deadline: None,
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
                sticky_queue: None,
                sticky_deadline: None,
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
            .poll_query_task(&queue, &sticky_worker, std::time::Duration::from_millis(5))
            .await
            .expect("sticky query should deliver to matching worker");

        assert_eq!(wrong_worker.query_type, "general");
        assert_eq!(sticky_worker.query_type, "sticky");
    }

    #[tokio::test]
    async fn sticky_queue_poll_claims_normal_indexed_query_for_its_owner_only() {
        let broker = InMemoryBroker::default();
        let normal_queue = workflow_queue("queue-a", None, None);
        let sticky_name = TaskQueueName("sticky-queue-a".into());
        let sticky_queue = QueueKey {
            task_queue: sticky_name.clone(),
            ..normal_queue.clone()
        };
        let worker = WorkerIdentity("worker-a".into());
        assert!(broker.record_workflow_poller(&sticky_queue, &worker).await);
        let (tx, _rx) = oneshot::channel();
        broker
            .publish_query_task(QueryTask {
                run_key: RunKey::new(),
                query_type: "sticky".into(),
                query_args: Payloads::default(),
                queue: normal_queue.clone(),
                sticky_preferred: Some(worker.clone()),
                sticky_queue: Some(sticky_name),
                sticky_deadline: None,
                response_tx: tx,
            })
            .await;

        assert!(
            broker
                .try_take_query(&normal_queue, &WorkerIdentity("worker-b".into()))
                .await
                .is_none()
        );
        assert!(
            broker
                .try_take_query(&sticky_queue, &WorkerIdentity("worker-b".into()))
                .await
                .is_none()
        );

        let task = broker
            .try_take_query(&sticky_queue, &worker)
            .await
            .expect("the named sticky queue and owning worker must claim the query");
        assert_eq!(task.query_type, "sticky");
    }

    #[tokio::test]
    async fn workflow_poll_delivers_direct_query_task() {
        let broker = InMemoryBroker::default();
        let queue = workflow_queue("queue-a", None, None);
        let (tx, _rx) = oneshot::channel();
        broker
            .publish_query_task(QueryTask {
                run_key: RunKey::new(),
                query_type: "direct".into(),
                query_args: Payloads::default(),
                queue: queue.clone(),
                sticky_preferred: None,
                sticky_queue: None,
                sticky_deadline: None,
                response_tx: tx,
            })
            .await;

        let polled = broker
            .poll_workflow_activation(
                &queue,
                None,
                &WorkerIdentity("worker-a".into()),
                std::time::Duration::from_millis(5),
            )
            .await
            .unwrap()
            .expect("workflow poll should deliver direct query");

        match polled {
            WorkflowPollResult::Query(task) => assert_eq!(task.query_type, "direct"),
            other => panic!("unexpected workflow poll result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn workflow_poll_promotes_expired_sticky_query_to_live() {
        let broker = InMemoryBroker::default();
        let queue = workflow_queue("queue-a", None, None);
        let (tx, _rx) = oneshot::channel();
        broker
            .publish_query_task(QueryTask {
                run_key: RunKey::new(),
                query_type: "sticky-expired".into(),
                query_args: Payloads::default(),
                queue: queue.clone(),
                sticky_preferred: Some(WorkerIdentity("worker-a".into())),
                sticky_queue: None,
                sticky_deadline: Some(OffsetDateTime::UNIX_EPOCH),
                response_tx: tx,
            })
            .await;

        let polled = broker
            .poll_workflow_activation(
                &queue,
                None,
                &WorkerIdentity("worker-b".into()),
                std::time::Duration::from_millis(5),
            )
            .await
            .unwrap()
            .expect("expired sticky query should fall back to live delivery");

        match polled {
            WorkflowPollResult::Query(task) => {
                assert_eq!(task.query_type, "sticky-expired");
                assert_eq!(task.sticky_preferred, None);
            }
            other => panic!("unexpected workflow poll result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn workflow_poll_falls_back_when_sticky_query_deadline_elapses() {
        let broker = InMemoryBroker::default();
        let queue = workflow_queue("queue-a", None, None);
        let sticky_worker = WorkerIdentity("worker-a".into());
        assert!(broker.record_workflow_poller(&queue, &sticky_worker).await);
        let (tx, _rx) = oneshot::channel();
        broker
            .publish_query_task(QueryTask {
                run_key: RunKey::new(),
                query_type: "sticky-waits".into(),
                query_args: Payloads::default(),
                queue: queue.clone(),
                sticky_preferred: Some(sticky_worker),
                sticky_queue: None,
                sticky_deadline: Some(OffsetDateTime::now_utc() + TimeDuration::milliseconds(1)),
                response_tx: tx,
            })
            .await;

        let polled = broker
            .poll_workflow_activation(
                &queue,
                None,
                &WorkerIdentity("worker-b".into()),
                std::time::Duration::from_millis(50),
            )
            .await
            .unwrap()
            .expect("live worker should receive query after sticky fallback deadline");

        match polled {
            WorkflowPollResult::Query(task) => {
                assert_eq!(task.query_type, "sticky-waits");
                assert_eq!(task.sticky_preferred, None);
            }
            other => panic!("unexpected workflow poll result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn unavailable_sticky_query_falls_back_without_waiting_for_its_deadline() {
        let broker = InMemoryBroker::default();
        let normal_queue = workflow_queue("normal", None, None);
        let (tx, _rx) = oneshot::channel();
        broker
            .publish_query_task(QueryTask {
                run_key: RunKey::new(),
                query_type: "fallback-now".into(),
                query_args: Payloads::default(),
                queue: normal_queue.clone(),
                sticky_preferred: Some(WorkerIdentity("departed-worker".into())),
                sticky_queue: Some(TaskQueueName("sticky".into())),
                sticky_deadline: Some(OffsetDateTime::now_utc() + TimeDuration::seconds(5)),
                response_tx: tx,
            })
            .await;

        let task = broker
            .poll_workflow_activation(
                &normal_queue,
                None,
                &WorkerIdentity("replacement-worker".into()),
                std::time::Duration::ZERO,
            )
            .await
            .unwrap()
            .expect("unavailable sticky target must route to the normal poller");
        let WorkflowPollResult::Query(task) = task else {
            panic!("expected a query task");
        };
        assert_eq!(task.query_type, "fallback-now");
        assert_eq!(task.sticky_preferred, None);
        assert_eq!(task.sticky_queue, None);
        assert_eq!(task.sticky_deadline, None);
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

        #[test]
        fn property_try_claim_workflow_task_targets_requested_run(
            queue_name in arb_small_string(),
            target_run in any::<u128>(),
            other_run in any::<u128>(),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                let broker = InMemoryBroker::default();
                let queue = workflow_queue(&queue_name, None, None);
                let target = DispatchableWorkflowTask {
                    run_key: RunKey(Uuid::from_u128(target_run)),
                    ..workflow_task(queue.clone())
                };
                let other = DispatchableWorkflowTask {
                    run_key: RunKey(Uuid::from_u128(other_run.wrapping_add(1))),
                    logical_seq: LogicalTaskSeq(2),
                    ..workflow_task(queue.clone())
                };

                broker.publish_workflow_task(other.clone(), None).await;
                broker.publish_workflow_task(target.clone(), None).await;

                let claimed = broker
                    .try_claim_workflow_task(&queue, target.run_key)
                    .await;
                let remaining = broker
                    .poll_workflow_task(
                        &queue,
                        &WorkerIdentity("worker-a".into()),
                        std::time::Duration::from_millis(5),
                    )
                    .await
                    .unwrap()
                    .and_then(|entry| entry.into_queued().map(|queued| queued.0));

                prop_assert_eq!(claimed.map(|entry| entry.0), Some(target));
                prop_assert_eq!(remaining, Some(other));
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }

        #[test]
        fn property_try_claim_activity_task_targets_requested_identity(
            queue_name in arb_small_string(),
            target_run in any::<u128>(),
            other_run in any::<u128>(),
            target_activity_id in arb_small_string(),
            other_activity_id in arb_small_string(),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                let broker = InMemoryActivityBroker::default();
                let queue = activity_queue(&queue_name);
                let target = DispatchableActivityTask {
                    run_key: RunKey(Uuid::from_u128(target_run)),
                    activity_id: target_activity_id.clone(),
                    ..activity_task(queue.clone())
                };
                let other = DispatchableActivityTask {
                    run_key: RunKey(Uuid::from_u128(other_run.wrapping_add(1))),
                    activity_id: format!("{other_activity_id}-other"),
                    attempt: 2,
                    ..activity_task(queue.clone())
                };

                broker.publish_activity_task(other.clone(), None).await.unwrap();
                broker.publish_activity_task(target.clone(), None).await.unwrap();

                let claimed = broker
                    .try_claim_activity_task(&queue, target.run_key, &target_activity_id)
                    .await;
                let remaining = broker
                    .poll_activity_task(&queue, std::time::Duration::from_millis(5))
                    .await
                    .unwrap()
                    .map(|entry| entry.0);

                prop_assert_eq!(claimed.map(|entry| entry.0), Some(target));
                prop_assert_eq!(remaining, Some(other));
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }

        #[test]
        fn property_claimed_workflow_task_is_excluded_from_normal_poll(
            queue_name in arb_small_string(),
            run in any::<u128>(),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                let broker = InMemoryBroker::default();
                let queue = workflow_queue(&queue_name, None, None);
                let task = DispatchableWorkflowTask {
                    run_key: RunKey(Uuid::from_u128(run)),
                    ..workflow_task(queue.clone())
                };

                broker.publish_workflow_task(task.clone(), None).await;
                let claimed = broker
                    .try_claim_workflow_task(&queue, task.run_key)
                    .await;
                let polled = broker
                    .poll_workflow_task(
                        &queue,
                        &WorkerIdentity("worker-a".into()),
                        std::time::Duration::from_millis(5),
                    )
                    .await
                    .unwrap();

                prop_assert_eq!(claimed.map(|entry| entry.0), Some(task));
                prop_assert_eq!(polled, None);
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }

        #[test]
        fn property_claimed_activity_task_is_excluded_from_normal_poll(
            queue_name in arb_small_string(),
            run in any::<u128>(),
            activity_id in arb_small_string(),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                let broker = InMemoryActivityBroker::default();
                let queue = activity_queue(&queue_name);
                let task = DispatchableActivityTask {
                    run_key: RunKey(Uuid::from_u128(run)),
                    activity_id: activity_id.clone(),
                    ..activity_task(queue.clone())
                };

                broker.publish_activity_task(task.clone(), None).await.unwrap();
                let claimed = broker
                    .try_claim_activity_task(&queue, task.run_key, &activity_id)
                    .await;
                let polled = broker
                    .poll_activity_task(&queue, std::time::Duration::from_millis(5))
                    .await
                    .unwrap();

                prop_assert_eq!(claimed.map(|entry| entry.0), Some(task));
                prop_assert_eq!(polled, None);
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }

    #[tokio::test]
    async fn active_sticky_poller_keeps_preferred_offer() {
        let broker = InMemoryBroker::default();
        let sticky_queue = workflow_queue("sticky", None, None);
        let normal_queue = QueueKey {
            task_queue: TaskQueueName("normal".into()),
            ..sticky_queue.clone()
        };
        let worker = WorkerIdentity("worker-a".into());
        let task = DispatchableWorkflowTask {
            sticky_preferred: Some(worker.clone()),
            normal_queue: Some(normal_queue),
            sticky_deadline: Some(OffsetDateTime::UNIX_EPOCH),
            ..workflow_task(sticky_queue.clone())
        };

        let poll = {
            let broker = broker.clone();
            let queue = sticky_queue.clone();
            let worker = worker.clone();
            tokio::spawn(async move {
                broker
                    .poll_workflow_task(&queue, &worker, std::time::Duration::from_secs(1))
                    .await
            })
        };
        await_workflow_waiter(&broker, &sticky_queue).await;
        broker.publish_workflow_task(task.clone(), None).await;

        let delivered = poll
            .await
            .unwrap()
            .unwrap()
            .expect("sticky poll receives task")
            .into_queued()
            .expect("workflow task");
        assert_eq!(delivered.0, task);
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

                prop_assert_eq!(
                    first.and_then(|entry| entry.into_queued().map(|queued| queued.0)),
                    Some(task)
                );
                prop_assert_eq!(second, None);
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }
}
