//! Demand-driven, read-only discovery of durable workflow-task intent.
//!
//! Queue homes scan across all execution homes. A pass keeps its continuation
//! across one-page scheduler slices, including pages with no admissible rows.
//! Demand and offers are volatile; only the authoritative start can consume work.

use std::{
    collections::HashMap,
    fmt::Debug,
    num::NonZeroU32,
    sync::{Arc, Mutex, RwLock},
};

use anyhow::Result;
use async_trait::async_trait;
use tokeira_kernel::LoadedRun;
use tokeira_storage::{
    DispatchableWorkflowTask, RunRepository, WorkflowDiscoveryRange, WorkflowDispatchPosition,
    WorkflowDispatchRouting, WorkflowDispatchRow, derive_workflow_dispatch,
};
use tokeira_types::{QueueKey, ShardId};
use tokio::{
    sync::Notify,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

use crate::{
    broker::InMemoryBroker, deployment_registry::DeploymentRegistry,
    runtime::workflow_task::resolve_workflow_task_target_version, workflow_offers::OfferAdmission,
};

pub(crate) const DISCOVERY_PERIOD: Duration = Duration::from_secs(1);
pub(crate) const PAGE_SIZE: u32 = 64;
pub(crate) const SLICE_PAGES: usize = 1;
pub(crate) const PASS_ADMISSIONS: usize = 64;
pub(crate) const CONCURRENT_QUERIES: usize = 8;
pub(crate) const IDLE_GRACE: Duration = Duration::from_secs(30);
pub(crate) const IDLE_CAPACITY: usize = 4_096;

/// A local queue-home assignment; cancellation retires its disposable work.
#[derive(Clone, Debug)]
pub(crate) struct QueueHome {
    pub(crate) id: ShardId,
    pub(crate) generation: u64,
    pub(crate) cancel: CancellationToken,
}

/// Supplies the queue partition's home, independently of execution placement.
///
/// Kept internal until the cutover wires placement and reconstruction together.
/// Default runtime construction supplies no provider and retains legacy delivery.
pub(crate) trait QueueHomeProvider: Debug + Send + Sync {
    fn local_home(&self, queue: &QueueKey) -> Option<QueueHome>;
}

/// The reusable page boundary has no workflow-specific admission semantics.
#[async_trait]
pub(crate) trait DiscoverySource: Send + Sync {
    type Range: Sync;
    type Position: Clone + Send + Sync;
    type Candidate: Send;

    async fn page(
        &self,
        range: &Self::Range,
        after: Option<Self::Position>,
        limit: NonZeroU32,
    ) -> Result<DiscoveryPage<Self::Position, Self::Candidate>>;
}

#[derive(Debug)]
pub(crate) struct DiscoveryPage<P, C> {
    pub(crate) candidates: Vec<C>,
    pub(crate) last_examined: Option<P>,
    pub(crate) exhausted: bool,
}

pub(crate) struct WorkflowSource<R> {
    pub(crate) repo: Arc<R>,
    pub(crate) registry: Arc<RwLock<Option<DeploymentRegistry>>>,
}

#[async_trait]
impl<R: RunRepository + 'static> DiscoverySource for WorkflowSource<R> {
    type Range = WorkflowDiscoveryRange;
    type Position = WorkflowDispatchPosition;
    type Candidate = WorkflowDispatchRow;

    async fn page(
        &self,
        range: &Self::Range,
        after: Option<Self::Position>,
        limit: NonZeroU32,
    ) -> Result<DiscoveryPage<Self::Position, Self::Candidate>> {
        let page = self
            .repo
            .list_workflow_dispatch_page(range, after, limit)
            .await?;
        Ok(DiscoveryPage {
            candidates: page.candidates,
            last_examined: page.last_examined,
            exhausted: page.exhausted,
        })
    }
}

impl<R: RunRepository + 'static> WorkflowSource<R> {
    async fn resolve(&self, row: &WorkflowDispatchRow) -> Result<Option<DispatchableWorkflowTask>> {
        let mut queue = QueueKey {
            namespace_id: row.namespace_id,
            task_queue: row.queue_name.clone(),
            task_kind: tokeira_types::TaskKind::Workflow,
            deployment: None,
            build_id: None,
        };
        match &row.routing {
            WorkflowDispatchRouting::Exact {
                deployment,
                build_id,
            } => {
                queue.deployment = Some(deployment.clone());
                queue.build_id = build_id.clone();
            }
            WorkflowDispatchRouting::Live => {
                // Hydration is serial within the page. Never retain 64 decoded
                // histories, and never turn a registry-selected target into intent.
                let LoadedRun::Existing(state) =
                    self.repo.load_run(row.incarnation.run_key).await?
                else {
                    return Ok(None);
                };
                let Some(current) = derive_workflow_dispatch(&state, row.execution_home) else {
                    return Ok(None);
                };
                if current != *row {
                    return Ok(None);
                }
                let registry = self
                    .registry
                    .read()
                    .expect("deployment registry lock poisoned")
                    .clone();
                if let Some(registry) = registry {
                    let preferred = state
                        .effective_deployment()
                        .map(|version| version.deployment_name.as_str())
                        .or(state.worker_deployment_name.as_deref());
                    let routing = registry
                        .workflow_task_routing_config(
                            state.namespace_id,
                            &queue.task_queue.0,
                            preferred,
                        )
                        .await?;
                    let target = resolve_workflow_task_target_version(&routing, &state);
                    if let Some(version) = target.deployment_version {
                        queue.deployment =
                            Some(tokeira_types::DeploymentId(version.deployment_name));
                        queue.build_id = Some(tokeira_types::BuildId(version.build_id));
                    }
                }
            }
        }
        Ok(Some(DispatchableWorkflowTask {
            run_key: row.incarnation.run_key,
            logical_seq: row.incarnation.logical_seq,
            queue,
            sticky_preferred: None,
            normal_queue: None,
            sticky_deadline: row.schedule_to_start_deadline,
            priority: row.priority.clone(),
            order: None,
        }))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct RangeKey {
    queue: QueueKey,
    live: bool,
    home: ShardId,
    generation: u64,
}

impl RangeKey {
    fn range(&self) -> WorkflowDiscoveryRange {
        WorkflowDiscoveryRange {
            namespace_id: self.queue.namespace_id,
            queue_name: self.queue.task_queue.clone(),
            routing: match &self.queue.deployment {
                Some(deployment) if !self.live => WorkflowDispatchRouting::Exact {
                    deployment: deployment.clone(),
                    build_id: self.queue.build_id.clone(),
                },
                _ => WorkflowDispatchRouting::Live,
            },
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct WorkflowPass {
    registration: u64,
    cancel: CancellationToken,
    pub(crate) after: Option<WorkflowDispatchPosition>,
    pub(crate) admissions: usize,
    pub(crate) pages: usize,
    pub(crate) examined: usize,
}

#[derive(Debug)]
struct Registration {
    id: u64,
    cancel: CancellationToken,
    demands: HashMap<QueueKey, usize>,
    home: QueueHome,
    last_used: Instant,
    next_pass: Instant,
    turn: u64,
    pass: Option<WorkflowPass>,
    running: bool,
    capacity_blocked: bool,
}

#[derive(Debug, Default)]
struct RegistryState {
    ranges: HashMap<RangeKey, Registration>,
    turn: u64,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct DiscoveryRegistry {
    state: Arc<Mutex<RegistryState>>,
    pub(crate) wake: Arc<Notify>,
}

#[derive(Debug)]
pub(crate) struct PollDemandGuard {
    registry: DiscoveryRegistry,
    keys: Vec<RangeKey>,
    queue: QueueKey,
}

impl Drop for PollDemandGuard {
    fn drop(&mut self) {
        let now = Instant::now();
        let mut state = self
            .registry
            .state
            .lock()
            .expect("discovery registry lock poisoned");
        for key in &self.keys {
            if let Some(registration) = state.ranges.get_mut(key) {
                if let Some(count) = registration.demands.get_mut(&self.queue) {
                    *count -= 1;
                    if *count == 0 {
                        registration.demands.remove(&self.queue);
                    }
                }
                registration.last_used = now;
            }
        }
        drop(state);
        self.registry.wake.notify_one();
    }
}

impl DiscoveryRegistry {
    #[cfg(test)]
    pub(crate) fn registered_ranges(&self) -> usize {
        self.state.lock().unwrap().ranges.len()
    }
    pub(crate) fn register_poll(
        &self,
        queue: QueueKey,
        home: QueueHome,
        now: Instant,
    ) -> PollDemandGuard {
        let mut live_queue = queue.clone();
        live_queue.deployment = None;
        live_queue.build_id = None;
        let mut keys = vec![RangeKey {
            queue: live_queue,
            live: true,
            home: home.id,
            generation: home.generation,
        }];
        // Unversioned state is classified Live, so there is no unversioned Exact
        // row to scan. All worker versions share the one family Live traversal.
        if queue.deployment.is_some() {
            keys.push(RangeKey {
                queue: queue.clone(),
                live: false,
                home: home.id,
                generation: home.generation,
            });
        }
        let mut state = self.state.lock().expect("discovery registry lock poisoned");
        for key in &keys {
            let turn = state.turn;
            state.turn += 1;
            let registration = state
                .ranges
                .entry(key.clone())
                .or_insert_with(|| Registration {
                    id: turn,
                    cancel: CancellationToken::new(),
                    demands: HashMap::new(),
                    home: home.clone(),
                    last_used: now,
                    next_pass: now,
                    turn,
                    pass: None,
                    running: false,
                    capacity_blocked: false,
                });
            *registration.demands.entry(queue.clone()).or_default() += 1;
            registration.last_used = now;
        }
        drop(state);
        self.wake.notify_one();
        PollDemandGuard {
            registry: self.clone(),
            keys,
            queue,
        }
    }

    fn compatible(&self, key: &RangeKey, expected_id: u64, queue: &QueueKey) -> bool {
        self.state
            .lock()
            .expect("discovery registry lock poisoned")
            .ranges
            .get(key)
            .is_some_and(|registration| {
                registration.id == expected_id
                    && !registration.home.cancel.is_cancelled()
                    && registration.demands.contains_key(queue)
            })
    }

    fn select(&self, now: Instant) -> Option<(RangeKey, QueueHome, WorkflowPass)> {
        let mut state = self.state.lock().expect("discovery registry lock poisoned");
        let key = state
            .ranges
            .iter()
            .filter(|(_, entry)| {
                !entry.running
                    && !entry.demands.is_empty()
                    && !entry.home.cancel.is_cancelled()
                    && (entry.pass.is_some() || now >= entry.next_pass)
            })
            .min_by_key(|(_, entry)| entry.turn)
            .map(|(key, _)| key.clone())?;
        let turn = state.turn;
        state.turn += 1;
        let entry = state
            .ranges
            .get_mut(&key)
            .expect("selected registration exists");
        entry.running = true;
        entry.turn = turn;
        let pass = entry.pass.take().unwrap_or_else(|| WorkflowPass {
            registration: entry.id,
            cancel: entry.cancel.clone(),
            ..WorkflowPass::default()
        });
        Some((key, entry.home.clone(), pass))
    }

    fn complete(&self, key: &RangeKey, outcome: SliceOutcome, pass: WorkflowPass, now: Instant) {
        let mut state = self.state.lock().expect("discovery registry lock poisoned");
        if let Some(entry) = state.ranges.get_mut(key)
            && entry.id == pass.registration
        {
            entry.running = false;
            entry.capacity_blocked = outcome == SliceOutcome::Capacity;
            entry.pass =
                (outcome == SliceOutcome::Yield || outcome == SliceOutcome::Idle).then_some(pass);
            entry.next_pass = now + DISCOVERY_PERIOD;
        }
    }

    fn retire(&self, now: Instant) -> Vec<(QueueHome, QueueKey)> {
        let mut state = self.state.lock().expect("discovery registry lock poisoned");
        let mut idle: HashMap<ShardId, Vec<(Instant, u64, RangeKey)>> = HashMap::new();
        for (key, entry) in &state.ranges {
            if entry.demands.is_empty() {
                idle.entry(key.home)
                    .or_default()
                    .push((entry.last_used, entry.turn, key.clone()));
            }
        }
        let mut evict = std::collections::HashSet::new();
        for entries in idle.values_mut() {
            entries.sort_by_key(|(at, turn, _)| (*at, *turn));
            for (_, _, key) in entries
                .iter()
                .take(entries.len().saturating_sub(IDLE_CAPACITY))
            {
                evict.insert(key.clone());
            }
        }
        let mut retired = Vec::new();
        state.ranges.retain(|key, entry| {
            let remove = entry.home.cancel.is_cancelled()
                || (entry.demands.is_empty()
                    && (now.duration_since(entry.last_used) >= IDLE_GRACE || evict.contains(key)));
            if remove {
                // A retired in-flight page must not finish into a registration
                // recreated for the same range by a later poll.
                entry.cancel.cancel();
                retired.push((entry.home.clone(), key.queue.clone()));
            }
            !remove
        });
        retired
    }

    fn capacity_returned(&self, now: Instant) {
        for entry in self
            .state
            .lock()
            .expect("discovery registry lock poisoned")
            .ranges
            .values_mut()
        {
            if entry.capacity_blocked {
                entry.next_pass = now;
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SliceOutcome {
    Yield,
    Complete,
    Capacity,
    Idle,
    Cancelled,
}

async fn run_slice<R: RunRepository + 'static>(
    source: &WorkflowSource<R>,
    registry: &DiscoveryRegistry,
    broker: &InMemoryBroker,
    key: &RangeKey,
    home: &QueueHome,
    pass: &mut WorkflowPass,
) -> Result<SliceOutcome> {
    let mut reservation = broker
        .reserve_offers(home, PASS_ADMISSIONS - pass.admissions)
        .await;
    if reservation.remaining() == 0 {
        return Ok(SliceOutcome::Capacity);
    }
    let range = key.range();
    debug_assert_eq!(SLICE_PAGES, 1, "one page per scheduler slice");
    let page = source
        .page(
            &range,
            pass.after,
            NonZeroU32::new(PAGE_SIZE).expect("nonzero page size"),
        )
        .await?;
    pass.pages += 1;
    for row in page.candidates {
        if home.cancel.is_cancelled() || pass.cancel.is_cancelled() {
            return Ok(SliceOutcome::Cancelled);
        }
        // Pausing demand retains the last fully examined position, even when a
        // poll disappears during hydration. Renewal resumes the same pass.
        let active = registry
            .state
            .lock()
            .expect("discovery registry lock poisoned")
            .ranges
            .get(key)
            .is_some_and(|entry| entry.id == pass.registration && !entry.demands.is_empty());
        if !active {
            return Ok(SliceOutcome::Idle);
        }
        pass.examined += 1;
        let position = row.position();
        if range.matches(&row)
            && !broker
                .knows_offer((row.incarnation.run_key, row.incarnation.logical_seq))
                .await
            && let Some(task) = source.resolve(&row).await?
            && registry.compatible(key, pass.registration, &task.queue)
        {
            match broker
                .publish_discovered(task, row.scheduled_at, &mut reservation)
                .await
            {
                OfferAdmission::Admitted => pass.admissions += 1,
                OfferAdmission::Known => {}
                OfferAdmission::Full => return Ok(SliceOutcome::Capacity),
            }
        }
        pass.after = Some(position);
        if pass.admissions == PASS_ADMISSIONS {
            return Ok(SliceOutcome::Complete);
        }
        if reservation.remaining() == 0 {
            return Ok(SliceOutcome::Capacity);
        }
    }
    pass.after = page.last_examined.or(pass.after);
    Ok(if page.exhausted {
        SliceOutcome::Complete
    } else {
        SliceOutcome::Yield
    })
}

pub(crate) async fn run_discovery<R: RunRepository + 'static>(
    source: WorkflowSource<R>,
    registry: DiscoveryRegistry,
    broker: InMemoryBroker,
    cancel: CancellationToken,
) {
    let source = Arc::new(source);
    let mut slices = tokio::task::JoinSet::new();
    loop {
        let now = Instant::now();
        broker.expire_offers(now).await;
        for (home, queue) in registry.retire(now) {
            broker.retire_offers(&home, &queue).await;
        }
        while slices.len() < CONCURRENT_QUERIES {
            let Some((key, home, mut pass)) = registry.select(now) else {
                break;
            };
            let source = source.clone();
            let registry = registry.clone();
            let broker = broker.clone();
            let cancel = cancel.clone();
            let retired = pass.cancel.clone();
            slices.spawn(async move {
                let outcome = tokio::select! {
                    _ = cancel.cancelled() => SliceOutcome::Cancelled,
                    _ = retired.cancelled() => SliceOutcome::Cancelled,
                    _ = home.cancel.cancelled() => SliceOutcome::Cancelled,
                    result = run_slice(source.as_ref(), &registry, &broker, &key, &home, &mut pass) => match result {
                        Ok(outcome) => outcome,
                        Err(error) => { tracing::warn!(?error, "workflow discovery page failed; next pass starts at head"); SliceOutcome::Complete }
                    }
                };
                registry.complete(&key, outcome, pass, Instant::now());
            });
        }
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = registry.wake.notified() => {},
            _ = broker.offer_capacity_changed() => { registry.capacity_returned(Instant::now()); },
            _ = slices.join_next(), if !slices.is_empty() => {},
            _ = tokio::time::sleep(DISCOVERY_PERIOD) => {},
        }
    }
    slices.abort_all();
    while slices.join_next().await.is_some() {}
}

#[cfg(test)]
pub(crate) mod tests;
