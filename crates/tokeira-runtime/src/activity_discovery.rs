//! Read-only activity discovery at queue homes, across every execution home.
//!
//! Every worker version shares a family cursor and fixed eligibility cutoff.
//! Skips advance that cursor; capacity and demand pauses preserve it. The common
//! discovery scheduler owns the node's query slots, rather than one pool per kind.

use std::{
    collections::HashMap,
    num::NonZeroU32,
    sync::{Arc, Mutex, RwLock},
};

use anyhow::Result;
use async_trait::async_trait;
use time::OffsetDateTime;
use tokeira_clock::Clock;
use tokeira_kernel::LoadedRun;
use tokeira_storage::{
    ActivityDiscoveryRange, ActivityDispatchPage, ActivityDispatchPosition, ActivityDispatchRow,
    DispatchableActivityTask, RunRepository, derive_activity_dispatch,
};
use tokeira_types::{QueueKey, ShardId, execution_home_bundle};
use tokio::{sync::Notify, time::Instant};
use tokio_util::sync::CancellationToken;

use crate::{
    InMemoryActivityBroker,
    deployment_registry::DeploymentRegistry,
    discovery::{
        DISCOVERY_PERIOD, IDLE_CAPACITY, IDLE_GRACE, PAGE_SIZE, PASS_ADMISSIONS, QueueHome,
        SliceOutcome,
    },
    runtime::workflow_task::route_activity_task_queue,
    shard::ShardOwner,
    workflow_offers::OfferAdmission,
};

const ACTIVE_CAPACITY: usize = 4_096;

#[cfg(test)]
mod tests;

/// A queue home has no permit for another active activity family. Existing
/// polls remain registered; the edge maps this retryable refusal to RESOURCE_EXHAUSTED.
#[derive(Debug, thiserror::Error)]
#[error("activity queue-home demand capacity exhausted; retry poll")]
pub struct ActivityDemandCapacity;

pub(crate) struct ActivityDiscovery<R> {
    pub(crate) source: Arc<ActivitySource<R>>,
    pub(crate) registry: ActivityDiscoveryRegistry,
    pub(crate) broker: InMemoryActivityBroker,
    /// Judges activity dispatch deadlines; the runtime's clock.
    pub(crate) clock: Arc<dyn Clock>,
}

impl<R: RunRepository + 'static> ActivityDiscovery<R> {
    /// Admit at most one slice to the shared node scheduler. Rotating families
    /// there, and FIFO turns here, prevents either task kind monopolizing slots.
    pub(crate) fn try_spawn(
        &self,
        slices: &mut tokio::task::JoinSet<()>,
        cancel: &CancellationToken,
        now: Instant,
    ) -> bool {
        let Some((key, home, mut pass)) = self.registry.select(now, self.clock.now()) else {
            return false;
        };
        let source = self.source.clone();
        let registry = self.registry.clone();
        let broker = self.broker.clone();
        let cancel = cancel.clone();
        let retired = pass.cancel.clone();
        slices.spawn(async move {
            let outcome = tokio::select! {
                _ = cancel.cancelled() => SliceOutcome::Cancelled,
                _ = retired.cancelled() => SliceOutcome::Cancelled,
                _ = home.cancel.cancelled() => SliceOutcome::Cancelled,
                result = run_slice(source.as_ref(), &registry, &broker, &key, &home, &mut pass) => match result {
                    Ok(outcome) => outcome,
                    Err(error) => { tracing::warn!(?error, "activity discovery page failed; next pass starts at head"); SliceOutcome::Complete }
                }
            };
            registry.complete(&key, outcome, pass, Instant::now());
        });
        true
    }
}

/// Narrow read-only seam also used by adversarial traversal fixtures.
#[async_trait]
pub(crate) trait ActivityDiscoverySource: Send + Sync {
    async fn page(
        &self,
        range: &ActivityDiscoveryRange,
        cutoff: OffsetDateTime,
        after: Option<ActivityDispatchPosition>,
        limit: NonZeroU32,
    ) -> Result<ActivityDispatchPage>;
    async fn resolve(&self, row: &ActivityDispatchRow) -> Result<Option<DispatchableActivityTask>>;
}

pub(crate) struct ActivitySource<R> {
    pub(crate) repo: Arc<R>,
    pub(crate) registry: Arc<RwLock<Option<DeploymentRegistry>>>,
    pub(crate) owner: Arc<RwLock<ShardOwner>>,
}

#[async_trait]
impl<R: RunRepository + 'static> ActivityDiscoverySource for ActivitySource<R> {
    async fn page(
        &self,
        range: &ActivityDiscoveryRange,
        cutoff: OffsetDateTime,
        after: Option<ActivityDispatchPosition>,
        limit: NonZeroU32,
    ) -> Result<ActivityDispatchPage> {
        self.repo
            .list_activity_dispatch_page(range, cutoff, after, limit)
            .await
    }

    async fn resolve(&self, row: &ActivityDispatchRow) -> Result<Option<DispatchableActivityTask>> {
        let LoadedRun::Existing(state) = tokeira_storage::read_probe::scope(
            "activity-discovery/resolve",
            self.repo.load_run(row.incarnation.run_key),
        )
        .await?
        else {
            return Ok(None);
        };
        let count = self
            .owner
            .read()
            .expect("shard owner lock poisoned")
            .shard_count();
        let home = execution_home_bundle(
            state.namespace_id.0.as_bytes(),
            state.workflow_id.0.as_bytes(),
            count,
        );
        let desired = derive_activity_dispatch(&state, home)?;
        if desired.get(&row.key) != Some(row) {
            return Ok(None);
        }
        let registry = self
            .registry
            .read()
            .expect("deployment registry lock poisoned")
            .clone();
        let (queue, dispatch_revision) = route_activity_task_queue(
            registry.as_ref(),
            &state,
            row.queue.clone(),
            row.dispatch_revision,
        )
        .await?;
        // Discovery never evaluates a mutating workflow rule. Even a newly
        // matching rule must reach the owner, whose fenced start applies it.
        Ok(Some(DispatchableActivityTask {
            queue,
            dispatch_revision,
            ..row.task()
        }))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct RangeKey {
    family: QueueKey,
    home: ShardId,
    generation: u64,
}

#[derive(Debug)]
pub(crate) struct ActivityPass {
    registration: u64,
    pub(crate) cancel: CancellationToken,
    cutoff: OffsetDateTime,
    after: Option<ActivityDispatchPosition>,
}

#[derive(Debug)]
struct Registration {
    id: u64,
    cancel: CancellationToken,
    demands: HashMap<QueueKey, usize>,
    home: QueueHome,
    last_used: Instant,
    next_slice: Instant,
    turn: u64,
    pass: Option<ActivityPass>,
    running: bool,
    capacity_blocked: bool,
}

#[derive(Debug, Default)]
struct RegistryState {
    ranges: HashMap<RangeKey, Registration>,
    turn: u64,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ActivityDiscoveryRegistry {
    #[cfg(test)]
    pub(crate) test_gate: Arc<crate::discovery::test_gate::TestPassGate>,
    state: Arc<Mutex<RegistryState>>,
    pub(crate) wake: Arc<Notify>,
}

#[derive(Debug)]
pub(crate) struct ActivityPollDemand {
    registry: ActivityDiscoveryRegistry,
    key: RangeKey,
    registration: u64,
    queue: QueueKey,
}

impl Drop for ActivityPollDemand {
    fn drop(&mut self) {
        let mut state = self
            .registry
            .state
            .lock()
            .expect("activity discovery lock poisoned");
        if let Some(entry) = state.ranges.get_mut(&self.key)
            && entry.id == self.registration
        {
            if let Some(count) = entry.demands.get_mut(&self.queue) {
                *count -= 1;
                if *count == 0 {
                    entry.demands.remove(&self.queue);
                }
            }
            entry.last_used = Instant::now();
        }
        // Enforce the idle bound on the drop itself, including a burst that
        // finishes before the scheduler next runs. Cancelling the registration
        // also invalidates its ready offers and outstanding page reservations.
        loop {
            let idle: Vec<_> = state
                .ranges
                .iter()
                .filter(|(key, entry)| {
                    key.home == self.key.home
                        && key.generation == self.key.generation
                        && entry.demands.is_empty()
                })
                .collect();
            if idle.len() <= IDLE_CAPACITY {
                break;
            }
            let oldest = idle
                .into_iter()
                .min_by_key(|(_, entry)| (entry.last_used, entry.turn))
                .map(|(key, _)| key.clone())
                .expect("overfull idle registry has an oldest entry");
            if let Some(entry) = state.ranges.remove(&oldest) {
                entry.cancel.cancel();
            }
        }
        drop(state);
        self.registry.wake.notify_one();
    }
}

impl ActivityDiscoveryRegistry {
    /// Coalesce registration changes and failed matches with the existing pass.
    /// Never reset its cursor: repeated notifications must not starve its tail.
    pub(crate) fn wake_family(&self, queue: &QueueKey, now: Instant) {
        let mut state = self.state.lock().expect("activity discovery lock poisoned");
        for (key, entry) in &mut state.ranges {
            if key.family.namespace_id == queue.namespace_id
                && key.family.task_queue == queue.task_queue
                && !entry.running
                && !entry.capacity_blocked
            {
                entry.next_slice = now;
            }
        }
        drop(state);
        self.wake.notify_one();
    }

    pub(crate) fn register_poll(
        &self,
        queue: QueueKey,
        home: QueueHome,
        now: Instant,
    ) -> Result<ActivityPollDemand> {
        anyhow::ensure!(
            !home.cancel.is_cancelled(),
            "activity queue-home acquisition ended"
        );
        let family = QueueKey {
            deployment: None,
            build_id: None,
            ..queue.clone()
        };
        let key = RangeKey {
            family,
            home: home.id,
            generation: home.generation,
        };
        let mut state = self.state.lock().expect("activity discovery lock poisoned");
        let already_active = state
            .ranges
            .get(&key)
            .is_some_and(|entry| !entry.demands.is_empty());
        let active = state
            .ranges
            .values()
            .filter(|entry| {
                entry.home.id == home.id
                    && entry.home.generation == home.generation
                    && !entry.demands.is_empty()
            })
            .count();
        if !already_active && active >= ACTIVE_CAPACITY {
            return Err(ActivityDemandCapacity.into());
        }
        let turn = state.turn;
        state.turn = state
            .turn
            .checked_add(1)
            .expect("activity discovery turn space exhausted");
        let entry = state
            .ranges
            .entry(key.clone())
            .or_insert_with(|| Registration {
                id: turn,
                cancel: CancellationToken::new(),
                demands: HashMap::new(),
                home,
                last_used: now,
                next_slice: now,
                turn,
                pass: None,
                running: false,
                capacity_blocked: false,
            });
        if entry.demands.is_empty() && !entry.running {
            // Renewal preserves a partial pass, but a new poll need not wait
            // out the idle period of an exhausted pass before rediscovery.
            entry.next_slice = now;
        }
        *entry.demands.entry(queue.clone()).or_default() += 1;
        entry.last_used = now;
        let registration = entry.id;
        drop(state);
        self.wake.notify_one();
        Ok(ActivityPollDemand {
            registry: self.clone(),
            key,
            registration,
            queue,
        })
    }

    fn active(&self, key: &RangeKey, registration: u64) -> bool {
        self.state
            .lock()
            .expect("activity discovery lock poisoned")
            .ranges
            .get(key)
            .is_some_and(|entry| {
                entry.id == registration
                    && !entry.demands.is_empty()
                    && !entry.home.cancel.is_cancelled()
            })
    }

    fn compatible(&self, key: &RangeKey, registration: u64, queue: &QueueKey) -> bool {
        self.state
            .lock()
            .expect("activity discovery lock poisoned")
            .ranges
            .get(key)
            .is_some_and(|entry| {
                entry.id == registration
                    && entry.demands.contains_key(queue)
                    && !entry.home.cancel.is_cancelled()
            })
    }

    pub(crate) fn select(
        &self,
        now: Instant,
        cutoff: OffsetDateTime,
    ) -> Option<(RangeKey, QueueHome, ActivityPass)> {
        #[cfg(test)]
        if self.test_gate.is_held() {
            return None;
        }
        let mut state = self.state.lock().expect("activity discovery lock poisoned");
        let key = state
            .ranges
            .iter()
            .filter(|(_, entry)| {
                !entry.running
                    && !entry.demands.is_empty()
                    && !entry.home.cancel.is_cancelled()
                    && now >= entry.next_slice
            })
            .min_by_key(|(_, entry)| entry.turn)
            .map(|(key, _)| key.clone())?;
        let turn = state.turn;
        state.turn = state
            .turn
            .checked_add(1)
            .expect("activity discovery turn space exhausted");
        let entry = state
            .ranges
            .get_mut(&key)
            .expect("selected activity range exists");
        entry.running = true;
        entry.turn = turn;
        let pass = entry.pass.take().unwrap_or_else(|| ActivityPass {
            registration: entry.id,
            cancel: entry.cancel.clone(),
            cutoff,
            after: None,
        });
        Some((key, entry.home.clone(), pass))
    }

    pub(crate) fn complete(
        &self,
        key: &RangeKey,
        outcome: SliceOutcome,
        pass: ActivityPass,
        now: Instant,
    ) {
        let mut state = self.state.lock().expect("activity discovery lock poisoned");
        if let Some(entry) = state.ranges.get_mut(key)
            && entry.id == pass.registration
        {
            entry.running = false;
            entry.capacity_blocked = outcome == SliceOutcome::Capacity;
            entry.pass = matches!(
                outcome,
                SliceOutcome::Yield | SliceOutcome::Idle | SliceOutcome::Capacity
            )
            .then_some(pass);
            entry.next_slice = if outcome == SliceOutcome::Yield || outcome == SliceOutcome::Idle {
                now
            } else {
                now + DISCOVERY_PERIOD
            };
        }
        #[cfg(test)]
        if outcome == SliceOutcome::Complete {
            self.test_gate.completed();
        }
    }

    pub(crate) fn capacity_returned(&self, now: Instant) {
        for entry in self
            .state
            .lock()
            .expect("activity discovery lock poisoned")
            .ranges
            .values_mut()
        {
            if entry.capacity_blocked {
                entry.next_slice = now;
            }
        }
    }

    pub(crate) fn retire(&self, now: Instant) -> Vec<(QueueHome, QueueKey)> {
        let mut state = self.state.lock().expect("activity discovery lock poisoned");
        let mut idle: HashMap<(ShardId, u64), Vec<(Instant, u64, RangeKey)>> = HashMap::new();
        for (key, entry) in &state.ranges {
            if entry.demands.is_empty() {
                idle.entry((key.home, key.generation)).or_default().push((
                    entry.last_used,
                    entry.turn,
                    key.clone(),
                ));
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
                entry.cancel.cancel();
                retired.push((entry.home.clone(), key.family.clone()));
            }
            !remove
        });
        retired.sort_by(|(a, qa), (b, qb)| {
            (a.id, a.generation, qa.namespace_id, &qa.task_queue.0).cmp(&(
                b.id,
                b.generation,
                qb.namespace_id,
                &qb.task_queue.0,
            ))
        });
        retired
    }
}

pub(crate) async fn run_slice<S: ActivityDiscoverySource>(
    source: &S,
    registry: &ActivityDiscoveryRegistry,
    broker: &InMemoryActivityBroker,
    key: &RangeKey,
    home: &QueueHome,
    pass: &mut ActivityPass,
) -> Result<SliceOutcome> {
    let mut reservation = broker.reserve_offers(home, PASS_ADMISSIONS).await;
    reservation.cancel = pass.cancel.clone();
    if reservation.remaining() == 0 {
        return Ok(SliceOutcome::Capacity);
    }
    let range = ActivityDiscoveryRange {
        namespace_id: key.family.namespace_id,
        queue_name: key.family.task_queue.clone(),
    };
    let page = source
        .page(
            &range,
            pass.cutoff,
            pass.after,
            NonZeroU32::new(PAGE_SIZE).expect("nonzero activity page"),
        )
        .await?;
    for candidate in page.candidates {
        if home.cancel.is_cancelled() || pass.cancel.is_cancelled() {
            return Ok(SliceOutcome::Cancelled);
        }
        if !registry.active(key, pass.registration) {
            return Ok(SliceOutcome::Idle);
        }
        // Payload/hydration failures belong to this row. Its physical position
        // still advances, so an unreadable prefix cannot hide healthy later work.
        let resolved = match candidate.row {
            Ok(row) if range.matches(&row) => {
                if broker.contains_activity_task(&row.task()).await {
                    None
                } else {
                    match source.resolve(&row).await {
                        Ok(task) => task,
                        Err(error) => {
                            tracing::warn!(?error, "activity discovery candidate failed");
                            None
                        }
                    }
                }
            }
            Ok(_) => None,
            Err(error) => {
                tracing::warn!(?error, "activity discovery row decode failed");
                None
            }
        };
        if home.cancel.is_cancelled() || pass.cancel.is_cancelled() {
            return Ok(SliceOutcome::Cancelled);
        }
        if !registry.active(key, pass.registration) {
            return Ok(SliceOutcome::Idle);
        }
        if let Some(task) = resolved
            && registry.compatible(key, pass.registration, &task.queue)
        {
            match broker.publish_discovered(task, &mut reservation).await {
                OfferAdmission::Full => return Ok(SliceOutcome::Capacity),
                OfferAdmission::Admitted | OfferAdmission::Known => {}
            }
        }
        pass.after = Some(candidate.position);
        if reservation.remaining() == 0 {
            return Ok(SliceOutcome::Yield);
        }
    }
    pass.after = page.last_examined.or(pass.after);
    Ok(if page.exhausted {
        SliceOutcome::Complete
    } else {
        SliceOutcome::Yield
    })
}
