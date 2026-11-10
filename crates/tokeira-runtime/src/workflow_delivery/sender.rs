//! Bounded scheduling shared by publication and recovery. Retries and fallback
//! re-enter the scheduler, releasing their old destination's concurrency slot.

use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    sync::{Arc, Mutex},
};

use time::OffsetDateTime;
use tokeira_storage::RunRepository;
use tokeira_types::{LogicalTaskSeq, RunKey, ShardId};
use tokio::{
    sync::Notify,
    task::JoinSet,
    time::{Duration, Instant},
};

use super::{
    RemoteWorkflowOfferSink, StickyUnavailableReason, WorkflowDelivery, WorkflowOffer,
    WorkflowOfferError, WorkflowOfferOutcome,
};
use crate::{
    discovery::{QueueHome, QueueHomeProvider},
    shard::ShardAcquisition,
    workflow_offers::OfferSource,
};

pub(super) const ENTRIES: usize = 1_024;
pub(super) const BYTES: usize = 16 * 1024 * 1024;
const CONCURRENCY: usize = 32;
const PER_DESTINATION: usize = 8;
pub(super) const ATTEMPT: Duration = Duration::from_millis(500);
pub(super) const TOTAL: Duration = Duration::from_secs(1);
const MAX_ATTEMPTS: u8 = 3;
type Key = (RunKey, LogicalTaskSeq);
type Scope = (ShardId, u64);

#[cfg(test)]
#[path = "sender_tests.rs"]
mod tests;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum PublicationSource {
    Publication,
    Recovery,
}
impl PublicationSource {
    fn label(self) -> &'static str {
        match self {
            Self::Publication => "publication",
            Self::Recovery => "recovery",
        }
    }
    fn index(self) -> usize {
        match self {
            Self::Publication => 0,
            Self::Recovery => 1,
        }
    }
    fn other(self) -> Self {
        match self {
            Self::Publication => Self::Recovery,
            Self::Recovery => Self::Publication,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EnqueueOutcome {
    Queued,
    Coalesced,
    Refused,
}

pub(super) fn record(source: PublicationSource, outcome: &'static str) {
    metrics::counter!(crate::metrics::WORKFLOW_OFFER_DELIVERY_TOTAL, "source" => source.label(), "outcome" => outcome).increment(1);
}
pub(super) fn record_result(
    source: PublicationSource,
    result: Result<WorkflowOfferOutcome, WorkflowOfferError>,
) {
    record(
        source,
        match result {
            Ok(WorkflowOfferOutcome::Admitted) => "admitted",
            Ok(WorkflowOfferOutcome::Known) => "known",
            Ok(WorkflowOfferOutcome::Full) => "capacity",
            Ok(WorkflowOfferOutcome::NotHome) => "ownership",
            Ok(WorkflowOfferOutcome::Expired) => "expired",
            Ok(WorkflowOfferOutcome::StickyUnavailable(_)) => "sticky_routing",
            Err(error) => error.label(),
        },
    );
}
pub(super) fn record_sticky(reason: StickyUnavailableReason) {
    metrics::counter!(crate::metrics::WORKFLOW_OFFER_STICKY_FALLBACK_TOTAL, "reason" => match reason {
        StickyUnavailableReason::NoRecentPoller => "no_recent_poller", StickyUnavailableReason::DeploymentChanged => "deployment_changed",
    }).increment(1);
}
fn saturation(bound: &'static str) {
    metrics::counter!(crate::metrics::WORKFLOW_OFFER_SENDER_SATURATION_TOTAL, "bound" => bound)
        .increment(1);
}

#[derive(Debug)]
struct Work {
    offer: WorkflowOffer,
    acquisition: ShardAcquisition,
    source: PublicationSource,
    destination: ShardId,
    bytes: usize,
    deadline: Instant,
    ready_at: Instant,
    refresh: bool,
    refreshed: bool,
    attempts: u8,
    fallback: bool,
}
impl Work {
    fn key(&self) -> Key {
        (
            self.offer.hint.task.run_key,
            self.offer.hint.task.logical_seq,
        )
    }
    fn scope(&self) -> Scope {
        (self.acquisition.shard_id, self.acquisition.generation)
    }
}
#[derive(Debug)]
struct State {
    pending: BTreeMap<(PublicationSource, ShardId), VecDeque<Work>>,
    keys: HashSet<Key>,
    bytes: usize,
    active: BTreeMap<ShardId, usize>,
    acquisitions: BTreeMap<Scope, (ShardAcquisition, usize)>,
    next_source: PublicationSource,
    last_home: [Option<ShardId>; 2],
}
impl Default for State {
    fn default() -> Self {
        Self {
            pending: BTreeMap::new(),
            keys: HashSet::new(),
            bytes: 0,
            active: BTreeMap::new(),
            acquisitions: BTreeMap::new(),
            next_source: PublicationSource::Publication,
            last_home: [None; 2],
        }
    }
}
impl State {
    fn insert(&mut self, work: Work) {
        self.pending
            .entry((work.source, work.destination))
            .or_default()
            .push_back(work);
    }
    fn release(&mut self, work: &Work) {
        self.keys.remove(&work.key());
        self.bytes -= work.bytes;
        let scope = work.scope();
        let (_, count) = self
            .acquisitions
            .get_mut(&scope)
            .expect("retained work owns an acquisition reference");
        *count -= 1;
        if *count == 0 {
            self.acquisitions.remove(&scope);
        }
    }
    fn prune(&mut self, now: Instant) {
        let mut removed = Vec::new();
        for queue in self.pending.values_mut() {
            let mut keep = VecDeque::new();
            while let Some(work) = queue.pop_front() {
                if !work.acquisition.valid() || work.deadline <= now {
                    removed.push(work);
                } else {
                    keep.push_back(work);
                }
            }
            *queue = keep;
        }
        self.pending.retain(|_, queue| !queue.is_empty());
        for work in removed {
            record(
                work.source,
                if work.acquisition.valid() {
                    "deadline"
                } else {
                    "ownership"
                },
            );
            self.release(&work);
        }
    }
    fn take(&mut self, now: Instant) -> Option<Work> {
        if self.active.values().sum::<usize>() >= CONCURRENCY {
            return None;
        }
        for source in [self.next_source, self.next_source.other()] {
            let homes: Vec<_> = self
                .pending
                .keys()
                .filter(|(s, _)| *s == source)
                .map(|(_, h)| *h)
                .collect();
            let start = homes
                .partition_point(|h| self.last_home[source.index()].is_some_and(|last| *h <= last));
            for home in homes[start..].iter().chain(homes[..start].iter()) {
                if self.active.get(home).copied().unwrap_or(0) >= PER_DESTINATION {
                    continue;
                }
                let queue = self
                    .pending
                    .get_mut(&(source, *home))
                    .expect("enumerated queue");
                if let Some(index) = queue.iter().position(|w| w.ready_at <= now) {
                    let work = queue.remove(index).expect("selected work");
                    if queue.is_empty() {
                        self.pending.remove(&(source, *home));
                    }
                    *self.active.entry(*home).or_default() += 1;
                    self.last_home[source.index()] = Some(*home);
                    self.next_source = source.other();
                    return Some(work);
                }
            }
        }
        None
    }
    fn next_wake(&self) -> Option<Instant> {
        let can_start = self.active.values().sum::<usize>() < CONCURRENCY;
        self.pending
            .iter()
            .flat_map(|((_, home), queue)| {
                let available =
                    can_start && self.active.get(home).copied().unwrap_or(0) < PER_DESTINATION;
                queue.iter().map(move |w| {
                    if available {
                        w.deadline.min(w.ready_at)
                    } else {
                        w.deadline
                    }
                })
            })
            .min()
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct WorkflowOfferSender {
    state: Arc<Mutex<State>>,
    wake: Arc<Notify>,
    #[cfg(test)]
    changed: Arc<Notify>,
}
impl WorkflowOfferSender {
    pub(super) fn spawn<R: RunRepository + 'static>(
        delivery: Arc<WorkflowDelivery<R>>,
        sink: Arc<dyn RemoteWorkflowOfferSink>,
    ) -> Self {
        let sender = Self::default();
        let _task = delivery
            .shutdown
            .spawn(run(sender.clone(), delivery.clone(), sink));
        sender
    }
    // The initial timestamp belongs to the publication, including a preceding
    // local sticky check. Queue residence, refresh and fallback share that budget.
    #[allow(clippy::too_many_arguments)] // Explicit admission operands keep the caller's absolute budget visible.
    pub(super) fn try_enqueue(
        &self,
        offer: WorkflowOffer,
        acquisition: ShardAcquisition,
        source: PublicationSource,
        destination: ShardId,
        started: Instant,
        fallback: bool,
        now_wall: OffsetDateTime,
    ) -> EnqueueOutcome {
        let now = Instant::now();
        let deadline = offer
            .hint
            .task
            .sticky_deadline
            .map_or(started + TOTAL, |at| {
                (started + TOTAL).min(
                    now + Duration::try_from(at - now_wall)
                        .unwrap_or_default()
                        .min(TOTAL),
                )
            });
        let mut work = Work {
            offer,
            acquisition,
            source,
            destination,
            bytes: 0,
            deadline,
            ready_at: now,
            refresh: false,
            refreshed: false,
            attempts: 0,
            fallback,
        };
        let mut state = self.state.lock().expect("offer sender lock poisoned");
        state.prune(now);
        if !work.acquisition.valid() || now >= deadline {
            record(
                source,
                if work.acquisition.valid() {
                    "deadline"
                } else {
                    "ownership"
                },
            );
            return EnqueueOutcome::Refused;
        }
        if state.keys.contains(&work.key()) {
            record(source, "coalesced");
            return EnqueueOutcome::Coalesced;
        }
        if state.keys.len() >= ENTRIES {
            saturation("entries");
            return EnqueueOutcome::Refused;
        }
        let bytes = crate::workflow_offers::serialized_hint_bytes(&work.offer, BYTES);
        work.bytes = bytes;
        if state.bytes.saturating_add(bytes) > BYTES {
            saturation("bytes");
            return EnqueueOutcome::Refused;
        }
        state.bytes += bytes;
        state.keys.insert(work.key());
        state
            .acquisitions
            .entry(work.scope())
            .or_insert_with(|| (work.acquisition.clone(), 0))
            .1 += 1;
        state.insert(work);
        drop(state);
        self.wake.notify_one();
        EnqueueOutcome::Queued
    }
}

async fn run<R: RunRepository + 'static>(
    sender: WorkflowOfferSender,
    delivery: Arc<WorkflowDelivery<R>>,
    sink: Arc<dyn RemoteWorkflowOfferSink>,
) {
    let cancel = delivery.shutdown.cancellation_token();
    let mut jobs = JoinSet::new();
    let mut watchers = JoinSet::new();
    let mut watching = BTreeMap::new();
    loop {
        let notified = sender.wake.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let next = {
            let mut state = sender.state.lock().expect("offer sender lock poisoned");
            state.prune(Instant::now());
            // Watch each retained execution acquisition once, not once per hint.
            // Remove idle watchers so successive runs cannot accumulate tasks.
            watching.retain(|scope, handle: &mut tokio::task::AbortHandle| {
                if state.acquisitions.contains_key(scope) {
                    true
                } else {
                    handle.abort();
                    false
                }
            });
            for (scope, (acquisition, _)) in &state.acquisitions {
                watching.entry(*scope).or_insert_with(|| {
                    let scope = *scope;
                    let home = QueueHome {
                        id: acquisition.shard_id,
                        generation: acquisition.generation,
                        cancel: acquisition.cancel.clone(),
                        deadline: acquisition.deadline.clone(),
                    };
                    watchers.spawn(async move {
                        home.retired().await;
                        scope
                    })
                });
            }
            while let Some(work) = state.take(Instant::now()) {
                let delivery = delivery.clone();
                let sink = sink.clone();
                jobs.spawn(async move { attempt(work, delivery, sink).await });
            }
            state.next_wake()
        };
        #[cfg(test)]
        sender.changed.notify_waiters();
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = notified => {},
            _ = watchers.join_next(), if !watchers.is_empty() => {},
            result = jobs.join_next(), if !jobs.is_empty() => {
                if let Some(Ok((mut work, old_destination, retry))) = result {
                    let mut state = sender.state.lock().expect("offer sender lock poisoned");
                    let count = state.active.get_mut(&old_destination).expect("attempt owns concurrency slot");
                    *count -= 1;
                    if *count == 0 { state.active.remove(&old_destination); }
                    let bytes = crate::workflow_offers::serialized_hint_bytes(&work.offer, BYTES);
                    if retry && work.acquisition.valid() && Instant::now() < work.deadline && (state.bytes - work.bytes).saturating_add(bytes) <= BYTES {
                        state.bytes = state.bytes - work.bytes + bytes;
                        work.bytes = bytes;
                        state.insert(work);
                    } else { state.release(&work); }
                }
            }
            _ = async { match next { Some(at) => tokio::time::sleep_until(at).await, None => std::future::pending().await } } => {},
        }
    }
    jobs.abort_all();
    watchers.abort_all();
    while jobs.join_next().await.is_some() {}
    while watchers.join_next().await.is_some() {}
    *sender.state.lock().expect("offer sender lock poisoned") = State::default();
}

async fn attempt<R: RunRepository + 'static>(
    mut work: Work,
    delivery: Arc<WorkflowDelivery<R>>,
    sink: Arc<dyn RemoteWorkflowOfferSink>,
) -> (Work, ShardId, bool) {
    let destination = work.destination;
    let home = QueueHome {
        id: work.acquisition.shard_id,
        generation: work.acquisition.generation,
        cancel: work.acquisition.cancel.clone(),
        deadline: work.acquisition.deadline.clone(),
    };
    let deadline = work.deadline.min(Instant::now() + ATTEMPT);
    let result = tokio::select! {
        biased;
        _ = home.retired() => { record(work.source, "ownership"); return (work, destination, false); },
        result = tokio::time::timeout_at(deadline, async {
            if work.fallback {
                // Routing fallback is a separate scheduled step. It never uses a
                // second home's RPC slot while still charged to the sticky home.
                return delivery.fallback(work.offer.clone()).await.map(Attempt::Fallback);
            }
            work.attempts += 1;
            if delivery.homes.local_home(&work.offer.hint.task.queue).is_some() {
                delivery.receive(&work.offer, destination, OfferSource::Local).await.map(Attempt::Reply)
            } else { sink.offer(&work.offer, destination, work.refresh).await.map(Attempt::Reply) }
        }) => result.unwrap_or(Err(WorkflowOfferError::Deadline)),
    };
    work.refresh = false;
    let retry = match result {
        Ok(Attempt::Fallback(Some(offer))) => {
            work.destination = delivery.destination(&offer.hint.task.queue);
            work.offer = offer;
            work.fallback = false;
            true
        }
        Ok(Attempt::Fallback(None)) => {
            record(work.source, "obsolete");
            false
        }
        Ok(Attempt::Reply(WorkflowOfferOutcome::Admitted)) => {
            record(work.source, "admitted");
            false
        }
        Ok(Attempt::Reply(WorkflowOfferOutcome::Known)) => {
            record(work.source, "known");
            false
        }
        Ok(Attempt::Reply(WorkflowOfferOutcome::Full)) => {
            record(work.source, "capacity");
            false
        }
        Ok(Attempt::Reply(WorkflowOfferOutcome::Expired)) => {
            record(work.source, "expired");
            false
        }
        Ok(Attempt::Reply(WorkflowOfferOutcome::NotHome)) => {
            record(work.source, "ownership");
            if work.refreshed {
                false
            } else {
                work.refreshed = true;
                work.refresh = true;
                true
            }
        }
        Ok(Attempt::Reply(WorkflowOfferOutcome::StickyUnavailable(reason))) => {
            record_sticky(reason);
            work.fallback = work.offer.hint.task.sticky_preferred.is_some();
            work.fallback
        }
        Err(error) => {
            record(work.source, error.label());
            matches!(
                error,
                WorkflowOfferError::Transport
                    | WorkflowOfferError::Routing
                    | WorkflowOfferError::Deadline
            ) && work.attempts < MAX_ATTEMPTS
        }
    };
    // Per-incarnation jitter avoids synchronizing different runs after movement;
    // deterministic input keeps finite scheduler tests reproducible.
    let jitter = (work.offer.hint.task.run_key.0.as_u128() as u64)
        .wrapping_add(work.offer.hint.task.logical_seq.0)
        .wrapping_add(u64::from(work.attempts) * 17)
        % 41;
    work.ready_at = Instant::now() + Duration::from_millis(10 + jitter);
    let retry = retry && work.attempts < MAX_ATTEMPTS;
    (work, destination, retry)
}

enum Attempt {
    Reply(WorkflowOfferOutcome),
    Fallback(Option<WorkflowOffer>),
}
