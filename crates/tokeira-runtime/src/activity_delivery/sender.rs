//! Activity payload adapter for the workflow offer scheduler's independent queues.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use tokeira_storage::ActivityIncarnation;
use tokeira_types::ShardId;
use tokio::{
    sync::Notify,
    task::JoinSet,
    time::{Duration, Instant},
};

use super::{
    ActivityDelivery, ActivityDeliverySource, ActivityOfferError, ActivityOfferOutcome,
    ActivityQueueOffer, RemoteActivityOfferSink,
};
use crate::{
    activity_offers::incarnation,
    discovery::{QueueHome, QueueHomeProvider},
    shard::ShardAcquisition,
    workflow_delivery::sender::{
        ATTEMPT, BYTES, ENTRIES, EnqueueOutcome, MAX_ATTEMPTS, PublicationSource, SenderOffer,
        State, TOTAL, Work,
    },
};

#[derive(Debug)]
struct ActivityPublication {
    offer: ActivityQueueOffer,
    source: ActivityDeliverySource,
}
impl std::ops::Deref for ActivityPublication {
    type Target = ActivityQueueOffer;
    fn deref(&self) -> &Self::Target {
        &self.offer
    }
}
pub(super) fn record(source: ActivityDeliverySource, outcome: &'static str) {
    metrics::counter!(crate::metrics::ACTIVITY_OFFER_DELIVERY_TOTAL, "source" => source.label(), "outcome" => outcome).increment(1);
}
impl SenderOffer for ActivityPublication {
    type Key = (ActivityIncarnation, ShardId, u64);
    fn sender_key(&self, acquisition: &ShardAcquisition) -> Self::Key {
        (
            incarnation(&self.task),
            acquisition.shard_id,
            acquisition.generation,
        )
    }
    fn record(&self, _source: PublicationSource, outcome: &'static str) {
        record(self.source, outcome);
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct ActivityOfferSender {
    state: Arc<Mutex<State<ActivityPublication>>>,
    wake: Arc<Notify>,
}
impl ActivityOfferSender {
    pub(super) fn spawn(
        delivery: Arc<ActivityDelivery>,
        sink: Arc<dyn RemoteActivityOfferSink>,
    ) -> Self {
        let sender = Self::default();
        let _task = delivery
            .shutdown
            .spawn(run(sender.clone(), delivery.clone(), sink));
        sender
    }
    pub(super) fn try_enqueue(
        &self,
        offer: ActivityQueueOffer,
        acquisition: ShardAcquisition,
        source: impl Into<ActivityDeliverySource>,
        destination: ShardId,
    ) -> EnqueueOutcome {
        let now = Instant::now();
        let source = source.into();
        let work = Work {
            bytes: offer.retained_bytes(),
            offer: ActivityPublication { offer, source },
            acquisition,
            source: source.scheduling_class(),
            destination,
            deadline: now + TOTAL,
            ready_at: now,
            refresh: false,
            refreshed: false,
            attempts: 0,
            fallback: false,
        };
        let mut state = self.state.lock().expect("activity sender lock poisoned");
        state.prune(now);
        if !work.acquisition.valid() {
            return EnqueueOutcome::Refused;
        }
        if state.keys.contains(&work.key()) {
            return EnqueueOutcome::Coalesced;
        }
        if state.keys.len() >= ENTRIES || state.bytes.saturating_add(work.bytes) > BYTES {
            crate::metrics::record_activity_offer_saturation(if state.keys.len() >= ENTRIES {
                "sender_entries"
            } else {
                "sender_bytes"
            });
            record(source, "capacity");
            return EnqueueOutcome::Refused;
        }
        state.bytes += work.bytes;
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

async fn run(
    sender: ActivityOfferSender,
    delivery: Arc<ActivityDelivery>,
    sink: Arc<dyn RemoteActivityOfferSink>,
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
            let mut state = sender.state.lock().expect("activity sender lock poisoned");
            state.prune(Instant::now());
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
                    let home = queue_home(acquisition);
                    watchers.spawn(async move { home.retired().await })
                });
            }
            while let Some(work) = state.take(Instant::now()) {
                jobs.spawn(attempt(work, delivery.clone(), sink.clone()));
            }
            state.next_wake()
        };
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = notified => {},
            _ = watchers.join_next(), if !watchers.is_empty() => {},
            result = jobs.join_next(), if !jobs.is_empty() => {
                if let Some(Ok((work, retry))) = result {
                    let mut state = sender.state.lock().expect("activity sender lock poisoned");
                    let count = state.active.get_mut(&work.destination).expect("attempt owns concurrency slot");
                    *count -= 1;
                    if *count == 0 { state.active.remove(&work.destination); }
                    if retry && work.acquisition.valid() && Instant::now() < work.deadline { state.insert(work); }
                    else { state.release(&work); }
                }
            }
            _ = async { match next { Some(at) => tokio::time::sleep_until(at).await, None => std::future::pending().await } } => {},
        }
    }
    jobs.abort_all();
    watchers.abort_all();
    while jobs.join_next().await.is_some() {}
    while watchers.join_next().await.is_some() {}
    *sender.state.lock().expect("activity sender lock poisoned") = State::default();
}

fn queue_home(acquisition: &ShardAcquisition) -> QueueHome {
    QueueHome {
        id: acquisition.shard_id,
        generation: acquisition.generation,
        cancel: acquisition.cancel.clone(),
        deadline: acquisition.deadline.clone(),
    }
}

async fn attempt(
    mut work: Work<ActivityPublication>,
    delivery: Arc<ActivityDelivery>,
    sink: Arc<dyn RemoteActivityOfferSink>,
) -> (Work<ActivityPublication>, bool) {
    let home = queue_home(&work.acquisition);
    work.attempts += 1;
    let result = tokio::select! {
        biased;
        _ = home.retired() => return (work, false),
        result = tokio::time::timeout_at(work.deadline.min(Instant::now() + ATTEMPT), async {
            if delivery.homes.local_home(&work.offer.task.queue).is_some() {
                Ok(delivery.receive(work.offer.offer.clone(), work.destination, work.offer.source).await)
            } else { sink.offer(&work.offer, work.destination, work.refresh).await }
        }) => result.unwrap_or(Err(ActivityOfferError::Deadline)),
    };
    work.refresh = false;
    let (label, retry) = match result {
        Ok(ActivityOfferOutcome::Admitted) => ("admitted", false),
        Ok(ActivityOfferOutcome::Known) => ("known", false),
        Ok(ActivityOfferOutcome::Full) => ("capacity", false),
        Ok(ActivityOfferOutcome::Expired) => ("expired", false),
        Ok(ActivityOfferOutcome::NotDue) => ("not_due", false),
        Ok(ActivityOfferOutcome::NotHome) => {
            let retry = !work.refreshed;
            work.refresh = retry;
            work.refreshed = true;
            ("ownership", retry)
        }
        Err(ActivityOfferError::Transport) => ("transport", true),
        Err(ActivityOfferError::Routing) => ("routing", true),
        Err(ActivityOfferError::Deadline) => ("deadline", true),
        Err(ActivityOfferError::Authentication) => ("authentication", false),
        Err(ActivityOfferError::Capacity) => ("capacity", false),
        Err(ActivityOfferError::Invalid) => ("invalid", false),
        Err(ActivityOfferError::Unimplemented) => ("unimplemented", false),
    };
    work.offer.record(work.source, label);
    let jitter = (work.offer.task.run_key.0.as_u128() as u64)
        .wrapping_add(work.offer.task.stamp)
        .wrapping_add(u64::from(work.attempts) * 17)
        % 41;
    work.ready_at = Instant::now() + Duration::from_millis(10 + jitter);
    let retry = retry && work.attempts < MAX_ATTEMPTS;
    (work, retry)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{discovery::PlacementQueueHomes, shard::ShardOwner};
    use proptest::prelude::*;
    use std::sync::RwLock;
    use tokeira_types::{NamespaceId, QueueKey, RunKey, ShardEpoch, TaskKind, TaskQueueName};

    fn offer(id: u128) -> ActivityQueueOffer {
        ActivityQueueOffer {
            task: tokeira_storage::DispatchableActivityTask {
                run_key: RunKey(uuid::Uuid::from_u128(id)),
                activity_id: "activity".into(),
                schedule_event_id: 1,
                attempt: 1,
                stamp: 0,
                dispatch_revision: 0,
                priority: None,
                order: None,
                queue: QueueKey {
                    namespace_id: NamespaceId(uuid::Uuid::from_u128(1)),
                    task_queue: TaskQueueName("activity".into()),
                    task_kind: TaskKind::Activity,
                    deployment: None,
                    build_id: None,
                },
            },
            execution_home: ShardId(0),
            dispatch_at: time::OffsetDateTime::UNIX_EPOCH,
            expires_at: time::OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(5),
        }
    }
    fn acquisition() -> ShardAcquisition {
        let mut owner = ShardOwner::new(8);
        owner.record_acquired(ShardId(0), ShardEpoch::ZERO);
        owner.mark_active(ShardId(0));
        owner.acquisition(ShardId(0)).unwrap()
    }
    #[derive(Debug)]
    struct Script {
        replies:
            Mutex<std::collections::VecDeque<Result<ActivityOfferOutcome, ActivityOfferError>>>,
        refreshes: Mutex<Vec<bool>>,
    }
    #[async_trait::async_trait]
    impl RemoteActivityOfferSink for Script {
        async fn offer(
            &self,
            _: &ActivityQueueOffer,
            _: ShardId,
            refresh: bool,
        ) -> Result<ActivityOfferOutcome, ActivityOfferError> {
            self.refreshes.lock().unwrap().push(refresh);
            self.replies.lock().unwrap().pop_front().unwrap()
        }
    }
    #[derive(Debug, Default)]
    struct PendingSink(std::sync::atomic::AtomicUsize);
    #[async_trait::async_trait]
    impl RemoteActivityOfferSink for PendingSink {
        async fn offer(
            &self,
            _: &ActivityQueueOffer,
            _: ShardId,
            _: bool,
        ) -> Result<ActivityOfferOutcome, ActivityOfferError> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            std::future::pending().await
        }
    }
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        // Feature: activity-queue-home-delivery, Property 4: cancellation interrupts active I/O and queue wait consumes the same total budget.
        #[test]
        fn activity_sender_cancellation_and_queue_budget(elapsed in 1u64..499, cancel in any::<bool>()) {
            tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap().block_on(async {
                let sender = ActivityOfferSender::default(); let acquisition = acquisition();
                let homes = Arc::new(PlacementQueueHomes { owner: Arc::new(RwLock::new(ShardOwner::new(8))), counts: RwLock::new((8, 8)) });
                let delivery = ActivityDelivery::new(crate::broker::InMemoryActivityBroker::with_discovery(homes.clone()), homes, crate::RuntimeShutdownHandle::new(), Arc::new(crate::test_clock::TestClock::new()));
                let sink = Arc::new(PendingSink::default());
                sender.try_enqueue(offer(1), acquisition.clone(), PublicationSource::Publication, ShardId(7));
                let work = sender.state.lock().unwrap().take(Instant::now()).unwrap();
                let pending = attempt(work, delivery, sink.clone());
                tokio::pin!(pending);
                prop_assert!(std::future::poll_fn(|cx| std::task::Poll::Ready(pending.as_mut().poll(cx).is_pending())).await);
                prop_assert_eq!(sink.0.load(std::sync::atomic::Ordering::Relaxed), 1);
                tokio::time::advance(Duration::from_millis(elapsed)).await;
                if cancel { acquisition.cancel.cancel(); }
                else { tokio::time::advance(ATTEMPT).await; }
                let (_, retry) = pending.await;
                prop_assert_eq!(retry, !cancel);
                let queued = ActivityOfferSender::default();
                queued.try_enqueue(offer(2), super::tests::acquisition(), PublicationSource::Recovery, ShardId(3));
                tokio::time::advance(TOTAL).await;
                let mut state = queued.state.lock().unwrap(); state.prune(Instant::now());
                prop_assert!(state.take(Instant::now()).is_none()); prop_assert_eq!(state.bytes, 0);
                Ok(())
            })?;
        }
        // Feature: activity-queue-home-delivery, Property 4: independent activity accounting includes active calls, rotation and the original queue budget.
        #[test]
        fn activity_sender_bounds_and_rotation(large in any::<bool>(), homes in 4u32..9) {
            tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap().block_on(async {
                let sender = ActivityOfferSender::default(); let acquisition = acquisition();
                let mut count = 0; let mut bytes = 0;
                loop {
                    let mut offer = offer(count as u128 + 1);
                    if large { offer.task.activity_id = "x".repeat(64 * 1024); }
                    let size = offer.retained_bytes();
                    let result = sender.try_enqueue(offer.clone(), acquisition.clone(), if count % 2 == 0 { PublicationSource::Publication } else { PublicationSource::Recovery }, ShardId((count as u32 / 2) % homes));
                    if result == EnqueueOutcome::Refused { prop_assert!(count == ENTRIES || bytes + size > BYTES); break; }
                    prop_assert_eq!(sender.try_enqueue(offer, acquisition.clone(), PublicationSource::Recovery, ShardId(7)), EnqueueOutcome::Coalesced);
                    count += 1; bytes += size;
                }
                let mut state = sender.state.lock().unwrap();
                prop_assert_eq!(state.bytes, bytes); prop_assert_eq!(state.keys.len(), count);
                for n in 0..32 {
                    let work = state.take(Instant::now()).unwrap();
                    prop_assert_eq!(work.source, if n % 2 == 0 { PublicationSource::Publication } else { PublicationSource::Recovery });
                    prop_assert_eq!(work.deadline, work.ready_at + TOTAL);
                }
                prop_assert!(state.take(Instant::now()).is_none());
                prop_assert!(state.active.values().all(|n| *n <= 8));
                prop_assert_eq!(state.bytes, bytes);
                acquisition.cancel.cancel(); state.prune(Instant::now());
                prop_assert_eq!(state.keys.len(), 32);
                Ok(())
            })?;
        }
        // Feature: activity-queue-home-delivery, Property 4: trusted refresh is used once; terminal outcomes never retry and no retry extends its budget.
        #[test]
        fn activity_sender_retry_budget(kind in 0u8..7, lost in 0usize..3) {
            tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap().block_on(async {
                let sender = ActivityOfferSender::default(); let acquisition = acquisition();
                let terminal = match kind { 0 => Ok(ActivityOfferOutcome::Admitted), 1 => Ok(ActivityOfferOutcome::Known), 2 => Ok(ActivityOfferOutcome::Full), 3 => Ok(ActivityOfferOutcome::Expired), 4 => Ok(ActivityOfferOutcome::NotDue), 5 => Err(ActivityOfferError::Unimplemented), _ => Ok(ActivityOfferOutcome::NotHome) };
                let script = Arc::new(Script { replies: Mutex::new(std::iter::repeat_n(Err(ActivityOfferError::Transport), lost).chain([terminal, terminal, terminal]).collect()), refreshes: Mutex::default() });
                let homes = Arc::new(PlacementQueueHomes { owner: Arc::new(RwLock::new(ShardOwner::new(8))), counts: RwLock::new((8, 8)) });
                let delivery = ActivityDelivery::new(crate::broker::InMemoryActivityBroker::with_discovery(homes.clone()), homes, crate::RuntimeShutdownHandle::new(), Arc::new(crate::test_clock::TestClock::new()));
                prop_assert_eq!(sender.try_enqueue(offer(1), acquisition, PublicationSource::Publication, ShardId(7)), EnqueueOutcome::Queued);
                let mut work = sender.state.lock().unwrap().take(Instant::now()).unwrap(); let deadline = work.deadline;
                loop {
                    let (next, retry) = attempt(work, delivery.clone(), script.clone()).await;
                    prop_assert_eq!(next.deadline, deadline);
                    if !retry { break; }
                    tokio::time::advance(next.ready_at - Instant::now()).await;
                    work = next;
                }
                let refreshes = script.refreshes.lock().unwrap();
                prop_assert!(refreshes.len() <= 3); prop_assert!(refreshes.iter().filter(|v| **v).count() <= 1);
                prop_assert_eq!(refreshes.len(), (lost + if kind == 6 { 2 } else { 1 }).min(3));
                Ok(())
            })?;
        }
    }
}
