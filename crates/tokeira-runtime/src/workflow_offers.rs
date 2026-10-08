//! Bounded, disposable normal-workflow offer accounting.
//!
//! A take retains the incarnation until its outcome or lease expiry. This module
//! never acknowledges durable intent: the run's start transaction does that.
//! The broker holds its mutex across accounting and queue mutations, so a
//! notification cannot race a discovered copy past deduplication or capacity.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use tokeira_types::{LogicalTaskSeq, QueueKey, RunKey, ShardId};
use tokio::time::{Duration, Instant};

use crate::discovery::QueueHome;

pub(crate) const OFFER_LEASE: Duration = Duration::from_secs(5);
pub(crate) const READY_RETENTION: Duration = Duration::from_secs(5);
pub(crate) const QUEUE_CAPACITY: usize = 256;
pub(crate) const HOME_CAPACITY: usize = 8_192;

pub(crate) type OfferKey = (RunKey, LogicalTaskSeq);

#[derive(Clone, Debug)]
struct RetainedOffer {
    queue: QueueKey,
    home: ShardId,
    generation: u64,
    entered_at: Instant,
    expires_at: Instant,
    in_flight: bool,
}

#[derive(Debug, Default)]
pub(crate) struct WorkflowOffers {
    retained: HashMap<OfferKey, RetainedOffer>,
    reserved: HashMap<ShardId, Arc<AtomicUsize>>,
}

/// A page owns its capacity even while awaiting storage. Dropping a cancelled
/// page returns every unused slot synchronously, without spawning cleanup work.
#[derive(Debug)]
pub(crate) struct OfferReservation {
    pub(crate) home: ShardId,
    pub(crate) generation: u64,
    remaining: usize,
    counter: Arc<AtomicUsize>,
}

impl OfferReservation {
    pub(crate) fn remaining(&self) -> usize {
        self.remaining
    }

    pub(crate) fn consume(&mut self) {
        assert!(self.remaining > 0, "admission requires a reserved slot");
        self.remaining -= 1;
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Drop for OfferReservation {
    fn drop(&mut self) {
        self.counter.fetch_sub(self.remaining, Ordering::Relaxed);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OfferAdmission {
    Admitted,
    Known,
    Full,
}

impl WorkflowOffers {
    pub(crate) fn contains(&self, key: OfferKey) -> bool {
        self.retained.contains_key(&key)
    }

    pub(crate) fn available(&self, home: ShardId) -> usize {
        HOME_CAPACITY.saturating_sub(
            self.retained
                .values()
                .filter(|entry| entry.home == home)
                .count()
                + self
                    .reserved
                    .get(&home)
                    .map_or(0, |count| count.load(Ordering::Relaxed)),
        )
    }

    pub(crate) fn reserve(&mut self, home: ShardId, requested: usize) -> OfferReservation {
        let granted = requested.min(self.available(home));
        let counter = self.reserved.entry(home).or_default().clone();
        counter.fetch_add(granted, Ordering::Relaxed);
        OfferReservation {
            home,
            generation: 0,
            remaining: granted,
            counter,
        }
    }

    pub(crate) fn admit(
        &mut self,
        key: OfferKey,
        queue: QueueKey,
        home: &QueueHome,
        now: Instant,
        reserved: bool,
    ) -> OfferAdmission {
        if self.contains(key) {
            return OfferAdmission::Known;
        }
        let queue_count = self
            .retained
            .values()
            .filter(|entry| entry.queue == queue)
            .count();
        if home.cancel.is_cancelled()
            || queue_count >= QUEUE_CAPACITY
            || (!reserved && self.available(home.id) == 0)
        {
            return OfferAdmission::Full;
        }
        self.retained.insert(
            key,
            RetainedOffer {
                queue,
                home: home.id,
                generation: home.generation,
                entered_at: now,
                expires_at: now + READY_RETENTION,
                in_flight: false,
            },
        );
        OfferAdmission::Admitted
    }

    pub(crate) fn take(&mut self, key: OfferKey, now: Instant) {
        if let Some(entry) = self.retained.get_mut(&key) {
            entry.in_flight = true;
            entry.expires_at = now + OFFER_LEASE;
        }
    }

    pub(crate) fn finish(&mut self, key: OfferKey, entered_at: Instant) -> bool {
        // A late reply must not release a newer offer admitted after lease expiry.
        if self
            .retained
            .get(&key)
            .is_some_and(|entry| entry.in_flight && entry.entered_at == entered_at)
        {
            self.retained.remove(&key);
            return true;
        }
        false
    }

    pub(crate) fn expire(&mut self, now: Instant) -> Vec<OfferKey> {
        let mut removed = Vec::new();
        self.retained.retain(|key, entry| {
            if now >= entry.expires_at {
                removed.push(*key);
                false
            } else {
                true
            }
        });
        removed.sort();
        removed
    }

    pub(crate) fn retire_home(&mut self, home: &QueueHome) -> Vec<OfferKey> {
        let mut removed = Vec::new();
        self.retained.retain(|key, entry| {
            if entry.home == home.id && entry.generation == home.generation {
                removed.push(*key);
                false
            } else {
                true
            }
        });
        removed.sort();
        removed
    }

    pub(crate) fn retire_ready_queue(
        &mut self,
        home: &QueueHome,
        queue: &QueueKey,
    ) -> Vec<OfferKey> {
        let mut removed = Vec::new();
        self.retained.retain(|key, entry| {
            if &entry.queue == queue
                && entry.home == home.id
                && entry.generation == home.generation
                && !entry.in_flight
            {
                removed.push(*key);
                false
            } else {
                true
            }
        });
        removed.sort();
        removed
    }

    pub(crate) fn remove_run(&mut self, run_key: RunKey) {
        self.retained.retain(|(run, _), _| *run != run_key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use tokeira_types::{NamespaceId, TaskKind, TaskQueueName};
    use tokio_util::sync::CancellationToken;

    fn queue(index: u16) -> QueueKey {
        QueueKey {
            namespace_id: NamespaceId(uuid::Uuid::nil()),
            task_queue: TaskQueueName(format!("q-{index}")),
            task_kind: TaskKind::Workflow,
            deployment: None,
            build_id: None,
        }
    }

    fn home(index: u32) -> QueueHome {
        QueueHome {
            id: ShardId(index),
            generation: 1,
            cancel: CancellationToken::new(),
        }
    }

    fn key(index: u16) -> OfferKey {
        (
            RunKey(uuid::Uuid::from_u128(u128::from(index) + 1)),
            LogicalTaskSeq(1),
        )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        // Feature: workflow-dispatch, Property 6: Volatile offer loss and ambiguity
        // Only outcomes or elapsed retention release an incarnation's capacity.
        #[test]
        fn offer_traces_match_independent_lease_model(actions in prop::collection::vec((0u8..7, 0u16..600, 0u64..7), 1..250)) {
            let origin = Instant::now();
            let mut tick = 0;
            let mut offers = WorkflowOffers::default();
            let mut model = std::collections::BTreeMap::<u16, (bool, u64, u64)>::new();
            for (action, id, advance) in actions {
                let now = origin + Duration::from_secs(tick);
                match action {
                    0 | 1 => {
                        let expected = if model.contains_key(&id) { OfferAdmission::Known }
                            else if model.keys().filter(|other| **other % 2 == id % 2).count() >= 256 { OfferAdmission::Full }
                            else { model.insert(id, (false, tick + 5, tick)); OfferAdmission::Admitted };
                        prop_assert_eq!(offers.admit(key(id), queue(id % 2), &home(u32::from(id % 2)), now, false), expected);
                    }
                    2 => {
                        if let Some((in_flight, deadline, _)) = model.get_mut(&id) && !*in_flight {
                            *in_flight = true; *deadline = tick + 5; offers.take(key(id), now);
                        }
                    }
                    3 => {
                        if let Some((true, _, entered)) = model.get(&id).copied() {
                            prop_assert!(offers.finish(key(id), origin + Duration::from_secs(entered)));
                            model.remove(&id);
                        }
                    }
                    4 => { /* Cancellation/ambiguous response leaves the bounded lease. */ }
                    5 => {
                        tick += advance;
                        offers.expire(origin + Duration::from_secs(tick));
                        model.retain(|_, (_, deadline, _)| *deadline > tick);
                    }
                    _ => {
                        offers.retire_home(&home(u32::from(id % 2)));
                        model.retain(|other, _| *other % 2 != id % 2);
                    }
                }
                prop_assert_eq!(offers.retained.len(), model.len());
                for (id, (in_flight, expires, entered)) in &model {
                    let actual = offers.retained.get(&key(*id)).unwrap();
                    prop_assert_eq!(actual.in_flight, *in_flight);
                    prop_assert_eq!(actual.expires_at, origin + Duration::from_secs(*expires));
                    prop_assert_eq!(actual.entered_at, origin + Duration::from_secs(*entered));
                }
            }
        }
    }

    #[test]
    fn capacity_and_reservations_bound_ready_plus_in_flight() {
        assert_eq!((QUEUE_CAPACITY, HOME_CAPACITY), (256, 8192));
        assert_eq!(
            (READY_RETENTION, OFFER_LEASE),
            (Duration::from_secs(5), Duration::from_secs(5))
        );
        let mut offers = WorkflowOffers::default();
        let now = Instant::now();
        for id in 0..8192u16 {
            assert_eq!(
                offers.admit(key(id), queue(id / 256), &home(0), now, false),
                OfferAdmission::Admitted
            );
            offers.take(key(id), now);
        }
        assert_eq!(
            offers.admit(key(9000), queue(90), &home(0), now, false),
            OfferAdmission::Full
        );
        assert_eq!(offers.reserve(ShardId(0), 64).remaining(), 0);
        offers.expire(now + OFFER_LEASE);
        let reservation = offers.reserve(ShardId(0), 64);
        assert_eq!(offers.available(ShardId(0)), 8192 - 64);
        drop(reservation);
        assert_eq!(offers.available(ShardId(0)), 8192);
    }

    #[test]
    fn late_start_reply_cannot_release_a_new_offer() {
        let mut offers = WorkflowOffers::default();
        let now = Instant::now();
        offers.admit(key(1), queue(0), &home(0), now, false);
        offers.take(key(1), now);
        offers.expire(now + OFFER_LEASE);
        offers.admit(key(1), queue(0), &home(0), now + OFFER_LEASE, false);
        offers.take(key(1), now + OFFER_LEASE);
        assert!(!offers.finish(key(1), now));
        assert!(offers.contains(key(1)));
    }
}
