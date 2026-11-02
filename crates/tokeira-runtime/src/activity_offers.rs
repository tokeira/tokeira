//! Bounded disposable activity offers, serialized by the broker's queue mutex.
//!
//! An incarnation stays counted after take until the caller releases its lease or
//! five seconds elapse. Per-offer release cells and per-acquisition reservation
//! counters ensure delayed drops cannot affect replacement offers or generations.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use tokeira_storage::{ActivityIncarnation, DispatchableActivityTask};
use tokeira_types::{QueueKey, RunKey, ShardId};
use tokio::{sync::Notify, time::Instant};
use tokio_util::sync::CancellationToken;

use crate::{
    discovery::QueueHome,
    workflow_offers::{
        HOME_CAPACITY, OFFER_LEASE, OfferAdmission, QUEUE_CAPACITY, READY_RETENTION,
    },
};

#[cfg(test)]
mod tests;

pub(crate) fn incarnation(task: &DispatchableActivityTask) -> ActivityIncarnation {
    ActivityIncarnation {
        run_key: task.run_key,
        activity_id: task.activity_id.clone(),
        schedule_event_id: task.schedule_event_id,
        attempt: task.attempt,
        stamp: task.stamp,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ActivityOfferToken {
    pub(crate) generation: u64,
    pub(crate) serial: u64,
}

#[derive(Debug)]
struct RetainedOffer {
    queue: QueueKey,
    home: QueueHome,
    token: ActivityOfferToken,
    expires_at: Instant,
    in_flight: bool,
    released: Arc<AtomicBool>,
    source_cancel: Option<CancellationToken>,
}

/// Cloneable ownership of one take. Only its last drop releases the offer;
/// cancellation needs no spawned task and never waits for the broker mutex.
#[derive(Clone, Debug)]
pub(crate) struct ActivityOfferLease {
    _release: Arc<LeaseRelease>,
}

#[derive(Debug)]
struct LeaseRelease {
    released: Arc<AtomicBool>,
    wake: Arc<Notify>,
}

impl Drop for LeaseRelease {
    fn drop(&mut self) {
        self.released.store(true, Ordering::Release);
        self.wake.notify_one();
    }
}

/// Capacity reserved before reading a page. The counter belongs to the captured
/// acquisition, so a late drop cannot subtract from the replacement's budget.
#[derive(Debug)]
pub(crate) struct ActivityOfferReservation {
    pub(crate) home: QueueHome,
    pub(crate) cancel: CancellationToken,
    remaining: usize,
    counter: Arc<AtomicUsize>,
}

impl ActivityOfferReservation {
    pub(crate) fn remaining(&self) -> usize {
        self.remaining
    }

    fn consume(&mut self) {
        assert!(
            self.remaining > 0,
            "activity admission requires reserved capacity"
        );
        self.remaining -= 1;
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Drop for ActivityOfferReservation {
    fn drop(&mut self) {
        self.counter.fetch_sub(self.remaining, Ordering::Relaxed);
    }
}

#[derive(Debug, Default)]
pub(crate) struct ActivityOffers {
    retained: HashMap<ActivityIncarnation, RetainedOffer>,
    reserved: HashMap<(ShardId, u64), Arc<AtomicUsize>>,
    next_serial: u64,
}

impl ActivityOffers {
    pub(crate) fn contains(&self, key: &ActivityIncarnation) -> bool {
        self.retained.contains_key(key)
    }

    fn available(&self, home: &QueueHome) -> usize {
        HOME_CAPACITY.saturating_sub(
            self.retained
                .values()
                .filter(|entry| entry.home.id == home.id)
                .count()
                + self
                    .reserved
                    .get(&(home.id, home.generation))
                    .map_or(0, |count| count.load(Ordering::Relaxed)),
        )
    }

    pub(crate) fn reserve(
        &mut self,
        home: &QueueHome,
        requested: usize,
    ) -> ActivityOfferReservation {
        let remaining = if home.cancel.is_cancelled() {
            0
        } else {
            requested.min(self.available(home))
        };
        let counter = self
            .reserved
            .entry((home.id, home.generation))
            .or_default()
            .clone();
        counter.fetch_add(remaining, Ordering::Relaxed);
        ActivityOfferReservation {
            home: home.clone(),
            cancel: home.cancel.clone(),
            remaining,
            counter,
        }
    }

    pub(crate) fn admit(
        &mut self,
        key: ActivityIncarnation,
        queue: QueueKey,
        home: &QueueHome,
        now: Instant,
        reservation: Option<&mut ActivityOfferReservation>,
    ) -> (OfferAdmission, Option<ActivityOfferToken>) {
        if self.contains(&key) {
            return (OfferAdmission::Known, None);
        }
        let reserved = reservation.as_ref().is_some_and(|r| {
            r.home.id == home.id
                && r.home.generation == home.generation
                && !r.home.cancel.is_cancelled()
                && !r.cancel.is_cancelled()
                && r.remaining > 0
        });
        if home.cancel.is_cancelled()
            || self
                .retained
                .values()
                .filter(|entry| entry.queue == queue)
                .count()
                >= QUEUE_CAPACITY
            || (reservation.is_some() && !reserved)
            || (!reserved && self.available(home) == 0)
        {
            return (OfferAdmission::Full, None);
        }
        self.next_serial = self
            .next_serial
            .checked_add(1)
            .expect("activity offer token space exhausted");
        let token = ActivityOfferToken {
            generation: home.generation,
            serial: self.next_serial,
        };
        let source_cancel = reservation.as_ref().map(|r| r.cancel.clone());
        if let Some(reservation) = reservation {
            reservation.consume();
        }
        self.retained.insert(
            key,
            RetainedOffer {
                queue,
                home: home.clone(),
                token,
                expires_at: now + READY_RETENTION,
                in_flight: false,
                released: Arc::new(AtomicBool::new(false)),
                source_cancel,
            },
        );
        (OfferAdmission::Admitted, Some(token))
    }

    pub(crate) fn take(
        &mut self,
        key: &ActivityIncarnation,
        token: ActivityOfferToken,
        now: Instant,
        wake: Arc<Notify>,
    ) -> Option<ActivityOfferLease> {
        let entry = self.retained.get_mut(key)?;
        if entry.token != token
            || entry.in_flight
            || entry.home.cancel.is_cancelled()
            || now >= entry.expires_at
            || entry.released.load(Ordering::Acquire)
        {
            return None;
        }
        entry.in_flight = true;
        entry.expires_at = now + OFFER_LEASE;
        Some(ActivityOfferLease {
            _release: Arc::new(LeaseRelease {
                released: entry.released.clone(),
                wake,
            }),
        })
    }

    pub(crate) fn expire(&mut self, now: Instant) -> Vec<ActivityIncarnation> {
        let mut removed = Vec::new();
        self.retained.retain(|key, entry| {
            let keep = now < entry.expires_at
                && !entry.home.cancel.is_cancelled()
                && (entry.in_flight
                    || !entry
                        .source_cancel
                        .as_ref()
                        .is_some_and(CancellationToken::is_cancelled))
                && !entry.released.load(Ordering::Acquire);
            if !keep {
                removed.push(key.clone());
            }
            keep
        });
        // A still-live reservation owns a separate Arc even after this map entry
        // is discarded. Keep active counters until they are actually returned.
        self.reserved
            .retain(|_, count| count.load(Ordering::Relaxed) != 0);
        removed.sort();
        removed
    }

    pub(crate) fn retire(
        &mut self,
        home: &QueueHome,
        queue: &QueueKey,
    ) -> Vec<ActivityIncarnation> {
        let mut removed = Vec::new();
        self.retained.retain(|key, entry| {
            let remove = entry.home.id == home.id
                && entry.home.generation == home.generation
                && (home.cancel.is_cancelled()
                    || (!entry.in_flight
                        && entry.queue.namespace_id == queue.namespace_id
                        && entry.queue.task_queue == queue.task_queue));
            if remove {
                removed.push(key.clone());
            }
            !remove
        });
        removed.sort();
        removed
    }

    pub(crate) fn remove_run(&mut self, run: RunKey) {
        self.retained.retain(|key, _| key.run_key != run);
    }

    #[cfg(test)]
    pub(crate) fn remove_ready(
        &mut self,
        key: &ActivityIncarnation,
        token: Option<ActivityOfferToken>,
    ) {
        if self
            .retained
            .get(key)
            .is_some_and(|entry| !entry.in_flight && Some(entry.token) == token)
        {
            self.retained.remove(key);
        }
    }
}
