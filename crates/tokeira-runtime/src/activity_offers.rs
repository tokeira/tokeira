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
        BYTE_CAPACITY, HOME_CAPACITY, OFFER_LEASE, OfferAdmission, QUEUE_CAPACITY, READY_RETENTION,
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
    bytes: usize,
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

#[derive(Debug)]
struct ReservationCounter {
    count: Arc<AtomicUsize>,
    cancel: CancellationToken,
}

#[derive(Debug, Default)]
pub(crate) struct ActivityOffers {
    retained: HashMap<ActivityIncarnation, RetainedOffer>,
    reserved: HashMap<(ShardId, u64), ReservationCounter>,
    next_serial: u64,
}

impl ActivityOffers {
    #[cfg(test)]
    pub(crate) fn accounting(&self, home: &QueueHome) -> (usize, usize, usize) {
        let entries = self
            .retained
            .values()
            .filter(|entry| entry.home.id == home.id && entry.home.generation == home.generation);
        let (mut ready, mut flight) = (0, 0);
        for entry in entries {
            if entry.in_flight {
                flight += 1;
            } else {
                ready += 1;
            }
        }
        (
            ready,
            flight,
            self.reserved
                .get(&(home.id, home.generation))
                .map_or(0, |n| n.count.load(Ordering::Relaxed)),
        )
    }

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
                    .map_or(0, |counter| counter.count.load(Ordering::Relaxed)),
        )
    }

    pub(crate) fn reserve(
        &mut self,
        home: &QueueHome,
        requested: usize,
    ) -> ActivityOfferReservation {
        let remaining = if !home.valid() {
            0
        } else {
            requested.min(self.available(home))
        };
        let (counter, cancel) = if remaining == 0 {
            (Arc::default(), home.cancel.child_token())
        } else {
            let counter = self
                .reserved
                .entry((home.id, home.generation))
                .or_insert_with(|| ReservationCounter {
                    count: Arc::default(),
                    cancel: home.cancel.child_token(),
                });
            (counter.count.clone(), counter.cancel.clone())
        };
        counter.fetch_add(remaining, Ordering::Relaxed);
        ActivityOfferReservation {
            home: home.clone(),
            cancel,
            remaining,
            counter,
        }
    }

    #[cfg(test)]
    pub(crate) fn admit(
        &mut self,
        key: ActivityIncarnation,
        queue: QueueKey,
        home: &QueueHome,
        now: Instant,
        reservation: Option<&mut ActivityOfferReservation>,
    ) -> (OfferAdmission, Option<ActivityOfferToken>) {
        self.admit_bounded(key, queue, home, now, reservation, 0, now + READY_RETENTION)
    }

    pub(crate) fn admit_bounded(
        &mut self,
        key: ActivityIncarnation,
        queue: QueueKey,
        home: &QueueHome,
        now: Instant,
        reservation: Option<&mut ActivityOfferReservation>,
        bytes: usize,
        hint_deadline: Instant,
    ) -> (OfferAdmission, Option<ActivityOfferToken>) {
        // Ownership must precede duplicate acknowledgement: a cancelled
        // acquisition cannot acknowledge a hint it no longer serves.
        if !home.valid() {
            return (OfferAdmission::NotHome, None);
        }
        if self.contains(&key) {
            return (OfferAdmission::Known, None);
        }
        let reserved = reservation.as_ref().is_some_and(|r| {
            r.home.id == home.id
                && r.home.generation == home.generation
                && r.home.valid()
                && !r.cancel.is_cancelled()
                && r.remaining > 0
        });
        let bound = if self
            .retained
            .values()
            .fold(0usize, |total, entry| total.saturating_add(entry.bytes))
            .saturating_add(bytes)
            > BYTE_CAPACITY
        {
            Some("hint_bytes")
        } else if self
            .retained
            .values()
            .filter(|entry| entry.queue == queue)
            .count()
            >= QUEUE_CAPACITY
        {
            Some("queue_count")
        } else if reservation.is_some() && !reserved {
            Some("reservation")
        } else if !reserved && self.available(home) == 0 {
            Some("home_count")
        } else {
            None
        };
        if let Some(bound) = bound {
            crate::metrics::record_activity_offer_saturation(bound);
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
                expires_at: hint_deadline.min(now + READY_RETENTION),
                bytes,
                in_flight: false,
                released: Arc::new(AtomicBool::new(false)),
                source_cancel,
            },
        );
        (OfferAdmission::Admitted, Some(token))
    }

    /// Rekeying changes only volatile coordinates, but must still fit admission's budgets.
    pub(crate) fn reroute(
        &mut self,
        key: &ActivityIncarnation,
        token: Option<ActivityOfferToken>,
        queue: &QueueKey,
    ) -> bool {
        let Some(entry) = self.retained.get(key) else {
            return token.is_none();
        };
        if Some(entry.token) != token || entry.in_flight {
            return false;
        }
        let queue_bytes = |q: &QueueKey| {
            q.task_queue
                .0
                .len()
                .saturating_add(q.deployment.as_ref().map_or(0, |v| v.0.len()))
                .saturating_add(q.build_id.as_ref().map_or(0, |v| v.0.len()))
                .saturating_mul(3)
        };
        let bytes = entry
            .bytes
            .saturating_sub(queue_bytes(&entry.queue))
            .saturating_add(queue_bytes(queue));
        let total = self
            .retained
            .values()
            .fold(0usize, |sum, other| sum.saturating_add(other.bytes));
        if total.saturating_sub(entry.bytes).saturating_add(bytes) > BYTE_CAPACITY
            || (entry.queue != *queue
                && self
                    .retained
                    .values()
                    .filter(|other| other.queue == *queue)
                    .count()
                    >= QUEUE_CAPACITY)
        {
            return false;
        }
        let entry = self
            .retained
            .get_mut(key)
            .expect("retained offer stays locked during rekey");
        entry.bytes = bytes;
        entry.queue = queue.clone();
        true
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
            || !entry.home.valid()
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

    #[cfg(test)]
    pub(crate) fn expire(&mut self, now: Instant) -> Vec<ActivityIncarnation> {
        self.expire_if_serving(now, |_, _| true)
    }

    /// The broker supplies its live provider check while holding the queue lock.
    pub(crate) fn expire_if_serving(
        &mut self,
        now: Instant,
        serving: impl Fn(&QueueHome, &QueueKey) -> bool,
    ) -> Vec<ActivityIncarnation> {
        let mut removed = Vec::new();
        self.retained.retain(|key, entry| {
            let keep = now < entry.expires_at
                && entry.home.valid()
                && serving(&entry.home, &entry.queue)
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
            .retain(|_, counter| counter.count.load(Ordering::Relaxed) != 0);
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
                && (!home.valid()
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

    /// Detach acquisition counters; late guards still own only the old cells.
    pub(crate) fn retire_home(&mut self, home: &QueueHome) -> Vec<ActivityIncarnation> {
        let mut removed = Vec::new();
        self.retained.retain(|key, entry| {
            let remove = entry.home.id == home.id && entry.home.generation == home.generation;
            if remove {
                removed.push(key.clone());
            }
            !remove
        });
        if let Some(counter) = self.reserved.remove(&(home.id, home.generation)) {
            counter.cancel.cancel();
        }
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
