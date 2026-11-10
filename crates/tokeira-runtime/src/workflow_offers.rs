//! Bounded, acquisition-scoped accounting for disposable workflow offers.
//!
//! The broker serializes this index and its ready queues. A revision survives a
//! take so expiry, retargeting and late results cannot release another offer.
//! Only the execution owner's fenced transition acknowledges durable intent.

use crate::discovery::QueueHome;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokeira_types::{LogicalTaskSeq, QueueKey, RunKey, ShardId};
use tokio::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

pub(crate) const OFFER_LEASE: Duration = Duration::from_secs(5);
pub(crate) const READY_RETENTION: Duration = Duration::from_secs(5);
pub(crate) const QUEUE_CAPACITY: usize = 256;
pub(crate) const HOME_CAPACITY: usize = 8_192;
/// Node-wide serialized-hint budget, shared by all sources and acquisition scopes.
pub(crate) const BYTE_CAPACITY: usize = 64 * 1024 * 1024;
pub(crate) type OfferKey = (RunKey, LogicalTaskSeq);
pub(crate) type OfferRevision = (OfferKey, Instant);
type Scope = (ShardId, u64, OfferKey);

#[derive(Clone, Debug)]
struct RetainedOffer {
    queue: QueueKey,
    home: QueueHome,
    revision: Instant,
    expires_at: Instant,
    in_flight: bool,
    bytes: usize,
    cancel: CancellationToken,
}

#[derive(Debug, Default)]
pub(crate) struct WorkflowOffers {
    retained: HashMap<Scope, RetainedOffer>,
    // Revisions are unique across scopes, so dequeue and late completion can
    // resolve their exact admission without scanning unrelated retained offers.
    by_revision: HashMap<OfferRevision, Scope>,
    // Legacy speculative suppression observes any retained scope for this key;
    // normal admission must still use the full acquisition scope above.
    key_counts: HashMap<OfferKey, usize>,
    reserved: HashMap<(ShardId, u64), Arc<AtomicUsize>>,
    bytes: usize,
    last_revision: Option<Instant>,
}

/// A page releases unused slots synchronously, including when storage is cancelled.
/// Retirement detaches its counter so a late drop cannot debit a new generation.
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
    pub(crate) fn release(&mut self) {
        self.counter.fetch_sub(self.remaining, Ordering::Relaxed);
        self.remaining = 0;
    }
}
impl Drop for OfferReservation {
    fn drop(&mut self) {
        self.release();
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OfferAdmission {
    Admitted,
    Known,
    Full,
    NotHome,
    Expired,
}
impl OfferAdmission {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Admitted => "admitted",
            Self::Known => "known",
            Self::Full => "full",
            Self::NotHome => "not_home",
            Self::Expired => "expired",
        }
    }
}

/// Origin is diagnostic only; it never confers ownership or routing authority.
#[derive(Clone, Copy, Debug)]
pub(crate) enum OfferSource {
    Local,
    Discovered,
    // The authenticated receiver will use this after routing validation is wired.
    #[allow(dead_code)]
    Pushed,
}
impl OfferSource {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Discovered => "discovered",
            Self::Pushed => "pushed",
        }
    }
}

/// Inputs whose checks are serialized with ready-queue mutation by the broker.
#[derive(Debug)]
pub(crate) struct OfferAdmissionInput<'a> {
    pub(crate) key: OfferKey,
    pub(crate) queue: QueueKey,
    pub(crate) home: &'a QueueHome,
    pub(crate) now: Instant,
    pub(crate) reserved: bool,
    pub(crate) bytes: usize,
    pub(crate) deadline: Option<Instant>,
    /// Set only after a current routing/availability decision, never by peer arrival order.
    pub(crate) retarget: bool,
}

impl WorkflowOffers {
    // Observation only. Admission uses the complete scope below.
    pub(crate) fn contains(&self, key: OfferKey) -> bool {
        self.key_counts.contains_key(&key)
    }
    pub(crate) fn available(&self, home: ShardId) -> usize {
        HOME_CAPACITY.saturating_sub(
            self.retained.keys().filter(|scope| scope.0 == home).count()
                + self
                    .reserved
                    .iter()
                    .filter(|(scope, _)| scope.0 == home)
                    .map(|(_, count)| count.load(Ordering::Relaxed))
                    .sum::<usize>(),
        )
    }
    pub(crate) fn reserve(&mut self, home: &QueueHome, requested: usize) -> OfferReservation {
        let granted = if home.valid() {
            requested.min(self.available(home.id))
        } else {
            0
        };
        let counter = self
            .reserved
            .entry((home.id, home.generation))
            .or_default()
            .clone();
        counter.fetch_add(granted, Ordering::Relaxed);
        OfferReservation {
            home: home.id,
            generation: home.generation,
            remaining: granted,
            counter,
        }
    }
    pub(crate) fn admit(
        &mut self,
        input: OfferAdmissionInput<'_>,
    ) -> (OfferAdmission, Instant, Vec<OfferRevision>) {
        let OfferAdmissionInput {
            key,
            queue,
            home,
            now,
            reserved,
            bytes,
            deadline,
            retarget,
        } = input;
        let scope = (home.id, home.generation, key);
        if !home.valid() {
            return (OfferAdmission::NotHome, now, vec![]);
        }
        if deadline.is_some_and(|at| at <= now) {
            return (OfferAdmission::Expired, now, vec![]);
        }
        let previous = self.retained.get(&scope);
        if let Some(previous) = previous {
            if previous.queue == queue {
                return (OfferAdmission::Known, previous.revision, vec![]);
            }
            if !retarget {
                return (OfferAdmission::Known, previous.revision, vec![]);
            }
        }
        let old_bytes = previous.map_or(0, |entry| entry.bytes);
        let count_full = self
            .retained
            .values()
            .filter(|entry| entry.queue == queue)
            .count()
            >= QUEUE_CAPACITY
            || (previous.is_none() && !reserved && self.available(home.id) == 0);
        let bytes_full = (self.bytes - old_bytes).saturating_add(bytes) > BYTE_CAPACITY;
        if count_full || bytes_full {
            for (full, bound) in [(count_full, "count"), (bytes_full, "bytes")] {
                if full {
                    metrics::counter!(crate::metrics::WORKFLOW_OFFER_SATURATION_TOTAL, "bound" => bound).increment(1);
                }
            }
            return (OfferAdmission::Full, now, vec![]);
        }
        let mut expires_at =
            deadline.map_or(now + READY_RETENTION, |at| at.min(now + READY_RETENTION));
        let mut removed = Vec::new();
        if let Some(previous) = self.remove(&scope) {
            expires_at = expires_at.min(previous.expires_at);
            removed.push((key, previous.revision));
        }
        // Existing broker APIs carry an Instant on completion. Make that revision
        // unique even with a paused clock or same-tick replacement; it is not a TTL.
        let revision = self
            .last_revision
            .map_or(now, |last| now.max(last + Duration::from_nanos(1)));
        self.last_revision = Some(revision);
        self.bytes += bytes;
        self.by_revision.insert((key, revision), scope);
        *self.key_counts.entry(key).or_default() += 1;
        self.retained.insert(
            scope,
            RetainedOffer {
                queue,
                home: home.clone(),
                revision,
                expires_at,
                in_flight: false,
                bytes,
                cancel: home.cancel.child_token(),
            },
        );
        (OfferAdmission::Admitted, revision, removed)
    }
    pub(crate) fn take(
        &mut self,
        key: OfferKey,
        revision: Instant,
        now: Instant,
    ) -> Option<CancellationToken> {
        let scope = self.by_revision.get(&(key, revision))?;
        let entry = self.retained.get_mut(scope).expect("revision is retained");
        if !entry.home.valid() || now >= entry.expires_at {
            return None;
        }
        entry.in_flight = true;
        // Taking and duplicate admission cannot extend retention or the absolute sticky deadline.
        entry.expires_at = entry.expires_at.min(now + OFFER_LEASE);
        Some(entry.cancel.clone())
    }
    pub(crate) fn finish(&mut self, key: OfferKey, revision: Instant) -> bool {
        let Some(scope) = self.by_revision.get(&(key, revision)).copied() else {
            return false;
        };
        if !self.retained[&scope].in_flight {
            return false;
        }
        self.remove(&scope);
        true
    }
    fn remove(&mut self, scope: &Scope) -> Option<RetainedOffer> {
        let entry = self.retained.remove(scope)?;
        let indexed = self.by_revision.remove(&(scope.2, entry.revision));
        debug_assert_eq!(indexed, Some(*scope));
        let count = self
            .key_counts
            .get_mut(&scope.2)
            .expect("retained key is indexed");
        *count -= 1;
        if *count == 0 {
            self.key_counts.remove(&scope.2);
        }
        self.bytes -= entry.bytes;
        entry.cancel.cancel();
        Some(entry)
    }
    fn remove_where(
        &mut self,
        predicate: impl Fn(&Scope, &RetainedOffer) -> bool,
    ) -> Vec<OfferRevision> {
        let scopes: Vec<_> = self
            .retained
            .iter()
            .filter_map(|(scope, entry)| predicate(scope, entry).then_some(*scope))
            .collect();
        let mut removed: Vec<_> = scopes
            .into_iter()
            .map(|scope| {
                let entry = self.remove(&scope).expect("selected offer is retained");
                (scope.2, entry.revision)
            })
            .collect();
        removed.sort();
        removed
    }
    pub(crate) fn expire(&mut self, now: Instant) -> Vec<OfferRevision> {
        // Acquisition watchers retire lost generations independently of expiry;
        // take checks the captured acquisition before a watcher has run.
        self.remove_where(|_, entry| now >= entry.expires_at)
    }
    pub(crate) fn retire_home(&mut self, home: &QueueHome) -> Vec<OfferRevision> {
        self.reserved.remove(&(home.id, home.generation));
        self.remove_where(|scope, _| scope.0 == home.id && scope.1 == home.generation)
    }
    pub(crate) fn retire_ready_queue(
        &mut self,
        home: &QueueHome,
        queue: &QueueKey,
    ) -> Vec<OfferRevision> {
        self.remove_where(|scope, entry| {
            scope.0 == home.id
                && scope.1 == home.generation
                && &entry.queue == queue
                && !entry.in_flight
        })
    }
    pub(crate) fn remove_run(&mut self, run_key: RunKey) {
        self.remove_where(|scope, _| scope.2.0 == run_key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use tokeira_types::{NamespaceId, TaskKind, TaskQueueName};

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
            deadline: Default::default(),
        }
    }
    fn key(index: u16) -> OfferKey {
        (
            RunKey(uuid::Uuid::from_u128(u128::from(index) + 1)),
            LogicalTaskSeq(1),
        )
    }
    fn admit(
        offers: &mut WorkflowOffers,
        id: u16,
        home: &QueueHome,
        now: Instant,
        bytes: usize,
        deadline: Option<Instant>,
    ) -> (OfferAdmission, Instant, Vec<OfferRevision>) {
        offers.admit(OfferAdmissionInput {
            key: key(id),
            queue: queue(id / 256),
            home,
            now,
            reserved: false,
            bytes,
            deadline,
            retarget: true,
        })
    }

    #[test]
    fn contains_survives_scope_removal_and_retargeting() {
        for final_removal in 0..3 {
            let mut offers = WorkflowOffers::default();
            let now = Instant::now();
            let first = home(0);
            let replacement = QueueHome {
                generation: 2,
                ..home(0)
            };
            let other = home(1);
            let old = admit(&mut offers, 7, &first, now, 1, None).1;
            let newer = admit(&mut offers, 7, &replacement, now, 1, None).1;
            let remote = admit(&mut offers, 7, &other, now, 1, None).1;
            assert_eq!(
                admit(&mut offers, 7, &first, now, 1, None).0,
                OfferAdmission::Known
            );
            let retargeted = offers.admit(OfferAdmissionInput {
                key: key(7),
                queue: queue(99),
                home: &first,
                now,
                reserved: false,
                bytes: 1,
                deadline: None,
                retarget: true,
            });
            assert_eq!(retargeted.0, OfferAdmission::Admitted);
            assert_eq!(
                offers.retire_ready_queue(&replacement, &queue(0)),
                vec![(key(7), newer)]
            );
            assert!(offers.contains(key(7)));
            assert!(offers.take(key(7), remote, now).is_some());
            assert!(offers.finish(key(7), remote));
            assert!(!offers.finish(key(7), old));
            assert!(offers.contains(key(7)));
            match final_removal {
                0 => {
                    offers.expire(now + READY_RETENTION);
                }
                1 => {
                    offers.retire_home(&first);
                }
                _ => offers.remove_run(key(7).0),
            }
            assert!(!offers.contains(key(7)));
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        // Feature: queue-home-delivery, Property 2: ready/in-flight expiry never extends on Known or late results.
        #[test]
        fn property_2_expiry_and_delayed_results(sticky_ms in 1u64..8000, take_ms in 0u64..5000, copies in 1..12usize) {
            let mut offers = WorkflowOffers::default();
            let now = Instant::now();
            let home = home(0);
            let deadline = now + Duration::from_millis(sticky_ms);
            let (result, old, _) = admit(&mut offers, 1, &home, now, 100, Some(deadline));
            prop_assert_eq!(result, OfferAdmission::Admitted);
            let expiry = deadline.min(now + READY_RETENTION);
            let taken_at = (now + Duration::from_millis(take_ms)).min(expiry - Duration::from_nanos(1));
            let token = offers.take(key(1), old, taken_at).unwrap();
            for _ in 0..copies {
                prop_assert_eq!(admit(&mut offers, 1, &home, taken_at, 100, None).0, OfferAdmission::Known);
            }
            prop_assert!(offers.expire(expiry - Duration::from_nanos(1)).is_empty());
            prop_assert_eq!(offers.expire(expiry), vec![(key(1), old)]);
            prop_assert!(token.is_cancelled());
            let (result, new, _) = admit(&mut offers, 1, &home, expiry, 100, None);
            prop_assert_eq!(result, OfferAdmission::Admitted);
            offers.take(key(1), new, expiry);
            prop_assert!(!offers.finish(key(1), old));
            prop_assert!(offers.contains(key(1)));
            prop_assert!(offers.finish(key(1), new));
            prop_assert_eq!(offers.bytes, 0);
        }

        // Feature: queue-home-delivery, Property 2: aggregate bytes and queue/home slots include in-flight work.
        #[test]
        fn property_2_count_and_byte_exhaustion(bytes_first in 1usize..BYTE_CAPACITY, inflight in any::<bool>(), reserved in 1usize..65) {
            let now = Instant::now();
            let home = home(0);
            let mut offers = WorkflowOffers::default();
            let first = admit(&mut offers, 0, &home, now, bytes_first, None);
            prop_assert_eq!(first.0, OfferAdmission::Admitted);
            if inflight { offers.take(key(0), first.1, now); }
            prop_assert_eq!(admit(&mut offers, 1, &home, now, BYTE_CAPACITY - bytes_first, None).0, OfferAdmission::Admitted);
            prop_assert_eq!(admit(&mut offers, 2, &home, now, 1, None).0, OfferAdmission::Full);
            offers.retire_home(&home);
            prop_assert_eq!(offers.bytes, 0);
            let reservation = offers.reserve(&home, HOME_CAPACITY - QUEUE_CAPACITY);
            for id in 0..QUEUE_CAPACITY {
                prop_assert_eq!(admit(&mut offers, id as u16, &home, now, 1, None).0, OfferAdmission::Admitted);
            }
            prop_assert_eq!(admit(&mut offers, 9000, &home, now, 1, None).0, OfferAdmission::Full);
            drop(reservation);
            prop_assert_eq!(offers.available(home.id), HOME_CAPACITY - QUEUE_CAPACITY);
            let reservation = offers.reserve(&home, reserved);
            prop_assert_eq!(offers.available(home.id), HOME_CAPACITY - QUEUE_CAPACITY - reserved);
            drop(reservation);
            prop_assert_eq!(admit(&mut offers, 255, &home, now, 1, None).0, OfferAdmission::Known);
            prop_assert_eq!(offers.admit(OfferAdmissionInput { key: key(10000), queue: queue(0), home: &home, now, reserved: false, bytes: 1, deadline: None, retarget: true }).0, OfferAdmission::Full);
        }
    }
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        // Feature: queue-home-delivery, Property 2: independent finite traces account for every ready/in-flight lease.
        #[test]
        fn property_2_retention_traces_match_model(actions in prop::collection::vec((0u8..7, 0u16..600, 0u64..7), 1..250)) {
            let origin = Instant::now();
            let home = home(0);
            let mut tick = 0;
            let mut offers = WorkflowOffers::default();
            let mut model = std::collections::BTreeMap::<u16, (bool, u64, Instant)>::new();
            let mut late = Vec::new();
            for (action, id, advance) in actions {
                let now = origin + Duration::from_secs(tick);
                match action {
                    0 | 1 => {
                        let known = model.contains_key(&id);
                        let (result, revision, _) = admit(&mut offers, id, &home, now, 1, None);
                        prop_assert_eq!(result, if known { OfferAdmission::Known } else { OfferAdmission::Admitted });
                        if !known { model.insert(id, (false, tick + 5, revision)); }
                    }
                    2 => {
                        if let Some((in_flight, _, revision)) = model.get_mut(&id) {
                            *in_flight = true;
                            prop_assert!(offers.take(key(id), *revision, now).is_some());
                        }
                    }
                    3 => {
                        if let Some((true, _, revision)) = model.get(&id).copied() {
                            prop_assert!(offers.finish(key(id), revision));
                            late.push((id, revision));
                            model.remove(&id);
                        }
                    }
                    4 => {
                        for (id, revision) in &late { prop_assert!(!offers.finish(key(*id), *revision)); }
                    }
                    5 => {
                        tick += advance;
                        offers.expire(origin + Duration::from_secs(tick));
                        model.retain(|id, (_, expires, revision)| {
                            if *expires <= tick { late.push((*id, *revision)); false } else { true }
                        });
                    }
                    _ => {
                        offers.retire_home(&home);
                        late.extend(model.iter().map(|(id, (_, _, revision))| (*id, *revision)));
                        model.clear();
                    }
                }
                prop_assert_eq!(offers.retained.len(), model.len());
                prop_assert_eq!(offers.by_revision.len(), model.len());
                prop_assert_eq!(offers.key_counts.len(), model.len());
                prop_assert_eq!(offers.bytes, model.len());
                for (id, (in_flight, expires, revision)) in &model {
                    let actual = &offers.retained[&(home.id, home.generation, key(*id))];
                    prop_assert_eq!(actual.in_flight, *in_flight);
                    prop_assert_eq!(actual.expires_at, origin + Duration::from_secs(*expires));
                    prop_assert_eq!(actual.revision, *revision);
                    prop_assert_eq!(offers.by_revision[&(key(*id), *revision)], (home.id, home.generation, key(*id)));
                    prop_assert_eq!(offers.key_counts[&key(*id)], 1);
                }
            }
        }
    }
}
