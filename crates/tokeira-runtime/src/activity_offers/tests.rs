//! Independent counted-set and overlapping-generation offer oracles.

use super::*;
use proptest::prelude::*;
use std::collections::{BTreeMap, BTreeSet};
use tokeira_types::{NamespaceId, TaskKind, TaskQueueName};
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;

fn home(generation: u64) -> QueueHome {
    QueueHome {
        id: ShardId(7),
        generation,
        cancel: CancellationToken::new(),
        deadline: Default::default(),
    }
}

fn queue(index: u16) -> QueueKey {
    QueueKey {
        namespace_id: NamespaceId(uuid::Uuid::nil()),
        task_queue: TaskQueueName(format!("offers-{index}")),
        task_kind: TaskKind::Activity,
        deployment: None,
        build_id: None,
    }
}

fn key(id: u16, schedule: i64) -> ActivityIncarnation {
    ActivityIncarnation {
        run_key: RunKey(uuid::Uuid::from_u128(1)),
        activity_id: format!("a-{id}"),
        schedule_event_id: schedule,
        attempt: 1,
        stamp: 1,
    }
}

#[derive(Debug)]
struct Expected {
    deadline: u64,
    flight: bool,
    serial: u64,
    queue: u16,
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: activity-dispatch, Property 5
    // Every incarnation is counted once through take, loss and replacement.
    #[test]
    fn activity_offer_traces_match_counted_set(actions in prop::collection::vec((0u8..8, 0u16..400, 1i64..4, 0u64..3), 100..350)) {
        let origin = Instant::now();
        let mut tick = 0;
        let mut assignment = home(1);
        let mut offers = ActivityOffers::default();
        let mut expected = BTreeMap::<ActivityIncarnation, Expected>::new();
        let mut tokens = HashMap::new();
        let mut leases = Vec::new();
        let mut serial = 0;
        for (action, id, schedule, elapsed) in actions {
            tick += elapsed;
            let now = origin + Duration::from_secs(tick);
            offers.expire(now);
            expected.retain(|_, entry| tick < entry.deadline);
            let key = key(id, schedule);
            let q = id % 3;
            match action {
                0 | 1 => {
                    let wanted = if expected.contains_key(&key) { OfferAdmission::Known }
                        else if expected.values().filter(|entry| entry.queue == q).count() == 256 { OfferAdmission::Full }
                        else { OfferAdmission::Admitted };
                    let (actual, token) = offers.admit(key.clone(), queue(q), &assignment, now, None);
                    assert_eq!(actual, wanted);
                    if actual == OfferAdmission::Admitted {
                        serial += 1;
                        expected.insert(key.clone(), Expected { deadline: tick + 5, flight: false, serial, queue: q });
                        tokens.insert(key, token.unwrap());
                    }
                }
                2 => {
                    if let Some(entry) = expected.get_mut(&key) && !entry.flight {
                        let lease = offers.take(&key, tokens[&key], now, Arc::new(Notify::new())).unwrap();
                        entry.flight = true;
                        entry.deadline = tick + 5;
                        leases.push((key, entry.serial, lease));
                    }
                }
                3 => {
                    if !leases.is_empty() {
                        let (key, serial, lease) = leases.swap_remove(usize::from(id) % leases.len());
                        drop(lease);
                        if expected.get(&key).is_some_and(|entry| entry.serial == serial) { expected.remove(&key); }
                    }
                }
                4 => {}
                5 => { offers.remove_run(key.run_key); expected.clear(); }
                6 => { offers = ActivityOffers::default(); expected.clear(); }
                _ => {
                    assignment.cancel.cancel();
                    assignment = home(assignment.generation + 1);
                    expected.clear();
                }
            }
            offers.expire(now);
            assert_eq!(offers.retained.keys().cloned().collect::<BTreeSet<_>>(), expected.keys().cloned().collect());
            assert_eq!(offers.available(&assignment), 8192 - expected.len());
        }
    }

    // Feature: activity-dispatch, Property 5
    // Ready and in-flight entries share capacity; a reused ID has a distinct schedule identity.
    #[test]
    fn activity_queue_capacity_includes_flight(count in 257u16..290, take in 0u16..256, schedule in 1i64..10000) {
        let now = Instant::now();
        let assignment = home(1);
        let mut offers = ActivityOffers::default();
        let mut leases = Vec::new();
        for id in 0..count {
            let identity = key(id, schedule);
            let (outcome, token) = offers.admit(identity.clone(), queue(0), &assignment, now, None);
            assert_eq!(outcome, if id < 256 { OfferAdmission::Admitted } else { OfferAdmission::Full });
            if id < take { leases.push(offers.take(&identity, token.unwrap(), now, Arc::new(Notify::new())).unwrap()); }
        }
        assert_eq!(offers.retained.len(), 256);
        drop(leases);
        offers.expire(now);
        assert_eq!(offers.retained.len(), 256 - usize::from(take));
        assert_eq!(offers.admit(key(0, schedule + 1), queue(0), &assignment, now, None).0,
            if take > 0 { OfferAdmission::Admitted } else { OfferAdmission::Full });
    }

    // Feature: activity-dispatch, Property 6
    // Old reservations, leases and late admissions cannot alter a new acquisition.
    #[test]
    fn activity_late_generation_drops_are_isolated(old_count in 1usize..8192, new_count in 1usize..8192) {
        let now = Instant::now();
        let old = home(1);
        let current = home(2);
        let mut offers = ActivityOffers::default();
        let mut old_reservation = offers.reserve(&old, old_count);
        let (admitted, token) = offers.admit(key(0, 1), queue(0), &old, now, Some(&mut old_reservation));
        assert_eq!(admitted, OfferAdmission::Admitted);
        let lease = offers.take(&key(0, 1), token.unwrap(), now, Arc::new(Notify::new())).unwrap();
        old.cancel.cancel();
        offers.expire(now);
        let mut new_reservation = offers.reserve(&current, new_count);
        assert_eq!(new_reservation.remaining(), new_count);
        assert_eq!(offers.admit(key(0, 1), queue(0), &current, now, Some(&mut new_reservation)).0, OfferAdmission::Admitted);
        let available = offers.available(&current);
        assert_eq!(offers.admit(key(1, 1), queue(0), &current, now, Some(&mut old_reservation)).0, OfferAdmission::Full);
        drop(old_reservation);
        drop(lease);
        offers.expire(now);
        assert!(offers.contains(&key(0, 1)));
        assert_eq!(offers.available(&current), available);
        drop(new_reservation);
        assert_eq!(offers.available(&current), 8191);
        let reservation = offers.reserve(&current, 8192);
        assert_eq!(reservation.remaining(), 8191);
        assert_eq!(offers.admit(key(2, 1), queue(1), &current, now, None).0, OfferAdmission::Full);
    }
}
