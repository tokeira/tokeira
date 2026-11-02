//! Generated traversal fixtures use their own sorted row and admission oracle.

use super::*;
use crate::discovery::tests::{Homes, home};
use proptest::prelude::*;
use std::{
    collections::BTreeSet,
    sync::atomic::{AtomicBool, Ordering},
};
use tokeira_storage::{ActivityDispatchCandidate, ActivityIncarnation};
use tokeira_types::{BuildId, NamespaceId, RunKey, TaskKind, TaskQueueName, WorkerIdentity};
use tokio::time::Duration;

fn queue() -> QueueKey {
    QueueKey {
        namespace_id: NamespaceId(uuid::Uuid::nil()),
        task_queue: TaskQueueName("activity-discovery".into()),
        task_kind: TaskKind::Activity,
        deployment: None,
        build_id: None,
    }
}

fn row(index: usize, tied: bool) -> ActivityDispatchRow {
    let queue = queue();
    ActivityDispatchRow {
        key: uuid::Uuid::from_u128(index as u128 + 1),
        incarnation: ActivityIncarnation {
            run_key: RunKey(uuid::Uuid::from_u128(index as u128 + 1)),
            activity_id: format!("a-{index}"),
            schedule_event_id: 1,
            attempt: 1,
            stamp: 1,
        },
        execution_home: ShardId(99),
        queue_key: ActivityDiscoveryRange {
            namespace_id: queue.namespace_id,
            queue_name: queue.task_queue.clone(),
        }
        .lookup_key(),
        queue,
        priority: None,
        dispatch_revision: 0,
        dispatch_at: OffsetDateTime::UNIX_EPOCH
            + time::Duration::seconds(if tied { 0 } else { index as i64 }),
    }
}

#[derive(Debug)]
struct Source {
    rows: Vec<ActivityDispatchRow>,
    prefix: usize,
    fail_page: AtomicBool,
    reads: Mutex<Vec<(OffsetDateTime, Option<ActivityDispatchPosition>)>>,
}

#[async_trait]
impl ActivityDiscoverySource for Source {
    async fn page(
        &self,
        _: &ActivityDiscoveryRange,
        cutoff: OffsetDateTime,
        after: Option<ActivityDispatchPosition>,
        limit: NonZeroU32,
    ) -> Result<ActivityDispatchPage> {
        self.reads.lock().unwrap().push((cutoff, after));
        if self.fail_page.swap(false, Ordering::Relaxed) {
            anyhow::bail!("injected page failure");
        }
        let candidates: Vec<_> = self
            .rows
            .iter()
            .enumerate()
            .filter(|(_, row)| {
                row.dispatch_at <= cutoff && after.is_none_or(|after| row.position() > after)
            })
            .take(limit.get() as usize)
            .map(|(i, row)| ActivityDispatchCandidate {
                position: row.position(),
                row: if i < self.prefix && i % 5 == 3 {
                    Err(anyhow::anyhow!("bad payload"))
                } else {
                    Ok(row.clone())
                },
            })
            .collect();
        Ok(ActivityDispatchPage {
            last_examined: candidates.last().map(|c| c.position),
            exhausted: candidates.len() < limit.get() as usize,
            candidates,
        })
    }

    async fn resolve(&self, row: &ActivityDispatchRow) -> Result<Option<DispatchableActivityTask>> {
        let i = row.key.as_u128() as usize - 1;
        if i < self.prefix {
            match i % 5 {
                1 => return Ok(None),
                2 => {
                    return Ok(Some(DispatchableActivityTask {
                        queue: QueueKey {
                            build_id: Some(BuildId("other".into())),
                            ..row.queue.clone()
                        },
                        ..row.task()
                    }));
                }
                4 => anyhow::bail!("injected hydration failure"),
                _ => {}
            }
        }
        Ok(Some(row.task()))
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: activity-dispatch, Property 4
    // Every healthy tail is reached despite a multi-page held, bad, stale or incompatible prefix and poll renewal.
    #[test]
    fn activity_discovery_advances_skipped_prefix(prefix in 65usize..220, tail in 1usize..60, renew in any::<bool>(), tied in any::<bool>()) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let now = Instant::now();
            let cutoff = OffsetDateTime::UNIX_EPOCH + time::Duration::hours(1);
            let source = Source { rows: (0..prefix + tail).map(|i| row(i, tied)).collect(), prefix, fail_page: AtomicBool::new(false), reads: Mutex::new(Vec::new()) };
            let assignment = home(0);
            let broker = InMemoryActivityBroker::with_discovery(Arc::new(Homes(Mutex::new(assignment.clone()))));
            let registry = ActivityDiscoveryRegistry::default();
            let mut demand = Some(registry.register_poll(queue(), assignment.clone(), now).unwrap());
            for i in (0..prefix).step_by(5) { broker.publish_activity_task(source.rows[i].task(), None).await.unwrap(); }
            for _ in 0..10 {
                let (key, home, mut pass) = registry.select(now, cutoff).unwrap();
                let outcome = run_slice(&source, &registry, &broker, &key, &home, &mut pass).await.unwrap();
                registry.complete(&key, outcome, pass, now);
                if outcome == SliceOutcome::Complete { break; }
                assert_eq!(outcome, SliceOutcome::Yield);
                if renew { drop(demand.take()); demand = Some(registry.register_poll(queue(), assignment.clone(), now).unwrap()); }
            }
            for expected in &source.rows[prefix..] { assert!(broker.contains_activity_task(&expected.task()).await); }
            let reads = source.reads.lock().unwrap();
            assert!(reads.len() >= 2 && reads.len() <= 5);
            assert!(reads.iter().all(|(observed, _)| *observed == cutoff));
            assert!(reads.windows(2).all(|pair| pair[1].1 > pair[0].1));
        });
    }

    // Feature: activity-dispatch, Property 4
    // Capacity refusal preserves the first unadmitted key, and releasing a take resumes that same pass.
    #[test]
    fn activity_discovery_capacity_preserves_cursor(tail in 2usize..20, tied in any::<bool>()) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let now = Instant::now();
            let cutoff = OffsetDateTime::UNIX_EPOCH + time::Duration::hours(1);
            let source = Source { rows: (0..256 + tail).map(|i| row(i, tied)).collect(), prefix: 0, fail_page: AtomicBool::new(false), reads: Mutex::new(Vec::new()) };
            let assignment = home(0);
            let broker = InMemoryActivityBroker::with_discovery(Arc::new(Homes(Mutex::new(assignment.clone()))));
            let registry = ActivityDiscoveryRegistry::default();
            let _demand = registry.register_poll(queue(), assignment, now).unwrap();
            for _ in 0..4 {
                let (key, home, mut pass) = registry.select(now, cutoff).unwrap();
                assert_eq!(run_slice(&source, &registry, &broker, &key, &home, &mut pass).await.unwrap(), SliceOutcome::Yield);
                registry.complete(&key, SliceOutcome::Yield, pass, now);
            }
            let (key, home, mut pass) = registry.select(now, cutoff).unwrap();
            let after = pass.after;
            assert_eq!(run_slice(&source, &registry, &broker, &key, &home, &mut pass).await.unwrap(), SliceOutcome::Capacity);
            assert_eq!(pass.after, after);
            registry.complete(&key, SliceOutcome::Capacity, pass, now);
            assert!(registry.select(now, cutoff).is_none());
            let offer = broker.poll_activity_offer_for_worker(&queue(), &WorkerIdentity("w".into()), Duration::ZERO).await.unwrap().unwrap();
            assert!(broker.contains_activity_task(&offer.task).await);
            drop(offer);
            registry.capacity_returned(now);
            let (key, home, mut pass) = registry.select(now, cutoff + time::Duration::hours(1)).unwrap();
            assert_eq!(pass.cutoff, cutoff);
            assert_eq!(pass.after, after);
            assert_eq!(run_slice(&source, &registry, &broker, &key, &home, &mut pass).await.unwrap(), SliceOutcome::Capacity);
            assert_eq!(pass.after, Some(source.rows[256].position()));
            assert!(broker.contains_activity_task(&source.rows[256].task()).await);
        });
    }

    // Feature: activity-dispatch, Property 4
    // A failed page restarts at the head; future work enters only a later fixed-cutoff pass.
    #[test]
    fn activity_page_failure_and_future_cutoff_preserve_progress(prefix in 65usize..120, tail in 1usize..30) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let now = Instant::now();
            let cutoff = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(prefix as i64 - 1);
            let source = Source { rows: (0..prefix + tail).map(|i| row(i, false)).collect(), prefix, fail_page: AtomicBool::new(false), reads: Mutex::new(Vec::new()) };
            let assignment = home(0);
            let broker = InMemoryActivityBroker::with_discovery(Arc::new(Homes(Mutex::new(assignment.clone()))));
            let registry = ActivityDiscoveryRegistry::default();
            let _demand = registry.register_poll(queue(), assignment, now).unwrap();
            let (key, home, mut pass) = registry.select(now, cutoff).unwrap();
            let outcome = run_slice(&source, &registry, &broker, &key, &home, &mut pass).await.unwrap();
            assert_eq!(outcome, SliceOutcome::Yield);
            registry.complete(&key, outcome, pass, now);
            source.fail_page.store(true, Ordering::Relaxed);
            let (key, home, mut pass) = registry.select(now, cutoff + time::Duration::days(1)).unwrap();
            assert_eq!(pass.cutoff, cutoff);
            assert!(run_slice(&source, &registry, &broker, &key, &home, &mut pass).await.is_err());
            registry.complete(&key, SliceOutcome::Complete, pass, now);
            assert!(registry.select(now, cutoff).is_none());
            for expected in &source.rows[prefix..] { assert!(!broker.contains_activity_task(&expected.task()).await); }
            let later = now + DISCOVERY_PERIOD;
            let new_cutoff = cutoff + time::Duration::days(1);
            for page in 0..4 {
                let (key, home, mut pass) = registry.select(later, new_cutoff).unwrap();
                if page == 0 { assert!(pass.after.is_none()); }
                assert_eq!(pass.cutoff, new_cutoff);
                let outcome = run_slice(&source, &registry, &broker, &key, &home, &mut pass).await.unwrap();
                registry.complete(&key, outcome, pass, later);
                if outcome == SliceOutcome::Complete { break; }
            }
            for expected in &source.rows[prefix..] { assert!(broker.contains_activity_task(&expected.task()).await); }
        });
    }

    // Feature: activity-dispatch, Property 6
    // Retired pages and demand drops cannot change a replacement registration.
    #[test]
    fn activity_discovery_late_registration_is_isolated(generation in 1u64..10000, renewals in 1usize..20) {
        let now = Instant::now();
        let registry = ActivityDiscoveryRegistry::default();
        let mut assignment = home(0);
        assignment.generation = generation;
        let mut old_demands = Vec::new();
        for _ in 0..renewals { old_demands.push(registry.register_poll(queue(), assignment.clone(), now).unwrap()); }
        let (key, _, pass) = registry.select(now, OffsetDateTime::UNIX_EPOCH).unwrap();
        assignment.cancel.cancel();
        assert_eq!(registry.retire(now).len(), 1);
        let mut replacement = home(0);
        replacement.generation = generation + 1;
        let _current = registry.register_poll(queue(), replacement, now).unwrap();
        drop(old_demands);
        registry.complete(&key, SliceOutcome::Complete, pass, now);
        let (current_key, _, _) = registry.select(now, OffsetDateTime::UNIX_EPOCH).unwrap();
        assert_ne!(key, current_key);
        assert_eq!(registry.state.lock().unwrap().ranges.len(), 1);
    }
}

struct PausedSource {
    source: Source,
    reached: Notify,
    resume: Notify,
}

#[async_trait]
impl ActivityDiscoverySource for PausedSource {
    async fn page(
        &self,
        range: &ActivityDiscoveryRange,
        cutoff: OffsetDateTime,
        after: Option<ActivityDispatchPosition>,
        limit: NonZeroU32,
    ) -> Result<ActivityDispatchPage> {
        self.reached.notify_one();
        self.resume.notified().await;
        self.source.page(range, cutoff, after, limit).await
    }

    async fn resolve(&self, row: &ActivityDispatchRow) -> Result<Option<DispatchableActivityTask>> {
        self.source.resolve(row).await
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: activity-dispatch, Property 6
    // A page returning after registration retirement or takeover cannot admit into its replacement.
    #[test]
    fn activity_late_page_cannot_use_replacement_capacity(takeover in any::<bool>(), count in 1usize..64) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let now = Instant::now();
            let cutoff = OffsetDateTime::UNIX_EPOCH + time::Duration::hours(1);
            let source = Arc::new(PausedSource {
                source: Source { rows: (0..count).map(|i| row(i, true)).collect(), prefix: 0, fail_page: AtomicBool::new(false), reads: Mutex::new(Vec::new()) },
                reached: Notify::new(), resume: Notify::new(),
            });
            let assignment = home(0);
            let homes = Arc::new(Homes(Mutex::new(assignment.clone())));
            let broker = InMemoryActivityBroker::with_discovery(homes.clone());
            let registry = ActivityDiscoveryRegistry::default();
            let demand = registry.register_poll(queue(), assignment.clone(), now).unwrap();
            let (key, captured, mut pass) = registry.select(now, cutoff).unwrap();
            let waiting = {
                let source = source.clone();
                let registry = registry.clone();
                let broker = broker.clone();
                tokio::spawn(async move {
                    let outcome = run_slice(source.as_ref(), &registry, &broker, &key, &captured, &mut pass).await.unwrap();
                    (key, outcome, pass)
                })
            };
            source.reached.notified().await;
            drop(demand);
            let mut replacement = assignment.clone();
            if takeover {
                assignment.cancel.cancel();
                replacement = home(0);
                replacement.generation += 1;
                *homes.0.lock().unwrap() = replacement.clone();
            }
            assert_eq!(registry.retire(now + IDLE_GRACE + Duration::from_secs(1)).len(), 1);
            let _current = registry.register_poll(queue(), replacement, now).unwrap();
            source.resume.notify_one();
            let (key, outcome, pass) = waiting.await.unwrap();
            assert_eq!(outcome, SliceOutcome::Cancelled);
            registry.complete(&key, outcome, pass, now);
            for row in &source.source.rows { assert!(!broker.contains_activity_task(&row.task()).await); }
            let (key, current, mut pass) = registry.select(now, cutoff).unwrap();
            assert_eq!(run_slice(&source.source, &registry, &broker, &key, &current, &mut pass).await.unwrap(), SliceOutcome::Complete);
            for row in &source.source.rows { assert!(broker.contains_activity_task(&row.task()).await); }
        });
    }
}

#[test]
fn activity_registration_bounds_preserve_active_demands() {
    let registry = ActivityDiscoveryRegistry::default();
    let assignment = home(0);
    let now = Instant::now();
    let demands: Vec<_> = (0..4096)
        .map(|index| {
            registry
                .register_poll(
                    QueueKey {
                        task_queue: TaskQueueName(format!("q-{index}")),
                        ..queue()
                    },
                    assignment.clone(),
                    now,
                )
                .unwrap()
        })
        .collect();
    assert!(
        registry
            .register_poll(queue(), assignment.clone(), now)
            .unwrap_err()
            .downcast_ref::<ActivityDemandCapacity>()
            .is_some()
    );
    drop(demands);
    for index in 4096..4100 {
        drop(
            registry
                .register_poll(
                    QueueKey {
                        task_queue: TaskQueueName(format!("q-{index}")),
                        ..queue()
                    },
                    assignment.clone(),
                    now,
                )
                .unwrap(),
        );
    }
    let state = registry.state.lock().unwrap();
    assert_eq!(state.ranges.len(), 4096);
    assert!(state.ranges.values().all(|entry| entry.demands.is_empty()));
    let turns: BTreeSet<_> = state.ranges.values().map(|entry| entry.turn).collect();
    assert_eq!(turns.len(), 4096);
}
