//! Fires due CHASM timers in deadline order with a stable execution tie-break. Each archetype may
//! retain an existing timeout evaluator; other roots execute registered pure tasks
//! under a fenced transition. The armed map is disposable and rebuilt from state.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use tokeira_chasm::ExecutionKey;

use super::ChasmEngine;

/// Applies due timeouts for one execution. Implemented by the activity edge bridge
/// over the pure retry/timeout semantics; the sweeper drives it without knowing the
/// archetype.
#[async_trait]
pub trait TimeoutEvaluator: Send + Sync {
    /// Evaluate the execution at `now`, applying at most one timeout (or a
    /// retry-reschedule) under a single fenced transition. Returns the next timeout
    /// deadline to re-arm — `None` when the execution is terminal, gone, or has no
    /// timeout outstanding. Idempotent and fenced: a not-actually-due or superseded
    /// evaluation is a no-op that still returns the next deadline.
    async fn evaluate_timeouts(&self, key: &ExecutionKey, now: i64) -> anyhow::Result<Option<i64>>;
}

/// Fires armed root timers through registered handlers or an archetype's
/// existing [`TimeoutEvaluator`]. Holds no authoritative execution state.
pub struct ChasmTimerSweeper {
    engine: Arc<ChasmEngine>,
    evaluators: HashMap<u32, Arc<dyn TimeoutEvaluator>>,
}

// Manual impl: composed of trait objects with no `Debug` bound.
impl std::fmt::Debug for ChasmTimerSweeper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChasmTimerSweeper").finish_non_exhaustive()
    }
}

impl ChasmTimerSweeper {
    /// Build a sweeper that executes registered pure handlers by default.
    pub fn new(engine: Arc<ChasmEngine>) -> Self {
        Self {
            engine,
            evaluators: HashMap::new(),
        }
    }

    /// Preserve an archetype's existing evaluator instead of invoking its pure
    /// handlers. The activity evaluator retains its timeout precedence rules.
    pub fn with_evaluator(
        mut self,
        archetype_id: u32,
        evaluator: Arc<dyn TimeoutEvaluator>,
    ) -> Self {
        self.evaluators.insert(archetype_id, evaluator);
        self
    }

    /// Evaluate armed timers at their deadlines; `interval` is the reconciliation
    /// backstop for failed evaluations. Arming an earlier timer interrupts the wait.
    /// Logical time comes only from the engine's clock, while Tokio owns the sleep.
    pub async fn run(
        &self,
        interval: std::time::Duration,
        cancel: tokio_util::sync::CancellationToken,
    ) {
        let wall = |now: i64| {
            time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(now))
                .expect("i64 nanoseconds are representable")
        };
        let wake = &self.engine.timer_wake;
        let mut full = crate::deadline_wake::FullPass::new(interval, wall(self.engine.now()));
        loop {
            let now = self.engine.now();
            let revision = wake.begin_plan();
            // Bound monotonic waits even when an embedder chooses a very long
            // reconciliation interval; the logical deadline remains unchanged.
            let delay = self.engine.timer_delay(
                now,
                full.remaining(wall(now))
                    .min(std::time::Duration::from_secs(86_400)),
            );
            let delay = wake.plan(wall(now), revision, delay);
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = wake.notify.notified() => continue,
                _ = tokio::time::sleep(delay) => {}
            }
            let now = self.engine.now();
            wake.start_scan(wall(now));
            self.sweep_at(now, full.start(wall(now))).await;
        }
    }

    async fn evaluate(&self, key: &ExecutionKey, now: i64) -> anyhow::Result<Option<i64>> {
        let Some(root) = self.engine.root_node(key).await? else {
            return Ok(None);
        };
        if let Some(evaluator) = self.evaluators.get(&root.metadata.component_type_id) {
            evaluator.evaluate_timeouts(key, now).await
        } else {
            Ok(self.engine.execute_due_pure_tasks(key, now).await?)
        }
    }

    /// One sweep pass at the engine's current logical time: fire every due armed
    /// timeout and re-arm to its next deadline. Returns how many executions were
    /// evaluated (due this pass). A single evaluation error is logged and skipped so
    /// one stuck execution never stalls the others.
    pub async fn sweep_once(&self) -> usize {
        self.sweep_at(self.engine.now(), true).await
    }

    async fn sweep_at(&self, now: i64, full: bool) -> usize {
        let mut evaluated = 0usize;
        // Only selected executions are copied; no lock crosses an await.
        for (key, _) in self.engine.due_timers(now, full) {
            match self.evaluate(&key, now).await {
                Ok(next) => {
                    // Re-arm to the state-derived next deadline (or clear it). This
                    // overrides the engine's pure-task-derived arming, which can be
                    // stale once a heartbeat pushes the deadline out — the sweeper's
                    // re-derivation from durable state is authoritative for "when
                    // next to wake" (Property 4: derived).
                    self.engine.set_armed_timer(&key, next);
                    evaluated += 1;
                }
                Err(error) => {
                    tracing::warn!(?error, ?key, "chasm timeout evaluation failed");
                }
            }
        }
        evaluated
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex, atomic::AtomicI64};

    use async_trait::async_trait;
    use tokeira_chasm::ExecutionKey;
    use tokeira_storage::InMemoryChasmNodeStore;

    use super::*;
    use crate::chasm::test_support::{self, Root};
    use tokeira_chasm::{Component, archetype_id_for_fqn};

    /// Records the keys it was asked to evaluate; re-arms to a fixed next deadline.
    #[derive(Default)]
    struct RecordingEvaluator {
        seen: Mutex<Vec<ExecutionKey>>,
        next: Option<i64>,
    }

    #[async_trait]
    impl TimeoutEvaluator for RecordingEvaluator {
        async fn evaluate_timeouts(
            &self,
            key: &ExecutionKey,
            _now: i64,
        ) -> anyhow::Result<Option<i64>> {
            self.seen
                .lock()
                .expect("seen lock poisoned")
                .push(key.clone());
            Ok(self.next)
        }
    }

    fn engine(now: Arc<AtomicI64>) -> Arc<ChasmEngine> {
        crate::chasm::test_support::engine(
            Arc::new(InMemoryChasmNodeStore::new()),
            now,
            crate::chasm::test_support::sink(),
        )
    }

    #[tokio::test]
    async fn fires_only_due_timers_and_rearms() {
        let now = Arc::new(AtomicI64::new(100));
        let engine = engine(now.clone());
        let due = ExecutionKey::new("ns", "due", "run");
        let future = ExecutionKey::new("ns", "future", "run");
        test_support::start(&engine, due.clone()).await;
        test_support::start(&engine, future.clone()).await;
        engine.set_armed_timer(&due, Some(50)); // already past
        engine.set_armed_timer(&future, Some(500)); // not yet

        let evaluator = Arc::new(RecordingEvaluator {
            next: Some(900),
            ..RecordingEvaluator::default()
        });
        let sweeper = ChasmTimerSweeper::new(engine.clone())
            .with_evaluator(archetype_id_for_fqn(Root::FQN), evaluator.clone());

        assert_eq!(sweeper.sweep_once().await, 1);
        let seen = evaluator.seen.lock().expect("seen lock poisoned").clone();
        assert_eq!(seen, vec![due.clone()], "only the due timer fires");
        // The due timer is re-armed to the evaluator's next deadline; the future one
        // is untouched.
        assert_eq!(engine.armed_timer(&due), Some(900));
        assert_eq!(engine.armed_timer(&future), Some(500));
    }

    #[tokio::test]
    async fn clears_timer_when_evaluator_returns_none() {
        let now = Arc::new(AtomicI64::new(100));
        let engine = engine(now.clone());
        let key = ExecutionKey::new("ns", "act", "run");
        test_support::start(&engine, key.clone()).await;
        engine.set_armed_timer(&key, Some(10));
        let evaluator = Arc::new(RecordingEvaluator::default()); // next = None
        let sweeper = ChasmTimerSweeper::new(engine.clone())
            .with_evaluator(archetype_id_for_fqn(Root::FQN), evaluator);
        sweeper.sweep_once().await;
        assert_eq!(engine.armed_timer(&key), None, "terminal clears the timer");
    }
}

#[cfg(test)]
mod properties {
    use super::ChasmTimerSweeper;
    use crate::chasm::test_support::{self as ts, Work};
    use proptest::prelude::*;
    use std::sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    };
    use tokeira_storage::InMemoryChasmNodeStore;

    // Feature: chasm-extension-archetypes, Property 3: pure-task execution model
    // Only due, currently valid tasks execute, once, in deadline/id order.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]
        #[test]
        fn pure_tasks_follow_clock_model(tasks in prop::collection::vec((1i64..80, 0i64..100), 0..24), increments in prop::collection::vec(0i64..20, 1..15)) {
            ts::runtime().block_on(async {
                let now = Arc::new(AtomicI64::new(0));
                let engine = ts::engine(Arc::new(InMemoryChasmNodeStore::new()), now.clone(), ts::sink());
                let key = ts::key(0);
                let reference = ts::start(&engine, key.clone()).await;
                let tasks: Vec<_> = tasks.iter().enumerate().map(|(i, &(deadline, drop_at))| Work::<false> { token: i as u32, deadline, drop_at, ..Default::default() }).collect();
                ts::stage(&engine, &reference, &tasks).await;
                let sweeper = ChasmTimerSweeper::new(engine.clone());
                let mut held = tasks.clone();
                let mut expected = Vec::new();
                let mut time = 0;
                for delta in increments.into_iter().chain([200]) {
                    time += delta;
                    now.store(time, Ordering::SeqCst);
                    let due = held.iter().any(|t| t.deadline <= time);
                    if due {
                        let mut ready: Vec<_> = held.iter().filter(|t| t.deadline <= time).collect();
                        ready.sort_by_key(|t| (t.deadline, t.token));
                        expected.extend(ready.into_iter().filter(|t| ts::valid(t.drop_at, time)).map(|t| t.token));
                        held.retain(|t| t.deadline > time && ts::valid(t.drop_at, time));
                    }
                    prop_assert_eq!(sweeper.sweep_once().await, usize::from(due));
                    prop_assert_eq!(ts::data(&engine, &key).await.pure, expected.clone());
                    prop_assert_eq!(engine.armed_timer(&key), held.iter().map(|t| t.deadline).min());
                    prop_assert_eq!(sweeper.sweep_once().await, 0);
                }
                Ok(())
            })?;
        }
    }
}

#[cfg(test)]
mod deadline_tests;
