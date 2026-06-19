//! The CHASM timer sweeper: the clock+loop that *fires* armed activity timeouts.
//!
//! The engine arms at most one physical-timer deadline per execution (the earliest
//! pure-task `fire_at`), but nothing in the engine fires it. This sweeper is that
//! missing piece, and nothing more: on each tick it scans the engine's armed-timer
//! map and, for every execution whose deadline is due, asks a
//! [`TimeoutEvaluator`] to apply the timeout under a fenced transition, then re-arms
//! the engine to the evaluator's next deadline.
//!
//! It holds **no authoritative state**. The timeout decision and the transition
//! live behind [`TimeoutEvaluator`] (implemented in the edge over the pure
//! `tokeira-chasm-activity` semantics), and the due deadlines are re-derivable from
//! node state — so a lost armed entry self-heals and a restart loses no timeout
//! (history is authority, timers are derived effects; `crates/tokeira-runtime/AGENTS.md`).
//! Keeping the trait here (rather than depending on the edge) preserves the crate
//! layering: the edge depends on the runtime, never the reverse.

use std::sync::Arc;

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

/// Fires armed activity timeouts on a tick. Runtime-only (clock + loop); all
/// semantics are in the [`TimeoutEvaluator`].
pub struct ChasmTimerSweeper {
    engine: Arc<ChasmEngine>,
    evaluator: Arc<dyn TimeoutEvaluator>,
}

impl ChasmTimerSweeper {
    /// Build a sweeper over `engine`, delegating timeout application to `evaluator`.
    pub fn new(engine: Arc<ChasmEngine>, evaluator: Arc<dyn TimeoutEvaluator>) -> Self {
        Self { engine, evaluator }
    }

    /// One sweep pass at the engine's current logical time: fire every due armed
    /// timeout and re-arm to its next deadline. Returns how many executions were
    /// evaluated (due this pass). A single evaluation error is logged and skipped so
    /// one stuck execution never stalls the others.
    pub async fn sweep_once(&self) -> usize {
        let now = self.engine.now();
        let mut evaluated = 0usize;
        // Snapshot the armed map so the lock is never held across an `await`.
        for (key, deadline) in self.engine.armed_timers_snapshot() {
            if deadline > now {
                continue;
            }
            match self.evaluator.evaluate_timeouts(&key, now).await {
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
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicI64, Ordering},
    };

    use async_trait::async_trait;
    use tokeira_chasm::ExecutionKey;
    use tokeira_storage::InMemoryChasmNodeStore;

    use super::*;
    use crate::chasm::{CollectingDispatchSink, NoopVisibilitySink};

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
            self.seen.lock().unwrap().push(key.clone());
            Ok(self.next)
        }
    }

    fn engine(now: Arc<AtomicI64>) -> Arc<ChasmEngine> {
        let registry = Arc::new(tokeira_chasm::Registry::builder().build());
        Arc::new(
            ChasmEngine::new(
                Arc::new(InMemoryChasmNodeStore::new()),
                registry,
                Arc::new(CollectingDispatchSink::default()),
                Arc::new(NoopVisibilitySink),
            )
            .with_clock(Arc::new(move || now.load(Ordering::SeqCst))),
        )
    }

    #[tokio::test]
    async fn fires_only_due_timers_and_rearms() {
        let now = Arc::new(AtomicI64::new(100));
        let engine = engine(now.clone());
        let due = ExecutionKey::new("ns", "due", "run");
        let future = ExecutionKey::new("ns", "future", "run");
        engine.set_armed_timer(&due, Some(50)); // already past
        engine.set_armed_timer(&future, Some(500)); // not yet

        let evaluator = Arc::new(RecordingEvaluator {
            next: Some(900),
            ..RecordingEvaluator::default()
        });
        let sweeper = ChasmTimerSweeper::new(engine.clone(), evaluator.clone());

        assert_eq!(sweeper.sweep_once().await, 1);
        let seen = evaluator.seen.lock().unwrap().clone();
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
        engine.set_armed_timer(&key, Some(10));
        let evaluator = Arc::new(RecordingEvaluator::default()); // next = None
        let sweeper = ChasmTimerSweeper::new(engine.clone(), evaluator);
        sweeper.sweep_once().await;
        assert_eq!(engine.armed_timer(&key), None, "terminal clears the timer");
    }
}
