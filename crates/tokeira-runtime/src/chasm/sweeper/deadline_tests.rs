//! Deadline delivery uses the same logical clock as timer arming.

use super::*;
use crate::{
    chasm::test_support::{self as ts, Root},
    test_clock::TestClock,
};
use std::{future::Future, sync::atomic::AtomicI64, task::Poll};
use tokeira_chasm::{Component, archetype_id_for_fqn};
use tokeira_clock::Clock;
use tokeira_storage::InMemoryChasmNodeStore;
use tokio_util::sync::CancellationToken;

struct Observe(tokio::sync::mpsc::UnboundedSender<(ExecutionKey, i64)>);
#[async_trait]
impl TimeoutEvaluator for Observe {
    async fn evaluate_timeouts(&self, key: &ExecutionKey, now: i64) -> anyhow::Result<Option<i64>> {
        self.0.send((key.clone(), now)).unwrap();
        Ok(None)
    }
}

async fn deadline(arm_asleep: bool) {
    let clock = Arc::new(TestClock::new());
    let engine = ts::engine(
        Arc::new(InMemoryChasmNodeStore::new()),
        Arc::new(AtomicI64::new(0)),
        ts::sink(),
    );
    let clock_fn = clock.clone();
    let engine = Arc::new(
        Arc::try_unwrap(engine)
            .unwrap()
            .with_clock(Arc::new(move || {
                i64::try_from(clock_fn.now().unix_timestamp_nanos()).unwrap()
            })),
    );
    let key = ExecutionKey::new("ns", "activity", "run");
    ts::start(&engine, key.clone()).await;
    let due = engine.now() + 200_000_000;
    if !arm_asleep {
        engine.set_armed_timer(&key, Some(due));
    }
    let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
    let sweeper = ChasmTimerSweeper::new(engine.clone())
        .with_evaluator(archetype_id_for_fqn(Root::FQN), Arc::new(Observe(sent)));
    let cancel = CancellationToken::new();
    let scan = sweeper.run(std::time::Duration::from_secs(3600), cancel.clone());
    tokio::pin!(scan);
    std::future::poll_fn(|cx| {
        assert!(scan.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let check = async {
        if arm_asleep {
            engine.set_armed_timer(&key, Some(due));
        }
        let (observed, at) =
            tokio::time::timeout(std::time::Duration::from_secs(2), received.recv())
                .await
                .expect("armed deadline must wake sweeper")
                .unwrap();
        assert_eq!(observed, key);
        assert!(at >= due && at < due + 10_000_000);
        cancel.cancel();
    };
    tokio::join!(scan, check);
}

#[tokio::test(start_paused = true)]
async fn armed_before_sleep_fires_at_deadline() {
    deadline(false).await;
}

#[tokio::test(start_paused = true)]
async fn earlier_arm_wakes_sleeping_sweeper() {
    deadline(true).await;
}

#[tokio::test(start_paused = true)]
async fn due_selection_is_ordered_and_failed_entries_wait_for_full_pass() {
    let engine = ts::engine(
        Arc::new(InMemoryChasmNodeStore::new()),
        Arc::new(AtomicI64::new(100)),
        ts::sink(),
    );
    for i in 0..1000 {
        engine.set_armed_timer(
            &ExecutionKey::new("ns", format!("future-{i}"), "run"),
            Some(500),
        );
    }
    let early = ExecutionKey::new("ns", "z", "run");
    let late = ExecutionKey::new("ns", "a", "run");
    engine.set_armed_timer(&late, Some(90));
    engine.set_armed_timer(&early, Some(80));
    assert_eq!(
        engine.due_timers(100, false),
        [(early.clone(), 80), (late, 90)]
    );
    assert!(engine.due_timers(100, false).is_empty());
    assert_eq!(
        engine.timer_delay(100, std::time::Duration::from_secs(1)),
        std::time::Duration::from_nanos(400)
    );
    let inserted = ExecutionKey::new("ns", "inserted", "run");
    engine.set_armed_timer(&inserted, Some(95));
    assert_eq!(engine.due_timers(100, false), [(inserted, 95)]);
    assert_eq!(engine.due_timers(100, true).len(), 3);
    engine.set_armed_timer(&early, Some(i64::MAX));
    assert!(
        engine.timer_delay(i64::MIN, std::time::Duration::from_secs(1))
            <= std::time::Duration::from_secs(1)
    );
}

struct Failing(tokio::sync::mpsc::UnboundedSender<String>);
#[async_trait]
impl TimeoutEvaluator for Failing {
    async fn evaluate_timeouts(&self, key: &ExecutionKey, _: i64) -> anyhow::Result<Option<i64>> {
        self.0.send(key.business_id.clone()).unwrap();
        if key.business_id == "failed" {
            anyhow::bail!("evaluation unavailable");
        }
        Ok(None)
    }
}

#[tokio::test(start_paused = true)]
async fn failed_evaluation_survives_unrelated_wake_until_full_pass() {
    let clock = Arc::new(TestClock::new());
    let engine = ts::engine(
        Arc::new(InMemoryChasmNodeStore::new()),
        Arc::new(AtomicI64::new(0)),
        ts::sink(),
    );
    let clock_fn = clock.clone();
    let engine = Arc::new(
        Arc::try_unwrap(engine)
            .unwrap()
            .with_clock(Arc::new(move || {
                i64::try_from(clock_fn.now().unix_timestamp_nanos()).unwrap()
            })),
    );
    let failed = ExecutionKey::new("ns", "failed", "run");
    let other = ExecutionKey::new("ns", "other", "run");
    ts::start(&engine, failed.clone()).await;
    ts::start(&engine, other.clone()).await;
    engine.set_armed_timer(&failed, Some(engine.now()));
    let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
    let sweeper = ChasmTimerSweeper::new(engine.clone())
        .with_evaluator(archetype_id_for_fqn(Root::FQN), Arc::new(Failing(sent)));
    let cancel = CancellationToken::new();
    let scan = sweeper.run(std::time::Duration::from_secs(3600), cancel.clone());
    let check = async {
        assert_eq!(received.recv().await.unwrap(), "failed");
        engine.set_armed_timer(&other, Some(engine.now() + 200_000_000));
        assert_eq!(received.recv().await.unwrap(), "other");
        assert!(received.try_recv().is_err());
        assert!(engine.armed_timer(&failed).is_some());
        tokio::time::advance(std::time::Duration::from_secs(3600)).await;
        assert_eq!(received.recv().await.unwrap(), "failed");
        cancel.cancel();
    };
    tokio::join!(scan, check);
}
