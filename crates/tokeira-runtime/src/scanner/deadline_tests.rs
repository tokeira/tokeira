//! Paging and wake isolation with counted storage listings and a supplied clock.

use super::*;
use crate::{runtime::repair_tests::Repo, test_clock::TestClock};
use std::{future::Future, task::Poll};
use tokeira_types::ShardEpoch;

fn owner() -> Arc<RwLock<ShardOwner>> {
    let mut owner = ShardOwner::new(4);
    for id in 0..3 {
        owner.record_acquired(ShardId(id), ShardEpoch(1));
        owner.mark_active(ShardId(id));
    }
    Arc::new(RwLock::new(owner))
}

fn row(now: OffsetDateTime, id: &str) -> DueTimer {
    DueTimer {
        run_key: RunKey::new(),
        timer_id: id.into(),
        fire_at: now,
    }
}

#[tokio::test(start_paused = true)]
async fn capped_backlog_rewinds_for_insert_and_defers_failed_identity() {
    let clock = Arc::new(TestClock::new());
    let repo = Arc::new(Repo::default());
    let owner = owner();
    let now = clock.now();
    let failed = row(now - time::Duration::seconds(3), "failed");
    let first = row(now - time::Duration::seconds(2), "first");
    let last = row(now - time::Duration::seconds(1), "last");
    *repo.timer_fixture.lock().unwrap() = Some([(ShardId(0), vec![last, first, failed])].into());
    let cancel = CancellationToken::new();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let scanner = run_timer_scanner_with_submit(
        repo.clone(),
        owner.clone(),
        TimerScannerConfig {
            scan_interval: std::time::Duration::from_secs(3600),
            max_timers_per_scan: 1,
        },
        cancel.clone(),
        clock.clone(),
        |due, fired_at| {
            let repo = repo.clone();
            let owner = owner.clone();
            let cancel = cancel.clone();
            let seen = seen.clone();
            async move {
                let mut seen = seen.lock().unwrap();
                seen.push(due.timer_id.clone());
                if due.timer_id == "failed" {
                    if seen.iter().filter(|id| *id == "failed").count() == 2 {
                        assert!(fired_at >= now + time::Duration::hours(1));
                        cancel.cancel();
                    }
                    anyhow::bail!("submission unavailable");
                }
                let mut fixture = repo.timer_fixture.lock().unwrap();
                let rows = fixture.as_mut().unwrap().get_mut(&ShardId(0)).unwrap();
                rows.retain(|row| row != &due);
                if due.timer_id == "first" {
                    let earlier = row(now - time::Duration::seconds(4), "inserted");
                    rows.push(earlier.clone());
                    owner
                        .read()
                        .unwrap()
                        .timer_wake
                        .changed(ShardId(0), earlier.fire_at);
                }
                if due.timer_id == "last" {
                    assert_eq!(fired_at, now, "the capped backlog must drain immediately");
                }
                Ok(())
            }
        },
    );
    tokio::time::timeout(std::time::Duration::from_secs(3601), scanner)
        .await
        .unwrap();
    assert_eq!(
        *seen.lock().unwrap(),
        ["failed", "first", "inserted", "last", "failed"]
    );
    assert_eq!(clock.now(), now + time::Duration::hours(1));
}

#[tokio::test(start_paused = true)]
async fn deadline_wake_lists_only_its_shard_and_acquisition_looks_ahead() {
    let clock = Arc::new(TestClock::new());
    let repo = Arc::new(Repo::default());
    *repo.timer_fixture.lock().unwrap() = Some(BTreeMap::new());
    let owner = owner();
    let cancel = CancellationToken::new();
    let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
    let scanner = run_timer_scanner_with_submit(
        repo.clone(),
        owner.clone(),
        TimerScannerConfig {
            scan_interval: std::time::Duration::from_secs(3600),
            max_timers_per_scan: 10,
        },
        cancel.clone(),
        clock.clone(),
        |due, _| {
            sent.send(due.timer_id).unwrap();
            std::future::ready(Ok(()))
        },
    );
    tokio::pin!(scanner);
    std::future::poll_fn(|cx| {
        assert!(scanner.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let check = async {
        // Let the initial zero-delay pass finish before measuring targeted reads.
        tokio::task::yield_now().await;
        repo.timer_reads.lock().unwrap().clear();
        let due = row(clock.now() + time::Duration::milliseconds(200), "commit");
        repo.timer_fixture
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .insert(ShardId(1), vec![due.clone()]);
        owner
            .read()
            .unwrap()
            .timer_wake
            .changed(ShardId(1), due.fire_at);
        assert_eq!(received.recv().await.unwrap(), "commit");
        assert_eq!(*repo.timer_reads.lock().unwrap(), [ShardId(1)]);
        repo.timer_reads.lock().unwrap().clear();
        let due = row(clock.now() + time::Duration::milliseconds(200), "acquired");
        repo.timer_fixture
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .insert(ShardId(3), vec![due]);
        {
            let mut owner = owner.write().unwrap();
            owner.record_acquired(ShardId(3), ShardEpoch(1));
            owner.mark_active(ShardId(3));
        }
        assert_eq!(received.recv().await.unwrap(), "acquired");
        assert!(
            repo.timer_reads
                .lock()
                .unwrap()
                .iter()
                .all(|home| *home == ShardId(3))
        );
        cancel.cancel();
    };
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        tokio::join!(scanner, check);
    })
    .await
    .unwrap();
}
