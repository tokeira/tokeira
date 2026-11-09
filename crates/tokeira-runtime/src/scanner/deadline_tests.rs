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
        4,
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
        4,
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

const LANES: usize = 4;

fn config(page: usize) -> TimerScannerConfig {
    TimerScannerConfig {
        scan_interval: std::time::Duration::from_secs(3600),
        max_timers_per_scan: page,
    }
}

fn runs_on_lane(lane: usize, count: usize) -> Vec<RunKey> {
    (0u128..)
        .map(|value| RunKey(uuid::Uuid::from_u128(value)))
        .filter(|run| lane_index_for_run_key(*run, LANES) == lane)
        .take(count)
        .collect()
}

fn due(run_key: RunKey, id: &str, fire_at: OffsetDateTime) -> DueTimer {
    DueTimer {
        run_key,
        timer_id: id.into(),
        fire_at,
    }
}

fn commit(repo: &Repo, due: &DueTimer) {
    repo.timer_fixture
        .lock()
        .unwrap()
        .as_mut()
        .unwrap()
        .get_mut(&ShardId(0))
        .unwrap()
        .retain(|row| row != due);
}

type Submission = std::pin::Pin<Box<dyn Future<Output = Result<()>>>>;

/// Reports each submission as it starts, holds it until `release` grants a
/// permit, then commits it and reports it done.
fn gated(
    repo: Arc<Repo>,
    release: Arc<tokio::sync::Semaphore>,
    started: tokio::sync::mpsc::UnboundedSender<String>,
    done: tokio::sync::mpsc::UnboundedSender<String>,
) -> impl Fn(DueTimer, OffsetDateTime) -> Submission {
    move |due, _| {
        let (repo, release, started, done) =
            (repo.clone(), release.clone(), started.clone(), done.clone());
        Box::pin(async move {
            started.send(due.timer_id.clone()).unwrap();
            release.acquire().await.unwrap().forget();
            commit(&repo, &due);
            done.send(due.timer_id).unwrap();
            Ok(())
        })
    }
}

fn drain(received: &mut tokio::sync::mpsc::UnboundedReceiver<String>) -> Vec<String> {
    std::iter::from_fn(|| received.try_recv().ok()).collect()
}

// Due timers on different lanes are submitted without waiting for each other's
// replies, and each fires exactly once.
#[tokio::test(start_paused = true)]
async fn due_timers_on_different_lanes_are_in_flight_together() {
    let clock = Arc::new(TestClock::new());
    let repo = Arc::new(Repo::default());
    let owner = owner();
    let at = clock.now() - time::Duration::seconds(1);
    let rows = (0..3)
        .map(|lane| due(runs_on_lane(lane, 1)[0], &format!("lane-{lane}"), at))
        .collect();
    *repo.timer_fixture.lock().unwrap() = Some([(ShardId(0), rows)].into());
    let cancel = CancellationToken::new();
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let (started, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let (done, mut done_rx) = tokio::sync::mpsc::unbounded_channel();
    let scanner = run_timer_scanner_with_submit(
        repo.clone(),
        owner,
        config(10),
        cancel.clone(),
        clock,
        LANES,
        gated(repo.clone(), release.clone(), started, done),
    );
    let check = async {
        let mut outstanding = BTreeSet::new();
        for _ in 0..3 {
            outstanding.insert(started_rx.recv().await.unwrap());
        }
        assert_eq!(
            outstanding,
            ["lane-0", "lane-1", "lane-2"].map(String::from).into()
        );
        assert!(done_rx.try_recv().is_err(), "nothing was released yet");
        release.add_permits(3);
        let mut fired = BTreeSet::new();
        for _ in 0..3 {
            assert!(fired.insert(done_rx.recv().await.unwrap()));
        }
        assert_eq!(fired, outstanding);
        cancel.cancel();
    };
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::join!(scanner, check);
    })
    .await
    .unwrap();
    assert!(
        drain(&mut started_rx).is_empty(),
        "no timer is submitted twice"
    );
}

// One run's later timer is submitted only after its earlier timer's reply,
// while another lane's timer proceeds alongside the earlier one.
#[tokio::test(start_paused = true)]
async fn one_runs_due_timers_are_submitted_in_fire_order() {
    let clock = Arc::new(TestClock::new());
    let repo = Arc::new(Repo::default());
    let owner = owner();
    let now = clock.now();
    let run = runs_on_lane(0, 1)[0];
    let other = runs_on_lane(1, 1)[0];
    let rows = vec![
        due(run, "late", now - time::Duration::seconds(1)),
        due(other, "other", now - time::Duration::seconds(1)),
        due(run, "early", now - time::Duration::seconds(2)),
    ];
    *repo.timer_fixture.lock().unwrap() = Some([(ShardId(0), rows)].into());
    let cancel = CancellationToken::new();
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let (started, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let (done, mut done_rx) = tokio::sync::mpsc::unbounded_channel();
    let scanner = run_timer_scanner_with_submit(
        repo.clone(),
        owner,
        config(10),
        cancel.clone(),
        clock,
        LANES,
        gated(repo.clone(), release.clone(), started, done),
    );
    let check = async {
        let first = BTreeSet::from([
            started_rx.recv().await.unwrap(),
            started_rx.recv().await.unwrap(),
        ]);
        assert_eq!(first, ["early", "other"].map(String::from).into());
        assert!(
            started_rx.try_recv().is_err(),
            "the later timer waits for the earlier one's reply"
        );
        release.add_permits(3);
        assert_eq!(started_rx.recv().await.unwrap(), "late");
        for _ in 0..3 {
            done_rx.recv().await.unwrap();
        }
        cancel.cancel();
    };
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::join!(scanner, check);
    })
    .await
    .unwrap();
    assert!(
        drain(&mut started_rx).is_empty(),
        "no timer is submitted twice"
    );
}

// A failed submission is settled once and stays suppressed when a new timer
// rewinds the cursor within the same pass.
#[tokio::test(start_paused = true)]
async fn failed_submission_is_settled_once_per_pass() {
    let clock = Arc::new(TestClock::new());
    let repo = Arc::new(Repo::default());
    let owner = owner();
    let now = clock.now();
    let closed = due(
        runs_on_lane(0, 1)[0],
        "closed",
        now - time::Duration::seconds(1),
    );
    let ok = due(
        runs_on_lane(1, 1)[0],
        "ok",
        now - time::Duration::seconds(1),
    );
    *repo.timer_fixture.lock().unwrap() = Some([(ShardId(0), vec![closed.clone(), ok])].into());
    let cancel = CancellationToken::new();
    let submitted = Mutex::new(Vec::new());
    let scanner = run_timer_scanner_with_submit(
        repo.clone(),
        owner.clone(),
        config(10),
        cancel.clone(),
        clock,
        LANES,
        |due, _| {
            submitted.lock().unwrap().push(due.timer_id.clone());
            let result = match due.timer_id.as_str() {
                "closed" => Err(KernelRejected(Reject::RunClosed(
                    tokeira_types::ExecutionStatus::Completed,
                ))
                .into()),
                "ok" => {
                    commit(&repo, &due);
                    let inserted = DueTimer {
                        run_key: runs_on_lane(2, 1)[0],
                        timer_id: "inserted".into(),
                        fire_at: now - time::Duration::seconds(2),
                    };
                    repo.timer_fixture
                        .lock()
                        .unwrap()
                        .as_mut()
                        .unwrap()
                        .get_mut(&ShardId(0))
                        .unwrap()
                        .push(inserted.clone());
                    owner
                        .read()
                        .unwrap()
                        .timer_wake
                        .changed(ShardId(0), inserted.fire_at);
                    Ok(())
                }
                _ => {
                    commit(&repo, &due);
                    cancel.cancel();
                    Ok(())
                }
            };
            std::future::ready(result)
        },
    );
    tokio::time::timeout(std::time::Duration::from_secs(10), scanner)
        .await
        .unwrap();
    let mut submitted = submitted.into_inner().unwrap();
    submitted.sort();
    assert_eq!(submitted, ["closed", "inserted", "ok"]);
    assert_eq!(*repo.timer_deletes.lock().unwrap(), [closed]);
}

// Once the shard's acquisition is lost, no lane submits another timer, even
// those whose current submission was already in flight.
#[tokio::test(start_paused = true)]
async fn lost_acquisition_stops_every_lane() {
    let clock = Arc::new(TestClock::new());
    let repo = Arc::new(Repo::default());
    let owner = owner();
    let now = clock.now();
    let earlier = now - time::Duration::seconds(2);
    let later = now - time::Duration::seconds(1);
    let (a, b) = (runs_on_lane(0, 2), runs_on_lane(1, 2));
    let rows = vec![
        due(a[0], "a-first", earlier),
        due(a[1], "a-second", later),
        due(b[0], "b-first", earlier),
        due(b[1], "b-second", later),
    ];
    *repo.timer_fixture.lock().unwrap() = Some([(ShardId(0), rows)].into());
    let cancel = CancellationToken::new();
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let (started, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let (done, mut done_rx) = tokio::sync::mpsc::unbounded_channel();
    let scanner = run_timer_scanner_with_submit(
        repo.clone(),
        owner.clone(),
        config(10),
        cancel.clone(),
        clock,
        LANES,
        gated(repo.clone(), release.clone(), started, done),
    );
    let check = async {
        let first = BTreeSet::from([
            started_rx.recv().await.unwrap(),
            started_rx.recv().await.unwrap(),
        ]);
        assert_eq!(first, ["a-first", "b-first"].map(String::from).into());
        owner.write().unwrap().mark_draining(ShardId(0));
        release.add_permits(2);
        done_rx.recv().await.unwrap();
        done_rx.recv().await.unwrap();
        cancel.cancel();
    };
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::join!(scanner, check);
    })
    .await
    .unwrap();
    assert!(
        drain(&mut started_rx).is_empty(),
        "a lost acquisition submits nothing more"
    );
}
