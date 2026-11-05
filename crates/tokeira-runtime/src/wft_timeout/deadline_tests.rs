//! Deadline selection and scanner wakes, independent of lane submission.

use std::{future::Future, task::Poll};

use proptest::prelude::*;
use tokio::sync::mpsc;

use super::{
    tests::{fixed_now, sample_entry},
    *,
};

#[derive(Debug)]
struct TestClock(tokio::time::Instant);

impl Clock for TestClock {
    fn now(&self) -> OffsetDateTime {
        fixed_now() + Duration::try_from(self.0.elapsed()).unwrap()
    }
}

fn active_tracking() -> (WftTimeoutTrackingState, Arc<RwLock<ShardOwner>>) {
    let owner = Arc::new(RwLock::new(ShardOwner::new(8)));
    owner
        .write()
        .unwrap()
        .record_acquired(ShardId(0), tokeira_types::ShardEpoch(1));
    owner.write().unwrap().mark_active(ShardId(0));
    let tracking = WftTimeoutTrackingState::default();
    tracking.set_owner(owner.clone());
    (tracking, owner)
}

#[test]
fn scan_delay_uses_the_earliest_active_deadline_bounded_by_interval() {
    let (tracking, _) = active_tracking();
    let now = fixed_now();
    let maximum = std::time::Duration::from_secs(10);
    let mut later = sample_entry(RunKey::new(), now);
    later.workflow_task_timeout = Duration::seconds(20);
    tracking.insert(later);
    assert_eq!(wft_timeout_scan_delay(&tracking, now, maximum), maximum);
    let mut earlier = sample_entry(RunKey::new(), now);
    earlier.kind = WftTimeoutKind::ScheduleToStart;
    earlier.workflow_task_timeout = Duration::milliseconds(200);
    tracking.insert(earlier);
    assert_eq!(
        wft_timeout_scan_delay(&tracking, now, maximum),
        std::time::Duration::from_millis(200)
    );
}

#[test]
fn scan_delay_matches_both_evaluator_boundaries_and_zero() {
    for kind in [
        WftTimeoutKind::ScheduleToStart,
        WftTimeoutKind::StartToClose,
    ] {
        for timeout in [Duration::ZERO, Duration::seconds(2)] {
            let tracking = WftTimeoutTrackingState::default();
            let now = fixed_now();
            let mut entry = sample_entry(RunKey::new(), now + Duration::seconds(1));
            entry.kind = kind;
            entry.workflow_task_timeout = timeout;
            let due = entry.started_at
                + timeout
                + if kind == WftTimeoutKind::StartToClose && !timeout.is_zero() {
                    Duration::NANOSECOND
                } else {
                    Duration::ZERO
                };
            assert!(!evaluate_wft_timeout(&entry, due - Duration::NANOSECOND));
            assert!(evaluate_wft_timeout(&entry, due));
            tracking.insert(entry);
            let delay = wft_timeout_scan_delay(&tracking, now, std::time::Duration::from_secs(10));
            assert_eq!(now + delay, due);
        }
    }
}

#[test]
fn scan_delay_ignores_overdue_and_inactive_entries() {
    let (tracking, owner) = active_tracking();
    let now = fixed_now();
    let maximum = std::time::Duration::from_secs(10);
    tracking.insert(sample_entry(RunKey::new(), now - Duration::seconds(20)));
    let old = owner.read().unwrap().acquisition(ShardId(0)).unwrap();
    let future = sample_entry(RunKey::new(), now);
    tracking.for_acquisition(old).insert(future);
    owner
        .write()
        .unwrap()
        .record_acquired(ShardId(0), tokeira_types::ShardEpoch(2));
    owner.write().unwrap().mark_active(ShardId(0));
    tracking.insert(sample_entry(RunKey::new(), now - Duration::seconds(20)));
    assert_eq!(wft_timeout_scan_delay(&tracking, now, maximum), maximum);
}

#[test]
fn scan_delay_saturates_at_the_end_of_the_date_range() {
    let end = time::Date::MAX.with_time(time::Time::MAX).assume_utc();
    for kind in [
        WftTimeoutKind::ScheduleToStart,
        WftTimeoutKind::StartToClose,
    ] {
        let tracking = WftTimeoutTrackingState::default();
        let now = end - Duration::seconds(1);
        let mut entry = sample_entry(RunKey::new(), now);
        entry.kind = kind;
        entry.workflow_task_timeout = Duration::MAX;
        tracking.insert(entry);
        let delay = wft_timeout_scan_delay(&tracking, now, std::time::Duration::from_secs(10));
        assert_eq!(delay, std::time::Duration::from_secs(1));
        assert_eq!(
            wft_timeout_scan_delay(&tracking, end, std::time::Duration::from_secs(10)),
            std::time::Duration::from_secs(10)
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: workflow-dispatch, Property 7: Deadline wakes respect the earliest active task.
    // The interval is an upper bound; an active future deadline is never crossed by the sleep.
    #[test]
    fn scan_delay_never_crosses_an_active_future_deadline(
        cases in prop::collection::vec((-10_000i64..10_000, 0i64..1000, any::<bool>(), any::<bool>()), 0..30),
        interval_ms in 1u64..20_000,
    ) {
        let (tracking, owner) = active_tracking();
        owner.write().unwrap().record_acquired(ShardId(1), tokeira_types::ShardEpoch(1));
        let now = fixed_now();
        let maximum = std::time::Duration::from_millis(interval_ms);
        let mut expected = maximum;
        for (anchor_ms, timeout_ms, schedule_to_start, active) in cases {
            let mut entry = sample_entry(RunKey::new(), now + Duration::milliseconds(anchor_ms));
            entry.shard_id = ShardId(u32::from(!active));
            entry.kind = if schedule_to_start { WftTimeoutKind::ScheduleToStart } else { WftTimeoutKind::StartToClose };
            entry.workflow_task_timeout = Duration::milliseconds(timeout_ms);
            let remaining_ns = (anchor_ms + timeout_ms) * 1_000_000 + i64::from(!schedule_to_start && timeout_ms != 0);
            if active && remaining_ns > 0 {
                expected = expected.min(std::time::Duration::from_nanos(remaining_ns as u64));
            }
            tracking.insert(entry);
        }
        let delay = wft_timeout_scan_delay(&tracking, now, maximum);
        prop_assert!(delay <= maximum);
        prop_assert_eq!(delay, expected);
    }
}

async fn deadline_loop(insert_while_asleep: bool) {
    let clock: Arc<dyn Clock> = Arc::new(TestClock(tokio::time::Instant::now()));
    let (tracking, owner) = active_tracking();
    let cancel = CancellationToken::new();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let config = WftTimeoutScannerConfig {
        scan_interval: std::time::Duration::from_secs(3600),
        ..Default::default()
    };
    let mut entry = sample_entry(RunKey::new(), clock.now());
    entry.workflow_task_timeout = Duration::milliseconds(100);
    if !insert_while_asleep {
        tracking.insert(entry.clone());
    }
    let scanner = run_wft_timeout_scanner_with_submit(
        tracking.clone(),
        owner,
        config,
        cancel.clone(),
        clock.clone(),
        move |entry, now| {
            tx.send((entry, now)).unwrap();
            async { Ok(()) }
        },
    );
    tokio::pin!(scanner);
    std::future::poll_fn(|cx| {
        assert!(scanner.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    if insert_while_asleep {
        entry.started_at = clock.now();
        tracking.insert(entry.clone());
    }
    let submitted = async {
        let (actual, now) = tokio::time::timeout(std::time::Duration::from_secs(3), rx.recv())
            .await
            .expect("the deadline must wake the one-hour scanner")
            .unwrap();
        assert_eq!(actual, entry);
        assert!(now > entry.started_at + entry.workflow_task_timeout);
        assert!(now <= clock.now());
        cancel.cancel();
    };
    tokio::join!(scanner, submitted);
}

#[tokio::test]
async fn loop_submits_a_deadline_tracked_before_sleep() {
    deadline_loop(false).await;
}

#[tokio::test]
async fn loop_wakes_for_an_earlier_deadline_inserted_while_asleep() {
    deadline_loop(true).await;
}

#[tokio::test]
async fn insert_notifies_only_when_it_advances_the_planned_wake() {
    let tracking = WftTimeoutTrackingState::default();
    let now = fixed_now();
    tracking.insert(sample_entry(RunKey::new(), now));
    tracking.wake.notified().await;
    wft_timeout_scan_delay(&tracking, now, std::time::Duration::from_secs(10));
    tracking.insert(sample_entry(RunKey::new(), now + Duration::seconds(1)));
    let notified = tracking.wake.notified();
    tokio::pin!(notified);
    std::future::poll_fn(|cx| {
        assert!(
            notified.as_mut().poll(cx).is_pending(),
            "a later entry must not wake the scanner"
        );
        Poll::Ready(())
    })
    .await;
    tracking.insert(sample_entry(RunKey::new(), now - Duration::seconds(1)));
    tokio::time::timeout(std::time::Duration::from_secs(3), notified)
        .await
        .unwrap();
}

#[tokio::test]
async fn future_insert_during_scan_does_not_store_a_wake() {
    let clock: Arc<dyn Clock> = Arc::new(TestClock(tokio::time::Instant::now()));
    let (tracking, owner) = active_tracking();
    let mut due = sample_entry(RunKey::new(), clock.now());
    due.workflow_task_timeout = Duration::ZERO;
    tracking.insert(due);
    let (submitted_tx, mut submitted_rx) = mpsc::unbounded_channel();
    let scanner = run_wft_timeout_scanner_with_submit(
        tracking.clone(),
        owner,
        WftTimeoutScannerConfig {
            scan_interval: std::time::Duration::from_secs(3600),
            ..Default::default()
        },
        CancellationToken::new(),
        clock.clone(),
        move |entry, _| {
            submitted_tx.send(entry).unwrap();
            std::future::pending::<Result<()>>()
        },
    );
    tokio::pin!(scanner);
    std::future::poll_fn(|cx| {
        assert!(scanner.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    submitted_rx
        .try_recv()
        .expect("the scan is submitting a timeout");
    tracking.insert(sample_entry(RunKey::new(), clock.now()));
    let notified = tracking.wake.notified();
    tokio::pin!(notified);
    std::future::poll_fn(|cx| {
        assert!(
            notified.as_mut().poll(cx).is_pending(),
            "a future entry tracked during scanning must not store a wake"
        );
        Poll::Ready(())
    })
    .await;
}

#[test]
fn scan_delay_is_immediate_for_an_entry_that_became_overdue_during_the_scan() {
    let (tracking, _) = active_tracking();
    let now = fixed_now();
    tracking.start_scan(now);
    let mut entry = sample_entry(RunKey::new(), now + Duration::seconds(1));
    entry.workflow_task_timeout = Duration::ZERO;
    tracking.insert(entry);
    assert_eq!(
        wft_timeout_scan_delay(
            &tracking,
            now + Duration::seconds(2),
            std::time::Duration::from_secs(10),
        ),
        std::time::Duration::ZERO
    );
}

#[tokio::test]
async fn scan_delay_keeps_the_interval_for_a_pre_scan_entry_whose_submission_failed() {
    let (tracking, _) = active_tracking();
    let now = fixed_now();
    let entry = sample_entry(RunKey::new(), now - Duration::seconds(10));
    tracking.insert(entry.clone());
    tracking.start_scan(now);
    scan_wft_timeouts_at(
        &tracking,
        Some(ShardId(0)),
        &WftTimeoutScannerConfig::default(),
        now,
        |_, _| async { Err(anyhow::anyhow!("submission failed")) },
    )
    .await;
    assert_eq!(tracking.snapshot(), vec![entry]);
    let maximum = std::time::Duration::from_secs(10);
    assert_eq!(
        wft_timeout_scan_delay(&tracking, now + Duration::seconds(2), maximum),
        maximum
    );
}

#[tokio::test]
async fn loop_does_not_lose_an_insert_during_submission() {
    let clock: Arc<dyn Clock> = Arc::new(TestClock(tokio::time::Instant::now()));
    let (tracking, owner) = active_tracking();
    let cancel = CancellationToken::new();
    let config = WftTimeoutScannerConfig {
        scan_interval: std::time::Duration::from_secs(3600),
        ..Default::default()
    };
    let mut first = sample_entry(RunKey::new(), clock.now());
    first.workflow_task_timeout = Duration::milliseconds(5);
    tracking.insert(first.clone());
    let (in_submit_tx, in_submit_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut barrier = Some((in_submit_tx, release_rx));
    let scanner = run_wft_timeout_scanner_with_submit(
        tracking.clone(),
        owner,
        config,
        cancel.clone(),
        clock.clone(),
        move |entry, now| {
            let barrier = if entry.run_key == first.run_key {
                barrier.take()
            } else {
                None
            };
            let tx = tx.clone();
            async move {
                if let Some((in_submit, release)) = barrier {
                    in_submit.send(()).unwrap();
                    release.await.unwrap();
                } else {
                    tx.send((entry, now)).unwrap();
                }
                Ok(())
            }
        },
    );
    let insert_during_scan = async {
        in_submit_rx.await.unwrap();
        let mut entry = sample_entry(RunKey::new(), clock.now());
        entry.workflow_task_timeout = Duration::ZERO;
        tracking.insert(entry.clone());
        release_tx.send(()).unwrap();
        let (actual, now) = rx.recv().await.unwrap();
        assert_eq!(actual, entry);
        assert!(evaluate_wft_timeout(&entry, now));
        assert!(now <= clock.now());
        cancel.cancel();
    };
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        tokio::join!(scanner, insert_during_scan);
    })
    .await
    .expect("an insert during scanning must reach the next pass");
}
