//! Supplied-clock deadline and notification oracles for the shared activity wake.

use std::{future::Future, task::Poll};

use proptest::prelude::*;
use tokeira_clock::Clock;
use tokeira_types::ShardEpoch;

use super::{tests::sample_activity, *};
use crate::test_clock::TestClock;

fn entry(now: OffsetDateTime) -> ActivityTrackingEntry {
    ActivityTrackingEntry {
        timeouts: Some(ActivityTimeouts::default()),
        run_key: RunKey::new(),
        shard_id: ShardId(0),
        activity_id: "a".into(),
        original_scheduled_at: now,
        last_dispatched_at: now,
        started_at: None,
        last_heartbeat_at: None,
        cancel_requested: false,
    }
}

#[test]
fn due_time_matches_each_strict_timeout_and_later_heartbeat() {
    let now = OffsetDateTime::UNIX_EPOCH;
    for kind in 0..4 {
        for heartbeat_offset in [-5, 0, 5] {
            let mut activity = sample_activity();
            let timeout = Some(Duration::seconds(10));
            match kind {
                0 => activity.schedule_to_close_timeout = timeout,
                1 => activity.schedule_to_start_timeout = timeout,
                2 => activity.start_to_close_timeout = timeout,
                _ => activity.heartbeat_timeout = timeout,
            }
            let mut tracked = entry(now);
            if kind >= 2 {
                tracked.started_at = Some(now);
            }
            tracked.last_heartbeat_at = Some(now + Duration::seconds(heartbeat_offset));
            tracked.timeouts = Some(ActivityTimeouts::from(&activity));
            let due = activity_timeout_due_at(&tracked).unwrap();
            assert_eq!(
                due,
                now + Duration::seconds(
                    10 + if kind == 3 {
                        heartbeat_offset.max(0)
                    } else {
                        0
                    }
                ) + Duration::nanoseconds(1)
            );
            assert!(
                evaluate_activity_timeout(&tracked, &activity, due - Duration::nanoseconds(1))
                    .is_none()
            );
            assert!(evaluate_activity_timeout(&tracked, &activity, due).is_some());
        }
    }
    let mut tracked = entry(now);
    assert_eq!(activity_timeout_due_at(&tracked), None);
    tracked.timeouts.as_mut().unwrap().schedule_to_close = Some(Duration::ZERO);
    assert_eq!(activity_timeout_due_at(&tracked), None);
    tracked.timeouts.as_mut().unwrap().schedule_to_close = Some(Duration::seconds(1));
    tracked.timeouts.as_mut().unwrap().paused = true;
    assert_eq!(activity_timeout_due_at(&tracked), None);
    tracked.timeouts = None;
    assert!(activity_timeout_due_at(&tracked).unwrap() <= now);
    tracked.timeouts = Some(ActivityTimeouts {
        schedule_to_close: Some(Duration::MAX),
        ..Default::default()
    });
    tracked.original_scheduled_at = time::PrimitiveDateTime::MAX.assume_utc();
    assert_eq!(activity_timeout_due_at(&tracked), None);
}

#[test]
fn planning_excludes_inactive_and_attempted_overdue_entries() {
    let now = OffsetDateTime::UNIX_EPOCH;
    let interval = std::time::Duration::from_secs(10);
    let tracking = ActivityTrackingState::default();
    let owner = Arc::new(RwLock::new(ShardOwner::new(1)));
    owner
        .write()
        .unwrap()
        .record_acquired(ShardId(0), ShardEpoch(1));
    tracking.set_owner(owner.clone());
    let mut tracked = entry(now - Duration::seconds(2));
    tracked.timeouts.as_mut().unwrap().schedule_to_close = Some(Duration::seconds(1));
    tracking.insert(tracked.clone());
    assert_eq!(tracking.scan_delay(now, interval), interval);
    owner.write().unwrap().mark_active(ShardId(0));
    tracking.wake.start_scan(now);
    assert_eq!(
        tracking.scan_delay(now, interval),
        std::time::Duration::ZERO
    );
    let submitted = tracking.inner.snapshot(None).pop().unwrap();
    tracking
        .inner
        .mark_attempted(&(tracked.run_key, tracked.activity_id.clone()), &submitted);
    assert_eq!(tracking.scan_delay(now, interval), interval);
    tracked.activity_id = "new overdue".into();
    tracking.insert(tracked);
    assert_eq!(
        tracking.scan_delay(now, interval),
        std::time::Duration::ZERO
    );
}

async fn notified(tracking: &ActivityTrackingState) -> bool {
    let future = tracking.wake.notify.notified();
    tokio::pin!(future);
    std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx).is_ready())).await
}

#[tokio::test]
async fn creating_and_reanchoring_notify_only_for_earlier_deadlines() {
    let now = OffsetDateTime::UNIX_EPOCH;
    for operation in 0..6 {
        for earlier in [false, true] {
            let tracking = ActivityTrackingState::default();
            let mut tracked = entry(now);
            tracked.timeouts = Some(ActivityTimeouts {
                schedule_to_start: Some(Duration::seconds(30)),
                start_to_close: Some(Duration::seconds(30)),
                heartbeat: Some(Duration::seconds(30)),
                ..Default::default()
            });
            tracking.insert(tracked.clone());
            assert!(notified(&tracking).await);
            let interval = std::time::Duration::from_secs(10);
            assert_eq!(tracking.scan_delay(now, interval), interval);
            let anchor = now + Duration::seconds(if earlier { -25 } else { 25 });
            match operation {
                0 => {
                    tracked.last_dispatched_at = anchor;
                    tracking.insert(tracked);
                }
                1 => tracking.record_scheduled_with_timeouts(
                    tracked.run_key,
                    tracked.shard_id,
                    "a",
                    anchor,
                    tracked.timeouts,
                ),
                2 => tracking.record_started(tracked.run_key, "a", anchor),
                3 => tracking.record_retry(tracked.run_key, "a", anchor),
                4 => tracking.set_timeouts(
                    tracked.run_key,
                    "a",
                    ActivityTimeouts {
                        schedule_to_start: Some(Duration::seconds(if earlier { 1 } else { 60 })),
                        ..Default::default()
                    },
                ),
                _ => {
                    tracking.record_started(tracked.run_key, "a", now);
                    assert!(!notified(&tracking).await);
                    tracking.record_heartbeat(tracked.run_key, "a", anchor);
                }
            }
            assert_eq!(notified(&tracking).await, earlier && operation != 5);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn future_insert_during_scan_is_quiet_but_overdue_is_not_lost() {
    let clock = TestClock::new();
    let now = clock.now();
    let tracking = ActivityTrackingState::default();
    tracking.wake.start_scan(now);
    let mut tracked = entry(now);
    tracked.timeouts.as_mut().unwrap().schedule_to_start = Some(Duration::seconds(1));
    tracking.insert(tracked);
    assert!(!notified(&tracking).await);
    tokio::time::advance(std::time::Duration::from_secs(2)).await;
    assert_eq!(
        tracking.scan_delay(clock.now(), std::time::Duration::from_secs(10)),
        std::time::Duration::ZERO
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: runtime-activity-timeouts, Property 17
    // A wake is bounded by the full-pass interval and every active future deadline.
    #[test]
    fn sleep_is_bounded_by_interval_and_active_future_deadlines(offsets in prop::collection::vec(1i64..100_000, 0..30), interval_ms in 1u64..100_000) {
        let now = OffsetDateTime::UNIX_EPOCH;
        let tracking = ActivityTrackingState::default();
        let mut expected = std::time::Duration::from_millis(interval_ms);
        for offset in offsets {
            let mut tracked = entry(now);
            tracked.timeouts.as_mut().unwrap().schedule_to_start = Some(Duration::milliseconds(offset));
            tracking.insert(tracked);
            expected = expected.min(std::time::Duration::from_millis(offset as u64) + std::time::Duration::from_nanos(1));
        }
        prop_assert_eq!(tracking.scan_delay(now, std::time::Duration::from_millis(interval_ms)), expected);
    }
}
