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
                activity.started_at = Some(now);
            }
            activity.current_attempt_scheduled_at = Some(now);
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

#[test]
fn evaluation_uses_reloaded_attempt_anchors() {
    let base = OffsetDateTime::UNIX_EPOCH;
    for started in [false, true] {
        for cached_start in [None, Some(base)] {
            let mut cached = entry(base);
            cached.started_at = cached_start;
            cached.last_heartbeat_at = Some(base + Duration::seconds(5));
            let mut activity = sample_activity();
            activity.current_attempt_scheduled_at = Some(base + Duration::seconds(40));
            activity.started_at = started.then_some(base + Duration::seconds(50));
            activity.schedule_to_start_timeout = Some(Duration::seconds(20));
            activity.start_to_close_timeout = Some(Duration::seconds(20));
            activity.heartbeat_timeout = Some(Duration::seconds(10));
            let now = base + Duration::seconds(55);
            assert_eq!(
                evaluate_activity_timeout(&cached, &activity, now),
                None,
                "cached anchors must not authorize a timeout for the reloaded attempt"
            );
            assert_eq!(
                evaluate_activity_timeout(&cached, &activity, base + Duration::seconds(60)),
                None
            );
            assert_eq!(
                evaluate_activity_timeout(
                    &cached,
                    &activity,
                    base + Duration::seconds(60) + Duration::nanoseconds(1)
                ),
                Some(if started {
                    TimeoutViolation::Heartbeat
                } else {
                    TimeoutViolation::ScheduleToStart
                })
            );
            if started {
                cached.last_heartbeat_at = Some(base + Duration::seconds(58));
                activity.last_heartbeat_at = Some(base + Duration::seconds(59));
                assert_eq!(
                    evaluate_activity_timeout(&cached, &activity, base + Duration::seconds(69)),
                    None
                );
                cached.last_heartbeat_at = Some(base + Duration::seconds(60));
                assert_eq!(
                    evaluate_activity_timeout(&cached, &activity, base + Duration::seconds(70)),
                    None
                );
                assert_eq!(
                    evaluate_activity_timeout(
                        &cached,
                        &activity,
                        base + Duration::seconds(70) + Duration::nanoseconds(1)
                    ),
                    Some(TimeoutViolation::Heartbeat)
                );
            }
        }
    }
}

#[test]
fn reload_replaces_superseded_attempt_cache() {
    let base = OffsetDateTime::UNIX_EPOCH;
    let mut cached = entry(base);
    cached.started_at = Some(base);
    cached.last_heartbeat_at = Some(base + Duration::seconds(10));
    let mut activity = sample_activity();
    activity.current_attempt_scheduled_at = Some(base + Duration::seconds(20));
    activity.last_heartbeat_at = cached.last_heartbeat_at;
    activity.schedule_to_start_timeout = Some(Duration::seconds(5));
    cached.refresh_from_activity(&activity);
    assert_eq!(cached.started_at, None);
    assert_eq!(cached.last_heartbeat_at, None);
    assert_eq!(
        Some(cached.last_dispatched_at),
        activity.current_attempt_scheduled_at
    );
    assert_eq!(
        activity_timeout_due_at(&cached),
        Some(base + Duration::seconds(25) + Duration::nanoseconds(1))
    );
    activity.started_at = Some(base + Duration::seconds(21));
    activity.heartbeat_timeout = Some(Duration::seconds(1));
    activity.last_heartbeat_at = Some(base + Duration::seconds(22));
    cached.refresh_from_activity(&activity);
    assert_eq!(cached.started_at, activity.started_at);
    assert_eq!(cached.last_heartbeat_at, activity.last_heartbeat_at);
    cached.last_heartbeat_at = Some(base + Duration::seconds(23));
    cached.refresh_from_activity(&activity);
    assert_eq!(cached.last_heartbeat_at, Some(base + Duration::seconds(23)));

    let tracking = ActivityTrackingState::default();
    let key = (cached.run_key, cached.activity_id.clone());
    tracking.insert(cached.clone());
    let captured = tracking.inner.snapshot(None).pop().unwrap();
    // A heartbeat lands while the scanner's read is in flight. The reload of
    // the same start still applies, and keeps the later cached heartbeat.
    tracking.record_heartbeat(key.0, &key.1, base + Duration::seconds(24));
    tracking
        .inner
        .refresh_scanned_if(&key, &captured, base, |tracked| {
            tracked.adopt(TransitionSeq(5), &activity)
        });
    let reloaded = tracking.inner.get(&key).unwrap();
    assert_eq!(reloaded.image, Some(TransitionSeq(5)));
    assert_eq!(reloaded.entry.started_at, activity.started_at);
    assert_eq!(
        reloaded.entry.last_heartbeat_at,
        Some(base + Duration::seconds(24))
    );
    // A read of an earlier image, here the preceding attempt's, is refused.
    let mut earlier = activity.clone();
    earlier.started_at = None;
    earlier.last_heartbeat_at = None;
    tracking
        .inner
        .refresh_scanned_if(&key, &captured, base, |tracked| {
            tracked.adopt(TransitionSeq(4), &earlier)
        });
    assert_eq!(tracking.inner.get(&key).unwrap(), reloaded);
}
