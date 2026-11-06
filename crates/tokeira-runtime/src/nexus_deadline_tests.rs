//! Supplied-clock deadline, acquisition and wake oracles for Nexus and callbacks.

use std::{future::Future, task::Poll};

use proptest::prelude::*;
use tokeira_types::ShardEpoch;

use super::*;

fn operation(now: OffsetDateTime) -> PendingNexusOperation {
    PendingNexusOperation {
        operation_id: "operation".into(),
        scheduled_event_id: 5,
        endpoint: String::new(),
        service: String::new(),
        operation: String::new(),
        schedule_to_close_timeout: None,
        schedule_to_start_timeout: None,
        start_to_close_timeout: None,
        scheduled_at: now,
        started: false,
        started_at: None,
        attempt: 0,
        last_attempt_failure: None,
        next_attempt_at: None,
        operation_token: String::new(),
        input: Payloads::default(),
        cancellation: None,
    }
}

fn entry(op: &PendingNexusOperation) -> NexusTimeoutEntry {
    NexusTimeoutEntry {
        run_key: RunKey::new(),
        shard_id: ShardId(0),
        operation_id: op.operation_id.clone(),
        scheduled_event_id: op.scheduled_event_id,
        scheduled_at: op.scheduled_at,
        timeouts: Some(NexusTimeouts::from(op)),
    }
}

#[test]
fn nexus_due_matches_inclusive_evaluator_and_disabled_unknown_overflow() {
    let now = OffsetDateTime::UNIX_EPOCH;
    for kind in 0..3 {
        let mut op = operation(now);
        match kind {
            0 => op.schedule_to_close_timeout = Some(Duration::seconds(5)),
            1 => op.schedule_to_start_timeout = Some(Duration::seconds(5)),
            _ => {
                op.started = true;
                op.started_at = Some(now);
                op.start_to_close_timeout = Some(Duration::seconds(5));
            }
        }
        let due = nexus_timeout_due_at(&entry(&op)).unwrap();
        assert_eq!(due, now + Duration::seconds(5));
        assert!(evaluate_nexus_timeout(&op, due - Duration::NANOSECOND).is_none());
        assert!(evaluate_nexus_timeout(&op, due).is_some());
        if kind == 1 {
            op.started = true;
            assert_eq!(nexus_timeout_due_at(&entry(&op)), None);
        }
    }
    let mut op = operation(now);
    assert_eq!(nexus_timeout_due_at(&entry(&op)), None);
    op.schedule_to_close_timeout = Some(Duration::ZERO);
    assert_eq!(nexus_timeout_due_at(&entry(&op)), None);
    let mut unknown = entry(&op);
    unknown.timeouts = None;
    assert!(nexus_timeout_due_at(&unknown).unwrap() <= now);
    op.scheduled_at = time::PrimitiveDateTime::MAX.assume_utc();
    op.schedule_to_close_timeout = Some(Duration::MAX);
    assert_eq!(nexus_timeout_due_at(&entry(&op)), Some(op.scheduled_at));
    assert!(evaluate_nexus_timeout(&op, op.scheduled_at).is_some());
}

#[test]
fn callback_due_follows_lifecycle() {
    let now = OffsetDateTime::UNIX_EPOCH;
    let mut entry = CompletionCallbackTrackingEntry {
        run_key: RunKey::new(),
        shard_id: ShardId(0),
        callback_index: 0,
        timing: None,
    };
    assert!(callback_due_at(&entry).unwrap() <= now);
    for state in [
        tokeira_kernel::CallbackState::Scheduled,
        tokeira_kernel::CallbackState::BackingOff,
        tokeira_kernel::CallbackState::Standby,
        tokeira_kernel::CallbackState::Blocked,
        tokeira_kernel::CallbackState::Failed,
        tokeira_kernel::CallbackState::Succeeded,
    ] {
        entry.timing = Some(CompletionCallbackTiming {
            state: state.clone(),
            next_attempt_at: Some(now),
        });
        match state {
            tokeira_kernel::CallbackState::BackingOff => {
                assert_eq!(callback_due_at(&entry), Some(now))
            }
            _ => assert_eq!(callback_due_at(&entry), None),
        }
    }
    entry.timing = Some(CompletionCallbackTiming {
        state: tokeira_kernel::CallbackState::BackingOff,
        next_attempt_at: None,
    });
    assert_eq!(callback_due_at(&entry), None);
    entry.timing.as_mut().unwrap().next_attempt_at =
        Some(time::PrimitiveDateTime::MAX.assume_utc());
    assert_eq!(
        callback_due_at(&entry),
        Some(time::PrimitiveDateTime::MAX.assume_utc())
    );
}

async fn notified(wake: &crate::deadline_wake::DeadlineWake) -> bool {
    let future = wake.notify.notified();
    tokio::pin!(future);
    std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx).is_ready())).await
}

#[tokio::test]
async fn nexus_and_callback_notify_only_earlier_deadlines() {
    let now = OffsetDateTime::UNIX_EPOCH;
    for earlier in [false, true] {
        let nexus = NexusTimeoutTrackingState::default();
        let callbacks = CompletionCallbackTrackingState::default();
        for wake in [&nexus.wake, &callbacks.wake] {
            let revision = wake.begin_plan();
            wake.plan(now, revision, std::time::Duration::from_secs(10));
        }
        let due = now + Duration::seconds(if earlier { 2 } else { 20 });
        let mut op = operation(now);
        op.schedule_to_close_timeout = Some(due - now);
        nexus.insert(entry(&op));
        callbacks.insert(CompletionCallbackTrackingEntry {
            run_key: RunKey::new(),
            shard_id: ShardId(0),
            callback_index: 0,
            timing: Some(CompletionCallbackTiming {
                state: tokeira_kernel::CallbackState::BackingOff,
                next_attempt_at: Some(due),
            }),
        });
        assert_eq!(notified(&nexus.wake).await, earlier);
        assert_eq!(notified(&callbacks.wake).await, earlier);
    }
}

#[test]
fn nexus_and_callback_planning_excludes_inactive_and_attempted_overdue() {
    let now = OffsetDateTime::UNIX_EPOCH;
    let interval = std::time::Duration::from_secs(10);
    let nexus = NexusTimeoutTrackingState::default();
    let callbacks = CompletionCallbackTrackingState::default();
    let owner = Arc::new(RwLock::new(ShardOwner::new(1)));
    owner
        .write()
        .unwrap()
        .record_acquired(ShardId(0), ShardEpoch(1));
    nexus.set_owner(owner.clone());
    callbacks.set_owner(owner.clone());
    let mut op = operation(now - Duration::seconds(2));
    op.schedule_to_close_timeout = Some(Duration::seconds(1));
    let nexus_entry = entry(&op);
    nexus.insert(nexus_entry.clone());
    let callback_entry = CompletionCallbackTrackingEntry {
        run_key: nexus_entry.run_key,
        shard_id: ShardId(0),
        callback_index: 0,
        timing: None,
    };
    callbacks.insert(callback_entry.clone());
    assert_eq!(nexus.inner.deadline_delay(now, interval), interval);
    assert_eq!(callbacks.inner.deadline_delay(now, interval), interval);
    owner.write().unwrap().mark_active(ShardId(0));
    assert_eq!(
        nexus.inner.deadline_delay(now, interval),
        std::time::Duration::ZERO
    );
    assert_eq!(
        callbacks.inner.deadline_delay(now, interval),
        std::time::Duration::ZERO
    );
    let key = (nexus_entry.run_key, nexus_entry.operation_id.clone());
    nexus
        .inner
        .mark_attempted(&key, &nexus.inner.tracked(&key).unwrap());
    let key = (callback_entry.run_key, 0);
    callbacks
        .inner
        .mark_attempted(&key, &callbacks.inner.tracked(&key).unwrap());
    assert_eq!(nexus.inner.deadline_delay(now, interval), interval);
    assert_eq!(callbacks.inner.deadline_delay(now, interval), interval);
    nexus.wake.start_scan(now);
    callbacks.wake.start_scan(now);
    nexus.insert(nexus_entry);
    callbacks.insert(callback_entry);
    assert_eq!(
        nexus.inner.deadline_delay(now, interval),
        std::time::Duration::ZERO
    );
    assert_eq!(
        callbacks.inner.deadline_delay(now, interval),
        std::time::Duration::ZERO
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Active future deadlines bound the sleep independently of the periodic backstop.
    // Feature: runtime-nexus-dispatch, Property 11; nexus-async-completion, Deadline wake
    #[test]
    fn nexus_and_callback_sleep_is_bounded(delays in prop::collection::vec(1i64..100_000, 1..60), maximum in 1u64..100_000) {
        let now = OffsetDateTime::UNIX_EPOCH;
        let nexus = NexusTimeoutTrackingState::default(); let callbacks = CompletionCallbackTrackingState::default();
        for delay in &delays {
            let mut op = operation(now); op.schedule_to_close_timeout = Some(Duration::milliseconds(*delay));
            nexus.insert(entry(&op));
            callbacks.insert(CompletionCallbackTrackingEntry { run_key: RunKey::new(), shard_id: ShardId(0), callback_index: 0, timing: Some(CompletionCallbackTiming { state: tokeira_kernel::CallbackState::BackingOff, next_attempt_at: Some(now + Duration::milliseconds(*delay)) }) });
        }
        let interval = std::time::Duration::from_millis(maximum);
        let expected = interval.min(std::time::Duration::from_millis(*delays.iter().min().unwrap() as u64));
        prop_assert_eq!(nexus.inner.deadline_delay(now, interval), expected);
        prop_assert_eq!(callbacks.inner.deadline_delay(now, interval), expected);
    }
}
