//! Shared wake planning for volatile deadline indexes.
//!
//! A scan's wake time suppresses notifications for future inserts during work.
//! Revisions recover inserts that become overdue before the next plan. Deadline
//! indexes also remember unattempted work when a bounded pass stops.

use std::sync::{Arc, Mutex};

use time::{Duration, OffsetDateTime};
use tokeira_kernel::deadline_after;
use tokio::sync::Notify;

#[derive(Debug, Default)]
pub(crate) struct DeadlinePlan {
    // None only before planning; while scanning, the actual wake suppresses
    // notifications for future inserts which the next plan will already see.
    planned_wake: Option<OffsetDateTime>,
    scan_started_revision: Option<u64>,
}

impl DeadlinePlan {
    pub(crate) fn should_notify(&self, due: Option<OffsetDateTime>) -> bool {
        due.is_some_and(|due| self.planned_wake.is_none_or(|wake| due < wake))
    }

    pub(crate) fn start_scan(&mut self, now: OffsetDateTime, revision: u64) {
        self.planned_wake = Some(now);
        self.scan_started_revision = Some(revision);
    }

    pub(crate) fn remaining(
        &self,
        now: OffsetDateTime,
        due: OffsetDateTime,
        revision: u64,
    ) -> Option<std::time::Duration> {
        if due > now {
            std::time::Duration::try_from(due - now).ok()
        } else if self
            .scan_started_revision
            .is_some_and(|started| revision > started)
        {
            Some(std::time::Duration::ZERO)
        } else {
            None
        }
    }

    pub(crate) fn set_wake(&mut self, now: OffsetDateTime, delay: std::time::Duration) {
        self.planned_wake = Some(deadline_after(
            now,
            Duration::try_from(delay).unwrap_or(Duration::MAX),
        ));
    }
}

#[derive(Debug, Default)]
struct WakeState {
    plan: DeadlinePlan,
    revision: u64,
    earliest_changed: Option<OffsetDateTime>,
}

/// A wake shared by one scanner and its deadline indexes.
#[derive(Clone, Debug, Default)]
pub(crate) struct DeadlineWake {
    state: Arc<Mutex<WakeState>>,
    pub(crate) notify: Arc<Notify>,
}

impl DeadlineWake {
    /// Called under the index's mutation lock, after acquisition validation.
    pub(crate) fn changed(&self, due: Option<OffsetDateTime>) {
        let mut state = self.state.lock().expect("deadline wake lock poisoned");
        state.revision = state
            .revision
            .checked_add(1)
            .expect("deadline revision exhausted");
        state.earliest_changed = state.earliest_changed.into_iter().chain(due).min();
        if state.plan.should_notify(due) {
            self.notify.notify_one();
        }
    }

    pub(crate) fn start_scan(&self, now: OffsetDateTime) {
        let mut state = self.state.lock().expect("deadline wake lock poisoned");
        let revision = state.revision;
        state.plan.start_scan(now, revision);
    }

    pub(crate) fn begin_plan(&self) -> u64 {
        let mut state = self.state.lock().expect("deadline wake lock poisoned");
        state.earliest_changed = None;
        state.revision
    }

    /// Mutations serialize index-before-wake, so snapshots never hold this lock.
    /// Merge deadlines inserted while snapshots were read before publishing the
    /// sleep. Later inserts neither notify nor force a rescan; an insertion that
    /// has become overdue still forces an immediate pass.
    pub(crate) fn plan(
        &self,
        now: OffsetDateTime,
        revision: u64,
        delay: std::time::Duration,
    ) -> std::time::Duration {
        let mut state = self.state.lock().expect("deadline wake lock poisoned");
        let delay = if revision != state.revision {
            state.earliest_changed.map_or(delay, |due| {
                delay.min(std::time::Duration::try_from(due - now).unwrap_or_default())
            })
        } else {
            delay
        };
        state.plan.set_wake(now, delay);
        delay
    }
}

/// Periodic reconciliation remains independent of frequent deadline wakes.
#[derive(Debug)]
pub(crate) struct FullPass {
    interval: std::time::Duration,
    next: OffsetDateTime,
}

impl FullPass {
    /// Anchor reconciliation to the same clock used for the scanner's deadlines.
    pub(crate) fn new(interval: std::time::Duration, now: OffsetDateTime) -> Self {
        Self {
            interval,
            next: deadline_after(now, Duration::try_from(interval).unwrap_or(Duration::MAX)),
        }
    }
    pub(crate) fn remaining(&self, now: OffsetDateTime) -> std::time::Duration {
        std::time::Duration::try_from(self.next - now)
            .unwrap_or_default()
            .min(self.interval)
    }
    pub(crate) fn start(&mut self, now: OffsetDateTime) -> bool {
        if now < self.next {
            return false;
        }
        self.next = deadline_after(
            now,
            Duration::try_from(self.interval).unwrap_or(Duration::MAX),
        );
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_clock::TestClock;
    use tokeira_clock::Clock;

    #[tokio::test(start_paused = true)]
    async fn full_pass_follows_the_supplied_clock() {
        let clock = TestClock::new();
        let interval = std::time::Duration::from_secs(1);
        let half = interval / 2;
        let mut full = FullPass::new(interval, clock.now());
        assert_eq!(full.remaining(clock.now()), interval);
        tokio::time::advance(half).await;
        assert!(!full.start(clock.now()));
        assert_eq!(full.remaining(clock.now()), half);
        tokio::time::advance(half).await;
        assert!(full.start(clock.now()));
        assert_eq!(full.remaining(clock.now()), interval);
    }

    #[test]
    fn planning_merges_racing_deadlines_without_rescanning_later_inserts() {
        let now = OffsetDateTime::UNIX_EPOCH;
        for (offset, expected) in [(-1, 0), (2, 2), (20, 10)] {
            let wake = DeadlineWake::default();
            wake.start_scan(now);
            let revision = wake.begin_plan();
            wake.changed(Some(now + Duration::seconds(offset)));
            assert_eq!(
                wake.plan(now, revision, std::time::Duration::from_secs(10)),
                std::time::Duration::from_secs(expected)
            );
        }
    }
}
