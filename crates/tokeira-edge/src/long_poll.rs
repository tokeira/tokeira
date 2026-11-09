//! Long-poll admission control.
//!
//! Long polls (workflow/activity/Nexus task polls, history waits) are a transport
//! concern: they tie up sockets, memory, and scheduler slots while the caller
//! blocks for work. The danger is letting that pressure reach down into the
//! authoritative planes — a blocked poll must **not** pin durable runtime state or
//! hold a DSQL session open. [`LongPollGate`] enforces that boundary with a
//! semaphore bounding concurrent long polls *before* the request reaches the
//! broker/runtime; admission beyond the bound fails fast with
//! [`EdgeError::LongPollAdmissionTimeout`]
//! rather than queueing unboundedly. The complementary "return empty just before
//! the caller's deadline" budget is computed here (`long_poll_budget`,
//! `task_poll_deadline`) and applied at each poll call site.

use std::{sync::Arc, time::Duration};

use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{Instant, timeout},
};

use crate::errors::{EdgeError, EdgeResult};

/// Longest a task poll waits for work: `matching.longPollExpirationInterval`'s one-minute
/// default (`common/dynamicconfig/constants.go @ v1.31.0`), which bounds the poll context
/// in `pollTask` (`service/matching/matching_engine.go @ v1.31.0`). Tokeira maps no
/// override for that key, so the default is the contract.
pub(crate) const TASK_POLL_TIMEOUT: Duration = Duration::from_secs(60);

/// Time a task poll leaves before the caller's deadline to answer empty instead of letting
/// that deadline fire. v1.31.0 applies the same second twice: the frontend answers empty
/// at once when less than this remains (`longPollTailRoom`,
/// `service/frontend/workflow_handler.go @ v1.31.0`), and matching ends the wait this long
/// before the deadline (`returnEmptyTaskTimeBudget`,
/// `service/matching/physical_task_queue_manager.go @ v1.31.0`).
pub(crate) const TASK_POLL_TAIL_ROOM: Duration = Duration::from_secs(1);

/// A long poll's wait: `Min(caller_remaining - buffer, timeout)`, floored at zero, or the
/// full `timeout` when the caller sent no deadline. This is v1.31.0's
/// `contextutil.WithDeadlineBuffer` (`common/contextutil/deadline.go @ v1.31.0`): with no
/// parent deadline the buffer is not reserved, because nothing will fire after the wait.
pub(crate) fn long_poll_budget(
    caller_remaining: Option<Duration>,
    timeout: Duration,
    buffer: Duration,
) -> Duration {
    match caller_remaining {
        Some(remaining) => remaining.saturating_sub(buffer).min(timeout),
        None => timeout,
    }
}

/// The instant a task poll stops waiting, or `None` when it must answer empty at once.
///
/// With a caller deadline the wait ends [`TASK_POLL_TAIL_ROOM`] before it, capped at
/// `timeout`; with none, after `timeout`. When no more than the tail room remains the
/// poll answers empty without waiting — v1.31.0's frontend returns an empty response
/// once `now + longPollTailRoom` passes the deadline, and matching's buffered wait is zero
/// at exactly one second, so both boundaries answer empty
/// (`service/frontend/workflow_handler.go`, `contextNearDeadline` @ v1.31.0). An empty
/// answer is a success the SDK re-polls at once; an expired deadline is an error it backs
/// off from.
pub(crate) fn task_poll_deadline(
    caller_deadline: Option<Instant>,
    now: Instant,
    timeout: Duration,
) -> Option<Instant> {
    let remaining = caller_deadline.map(|deadline| deadline.saturating_duration_since(now));
    if remaining.is_some_and(|remaining| remaining <= TASK_POLL_TAIL_ROOM) {
        return None;
    }
    Some(now + long_poll_budget(remaining, timeout, TASK_POLL_TAIL_ROOM))
}

/// Admission control for long-poll requests.
///
/// Long polls are primarily an edge concern: they tie up sockets, memory, and
/// scheduler slots. They should *not* cause the runtime to pin durable state or
/// the storage layer to pin DSQL sessions.
///
/// This gate therefore limits concurrency before the request reaches the broker/runtime.
#[derive(Clone, Debug)]
pub struct LongPollConfig {
    pub max_concurrent: usize,
    pub acquire_timeout: Duration,
}

impl Default for LongPollConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 10_000,
            acquire_timeout: Duration::from_millis(250),
        }
    }
}

#[derive(Clone, Debug)]
pub struct LongPollGate {
    sem: Arc<Semaphore>,
    config: LongPollConfig,
}

#[derive(Debug)]
pub struct LongPollPermit {
    _permit: OwnedSemaphorePermit,
}

impl LongPollGate {
    pub fn new(config: LongPollConfig) -> Self {
        Self {
            sem: Arc::new(Semaphore::new(config.max_concurrent)),
            config,
        }
    }

    pub fn available_permits(&self) -> usize {
        self.sem.available_permits()
    }

    pub async fn acquire(&self) -> EdgeResult<LongPollPermit> {
        let sem = self.sem.clone();

        let permit = timeout(self.config.acquire_timeout, sem.acquire_owned())
            .await
            .map_err(|_| EdgeError::LongPollAdmissionTimeout)?
            .map_err(|_| EdgeError::TooManyLongPolls)?;

        Ok(LongPollPermit { _permit: permit })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deadline_after(now: Instant, millis: u64) -> Option<Instant> {
        Some(now + Duration::from_millis(millis))
    }

    #[test]
    fn far_caller_deadline_is_capped_at_the_long_poll_timeout() {
        let now = Instant::now();
        assert_eq!(
            task_poll_deadline(deadline_after(now, 600_000), now, TASK_POLL_TIMEOUT),
            Some(now + TASK_POLL_TIMEOUT)
        );
    }

    #[test]
    fn near_caller_deadline_waits_until_one_second_before_it() {
        let now = Instant::now();
        assert_eq!(
            task_poll_deadline(deadline_after(now, 5_000), now, TASK_POLL_TIMEOUT),
            Some(now + Duration::from_secs(4))
        );
        assert_eq!(
            task_poll_deadline(deadline_after(now, 1_001), now, TASK_POLL_TIMEOUT),
            Some(now + Duration::from_millis(1))
        );
    }

    #[test]
    fn caller_deadline_within_the_tail_room_answers_empty_at_once() {
        let now = Instant::now();
        for millis in [0, 500, 999, 1_000] {
            assert_eq!(
                task_poll_deadline(deadline_after(now, millis), now, TASK_POLL_TIMEOUT),
                None,
                "{millis} ms left"
            );
        }
        let passed = now.checked_sub(Duration::from_secs(1));
        assert_eq!(task_poll_deadline(passed, now, TASK_POLL_TIMEOUT), None);
    }

    #[test]
    fn no_caller_deadline_waits_the_full_long_poll_timeout() {
        let now = Instant::now();
        assert_eq!(
            task_poll_deadline(None, now, TASK_POLL_TIMEOUT),
            Some(now + Duration::from_secs(60))
        );
        // An explicit zero wait still polls once rather than answering empty unseen.
        assert_eq!(task_poll_deadline(None, now, Duration::ZERO), Some(now));
    }
}
