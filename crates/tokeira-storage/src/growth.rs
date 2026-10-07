//! The checks both stores run on each commit against the run's growth limits
//! (`run-growth-limits` criteria 2.1-2.4, 2.6, 2.9 and 2.10).
//!
//! Each store measures what it stores and calls [`check_commit`] before its
//! first write, so a refused commit writes nothing. The runtime puts the limits
//! on the transitions it commits; a transition without them isn't checked.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use tokeira_kernel::{
    Transition,
    limits::{RunGrowthLimits, RunLimit, RunLimitExceeded},
};
use tokeira_types::{RunKey, TransitionSeq};

/// What a store measured for one commit.
#[derive(Clone, Copy, Debug)]
pub(crate) struct CommitMeasures {
    /// The run's History Size before this commit.
    pub prior_history_size: i64,
    /// The run's encoded state after this commit, less each activity's encoded
    /// input.
    pub measured_state_size: usize,
    /// The commit's encoded history batch; zero for a commit without events.
    pub batch_size: usize,
}

/// Check a commit against its growth limits in v1.31.0's order: the History
/// Size, the event count and the measured state for a commit that leaves an
/// existing run open (`context.go:406-463 @ v1.31.0`), then the history batch
/// for any commit (`common/persistence/history_manager.go:362-373 @
/// v1.31.0`). Returns the first breach.
pub(crate) fn check_commit(
    run_key: RunKey,
    transition: &Transition,
    measures: CommitMeasures,
) -> Result<(), RunLimitExceeded> {
    let Some(limits) = transition.growth_limits else {
        return Ok(());
    };
    // A run's first write, and a write that closes the run or continues it as
    // a new run, aren't checked (`context.go:465-481 @ v1.31.0`).
    if transition.expected_seq != TransitionSeq::ZERO && transition.next_state.status.is_open() {
        check_open_run(run_key, transition, &limits, &measures)?;
    }
    if measures.batch_size > limits.transaction_size {
        return Err(RunLimitExceeded::transaction_size(
            measures.batch_size,
            limits.transaction_size,
        ));
    }
    Ok(())
}

fn check_open_run(
    run_key: RunKey,
    transition: &Transition,
    limits: &RunGrowthLimits,
    measures: &CommitMeasures,
) -> Result<(), RunLimitExceeded> {
    // The stored size before this write, so the write that crosses the limit
    // succeeds and the next one is refused (`context.go:1019-1038`;
    // `common/persistence/execution_manager.go:149 @ v1.31.0`).
    check(
        run_key,
        RunLimit::HistorySize,
        usize::try_from(measures.prior_history_size).unwrap_or(0),
        limits.history_size_error,
        limits.history_size_warn,
    )?;
    // The events v1.31.0 numbers before it finishes the write
    // (`context.go:1057-1076 @ v1.31.0`).
    let count = transition
        .next_state
        .last_event_id
        .saturating_sub(i64::from(transition.events_numbered_at_close));
    check(
        run_key,
        RunLimit::HistoryCount,
        usize::try_from(count).unwrap_or(0),
        limits.history_count_error,
        limits.history_count_warn,
    )?;
    check(
        run_key,
        RunLimit::StateSize,
        measures.measured_state_size,
        limits.state_size_error,
        limits.state_size_warn,
    )
}

/// Refuse a value over its error limit, and log one over its warn limit.
fn check(
    run_key: RunKey,
    limit: RunLimit,
    value: usize,
    error: usize,
    warn: usize,
) -> Result<(), RunLimitExceeded> {
    if value > error {
        return Err(RunLimitExceeded::of(limit));
    }
    if value > warn {
        warn_throttled(run_key, limit, value, warn);
    }
    Ok(())
}

/// v1.31.0 logs a write over a warn limit through its throttled logger
/// (`context.go:1033, 1071, 1108 @ v1.31.0`); this logs at most once a second
/// for each limit.
fn warn_throttled(run_key: RunKey, limit: RunLimit, value: usize, warn: usize) {
    static LAST_LOGGED: [AtomicU64; 3] = [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)];
    let slot = match limit {
        RunLimit::HistorySize => &LAST_LOGGED[0],
        RunLimit::HistoryCount => &LAST_LOGGED[1],
        RunLimit::StateSize | RunLimit::TransactionSize => &LAST_LOGGED[2],
    };
    let second = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs());
    let last = slot.load(Ordering::Relaxed);
    if last != second
        && slot
            .compare_exchange(last, second, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    {
        tracing::warn!(
            run_key = %run_key.0,
            limit = ?limit,
            value,
            warn_limit = warn,
            "run growth exceeds its warn limit"
        );
    }
}
