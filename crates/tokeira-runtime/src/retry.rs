//! Activity retry evaluation and backoff computation.
//!
//! Pure functions that decide whether a failed activity should be retried
//! and how long to wait before the next attempt.

use time::Duration;
use tokeira_types::RetryPolicy;

/// Why a retry chain ended, mirroring the v1.31.0 `RetryState` derivation in
/// `nextBackoffInterval` / `RetryActivity` (`service/history/workflow/retry.go:70-113`,
/// `mutable_state_impl.go:6243-6270`). Carried so terminal resolutions can
/// report the correct `retry_state` instead of a collapsed generic value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetryExhaustedReason {
    /// The failure type is in the policy's non-retryable list (or the worker
    /// flagged the failure non-retryable).
    NonRetryableFailure,
    /// `maximum_attempts` was reached (`retry.go:96-98`).
    MaximumAttemptsReached,
    /// The next attempt could not begin before the retry-expiration deadline
    /// (schedule-to-close anchor; `retry.go:108-110`).
    Timeout,
}

/// Outcome of evaluating an activity retry policy.
#[derive(Clone, Debug, PartialEq)]
pub enum RetryDecision {
    /// The activity should be retried at `next_attempt` after `backoff`.
    Retry {
        next_attempt: u32,
        backoff: Duration,
    },
    /// The retry chain ends; `reason` maps to the terminal `RetryState`.
    Exhausted { reason: RetryExhaustedReason },
}

/// Evaluate whether a failed activity should be retried, mirroring
/// `nextBackoffInterval` (`retry.go:70-113 @ v1.31.0`): non-retryable type →
/// terminal; max attempts → terminal; next attempt past `expiration`
/// (first-schedule + schedule-to-close) → terminal Timeout; otherwise retry
/// after the exponential backoff.
pub fn evaluate_activity_retry(
    policy: &RetryPolicy,
    current_attempt: u32,
    failure_error_type: Option<&str>,
    now: time::OffsetDateTime,
    expiration: Option<time::OffsetDateTime>,
) -> RetryDecision {
    if let Some(error_type) = failure_error_type
        && policy
            .non_retryable_error_types
            .iter()
            .any(|candidate| candidate == error_type)
    {
        return RetryDecision::Exhausted {
            reason: RetryExhaustedReason::NonRetryableFailure,
        };
    }

    if policy.maximum_attempts > 0 && current_attempt >= policy.maximum_attempts {
        return RetryDecision::Exhausted {
            reason: RetryExhaustedReason::MaximumAttemptsReached,
        };
    }

    let backoff = compute_retry_backoff(policy, current_attempt);
    if let Some(expiration) = expiration
        && now + backoff >= expiration
    {
        return RetryDecision::Exhausted {
            reason: RetryExhaustedReason::Timeout,
        };
    }

    RetryDecision::Retry {
        next_attempt: current_attempt.saturating_add(1),
        backoff,
    }
}

/// Compute the backoff duration for a retry attempt
/// using exponential backoff with an optional cap.
pub fn compute_retry_backoff(policy: &RetryPolicy, attempt: u32) -> Duration {
    if policy.initial_interval.is_zero() {
        return Duration::ZERO;
    }

    let coefficient = policy.backoff_coefficient.max(1.0);
    let exponent = attempt.saturating_sub(1) as i32;
    let millis = (policy.initial_interval.whole_milliseconds() as f64) * coefficient.powi(exponent);
    let mut computed = Duration::milliseconds(millis.round() as i64);
    if let Some(maximum) = policy.maximum_interval
        && computed > maximum
    {
        computed = maximum;
    }
    computed
}
