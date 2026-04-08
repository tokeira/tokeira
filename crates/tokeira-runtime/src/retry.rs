//! Activity retry evaluation and backoff computation.
//!
//! Pure functions that decide whether a failed activity should be retried
//! and how long to wait before the next attempt.

use time::Duration;
use tokeira_types::RetryPolicy;

/// Outcome of evaluating an activity retry policy.
#[derive(Clone, Debug, PartialEq)]
pub enum RetryDecision {
    /// The activity should be retried at `next_attempt`.
    Retry { next_attempt: u32 },
    /// All retry attempts have been exhausted (or the
    /// error is non-retryable).
    Exhausted,
}

/// Evaluate whether a failed activity should be retried.
///
/// Returns [`RetryDecision::Exhausted`] when the attempt
/// count has reached `maximum_attempts` or the error type
/// is listed in `non_retryable_error_types`.
pub fn evaluate_activity_retry(
    policy: &RetryPolicy,
    current_attempt: u32,
    failure_error_type: Option<&str>,
) -> RetryDecision {
    if let Some(error_type) = failure_error_type {
        if policy
            .non_retryable_error_types
            .iter()
            .any(|candidate| candidate == error_type)
        {
            return RetryDecision::Exhausted;
        }
    }

    if policy.maximum_attempts > 0 && current_attempt >= policy.maximum_attempts {
        return RetryDecision::Exhausted;
    }

    RetryDecision::Retry {
        next_attempt: current_attempt.saturating_add(1),
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
    let millis = (policy.initial_interval.whole_milliseconds() as f64)
        * coefficient.powi(exponent);
    let mut computed = Duration::milliseconds(millis.round() as i64);
    if let Some(maximum) = policy.maximum_interval {
        if computed > maximum {
            computed = maximum;
        }
    }
    computed
}
