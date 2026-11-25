//! Workflow retry-policy defaults and activity retry evaluation.
//!
//! Workflow admission shares defaults with schedule firing so persisted policies
//! agree across entry points. Activity evaluation decides whether to retry and
//! how long to wait; it does not use the workflow defaulting function.

use time::Duration;
use tokeira_types::RetryPolicy;

/// Fill unset fields of a supplied workflow policy before creating a run.
///
/// Callers must preserve `None` policies. Zero attempts remain unlimited, and
/// explicit nonzero fields are retained. Admission validates wire durations;
/// this function does not validate policies or decide whether a failure retries.
/// Defaults follow common/retrypolicy/retry_policy.go:32-56 @ v1.31.0. The derived
/// maximum retains the admission cap (common/primitives/timestamp/duration.go:10-12,82-85
/// @ v1.31.0), matching the workflow start translator.
///
/// Schedules retain unset fields at create/update and default a copy at firing:
/// service/frontend/workflow_handler.go:3812-3861,4528-4532;
/// service/worker/scheduler/activities.go:67-74 @ v1.31.0.
pub fn workflow_retry_policy_with_defaults(mut policy: RetryPolicy) -> RetryPolicy {
    if policy.initial_interval.is_zero() {
        policy.initial_interval = Duration::seconds(1);
    }
    if policy.backoff_coefficient == 0.0 {
        policy.backoff_coefficient = 2.0;
    }
    if policy
        .maximum_interval
        .is_none_or(|interval| interval.is_zero())
    {
        policy.maximum_interval = Some(
            policy
                .initial_interval
                .saturating_mul(100)
                .min(Duration::seconds(3_153_600_000)),
        );
    }
    policy
}

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
    /// The next attempt would begin strictly after the retry-expiration deadline
    /// (schedule-to-close anchor; `service/history/workflow/retry.go:109 @ v1.31.0`).
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
    failure_non_retryable: bool,
    now: time::OffsetDateTime,
    expiration: Option<time::OffsetDateTime>,
) -> RetryDecision {
    // A worker-flagged non-retryable failure never retries, regardless of the
    // policy's non-retryable list (`isRetryable` checks
    // `ApplicationFailureInfo.NonRetryable` before the type list,
    // retry.go:139-147 @ v1.31.0).
    if failure_non_retryable {
        return RetryDecision::Exhausted {
            reason: RetryExhaustedReason::NonRetryableFailure,
        };
    }
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
    // Equality permits retry (service/history/workflow/retry.go:109 @ v1.31.0).
    // Do not saturate this comparison: overflow past the latest stored date
    // is still after expiration, not equality with that date.
    if let Some(expiration) = expiration
        && now
            .checked_add(backoff)
            .is_none_or(|next| next > expiration)
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
    let exponent = i32::try_from(attempt.saturating_sub(1)).unwrap_or(i32::MAX);
    let millis = (policy.initial_interval.whole_milliseconds() as f64) * coefficient.powi(exponent);
    let mut computed = Duration::milliseconds(millis.round() as i64);
    if let Some(maximum) = policy.maximum_interval
        && computed > maximum
    {
        computed = maximum;
    }
    computed
}

#[cfg(test)]
mod duration_tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn activity_retry_expiration_is_inclusive() {
        let now = time::OffsetDateTime::UNIX_EPOCH;
        let backoff = Duration::seconds(1);
        let policy = RetryPolicy {
            initial_interval: Duration::seconds(2),
            backoff_coefficient: 1.0,
            maximum_interval: Some(backoff),
            maximum_attempts: 0,
            non_retryable_error_types: Vec::new(),
        };
        let retry = RetryDecision::Retry {
            next_attempt: 2,
            backoff,
        };
        let exhausted = RetryDecision::Exhausted {
            reason: RetryExhaustedReason::Timeout,
        };
        let latest = time::PrimitiveDateTime::MAX.assume_utc();
        for (case, now, expiration, expected) in [
            // service/history/workflow/retry.go:105-109 @ v1.31.0: cap before comparing; equality retries.
            (
                "exactly at expiration",
                now,
                Some(now + backoff),
                retry.clone(),
            ),
            // service/history/workflow/retry.go:109 @ v1.31.0: one nanosecond after expires.
            (
                "one nanosecond after",
                now,
                Some(now + backoff - Duration::nanoseconds(1)),
                exhausted.clone(),
            ),
            // service/history/workflow/retry.go:109 @ v1.31.0: one nanosecond before retries.
            (
                "one nanosecond before",
                now,
                Some(now + backoff + Duration::nanoseconds(1)),
                retry.clone(),
            ),
            // service/history/workflow/retry.go:109 @ v1.31.0: nil expiration imposes no time limit.
            ("no expiration", now, None, retry.clone()),
            // service/history/workflow/retry.go:109 @ v1.31.0: equality also retries at the latest stored date.
            (
                "exactly at latest date",
                latest - backoff,
                Some(latest),
                retry,
            ),
            // service/history/workflow/retry.go:109 @ v1.31.0: saturation must not turn a later time into equality.
            (
                "beyond latest date",
                latest - backoff + Duration::nanoseconds(1),
                Some(latest),
                exhausted,
            ),
        ] {
            assert_eq!(
                evaluate_activity_retry(&policy, 1, None, false, now, expiration),
                expected,
                "{case}"
            );
        }
    }

    proptest! {
        // Feature: runtime-activity-timeouts, Property 16: stored retry inputs never overflow.
        #[test]
        fn stored_retry_backoff_is_total(
            seconds in any::<i64>(),
            coefficient in any::<f64>(),
            attempt in any::<u32>(),
        ) {
            let policy = RetryPolicy {
                initial_interval: Duration::seconds(seconds),
                backoff_coefficient: coefficient,
                maximum_interval: None,
                maximum_attempts: 0,
                non_retryable_error_types: Vec::new(),
            };
            let _ = compute_retry_backoff(&policy, attempt);
            let _ = crate::runtime::workflow_task::retry_backoff(&policy, attempt);
            let _ = evaluate_activity_retry(
                &policy,
                attempt,
                None,
                false,
                time::OffsetDateTime::UNIX_EPOCH,
                Some(time::OffsetDateTime::UNIX_EPOCH.saturating_add(Duration::MAX)),
            );
        }
    }
}
