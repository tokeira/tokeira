//! Workflow retry-policy defaults and activity retry evaluation.
//!
//! Workflow admission shares defaults with schedule firing so persisted policies
//! agree across entry points. Activity evaluation decides whether to retry and
//! how long to wait; it does not use the workflow defaulting function.

use time::Duration;
use tokeira_proto::enums::TimeoutType;
use tokeira_types::RetryPolicy;

/// Retry-relevant fields of the outer worker failure, or a server timeout.
///
/// Causes remain in the opaque history payload but never influence this decision
/// (service/history/workflow/retry.go:56-68,115-151 @ v1.31.0). This is request
/// data, not durable state; the edge preserves duration conversion semantics.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum ActivityFailure {
    /// Missing failures and unrecognized outer kinds are retryable.
    #[default]
    Other,
    /// Cancellation failures never retry.
    Canceled,
    /// Termination failures never retry.
    Terminated,
    /// Preserve unknown enum values as well as the four named timeout kinds.
    Timeout(i32),
    /// Server failures ignore application-type exclusions.
    Server {
        /// Worker or payload-admission decision to end retries.
        non_retryable: bool,
    },
    /// Application failures match the exact type, including an empty string.
    Application {
        /// Case-sensitive policy exclusion key.
        error_type: String,
        /// Worker decision to end retries before checking the retry budget.
        non_retryable: bool,
        /// Overrides both the interval and maximum, even when zero or negative.
        next_retry_delay: Option<Duration>,
    },
}

impl ActivityFailure {
    /// Whether the outer failure permits retry under the policy's exclusions.
    /// Schedule timeouts additionally carry terminal `RetryState::Timeout` in
    /// the runtime (service/history/workflow/mutable_state_impl.go:6260-6269
    /// @ v1.31.0).
    pub fn is_retryable(&self, excluded: &[String]) -> bool {
        // Only application and eligible timeout kinds consult the type list:
        // service/history/workflow/retry.go:115-151 @ v1.31.0.
        match self {
            Self::Other => true,
            Self::Canceled | Self::Terminated => false,
            Self::Server { non_retryable } => !non_retryable,
            Self::Application {
                error_type,
                non_retryable,
                ..
            } => !non_retryable && !excluded.contains(error_type),
            Self::Timeout(kind) => {
                let name = match TimeoutType::try_from(*kind) {
                    Ok(TimeoutType::StartToClose) => "TemporalTimeout:StartToClose",
                    Ok(TimeoutType::Heartbeat) => "TemporalTimeout:Heartbeat",
                    _ => return false,
                };
                !excluded.iter().any(|candidate| candidate == name)
            }
        }
    }

    pub(crate) fn is_schedule_timeout(&self) -> bool {
        matches!(self, Self::Timeout(kind) if matches!(TimeoutType::try_from(*kind),
            Ok(TimeoutType::ScheduleToStart | TimeoutType::ScheduleToClose)))
    }
}

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
    /// The interval is nonpositive without a maximum, or the next attempt begins
    /// strictly after retry expiration (service/history/workflow/retry.go:100-110
    /// @ v1.31.0).
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
/// `nextBackoffInterval` (service/history/workflow/retry.go:70-113 @ v1.31.0):
/// classify the outer failure, enforce the attempt limit, then test the interval
/// and exact expiration. An application's requested delay replaces policy timing.
/// The caller owns no-policy, cancel, schedule-timeout and pause precedence.
pub fn evaluate_activity_retry(
    policy: &RetryPolicy,
    current_attempt: u32,
    failure: &ActivityFailure,
    now: time::OffsetDateTime,
    expiration: Option<time::OffsetDateTime>,
) -> RetryDecision {
    if !failure.is_retryable(&policy.non_retryable_error_types) {
        return RetryDecision::Exhausted {
            reason: RetryExhaustedReason::NonRetryableFailure,
        };
    }

    if policy.maximum_attempts > 0 && current_attempt.max(1) >= policy.maximum_attempts {
        return RetryDecision::Exhausted {
            reason: RetryExhaustedReason::MaximumAttemptsReached,
        };
    }

    // A requested delay replaces both interval and maximum after the attempt
    // limit. Negative delays deliberately survive: a nonzero maximum replaces
    // a nonpositive interval with itself (service/history/workflow/mutable_state_impl.go:6300-6317;
    // service/history/workflow/retry.go:96-107 @ v1.31.0).
    let (backoff, maximum) = match failure {
        ActivityFailure::Application {
            next_retry_delay: Some(delay),
            ..
        } => (*delay, *delay),
        _ => (
            compute_retry_backoff(policy, current_attempt),
            policy.maximum_interval.unwrap_or_default(),
        ),
    };
    if maximum.is_zero() && backoff <= Duration::ZERO {
        return RetryDecision::Exhausted {
            reason: RetryExhaustedReason::Timeout,
        };
    }
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

/// Compute a nanosecond retry interval; zero maximum means uncapped.
/// A nonzero maximum replaces both nonpositive and over-maximum results.
pub fn compute_retry_backoff(policy: &RetryPolicy, attempt: u32) -> Duration {
    // The interval domain is signed nanoseconds, including conversion of legacy
    // stored inputs. Rust's float cast saturates overflow; this selects the
    // saturating interpretation of the architecture-dependent Go conversion
    // (common/backoff/retry.go:183-185 @ v1.31.0).
    let initial = policy
        .initial_interval
        .whole_nanoseconds()
        .clamp(i128::from(i64::MIN), i128::from(i64::MAX));
    let nanos =
        initial as f64 * integer_retry_power(policy.backoff_coefficient, attempt.saturating_sub(1));
    let mut computed = Duration::nanoseconds((nanos as i64).max(0));
    if let Some(maximum) = policy.maximum_interval.map(|maximum| {
        Duration::nanoseconds(
            maximum
                .whole_nanoseconds()
                .clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64,
        )
    }) && !maximum.is_zero()
        && (computed <= Duration::ZERO || computed > maximum)
    {
        computed = maximum;
    }
    computed
}

// Match Go's low-bit multiplication order so platform powf/powi rounding cannot
// change the truncated nanosecond interval (common/backoff/retry.go:183-185
// @ v1.31.0; Go src/math/pow.go:131-156 @ go1.26.2).
fn integer_retry_power(mut factor: f64, mut exponent: u32) -> f64 {
    let mut result = 1.0;
    while exponent != 0 {
        if exponent & 1 == 1 {
            result *= factor;
        }
        factor *= factor;
        exponent >>= 1;
    }
    result
}

#[cfg(test)]
mod duration_tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn retry_backoff_matches_go_integer_power_rounding() {
        // Expected intervals come from Go's math.Pow and the nanosecond conversion
        // in common/backoff/retry.go:183-185 @ v1.31.0, without a maximum masking rounding.
        let actual = [
            (1.01, 4, Duration::seconds(1)),
            (1.2, 4, Duration::seconds(1)),
            (1.2, 6, Duration::seconds(1)),
            (3.0, 35, Duration::nanoseconds(1)),
        ]
        .map(|(backoff_coefficient, attempt, initial_interval)| {
            let policy = RetryPolicy {
                initial_interval,
                backoff_coefficient,
                maximum_interval: None,
                maximum_attempts: 0,
                non_retryable_error_types: Vec::new(),
            };
            compute_retry_backoff(&policy, attempt).whole_nanoseconds()
        });
        assert_eq!(
            actual,
            [
                1_030_300_999,
                1_728_000_000,
                2_488_320_000,
                16_677_181_699_666_568,
            ]
        );
    }

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
                evaluate_activity_retry(
                    &policy,
                    1,
                    &crate::retry::ActivityFailure::Other,
                    now,
                    expiration
                ),
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
                attempt, &crate::retry::ActivityFailure::Other,
                time::OffsetDateTime::UNIX_EPOCH,
                Some(time::OffsetDateTime::UNIX_EPOCH.saturating_add(Duration::MAX)),
            );
        }
    }
}
