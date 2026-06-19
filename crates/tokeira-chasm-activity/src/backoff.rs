//! Exponential retry-interval computation for activity retries.
//!
//! Ground truth: `common/backoff/retry.go @ v1.31.0` —
//! `CalculateExponentialRetryInterval` over `ExponentialBackoffAlgorithm`. The
//! formula is `initial * coefficient^(attempt - 1)`, capped to `maximum` when the
//! maximum is set (non-zero), clamped to `[0, i64::MAX]`. This is the pure,
//! proto-free half of the retry decision (the policy is folded into
//! [`ActivityState`](crate::state::ActivityState) scalar fields at the edge), so it
//! lives in the pure crate alongside the state machine.

/// Compute the retry interval (in nanoseconds) for `attempt`, mirroring
/// `backoff.CalculateExponentialRetryInterval(policy, attempt) @ v1.31.0`.
///
/// `initial_nanos` and `maximum_nanos` are the policy's initial/maximum intervals
/// in nanoseconds; `coefficient` is the backoff coefficient; `attempt` is the
/// 1-based current attempt count (`attempt.Count` in the source). The interval is
/// `initial * coefficient^(attempt - 1)`:
///
/// - capped to `maximum_nanos` only when it is non-zero (`maxInterval.AsDuration()
///   != 0` in the source — a zero maximum means "no cap");
/// - clamped to `[0, i64::MAX]` so an overflowing product saturates rather than
///   wrapping (`max(0, min(int64(result), MaxInt64))` in the source).
pub fn exponential_retry_interval(
    initial_nanos: i64,
    coefficient: f64,
    maximum_nanos: i64,
    attempt: i32,
) -> i64 {
    // `attempt - 1` can be negative only if a caller passes attempt 0; the source
    // is always called with a 1-based count, so the exponent is >= 0 in practice.
    // `powi` with a negative exponent would shrink the interval below `initial`,
    // which is harmless (clamped to >= 0), so no separate guard is needed.
    let raw = initial_nanos as f64 * coefficient.powi(attempt - 1);
    // Saturating conversion: NaN/inf and out-of-range floats clamp to the i64
    // bounds, reproducing the source's `max(0, min(int64(result), MaxInt64))`.
    let interval = if raw.is_nan() {
        0
    } else if raw >= i64::MAX as f64 {
        i64::MAX
    } else if raw <= 0.0 {
        0
    } else {
        raw as i64
    };
    if maximum_nanos != 0 && interval > maximum_nanos {
        maximum_nanos
    } else {
        interval
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEC: i64 = 1_000_000_000;

    #[test]
    fn first_attempt_is_initial_interval() {
        // attempt 1 → initial * coef^0 = initial.
        assert_eq!(exponential_retry_interval(SEC, 2.0, 100 * SEC, 1), SEC);
    }

    #[test]
    fn grows_by_coefficient_each_attempt() {
        // attempt 2 → initial * 2^1 = 2s; attempt 3 → 4s.
        assert_eq!(exponential_retry_interval(SEC, 2.0, 100 * SEC, 2), 2 * SEC);
        assert_eq!(exponential_retry_interval(SEC, 2.0, 100 * SEC, 3), 4 * SEC);
    }

    #[test]
    fn caps_at_maximum_when_set() {
        // 2^10 = 1024s would exceed a 10s cap.
        assert_eq!(exponential_retry_interval(SEC, 2.0, 10 * SEC, 11), 10 * SEC);
    }

    #[test]
    fn zero_maximum_means_no_cap() {
        assert_eq!(exponential_retry_interval(SEC, 2.0, 0, 11), 1024 * SEC);
    }

    #[test]
    fn overflow_saturates_to_i64_max() {
        // A huge coefficient at a high attempt overflows the float product; the
        // source saturates to MaxInt64 rather than wrapping.
        assert_eq!(exponential_retry_interval(i64::MAX, 2.0, 0, 64), i64::MAX);
    }
}
