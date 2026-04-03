use serde::{Deserialize, Serialize};
use time::Duration;

/// Retry policy attached to a workflow or activity execution.
///
/// The kernel evaluates this policy when deciding whether a
/// failed activity or workflow task should be retried or
/// should transition to a terminal failure state.
///
/// The backoff formula is:
///
/// ```text
/// delay = min(
///     initial_interval * backoff_coefficient ^ (attempt - 1),
///     maximum_interval,
/// )
/// ```
///
/// See `docs/architecture/020-kernel.md` for how the kernel
/// applies retry decisions during history replay.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Base delay before the first retry.
    pub initial_interval: Duration,
    /// Multiplier applied to the interval after each attempt.
    ///
    /// A coefficient of `1.0` produces fixed-interval retries;
    /// values above `1.0` produce exponential backoff.
    pub backoff_coefficient: f64,
    /// Upper bound on the computed delay. `None` means no cap.
    pub maximum_interval: Option<Duration>,
    /// Total number of attempts (including the initial one).
    ///
    /// `0` means unlimited retries (subject to execution
    /// timeout).
    pub maximum_attempts: u32,
    /// Error type strings that should not be retried.
    ///
    /// If the failure's type matches any entry in this list
    /// the kernel skips retry and transitions directly to
    /// `Failed`.
    pub non_retryable_error_types: Vec<String>,
}
