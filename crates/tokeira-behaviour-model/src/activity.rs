//! Observable workflow-activity decisions, independent of execution machinery.
//!
//! Timestamps are signed Unix nanoseconds within the protobuf timestamp domain.
//! Duration arithmetic uses Temporal's signed 64-bit nanosecond domain; admission
//! retains wire fields until validation. Arithmetic on absolute times uses i128
//! so adding any supported duration remains exact.

use prost_types::Duration;
use tokeira_proto::{
    common::RetryPolicy,
    enums::{RetryState, TimeoutType},
    failure::{Failure, failure::FailureInfo},
};

/// Protobuf duration converted to the saturated signed nanosecond domain used by Go.
pub fn duration_ns(value: Option<&Duration>) -> i64 {
    value.map_or(0, |d| {
        // AsDuration retains seconds-product overflow even when opposite-sign
        // nanos would bring the exact sum back into range. The server pins this
        // dependency: types/known/durationpb/duration.pb.go:171-187
        // @ google.golang.org/protobuf v1.36.10 (outside the source index).
        d.seconds
            .checked_mul(1_000_000_000)
            .and_then(|seconds| seconds.checked_add(i64::from(d.nanos)))
            .unwrap_or(if d.seconds < 0 { i64::MIN } else { i64::MAX })
    })
}

/// Signed nanoseconds represented without discarding sub-second precision.
pub fn duration(value: i64) -> Duration {
    Duration {
        seconds: value / 1_000_000_000,
        nanos: (value % 1_000_000_000) as i32,
    }
}

/// Apply default activity settings, preserving explicitly nonzero fields.
///
/// `common/retrypolicy/retry_policy.go:32-56 @ v1.31.0`.
pub fn default_policy(mut policy: RetryPolicy) -> RetryPolicy {
    if duration_ns(policy.initial_interval.as_ref()) == 0 {
        policy.initial_interval = Some(duration(1_000_000_000));
    }
    if duration_ns(policy.maximum_interval.as_ref()) == 0 {
        policy.maximum_interval = Some(duration(
            duration_ns(policy.initial_interval.as_ref()).wrapping_mul(100),
        ));
    }
    if policy.backoff_coefficient == 0.0 {
        policy.backoff_coefficient = 2.0;
    }
    policy
}

/// Validate an already defaulted activity policy, retaining its recorded wire values.
///
/// Returns the InvalidArgument message at the first failed check. One attempt
/// bypasses every check and cap; otherwise intervals above 100 years are capped.
/// `common/retrypolicy/retry_policy.go:65-97 @ v1.31.0`.
pub fn validate_policy(mut policy: RetryPolicy) -> Result<RetryPolicy, String> {
    if policy.maximum_attempts == 1 {
        return Ok(policy);
    }
    validate_interval(&mut policy.initial_interval)
        .map_err(|error| format!("invalid InitialInterval set on retry policy: {error}"))?;
    if policy.backoff_coefficient < 1.0 {
        return Err("BackoffCoefficient cannot be less than 1 on retry policy.".into());
    }
    validate_interval(&mut policy.maximum_interval)
        .map_err(|error| format!("invalid MaximumInterval set on retry policy: {error}"))?;
    let maximum = duration_ns(policy.maximum_interval.as_ref());
    if maximum > 0 && maximum < duration_ns(policy.initial_interval.as_ref()) {
        return Err("MaximumInterval cannot be less than InitialInterval on retry policy.".into());
    }
    if policy.maximum_attempts < 0 {
        return Err("MaximumAttempts cannot be negative on retry policy.".into());
    }
    for excluded in &policy.non_retryable_error_types {
        if let Some(name) = excluded.strip_prefix("TemporalTimeout:") {
            // The API module pinned by v1.31.0 accepts canonical and shorthand
            // names, not numeric strings: enums/v1/workflow.go-helpers.pb.go:199-217
            // @ go.temporal.io/api v1.62.8. This dependency is outside the source index.
            if !matches!(
                name,
                "StartToClose"
                    | "ScheduleToStart"
                    | "ScheduleToClose"
                    | "Heartbeat"
                    | "TIMEOUT_TYPE_START_TO_CLOSE"
                    | "TIMEOUT_TYPE_SCHEDULE_TO_START"
                    | "TIMEOUT_TYPE_SCHEDULE_TO_CLOSE"
                    | "TIMEOUT_TYPE_HEARTBEAT"
            ) {
                return Err(format!("Invalid timeout type value: {name}."));
            }
        }
    }
    Ok(policy)
}

// Wire signs must be checked before conversion loses their representation. The
// upstream validator deliberately permits oversized durations and caps them;
// protobuf validity alone is stricter (common/primitives/timestamp/duration.go:57-88
// @ v1.31.0).
fn validate_interval(value: &mut Option<Duration>) -> Result<(), &'static str> {
    let Some(value) = value else {
        return Ok(());
    };
    if (value.seconds > 0 && value.nanos < 0) || (value.seconds < 0 && value.nanos > 0) {
        return Err("duration has seconds and nanos with different signs");
    }
    let nanos = duration_ns(Some(value));
    if nanos < 0 {
        return Err("negative duration");
    }
    if nanos > 3_153_600_000_000_000_000 {
        *value = duration(3_153_600_000_000_000_000);
    }
    Ok(())
}

/// Whether a failure permits retry, before policy, cancellation, and budget checks.
///
/// `service/history/workflow/retry.go:115-151 @ v1.31.0`.
pub fn retryable(failure: Option<&Failure>, excluded: &[String]) -> bool {
    match failure.and_then(|f| f.failure_info.as_ref()) {
        Some(FailureInfo::CanceledFailureInfo(_) | FailureInfo::TerminatedFailureInfo(_)) => false,
        Some(FailureInfo::TimeoutFailureInfo(info)) => {
            let name = match TimeoutType::try_from(info.timeout_type) {
                Ok(TimeoutType::StartToClose) => "TemporalTimeout:StartToClose",
                Ok(TimeoutType::Heartbeat) => "TemporalTimeout:Heartbeat",
                _ => return false,
            };
            !excluded.iter().any(|s| s == name)
        }
        Some(FailureInfo::ServerFailureInfo(info)) => !info.non_retryable,
        Some(FailureInfo::ApplicationFailureInfo(info)) => {
            !info.non_retryable && !excluded.contains(&info.r#type)
        }
        _ => true,
    }
}

/// Exponential interval for the supplied attempt, with nanosecond precision.
///
/// The whole power follows Go's `math.Pow` multiplication order to preserve its
/// rounding (`common/backoff/retry.go:184 @ v1.31.0`;
/// `src/math/pow.go:131-165 @ go1.26.2`).
///
/// `common/backoff/retry.go:180-185 @ v1.31.0`. Positive overflow follows
/// the saturating conversion on arm64; Go's out-of-range float conversion is
/// architecture dependent, so that part of the domain is not portable evidence.
pub fn exponential_interval(initial: i64, coefficient: f64, attempt: i32) -> i64 {
    let exponent = attempt.wrapping_sub(1);
    ((initial as f64 * integer_power(coefficient, exponent)) as i64).max(0)
}

// Go rescales intermediates by exact powers of two. For finite bases >= 1 this
// multiplication order preserves normal results; other results still truncate
// to zero or saturate in exponential_interval. Neither powf nor powi guarantees
// this rounding order.
fn integer_power(mut factor: f64, exponent: i32) -> f64 {
    let mut result = 1.0;
    let mut magnitude = exponent.unsigned_abs();
    while magnitude != 0 {
        if magnitude & 1 == 1 {
            result *= factor;
        }
        factor *= factor;
        magnitude >>= 1;
    }
    if exponent < 0 { 1.0 / result } else { result }
}

/// Earliest retry decision, before delay calculation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetryPrecedence {
    /// Retry is disallowed for the stated public reason.
    Stop(RetryState),
    /// Pause retains the activity and increments its attempt without testing budget.
    Pause,
    /// The attempt budget and time window must now be evaluated.
    Backoff,
}

/// Ordered retry guards, including cancellation before failure classification.
///
/// `service/history/workflow/mutable_state_impl.go:6243-6297 @ v1.31.0`.
/// The paused input includes any workflow-rule match; infrastructure failures and
/// workflow mutability checks are outside this decision's domain.
pub fn retry_precedence(
    policy: Option<&RetryPolicy>,
    cancel_requested: bool,
    paused: bool,
    failure: Option<&Failure>,
) -> RetryPrecedence {
    let Some(policy) = policy else {
        return RetryPrecedence::Stop(RetryState::RetryPolicyNotSet);
    };
    if cancel_requested {
        return RetryPrecedence::Stop(RetryState::CancelRequested);
    }
    if matches!(failure.and_then(|f| f.failure_info.as_ref()), Some(FailureInfo::TimeoutFailureInfo(info)) if matches!(TimeoutType::try_from(info.timeout_type), Ok(TimeoutType::ScheduleToStart | TimeoutType::ScheduleToClose)))
    {
        return RetryPrecedence::Stop(RetryState::Timeout);
    }
    if !retryable(failure, &policy.non_retryable_error_types) {
        return RetryPrecedence::Stop(RetryState::NonRetryableFailure);
    }
    if paused {
        RetryPrecedence::Pause
    } else {
        RetryPrecedence::Backoff
    }
}

/// Retry scheduling result; a pause has no next dispatch time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetryDecision {
    /// The current failure becomes terminal with this public retry state.
    Stop(RetryState),
    /// The activity survives with an incremented attempt, waiting for unpause.
    Paused,
    /// Delay before the next attempt, in signed nanoseconds.
    After(i64),
}

/// Budget, cap, and strict expiration decision for an already calculated interval.
///
/// `service/history/workflow/retry.go:81-112 @ v1.31.0`.
/// A missing expiration includes the upstream zero-time sentinel.
pub fn next_backoff(
    now: i128,
    attempt: i32,
    maximum_attempts: i32,
    interval: i64,
    maximum_interval: i64,
    expiration: Option<i128>,
) -> RetryDecision {
    if maximum_attempts > 0 && attempt.max(1) >= maximum_attempts {
        return RetryDecision::Stop(RetryState::MaximumAttemptsReached);
    }
    if interval <= 0 && maximum_interval == 0 {
        return RetryDecision::Stop(RetryState::Timeout);
    }
    let delay = if maximum_interval != 0 && (interval <= 0 || interval > maximum_interval) {
        maximum_interval
    } else {
        interval
    };
    if expiration.is_some_and(|deadline| now + i128::from(delay) > deadline) {
        RetryDecision::Stop(RetryState::Timeout)
    } else {
        RetryDecision::After(delay)
    }
}

/// Minimal observable state needed to choose an activity retry.
#[derive(Clone, Debug, Default)]
pub struct RetryInput {
    /// Effective policy after admission defaults; absent means no retry policy.
    pub policy: Option<RetryPolicy>,
    /// Current one-based attempt; nonpositive values normalize to one for backoff.
    pub attempt: i32,
    /// Durable cancellation request wins over pause and failure classification.
    pub cancel_requested: bool,
    /// Effective pause after workflow rules have been applied.
    pub paused: bool,
    /// Schedule-to-close expiration, if set.
    pub expiration: Option<i128>,
}

/// Full activity retry decision, retaining worker-requested delay precedence.
///
/// `service/history/workflow/mutable_state_impl.go:6243-6325 @ v1.31.0` and
/// `service/history/workflow/retry.go:56-112 @ v1.31.0`.
pub fn retry(input: &RetryInput, failure: Option<&Failure>, now: i128) -> RetryDecision {
    match retry_precedence(
        input.policy.as_ref(),
        input.cancel_requested,
        input.paused,
        failure,
    ) {
        RetryPrecedence::Stop(reason) => RetryDecision::Stop(reason),
        RetryPrecedence::Pause => RetryDecision::Paused,
        RetryPrecedence::Backoff => {
            let policy = input
                .policy
                .as_ref()
                .expect("backoff requires an effective policy");
            let requested = match failure.and_then(|f| f.failure_info.as_ref()) {
                Some(FailureInfo::ApplicationFailureInfo(info)) => {
                    info.next_retry_delay.as_ref().map(|d| duration_ns(Some(d)))
                }
                _ => None,
            };
            let interval = requested.unwrap_or_else(|| {
                exponential_interval(
                    duration_ns(policy.initial_interval.as_ref()),
                    policy.backoff_coefficient,
                    input.attempt.max(1),
                )
            });
            let maximum =
                requested.unwrap_or_else(|| duration_ns(policy.maximum_interval.as_ref()));
            next_backoff(
                now,
                input.attempt,
                policy.maximum_attempts,
                interval,
                maximum,
                input.expiration,
            )
        }
    }
}

/// Options-update rescheduling uses the current attempt and last worker completion.
///
/// `service/history/workflow/activity.go:231-251 @ v1.31.0`. An absent completion
/// timestamp has protobuf's Unix-epoch value, not the previous scheduled time.
pub fn next_scheduled_time(
    policy: &RetryPolicy,
    attempt: i32,
    scheduled: i128,
    last_complete: Option<i128>,
) -> i128 {
    if attempt <= 1 {
        return scheduled;
    }
    let interval = exponential_interval(
        duration_ns(policy.initial_interval.as_ref()),
        policy.backoff_coefficient,
        attempt,
    );
    let maximum = duration_ns(policy.maximum_interval.as_ref());
    let delay = if maximum != 0 && (interval <= 0 || interval > maximum) {
        maximum
    } else {
        interval
    };
    if delay > 0 {
        last_complete.unwrap_or(0) + i128::from(delay)
    } else {
        scheduled
    }
}

/// Source of an unsuccessful activity attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompletionOrigin {
    /// RespondActivityTaskFailed records the completion time before retry.
    WorkerFailure,
    /// A server timer does not record a worker completion time.
    Timeout,
}

/// Completion-time provenance survives retries and options changes.
///
/// `service/history/api/respondactivitytaskfailed/api.go:97-105 @ v1.31.0`;
/// `service/history/timer_queue_active_task_executor.go:292-361 @ v1.31.0`.
pub fn completion_time(
    previous: Option<i128>,
    now: i128,
    origin: CompletionOrigin,
) -> Option<i128> {
    match origin {
        CompletionOrigin::WorkerFailure => Some(now),
        CompletionOrigin::Timeout => previous,
    }
}

/// Timing values visible through activity lifecycle observations.
#[derive(Clone, Debug, Default)]
pub struct ActivityTiming {
    /// Whether the scheduled event exists.
    pub scheduled: bool,
    /// Effective pause removes the activity from timer selection.
    pub paused: bool,
    /// Current attempt's scheduled time.
    pub scheduled_time: i128,
    /// First scheduled time; absent for older execution state.
    pub first_scheduled_time: Option<i128>,
    /// Started event's time; absence denotes an unstarted attempt.
    pub started_time: Option<i128>,
    /// Last heartbeat, which may predate the current start.
    pub heartbeat_time: Option<i128>,
    /// Zero disables this deadline.
    pub schedule_to_close: i64,
    /// Zero disables this deadline.
    pub schedule_to_start: i64,
    /// Zero disables this deadline.
    pub start_to_close: i64,
    /// Zero disables this deadline.
    pub heartbeat: i64,
}

/// Ordered deadlines for one activity, with timeout enum order breaking ties.
///
/// `service/history/workflow/timer_sequence.go:174-210,229-359,393-425 @ v1.31.0`.
pub fn deadlines(activity: &ActivityTiming) -> Vec<(i128, TimeoutType)> {
    if !activity.scheduled || activity.paused {
        return Vec::new();
    }
    let mut values = Vec::new();
    if activity.schedule_to_close != 0 {
        values.push((
            activity
                .first_scheduled_time
                .unwrap_or(activity.scheduled_time)
                + i128::from(activity.schedule_to_close),
            TimeoutType::ScheduleToClose,
        ));
    }
    match activity.started_time {
        None if activity.schedule_to_start != 0 => values.push((
            activity.scheduled_time + i128::from(activity.schedule_to_start),
            TimeoutType::ScheduleToStart,
        )),
        Some(started) => {
            if activity.start_to_close != 0 {
                values.push((
                    started + i128::from(activity.start_to_close),
                    TimeoutType::StartToClose,
                ));
            }
            if activity.heartbeat != 0 {
                values.push((
                    started.max(activity.heartbeat_time.unwrap_or(started))
                        + i128::from(activity.heartbeat),
                    TimeoutType::Heartbeat,
                ));
            }
        }
        _ => {}
    }
    values.sort_by_key(|&(at, kind)| (at, kind as i32));
    values
}

/// Whether a scheduled timer is due at the executor's millisecond precision.
///
/// `service/history/queues/queue_scheduled.go:280-300 @ v1.31.0`.
pub fn timer_due(deadline: i128, now: i128, task_fire_time: i128) -> bool {
    deadline.div_euclid(1_000_000) <= now.max(task_fire_time).div_euclid(1_000_000)
}

/// Timeout disposition after retry, excluding storage errors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeoutDecision {
    /// An older attempt's timer cannot affect the current attempt.
    Stale,
    /// Retry or pause keeps the activity open and emits no timeout event.
    Retained,
    /// Terminal timeout type and public retry state.
    Closed(TimeoutType, RetryState),
}

/// Interpret a timeout through the same retry precedence as a worker failure.
///
/// `service/history/timer_queue_active_task_executor.go:292-361 @ v1.31.0`.
pub fn timeout_disposition(
    task_attempt: i32,
    current_attempt: i32,
    kind: TimeoutType,
    result: RetryDecision,
) -> TimeoutDecision {
    if task_attempt < current_attempt {
        return TimeoutDecision::Stale;
    }
    match result {
        RetryDecision::After(_) | RetryDecision::Paused => TimeoutDecision::Retained,
        RetryDecision::Stop(state) => TimeoutDecision::Closed(
            if state == RetryState::Timeout && kind != TimeoutType::ScheduleToStart {
                TimeoutType::ScheduleToClose
            } else {
                kind
            },
            state,
        ),
    }
}

/// Effect of a workflow activity-cancel command on valid workflow-task input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CancelDecision {
    /// Unknown activity without a buffered finish, or a duplicate request.
    Invalid,
    /// A buffered finish already resolved the activity; record only the request.
    RecordOnly,
    /// Record request and cancellation together for an unstarted attempt.
    CancelImmediately,
    /// Leave the started activity pending and expose cancellation to its worker.
    RequestWorker,
}

/// Cancel command ordering, including a scheduled retry between attempts.
///
/// `service/history/workflow/mutable_state_impl.go:4364-4396 @ v1.31.0`;
/// `service/history/api/respondworkflowtaskcompleted/workflow_task_completed_handler.go:638-667 @ v1.31.0`.
pub fn request_cancel(
    pending: bool,
    started: bool,
    already_requested: bool,
    buffered_finish: bool,
) -> CancelDecision {
    if !pending {
        return if buffered_finish {
            CancelDecision::RecordOnly
        } else {
            CancelDecision::Invalid
        };
    }
    if already_requested {
        return CancelDecision::Invalid;
    }
    if started {
        CancelDecision::RequestWorker
    } else {
        CancelDecision::CancelImmediately
    }
}

/// Heartbeat cancellation flag for a valid held activity token.
///
/// `service/history/api/recordactivitytaskheartbeat/api.go:73-105 @ v1.31.0`.
pub fn heartbeat_cancel_requested(cancel_requested: bool) -> bool {
    cancel_requested
}

/// Whether a valid held activity token may acknowledge cancellation.
///
/// `service/history/api/respondactivitytaskcanceled/api.go:81-107 @ v1.31.0`.
/// False means ActivityTaskNotCancelRequested; true emits cancellation and wakes the workflow.
pub fn accept_cancellation(cancel_requested: bool) -> bool {
    cancel_requested
}
