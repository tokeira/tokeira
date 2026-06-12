//! Standalone-activity request validation and timeout normalization.
//!
//! Ground truth: `chasm/lib/activity/validator.go @ v1.31.0`
//! (`ValidateAndNormalizeStandaloneActivity` →
//! `validateAndNormalizeActivityAttributes` → `validateAndNormalizeTimeouts`). The
//! rules are reproduced, not invented (Requirement 11.9):
//!
//! - a user-defined task queue is required;
//! - `activityId` and `activityType` are non-empty and within `MaxIDLengthLimit`;
//! - timeout normalization:
//!   1. if schedule-to-close is set, fill schedule-to-start and start-to-close from
//!      it (taking the min where the caller also set them);
//!   2. else if start-to-close is set, set schedule-to-close to the run timeout and
//!      fill schedule-to-start from the run timeout when unset;
//!   3. else error — neither start-to-close nor schedule-to-close is set;
//!   4. cap every timeout to the run timeout when the run timeout is set;
//!   5. clamp heartbeat to no more than start-to-close.
//!
//! Durations are Unix-nanosecond `i64`s; `0` means unset. `MinDurationPtr` in the
//! Go source is `min(DurationValue(d1), DurationValue(d2))` with `nil → 0`, so plain
//! integer `min` reproduces it (the `> 0` guards ensure only set values are mined).

use tokeira_chasm::ChasmError;

/// The activity attributes to validate and normalize. Durations are nanoseconds
/// (`0` = unset).
#[derive(Debug, Clone)]
pub struct ActivityRequest {
    /// Application-level activity id (required, length-limited).
    pub activity_id: String,
    /// Application-level activity type (required, length-limited).
    pub activity_type: String,
    /// User-defined task queue (required, non-empty).
    pub task_queue: String,
    /// Requested schedule-to-start timeout in nanoseconds (`0` = unset).
    pub schedule_to_start_nanos: i64,
    /// Requested schedule-to-close timeout in nanoseconds (`0` = unset).
    pub schedule_to_close_nanos: i64,
    /// Requested start-to-close timeout in nanoseconds (`0` = unset).
    pub start_to_close_nanos: i64,
    /// Requested heartbeat timeout in nanoseconds (`0` = unset).
    pub heartbeat_nanos: i64,
    /// The enclosing run timeout in nanoseconds (`0` = unset); the cap for all
    /// activity timeouts.
    pub run_timeout_nanos: i64,
    /// `MaxIDLengthLimit` — the maximum length for `activity_id`/`activity_type`.
    pub max_id_length: usize,
}

/// The normalized activity timeouts, in nanoseconds (`0` = unset).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NormalizedTimeouts {
    /// Normalized schedule-to-start timeout.
    pub schedule_to_start_nanos: i64,
    /// Normalized schedule-to-close timeout.
    pub schedule_to_close_nanos: i64,
    /// Normalized start-to-close timeout.
    pub start_to_close_nanos: i64,
    /// Normalized heartbeat timeout (`≤` start-to-close).
    pub heartbeat_nanos: i64,
}

/// Validate and normalize a standalone-activity request, returning the normalized
/// timeouts or a [`ChasmError::Validation`] with the targeted-release-aligned
/// message (Requirement 11.9).
pub fn validate_and_normalize(req: &ActivityRequest) -> Result<NormalizedTimeouts, ChasmError> {
    // User-defined task queue is required for standalone activities
    // (`tqid.NormalizeAndValidateUserDefined` rejects an empty queue).
    if req.task_queue.trim().is_empty() {
        return Err(ChasmError::Validation("task queue is not set".to_owned()));
    }
    if req.activity_id.is_empty() {
        return Err(ChasmError::Validation("activityId is not set".to_owned()));
    }
    if req.activity_type.is_empty() {
        return Err(ChasmError::Validation("activityType is not set".to_owned()));
    }
    if req.activity_id.len() > req.max_id_length {
        return Err(ChasmError::Validation(format!(
            "activityId exceeds length limit. Length={} Limit={}",
            req.activity_id.len(),
            req.max_id_length
        )));
    }
    if req.activity_type.len() > req.max_id_length {
        return Err(ChasmError::Validation(format!(
            "activityType exceeds length limit. Length={} Limit={}",
            req.activity_type.len(),
            req.max_id_length
        )));
    }

    normalize_timeouts(req)
}

/// The timeout normalization half (`validateAndNormalizeTimeouts @ v1.31.0`).
fn normalize_timeouts(req: &ActivityRequest) -> Result<NormalizedTimeouts, ChasmError> {
    let schedule_to_close_set = req.schedule_to_close_nanos > 0;
    let schedule_to_start_set = req.schedule_to_start_nanos > 0;
    let start_to_close_set = req.start_to_close_nanos > 0;

    let mut schedule_to_start = req.schedule_to_start_nanos;
    let mut schedule_to_close = req.schedule_to_close_nanos;
    let mut start_to_close = req.start_to_close_nanos;
    let mut heartbeat = req.heartbeat_nanos;

    if schedule_to_close_set {
        schedule_to_start = if schedule_to_start_set {
            schedule_to_start.min(schedule_to_close)
        } else {
            schedule_to_close
        };
        start_to_close = if start_to_close_set {
            start_to_close.min(schedule_to_close)
        } else {
            schedule_to_close
        };
    } else if start_to_close_set {
        // ScheduleToClose deduces to the run timeout (which may itself be unset).
        schedule_to_close = req.run_timeout_nanos;
        if !schedule_to_start_set {
            schedule_to_start = req.run_timeout_nanos;
        }
    } else {
        return Err(ChasmError::Validation(format!(
            "a valid StartToClose or ScheduleToCloseTimeout is not set. ActivityId={} ActivityType={}",
            req.activity_id, req.activity_type
        )));
    }

    // Cap every timeout to the run timeout when it is set.
    let run_timeout = req.run_timeout_nanos;
    if run_timeout > 0 {
        schedule_to_close = schedule_to_close.min(run_timeout);
        schedule_to_start = schedule_to_start.min(run_timeout);
        start_to_close = start_to_close.min(run_timeout);
        if heartbeat > run_timeout {
            heartbeat = run_timeout;
        }
    }

    // Heartbeat never exceeds start-to-close (min with nil→0 leaves unset as unset).
    heartbeat = heartbeat.min(start_to_close);

    Ok(NormalizedTimeouts {
        schedule_to_start_nanos: schedule_to_start,
        schedule_to_close_nanos: schedule_to_close,
        start_to_close_nanos: start_to_close,
        heartbeat_nanos: heartbeat,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEC: i64 = 1_000_000_000;

    fn base() -> ActivityRequest {
        ActivityRequest {
            activity_id: "act-1".to_owned(),
            activity_type: "Type".to_owned(),
            task_queue: "queue".to_owned(),
            schedule_to_start_nanos: 0,
            schedule_to_close_nanos: 0,
            start_to_close_nanos: 0,
            heartbeat_nanos: 0,
            run_timeout_nanos: 0,
            max_id_length: 1000,
        }
    }

    #[test]
    fn missing_task_queue_is_rejected() {
        let req = ActivityRequest {
            task_queue: "  ".to_owned(),
            start_to_close_nanos: 10 * SEC,
            ..base()
        };
        assert!(matches!(
            validate_and_normalize(&req),
            Err(ChasmError::Validation(_))
        ));
    }

    #[test]
    fn missing_ids_are_rejected() {
        let req = ActivityRequest {
            activity_id: String::new(),
            start_to_close_nanos: SEC,
            ..base()
        };
        assert!(validate_and_normalize(&req).is_err());
        let req = ActivityRequest {
            activity_type: String::new(),
            start_to_close_nanos: SEC,
            ..base()
        };
        assert!(validate_and_normalize(&req).is_err());
    }

    #[test]
    fn over_length_id_is_rejected() {
        let req = ActivityRequest {
            activity_id: "x".repeat(11),
            max_id_length: 10,
            start_to_close_nanos: SEC,
            ..base()
        };
        assert!(matches!(
            validate_and_normalize(&req),
            Err(ChasmError::Validation(_))
        ));
    }

    #[test]
    fn no_timeouts_is_rejected() {
        assert!(validate_and_normalize(&base()).is_err());
    }

    #[test]
    fn schedule_to_close_fills_missing_timeouts() {
        let req = ActivityRequest {
            schedule_to_close_nanos: 10 * SEC,
            ..base()
        };
        let n = validate_and_normalize(&req).expect("normalize");
        assert_eq!(n.schedule_to_start_nanos, 10 * SEC);
        assert_eq!(n.start_to_close_nanos, 10 * SEC);
        assert_eq!(n.schedule_to_close_nanos, 10 * SEC);
    }

    #[test]
    fn start_to_close_deduces_close_from_run_timeout_and_caps() {
        let req = ActivityRequest {
            start_to_close_nanos: 100 * SEC,
            run_timeout_nanos: 30 * SEC,
            ..base()
        };
        let n = validate_and_normalize(&req).expect("normalize");
        // schedule_to_close deduced to run timeout; start_to_close capped to it.
        assert_eq!(n.schedule_to_close_nanos, 30 * SEC);
        assert_eq!(n.schedule_to_start_nanos, 30 * SEC);
        assert_eq!(n.start_to_close_nanos, 30 * SEC);
    }

    #[test]
    fn heartbeat_clamped_to_start_to_close() {
        let req = ActivityRequest {
            start_to_close_nanos: 10 * SEC,
            schedule_to_close_nanos: 20 * SEC,
            heartbeat_nanos: 15 * SEC,
            ..base()
        };
        let n = validate_and_normalize(&req).expect("normalize");
        // start_to_close = min(10, 20) = 10; heartbeat clamped to 10.
        assert_eq!(n.start_to_close_nanos, 10 * SEC);
        assert_eq!(n.heartbeat_nanos, 10 * SEC);
    }

    #[test]
    fn unset_heartbeat_stays_unset() {
        let req = ActivityRequest {
            start_to_close_nanos: 10 * SEC,
            schedule_to_close_nanos: 20 * SEC,
            ..base()
        };
        let n = validate_and_normalize(&req).expect("normalize");
        assert_eq!(n.heartbeat_nanos, 0);
    }
}
