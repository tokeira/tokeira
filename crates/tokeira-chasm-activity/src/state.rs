//! The activity state proto, status enum, and lifecycle mapping.
//!
//! Ground truth: `chasm/lib/activity/proto/v1/activity_state.proto` (states) and
//! `chasm/lib/activity/activity.go:90` (lifecycle mapping) `@ v1.31.0`.
//!
//! [`ActivityState`] is the single `#[chasm(data)]` payload of the activity root
//! component (see the crate MVP note). It is a tokeira-owned proto — activity state
//! is internal engine state, never on the public SDK wire — so the field numbering
//! is ours; only the *behaviour* (states, lifecycle mapping, stamp semantics)
//! tracks the targeted release.

use serde::{Deserialize, Serialize};
use tokeira_chasm::LifecycleState;

/// The eight activity execution states (`activity_state.proto @ v1.31.0`), plus the
/// `Unspecified` zero value a freshly constructed state carries before its first
/// `Scheduled` transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ::prost::Enumeration)]
#[repr(i32)]
pub enum ActivityStatus {
    /// No status assigned yet (pre-`Scheduled`). prost uses this 0-discriminant
    /// variant as the enum's `Default`.
    Unspecified = 0,
    /// Scheduled and awaiting dispatch to a worker.
    Scheduled = 1,
    /// Picked up by a worker.
    Started = 2,
    /// A cancel has been requested but not yet acknowledged.
    CancelRequested = 3,
    /// Completed successfully (terminal).
    Completed = 4,
    /// Failed (terminal).
    Failed = 5,
    /// Canceled (terminal).
    Canceled = 6,
    /// Terminated by an operator (terminal).
    Terminated = 7,
    /// Timed out (terminal).
    TimedOut = 8,
}

impl ActivityStatus {
    /// True for the terminal states (`Completed`/`Failed`/`Canceled`/`Terminated`/
    /// `TimedOut`).
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            ActivityStatus::Completed
                | ActivityStatus::Failed
                | ActivityStatus::Canceled
                | ActivityStatus::Terminated
                | ActivityStatus::TimedOut
        )
    }
}

/// Map an activity status to the component [`LifecycleState`]
/// (`activity.go:90 @ v1.31.0`): `COMPLETED → Completed`;
/// `FAILED | CANCELED | TERMINATED | TIMED_OUT → Failed`; everything else
/// (`UNSPECIFIED`, `SCHEDULED`, `STARTED`, `CANCEL_REQUESTED`) `→ Running`
/// (Requirement 11.3).
pub fn lifecycle_for(status: ActivityStatus) -> LifecycleState {
    match status {
        ActivityStatus::Completed => LifecycleState::Completed,
        ActivityStatus::Failed
        | ActivityStatus::Canceled
        | ActivityStatus::Terminated
        | ActivityStatus::TimedOut => LifecycleState::Failed,
        ActivityStatus::Unspecified
        | ActivityStatus::Scheduled
        | ActivityStatus::Started
        | ActivityStatus::CancelRequested => LifecycleState::Running,
    }
}

/// The persisted activity state — the activity component's single data field.
///
/// Carries the status, the current attempt and its fencing `stamp` (the per-attempt
/// token transitions and timers validate against — Requirement 11.6), the
/// identifying fields and task queue, the normalized timeouts (in Unix-nanosecond
/// durations; `0` means unset), the retry bound, and the input/result/failure
/// payloads. It also carries the Start request's describe-echo fields (header,
/// retry policy, priority, search attributes, user metadata) as opaque encoded
/// bytes so `DescribeActivityExecution` can return them verbatim without the
/// component depending on the public API types. Durations are nanos rather than a
/// proto `Duration` so the type stays a plain prost message; the edge/runtime
/// convert at their boundary.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ActivityState {
    /// Current status.
    #[prost(enumeration = "ActivityStatus", tag = "1")]
    pub status: i32,
    /// Current attempt count (1-based once scheduled).
    #[prost(int32, tag = "2")]
    pub attempt: i32,
    /// The per-attempt fencing stamp; bumped on each (re)schedule (Requirement
    /// 11.6).
    #[prost(int64, tag = "3")]
    pub stamp: i64,
    /// User-defined task queue (required).
    #[prost(string, tag = "4")]
    pub task_queue: String,
    /// Application-level activity id.
    #[prost(string, tag = "5")]
    pub activity_id: String,
    /// Application-level activity type.
    #[prost(string, tag = "6")]
    pub activity_type: String,
    /// Schedule-to-start timeout in nanoseconds (`0` = unset).
    #[prost(int64, tag = "7")]
    pub schedule_to_start_nanos: i64,
    /// Schedule-to-close timeout in nanoseconds (`0` = unset).
    #[prost(int64, tag = "8")]
    pub schedule_to_close_nanos: i64,
    /// Start-to-close timeout in nanoseconds (`0` = unset).
    #[prost(int64, tag = "9")]
    pub start_to_close_nanos: i64,
    /// Heartbeat timeout in nanoseconds (`0` = unset).
    #[prost(int64, tag = "10")]
    pub heartbeat_nanos: i64,
    /// Serialized activity input payload.
    #[prost(bytes = "vec", tag = "11")]
    pub input: Vec<u8>,
    /// Serialized activity result payload (set on `Completed`).
    #[prost(bytes = "vec", tag = "12")]
    pub result: Vec<u8>,
    /// Failure message (set on `Failed`/`Terminated`/`TimedOut`).
    #[prost(string, tag = "13")]
    pub failure: String,
    /// Maximum attempts from the retry policy (`0` = unlimited).
    #[prost(int32, tag = "14")]
    pub maximum_attempts: i32,
    /// Last scheduled time in Unix nanoseconds.
    #[prost(int64, tag = "15")]
    pub scheduled_time_nanos: i64,
    /// Last started time in Unix nanoseconds (`0` = not started).
    #[prost(int64, tag = "16")]
    pub started_time_nanos: i64,
    /// Close time in Unix nanoseconds (`0` = not closed). Recorded when the activity
    /// transitions to a terminal status so the visibility snapshot's close time is
    /// recomputable from persisted node state alone — the Stage-4 repair scanner
    /// depends on every snapshot input being node-resident (Req 10.11).
    #[prost(int64, tag = "17")]
    pub close_time_nanos: i64,
    /// Identity of the worker that polled/started the current attempt (empty until a
    /// worker picks the activity up). Surfaced as
    /// `DescribeActivityExecution.info.last_worker_identity`
    /// (`activity-executions-first-class` Req 3; `standalone_activity_test.go:4831`).
    #[prost(string, tag = "18")]
    pub last_worker_identity: String,
    /// Encoded `temporal.api.common.v1.Header` from the Start request, stored opaque
    /// and surfaced verbatim on `DescribeActivityExecution.info.header` (Req 5;
    /// `standalone_activity_test.go:3122` asserts `ProtoEqual`). Empty when unset.
    #[prost(bytes = "vec", tag = "19")]
    pub header: Vec<u8>,
    /// Encoded `temporal.api.common.v1.RetryPolicy` from the Start request. Stored in
    /// full (beyond the `maximum_attempts` the retry bound uses) so describe can
    /// echo it exactly (`info.retry_policy`, Req 5). Empty when unset.
    #[prost(bytes = "vec", tag = "20")]
    pub retry_policy: Vec<u8>,
    /// Encoded `temporal.api.common.v1.Priority` from the Start request, echoed on
    /// `info.priority`. Empty when unset.
    #[prost(bytes = "vec", tag = "21")]
    pub priority: Vec<u8>,
    /// Encoded `temporal.api.common.v1.SearchAttributes` from the Start request,
    /// echoed on `info.search_attributes`. Empty when unset.
    #[prost(bytes = "vec", tag = "22")]
    pub search_attributes: Vec<u8>,
    /// Encoded `temporal.api.sdk.v1.UserMetadata` from the Start request, echoed on
    /// `info.user_metadata`. Empty when unset.
    #[prost(bytes = "vec", tag = "23")]
    pub user_metadata: Vec<u8>,
    /// Encoded `temporal.api.failure.v1.Failure` recorded on a worker
    /// `RespondActivityTaskFailed` — the full structured failure (e.g. carrying
    /// `ApplicationFailureInfo`), stored so `DescribeActivityExecution.outcome`
    /// round-trips it exactly rather than only the `failure` message (Req 5;
    /// `standalone_activity_test.go:3047` asserts `ProtoEqual` on the failure).
    /// Empty for non-failure terminals (Terminated/Canceled build their failure from
    /// the reason at the edge).
    #[prost(bytes = "vec", tag = "24")]
    pub failure_payload: Vec<u8>,
}

// `status()` and `set_status()` accessors for the `status` enumeration field are
// generated by the `prost::Message` derive, so they are not defined by hand here.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_mapping_matches_v1_31_0() {
        assert_eq!(
            lifecycle_for(ActivityStatus::Completed),
            LifecycleState::Completed
        );
        for failed in [
            ActivityStatus::Failed,
            ActivityStatus::Canceled,
            ActivityStatus::Terminated,
            ActivityStatus::TimedOut,
        ] {
            assert_eq!(lifecycle_for(failed), LifecycleState::Failed);
        }
        for running in [
            ActivityStatus::Unspecified,
            ActivityStatus::Scheduled,
            ActivityStatus::Started,
            ActivityStatus::CancelRequested,
        ] {
            assert_eq!(lifecycle_for(running), LifecycleState::Running);
        }
    }

    #[test]
    fn terminal_classification() {
        assert!(ActivityStatus::Completed.is_terminal());
        assert!(!ActivityStatus::Scheduled.is_terminal());
        assert!(!ActivityStatus::CancelRequested.is_terminal());
    }

    #[test]
    fn status_round_trips_through_proto_int() {
        let mut state = ActivityState::default();
        state.set_status(ActivityStatus::Started);
        assert_eq!(state.status(), ActivityStatus::Started);
        let bytes = {
            use prost::Message as _;
            state.encode_to_vec()
        };
        let back = {
            use prost::Message as _;
            ActivityState::decode(bytes.as_slice()).expect("decode")
        };
        assert_eq!(back.status(), ActivityStatus::Started);
    }
}
