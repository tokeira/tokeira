//! The activity state proto, status enum, and lifecycle mapping.
//!
//! Ground truth: `chasm/lib/activity/proto/v1/activity_state.proto` (states) and
//! `chasm/lib/activity/activity.go:90` (lifecycle mapping) `@ v1.31.0`.
//! Callback fields follow `chasm/lib/callback @ v1.32.0`; lifecycle retention
//! until their settlement is Tokeira-owned so rebuild scans can finish delivery.
//!
//! [`ActivityState`] is the single `#[chasm(data)]` payload of the activity root
//! component (see the crate MVP note). It is a tokeira-owned proto — activity state
//! is internal engine state, never on the public SDK wire — so the field numbering
//! is ours; only the *behaviour* (states, lifecycle mapping, stamp semantics)
//! tracks the targeted release.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use tokeira_chasm::LifecycleState;
use tokeira_proto::enums::CallbackState;

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

/// Keep a terminal activity discoverable by the Running-execution rebuild scan
/// until every callback settles. Without callbacks this is exactly [`lifecycle_for`].
/// Status and close time remain the activity's public outcome, independent of delivery.
pub fn lifecycle_of(state: &ActivityState) -> LifecycleState {
    if state
        .callbacks
        .iter()
        .any(|callback| !callback.is_settled())
    {
        LifecycleState::Running
    } else {
        lifecycle_for(state.status())
    }
}

/// Callback state flattened into the activity root (decision D4), in attach order.
/// State values 0–5 match `chasm/lib/callback/proto/v1/message.proto @ v1.32.0`.
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct ActivityCallback {
    /// Stable `<request_id>-<index>` identity (`activity.go:471 @ v1.32.0`).
    #[prost(string, tag = "1")]
    pub id: String,
    /// One CHASM clock reading for the attaching batch (`activity.go:455 @ v1.32.0`).
    #[prost(int64, tag = "2")]
    pub registration_time_nanos: i64,
    /// Public callback enum, sharing the upstream internal status discriminants.
    #[prost(enumeration = "::tokeira_proto::enums::CallbackState", tag = "3")]
    pub state: i32,
    /// Completed delivery attempts, also the task's generation fence.
    #[prost(int32, tag = "4")]
    pub attempt: i32,
    /// Completion time of the latest delivery attempt; zero before the first.
    #[prost(int64, tag = "5")]
    pub last_attempt_complete_time_nanos: i64,
    /// Encoded `temporal.api.failure.v1.Failure`; empty when absent.
    #[prost(bytes = "vec", tag = "6")]
    pub last_attempt_failure: Vec<u8>,
    /// Retry deadline in Unix nanoseconds; zero when no retry is scheduled.
    #[prost(int64, tag = "7")]
    pub next_attempt_time_nanos: i64,
    /// Each element is one encoded `temporal.api.common.v1.Link`.
    #[prost(bytes = "vec", repeated, tag = "8")]
    pub links: Vec<Vec<u8>>,
    /// Nexus URL or an opaque in-process return address; internal targets never cross the API.
    #[prost(oneof = "activity_callback::Target", tags = "10, 11")]
    pub target: Option<activity_callback::Target>,
}

impl ActivityCallback {
    /// Only successful or permanently failed delivery releases lifecycle retention.
    pub fn is_settled(&self) -> bool {
        matches!(
            self.state(),
            CallbackState::Succeeded | CallbackState::Failed
        )
    }
}

/// Persisted callback target variants. Their delivery is owned by runtime executors.
pub mod activity_callback {
    /// One delivery destination; missing targets indicate malformed persisted state.
    #[derive(Clone, PartialEq, Eq, ::prost::Oneof)]
    pub enum Target {
        /// HTTP Nexus completion destination.
        #[prost(message, tag = "10")]
        Nexus(super::NexusTarget),
        /// In-process CHASM outcome destination (decision D2).
        #[prost(message, tag = "11")]
        Internal(super::InternalTarget),
    }
}

/// Nexus completion address. Ordered headers keep root-proto encoding deterministic.
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct NexusTarget {
    /// Completion URL validated at the edge.
    #[prost(string, tag = "1")]
    pub url: String,
    /// Headers in lexical order, independent of their insertion order.
    #[prost(btree_map = "string, string", tag = "2")]
    pub header: BTreeMap<String, String>,
}

/// Opaque return address populated only by the start executor, never a public request.
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct InternalTarget {
    /// Bytes produced by `tokeira_chasm::ComponentRef::encode()`.
    #[prost(bytes = "vec", tag = "1")]
    pub component_ref: Vec<u8>,
    /// Registered task type on the addressed component.
    #[prost(uint32, tag = "2")]
    pub task_type_id: u32,
    /// Postcard-encoded `tokeira_chasm::TaskId`, decoded by the delivery executor.
    #[prost(bytes = "vec", tag = "3")]
    pub task_id: Vec<u8>,
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
    /// Encoded `temporal.api.common.v1.Payloads` carrying the worker's last
    /// heartbeat details. Set when a worker supplies `LastHeartbeatDetails` on
    /// `RespondActivityTaskFailed` (`statemachine.go:220 @ v1.31.0` records it onto
    /// `LastHeartbeat.Details`), echoed verbatim on
    /// `DescribeActivityExecution.info.heartbeat_details`
    /// (`activity.go:215,674 @ v1.31.0`; `standalone_activity_test.go:4908`). Empty
    /// when no heartbeat details were captured.
    #[prost(bytes = "vec", tag = "25")]
    pub last_heartbeat_details: Vec<u8>,
    /// The `request_id` of the cancel request that drove the activity into
    /// `CANCEL_REQUESTED` (`ActivityCancelState.request_id`, `activity.go:290 @
    /// v1.31.0`). A second `RequestCancelActivityExecution` with a different
    /// request_id is `FailedPrecondition`; the same id is an idempotent no-op
    /// (`activity.go:402-409`). Empty until a cancel is requested.
    #[prost(string, tag = "26")]
    pub cancel_request_id: String,
    /// The cancel request's reason, echoed on `info.canceled_reason`
    /// (`ActivityCancelState.reason`; `standalone_activity_test.go:1313`). Empty
    /// until a cancel is requested.
    #[prost(string, tag = "27")]
    pub cancel_reason: String,
    /// The `request_id` of the terminate request (`ActivityTerminateState.request_id`,
    /// `activity.go:363 @ v1.31.0`). A second `TerminateActivityExecution` with a
    /// different request_id is `FailedPrecondition`; the same id is an idempotent
    /// no-op. Empty until a terminate is requested.
    #[prost(string, tag = "28")]
    pub terminate_request_id: String,
    /// Retry-policy initial interval in nanoseconds (`RetryPolicy.initial_interval`).
    /// Folded out of the opaque `retry_policy` blob into a scalar so the pure retry
    /// decision can compute backoff without decoding the proto — the pure crate
    /// stays proto-free (the edge parses the policy and applies Temporal's defaults
    /// before Start, `retrypolicy.EnsureDefaults @ v1.31.0`). `0` is only seen on
    /// states predating a retry policy (none in the build phase); the edge always
    /// writes the defaulted `1s`.
    #[prost(int64, tag = "29")]
    pub retry_initial_interval_nanos: i64,
    /// Retry-policy backoff coefficient (`RetryPolicy.backoff_coefficient`). Stored
    /// as the proto's `double` so `exponential_retry_interval` reproduces
    /// `backoff.ExponentialBackoffAlgorithm @ v1.31.0` exactly. The edge writes the
    /// defaulted `2.0` when unset.
    #[prost(double, tag = "30")]
    pub retry_backoff_coefficient: f64,
    /// Retry-policy maximum interval cap in nanoseconds (`RetryPolicy.maximum_interval`).
    /// `0` means "no cap" — matching `CalculateExponentialRetryInterval @ v1.31.0`,
    /// which only caps when the maximum is non-zero. The edge writes the defaulted
    /// `100 × initial_interval` when unset.
    #[prost(int64, tag = "31")]
    pub retry_maximum_interval_nanos: i64,
    /// The time of the most recent worker heartbeat, Unix nanoseconds (`0` = none
    /// this attempt). The heartbeat-timeout deadline is
    /// `max(last_heartbeat, started) + heartbeat`, mirroring `lastHbTime :=
    /// MaxTime(lastHb.RecordedTime, attemptStartTime)` in the v1.31.0
    /// heartbeat-timeout `Validate` (`activity_tasks.go @ v1.31.0`); a fresh
    /// heartbeat pushes the deadline out.
    #[prost(int64, tag = "32")]
    pub last_heartbeat_time_nanos: i64,
    /// The current attempt's scheduled-to-start anchor, Unix nanoseconds. On the
    /// first `Scheduled` this equals [`scheduled_time_nanos`](Self::scheduled_time_nanos);
    /// on a retry it is the retry's start time (`completeTime + retryInterval`,
    /// `attemptScheduleTimeForRetry @ v1.31.0`), which is when the delayed dispatch
    /// fires and from which the schedule-to-start timer is re-anchored
    /// (`statemachine.go:109,119 @ v1.31.0`). It is distinct from
    /// `scheduled_time_nanos` because the schedule-to-close budget stays pinned to
    /// the original schedule time across retries while schedule-to-start tracks each
    /// attempt.
    #[prost(int64, tag = "33")]
    pub attempt_scheduled_time_nanos: i64,
    /// Identity of the client that requested cancellation. The canceled outcome
    /// reports this requester (not the worker acknowledging cancellation), matching
    /// `CanceledFailureInfo.identity` (`statemachine.go @ v1.31.0`).
    #[prost(string, tag = "34")]
    pub cancel_identity: String,
    /// Encoded `Payloads` supplied when the worker acknowledged cancellation.
    #[prost(bytes = "vec", tag = "35")]
    pub canceled_details: Vec<u8>,
    /// Identity of the client that terminated the activity, surfaced through the
    /// terminal outcome's `TerminatedFailureInfo`.
    #[prost(string, tag = "36")]
    pub terminate_identity: String,
    /// Completion time of the previous attempt, used with the current retry
    /// interval to project the next attempt's schedule time.
    #[prost(int64, tag = "37")]
    pub last_attempt_complete_time_nanos: i64,
    /// Backoff selected for the currently scheduled retry attempt.
    #[prost(int64, tag = "38")]
    pub current_retry_interval_nanos: i64,
    /// D1: set only by the runtime's start executor, never the public start path.
    #[prost(message, optional, tag = "39")]
    pub version_target: Option<tokeira_chasm::DeploymentVersionTarget>,
    /// Attach-ordered callbacks (`activity.go:111–113 @ v1.32.0`); the upstream
    /// `Callbacks chasm.Map` is flattened into this root per decision D4.
    #[prost(message, repeated, tag = "40")]
    pub callbacks: Vec<ActivityCallback>,
}

// `status()` and `set_status()` accessors for the `status` enumeration field are
// generated by the `prost::Message` derive, so they are not defined by hand here.

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message as _;
    use tokeira_chasm::DeploymentVersionTarget;

    #[test]
    fn legacy_state_bytes_leave_extension_fields_absent() {
        let bytes = [8, 2];
        let state = ActivityState::decode(bytes.as_slice()).expect("legacy Started state");
        assert_eq!(state.status(), ActivityStatus::Started);
        assert!(state.version_target.is_none());
        assert!(state.callbacks.is_empty());
        assert_eq!(state.encode_to_vec(), bytes);
    }

    #[test]
    fn callback_targets_links_and_version_target_round_trip_deterministically() {
        let nexus = NexusTarget {
            url: "https://example.test/completion".to_owned(),
            header: [
                ("z".to_owned(), "last".to_owned()),
                ("a".to_owned(), "first".to_owned()),
            ]
            .into(),
        };
        let state = ActivityState {
            version_target: Some(DeploymentVersionTarget {
                deployment_name: "deployment".to_owned(),
                build_id: "build".to_owned(),
            }),
            callbacks: vec![
                ActivityCallback {
                    id: "request-0".to_owned(),
                    registration_time_nanos: 123,
                    state: CallbackState::BackingOff as i32,
                    attempt: 2,
                    last_attempt_complete_time_nanos: 234,
                    last_attempt_failure: vec![1, 2, 3],
                    next_attempt_time_nanos: 345,
                    links: vec![vec![4, 5], vec![6, 7]],
                    target: Some(activity_callback::Target::Nexus(nexus.clone())),
                },
                ActivityCallback {
                    id: "request-1".to_owned(),
                    target: Some(activity_callback::Target::Internal(InternalTarget {
                        component_ref: vec![8, 9],
                        task_type_id: 42,
                        task_id: vec![10, 11],
                    })),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let encoded = state.encode_to_vec();
        assert_eq!(
            ActivityState::decode(encoded.as_slice()).expect("decode extensions"),
            state
        );
        let mut reversed = state.clone();
        reversed.callbacks[0].target = Some(activity_callback::Target::Nexus(NexusTarget {
            header: nexus.header.into_iter().rev().collect(),
            ..nexus
        }));
        assert_eq!(reversed.encode_to_vec(), encoded);
    }

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
        let bytes = state.encode_to_vec();
        let back = ActivityState::decode(bytes.as_slice()).expect("decode");
        assert_eq!(back.status(), ActivityStatus::Started);
    }
}
