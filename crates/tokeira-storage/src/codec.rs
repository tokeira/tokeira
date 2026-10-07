//! Versioned blob envelopes and the history-size accounting shared by every store.
//!
//! Kernel values are persisted with postcard, which is positional: a field appended
//! to `WorkflowState` or to a `HistoryEventKind` variant leaves an older blob short,
//! and decoding it yields `DeserializeUnexpectedEnd` at best or a misread value at
//! worst. The two envelopes here put a 32-bit magic in front of the hot-state and
//! history-batch blobs so that any blob written before the envelope fails loudly
//! with [`BlobFormatError`] instead of decoding into something plausible
//! (continue-as-new-advice, Requirement 10). The magic values are chosen so that no
//! pre-envelope blob can match them: a bare `WorkflowState` starts with the run key's
//! 16-byte length prefix and a bare batch starts with its event count.
//!
//! # The state extension
//!
//! A hot-state blob may carry data that is not part of `WorkflowState`'s positional
//! layout in a *state extension* written after the state (activity-heartbeat-time,
//! Requirement 1):
//!
//! ```text
//! varint WORKFLOW_STATE_ENVELOPE_VERSION    unchanged
//! WorkflowState (postcard)                  unchanged; extension fields are #[serde(skip)]
//! [ varint WORKFLOW_STATE_EXTENSION_MAGIC   absent when no section has data
//!   Vec<ExtensionSection { tag, payload }> ] each tag once, in ascending order
//! ```
//!
//! Every Tokeira release from 0.2.0 to 0.5.1 decodes the state with
//! `postcard::from_bytes`, which ignores the bytes after it, so those releases still
//! read a blob that carries an extension; they drop its data when they rewrite the
//! run. This module's reader is strict about the extension's framing and fails with
//! [`StateExtensionError`] on any defect, but it ignores sections whose tag it does not
//! know. The rules that keep both directions safe:
//!
//! - A section carries only data whose absence leaves a reader's behaviour as it was
//!   before that data existed. Anything else needs a new envelope version.
//! - A released tag's payload layout never changes; a different layout takes a new
//!   tag.
//! - A change to `WorkflowState`'s positional layout takes a new envelope version
//!   (activity-heartbeat-time, Requirement 1.11; continue-as-new-advice,
//!   Requirement 10).
//!
//! Section [`ACTIVITY_HEARTBEAT_SECTION`] holds each activity's last heartbeat time.
//!
//! [`history_batch_encoded_len`] is the one definition of a batch's persisted size.
//! The DSQL repository and the in-memory store both account the per-run History Size
//! with it, so the number a workflow task is told, the number `DescribeWorkflowExecution`
//! reports, and the visibility `HistorySizeBytes` attribute cannot drift apart
//! (Requirement 1.4).
//!
//! This module is deliberately outside the `dsql` feature: the in-memory store needs
//! the same envelopes and size function.

use std::collections::BTreeSet;

use anyhow::Result;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use time::OffsetDateTime;
use tokeira_kernel::{HistoryEvent, WorkflowState};
use tokeira_types::RunKey;

/// Magic prefix of an enveloped `workflow_hot.state_data` blob (`"TKWS"`).
pub const WORKFLOW_STATE_ENVELOPE_VERSION: u32 = 0x544B_5753;

/// Magic prefix of an enveloped `history_batch.events_data` blob (`"TKHB"`).
pub const HISTORY_BATCH_ENVELOPE_VERSION: u32 = 0x544B_4842;

/// Magic that opens the state extension after a hot-state blob's state (`"TKWX"`).
pub const WORKFLOW_STATE_EXTENSION_MAGIC: u32 = 0x544B_5758;

/// State-extension tag of the section holding each activity's last heartbeat time.
pub const ACTIVITY_HEARTBEAT_SECTION: u32 = 1;

/// One tagged section of an extension. The payload layout is fixed per tag.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ExtensionSection {
    pub(crate) tag: u32,
    pub(crate) payload: Vec<u8>,
}

/// One entry of the [`ACTIVITY_HEARTBEAT_SECTION`] payload, a `Vec` in activity-id
/// order. This layout is frozen.
#[derive(Serialize, Deserialize)]
struct ActivityHeartbeat {
    activity_id: String,
    last_heartbeat_at: OffsetDateTime,
}

/// Encode a persisted value with postcard.
///
/// Keep this generic helper small and boring: schema compatibility is enforced by
/// the typed wrappers around it, so call sites should name the domain payload they
/// are storing instead of invoking postcard directly.
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    postcard::to_allocvec(value).map_err(Into::into)
}

/// Decode a persisted value with postcard.
///
/// Decode errors are intentionally surfaced as storage errors. A corrupt blob means
/// the repository cannot safely infer derived state from that row.
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    postcard::from_bytes(bytes).map_err(Into::into)
}

/// A hot-state or history blob whose leading version is not the one this build writes.
///
/// The message is operator-facing: the only known producer of such a blob is a
/// Tokeira release older than the envelopes, whose state this release cannot read.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error(
    "{kind} blob for run {} has unsupported format version {observed:#x}; hot state \
     written before Tokeira 0.1.3 cannot be read, recreate the cluster",
    run_key.0
)]
pub struct BlobFormatError {
    /// Which persisted blob was read: `workflow_hot.state_data` or
    /// `history_batch.events_data`.
    pub kind: &'static str,
    /// The run whose blob was rejected.
    pub run_key: RunKey,
    /// The leading version actually found. A pre-envelope blob reports its first
    /// postcard varint, `0x10` for hot state and the event count for a batch.
    pub observed: u32,
}

/// A hot-state blob whose bytes after the state are not a well-formed state
/// extension.
///
/// Only Tokeira writes the extension, so this means a corrupt blob; the run's state
/// is not returned.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{kind} blob for run {} has a malformed extension: {defect}", run_key.0)]
pub struct StateExtensionError {
    /// Which persisted blob was read.
    pub kind: &'static str,
    /// The run whose blob was rejected.
    pub run_key: RunKey,
    /// What is wrong with the extension.
    pub defect: &'static str,
}

/// The persisted blob kind of `workflow_hot.state_data`, as errors name it.
const WORKFLOW_STATE_KIND: &str = "workflow_hot.state_data";

/// Serialize the current materialized workflow state for `workflow_hot`: the
/// envelope, the state, then its state extension when one has data.
///
/// A state with nothing to carry in an extension encodes to exactly the bytes
/// Tokeira 0.2.0–0.5.1 write for it.
pub fn encode_workflow_state(state: &WorkflowState) -> Result<Vec<u8>> {
    let mut bytes = encode_enveloped(WORKFLOW_STATE_ENVELOPE_VERSION, state)?;
    bytes.extend(encode_state_extension(state)?);
    Ok(bytes)
}

/// The size [`encode_workflow_state`] writes for `state`, measured without
/// encoding the state.
pub fn workflow_state_encoded_len(state: &WorkflowState) -> Result<usize> {
    let enveloped =
        postcard::experimental::serialized_size(&(WORKFLOW_STATE_ENVELOPE_VERSION, state))?;
    Ok(enveloped + encode_state_extension(state)?.len())
}

/// The measured state of `run-growth-limits` (criterion 2.9): the state's
/// encoded size less each activity's encoded input, which v1.31.0 keeps only in
/// history. An input encodes inside the state exactly as it does alone.
pub fn measured_state_len(encoded_state_len: usize, state: &WorkflowState) -> Result<usize> {
    let mut inputs = 0usize;
    for activity in state.activities.values() {
        inputs = inputs.saturating_add(postcard::experimental::serialized_size(&activity.input)?);
    }
    Ok(encoded_state_len.saturating_sub(inputs))
}

/// Deserialize the authoritative hot-state snapshot for one run.
///
/// Returns [`BlobFormatError`] for a blob without the envelope and
/// [`StateExtensionError`] for a malformed extension; it never returns a value
/// decoded from such bytes.
pub fn decode_workflow_state(run_key: RunKey, bytes: &[u8]) -> Result<WorkflowState> {
    let payload = envelope_payload(
        WORKFLOW_STATE_KIND,
        WORKFLOW_STATE_ENVELOPE_VERSION,
        run_key,
        bytes,
    )?;
    let (mut state, extension) = postcard::take_from_bytes::<WorkflowState>(payload)?;
    if !extension.is_empty() {
        apply_state_extension(&mut state, extension).map_err(|defect| StateExtensionError {
            kind: WORKFLOW_STATE_KIND,
            run_key,
            defect,
        })?;
    }
    Ok(state)
}

/// The state extension for `state`, or no bytes when no section has data.
pub(crate) fn encode_state_extension(state: &WorkflowState) -> postcard::Result<Vec<u8>> {
    // `activities` is a `BTreeMap`, so entries come out in activity-id order.
    let heartbeats = state
        .activities
        .iter()
        .filter_map(|(activity_id, activity)| {
            activity
                .last_heartbeat_at
                .map(|last_heartbeat_at| ActivityHeartbeat {
                    activity_id: activity_id.clone(),
                    last_heartbeat_at,
                })
        })
        .collect::<Vec<_>>();
    let mut sections = Vec::new();
    if !heartbeats.is_empty() {
        sections.push(ExtensionSection {
            tag: ACTIVITY_HEARTBEAT_SECTION,
            payload: postcard::to_allocvec(&heartbeats)?,
        });
    }
    encode_extension(WORKFLOW_STATE_EXTENSION_MAGIC, &sections)
}

/// Apply a state extension to the state decoded from the same blob or run.
///
/// Returns the defect when the extension is malformed; the caller must then
/// discard `state`.
pub(crate) fn apply_state_extension(
    state: &mut WorkflowState,
    bytes: &[u8],
) -> std::result::Result<(), &'static str> {
    for section in decode_extension(WORKFLOW_STATE_EXTENSION_MAGIC, bytes)? {
        if section.tag != ACTIVITY_HEARTBEAT_SECTION {
            // A later release's section: its data is safe to drop by rule.
            continue;
        }
        let heartbeats = decode_exact::<Vec<ActivityHeartbeat>>(&section.payload)
            .ok_or("undecodable heartbeat section")?;
        let mut listed = BTreeSet::new();
        if !heartbeats
            .iter()
            .all(|heartbeat| listed.insert(heartbeat.activity_id.as_str()))
        {
            return Err("activity listed twice in the heartbeat section");
        }
        for heartbeat in heartbeats {
            // An activity the state does not hold is skipped; the writer never
            // lists one, and dropping it is safe.
            if let Some(activity) = state.activities.get_mut(&heartbeat.activity_id) {
                activity.last_heartbeat_at = Some(heartbeat.last_heartbeat_at);
            }
        }
    }
    Ok(())
}

/// `magic` followed by `sections`, or no bytes when there are no sections.
pub(crate) fn encode_extension(
    magic: u32,
    sections: &[ExtensionSection],
) -> postcard::Result<Vec<u8>> {
    if sections.is_empty() {
        return Ok(Vec::new());
    }
    postcard::to_allocvec(&(magic, sections))
}

/// Split extension bytes into their sections, checking the framing: the magic,
/// a decodable section sequence that consumes every byte, and no repeated tag.
pub(crate) fn decode_extension(
    magic: u32,
    bytes: &[u8],
) -> std::result::Result<Vec<ExtensionSection>, &'static str> {
    let (found, rest) =
        postcard::take_from_bytes::<u32>(bytes).map_err(|_| "truncated extension magic")?;
    if found != magic {
        return Err("unknown extension magic");
    }
    let (sections, rest) = postcard::take_from_bytes::<Vec<ExtensionSection>>(rest)
        .map_err(|_| "undecodable extension sections")?;
    if !rest.is_empty() {
        return Err("bytes after the extension sections");
    }
    let mut tags = BTreeSet::new();
    if !sections.iter().all(|section| tags.insert(section.tag)) {
        return Err("repeated extension section tag");
    }
    Ok(sections)
}

/// Decode a value that must consume `bytes` exactly.
pub(crate) fn decode_exact<T: DeserializeOwned>(bytes: &[u8]) -> Option<T> {
    match postcard::take_from_bytes::<T>(bytes) {
        Ok((value, [])) => Some(value),
        _ => None,
    }
}

/// Serialize one committed history batch.
///
/// Batches are encoded as a vector because DSQL row limits and transaction shape are
/// controlled by the commit path, not by individual event rows.
pub fn encode_history_events(events: &[HistoryEvent]) -> Result<Vec<u8>> {
    encode_enveloped(HISTORY_BATCH_ENVELOPE_VERSION, &events)
}

/// Deserialize a committed history batch.
///
/// Returns [`BlobFormatError`] for a blob without the envelope.
pub fn decode_history_events(run_key: RunKey, bytes: &[u8]) -> Result<Vec<HistoryEvent>> {
    decode_enveloped(
        "history_batch.events_data",
        HISTORY_BATCH_ENVELOPE_VERSION,
        run_key,
        bytes,
    )
}

/// Encoded size of one history batch as the stores persist it, envelope included.
///
/// Both stores add this number to the run's History Size in the commit that writes
/// the batch, so the statistic has exactly one definition
/// (`ExecutionStats.HistorySize` in `mutable_state_impl.go:6610-6616 @ v1.31.0` is
/// likewise the store's own encoded size).
pub fn history_batch_encoded_len(events: &[HistoryEvent]) -> Result<i64> {
    let encoded = encode_history_events(events)?;
    Ok(i64::try_from(encoded.len()).unwrap_or(i64::MAX))
}

fn encode_enveloped<T: Serialize>(version: u32, payload: &T) -> Result<Vec<u8>> {
    // A `(version, payload)` pair encodes as the version varint followed by the
    // payload bytes, which is exactly what `decode_enveloped` peels apart.
    encode(&(version, payload))
}

fn decode_enveloped<T: DeserializeOwned>(
    kind: &'static str,
    version: u32,
    run_key: RunKey,
    bytes: &[u8],
) -> Result<T> {
    decode(envelope_payload(kind, version, run_key, bytes)?)
}

/// The bytes after a blob's envelope version, once the version matches.
fn envelope_payload<'a>(
    kind: &'static str,
    version: u32,
    run_key: RunKey,
    bytes: &'a [u8],
) -> Result<&'a [u8]> {
    // The version is checked before the payload is touched, so a pre-envelope blob
    // is rejected on its first varint and never partially decoded.
    let observed = postcard::take_from_bytes::<u32>(bytes).ok();
    match observed {
        Some((found, rest)) if found == version => Ok(rest),
        Some((found, _)) => Err(BlobFormatError {
            kind,
            run_key,
            observed: found,
        }
        .into()),
        None => Err(BlobFormatError {
            kind,
            run_key,
            observed: 0,
        }
        .into()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use proptest::prelude::*;
    use time::{Duration, OffsetDateTime};
    use tokeira_kernel::{
        ActivityPauseInfo, ActivityState, HistoryEventKind, PendingWorkflowTask, Priority,
        TimerState, WorkflowTaskType,
    };
    use tokeira_types::{
        BuildId, DeploymentId, ExecutionStatus, Headers, LogicalTaskSeq, Memo, NamespaceId,
        Payload, Payloads, RetryPolicy, RunId, SearchAttributes, TaskQueueName, TransitionSeq,
        WorkerIdentity, WorkflowId, WorkflowType,
    };
    use uuid::Uuid;

    use super::*;

    fn run_key() -> RunKey {
        RunKey(Uuid::from_u128(7))
    }

    fn events(count: usize) -> Vec<HistoryEvent> {
        (1..=count)
            .map(|event_id| HistoryEvent {
                event_id: event_id as i64,
                happened_at: OffsetDateTime::UNIX_EPOCH,
                kind: HistoryEventKind::WorkflowTaskScheduled {
                    logical_seq: tokeira_types::LogicalTaskSeq(event_id as u64),
                    task_queue: tokeira_types::TaskQueueName(format!("queue-{event_id}")),
                    workflow_task_timeout: time::Duration::seconds(10),
                    attempt: 1,
                },
            })
            .collect()
    }

    #[test]
    fn pre_envelope_batch_is_rejected_with_its_leading_count() {
        // Feature: continue-as-new-advice, Property 9: envelopes round-trip and
        // reject pre-envelope blobs — the 0.1.2 layout was a bare `Vec<HistoryEvent>`.
        let batch = events(3);
        let bare = encode(&batch).expect("bare batch encodes");
        let error = decode_history_events(run_key(), &bare)
            .expect_err("bare batch must not decode as an envelope")
            .downcast::<BlobFormatError>()
            .expect("a format error, not a partial decode");
        assert_eq!(error.kind, "history_batch.events_data");
        assert_eq!(error.run_key, run_key());
        assert_eq!(error.observed, 3);
        assert!(error.to_string().contains("recreate the cluster"));
        assert!(
            decode_history_events(run_key(), &[])
                .expect_err("empty input is not an envelope")
                .downcast::<BlobFormatError>()
                .is_ok_and(|error| error.observed == 0)
        );
    }

    #[test]
    fn history_batch_encoded_len_is_the_enveloped_size() {
        let batch = events(2);
        let encoded = encode_history_events(&batch).expect("encodes");
        assert_eq!(
            history_batch_encoded_len(&batch).expect("size"),
            i64::try_from(encoded.len()).expect("fits")
        );
        assert!(history_batch_encoded_len(&[]).expect("size") > 0);
    }

    proptest! {
        // Feature: continue-as-new-advice, Property 9: envelopes round-trip and
        // reject pre-envelope blobs
        #[test]
        fn history_batches_round_trip_and_reject_other_versions(
            count in 0usize..8,
            other_version in any::<u32>(),
        ) {
            prop_assume!(other_version != HISTORY_BATCH_ENVELOPE_VERSION);
            let batch = events(count);
            let encoded = encode_history_events(&batch).expect("encodes");
            prop_assert_eq!(decode_history_events(run_key(), &encoded).expect("decodes"), batch.clone());

            let restamped = encode(&(other_version, &batch)).expect("restamped encodes");
            let error = decode_history_events(run_key(), &restamped)
                .expect_err("other versions are rejected")
                .downcast::<BlobFormatError>()
                .expect("format error");
            prop_assert_eq!(error.observed, other_version);
        }
    }

    // ---- State extension (activity-heartbeat-time) ----

    fn at(offset_nanos: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp_nanos(
            1_700_000_000_000_000_000 + i128::from(offset_nanos),
        )
        .expect("in range")
    }

    fn payload(data: &str) -> Payload {
        Payload {
            data: data.as_bytes().to_vec(),
            metadata: BTreeMap::from([("encoding".to_owned(), "json/plain".to_owned())]),
            external_payloads: Vec::new(),
        }
    }

    /// An activity with every optional field set, so the layout fixture covers
    /// each of them.
    fn activity(activity_id: &str) -> ActivityState {
        ActivityState {
            activity_id: activity_id.to_owned(),
            activity_type: "activity-type".to_owned(),
            schedule_event_id: 5,
            task_queue: TaskQueueName("activity-queue".to_owned()),
            deployment: Some(DeploymentId("deployment".to_owned())),
            build_id: Some(BuildId("build".to_owned())),
            input: Payloads(vec![payload("input")]),
            header: Some(Headers(BTreeMap::from([(
                "trace".to_owned(),
                payload("header"),
            )]))),
            attempt: 3,
            retry_policy: Some(RetryPolicy {
                initial_interval: Duration::seconds(1),
                backoff_coefficient: 2.0,
                maximum_interval: Some(Duration::seconds(60)),
                maximum_attempts: 5,
                non_retryable_error_types: vec!["Fatal".to_owned()],
            }),
            schedule_to_close_timeout: Some(Duration::seconds(300)),
            schedule_to_start_timeout: Some(Duration::seconds(30)),
            start_to_close_timeout: Some(Duration::seconds(120)),
            heartbeat_timeout: Some(Duration::seconds(10)),
            scheduled_at: at(0),
            current_attempt_scheduled_at: Some(at(2_000_000_000)),
            started_at: Some(at(3_000_000_000)),
            started_event_id: Some(7),
            last_failure: Some(payload("failure")),
            started_identity: Some(WorkerIdentity("worker-a".to_owned())),
            retry_last_worker_identity: Some(WorkerIdentity("worker-b".to_owned())),
            heartbeat_details: Some(Payloads(vec![payload("progress")])),
            cancel_requested: true,
            pause_info: Some(ActivityPauseInfo {
                pause_time: at(4_000_000_000),
                identity: "operator".to_owned(),
                reason: "maintenance".to_owned(),
                rule_id: Some("rule".to_owned()),
            }),
            stamp: 9,
            activity_reset: true,
            reset_heartbeats: true,
            priority: Some(Priority {
                priority_key: 2,
                fairness_key: "tenant".to_owned(),
                fairness_weight: 1.5,
            }),
            last_attempt_complete_time: Some(at(1_000_000_000)),
            last_heartbeat_at: None,
        }
    }

    /// A deterministic run holding one fully populated activity, a timer and a
    /// started workflow task.
    fn layout_state() -> WorkflowState {
        WorkflowState {
            completed_update_count: 1,
            run_key: run_key(),
            namespace_id: NamespaceId(Uuid::from_u128(11)),
            workflow_id: WorkflowId("workflow".into()),
            run_id: RunId(Uuid::from_u128(12)),
            workflow_type: WorkflowType("wf".into()),
            task_queue: TaskQueueName("queue".into()),
            deployment: None,
            build_id: None,
            status: ExecutionStatus::Running,
            transition_seq: TransitionSeq(4),
            last_event_id: 7,
            external_payload_count: 0,
            external_payload_size_bytes: 0,
            next_workflow_task_seq: LogicalTaskSeq(2),
            pending_workflow_task: Some(PendingWorkflowTask {
                advice: Default::default(),
                task_type: WorkflowTaskType::Normal,
                schedule_to_start_deadline: None,
                logical_seq: LogicalTaskSeq(1),
                scheduled_event_id: 2,
                scheduled_at: at(0),
                started_event_id: Some(3),
                started_at: Some(at(500_000_000)),
                attempt: 1,
                target_worker_deployment_version_changed: false,
                target_version_changed_enabled: false,
                target_deployment_version: None,
            }),
            previous_started_event_id: 3,
            workflow_task_attempt: 1,
            workflow_task_attempts_since_last_success: 0,
            last_workflow_task_problem: None,
            sticky: None,
            pause_info: None,
            cancel_requested: false,
            wft_stamp: 1,
            memo: Memo::default(),
            search_attributes: SearchAttributes::default(),
            workflow_execution_timeout: Some(Duration::hours(1)),
            workflow_run_timeout: Some(Duration::minutes(30)),
            workflow_task_timeout: Duration::seconds(10),
            retry_policy: None,
            attempt: 1,
            first_execution_run_id: None,
            original_execution_run_id: None,
            reset_run_id: None,
            parent_run_key: None,
            parent_workflow_id: None,
            parent_run_id: None,
            parent_namespace_id: None,
            parent_namespace_name: None,
            parent_initiated_event_id: 0,
            root_workflow_id: None,
            root_run_id: None,
            last_completion_result: None,
            activities: BTreeMap::from([("activity-a".to_owned(), activity("activity-a"))]),
            timers: BTreeMap::from([(
                "timer-a".to_owned(),
                TimerState {
                    timer_id: "timer-a".to_owned(),
                    started_event_id: 6,
                    fire_at: at(60_000_000_000),
                },
            )]),
            children: Default::default(),
            pending_external_signals: Default::default(),
            pending_external_cancels: Default::default(),
            pending_updates: Default::default(),
            admitted_updates: Default::default(),
            pending_nexus_operations: Default::default(),
            versioning_info: None,
            worker_deployment_name: None,
            completion_callbacks: Vec::new(),
            user_metadata: None,
            links: Vec::new(),
            workflow_start_delay: None,
            priority: None,
            started_at: at(0),
            first_run_started_at: None,
            closed_at: None,
            close_result: None,
            close_failure: None,
            request_id_infos: BTreeMap::new(),
            buffered_events: Vec::new(),
            auto_reset_points: Vec::new(),
        }
    }

    /// What Tokeira 0.2.0–0.5.1 write for `state`: the envelope, then the state.
    fn released_encoding(state: &WorkflowState) -> Vec<u8> {
        encode(&(WORKFLOW_STATE_ENVELOPE_VERSION, state)).expect("encodes")
    }

    /// The hot-state decoder of Tokeira 0.2.0–0.5.1 (`decode_enveloped` at
    /// v0.5.1): the version, then `postcard::from_bytes`, which ignores any bytes
    /// after the state.
    fn released_decode(bytes: &[u8]) -> WorkflowState {
        let (version, rest) = postcard::take_from_bytes::<u32>(bytes).expect("version");
        assert_eq!(version, WORKFLOW_STATE_ENVELOPE_VERSION);
        postcard::from_bytes(rest).expect("state")
    }

    fn without_times(mut state: WorkflowState) -> WorkflowState {
        for activity in state.activities.values_mut() {
            activity.last_heartbeat_at = None;
        }
        state
    }

    fn heartbeat_section(entries: &[(&str, OffsetDateTime)]) -> ExtensionSection {
        let entries = entries
            .iter()
            .map(|(activity_id, last_heartbeat_at)| ActivityHeartbeat {
                activity_id: (*activity_id).to_owned(),
                last_heartbeat_at: *last_heartbeat_at,
            })
            .collect::<Vec<_>>();
        ExtensionSection {
            tag: ACTIVITY_HEARTBEAT_SECTION,
            payload: postcard::to_allocvec(&entries).expect("encodes"),
        }
    }

    fn with_extension(state: &WorkflowState, extension: &[u8]) -> Vec<u8> {
        let mut bytes = released_encoding(state);
        bytes.extend_from_slice(extension);
        bytes
    }

    /// States whose activities carry any combination of heartbeat times, with
    /// nanosecond precision.
    fn arb_state() -> impl Strategy<Value = WorkflowState> {
        proptest::collection::btree_map(
            "[a-z]{1,8}",
            proptest::option::of(-86_400_000_000_000i64..86_400_000_000_000),
            0..5,
        )
        .prop_map(|activities| {
            let mut state = layout_state();
            state.activities = activities
                .into_iter()
                .map(|(activity_id, time)| {
                    let mut entry = activity(&activity_id);
                    entry.last_heartbeat_at = time.map(at);
                    (activity_id, entry)
                })
                .collect();
            state
        })
    }

    /// One way to make the bytes after a state not a well-formed extension.
    #[derive(Clone, Debug)]
    enum Malformed {
        WrongMagic(u32),
        Truncated(prop::sample::Index),
        Trailing(Vec<u8>),
        RepeatedTag,
        TruncatedHeartbeats(prop::sample::Index),
        OverlongHeartbeats(u8),
        ActivityListedTwice,
    }

    fn arb_malformed() -> impl Strategy<Value = Malformed> {
        prop_oneof![
            any::<u32>()
                .prop_filter("not the magic", |magic| *magic
                    != WORKFLOW_STATE_EXTENSION_MAGIC)
                .prop_map(Malformed::WrongMagic),
            any::<prop::sample::Index>().prop_map(Malformed::Truncated),
            prop::collection::vec(any::<u8>(), 1..8).prop_map(Malformed::Trailing),
            Just(Malformed::RepeatedTag),
            any::<prop::sample::Index>().prop_map(Malformed::TruncatedHeartbeats),
            any::<u8>().prop_map(Malformed::OverlongHeartbeats),
            Just(Malformed::ActivityListedTwice),
        ]
    }

    fn malformed_extension(malformed: &Malformed) -> Vec<u8> {
        let valid = heartbeat_section(&[("activity-a", at(5))]);
        let framed = |magic: u32, sections: &[ExtensionSection]| {
            postcard::to_allocvec(&(magic, sections)).expect("encodes")
        };
        match malformed {
            Malformed::WrongMagic(magic) => framed(*magic, std::slice::from_ref(&valid)),
            Malformed::Truncated(cut) => {
                let full = framed(WORKFLOW_STATE_EXTENSION_MAGIC, std::slice::from_ref(&valid));
                // A non-empty proper prefix: empty bytes mean "no extension".
                full[..1 + cut.index(full.len() - 1)].to_vec()
            }
            Malformed::Trailing(extra) => {
                let mut bytes =
                    framed(WORKFLOW_STATE_EXTENSION_MAGIC, std::slice::from_ref(&valid));
                bytes.extend_from_slice(extra);
                bytes
            }
            Malformed::RepeatedTag => framed(
                WORKFLOW_STATE_EXTENSION_MAGIC,
                &[valid.clone(), valid.clone()],
            ),
            Malformed::TruncatedHeartbeats(cut) => {
                let mut section = valid.clone();
                section.payload.truncate(cut.index(section.payload.len()));
                framed(WORKFLOW_STATE_EXTENSION_MAGIC, &[section])
            }
            Malformed::OverlongHeartbeats(extra) => {
                let mut section = valid.clone();
                section.payload.push(*extra);
                framed(WORKFLOW_STATE_EXTENSION_MAGIC, &[section])
            }
            Malformed::ActivityListedTwice => framed(
                WORKFLOW_STATE_EXTENSION_MAGIC,
                &[heartbeat_section(&[
                    ("activity-a", at(5)),
                    ("activity-a", at(6)),
                ])],
            ),
        }
    }

    // The State_Layout as Tokeira 0.2.0–0.5.1 write it for `layout_state()`.
    // The kernel's state types are unchanged since v0.5.1, so these bytes are
    // what that release writes. A change to `WorkflowState`'s positional layout
    // changes them, and must take a new envelope version instead
    // (activity-heartbeat-time, Requirement 1.11).
    const FROZEN_LAYOUT_HEX: &str = concat!(
        "d3aeada20510000000000000000000000000000000071000000000000000000000000000",
        "00000b08776f726b666c6f77100000000000000000000000000000000c02776605717565",
        "75650000000000040e00000102010104ce1fbe02160d1400000000010601ce1fbe02160d",
        "1480cab5ee010000000100000000000000000601000000000001000001a0380001901c00",
        "14000001000000000000000000000000010a61637469766974792d610a61637469766974",
        "792d610d61637469766974792d747970650a0e61637469766974792d7175657565010a64",
        "65706c6f796d656e7401056275696c640105696e7075740108656e636f64696e670a6a73",
        "6f6e2f706c61696e000101057472616365066865616465720108656e636f64696e670a6a",
        "736f6e2f706c61696e00030102000000000000000040017800050105466174616c01d804",
        "00013c0001f00100011400ce1fbe02160d140000000001ce1fbe02160d160000000001ce",
        "1fbe02160d1700000000010e01076661696c7572650108656e636f64696e670a6a736f6e",
        "2f706c61696e000108776f726b65722d610108776f726b65722d6201010870726f677265",
        "73730108656e636f64696e670a6a736f6e2f706c61696e000101ce1fbe02160d18000000",
        "00086f70657261746f720b6d61696e74656e616e6365010472756c650901010104067465",
        "6e616e740000c03f01ce1fbe02160d1500000000010774696d65722d610774696d65722d",
        "610cce1fbe02160e14000000000000000000000000000000ce1fbe02160d140000000000",
        "000000000000",
    );

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn workflow_state_layout_is_frozen() {
        let state = layout_state();
        let encoded = encode_workflow_state(&state).expect("encodes");
        assert_eq!(hex(&encoded), FROZEN_LAYOUT_HEX);
        assert_eq!(encoded, released_encoding(&state));
        assert_eq!(
            decode_workflow_state(run_key(), &encoded).expect("decodes"),
            state
        );
    }

    #[test]
    fn heartbeat_time_travels_in_the_extension_only() {
        let mut state = layout_state();
        let plain = encode_workflow_state(&state).expect("encodes");
        state
            .activities
            .get_mut("activity-a")
            .expect("activity")
            .last_heartbeat_at = Some(at(7));
        let extended = encode_workflow_state(&state).expect("encodes");
        // The state's bytes are unchanged; the time follows them.
        assert_eq!(&extended[..plain.len()], plain.as_slice());
        let extension = encode_extension(
            WORKFLOW_STATE_EXTENSION_MAGIC,
            &[heartbeat_section(&[("activity-a", at(7))])],
        )
        .expect("encodes");
        assert_eq!(&extended[plain.len()..], extension.as_slice());
    }

    #[cfg(feature = "dsql")]
    #[test]
    fn activity_side_table_blob_carries_no_heartbeat_time() {
        // `activity_state.state_data` keeps its layout: the skipped field adds
        // nothing (activity-heartbeat-time, Requirement 3.6).
        let plain = activity("activity-a");
        let mut timed = plain.clone();
        timed.last_heartbeat_at = Some(at(9));
        assert_eq!(
            crate::dsql::codec::encode_activity_state(&timed).expect("encodes"),
            crate::dsql::codec::encode_activity_state(&plain).expect("encodes"),
        );
    }

    #[test]
    fn malformed_extension_error_names_the_blob_and_run() {
        let error =
            decode_workflow_state(run_key(), &with_extension(&layout_state(), &[0xff, 0x01]))
                .expect_err("garbage after the state is refused")
                .downcast::<StateExtensionError>()
                .expect("an extension error");
        assert_eq!(error.kind, "workflow_hot.state_data");
        assert_eq!(error.run_key, run_key());
        let message = error.to_string();
        assert!(message.contains("workflow_hot.state_data"));
        assert!(message.contains(&run_key().0.to_string()));
    }

    proptest! {
        // Feature: activity-heartbeat-time, Property 1: Hot-state blobs round-trip
        // with their times
        #[test]
        fn property_hot_state_blobs_round_trip_with_their_times(state in arb_state()) {
            let encoded = encode_workflow_state(&state).expect("encodes");
            prop_assert_eq!(decode_workflow_state(run_key(), &encoded).expect("decodes"), state);
        }

        // Feature: activity-heartbeat-time, Property 2: Without times, the bytes are
        // unchanged
        #[test]
        fn property_without_times_the_bytes_are_unchanged(state in arb_state()) {
            let state = without_times(state);
            let encoded = encode_workflow_state(&state).expect("encodes");
            prop_assert_eq!(&encoded, &released_encoding(&state));
            prop_assert_eq!(decode_workflow_state(run_key(), &encoded).expect("decodes"), state);
        }

        // Feature: activity-heartbeat-time, Property 3: Pre-extension readers see the
        // state
        #[test]
        fn property_pre_extension_readers_see_the_state(state in arb_state()) {
            let encoded = encode_workflow_state(&state).expect("encodes");
            prop_assert_eq!(released_decode(&encoded), without_times(state));
        }

        // Feature: activity-heartbeat-time, Property 4: Malformed extensions are
        // rejected
        #[test]
        fn property_malformed_extensions_are_rejected(
            state in arb_state(),
            malformed in arb_malformed(),
        ) {
            let bytes = with_extension(&state, &malformed_extension(&malformed));
            let error = decode_workflow_state(run_key(), &bytes)
                .expect_err("a malformed extension is refused")
                .downcast::<StateExtensionError>()
                .expect("an extension error, not a decoded state");
            prop_assert_eq!(error.kind, "workflow_hot.state_data");
            prop_assert_eq!(error.run_key, run_key());
        }

        // Feature: activity-heartbeat-time, Property 5: Unknown sections and unknown
        // activities are ignored
        #[test]
        fn property_unknown_sections_and_activities_are_ignored(
            state in arb_state(),
            unknown in proptest::collection::btree_map(
                2u32..64,
                proptest::collection::vec(any::<u8>(), 0..16),
                0..3,
            ),
            ghosts in proptest::collection::btree_set("[A-Z]{1,8}", 0..3),
        ) {
            // Ghost ids are upper-case, so the state (lower-case ids) holds none.
            let mut entries = state
                .activities
                .iter()
                .filter_map(|(activity_id, activity)| {
                    activity.last_heartbeat_at.map(|at| (activity_id.as_str(), at))
                })
                .collect::<Vec<_>>();
            entries.extend(ghosts.iter().map(|ghost| (ghost.as_str(), at(1))));
            let mut sections = Vec::new();
            if !entries.is_empty() {
                sections.push(heartbeat_section(&entries));
            }
            sections.extend(unknown.into_iter().map(|(tag, payload)| ExtensionSection {
                tag,
                payload,
            }));
            let extension = encode_extension(WORKFLOW_STATE_EXTENSION_MAGIC, &sections)
                .expect("encodes");
            let bytes = with_extension(&without_times(state.clone()), &extension);
            prop_assert_eq!(decode_workflow_state(run_key(), &bytes).expect("decodes"), state);
        }
    }
}
