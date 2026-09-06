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
//! [`history_batch_encoded_len`] is the one definition of a batch's persisted size.
//! The DSQL repository and the in-memory store both account the per-run History Size
//! with it, so the number a workflow task is told, the number `DescribeWorkflowExecution`
//! reports, and the visibility `HistorySizeBytes` attribute cannot drift apart
//! (Requirement 1.4).
//!
//! This module is deliberately outside the `dsql` feature: the in-memory store needs
//! the same envelopes and size function.

use anyhow::Result;
use serde::{Serialize, de::DeserializeOwned};
use tokeira_kernel::{HistoryEvent, WorkflowState};
use tokeira_types::RunKey;

/// Magic prefix of an enveloped `workflow_hot.state_data` blob (`"TKWS"`).
pub const WORKFLOW_STATE_ENVELOPE_VERSION: u32 = 0x544B_5753;

/// Magic prefix of an enveloped `history_batch.events_data` blob (`"TKHB"`).
pub const HISTORY_BATCH_ENVELOPE_VERSION: u32 = 0x544B_4842;

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

/// Serialize the current materialized workflow state for `workflow_hot`.
pub fn encode_workflow_state(state: &WorkflowState) -> Result<Vec<u8>> {
    encode_enveloped(WORKFLOW_STATE_ENVELOPE_VERSION, state)
}

/// Deserialize the authoritative hot-state snapshot for one run.
///
/// Returns [`BlobFormatError`] for a blob without the envelope; it never returns a
/// value decoded from pre-envelope bytes.
pub fn decode_workflow_state(run_key: RunKey, bytes: &[u8]) -> Result<WorkflowState> {
    decode_enveloped(
        "workflow_hot.state_data",
        WORKFLOW_STATE_ENVELOPE_VERSION,
        run_key,
        bytes,
    )
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
    // The version is checked before the payload is touched, so a pre-envelope blob
    // is rejected on its first varint and never partially decoded.
    let observed = postcard::take_from_bytes::<u32>(bytes).ok();
    match observed {
        Some((found, rest)) if found == version => decode(rest),
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
    use proptest::prelude::*;
    use time::OffsetDateTime;
    use tokeira_kernel::HistoryEventKind;
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
}
