//! Protobuf-encoded sizes of the run's stored memo and search attributes, as
//! Temporal v1.31.0 measures them (`workflow-task-command-limits` criterion
//! 2.12), and of the payloads a buffered event carries (`run-growth-limits`
//! criterion 2.9).
//!
//! The edge measures what a command carries on the proto it received. Only an
//! upsert's merged map and the buffered events need the run's stored values,
//! which the kernel holds as domain types, so this mirrors protobuf's encoding
//! of `temporal.api.common.v1.Payload`, `Payloads`, `Header`, `Memo` and
//! `SearchAttributes` for them.

use std::collections::BTreeMap;

use time::format_description::well_known::Rfc3339;
use tokeira_types::{Headers, Memo, Payload, Payloads, SearchAttrValue, SearchAttributes};

use crate::{
    command::{FieldChange, MemoPatch, PayloadSize, SearchAttributesPatch},
    event::HistoryEventKind,
};

/// The length of `value` as a protobuf varint.
fn varint_len(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

/// A length-delimited field with a one-byte tag (field numbers 1-15). Proto3
/// omits an empty one.
fn len_field(len: usize) -> usize {
    if len == 0 {
        0
    } else {
        1 + varint_len(len as u64) + len
    }
}

/// One entry of a `map<string, V>` field numbered 1-15 whose values are
/// length-delimited: the entry message (key field 1, value field 2) wrapped
/// in the map field.
fn map_entry(key_len: usize, value_len: usize) -> usize {
    let entry = len_field(key_len) + len_field(value_len);
    1 + varint_len(entry as u64) + entry
}

/// A payload's encoded size: its metadata (field 1, `map<string, bytes>`), its
/// data (field 2) and its external payload details (field 3).
pub fn payload_encoded_len(payload: &Payload) -> usize {
    let metadata: usize = payload
        .metadata
        .iter()
        .map(|(key, value)| map_entry(key.len(), value.len()))
        .sum();
    let external: usize = payload
        .external_payloads
        .iter()
        .map(|detail| {
            // `size_bytes` is an int64 (field 1), omitted when zero.
            let fields = if detail.size_bytes == 0 {
                0
            } else {
                1 + varint_len(detail.size_bytes as u64)
            };
            1 + varint_len(fields as u64) + fields
        })
        .sum();
    metadata + len_field(payload.data.len()) + external
}

/// A `Payloads` message's encoded size: each payload in its repeated field 1,
/// written even when empty.
pub fn payloads_encoded_len(payloads: &Payloads) -> usize {
    payloads
        .0
        .iter()
        .map(|payload| {
            let len = payload_encoded_len(payload);
            1 + varint_len(len as u64) + len
        })
        .sum()
}

/// A `Header` message's encoded size: its fields (field 1, `map<string, Payload>`).
pub fn headers_encoded_len(headers: &Headers) -> usize {
    headers
        .0
        .iter()
        .map(|(key, payload)| map_entry(key.len(), payload_encoded_len(payload)))
        .sum()
}

/// A failure's encoded size. The kernel keeps a failure as a payload whose data
/// is the encoded `Failure`, which is what v1.31.0's event holds.
fn failure_len(failure: &Payload) -> usize {
    failure.data.len()
}

/// The size of the payloads a buffered event carries: its input, result,
/// details, failure and header. v1.31.0 sums each buffered event's whole
/// encoded size (`SizeInBytesOfBufferedEvents`, event_store.go:120-129 @
/// v1.31.0). The kernel holds domain events, and their payloads are what make
/// them large, so this errs low by each event's other fields.
///
/// The match is exhaustive, so a new event kind doesn't compile until it is
/// measured here.
pub fn buffered_event_payload_size(kind: &HistoryEventKind) -> usize {
    match kind {
        HistoryEventKind::WorkflowExecutionSignaled { input, header, .. } => {
            payloads_encoded_len(input) + header.as_ref().map_or(0, headers_encoded_len)
        }
        HistoryEventKind::ActivityTaskStarted { last_failure, .. } => {
            last_failure.as_ref().map_or(0, failure_len)
        }
        HistoryEventKind::ActivityTaskCompleted { result, .. }
        | HistoryEventKind::ChildWorkflowExecutionCompleted { result, .. }
        | HistoryEventKind::NexusOperationCompleted { result, .. } => payloads_encoded_len(result),
        HistoryEventKind::ActivityTaskFailed { failure, .. }
        | HistoryEventKind::ChildWorkflowExecutionFailed { failure, .. }
        | HistoryEventKind::NexusOperationFailed { failure, .. }
        | HistoryEventKind::NexusOperationCancelRequestFailed { failure, .. }
        | HistoryEventKind::WorkflowExecutionUpdateRejected { failure, .. } => failure_len(failure),
        HistoryEventKind::ActivityTaskTimedOut { failure, .. } => {
            failure.as_ref().map_or(0, failure_len)
        }
        HistoryEventKind::ActivityTaskCanceled { details, .. }
        | HistoryEventKind::ChildWorkflowExecutionCanceled { details, .. } => {
            details.as_ref().map_or(0, payloads_encoded_len)
        }
        HistoryEventKind::ChildWorkflowExecutionStarted { header, .. } => {
            header.as_ref().map_or(0, headers_encoded_len)
        }
        HistoryEventKind::WorkflowExecutionUpdateAdmitted { input, .. } => {
            payloads_encoded_len(input)
        }
        // Buffered kinds that carry no payload.
        HistoryEventKind::TimerFired { .. }
        | HistoryEventKind::WorkflowExecutionCancelRequested { .. }
        | HistoryEventKind::StartChildWorkflowExecutionFailed { .. }
        | HistoryEventKind::ChildWorkflowExecutionTerminated { .. }
        | HistoryEventKind::ChildWorkflowExecutionTimedOut { .. }
        | HistoryEventKind::ExternalWorkflowExecutionSignaled { .. }
        | HistoryEventKind::SignalExternalWorkflowExecutionFailed { .. }
        | HistoryEventKind::ExternalWorkflowExecutionCancelRequested { .. }
        | HistoryEventKind::RequestCancelExternalWorkflowExecutionFailed { .. }
        | HistoryEventKind::NexusOperationStarted { .. }
        | HistoryEventKind::NexusOperationCanceled { .. }
        | HistoryEventKind::NexusOperationTimedOut { .. }
        | HistoryEventKind::NexusOperationCancelRequestCompleted { .. }
        | HistoryEventKind::WorkflowExecutionPaused { .. }
        | HistoryEventKind::WorkflowExecutionUnpaused { .. }
        | HistoryEventKind::WorkflowExecutionOptionsUpdated { .. } => 0,
        // Kinds that never buffer.
        HistoryEventKind::WorkflowExecutionStarted { .. }
        | HistoryEventKind::WorkflowExecutionStartedV2 { .. }
        | HistoryEventKind::WorkflowExecutionCompleted { .. }
        | HistoryEventKind::WorkflowExecutionFailed { .. }
        | HistoryEventKind::WorkflowExecutionTimedOut { .. }
        | HistoryEventKind::WorkflowExecutionTerminated { .. }
        | HistoryEventKind::WorkflowExecutionContinuedAsNew { .. }
        | HistoryEventKind::WorkflowExecutionCanceled { .. }
        | HistoryEventKind::WorkflowTaskScheduled { .. }
        | HistoryEventKind::WorkflowTaskStarted { .. }
        | HistoryEventKind::WorkflowTaskCompleted { .. }
        | HistoryEventKind::WorkflowTaskFailed { .. }
        | HistoryEventKind::WorkflowTaskTimedOut { .. }
        | HistoryEventKind::ActivityTaskScheduled { .. }
        | HistoryEventKind::ActivityTaskCancelRequested { .. }
        | HistoryEventKind::TimerStarted { .. }
        | HistoryEventKind::TimerCanceled { .. }
        | HistoryEventKind::MarkerRecorded { .. }
        | HistoryEventKind::StartChildWorkflowExecutionInitiated { .. }
        | HistoryEventKind::SignalExternalWorkflowExecutionInitiated { .. }
        | HistoryEventKind::RequestCancelExternalWorkflowExecutionInitiated { .. }
        | HistoryEventKind::UpsertWorkflowSearchAttributes { .. }
        | HistoryEventKind::WorkflowPropertiesModified { .. }
        | HistoryEventKind::NexusOperationScheduled { .. }
        | HistoryEventKind::NexusOperationCancelRequested { .. }
        | HistoryEventKind::WorkflowExecutionUpdateAccepted { .. }
        | HistoryEventKind::WorkflowExecutionUpdateCompleted { .. }
        | HistoryEventKind::WorkflowExecutionUpdateCompletedV2 { .. } => 0,
    }
}

/// A memo's encoded size: its fields (field 1, `map<string, Payload>`).
pub fn memo_encoded_len(memo: &Memo) -> usize {
    memo.0
        .iter()
        .map(|(key, payload)| map_entry(key.len(), payload_encoded_len(payload)))
        .sum()
}

/// The payload Tokeira encodes for a stored search attribute value, as
/// `search_attr_value_to_payload` in `tokeira-proto` does, but without the
/// `type` metadata Tokeira adds and an SDK needn't send: the JSON value with
/// `encoding` `json/plain`, so that the merged check can't terminate a run
/// over metadata Tokeira added.
pub fn search_attribute_payload_size(value: &SearchAttrValue) -> PayloadSize {
    let json = match value {
        SearchAttrValue::Keyword(value) | SearchAttrValue::Text(value) => {
            serde_json::json!(value)
        }
        SearchAttrValue::KeywordList(value) => serde_json::json!(value),
        SearchAttrValue::Int(value) => serde_json::json!(value),
        SearchAttrValue::Double(value) => serde_json::json!(value),
        SearchAttrValue::Bool(value) => serde_json::json!(value),
        SearchAttrValue::Datetime(value) => {
            serde_json::json!(value.format(&Rfc3339).unwrap_or_default())
        }
    };
    let data = serde_json::to_vec(&json).map_or(0, |data| data.len());
    PayloadSize {
        data,
        encoded: map_entry("encoding".len(), "json/plain".len()) + len_field(data),
    }
}

/// The run's search attributes after `patch`, as v1.31.0's `ValidateSize`
/// measures the merged map (`workflow_task_completed_handler.go:1250-1264 @
/// v1.31.0`): each value's data length by key, and the map's encoded size.
/// A field the patch sets is measured as the edge measured it in `set_fields`,
/// or from its value when the edge didn't measure it.
pub fn merged_search_attribute_sizes(
    stored: &SearchAttributes,
    patch: &SearchAttributesPatch,
    set_fields: &BTreeMap<String, PayloadSize>,
) -> (BTreeMap<String, usize>, usize) {
    let mut merged: BTreeMap<&str, PayloadSize> = stored
        .0
        .iter()
        .map(|(key, value)| (key.as_str(), search_attribute_payload_size(value)))
        .collect();
    for (key, change) in &patch.0 {
        match change {
            FieldChange::Unchanged => {}
            FieldChange::Set(value) => {
                let size = set_fields
                    .get(key)
                    .copied()
                    .unwrap_or_else(|| search_attribute_payload_size(value));
                merged.insert(key.as_str(), size);
            }
            FieldChange::Clear => {
                merged.remove(key.as_str());
            }
        }
    }
    let total = merged
        .iter()
        .map(|(key, size)| map_entry(key.len(), size.encoded))
        .sum();
    let values = merged
        .into_iter()
        .map(|(key, size)| (key.to_owned(), size.data))
        .collect();
    (values, total)
}

/// The run's memo after `patch`, measured as v1.31.0 measures the merged memo
/// (`workflow_task_completed_handler.go:1301-1311 @ v1.31.0`). A field the
/// patch sets is measured as the edge measured it in `set_fields`, or from its
/// payload when the edge didn't measure it.
pub fn merged_memo_encoded_len(
    stored: &Memo,
    patch: &MemoPatch,
    set_fields: &BTreeMap<String, PayloadSize>,
) -> usize {
    let mut merged: BTreeMap<&str, usize> = stored
        .0
        .iter()
        .map(|(key, payload)| (key.as_str(), payload_encoded_len(payload)))
        .collect();
    for (key, change) in &patch.0 {
        match change {
            FieldChange::Unchanged => {}
            FieldChange::Set(payload) => {
                let encoded = set_fields
                    .get(key)
                    .map_or_else(|| payload_encoded_len(payload), |size| size.encoded);
                merged.insert(key.as_str(), encoded);
            }
            FieldChange::Clear => {
                merged.remove(key.as_str());
            }
        }
    }
    merged
        .into_iter()
        .map(|(key, encoded)| map_entry(key.len(), encoded))
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varints_grow_every_seven_bits() {
        assert_eq!(varint_len(0), 1);
        assert_eq!(varint_len(127), 1);
        assert_eq!(varint_len(128), 2);
        assert_eq!(varint_len(16_383), 2);
        assert_eq!(varint_len(16_384), 3);
        assert_eq!(varint_len(u64::MAX), 10);
    }
}
