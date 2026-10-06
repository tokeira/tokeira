//! Protobuf-encoded sizes of the run's stored memo and search attributes, as
//! Temporal v1.31.0 measures them (`workflow-task-command-limits` criterion
//! 2.12).
//!
//! The edge measures what a command carries on the proto it received. Only an
//! upsert's merged map needs the run's stored values, which the kernel holds
//! as domain types, so this mirrors protobuf's encoding of
//! `temporal.api.common.v1.Payload`, `Memo` and `SearchAttributes` for them.

use std::collections::BTreeMap;

use time::format_description::well_known::Rfc3339;
use tokeira_types::{Memo, Payload, SearchAttrValue, SearchAttributes};

use crate::command::{FieldChange, MemoPatch, PayloadSize, SearchAttributesPatch};

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
