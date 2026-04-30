use anyhow::Result;
use serde::{Serialize, de::DeserializeOwned};
use tokeira_kernel::{ActivityState, HistoryEvent, ProjectionOp, TimerState, WorkflowState};
use tokeira_types::ProjectionCursor;

use crate::{BacklogPayload, ProjectionContext};

/// Encode a persisted value with postcard.
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    postcard::to_allocvec(value).map_err(Into::into)
}

/// Decode a persisted value with postcard.
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    postcard::from_bytes(bytes).map_err(Into::into)
}

pub fn encode_workflow_state(state: &WorkflowState) -> Result<Vec<u8>> {
    encode(state)
}

pub fn decode_workflow_state(bytes: &[u8]) -> Result<WorkflowState> {
    decode(bytes)
}

pub fn encode_history_events(events: &[HistoryEvent]) -> Result<Vec<u8>> {
    encode(&events)
}

pub fn decode_history_events(bytes: &[u8]) -> Result<Vec<HistoryEvent>> {
    decode(bytes)
}

pub fn encode_activity_state(state: &ActivityState) -> Result<Vec<u8>> {
    encode(state)
}

pub fn decode_activity_state(bytes: &[u8]) -> Result<ActivityState> {
    decode(bytes)
}

pub fn encode_timer_state(state: &TimerState) -> Result<Vec<u8>> {
    encode(state)
}

pub fn decode_timer_state(bytes: &[u8]) -> Result<TimerState> {
    decode(bytes)
}

pub fn encode_backlog_payload(payload: &BacklogPayload) -> Result<Vec<u8>> {
    encode(payload)
}

pub fn decode_backlog_payload(bytes: &[u8]) -> Result<BacklogPayload> {
    decode(bytes)
}

pub fn encode_projection_context(ctx: &ProjectionContext) -> Result<Vec<u8>> {
    encode(ctx)
}

pub fn decode_projection_context(bytes: &[u8]) -> Result<ProjectionContext> {
    decode(bytes)
}

pub fn encode_projection_ops(ops: &[ProjectionOp]) -> Result<Vec<u8>> {
    encode(&ops)
}

pub fn decode_projection_ops(bytes: &[u8]) -> Result<Vec<ProjectionOp>> {
    decode(bytes)
}

pub fn encode_projection_cursor(cursor: &ProjectionCursor) -> Result<Vec<u8>> {
    encode(cursor)
}

pub fn decode_projection_cursor(bytes: &[u8]) -> Result<ProjectionCursor> {
    decode(bytes)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use tokeira_types::LogicalTaskSeq;

    use super::*;

    proptest! {
        #[test]
        fn projection_cursor_round_trips(partition_id in 0u32..64, fanout in 1u16..64) {
            let cursor = ProjectionCursor::beginning(partition_id, fanout);
            let encoded = encode_projection_cursor(&cursor).unwrap();
            let decoded = decode_projection_cursor(&encoded).unwrap();
            prop_assert_eq!(decoded, cursor);
        }

        #[test]
        fn backlog_payload_round_trips(logical_seq in 1u64..1_000_000) {
            let payload = BacklogPayload::Workflow {
                logical_seq: LogicalTaskSeq(logical_seq),
            };
            let encoded = encode_backlog_payload(&payload).unwrap();
            let decoded = decode_backlog_payload(&encoded).unwrap();
            prop_assert_eq!(decoded, payload);
        }
    }
}
