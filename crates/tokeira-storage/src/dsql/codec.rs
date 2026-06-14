//! Binary persistence codecs for DSQL `BYTEA` columns.
//!
//! The DSQL schema deliberately keeps many domain objects as opaque blobs
//! instead of decomposing every kernel field into relational columns. The
//! authoritative transition log remains history-first; side tables expose only
//! the indexed fields needed for routing, sweeping, and dispatch. Postcard is
//! used here because these blobs are internal to Tokeira storage, not part of
//! the public Temporal wire contract.

use anyhow::Result;
use serde::{Serialize, de::DeserializeOwned};
use tokeira_kernel::{ActivityState, HistoryEvent, ProjectionOp, TimerState, WorkflowState};
use tokeira_types::{Payloads, ProjectionCursor};

use crate::{BacklogPayload, ProjectionContext, StoredWorkerDeployment};

/// Encode a persisted value with postcard.
///
/// Keep this generic helper small and boring: schema compatibility is enforced
/// by the typed wrappers below, so call sites should name the domain payload
/// they are storing instead of invoking postcard directly.
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    postcard::to_allocvec(value).map_err(Into::into)
}

/// Decode a persisted value with postcard.
///
/// Decode errors are intentionally surfaced as storage errors. A corrupt blob
/// means the repository cannot safely infer derived state from that row.
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    postcard::from_bytes(bytes).map_err(Into::into)
}

/// Serialize the current materialized workflow state for `workflow_hot`.
pub fn encode_workflow_state(state: &WorkflowState) -> Result<Vec<u8>> {
    encode(state)
}

/// Deserialize the authoritative hot-state snapshot for one run.
pub fn decode_workflow_state(bytes: &[u8]) -> Result<WorkflowState> {
    decode(bytes)
}

/// Serialize one committed history batch.
///
/// Batches are encoded as a vector because DSQL row limits and transaction
/// shape are controlled by the commit path, not by individual event rows.
pub fn encode_history_events(events: &[HistoryEvent]) -> Result<Vec<u8>> {
    encode(&events)
}

/// Deserialize a committed history batch.
pub fn decode_history_events(bytes: &[u8]) -> Result<Vec<HistoryEvent>> {
    decode(bytes)
}

/// Serialize the activity materialization used by timeout sweepers.
pub fn encode_activity_state(state: &ActivityState) -> Result<Vec<u8>> {
    encode(state)
}

/// Deserialize one activity side-table row.
pub fn decode_activity_state(bytes: &[u8]) -> Result<ActivityState> {
    decode(bytes)
}

/// Serialize timer metadata that is not already indexed in `timer_bucket`.
pub fn encode_timer_state(state: &TimerState) -> Result<Vec<u8>> {
    encode(state)
}

/// Deserialize one timer side-table row.
pub fn decode_timer_state(bytes: &[u8]) -> Result<TimerState> {
    decode(bytes)
}

/// Serialize backlog payloads for the generic dispatch backlog table.
pub fn encode_backlog_payload(payload: &BacklogPayload) -> Result<Vec<u8>> {
    encode(payload)
}

/// Deserialize backlog payloads after a queue drain.
pub fn decode_backlog_payload(bytes: &[u8]) -> Result<BacklogPayload> {
    decode(bytes)
}

/// Serialize activity input payloads for `activity_dispatch.input_data`.
///
/// This intentionally stores only `Payloads`: row columns already carry the
/// queue, activity id, attempt, and schedule event id, so duplicating a whole
/// backlog payload would create two sources of truth.
pub fn encode_payloads(payloads: &Payloads) -> Result<Vec<u8>> {
    encode(payloads)
}

/// Deserialize activity input payloads from `activity_dispatch.input_data`.
pub fn decode_payloads(bytes: &[u8]) -> Result<Payloads> {
    decode(bytes)
}

/// Serialize projection delivery context for replayable projection records.
pub fn encode_projection_context(ctx: &ProjectionContext) -> Result<Vec<u8>> {
    encode(ctx)
}

/// Deserialize projection delivery context.
pub fn decode_projection_context(bytes: &[u8]) -> Result<ProjectionContext> {
    decode(bytes)
}

/// Serialize the projection operations emitted by a transition.
pub fn encode_projection_ops(ops: &[ProjectionOp]) -> Result<Vec<u8>> {
    encode(&ops)
}

/// Deserialize projection operations for a projection worker batch.
pub fn decode_projection_ops(bytes: &[u8]) -> Result<Vec<ProjectionOp>> {
    decode(bytes)
}

/// Serialize projection cursors used by projector checkpoints.
pub fn encode_projection_cursor(cursor: &ProjectionCursor) -> Result<Vec<u8>> {
    encode(cursor)
}

/// Deserialize projection cursors used by projector checkpoints.
pub fn decode_projection_cursor(bytes: &[u8]) -> Result<ProjectionCursor> {
    decode(bytes)
}

/// Serialize one Worker Deployment registry document.
pub fn encode_worker_deployment(record: &StoredWorkerDeployment) -> Result<Vec<u8>> {
    encode(record)
}

/// Deserialize one Worker Deployment registry document.
pub fn decode_worker_deployment(bytes: &[u8]) -> Result<StoredWorkerDeployment> {
    decode(bytes)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use proptest::prelude::*;
    use time::OffsetDateTime;
    use tokeira_types::{
        ArchetypeId, ExecutionStatus, LogicalTaskSeq, Memo, NamespaceId, Payload, ProjectionCursor,
        RunId, RunKey, SearchAttributes, TaskQueueName, TransitionSeq, VisibilityLifecycleState,
        WorkflowId, WorkflowType,
    };
    use uuid::Uuid;

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
        fn projection_cursor_position_round_trips(
            partition_id in 0u32..64,
            fanout in 1u16..64,
            run_key in proptest::option::of(any::<u128>()),
            transition_seq in proptest::option::of(0u64..10_000),
        ) {
            let cursor = ProjectionCursor {
                partition_id,
                fanout,
                last_run_key: run_key.map(|value| RunKey(Uuid::from_u128(value))),
                last_transition_seq: transition_seq.map(TransitionSeq),
            };
            let encoded = encode_projection_cursor(&cursor).unwrap();
            let decoded = decode_projection_cursor(&encoded).unwrap();
            prop_assert_eq!(decoded, cursor);
        }

        #[test]
        fn projection_context_round_trips(ctx in arb_projection_context()) {
            let encoded = encode_projection_context(&ctx).unwrap();
            let decoded = decode_projection_context(&encoded).unwrap();
            prop_assert_eq!(decoded, ctx);
        }

        #[test]
        fn projection_ops_round_trips(ops in proptest::collection::vec(arb_projection_op(), 0..8)) {
            let encoded = encode_projection_ops(&ops).unwrap();
            let decoded = decode_projection_ops(&encoded).unwrap();
            prop_assert_eq!(decoded, ops);
        }

        #[test]
        fn memo_round_trips(memo in arb_memo()) {
            let encoded = encode(&memo).unwrap();
            let decoded = decode::<Memo>(&encoded).unwrap();
            prop_assert_eq!(decoded, memo);
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

        #[test]
        fn payloads_round_trips(bytes in prop::collection::vec(any::<u8>(), 0..128)) {
            let payloads = Payloads(vec![tokeira_types::Payload::new(bytes)]);
            let encoded = encode_payloads(&payloads).unwrap();
            let decoded = decode_payloads(&encoded).unwrap();
            prop_assert_eq!(decoded, payloads);
        }
    }

    prop_compose! {
        fn arb_payload()(bytes in proptest::collection::vec(any::<u8>(), 0..64)) -> Payload {
            Payload::new(bytes)
        }
    }

    prop_compose! {
        fn arb_memo()(
            entries in proptest::collection::btree_map("[a-z][a-z0-9_]{0,12}", arb_payload(), 0..8),
        ) -> Memo {
            Memo(entries)
        }
    }

    fn arb_execution_status() -> impl Strategy<Value = ExecutionStatus> {
        prop_oneof![
            Just(ExecutionStatus::Running),
            Just(ExecutionStatus::Paused),
            Just(ExecutionStatus::Completed),
            Just(ExecutionStatus::Failed),
            Just(ExecutionStatus::Cancelled),
            Just(ExecutionStatus::Terminated),
            Just(ExecutionStatus::ContinuedAsNew),
            Just(ExecutionStatus::TimedOut),
        ]
    }

    prop_compose! {
        fn arb_timestamp()(seconds in -1_000_000i64..1_000_000) -> OffsetDateTime {
            OffsetDateTime::from_unix_timestamp(seconds).unwrap()
        }
    }

    prop_compose! {
        fn arb_projection_context()(
            namespace in any::<u128>(),
            workflow in "[a-z][a-z0-9-]{0,24}",
            run_id in any::<u128>(),
            workflow_type in "[a-z][a-z0-9_]{0,24}",
            task_queue in "[a-z][a-z0-9_]{0,24}",
            execution_status in arb_execution_status(),
            start_time in arb_timestamp(),
            execution_time in proptest::option::of(arb_timestamp()),
            close_time in proptest::option::of(arb_timestamp()),
            history_length in 0i64..10_000,
            execution_duration in proptest::option::of(0i64..10_000),
            state_transition_count in 0i64..10_000,
            history_size_bytes in 0i64..10_000,
            parent_workflow in proptest::option::of("[a-z][a-z0-9-]{0,24}"),
            parent_run in proptest::option::of(any::<u128>()),
            root_workflow in proptest::option::of("[a-z][a-z0-9-]{0,24}"),
            root_run in proptest::option::of(any::<u128>()),
        ) -> ProjectionContext {
            ProjectionContext {
                archetype_id: ArchetypeId::WORKFLOW,
                namespace_id: NamespaceId(Uuid::from_u128(namespace)),
                business_id: workflow.clone(),
                authority_epoch: 0,
                status_keyword: format!("{execution_status:?}"),
                lifecycle_state: if execution_status.is_open() {
                    VisibilityLifecycleState::Open
                } else {
                    VisibilityLifecycleState::Closed
                },
                workflow_id: WorkflowId(workflow),
                run_id: RunId(Uuid::from_u128(run_id)),
                workflow_type: WorkflowType(workflow_type),
                task_queue: TaskQueueName(task_queue),
                execution_status,
                start_time,
                update_time: close_time.unwrap_or(start_time),
                execution_time,
                close_time,
                history_length,
                execution_duration,
                state_transition_count,
                transition_count: state_transition_count,
                history_size_bytes,
                parent_workflow_id: parent_workflow.map(WorkflowId),
                parent_run_id: parent_run.map(|run_id| RunId(Uuid::from_u128(run_id))),
                root_workflow_id: root_workflow.map(WorkflowId),
                root_run_id: root_run.map(|run_id| RunId(Uuid::from_u128(run_id))),
                search_attr_generation: state_transition_count as u64,
                memo: Memo::default(),
                search_attributes: SearchAttributes::default(),
            }
        }
    }

    prop_compose! {
        fn arb_projection_op()(
            status in arb_execution_status(),
            memo_patch in arb_memo(),
            closed_at in arb_timestamp(),
            close in any::<bool>(),
        ) -> ProjectionOp {
            if close {
                ProjectionOp::CloseExecution { status, closed_at }
            } else {
                ProjectionOp::UpsertExecution {
                    status,
                    memo_patch,
                    search_attr_patch: SearchAttributes(BTreeMap::new()),
                }
            }
        }
    }
}
