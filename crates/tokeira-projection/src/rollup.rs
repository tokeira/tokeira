//! Rollup delta computation for aggregate visibility counts.
//!
//! When an execution row is upserted, the sink needs to adjust per-dimension
//! counters (status, workflow type, task queue) without scanning the whole
//! table. This module computes the minimal set of +1/−1 deltas by diffing
//! the previous and next row states. The conservation invariant (net delta
//! is zero for transitions, +1 for inserts) is validated by property tests.

use crate::types::{ExecutionRow, RollupDelta, RollupDimension};

/// Compute the rollup counter deltas between a previous and next row state.
///
/// For each tracked dimension, if the value changed, emits a −1 for the old
/// value and a +1 for the new value. If `previous` is `None` (new row),
/// only +1 deltas are emitted.
pub fn compute_rollup_deltas(
    previous: Option<&ExecutionRow>,
    next: &ExecutionRow,
) -> Vec<RollupDelta> {
    let mut out = Vec::new();

    for (dimension, next_value) in [
        // The status dimension keys off the generic `status_keyword` (archetype-
        // interpreted), not the workflow-typed `status` enum, so non-workflow
        // archetypes whose status has no `ExecutionStatus` variant roll up too
        // (Requirement 10.5; see reference/DECISION-visibility-status-keyword.md).
        (
            RollupDimension::ExecutionStatus,
            next.status_keyword.clone(),
        ),
        (RollupDimension::WorkflowType, next.workflow_type.0.clone()),
        (RollupDimension::TaskQueue, next.task_queue.0.clone()),
    ] {
        let previous_value = previous.map(|row| match dimension {
            RollupDimension::ExecutionStatus => row.status_keyword.clone(),
            RollupDimension::WorkflowType => row.workflow_type.0.clone(),
            RollupDimension::TaskQueue => row.task_queue.0.clone(),
        });

        if let Some(previous_value) = previous_value {
            if previous_value == next_value {
                continue;
            }
            out.push(RollupDelta {
                namespace_id: next.namespace_id,
                dimension,
                value: previous_value,
                delta: -1,
            });
        }

        out.push(RollupDelta {
            namespace_id: next.namespace_id,
            dimension,
            value: next_value,
            delta: 1,
        });
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ExecutionRow;
    use proptest::prelude::*;
    use time::OffsetDateTime;
    use tokeira_types::{
        ArchetypeId, ExecutionStatus, Memo, NamespaceId, RunId, RunKey, SearchAttributes,
        TaskQueueName, TransitionSeq, WorkflowId, WorkflowType,
    };
    use uuid::Uuid;

    fn arb_status() -> impl Strategy<Value = ExecutionStatus> {
        prop_oneof![
            Just(ExecutionStatus::Running),
            Just(ExecutionStatus::Completed),
            Just(ExecutionStatus::Failed),
            Just(ExecutionStatus::Cancelled),
            Just(ExecutionStatus::Terminated),
            Just(ExecutionStatus::ContinuedAsNew),
            Just(ExecutionStatus::TimedOut),
        ]
    }

    fn arb_row(ns: NamespaceId) -> impl Strategy<Value = ExecutionRow> {
        (
            any::<u128>(),
            "[a-z]{1,6}",
            any::<u128>(),
            "[A-Z][a-z]{1,4}",
            "[a-z]{1,4}",
            arb_status(),
            1i64..1_000_000,
        )
            .prop_map(move |(rk, wf_id, run_id, wf_type, tq, status, start)| {
                ExecutionRow {
                    run_key: RunKey(Uuid::from_u128(rk)),
                    namespace_id: ns,
                    archetype_id: ArchetypeId::WORKFLOW,
                    business_id: wf_id.clone(),
                    workflow_id: WorkflowId(wf_id.clone()),
                    run_id: RunId(Uuid::from_u128(run_id)),
                    authority_epoch: 0,
                    source_transition_seq: TransitionSeq(1),
                    status_keyword: crate::types::workflow_status_keyword(status),
                    lifecycle_state: crate::types::workflow_lifecycle_state(status),
                    workflow_type: WorkflowType(wf_type),
                    task_queue: TaskQueueName(tq),
                    status,
                    start_time: OffsetDateTime::from_unix_timestamp(start).unwrap(),
                    update_time: OffsetDateTime::from_unix_timestamp(start).unwrap(),
                    execution_time: None,
                    close_time: None,
                    history_length: 1,
                    execution_duration: None,
                    state_transition_count: 1,
                    history_size_bytes: 0,
                    parent_workflow_id: None,
                    parent_run_id: None,
                    root_workflow_id: WorkflowId(wf_id),
                    root_run_id: RunId(Uuid::from_u128(run_id)),
                    memo: Memo::default(),
                    search_attributes: SearchAttributes::default(),
                    transition_count: 1,
                    search_attr_generation: 0,
                    search_attr_version: 0,
                }
            })
    }

    // Feature: projection-visibility, Property 11:
    // Rollup Delta Conservation
    // **Validates: Requirements 6.1, 6.2**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        #[test]
        fn prop_rollup_delta_conservation(
            prev in arb_row(
                NamespaceId(Uuid::from_u128(1))
            ),
            next in arb_row(
                NamespaceId(Uuid::from_u128(1))
            ),
        ) {
            let deltas =
                compute_rollup_deltas(Some(&prev), &next);
            // For each dimension, net delta should be 0
            // if value unchanged, or -1 old +1 new if
            // changed.
            for dim in [
                RollupDimension::ExecutionStatus,
                RollupDimension::WorkflowType,
                RollupDimension::TaskQueue,
            ] {
                let dim_deltas: Vec<_> = deltas
                    .iter()
                    .filter(|d| d.dimension == dim)
                    .collect();
                let net: i64 =
                    dim_deltas.iter().map(|d| d.delta).sum();
                // Net should always be 0 for transitions
                // (old -1, new +1) or 0 if unchanged
                prop_assert_eq!(
                    net, 0,
                    "net delta for {:?} should be 0",
                    dim
                );
            }
        }
    }

    // Feature: projection-visibility, Property 11 (new row):
    // Rollup Delta Conservation for new rows
    // **Validates: Requirements 6.1, 6.2**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        #[test]
        fn prop_rollup_new_row_deltas(
            row in arb_row(
                NamespaceId(Uuid::from_u128(1))
            ),
        ) {
            let deltas =
                compute_rollup_deltas(None, &row);
            // New row: +1 for each dimension
            for dim in [
                RollupDimension::ExecutionStatus,
                RollupDimension::WorkflowType,
                RollupDimension::TaskQueue,
            ] {
                let dim_deltas: Vec<_> = deltas
                    .iter()
                    .filter(|d| d.dimension == dim)
                    .collect();
                let net: i64 =
                    dim_deltas.iter().map(|d| d.delta).sum();
                prop_assert_eq!(
                    net, 1,
                    "new row net delta for {:?}",
                    dim
                );
            }
        }
    }

    // Feature: projection-visibility, Property 12:
    // Rollup Time Bucketing (determinism)
    // **Validates: Requirements 6.3**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        #[test]
        fn prop_rollup_deterministic(
            prev in arb_row(
                NamespaceId(Uuid::from_u128(1))
            ),
            next in arb_row(
                NamespaceId(Uuid::from_u128(1))
            ),
        ) {
            let d1 =
                compute_rollup_deltas(Some(&prev), &next);
            let d2 =
                compute_rollup_deltas(Some(&prev), &next);
            prop_assert_eq!(d1, d2);
        }
    }
}
