//! Integrity checks for the exceptional close-and-reserve transaction.
//! The runtime selects workflow semantics; storage binds the frozen recipe to
//! the terminal link and protects that exact lineage until atomic publication.

use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use tokeira_kernel::{HistoryEventKind, StartRequest, Transition, WorkflowState};
use tokeira_types::{RunKey, ShardId, TransitionSeq};

use super::{IntentKey, IntentKind, IntentPayload, IntentRow, RecoveryError};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SuccessorReservation {
    pub intent: IntentKey,
    pub successor: RunKey,
}

pub(crate) fn freeze(
    run: RunKey,
    closing: &Transition,
    start: Box<StartRequest>,
    home: ShardId,
) -> Result<(SuccessorReservation, IntentRow)> {
    let state = &closing.next_state;
    ensure!(
        state.run_key == run
            && !state.status.is_open()
            && closing.expected_seq != TransitionSeq::ZERO
            && state.transition_seq == closing.expected_seq.next(),
        "successor reservation requires a single existing-run close"
    );
    ensure!(
        start.namespace_id == state.namespace_id
            && start.workflow_id == state.workflow_id
            && start.run_key != run
            && start.run_id != state.run_id
            && start.run_key
                == RunKey::derive(start.namespace_id, &start.workflow_id, start.run_id)
            && start.continued_execution_run_id == Some(state.run_id)
            && start.first_execution_run_id
                == Some(state.first_execution_run_id.unwrap_or(state.run_id)),
        "frozen successor does not match predecessor lineage"
    );
    let terminal = closing
        .history_events
        .last()
        .ok_or(RecoveryError::InvalidPreparation(
            "successor close has no terminal event",
        ))?;
    let linked = match &terminal.kind {
        HistoryEventKind::WorkflowExecutionContinuedAsNew { new_run_id, .. } => Some(*new_run_id),
        HistoryEventKind::WorkflowExecutionCompleted {
            new_execution_run_id,
            ..
        }
        | HistoryEventKind::WorkflowExecutionFailed {
            new_execution_run_id,
            ..
        }
        | HistoryEventKind::WorkflowExecutionTimedOut {
            new_execution_run_id,
            ..
        } => *new_execution_run_id,
        _ => None,
    };
    ensure!(
        linked == Some(start.run_id),
        "terminal event does not name frozen successor"
    );
    let key = IntentKey {
        source_run: run,
        close_seq: state.transition_seq,
        kind: IntentKind::Successor,
        ordinal: 0,
    };
    let reservation = SuccessorReservation {
        intent: key,
        successor: start.run_key,
    };
    let row = IntentRow {
        key,
        source_home: home,
        created_at: terminal.happened_at,
        target: Some((start.namespace_id, start.workflow_id.clone())),
        payload: IntentPayload::Successor { start },
    };
    Ok((reservation, row))
}

pub(crate) fn validate_publication(
    reservation: &SuccessorReservation,
    intent: &IntentRow,
    expected: IntentKey,
    state: &WorkflowState,
) -> Result<()> {
    let IntentPayload::Successor { start } = &intent.payload else {
        return Err(RecoveryError::InvalidPreparation(
            "reservation does not name a successor recipe",
        )
        .into());
    };
    ensure!(
        expected.kind == IntentKind::Successor
            && reservation.intent == expected
            && intent.key == expected
            && reservation.successor == state.run_key
            && start.run_key == state.run_key
            && start.run_id == state.run_id
            && start.namespace_id == state.namespace_id
            && start.workflow_id == state.workflow_id
            && start.now == state.started_at
            && start.first_execution_run_id
                == Some(state.first_execution_run_id.unwrap_or(state.run_id)),
        "publication does not match its reserved successor"
    );
    Ok(())
}
