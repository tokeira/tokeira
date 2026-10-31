//! Close, immutable recipe and pointer reservation share one atomic store write.

use super::*;
use crate::{IntentKind, RecoveryError, outbound::successor};

pub(super) fn guard_pointer(store: &StoreState, state: &WorkflowState) -> Result<()> {
    if let Some(reservation) = store
        .successor_reservations
        .get(&(state.namespace_id, state.workflow_id.0.clone()))
    {
        return Err(RecoveryError::SuccessorReserved {
            successor: reservation.successor,
        }
        .into());
    }
    Ok(())
}

impl InMemoryStore {
    pub(super) async fn commit_fallback(
        &self,
        run: RunKey,
        closing: Transition,
        start: Box<tokeira_kernel::StartRequest>,
        fence: WriteFence,
    ) -> Result<CommitResult> {
        let mut store = self.inner.lock().await;
        let state = &closing.next_state;
        let home = tokeira_types::execution_home_bundle(
            state.namespace_id.0.as_bytes(),
            state.workflow_id.0.as_bytes(),
            Self::effective_shard_count(&store),
        );
        fence.ensure_home(home)?;
        store.check_fence(&fence)?;
        let (reservation, intent) = successor::freeze(run, &closing, start, home)?;
        let workflow = (state.namespace_id, state.workflow_id.0.clone());
        if store.successor_reservations.get(&workflow) == Some(&reservation)
            && store.outbound_intents.get(&intent.key) == Some(&intent)
        {
            return Ok(CommitResult::Duplicate);
        }
        guard_pointer(&store, state)?;
        if store.current_execution.get(&workflow) != Some(&run)
            || store.runs.get(&run).map(|s| s.transition_seq) != Some(closing.expected_seq)
        {
            return Ok(CommitResult::Conflict {
                reason: "successor predecessor or current pointer changed".into(),
            });
        }
        if let Some(remaining) = store.conflict_injections.get_mut(&run)
            && *remaining > 0
        {
            *remaining -= 1;
            return Ok(CommitResult::Conflict {
                reason: "injected fallback conflict".into(),
            });
        }
        let prepared = transition::prepare(&store, run, closing)?;
        let state = transition::write(&mut store, run, prepared);
        store.outbound_intents.insert(intent.key, intent);
        store.successor_reservations.insert(workflow, reservation);
        Ok(CommitResult::Applied { new_state: state })
    }
}

pub(super) fn validate_reserved_publication(
    store: &StoreState,
    publication: &crate::RunPreparation,
) -> Result<()> {
    let state = &publication.new_run.next_state;
    if let Some(key) = publication.successor_intent {
        let reservation = store
            .successor_reservations
            .get(&(state.namespace_id, state.workflow_id.0.clone()))
            .ok_or(RecoveryError::InvalidPreparation(
                "successor reservation is absent",
            ))?;
        let intent = store
            .outbound_intents
            .get(&key)
            .ok_or(RecoveryError::InvalidPreparation(
                "successor intent is absent",
            ))?;
        successor::validate_publication(reservation, intent, key, state)
    } else {
        guard_pointer(store, state)
    }
}

pub(super) fn ensure_generic_retirement(kind: IntentKind) -> Result<()> {
    if kind == IntentKind::Successor {
        return Err(RecoveryError::InvalidPreparation(
            "successor intent retires only with its publication",
        )
        .into());
    }
    Ok(())
}
