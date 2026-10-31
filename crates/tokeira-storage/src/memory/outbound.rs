//! Fenced source maintenance and destination validation under the store write lock.

use super::*;
use crate::{
    DeliveryGuard, IntentKey, IntentRetirement, OperationRow, OutboundPage, RecoveryError,
    ScanBound,
};
use tokeira_kernel::{OperationKey, OperationKind};

impl InMemoryStore {
    pub(super) async fn outbound_operation_page(
        &self,
        home: ShardId,
        kind: OperationKind,
        bound: ScanBound<OperationKey>,
        after: Option<OperationKey>,
        limit: usize,
    ) -> Result<OutboundPage<OperationKey, OperationRow>> {
        crate::outbound::validate_page_limit(limit)?;
        let store = self.inner.lock().await;
        let matching = || {
            store
                .outbound_operations
                .iter()
                .filter(|(_, row)| row.source_home == home && row.incarnation.key.kind == kind)
                .filter(|(key, _)| store.runs.contains_key(&key.source_run))
        };
        let through = match bound {
            ScanBound::Capture => matching().next_back().map(|(key, _)| *key),
            ScanBound::Through(key) => Some(key),
        };
        let rows = matching()
            .filter(|(key, _)| {
                after.is_none_or(|after| **key > after) && through.is_some_and(|end| **key <= end)
            })
            .take(limit)
            .map(|(_, row)| row.clone())
            .collect();
        Ok(OutboundPage { through, rows })
    }

    pub(super) async fn outbound_intent_page(
        &self,
        home: ShardId,
        bound: ScanBound<IntentKey>,
        after: Option<IntentKey>,
        limit: usize,
    ) -> Result<OutboundPage<IntentKey, crate::IntentDescriptor>> {
        crate::outbound::validate_page_limit(limit)?;
        let store = self.inner.lock().await;
        let matching = || {
            store
                .outbound_intents
                .iter()
                .filter(|(_, row)| row.source_home == home)
        };
        let through = match bound {
            ScanBound::Capture => matching().next_back().map(|(key, _)| *key),
            ScanBound::Through(key) => Some(key),
        };
        let rows = matching()
            .filter(|(key, _)| {
                after.is_none_or(|after| **key > after) && through.is_some_and(|end| **key <= end)
            })
            .take(limit)
            .map(|(_, row)| row.into())
            .collect();
        Ok(OutboundPage { through, rows })
    }

    pub(super) async fn repair_outbound(
        &self,
        home: ShardId,
        run: RunKey,
        fence: WriteFence,
    ) -> Result<()> {
        let mut store = self.inner.lock().await;
        fence.ensure_home(home)?;
        store.check_fence(&fence)?;
        let rows = if let Some(state) = store.runs.get(&run) {
            let actual = tokeira_types::execution_home_bundle(
                state.namespace_id.0.as_bytes(),
                state.workflow_id.0.as_bytes(),
                Self::effective_shard_count(&store),
            );
            if home != actual {
                return Err(RecoveryError::InvalidIdentity {
                    run,
                    detail: "repair home does not own the source",
                }
                .into());
            }
            crate::derive_operations(state, home)?
        } else {
            Vec::new()
        };
        store
            .outbound_operations
            .retain(|key, _| key.source_run != run);
        store
            .outbound_operations
            .extend(rows.into_iter().map(|row| (row.incarnation.key, row)));
        Ok(())
    }

    pub(super) async fn retire_outbound_intent(
        &self,
        proof: IntentRetirement,
        fence: WriteFence,
    ) -> Result<bool> {
        let mut store = self.inner.lock().await;
        store.check_fence(&fence)?;
        super::successor::ensure_generic_retirement(proof.key.kind)?;
        if let Some(row) = store.outbound_intents.get(&proof.key) {
            fence.ensure_home(row.source_home)?;
        }
        Ok(store.outbound_intents.remove(&proof.key).is_some())
    }

    pub(super) async fn commit_guarded(
        &self,
        run_key: RunKey,
        transition: Transition,
        guard: Option<(DeliveryGuard, WriteFence)>,
        fence: WriteFence,
    ) -> Result<CommitResult> {
        if crate::read_probe::reject_commit(run_key) {
            return Ok(CommitResult::Conflict {
                reason: "read-baseline injected OCC conflict".into(),
            });
        }
        let started = Instant::now();
        if transition.history_events.len() != transition.event_principals.len() {
            anyhow::bail!(
                "history/principal sidecar length mismatch: {} events, {} principals",
                transition.history_events.len(),
                transition.event_principals.len()
            );
        }
        let state = transition.next_state.clone();
        let namespace = Some(state.namespace_id.0.to_string());
        let mut store = self.inner.lock().await;
        let home = tokeira_types::execution_home_bundle(
            state.namespace_id.0.as_bytes(),
            state.workflow_id.0.as_bytes(),
            Self::effective_shard_count(&store),
        );
        fence.ensure_home(home)?;
        if let Err(lost) = store.check_fence(&fence) {
            storage_metrics::record_commit_transition_duration(
                namespace.clone(),
                "lease_lost",
                started.elapsed(),
            );
            storage_metrics::record_storage_operation("commit_transition", "lease_lost");
            return Err(lost.into());
        }
        if let Some(remaining) = store.conflict_injections.get_mut(&run_key)
            && *remaining > 0
        {
            *remaining -= 1;
            storage_metrics::record_commit_transition_duration(
                namespace.clone(),
                "conflict",
                started.elapsed(),
            );
            storage_metrics::record_storage_operation("commit_transition", "conflict");
            return Ok(CommitResult::Conflict {
                reason: format!("injected conflict for run {:?}", run_key),
            });
        }

        if let Some((guard, source_fence)) = &guard {
            validate_guard(&store, *guard, source_fence, &state)?;
        }

        if transition.expected_seq == TransitionSeq::ZERO {
            super::successor::guard_pointer(&store, &state)?;
        }
        if transition.expected_seq == TransitionSeq::ZERO
            && super::preparation::target_is_preparing(&store, run_key)
        {
            return Err(RecoveryError::InvalidPreparation("target run is being prepared").into());
        }

        let current_seq = store
            .runs
            .get(&run_key)
            .map(|state| state.transition_seq)
            .unwrap_or(tokeira_types::TransitionSeq::ZERO);
        if current_seq != transition.expected_seq {
            storage_metrics::record_commit_transition_duration(
                namespace.clone(),
                "conflict",
                started.elapsed(),
            );
            storage_metrics::record_storage_operation("commit_transition", "conflict");
            return Ok(CommitResult::Conflict {
                reason: format!(
                    "expected seq {:?}, found {:?}",
                    transition.expected_seq, current_seq
                ),
            });
        }

        let workflow_key = (state.namespace_id, state.workflow_id.0.clone());

        // Guarded idempotent observations still validate source and target in
        // one critical section, but must not append another projection image at
        // the same transition sequence.
        if transition.is_noop() {
            return Ok(CommitResult::Applied { new_state: state });
        }

        for op in &transition.request_dedupe_ops {
            let dedupe_key = (
                workflow_key.0,
                workflow_key.1.clone(),
                op.request_id.0.clone(),
            );
            // Request-id dedupe is scoped to the RUN, mirroring v1.31.0 where
            // request ids live in the run's mutable state: a request id reused
            // against a NEW run (e.g. signal-with-start after the prior run
            // closed) is fresh, not a duplicate
            // (`pendingSignalRequestedIDs` / `request_id_infos` are per-run,
            // mutable_state_impl.go:2361-2398 @ v1.31.0; start-retry dedup is
            // the conflict-resolution layer's job, not this table's).
            if store
                .request_dedupe
                .get(&dedupe_key)
                .is_some_and(|record| record.run_id == state.run_id)
            {
                storage_metrics::record_commit_transition_duration(
                    namespace.clone(),
                    "duplicate",
                    started.elapsed(),
                );
                storage_metrics::record_storage_operation("commit_transition", "duplicate");
                return Ok(CommitResult::Duplicate);
            }
        }

        if transition.expected_seq == tokeira_types::TransitionSeq::ZERO && state.status.is_open() {
            match store.conflict_policy {
                CurrentExecutionConflictPolicy::Reject
                | CurrentExecutionConflictPolicy::AllowAfterClose => {
                    if let Some(existing_run) = store.current_open.get(&workflow_key)
                        && *existing_run != run_key
                        && let Some(existing_state) = store.runs.get(existing_run)
                        && existing_state.status.is_open()
                    {
                        storage_metrics::record_commit_transition_duration(
                            namespace.clone(),
                            "conflict",
                            started.elapsed(),
                        );
                        storage_metrics::record_storage_operation("commit_transition", "conflict");
                        // Report the current-execution collision (not deny): the
                        // runtime resolves it by the request's
                        // WorkflowIdConflictPolicy. Distinct from a transient
                        // `Conflict` so the lane does not OCC-retry it.
                        return Ok(CommitResult::CurrentExecutionConflict {
                            existing_run_key: *existing_run,
                            existing_status: existing_state.status,
                            request_ids: existing_state
                                .request_id_infos
                                .iter()
                                .map(|(id, info)| (id.clone(), info.clone()))
                                .collect(),
                        });
                    }
                }
            }
        }

        let mut prepared = transition::prepare(&store, run_key, transition)?;
        prepared.outbound_origin = guard
            .as_ref()
            .and_then(|(guard, _)| guard.operation())
            .map(|incarnation| tokeira_kernel::OutboundOrigin { incarnation });
        let state = transition::write(&mut store, run_key, prepared);

        storage_metrics::record_commit_transition_duration(namespace, "applied", started.elapsed());
        storage_metrics::record_storage_operation("commit_transition", "success");
        Ok(CommitResult::Applied { new_state: state })
    }
}

pub(super) fn purge_pinned(store: &StoreState, run: RunKey) -> Result<bool> {
    if store
        .outbound_intents
        .keys()
        .any(|key| key.source_run == run)
    {
        return Ok(true);
    }
    let Some(bytes) = store.retained_runs.get(&run) else {
        return Ok(false);
    };
    let state = crate::codec::decode_workflow_state(run, bytes)?;
    let identity = (state.namespace_id, state.workflow_id.clone());
    if store.outbound_operations.values().any(|row| {
        row.target.as_ref() == Some(&identity)
            && store.runs.contains_key(&row.incarnation.key.source_run)
    }) || store
        .outbound_intents
        .values()
        .any(|row| row.target.as_ref() == Some(&identity))
        || store.runs.values().any(|child| {
            child.is_open()
                && child.parent_run_key.is_some()
                && child.parent_namespace_id.unwrap_or(child.namespace_id) == state.namespace_id
                && child.parent_workflow_id.as_ref() == Some(&state.workflow_id)
        })
    {
        return Ok(true);
    }
    // A deferred child successor temporarily has no open child run. Its frozen
    // start carries the same parent relationship until atomic publication.
    Ok(store.outbound_intents.values().any(|row| {
        matches!(&row.payload,
        crate::IntentPayload::Successor { start } if start.parent_run_key.is_some()
            && start.parent_namespace_id.unwrap_or(start.namespace_id) == state.namespace_id
            && start.parent_workflow_id.as_ref() == Some(&state.workflow_id))
    }))
}

fn validate_guard(
    store: &StoreState,
    guard: DeliveryGuard,
    source_fence: &WriteFence,
    target: &tokeira_kernel::WorkflowState,
) -> Result<()> {
    store.check_fence(source_fence)?;
    if let DeliveryGuard::ChildStart {
        expected_current, ..
    }
    | DeliveryGuard::CurrentIntent {
        expected_current, ..
    } = guard
        && store
            .current_execution
            .get(&(target.namespace_id, target.workflow_id.0.clone()))
            .copied()
            != expected_current
    {
        return Err(RecoveryError::Obsolete.into());
    }
    match guard {
        DeliveryGuard::Operation {
            incarnation,
            source_seq,
        }
        | DeliveryGuard::ChildStart {
            incarnation,
            source_seq,
            ..
        } => {
            let Some(source) = store.runs.get(&incarnation.key.source_run) else {
                return Err(RecoveryError::Obsolete.into());
            };
            let home = tokeira_types::execution_home_bundle(
                source.namespace_id.0.as_bytes(),
                source.workflow_id.0.as_bytes(),
                InMemoryStore::effective_shard_count(store),
            );
            source_fence.ensure_home(home)?;
            if source.transition_seq != source_seq
                || !crate::derive_operations(source, home)?
                    .iter()
                    .any(|row| row.incarnation == incarnation)
            {
                return Err(RecoveryError::Obsolete.into());
            }
        }
        DeliveryGuard::Intent(key) | DeliveryGuard::CurrentIntent { key, .. } => {
            let row = store
                .outbound_intents
                .get(&key)
                .ok_or(RecoveryError::Obsolete)?;
            source_fence.ensure_home(row.source_home)?;
        }
    }
    Ok(())
}
