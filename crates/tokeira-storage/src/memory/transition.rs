//! Prepared memory write sets: every fallible operation precedes publication.
//! A pair prepares both runs while holding the store lock, then installs both
//! with no await or fallible work between them.

use super::*;
use crate::{PairCommitResult, ProjectionContext};

pub(super) struct PreparedTransition {
    transition: Transition,
    state: WorkflowState,
    context: ProjectionContext,
    history_size_bytes: i64,
    workflow_dispatch: Option<WorkflowDispatchRow>,
    activity_dispatch: BTreeMap<String, ActivityDispatchEntry>,
    operations: Vec<crate::OperationRow>,
    intents: Vec<crate::IntentRow>,
    pub(super) outbound_origin: Option<tokeira_kernel::OutboundOrigin>,
}

pub(super) fn validate_activity_slots(
    store: &StoreState,
    run_key: RunKey,
    wanted: &BTreeMap<uuid::Uuid, crate::ActivityDispatchRow>,
) -> Result<()> {
    // Memory keys its map by raw identity, whereas DSQL uses the physical hash.
    // Check both coordinates before publishing so injected collisions/corruption
    // have the same all-or-nothing outcome in either repository.
    for ((stored_run, id), existing) in &store.activity_dispatch {
        if *stored_run != run_key && !wanted.contains_key(&existing.key) {
            continue;
        }
        anyhow::ensure!(
            existing.task.run_key == *stored_run
                && existing.task.activity_id == *id
                && existing.key == crate::activity_dispatch_key(*stored_run, id),
            "activity dispatch slot disagrees with raw identity"
        );
        if let Some(desired) = wanted.get(&existing.key) {
            anyhow::ensure!(
                desired.incarnation.run_key == *stored_run
                    && desired.incarnation.activity_id == *id,
                "activity dispatch slot hash collision"
            );
        }
    }
    Ok(())
}

/// Separate prepared runs must also agree on the physical keys they will write.
/// Existing-row checks alone cannot catch two new colliding slots in one batch.
pub(super) fn validate_activity_batch<'a>(
    prepared: impl Iterator<Item = &'a PreparedTransition>,
) -> Result<()> {
    let mut slots = BTreeMap::new();
    for prepared in prepared {
        for row in prepared.activity_dispatch.values() {
            let identity = (row.task.run_key, row.task.activity_id.as_str());
            if let Some(previous) = slots.insert(row.key, identity) {
                anyhow::ensure!(
                    previous == identity,
                    "activity dispatch slot hash collision within publication"
                );
            }
        }
    }
    Ok(())
}

pub(super) fn prepare(
    store: &StoreState,
    run_key: RunKey,
    transition: Transition,
) -> Result<PreparedTransition> {
    if transition.history_events.len() != transition.event_principals.len() {
        anyhow::bail!("history/principal sidecar length mismatch");
    }
    let state = transition.next_state.clone();
    crate::codec::validate_activity_receipts(&state).map_err(|defect| {
        crate::codec::StateExtensionError {
            kind: "workflow_hot.state_data",
            run_key,
            defect,
        }
    })?;
    let prior_history_size = store.history_size.get(&run_key).copied().unwrap_or(0);
    let appended = if transition.history_events.is_empty() {
        0
    } else {
        crate::codec::history_batch_encoded_len(&transition.history_events)?
    };
    let history_size_bytes = prior_history_size.saturating_add(appended);
    // Preparation touches only the local clone. A missing seed, invalid image
    // or growth refusal must leave every durable map and statistic untouched.
    let context = prepare_workflow_projection(&state, history_size_bytes)?;
    let execution_home = tokeira_types::execution_home_bundle(
        state.namespace_id.0.as_bytes(),
        state.workflow_id.0.as_bytes(),
        InMemoryStore::effective_shard_count(store),
    );
    let workflow_dispatch = derive_workflow_dispatch(&state, execution_home);
    let wanted = crate::derive_activity_dispatch(&state, execution_home)?;
    validate_activity_slots(store, run_key, &wanted)?;
    let activity_dispatch = wanted
        .into_values()
        .map(|row| {
            let activity = &state.activities[&row.incarnation.activity_id];
            (
                activity.activity_id.clone(),
                ActivityDispatchEntry::from_row(&row, activity),
            )
        })
        .collect();
    let operations = crate::derive_operations(&state, execution_home)?;
    let intents =
        crate::derive_close_intents(store.runs.get(&run_key), &transition, execution_home)?;
    if let Some(row) = &workflow_dispatch {
        row.validate()?;
    }
    // The growth checks run before the first change, so a refused commit
    // leaves the run as it was. The state is measured as DSQL stores it
    // (`run-growth-limits`).
    if transition.growth_limits.is_some() {
        let measured_state_size = crate::codec::measured_state_len(
            crate::codec::workflow_state_encoded_len(&state)?,
            &state,
        )?;
        crate::growth::check_commit(
            run_key,
            &transition,
            crate::growth::CommitMeasures {
                prior_history_size,
                measured_state_size,
                batch_size: usize::try_from(appended).unwrap_or(usize::MAX),
            },
        )?;
    }
    Ok(PreparedTransition {
        transition,
        state,
        context,
        history_size_bytes,
        workflow_dispatch,
        activity_dispatch,
        operations,
        intents,
        outbound_origin: None,
    })
}

pub(super) fn write(
    store: &mut StoreState,
    run_key: RunKey,
    prepared: PreparedTransition,
) -> WorkflowState {
    let PreparedTransition {
        transition,
        state,
        context,
        history_size_bytes,
        workflow_dispatch,
        activity_dispatch,
        operations,
        intents,
        outbound_origin,
    } = prepared;
    store
        .outbound_operations
        .retain(|key, _| key.source_run != run_key);
    for row in operations {
        store.outbound_operations.insert(row.incarnation.key, row);
    }
    for row in intents {
        store.outbound_intents.insert(row.key, row);
    }
    let workflow_key = (state.namespace_id, state.workflow_id.0.clone());
    // The History Size advances with the batch it accounts for, under the
    // same lock acquisition, exactly as the DSQL commit writes the column
    // in the transaction that inserts the batch (Requirement 1.1). A
    // transition without events leaves it unchanged.
    store.history_size.insert(run_key, history_size_bytes);
    store
        .history
        .entry(run_key)
        .or_default()
        .extend(transition.history_events.iter().cloned());
    store
        .history_principals
        .entry(run_key)
        .or_default()
        .extend(transition.event_principals.iter().cloned());

    store
        .transition_audit
        .entry(run_key)
        .or_default()
        .push(TransitionAuditRecord {
            run_key,
            transition_seq: state.transition_seq,
            history_events: transition.history_events.iter().cloned().collect(),
            activity_ops: transition.activity_ops.iter().cloned().collect(),
            timer_ops: transition.timer_ops.iter().cloned().collect(),
            dispatch_ops: transition.dispatch_ops.iter().cloned().collect(),
        });

    for op in &transition.request_dedupe_ops {
        let key = (
            workflow_key.0,
            workflow_key.1.clone(),
            op.request_id.0.clone(),
        );
        store.request_dedupe.insert(
            key,
            RequestRecord {
                namespace_id: state.namespace_id,
                workflow_id: state.workflow_id.clone(),
                run_id: state.run_id,
                run_key,
                request_id: op.request_id.clone(),
                first_seen_transition_seq: state.transition_seq,
                outbound_origin,
            },
        );
    }

    // Prepare derived rows before changing any map. Unchanged activities are
    // included because workflow-level priority/routing/pause can change them.
    store
        .activity_dispatch
        .retain(|(key, id), _| *key != run_key || activity_dispatch.contains_key(id));
    for (id, desired) in activity_dispatch {
        let key = (run_key, id);
        if store.activity_dispatch.get(&key) != Some(&desired) {
            store.activity_dispatch.insert(key, desired);
        }
    }

    let execution_home = tokeira_types::execution_home_bundle(
        state.namespace_id.0.as_bytes(),
        state.workflow_id.0.as_bytes(),
        InMemoryStore::effective_shard_count(store),
    );
    for op in &transition.timer_ops {
        match op {
            TimerOp::Upsert(timer) => {
                store.timer_bucket.insert(
                    timer_position(run_key, execution_home, timer),
                    timer.clone(),
                );
            }
            TimerOp::Delete { timer_id } => {
                store
                    .timer_bucket
                    .retain(|key, _| key.run_key != run_key || key.timer_id != *timer_id);
            }
        }
    }

    // Every fallible preparation completed before either map changes. The
    // same lock protects state and derived intent from partial observation.
    match workflow_dispatch {
        Some(row) => {
            store.workflow_dispatch.insert(run_key, row);
        }
        None => {
            store.workflow_dispatch.remove(&run_key);
        }
    }
    store.runs.insert(run_key, state.clone());
    store.execution_index.insert(
        (
            state.namespace_id,
            state.workflow_id.0.clone(),
            state.run_id,
        ),
        run_key,
    );

    if transition.expected_seq == tokeira_types::TransitionSeq::ZERO {
        store
            .current_execution
            .insert(workflow_key.clone(), run_key);
    }

    if state.status.is_open() {
        store.current_open.insert(workflow_key.clone(), run_key);
    } else if store.current_open.get(&workflow_key) == Some(&run_key) {
        store.current_open.remove(&workflow_key);
    }

    // The projection log carries a complete post-transition visibility
    // image. Emitting a row for every committed transition keeps the
    // in-memory reference store aligned with DSQL and prevents visibility
    // freshness from depending on whether the kernel emitted a legacy delta.
    store.append_projection_record(ProjectionRecord {
        partition_id: partition_for(run_key),
        fanout: 1,
        run_key,
        transition_seq: state.transition_seq,
        context,
    });

    store.run_shard_map.insert(run_key, execution_home);

    state
}

impl InMemoryStore {
    pub(super) async fn commit_pair(
        &self,
        run_key: RunKey,
        closing: Transition,
        successor_run_key: RunKey,
        successor: Transition,
        fence: WriteFence,
    ) -> Result<PairCommitResult> {
        crate::api::validate_transition_pair(run_key, &closing, successor_run_key, &successor)?;
        let mut store = self.inner.lock().await;
        pair_checkpoint(&mut store, 0)?;
        let home = tokeira_types::execution_home_bundle(
            closing.next_state.namespace_id.0.as_bytes(),
            closing.next_state.workflow_id.0.as_bytes(),
            Self::effective_shard_count(&store),
        );
        fence.ensure_home(home)?;
        store.check_fence(&fence)?;
        pair_checkpoint(&mut store, 1)?;
        if super::preparation::target_is_preparing(&store, successor_run_key) {
            return Err(
                crate::RecoveryError::InvalidPreparation("successor is being prepared").into(),
            );
        }
        if let Some(result) = check_run(&mut store, run_key, &closing, false) {
            return Ok(result);
        }
        pair_checkpoint(&mut store, 2)?;
        if let Some(result) = check_run(&mut store, successor_run_key, &successor, true) {
            return Ok(result);
        }
        pair_checkpoint(&mut store, 3)?;
        let workflow_key = (
            closing.next_state.namespace_id,
            closing.next_state.workflow_id.0.clone(),
        );
        // Even a closed or absent pointer is not permission to replace another
        // lineage. Temporal compares the exact predecessor id in the same
        // transaction (common/persistence/sql/execution_util.go:966-1009 @ v1.31.0).
        if store.current_execution.get(&workflow_key) != Some(&run_key) {
            return Ok(PairCommitResult::Conflict {
                reason: "current execution no longer names closing run".into(),
            });
        }
        super::successor::guard_pointer(&store, &successor.next_state)?;
        pair_checkpoint(&mut store, 4)?;
        let closing = prepare(&store, run_key, closing)?;
        pair_checkpoint(&mut store, 5)?;
        let successor = prepare(&store, successor_run_key, successor)?;
        validate_activity_batch([&closing, &successor].into_iter())?;
        pair_checkpoint(&mut store, 6)?;
        // Publication cannot fail or yield. Both complete write sets have been
        // prepared, and readers cannot acquire the lock between these writes.
        let closing_state = write(&mut store, run_key, closing);
        let successor_state = write(&mut store, successor_run_key, successor);
        Ok(PairCommitResult::Applied {
            closing_state,
            successor_state,
        })
    }
}

fn check_run(
    store: &mut StoreState,
    key: RunKey,
    transition: &Transition,
    must_be_absent: bool,
) -> Option<PairCommitResult> {
    if let Some(remaining) = store.conflict_injections.get_mut(&key)
        && *remaining > 0
    {
        *remaining -= 1;
        return Some(PairCommitResult::Conflict {
            reason: "injected pair conflict".into(),
        });
    }
    let existing = store.runs.get(&key);
    if (must_be_absent && existing.is_some())
        || existing.map_or(TransitionSeq::ZERO, |state| state.transition_seq)
            != transition.expected_seq
    {
        return Some(PairCommitResult::Conflict {
            reason: "pair run sequence or absence check failed".into(),
        });
    }
    let state = &transition.next_state;
    for op in &transition.request_dedupe_ops {
        if store
            .request_dedupe
            .get(&(
                state.namespace_id,
                state.workflow_id.0.clone(),
                op.request_id.0.clone(),
            ))
            .is_some_and(|record| record.run_id == state.run_id)
        {
            return Some(PairCommitResult::Duplicate);
        }
    }
    None
}

fn pair_checkpoint(_store: &mut StoreState, _step: usize) -> Result<()> {
    #[cfg(test)]
    if _store.pair_failure == Some(_step) {
        _store.pair_failure = None;
        anyhow::bail!("injected pair preparation failure at step {}", _step);
    }
    Ok(())
}
