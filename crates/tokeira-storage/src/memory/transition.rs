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
    } = prepared;
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
            },
        );
    }

    // The activity itself lives only in the run's state; an activity op
    // maintains its dispatch entry and records nothing else, as on DSQL
    // (`activity-state-writes` criterion 2.1).
    for op in &transition.activity_ops {
        match op {
            ActivityOp::Upsert(activity) => {
                // Dispatch entries only survive while the activity remains
                // eligible to be offered to a worker.
                if activity.started_at.is_some() || activity.pause_info.is_some() {
                    store
                        .activity_dispatch
                        .remove(&(run_key, activity.activity_id.clone()));
                } else if let Some(entry) = store
                    .activity_dispatch
                    .get_mut(&(run_key, activity.activity_id.clone()))
                {
                    entry.task.queue = QueueKey {
                        namespace_id: state.namespace_id,
                        task_queue: activity.task_queue.clone(),
                        task_kind: TaskKind::Activity,
                        deployment: activity
                            .deployment
                            .clone()
                            .or_else(|| state.deployment.clone()),
                        build_id: activity.build_id.clone().or_else(|| state.build_id.clone()),
                    };
                    entry.task.schedule_event_id = activity.schedule_event_id;
                    entry.task.attempt = activity.attempt;
                    // Keep the dispatch row's stamp in step with the activity
                    // so a recovery-reconstructed task is never spuriously
                    // fenced at start.
                    entry.task.stamp = activity.stamp;
                    entry.task.priority =
                        merge_priority(state.priority.as_ref(), activity.priority.as_ref());
                    // Options/retry updates re-anchor eligibility from the
                    // authoritative attempt schedule; a state without one
                    // (legacy/initial) keeps the row's existing time.
                    entry.dispatch_at = activity
                        .current_attempt_scheduled_at
                        .unwrap_or(entry.dispatch_at);
                    entry.schedule_to_close_timeout = activity.schedule_to_close_timeout;
                    entry.schedule_to_start_timeout = activity.schedule_to_start_timeout;
                    entry.start_to_close_timeout = activity.start_to_close_timeout;
                    entry.heartbeat_timeout = activity.heartbeat_timeout;
                }
            }
            ActivityOp::Delete { activity_id } => {
                store
                    .activity_dispatch
                    .remove(&(run_key, activity_id.clone()));
            }
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

    for op in &transition.dispatch_ops {
        if let DispatchOp::EnqueueActivityTask {
            queue,
            activity_id,
            schedule_event_id,
            attempt,
            dispatch_revision,
            stamp,
            dispatch_at,
            schedule_to_close_timeout,
            schedule_to_start_timeout,
            start_to_close_timeout,
            heartbeat_timeout,
            priority,
        } = op
        {
            // Enqueue is the only state transition effect that creates an
            // activity dispatch entry. Later ActivityOp::Upsert values may
            // update or remove this entry, but they cannot create it.
            store.activity_dispatch.insert(
                (run_key, activity_id.clone()),
                ActivityDispatchEntry {
                    task: DispatchableActivityTask {
                        run_key,
                        queue: queue.clone(),
                        activity_id: activity_id.clone(),
                        schedule_event_id: *schedule_event_id,
                        attempt: *attempt,
                        dispatch_revision: *dispatch_revision,
                        stamp: *stamp,
                        priority: priority.clone(),
                        order: None,
                    },
                    dispatch_at: *dispatch_at,
                    schedule_to_close_timeout: *schedule_to_close_timeout,
                    schedule_to_start_timeout: *schedule_to_start_timeout,
                    start_to_close_timeout: *start_to_close_timeout,
                    heartbeat_timeout: *heartbeat_timeout,
                },
            );
        }
    }
    if state.status == ExecutionStatus::Paused {
        // Workflow pause suppresses all queued activity dispatch for this
        // run; its activities stay in run state for the unpause to re-enqueue.
        store
            .activity_dispatch
            .retain(|(entry_run_key, _), _| entry_run_key != &run_key);
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
        pair_checkpoint(&mut store, 4)?;
        let closing = prepare(&store, run_key, closing)?;
        pair_checkpoint(&mut store, 5)?;
        let successor = prepare(&store, successor_run_key, successor)?;
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
