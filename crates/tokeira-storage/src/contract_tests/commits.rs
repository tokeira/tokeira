//! Accepted writes are checked against authored effects; rejected writes compare
//! complete quiescent observations. Concurrent writers may choose either winner.

use std::{num::NonZeroU32, sync::Arc};

use proptest::prelude::*;
use time::Duration;
use tokeira_kernel::{
    ActivityOp, ChildWorkflowState, DispatchOp, HistoryEventKind, OperationKind, ParentClosePolicy,
    PendingExternalSignal, RequestDedupeOp, TimerOp, TimerState,
};
use tokeira_types::{
    EventPrincipal, ExecutionRef, ExecutionStatus, Payloads, QueueKey, RequestId, ShardEpoch,
    TaskKind, TaskQueueName, TransitionSeq, WorkflowType,
};

use super::*;
use crate::{
    ExecutionHomeFence, LeaseFenceLost, LeaseOutcome, ScanBound,
    memory::projection_accumulator_tests::{activity_fixture, following},
    projection_accumulator_oracle::workflow_projection_context,
};

pub(crate) fn strategy() -> impl Strategy<Value = (u8, bool, u8)> {
    (0u8..4, any::<bool>(), 1u8..100)
}

fn execution(state: &WorkflowState) -> ExecutionRef {
    ExecutionRef {
        namespace_id: state.namespace_id,
        workflow_id: state.workflow_id.clone(),
        run_id: None,
    }
}

fn effects(mut transition: Transition, marker: u8) -> Transition {
    let state = &mut transition.next_state;
    let event_id = state.last_event_id + 1;
    let timer = TimerState {
        timer_id: "contract-timer".into(),
        started_event_id: event_id,
        fire_at: now() + Duration::seconds(i64::from(marker)),
    };
    if state.timers.contains_key(&timer.timer_id) {
        transition.timer_ops.push(TimerOp::Delete {
            timer_id: timer.timer_id.clone(),
        });
    }
    state.timers.insert(timer.timer_id.clone(), timer.clone());
    transition.timer_ops.push(TimerOp::Upsert(timer));
    let mut activity = activity_fixture();
    activity.activity_id = "contract-activity".into();
    activity.schedule_event_id = event_id;
    activity.attempt = u32::from(marker) + 1;
    activity.stamp = u64::from(marker);
    activity.scheduled_at = now();
    activity.current_attempt_scheduled_at = Some(now());
    activity.started_at = None;
    state
        .activities
        .insert(activity.activity_id.clone(), activity.clone());
    transition
        .activity_ops
        .push(ActivityOp::Upsert(activity.clone()));
    transition
        .dispatch_ops
        .push(DispatchOp::EnqueueActivityTask {
            queue: QueueKey {
                namespace_id: state.namespace_id,
                task_queue: activity.task_queue.clone(),
                task_kind: TaskKind::Activity,
                deployment: None,
                build_id: None,
            },
            activity_id: activity.activity_id,
            schedule_event_id: event_id,
            attempt: u32::from(marker) + 1,
            dispatch_revision: 0,
            stamp: u64::from(marker),
            dispatch_at: now(),
            schedule_to_close_timeout: activity.schedule_to_close_timeout,
            schedule_to_start_timeout: activity.schedule_to_start_timeout,
            start_to_close_timeout: activity.start_to_close_timeout,
            heartbeat_timeout: activity.heartbeat_timeout,
            priority: None,
        });
    state.task_queue = TaskQueueName(format!("contract-{marker}"));
    if let Some(task) = &mut state.pending_workflow_task {
        task.attempt = u32::from(marker) + 1;
    }
    state.pending_external_signals.insert(
        event_id,
        PendingExternalSignal {
            initiated_event_id: event_id,
            target_namespace_id: state.namespace_id,
            target_namespace: None,
            target_workflow_id: WorkflowId(format!("signal-{marker}")),
            target_run_id: None,
            signal_name: "contract".into(),
        },
    );
    transition.request_dedupe_ops.push(RequestDedupeOp {
        request_id: RequestId(format!("request-{marker}")),
    });
    transition.history_events.push(HistoryEvent {
        event_id,
        happened_at: now(),
        kind: crate::bulk_write_tests::signal(usize::from(marker), 8),
    });
    transition.event_principals.push(Some(EventPrincipal {
        principal_type: "fixture".into(),
        name: format!("writer-{marker}"),
    }));
    state.last_event_id = event_id;
    transition
}

fn child(state: &mut WorkflowState) {
    let id = WorkflowId("contract-child".into());
    state.children.insert(
        id.clone(),
        ChildWorkflowState {
            child_workflow_id: id,
            namespace_id: state.namespace_id,
            namespace: None,
            workflow_type: WorkflowType("fixture".into()),
            header: None,
            child_run_id: None,
            initiated_event_id: 1,
            started_event_id: None,
            parent_close_policy: ParentClosePolicy::Terminate,
        },
    );
}

fn close(transition: &mut Transition) {
    transition.next_state.status = ExecutionStatus::Completed;
    transition.next_state.closed_at = Some(now());
    transition.next_state.pending_workflow_task = None;
    transition.next_state.last_event_id += 1;
    transition.history_events.push(HistoryEvent {
        event_id: transition.next_state.last_event_id,
        happened_at: now(),
        kind: HistoryEventKind::WorkflowExecutionCompleted {
            workflow_task_completed_event_id: 0,
            result: Payloads::default(),
            new_execution_run_id: None,
        },
    });
    transition.event_principals.push(None);
}

async fn verify(
    backend: &impl ContractBackend,
    transition: &Transition,
    history: &[HistoryEvent],
    principals: &[Option<EventPrincipal>],
    marker: u8,
) -> Result<()> {
    let state = &transition.next_state;
    equal(
        &backend.repo().load_run(state.run_key).await?,
        &LoadedRun::Existing(state.clone()),
        "accepted state",
    )?;
    equal(
        &backend.repo().read_history_to_end(state.run_key, 0).await?,
        &history.to_vec(),
        "accepted history",
    )?;
    equal(
        &history.len(),
        &principals.len(),
        "authored attribution shape",
    )?;
    equal(
        &backend
            .repo()
            .read_attributed_history_to_end(state.run_key, 0)
            .await?,
        &history
            .iter()
            .cloned()
            .zip(principals.iter().cloned())
            .map(|(event, principal)| AttributedHistoryEvent { event, principal })
            .collect(),
        "accepted event attribution",
    )?;
    equal(
        &backend.repo().resolve_execution(&execution(state)).await?,
        &state.status.is_open().then_some(state.run_key),
        "open current pointer",
    )?;
    equal(
        &backend
            .repo()
            .find_latest_run(state.namespace_id, &state.workflow_id)
            .await?,
        &Some(state.run_key),
        "retained current pointer",
    )?;
    let dedupe = backend
        .repo()
        .lookup_request_dedupe(&execution(state), &RequestId(format!("request-{marker}")))
        .await?
        .context("missing accepted dedupe")?;
    ensure!(
        dedupe.run_key == state.run_key
            && dedupe.run_id == state.run_id
            && dedupe.first_seen_transition_seq == state.transition_seq
            && dedupe.outbound_origin.is_none(),
        "dedupe differs from authored transition"
    );
    let timers = backend
        .repo()
        .list_due_timers_for_shard(HOMES[0], now() + Duration::days(1), None, 100)
        .await?;
    equal(
        &timers,
        &vec![crate::DueTimer {
            run_key: state.run_key,
            timer_id: "contract-timer".into(),
            fire_at: now() + Duration::seconds(i64::from(marker)),
        }],
        "winner's timer",
    )?;
    let activities = backend
        .repo()
        .list_due_dispatchable_activity_tasks_for_shard(HOMES[0], now(), None, 100)
        .await?;
    let activity = &state.activities["contract-activity"];
    equal(
        &activities,
        &vec![crate::DueActivityDispatch {
            task: crate::DispatchableActivityTask {
                run_key: state.run_key,
                queue: QueueKey {
                    namespace_id: state.namespace_id,
                    task_queue: activity.task_queue.clone(),
                    task_kind: TaskKind::Activity,
                    deployment: None,
                    build_id: None,
                },
                activity_id: "contract-activity".into(),
                schedule_event_id: activity.schedule_event_id,
                attempt: u32::from(marker) + 1,
                dispatch_revision: 0,
                stamp: u64::from(marker),
                priority: None,
                order: None,
            },
            dispatch_at: now(),
        }],
        "complete activity dispatch identity",
    )?;
    let workflow = backend
        .repo()
        .list_workflow_dispatch_for_home(HOMES[0], None, NonZeroU32::new(100).unwrap())
        .await?;
    equal(
        &workflow,
        &if state.status.is_open() {
            vec![state.run_key]
        } else {
            Vec::new()
        },
        "workflow dispatch membership",
    )?;
    let operations = backend
        .repo()
        .read_operation_page(
            HOMES[0],
            OperationKind::ExternalSignal,
            ScanBound::Capture,
            None,
            100,
        )
        .await?
        .rows;
    equal(
        &operations.len(),
        &if state.status.is_open() {
            state.pending_external_signals.len()
        } else {
            0
        },
        "operation membership",
    )?;
    for operation in operations {
        let input = &state.pending_external_signals[&operation.incarnation.key.initiating_event];
        ensure!(
            operation.incarnation.key.source_run == state.run_key
                && operation.incarnation.key.kind == OperationKind::ExternalSignal
                && operation.incarnation.requested_event == 0
                && operation.incarnation.attempt == 0
                && operation.source_home == HOMES[0]
                && operation.due_at == now()
                && operation.target == Some((state.namespace_id, input.target_workflow_id.clone())),
            "operation differs from authored identity"
        );
    }
    let (_, stats) = backend.repo().load_run_with_stats(state.run_key).await?;
    let mut expected_dispatch = crate::workflow_dispatch_tests::reference(state);
    if let Some(row) = &mut expected_dispatch {
        row.execution_home = HOMES[0];
    }
    equal(
        &backend.workflow_row(state.run_key).await?,
        &expected_dispatch,
        "complete workflow dispatch image",
    )?;
    let images = backend.log().read_page(3, 1, None, 100).await?.images;
    let image = images
        .iter()
        .find(|row| row.run_key == state.run_key && row.transition_seq == state.transition_seq)
        .context("missing accepted image")?;
    equal(
        &image.context,
        &workflow_projection_context(state, stats.history_size_bytes)?,
        "accepted projection context",
    )?;
    if !state.status.is_open() {
        let intents = backend
            .repo()
            .read_intent_page(HOMES[0], ScanBound::Capture, None, 100)
            .await?
            .rows;
        ensure!(
            intents.len() == 1
                && intents[0].key.source_run == state.run_key
                && intents[0].key.kind == crate::IntentKind::ParentClose,
            "close lost its child intent"
        );
        let intent = backend
            .repo()
            .load_intent(intents[0].key)
            .await?
            .context("missing close recipe")?;
        equal(
            &intent,
            &crate::IntentRow {
                key: crate::IntentKey {
                    source_run: state.run_key,
                    close_seq: state.transition_seq,
                    kind: crate::IntentKind::ParentClose,
                    ordinal: 0,
                },
                source_home: HOMES[0],
                created_at: now(),
                target: Some((state.namespace_id, WorkflowId("contract-child".into()))),
                payload: crate::IntentPayload::ParentClose {
                    namespace_id: state.namespace_id,
                    workflow_id: WorkflowId("contract-child".into()),
                    initiating_event: 1,
                    first_child_run: None,
                    policy: ParentClosePolicy::Terminate,
                },
            },
            "complete close recipe",
        )?;
    }
    Ok(())
}

// Feature: storage-contract-tests, Property 5: Commit outcomes publish all or none of the attempted writes
pub(crate) async fn outcomes(
    backend: &impl ContractBackend,
    (outcome, bundle, marker): (u8, bool, u8),
) -> Result<()> {
    let mut scope = FixtureScope::new(2);
    scope.place(0, HOMES[0]);
    child(&mut scope.states[0]);
    scope.states[1].workflow_id = scope.states[0].workflow_id.clone();
    scoped(backend, &scope, async {
        let baseline = effects(start(&scope.states[0]), 0);
        let state = apply(backend, baseline.clone()).await?;
        let mut attempt = effects(following(&state), marker);
        // Both successful and rejected closes exercise immutable intents as well
        // as ordinary operations. Its child was present in the committed before-state.
        if marker % 2 == 0 {
            close(&mut attempt);
        }
        match outcome {
            1 => attempt.expected_seq = TransitionSeq::ZERO,
            2 => attempt.request_dedupe_ops[0].request_id = RequestId("request-0".into()),
            3 => attempt = effects(start(&scope.states[1]), marker),
            _ => {}
        }
        let before = observe(backend, &scope).await?;
        let result = commit(
            backend,
            attempt.clone(),
            bundle,
            HOMES[0],
            WriteFence::Unfenced,
        )
        .await?;
        match (outcome, result) {
            (0, CommitResult::Applied { new_state }) => {
                equal(&new_state, &attempt.next_state, "accepted return value")?;
                let history: Vec<_> = baseline
                    .history_events
                    .iter()
                    .chain(&attempt.history_events)
                    .cloned()
                    .collect();
                let principals: Vec<_> = baseline
                    .event_principals
                    .iter()
                    .chain(&attempt.event_principals)
                    .cloned()
                    .collect();
                verify(backend, &attempt, &history, &principals, marker).await?;
            }
            (1, CommitResult::Conflict { .. }) | (2, CommitResult::Duplicate) => equal(
                &observe(backend, &scope).await?,
                &before,
                "refused write changed storage",
            )?,
            (
                3,
                CommitResult::CurrentExecutionConflict {
                    existing_run_key,
                    existing_status,
                    request_ids,
                },
            ) => {
                ensure!(
                    existing_run_key == state.run_key && existing_status == state.status,
                    "wrong incumbent"
                );
                equal(
                    &request_ids,
                    &state
                        .request_id_infos
                        .iter()
                        .map(|(id, info)| (id.clone(), info.clone()))
                        .collect(),
                    "incumbent request IDs",
                )?;
                equal(
                    &observe(backend, &scope).await?,
                    &before,
                    "losing start changed storage",
                )?;
            }
            (_, other) => anyhow::bail!("unexpected outcome {outcome}: {other:?}"),
        }
        Ok(())
    })
    .await
}

// Feature: storage-contract-tests, Property 6: Competing writers leave one coherent winner
pub(crate) async fn competing(
    backend: &(impl ContractBackend + Clone + Send + 'static),
    (mode, bundle, marker): (u8, bool, u8),
) -> Result<()> {
    let mut scope = FixtureScope::new(2);
    scope.place(0, HOMES[0]);
    scope.states[1].workflow_id = scope.states[0].workflow_id.clone();
    scoped(backend, &scope, async {
        let starts = mode % 2 == 0;
        let base = if starts {
            None
        } else {
            Some(apply(backend, start(&scope.states[0])).await?)
        };
        let first = effects(
            base.as_ref()
                .map_or_else(|| start(&scope.states[0]), following),
            marker,
        );
        let second = effects(
            base.as_ref()
                .map_or_else(|| start(&scope.states[1]), following),
            marker + 100,
        );
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let write = |transition: Transition| {
            let backend = (*backend).clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                commit(&backend, transition, bundle, HOMES[0], WriteFence::Unfenced)
                    .await
                    .map(Box::new)
            })
        };
        // Poll each writer as its own runtime task instead of nesting both DSQL
        // paths beneath this case's stack frame. Box the large results too, and
        // join both tasks before observing or cleaning their shared store.
        let (left, right) = tokio::join!(write(first.clone()), write(second.clone()));
        let (left, right) = (left??, right??);
        let (winner, loser, winner_marker) = match (left.as_ref(), right.as_ref()) {
            (
                CommitResult::Applied { .. },
                CommitResult::Conflict { .. } | CommitResult::CurrentExecutionConflict { .. },
            ) => (&first, &second, marker),
            (
                CommitResult::Conflict { .. } | CommitResult::CurrentExecutionConflict { .. },
                CommitResult::Applied { .. },
            ) => (&second, &first, marker + 100),
            _ => anyhow::bail!("race did not choose one winner: {left:?}, {right:?}"),
        };
        verify(
            backend,
            winner,
            &winner.history_events,
            &winner.event_principals,
            winner_marker,
        )
        .await?;
        let losing_request = &loser.request_dedupe_ops[0].request_id;
        ensure!(
            backend
                .repo()
                .lookup_request_dedupe(&execution(&winner.next_state), losing_request)
                .await?
                .is_none(),
            "losing dedupe survived"
        );
        if starts {
            let loser_scope = FixtureScope {
                states: vec![loser.next_state.clone()],
            };
            ensure!(
                backend
                    .rows(&loser_scope)
                    .await?
                    .values()
                    .all(Vec::is_empty),
                "losing start left orphan rows"
            );
            equal(
                &backend.repo().load_run(loser.next_state.run_key).await?,
                &LoadedRun::Absent,
                "losing hot state",
            )?;
        }
        Ok(())
    })
    .await
}

async fn acquire(backend: &impl ContractBackend, owner: &str) -> Result<ShardEpoch> {
    match backend
        .leases()
        .try_acquire_bundle(HOMES[0], owner.to_owned(), "fixture".into())
        .await?
    {
        LeaseOutcome::Acquired { epoch } => Ok(epoch),
        outcome => anyhow::bail!("fixture home is not available: {outcome:?}"),
    }
}

// Feature: storage-contract-tests, Property 7: An old lease cannot authorize a later transition
pub(crate) async fn fence(
    backend: &impl ContractBackend,
    (_, bundle, marker): (u8, bool, u8),
) -> Result<()> {
    let mut scope = FixtureScope::new(1);
    scope.place(0, HOMES[0]);
    let owner = format!("contract-{}", scope.key(0).0);
    let epoch = acquire(backend, &owner).await?;
    let result = scoped(backend, &scope, async {
        let state = apply(backend, effects(start(&scope.states[0]), 0)).await?;
        backend.expire_lease(HOMES[0]).await?;
        let newer = acquire(backend, &format!("{owner}-new")).await?;
        ensure!(newer != epoch, "takeover did not advance epoch");
        let before = observe(backend, &scope).await?;
        let result = commit(
            backend,
            effects(following(&state), marker),
            bundle,
            HOMES[0],
            WriteFence::Lease(ExecutionHomeFence {
                home: HOMES[0],
                owner: owner.clone(),
                epoch,
            }),
        )
        .await;
        ensure!(
            result
                .as_ref()
                .is_err_and(|error| error.downcast_ref::<LeaseFenceLost>().is_some()),
            "old lease was not refused: {result:?}"
        );
        equal(
            &observe(backend, &scope).await?,
            &before,
            "lost fence changed storage",
        )
    })
    .await;
    let cleanup = backend.clear_lease(HOMES[0], &owner).await;
    match (result, cleanup) {
        (Err(error), Err(cleanup)) => {
            Err(error.context(format!("lease cleanup also failed: {cleanup:#}")))
        }
        (Err(error), _) | (_, Err(error)) => Err(error),
        _ => Ok(()),
    }
}
