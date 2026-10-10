//! Shared generated storage traces, with an oracle expressed from fixture
//! coordinates rather than calls to production row derivation.

use std::num::NonZeroU32;

use anyhow::Result;
use async_trait::async_trait;
use proptest::{
    prelude::*,
    test_runner::{TestCaseResult, TestRunner},
};
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    BasicKernel, Command, Kernel, LoadedRun, Priority, RequestDedupeOp, Transition, WorkflowState,
    WorkflowTaskTimedOutRequest, WorkflowTaskTimeoutType, WorkflowTaskType,
};
use tokeira_types::{
    BuildId, DeploymentId, ExecutionStatus, LogicalTaskSeq, RequestId, RunId, RunKey, ShardEpoch,
    ShardId, StickyAffinity, TaskQueueName, WorkerIdentity,
};

use crate::{
    CommitResult, DeleteRunRequest, DeleteRunResult, RunRepository, WorkflowDiscoveryRange,
    WorkflowDispatchRouting, WorkflowDispatchRow, WorkflowTaskIncarnation,
    memory::projection_accumulator_tests::{applied, following, fresh_transition, reset_history},
};

#[async_trait]
pub(crate) trait Backend {
    fn repo(&self) -> &dyn RunRepository;
    async fn row(&self, key: RunKey) -> Result<Option<WorkflowDispatchRow>>;
    async fn seed_stale_row(&self, state: &WorkflowState) -> Result<()>;
}

pub(crate) async fn commit(backend: &impl Backend, transition: Transition) -> Result<CommitResult> {
    for attempt in 0..10 {
        let result = if transition.next_state.transition_seq.0.is_multiple_of(2) {
            backend
                .repo()
                .commit_transition_for_bundle(
                    transition.next_state.run_key,
                    ShardId(0),
                    transition.clone(),
                    ShardEpoch::ZERO,
                )
                .await?
        } else {
            backend
                .repo()
                .commit_transition(
                    transition.next_state.run_key,
                    transition.clone(),
                    ShardEpoch::ZERO,
                )
                .await?
        };
        if attempt < 9
            && matches!(&result, CommitResult::Conflict { reason } if reason == "DSQL serialization conflict")
        {
            continue;
        }
        return Ok(result);
    }
    unreachable!("last attempt returns")
}

fn reference(state: &WorkflowState) -> Option<WorkflowDispatchRow> {
    let pending = state.pending_workflow_task.as_ref()?;
    match (state.status, pending.started_event_id, pending.task_type) {
        (ExecutionStatus::Running, None, WorkflowTaskType::Normal) => {}
        _ => return None,
    }
    let affinity = match (&state.sticky, pending.schedule_to_start_deadline) {
        (Some(affinity), Some(_)) if !affinity.sticky_queue.0.is_empty() => Some(affinity),
        _ => None,
    };
    let key = state
        .priority
        .as_ref()
        .map_or(0, |priority| priority.priority_key);
    Some(WorkflowDispatchRow {
        incarnation: WorkflowTaskIncarnation {
            run_key: state.run_key,
            logical_seq: pending.logical_seq,
        },
        execution_home: ShardId(0),
        namespace_id: state.namespace_id,
        queue_name: TaskQueueName(
            affinity
                .map_or(&state.task_queue.0, |affinity| &affinity.sticky_queue.0)
                .clone(),
        ),
        normal_queue_name: state.task_queue.clone(),
        sticky: affinity.is_some(),
        routing: match &state.deployment {
            Some(deployment) => WorkflowDispatchRouting::Exact {
                deployment: deployment.clone(),
                build_id: state.build_id.clone(),
            },
            None => WorkflowDispatchRouting::Live,
        },
        scheduled_at: pending.scheduled_at,
        priority_key: match key {
            0 => 3,
            i32::MIN..=1 => 1,
            2 => 2,
            3 => 3,
            4 => 4,
            _ => 5,
        },
        priority: state.priority.clone(),
        sticky_worker: affinity.map(|affinity| affinity.worker_identity.clone()),
        schedule_to_start_deadline: pending.schedule_to_start_deadline,
    })
}

pub(crate) fn run_atomic_cases(backend: &impl Backend, runtime: &tokio::runtime::Runtime) {
    let strategy = (
        0usize..4,
        prop::collection::vec(
            (
                0u8..5,
                0u8..4,
                any::<bool>(),
                -2i32..8,
                prop::option::of("[ab]{0,3}"),
            ),
            1..12,
        ),
    );
    let mut runner = TestRunner::new(ProptestConfig {
        cases: 100,
        failure_persistence: None,
        ..ProptestConfig::default()
    });
    runner
        .run(&strategy, |(reset, steps)| {
            runtime.block_on(atomic_case(backend, reset, steps))
        })
        .unwrap();
}

pub(crate) fn run_page_cases(backend: &impl Backend, runtime: &tokio::runtime::Runtime) {
    let strategy = (prop::collection::vec((1i32..6, 0i64..4), 2..24), 1u32..9);
    let mut runner = TestRunner::new(ProptestConfig {
        cases: 100,
        failure_persistence: None,
        ..ProptestConfig::default()
    });
    runner
        .run(&strategy, |(coordinates, page_size)| {
            runtime.block_on(async {
                // Feature: workflow-dispatch, Property 3: Ordered read-only traversal
                // Every page advances in tuple order; a new head sees work moved behind its cursor.
                let mut template = fresh_transition(RunKey::new());
                template.next_state.namespace_id = tokeira_types::NamespaceId::new();
                let range = WorkflowDiscoveryRange {
                    namespace_id: template.next_state.namespace_id,
                    queue_name: template.next_state.task_queue.clone(),
                    routing: WorkflowDispatchRouting::Live,
                };
                let mut states = Vec::new();
                let mut homes = std::collections::BTreeSet::new();
                for (index, (priority, scheduled)) in coordinates.iter().enumerate() {
                    let mut transition = template.clone();
                    transition.next_state.run_key = RunKey::new();
                    transition.next_state.workflow_id.0 = format!("traversal-{index}");
                    let mut home = tokeira_types::execution_home_bundle(
                        range.namespace_id.0.as_bytes(),
                        transition.next_state.workflow_id.0.as_bytes(),
                        8,
                    );
                    if index == 1 {
                        for suffix in 0..100 {
                            if !homes.contains(&home) {
                                break;
                            }
                            transition.next_state.workflow_id.0 =
                                format!("traversal-{index}-{suffix}");
                            home = tokeira_types::execution_home_bundle(
                                range.namespace_id.0.as_bytes(),
                                transition.next_state.workflow_id.0.as_bytes(),
                                8,
                            );
                        }
                    }
                    homes.insert(home);
                    transition.next_state.priority = Some(Priority {
                        priority_key: *priority,
                        fairness_key: String::new(),
                        fairness_weight: 1.0,
                    });
                    transition
                        .next_state
                        .pending_workflow_task
                        .as_mut()
                        .unwrap()
                        .scheduled_at = OffsetDateTime::UNIX_EPOCH + Duration::seconds(*scheduled);
                    states.push(applied(commit(backend, transition).await.unwrap()));
                    prop_assert_eq!(
                        backend
                            .row(states.last().unwrap().run_key)
                            .await
                            .unwrap()
                            .unwrap()
                            .execution_home,
                        home
                    );
                }
                prop_assert!(homes.len() > 1);
                let mut expected: Vec<_> = states
                    .iter()
                    .map(|state| {
                        (
                            state.priority.as_ref().unwrap().priority_key as i16,
                            state.pending_workflow_task.as_ref().unwrap().scheduled_at,
                            state.run_key,
                        )
                    })
                    .collect();
                expected.sort();
                let mut before = Vec::new();
                for state in &states {
                    before.push(backend.row(state.run_key).await.unwrap());
                }
                let mut seen = Vec::new();
                let mut after = None;
                loop {
                    let page = backend
                        .repo()
                        .list_workflow_dispatch_page(
                            &range,
                            after,
                            NonZeroU32::new(page_size).unwrap(),
                        )
                        .await
                        .unwrap();
                    prop_assert!(page.candidates.len() <= page_size as usize);
                    for row in &page.candidates {
                        prop_assert!(after.is_none_or(|position| row.position() > position));
                        seen.push((row.priority_key, row.scheduled_at, row.incarnation.run_key));
                    }
                    prop_assert_eq!(
                        page.last_examined,
                        page.candidates.last().map(WorkflowDispatchRow::position)
                    );
                    after = page.last_examined;
                    if page.exhausted {
                        break;
                    }
                }
                prop_assert_eq!(&seen, &expected);
                for (state, row) in states.iter().zip(before) {
                    prop_assert_eq!(backend.row(state.run_key).await.unwrap(), row);
                    let LoadedRun::Existing(loaded) =
                        backend.repo().load_run(state.run_key).await.unwrap()
                    else {
                        panic!("run missing")
                    };
                    prop_assert_eq!(loaded.transition_seq, state.transition_seq);
                }
                let first = backend
                    .repo()
                    .list_workflow_dispatch_page(&range, None, NonZeroU32::new(1).unwrap())
                    .await
                    .unwrap();
                let last_key = expected.last().unwrap().2;
                let state = states
                    .iter()
                    .find(|state| state.run_key == last_key)
                    .unwrap();
                let mut moved = following(state);
                moved.next_state.priority.as_mut().unwrap().priority_key = 1;
                moved
                    .next_state
                    .pending_workflow_task
                    .as_mut()
                    .unwrap()
                    .scheduled_at = OffsetDateTime::UNIX_EPOCH - Duration::seconds(1);
                applied(
                    backend
                        .repo()
                        .commit_transition(last_key, moved, ShardEpoch::ZERO)
                        .await
                        .unwrap(),
                );
                let continuation = backend
                    .repo()
                    .list_workflow_dispatch_page(
                        &range,
                        first.last_examined,
                        NonZeroU32::new(64).unwrap(),
                    )
                    .await
                    .unwrap();
                prop_assert!(
                    continuation
                        .candidates
                        .iter()
                        .all(|row| row.incarnation.run_key != last_key)
                );
                let head = backend
                    .repo()
                    .list_workflow_dispatch_page(&range, None, NonZeroU32::new(1).unwrap())
                    .await
                    .unwrap();
                prop_assert_eq!(head.candidates[0].incarnation.run_key, last_key);
                Ok(())
            })
        })
        .unwrap();
}

pub(crate) fn run_sticky_recovery_cases(backend: &impl Backend, runtime: &tokio::runtime::Runtime) {
    let strategy = (any::<bool>(), 0u8..5, -100i64..100, any::<bool>());
    let mut runner = TestRunner::new(ProptestConfig {
        cases: 100,
        failure_persistence: None,
        ..ProptestConfig::default()
    });
    runner
        .run(&strategy, |(affinity, shape, seconds, legacy)| {
            runtime.block_on(async {
                // Feature: workflow-dispatch, Property 7: Sticky recovery and affinity independence
                // Reloaded pending deadlines depend on task state, never the affinity hint or recovery clock.
                let mut transition = fresh_transition(RunKey::new());
                let state = &mut transition.next_state;
                state.namespace_id = tokeira_types::NamespaceId::new();
                let deadline = OffsetDateTime::UNIX_EPOCH + Duration::seconds(seconds);
                let pending = state.pending_workflow_task.as_mut().unwrap();
                pending.schedule_to_start_deadline = (!legacy).then_some(deadline);
                if shape == 1 {
                    pending.task_type = WorkflowTaskType::Speculative;
                }
                if shape == 2 {
                    pending.started_event_id = Some(3);
                    pending.started_at = Some(deadline);
                }
                if shape == 3 {
                    state.status = ExecutionStatus::Paused;
                }
                if shape == 4 {
                    state.status = ExecutionStatus::Completed;
                }
                state.sticky = affinity.then(|| StickyAffinity {
                    worker_identity: WorkerIdentity("sticky-worker".into()),
                    sticky_queue: TaskQueueName("sticky-q".into()),
                    schedule_to_start_timeout: Duration::seconds(5),
                });
                let expected_sequence = pending.logical_seq;
                state.next_workflow_task_seq = LogicalTaskSeq(expected_sequence.0 + 1);
                let state = applied(commit(backend, transition).await.unwrap());
                let LoadedRun::Existing(loaded) =
                    backend.repo().load_run(state.run_key).await.unwrap()
                else {
                    panic!("run missing")
                };
                let entries = crate::recovery_entries(&loaded);
                let expected = !legacy && matches!(shape, 0 | 3);
                prop_assert_eq!(entries.sticky_deadline.is_some(), expected);
                if let Some(entry) = entries.sticky_deadline {
                    prop_assert_eq!(entry.deadline, deadline);
                    prop_assert_eq!(entry.logical_seq, expected_sequence);
                    prop_assert!(crate::recovery_needed(&loaded));
                }
                prop_assert_eq!(&loaded.sticky, &state.sticky);
                if shape == 0 && legacy {
                    prop_assert!(!backend.row(state.run_key).await.unwrap().unwrap().sticky);
                }
                if expected {
                    let timed_out = BasicKernel
                        .apply(
                            LoadedRun::Existing(loaded),
                            Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest {
                                logical_seq: expected_sequence,
                                started_event_id: 0,
                                timeout_type: WorkflowTaskTimeoutType::ScheduleToStart,
                                now: deadline,
                            }),
                        )
                        .unwrap();
                    let replacement = applied(commit(backend, timed_out).await.unwrap());
                    prop_assert!(replacement.sticky.is_none());
                    prop_assert!(
                        crate::recovery_entries(&replacement)
                            .sticky_deadline
                            .is_none()
                    );
                    if shape == 0 {
                        let row = backend.row(state.run_key).await.unwrap().unwrap();
                        prop_assert!(!row.sticky);
                        prop_assert!(row.incarnation.logical_seq > expected_sequence);
                        let page = backend
                            .repo()
                            .list_workflow_dispatch_page(
                                &WorkflowDiscoveryRange {
                                    namespace_id: replacement.namespace_id,
                                    queue_name: replacement.task_queue.clone(),
                                    routing: row.routing,
                                },
                                None,
                                NonZeroU32::new(64).unwrap(),
                            )
                            .await
                            .unwrap();
                        prop_assert!(
                            page.candidates
                                .iter()
                                .any(|row| row.incarnation.run_key == state.run_key)
                        );
                    } else {
                        prop_assert!(backend.row(state.run_key).await.unwrap().is_none());
                    }
                }
                Ok(())
            })
        })
        .unwrap();
}

// Feature: workflow-dispatch, Property 1: Atomic derived equality
// Rejection, deduplication, and rollback leave both state and dispatch at the preceding commit.
async fn atomic_case(
    backend: &impl Backend,
    reset: usize,
    steps: Vec<(u8, u8, bool, i32, Option<String>)>,
) -> TestCaseResult {
    let mut initial = fresh_transition(RunKey::new());
    let history = reset_history(&initial.next_state);
    initial.next_state.last_event_id = history.last().unwrap().event_id;
    initial.event_principals = vec![None; history.len()].into();
    initial.history_events = history.into();
    let template = initial.next_state.pending_workflow_task.clone().unwrap();
    let mut state = applied(commit(backend, initial).await.unwrap());
    let original = state.clone();
    if reset > 0 {
        let run_id = RunId::new();
        let key = RunKey::derive(state.namespace_id, &state.workflow_id, run_id);
        let expected = backend
            .repo()
            .find_latest_run(state.namespace_id, &state.workflow_id)
            .await
            .unwrap();
        backend
            .repo()
            .materialize_reset_successor(state.run_key, [0, 3, 4, 7][reset], run_id, expected)
            .await
            .unwrap();
        let LoadedRun::Existing(successor) = backend.repo().load_run(key).await.unwrap() else {
            panic!("reset successor missing")
        };
        prop_assert_eq!(backend.row(key).await.unwrap(), reference(&successor));
        prop_assert_eq!(backend.row(state.run_key).await.unwrap(), reference(&state));
        prop_assert!(
            successor.next_workflow_task_seq
                > successor
                    .pending_workflow_task
                    .as_ref()
                    .unwrap()
                    .logical_seq
        );
    }
    for (index, (shape, sticky, exact, priority_key, build)) in steps.into_iter().enumerate() {
        let mut transition = following(&state);
        let next = &mut transition.next_state;
        next.status = if shape == 2 {
            ExecutionStatus::Paused
        } else {
            ExecutionStatus::Running
        };
        let mut pending = template.clone();
        pending.logical_seq = LogicalTaskSeq(index as u64 + 2);
        pending.attempt = index as u32 + 1;
        pending.scheduled_at = OffsetDateTime::UNIX_EPOCH + Duration::seconds(index as i64);
        pending.started_event_id = (shape == 1).then_some(2);
        pending.task_type = if shape == 3 {
            WorkflowTaskType::Speculative
        } else {
            WorkflowTaskType::Normal
        };
        pending.schedule_to_start_deadline =
            (sticky >= 2).then_some(OffsetDateTime::UNIX_EPOCH + Duration::seconds(60));
        next.pending_workflow_task = (shape != 4).then_some(pending);
        next.sticky = (sticky == 1 || sticky == 2).then_some(StickyAffinity {
            sticky_queue: TaskQueueName("sticky".into()),
            schedule_to_start_timeout: Duration::seconds(60),
            worker_identity: WorkerIdentity("worker".into()),
        });
        next.deployment = exact.then_some(DeploymentId("deployment".into()));
        next.build_id = build.map(BuildId);
        next.priority = Some(Priority {
            priority_key,
            fairness_key: format!("key-{index}"),
            fairness_weight: 1.25,
        });
        transition.request_dedupe_ops.push(RequestDedupeOp {
            request_id: RequestId(format!("step-{index}")),
        });
        let mut stale = original.clone();
        stale.task_queue = TaskQueueName("stale-destination".into());
        backend.seed_stale_row(&stale).await.unwrap();
        state = applied(commit(backend, transition.clone()).await.unwrap());
        prop_assert_eq!(backend.row(state.run_key).await.unwrap(), reference(&state));

        let mut duplicate = transition.clone();
        duplicate.expected_seq = state.transition_seq;
        duplicate.next_state.transition_seq = state.transition_seq.next();
        duplicate.next_state.pending_workflow_task = None;
        prop_assert!(matches!(
            commit(backend, duplicate).await.unwrap(),
            CommitResult::Duplicate
        ));
        prop_assert!(
            matches!(
                commit(backend, transition).await.unwrap(),
                CommitResult::Conflict { .. }
            ),
            "stale commit must conflict"
        );
        prop_assert_eq!(backend.row(state.run_key).await.unwrap(), reference(&state));

        // Invalid SQL identity fails after DSQL's hot-state statement; memory
        // validates before its first map mutation. Neither may expose the copy.
        let mut invalid = following(&state);
        invalid.next_state.status = ExecutionStatus::Running;
        let mut invalid_pending = template.clone();
        invalid_pending.logical_seq = LogicalTaskSeq(u64::MAX);
        invalid.next_state.pending_workflow_task = Some(invalid_pending);
        prop_assert!(commit(backend, invalid).await.is_err());
        let LoadedRun::Existing(loaded) = backend.repo().load_run(state.run_key).await.unwrap()
        else {
            panic!("committed run missing")
        };
        prop_assert_eq!(loaded.transition_seq, state.transition_seq);
        prop_assert_eq!(backend.row(state.run_key).await.unwrap(), reference(&state));
    }
    let mut close = following(&state);
    close.next_state.status = ExecutionStatus::Completed;
    close.next_state.closed_at = Some(OffsetDateTime::UNIX_EPOCH);
    state = applied(commit(backend, close).await.unwrap());
    prop_assert!(backend.row(state.run_key).await.unwrap().is_none());
    backend.seed_stale_row(&original).await.unwrap();
    let rejected = backend
        .repo()
        .delete_run_for_bundle(
            state.run_key,
            ShardId(0),
            DeleteRunRequest {
                expected_seq: state.transition_seq.next(),
                deleted_at: state.started_at,
            },
            ShardEpoch::ZERO,
        )
        .await
        .unwrap();
    prop_assert!(
        matches!(rejected, DeleteRunResult::Conflict { .. }),
        "stale deletion must be rejected"
    );
    prop_assert!(backend.row(state.run_key).await.unwrap().is_some());
    let deleted = backend
        .repo()
        .delete_run_for_bundle(
            state.run_key,
            ShardId(0),
            DeleteRunRequest {
                expected_seq: state.transition_seq,
                deleted_at: state.started_at,
            },
            ShardEpoch::ZERO,
        )
        .await
        .unwrap();
    prop_assert!(
        matches!(deleted, DeleteRunResult::Deleted { .. }),
        "closed run deletion must apply"
    );
    prop_assert!(backend.row(state.run_key).await.unwrap().is_none());
    Ok(())
}

pub(crate) async fn reset_materialization(backend: &impl Backend) {
    let mut transition = fresh_transition(RunKey::new());
    let history = reset_history(&transition.next_state);
    transition.next_state.last_event_id = history.last().unwrap().event_id;
    transition.event_principals = vec![None; history.len()].into();
    transition.history_events = history.into();
    let source = applied(commit(backend, transition).await.unwrap());
    for boundary in [3, 4, 7] {
        let run_id = RunId::new();
        let key = RunKey::derive(source.namespace_id, &source.workflow_id, run_id);
        let mut stale = source.clone();
        stale.run_key = key;
        backend.seed_stale_row(&stale).await.unwrap();
        let expected = backend
            .repo()
            .find_latest_run(source.namespace_id, &source.workflow_id)
            .await
            .unwrap();
        backend
            .repo()
            .materialize_reset_successor(source.run_key, boundary, run_id, expected)
            .await
            .unwrap();
        let LoadedRun::Existing(state) = backend.repo().load_run(key).await.unwrap() else {
            panic!("successor missing")
        };
        assert_eq!(backend.row(key).await.unwrap(), reference(&state));
        assert!(
            state.next_workflow_task_seq
                > state.pending_workflow_task.as_ref().unwrap().logical_seq
        );
        assert_eq!(
            backend.row(source.run_key).await.unwrap(),
            reference(&source)
        );
    }
}

pub(crate) async fn speculative_legacy_delivery(backend: &impl Backend) {
    let mut transition = fresh_transition(RunKey::new());
    transition
        .next_state
        .pending_workflow_task
        .as_mut()
        .unwrap()
        .task_type = WorkflowTaskType::Speculative;
    let state = applied(commit(backend, transition).await.unwrap());
    let queue = tokeira_types::QueueKey {
        namespace_id: state.namespace_id,
        task_queue: state.task_queue.clone(),
        task_kind: tokeira_types::TaskKind::Workflow,
        deployment: state.deployment.clone(),
        build_id: state.build_id.clone(),
    };
    let tasks = backend
        .repo()
        .list_dispatchable_workflow_tasks(&queue, 1)
        .await
        .unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].run_key, state.run_key);
    assert_eq!(
        tasks[0].logical_seq,
        state.pending_workflow_task.as_ref().unwrap().logical_seq
    );
    assert!(backend.row(state.run_key).await.unwrap().is_none());
}

pub(crate) async fn ordered_pages(backend: &impl Backend) {
    let initial = fresh_transition(RunKey::new());
    let range = WorkflowDiscoveryRange {
        namespace_id: initial.next_state.namespace_id,
        queue_name: initial.next_state.task_queue.clone(),
        routing: WorkflowDispatchRouting::Live,
    };
    let mut expected = Vec::new();
    for index in (0..19).rev() {
        let mut transition = fresh_transition(RunKey::new());
        transition.next_state.namespace_id = range.namespace_id;
        transition.next_state.workflow_id.0 = format!("page-{index}");
        transition.next_state.priority = Some(Priority {
            priority_key: index % 5 + 1,
            fairness_key: String::new(),
            fairness_weight: 1.0,
        });
        transition
            .next_state
            .pending_workflow_task
            .as_mut()
            .unwrap()
            .scheduled_at = OffsetDateTime::UNIX_EPOCH + Duration::seconds(i64::from(index % 3));
        let state = applied(commit(backend, transition).await.unwrap());
        expected.push(reference(&state).unwrap());
    }
    expected.sort_by_key(WorkflowDispatchRow::position);
    let mut actual = Vec::new();
    let mut after = None;
    loop {
        let page = backend
            .repo()
            .list_workflow_dispatch_page(&range, after, NonZeroU32::new(3).unwrap())
            .await
            .unwrap();
        assert!(page.candidates.iter().all(|row| range.matches(row)));
        assert_eq!(
            page.last_examined,
            page.candidates.last().map(WorkflowDispatchRow::position)
        );
        after = page.last_examined;
        actual.extend(page.candidates);
        if page.exhausted {
            break;
        }
    }
    assert_eq!(actual, expected);
    for row in expected {
        assert_eq!(
            backend.row(row.incarnation.run_key).await.unwrap(),
            Some(row)
        );
    }
}

pub(crate) async fn routing_and_home_pages(backend: &impl Backend) {
    let mut initial = fresh_transition(RunKey::new());
    initial.next_state.task_queue.0 = "q".repeat(1000);
    let mut expected = Vec::new();
    for (index, (deployment, build, sticky)) in [
        (None, None, false),
        (Some(""), None, false),
        (Some(""), Some(""), false),
        (Some("deployment"), Some("build"), false),
        (None, None, true),
    ]
    .into_iter()
    .enumerate()
    {
        let mut transition = initial.clone();
        transition.next_state.run_key = RunKey::new();
        transition.next_state.workflow_id.0 = format!("routing-{index}");
        transition.next_state.deployment = deployment.map(|value| DeploymentId(value.into()));
        transition.next_state.build_id = build.map(|value| BuildId(value.into()));
        if sticky {
            transition.next_state.sticky = Some(StickyAffinity {
                sticky_queue: TaskQueueName("sticky".into()),
                schedule_to_start_timeout: Duration::seconds(60),
                worker_identity: WorkerIdentity("worker".into()),
            });
            transition
                .next_state
                .pending_workflow_task
                .as_mut()
                .unwrap()
                .schedule_to_start_deadline = Some(OffsetDateTime::UNIX_EPOCH);
        }
        let state = applied(commit(backend, transition).await.unwrap());
        expected.push(reference(&state).unwrap());
    }
    for row in expected.iter().filter(|row| !row.sticky) {
        let range = WorkflowDiscoveryRange {
            namespace_id: row.namespace_id,
            queue_name: row.queue_name.clone(),
            routing: row.routing.clone(),
        };
        let page = backend
            .repo()
            .list_workflow_dispatch_page(&range, None, NonZeroU32::new(2).unwrap())
            .await
            .unwrap();
        assert_eq!(page.candidates, vec![row.clone()]);
        assert!(page.exhausted);
    }
    let mut keys = Vec::new();
    let mut after = None;
    loop {
        let page = backend
            .repo()
            .list_workflow_dispatch_for_home(ShardId(0), after, NonZeroU32::new(7).unwrap())
            .await
            .unwrap();
        assert!(page.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(
            page.first()
                .is_none_or(|first| after.is_none_or(|last| last < *first))
        );
        let done = page.len() < 7;
        after = page.last().copied();
        keys.extend(page);
        if done {
            break;
        }
    }
    for row in &expected {
        assert!(
            keys.contains(&row.incarnation.run_key),
            "home walk must include sticky intent"
        );
        assert_eq!(
            backend.row(row.incarnation.run_key).await.unwrap(),
            Some(row.clone())
        );
    }
    // Other fixtures may populate this home; only this fixture's run keys
    // must be excluded. Walk every page so a leaked key cannot hide later.
    let mut after = None;
    loop {
        let page = backend
            .repo()
            .list_workflow_dispatch_for_home(ShardId(1), after, NonZeroU32::new(64).unwrap())
            .await
            .unwrap();
        assert!(
            page.iter()
                .all(|key| { expected.iter().all(|row| row.incarnation.run_key != *key) })
        );
        if page.len() < 64 {
            break;
        }
        after = page.last().copied();
    }
}

/// Test-only corruption and observation around the shared repair algorithm.
#[async_trait]
pub(crate) trait RepairBackend: Backend {
    async fn remove_row(&self, key: RunKey) -> Result<()>;
    async fn legacy_recovery_flag(&self, key: RunKey) -> Result<()>;
    async fn authority(&self, keys: &[RunKey]) -> Result<Vec<u8>>;
    async fn finish_repair_case(&self, keys: &[RunKey]) -> Result<()>;
}

pub(crate) fn run_repair_cases(backend: &impl RepairBackend, runtime: &tokio::runtime::Runtime) {
    let strategy = (
        0usize..20,
        1usize..5,
        any::<bool>(),
        any::<bool>(),
        1u64..100,
    );
    let mut runner = TestRunner::new(ProptestConfig {
        cases: 100,
        failure_persistence: None,
        ..ProptestConfig::default()
    });
    runner
        .run(
            &strategy,
            |(interrupt, page_size, exact, legacy, generation)| {
                runtime.block_on(async {
                    // Feature: workflow-dispatch, Property 8: Complete bounded repair
                    // Partial walks may disagree; a complete head restart converges without changing authority.
                    let mut expected = Vec::new();
                    for kind in 0..7 {
                        let mut transition = fresh_transition(RunKey::new());
                        transition.next_state.workflow_id.0 =
                            format!("repair-{}", transition.next_state.run_key.0);
                        transition
                            .next_state
                            .pending_workflow_task
                            .as_mut()
                            .unwrap()
                            .logical_seq = LogicalTaskSeq(generation);
                        if exact {
                            transition.next_state.deployment =
                                Some(DeploymentId("repair-deployment".into()));
                            transition.next_state.build_id = Some(BuildId("repair-build".into()));
                        }
                        let stale = transition.next_state.clone();
                        match kind {
                            2 => {
                                // A stale incarnation and routing coordinate.
                                transition
                                    .next_state
                                    .pending_workflow_task
                                    .as_mut()
                                    .unwrap()
                                    .logical_seq = LogicalTaskSeq(generation + 1);
                                transition.next_state.task_queue =
                                    TaskQueueName("repaired-queue".into());
                            }
                            3 => {
                                transition.next_state.sticky = Some(StickyAffinity {
                                    sticky_queue: TaskQueueName("repair-sticky".into()),
                                    worker_identity: WorkerIdentity("repair-worker".into()),
                                    schedule_to_start_timeout: Duration::seconds(5),
                                });
                                transition
                                    .next_state
                                    .pending_workflow_task
                                    .as_mut()
                                    .unwrap()
                                    .schedule_to_start_deadline =
                                    Some(transition.next_state.started_at + Duration::seconds(5));
                            }
                            4 => {
                                transition.next_state.status = ExecutionStatus::Completed;
                                transition.next_state.closed_at =
                                    Some(transition.next_state.started_at);
                                transition.next_state.pending_workflow_task = None;
                            }
                            5 => {
                                transition
                                    .next_state
                                    .pending_workflow_task
                                    .as_mut()
                                    .unwrap()
                                    .task_type = WorkflowTaskType::Speculative
                            }
                            _ => {}
                        }
                        let key = transition.next_state.run_key;
                        if kind != 6 {
                            let state = applied(commit(backend, transition).await.unwrap());
                            if legacy {
                                backend.legacy_recovery_flag(key).await.unwrap();
                            }
                            expected.push((key, reference(&state)));
                        } else {
                            expected.push((key, None));
                        }
                        if kind == 0 {
                            backend.remove_row(key).await.unwrap();
                        } else {
                            backend.seed_stale_row(&stale).await.unwrap();
                        }
                    }
                    let keys: Vec<_> = expected.iter().map(|(key, _)| *key).collect();
                    let before = backend.authority(&keys).await.unwrap();
                    for budget in [Some(interrupt), None] {
                        let mut remaining = budget.unwrap_or(usize::MAX);
                        let mut cursor = None;
                        loop {
                            let page = backend
                                .repo()
                                .list_recovery_candidates_for_shard(
                                    ShardId(0),
                                    cursor.as_ref(),
                                    page_size,
                                )
                                .await
                                .unwrap();
                            for state in page.states {
                                if remaining == 0 {
                                    break;
                                }
                                repair_retry(backend, state.run_key).await.unwrap();
                                remaining -= 1;
                            }
                            if remaining == 0 || page.next.is_none() {
                                break;
                            }
                            cursor = page.next;
                        }
                        let mut cursor = None;
                        while remaining > 0 {
                            let page = backend
                                .repo()
                                .list_workflow_dispatch_for_home(
                                    ShardId(0),
                                    cursor,
                                    NonZeroU32::new(page_size as u32).unwrap(),
                                )
                                .await
                                .unwrap();
                            if page.is_empty() {
                                break;
                            }
                            cursor = page.last().copied();
                            for key in page {
                                if remaining == 0 {
                                    break;
                                }
                                repair_retry(backend, key).await.unwrap();
                                remaining -= 1;
                            }
                        }
                        prop_assert_eq!(backend.authority(&keys).await.unwrap(), before.clone());
                    }
                    for (key, row) in &expected {
                        prop_assert_eq!(backend.row(*key).await.unwrap(), row.clone());
                    }
                    // Repeating a repair is also a one-run transaction and remains read-only for authority.
                    for key in &keys {
                        repair_retry(backend, *key).await.unwrap();
                    }
                    prop_assert_eq!(backend.authority(&keys).await.unwrap(), before);
                    backend.finish_repair_case(&keys).await.unwrap();
                    Ok(())
                })
            },
        )
        .unwrap();
}

async fn repair_retry(backend: &impl Backend, key: RunKey) -> Result<()> {
    for attempt in 0..10 {
        match backend
            .repo()
            .reconcile_workflow_dispatch_run(ShardId(0), key)
            .await
        {
            Err(error) if error.is::<crate::WorkflowDispatchRepairConflict>() && attempt < 9 => {
                continue;
            }
            result => return result,
        }
    }
    unreachable!("last attempt returns")
}
