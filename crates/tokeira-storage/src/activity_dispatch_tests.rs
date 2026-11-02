//! Shared activity-dispatch fixtures and independent generated storage oracles.
//! Expected rows are enumerated from fixture coordinates, never from production
//! derivation, so a missing row cannot redefine the success criterion.

use std::{collections::BTreeMap, num::NonZeroU32};

use async_trait::async_trait;
use proptest::{prelude::*, test_runner::TestRunner};
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{ActivityPauseInfo, LoadedRun, Priority, WorkflowState};
use tokeira_types::{
    BuildId, DeploymentId, ExecutionStatus, QueueKey, RunKey, ShardId, TaskKind, TaskQueueName,
};

use crate::{
    ActivityDiscoveryRange, ActivityDispatchRow, ActivityIncarnation, CommitResult, InMemoryStore,
    RunRepository, WriteFence, derive_activity_dispatch,
    memory::projection_accumulator_tests::{activity_fixture, following, fresh_transition},
};

#[async_trait]
pub(crate) trait Backend: Sync {
    fn activity_repo(&self) -> &dyn RunRepository;
    fn activity_shard_count(&self) -> u32;
    async fn clear_activity_rows(&self, run: RunKey) -> anyhow::Result<()>;
    /// Bypass normal validation to represent corrupt or pre-upgrade physical rows.
    async fn seed_activity_rows(&self, rows: &[(ActivityDispatchRow, bool)]) -> anyhow::Result<()>;
    /// Set up retained copies independently of the disposal path being verified.
    async fn seed_activity_backlog(&self, entries: Vec<crate::BacklogEntry>) -> anyhow::Result<()> {
        self.activity_repo().persist_to_backlog(entries).await?;
        Ok(())
    }
}

// Feature: activity-dispatch, Property 17
// Each physical page passes deferred slots, deletes only covered copies, and preserves authority.
pub(crate) fn disposal_cases(backend: &impl Backend, runtime: &tokio::runtime::Runtime) {
    let mut runner = TestRunner::new(ProptestConfig {
        cases: 100,
        failure_persistence: None,
        ..Default::default()
    });
    runner
        .run(
            &(65usize..81, 0usize..7, any::<bool>()),
            |(count, shift, lose_cursor)| {
                runtime.block_on(async {
                    let repo = backend.activity_repo();
                    let mut initial = fresh_transition(RunKey::new());
                    let run = initial.next_state.run_key;
                    for index in 0..count {
                        if (index + shift) % 7 < 4 {
                            let id = format!("slot-{index:03}");
                            initial
                                .next_state
                                .activities
                                .insert(id.clone(), pending_activity(&id, index as i64 + 3));
                        }
                    }
                    let CommitResult::Applied { new_state: state } = repo
                        .commit_transition(run, initial, WriteFence::Unfenced)
                        .await
                        .unwrap()
                    else {
                        panic!("disposal fixture")
                    };
                    let home = tokeira_types::execution_home_bundle(
                        state.namespace_id.0.as_bytes(),
                        state.workflow_id.0.as_bytes(),
                        backend.activity_shard_count(),
                    );
                    let wanted = reference(&state, home);
                    backend.clear_activity_rows(run).await.unwrap();
                    let mut physical = Vec::new();
                    for (index, row) in (0..count).filter_map(|index| {
                        wanted
                            .values()
                            .find(|row| row.incarnation.activity_id == format!("slot-{index:03}"))
                            .map(|row| (index, row))
                    }) {
                        let mode = (index + shift) % 7;
                        if mode == 1 {
                            continue;
                        }
                        let mut row = row.clone();
                        if mode == 2 {
                            row.incarnation.stamp += 1;
                        }
                        physical.push((row, mode == 3));
                    }
                    backend.seed_activity_rows(&physical).await.unwrap();
                    let queue = QueueKey {
                        namespace_id: state.namespace_id,
                        task_queue: TaskQueueName("activity-contract".into()),
                        task_kind: TaskKind::Activity,
                        deployment: None,
                        build_id: None,
                    };
                    let mut entries = Vec::new();
                    let mut deferred = Vec::new();
                    let mut unknown = Vec::new();
                    for index in 0..count {
                        let mode = (index + shift) % 7;
                        let entry = crate::BacklogEntry {
                            run_key: if mode == 5 { RunKey::new() } else { run },
                            queue: queue.clone(),
                            payload: if mode == 6 {
                                crate::BacklogPayload::Workflow {
                                    logical_seq: tokeira_types::LogicalTaskSeq(index as u64 + 1),
                                }
                            } else {
                                crate::BacklogPayload::Activity {
                                    activity_id: format!("slot-{index:03}"),
                                    retired_input: tokeira_kernel::RetiredField::default(),
                                    schedule_event_id: index as i64 + 3,
                                    attempt: 1,
                                    dispatch_revision: 0,
                                    stamp: 0,
                                }
                            },
                            priority: Some(Priority {
                                priority_key: (index % 5 + 1) as i32,
                                fairness_key: format!("fair-{index}"),
                                fairness_weight: 1.0,
                            }),
                            scheduled_at: state.started_at,
                            order: crate::DeliveryOrder {
                                insertion_tie: index as u64,
                                ..Default::default()
                            },
                        };
                        if matches!(mode, 1..=3 | 6) {
                            deferred.push(entry.clone());
                        }
                        if mode == 6 {
                            unknown.push(entry.clone());
                        }
                        entries.push(entry);
                    }
                    backend.seed_activity_backlog(entries).await.unwrap();
                    let mut after = None;
                    let mut first = true;
                    loop {
                        let page = repo.dispose_workflow_backlog_page(after).await.unwrap();
                        prop_assert!(
                            page.examined <= 64 && page.deleted + page.deferred == page.examined
                        );
                        if page.exhausted {
                            break;
                        }
                        prop_assert!(page.last_examined > after);
                        after = if first && lose_cursor {
                            None
                        } else {
                            page.last_examined
                        };
                        first = false;
                    }
                    let actual = repo.drain_backlog(&queue, count + 1).await.unwrap();
                    let encode = |entries: &[crate::BacklogEntry]| {
                        entries
                            .iter()
                            .map(|entry| crate::codec::encode(entry).unwrap())
                            .collect::<std::collections::BTreeSet<_>>()
                    };
                    prop_assert_eq!(encode(&actual), encode(&deferred));
                    prop_assert_eq!(
                        repo.load_run(run).await.unwrap(),
                        LoadedRun::Existing(state.clone())
                    );
                    backend.seed_activity_backlog(actual).await.unwrap();
                    let mut continuation = None;
                    loop {
                        let repaired = repo
                            .reconcile_activity_dispatch_page(
                                home,
                                run,
                                continuation,
                                WriteFence::Unfenced,
                            )
                            .await
                            .unwrap();
                        if repaired.continuation.is_none() {
                            break;
                        }
                        continuation = repaired.continuation;
                    }
                    after = None;
                    loop {
                        let page = repo.dispose_workflow_backlog_page(after).await.unwrap();
                        prop_assert!(page.examined <= 64);
                        if page.exhausted {
                            break;
                        }
                        prop_assert!(page.last_examined > after);
                        after = page.last_examined;
                    }
                    let remaining = repo.drain_backlog(&queue, count + 1).await.unwrap();
                    prop_assert_eq!(encode(&remaining), encode(&unknown));
                    prop_assert_eq!(
                        repo.load_run(run).await.unwrap(),
                        LoadedRun::Existing(state)
                    );
                    Ok(())
                })
            },
        )
        .unwrap();
}

// Feature: activity-dispatch, Property 8
// Replay observation is read-only and rejects any changed lineage, attempt or receipt.
pub(crate) fn observation_cases(backend: &impl Backend, runtime: &tokio::runtime::Runtime) {
    let mut runner = TestRunner::new(ProptestConfig {
        cases: 100,
        failure_persistence: None,
        ..Default::default()
    });
    runner
        .run(
            &(1u32..100, 0u64..1000, 0u8..7),
            |(attempt, stamp, change)| {
                runtime.block_on(async {
                    let mut start = fresh_transition(RunKey::new());
                    let run = start.next_state.run_key;
                    let mut activity = pending_activity("observed", 3);
                    activity.attempt = attempt;
                    activity.stamp = stamp;
                    activity.started_at = Some(activity.scheduled_at);
                    activity.started_event_id = Some(-2);
                    activity.started_identity =
                        Some(tokeira_types::WorkerIdentity("worker".into()));
                    let receipt = tokeira_kernel::ActivityStartReceipt {
                        request_id: uuid::Uuid::new_v4(),
                        schedule_event_id: 3,
                        attempt,
                        offered_stamp: stamp,
                        worker_identity: tokeira_types::WorkerIdentity("worker".into()),
                        selected_queue: QueueKey {
                            namespace_id: start.next_state.namespace_id,
                            task_queue: activity.task_queue.clone(),
                            task_kind: TaskKind::Activity,
                            deployment: None,
                            build_id: None,
                        },
                        dispatch_revision: 0,
                    };
                    start
                        .next_state
                        .activities
                        .insert("observed".into(), activity);
                    start
                        .next_state
                        .activity_start_receipts
                        .insert("observed".into(), receipt);
                    let CommitResult::Applied {
                        new_state: observed,
                    } = backend
                        .activity_repo()
                        .commit_transition(run, start, WriteFence::Unfenced)
                        .await
                        .unwrap()
                    else {
                        panic!("seed observed start")
                    };
                    let home = tokeira_types::execution_home_bundle(
                        observed.namespace_id.0.as_bytes(),
                        observed.workflow_id.0.as_bytes(),
                        backend.activity_shard_count(),
                    );
                    prop_assert!(
                        backend
                            .activity_repo()
                            .validate_activity_start_observation(
                                &observed,
                                "observed",
                                home,
                                WriteFence::Unfenced
                            )
                            .await
                            .unwrap()
                    );
                    let mut changed = following(&observed);
                    match change {
                        0 => {
                            changed.next_state.activities.clear();
                            changed.next_state.activity_start_receipts.clear();
                        }
                        1 => {
                            changed
                                .next_state
                                .activity_start_receipts
                                .get_mut("observed")
                                .unwrap()
                                .request_id = uuid::Uuid::new_v4();
                        }
                        2 => {
                            changed
                                .next_state
                                .activities
                                .get_mut("observed")
                                .unwrap()
                                .stamp += 1;
                        }
                        3 => {
                            changed.next_state.status = ExecutionStatus::Completed;
                        }
                        4 => {
                            changed.next_state.status = ExecutionStatus::Paused;
                        }
                        5 => {
                            changed.next_state.signal_count += 1;
                        }
                        _ => {
                            changed.next_state.activity_start_receipts.clear();
                        }
                    }
                    let CommitResult::Applied { new_state: current } = backend
                        .activity_repo()
                        .commit_transition(run, changed, WriteFence::Unfenced)
                        .await
                        .unwrap()
                    else {
                        panic!("change observed start")
                    };
                    prop_assert!(
                        !backend
                            .activity_repo()
                            .validate_activity_start_observation(
                                &observed,
                                "observed",
                                home,
                                WriteFence::Unfenced
                            )
                            .await
                            .unwrap()
                    );
                    prop_assert_eq!(
                        backend.activity_repo().load_run(run).await.unwrap(),
                        LoadedRun::Existing(current)
                    );
                    Ok(())
                })
            },
        )
        .unwrap();
}

pub(crate) async fn repair_all(
    repo: &dyn RunRepository,
    home: ShardId,
    run: RunKey,
    fence: WriteFence,
) -> (usize, usize) {
    let mut cursor = None;
    let mut examined = 0;
    let mut changed = 0;
    for _ in 0..1000 {
        let page = repo
            .reconcile_activity_dispatch_page(home, run, cursor, fence.clone())
            .await
            .unwrap();
        assert!(page.examined <= 64 && page.changed <= page.examined);
        examined += page.examined;
        changed += page.changed;
        if page.continuation.is_none() {
            return (examined, changed);
        }
        assert_ne!(page.continuation, cursor, "repair cursor must progress");
        cursor = page.continuation;
    }
    panic!("finite fixture repair did not terminate")
}

// Feature: activity-dispatch, Property 13
// Two bounded phases reconstruct missing/future/legacy slots and remove obsolete/orphan slots without changing authority.
pub(crate) fn repair_cases(backend: &impl Backend, runtime: &tokio::runtime::Runtime) {
    let mut runner = TestRunner::new(ProptestConfig {
        cases: 100,
        failure_persistence: None,
        ..Default::default()
    });
    runner
        .run(
            &(prop::collection::vec(0u8..7, 80..150), 0u8..3),
            |(faults, status)| {
                runtime.block_on(async {
                    let mut start = fresh_transition(RunKey::new());
                    for index in 0..faults.len() {
                        let mut activity =
                            pending_activity(&format!("repair-{index}"), index as i64 + 3);
                        activity.current_attempt_scheduled_at =
                            Some(activity.scheduled_at + Duration::days(index as i64));
                        start
                            .next_state
                            .activities
                            .insert(activity.activity_id.clone(), activity);
                    }
                    let run = start.next_state.run_key;
                    let home = tokeira_types::execution_home_bundle(
                        start.next_state.namespace_id.0.as_bytes(),
                        start.next_state.workflow_id.0.as_bytes(),
                        backend.activity_shard_count().max(1),
                    );
                    let source_rows = reference(&start.next_state, home);
                    start.next_state.status = match status {
                        0 => ExecutionStatus::Running,
                        1 => ExecutionStatus::Paused,
                        _ => ExecutionStatus::Completed,
                    };
                    let CommitResult::Applied { new_state: state } = backend
                        .activity_repo()
                        .commit_transition(run, start, WriteFence::Unfenced)
                        .await
                        .unwrap()
                    else {
                        panic!("seed")
                    };
                    let expected = reference(&state, home);
                    backend.clear_activity_rows(run).await.unwrap();
                    let mut rows = Vec::new();
                    for (mut row, fault) in source_rows.into_values().zip(faults) {
                        match fault {
                            0 => continue,
                            2 => row.dispatch_at -= Duration::days(2),
                            3 => row.incarnation.attempt += 1,
                            4 => {
                                row.priority = Some(Priority {
                                    priority_key: 5,
                                    fairness_key: "wrong".into(),
                                    fairness_weight: 3.0,
                                })
                            }
                            5 => row.execution_home = ShardId(home.0 + 1),
                            6 => {
                                row.incarnation.activity_id =
                                    format!("obsolete-{}", row.incarnation.activity_id);
                                row.key =
                                    crate::activity_dispatch_key(run, &row.incarnation.activity_id);
                            }
                            _ => {}
                        }
                        rows.push((row, fault == 1));
                    }
                    let mut orphan = state.clone();
                    orphan.run_key = RunKey::new();
                    orphan.status = ExecutionStatus::Running;
                    orphan.activities.clear();
                    orphan
                        .activities
                        .insert("orphan".into(), pending_activity("orphan", 3));
                    rows.extend(
                        reference(&orphan, home)
                            .into_values()
                            .map(|row| (row, true)),
                    );
                    backend.seed_activity_rows(&rows).await.unwrap();
                    repair_all(backend.activity_repo(), home, run, WriteFence::Unfenced).await;
                    repair_all(
                        backend.activity_repo(),
                        home,
                        orphan.run_key,
                        WriteFence::Unfenced,
                    )
                    .await;
                    let actual = all_rows(backend.activity_repo(), &state, 13)
                        .await
                        .into_iter()
                        .map(|row| (row.key, row))
                        .collect::<BTreeMap<_, _>>();
                    prop_assert_eq!(actual, expected);
                    prop_assert_eq!(
                        backend.activity_repo().load_run(run).await.unwrap(),
                        LoadedRun::Existing(state.clone())
                    );
                    prop_assert!(
                        matches!(
                            backend
                                .activity_repo()
                                .load_run(orphan.run_key)
                                .await
                                .unwrap(),
                            LoadedRun::Absent
                        ),
                        "orphan is not resurrected"
                    );
                    let (_, changed) =
                        repair_all(backend.activity_repo(), home, run, WriteFence::Unfenced).await;
                    prop_assert_eq!(changed, 0);
                    Ok(())
                })
            },
        )
        .unwrap();
}

pub(crate) fn scheduled_activity_events(count: usize) -> Vec<tokeira_kernel::HistoryEventKind> {
    (0..count)
        .map(
            |index| tokeira_kernel::HistoryEventKind::ActivityTaskScheduled {
                workflow_task_completed_event_id: 4,
                activity_id: format!("staged-{index}"),
                activity_type: "activity".into(),
                task_queue: TaskQueueName("activity-contract".into()),
                input: Default::default(),
                header: None,
                retry_policy: None,
                schedule_to_close_timeout: Some(Duration::hours(1)),
                schedule_to_start_timeout: None,
                start_to_close_timeout: None,
                heartbeat_timeout: None,
                priority: None,
            },
        )
        .collect()
}

// Feature: activity-dispatch, Property 2
// Reset publication exposes a complete desired set, including a multi-page staged run.
pub(crate) fn staged_cases(
    backend: &(impl Backend + crate::bulk_write_tests::Backend),
    runtime: &tokio::runtime::Runtime,
) {
    let mut runner = TestRunner::new(ProptestConfig {
        cases: 100,
        failure_persistence: None,
        ..Default::default()
    });
    let case = std::sync::atomic::AtomicU32::new(0);
    runner
        .run(&(1usize..35), |count| {
            runtime.block_on(async {
                let count = if case.fetch_add(1, std::sync::atomic::Ordering::Relaxed) == 0 {
                    1005
                } else {
                    count
                };
                let events = scheduled_activity_events(count);
                let chunks = events.chunks(400).map(<[_]>::to_vec).collect();
                let (base, fork) = crate::bulk_write_tests::commit_base(backend, chunks).await;
                let run_id = tokeira_types::RunId::new();
                let run = RunKey::derive(base.namespace_id, &base.workflow_id, run_id);
                backend
                    .activity_repo()
                    .materialize_reset_successor(
                        base.run_key,
                        fork,
                        run_id,
                        Some(base.run_key),
                        WriteFence::Unfenced,
                    )
                    .await
                    .unwrap();
                let LoadedRun::Existing(state) =
                    backend.activity_repo().load_run(run).await.unwrap()
                else {
                    panic!("published state")
                };
                let rows = all_rows(backend.activity_repo(), &state, 17).await;
                prop_assert_eq!(rows.len(), count);
                let by_id = rows
                    .into_iter()
                    .map(|row| (row.incarnation.activity_id.clone(), row))
                    .collect::<BTreeMap<_, _>>();
                for index in 0..count {
                    let row = &by_id[&format!("staged-{index}")];
                    prop_assert_eq!(row.incarnation.run_key, run);
                    prop_assert_eq!(row.incarnation.schedule_event_id, index as i64 + 5);
                    prop_assert_eq!(row.incarnation.attempt, 1);
                    prop_assert_eq!(row.dispatch_at, base.started_at);
                }
                Ok(())
            })
        })
        .unwrap();
}

pub(crate) fn pending_activity(id: &str, schedule: i64) -> tokeira_kernel::ActivityState {
    let mut activity = activity_fixture();
    activity.activity_id = id.into();
    activity.schedule_event_id = schedule;
    activity.attempt = 1;
    activity.task_queue = TaskQueueName("activity-contract".into());
    activity.scheduled_at = OffsetDateTime::UNIX_EPOCH + Duration::seconds(1_700_000_000);
    activity.current_attempt_scheduled_at = Some(activity.scheduled_at);
    activity.started_at = None;
    activity.started_event_id = None;
    activity.pause_info = None;
    activity.stamp = 0;
    activity
}

fn reference(state: &WorkflowState, home: ShardId) -> BTreeMap<uuid::Uuid, ActivityDispatchRow> {
    let mut rows = BTreeMap::new();
    if state.status != ExecutionStatus::Running {
        return rows;
    }
    for (id, activity) in &state.activities {
        if activity.pause_info.is_some() || activity.started_event_id.is_some() {
            continue;
        }
        let priority = match (&state.priority, &activity.priority) {
            (None, None) => None,
            (base, override_) => Some(Priority {
                priority_key: override_
                    .as_ref()
                    .filter(|v| v.priority_key != 0)
                    .or(base.as_ref())
                    .map_or(0, |v| v.priority_key),
                fairness_key: override_
                    .as_ref()
                    .filter(|v| !v.fairness_key.is_empty())
                    .or(base.as_ref())
                    .map_or_else(String::new, |v| v.fairness_key.clone()),
                fairness_weight: override_
                    .as_ref()
                    .filter(|v| v.fairness_weight != 0.0)
                    .or(base.as_ref())
                    .map_or(0.0, |v| v.fairness_weight),
            }),
        };
        let key = tokeira_types::dsql_spread_uuid(&[
            b"activity-dispatch",
            state.run_key.0.as_bytes(),
            id.as_bytes(),
        ]);
        // Timestamp fixtures are integral microseconds; no production timestamp
        // normalizer is used in this row oracle.
        let queue_key = reference_queue_digest(&activity.task_queue.0);
        rows.insert(
            key,
            ActivityDispatchRow {
                key,
                incarnation: ActivityIncarnation {
                    run_key: state.run_key,
                    activity_id: id.clone(),
                    schedule_event_id: activity.schedule_event_id,
                    attempt: activity.attempt,
                    stamp: activity.stamp,
                },
                execution_home: home,
                queue: QueueKey {
                    namespace_id: state.namespace_id,
                    task_queue: activity.task_queue.clone(),
                    task_kind: TaskKind::Activity,
                    deployment: activity
                        .deployment
                        .as_ref()
                        .or(state.deployment.as_ref())
                        .cloned(),
                    build_id: activity
                        .build_id
                        .as_ref()
                        .or(state.build_id.as_ref())
                        .cloned(),
                },
                queue_key,
                priority,
                dispatch_revision: state
                    .versioning_info
                    .as_ref()
                    .map_or(0, |v| v.revision_number),
                dispatch_at: activity.current_attempt_scheduled_at.unwrap(),
            },
        );
    }
    rows
}

fn reference_queue_digest(queue: &str) -> String {
    let mut bytes = b"tokeira-workflow-dispatch-v1\0".to_vec();
    bytes.extend_from_slice(&5u64.to_be_bytes());
    bytes.extend_from_slice(b"queue\x01");
    bytes.extend_from_slice(&(queue.len() as u64).to_be_bytes());
    bytes.extend_from_slice(queue.as_bytes());
    crate::schema_contract::sha256_hex(&bytes)
}

type DerivationEntry = (u8, u32, u64, i64, bool);

fn populate_derivation(
    state: &mut WorkflowState,
    entries: Vec<DerivationEntry>,
    running: bool,
    base_priority: i32,
) {
    state.status = if running {
        ExecutionStatus::Running
    } else {
        ExecutionStatus::Paused
    };
    state.deployment = Some(DeploymentId("workflow-deployment".into()));
    state.build_id = Some(BuildId("workflow-build".into()));
    state.priority = Some(Priority {
        priority_key: base_priority,
        fairness_key: "base".into(),
        fairness_weight: 2.0,
    });
    for (i, (mode, attempt, stamp, delay, override_)) in entries.into_iter().enumerate() {
        let mut activity = pending_activity(&format!("a-{i}"), i as i64 + 1);
        activity.attempt = attempt;
        activity.stamp = stamp;
        activity.current_attempt_scheduled_at =
            Some(activity.scheduled_at + Duration::seconds(delay));
        if mode == 1 {
            activity.started_at = Some(activity.scheduled_at);
            activity.started_event_id = Some(-1);
        } else if mode == 2 {
            activity.pause_info = Some(ActivityPauseInfo {
                pause_time: activity.scheduled_at,
                identity: String::new(),
                reason: String::new(),
                rule_id: None,
            });
        } else if mode == 3 {
            activity.cancel_requested = true;
        }
        if override_ {
            activity.deployment = Some(DeploymentId("activity-deployment".into()));
            activity.build_id = Some(BuildId(String::new()));
            activity.priority = Some(Priority {
                priority_key: 1,
                fairness_key: String::new(),
                fairness_weight: 0.0,
            });
        }
        state
            .activities
            .insert(activity.activity_id.clone(), activity);
    }
}

// Feature: activity-dispatch, Property 1
// Both repositories persist every derived field against the same independent oracle.
pub(crate) fn derivation_cases(backend: &impl Backend, runtime: &tokio::runtime::Runtime) {
    let mut runner = TestRunner::new(ProptestConfig {
        cases: 100,
        failure_persistence: None,
        ..Default::default()
    });
    runner
        .run(
            &(
                prop::collection::vec(
                    (0u8..4, 1u32..20, 0u64..10000, -100i64..100, any::<bool>()),
                    0..140,
                ),
                any::<bool>(),
                0i32..6,
            ),
            |(entries, running, priority)| {
                runtime.block_on(async {
                    let mut start = fresh_transition(RunKey::new());
                    populate_derivation(&mut start.next_state, entries, running, priority);
                    let run = start.next_state.run_key;
                    let home = tokeira_types::execution_home_bundle(
                        start.next_state.namespace_id.0.as_bytes(),
                        start.next_state.workflow_id.0.as_bytes(),
                        backend.activity_shard_count(),
                    );
                    let expected = reference(&start.next_state, home);
                    let CommitResult::Applied { new_state: state } = backend
                        .activity_repo()
                        .commit_transition(run, start, WriteFence::Unfenced)
                        .await
                        .unwrap()
                    else {
                        panic!("derivation fixture commit");
                    };
                    let actual = all_rows(backend.activity_repo(), &state, 17)
                        .await
                        .into_iter()
                        .map(|row| (row.key, row))
                        .collect::<BTreeMap<_, _>>();
                    prop_assert_eq!(actual, expected);
                    prop_assert_eq!(
                        backend.activity_repo().load_run(run).await.unwrap(),
                        LoadedRun::Existing(state)
                    );
                    Ok(())
                })
            },
        )
        .unwrap();
}

#[test]
fn memory_complete_activity_derivation() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    derivation_cases(&InMemoryStore::default(), &runtime);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: activity-dispatch, Property 1
    // Every eligible fixture contributes exactly one complete row; delivery time is irrelevant.
    #[test]
    fn complete_deterministic_activity_derivation(
        entries in prop::collection::vec((0u8..4, 1u32..20, 0u64..10000, -100i64..100, any::<bool>()), 0..140),
        running in any::<bool>(), home in 0u32..4096, base_priority in 0i32..6,
    ) {
        let mut state = fresh_transition(RunKey::new()).next_state;
        populate_derivation(&mut state, entries, running, base_priority);
        let expected = reference(&state, ShardId(home));
        prop_assert_eq!(derive_activity_dispatch(&state, ShardId(home)).unwrap(), expected.clone());
        let reversed: Vec<_> = state.activities.clone().into_iter().rev().collect();
        state.activities.clear();
        state.activities.extend(reversed);
        prop_assert_eq!(derive_activity_dispatch(&state, ShardId(home)).unwrap(), expected);
    }
}

#[test]
fn invalid_activity_authority_is_an_error() {
    for invalid in 0..6 {
        let mut state = fresh_transition(RunKey::new()).next_state;
        let mut activity = pending_activity("a", 1);
        match invalid {
            0 => activity.attempt = 0,
            1 => activity.stamp = u64::MAX,
            2 => activity.current_attempt_scheduled_at = None,
            3 => activity.started_event_id = Some(-1),
            4 => activity.schedule_event_id = 0,
            _ => activity.activity_id = "wrong-map-key".into(),
        }
        state.activities.insert("a".into(), activity);
        assert!(
            derive_activity_dispatch(&state, ShardId(0)).is_err(),
            "invalid case {invalid}"
        );
    }
}

pub(crate) async fn all_rows(
    repo: &dyn RunRepository,
    state: &WorkflowState,
    page_size: u32,
) -> Vec<ActivityDispatchRow> {
    let range = ActivityDiscoveryRange {
        namespace_id: state.namespace_id,
        queue_name: TaskQueueName("activity-contract".into()),
    };
    let mut after = None;
    let mut rows = Vec::new();
    loop {
        let page = repo
            .list_activity_dispatch_page(
                &range,
                OffsetDateTime::UNIX_EPOCH + Duration::seconds(2_000_000_000),
                after,
                NonZeroU32::new(page_size).unwrap(),
            )
            .await
            .unwrap();
        assert!(page.candidates.len() <= page_size.min(64) as usize);
        for candidate in page.candidates {
            assert!(after.is_none_or(|p| candidate.position > p));
            after = Some(candidate.position);
            rows.push(candidate.row.unwrap());
        }
        assert_eq!(page.last_examined.or(after), after);
        if page.exhausted {
            break;
        }
    }
    rows
}

// Feature: activity-dispatch, Property 2
// A refused or conflicting transition leaves both authority and every derived row unchanged.
pub(crate) fn atomic_cases(repo: &dyn RunRepository, runtime: &tokio::runtime::Runtime) {
    let mut runner = TestRunner::new(ProptestConfig {
        cases: 100,
        failure_persistence: None,
        ..ProptestConfig::default()
    });
    runner
        .run(
            &(1usize..75, 0i32..6, 1u32..65),
            |(count, priority, page_size)| {
                runtime.block_on(async {
                    let mut transition = fresh_transition(RunKey::new());
                    for index in 0..count {
                        let activity = pending_activity(&format!("a-{index}"), index as i64 + 3);
                        transition
                            .next_state
                            .activities
                            .insert(activity.activity_id.clone(), activity);
                    }
                    let run_key = transition.next_state.run_key;
                    let CommitResult::Applied { new_state: state } = repo
                        .commit_transition(run_key, transition.clone(), WriteFence::Unfenced)
                        .await
                        .unwrap()
                    else {
                        panic!("initial commit")
                    };
                    let before = all_rows(repo, &state, page_size).await;
                    prop_assert_eq!(before.len(), count);
                    prop_assert!(
                        matches!(
                            repo.commit_transition(run_key, transition, WriteFence::Unfenced)
                                .await
                                .unwrap(),
                            CommitResult::Conflict { .. }
                        ),
                        "stale transition must conflict"
                    );
                    prop_assert_eq!(all_rows(repo, &state, page_size).await, before.clone());
                    let mut invalid = following(&state);
                    invalid
                        .next_state
                        .activities
                        .values_mut()
                        .next()
                        .unwrap()
                        .stamp = u64::MAX;
                    prop_assert!(
                        repo.commit_transition(run_key, invalid, WriteFence::Unfenced)
                            .await
                            .is_err()
                    );
                    prop_assert_eq!(
                        repo.load_run(run_key).await.unwrap(),
                        LoadedRun::Existing(state.clone())
                    );
                    prop_assert_eq!(all_rows(repo, &state, page_size).await, before);
                    let mut changed = following(&state);
                    changed.next_state.priority = Some(Priority {
                        priority_key: priority,
                        fairness_key: "changed".into(),
                        fairness_weight: 3.0,
                    });
                    changed.next_state.activities.remove("a-0");
                    let CommitResult::Applied { new_state: next } = repo
                        .commit_transition(run_key, changed, WriteFence::Unfenced)
                        .await
                        .unwrap()
                    else {
                        panic!("update commit")
                    };
                    let actual = all_rows(repo, &next, page_size).await;
                    prop_assert_eq!(actual.len(), count - 1);
                    for row in actual {
                        prop_assert_ne!(&row.incarnation.activity_id, "a-0");
                        prop_assert_eq!(row.priority, next.priority.clone());
                    }
                    Ok(())
                })
            },
        )
        .unwrap();
}

#[test]
fn memory_atomic_activity_rows() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    atomic_cases(&InMemoryStore::default(), &runtime);
}

// Feature: activity-dispatch, Property 3
// Keyset traversal follows fixture timestamp/UUID order even when its previous row disappears.
pub(crate) fn ordered_cases(repo: &dyn RunRepository, runtime: &tokio::runtime::Runtime) {
    let mut runner = TestRunner::new(ProptestConfig {
        cases: 100,
        failure_persistence: None,
        ..ProptestConfig::default()
    });
    runner
        .run(
            &(prop::collection::vec(-3i64..4, 65..140), 1u32..80),
            |(times, page_size)| {
                runtime.block_on(async {
                    let mut transition = fresh_transition(RunKey::new());
                    let cutoff = OffsetDateTime::UNIX_EPOCH + Duration::seconds(1_700_000_000);
                    let mut expected = BTreeMap::new();
                    for (index, delta) in times.iter().copied().enumerate() {
                        let mut activity =
                            pending_activity(&format!("ordered-{index}"), index as i64 + 5);
                        activity.current_attempt_scheduled_at =
                            Some(cutoff + Duration::microseconds(delta));
                        if delta <= 0 {
                            let key = tokeira_types::dsql_spread_uuid(&[
                                b"activity-dispatch",
                                transition.next_state.run_key.0.as_bytes(),
                                activity.activity_id.as_bytes(),
                            ]);
                            expected.insert(
                                crate::ActivityDispatchPosition {
                                    dispatch_at: cutoff + Duration::microseconds(delta),
                                    key,
                                },
                                activity.activity_id.clone(),
                            );
                        }
                        transition
                            .next_state
                            .activities
                            .insert(activity.activity_id.clone(), activity);
                    }
                    let run_key = transition.next_state.run_key;
                    let CommitResult::Applied {
                        new_state: mut state,
                    } = repo
                        .commit_transition(run_key, transition, WriteFence::Unfenced)
                        .await
                        .unwrap()
                    else {
                        panic!("fixture commit")
                    };
                    let range = ActivityDiscoveryRange {
                        namespace_id: state.namespace_id,
                        queue_name: TaskQueueName("activity-contract".into()),
                    };
                    let mut after = None;
                    let mut seen = BTreeMap::new();
                    let mut deleted = false;
                    loop {
                        let page = repo
                            .list_activity_dispatch_page(
                                &range,
                                cutoff,
                                after,
                                NonZeroU32::new(page_size).unwrap(),
                            )
                            .await
                            .unwrap();
                        prop_assert!(page.candidates.len() <= page_size.min(64) as usize);
                        let count = page.candidates.len();
                        for candidate in page.candidates {
                            prop_assert!(after.is_none_or(|p| candidate.position > p));
                            let row = candidate.row.unwrap();
                            prop_assert!(row.dispatch_at <= cutoff);
                            prop_assert_eq!(candidate.position, row.position());
                            prop_assert!(range.matches(&row));
                            after = Some(candidate.position);
                            seen.insert(candidate.position, row.incarnation.activity_id);
                        }
                        prop_assert_eq!(page.exhausted, count < page_size.min(64) as usize);
                        if !deleted && let Some(last) = after {
                            let mut removal = following(&state);
                            removal.next_state.activities.remove(&seen[&last]);
                            let CommitResult::Applied { new_state } = repo
                                .commit_transition(run_key, removal, WriteFence::Unfenced)
                                .await
                                .unwrap()
                            else {
                                panic!("cursor deletion")
                            };
                            state = new_state;
                            deleted = true;
                        }
                        if page.exhausted {
                            break;
                        }
                    }
                    prop_assert_eq!(seen, expected);
                    Ok(())
                })
            },
        )
        .unwrap();
}

#[test]
fn memory_ordered_activity_pages() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    ordered_cases(&InMemoryStore::default(), &runtime);
}

// Feature: activity-dispatch, Property 2
// A successor's invalid desired set cannot publish the predecessor's closure.
pub(crate) fn pair_cases(repo: &dyn RunRepository, runtime: &tokio::runtime::Runtime) {
    let mut runner = TestRunner::new(ProptestConfig {
        cases: 100,
        failure_persistence: None,
        ..Default::default()
    });
    runner
        .run(&(1usize..16, 0u8..4), |(count, kind)| {
            runtime.block_on(async {
                let mut start = crate::atomic_successors_test_support::start();
                for index in 0..count {
                    let activity = pending_activity(&format!("prior-{index}"), index as i64 + 3);
                    start
                        .next_state
                        .activities
                        .insert(activity.activity_id.clone(), activity);
                }
                let run = start.next_state.run_key;
                let CommitResult::Applied { new_state: state } = repo
                    .commit_transition(run, start, WriteFence::Unfenced)
                    .await
                    .unwrap()
                else {
                    panic!("seed")
                };
                let (closing, mut successor) = crate::atomic_successors_test_support::pair(
                    &state,
                    kind,
                    Default::default(),
                    Default::default(),
                    None,
                );
                for index in 0..count {
                    let activity = pending_activity(&format!("next-{index}"), index as i64 + 3);
                    successor
                        .next_state
                        .activities
                        .insert(activity.activity_id.clone(), activity);
                }
                let successor_run = successor.next_state.run_key;
                let before = all_rows(repo, &state, 5).await;
                let mut invalid = successor.clone();
                invalid
                    .next_state
                    .activities
                    .get_mut("next-0")
                    .unwrap()
                    .attempt = 0;
                prop_assert!(
                    repo.commit_transition_pair(
                        run,
                        closing.clone(),
                        successor_run,
                        invalid,
                        WriteFence::Unfenced
                    )
                    .await
                    .is_err()
                );
                prop_assert_eq!(
                    repo.load_run(run).await.unwrap(),
                    LoadedRun::Existing(state.clone())
                );
                prop_assert!(
                    matches!(
                        repo.load_run(successor_run).await.unwrap(),
                        LoadedRun::Absent
                    ),
                    "successor remains absent"
                );
                prop_assert_eq!(all_rows(repo, &state, 5).await, before);
                let result = repo
                    .commit_transition_pair(
                        run,
                        closing,
                        successor_run,
                        successor,
                        WriteFence::Unfenced,
                    )
                    .await
                    .unwrap();
                prop_assert!(
                    matches!(result, crate::PairCommitResult::Applied { .. }),
                    "pair must apply"
                );
                let rows = all_rows(repo, &state, 5).await;
                prop_assert_eq!(rows.len(), count);
                for row in rows {
                    prop_assert_eq!(row.incarnation.run_key, successor_run);
                    prop_assert!(row.incarnation.activity_id.starts_with("next-"));
                }
                Ok(())
            })
        })
        .unwrap();
}

// Feature: activity-dispatch, Property 3
// Malformed payloads and digest collisions consume their physical positions; home pages include legacy orphans.
pub(crate) fn malformed_page_cases(backend: &impl Backend, runtime: &tokio::runtime::Runtime) {
    let mut runner = TestRunner::new(ProptestConfig {
        cases: 100,
        failure_persistence: None,
        ..Default::default()
    });
    let case = std::sync::atomic::AtomicU32::new(0);
    runner
        .run(
            &(prop::collection::vec(0u8..5, 65..140), 1u32..85),
            |(faults, size)| {
                let case = case.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                runtime.block_on(async {
                    let home = ShardId(500_000 + case * 2);
                    let mut state = fresh_transition(RunKey::new()).next_state;
                    let cutoff = OffsetDateTime::UNIX_EPOCH + Duration::seconds(1_700_000_000);
                    for index in 0..faults.len() {
                        let activity =
                            pending_activity(&format!("raw-{index:03}"), index as i64 + 3);
                        state
                            .activities
                            .insert(activity.activity_id.clone(), activity);
                    }
                    let canonical = reference(&state, home);
                    let mut due = BTreeMap::new();
                    let mut homes = Vec::new();
                    let mut seeded = Vec::new();
                    for (index, (mut row, fault)) in canonical.into_values().zip(faults).enumerate()
                    {
                        if index % 3 == 0 {
                            row.execution_home = ShardId(home.0 + 1);
                        }
                        let legacy = fault == 1;
                        match fault {
                            2 => row.queue.task_queue.0 = "digest-collision".into(),
                            3 => row.incarnation.attempt = 0,
                            4 => row.incarnation.stamp = u64::MAX,
                            _ => {}
                        }
                        if !legacy {
                            due.insert(row.position(), fault == 0);
                        }
                        if row.execution_home == home {
                            homes.push(crate::ActivityHomePosition {
                                run_key: row.incarnation.run_key,
                                key: row.key,
                            });
                        }
                        seeded.push((row, legacy));
                    }
                    homes.sort();
                    backend.seed_activity_rows(&seeded).await.unwrap();
                    let range = ActivityDiscoveryRange {
                        namespace_id: state.namespace_id,
                        queue_name: TaskQueueName("activity-contract".into()),
                    };
                    let mut after = None;
                    let mut seen = BTreeMap::new();
                    loop {
                        let page = backend
                            .activity_repo()
                            .list_activity_dispatch_page(
                                &range,
                                cutoff,
                                after,
                                NonZeroU32::new(size).unwrap(),
                            )
                            .await
                            .unwrap();
                        prop_assert!(page.candidates.len() <= size.min(64) as usize);
                        for candidate in page.candidates {
                            prop_assert!(after.is_none_or(|key| candidate.position > key));
                            after = Some(candidate.position);
                            seen.insert(candidate.position, candidate.row.is_ok());
                        }
                        prop_assert_eq!(page.last_examined.or(after), after);
                        if page.exhausted {
                            break;
                        }
                    }
                    prop_assert_eq!(seen, due);
                    let mut after = None;
                    let mut seen = Vec::new();
                    loop {
                        let page = backend
                            .activity_repo()
                            .list_activity_dispatch_home_page(
                                home,
                                after,
                                NonZeroU32::new(size).unwrap(),
                            )
                            .await
                            .unwrap();
                        prop_assert!(page.candidates.len() <= size.min(64) as usize);
                        for position in page.candidates {
                            prop_assert!(after.is_none_or(|key| position > key));
                            after = Some(position);
                            seen.push(position);
                        }
                        prop_assert_eq!(page.last_examined.or(after), after);
                        if page.exhausted {
                            break;
                        }
                    }
                    prop_assert_eq!(seen, homes);
                    Ok(())
                })
            },
        )
        .unwrap();
}
