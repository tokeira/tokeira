//! Controlled physical transactions make late repair, replay and legacy deletes
//! compete with committed replacement state rather than simulated OCC results.

use std::sync::Arc;

use anyhow::Result;
use proptest::{prelude::*, test_runner::TestRunner};
use tokeira_kernel::{ActivityStartReceipt, LoadedRun};
use tokeira_types::{QueueKey, RunKey, ShardId, TaskKind, WorkerIdentity};
use uuid::Uuid;

use super::{Backend, fixture};
use crate::{
    BacklogEntry, BacklogPayload, CommitResult, DeliveryOrder, WriteFence,
    activity_dispatch_tests::pending_activity,
    dsql::{
        DsqlRunRepository,
        run_repository::pause_point::{self, PauseKey},
    },
    memory::projection_accumulator_tests::{following, fresh_transition},
};

// Feature: activity-dispatch, Property 14
// A transaction begun before a competing commit cannot publish old derived state.
#[test]
fn activity_dispatch_live_competing_repair_and_replay() -> Result<()> {
    let (runtime, fixture) = fixture()?;
    let fixture = Arc::new(fixture);
    let mut runner = TestRunner::new(ProptestConfig {
        cases: 100,
        failure_persistence: None,
        ..Default::default()
    });
    runner
        .run(&(any::<bool>(), 1u64..1000), |(remove, stamp)| {
            runtime.block_on(async {
                let mut initial = fresh_transition(RunKey::new());
                let run = initial.next_state.run_key;
                let mut activity = pending_activity("race", 3);
                activity.stamp = stamp;
                initial
                    .next_state
                    .activities
                    .insert("race".into(), activity.clone());
                let CommitResult::Applied { new_state: state } = fixture
                    .activity_repo()
                    .commit_transition(run, initial, WriteFence::Unfenced)
                    .await
                    .unwrap()
                else {
                    panic!("seed")
                };
                fixture.clear_activity_rows(run).await.unwrap();
                let (entered, resume) = pause_point::arm(PauseKey::ActivityRepair(run));
                let repairing = fixture.clone();
                let repair = tokio::spawn(async move {
                    repairing
                        .activity_repo()
                        .reconcile_activity_dispatch_page(
                            ShardId(0),
                            run,
                            None,
                            WriteFence::Unfenced,
                        )
                        .await
                });
                entered.await.unwrap();
                let mut changed = following(&state);
                if remove {
                    changed.next_state.activities.clear();
                } else {
                    changed.next_state.activities.get_mut("race").unwrap().stamp += 1;
                }
                let CommitResult::Applied {
                    new_state: replaced,
                } = fixture
                    .activity_repo()
                    .commit_transition(run, changed, WriteFence::Unfenced)
                    .await
                    .unwrap()
                else {
                    panic!("replacement")
                };
                resume.send(()).unwrap();
                prop_assert!(
                    repair.await.unwrap().is_err(),
                    "old repair must fail physical OCC"
                );
                let mut cursor = None;
                loop {
                    let page = fixture
                        .activity_repo()
                        .reconcile_activity_dispatch_page(
                            ShardId(0),
                            run,
                            cursor,
                            WriteFence::Unfenced,
                        )
                        .await
                        .unwrap();
                    cursor = page.continuation;
                    if cursor.is_none() {
                        break;
                    }
                }
                prop_assert_eq!(
                    fixture.activity_repo().load_run(run).await.unwrap(),
                    LoadedRun::Existing(replaced.clone())
                );
                let range = crate::ActivityDiscoveryRange {
                    namespace_id: state.namespace_id,
                    queue_name: activity.task_queue.clone(),
                };
                let page = fixture
                    .activity_repo()
                    .list_activity_dispatch_page(
                        &range,
                        state.started_at + time::Duration::days(1),
                        None,
                        std::num::NonZeroU32::new(64).unwrap(),
                    )
                    .await
                    .unwrap();
                let rows = page
                    .candidates
                    .into_iter()
                    .map(|c| c.row.unwrap())
                    .collect::<Vec<_>>();
                prop_assert_eq!(rows.len(), usize::from(!remove));
                if !remove {
                    prop_assert_eq!(rows[0].incarnation.stamp, stamp + 1);
                }

                let mut started = following(&replaced);
                activity.started_at = Some(activity.scheduled_at);
                activity.started_event_id = Some(-2);
                activity.started_identity = Some(WorkerIdentity("worker".into()));
                started
                    .next_state
                    .activities
                    .insert("race".into(), activity.clone());
                started.next_state.activity_start_receipts.insert(
                    "race".into(),
                    ActivityStartReceipt {
                        request_id: Uuid::new_v4(),
                        schedule_event_id: 3,
                        attempt: 1,
                        offered_stamp: stamp,
                        worker_identity: WorkerIdentity("worker".into()),
                        dispatch_revision: 0,
                        selected_queue: QueueKey {
                            namespace_id: state.namespace_id,
                            task_queue: activity.task_queue,
                            task_kind: TaskKind::Activity,
                            deployment: None,
                            build_id: None,
                        },
                    },
                );
                let CommitResult::Applied {
                    new_state: observed,
                } = fixture
                    .activity_repo()
                    .commit_transition(run, started, WriteFence::Unfenced)
                    .await
                    .unwrap()
                else {
                    panic!("start")
                };
                let (entered, resume) = pause_point::arm(PauseKey::ActivityObservation(run));
                let reading = fixture.clone();
                let replay_state = observed.clone();
                let replay = tokio::spawn(async move {
                    reading
                        .store
                        .run_repository()
                        .do_validate_activity_start_observation(
                            &replay_state,
                            "race",
                            ShardId(0),
                            &WriteFence::Unfenced,
                        )
                        .await
                });
                entered.await.unwrap();
                let mut resolved = following(&observed);
                resolved.next_state.activities.clear();
                resolved.next_state.activity_start_receipts.clear();
                assert!(matches!(
                    fixture
                        .activity_repo()
                        .commit_transition(run, resolved, WriteFence::Unfenced)
                        .await
                        .unwrap(),
                    CommitResult::Applied { .. }
                ));
                resume.send(()).unwrap();
                // Inspect the physical conflict before the public adapter maps
                // OCC to Ok(false), which tells the owner to reread authority.
                let rejected = replay.await.unwrap().unwrap_err();
                prop_assert!(
                    rejected
                        .downcast_ref::<sqlx::Error>()
                        .is_some_and(DsqlRunRepository::is_serialization_failure),
                    "resolved start must invalidate earlier read snapshot: {rejected:?}"
                );
                prop_assert!(
                    !fixture
                        .activity_repo()
                        .validate_activity_start_observation(
                            &observed,
                            "race",
                            ShardId(0),
                            WriteFence::Unfenced
                        )
                        .await
                        .unwrap()
                );
                Ok(())
            })
        })
        .unwrap();
    runtime.block_on(fixture.store.shutdown())
}

// Feature: activity-dispatch, Property 17
// Disposal must not delete a replacement inserted at the same physical key.
#[test]
fn activity_dispatch_live_disposal_replacement() -> Result<()> {
    let (runtime, fixture) = fixture()?;
    let fixture = Arc::new(fixture);
    let mut runner = TestRunner::new(ProptestConfig {
        cases: 100,
        failure_persistence: None,
        ..Default::default()
    });
    runner
        .run(&(1u32..100, 1u64..1000), |(attempt, stamp)| {
            runtime.block_on(async {
                let state = fresh_transition(RunKey::new()).next_state;
                let queue = QueueKey {
                    namespace_id: state.namespace_id,
                    task_queue: state.task_queue,
                    task_kind: TaskKind::Activity,
                    deployment: None,
                    build_id: None,
                };
                let entry = BacklogEntry {
                    run_key: state.run_key,
                    queue: queue.clone(),
                    payload: BacklogPayload::Activity {
                        activity_id: "gone".into(),
                        retired_input: Default::default(),
                        schedule_event_id: 3,
                        attempt,
                        stamp,
                        dispatch_revision: 0,
                    },
                    priority: None,
                    scheduled_at: state.started_at,
                    order: DeliveryOrder::default(),
                };
                fixture
                    .activity_repo()
                    .persist_to_backlog(vec![entry])
                    .await
                    .unwrap();
                let key: Uuid =
                    sqlx::query_scalar("SELECT key FROM dispatch_backlog WHERE run_key=$1")
                        .bind(state.run_key.0)
                        .fetch_one(&fixture.pool)
                        .await
                        .unwrap();
                let after = key.as_u128().checked_sub(1).map(Uuid::from_u128);
                let (entered, resume) = pause_point::arm(PauseKey::Disposal);
                let deleting = fixture.clone();
                let deletion = tokio::spawn(async move {
                    deleting
                        .activity_repo()
                        .dispose_workflow_backlog_page(after)
                        .await
                });
                entered.await.unwrap();
                let replacement = vec![0xf0, (attempt % 256) as u8, (stamp % 256) as u8];
                sqlx::query("UPDATE dispatch_backlog SET payload_data=$2 WHERE key=$1")
                    .bind(key)
                    .bind(&replacement)
                    .execute(&fixture.pool)
                    .await
                    .unwrap();
                resume.send(()).unwrap();
                prop_assert!(
                    deletion.await.unwrap().is_err(),
                    "replacement must conflict with old conditional delete"
                );
                let actual: Vec<u8> =
                    sqlx::query_scalar("SELECT payload_data FROM dispatch_backlog WHERE key=$1")
                        .bind(key)
                        .fetch_one(&fixture.pool)
                        .await
                        .unwrap();
                prop_assert_eq!(actual, replacement);
                // The raw corrupt replacement is this fixture's own row; removing
                // it prevents unrelated subsequent disposal pages from growing.
                sqlx::query("DELETE FROM dispatch_backlog WHERE key=$1")
                    .bind(key)
                    .execute(&fixture.pool)
                    .await
                    .unwrap();
                Ok(())
            })
        })
        .unwrap();
    runtime.block_on(fixture.store.shutdown())
}
