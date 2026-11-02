//! Isolate the lease-key conflict from run/dispatch conflicts for activity
//! repair and receipt observation; renewals remain compatible with both.

use super::*;
use tokeira_kernel::ActivityStartReceipt;
use tokeira_types::{QueueKey, TaskKind, WorkerIdentity};

// Feature: activity-dispatch, Property 14
// Lease renewal preserves an admitted operation; takeover/release invalidates it.
#[test]
fn activity_dispatch_live_fenced_repair_and_observation() -> anyhow::Result<()> {
    anyhow::ensure!(
        std::env::var_os("TOKEIRA_DSQL_TEST_DATABASE_URL").is_some(),
        "explicit activity fence test requires TOKEIRA_DSQL_TEST_DATABASE_URL"
    );
    let runtime = Runtime::new()?;
    let fixture = runtime
        .block_on(Fixture::connect())?
        .expect("configured fixture");
    let mut runner = TestRunner::new(ProptestConfig {
        cases: 100,
        failure_persistence: None,
        ..Default::default()
    });
    runner
        .run(&(any::<bool>(), 0u8..3), |(replay, change)| {
            runtime.block_on(async {
                fixture.clear_lease().await.unwrap();
                let epoch = fixture.acquire_old().await.unwrap();
                let mut start = start_at_home();
                let mut activity = crate::activity_dispatch_tests::pending_activity("fenced", 3);
                if replay {
                    activity.started_at = Some(activity.scheduled_at);
                    activity.started_event_id = Some(-2);
                    activity.started_identity = Some(WorkerIdentity("worker".into()));
                    start.next_state.activity_start_receipts.insert(
                        "fenced".into(),
                        ActivityStartReceipt {
                            request_id: uuid::Uuid::new_v4(),
                            schedule_event_id: 3,
                            attempt: 1,
                            offered_stamp: 0,
                            worker_identity: WorkerIdentity("worker".into()),
                            dispatch_revision: 0,
                            selected_queue: QueueKey {
                                namespace_id: start.next_state.namespace_id,
                                task_queue: activity.task_queue.clone(),
                                task_kind: TaskKind::Activity,
                                deployment: None,
                                build_id: None,
                            },
                        },
                    );
                }
                start
                    .next_state
                    .activities
                    .insert("fenced".into(), activity);
                let run = start.next_state.run_key;
                let state = applied(
                    fixture
                        .repo()
                        .commit_transition_for_bundle(run, HOME, start, old_fence(epoch))
                        .await
                        .unwrap(),
                );
                if !replay {
                    sqlx::query("DELETE FROM activity_dispatch WHERE run_key=$1")
                        .bind(run.0)
                        .execute(&fixture.pool)
                        .await
                        .unwrap();
                }
                let (entered, resume) = pause_point::arm(PauseKey::Write(run));
                let operation = async {
                    if replay {
                        fixture
                            .repo()
                            .do_validate_activity_start_observation(
                                &state,
                                "fenced",
                                HOME,
                                &old_fence(epoch),
                            )
                            .await
                            .map(|value| {
                                assert!(value);
                            })
                    } else {
                        fixture
                            .repo()
                            .reconcile_activity_dispatch_page(HOME, run, None, old_fence(epoch))
                            .await
                            .map(|_| ())
                    }
                };
                let driver = async {
                    entered.await.unwrap();
                    match change {
                        0 => {
                            assert!(matches!(
                                fixture
                                    .repo()
                                    .renew_bundle(HOME, OLD_OWNER.into(), epoch, ENDPOINT.into())
                                    .await
                                    .unwrap(),
                                LeaseOutcome::Renewed { .. }
                            ));
                        }
                        1 => {
                            fixture.take_over().await.unwrap();
                        }
                        _ => {
                            assert!(matches!(
                                fixture
                                    .repo()
                                    .relinquish_bundle(HOME, OLD_OWNER.into(), epoch)
                                    .await
                                    .unwrap(),
                                LeaseOutcome::Acquired { .. }
                            ));
                        }
                    }
                    resume.send(()).unwrap();
                };
                let (result, ()) = tokio::join!(operation, driver);
                prop_assert_eq!(result.is_ok(), change == 0, "{:?}", result);
                if replay && change != 0 {
                    // The public adapter maps this physical conflict to false;
                    // assert its cause here rather than mistaking false for an
                    // accepted replay or requiring the public API to return Err.
                    prop_assert!(
                        result
                            .as_ref()
                            .unwrap_err()
                            .downcast_ref::<sqlx::Error>()
                            .is_some_and(DsqlRunRepository::is_serialization_failure),
                        "{result:?}"
                    );
                }
                prop_assert_eq!(
                    fixture.repo().load_run(run).await.unwrap(),
                    LoadedRun::Existing(state.clone())
                );
                if change != 0 {
                    let retry = if replay {
                        fixture
                            .repo()
                            .validate_activity_start_observation(
                                &state,
                                "fenced",
                                HOME,
                                old_fence(epoch),
                            )
                            .await
                            .map(|_| ())
                    } else {
                        fixture
                            .repo()
                            .reconcile_activity_dispatch_page(HOME, run, None, old_fence(epoch))
                            .await
                            .map(|_| ())
                    };
                    prop_assert!(retry.unwrap_err().is::<LeaseFenceLost>());
                    let rows: i64 = sqlx::query_scalar(
                        "SELECT count(*) FROM activity_dispatch WHERE run_key=$1",
                    )
                    .bind(run.0)
                    .fetch_one(&fixture.pool)
                    .await
                    .unwrap();
                    prop_assert_eq!(rows, 0);
                }
                Ok(())
            })
        })
        .unwrap();
    runtime.block_on(async {
        fixture.clear_lease().await?;
        fixture.store.shutdown().await
    })
}
