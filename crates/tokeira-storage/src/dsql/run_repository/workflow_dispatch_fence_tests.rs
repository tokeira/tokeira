//! Repair interleavings isolate the lease row from ordinary dispatch-row OCC.
//! Every case owns its run key; no whole-home scan includes another suite's data.

use super::*;
use crate::{WorkflowDispatchRepairConflict, WorkflowDispatchRow};

async fn dispatch(fixture: &Fixture, key: RunKey) -> anyhow::Result<Option<WorkflowDispatchRow>> {
    let mut query = sqlx::QueryBuilder::new(super::super::workflow_dispatch::SELECT_ROW);
    query.push(" WHERE run_key=").push_bind(key.0);
    query
        .build()
        .fetch_optional(&fixture.pool)
        .await?
        .map(super::super::workflow_dispatch::decode)
        .transpose()
}

async fn repair_case(
    fixture: &Fixture,
    event: LeaseEvent,
    delete: bool,
    successor_write: bool,
    release_last: bool,
) -> anyhow::Result<()> {
    fixture.clear_lease().await?;
    let epoch = fixture.acquire_old().await?;
    let transition = loop {
        let candidate = start_at_home();
        if candidate.next_state.run_key.0.as_u128() as u32 % SHARD_COUNT != HOME.0 {
            break candidate;
        }
    };
    let state = applied(
        fixture
            .repo()
            .commit_transition_for_bundle(
                transition.next_state.run_key,
                HOME,
                transition,
                old_fence(epoch),
            )
            .await?,
    );
    let key = state.run_key;
    // Deletion repairs an orphan; insertion reconstructs a missing row. The
    // isolated controls only change the lease; generated cases also race a
    // successor write. The controls cannot pass solely because of dispatch OCC.
    let sql = if delete {
        "DELETE FROM workflow_hot WHERE run_key=$1"
    } else {
        "DELETE FROM workflow_dispatch WHERE run_key=$1"
    };
    sqlx::query(sql).bind(key.0).execute(&fixture.pool).await?;
    let before = dispatch(fixture, key).await?;
    let (at_repair, resume_repair) = pause_point::arm(PauseKey::Write(key));
    let (start_takeover, take_over_now) = oneshot::channel();
    let (repair_done, wait_repair) = oneshot::channel();
    let repair = async {
        let result = fixture
            .repo()
            .reconcile_workflow_dispatch_run(HOME, key, old_fence(epoch))
            .await;
        let _ = repair_done.send(());
        result
    };
    let takeover = async {
        if matches!(event, LeaseEvent::TakeoverLandingLast) {
            take_over_now.await?;
            Ok(Some(if release_last {
                fixture
                    .repo()
                    .relinquish_bundle(HOME, OLD_OWNER.into(), epoch)
                    .await
                    .map(|outcome| match outcome {
                        LeaseOutcome::Acquired { epoch } => epoch,
                        other => panic!("release: {other:?}"),
                    })
            } else {
                fixture.take_over().await
            }))
        } else {
            anyhow::Ok(None)
        }
    };
    let driver = async {
        at_repair.await?;
        let mut held_takeover = None;
        match event {
            LeaseEvent::Nothing => {}
            LeaseEvent::Renew => {
                assert_eq!(
                    fixture
                        .repo()
                        .renew_bundle(HOME, OLD_OWNER.into(), epoch, ENDPOINT.into())
                        .await?,
                    LeaseOutcome::Renewed { epoch }
                );
            }
            LeaseEvent::Refresh => {
                assert_eq!(fixture.acquire_old().await?, epoch);
            }
            LeaseEvent::Takeover => {
                fixture.take_over().await?;
            }
            LeaseEvent::Reacquire => {
                fixture.take_over_at(HOME, OLD_OWNER).await?;
            }
            LeaseEvent::Release => {
                fixture
                    .repo()
                    .relinquish_bundle(HOME, OLD_OWNER.into(), epoch)
                    .await?;
            }
            LeaseEvent::TakeoverLandingLast => {
                let (at_takeover, resume_takeover) = pause_point::arm(if release_last {
                    PauseKey::Release(HOME)
                } else {
                    PauseKey::Acquisition(HOME)
                });
                start_takeover.send(()).unwrap();
                at_takeover.await?;
                held_takeover = Some(resume_takeover);
            }
        }
        let successor_row = if successor_write && event.changes_ownership_first() {
            let next = fixture.take_over_at(HOME, NEW_OWNER).await?;
            let fence = WriteFence::Lease(ExecutionHomeFence {
                home: HOME,
                owner: NEW_OWNER.into(),
                epoch: next,
            });
            if delete {
                fixture
                    .repo()
                    .reconcile_workflow_dispatch_run(HOME, key, fence)
                    .await?;
            } else {
                let mut transition = following(&state);
                transition.next_state.task_queue.0 = "successor-queue".into();
                applied(
                    fixture
                        .repo()
                        .commit_transition_for_bundle(key, HOME, transition, fence)
                        .await?,
                );
            }
            let successor_row = dispatch(fixture, key).await?;
            assert_eq!(successor_row.is_some(), !delete);
            if let Some(row) = &successor_row {
                assert_eq!(row.queue_name.0, "successor-queue");
            }
            Some(successor_row)
        } else {
            None
        };
        let _ = resume_repair.send(());
        wait_repair.await?;
        if let Some(resume) = held_takeover {
            let _ = resume.send(());
        }
        anyhow::Ok(successor_row)
    };
    let (repaired, taken, driven) = tokio::join!(repair, takeover, driver);
    let expected_row = driven?.unwrap_or(before);
    if event.changes_ownership_first() {
        assert!(
            repaired
                .as_ref()
                .is_err_and(|error| error.is::<WorkflowDispatchRepairConflict>()),
            "stale repair must conflict (successor_write={successor_write}): {event:?}, delete={delete}, {repaired:?}"
        );
        assert_eq!(
            dispatch(fixture, key).await?,
            expected_row,
            "aborted repair preserves the committed dispatch state"
        );
        let retried = fixture
            .repo()
            .reconcile_workflow_dispatch_run(HOME, key, old_fence(epoch))
            .await;
        assert!(
            is_lease_lost(&retried),
            "ownership loss must be terminal: {retried:?}"
        );
    } else {
        repaired?;
        let row = dispatch(fixture, key).await?;
        assert_eq!(row.is_some(), !delete);
        if let Some(row) = row {
            assert_eq!(row.incarnation.run_key, key);
            assert_eq!(
                row.incarnation.logical_seq,
                state.pending_workflow_task.as_ref().unwrap().logical_seq
            );
        }
        if matches!(event, LeaseEvent::TakeoverLandingLast) {
            let error = taken?
                .expect("takeover attempted")
                .expect_err("repair landing first aborts takeover");
            assert!(
                error
                    .downcast_ref::<sqlx::Error>()
                    .is_some_and(DsqlRunRepository::is_serialization_failure),
                "{error:#}"
            );
            assert_eq!(
                fixture.lease_at(HOME).await?,
                Some((Some(OLD_OWNER.into()), epoch))
            );
        }
    }
    // Remove this case's recovery candidates and dispatch keys, so no later
    // whole-home suite inherits repair work. History is never scanned by repair.
    for sql in [
        "DELETE FROM workflow_hot WHERE run_key=$1",
        "DELETE FROM workflow_dispatch WHERE run_key=$1",
    ] {
        sqlx::query(sql).bind(key.0).execute(&fixture.pool).await?;
    }
    fixture.clear_lease().await?;
    Ok(())
}

// Feature: workflow-dispatch, Property 9: Ownership loss fences repair
#[test]
fn workflow_dispatch_live_fenced_repair_generated_interleavings() {
    live_property(
        (lease_event(), any::<bool>(), any::<bool>(), any::<bool>()),
        100,
        |runtime, fixture, (event, delete, successor, release_last)| {
            runtime
                .block_on(repair_case(fixture, event, delete, successor, release_last))
                .unwrap();
            Ok(())
        },
    );
}

// Feature: workflow-dispatch, Property 9: Ownership loss fences repair
#[tokio::test]
async fn workflow_dispatch_live_fenced_repair_isolates_lease_conflicts() -> anyhow::Result<()> {
    let Some(fixture) = Fixture::connect().await? else {
        return Ok(());
    };
    for event in [
        LeaseEvent::Takeover,
        LeaseEvent::Release,
        LeaseEvent::TakeoverLandingLast,
        LeaseEvent::Renew,
    ] {
        for delete in [false, true] {
            repair_case(&fixture, event, delete, false, false).await?;
        }
    }
    for delete in [false, true] {
        repair_case(
            &fixture,
            LeaseEvent::TakeoverLandingLast,
            delete,
            false,
            true,
        )
        .await?;
    }
    Ok(())
}
