//! Disposable-cluster checks of the derived-row record (`dsql-derived-row-record`):
//! the rows and record each commit leaves as every kind crosses between wanted
//! and none, an older release's commit, both dispatch repairs under a record
//! that rules their kind out, and a reset successor whose first commit must read.

use std::collections::BTreeMap;

use super::{
    bulk_write_tests::Fixture,
    derived_record::{DerivedKinds, read_record},
    *,
};
use crate::{
    activity_dispatch_tests::{pending_activity, scheduled_activity_events},
    memory::projection_accumulator_tests::{applied, following, fresh_transition},
    value_parts,
};
use tokeira_kernel::{PendingExternalSignal, PendingWorkflowTask};

/// The hot-state upsert before the record existed, as an older release still
/// sends it: it names neither record column.
const OLDER_RELEASE_UPSERT: &str = "INSERT INTO workflow_hot
     (run_key, namespace_id, workflow_id, shard_id, transition_seq, state_data,
      history_size_bytes, recovery_needed, state_parts, updated_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now())
     ON CONFLICT (run_key) DO UPDATE SET
         transition_seq = EXCLUDED.transition_seq,
         state_data = EXCLUDED.state_data,
         history_size_bytes = EXCLUDED.history_size_bytes,
         recovery_needed = EXCLUDED.recovery_needed,
         state_parts = EXCLUDED.state_parts,
         shard_id = EXCLUDED.shard_id,
         updated_at = EXCLUDED.updated_at";

/// What a state wants: a workflow dispatch row, activity rows, operation rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Shape {
    workflow: bool,
    activities: usize,
    operations: usize,
}

const fn shape(workflow: bool, activities: usize, operations: usize) -> Shape {
    Shape {
        workflow,
        activities,
        operations,
    }
}

impl Shape {
    fn record(self) -> DerivedKinds {
        DerivedKinds::after_commit(self.workflow, self.activities > 0, self.operations > 0)
    }
}

/// Give `state` exactly the wanted rows `shape` names. `task` is the pending
/// workflow task the start fixture schedules.
fn reshape(state: &mut WorkflowState, task: &Option<PendingWorkflowTask>, shape: Shape) {
    state.pending_workflow_task = if shape.workflow { task.clone() } else { None };
    state.activities = (0..shape.activities)
        .map(|index| pending_activity(&format!("a-{index}"), index as i64 + 3))
        .map(|activity| (activity.activity_id.clone(), activity))
        .collect();
    state.pending_external_signals = (0..shape.operations)
        .map(|index| {
            let initiated_event_id = 100 + index as i64;
            let signal = PendingExternalSignal {
                initiated_event_id,
                target_namespace_id: state.namespace_id,
                target_namespace: None,
                target_workflow_id: WorkflowId(format!("target-{index}")),
                target_run_id: None,
                signal_name: "signal".into(),
            };
            (initiated_event_id, signal)
        })
        .collect::<BTreeMap<_, _>>();
}

async fn held(fixture: &Fixture, run: RunKey) -> Result<Shape> {
    let count = |sql: &'static str| async move {
        let (count,) = sqlx::query_as::<_, (i64,)>(sql)
            .bind(run.0)
            .fetch_one(&fixture.pool)
            .await?;
        anyhow::Ok(usize::try_from(count)?)
    };
    Ok(Shape {
        workflow: count("SELECT count(*) FROM workflow_dispatch WHERE run_key=$1").await? == 1,
        activities: count("SELECT count(*) FROM activity_dispatch WHERE run_key=$1").await?,
        operations: count("SELECT count(*) FROM run_operation WHERE source_run_key=$1").await?,
    })
}

/// The run's record as a commit would read it, and the raw stamp.
async fn record(fixture: &Fixture, run: RunKey) -> Result<(Option<DerivedKinds>, Option<i64>)> {
    let (seq, kinds, stamp) = sqlx::query_as::<_, (i64, Option<i16>, Option<i64>)>(
        "SELECT transition_seq, derived_kinds, derived_kinds_seq FROM workflow_hot WHERE run_key=$1",
    )
    .bind(run.0)
    .fetch_one(&fixture.pool)
    .await?;
    Ok((read_record(seq, kinds, stamp), stamp))
}

async fn home(fixture: &Fixture, run: RunKey) -> Result<ShardId> {
    let (uuid,) =
        sqlx::query_as::<_, (Uuid,)>("SELECT shard_id FROM workflow_hot WHERE run_key=$1")
            .bind(run.0)
            .fetch_one(&fixture.pool)
            .await?;
    DsqlRunRepository::shard_id_from_uuid(uuid)
}

/// Check that the run's record is valid and names exactly the kinds its stored
/// state wants: what a commit that read every kind writes.
pub(super) async fn assert_exact_record(fixture: &Fixture, run: RunKey) -> Result<()> {
    let LoadedRun::Existing(state) = fixture.store.run_repository().load_run(run).await? else {
        panic!("{run:?} is published")
    };
    let shard = home(fixture, run).await?;
    let wanted = DerivedKinds::after_commit(
        crate::derive_workflow_dispatch(&state, shard).is_some(),
        !crate::derive_activity_dispatch(&state, shard)?.is_empty(),
        !crate::derive_operations(&state, shard)?.is_empty(),
    );
    assert_eq!(record(fixture, run).await?.0, Some(wanted));
    Ok(())
}

/// Commit `shape` from `state` and check the rows and the exact record left.
async fn commit_shape(
    fixture: &Fixture,
    state: &WorkflowState,
    task: &Option<PendingWorkflowTask>,
    wanted: Shape,
) -> Result<WorkflowState> {
    let mut transition = following(state);
    reshape(&mut transition.next_state, task, wanted);
    let next = applied(
        fixture
            .store
            .run_repository()
            .commit_transition(state.run_key, transition, WriteFence::Unfenced)
            .await?,
    );
    assert_eq!(held(fixture, next.run_key).await?, wanted);
    assert_eq!(
        record(fixture, next.run_key).await?.0,
        Some(wanted.record())
    );
    Ok(next)
}

/// Replace the stored state without a commit, keeping its sequence and record,
/// as a release whose derivation differs would leave it to a repair.
async fn store_state_only(fixture: &Fixture, state: &WorkflowState) -> Result<()> {
    let data = codec::encode_workflow_state(state)?;
    assert!(
        data.len() <= value_parts::PART_BYTES,
        "fixture state fits one part"
    );
    sqlx::query("UPDATE workflow_hot SET state_data=$2, state_parts=NULL WHERE run_key=$1")
        .bind(state.run_key.0)
        .bind(data)
        .execute(&fixture.pool)
        .await?;
    Ok(())
}

async fn start(fixture: &Fixture) -> Result<(WorkflowState, Option<PendingWorkflowTask>)> {
    let initial = fresh_transition(RunKey::new());
    let task = initial.next_state.pending_workflow_task.clone();
    assert!(
        task.is_some(),
        "the start fixture schedules a workflow task"
    );
    let run = initial.next_state.run_key;
    let state = applied(
        fixture
            .store
            .run_repository()
            .commit_transition(run, initial, WriteFence::Unfenced)
            .await?,
    );
    Ok((state, task))
}

#[tokio::test]
async fn derived_record_live_follows_every_kind_across_none_and_back() -> Result<()> {
    let Some(fixture) = Fixture::connect().await? else {
        return Ok(());
    };
    let (mut state, task) = start(&fixture).await?;
    for wanted in [
        shape(true, 2, 2),
        shape(false, 2, 2),
        shape(false, 0, 2),
        shape(false, 0, 0),
        shape(false, 0, 0),
        shape(true, 0, 0),
        shape(true, 3, 0),
        shape(false, 0, 1),
        shape(true, 1, 0),
        shape(false, 0, 0),
    ] {
        state = commit_shape(&fixture, &state, &task, wanted).await?;
    }
    fixture.store.shutdown().await
}

#[tokio::test]
async fn derived_record_live_an_older_release_commit_leaves_it_unknown() -> Result<()> {
    let Some(fixture) = Fixture::connect().await? else {
        return Ok(());
    };
    let (state, task) = start(&fixture).await?;
    let state = commit_shape(&fixture, &state, &task, shape(false, 0, 0)).await?;
    let shard = home(&fixture, state.run_key).await?;

    // The older release maintains every kind itself and moves the sequence
    // through its own upsert.
    let mut older = following(&state).next_state;
    reshape(&mut older, &task, shape(true, 2, 1));
    let data = codec::encode_workflow_state(&older)?;
    let mut tx = fixture.pool.begin().await?;
    sqlx::query(OLDER_RELEASE_UPSERT)
        .bind(older.run_key.0)
        .bind(older.namespace_id.0)
        .bind(&older.workflow_id.0)
        .bind(DsqlRunRepository::shard_id_to_uuid(shard))
        .bind(convert::i64_from_u64(
            older.transition_seq.0,
            "transition_seq",
        )?)
        .bind(data)
        .bind(0_i64)
        .bind(false)
        .bind(None::<i32>)
        .execute(&mut *tx)
        .await?;
    super::workflow_dispatch::maintain(&mut tx, &older, shard).await?;
    for row in crate::derive_activity_dispatch(&older, shard)?.values() {
        super::activity_dispatch::upsert(&mut tx, row).await?;
    }
    for row in crate::derive_operations(&older, shard)? {
        super::outbound::write_operation_row(&mut tx, &row).await?;
    }
    tx.commit().await?;
    assert_eq!(held(&fixture, older.run_key).await?, shape(true, 2, 1));
    let (known, stamp) = record(&fixture, older.run_key).await?;
    assert_eq!(known, None, "the stamp no longer matches the sequence");
    assert!(
        stamp.is_some(),
        "the older upsert left the columns as they were"
    );

    // This release's next commit reads every kind and removes what it finds.
    commit_shape(&fixture, &older, &task, shape(false, 0, 0)).await?;
    fixture.store.shutdown().await
}

#[tokio::test]
async fn derived_record_live_repairs_forget_a_record_that_rules_their_kind_out() -> Result<()> {
    let Some(fixture) = Fixture::connect().await? else {
        return Ok(());
    };
    let repo = fixture.store.run_repository();

    // Workflow dispatch: the stored state wants a row its record rules out.
    let (state, task) = start(&fixture).await?;
    let state = commit_shape(&fixture, &state, &task, shape(false, 0, 0)).await?;
    let shard = home(&fixture, state.run_key).await?;
    let mut differing = state.clone();
    reshape(&mut differing, &task, shape(true, 0, 0));
    store_state_only(&fixture, &differing).await?;
    repo.reconcile_workflow_dispatch_run(shard, state.run_key, WriteFence::Unfenced)
        .await?;
    assert_eq!(held(&fixture, state.run_key).await?, shape(true, 0, 0));
    assert_eq!(record(&fixture, state.run_key).await?, (None, None));
    commit_shape(&fixture, &differing, &task, shape(false, 0, 0)).await?;

    // A repair whose kind the record includes leaves it as it is.
    let (state, task) = start(&fixture).await?;
    let state = commit_shape(&fixture, &state, &task, shape(true, 0, 0)).await?;
    let shard = home(&fixture, state.run_key).await?;
    repo.reconcile_workflow_dispatch_run(shard, state.run_key, WriteFence::Unfenced)
        .await?;
    assert_eq!(
        record(&fixture, state.run_key).await?.0,
        Some(shape(true, 0, 0).record())
    );

    // Activity dispatch: the same, through one repair page.
    let (state, task) = start(&fixture).await?;
    let state = commit_shape(&fixture, &state, &task, shape(false, 0, 0)).await?;
    let shard = home(&fixture, state.run_key).await?;
    let mut differing = state.clone();
    reshape(&mut differing, &task, shape(false, 2, 0));
    store_state_only(&fixture, &differing).await?;
    let page = repo
        .reconcile_activity_dispatch_page(shard, state.run_key, None, WriteFence::Unfenced)
        .await?;
    assert_eq!(page.changed, 2);
    assert_eq!(held(&fixture, state.run_key).await?, shape(false, 2, 0));
    assert_eq!(record(&fixture, state.run_key).await?, (None, None));
    commit_shape(&fixture, &differing, &task, shape(false, 0, 0)).await?;
    fixture.store.shutdown().await
}

#[tokio::test]
async fn derived_record_live_a_reset_successor_reads_its_staged_rows() -> Result<()> {
    let Some(fixture) = Fixture::connect().await? else {
        return Ok(());
    };
    let repo = fixture.store.run_repository();
    let events = scheduled_activity_events(3);
    let (base, fork) = crate::bulk_write_tests::commit_base(&fixture, vec![events]).await;
    let run_id = tokeira_types::RunId::new();
    let run = RunKey::derive(base.namespace_id, &base.workflow_id, run_id);
    repo.materialize_reset_successor(
        base.run_key,
        fork,
        run_id,
        Some(base.run_key),
        WriteFence::Unfenced,
    )
    .await?;
    let LoadedRun::Existing(state) = repo.load_run(run).await? else {
        panic!("the reset successor is published")
    };
    assert_eq!(held(&fixture, run).await?.activities, 3);
    assert_eq!(record(&fixture, run).await?, (None, None));

    // Its first commit wants no activity rows, so it must find the staged ones.
    let mut transition = following(&state);
    transition.next_state.activities.clear();
    let next = applied(
        repo.commit_transition(run, transition, WriteFence::Unfenced)
            .await?,
    );
    assert_eq!(held(&fixture, run).await?.activities, 0);
    let (known, _) = record(&fixture, run).await?;
    assert!(known.is_some_and(|kinds| !kinds.contains(DerivedKinds::ACTIVITY_DISPATCH)));
    assert_eq!(next.transition_seq, state.transition_seq.next());
    fixture.store.shutdown().await
}
