//! Live placement upgrade atomicity, diagnostics and bounded query-plan evidence.
//! All data belongs to the disposable URL-gated test cluster.

use std::time::Instant;

use anyhow::{Result, ensure};
use sqlx::{Execute, Postgres, QueryBuilder};
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{TimerOp, TimerState};
use tokeira_types::{NamespaceId, RunKey, ShardEpoch, ShardId, execution_home_bundle};
use uuid::Uuid;

use super::{
    placement::{hot_screen_query, timer_screen_query},
    workflow_dispatch_tests::Fixture,
};
use crate::{
    CommitResult, PlacementPage, PlacementPhase, RunRepository, TimerPosition,
    memory::projection_accumulator_tests::fresh_transition,
    placement_upgrade::{PLACEMENT_UPGRADE, shard_uuid},
    prepare_execution_placement,
};

async fn reset_marker(f: &Fixture) -> Result<()> {
    sqlx::query("DELETE FROM workflow_placement_upgrade WHERE name=$1")
        .bind(PLACEMENT_UPGRADE)
        .execute(&f.pool)
        .await?;
    Ok(())
}

async fn seed(
    f: &Fixture,
    index: u32,
    hot: bool,
    timer: bool,
) -> Result<(RunKey, ShardId, Vec<u8>)> {
    let mut transition = fresh_transition(RunKey::new());
    transition.next_state.workflow_id.0 = format!("placement-live-{index}-{}", Uuid::new_v4());
    let home = execution_home_bundle(
        transition.next_state.namespace_id.0.as_bytes(),
        transition.next_state.workflow_id.0.as_bytes(),
        8,
    );
    let key = RunKey(Uuid::from_u128(
        (Uuid::new_v4().as_u128() & !7) | u128::from((home.0 + 1) % 8),
    ));
    transition.next_state.run_key = key;
    let timer_state = TimerState {
        timer_id: "placement-timer".into(),
        started_event_id: 2,
        fire_at: OffsetDateTime::UNIX_EPOCH + Duration::days(10),
    };
    transition
        .next_state
        .timers
        .insert(timer_state.timer_id.clone(), timer_state.clone());
    transition.timer_ops.push(TimerOp::Upsert(timer_state));
    let result = f
        .store
        .run_repository()
        .commit_transition(key, transition, ShardEpoch::ZERO)
        .await?;
    ensure!(
        matches!(result, CommitResult::Applied { .. }),
        "fixture must commit"
    );
    let bytes: Vec<u8> = sqlx::query_scalar("SELECT state_data FROM workflow_hot WHERE run_key=$1")
        .bind(key.0)
        .fetch_one(&f.pool)
        .await?;
    let old = shard_uuid(ShardId((key.0.as_u128() as u32) % 8));
    if hot {
        sqlx::query("UPDATE workflow_hot SET shard_id=$1 WHERE run_key=$2")
            .bind(old)
            .bind(key.0)
            .execute(&f.pool)
            .await?;
    }
    if timer {
        let mut tx = f.pool.begin().await?;
        sqlx::query("INSERT INTO timer_bucket (shard_id,fire_at,run_key,timer_id,timer_data,created_at) SELECT $1,fire_at,run_key,timer_id,timer_data,created_at FROM timer_bucket WHERE run_key=$2 AND shard_id=$3")
            .bind(old).bind(key.0).bind(shard_uuid(home)).execute(&mut *tx).await?;
        sqlx::query("DELETE FROM timer_bucket WHERE run_key=$1 AND shard_id=$2")
            .bind(key.0)
            .bind(shard_uuid(home))
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
    }
    sqlx::query("DELETE FROM workflow_dispatch WHERE run_key=$1")
        .bind(key.0)
        .execute(&f.pool)
        .await?;
    Ok((key, home, bytes))
}

#[tokio::test]
async fn placement_live_concurrent_starters_and_late_page() -> Result<()> {
    if std::env::var_os("TOKEIRA_DSQL_TEST_DATABASE_URL").is_none() {
        return Ok(());
    }
    let f = Fixture::connect(8).await?;
    reset_marker(&f).await?;
    let mut fixtures = Vec::new();
    for index in 0..100 {
        fixtures.push(seed(&f, index, index % 2 == 0, index % 3 != 0).await?);
    }
    let repo = f.store.run_repository();
    let PlacementPage::Committed(initial) = repo.prepare_placement_page().await? else {
        anyhow::bail!("initial marker lost without a competitor");
    };
    assert!(matches!(initial.phase, PlacementPhase::Hot(None)));
    let (key, home, _) = &fixtures[1];
    // This old page would put a valid row back on the wrong shard. It changes
    // no row a later page needs to write: only the shared marker fences it.
    let mut late = f.pool.begin().await?;
    let revision: i64 =
        sqlx::query_scalar("SELECT revision FROM workflow_placement_upgrade WHERE name=$1")
            .bind(PLACEMENT_UPGRADE)
            .fetch_one(&mut *late)
            .await?;
    repo.prepare_placement_page().await?;
    let (a, b, c) = tokio::join!(
        prepare_execution_placement(repo),
        prepare_execution_placement(repo),
        prepare_execution_placement(repo)
    );
    let progress = a?;
    assert_eq!(progress, b?);
    assert_eq!(progress, c?);
    assert_eq!(progress.hot_moved, 50);
    assert_eq!(progress.timers_moved, 66);
    sqlx::query("UPDATE workflow_hot SET shard_id=$1 WHERE run_key=$2")
        .bind(shard_uuid(ShardId((home.0 + 2) % 8)))
        .bind(key.0)
        .execute(&mut *late)
        .await?;
    let changed = sqlx::query(
        "UPDATE workflow_placement_upgrade SET revision=revision+1 WHERE name=$1 AND revision=$2",
    )
    .bind(PLACEMENT_UPGRADE)
    .bind(revision)
    .execute(&mut *late)
    .await;
    let rejected = match changed {
        Ok(_) => late.commit().await,
        Err(error) => Err(error),
    };
    assert!(
        matches!(rejected, Err(sqlx::Error::Database(ref db)) if db.code().is_some_and(|code| code == "40001"))
    );
    for (key, home, bytes) in fixtures {
        let actual: (Uuid, Vec<u8>) =
            sqlx::query_as("SELECT shard_id,state_data FROM workflow_hot WHERE run_key=$1")
                .bind(key.0)
                .fetch_one(&f.pool)
                .await?;
        assert_eq!(actual, (shard_uuid(home), bytes));
        let timers: Vec<Uuid> =
            sqlx::query_scalar("SELECT shard_id FROM timer_bucket WHERE run_key=$1")
                .bind(key.0)
                .fetch_all(&f.pool)
                .await?;
        assert_eq!(timers, [shard_uuid(home)]);
        repo.reconcile_workflow_dispatch_run(home, key).await?;
        let dispatch: Uuid =
            sqlx::query_scalar("SELECT shard_id FROM workflow_dispatch WHERE run_key=$1")
                .bind(key.0)
                .fetch_one(&f.pool)
                .await?;
        assert_eq!(dispatch, shard_uuid(home));
    }
    let revision: i64 =
        sqlx::query_scalar("SELECT revision FROM workflow_placement_upgrade WHERE name=$1")
            .bind(PLACEMENT_UPGRADE)
            .fetch_one(&f.pool)
            .await?;
    assert_eq!(prepare_execution_placement(repo).await?, progress);
    let repeated: i64 =
        sqlx::query_scalar("SELECT revision FROM workflow_placement_upgrade WHERE name=$1")
            .bind(PLACEMENT_UPGRADE)
            .fetch_one(&f.pool)
            .await?;
    assert_eq!(revision, repeated);
    f.store.shutdown().await?;
    f.pool.close().await;
    Ok(())
}

#[tokio::test]
async fn placement_live_fail_stop_and_raw_lease_guard() -> Result<()> {
    if std::env::var_os("TOKEIRA_DSQL_TEST_DATABASE_URL").is_none() {
        return Ok(());
    }
    let f = Fixture::connect(8).await?;
    let (key, home, bytes) = seed(&f, 101, false, false).await?;
    let repo = f.store.run_repository();
    for (shard, data, expected) in [
        (Uuid::new_v4(), bytes.clone(), "source is neither"),
        (
            shard_uuid(ShardId((key.0.as_u128() as u32) % 8)),
            vec![255],
            "cannot decode relocation identity",
        ),
    ] {
        reset_marker(&f).await?;
        sqlx::query("UPDATE workflow_hot SET shard_id=$1,state_data=$2 WHERE run_key=$3")
            .bind(shard)
            .bind(data)
            .bind(key.0)
            .execute(&f.pool)
            .await?;
        let error = prepare_execution_placement(repo)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains(expected)
                && error.contains(&format!("{key:?}"))
                && error.contains("computed_home"),
            "{error}"
        );
        sqlx::query("UPDATE workflow_hot SET shard_id=$1,state_data=$2 WHERE run_key=$3")
            .bind(shard_uuid(home))
            .bind(&bytes)
            .bind(key.0)
            .execute(&f.pool)
            .await?;
    }
    reset_marker(&f).await?;
    let unknown_encoding = Uuid::new_v4();
    sqlx::query("INSERT INTO shard_lease (shard_id,owner,epoch,lease_expiry) VALUES ($1,'placement-test',1,$2)")
        .bind(unknown_encoding).bind(OffsetDateTime::now_utc() + Duration::hours(1)).execute(&f.pool).await?;
    let error = prepare_execution_placement(repo)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("live shard lease"));
    sqlx::query("DELETE FROM shard_lease WHERE shard_id=$1")
        .bind(unknown_encoding)
        .execute(&f.pool)
        .await?;
    prepare_execution_placement(repo).await?;
    f.store.shutdown().await?;
    f.pool.close().await;
    Ok(())
}

async fn explain(f: &Fixture, mut query: QueryBuilder<Postgres>) -> Result<Vec<String>> {
    let mut query = query.build();
    let args = query
        .take_arguments()
        .map_err(anyhow::Error::from_boxed)?
        .expect("bound query");
    let mut builder = QueryBuilder::<Postgres>::with_arguments("EXPLAIN ", args);
    builder.push(query.sql().as_str());
    Ok(builder
        .build_query_as::<(String,)>()
        .fetch_all(&f.pool)
        .await?
        .into_iter()
        .map(|(line,)| line)
        .collect())
}

#[tokio::test]
async fn placement_live_joined_continuation_and_screening_latency() -> Result<()> {
    if std::env::var_os("TOKEIRA_DSQL_TEST_DATABASE_URL").is_none() {
        return Ok(());
    }
    let f = Fixture::connect(8).await?;
    let namespace = NamespaceId::new();
    let mut positions = Vec::new();
    let mut keys = Vec::new();
    let fire_at = OffsetDateTime::UNIX_EPOCH;
    for batch in 0..48 {
        let entries: Vec<_> = (0..256)
            .map(|index| {
                let workflow = format!("screening-{}", batch * 256 + index);
                let home = execution_home_bundle(namespace.0.as_bytes(), workflow.as_bytes(), 8);
                (RunKey::new(), workflow, home)
            })
            .collect();
        let mut tx = f.pool.begin().await?;
        let mut hot = QueryBuilder::<Postgres>::new(
            "INSERT INTO workflow_hot (run_key,namespace_id,workflow_id,shard_id,transition_seq,state_data) ",
        );
        hot.push_values(&entries, |mut row, (key, workflow, home)| {
            row.push_bind(key.0)
                .push_bind(namespace.0)
                .push_bind(workflow.clone())
                .push_bind(shard_uuid(*home))
                .push_bind(1i64)
                .push_bind(vec![255u8]);
        });
        hot.build().execute(&mut *tx).await?;
        let mut timers = QueryBuilder::<Postgres>::new(
            "INSERT INTO timer_bucket (shard_id,fire_at,run_key,timer_id,timer_data,created_at) ",
        );
        timers.push_values(&entries, |mut row, (key, _, home)| {
            row.push_bind(shard_uuid(*home))
                .push_bind(fire_at)
                .push_bind(key.0)
                .push_bind("screening")
                .push_bind(vec![255u8])
                .push_bind(fire_at);
        });
        timers.build().execute(&mut *tx).await?;
        tx.commit().await?;
        for (key, _, home) in entries {
            positions.push(TimerPosition {
                shard: shard_uuid(home),
                fire_at,
                run_key: key,
                timer_id: "screening".into(),
            });
            keys.push(key);
        }
    }
    positions.sort();
    let mut report = String::from(
        "Screening sample: 12,288 hot rows and 12,288 timers, all correctly placed; payloads are deliberately undecodable to prove screening reads only metadata.\n",
    );
    for offset in [0, 4_096, 11_000] {
        let cursor = (offset > 0).then(|| &positions[offset - 1]);
        let plan = explain(&f, timer_screen_query(cursor)).await?;
        if cursor.is_some() {
            for part in ["shard_id >", "fire_at >", "run_key >", "timer_id >"] {
                assert!(
                    plan.iter()
                        .any(|line| line.contains("Index Cond:") && line.contains(part)),
                    "missing seek {part}: {plan:#?}"
                );
            }
        }
        assert!(
            plan.iter()
                .any(|line| line.contains("Index Cond:") && line.contains("run_key = ANY")),
            "join must use hot primary key: {plan:#?}"
        );
        report.push_str(&format!(
            "\nJoined timer continuation offset {offset}:\n{}\n",
            plan.join("\n")
        ));
    }
    keys.sort();
    for cursor in [None, Some(keys[8_000])] {
        let plan = explain(&f, hot_screen_query(cursor)).await?;
        if cursor.is_some() {
            assert!(
                plan.iter()
                    .any(|line| line.contains("Index Cond:") && line.contains("run_key >"))
            );
        }
        report.push_str(&format!("\nHot continuation:\n{}\n", plan.join("\n")));
    }
    reset_marker(&f).await?;
    let started = Instant::now();
    let progress = prepare_execution_placement(f.store.run_repository()).await?;
    let elapsed = started.elapsed().as_secs_f64();
    assert_eq!(progress.hot_moved + progress.timers_moved, 0);
    let screened = progress.hot_examined + progress.timers_examined;
    report.push_str(&format!("\nCommitted unchanged rows screened: {screened}; elapsed seconds: {elapsed:.3}; extrapolated seconds for one million runs plus one million timers: {:.1}. This extrapolation excludes relocation and is not a million-row benchmark.\n", elapsed * 2_000_000.0 / screened as f64));
    if let Ok(path) = std::env::var("TOKEIRA_PLACEMENT_REPORT") {
        std::fs::write(path, report)?;
    }
    // Only remove this test's deliberately invalid payload fixtures. Keeping
    // them would make unrelated acquisition suites fail on intentional corruption.
    for batch in keys.chunks(256) {
        let ids: Vec<_> = batch.iter().map(|key| key.0).collect();
        let mut tx = f.pool.begin().await?;
        sqlx::query("DELETE FROM timer_bucket WHERE run_key = ANY($1)")
            .bind(&ids)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM workflow_hot WHERE run_key = ANY($1)")
            .bind(&ids)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
    }
    f.store.shutdown().await?;
    f.pool.close().await;
    Ok(())
}

#[tokio::test]
async fn placement_live_stopped_reset_duplicates_and_orphan_timers() -> Result<()> {
    if std::env::var_os("TOKEIRA_DSQL_TEST_DATABASE_URL").is_none() {
        return Ok(());
    }
    let f = Fixture::connect(8).await?;
    let repo = f.store.run_repository();
    let successor = crate::placement_upgrade_tests::reset_successor(repo).await?;
    let key = successor.run_key;
    let home = execution_home_bundle(
        successor.namespace_id.0.as_bytes(),
        successor.workflow_id.0.as_bytes(),
        8,
    );
    let old = shard_uuid(ShardId((key.0.as_u128() as u32) % 8));
    let history = repo.read_history(key, 0, 128).await?;
    let before: Vec<u8> =
        sqlx::query_scalar("SELECT state_data FROM workflow_hot WHERE run_key=$1")
            .bind(key.0)
            .fetch_one(&f.pool)
            .await?;
    sqlx::query("UPDATE workflow_hot SET shard_id=$1 WHERE run_key=$2")
        .bind(old)
        .bind(key.0)
        .execute(&f.pool)
        .await?;
    sqlx::query("INSERT INTO timer_bucket (shard_id,fire_at,run_key,timer_id,timer_data,created_at) SELECT $1,fire_at,run_key,timer_id,timer_data,created_at FROM timer_bucket WHERE run_key=$2 AND shard_id=$3")
        .bind(old).bind(key.0).bind(shard_uuid(home)).execute(&f.pool).await?;
    sqlx::query("DELETE FROM timer_bucket WHERE shard_id=$1 AND run_key=$2")
        .bind(shard_uuid(home))
        .bind(key.0)
        .execute(&f.pool)
        .await?;
    sqlx::query("DELETE FROM workflow_dispatch WHERE run_key=$1")
        .bind(key.0)
        .execute(&f.pool)
        .await?;
    reset_marker(&f).await?;
    prepare_execution_placement(repo).await?;
    let actual: (Uuid, Vec<u8>) =
        sqlx::query_as("SELECT shard_id,state_data FROM workflow_hot WHERE run_key=$1")
            .bind(key.0)
            .fetch_one(&f.pool)
            .await?;
    assert_eq!(actual, (shard_uuid(home), before));
    assert_eq!(repo.read_history(key, 0, 128).await?, history);
    assert_eq!(
        repo.find_latest_run(successor.namespace_id, &successor.workflow_id)
            .await?,
        Some(key)
    );
    repo.reconcile_workflow_dispatch_run(home, key).await?;
    let row: Uuid = sqlx::query_scalar("SELECT shard_id FROM workflow_dispatch WHERE run_key=$1")
        .bind(key.0)
        .fetch_one(&f.pool)
        .await?;
    assert_eq!(row, shard_uuid(home));

    for conflicting in [true, false] {
        sqlx::query("INSERT INTO timer_bucket (shard_id,fire_at,run_key,timer_id,timer_data,created_at) SELECT $1,fire_at,run_key,timer_id,timer_data,created_at FROM timer_bucket WHERE run_key=$2 AND shard_id=$3")
            .bind(old).bind(key.0).bind(shard_uuid(home)).execute(&f.pool).await?;
        if conflicting {
            sqlx::query("UPDATE timer_bucket SET timer_data=$1 WHERE shard_id=$2 AND run_key=$3")
                .bind(vec![255u8])
                .bind(old)
                .bind(key.0)
                .execute(&f.pool)
                .await?;
        }
        reset_marker(&f).await?;
        if conflicting {
            let error = prepare_execution_placement(repo)
                .await
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("conflicting destination timer payload")
                    && error.contains(&format!("{key:?}"))
            );
            let count: i64 =
                sqlx::query_scalar("SELECT count(*) FROM timer_bucket WHERE run_key=$1")
                    .bind(key.0)
                    .fetch_one(&f.pool)
                    .await?;
            assert_eq!(count, 2, "failed page retains both rows");
            sqlx::query("DELETE FROM timer_bucket WHERE shard_id=$1 AND run_key=$2")
                .bind(old)
                .bind(key.0)
                .execute(&f.pool)
                .await?;
        } else {
            prepare_execution_placement(repo).await?;
            let count: i64 =
                sqlx::query_scalar("SELECT count(*) FROM timer_bucket WHERE run_key=$1")
                    .bind(key.0)
                    .fetch_one(&f.pool)
                    .await?;
            assert_eq!(count, 1, "equivalent destination is retained");
        }
    }
    let orphan = RunKey::new();
    sqlx::query("INSERT INTO timer_bucket (shard_id,fire_at,run_key,timer_id,timer_data,created_at) VALUES ($1,$2,$3,'orphan',$4,$2)")
        .bind(old).bind(OffsetDateTime::UNIX_EPOCH).bind(orphan.0).bind(vec![255u8]).execute(&f.pool).await?;
    reset_marker(&f).await?;
    prepare_execution_placement(repo).await?;
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM timer_bucket WHERE run_key=$1")
        .bind(orphan.0)
        .fetch_one(&f.pool)
        .await?;
    assert_eq!(count, 1);
    sqlx::query("DELETE FROM timer_bucket WHERE run_key=$1")
        .bind(orphan.0)
        .execute(&f.pool)
        .await?;
    f.store.shutdown().await?;
    f.pool.close().await;
    Ok(())
}
