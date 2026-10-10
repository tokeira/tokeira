//! The shared bounded-bulk-writes contracts against an operator-owned ephemeral
//! Aurora DSQL cluster, and the cases only DSQL can show: a purge resumed by two
//! purges at once, and a materialization's transaction fenced by the switch of
//! its record. No connection occurs unless the dedicated test URL is provided.

use std::path::Path;

use sqlx::{PgPool, postgres::PgPoolOptions};
use tokeira_kernel::TimerState;

use super::{
    bulk_write::{CopyItem, PURGE_TABLES, SELECT_RECORD_FOR_UPDATE},
    *,
};
use crate::{
    BulkWritePhase,
    bulk_write_tests::{
        Backend, OwnedRows, StoredBatch, delete_first, deletion_of_a_large_run, large_closed_run,
        plain_reset_of_a_closed_workflow, reset_over_one_mib, reset_over_ten_mib,
        reset_with_a_start_between, reset_with_many_timers, spill_of_large_entries,
        spill_of_many_small_entries,
    },
    dsql::{DsqlPoolConfig, DsqlStore, MigrationConfig, ReservoirConfig},
    write_budget::MAX_ROWS_PER_TRANSACTION,
};

const SHARD_COUNT: u32 = 1;

struct Fixture {
    pool: PgPool,
    store: DsqlStore,
}

impl Fixture {
    /// The cluster's store, or none when the live suite isn't configured.
    async fn connect() -> Result<Option<Self>> {
        let Ok(url) = std::env::var("TOKEIRA_DSQL_TEST_DATABASE_URL") else {
            return Ok(None);
        };
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .min_connections(4)
            .idle_timeout(None)
            .connect(&url)
            .await?;
        let store = DsqlStore::from_database_url_for_tests(
            url,
            DsqlPoolConfig {
                migration: MigrationConfig {
                    migrations_dir: Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations"),
                },
                reservoir: ReservoirConfig {
                    target_ready: 4,
                    inflight_limit: 2,
                    ..ReservoirConfig::default()
                },
                shard_count: SHARD_COUNT,
                conflict_policy: CurrentExecutionConflictPolicy::Reject,
                ..DsqlPoolConfig::default()
            },
        )
        .await?;
        store.migration_runner().apply(&pool).await?;
        Ok(Some(Self { pool, store }))
    }

    async fn count(&self, sql: &'static str, run_key: RunKey) -> Result<usize> {
        let (count,) = sqlx::query_as::<_, (i64,)>(sql)
            .bind(run_key.0)
            .fetch_one(&self.pool)
            .await?;
        Ok(usize::try_from(count)?)
    }

    async fn recorded(&self, run_key: RunKey) -> Result<bool> {
        Ok(self
            .count(
                "SELECT count(*) FROM run_bulk_write WHERE run_key = $1",
                run_key,
            )
            .await?
            == 1)
    }
}

#[async_trait]
impl Backend for Fixture {
    fn repo(&self) -> &dyn RunRepository {
        self.store.run_repository()
    }

    fn shard_count(&self) -> u32 {
        SHARD_COUNT
    }

    async fn owned_rows(&self, run_key: RunKey) -> Result<OwnedRows> {
        Ok(OwnedRows {
            hot: self
                .count(
                    "SELECT count(*) FROM workflow_hot WHERE run_key = $1",
                    run_key,
                )
                .await?,
            history: self
                .count(
                    "SELECT count(*) FROM history_batch WHERE run_key = $1",
                    run_key,
                )
                .await?,
            request_dedupe: self
                .count(
                    "SELECT count(*) FROM request_dedupe WHERE run_key = $1",
                    run_key,
                )
                .await?,
            timers: self
                .count(
                    "SELECT count(*) FROM timer_bucket WHERE run_key = $1",
                    run_key,
                )
                .await?,
            activity_dispatch: self
                .count(
                    "SELECT count(*) FROM activity_dispatch WHERE run_key = $1",
                    run_key,
                )
                .await?,
            workflow_dispatch: self
                .count(
                    "SELECT count(*) FROM workflow_dispatch WHERE run_key = $1",
                    run_key,
                )
                .await?,
            backlog: self
                .count(
                    "SELECT count(*) FROM dispatch_backlog WHERE run_key = $1",
                    run_key,
                )
                .await?,
        })
    }

    async fn history_batches(&self, run_key: RunKey) -> Result<Vec<StoredBatch>> {
        sqlx::query_as::<_, (i64, i64, i32)>(
            "SELECT first_event_id, last_event_id, octet_length(events_data)
             FROM history_batch WHERE run_key = $1 ORDER BY first_event_id",
        )
        .bind(run_key.0)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|(first, last, bytes)| {
            Ok(StoredBatch {
                events: usize::try_from(last - first + 1)?,
                bytes: usize::try_from(bytes)?,
            })
        })
        .collect()
    }
}

#[tokio::test]
async fn dsql_bulk_spill_of_many_small_entries() -> Result<()> {
    if let Some(fixture) = Fixture::connect().await? {
        spill_of_many_small_entries(&fixture).await;
    }
    Ok(())
}

#[tokio::test]
async fn dsql_bulk_spill_of_large_entries() -> Result<()> {
    if let Some(fixture) = Fixture::connect().await? {
        spill_of_large_entries(&fixture).await;
    }
    Ok(())
}

#[tokio::test]
async fn dsql_bulk_deletion_of_a_large_run() -> Result<()> {
    if let Some(fixture) = Fixture::connect().await? {
        deletion_of_a_large_run(&fixture).await;
    }
    Ok(())
}

#[tokio::test]
async fn dsql_bulk_reset_over_one_mib() -> Result<()> {
    if let Some(fixture) = Fixture::connect().await? {
        reset_over_one_mib(&fixture).await;
    }
    Ok(())
}

#[tokio::test]
async fn dsql_bulk_reset_over_ten_mib() -> Result<()> {
    if let Some(fixture) = Fixture::connect().await? {
        reset_over_ten_mib(&fixture).await;
    }
    Ok(())
}

#[tokio::test]
async fn dsql_bulk_reset_with_many_timers() -> Result<()> {
    if let Some(fixture) = Fixture::connect().await? {
        reset_with_many_timers(&fixture).await;
    }
    Ok(())
}

#[tokio::test]
async fn dsql_bulk_plain_reset_of_a_closed_workflow() -> Result<()> {
    if let Some(fixture) = Fixture::connect().await? {
        plain_reset_of_a_closed_workflow(&fixture).await;
    }
    Ok(())
}

#[tokio::test]
async fn dsql_bulk_reset_with_a_start_between() -> Result<()> {
    if let Some(fixture) = Fixture::connect().await? {
        reset_with_a_start_between(&fixture).await;
    }
    Ok(())
}

/// A deletion whose purge stopped after one page, finished by two purges at
/// once. The run stays unreachable while its record remains; a purge that keeps
/// losing conflicts to the other leaves the rest to the next; and the purges
/// leave none of the run's rows.
#[tokio::test]
async fn dsql_bulk_interrupted_deletion() -> Result<()> {
    let Some(fixture) = Fixture::connect().await? else {
        return Ok(());
    };
    let repo = fixture.store.run_repository();
    let state = large_closed_run(&fixture).await;
    delete_first(&fixture, &state).await;
    let before = fixture.owned_rows(state.run_key).await?;
    assert_eq!((before.hot, before.workflow_dispatch), (0, 0));

    let request_dedupe = &PURGE_TABLES[0];
    assert_eq!(request_dedupe.table, "request_dedupe");
    let purged = repo
        .purge_page(state.run_key, request_dedupe, MAX_ROWS_PER_TRANSACTION)
        .await?;
    assert_eq!(purged, MAX_ROWS_PER_TRANSACTION);
    let stopped = fixture.owned_rows(state.run_key).await?;
    assert_eq!(stopped.request_dedupe, before.request_dedupe - purged);
    assert_eq!(stopped.history, before.history);
    assert!(fixture.recorded(state.run_key).await?);
    assert!(matches!(
        repo.load_run(state.run_key).await?,
        LoadedRun::Absent
    ));

    let (first, second) =
        tokio::join!(repo.purge_run(state.run_key), repo.purge_run(state.run_key));
    for result in [first, second] {
        if let Err(error) = result {
            assert!(is_serialization_failure_error(&error), "{error:?}");
        }
    }
    repo.purge_run(state.run_key).await?;
    assert_eq!(
        fixture.owned_rows(state.run_key).await?,
        OwnedRows::default()
    );
    assert!(!fixture.recorded(state.run_key).await?);
    Ok(())
}

/// A materialization abandoned while one of its copy transactions is open.
/// That transaction read the record before the switch, so DSQL refuses its
/// commit; a copy that starts after the switch reads it and stops; and the
/// purge leaves none of the successor's rows.
#[tokio::test]
async fn dsql_bulk_abandoned_materialization_fences_its_stragglers() -> Result<()> {
    let Some(fixture) = Fixture::connect().await? else {
        return Ok(());
    };
    let repo = fixture.store.run_repository();
    let successor = RunKey::new();
    let shard = ShardId(0);
    let timer = TimerState {
        timer_id: "timer".to_string(),
        started_event_id: 5,
        fire_at: OffsetDateTime::now_utc() + Duration::hours(1),
    };
    let page = [CopyItem::Timer(&timer)];
    repo.record_materialization(successor, shard).await?;
    repo.copy_materialization_page(successor, shard, TransitionSeq(1), &page)
        .await?;

    let mut straggler = fixture.pool.begin().await?;
    let (phase,) = sqlx::query_as::<_, (i16,)>(SELECT_RECORD_FOR_UPDATE)
        .bind(successor.0)
        .fetch_one(&mut *straggler)
        .await?;
    assert_eq!(
        BulkWritePhase::from_db_smallint(phase)?,
        BulkWritePhase::Materializing
    );
    sqlx::query(
        "INSERT INTO history_batch
         (run_key, first_event_id, last_event_id, transition_seq, events_data, created_at)
         VALUES ($1, 1, 1, 1, $2, now())",
    )
    .bind(successor.0)
    .bind(vec![0u8; 16])
    .execute(&mut *straggler)
    .await?;

    repo.abandon_materialization(successor).await?;
    let refused = straggler
        .commit()
        .await
        .expect_err("the straggler read the record before the switch");
    assert!(
        DsqlRunRepository::is_serialization_failure(&refused),
        "{refused:?}"
    );
    assert!(
        repo.copy_materialization_page(successor, shard, TransitionSeq(1), &page)
            .await
            .is_err(),
        "a copy that starts after the switch reads it"
    );

    repo.purge_run(successor).await?;
    assert_eq!(fixture.owned_rows(successor).await?, OwnedRows::default());
    assert!(!fixture.recorded(successor).await?);
    Ok(())
}
