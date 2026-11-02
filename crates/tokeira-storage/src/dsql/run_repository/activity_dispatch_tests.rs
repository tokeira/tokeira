//! Explicit live DSQL adapters for the same generated activity storage oracles.
//!
//! Selecting these tests without an approved test database fails preflight. They
//! never report a missing live environment as successful physical verification.

use super::{bulk_write_tests::Fixture, *};
use crate::activity_dispatch_tests::{self as contracts, Backend};

#[async_trait]
impl Backend for Fixture {
    fn activity_repo(&self) -> &dyn RunRepository {
        self.store.run_repository()
    }

    fn activity_shard_count(&self) -> u32 {
        1
    }

    async fn seed_activity_backlog(&self, entries: Vec<BacklogEntry>) -> Result<()> {
        // These are small fixed-shape fixture copies, not production delivery.
        // Batch setup so the physical test measures all 100 disposal traces
        // without spending its deadline on the retired producer's round trips.
        let rows = entries
            .into_iter()
            .map(|entry| {
                let partition = partition_for(
                    entry.run_key,
                    self.store.run_repository().projection_partition_count,
                );
                let key = DsqlRunRepository::dispatch_backlog_key(
                    partition,
                    entry.queue.namespace_id,
                    &entry.queue.task_queue.0,
                    entry.queue.task_kind,
                    entry
                        .queue
                        .deployment
                        .as_ref()
                        .map(|value| value.0.as_str()),
                    entry.queue.build_id.as_ref().map(|value| value.0.as_str()),
                    entry.run_key,
                    &entry.payload,
                );
                let payload =
                    codec::encode_backlog_payload(&entry.payload, entry.priority.as_ref())?;
                let tie = i64::try_from(entry.order.insertion_tie)?;
                Ok((entry, i32::try_from(partition)?, key, payload, tie))
            })
            .collect::<Result<Vec<_>>>()?;
        for page in rows.chunks(64) {
            let mut query = sqlx::QueryBuilder::<sqlx::Postgres>::new(
                "INSERT INTO dispatch_backlog (key, partition_id, queue_namespace, queue_name,
                 task_kind, deployment, build_id, priority_key, fair_pass, insertion_tie,
                 run_key, payload_data, scheduled_at) ",
            );
            query.push_values(page, |mut values, (entry, partition, key, payload, tie)| {
                values
                    .push_bind(key)
                    .push_bind(partition)
                    .push_bind(entry.queue.namespace_id.0)
                    .push_bind(&entry.queue.task_queue.0)
                    .push_bind(entry.queue.task_kind.to_db_smallint())
                    .push_bind(entry.queue.deployment.as_ref().map(|value| &value.0))
                    .push_bind(entry.queue.build_id.as_ref().map(|value| &value.0))
                    .push_bind(entry.order.priority_key)
                    .push_bind(entry.order.fair_pass)
                    .push_bind(tie)
                    .push_bind(entry.run_key.0)
                    .push_bind(payload)
                    .push_bind(entry.scheduled_at);
            });
            query.push(" ON CONFLICT (key) DO NOTHING");
            query.build().execute(&self.pool).await?;
        }
        Ok(())
    }

    async fn clear_activity_rows(&self, run: RunKey) -> Result<()> {
        loop {
            let mut tx = self.pool.begin().await?;
            let keys = sqlx::query_scalar::<_, Uuid>(
                "SELECT key FROM activity_dispatch WHERE run_key=$1 ORDER BY key LIMIT 64",
            )
            .bind(run.0)
            .fetch_all(&mut *tx)
            .await?;
            if keys.is_empty() {
                tx.rollback().await?;
                return Ok(());
            }
            sqlx::query("DELETE FROM activity_dispatch WHERE key=ANY($1)")
                .bind(keys)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
        }
    }

    async fn seed_activity_rows(&self, rows: &[(crate::ActivityDispatchRow, bool)]) -> Result<()> {
        for page in rows.chunks(64) {
            let mut tx = self.pool.begin().await?;
            for (row, legacy) in page {
                // Raw seeding deliberately bypasses checked production upsert:
                // negative stored stamps and unequal digests are test corruption.
                sqlx::query("INSERT INTO activity_dispatch
                    (key, run_key, activity_id, shard_id, queue_namespace, queue_name, task_kind,
                     deployment, build_id, schedule_event_id, attempt, dispatch_revision, stamp,
                     input_data, priority_data, dispatch_at, created_at, queue_key)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,now(),$17)
                    ON CONFLICT (key) DO UPDATE SET run_key=EXCLUDED.run_key, activity_id=EXCLUDED.activity_id,
                     shard_id=EXCLUDED.shard_id, queue_namespace=EXCLUDED.queue_namespace, queue_name=EXCLUDED.queue_name,
                     task_kind=EXCLUDED.task_kind, deployment=EXCLUDED.deployment, build_id=EXCLUDED.build_id,
                     schedule_event_id=EXCLUDED.schedule_event_id, attempt=EXCLUDED.attempt, dispatch_revision=EXCLUDED.dispatch_revision,
                     stamp=EXCLUDED.stamp, input_data=EXCLUDED.input_data, priority_data=EXCLUDED.priority_data,
                     dispatch_at=EXCLUDED.dispatch_at, queue_key=EXCLUDED.queue_key")
                    .bind(row.key).bind(row.incarnation.run_key.0).bind(&row.incarnation.activity_id)
                    .bind(DsqlRunRepository::shard_id_to_uuid(row.execution_home))
                    .bind(row.queue.namespace_id.0).bind(&row.queue.task_queue.0).bind(row.queue.task_kind.to_db_smallint())
                    .bind(row.queue.deployment.as_ref().map(|value| &value.0)).bind(row.queue.build_id.as_ref().map(|value| &value.0))
                    .bind(row.incarnation.schedule_event_id).bind(row.incarnation.attempt as i32).bind(row.dispatch_revision)
                    .bind(row.incarnation.stamp as i64).bind(codec::empty_input_data()?)
                    .bind(row.priority.as_ref().map(codec::encode_priority).transpose()?)
                    .bind(row.dispatch_at).bind((!*legacy).then_some(&row.queue_key))
                    .execute(&mut *tx).await?;
            }
            tx.commit().await?;
        }
        Ok(())
    }
}

pub(super) fn fixture() -> Result<(tokio::runtime::Runtime, Fixture)> {
    anyhow::ensure!(
        std::env::var_os("TOKEIRA_DSQL_TEST_DATABASE_URL").is_some(),
        "live activity-dispatch verification requires TOKEIRA_DSQL_TEST_DATABASE_URL; no live test ran"
    );
    let runtime = tokio::runtime::Runtime::new()?;
    let fixture = runtime.block_on(async {
        let fixture = Fixture::connect()
            .await?
            .context("live activity database configuration disappeared")?;
        let jobs = sqlx::query_as::<_, (String, String)>(
            "SELECT job_id, status FROM sys.jobs WHERE job_type='INDEX_BUILD'",
        )
        .fetch_all(&fixture.pool)
        .await?;
        for (job, status) in jobs {
            if status == "submitted" || status == "processing" {
                anyhow::ensure!(
                    sqlx::query_scalar::<_, bool>("CALL sys.wait_for_job($1)")
                        .bind(job)
                        .fetch_one(&fixture.pool)
                        .await?,
                    "activity index build did not finish"
                );
            } else {
                anyhow::ensure!(
                    status == "completed",
                    "activity index build failed: {status}"
                );
            }
        }
        anyhow::Ok(fixture)
    })?;
    Ok((runtime, fixture))
}

#[test]
fn activity_dispatch_live_derivation() -> Result<()> {
    let (runtime, fixture) = fixture()?;
    contracts::derivation_cases(&fixture, &runtime);
    runtime.block_on(fixture.store.shutdown())
}

#[test]
fn activity_dispatch_live_legacy_disposal() -> Result<()> {
    let (runtime, fixture) = fixture()?;
    contracts::disposal_cases(&fixture, &runtime);
    runtime.block_on(fixture.store.shutdown())
}

#[test]
fn activity_dispatch_live_atomic() -> Result<()> {
    let (runtime, fixture) = fixture()?;
    contracts::atomic_cases(fixture.activity_repo(), &runtime);
    runtime.block_on(fixture.store.shutdown())
}

#[test]
fn activity_dispatch_live_pair() -> Result<()> {
    let (runtime, fixture) = fixture()?;
    contracts::pair_cases(fixture.activity_repo(), &runtime);
    runtime.block_on(fixture.store.shutdown())
}

#[test]
fn activity_dispatch_live_ordered_pages() -> Result<()> {
    let (runtime, fixture) = fixture()?;
    contracts::ordered_cases(fixture.activity_repo(), &runtime);
    runtime.block_on(fixture.store.shutdown())
}

#[test]
fn activity_dispatch_live_malformed_pages() -> Result<()> {
    let (runtime, fixture) = fixture()?;
    contracts::malformed_page_cases(&fixture, &runtime);
    runtime.block_on(fixture.store.shutdown())
}

#[test]
fn activity_dispatch_live_repair() -> Result<()> {
    let (runtime, fixture) = fixture()?;
    contracts::repair_cases(&fixture, &runtime);
    runtime.block_on(fixture.store.shutdown())
}

#[test]
fn activity_dispatch_live_staged() -> Result<()> {
    let (runtime, fixture) = fixture()?;
    contracts::staged_cases(&fixture, &runtime);
    runtime.block_on(fixture.store.shutdown())
}

#[test]
fn activity_dispatch_live_replay_observation() -> Result<()> {
    let (runtime, fixture) = fixture()?;
    contracts::observation_cases(&fixture, &runtime);
    runtime.block_on(fixture.store.shutdown())
}

#[test]
fn activity_dispatch_live_physical_refusal_is_atomic() -> Result<()> {
    let (runtime, fixture) = fixture()?;
    runtime.block_on(async {
        let initial = crate::memory::projection_accumulator_tests::fresh_transition(RunKey::new());
        let run = initial.next_state.run_key;
        let CommitResult::Applied { new_state: before } = fixture
            .activity_repo()
            .commit_transition(run, initial, WriteFence::Unfenced)
            .await?
        else {
            panic!("seed")
        };
        let mut excessive = crate::memory::projection_accumulator_tests::following(&before);
        for index in 0..=crate::write_budget::DSQL_MAX_ROWS_PER_TRANSACTION {
            let id = format!("limit-{index}");
            excessive.next_state.activities.insert(
                id.clone(),
                contracts::pending_activity(&id, index as i64 + 3),
            );
        }
        let error = fixture
            .activity_repo()
            .commit_transition(run, excessive, WriteFence::Unfenced)
            .await
            .expect_err("DSQL must refuse the whole oversized ordinary transition");
        let code = error
            .downcast_ref::<sqlx::Error>()
            .and_then(|error| match error {
                sqlx::Error::Database(database) => database.code().map(|code| code.into_owned()),
                _ => None,
            });
        assert_eq!(
            code.as_deref(),
            Some("54000"),
            "expected physical program_limit_exceeded: {error:#}"
        );
        assert_eq!(
            fixture.activity_repo().load_run(run).await?,
            LoadedRun::Existing(before)
        );
        let rows: i64 =
            sqlx::query_scalar("SELECT count(*) FROM activity_dispatch WHERE run_key=$1")
                .bind(run.0)
                .fetch_one(&fixture.pool)
                .await?;
        assert_eq!(
            rows, 0,
            "refused transition left partially published activities"
        );
        fixture.store.shutdown().await
    })
}

#[path = "activity_query_plan_tests.rs"]
mod query_plans;

#[path = "activity_race_tests.rs"]
mod races;
