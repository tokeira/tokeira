//! Opt-in shared contracts against an operator-owned ephemeral Aurora DSQL
//! cluster. No connection occurs unless the dedicated test URL is provided.

use std::path::Path;

use sqlx::{Execute, PgPool, postgres::PgPoolOptions};
use tokeira_kernel::HistoryEventKind;
use tokeira_types::{LogicalTaskSeq, WorkerIdentity};

use super::*;
use crate::{
    WorkflowDispatchRow,
    dsql::{DsqlPoolConfig, DsqlStore, MigrationConfig, ReservoirConfig},
    workflow_dispatch_tests::{
        Backend, ordered_pages, reset_materialization, routing_and_home_pages, run_atomic_cases,
        speculative_legacy_delivery,
    },
};

pub(super) struct Fixture {
    pub(super) pool: PgPool,
    pub(super) store: DsqlStore,
    repair_home: ShardId,
    shard_count: u32,
}

// Feature: queue-home-delivery, Property 8: Complete bounded sticky traversal.
#[tokio::test]
async fn queue_home_delivery_live_sticky_query_plans() -> Result<()> {
    if std::env::var_os("TOKEIRA_DSQL_TEST_DATABASE_URL").is_none() {
        return Ok(());
    }
    let fixture = Fixture::connect(8).await?;
    let home = ShardId(2_345_678);
    let mut template =
        crate::memory::projection_accumulator_tests::fresh_transition(RunKey::new()).next_state;
    let mut keys = Vec::new();
    let mut target = Vec::new();
    for batch in 0..32 {
        let mut rows = Vec::new();
        for offset in 0..256 {
            let index = batch * 256 + offset;
            template.run_key = RunKey::new();
            template.sticky = (index % 2 == 0).then(|| tokeira_types::StickyAffinity {
                worker_identity: WorkerIdentity("plan-worker".into()),
                sticky_queue: TaskQueueName("plan-sticky".into()),
                schedule_to_start_timeout: Duration::seconds(30),
            });
            template
                .pending_workflow_task
                .as_mut()
                .unwrap()
                .schedule_to_start_deadline = template
                .sticky
                .as_ref()
                .map(|_| OffsetDateTime::UNIX_EPOCH + Duration::seconds(30));
            let assigned = if index < 2048 {
                home
            } else {
                ShardId(home.0 + 1 + index % 64)
            };
            keys.push(template.run_key);
            if assigned == home && template.sticky.is_some() {
                target.push(template.run_key);
            }
            rows.push((assigned, template.clone()));
        }
        fixture.sticky_seed(&rows).await?;
    }
    target.sort();
    let mut report = String::from(
        "Sticky page plans: 8192 fixture rows; 1024 sticky rows in the selected home; limit 64.\n",
    );
    for offset in [0, 512, 960] {
        let after = (offset > 0).then(|| target[offset - 1]);
        let limit = std::num::NonZeroU32::new(64).unwrap();
        let page = fixture
            .repo()
            .list_sticky_workflow_dispatch(home, after, limit)
            .await?;
        assert_eq!(
            page.candidates
                .iter()
                .map(|row| row.incarnation.run_key)
                .collect::<Vec<_>>(),
            target[offset..offset + 64]
        );
        let mut builder = super::workflow_dispatch::sticky_page_query(home, after, limit);
        let mut query = builder.build();
        let arguments = query
            .take_arguments()
            .map_err(anyhow::Error::from_boxed)?
            .expect("bound sticky page");
        let mut explain =
            sqlx::QueryBuilder::<sqlx::Postgres>::with_arguments("EXPLAIN ", arguments);
        explain.push(query.sql().as_str());
        let plan = explain
            .build_query_as::<(String,)>()
            .fetch_all(&fixture.pool)
            .await?;
        assert!(
            plan.iter()
                .any(|(line,)| line.contains("idx_workflow_dispatch_sticky_home")),
            "{plan:#?}"
        );
        assert!(
            plan.iter().any(|(line,)| line.contains("Index Cond:")
                && line.contains("shard_id =")
                && (after.is_none() || line.contains("run_key >"))),
            "{plan:#?}"
        );
        assert!(
            plan.iter()
                .any(|(line,)| line.contains("Index Cond:") && line.contains("run_key =")),
            "payload fetch must use bounded primary-key lookups: {plan:#?}"
        );
        report.push_str(&format!("\nOffset {offset}:\n"));
        for (line,) in plan {
            report.push_str(&line);
            report.push('\n');
        }
    }
    for batch in keys.chunks(256) {
        fixture.sticky_remove(batch).await?;
    }
    if let Some(path) = std::env::var_os("TOKEIRA_QUEUE_HOME_PLAN_OUTPUT") {
        std::fs::write(path, report)?;
    }
    fixture.store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn workflow_dispatch_live_query_plans() -> Result<()> {
    if std::env::var_os("TOKEIRA_DSQL_TEST_DATABASE_URL").is_none() {
        return Ok(());
    }
    let fixture = Fixture::connect(8).await?;
    let mut template =
        crate::memory::projection_accumulator_tests::fresh_transition(RunKey::new()).next_state;
    template.namespace_id = tokeira_types::NamespaceId::new();
    let namespace_id = template.namespace_id;
    let mut positions = [Vec::new(), Vec::new()];
    let scheduled_at = template
        .pending_workflow_task
        .as_ref()
        .unwrap()
        .scheduled_at;
    for batch in 0..64 {
        let mut tx = fixture.pool.begin().await?;
        for offset in 0..256 {
            let index = batch * 256 + offset;
            template.run_key = RunKey::new();
            template.task_queue.0 = format!(
                "explain-queue-{}",
                if index < 8192 {
                    0
                } else {
                    1 + (index / 2) % 128
                }
            );
            template
                .pending_workflow_task
                .as_mut()
                .unwrap()
                .scheduled_at =
                scheduled_at + Duration::seconds(((index / 2) % 4096 / 1024) as i64);
            template.deployment =
                (index % 2 == 1).then(|| tokeira_types::DeploymentId("explain-deployment".into()));
            template.build_id = template
                .deployment
                .as_ref()
                .map(|_| tokeira_types::BuildId("explain-build".into()));
            let home = ShardId(batch);
            super::workflow_dispatch::maintain(&mut tx, &template, home).await?;
            if index < 8192 {
                positions[(index % 2) as usize].push(
                    crate::derive_workflow_dispatch(&template, home)
                        .unwrap()
                        .position(),
                );
            }
        }
        tx.commit().await?;
    }
    for mode in &mut positions {
        mode.sort();
    }
    let total: i64 = sqlx::query_scalar("SELECT count(*) FROM workflow_dispatch")
        .fetch_one(&fixture.pool)
        .await?;
    let mut report = format!(
        "Seeded rows: 16384; table rows after seeding: {total}.\nLive and Exact each contain 4096 rows in queue 0, all priority 3, with 1024 equal timestamps per group. Another 8192 rows span 128 unrelated queue families.\n64 execution homes contain 256 seeded rows each.\nASYNC indexes were awaited by the migration runner before seeding.\n"
    );
    for (index, routing) in [
        crate::WorkflowDispatchRouting::Live,
        crate::WorkflowDispatchRouting::Exact {
            deployment: tokeira_types::DeploymentId("explain-deployment".into()),
            build_id: Some(tokeira_types::BuildId("explain-build".into())),
        },
    ]
    .into_iter()
    .enumerate()
    {
        let range = crate::WorkflowDiscoveryRange {
            namespace_id,
            queue_name: tokeira_types::TaskQueueName("explain-queue-0".into()),
            routing,
        };
        for offset in [0, 768, 2048, 3840] {
            let cursor = (offset > 0).then(|| positions[index][offset - 1]);
            let actual = fixture
                .repo()
                .list_workflow_dispatch_page(&range, cursor, std::num::NonZeroU32::new(64).unwrap())
                .await?;
            assert_eq!(
                actual
                    .candidates
                    .iter()
                    .map(WorkflowDispatchRow::position)
                    .collect::<Vec<_>>(),
                positions[index][offset..offset + 64]
            );
            for before in [true, false] {
                let mut builder = if before {
                    legacy_queue_page_query(&range, cursor)
                } else {
                    super::workflow_dispatch::queue_page_query(
                        &range,
                        cursor,
                        std::num::NonZeroU32::new(64).unwrap(),
                    )
                };
                let mut query = builder.build();
                let arguments = query
                    .take_arguments()
                    .map_err(anyhow::Error::from_boxed)?
                    .expect("page query has bound arguments");
                let mut explain =
                    sqlx::QueryBuilder::<sqlx::Postgres>::with_arguments("EXPLAIN ", arguments);
                explain.push(query.sql().as_str());
                let plan = explain
                    .build_query_as::<(String,)>()
                    .fetch_all(&fixture.pool)
                    .await?;
                if !before {
                    assert!(
                        plan.iter()
                            .any(|(line,)| line.contains("Index Cond:")
                                && line.contains("run_key =")),
                        "bounded payload page requires primary-key equality lookups: {plan:#?}"
                    );
                }
                if !before && cursor.is_some() {
                    let conditions: Vec<_> = plan
                        .iter()
                        .map(|(line,)| line.as_str())
                        .filter(|line| line.contains("Index Cond:"))
                        .collect();
                    for required in [
                        &["priority_key =", "scheduled_at =", "run_key >"][..],
                        &["priority_key =", "scheduled_at >"][..],
                        &["priority_key >"][..],
                    ] {
                        assert!(
                            conditions
                                .iter()
                                .any(|line| required.iter().all(|part| line.contains(part))),
                            "continuation lacks complete scalar seek {required:?}: {plan:#?}"
                        );
                    }
                }
                report.push_str(&format!("\nQueue mode {index}, offset={offset}, before={before}, range selectivity={:.6}:\n", 4096.0 / total as f64));
                for (line,) in plan {
                    report.push_str(&line);
                    report.push('\n');
                }
            }
        }
        let mut cursor = None;
        let mut traversed = Vec::new();
        let started = std::time::Instant::now();
        let mut statements = 0;
        loop {
            let page = fixture
                .repo()
                .list_workflow_dispatch_page(&range, cursor, std::num::NonZeroU32::new(64).unwrap())
                .await?;
            statements += 1;
            traversed.extend(page.candidates.iter().map(WorkflowDispatchRow::position));
            cursor = page.last_examined;
            if page.exhausted {
                break;
            }
        }
        assert_eq!(
            traversed, positions[index],
            "all ties survive complete traversal"
        );
        report.push_str(&format!(
            "\nQueue mode {index} complete pass: {} returned rows, {statements} page SQL statements, {:.3} seconds. Storage-engine rows examined are not measured by EXPLAIN.\n",
            traversed.len(),
            started.elapsed().as_secs_f64(),
        ));
    }
    let home = ShardId(0);
    let home_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM workflow_dispatch WHERE shard_id=$1")
            .bind(DsqlRunRepository::shard_id_to_uuid(home))
            .fetch_one(&fixture.pool)
            .await?;
    for continued in [false, true] {
        // Both SQL fragments are static; fixture coordinates stay bound.
        let mut explain = sqlx::QueryBuilder::<sqlx::Postgres>::new("EXPLAIN ");
        explain.push(super::workflow_dispatch::home_page_sql(continued));
        let mut query = explain
            .build_query_as::<(String,)>()
            .bind(DsqlRunRepository::shard_id_to_uuid(home))
            .bind(64i64);
        if continued {
            query = query.bind(positions[0][0].run_key.0);
        }
        let plan = query.fetch_all(&fixture.pool).await?;
        report.push_str(&format!(
            "\nHome continuation={continued}, matching rows={home_count}, selectivity={:.6}:\n",
            home_count as f64 / total as f64
        ));
        for (line,) in plan {
            report.push_str(&line);
            report.push('\n');
        }
    }
    if let Some(path) = std::env::var_os("TOKEIRA_WORKFLOW_DISPATCH_PLAN_OUTPUT") {
        std::fs::write(path, report)?;
    }
    fixture.store.shutdown().await?;
    fixture.pool.close().await;
    Ok(())
}

// The pre-change tuple predicate is retained only as a live plan negative control.
fn legacy_queue_page_query(
    range: &crate::WorkflowDiscoveryRange,
    after: Option<crate::WorkflowDispatchPosition>,
) -> sqlx::QueryBuilder<sqlx::Postgres> {
    let mut query = sqlx::QueryBuilder::new(super::workflow_dispatch::SELECT_ROW);
    let [queue, deployment, build] = range.lookup_keys();
    let (mode, _, _) = range.routing.coordinates();
    query
        .push(" WHERE queue_namespace=")
        .push_bind(range.namespace_id.0)
        .push(" AND queue_key=")
        .push_bind(queue)
        .push(" AND routing_mode=")
        .push_bind(mode)
        .push(" AND deployment_key=")
        .push_bind(deployment)
        .push(" AND build_key=")
        .push_bind(build)
        .push(" AND sticky=false");
    if let Some(after) = after {
        query
            .push(" AND (priority_key, scheduled_at, run_key) > (")
            .push_bind(after.priority_key)
            .push(",")
            .push_bind(after.scheduled_at)
            .push(",")
            .push_bind(after.run_key.0)
            .push(")");
    }
    query.push(" ORDER BY priority_key, scheduled_at, run_key LIMIT 64");
    query
}

impl Fixture {
    pub(super) async fn connect(shard_count: u32) -> Result<Self> {
        let url = std::env::var("TOKEIRA_DSQL_TEST_DATABASE_URL")?;
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
                shard_count,
                conflict_policy: CurrentExecutionConflictPolicy::Reject,
                ..DsqlPoolConfig::default()
            },
        )
        .await?;
        store.migration_runner().apply(&pool).await?;
        let jobs = sqlx::query_as::<_, (String, String)>(
            "SELECT job_id, status FROM sys.jobs WHERE job_type = 'INDEX_BUILD'",
        )
        .fetch_all(&pool)
        .await?;
        for (job, status) in jobs {
            if status == "submitted" || status == "processing" {
                anyhow::ensure!(
                    sqlx::query_scalar::<_, bool>("CALL sys.wait_for_job($1)")
                        .bind(job)
                        .fetch_one(&pool)
                        .await?,
                    "index build did not complete"
                );
            } else {
                anyhow::ensure!(status == "completed", "index build failed: {status}");
            }
        }
        Ok(Self {
            pool,
            store,
            repair_home: ShardId(0),
            shard_count,
        })
    }
}

#[async_trait]
impl Backend for Fixture {
    async fn sticky_seed(&self, rows: &[(ShardId, WorkflowState)]) -> Result<()> {
        let rows = rows
            .iter()
            .map(|(home, state)| {
                let row = crate::derive_workflow_dispatch(state, *home)
                    .expect("sticky traversal fixture has pending normal intent");
                row.validate()?;
                let priority = row
                    .priority
                    .as_ref()
                    .map(codec::encode_priority)
                    .transpose()?;
                Ok((row, priority))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut query = sqlx::QueryBuilder::new(
            "INSERT INTO workflow_dispatch (run_key, shard_id, queue_namespace, queue_name,
             normal_queue_name, sticky, routing_mode, deployment, build_id, queue_key,
             deployment_key, build_key, logical_seq, scheduled_at, priority_key, priority_data,
             sticky_worker, schedule_to_start_deadline) ",
        );
        query.push_values(&rows, |mut values, (row, priority)| {
            let [queue, deployment_key, build_key] = row.lookup_keys();
            let (mode, deployment, build) = row.routing.coordinates();
            values
                .push_bind(row.incarnation.run_key.0)
                .push_bind(DsqlRunRepository::shard_id_to_uuid(row.execution_home))
                .push_bind(row.namespace_id.0)
                .push_bind(&row.queue_name.0)
                .push_bind(&row.normal_queue_name.0)
                .push_bind(row.sticky)
                .push_bind(mode)
                .push_bind(deployment)
                .push_bind(build)
                .push_bind(queue)
                .push_bind(deployment_key)
                .push_bind(build_key)
                .push_bind(i64::try_from(row.incarnation.logical_seq.0).unwrap())
                .push_bind(row.scheduled_at)
                .push_bind(row.priority_key)
                .push_bind(priority)
                .push_bind(row.sticky_worker.as_ref().map(|worker| worker.0.as_str()))
                .push_bind(row.schedule_to_start_deadline);
        });
        let mut tx = self.pool.begin().await?;
        query.build().execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn sticky_remove(&self, keys: &[RunKey]) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM workflow_dispatch WHERE run_key=ANY($1)")
            .bind(keys.iter().map(|key| key.0).collect::<Vec<_>>())
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    fn repo(&self) -> &dyn RunRepository {
        self.store.run_repository()
    }

    async fn row(&self, key: RunKey) -> Result<Option<WorkflowDispatchRow>> {
        let mut query =
            sqlx::QueryBuilder::<sqlx::Postgres>::new(super::workflow_dispatch::SELECT_ROW);
        query.push(" WHERE run_key=").push_bind(key.0);
        query
            .build()
            .fetch_optional(&self.pool)
            .await?
            .map(super::workflow_dispatch::decode)
            .transpose()
    }

    async fn seed_stale_row(&self, state: &WorkflowState) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        super::workflow_dispatch::maintain(&mut tx, state, self.repair_home).await?;
        tx.commit().await?;
        Ok(())
    }
}

#[test]
fn queue_home_delivery_live_sticky_pages() {
    if std::env::var_os("TOKEIRA_DSQL_TEST_DATABASE_URL").is_none() {
        return;
    }
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let fixture = runtime.block_on(Fixture::connect(8)).unwrap();
    crate::workflow_dispatch_tests::run_sticky_page_cases(&fixture, &runtime);
    runtime.block_on(fixture.store.shutdown()).unwrap();
}

#[test]
fn workflow_dispatch_live_atomic_reference_traces() {
    if std::env::var_os("TOKEIRA_DSQL_TEST_DATABASE_URL").is_none() {
        return;
    }
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let fixture = runtime.block_on(Fixture::connect(1)).unwrap();
    run_atomic_cases(&fixture, &runtime);
    runtime.block_on(fixture.store.shutdown()).unwrap();
    runtime.block_on(fixture.pool.close());
}

#[tokio::test]
async fn workflow_dispatch_live_ordered_pages() {
    if std::env::var_os("TOKEIRA_DSQL_TEST_DATABASE_URL").is_none() {
        return;
    }
    let fixture = Fixture::connect(1).await.unwrap();
    ordered_pages(&fixture).await;
    routing_and_home_pages(&fixture).await;
    reset_materialization(&fixture).await;
    speculative_legacy_delivery(&fixture).await;
    digest_collision(&fixture).await.unwrap();
    fixture.store.shutdown().await.unwrap();
    fixture.pool.close().await;
}

#[tokio::test]
async fn workflow_dispatch_live_reset_uses_execution_home() {
    if std::env::var_os("TOKEIRA_DSQL_TEST_DATABASE_URL").is_none() {
        return;
    }
    let fixture = Fixture::connect(8).await.unwrap();
    reset_uses_execution_home(&fixture).await.unwrap();
    fixture.store.shutdown().await.unwrap();
    fixture.pool.close().await;
}

async fn reset_uses_execution_home(fixture: &Fixture) -> Result<()> {
    let mut transition =
        crate::memory::projection_accumulator_tests::fresh_transition(RunKey::new());
    let template = transition.next_state.clone();
    let home = tokeira_types::execution_home_bundle(
        template.namespace_id.0.as_bytes(),
        template.workflow_id.0.as_bytes(),
        8,
    );
    let (run_id, successor_key) = (1..=256)
        .find_map(|value| {
            let run_id = RunId(Uuid::from_u128(value));
            let key = RunKey::derive(template.namespace_id, &template.workflow_id, run_id);
            let run_shard = DsqlRunRepository::shard_for_run_key_with_count(key, 8)
                .expect("fixture shard count is nonzero");
            (run_shard != home).then_some((run_id, key))
        })
        .expect("fixture must distinguish execution home from run-hash placement");
    let mut history = crate::memory::projection_accumulator_tests::reset_history(&template);
    history.truncate(4);
    let fire_at = template.started_at + Duration::days(1);
    for kind in [
        HistoryEventKind::TimerStarted {
            workflow_task_completed_event_id: 4,
            timer_id: "reset-timer".into(),
            fire_at,
        },
        HistoryEventKind::WorkflowTaskScheduled {
            logical_seq: LogicalTaskSeq(2),
            task_queue: template.task_queue.clone(),
            workflow_task_timeout: template.workflow_task_timeout,
            attempt: 1,
        },
        HistoryEventKind::WorkflowTaskStarted {
            logical_seq: LogicalTaskSeq(2),
            scheduled_event_id: 6,
            attempt: 1,
            identity: WorkerIdentity("worker".into()),
            request_id: "reset-task".into(),
            history_size_bytes: 0,
            suggest_continue_as_new: false,
            suggest_continue_as_new_reasons: Vec::new(),
            target_worker_deployment_version_changed: false,
            target_version_changed_enabled: false,
            target_deployment_version: None,
        },
    ] {
        history.push(HistoryEvent {
            event_id: history.len() as i64 + 1,
            happened_at: template.started_at,
            kind,
        });
    }
    transition.next_state = BasicKernel.replay_history_prefix(
        ReplayContext {
            run_key: template.run_key,
            namespace_id: template.namespace_id,
            workflow_id: template.workflow_id.clone(),
            run_id: template.run_id,
            deployment: template.deployment,
            build_id: template.build_id,
            parent_run_key: template.parent_run_key,
            parent_workflow_id: template.parent_workflow_id,
            first_run_started_at: template.first_run_started_at,
        },
        &history,
    )?;
    transition.next_state.transition_seq = template.transition_seq;
    transition.timer_ops = transition
        .next_state
        .timers
        .values()
        .cloned()
        .map(TimerOp::Upsert)
        .collect();
    transition.event_principals = vec![None; history.len()].into();
    transition.history_events = history.into();
    let source = crate::memory::projection_accumulator_tests::applied(
        crate::workflow_dispatch_tests::commit(fixture, transition).await?,
    );
    fixture
        .repo()
        .materialize_reset_successor(
            source.run_key,
            7,
            run_id,
            Some(source.run_key),
            WriteFence::Unfenced,
        )
        .await?;
    // Observe materialization before another commit can mask wrong placement.
    let hot_shard =
        sqlx::query_scalar::<_, Uuid>("SELECT shard_id FROM workflow_hot WHERE run_key=$1")
            .bind(successor_key.0)
            .fetch_one(&fixture.pool)
            .await?;
    let timer_shards = sqlx::query_as::<_, (Uuid, String)>(
        "SELECT shard_id, timer_id FROM timer_bucket WHERE run_key=$1",
    )
    .bind(successor_key.0)
    .fetch_all(&fixture.pool)
    .await?;
    let expected_shard = DsqlRunRepository::shard_id_to_uuid(home);
    assert_eq!(hot_shard, expected_shard);
    assert_eq!(timer_shards, vec![(expected_shard, "reset-timer".into())]);
    let dispatch = fixture
        .row(successor_key)
        .await?
        .expect("unstarted reset task is dispatchable");
    assert_eq!(dispatch.execution_home, home);
    assert_eq!(dispatch.incarnation.logical_seq, LogicalTaskSeq(2));
    Ok(())
}

async fn digest_collision(fixture: &Fixture) -> Result<()> {
    let mut wanted = crate::memory::projection_accumulator_tests::fresh_transition(RunKey::new());
    wanted.next_state.priority = Some(tokeira_kernel::Priority {
        priority_key: 5,
        fairness_key: String::new(),
        fairness_weight: 1.0,
    });
    let range = crate::WorkflowDiscoveryRange {
        namespace_id: wanted.next_state.namespace_id,
        queue_name: wanted.next_state.task_queue.clone(),
        routing: crate::WorkflowDispatchRouting::Live,
    };
    let mut collision = wanted.clone();
    collision.next_state.run_key = RunKey::new();
    collision.next_state.workflow_id.0 = "collision".into();
    collision.next_state.task_queue.0 = "different-queue".into();
    collision
        .next_state
        .priority
        .as_mut()
        .expect("fixture has priority")
        .priority_key = 1;
    let collision_key = collision.next_state.run_key;
    crate::workflow_dispatch_tests::commit(fixture, wanted).await?;
    crate::workflow_dispatch_tests::commit(fixture, collision).await?;
    // Force a lookup collision while keeping authoritative raw coordinates.
    sqlx::query("UPDATE workflow_dispatch SET queue_key=$1 WHERE run_key=$2")
        .bind(&range.lookup_keys()[0])
        .bind(collision_key.0)
        .execute(&fixture.pool)
        .await?;
    let limit = std::num::NonZeroU32::new(1).expect("one is nonzero");
    let page = fixture
        .repo()
        .list_workflow_dispatch_page(&range, None, limit)
        .await?;
    assert!(!range.matches(&page.candidates[0]));
    let next = fixture
        .repo()
        .list_workflow_dispatch_page(&range, page.last_examined, limit)
        .await?;
    assert!(range.matches(&next.candidates[0]));
    Ok(())
}

#[test]
fn workflow_dispatch_live_generated_ordered_traversal() {
    if std::env::var_os("TOKEIRA_DSQL_TEST_DATABASE_URL").is_none() {
        return;
    }
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let fixture = runtime.block_on(Fixture::connect(8)).unwrap();
    crate::workflow_dispatch_tests::run_page_cases(&fixture, &runtime);
    runtime.block_on(fixture.store.shutdown()).unwrap();
    runtime.block_on(fixture.pool.close());
}

#[test]
fn workflow_dispatch_live_generated_sticky_recovery() {
    if std::env::var_os("TOKEIRA_DSQL_TEST_DATABASE_URL").is_none() {
        return;
    }
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let fixture = runtime.block_on(Fixture::connect(8)).unwrap();
    crate::workflow_dispatch_tests::run_sticky_recovery_cases(&fixture, &runtime);
    runtime.block_on(fixture.store.shutdown()).unwrap();
    runtime.block_on(fixture.pool.close());
}

#[async_trait]
impl crate::workflow_dispatch_tests::RepairBackend for Fixture {
    fn repair_home(&self) -> ShardId {
        self.repair_home
    }
    fn repair_shard_count(&self) -> u32 {
        self.shard_count
    }
    async fn remove_row(&self, key: RunKey) -> Result<()> {
        sqlx::query("DELETE FROM workflow_dispatch WHERE run_key=$1")
            .bind(key.0)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
    async fn legacy_recovery_flag(&self, key: RunKey) -> Result<()> {
        sqlx::query("UPDATE workflow_hot SET recovery_needed=NULL WHERE run_key=$1")
            .bind(key.0)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
    async fn authority(&self, keys: &[RunKey]) -> Result<Vec<u8>> {
        let keys: Vec<_> = keys.iter().map(|key| key.0).collect();
        let hot = sqlx::query_as::<_, (Uuid, i64, Vec<u8>, Option<bool>, OffsetDateTime)>(
            "SELECT run_key, transition_seq, state_data, recovery_needed, updated_at FROM workflow_hot WHERE run_key=ANY($1) ORDER BY run_key")
            .bind(&keys).fetch_all(&self.pool).await?;
        let projection = sqlx::query_as::<_, (Uuid, i64, Vec<u8>, Vec<u8>)>(
            "SELECT run_key, transition_seq, context_data, ops_data FROM projection_log WHERE run_key=ANY($1) ORDER BY run_key, transition_seq")
            .bind(&keys).fetch_all(&self.pool).await?;
        let mut history = Vec::new();
        for key in &keys {
            history.push(self.repo().read_history(RunKey(*key), 0, 100).await?);
        }
        Ok(format!("{hot:?}{projection:?}{history:?}").into_bytes())
    }
    async fn finish_repair_case(&self, keys: &[RunKey]) -> Result<()> {
        let keys: Vec<_> = keys.iter().map(|key| key.0).collect();
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM workflow_dispatch WHERE run_key=ANY($1)")
            .bind(&keys)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM workflow_hot WHERE run_key=ANY($1)")
            .bind(&keys)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }
}

#[test]
fn workflow_dispatch_live_generated_complete_repair() {
    if std::env::var_os("TOKEIRA_DSQL_TEST_DATABASE_URL").is_none() {
        return;
    }
    let runtime = tokio::runtime::Runtime::new().unwrap();
    // Other shared-cluster fixtures use homes below 256, including the
    // lease-fence corpus. Keep complete unfiltered walks on a reserved home.
    let mut fixture = runtime.block_on(Fixture::connect(1024)).unwrap();
    fixture.repair_home = ShardId(1023);
    crate::workflow_dispatch_tests::run_repair_cases(&fixture, &runtime);
    runtime.block_on(fixture.store.shutdown()).unwrap();
    runtime.block_on(fixture.pool.close());
}

#[tokio::test]
async fn workflow_dispatch_live_repair_decode_and_encoding_failures_preserve_authority()
-> Result<()> {
    if std::env::var_os("TOKEIRA_DSQL_TEST_DATABASE_URL").is_none() {
        return Ok(());
    }
    let fixture = Fixture::connect(1).await?;
    let transition = crate::memory::projection_accumulator_tests::fresh_transition(RunKey::new());
    let key = transition.next_state.run_key;
    let mut state = crate::memory::projection_accumulator_tests::applied(
        crate::workflow_dispatch_tests::commit(&fixture, transition).await?,
    );
    let expected_row = fixture.row(key).await?;
    assert!(
        fixture
            .repo()
            .reconcile_workflow_dispatch_run(ShardId(1), key, WriteFence::Unfenced)
            .await
            .is_err()
    );
    let original =
        sqlx::query_scalar::<_, Vec<u8>>("SELECT state_data FROM workflow_hot WHERE run_key=$1")
            .bind(key.0)
            .fetch_one(&fixture.pool)
            .await?;
    state.pending_workflow_task.as_mut().unwrap().logical_seq = LogicalTaskSeq(u64::MAX);
    for corrupt in [vec![1, 2, 3], codec::encode_workflow_state(&state)?] {
        sqlx::query("UPDATE workflow_hot SET state_data=$2 WHERE run_key=$1")
            .bind(key.0)
            .bind(corrupt)
            .execute(&fixture.pool)
            .await?;
        let before =
            crate::workflow_dispatch_tests::RepairBackend::authority(&fixture, &[key]).await?;
        assert!(
            fixture
                .repo()
                .reconcile_workflow_dispatch_run(ShardId(0), key, WriteFence::Unfenced)
                .await
                .is_err()
        );
        assert_eq!(
            crate::workflow_dispatch_tests::RepairBackend::authority(&fixture, &[key]).await?,
            before
        );
        assert_eq!(fixture.row(key).await?, expected_row);
    }
    sqlx::query("UPDATE workflow_hot SET state_data=$2 WHERE run_key=$1")
        .bind(key.0)
        .bind(original)
        .execute(&fixture.pool)
        .await?;
    crate::workflow_dispatch_tests::RepairBackend::finish_repair_case(&fixture, &[key]).await?;
    fixture.store.shutdown().await?;
    fixture.pool.close().await;
    Ok(())
}

#[async_trait]
impl crate::workflow_dispatch_tests::DisposalBackend for Fixture {
    async fn disposal_authority(&self, keys: &[RunKey]) -> Result<Vec<u8>> {
        crate::workflow_dispatch_tests::RepairBackend::authority(self, keys).await
    }
    async fn backlog_bytes(&self, queue: &QueueKey) -> Result<Vec<Vec<u8>>> {
        // All persisted columns are compared, not merely decoded payloads.
        let rows = sqlx::query_as::<_, (Uuid, i32, i16, Option<String>, Option<String>, i16, i64, i64, Uuid, Vec<u8>, OffsetDateTime)>(
            "SELECT key, partition_id, task_kind, deployment, build_id, priority_key, fair_pass, insertion_tie, run_key, payload_data, scheduled_at FROM dispatch_backlog WHERE queue_namespace=$1 AND queue_name=$2 AND task_kind=$3 ORDER BY key")
            .bind(queue.namespace_id.0).bind(&queue.task_queue.0).bind(queue.task_kind.to_db_smallint()).fetch_all(&self.pool).await?;
        Ok(rows
            .iter()
            .map(|row| format!("{row:?}").into_bytes())
            .collect())
    }
}

#[test]
fn workflow_dispatch_live_generated_legacy_disposal() {
    if std::env::var_os("TOKEIRA_DSQL_TEST_DATABASE_URL").is_none() {
        return;
    }
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let fixture = runtime.block_on(Fixture::connect(1)).unwrap();
    crate::workflow_dispatch_tests::run_disposal_cases(&fixture, &runtime);
    runtime.block_on(fixture.store.shutdown()).unwrap();
    runtime.block_on(fixture.pool.close());
}

#[tokio::test]
async fn workflow_dispatch_live_disposal_rolls_back_cancelled_page_and_keeps_bounds() -> Result<()>
{
    if std::env::var_os("TOKEIRA_DSQL_TEST_DATABASE_URL").is_none() {
        return Ok(());
    }
    let fixture = Arc::new(Fixture::connect(1).await?);
    let state =
        crate::memory::projection_accumulator_tests::fresh_transition(RunKey::new()).next_state;
    let queue = QueueKey {
        namespace_id: tokeira_types::NamespaceId::new(),
        task_queue: state.task_queue,
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    };
    let entries = (0..130)
        .map(|_| BacklogEntry {
            run_key: RunKey::new(),
            queue: queue.clone(),
            payload: BacklogPayload::Workflow {
                logical_seq: LogicalTaskSeq::ONE,
            },
            priority: None,
            order: DeliveryOrder::default(),
            scheduled_at: state.started_at,
        })
        .collect();
    fixture.repo().persist_to_backlog(entries).await?;
    let before =
        crate::workflow_dispatch_tests::DisposalBackend::backlog_bytes(fixture.as_ref(), &queue)
            .await?;
    // Start immediately before our first key, so the cancelled page necessarily
    // issued a delete for this fixture even after another suite left backlog.
    let first: Uuid = sqlx::query_scalar("SELECT key FROM dispatch_backlog WHERE queue_namespace=$1 AND queue_name=$2 AND task_kind=$3 ORDER BY key LIMIT 1")
        .bind(queue.namespace_id.0).bind(&queue.task_queue.0)
        .bind(TaskKind::Workflow.to_db_smallint()).fetch_one(&fixture.pool).await?;
    let after_first = first.as_u128().checked_sub(1).map(Uuid::from_u128);
    let (entered, resume) = super::pause_point::arm(super::pause_point::PauseKey::Disposal);
    let writing = fixture.clone();
    let page = tokio::spawn(async move {
        writing
            .repo()
            .dispose_workflow_backlog_page(after_first)
            .await
    });
    entered.await?;
    page.abort();
    assert!(page.await.unwrap_err().is_cancelled());
    drop(resume);
    assert_eq!(
        crate::workflow_dispatch_tests::DisposalBackend::backlog_bytes(fixture.as_ref(), &queue)
            .await?,
        before
    );
    let mut after = None;
    loop {
        let page = fixture.repo().dispose_workflow_backlog_page(after).await?;
        assert!(page.examined <= crate::LEGACY_WORKFLOW_KEYS && page.deleted <= page.examined);
        if page.exhausted {
            break;
        }
        assert!(page.last_examined > after);
        after = page.last_examined;
    }
    assert!(
        crate::workflow_dispatch_tests::DisposalBackend::backlog_bytes(fixture.as_ref(), &queue)
            .await?
            .is_empty()
    );
    fixture.store.shutdown().await?;
    fixture.pool.close().await;
    Ok(())
}

#[tokio::test]
async fn workflow_dispatch_live_disposal_bounds_activity_prefix_and_passes_corruption() -> Result<()>
{
    if std::env::var_os("TOKEIRA_DSQL_TEST_DATABASE_URL").is_none() {
        return Ok(());
    }
    let fixture = Fixture::connect(1).await?;
    let transition = crate::memory::projection_accumulator_tests::fresh_transition(RunKey::new());
    let corrupt_run = transition.next_state.run_key;
    crate::workflow_dispatch_tests::commit(&fixture, transition).await?;
    let original: Vec<u8> =
        sqlx::query_scalar("SELECT state_data FROM workflow_hot WHERE run_key=$1")
            .bind(corrupt_run.0)
            .fetch_one(&fixture.pool)
            .await?;
    sqlx::query("UPDATE workflow_hot SET state_data=$2 WHERE run_key=$1")
        .bind(corrupt_run.0)
        .bind(vec![1u8, 2, 3])
        .execute(&fixture.pool)
        .await?;
    let base = Uuid::new_v4().as_u128() & !0xffff;
    let namespace = Uuid::new_v4();
    let mut tx = fixture.pool.begin().await?;
    for index in 0..66u128 {
        let kind = if index < 64 {
            TaskKind::Activity
        } else {
            TaskKind::Workflow
        };
        let run = if index == 64 {
            corrupt_run.0
        } else {
            Uuid::new_v4()
        };
        sqlx::query("INSERT INTO dispatch_backlog (key,partition_id,queue_namespace,queue_name,task_kind,priority_key,fair_pass,insertion_tie,run_key,payload_data,scheduled_at) VALUES ($1,0,$2,'disposal-bounds',$3,0,0,0,$4,$5,$6)")
            .bind(Uuid::from_u128(base + index)).bind(namespace).bind(kind.to_db_smallint())
            .bind(run).bind(Vec::<u8>::new()).bind(OffsetDateTime::now_utc()).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    let page = fixture
        .repo()
        .dispose_workflow_backlog_page(Some(Uuid::from_u128(base - 1)))
        .await?;
    assert_eq!(page.examined, 64);
    assert_eq!((page.deleted, page.deferred), (0, 0));
    assert_eq!(page.last_examined, Some(Uuid::from_u128(base + 63)));
    let next = fixture
        .repo()
        .dispose_workflow_backlog_page(page.last_examined)
        .await?;
    assert!(next.examined <= 64 && next.deferred >= 1);
    let remaining: Vec<Uuid> = sqlx::query_scalar(
        "SELECT key FROM dispatch_backlog WHERE queue_namespace=$1 ORDER BY key",
    )
    .bind(namespace)
    .fetch_all(&fixture.pool)
    .await?;
    assert!(remaining.contains(&Uuid::from_u128(base + 64)));
    assert!(
        !remaining.contains(&Uuid::from_u128(base + 65)),
        "corruption must not stop the following covered key"
    );
    let mut explain = sqlx::QueryBuilder::<sqlx::Postgres>::new("EXPLAIN ");
    explain.push(super::dispatch::DISPOSAL_NEXT_KEYS_SQL);
    let plan = explain
        .build_query_as::<(String,)>()
        .bind(Uuid::from_u128(base + 63))
        .bind(64i64)
        .fetch_all(&fixture.pool)
        .await?;
    assert!(
        plan.iter()
            .any(|(line,)| line.contains("Index Cond:") && line.contains("key >")),
        "continuation must seek: {plan:?}"
    );
    assert!(
        !plan
            .iter()
            .any(|(line,)| line.contains("Filter:") && line.contains("task_kind"))
    );
    if let Some(path) = std::env::var_os("TOKEIRA_WORKFLOW_DISPOSAL_PLAN_OUTPUT") {
        std::fs::write(
            path,
            plan.into_iter()
                .map(|(line,)| line)
                .collect::<Vec<_>>()
                .join("\n"),
        )?;
    }
    sqlx::query("UPDATE workflow_hot SET state_data=$2 WHERE run_key=$1")
        .bind(corrupt_run.0)
        .bind(original)
        .execute(&fixture.pool)
        .await?;
    sqlx::query("DELETE FROM dispatch_backlog WHERE queue_namespace=$1")
        .bind(namespace)
        .execute(&fixture.pool)
        .await?;
    fixture.store.shutdown().await?;
    fixture.pool.close().await;
    Ok(())
}
