//! Opt-in contracts on an ephemeral Aurora DSQL cluster. SQLx statement events
//! observe executed queries; per-run channels coordinate snapshot races without
//! delays, production switches, or changes to the connection director.

use std::{
    collections::BTreeMap,
    path::Path,
    sync::{LazyLock, Mutex},
};

use proptest::{
    prelude::*,
    test_runner::{TestCaseResult, TestRunner},
};
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokeira_kernel::{
    RequestDedupeOp,
    limits::{RunLimit, RunLimitExceeded},
};
use tokeira_types::SearchAttrValue;
use tokio::{runtime::Runtime, sync::oneshot};
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{Layer, layer::Context as LayerContext, prelude::*};

use super::*;
use crate::{
    api::ProjectionContext,
    dsql::{DsqlPoolConfig, DsqlStore, MigrationConfig, ReservoirConfig},
    memory::projection_accumulator_tests::{
        applied, commit as commit_once, following, fresh_transition, image_versions, legacy_step,
        observations, observe, reset_history, state_limit,
    },
    projection_accumulator_oracle::{
        legacy_codec, workflow_projection_context_with_previous as baseline_image,
    },
};

/// Test-only points bracketing the repeatable-read snapshot's first read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum LoadStage {
    BeforeSnapshot,
    AfterHotRead,
}

struct Pause {
    stage: LoadStage,
    reached: oneshot::Sender<()>,
    resume: oneshot::Receiver<()>,
}

static PAUSES: LazyLock<Mutex<BTreeMap<RunKey, Pause>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));

async fn commit(
    repo: &dyn RunRepository,
    transition: Transition,
    bundle: bool,
    epoch: ShardEpoch,
) -> Result<CommitResult> {
    // DSQL can abort even serial test writes during asynchronous catalog work.
    // Exercise the existing caller retry contract only for the repository's
    // normalized transaction abort; stale sequence/fence outcomes remain visible.
    for attempt in 0..10 {
        let result = commit_once(repo, transition.clone(), bundle, epoch).await?;
        if attempt < 9
            && matches!(&result, CommitResult::Conflict { reason } if reason == "DSQL serialization conflict")
        {
            continue;
        }
        return Ok(result);
    }
    unreachable!("the final attempt always returns its outcome")
}

/// Suspend only the selected test-owned run, releasing the map before awaiting.
pub(super) async fn pause(key: RunKey, stage: LoadStage) {
    let pause = {
        let mut pauses = PAUSES.lock().unwrap();
        if pauses.get(&key).is_some_and(|pause| pause.stage == stage) {
            pauses.remove(&key)
        } else {
            None
        }
    };
    if let Some(pause) = pause {
        pause.reached.send(()).unwrap();
        pause.resume.await.unwrap();
    }
}

fn pause_at(key: RunKey, stage: LoadStage) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
    let (reached, wait) = oneshot::channel();
    let (resume, release) = oneshot::channel();
    assert!(
        PAUSES
            .lock()
            .unwrap()
            .insert(
                key,
                Pause {
                    stage,
                    reached,
                    resume: release
                }
            )
            .is_none()
    );
    (wait, resume)
}

#[derive(Clone, Default)]
struct Statements(Arc<Mutex<Vec<String>>>);

#[derive(Default)]
struct Statement {
    sql: String,
    summary: String,
}

impl Visit for Statement {
    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "db.statement" => self.sql = value.to_owned(),
            "summary" => self.summary = value.to_owned(),
            _ => {}
        }
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.record_str(field, &format!("{value:?}"));
    }
}

impl<S: Subscriber> Layer<S> for Statements {
    fn on_event(&self, event: &Event<'_>, _: LayerContext<'_, S>) {
        if event.metadata().target() == "sqlx::query" {
            let mut statement = Statement::default();
            event.record(&mut statement);
            self.0.lock().unwrap().push(if statement.sql.is_empty() {
                statement.summary
            } else {
                statement.sql
            });
        }
    }
}

impl Statements {
    fn take(&self) -> Vec<String> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }

    fn assert_no_projection_reads(&self) {
        let statements = self.take();
        assert!(
            statements.iter().any(|sql| sql.contains("workflow_hot")),
            "statement capture must observe actual SQL"
        );
        assert!(
            !statements
                .iter()
                .any(|sql| sql.to_ascii_uppercase().contains("SELECT")
                    && sql.contains("projection_log")),
            "projection SELECT: {statements:?}"
        );
    }
}

struct Fixture {
    pool: PgPool,
    store: DsqlStore,
    statements: Statements,
    owned: Mutex<BTreeMap<RunKey, (NamespaceId, WorkflowId)>>,
}

impl Fixture {
    async fn connect() -> Result<Self> {
        let url = std::env::var("TOKEIRA_DSQL_TEST_DATABASE_URL")
            .context("set the ephemeral Aurora DSQL test URL")?;
        let statements = Statements::default();
        tracing::subscriber::set_global_default(
            tracing_subscriber::registry().with(statements.clone()),
        )?;
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .min_connections(8)
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
                shard_count: 1,
                conflict_policy: CurrentExecutionConflictPolicy::Reject,
                ..DsqlPoolConfig::default()
            },
        )
        .await?;
        // ASYNC index completion can change the catalog between migration
        // transactions on a fresh DSQL cluster. Resume the idempotent runner
        // only for its explicit schema/serialization conflict, never DDL errors.
        for attempt in 0..20 {
            match store.migration_runner().apply(&pool).await {
                Ok(_) => break,
                Err(error)
                    if attempt < 19
                        && error
                            .downcast_ref::<sqlx::Error>()
                            .and_then(|error| error.as_database_error())
                            .and_then(|error| error.code())
                            .is_some_and(|code| matches!(code.as_ref(), "OC001" | "40001")) => {}
                Err(error) => return Err(error),
            }
        }
        // The pool-based fixture migrator submits ASYNC indexes without the
        // production guarded migrator's completion barrier. Wait for those jobs
        // before properties begin, so catalog churn cannot abort fixture writes.
        let jobs = sqlx::query_as::<_, (String, String)>(
            "SELECT job_id, status FROM sys.jobs WHERE job_type = 'INDEX_BUILD'",
        )
        .fetch_all(&pool)
        .await?;
        for (job_id, status) in jobs {
            if status == "submitted" || status == "processing" {
                let completed = sqlx::query_scalar::<_, bool>("CALL sys.wait_for_job($1)")
                    .bind(job_id)
                    .fetch_one(&pool)
                    .await?;
                anyhow::ensure!(completed, "fixture index build did not complete");
            } else {
                anyhow::ensure!(
                    status == "completed",
                    "fixture index build failed: {status}"
                );
            }
        }
        Ok(Self {
            pool,
            store,
            statements,
            owned: Mutex::new(BTreeMap::new()),
        })
    }

    fn repo(&self) -> &DsqlRunRepository {
        self.store.run_repository()
    }

    fn fresh(&self) -> Transition {
        let transition = fresh_transition(RunKey::new());
        self.track(&transition.next_state);
        transition
    }

    fn track(&self, state: &WorkflowState) {
        self.owned.lock().unwrap().insert(
            state.run_key,
            (state.namespace_id, state.workflow_id.clone()),
        );
    }

    fn recreated(&self) -> DsqlRunRepository {
        DsqlRunRepository::new_with_acquirer(
            self.repo().director.clone(),
            1,
            self.repo().projection_partition_count,
            CurrentExecutionConflictPolicy::Reject,
            Duration::seconds(30),
        )
        .unwrap()
    }

    async fn hot(&self, key: RunKey) -> (Vec<u8>, i64) {
        sqlx::query_as("SELECT state_data, history_size_bytes FROM workflow_hot WHERE run_key = $1")
            .bind(key.0)
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }

    async fn image(&self, key: RunKey) -> Option<ProjectionContext> {
        sqlx::query_scalar::<_, Vec<u8>>("SELECT context_data FROM projection_log WHERE run_key = $1 ORDER BY transition_seq DESC LIMIT 1").bind(key.0).fetch_optional(&self.pool).await.unwrap().map(|bytes| codec::decode_projection_context(&bytes).unwrap())
    }

    async fn replace(&self, state: &WorkflowState, stats: i64, image: Option<(i32, i64, Vec<u8>)>) {
        let mut tx = self.pool.begin().await.unwrap();
        sqlx::query("UPDATE workflow_hot SET state_data = $2, transition_seq = $3, history_size_bytes = $4 WHERE run_key = $1")
            .bind(state.run_key.0).bind(codec::encode_workflow_state(state).unwrap()).bind(state.transition_seq.0 as i64).bind(stats).execute(&mut *tx).await.unwrap();
        if let Some((partition, seq, bytes)) = image {
            sqlx::query("INSERT INTO projection_log (partition_id, fanout, run_key, transition_seq, context_data, ops_data) VALUES ($1, 1, $2, $3, $4, $5)")
                .bind(partition).bind(state.run_key.0).bind(seq).bind(bytes).bind(codec::LEGACY_EMPTY_PROJECTION_OPS_DATA).execute(&mut *tx).await.unwrap();
        }
        tx.commit().await.unwrap();
    }

    async fn prune(&self, key: RunKey) {
        sqlx::query("DELETE FROM projection_log WHERE run_key = $1")
            .bind(key.0)
            .execute(&self.pool)
            .await
            .unwrap();
    }

    async fn legacy_write(
        &self,
        key: RunKey,
        observation: &crate::memory::projection_accumulator_tests::Observation,
    ) {
        let (blob, stats) = self.hot(key).await;
        let previous = self.image(key).await;
        let state = codec::decode_workflow_state(key, &blob).unwrap();
        let (legacy, context) = legacy_step(&state, previous.as_ref(), observation, stats);
        self.replace(
            &legacy,
            stats,
            Some((
                0,
                legacy.transition_seq.0 as i64,
                codec::encode_projection_context(&context).unwrap(),
            )),
        )
        .await;
    }

    async fn cleanup(&self) {
        let owned = self.owned.lock().unwrap().clone();
        for (key, (namespace, workflow)) in owned {
            for statement in [
                "DELETE FROM projection_log WHERE run_key = $1",
                "DELETE FROM history_batch WHERE run_key = $1",
                "DELETE FROM request_dedupe WHERE run_key = $1",
                "DELETE FROM activity_dispatch WHERE run_key = $1",
                "DELETE FROM timer_bucket WHERE run_key = $1",
                "DELETE FROM workflow_hot WHERE run_key = $1",
            ] {
                sqlx::query(statement)
                    .bind(key.0)
                    .execute(&self.pool)
                    .await
                    .unwrap();
            }
            sqlx::query("DELETE FROM current_execution WHERE key = $1")
                .bind(DsqlRunRepository::current_execution_key(
                    namespace, &workflow,
                ))
                .execute(&self.pool)
                .await
                .unwrap();
        }
    }

    async fn durable(&self, key: RunKey) -> ((Vec<u8>, i64), Vec<(i64, Vec<u8>)>, Vec<i64>) {
        let hot = self.hot(key).await;
        let images = sqlx::query_as("SELECT transition_seq, context_data FROM projection_log WHERE run_key = $1 ORDER BY transition_seq, partition_id").bind(key.0).fetch_all(&self.pool).await.unwrap();
        let mut counts = Vec::new();
        for statement in [
            "SELECT count(*) FROM history_batch WHERE run_key = $1",
            "SELECT count(*) FROM request_dedupe WHERE run_key = $1",
            "SELECT count(*) FROM activity_dispatch WHERE run_key = $1",
            "SELECT count(*) FROM timer_bucket WHERE run_key = $1",
        ] {
            counts.push(
                sqlx::query_scalar(statement)
                    .bind(key.0)
                    .fetch_one(&self.pool)
                    .await
                    .unwrap(),
            );
        }
        (hot, images, counts)
    }
}

fn contract<S: Strategy>(
    strategy: S,
    case: impl Fn(&Runtime, &Fixture, S::Value) -> TestCaseResult,
) {
    let runtime = Runtime::new().unwrap();
    let fixture = runtime.block_on(Fixture::connect()).unwrap();
    let result = TestRunner::new(ProptestConfig {
        source_file: Some(file!()),
        ..ProptestConfig::with_cases(100)
    })
    .run(&strategy, |value| case(&runtime, &fixture, value));
    runtime.block_on(fixture.cleanup());
    result.unwrap();
}

// Feature: projection-accumulator, Property 1: no projection reads in commits
// Feature: projection-accumulator, Property 2: complete baseline image equivalence
// SQL commits preserve complete images without reading a preceding projection.
#[test]
#[ignore = "requires an ephemeral Aurora DSQL cluster"]
fn dsql_projection_accumulator_commit_images() {
    contract(observations(), |runtime, fixture, steps| {
        runtime.block_on(async {
            for bundle in [false, true] {
                let mut transition = fixture.fresh();
                let key = transition.next_state.run_key;
                let mut previous = None;
                for observation in &steps {
                    observe(&mut transition.next_state, observation);
                    let expected =
                        baseline_image(&transition.next_state, previous.as_ref(), 0).unwrap();
                    fixture.statements.take();
                    let state = applied(
                        commit(fixture.repo(), transition, bundle, ShardEpoch::ZERO)
                            .await
                            .unwrap(),
                    );
                    fixture.statements.assert_no_projection_reads();
                    fixture.statements.take();
                    prop_assert_eq!(
                        fixture.repo().load_run(key).await.unwrap(),
                        LoadedRun::Existing(state.clone())
                    );
                    fixture.statements.assert_no_projection_reads();
                    prop_assert_eq!(
                        state.used_worker_deployment_versions.as_ref(),
                        Some(&image_versions(&expected))
                    );
                    prop_assert_eq!(
                        codec::decode_workflow_state(key, &fixture.hot(key).await.0).unwrap(),
                        state.clone()
                    );
                    prop_assert_eq!(fixture.image(key).await, Some(expected.clone()));
                    previous = Some(expected);
                    transition = following(&state);
                }
                sqlx::query("UPDATE projection_log SET context_data = $2 WHERE run_key = $1")
                    .bind(key.0)
                    .bind(vec![255u8])
                    .execute(&fixture.pool)
                    .await
                    .unwrap();
                fixture.statements.take();
                let LoadedRun::Existing(state) = fixture.repo().load_run(key).await.unwrap() else {
                    panic!("ready")
                };
                applied(
                    commit(fixture.repo(), following(&state), bundle, ShardEpoch::ZERO)
                        .await
                        .unwrap(),
                );
                fixture.statements.assert_no_projection_reads();
            }
            Ok(())
        })
    });
}

// Feature: projection-accumulator, Property 4: legacy loads validate one non-durable snapshot across historical partitions.
#[test]
#[ignore = "requires an ephemeral Aurora DSQL cluster"]
fn dsql_projection_accumulator_legacy_seeds() {
    contract(
        (
            prop::collection::vec("[ab]{0,8}", 0..16),
            64i32..128,
            0u8..9,
        ),
        |runtime, fixture, (values, partition, defect)| {
            runtime.block_on(async {
                let mut state = applied(
                    commit(fixture.repo(), fixture.fresh(), false, ShardEpoch::ZERO)
                        .await
                        .unwrap(),
                );
                let key = state.run_key;
                fixture.prune(key).await;
                state.transition_seq = TransitionSeq(5);
                state.used_worker_deployment_versions = None;
                let mut image = baseline_image(&state, None, 37).unwrap();
                image.search_attributes.0.insert(
                    "TemporalUsedWorkerDeploymentVersions".into(),
                    SearchAttrValue::KeywordList(values.clone()),
                );
                match defect {
                    2 => image.namespace_id = NamespaceId::new(),
                    3 => image.workflow_id.0.push_str("-wrong"),
                    4 => image.run_id = RunId::new(),
                    6 => {
                        image
                            .search_attributes
                            .0
                            .remove("TemporalUsedWorkerDeploymentVersions");
                    }
                    7 => {
                        image.search_attributes.0.insert(
                            "TemporalUsedWorkerDeploymentVersions".into(),
                            SearchAttrValue::Keyword("wrong type".into()),
                        );
                    }
                    _ => {}
                }
                let bytes = if defect == 5 {
                    vec![255]
                } else {
                    codec::encode_projection_context(&image).unwrap()
                };
                fixture
                    .replace(
                        &state,
                        37,
                        (defect != 8).then_some((
                            partition,
                            if defect == 1 { 6 } else { 3 },
                            bytes,
                        )),
                    )
                    .await;
                let before = fixture.hot(key).await;
                fixture.statements.take();
                let loaded = fixture.repo().load_run_with_stats(key).await;
                let statements = fixture.statements.take();
                prop_assert!(
                    statements
                        .iter()
                        .any(|sql| sql.contains("projection_log") && sql.contains("SELECT"))
                );
                prop_assert!(
                    !statements
                        .iter()
                        .any(|sql| sql.to_ascii_uppercase().contains("UPDATE"))
                );
                prop_assert_eq!(fixture.hot(key).await, before);
                if (1..=5).contains(&defect) {
                    prop_assert!(loaded.is_err());
                } else {
                    let (LoadedRun::Existing(loaded), stats) = loaded.unwrap() else {
                        panic!("legacy")
                    };
                    prop_assert_eq!(stats.history_size_bytes, 37);
                    prop_assert_eq!(
                        loaded.used_worker_deployment_versions,
                        Some(if defect >= 6 { Vec::new() } else { values })
                    );
                }
                Ok(())
            })
        },
    );
}

// Feature: projection-accumulator, Property 4: rereads and images observe one real SQLx snapshot.
#[tokio::test]
#[ignore = "requires an ephemeral Aurora DSQL cluster"]
async fn dsql_projection_accumulator_snapshot_races() {
    let fixture = Fixture::connect().await.unwrap();
    for stage in [LoadStage::BeforeSnapshot, LoadStage::AfterHotRead] {
        for change in 0..3 {
            let mut original = applied(
                commit(fixture.repo(), fixture.fresh(), false, ShardEpoch::ZERO)
                    .await
                    .unwrap(),
            );
            let key = original.run_key;
            fixture.prune(key).await;
            original.used_worker_deployment_versions = None;
            original.transition_seq = TransitionSeq(5);
            let mut image = baseline_image(&original, None, 50).unwrap();
            image.search_attributes.0.insert(
                "TemporalUsedWorkerDeploymentVersions".into(),
                SearchAttrValue::KeywordList(vec!["before".into()]),
            );
            fixture
                .replace(
                    &original,
                    50,
                    Some((65, 5, codec::encode_projection_context(&image).unwrap())),
                )
                .await;
            let (reached, resume) = pause_at(key, stage);
            let reader = fixture.recreated();
            let read = tokio::spawn(async move { reader.load_run_with_stats(key).await });
            reached.await.unwrap();
            let mut updated = original.clone();
            updated.transition_seq = TransitionSeq(6);
            image.search_attributes.0.insert(
                "TemporalUsedWorkerDeploymentVersions".into(),
                SearchAttrValue::KeywordList(vec!["after".into()]),
            );
            if change == 2 {
                sqlx::query("DELETE FROM workflow_hot WHERE run_key = $1")
                    .bind(key.0)
                    .execute(&fixture.pool)
                    .await
                    .unwrap();
            } else {
                if change == 1 {
                    updated.used_worker_deployment_versions = Some(vec!["durably-ready".into()]);
                }
                fixture
                    .replace(
                        &updated,
                        60,
                        Some((66, 6, codec::encode_projection_context(&image).unwrap())),
                    )
                    .await;
            }
            resume.send(()).unwrap();
            let (loaded, stats) = read.await.unwrap().unwrap();
            if stage == LoadStage::BeforeSnapshot && change == 2 {
                assert_eq!(loaded, LoadedRun::Absent);
                assert_eq!(stats.history_size_bytes, 0);
            } else {
                let LoadedRun::Existing(state) = loaded else {
                    panic!("snapshot")
                };
                if stage == LoadStage::AfterHotRead {
                    assert_eq!(state.transition_seq, TransitionSeq(5));
                    assert_eq!(stats.history_size_bytes, 50);
                    assert_eq!(
                        state.used_worker_deployment_versions,
                        Some(vec!["before".into()])
                    );
                    assert!(matches!(
                        commit(fixture.repo(), following(&state), false, ShardEpoch::ZERO)
                            .await
                            .unwrap(),
                        CommitResult::Conflict { .. }
                    ));
                } else {
                    assert_eq!(state.transition_seq, TransitionSeq(6));
                    assert_eq!(stats.history_size_bytes, 60);
                    assert_eq!(
                        state.used_worker_deployment_versions,
                        Some(vec![
                            if change == 1 {
                                "durably-ready"
                            } else {
                                "after"
                            }
                            .into()
                        ])
                    );
                }
            }
        }
    }
    fixture.cleanup().await;
}

// Feature: projection-accumulator, Property 7: every rejected commit preserves its entire write set.
#[test]
#[ignore = "requires an ephemeral Aurora DSQL cluster"]
fn dsql_projection_accumulator_failure_isolation() {
    contract(
        prop::collection::vec("[ab]{0,8}", 0..16),
        |runtime, fixture, values| {
            runtime.block_on(async {
                for bundle in [false, true] {
                    let mut start = fixture.fresh();
                    start.next_state.used_worker_deployment_versions = Some(values.clone());
                    start.request_dedupe_ops.push(RequestDedupeOp {
                        request_id: RequestId("duplicate".into()),
                    });
                    let winner = applied(
                        commit(fixture.repo(), start, bundle, ShardEpoch::ZERO)
                            .await
                            .unwrap(),
                    );
                    let key = winner.run_key;
                    for failure in 0..6 {
                        let before = fixture.durable(key).await;
                        let mut transition = following(&winner);
                        transition.next_state.used_worker_deployment_versions = None;
                        let mut epoch = ShardEpoch::ZERO;
                        match failure {
                            0 => epoch = ShardEpoch(999_999),
                            1 => transition.expected_seq = TransitionSeq::ZERO,
                            2 => transition.request_dedupe_ops.push(RequestDedupeOp {
                                request_id: RequestId("duplicate".into()),
                            }),
                            3 => {}
                            4 => {
                                transition.next_state.used_worker_deployment_versions =
                                    Some(values.clone());
                                transition.next_state.transition_seq = TransitionSeq(u64::MAX);
                            }
                            _ => {
                                transition.expected_seq = TransitionSeq::ZERO;
                                transition.next_state.run_key = RunKey::new();
                                transition.next_state.run_id = RunId::new();
                                fixture.track(&transition.next_state);
                            }
                        }
                        let result = commit(fixture.repo(), transition, bundle, epoch).await;
                        match failure {
                            0 | 1 => {
                                prop_assert!(
                                    matches!(result, Ok(CommitResult::Conflict { .. })),
                                    "fence/OCC must win"
                                )
                            }
                            2 => prop_assert!(matches!(result, Ok(CommitResult::Duplicate))),
                            3 | 4 => prop_assert!(result.is_err()),
                            _ => prop_assert!(
                                matches!(result, Ok(CommitResult::CurrentExecutionConflict { .. })),
                                "current execution must win"
                            ),
                        }
                        prop_assert_eq!(fixture.durable(key).await, before);
                    }
                    let mut collision = baseline_image(&winner, None, 0).unwrap();
                    collision.transition_count += 1;
                    let seq = winner.transition_seq.next();
                    let partition =
                        super::partition_for(key, fixture.repo().projection_partition_count) as i32;
                    fixture
                        .replace(
                            &winner,
                            0,
                            Some((
                                partition,
                                seq.0 as i64,
                                codec::encode_projection_context(&collision).unwrap(),
                            )),
                        )
                        .await;
                    let before = fixture.durable(key).await;
                    let mut transition = following(&winner);
                    transition.request_dedupe_ops.push(RequestDedupeOp {
                        request_id: RequestId("rolled-back".into()),
                    });
                    let result = commit(fixture.repo(), transition, bundle, ShardEpoch::ZERO).await;
                    prop_assert!(
                        result.is_err() || matches!(result, Ok(CommitResult::Conflict { .. })),
                        "collision must roll back"
                    );
                    prop_assert_eq!(fixture.durable(key).await, before);
                    fixture.prune(key).await;
                    let mut close = following(&winner);
                    close.next_state.status = ExecutionStatus::Completed;
                    close.next_state.closed_at = Some(winner.started_at);
                    let closed = applied(
                        commit(fixture.repo(), close, bundle, ShardEpoch::ZERO)
                            .await
                            .unwrap(),
                    );
                    let deleted = fixture
                        .repo()
                        .delete_run_for_bundle(
                            key,
                            ShardId(0),
                            DeleteRunRequest {
                                expected_seq: closed.transition_seq,
                                deleted_at: closed.started_at,
                            },
                            ShardEpoch::ZERO,
                        )
                        .await
                        .unwrap();
                    let DeleteRunResult::Deleted { tombstone } = deleted else {
                        panic!("delete")
                    };
                    prop_assert!(tombstone.context.search_attributes.0.is_empty());
                    prop_assert!(tombstone.context.memo.0.is_empty());
                }
                Ok(())
            })
        },
    );
}

// Feature: projection-accumulator, Property 8: SQL persists exactly the bytes used by growth admission.
#[test]
#[ignore = "requires an ephemeral Aurora DSQL cluster"]
fn dsql_projection_accumulator_growth_accounting() {
    contract(
        (
            prop::collection::vec("[ab]{0,80}", 0..140),
            prop::collection::vec(any::<u8>(), 0..128),
            any::<bool>(),
        ),
        |runtime, fixture, (values, input, observe_new)| {
            runtime.block_on(async {
                for bundle in [false, true] {
                    let mut start = fixture.fresh();
                    start.growth_limits = Some(state_limit(0));
                    let state = applied(
                        commit(fixture.repo(), start, bundle, ShardEpoch::ZERO)
                            .await
                            .unwrap(),
                    );
                    let key = state.run_key;
                    let mut transition = following(&state);
                    transition.next_state.used_worker_deployment_versions = Some(values.clone());
                    observe(
                        &mut transition.next_state,
                        &(observe_new.then(|| "new".into()), 0, false),
                    );
                    let mut activity =
                        crate::memory::projection_accumulator_tests::activity_fixture();
                    activity.input = Payloads(vec![tokeira_types::Payload::new(input.clone())]);
                    let excluded = codec::encode(&activity.input).unwrap().len();
                    transition
                        .next_state
                        .activities
                        .insert(activity.activity_id.clone(), activity);
                    let mut expected_state = transition.next_state.clone();
                    if observe_new {
                        expected_state
                            .used_worker_deployment_versions
                            .as_mut()
                            .unwrap()
                            .push("deployment:new".into());
                    }
                    let expected = codec::encode_workflow_state(&expected_state).unwrap();
                    let measured = expected.len() - excluded;
                    transition.growth_limits = Some(state_limit(measured - 1));
                    let before = fixture.durable(key).await;
                    let error =
                        commit(fixture.repo(), transition.clone(), bundle, ShardEpoch::ZERO)
                            .await
                            .unwrap_err();
                    prop_assert_eq!(
                        error.downcast_ref::<RunLimitExceeded>().unwrap().limit,
                        RunLimit::StateSize
                    );
                    prop_assert_eq!(fixture.durable(key).await, before);
                    transition.growth_limits = Some(state_limit(measured));
                    let state = applied(
                        commit(fixture.repo(), transition, bundle, ShardEpoch::ZERO)
                            .await
                            .unwrap(),
                    );
                    prop_assert_eq!(fixture.hot(key).await.0, expected);
                    prop_assert_eq!(
                        &state.used_worker_deployment_versions,
                        &expected_state.used_worker_deployment_versions
                    );
                    let mut close = following(&state);
                    close.next_state.status = ExecutionStatus::Completed;
                    close.growth_limits = Some(state_limit(0));
                    applied(
                        commit(fixture.repo(), close, bundle, ShardEpoch::ZERO)
                            .await
                            .unwrap(),
                    );
                }
                Ok(())
            })
        },
    );
}

// Feature: projection-accumulator, Property 3: reset materialization creates no image and a ready-empty run.
#[test]
#[ignore = "requires an ephemeral Aurora DSQL cluster"]
fn dsql_projection_accumulator_reset_boundaries() {
    contract(
        prop::collection::vec("[ab]{0,8}", 0..16),
        |runtime, fixture, values| {
            runtime.block_on(async {
                for bundle in [false, true] {
                    let mut start = fixture.fresh();
                    start.next_state.used_worker_deployment_versions = Some(values.clone());
                    start.history_events = reset_history(&start.next_state).into();
                    start.event_principals = vec![None; start.history_events.len()].into();
                    start.next_state.last_event_id = 10;
                    start.next_state.status = ExecutionStatus::Completed;
                    let base = applied(
                        commit(fixture.repo(), start, bundle, ShardEpoch::ZERO)
                            .await
                            .unwrap(),
                    );
                    let mut predecessor = base.run_key;
                    for (generation, changed) in [(0, false), (1, true)] {
                        let id = RunId::new();
                        let key = RunKey::derive(base.namespace_id, &base.workflow_id, id);
                        fixture
                            .repo()
                            .materialize_reset_successor(
                                predecessor,
                                if generation == 0 { 10 } else { 7 },
                                id,
                            )
                            .await
                            .unwrap();
                        let (blob, stats) = fixture.hot(key).await;
                        let mut state = codec::decode_workflow_state(key, &blob).unwrap();
                        fixture.track(&state);
                        prop_assert_eq!(&state.used_worker_deployment_versions, &Some(Vec::new()));
                        prop_assert!(fixture.image(key).await.is_none());
                        if generation == 1 {
                            state.used_worker_deployment_versions = None;
                            fixture.replace(&state, stats, None).await;
                            let result =
                                commit(fixture.repo(), following(&state), bundle, ShardEpoch::ZERO)
                                    .await;
                            prop_assert!(result.is_err());
                        }
                        let LoadedRun::Existing(state) =
                            fixture.repo().load_run(key).await.unwrap()
                        else {
                            panic!("reset")
                        };
                        let mut transition = following(&state);
                        if changed {
                            observe(&mut transition.next_state, &(Some("v3".into()), 0, false));
                        }
                        let expected = baseline_image(&transition.next_state, None, stats).unwrap();
                        let committed = applied(
                            commit(fixture.repo(), transition, bundle, ShardEpoch::ZERO)
                                .await
                                .unwrap(),
                        );
                        prop_assert_eq!(
                            committed.used_worker_deployment_versions,
                            Some(vec![
                                if changed {
                                    "deployment:v3"
                                } else {
                                    "deployment:v2"
                                }
                                .into()
                            ])
                        );
                        prop_assert_eq!(fixture.image(key).await, Some(expected));
                        predecessor = key;
                    }
                }
                Ok(())
            })
        },
    );
}

// Feature: projection-accumulator, Property 6: old codec rewrites remain recoverable until retention prerequisites hold.
#[test]
#[ignore = "requires an ephemeral Aurora DSQL cluster"]
fn dsql_projection_accumulator_mixed_writers_and_pruning() {
    contract(
        (observations(), any::<bool>(), 0u8..3),
        |runtime, fixture, (steps, old_on_even, boundary)| {
            runtime.block_on(async {
                for bundle in [false, true] {
                    let mut start = fixture.fresh();
                    if boundary == 1 {
                        observe(&mut start.next_state, &(Some("inherited".into()), 0, false));
                    }
                    if boundary == 2 {
                        start.history_events = reset_history(&start.next_state).into();
                        start.event_principals = vec![None; start.history_events.len()].into();
                        start.next_state.last_event_id = 10;
                        start.next_state.status = ExecutionStatus::Completed;
                    }
                    let initial = applied(
                        commit(fixture.repo(), start, bundle, ShardEpoch::ZERO)
                            .await
                            .unwrap(),
                    );
                    let mut key = initial.run_key;
                    if boundary == 2 {
                        let id = RunId::new();
                        fixture
                            .repo()
                            .materialize_reset_successor(key, 10, id)
                            .await
                            .unwrap();
                        key = RunKey::derive(initial.namespace_id, &initial.workflow_id, id);
                        let LoadedRun::Existing(state) =
                            fixture.repo().load_run(key).await.unwrap()
                        else {
                            panic!("reset")
                        };
                        fixture.track(&state);
                    }
                    let history_size = fixture.hot(key).await.1;
                    let mut previous = fixture.image(key).await;
                    for (index, observation) in steps.iter().enumerate() {
                        let before = fixture.hot(key).await;
                        let repo = fixture.recreated();
                        let LoadedRun::Existing(state) = repo.load_run(key).await.unwrap() else {
                            panic!("recreated")
                        };
                        prop_assert_eq!(fixture.hot(key).await, before);
                        let mut transition = following(&state);
                        observe(&mut transition.next_state, observation);
                        let expected =
                            baseline_image(&transition.next_state, previous.as_ref(), history_size)
                                .unwrap();
                        if (index % 2 == 0) == old_on_even {
                            fixture.legacy_write(key, observation).await;
                            prop_assert!(
                                codec::decode_workflow_state(key, &fixture.hot(key).await.0)
                                    .unwrap()
                                    .used_worker_deployment_versions
                                    .is_none()
                            );
                        } else {
                            let state = applied(
                                commit(&repo, transition, bundle, ShardEpoch::ZERO)
                                    .await
                                    .unwrap(),
                            );
                            prop_assert_eq!(
                                state.used_worker_deployment_versions,
                                Some(image_versions(&expected))
                            );
                        }
                        prop_assert_eq!(fixture.image(key).await, Some(expected.clone()));
                        let LoadedRun::Existing(loaded) =
                            fixture.recreated().load_run(key).await.unwrap()
                        else {
                            panic!("load")
                        };
                        prop_assert_eq!(
                            loaded.used_worker_deployment_versions,
                            Some(image_versions(&expected))
                        );
                        previous = Some(expected);
                    }
                    let LoadedRun::Existing(state) = fixture.repo().load_run(key).await.unwrap()
                    else {
                        panic!("final")
                    };
                    let mut persist = following(&state);
                    observe(&mut persist.next_state, &(None, 0, false));
                    persist
                        .next_state
                        .used_worker_deployment_versions
                        .as_mut()
                        .unwrap()
                        .push("retained-only".into());
                    let ready = applied(
                        commit(fixture.repo(), persist, bundle, ShardEpoch::ZERO)
                            .await
                            .unwrap(),
                    );
                    let retained = fixture.image(key).await.unwrap();
                    fixture.prune(key).await;
                    let repo = fixture.recreated();
                    let LoadedRun::Existing(reloaded) = repo.load_run(key).await.unwrap() else {
                        panic!("ready after prune")
                    };
                    prop_assert_eq!(&reloaded, &ready);
                    let transition = following(&reloaded);
                    let expected =
                        baseline_image(&transition.next_state, Some(&retained), history_size)
                            .unwrap();
                    let persisted = applied(
                        commit(&repo, transition, bundle, ShardEpoch::ZERO)
                            .await
                            .unwrap(),
                    );
                    prop_assert_eq!(fixture.image(key).await, Some(expected.clone()));
                    prop_assert_eq!(
                        persisted.used_worker_deployment_versions,
                        Some(image_versions(&expected))
                    );

                    fixture.prune(key).await;
                    fixture.legacy_write(key, &(None, 0, false)).await;
                    let LoadedRun::Existing(lost) =
                        fixture.recreated().load_run(key).await.unwrap()
                    else {
                        panic!("old writer control")
                    };
                    prop_assert_eq!(&lost.used_worker_deployment_versions, &Some(Vec::new()));

                    let legacy = legacy_codec::decode_workflow_state(
                        key,
                        &codec::encode_workflow_state(&ready).unwrap(),
                    )
                    .unwrap();
                    fixture.replace(&legacy, 0, None).await;
                    fixture.prune(key).await;
                    let LoadedRun::Existing(lost) =
                        fixture.recreated().load_run(key).await.unwrap()
                    else {
                        panic!("unseeded control")
                    };
                    prop_assert_eq!(&lost.used_worker_deployment_versions, &Some(Vec::new()));
                    prop_assert_ne!(
                        lost.used_worker_deployment_versions,
                        ready.used_worker_deployment_versions
                    );
                }
                Ok(())
            })
        },
    );
}
