//! Projection passes against an operator-owned ephemeral Aurora DSQL cluster
//! (`projection-passes`, Live Test). No connection occurs unless the dedicated test
//! URL is provided. Each test writes its images into a partition of its own, so it
//! never meets another suite's images, and applies them into a namespace of its own.

use std::{
    collections::BTreeMap,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use anyhow::Result;
use async_trait::async_trait;
use sqlx::PgPool;
use time::OffsetDateTime;
use tokeira_storage::{
    ProjectionContext, ProjectionImageKey, ProjectionLog, ProjectionPage, ProjectionRecord,
    dsql::{DsqlPoolConfig, DsqlProjectionLog, DsqlStore, MigrationConfig, codec},
};
use tokeira_types::{
    ArchetypeId, ExecutionStatus, Memo, NamespaceId, RunId, RunKey, SearchAttrValue,
    SearchAttributes, TaskQueueName, TransitionSeq, VisibilityLifecycleState, WorkflowId,
    WorkflowType,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::apply_faults::{self, Fault};
use crate::{
    DsqlVisibilityStore, PAGE_SIZE, Projector, RollupDimension, VisibilityStore,
    seed_predefined_search_attributes, workflow_status_keyword,
};

const USED_VERSIONS: &str = "TemporalUsedWorkerDeploymentVersions";

struct Live {
    pool: PgPool,
    store: DsqlStore,
    visibility: DsqlVisibilityStore,
    namespace_id: NamespaceId,
    partition_id: u32,
}

impl Live {
    async fn connect() -> Result<Option<Self>> {
        let Ok(url) = std::env::var("TOKEIRA_DSQL_TEST_DATABASE_URL") else {
            return Ok(None);
        };
        let pool = PgPool::connect(&url).await?;
        let config = DsqlPoolConfig {
            // nextest starts this test in its crate directory, not the workspace root.
            migration: MigrationConfig {
                migrations_dir: Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../tokeira-storage/migrations"),
            },
            ..DsqlPoolConfig::default()
        };
        let store = DsqlStore::from_database_url_for_tests(url, config).await?;
        store.migration_runner().apply(&pool).await?;
        let visibility = DsqlVisibilityStore::new(store.connection_director_arc());
        let namespace_id = NamespaceId(Uuid::new_v4());
        seed_predefined_search_attributes(&visibility, namespace_id).await?;
        Ok(Some(Self {
            pool,
            store,
            visibility,
            namespace_id,
            partition_id: fresh_partition(),
        }))
    }

    fn log(&self) -> DsqlProjectionLog {
        self.store.projection_log().clone()
    }

    fn projector<L: ProjectionLog>(
        &self,
        log: L,
        visibility: DsqlVisibilityStore,
        page_size: usize,
    ) -> Projector<L, DsqlVisibilityStore> {
        Projector {
            log,
            sink: visibility,
            partition_id: self.partition_id,
            fanout: 1,
            page_size,
        }
    }

    fn image(
        &self,
        run_key: RunKey,
        transition_seq: u64,
        status: ExecutionStatus,
    ) -> ProjectionRecord {
        image_in(
            self.partition_id,
            self.namespace_id,
            run_key,
            transition_seq,
            status,
        )
    }

    async fn commit(&self, image: &ProjectionRecord) -> Result<()> {
        commit_image(&self.pool, image).await
    }

    async fn images(&self, partition_id: u32) -> Result<Vec<(RunKey, u64)>> {
        Ok(sqlx::query_as::<_, (Uuid, i64)>(
            "SELECT run_key, transition_seq FROM projection_log \
             WHERE partition_id = $1 AND fanout = 1 ORDER BY run_key, transition_seq",
        )
        .bind(i32::try_from(partition_id)?)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|(run_key, transition_seq)| (RunKey(run_key), transition_seq as u64))
        .collect())
    }

    async fn stored_seq(&self, run_key: RunKey) -> Result<Option<u64>> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT source_transition_seq FROM execution_visibility_current \
             WHERE namespace_id = $1 AND archetype_id = 0 AND run_key = $2",
        )
        .bind(self.namespace_id.0)
        .bind(run_key.0)
        .fetch_optional(&self.pool)
        .await?
        .map(|seq| seq as u64))
    }

    async fn status_counts(&self) -> Result<BTreeMap<String, i64>> {
        Ok(self
            .visibility
            .count_from_rollup(
                self.namespace_id,
                ArchetypeId::WORKFLOW,
                RollupDimension::ExecutionStatus,
            )
            .await?
            .groups
            .into_iter()
            .map(|group| (group.value, group.count))
            .collect())
    }

    /// Each of the run's used-versions attribute rows, as its keyword and its whole
    /// encoded value: the rows the storage load seeds an unseeded run from.
    async fn used_versions_rows(&self, run_key: RunKey) -> Result<Vec<(String, SearchAttrValue)>> {
        sqlx::query_as::<_, (Option<String>, Vec<u8>)>(
            "SELECT a.keyword_value, a.value_data FROM execution_visibility_attr_index a \
             JOIN sa_registry r ON r.namespace_id = a.namespace_id AND r.attr_id = a.attr_id \
             WHERE a.namespace_id = $1 AND a.run_key = $2 AND r.attr_name = $3",
        )
        .bind(self.namespace_id.0)
        .bind(run_key.0)
        .bind(USED_VERSIONS)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|(keyword, data)| Ok((keyword.unwrap_or_default(), codec::decode(&data)?)))
        .collect()
    }

    async fn finish(self, other_partitions: &[u32]) -> Result<()> {
        for partition_id in [self.partition_id].iter().chain(other_partitions) {
            sqlx::query("DELETE FROM projection_log WHERE partition_id = $1 AND fanout = 1")
                .bind(i32::try_from(*partition_id)?)
                .execute(&self.pool)
                .await?;
        }
        for statement in [
            "DELETE FROM execution_visibility_attr_index WHERE namespace_id = $1",
            "DELETE FROM execution_visibility_current WHERE namespace_id = $1",
            "DELETE FROM execution_visibility_rollup WHERE namespace_id = $1",
            "DELETE FROM sa_registry WHERE namespace_id = $1",
        ] {
            sqlx::query(statement)
                .bind(self.namespace_id.0)
                .execute(&self.pool)
                .await?;
        }
        self.pool.close().await;
        self.store.shutdown().await
    }
}

/// A positive partition id outside the ones commits use.
fn fresh_partition() -> u32 {
    ((Uuid::new_v4().as_u128() as u32) & 0x3fff_ffff) | 0x4000_0000
}

/// A run key that sorts at `rank` among keys with other ranks: its first byte.
fn run_key_ranked(rank: u8) -> RunKey {
    RunKey(Uuid::from_u128(
        (u128::from(rank) << 120) | (Uuid::new_v4().as_u128() >> 8),
    ))
}

fn image_in(
    partition_id: u32,
    namespace_id: NamespaceId,
    run_key: RunKey,
    transition_seq: u64,
    status: ExecutionStatus,
) -> ProjectionRecord {
    let workflow_id = WorkflowId(format!("workflow-{}", run_key.0));
    let closed = status != ExecutionStatus::Running;
    ProjectionRecord {
        partition_id,
        fanout: 1,
        run_key,
        transition_seq: TransitionSeq(transition_seq),
        context: ProjectionContext {
            archetype_id: ArchetypeId::WORKFLOW,
            namespace_id,
            business_id: workflow_id.0.clone(),
            authority_epoch: 0,
            status_keyword: workflow_status_keyword(status),
            lifecycle_state: if closed {
                VisibilityLifecycleState::Closed
            } else {
                VisibilityLifecycleState::Open
            },
            workflow_id: workflow_id.clone(),
            run_id: RunId(run_key.0),
            workflow_type: WorkflowType("workflow-type".to_owned()),
            task_queue: TaskQueueName("queue".to_owned()),
            execution_status: status,
            start_time: OffsetDateTime::UNIX_EPOCH,
            update_time: OffsetDateTime::UNIX_EPOCH,
            execution_time: Some(OffsetDateTime::UNIX_EPOCH),
            close_time: closed.then_some(OffsetDateTime::UNIX_EPOCH),
            history_length: transition_seq as i64,
            execution_duration: None,
            state_transition_count: transition_seq as i64,
            transition_count: transition_seq as i64,
            history_size_bytes: 0,
            parent_workflow_id: None,
            parent_run_id: None,
            root_workflow_id: Some(workflow_id),
            root_run_id: Some(RunId(run_key.0)),
            search_attr_generation: transition_seq,
            memo: Memo::default(),
            search_attributes: SearchAttributes::default(),
        },
    }
}

fn with_used_versions(mut image: ProjectionRecord, versions: &[&str]) -> ProjectionRecord {
    image.context.search_attributes.0.insert(
        USED_VERSIONS.to_owned(),
        SearchAttrValue::KeywordList(versions.iter().map(|&version| version.to_owned()).collect()),
    );
    image
}

/// Write an image as a commit does.
async fn commit_image(pool: &PgPool, image: &ProjectionRecord) -> Result<()> {
    sqlx::query(
        "INSERT INTO projection_log \
         (partition_id, fanout, run_key, transition_seq, context_data, ops_data) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(i32::try_from(image.partition_id)?)
    .bind(i16::try_from(image.fanout)?)
    .bind(image.run_key.0)
    .bind(i64::try_from(image.transition_seq.0)?)
    .bind(codec::encode_projection_context(&image.context)?)
    .bind(codec::LEGACY_EMPTY_PROJECTION_OPS_DATA)
    .execute(pool)
    .await?;
    Ok(())
}

/// The DSQL log, committing an image right after the first page a pass reads, as a
/// commit that lands behind the pass's position.
struct CommitsDuringAPass {
    log: DsqlProjectionLog,
    pool: PgPool,
    late: Mutex<Option<ProjectionRecord>>,
}

#[async_trait]
impl ProjectionLog for CommitsDuringAPass {
    async fn read_page(
        &self,
        partition_id: u32,
        fanout: u16,
        after: Option<ProjectionImageKey>,
        limit: usize,
    ) -> Result<ProjectionPage> {
        let page = self
            .log
            .read_page(partition_id, fanout, after, limit)
            .await?;
        let late = self.late.lock().expect("late image lock").take();
        if let Some(late) = late {
            commit_image(&self.pool, &late).await?;
        }
        Ok(page)
    }

    async fn delete_images(
        &self,
        partition_id: u32,
        fanout: u16,
        keys: &[ProjectionImageKey],
    ) -> Result<u64> {
        self.log.delete_images(partition_id, fanout, keys).await
    }
}

/// The DSQL log, whose first read fails.
struct FailsItsFirstRead {
    log: DsqlProjectionLog,
    failed: AtomicBool,
}

#[async_trait]
impl ProjectionLog for FailsItsFirstRead {
    async fn read_page(
        &self,
        partition_id: u32,
        fanout: u16,
        after: Option<ProjectionImageKey>,
        limit: usize,
    ) -> Result<ProjectionPage> {
        if !self.failed.swap(true, Ordering::SeqCst) {
            anyhow::bail!("injected read failure");
        }
        self.log.read_page(partition_id, fanout, after, limit).await
    }

    async fn delete_images(
        &self,
        partition_id: u32,
        fanout: u16,
        keys: &[ProjectionImageKey],
    ) -> Result<u64> {
        self.log.delete_images(partition_id, fanout, keys).await
    }
}

// `projection-passes` Live Test step 1, criteria 2.1-2.3: images below and above a
// pass's position, and one committed behind it during the pass, are all applied by
// the next pass, and every image goes.
#[tokio::test]
async fn dsql_passes_apply_images_on_both_sides_of_the_position_and_behind_it() -> Result<()> {
    let Some(live) = Live::connect().await? else {
        return Ok(());
    };
    let low = run_key_ranked(0x10);
    let middle = run_key_ranked(0x80);
    let high = run_key_ranked(0xf0);
    live.commit(&live.image(middle, 1, ExecutionStatus::Running))
        .await?;
    live.commit(&live.image(high, 1, ExecutionStatus::Running))
        .await?;
    live.commit(&live.image(high, 2, ExecutionStatus::Completed))
        .await?;
    let log = CommitsDuringAPass {
        log: live.log(),
        pool: live.pool.clone(),
        late: Mutex::new(Some(live.image(low, 1, ExecutionStatus::Running))),
    };
    // A page of one image keeps a position inside the pass from its first read.
    let projector = live.projector(log, live.visibility.clone(), 1);

    let first = projector.pass(&CancellationToken::new()).await;
    let behind = (
        live.stored_seq(low).await?,
        live.images(live.partition_id).await?,
    );
    let second = projector.pass(&CancellationToken::new()).await;

    let stored = [
        live.stored_seq(low).await?,
        live.stored_seq(middle).await?,
        live.stored_seq(high).await?,
    ];
    let left = live.images(live.partition_id).await?;
    live.finish(&[]).await?;
    assert!(!first.failed() && !second.failed(), "{first:?} {second:?}");
    assert_eq!(
        behind,
        (None, vec![(low, 1)]),
        "the late image is behind the first pass's position"
    );
    assert_eq!(
        stored,
        [Some(1), Some(1), Some(2)],
        "every run at its newest image"
    );
    assert!(left.is_empty(), "every image deleted: {left:?}");
    Ok(())
}

// `projection-passes` Live Test step 2, criteria 2.5 and 2.6: two projectors of one
// partition, each over a store of its own as on two nodes, both outlive a failure and
// leave Count exact.
#[tokio::test]
async fn dsql_two_projectors_of_one_partition_both_run_and_count_exactly() -> Result<()> {
    let Some(live) = Live::connect().await? else {
        return Ok(());
    };
    let runs = (0..40)
        .map(|index| run_key_ranked(index * 6))
        .collect::<Vec<_>>();
    for run_key in &runs {
        live.commit(&live.image(*run_key, 1, ExecutionStatus::Running))
            .await?;
        live.commit(&live.image(*run_key, 2, ExecutionStatus::Running))
            .await?;
        live.commit(&live.image(*run_key, 3, ExecutionStatus::Completed))
            .await?;
    }
    let first = Arc::new(live.projector(
        FailsItsFirstRead {
            log: live.log(),
            failed: AtomicBool::new(false),
        },
        DsqlVisibilityStore::new(live.store.connection_director_arc()),
        4,
    ));
    let second = Arc::new(live.projector(
        live.log(),
        DsqlVisibilityStore::new(live.store.connection_director_arc()),
        4,
    ));
    let cancel = CancellationToken::new();
    let running = [
        tokio::spawn({
            let (projector, cancel) = (first.clone(), cancel.clone());
            async move { projector.run(cancel).await }
        }),
        tokio::spawn({
            let (projector, cancel) = (second.clone(), cancel.clone());
            async move { projector.run(cancel).await }
        }),
    ];

    let started = Instant::now();
    while !live.images(live.partition_id).await?.is_empty()
        && started.elapsed() < std::time::Duration::from_secs(120)
    {
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    // Both keep running their idle passes.
    tokio::time::sleep(crate::IDLE_PAUSE * 2).await;
    let both_running = running.iter().all(|task| !task.is_finished());
    cancel.cancel();
    for task in running {
        task.await?;
    }
    let left = live.images(live.partition_id).await?;
    let counts = live.status_counts().await?;
    let mut stored = Vec::new();
    for run_key in &runs {
        stored.push(live.stored_seq(*run_key).await?);
    }
    live.finish(&[]).await?;
    assert!(both_running, "neither projector ended");
    assert!(left.is_empty(), "every image deleted: {left:?}");
    assert_eq!(
        counts,
        BTreeMap::from([(workflow_status_keyword(ExecutionStatus::Completed), 40)]),
        "each run counted once, in its newest status"
    );
    assert!(stored.iter().all(|seq| *seq == Some(3)), "{stored:?}");
    Ok(())
}

// `projection-passes` Live Test step 3, criteria 2.3 and 2.4: a pass deletes a run's
// images only after its newest image applied, so the visibility store holds that
// image's used-versions list, whole, on every attribute row, where an unseeded run's
// next load seeds from it. A run whose apply fails keeps its images, and an older
// image under another partition id is never read, so never deleted.
#[tokio::test]
async fn dsql_a_pass_deletes_images_only_once_their_list_is_applied() -> Result<()> {
    let Some(live) = Live::connect().await? else {
        return Ok(());
    };
    let applied = run_key_ranked(0x20);
    let failing = run_key_ranked(0xa0);
    let earlier_partition = fresh_partition();
    let older = image_in(
        earlier_partition,
        live.namespace_id,
        applied,
        1,
        ExecutionStatus::Running,
    );
    live.commit(&with_used_versions(older, &["old"])).await?;
    live.commit(&with_used_versions(
        live.image(applied, 2, ExecutionStatus::Running),
        &["a"],
    ))
    .await?;
    live.commit(&with_used_versions(
        live.image(applied, 3, ExecutionStatus::Running),
        &["a", "b", "a"],
    ))
    .await?;
    for transition_seq in 1..=2 {
        live.commit(&with_used_versions(
            live.image(failing, transition_seq, ExecutionStatus::Running),
            &["x"],
        ))
        .await?;
    }
    let projector = live.projector(live.log(), live.visibility.clone(), PAGE_SIZE);
    apply_faults::arm(Fault::StoredRowRead(failing));

    let first = projector.pass(&CancellationToken::new()).await;
    let kept = live.images(live.partition_id).await?;
    let applied_rows = live.used_versions_rows(applied).await?;
    let second = projector.pass(&CancellationToken::new()).await;

    let left = live.images(live.partition_id).await?;
    let failing_rows = live.used_versions_rows(failing).await?;
    let stored = [
        live.stored_seq(applied).await?,
        live.stored_seq(failing).await?,
    ];
    let earlier = live.images(earlier_partition).await?;
    live.finish(&[earlier_partition]).await?;
    assert_eq!(first.failed_applies, 1, "{first:?}");
    assert_eq!(
        kept,
        vec![(failing, 1), (failing, 2)],
        "the failed run's images stay"
    );
    let list = SearchAttrValue::KeywordList(vec!["a".into(), "b".into(), "a".into()]);
    let mut keywords = applied_rows
        .iter()
        .map(|(keyword, _)| keyword.as_str())
        .collect::<Vec<_>>();
    keywords.sort_unstable();
    assert_eq!(keywords, ["a", "b"], "a row for each element");
    assert!(
        applied_rows.iter().all(|(_, value)| *value == list),
        "each row carries the newest image's whole list: {applied_rows:?}"
    );
    assert!(!second.failed(), "{second:?}");
    assert!(
        left.is_empty(),
        "every image deleted once applied: {left:?}"
    );
    assert_eq!(
        failing_rows,
        vec![(
            "x".to_owned(),
            SearchAttrValue::KeywordList(vec!["x".into()])
        )]
    );
    assert_eq!(stored, [Some(3), Some(2)]);
    assert_eq!(
        earlier,
        vec![(applied, 1)],
        "the older partition is never read"
    );
    Ok(())
}

// `projection-passes` Live Test step 4, criteria 2.6 and 2.8: applies that fail on a
// counter upsert or on the read of the stored row keep their runs' images, and once
// a later pass applies them, Count is exact.
#[tokio::test]
async fn dsql_failed_applies_keep_their_images_and_count_stays_exact() -> Result<()> {
    let Some(live) = Live::connect().await? else {
        return Ok(());
    };
    let first_run = run_key_ranked(0x30);
    let second_run = run_key_ranked(0xb0);
    for run_key in [first_run, second_run] {
        live.commit(&live.image(run_key, 1, ExecutionStatus::Running))
            .await?;
    }
    let projector = live.projector(live.log(), live.visibility.clone(), PAGE_SIZE);
    projector.pass(&CancellationToken::new()).await;
    for run_key in [first_run, second_run] {
        live.commit(&live.image(run_key, 2, ExecutionStatus::Completed))
            .await?;
    }
    // The first run's apply fails on its counters; the second's, on its read.
    apply_faults::arm(Fault::RollupUpsert(live.namespace_id));
    apply_faults::arm(Fault::StoredRowRead(second_run));

    let failing = projector.pass(&CancellationToken::new()).await;
    let kept = live.images(live.partition_id).await?;
    let counts_after_failures = live.status_counts().await?;
    let recovering = projector.pass(&CancellationToken::new()).await;

    let left = live.images(live.partition_id).await?;
    let counts = live.status_counts().await?;
    live.finish(&[]).await?;
    assert_eq!(failing.failed_applies, 2, "{failing:?}");
    assert_eq!(kept, vec![(first_run, 2), (second_run, 2)]);
    assert_eq!(
        counts_after_failures,
        BTreeMap::from([(workflow_status_keyword(ExecutionStatus::Running), 2)]),
        "a failed apply moves no counter"
    );
    assert!(!recovering.failed(), "{recovering:?}");
    assert!(left.is_empty(), "{left:?}");
    assert_eq!(
        counts,
        BTreeMap::from([(workflow_status_keyword(ExecutionStatus::Completed), 2)]),
        "each run counted once, in its new status"
    );
    Ok(())
}

/// Runs, and images of each, that the throughput measurement applies.
const MEASURED_RUNS: u8 = 250;
const MEASURED_TRANSITIONS: u64 = 4;

// `projection-passes` Live Test step 6: applies a second for one partition, with one
// projector applying runs one at a time. Set `TOKEIRA_PROJECTION_THROUGHPUT_OUTPUT`
// to keep the figures with the run's evidence.
#[tokio::test]
async fn dsql_projection_pass_throughput() -> Result<()> {
    let Some(live) = Live::connect().await? else {
        return Ok(());
    };
    for index in 0..MEASURED_RUNS {
        let run_key = run_key_ranked(index);
        for transition_seq in 1..=MEASURED_TRANSITIONS {
            live.commit(&live.image(run_key, transition_seq, ExecutionStatus::Running))
                .await?;
        }
    }
    let projector = live.projector(live.log(), live.visibility.clone(), PAGE_SIZE);

    let started = Instant::now();
    let mut passes = Vec::new();
    loop {
        let pass = projector.pass(&CancellationToken::new()).await;
        let idle = pass.idle();
        passes.push(pass);
        if idle {
            break;
        }
    }
    let elapsed = started.elapsed();
    let left = live.images(live.partition_id).await?;
    live.finish(&[]).await?;
    let applied: usize = passes.iter().map(|pass| pass.applied).sum();
    let deleted: u64 = passes.iter().map(|pass| pass.deleted).sum();
    let report = format!(
        "{applied} runs applied and {deleted} images deleted in {} passes, {:.3} s: \
         {:.1} applies/s, {:.1} images/s\n",
        passes.len(),
        elapsed.as_secs_f64(),
        applied as f64 / elapsed.as_secs_f64(),
        deleted as f64 / elapsed.as_secs_f64(),
    );
    if let Some(path) = std::env::var_os("TOKEIRA_PROJECTION_THROUGHPUT_OUTPUT") {
        std::fs::write(path, &report)?;
    }
    assert!(left.is_empty(), "{left:?}");
    assert_eq!(applied, usize::from(MEASURED_RUNS), "{report}");
    assert_eq!(
        deleted,
        u64::from(MEASURED_RUNS) * MEASURED_TRANSITIONS,
        "{report}"
    );
    Ok(())
}
