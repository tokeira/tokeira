//! Visibility images, CHASM nodes' data and worker invocation requests over the part size
//! (`large-value-parts`) against an operator-owned ephemeral Aurora DSQL cluster:
//! the shared contracts, and the cases only DSQL can show: the parts each write
//! leaves, those a deletion removes, and a load held against a persist. No
//! connection occurs unless the dedicated test URL is provided.

use std::path::Path;

use anyhow::{Context as _, Result, ensure};
use proptest::{
    prelude::*,
    test_runner::{TestCaseResult, TestRunner},
};
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokeira_chasm::{
    ChasmNode, ExecutionKey, LifecycleState, NodeMetadata, VersionedTransition,
    path::{self, PathSegment},
};
use tokeira_kernel::{
    LoadedRun,
    limits::{MEMO_SIZE_LIMIT_ERROR, RunGrowthLimits},
    payload_size::memo_encoded_len,
};
use tokeira_types::{Payload, RunKey, TransitionSeq};
use tokio::runtime::Runtime;
use uuid::Uuid;

use crate::{
    ChasmNodeRepository, CommitResult, CurrentRun, ExpectedVersion, NodePersistOutcome, NodeWrite,
    ProjectionImageKey, ProjectionLog, RunRepository, WriteFence,
    api::prepare_workflow_projection,
    bulk_write_tests::noise,
    codec,
    dsql::{
        DsqlPoolConfig, DsqlStore, MigrationConfig, ReservoirConfig,
        run_repository::pause_point::{self, PauseKey},
        value_parts::{self, PART_BYTES},
    },
    large_value_tests::{
        nodes_over_the_part_size_read_back, requests_over_the_part_size_read_back,
    },
    memory::projection_accumulator_tests::{following, fresh_transition},
};

const MIB: usize = 1024 * 1024;

/// The archetype of the executions these cases write.
const ARCHETYPE: u32 = 7;

async fn forget_visibility_run(pool: &PgPool, run: RunKey) -> Result<()> {
    for sql in [
        "DELETE FROM current_execution WHERE run_key = $1",
        "DELETE FROM workflow_dispatch WHERE run_key = $1",
        "DELETE FROM run_value_part WHERE run_key = $1",
        "DELETE FROM workflow_hot WHERE run_key = $1",
    ] {
        sqlx::query(sql).bind(run.0).execute(pool).await?;
    }
    Ok(())
}

async fn image_partition(pool: &PgPool, run: RunKey) -> Result<(u32, u16)> {
    let (partition, fanout) = sqlx::query_as::<_, (i32, i16)>(
        "SELECT partition_id, fanout FROM projection_log WHERE run_key = $1 LIMIT 1",
    )
    .bind(run.0)
    .fetch_one(pool)
    .await?;
    Ok((u32::try_from(partition)?, u16::try_from(fanout)?))
}

// Feature: large-value-parts, Property 6: Removing a value removes its parts
#[test]
fn dsql_deleting_images_removes_their_parts() {
    let runtime = Runtime::new().unwrap();
    let Some((pool, store)) = runtime.block_on(connect()).unwrap() else {
        return;
    };
    let result = TestRunner::new(ProptestConfig::with_cases(8)).run(
        &(PART_BYTES..=(2 * PART_BYTES)),
        |len| {
            runtime
                .block_on(async {
                    let run = RunKey::new();
                    let mut start = fresh_transition(run);
                    start
                        .next_state
                        .memo
                        .0
                        .insert("large".to_owned(), Payload::new(noise(len, 1)));
                    let CommitResult::Applied { new_state } = store
                        .run_repository()
                        .commit_transition(run, start, WriteFence::Unfenced)
                        .await?
                    else {
                        anyhow::bail!("the image commits")
                    };
                    let mut next = following(&new_state);
                    next.next_state
                        .memo
                        .0
                        .insert("large".to_owned(), Payload::new(noise(len, 2)));
                    let CommitResult::Applied { new_state: latest } = store
                        .run_repository()
                        .commit_transition(run, next, WriteFence::Unfenced)
                        .await?
                    else {
                        anyhow::bail!("the next image commits")
                    };
                    let (partition, fanout) = image_partition(&pool, run).await?;
                    let key = ProjectionImageKey {
                        run_key: run,
                        transition_seq: new_state.transition_seq,
                    };
                    let deleted = store
                        .projection_log()
                        .delete_images(partition, fanout, &[key])
                        .await?;
                    ensure!(deleted == 1, "one image was deleted");
                    let remaining: i64 = sqlx::query_scalar(
                        "SELECT count(*) FROM projection_image_part
                         WHERE run_key = $1 AND transition_seq = $2",
                    )
                    .bind(run.0)
                    .bind(i64::try_from(key.transition_seq.0)?)
                    .fetch_one(&pool)
                    .await?;
                    ensure!(remaining == 0, "the deleted image leaves no parts");
                    let page = store
                        .projection_log()
                        .read_page(partition, fanout, Some(key), 1)
                        .await?;
                    ensure!(
                        page.images.len() == 1 && page.images[0].context.memo == latest.memo,
                        "the later image's parts remain intact",
                    );
                    let key = ProjectionImageKey {
                        run_key: run,
                        transition_seq: latest.transition_seq,
                    };
                    store
                        .projection_log()
                        .delete_images(partition, fanout, &[key])
                        .await?;
                    let remaining: i64 = sqlx::query_scalar(
                        "SELECT count(*) FROM projection_image_part WHERE run_key = $1",
                    )
                    .bind(run.0)
                    .fetch_one(&pool)
                    .await?;
                    ensure!(remaining == 0, "all deleted images' parts are gone");
                    forget_visibility_run(&pool, run).await
                })
                .map_err(|error| TestCaseError::fail(format!("{error:?}")))
        },
    );
    runtime.block_on(store.shutdown()).unwrap();
    result.unwrap();
}

// Feature: large-value-parts, Property 3: A stored value reads back whole
#[tokio::test]
async fn dsql_a_split_image_seeds_the_legacy_accumulator() -> Result<()> {
    let Some((pool, store)) = connect().await? else {
        return Ok(());
    };
    let run = RunKey::new();
    let mut start = fresh_transition(run);
    start
        .next_state
        .memo
        .0
        .insert("large".to_owned(), Payload::new(noise(2 * PART_BYTES, 9)));
    start.next_state.used_worker_deployment_versions = Some(vec!["deployment.build".to_owned()]);
    let CommitResult::Applied { mut new_state } = store
        .run_repository()
        .commit_transition(run, start, WriteFence::Unfenced)
        .await?
    else {
        anyhow::bail!("the image commits")
    };
    new_state.used_worker_deployment_versions = None;
    let encoded = codec::encode_workflow_state(&new_state)?;
    let split = value_parts::split(&encoded);
    let mut tx = pool.begin().await?;
    sqlx::query("UPDATE workflow_hot SET state_data = $2, state_parts = $3 WHERE run_key = $1")
        .bind(run.0)
        .bind(split.first)
        .bind(split.count)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM run_value_part WHERE run_key = $1 AND value_kind = 1")
        .bind(run.0)
        .execute(&mut *tx)
        .await?;
    for (part_no, data) in split.numbered() {
        sqlx::query("INSERT INTO run_value_part (run_key, value_kind, value_key, part_no, data) VALUES ($1, 1, 0, $2, $3)")
            .bind(run.0).bind(part_no).bind(data).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    let loaded = store.run_repository().load_run(run).await;
    let (partition, fanout) = image_partition(&pool, run).await?;
    store
        .projection_log()
        .delete_images(
            partition,
            fanout,
            &[ProjectionImageKey {
                run_key: run,
                transition_seq: new_state.transition_seq,
            }],
        )
        .await?;
    forget_visibility_run(&pool, run).await?;
    store.shutdown().await?;
    let LoadedRun::Existing(loaded) = loaded? else {
        anyhow::bail!("the run loads")
    };
    ensure!(
        loaded.memo == new_state.memo,
        "the state still contains the whole memo"
    );
    ensure!(
        loaded.used_worker_deployment_versions == Some(vec!["deployment.build".to_owned()]),
        "the seed joins the whole image before decoding its attributes"
    );
    Ok(())
}

/// The cluster's store with Tokeira's schema, and a pool for checking its rows, or
/// none when the live suite isn't configured.
async fn connect() -> Result<Option<(PgPool, DsqlStore)>> {
    let Ok(url) = std::env::var("TOKEIRA_DSQL_TEST_DATABASE_URL") else {
        return Ok(None);
    };
    let pool = PgPoolOptions::new()
        .max_connections(2)
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
            ..DsqlPoolConfig::default()
        },
    )
    .await?;
    store.migration_runner().apply(&pool).await?;
    Ok(Some((pool, store)))
}

// Feature: large-value-parts, exploratory bug condition checking: visibility image.
// The 1.25 MiB memo fits the 2 MiB admission limit, but makes context_data exceed
// DSQL's 1 MiB value limit (SQLSTATE 54000 before visibility parts). State parts
// already exist; an empty history isolates the unsplit projection image.
#[tokio::test]
async fn dsql_a_visibility_image_over_one_mib_commits_and_reads_back() -> Result<()> {
    let Some((pool, store)) = connect().await? else {
        return Ok(());
    };
    let run_key = RunKey::new();
    let checked = async {
        let mut transition = fresh_transition(run_key);
        transition.growth_limits = Some(RunGrowthLimits::default());
        transition
            .next_state
            .memo
            .0
            .insert("large".to_owned(), Payload::new(noise(MIB + MIB / 4, 1)));
        ensure!(
            memo_encoded_len(&transition.next_state.memo) <= MEMO_SIZE_LIMIT_ERROR,
            "the memo must fit Tokeira's admission limit"
        );
        let context = prepare_workflow_projection(&transition.next_state, 0)?;
        let encoded = codec::encode_projection_context(&context)?;
        ensure!(encoded.len() > MIB, "the image must exceed 1 MiB");
        ensure!(
            codec::encode_workflow_state(&transition.next_state)?.len() + encoded.len() < 4 * MIB,
            "the state and image together must stay below the transaction budget"
        );
        let image_key = ProjectionImageKey {
            run_key,
            transition_seq: transition.next_state.transition_seq,
        };
        let committed = store
            .run_repository()
            .commit_transition(run_key, transition, WriteFence::Unfenced)
            .await
            .context("a commit with a visibility image over 1 MiB must succeed")?;
        ensure!(
            matches!(committed, CommitResult::Applied { .. }),
            "the commit did not apply: {committed:?}"
        );
        let (partition_id, fanout) = sqlx::query_as::<_, (i32, i16)>(
            "SELECT partition_id, fanout FROM projection_log WHERE run_key = $1",
        )
        .bind(run_key.0)
        .fetch_one(&pool)
        .await?;
        let partition_id = u32::try_from(partition_id)?;
        let fanout = u16::try_from(fanout)?;
        let page = store
            .projection_log()
            .read_page(
                partition_id,
                fanout,
                Some(ProjectionImageKey {
                    run_key,
                    transition_seq: TransitionSeq::ZERO,
                }),
                1,
            )
            .await;
        store
            .projection_log()
            .delete_images(partition_id, fanout, &[image_key])
            .await?;
        let page = page?;
        ensure!(page.images.len() == 1, "the image must read back");
        let image = &page.images[0];
        ensure!(
            image.run_key == run_key && image.transition_seq == image_key.transition_seq,
            "the page must return the committed image"
        );
        ensure!(
            codec::encode_projection_context(&image.context)? == encoded,
            "the visibility context must read back byte for byte, including its whole memo"
        );
        Ok(())
    }
    .await;
    let cleanup = async {
        for statement in [
            "DELETE FROM current_execution WHERE run_key = $1",
            "DELETE FROM workflow_dispatch WHERE run_key = $1",
            "DELETE FROM run_value_part WHERE run_key = $1",
            "DELETE FROM workflow_hot WHERE run_key = $1",
        ] {
            sqlx::query(statement)
                .bind(run_key.0)
                .execute(&pool)
                .await?;
        }
        anyhow::Ok(())
    }
    .await;
    pool.close().await;
    let shutdown = store.shutdown().await;
    checked.and(cleanup).and(shutdown)
}

#[tokio::test]
async fn dsql_nodes_over_the_part_size_read_back() -> Result<()> {
    if let Some((_, store)) = connect().await? {
        nodes_over_the_part_size_read_back(&store.chasm_node_repository()).await;
        store.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn dsql_requests_over_the_part_size_read_back() -> Result<()> {
    if let Some((pool, store)) = connect().await? {
        let requests =
            requests_over_the_part_size_read_back(&store.worker_compute_repository()).await;
        for (action_id, request) in &requests {
            assert_request_stored_exactly(&pool, *action_id, request).await?;
        }
        store.shutdown().await?;
    }
    Ok(())
}

/// Check that the action's row and `worker_compute_action_part` hold exactly
/// `request`: its parts join to it, and a request within the part size is stored
/// whole, with no count and no parts.
async fn assert_request_stored_exactly(
    pool: &PgPool,
    action_id: Uuid,
    request: &[u8],
) -> Result<()> {
    let (first, count) = sqlx::query_as::<_, (Vec<u8>, Option<i32>)>(
        "SELECT request_data, request_parts FROM worker_compute_action WHERE action_id = $1",
    )
    .bind(action_id)
    .fetch_one(pool)
    .await?;
    let parts = sqlx::query_as::<_, (i32, Vec<u8>)>(
        "SELECT part_no, data FROM worker_compute_action_part
         WHERE action_id = $1 ORDER BY part_no",
    )
    .bind(action_id)
    .fetch_all(pool)
    .await?;
    ensure!(
        (count.is_none() && parts.is_empty()) == (request.len() <= PART_BYTES),
        "a request of {} bytes is stored with count {count:?} and {} parts",
        request.len(),
        parts.len()
    );
    let stored = value_parts::join(|| format!("action {action_id}"), first, count, parts)?;
    ensure!(
        stored == request,
        "action {action_id} stores another request"
    );
    Ok(())
}

/// A new execution of one root node at its first transition, holding `data`.
async fn create(
    repo: &dyn ChasmNodeRepository,
    data: Option<Vec<u8>>,
) -> Result<(ExecutionKey, ChasmNode)> {
    let key = ExecutionKey::new(
        Uuid::new_v4().to_string(),
        "large-node",
        Uuid::new_v4().to_string(),
    );
    let root = ChasmNode {
        metadata: NodeMetadata::new(
            ARCHETYPE,
            Some(LifecycleState::Running),
            VersionedTransition::new(1, 1),
        ),
        data,
    };
    let current = CurrentRun {
        run_id: key.run_id.clone(),
        request_id: "large-node".to_owned(),
        status: LifecycleState::Running,
        vt_epoch: VersionedTransition::new(1, 1),
    };
    let outcome = repo
        .persist_new_execution(
            &key,
            ARCHETYPE,
            vec![NodeWrite {
                encoded_path: Vec::new(),
                node: root.clone(),
                expected: ExpectedVersion::Absent,
            }],
            current,
            None,
        )
        .await?;
    ensure!(
        outcome == NodePersistOutcome::Applied,
        "the start answered {outcome:?}"
    );
    Ok((key, root))
}

/// Persist the root node at its next transition, holding `data`, fenced on the
/// version it holds now.
async fn rewrite(
    repo: &dyn ChasmNodeRepository,
    key: &ExecutionKey,
    mut root: ChasmNode,
    data: Option<Vec<u8>>,
) -> Result<ChasmNode> {
    let vt = root.metadata.versioned_transition;
    let expected = ExpectedVersion::Vt(vt);
    root.metadata.versioned_transition =
        VersionedTransition::new(vt.namespace_failover_version, vt.transition_count + 1);
    root.data = data;
    let outcome = repo
        .persist_dirty(
            key,
            vec![NodeWrite {
                encoded_path: Vec::new(),
                node: root.clone(),
                expected,
            }],
        )
        .await?;
    ensure!(
        outcome == NodePersistOutcome::Applied,
        "the rewrite answered {outcome:?}"
    );
    Ok(root)
}

/// The UUID columns of an execution's key.
fn uuids(key: &ExecutionKey) -> Result<(Uuid, Uuid)> {
    Ok((
        Uuid::parse_str(&key.namespace_id)?,
        Uuid::parse_str(&key.run_id)?,
    ))
}

/// Check that the root's row and `chasm_node_part` hold exactly `root`'s data: its
/// parts join to the data, with no part beyond its count, and data within the
/// part size, or no data, is stored with no count and no parts.
async fn assert_root_stored_exactly(
    pool: &PgPool,
    key: &ExecutionKey,
    root: &ChasmNode,
) -> Result<()> {
    let (namespace_id, run_id) = uuids(key)?;
    let (first, count) = sqlx::query_as::<_, (Option<Vec<u8>>, Option<i32>)>(
        "SELECT data, data_parts FROM chasm_node
         WHERE namespace_id = $1 AND business_id = $2 AND run_id = $3 AND encoded_path = ''",
    )
    .bind(namespace_id)
    .bind(&key.business_id)
    .bind(run_id)
    .fetch_one(pool)
    .await?;
    let parts = sqlx::query_as::<_, (i32, Vec<u8>)>(
        "SELECT part_no, data FROM chasm_node_part
         WHERE namespace_id = $1 AND business_id = $2 AND run_id = $3 AND encoded_path = ''
         ORDER BY part_no",
    )
    .bind(namespace_id)
    .bind(&key.business_id)
    .bind(run_id)
    .fetch_all(pool)
    .await?;
    let Some(data) = &root.data else {
        ensure!(
            first.is_none() && count.is_none() && parts.is_empty(),
            "a root with no data is stored with count {count:?} and {} parts",
            parts.len()
        );
        return Ok(());
    };
    ensure!(
        (count.is_none() && parts.is_empty()) == (data.len() <= PART_BYTES),
        "data of {} bytes is stored with count {count:?} and {} parts",
        data.len(),
        parts.len()
    );
    let first = first.context("the root's data is stored without its first part")?;
    let stored = value_parts::join(|| "the root's data".to_owned(), first, count, parts)?;
    ensure!(&stored == data, "the root stores other data");
    Ok(())
}

/// A node's data length: around a multiple of the part size, up to the three
/// payloads of 2 MiB a standalone activity's node may hold; or no data.
fn data_len() -> impl Strategy<Value = Option<usize>> {
    prop_oneof![
        1 => Just(None),
        6 => (0usize..=6, -4_096isize..=4_096).prop_map(|(parts, offset)| {
            Some((parts * PART_BYTES).saturating_add_signed(offset).min(6 * MIB))
        }),
    ]
}

// Feature: large-value-parts, Property 4: A replaced value leaves only its own parts
#[test]
fn dsql_a_node_leaves_only_its_own_parts() {
    let runtime = Runtime::new().expect("a tokio runtime");
    let Some((pool, store)) = runtime
        .block_on(connect())
        .expect("the live fixture connects")
    else {
        return;
    };
    let repo = store.chasm_node_repository();
    let result = TestRunner::new(ProptestConfig {
        failure_persistence: None,
        ..ProptestConfig::with_cases(8)
    })
    .run(
        &prop::collection::vec(data_len(), 1..6),
        |lens| -> TestCaseResult {
            runtime
                .block_on(async {
                    let mut lens = lens.into_iter().enumerate();
                    let (_, first) = lens.next().expect("the case holds a length");
                    let (key, mut root) = create(&repo, first.map(|len| noise(len, 0))).await?;
                    assert_root_stored_exactly(&pool, &key, &root).await?;
                    for (index, len) in lens {
                        let data = len.map(|len| noise(len, index as u64));
                        root = rewrite(&repo, &key, root, data).await?;
                        assert_root_stored_exactly(&pool, &key, &root).await?;
                    }
                    repo.delete_execution(&key).await
                })
                .map_err(|error| TestCaseError::fail(format!("{error:?}")))
        },
    );
    runtime
        .block_on(store.shutdown())
        .expect("the store shuts down");
    result.expect("every write leaves exactly its node's parts");
}

const COUNT_NODES_SQL: &str = "SELECT count(*) FROM chasm_node
     WHERE namespace_id = $1 AND business_id = $2 AND run_id = $3";

const COUNT_PARTS_SQL: &str = "SELECT count(*) FROM chasm_node_part
     WHERE namespace_id = $1 AND business_id = $2 AND run_id = $3";

/// The rows of `key`'s execution that `sql`, one of the counts above, counts.
async fn count_rows(pool: &PgPool, sql: &'static str, key: &ExecutionKey) -> Result<usize> {
    let (namespace_id, run_id) = uuids(key)?;
    let (count,) = sqlx::query_as::<_, (i64,)>(sql)
        .bind(namespace_id)
        .bind(&key.business_id)
        .bind(run_id)
        .fetch_one(pool)
        .await?;
    Ok(usize::try_from(count)?)
}

// Feature: large-value-parts, Property 6: Removing a value removes its parts
/// Deleting an execution whose nodes hold data over the part size leaves none of
/// its nodes and none of their parts.
#[tokio::test]
async fn dsql_deleting_an_execution_removes_its_parts() -> Result<()> {
    let Some((pool, store)) = connect().await? else {
        return Ok(());
    };
    let repo = store.chasm_node_repository();
    let (key, _) = create(&repo, Some(noise(3 * MIB, 1))).await?;
    let mut lens = vec![3 * MIB];
    for (name, len) in [("child", 2 * MIB), ("sibling", 100)] {
        let node = ChasmNode {
            metadata: NodeMetadata::new(ARCHETYPE, None, VersionedTransition::new(1, 2)),
            data: Some(noise(len, len as u64)),
        };
        let outcome = repo
            .persist_dirty(
                &key,
                vec![NodeWrite {
                    encoded_path: path::encode(&[PathSegment::field(name)])?,
                    node,
                    expected: ExpectedVersion::Absent,
                }],
            )
            .await?;
        ensure!(
            outcome == NodePersistOutcome::Applied,
            "{name}: {outcome:?}"
        );
        lens.push(len);
    }
    let expected_parts = lens
        .iter()
        .map(|&len| value_parts::parts_of(len) - 1)
        .sum::<usize>();
    ensure!(
        count_rows(&pool, COUNT_PARTS_SQL, &key).await? == expected_parts,
        "the execution's nodes hold {expected_parts} parts after their first"
    );
    repo.delete_execution(&key).await?;
    ensure!(
        count_rows(&pool, COUNT_NODES_SQL, &key).await? == 0,
        "a node remains"
    );
    ensure!(
        count_rows(&pool, COUNT_PARTS_SQL, &key).await? == 0,
        "a part remains"
    );
    store.shutdown().await?;
    Ok(())
}

/// A load that met a node stored in parts, held before it reads the node again
/// with its parts while a persist replaces the data, returns one version of the
/// data whole. The two versions have three parts each and differ in every part, so
/// the parts of one joined to the first part of the other would fit together and
/// read as neither.
#[tokio::test]
async fn dsql_a_load_held_before_its_parts_reads_one_version() -> Result<()> {
    let Some((_, store)) = connect().await? else {
        return Ok(());
    };
    let repo = store.chasm_node_repository();
    let old = noise(2 * PART_BYTES + 1_000, 1);
    let new = noise(2 * PART_BYTES + 50_000, 2);
    let (key, root) = create(&repo, Some(old.clone())).await?;
    let (_, run_id) = uuids(&key)?;
    let (reached, resume) = pause_point::arm(PauseKey::NodeParts(run_id));
    let load = tokio::spawn({
        let repo = store.chasm_node_repository();
        let key = key.clone();
        async move { repo.load_execution(&key).await }
    });
    reached.await?;
    rewrite(&repo, &key, root, Some(new.clone())).await?;
    drop(resume);
    let loaded = load.await??;
    let data = loaded
        .first()
        .and_then(|(_, root)| root.data.as_ref())
        .context("the load read the root's data")?;
    ensure!(
        data == &old || data == &new,
        "the load read {} bytes that are neither version of the data",
        data.len()
    );
    repo.delete_execution(&key).await?;
    store.shutdown().await?;
    Ok(())
}
