//! Visibility values across the real kernel start, durable commit, projector read,
//! sink apply and List paths. The same contract runs in memory and, when configured,
//! against an operator-owned DSQL cluster; no engine provisioning is involved.

use anyhow::{Result, ensure};
use proptest::prelude::*;
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    BasicKernel, Command, ContinueAsNewAdvicePolicy, HistoryEventKind, Kernel, LoadedRun,
    StartRequest, WorkflowIdConflictPolicy, WorkflowIdReusePolicy,
    limits::{MEMO_SIZE_LIMIT_ERROR, RunGrowthLimits},
    payload_size::memo_encoded_len,
};
use tokeira_projection::{
    ApplyOutcome, CompiledFilter, InMemoryVisibilityStore, PageBounds, ProjectionSink, SortOrder,
    VisibilitySink, VisibilityStore,
};
use tokeira_storage::{
    CommitResult, InMemoryStore, ProjectionImageKey, ProjectionLog, ProjectionRecord,
    RunRepository, WriteFence, dsql::value_parts::PART_BYTES,
};
use tokeira_types::{
    Memo, NamespaceId, Payload, Payloads, RequestContext, RequestId, RunId, RunKey,
    SearchAttributes, TaskQueueName, TransitionSeq, WorkflowId, WorkflowType,
};
use tokio::runtime::Runtime;

/// Bytes no store can shrink: a SplitMix64 stream, so a value's stored size is
/// its length whatever a store compresses.
fn noise(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed ^ 0x9E37_79B9_7F4A_7C15;
    let mut bytes = Vec::with_capacity(len + 8);
    while bytes.len() < len {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        bytes.extend_from_slice(&(z ^ (z >> 31)).to_le_bytes());
    }
    bytes.truncate(len);
    bytes
}

fn memo(len: usize, seed: u64) -> Memo {
    Memo([("large".to_owned(), Payload::new(noise(len, seed)))].into())
}

fn admission_limit_memo() -> Memo {
    let mut memo = memo(MEMO_SIZE_LIMIT_ERROR, 7);
    while memo_encoded_len(&memo) != MEMO_SIZE_LIMIT_ERROR {
        let actual = memo_encoded_len(&memo);
        let data = &mut memo.0.get_mut("large").expect("the field").data;
        data.resize(data.len() + MEMO_SIZE_LIMIT_ERROR - actual, 17);
    }
    memo
}

fn sizes() -> impl Strategy<Value = (usize, u64)> {
    (
        prop_oneof![
            Just(0),
            (PART_BYTES - 4096)..=(PART_BYTES + 4096),
            Just(2 * PART_BYTES)
        ],
        any::<u64>(),
    )
}

fn start(memo: Memo) -> StartRequest {
    let namespace_id = NamespaceId::new();
    let workflow_id = WorkflowId("large-visibility".to_owned());
    let run_id = RunId::new();
    let now = OffsetDateTime::UNIX_EPOCH;
    StartRequest {
        run_key: RunKey::derive(namespace_id, &workflow_id, run_id),
        advice_policy: ContinueAsNewAdvicePolicy::V1_31_0,
        namespace_id,
        workflow_id,
        run_id,
        workflow_type: WorkflowType("large-visibility".to_owned()),
        task_queue: TaskQueueName("large-visibility".to_owned()),
        input: Payloads::default(),
        header: None,
        memo,
        search_attributes: SearchAttributes::default(),
        workflow_execution_timeout: None,
        workflow_run_timeout: None,
        workflow_task_timeout: Duration::seconds(10),
        retry_policy: None,
        conflict_policy: WorkflowIdConflictPolicy::Fail,
        reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
        deployment: None,
        build_id: None,
        versioning_override: None,
        workflow_start_delay: None,
        completion_callbacks: Vec::new(),
        user_metadata: None,
        links: Vec::new(),
        on_conflict_options: None,
        priority: None,
        initiator: None,
        attempt: 1,
        continued_execution_run_id: None,
        first_execution_run_id: None,
        parent_run_key: None,
        parent_workflow_id: None,
        parent_run_id: None,
        parent_namespace_id: None,
        parent_namespace_name: None,
        parent_initiated_event_id: 0,
        root_workflow_id: None,
        root_run_id: None,
        original_execution_run_id: None,
        continued_failure: None,
        last_completion_result: None,
        first_run_started_at: None,
        request: RequestContext {
            request_id: RequestId(namespace_id.0.to_string()),
            caller_identity: None,
            principal: None,
            received_at: now,
        },
        now,
        client_cron_schedule: None,
        cron_schedule: None,
        eager_execution_accepted: false,
        reserved_poller_identity: None,
        inherited_versioning_info: None,
    }
}

async fn round_trip(
    repo: &dyn RunRepository,
    log: &dyn ProjectionLog,
    visibility: &(impl VisibilityStore + ProjectionSink),
    memo: Memo,
) -> Result<ProjectionRecord> {
    ensure!(
        memo_encoded_len(&memo) <= MEMO_SIZE_LIMIT_ERROR,
        "the start's memo is admitted"
    );
    let request = start(memo.clone());
    let run_key = request.run_key;
    let mut transition = BasicKernel.apply(LoadedRun::Absent, Command::Start(request))?;
    transition.growth_limits = Some(RunGrowthLimits::default());
    let events = transition.history_events.clone();
    ensure!(
        matches!(&events[0].kind, HistoryEventKind::WorkflowExecutionStartedV2 { memo: stored, .. } if stored == &memo),
        "the real start event carries the memo"
    );
    let outcome = repo
        .commit_transition(run_key, transition, WriteFence::Unfenced)
        .await?;
    ensure!(
        matches!(outcome, CommitResult::Applied { .. }),
        "the real start commits"
    );
    let LoadedRun::Existing(state) = repo.load_run(run_key).await? else {
        anyhow::bail!("the started run loads")
    };
    ensure!(state.memo == memo, "the hot state preserves the whole memo");
    ensure!(
        repo.read_history_to_end(run_key, 0).await?.as_slice() == events.as_slice(),
        "the whole start event reads back"
    );
    let partition = (run_key.0.as_u128() as u32) % 16;
    let page = log
        .read_page(
            partition,
            1,
            Some(ProjectionImageKey {
                run_key,
                transition_seq: TransitionSeq::ZERO,
            }),
            1,
        )
        .await?;
    ensure!(
        page.images.len() == 1 && page.images[0].run_key == run_key,
        "the start's image reads back"
    );
    let image = page.images.into_iter().next().expect("one image");
    ensure!(
        image.context.memo == memo,
        "the image preserves the whole memo"
    );
    ensure!(
        visibility.apply(&image, partition).await? == ApplyOutcome::Advanced,
        "the image applies"
    );
    let listed = visibility
        .list_executions(
            state.namespace_id,
            &CompiledFilter {
                archetype: None,
                expr: None,
            },
            SortOrder::Default,
            &PageBounds {
                limit: 1,
                after: None,
            },
        )
        .await?;
    ensure!(
        listed.rows.len() == 1 && listed.rows[0].run_key == run_key,
        "the run lists"
    );
    ensure!(listed.rows[0].memo == memo, "List preserves the whole memo");
    Ok(image)
}

// Feature: large-value-parts, Property 3: A stored value reads back whole
proptest! {
    #![proptest_config(ProptestConfig::with_cases(8))]
    #[test]
    fn memory_visibility_values_read_back_whole((len, seed) in sizes()) {
        let runtime = Runtime::new().unwrap();
        runtime.block_on(async {
            let store = InMemoryStore::default();
            let visibility = VisibilitySink::new(InMemoryVisibilityStore::default());
            for len in [PART_BYTES - 4096, PART_BYTES + 4096, len] {
                round_trip(&store, &store, &visibility, memo(len, seed)).await.unwrap();
            }
        });
    }
}

// Feature: large-value-parts, Property 3: A stored value reads back whole
#[tokio::test]
async fn memory_a_start_with_a_memo_at_the_admission_limit_reads_back_whole() -> Result<()> {
    let store = InMemoryStore::default();
    let visibility = VisibilitySink::new(InMemoryVisibilityStore::default());
    round_trip(&store, &store, &visibility, admission_limit_memo()).await?;
    Ok(())
}

#[cfg(feature = "dsql-integration")]
mod live {
    use proptest::test_runner::{TestCaseError, TestRunner};
    use std::path::Path;
    use tokeira_storage::codec;

    use sqlx::PgPool;
    use tokeira_projection::DsqlVisibilityStore;
    use tokeira_storage::dsql::{
        DsqlPoolConfig, DsqlStore, MigrationConfig, ReservoirConfig, value_parts,
    };

    use super::*;

    async fn connect() -> Result<Option<(PgPool, DsqlStore, DsqlVisibilityStore)>> {
        let Ok(url) = std::env::var("TOKEIRA_DSQL_TEST_DATABASE_URL") else {
            return Ok(None);
        };
        let pool = PgPool::connect(&url).await?;
        let store = DsqlStore::from_database_url_for_tests(
            url,
            DsqlPoolConfig {
                migration: MigrationConfig {
                    migrations_dir: Path::new(env!("CARGO_MANIFEST_DIR"))
                        .join("../tokeira-storage/migrations"),
                },
                shard_count: 1,
                projection_partition_count: 16,
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
        let visibility = DsqlVisibilityStore::new(store.connection_director_arc());
        Ok(Some((pool, store, visibility)))
    }

    async fn check_storage(pool: &PgPool, image: &ProjectionRecord) -> Result<()> {
        let key = image.run_key.0;
        let (first, count) = sqlx::query_as::<_, (Vec<u8>, Option<i32>)>("SELECT context_data, context_parts FROM projection_log WHERE partition_id = $1 AND fanout = $2 AND run_key = $3 AND transition_seq = $4")
            .bind(i32::try_from(image.partition_id)?).bind(i16::try_from(image.fanout)?).bind(key).bind(i64::try_from(image.transition_seq.0)?).fetch_one(pool).await?;
        let rest = sqlx::query_as::<_, (i32, Vec<u8>)>("SELECT part_no, data FROM projection_image_part WHERE partition_id = $1 AND fanout = $2 AND run_key = $3 AND transition_seq = $4 ORDER BY part_no")
            .bind(i32::try_from(image.partition_id)?).bind(i16::try_from(image.fanout)?).bind(key).bind(i64::try_from(image.transition_seq.0)?).fetch_all(pool).await?;
        let encoded = codec::encode_projection_context(&image.context)?;
        ensure!(
            (count.is_none() && rest.is_empty()) == (encoded.len() <= PART_BYTES),
            "whole images have no count and no parts"
        );
        ensure!(
            value_parts::join(|| "test image".to_owned(), first, count, rest)? == encoded,
            "image parts are exact"
        );
        let (first, count) = sqlx::query_as::<_, (Vec<u8>, Option<i32>)>("SELECT memo_blob, memo_parts FROM execution_visibility_current WHERE namespace_id = $1 AND archetype_id = $2 AND run_key = $3")
            .bind(image.context.namespace_id.0).bind(i64::from(image.context.archetype_id.0)).bind(key).fetch_one(pool).await?;
        let rest = sqlx::query_as::<_, (i32, Vec<u8>)>("SELECT part_no, data FROM visibility_memo_part WHERE namespace_id = $1 AND archetype_id = $2 AND run_key = $3 ORDER BY part_no")
            .bind(image.context.namespace_id.0).bind(i64::from(image.context.archetype_id.0)).bind(key).fetch_all(pool).await?;
        let encoded = codec::encode(&image.context.memo)?;
        ensure!(
            (count.is_none() && rest.is_empty()) == (encoded.len() <= PART_BYTES),
            "whole memos have no count and no parts"
        );
        ensure!(
            value_parts::join(|| "test memo".to_owned(), first, count, rest)? == encoded,
            "memo parts are exact"
        );
        Ok(())
    }

    async fn cleanup(pool: &PgPool, store: &DsqlStore, image: &ProjectionRecord) -> Result<()> {
        store
            .projection_log()
            .delete_images(
                image.partition_id,
                image.fanout,
                &[ProjectionImageKey {
                    run_key: image.run_key,
                    transition_seq: image.transition_seq,
                }],
            )
            .await?;
        for sql in [
            "DELETE FROM visibility_memo_part WHERE run_key = $1",
            "DELETE FROM execution_visibility_current WHERE run_key = $1",
            "DELETE FROM history_batch WHERE run_key = $1",
            "DELETE FROM run_value_part WHERE run_key = $1",
            "DELETE FROM request_dedupe WHERE run_key = $1",
            "DELETE FROM current_execution WHERE run_key = $1",
            "DELETE FROM workflow_dispatch WHERE run_key = $1",
            "DELETE FROM workflow_hot WHERE run_key = $1",
        ] {
            sqlx::query(sql).bind(image.run_key.0).execute(pool).await?;
        }
        sqlx::query("DELETE FROM execution_visibility_rollup WHERE namespace_id = $1")
            .bind(image.context.namespace_id.0)
            .execute(pool)
            .await?;
        Ok(())
    }

    // Feature: large-value-parts, Property 3: A stored value reads back whole
    #[test]
    fn dsql_visibility_values_read_back_whole() {
        let runtime = Runtime::new().unwrap();
        let Some((pool, store, visibility)) = runtime.block_on(connect()).unwrap() else {
            return;
        };
        let result = TestRunner::new(ProptestConfig::with_cases(8)).run(&sizes(), |(len, seed)| {
            runtime
                .block_on(async {
                    for len in [PART_BYTES - 4096, PART_BYTES + 4096, len] {
                        let image = round_trip(
                            store.run_repository(),
                            store.projection_log(),
                            &visibility,
                            memo(len, seed),
                        )
                        .await?;
                        let checked = check_storage(&pool, &image).await;
                        cleanup(&pool, &store, &image).await?;
                        checked?;
                    }
                    Ok(())
                })
                .map_err(|error: anyhow::Error| TestCaseError::fail(format!("{error:?}")))
        });
        runtime.block_on(store.shutdown()).unwrap();
        result.unwrap();
    }

    // Feature: large-value-parts, Property 3: A stored value reads back whole
    #[tokio::test]
    async fn dsql_a_start_with_a_memo_at_the_admission_limit_reads_back_whole() -> Result<()> {
        let Some((pool, store, visibility)) = connect().await? else {
            return Ok(());
        };
        let memo = admission_limit_memo();
        ensure!(
            memo_encoded_len(&memo) == MEMO_SIZE_LIMIT_ERROR,
            "the memo is exactly at the wire admission limit"
        );
        let image = round_trip(
            store.run_repository(),
            store.projection_log(),
            &visibility,
            memo,
        )
        .await?;
        check_storage(&pool, &image).await?;
        cleanup(&pool, &store, &image).await?;
        store.shutdown().await
    }
}
