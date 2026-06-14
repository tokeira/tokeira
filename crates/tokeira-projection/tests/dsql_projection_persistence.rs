#![cfg(feature = "dsql-integration")]

use anyhow::Result;
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use time::OffsetDateTime;
use tokeira_projection::{
    DsqlVisibilityStore, ProjectionSink, VisibilityStore, workflow_status_keyword,
};
use tokeira_storage::{
    ProjectionContext, ProjectionLog, ProjectionRecord,
    dsql::{DsqlPoolConfig, DsqlStore, codec},
};
use tokeira_types::{
    ArchetypeId, ExecutionStatus, Memo, NamespaceId, Payload, ProjectionCursor, RunId, RunKey,
    SearchAttributes, TaskQueueName, TransitionSeq, VisibilityLifecycleState, WorkflowId,
    WorkflowType,
};
use uuid::Uuid;

#[tokio::test]
async fn read_from_paginates_projection_log_rows() -> Result<()> {
    let Some(context) = TestContext::connect().await? else {
        return Ok(());
    };
    let fixture = ProjectionFixture::new(10, 1);
    context.clear_projection_rows(&fixture.run_keys).await?;
    context.insert_projection_rows(&fixture).await?;

    let first = context
        .store
        .projection_log()
        .read_from(
            &ProjectionCursor::beginning(fixture.partition_id, fixture.fanout),
            2,
        )
        .await?;
    assert_eq!(first.records.len(), 2);
    assert_eq!(first.records[0].run_key, fixture.run_keys[0]);
    assert_eq!(first.records[1].run_key, fixture.run_keys[1]);

    let second = context
        .store
        .projection_log()
        .read_from(&first.next_cursor, 2)
        .await?;
    assert_eq!(second.records.len(), 1);
    assert_eq!(second.records[0].run_key, fixture.run_keys[2]);

    let third = context
        .store
        .projection_log()
        .read_from(&second.next_cursor, 2)
        .await?;
    assert!(third.records.is_empty());
    assert_eq!(third.next_cursor, second.next_cursor);
    Ok(())
}

#[tokio::test]
async fn checkpoint_persists_and_resumes_by_sink_id() -> Result<()> {
    let Some(context) = TestContext::connect().await? else {
        return Ok(());
    };
    let store = context.visibility_store().await?;
    let sink_id = format!("visibility-persistence-test-{}", Uuid::new_v4());
    let first = ProjectionCursor {
        partition_id: 2,
        fanout: 8,
        last_run_key: Some(RunKey(Uuid::from_u128(1))),
        last_transition_seq: Some(TransitionSeq(3)),
    };
    let second = ProjectionCursor {
        partition_id: 2,
        fanout: 8,
        last_run_key: Some(RunKey(Uuid::from_u128(2))),
        last_transition_seq: Some(TransitionSeq(9)),
    };

    assert_eq!(store.load_checkpoint(&sink_id).await?, None);
    store.save_checkpoint(&sink_id, &first).await?;
    assert_eq!(store.load_checkpoint(&sink_id).await?, Some(first));
    store.save_checkpoint(&sink_id, &second).await?;
    assert_eq!(store.load_checkpoint(&sink_id).await?, Some(second));
    Ok(())
}

#[tokio::test]
async fn visibility_sink_materializes_open_and_closed_execution_rows() -> Result<()> {
    let Some(context) = TestContext::connect().await? else {
        return Ok(());
    };
    let store = context.visibility_store().await?;
    let run_key = RunKey(Uuid::new_v4());
    context.clear_visibility_rows(&[run_key]).await?;
    let projection_context = sample_context(run_key);

    store
        .apply(&ProjectionRecord {
            partition_id: 0,
            fanout: 1,
            run_key,
            transition_seq: TransitionSeq(1),
            context: projection_context.clone(),
        })
        .await?;
    let open = context.read_visibility_row(run_key).await?.unwrap();
    assert_eq!(open.0, ExecutionStatus::Running.to_db_smallint());
    assert!(open.1.is_none());

    let closed_at = OffsetDateTime::from_unix_timestamp(1_000_000).unwrap();
    let closed_context = context_with_status(
        projection_context,
        ExecutionStatus::Completed,
        Some(closed_at),
    );
    store
        .apply(&ProjectionRecord {
            partition_id: 0,
            fanout: 1,
            run_key,
            transition_seq: TransitionSeq(2),
            context: closed_context,
        })
        .await?;
    let closed = context.read_visibility_row(run_key).await?.unwrap();
    assert_eq!(closed.0, ExecutionStatus::Completed.to_db_smallint());
    assert_eq!(closed.1, Some(closed_at));
    Ok(())
}

#[tokio::test]
async fn close_execution_can_insert_catch_up_visibility_row() -> Result<()> {
    let Some(context) = TestContext::connect().await? else {
        return Ok(());
    };
    let store = context.visibility_store().await?;
    let run_key = RunKey(Uuid::new_v4());
    context.clear_visibility_rows(&[run_key]).await?;
    let closed_at = OffsetDateTime::from_unix_timestamp(1_000_001).unwrap();

    store
        .apply(&ProjectionRecord {
            partition_id: 0,
            fanout: 1,
            run_key,
            transition_seq: TransitionSeq(1),
            context: context_with_status(
                sample_context(run_key),
                ExecutionStatus::Failed,
                Some(closed_at),
            ),
        })
        .await?;

    let row = context.read_visibility_row(run_key).await?.unwrap();
    assert_eq!(row.0, ExecutionStatus::Failed.to_db_smallint());
    assert_eq!(row.1, Some(closed_at));
    Ok(())
}

#[tokio::test]
async fn memo_merge_persists_across_visibility_updates() -> Result<()> {
    let Some(context) = TestContext::connect().await? else {
        return Ok(());
    };
    let store = context.visibility_store().await?;
    let run_key = RunKey(Uuid::new_v4());
    context.clear_visibility_rows(&[run_key]).await?;
    let mut first_context = sample_context(run_key);
    first_context.memo = memo_entries(&[("key_a", "payload_a")]);
    let mut second_context = sample_context(run_key);
    second_context.memo = memo_entries(&[("key_a", "payload_a"), ("key_b", "payload_b")]);
    let third_context = second_context.clone();

    store
        .apply(&ProjectionRecord {
            partition_id: 0,
            fanout: 1,
            run_key,
            transition_seq: TransitionSeq(1),
            context: first_context,
        })
        .await?;
    store
        .apply(&ProjectionRecord {
            partition_id: 0,
            fanout: 1,
            run_key,
            transition_seq: TransitionSeq(2),
            context: second_context,
        })
        .await?;
    store
        .apply(&ProjectionRecord {
            partition_id: 0,
            fanout: 1,
            run_key,
            transition_seq: TransitionSeq(3),
            context: third_context,
        })
        .await?;

    let memo = context.read_visibility_memo(run_key).await?.unwrap();
    assert_eq!(memo.0["key_a"].data, b"payload_a");
    assert_eq!(memo.0["key_b"].data, b"payload_b");
    Ok(())
}

#[derive(Debug)]
struct TestContext {
    pool: PgPool,
    store: DsqlStore,
}

impl TestContext {
    async fn connect() -> Result<Option<Self>> {
        let Some(url) = std::env::var("TOKEIRA_DSQL_TEST_DATABASE_URL")
            .ok()
            .or_else(|| std::env::var("DATABASE_URL").ok())
        else {
            return Ok(None);
        };
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect(&url)
            .await?;
        let config = DsqlPoolConfig {
            reservoir: tokeira_storage::dsql::ReservoirConfig {
                target_ready: 4,
                inflight_limit: 2,
                ..tokeira_storage::dsql::ReservoirConfig::default()
            },
            ..DsqlPoolConfig::default()
        };
        let store = DsqlStore::from_database_url_for_tests(url.clone(), config).await?;
        store.migration_runner().apply(&pool).await?;
        Ok(Some(Self { pool, store }))
    }

    async fn visibility_store(&self) -> Result<DsqlVisibilityStore> {
        Ok(DsqlVisibilityStore::new(
            self.store.connection_director_arc(),
        ))
    }

    async fn clear_projection_rows(&self, run_keys: &[RunKey]) -> Result<()> {
        for run_key in run_keys {
            sqlx::query("DELETE FROM projection_log WHERE run_key = $1")
                .bind(run_key.0)
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    async fn clear_visibility_rows(&self, run_keys: &[RunKey]) -> Result<()> {
        for run_key in run_keys {
            sqlx::query("DELETE FROM vis_execution WHERE run_key = $1")
                .bind(run_key.0)
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    async fn insert_projection_rows(&self, fixture: &ProjectionFixture) -> Result<()> {
        for (index, run_key) in fixture.run_keys.iter().enumerate() {
            sqlx::query(
                r#"
                INSERT INTO projection_log (
                    partition_id,
                    fanout,
                    run_key,
                    transition_seq,
                    context_data,
                    ops_data
                )
                VALUES ($1, $2, $3, $4, $5, $6)
                "#,
            )
            .bind(fixture.partition_id as i32)
            .bind(fixture.fanout as i16)
            .bind(run_key.0)
            .bind((index + 1) as i64)
            .bind(codec::encode_projection_context(&sample_context(*run_key))?)
            .bind(codec::encode_projection_ops(&[])?)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    async fn read_visibility_row(
        &self,
        run_key: RunKey,
    ) -> Result<Option<(i16, Option<OffsetDateTime>)>> {
        let row = sqlx::query(
            "SELECT execution_status, close_time FROM vis_execution WHERE run_key = $1",
        )
        .bind(run_key.0)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| Ok((row.try_get("execution_status")?, row.try_get("close_time")?)))
            .transpose()
    }

    async fn read_visibility_memo(&self, run_key: RunKey) -> Result<Option<Memo>> {
        let row = sqlx::query("SELECT memo FROM vis_execution WHERE run_key = $1")
            .bind(run_key.0)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| {
            let data: Vec<u8> = row.try_get("memo")?;
            codec::decode::<Memo>(&data)
        })
        .transpose()
    }
}

#[derive(Debug)]
struct ProjectionFixture {
    partition_id: u32,
    fanout: u16,
    run_keys: Vec<RunKey>,
}

impl ProjectionFixture {
    fn new(partition_id: u32, fanout: u16) -> Self {
        let run_keys = [1_u128, 2, 3]
            .into_iter()
            .map(|value| RunKey(Uuid::from_u128(value)))
            .collect();
        Self {
            partition_id,
            fanout,
            run_keys,
        }
    }
}

fn sample_context(run_key: RunKey) -> ProjectionContext {
    let run_id = RunId(Uuid::new_v4());
    let workflow_id = format!("workflow-{}", run_key.0);
    ProjectionContext {
        archetype_id: ArchetypeId::WORKFLOW,
        namespace_id: NamespaceId(Uuid::from_u128(1)),
        business_id: workflow_id.clone(),
        authority_epoch: 0,
        status_keyword: "Running".to_owned(),
        lifecycle_state: VisibilityLifecycleState::Open,
        workflow_id: WorkflowId(workflow_id.clone()),
        run_id,
        workflow_type: WorkflowType("workflow-type".to_owned()),
        task_queue: TaskQueueName("queue".to_owned()),
        execution_status: ExecutionStatus::Running,
        start_time: OffsetDateTime::from_unix_timestamp(100).unwrap(),
        update_time: OffsetDateTime::from_unix_timestamp(100).unwrap(),
        execution_time: None,
        close_time: None,
        history_length: 1,
        execution_duration: None,
        state_transition_count: 1,
        transition_count: 1,
        history_size_bytes: 0,
        parent_workflow_id: None,
        parent_run_id: None,
        root_workflow_id: Some(WorkflowId(workflow_id)),
        root_run_id: Some(run_id),
        search_attr_generation: 0,
        memo: Memo::default(),
        search_attributes: SearchAttributes::default(),
    }
}

fn context_with_status(
    mut context: ProjectionContext,
    status: ExecutionStatus,
    close_time: Option<OffsetDateTime>,
) -> ProjectionContext {
    context.execution_status = status;
    context.status_keyword = workflow_status_keyword(status);
    context.lifecycle_state = if close_time.is_some() {
        VisibilityLifecycleState::Closed
    } else {
        VisibilityLifecycleState::Open
    };
    context.close_time = close_time;
    context.update_time = close_time.unwrap_or(context.update_time);
    context
}

fn memo_entries(entries: &[(&str, &str)]) -> Memo {
    let mut memo = Memo::default();
    for (key, value) in entries {
        memo.0
            .insert((*key).to_owned(), Payload::new(value.as_bytes().to_vec()));
    }
    memo
}
