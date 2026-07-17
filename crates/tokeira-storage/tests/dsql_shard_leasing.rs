#![cfg(feature = "dsql-integration")]

use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

use anyhow::Result;
use sqlx::{PgPool, postgres::PgPoolOptions};
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{PendingWorkflowTask, Transition, WorkflowState};
use tokeira_storage::{
    CommitResult, CurrentExecutionConflictPolicy, LeaseOutcome, LeaseRepository, RunRepository,
    dsql::{DsqlPoolConfig, DsqlStore},
};
use tokeira_types::{
    ExecutionStatus, LogicalTaskSeq, Memo, NamespaceId, RunId, RunKey, SearchAttributes,
    ShardEpoch, ShardId, TaskQueueName, TransitionSeq, WorkflowId, WorkflowType, dsql_spread_uuid,
};

static NEXT_SHARD: AtomicU32 = AtomicU32::new(20_000);

#[tokio::test]
async fn acquire_renew_expire_takeover_cycle() -> Result<()> {
    let Some(context) = TestContext::connect().await? else {
        return Ok(());
    };
    let shard_id = next_shard();
    context.clear_lease(shard_id).await?;

    assert_eq!(
        context
            .store
            .run_repository()
            .try_acquire_bundle(shard_id, "owner-a".to_owned(), "127.0.0.1:7233".to_owned())
            .await?,
        LeaseOutcome::Acquired {
            epoch: ShardEpoch(1)
        }
    );
    assert_eq!(
        context
            .store
            .run_repository()
            .renew_bundle(
                shard_id,
                "owner-a".to_owned(),
                ShardEpoch(1),
                "127.0.0.1:7233".to_owned(),
            )
            .await?,
        LeaseOutcome::Renewed {
            epoch: ShardEpoch(1)
        }
    );

    context.expire_lease(shard_id).await?;

    assert_eq!(
        context
            .store
            .run_repository()
            .try_acquire_bundle(shard_id, "owner-b".to_owned(), "127.0.0.1:7234".to_owned())
            .await?,
        LeaseOutcome::Acquired {
            epoch: ShardEpoch(2)
        }
    );
    Ok(())
}

#[tokio::test]
async fn concurrent_first_acquire_has_single_winner() -> Result<()> {
    let Some(context) = TestContext::connect().await? else {
        return Ok(());
    };
    let shard_id = next_shard();
    context.clear_lease(shard_id).await?;

    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let first = spawn_acquire(
        Arc::clone(&context.store),
        Arc::clone(&barrier),
        shard_id,
        "owner-a",
    );
    let second = spawn_acquire(Arc::clone(&context.store), barrier, shard_id, "owner-b");

    let first = first.await?;
    let second = second.await?;
    let outcomes = [first, second];
    let acquired = outcomes
        .iter()
        .filter(|outcome| matches!(outcome, Ok(LeaseOutcome::Acquired { .. })))
        .count();

    assert_eq!(acquired, 1);
    let row = context
        .read_lease(shard_id)
        .await?
        .expect("lease row exists");
    assert_eq!(row.1, 1);
    Ok(())
}

#[tokio::test]
async fn stale_epoch_is_fenced_after_expired_takeover() -> Result<()> {
    let Some(context) = TestContext::connect_with_shard_count(1).await? else {
        return Ok(());
    };
    let shard_id = ShardId(0);
    context.clear_lease(shard_id).await?;

    assert_eq!(
        context
            .store
            .run_repository()
            .try_acquire_bundle(shard_id, "owner-a".to_owned(), "127.0.0.1:7233".to_owned())
            .await?,
        LeaseOutcome::Acquired {
            epoch: ShardEpoch(1)
        }
    );
    context.expire_lease(shard_id).await?;
    assert_eq!(
        context
            .store
            .run_repository()
            .try_acquire_bundle(shard_id, "owner-b".to_owned(), "127.0.0.1:7234".to_owned())
            .await?,
        LeaseOutcome::Acquired {
            epoch: ShardEpoch(2)
        }
    );

    let run_key = RunKey::new();
    let stale = context
        .store
        .run_repository()
        .commit_transition(run_key, sample_transition(run_key), ShardEpoch(1))
        .await?;
    assert!(matches!(stale, CommitResult::Conflict { .. }));

    let current = context
        .store
        .run_repository()
        .commit_transition(run_key, sample_transition(run_key), ShardEpoch(2))
        .await?;
    assert!(matches!(current, CommitResult::Applied { .. }));
    Ok(())
}

#[tokio::test]
async fn active_same_owner_reacquire_is_idempotent() -> Result<()> {
    let Some(context) = TestContext::connect().await? else {
        return Ok(());
    };
    let shard_id = next_shard();
    context.clear_lease(shard_id).await?;

    assert_eq!(
        context
            .store
            .run_repository()
            .try_acquire_bundle(shard_id, "owner-a".to_owned(), "127.0.0.1:7233".to_owned())
            .await?,
        LeaseOutcome::Acquired {
            epoch: ShardEpoch(1)
        }
    );
    assert_eq!(
        context
            .store
            .run_repository()
            .try_acquire_bundle(shard_id, "owner-a".to_owned(), "127.0.0.1:7233".to_owned())
            .await?,
        LeaseOutcome::Acquired {
            epoch: ShardEpoch(1)
        }
    );
    assert_eq!(
        context
            .store
            .run_repository()
            .renew_bundle(
                shard_id,
                "owner-a".to_owned(),
                ShardEpoch(1),
                "127.0.0.1:7233".to_owned(),
            )
            .await?,
        LeaseOutcome::Renewed {
            epoch: ShardEpoch(1)
        }
    );
    Ok(())
}

fn spawn_acquire(
    store: Arc<DsqlStore>,
    barrier: Arc<tokio::sync::Barrier>,
    shard_id: ShardId,
    owner: &'static str,
) -> tokio::task::JoinHandle<Result<LeaseOutcome>> {
    tokio::spawn(async move {
        barrier.wait().await;
        store
            .run_repository()
            .try_acquire_bundle(shard_id, owner.to_owned(), "127.0.0.1:7233".to_owned())
            .await
    })
}

#[derive(Debug)]
struct TestContext {
    pool: PgPool,
    store: Arc<DsqlStore>,
}

impl TestContext {
    async fn connect() -> Result<Option<Self>> {
        Self::connect_with_shard_count(64).await
    }

    async fn connect_with_shard_count(shard_count: u32) -> Result<Option<Self>> {
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
            shard_count,
            conflict_policy: CurrentExecutionConflictPolicy::Reject,
            ..DsqlPoolConfig::default()
        };
        let store = DsqlStore::from_database_url_for_tests(url.clone(), config).await?;
        store.migration_runner().apply(&pool).await?;
        Ok(Some(Self {
            pool,
            store: Arc::new(store),
        }))
    }

    async fn clear_lease(&self, shard_id: ShardId) -> Result<()> {
        sqlx::query("DELETE FROM shard_lease WHERE shard_id = $1")
            .bind(shard_id_to_uuid(shard_id))
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn expire_lease(&self, shard_id: ShardId) -> Result<()> {
        sqlx::query("UPDATE shard_lease SET lease_expiry = $1 WHERE shard_id = $2")
            .bind(OffsetDateTime::now_utc() - Duration::seconds(1))
            .bind(shard_id_to_uuid(shard_id))
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn read_lease(&self, shard_id: ShardId) -> Result<Option<(String, i64)>> {
        Ok(sqlx::query_as::<_, (String, i64)>(
            "SELECT owner, epoch FROM shard_lease WHERE shard_id = $1",
        )
        .bind(shard_id_to_uuid(shard_id))
        .fetch_optional(&self.pool)
        .await?)
    }
}

fn shard_id_to_uuid(shard_id: ShardId) -> uuid::Uuid {
    dsql_spread_uuid(&[b"shard", &shard_id.0.to_le_bytes()])
}

fn next_shard() -> ShardId {
    ShardId(NEXT_SHARD.fetch_add(1, Ordering::Relaxed))
}

fn sample_transition(run_key: RunKey) -> Transition {
    Transition {
        expected_seq: TransitionSeq::ZERO,
        next_state: sample_state(run_key),
        history_events: Default::default(),
        event_principals: Default::default(),
        request_dedupe_ops: Default::default(),
        activity_ops: Default::default(),
        timer_ops: Default::default(),
        dispatch_ops: Default::default(),
        projection_ops: Default::default(),
    }
}

fn sample_state(run_key: RunKey) -> WorkflowState {
    WorkflowState {
        run_key,
        namespace_id: NamespaceId::new(),
        workflow_id: WorkflowId("workflow".to_owned()),
        run_id: RunId::new(),
        workflow_type: WorkflowType("workflow-type".to_owned()),
        task_queue: TaskQueueName("queue".to_owned()),
        deployment: None,
        build_id: None,
        status: ExecutionStatus::Running,
        transition_seq: TransitionSeq(1),
        last_event_id: 0,
        next_workflow_task_seq: LogicalTaskSeq(1),
        pending_workflow_task: Some(PendingWorkflowTask {
            logical_seq: LogicalTaskSeq(1),
            scheduled_event_id: 1,
            scheduled_at: OffsetDateTime::now_utc(),
            started_event_id: None,
            started_at: None,
            attempt: 1,
        }),
        previous_started_event_id: 0,
        workflow_task_attempt: 1,
        workflow_task_attempts_since_last_success: 0,
        last_workflow_task_problem: None,
        sticky: None,
        pause_info: None,
        wft_stamp: 0,
        memo: Memo::default(),
        search_attributes: SearchAttributes::default(),
        workflow_execution_timeout: None,
        workflow_run_timeout: None,
        workflow_task_timeout: Duration::seconds(10),
        retry_policy: None,
        attempt: 1,
        first_execution_run_id: None,
        original_execution_run_id: None,
        reset_run_id: None,
        parent_run_key: None,
        parent_workflow_id: None,
        parent_run_id: None,
        parent_namespace_id: None,
        parent_namespace_name: None,
        parent_initiated_event_id: 0,
        last_completion_result: None,
        activities: Default::default(),
        timers: Default::default(),
        children: Default::default(),
        pending_external_signals: Default::default(),
        pending_external_cancels: Default::default(),
        pending_updates: Default::default(),
        admitted_updates: Default::default(),
        pending_nexus_operations: Default::default(),
        versioning_override: None,
        completion_callbacks: Vec::new(),
        started_at: OffsetDateTime::now_utc(),
        first_run_started_at: None,
        closed_at: None,
        close_result: None,
        close_failure: None,
    }
}
