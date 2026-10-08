#![cfg(feature = "dsql-integration")]

//! Live lease fencing checks against an explicitly selected disposable database.

use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

use anyhow::{Context as _, Result, anyhow, ensure};
use proptest::{
    prelude::*,
    strategy::ValueTree,
    test_runner::{Config as ProptestConfig, TestRunner},
};
use sqlx::{PgPool, postgres::PgPoolOptions};
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{PendingWorkflowTask, Transition, WorkflowState};
use tokeira_storage::{
    CommitResult, CurrentExecutionConflictPolicy, LeaseOutcome, LeaseRepository, RunRepository,
    dsql::{DsqlPoolConfig, DsqlStore, MigrationConfig},
};
use tokeira_types::{
    ExecutionStatus, LogicalTaskSeq, Memo, NamespaceId, RunId, RunKey, SearchAttributes,
    ShardEpoch, ShardId, TaskQueueName, TransitionSeq, WorkflowId, WorkflowType,
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

/// Who holds a lease row a case starts with.
#[derive(Clone, Copy, Debug)]
enum Holder {
    Caller,
    Other,
    Nobody,
}

/// `cases` values of `strategy`, unshrunk: each case costs DSQL round trips, and
/// a failure names its case.
fn live_cases<S: Strategy>(strategy: S, cases: usize) -> Result<Vec<S::Value>> {
    let mut runner = TestRunner::new(ProptestConfig {
        failure_persistence: None,
        ..ProptestConfig::default()
    });
    (0..cases)
        .map(|_| {
            strategy
                .new_tree(&mut runner)
                .map(|tree| tree.current())
                .map_err(|reason| anyhow!("{reason}"))
        })
        .collect()
}

// Feature: on-conflict-row-counts, Property 1: A shard lease is decided from what was written
#[test]
fn a_shard_lease_is_decided_from_what_was_written() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let Some(context) = runtime.block_on(TestContext::connect())? else {
        return Ok(());
    };
    // Every lease row a case can start with: none, or a holder, live or expired,
    // at a generated epoch.
    let kinds = [
        None,
        Some((Holder::Caller, true)),
        Some((Holder::Caller, false)),
        Some((Holder::Other, true)),
        Some((Holder::Other, false)),
        Some((Holder::Nobody, true)),
        Some((Holder::Nobody, false)),
    ];
    let epochs = live_cases(1i64..4, kinds.len())?;
    for (kind, epoch) in kinds.into_iter().zip(epochs) {
        let row = kind.map(|(holder, live)| (holder, live, epoch));
        runtime
            .block_on(lease_case(&context, row))
            .with_context(|| format!("lease row {row:?}"))?;
    }
    Ok(())
}

async fn lease_case(context: &TestContext, row: Option<(Holder, bool, i64)>) -> Result<()> {
    let shard_id = next_shard();
    context.clear_lease(shard_id).await?;
    // Whole seconds, so a row the acquire didn't rewrite keeps exactly this expiry.
    let now = OffsetDateTime::from_unix_timestamp(OffsetDateTime::now_utc().unix_timestamp())?;
    let fixture_expiry = row.map(|(_, live, _)| {
        if live {
            now + Duration::minutes(5)
        } else {
            now - Duration::minutes(5)
        }
    });
    if let Some((holder, _, epoch)) = row {
        let owner = match holder {
            Holder::Caller => Some("caller"),
            Holder::Other => Some("other"),
            Holder::Nobody => None,
        };
        sqlx::query(
            "INSERT INTO shard_lease (shard_id, owner, epoch, lease_expiry, node_endpoint)
             VALUES ($1, $2, $3, $4, 'fixture')",
        )
        .bind(shard_id_to_uuid(shard_id))
        .bind(owner)
        .bind(epoch)
        .bind(fixture_expiry)
        .execute(&context.pool)
        .await?;
    }
    let outcome = context
        .store
        .run_repository()
        .try_acquire_bundle(shard_id, "caller".to_owned(), "127.0.0.1:7233".to_owned())
        .await?;
    let (owner, epoch, expiry) = sqlx::query_as::<_, (Option<String>, i64, OffsetDateTime)>(
        "SELECT owner, epoch, lease_expiry FROM shard_lease WHERE shard_id = $1",
    )
    .bind(shard_id_to_uuid(shard_id))
    .fetch_one(&context.pool)
    .await?;
    context.clear_lease(shard_id).await?;
    let epoch_of = |value: i64| u64::try_from(value).map(ShardEpoch);
    let expected = match row {
        None => LeaseOutcome::Acquired {
            epoch: ShardEpoch(1),
        },
        Some((Holder::Caller, true, held)) => LeaseOutcome::Acquired {
            epoch: epoch_of(held)?,
        },
        Some((Holder::Other, true, held)) => LeaseOutcome::Rejected {
            current_owner: "other".to_owned(),
            current_epoch: epoch_of(held)?,
        },
        // Expired, or held by no one: taken over at the next epoch.
        Some((_, _, held)) => LeaseOutcome::Acquired {
            epoch: epoch_of(held + 1)?,
        },
    };
    ensure!(
        outcome == expected,
        "answered {outcome:?}, expected {expected:?}"
    );
    match expected {
        LeaseOutcome::Acquired { epoch: acquired } => ensure!(
            owner.as_deref() == Some("caller")
                && epoch_of(epoch)? == acquired
                && Some(expiry) != fixture_expiry,
            "the stored lease is {owner:?} at {epoch}, expiring {expiry}, not the acquired one"
        ),
        _ => ensure!(
            owner.as_deref() == Some("other") && Some(expiry) == fixture_expiry,
            "a rejected acquire changed the lease: {owner:?} at {epoch}, expiring {expiry}"
        ),
    }
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
            // nextest starts this test in its crate directory, not the workspace root.
            migration: MigrationConfig {
                migrations_dir: Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations"),
            },
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
        let result = sqlx::query("UPDATE shard_lease SET lease_expiry = $1 WHERE shard_id = $2")
            .bind(OffsetDateTime::now_utc() - Duration::seconds(1))
            .bind(shard_id_to_uuid(shard_id))
            .execute(&self.pool)
            .await?;
        assert_eq!(
            result.rows_affected(),
            1,
            "the fixture must expire the acquired lease"
        );
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
    // Fixture SQL must match DsqlRunRepository::shard_id_to_uuid's reversible encoding.
    let mut bytes = *b"tokeira-shard-id";
    bytes[12..16].copy_from_slice(&shard_id.0.to_be_bytes());
    uuid::Uuid::from_bytes(bytes)
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
        events_numbered_at_close: 0,
        growth_limits: None,
    }
}

fn sample_state(run_key: RunKey) -> WorkflowState {
    WorkflowState {
        used_worker_deployment_versions: Some(Vec::new()),
        completed_update_count: 0,
        signal_count: 0,
        run_key,
        namespace_id: NamespaceId::new(),
        workflow_id: WorkflowId("workflow".to_owned()),
        run_id: RunId::new(),
        workflow_type: WorkflowType("workflow-type".to_owned()),
        task_queue: TaskQueueName("queue".to_owned()),
        deployment: None,
        build_id: None,
        versioning_info: None,
        worker_deployment_name: None,
        status: ExecutionStatus::Running,
        transition_seq: TransitionSeq(1),
        last_event_id: 0,
        external_payload_count: 0,
        external_payload_size_bytes: 0,
        next_workflow_task_seq: LogicalTaskSeq(1),
        pending_workflow_task: Some(PendingWorkflowTask {
            advice: Default::default(),
            task_type: tokeira_kernel::WorkflowTaskType::Normal,
            logical_seq: LogicalTaskSeq(1),
            scheduled_event_id: 1,
            scheduled_at: OffsetDateTime::now_utc(),
            started_event_id: None,
            started_at: None,
            attempt: 1,
            schedule_to_start_deadline: None,
            target_worker_deployment_version_changed: false,
            target_version_changed_enabled: false,
            target_deployment_version: None,
        }),
        previous_started_event_id: 0,
        workflow_task_attempt: 1,
        workflow_task_attempts_since_last_success: 0,
        last_workflow_task_problem: None,
        sticky: None,
        pause_info: None,
        cancel_requested: false,
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
        root_workflow_id: None,
        root_run_id: None,
        last_completion_result: None,
        activities: Default::default(),
        timers: Default::default(),
        children: Default::default(),
        pending_external_signals: Default::default(),
        pending_external_cancels: Default::default(),
        pending_updates: Default::default(),
        admitted_updates: Default::default(),
        pending_nexus_operations: Default::default(),
        completion_callbacks: Vec::new(),
        user_metadata: None,
        links: Vec::new(),
        workflow_start_delay: None,
        priority: None,
        started_at: OffsetDateTime::now_utc(),
        first_run_started_at: None,
        closed_at: None,
        close_result: None,
        close_failure: None,
        request_id_infos: std::collections::BTreeMap::new(),
        buffered_events: Vec::new(),
        auto_reset_points: Vec::new(),
    }
}
