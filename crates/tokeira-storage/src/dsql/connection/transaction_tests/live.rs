//! Real SQLx startup cancellation on a one-slot reservoir. The coordinator
//! observes retirement without replacing the production return processor.

use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Waker},
    time::{Duration, Instant},
};

use anyhow::{Result, ensure};
use async_trait::async_trait;
use sqlx::{PgConnection, postgres::PgPoolOptions};
use uuid::Uuid;

use crate::{
    ConnectionDirector, DbClass,
    contract_tests::required_url,
    dsql::{
        DatabaseUrlConnectionFactory, DsqlConnectionDirector, DsqlPermit, MigrationRunner,
        ReservoirConfig,
        connection_coordinator::{
            EmbeddedConnectionCoordinator, ProcessLocalConnectionCoordinator,
        },
        embedded_reservoir::EmbeddedReservoir,
    },
};

#[derive(Debug)]
struct CountedCoordinator {
    inner: ProcessLocalConnectionCoordinator,
    creations: AtomicUsize,
    releases: AtomicUsize,
}

#[async_trait]
impl EmbeddedConnectionCoordinator for CountedCoordinator {
    async fn validate(&self) -> Result<()> {
        self.inner.validate().await
    }
    async fn acquire_slot(&self) -> Result<()> {
        self.inner.acquire_slot().await
    }
    async fn acquire_creation_token(&self) -> Result<()> {
        self.inner.acquire_creation_token().await?;
        self.creations.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn release_slot(&self) {
        self.releases.fetch_add(1, Ordering::SeqCst);
        self.inner.release_slot();
    }
    fn used_slots(&self) -> usize {
        self.inner.used_slots()
    }
    async fn shutdown(&self) -> Result<()> {
        self.inner.shutdown().await
    }
}

fn cancel_pending(start: impl Future) -> Result<()> {
    let mut start = Box::pin(start);
    ensure!(
        start
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending(),
        "BEGIN completed on its first poll; cancellation window was not exercised"
    );
    Ok(())
}

async fn insert_owned(connection: &mut PgConnection, key: Uuid) -> Result<()> {
    sqlx::query(
        "INSERT INTO request_dedupe
         (key, namespace_id, workflow_id, request_id, run_key, run_id, first_seen_transition_seq)
         VALUES ($1, $1, 'transaction-start-contract', $2, $1, $1, 1)",
    )
    .bind(key)
    .bind(key.to_string())
    .execute(connection)
    .await?;
    Ok(())
}

async fn count_owned(connection: &mut PgConnection, key: Uuid) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT COUNT(*) FROM request_dedupe WHERE key = $1")
            .bind(key)
            .fetch_one(connection)
            .await?,
    )
}

async fn preserve_transactions(permit: &mut DsqlPermit, key: Uuid) -> Result<()> {
    let mut dropped = permit.begin().await?;
    insert_owned(&mut dropped, key).await?;
    drop(dropped);
    let mut read = permit.begin_read_only().await?;
    ensure!(
        count_owned(&mut read, key).await? == 0,
        "dropped write was not rolled back"
    );
    read.commit().await?;
    let mut write = permit.begin().await?;
    insert_owned(&mut write, key).await?;
    write.commit().await?;
    let mut read = permit.begin_read_only().await?;
    ensure!(
        count_owned(&mut read, key).await? == 1,
        "committed write was not visible"
    );
    read.commit().await?;
    let mut write = permit.begin().await?;
    sqlx::query("DELETE FROM request_dedupe WHERE key = $1")
        .bind(key)
        .execute(&mut *write)
        .await?;
    write.commit().await?;
    Ok(())
}

// Feature: storage-contract-tests, Property 10: A cancelled BEGIN cannot contaminate the next borrower.
#[tokio::test]
#[ignore = "requires an explicitly configured ephemeral DSQL database"]
async fn dsql_transaction_start_cancellation_and_preservation() -> Result<()> {
    let url = required_url(std::env::var("TOKEIRA_DSQL_TEST_DATABASE_URL").ok())?;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .idle_timeout(None)
        .connect(&url)
        .await?;
    MigrationRunner::embedded().apply(&pool).await?;
    let config = ReservoirConfig {
        target_ready: 1,
        inflight_limit: 1,
        ..Default::default()
    };
    let coordinator = Arc::new(CountedCoordinator {
        inner: ProcessLocalConnectionCoordinator::new(1, 10.0, 1)?,
        creations: AtomicUsize::new(0),
        releases: AtomicUsize::new(0),
    });
    let reservoir = EmbeddedReservoir::start_with_deadline(
        config.clone(),
        Arc::new(DatabaseUrlConnectionFactory::new(url)),
        coordinator.clone(),
        Instant::now() + Duration::from_secs(30),
    )
    .await?;
    let director = DsqlConnectionDirector::start_embedded(&config, reservoir)?;
    let key = Uuid::new_v4();
    let result = async {
        let mut permit = director.acquire(DbClass::Read).await?;
        drop(permit.begin_read_only());
        ensure!(
            !permit.marked_bad,
            "an unpolled BEGIN changed permit health"
        );
        for (index, read_only) in [true, false].into_iter().enumerate() {
            // A manual first poll reaches SQLx's real socket wait. Cancellation
            // is driven by Pending, not a timer racing network latency.
            if read_only {
                cancel_pending(permit.begin_read_only())?;
            } else {
                cancel_pending(permit.begin())?;
            }
            ensure!(
                permit.marked_bad,
                "cancelled startup left the permit reusable"
            );
            ensure!(
                coordinator.used_slots() == 1,
                "startup guard released the slot early"
            );
            drop(permit);
            // With one physical slot, this awaited checkout cannot complete
            // until the return processor retires and the refiller replaces it.
            permit = director.acquire(DbClass::Commit).await?;
            ensure!(coordinator.creations.load(Ordering::SeqCst) == index + 2);
            ensure!(coordinator.releases.load(Ordering::SeqCst) == index + 1);
            ensure!(coordinator.used_slots() == 1);
            let mut write = permit.begin().await?;
            sqlx::query("SELECT 1 FROM workflow_hot WHERE false FOR UPDATE")
                .fetch_all(&mut *write)
                .await?;
            write.rollback().await?;
        }
        preserve_transactions(&mut permit, key).await?;
        ensure!(
            !permit.marked_bad,
            "successful transactions poisoned the permit"
        );
        drop(permit);
        let mut permit = director.acquire(DbClass::Read).await?;
        ensure!(
            coordinator.creations.load(Ordering::SeqCst) == 3,
            "healthy return replaced its connection"
        );
        ensure!(coordinator.releases.load(Ordering::SeqCst) == 2);
        permit.mark_bad();
        permit.begin_read_only().await?.commit().await?;
        permit.begin().await?.rollback().await?;
        ensure!(
            permit.marked_bad,
            "successful startup cleared a prior bad mark"
        );
        drop(permit);
        let permit = director.acquire(DbClass::Read).await?;
        ensure!(coordinator.creations.load(Ordering::SeqCst) == 4);
        ensure!(coordinator.releases.load(Ordering::SeqCst) == 3);
        drop(permit);
        Ok::<(), anyhow::Error>(())
    }
    .await;
    let shutdown = director.shutdown().await;
    let cleanup = async {
        sqlx::query("DELETE FROM request_dedupe WHERE key = $1")
            .bind(key)
            .execute(&pool)
            .await?;
        let remaining: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM request_dedupe WHERE key = $1")
                .bind(key)
                .fetch_one(&pool)
                .await?;
        ensure!(remaining == 0, "owned transaction fixture was not cleaned");
        Ok::<(), anyhow::Error>(())
    }
    .await;
    pool.close().await;
    let result = [
        ("connection shutdown", shutdown),
        ("fixture cleanup", cleanup),
    ]
    .into_iter()
    .fold(result, |result, (step, cleanup)| match (result, cleanup) {
        (Err(error), Err(cleanup)) => {
            Err(error.context(format!("{step} also failed: {cleanup:#}")))
        }
        (Err(error), _) | (_, Err(error)) => Err(error),
        _ => Ok(()),
    });
    result?;
    ensure!(coordinator.used_slots() == 0);
    ensure!(coordinator.creations.load(Ordering::SeqCst) == 4);
    ensure!(
        coordinator.releases.load(Ordering::SeqCst) == 4,
        "physical slots were not released exactly once"
    );
    Ok(())
}
