#![cfg(feature = "dsql-integration")]

//! Opt-in load on a live cluster through the production connection factory:
//! a burst of operations through a small reservoir, and a connection storm from
//! several reservoirs refilling at once.
//!
//! Both need `TOKEIRA_DSQL_TEST_ENDPOINT` and `TOKEIRA_DSQL_TEST_REGION`; an
//! unset gate returns without connecting. Nothing here creates schema. The
//! storm opens thousands of connections and bypasses the cluster-wide rate
//! limiter on purpose, so point it only at a throwaway cluster. Each test
//! prints what it measured.

#![allow(clippy::print_stderr)] // A live probe reports its measurements.

use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::Result;
use async_trait::async_trait;
use sqlx::PgConnection;
use tokeira_storage::{
    ConnectionDirector, DbClass, PersistenceCapacityExhausted,
    dsql::{
        ConnectionFactory, ConnectionFactoryError, DistributedTokenBucket, DsqlConnectionDirector,
        DsqlPoolConfig, PhysicalConnectionFactory, Reservoir, ReservoirConfig, SlotBlockManager,
        in_operation,
    },
};
use tokio::task::JoinSet;

const CLASSES: [DbClass; 5] = [
    DbClass::Control,
    DbClass::Commit,
    DbClass::Read,
    DbClass::Projection,
    DbClass::Maintenance,
];

fn live_cluster() -> Option<(String, String)> {
    Some((
        std::env::var("TOKEIRA_DSQL_TEST_ENDPOINT").ok()?,
        std::env::var("TOKEIRA_DSQL_TEST_REGION").ok()?,
    ))
}

/// The production factory, counting how each connect ended.
#[derive(Debug)]
struct Counted {
    inner: ConnectionFactory,
    created: AtomicUsize,
    failures: Mutex<BTreeMap<String, usize>>,
}

impl Counted {
    fn new(endpoint: &str, region: &str) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            inner: ConnectionFactory::new(endpoint, region)?,
            created: AtomicUsize::new(0),
            failures: Mutex::new(BTreeMap::new()),
        }))
    }

    fn failures(&self) -> BTreeMap<String, usize> {
        self.failures
            .lock()
            .expect("the tally is never poisoned")
            .clone()
    }
}

#[async_trait]
impl PhysicalConnectionFactory for Counted {
    async fn create_connection(&self) -> Result<PgConnection, ConnectionFactoryError> {
        let created = self.inner.create_connection().await;
        match &created {
            Ok(_) => {
                self.created.fetch_add(1, Ordering::AcqRel);
            }
            Err(error) => {
                // Keep the SQLSTATE and the server's words; drop the rest.
                let message = error.to_string();
                let key = message
                    .find("SQLSTATE")
                    .map_or_else(|| message.clone(), |at| message[at..].to_owned());
                *self
                    .failures
                    .lock()
                    .expect("the tally is never poisoned")
                    .entry(key)
                    .or_default() += 1;
            }
        }
        created
    }
}

fn config(target_ready: usize, inflight_limit: usize) -> ReservoirConfig {
    ReservoirConfig {
        target_ready,
        inflight_limit,
        ..ReservoirConfig::default()
    }
}

/// A reservoir over `factory` with process-local rate and slot accounting.
/// The rate is generous enough that only the cluster itself limits connects;
/// the reservoir may hold `slots` connections at once.
async fn reservoir(
    factory: &Arc<Counted>,
    config: ReservoirConfig,
    slots: usize,
) -> Result<Reservoir> {
    Reservoir::start(
        config,
        Arc::clone(factory) as Arc<dyn PhysicalConnectionFactory>,
        Arc::new(DistributedTokenBucket::local_for_tests(10_000.0, 10_000)),
        SlotBlockManager::local_for_tests(slots),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_burst_through_a_small_reservoir_never_finds_it_empty() -> Result<()> {
    let Some((endpoint, region)) = live_cluster() else {
        return Ok(());
    };
    let factory = Counted::new(&endpoint, &region)?;
    let reservoir_config = config(10, 8);
    // Returned connections may briefly lift the reservoir to twice its target.
    let reservoir = reservoir(&factory, reservoir_config.clone(), 2 * 10 + 8).await?;
    let director = Arc::new(DsqlConnectionDirector::start(
        DsqlPoolConfig {
            reservoir: reservoir_config,
            ..DsqlPoolConfig::default()
        },
        reservoir,
    )?);

    let started = Instant::now();
    let mut workers: JoinSet<Result<usize>> = JoinSet::new();
    for worker in 0..64 {
        let director = Arc::clone(&director);
        let class = CLASSES[worker % CLASSES.len()];
        workers.spawn(async move {
            let mut empty = 0usize;
            for _ in 0..50 {
                let operation: Result<()> =
                    in_operation(tracing::debug_span!("dsql.live_burst"), async {
                        let mut permit = director.acquire(class).await?;
                        sqlx::query("SELECT 1")
                            .execute(permit.connection()?)
                            .await?;
                        Ok(())
                    })
                    .await;
                match operation {
                    Ok(()) => {}
                    Err(error) if error.is::<PersistenceCapacityExhausted>() => empty += 1,
                    Err(error) => return Err(error),
                }
            }
            Ok(empty)
        });
    }
    let mut empty = 0;
    while let Some(worker) = workers.join_next().await {
        empty += worker??;
    }
    let elapsed = started.elapsed();
    eprintln!(
        "burst: 3200 operations in {elapsed:?}, {empty} refused for an empty reservoir, {} \
         connections made, failed connects {:?}",
        factory.created.load(Ordering::Acquire),
        factory.failures()
    );
    assert_eq!(
        empty, 0,
        "the class budgets admitted work the reservoir could not serve"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_connection_storm_refills_every_reservoir_without_leaking_a_slot() -> Result<()> {
    let Some((endpoint, region)) = live_cluster() else {
        return Ok(());
    };

    // The same refill one connect at a time, which is how the refiller worked
    // before it made connections in parallel.
    let factory = Counted::new(&endpoint, &region)?;
    let serial = reservoir(&factory, config(50, 1), 50).await?;
    serial.retire_excess(1);
    let started = Instant::now();
    serial.warmup().await?;
    let serial_refill = started.elapsed();
    drop(serial);

    // Eight reservoirs, eight connects in flight each, retiring and refilling
    // fifty connections five times over: thousands of connects in seconds,
    // past the cluster's connection burst. Each reservoir has exactly the
    // slots its target needs, so a slot that a retirement, a refused connect
    // or a timed-out connect failed to give back stalls the next refill and
    // its warmup times out.
    let factory = Counted::new(&endpoint, &region)?;
    let mut storm: JoinSet<Result<Vec<Duration>>> = JoinSet::new();
    for _ in 0..8 {
        let factory = Arc::clone(&factory);
        storm.spawn(async move {
            let reservoir = reservoir(&factory, config(50, 8), 50).await?;
            let mut refills = Vec::new();
            for _ in 0..5 {
                reservoir.retire_excess(1);
                let started = Instant::now();
                reservoir.warmup().await?;
                refills.push(started.elapsed());
            }
            reservoir.shutdown().await?;
            Ok(refills)
        });
    }
    let started = Instant::now();
    let mut refills = Vec::new();
    while let Some(reservoir) = storm.join_next().await {
        refills.extend(reservoir??);
    }
    let elapsed = started.elapsed();
    refills.sort();
    eprintln!(
        "storm: serial refill of 49 took {serial_refill:?}; 8 reservoirs x 5 refills of 49 in \
         {elapsed:?}, {} connections made; refill median {:?}, max {:?}; failed connects {:?}",
        factory.created.load(Ordering::Acquire),
        refills[refills.len() / 2],
        refills[refills.len() - 1],
        factory.failures()
    );
    Ok(())
}
