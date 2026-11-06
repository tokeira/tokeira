#![cfg(feature = "dsql-integration")]

//! Opt-in load on a live cluster through the production connection factory:
//! a burst of operations through a small reservoir, a connection storm from
//! several reservoirs refilling at once, and several reservoirs starting
//! together through one shared rate limiter table.
//!
//! All need `TOKEIRA_DSQL_TEST_ENDPOINT` and `TOKEIRA_DSQL_TEST_REGION`, and
//! the shared start also `TOKEIRA_DSQL_TEST_RATE_LIMITER_TABLE`, a DynamoDB
//! table in that Region keyed by the string `pk`; an unset gate returns without
//! connecting. Nothing here creates schema. The storm opens thousands of
//! connections and bypasses the cluster-wide rate limiter on purpose, so point
//! these only at a throwaway cluster, and run them one at a time: each spends
//! the cluster's connection burst. Each test prints what it measured.

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
        DsqlPoolConfig, PhysicalConnectionFactory, Reservoir, ReservoirConfig, WarmupOutcome,
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

    /// Connects the cluster refused for its connection rate.
    fn refused_for_rate(&self) -> usize {
        self.failures()
            .iter()
            .filter(|(key, _)| key.starts_with("SQLSTATE 53400"))
            .map(|(_, count)| count)
            .sum()
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

/// A reservoir over `factory` with a process-local rate limiter generous
/// enough that only the cluster itself limits connects.
async fn reservoir(factory: &Arc<Counted>, config: ReservoirConfig) -> Result<Reservoir> {
    Reservoir::start(
        config,
        Arc::clone(factory) as Arc<dyn PhysicalConnectionFactory>,
        Arc::new(DistributedTokenBucket::local_for_tests(10_000.0, 10_000)),
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
    let reservoir = reservoir(&factory, reservoir_config.clone()).await?;
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
async fn a_connection_storm_refills_every_reservoir_without_losing_a_place() -> Result<()> {
    let Some((endpoint, region)) = live_cluster() else {
        return Ok(());
    };

    // The same refill one connect at a time, which is how the refiller worked
    // before it made connections in parallel.
    let factory = Counted::new(&endpoint, &region)?;
    let serial = reservoir(&factory, config(50, 1)).await?;
    serial.retire_ready_for_tests(1);
    let started = Instant::now();
    assert!(matches!(serial.warmup().await, WarmupOutcome::Full { .. }));
    let serial_refill = started.elapsed();
    drop(serial);

    // Eight reservoirs, eight connects in flight each, retiring and refilling
    // fifty connections five times over: thousands of connects in seconds,
    // past the cluster's connection burst. A place in a live count that a
    // retirement, a refused connect or a timed-out connect failed to give
    // back would stall the next refill one short, and its warmup would come
    // back partial.
    let factory = Counted::new(&endpoint, &region)?;
    let mut storm: JoinSet<Result<Vec<Duration>>> = JoinSet::new();
    for _ in 0..8 {
        let factory = Arc::clone(&factory);
        storm.spawn(async move {
            let reservoir = reservoir(&factory, config(50, 8)).await?;
            let mut refills = Vec::new();
            for _ in 0..5 {
                reservoir.retire_ready_for_tests(1);
                let started = Instant::now();
                let outcome = reservoir.warmup().await;
                refills.push(started.elapsed());
                anyhow::ensure!(
                    matches!(outcome, WarmupOutcome::Full { .. }),
                    "a refill stopped short: {outcome:?}"
                );
                anyhow::ensure!(reservoir.live_count() == 50, "a place was lost");
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

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn reservoirs_sharing_a_rate_limiter_table_start_together_past_the_burst() -> Result<()> {
    let Some((endpoint, region)) = live_cluster() else {
        return Ok(());
    };
    let Ok(table) = std::env::var("TOKEIRA_DSQL_TEST_RATE_LIMITER_TABLE") else {
        return Ok(());
    };
    let aws = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(region.clone()))
        .load()
        .await;
    let client = aws_sdk_dynamodb::Client::new(&aws);

    // Twenty-four reservoirs of fifty: 1,200 connects, past the cluster's
    // burst of 1,000, so the last 200 wait for its rate.
    let factory = Counted::new(&endpoint, &region)?;
    let started = Instant::now();
    let mut nodes: JoinSet<Result<(Duration, Reservoir)>> = JoinSet::new();
    for _ in 0..24 {
        let factory = Arc::clone(&factory);
        let bucket =
            DistributedTokenBucket::new(client.clone(), table.clone(), &endpoint, 100.0, 1_000);
        nodes.spawn(async move {
            bucket.validate_table().await?;
            let reservoir = Reservoir::start(
                config(50, 8),
                factory as Arc<dyn PhysicalConnectionFactory>,
                Arc::new(bucket),
            )
            .await?;
            Ok((started.elapsed(), reservoir))
        });
    }
    let mut starts = Vec::new();
    let mut reservoirs = Vec::new();
    while let Some(node) = nodes.join_next().await {
        let (start, reservoir) = node??;
        starts.push(start);
        reservoirs.push(reservoir);
    }
    let mut full = 0;
    for reservoir in &reservoirs {
        if matches!(reservoir.warmup().await, WarmupOutcome::Full { .. }) {
            full += 1;
        }
    }
    let filled = started.elapsed();
    starts.sort();
    eprintln!(
        "shared start: 24 reservoirs of 50 started in at most {:?} (median {:?}), {full} full          after {filled:?}; {} connections made, {} refused for the rate; failed connects {:?}",
        starts[starts.len() - 1],
        starts[starts.len() / 2],
        factory.created.load(Ordering::Acquire),
        factory.refused_for_rate(),
        factory.failures()
    );
    assert_eq!(full, 24, "every reservoir fills");
    Ok(())
}
