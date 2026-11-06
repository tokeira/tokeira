//! The reservoir and director under load, on a paused clock.
//!
//! Each test opens real `PgConnection`s against a fake server that speaks only
//! PostgreSQL's startup handshake, then pauses the clock. From then on a
//! scripted factory hands out those connections, refuses, or hangs, with
//! latency in virtual time, so refill timing, timeouts and backoff are exact
//! and nothing waits on the wall clock.

use std::{
    collections::VecDeque,
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use proptest::prelude::*;
use sqlx::{
    Connection, PgConnection,
    postgres::{PgConnectOptions, PgSslMode},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinSet,
    time::Instant,
};
use tracing::debug_span;

use super::*;
use crate::{
    ConnectionDirector, DbClass,
    dsql::{
        ConnectionBudget, ConnectionFactoryError, DsqlConnectionDirector, DsqlPoolConfig,
        connection_factory::CONNECT_TIMEOUT, in_operation,
    },
};

const CONNECT_LATENCY: Duration = Duration::from_millis(100);

const CLASSES: [DbClass; 5] = [
    DbClass::Control,
    DbClass::Commit,
    DbClass::Read,
    DbClass::Projection,
    DbClass::Maintenance,
];

/// Open `count` connections to a server that completes PostgreSQL's startup
/// handshake (AuthenticationOk, BackendKeyData, ReadyForQuery) and then only
/// holds the socket.
async fn fake_connections(count: usize) -> Vec<PgConnection> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            tokio::spawn(serve_startup(socket));
        }
    });
    let options = PgConnectOptions::new()
        .host("127.0.0.1")
        .port(port)
        .username("tokeira")
        .database("postgres")
        .ssl_mode(PgSslMode::Disable);
    let mut connections = Vec::with_capacity(count);
    for _ in 0..count {
        connections.push(PgConnection::connect_with(&options).await.unwrap());
    }
    server.abort();
    connections
}

async fn serve_startup(mut socket: TcpStream) {
    let Ok(length) = socket.read_i32().await else {
        return;
    };
    let mut startup = vec![0; usize::try_from(length - 4).unwrap()];
    if socket.read_exact(&mut startup).await.is_err() {
        return;
    }
    let mut reply = Vec::new();
    reply.push(b'R');
    reply.extend(8_i32.to_be_bytes());
    reply.extend(0_i32.to_be_bytes());
    reply.push(b'K');
    reply.extend(12_i32.to_be_bytes());
    reply.extend(1_i32.to_be_bytes());
    reply.extend(2_i32.to_be_bytes());
    reply.push(b'Z');
    reply.extend(5_i32.to_be_bytes());
    reply.push(b'I');
    if socket.write_all(&reply).await.is_err() {
        return;
    }
    let mut sink = [0; 256];
    while matches!(socket.read(&mut sink).await, Ok(read) if read > 0) {}
}

#[derive(Clone, Copy, Debug)]
enum Attempt {
    Connect,
    /// DSQL refusing a connection, as it does past its connection count
    /// (SQLSTATE 53300) or connection rate (53400).
    Refuse,
    Hang,
}

#[derive(Debug)]
struct ScriptedFactory {
    stash: Mutex<Vec<PgConnection>>,
    script: Mutex<VecDeque<Attempt>>,
    otherwise: Mutex<Attempt>,
    attempts: AtomicUsize,
    running: Arc<AtomicUsize>,
    peak: AtomicUsize,
}

impl ScriptedFactory {
    fn new(
        stash: Vec<PgConnection>,
        script: impl IntoIterator<Item = Attempt>,
        otherwise: Attempt,
    ) -> Arc<Self> {
        Arc::new(Self {
            stash: Mutex::new(stash),
            script: Mutex::new(script.into_iter().collect()),
            otherwise: Mutex::new(otherwise),
            attempts: AtomicUsize::new(0),
            running: Arc::new(AtomicUsize::new(0)),
            peak: AtomicUsize::new(0),
        })
    }

    fn set_otherwise(&self, attempt: Attempt) {
        *self.otherwise.lock().unwrap() = attempt;
    }

    fn attempts(&self) -> usize {
        self.attempts.load(Ordering::Acquire)
    }

    fn running(&self) -> usize {
        self.running.load(Ordering::Acquire)
    }
}

struct Running(Arc<AtomicUsize>);

impl Drop for Running {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

#[async_trait]
impl PhysicalConnectionFactory for ScriptedFactory {
    async fn create_connection(&self) -> Result<PgConnection, ConnectionFactoryError> {
        self.attempts.fetch_add(1, Ordering::AcqRel);
        let attempt = self
            .script
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(*self.otherwise.lock().unwrap());
        let running = self.running.fetch_add(1, Ordering::AcqRel) + 1;
        self.peak.fetch_max(running, Ordering::AcqRel);
        let _running = Running(Arc::clone(&self.running));
        match attempt {
            Attempt::Connect => {
                tokio::time::sleep(CONNECT_LATENCY).await;
                self.stash
                    .lock()
                    .unwrap()
                    .pop()
                    .ok_or_else(|| ConnectionFactoryError::Connection("stash empty".to_owned()))
            }
            Attempt::Refuse => {
                tokio::time::sleep(CONNECT_LATENCY).await;
                Err(ConnectionFactoryError::Refused(
                    "too many connections (SQLSTATE 53300)".to_owned(),
                ))
            }
            Attempt::Hang => std::future::pending().await,
        }
    }
}

fn reservoir_config(target_ready: usize, inflight_limit: usize) -> ReservoirConfig {
    ReservoirConfig {
        target_ready,
        inflight_limit,
        ..ReservoirConfig::default()
    }
}

async fn start(factory: &Arc<ScriptedFactory>, config: ReservoirConfig) -> Reservoir {
    Reservoir::start(
        config,
        Arc::clone(factory) as Arc<dyn PhysicalConnectionFactory>,
        Arc::new(DistributedTokenBucket::local_for_tests(1_000.0, 1_000)),
    )
    .await
    .unwrap()
}

fn director(config: ReservoirConfig, reservoir: Reservoir) -> DsqlConnectionDirector {
    DsqlConnectionDirector::start(
        DsqlPoolConfig {
            reservoir: config,
            ..DsqlPoolConfig::default()
        },
        reservoir,
    )
    .unwrap()
}

/// Advance virtual time until `condition` holds; returns how long that took.
async fn until(mut condition: impl FnMut() -> bool) -> Duration {
    let started = Instant::now();
    while !condition() {
        assert!(
            started.elapsed() < Duration::from_secs(120),
            "not reached within two virtual minutes"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    started.elapsed()
}

#[tokio::test]
async fn a_mass_retirement_refills_in_parallel_without_overshoot() {
    let factory = ScriptedFactory::new(fake_connections(40).await, [], Attempt::Connect);
    tokio::time::pause();
    let reservoir = start(&factory, reservoir_config(16, 8)).await;
    assert_eq!(factory.attempts(), 16);

    assert_eq!(reservoir.retire_ready_for_tests(1), 15);
    let refill = until(|| reservoir.ready_count() == 16).await;

    // Eight connects at a time take two rounds; one at a time would take
    // fifteen. The refiller notices the gap within its idle interval.
    assert!(
        refill <= REFILLER_IDLE_INTERVAL + 2 * CONNECT_LATENCY + Duration::from_millis(10),
        "{refill:?}"
    );
    assert_eq!(factory.attempts(), 31, "exactly the missing connections");
    assert!(factory.peak.load(Ordering::Acquire) <= 8);
    assert_eq!(reservoir.live_count(), 16);
}

#[tokio::test]
async fn a_class_permit_comes_back_only_with_its_connection() {
    // After warmup, connects hang, so nothing refills the reservoir.
    let factory = ScriptedFactory::new(
        fake_connections(5).await,
        [Attempt::Connect; 5],
        Attempt::Hang,
    );
    tokio::time::pause();
    let config = reservoir_config(5, 2);
    let reservoir = start(&factory, config.clone()).await;
    let director = director(config, reservoir);
    // At this target every class has one permit: take every connection.
    let mut held = Vec::new();
    for class in CLASSES {
        held.push(director.acquire(class).await.unwrap());
    }
    let commit = held.remove(1);
    assert_eq!(commit.class, DbClass::Commit);

    drop(commit);
    // The next commit admitted must find the returned connection, not an
    // empty reservoir: its permit is only free once the connection is back.
    let again = director.acquire(DbClass::Commit).await;
    assert!(again.is_ok(), "{:?}", again.err());
}

#[tokio::test]
async fn a_burst_within_the_class_budgets_never_finds_the_reservoir_empty() {
    let target = 10;
    let factory = ScriptedFactory::new(fake_connections(target + 4).await, [], Attempt::Connect);
    tokio::time::pause();
    let config = reservoir_config(target, 4);
    let reservoir = start(&factory, config.clone()).await;
    let director = Arc::new(director(config, reservoir));

    let mut workers = JoinSet::new();
    for worker in 0..40 {
        let director = Arc::clone(&director);
        let class = CLASSES[worker % CLASSES.len()];
        workers.spawn(async move {
            for _ in 0..25 {
                let permit = director.acquire(class).await?;
                tokio::time::sleep(Duration::from_millis(3)).await;
                drop(permit);
            }
            anyhow::Ok(())
        });
    }
    while let Some(worker) = workers.join_next().await {
        worker.unwrap().unwrap();
    }
    // Returns always fit in the ready queue and a connection in use is still
    // live, so no connection was retired, remade or added past the target.
    assert_eq!(factory.attempts(), target);
}

#[tokio::test]
async fn a_hung_connect_times_out_and_gives_its_place_back() {
    let factory = ScriptedFactory::new(
        fake_connections(10).await,
        [Attempt::Connect; 5].into_iter().chain([Attempt::Hang]),
        Attempt::Connect,
    );
    tokio::time::pause();
    let reservoir = start(&factory, reservoir_config(5, 2)).await;
    assert_eq!(reservoir.retire_ready_for_tests(1), 4);

    // One lane hangs; the other keeps filling. The hung attempt still counts
    // toward the target, so the reservoir stops one short of it.
    until(|| reservoir.ready_count() == 4).await;
    assert_eq!(factory.running(), 1);
    assert_eq!(reservoir.live_count(), 5, "four ready and the hung attempt");

    tokio::time::sleep(CONNECT_TIMEOUT).await;
    until(|| factory.running() == 0 && reservoir.ready_count() == 5).await;
    assert_eq!(reservoir.live_count(), 5, "the timed-out place came back");
}

#[tokio::test]
async fn refused_connects_back_off_and_recover_on_the_first_success() {
    let factory = ScriptedFactory::new(fake_connections(12).await, [], Attempt::Connect);
    tokio::time::pause();
    let reservoir = start(&factory, reservoir_config(5, 8)).await;
    factory.set_otherwise(Attempt::Refuse);
    assert_eq!(reservoir.retire_ready_for_tests(1), 4);

    let before = factory.attempts();
    tokio::time::sleep(Duration::from_secs(60)).await;
    let refused = factory.attempts() - before;
    // A fixed 250 ms retry would make about two hundred attempts in a minute;
    // doubling delays capped at ten seconds make a few dozen at most.
    assert!(
        (6..=24).contains(&refused),
        "{refused} attempts in a minute"
    );
    assert_eq!(
        reservoir.live_count(),
        reservoir.ready_count() + factory.running(),
        "a refused attempt keeps no place"
    );

    factory.set_otherwise(Attempt::Connect);
    let recovered = until(|| reservoir.ready_count() == 5).await;
    assert!(
        recovered <= REFILLER_MAX_BACKOFF + 3 * CONNECT_LATENCY + REFILLER_IDLE_INTERVAL,
        "{recovered:?}"
    );
}

#[tokio::test]
async fn a_connection_an_operation_broke_is_retired_and_replaced() {
    let factory = ScriptedFactory::new(fake_connections(8).await, [], Attempt::Connect);
    tokio::time::pause();
    let config = reservoir_config(5, 2);
    let reservoir = start(&factory, config.clone()).await;
    let director = director(config, reservoir);

    let failed: anyhow::Result<()> = in_operation(debug_span!("dsql.test"), async {
        let _permit = director.acquire(DbClass::Commit).await?;
        Err(anyhow::Error::new(sqlx::Error::Io(io::Error::new(
            io::ErrorKind::ConnectionReset,
            "connection reset by peer",
        ))))
    })
    .await;
    assert!(failed.is_err());

    // Returned with its broken flag set, the connection retires and the
    // refiller makes a replacement.
    until(|| factory.attempts() == 6).await;
}

#[tokio::test]
async fn checkout_skips_a_connection_flagged_after_its_return() {
    let factory = ScriptedFactory::new(fake_connections(8).await, [], Attempt::Connect);
    tokio::time::pause();
    let reservoir = start(&factory, reservoir_config(5, 2)).await;
    let entry = reservoir.checkout().await.unwrap();
    let flag = entry.broken.clone();
    reservoir
        .return_sender()
        .send(ReturnedConnection {
            entry,
            marked_bad: false,
            class_permit: None,
        })
        .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    let ready = reservoir.ready_count();
    flag.mark();
    // Checkout retires the flagged connection when it meets it, and the
    // refiller's replacement arrives within the checkout's wait.
    let mut checked_out = Vec::new();
    while let Ok(entry) = reservoir.checkout().await {
        assert!(!entry.broken.is_set());
        checked_out.push(entry);
    }
    assert_eq!(checked_out.len(), ready);
    assert_eq!(factory.attempts(), 6);
    assert_eq!(reservoir.live_count(), ready);
}

#[tokio::test]
async fn a_checkout_that_finds_the_reservoir_empty_takes_the_connection_the_refiller_creates() {
    let factory = ScriptedFactory::new(fake_connections(6).await, [], Attempt::Connect);
    tokio::time::pause();
    let reservoir = start(&factory, reservoir_config(5, 2)).await;
    let mut held = Vec::new();
    for _ in 0..5 {
        held.push(reservoir.checkout().await.unwrap());
    }
    // An operation broke one of them: its return retires it and frees its
    // place for a replacement.
    reservoir
        .return_sender()
        .send(ReturnedConnection {
            entry: held.pop().unwrap(),
            marked_bad: true,
            class_permit: None,
        })
        .unwrap();

    let started = Instant::now();
    let entry = reservoir.checkout().await;
    assert!(entry.is_ok(), "{:?}", entry.err());
    assert!(
        started.elapsed() <= REFILLER_IDLE_INTERVAL + CONNECT_LATENCY + Duration::from_millis(10),
        "{:?}",
        started.elapsed()
    );
    assert!(started.elapsed() < CHECKOUT_WAIT);
    assert_eq!(factory.attempts(), 6);
    assert_eq!(reservoir.live_count(), 5);
}

#[tokio::test]
async fn a_lowered_target_retires_idle_connections_now_and_the_rest_as_they_return() {
    let factory = ScriptedFactory::new(fake_connections(12).await, [], Attempt::Connect);
    tokio::time::pause();
    let config = reservoir_config(10, 4);
    let reservoir = start(&factory, config.clone()).await;
    let director = director(config, reservoir);
    // At a target of ten: control 1, commit 3, read 2, projection 1 and
    // maintenance 3. Hold eight connections.
    let mut held = Vec::new();
    for (class, count) in [
        (DbClass::Commit, 3),
        (DbClass::Read, 2),
        (DbClass::Maintenance, 3),
    ] {
        for _ in 0..count {
            held.push(director.acquire(class).await.unwrap());
        }
    }
    assert_eq!(director.ready_connections(), 2);

    let target = director
        .apply_connection_budget(ConnectionBudget {
            rate_per_second: 1.0,
            capacity: 5,
            max_reservoir_size: 10_000,
        })
        .await
        .unwrap();
    assert_eq!(target, 5);
    assert_eq!(director.reservoir_target(), 5);
    assert_eq!(
        director.ready_connections(),
        0,
        "the idle two retire at once"
    );

    // Eight come back: the first three retire, the other five are kept.
    held.clear();
    until(|| director.ready_connections() == 5).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(director.ready_connections(), 5);
    assert_eq!(factory.attempts(), 10, "nothing was replaced or added");

    // The class budgets were recomputed for five: one commit permit.
    let commit = director.acquire(DbClass::Commit).await.unwrap();
    let second =
        tokio::time::timeout(Duration::from_secs(5), director.acquire(DbClass::Commit)).await;
    assert!(second.is_err(), "a second commit was admitted");
    drop(commit);
}

fn ready_entry(connection: PgConnection, reservoir: &Reservoir) -> ReservoirEntry {
    ReservoirEntry {
        connection,
        created_at: std::time::Instant::now(),
        max_lifetime: BASE_LIFETIME,
        broken: BrokenFlag::default(),
        live: reservoir.live.try_charge().expect("room in the live count"),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: dsql-reservoir-redesign, Property 11: Bounded Checkout
    #[test]
    fn checkout_takes_what_arrives_within_its_wait_and_otherwise_reports_empty(
        ready in 0usize..=2,
        broken_first in any::<bool>(),
        arrivals in prop::collection::vec(1u64..2_000, 0..=2),
        cancel_at in prop::option::of(1u64..1_500),
    ) {
        let wait = u64::try_from(CHECKOUT_WAIT.as_millis()).unwrap();
        // Two events at the same instant have no defined order.
        prop_assume!(!arrivals.contains(&wait) && cancel_at != Some(wait));
        prop_assume!(cancel_at.is_none_or(|cancel| !arrivals.contains(&cancel)));
        let usable = ready - usize::from(broken_first && ready > 0);
        let taken_at = if usable > 0 {
            Some(0)
        } else {
            arrivals.iter().copied().filter(|at| *at < wait).min()
        };
        let cancelled = cancel_at.is_some_and(|cancel| cancel < taken_at.unwrap_or(wait));

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut connections = fake_connections(ready + arrivals.len()).await;
            tokio::time::pause();
            let (reservoir, ready_tx) =
                Reservoir::idle_with_ready_sender(reservoir_config(MIN_TARGET, 1));
            for index in 0..ready {
                let entry = ready_entry(connections.pop().unwrap(), &reservoir);
                if index == 0 && broken_first {
                    entry.broken.mark();
                }
                ready_tx.try_send(entry).unwrap();
            }
            for at in &arrivals {
                let entry = ready_entry(connections.pop().unwrap(), &reservoir);
                let ready_tx = ready_tx.clone();
                let at = Duration::from_millis(*at);
                tokio::spawn(async move {
                    tokio::time::sleep(at).await;
                    ready_tx.send(entry).await.unwrap();
                });
            }

            let started = Instant::now();
            let checkout = reservoir.checkout();
            let outcome = match cancel_at {
                Some(cancel) => {
                    tokio::time::timeout(Duration::from_millis(cancel), checkout).await.ok()
                }
                None => Some(checkout.await),
            };
            let elapsed = started.elapsed();
            let expected = Duration::from_millis(match (cancelled, taken_at) {
                (true, _) => cancel_at.unwrap(),
                (false, Some(at)) => at,
                (false, None) => wait,
            });
            // Paused time advances to whole-millisecond timer ticks.
            prop_assert!(
                elapsed >= expected && elapsed <= expected + Duration::from_millis(1),
                "{:?} instead of {:?}", elapsed, expected
            );
            match outcome {
                None => prop_assert!(cancelled),
                Some(Ok(entry)) => {
                    prop_assert!(!cancelled && taken_at.is_some());
                    prop_assert!(!entry.broken.is_set());
                }
                Some(Err(error)) => {
                    prop_assert!(!cancelled && taken_at.is_none());
                    prop_assert!(matches!(error, ReservoirError::Empty));
                }
            }

            // Every connection not taken is still in the ready queue once all
            // have arrived, a cancelled wait included.
            tokio::time::sleep(Duration::from_secs(3)).await;
            let taken = usize::from(!cancelled && taken_at.is_some());
            prop_assert_eq!(reservoir.ready_count(), usable + arrivals.len() - taken);
            Ok(())
        })?;
    }
}
