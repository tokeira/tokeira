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
        ConnectionFactoryError, DsqlConnectionDirector, DsqlPoolConfig,
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
                Err(ConnectionFactoryError::Database(
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

async fn start(
    factory: &Arc<ScriptedFactory>,
    config: ReservoirConfig,
) -> (Reservoir, Arc<SlotBlockManager>) {
    let slots = SlotBlockManager::local_for_tests(1_000);
    let reservoir = Reservoir::start(
        config,
        Arc::clone(factory) as Arc<dyn PhysicalConnectionFactory>,
        Arc::new(DistributedTokenBucket::local_for_tests(1_000.0, 1_000)),
        Arc::clone(&slots),
    )
    .await
    .unwrap();
    (reservoir, slots)
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
    let (reservoir, slots) = start(&factory, reservoir_config(16, 8)).await;
    assert_eq!(factory.attempts(), 16);

    assert_eq!(reservoir.retire_excess(1), 15);
    let refill = until(|| reservoir.ready_count() == 16).await;

    // Eight connects at a time take two rounds; one at a time would take
    // fifteen. The refiller notices the gap within its idle interval.
    assert!(
        refill <= REFILLER_IDLE_INTERVAL + 2 * CONNECT_LATENCY + Duration::from_millis(10),
        "{refill:?}"
    );
    assert_eq!(factory.attempts(), 31, "exactly the missing connections");
    assert!(factory.peak.load(Ordering::Acquire) <= 8);
    assert_eq!(slots.used_slots_for_tests(), 16);
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
    let (reservoir, _slots) = start(&factory, config.clone()).await;
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
    let factory =
        ScriptedFactory::new(fake_connections(2 * target + 4).await, [], Attempt::Connect);
    tokio::time::pause();
    let config = reservoir_config(target, 4);
    let (reservoir, _slots) = start(&factory, config.clone()).await;
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
    // Returns always fit in the ready queue, so no healthy connection was
    // retired and remade: the reservoir never held more than twice its target.
    assert!(factory.attempts() <= 2 * target, "{}", factory.attempts());
}

#[tokio::test]
async fn a_hung_connect_times_out_and_gives_its_slot_back() {
    let factory = ScriptedFactory::new(
        fake_connections(8).await,
        [Attempt::Connect; 4].into_iter().chain([Attempt::Hang]),
        Attempt::Connect,
    );
    tokio::time::pause();
    let (reservoir, slots) = start(&factory, reservoir_config(4, 2)).await;
    assert_eq!(reservoir.retire_excess(1), 3);

    // One lane hangs; the other keeps filling. The hung attempt still counts
    // toward the target, so the reservoir stops one short of it.
    until(|| reservoir.ready_count() == 3).await;
    assert_eq!(factory.running(), 1);
    assert_eq!(
        slots.used_slots_for_tests(),
        4,
        "three ready and the hung attempt"
    );

    tokio::time::sleep(CONNECT_TIMEOUT).await;
    until(|| factory.running() == 0 && reservoir.ready_count() == 4).await;
    assert_eq!(
        slots.used_slots_for_tests(),
        4,
        "the timed-out slot came back"
    );
}

#[tokio::test]
async fn refused_connects_back_off_and_recover_on_the_first_success() {
    let factory = ScriptedFactory::new(fake_connections(12).await, [], Attempt::Connect);
    tokio::time::pause();
    let (reservoir, slots) = start(&factory, reservoir_config(4, 8)).await;
    factory.set_otherwise(Attempt::Refuse);
    assert_eq!(reservoir.retire_excess(1), 3);

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
        slots.used_slots_for_tests(),
        reservoir.ready_count() + factory.running(),
        "a refused attempt keeps no slot"
    );

    factory.set_otherwise(Attempt::Connect);
    let recovered = until(|| reservoir.ready_count() == 4).await;
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
    let (reservoir, _slots) = start(&factory, config.clone()).await;
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
    let (reservoir, slots) = start(&factory, reservoir_config(3, 2)).await;
    let entry = reservoir.checkout().unwrap();
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
    let mut checked_out = Vec::new();
    while let Ok(entry) = reservoir.checkout() {
        assert!(!entry.broken.is_set());
        checked_out.push(entry);
    }
    assert_eq!(checked_out.len(), ready - 1);
    assert_eq!(slots.used_slots_for_tests(), ready - 1);
}
