//! Fleets of refillers sharing one rate limiter item, against a simulated
//! cluster that enforces DSQL's connection rate, burst and limit, on a paused
//! clock.
//!
//! Each node is the production refiller loop ([`refill`]) with its own live
//! count and its own bucket over one shared in-process arrival-time item. A
//! connection is a place in the simulated cluster rather than a socket, so a
//! fleet of ten thousand connections runs in one process. The nodes' clocks
//! agree; Property 10 covers clocks that differ.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::{sync::Semaphore, task::JoinHandle, time::Instant};

use super::*;
use crate::dsql::distributed_bucket::{ArrivalTimeStore, InProcessArrivalTime, WallClock};

/// DSQL's connection rate, burst and connection limit per cluster.
const RATE: u64 = 100;
const BURST: u64 = 1_000;
const CONNECTION_LIMIT: usize = 10_000;
const CONNECT_LATENCY: Duration = Duration::from_millis(100);
const ENDPOINT: &str = "cluster.dsql.example.on.aws";
const EPOCH_US: i64 = 1_800_000_000_000_000;
/// One connect in the cluster's rate bucket, counted in millionths so a refill
/// over whole microseconds stays exact.
const CONNECT: u64 = 1_000_000;

#[derive(Debug)]
struct ClusterState {
    tokens: u64,
    refilled_at: Instant,
    open: usize,
    refused_for_rate: usize,
    refused_for_connections: usize,
}

#[derive(Debug)]
struct Cluster {
    state: Mutex<ClusterState>,
}

impl Cluster {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(ClusterState {
                tokens: BURST * CONNECT,
                refilled_at: Instant::now(),
                open: 0,
                refused_for_rate: 0,
                refused_for_connections: 0,
            }),
        })
    }

    /// A connect arriving now: refused past the connection rate (`53400`),
    /// then past the connection limit (`53300`).
    fn arrive(&self) -> bool {
        let mut state = self.state.lock().unwrap();
        let now = Instant::now();
        let elapsed = u64::try_from((now - state.refilled_at).as_micros()).unwrap();
        state.tokens = (state.tokens + elapsed * RATE).min(BURST * CONNECT);
        state.refilled_at = now;
        if state.tokens < CONNECT {
            state.refused_for_rate += 1;
            return false;
        }
        state.tokens -= CONNECT;
        if state.open >= CONNECTION_LIMIT {
            state.refused_for_connections += 1;
            return false;
        }
        state.open += 1;
        true
    }

    /// Spend the cluster's rate, as other traffic would.
    fn drain(&self) {
        let mut state = self.state.lock().unwrap();
        state.tokens = 0;
        state.refilled_at = Instant::now();
    }

    fn open(&self) -> usize {
        self.state.lock().unwrap().open
    }

    /// Connects refused for the rate and for the connection limit.
    fn refused(&self) -> (usize, usize) {
        let state = self.state.lock().unwrap();
        (state.refused_for_rate, state.refused_for_connections)
    }
}

/// A connect attempt, and once the cluster accepted it, a connection.
#[derive(Debug)]
struct Connection {
    cluster: Arc<Cluster>,
    open: bool,
    _place: LiveCharge,
}

impl Drop for Connection {
    fn drop(&mut self) {
        if self.open {
            self.cluster.state.lock().unwrap().open -= 1;
        }
    }
}

#[derive(Debug)]
struct FleetClock {
    origin: Instant,
}

impl WallClock for FleetClock {
    fn now_us(&self) -> i64 {
        EPOCH_US + i64::try_from(self.origin.elapsed().as_micros()).unwrap()
    }
}

#[derive(Debug)]
struct Node {
    live: Arc<LiveCount>,
    held: Arc<Mutex<Vec<Connection>>>,
    bucket: Arc<DistributedTokenBucket>,
    refiller: JoinHandle<()>,
}

impl Node {
    fn held(&self) -> usize {
        self.held.lock().unwrap().len()
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.refiller.abort();
    }
}

struct Fleet {
    cluster: Arc<Cluster>,
    store: Arc<InProcessArrivalTime>,
    origin: Instant,
}

impl Fleet {
    fn new() -> Self {
        Self {
            cluster: Cluster::new(),
            store: Arc::new(InProcessArrivalTime::default()),
            origin: Instant::now(),
        }
    }

    fn start_node(&self, target: usize) -> Node {
        let live = LiveCount::new(target);
        let held = Arc::new(Mutex::new(Vec::new()));
        let bucket = Arc::new(DistributedTokenBucket::with_store(
            Arc::clone(&self.store) as Arc<dyn ArrivalTimeStore>,
            Arc::new(FleetClock {
                origin: self.origin,
            }),
            ENDPOINT,
            RATE as f64,
            BURST,
        ));
        let cluster = Arc::clone(&self.cluster);
        let connections = Arc::clone(&held);
        let refiller = tokio::spawn(refill(
            Arc::new(Semaphore::new(INFLIGHT_LIMIT)),
            Arc::clone(&live),
            Arc::clone(&bucket),
            move |place, _| {
                let cluster = Arc::clone(&cluster);
                let connections = Arc::clone(&connections);
                async move {
                    let open = cluster.arrive();
                    let attempt = Connection {
                        cluster,
                        open,
                        _place: place,
                    };
                    tokio::time::sleep(CONNECT_LATENCY).await;
                    if attempt.open {
                        connections.lock().unwrap().push(attempt);
                    }
                    open
                }
            },
        ));
        Node {
            live,
            held,
            bucket,
            refiller,
        }
    }
}

fn total_held(nodes: &[Node]) -> usize {
    nodes.iter().map(Node::held).sum()
}

/// Advance virtual time until `condition` holds, within `limit`; returns how
/// long that took.
async fn until(mut condition: impl FnMut() -> bool, limit: Duration) -> Duration {
    let started = Instant::now();
    while !condition() {
        assert!(started.elapsed() < limit, "not reached within {limit:?}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    started.elapsed()
}

#[tokio::test(start_paused = true)]
async fn a_fleet_started_at_once_fills_through_the_burst_then_the_rate_without_a_refusal() {
    let fleet = Fleet::new();
    let nodes: Vec<_> = (0..100).map(|_| fleet.start_node(50)).collect();

    let serving = until(
        || nodes.iter().all(|node| node.held() >= MIN_TARGET),
        WARMUP_TIMEOUT,
    )
    .await;
    let filled = serving + until(|| total_held(&nodes) == 5_000, Duration::from_secs(120)).await;

    // A thousand connects from the burst, the other four thousand at a
    // hundred a second.
    assert!(
        (Duration::from_secs(40)..=Duration::from_secs(41)).contains(&filled),
        "the fleet filled in {filled:?}"
    );
    assert_eq!(fleet.cluster.refused(), (0, 0));
}

#[tokio::test(start_paused = true)]
async fn a_rolling_restart_refills_each_node_within_the_burst() {
    let fleet = Fleet::new();
    let mut nodes: Vec<_> = (0..100).map(|_| fleet.start_node(50)).collect();
    until(|| total_held(&nodes) == 5_000, Duration::from_secs(120)).await;
    // The burst recovers between deployments.
    tokio::time::sleep(Duration::from_secs(10)).await;

    for index in 0..nodes.len() {
        // The old node stops and its connections close as the new one starts.
        nodes[index] = fleet.start_node(50);
        let refill = until(|| nodes[index].held() == 50, Duration::from_secs(30)).await;
        // Fifty connects eight at a time: no admission waited for the rate.
        assert!(
            refill <= 7 * CONNECT_LATENCY + Duration::from_millis(20),
            "node {index} refilled in {refill:?}"
        );
    }
    assert_eq!(fleet.cluster.refused(), (0, 0));
    assert_eq!(fleet.cluster.open(), 5_000);
}

#[tokio::test(start_paused = true)]
async fn a_dynamodb_outage_keeps_refills_going_at_the_directive_shares() {
    let fleet = Fleet::new();
    let nodes: Vec<_> = (0..100).map(|_| fleet.start_node(50)).collect();
    until(|| total_held(&nodes) == 5_000, Duration::from_secs(120)).await;
    for node in &nodes {
        // A directive's share of the cluster's rate among a hundred nodes.
        node.bucket.set_fallback_rate(1.0);
    }

    // The shared item stops answering, the cluster's rate is already spent,
    // and every connection reaches the end of its lifetime at once.
    fleet.store.set_failing(true);
    fleet.cluster.drain();
    for node in &nodes {
        node.held.lock().unwrap().clear();
    }
    assert_eq!(fleet.cluster.open(), 0);

    let refilled = until(|| total_held(&nodes) == 5_000, Duration::from_secs(120)).await;
    // Fifty connects a node at one a second, the first lost to the drained
    // rate.
    assert!(refilled <= Duration::from_secs(55), "{refilled:?}");
    let (refused_for_rate, _) = fleet.cluster.refused();
    assert!(
        refused_for_rate <= 100,
        "the fallback bursts of one connect a node add at most a hundred refusals, not \
         {refused_for_rate}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_fleet_past_the_connection_limit_is_refused_backs_off_and_keeps_what_it_holds() {
    let fleet = Fleet::new();
    let nodes: Vec<_> = (0..210).map(|_| fleet.start_node(50)).collect();

    // Ten thousand of the 10,500 wanted: a thousand from the burst, then nine
    // thousand at a hundred a second.
    until(
        || fleet.cluster.open() == CONNECTION_LIMIT,
        Duration::from_secs(150),
    )
    .await;
    let mut floors: Vec<_> = nodes.iter().map(Node::held).collect();
    let mut refused_half_a_minute_in = 0;
    for second in 1..=60 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        for (node, floor) in nodes.iter().zip(&mut floors) {
            let held = node.held();
            assert!(held >= *floor, "a node lost a connection it held");
            *floor = held;
        }
        if second == 30 {
            refused_half_a_minute_in = fleet.cluster.refused().1;
        }
    }
    assert_eq!(total_held(&nodes), CONNECTION_LIMIT);
    let (refused_for_rate, refused_for_connections) = fleet.cluster.refused();
    assert_eq!(refused_for_rate, 0);
    assert!(refused_for_connections > 0);
    // Half a minute past the limit every node short of its target has been
    // refused often enough to retry at most once every five seconds; the rate
    // limiter alone would let 3,000 attempts through.
    let late = refused_for_connections - refused_half_a_minute_in;
    assert!(
        late <= nodes.len() * 8,
        "{late} refused in the last half minute"
    );

    // Stopping the refillers cancels their attempts; each node's live count is
    // then exactly the connections it holds.
    for node in &nodes {
        node.refiller.abort();
    }
    tokio::time::sleep(Duration::from_millis(10)).await;
    for node in &nodes {
        assert_eq!(node.live.count(), node.held());
    }
}
