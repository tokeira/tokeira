//! Who runs in a scenario, and the loop that drives them.
//!
//! Two topologies:
//!
//! - [`single_node`]: one engine node, `node`, with no controller. It owns
//!   its single bundle itself.
//! - [`cluster`]: a placement controller and two engine nodes, `node-a` and
//!   `node-b`, composed from `tokeira-controller`'s public parts as
//!   `apps/tokeira-controller` composes them. Every connection between them
//!   crosses turmoil's network.
//!
//! Every node is a listener-backed node started through
//! [`TokeiradHandle::start_in_memory_with_transport`]: its gRPC listener, its
//! calls to the other node and its calls to the controller are turmoil TCP,
//! and it reads the time from [`SimClock`]. Each topology's store is created
//! outside the hosts, standing in for the DSQL database, so it outlives a
//! crashed or bounced node. The store is shared memory, not network, so a
//! partition cuts traffic between hosts but never a node's store.
//!
//! A topology returns a [`Run`]. A scenario adds a worker and any observers
//! to it, then [`Run::drive`]s it with a fault schedule.
//!
//! # How the cluster forms
//!
//! The controller offers a node, when it registers, every bundle nobody owns,
//! and it never moves an owned bundle (`compute_desired_placement` in
//! `tokeira-controller`). Two nodes started together would split the bundles
//! only by the luck of a race, and the node that registers second usually
//! owns nothing. So a previous generation, a fifth host, holds every bundle's
//! lease until both nodes have registered, then hands them back, and the
//! controller's next placement splits them between the two.
//!
//! # Leases and node ids
//!
//! The cluster's store grants leases for [`LEASE`], as DSQL grants them for a
//! duration, and judges their expiry on [`SimClock`]. Each node has its own
//! handle on the store, so a fault can stall one node's lease renewals
//! while the others renew.
//!
//! Each node's incarnation id is drawn from the seed and the host's name. The
//! controller orders nodes by id, so this fixes which bundles each node owns
//! for a seed, and with it the queue and workflow a cluster workload picks.

use std::{
    collections::BTreeMap,
    error::Error,
    future::Future,
    hash::{DefaultHasher, Hash as _, Hasher as _},
    io::Write as _,
    net::{IpAddr, Ipv4Addr},
    os::unix::fs::PermissionsExt as _,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context as _, bail};
use connectrpc::client::{Http2Connection, SharedHttp2Connection};
use hyper::server::conn::http2;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    service::TowerToHyperService,
};
use tokeira_auth::peer::PeerMaterial;
use tokeira_config::peer::{PeerConfig, create_protected_file};
use tokeira_controller::{ConnectPlacementController, ControllerConfig, PlacementControllerState};
use tokeira_edge::PeerConnector;
use tokeira_engine::{NodeTransport, TokeiraConfig, TokeiradHandle};
use tokeira_proto::connect::tokeira::internal::controller::v1::PlacementControllerExt as _;
use tokeira_storage::{ControlRepository, InMemoryStore, LeaseOutcome, LeaseRepository};
use tokeira_types::{IncarnationId, PlacementConfig, ShardEpoch, ShardId};
use tokio::sync::Semaphore;
use tonic::transport::Endpoint;
use turmoil::{Sim, net::TcpListener};
use uuid::Uuid;

use crate::{
    PORT, SimClock, Trace, WORKER, epoch,
    faults::{Fault, Schedule},
    record, turmoil_connector,
};

/// Simulated host name of the single node in [`single_node`].
pub const NODE: &str = "node";
/// Simulated host name of the placement controller in [`cluster`].
pub const CONTROLLER: &str = "controller";
/// Simulated host names of the two engine nodes in [`cluster`].
pub const NODES: [&str; 2] = ["node-a", "node-b"];
/// Simulated host name of an observer added with [`Run::observe`].
pub const OBSERVER: &str = "observer";
/// Port the controller serves on, inside the simulated network.
pub const CONTROLLER_PORT: u16 = 7240;

/// How long the cluster's store grants a lease for. A node renews its leases
/// every second, so a lease lapses only when its owner stops renewing.
pub const LEASE: Duration = Duration::from_secs(10);

/// Small enough that two nodes split the bundles visibly; `shard_count` must
/// equal `bundle_count`, which the node's config validation enforces.
pub(crate) const BUNDLES: u32 = 4;
pub(crate) const PARTITIONS: u32 = 16;
/// Simulated time a run may take. Each tick is turmoil's default 1 ms, and
/// computation takes no simulated time, so this bounds waiting, not work.
const SINGLE_NODE_DURATION: Duration = Duration::from_secs(300);
const CLUSTER_DURATION: Duration = Duration::from_secs(600);
/// Bytes buffered in each direction of a node's in-memory pipe.
const PIPE_BUFFER: usize = 64 * 1024;
const PLACEMENT_INTERVAL: Duration = Duration::from_secs(1);
const BUDGET_INTERVAL: Duration = Duration::from_secs(5);
/// Simulated host name of the cluster generation the two nodes replace. It
/// serves nothing; its name only labels the leases it holds.
const PREVIOUS_GENERATION: &str = "previous-generation";
/// How long the previous generation waits, after both nodes have started,
/// before it hands its bundles back. A node registers with the controller a
/// few message latencies after it starts, and turmoil's default latency is at
/// most 100ms, so this covers registration many times over.
const REGISTRATION_ALLOWANCE: Duration = Duration::from_secs(2);

/// A built topology, ready for a worker, observers and a fault schedule.
pub struct Run {
    sim: Sim<'static>,
    trace: Arc<Mutex<Trace>>,
    store: InMemoryStore,
    /// Each node's own handle on the store, by host, for the faults that act
    /// on one node's store calls.
    node_stores: BTreeMap<&'static str, InMemoryStore>,
    /// Kept for the run's length: the cluster's nodes read it at start.
    _peer_file: Option<PeerFile>,
}

impl Run {
    /// Run `script` on the worker host. A run ends when the script returns.
    ///
    /// An error from the script fails the run, with its whole chain, so a
    /// failing run says why a call failed and not only which call it was.
    pub fn worker<F, Fut>(&mut self, script: F)
    where
        F: FnOnce(Arc<Mutex<Trace>>) -> Fut,
        Fut: Future<Output = anyhow::Result<()>> + 'static,
    {
        let script = script(Arc::clone(&self.trace));
        self.sim.client(WORKER, async move {
            script.await.map_err(|error| format!("{error:#}"))?;
            Ok(())
        });
    }

    /// Run `check` on an observer host with a handle on the topology's
    /// store that reads [`SimClock`]. An error from it fails the run.
    pub fn observe<F, Fut>(&mut self, check: F)
    where
        F: Fn(InMemoryStore, Arc<Mutex<Trace>>) -> Fut + 'static,
        Fut: Future<Output = Result<(), Box<dyn Error>>> + 'static,
    {
        let store = self.store.clone().with_clock(Arc::new(SimClock));
        let trace = Arc::clone(&self.trace);
        self.sim
            .host(OBSERVER, move || check(store.clone(), Arc::clone(&trace)));
    }

    /// Step the simulation until the worker's script returns, applying each
    /// fault when its trigger is met.
    ///
    /// Fails if a host or the worker returned an error, or if simulated time
    /// ran out first; the error carries the trace so far.
    pub fn drive(mut self, faults: impl IntoIterator<Item = Fault>) -> anyhow::Result<Trace> {
        let mut schedule = Schedule::new(faults);
        loop {
            match self.sim.step() {
                Ok(true) => break,
                Ok(false) => {}
                Err(error) => {
                    let trace = self.trace();
                    bail!("simulation failed: {error}\ntrace so far:\n{trace}");
                }
            }
            let now = self.sim.elapsed();
            schedule.notice(
                now,
                &self
                    .trace
                    .lock()
                    .expect("trace lock is never poisoned")
                    .steps,
            );
            for applied in schedule.apply_due(&mut self.sim, &self.node_stores, now) {
                record(&self.trace, now, applied);
            }
        }
        let trace = self.trace();
        // For reading a passing run:
        // `RUST_LOG=turmoil_sim::trace=debug cargo test -- --nocapture`.
        tracing::debug!(target: "turmoil_sim::trace", "the run's trace:\n{trace}");
        Ok(trace)
    }

    fn trace(&self) -> Trace {
        self.trace
            .lock()
            .expect("trace lock is never poisoned")
            .clone()
    }
}

/// One engine node, [`NODE`], with no controller, on a fresh store.
pub fn single_node(seed: u64) -> Run {
    let store = InMemoryStore::default();
    let mut sim = build_sim(seed, SINGLE_NODE_DURATION);
    let node_store = store.clone().with_own_renewal_stall();
    let node_stores = BTreeMap::from([(NODE, node_store.clone())]);
    let starts = AtomicU64::new(0);
    sim.host(NODE, move || {
        let node_id = incarnation(seed, NODE, starts.fetch_add(1, Ordering::Relaxed));
        serve_piped_node(
            node_store.clone(),
            single_node_config(),
            None,
            node_id,
            || {},
        )
    });
    Run {
        sim,
        trace: Arc::default(),
        store,
        node_stores,
        _peer_file: None,
    }
}

/// A placement controller and two engine nodes, [`NODES`], on one shared
/// store, forming as a rolling replacement (see the module docs).
pub fn cluster(seed: u64) -> anyhow::Result<Run> {
    let peer_file = PeerFile::generate()?;
    // The store homes each run by its own shard count, which must match the
    // nodes' or a fenced write lands at a home its lease doesn't cover.
    let store = InMemoryStore::with_shard_count(BUNDLES).with_lease_duration(
        time::Duration::try_from(LEASE).expect("the lease duration fits a time::Duration"),
    );
    let mut sim = build_sim(seed, CLUSTER_DURATION);
    // Each node adds one permit once it has started.
    let started = Arc::new(Semaphore::new(0));
    sim.host(PREVIOUS_GENERATION, || {
        std::future::pending::<Result<(), Box<dyn Error>>>()
    });
    // The controller is registered first so it is listening before a node's
    // startup asks it for the first routing snapshot.
    let controller_store = store.clone();
    let nodes_started = Arc::clone(&started);
    let controller_starts = AtomicU64::new(0);
    sim.host(CONTROLLER, move || {
        let start = controller_starts.fetch_add(1, Ordering::Relaxed);
        // The cluster forms once: a restarted controller finds the bundles
        // owned by the nodes, so only its first start stands in for the
        // previous generation.
        let previous = (start == 0).then(|| incarnation(seed, PREVIOUS_GENERATION, 0));
        // The controller judges leases the nodes renew, so its handle on the
        // shared store reads the same simulated time theirs do.
        serve_controller(
            controller_store.clone().with_clock(Arc::new(SimClock)),
            previous,
            incarnation(seed, CONTROLLER, start),
            Arc::clone(&nodes_started),
        )
    });
    let mut node_stores = BTreeMap::new();
    for node in NODES {
        let node_store = store.clone().with_own_renewal_stall();
        node_stores.insert(node, node_store.clone());
        let peer_path = peer_file.path.clone();
        let node_started = Arc::clone(&started);
        let starts = AtomicU64::new(0);
        sim.host(node, move || {
            serve_cluster_node(
                node,
                node_store.clone(),
                peer_path.clone(),
                incarnation(seed, node, starts.fetch_add(1, Ordering::Relaxed)),
                Arc::clone(&node_started),
            )
        });
    }
    Ok(Run {
        sim,
        trace: Arc::default(),
        store,
        node_stores,
        _peer_file: Some(peer_file),
    })
}

/// The incarnation id of `host`'s `start`th start in a run of `seed`.
///
/// `DefaultHasher::new` hashes with fixed keys, so one seed and host give one
/// id on every run. A restart is a new incarnation, as a restarted process
/// is, so the start count is part of the id.
fn incarnation(seed: u64, host: &str, start: u64) -> IncarnationId {
    let mut hasher = DefaultHasher::new();
    (seed, host).hash(&mut hasher);
    IncarnationId(Uuid::from_u64_pair(hasher.finish(), start))
}

fn build_sim(seed: u64, duration: Duration) -> Sim<'static> {
    let mut builder = turmoil::Builder::new();
    builder
        .rng_seed(seed)
        .epoch(epoch())
        .simulation_duration(duration);
    builder.build()
}

/// The default placement is one shard in one bundle, which a node with no
/// controller seeds and owns itself. The default store homes every run there.
fn single_node_config() -> TokeiraConfig {
    let mut config = TokeiraConfig::default();
    let placement = &mut config.infrastructure.placement;
    placement.node_host = NODE.to_owned();
    placement.node_port = Some(PORT);
    config
}

/// The controller host's software, composed as `apps/tokeira-controller`
/// composes it, minus process signals, metrics and the DSQL connection.
/// `previous`, when set, is the previous generation's id: the controller
/// takes every bundle for it before it listens, then hands them back once
/// both nodes have started.
async fn serve_controller(
    store: InMemoryStore,
    previous: Option<IncarnationId>,
    allocator: IncarnationId,
    nodes_started: Arc<Semaphore>,
) -> Result<(), Box<dyn Error>> {
    let config = ControllerConfig {
        controller_addr: format!("{CONTROLLER}:{CONTROLLER_PORT}"),
        bundle_count: BUNDLES,
        partition_count: PARTITIONS,
        shard_count: BUNDLES,
        ..ControllerConfig::default()
    };
    let leases: Arc<dyn LeaseRepository> = Arc::new(store.clone());
    if let Some(previous) = previous {
        // Taken before the controller listens, so no node can register while
        // a bundle is still unowned.
        let previous = previous.to_string();
        let held = hold_every_bundle(leases.as_ref(), &previous).await?;
        tokio::spawn(hand_back_once_nodes_start(
            Arc::clone(&leases),
            previous,
            held,
            nodes_started,
        ));
    }
    let control: Arc<dyn ControlRepository> = Arc::new(store);
    let state = PlacementControllerState::new(config, leases, control);
    tokio::spawn(placement_loop(state.clone()));
    tokio::spawn(budget_loop(state.clone(), allocator));

    let router =
        Arc::new(ConnectPlacementController::new(state)).register(connectrpc::Router::new());
    let service = connectrpc::ConnectRpcService::new(router);
    let listener = TcpListener::bind((IpAddr::V4(Ipv4Addr::UNSPECIFIED), CONTROLLER_PORT)).await?;
    loop {
        let (stream, peer) = listener.accept().await?;
        let service = TowerToHyperService::new(service.clone());
        tokio::spawn(async move {
            if let Err(error) = http2::Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(stream), service)
                .await
            {
                tracing::warn!(%peer, %error, "connection to the controller ended with an error");
            }
        });
    }
}

/// `run_placement_loop` in `apps/tokeira-controller`, which is private to that
/// binary: advance the snapshot generation, then push desired placement.
async fn placement_loop(state: PlacementControllerState) {
    let mut ticker = tokio::time::interval(PLACEMENT_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        let generation = match state.generation.current_generation().await {
            Ok(generation) => generation,
            Err(error) => {
                tracing::warn!(%error, "placement loop could not read the generation");
                continue;
            }
        };
        match state.advance_snapshot_generation(generation).await {
            Ok(_) => {
                if let Err(error) = state.publish_desired_placements().await {
                    tracing::warn!(%error, "placement loop could not publish placements");
                }
            }
            Err(error) => tracing::warn!(%error, "placement loop could not advance the generation"),
        }
    }
}

/// `run_budget_loop` in `apps/tokeira-controller`.
async fn budget_loop(state: PlacementControllerState, allocator: IncarnationId) {
    let mut ticker = tokio::time::interval(BUDGET_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        if let Err(error) = state
            .allocate_and_publish_connection_budgets(allocator)
            .await
        {
            tracing::warn!(%error, "budget loop could not allocate budgets");
        }
    }
}

/// Take every bundle's lease for the previous generation, before any node
/// exists.
async fn hold_every_bundle(
    leases: &dyn LeaseRepository,
    owner: &str,
) -> anyhow::Result<Vec<(ShardId, ShardEpoch)>> {
    let endpoint = format!("{PREVIOUS_GENERATION}:{PORT}");
    let mut held = Vec::new();
    for bundle in (0..BUNDLES).map(ShardId) {
        match leases
            .try_acquire_bundle(bundle, owner.to_owned(), endpoint.clone())
            .await?
        {
            LeaseOutcome::Acquired { epoch } => held.push((bundle, epoch)),
            outcome => bail!("the previous generation could not take {bundle:?}: {outcome:?}"),
        }
    }
    Ok(held)
}

/// Hand the previous generation's bundles back once both nodes have started
/// and had time to register, so the controller's next placement sees both.
async fn hand_back_once_nodes_start(
    leases: Arc<dyn LeaseRepository>,
    owner: String,
    held: Vec<(ShardId, ShardEpoch)>,
    nodes_started: Arc<Semaphore>,
) {
    let node_count = u32::try_from(NODES.len()).expect("two nodes fit in a u32");
    if nodes_started.acquire_many(node_count).await.is_err() {
        return;
    }
    tokio::time::sleep(REGISTRATION_ALLOWANCE).await;
    for (bundle, epoch) in held {
        if let Err(error) = leases.relinquish_bundle(bundle, owner.clone(), epoch).await {
            tracing::warn!(?bundle, %error, "the previous generation could not hand a bundle back");
        }
    }
}

/// An engine node host's software in the cluster: a piped node that reaches
/// the controller.
async fn serve_cluster_node(
    host: &'static str,
    store: InMemoryStore,
    peer_file: PathBuf,
    node_id: IncarnationId,
    started: Arc<Semaphore>,
) -> Result<(), Box<dyn Error>> {
    serve_piped_node(
        store,
        cluster_node_config(host, &peer_file)?,
        Some(controller_connection()),
        node_id,
        || started.add_permits(1),
    )
    .await
}

/// Listen on [`PORT`], start a node as `node_id` on `store` that reads
/// [`SimClock`], call `started`, then hand every accepted connection to the
/// node as an in-memory pipe.
///
/// `controller` must be set exactly when `config` names a controller, as
/// [`TokeiradHandle::start_in_memory_with_transport`] requires.
async fn serve_piped_node(
    store: InMemoryStore,
    config: TokeiraConfig,
    controller: Option<SharedHttp2Connection>,
    node_id: IncarnationId,
    started: impl FnOnce(),
) -> Result<(), Box<dyn Error>> {
    // Bound before the node starts, so a peer that learns this node's address
    // from the controller never finds the port closed.
    let listener = TcpListener::bind((IpAddr::V4(Ipv4Addr::UNSPECIFIED), PORT)).await?;
    let (pipes, incoming) = tokio::sync::mpsc::channel(64);
    let transport = NodeTransport {
        incoming,
        peers: peer_connector(),
        controller,
    };
    // Dropping the handle stops the node, so it lives as long as the host.
    let _node = TokeiradHandle::start_in_memory_with_transport(
        store,
        transport,
        config,
        Arc::new(SimClock),
        node_id,
    )
    .await?;
    started();
    loop {
        let (mut stream, _) = listener.accept().await?;
        let (node_end, mut host_end) = tokio::io::duplex(PIPE_BUFFER);
        pipes.send(node_end).await?;
        tokio::spawn(async move {
            // Either side closing ends the copy, and dropping both ends then
            // closes the other side, which is all a connection teardown needs.
            let _ = tokio::io::copy_bidirectional(&mut stream, &mut host_end).await;
        });
    }
}

fn cluster_node_config(host: &str, peer_file: &Path) -> anyhow::Result<TokeiraConfig> {
    let mut config = TokeiraConfig::default();
    let placement = &mut config.infrastructure.placement;
    placement.controller_endpoint = Some(controller_endpoint());
    placement.node_host = host.to_owned();
    placement.node_port = Some(PORT);
    placement.shard_count = BUNDLES;
    placement.bundle_count = BUNDLES;
    placement.partition_count = PARTITIONS;
    config.infrastructure.peer = Some(PeerConfig {
        path: peer_file
            .to_str()
            .context("the peer file's path is not UTF-8")?
            .to_owned(),
    });
    Ok(config)
}

pub(crate) fn controller_endpoint() -> String {
    format!("http://{CONTROLLER}:{CONTROLLER_PORT}")
}

/// One HTTP/2 connection to the controller over turmoil's TCP. Building it
/// spawns its driver on the current host's runtime, so each host builds its
/// own.
pub(crate) fn controller_connection() -> SharedHttp2Connection {
    let authority = controller_endpoint()
        .parse()
        .expect("the controller endpoint is a valid URI");
    Http2Connection::lazy_with_connector(turmoil_connector(), authority).shared(1024)
}

fn peer_connector() -> PeerConnector {
    Arc::new(|endpoint: &Endpoint| endpoint.connect_with_connector_lazy(turmoil_connector()))
}

pub(crate) fn placement_config() -> PlacementConfig {
    PlacementConfig {
        shard_count: BUNDLES,
        bundle_count: BUNDLES,
        partition_count: PARTITIONS,
        hash_version: 1,
    }
}

/// The cluster's peer credential, generated for one run and removed after it.
///
/// Both nodes load the same file, as nodes of one cluster do. The file is
/// created owner-only, and its path resolves no symlinks, because peer
/// bootstrap refuses anything else.
struct PeerFile {
    dir: PathBuf,
    path: PathBuf,
}

impl PeerFile {
    fn generate() -> anyhow::Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        // macOS's temporary directory sits behind the `/var` symlink.
        let base = std::env::temp_dir()
            .canonicalize()
            .context("could not resolve the temporary directory")?;
        let dir = base.join(format!(
            "turmoil-sim-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&dir).with_context(|| format!("could not create {}", dir.display()))?;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))?;
        let path = dir.join("peer.json");
        let material = PeerMaterial::generate()?;
        let mut file = create_protected_file(&path)?;
        file.write_all(&material.protected_bytes()?)?;
        file.sync_all()?;
        Ok(Self { dir, path })
    }
}

impl Drop for PeerFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
