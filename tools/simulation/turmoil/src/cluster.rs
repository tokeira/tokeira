//! Step two: two engine nodes and a placement controller on turmoil.
//!
//! Five simulated hosts:
//!
//! - `controller` runs the placement controller, composed from
//!   `tokeira-controller`'s public parts as `apps/tokeira-controller` composes
//!   them, and serves it over turmoil's TCP.
//! - `node-a` and `node-b` each run a listener-backed engine node through
//!   [`TokeiradHandle::start_in_memory_with_transport`]. Each node's gRPC
//!   listener, its calls to the other node and its calls to the controller all
//!   cross turmoil's network.
//! - `worker` learns placement from the controller with the edge's own routing
//!   code, picks a task queue and a workflow id that put the work where the
//!   scenario needs it, and drives a one-activity workflow through the public
//!   gRPC API. Like a client behind a load balancer, it sends each call to one
//!   node and, when that node refuses it, to the other.
//! - `previous-generation` stands for the cluster the two nodes replace (see
//!   below). It runs nothing.
//!
//! The controller and both nodes share one [`InMemoryStore`], standing in for
//! the DSQL database a cluster shares. The store is shared memory, not
//! network, so a partition cuts node-to-node and node-to-controller traffic
//! but never a node's store.
//!
//! The cluster forms as a rolling replacement. The controller offers a node,
//! when it registers, every bundle nobody owns, and it never moves an owned
//! bundle (`compute_desired_placement` in `tokeira-controller`). Two nodes
//! started together would split the bundles only by the luck of a race, and
//! the node that registers second usually owns nothing. So a previous
//! generation, the fifth host, holds every bundle's lease until both nodes
//! have registered, then hands them back, and the controller's next placement
//! splits them between the two.
//!
//! Known limits, so they are not mistaken for oversights:
//! - Node incarnation ids are random UUIDs, not drawn from turmoil's seeded
//!   RNG, and the controller orders nodes by id. Placement, and with it the
//!   queue and workflow the worker picks, can differ between two runs of one
//!   seed, so these runs are not yet replayable.
//! - The controller times node heartbeats with `std::time::Instant`, which
//!   turmoil does not move. Faults that depend on a node's heartbeat lapsing
//!   at the controller need the controller on simulated time too. The nodes
//!   and every store handle already read [`SimClock`].

use std::{
    collections::BTreeSet,
    error::Error,
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

use anyhow::{Context as _, anyhow, bail};
use connectrpc::client::{Http2Connection, SharedHttp2Connection};
use hyper::server::conn::http2;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    service::TowerToHyperService,
};
use tokeira_auth::peer::PeerMaterial;
use tokeira_config::peer::{PeerConfig, create_protected_file};
use tokeira_controller::{ConnectPlacementController, ControllerConfig, PlacementControllerState};
use tokeira_edge::{
    CacheBackedRouter, EdgeRouter as _, PeerConnector, RouteTarget, RoutingCache,
    subscribe_routing_once, translate::to_internal::namespace_id_for,
};
use tokeira_engine::{NodeTransport, TokeiraConfig, TokeiradHandle};
use tokeira_proto::{
    common::{ActivityType, WorkflowExecution, WorkflowType},
    connect::tokeira::internal::controller::v1::PlacementControllerExt as _,
    enums::{CommandType, EventType},
    public::temporal::api::command::v1::{
        Command, CompleteWorkflowExecutionCommandAttributes, ScheduleActivityTaskCommandAttributes,
        command::Attributes,
    },
    taskqueue::TaskQueue,
    workflowservice::{
        GetWorkflowExecutionHistoryRequest, PollActivityTaskQueueRequest,
        PollActivityTaskQueueResponse, PollWorkflowTaskQueueRequest, PollWorkflowTaskQueueResponse,
        RespondActivityTaskCompletedRequest, RespondWorkflowTaskCompletedRequest,
        StartWorkflowExecutionRequest, workflow_service_client::WorkflowServiceClient,
    },
};
use tokeira_storage::{ControlRepository, InMemoryStore, LeaseOutcome, LeaseRepository};
use tokeira_types::{
    IncarnationId, PlacementConfig, RoutingSnapshot, ShardEpoch, ShardId, TaskKind, WorkflowId,
    bundle_for_partition, workflow_queue_partition,
};
use tokio::sync::Semaphore;
use tonic::{
    Status,
    transport::{Channel, Endpoint},
};
use turmoil::net::TcpListener;

use crate::{
    IDENTITY, NAMESPACE, PORT, SimClock, Trace, epoch, observe, text_payloads, turmoil_connector,
};

/// Simulated host name of the placement controller.
pub const CONTROLLER: &str = "controller";
/// Simulated host names of the two engine nodes.
pub const NODES: [&str; 2] = ["node-a", "node-b"];
/// Port the controller serves on, inside the simulated network.
pub const CONTROLLER_PORT: u16 = 7240;

const WORKER: &str = "worker";
/// Small enough that two nodes split the bundles visibly; `shard_count` must
/// equal `bundle_count`, which the node's config validation enforces.
const BUNDLES: u32 = 4;
const PARTITIONS: u32 = 16;
const SIMULATION_DURATION: Duration = Duration::from_secs(600);
/// How long the worker waits for both nodes to own bundles.
const SETTLE_PATIENCE: Duration = Duration::from_secs(120);
/// How long the worker keeps offering one call to the nodes before it gives
/// up and records that no node served it.
const CALL_PATIENCE: Duration = Duration::from_secs(120);
/// The worker's pause between rounds of offering a refused call. It is the
/// simulated worker's own backoff, in simulated time, not a test waiting on
/// real time.
const RETRY_DELAY: Duration = Duration::from_millis(500);
/// How long one poll may wait on a node for a task.
const POLL_TIMEOUT: Duration = Duration::from_secs(10);
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

/// Where a scenario puts its task queue, relative to the two places that
/// decide an activity queue's home.
///
/// The edge sends an activity poll to the node that owns the bundle it
/// computes from the namespace *name* and the Activity kind. The runtime
/// serves the poll only at the queue's home, which it computes from the
/// namespace *id* and the Workflow kind for every queue. This is the brief's
/// defect 1. Whether the two bundles have one owner depends on the queue name
/// and on placement, so the worker searches for a queue name that gives the
/// scenario it was asked for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueuePlacement {
    /// The edge's choice and the runtime's home are the same node.
    Agreeing,
    /// The edge's choice and the runtime's home are different nodes.
    Disagreeing,
}

/// Run a placement controller, two engine nodes and a scripted worker through
/// a one-activity workflow, with turmoil's RNG seeded from `seed`.
///
/// The workflow is homed on the node that is not its task queue's home, so its
/// workflow and activity tasks start across nodes. The returned trace records
/// placement, every distinct refusal the worker met, and the history; it ends
/// with a completed workflow only if every call found a node that served it.
pub fn run_two_node_activity_workflow(
    seed: u64,
    placement: QueuePlacement,
) -> anyhow::Result<Trace> {
    let peer_file = PeerFile::generate()?;
    // The store homes each run by its own shard count, which must match the
    // nodes' or a fenced write lands at a home its lease doesn't cover.
    let store = InMemoryStore::with_shard_count(BUNDLES);
    let trace = Arc::new(Mutex::new(Trace::default()));

    let mut builder = turmoil::Builder::new();
    builder
        .rng_seed(seed)
        .epoch(epoch())
        .simulation_duration(SIMULATION_DURATION);
    let mut sim = builder.build();
    // Each node adds one permit once it has started.
    let started = Arc::new(Semaphore::new(0));
    sim.host(PREVIOUS_GENERATION, || {
        std::future::pending::<Result<(), Box<dyn Error>>>()
    });
    // The controller is registered first so it is listening before a node's
    // startup asks it for the first routing snapshot.
    let controller_store = store.clone();
    let nodes_started = Arc::clone(&started);
    sim.host(CONTROLLER, move || {
        // The controller judges leases the nodes renew, so its handle on the
        // shared store reads the same simulated time theirs do.
        serve_controller(
            controller_store.clone().with_clock(Arc::new(SimClock)),
            Arc::clone(&nodes_started),
        )
    });
    for node in NODES {
        let node_store = store.clone();
        let peer_path = peer_file.path.clone();
        let node_started = Arc::clone(&started);
        sim.host(node, move || {
            serve_node(
                node,
                node_store.clone(),
                peer_path.clone(),
                Arc::clone(&node_started),
            )
        });
    }
    let observed = Arc::clone(&trace);
    sim.client(WORKER, async move {
        drive_worker(placement, &observed).await?;
        Ok(())
    });
    sim.run()
        .map_err(|error| anyhow!("simulation failed: {error}"))?;
    let trace = trace.lock().expect("trace lock is never poisoned").clone();
    Ok(trace)
}

/// The controller host's software, composed as `apps/tokeira-controller`
/// composes it, minus process signals, metrics and the DSQL connection.
async fn serve_controller(
    store: InMemoryStore,
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
    // Taken before the controller listens, so no node can register while a
    // bundle is still unowned.
    let previous = IncarnationId::new().to_string();
    let held = hold_every_bundle(leases.as_ref(), &previous).await?;
    tokio::spawn(hand_back_once_nodes_start(
        Arc::clone(&leases),
        previous,
        held,
        nodes_started,
    ));
    let control: Arc<dyn ControlRepository> = Arc::new(store);
    let state = PlacementControllerState::new(config, leases, control);
    tokio::spawn(placement_loop(state.clone()));
    tokio::spawn(budget_loop(state.clone()));

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
async fn budget_loop(state: PlacementControllerState) {
    let allocator = IncarnationId::new();
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
async fn serve_node(
    host: &'static str,
    store: InMemoryStore,
    peer_file: PathBuf,
    started: Arc<Semaphore>,
) -> Result<(), Box<dyn Error>> {
    serve_piped_node(
        store,
        node_config(host, &peer_file)?,
        Some(controller_connection()),
        || started.add_permits(1),
    )
    .await
}

/// Listen on [`PORT`], start a node on `store` that reads [`SimClock`], call
/// `started`, then hand every accepted connection to the node as an
/// in-memory pipe.
///
/// `controller` must be set exactly when `config` names a controller, as
/// [`TokeiradHandle::start_in_memory_with_transport`] requires.
pub(crate) async fn serve_piped_node(
    store: InMemoryStore,
    config: TokeiraConfig,
    controller: Option<SharedHttp2Connection>,
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

fn node_config(host: &str, peer_file: &Path) -> anyhow::Result<TokeiraConfig> {
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

fn controller_endpoint() -> String {
    format!("http://{CONTROLLER}:{CONTROLLER_PORT}")
}

/// One HTTP/2 connection to the controller over turmoil's TCP. Building it
/// spawns its driver on the current host's runtime, so each host builds its
/// own.
fn controller_connection() -> SharedHttp2Connection {
    let authority = controller_endpoint()
        .parse()
        .expect("the controller endpoint is a valid URI");
    Http2Connection::lazy_with_connector(turmoil_connector(), authority).shared(1024)
}

fn peer_connector() -> PeerConnector {
    Arc::new(|endpoint: &Endpoint| endpoint.connect_with_connector_lazy(turmoil_connector()))
}

fn placement_config() -> PlacementConfig {
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

/// The worker host's script.
async fn drive_worker(placement: QueuePlacement, trace: &Mutex<Trace>) -> anyhow::Result<()> {
    let routing = settled_placement().await?;
    let snapshot = routing.snapshot();
    observe(trace, describe_placement(&snapshot));

    let (queue, edge_host, home_host) = pick_queue(&routing, &snapshot, placement).await?;
    observe(
        trace,
        format!(
            "task queue {queue}: the edge sends activity polls to {edge_host}, the runtime \
             serves them on {home_host}"
        ),
    );
    let (workflow_id, workflow_host) = pick_workflow(&routing, &home_host)?;
    observe(
        trace,
        format!("workflow {workflow_id} is homed on {workflow_host}"),
    );

    let cluster = Cluster::new(trace)?;
    let task_queue = Some(TaskQueue {
        name: queue.clone(),
        ..Default::default()
    });

    let start = StartWorkflowExecutionRequest {
        namespace: NAMESPACE.to_owned(),
        workflow_id: workflow_id.clone(),
        workflow_type: Some(WorkflowType {
            name: "one-activity".to_owned(),
        }),
        task_queue: task_queue.clone(),
        request_id: "turmoil-start".to_owned(),
        identity: IDENTITY.to_owned(),
        ..Default::default()
    };
    let Some(_) = cluster
        .call(
            "StartWorkflowExecution",
            |_| true,
            |mut client| {
                let request = start.clone();
                async move { client.start_workflow_execution(request).await }
            },
        )
        .await
    else {
        return Ok(());
    };

    let workflow_poll = PollWorkflowTaskQueueRequest {
        namespace: NAMESPACE.to_owned(),
        task_queue: task_queue.clone(),
        identity: IDENTITY.to_owned(),
        ..Default::default()
    };
    let Some(first) = cluster.poll_workflow_task(&workflow_poll).await else {
        return Ok(());
    };
    let schedule = Command {
        command_type: CommandType::ScheduleActivityTask as i32,
        user_metadata: None,
        attributes: Some(Attributes::ScheduleActivityTaskCommandAttributes(
            ScheduleActivityTaskCommandAttributes {
                activity_id: "work-1".to_owned(),
                activity_type: Some(ActivityType {
                    name: "work".to_owned(),
                }),
                task_queue: task_queue.clone(),
                start_to_close_timeout: Some(prost_types::Duration {
                    seconds: 60,
                    nanos: 0,
                }),
                ..Default::default()
            },
        )),
    };
    if !cluster.complete_workflow_task(first, schedule).await {
        return Ok(());
    }

    let activity_poll = PollActivityTaskQueueRequest {
        namespace: NAMESPACE.to_owned(),
        task_queue: task_queue.clone(),
        identity: IDENTITY.to_owned(),
        ..Default::default()
    };
    let Some(activity) = cluster
        .call(
            "PollActivityTaskQueue",
            |task: &PollActivityTaskQueueResponse| !task.task_token.is_empty(),
            |mut client| {
                let request = with_poll_timeout(activity_poll.clone());
                async move { client.poll_activity_task_queue(request).await }
            },
        )
        .await
    else {
        return Ok(());
    };
    let completed = RespondActivityTaskCompletedRequest {
        namespace: NAMESPACE.to_owned(),
        task_token: activity.task_token,
        result: Some(text_payloads("done")),
        identity: IDENTITY.to_owned(),
        ..Default::default()
    };
    let Some(_) = cluster
        .call(
            "RespondActivityTaskCompleted",
            |_| true,
            |mut client| {
                let request = completed.clone();
                async move { client.respond_activity_task_completed(request).await }
            },
        )
        .await
    else {
        return Ok(());
    };

    let Some(second) = cluster.poll_workflow_task(&workflow_poll).await else {
        return Ok(());
    };
    let complete = Command {
        command_type: CommandType::CompleteWorkflowExecution as i32,
        user_metadata: None,
        attributes: Some(Attributes::CompleteWorkflowExecutionCommandAttributes(
            CompleteWorkflowExecutionCommandAttributes {
                result: Some(text_payloads("finished")),
            },
        )),
    };
    if !cluster.complete_workflow_task(second, complete).await {
        return Ok(());
    }

    let history_request = GetWorkflowExecutionHistoryRequest {
        namespace: NAMESPACE.to_owned(),
        execution: Some(WorkflowExecution {
            workflow_id,
            run_id: String::new(),
        }),
        ..Default::default()
    };
    let Some(history) = cluster
        .call(
            "GetWorkflowExecutionHistory",
            |_| true,
            |mut client| {
                let request = history_request.clone();
                async move { client.get_workflow_execution_history(request).await }
            },
        )
        .await
    else {
        return Ok(());
    };
    for event in history.history.unwrap_or_default().events {
        let name = EventType::try_from(event.event_type)
            .map_or("EVENT_TYPE_UNKNOWN", |kind| kind.as_str_name());
        observe(trace, format!("history event {} {name}", event.event_id));
    }
    Ok(())
}

/// Ask the controller for routing until every bundle has an owner and both
/// nodes own at least one, using the edge's own subscription code.
async fn settled_placement() -> anyhow::Result<Arc<RoutingCache>> {
    let connection = controller_connection();
    let mut last = String::from("no snapshot yet");
    while elapsed() < SETTLE_PATIENCE {
        let routing = Arc::new(RoutingCache::new(placement_config()));
        match subscribe_routing_once(
            Arc::clone(&routing),
            controller_endpoint(),
            Some(connection.clone()),
        )
        .await
        {
            Ok(()) => {
                let snapshot = routing.snapshot();
                let owners: BTreeSet<&str> = (0..BUNDLES)
                    .filter_map(|bundle| owner_host(&snapshot, ShardId(bundle)))
                    .collect();
                let every_bundle_owned =
                    (0..BUNDLES).all(|bundle| owner_host(&snapshot, ShardId(bundle)).is_some());
                let only_nodes_own = owners.iter().all(|host| NODES.contains(host));
                if every_bundle_owned && only_nodes_own && owners.len() == NODES.len() {
                    return Ok(routing);
                }
                last = describe_placement(&snapshot);
            }
            Err(error) => last = format!("{error:#}"),
        }
        tokio::time::sleep(RETRY_DELAY).await;
    }
    bail!("placement did not settle within {SETTLE_PATIENCE:?}; last seen: {last}")
}

/// The host that owns `bundle` in `snapshot`, if any.
fn owner_host(snapshot: &RoutingSnapshot, bundle: ShardId) -> Option<&str> {
    let owner = snapshot.lookup_bundle_owner(bundle)?;
    Some(snapshot.lookup_node_endpoint(owner.node_id)?.host.as_str())
}

fn describe_placement(snapshot: &RoutingSnapshot) -> String {
    let owners: Vec<String> = (0..BUNDLES)
        .map(|bundle| {
            format!(
                "bundle {bundle} on {}",
                owner_host(snapshot, ShardId(bundle)).unwrap_or("nobody")
            )
        })
        .collect();
    format!("placement settled: {}", owners.join(", "))
}

/// Find a queue name whose activity polls the edge sends to the runtime's
/// queue home (`Agreeing`) or away from it (`Disagreeing`).
///
/// The edge's side is asked of the edge's own router. The runtime's side uses
/// the function the runtime's queue-home provider calls
/// (`tokeira_runtime::discovery::PlacementQueueHomes::local_home`).
async fn pick_queue(
    routing: &Arc<RoutingCache>,
    snapshot: &RoutingSnapshot,
    placement: QueuePlacement,
) -> anyhow::Result<(String, String, String)> {
    // An id no node has, so every route names the node it goes to.
    let router = CacheBackedRouter::new(Arc::clone(routing), IncarnationId::new());
    for index in 0..256 {
        let queue = format!("turmoil-queue-{index}");
        let RouteTarget::Remote { target } = router
            .route_task_queue(NAMESPACE, &queue, TaskKind::Activity)
            .await?
        else {
            bail!("the edge routed {queue} to an outside node id");
        };
        let edge_host = target.split(':').next().unwrap_or_default().to_owned();
        let home = bundle_for_partition(
            workflow_queue_partition(namespace_id_for(NAMESPACE), &queue, PARTITIONS),
            BUNDLES,
        );
        let home_host = owner_host(snapshot, home)
            .context("a settled placement owns every bundle")?
            .to_owned();
        if (edge_host == home_host) == (placement == QueuePlacement::Agreeing) {
            return Ok((queue, edge_host, home_host));
        }
    }
    bail!("no queue name among 256 gives {placement:?} placement")
}

/// Find a workflow id homed away from `avoid_host`, so starting its tasks
/// crosses from the queue's home to the execution's home.
fn pick_workflow(routing: &RoutingCache, avoid_host: &str) -> anyhow::Result<(String, String)> {
    for index in 0..256 {
        let workflow_id = format!("turmoil-workflow-{index}");
        let (_, endpoint, _) = routing
            .resolve_execution_home(
                namespace_id_for(NAMESPACE),
                &WorkflowId(workflow_id.clone()),
            )
            .context("a settled placement routes every workflow")?;
        if endpoint.host != avoid_host {
            return Ok((workflow_id, endpoint.host));
        }
    }
    bail!("every workflow id among 256 is homed on {avoid_host}")
}

/// A poll that gives up on one node after [`POLL_TIMEOUT`], so the worker can
/// try the other.
fn with_poll_timeout<T>(message: T) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    request.set_timeout(POLL_TIMEOUT);
    request
}

/// The worker's view of the cluster: one client per node, and a trace.
struct Cluster<'a> {
    nodes: Vec<(&'static str, WorkflowServiceClient<Channel>)>,
    trace: &'a Mutex<Trace>,
}

impl<'a> Cluster<'a> {
    fn new(trace: &'a Mutex<Trace>) -> anyhow::Result<Self> {
        let nodes = NODES
            .iter()
            .map(|host| {
                let channel = Endpoint::from_shared(format!("http://{host}:{PORT}"))?
                    .connect_with_connector_lazy(turmoil_connector());
                Ok((*host, WorkflowServiceClient::new(channel)))
            })
            .collect::<anyhow::Result<_>>()?;
        Ok(Self { nodes, trace })
    }

    /// Offer one call to each node in turn until a node's reply satisfies
    /// `served`, as a client behind a load balancer would.
    ///
    /// Each distinct refusal is traced once, so the trace shows how nodes
    /// refuse without one line per retry. Returns `None`, after tracing that
    /// it gave up, if no node served the call within [`CALL_PATIENCE`].
    async fn call<T, F, Fut>(
        &self,
        what: &str,
        served: impl Fn(&T) -> bool,
        mut call: F,
    ) -> Option<T>
    where
        F: FnMut(WorkflowServiceClient<Channel>) -> Fut,
        Fut: Future<Output = Result<tonic::Response<T>, Status>>,
    {
        let give_up_at = elapsed() + CALL_PATIENCE;
        let mut refusals = BTreeSet::new();
        let mut attempts = 0_u32;
        loop {
            for (host, client) in &self.nodes {
                attempts += 1;
                match call(client.clone()).await {
                    Ok(reply) => {
                        let reply = reply.into_inner();
                        if served(&reply) {
                            observe(self.trace, format!("{what} served by {host}"));
                            return Some(reply);
                        }
                    }
                    Err(status) => {
                        let refusal = format!(
                            "{what} refused by {host}: {:?}: {}",
                            status.code(),
                            status.message()
                        );
                        if refusals.insert(refusal.clone()) {
                            observe(self.trace, refusal);
                        }
                    }
                }
            }
            if elapsed() >= give_up_at {
                observe(
                    self.trace,
                    format!("gave up on {what} after {attempts} attempts across both nodes"),
                );
                return None;
            }
            tokio::time::sleep(RETRY_DELAY).await;
        }
    }

    async fn poll_workflow_task(
        &self,
        request: &PollWorkflowTaskQueueRequest,
    ) -> Option<PollWorkflowTaskQueueResponse> {
        self.call(
            "PollWorkflowTaskQueue",
            |task: &PollWorkflowTaskQueueResponse| !task.task_token.is_empty(),
            |mut client| {
                let request = with_poll_timeout(request.clone());
                async move { client.poll_workflow_task_queue(request).await }
            },
        )
        .await
    }

    /// Complete `task` with one command. Returns whether a node accepted it.
    async fn complete_workflow_task(
        &self,
        task: PollWorkflowTaskQueueResponse,
        command: Command,
    ) -> bool {
        let request = RespondWorkflowTaskCompletedRequest {
            namespace: NAMESPACE.to_owned(),
            task_token: task.task_token,
            commands: vec![command],
            identity: IDENTITY.to_owned(),
            ..Default::default()
        };
        self.call(
            "RespondWorkflowTaskCompleted",
            |_| true,
            |mut client| {
                let request = request.clone();
                async move { client.respond_workflow_task_completed(request).await }
            },
        )
        .await
        .is_some()
    }
}

fn elapsed() -> Duration {
    turmoil::sim_elapsed().unwrap_or_default()
}
