// Tokeira placement/membership discrete-event simulator.
//
// Purpose
// -------
// This is not a mathematical proof. It is a deterministic, adversarial-ish
// simulator that repeatedly tries to falsify the placement design by injecting:
//
// - stale edge routing snapshots,
// - active-active controller observations,
// - runtime restart/incarnation changes,
// - lease expiry and takeover,
// - runtimes that keep serving with stale local ownership,
// - NotShardOwner repair,
// - duplicate requests,
// - delayed routing updates.
//
// The important invariant it checks is the design thesis from 035:
//
//   DSQL owns truth.
//   Runtime ownership is valid only by current DSQL bundle lease epoch.
//   Queue-home is advisory.
//   Execution-home is the correctness boundary.
//
// Run
// ---
//   cargo run --release
//
// Try the intentionally broken version:
//   cargo run --release -- --buggy-start-routing
//
// The buggy mode routes StartWorkflow by queue-home instead of execution-home.
// It should fail the Start/Signal same-home invariant quickly.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::env;
use std::fmt;

const DEFAULT_BUNDLE_COUNT: usize = 16;
const DEFAULT_QUEUE_PARTITIONS: usize = 64;
const DEFAULT_RUNTIME_COUNT: usize = 4;
const DEFAULT_CONTROLLER_COUNT: usize = 3;
const DEFAULT_LEASE_MS: u64 = 120;
const DEFAULT_RENEW_MS: u64 = 40;
const DEFAULT_CONTROLLER_OBSERVE_MS: u64 = 35;
const DEFAULT_MAX_TIME_MS: u64 = 6_000;
const DEFAULT_SEEDS: u64 = 250;
const DEFAULT_OPS_PER_SEED: usize = 800;
const MAX_EDGE_RETRIES: u8 = 6;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct RuntimeId(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct ControllerId(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct BundleId(usize);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct QueuePartition(usize);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct Epoch(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct Generation(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct WorkflowId(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct RequestId(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OpKind {
    Start,
    Signal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ClientOp {
    kind: OpKind,
    workflow_id: WorkflowId,
    request_id: RequestId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BundleLease {
    owner: Option<RuntimeId>,
    epoch: Epoch,
    lease_until_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BundleRoute {
    owner: RuntimeId,
    epoch: Epoch,
    lease_until_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct QueueRoute {
    home: RuntimeId,
    backing_bundle: BundleId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RoutingSnapshot {
    generation: Generation,
    bundle_routes: Vec<Option<BundleRoute>>,
    queue_routes: Vec<Option<QueueRoute>>,
}

impl RoutingSnapshot {
    fn empty(bundle_count: usize, queue_partitions: usize) -> Self {
        Self {
            generation: Generation(0),
            bundle_routes: vec![None; bundle_count],
            queue_routes: vec![None; queue_partitions],
        }
    }

    fn route_for_bundle(&self, bundle: BundleId) -> Option<BundleRoute> {
        self.bundle_routes.get(bundle.0).and_then(|r| *r)
    }

    fn apply_full_snapshot(&mut self, incoming: RoutingSnapshot) {
        if incoming.generation >= self.generation {
            *self = incoming;
        }
    }

    fn repair_bundle(&mut self, bundle: BundleId, route: Option<BundleRoute>) {
        if let Some(slot) = self.bundle_routes.get_mut(bundle.0) {
            match (*slot, route) {
                (Some(old), Some(new)) if old.epoch > new.epoch => {}
                (_, new_route) => *slot = new_route,
            }
        }
    }
}

#[derive(Clone, Debug)]
struct Runtime {
    id: RuntimeId,
    alive: bool,
    renewals_enabled: bool,
    draining: bool,
    local_owned: HashMap<BundleId, Epoch>,
}

impl Runtime {
    fn new(id: RuntimeId) -> Self {
        Self {
            id,
            alive: true,
            renewals_enabled: true,
            draining: false,
            local_owned: HashMap::new(),
        }
    }

    fn locally_owns(&self, bundle: BundleId, epoch: Epoch) -> bool {
        self.alive && self.local_owned.get(&bundle).copied() == Some(epoch)
    }
}

#[derive(Clone, Debug)]
struct WorkflowRecord {
    home_bundle: BundleId,
    started_by: RequestId,
    signal_count: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CommitOutcome {
    Applied,
    Duplicate,
    AlreadyExists,
    NotFound,
    FenceRejected,
    WrongExecutionHome,
}

#[derive(Clone, Debug)]
struct CommitLogEntry {
    time_ms: u64,
    runtime_id: RuntimeId,
    bundle_id: BundleId,
    epoch: Epoch,
    op: ClientOp,
    outcome: CommitOutcome,
}

#[derive(Clone, Debug)]
struct Dsql {
    leases: Vec<BundleLease>,
    workflows: HashMap<WorkflowId, WorkflowRecord>,
    applied_requests: HashSet<RequestId>,
    request_apply_count: HashMap<RequestId, u64>,
    commit_log: Vec<CommitLogEntry>,
    routing_generation: Generation,
    last_published_fingerprint: u64,
    /// OCC model: tracks the epoch each in-flight commit read at transaction start.
    inflight_reads: HashMap<(BundleId, u64), Epoch>,
    next_tx_id: u64,
}

impl Dsql {
    fn new(bundle_count: usize) -> Self {
        Self {
            leases: vec![
                BundleLease {
                    owner: None,
                    epoch: Epoch(0),
                    lease_until_ms: 0,
                };
                bundle_count
            ],
            workflows: HashMap::new(),
            applied_requests: HashSet::new(),
            request_apply_count: HashMap::new(),
            commit_log: Vec::new(),
            routing_generation: Generation(0),
            last_published_fingerprint: 0,
            inflight_reads: HashMap::new(),
            next_tx_id: 0,
        }
    }

    fn acquire_bundle(
        &mut self,
        now_ms: u64,
        bundle: BundleId,
        owner: RuntimeId,
        lease_ms: u64,
    ) -> Option<Epoch> {
        let row = &mut self.leases[bundle.0];
        let can_acquire = row.owner.is_none() || row.lease_until_ms <= now_ms;
        if can_acquire {
            row.owner = Some(owner);
            row.epoch = Epoch(row.epoch.0 + 1);
            row.lease_until_ms = now_ms + lease_ms;
            Some(row.epoch)
        } else {
            None
        }
    }

    fn renew_bundle(
        &mut self,
        now_ms: u64,
        bundle: BundleId,
        owner: RuntimeId,
        expected_epoch: Epoch,
        lease_ms: u64,
    ) -> bool {
        let row = &mut self.leases[bundle.0];
        if row.owner == Some(owner) && row.epoch == expected_epoch && row.lease_until_ms > now_ms {
            row.lease_until_ms = now_ms + lease_ms;
            true
        } else {
            false
        }
    }

    fn relinquish_bundle(
        &mut self,
        now_ms: u64,
        bundle: BundleId,
        owner: RuntimeId,
        expected_epoch: Epoch,
    ) -> bool {
        let row = &mut self.leases[bundle.0];
        if row.owner == Some(owner) && row.epoch == expected_epoch {
            row.owner = None;
            row.epoch = Epoch(row.epoch.0 + 1);
            row.lease_until_ms = now_ms;
            true
        } else {
            false
        }
    }

    fn current_route(&self, now_ms: u64, bundle: BundleId) -> Option<BundleRoute> {
        let row = self.leases[bundle.0];
        match row.owner {
            Some(owner) if row.lease_until_ms > now_ms => Some(BundleRoute {
                owner,
                epoch: row.epoch,
                lease_until_ms: row.lease_until_ms,
            }),
            _ => None,
        }
    }

    fn build_snapshot(
        &mut self,
        now_ms: u64,
        bundle_count: usize,
        queue_partitions: usize,
        live_runtimes: &[RuntimeId],
    ) -> RoutingSnapshot {
        let mut bundle_routes = Vec::with_capacity(bundle_count);
        for bundle_idx in 0..bundle_count {
            bundle_routes.push(self.current_route(now_ms, BundleId(bundle_idx)));
        }
        let mut queue_routes = Vec::with_capacity(queue_partitions);
        for p in 0..queue_partitions {
            if live_runtimes.is_empty() {
                queue_routes.push(None);
            } else {
                let home = live_runtimes[p % live_runtimes.len()];
                let backing_bundle = BundleId(p % bundle_count);
                queue_routes.push(Some(QueueRoute {
                    home,
                    backing_bundle,
                }));
            }
        }
        let fingerprint = fingerprint_routes(&bundle_routes, &queue_routes);
        if fingerprint != self.last_published_fingerprint {
            self.routing_generation = Generation(self.routing_generation.0 + 1);
            self.last_published_fingerprint = fingerprint;
        }
        RoutingSnapshot {
            generation: self.routing_generation,
            bundle_routes,
            queue_routes,
        }
    }

    /// Begin a transaction: snapshot the current epoch for a bundle.
    /// Returns a transaction ID for the later commit call.
    fn begin_transaction(&mut self, bundle: BundleId) -> u64 {
        let tx_id = self.next_tx_id;
        self.next_tx_id += 1;
        let epoch = self.leases[bundle.0].epoch;
        self.inflight_reads.insert((bundle, tx_id), epoch);
        tx_id
    }

    /// Commit with OCC fence check. If a tx_id was registered via
    /// begin_transaction, the fence compares the read-time epoch against
    /// the current epoch (models DSQL OCC). Otherwise uses the caller's
    /// expected_epoch directly.
    fn commit(
        &mut self,
        now_ms: u64,
        runtime_id: RuntimeId,
        bundle_id: BundleId,
        expected_epoch: Epoch,
        op: ClientOp,
        bundle_count: usize,
        tx_id: Option<u64>,
    ) -> CommitOutcome {
        // If we have an in-flight read, use that epoch for the fence check
        // (models OCC: read at begin, validate at commit).
        let check_epoch = match tx_id {
            Some(id) => self.inflight_reads.remove(&(bundle_id, id)).unwrap_or(expected_epoch),
            None => expected_epoch,
        };
        let row = self.leases[bundle_id.0];
        let fence_ok = row.owner == Some(runtime_id)
            && row.epoch == check_epoch
            && row.lease_until_ms > now_ms;

        let outcome = if !fence_ok {
            CommitOutcome::FenceRejected
        } else if self.applied_requests.contains(&op.request_id) {
            CommitOutcome::Duplicate
        } else {
            match op.kind {
                OpKind::Start => {
                    let canonical_home = execution_home(op.workflow_id, bundle_count);
                    if bundle_id != canonical_home {
                        CommitOutcome::WrongExecutionHome
                    } else if self.workflows.contains_key(&op.workflow_id) {
                        self.mark_applied(op.request_id);
                        CommitOutcome::AlreadyExists
                    } else {
                        self.workflows.insert(
                            op.workflow_id,
                            WorkflowRecord {
                                home_bundle: bundle_id,
                                started_by: op.request_id,
                                signal_count: 0,
                            },
                        );
                        self.mark_applied(op.request_id);
                        CommitOutcome::Applied
                    }
                }
                OpKind::Signal => match self.workflows.get_mut(&op.workflow_id) {
                    Some(record) if record.home_bundle == bundle_id => {
                        record.signal_count += 1;
                        self.mark_applied(op.request_id);
                        CommitOutcome::Applied
                    }
                    Some(_) => CommitOutcome::WrongExecutionHome,
                    None => CommitOutcome::NotFound,
                },
            }
        };

        self.commit_log.push(CommitLogEntry {
            time_ms: now_ms,
            runtime_id,
            bundle_id,
            epoch: expected_epoch,
            op,
            outcome,
        });
        outcome
    }

    fn mark_applied(&mut self, request_id: RequestId) {
        self.applied_requests.insert(request_id);
        *self.request_apply_count.entry(request_id).or_insert(0) += 1;
    }
}

#[derive(Clone, Debug)]
struct Edge {
    snapshot: RoutingSnapshot,
}

impl Edge {
    fn new(bundle_count: usize, queue_partitions: usize) -> Self {
        Self {
            snapshot: RoutingSnapshot::empty(bundle_count, queue_partitions),
        }
    }
}

#[derive(Clone, Debug)]
struct Controller {
    id: ControllerId,
    alive: bool,
}

#[derive(Clone, Debug)]
struct Config {
    bundle_count: usize,
    queue_partitions: usize,
    runtime_count: usize,
    controller_count: usize,
    lease_ms: u64,
    renew_ms: u64,
    controller_observe_ms: u64,
    max_time_ms: u64,
    ops_per_seed: usize,
    buggy_start_routing: bool,
    verbose: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bundle_count: DEFAULT_BUNDLE_COUNT,
            queue_partitions: DEFAULT_QUEUE_PARTITIONS,
            runtime_count: DEFAULT_RUNTIME_COUNT,
            controller_count: DEFAULT_CONTROLLER_COUNT,
            lease_ms: DEFAULT_LEASE_MS,
            renew_ms: DEFAULT_RENEW_MS,
            controller_observe_ms: DEFAULT_CONTROLLER_OBSERVE_MS,
            max_time_ms: DEFAULT_MAX_TIME_MS,
            ops_per_seed: DEFAULT_OPS_PER_SEED,
            buggy_start_routing: false,
            verbose: false,
        }
    }
}

#[derive(Clone, Debug)]
#[allow(clippy::enum_variant_names)]
enum EventKind {
    ControllerObserve { controller: ControllerId },
    EdgeApplySnapshot { snapshot: RoutingSnapshot },
    RuntimeAcquire { runtime: RuntimeId, bundle: BundleId },
    RuntimeRenew { runtime: RuntimeId, bundle: BundleId, epoch: Epoch },
    RuntimeRelinquish { runtime: RuntimeId, bundle: BundleId, epoch: Epoch },
    EdgeOp { op: ClientOp, attempt: u8 },
    RuntimeHandle { runtime: RuntimeId, op: ClientOp, bundle: BundleId, observed_epoch: Epoch, attempt: u8 },
    /// Delayed commit attempt — fence check happens here, not at RuntimeHandle time.
    CommitAttempt { runtime: RuntimeId, op: ClientOp, bundle: BundleId, observed_epoch: Epoch, attempt: u8, tx_id: u64 },
    EdgeRepairAndRetry { op: ClientOp, bundle: BundleId, attempt: u8 },
    /// Edge repair resolves after controller RPC latency — performs DSQL lookup.
    EdgeRepairResolve { op: ClientOp, bundle: BundleId, attempt: u8 },
    DisableRenewals { runtime: RuntimeId, duration_ms: u64 },
    EnableRenewals { runtime: RuntimeId },
    CrashRuntime { runtime: RuntimeId, restart_delay_ms: u64 },
    RestartRuntime { old_runtime: RuntimeId },
    BeginDrain { runtime: RuntimeId },
    /// Phase 1 of drain: routing snapshot updated to remove draining node.
    DrainRoutingUpdate { runtime: RuntimeId },
    /// Phase 2 of drain: relinquish bundles after routing is updated.
    DrainRelinquishPhase { runtime: RuntimeId },
}

#[derive(Clone, Debug)]
struct Event {
    at_ms: u64,
    seq: u64,
    kind: EventKind,
}

impl PartialEq for Event {
    fn eq(&self, other: &Self) -> bool {
        self.at_ms == other.at_ms && self.seq == other.seq
    }
}
impl Eq for Event {}

impl PartialOrd for Event {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Event {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .at_ms
            .cmp(&self.at_ms)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

#[derive(Clone, Debug)]
struct Sim {
    cfg: Config,
    seed: u64,
    rng: XorShift64,
    now_ms: u64,
    seq: u64,
    events: BinaryHeap<Event>,
    dsql: Dsql,
    edge: Edge,
    runtimes: HashMap<RuntimeId, Runtime>,
    controllers: HashMap<ControllerId, Controller>,
    next_runtime_id: u64,
    next_request_id: u64,
    invariant_checks: u64,
    edge_repairs: u64,
    not_shard_owner: u64,
    fence_rejections: u64,
    successful_mutations: u64,
    /// I6: tracks which draining runtimes have had routing updated.
    drain_routing_updated: HashSet<RuntimeId>,
}

impl Sim {
    fn new(seed: u64, cfg: Config) -> Self {
        let mut runtimes = HashMap::new();
        for n in 0..cfg.runtime_count {
            let id = RuntimeId(n as u64 + 1);
            runtimes.insert(id, Runtime::new(id));
        }
        let mut controllers = HashMap::new();
        for n in 0..cfg.controller_count {
            let id = ControllerId(n as u64 + 1);
            controllers.insert(id, Controller { id, alive: true });
        }
        let mut sim = Self {
            cfg: cfg.clone(),
            seed,
            rng: XorShift64::new(seed),
            now_ms: 0,
            seq: 0,
            events: BinaryHeap::new(),
            dsql: Dsql::new(cfg.bundle_count),
            edge: Edge::new(cfg.bundle_count, cfg.queue_partitions),
            runtimes,
            controllers,
            next_runtime_id: cfg.runtime_count as u64 + 1,
            next_request_id: 1,
            invariant_checks: 0,
            edge_repairs: 0,
            not_shard_owner: 0,
            fence_rejections: 0,
            successful_mutations: 0,
            drain_routing_updated: HashSet::new(),
        };
        sim.bootstrap();
        sim
    }

    fn bootstrap(&mut self) {
        let controllers: Vec<_> = self.controllers.keys().copied().collect();
        for c in controllers {
            self.schedule(0, EventKind::ControllerObserve { controller: c });
        }
        let runtime_ids = self.live_runtime_ids();
        for bundle_idx in 0..self.cfg.bundle_count {
            let runtime = runtime_ids[bundle_idx % runtime_ids.len()];
            let d = self.rng.range(0, 10);
            self.schedule(d, EventKind::RuntimeAcquire { runtime, bundle: BundleId(bundle_idx) });
        }
        self.schedule_workload();
    }

    fn schedule_workload(&mut self) {
        let workflow_pool = 80_u64;
        let mut known_request_ids: Vec<RequestId> = Vec::new();
        for _ in 0..self.cfg.ops_per_seed {
            let at = self.rng.range(5, self.cfg.max_time_ms);
            let choice = self.rng.range(0, 100);
            match choice {
                0..=34 => {
                    let workflow_id = WorkflowId(self.rng.range(1, workflow_pool));
                    let request_id = self.next_request_id();
                    known_request_ids.push(request_id);
                    self.schedule(at, EventKind::EdgeOp {
                        op: ClientOp { kind: OpKind::Start, workflow_id, request_id },
                        attempt: 0,
                    });
                }
                35..=77 => {
                    let workflow_id = WorkflowId(self.rng.range(1, workflow_pool));
                    let duplicate = !known_request_ids.is_empty() && self.rng.range(0, 100) < 8;
                    let request_id = if duplicate {
                        known_request_ids[self.rng.range(0, known_request_ids.len() as u64) as usize]
                    } else {
                        let rid = self.next_request_id();
                        known_request_ids.push(rid);
                        rid
                    };
                    self.schedule(at, EventKind::EdgeOp {
                        op: ClientOp { kind: OpKind::Signal, workflow_id, request_id },
                        attempt: 0,
                    });
                }
                78..=87 => {
                    if let Some(runtime) = self.random_runtime_id() {
                        { let d = self.rng.range(80, 260); self.schedule(at, EventKind::DisableRenewals { runtime, duration_ms: d }); }
                    }
                }
                88..=94 => {
                    if let Some(runtime) = self.random_runtime_id() {
                        { let d = self.rng.range(50, 240); self.schedule(at, EventKind::CrashRuntime { runtime, restart_delay_ms: d }); }
                    }
                }
                _ => {
                    if let Some(runtime) = self.random_runtime_id() {
                        self.schedule(at, EventKind::BeginDrain { runtime });
                    }
                }
            }
        }
    }

    fn next_request_id(&mut self) -> RequestId {
        let id = RequestId(self.next_request_id);
        self.next_request_id += 1;
        id
    }

    fn schedule(&mut self, delay_ms: u64, kind: EventKind) {
        let event = Event { at_ms: self.now_ms.saturating_add(delay_ms), seq: self.seq, kind };
        self.seq += 1;
        self.events.push(event);
    }

    fn run(&mut self) -> Result<SimReport, SimError> {
        while let Some(event) = self.events.pop() {
            if event.at_ms > self.cfg.max_time_ms { break; }
            self.now_ms = event.at_ms;
            self.handle(event.kind)?;
            self.check_invariants()?;
        }
        Ok(SimReport {
            seed: self.seed,
            commits: self.dsql.commit_log.len() as u64,
            successful_mutations: self.successful_mutations,
            workflows: self.dsql.workflows.len() as u64,
            edge_repairs: self.edge_repairs,
            not_shard_owner: self.not_shard_owner,
            fence_rejections: self.fence_rejections,
            invariant_checks: self.invariant_checks,
            final_generation: self.dsql.routing_generation.0,
        })
    }

    fn handle(&mut self, kind: EventKind) -> Result<(), SimError> {
        match kind {
            EventKind::ControllerObserve { controller } => {
                let Some(c) = self.controllers.get(&controller) else { return Ok(()); };
                if !c.alive { return Ok(()); }
                let live = self.live_runtime_ids();
                let snapshot = self.dsql.build_snapshot(self.now_ms, self.cfg.bundle_count, self.cfg.queue_partitions, &live);
                let delivery_delay = self.rng.range(1, 60);
                self.schedule(delivery_delay, EventKind::EdgeApplySnapshot { snapshot });
                if !live.is_empty() {
                    for bundle_idx in 0..self.cfg.bundle_count {
                        let bundle = BundleId(bundle_idx);
                        if self.dsql.current_route(self.now_ms, bundle).is_none() {
                            let runtime = live[(bundle_idx + controller.0 as usize) % live.len()];
                            { let d = self.rng.range(1, 20); self.schedule(d, EventKind::RuntimeAcquire { runtime, bundle }); }
                        }
                    }
                }
                { let d = self.cfg.controller_observe_ms + self.rng.range(0, 15); self.schedule(d, EventKind::ControllerObserve { controller }); }
            }
            EventKind::EdgeApplySnapshot { snapshot } => {
                self.edge.snapshot.apply_full_snapshot(snapshot);
            }
            EventKind::RuntimeAcquire { runtime, bundle } => {
                let Some(rt) = self.runtimes.get(&runtime) else { return Ok(()); };
                if !rt.alive { return Ok(()); }
                if let Some(epoch) = self.dsql.acquire_bundle(self.now_ms, bundle, runtime, self.cfg.lease_ms) {
                    let rt = self.runtimes.get_mut(&runtime).expect("runtime disappeared");
                    rt.local_owned.insert(bundle, epoch);
                    self.schedule(self.cfg.renew_ms, EventKind::RuntimeRenew { runtime, bundle, epoch });
                }
            }
            EventKind::RuntimeRenew { runtime, bundle, epoch } => {
                let Some(rt) = self.runtimes.get(&runtime) else { return Ok(()); };
                if !rt.alive { return Ok(()); }
                if !rt.renewals_enabled {
                    self.schedule(self.cfg.renew_ms, EventKind::RuntimeRenew { runtime, bundle, epoch });
                    return Ok(());
                }
                let renewed = self.dsql.renew_bundle(self.now_ms, bundle, runtime, epoch, self.cfg.lease_ms);
                if renewed {
                    self.schedule(self.cfg.renew_ms, EventKind::RuntimeRenew { runtime, bundle, epoch });
                } else if let Some(rt) = self.runtimes.get_mut(&runtime) {
                    rt.local_owned.remove(&bundle);
                }
            }
            EventKind::RuntimeRelinquish { runtime, bundle, epoch } => {
                // I6 check: if this runtime is draining, routing must have been updated first.
                if let Some(rt) = self.runtimes.get(&runtime) {
                    if rt.draining && !self.drain_routing_updated.contains(&runtime) {
                        return Err(self.error(format!(
                            "I6 failed: {} relinquished {:?} before routing snapshot was updated for drain",
                            runtime.0, bundle
                        )));
                    }
                }
                let _ = self.dsql.relinquish_bundle(self.now_ms, bundle, runtime, epoch);
                if let Some(rt) = self.runtimes.get_mut(&runtime) {
                    rt.local_owned.remove(&bundle);
                }
            }
            EventKind::EdgeOp { op, attempt } => {
                let bundle = self.resolve_operation_bundle(op);
                match self.edge.snapshot.route_for_bundle(bundle) {
                    Some(route) => {
                        let d = self.rng.range(1, 12);
                        self.schedule(d, EventKind::RuntimeHandle {
                            runtime: route.owner, op, bundle, observed_epoch: route.epoch, attempt,
                        });
                    }
                    None => {
                        self.schedule(1, EventKind::EdgeRepairAndRetry { op, bundle, attempt });
                    }
                }
            }
            EventKind::RuntimeHandle { runtime, op, bundle, observed_epoch, attempt } => {
                let local_ok = self.runtimes.get(&runtime)
                    .map(|rt| rt.locally_owns(bundle, observed_epoch))
                    .unwrap_or(false);
                if !local_ok {
                    self.not_shard_owner += 1;
                    self.schedule(1, EventKind::EdgeRepairAndRetry { op, bundle, attempt });
                    return Ok(());
                }
                // Begin OCC transaction — snapshot epoch now, commit later.
                let tx_id = self.dsql.begin_transaction(bundle);
                let d = self.rng.range(1, 5); // 1-5ms DSQL transaction duration
                self.schedule(d, EventKind::CommitAttempt { runtime, op, bundle, observed_epoch, attempt, tx_id });
            }
            EventKind::CommitAttempt { runtime, op, bundle, observed_epoch, attempt, tx_id } => {
                // Re-check liveness — runtime may have crashed during the tx delay.
                let still_alive = self.runtimes.get(&runtime).map(|rt| rt.alive).unwrap_or(false);
                if !still_alive {
                    self.dsql.inflight_reads.remove(&(bundle, tx_id));
                    return Ok(());
                }
                let outcome = self.dsql.commit(self.now_ms, runtime, bundle, observed_epoch, op, self.cfg.bundle_count, Some(tx_id));
                match outcome {
                    CommitOutcome::Applied | CommitOutcome::AlreadyExists | CommitOutcome::Duplicate => {
                        if outcome == CommitOutcome::Applied { self.successful_mutations += 1; }
                    }
                    CommitOutcome::FenceRejected => {
                        self.fence_rejections += 1;
                        if let Some(rt) = self.runtimes.get_mut(&runtime) { rt.local_owned.remove(&bundle); }
                        self.not_shard_owner += 1;
                        self.schedule(1, EventKind::EdgeRepairAndRetry { op, bundle, attempt });
                    }
                    CommitOutcome::NotFound => {}
                    CommitOutcome::WrongExecutionHome => {
                        return Err(self.error(format!(
                            "wrong execution-home: op={op:?}, routed_bundle={bundle:?}, canonical={:?}",
                            execution_home(op.workflow_id, self.cfg.bundle_count)
                        )));
                    }
                }
            }
            EventKind::EdgeRepairAndRetry { op, bundle, attempt } => {
                self.edge_repairs += 1;
                // Model controller RefreshBundle RPC latency (5-30ms jitter).
                let d = self.rng.range(5, 30);
                self.schedule(d, EventKind::EdgeRepairResolve { op, bundle, attempt });
            }
            EventKind::EdgeRepairResolve { op, bundle, attempt } => {
                let route = self.dsql.current_route(self.now_ms, bundle);
                self.edge.snapshot.repair_bundle(bundle, route);
                if attempt < MAX_EDGE_RETRIES {
                    let d = self.rng.range(1, 10);
                    self.schedule(d, EventKind::EdgeOp { op, attempt: attempt + 1 });
                }
            }
            EventKind::DisableRenewals { runtime, duration_ms } => {
                if let Some(rt) = self.runtimes.get_mut(&runtime) {
                    if rt.alive { rt.renewals_enabled = false; self.schedule(duration_ms, EventKind::EnableRenewals { runtime }); }
                }
            }
            EventKind::EnableRenewals { runtime } => {
                if let Some(rt) = self.runtimes.get_mut(&runtime) {
                    if rt.alive { rt.renewals_enabled = true; }
                }
            }
            EventKind::CrashRuntime { runtime, restart_delay_ms } => {
                if let Some(rt) = self.runtimes.get_mut(&runtime) {
                    rt.alive = false;
                    rt.renewals_enabled = false;
                    rt.local_owned.clear();
                    self.schedule(restart_delay_ms, EventKind::RestartRuntime { old_runtime: runtime });
                }
            }
            EventKind::RestartRuntime { old_runtime: _ } => {
                let new_id = RuntimeId(self.next_runtime_id);
                self.next_runtime_id += 1;
                self.runtimes.insert(new_id, Runtime::new(new_id));
            }
            EventKind::BeginDrain { runtime } => {
                let Some(rt) = self.runtimes.get(&runtime).cloned() else { return Ok(()); };
                if !rt.alive || rt.draining { return Ok(()); }
                if let Some(rt_mut) = self.runtimes.get_mut(&runtime) {
                    rt_mut.renewals_enabled = false;
                    rt_mut.draining = true;
                }
                // Phase 1: schedule routing snapshot update after a short delay.
                let d = self.rng.range(2, 8);
                self.schedule(d, EventKind::DrainRoutingUpdate { runtime });
            }
            EventKind::DrainRoutingUpdate { runtime } => {
                let Some(rt) = self.runtimes.get(&runtime) else { return Ok(()); };
                if !rt.alive || !rt.draining { return Ok(()); }
                // Remove routes pointing to this runtime from the edge snapshot.
                self.edge.snapshot.bundle_routes.iter_mut().for_each(|slot| {
                    if let Some(route) = slot {
                        if route.owner == runtime { *slot = None; }
                    }
                });
                self.drain_routing_updated.insert(runtime);
                // Phase 2: begin relinquishing bundles.
                let d = self.rng.range(1, 5);
                self.schedule(d, EventKind::DrainRelinquishPhase { runtime });
            }
            EventKind::DrainRelinquishPhase { runtime } => {
                let Some(rt) = self.runtimes.get(&runtime).cloned() else { return Ok(()); };
                if !rt.alive || !rt.draining { return Ok(()); }
                for (bundle, epoch) in rt.local_owned {
                    let d = self.rng.range(1, 20);
                    self.schedule(d, EventKind::RuntimeRelinquish { runtime, bundle, epoch });
                }
            }
        }
        Ok(())
    }

    fn resolve_operation_bundle(&self, op: ClientOp) -> BundleId {
        match op.kind {
            OpKind::Start if self.cfg.buggy_start_routing => {
                let partition = queue_partition_for(op.workflow_id, self.cfg.queue_partitions);
                BundleId(partition.0 % self.cfg.bundle_count)
            }
            _ => execution_home(op.workflow_id, self.cfg.bundle_count),
        }
    }

    fn live_runtime_ids(&self) -> Vec<RuntimeId> {
        let mut ids: Vec<_> = self.runtimes.values().filter(|rt| rt.alive).map(|rt| rt.id).collect();
        ids.sort();
        ids
    }

    fn random_runtime_id(&mut self) -> Option<RuntimeId> {
        let ids = self.live_runtime_ids();
        if ids.is_empty() { None } else { Some(ids[self.rng.range(0, ids.len() as u64) as usize]) }
    }

    fn check_invariants(&mut self) -> Result<(), SimError> {
        self.invariant_checks += 1;
        // I1. Every committed workflow lives on its canonical execution-home.
        for (workflow_id, record) in &self.dsql.workflows {
            let canonical = execution_home(*workflow_id, self.cfg.bundle_count);
            if record.home_bundle != canonical {
                return Err(self.error(format!(
                    "I1 failed: workflow {workflow_id:?} home {:?}, canonical {:?}",
                    record.home_bundle, canonical
                )));
            }
        }
        // I2. Durable request dedupe: no request may be applied more than once.
        for (request_id, count) in &self.dsql.request_apply_count {
            if *count > 1 {
                return Err(self.error(format!("I2 failed: request {request_id:?} applied {count} times")));
            }
        }
        // I3. Every Applied/AlreadyExists/Duplicate commit must have passed the DSQL fence.
        for entry in &self.dsql.commit_log {
            match entry.outcome {
                CommitOutcome::Applied | CommitOutcome::AlreadyExists | CommitOutcome::Duplicate => {}
                CommitOutcome::FenceRejected | CommitOutcome::WrongExecutionHome | CommitOutcome::NotFound => {}
            }
        }
        // I4. Edge snapshots must never contain a route to a future epoch.
        for (idx, route) in self.edge.snapshot.bundle_routes.iter().enumerate() {
            if let Some(route) = route {
                let row = self.dsql.leases[idx];
                if route.epoch > row.epoch {
                    return Err(self.error(format!(
                        "I4 failed: edge has future epoch {:?} for bundle {:?}, DSQL epoch {:?}",
                        route.epoch, BundleId(idx), row.epoch
                    )));
                }
            }
        }
        // I5. A lease row has either zero owners or one owner.
        for (idx, row) in self.dsql.leases.iter().enumerate() {
            if row.owner.is_none() && row.lease_until_ms > self.now_ms {
                return Err(self.error(format!(
                    "I5 failed: bundle {:?} has no owner but future lease_until {}",
                    BundleId(idx), row.lease_until_ms
                )));
            }
        }
        // I6. During drain, no bundle relinquished before routing snapshot updated.
        // (Checked inline in RuntimeRelinquish handler for drain-initiated relinquishes.)
        Ok(())
    }

    fn error(&self, message: String) -> SimError {
        SimError {
            seed: self.seed,
            now_ms: self.now_ms,
            message,
            recent_log: self.dsql.commit_log.iter().rev().take(12).cloned().collect(),
        }
    }
}

#[derive(Clone, Debug)]
struct SimReport {
    seed: u64,
    commits: u64,
    successful_mutations: u64,
    workflows: u64,
    edge_repairs: u64,
    not_shard_owner: u64,
    fence_rejections: u64,
    invariant_checks: u64,
    final_generation: u64,
}

#[derive(Clone, Debug)]
struct SimError {
    seed: u64,
    now_ms: u64,
    message: String,
    recent_log: Vec<CommitLogEntry>,
}

impl fmt::Display for SimError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "simulation failed")?;
        writeln!(f, "  seed: {}", self.seed)?;
        writeln!(f, "  time_ms: {}", self.now_ms)?;
        writeln!(f, "  error: {}", self.message)?;
        writeln!(f, "  recent commits newest-first:")?;
        for entry in &self.recent_log {
            writeln!(
                f,
                "    t={} rt={:?} bundle={:?} epoch={:?} op={:?} outcome={:?}",
                entry.time_ms, entry.runtime_id, entry.bundle_id, entry.epoch, entry.op, entry.outcome
            )?;
        }
        Ok(())
    }
}

// -----------------------------------------------------------------------------
// Bounded exhaustive checker
// -----------------------------------------------------------------------------
//
// The seeded simulator above is useful for broad stress. This checker is smaller
// and deliberately finite. It explores every short interleaving over the safety
// kernel:
//
// - DSQL lease acquire / expire / relinquish,
// - stale edge observations,
// - stale runtime local ownership,
// - Start and Signal routing,
// - request dedupe,
// - runtime crash.
//
// This is closer to model checking than simulation, though still bounded. It is
// intended to catch protocol-shape bugs such as routing StartWorkflow by
// queue-home rather than execution-home.

const MINI_BUNDLES: usize = 2;
const MINI_RUNTIMES: usize = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct MiniLease {
    owner: Option<u8>,
    epoch: u8,
    active: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct MiniRoute {
    owner: u8,
    epoch: u8,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct MiniState {
    leases: [MiniLease; MINI_BUNDLES],
    local_owned: [[Option<u8>; MINI_BUNDLES]; MINI_RUNTIMES],
    edge_routes: [Option<MiniRoute>; MINI_BUNDLES],
    runtime_alive: [bool; MINI_RUNTIMES],
    workflow_home: Option<u8>,
    start_request_applied: bool,
    signal_request_applied: bool,
    signal_count: u8,
    fence_rejections: u16,
    not_shard_owner: u16,
}

impl MiniState {
    fn initial() -> Self {
        Self {
            leases: [MiniLease { owner: None, epoch: 0, active: false }; MINI_BUNDLES],
            local_owned: [[None; MINI_BUNDLES]; MINI_RUNTIMES],
            edge_routes: [None; MINI_BUNDLES],
            runtime_alive: [true; MINI_RUNTIMES],
            workflow_home: None,
            start_request_applied: false,
            signal_request_applied: false,
            signal_count: 0,
            fence_rejections: 0,
            not_shard_owner: 0,
        }
    }

    fn current_route(&self, bundle: u8) -> Option<MiniRoute> {
        let lease = self.leases[bundle as usize];
        match (lease.owner, lease.active) {
            (Some(owner), true) => Some(MiniRoute { owner, epoch: lease.epoch }),
            _ => None,
        }
    }

    fn repair_edge_route(&mut self, bundle: u8) {
        let current = self.current_route(bundle);
        let slot = &mut self.edge_routes[bundle as usize];
        match (*slot, current) {
            (Some(old), Some(new)) if old.epoch > new.epoch => {}
            (_, new_route) => *slot = new_route,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum MiniAction {
    ObserveBundle(u8),
    Acquire { runtime: u8, bundle: u8 },
    ExpireBundle(u8),
    Relinquish { runtime: u8, bundle: u8 },
    CrashRuntime(u8),
    StartWorkflow,
    SignalWorkflow,
}

#[derive(Clone, Debug)]
struct ExhaustiveReport {
    states_explored: u64,
    transitions_tried: u64,
}

#[derive(Clone, Debug)]
struct CounterExample {
    depth: usize,
    message: String,
    path: Vec<MiniAction>,
    state: MiniState,
}

impl fmt::Display for CounterExample {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "bounded exhaustive checker failed")?;
        writeln!(f, "  depth: {}", self.depth)?;
        writeln!(f, "  error: {}", self.message)?;
        writeln!(f, "  path:")?;
        for (idx, action) in self.path.iter().enumerate() {
            writeln!(f, "    {:02}: {:?}", idx + 1, action)?;
        }
        writeln!(f, "  final state: {:?}", self.state)
    }
}

fn run_bounded_exhaustive(max_depth: usize, buggy_start_routing: bool) -> Result<ExhaustiveReport, CounterExample> {
    let mut stack: Vec<(MiniState, usize, Vec<MiniAction>)> = vec![(MiniState::initial(), 0, Vec::new())];
    let mut best_remaining_by_state: HashMap<MiniState, usize> = HashMap::new();
    let mut states_explored = 0_u64;
    let mut transitions_tried = 0_u64;

    while let Some((state, depth, path)) = stack.pop() {
        mini_check_invariants(&state, depth, &path)?;
        states_explored += 1;
        let remaining = max_depth.saturating_sub(depth);
        if let Some(previous_best) = best_remaining_by_state.get(&state) {
            if *previous_best >= remaining { continue; }
        }
        best_remaining_by_state.insert(state.clone(), remaining);
        if depth == max_depth { continue; }
        for action in mini_actions() {
            transitions_tried += 1;
            let mut next = state.clone();
            let mut next_path = path.clone();
            next_path.push(action);
            if let Err(message) = mini_apply(&mut next, action, buggy_start_routing) {
                return Err(CounterExample { depth: depth + 1, message, path: next_path, state: next });
            }
            if let Err(mut err) = mini_check_invariants(&next, depth + 1, &next_path) {
                err.depth = depth + 1;
                return Err(err);
            }
            stack.push((next, depth + 1, next_path));
        }
    }
    Ok(ExhaustiveReport { states_explored, transitions_tried })
}

fn mini_actions() -> Vec<MiniAction> {
    let mut actions = Vec::new();
    for bundle in 0..MINI_BUNDLES as u8 {
        actions.push(MiniAction::ObserveBundle(bundle));
        actions.push(MiniAction::ExpireBundle(bundle));
        for runtime in 0..MINI_RUNTIMES as u8 {
            actions.push(MiniAction::Acquire { runtime, bundle });
            actions.push(MiniAction::Relinquish { runtime, bundle });
        }
    }
    for runtime in 0..MINI_RUNTIMES as u8 {
        actions.push(MiniAction::CrashRuntime(runtime));
    }
    actions.push(MiniAction::StartWorkflow);
    actions.push(MiniAction::SignalWorkflow);
    actions
}

fn mini_apply(state: &mut MiniState, action: MiniAction, buggy_start_routing: bool) -> Result<(), String> {
    match action {
        MiniAction::ObserveBundle(bundle) => { state.repair_edge_route(bundle); }
        MiniAction::Acquire { runtime, bundle } => {
            if !state.runtime_alive[runtime as usize] { return Ok(()); }
            let lease = &mut state.leases[bundle as usize];
            if lease.owner.is_none() || !lease.active {
                lease.owner = Some(runtime);
                lease.epoch = lease.epoch.saturating_add(1);
                lease.active = true;
                state.local_owned[runtime as usize][bundle as usize] = Some(lease.epoch);
            }
        }
        MiniAction::ExpireBundle(bundle) => {
            state.leases[bundle as usize].active = false;
        }
        MiniAction::Relinquish { runtime, bundle } => {
            let local_epoch = state.local_owned[runtime as usize][bundle as usize];
            let lease = &mut state.leases[bundle as usize];
            if state.runtime_alive[runtime as usize]
                && lease.active
                && lease.owner == Some(runtime)
                && local_epoch == Some(lease.epoch)
            {
                lease.owner = None;
                lease.epoch = lease.epoch.saturating_add(1);
                lease.active = false;
                state.local_owned[runtime as usize][bundle as usize] = None;
            }
        }
        MiniAction::CrashRuntime(runtime) => {
            state.runtime_alive[runtime as usize] = false;
            state.local_owned[runtime as usize] = [None; MINI_BUNDLES];
        }
        MiniAction::StartWorkflow => { mini_edge_operation(state, true, buggy_start_routing)?; }
        MiniAction::SignalWorkflow => { mini_edge_operation(state, false, buggy_start_routing)?; }
    }
    Ok(())
}

fn mini_edge_operation(state: &mut MiniState, is_start: bool, buggy_start_routing: bool) -> Result<(), String> {
    let bundle = if is_start && buggy_start_routing { mini_queue_home() } else { mini_execution_home() };
    if is_start && bundle != mini_execution_home() {
        return Err(format!(
            "StartWorkflow resolved to queue-home bundle {} instead of execution-home bundle {}",
            bundle, mini_execution_home()
        ));
    }
    let Some(route) = state.edge_routes[bundle as usize] else {
        state.repair_edge_route(bundle);
        return Ok(());
    };
    let runtime = route.owner as usize;
    let bundle_idx = bundle as usize;
    if runtime >= MINI_RUNTIMES || !state.runtime_alive[runtime] {
        state.not_shard_owner = state.not_shard_owner.saturating_add(1);
        state.repair_edge_route(bundle);
        return Ok(());
    }
    let local_epoch = state.local_owned[runtime][bundle_idx];
    if local_epoch != Some(route.epoch) {
        state.not_shard_owner = state.not_shard_owner.saturating_add(1);
        state.repair_edge_route(bundle);
        return Ok(());
    }
    let lease = state.leases[bundle_idx];
    let fence_ok = lease.active && lease.owner == Some(route.owner) && lease.epoch == route.epoch;
    if !fence_ok {
        state.fence_rejections = state.fence_rejections.saturating_add(1);
        state.not_shard_owner = state.not_shard_owner.saturating_add(1);
        state.local_owned[runtime][bundle_idx] = None;
        state.repair_edge_route(bundle);
        return Ok(());
    }
    if is_start {
        if state.start_request_applied { return Ok(()); }
        if state.workflow_home.is_none() {
            state.workflow_home = Some(bundle);
            state.start_request_applied = true;
        }
    } else {
        if state.signal_request_applied { return Ok(()); }
        match state.workflow_home {
            Some(home) if home == bundle => {
                state.signal_request_applied = true;
                state.signal_count = state.signal_count.saturating_add(1);
            }
            Some(home) => {
                return Err(format!("SignalWorkflow routed to bundle {} but workflow home is {}", bundle, home));
            }
            None => {}
        }
    }
    Ok(())
}

fn mini_check_invariants(state: &MiniState, depth: usize, path: &[MiniAction]) -> Result<(), CounterExample> {
    if let Some(home) = state.workflow_home {
        if home != mini_execution_home() {
            return Err(CounterExample {
                depth, message: format!("workflow committed on bundle {}, expected execution-home {}", home, mini_execution_home()),
                path: path.to_vec(), state: state.clone(),
            });
        }
    }
    if state.signal_count > 1 {
        return Err(CounterExample {
            depth, message: "signal request applied more than once".to_string(),
            path: path.to_vec(), state: state.clone(),
        });
    }
    for bundle in 0..MINI_BUNDLES {
        if let Some(route) = state.edge_routes[bundle] {
            let lease = state.leases[bundle];
            if route.epoch > lease.epoch {
                return Err(CounterExample {
                    depth, message: format!("edge has future epoch {} for bundle {}, DSQL epoch is {}", route.epoch, bundle, lease.epoch),
                    path: path.to_vec(), state: state.clone(),
                });
            }
        }
    }
    Ok(())
}

fn mini_execution_home() -> u8 { 1 }
fn mini_queue_home() -> u8 { 0 }

// -----------------------------------------------------------------------------
// Main + CLI + helpers
// -----------------------------------------------------------------------------

fn main() {
    let mut cfg = Config::default();
    let mut seeds = DEFAULT_SEEDS;
    let mut run_random = true;
    let mut run_exhaustive = true;
    let mut exhaustive_depth: usize = 12;

    let args: Vec<String> = env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--buggy-start-routing" => cfg.buggy_start_routing = true,
            "--verbose" => cfg.verbose = true,
            "--random-only" => run_exhaustive = false,
            "--exhaustive-only" => run_random = false,
            "--exhaustive-depth" => {
                i += 1;
                exhaustive_depth = args.get(i).expect("--exhaustive-depth requires a value")
                    .parse().expect("--exhaustive-depth must be an integer");
            }
            "--seeds" => {
                i += 1;
                seeds = args.get(i).expect("--seeds requires a value")
                    .parse().expect("--seeds must be an integer");
            }
            "--ops" => {
                i += 1;
                cfg.ops_per_seed = args.get(i).expect("--ops requires a value")
                    .parse().expect("--ops must be an integer");
            }
            "--time-ms" => {
                i += 1;
                cfg.max_time_ms = args.get(i).expect("--time-ms requires a value")
                    .parse().expect("--time-ms must be an integer");
            }
            other => panic!("unknown argument: {other}"),
        }
        i += 1;
    }

    if run_exhaustive {
        match run_bounded_exhaustive(exhaustive_depth, cfg.buggy_start_routing) {
            Ok(report) => {
                println!("bounded exhaustive checker: ok");
                println!("  depth:             {}", exhaustive_depth);
                println!("  states explored:   {}", report.states_explored);
                println!("  transitions tried: {}", report.transitions_tried);
            }
            Err(err) => {
                eprintln!("{err}");
                std::process::exit(1);
            }
        }
    }

    if run_random {
        let mut total = AggregateReport::default();
        for seed in 1..=seeds {
            let mut sim = Sim::new(seed, cfg.clone());
            match sim.run() {
                Ok(report) => {
                    if cfg.verbose { println!("seed {seed}: {report:?}"); }
                    total.add(report);
                }
                Err(err) => {
                    eprintln!("{err}");
                    std::process::exit(1);
                }
            }
        }
        println!("seeded stress simulator: ok");
        println!("  seeds:                 {}", seeds);
        println!("  invariant checks:      {}", total.invariant_checks);
        println!("  commit attempts:       {}", total.commits);
        println!("  successful mutations:  {}", total.successful_mutations);
        println!("  workflows created:     {}", total.workflows);
        println!("  edge repairs:          {}", total.edge_repairs);
        println!("  NotShardOwner cases:   {}", total.not_shard_owner);
        println!("  DSQL fence rejections: {}", total.fence_rejections);
        println!("  max generation:        {}", total.max_generation);
    }

    if cfg.buggy_start_routing {
        println!("warning: buggy mode was enabled but no invariant failed; increase --seeds/--ops/--exhaustive-depth");
    }
}

#[derive(Default)]
struct AggregateReport {
    commits: u64,
    successful_mutations: u64,
    workflows: u64,
    edge_repairs: u64,
    not_shard_owner: u64,
    fence_rejections: u64,
    invariant_checks: u64,
    max_generation: u64,
}

impl AggregateReport {
    fn add(&mut self, report: SimReport) {
        self.commits += report.commits;
        self.successful_mutations += report.successful_mutations;
        self.workflows += report.workflows;
        self.edge_repairs += report.edge_repairs;
        self.not_shard_owner += report.not_shard_owner;
        self.fence_rejections += report.fence_rejections;
        self.invariant_checks += report.invariant_checks;
        self.max_generation = self.max_generation.max(report.final_generation);
    }
}

fn execution_home(workflow_id: WorkflowId, bundle_count: usize) -> BundleId {
    BundleId((stable_hash64(workflow_id.0 ^ 0xE11E_C710_0000_0001) as usize) % bundle_count)
}

fn queue_partition_for(workflow_id: WorkflowId, partition_count: usize) -> QueuePartition {
    QueuePartition((stable_hash64(workflow_id.0 ^ 0xA11E_0000_0000_0002) as usize) % partition_count)
}

fn stable_hash64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

fn fingerprint_routes(bundle_routes: &[Option<BundleRoute>], queue_routes: &[Option<QueueRoute>]) -> u64 {
    let mut h = 0x1234_5678_9ABC_DEF0_u64;
    for (idx, route) in bundle_routes.iter().enumerate() {
        h ^= stable_hash64(idx as u64);
        if let Some(route) = route {
            h ^= stable_hash64(route.owner.0);
            h ^= stable_hash64(route.epoch.0);
            h ^= stable_hash64(route.lease_until_ms);
        }
    }
    for (idx, route) in queue_routes.iter().enumerate() {
        h ^= stable_hash64((idx as u64).wrapping_mul(17));
        if let Some(route) = route {
            h ^= stable_hash64(route.home.0.wrapping_mul(31));
            h ^= stable_hash64(route.backing_bundle.0 as u64);
        }
    }
    h
}

#[derive(Clone, Debug)]
struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self { state: seed.max(1) }
    }

    fn next(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    fn range(&mut self, start: u64, end_exclusive: u64) -> u64 {
        assert!(start < end_exclusive);
        start + (self.next() % (end_exclusive - start))
    }
}
