//! Domain types for the placement/membership model.
//!
//! These re-model the shape of Tokeira's placement design (architecture doc
//! 035) without importing any server crate — the same re-modeling choice the
//! broker simulator makes. The load-bearing distinction the whole simulator
//! exists to defend is encoded directly in the type split:
//!
//! - [`Dsql`] is **authority**. A bundle's lease row (owner + epoch + expiry) is
//!   the single source of truth for who may mutate that bundle, and the
//!   workflow table records each run's canonical execution-home. Nothing else
//!   may make workflow state true.
//! - [`Edge`] holds an **advisory** [`RoutingSnapshot`]. It is a cache that can
//!   lag, point at a stale epoch, or name a former owner; it never grants
//!   authority. Every edge-routed operation is revalidated at commit against the
//!   DSQL fence, so a stale edge can only cause a retry, never a wrong write.
//! - [`Runtime`] holds **local** ownership belief (`local_owned`) which is
//!   likewise advisory: a runtime that kept serving on a lapsed lease is fenced
//!   out at commit time.
//!
//! The correctness thesis (035): *DSQL owns truth; runtime ownership is valid
//! only by the current DSQL bundle lease epoch; queue-home is advisory;
//! execution-home is the correctness boundary.*

use std::collections::{hash_map::Entry, HashMap, HashSet};

/// A runtime process. Identity is per-incarnation: a crash/restart mints a fresh
/// [`RuntimeId`], so stale references to a dead incarnation never alias a live one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RuntimeId(pub u64);

/// A controller process. Controllers observe DSQL and publish routing snapshots
/// to the edge; multiple may run active-active, which the model exercises.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ControllerId(pub u64);

/// A bundle: the unit of lease ownership and the correctness boundary for a
/// workflow's execution-home.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BundleId(pub usize);

/// A queue partition: the unit of *advisory* queue-home routing. Deliberately
/// distinct from [`BundleId`] because conflating the two is the canonical bug
/// (`--bug=buggy-start-routing`) the simulator is built to catch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct QueuePartition(pub usize);

/// A lease epoch. Monotonic per bundle row; every acquire/relinquish bumps it,
/// so an epoch uniquely fences one ownership interval. Comparisons of `Epoch`
/// are the heart of the OCC fence.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Epoch(pub u64);

/// A routing-snapshot generation. Bumped only when the published routing
/// fingerprint changes, so the edge can cheaply reject older snapshots.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Generation(pub u64);

/// A workflow identity. Its execution-home bundle is a pure function of this id
/// ([`execution_home`]), which is what makes "did this commit on the right
/// bundle?" a checkable invariant.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WorkflowId(pub u64);

/// A client request identity, used for durable dedupe. Replaying the same
/// `RequestId` must apply at most once (invariant I2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RequestId(pub u64);

/// The two client mutations the model drives. `Start` creates a workflow at its
/// execution-home; `Signal` mutates an existing one and must land on the same
/// home the start did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpKind {
    /// Create a workflow at its canonical execution-home bundle.
    Start,
    /// Mutate an existing workflow; must target its established home.
    Signal,
}

/// One client operation: what kind, against which workflow, under which request
/// id (the dedupe key).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientOp {
    /// Start vs Signal.
    pub kind: OpKind,
    /// Target workflow.
    pub workflow_id: WorkflowId,
    /// Dedupe identity — the same id applied twice must be a no-op the second time.
    pub request_id: RequestId,
}

/// A DSQL bundle-lease row: the authoritative ownership record.
///
/// `owner == None` means unowned; a non-`None` owner is valid only while
/// `lease_until_ms` is in the future. `epoch` fences the ownership interval.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BundleLease {
    /// Current owner, or `None` if unowned.
    pub owner: Option<RuntimeId>,
    /// Fencing epoch for this ownership interval.
    pub epoch: Epoch,
    /// Absolute simulated time the lease expires.
    pub lease_until_ms: u64,
}

/// An advisory route to a bundle's believed owner, as cached at the edge.
///
/// The `epoch` is what the edge *thought* was current when the snapshot was
/// built; the fence at commit compares it against the live DSQL epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BundleRoute {
    /// Believed owner runtime.
    pub owner: RuntimeId,
    /// Believed epoch (may be stale).
    pub epoch: Epoch,
    /// Believed lease expiry (advisory only).
    pub lease_until_ms: u64,
}

/// An advisory queue-home route: which runtime fronts a queue partition and
/// which bundle backs it. Queue-home is *not* the correctness boundary — it is a
/// dispatch hint only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueRoute {
    /// Runtime currently fronting this partition.
    pub home: RuntimeId,
    /// Bundle backing the partition.
    pub backing_bundle: BundleId,
}

/// A full routing snapshot the controller publishes and the edge caches.
///
/// `generation` lets the edge ignore snapshots no newer than what it holds; the
/// per-bundle and per-partition route vectors are the advisory routing tables.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoutingSnapshot {
    /// Monotonic publication generation.
    pub generation: Generation,
    /// Advisory bundle → owner routes (index is the bundle id).
    pub bundle_routes: Vec<Option<BundleRoute>>,
    /// Advisory queue-partition → home routes (index is the partition).
    pub queue_routes: Vec<Option<QueueRoute>>,
}

impl RoutingSnapshot {
    /// An empty snapshot at generation 0 sized for the topology.
    pub fn empty(bundle_count: usize, queue_partitions: usize) -> Self {
        Self {
            generation: Generation(0),
            bundle_routes: vec![None; bundle_count],
            queue_routes: vec![None; queue_partitions],
        }
    }

    /// The advisory route for a bundle, if any.
    pub fn route_for_bundle(&self, bundle: BundleId) -> Option<BundleRoute> {
        self.bundle_routes.get(bundle.0).and_then(|r| *r)
    }

    /// Replace this snapshot with `incoming` only if it is at least as new.
    ///
    /// Older generations are dropped, so out-of-order delivery of controller
    /// snapshots cannot move the edge backwards — the monotone-cache property
    /// invariant I4 relies on.
    pub fn apply_full_snapshot(&mut self, incoming: RoutingSnapshot) {
        if incoming.generation >= self.generation {
            *self = incoming;
        }
    }

    /// Point-repair a single bundle's route after a fence miss, but never
    /// regress to an older epoch than already cached.
    ///
    /// The edge repairs one bundle at a time (a `RefreshBundle`-style lookup)
    /// rather than waiting for a whole new snapshot; the epoch guard keeps the
    /// repair monotone so a slow lookup cannot install a stale route over a
    /// fresher one.
    pub fn repair_bundle(&mut self, bundle: BundleId, route: Option<BundleRoute>) {
        if let Some(slot) = self.bundle_routes.get_mut(bundle.0) {
            match (*slot, route) {
                (Some(old), Some(new)) if old.epoch > new.epoch => {}
                (_, new_route) => *slot = new_route,
            }
        }
    }
}

/// A runtime's local view of which bundles it believes it owns.
///
/// `local_owned` is advisory belief, not authority: a runtime can keep this map
/// populated after its lease lapsed (renewals disabled, network blip), which is
/// exactly the adversarial case the fence must catch. `alive` is per-incarnation.
#[derive(Clone, Debug)]
pub struct Runtime {
    /// This runtime's incarnation id.
    pub id: RuntimeId,
    /// Whether this incarnation is up.
    pub alive: bool,
    /// Whether lease renewals currently fire (faults can suppress them).
    pub renewals_enabled: bool,
    /// Whether a graceful drain is in progress.
    pub draining: bool,
    /// Believed-owned bundles and the epoch each was acquired at.
    pub local_owned: HashMap<BundleId, Epoch>,
}

impl Runtime {
    /// A fresh, alive, renewing runtime owning nothing.
    pub fn new(id: RuntimeId) -> Self {
        Self {
            id,
            alive: true,
            renewals_enabled: true,
            draining: false,
            local_owned: HashMap::new(),
        }
    }

    /// Whether this runtime *believes* it owns `bundle` at `epoch`.
    ///
    /// This is belief, not truth: it gates whether the runtime will *attempt* a
    /// commit, but the DSQL fence is what actually authorises one.
    pub fn locally_owns(&self, bundle: BundleId, epoch: Epoch) -> bool {
        self.alive && self.local_owned.get(&bundle).copied() == Some(epoch)
    }
}

/// The authoritative record of a committed workflow: its home bundle, the
/// request that started it, and how many signals have applied.
#[derive(Clone, Debug)]
pub struct WorkflowRecord {
    /// The bundle the workflow was created on — must equal its execution-home.
    pub home_bundle: BundleId,
    /// The request id that started it (for traceability).
    pub started_by: RequestId,
    /// Applied signal count (used to detect double-apply).
    pub signal_count: u64,
}

/// The outcome of a commit attempt against DSQL. Only the first three represent
/// a commit that the fence admitted; the rest are rejections the runtime must
/// handle (retry, repair, or surface as a bug).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitOutcome {
    /// The mutation applied for the first time.
    Applied,
    /// A duplicate request id — idempotent no-op (I2 protects this).
    Duplicate,
    /// Start of an already-existing workflow — idempotent no-op.
    AlreadyExists,
    /// Signal for an unknown workflow.
    NotFound,
    /// The OCC fence rejected the commit (stale owner/epoch/expiry).
    FenceRejected,
    /// The op was routed to a bundle that is not the workflow's execution-home.
    /// Reaching this with `Applied` semantics would be an I1 violation; the
    /// model surfaces it so the invariant can fire.
    WrongExecutionHome,
}

/// One entry in the DSQL commit log: enough to reconstruct what was attempted,
/// by whom, under which fence, and how it resolved. The recent tail is printed
/// in a failure report.
#[derive(Clone, Debug)]
pub struct CommitLogEntry {
    /// Simulated time of the commit attempt.
    pub time_ms: u64,
    /// Runtime that attempted it.
    pub runtime_id: RuntimeId,
    /// Bundle it targeted.
    pub bundle_id: BundleId,
    /// Epoch the runtime presented.
    pub epoch: Epoch,
    /// The operation.
    pub op: ClientOp,
    /// How DSQL resolved it.
    pub outcome: CommitOutcome,
}

/// The authoritative store: bundle leases, the workflow table, durable dedupe
/// state, the commit log, and the OCC read-set for in-flight transactions.
///
/// Everything correctness-bearing lives here. The edge and runtimes hold only
/// advisory copies; a crash that wipes their state loses no truth, because truth
/// is exactly this struct.
#[derive(Clone, Debug)]
pub struct Dsql {
    /// Per-bundle lease rows (index is the bundle id).
    pub leases: Vec<BundleLease>,
    /// The committed workflow table.
    pub workflows: HashMap<WorkflowId, WorkflowRecord>,
    /// Set of request ids that have applied (dedupe).
    pub applied_requests: HashSet<RequestId>,
    /// How many times each request id applied — must never exceed 1 (I2).
    pub request_apply_count: HashMap<RequestId, u64>,
    /// Append-only commit log for diagnostics.
    pub commit_log: Vec<CommitLogEntry>,
    /// Current routing-snapshot generation.
    pub routing_generation: Generation,
    /// Fingerprint of the last published routing, to detect changes.
    pub last_published_fingerprint: u64,
    /// OCC read-set: the epoch each in-flight commit read at transaction start,
    /// keyed by `(bundle, tx_id)`. Validated against the live epoch at commit —
    /// this models DSQL optimistic concurrency: read at begin, fence at commit.
    pub inflight_reads: HashMap<(BundleId, u64), Epoch>,
    /// Monotonic transaction-id source.
    pub next_tx_id: u64,
}

/// The advisory edge cache: just the latest routing snapshot it has accepted.
#[derive(Clone, Debug)]
pub struct Edge {
    /// The edge's current (possibly stale) routing view.
    pub snapshot: RoutingSnapshot,
}

impl Edge {
    /// An edge with an empty snapshot sized for the topology.
    pub fn new(bundle_count: usize, queue_partitions: usize) -> Self {
        Self {
            snapshot: RoutingSnapshot::empty(bundle_count, queue_partitions),
        }
    }
}

/// A controller process; just an id and liveness for this model's purposes.
#[derive(Clone, Debug)]
pub struct Controller {
    /// Controller id.
    pub id: ControllerId,
    /// Whether it is up and observing.
    pub alive: bool,
}

/// Tunable topology and timing for a placement run.
///
/// Meanings are fixed; the defaults match the original `placement-sim`. The
/// buggy-routing toggle is threaded through here so both verification modes can
/// inject the same defect.
#[derive(Clone, Debug)]
pub struct PlacementCfg {
    /// Number of bundles (lease rows / execution-home space).
    pub bundle_count: usize,
    /// Number of queue partitions (advisory queue-home space).
    pub queue_partitions: usize,
    /// Initial runtime count.
    pub runtime_count: usize,
    /// Controller count (active-active when > 1).
    pub controller_count: usize,
    /// Lease duration granted on acquire/renew.
    pub lease_ms: u64,
    /// Renewal interval.
    pub renew_ms: u64,
    /// Controller observation cadence.
    pub controller_observe_ms: u64,
    /// Simulated time bound for one seed.
    pub max_time_ms: u64,
    /// Operations scheduled per seed.
    pub ops_per_seed: usize,
    /// When true, `Start` routes by queue-home instead of execution-home — the
    /// deliberate bug that must violate the same-home invariant (I1).
    pub buggy_start_routing: bool,
}

impl Default for PlacementCfg {
    fn default() -> Self {
        Self {
            bundle_count: 16,
            queue_partitions: 64,
            runtime_count: 4,
            controller_count: 3,
            lease_ms: 120,
            renew_ms: 40,
            controller_observe_ms: 35,
            max_time_ms: 6_000,
            ops_per_seed: 800,
            buggy_start_routing: false,
        }
    }
}

/// Maximum edge repair/retry attempts for one operation before it is dropped.
/// Bounds the retry storm a persistently-stale route could otherwise cause.
pub const MAX_EDGE_RETRIES: u8 = 6;

/// The canonical execution-home bundle for a workflow — a pure function of its
/// id, so "did this commit on the right bundle?" is decidable. This is *the*
/// correctness boundary: every Start/Signal for a workflow must resolve here.
pub fn execution_home(workflow_id: WorkflowId, bundle_count: usize) -> BundleId {
    BundleId((stable_hash64(workflow_id.0 ^ 0xE11E_C710_0000_0001) as usize) % bundle_count)
}

/// The advisory queue-home partition for a workflow — a *different* hash from
/// [`execution_home`]. Routing a `Start` here instead of the execution-home is
/// the canonical bug; keeping the hashes distinct guarantees the two diverge.
pub fn queue_partition_for(workflow_id: WorkflowId, partition_count: usize) -> QueuePartition {
    QueuePartition(
        (stable_hash64(workflow_id.0 ^ 0xA11E_0000_0000_0002) as usize) % partition_count,
    )
}

/// A fast, stable 64-bit mixer (SplitMix64-style finalizer). Deterministic and
/// seed-free so home assignments are stable across runs.
pub fn stable_hash64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// Fold the routing tables into a single fingerprint, so the controller only
/// bumps the generation when routing actually changed (avoids churning the edge
/// and inflating the generation counter on no-op observations).
pub fn fingerprint_routes(
    bundle_routes: &[Option<BundleRoute>],
    queue_routes: &[Option<QueueRoute>],
) -> u64 {
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

impl Dsql {
    /// A fresh store with `bundle_count` unowned lease rows.
    pub fn new(bundle_count: usize) -> Self {
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

    /// Acquire a bundle lease for `owner` if it is free or expired.
    ///
    /// Acquisition bumps the epoch, which fences any holder of the prior epoch:
    /// the moment a new owner takes over, every in-flight commit that read the
    /// old epoch will fail its fence. Returns the new epoch on success.
    pub fn acquire_bundle(
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

    /// Extend a lease the caller still legitimately holds (same owner, same
    /// epoch, not yet expired). A renewal does **not** bump the epoch — it only
    /// pushes out the expiry — so existing valid commits stay valid.
    pub fn renew_bundle(
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

    /// Voluntarily give up a lease the caller holds, bumping the epoch so the
    /// next acquirer is fenced cleanly against the prior owner.
    pub fn relinquish_bundle(
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

    /// The currently-valid route for a bundle: its owner and epoch iff the lease
    /// has not expired. `None` for unowned or lapsed leases. This is the live
    /// truth a controller snapshot or an edge repair reads.
    pub fn current_route(&self, now_ms: u64, bundle: BundleId) -> Option<BundleRoute> {
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

    /// Build a fresh routing snapshot from live lease state, assigning advisory
    /// queue-homes round-robin over the live runtimes.
    ///
    /// The generation is bumped only when the routing fingerprint changes, so an
    /// observation that sees no change does not churn the edge. Bundle routes
    /// reflect only valid (unexpired) leases — an expired lease publishes as
    /// `None`, never a future epoch (the property I4 checks).
    pub fn build_snapshot(
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

    /// Begin an OCC transaction by snapshotting the bundle's current epoch under
    /// a fresh tx id. The later [`commit`](Dsql::commit) validates this read-time
    /// epoch against the live epoch — modelling read-at-begin / fence-at-commit,
    /// which is what makes a lease change *during* a transaction fatal to it.
    pub fn begin_transaction(&mut self, bundle: BundleId) -> u64 {
        let tx_id = self.next_tx_id;
        self.next_tx_id += 1;
        let epoch = self.leases[bundle.0].epoch;
        self.inflight_reads.insert((bundle, tx_id), epoch);
        tx_id
    }

    /// Discard an in-flight OCC read without committing (e.g. the runtime died
    /// mid-transaction). Keeps the read-set from leaking entries.
    pub fn abort_transaction(&mut self, bundle: BundleId, tx_id: u64) {
        self.inflight_reads.remove(&(bundle, tx_id));
    }

    /// Attempt to commit `op` under the OCC fence.
    ///
    /// The fence admits the commit only if, at commit time, the bundle is still
    /// owned by `runtime_id` at the epoch the transaction read, and the lease
    /// has not expired. A `tx_id` supplies the read-time epoch (true OCC); absent
    /// one, `expected_epoch` is used directly. This single check is what makes a
    /// stale edge route or a lapsed local belief *safe*: it can cause a
    /// rejection and retry, never a wrong write.
    ///
    /// On a passing fence the op is applied with execution-home and dedupe
    /// enforcement; `WrongExecutionHome` is returned (not applied) if the op was
    /// routed off its canonical home — the condition invariant I1 guards.
    #[allow(clippy::too_many_arguments)]
    pub fn commit(
        &mut self,
        now_ms: u64,
        runtime_id: RuntimeId,
        bundle_id: BundleId,
        expected_epoch: Epoch,
        op: ClientOp,
        bundle_count: usize,
        tx_id: Option<u64>,
    ) -> CommitOutcome {
        // OCC: prefer the epoch read at transaction begin over the caller's
        // claim, so a lease change between begin and commit is detected.
        let check_epoch = match tx_id {
            Some(id) => self
                .inflight_reads
                .remove(&(bundle_id, id))
                .unwrap_or(expected_epoch),
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
                    } else {
                        // Idempotent create: a re-run of a start that already
                        // landed is an AlreadyExists no-op; a fresh id inserts
                        // the authoritative record. Either way the request id is
                        // marked applied so a later replay dedupes.
                        let outcome = match self.workflows.entry(op.workflow_id) {
                            Entry::Occupied(_) => CommitOutcome::AlreadyExists,
                            Entry::Vacant(slot) => {
                                slot.insert(WorkflowRecord {
                                    home_bundle: bundle_id,
                                    started_by: op.request_id,
                                    signal_count: 0,
                                });
                                CommitOutcome::Applied
                            }
                        };
                        self.mark_applied(op.request_id);
                        outcome
                    }
                }
                OpKind::Signal => match self.workflows.get_mut(&op.workflow_id) {
                    Some(record) if record.home_bundle == bundle_id => {
                        record.signal_count += 1;
                        self.mark_applied(op.request_id);
                        CommitOutcome::Applied
                    }
                    // The workflow exists but on a different home: routing
                    // resolved to the wrong bundle. Surfaced, never applied.
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

    /// Record a request id as applied and bump its apply count. The count is
    /// what invariant I2 watches: a value above 1 means dedupe failed.
    fn mark_applied(&mut self, request_id: RequestId) {
        self.applied_requests.insert(request_id);
        *self.request_apply_count.entry(request_id).or_insert(0) += 1;
    }
}
