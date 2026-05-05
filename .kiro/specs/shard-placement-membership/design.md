# Design Document: Shard Placement and Membership

## Overview

This design implements the shard placement and membership system for multi-node Tokeira deployments. The system introduces active-active placement controllers that observe DSQL lease state, compute desired placement, and distribute routing snapshots to edges and runtimes. The design preserves the core invariant that DSQL lease fencing is the only authoritative ownership mechanism — the controller is advisory, not authoritative.

The critical correctness distinction is between **execution-home** and **queue-home**:

- **Execution-home** is the correctness boundary. Start, Signal, Query, Update, Cancel all route by execution-home derived from the canonical execution key `hash(namespace_id, workflow_id) -> bundle_id`. This ensures all operations on the same workflow identity reach the same bundle owner.
- **Queue-home** is an optimisation for poll/task locality. Queue partition placement is used only for dispatching workflow tasks and activities after the execution exists. For this spec (MVP), queue-home always coincides with bundle ownership — `partition → bundle_for_partition → bundle owner`. Independent queue-partition placement (finer-grained than bundles) is deferred to 037-dynamic-placement.

The implementation is organized into 5 phases:

1. **Phase 1:** Placement types, deterministic mapping functions, and RoutingSnapshot (`tokeira-types`)
2. **Phase 2:** LeaseRepository extensions and bundle-to-partition mapping (`tokeira-storage`)
3. **Phase 3:** Controller gRPC service, active-active coordination, membership tracking, routing snapshot computation (`tokeira-controller`)
4. **Phase 4:** Runtime membership stream client, placement directive handling, and two-phase drain protocol (`tokeira-runtime`)
5. **Phase 5:** Edge routing cache with ArcSwap, NotShardOwner recovery with redirect hints, execution-home/queue-home resolution (`tokeira-edge`)

## Architecture

```
┌─────────────┐     SubscribeRouting      ┌──────────────────┐     RuntimeMembership     ┌──────────────┐
│  Edge Node   │◄────(server-stream)──────│    Placement      │◄────(bidi-stream)────────│ Runtime Node  │
│              │                           │    Controller     │                           │              │
│ ArcSwap<     │                           │  (active-active)  │                           │ ShardOwner   │
│  RoutingSnap>│                           │                   │                           │ BundleLeases │
│ ExecHome     │     RefreshBundle         │ LiveMembership    │  DesiredPlacement         │ DrainState   │
│ QueueHome    │────(unary)───────────────▶│ RoutingSnapshot   │──Directive───────────────▶│ Heartbeat    │
│ NotShardOwner│                           │ DesiredPlacement  │                           │              │
│   recovery   │                           │ CAS generation    │                           │              │
└──────┬───────┘                           └────────┬─────────┘                           └──────┬───────┘
       │                                            │                                            │
       │         request (bundle owner + epoch)     │        lease acquire/renew/relinquish       │
       └────────────────────────────────────────────┼────────────────────────────────────────────┘
                                                    │
                                              ┌─────┴─────┐
                                              │   DSQL     │
                                              │            │
                                              │ bundle_lease│
                                              │ routing_gen │
                                              │ budget_alloc│
                                              │ workflow_hot│
                                              └────────────┘
```

### Safe Convergence Loop

```
desired placement ──directive──▶ runtime
runtime ──CAS/lease──▶ DSQL
controller ──list leases──▶ actual ownership
controller ──snapshot──▶ edge
edge ──route──▶ actual owner
```

### Crate Dependency Graph

| Crate | New Dependencies | Role |
|---|---|---|
| `tokeira-types` | `blake3` | `IncarnationId`, `BundleId`, `QueuePartition`, `QueuePartitionKey`, `GenerationCounter`, `NodeReachability`, `PlacementConfig`, `BundleOwner`, `RoutingSnapshot`, mapping functions |
| `tokeira-storage` | *(none)* | `LeaseRepository` extensions: `list_bundle_leases`, `relinquish_bundle` |
| `tokeira-controller` | `tonic`, `tokio`, `tracing`, `tokeira-types`, `tokeira-storage`, `tokeira-proto` | New crate: active-active controller service, membership, routing, desired placement |
| `tokeira-runtime` | `tokeira-proto` (for controller client) | Membership stream client with registration, placement directive handling, two-phase drain protocol |
| `tokeira-edge` | `tokeira-types` (for `RoutingSnapshot`), `arc-swap` | Routing cache with `ArcSwap`, NotShardOwner recovery with redirect hints, execution-home resolution for all operations |
| `tokeira-proto` | *(none)* | New proto definitions for controller service with `oneof` message types |


### Request Flow: Start Workflow (Execution-Home Routing)

```
1. Edge receives StartWorkflowExecution with (namespace_id, workflow_id)
2. Edge derives execution_key = hash(namespace_id, workflow_id)
3. Edge computes bundle_id from execution_key (execution-home)
4. Edge looks up bundle_id → BundleOwner { node_id, epoch } in RoutingSnapshot.execution_bundle_owners
5. Edge looks up node_id → endpoint in RoutingSnapshot.node_endpoints
6. Edge forwards request to runtime node with observed_bundle_epoch
7. Runtime validates bundle ownership (epoch check — fast-fail if mismatch before DSQL transaction)
8. Runtime atomically creates current_execution, workflow_hot, history_batch, request_dedupe
9. Queue partition is used ONLY for dispatching workflow tasks/activities AFTER the execution exists
```

### Request Flow: Signal Workflow (Execution-Home Routing)

```
1. Edge receives SignalWorkflowExecution with (namespace_id, workflow_id)
2. Edge derives execution_key = hash(namespace_id, workflow_id) — SAME key as Start
3. Edge computes bundle_id from execution_key (execution-home)
4. Edge looks up bundle_id → BundleOwner { node_id, epoch } in RoutingSnapshot.execution_bundle_owners
5. Edge looks up node_id → endpoint in RoutingSnapshot.node_endpoints
6. Edge forwards request to runtime node with observed_bundle_epoch
7. If runtime returns NotShardOwner { current_owner_node_id, current_epoch }:
   a. Try hint: route to current_owner_node_id if present
   b. Try controller: RefreshBundle(bundle_id) unary call
   c. Fallback: DSQL lease lookup
   d. Retry (up to 3 times)
```

Note: Start and Signal for the same `(namespace_id, workflow_id)` resolve to the SAME execution-home bundle. This is a correctness requirement.

## Components and Interfaces

### 1. Placement Types (`tokeira-types`)

New types added to `tokeira-types/src/ids.rs`:

```rust
/// Unique identifier for a runtime node incarnation.
/// Assigned at startup, stable only for the process lifetime.
/// This is NOT a durable node identity — it changes on restart.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IncarnationId(pub Uuid);

impl IncarnationId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

/// Identifies a queue partition — the main unit of dynamic placement.
/// Finer-grained than bundles. Always scoped to a queue family via QueuePartitionKey.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QueuePartition(pub u32);

/// Full key for a queue partition, scoping it to a queue family.
/// Different queue families can hash to the same partition number,
/// so the bare QueuePartition(u32) is not a unique map key.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QueuePartitionKey {
    pub namespace_id: NamespaceId,
    pub task_queue: TaskQueueName,
    pub task_kind: TaskKind,
    pub partition: QueuePartition,
}

/// Monotonically increasing counter on routing snapshots.
/// Persisted in DSQL with CAS protection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct GenerationCounter(pub u64);

impl GenerationCounter {
    pub const ZERO: Self = Self(0);

    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

/// Health assessment of a node, separate from ownership state.
/// Routing continues while a node is Suspect.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NodeReachability {
    Healthy,
    Suspect,
    Unavailable,
}
```

`BundleId` is an alias for the existing `ShardId`:

```rust
/// A lease bundle — the authoritative ownership unit.
/// Alias for ShardId since bundles and shards are the same concept.
pub type BundleId = ShardId;
```

### 2. Deterministic Mapping Functions (`tokeira-types`)

```rust
/// Derive a queue partition from a placement key.
///
/// The placement key is hashed to produce a uniform distribution
/// across the partition space. Uses blake3 (new dependency for tokeira-types).
pub fn queue_partition_for(placement_key: &[u8], partition_count: u32) -> QueuePartition {
    assert!(partition_count > 0, "partition_count must be > 0");
    let hash = blake3::hash(placement_key);
    let value = u32::from_le_bytes(hash.as_bytes()[..4].try_into().unwrap());
    QueuePartition(value % partition_count)
}

/// Map a queue partition to the bundle that owns it.
///
/// This is a simple modular mapping. With 1024 partitions and 64 bundles,
/// each bundle owns ~16 partitions.
pub fn bundle_for_partition(partition: QueuePartition, bundle_count: u32) -> BundleId {
    assert!(bundle_count > 0, "bundle_count must be > 0");
    ShardId(partition.0 % bundle_count)
}

/// Derive the execution-home bundle from the canonical execution key.
///
/// This is the ONLY correct way to route Start, Signal, Query, Update, Cancel.
/// The execution key is (namespace_id, workflow_id).
pub fn execution_home_bundle(namespace_id: &[u8], workflow_id: &[u8], bundle_count: u32) -> BundleId {
    assert!(bundle_count > 0, "bundle_count must be > 0");
    let mut hasher = blake3::Hasher::new();
    hasher.update(namespace_id);
    hasher.update(workflow_id);
    let hash = hasher.finalize();
    let value = u32::from_le_bytes(hash.as_bytes()[..4].try_into().unwrap());
    ShardId(value % bundle_count)
}
```

The existing `shard_for(run_key, shard_count)` function maps run keys to shards/bundles. For execution-home resolution, the edge uses `execution_home_bundle` which hashes the canonical `(namespace_id, workflow_id)` pair.

### 3. Routing Snapshot (`tokeira-types`)

```rust
/// Network endpoint for a runtime node.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeEndpoint {
    pub host: String,
    pub port: u16,
}

/// Owner of a bundle with epoch for fencing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BundleOwner {
    pub node_id: IncarnationId,
    pub epoch: ShardEpoch,
}

/// Home assignment for a queue partition — no longer stored in the snapshot.
/// Queue-home is derived on demand: partition → bundle_for_partition → bundle owner.
/// Retained as a convenience type for callers that need both node_id and bundle_id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QueuePartitionHome {
    pub node_id: IncarnationId,
    pub bundle_id: BundleId,
}

/// Hash and mapping version configuration embedded in snapshots.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PlacementConfig {
    pub shard_count: u32,
    pub bundle_count: u32,
    pub partition_count: u32,
    pub hash_version: u32,
}

/// Compact routing state distributed by the controller.
///
/// Edges cache this via ArcSwap for lock-free reads.
/// Contains ONLY actual (DSQL-confirmed) ownership, never desired placement.
/// Node endpoints are sourced from the `node_endpoint` column on `shard_lease`
/// rows in DSQL, not from membership streams.
///
/// Queue-home is NOT a pre-materialized map because queue families are unbounded.
/// Instead, `resolve_queue_home` derives the bundle on demand from the partition.
#[derive(Clone, Debug)]
pub struct RoutingSnapshot {
    /// Execution bundle ownership: bundle_id → owner with epoch.
    execution_bundle_owners: HashMap<BundleId, BundleOwner>,
    /// Node endpoints: node_id → network address.
    /// Sourced from shard_lease.node_endpoint in DSQL.
    node_endpoints: HashMap<IncarnationId, NodeEndpoint>,
    /// Hash and mapping configuration.
    placement_config: PlacementConfig,
    /// Monotonically increasing generation.
    generation: GenerationCounter,
}

impl RoutingSnapshot {
    pub fn new(placement_config: PlacementConfig) -> Self {
        Self {
            execution_bundle_owners: HashMap::new(),
            node_endpoints: HashMap::new(),
            placement_config,
            generation: GenerationCounter::ZERO,
        }
    }

    pub fn generation(&self) -> GenerationCounter {
        self.generation
    }

    pub fn placement_config(&self) -> &PlacementConfig {
        &self.placement_config
    }

    pub fn lookup_bundle_owner(&self, bundle_id: BundleId) -> Option<&BundleOwner> {
        self.execution_bundle_owners.get(&bundle_id)
    }

    pub fn lookup_node_endpoint(&self, node_id: IncarnationId) -> Option<&NodeEndpoint> {
        self.node_endpoints.get(&node_id)
    }

    pub fn resolve_queue_home(&self, key: &QueuePartitionKey) -> Option<&BundleOwner> {
        let bundle_id = bundle_for_partition(key.partition, self.placement_config.bundle_count);
        self.execution_bundle_owners.get(&bundle_id)
    }

    /// Apply an incremental delta to update the snapshot.
    /// Returns Err if base_generation does not match current generation.
    pub fn apply_delta(&mut self, delta: RoutingDelta) -> Result<(), RoutingDeltaError> {
        if delta.base_generation != self.generation {
            return Err(RoutingDeltaError::GenerationMismatch {
                expected: delta.base_generation,
                actual: self.generation,
            });
        }
        for (bundle_id, owner) in delta.bundle_updates {
            match owner {
                Some(bo) => { self.execution_bundle_owners.insert(bundle_id, bo); }
                None => { self.execution_bundle_owners.remove(&bundle_id); }
            }
        }
        for (node_id, endpoint) in delta.node_updates {
            match endpoint {
                Some(ep) => { self.node_endpoints.insert(node_id, ep); }
                None => { self.node_endpoints.remove(&node_id); }
            }
        }
        self.generation = delta.generation;
        Ok(())
    }
}

/// Incremental update to a routing snapshot.
#[derive(Clone, Debug)]
pub struct RoutingDelta {
    /// Generation this delta is based on. Must match the snapshot's current generation.
    pub base_generation: GenerationCounter,
    /// Changed bundle ownership. None value means bundle is unowned.
    pub bundle_updates: Vec<(BundleId, Option<BundleOwner>)>,
    /// Changed node endpoints. None value means node removed.
    pub node_updates: Vec<(IncarnationId, Option<NodeEndpoint>)>,
    /// New generation after applying this delta.
    pub generation: GenerationCounter,
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum RoutingDeltaError {
    #[error("generation mismatch: delta base {expected:?} != snapshot {actual:?}")]
    GenerationMismatch {
        expected: GenerationCounter,
        actual: GenerationCounter,
    },
}
```


### 4. LeaseRepository Extensions (`tokeira-storage`)

```rust
// Updated signatures on the existing LeaseRepository trait in api.rs:

/// Attempt to acquire ownership of `bundle`.
/// The `node_endpoint` is written to the lease row so controllers
/// can source endpoints from DSQL for active-active snapshot computation.
/// Treats `owner IS NULL` rows as immediately acquirable (after relinquish).
async fn try_acquire_bundle(
    &self,
    bundle: ShardId,
    owner: String,
    node_endpoint: String,
) -> Result<LeaseOutcome>;

/// Renew an existing lease for `bundle` at the given `epoch`.
/// Updates `node_endpoint` on each renewal so endpoint changes propagate.
async fn renew_bundle(
    &self,
    bundle: ShardId,
    owner: String,
    epoch: ShardEpoch,
    node_endpoint: String,
) -> Result<LeaseOutcome>;

// New methods added to the LeaseRepository trait:

/// List all current bundle leases.
///
/// Used by the controller to build the initial routing snapshot
/// and to reconcile membership with lease state.
async fn list_bundle_leases(&self) -> Result<Vec<BundleLease>>;

/// Explicitly relinquish ownership of a bundle using epoch-checked CAS.
///
/// Clears the owner and advances the epoch so another node
/// can acquire the bundle immediately without waiting for
/// lease expiry.
async fn relinquish_bundle(
    &self,
    bundle: ShardId,
    owner: String,
    epoch: ShardEpoch,
) -> Result<LeaseOutcome>;

// New return type:
#[derive(Clone, Debug, PartialEq)]
pub struct BundleLease {
    pub bundle_id: ShardId,
    /// None when the bundle is unowned (after relinquish).
    pub owner_node_id: Option<String>,
    pub epoch: ShardEpoch,
    pub lease_until: OffsetDateTime,
    /// Network endpoint written by the runtime when acquiring the lease.
    /// Sourced from shard_lease.node_endpoint in DSQL.
    pub node_endpoint: Option<String>,
}
```

### 5. Controller gRPC Service (`tokeira-proto`)

Proto definitions in `proto/tokeira/internal/controller/controller.proto`:

```protobuf
syntax = "proto3";
package tokeira.internal.controller;

import "google/protobuf/timestamp.proto";

service PlacementController {
    // Bidirectional stream for runtime membership.
    // Runtime sends registration + heartbeats; controller sends directives.
    rpc RuntimeMembership(stream RuntimeMembershipRequest) returns (stream ControllerDirective);

    // Server-streaming subscription for routing snapshot updates.
    rpc SubscribeRouting(SubscribeRoutingRequest) returns (stream RoutingUpdate);

    // Unary RPC for edge to refresh a specific bundle's routing.
    rpc RefreshBundle(RefreshBundleRequest) returns (RefreshBundleResponse);

    // Nominate nodes suitable for scale-in retirement.
    rpc NominateScaleInCandidates(NominateRequest) returns (NominateResponse);

    // Mark a node as draining for scale-in.
    rpc MarkNodeDraining(MarkDrainingRequest) returns (MarkDrainingResponse);
}

// Split registration + heartbeat using oneof (not empty-string sentinels)
message RuntimeMembershipRequest {
    oneof request {
        RuntimeRegistration registration = 1;
        RuntimeHeartbeat heartbeat = 2;
    }
}

message RuntimeRegistration {
    string node_id = 1;       // IncarnationId as UUID string
    string host = 2;
    uint32 port = 3;
    string zone = 4;
    string version = 5;
    string build_id = 6;
}

message RuntimeHeartbeat {
    string node_id = 1;
    repeated uint32 owned_bundles = 2;
    uint32 owned_bundle_count = 3;
    uint64 runnable_transitions = 4;
    uint64 active_actor_count = 5;
    uint64 backlog_depth = 6;
    uint32 available_connections = 7;
    float connection_rate_headroom = 8;
    NodeDrainState drain_state = 9;
    repeated LanePressure lane_pressures = 10;
}

message LanePressure {
    uint32 lane_id = 1;
    uint64 runnable_depth = 2;
    uint64 active_actors = 3;
    float utilization = 4;
}

enum NodeDrainState {
    NODE_DRAIN_STATE_ACTIVE = 0;
    NODE_DRAIN_STATE_DRAINING = 1;
    NODE_DRAIN_STATE_SAFE_TO_TERMINATE = 2;
}

message ControllerDirective {
    oneof directive {
        DrainDirective drain = 1;
        RoutingUpdate routing_update = 2;
        ConnectionBudgetDirective connection_budget = 3;
        DesiredPlacementDirective desired_placement = 4;
    }
}

message DrainDirective {
    // Instructs the runtime to begin two-phase drain.
}

message DesiredPlacementDirective {
    // Bundles the controller wants this runtime to acquire.
    repeated uint32 acquire_bundles = 1;
    // Bundles the controller wants this runtime to relinquish.
    repeated uint32 relinquish_bundles = 2;
}

message ConnectionBudgetDirective {
    // Per-node share of the cluster-wide DSQL connection budget.
    double rate_per_second = 1;
    uint64 capacity = 2;
    uint32 max_reservoir_size = 3;
    google.protobuf.Timestamp valid_until = 4;
}

message SubscribeRoutingRequest {
    // Optional: last known generation for delta-only updates.
    uint64 last_known_generation = 1;
}

message RoutingUpdate {
    oneof update {
        FullRoutingSnapshot full_snapshot = 1;
        RoutingDeltaMessage delta = 2;
    }
}

message FullRoutingSnapshot {
    repeated BundleOwnershipEntry bundles = 1;
    // Queue-home is derived on demand from partition → bundle mapping.
    // No QueuePartitionHomeEntry needed — queue families are unbounded.
    repeated NodeEndpointEntry nodes = 2;
    PlacementConfigMessage placement_config = 3;
    uint64 generation = 4;
}

message RoutingDeltaMessage {
    uint64 base_generation = 1;
    repeated BundleOwnershipEntry bundle_updates = 2;
    repeated NodeEndpointEntry node_updates = 3;
    uint64 generation = 4;
}

// Uses oneof for ownership state instead of empty-string sentinels
message BundleOwnershipEntry {
    uint32 bundle_id = 1;
    oneof state {
        BundleOwnerMessage owned = 2;
        bool unowned = 3;  // true means explicitly unowned
    }
}

message BundleOwnerMessage {
    string owner_node_id = 1;  // IncarnationId UUID text
    uint64 epoch = 2;
}

// QueuePartitionHomeEntry removed — queue-home is derived on demand
// from partition → bundle_for_partition → bundle owner lookup.
// Queue families are unbounded so cannot be pre-materialized.

message NodeEndpointEntry {
    string node_id = 1;
    oneof state {
        NodeEndpointMessage endpoint = 2;
        bool removed = 3;
    }
}

message NodeEndpointMessage {
    string host = 1;
    uint32 port = 2;
}

message PlacementConfigMessage {
    uint32 shard_count = 1;
    uint32 bundle_count = 2;
    uint32 partition_count = 3;
    uint32 hash_version = 4;
}

message RefreshBundleRequest {
    uint32 bundle_id = 1;
}

message RefreshBundleResponse {
    BundleOwnershipEntry bundle = 1;
    NodeEndpointEntry node = 2;
}

message NominateRequest {
    uint32 max_candidates = 1;
}

message NominateResponse {
    repeated ScaleInCandidate candidates = 1;
}

message ScaleInCandidate {
    string node_id = 1;
    uint32 owned_bundle_count = 2;
    uint64 runnable_transitions = 3;
}

message MarkDrainingRequest {
    string node_id = 1;
}

message MarkDrainingResponse {
    bool accepted = 1;
    string reason = 2;
}
```


### 6. Placement Controller (`tokeira-controller`)

The controller is a new crate with the following internal structure:

```
tokeira-controller/
├── src/
│   ├── lib.rs              — crate root, re-exports
│   ├── membership.rs       — live node membership tracking
│   ├── placement.rs        — routing snapshot computation, desired placement
│   ├── service.rs          — gRPC service implementation
│   ├── drain.rs            — two-phase drain coordination
│   ├── generation.rs       — CAS-based generation counter
│   └── config.rs           — controller configuration
```

#### Active-Active Coordination (`generation.rs`)

No leader election. All controllers read DSQL lease rows independently. The generation counter is the only shared mutable state, protected by CAS:

```rust
pub struct GenerationManager {
    control_repo: Arc<dyn ControlRepository>,
}

impl GenerationManager {
    /// Advance the generation counter using CAS.
    /// Returns the new generation on success, or the current generation on CAS failure.
    pub async fn advance_generation(&self, expected: GenerationCounter) -> Result<GenerationAdvanceResult> {
        self.control_repo.advance_generation(expected).await
    }

    /// Read the current generation from DSQL.
    pub async fn current_generation(&self) -> Result<GenerationCounter> {
        self.control_repo.current_generation().await
    }
}

pub enum GenerationAdvanceResult {
    Advanced(GenerationCounter),
    Conflict(GenerationCounter), // another controller advanced first
}
```

#### Control Repository (`tokeira-storage`)

The `ControlRepository` trait provides the storage surface for generation counter and budget allocation — the two CAS-protected DSQL rows that active-active controllers coordinate through:

```rust
/// Repository for controller coordination state (generation counter, budget allocation).
///
/// Separate from LeaseRepository because these operations are controller-only,
/// not runtime-only. Both traits may be implemented by the same backing store.
#[async_trait]
pub trait ControlRepository: Send + Sync {
    /// Advance the generation counter using CAS.
    /// UPDATE routing_generation SET generation = generation + 1
    /// WHERE id = 1 AND generation = $expected RETURNING generation
    async fn advance_generation(&self, expected: GenerationCounter) -> Result<GenerationAdvanceResult>;

    /// Read the current generation.
    async fn current_generation(&self) -> Result<GenerationCounter>;

    /// Attempt to allocate connection budget using CAS on the version column.
    /// UPDATE budget_allocation SET version = version + 1, allocator_id = $id,
    ///   allocated_at = now(), rate_budget = $rate, capacity_budget = $cap
    /// WHERE id = 1 AND version = $expected RETURNING version
    async fn allocate_budget(
        &self,
        expected_version: u64,
        allocator_id: Uuid,
        rate_budget: f64,
        capacity_budget: u64,
    ) -> Result<BudgetAllocationResult>;

    /// Read the current budget allocation version.
    async fn current_budget_version(&self) -> Result<u64>;
}

pub enum BudgetAllocationResult {
    Allocated { version: u64 },
    Conflict { current_version: u64 },
}
```

#### Live Membership (`membership.rs`)

Tracks connected runtime nodes and their last heartbeat state:

```rust
pub struct LiveMembership {
    nodes: HashMap<IncarnationId, LiveNode>,
    grace_interval: Duration,
}

pub struct LiveNode {
    pub node_id: IncarnationId,
    pub registration: RuntimeRegistration,
    pub last_heartbeat: Instant,
    pub heartbeat: RuntimeHeartbeat,
    pub state: NodeMembershipState,
    pub reachability: NodeReachability,
    /// Handle to the stream for sending directives.
    pub directive_tx: mpsc::Sender<ControllerDirective>,
    // Note: node_endpoint is NOT stored here. Endpoints are sourced from
    // shard_lease.node_endpoint in DSQL. Membership streams carry pressure
    // metrics only.
}

pub enum NodeMembershipState {
    Active,
    GracePeriod { deadline: Instant },
    Unavailable,
    Draining,
}
```

#### Routing Snapshot Computation (`placement.rs`)

The controller computes the routing snapshot by combining live membership with DSQL lease state. It also computes desired placement and sends directives:

```rust
pub fn compute_routing_snapshot(
    membership: &LiveMembership,
    leases: &[BundleLease],
    placement_config: &PlacementConfig,
    previous_generation: GenerationCounter,
) -> (RoutingSnapshot, RoutingDelta) {
    // 1. Build execution_bundle_owners from DSQL lease rows alone (BundleOwner with epoch).
    //    Include ALL owned bundles regardless of membership state — DSQL is authoritative.
    //    Membership is NOT consulted for actual ownership; only DSQL leases determine
    //    what the RoutingSnapshot advertises. This ensures active-active determinism.
    //    Parse owner string as IncarnationId UUID; skip rows with malformed owner (log warning).
    // 2. Build node_endpoints from shard_lease.node_endpoint (DSQL), NOT from membership
    //    Parse node_endpoint string as host:port; skip rows with missing/malformed endpoint.
    // 3. Queue-home is derived on demand — no pre-materialized map needed.
    // 4. Compute delta from previous snapshot
    // 5. Set base_generation on delta
}

pub fn compute_desired_placement(
    membership: &LiveMembership,
    leases: &[BundleLease],
    bundle_count: u32,
) -> Vec<DesiredPlacementDirective> {
    // Compute which bundles each runtime should acquire or relinquish
    // based on load balancing, zone awareness, etc.
}
```

#### Connection Budget Allocation

When the active node count changes, the controller recomputes per-node shares and sends `ConnectionBudgetDirective` with expiry:

```rust
pub fn compute_connection_budget(
    cluster_rate: f64,
    cluster_capacity: u64,
    active_nodes: &[IncarnationId], // sorted for deterministic remainder distribution
    valid_duration: Duration,
) -> Vec<(IncarnationId, ConnectionBudgetDirective)> {
    let node_count = active_nodes.len();
    if node_count == 0 {
        return vec![]; // handle zero active nodes
    }
    let base_rate = cluster_rate / node_count as f64;
    let base_capacity = cluster_capacity / node_count as u64;
    let remainder = cluster_capacity % node_count as u64;
    // Distribute remainder deterministically by sorted IncarnationId
    // Set valid_until = now + valid_duration
}
```

### 7. Runtime Membership Client (`tokeira-runtime`)

The runtime opens a membership stream to the controller at startup, sending registration first:

```rust
pub struct MembershipClient {
    controller_endpoint: String,
    node_id: IncarnationId,
    node_endpoint: NodeEndpoint,
    registration: RuntimeRegistration,
    heartbeat_interval: Duration,
    cancel: CancellationToken,
}

impl MembershipClient {
    /// Run the membership stream loop.
    /// Sends registration, then heartbeats. Receives directives.
    /// Reconnects with backoff on stream drop.
    pub async fn run(
        &self,
        shard_owner: Arc<RwLock<ShardOwner>>,
        drain_signal: Arc<Notify>,
        rate_limiter: Arc<TokenBucketRateLimiter>,
        reservoir: Arc<Reservoir>,
    ) -> Result<()> {
        // 1. Connect to controller
        // 2. Send RuntimeRegistration as first message
        // 3. Loop: send heartbeats, receive directives
        // 4. On DesiredPlacementDirective: attempt acquire/relinquish
        // 5. On ConnectionBudgetDirective: reconfigure rate limiter via
        //    TokenBucketRateLimiter::reconfigure(rate_per_second, capacity),
        //    cap reservoir via Reservoir::reconfigure_target(max_reservoir_size),
        //    retire excess via Reservoir::retire_excess(max_reservoir_size) if oversized,
        //    track valid_until
        // 6. On DrainDirective: begin two-phase drain
        // 7. On disconnect: retain budget until valid_until, then degrade to conservative default
    }
}
```

### 8. Edge Routing Cache (`tokeira-edge`)

```rust
use arc_swap::ArcSwap;

pub struct RoutingCache {
    snapshot: ArcSwap<RoutingSnapshot>,
    controller_endpoint: String,
    cancel: CancellationToken,
}

impl RoutingCache {
    /// Resolve the runtime endpoint for an execution-home operation.
    /// Used for Start, Signal, Update, Query, Cancel.
    pub fn resolve_execution_home(
        &self,
        namespace_id: &[u8],
        workflow_id: &[u8],
    ) -> Option<(IncarnationId, NodeEndpoint, ShardEpoch)> {
        let snap = self.snapshot.load();
        let bundle_id = execution_home_bundle(
            namespace_id,
            workflow_id,
            snap.placement_config().bundle_count,
        );
        let owner = snap.lookup_bundle_owner(bundle_id)?;
        let endpoint = snap.lookup_node_endpoint(owner.node_id)?;
        Some((owner.node_id, endpoint.clone(), owner.epoch))
    }

    /// Resolve the runtime endpoint for a queue-home operation.
    /// Used for poll routing and task dispatch.
    /// Derives the bundle on demand from the partition — no pre-materialized map.
    pub fn resolve_queue_home(
        &self,
        key: &QueuePartitionKey,
    ) -> Option<(IncarnationId, NodeEndpoint, ShardEpoch)> {
        let snap = self.snapshot.load();
        let owner = snap.resolve_queue_home(key)?;
        let endpoint = snap.lookup_node_endpoint(owner.node_id)?;
        Some((owner.node_id, endpoint.clone(), owner.epoch))
    }

    /// Run the background subscription loop.
    pub async fn run_subscription(&self) -> Result<()> { ... }
}
```

### 9. NotShardOwner Recovery Flow with Redirect Hints

When the edge receives a `NotShardOwner` error:

```rust
pub async fn route_with_retry<F, R>(
    cache: &RoutingCache,
    namespace_id: &[u8],
    workflow_id: &[u8],
    controller_client: &ControllerClient,
    lease_repo: &dyn LeaseRepository,
    max_retries: u32,
    make_request: F,
) -> Result<R>
where
    F: Fn(&NodeEndpoint, ShardEpoch) -> Pin<Box<dyn Future<Output = Result<R>>>>,
{
    let mut retries = 0;
    loop {
        let (node_id, endpoint, epoch) = cache.resolve_execution_home(namespace_id, workflow_id)
            .ok_or_else(|| anyhow!("no owner for execution home"))?;

        match make_request(&endpoint, epoch).await {
            Ok(result) => return Ok(result),
            Err(e) if let Some(nso) = extract_not_shard_owner(&e) => {
                retries += 1;
                if retries > max_retries {
                    return Err(anyhow!("routing failed after {} retries", max_retries));
                }
                // Recovery option 1: use hint — route directly to hinted owner,
                // bypassing the stale cache entry
                if let Some(hint_node_id) = nso.current_owner_node_id {
                    if let Some(hinted_endpoint) = cache.lookup_node_endpoint(hint_node_id) {
                        match make_request(&hinted_endpoint, nso.current_epoch).await {
                            Ok(result) => return Ok(result),
                            Err(_) => {
                                // Hint failed, fall through to controller refresh
                            }
                        }
                    }
                }
                // Recovery option 2: RefreshBundle from controller
                if let Ok(refresh) = controller_client.refresh_bundle(nso.bundle_id).await {
                    cache.apply_refresh(refresh);
                    continue;
                }
                // Recovery option 3: DSQL lease lookup fallback
                if let Ok(lease) = lease_repo.list_bundle_leases().await {
                    if let Some(bl) = lease.iter().find(|l| l.bundle_id == nso.bundle_id) {
                        if let (Some(owner_str), Some(ep_str)) = (&bl.owner_node_id, &bl.node_endpoint) {
                            // Parse owner as IncarnationId UUID — skip if malformed
                            if let (Ok(node_id), Ok(endpoint)) = (
                                owner_str.parse::<Uuid>().map(IncarnationId),
                                NodeEndpoint::parse(ep_str),
                            ) {
                                let owner = BundleOwner { node_id, epoch: bl.epoch };
                                cache.apply_dsql_fallback(nso.bundle_id, owner, &endpoint);
                                continue;
                            }
                        }
                    }
                }
            }
            Err(e) => return Err(e),
        }
    }
}
```

Edge retry is safe only because `request_dedupe` is persisted atomically with workflow state mutation.

## Data Models

### Controller Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `controller_addr` | `SocketAddr` | `0.0.0.0:9091` | gRPC listen address |
| `heartbeat_interval` | `Duration` | `5s` | Expected heartbeat interval from runtimes |
| `grace_interval` | `Duration` | `30s` | Time before marking a disconnected node unavailable |
| `snapshot_publish_interval` | `Duration` | `5s` | Minimum interval between snapshot publications |
| `bundle_count` | `u32` | `64` | Total number of lease bundles |
| `partition_count` | `u32` | `1024` | Total number of queue partitions |
| `shard_count` | `u32` | `64` | Total number of shards (== bundle_count for MVP) |
| `hash_version` | `u32` | `1` | Hash algorithm version for placement config |
| `budget_directive_validity` | `Duration` | `60s` | How long a connection budget directive is valid |

### Edge Routing Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `controller_endpoint` | `String` | `controller:9091` | Controller gRPC endpoint |
| `max_not_shard_owner_retries` | `u32` | `3` | Max retries on NotShardOwner |
| `staleness_warning_threshold` | `Duration` | `60s` | Log warning when cache is this old |

### Runtime Membership Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `controller_endpoint` | `String` | `controller:9091` | Controller gRPC endpoint |
| `heartbeat_interval` | `Duration` | `5s` | Heartbeat send interval |
| `reconnect_backoff_base` | `Duration` | `1s` | Base backoff for reconnection |
| `reconnect_backoff_max` | `Duration` | `30s` | Max backoff for reconnection |

### DSQL Tables

#### Shard Lease (updated in-place in V002)

The `shard_lease` table is updated in-place (schema version 1 convention) to make `owner` nullable and add `node_endpoint`:

```sql
CREATE TABLE IF NOT EXISTS shard_lease (
    shard_id        UUID        NOT NULL,
    owner           TEXT,
    epoch           BIGINT      NOT NULL,
    lease_expiry    TIMESTAMPTZ NOT NULL,
    node_endpoint   TEXT,
    PRIMARY KEY (shard_id)
);
```

`owner` is nullable so that `relinquish_bundle` can set it to `NULL` to represent an unowned bundle. `node_endpoint` stores the runtime's `host:port` so controllers can source endpoints from DSQL lease rows rather than membership streams.

#### Generation Counter

```sql
-- V043: CREATE TABLE
CREATE TABLE IF NOT EXISTS routing_generation (
    id          integer PRIMARY KEY,
    generation  bigint NOT NULL DEFAULT 0,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- V044: Seed singleton row
INSERT INTO routing_generation (id, generation, updated_at)
VALUES (1, 0, now())
ON CONFLICT (id) DO NOTHING;
```

The singleton row with `id = 1` must exist for `SELECT ... WHERE id = 1` and CAS updates to work. The seed migration ensures this on a fresh schema.

#### Connection Budget Allocation

```sql
-- V045: CREATE TABLE
CREATE TABLE IF NOT EXISTS budget_allocation (
    id              integer PRIMARY KEY,
    version         bigint NOT NULL DEFAULT 0,
    allocator_id    uuid,
    allocated_at    timestamptz,
    rate_budget     double precision NOT NULL DEFAULT 100.0,
    capacity_budget bigint NOT NULL DEFAULT 10000
);

-- V046: Seed singleton row
INSERT INTO budget_allocation (id, version, rate_budget, capacity_budget)
VALUES (1, 0, 100.0, 10000)
ON CONFLICT (id) DO NOTHING;
```

The `version` column provides CAS protection: `UPDATE ... WHERE id = 1 AND version = $expected RETURNING version`. Competing active-active controllers race on the version — exactly one succeeds per allocation cycle.

Each table is created by a dedicated migration file, with a separate seed file (one SQL statement per file per DSQL convention — 4 files total).


## Safety Analysis and Known Limitations

This section documents the safety properties the design relies on, the failure modes it tolerates, and the gaps that remain. This analysis is intended to inform a future TLA+ model.

### Why active-active controllers are safe

All controllers read the same DSQL lease rows and compute routing snapshots deterministically from that state. Node endpoints are sourced from the `node_endpoint` column on `shard_lease` rows — not from membership streams — so any controller reading the same lease rows produces the same endpoint map. The only shared mutable state is the generation counter, which is protected by CAS:

- **No split-brain risk from dual computation:** Two controllers computing snapshots from the same DSQL state produce identical routing maps. If they read at slightly different times, the CAS on the generation counter ensures only one publishes per increment.
- **CAS failure is benign:** A controller that loses the CAS race simply re-reads the current generation and retries. No stale snapshot is published because the generation counter is monotonically increasing.
- **Connection budget allocation:** Uses a narrow CAS-protected budget allocation row in DSQL. Multiple controllers may race to allocate, but CAS ensures only one succeeds per allocation cycle.

**Compared to leader election:** The previous design used a DSQL time-based lease for leader election, which had a stale-leader window where two controllers could both believe they were leader. Active-active eliminates this failure mode entirely — there is no leader, so there is no stale leader.

### Bundle lease fencing — the real correctness boundary

Bundle leases in DSQL are the authoritative fence for workflow commits. The safety argument:

1. Owner acquires bundle lease with epoch N.
2. Owner commits transitions checking `epoch == N` in the same DSQL transaction.
3. If the lease expires and another node acquires with epoch N+1, the old owner's commits fail because `epoch != N+1`.

**This works if and only if** the old owner's in-flight transaction completes (commit or abort) before the new owner's first commit succeeds. DSQL's OCC with Repeatable Read should guarantee this — a transaction that read epoch N will conflict with a concurrent write that changed epoch to N+1. The `SELECT epoch ... FOR UPDATE` in `commit_transition` should ensure this.

**Execution-home bundle in commit fencing:** The edge routes by `execution_home_bundle(namespace_id, workflow_id)` which hashes the canonical execution key. The runtime's `commit_transition` must accept the execution-home `BundleId` from the edge request and fence against that bundle's epoch — not derive the bundle from `run_key` (which includes `run_id` and produces a different hash via `shard_for`). This ensures the commit fence matches the routing path.

**Internal commits (non-edge-routed):** Workflow task completions, activity completions, timer firings, scanner-driven transitions, and recovery replays are not directly edge-routed. For these, the runtime derives the execution-home `BundleId` from the loaded run's `(namespace_id, workflow_id)` using `execution_home_bundle`. The `ShardOwner` validates the derived bundle matches a locally-owned bundle before committing. This is safe because the runtime only processes runs for bundles it owns.

**Epoch in routing entries provides fast-fail:** The edge includes `observed_bundle_epoch` in requests. The runtime checks this against its local epoch before attempting a DSQL transaction. This avoids wasting a DSQL round-trip on requests that are already known to be stale.

**Gap for TLA+ verification:** What happens if the old owner's transaction started before the epoch change but attempts to commit after? DSQL's OCC should reject it (SQLSTATE 40001 serialization conflict), but this depends on DSQL treating the epoch read as part of the transaction's read set. The `SELECT epoch ... FOR UPDATE` in `commit_transition` should ensure this, but it has not been formally verified.

**Recommended TLA+ model scope:** Model the epoch-fenced commit protocol with two concurrent owners, one stale. Verify that no two successful commits for the same bundle can occur at different epochs without a serialization conflict between them.

### Desired vs actual placement safety

The desired-vs-actual split ensures the RoutingSnapshot never advertises ownership that DSQL has not confirmed:

1. Controller computes desired placement and sends directives to runtimes.
2. Runtimes attempt to acquire leases in DSQL (may fail due to CAS conflict).
3. Controller reads actual lease state from DSQL.
4. Controller publishes snapshot with only actual ownership.

**Key invariant:** The RoutingSnapshot is always a lagging view of DSQL truth. It may be stale (a lease expired but the snapshot hasn't updated yet), but it never advertises ownership that doesn't exist in DSQL. Stale routing is repaired by NotShardOwner.

### Connection budget fair-share during controller coordination

With active-active controllers, connection budget allocation uses a CAS-protected DSQL row:

- Any controller can attempt to allocate budgets.
- CAS ensures only one allocation succeeds per cycle.
- The `valid_until` field on directives ensures runtimes degrade to conservative defaults if no controller is available.

**Bounded overshoot:** During the window between a node joining/leaving and the next budget reallocation, the sum of retained shares may not equal the cluster budget. The `valid_until` expiry bounds this window.

**Known limitation:** This is not work-conserving. If one node is idle and another is bursting, the idle node's unused share is not redistributed.

### Two-phase drain safety

The two-phase drain protocol ensures routing moves before ownership is relinquished, but routing does not advertise the new owner until DSQL confirms the new lease:

1. Node marked DRAINING → routing snapshot stops sending new work to it.
2. Runtime relinquishes bundles with epoch-checked CAS.
3. New owner acquires bundle in DSQL with new epoch.
4. Controller observes new ownership in DSQL → publishes updated snapshot.

**Key invariant:** There is no window where the routing snapshot advertises a new owner that hasn't actually acquired the DSQL lease.

### Execution-home correctness

The previous design had a correctness bug: Start routed by queue partition (producing one bundle) while Signal routed by run_key (producing a different bundle). This meant Start and Signal for the same workflow identity could route to different runtime nodes.

**Fix:** All operations (Start, Signal, Query, Update, Cancel) now route by execution-home derived from `hash(namespace_id, workflow_id) -> bundle_id`. Queue partition is used only for task dispatch after the execution exists.

### Summary of safety properties

| Property | Mechanism | Confidence | Notes |
|---|---|---|---|
| No leader election needed | Active-active with CAS generation | High | Eliminates stale-leader window entirely |
| At most one bundle owner can commit | DSQL epoch fence in same transaction | High | Depends on DSQL OCC detecting epoch read conflicts |
| Routing snapshots are monotonically ordered | CAS-protected generation counter in DSQL | High | Any controller can advance; CAS ensures ordering |
| Routing snapshot reflects only actual ownership | Controller reads DSQL leases, not desired state | High | Desired-vs-actual split is explicit |
| Connection budget never exceeds cluster limit | CAS-protected allocation + valid_until expiry | Medium | Bounded overshoot during reallocation; not work-conserving |
| Stale routing is recoverable | NotShardOwner + hint + controller refresh + DSQL fallback | High | Multiple recovery paths; request_dedupe ensures retry safety |
| Controller unavailability does not affect correctness | DSQL leases + cached routing | High | Only rebalance and budget updates pause |
| Start and Signal route to same execution-home | Both use hash(namespace_id, workflow_id) | High | Correctness fix from previous design |
| Commit fencing matches routing path | commit_transition accepts execution-home BundleId from edge | High | Prevents mismatch between routing hash and fencing hash |
| Node endpoints are deterministic across controllers | Sourced from shard_lease.node_endpoint in DSQL | High | All controllers reading same lease rows produce same endpoint map |
| Drain does not lose in-flight work | Two-phase: routing moves before ownership relinquished | High | DSQL confirms new owner before routing advertises it |

### Candidates for TLA+ verification

1. **Epoch-fenced commit protocol** — two concurrent owners, one stale, verify no dual-commit.
2. **Bundle lease acquire/renew/expire lifecycle** — verify epoch monotonicity and single-owner invariant.
3. **Routing snapshot generation monotonicity with CAS** — verify no regression across multiple active controllers.
4. **Two-phase drain** — verify no window where routing advertises unconfirmed ownership.

## Correctness Properties

### Property 1: Deterministic queue partition mapping

*For any* placement key and partition count > 0, `queue_partition_for(key, count)` SHALL always produce the same `QueuePartition`, and the result SHALL be in the range `[0, count)`.

**Validates: Requirements 6.1.1, 6.1.3, 6.1.4**

### Property 2: Deterministic bundle-for-partition mapping

*For any* `QueuePartition` and bundle count > 0, `bundle_for_partition(partition, count)` SHALL always produce the same `BundleId`, and the result SHALL be in the range `[0, count)`.

**Validates: Requirements 3.3.1, 3.3.2, 3.3.4**

### Property 3: Routing snapshot delta round-trip with base_generation validation

*For any* `RoutingSnapshot` S and `RoutingDelta` D where `D.base_generation == S.generation`, applying D to S SHALL succeed and produce a snapshot with `generation == D.generation`. If `D.base_generation != S.generation`, `apply_delta` SHALL return `Err`.

**Validates: Requirements 4.2.4, 7.2.7**

### Property 4: Routing snapshot generation monotonicity

*For any* sequence of successfully applied `RoutingDelta` applications, the `GenerationCounter` of the resulting snapshot SHALL be strictly greater than the generation of the previous snapshot.

**Validates: Requirements 4.1.3, 4.3.1**

### Property 5: Bundle ownership consistency

*For any* set of `BundleLease` rows where each bundle has at most one owner, `compute_routing_snapshot` SHALL produce a `RoutingSnapshot` where each bundle maps to at most one `BundleOwner`, and that owner's `node_id` and `epoch` match the lease row.

**Validates: Requirements 4.1.1, 4.1.7**

### Property 6: Queue partition uniform distribution

*For any* set of N random placement keys and partition count P, the distribution of `queue_partition_for` results across partitions SHALL have a chi-squared statistic below the critical value for uniform distribution at the 99% confidence level (for sufficiently large N). The chi-squared test SHALL be deterministically seeded.

**Validates: Requirements 6.1.4**

### Property 7: Execution-home consistency for Start and Signal

*For any* `(namespace_id, workflow_id)` pair and bundle count, `execution_home_bundle(namespace_id, workflow_id, bundle_count)` SHALL produce the same `BundleId` regardless of which operation (Start, Signal, Query, Update, Cancel) is being routed. This validates that the Start flow and Signal flow resolve to the same execution-home.

**Validates: Requirements 5.1.3, 5.1.4, 6.3.1, 6.3.2**

## Error Handling

### Active-Active Controller Coordination Failures

- If DSQL is unreachable, the controller retries with exponential backoff. It does not publish stale snapshots.
- If the CAS generation advance fails (another controller advanced first), the controller re-reads the current generation and retries.
- Multiple controllers racing to advance the generation is safe — CAS ensures exactly one succeeds per increment.

### Membership Stream Failures

- Runtime sends `RuntimeRegistration` as the first message on reconnect.
- Runtime reconnects with exponential backoff (1s base, 30s max).
- Controller uses grace interval before marking node unavailable.
- Stream drops do NOT revoke bundle ownership — only DSQL lease expiry does that.

### Routing Cache Failures

- If the controller subscription drops, the edge continues with its last-known snapshot via `ArcSwap`.
- NotShardOwner errors trigger multi-path recovery: hint routing, controller refresh, DSQL fallback.
- After max retries, the edge returns a transient error to the caller.

### Bundle Relinquish Failures

- If relinquish fails due to epoch mismatch, the bundle was already transferred — the runtime treats this as success.
- If relinquish fails due to DSQL unavailability, the runtime retries. The bundle will eventually expire if the runtime crashes.
- Relinquish uses epoch-checked CAS to prevent race conditions during drain.

### Connection Budget Expiry

- When a `ConnectionBudgetDirective` expires (past `valid_until`), the runtime degrades to conservative defaults.
- This prevents a disconnected runtime from consuming more than its fair share indefinitely.

## Testing Strategy

### Property-Based Tests

| Property | Test Location | Generator Strategy |
|---|---|---|
| Property 1: Queue partition determinism | `tokeira-types/src/routing.rs` | Generate random byte slices and partition counts 1..4096. Assert same input -> same output, result in range. |
| Property 2: Bundle-for-partition determinism | `tokeira-types/src/routing.rs` | Generate random QueuePartition values and bundle counts 1..256. Assert same input -> same output, result in range. |
| Property 3: Routing snapshot delta round-trip | `tokeira-types/src/routing.rs` | Generate random snapshots and deltas with matching base_generation. Apply delta, verify entries match. Generate mismatched base_generation, verify Err. |
| Property 4: Generation monotonicity | `tokeira-types/src/routing.rs` | Generate sequences of deltas with increasing generations and correct base_generation chaining. Apply in order, verify generation always increases. |
| Property 5: Bundle ownership consistency | `tokeira-controller/src/placement.rs` | Generate random BundleLease sets (at most one owner per bundle). Compute snapshot, verify 1:1 mapping with epoch. |
| Property 6: Queue partition distribution | `tokeira-types/src/routing.rs` | Generate 10,000 random keys with deterministic seed, compute partitions, chi-squared test for uniformity. |
| Property 7: Execution-home consistency | `tokeira-types/src/routing.rs` | Generate random (namespace_id, workflow_id) pairs, verify execution_home_bundle produces same result for same input. |

### Unit Tests

- **Active-active coordination:** Verify CAS generation advance succeeds for one controller, fails for concurrent. Verify re-read and retry.
- **Membership tracking:** Verify node registration, heartbeat update, grace-period transitions, drain state. Verify NodeReachability transitions.
- **Routing snapshot:** Verify full snapshot construction from leases with epoch. Verify delta application with base_generation validation. Verify generation advancement. Verify on-demand queue-home derivation via `resolve_queue_home`.
- **Routing cache:** Verify ArcSwap-based lookup returns correct endpoint with epoch. Verify execution-home resolution for Start and Signal produces same result. Verify stale cache still serves.
- **NotShardOwner retry:** Verify hint-based routing. Verify controller refresh fallback. Verify DSQL fallback. Verify max retries.
- **Two-phase drain:** Verify routing moves before ownership relinquished. Verify SAFE_TO_TERMINATE conditions. Verify epoch-checked CAS relinquish.
- **Bundle relinquish:** Verify epoch validation. Verify epoch advancement on success.
- **Connection budget:** Verify zero-node handling. Verify remainder distribution by sorted IncarnationId. Verify valid_until expiry triggers conservative default.

### Integration Tests

- **End-to-end routing:** Start controller + 2 runtimes + 1 edge. Verify edge routes Start and Signal for same workflow_id to same runtime. Kill one runtime, verify edge recovers via NotShardOwner with hints.
- **Active-active controllers:** Start 2 controllers. Verify both can serve routing snapshots. Verify CAS generation ordering.
- **Two-phase drain:** Mark a runtime as draining. Verify routing moves first, then ownership relinquished, then SAFE_TO_TERMINATE reported.
