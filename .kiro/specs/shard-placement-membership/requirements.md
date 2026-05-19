# Requirements Document: Shard Placement and Membership

## Introduction

This document captures the requirements for implementing the shard placement and membership system — the foundation for multi-node Tokeira deployments. Currently Tokeira runs single-node with in-memory shard ownership. This spec introduces distributed shard assignment via a placement controller, runtime membership streams, routing snapshot distribution, edge routing caches, a node drain protocol for safe scale-in, and controller-coordinated DSQL connection budget allocation.

The authoritative architecture documents are [035-placement-and-membership](../../../docs/architecture/035-placement-and-membership.md), [037-dynamic-placement](../../../docs/architecture/037-dynamic-placement.md), [045-autoscaling-on-ecs-ec2](../../../docs/architecture/045-autoscaling-on-ecs-ec2.md), and [090-failover-and-recovery](../../../docs/architecture/090-failover-and-recovery.md).

### Mental model

> DSQL owns truth. Runtimes own bundles only by DSQL epoch lease. Controllers observe actual ownership and compute desired movement. Edges consume routing hints. Every routed mutation is retried safely only because request dedupe is durable. Every stale route is repaired by NotShardOwner. Queue placement is an optimisation layer, not an execution correctness layer.

### Key design principles

1. **No external coordination store on the request path.** The hot path for a workflow mutation or worker poll consults only the edge's local cached placement map — no DynamoDB read, no controller RPC, no membership lookup.
2. **Membership is advisory; DSQL fencing is authoritative.** Membership and placement can be stale for a short time as long as stale owners cannot commit. DSQL lease epochs are the only authoritative ownership mechanism.
3. **Controller unavailability must not affect workflow correctness.** Existing runtime owners continue renewing bundle leases, edges continue using cached routing. Only rebalance and new-placement decisions pause.
4. **Routing granularity is finer than lease granularity.** Queue partitions (large space) provide even spread and cheap rebalance. Lease bundles (smaller space) keep authoritative lease renewal cheap. For this spec (MVP), queue-home always coincides with bundle ownership — `partition → bundle_for_partition → bundle owner`. Independent queue-partition placement (finer-grained than bundles) is deferred to the 037-dynamic-placement spec.
5. **The workflow run remains the unit of correctness.** Cells, bundles, and queue partitions are placement devices, not execution subsystems.
6. **Execution-home is the correctness boundary; queue-home is an optimisation.** Start, Signal, Query, Update all route by execution-home derived from the canonical execution key `(namespace_id, workflow_id)`. Queue partition placement is used only for dispatching workflow tasks and activities after the execution exists.
7. **Controllers are active-active.** All controllers read DSQL lease rows independently and can serve full routing snapshots. No leader election is required.
8. **Desired placement is separate from actual ownership.** The controller computes desired placement and sends directives. Runtimes attempt to acquire/relinquish leases in DSQL. The RoutingSnapshot advertises only DSQL-confirmed actual ownership.

### What already exists

- `ShardId`, `ShardEpoch`, `RunKey` types in `tokeira-types`
- `ShardOwner` struct with `Sweeping -> Active -> Draining` lifecycle in `tokeira-runtime/src/shard.rs`
- `shard_for(run_key, shard_count) -> ShardId` deterministic mapping
- `LeaseRepository` trait with `try_acquire_bundle` and `renew_bundle` in `tokeira-storage/src/api.rs`
- `DsqlRunRepository` implements `LeaseRepository` with epoch-fenced acquire/renew against DSQL
- `InMemoryStore` implements `LeaseRepository` with in-memory `bundle_leases` HashMap
- Shard-aware lane routing in `tokeira-runtime` (lanes route by `shard_for(run_key, shard_count)`)
- Internal proto directory at `proto/tokeira/internal/`

### What this spec does NOT cover

- Dynamic placement policy (when to move queue partitions independently of bundles) — separate spec per 037-dynamic-placement. This spec uses bundle-coincident queue-home only.
- ECS deployment specifics — separate `ecs-deployment` spec
- Autoscaler implementation — separate spec, but this spec provides the controller APIs the autoscaler needs
- Multi-DSQL-cluster placement — future, per 037


## Glossary

- **Placement_Controller**: Internal service that receives runtime heartbeats, observes DSQL lease state, computes desired bundle and queue-partition placement, and publishes routing snapshots. Runs as a small REPLICA ECS service. Multiple instances run active-active — no leader election required.
- **Runtime_Node**: A Tokeira runtime process that owns zero or more lease bundles, hosts lanes and run actors, and commits authoritative transitions to DSQL.
- **Edge_Node**: A Tokeira edge process that routes incoming API requests to the correct runtime node using a locally cached routing map.
- **Membership_Stream**: A long-lived bidirectional gRPC stream between a runtime node and the placement controller, carrying registration, heartbeat data (node identity, pressure metrics, drain state), and controller directives (placement directives, connection budgets, drain commands).
- **Bundle**: The authoritative ownership unit. A bundle groups many queue partitions and workflow executions under a single DSQL lease row. Bundles keep routing fine-grained while keeping the authoritative lease set small.
- **Bundle_Epoch**: Monotonically increasing fence token for bundle ownership. Owner changes bump the epoch; renewals keep the same epoch. Every mutating workflow commit validates the expected bundle_id and epoch.
- **Queue_Partition**: A placement unit derived from `hash(placement_key) mod N`. Queue partitions are the main unit of dynamic placement — finer-grained than bundles. A queue partition is always scoped to a queue family — the full key is `QueuePartitionKey { namespace_id, task_queue, task_kind, partition }`. For this spec (MVP), queue-home always coincides with bundle ownership via `bundle_for_partition`. Independent queue-partition placement is deferred to 037-dynamic-placement.
- **Queue_Family**: Identified by `(namespace_id, task_queue, task_kind)`. A queue family contains multiple queue partitions.
- **Queue_Home**: The placement preference for a queue family partition — determines where workers should poll and where workflow/activity tasks are dispatched. Queue-home is an optimisation for poll/task locality, not an execution correctness boundary. For this spec (MVP), queue-home always equals the bundle owner derived from `bundle_for_partition(partition, bundle_count)`. Independent queue-home placement is deferred to 037-dynamic-placement.
- **Execution_Home**: The authoritative home of a workflow execution, determined by `hash(namespace_id, workflow_id) -> bundle_id -> owner_node_id`. Used for Start, Signal, Update, Query, Cancel, and exact-run operations. Execution-home is the correctness boundary.
- **Routing_Snapshot**: A compact data structure published by the controller containing `execution_bundle_owners`, `node_endpoints`, `placement_config`, and a generation counter. Queue-home is derived on demand from `partition → bundle_for_partition → bundle owner` (not pre-materialized, since queue families are unbounded). Advertises only DSQL-confirmed actual ownership. Node endpoints are sourced from DSQL lease rows (the `node_endpoint` column on `shard_lease`), not from membership streams.
- **Incarnation_Id**: A UUID identifying a runtime node incarnation. Assigned at startup and stable only for the lifetime of the process. Previously called `NodeId` — the name clarifies it is not a durable node identity. The UUID text representation of `IncarnationId` is the canonical owner identity string used in lease APIs (`shard_lease.owner`). All lease owner strings MUST be parseable as `IncarnationId` UUIDs.
- **Generation_Counter**: A monotonically increasing counter on routing snapshots, persisted in DSQL with CAS protection. Edges and runtimes use it to detect stale snapshots.
- **NotShardOwner**: An error response returned by a runtime when it receives a request for a bundle it does not own. Contains optional `current_owner_node_id` hint and `current_epoch`. Triggers edge recovery (hint routing, controller refresh, or DSQL fallback).
- **Desired_Placement**: The controller's computed target for which runtimes should own which bundles. Communicated via `DesiredPlacementDirective` on the membership stream.
- **Actual_Ownership**: The DSQL-confirmed lease state. Only actual ownership is advertised in the RoutingSnapshot.
- **Drain_State**: A node lifecycle state indicating the node is being retired. The node stops acquiring new bundles and releases owned bundles.
- **Placement_Key**: The key used to derive a queue partition. Prefers an explicit application affinity key, then `workflow_id`, then a deterministic fallback.
- **Execution_Key**: The canonical key `(namespace_id, workflow_id)` used to derive execution-home bundle assignment. All operations on a workflow identity route through this key.
- **Node_Reachability**: Health assessment of a node separate from ownership: Healthy, Suspect, or Unavailable. Routing continues while a node is Suspect.
- **Placement_Config**: Configuration embedded in the RoutingSnapshot containing `shard_count`, `bundle_count`, `partition_count`, and `hash_version` for deterministic hash/mapping versioning.

## Requirements

---

## Feature 1: Active-Active Placement Controller

### Requirement 1.1: Active-Active Controller Design

**User Story:** As a Tokeira operator, I want the placement controller to run active-active without leader election, so that any controller instance can serve routing snapshots and the system tolerates controller failures without a leader failover window.

#### Acceptance Criteria

1. THE Placement_Controller SHALL run N instances active-active, where each instance independently reads DSQL lease rows to compute routing state.
2. ALL Placement_Controller instances SHALL be able to serve full routing snapshots, since the computation is deterministic from the same DSQL lease state.
3. THE Placement_Controller SHALL NOT use leader election or a control lease for computing or publishing placement.
4. WHEN mutating the generation counter, THE Placement_Controller SHALL use a DSQL CAS operation scoped to the singleton row (`UPDATE ... WHERE id = 1 AND generation = $expected RETURNING generation`) so that only one controller succeeds per increment.
5. THE Placement_Controller SHALL use a narrow CAS-protected budget allocation row in DSQL for connection budget distribution, not broad controller leadership.
6. WHEN multiple controllers attempt to advance the generation counter simultaneously, exactly one SHALL succeed and the others SHALL retry with the new base generation.

### Requirement 1.2: Controller gRPC Service

**User Story:** As a Tokeira developer, I want the placement controller to expose a gRPC service, so that runtime nodes and edge nodes can connect to it for membership and routing.

#### Acceptance Criteria

1. THE Placement_Controller SHALL expose a gRPC service defined in `proto/tokeira/internal/controller/`.
2. THE gRPC service SHALL include a bidirectional streaming RPC for runtime membership (`RuntimeMembership`).
3. THE gRPC service SHALL include a server-streaming RPC for routing snapshot subscription (`SubscribeRouting`).
4. THE gRPC service SHALL include a unary RPC for the autoscaler to request scale-in candidates (`NominateScaleInCandidates`).
5. THE gRPC service SHALL include a unary RPC for the autoscaler to mark nodes as draining (`MarkNodeDraining`).
6. THE gRPC service SHALL include a unary RPC for edge bundle refresh (`RefreshBundle`).

### Requirement 1.3: Controller Availability Model

**User Story:** As a Tokeira operator, I want the placement controller to be non-critical for workflow correctness, so that controller unavailability only pauses rebalance decisions.

#### Acceptance Criteria

1. WHILE the Placement_Controller is unavailable, THE Runtime_Node SHALL continue renewing bundle leases directly in DSQL.
2. WHILE the Placement_Controller is unavailable, THE Edge_Node SHALL continue using its cached Routing_Snapshot.
3. WHILE the Placement_Controller is unavailable, THE system SHALL continue processing workflow mutations and worker polls without error.
4. WHEN the Placement_Controller becomes available again, THE Placement_Controller SHALL rebuild live membership state from reconnecting runtime streams.

### Requirement 1.4: Desired vs Actual Placement

**User Story:** As a Tokeira developer, I want the controller to compute desired placement separately from actual ownership, so that the RoutingSnapshot only advertises DSQL-confirmed ownership and placement changes follow a safe convergence loop.

#### Acceptance Criteria

1. THE Placement_Controller SHALL compute desired placement and communicate it to runtimes via `DesiredPlacementDirective` messages on the membership stream.
2. THE Runtime_Node SHALL attempt to acquire or relinquish DSQL leases in response to placement directives.
3. THE Placement_Controller SHALL observe actual ownership by reading DSQL lease rows.
4. THE Routing_Snapshot SHALL advertise ONLY actual (DSQL-confirmed) ownership, never desired placement.
5. THE safe convergence loop SHALL be: desired placement -> directive -> runtime -> CAS/lease -> DSQL -> controller reads actual -> snapshot -> edge routes to actual owner.


---

## Feature 2: Runtime Membership Stream

### Requirement 2.1: Membership Stream Establishment with Explicit Registration

**User Story:** As a Tokeira developer, I want each runtime node to open a long-lived gRPC stream to the controller with an explicit registration message, so that the controller has live membership information including node identity and capabilities.

#### Acceptance Criteria

1. WHEN a Runtime_Node starts, THE Runtime_Node SHALL open a bidirectional gRPC stream to the Placement_Controller.
2. THE first message on the stream SHALL be a `RuntimeRegistration` containing the node's `Incarnation_Id`, host, port, zone, software version, and build identifier.
3. THE stream message type SHALL use a `oneof` discriminator between `RuntimeRegistration` and `RuntimeHeartbeat` — not empty-string sentinels for distinguishing message types.
4. IF the stream cannot be established, THEN THE Runtime_Node SHALL retry with exponential backoff.
5. WHEN the stream is established and registration is accepted, THE Runtime_Node SHALL send periodic `RuntimeHeartbeat` messages at a configurable interval (default: 5 seconds).

### Requirement 2.2: Heartbeat Content

**User Story:** As a Tokeira developer, I want heartbeat messages to carry operational pressure metrics, so that the controller can make informed placement decisions.

#### Acceptance Criteria

1. THE heartbeat message SHALL include the Runtime_Node's current owned bundle count and list of owned bundle IDs.
2. THE heartbeat message SHALL include queue pressure metrics: runnable transitions queued per lane, backlog depth.
3. THE heartbeat message SHALL include per-lane pressure metrics via a repeated `LanePressure` field containing: lane identifier, runnable depth, active actor count, and utilization ratio.
4. THE heartbeat message SHALL include DSQL connection-budget headroom: available connections, connection creation rate headroom.
5. THE heartbeat message SHALL include the Runtime_Node's current Drain_State (active, draining, or safe-to-terminate).

### Requirement 2.3: Stream Drop Handling

**User Story:** As a Tokeira developer, I want the controller to handle stream drops gracefully, so that transient network issues do not cause unnecessary placement churn.

#### Acceptance Criteria

1. WHEN a Membership_Stream drops, THE Placement_Controller SHALL start a configurable grace interval (default: 30 seconds) before considering the node unavailable.
2. WHILE the grace interval is active, THE Placement_Controller SHALL retain the node's last-known state in its live membership map.
3. WHEN the grace interval expires without reconnection, THE Placement_Controller SHALL mark the node as unavailable and exclude it from new placement decisions.
4. WHEN a Runtime_Node reconnects after a stream drop, THE Placement_Controller SHALL cancel the grace interval and restore the node to active membership.
5. THE Placement_Controller SHALL NOT revoke bundle ownership on stream drop — DSQL lease expiry is the authoritative ownership mechanism.

---

## Feature 3: Bundle Lease Management Extensions

### Requirement 3.1: Lease Observation for Controller

**User Story:** As a Tokeira developer, I want the controller to observe the current state of all bundle leases, so that it can compute accurate placement without being the lease authority.

#### Acceptance Criteria

1. THE LeaseRepository trait SHALL be extended with a `list_bundle_leases` method that returns all current bundle lease rows.
2. THE `list_bundle_leases` method SHALL return each lease's `bundle_id`, `owner_node_id` (nullable — `None` when unowned), `epoch`, `lease_until` timestamp, and `node_endpoint` (nullable).
3. THE DsqlRunRepository SHALL implement `list_bundle_leases` by querying `shard_lease` (columns: `shard_id UUID`, `owner TEXT NULL`, `epoch BIGINT`, `lease_expiry TIMESTAMPTZ`, `node_endpoint TEXT NULL`).
4. THE InMemoryStore SHALL implement `list_bundle_leases` by iterating its `bundle_leases` HashMap. The `bundle_leases` map SHALL use `Option<String>` for the owner field to represent unowned bundles.

### Requirement 3.2: Lease Relinquish

**User Story:** As a Tokeira developer, I want a runtime node to explicitly relinquish a bundle lease, so that drain and rebalance operations can transfer ownership without waiting for lease expiry.

#### Acceptance Criteria

1. THE LeaseRepository trait SHALL be extended with a `relinquish_bundle` method that releases ownership of a bundle.
2. WHEN `relinquish_bundle` is called, THE LeaseRepository SHALL set `owner_node_id` to `NULL` (clearing ownership) and advance the epoch, making the bundle available for acquisition by another node.
3. THE `relinquish_bundle` method SHALL validate that the caller's epoch matches the current epoch before releasing.
4. IF the caller's epoch is stale, THEN THE `relinquish_bundle` method SHALL return a rejection indicating the lease was already transferred.
5. THE `relinquish_bundle` SHALL use epoch-checked CAS to ensure safe concurrent relinquish.
6. THE `try_acquire_bundle` method SHALL treat a lease row with `owner IS NULL` as immediately acquirable regardless of `lease_expiry`. The DSQL acquire predicate SHALL include `owner IS NULL` alongside the existing `owner = $caller` and `lease_expiry <= now` conditions.
7. THE InMemoryStore `try_acquire_bundle` SHALL treat entries with `None` owner as acquirable (advance epoch, set new owner). Acquisition MAY still fail if the caller's epoch is stale or the bundle is in a transitional state unrelated to owner presence.

### Requirement 3.2a: Lease Endpoint Write Path

**User Story:** As a Tokeira developer, I want the runtime to write its network endpoint into the lease row when acquiring a bundle, so that controllers can source node endpoints from DSQL lease rows for active-active snapshot computation.

#### Acceptance Criteria

1. THE `try_acquire_bundle` method SHALL accept a `node_endpoint: String` parameter (formatted as `host:port`) and write it to the `node_endpoint` column on successful acquisition.
2. THE `renew_bundle` method SHALL accept a `node_endpoint: String` parameter and update the `node_endpoint` column on successful renewal, so that endpoint changes (e.g., after a restart with a new port) are propagated.
3. THE `node_endpoint` column SHALL be updated atomically with the lease acquisition or renewal — not via a separate API call.
4. THE InMemoryStore SHALL store `node_endpoint` alongside owner and epoch in the `bundle_leases` map.

### Requirement 3.3: Bundle-to-Queue-Partition Mapping

**User Story:** As a Tokeira developer, I want a deterministic mapping from queue partitions to bundles, so that the controller can compute which bundle owns which queue partitions.

#### Acceptance Criteria

1. THE system SHALL define a deterministic function `bundle_for_partition(queue_partition, bundle_count) -> BundleId` that maps queue partitions to bundles.
2. FOR ALL valid queue partition values, `bundle_for_partition` SHALL produce a BundleId in the range `[0, bundle_count)`.
3. THE mapping SHALL distribute queue partitions approximately evenly across bundles.
4. THE mapping SHALL be stable: the same `(queue_partition, bundle_count)` always produces the same `BundleId`.

---

## Feature 4: Routing Snapshot Distribution

### Requirement 4.1: Routing Snapshot Computation

**User Story:** As a Tokeira developer, I want the controller to compute a routing snapshot from DSQL lease state and live membership, so that edges and runtimes can route requests without querying DSQL.

#### Acceptance Criteria

1. THE Placement_Controller SHALL compute a Routing_Snapshot containing `execution_bundle_owners: HashMap<BundleId, BundleOwner>` for all owned bundles, where `BundleOwner` includes `node_id` and `epoch`.
2. THE Routing_Snapshot SHALL NOT contain a pre-materialized `queue_partition_homes` map for all queue families, since task queues and namespaces are unbounded. Instead, queue-home resolution SHALL be computed on demand: given a `QueuePartitionKey`, derive `partition -> bundle_for_partition(partition, bundle_count) -> bundle_id`, then look up `bundle_id -> BundleOwner` in `execution_bundle_owners`. The `QueuePartitionHome` is the bundle's owner.
3. THE Routing_Snapshot SHALL contain a monotonically increasing Generation_Counter persisted in DSQL with CAS protection, so that any controller instance after restart starts from the last published generation.
4. THE Routing_Snapshot SHALL contain a `node_endpoints: HashMap<Incarnation_Id, NodeEndpoint>` map so that edges can resolve node IDs to concrete network addresses. Node endpoints SHALL be sourced from the `node_endpoint` column on `shard_lease` rows in DSQL, not from membership streams. Membership streams carry pressure metrics only.
5. THE Routing_Snapshot SHALL contain a `PlacementConfig` with `shard_count`, `bundle_count`, `partition_count`, and `hash_version` for deterministic hash/mapping versioning.
6. WHEN bundle ownership changes (new acquisition, relinquish, or lease expiry), OR WHEN routing-relevant configuration changes (placement config update, node endpoint change), THE Placement_Controller SHALL recompute and publish an updated Routing_Snapshot.
7. THE bundle routing entries SHALL include the lease epoch so that edges can include `observed_bundle_epoch` in requests and runtimes can fast-fail on epoch mismatch before attempting a DSQL transaction.

### Requirement 4.2: Routing Snapshot Subscription

**User Story:** As a Tokeira developer, I want edges and runtimes to subscribe to routing snapshot updates, so that they receive placement changes without polling.

#### Acceptance Criteria

1. THE Placement_Controller SHALL publish Routing_Snapshot updates to all connected subscribers via the `SubscribeRouting` server-streaming RPC.
2. WHEN a subscriber connects, THE Placement_Controller SHALL send the current full Routing_Snapshot as the first message.
3. THE Placement_Controller SHALL send incremental updates (deltas) for subsequent changes to minimize bandwidth.
4. THE subscriber SHALL apply incremental updates to its local cached snapshot, advancing the Generation_Counter. THE `apply_delta` method SHALL return `Result` and reject if `delta.base_generation != local.generation`.
5. IF a subscriber detects a gap in Generation_Counter, THEN THE subscriber SHALL request a full snapshot resync.

### Requirement 4.3: CAS-Based Generation Counter

**User Story:** As a Tokeira developer, I want the generation counter to be persisted in DSQL with CAS protection, so that multiple active-active controllers produce monotonically ordered snapshots.

#### Acceptance Criteria

1. THE generation counter SHALL be persisted in a dedicated singleton DSQL row with CAS protection: `UPDATE routing_generation SET generation = generation + 1 WHERE id = 1 AND generation = $expected_generation RETURNING generation`.
2. THE RoutingDelta SHALL carry `base_generation` and `generation` fields so that subscribers can validate delta ordering.
3. IF a CAS increment fails (another controller advanced the generation), THE controller SHALL re-read the current generation and retry.

### Requirement 4.3a: Generation and Budget Storage Surface

**User Story:** As a Tokeira developer, I want DSQL migration files and a repository API for the `routing_generation` and `budget_allocation` tables, so that the generation counter and budget allocation have a concrete storage implementation.

#### Acceptance Criteria

1. THE `routing_generation` table SHALL be created by a dedicated migration file (one SQL statement per file per DSQL convention).
2. THE `budget_allocation` table SHALL be created by a dedicated migration file. Each table SHALL have a separate seed migration file that inserts the singleton row.
3. THE `LeaseRepository` trait (or a new `ControlRepository` trait) SHALL expose `advance_generation(expected: GenerationCounter) -> Result<GenerationAdvanceResult>` and `current_generation() -> Result<GenerationCounter>` methods.
4. THE `LeaseRepository` trait (or `ControlRepository`) SHALL expose `allocate_budget(expected_version: u64, allocator_id: Uuid, rate_budget: f64, capacity_budget: u64) -> Result<BudgetAllocationResult>` with CAS protection on a `version` column. Competing controllers race on the version — exactly one succeeds per allocation cycle.
5. THE `InMemoryStore` SHALL implement these methods for testing.
6. THE `DsqlRunRepository` SHALL implement these methods against the DSQL tables.

### Requirement 4.4: Routing Snapshot Serialization

**User Story:** As a Tokeira developer, I want routing snapshots to be compact and efficiently serializable, so that snapshot distribution does not become a bottleneck.

#### Acceptance Criteria

1. THE Routing_Snapshot SHALL be serializable using protobuf for wire transmission.
2. THE Routing_Snapshot SHALL support incremental delta encoding: only changed entries are transmitted after the initial full snapshot.
3. THE Routing_Snapshot wire format SHALL be defined in `proto/tokeira/internal/controller/`.
4. THE protobuf messages SHALL use `oneof` for ownership state instead of empty-string sentinels.

---

## Feature 5: Edge Routing Cache

### Requirement 5.1: Local Routing Cache

**User Story:** As a Tokeira developer, I want each edge node to maintain a local routing cache, so that request routing does not require any external lookup on the hot path.

#### Acceptance Criteria

1. THE Edge_Node SHALL maintain an in-memory Routing_Snapshot cache populated from the controller's `SubscribeRouting` stream, using `ArcSwap<RoutingSnapshot>` for lock-free reads.
2. WHEN routing a poll request, THE Edge_Node SHALL look up the queue partition's home and route to the owning runtime node.
3. WHEN routing a start request, THE Edge_Node SHALL derive `execution_key = hash(namespace_id, workflow_id) -> bundle_id` and route to the execution-home bundle's owner. Queue partition is used ONLY for dispatching workflow tasks/activities AFTER the execution exists.
4. WHEN routing a signal, update, query, cancel, or exact-run request, THE Edge_Node SHALL derive `execution_key = hash(namespace_id, workflow_id) -> bundle_id` and route to the execution-home bundle's owner.
5. THE routing cache lookup SHALL be an in-memory operation with no network calls, no DSQL reads, and no controller RPCs.
6. THE Edge_Node SHALL include `observed_bundle_epoch` from the cached routing entry in requests to the runtime, enabling fast-fail on epoch mismatch.

### Requirement 5.2: NotShardOwner Recovery with Redirect Hints

**User Story:** As a Tokeira developer, I want the edge to recover from stale routing using redirect hints, controller refresh, or DSQL fallback, so that ownership changes are transparent to API callers.

#### Acceptance Criteria

1. WHEN a Runtime_Node receives a request for a bundle it does not own, THE Runtime_Node SHALL reject the request with a `NotShardOwner` error containing the `bundle_id`, `current_epoch`, and an optional `current_owner_node_id` hint.
2. WHEN the Edge_Node receives a `NotShardOwner` error with a `current_owner_node_id` hint, THE Edge_Node SHALL first attempt to route directly to the suggested owner using `make_request` with the hinted endpoint and epoch, bypassing the stale cache entry.
3. IF the hint is absent or the hinted owner also rejects, THE Edge_Node SHALL perform a `RefreshBundle(bundle_id)` unary call to the controller.
4. IF the controller is unavailable, THE Edge_Node SHALL fall back to a DSQL lease lookup for the bundle.
5. THE Edge_Node SHALL limit NotShardOwner retries to a configurable maximum (default: 3) to prevent infinite retry loops.
6. IF the maximum retry count is exceeded, THEN THE Edge_Node SHALL return an error to the API caller indicating a routing failure.
7. Edge retry SHALL be safe because `request_dedupe` is persisted atomically with workflow state mutation — this invariant SHALL be explicitly documented.

### Requirement 5.3: Cache Staleness Handling

**User Story:** As a Tokeira developer, I want the edge routing cache to handle controller disconnection gracefully, so that temporary controller unavailability does not break request routing.

#### Acceptance Criteria

1. WHILE the controller subscription is disconnected, THE Edge_Node SHALL continue routing using its last-known Routing_Snapshot.
2. WHEN the controller subscription reconnects, THE Edge_Node SHALL receive a full snapshot and replace its cache.
3. THE Edge_Node SHALL log a warning when the cached Routing_Snapshot age exceeds a configurable staleness threshold (default: 60 seconds).
4. THE Edge_Node SHALL NOT reject requests solely because the routing cache is stale — stale routing is recoverable via NotShardOwner.


---

## Feature 6: Queue-Home and Execution-Home Resolution

### Requirement 6.1: Queue Partition Derivation

**User Story:** As a Tokeira developer, I want a deterministic function to derive queue partitions from placement keys, so that related work is consistently routed to the same partition.

#### Acceptance Criteria

1. THE system SHALL define a function `queue_partition_for(placement_key, partition_count) -> QueuePartition` that deterministically maps placement keys to partitions.
2. THE Placement_Key SHALL prefer: (a) an explicit application affinity key if provided, (b) otherwise the `workflow_id`, (c) otherwise a deterministic fallback.
3. FOR ALL valid placement keys, `queue_partition_for` SHALL produce a QueuePartition in the range `[0, partition_count)`.
4. THE mapping SHALL distribute placement keys approximately uniformly across partitions.

### Requirement 6.2: Queue-Home Resolution

**User Story:** As a Tokeira developer, I want queue-home resolution to determine where workers should poll and where workflow/activity tasks are dispatched, so that delivery locality is maximized.

#### Acceptance Criteria

1. THE Edge_Node SHALL resolve queue-home for a poll request by constructing a `QueuePartitionKey { namespace_id, task_queue, task_kind, queue_partition }` and calling `resolve_queue_home`, which derives the bundle on demand via `bundle_for_partition(partition, bundle_count)` and looks up the bundle owner in `execution_bundle_owners`.
2. THE Edge_Node SHALL resolve queue-home for task dispatch by deriving the queue partition from the Placement_Key and looking up the derived bundle owner.
3. WHEN the derived bundle has no current owner or no endpoint in the Routing_Snapshot, THE Edge_Node SHALL fall back to a default routing strategy (round-robin across available runtime nodes). This fallback is a transient availability path, not an independent queue-home preference.

### Requirement 6.3: Execution-Home Resolution

**User Story:** As a Tokeira developer, I want execution-home resolution to route Start, Signal, Update, Query, and Cancel to the authoritative owner, so that all workflow mutations for the same identity reach the correct runtime node.

#### Acceptance Criteria

1. THE Edge_Node SHALL resolve execution-home for ALL workflow operations (Start, Signal, Update, Query, Cancel) by computing `execution_key = hash(namespace_id, workflow_id) -> bundle_id` and looking up `bundle_id -> BundleOwner` in the Routing_Snapshot's `execution_bundle_owners` map.
2. Start and Signal for the same `(namespace_id, workflow_id)` SHALL resolve to the same execution-home bundle. This is a correctness requirement — the Start flow MUST NOT route by queue partition for execution-home.
3. WHEN the execution-home lookup returns no owner (bundle is unowned), THE Edge_Node SHALL return a transient error indicating the execution is temporarily unavailable.
4. THE execution-home resolution SHALL NOT require a DSQL read on the hot path.
5. THE runtime's `commit_transition` SHALL accept the execution-home `BundleId` from the edge request and fence against that bundle's epoch, rather than deriving the bundle from `run_key` (which includes `run_id` and produces a different hash). This ensures the commit fence matches the routing path.
6. FOR internal commits not directly edge-routed (workflow task completions, activity completions, timer firings, scanner-driven transitions, and recovery replays), THE runtime SHALL derive the execution-home `BundleId` from the loaded run's `(namespace_id, workflow_id)` using `execution_home_bundle`. The `ShardOwner` already knows which bundles it owns, so the runtime can validate the derived bundle matches a locally-owned bundle before committing.

---

## Feature 7: Placement Types

### Requirement 7.1: Core Placement Type Definitions

**User Story:** As a Tokeira developer, I want well-defined placement types in `tokeira-types`, so that all crates share a consistent vocabulary for placement concepts.

#### Acceptance Criteria

1. THE `tokeira-types` crate SHALL define an `IncarnationId` type as a UUID wrapper identifying a runtime node incarnation, stable only for the lifetime of the process.
2. THE `tokeira-types` crate SHALL define a `BundleId` type as a u32 wrapper identifying a lease bundle (aliased from existing `ShardId` or replacing it).
3. THE `tokeira-types` crate SHALL define a `QueuePartition` type as a u32 wrapper identifying a queue partition.
4. THE `tokeira-types` crate SHALL define a `GenerationCounter` type as a u64 wrapper for routing snapshot generations.
5. ALL placement types SHALL derive `Debug`, `Clone`, `Copy`, `PartialEq`, `Eq`, `Hash`, `Serialize`, `Deserialize`.
6. THE `tokeira-types` crate SHALL define a `NodeReachability` enum with variants `Healthy`, `Suspect`, `Unavailable`, separate from ownership state. Routing SHALL continue while a node is `Suspect`.
7. THE `tokeira-types` crate SHALL note `blake3` as a new dependency for deterministic hashing.
8. THE lease owner identity string in `shard_lease.owner` SHALL be the UUID text representation of `IncarnationId`. Runtime nodes SHALL pass `incarnation_id.to_string()` as the owner when calling `try_acquire_bundle` and `renew_bundle`. Snapshot computation and DSQL fallback SHALL parse the owner string as `IncarnationId` and skip (with a warning) any lease row with a malformed owner.

### Requirement 7.2: Routing Snapshot Type

**User Story:** As a Tokeira developer, I want a `RoutingSnapshot` type that can be cached and queried efficiently, so that edge and runtime routing lookups are fast.

#### Acceptance Criteria

1. THE `RoutingSnapshot` type SHALL contain `execution_bundle_owners: HashMap<BundleId, BundleOwner>` where `BundleOwner` includes `node_id: IncarnationId` and `epoch: ShardEpoch`.
2. THE `RoutingSnapshot` type SHALL provide a `resolve_queue_home(key: &QueuePartitionKey) -> Option<&BundleOwner>` method that derives the bundle on demand via `bundle_for_partition(key.partition, placement_config.bundle_count)` and looks up the bundle owner in `execution_bundle_owners`. Queue-home is NOT a separate pre-materialized map because queue families are unbounded.
3. THE `RoutingSnapshot` type SHALL contain `node_endpoints: HashMap<IncarnationId, NodeEndpoint>` mapping node IDs to network endpoints.
4. THE `RoutingSnapshot` type SHALL contain a `PlacementConfig` with `shard_count: u32`, `bundle_count: u32`, `partition_count: u32`, `hash_version: u32`.
5. THE `RoutingSnapshot` type SHALL contain a `GenerationCounter` for staleness detection.
6. THE `RoutingSnapshot` type SHALL provide `lookup_bundle_owner(bundle_id) -> Option<&BundleOwner>` and `lookup_node_endpoint(node_id) -> Option<&NodeEndpoint>` methods.
7. THE `RoutingSnapshot` type SHALL support applying incremental deltas with `base_generation` validation. `apply_delta` SHALL return `Result` and reject if `delta.base_generation != local.generation`.

---

## Feature 8: Node Drain Protocol

### Requirement 8.1: Controller-Initiated Drain

**User Story:** As a Tokeira developer, I want the controller to mark a node as draining, so that the autoscaler can safely retire runtime nodes without corrupting workflow state.

#### Acceptance Criteria

1. WHEN the Placement_Controller receives a `MarkNodeDraining` request for a node, THE Placement_Controller SHALL update the node's state to `DRAINING` in its live membership map.
2. THE Placement_Controller SHALL stop assigning new bundles to a node in `DRAINING` state.
3. THE Placement_Controller SHALL NOT assign separate queue-home preferences in this MVP; queue-home drain behavior follows bundle reassignment because queue-home is derived from bundle ownership.
4. THE Placement_Controller SHALL notify the Runtime_Node of its drain state via the Membership_Stream.

### Requirement 8.2: Two-Phase Drain Protocol

**User Story:** As a Tokeira developer, I want a two-phase drain protocol that moves routing before ownership and confirms ownership before advertising new owners, so that drain is safe and complete.

#### Acceptance Criteria

1. WHEN a Runtime_Node receives a drain notification, THE Runtime_Node SHALL be marked DRAINING in membership.
2. THE Placement_Controller SHALL publish a routing snapshot that stops sending new queue work to the draining node.
3. THE draining Runtime_Node SHALL stop acquiring new bundles.
4. THE draining Runtime_Node SHALL stop accepting new externally-routed work except for owned in-flight execution work.
5. THE draining Runtime_Node SHALL complete or reject in-flight transitions.
6. THE draining Runtime_Node SHALL relinquish bundle leases using epoch-checked CAS.
7. THE Placement_Controller SHALL observe actual DSQL ownership moved/expired before advertising new owners in the routing snapshot.
8. THE Runtime_Node SHALL report `SAFE_TO_TERMINATE` only when: `owned_bundle_count == 0`, `inflight_transition_count == 0`, and `pending_wft_replies == 0`.
9. Routing MUST move before ownership is relinquished, but routing MUST NOT advertise the new owner until DSQL confirms the new lease.

### Requirement 8.3: Scale-In Candidate Nomination

**User Story:** As a Tokeira developer, I want the controller to nominate safe scale-in candidates, so that the autoscaler can choose which nodes to retire.

#### Acceptance Criteria

1. WHEN the Placement_Controller receives a `NominateScaleInCandidates` request, THE Placement_Controller SHALL return a ranked list of nodes suitable for retirement.
2. THE ranking SHALL prefer nodes with the lowest owned-bundle count.
3. THE ranking SHALL prefer nodes with the lowest runnable-lane pressure.
4. THE ranking SHALL exclude nodes that are already draining.
5. THE ranking SHALL exclude nodes that are performing active sweep or repair operations.

---

## Feature 9: Connection Budget Allocation

### Requirement 9.1: Controller-Coordinated Connection Budget with Expiry

**User Story:** As a Tokeira developer, I want the controller to distribute DSQL connection budget shares to runtime nodes with expiry, so that multi-node deployments respect the cluster-wide connection rate limit (100/sec sustained) and connection limit (10,000 default), and runtimes degrade safely when directives expire.

#### Acceptance Criteria

1. THE Placement_Controller SHALL compute a per-node connection rate share as `cluster_connection_rate / active_node_count` and a per-node connection capacity share as `cluster_connection_budget / active_node_count`.
2. WHEN a Runtime_Node joins or leaves the active membership, THE Placement_Controller SHALL recompute and redistribute connection budget shares to all active nodes.
3. THE Placement_Controller SHALL send connection budget shares to each Runtime_Node via a `ConnectionBudgetDirective` on the Membership_Stream.
4. THE `ConnectionBudgetDirective` SHALL carry `rate_per_second: f64`, `capacity: u64`, `max_reservoir_size: u32`, and `valid_until: Timestamp` fields.
5. WHEN the `valid_until` timestamp expires without a new directive, THE Runtime_Node SHALL degrade to a conservative default budget.
6. WHEN there are zero active nodes, THE Placement_Controller SHALL handle the edge case without division by zero.
7. WHEN distributing remainder capacity, THE Placement_Controller SHALL distribute deterministically by sorted Incarnation_Id.

### Requirement 9.2: Runtime Connection Budget Application

**User Story:** As a Tokeira developer, I want each runtime node to apply the controller-assigned connection budget to its local rate limiter and reservoir, so that the cluster-wide limits are respected across all nodes.

#### Acceptance Criteria

1. WHEN a Runtime_Node receives a `ConnectionBudgetDirective`, THE Runtime_Node SHALL call `TokenBucketRateLimiter::reconfigure(rate_per_second, capacity)` on its local rate limiter.
2. WHEN a Runtime_Node receives a `ConnectionBudgetDirective`, THE Runtime_Node SHALL cap its reservoir `target_ready` to `min(configured_target_ready, max_reservoir_size)` via a `reconfigure_target(new_target: u32)` method on the reservoir.
3. IF the current reservoir size exceeds the new cap, THE Runtime_Node SHALL proactively retire excess connections via a `retire_excess(target: u32)` method on the reservoir.
4. WHEN the Membership_Stream is disconnected, THE Runtime_Node SHALL retain its last-known connection budget share until the `valid_until` expiry, then degrade to conservative defaults.
5. WHEN a Runtime_Node starts without a controller connection, THE Runtime_Node SHALL use a conservative default share (e.g., `cluster_rate / expected_max_nodes`) until the controller assigns a share.

### Requirement 9.3: Connection Budget Configuration

**User Story:** As a Tokeira operator, I want the cluster-wide DSQL connection budget configurable in the controller config, so that the budget can be tuned for different DSQL cluster sizes.

#### Acceptance Criteria

1. THE ControllerConfig SHALL include `dsql_connection_rate_budget` (default: 100, matching DSQL's sustained rate) and `dsql_connection_capacity_budget` (default: 10,000, matching DSQL's default connection limit).
2. THE Placement_Controller SHALL use these values when computing per-node shares.

### Requirement 9.4: Controller Advisory Connection Headroom

**User Story:** As a Tokeira developer, I want the controller to report aggregate connection headroom to the autoscaler, so that the scaling envelope can use accurate cluster-wide connection data.

#### Acceptance Criteria

1. THE Placement_Controller SHALL aggregate `available_connections` and `connection_rate_headroom` from runtime heartbeats.
2. THE Placement_Controller SHALL expose aggregate connection headroom via the `NominateScaleInCandidates` response or a dedicated query, so the autoscaler can compute the connection-aware scaling envelope.
