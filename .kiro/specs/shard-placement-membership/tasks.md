# Implementation Plan: Shard Placement and Membership

## Overview

Implement the shard placement and membership system for multi-node Tokeira deployments. The work is organized into 5 phases: placement types, storage extensions, controller service, runtime membership client, and edge routing cache. Each phase builds on the previous one. The controller is a new `tokeira-controller` crate; all other changes extend existing crates.

Key changes from the previous plan:
- Active-active controllers replace leader election (no control lease)
- Execution-home routing for ALL operations including Start (correctness fix)
- Desired-vs-actual placement with DesiredPlacementDirective
- Lease epoch in all bundle routing entries (BundleOwner with epoch)
- Separate execution_bundle_owners and queue_partition_homes maps in RoutingSnapshot
- CAS-based generation counter with base_generation validation
- Explicit RuntimeRegistration message (oneof, not empty-string sentinels)
- ConnectionBudgetDirective with valid_until expiry
- NotShardOwner with redirect hints and multi-path recovery
- Two-phase drain protocol
- ArcSwap for edge routing cache
- IncarnationId (renamed from NodeId) to clarify process-lifetime scope
- NodeReachability (Healthy/Suspect/Unavailable) separate from ownership
- PlacementConfig in RoutingSnapshot for hash/mapping versioning
- blake3 as new dependency for tokeira-types
- oneof in proto instead of empty-string sentinels

## Tasks

- [ ] 1. Phase 1 — Placement Types and Deterministic Mapping Functions
  - [ ] 1.1 Add `IncarnationId` type to `tokeira-types/src/ids.rs`
    - Define `IncarnationId(pub Uuid)` with `new()` constructor generating UUIDv4
    - Derive `Debug`, `Clone`, `Copy`, `PartialEq`, `Eq`, `Hash`, `Serialize`, `Deserialize`
    - Implement `Display` and `Default` (default generates new UUID)
    - Doc comment: "Identifies a runtime node incarnation. Stable only for the process lifetime."
    - _Requirements: 7.1.1, 7.1.5_

  - [ ] 1.2 Add `QueuePartition`, `GenerationCounter`, `NodeReachability`, and `PlacementConfig` types
    - Define `QueuePartition(pub u32)` with derives matching `ShardId`
    - Define `GenerationCounter(pub u64)` with `ZERO` constant and `next()` method
    - Add `BundleId` as a type alias for `ShardId`
    - Define `NodeReachability` enum with `Healthy`, `Suspect`, `Unavailable` variants — separate from ownership state
    - Define `PlacementConfig { shard_count: u32, bundle_count: u32, partition_count: u32, hash_version: u32 }`
    - Derive `Debug`, `Clone`, `Copy`, `PartialEq`, `Eq`, `Hash`, `Serialize`, `Deserialize` on all types
    - _Requirements: 7.1.2, 7.1.3, 7.1.4, 7.1.5, 7.1.6_

  - [ ] 1.3 Implement deterministic mapping functions in `tokeira-types`
    - Add `queue_partition_for(placement_key: &[u8], partition_count: u32) -> QueuePartition` using blake3 hash
    - Add `bundle_for_partition(partition: QueuePartition, bundle_count: u32) -> BundleId` using modular mapping
    - Add `execution_home_bundle(namespace_id: &[u8], workflow_id: &[u8], bundle_count: u32) -> BundleId` using blake3 hash of canonical execution key
    - All functions panic on zero count (matching `shard_for` convention)
    - Add `blake3` as a dependency of `tokeira-types` (note: new dependency)
    - _Requirements: 3.3.1, 3.3.2, 3.3.3, 3.3.4, 6.1.1, 6.1.2, 6.1.3, 6.1.4, 6.3.1, 6.3.2_

  - [ ] 1.4 Implement `RoutingSnapshot`, `RoutingDelta`, and supporting types
    - Create `tokeira-types/src/routing.rs` module
    - Define `NodeEndpoint { host: String, port: u16 }` with standard derives
    - Define `BundleOwner { node_id: IncarnationId, epoch: ShardEpoch }` — includes epoch for fencing
    - Define `QueuePartitionHome { node_id: IncarnationId, bundle_id: BundleId }`
    - Define `RoutingSnapshot` with separate maps: `execution_bundle_owners: HashMap<BundleId, BundleOwner>`, `queue_partition_homes: HashMap<QueuePartition, QueuePartitionHome>`, `node_endpoints: HashMap<IncarnationId, NodeEndpoint>`, `placement_config: PlacementConfig`, `generation: GenerationCounter`
    - Implement `lookup_bundle_owner`, `lookup_node_endpoint`, `lookup_queue_partition_home` methods
    - Define `RoutingDelta` with `base_generation`, bundle updates (with `Option<BundleOwner>`), queue partition updates, node updates, and `generation`
    - Implement `apply_delta` returning `Result<(), RoutingDeltaError>` — rejects if `delta.base_generation != local.generation`
    - Define `RoutingDeltaError` with `thiserror`
    - _Requirements: 7.2.1, 7.2.2, 7.2.3, 7.2.4, 7.2.5, 7.2.6, 7.2.7, 4.1.7, 4.2.4, 4.3.2_

  - [ ]* 1.5 Write property tests for mapping functions
    - **Property 1: Queue partition determinism** — same key + count -> same result, result in range
    - **Property 2: Bundle-for-partition determinism** — same partition + count -> same result, result in range
    - **Property 6: Queue partition uniform distribution** — chi-squared test over 10,000 random keys with deterministic seed
    - **Property 7: Execution-home consistency** — execution_home_bundle produces same result for same (namespace_id, workflow_id) regardless of call context; Start and Signal resolve to same bundle
    - Use `proptest` crate, minimum 100 iterations per property
    - _Validates: Requirements 3.3, 6.1, 6.3_

  - [ ]* 1.6 Write property tests for routing snapshot
    - **Property 3: Routing snapshot delta round-trip with base_generation** — apply delta with matching base_generation succeeds; mismatched base_generation returns Err
    - **Property 4: Generation monotonicity** — generation always increases after successful delta application
    - Use `proptest` crate, minimum 100 iterations per property
    - _Validates: Requirements 4.1.3, 4.2.4, 4.3.2, 7.2.7_

  - [ ]* 1.7 Write unit tests for placement types
    - Test `IncarnationId::new()` generates unique IDs
    - Test `QueuePartition` and `GenerationCounter` basic operations
    - Test `BundleId` alias works with existing `ShardId` functions
    - Test `NodeReachability` enum variants
    - Test `PlacementConfig` construction and equality
    - Test `BundleOwner` includes epoch correctly
    - Test `RoutingSnapshot` with separate `execution_bundle_owners` and `queue_partition_homes` maps
    - Test `RoutingSnapshot::apply_delta` adds, updates, and removes entries correctly
    - Test `RoutingSnapshot::apply_delta` rejects mismatched base_generation
    - _Requirements: 7.1, 7.2_

- [ ] 2. Checkpoint — Ensure all Phase 1 tests pass
  - Run `cargo test --workspace` and verify all new and existing tests pass.


- [ ] 3. Phase 2 — LeaseRepository Extensions
  - [ ] 3.1 Add `BundleLease` type and extend `LeaseRepository` trait
    - Define `BundleLease { bundle_id: ShardId, owner_node_id: String, epoch: ShardEpoch, lease_until: OffsetDateTime }` in `tokeira-storage/src/api.rs`
    - Add `list_bundle_leases(&self) -> Result<Vec<BundleLease>>` to `LeaseRepository` trait
    - Add `relinquish_bundle(&self, bundle: ShardId, owner: String, epoch: ShardEpoch) -> Result<LeaseOutcome>` to `LeaseRepository` trait
    - Add blanket impl for `Arc<T>` delegation
    - _Requirements: 3.1.1, 3.1.2, 3.2.1, 3.2.2, 3.2.3, 3.2.4, 3.2.5_

  - [ ] 3.2 Implement `list_bundle_leases` and `relinquish_bundle` in `InMemoryStore`
    - `list_bundle_leases`: iterate `bundle_leases` HashMap, return all entries as `BundleLease`
    - `relinquish_bundle`: validate epoch matches (epoch-checked CAS), clear owner, advance epoch, return `LeaseOutcome::Acquired` with new epoch (or `Rejected` on mismatch)
    - _Requirements: 3.1.4, 3.2.1, 3.2.2, 3.2.3, 3.2.4, 3.2.5_

  - [ ] 3.3 Implement `list_bundle_leases` and `relinquish_bundle` in `DsqlRunRepository`
    - `list_bundle_leases`: `SELECT bundle_id, owner_node_id, epoch, lease_until FROM control.bundle_lease`
    - `relinquish_bundle`: validate epoch with CAS, clear owner_node_id, increment epoch in a single DSQL transaction
    - Use `DbClass::Control` for connection acquisition
    - _Requirements: 3.1.3, 3.2.1, 3.2.2, 3.2.3, 3.2.4, 3.2.5_

  - [ ] 3.4 Update `HistoryNotifyingRepository` wrapper in `tokeira-edge`
    - Delegate `list_bundle_leases` and `relinquish_bundle` to the inner repository
    - _Requirements: 3.1, 3.2_

  - [ ]* 3.5 Write unit tests for LeaseRepository extensions
    - Test `list_bundle_leases` returns all leases after multiple acquires
    - Test `relinquish_bundle` clears owner and advances epoch
    - Test `relinquish_bundle` rejects stale epoch (epoch-checked CAS)
    - Test `relinquish_bundle` makes bundle available for re-acquisition
    - **Property 5: Bundle ownership consistency** — generate random lease sets, verify snapshot mapping with epoch
    - _Validates: Requirements 3.1, 3.2, 4.1_

- [ ] 4. Checkpoint — Ensure all Phase 2 tests pass
  - Run `cargo test --workspace` and verify all new and existing tests pass.

- [ ] 5. Phase 3 — Controller gRPC Service and Active-Active Placement Logic
  - [ ] 5.1 Define controller proto in `proto/tokeira/internal/controller/controller.proto`
    - Define `PlacementController` service with 5 RPCs: `RuntimeMembership`, `SubscribeRouting`, `RefreshBundle`, `NominateScaleInCandidates`, `MarkNodeDraining`
    - Define `RuntimeMembershipRequest` with `oneof request { RuntimeRegistration, RuntimeHeartbeat }` — not empty-string sentinels
    - Define `RuntimeRegistration` with node_id, host, port, zone, version, build_id
    - Define `RuntimeHeartbeat` with pressure metrics and drain state
    - Define `ControllerDirective` with `oneof directive { DrainDirective, RoutingUpdate, ConnectionBudgetDirective, DesiredPlacementDirective }`
    - Define `ConnectionBudgetDirective` with rate_per_second, capacity, max_reservoir_size, valid_until (Timestamp)
    - Define `DesiredPlacementDirective` with acquire_bundles and relinquish_bundles
    - Define `RoutingUpdate` with `oneof { FullRoutingSnapshot, RoutingDeltaMessage }`
    - Define `FullRoutingSnapshot` with BundleOwnershipEntry (including epoch), QueuePartitionHomeEntry, NodeEndpointEntry, PlacementConfigMessage, generation
    - Define `RoutingDeltaMessage` with base_generation field
    - Use `oneof` for ownership state in BundleOwnershipEntry, QueuePartitionHomeEntry, NodeEndpointEntry — not empty-string sentinels
    - Define `RefreshBundleRequest/Response` for edge bundle refresh
    - Define all remaining message types: NominateRequest, NominateResponse, ScaleInCandidate, MarkDrainingRequest, MarkDrainingResponse
    - Define `NodeDrainState` enum
    - Update `buf.yaml` and `buf.gen.yaml` if needed for the new proto path
    - _Requirements: 1.2.1, 1.2.2, 1.2.3, 1.2.4, 1.2.5, 1.2.6, 2.1.2, 2.1.3, 4.3.1, 4.3.2, 4.4.1, 4.4.2, 4.4.3, 4.4.4_

  - [ ] 5.2 Create `tokeira-controller` crate scaffold
    - Add `crates/tokeira-controller/` with `Cargo.toml`, `src/lib.rs`
    - Add to workspace `Cargo.toml` members
    - Dependencies: `tonic`, `tokio`, `tracing`, `anyhow`, `thiserror`, `tokeira-types`, `tokeira-storage`, `tokeira-proto`, `time`, `uuid`
    - Create module structure: `generation.rs`, `membership.rs`, `placement.rs`, `service.rs`, `drain.rs`, `config.rs`
    - _Requirements: 1.1, 1.2, 1.3_

  - [ ] 5.3 Implement CAS-based generation counter in `generation.rs`
    - Implement `GenerationManager` struct with `advance_generation` and `current_generation` methods
    - `advance_generation`: `UPDATE routing_generation SET generation = generation + 1 WHERE generation = $expected RETURNING generation`
    - Return `GenerationAdvanceResult::Advanced` on success, `GenerationAdvanceResult::Conflict` on CAS failure
    - On CAS failure: re-read current generation for retry
    - No leader election — any controller can advance the generation
    - _Requirements: 1.1.4, 4.1.3, 4.3.1, 4.3.3_

  - [ ] 5.4 Implement live membership tracking in `membership.rs`
    - Implement `LiveMembership` struct with `HashMap<IncarnationId, LiveNode>`
    - Store `RuntimeRegistration` data from the initial registration message
    - Track `NodeReachability` (Healthy/Suspect/Unavailable) separate from `NodeMembershipState`
    - Implement `register_node`, `update_heartbeat`, `mark_grace_period`, `mark_unavailable`, `mark_draining`, `remove_node` methods
    - Implement grace interval timer: on stream drop, start timer; on reconnect, cancel timer; on expiry, mark unavailable
    - Implement `active_nodes()` iterator returning only Active nodes
    - Route while node is Suspect (NodeReachability::Suspect)
    - _Requirements: 2.1.1, 2.1.2, 2.1.5, 2.3.1, 2.3.2, 2.3.3, 2.3.4, 2.3.5, 7.1.6_

  - [ ] 5.5 Implement routing snapshot computation and desired placement in `placement.rs`
    - Implement `compute_routing_snapshot(membership, leases, placement_config, previous_generation) -> (RoutingSnapshot, RoutingDelta)`
    - Build `execution_bundle_owners` from lease rows with `BundleOwner { node_id, epoch }` (only for nodes in active membership)
    - Build `queue_partition_homes` from bundle owners + partition mapping
    - Build `node_endpoints` from live membership
    - Include `PlacementConfig` in snapshot
    - Compute delta with `base_generation` set to previous snapshot's generation
    - Implement `compute_desired_placement(membership, leases, bundle_count) -> Vec<DesiredPlacementDirective>`
    - _Requirements: 1.4.1, 1.4.2, 1.4.3, 1.4.4, 1.4.5, 4.1.1, 4.1.2, 4.1.3, 4.1.4, 4.1.5, 4.1.6, 4.1.7_

  - [ ] 5.6 Implement gRPC service in `service.rs`
    - Implement `PlacementController` tonic service trait
    - `RuntimeMembership`: accept bidi stream, expect `RuntimeRegistration` as first message, process heartbeats, send directives (including `DesiredPlacementDirective`)
    - `SubscribeRouting`: send full snapshot on connect, then stream deltas with base_generation
    - `RefreshBundle`: look up current bundle owner from DSQL, return BundleOwnershipEntry with epoch and NodeEndpointEntry
    - `NominateScaleInCandidates`: query membership, rank by bundle count and pressure
    - `MarkNodeDraining`: update membership state, send drain directive to runtime
    - All controller instances serve active responses (no leader check)
    - _Requirements: 1.1.1, 1.1.2, 1.2.2, 1.2.3, 1.2.4, 1.2.5, 1.2.6, 4.2.1, 4.2.2, 4.2.3, 8.1.1, 8.1.4, 8.3.1, 8.3.2, 8.3.3, 8.3.4, 8.3.5_

  - [ ] 5.7 Implement two-phase drain coordination in `drain.rs`
    - Implement `DrainCoordinator` that tracks draining nodes
    - On `MarkNodeDraining`: update membership, send `DrainDirective` via stream
    - Publish routing snapshot that stops sending new queue work to draining node (phase 1: routing moves)
    - Track drain progress via heartbeat `drain_state` field
    - Observe actual DSQL ownership moved/expired before advertising new owners (phase 2: ownership confirmed)
    - _Requirements: 8.1.1, 8.1.2, 8.1.3, 8.1.4, 8.2.1, 8.2.2, 8.2.7, 8.2.9_

  - [ ] 5.8 Implement controller configuration in `config.rs`
    - Define `ControllerConfig` with fields: `controller_addr`, `heartbeat_interval`, `grace_interval`, `snapshot_publish_interval`, `bundle_count`, `partition_count`, `shard_count`, `hash_version`, `budget_directive_validity`, `dsql_connection_rate_budget`, `dsql_connection_capacity_budget`
    - No leader_lease_duration or leader_renewal_interval (active-active, no leader election)
    - Use `serde(deny_unknown_fields)` and sensible defaults (connection rate: 100, capacity: 10,000)
    - Implement `validate()` method
    - _Requirements: 1.1, 1.2, 1.3, 9.3.1, 9.3.2_

  - [ ] 5.9 Implement connection budget allocation with expiry
    - Implement `compute_connection_budget(cluster_rate, cluster_capacity, active_nodes_sorted, valid_duration) -> Vec<(IncarnationId, ConnectionBudgetDirective)>`
    - Handle zero active nodes without division by zero
    - Distribute remainder capacity deterministically by sorted IncarnationId
    - Set `valid_until` on each directive
    - Use CAS-protected budget allocation row in DSQL for coordination between active-active controllers
    - On membership change (node join/leave): recompute per-node shares and send directives
    - _Requirements: 9.1.1, 9.1.2, 9.1.3, 9.1.4, 9.1.5, 9.1.6, 9.1.7_

  - [ ] 5.10 Implement aggregate connection headroom reporting
    - Aggregate `available_connections` and `connection_rate_headroom` from runtime heartbeats
    - Expose aggregate headroom via `NominateScaleInCandidates` response for the autoscaler's scaling envelope
    - _Requirements: 9.4.1, 9.4.2_

  - [ ]* 5.11 Write unit tests for controller components
    - Test CAS generation advance: one controller succeeds, concurrent controller gets Conflict, re-read and retry works
    - Test membership: registration with full identity, heartbeat update, grace period, unavailable transition, drain, NodeReachability transitions
    - Test routing snapshot computation: correct execution_bundle_owners with epoch, queue_partition_homes, delta with base_generation, PlacementConfig included
    - Test desired placement computation: directives for acquire/relinquish
    - Test scale-in nomination: ranking by bundle count and pressure, exclusion of draining nodes
    - Test connection budget: zero nodes handled, 1 node gets full budget, 2 nodes get half each, remainder distributed by sorted IncarnationId, valid_until set correctly
    - Test aggregate connection headroom: sum of heartbeat values
    - Test RefreshBundle: returns current owner with epoch
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 2.3, 4.1, 4.3, 8.1, 8.3, 9.1, 9.4_

- [ ] 6. Checkpoint — Ensure all Phase 3 tests pass
  - Run `cargo test --workspace` and verify all new and existing tests pass.


- [ ] 7. Phase 4 — Runtime Membership Stream Client and Two-Phase Drain Protocol
  - [ ] 7.1 Implement membership stream client with explicit registration in `tokeira-runtime`
    - Create `crates/tokeira-runtime/src/membership.rs`
    - Implement `MembershipClient` struct with `run()` async method
    - On startup: connect to controller, send `RuntimeRegistration` as first message (node_id, host, port, zone, version, build_id)
    - Send periodic `RuntimeHeartbeat` messages at configurable interval (default 5s)
    - Receive and process `ControllerDirective` messages:
      - `DesiredPlacementDirective`: attempt to acquire/relinquish bundles in DSQL
      - `ConnectionBudgetDirective`: call `TokenBucketRateLimiter::reconfigure(rate_per_second, capacity)`, cap reservoir `target_ready` to `max_reservoir_size`, track `valid_until`
      - `DrainDirective`: begin two-phase drain
      - `RoutingUpdate`: update local routing state
    - On stream drop: reconnect with exponential backoff (1s base, 30s max); retain last-known connection budget until `valid_until` expiry, then degrade to conservative default
    - _Requirements: 2.1.1, 2.1.2, 2.1.3, 2.1.4, 2.1.5, 2.2.1, 2.2.2, 2.2.3, 2.2.4, 2.2.5, 9.2.1, 9.2.2, 9.2.3, 9.2.4, 9.2.5_

  - [ ] 7.2 Implement heartbeat data collection
    - Collect owned bundle count and IDs from `ShardOwner`
    - Collect queue pressure from lane handles (runnable transitions, backlog depth)
    - Collect lane pressure (active actor count)
    - Collect DSQL connection headroom from `ConnectionDirector` (if available)
    - Collect drain state from runtime drain flag
    - _Requirements: 2.2.1, 2.2.2, 2.2.3, 2.2.4, 2.2.5_

  - [ ] 7.3 Implement two-phase runtime drain protocol
    - Create `crates/tokeira-runtime/src/drain.rs`
    - On receiving `DrainDirective`:
      1. Stop acquiring new bundles
      2. Stop accepting new externally-routed work except for owned in-flight execution work
      3. Complete or reject in-flight transitions
      4. Relinquish bundle leases using epoch-checked CAS via `LeaseRepository::relinquish_bundle`
      5. After each relinquish: mark shard as `Draining` in `ShardOwner`, wait for in-flight work to complete
    - Report `SAFE_TO_TERMINATE` only when: `owned_bundle_count == 0`, `inflight_transition_count == 0`, `pending_wft_replies == 0`
    - Continue processing in-flight work for bundles still owned during drain
    - _Requirements: 8.2.1, 8.2.2, 8.2.3, 8.2.4, 8.2.5, 8.2.6, 8.2.8, 8.2.9_

  - [ ] 7.4 Wire membership client into runtime startup
    - Add `MembershipConfig` to runtime configuration (controller endpoint, heartbeat interval, backoff settings)
    - Spawn membership client as a background Tokio task during runtime initialization
    - Pass `ShardOwner`, drain signal, and `TokenBucketRateLimiter` to the membership client
    - Gracefully shut down membership stream on runtime shutdown
    - _Requirements: 2.1.1, 2.1.2_

  - [ ]* 7.5 Write unit tests for runtime membership and drain
    - Test registration message includes all required fields (node_id, host, port, zone, version, build_id)
    - Test heartbeat construction includes all required fields
    - Test DesiredPlacementDirective handling: acquire and relinquish bundles
    - Test ConnectionBudgetDirective: reconfigure rate limiter, cap reservoir, track valid_until, degrade on expiry
    - Test two-phase drain protocol: stops new acquisition, stops accepting new external work, completes in-flight, relinquishes with epoch-checked CAS, reports SAFE_TO_TERMINATE with all three conditions
    - Test reconnection backoff: verify exponential backoff timing
    - Test drain does not interrupt in-flight work
    - _Requirements: 2.1, 2.2, 8.2, 9.2_

- [ ] 8. Checkpoint — Ensure all Phase 4 tests pass
  - Run `cargo test --workspace` and verify all new and existing tests pass.

- [ ] 9. Phase 5 — Edge Routing Cache and NotShardOwner Recovery
  - [ ] 9.1 Implement routing cache with ArcSwap in `tokeira-edge`
    - Create `crates/tokeira-edge/src/routing_cache.rs`
    - Implement `RoutingCache` struct with `ArcSwap<RoutingSnapshot>` (not `RwLock`) and controller subscription
    - Implement `resolve_execution_home(namespace_id, workflow_id) -> Option<(IncarnationId, NodeEndpoint, ShardEpoch)>` — uses `execution_home_bundle` for ALL operations
    - Implement `resolve_queue_home(partition) -> Option<(IncarnationId, NodeEndpoint)>` — for poll/task dispatch only
    - Implement `invalidate_bundle(bundle_id)` for NotShardOwner recovery
    - _Requirements: 5.1.1, 5.1.2, 5.1.3, 5.1.4, 5.1.5, 5.1.6_

  - [ ] 9.2 Implement routing subscription background task
    - Connect to controller's `SubscribeRouting` RPC
    - On connect: receive full snapshot, swap into ArcSwap
    - On delta: apply incremental update with base_generation validation, swap into ArcSwap
    - On disconnect: log warning, continue with stale cache, reconnect with backoff
    - On generation gap (apply_delta returns Err): request full resync
    - _Requirements: 4.2.1, 4.2.2, 4.2.3, 4.2.4, 4.2.5, 5.3.1, 5.3.2, 5.3.3, 5.3.4_

  - [ ] 9.3 Implement NotShardOwner error type with redirect hints
    - Define `NotShardOwner { bundle_id: BundleId, current_epoch: ShardEpoch, current_owner_node_id: Option<IncarnationId> }` error variant
    - In runtime request handling: when a request arrives for a bundle the node does not own, return `NotShardOwner` with bundle_id, current_epoch, and optional current_owner_node_id hint
    - Runtime fast-fails if `observed_bundle_epoch` from edge differs from local epoch, before trying a DSQL transaction
    - Map `NotShardOwner` to appropriate gRPC status code (e.g., `ABORTED` with metadata)
    - _Requirements: 5.2.1_

  - [ ] 9.4 Implement NotShardOwner multi-path retry logic in edge routing
    - Implement `route_with_retry` function that wraps request dispatch
    - On `NotShardOwner`:
      1. Try hint: route directly to `current_owner_node_id` if present
      2. Try controller: `RefreshBundle(bundle_id)` unary call
      3. Fallback: DSQL lease lookup if controller unavailable
    - Limit retries to configurable max (default 3)
    - On max retries exceeded: return transient error to caller
    - Document: edge retry is safe only because `request_dedupe` is persisted atomically with workflow state mutation
    - _Requirements: 5.2.2, 5.2.3, 5.2.4, 5.2.5, 5.2.6, 5.2.7_

  - [ ] 9.5 Implement execution-home and queue-home resolution in edge handlers
    - For Start requests: derive `execution_key = hash(namespace_id, workflow_id) -> bundle_id` (execution-home), route to bundle owner. Queue partition used ONLY for task dispatch after execution exists.
    - For Signal/Update/Query/Cancel requests: derive same `execution_key = hash(namespace_id, workflow_id) -> bundle_id` (execution-home), route to bundle owner
    - For poll requests: derive queue partition from `(namespace_id, task_queue, task_kind)`, resolve queue-home via routing cache
    - Include `observed_bundle_epoch` from routing cache in all execution-home requests
    - Add fallback: when no queue-home preference exists, use round-robin across available nodes
    - _Requirements: 5.1.3, 5.1.4, 5.1.6, 6.2.1, 6.2.2, 6.2.3, 6.3.1, 6.3.2, 6.3.3, 6.3.4_

  - [ ] 9.6 Wire routing cache into edge startup and request handlers
    - Add `EdgeRoutingConfig` to edge configuration
    - Spawn routing subscription as a background Tokio task during edge initialization
    - Integrate routing cache lookups into existing gRPC handler dispatch paths
    - Add staleness warning logging when cache age exceeds threshold
    - _Requirements: 5.1.1, 5.3.3_

  - [ ]* 9.7 Write unit tests for edge routing cache
    - Test `resolve_execution_home` returns correct endpoint with epoch from snapshot
    - Test `resolve_execution_home` for Start and Signal with same (namespace_id, workflow_id) produces same result (Property 7 validation)
    - Test `resolve_queue_home` uses queue_partition_homes map correctly
    - Test ArcSwap-based cache provides lock-free reads
    - Test `invalidate_bundle` clears the entry
    - Test stale cache continues serving after controller disconnect
    - Test NotShardOwner multi-path retry: hint routing, controller refresh, DSQL fallback, max retries
    - Test observed_bundle_epoch is included in requests
    - Test apply_delta with base_generation mismatch triggers full resync
    - _Requirements: 5.1, 5.2, 5.3, 6.2, 6.3_

- [ ] 10. Final checkpoint — Ensure all tests pass
  - Run `cargo test --workspace` and verify all new and existing tests pass.
  - Run `cargo lint` and `cargo +nightly fmt --all --check` to verify code quality.

## Notes

- Tasks marked with `*` are test tasks — ALL tests are REQUIRED per project convention
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation per phase
- The `tokeira-controller` crate is new; all other changes extend existing crates
- `BundleId` is a type alias for `ShardId` — no breaking rename needed
- `IncarnationId` replaces the previous `NodeId` name to clarify it is not a durable node identity
- Active-active controllers replace leader election — no control lease needed
- The controller uses the existing `LeaseRepository` trait for reading lease state
- For the single-cell MVP, queue-partition -> cell mapping is trivial (all partitions map to the same cell)
- `blake3` is a new dependency for `tokeira-types` for deterministic hashing
- `arc-swap` is a new dependency for `tokeira-edge` for lock-free routing cache reads
- The chi-squared uniformity test (Property 6) must be deterministically seeded
- All proto messages use `oneof` for discriminated unions, not empty-string sentinels