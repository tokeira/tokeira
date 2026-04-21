# Requirements Document: Shard-Aware Lane Routing

## Introduction

This document captures the requirements for replacing the current hash-based lane routing (`run_key hash % lane_count`) with shard-aware lane routing (`shard_id % lane_count`). The current routing distributes runs uniformly across lanes but is blind to shard ownership. When shards move between nodes (acquisition/relinquishment), runs from the same shard can be scattered across all lanes, making shard movement expensive because every lane must drain affected runs.

Shard-aware routing ensures all runs belonging to the same shard land on the same lane. This localizes the blast radius of shard movement to a single lane, making shard acquisition and relinquishment cheaper and more predictable.

The change touches the core lane routing functions in `scanner.rs`, their callers in `runtime.rs`, and all subsystems that route work to lanes: timer scanning, WFT timeout scanning, activity timeout scanning, nexus timeout scanning, recovery (sweeper), and the effect publisher.

## Glossary

- **Lane**: A single-thread executor hosting run actors and lane-local services. Lane count is fixed at construction time (configurable, default 4).
- **Lane_Router**: The functions `lane_index_for` and `pick_lane` in `scanner.rs` that deterministically map a run to a lane.
- **Shard**: The ownership and fencing unit. The run space is partitioned into shards so that multiple nodes can own non-overlapping subsets.
- **ShardId**: A `u32` value identifying a shard, produced by `shard_for(run_key, shard_count)`.
- **Shard_Count**: The total number of shards in the cluster, held on `ShardOwner` and fixed at construction time.
- **ShardOwner**: The runtime struct that tracks which shards the current node owns, their epochs, and their lifecycle state (Sweeping → Active → Draining).
- **Run_Key**: A UUID identifying a workflow execution run. Currently used as the hash input for lane routing.
- **Sweeper**: The recovery subsystem (`recovery.rs::sweep_shard`) that republishes dispatchable work to lanes and brokers after shard acquisition.
- **Publisher**: The `RuntimeDispatchPublisher` that routes derived effects (child starts, signals, cancellations, nexus operations) to the correct lane.
- **Timer_Scanner**: The background task that scans durable storage for due timers and submits them to the appropriate lane.
- **WFT_Timeout_Scanner**: The background task that detects timed-out workflow tasks and submits timeout commands to the appropriate lane.
- **Activity_Timeout_Scanner**: The background task that detects timed-out activities and submits timeout commands to the appropriate lane.
- **Nexus_Timeout_Scanner**: The background task that detects timed-out nexus operations and submits timeout commands to the appropriate lane.

## Requirements

---

### Requirement 1: Shard-based lane index computation

**User Story:** As a Tokeira runtime developer, I want the lane routing function to compute lane index from `shard_id` instead of hashing `run_key`, so that all runs in the same shard are co-located on the same lane.

#### Acceptance Criteria

1. THE Lane_Router SHALL compute lane index as `shard_id.0 as usize % lane_count` instead of hashing Run_Key.
2. WHEN `lane_index_for` is called, THE Lane_Router SHALL accept a ShardId parameter instead of a Run_Key parameter.
3. WHEN `pick_lane` is called, THE Lane_Router SHALL accept a ShardId parameter instead of a Run_Key parameter.
4. THE Lane_Router SHALL produce the same lane index for any two Run_Key values that map to the same ShardId.

---

### Requirement 2: Shard-lane affinity invariant

**User Story:** As a Tokeira runtime developer, I want a guarantee that all runs in the same shard land on the same lane, so that shard movement only affects one lane.

#### Acceptance Criteria

1. FOR ALL Run_Key values where `shard_for(run_key, shard_count)` produces the same ShardId, THE Lane_Router SHALL return the same lane index.
2. FOR ALL valid ShardId and lane_count combinations, THE Lane_Router SHALL produce a lane index in the range `[0, lane_count)`.
3. FOR ANY given ShardId and lane_count, calling the Lane_Router twice with the same inputs SHALL produce the same lane index (determinism).

---

### Requirement 3: Timer scanner uses shard-aware routing

**User Story:** As a Tokeira runtime developer, I want the timer scanner to route due timers to lanes using shard-aware routing, so that timer-fired commands land on the correct shard-affiliated lane.

#### Acceptance Criteria

1. WHEN `run_timer_scanner` iterates over active shards and submits a due timer to a lane, THE Timer_Scanner SHALL use the shard's ShardId (already available from the `for shard_id in active_shards` iteration) for lane routing. No `shard_for` derivation from Run_Key is needed because the scanner already queries timers per-shard.

---

### Requirement 4: WFT timeout scanner uses shard-aware routing

**User Story:** As a Tokeira runtime developer, I want the WFT timeout scanner to route timeout commands to lanes using shard-aware routing, so that workflow task timeout commands land on the correct shard-affiliated lane.

#### Acceptance Criteria

1. WHEN the WFT_Timeout_Scanner submits a timeout command to a lane, THE WFT_Timeout_Scanner SHALL use `entry.shard_id` from the `WftTimeoutEntry` for lane selection.

---

### Requirement 5: Activity timeout scanner uses shard-aware routing

**User Story:** As a Tokeira runtime developer, I want the activity timeout scanner to route timeout commands to lanes using shard-aware routing, so that activity timeout commands land on the correct shard-affiliated lane.

#### Acceptance Criteria

1. WHEN the Activity_Timeout_Scanner submits a timeout command to a lane, THE Activity_Timeout_Scanner SHALL use `entry.shard_id` from the `ActivityTrackingEntry` for lane selection. This works for both shard-filtered and unfiltered scans.

---

### Requirement 6: Nexus timeout scanner uses shard-aware routing

**User Story:** As a Tokeira runtime developer, I want the nexus timeout scanner to route timeout commands to lanes using shard-aware routing, so that nexus timeout commands land on the correct shard-affiliated lane.

#### Acceptance Criteria

1. WHEN the Nexus_Timeout_Scanner submits a timeout command to a lane, THE Nexus_Timeout_Scanner SHALL use `entry.shard_id` from the `NexusTimeoutEntry` for lane selection.

---

### Requirement 6a: Workflow execution timeout scanner uses shard-aware routing

**User Story:** As a Tokeira runtime developer, I want the workflow execution timeout scanner to route timeout commands to lanes using shard-aware routing, so that workflow execution timeout commands land on the correct shard-affiliated lane.

#### Acceptance Criteria

1. WHEN the `run_workflow_timeout_scanner` submits a timeout command to a lane, THE scanner SHALL use `entry.shard_id` from the tracking entry for lane selection.

---

### Requirement 7: Recovery sweeper uses shard-aware routing

**User Story:** As a Tokeira runtime developer, I want the recovery sweeper to republish recovered work to lanes using shard-aware routing, so that recovered work lands on the correct shard-affiliated lane.

#### Acceptance Criteria

1. WHEN `sweep_shard` republishes due timers during shard sweep, THE Sweeper SHALL use its `shard_id` parameter directly for lane selection. No `shard_for` derivation from Run_Key is needed because `sweep_shard` is already scoped to a single shard.
2. THE Sweeper SHALL NOT require Shard_Count for lane routing on this path.

---

### Requirement 8: Publisher uses shard-aware routing

**User Story:** As a Tokeira runtime developer, I want the effect publisher to route derived effects (child starts, signals, cancellations, nexus operations) to lanes using shard-aware routing, so that published commands land on the correct shard-affiliated lane.

#### Acceptance Criteria

1. WHEN the Publisher routes a command to a lane, THE Publisher SHALL derive the ShardId from the target Run_Key and Shard_Count, then use the ShardId for lane selection.
2. THE Publisher SHALL have access to the Shard_Count at construction time or through the ShardOwner.

---

### Requirement 9: Runtime command submission uses shard-aware routing

**User Story:** As a Tokeira runtime developer, I want the runtime's `pick_lane` and `lane_index` methods to use shard-aware routing, so that all command submissions to lanes are consistent with the new routing scheme.

#### Acceptance Criteria

1. WHEN `TokeiraRuntime::pick_lane` is called, THE runtime SHALL derive the ShardId from the Run_Key and Shard_Count, then delegate to the updated Lane_Router.
2. WHEN `TokeiraRuntime::lane_index` is called, THE runtime SHALL derive the ShardId from the Run_Key and Shard_Count, then delegate to the updated Lane_Router.
3. THE runtime SHALL obtain Shard_Count from the ShardOwner that is already available on the runtime struct.

---

### Requirement 10: Shard_Count availability at routing time

**User Story:** As a Tokeira runtime developer, I want the shard count to be available to callers that only have a Run_Key, so that they can derive ShardId before routing.

#### Acceptance Criteria

1. THE runtime SHALL make Shard_Count accessible to the Publisher and the Runtime facade — the only two callers that derive ShardId from Run_Key via `shard_for(run_key, shard_count)`.
2. Scanners (Timer, WFT, Activity, Nexus, Workflow Execution Timeout) and the Sweeper SHALL NOT require Shard_Count for lane routing because they already have ShardId in scope (from the shard iteration loop or from the tracking entry).

---

### Requirement 11: Deterministic and stable shard-to-lane mapping

**User Story:** As a Tokeira runtime developer, I want the shard-to-lane mapping to be deterministic and stable, so that the same shard always routes to the same lane for a given lane count.

#### Acceptance Criteria

1. FOR ANY ShardId and lane_count, `shard_id.0 as usize % lane_count` SHALL be a pure function with no hidden state or randomness.
2. FOR ANY Run_Key and Shard_Count, `shard_for(run_key, shard_count) % lane_count` SHALL produce the same result across invocations, processes, and restarts.
3. THE mapping SHALL NOT depend on the order of shard acquisition, the number of owned shards, or any runtime state beyond ShardId and lane_count.

---

## Non-Goals

- **Dynamic lane count**: Lane count remains fixed at construction time. No runtime resizing.
- **Shard-pinned lanes (1:1 shard:lane mapping)**: Multiple shards may share a lane. The mapping is `shard_id % lane_count`, not a dedicated lane per shard.
- **Lane draining on shard relinquishment**: Runs are evicted when the shard epoch changes. No graceful lane-level drain is introduced by this change.
