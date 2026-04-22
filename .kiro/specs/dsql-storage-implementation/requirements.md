# Requirements Document: DSQL Storage Implementation

## Introduction

This document captures the full requirements for implementing the Aurora DSQL storage layer — the production persistence backend for Tokeira. Currently all storage is served by `InMemoryStore` in `tokeira-storage/src/memory.rs`. This spec covers the complete DSQL implementation: schema design, `RunRepository` implementation, connection management integration, shard leasing, and projection persistence.

The authoritative architecture documents are [050-dsql-storage](../../../docs/architecture/050-dsql-storage.md), [060-connection-management](../../../docs/architecture/060-connection-management.md), [070-projection-plane](../../../docs/architecture/070-projection-plane.md), [080-sql-visibility](../../../docs/architecture/080-sql-visibility.md), and [090-failover-and-recovery](../../../docs/architecture/090-failover-and-recovery.md).

The schema is designed from scratch for Tokeira's architecture and DSQL constraints. It is NOT a port of the temporal-dsql compatibility schema. Key architectural principles:

- History is the authority (010)
- One transition = one DSQL transaction
- Delivery is derived, not co-equal
- Single-writer per run via shard fencing
- DSQL constraints shape the schema directly (no BIGSERIAL, no CHECK, no temp tables, UUID PKs, 3000-row mutation limit, OCC)

The implementation is organized into 6 features with explicit dependency ordering.

**Dependency graph:**

- Feature 1 (Schema and Connection Foundation) — no dependencies
- Feature 2 (Core Persistence — RunRepository on DSQL) — depends on Feature 1
- Feature 3 (Side Tables — Activity, Timer, Nexus State) — depends on Feature 2
- Feature 4 (Shard Lease Management) — depends on Feature 1
- Feature 5 (Dispatch Backlog Persistence) — depends on Feature 2
- Feature 6 (Projection Persistence) — depends on Feature 1, independent of Features 2–5

The actual design and tasks for each feature will live in child specs:
- `dsql-schema-connection` (Feature 1)
- `dsql-core-persistence` (Feature 2)
- `dsql-side-tables` (Feature 3)
- `dsql-shard-leasing` (Feature 4)
- `dsql-dispatch-backlog` (Feature 5)
- `dsql-projection-persistence` (Feature 6)

## Audit Gap Traceability

The table below maps every `RunRepository`, `ProjectionLog`, `LeaseRepository`, and `ConnectionDirector` trait method to the feature that implements it against DSQL.

### RunRepository Methods

| Method | Feature | Notes |
|---|---|---|
| `commit_transition` | Feature 2 | Primary write path — one fenced DSQL transaction |
| `load_run` | Feature 2 | Read `workflow_hot` |
| `resolve_execution` | Feature 2 | `current_execution` lookup |
| `find_latest_run` | Feature 2 | `current_execution` lookup (open or closed) |
| `read_history` | Feature 2 | Paginated reads from `history_batch` |
| `lookup_request_dedupe` | Feature 2 | Idempotency check against `request_dedupe` |
| `read_transition_audit` | Feature 2 | Debug/test audit log (may be test-only) |
| `materialize_reset_successor` | Feature 2 | Reset fork materialization |
| `list_dispatchable_workflow_tasks` | Feature 3 | Queue-filtered query on `workflow_hot` |
| `list_dispatchable_activity_tasks` | Feature 3 | Queue-filtered query on `activity_state` |
| `list_due_timers` | Feature 3 | Time-filtered query on `timer_bucket` |
| `list_dispatchable_workflow_tasks_for_shard` | Feature 3 | Shard-filtered sweep |
| `list_dispatchable_activity_tasks_for_shard` | Feature 3 | Shard-filtered sweep |
| `list_due_timers_for_shard` | Feature 3 | Shard-filtered sweep |
| `list_runs_with_workflow_timeouts_for_shard` | Feature 3 | Shard-filtered sweep |
| `list_started_workflow_tasks_for_shard` | Feature 3 | Shard-filtered sweep |
| `list_open_activities_for_shard` | Feature 3 | Shard-filtered sweep |
| `list_pending_nexus_operations_for_shard` | Feature 3 | Shard-filtered sweep |
| `persist_to_backlog` | Feature 5 | Backlog insert |
| `drain_backlog` | Feature 5 | Backlog drain (FIFO) |

### Other Trait Methods

| Trait | Method | Feature | Notes |
|---|---|---|---|
| `LeaseRepository` | `try_acquire_bundle` | Feature 4 | Shard lease acquisition with epoch fencing |
| `LeaseRepository` | `renew_bundle` | Feature 4 | Shard lease renewal with epoch check |
| `ProjectionLog` | `read_from` | Feature 6 | Partitioned projection log reads |
| `ConnectionDirector` | `acquire` | Feature 1 | Class-based connection permit |

### DSQL Constraints Honored

| Constraint | How Addressed | Feature |
|---|---|---|
| No BIGSERIAL | UUID primary keys, Snowflake IDs where needed | Feature 1 |
| No CHECK constraints | Application-level validation in Rust | All |
| No temp tables | CTE-based query compilation | Features 2, 3, 6 |
| 3,000-row mutation limit | Bounded transition write sets | Feature 2 |
| Fixed Repeatable Read isolation | OCC with commit-time conflict detection | Feature 2 |
| 5-minute max transaction time | Narrow, single-transition transactions | Feature 2 |
| 60-minute max connection lifetime | Session recycling with jitter | Feature 1 |
| One database per cluster | Single schema for all tables | Feature 1 |
| PK distribution matters | UUID/hash-prefixed keys for hot tables | Feature 1 |
| INDEX ASYNC | Non-blocking index creation in migrations | Feature 1 |
| 100 connections/sec rate limit | Token-bucket rate limiting in ConnectionDirector | Feature 1 |
| 10,000 connection limit | Budget-based allocation across nodes | Feature 1 |

## What This Spec Does NOT Cover

- Multi-cluster placement (deferred to 037-dynamic-placement)
- Archival to S3 (deferred to 075-archival-to-s3)
- Snapshot + suffix recovery (deferred; Continue-As-New is the MVP compaction strategy)
- Advanced visibility query compiler (covered by `projection-visibility` spec)

## Technology Decisions

### DSQL Rust Connector

The implementation SHALL evaluate the official AWS DSQL Rust connector (`aurora-dsql-connectors/rust/sqlx`) from https://github.com/awslabs/aurora-dsql-connectors/tree/main/rust/sqlx as the database driver layer. This provides native sqlx integration with DSQL-specific IAM token authentication and PostgreSQL wire protocol compatibility. The temporal-dsql workspace's connection lifecycle management (reservoir, token-refreshing driver, proactive expiry scanning, guard windows, in-flight semaphores) may still be needed as a layer above the official connector — the child spec for Feature 1 should evaluate what the official connector provides vs. what must be retained from the temporal-dsql implementation.

### Schema Analysis

All schema designs SHALL be validated using the Kiro DSQL Power for compatibility analysis before implementation. This includes verifying primary key distribution, index strategy, transaction patterns, and DSQL-specific constraint compliance.

## Glossary

- **DSQL**: Amazon Aurora DSQL — a PostgreSQL-compatible serverless distributed SQL database with fixed Repeatable Read isolation and optimistic concurrency control.
- **RunRepository**: The primary storage trait in `tokeira-storage/src/api.rs` defining 20+ methods for durable run persistence. All methods must be implemented against DSQL.
- **LeaseRepository**: The storage trait for shard lease management with epoch-fenced acquire and renew operations.
- **ProjectionLog**: The read-only trait for projection workers to consume partitioned projection log substreams.
- **ConnectionDirector**: The trait for class-based connection budget control, enforcing per-class connection limits and open-rate budgets.
- **InMemoryStore**: The current development/test storage backend in `tokeira-storage/src/memory.rs` that implements all storage traits using in-memory data structures.
- **DsqlStore**: The production DSQL storage backend to be implemented, replacing `InMemoryStore` for production deployments.
- **Transition**: The bounded, explicit description of what must be committed as a result of one kernel `apply` call. Contains next_state, history events, dispatch ops, projection ops, activity/timer ops, and request dedupe ops.
- **TransitionSeq**: Internal fence/checkpoint number for committed state transitions, incremented exactly once per transition.
- **ShardEpoch**: Monotonically increasing epoch number for shard ownership fencing. A stale epoch causes commit rejection.
- **RunKey**: UUID-based durable storage key for a workflow run. Used as primary key in `workflow_hot` and clustering key in related tables.
- **OCC**: Optimistic Concurrency Control — DSQL's conflict detection model where transactions proceed optimistically and conflicts are detected at commit time.
- **CTE**: Common Table Expression — SQL WITH clauses used instead of temp tables per DSQL migration guidance.
- **Reservoir_Pattern**: Channel-based connection buffer with continuous refiller, proactive expiry scanner, and guard window for managing DSQL connection lifecycle.
- **Shard_Fencing**: The mechanism by which every write transaction validates the caller's shard epoch against the durable epoch, preventing stale owners from committing after failover.
- **History_Batch**: Immutable append-only event batch table. Each row contains a contiguous range of history events from one transition.
- **Workflow_Hot**: Small current summary row per open run, containing the compact `WorkflowState` needed for the runtime to process the next command.
- **Current_Execution**: Mapping table from `(namespace_id, workflow_id)` to the current run identity and open/closed status.
- **Dispatch_Backlog**: Durable fallback table for unmatched tasks when no worker is immediately available.
- **Projection_Log**: Typed durable mutation log consumed by projection sinks to maintain visibility and search-attribute tables.
- **Projector_Checkpoint**: Per-sink, per-substream cursor tracking table for projection consumer progress.
- **Vis_Execution**: Materialized visibility row store for Temporal-compatible list/filter/count queries.
- **Timer_Bucket**: Bucketed wakeup record table for due-time scanning, keyed by shard and fire-at time.
- **Activity_State**: Normalized current state table for open activities, keyed by run_key and schedule_event_id.
- **Request_Dedupe**: Idempotency record table for external command deduplication.
- **DbClass**: Enumerated workload class (Control, Commit, Read, Projection, Maintenance) used for connection budget prioritization.
- **Fanout_Key**: Hash-based partition dimension prepended to append-like tables (dispatch_backlog, projection_log) to avoid hot write partitions.

## Requirements

---

## Feature 1: Schema and Connection Foundation

### Requirement 1.1: DSQL Schema DDL

**User Story:** As a Tokeira operator, I want a complete DSQL schema definition, so that all tables required for production persistence can be created in an Aurora DSQL cluster.

#### Acceptance Criteria

1. THE DsqlStore SHALL define DDL for all core tables: `shard_lease`, `current_execution`, `workflow_hot`, `history_batch`, `request_dedupe`, `activity_state`, `timer_bucket`.
2. THE DsqlStore SHALL define DDL for the delivery table: `dispatch_backlog`.
3. THE DsqlStore SHALL define DDL for the projection tables: `projection_log`, `projector_checkpoint`, `vis_execution`.
4. THE DDL SHALL use UUID primary keys for hot-write tables (`workflow_hot`, `history_batch`, `activity_state`) to avoid monotonic hot partitions.
5. THE DDL SHALL cluster related rows by `run_key` where possible to exploit single-writer affinity.
6. THE DDL SHALL prepend a fanout/hash dimension to append-like tables (`dispatch_backlog`, `projection_log`) to distribute writes across multiple DSQL partitions.
7. THE DDL SHALL NOT use BIGSERIAL, CHECK constraints, or temporary tables.
8. THE DDL SHALL use INDEX ASYNC for all secondary index creation to avoid blocking base-table operations.
9. THE DDL SHALL fit within a single DSQL database and single schema for MVP.

### Requirement 1.2: Schema Migration Tooling

**User Story:** As a Tokeira operator, I want schema migration tooling, so that I can create and evolve the DSQL schema safely in production.

#### Acceptance Criteria

1. THE Migration_Tooling SHALL apply DDL migrations separately from runtime DML traffic.
2. THE Migration_Tooling SHALL track applied migration versions to prevent re-application.
3. THE Migration_Tooling SHALL support forward-only migrations for MVP (rollback is manual).
4. IF a migration fails partway through, THEN THE Migration_Tooling SHALL report the failure state clearly and allow manual recovery.

### Requirement 1.3: Connection Pool with Reservoir Pattern

**User Story:** As a Tokeira developer, I want a connection pool that respects DSQL's connection rate limits and lifetime constraints, so that the system avoids reconnection storms and connection exhaustion.

#### Acceptance Criteria

1. THE ConnectionDirector SHALL maintain a node-local connection pool with class-based permits for Control, Commit, Read, Projection, and Maintenance workloads.
2. THE ConnectionDirector SHALL implement the Reservoir Pattern: a channel-based connection buffer with a continuous refiller goroutine, proactive expiry scanner, guard window before hard lifetime cutoff, and in-flight semaphore to limit concurrent connection creation.
3. THE ConnectionDirector SHALL rate-limit new connection creation using a token-bucket algorithm respecting DSQL's 100 connections/second sustained rate and 1,000 burst capacity.
4. THE ConnectionDirector SHALL recycle connections before DSQL's 60-minute hard lifetime cutoff, using a configurable base lifetime (default 50–55 minutes) with jitter to prevent mass recycling.
5. WHEN the connection budget is exhausted, THE ConnectionDirector SHALL degrade workload classes in priority order: stop Maintenance first, then Projection, then throttle Read, while protecting Control and Commit traffic.
6. THE ConnectionDirector SHALL expose metrics for pool utilization, checkout latency, empty-reservoir events, connection recycling, and refill rate.
7. THE Reservoir SHALL maintain a configurable target number of ready connections (default 50) and proactively refill when the ready count drops below the target.
8. THE Reservoir SHALL use the official AWS DSQL Rust connector (`aurora-dsql-connectors/rust/sqlx`) for connection creation, IAM token authentication, and connection lifecycle management.

### Requirement 1.4: IAM Token-Refreshing Authentication

**User Story:** As a Tokeira operator, I want DSQL connections to authenticate using IAM tokens that refresh automatically, so that long-running nodes maintain valid database access without manual credential rotation.

#### Acceptance Criteria

1. THE DsqlStore SHALL authenticate new DSQL connections using IAM-generated authentication tokens.
2. THE DsqlStore SHALL cache authentication tokens and refresh them before expiry.
3. WHEN a token refresh fails, THE DsqlStore SHALL retry with backoff and continue using existing valid connections.
4. THE DsqlStore SHALL support configurable IAM role separation for admin/migration, runtime, and read-only workloads.

### Requirement 1.5: Primary Key Distribution Strategy

**User Story:** As a Tokeira developer, I want primary keys designed for DSQL's distributed storage model, so that write traffic is spread across partitions and avoids hot spots.

#### Acceptance Criteria

1. THE DsqlStore SHALL use UUID-typed primary keys for `workflow_hot`, `current_execution`, and `request_dedupe`.
2. THE DsqlStore SHALL use composite keys `(run_key, first_event_id)` for `history_batch` to cluster events by run.
3. THE DsqlStore SHALL use composite keys `(run_key, schedule_event_id)` for `activity_state` to cluster by run.
4. THE DsqlStore SHALL use composite keys `(shard_id, fire_at, run_key, timer_id)` for `timer_bucket` to enable efficient shard-filtered time-range scans.
5. THE DsqlStore SHALL prepend a hash-based `partition_id` to `dispatch_backlog` and `projection_log` primary keys to distribute append traffic.

---

## Feature 2: Core Persistence — RunRepository on DSQL

### Requirement 2.1: Fenced Commit Transaction

**User Story:** As a Tokeira developer, I want `commit_transition` to execute as a single fenced DSQL transaction, so that one workflow transition is atomically persisted with OCC conflict detection.

#### Acceptance Criteria

1. WHEN `commit_transition` is called, THE DsqlStore SHALL execute all writes for the transition within a single DSQL transaction.
2. THE transaction SHALL validate the caller's `TransitionSeq` against the durable value in `workflow_hot` and return `CommitResult::Conflict` on mismatch.
3. THE transaction SHALL validate the caller's `ShardEpoch` against the durable shard lease epoch and return `CommitResult::Conflict` if the epoch is stale.
4. WHEN the transaction succeeds, THE DsqlStore SHALL return `CommitResult::Applied` with the new authoritative `WorkflowState`.
5. WHEN a DSQL OCC conflict is detected at commit time, THE DsqlStore SHALL classify the outcome as retryable conflict and return `CommitResult::Conflict`.
6. THE transaction write set SHALL remain within DSQL's 3,000-row mutation limit for any single transition.
7. THE transaction SHALL complete within DSQL's 5-minute maximum transaction time.

### Requirement 2.2: Commit Transaction Write Set

**User Story:** As a Tokeira developer, I want `commit_transition` to persist all transition components atomically, so that history, state, side effects, and derived data are never partially visible.

#### Acceptance Criteria

1. WHEN `commit_transition` succeeds, THE DsqlStore SHALL have upserted the `workflow_hot` row with the new `WorkflowState`.
2. WHEN `commit_transition` succeeds and the transition contains history events, THE DsqlStore SHALL have appended a `history_batch` row containing the events.
3. WHEN `commit_transition` succeeds and the transition contains `ActivityOp` entries, THE DsqlStore SHALL have applied upserts and deletes to `activity_state`.
4. WHEN `commit_transition` succeeds and the transition contains `TimerOp` entries, THE DsqlStore SHALL have applied upserts and deletes to `timer_bucket`.
5. WHEN `commit_transition` succeeds and the transition contains `RequestDedupeOp` entries, THE DsqlStore SHALL have inserted records into `request_dedupe`.
6. WHEN `commit_transition` succeeds and the transition contains `ProjectionOp` entries, THE DsqlStore SHALL have appended records to `projection_log`.
7. WHEN `commit_transition` succeeds and the transition contains `DispatchOp` entries that require backlog persistence, THE DsqlStore SHALL have inserted records into `dispatch_backlog`.
8. WHEN `commit_transition` is called for a Start command, THE DsqlStore SHALL insert a `current_execution` row mapping `(namespace_id, workflow_id)` to the new run identity.

### Requirement 2.3: Start Workflow with Conflict Policy

**User Story:** As a Tokeira developer, I want `commit_transition` for a Start command to respect the current-execution conflict policy, so that workflow-id reuse semantics are enforced at the storage level.

#### Acceptance Criteria

1. WHEN a Start transition is committed and no `current_execution` row exists for `(namespace_id, workflow_id)`, THE DsqlStore SHALL insert the new mapping.
2. WHEN a Start transition is committed and a `current_execution` row exists with an open run under the Reject policy, THE DsqlStore SHALL return `CommitResult::Conflict`.
3. WHEN a Start transition is committed and a `current_execution` row exists with a closed run under the AllowAfterClose policy, THE DsqlStore SHALL replace the mapping with the new run identity.
4. WHEN a Start transition is committed and a `current_execution` row exists with an open run under the AllowAfterClose policy, THE DsqlStore SHALL return `CommitResult::Conflict`.

### Requirement 2.4: Duplicate Request Detection

**User Story:** As a Tokeira developer, I want `commit_transition` to detect duplicate requests within the same transaction, so that idempotent handling is enforced at the storage level.

#### Acceptance Criteria

1. WHEN `commit_transition` inserts a `request_dedupe` record and a record with the same `(namespace_id, workflow_id, request_id)` already exists, THE DsqlStore SHALL return `CommitResult::Duplicate`.
2. THE duplicate check SHALL be performed within the same transaction as the rest of the commit to prevent race conditions.

### Requirement 2.5: Load Run State

**User Story:** As a Tokeira developer, I want `load_run` to read the current `WorkflowState` from DSQL, so that the runtime can process the next command for a run.

#### Acceptance Criteria

1. WHEN `load_run` is called with a known `RunKey`, THE DsqlStore SHALL return `LoadedRun::Existing` with the `WorkflowState` from the `workflow_hot` row.
2. WHEN `load_run` is called with an unknown `RunKey`, THE DsqlStore SHALL return `LoadedRun::Absent`.
3. THE DsqlStore SHALL deserialize the `WorkflowState` faithfully, preserving all fields including pending WFT, activities, timers, children, updates, and Nexus operations.

### Requirement 2.6: Resolve Execution and Find Latest Run

**User Story:** As a Tokeira developer, I want `resolve_execution` and `find_latest_run` to look up run identity from `current_execution`, so that the runtime can route commands to the correct run.

#### Acceptance Criteria

1. WHEN `resolve_execution` is called with an `ExecutionRef` that has no `run_id`, THE DsqlStore SHALL return the `RunKey` of the current open run from `current_execution`, or `None` if no open run exists.
2. WHEN `resolve_execution` is called with an `ExecutionRef` that has a specific `run_id`, THE DsqlStore SHALL return the `RunKey` for that specific run if known, even if the run is closed.
3. WHEN `find_latest_run` is called, THE DsqlStore SHALL return the `RunKey` of the most recent run for the given `(namespace_id, workflow_id)`, whether open or closed, or `None` if no run has ever existed.

### Requirement 2.7: Read History

**User Story:** As a Tokeira developer, I want `read_history` to return paginated history events from DSQL, so that the runtime and edge layer can serve history to workers and API callers.

#### Acceptance Criteria

1. WHEN `read_history` is called, THE DsqlStore SHALL return history events from `history_batch` rows for the given `RunKey` where `event_id > after_event_id`, ordered by event ID ascending, up to `limit` events.
2. WHEN no events exist after `after_event_id`, THE DsqlStore SHALL return an empty vector.
3. THE DsqlStore SHALL reconstruct individual `HistoryEvent` values from the batch storage format.

### Requirement 2.8: Request Deduplication Lookup

**User Story:** As a Tokeira developer, I want `lookup_request_dedupe` to check for previously committed requests, so that the runtime can short-circuit duplicate external commands.

#### Acceptance Criteria

1. WHEN `lookup_request_dedupe` is called for a request that was previously committed, THE DsqlStore SHALL return the `RequestRecord` with the original run identity and transition sequence.
2. WHEN `lookup_request_dedupe` is called for an unknown request, THE DsqlStore SHALL return `None`.

### Requirement 2.9: Materialize Reset Successor

**User Story:** As a Tokeira developer, I want `materialize_reset_successor` to create a new run by copying a prefix of the base run's history, so that workflow reset can fork execution from a prior point.

#### Acceptance Criteria

1. WHEN `materialize_reset_successor` is called, THE DsqlStore SHALL copy history events from the base run through `fork_event_id` into the successor run's `history_batch`.
2. WHEN `materialize_reset_successor` is called, THE DsqlStore SHALL derive the successor's `WorkflowState` by replaying the copied history prefix.
3. WHEN `materialize_reset_successor` is called, THE DsqlStore SHALL insert a `workflow_hot` row for the successor run.
4. IF `fork_event_id` is invalid (beyond the base run's history), THEN THE DsqlStore SHALL return an error.

### Requirement 2.10: OCC Retry Classification

**User Story:** As a Tokeira developer, I want the DSQL storage layer to classify OCC conflicts into actionable categories, so that the runtime can decide whether to retry, reload, or reject.

#### Acceptance Criteria

1. WHEN a DSQL transaction fails with a serialization conflict (OCC), THE DsqlStore SHALL classify the outcome as a retryable conflict.
2. WHEN a transition-seq or epoch fence check fails, THE DsqlStore SHALL classify the outcome as a validation conflict (not blindly retryable — runtime must reload).
3. THE DsqlStore SHALL NOT silently retry OCC conflicts internally; conflict classification is returned to the runtime for decision.

---

## Feature 3: Side Tables — Activity, Timer, Nexus State

### Requirement 3.1: Activity State Table

**User Story:** As a Tokeira developer, I want open activity state persisted in a normalized `activity_state` table, so that shard-filtered sweep queries can reconstruct activity timeout tracking after failover.

#### Acceptance Criteria

1. WHEN `commit_transition` processes an `ActivityOp::Upsert`, THE DsqlStore SHALL upsert a row in `activity_state` with the activity's current state including schedule_event_id, attempt, timeouts, started_at, and heartbeat details.
2. WHEN `commit_transition` processes an `ActivityOp::Delete`, THE DsqlStore SHALL delete the corresponding row from `activity_state`.
3. THE `activity_state` table SHALL be keyed by `(run_key, schedule_event_id)` to cluster by run.

### Requirement 3.2: Timer Bucket Table

**User Story:** As a Tokeira developer, I want timer wakeup records persisted in a bucketed `timer_bucket` table, so that due-time scanning can efficiently find timers ready to fire.

#### Acceptance Criteria

1. WHEN `commit_transition` processes a `TimerOp::Upsert`, THE DsqlStore SHALL upsert a row in `timer_bucket` with the timer's fire_at time, shard_id, run_key, and timer_id.
2. WHEN `commit_transition` processes a `TimerOp::Delete`, THE DsqlStore SHALL delete the corresponding row from `timer_bucket`.
3. THE `timer_bucket` table SHALL support efficient range scans by `(shard_id, fire_at)` for due-timer queries.

### Requirement 3.3: Queue-Filtered Dispatchable Queries

**User Story:** As a Tokeira developer, I want `list_dispatchable_workflow_tasks` and `list_dispatchable_activity_tasks` to query DSQL by task queue, so that the broker can find work for specific queues.

#### Acceptance Criteria

1. WHEN `list_dispatchable_workflow_tasks` is called, THE DsqlStore SHALL return runs from `workflow_hot` that have a pending but not-yet-started workflow task matching the given queue, up to `limit`.
2. WHEN `list_dispatchable_activity_tasks` is called, THE DsqlStore SHALL return activity entries from `activity_state` that are in a schedulable state for the given queue, up to `limit`.
3. WHEN `list_due_timers` is called, THE DsqlStore SHALL return timer entries from `timer_bucket` where `fire_at <= now`, up to `limit`.

### Requirement 3.4: Shard-Filtered Sweep Queries

**User Story:** As a Tokeira developer, I want 7 shard-filtered sweep queries to reconstruct volatile delivery state after shard acquisition, so that failover recovery can republish pending work.

#### Acceptance Criteria

1. WHEN `list_dispatchable_workflow_tasks_for_shard` is called, THE DsqlStore SHALL return runs with pending workflow tasks for the given shard, up to `limit`.
2. WHEN `list_dispatchable_activity_tasks_for_shard` is called, THE DsqlStore SHALL return schedulable activities for the given shard, up to `limit`.
3. WHEN `list_due_timers_for_shard` is called, THE DsqlStore SHALL return due timers for the given shard where `fire_at <= now`, up to `limit`.
4. WHEN `list_runs_with_workflow_timeouts_for_shard` is called, THE DsqlStore SHALL return open runs with workflow timeout configuration for the given shard, up to `limit`.
5. WHEN `list_started_workflow_tasks_for_shard` is called, THE DsqlStore SHALL return runs with started (in-progress) workflow tasks for the given shard, up to `limit`.
6. WHEN `list_open_activities_for_shard` is called, THE DsqlStore SHALL return open activities with timeout configuration for the given shard, up to `limit`.
7. WHEN `list_pending_nexus_operations_for_shard` is called, THE DsqlStore SHALL return pending Nexus operations with schedule_to_close_timeout for the given shard, up to `limit`.
8. ALL shard-filtered sweep queries SHALL derive shard assignment from `run_key` using the same deterministic mapping as the runtime.

### Requirement 3.5: Nexus Operation State Tracking

**User Story:** As a Tokeira developer, I want pending Nexus operation state queryable for shard-filtered sweeps, so that Nexus timeout tracking can be reconstructed after failover.

#### Acceptance Criteria

1. THE DsqlStore SHALL persist Nexus operation state as part of the `WorkflowState` in `workflow_hot` (Nexus operations are tracked in the workflow's pending state, not a separate table for MVP).
2. WHEN `list_pending_nexus_operations_for_shard` is called, THE DsqlStore SHALL extract pending Nexus operations with `schedule_to_close_timeout` from `workflow_hot` rows belonging to the given shard.

---

## Feature 4: Shard Lease Management

### Requirement 4.1: Shard Lease Table

**User Story:** As a Tokeira developer, I want a `shard_lease` table in DSQL, so that shard ownership can be tracked with epoch fencing for single-writer guarantees.

#### Acceptance Criteria

1. THE DsqlStore SHALL define a `shard_lease` table with columns for shard_id, owner, epoch, and lease_expiry.
2. THE `shard_lease` table SHALL use `shard_id` as the primary key.
3. THE epoch column SHALL be a monotonically increasing integer that is incremented on every successful acquisition.

### Requirement 4.2: Lease Acquisition

**User Story:** As a Tokeira developer, I want `try_acquire_bundle` to atomically acquire shard ownership with epoch fencing, so that exactly one node owns each shard at any given epoch.

#### Acceptance Criteria

1. WHEN `try_acquire_bundle` is called and no lease exists for the shard, THE DsqlStore SHALL insert a new lease row with epoch 1 and return `LeaseOutcome::Acquired`.
2. WHEN `try_acquire_bundle` is called and the existing lease has expired, THE DsqlStore SHALL update the lease with a new owner, incremented epoch, and new expiry, returning `LeaseOutcome::Acquired`.
3. WHEN `try_acquire_bundle` is called and the existing lease is held by another owner and has not expired, THE DsqlStore SHALL return `LeaseOutcome::Rejected` with the current owner and epoch.
4. THE lease acquisition SHALL use DSQL's OCC to prevent two nodes from acquiring the same shard simultaneously.

### Requirement 4.3: Lease Renewal

**User Story:** As a Tokeira developer, I want `renew_bundle` to extend an existing lease with epoch validation, so that the current owner can maintain ownership without re-acquisition.

#### Acceptance Criteria

1. WHEN `renew_bundle` is called with a matching owner and epoch, THE DsqlStore SHALL update the lease expiry and return `LeaseOutcome::Renewed`.
2. WHEN `renew_bundle` is called with a stale epoch, THE DsqlStore SHALL return `LeaseOutcome::Rejected` with the current owner and epoch.
3. WHEN `renew_bundle` is called with a non-matching owner, THE DsqlStore SHALL return `LeaseOutcome::Rejected`.

### Requirement 4.4: Epoch Fencing in Commit Path

**User Story:** As a Tokeira developer, I want every `commit_transition` to validate the shard epoch, so that a stale owner cannot commit after failover.

#### Acceptance Criteria

1. WHEN `commit_transition` is called, THE DsqlStore SHALL read the current shard epoch for the run's shard within the transaction.
2. IF the caller's epoch does not match the durable shard epoch, THEN THE DsqlStore SHALL abort the transaction and return `CommitResult::Conflict` with a reason indicating epoch mismatch.
3. THE epoch check SHALL be performed within the same transaction as the state mutation to prevent TOCTOU races.

---

## Feature 5: Dispatch Backlog Persistence

### Requirement 5.1: Dispatch Backlog Table

**User Story:** As a Tokeira developer, I want a `dispatch_backlog` table in DSQL, so that unmatched tasks can be durably persisted for later retry when no worker is immediately available.

#### Acceptance Criteria

1. THE DsqlStore SHALL define a `dispatch_backlog` table with columns for partition_id (fanout hash), queue identity, run_key, payload, scheduled_at, and insertion_seq.
2. THE `dispatch_backlog` primary key SHALL include a fanout/hash dimension to distribute writes across DSQL partitions.
3. THE `dispatch_backlog` table SHALL support FIFO ordering within a queue via the insertion_seq column.

### Requirement 5.2: Persist to Backlog

**User Story:** As a Tokeira developer, I want `persist_to_backlog` to durably store unmatched task entries, so that they survive node failures and can be drained on the next sweep cycle.

#### Acceptance Criteria

1. WHEN `persist_to_backlog` is called with a list of `BacklogEntry` values, THE DsqlStore SHALL insert all entries into `dispatch_backlog`.
2. THE DsqlStore SHALL preserve the input ordering by assigning monotonically increasing `insertion_seq` values.
3. THE DsqlStore SHALL support both Workflow and Activity backlog payload types.

### Requirement 5.3: Drain Backlog

**User Story:** As a Tokeira developer, I want `drain_backlog` to atomically remove and return backlog entries in FIFO order, so that the runtime can retry dispatching tasks to newly available workers.

#### Acceptance Criteria

1. WHEN `drain_backlog` is called, THE DsqlStore SHALL return up to `limit` entries for the given queue in FIFO order (ascending `insertion_seq`).
2. WHEN `drain_backlog` returns entries, THE DsqlStore SHALL delete those entries from `dispatch_backlog` within the same transaction.
3. WHEN no backlog entries exist for the given queue, THE DsqlStore SHALL return an empty vector.

---

## Feature 6: Projection Persistence

### Requirement 6.1: Projection Log Table

**User Story:** As a Tokeira developer, I want a `projection_log` table in DSQL, so that typed projection mutations from authoritative transitions are durably stored for consumption by projection sinks.

#### Acceptance Criteria

1. THE DsqlStore SHALL define a `projection_log` table with columns for partition_id, fanout, run_key, transition_seq, projection context, and serialized projection ops.
2. THE `projection_log` primary key SHALL include `(partition_id, fanout, run_key, transition_seq)` to enable partitioned reads and per-run ordering.
3. THE `projection_log` table SHALL prepend a hash-based `partition_id` to distribute writes across DSQL partitions.

### Requirement 6.2: Projection Log Reads

**User Story:** As a Tokeira developer, I want `ProjectionLog::read_from` to return batches of projection records from a cursor position, so that projection sinks can consume the log incrementally.

#### Acceptance Criteria

1. WHEN `read_from` is called with a cursor, THE DsqlStore SHALL return up to `limit` `ProjectionRecord` values from the specified partition after the cursor position, ordered by `(run_key, transition_seq)`.
2. WHEN `read_from` returns records, THE DsqlStore SHALL return an updated cursor pointing past the last returned record.
3. WHEN no records exist after the cursor position, THE DsqlStore SHALL return an empty batch with the same cursor.

### Requirement 6.3: Projector Checkpoint Table

**User Story:** As a Tokeira developer, I want a `projector_checkpoint` table in DSQL, so that each projection sink can track its consumption progress per substream independently.

#### Acceptance Criteria

1. THE DsqlStore SHALL define a `projector_checkpoint` table with columns for sink_id, partition_id, fanout, and last_applied_cursor.
2. THE DsqlStore SHALL support atomic checkpoint advancement (read current cursor, apply batch, write new cursor) to prevent duplicate application.
3. WHEN a sink crashes and restarts, THE DsqlStore SHALL allow the sink to resume from its last checkpointed cursor.

### Requirement 6.4: Visibility Execution Table

**User Story:** As a Tokeira developer, I want a `vis_execution` table in DSQL, so that the canonical visibility sink can maintain materialized rows for Temporal-compatible list/filter/count queries.

#### Acceptance Criteria

1. THE DsqlStore SHALL define a `vis_execution` table with columns for run_key, namespace_id, workflow_id, run_id, workflow_type, task_queue, execution_status, start_time, execution_time, close_time, history_length, state_transition_count, and memo.
2. WHEN the canonical visibility sink processes a `ProjectionOp::UpsertExecution`, THE DsqlStore SHALL upsert the corresponding `vis_execution` row.
3. WHEN the canonical visibility sink processes a `ProjectionOp::CloseExecution`, THE DsqlStore SHALL update the `vis_execution` row with the terminal status and close_time.
4. THE `vis_execution` table SHALL support namespace-scoped queries ordered by `(close_time DESC NULLS FIRST, start_time DESC, run_key DESC)` for stable pagination.

### Requirement 6.5: Projection Write Within Commit Transaction

**User Story:** As a Tokeira developer, I want projection log entries written within the same transaction as the authoritative commit, so that projection data is never ahead of or missing from committed transitions.

#### Acceptance Criteria

1. WHEN `commit_transition` succeeds and the transition contains `ProjectionOp` entries, THE DsqlStore SHALL have appended the corresponding `projection_log` records within the same DSQL transaction.
2. THE projection log write SHALL NOT be a separate asynchronous operation; it is part of the atomic commit.
