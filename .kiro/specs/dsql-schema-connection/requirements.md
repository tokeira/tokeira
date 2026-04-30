# Requirements Document

## Introduction

This spec covers Feature 1 (Schema and Connection Foundation) from the umbrella `dsql-storage-implementation` spec. It is the foundational feature — all other DSQL features depend on it.

The scope is:

1. **DSQL Schema DDL** — complete table definitions for all 11 tables across core, delivery, and projection domains, designed from scratch for Tokeira's architecture and DSQL constraints.
2. **Schema Migration Tooling** — forward-only migration runner that applies DDL separately from runtime DML traffic.
3. **Connection Pool with Reservoir Pattern** — node-local connection management layered on the official `aurora-dsql-sqlx-connector`, adding reservoir buffering, class-based budget allocation, rate-limited creation, and proactive expiry scanning.
4. **IAM Token-Refreshing Authentication** — leveraging the official connector's built-in IAM token generation with role separation for admin, runtime, and read-only workloads.
5. **Primary Key Distribution Strategy** — UUID keys for hot-write tables, composite keys clustering by `run_key`, and fanout/hash dimensions for append-like tables.

The authoritative architecture documents are [050-dsql-storage](../../../docs/architecture/050-dsql-storage.md) and [060-connection-management](../../../docs/architecture/060-connection-management.md). The schema is NOT a port of the temporal-dsql compatibility schema — it is a clean design shaped by DSQL's actual constraints (no BIGSERIAL, no CHECK, no temp tables, UUID PKs, INDEX ASYNC, 3000-row mutation limit, OCC, single database per cluster).

The official `aurora-dsql-sqlx-connector` (v0.1.2) provides automatic IAM token generation, SQLx-based async PostgreSQL connectivity, connection pooling with background token refresh, and OCC retry helpers. What it does NOT provide (and this spec must deliver): reservoir pattern with proactive connection buffering, class-based connection budget allocation, rate-limited connection creation via token-bucket, proactive expiry scanning with guard window, in-flight semaphore for concurrent connection creation, and workload class degradation under pressure.

### Phased Delivery

- **Phase 1**: Schema DDL + migration tooling
- **Phase 2**: Connection pool with reservoir (layered on official connector)
- **Phase 3**: IAM authentication integration
- **Phase 4**: Schema validation and testing

### What This Spec Does NOT Cover

- `RunRepository` implementation against DSQL (Feature 2: `dsql-core-persistence`)
- Side table queries — activity, timer, Nexus sweep (Feature 3: `dsql-side-tables`)
- Shard lease management (Feature 4: `dsql-shard-leasing`)
- Dispatch backlog persistence (Feature 5: `dsql-dispatch-backlog`)
- Projection persistence (Feature 6: `dsql-projection-persistence`)
- Cluster-wide `BudgetAllocator` in DynamoDB (deferred to connection-budget-allocator spec)
- Multi-cluster placement (deferred to 037-dynamic-placement)

## Glossary

- **DSQL**: Amazon Aurora DSQL — a PostgreSQL-compatible serverless distributed SQL database with fixed Repeatable Read isolation and optimistic concurrency control.
- **DsqlStore**: The production DSQL storage backend to be implemented in `tokeira-storage`, replacing `InMemoryStore` for production deployments.
- **ConnectionDirector**: The trait in `tokeira-storage/src/api.rs` for class-based connection budget control, enforcing per-class connection limits and open-rate budgets.
- **DbClass**: Enumerated workload class (Control, Commit, Read, Projection, Maintenance) used for connection budget prioritization. Priority order: Control > Commit > Read > Projection > Maintenance.
- **DbPermit**: A held connection permit scoped to a `DbClass`, returned by `ConnectionDirector::acquire`.
- **Reservoir_Pattern**: Channel-based connection buffer with continuous refiller task, proactive expiry scanner, guard window before hard lifetime cutoff, and in-flight semaphore for managing DSQL connection lifecycle.
- **Official_Connector**: The `aurora-dsql-sqlx-connector` crate (v0.1.2) from `awslabs/aurora-dsql-connectors` providing SQLx-based DSQL connectivity with IAM token refresh and OCC retry helpers.
- **Token_Bucket**: Rate-limiting algorithm for connection creation, respecting DSQL's 100 connections/second sustained rate and 1,000 burst capacity.
- **Guard_Window**: Configurable time period before DSQL's 60-minute hard connection lifetime cutoff during which the reservoir proactively retires connections.
- **RunKey**: UUID-based durable storage key for a workflow run, used as primary key in `workflow_hot` and clustering key in related tables.
- **Fanout_Key**: Hash-based partition dimension prepended to append-like tables (`dispatch_backlog`, `projection_log`) to avoid hot write partitions in DSQL.
- **INDEX_ASYNC**: DSQL's non-blocking index creation mechanism that avoids blocking base-table DML during index builds.
- **Migration_Tooling**: Forward-only schema migration runner that applies DDL separately from runtime DML traffic and tracks applied versions.
- **Shard_Lease**: Table tracking shard ownership with epoch fencing for single-writer guarantees.
- **Current_Execution**: Mapping table from `(namespace_id, workflow_id)` to the current run identity and open/closed status.
- **Workflow_Hot**: Small current summary row per open run, containing the compact `WorkflowState`.
- **History_Batch**: Immutable append-only event batch table, each row containing a contiguous range of history events from one transition.
- **Request_Dedupe**: Idempotency record table for external command deduplication.
- **Activity_State**: Normalized current state table for open activities, keyed by `(run_key, schedule_event_id)`.
- **Timer_Bucket**: Bucketed wakeup record table for due-time scanning, keyed by `(shard_id, fire_at, run_key, timer_id)`.
- **Dispatch_Backlog**: Durable fallback table for unmatched tasks, with fanout/hash dimension for write distribution.
- **Projection_Log**: Typed durable mutation log consumed by projection sinks, with fanout/hash dimension.
- **Projector_Checkpoint**: Per-sink, per-substream cursor tracking table for projection consumer progress.
- **Vis_Execution**: Materialized visibility row store for Temporal-compatible list/filter/count queries.

## Requirements

### Requirement 1: Core Table DDL

**User Story:** As a Tokeira operator, I want DDL definitions for all 7 core tables, so that the foundational persistence schema can be created in an Aurora DSQL cluster.

#### Acceptance Criteria

1. THE DDL SHALL define a `shard_lease` table with columns for `shard_id` (UUID, primary key), `owner` (TEXT), `epoch` (BIGINT), and `lease_expiry` (TIMESTAMPTZ).
2. THE DDL SHALL define a `current_execution` table with columns for `namespace_id` (UUID), `workflow_id` (TEXT), `run_key` (UUID), `run_id` (TEXT), `is_open` (BOOLEAN), and `created_at` (TIMESTAMPTZ), with primary key `(namespace_id, workflow_id)`.
3. THE DDL SHALL define a `workflow_hot` table with columns for `run_key` (UUID, primary key), `namespace_id` (UUID), `workflow_id` (TEXT), `shard_id` (UUID), `transition_seq` (BIGINT), `state_data` (BYTEA for serialized `WorkflowState`), and `updated_at` (TIMESTAMPTZ).
4. THE DDL SHALL define a `history_batch` table with columns for `run_key` (UUID), `first_event_id` (BIGINT), `last_event_id` (BIGINT), `transition_seq` (BIGINT), `events_data` (BYTEA for serialized events), and `created_at` (TIMESTAMPTZ), with primary key `(run_key, first_event_id)`.
5. THE DDL SHALL define a `request_dedupe` table with columns for `namespace_id` (UUID), `workflow_id` (TEXT), `request_id` (TEXT), `run_key` (UUID), `first_seen_transition_seq` (BIGINT), and `created_at` (TIMESTAMPTZ), with primary key `(namespace_id, workflow_id, request_id)`.
6. THE DDL SHALL define an `activity_state` table with columns for `run_key` (UUID), `schedule_event_id` (BIGINT), `shard_id` (UUID), `activity_id` (TEXT), `queue_namespace` (UUID), `queue_name` (TEXT), `attempt` (INTEGER), `state_data` (BYTEA for serialized activity state), and `updated_at` (TIMESTAMPTZ), with primary key `(run_key, schedule_event_id)`.
7. THE DDL SHALL define a `timer_bucket` table with columns for `shard_id` (UUID), `fire_at` (TIMESTAMPTZ), `run_key` (UUID), `timer_id` (TEXT), `timer_data` (BYTEA for serialized timer state), and `created_at` (TIMESTAMPTZ), with primary key `(shard_id, fire_at, run_key, timer_id)`.
8. THE DDL SHALL NOT use BIGSERIAL, CHECK constraints, or temporary tables in any core table definition.

### Requirement 2: Delivery and Projection Table DDL

**User Story:** As a Tokeira operator, I want DDL definitions for the delivery and projection tables, so that dispatch backlog, projection log, projector checkpoints, and visibility rows can be persisted in DSQL.

#### Acceptance Criteria

1. THE DDL SHALL define a `dispatch_backlog` table with columns for `partition_id` (INTEGER, hash-based fanout), `queue_namespace` (UUID), `queue_name` (TEXT), `insertion_seq` (BIGINT), `run_key` (UUID), `payload_data` (BYTEA), and `scheduled_at` (TIMESTAMPTZ), with primary key `(partition_id, queue_namespace, queue_name, insertion_seq)`.
2. THE DDL SHALL define a `projection_log` table with columns for `partition_id` (INTEGER, hash-based fanout), `fanout` (SMALLINT), `run_key` (UUID), `transition_seq` (BIGINT), `context_data` (BYTEA), `ops_data` (BYTEA), and `created_at` (TIMESTAMPTZ), with primary key `(partition_id, fanout, run_key, transition_seq)`.
3. THE DDL SHALL define a `projector_checkpoint` table with columns for `sink_id` (TEXT), `partition_id` (INTEGER), `fanout` (SMALLINT), `last_applied_cursor` (BYTEA), and `updated_at` (TIMESTAMPTZ), with primary key `(sink_id, partition_id, fanout)`.
4. THE DDL SHALL define a `vis_execution` table with columns for `run_key` (UUID, primary key), `namespace_id` (UUID), `workflow_id` (TEXT), `run_id` (TEXT), `workflow_type` (TEXT), `task_queue` (TEXT), `execution_status` (SMALLINT), `start_time` (TIMESTAMPTZ), `execution_time` (TIMESTAMPTZ), `close_time` (TIMESTAMPTZ), `history_length` (BIGINT), `state_transition_count` (BIGINT), and `memo` (BYTEA).
5. THE DDL SHALL NOT use BIGSERIAL, CHECK constraints, or temporary tables in any delivery or projection table definition.

### Requirement 3: Secondary Index Definitions

**User Story:** As a Tokeira operator, I want secondary indexes defined for query-critical access patterns, so that the runtime can efficiently query by shard, queue, time range, and namespace without full table scans.

#### Acceptance Criteria

1. THE DDL SHALL define an index on `workflow_hot(shard_id)` to support shard-filtered sweep queries.
2. THE DDL SHALL define an index on `activity_state(shard_id)` to support shard-filtered activity sweep queries.
3. THE DDL SHALL define an index on `activity_state(queue_namespace, queue_name)` to support queue-filtered dispatchable activity queries.
4. THE DDL SHALL define an index on `timer_bucket(shard_id, fire_at)` to support shard-filtered due-timer range scans.
5. THE DDL SHALL define an index on `vis_execution(namespace_id, close_time DESC NULLS FIRST, start_time DESC, run_key DESC)` to support namespace-scoped visibility list queries with stable pagination.
6. THE DDL SHALL define an index on `vis_execution(namespace_id, workflow_type)` to support workflow-type filtered visibility queries.
7. ALL secondary index creation statements SHALL use INDEX ASYNC to avoid blocking base-table DML operations during index builds.

### Requirement 4: Primary Key Distribution Strategy

**User Story:** As a Tokeira developer, I want primary keys designed for DSQL's distributed storage model, so that write traffic is spread across partitions and avoids hot spots.

#### Acceptance Criteria

1. THE DDL SHALL use UUID-typed primary keys for `workflow_hot`, `shard_lease`, and `vis_execution` to distribute writes across DSQL partitions.
2. THE DDL SHALL use composite key `(namespace_id, workflow_id)` for `current_execution` to cluster by namespace and enable efficient workflow-id lookups.
3. THE DDL SHALL use composite key `(namespace_id, workflow_id, request_id)` for `request_dedupe` to cluster by workflow scope.
4. THE DDL SHALL use composite key `(run_key, first_event_id)` for `history_batch` to cluster events by run.
5. THE DDL SHALL use composite key `(run_key, schedule_event_id)` for `activity_state` to cluster by run.
6. THE DDL SHALL use composite key `(shard_id, fire_at, run_key, timer_id)` for `timer_bucket` to enable efficient shard-filtered time-range scans.
7. THE DDL SHALL prepend a hash-based `partition_id` to `dispatch_backlog` and `projection_log` primary keys to distribute append traffic across multiple DSQL write ranges.
8. THE DDL SHALL NOT use monotonically increasing keys (BIGSERIAL, auto-increment) as leading primary key columns on any table.

### Requirement 5: DDL Constraint Compliance

**User Story:** As a Tokeira developer, I want the schema DDL to comply with all DSQL-specific constraints, so that table creation succeeds on Aurora DSQL clusters without compatibility errors.

#### Acceptance Criteria

1. THE DDL SHALL NOT contain any CHECK constraints; although DSQL supports CHECK in CREATE TABLE, Tokeira uses application-level validation in Rust for testability and flexibility.
2. THE DDL SHALL NOT contain any temporary table definitions; CTE-based query compilation SHALL be used where temporary staging is needed.
3. THE DDL SHALL NOT contain any BIGSERIAL or SERIAL column types; application-generated identifiers (UUID, Snowflake ID) SHALL be used instead.
4. THE DDL SHALL NOT contain any foreign key constraints for MVP; referential integrity SHALL be maintained by application logic.
5. THE DDL SHALL fit within a single DSQL database and single schema (public) for MVP.
6. THE DDL SHALL NOT contain any PL/pgSQL functions or triggers; all behavioral logic SHALL reside in application code.

### Requirement 6: Schema Migration Tooling

**User Story:** As a Tokeira operator, I want a forward-only schema migration tool, so that I can create and evolve the DSQL schema safely in production without interfering with runtime traffic.

#### Acceptance Criteria

1. THE Migration_Tooling SHALL apply DDL migrations in a dedicated connection separate from runtime DML traffic.
2. THE Migration_Tooling SHALL maintain a `schema_version` tracking table that records each applied migration's version number, name, applied timestamp, and checksum.
3. WHEN a migration is applied, THE Migration_Tooling SHALL insert a record into `schema_version` to prevent re-application.
4. WHEN the Migration_Tooling detects that a migration version has already been applied, THE Migration_Tooling SHALL skip that migration and log a message.
5. THE Migration_Tooling SHALL apply migrations in strict version order, refusing to apply version N+1 if version N has not been applied.
6. IF a migration fails partway through, THEN THE Migration_Tooling SHALL report the failure state including the migration version, the failing statement, and the error message, and allow manual recovery.
7. THE Migration_Tooling SHALL support a dry-run mode that prints the SQL statements that would be executed without applying them.
8. THE Migration_Tooling SHALL support forward-only migrations for MVP; rollback is a manual operator procedure.

### Requirement 7: Migration File Organization

**User Story:** As a Tokeira developer, I want migration files organized in a predictable directory structure with versioned naming, so that migrations are discoverable, auditable, and applied in deterministic order.

#### Acceptance Criteria

1. THE Migration_Tooling SHALL read migration files from a `migrations/` directory within the `tokeira-storage` crate.
2. THE Migration_Tooling SHALL require migration files to follow the naming convention `V{version}__{description}.sql` where `{version}` is a zero-padded integer and `{description}` is a snake_case name.
3. THE Migration_Tooling SHALL parse the version number from the filename and apply migrations in ascending version order.
4. THE Migration_Tooling SHALL compute a checksum of each migration file and store it in `schema_version` to detect tampering with previously applied migrations.
5. IF a previously applied migration's file checksum does not match the stored checksum, THEN THE Migration_Tooling SHALL refuse to proceed and report the mismatch.

### Requirement 8: Connection Pool with Reservoir Pattern

**User Story:** As a Tokeira developer, I want a node-local connection pool that implements the reservoir pattern on top of the official DSQL connector, so that the system maintains a buffer of ready connections and avoids reconnection storms under DSQL's rate limits.

#### Acceptance Criteria

1. THE ConnectionDirector SHALL use the Official_Connector (`aurora-dsql-sqlx-connector`) as the underlying driver for creating DSQL connections with IAM token authentication.
2. THE ConnectionDirector SHALL implement a Reservoir: an async channel-based buffer that holds pre-created, validated connections ready for immediate checkout.
3. THE Reservoir SHALL maintain a configurable target number of ready connections (default 50) and run a continuous refiller task that creates new connections when the ready count drops below the target.
4. THE Reservoir SHALL enforce an in-flight semaphore (default limit 8) to cap the number of concurrent connection creation attempts, preventing burst overload of the DSQL endpoint.
5. THE Reservoir SHALL run a proactive expiry scanner that retires connections approaching the DSQL 60-minute hard lifetime cutoff, using a configurable guard window (default 45 seconds before expiry).
6. THE Reservoir SHALL assign each connection a base lifetime of 50–55 minutes with per-connection jitter to prevent mass recycling events.
7. WHEN a connection is returned to the Reservoir after use, THE Reservoir SHALL validate the connection is still alive and within its lifetime before making it available for reuse; expired or broken connections SHALL be discarded.

### Requirement 9: Node-Local Connection Rate Limiting

**User Story:** As a Tokeira developer, I want connection creation rate-limited at the node level to respect DSQL's cluster-wide 100 connections/second sustained rate, so that a single node does not overwhelm the DSQL endpoint.

#### Acceptance Criteria

1. THE ConnectionDirector SHALL implement a node-local token-bucket rate limiter for new connection creation, enforcing a configurable sustained rate and burst capacity.
2. WHEN a new connection needs to be created and the local token bucket is exhausted, THE ConnectionDirector SHALL wait until tokens become available rather than exceeding the rate limit.
3. FOR single-node deployments (local or compose platform with no distributed coordination configured), THE rate limiter SHALL use the full cluster-wide budget (default: 100/sec sustained, 1,000 burst).
4. THE rate limiter SHALL expose a `reconfigure(rate, capacity)` method so a future distributed coordination backend can adjust the per-node share at runtime. This spec implements the method; the backend that calls it is deferred to the `connection-budget-allocator` spec.
5. THE node-local rate limiter SHALL always be active when DSQL storage is selected, regardless of whether distributed coordination is configured.

### Requirement 9b: Distributed Rate Coordination Interface (Deferred)

**User Story:** As a Tokeira operator running a multi-node deployment, I want the rate limiter to support future distributed coordination, so that the cluster-wide DSQL rate limit can be respected across nodes without manual per-node configuration.

#### Acceptance Criteria

1. THE `TokenBucketRateLimiter` SHALL expose a `reconfigure(rate_per_second, capacity)` method that a future distributed coordination backend can call to adjust the per-node share at runtime.
2. WHEN no coordination backend is configured (the default), THE rate limiter SHALL use the full cluster-wide budget as set at construction.
3. THE distributed coordination backend itself (DynamoDB-backed token bucket, node discovery, budget allocation) is NOT implemented in this spec. It is deferred to the `connection-budget-allocator` spec.

### Requirement 10: Class-Based Connection Budget

**User Story:** As a Tokeira developer, I want connections allocated by workload class with priority-based degradation, so that critical control and commit traffic is protected when the connection budget is exhausted.

#### Acceptance Criteria

1. THE ConnectionDirector SHALL accept a `DbClass` parameter on every `acquire` call and return a permit (`Self::Permit`) scoped to that class. For the in-memory backend this is `DbPermit` (a no-op marker); for DSQL this is `DsqlPermit` (carrying a real connection).
2. THE ConnectionDirector SHALL enforce per-class connection limits that sum to the node's total connection budget.
3. WHEN the total connection budget is exhausted, THE ConnectionDirector SHALL degrade workload classes in priority order: stop Maintenance first, then Projection, then throttle Read, while protecting Control and Commit traffic.
4. THE ConnectionDirector SHALL support runtime reconfiguration of per-class budget allocations without requiring a restart.
5. WHEN a permit is dropped, THE ConnectionDirector SHALL return the connection to the Reservoir for reuse or discard it if expired. For `DsqlPermit`, the connection field is `Option<PoolConnection>` so `Drop` can `take()` ownership.

### Requirement 11: Connection Lifecycle Management

**User Story:** As a Tokeira developer, I want connections recycled before DSQL's hard lifetime cutoff with jitter to prevent thundering herd, so that the pool maintains healthy connections without mass recycling events.

#### Acceptance Criteria

1. THE ConnectionDirector SHALL track the creation timestamp of each connection and retire connections that exceed their assigned lifetime (base 50–55 minutes plus per-connection jitter).
2. THE ConnectionDirector SHALL NOT allow any connection to reach DSQL's 60-minute hard cutoff; the Guard_Window (default 45 seconds) ensures connections are retired before the server forcibly closes them.
3. WHEN a connection is retired, THE ConnectionDirector SHALL close the connection gracefully and signal the refiller task to create a replacement.
4. THE ConnectionDirector SHALL add per-connection jitter (drawn from a uniform distribution over the configured jitter range) to the base lifetime to spread recycling events across time.
5. IF a connection is detected as broken during checkout or return (network error, server-side close), THEN THE ConnectionDirector SHALL discard the connection immediately and signal the refiller task.

### Requirement 12: IAM Token-Refreshing Authentication

**User Story:** As a Tokeira operator, I want DSQL connections to authenticate using IAM tokens that refresh automatically via the official connector, so that long-running nodes maintain valid database access without manual credential rotation.

#### Acceptance Criteria

1. THE ConnectionDirector SHALL use the Official_Connector's built-in IAM token generation and background refresh mechanism for all new DSQL connections.
2. THE ConnectionDirector SHALL configure the Official_Connector with the DSQL cluster endpoint and AWS region, supporting region auto-detection from the cluster hostname.
3. WHEN the Official_Connector's background token refresh fails, THE ConnectionDirector SHALL log the failure and continue using existing valid connections until refresh succeeds.
4. THE ConnectionDirector SHALL support configurable IAM role ARNs for role separation: admin/migration connections, runtime connections, and read-only connections.
5. THE ConnectionDirector SHALL NOT implement custom token caching or refresh logic; the Official_Connector's `pool` feature handles token lifecycle.

### Requirement 13: Connection Pool Observability

**User Story:** As a Tokeira operator, I want metrics exposed for connection pool health, so that I can monitor reservoir utilization, checkout latency, and connection lifecycle events in production.

#### Acceptance Criteria

1. THE ConnectionDirector SHALL expose a metric for current pool utilization: total connections, idle connections, in-use connections, and connections pending creation.
2. THE ConnectionDirector SHALL expose a metric for checkout latency (time from `acquire` call to permit return) with per-class breakdown.
3. THE ConnectionDirector SHALL expose a counter for empty-reservoir events (when a checkout finds no ready connections and must wait for creation).
4. THE ConnectionDirector SHALL expose counters for connection lifecycle events: connections created, connections retired (lifetime expiry), connections discarded (broken/error), and connections returned.
5. THE ConnectionDirector SHALL expose the current refill rate and token-bucket state (available tokens, current sustained rate).
6. THE ConnectionDirector SHALL expose per-class budget utilization: allocated budget, current in-use count, and queued waiters per `DbClass`.

### Requirement 14: DDL Serialization Format

**User Story:** As a Tokeira developer, I want the serialization format for BYTEA columns defined and documented, so that all DSQL features use a consistent encoding for `WorkflowState`, history events, activity state, timer state, projection ops, and backlog payloads.

#### Acceptance Criteria

1. THE DsqlStore SHALL use `postcard` (compact binary serde encoding) for all BYTEA columns across all tables, documented in the migration file comments. This spec adds `Serialize, Deserialize` derives to the domain types that do not yet have them (see Task 1.3).
2. THE DsqlStore SHALL define Rust serialization/deserialization functions for each BYTEA column type: `WorkflowState`, `Vec<HistoryEvent>`, `ActivityState`, `TimerState`, `ProjectionContext`, `Vec<ProjectionOp>`, `BacklogPayload`, and `ProjectionCursor`. These functions depend on the serde derives added in Task 1.3.
3. FOR ALL serializable types, serializing then deserializing SHALL produce a value equal to the original (round-trip property).
4. THE serialization format choice SHALL be documented in the first migration file and in the crate-level documentation for `tokeira-storage`.

### Requirement 15: Schema Validation with DSQL Power

**User Story:** As a Tokeira developer, I want the schema DDL validated against DSQL compatibility rules before deployment, so that incompatible constructs are caught during development rather than at migration time.

#### Acceptance Criteria

1. THE Migration_Tooling SHALL support a `validate` subcommand that checks all migration files against known DSQL constraints: no BIGSERIAL, no CHECK, no temp tables, no PL/pgSQL, no foreign keys in hot path, INDEX ASYNC for secondary indexes.
2. WHEN validation detects a non-compliant construct, THE Migration_Tooling SHALL report the migration file, line number, and specific violation.
3. THE validation SHALL verify that all primary keys on hot-write tables use UUID or composite keys with a UUID leading column, not monotonically increasing types.
4. THE validation SHALL verify that all secondary index creation statements use the ASYNC keyword.
