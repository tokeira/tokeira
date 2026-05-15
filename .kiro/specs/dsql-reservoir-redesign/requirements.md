# Requirements Document

## Introduction

The DSQL reservoir redesign eliminates the sqlx PgPool from the connection path entirely. The current implementation wraps a sqlx PgPool inside the reservoir: the refiller calls `pool.acquire()` to create connections, but the pool's `max_connections` equals `target_ready`, so when the reservoir holds all connections in its ready channel, the pool has nothing left for the refiller — causing `pool timed out` errors under load.

The redesign makes the reservoir the sole connection owner. Physical connections are created directly via IAM-authenticated TCP/TLS (using `aurora_dsql_sqlx_connector`), bypassing the sqlx pool entirely. The reservoir IS the pool. A background refiller creates connections respecting DSQL's cluster-wide rate limit. For multi-node deployments, DynamoDB-backed coordination distributes the rate budget and connection count across nodes.

All connection management parameters are internal constants derived from DSQL's known constraints. The operator provides an endpoint and credentials. Everything else is automatic.

## Glossary

- **Reservoir**: The channel-based connection buffer that holds pre-created physical DSQL connections ready for immediate checkout.
- **Refiller**: A background tokio task that continuously creates new physical connections to maintain the reservoir at its target level.
- **Expiry_Scanner**: A background tokio task that periodically inspects the ready channel and retires connections approaching their IAM token expiry.
- **Return_Processor**: A background tokio task that validates returned connections via lifetime and bad-flag checks before placing them back in the ready channel.
- **Connection_Factory**: The component that creates raw `PgConnection` instances via IAM-authenticated TCP/TLS without using a sqlx pool.
- **Token_Bucket**: A rate limiter that enforces DSQL's cluster-wide connection creation rate (100/sec sustained, 1000 burst).
- **Distributed_Token_Bucket**: A DynamoDB-backed token bucket that coordinates the cluster-wide connection creation rate across multiple nodes.
- **Slot_Block_Manager**: A DynamoDB-backed allocator that partitions the 10,000 connection limit across nodes using block-based allocation.
- **Class_Budget**: A semaphore-based admission control layer that reserves connection capacity for each operation class (commit, read, projection, control, maintenance).
- **Guard_Window**: The time period before a connection's lifetime expiry during which the connection is retired rather than handed out.
- **Lifetime_Jitter**: A random offset applied to each connection's base lifetime to prevent synchronized retirement storms.
- **In_Flight_Semaphore**: A semaphore that limits the number of concurrent connection creation attempts to prevent TCP/TLS handshake pile-ups.
- **IAM_Token_Provider**: The component that generates fresh IAM authentication tokens for each new DSQL connection.
- **Ready_Channel**: The bounded async channel that holds validated, ready-to-use connections.
- **Backpressure**: The signal returned to callers when the reservoir is empty, indicating the system is at capacity.

## Requirements

### Requirement 1: Direct Connection Factory

**User Story:** As a tokeira operator, I want the reservoir to create physical connections directly via IAM auth, so that connection creation is not constrained by a secondary pool's internal limits.

#### Acceptance Criteria

1. THE Connection_Factory SHALL create raw `PgConnection` instances using `aurora_dsql_sqlx_connector` IAM authentication without instantiating or acquiring from a sqlx PgPool.
2. WHEN the Connection_Factory creates a connection, THE Connection_Factory SHALL generate a fresh IAM token for that specific connection attempt.
3. THE Connection_Factory SHALL establish TCP/TLS connections directly to the DSQL endpoint specified in the operator-provided configuration.
4. IF the Connection_Factory fails to create a connection, THEN THE Connection_Factory SHALL return a typed error indicating the failure category (TLS, timeout, refused, IAM).

### Requirement 2: Reservoir as Sole Connection Owner

**User Story:** As a tokeira developer, I want the reservoir to be the only component that creates and holds DSQL connections, so that there is no conflict between connection management layers.

#### Acceptance Criteria

1. THE Reservoir SHALL hold physical connections in a bounded async channel (the Ready_Channel).
2. WHEN a caller checks out a connection, THE Reservoir SHALL receive the connection from the Ready_Channel.
3. WHEN a caller returns a connection, THE Reservoir SHALL send the connection to the Return_Processor for validation before placing it back in the Ready_Channel.
4. THE Reservoir SHALL be the only component in the tokeira process that creates or holds DSQL connections.
5. THE Reservoir SHALL not depend on or instantiate a sqlx PgPool for connection management.

### Requirement 3: Non-Blocking Checkout with Backpressure

**User Story:** As a tokeira runtime developer, I want connection checkout to return immediately when the reservoir is empty, so that the hot path never blocks on connection creation.

#### Acceptance Criteria

1. WHEN the Ready_Channel contains at least one connection, THE Reservoir SHALL return a connection immediately via channel receive.
2. WHEN the Ready_Channel is empty, THE Reservoir SHALL return a backpressure error immediately without blocking.
3. THE Reservoir SHALL emit a metric each time a checkout encounters an empty Ready_Channel.
4. THE Class_Budget holder (the caller) SHALL handle the backpressure error via retry or propagation to the upstream operation.

### Requirement 4: Background Refiller

**User Story:** As a tokeira operator, I want a background task to continuously maintain the reservoir at its target level, so that connections are always available for checkout without hot-path creation latency.

#### Acceptance Criteria

1. THE Refiller SHALL run as a dedicated tokio task that continuously creates connections to maintain the Ready_Channel at `target_ready` (50) connections.
2. WHILE the Ready_Channel contains fewer connections than `target_ready`, THE Refiller SHALL attempt to create new connections.
3. WHEN the Refiller creates a connection, THE Refiller SHALL acquire resources in this order: `In_Flight_Semaphore`, `Slot_Block_Manager.acquire_slot()`, `Distributed_Token_Bucket.wait()`, then `Connection_Factory.create_connection()`.
4. IF rate limiting or connection creation fails after a slot is reserved, THEN THE Refiller SHALL call `Slot_Block_Manager.release_slot()` before backing off.
5. IF the Connection_Factory returns an error, THEN THE Refiller SHALL back off with exponential delay before retrying.
6. THE Refiller SHALL assign each new connection a jittered lifetime of `base_lifetime` ± `lifetime_jitter`.
7. THE Refiller SHALL record the creation timestamp on each connection for lifetime enforcement.
8. IF the Refiller creates a connection but cannot place it into the Ready_Channel, THEN THE Refiller SHALL call `Slot_Block_Manager.release_slot()` exactly once for that physical connection before dropping it.

### Requirement 5: Expiry Scanner

**User Story:** As a tokeira operator, I want near-expired connections to be proactively retired from the ready channel, so that callers never receive a connection that will expire mid-transaction.

#### Acceptance Criteria

1. THE Expiry_Scanner SHALL run as a dedicated tokio task that periodically scans the Ready_Channel at `scan_interval` (1 second).
2. WHEN the Expiry_Scanner finds a connection whose elapsed age plus `guard_window` (45 seconds) exceeds its assigned lifetime, THE Expiry_Scanner SHALL retire that connection by dropping it.
3. THE Expiry_Scanner SHALL emit a metric for each connection retired due to guard window proximity.
4. THE Expiry_Scanner SHALL not drain the entire Ready_Channel in a single scan pass; scanning SHALL be bounded to prevent starvation of concurrent checkout callers.
5. WHEN the Expiry_Scanner retires a connection, THE Expiry_Scanner SHALL call `Slot_Block_Manager.release_slot()` for that physical connection.
6. IF the Expiry_Scanner cannot place a still-healthy scanned connection back into the Ready_Channel, THEN THE Expiry_Scanner SHALL call `Slot_Block_Manager.release_slot()` exactly once before dropping that physical connection.

### Requirement 6: Return Processor

**User Story:** As a tokeira developer, I want returned connections to be validated before reuse, so that callers never receive a broken connection from the reservoir.

#### Acceptance Criteria

1. THE Return_Processor SHALL run as a dedicated tokio task that receives connections from the return channel.
2. WHEN the Return_Processor receives a connection, THE Return_Processor SHALL check whether the connection is within the guard window; connections within the guard window SHALL be discarded.
3. WHEN the Return_Processor receives a connection, THE Return_Processor SHALL validate it using only the lifetime check and caller-provided bad-flag.
4. IF the connection is outside the guard window and is not marked bad, THEN THE Return_Processor SHALL place the connection back in the Ready_Channel.
5. IF the connection is marked bad, THEN THE Return_Processor SHALL discard the connection and emit a metric indicating the failure reason.
6. IF the Return_Processor discards a returned connection, THEN THE Return_Processor SHALL call `Slot_Block_Manager.release_slot()` for that physical connection.
7. IF the Return_Processor cannot place a reusable returned connection back into the Ready_Channel, THEN THE Return_Processor SHALL call `Slot_Block_Manager.release_slot()` exactly once before dropping that physical connection.

### Requirement 7: Distributed Token Bucket as Sole Rate Limiter

**User Story:** As a tokeira developer, I want the DynamoDB-backed distributed token bucket to be the sole rate limiter for connection creation, so that there is one authoritative coordination point for the cluster-wide 100/sec DSQL rate limit.

#### Acceptance Criteria

1. THE Distributed_Token_Bucket SHALL be the only rate limiter in the connection creation path. There SHALL NOT be a separate local token bucket.
2. THE Refiller SHALL acquire a token from the Distributed_Token_Bucket before each connection creation attempt.
3. THE Distributed_Token_Bucket SHALL enforce DSQL's cluster-wide sustained rate of 100 connections per second with a burst capacity of 1,000 connections.
4. THE Distributed_Token_Bucket SHALL use a DynamoDB table with a single item per DSQL endpoint, tracking token count and last refill timestamp.
5. THE Distributed_Token_Bucket SHALL use atomic conditional updates (DynamoDB condition expressions) to prevent race conditions between nodes.

### Requirement 8: Distributed Slot Block Manager

**User Story:** As a tokeira developer, I want the 10,000 connection limit to be partitioned across nodes using block-based allocation, so that no single node can consume the entire cluster's connection budget.

#### Acceptance Criteria

1. THE Slot_Block_Manager SHALL use a DynamoDB table to allocate blocks of connection slots (100 slots per block) to individual nodes.
2. WHEN a node acquires a slot block, THE Slot_Block_Manager SHALL record the allocation with a TTL for crash recovery.
3. THE Refiller SHALL only create connections within the node's allocated slot budget (number of acquired blocks × block size).
4. THE Slot_Block_Manager SHALL periodically renew its block leases to prevent expiry during normal operation.
5. IF a node crashes, THEN THE Slot_Block_Manager SHALL release the node's blocks via TTL expiry, making them available to other nodes.
6. THE Slot_Block_Manager SHALL use a separate DynamoDB table from the Distributed_Token_Bucket, with the slot table dedicated to connection slot allocation.
7. WHEN the DynamoDB slot block table does not exist or is unreachable at startup, THE Reservoir SHALL fail fast with a clear error.
8. WHEN slot block renewal fails because ownership was lost, THE Slot_Block_Manager SHALL remove the block locally, reduce total slot capacity by one block, emit `tokeira_dsql_slot_block_lost_total`, and continue operating with reduced capacity.

### Requirement 9: Class-Based Admission Control

**User Story:** As a tokeira developer, I want operation classes to have reserved connection budgets, so that projection bursts cannot starve commit operations.

#### Acceptance Criteria

1. THE Class_Budget SHALL allocate semaphore permits from `target_ready` as: commit 50%, read 20%, projection 10%, control 10%, maintenance 10%.
2. THE Class_Budget allocation SHALL be derived from `target_ready` as internal constants; the allocation SHALL NOT be operator-configurable.
3. WHEN a caller acquires a connection, THE Class_Budget SHALL first require a semaphore permit for the caller's operation class before checking out from the Ready_Channel.
4. FOR ALL valid system states, the projection class SHALL NOT be able to acquire permits reserved for the commit class (class isolation invariant).
5. THE Class_Budget SHALL emit metrics for each class: total permits, in-use permits, and wait duration.

### Requirement 10: IAM Token Provider

**User Story:** As a tokeira developer, I want fresh IAM tokens generated for each new connection, so that connections authenticate correctly without sharing expired tokens.

#### Acceptance Criteria

1. THE IAM_Token_Provider SHALL generate a fresh authentication token for each connection creation attempt by the Refiller.
2. THE IAM_Token_Provider SHALL NOT cache tokens across connection creation attempts.
3. THE IAM_Token_Provider SHALL use the DSQL endpoint and region from operator-provided configuration to generate tokens.
4. IF the IAM_Token_Provider fails to generate a token, THEN THE Connection_Factory SHALL propagate the error to the Refiller as a connection creation failure.

### Requirement 11: Connection Lifecycle Constants

**User Story:** As a tokeira operator, I want all connection lifecycle parameters to be derived from DSQL's known constraints, so that I do not need to configure or tune connection management.

#### Acceptance Criteria

1. THE Reservoir SHALL use the following internal constants derived from DSQL constraints: `target_ready` = 50, `base_lifetime` = 10 minutes, `lifetime_jitter` = ±2 minutes, `guard_window` = 45 seconds, `inflight_limit` = 8, `scan_interval` = 1 second.
2. THE Reservoir SHALL NOT expose connection lifecycle parameters as operator-configurable settings.
3. FOR ALL connections created by the Refiller, the assigned lifetime SHALL be within the range `[base_lifetime - lifetime_jitter, base_lifetime + lifetime_jitter]` (8 to 12 minutes).
4. FOR ALL connections created by the Refiller, the assigned lifetime plus guard_window SHALL NOT exceed the DSQL IAM token TTL of 15 minutes.

### Requirement 12: Graceful Degradation

**User Story:** As a tokeira operator, I want the system to degrade gracefully when the reservoir is empty and connections cannot be created, so that operations queue predictably rather than failing catastrophically.

#### Acceptance Criteria

1. THE system SHALL queue operations at the Class_Budget semaphore for admission before reservoir checkout.
2. WHEN the Reservoir is empty after class admission, THE caller SHALL release the class permit and signal backpressure immediately with `ReservoirError::Empty`.
3. WHILE the Reservoir is in a degraded state, THE Reservoir SHALL emit metrics exposing the backpressure: empty reservoir events, refiller in-flight count, and rate limiter tokens remaining.
4. WHEN the Refiller encounters repeated connection creation failures, THE Refiller SHALL apply exponential backoff up to a maximum delay without stopping the refill loop.
5. WHEN DSQL becomes available again after an outage, THE Refiller SHALL resume filling the reservoir without operator intervention.

### Requirement 13: Observability

**User Story:** As a tokeira operator, I want comprehensive metrics for the reservoir and its subsystems, so that I can monitor connection health and diagnose performance issues.

#### Acceptance Criteria

1. THE Reservoir SHALL emit the following metrics via the `metrics` crate: reservoir size (gauge), checkout latency (histogram), empty reservoir events (counter), refiller in-flight count (gauge), connection age at retirement (histogram), rate limiter tokens remaining (gauge), class budget utilization per class (gauge), and class permit wait duration (histogram).
2. THE Reservoir SHALL NOT expose metrics configuration as operator-configurable settings.
3. WHEN a connection is retired, THE Reservoir SHALL record the retirement reason (expired, guard_window, unhealthy, budget_cap) as a metric label.
4. THE Reservoir SHALL emit a metric for connection creation duration (histogram) to track IAM auth and TCP/TLS handshake latency.

### Requirement 14: DynamoDB Coordination for All DSQL Deployments

**User Story:** As a tokeira developer, I want the full DynamoDB-backed coordination (distributed token bucket and slot block manager) active for ALL DSQL deployments including compose, so that developers exercise the production coordination path locally and catch coordination bugs before they reach multi-node deployments.

#### Acceptance Criteria

1. WHEN the storage backend is DSQL, THE compose platform's IaC DSQL module SHALL provision the DynamoDB coordination tables (rate limiter table and slot block table) alongside the DSQL cluster.
2. WHEN the tokeira process starts with DSQL storage, THE Reservoir SHALL always use the Distributed_Token_Bucket and Slot_Block_Manager regardless of whether the deployment is single-node or multi-node.
3. THE Reservoir SHALL NOT have a "local-only" fallback mode for DSQL deployments. If the DynamoDB tables are unreachable, the Reservoir SHALL fail with a clear error rather than silently degrading to uncoordinated operation.
4. THE DynamoDB tables SHALL be provisioned with on-demand billing (pay-per-request) so they cost nothing when idle.
5. THE compose platform SHALL provision the DynamoDB tables in the same region as the DSQL cluster, using the same AWS credentials.
6. WHEN `tokeirad` starts with DSQL storage, THE DSQL startup path SHALL populate `DsqlCoordinationConfig` with a DynamoDB client plus table names derived from the effective project identifier: `{project}-dsql-rate-limiter` and `{project}-dsql-conn-lease`.
7. THE `DsqlStore::connect(auth, config)` startup path SHALL use `config.coordination.rate_limiter_table`, `config.coordination.conn_lease_table`, and `config.coordination.ddb_client` to construct the Distributed_Token_Bucket and Slot_Block_Manager.

### Requirement 15: Pool Warmup

**User Story:** As a tokeira operator, I want the reservoir to be fully warmed before the server accepts traffic, so that the first requests do not experience cold-start latency.

#### Acceptance Criteria

1. WHEN the tokeira process starts, THE Refiller SHALL fill the reservoir to `target_ready` before the server begins accepting gRPC traffic.
2. WHILE warming up, THE Refiller SHALL respect the Token_Bucket rate limiter (creating connections at the sustained rate of 100/sec).
3. THE warmup phase SHALL complete within a bounded time derived from `target_ready` and the rate limit (50 connections at 100/sec = 500ms minimum).
4. IF warmup cannot complete (DSQL unavailable), THEN THE Reservoir SHALL log a warning and allow the server to start with a partially filled reservoir rather than blocking indefinitely.

### Requirement 16: Connection Return with Lifetime Check

**User Story:** As a tokeira developer, I want returned connections to be checked for remaining lifetime before reuse, so that near-expired connections are discarded without adding per-return network overhead.

#### Acceptance Criteria

1. WHEN a connection is returned to the Reservoir, THE Return_Processor SHALL check the connection's remaining lifetime against the guard window.
2. IF the connection is within the guard window (remaining lifetime < 45 seconds), THEN THE Return_Processor SHALL discard the connection and emit a retirement metric.
3. IF the connection was marked as bad by the caller (e.g., received a connection-level error during use), THEN THE Return_Processor SHALL discard the connection.
4. IF the connection is outside the guard window and not marked bad, THEN THE Return_Processor SHALL place the connection back in the Ready_Channel immediately without a network round-trip.
5. THE Return_Processor SHALL NOT execute a ping or any network operation on returned connections.
6. IF a permit drop path discards an expired connection before it reaches the Return_Processor, THEN the permit SHALL call `Slot_Block_Manager.release_slot()` for that physical connection.
7. IF a permit drop path cannot send a live connection to the Return_Processor, THEN the permit SHALL call `Slot_Block_Manager.release_slot()` exactly once before dropping that physical connection.

### Requirement 17: Metrics Are Internal

**User Story:** As a tokeira developer, I want all reservoir metrics to be emitted automatically without operator configuration, so that observability is always available without tuning.

#### Acceptance Criteria

1. THE Reservoir SHALL emit all metrics (Requirement 13) unconditionally via the `metrics` crate.
2. THE Reservoir SHALL NOT provide configuration options to enable, disable, or filter metric emission.
3. THE metric names and labels SHALL follow the existing `tokeira_dsql_reservoir_*` and `tokeira_dsql_pool_*` naming conventions established in the codebase.

### Requirement 18: Architecture Documentation

**User Story:** As a tokeira developer, I want comprehensive documentation of the connection management architecture, so that contributors understand the design rationale, invariants, and operational behaviour without reading the implementation.

#### Acceptance Criteria

1. THE spec SHALL produce or update `docs/architecture/060-connection-management.md` documenting the full connection management architecture.
2. THE documentation SHALL cover: the reservoir as sole connection owner, the refiller's rate-limited creation loop, the expiry scanner's proactive retirement, the return processor's validation, the class budget admission control, the distributed token bucket coordination, and the slot block manager.
3. THE documentation SHALL explain WHY each design decision was made, referencing DSQL's known constraints (100/sec rate limit, 10k connection limit, 15-minute token TTL).
4. THE documentation SHALL include a data flow diagram showing the connection lifecycle from creation through checkout, use, return, validation, and retirement.
5. THE documentation SHALL document the invariants that must hold: class isolation (projection cannot starve commit), non-blocking checkout, rate-limited creation, guard window enforcement.
6. THE documentation SHALL explain the DynamoDB coordination tables (schema, TTL behaviour, cost model) and why they are provisioned for all DSQL deployments including compose.
7. THE documentation SHALL NOT include operator-tunable configuration guidance — all parameters are internal constants.

### Requirement 19: Compose IaC DynamoDB Table Provisioning

**User Story:** As a tokeira operator using compose+DSQL, I want the DynamoDB coordination tables provisioned automatically by `tkr infra apply`, so that the full coordination path works without manual AWS console work.

#### Acceptance Criteria

1. WHEN the compose platform's DSQL IaC module is applied, THE module SHALL provision a DynamoDB table for the distributed token bucket rate limiter.
2. WHEN the compose platform's DSQL IaC module is applied, THE module SHALL provision a DynamoDB table for the distributed slot block manager.
3. THE DynamoDB tables SHALL use on-demand billing mode (pay-per-request).
4. THE DynamoDB tables SHALL be provisioned in the same AWS region as the DSQL cluster.
5. THE DynamoDB table names SHALL be derived from the project name (e.g., `{project}-dsql-rate-limiter`, `{project}-dsql-conn-lease`).
6. THE DynamoDB tables SHALL have TTL enabled for automatic cleanup of expired entries.
7. WHEN `tkr infra destroy` is run, THE module SHALL delete the DynamoDB tables.
