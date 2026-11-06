# Requirements Document

## Introduction

The DSQL reservoir redesign eliminates the sqlx PgPool from the connection path entirely. The current implementation wraps a sqlx PgPool inside the reservoir: the refiller calls `pool.acquire()` to create connections, but the pool's `max_connections` equals `target_ready`, so when the reservoir holds all connections in its ready channel, the pool has nothing left for the refiller — causing `pool timed out` errors under load.

The redesign makes the reservoir the sole connection owner. Physical connections are created directly via IAM-authenticated TCP/TLS (using `aurora_dsql_sqlx_connector`), bypassing the sqlx pool entirely. The reservoir IS the pool. A background refiller creates connections respecting DSQL's cluster-wide rate limit. A DynamoDB-backed limiter shares that rate across every node. Each node holds at most its target of connections, and the placement controller caps each node's target to its share of the cluster's connection limit.

All connection management parameters are internal constants derived from DSQL's known constraints. The operator provides an endpoint and credentials. Everything else is automatic.

The constraints, from the Aurora DSQL quotas page (https://docs.aws.amazon.com/aurora-dsql/latest/userguide/CHAP_quotas.html) and its authentication token page (https://docs.aws.amazon.com/aurora-dsql/latest/userguide/SECTION_authentication-token.html):

| Constraint | Value | When exceeded |
|------------|-------|---------------|
| Connections per cluster | 10,000 (adjustable) | The connect is refused with SQLSTATE `53300` |
| Connection rate per cluster | 100 per second, burst 1,000 (not adjustable) | The connect is refused with SQLSTATE `53400` |
| Connection duration | 60 minutes | DSQL closes the connection |
| Transaction duration | 5 minutes | The transaction fails with SQLSTATE `54000` |
| IAM authentication token | 15 minutes by default | Checked only when a connection opens; an open connection stays valid after its token expires |

The embedded engine's reservoirs follow the managed embedded DSQL specification, except that they take the connection lifetimes of Requirement 11 and the connect bound of Requirement 1.

## Glossary

- **Reservoir**: The channel-based connection buffer that holds pre-created physical DSQL connections ready for immediate checkout.
- **Refiller**: A background tokio task that continuously creates new physical connections to keep the reservoir at its target.
- **Live_Connections**: The physical connections a node holds: ready in the Ready_Channel, checked out by callers, and still being created. A connection leaves the count exactly once, when it is dropped or its creation fails.
- **Expiry_Scanner**: A background tokio task that periodically inspects the ready channel and retires connections approaching the end of their assigned lifetime.
- **Return_Processor**: A background tokio task that validates returned connections via lifetime and bad-flag checks before placing them back in the ready channel.
- **Connection_Factory**: The component that creates raw `PgConnection` instances via IAM-authenticated TCP/TLS without using a sqlx pool.
- **Distributed_Token_Bucket**: The rate limiter every node shares through one DynamoDB item per DSQL endpoint. It admits connection creations at DSQL's cluster-wide rate (100 per second sustained, 1,000 burst). It stores an Arrival_Time rather than a token count and a timestamp (the generic cell rate algorithm), and admits exactly what a token bucket of the same rate and burst admits.
- **Arrival_Time**: The time by which every admission granted so far would have been paid for at the sustained rate. Each admission moves it on by one interval (10 ms at 100 per second). An admission at time `t` is immediate while the Arrival_Time is no later than `t` plus the burst's worth of intervals.
- **Fallback_Limiter**: A process-local rate limiter that admits connection creations for a short period after the Distributed_Token_Bucket's DynamoDB call fails.
- **Class_Budget**: A semaphore-based admission control layer that reserves connection capacity for each operation class (commit, read, projection, control, maintenance).
- **Guard_Window**: The time period before a connection's lifetime expiry during which the connection is retired rather than handed out.
- **Lifetime_Jitter**: A random offset added to each connection's base lifetime to prevent synchronized retirement storms.
- **In_Flight_Semaphore**: A semaphore that limits the number of concurrent connection creation attempts to prevent TCP/TLS handshake pile-ups.
- **IAM_Token_Provider**: The component that generates fresh IAM authentication tokens for each new DSQL connection.
- **Ready_Channel**: The bounded async channel that holds validated, ready-to-use connections.
- **Backpressure**: The signal returned to callers when no connection is free within the checkout wait, indicating the system is at capacity.

## Requirements

### Requirement 1: Direct Connection Factory

**User Story:** As a tokeira operator, I want the reservoir to create physical connections directly via IAM auth, so that connection creation is not constrained by a secondary pool's internal limits.

#### Acceptance Criteria

1. THE Connection_Factory SHALL create raw `PgConnection` instances using `aurora_dsql_sqlx_connector` IAM authentication without instantiating or acquiring from a sqlx PgPool.
2. WHEN the Connection_Factory creates a connection, THE Connection_Factory SHALL generate a fresh IAM token for that specific connection attempt.
3. THE Connection_Factory SHALL establish TCP/TLS connections directly to the DSQL endpoint specified in the operator-provided configuration.
4. IF the Connection_Factory fails to create a connection, THEN THE Connection_Factory SHALL return a typed error whose kind is one of `config`, `token`, `connection`, `database`, `refused` or `timeout`.
5. WHEN DSQL refuses a connection because the cluster's connection rate is exceeded (SQLSTATE `53400`) or the cluster is at its connection limit (SQLSTATE `53300`), THE Connection_Factory SHALL report the `refused` kind, whichever error type the connector wraps the refusal in.
6. IF a connection attempt has not completed within `CONNECT_TIMEOUT` (30 seconds), THEN THE Connection_Factory SHALL abandon it and report the `timeout` kind.

### Requirement 2: Reservoir as Sole Connection Owner

**User Story:** As a tokeira developer, I want the reservoir to be the only component that creates and holds DSQL connections, so that there is no conflict between connection management layers.

#### Acceptance Criteria

1. THE Reservoir SHALL hold physical connections in a bounded async channel (the Ready_Channel).
2. WHEN a caller checks out a connection, THE Reservoir SHALL receive the connection from the Ready_Channel.
3. WHEN a caller returns a connection, THE Reservoir SHALL send the connection to the Return_Processor for validation before placing it back in the Ready_Channel.
4. THE Reservoir SHALL be the only component in the tokeira process that creates or holds DSQL connections.
5. THE Reservoir SHALL not depend on or instantiate a sqlx PgPool for connection management.

### Requirement 3: Bounded Checkout with Backpressure

**User Story:** As a tokeira runtime developer, I want connection checkout to wait briefly for a connection that is on its way and then give up, so that a momentary gap between a retirement and its replacement is absorbed while a reservoir that cannot refill still answers promptly.

#### Acceptance Criteria

1. WHEN the Ready_Channel contains at least one connection, THE Reservoir SHALL return a connection immediately via channel receive.
2. WHEN the Ready_Channel is empty, THE Reservoir SHALL wait up to `CHECKOUT_WAIT` (1 second) for a returned or newly created connection, and SHALL return a backpressure error if none arrives within it.
3. THE Reservoir SHALL emit a metric each time a checkout returns the backpressure error.
4. THE Class_Budget holder (the caller) SHALL handle the backpressure error via retry or propagation to the upstream operation.
5. WHILE a checkout waits, THE Reservoir SHALL NOT create a connection for the caller, call DynamoDB, or perform any other network I/O on the caller's behalf.
6. WHEN a caller's operation is cancelled while it waits for a connection, THE Reservoir SHALL end the wait without taking a connection out of the Ready_Channel.

### Requirement 4: Background Refiller

**User Story:** As a tokeira operator, I want a background task to continuously maintain the reservoir at its target level, so that connections are always available for checkout without hot-path creation latency.

#### Acceptance Criteria

1. THE Refiller SHALL run as a dedicated tokio task that continuously creates connections to keep the node's Live_Connections at `target_ready` (50).
2. WHILE Live_Connections are fewer than `target_ready`, THE Refiller SHALL attempt to create new connections.
3. WHEN the Refiller creates a connection, THE Refiller SHALL acquire resources in this order: `In_Flight_Semaphore`, a place in Live_Connections, an admission from the Distributed_Token_Bucket, then `Connection_Factory.create_connection()`.
4. IF rate limiting or connection creation fails after the Refiller took a place in Live_Connections, THEN THE Refiller SHALL give the place back before backing off.
5. IF the Connection_Factory returns an error, THEN THE Refiller SHALL back off with exponential delay before retrying.
6. THE Refiller SHALL assign each new connection a lifetime of `base_lifetime` plus a random Lifetime_Jitter between zero and `lifetime_jitter`.
7. THE Refiller SHALL record the creation timestamp on each connection for lifetime enforcement.
8. IF the Refiller creates a connection but cannot place it into the Ready_Channel, THEN THE Reservoir SHALL count that physical connection out of Live_Connections exactly once as it drops it.
9. WHILE a connection attempt is running, THE Reservoir SHALL count it in Live_Connections until it succeeds, fails or reaches `CONNECT_TIMEOUT`.

### Requirement 5: Expiry Scanner

**User Story:** As a tokeira operator, I want near-expired connections to be proactively retired from the ready channel, so that callers never receive a connection that will expire mid-transaction.

#### Acceptance Criteria

1. THE Expiry_Scanner SHALL run as a dedicated tokio task that periodically scans the Ready_Channel at `scan_interval` (1 second).
2. WHEN the Expiry_Scanner finds a connection whose elapsed age plus `guard_window` (45 seconds) exceeds its assigned lifetime, THE Expiry_Scanner SHALL retire that connection by dropping it.
3. THE Expiry_Scanner SHALL emit a metric for each connection retired due to guard window proximity.
4. THE Expiry_Scanner SHALL not drain the entire Ready_Channel in a single scan pass; scanning SHALL be bounded to prevent starvation of concurrent checkout callers.
5. WHEN the Expiry_Scanner retires a connection, THE Reservoir SHALL count that physical connection out of Live_Connections.
6. IF the Expiry_Scanner cannot place a still-healthy scanned connection back into the Ready_Channel, THEN THE Reservoir SHALL count that physical connection out of Live_Connections exactly once as it drops it.

### Requirement 6: Return Processor

**User Story:** As a tokeira developer, I want returned connections to be validated before reuse, so that callers never receive a broken connection from the reservoir.

#### Acceptance Criteria

1. THE Return_Processor SHALL run as a dedicated tokio task that receives connections from the return channel.
2. WHEN the Return_Processor receives a connection, THE Return_Processor SHALL check whether the connection is within the guard window; connections within the guard window SHALL be discarded.
3. WHEN the Return_Processor receives a connection, THE Return_Processor SHALL validate it using only the lifetime check and caller-provided bad-flag.
4. IF the connection is outside the guard window and is not marked bad, THEN THE Return_Processor SHALL place the connection back in the Ready_Channel.
5. IF the connection is marked bad, THEN THE Return_Processor SHALL discard the connection and emit a metric indicating the failure reason.
6. IF the Return_Processor discards a returned connection, THEN THE Reservoir SHALL count that physical connection out of Live_Connections.
7. IF the Return_Processor cannot place a reusable returned connection back into the Ready_Channel, THEN THE Reservoir SHALL count that physical connection out of Live_Connections exactly once as it drops it.

### Requirement 7: Cluster-Wide Connection Rate

**User Story:** As a tokeira developer, I want every node's connection creations to share DSQL's cluster-wide rate through one DynamoDB item, so that a fleet never exceeds the 100 per second connection rate, and a DynamoDB failure slows refills rather than stopping them.

#### Acceptance Criteria

1. THE Distributed_Token_Bucket SHALL be the only rate limiter in the connection creation path while its DynamoDB calls succeed. There SHALL NOT be a separate local token bucket in front of it.
2. THE Refiller SHALL obtain an admission from the Distributed_Token_Bucket before each connection creation attempt.
3. THE Distributed_Token_Bucket SHALL admit, across all nodes sharing a DSQL endpoint, no more than `DISTRIBUTED_BURST` (1,000) creations at once and no more than `DISTRIBUTED_RATE` (100) creations per second sustained.
4. THE Distributed_Token_Bucket SHALL keep its state in a single DynamoDB item per DSQL endpoint holding the Arrival_Time.
5. THE Distributed_Token_Bucket SHALL grant an admission with one conditional DynamoDB update, or two when the item's state differs from the first update's assumption, and SHALL NOT read the item before updating it.
6. THE Distributed_Token_Bucket SHALL compute each admission from the stored Arrival_Time and the admitting node's own clock, and SHALL NOT compare one node's clock against a timestamp written by another node.
7. WHEN an admission would have to wait longer than `RATE_MAX_WAIT` (30 seconds), THE Distributed_Token_Bucket SHALL refuse it without changing the item, and THE Refiller SHALL back off.
8. IF a DynamoDB call fails with any error other than a failed condition, after the AWS SDK's own retries, THEN THE Distributed_Token_Bucket SHALL admit connection creations from the Fallback_Limiter for `FALLBACK_PERIOD` (30 seconds) before calling DynamoDB again.
9. THE Fallback_Limiter SHALL admit at the node's rate share from its latest connection budget directive, or at `FALLBACK_RATE` (10 per second) when the node has received none, with a burst of one second's admissions and no fewer than one.
10. WHILE the Fallback_Limiter admits connection creations, THE Reservoir SHALL report it through a warning log when the fallback begins and a gauge that reads one until DynamoDB answers again.

### Requirement 8: Fleet Connection Capacity

**User Story:** As a tokeira operator, I want each node's connections bounded by its target and the fleet's targets bounded by the cluster's connection limit, so that the fleet stays within DSQL's 10,000 connections without a per-connection lease that a coordination failure could leave a node unable to hold.

#### Acceptance Criteria

1. THE Reservoir SHALL hold no more Live_Connections than its target.
2. WHEN a node receives a connection budget directive from the placement controller, THE Reservoir SHALL cap its target at the directive's `capacity` share, so that the targets of the nodes the controller has budgeted sum to no more than the controller's `dsql_connection_capacity_budget` (shard placement membership Requirement 9.2.2).
3. THE Reservoir SHALL NOT take a DynamoDB lease, or any other shared record, per connection or per block of connections.
4. WHEN DSQL refuses a connection because the cluster is at its connection limit (SQLSTATE `53300`), THE Refiller SHALL back off as for any refused connect and SHALL leave the node's existing connections in service.
5. WHERE a deployment runs without a placement controller, THE Reservoir SHALL hold up to its configured target, so the deployment's connections are bounded by its node count times that target.
6. WHEN the Reservoir's target falls below its Live_Connections, THE Reservoir SHALL retire ready connections above the target at once and returned connections above the target as they come back, and SHALL NOT interrupt a checked-out connection.

### Requirement 9: Class-Based Admission Control

**User Story:** As a tokeira developer, I want operation classes to have reserved connection budgets, so that projection bursts cannot starve commit operations.

#### Acceptance Criteria

1. THE Class_Budget SHALL allocate semaphore permits from `target_ready` as: commit 50%, read 20%, projection 10%, control 10%, maintenance 10%.
2. THE Class_Budget allocation SHALL be derived from `target_ready` as internal constants; the allocation SHALL NOT be operator-configurable.
3. WHEN a caller acquires a connection, THE Class_Budget SHALL first require a semaphore permit for the caller's operation class before checking out from the Ready_Channel.
4. FOR ALL valid system states, the projection class SHALL NOT be able to acquire permits reserved for the commit class (class isolation invariant).
5. THE Class_Budget SHALL emit metrics for each class: total permits, in-use permits, and wait duration.
6. WHEN the Reservoir's target changes, THE Class_Budget SHALL recompute its allocations from the new target, so that once permits already held are returned, the permits across all classes sum to the target.
7. THE Reservoir's target SHALL NOT fall below five, one connection for each class.

### Requirement 10: IAM Token Provider

**User Story:** As a tokeira developer, I want fresh IAM tokens generated for each new connection, so that connections authenticate correctly without sharing expired tokens.

#### Acceptance Criteria

1. THE IAM_Token_Provider SHALL generate a fresh authentication token for each connection creation attempt by the Refiller.
2. THE IAM_Token_Provider SHALL NOT cache tokens across connection creation attempts.
3. THE IAM_Token_Provider SHALL use the DSQL endpoint and region from operator-provided configuration to generate tokens.
4. IF the IAM_Token_Provider fails to generate a token, THEN THE Connection_Factory SHALL propagate the error to the Refiller as a connection creation failure.
5. THE Connection_Factory SHALL resolve AWS credentials once and share them across connection attempts until five minutes before they expire, so that signing a token is a local computation with no network call.

### Requirement 11: Connection Lifecycle Constants

**User Story:** As a tokeira operator, I want all connection lifecycle parameters to be derived from DSQL's known constraints, so that I do not need to configure or tune connection management.

#### Acceptance Criteria

1. THE Reservoir SHALL use the following internal constants derived from DSQL constraints: `target_ready` = 50, `base_lifetime` = 50 minutes, `lifetime_jitter` = up to 4 minutes, `guard_window` = 45 seconds, `inflight_limit` = 8, `scan_interval` = 1 second, `checkout_wait` = 1 second.
2. THE Reservoir SHALL NOT expose connection lifecycle parameters as operator-configurable settings.
3. FOR ALL connections created by the Refiller, the assigned lifetime SHALL be within the range `[base_lifetime, base_lifetime + lifetime_jitter]` (50 to 54 minutes).
4. FOR ALL connections created by the Refiller, the assigned lifetime plus DSQL's 5-minute transaction limit SHALL NOT exceed DSQL's 60-minute connection duration, so that a transaction begun on a connection before its guard window ends before DSQL closes the connection.
5. THE Reservoir SHALL NOT retire a connection because the IAM token it opened with has expired: DSQL checks the token only when a connection opens.

### Requirement 12: Graceful Degradation

**User Story:** As a tokeira operator, I want the system to degrade gracefully when the reservoir is empty and connections cannot be created, so that operations queue predictably rather than failing catastrophically.

#### Acceptance Criteria

1. THE system SHALL queue operations at the Class_Budget semaphore for admission before reservoir checkout.
2. WHEN no connection reaches a caller within `CHECKOUT_WAIT` after class admission, THE caller SHALL release the class permit and signal backpressure with `ReservoirError::Empty`.
3. WHILE the Reservoir is in a degraded state, THE Reservoir SHALL emit metrics exposing the backpressure: empty reservoir events, refiller in-flight count, rate limiter tokens remaining, and whether the Fallback_Limiter is in use.
4. WHEN the Refiller encounters repeated connection creation failures, THE Refiller SHALL apply exponential backoff up to a maximum delay without stopping the refill loop.
5. WHEN DSQL becomes available again after an outage, THE Refiller SHALL resume filling the reservoir without operator intervention.

### Requirement 13: Observability

**User Story:** As a tokeira operator, I want comprehensive metrics for the reservoir and its subsystems, so that I can monitor connection health and diagnose performance issues.

#### Acceptance Criteria

1. THE Reservoir SHALL emit the following metrics via the `metrics` crate: reservoir size (gauge), live connections (gauge), checkout latency (histogram), empty reservoir events (counter), refiller in-flight count (gauge), connection age at retirement (histogram), rate limiter tokens remaining (gauge), fallback limiter in use (gauge), class budget utilization per class (gauge), and class permit wait duration (histogram).
2. THE Reservoir SHALL NOT expose metrics configuration as operator-configurable settings.
3. WHEN a connection is retired, THE Reservoir SHALL record the retirement reason (expired, guard_window, unhealthy, budget_cap) as a metric label.
4. THE Reservoir SHALL emit a metric for connection creation duration (histogram) to track IAM auth and TCP/TLS handshake latency.

### Requirement 14: DynamoDB Coordination for All DSQL Deployments

**User Story:** As a tokeira developer, I want the DynamoDB-backed rate limiter active for ALL DSQL deployments including compose, so that developers exercise the production coordination path locally and catch coordination bugs before they reach multi-node deployments.

#### Acceptance Criteria

1. WHEN the storage backend is DSQL, THE IaC DSQL module of every platform that provisions a DSQL cluster SHALL provision the DynamoDB rate limiter table alongside the cluster.
2. WHEN the tokeira process starts with DSQL storage, THE Reservoir SHALL always use the Distributed_Token_Bucket regardless of whether the deployment is single-node or multi-node.
3. IF the DynamoDB rate limiter table does not exist or cannot be reached at startup, THEN THE Reservoir SHALL fail with an error that names the table and says how to provision it.
4. THE DynamoDB rate limiter table SHALL be provisioned with on-demand billing (pay-per-request) so it costs nothing when idle.
5. THE platform SHALL provision the DynamoDB rate limiter table in the same region as the DSQL cluster, using the same AWS credentials.
6. WHEN `tokeirad` starts with DSQL storage, THE DSQL startup path SHALL populate `DsqlCoordinationConfig` with a DynamoDB client plus the rate limiter table name derived from the effective project identifier: `{project}-dsql-rate-limiter`.
7. THE `DsqlStore::connect(auth, config)` startup path SHALL use `config.coordination.rate_limiter_table` and `config.coordination.ddb_client` to construct the Distributed_Token_Bucket.
8. WHEN the configuration sets `infrastructure.dsql.conn_lease_table`, THE startup path SHALL accept the configuration, ignore the key, and log a warning that names the key as no longer used.

### Requirement 15: Pool Warmup

**User Story:** As a tokeira operator, I want the reservoir to be warmed before the server accepts traffic, so that the first requests do not experience cold-start latency, and a slow or crowded warmup never stops the server from starting.

#### Acceptance Criteria

1. WHEN the tokeira process starts, THE Refiller SHALL fill the reservoir toward `target_ready` before the server begins accepting gRPC traffic.
2. WHILE warming up, THE Refiller SHALL obtain every creation's admission from the Distributed_Token_Bucket, as at any other time.
3. THE warmup phase SHALL end when the reservoir reaches its target or `WARMUP_TIMEOUT` (30 seconds) passes, whichever comes first.
4. IF warmup ends at `WARMUP_TIMEOUT` before the reservoir reaches its target, THEN THE Reservoir SHALL log a warning with the number of connections it holds and allow the server to start with the partially filled reservoir, and THE Refiller SHALL continue filling it.
5. WHEN many nodes start at once, THE Distributed_Token_Bucket SHALL keep their combined connection creations within DSQL's burst and rate, and every node SHALL begin serving within `WARMUP_TIMEOUT` with the connections it holds by then.

### Requirement 16: Connection Return with Lifetime Check

**User Story:** As a tokeira developer, I want returned connections to be checked for remaining lifetime before reuse, so that near-expired connections are discarded without adding per-return network overhead.

#### Acceptance Criteria

1. WHEN a connection is returned to the Reservoir, THE Return_Processor SHALL check the connection's remaining lifetime against the guard window.
2. IF the connection is within the guard window (remaining lifetime < 45 seconds), THEN THE Return_Processor SHALL discard the connection and emit a retirement metric.
3. IF the connection was marked as bad by the caller (e.g., received a connection-level error during use), THEN THE Return_Processor SHALL discard the connection.
4. IF the connection is outside the guard window and not marked bad, THEN THE Return_Processor SHALL place the connection back in the Ready_Channel immediately without a network round-trip.
5. THE Return_Processor SHALL NOT execute a ping or any network operation on returned connections.
6. IF a permit drop path discards an expired connection before it reaches the Return_Processor, THEN THE Reservoir SHALL count that physical connection out of Live_Connections.
7. IF a permit drop path cannot send a live connection to the Return_Processor, THEN THE Reservoir SHALL count that physical connection out of Live_Connections exactly once as it drops it.
8. WHEN a connection is returned while Live_Connections exceed the target, THE Return_Processor SHALL retire it.

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
2. THE documentation SHALL cover: the reservoir as sole connection owner, the refiller's rate-limited creation loop, the expiry scanner's proactive retirement, the return processor's validation, the class budget admission control, the bounded checkout wait, the distributed token bucket and its fallback, and the fleet's connection capacity.
3. THE documentation SHALL explain WHY each design decision was made, referencing DSQL's known constraints (100/sec rate limit and 1,000 burst, 10k connection limit, 60-minute connection duration, 5-minute transaction limit, and a 15-minute token checked only at connect).
4. THE documentation SHALL include a data flow diagram showing the connection lifecycle from creation through checkout, use, return, validation, and retirement.
5. THE documentation SHALL document the invariants that must hold: class isolation (projection cannot starve commit), bounded checkout, rate-limited creation, guard window enforcement, and live connections within the target.
6. THE documentation SHALL explain the DynamoDB rate limiter table (schema, TTL behaviour, cost model) and why it is provisioned for all DSQL deployments including compose.
7. THE documentation SHALL NOT include operator-tunable configuration guidance — all parameters are internal constants.

### Requirement 19: Compose IaC DynamoDB Table Provisioning

**User Story:** As a tokeira operator using compose+DSQL, I want the DynamoDB rate limiter table provisioned automatically by `tkr infra apply`, so that the full coordination path works without manual AWS console work.

#### Acceptance Criteria

1. WHEN the compose platform's DSQL IaC module is applied, THE module SHALL provision a DynamoDB table for the distributed token bucket rate limiter.
2. THE compose, ECS and EKS platform definitions SHALL NOT provision a DynamoDB table for connection slot leases.
3. THE DynamoDB table SHALL use on-demand billing mode (pay-per-request).
4. THE DynamoDB table SHALL be provisioned in the same AWS region as the DSQL cluster.
5. THE DynamoDB table name SHALL be derived from the project name: `{project}-dsql-rate-limiter`.
6. THE DynamoDB table SHALL have TTL enabled for automatic cleanup of expired entries.
7. WHEN `tkr infra destroy` is run, THE module SHALL delete the DynamoDB table.
