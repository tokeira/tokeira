# Requirements Document

## Introduction

This document captures the requirements for adding quality metrics to the DSQL storage and projection layers, plus dashboard updates that deliver real operational insight. The observability foundation (Phases 1–4) is complete — counters, gauges, and histograms work via `metrics::counter!`, `metrics::gauge!`, `metrics::histogram!`, and the Prometheus endpoint on `:9090` renders summaries with quantile labels.

The DSQL storage layer (`crates/tokeira-storage/src/dsql/`) already has:
- A `DsqlConnectionDirector` with reservoir pattern, class-based budgets, and token-bucket rate limiter — with metrics for pool lifecycle, class budgets, and checkout latency already wired.
- A `DsqlRunRepository` with instrumented methods (tracing spans) but NO `metrics` crate calls for per-operation latency or DSQL-specific failure modes.
- A `DsqlProjectionLog` with no metrics for read latency or batch sizes.

The compose platform has a `storage-projection-health.json` dashboard that references metrics like `tokeira_dsql_pool_connections_total` and `tokeira_storage_commit_transition_duration_seconds`. Some of these metrics exist but the dashboard panels need updating to match the actual summary format (quantile labels, not `_bucket` suffixes). Other referenced metrics (pool lifecycle, class budgets) exist in the metrics manifest but are not yet emitted by the DSQL code paths.

The bench showed 704 wps with DSQL — real traffic is flowing and operators need visibility into DSQL-specific behaviour: OCC conflicts, retry storms, reservoir pressure, per-operation latency breakdown, connection lifecycle decomposition, rate limiter internals, query-level decomposition, predictive signals, projection deep metrics, per-shard distribution, and error classification.

Key constraints:
- Metrics use the `metrics` crate recording API (`counter!`, `gauge!`, `histogram!`).
- Metric names follow `tokeira_{crate}_{subsystem}_{metric}_{unit}` convention.
- The kernel stays pure — no metrics in `tokeira-kernel`.
- `metrics-exporter-prometheus` renders `histogram!()` as summaries with `{quantile="0.5"}` labels, not histogram buckets.
- Dashboard datasource UIDs are `mimir` (Prometheus) and `loki` (logs).
- All metrics are zero-cost when unobserved — the only cost is implementation time.

## Glossary

- **Connection_Director**: The `DsqlConnectionDirector` in `tokeira-storage::dsql::connection` that manages class-based admission control and delegates physical connection management to the Reservoir.
- **Reservoir**: The warm connection pool in `tokeira-storage::dsql::reservoir` that pre-creates DSQL connections, retires expired ones, and validates returned connections.
- **Run_Repository**: The `DsqlRunRepository` in `tokeira-storage::dsql::run_repository` that implements the semantic `RunRepository` contract over DSQL tables.
- **Projection_Log**: The `DsqlProjectionLog` in `tokeira-storage::dsql::projection_log` that reads projection records for projection workers.
- **OCC_Conflict**: An optimistic concurrency control conflict (SQLSTATE 40001) returned by DSQL when a transaction's read set was modified by a concurrent commit.
- **Reservoir_Entry**: A single warmed DSQL connection held in the reservoir's ready channel, with creation time and jittered lifetime.
- **Class_Budget**: A per-operation-class semaphore in the Connection_Director that prevents one workload type from starving another.
- **Summary**: The Prometheus metric type rendered by `metrics-exporter-prometheus` for `histogram!()` calls — exposes quantile labels (0.5, 0.9, 0.95, 0.99) rather than bucket boundaries.
- **Dashboard**: A Grafana JSON model provisioned at `platforms/compose/dashboards/` and auto-loaded by the compose observability stack.
- **Rate_Limiter**: The token-bucket rate limiter in `tokeira-storage::dsql::rate_limiter` that throttles connection creation to respect DSQL's cluster-wide connection rate limit.
- **Projection_Worker**: The worker in `tokeira-projection` that reads from the projection log and writes to visibility sinks.
- **Dsql_Visibility_Store**: The `DsqlVisibilityStore` in `tokeira-projection` that implements DSQL-backed visibility list/count queries and search-attribute index access.
- **Shard**: A logical partition of workflow data in the DSQL tables, determined by the shard key derivation function.
- **Visibility_Query**: A read query against the projection sink tables used by the visibility API (List/Count workflows).

## Requirements

---

## Phase 1: DSQL Run Repository Operation Metrics

### Requirement 1: Per-Operation Latency Recording

**User Story:** As a Tokeira operator, I want per-operation latency metrics for DSQL repository methods, so that I can identify which storage operations are slow and correlate latency with DSQL behaviour.

#### Acceptance Criteria

1. WHEN a `DsqlRunRepository` method completes, THE Run_Repository SHALL record `tokeira_storage_dsql_operation_duration_seconds` as a histogram with labels `operation` and `outcome`.
2. THE Run_Repository SHALL record the `operation` label using the DSQL repository method name (e.g., `resolve_execution`, `commit_transition`, `load_run`, `read_history`, `find_latest_run`, `list_dispatchable_workflow_tasks`), independent of tracing span names.
3. THE Run_Repository SHALL record the `outcome` label as `success`, `conflict`, or `error`.
4. THE Run_Repository SHALL measure duration from immediately before connection acquisition through query completion, capturing the full DSQL round-trip including checkout wait time.

### Requirement 2: OCC Conflict Counting

**User Story:** As a Tokeira operator, I want a counter for OCC serialization conflicts, so that I can detect contention hotspots and tune shard distribution.

#### Acceptance Criteria

1. WHEN a DSQL operation returns SQLSTATE 40001 (serialization failure), THE Run_Repository SHALL increment `tokeira_storage_dsql_occ_conflict_total` with label `operation`.
2. THE Run_Repository SHALL count conflicts independently from the retry mechanism — each conflict increments the counter regardless of whether the operation is retried.

### Requirement 3: Retry Counting

**User Story:** As a Tokeira operator, I want a counter for operation retries, so that I can distinguish transient conflicts from persistent contention.

#### Acceptance Criteria

1. WHEN a DSQL operation is retried after an OCC conflict, THE Run_Repository SHALL increment `tokeira_storage_dsql_retry_total` with labels `operation` and `outcome` (success or exhausted).
2. THE Run_Repository SHALL record `outcome` as `success` when the retry succeeds and `exhausted` when all retry attempts fail.

### Requirement 4: Operation Throughput Counter

**User Story:** As a Tokeira operator, I want a per-operation counter, so that I can compute operation rates and correlate throughput with latency changes.

#### Acceptance Criteria

1. WHEN a `DsqlRunRepository` method completes, THE Run_Repository SHALL increment `tokeira_storage_dsql_operation_total` with labels `operation` and `outcome`.
2. THE Run_Repository SHALL use the same `operation` and `outcome` label values as the latency histogram (Requirement 1.1).

---

## Phase 2: DSQL Connection Director and Reservoir Metrics

### Requirement 5: Reservoir Ready Connections Gauge

**User Story:** As a Tokeira operator, I want a gauge showing the number of ready connections in the reservoir, so that I can detect reservoir depletion before it causes checkout latency spikes.

#### Acceptance Criteria

1. THE Reservoir SHALL record the existing `tokeira_dsql_pool_connections_total` gauge to reflect the current number of connections available for immediate checkout.
2. THE Reservoir SHALL update this gauge after each checkout, return, creation, and retirement event by calling the existing `record_dsql_pool_connections_total(count)` helper.

### Requirement 6: In-Flight Checkout Gauge

**User Story:** As a Tokeira operator, I want a gauge showing the number of connections currently checked out, so that I can monitor connection utilization.

#### Acceptance Criteria

1. THE Connection_Director SHALL record `tokeira_dsql_reservoir_in_flight` as a gauge reflecting the number of connections currently held by operations (checked out but not yet returned).
2. THE Connection_Director SHALL increment this gauge on checkout and decrement on return or retirement.

### Requirement 7: Reservoir Empty Events Counter

**User Story:** As a Tokeira operator, I want a counter for reservoir empty events, so that I can detect when checkout demand exceeds the reservoir's refill rate.

#### Acceptance Criteria

1. WHEN a checkout attempt finds the reservoir empty and must wait for a new connection, THE Reservoir SHALL increment the existing `tokeira_dsql_pool_empty_reservoir_total` metric.
2. THE Reservoir code path SHALL call the existing `record_dsql_pool_empty_reservoir()` helper at the point where an empty reservoir is detected.

### Requirement 8: Connection Lifecycle Counters

**User Story:** As a Tokeira operator, I want counters for connection creation, retirement, and return events, so that I can monitor reservoir churn and detect abnormal connection turnover.

#### Acceptance Criteria

1. WHEN the Reservoir creates a new physical connection, THE Reservoir SHALL call the existing `record_dsql_pool_connection_created()` helper.
2. WHEN the Reservoir retires a connection, THE Reservoir SHALL call the existing `record_dsql_pool_connection_retired(reason)` helper with reason `expired`, `unhealthy`, `guard_window`, or `budget_cap`.
3. WHEN a connection is returned to the Reservoir after use, THE Reservoir SHALL call the existing `record_dsql_pool_connection_returned()` helper.

### Requirement 9: Class Budget Periodic Reporting

**User Story:** As a Tokeira operator, I want periodic class budget snapshots, so that I can see per-class saturation without relying on high-frequency per-checkout updates.

#### Acceptance Criteria

1. THE Connection_Director SHALL periodically (every 5 seconds) record class budget state by calling the existing `record_dsql_pool_class_budget(class, total, in_use, waiters)` helper for each configured class.
2. THE Connection_Director SHALL report budget state for all configured `DbClass` variants: `control`, `commit`, `read`, `projection`, and `maintenance`.

---

## Phase 3: DSQL Projection Log Metrics

### Requirement 10: Projection Log Read Latency

**User Story:** As a Tokeira operator, I want latency metrics for projection log reads, so that I can detect when DSQL read performance degrades and affects projection freshness.

#### Acceptance Criteria

1. WHEN `DsqlProjectionLog::read_from` completes a database query, THE Projection_Log SHALL record `tokeira_storage_dsql_projection_read_duration_seconds` as a histogram with label `partition_id`.
2. THE Projection_Log SHALL measure duration from connection acquisition through query result decoding.

### Requirement 11: Projection Log Batch Size

**User Story:** As a Tokeira operator, I want batch size metrics for projection log reads, so that I can tune the read limit and detect partitions with uneven load.

#### Acceptance Criteria

1. WHEN `DsqlProjectionLog::read_from` returns records, THE Projection_Log SHALL record `tokeira_storage_dsql_projection_batch_size` as a histogram with label `partition_id`.
2. THE Projection_Log SHALL record the number of records returned in each batch (0 for empty reads).

---

## Phase 4: Dashboard Updates

### Requirement 12: Fix Latency Panel Queries for Summary Format

**User Story:** As a Tokeira operator, I want the storage-projection dashboard latency panels to show real data, so that I can monitor DSQL performance without manual PromQL editing.

#### Acceptance Criteria

1. THE Dashboard SHALL use PromQL queries that reference summary quantile labels (e.g., `{quantile="0.95"}`) for all latency panels.
2. THE Dashboard SHALL NOT use `histogram_quantile()` or `_bucket` suffix queries for metrics rendered by `metrics-exporter-prometheus`.
3. THE Dashboard latency panels SHALL display commit, load, and history-read p50 and p95 latencies using the existing `tokeira_storage_commit_transition_duration_seconds`, `tokeira_storage_load_run_duration_seconds`, and `tokeira_storage_read_history_duration_seconds` metrics.

### Requirement 13: DSQL Health Stat Row

**User Story:** As a Tokeira operator, I want a top-level DSQL health summary row in the dashboard, so that I can assess DSQL health at a glance.

#### Acceptance Criteria

1. THE Dashboard SHALL include a "DSQL Health" stat row at the top of the dashboard showing: reservoir ready connections, OCC conflict rate (conflicts/sec over 5m), checkout p95 latency, and total DSQL operation rate.
2. THE Dashboard stat panels SHALL use datasource UID `mimir`.
3. THE Dashboard stat panels SHALL use `lastNotNull` as the reduce calculation.
4. THE Dashboard stat row SHALL use a 24-column grid layout with stat panels evenly distributed across the row width.

### Requirement 14: DSQL Operations Row

**User Story:** As a Tokeira operator, I want a DSQL operations breakdown row, so that I can see per-operation latency and conflict rates in DSQL-specific context.

#### Acceptance Criteria

1. THE Dashboard SHALL include a "DSQL Operations" row with panels for: per-operation latency (p50, p95 by operation), operation rate by outcome, and OCC conflict rate by operation.
2. THE Dashboard operation latency panel SHALL use `tokeira_storage_dsql_operation_duration_seconds{quantile="0.5"}` and `tokeira_storage_dsql_operation_duration_seconds{quantile="0.95"}` queries grouped by `operation` label.
3. THE Dashboard conflict rate panel SHALL use `sum by (operation) (rate(tokeira_storage_dsql_occ_conflict_total[5m]))`.
4. THE Dashboard timeseries panels SHALL use smooth line interpolation, `showPoints` set to `never`, and `pointSize` set to 0.
5. THE Dashboard timeseries panels SHALL use bottom-placed table legends with `lastNotNull`, `mean`, and `max` calculations.
6. THE Dashboard rate panels SHALL use explicit rate units (`ops` for operations per second).

### Requirement 15: DSQL Pool Section with Reservoir Health

**User Story:** As a Tokeira operator, I want the DSQL Pool dashboard section to show reservoir health once the metrics are emitted, so that I can monitor connection lifecycle and detect pressure.

#### Acceptance Criteria

1. THE Dashboard DSQL Pool section SHALL include panels for: reservoir ready connections over time, in-flight connections over time, connection lifecycle rates (created/s, retired/s, returned/s, empty/s), and class budget utilization.
2. THE Dashboard pool lifecycle panel SHALL use `rate(tokeira_dsql_pool_connections_created_total[5m])`, `rate(tokeira_dsql_pool_connections_retired_total[5m])`, `rate(tokeira_dsql_pool_connections_returned_total[5m])`, and `rate(tokeira_dsql_pool_empty_reservoir_total[5m])`.
3. THE Dashboard class budget panel SHALL use `tokeira_dsql_pool_class_budget_total`, `tokeira_dsql_pool_class_in_use`, and `tokeira_dsql_pool_class_waiters` grouped by `class` label.
4. THE Dashboard timeseries panels SHALL use smooth line interpolation, `showPoints` set to `never`, and `pointSize` set to 0.
5. THE Dashboard timeseries panels SHALL use bottom-placed table legends with `lastNotNull`, `mean`, and `max` calculations.
6. THE Dashboard panels SHALL include description annotations explaining the signal meaning and expected operational use.

### Requirement 16: Dashboard Layout and Style Conventions

**User Story:** As a Tokeira developer, I want the dashboard to follow consistent layout and style conventions, so that panels are readable and visually coherent.

#### Acceptance Criteria

1. THE Dashboard SHALL use an aligned 24-column grid layout with stat panels and timeseries panels arranged consistently within each row.
2. THE Dashboard timeseries panels SHALL use smooth line interpolation (`lineInterpolation: "smooth"`), `showPoints: "never"`, and `pointSize: 0`.
3. THE Dashboard timeseries panels SHALL use bottom-placed table legends with `lastNotNull`, `mean`, and `max` calculations.
4. THE Dashboard rate panels SHALL use `rate()` PromQL functions and explicit rate units (e.g., `ops` for operations/second).
5. THE Dashboard panels SHALL include `description` annotations explaining what the signal means and how operators should interpret the values.

### Requirement 17: Metric Name Registration

**User Story:** As a Tokeira developer, I want all new metrics registered in the `METRIC_NAMES` manifest, so that naming validation tests cover the new metrics.

#### Acceptance Criteria

1. THE `tokeira-storage` crate `metrics.rs` module SHALL include all new storage DSQL metric names in the `METRIC_NAMES` manifest with correct `MetricType` annotations.
2. THE `tokeira-projection` crate `metrics.rs` module SHALL include all new projection and visibility metric names in the `METRIC_NAMES` manifest with correct `MetricType` annotations.
3. THE shared `tokeira-types::MetricType` enum and `validate_metric_name` helper SHALL support unitless histogram metrics used for distributions such as batch size, rows read/written, and retry count.
4. THE existing `manifest_uses_valid_metric_names` tests SHALL pass for all new metric entries in both crates.

---

## Phase 5: Connection Lifecycle Decomposition

### Requirement 18: Checkout Wait Time

**User Story:** As a Tokeira operator, I want a histogram measuring the time from checkout request to connection acquired, so that I can identify latency hiding in the gap between "reservoir has connections" and "my operation got one".

#### Acceptance Criteria

1. WHEN a checkout request completes, THE Connection_Director SHALL record the existing `tokeira_dsql_pool_checkout_duration_seconds` metric as a histogram with label `class`.
2. THE Reservoir SHALL measure duration from the moment the checkout is requested until the connection is handed to the caller, including any time spent waiting for a class budget permit.
3. THE Reservoir SHALL record this metric independently from the existing `tokeira_storage_dsql_operation_duration_seconds` so operators can decompose total operation latency into checkout wait vs SQL execution.

### Requirement 19: Connection Creation Time

**User Story:** As a Tokeira operator, I want a histogram measuring the time to create a new DSQL connection (IAM token generation + TCP + TLS + auth handshake), so that I can detect infrastructure-level slowdowns in connection establishment.

#### Acceptance Criteria

1. WHEN the Reservoir creates a new physical connection, THE Reservoir SHALL record `tokeira_dsql_reservoir_connection_create_duration_seconds` as a histogram.
2. THE Reservoir SHALL measure duration from the start of connection creation (including IAM token generation if needed) through successful authentication and readiness for queries.

### Requirement 20: Connection Validation Time

**User Story:** As a Tokeira operator, I want a histogram measuring the time spent validating a returned connection (ping/query to verify liveness), so that I can detect when validation overhead becomes significant.

#### Acceptance Criteria

1. WHEN the Reservoir validates a returned connection before placing it back in the ready pool, THE Reservoir SHALL record `tokeira_dsql_reservoir_connection_validate_duration_seconds` as a histogram.
2. THE Reservoir SHALL measure duration from the start of the validation query through its completion.

### Requirement 21: Connection Age at Retirement

**User Story:** As a Tokeira operator, I want a histogram measuring how long each connection actually lived before retirement, so that I can compare actual connection lifetimes against configured lifetime and detect premature retirements.

#### Acceptance Criteria

1. WHEN the Reservoir retires a connection, THE Reservoir SHALL record `tokeira_dsql_reservoir_connection_age_seconds` as a histogram with label `retirement_reason`.
2. THE Reservoir SHALL compute the age as the elapsed time since the connection was originally created.
3. THE Reservoir SHALL use `retirement_reason` values matching the existing retirement reason labels: `expired`, `unhealthy`, `guard_window`, or `budget_cap`.

---

## Phase 6: Rate Limiter Internals

### Requirement 22: Token Bucket Fill Level

**User Story:** As a Tokeira operator, I want a gauge showing the tokens remaining in the rate limiter bucket, so that I can detect when the rate limiter is close to throttling new connection creation.

#### Acceptance Criteria

1. THE Rate_Limiter SHALL record `tokeira_dsql_rate_limiter_tokens_remaining` as a gauge reflecting the current number of tokens available in the token bucket.
2. THE Rate_Limiter SHALL update this gauge after each token acquisition and after each refill event.

### Requirement 23: Requests Throttled Counter

**User Story:** As a Tokeira operator, I want a counter for requests that had to wait for a rate limiter token, so that I can detect when connection creation demand exceeds the configured rate.

#### Acceptance Criteria

1. WHEN a connection creation request must wait because the token bucket is empty, THE Rate_Limiter SHALL increment `tokeira_dsql_rate_limiter_throttled_total`.
2. THE Rate_Limiter SHALL count each throttle event independently — a single request that waits counts as one throttle event regardless of wait duration.

### Requirement 24: Throttle Wait Duration

**User Story:** As a Tokeira operator, I want a histogram measuring how long throttled requests waited for a rate limiter token, so that I can quantify the latency impact of rate limiting on connection creation.

#### Acceptance Criteria

1. WHEN a throttled request acquires a token after waiting, THE Rate_Limiter SHALL record `tokeira_dsql_rate_limiter_throttle_duration_seconds` as a histogram.
2. THE Rate_Limiter SHALL measure duration from the moment the request was throttled until the token was acquired.

---

## Phase 7: Query-Level Decomposition

### Requirement 25: SQL Execution Time

**User Story:** As a Tokeira operator, I want a histogram measuring just the SQL query execution time (excluding checkout wait), so that I can decompose total operation latency into checkout overhead vs actual DSQL query time.

#### Acceptance Criteria

1. WHEN a SQL query completes execution, THE Run_Repository SHALL record `tokeira_storage_dsql_query_duration_seconds` as a histogram with labels `operation` and `outcome`.
2. THE Run_Repository SHALL measure duration from the moment the query is submitted to the connection through result receipt, excluding connection acquisition time.
3. THE Run_Repository SHALL use the same `operation` and `outcome` label values as the full operation duration histogram (Requirement 1).

### Requirement 26: Rows Read Per Operation

**User Story:** As a Tokeira operator, I want a histogram measuring rows returned by each read operation, so that I can detect unexpectedly large result sets and correlate row counts with latency.

#### Acceptance Criteria

1. WHEN a read operation completes, THE Run_Repository SHALL record `tokeira_storage_dsql_rows_read` as a histogram with label `operation`.
2. THE Run_Repository SHALL record the number of rows returned by the query (0 for queries that return no rows).

### Requirement 27: Rows Written Per Operation

**User Story:** As a Tokeira operator, I want a histogram measuring rows affected by each write operation, so that I can detect bulk writes and correlate write volume with OCC conflict rates.

#### Acceptance Criteria

1. WHEN a write operation completes, THE Run_Repository SHALL record `tokeira_storage_dsql_rows_written` as a histogram with label `operation`.
2. THE Run_Repository SHALL record the number of rows affected by the write (as reported by the database driver's rows-affected count).

### Requirement 28: Transaction Retries Per Commit

**User Story:** As a Tokeira operator, I want a histogram measuring how many retries each successful commit needed, so that I can distinguish first-attempt successes from operations that required multiple attempts.

#### Acceptance Criteria

1. WHEN a commit operation completes successfully (after zero or more retries), THE Run_Repository SHALL record `tokeira_storage_dsql_commit_retries` as a histogram.
2. THE Run_Repository SHALL record the value 0 for operations that succeed on the first attempt, and the retry count for operations that required retries.

---

## Phase 8: Reservoir Predictive Signals

### Requirement 29: Reservoir Utilization Ratio Gauge

**User Story:** As a Tokeira operator, I want a gauge showing the ratio of in-flight connections to total managed connections, so that I can detect when the reservoir is approaching exhaustion (ratio approaching 1.0).

#### Acceptance Criteria

1. THE Reservoir SHALL record `tokeira_dsql_reservoir_utilization_ratio` as a gauge computed as `in_flight / (in_flight + ready)`.
2. THE Reservoir SHALL update this gauge after each checkout and return event.
3. IF both `in_flight` and `ready` are zero, THE Reservoir SHALL record the utilization ratio as 0.0.

---

## Phase 9: Projection Deep Metrics

### Requirement 30: Visibility Query Latency

**User Story:** As a Tokeira operator, I want a histogram measuring the read path latency for visibility queries (List/Count), so that I can monitor the user-facing query performance independently from the write path.

#### Acceptance Criteria

1. WHEN a DSQL visibility query (List or Count) completes, THE Dsql_Visibility_Store SHALL record `tokeira_projection_visibility_query_duration_seconds` as a histogram with label `query_type`.
2. THE Dsql_Visibility_Store SHALL use `query_type` values of `list` and `count` to distinguish the two query patterns.
3. THE Dsql_Visibility_Store SHALL measure duration from query submission through result return inside the `VisibilityStore` implementation.

### Requirement 31: Search Attribute Index Query Attribution

**User Story:** As a Tokeira operator, I want visibility query duration attributed to referenced search attribute index tables, so that I can identify which custom-attribute predicates correlate with slow visibility queries.

#### Acceptance Criteria

1. WHEN a DSQL visibility query references one or more search attribute index tables in its compiled SQL, THE Dsql_Visibility_Store SHALL record `tokeira_projection_sa_index_scan_duration_seconds` as a histogram with label `index_table`.
2. THE Dsql_Visibility_Store SHALL measure the full DSQL query duration once per referenced search-attribute index table, because current filter compilation embeds index access as subqueries inside the list/count SQL rather than issuing standalone index scans.
3. THE Dsql_Visibility_Store SHALL derive `index_table` labels from the typed index tables named by the compiled filter or custom group-by path (`sa_keyword_idx`, `sa_keyword_list_idx`, `sa_int_idx`, `sa_bool_idx`, `sa_double_idx`, `sa_datetime_idx`, `sa_text_token_idx`).

### Requirement 32: Projection Checkpoint Write Latency

**User Story:** As a Tokeira operator, I want a histogram measuring the time to persist the projection worker's cursor checkpoint, so that I can detect when checkpoint writes become a bottleneck for projection throughput.

#### Acceptance Criteria

1. WHEN a projection worker persists its cursor checkpoint, THE Projection_Worker SHALL record `tokeira_projection_checkpoint_write_duration_seconds` as a histogram with label `partition_id`.
2. THE Projection_Worker SHALL measure duration from the start of the checkpoint write through confirmation of persistence.

---

## Phase 10: Per-Shard Distribution

### Requirement 33: Operations Per Shard Counter

**User Story:** As a Tokeira operator, I want a counter with shard labels for each operation, so that I can detect shard hotspots where one shard receives disproportionate traffic.

#### Acceptance Criteria

1. WHEN a DSQL operation targets a specific shard, THE Run_Repository SHALL increment `tokeira_storage_dsql_shard_operation_total` with labels `shard_id` and `operation`.
2. THE Run_Repository SHALL derive the `shard_id` label from the shard key used in the query.

### Requirement 34: Conflicts Per Shard Counter

**User Story:** As a Tokeira operator, I want a counter for OCC conflicts with shard labels, so that I can identify which shards are contended and correlate contention with shard key distribution.

#### Acceptance Criteria

1. WHEN a DSQL operation on a specific shard returns SQLSTATE 40001, THE Run_Repository SHALL increment `tokeira_storage_dsql_shard_conflict_total` with label `shard_id`.
2. THE Run_Repository SHALL derive the `shard_id` label from the shard key of the conflicting operation.

### Requirement 35: Latency Per Shard Histogram

**User Story:** As a Tokeira operator, I want a latency histogram with shard labels, so that I can detect if one shard is consistently slower than others (indicating data skew or infrastructure issues).

#### Acceptance Criteria

1. WHEN a DSQL operation targeting a specific shard completes, THE Run_Repository SHALL record `tokeira_storage_dsql_shard_duration_seconds` as a histogram with label `shard_id`.
2. THE Run_Repository SHALL measure the same duration as the per-operation latency histogram (Requirement 1) but with shard-level granularity.

---

## Phase 11: Error Classification

### Requirement 36: Connection Error Classification Counter

**User Story:** As a Tokeira operator, I want a counter for connection errors classified by error kind, so that I can distinguish between network issues (reset, timeout, DNS, TLS) and application-level failures.

#### Acceptance Criteria

1. WHEN a DSQL connection fails due to a network or transport error, THE Connection_Director SHALL increment `tokeira_dsql_connection_error_total` with label `error_kind`.
2. THE Connection_Director SHALL classify errors into the following `error_kind` values: `reset`, `timeout`, `dns`, `tls`, `refused`.
3. THE Connection_Director SHALL count connection errors during creation, checkout validation, and mid-operation failures.

### Requirement 37: DSQL Error Code Counter

**User Story:** As a Tokeira operator, I want a counter for DSQL-specific SQLSTATE error codes beyond OCC (40001), so that I can detect rate limiting (53400), server unavailability (57P03), connection loss (08006), and other DSQL-specific failure modes.

#### Acceptance Criteria

1. WHEN a DSQL operation returns a SQLSTATE error code, THE Run_Repository SHALL increment `tokeira_dsql_error_code_total` with label `sqlstate`.
2. THE Run_Repository SHALL record all SQLSTATE codes encountered, including but not limited to: `40001` (serialization failure), `53400` (connection rate limit), `57P03` (server not available), `08006` (connection failure).
3. THE Run_Repository SHALL record the raw 5-character SQLSTATE code as the label value.

---

## Phase 12: Extended Dashboard Updates

### Requirement 38: Connection Lifecycle Dashboard Row

**User Story:** As a Tokeira operator, I want a "Connection Lifecycle" dashboard row showing checkout wait time, creation time, and connection age distribution, so that I can diagnose where connection-related latency originates.

#### Acceptance Criteria

1. THE Dashboard SHALL include a "Connection Lifecycle" row with panels for: checkout wait time (p50, p95 by class), connection creation time (p50, p95), connection validation time (p50, p95), and connection age at retirement distribution.
2. THE Dashboard checkout wait panel SHALL use `tokeira_dsql_pool_checkout_duration_seconds{quantile="0.5"}` and `tokeira_dsql_pool_checkout_duration_seconds{quantile="0.95"}` queries grouped by `class` label.
3. THE Dashboard connection creation panel SHALL use `tokeira_dsql_reservoir_connection_create_duration_seconds{quantile="0.5"}` and `tokeira_dsql_reservoir_connection_create_duration_seconds{quantile="0.95"}`.
4. THE Dashboard connection age panel SHALL use `tokeira_dsql_reservoir_connection_age_seconds{quantile="0.5"}` and `tokeira_dsql_reservoir_connection_age_seconds{quantile="0.95"}` grouped by `retirement_reason`.
5. THE Dashboard panels SHALL use datasource UID `mimir`.

### Requirement 39: Rate Limiter Dashboard Row

**User Story:** As a Tokeira operator, I want a "Rate Limiter" dashboard row showing token fill level, throttled requests, and throttle duration, so that I can detect when the rate limiter is constraining connection creation.

#### Acceptance Criteria

1. THE Dashboard SHALL include a "Rate Limiter" row with panels for: token bucket fill level over time, throttled requests rate, and throttle wait duration (p50, p95).
2. THE Dashboard token fill panel SHALL use `tokeira_dsql_rate_limiter_tokens_remaining` as a timeseries gauge.
3. THE Dashboard throttled rate panel SHALL use `rate(tokeira_dsql_rate_limiter_throttled_total[5m])`.
4. THE Dashboard throttle duration panel SHALL use `tokeira_dsql_rate_limiter_throttle_duration_seconds{quantile="0.5"}` and `tokeira_dsql_rate_limiter_throttle_duration_seconds{quantile="0.95"}`.
5. THE Dashboard panels SHALL use datasource UID `mimir`.

### Requirement 40: Query Decomposition Dashboard Row

**User Story:** As a Tokeira operator, I want a "Query Decomposition" dashboard row showing SQL execution time vs checkout time, rows read/written, and commit retries, so that I can pinpoint whether latency is in the database or in connection management.

#### Acceptance Criteria

1. THE Dashboard SHALL include a "Query Decomposition" row with panels for: SQL execution time (p50, p95 by operation), checkout vs execution time comparison, rows read per operation, rows written per operation, and commit retry distribution.
2. THE Dashboard SQL execution panel SHALL use `tokeira_storage_dsql_query_duration_seconds{quantile="0.5"}` and `tokeira_storage_dsql_query_duration_seconds{quantile="0.95"}` grouped by `operation`.
3. THE Dashboard rows read panel SHALL use `tokeira_storage_dsql_rows_read{quantile="0.95"}` grouped by `operation`.
4. THE Dashboard rows written panel SHALL use `tokeira_storage_dsql_rows_written{quantile="0.95"}` grouped by `operation`.
5. THE Dashboard commit retries panel SHALL use `tokeira_storage_dsql_commit_retries{quantile="0.95"}`.
6. THE Dashboard panels SHALL use datasource UID `mimir`.

### Requirement 41: Shard Distribution Dashboard Row

**User Story:** As a Tokeira operator, I want a "Shard Distribution" dashboard row showing per-shard operation rates, conflict rates, and latency, so that I can detect hotspots and data skew across shards.

#### Acceptance Criteria

1. THE Dashboard SHALL include a "Shard Distribution" row with panels for: per-shard operation rate, per-shard conflict rate, and per-shard latency (p95).
2. THE Dashboard shard operation panel SHALL use `sum by (shard_id) (rate(tokeira_storage_dsql_shard_operation_total[5m]))`.
3. THE Dashboard shard conflict panel SHALL use `sum by (shard_id) (rate(tokeira_storage_dsql_shard_conflict_total[5m]))`.
4. THE Dashboard shard latency panel SHALL use `tokeira_storage_dsql_shard_duration_seconds{quantile="0.95"}` grouped by `shard_id`.
5. THE Dashboard panels SHALL use datasource UID `mimir`.

### Requirement 42: Predictive Signals Dashboard Row

**User Story:** As a Tokeira operator, I want a "Predictive Signals" dashboard row showing reservoir utilization and refill vs retirement rate, so that I can anticipate reservoir pressure before it causes latency spikes.

#### Acceptance Criteria

1. THE Dashboard SHALL include a "Predictive Signals" row with panels for: reservoir utilization ratio and refill rate vs retirement rate comparison.
2. THE Dashboard utilization ratio panel SHALL use `tokeira_dsql_reservoir_utilization_ratio` as a timeseries gauge with a threshold line at 0.8.
3. THE Dashboard refill vs retirement panel SHALL use `rate(tokeira_dsql_pool_connections_created_total[5m])` and `rate(tokeira_dsql_pool_connections_retired_total[5m])` as overlaid timeseries.
4. THE Dashboard panels SHALL use datasource UID `mimir`.

### Requirement 43: Error Classification Dashboard Row

**User Story:** As a Tokeira operator, I want an "Error Classification" dashboard row showing connection error kinds and SQLSTATE codes, so that I can quickly identify the category of failures affecting the DSQL layer.

#### Acceptance Criteria

1. THE Dashboard SHALL include an "Error Classification" row with panels for: connection errors by kind, and DSQL SQLSTATE error codes.
2. THE Dashboard connection error panel SHALL use `sum by (error_kind) (rate(tokeira_dsql_connection_error_total[5m]))`.
3. THE Dashboard SQLSTATE panel SHALL use `sum by (sqlstate) (rate(tokeira_dsql_error_code_total[5m]))`.
4. THE Dashboard panels SHALL use datasource UID `mimir`.
5. THE Dashboard panels SHALL include description annotations explaining common error kinds and SQLSTATE codes with their operational meaning.

### Requirement 44: Projection Deep Metrics Dashboard Row

**User Story:** As a Tokeira operator, I want a "Projection Deep Metrics" dashboard row showing record-count lag, visibility query latency, search-attribute-attributed query latency, and checkpoint write latency, so that I can diagnose projection pipeline bottlenecks.

#### Acceptance Criteria

1. THE Dashboard SHALL include a "Projection Deep Metrics" row with panels for: existing record-count lag per partition, visibility query latency (p50, p95 by query type), search-attribute-attributed query latency (p95 by index table), and checkpoint write latency (p95 by partition).
2. THE Dashboard record-lag panel SHALL use the existing `tokeira_projection_worker_lag_records` gauge grouped by `partition_id`.
3. THE Dashboard visibility query panel SHALL use `tokeira_projection_visibility_query_duration_seconds{quantile="0.5"}` and `tokeira_projection_visibility_query_duration_seconds{quantile="0.95"}` grouped by `query_type`.
4. THE Dashboard search-attribute attribution panel SHALL use `tokeira_projection_sa_index_scan_duration_seconds{quantile="0.95"}` grouped by `index_table`.
5. THE Dashboard checkpoint panel SHALL use `tokeira_projection_checkpoint_write_duration_seconds{quantile="0.95"}` grouped by `partition_id`.
6. THE Dashboard panels SHALL use datasource UID `mimir`.
