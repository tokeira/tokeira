# Implementation Plan: DSQL Observability Metrics

## Overview

Add comprehensive metrics instrumentation to the DSQL storage and projection layers. The implementation follows the established pattern: per-crate `metrics.rs` modules with `METRIC_NAMES` manifests and thin recording helper functions, called from operational code paths. Each task is mechanical — adding constants, helpers, and recording calls at the right locations.

## Tasks

- [x] 1. Add metric constants and recording helpers to `tokeira-storage::metrics`
  - [x] 1.1 Add Phase 1 metric constants (DSQL_OPERATION_DURATION_SECONDS, DSQL_OCC_CONFLICT_TOTAL, DSQL_RETRY_TOTAL, DSQL_OPERATION_TOTAL) and register in METRIC_NAMES manifest
    - Add the four constants to `crates/tokeira-storage/src/metrics.rs`
    - Add entries to the `METRIC_NAMES` manifest with correct `MetricType` annotations
    - _Requirements: 1.1, 2.1, 3.1, 4.1, 17.1_

  - [x] 1.2 Add Phase 1 recording helpers (record_dsql_operation_duration, record_dsql_occ_conflict, record_dsql_retry, record_dsql_operation_total)
    - Implement each helper with correct labels (operation, outcome)
    - Add doc comment on each helper explaining WHAT the metric measures and WHY an operator would look at it
    - _Requirements: 1.1, 2.1, 3.1, 4.1_

  - [x] 1.3 Add Phase 2 metric constant (DSQL_RESERVOIR_IN_FLIGHT) and register in METRIC_NAMES
    - Add only the new in-flight gauge constant to `crates/tokeira-storage/src/metrics.rs`
    - Reuse existing `DSQL_POOL_CONNECTIONS_TOTAL` for ready connection count
    - _Requirements: 5.1, 6.1, 17.1_

  - [x] 1.4 Add Phase 2 recording helper (set_dsql_reservoir_in_flight)
    - Implement the new gauge-setting helper
    - Reuse existing `record_dsql_pool_connections_total(count)` for ready connection count
    - Add doc comments explaining operational meaning
    - _Requirements: 5.1, 6.1_

  - [x] 1.5 Add Phase 3 metric constants (DSQL_PROJECTION_READ_DURATION_SECONDS, DSQL_PROJECTION_BATCH_SIZE) and register in METRIC_NAMES
    - _Requirements: 10.1, 11.1, 17.1_

  - [x] 1.6 Add Phase 3 recording helpers (record_dsql_projection_read_duration, record_dsql_projection_batch_size)
    - Implement with `partition_id` label (u32 → String)
    - Add doc comments
    - _Requirements: 10.1, 11.1_

  - [x] 1.7 Add Phase 5 metric constants (DSQL_RESERVOIR_CONNECTION_CREATE_DURATION_SECONDS, DSQL_RESERVOIR_CONNECTION_VALIDATE_DURATION_SECONDS, DSQL_RESERVOIR_CONNECTION_AGE_SECONDS) and register in METRIC_NAMES
    - Reuse existing `DSQL_POOL_CHECKOUT_DURATION_SECONDS` for checkout wait time
    - _Requirements: 18.1, 19.1, 20.1, 21.1, 17.1_

  - [x] 1.8 Add Phase 5 recording helpers (record_dsql_reservoir_connection_create_duration, record_dsql_reservoir_connection_validate_duration, record_dsql_reservoir_connection_age)
    - Reuse existing `record_dsql_pool_checkout_duration(class, duration)` for checkout wait time
    - Implement with correct labels (retirement_reason)
    - Add doc comments explaining duration decomposition and operational use
    - _Requirements: 18.1, 19.1, 20.1, 21.1_

  - [x] 1.9 Add Phase 6 metric constants (DSQL_RATE_LIMITER_TOKENS_REMAINING, DSQL_RATE_LIMITER_THROTTLED_TOTAL, DSQL_RATE_LIMITER_THROTTLE_DURATION_SECONDS) and register in METRIC_NAMES
    - _Requirements: 22.1, 23.1, 24.1, 17.1_

  - [x] 1.10 Add Phase 6 recording helpers (set_dsql_rate_limiter_tokens_remaining, record_dsql_rate_limiter_throttled, record_dsql_rate_limiter_throttle_duration)
    - Add doc comments
    - _Requirements: 22.1, 23.1, 24.1_

  - [x] 1.11 Add Phase 7 metric constants (DSQL_QUERY_DURATION_SECONDS, DSQL_ROWS_READ, DSQL_ROWS_WRITTEN, DSQL_COMMIT_RETRIES) and register in METRIC_NAMES
    - Add `MetricType::Histogram` to `crates/tokeira-types/src/observability.rs` for unitless distribution metrics
    - Update `validate_metric_name` and its property generator to cover the new unitless histogram variant
    - _Requirements: 25.1, 26.1, 27.1, 28.1, 17.1, 17.3_

  - [x] 1.12 Add Phase 7 recording helpers (record_dsql_query_duration, record_dsql_rows_read, record_dsql_rows_written, record_dsql_commit_retries)
    - Implement with correct labels (operation, outcome for query_duration)
    - Add doc comments explaining query-level decomposition
    - _Requirements: 25.1, 26.1, 27.1, 28.1_

  - [x] 1.13 Add Phase 8 metric constant (DSQL_RESERVOIR_UTILIZATION_RATIO) and register in METRIC_NAMES
    - _Requirements: 29.1, 17.1_

  - [x] 1.14 Add Phase 8 recording helper (set_dsql_reservoir_utilization_ratio)
    - Implement utilization ratio with the (0,0) → 0.0 edge case
    - Add doc comments explaining predictive signal purpose
    - _Requirements: 29.1, 29.2, 29.3_

  - [x] 1.15 Add Phase 10 metric constants (DSQL_SHARD_OPERATION_TOTAL, DSQL_SHARD_CONFLICT_TOTAL, DSQL_SHARD_DURATION_SECONDS) and register in METRIC_NAMES
    - _Requirements: 33.1, 34.1, 35.1, 17.1_

  - [x] 1.16 Add Phase 10 recording helpers (record_dsql_shard_operation, record_dsql_shard_conflict, record_dsql_shard_duration)
    - Implement with `shard_id` label (u32 → String)
    - Add doc comments explaining shard distribution visibility
    - _Requirements: 33.1, 34.1, 35.1_

  - [x] 1.17 Add Phase 11 metric constants (DSQL_CONNECTION_ERROR_TOTAL, DSQL_ERROR_CODE_TOTAL) and register in METRIC_NAMES
    - _Requirements: 36.1, 37.1, 17.1_

  - [x] 1.18 Add Phase 11 recording helpers (record_dsql_connection_error, record_dsql_error_code)
    - Implement with labels (error_kind, sqlstate)
    - Add doc comments explaining error classification categories
    - _Requirements: 36.1, 37.1_

  - [x] 1.19 Add classify_outcome helper function and classify_connection_error / extract_sqlstate utilities
    - Implement `classify_outcome` mapping Ok → "success", serialization failure → "conflict", other → "error"
    - Implement `classify_connection_error` mapping IO error kinds to error_kind labels
    - Implement `extract_sqlstate` extracting 5-char SQLSTATE from sqlx::Error::Database
    - Add doc comments on each
    - _Requirements: 1.3, 36.2, 37.3_

- [x] 2. Checkpoint — Verify metric constants and helpers compile
  - Ensure all tests pass (`cargo test -p tokeira-storage`), ask the user if questions arise.

- [x] 3. Add metric constants and recording helpers to `tokeira-projection::metrics`
  - [x] 3.1 Add Phase 9 metric constants (VISIBILITY_QUERY_DURATION_SECONDS, SA_INDEX_SCAN_DURATION_SECONDS, CHECKPOINT_WRITE_DURATION_SECONDS) and register in METRIC_NAMES
    - Add to `crates/tokeira-projection/src/metrics.rs`
    - Do not add wall-clock worker lag; keep using existing `tokeira_projection_worker_lag_records`
    - _Requirements: 30.1, 31.1, 32.1, 17.2_

  - [x] 3.2 Add Phase 9 recording helpers (record_visibility_query_duration, record_sa_index_scan_duration, record_checkpoint_write_duration)
    - Implement with correct labels (partition_id, query_type, index_table)
    - Add doc comments explaining projection deep metrics purpose
    - _Requirements: 30.1, 31.1, 32.1_

- [x] 4. Wire recording calls into DsqlRunRepository
  - [x] 4.1 Add operation duration and throughput recording to all DsqlRunRepository methods
    - At the end of each method: record_dsql_operation_duration, record_dsql_operation_total
    - Use the method name as the operation label, including shard-scoped variants
    - Measure duration from before connection acquisition through query completion
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 4.1, 4.2_

  - [x] 4.2 Add OCC conflict and retry recording to DsqlRunRepository retry paths
    - On serialization failure: record_dsql_occ_conflict
    - On retry outcome: record_dsql_retry with success/exhausted
    - _Requirements: 2.1, 2.2, 3.1, 3.2_

  - [x] 4.3 Add query-level decomposition recording (query duration, rows read, rows written, commit retries)
    - Wrap SQL execution with timing for record_dsql_query_duration
    - After read queries: record_dsql_rows_read with row count
    - After write queries: record_dsql_rows_written with rows_affected
    - After successful commits: record_dsql_commit_retries with retry count
    - _Requirements: 25.1, 25.2, 26.1, 26.2, 27.1, 27.2, 28.1, 28.2_

  - [x] 4.4 Add per-shard distribution recording to DsqlRunRepository
    - Compute shard_id from RunKey before operation
    - After operation: record_dsql_shard_operation, record_dsql_shard_duration
    - On conflict: record_dsql_shard_conflict
    - _Requirements: 33.1, 33.2, 34.1, 34.2, 35.1, 35.2_

  - [x] 4.5 Add error classification recording to DsqlRunRepository error paths
    - On connection errors: record_dsql_connection_error with classified error_kind
    - On SQLSTATE errors: record_dsql_error_code with raw 5-char code
    - _Requirements: 36.1, 36.2, 37.1, 37.2, 37.3_

- [x] 5. Checkpoint — Verify DsqlRunRepository instrumentation compiles
  - Ensure all tests pass (`cargo test -p tokeira-storage`), ask the user if questions arise.

- [x] 6. Wire recording calls into Reservoir and DsqlConnectionDirector
  - [x] 6.1 Add reservoir ready connections gauge updates to Reservoir checkout/return/create/retire paths
    - After each checkout: `record_dsql_pool_connections_total`
    - After each return: `record_dsql_pool_connections_total`
    - After each creation: `record_dsql_pool_connections_total`, call existing `record_dsql_pool_connection_created`
    - After each retirement: `record_dsql_pool_connections_total`, call existing `record_dsql_pool_connection_retired`
    - _Requirements: 5.1, 5.2, 8.1, 8.2, 8.3_

  - [x] 6.2 Add in-flight gauge and checkout duration recording to DsqlConnectionDirector.acquire/release
    - In acquire(): measure checkout duration, call existing `record_dsql_pool_checkout_duration`, increment in-flight gauge
    - In DsqlPermit::drop(): decrement in-flight gauge
    - _Requirements: 6.1, 6.2, 18.1, 18.2, 18.3_

  - [x] 6.3 Add connection creation duration recording to Reservoir refiller
    - Wrap connection creation with timing, record_dsql_reservoir_connection_create_duration
    - _Requirements: 19.1, 19.2_

  - [x] 6.4 Add connection validation duration recording to Reservoir return processor
    - Wrap validation with timing, record_dsql_reservoir_connection_validate_duration
    - _Requirements: 20.1, 20.2_

  - [x] 6.5 Add connection age recording to Reservoir scanner (retirement path)
    - Compute age from entry.created_at.elapsed(), record_dsql_reservoir_connection_age with retirement_reason
    - _Requirements: 21.1, 21.2, 21.3_

  - [x] 6.6 Add reservoir empty event recording to Reservoir checkout path
    - Call existing record_dsql_pool_empty_reservoir() when checkout finds empty reservoir
    - _Requirements: 7.1, 7.2_

  - [x] 6.7 Add connection error classification to Reservoir/Director error paths
    - On connection creation failure: record_dsql_connection_error
    - On validation failure: record_dsql_connection_error
    - _Requirements: 36.1, 36.3_

- [x] 7. Checkpoint — Verify Reservoir and Director instrumentation compiles
  - Ensure all tests pass (`cargo test -p tokeira-storage`), ask the user if questions arise.

- [x] 8. Wire recording calls into DsqlProjectionLog
  - [x] 8.1 Add projection read duration and batch size recording to DsqlProjectionLog::read_from
    - Measure duration from connection acquisition through query result decoding
    - Record record_dsql_projection_read_duration with partition_id
    - Record record_dsql_projection_batch_size with partition_id and row count (0 for empty)
    - _Requirements: 10.1, 10.2, 11.1, 11.2_

- [x] 9. Wire recording calls into TokenBucketRateLimiter
  - [x] 9.1 Add rate limiter metrics to TokenBucketRateLimiter acquire path
    - On throttle: record_dsql_rate_limiter_throttled
    - On throttle completion: record_dsql_rate_limiter_throttle_duration
    - After token acquisition/refill: set_dsql_rate_limiter_tokens_remaining
    - _Requirements: 22.1, 22.2, 23.1, 23.2, 24.1, 24.2_

- [x] 10. Implement periodic reporter task
  - [x] 10.1 Implement spawn_periodic_reporter in DsqlConnectionDirector
    - Create a tokio::spawn task with 5-second interval
    - Report class budget state for all configured classes (`control`, `commit`, `read`, `projection`, `maintenance`) via existing record_dsql_pool_class_budget
    - Report predictive signals: `record_dsql_pool_connections_total`, `set_dsql_reservoir_in_flight`, `set_dsql_reservoir_utilization_ratio`
    - _Requirements: 9.1, 9.2, 29.1, 29.2_

  - [x] 10.2 Wire spawn_periodic_reporter into DsqlConnectionDirector::start
    - Start the reporter task during director initialization
    - Store the JoinHandle for graceful shutdown
    - _Requirements: 9.1_

- [x] 11. Checkpoint — Verify all recording call wiring compiles
  - Ensure all tests pass (`cargo test -p tokeira-storage`), ask the user if questions arise.

- [x] 12. Wire recording calls into DSQL visibility and projection checkpoint paths
  - [x] 12.1 Add visibility query and search-attribute index metrics to `DsqlVisibilityStore`
    - In `list_executions`: record_visibility_query_duration with query_type `list`
    - In `count_executions`: record_visibility_query_duration with query_type `count`
    - Add a helper that derives referenced search-attribute index table names from the compiled filter/custom group-by path
    - After the DSQL list/count query completes, call record_sa_index_scan_duration once per referenced index table using the full query duration
    - _Requirements: 30.1, 30.2, 30.3, 31.1, 31.2, 31.3_

  - [x] 12.2 Add checkpoint write latency recording to projection checkpoint writes
    - On checkpoint writes: record_checkpoint_write_duration with partition_id
    - _Requirements: 32.1, 32.2_

- [x] 13. Checkpoint — Verify projection instrumentation compiles
  - Ensure all tests pass (`cargo test -p tokeira-projection`), ask the user if questions arise.

- [x] 14. Update dashboard JSON
  - [x] 14.1 Add DSQL Health stat row at the top of the dashboard
    - Add 4 stat panels: reservoir ready, OCC conflict rate, checkout p95, operation rate
    - Use datasource uid "mimir", lastNotNull reduce, 24-column grid
    - _Requirements: 13.1, 13.2, 13.3, 13.4_

  - [x] 14.2 Add DSQL Operations row with per-operation latency, rate by outcome, and conflict rate panels
    - Use summary quantile queries (NOT histogram_quantile)
    - Apply smooth line interpolation, showPoints never, pointSize 0
    - Bottom-placed table legends with lastNotNull, mean, max
    - _Requirements: 12.1, 12.2, 12.3, 14.1, 14.2, 14.3, 14.4, 14.5, 14.6_

  - [x] 14.3 Update existing DSQL Pool row with reservoir health panels
    - Add reservoir ready connections and in-flight timeseries
    - Preserve existing lifecycle and class budget panels
    - _Requirements: 15.1, 15.2, 15.3, 15.4, 15.5, 15.6_

  - [x] 14.4 Add Connection Lifecycle row (checkout wait, creation time, validation time, connection age)
    - Use summary quantile queries grouped by class/retirement_reason
    - _Requirements: 38.1, 38.2, 38.3, 38.4, 38.5_

  - [x] 14.5 Add Rate Limiter row (token fill level, throttled rate, throttle duration)
    - _Requirements: 39.1, 39.2, 39.3, 39.4, 39.5_

  - [x] 14.6 Add Query Decomposition row (SQL execution time, rows read, rows written, commit retries)
    - _Requirements: 40.1, 40.2, 40.3, 40.4, 40.5, 40.6_

  - [x] 14.7 Add Shard Distribution row (per-shard op rate, conflict rate, latency)
    - _Requirements: 41.1, 41.2, 41.3, 41.4, 41.5_

  - [x] 14.8 Add Predictive Signals row (utilization ratio with 0.8 threshold, refill vs retirement)
    - _Requirements: 42.1, 42.2, 42.3, 42.4_

  - [x] 14.9 Add Error Classification row (connection errors by kind, SQLSTATE codes)
    - Include description annotations explaining common error kinds and SQLSTATE codes
    - _Requirements: 43.1, 43.2, 43.3, 43.4, 43.5_

  - [x] 14.10 Add Projection Deep Metrics row (record lag, visibility query, SA index attribution, checkpoint write)
    - _Requirements: 44.1, 44.2, 44.3, 44.4, 44.5, 44.6_

  - [x] 14.11 Apply consistent style conventions across all new and existing panels
    - Ensure all timeseries use smooth interpolation, showPoints never, pointSize 0
    - Ensure all panels have description annotations
    - Ensure all panels use datasource uid "mimir"
    - Ensure all rate panels use explicit rate units (ops)
    - _Requirements: 16.1, 16.2, 16.3, 16.4, 16.5_

- [x] 15. Checkpoint — Verify dashboard JSON is valid
  - Parse the JSON to verify structural validity, ask the user if questions arise.

- [x] 16. Property tests and unit tests
  - [x]* 16.1 Write property test for metric name validation (Property 1)
    - **Property 1: Metric name validation**
    - Extend `tokeira-types` observability generators to include `MetricType::Histogram`
    - Iterate all entries in METRIC_NAMES (both tokeira-storage and tokeira-projection), call validate_metric_name
    - **Validates: Requirements 17.1, 17.2, 17.3, 17.4**

  - [x]* 16.2 Write property test for counter accounting accuracy (Property 2)
    - **Property 2: Counter accounting accuracy**
    - Generate random sequences of (operation, count) pairs, install DebuggingRecorder, replay, assert counter equals sum
    - **Validates: Requirements 2.1, 3.1, 4.1, 23.1, 33.1, 34.1, 36.1, 37.1**

  - [x]* 16.3 Write property test for histogram observation accuracy (Property 3)
    - **Property 3: Histogram observation accuracy**
    - Generate random sequences of (duration_ms, operation) pairs, install DebuggingRecorder, replay, assert observation count and values match
    - **Validates: Requirements 1.1, 10.1, 11.1, 18.1, 19.1, 20.1, 21.1, 24.1, 25.1, 26.1, 27.1, 28.1, 35.1**

  - [x]* 16.4 Write property test for gauge last-write-wins (Property 4)
    - **Property 4: Gauge last-write-wins**
    - Generate random sequences of f64 values, install DebuggingRecorder, set each, assert final snapshot equals last value
    - **Validates: Requirements 5.1, 6.1, 22.1, 29.1**

  - [x]* 16.5 Write property test for utilization ratio computation (Property 5)
    - **Property 5: Utilization ratio computation**
    - Generate random (in_flight: u32, ready: u32) pairs, compute expected ratio, call helper, assert gauge matches
    - Include (0, 0) edge case
    - **Validates: Requirements 29.1, 29.2, 29.3**

  - [x]* 16.6 Write property test for shard ID derivation determinism (Property 6)
    - **Property 6: Shard ID derivation determinism**
    - Generate random RunKey values and shard_count in 1..=1024, assert deterministic and in range [0, shard_count)
    - **Validates: Requirements 33.2, 34.2, 35.2**

  - [x]* 16.7 Write unit tests for outcome classification and error classification helpers
    - Test classify_outcome: Ok → "success", serialization failure → "conflict", other → "error"
    - Test classify_connection_error: IO error kinds → correct error_kind values
    - Test extract_sqlstate: database errors → 5-char code
    - _Requirements: 1.3, 36.2, 37.3_

  - [x]* 16.8 Write unit tests for dashboard JSON correctness
    - Assert no panel target contains `histogram_quantile(` or `_bucket`
    - Assert all panels use datasource uid "mimir"
    - Assert all timeseries panels have non-empty description
    - Assert style consistency (smooth interpolation, showPoints never, pointSize 0)
    - Assert legend consistency (bottom-placed table, lastNotNull/mean/max calcs)
    - _Requirements: 12.1, 12.2, 16.1, 16.2, 16.3, 16.4, 16.5_

- [x] 17. Final checkpoint — Ensure all tests pass
  - Ensure all tests pass (`cargo test --workspace`), ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation after each major phase
- Property tests validate universal correctness properties from the design document
- Unit tests validate specific examples and edge cases
- The design specifies Rust — all code examples use Rust with the `metrics` crate macros
- Recording helpers follow the established pattern: thin wrappers around `counter!`, `gauge!`, `histogram!` macros
- Doc comments on helpers explain WHAT the metric measures and WHY an operator would look at it (per AGENTS.md: "Comments explain WHY, not WHAT" — but doc comments on public API are the exception, they document the contract)

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.3", "1.5", "1.7", "1.9", "1.11", "1.13", "1.15", "1.17", "3.1"] },
    { "id": 1, "tasks": ["1.2", "1.4", "1.6", "1.8", "1.10", "1.12", "1.14", "1.16", "1.18", "1.19", "3.2"] },
    { "id": 2, "tasks": ["4.1", "4.2", "4.3", "8.1", "9.1"] },
    { "id": 3, "tasks": ["4.4", "4.5", "6.1", "6.2", "6.3", "6.4", "6.5", "6.6", "6.7"] },
    { "id": 4, "tasks": ["10.1", "12.1", "12.2"] },
    { "id": 5, "tasks": ["10.2"] },
    { "id": 6, "tasks": ["14.1", "14.2", "14.3", "14.4", "14.5", "14.6", "14.7", "14.8", "14.9", "14.10"] },
    { "id": 7, "tasks": ["14.11"] },
    { "id": 8, "tasks": ["16.1", "16.2", "16.3", "16.4", "16.5", "16.6", "16.7", "16.8"] }
  ]
}
```
