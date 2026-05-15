# Implementation Plan: DSQL Performance Fixes

## Overview

Eliminate the artificial serialization bottleneck in compose+DSQL deployments (5 wf/s → 50–100 wf/s at c=20). The implementation follows priority order: self-assign shards and local epoch validation first (unblock the primary bottleneck), then schema migration and projection fixes, then observability and configuration.

## Tasks

- [x] 1. Self-assign shards and local epoch validation (critical path)
  - [x] 1.1 Implement self-assignment loop in `build_and_serve_with_storage`
    - In `apps/tokeirad/src/lib.rs`, after runtime construction and before spawning projection workers
    - When `controller_endpoint` is `None`, loop over `0..shard_count` calling `try_acquire_bundle(shard, owner, node_endpoint)` for each
    - Record each acquired shard in ShardOwner with the epoch returned by `LeaseOutcome::Acquired { epoch }` or `LeaseOutcome::Renewed { epoch }`, then mark active
    - Treat `LeaseOutcome::Rejected` as a warning and continue with the remaining shards
    - Log warning on individual failures, log info with acquired count on completion
    - Skip self-assignment entirely when `controller_endpoint` is configured
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_

  - [x] 1.2 Add a runtime-owned self-assignment/epoch validation seam
    - Keep `ShardOwner` ownership in `tokeira-runtime`; do not add a `tokeira-storage` dependency on runtime types
    - Add a narrow runtime helper, for example `record_self_assigned_shard(shard_id, epoch)`, so `build_and_serve_with_storage` can record acquired shards without accessing private runtime fields
    - Thread a small runtime config flag, for example `controller_managed_placement`, from `controller_endpoint.is_some()` so the lane can choose between the zero-epoch no-controller fast path and the controller-managed durable epoch path
    - Ensure the lane can validate `ShardOwner::epoch_of(execution_home_bundle)` before calling storage
    - _Requirements: 2.1, 2.2_

  - [x] 1.3 Replace per-transition lease row SELECT with lane-local epoch validation in no-controller mode
    - In `crates/tokeira-runtime/src/lane.rs`, validate the derived execution-home shard against `ShardOwner::epoch_of(execution_home_bundle)` before calling storage
    - Return `CommitResult::Conflict` immediately if the shard is not locally owned
    - When `controller_endpoint` is `None`, call `commit_transition_for_bundle` with `ShardEpoch::ZERO` after successful local validation
    - When `controller_endpoint` is configured, pass the real local epoch to `commit_transition_for_bundle`
    - In the DSQL repository, bypass the per-transition `SELECT epoch FROM shard_lease` only for `ShardEpoch::ZERO`; keep the existing shard_lease read for non-zero epochs so controller-managed takeover remains durably fenced
    - _Requirements: 2.1, 2.3, 2.6, 2.7_

  - [ ]* 1.4 Write property test for self-assignment completeness (Property 1)
    - **Property 1: Self-assignment completeness**
    - For any `shard_count` in 1..64 and any failure pattern, verify the loop attempts all shards and ShardOwner contains exactly the successful ones with the returned lease epochs
    - **Validates: Requirements 1.1, 1.2, 1.3**

  - [ ]* 1.5 Write property test for local epoch validation (Property 2)
    - **Property 2: Local epoch validation without database query in no-controller mode**
    - For any ShardOwner state and any execution-home shard input, verify a missing local epoch returns Conflict without calling storage/SQL, a matching epoch in no-controller mode calls storage with `ShardEpoch::ZERO`, and a matching epoch in controller mode calls storage with the real epoch
    - **Validates: Requirements 2.1, 2.3, 2.6**

- [ ] 2. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 3. vis_rollup schema migration and partition sharding
  - [x] 3.1 Modify existing vis_rollup schema to include `partition_id`
    - Find the existing migration file that creates `vis_rollup` and modify it directly to include `partition_id INT NOT NULL DEFAULT 0` in the table definition
    - Update the PRIMARY KEY to `(namespace_id, dimension, value, partition_id)`
    - This is a direct schema change for fresh databases, not an ALTER TABLE migration
    - Document that existing DSQL databases with the old migration recorded in `schema_version` require a schema reset before this spec is applied
    - _Requirements: 3.1_

  - [x] 3.2 Update `accumulate_rollup` to accept and use `partition_id`
    - In `crates/tokeira-projection/src/dsql_store.rs`, add `partition_id: u32` parameter to `accumulate_rollup`
    - Update the INSERT/ON CONFLICT statement to include `partition_id` in both the values and conflict target
    - _Requirements: 3.2, 3.5_

  - [x] 3.3 Thread configured projection partition count through DSQL projection log writes and worker startup
    - Replace the hard-coded DSQL projection partition count with the configured `infrastructure.placement.partition_count`
    - Thread the configured value into `DsqlPoolConfig`/`DsqlStore` construction so `DsqlRunRepository` writes projection records into the same partition range that workers read
    - In `apps/tokeirad/src/lib.rs`, spawn exactly `effective_config.infrastructure.placement.partition_count` projection workers
    - Add a regression test that `partition_count = 4` never writes records into partitions 4..15
    - _Requirements: 3.2, 9.4_

  - [x] 3.4 Update visibility read path to SUM across partitions
    - In the rollup query (`count_workflow_executions` or equivalent), change to `SELECT dimension, value, SUM(counter) as counter FROM vis_rollup WHERE namespace_id = $1 GROUP BY dimension, value`
    - _Requirements: 3.3_

  - [x] 3.5 Extend `ProjectionSink::apply` trait and `DsqlVisibilityStore::apply` with `partition_id`
    - Add `partition_id: u32` to the `apply` method signature on the trait
    - Update `ProjectionWorker` to pass its partition_id to `sink.apply(record, partition_id)`
    - _Requirements: 3.4_

  - [ ]* 3.6 Write property test for partition-sharded rollup isolation (Property 3)
    - **Property 3: Partition-sharded rollup isolation**
    - For any two distinct partition_ids and any (namespace_id, dimension, value), verify separate rows are produced
    - **Validates: Requirements 3.2, 3.5**

  - [ ]* 3.7 Write property test for rollup read aggregation (Property 4)
    - **Property 4: Rollup read aggregation**
    - For any set of rollup entries across partitions, verify the read path returns the sum of all partition counters
    - **Validates: Requirements 3.3**

- [x] 4. OCC retry with connection release
  - [x] 4.1 Implement OCC retry loop in `accumulate_rollup`
    - In `crates/tokeira-projection/src/dsql_store.rs`, implement the acquire→try→drop(permit)→sleep→re-acquire pattern
    - Add `is_occ_conflict` helper to detect SQLSTATE OC000/40001
    - Compute delay as `10ms × attempt + random(0..50ms)`
    - Abandon after 5 attempts, propagate error
    - Record `tokeira_storage_dsql_occ_conflict_total` on each conflict
    - Record `tokeira_storage_dsql_retry_total` with outcome "success" or "exhausted"
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8_

  - [ ]* 4.2 Write property test for connection release during retry (Property 5)
    - **Property 5: Connection release during retry backoff**
    - Verify permit is dropped before sleep and re-acquired after
    - **Validates: Requirements 4.2, 4.3, 5.4**

  - [ ]* 4.3 Write property test for retry delay bounds (Property 6)
    - **Property 6: Retry delay bounds**
    - For attempt numbers 1..=5, verify delay is in `[10*n ms, 10*n + 50 ms)`
    - **Validates: Requirements 4.4**

- [x] 5. Single-transaction projection apply
  - [x] 5.1 Restructure `DsqlVisibilityStore::apply` for single-transaction execution+search-attrs
    - In `crates/tokeira-projection/src/dsql_store.rs`, wrap `upsert_execution` and search attribute upserts in a single transaction
    - Apply OCC retry at the transaction level (restart entire tx on conflict, up to 5 attempts)
    - Release permit before sleep on retry
    - Keep `accumulate_rollup` as separate autocommit operations after the transaction commits
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6_

  - [ ]* 5.2 Write property test for execution and search attribute atomicity (Property 7)
    - **Property 7: Execution and search attribute atomicity**
    - Verify either all writes commit or none commit
    - **Validates: Requirements 5.1**

  - [ ]* 5.3 Write property test for rollup independence (Property 8)
    - **Property 8: Rollup independence from execution transaction**
    - Verify execution data persists even if rollup fails
    - **Validates: Requirements 5.2**

- [ ] 6. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 7. Statement-level duration metrics
  - [x] 7.1 Add `tokeira_storage_dsql_statement_duration_seconds` histogram
    - Define the histogram with `operation` and `statement` labels
    - Use the same bucket configuration as existing DSQL duration histograms
    - Add `record_dsql_statement_duration` helper function in the metrics module
    - _Requirements: 6.5_

  - [x] 7.2 Instrument commit path statements
    - In `crates/tokeira-storage/src/dsql/run_repository.rs`, wrap each SQL statement in `commit_transition` with timing
    - Emit durations for: `load_hot`, `append_history`, `update_execution`, `dedupe_check`, `current_execution_check`
    - Use labels `operation="commit_transition"` and `statement=<name>`
    - _Requirements: 6.1, 6.2_

  - [x] 7.3 Instrument projection path statements
    - In `crates/tokeira-projection/src/dsql_store.rs`, wrap each SQL statement in `apply` with timing
    - Emit durations for: `upsert_execution`, `upsert_search_attr`, `upsert_rollup`
    - Use labels `operation="projection_apply"` and `statement=<name>`
    - _Requirements: 6.3, 6.4_

- [x] 8. Class permit wait metrics
  - [x] 8.1 Add permit wait duration and pool waiting gauge metrics
    - In `crates/tokeira-storage/src/dsql/connection.rs`, inside `ClassBudgets::acquire`
    - Record `tokeira_dsql_class_permit_wait_duration_seconds` histogram with `class` label
    - Maintain `tokeira_dsql_pool_waiting` gauge (increment before acquire, decrement after)
    - _Requirements: 7.1, 7.2_

  - [ ]* 8.2 Write property test for commit budget isolation (Property 9)
    - **Property 9: Commit budget isolation from projection**
    - Saturate projection permits, verify commit permits remain available
    - **Validates: Requirements 7.5**

- [x] 9. Generic storage metrics from DSQL path
  - [x] 9.1 Extend `record_dsql_commit_operation!` macro to emit generic metrics
    - In `crates/tokeira-storage/src/dsql/run_repository.rs`, after recording DSQL-specific metrics, also emit:
      - `tokeira_storage_repository_operation_total` with operation name and outcome
      - `tokeira_storage_commit_transition_duration_seconds` for commit operations
    - Add generic metric emission for `load_run` and `read_history` at their call sites
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5_

- [x] 10. Default configuration for DSQL deployments
  - [x] 10.1 Implement `apply_storage_defaults` in `TokeiraConfig`
    - In `crates/tokeira-config/src/lib.rs`, add `apply_storage_defaults(&mut self)` method
    - When storage is DSQL: promote `shard_count` from 1 to 32, promote `partition_count` from 16 to 4
    - Do not attempt to preserve an explicit `shard_count = 1` or `partition_count = 16`; the config model does not track explicitness, so DSQL treats those as legacy/default values by value
    - Preserve `shard_count > 1` and `partition_count != 16`
    - Call this method in `TokeiraConfig::resolve` after parsing, before validation
    - In `apps/tokeirad/src/lib.rs`, construct `DsqlPoolConfig` from the effective config rather than `DsqlPoolConfig::default()`: set `shard_count` from `effective_config.infrastructure.placement.shard_count`, and set projection partition count from `effective_config.infrastructure.placement.partition_count` once Task 3.3 adds that field
    - Pass the configured `DsqlPoolConfig` into `DsqlStore` construction so runtime routing and DSQL persistence use the same shard/partition counts
    - _Requirements: 1.6, 1.7, 9.1, 9.2, 9.3, 10.1, 10.2, 10.3, 10.4_

  - [x]* 10.2 Write property test for legacy default promotion (Property 10)
    - **Property 10: DSQL promotes legacy defaults by value**
    - For DSQL configs, verify `shard_count = 1` becomes 32 and `partition_count = 16` becomes 4
    - For DSQL configs, verify `shard_count > 1` and `partition_count != 16` are preserved
    - For in-memory configs, verify existing defaults remain unchanged
    - **Validates: Requirements 9.3, 10.3**

  - [x]* 10.3 Write property test for DSQL no single-shard (Property 11)
    - **Property 11: DSQL deployments never default to single-shard**
    - For any DSQL config with `shard_count=1` or unset, verify effective value > 1 after defaults
    - **Validates: Requirements 10.4**

- [ ] 11. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 12. Validation benchmark documentation
  - [x] 12.1 Document the validation benchmark command and success criteria
    - Add a section to `docs/compose-dsql-performance.md` (or equivalent) documenting:
      - Command: `cargo run -p tokeira-bench -- --workflows 2000 --concurrency 20`
      - Prerequisites: all fixes from Requirements 1–5 applied, DSQL storage configured, shard_count=32, partition_count=4
      - Success criteria: 50+ wf/s sustained throughput
      - Expected performance model from the design document
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.5, 11.6_

  - [x] 12.2 Apply dashboard style contract to DSQL performance panels
    - Update the compose observability dashboard definitions touched by this spec to use an aligned 24-column grid with consistent stat/timeseries layout within each row
    - Ensure every timeseries panel uses `lineInterpolation: "smooth"`, `showPoints: "never"`, and `pointSize: 0`
    - Ensure every timeseries panel uses a bottom table legend with `lastNotNull`, `mean`, and `max`
    - Ensure every rate panel uses a `rate()` PromQL expression and an explicit rate unit such as `ops`
    - Add a meaningful `description` to every panel explaining the signal and operator interpretation
    - Add regression coverage that renders or inspects the generated dashboard JSON for these style invariants
    - _Requirements: 12.1, 12.2, 12.3, 12.4, 12.5_

- [ ] 13. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation between major phases
- Property tests validate universal correctness properties from the design document
- Priority order: self-assign shards + local epoch validation (wave 0–1) unblock the primary bottleneck; schema migration and projection fixes follow (wave 2–3); observability and config are independent (wave 3–4)
- The implementation language is Rust throughout (matching the design document)

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2", "10.1"] },
    { "id": 1, "tasks": ["1.3", "1.4", "3.1", "10.2", "10.3"] },
    { "id": 2, "tasks": ["1.5", "3.2", "3.3", "3.4", "3.5", "7.1", "8.1"] },
    { "id": 3, "tasks": ["3.6", "3.7", "4.1", "7.2", "7.3", "8.2", "9.1"] },
    { "id": 4, "tasks": ["4.2", "4.3", "5.1"] },
    { "id": 5, "tasks": ["5.2", "5.3", "12.1", "12.2"] }
  ]
}
```
