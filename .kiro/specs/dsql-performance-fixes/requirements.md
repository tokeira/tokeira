# Requirements Document

## Introduction

This spec addresses performance fixes for the DSQL storage backend identified in `docs/compose-dsql-performance.md`. The observed 5 wf/s at c=20 is consistent with the DSQL commit path serializing through a single shard/fence row: 1/(3×0.0527s) = 6.3 wf/s matches the observation almost exactly. Single-shard serialization is the PRIMARY explanation. Projection OCC conflicts are secondary — they worsen pool pressure but are not needed to explain the headline throughput.

The fixes target: single-shard serialization (smoking gun), per-transition lease row locking, vis_rollup schema contention, projection OCC retry without connection release, multi-connection-acquisition per projection apply, missing observability for sub-operation timing and class permit contention, generic storage metrics on the DSQL path, default configuration for developer DSQL mode, and a validation benchmark to confirm the architecture.

Target: move compose+DSQL from 5 wf/s to 50–100 wf/s at c=20 by eliminating artificial serialization. Co-located estimates (1,000–3,000 wf/s) are aspirational bounds.

## Glossary

- **DsqlVisibilityStore**: The DSQL-backed implementation of `ProjectionSink` and `VisibilityStore` traits in `crates/tokeira-projection/src/dsql_store.rs`.
- **DsqlRunRepository**: The DSQL-backed implementation of `RunRepository` and `LeaseRepository` in `crates/tokeira-storage/src/dsql/run_repository.rs`.
- **OCC_Conflict**: An optimistic concurrency control serialization failure returned by Aurora DSQL with SQLSTATE OC000 or 40001.
- **vis_rollup**: The projection rollup table that maintains aggregate counters per (namespace_id, dimension, value).
- **ProjectionWorker**: A background task that reads from the projection log and applies records to a `ProjectionSink`.
- **ShardOwner**: The runtime component that tracks which shards the current node owns, with their epochs and lifecycle state.
- **ConnectionDirector**: The DSQL connection pool manager that enforces class-based budgets (commit, read, projection, control, maintenance).
- **partition_count**: The number of projection workers spawned at startup, each consuming one partition of the projection log.
- **shard_count**: The number of logical shards used by the runtime lane to derive `execution_home_bundle()` and epoch-fenced commit routing.
- **bundle_count**: Placement-controller routing metadata. It does not drive the current single-node lane commit routing path.
- **Generic_Storage_Metrics**: The `tokeira_storage_repository_operation_total`, `tokeira_storage_commit_transition_duration_seconds`, `tokeira_storage_load_run_duration_seconds`, and `tokeira_storage_read_history_duration_seconds` metric families.
- **Class_Permit**: A semaphore-based admission token from the ConnectionDirector that gates access to a pooled connection for a specific DbClass.
- **shard_lease**: The DSQL table that records bundle ownership with columns (shard_id, owner, epoch, expires_at). Used for fencing authority during takeover.

## Requirements

### Requirement 1: Self-Assign Shards in Single-Node Mode

**User Story:** As an operator running a compose deployment without a placement controller, I want tokeirad to self-assign all shards on startup with a multi-shard default, so that commits distribute across multiple fence rows and eliminate single-shard serialization.

#### Acceptance Criteria

1. WHILE `controller_endpoint` is not configured, WHEN tokeirad starts, THE Server SHALL call `try_acquire_bundle` for each shard in `0..shard_count`.
2. WHILE `controller_endpoint` is not configured, WHEN tokeirad starts, THE Server SHALL record each successfully acquired shard in the ShardOwner with the epoch returned by `LeaseOutcome::Acquired { epoch }` or `LeaseOutcome::Renewed { epoch }`.
3. IF `try_acquire_bundle` fails for any shard during self-assignment, THEN THE Server SHALL log a warning and continue attempting the remaining shards.
4. WHILE `controller_endpoint` is configured, THE Server SHALL NOT perform self-assignment and SHALL rely on the placement controller for shard/bundle acquisition.
5. WHEN self-assignment completes, THE Server SHALL log the count of successfully acquired shards at info level.
6. THE Server SHALL default `shard_count` to 32 for compose+DSQL deployments.
7. THE Server SHALL default `partition_count` to 4 for compose+DSQL deployments.

### Requirement 2: Eliminate Per-Transition Lease Row Locking in Single-Node Mode

**User Story:** As an operator running compose+DSQL without a placement controller, I want the commit path to skip the shard_lease read because there is no takeover scenario, while multi-node deployments keep the durable lease fence.

#### Acceptance Criteria

1. WHILE `controller_endpoint` is not configured, BEFORE the lane calls `commit_transition_for_bundle`, THE runtime lane SHALL validate ownership from local ShardOwner state, not by executing a SELECT against the shard_lease table.
2. WHEN a shard is acquired or renewed, THE ShardOwner SHALL record the shard and returned epoch locally.
3. WHEN the lane-local epoch check fails, THE runtime lane SHALL return a Conflict result without issuing any database query.
4. WHILE `controller_endpoint` is not configured, AFTER successful lane-local ownership validation, THE runtime lane SHALL call `commit_transition_for_bundle` with `ShardEpoch::ZERO` so the DSQL repository does not perform its legacy lease-row validation query.
5. WHILE `controller_endpoint` is configured, THE runtime lane SHALL pass the real lease epoch to `commit_transition_for_bundle` and the DSQL repository SHALL keep the existing shard_lease read for durable takeover fencing.
6. WHEN a takeover occurs in controller-managed mode, THE new owner SHALL increment the epoch in the shard_lease row and the old owner SHALL be invalidated through the existing durable epoch mismatch on subsequent commit attempts.
7. THE shard_lease row SHALL fence authority during acquisition and renewal using strong DSQL writes, and SHALL remain the per-transition durable fence for controller-managed deployments.

### Requirement 3: Per-Partition Rollup Sharding

**User Story:** As an operator, I want the vis_rollup table to be sharded by projection partition, so that concurrent projection workers never conflict on the same row and the OCC hotspot is removed.

#### Acceptance Criteria

1. THE vis_rollup table schema SHALL include a `partition_id` column as part of the primary key: `(namespace_id, dimension, value, partition_id)`.
2. WHEN a ProjectionWorker writes a rollup delta, THE DsqlVisibilityStore SHALL use the worker's partition_id as the `partition_id` value in the upsert.
3. WHEN the visibility API reads rollup counters, THE DsqlVisibilityStore SHALL sum across all partition_id values for a given (namespace_id, dimension, value) tuple.
4. THE DsqlVisibilityStore SHALL accept a partition_id parameter in its `apply` method to identify which partition the worker owns.
5. WHEN two ProjectionWorkers with different partition_ids write rollup deltas for the same (namespace_id, dimension, value), THE vis_rollup table SHALL store them as separate rows that do not conflict.

### Requirement 4: Projection OCC Retry with Connection Release

**User Story:** As an operator running a compose+DSQL deployment, I want the projection sink to retry OCC conflicts locally per-statement while releasing the connection during backoff sleep, so that conflicts do not hold pool connections idle and cannot starve the commit path.

#### Acceptance Criteria

1. WHEN `DsqlVisibilityStore::accumulate_rollup` encounters an OCC_Conflict on an individual rollup upsert, THE DsqlVisibilityStore SHALL retry that individual statement with jittered backoff.
2. WHEN an OCC_Conflict retry requires a backoff sleep, THE DsqlVisibilityStore SHALL release the pool connection before sleeping and re-acquire a connection after waking.
3. THE DsqlVisibilityStore SHALL NOT hold a pool connection or class permit while sleeping during retry backoff.
4. THE DsqlVisibilityStore SHALL compute retry delay as `10ms × attempt_number + random(0..50ms)` for each retry attempt.
5. THE DsqlVisibilityStore SHALL abandon the retry loop and propagate the error after 5 failed attempts.
6. WHEN an OCC_Conflict retry succeeds, THE DsqlVisibilityStore SHALL increment the `tokeira_storage_dsql_occ_conflict_total` counter with the operation label.
7. WHEN an OCC_Conflict retry is exhausted, THE DsqlVisibilityStore SHALL increment the `tokeira_storage_dsql_retry_total` counter with outcome "exhausted".
8. WHEN an OCC_Conflict retry succeeds after one or more failures, THE DsqlVisibilityStore SHALL increment the `tokeira_storage_dsql_retry_total` counter with outcome "success".

### Requirement 5: Single-Transaction Projection Apply (Execution and Search Attributes Only)

**User Story:** As an operator, I want the execution upsert and search attribute upserts within `DsqlVisibilityStore::apply()` to execute in a single transaction, so that connection acquisitions drop and partial application on failure is eliminated, while rollup remains as a separate autocommit operation.

#### Acceptance Criteria

1. WHEN `DsqlVisibilityStore::apply` is called for a projection record, THE DsqlVisibilityStore SHALL execute `upsert_execution` and search attribute upserts within a single database transaction.
2. THE DsqlVisibilityStore SHALL execute `accumulate_rollup` as separate autocommit operations outside the execution/search-attr transaction.
3. IF the execution/search-attr transaction encounters an OCC abort, THEN THE DsqlVisibilityStore SHALL restart the entire transaction scope, not retry individual statements within it.
4. THE DsqlVisibilityStore SHALL NOT sleep while holding a pool connection or class permit during any retry within `apply`.
5. WHEN the execution/search-attr transaction commits successfully, THE DsqlVisibilityStore SHALL proceed to rollup accumulation.
6. THE `accumulate_rollup` operation SHALL use per-statement retry with connection release as specified in Requirement 4.

### Requirement 6: Statement-Level Duration Decomposition Metrics

**User Story:** As an operator, I want sub-operation timing within commit_transition and projection_apply, so that I can diagnose where latency accumulates beyond the 52.7ms average.

#### Acceptance Criteria

1. WHEN `commit_transition_for_bundle` executes, THE DsqlRunRepository SHALL record `tokeira_storage_dsql_statement_duration_seconds` with labels `operation="commit_transition"` and `statement` set to the sub-operation name.
2. THE DsqlRunRepository SHALL emit statement-level durations for at least: `load_hot`, `append_history`, `update_execution`.
3. WHEN `DsqlVisibilityStore::apply` executes, THE DsqlVisibilityStore SHALL record `tokeira_storage_dsql_statement_duration_seconds` with labels `operation="projection_apply"` and `statement` set to the sub-operation name.
4. THE DsqlVisibilityStore SHALL emit statement-level durations for at least: `upsert_execution`, `upsert_rollup`.
5. THE `tokeira_storage_dsql_statement_duration_seconds` histogram SHALL use the same bucket configuration as existing DSQL duration histograms.

### Requirement 7: Class Permit Wait Duration Metrics and Hard Isolation Invariant

**User Story:** As an operator, I want to observe how long operations wait for class permits and have a guarantee that projection cannot starve commit, so that I can prove class isolation is working.

#### Acceptance Criteria

1. WHEN an operation waits for a class permit from the ConnectionDirector, THE ConnectionDirector SHALL record `tokeira_dsql_class_permit_wait_duration_seconds` with a `class` label.
2. THE ConnectionDirector SHALL maintain a `tokeira_dsql_pool_waiting` gauge that tracks the number of operations currently waiting for a class permit, labeled by `class`.
3. THE ConnectionDirector SHALL be the sole path for acquiring DSQL connections on all DSQL code paths.
4. THE ConnectionDirector SHALL NOT allow any projection path to bypass class budgets and acquire connections from the raw pool.
5. THE ConnectionDirector SHALL guarantee that commit class budget is reserved exclusively for commit operations and cannot be consumed by projection operations.

### Requirement 8: Emit Generic Storage Metrics from DSQL Path

**User Story:** As an operator, I want the DSQL storage path to emit the same generic storage metrics as the in-memory path, so that the "Repository Operations" dashboard panels work for both backends without modification.

#### Acceptance Criteria

1. WHEN `DsqlRunRepository::commit_transition` completes, THE DsqlRunRepository SHALL record a `tokeira_storage_commit_transition_duration_seconds` observation.
2. WHEN `DsqlRunRepository::load_run` completes, THE DsqlRunRepository SHALL record a `tokeira_storage_load_run_duration_seconds` observation.
3. WHEN `DsqlRunRepository::read_history` completes, THE DsqlRunRepository SHALL record a `tokeira_storage_read_history_duration_seconds` observation.
4. WHEN any `DsqlRunRepository` operation completes, THE DsqlRunRepository SHALL increment `tokeira_storage_repository_operation_total` with the operation name and outcome labels.
5. THE DsqlRunRepository SHALL emit generic storage metrics alongside the existing DSQL-specific metrics, not as a replacement.

### Requirement 9: Reduce Default Partition Count for DSQL

**User Story:** As an operator running a compose+DSQL deployment, I want the default partition_count to be 4 instead of 16, so that fewer projection workers compete for the projection connection budget.

#### Acceptance Criteria

1. WHILE `infrastructure.storage` is `dsql`, THE Server SHALL default `infrastructure.placement.partition_count` to 4.
2. WHILE `infrastructure.storage` is `in-memory`, THE Server SHALL continue to default `infrastructure.placement.partition_count` to 16.
3. WHILE `infrastructure.storage` is `dsql`, THE Server SHALL promote the legacy/default value `infrastructure.placement.partition_count = 16` to 4.
4. WHERE `infrastructure.placement.partition_count` is any value other than 16, THE Server SHALL use that value regardless of storage backend.
5. THE Server SHALL spawn exactly `partition_count` ProjectionWorkers at startup.

### Requirement 10: Multi-Shard Default for Compose DSQL

**User Story:** As an operator running a compose+DSQL deployment, I want the default shard_count to be 32 so that a developer DSQL deployment is a performance baseline, not a correctness smoke test.

#### Acceptance Criteria

1. WHILE `infrastructure.storage` is `dsql` and the deployment is compose, THE Server SHALL default `infrastructure.placement.shard_count` to 32.
2. WHILE `infrastructure.storage` is `in-memory`, THE Server SHALL continue to default `infrastructure.placement.shard_count` to 1.
3. WHILE `infrastructure.storage` is `dsql`, THE Server SHALL promote the legacy/default value `infrastructure.placement.shard_count = 1` to 32.
4. WHERE `infrastructure.placement.shard_count` is greater than 1, THE Server SHALL use that value regardless of storage backend.
5. THE Server SHALL NOT default to `shard_count = 1` for any DSQL deployment.

### Requirement 11: Validation Benchmark

**User Story:** As a developer, I want a documented benchmark that validates the DSQL architecture after self-assigning shards with shard_count=32 and partition_count=4, so that I can confirm the fixes achieve the target throughput.

#### Acceptance Criteria

1. WHEN the validation benchmark is run with `shard_count=32`, `partition_count=4`, and concurrency 20, THE benchmark SHALL target 50+ wf/s sustained throughput.
2. THE validation benchmark SHALL use the existing `tokeira-bench` binary with 2000 workflows at concurrency 20.
3. THE validation benchmark SHALL document the expected command: `cargo run -p tokeira-bench -- --workflows 2000 --concurrency 20`.
4. THE validation benchmark SHALL define success as achieving 50 wf/s or higher sustained throughput.
5. IF the validation benchmark achieves 50+ wf/s, THEN the DSQL architecture SHALL be considered vindicated for the compose+DSQL deployment model.
6. THE validation benchmark SHALL be run against a compose+DSQL deployment with all fixes from Requirements 1–5 applied.

### Requirement 12: Dashboard Layout and Panel Style

**User Story:** As an operator, I want the DSQL performance dashboard panels to be consistently laid out, annotated, and styled, so that the dashboard is readable under load and explains each signal without requiring source-code context.

#### Acceptance Criteria

1. THE Dashboard SHALL use an aligned 24-column grid layout with stat panels and timeseries panels arranged consistently within each row.
2. THE Dashboard timeseries panels SHALL use smooth line interpolation (`lineInterpolation: "smooth"`), `showPoints: "never"`, and `pointSize: 0`.
3. THE Dashboard timeseries panels SHALL use bottom-placed table legends with `lastNotNull`, `mean`, and `max` calculations.
4. THE Dashboard rate panels SHALL use `rate()` PromQL functions and explicit rate units, for example `ops` for operations per second.
5. THE Dashboard panels SHALL include `description` annotations explaining what the signal means and how operators should interpret the values.
