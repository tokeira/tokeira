# Requirements Document: DSQL Spread Keys

## Introduction

This document captures the requirements for introducing hash-derived UUIDv8 primary keys to eliminate hot-key concentration in Tokeira's DSQL tables. DSQL distributes data by hashing the leading primary key column. When that column has low cardinality or is namespace-prefixed, all writes for a single tenant cluster on the same storage partition, creating a hot spot that limits throughput.

The solution is a general-purpose utility function, `dsql_spread_uuid`, that deterministically derives a uniformly-distributed UUIDv8 from arbitrary logical inputs using BLAKE3. Tables whose composite primary keys suffer from leading-column clustering are revised to use a spread UUID as the sole primary key, with the original logical key preserved as a unique index for lookups. Additionally, `RunKey` is changed from a random UUID v4 to a deterministic derivation from `(namespace_id, workflow_id, run_id)`, eliminating a class of lookup-table queries.

This spec depends on Feature 1 (`dsql-schema-connection`) for the current schema DDL and Feature 2 (`dsql-core-persistence`) for `DsqlRunRepository` and `RunKey` usage. Tokeira is pre-production — all development targets schema version 1. There are no migrations; the existing DDL files in `migrations/` are updated in-place to reflect the revised table definitions.

## Glossary

- **Spread_UUID**: A deterministic UUIDv8 value derived by hashing logical key parts with BLAKE3, designed to distribute uniformly across the UUID keyspace and avoid hot-key concentration in DSQL.
- **BLAKE3**: A cryptographic hash function chosen for speed on ARM/Graviton and excellent avalanche properties. Used as the hash primitive inside `dsql_spread_uuid`.
- **UUIDv8**: An RFC 9562 application-defined UUID variant. Bits `[48..51]` are set to `0b1000` (version 8) and bits `[64..65]` are set to `0b10` (RFC variant). The remaining 122 bits are application-controlled.
- **Domain_Separation**: A fixed prefix (`"tokeira/dsql-key/v1\0"`) prepended to the hash input so that `dsql_spread_uuid` outputs cannot collide with other BLAKE3 uses in the codebase.
- **Length_Prefixing**: Encoding each input part's byte length as a big-endian `u64` before the part data, so that `["ab", "c"]` and `["a", "bc"]` produce different hashes.
- **Hot_Key_Concentration**: A DSQL performance anti-pattern where a low-cardinality or monotonic leading PK column causes all writes to land on the same storage partition.
- **RunKey**: Internal durable row key for a workflow run, currently generated as random UUID v4. This spec changes it to a deterministic derivation from `(namespace_id, workflow_id, run_id)`.
- **RunId**: User-visible workflow run identifier (UUID v4). Remains random and unchanged by this spec.
- **DsqlRunRepository**: The production `RunRepository` implementation in `tokeira-storage/src/dsql/run_repository.rs` that executes fenced DSQL transactions.
- **Current_Execution**: Table mapping `(namespace_id, workflow_id)` to the current run identity and open/closed status.
- **Request_Dedupe**: Idempotency record table for external command deduplication.
- **Dispatch_Backlog**: Durable fallback table for unmatched tasks when no worker is immediately available.
- **Shard_ID_To_UUID**: The existing `DsqlRunRepository::shard_id_to_uuid` method that hashes `ShardId(u32)` into a UUID using SHA-256. Replaced by `dsql_spread_uuid` in this spec.

## Requirements

---

## Item 1: `dsql_spread_uuid` Utility Function

### Requirement 1: Spread UUID Function

**User Story:** As a Tokeira developer, I want a general-purpose utility that produces deterministic, uniformly-distributed UUIDv8 values from arbitrary logical inputs, so that DSQL primary keys derived from application data are spread across the UUID keyspace without hot-key concentration.

#### Acceptance Criteria

1. THE Spread_UUID function SHALL accept an ordered slice of byte-slice parts and return a single `Uuid` value.
2. THE Spread_UUID function SHALL produce identical output for identical input (determinism).
3. THE Spread_UUID function SHALL use BLAKE3 as the hash primitive.
4. THE Spread_UUID function SHALL prepend the domain separation tag `"tokeira/dsql-key/v1\0"` to the hash input before any part data.
5. THE Spread_UUID function SHALL length-prefix each part by writing the part's byte length as a big-endian `u64` before the part data, so that `[b"ab", b"c"]` and `[b"a", b"bc"]` produce different UUIDs.
6. THE Spread_UUID function SHALL set UUIDv8 version bits (bits `[48..51]` = `0b1000`) and RFC 9562 variant bits (bits `[64..65]` = `0b10`) on the output, consuming 6 bits of entropy.
7. THE Spread_UUID function SHALL exhibit avalanche behavior: a single-bit change in any input part SHALL change approximately half the output bits.
8. THE Spread_UUID function SHALL live in the `tokeira-types` crate, not behind a feature gate, because it is used by the kernel and runtime layers in addition to storage.

### Requirement 2: BLAKE3 Dependency

**User Story:** As a Tokeira developer, I want BLAKE3 added as a dependency to `tokeira-types`, so that the spread UUID function has access to a fast, high-quality hash primitive.

#### Acceptance Criteria

1. THE `tokeira-types` crate SHALL add `blake3 = "1"` as a dependency.
2. THE BLAKE3 dependency SHALL NOT be feature-gated because `dsql_spread_uuid` is used unconditionally by `RunKey::derive`.

---

## Item 2: Replace `shard_id_to_uuid`

### Requirement 3: Shard UUID Migration to Spread UUID

**User Story:** As a Tokeira developer, I want `DsqlRunRepository::shard_id_to_uuid` replaced with `dsql_spread_uuid`, so that shard UUID derivation uses the same hash utility as all other spread keys and benefits from BLAKE3's speed on Graviton.

#### Acceptance Criteria

1. WHEN a `ShardId` is converted to a UUID for SQL binding, THE DsqlRunRepository SHALL call `dsql_spread_uuid(&[b"shard", &shard_id.0.to_le_bytes()])` instead of the current SHA-256 based `shard_id_to_uuid`.
2. THE replacement SHALL produce different UUID values than the current SHA-256 implementation for the same `ShardId` input.
3. THE `sha2` import in `run_repository.rs` SHALL be removed since the shard helper no longer uses it. The `sha2` dependency itself SHALL be retained in `tokeira-storage` because `migration.rs` uses it for migration file checksums.

---

## Item 3: Schema Revision for Hot-Key Tables

### Requirement 4: Current Execution Table Revision

**User Story:** As a Tokeira developer, I want the `current_execution` table revised with a spread UUID primary key, so that single-tenant deployments do not concentrate all writes on the same DSQL partition.

#### Acceptance Criteria

1. THE revised `current_execution` table SHALL have a single-column primary key `(key)` where `key` is a UUID derived from `dsql_spread_uuid(&[b"current-execution", namespace_id_bytes, workflow_id_bytes])`.
2. THE revised `current_execution` table SHALL retain `namespace_id`, `workflow_id`, `run_key`, `run_id`, `is_open`, and `created_at` columns.
3. THE revised `current_execution` table SHALL have a unique async index on `(namespace_id, workflow_id)` for logical key lookups.
4. WHEN inserting or upserting a `current_execution` row, THE DsqlRunRepository SHALL compute the spread UUID from the logical key and use it as the `key` column value.
5. WHEN querying `current_execution` by `(namespace_id, workflow_id)`, THE DsqlRunRepository SHALL compute the spread UUID from the logical key and use it as the primary key for lookups, avoiding index-only paths where possible.
6. THE existing `V003__current_execution.sql` migration file SHALL be updated in-place with the revised table definition. No separate migration is needed because Tokeira targets schema version 1.

### Requirement 5: Request Dedupe Table Revision

**User Story:** As a Tokeira developer, I want the `request_dedupe` table revised with a spread UUID primary key, so that single-tenant deployments do not concentrate all idempotency writes on the same DSQL partition.

#### Acceptance Criteria

1. THE revised `request_dedupe` table SHALL have a single-column primary key `(key)` where `key` is a UUID derived from `dsql_spread_uuid(&[b"request-dedupe", namespace_id_bytes, workflow_id_bytes, request_id_bytes])`.
2. THE revised `request_dedupe` table SHALL retain `namespace_id`, `workflow_id`, `request_id`, `run_key`, `run_id`, `first_seen_transition_seq`, and `created_at` columns.
3. THE revised `request_dedupe` table SHALL have a unique async index on `(namespace_id, workflow_id, request_id)` for logical key lookups.
4. WHEN inserting a `request_dedupe` row, THE DsqlRunRepository SHALL compute the spread UUID from the logical key and use it as the `key` column value.
5. WHEN querying `request_dedupe` by `(namespace_id, workflow_id, request_id)`, THE DsqlRunRepository SHALL compute the spread UUID from the logical key and use it as the primary key for lookups.
6. THE existing `V006__request_dedupe.sql` migration file SHALL be updated in-place with the revised table definition. No separate migration is needed because Tokeira targets schema version 1.

### Requirement 6: Dispatch Backlog Table Revision

**User Story:** As a Tokeira developer, I want the `dispatch_backlog` table revised with a spread UUID primary key, so that low-cardinality `partition_id` leading columns do not concentrate writes on a few DSQL partitions.

#### Acceptance Criteria

1. THE revised `dispatch_backlog` table SHALL have a single-column primary key `(key)` where `key` is a UUID derived from `dsql_spread_uuid` using the full logical key including all queue identity fields.
2. THE revised `dispatch_backlog` table SHALL retain `partition_id`, `queue_namespace`, `queue_name`, `insertion_seq`, `run_key`, `payload_data`, and `scheduled_at` columns, and SHALL add `task_kind` (SMALLINT NOT NULL), `deployment` (TEXT, nullable), and `build_id` (TEXT, nullable) columns to store the full `QueueKey` identity.
3. WHEN inserting a `dispatch_backlog` row, THE DsqlRunRepository SHALL compute the spread UUID from the full logical key (including `task_kind`, `deployment`, `build_id`) and use it as the `key` column value.
4. THE existing `V009__dispatch_backlog.sql` migration file SHALL be updated in-place with the revised table definition. No separate migration is needed because Tokeira targets schema version 1.
5. THE revised `dispatch_backlog` table SHALL have a secondary async index on `(queue_namespace, queue_name, task_kind, deployment, build_id, insertion_seq)` to support `drain_backlog(queue, limit)` with FIFO ordering and versioned queue discrimination without a full table scan. The drain predicate SHALL use `IS NOT DISTINCT FROM` for null-safe matching on nullable `deployment` and `build_id` columns.

### Requirement 7: Timer Bucket Table Exclusion

**User Story:** As a Tokeira developer, I want the `timer_bucket` table intentionally excluded from spread-key revision, so that shard-filtered time-range sweep queries remain efficient.

#### Acceptance Criteria

1. THE `timer_bucket` table SHALL retain its existing primary key `(shard_id, fire_at, run_key, timer_id)` without a spread UUID leading column.
2. THE `timer_bucket` table's `shard_id` column SHALL continue to use the shard UUID derived from `dsql_spread_uuid` (via the shard-to-UUID conversion), which already provides adequate distribution because shard IDs span the full UUID keyspace after hashing.

---

## Item 4: RunKey as Derived Key

### Requirement 8: Deterministic RunKey Derivation

**User Story:** As a Tokeira developer, I want `RunKey` derived deterministically from `(namespace_id, workflow_id, run_id)` using `dsql_spread_uuid`, so that the runtime can compute a run's storage key without a lookup table query.

#### Acceptance Criteria

1. THE `RunKey` type SHALL provide a `derive(namespace_id: NamespaceId, workflow_id: &WorkflowId, run_id: RunId)` constructor that returns `RunKey(dsql_spread_uuid(&[b"run", namespace_id.0.as_bytes(), workflow_id.0.as_bytes(), run_id.0.as_bytes()]))`.
2. THE `RunKey::derive` constructor SHALL produce identical output for identical `(namespace_id, workflow_id, run_id)` input (determinism).
3. THE `RunKey::new()` constructor (random UUID v4) SHALL be removed from production code. It SHALL be retained behind `#[cfg(any(test, feature = "test-support"))]` so that downstream crate tests can use it via the `test-support` feature on `tokeira-types`.
4. ALL production call sites that create a `RunKey` SHALL use `RunKey::derive` with the logical identity triple `(namespace_id, workflow_id, run_id)`.

### Requirement 9: Resolve Execution Optimization

**User Story:** As a Tokeira developer, I want `resolve_execution` with an explicit `run_id` to compute the `RunKey` directly instead of scanning `workflow_hot`, so that the explicit-run-id path avoids deserialization overhead.

#### Acceptance Criteria

1. WHEN `resolve_execution` is called with an `ExecutionRef` that has a specific `run_id`, THE DsqlRunRepository SHALL compute `RunKey::derive(namespace_id, workflow_id, run_id)` and verify the run exists by querying `workflow_hot` with the derived `run_key` as a single-row primary key lookup.
2. THE optimized path SHALL NOT scan `workflow_hot` by `(namespace_id, workflow_id)` and deserialize `WorkflowState` rows to match `run_id`.
3. WHEN `resolve_execution` is called without a `run_id`, THE DsqlRunRepository SHALL continue to use the `current_execution` table to find the current open run (no change to this path).

### Requirement 10: RunKey Derivation Across Layers

**User Story:** As a Tokeira developer, I want all layers (kernel, runtime, edge, storage) to pass `(namespace_id, workflow_id, run_id)` when creating a `RunKey`, so that the deterministic derivation is used consistently throughout the system.

#### Acceptance Criteria

1. WHEN the runtime creates a new workflow run, THE Runtime SHALL generate a fresh `RunId` (UUID v4) and derive `RunKey` from `(namespace_id, workflow_id, run_id)` using `RunKey::derive`.
2. WHEN the kernel or runtime reconstructs a `RunKey` from persisted data, THE reconstruction SHALL use `RunKey::derive` with the stored `(namespace_id, workflow_id, run_id)` triple.
3. THE `workflow_hot`, `history_batch`, `activity_state`, and `timer_bucket` tables SHALL continue to use `run_key` as a leading PK column, which is now hash-derived UUIDv8 instead of random UUID v4, preserving the same uniform distribution property.
4. THE `current_execution` table SHALL remain necessary for the no-`run_id` path of `resolve_execution` and for `find_latest_run`, because those operations cannot derive a `RunKey` without knowing the `run_id`.

### Requirement 11: Materialize Reset Successor with Derived RunKey

**User Story:** As a Tokeira developer, I want `materialize_reset_successor` to use `RunKey::derive` for the successor run, so that reset forks use deterministic keys consistent with the rest of the system.

#### Acceptance Criteria

1. WHEN `materialize_reset_successor` is called, THE DsqlRunRepository SHALL derive the successor's `RunKey` from `(namespace_id, workflow_id, successor_run_id)` using `RunKey::derive`.
2. THE `RunRepository` trait signature for `materialize_reset_successor` SHALL change to remove the `successor_run_key: RunKey` parameter, accepting only `(base_run_key, fork_event_id, successor_run_id)`. The repository derives the key internally from the base run's `(namespace_id, workflow_id)` and the provided `successor_run_id`.
3. ALL callers of `materialize_reset_successor` (runtime, tests) SHALL be updated to pass `successor_run_id` instead of a pre-computed `successor_run_key`.

### Requirement 12: In-Memory Store Compatibility

**User Story:** As a Tokeira developer, I want the in-memory store to remain the behavioral reference after the RunKey and trait changes, so that property tests and semantic tests continue to validate storage correctness.

#### Acceptance Criteria

1. THE `InMemoryStore` implementation of `materialize_reset_successor` SHALL be updated to match the revised trait signature (no `successor_run_key` parameter) and SHALL derive the successor `RunKey` internally from the base run's `(namespace_id, workflow_id)` and the provided `successor_run_id` using `RunKey::derive`.
2. THE `InMemoryStore` implementation SHALL continue to use `RunKey` as the primary map key for `runs`, `history`, `execution_index`, and all other internal data structures. The change from random UUID v4 to hash-derived UUIDv8 is transparent to the in-memory store because it only compares and stores `RunKey` values — it does not depend on their internal structure.
3. ALL `InMemoryStore` test fixtures that call `RunKey::new()` SHALL be updated to use `RunKey::derive(namespace_id, workflow_id, run_id)` or a local fixture helper. `RunKey::new()` is available via the `test-support` feature on `tokeira-types` for test convenience where the logical identity triple is not meaningful.
