# Implementation Plan: DSQL Spread Keys

## Overview

Introduce hash-derived UUIDv8 primary keys to eliminate hot-key concentration in Tokeira's DSQL tables. Implementation proceeds bottom-up: utility function → RunKey derivation → schema DDL → repository SQL → trait signature → production call sites → tests.

## Tasks

- [x] 1. Add `dsql_spread_uuid` utility and `RunKey::derive`
  - [x] 1.1 Add `blake3 = "1"` dependency to `tokeira-types/Cargo.toml`
    - Add `blake3 = "1"` under `[dependencies]`
    - _Requirements: 2.1, 2.2_

  - [x] 1.2 Create `tokeira-types/src/spread.rs` with `dsql_spread_uuid`
    - Implement `pub fn dsql_spread_uuid(parts: &[&[u8]]) -> Uuid`
    - Domain separation tag: `"tokeira/dsql-key/v1\0"`
    - Length-prefix each part as big-endian `u64` before part data
    - Set UUIDv8 version bits `[48..51]` = `0b1000` and RFC 9562 variant bits `[64..65]` = `0b10`
    - Register module in `tokeira-types/src/lib.rs`: `pub mod spread;` and `pub use spread::*;`
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.8_

  - [x] 1.3 Add `RunKey::derive` constructor and restrict `RunKey::new()` to test builds
    - Add `RunKey::derive(namespace_id: NamespaceId, workflow_id: &WorkflowId, run_id: RunId) -> Self` calling `dsql_spread_uuid(&[b"run", namespace_id.0.as_bytes(), workflow_id.0.as_bytes(), run_id.0.as_bytes()])`
    - Move `RunKey::new()` behind `#[cfg(any(test, feature = "test-support"))]` — `cfg(test)` alone only works within `tokeira-types`; downstream crate tests need the `test-support` feature
    - Move `RunKey::Default` impl behind `#[cfg(any(test, feature = "test-support"))]`
    - Add `test-support = []` feature to `tokeira-types/Cargo.toml`
    - Add `tokeira-types = { path = "../tokeira-types", features = ["test-support"] }` to `[dev-dependencies]` of downstream crates that use `RunKey::new()` in tests
    - _Requirements: 8.1, 8.2, 8.3_

  - [x] 1.4 Write unit tests for `dsql_spread_uuid` and `RunKey::derive`
    - Known test vectors: verify output against pre-computed BLAKE3 hashes
    - Empty parts: `dsql_spread_uuid(&[])` and `dsql_spread_uuid(&[b""])` produce valid but different UUIDs
    - `RunKey::derive` with same inputs returns same key
    - _Requirements: 1.2, 8.2_

  - [x] 1.5 Write property test: P1 Determinism
    - **Property 1: Determinism** — for any ordered sequence of byte-slice parts, calling `dsql_spread_uuid` twice with the same input produces identical output
    - **Validates: Requirements 1.2, 8.2**

  - [x] 1.6 Write property test: P2 Length-Prefix Collision Resistance
    - **Property 2: Length-Prefix Collision Resistance** — for any byte sequence split at different boundaries, `dsql_spread_uuid` produces different UUIDs
    - **Validates: Requirements 1.5**

  - [x] 1.7 Write property test: P3 UUIDv8 Format Invariant
    - **Property 3: UUIDv8 Format Invariant** — for any input, the output UUID has version 8 and RFC 9562 variant bits
    - **Validates: Requirements 1.6**

  - [x] 1.8 Write property test: P4 Avalanche Behavior
    - **Property 4: Avalanche Behavior** — flipping a single bit in any input part changes approximately half the output bits (Hamming distance 30–98 out of 122 variable bits)
    - **Validates: Requirements 1.7**

- [x] 2. Checkpoint — Verify `tokeira-types` compiles and tests pass
  - `cargo test -p tokeira-types` passed.

- [x] 3. Update schema DDLs and remove obsolete migrations
  - [x] 3.1 Revise `V003__current_execution.sql` with spread UUID PK
    - Add `key UUID NOT NULL` as sole `PRIMARY KEY (key)`
    - Change `run_id` column type from `TEXT` to `UUID NOT NULL`
    - Retain `namespace_id`, `workflow_id`, `run_key`, `run_id`, `is_open`, `created_at`
    - Do NOT include the index in this file — DSQL requires one DDL per migration
    - _Requirements: 4.1, 4.2, 4.6_

  - [x] 3.2 Revise `V006__request_dedupe.sql` with spread UUID PK
    - Add `key UUID NOT NULL` as sole `PRIMARY KEY (key)`
    - Add `run_id UUID NOT NULL` column (was nullable TEXT via V019 ALTER, now part of initial DDL)
    - Retain `namespace_id`, `workflow_id`, `request_id`, `run_key`, `first_seen_transition_seq`, `created_at`
    - Do NOT include the index in this file — DSQL requires one DDL per migration
    - _Requirements: 5.1, 5.2, 5.6_

  - [x] 3.3 Revise `V009__dispatch_backlog.sql` with spread UUID PK
    - Add `key UUID NOT NULL` as sole `PRIMARY KEY (key)`
    - Add `task_kind SMALLINT NOT NULL`, `deployment TEXT`, `build_id TEXT` columns to store the full `QueueKey` identity — required for `drain_backlog` to distinguish workflow vs activity queues and versioned variants
    - Retain `partition_id`, `queue_namespace`, `queue_name`, `insertion_seq`, `run_key`, `payload_data`, `scheduled_at`
    - _Requirements: 6.1, 6.2, 6.4_

  - [x] 3.4 Repurpose `V019__request_dedupe_add_run_id.sql` → `V019__idx_current_execution_ns_wf.sql`
    - Replace content with: `CREATE UNIQUE INDEX ASYNC idx_current_execution_ns_wf ON current_execution (namespace_id, workflow_id);`
    - The original V019 (ALTER TABLE ADD COLUMN run_id) is obsolete because run_id is now in the initial V006 DDL
    - _Requirements: 4.3_

  - [x] 3.5 Repurpose `V020__idx_workflow_hot_ns_wf.sql` → `V020__idx_request_dedupe_ns_wf_req.sql`
    - Replace content with: `CREATE UNIQUE INDEX ASYNC idx_request_dedupe_ns_wf_req ON request_dedupe (namespace_id, workflow_id, request_id);`
    - The original V020 (workflow_hot ns_wf index) is obsolete because resolve_execution with explicit run_id now uses RunKey::derive + PK lookup
    - _Requirements: 5.3_

  - [x] 3.6 Create `V023__idx_dispatch_backlog_queue_seq.sql`
    - Content: `CREATE INDEX ASYNC idx_dispatch_backlog_queue_seq ON dispatch_backlog (queue_namespace, queue_name, task_kind, deployment, build_id, insertion_seq);`
    - Required for `drain_backlog(queue, limit)` which needs an indexed path to find entries for a specific queue (including versioned variants) in FIFO order
    - The drain predicate uses `IS NOT DISTINCT FROM` for null-safe matching on `deployment` and `build_id`
    - _Requirements: 6.1, 6.5_

  - [x] 3.7 Update DDL validator to handle `CREATE UNIQUE INDEX` form
    - The current validator checks `normalized.contains("create index") && !normalized.contains("create index async")`, which misses `CREATE UNIQUE INDEX` forms entirely
    - Fix the check so both `CREATE INDEX` and `CREATE UNIQUE INDEX` require the `ASYNC` keyword
    - Add a test case for `CREATE UNIQUE INDEX ASYNC idx ON t (a);` — should pass validation
    - Add a test case for `CREATE UNIQUE INDEX idx ON t (a);` — should fail with `MissingAsyncKeyword`
    - _Requirements: 4.3, 5.3_

- [x] 4. Replace `shard_id_to_uuid` and update `DsqlRunRepository` SQL
  - [x] 4.1 Replace `shard_id_to_uuid` with `dsql_spread_uuid` call
    - Change `DsqlRunRepository::shard_id_to_uuid` to call `dsql_spread_uuid(&[b"shard", &shard_id.0.to_le_bytes()])`
    - Remove `use sha2::{Digest, Sha256}` import
    - _Requirements: 3.1, 3.2_

  - [x] 4.2 Remove `sha2` import from `run_repository.rs`
    - Remove `use sha2::{Digest, Sha256}` from `run_repository.rs` (the shard helper no longer uses it)
    - Do NOT remove `sha2` from `tokeira-storage/Cargo.toml` — `migration.rs` still uses it for migration file checksums
    - _Requirements: 3.1_

  - [x] 4.3 Add spread UUID table key helper functions
    - Add `current_execution_key(namespace_id, workflow_id) -> Uuid`
    - Add `request_dedupe_key(namespace_id, workflow_id, request_id) -> Uuid`
    - Add `dispatch_backlog_key(partition_id, queue_namespace, queue_name, task_kind, deployment, build_id, insertion_seq) -> Uuid` — includes full `QueueKey` identity; uses explicit option tag (0x00/0x01) for nullable `deployment`/`build_id` to prevent None/empty-string collision
    - Each calls `dsql_spread_uuid` with the appropriate domain tag and parts
    - _Requirements: 4.4, 5.4, 6.3_

  - [x] 4.3a Add stable `TaskKind` numeric mapping
    - Add `TaskKind::to_db_smallint() -> i16` returning `Workflow = 0`, `Activity = 1`
    - Add `TryFrom<i16> for TaskKind` for reading from DSQL
    - Add public `TaskKindDecodeError` for unknown database values
    - This mapping is durable data — changing it would break existing spread UUIDs and rows
    - Add unit tests verifying the mapping is stable, known values round-trip, and unknown values return `TaskKindDecodeError`
    - _Requirements: 6.2, 6.3_

  - [x] 4.4 Update all `current_execution` SQL in `run_repository.rs`
    - INSERT/UPSERT: compute spread UUID via `current_execution_key`, bind as `$1` (`key` column)
    - INSERT/UPSERT: bind `run_id` as `Uuid` directly (`state.run_id.0`) — the column is now `UUID NOT NULL`, not `TEXT`. Remove the `.to_string()` conversion from the current `upsert_current_execution_start`
    - SELECT (resolve_execution no-run_id path): compute spread UUID, query by `key = $1 AND is_open = true`
    - SELECT (find_latest_run): compute spread UUID, query by `key = $1`
    - UPDATE (close): update by `key = $1 AND run_key = $2` — the `run_key` guard prevents a stale run from closing a successor's row after continue-as-new/reset
    - SELECT (conflict policy check in commit_transition): query by `key = $1`
    - _Requirements: 4.4, 4.5_

  - [x] 4.5 Update all `request_dedupe` SQL in `run_repository.rs`
    - INSERT: compute spread UUID via `request_dedupe_key`, bind as `$1` (`key` column)
    - INSERT: bind `run_id` as `Uuid` directly (`state.run_id.0`) — the column is now `UUID NOT NULL`, not nullable `TEXT`. Remove the `.to_string()` conversion
    - SELECT (dedupe check): query by `key = $1`
    - SELECT (lookup_request_dedupe): query by `key = $1`; read `run_id` as `Uuid` (not `Option<String>`) — simplify the current nullable-text-era parsing to a direct `Uuid` read
    - _Requirements: 5.4, 5.5_

  - [x] 4.6 Update all `dispatch_backlog` SQL in `run_repository.rs`
    - INSERT: compute spread UUID via `dispatch_backlog_key` with full `QueueKey` fields, bind as `$1` (`key` column)
    - INSERT: bind `task_kind` as `i16` using the stable numeric mapping (`Workflow = 0`, `Activity = 1`), `deployment` and `build_id` as `Option<&str>`
    - _Requirements: 6.3_

  - [x] 4.7 Optimize `resolve_execution` with explicit `run_id`
    - When `execution.run_id` is `Some`, compute `RunKey::derive(namespace_id, workflow_id, run_id)` and verify existence via `SELECT 1 FROM workflow_hot WHERE run_key = $1`
    - Remove the O(N) scan + deserialization path for the explicit `run_id` case
    - _Requirements: 9.1, 9.2, 9.3_

  - [x] 4.8 Write unit tests for table key helpers and shard UUID migration
    - Verify `current_execution_key`, `request_dedupe_key`, `dispatch_backlog_key` produce deterministic output for known inputs
    - Verify `shard_id_to_uuid` with BLAKE3 produces different output than old SHA-256 for same `ShardId`
    - _Requirements: 3.2, 4.4, 5.4, 6.3_

- [x] 5. Checkpoint — Verify `tokeira-storage` compiles with `dsql` feature
  - `cargo check -p tokeira-storage --features dsql` passed.
  - `cargo test -p tokeira-storage --features dsql` passed.

- [x] 6. Update `materialize_reset_successor` trait signature
  - [x] 6.1 Change `RunRepository` trait signature in `api.rs`
    - Remove `successor_run_key: RunKey` parameter from `materialize_reset_successor`
    - New signature: `async fn materialize_reset_successor(&self, base_run_key: RunKey, fork_event_id: i64, successor_run_id: RunId) -> Result<()>`
    - _Requirements: 11.2_

  - [x] 6.2 Update `Arc<T>` blanket impl in `api.rs`
    - Match the revised trait signature in the `impl<T> RunRepository for std::sync::Arc<T>` block
    - _Requirements: 11.2_

  - [x] 6.3 Update `DsqlRunRepository::materialize_reset_successor`
    - Remove `successor_run_key` parameter
    - Load base run state to get `(namespace_id, workflow_id)`
    - Derive successor key: `RunKey::derive(base_state.namespace_id, &base_state.workflow_id, successor_run_id)`
    - _Requirements: 11.1, 11.2_

  - [x] 6.4 Update `InMemoryStore::materialize_reset_successor`
    - Remove `successor_run_key` parameter
    - Derive successor key internally from base run's `(namespace_id, workflow_id)` and `successor_run_id`
    - _Requirements: 12.1, 12.2_

  - [x] 6.5 Update `HistoryNotifyingRepository` wrapper in `tokeira-edge/src/history_wait.rs`
    - Match the revised trait signature
    - _Requirements: 11.2_

  - [x] 6.6 Update all mock `RunRepository` implementations
    - Update mock in `tokeira-runtime/src/lane.rs` (test mock)
    - Update mock in `tokeira-runtime/src/backlog.rs` (test mock)
    - Update mock in `tokeira-runtime/src/runtime.rs` (test mock)
    - Match the revised trait signature in each
    - _Requirements: 11.3_

- [x] 7. Update production `RunKey::new()` call sites to `RunKey::derive`
  - [x] 7.1 Update `tokeira-edge/src/translate/to_internal.rs`
    - `start_request`: change `run_key: req.run_key.unwrap_or_default()` to derive from `(namespace_id, workflow_id, run_id)` using `RunKey::derive`
    - `signal_with_start_request`: change `run_key: RunKey::new()` to `RunKey::derive(namespace_id, &workflow_id, run_id)`
    - _Requirements: 8.4, 10.1_

  - [x] 7.2 Update `tokeira-edge/src/workflow_service.rs` (schedule-triggered start)
    - Change `let run_key = RunKey::new()` to `let run_key = RunKey::derive(namespace_id, &workflow_id, run_id)` in the schedule start path (~line 1327)
    - _Requirements: 8.4, 10.1_

  - [x] 7.3 Update `tokeira-runtime/src/lane.rs` (continue-as-new)
    - Change `let successor_run_key = RunKey::new()` to `RunKey::derive(new_state.namespace_id, &new_state.workflow_id, successor_run_id)` (~line 468)
    - _Requirements: 8.4, 10.1_

  - [x] 7.4 Update `tokeira-runtime/src/lane.rs` (reset)
    - Change `let successor_run_key = RunKey(successor_run_id.0)` to remove the pre-computed key
    - Update the `materialize_reset_successor` call to pass only `(base_run_key, fork_event_id, successor_run_id)` — the repository derives the key internally
    - Update the subsequent `load_run` call to use `RunKey::derive(...)` for the successor
    - _Requirements: 8.4, 11.3_

  - [x] 7.5 Update `tokeira-runtime/src/publisher.rs` (child workflow start)
    - Change `let child_run_key = RunKey::new()` to `RunKey::derive(namespace_id, &child_workflow_id, child_run_id)` (~line 208)
    - _Requirements: 8.4, 10.1_

  - [x] 7.6 Update `tokeira-runtime/src/schedule.rs` (scheduled start)
    - Change `let run_key = RunKey::new()` to `RunKey::derive(namespace_id, &workflow_id, run_id)` (~line 793)
    - _Requirements: 8.4, 10.1_

- [x] 8. Checkpoint — Full workspace compilation
  - `cargo check --workspace` passed.
  - `cargo lint` passed.

- [x] 9. Property tests for RunKey derivation
  - [x] 9.1 Write property test: P5 RunKey Derive Round-Trip
    - **Property 5: RunKey Derive Round-Trip** — for any `(namespace_id, workflow_id, run_id)` triple, `RunKey::derive` called twice with the same inputs produces the same `RunKey`
    - Place in `tokeira-types/src/ids.rs` or `tokeira-storage/src/memory.rs`
    - **Validates: Requirements 8.1, 9.1**

  - [x] 9.2 Write property test: P6 Reset Successor Key Consistency
    - **Property 6: Reset Successor Key Consistency** — for any base run `(namespace_id, workflow_id)` and `successor_run_id`, the `RunKey` produced by `materialize_reset_successor` equals `RunKey::derive(namespace_id, workflow_id, successor_run_id)`
    - Place in `tokeira-storage/src/memory.rs`
    - **Validates: Requirements 11.1, 11.2**

- [ ] 10. Final checkpoint — Full test suite
  - `cargo test --workspace` was run.
  - Blocked by unrelated `tokeira-state` AWS native-root certificate tests: `TrustStore configured to enable native roots but no valid root certificates parsed!`
  - Re-run after the local AWS native-root test environment is fixed.

## Notes

- All unit and property-based tests are required
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties from the design document
- `timer_bucket` is intentionally excluded from spread-key revision (Requirement 7)
- `RunKey::new()` is available via the `test-support` feature on `tokeira-types` — downstream crate tests enable it in `[dev-dependencies]`
- All DDL changes are in-place updates (schema version 1, no production data)
