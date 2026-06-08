# Implementation Plan: DSQL Core Persistence — DsqlRunRepository

## Overview

Implement `DsqlRunRepository` — the production `RunRepository` trait implementation against Aurora DSQL. This builds on Feature 1's `DsqlConnectionDirector`, `DsqlPermit`, and codec module. The implementation follows a bottom-up approach: schema migration first, then the struct and helpers, then the commit transaction (primary write path), then read operations, then reset materialization, then wiring into `DsqlStore`, and finally tests.

All code is gated behind `#[cfg(feature = "dsql")]`. Integration tests requiring a live DSQL cluster are gated behind `dsql-integration`.

## Tasks

- [x] 1. Schema migration and module scaffolding
  - [x] 1.1 Create schema migrations
    - Updated `V003__current_execution.sql` to use a spread UUID `key` primary key, retain `namespace_id`/`workflow_id` as logical columns, and store `run_id` as `UUID NOT NULL`
    - Updated `V006__request_dedupe.sql` to use a spread UUID `key` primary key and store `run_id` as `UUID NOT NULL`
    - Add file `tokeira-storage/migrations/V019__idx_current_execution_ns_wf.sql`
    - Content: `CREATE UNIQUE INDEX ASYNC idx_current_execution_ns_wf ON current_execution (namespace_id, workflow_id);`
    - Add file `tokeira-storage/migrations/V020__idx_request_dedupe_ns_wf_req.sql`
    - Content: `CREATE UNIQUE INDEX ASYNC idx_request_dedupe_ns_wf_req ON request_dedupe (namespace_id, workflow_id, request_id);`
    - Add file `tokeira-storage/migrations/V021__idx_activity_state_run_activity.sql`
    - Content: `CREATE INDEX ASYNC idx_activity_state_run_activity ON activity_state (run_key, activity_id);`
    - Required for `ActivityOp::Delete` which uses non-PK WHERE `(run_key, activity_id)` — without this index, deletes degrade to table scans as activity state grows
    - Add file `tokeira-storage/migrations/V022__idx_timer_bucket_run_timer.sql`
    - Content: `CREATE INDEX ASYNC idx_timer_bucket_run_timer ON timer_bucket (run_key, timer_id);`
    - Required for `TimerOp::Delete` which uses non-PK WHERE `(run_key, timer_id)` — without this index, deletes degrade to table scans as timer state grows
    - _Requirements: 3.4, 3.6, 7.2, 10.1, 10.2_

  - [x] 1.2 Create `dsql/run_repository.rs` module with `DsqlRunRepository` struct
    - Create `tokeira-storage/src/dsql/run_repository.rs`
    - Define `DsqlRunRepository` struct with `director: Arc<dyn DsqlConnectionAcquirer>`, `shard_count: u32`, `conflict_policy: CurrentExecutionConflictPolicy`
    - Implement `DsqlConnectionAcquirer` for `DsqlConnectionDirector` so production uses the real director and unit tests can inject a mock acquisition boundary
    - Implement `DsqlRunRepository::new()` constructor; reject `shard_count == 0` with an error rather than silently treating it as 1 — this prevents configuration errors from diverging with runtime shard ownership
    - Implement `fn shard_for_run_key(&self, run_key: RunKey) -> ShardId` using `(run_key.0.as_u128() as u32) % self.shard_count` — no `.max(1)` guard needed because the constructor rejects zero
    - Implement `fn shard_id_to_uuid(shard_id: ShardId) -> uuid::Uuid` using `dsql_spread_uuid(&[b"shard", &shard_id.0.to_le_bytes()])` — the schema stores `shard_id` as UUID but `ShardId` is `u32`; this provides a stable, deterministic encoding used in all SQL bindings for `shard_lease`, `workflow_hot`, `activity_state`, and `timer_bucket`
    - Add spread-key helpers for `current_execution`, `request_dedupe`, and `dispatch_backlog`
    - Implement `fn is_serialization_failure(err: &sqlx::Error) -> bool` checking for SQLSTATE 40001
    - Register `pub mod run_repository;` in `dsql/mod.rs`
    - _Requirements: 1.1, 1.2, 1.5, 12.1, 13.5_

- [x] 2. Implement `commit_transition` — the fenced commit transaction
  - [x] 2.1 Implement shard epoch fence and transition sequence fence
    - Acquire `DbClass::Commit` permit via `self.director.acquire(DbClass::Commit).await?`
    - Begin transaction on the permit's connection: `permit.connection()?.begin().await?`
    - Step 1: When `epoch != ShardEpoch::ZERO`, query `SELECT epoch FROM shard_lease WHERE shard_id = $1` within the transaction; return `CommitResult::Conflict` on mismatch or missing row
    - Step 2: Query `SELECT transition_seq FROM workflow_hot WHERE run_key = $1 FOR UPDATE`; for new runs (`expected_seq == TransitionSeq::ZERO`) absence is expected; for existing runs, mismatch returns `CommitResult::Conflict`
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.6, 2.7, 12.1, 12.2, 12.3, 12.4, 13.1, 13.2, 13.3, 13.4, 13.5_

  - [x] 2.2 Implement request deduplication check and conflict policy check
    - Step 3: For each `RequestDedupeOp`, derive `request_dedupe.key` from `(namespace_id, workflow_id, request_id)` and query `SELECT 1 FROM request_dedupe WHERE key = $1`; if found, rollback and return `CommitResult::Duplicate`
    - Step 4: For start transitions (`expected_seq == 0` and `next_state.status.is_open()`), derive `current_execution.key` from `(namespace_id, workflow_id)` and query `SELECT run_key, is_open FROM current_execution WHERE key = $1`; apply `Reject` / `AllowAfterClose` policy logic; return `CommitResult::Conflict` when violated
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 5.1, 5.2_

  - [x] 2.3 Implement the write set (upserts, inserts, deletes)
    - Upsert `workflow_hot` with `INSERT ... ON CONFLICT (run_key) DO UPDATE SET transition_seq, state_data, shard_id, updated_at`; serialize `next_state` via `codec::encode_workflow_state`
    - Insert `history_batch` row if transition has history events; serialize via `codec::encode_history_events`; set `first_event_id`, `last_event_id`, `transition_seq`
    - Insert `request_dedupe` records including spread `key`, logical namespace/workflow/request columns, `run_key`, `run_id`, and `first_seen_transition_seq`
    - For each `ActivityOp::Upsert`: upsert `activity_state` with `INSERT ... ON CONFLICT (run_key, schedule_event_id) DO UPDATE`; serialize via `codec::encode_activity_state`
    - For each `ActivityOp::Delete`: `DELETE FROM activity_state WHERE run_key = $1 AND activity_id = $2` — non-PK WHERE clause; DSQL supports DELETE with arbitrary WHERE (only FOR UPDATE requires PK equality); validated against live cluster
    - For each `TimerOp::Upsert`: upsert `timer_bucket` with `INSERT ... ON CONFLICT (shard_id, fire_at, run_key, timer_id) DO UPDATE`; serialize via `codec::encode_timer_state`; bind shard_id via `shard_id_to_uuid`
    - For each `TimerOp::Delete`: `DELETE FROM timer_bucket WHERE run_key = $1 AND timer_id = $2` — non-PK WHERE clause; same rationale as activity delete
    - Upsert `current_execution` ONLY on start transitions (expected_seq == 0, status is open) and close transitions (status is terminal). Intermediate open transitions do NOT touch `current_execution`. Start: `INSERT ... ON CONFLICT (key) DO UPDATE SET run_key, run_id, is_open = true`. Close: `UPDATE current_execution SET is_open = false WHERE key = $1 AND run_key = $2`
    - Insert `projection_log` if transition has projection ops; serialize via `codec::encode_projection_context` and `codec::encode_projection_ops`
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 3.9, 3.10, 4.5, 15.1, 15.2_

  - [x] 2.4 Implement commit and OCC conflict mapping
    - Call `tx.commit().await`; on success return `CommitResult::Applied { new_state: transition.next_state }`
    - Catch `sqlx::Error::Database` with code `40001` via `is_serialization_failure`; map to `CommitResult::Conflict` with reason "DSQL serialization conflict"
    - Propagate other errors as `anyhow::Error`
    - _Requirements: 2.5, 2.6, 2.7_

- [x] 3. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 4. Implement read operations
  - [x] 4.1 Implement `load_run`
    - Acquire `DbClass::Read` permit
    - Query `SELECT state_data FROM workflow_hot WHERE run_key = $1`
    - Deserialize via `codec::decode_workflow_state`; return `LoadedRun::Existing` if found, `LoadedRun::Absent` if not
    - _Requirements: 1.4, 6.1, 6.2, 6.3_

  - [x] 4.2 Implement `resolve_execution`
    - Acquire `DbClass::Read` permit
    - Without `run_id`: derive `current_execution.key` and query `SELECT run_key FROM current_execution WHERE key = $1 AND is_open = true`
    - With `run_id`: derive `RunKey` from `(namespace_id, workflow_id, run_id)` and query `SELECT 1 FROM workflow_hot WHERE run_key = $1`; return the derived key only when the row exists
    - Return `Some(RunKey)` or `None`
    - _Requirements: 1.4, 7.1, 7.2_

  - [x] 4.3 Implement `find_latest_run`
    - Acquire `DbClass::Read` permit
    - Derive `current_execution.key` and query `SELECT run_key FROM current_execution WHERE key = $1`
    - Return `Some(RunKey)` or `None`
    - _Requirements: 1.4, 8.1_

  - [x] 4.4 Implement `read_history`
    - Acquire `DbClass::Read` permit
    - Query `SELECT first_event_id, last_event_id, events_data FROM history_batch WHERE run_key = $1 AND last_event_id > $2 ORDER BY first_event_id ASC`
    - Deserialize each batch via `codec::decode_history_events`; filter events with `event_id <= after_event_id` from first batch; collect up to `limit` events
    - Return empty vec if no events found
    - _Requirements: 1.4, 9.1, 9.2, 9.3, 9.4, 9.5_

  - [x] 4.5 Implement `lookup_request_dedupe`
    - Acquire `DbClass::Read` permit
    - Derive `request_dedupe.key` and query `SELECT run_key, request_id, first_seen_transition_seq, run_id FROM request_dedupe WHERE key = $1`
    - If caller's `ExecutionRef` has a `run_id`, only return the record if the stored `run_id` matches
    - Return `Some(RequestRecord)` or `None`
    - _Requirements: 1.4, 10.1, 10.2, 10.3_

  - [x] 4.6 Implement `read_transition_audit`
    - Acquire `DbClass::Read` permit
    - Query `SELECT transition_seq, events_data FROM history_batch WHERE run_key = $1 ORDER BY first_event_id ASC`
    - Reconstruct `TransitionAuditRecord` per batch with history events populated; activity_ops, timer_ops, dispatch_ops, projection_ops as empty vectors
    - Return empty vec if no batches found
    - _Requirements: 1.4, 14.1, 14.2_

- [x] 5. Implement `materialize_reset_successor`
  - [x] 5.1 Implement reset successor materialization
    - Acquire `DbClass::Commit` permit; begin transaction
    - Load base run's `WorkflowState` from `workflow_hot`: `SELECT state_data FROM workflow_hot WHERE run_key = $1`; error if base run not found
    - Read base run's history batches: `SELECT events_data, first_event_id, last_event_id FROM history_batch WHERE run_key = $1 ORDER BY first_event_id ASC`
    - Collect events through `fork_event_id`; error if `fork_event_id` is beyond committed history
    - Build `ReplayContext` from base `WorkflowState` (copy `namespace_id`, `workflow_id`, `deployment`, `build_id`, `parent_run_key`, `parent_workflow_id`, `first_run_started_at`) and substitute successor identity (`successor_run_key`, `successor_run_id`)
    - Call `BasicKernel::replay_history_prefix(replay_ctx, &copied_events)` to derive successor `WorkflowState`
    - Insert `workflow_hot` row for successor with derived state
    - Insert `history_batch` row for successor with copied events
    - Insert `current_execution` row for successor
    - Insert `activity_state` rows for each activity in successor state
    - Insert `timer_bucket` rows for each timer in successor state
    - Use `shard_for_run_key(successor_run_key)` for shard assignment; bind via `shard_id_to_uuid`
    - Commit transaction
    - _Requirements: 1.3, 11.1, 11.2, 11.3, 11.4, 11.5, 11.6, 11.7_

- [x] 6. Implement stub methods and wire into DsqlStore
  - [x] 6.1 Implement DSQL backlog persistence/drain and stub remaining side-table reads
    - `list_dispatchable_workflow_tasks` → `unimplemented!("Feature 3: dsql-side-tables")`
    - `list_dispatchable_activity_tasks` → `unimplemented!("Feature 3: dsql-side-tables")`
    - `persist_to_backlog` inserts `dispatch_backlog` rows with spread `key`, `partition_id`, full `QueueKey` identity (`queue_namespace`, `queue_name`, `task_kind`, `deployment`, `build_id`), `insertion_seq`, and serialized payload
    - `drain_backlog` uses the queue/FIFO async index, null-safe `deployment`/`build_id` predicates, and deletes drained rows by spread `key`
    - Add `V023__idx_dispatch_backlog_queue_seq.sql` for the full queue identity drain path
    - `list_due_timers` → `unimplemented!("Feature 3: dsql-side-tables")`
    - `list_dispatchable_workflow_tasks_for_shard` → `unimplemented!("Feature 3: dsql-side-tables")`
    - `list_dispatchable_activity_tasks_for_shard` → `unimplemented!("Feature 3: dsql-side-tables")`
    - `list_due_timers_for_shard` → `unimplemented!("Feature 3: dsql-side-tables")`
    - `list_runs_with_workflow_timeouts_for_shard` → `unimplemented!("Feature 3: dsql-side-tables")`
    - `list_started_workflow_tasks_for_shard` → `unimplemented!("Feature 3: dsql-side-tables")`
    - `list_open_activities_for_shard` → `unimplemented!("Feature 3: dsql-side-tables")`
    - `list_pending_nexus_operations_for_shard` → `unimplemented!("Feature 3: dsql-side-tables")`
    - _Requirements: 1.1_

  - [x] 6.2 Wire `DsqlRunRepository` into `DsqlStore`
    - Change `DsqlStore.director` field from `DsqlConnectionDirector` to `Arc<DsqlConnectionDirector>` so both the store accessor and the repository share the same `Arc`
    - Update `connection_director()` to return `&DsqlConnectionDirector` (deref through Arc)
    - Add `run_repository: DsqlRunRepository` field to `DsqlStore`
    - Add `pub fn run_repository(&self) -> &DsqlRunRepository` accessor
    - Update `DsqlStore::from_connector` to wrap director in `Arc`, construct `DsqlRunRepository` with `Arc::clone(&director)`, `config.shard_count`, and `config.conflict_policy`
    - _Requirements: 1.1, 1.2_

- [x] 7. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 8. Unit tests
  - [x] 8.1 Write unit tests for `DsqlRunRepository` helpers
    - Test `shard_for_run_key` determinism: same `RunKey` and shard count always produces same `ShardId`
    - Test `shard_for_run_key` matches in-memory store's `shard_for_run_key` function
    - Test `shard_id_to_uuid` determinism: same `ShardId` always produces same UUID
    - Test `shard_id_to_uuid` uses the spread UUID helper rather than the removed SHA-256 shard helper
    - Test `DsqlRunRepository::new()` rejects `shard_count == 0` with an error
    - Test `is_serialization_failure` returns `true` for SQLSTATE 40001 and `false` for other codes
    - Test `ShardEpoch::ZERO` bypass logic (verify epoch fence is skipped)
    - All tests in `#[cfg(test)] mod tests` inside `run_repository.rs`
    - _Requirements: 12.1, 13.5_

  - [x] 8.2 Write unit tests for DbClass routing
    - Introduce a small internal `ConnectionAcquirer` trait (or make `DsqlRunRepository` generic over `ConnectionDirector`) so that a test-only mock can record which `DbClass` was requested without starting a real reservoir
    - Verify `commit_transition` and `materialize_reset_successor` use `DbClass::Commit`
    - Verify `load_run`, `resolve_execution`, `find_latest_run`, `read_history`, `lookup_request_dedupe`, `read_transition_audit` use `DbClass::Read`
    - _Requirements: 1.3, 1.4_

- [x] 9. Property-based tests
  - [x] 9.1 Write property test for shard assignment determinism
    - **Property 13: Shard assignment determinism**
    - Generate random `RunKey` values and non-zero shard counts (1..=u32::MAX); verify `shard_for_run_key` is deterministic and matches the in-memory store's mapping
    - **Validates: Requirements 13.5**

  - [x] 9.2 Write property test for codec serialization round trip
    - **Property 7: Codec serialization round trip**
    - Extend existing codec tests: generate random `WorkflowState`, `Vec<HistoryEvent>`, `ActivityState`, `TimerState`, `ProjectionContext`, `Vec<ProjectionOp>` instances; encode then decode; verify equality
    - **Validates: Requirements 6.3, 9.5**

  - [x] 9.3 Write property test for OCC fencing rejects stale callers
    - **Property 1: OCC fencing rejects stale callers**
    - Generate random `(current_seq, expected_seq, caller_epoch, durable_epoch)` tuples; verify mismatches produce `CommitResult::Conflict` in the in-memory store
    - **Validates: Requirements 2.2, 2.3, 12.2, 12.3, 13.2, 13.3**

  - [x] 9.4 Write property test for commit-then-load round trip
    - **Property 2: Commit-then-load round trip**
    - Generate valid transitions; commit to in-memory store; load_run and verify state equals `transition.next_state`
    - **Validates: Requirements 2.5, 3.1, 6.1, 6.3**

  - [x] 9.5 Write property test for commit-then-read-history round trip
    - **Property 3: Commit-then-read-history round trip**
    - Generate transitions with history events; commit to in-memory store; read_history with `after_event_id = 0`; verify events match
    - **Validates: Requirements 3.2, 9.1, 9.2, 9.5**

  - [x] 9.6 Write property test for commit-then-lookup-dedupe round trip
    - **Property 4: Commit-then-lookup-dedupe round trip**
    - Generate transitions with `RequestDedupeOp` entries; commit to in-memory store; lookup and verify `RequestRecord` fields match
    - **Validates: Requirements 3.7, 10.1**

  - [x] 9.7 Write property test for start-workflow conflict policy enforcement
    - **Property 5: Start-workflow conflict policy enforcement**
    - Generate workflow starts with various conflict policies and existing execution states; verify conflict detection matches in-memory store behavior
    - **Validates: Requirements 4.1, 4.2, 4.3, 4.4**

  - [x] 9.8 Write property test for duplicate request detection
    - **Property 6: Duplicate request detection**
    - Generate request IDs; commit once; attempt duplicate; verify `CommitResult::Duplicate`
    - **Validates: Requirements 5.1, 5.2**

  - [x] 9.9 Write property test for resolve execution correctness
    - **Property 8: Resolve execution correctness**
    - Generate committed runs; test `resolve_execution` with and without `run_id`; verify results match in-memory store
    - **Validates: Requirements 7.1, 7.2**

  - [x] 9.10 Write property test for find latest run returns most recent
    - **Property 9: Find latest run returns most recent**
    - Generate multiple committed runs for the same workflow; verify `find_latest_run` returns the most recently committed run
    - **Validates: Requirements 8.1**

  - [x] 9.11 Write property test for read history respects limit
    - **Property 10: Read history respects limit**
    - Generate histories and limit values; verify result length <= limit, correct ordering, and `event_id > after_event_id`
    - **Validates: Requirements 9.3**

  - [x] 9.12 Write property test for workflow close updates current execution
    - **Property 11: Workflow close updates current execution**
    - Generate transitions that close workflows; verify `resolve_execution` returns `None` for open-only queries after close
    - **Validates: Requirements 4.5, 15.1, 15.2**

  - [x] 9.13 Write property test for materialize reset successor preserves history prefix
    - **Property 12: Materialize reset successor preserves history prefix**
    - Generate base runs with history and valid `fork_event_id`; materialize successor; verify `read_history` returns prefix and `load_run` returns replayed state
    - **Validates: Requirements 11.1, 11.2, 11.3, 11.4, 11.5**

- [x] 10. Final checkpoint — Ensure all tests pass
  - Completed: `cargo test -p tokeira-types`, `cargo test -p tokeira-storage --features dsql`, `cargo check --workspace`, and `cargo lint`
  - Resolved (Task 11): the macOS-debug `TrustStore configured to enable native roots...` failure is fixed. Root cause was `DsqlCoordinationConfig::default()` and the two `local_for_tests` constructors eagerly building a real `aws_sdk_dynamodb::Client` (constructing a rustls/native-roots TLS connector) for clients that never dial — a `debug_assert!` in `aws-smithy-http-client` that fires only in debug builds when native roots yield zero parseable certs. NOT `tokeira-state` (the original note mis-attributed it) and NOT environmental.
  - `cargo test -p tokeira-storage --features dsql` is green on macOS debug (102 dsql tests pass, including the new `aws_http` guards).

- [x] 11. Remove eager AWS client construction from defaulting paths (all-platform, test + production)
  - **Principle:** Config defaults SHALL default pure values only — never external clients, network stacks, TLS providers, credential providers, or OS trust-store reads. A live `aws_sdk_dynamodb::Client` is a runtime resource, not configuration data, and SHALL be injected explicitly. The fix MUST be unconditional across platforms — no `#[cfg(target_os)]` gates, no trust-store workaround, no swap of the production TLS root source.
  - **Non-goal:** Do NOT change the production TLS/root-store path. Production keeps the AWS SDK default HTTPS stack (hyper + rustls + aws-lc-rs, native roots) built via `aws_config::defaults(BehaviorVersion::latest())`. Private/corporate-CA support via a custom `TlsContext`/`TrustStore` is explicitly out of scope and, if ever needed, is a separate future task.

  - [x] 11.1 Make `DsqlCoordinationConfig` pure data; thread the client through `connect`
    - In `crates/tokeira-storage/src/dsql/config.rs`: remove the `ddb_client: aws_sdk_dynamodb::Client` field from `DsqlCoordinationConfig`. The struct retains only `rate_limiter_table: String` and `conn_lease_table: String`. Restore a derived `#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]` (now possible because all fields are pure) with table-name defaults preserved via `Default`/field defaults; delete the hand-written `impl Default` that called `Client::from_conf`.
    - `DsqlPoolConfig::default()` becomes panic-free and network-free transitively. Keep `DsqlPoolConfig`'s manual `Default` for its non-serde fields if still required, but it MUST NOT construct any AWS client.
    - In `crates/tokeira-storage/src/dsql/mod.rs`: change `DsqlStore::connect(auth, config)` to `DsqlStore::connect(auth, config, ddb_client: aws_sdk_dynamodb::Client)`. Pass `ddb_client.clone()` into `DistributedTokenBucket::new(...)` and `SlotBlockManager::start(...)` where `config.coordination.ddb_client` is read today (mod.rs ~82 and ~90). `from_reservoir` is unchanged (it never referenced the client).
    - _Requirements: 1.1_

  - [x] 11.2 Add a single shared offline HTTP client helper for test/local paths
    - Add `crates/tokeira-storage/src/dsql/aws_http.rs` (gated behind the `dsql` feature) exposing `pub(crate) fn offline_ddb_client() -> aws_sdk_dynamodb::Client`.
    - Build it with a no-network HTTP client injected via `aws_sdk_dynamodb::config::Builder::http_client(...)`, using `aws_smithy_runtime_api::client::http::http_client_fn(...)` returning a `SharedHttpConnector::new(...)` whose `HttpConnector::call` resolves immediately to a `ConnectorError` (never touches DNS, TLS, credentials, OS roots, or sockets). All of `http_client_fn`, `SharedHttpConnector`, `HttpConnector`, `HttpConnectorFuture`, and `ConnectorError` are reachable transitively / via `aws_sdk_dynamodb::config` re-exports — **no new dependency** is added. Set a dummy region + `BehaviorVersion::latest()` so client construction is total.
    - Register `mod aws_http;` in `dsql/mod.rs`.
    - _Requirements: 1.1_

  - [x] 11.3 Route every test/local constructor through the offline client
    - `DistributedTokenBucket::local_for_tests` (distributed_bucket.rs ~62): replace the inline `Client::from_conf(...)` with `crate::dsql::aws_http::offline_ddb_client()`.
    - `SlotBlockManager::local_for_tests` (slot_block_manager.rs ~71): same replacement.
    - These remain `#[cfg(any(test, feature = "dsql-integration"))]` and keep their `local_only`/test semantics (they already never dial — `validate_table` short-circuits for the bucket; the slot manager test path never calls AWS).
    - _Requirements: 1.1_

  - [x] 11.4 Update production and test call sites for the new `connect` signature
    - `apps/tokeirad/src/lib.rs`: pass the real `aws_sdk_dynamodb::Client` (already built in `dsql_pool_config`/startup) as the new `connect` argument; drop `ddb_client` from the `DsqlCoordinationConfig` literal in `dsql_pool_config_with_client`. Update the unit test at ~1145 that reads `DsqlCoordinationConfig::default().ddb_client` to use `offline_ddb_client()` (or a test-only re-export) instead.
    - `apps/tokeira-controller/src/main.rs` (~325) and `apps/tokeira-autoscaler/src/main.rs` (~199): they already build `ddb_client` via `aws_sdk_dynamodb::Client::new(&sdk_config)`; pass it to `connect(...)` and drop `ddb_client` from the `DsqlCoordinationConfig` literal (set only the two table names).
    - `from_database_url_for_tests` (mod.rs ~112): unchanged in signature — it already uses the `local_for_tests` constructors, which now build offline clients internally.
    - _Requirements: 1.1_

  - [x] 11.5 Guard test — defaults construct with no panic and no network/TLS init
    - In `dsql/config.rs` tests: assert `DsqlCoordinationConfig::default()` and `DsqlPoolConfig::default()` construct and `validate()` with no panic (this is the regression guard for the original `debug_assert!`).
    - Add a property/unit test asserting `offline_ddb_client()` constructs successfully and that a representative call (e.g. a `describe_table` future) resolves to an error rather than attempting a real connection — proving the connector is inert.
    - These run green under `cargo test --workspace` on macOS (debug) and Linux.
    - _Requirements: 1.1_

  - [x] 11.6 Verify
    - `cargo test -p tokeira-storage --features dsql` (previously-panicking `config`, `connection`, `slot_block_manager` tests pass on macOS debug).
    - `cargo check --workspace` and `cargo lint` (production binaries compile against the new `connect` signature).
    - `cargo +nightly fmt --all`.
    - _Requirements: 1.1_

## Notes

- All unit and property-based tests are required
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties against the in-memory store as behavioral oracle
- Unit tests validate specific examples and edge cases
- All DSQL code is gated behind `#[cfg(feature = "dsql")]`
- Integration tests against a live DSQL cluster are gated behind `dsql-integration` feature flag and are NOT part of this task list — they are run manually
- `current_execution`, `request_dedupe`, and `dispatch_backlog` now use spread UUID primary keys while retaining logical columns and async indexes for operator/query paths.
