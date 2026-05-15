# Implementation Plan: DSQL Reservoir Redesign

## Overview

Eliminate the sqlx `PgPool` from the DSQL connection path. The reservoir becomes the sole connection owner, creating physical connections directly via IAM-authenticated TCP/TLS. Background tasks manage the connection lifecycle (refiller, expiry scanner, return processor). DynamoDB-backed coordination distributes the rate budget and connection count across nodes.

Implementation follows the natural dependency order: foundational components first (connection factory, DynamoDB coordination), then the restructured reservoir, then admission control and integration.

## Tasks

- [ ] 0. Prepare DSQL coordination configuration and dependencies
  - [ ] 0.1 Add AWS SDK dependencies for DSQL coordination
    - Add `aws-sdk-dynamodb` to `crates/tokeira-storage/Cargo.toml` as an optional dependency behind the `dsql` feature.
    - Add `aws-config` where the DynamoDB client is constructed; if `apps/tokeirad` constructs the client, add it to `apps/tokeirad/Cargo.toml`.
    - Add `aws-sdk-dynamodb` to `apps/tokeirad/Cargo.toml` if `apps/tokeirad` constructs `aws_sdk_dynamodb::Client` directly for `DsqlCoordinationConfig`.
    - Ensure `cargo check --workspace` can compile `crates/tokeira-storage/src/dsql/distributed_bucket.rs` and `slot_block_manager.rs` without hidden dependency assumptions.
    - _Requirements: 7.4, 8.1, 14.2, 14.7_

  - [ ] 0.2 Add `DsqlCoordinationConfig` to the DSQL startup config
    - Define `DsqlCoordinationConfig` in `crates/tokeira-storage/src/dsql/config.rs` with `rate_limiter_table: String`, `conn_lease_table: String`, and `ddb_client: aws_sdk_dynamodb::Client`.
    - Add `coordination: DsqlCoordinationConfig` to `DsqlPoolConfig`.
    - Adjust `DsqlPoolConfig` derives/manual impls as needed because `aws_sdk_dynamodb::Client` is runtime state and is not TOML-serializable.
    - In `apps/tokeirad/src/lib.rs`, derive table names from `effective_config.infrastructure.cluster_name`: `{project}-dsql-rate-limiter` and `{project}-dsql-conn-lease`.
    - Construct an AWS SDK config/client using the effective DSQL region and populate `DsqlPoolConfig.coordination` before calling `DsqlStore::connect(auth, pool_config)`.
    - _Requirements: 14.2, 14.6, 14.7, 19.5_

  - [ ] 0.3 Generalize `MigrationRunner` to support raw DSQL connections
    - Change `MigrationRunner::{apply,status,dry_run}` signatures from `&PgPool` to `impl sqlx::Executor<'_, Database = Postgres>` where the method touches the database.
    - Preserve compatibility for existing `&PgPool` callers while allowing `&mut PgConnection` callers.
    - Update `apps/tkr/src/commands/schema.rs` to create one raw `PgConnection` through `ConnectionFactory` and pass it directly to the migration runner for schema setup/status.
    - Keep schema commands outside the reservoir; schema operations do not need warm pooled connections.
    - _Requirements: 1.1, 2.5_

- [ ] 1. Implement Connection Factory
  - [ ] 1.1 Create `crates/tokeira-storage/src/dsql/connection_factory.rs`
    - Define `ConnectionFactory` struct with `endpoint: String` and `region: String`
    - Implement `create_connection(&self) -> Result<PgConnection, ConnectionFactoryError>` using `DsqlConnectOptions` and `aurora_dsql_sqlx_connector::connection::connect_with(&options)`
    - Define `ConnectionFactoryError` enum with variants: `Iam`, `Tls`, `Timeout`, `Refused`, `Other`
    - Implement `category(&self) -> &'static str` for metric labelling
    - Implement `from_dsql_error` helper to classify `aurora_dsql_sqlx_connector::DsqlError` into the correct variant
    - Register module in `crates/tokeira-storage/src/dsql/mod.rs`
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 10.1, 10.2, 10.3, 10.4_

  - [ ]* 1.2 Write unit tests for Connection Factory error classification
    - Test that relevant `DsqlError` variants map to the correct `ConnectionFactoryError` variant, including embedded `sqlx::Error` classification for `ConnectionError`
    - Test `category()` returns expected string for each variant
    - _Requirements: 1.4_

  - [ ] 1.3 Migrate DSQL entry points away from PgPool
    - Remove `DsqlConnector::new(pool: PgPool)` and replace production construction with `ConnectionFactory::new(endpoint, region)`
    - Remove `DsqlStore::from_pool(pool, config)` and replace it with a test-only constructor that accepts a pre-built reservoir or mock connection source
    - Update `DsqlStore::connect(auth, config)` to use the new reservoir startup path instead of creating a `DsqlConnector` with a pool
    - Update DSQL integration tests that use `DsqlStore::from_pool` to use the test factory or the new `ConnectionFactory` directly
    - _Requirements: 1.1, 2.4, 2.5_

- [ ] 2. Implement Distributed Token Bucket
  - [ ] 2.1 Create `crates/tokeira-storage/src/dsql/distributed_bucket.rs`
    - Define `DistributedTokenBucket` struct with DynamoDB client, table name, endpoint, rate, capacity
    - Implement `wait(&self) -> Result<(), TokenBucketError>` with optimistic read-modify-write loop
    - Implement `try_acquire(&self) -> Result<(bool, i64), TokenBucketError>` with milli-token math
    - Use `ConsistentRead: true` on GetItem
    - Use condition expression `last_refill_ms = :expected_refill` for existing buckets
    - Use condition expression `attribute_not_exists(pk)` for new buckets
    - Handle `ConditionalCheckFailedException` as retry (not error)
    - Implement `MAX_WAIT = 30s` deadline with jittered backoff
    - Define `TokenBucketError` enum: `Timeout`, `DynamoDb`
    - Register module in `crates/tokeira-storage/src/dsql/mod.rs`
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5_

  - [ ]* 2.2 Write property test for distributed rate burst limit
    - **Property 10: Distributed Rate Burst Limit**
    - Verify bucket cannot dispense more than `DISTRIBUTED_BURST` (1000) tokens without refill time
    - **Validates: Requirements 7.3**

- [ ] 3. Implement Slot Block Manager
  - [ ] 3.1 Create `crates/tokeira-storage/src/dsql/slot_block_manager.rs`
    - Define `SlotBlockManager` struct with DynamoDB client, table, endpoint, owner_id, slots_per_block, block_count, ttl, renew_period
    - Generate `owner_id` as hex-encoded 16 random bytes at construction
    - Implement `acquire_slots(&self, target_slots: u32) -> Result<u32>` with randomized start index
    - Implement `try_acquire_block(&self, block_idx: u32) -> Result<bool>` with conditional PutItem
    - Condition: `attribute_not_exists(pk) OR owner_id = :empty OR ttl_epoch < :now`
    - Implement `has_budget(&self) -> bool` as O(1) atomic check
    - Implement `acquire_slot(&self) -> Result<(), SlotBudgetExhausted>` with atomic increment/rollback
    - Implement `release_slot(&self)` with atomic decrement
    - Implement `renew_loop` background task with `renew_block` conditional update
    - If `renew_block` returns `ConditionalCheckFailedException`, remove the block from `owned_blocks`, subtract `SLOT_BLOCK_SIZE` from `total_slots`, emit `tokeira_dsql_slot_block_lost_total`, and continue with reduced capacity
    - Implement `release_all(&self)` for graceful shutdown (clear owner_id, don't delete)
    - Use `RwLock<HashSet<u32>>` for owned_blocks, `AtomicU32` for total_slots, `AtomicI64` for used_slots
    - Register module in `crates/tokeira-storage/src/dsql/mod.rs`
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7_

  - [ ]* 3.2 Write property test for slot budget enforcement
    - **Property 9: Slot Budget Enforcement**
    - For random `blocks in 0..10` and `connections in 0..1000`, verify `has_budget()` and `acquire_slot()` enforce `N × SLOT_BLOCK_SIZE` limit
    - **Validates: Requirements 8.3**

- [ ] 4. Checkpoint — Connection Factory and DynamoDB coordination compile
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 5. Restructure Reservoir
  - [ ] 5.1 Rewrite `crates/tokeira-storage/src/dsql/reservoir.rs`
    - Define `PhysicalConn` struct: `connection: PgConnection`, `created_at: Instant`, `lifetime: Duration`
    - Implement `remaining_lifetime()` and `within_guard_window(guard_window)` methods
    - Define `ReturnedConn` struct: `connection: PgConnection`, `created_at: Instant`, `lifetime: Duration`, `marked_bad: bool`
    - Define `Reservoir` struct with `ready_rx/ready_tx: async_channel`, `return_tx: mpsc::UnboundedSender`, task handles
    - Implement `Reservoir::start(factory, bucket, slots) -> Result<Self>` spawning three background tasks
    - Implement `checkout(&self) -> Result<PhysicalConn, ReservoirError>` as non-blocking `try_recv()`
    - Implement `warmup(&self) -> WarmupResult` polling until `ready_count >= TARGET_READY` or `WARMUP_TIMEOUT`
    - Implement `return_sender()` and `ready_count()`
    - Define all internal constants: `TARGET_READY=50`, `SCAN_BUDGET=TARGET_READY/2`, `BASE_LIFETIME=10min`, `LIFETIME_JITTER=±2min`, `GUARD_WINDOW=45s`, `INFLIGHT_LIMIT=8`, `SCAN_INTERVAL=1s`, `WARMUP_TIMEOUT=30s`, `REFILLER_IDLE_INTERVAL=100ms`, `REFILLER_ERROR_BACKOFF=250ms`
    - Remove all sqlx `PgPool` usage from the module
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 3.1, 3.2, 3.3, 11.1, 11.2, 15.1, 15.4_

  - [ ] 5.2 Implement refiller loop
    - Spawn as dedicated tokio task
    - Loop: check `ready_tx.len() >= TARGET_READY` → idle sleep; check `slot_manager.has_budget()` → idle sleep
    - Acquire inflight semaphore permit (max 8 concurrent)
    - Call `slot_manager.acquire_slot()` to reserve one connection slot before attempting creation
    - Call `distributed_bucket.wait()` for rate limiting
    - Call `factory.create_connection()` → on success, assign jittered lifetime and send to ready channel
    - If sending the created connection to `ready_tx` fails, call `slot_manager.release_slot()` and drop the connection
    - On rate-limit or connection creation error: call `slot_manager.release_slot()`, log, backoff 250ms with exponential increase, retry
    - Implement `assign_jittered_lifetime()` using `rand::gen_range`
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 12.3, 12.4_

  - [ ] 5.3 Implement expiry scanner
    - Spawn as dedicated tokio task with `SCAN_INTERVAL` (1s) interval
    - Each pass: drain up to `SCAN_BUDGET` (`TARGET_READY / 2`, 25 entries) from ready channel
    - For each: if `within_guard_window(GUARD_WINDOW)` → call `slot_manager.release_slot()`, drop, and emit metric; else → put back
    - If putting a healthy scanned connection back into `ready_tx` fails, call `slot_manager.release_slot()` and drop the connection
    - Thread `Arc<SlotBlockManager>` into the scanner task
    - Bounded scan prevents starvation of concurrent checkout callers
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5_

  - [ ] 5.4 Implement return processor
    - Spawn as dedicated tokio task receiving from `return_rx`
    - If `marked_bad` → call `slot_manager.release_slot()`, discard, and emit metric
    - If `within_guard_window(GUARD_WINDOW)` → call `slot_manager.release_slot()`, discard, and emit metric
    - Otherwise → send back to ready channel (NO ping, no network I/O)
    - If sending the returned connection back to `ready_tx` fails, call `slot_manager.release_slot()` and drop the connection
    - Thread `Arc<SlotBlockManager>` into the return processor task
    - _Requirements: 6.1, 6.2, 6.6, 16.1, 16.2, 16.3, 16.4, 16.5_

  - [ ]* 5.5 Write property tests for reservoir invariants
    - **Property 1: Lifetime Jitter Bounds** — verify `assign_jittered_lifetime()` output in [8min, 12min]
    - **Property 2: Lifetime Safety Against Token TTL** — verify `BASE_LIFETIME + LIFETIME_JITTER + GUARD_WINDOW < 15min`
    - **Property 3: Guard Window Enforcement** — random (age, lifetime) pairs, verify retirement decision
    - **Property 8: Bounded Scan Per Pass** — verify scanner examines at most `SCAN_BUDGET` entries
    - **Property 11: Non-Blocking Checkout** — verify `checkout()` returns immediately for any channel state
    - **Property 12: Inflight Concurrency Limit** — verify semaphore enforcement at `INFLIGHT_LIMIT`
    - **Validates: Requirements 4.6, 5.2, 5.4, 11.3, 11.4, 3.1, 3.2, 4.4**

- [ ] 6. Checkpoint — Reservoir restructure compiles and property tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 7. Implement Class Budgets and DsqlPermit
  - [ ] 7.1 Modify `crates/tokeira-storage/src/dsql/connection.rs` for class-based admission
    - Define `ClassBudgets` struct with per-class `Arc<Semaphore>` instances
    - Implement allocation from `TARGET_READY`: commit 50%, read 20%, projection 10%, control 10%, maintenance 10%
    - Implement `acquire(class: DbClass) -> Result<OwnedSemaphorePermit>` that blocks until permit available
    - Modify `DsqlConnectionDirector::acquire` to: acquire class permit first, then `reservoir.checkout()`
    - On `ReservoirError::Empty`: drop class guard, return backpressure error
    - Emit per-class metrics: total permits, in-use, wait duration
    - Record `tokeira_dsql_class_permit_wait_duration_seconds` from before `semaphore.acquire_owned()` to after acquisition
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5_

  - [ ] 7.2 Rewrite `DsqlPermit` in `crates/tokeira-storage/src/dsql/connection.rs`
    - Define `DsqlPermit` struct: `class`, `connection: Option<PgConnection>`, `created_at`, `lifetime`, `marked_bad`, `_class_guard: OwnedSemaphorePermit`, `reservoir_return: mpsc::UnboundedSender<ReturnedConn>`, `slot_manager: Arc<SlotBlockManager>`, `director_in_flight: Arc<AtomicUsize>`
    - Implement `connection(&mut self) -> Result<&mut PgConnection>`
    - Implement `mark_bad(&mut self)` setting the flag
    - Implement `Drop` for `DsqlPermit`: decrement in_flight, send `ReturnedConn` to return channel, or call `slot_manager.release_slot()` if lifetime is already exceeded or the return-channel send fails and the connection is discarded synchronously
    - Remove all sqlx `PoolConnection<Postgres>` usage
    - _Requirements: 2.3, 16.3, 16.6_

  - [ ]* 7.3 Write property tests for class budget invariants
    - **Property 6: Class Budget Sum Invariant** — for `target_ready in 5..500`, verify sum equals target and each class ≥ 1
    - **Property 7: Class Isolation** — exhaust one class, verify others unchanged
    - **Validates: Requirements 9.1, 9.4**

  - [ ]* 7.4 Write unit tests for DsqlPermit lifecycle
    - Test `Drop` sends `ReturnedConn` to return channel
    - Test `mark_bad()` propagates to `ReturnedConn.marked_bad`
    - Test `connection()` returns mutable reference
    - _Requirements: 16.3_

  - [ ]* 7.5 Write property test for bad connection discard and no-network return
    - **Property 4: Bad Connection Discard** — random lifetimes with `marked_bad=true`, verify always discarded
    - **Property 5: No-Network Return Path** — verify no I/O calls on healthy return outside guard window
    - **Validates: Requirements 6.6, 16.3, 16.4, 16.5, 16.6**

- [ ] 8. Checkpoint — Class budgets and DsqlPermit compile, all property tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 9. Warmup and Startup Integration
  - [ ] 9.1 Implement startup sequence in `apps/tokeirad/src/lib.rs`
    - Keep the existing `build_and_serve` DSQL branch calling `DsqlStore::connect(auth, config)`
    - Derive `DsqlCoordinationConfig` table names from `effective_config.infrastructure.cluster_name`: `{project}-dsql-rate-limiter` and `{project}-dsql-conn-lease`
    - Construct `aws_sdk_dynamodb::Client` from the effective DSQL region and place it in `DsqlPoolConfig.coordination`
    - Rewrite the internals of `DsqlStore::connect(auth, config)` to own the full reservoir startup sequence
    - Add DynamoDB table reachability check (fail hard with clear error if unreachable)
    - Instantiate `SlotBlockManager` with `config.coordination.conn_lease_table` and call `acquire_slots(TARGET_READY)`
    - Instantiate `DistributedTokenBucket` with `config.coordination.rate_limiter_table`
    - Call `Reservoir::start(factory, bucket, slots)`
    - Call `reservoir.warmup()` — log warning if partial fill, proceed
    - Build `DsqlConnectionDirector` with `ClassBudgets`
    - Ensure gRPC traffic is not accepted until warmup completes
    - Do not create a separate bootstrap flow and do not replace the external `DsqlStore::connect` API at the call site
    - _Requirements: 14.2, 14.3, 14.6, 14.7, 15.1, 15.2, 15.3, 15.4_

  - [ ] 9.2 Implement graceful shutdown for reservoir components
    - Abort refiller, scanner, return processor tasks
    - Close ready and return channels
    - Call `SlotBlockManager::release_all()`
    - Drop all `PhysicalConn` instances
    - Wire into existing `tokeirad` shutdown sequence
    - _Requirements: 8.5_

  - [ ] 9.3 Add observability metrics emission
    - Emit all metrics from design: reservoir size gauge, checkout histogram, empty counter, in-flight gauge, connection age histogram, rate limiter tokens gauge, class budget gauges, class permit wait histogram, connection create duration histogram, error counter with `error_kind` label, retirement counter with `reason` label
    - Follow existing `tokeira_dsql_reservoir_*` and `tokeira_dsql_pool_*` naming conventions
    - Emit unconditionally via `metrics` crate — no configuration options
    - _Requirements: 13.1, 13.2, 13.3, 13.4, 17.1, 17.2, 17.3_

- [ ] 10. Checkpoint — Full startup sequence works end-to-end
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 11. Compose IaC DynamoDB Provisioning
  - [ ] 11.1 Extend `platforms/compose/src/modules.rs` DSQL module for DynamoDB tables
    - Add DynamoDB table resource for `{project}-dsql-rate-limiter`: on-demand billing, TTL on `ttl_epoch`, partition key `pk` (String)
    - Add DynamoDB table resource for `{project}-dsql-conn-lease`: on-demand billing, TTL on `ttl_epoch`, partition key `pk` (String)
    - Provision in same region as DSQL cluster
    - Tables created by `tkr infra apply`, destroyed by `tkr infra destroy`
    - _Requirements: 14.1, 14.4, 14.5, 19.1, 19.2, 19.3, 19.4, 19.5, 19.6, 19.7_

- [ ] 12. Architecture Documentation
  - [ ] 12.1 Create `docs/architecture/060-connection-management.md`
    - Document reservoir as sole connection owner (no sqlx PgPool)
    - Document refiller's rate-limited creation loop with DynamoDB token bucket
    - Document expiry scanner's proactive retirement with guard window
    - Document return processor's no-ping validation
    - Document class budget admission control with allocation percentages
    - Document distributed token bucket coordination (schema, TTL, milli-token math)
    - Document slot block manager (schema, TTL, crash recovery, graceful release)
    - Explain WHY each decision was made referencing DSQL constraints (100/sec, 10k, 15-min TTL)
    - Include data flow diagram: creation → checkout → use → return → validation → retirement
    - Document invariants: class isolation, non-blocking checkout, rate-limited creation, guard window enforcement
    - Explain DynamoDB tables (schema, TTL behaviour, cost model) and why provisioned for all DSQL deployments
    - No operator-tunable configuration guidance — all parameters are internal constants
    - _Requirements: 18.1, 18.2, 18.3, 18.4, 18.5, 18.6, 18.7_

- [ ] 13. Remove legacy rate limiter
  - [ ] 13.1 Remove `crates/tokeira-storage/src/dsql/rate_limiter.rs`
    - Delete the file (replaced by distributed_bucket.rs)
    - Remove module declaration from `mod.rs`
    - Update any imports that referenced the old rate limiter
    - _Requirements: 7.1_

- [ ] 14. Final checkpoint — Full compilation, all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation between major phases
- Property tests validate universal correctness properties from the design document
- The design uses exact Rust pseudocode — implementation should follow it closely
- All parameters are internal constants (no config changes needed)
- `proptest` is already in the workspace — no new test framework needed
- The existing `rate_limiter.rs` is replaced by `distributed_bucket.rs` (task 13)

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["0.1", "0.2", "0.3"] },
    { "id": 1, "tasks": ["1.1", "2.1", "3.1"] },
    { "id": 2, "tasks": ["1.2", "2.2", "3.2"] },
    { "id": 3, "tasks": ["1.3", "5.1"] },
    { "id": 4, "tasks": ["5.2", "5.3", "5.4"] },
    { "id": 5, "tasks": ["5.5", "7.1"] },
    { "id": 6, "tasks": ["7.2", "7.3"] },
    { "id": 7, "tasks": ["7.4", "7.5"] },
    { "id": 8, "tasks": ["9.1", "9.2", "9.3"] },
    { "id": 9, "tasks": ["11.1", "12.1", "13.1"] }
  ]
}
```
