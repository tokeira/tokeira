# Implementation Plan: DSQL Schema and Connection Foundation

## Overview

This plan implements the foundational DSQL layer for Tokeira in four phases: schema DDL with migration tooling, connection pool with reservoir pattern, IAM authentication integration, and schema validation with testing. All code lives in `tokeira-storage/src/dsql/` with migration files in `tokeira-storage/migrations/`. The implementation builds incrementally — each phase depends on the previous — and ends with full wiring and integration tests.

## Tasks

- [x] 1. Set up dsql module structure and dependencies
  - [x] 1.1 Add DSQL dependencies to tokeira-storage Cargo.toml
    - Add `sqlx` with `runtime-tokio`, `tls-rustls`, `postgres` features
    - Add `aurora-dsql-sqlx-connector` v0.1.2
    - Add `sha2` for migration checksum computation
    - Add `postcard` with `alloc` feature for compact binary serde encoding of BYTEA columns
    - Add `async-channel` for multi-consumer ready pool in reservoir
    - Add `dsql-integration` feature flag gating integration tests
    - _Requirements: 8.1, 12.1, 14.1_

  - [x] 1.2 Create dsql module skeleton and update ConnectionDirector trait
    - Create `src/dsql/mod.rs` with `#[cfg(feature = "dsql")]` gate and submodule declarations
    - Create empty files: `connection.rs`, `reservoir.rs`, `rate_limiter.rs`, `migration.rs`, `validation.rs`, `config.rs`, `codec.rs`
    - Register `pub mod dsql;` in `src/lib.rs` behind feature gate
    - Update `ConnectionDirector` trait in `api.rs` to use associated `Permit` type: `type Permit: Send;` and `async fn acquire(&self, class: DbClass) -> Result<Self::Permit>;`
    - Update `InMemoryStore`'s `ConnectionDirector` impl to use `type Permit = DbPermit;`
    - Update the `impl<T> ConnectionDirector for Arc<T>` blanket impl to forward `type Permit = T::Permit`
    - _Requirements: 8.1, 10.1_

  - [x] 1.3 Add serde derives to domain types for postcard codec
    - Add `Serialize, Deserialize` derives to all domain types that will be persisted as BYTEA: `WorkflowState`, `HistoryEvent`, `ActivityState`, `TimerState`, `BacklogPayload`, `ProjectionContext`, `ProjectionOp`, `ProjectionCursor` and their transitive field types
    - Add `serde` dependency to `tokeira-kernel`, `tokeira-types`, and any other crates owning these types (if not already present)
    - Verify compilation after adding derives
    - _Requirements: 14.1, 14.2_

- [x] 2. Implement configuration types
  - [x] 2.1 Implement internal config types and extend operator-facing config
    - Write internal config structs in `src/dsql/config.rs`: `ReservoirConfig`, `MigrationConfig`, `DsqlPoolConfig` with `serde(deny_unknown_fields)` and defaults (target_ready=50, inflight_limit=8, base_lifetime=50min, lifetime_jitter=5min, guard_window=45s, scan_interval=10s)
    - Extend `DsqlInfraConfig` in `tokeira-config` with `region: Option<String>`, `admin_role_arn: Option<String>`, `runtime_role_arn: Option<String>`, `readonly_role_arn: Option<String>` (all default None)
    - Do NOT add reservoir/rate-limiter fields to `tokeira-config` — these are internal defaults per 015-configuration philosophy
    - Implement validation methods rejecting invalid values (zero targets, lifetime exceeding DSQL hard cutoff minus guard window)
    - _Requirements: 8.3, 8.5, 8.6, 9.1, 11.1, 12.2, 12.4_

  - [x]* 2.2 Write property test for class budget sum invariant
    - **Property 11: Class budget sum invariant**
    - Generate random per-class allocations and verify they sum to the total budget with no negative values
    - **Validates: Requirements 10.2**

  - [x]* 2.3 Write unit tests for config validation
    - Test that invalid configs (zero target_ready, base_lifetime + jitter > 59m15s, negative values) are rejected
    - Test that default configs pass validation
    - Test serde round-trip for all config structs
    - _Requirements: 8.3, 8.5, 8.6_

- [x] 3. Implement schema DDL migration files
  - [x] 3.1 Create migration files V001–V012 (one table per file)
    - Write one migration file per table, each containing a single CREATE TABLE IF NOT EXISTS statement (DSQL one-DDL-per-transaction constraint):
      - `V001__schema_version.sql`, `V002__shard_lease.sql`, `V003__current_execution.sql`, `V004__workflow_hot.sql`, `V005__history_batch.sql`, `V006__request_dedupe.sql`, `V007__activity_state.sql`, `V008__timer_bucket.sql`, `V009__dispatch_backlog.sql`, `V010__projection_log.sql`, `V011__projector_checkpoint.sql`, `V012__vis_execution.sql`
    - Use UUID primary keys for hot-write tables, composite keys per distribution strategy
    - Use BYTEA for all serialized columns with postcard format comments
    - No BIGSERIAL, no CHECK, no FOREIGN KEY, no temp tables, no PL/pgSQL
    - _Requirements: 1.1–1.8, 2.1–2.5, 4.1–4.8, 5.1–5.6_

  - [x] 3.2 Create migration files V013–V018 (one index per file)
    - Write one migration file per index, each containing a single CREATE INDEX ASYNC statement:
      - `V013__idx_workflow_hot_shard.sql`, `V014__idx_activity_state_shard.sql`, `V015__idx_activity_state_queue.sql`, `V016__idx_timer_bucket_shard_fire.sql`, `V017__idx_vis_execution_ns_close.sql`, `V018__idx_vis_execution_ns_type.sql`
    - _Requirements: 3.1–3.7_

- [x] 4. Implement DDL validator
  - [x] 4.1 Implement DdlValidator with DSQL constraint checks
    - Write `src/dsql/validation.rs` with `DdlValidator::validate(sql, filename)` returning `Vec<ValidationIssue>`
    - Detect: BIGSERIAL/SERIAL, CHECK constraints, temp tables, PL/pgSQL/triggers, FOREIGN KEY/REFERENCES, CREATE INDEX without ASYNC, monotonic leading PK columns
    - Define `ValidationIssue` struct with file, line, kind, message fields
    - Define `ValidationKind` enum for each violation type
    - _Requirements: 5.1–5.6, 15.1–15.4_

  - [x]* 4.2 Write property test for DSQL DDL compliance
    - **Property 1: DSQL DDL compliance**
    - Generate SQL strings containing prohibited constructs (BIGSERIAL, CHECK, TEMP TABLE, FOREIGN KEY, CREATE INDEX without ASYNC, PL/pgSQL) and verify DdlValidator catches them; also generate valid DDL and verify it passes
    - **Validates: Requirements 1.8, 2.5, 3.7, 4.8, 5.1–5.4, 5.6, 15.3, 15.4**

  - [x]* 4.3 Write unit tests for DDL validator
    - Test each ValidationKind with specific SQL snippets
    - Test that all 18 migration files (V001–V018) pass validation
    - _Requirements: 15.1, 15.2_

- [x] 5. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 6. Implement migration runner
  - [x] 6.1 Implement migration filename parser and file discovery
    - Write filename parsing logic in `src/dsql/migration.rs` that extracts version number and description from `V{version}__{description}.sql` pattern
    - Implement file discovery that reads `migrations/` directory and sorts by version
    - Implement SHA-256 checksum computation for migration file content
    - _Requirements: 7.1–7.4_

  - [x]* 6.2 Write property test for migration filename parsing
    - **Property 4: Migration filename parsing**
    - Generate random strings and verify the parser correctly accepts/rejects based on the `V{version}__{description}.sql` naming convention
    - **Validates: Requirements 7.2**

  - [x]* 6.3 Write property test for migration version ordering
    - **Property 2: Migration version ordering**
    - Generate random sets of migration file metadata with version numbers and verify the runner sorts them in strictly ascending order
    - **Validates: Requirements 6.5, 7.3**

  - [x]* 6.4 Write property test for migration checksum determinism
    - **Property 5: Migration checksum determinism**
    - Generate random byte sequences and verify computing the checksum twice produces identical results
    - **Validates: Requirements 7.4**

  - [x] 6.5 Implement MigrationRunner with apply, dry_run, validate, and status
    - Implement `MigrationRunner::new(config)`, `apply(pool)`, `dry_run(pool)`, `validate()`, `status(pool)`
    - Each migration file contains exactly one DDL statement — execute it in its own transaction (DSQL one-DDL-per-transaction constraint)
    - Insert `schema_version` record in a separate DML transaction after the DDL succeeds
    - Skip already-applied migrations, detect checksum mismatches, enforce version ordering
    - Report failure state with migration version, failing statement, and error message
    - _Requirements: 6.1–6.8, 7.1–7.5_

  - [x]* 6.6 Write unit tests for migration runner
    - Test filename parsing with valid and invalid filenames
    - Test version ordering with out-of-order files
    - Test dry-run output format
    - Test checksum mismatch detection
    - Test version gap detection
    - _Requirements: 6.4–6.7, 7.2–7.5_

- [x] 7. Implement serialization codec
  - [x] 7.1 Implement postcard codec for all BYTEA column types (depends on Task 1.3)
    - Write `src/dsql/codec.rs` with generic `encode<T: Serialize>` and `decode<T: DeserializeOwned>` using `postcard`, plus typed wrappers for: `WorkflowState`, `Vec<HistoryEvent>`, `ActivityState`, `TimerState`, `BacklogPayload`, `ProjectionContext`, `Vec<ProjectionOp>`, `ProjectionCursor`
    - Requires the serde derives added by Task 1.3 — will not compile without them
    - _Requirements: 14.1–14.4_

  - [x]* 7.2 Write property test for serialization round-trip
    - **Property 7: Serialization round-trip**
    - Generate random instances of each serializable type using proptest `Arbitrary` implementations and verify encode then decode produces a value equal to the original
    - **Validates: Requirements 14.3**

- [x] 8. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 9. Implement token-bucket rate limiter
  - [x] 9.1 Implement TokenBucketRateLimiter
    - Write `src/dsql/rate_limiter.rs` with `TokenBucketRateLimiter` using atomic operations for lock-free token management
    - Implement `new(rate_per_second, capacity)`, `acquire()` (async wait), `try_acquire()` (non-blocking), `reconfigure(rate, capacity)`
    - Default to full cluster budget (100/sec sustained, 1,000 burst) for single-node mode
    - Use fixed-point representation for fractional tokens stored in AtomicU64
    - Store time as elapsed nanoseconds from a base `Instant` captured at construction (not raw `Instant` in atomic — `Instant` has no stable epoch)
    - _Requirements: 9.1–9.5_

  - [x]* 9.2 Write property test for rate limiter token bucket invariant
    - **Property 10: Rate limiter token bucket invariant**
    - Generate random sequences of acquire calls with varying timing and verify the actual rate never exceeds the configured sustained rate and burst never exceeds capacity
    - **Validates: Requirements 9.1, 9.2**

  - [x]* 9.3 Write unit tests for rate limiter
    - Test that try_acquire succeeds up to capacity and then fails
    - Test that reconfigure changes rate and capacity at runtime
    - Test token refill over time
    - _Requirements: 9.1–9.5_

- [x] 10. Implement reservoir and connection director
  - [x] 10.1 Implement Reservoir with refiller and expiry scanner
    - Write `src/dsql/reservoir.rs` with `Reservoir` using `async_channel` for the multi-consumer ready pool and `tokio::sync::mpsc::UnboundedSender/UnboundedReceiver` for the permit return path
    - Implement `start()` spawning background refiller, expiry scanner, and return-processor tasks
    - Refiller continuously creates connections when ready count < target, respecting rate limiter and in-flight semaphore
    - Expiry scanner runs on configurable interval, retiring connections within guard window of their max lifetime
    - Implement `checkout()` against the ready pool; returned permits are sent through the unbounded return channel from `DsqlPermit::Drop`
    - Implement the return processor to validate returned connections for lifetime and health, requeue valid connections to the ready pool, and discard expired or broken connections
    - Assign per-connection lifetime with jitter drawn from uniform distribution over `[base_lifetime, base_lifetime + lifetime_jitter]`
    - _Requirements: 8.2–8.7, 11.1–11.5_

  - [x]* 10.2 Write property test for connection lifetime jitter range
    - **Property 8: Connection lifetime jitter range**
    - Generate random reservoir configs and verify assigned lifetimes fall within `[base_lifetime, base_lifetime + lifetime_jitter]` and never exceed `DSQL_HARD_CUTOFF - guard_window`
    - **Validates: Requirements 8.6, 11.4**

  - [x] 10.3 Implement ClassBudgets with per-class semaphores
    - Write `ClassBudgets` in `src/dsql/connection.rs` with `HashMap<DbClass, Arc<Semaphore>>`
    - Implement `new(allocations)`, `acquire(class)`, `reconfigure(allocations)`
    - Verify allocations sum to total budget on construction and reconfiguration
    - _Requirements: 10.1–10.5_

  - [x] 10.4 Implement DsqlConnectionDirector
    - Write `DsqlConnectionDirector` in `src/dsql/connection.rs` implementing `ConnectionDirector` with `type Permit = DsqlPermit`
    - Wire together: ClassBudgets for per-class permits, Reservoir for connection checkout, TokenBucketRateLimiter for creation throttling
    - Implement `DsqlPermit` with `connection()` accessor for SQLx queries and Drop returning connection to reservoir or discarding if expired/broken
    - _Requirements: 8.1–8.7, 10.1–10.5, 11.1–11.5_

  - [x]* 10.5 Write unit tests for reservoir and connection director
    - Test reservoir checkout and return flow
    - Test expired connection discard on return
    - Test class budget acquire and release
    - Test class budget reconfiguration
    - _Requirements: 8.2–8.7, 10.1–10.5_

- [x] 11. Implement connection pool metrics
  - [x] 11.1 Register all connection pool metrics
    - Add metrics to `src/metrics.rs` using the `metrics` crate: `tokeira_dsql_pool_connections_total` (gauge), `tokeira_dsql_pool_checkout_duration_seconds` (histogram), `tokeira_dsql_pool_empty_reservoir_total` (counter), `tokeira_dsql_pool_connections_created_total` (counter), `tokeira_dsql_pool_connections_retired_total` (counter with reason label), `tokeira_dsql_pool_connections_returned_total` (counter), `tokeira_dsql_pool_rate_limiter_tokens` (gauge), `tokeira_dsql_pool_rate_limiter_rate` (gauge), `tokeira_dsql_pool_class_budget_total` (gauge), `tokeira_dsql_pool_class_in_use` (gauge), `tokeira_dsql_pool_class_waiters` (gauge)
    - Wire metric emissions into DsqlConnectionDirector, Reservoir, and TokenBucketRateLimiter
    - _Requirements: 13.1–13.6_

  - [x]* 11.2 Write unit tests for metrics emission
    - Use `metrics_util::DebuggingRecorder` to verify all metrics are emitted with correct names and labels
    - _Requirements: 13.1–13.6_

- [x] 12. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 13. Implement IAM authentication integration
  - [x] 13.1 Implement IAM connector configuration and region auto-detection
    - Write IAM integration in `src/dsql/connection.rs` (or a dedicated auth helper)
    - Configure `aurora-dsql-sqlx-connector` with endpoint, region (auto-detected from hostname if not specified), and optional role ARNs
    - Support three role configurations: admin (migration), runtime (commit/read/control), readonly (projection/visibility)
    - When all role ARNs are None, use default AWS credential chain (local/compose mode)
    - _Requirements: 12.1–12.5_

  - [x]* 13.2 Write unit tests for IAM configuration
    - Test region auto-detection from endpoint hostname (e.g., `*.dsql.us-east-1.on.aws` → `us-east-1`)
    - Test that None role ARNs fall back to default credential chain
    - Test that explicit role ARNs are passed to the connector
    - _Requirements: 12.2, 12.4_

- [x] 14. Wire DsqlStore and integrate all components
  - [x] 14.1 Implement DsqlStore struct in dsql/mod.rs
    - Create `DsqlStore` struct holding `DsqlConnectionDirector`, `MigrationRunner`, and codec references
    - Implement constructor that builds the full stack: config → auth → connector → rate limiter → reservoir → class budgets → director
    - Expose `migration_runner()` for schema management and `connection_director()` for runtime use
    - _Requirements: 8.1, 12.1_

  - [x]* 14.2 Write unit tests for DsqlStore construction
    - Test that DsqlStore can be constructed with default config
    - Test that invalid config is rejected at construction time
    - _Requirements: 8.1_

- [x] 15. Final checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation after each major phase
- Property tests validate universal correctness properties from the design document (Properties 1, 2, 4, 5, 7, 8, 10, 11)
- Properties 3, 6, 9, 12 require a live DSQL cluster and are covered by integration tests gated behind the `dsql-integration` feature flag — not included as tasks here
- Unit tests validate specific examples and edge cases
- All code targets Rust edition 2024, toolchain 1.95, using proptest for property-based testing
- Integration tests (migration apply, reservoir against real DSQL, IAM auth) are deferred to manual execution with `--features dsql-integration`
