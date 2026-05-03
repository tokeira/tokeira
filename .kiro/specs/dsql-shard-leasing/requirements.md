# Requirements Document: DSQL Shard Lease Management

## Introduction

This document captures the requirements for implementing the `LeaseRepository` trait methods (`try_acquire_bundle`, `renew_bundle`) against the DSQL `shard_lease` table. This is Feature 4 from the umbrella `dsql-storage-implementation` spec.

Shard leasing is the mechanism by which Tokeira nodes claim exclusive ownership of shard bundles. Each shard has exactly one owner at any given epoch. The lease is epoch-fenced: every successful acquisition increments the epoch, and every `commit_transition` validates the caller's epoch against the durable value. This prevents stale owners from writing after failover.

The `LeaseRepository` trait is defined in `tokeira-storage/src/api.rs`. The in-memory implementation in `memory.rs` is the behavioral reference for basic semantics, but the DSQL implementation adds expiry-based takeover — a capability the in-memory store omits because it has no concept of wall-clock lease expiry.

The `shard_lease` table was created by Feature 1 (`dsql-schema-connection`). The epoch fence read inside `commit_transition` was implemented by Feature 2 (`dsql-core-persistence`). This spec provides the write path that maintains the epoch and expiry values those features depend on.

### Scope

- Implement `LeaseRepository` for `DsqlRunRepository` (or a dedicated `DsqlLeaseRepository`)
- `try_acquire_bundle`: insert new lease or takeover expired lease, reject active lease held by another owner
- `renew_bundle`: extend expiry for current owner at matching epoch, reject stale callers
- OCC conflict surfacing (no silent retry)
- `tracing::instrument` on all methods
- `DbClass::Control` for all lease operations

### Out of Scope

- Core persistence methods (Feature 2)
- Side-table queries (Feature 3)
- Dispatch backlog (Feature 5)
- Projection persistence (Feature 6)
- Placement controller logic (runtime concern)
- Lease relinquish, bulk observation, generation-aware placement (future TODO in trait)

## Glossary

- **LeaseRepository**: The storage trait in `tokeira-storage/src/api.rs` defining `try_acquire_bundle` and `renew_bundle` for shard lease management with epoch-fenced acquire and renew operations.
- **LeaseOutcome**: Enum returned by lease operations — `Acquired { epoch }`, `Renewed { epoch }`, or `Rejected { current_owner, current_epoch }`.
- **ShardId**: A `u32` identifying a shard bundle. Stored as UUID in DSQL via `shard_id_to_uuid` (BLAKE3-based spread UUID).
- **ShardEpoch**: A `u64` monotonically increasing epoch number for shard ownership fencing. `ShardEpoch::ZERO` (0) represents the state before any lease has been acquired.
- **shard_lease**: DSQL table with columns `shard_id UUID PK`, `owner TEXT`, `epoch BIGINT`, `lease_expiry TIMESTAMPTZ`. Created by Feature 1.
- **DsqlRunRepository**: The production DSQL storage backend in `tokeira-storage/src/dsql/run_repository.rs` that implements `RunRepository` and will also implement `LeaseRepository`.
- **DbClass::Control**: The highest-priority connection class, used for shard lease and cluster-control operations.
- **OCC**: Optimistic Concurrency Control — DSQL's conflict detection model where transactions proceed optimistically and conflicts are detected at commit time (SQLSTATE 40001).
- **Lease_Expiry**: A wall-clock `TIMESTAMPTZ` indicating when a lease becomes stale. The runtime uses this to detect abandoned leases and attempt takeover.
- **Lease_Duration**: A configurable duration added to the current time to compute `lease_expiry` when acquiring or renewing a lease. Not part of the trait signature — computed internally by the implementation.
- **Epoch_Fence**: The mechanism by which `commit_transition` (Feature 2) reads `shard_lease.epoch` within the commit transaction to prevent stale owners from writing.
- **shard_id_to_uuid**: A method on `DsqlRunRepository` that deterministically converts `ShardId(u32)` to a UUID using `dsql_spread_uuid` for SQL binding.

## Requirements

---

### Requirement 1: Lease Acquisition — No Existing Lease

**User Story:** As a Tokeira runtime node, I want to acquire ownership of an unleased shard, so that I can begin processing workflow transitions for that shard.

#### Acceptance Criteria

1. WHEN `try_acquire_bundle` is called and no `shard_lease` row exists for the given shard, THE DsqlLeaseRepository SHALL insert a new row with epoch 1, the caller's owner identity, and a computed lease_expiry, and return `LeaseOutcome::Acquired { epoch: ShardEpoch(1) }`.
2. THE DsqlLeaseRepository SHALL use `DbClass::Control` when acquiring a connection for the lease operation.
3. THE DsqlLeaseRepository SHALL bind the shard_id as UUID via `shard_id_to_uuid`.

### Requirement 2: Lease Acquisition — Expired Lease Takeover

**User Story:** As a Tokeira runtime node, I want to take over a shard whose lease has expired, so that the system recovers from node failures without manual intervention.

#### Acceptance Criteria

1. WHEN `try_acquire_bundle` is called and a `shard_lease` row exists with `lease_expiry <= now()`, THE DsqlLeaseRepository SHALL update the row with the new owner, epoch incremented by 1, and a new lease_expiry, and return `LeaseOutcome::Acquired` with the new epoch.
2. WHEN `try_acquire_bundle` is called by the same owner that already holds the lease and the lease has expired, THE DsqlLeaseRepository SHALL treat this as a takeover — incrementing the epoch and updating the expiry — and return `LeaseOutcome::Acquired` with the new epoch. This differs from active same-owner re-acquire (Requirement 3.2) because the expired epoch may have been used by another node during the expiry window.

### Requirement 3: Lease Acquisition — Active Lease Rejection

**User Story:** As a Tokeira runtime node, I want lease acquisition to be rejected when another node holds an active lease, so that single-writer guarantees are maintained.

#### Acceptance Criteria

1. WHEN `try_acquire_bundle` is called and a `shard_lease` row exists with `lease_expiry > now()` and the owner differs from the caller, THE DsqlLeaseRepository SHALL return `LeaseOutcome::Rejected` with the current owner and current epoch.
2. WHEN `try_acquire_bundle` is called by the same owner that already holds an active (non-expired) lease, THE DsqlLeaseRepository SHALL refresh the expiry without incrementing the epoch and return `LeaseOutcome::Acquired` with the existing epoch. This makes same-owner re-acquire idempotent — the runtime's existing renewer continues working with the same epoch, avoiding the stale-renewer self-fencing problem.

### Requirement 4: Lease Acquisition — OCC Conflict Handling

**User Story:** As a Tokeira developer, I want concurrent lease acquisition attempts to be naturally serialized by DSQL's OCC, so that exactly one node succeeds per epoch transition.

#### Acceptance Criteria

1. WHEN two nodes attempt `try_acquire_bundle` for the same shard concurrently, THE DsqlLeaseRepository SHALL rely on DSQL's OCC to ensure only one transaction commits successfully.
2. WHEN a DSQL OCC conflict is detected (SQLSTATE 40001) during `try_acquire_bundle`, THE DsqlLeaseRepository SHALL surface the error to the caller without silent retry.

### Requirement 5: Lease Renewal — Matching Owner and Epoch

**User Story:** As a Tokeira runtime node, I want to renew my shard lease by extending its expiry, so that I maintain ownership without re-acquisition while actively processing the shard.

#### Acceptance Criteria

1. WHEN `renew_bundle` is called with an owner and epoch that match the durable `shard_lease` row, THE DsqlLeaseRepository SHALL update the `lease_expiry` to a new computed value and return `LeaseOutcome::Renewed { epoch }`.
2. THE DsqlLeaseRepository SHALL use `DbClass::Control` when acquiring a connection for the renewal operation.
3. THE DsqlLeaseRepository SHALL bind the shard_id as UUID via `shard_id_to_uuid`.

### Requirement 6: Lease Renewal — Stale Epoch Rejection

**User Story:** As a Tokeira developer, I want lease renewal to be rejected when the caller's epoch is stale, so that a node that lost ownership cannot silently extend a lease it no longer holds.

#### Acceptance Criteria

1. WHEN `renew_bundle` is called with an epoch that does not match the durable epoch in `shard_lease`, THE DsqlLeaseRepository SHALL return `LeaseOutcome::Rejected` with the current owner and current epoch.
2. WHEN `renew_bundle` is called with an owner that does not match the durable owner in `shard_lease`, THE DsqlLeaseRepository SHALL return `LeaseOutcome::Rejected` with the current owner and current epoch.

### Requirement 7: Lease Renewal — No Existing Lease

**User Story:** As a Tokeira developer, I want lease renewal to be rejected when no lease exists, so that the runtime detects the anomalous state and can re-acquire.

#### Acceptance Criteria

1. WHEN `renew_bundle` is called and no `shard_lease` row exists for the given shard, THE DsqlLeaseRepository SHALL return `LeaseOutcome::Rejected` with an empty owner string and `ShardEpoch::ZERO`.

### Requirement 8: Lease Renewal — OCC Conflict Handling

**User Story:** As a Tokeira developer, I want concurrent renewal attempts to be naturally serialized by DSQL's OCC, so that conflicting renewals are detected at commit time.

#### Acceptance Criteria

1. WHEN a DSQL OCC conflict is detected (SQLSTATE 40001) during `renew_bundle`, THE DsqlLeaseRepository SHALL surface the error to the caller without silent retry.

### Requirement 9: Lease Expiry Computation

**User Story:** As a Tokeira operator, I want lease expiry to be computed from a configurable duration, so that I can tune the lease lifetime to match my deployment's failure detection characteristics.

#### Acceptance Criteria

1. THE DsqlLeaseRepository SHALL compute `lease_expiry` as `now() + lease_duration` when acquiring or renewing a lease.
2. THE DsqlLeaseRepository SHALL accept a configurable lease duration at construction time.
3. THE lease duration SHALL have a sensible default (e.g., 30 seconds) suitable for production deployments where the renewal interval is shorter than the lease duration.

### Requirement 10: Epoch Storage and Type Mapping

**User Story:** As a Tokeira developer, I want the epoch stored as BIGINT to faithfully represent the `ShardEpoch(u64)` domain type, so that epoch fencing is correct across the Rust/SQL boundary.

#### Acceptance Criteria

1. THE DsqlLeaseRepository SHALL store `ShardEpoch` as BIGINT (i64) in the `shard_lease.epoch` column.
2. THE DsqlLeaseRepository SHALL convert between `ShardEpoch(u64)` and SQL BIGINT (i64) using a consistent, lossless mapping for all epoch values that occur in practice (epochs start at 1 and increment by 1, so overflow is not a practical concern).

### Requirement 11: Observability

**User Story:** As a Tokeira operator, I want lease operations to be instrumented with tracing spans, so that I can diagnose lease acquisition failures and renewal latency in production.

#### Acceptance Criteria

1. THE DsqlLeaseRepository SHALL annotate `try_acquire_bundle` with `tracing::instrument`, including the shard_id and owner in the span fields.
2. THE DsqlLeaseRepository SHALL annotate `renew_bundle` with `tracing::instrument`, including the shard_id, owner, and epoch in the span fields.

### Requirement 12: Feature Gating

**User Story:** As a Tokeira developer, I want all DSQL lease code gated behind the `dsql` feature flag, so that builds without DSQL support are not affected.

#### Acceptance Criteria

1. THE DsqlLeaseRepository implementation SHALL be gated behind `#[cfg(feature = "dsql")]`.
2. THE DsqlLeaseRepository SHALL compile and pass clippy when the `dsql` feature is enabled.

### Requirement 13: Transaction Isolation for Lease Operations

**User Story:** As a Tokeira developer, I want lease operations to use transactions with FOR UPDATE on the shard_lease primary key, so that DSQL's OCC provides correct serialization of concurrent lease mutations.

#### Acceptance Criteria

1. WHEN `try_acquire_bundle` acquires a lease, THE DsqlLeaseRepository SHALL use `INSERT ... ON CONFLICT (shard_id) DO NOTHING` for the new-lease path and SHALL run the conditional `UPDATE ... WHERE shard_id = $1 AND (owner = $2 OR lease_expiry <= $4)` only when the insert affects zero rows. The UPDATE uses `CASE WHEN owner = $2 AND lease_expiry > $4 THEN epoch ELSE epoch + 1 END` to preserve the epoch for active same-owner re-acquire and increment it for all takeover paths. This two-statement transactional approach eliminates the no-row race and is validated against live DSQL.
2. WHEN `renew_bundle` reads the `shard_lease` row, THE DsqlLeaseRepository SHALL use `SELECT ... FOR UPDATE` with an equality predicate on the `shard_id` primary key to lock the row within the transaction.
3. THE lease read and subsequent write SHALL occur within the same DSQL transaction to prevent TOCTOU races.
