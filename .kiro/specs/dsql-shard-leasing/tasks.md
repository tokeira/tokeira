# Implementation Plan: DSQL Shard Lease Management

## Overview

Implement `LeaseRepository` for `DsqlRunRepository` with `try_acquire_bundle` and `renew_bundle` against the existing `shard_lease` table. The implementation adds `lease_duration` to config and the repository struct, extracts lease decision logic into pure helper functions for property testing, and wires the SQL transaction paths for acquire, takeover, re-acquire, renewal, and rejection.

## Tasks

- [ ] 1. Add `lease_duration` to `DsqlPoolConfig` and validate
  - [ ] 1.1 Add `lease_duration` field to `DsqlPoolConfig` with 30-second default
    - Add `fn default_lease_duration() -> time::Duration` returning `time::Duration::seconds(30)`
    - Add `#[serde(default = "default_lease_duration")] pub lease_duration: time::Duration` to `DsqlPoolConfig`
    - Update `DsqlPoolConfig::default()` to include `lease_duration: default_lease_duration()`
    - Add validation in `DsqlPoolConfig::validate()`: `if self.lease_duration <= time::Duration::ZERO { bail!("lease_duration must be positive"); }`
    - _Requirements: 9.1, 9.2, 9.3_

  - [ ] 1.2 Write unit tests for `lease_duration` config validation
    - Test that default config still validates (`defaults_validate` already exists — ensure it still passes with the new field)
    - Test that `lease_duration` of zero is rejected
    - Test that negative `lease_duration` is rejected
    - Test that a positive `lease_duration` validates successfully
    - Test that `DsqlPoolConfig` serde round-trip preserves `lease_duration`
    - _Requirements: 9.2, 9.3_

- [ ] 2. Add `lease_duration` to `DsqlRunRepository` and update constructors
  - [ ] 2.1 Add `lease_duration` field to `DsqlRunRepository` struct and update `new` / `new_with_acquirer`
    - Add `lease_duration: time::Duration` field to `DsqlRunRepository`
    - Update `DsqlRunRepository::new` signature to accept `lease_duration: time::Duration`
    - Update `DsqlRunRepository::new_with_acquirer` signature to accept `lease_duration: time::Duration`
    - Store `lease_duration` in `Self` in both constructors
    - _Requirements: 9.2_

  - [ ] 2.2 Update `DsqlStore::from_connector` to pass `config.lease_duration`
    - Pass `config.lease_duration` as the fourth argument to `DsqlRunRepository::new`
    - _Requirements: 9.2_

  - [ ] 2.3 Update existing test helpers that call `new_with_acquirer`
    - Update `test_repo` helper in `run_repository.rs` tests to pass a default `lease_duration` (e.g., `time::Duration::seconds(30)`)
    - Ensure all existing tests still compile and pass
    - _Requirements: 9.2_

- [ ] 3. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 4. Extract pure lease decision helpers
  - [ ] 4.1 Implement `epoch_to_sql` and `epoch_from_sql` checked conversion helpers
    - Add `fn epoch_to_sql(epoch: ShardEpoch) -> Result<i64>` using `i64_from_u64(epoch.0, "shard_lease.epoch")` — reuses the existing checked helper, rejects `u64 > i64::MAX`
    - Add `fn epoch_from_sql(value: i64) -> Result<ShardEpoch>` using `Ok(ShardEpoch(u64_from_i64(value, "shard_lease.epoch")?))` — rejects negative BIGINT values
    - No unchecked `as` casts — matches the existing conversion style in `commit_transition`
    - _Requirements: 10.1, 10.2_

  - [ ] 4.2 Implement `interpret_acquire` pure helper function
    - Create a pure function that interprets the two-statement SQL outcome
    - Signature: `fn interpret_acquire(insert_rows_affected: u64, update_rows_affected: u64, acquired_epoch: Option<i64>, rejected_row: Option<(String, i64)>) -> Result<LeaseOutcome>`
    - When `insert_rows_affected == 1`: return `Acquired { epoch: ShardEpoch(1) }`
    - When `update_rows_affected == 1` and `acquired_epoch` is `Some(e)`: return `Acquired { epoch: epoch_from_sql(e)? }`
    - When both are 0 and `rejected_row` is `Some((owner, epoch))`: return `Rejected { current_owner, current_epoch: epoch_from_sql(epoch)? }`
    - When both are 0 and `rejected_row` is `None`: return error (unexpected state — row should exist after INSERT ON CONFLICT DO NOTHING)
    - Reject negative epoch values via `epoch_from_sql`
    - _Requirements: 1.1, 2.1, 2.2, 3.1, 3.2_

  - [ ] 4.3 Implement `decide_renew` pure helper function
    - Create a pure function that encodes the renewal decision logic
    - Input: optional existing row `(owner: &str, epoch: i64)`, caller owner, caller epoch
    - Output: an enum representing the decision — `Renew` or `Reject { current_owner: String, current_epoch: ShardEpoch }` (with empty/ZERO for absent row)
    - Logic: no row → `Reject { owner: "", epoch: ZERO }`; owner+epoch match → `Renew`; mismatch → `Reject`
    - _Requirements: 5.1, 6.1, 6.2, 7.1_

- [ ] 5. Implement `LeaseRepository for DsqlRunRepository`
  - [ ] 5.1 Implement `try_acquire_bundle`
    - Add `#[instrument(name = "dsql.try_acquire_bundle", skip(self), fields(shard_id = bundle.0, owner = %owner))]`
    - Acquire `DbClass::Control` permit via `self.director.acquire(DbClass::Control).await?`
    - Convert shard_id to UUID via `Self::shard_id_to_uuid(bundle)`
    - Capture `app_now = OffsetDateTime::now_utc()` once at the start — used for both expiry computation and comparison
    - Compute `new_expiry = app_now + self.lease_duration`
    - Begin transaction on the acquired connection
    - Execute `INSERT INTO shard_lease ... ON CONFLICT (shard_id) DO NOTHING` with `$1 = shard_uuid, $2 = owner, $3 = new_expiry`
    - Execute `UPDATE shard_lease SET owner = $2, epoch = CASE WHEN owner = $2 AND lease_expiry > $4 THEN epoch ELSE epoch + 1 END, lease_expiry = $3 WHERE shard_id = $1 AND (owner = $2 OR lease_expiry <= $4)` with `$4 = app_now` — no SQL `now()`
    - Check rows_affected from INSERT and UPDATE: if either affected 1 row, SELECT epoch back and call `interpret_acquire`; if neither affected rows, SELECT owner+epoch and call `interpret_acquire` with rejection data
    - Map SQLSTATE 40001 to `anyhow::Error` with context (no retry)
    - _Requirements: 1.1, 1.2, 1.3, 2.1, 2.2, 3.1, 3.2, 4.1, 4.2, 9.1, 11.1, 12.1, 13.1, 13.3_

  - [ ] 5.2 Implement `renew_bundle`
    - Add `#[instrument(name = "dsql.renew_bundle", skip(self), fields(shard_id = bundle.0, owner = %owner, epoch = epoch.0))]`
    - Acquire `DbClass::Control` permit via `self.director.acquire(DbClass::Control).await?`
    - Convert shard_id to UUID via `Self::shard_id_to_uuid(bundle)`
    - Begin transaction on the acquired connection
    - Execute `SELECT owner, epoch FROM shard_lease WHERE shard_id = $1 FOR UPDATE`
    - Call `decide_renew` with the row (or None) and caller arguments
    - On `Renew`: execute `UPDATE shard_lease SET lease_expiry = $1 WHERE shard_id = $2` with expiry = `OffsetDateTime::now_utc() + self.lease_duration`, commit, return `Renewed { epoch }`
    - On `Reject`: rollback, return `Rejected { current_owner, current_epoch }`
    - Map SQLSTATE 40001 to `anyhow::Error` with context (no retry)
    - _Requirements: 5.1, 5.2, 5.3, 6.1, 6.2, 7.1, 8.1, 9.1, 11.2, 12.1, 13.2, 13.3_

- [ ] 6. Checkpoint — Ensure compilation and existing tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 7. Property-based tests for lease decision logic
  - [ ] 7.1 Write property test for epoch monotonicity on acquire (Property 1)
    - **Property 1: Epoch Monotonicity on Acquire**
    - Use `proptest` to generate random `interpret_acquire` inputs: `insert_rows_affected` (0 or 1), `update_rows_affected` (0 or 1), `acquired_epoch` (Option<i64>), `rejected_row` (Option<(String, i64)>)
    - For valid acquired outcomes: verify epoch is 1 (insert) or matches the read-back epoch (update)
    - For valid rejected outcomes: verify the returned owner and epoch match the rejection row
    - For invalid combinations (both counts 1, both 0 with no rejection row): verify error is returned
    - Test negative epoch values are rejected by `epoch_from_sql`
    - **Validates: Requirements 1.1, 2.1, 2.2, 3.2**

  - [ ] 7.2 Write property test for active lease single-writer rejection (Property 2)
    - **Property 2: Active Lease Single-Writer Rejection**
    - Use `proptest` to generate random owner pairs (A ≠ B), epochs, and non-expired expiry times
    - Verify that `interpret_acquire` with `insert_rows_affected = 0`, `update_rows_affected = 0`, and a rejection row `(owner_A, epoch)` returns `Rejected` with the correct values
    - **Validates: Requirements 3.1**

  - [ ] 7.3 Write property test for renewal fidelity (Property 3)
    - **Property 3: Renewal Fidelity**
    - Use `proptest` to generate random owner strings, epochs, and existing-row states (present or absent)
    - Verify `decide_renew` returns `Renew` if and only if both owner and epoch match
    - Verify `decide_renew` returns `Reject` with correct values on any mismatch
    - Verify absent row returns `Reject { owner: "", epoch: ZERO }`
    - **Validates: Requirements 5.1, 6.1, 6.2, 7.1**

  - [ ] 7.4 Write property test for ShardEpoch round-trip (Property 4)
    - **Property 4: ShardEpoch Round-Trip**
    - Use `proptest` to generate `u64` values in range `1..=i64::MAX as u64`
    - Verify `epoch_from_sql(epoch_to_sql(ShardEpoch(v))?)? == ShardEpoch(v)` for all generated values
    - Verify `epoch_to_sql(ShardEpoch(i64::MAX as u64 + 1))` returns an error (overflow)
    - Verify `epoch_from_sql(-1)` returns an error (negative)
    - **Validates: Requirements 10.2**

- [ ] 8. Unit tests for specific behaviors
  - [ ] 8.1 Write unit test for `DbClass::Control` routing
    - Use `RecordingAcquirer` to verify `try_acquire_bundle` acquires `DbClass::Control`
    - Use `RecordingAcquirer` to verify `renew_bundle` acquires `DbClass::Control`
    - _Requirements: 1.2, 5.2_

  - [ ] 8.2 Write unit test for absent-lease renewal rejection
    - Verify `decide_renew(None, ...)` returns `Rejected { current_owner: "", current_epoch: ShardEpoch::ZERO }`
    - _Requirements: 7.1_

  - [ ] 8.3 Write unit test for `shard_id_to_uuid` determinism in lease binding
    - Verify `shard_id_to_uuid(ShardId(N))` produces a consistent UUID for the same input
    - Verify different shard IDs produce different UUIDs
    - _Requirements: 1.3, 5.3_

  - [ ] 8.4 Write unit test for OCC error classification
    - Verify `is_serialization_failure` returns `true` for SQLSTATE 40001 errors
    - Verify `is_serialization_failure` returns `false` for other database errors
    - _Requirements: 4.2, 8.1_

- [ ] 9. Integration tests (gated behind `dsql-integration` feature)
  - [ ] 9.1 Integration test: acquire → renew → expire → takeover cycle
    - Acquire a lease, renew it, wait for expiry (or use a short lease_duration), acquire from a different owner
    - Verify epoch increments correctly at each step
    - _Requirements: 1.1, 2.1, 5.1_

  - [ ] 9.2 Integration test: concurrent acquire — exactly one succeeds
    - Two tasks attempt `try_acquire_bundle` for the same shard simultaneously
    - Verify exactly one gets `Acquired`; the other gets either an OCC error (SQLSTATE 40001) or `LeaseOutcome::Rejected` (the ON CONFLICT WHERE clause rejected the update for an active different-owner lease)
    - Verify the durable `shard_lease` row has the winning owner and epoch = 1
    - _Requirements: 4.1, 4.2_

  - [ ] 9.3 Integration test: epoch fence integration with expired takeover
    - Acquire a lease as owner A, let it expire (use short lease_duration)
    - Acquire as owner B (expired takeover — epoch increments)
    - `commit_transition` with owner A's old epoch → should get `CommitResult::Conflict`
    - `commit_transition` with owner B's new epoch → should pass
    - _Requirements: 1.1, 2.1_

  - [ ] 9.4 Integration test: INSERT ON CONFLICT race on first acquire
    - Two tasks attempt first-time `try_acquire_bundle` for a shard that has no existing row
    - Verify exactly one succeeds with epoch 1, the other either gets OCC error or `Rejected`
    - _Requirements: 4.1, 13.1_

  - [ ] 9.5 Integration test: active same-owner reacquire is idempotent
    - Acquire a lease as owner A (epoch 1)
    - Immediately re-acquire as owner A (active, non-expired)
    - Verify the returned epoch is still 1 (unchanged)
    - Renew with epoch 1 → should succeed (the old renewer's epoch is still valid)
    - _Requirements: 3.2_

- [ ] 10. Final checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- All tests are required — none are marked optional per project convention.
- Property tests target the pure `interpret_acquire` and `decide_renew` helpers, keeping them fast and deterministic without a live DSQL cluster.
- The `RecordingAcquirer` mock (already exists in `run_repository.rs` tests) is reused for `DbClass::Control` routing verification.
- Each task references specific requirements for traceability.
- Checkpoints ensure incremental validation.
- No schema changes are needed — the `shard_lease` table already exists from Feature 1.
