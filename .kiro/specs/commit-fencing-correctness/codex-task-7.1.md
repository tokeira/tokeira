# Codex Task: Split `crates/tokeira-runtime/src/runtime.rs` into Sub-Modules

## Objective

Split `crates/tokeira-runtime/src/runtime.rs` (4081 lines) into focused sub-modules along correctness boundaries. Unlike the storage split (which used trait delegation), this is simpler: `TokeiraRuntime<R>` has no trait to implement — all methods are inherent. We move method groups directly into sub-module files as `impl<R> TokeiraRuntime<R> where R: RunRepository + 'static { ... }` blocks.

## Current File Layout

```
crates/tokeira-runtime/src/runtime.rs   — 4081 lines (single file)
```

`lib.rs` declares `pub mod runtime;` — when we create `runtime/mod.rs`, Rust picks up the directory form automatically.

## Target File Layout

```
crates/tokeira-runtime/src/runtime/
├── mod.rs              — struct, constructors, accessors, shared helpers, public types, tests
├── workflow_task.rs    — WFT poll/claim/complete, start_polled_workflow_task, started_workflow_task_from_state
├── activity.rs         — activity start/complete/fail/retry/heartbeat, validate_activity_token
├── query.rs            — query_workflow, update_workflow (buffered query resolution)
├── commit.rs           — submit, submit_for_owned_shard, handle_post_commit, current_shard_epoch, shard_epoch_for_completion, shard_id_for
├── lifecycle.rs        — start_workflow, start_workflow_with_policy, signal_with_start_workflow, signal_workflow, terminate_workflow, cancel_workflow, reset_workflow, resolve_conflict, terminate_existing_for_conflict
└── membership.rs       — acquire_shard, relinquish_shard, spawn_membership_client, record_self_assigned_shard
```

## Key Difference from Storage Split

- **No trait delegation needed.** `TokeiraRuntime<R>` is a concrete generic struct with only inherent methods. Each sub-module simply defines its own `impl<R> TokeiraRuntime<R> where R: RunRepository + 'static { ... }` block containing the moved methods.
- **Methods stay `pub` or private as-is.** No renaming, no `do_` prefix. The methods move verbatim.
- **The generic bound is `R: RunRepository + 'static`** for the main impl block (lines 259–2361). There's a second smaller impl block (lines 2362–2381) with `R: RunRepository + LeaseRepository + 'static` for `spawn_membership_client` — this goes in `membership.rs`.

## What Stays in `mod.rs`

### Struct + Types (keep verbatim):
- Lines 1–85: Module doc, all `use` imports
- Lines 86–167: `pub struct TokeiraRuntime<R>` (all fields)
- Lines 168–240: `ResetWorkflowResult`, `MutationMetadata`, `StartWorkflowResult`, `SignalWithStartResult`, `RuntimeConfig`, `ConflictResolution`, `BufferedQueryCleanup` (struct + Default + Drop impls)
- Lines 2383–2467: Free functions (`is_externally_routed_command`, `mutation_metadata`) + public types (`StartedWorkflowTask`, `StartedActivityTask`)
- Lines 2469–4081: `#[cfg(test)] mod tests` block

### Constructor + Accessors (keep in mod.rs):
- Lines 259–626: All `new*` constructors
- Lines 627–787: All accessor methods (`broker()`, `activity_broker()`, `repo()`, `delivery_metrics()`, etc. through `heartbeat_inputs()`)

### Shared Helpers (keep in mod.rs):
- `pick_lane` (line 1937)
- `lane_index` (line 1942)
- `validate_workflow_task_token` (line 1844)

### Shutdown Methods (keep in mod.rs):
- Lines 1948–2050: All `shutdown_*` methods (timer_scanner, workflow_timeout_scanner, wft_timeout_scanner, nexus_timeout_scanner, activity_timeout_scanner, grace_scanner, drain_loop, control_loop, heartbeat_maintenance)
- Lines 2052–2080: `republish_queue`, `republish_activity_queue`

### Module Declarations (add to mod.rs):
```rust
mod activity;
mod commit;
mod lifecycle;
mod membership;
mod query;
mod workflow_task;
```

## Sub-Module Extraction Plan

### `commit.rs` — The Fenced Commit Entry Point

This is the correctness-critical module. All durable mutations flow through here.

**Methods to move (lines 1753–1843):**
- `pub async fn submit(&self, run_key: RunKey, command: Command) -> Result<CommitResult>` (line 1753)
- `async fn submit_for_owned_shard(...)` (line 1780)
- `fn handle_post_commit(&self, run_key: RunKey, result: &CommitResult)` (line 1801)
- `async fn current_shard_epoch(&self, run_key: RunKey) -> Result<ShardEpoch>` (line 1819)
- `async fn shard_epoch_for_completion(&self, run_key: RunKey) -> Result<ShardEpoch>` (line 1831)
- `async fn shard_id_for(&self, run_key: RunKey) -> ShardId` (line 1839)

**Imports needed:** `RunKey`, `ShardEpoch`, `ShardId`, `Command`, `CommitResult`, `RunRepository`, `execution_home_bundle`, `shard_for`, `ShardOwner`, `NotShardOwner`, `runtime_metrics`, `is_externally_routed_command`.

**Note:** `submit_for_owned_shard` calls `self.pick_lane(run_key)` which stays in `mod.rs`. This is fine — inherent methods on the same type are accessible across `impl` blocks in different files within the same module tree.

### `workflow_task.rs` — WFT Poll/Claim/Complete

**Methods to move (lines 1399–1751):**
- `pub async fn poll_workflow_task(...)` (line 1399)
- `pub async fn try_claim_workflow_task(...)` (line 1431)
- `pub async fn complete_workflow_task(...)` (line 1461)
- `async fn start_polled_workflow_task(...)` (line 1639)
- `async fn started_workflow_task_from_state(...)` (line 1710)
- `pub async fn try_reserve_start_poller(...)` (line 1032) — only if it's exclusively WFT-related
- `pub async fn deliver_reserved_start_workflow_task(...)` (line 1043) — same

**Note:** `try_reserve_start_poller` and `deliver_reserved_start_workflow_task` (lines 1032–1059) are called from `start_workflow`. They could stay in `lifecycle.rs` or move to `workflow_task.rs`. Since they deal with WFT delivery mechanics, put them in `workflow_task.rs` and call them from `lifecycle.rs` via `self.try_reserve_start_poller(...)` (cross-file inherent method call works fine).

### `activity.rs` — Activity Start/Complete/Fail/Retry/Heartbeat

**Methods to move (lines 1473–1637 + 2082–2381):**
- `pub async fn poll_activity_task(...)` (line 1473)
- `pub async fn try_claim_activity_task(...)` (line 1498)
- `pub async fn complete_activity_task(...)` (line 1519)
- `pub async fn fail_activity_task(...)` (line 1551)
- `pub async fn record_activity_heartbeat(...)` (line 1596)
- `pub async fn resolve_nexus_operation(...)` (line 1609)
- `async fn start_activity_task(...)` (line 2082)
- `async fn validate_activity_token(...)` (line 2220)
- `async fn retry_activity_task(...)` (line 2242)

### `query.rs` — Query and Update Dispatch

**Methods to move (lines 789–985):**
- `pub async fn query_workflow(...)` (line 789)
- `pub async fn update_workflow(...)` (line 883)

### `lifecycle.rs` — Workflow Start/Signal/Terminate/Cancel/Reset

**Methods to move (lines 986–1398):**
- `pub async fn start_workflow(...)` (line 986)
- `pub async fn start_workflow_with_policy(...)` (line 1060)
- `pub async fn signal_with_start_workflow(...)` (line 1117)
- `pub async fn signal_workflow(...)` (line 1200)
- `pub async fn terminate_workflow(...)` (line 1214)
- `pub async fn cancel_workflow(...)` (line 1228)
- `pub async fn reset_workflow(...)` (line 1242)
- `async fn resolve_conflict(...)` (line 1273)
- `async fn terminate_existing_for_conflict(...)` (line 1353)

### `membership.rs` — Shard Ownership

**Methods to move (lines 1853–1935 + 2362–2381):**
- `pub async fn acquire_shard(...)` (line 1853)
- `pub async fn relinquish_shard(...)` (line 1927)
- `pub fn spawn_membership_client(...)` (line 2366) — note: this is in a SEPARATE impl block with `R: RunRepository + LeaseRepository + 'static`

## Rust Constraints

1. **Generic bounds must be repeated.** Each sub-module's `impl` block must carry the full bound:
   ```rust
   impl<R> TokeiraRuntime<R>
   where
       R: RunRepository + 'static,
   {
       // methods here
   }
   ```
   For `membership.rs`, the bound is `R: RunRepository + LeaseRepository + 'static`.

2. **Cross-file method calls work.** A method in `commit.rs` can call `self.pick_lane(run_key)` defined in `mod.rs` because they're all `impl` blocks on the same type within the same module tree. No `pub(super)` needed for methods that are already `pub` or accessible within the crate.

3. **Private methods stay private.** Methods like `submit_for_owned_shard`, `start_polled_workflow_task`, etc. that are `async fn` (no `pub`) remain accessible to other methods on the same type regardless of which file they're in — as long as they're within the same module (which they are, since all sub-modules are children of `runtime/`).

   **CORRECTION**: Actually, private methods in one file are NOT accessible from another file's `impl` block. In Rust, visibility within a module tree means `pub(super)` or `pub(crate)` is needed for cross-file access within the same module. Methods that are called from other sub-modules must be `pub(super)`.

   Specifically:
   - `pick_lane` (in mod.rs, called from commit.rs, lifecycle.rs) → must be `pub(super)`
   - `lane_index` (in mod.rs, called from commit.rs) → must be `pub(super)`
   - `validate_workflow_task_token` (in mod.rs, called from workflow_task.rs) → must be `pub(super)`
   - `handle_post_commit` (in commit.rs, called from lifecycle.rs) → must be `pub(super)`
   - `current_shard_epoch` (in commit.rs, called from activity.rs, lifecycle.rs) → must be `pub(super)`
   - `shard_epoch_for_completion` (in commit.rs, called from workflow_task.rs) → must be `pub(super)`
   - `shard_id_for` (in commit.rs, called from lifecycle.rs) → must be `pub(super)`
   - `submit_for_owned_shard` (in commit.rs, called from lifecycle.rs) → must be `pub(super)`
   - `try_reserve_start_poller` (in workflow_task.rs, called from lifecycle.rs) → must be `pub(super)`
   - `deliver_reserved_start_workflow_task` (in workflow_task.rs, called from lifecycle.rs) → must be `pub(super)`
   - `start_polled_workflow_task` (in workflow_task.rs, called from activity.rs?) → check callers
   - `validate_activity_token` (in activity.rs, only called within activity.rs) → stays private
   - `start_activity_task` (in activity.rs, only called within activity.rs) → stays private
   - `retry_activity_task` (in activity.rs, only called within activity.rs) → stays private

4. **Imports from parent module.** Sub-modules import the struct and helpers via:
   ```rust
   use super::{TokeiraRuntime, RuntimeConfig, MutationMetadata, ...};
   ```
   Or import from the crate root for sibling modules:
   ```rust
   use crate::shard::{ShardOwner, shard_for};
   use crate::broker::InMemoryBroker;
   ```

5. **Free functions.** `is_externally_routed_command` and `mutation_metadata` (lines 2383–2411) stay in `mod.rs` as module-level functions. Sub-modules access them via `use super::is_externally_routed_command;`.

6. **Public types.** `StartedWorkflowTask` and `StartedActivityTask` (lines 2413–2467) stay in `mod.rs` and are re-exported. Sub-modules that return these types import them via `use super::StartedWorkflowTask;`.

7. **The `#[cfg(test)] mod tests` block** stays in `mod.rs`. It tests integrated behavior and references internal state.

## File Creation Order

1. Rename `runtime.rs` → `runtime/mod.rs` (or create directory and move)
2. Create `runtime/commit.rs`
3. Create `runtime/workflow_task.rs`
4. Create `runtime/activity.rs`
5. Create `runtime/query.rs`
6. Create `runtime/lifecycle.rs`
7. Create `runtime/membership.rs`
8. Update `mod.rs`: remove extracted methods, add `mod` declarations, adjust visibility of shared helpers to `pub(super)`

## Verification

```bash
cargo check -p tokeira-runtime
cargo test -p tokeira-runtime --lib
cargo +nightly fmt --all --check
```

All existing tests must pass unchanged. No public API changes. No behavioral changes.

## Critical Visibility Rule

When you move a private method to a sub-module file and it's called from another sub-module file, you MUST change it to `pub(super)`. The compiler will tell you — if a method is not found, it's a visibility issue. Derive the correct visibility from compiler errors rather than guessing.

The safe default: make all methods that were previously private but are called from other methods (now in different files) `pub(super)`. Methods that are only called within the same file can stay private.
