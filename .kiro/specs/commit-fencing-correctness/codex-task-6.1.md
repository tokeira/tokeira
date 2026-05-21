# Codex Task: Split `run_repository/mod.rs` into Sub-Modules (Option C — Inherent Method Delegation)

## Objective

Split `crates/tokeira-storage/src/dsql/run_repository/mod.rs` (3210 lines) into focused sub-modules along correctness boundaries. The `impl RunRepository for DsqlRunRepository` trait block stays in `mod.rs` as a thin delegation layer. Each sub-module implements the actual logic as `pub(super)` inherent methods on `DsqlRunRepository`.

## Current File Layout

```
run_repository/
├── mod.rs      — 3210 lines (everything)
└── leases.rs   — already extracted (LeaseRepository + ControlRepository impl)
```

## Target File Layout

```
run_repository/
├── mod.rs          — struct, constructor, helpers, macros, thin trait delegation, tests
├── leases.rs       — (unchanged) LeaseRepository + ControlRepository impl
├── commit.rs       — commit_transition, commit_transition_for_bundle, write_transition, all mutation helpers
├── load.rs         — resolve_execution, find_latest_run, load_run, read_history, lookup_request_dedupe, read_transition_audit, materialize_reset_successor
├── dispatch.rs     — list_dispatchable_workflow_tasks[_for_shard], persist_to_backlog, drain_backlog, collect_dispatchable_workflow_tasks, sticky_fields
├── activity.rs     — list_dispatchable_activity_tasks[_for_shard], list_open_activities_for_shard, activity_dispatch_from_row, collect_activity_sweep_entries
├── timers.rs       — list_due_timers[_for_shard]
└── visibility.rs   — list_runs_with_workflow_timeouts_for_shard, list_started_workflow_tasks_for_shard, list_pending_nexus_operations_for_shard, collect_workflow_timeout_entries, collect_started_workflow_task_entries, collect_nexus_sweep_entries
```

## Architecture: Inherent Method Delegation

The `RunRepository` trait is monolithic (~20 methods). Rust does not allow splitting a single trait impl across files. The solution:

1. Each sub-module defines `impl DsqlRunRepository { pub(super) async fn do_<method_name>(...) }` with the full implementation
2. `mod.rs` keeps the `#[async_trait] impl RunRepository for DsqlRunRepository` block, but each method body is a one-liner delegation: `self.do_<method_name>(...).await`
3. Private helper functions (e.g., `write_transition`, `insert_workflow_hot`) move into the sub-module that calls them and become plain `async fn` (not `pub(super)`) within that sub-module's `impl DsqlRunRepository` block

## Rust Constraints to Respect

1. **Macro definition order**: `macro_rules!` definitions must appear BEFORE `mod` declarations that use them. The macros `record_dsql_operation!` and `record_dsql_commit_operation!` are already defined before `mod leases;` — keep all new `mod` declarations after the macro definitions.

2. **Macro import in sub-modules**: Sub-modules access macros via `use super::record_dsql_operation;` (proven pattern from `leases.rs`). However, `macro_rules!` macros are NOT items — they cannot be imported with `use`. They are available in sub-modules automatically because they are defined in the parent module. The `leases.rs` file does NOT have `use super::record_dsql_operation;` — it just uses the macro directly. Follow this pattern.

3. **`pub(super)` for cross-module helpers**: Functions used by multiple sub-modules (like `epoch_to_sql`, `should_check_epoch`, `partition_for`) stay in `mod.rs` and are `pub(super)` or module-level `fn`. Sub-modules access them via `use super::function_name;`.

4. **`pub(crate)` stays `pub(crate)`**: Functions like `epoch_to_sql`, `epoch_from_sql` that are used outside the `run_repository` module (e.g., by `leases.rs` which imports them as `use super::epoch_from_sql`) must remain `pub(crate)`.

5. **Single `impl` block per file for the same type is fine**: Rust allows multiple `impl DsqlRunRepository` blocks across different files in the same module tree.

6. **`#[instrument]` attributes**: Move with the method body into the sub-module. The trait delegation in `mod.rs` does NOT need `#[instrument]` — the span is created in the inherent method.

7. **The `record_dsql_operation!` macro captures `self`**: It calls `$repo.record_operation_result(...)`. The `record_operation_result` and `record_commit_operation_result` methods must remain accessible from sub-modules. They are inherent methods on `DsqlRunRepository` defined in `mod.rs`, so they're accessible via `self.` in any `impl DsqlRunRepository` block.

8. **Type aliases**: `type ActivityDispatchRow = (...)` at line 1418 — move to `activity.rs` since it's only used there.

## Detailed Extraction Plan

### What stays in `mod.rs` (lines to KEEP):

- Lines 1–76: Module doc, imports, constants, `effective_history_limit`, macro definitions
- Line 77: `mod leases;` declaration
- Lines 77–167: `DsqlRunRepository` struct, `new()`, `new_with_acquirer()`, `shard_for_run_key_with_count`, `shard_for_run_key`
- Lines 168–333: Utility methods (`shard_id_to_uuid`, `shard_id_from_uuid`, `current_execution_key`, `request_dedupe_key`, `dispatch_backlog_key`, `activity_dispatch_key`, `is_serialization_failure`, `record_operation_result`, `record_commit_operation_result`)
- Lines 334–379: Free functions (`classify_outcome`, `is_serialization_failure_error`, `extract_sqlstate`, `classify_connection_error`)
- Lines 2094–2138: Free functions (`partition_for`, `option_key_part`, `epoch_to_sql`, `epoch_from_sql`, `should_check_epoch`)
- Lines 2140–3210: `#[cfg(test)] mod tests` block

- **NEW**: `mod commit;`, `mod load;`, `mod dispatch;`, `mod activity;`, `mod timers;`, `mod visibility;` declarations (after `mod leases;`)
- **NEW**: Rewritten `impl RunRepository for DsqlRunRepository` block (thin delegation, ~80 lines)

### `commit.rs` — Extract from lines 596–783 (commit_transition) + 784–840 (commit_transition_for_bundle) + 1642–2092 (write_transition and all mutation helpers)

Methods to move as `pub(super)`:
- `do_commit_transition` (was `commit_transition`, lines 596–783)
- `do_commit_transition_for_bundle` (was `commit_transition_for_bundle`, lines 784–840)

Private helpers to move (stay private within `commit.rs`):
- `write_transition` (line 1642)
- `insert_workflow_hot` (line 1782)
- `insert_history_batch` (line 1820)
- `upsert_activity` (line 1855)
- `upsert_activity_dispatch_from_dispatch_op` (line 1887)
- `update_existing_activity_dispatch` (line 1933)
- `delete_activity_dispatch` (line 1973)
- `delete_activity_dispatch_for_run` (line 1986)
- `upsert_timer` (line 1997)
- `upsert_current_execution_start` (line 2022)
- `insert_projection_log` (line 2050)

Free functions to move:
- `partition_for` (line 2094) — used only by `insert_projection_log`
- `option_key_part` (line 2107) — used only by `upsert_current_execution_start`

Imports needed in `commit.rs`:
```rust
use anyhow::{Context, Result, anyhow};
use sqlx::Connection;
use time::OffsetDateTime;
use tokeira_kernel::{ActivityOp, DispatchOp, HistoryEvent, ProjectionOp, TimerOp, Transition, WorkflowState};
use tokeira_types::{
    BuildId, DeploymentId, ExecutionStatus, NamespaceId, Payloads, RunId, RunKey, ShardEpoch,
    ShardId, TaskKind, TaskQueueName, TransitionSeq, WorkflowId, dsql_spread_uuid,
};
use tracing::instrument;
use uuid::Uuid;

use crate::{
    CommitResult, CurrentExecutionConflictPolicy, DbClass, ProjectionContext, metrics,
};
use super::{DsqlRunRepository, should_check_epoch, epoch_to_sql, PROJECTION_FANOUT};
use crate::dsql::{codec, convert};
```

### `load.rs` — Extract from lines 384–595 + 842–956

Methods to move as `pub(super)`:
- `do_resolve_execution` (was `resolve_execution`, lines 384–414)
- `do_find_latest_run` (was `find_latest_run`, lines 415–436)
- `do_load_run` (was `load_run`, lines 437–455)
- `do_read_history` (was `read_history`, lines 456–509)
- `do_lookup_request_dedupe` (was `lookup_request_dedupe`, lines 510–559)
- `do_read_transition_audit` (was `read_transition_audit`, lines 560–595)
- `do_materialize_reset_successor` (was `materialize_reset_successor`, lines 842–956)

Imports needed in `load.rs`:
```rust
use anyhow::{Context, Result, anyhow};
use sqlx::Connection;
use tokeira_kernel::{BasicKernel, HistoryEvent, LoadedRun, ReplayContext, Transition, WorkflowState};
use tokeira_types::{
    ExecutionRef, ExecutionStatus, NamespaceId, RunId, RunKey, ShardEpoch, ShardId,
    TaskQueueName, TransitionSeq, WorkflowId,
};
use tracing::instrument;
use uuid::Uuid;

use crate::{CommitResult, DbClass, LoadedRun, RequestRecord, TransitionAuditRecord};
use super::{DsqlRunRepository, effective_history_limit};
use crate::dsql::{codec, convert};
```

### `dispatch.rs` — Extract from lines 958–1148

Methods to move as `pub(super)`:
- `do_list_dispatchable_workflow_tasks` (lines 958–983)
- `do_persist_to_backlog` (lines 1025–1076)
- `do_drain_backlog` (lines 1077–1148)
- `do_list_dispatchable_workflow_tasks_for_shard` (lines 1175–1208)

Free functions to move:
- `collect_dispatchable_workflow_tasks` (line 1431)
- `sticky_fields` (line 1480)

### `activity.rs` — Extract from lines 984–1024 + 1210–1247 + 1354–1381

Methods to move as `pub(super)`:
- `do_list_dispatchable_activity_tasks` (lines 985–1024)
- `do_list_dispatchable_activity_tasks_for_shard` (lines 1210–1247)
- `do_list_open_activities_for_shard` (lines 1354–1381)

Type alias + free functions to move:
- `type ActivityDispatchRow` (line 1418)
- `activity_dispatch_from_row` (line 1496)
- `collect_activity_sweep_entries` (line 1588)

### `timers.rs` — Extract from lines 1149–1174 + 1249–1285

Methods to move as `pub(super)`:
- `do_list_due_timers` (lines 1150–1173)
- `do_list_due_timers_for_shard` (lines 1249–1285)

### `visibility.rs` — Extract from lines 1287–1416

Methods to move as `pub(super)`:
- `do_list_runs_with_workflow_timeouts_for_shard` (lines 1287–1320)
- `do_list_started_workflow_tasks_for_shard` (lines 1322–1353)
- `do_list_pending_nexus_operations_for_shard` (lines 1383–1416)

Free functions to move:
- `collect_workflow_timeout_entries` (line 1525)
- `collect_started_workflow_task_entries` (line 1557)
- `collect_nexus_sweep_entries` (line 1608)

## The Thin Delegation Block in `mod.rs`

Replace the current `impl RunRepository for DsqlRunRepository` block (lines 381–1416) with:

```rust
#[async_trait]
impl RunRepository for DsqlRunRepository {
    async fn resolve_execution(&self, execution: &ExecutionRef) -> Result<Option<RunKey>> {
        self.do_resolve_execution(execution).await
    }

    async fn find_latest_run(&self, namespace_id: NamespaceId, workflow_id: &WorkflowId) -> Result<Option<RunKey>> {
        self.do_find_latest_run(namespace_id, workflow_id).await
    }

    async fn load_run(&self, run_key: RunKey) -> Result<LoadedRun> {
        self.do_load_run(run_key).await
    }

    async fn read_history(&self, run_key: RunKey, after_event_id: i64, limit: usize) -> Result<Vec<HistoryEvent>> {
        self.do_read_history(run_key, after_event_id, limit).await
    }

    async fn lookup_request_dedupe(&self, execution: &ExecutionRef, request_id: &RequestId) -> Result<Option<RequestRecord>> {
        self.do_lookup_request_dedupe(execution, request_id).await
    }

    async fn read_transition_audit(&self, run_key: RunKey) -> Result<Vec<TransitionAuditRecord>> {
        self.do_read_transition_audit(run_key).await
    }

    async fn commit_transition(&self, run_key: RunKey, transition: Transition, epoch: ShardEpoch) -> Result<CommitResult> {
        self.do_commit_transition(run_key, transition, epoch).await
    }

    async fn commit_transition_for_bundle(&self, run_key: RunKey, execution_home_bundle: ShardId, transition: Transition, epoch: ShardEpoch) -> Result<CommitResult> {
        self.do_commit_transition_for_bundle(run_key, execution_home_bundle, transition, epoch).await
    }

    async fn materialize_reset_successor(&self, base_run_key: RunKey, fork_event_id: i64, successor_run_id: RunId) -> Result<()> {
        self.do_materialize_reset_successor(base_run_key, fork_event_id, successor_run_id).await
    }

    async fn list_dispatchable_workflow_tasks(&self, queue: &QueueKey, limit: usize) -> Result<Vec<DispatchableWorkflowTask>> {
        self.do_list_dispatchable_workflow_tasks(queue, limit).await
    }

    async fn list_dispatchable_activity_tasks(&self, queue: &QueueKey, limit: usize) -> Result<Vec<DispatchableActivityTask>> {
        self.do_list_dispatchable_activity_tasks(queue, limit).await
    }

    async fn persist_to_backlog(&self, entries: Vec<BacklogEntry>) -> Result<()> {
        self.do_persist_to_backlog(entries).await
    }

    async fn drain_backlog(&self, queue: &QueueKey, limit: usize) -> Result<Vec<BacklogEntry>> {
        self.do_drain_backlog(queue, limit).await
    }

    async fn list_due_timers(&self, now: OffsetDateTime, limit: usize) -> Result<Vec<DueTimer>> {
        self.do_list_due_timers(now, limit).await
    }

    async fn list_dispatchable_workflow_tasks_for_shard(&self, shard_id: ShardId, limit: usize) -> Result<Vec<DispatchableWorkflowTask>> {
        self.do_list_dispatchable_workflow_tasks_for_shard(shard_id, limit).await
    }

    async fn list_dispatchable_activity_tasks_for_shard(&self, shard_id: ShardId, limit: usize) -> Result<Vec<DispatchableActivityTask>> {
        self.do_list_dispatchable_activity_tasks_for_shard(shard_id, limit).await
    }

    async fn list_due_timers_for_shard(&self, shard_id: ShardId, now: OffsetDateTime, limit: usize) -> Result<Vec<DueTimer>> {
        self.do_list_due_timers_for_shard(shard_id, now, limit).await
    }

    async fn list_runs_with_workflow_timeouts_for_shard(&self, shard_id: ShardId, limit: usize) -> Result<Vec<WorkflowTimeoutSweepEntry>> {
        self.do_list_runs_with_workflow_timeouts_for_shard(shard_id, limit).await
    }

    async fn list_started_workflow_tasks_for_shard(&self, shard_id: ShardId, limit: usize) -> Result<Vec<WftTimeoutSweepEntry>> {
        self.do_list_started_workflow_tasks_for_shard(shard_id, limit).await
    }

    async fn list_open_activities_for_shard(&self, shard_id: ShardId, limit: usize) -> Result<Vec<ActivitySweepEntry>> {
        self.do_list_open_activities_for_shard(shard_id, limit).await
    }

    async fn list_pending_nexus_operations_for_shard(&self, shard_id: ShardId, limit: usize) -> Result<Vec<NexusSweepEntry>> {
        self.do_list_pending_nexus_operations_for_shard(shard_id, limit).await
    }
}
```

## Naming Convention

All delegated methods use the prefix `do_` followed by the trait method name. Example:
- Trait method: `commit_transition` → Inherent method: `do_commit_transition`
- Trait method: `load_run` → Inherent method: `do_load_run`

## Verification

After the split:
```bash
cargo check -p tokeira-storage --features dsql
cargo test -p tokeira-storage --features dsql
cargo clippy -p tokeira-storage --features dsql --no-deps
```

All existing tests must pass unchanged. No public API changes. No behavioral changes.

## Key Gotcha: The `leases.rs` Pattern

Look at how `leases.rs` already works — it does NOT use `use super::record_dsql_operation;`. The macro is available because it's defined in the parent module. Sub-modules just use the macro name directly. Follow this exact pattern.

The imports in `leases.rs` are:
```rust
use super::{DsqlRunRepository, epoch_from_sql, epoch_to_sql, record_dsql_operation};
```

Wait — actually it DOES import `record_dsql_operation`. Let me check... The import is there but it's importing the macro by name. In Rust 2021+, `macro_rules!` macros defined in a module are available to child modules without explicit import. But the explicit `use super::record_dsql_operation;` also works (it's a no-op but doesn't hurt). Follow whatever `leases.rs` does for consistency.

## Order of Operations

1. Create `commit.rs` — extract commit methods + all mutation helpers + `partition_for` + `option_key_part`
2. Create `load.rs` — extract read/resolve methods + `materialize_reset_successor`
3. Create `dispatch.rs` — extract workflow dispatch + backlog methods + `collect_dispatchable_workflow_tasks` + `sticky_fields`
4. Create `activity.rs` — extract activity dispatch methods + `ActivityDispatchRow` type + `activity_dispatch_from_row` + `collect_activity_sweep_entries`
5. Create `timers.rs` — extract timer methods
6. Create `visibility.rs` — extract sweep/visibility methods + `collect_workflow_timeout_entries` + `collect_started_workflow_task_entries` + `collect_nexus_sweep_entries`
7. Rewrite `mod.rs` — remove extracted code, add `mod` declarations, replace trait impl with thin delegation
8. Run `cargo check -p tokeira-storage --features dsql`
