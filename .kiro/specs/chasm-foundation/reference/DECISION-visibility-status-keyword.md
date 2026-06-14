# Decision note — execution status in the shared visibility index is a generic `status_keyword`

**For:** the chasm-foundation visibility work (finishing Stage 1; unblocking Stage 2 / Task 24).
**Status:** decided, ground-truthed to Temporal `v1.31.0` (`AGENTS §8`). Implement as below.

---

## TL;DR

- The shared, archetype-neutral visibility index represents execution status as a **generic,
  low-cardinality `status_keyword` string column**, interpreted per archetype — **not** a
  workflow-typed `ExecutionStatus` enum (Requirement 10.5).
- This is exactly what `v1.31.0` does: every CHASM archetype contributes `ExecutionStatus` as a
  **low-cardinality keyword search attribute**, and the value it stores is the **collapsed
  API-level status string**.
- Therefore the projection **read paths** (list filter, group-by, rollup dimension) must key off
  `status_keyword` (archetype-scoped), not the typed `ExecutionStatus` enum. That read-path
  migration **completes Stage 1's generalization** (it is the part 23.1/23.3 generalized the
  record/sink for but did not carry into the query layer); it is **not** a Task-24 bolt-on.
- The activity's contributed `status_keyword` is the **collapsed** API status
  (`Scheduled`/`Started`/`CancelRequested` → `Running`; terminals map through), not the
  fine-grained internal status. The fine-grained internal status surfaces only in
  describe / `PendingActivityInfo`.
- The wire `ActivityExecutionListInfo.status` is activity's **own** `ActivityExecutionStatus`
  enum, mapped from the internal status **at the edge** (Stage 3 / Task 25), independent of how the
  index stores status.

## Ground truth (`v1.31.0`)

- **Status is a low-cardinality keyword SA, not an enum column.** The activity and scheduler both
  declare it as a keyword:
  - `chasm/lib/activity/activity.go:40 @ v1.31.0` —
    `StatusSearchAttribute = chasm.NewSearchAttributeKeyword("ExecutionStatus", chasm.SearchAttributeFieldLowCardinalityKeyword01)`
  - `chasm/lib/scheduler/scheduler.go:68 @ v1.31.0` — same pattern for the scheduler archetype.
  - `chasm/search_attribute.go:55 @ v1.31.0` — `SearchAttributeFieldLowCardinalityKeyword01` is a
    keyword (string) field.
- **The stored value is the collapsed API status.** `chasm/lib/activity/activity.go:932 @ v1.31.0`:
  `StatusSearchAttribute.Value(InternalStatusToAPIStatus(a.GetStatus()).String())`, where
  `InternalStatusToAPIStatus` (`activity.go:594 @ v1.31.0`) maps
  `ACTIVITY_EXECUTION_STATUS_{SCHEDULED,STARTED,CANCEL_REQUESTED}` → `RUNNING` and passes the
  terminal statuses through.
- **The wire list status is its own enum.** `ActivityExecutionListInfo.status` is
  `temporal.api.enums.v1.ActivityExecutionStatus`
  (`proto/upstream/temporal/api/activity/v1/message.proto`), distinct from
  `WorkflowExecutionStatus`. Its doc notes only scheduled/terminal statuses appear in the list.

## Consequences for Tokeira

1. **Finish Stage 1 (new task 23.7).** Migrate the in-memory and DSQL projection read paths — the
   `ExecutionStatus` list filter, the `group_by` value, and the rollup dimension — from
   `format!("{:?}", row.status)` / `FilterValue::Status(row.status)` to the generic
   `row.status_keyword`, archetype-scoped. Workflow List/Count/UI must stay green (existing tests +
   Properties 12/13). The typed `ExecutionRow.status: ExecutionStatus` becomes workflow-internal
   only (or is removed) and is no longer the index query key.
2. **Stage 2 / Task 24.3.** `ActivityExecution::visibility_snapshot().status_keyword` is the
   collapsed API status name (`Running`/`Completed`/`Failed`/`Canceled`/`Terminated`/`TimedOut`/
   `Unspecified`), not the fine-grained internal name.
3. **Stage 3 / Task 25.3.** The edge maps the internal activity status →
   `enums.v1.ActivityExecutionStatus` for `ActivityExecutionListInfo.status`; list/count *filtering*
   by `ExecutionStatus` matches against the stored collapsed `status_keyword`.

## Why not a per-archetype status enum in the index

A typed enum per archetype would force list/count to reconcile heterogeneous status types across one
shared index (Req 10.1) and reintroduces exactly the N-way-merge the single-index design avoids. A
single generic keyword keeps one composite index `(namespace_id, archetype_id, status_keyword,
start_time DESC, run_key)` and one rollup dimension across all archetypes, which is both the design's
intent and `v1.31.0`'s actual representation.
