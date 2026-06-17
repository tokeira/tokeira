# DIRECTION — C3 Visibility: search-attribute seeding + system-field coverage

**Audience:** Codex (or any implementer) picking up the remaining C3 conformance work.
**Date:** 2026-06-11.
**Status:** ready to implement. No code changes have been made for this yet.

## TL;DR

The visibility *machinery* is already built and wired (legacy list RPCs, projection
store, query/filter compiler, `GetSearchAttributes`, per-partition projection workers in
`tokeirad`). What is missing is **data, not plumbing**: the standard search attributes are
never registered, so advanced-visibility queries fail with
`unknown search attribute: …` and `GetSearchAttributes` is empty. Two concrete changes:

1. **Seed the v1.31.0 `system` + `predefined` search attributes at server startup** into
   both (a) the operator catalog (`OperatorApi`) and (b) the visibility store attribute
   registry, per namespace.
2. **Add the 7 missing `SystemField` variants** and wire them through the filter/column
   mapping (with a schema check — see the caveat in step 2).

Then run the targeted C3 suites and close any residual query-surface gaps.

## Hard constraints (AGENTS.md)

- **Ground truth is v1.31.0.** Verify every behaviour against `proto/upstream/` (shape) and
  the local Temporal checkout at `../temporal`, tag `v1.31.0`, via
  `git -C ../temporal show v1.31.0:<path>`. Cite path + tag inline in code comments.
- **No kernel additions.** This is edge / runtime / projection / storage only.
- **Edition 2024, `cargo +nightly fmt`, `cargo lint` (clippy -D warnings), `thiserror` in
  libs.** Comments explain WHY, per §9.
- Do **not** stage the ephemeral `.kiro/specs/temporal-functional-conformance/reference/`
  folder in code commits.

## Ground truth — the standard search-attribute sets

Source: `common/searchattribute/sadefs/constants.go @ v1.31.0`.

### `system` (16) — stored as separate fields/columns, not inside the SA blob

| Name (constant string)          | Type        |
|---------------------------------|-------------|
| `WorkflowId`                    | KEYWORD     |
| `RunId`                         | KEYWORD     |
| `WorkflowType`                  | KEYWORD     |
| `StartTime`                     | DATETIME    |
| `ExecutionTime`                 | DATETIME    |
| `CloseTime`                     | DATETIME    |
| `ExecutionStatus`               | KEYWORD     |
| `TaskQueue`                     | KEYWORD     |
| `HistoryLength`                 | INT         |
| `ExecutionDuration`             | INT         |
| `StateTransitionCount`          | INT         |
| `HistorySizeBytes`              | INT         |
| `ParentWorkflowId`              | KEYWORD     |
| `ParentRunId`                   | KEYWORD     |
| `RootWorkflowId`                | KEYWORD     |
| `RootRunId`                     | KEYWORD     |

tokeira `SystemField` (`crates/tokeira-projection/src/types.rs:130`) currently has 9:
`WorkflowId, RunId, WorkflowType, TaskQueue, ExecutionStatus, StartTime, CloseTime,
HistoryLength, StateTransitionCount`. **Missing 7:** `ExecutionTime, ExecutionDuration,
HistorySizeBytes, ParentWorkflowId, ParentRunId, RootWorkflowId, RootRunId`.

### `predefined` — internal SAs carried inside the SA object

`TemporalChangeVersion` (KEYWORD_LIST), `BinaryChecksums` (KEYWORD_LIST), `BuildIds`
(KEYWORD_LIST), `BatcherNamespace` (KEYWORD), `BatcherUser` (KEYWORD),
`TemporalScheduledStartTime` (DATETIME), `TemporalScheduledById` (KEYWORD),
`TemporalSchedulePaused` (BOOL), `TemporalNamespaceDivision` (KEYWORD), `TemporalPauseInfo`
(KEYWORD_LIST), `TemporalReportedProblems` (KEYWORD_LIST), `TemporalWorkerDeploymentVersion`
(KEYWORD), `TemporalWorkflowVersioningBehavior` (KEYWORD), `TemporalWorkerDeployment`
(KEYWORD), `TemporalUsedWorkerDeploymentVersions` (KEYWORD_LIST),
`TemporalExternalPayloadCount` (INT), `TemporalExternalPayloadSizeBytes` (INT).

`predefinedWhiteList` is the user-allowed subset of `predefined` (the rest are internal-only,
banned from user-facing add but still accepted in queries).

### Decision to ground-truth BEFORE coding

`GetSearchAttributes` and the visibility query registry have **different** visibility:
- The query/filter registry must accept `system` + the full `predefined` set so advanced
  queries compile (e.g. `TemporalExternalPayloadCount`, `BuildIds`).
- `GetSearchAttributes` returns what Temporal exposes to users. Confirm the exact response
  shape against the v1.31.0 handler before deciding whether `predefined`/`predefinedWhiteList`
  appear in the `GetSearchAttributes` response or only `system` + custom. Read:
  `service/frontend/workflow_handler.go` (`GetSearchAttributes`) and
  `common/searchattribute/*.go` (provider/manager) at tag `v1.31.0`. Do **not** guess — cite
  the handler.

## Step 1 — Seed system + predefined SAs at startup

**Where the registries are constructed:** `apps/tokeirad/src/lib.rs` (the in-memory branch
~`:453` and the DSQL branch ~`:485`). The operator API is `InMemoryOperatorApi::new`
(`crates/tokeira-edge/src/operator_service.rs:55`) — currently an empty `attrs` map. The
visibility store registry is empty by default (`InMemoryVisibilityStore` / `DsqlVisibilityStore`
`register_attr`).

**Do:**

1. Add a single source-of-truth table of the v1.31.0 standard SAs (name → `SearchAttrType`)
   in `tokeira-projection` (e.g. `src/system_attrs.rs`) so both the registry and the catalog
   seed from one list. Cite `sadefs/constants.go @ v1.31.0`. Map Temporal `IndexedValueType`
   → tokeira `SearchAttrType` (`KEYWORD→Keyword`, `KEYWORD_LIST→KeywordList`, `INT→Int`,
   `DOUBLE→Double`, `BOOL→Bool`, `DATETIME→Datetime`, `TEXT→Text`).
2. At startup, for the default namespace(s), register every standard SA:
   - into the visibility store via `register_attr(namespace_id, name, type)` so
     `compile_filter` resolves them;
   - into the operator catalog via `upsert_search_attribute` (only the user-visible subset
     per the ground-truth decision above) so `GetSearchAttributes` returns them.
3. Make seeding **idempotent** (register-if-absent) — it must be safe across the per-partition
   workers and restarts. `register_attr` is already idempotent by `(namespace_id, attr_name)`.
4. Note: `system` fields are resolved directly by the filter compiler's `SystemField` match
   (`query_service.rs` / `filter.rs`), so they do not strictly need registry rows to be
   *queryable*; but `GetSearchAttributes` must still report them. The `predefined` set DOES
   need registry rows because it flows through the custom-attribute path.

## Step 2 — Add the 7 missing `SystemField` variants

Add `ExecutionTime, ExecutionDuration, HistorySizeBytes, ParentWorkflowId, ParentRunId,
RootWorkflowId, RootRunId` to `SystemField` (`crates/tokeira-projection/src/types.rs:130`)
and extend:
- the name→field parse in `query_service.rs` (`parse_group_by` and the filter field resolver
  in `filter.rs`),
- the `SystemField` → `vis_execution` column mapping in `dsql_store.rs::compile_filter_sql`
  and the in-memory evaluator in `memory.rs`.

**CAVEAT — schema check required (this is the real scoping risk).** The `system` fields are
stored columns. Verify `vis_execution` actually has columns for the new fields
(`crates/tokeira-storage/migrations/` — the `vis_execution` `CREATE TABLE` and follow-ups).
If columns for `ExecutionTime / ExecutionDuration / HistorySizeBytes / ParentWorkflowId /
ParentRunId / RootWorkflowId / RootRunId` are absent:
- decide per field whether the conformance suites actually query/sort on it (run the suites
  first to see which appear), and
- for the ones that matter, add columns (build-phase: **fold into the base `vis_execution`
  CREATE migration, no `ALTER`** per `tokeira-storage/AGENTS.md`) **and** populate them in the
  `VisibilitySink` from the projection record. Parent/Root IDs come from the run's parent/root
  execution info; `ExecutionTime` is the (possibly backoff-adjusted) first-run time;
  `ExecutionDuration` = close−start; `HistorySizeBytes` from the projection record. Ground-truth
  each against `service/history/visibility_queue_task_executor.go @ v1.31.0` and the
  visibility record shape.

Do not blindly add all 7 columns — gate on what the suites need, and prefer the smallest
change that turns the suites green.

## Step 3 — Run the targeted C3 suites and close residual gaps

Boot `tokeirad` (in-memory) and run only the C3 suites — do **not** run the full 2 hr corpus:

- `TestAdvancedVisibilitySuite`
- `TestAdvancedVisibilitySuiteLegacy`
- `TestWorkflowVisibilityTestSuite`
- `TestWorkflowMemoTestSuite`
- `TestListWorkflow*`

Likely residual query-surface gaps to confirm against failures (don't pre-fix; let the run
drive it): `ORDER BY`, `BETWEEN`, `STARTS_WITH`, keyword-list `IN`, null-`CloseTime` ordering,
and memo round-trip through the sink. The DSQL/in-memory query compiler already supports most
of these (`dsql_store.rs::compile_filter_sql`, `memory.rs::eval_expr`); verify rather than
assume.

For any test that depends on an internal/unsupported surface (e.g. dynamic-config-driven
visibility limits), use the conformance skip registry pattern (fork
`tests/testcore/tokeira_conformance_skip.go`) rather than forcing a fix — same approach used
for C2. Classify as `out-of-public-scope` with a citation.

## Files in play

| File | Change |
|------|--------|
| `crates/tokeira-projection/src/system_attrs.rs` (new) | v1.31.0 standard SA table + IndexedValueType→SearchAttrType map |
| `apps/tokeirad/src/lib.rs` | seed SAs into registry + catalog at startup (both in-memory and DSQL branches) |
| `crates/tokeira-edge/src/operator_service.rs` | accept seeding; confirm GetSearchAttributes exposure subset |
| `crates/tokeira-projection/src/types.rs` | add 7 `SystemField` variants |
| `crates/tokeira-projection/src/query_service.rs`, `filter.rs` | parse/resolve new system fields |
| `crates/tokeira-projection/src/dsql_store.rs`, `memory.rs` | column/eval mapping for new fields |
| `crates/tokeira-storage/migrations/` | only if new `vis_execution` columns are needed (fold into base CREATE) |
| fork `tests/testcore/tokeira_conformance_skip.go` | skip any out-of-scope C3 tests |

## Validation / definition of done

- `cargo +nightly fmt --all --check`, `cargo lint`, `cargo test-lint` clean.
- `cargo test -p tokeira-projection` and `cargo test -p tokeira-edge` green.
- `GetSearchAttributes` returns the expected v1.31.0 set (ground-truthed).
- The C3 suites pass (or are explicitly, citation-backed skipped) in a targeted run.
- Update the C3 block in `FINDINGS.md` with the final pass/skip numbers.

## Related, already-done context

- `projection-visibility` spec — COMPLETE (DSQL query surface, migrations `V029`–`V042`,
  filter compiler, rollups). This direction is about *seeding the data*, not the query surface.
- `api-conformance-visibility-legacy` spec — the list-RPC translators it describes are
  implemented in `tokeira-edge` even though its `tasks.md` boxes are unchecked.
