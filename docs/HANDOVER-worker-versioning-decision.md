# Hand-over — Worker Versioning V1/V2 decision landed (drive back to Codex)

**Author:** Claude (decision research + implementation) · **Date:** 2026-07-12 · **For:** Codex (functional-conformance drive)
**Status:** ✅ IMPLEMENTED, uncommitted in the working tree, verified (`cargo fmt --check` clean; 115
test binaries / 2,125 tests green; the only 2 failures are in *your* in-progress files — see §3).

> **TL;DR.** decisions.md item 2 is resolved and implemented: conformance targets the GA Worker
> Deployment surface only; the five deprecated V1/V2 RPCs are now **admitted and rejected exactly like
> a default-config v1.31.0 server** (`PERMISSION_DENIED`, the fixed v0.1/v0.2 messages, a
> `PermissionDeniedFailure{reason:""}` trailer detail, gate-before-field-validation, and the
> `GetWorkerTaskReachability` v0.1-gate/v0.2-message quirk). The old in-memory `VersioningRuleStore`
> and its dispatch integration are **deleted** (~−1,300 net lines). Decision record with the full
> verified factual case: [`docs/conformance/v1.31.0/worker-versioning.md`](./conformance/v1.31.0/worker-versioning.md).

## 1. What changed (all uncommitted, in the working tree alongside your in-progress work)

**Decision + docs**

- NEW [`docs/conformance/v1.31.0/worker-versioning.md`](./conformance/v1.31.0/worker-versioning.md) —
  standalone decision record (the ground-truth case, the boundary of what stays in-surface, consequences).
- `decisions.md` item 2 → resolved; `excluded.md` §4 gains the V1/V2 enabled-path row; `supported.md`
  Worker Deployments section documents the five RPCs as in-surface-as-rejections; `README.md` indexes
  the new page.
- `docs/readiness/conformance.md` C2 row: **`TestVersioningFunctionalSuite` (406) is now OUT OF
  SURFACE** (it requires non-default dynamic config `frontend.workerVersioning{Data,Rule}APIs=true`);
  C2's untriaged denominator drops to ~88 (deployment suites only). Same change reflected in
  `docs/readiness/functional-test-order.md` (deferred table) and `docs/readiness/checklist.md`.

**Engine (tokeira)**

- `crates/tokeira-edge/src/grpc/errors.rs` — new `worker_versioning_v{1,2}_disabled_status()` builders:
  `PERMISSION_DENIED` + hand-encoded `google.rpc.Status` trailer carrying
  `PermissionDeniedFailure{reason:""}` (same pattern as the other typed errors in that file).
- `crates/tokeira-edge/src/grpc/workflow_service.rs` — the five handlers are now one-line rejections
  (V1 previously returned `UNIMPLEMENTED`; V2 was a live, *accepting* implementation — both deviated
  from stock default). New test `worker_versioning_v1_v2_rpcs_are_rejected_like_stock_default_config`
  pins code, exact messages, full trailer shape, and the reachability quirk.
- **`VersioningRuleStore` deleted** (`crates/tokeira-runtime/src/versioning.rs` removed) along with all
  threading: runtime constructor ladder simplified (`new_with_nexus_and_versioning*` gone;
  `new_with_nexus_config` replaces `new_with_nexus_and_versioning_config`), publisher redirect-rule
  rewriting removed, schedule-trigger assignment stamping removed, edge `start_versioning` keeps only
  the GA pinned-override arm, `WorkerRegistry::has_recent_poller_for_build_id` removed. The edge inner
  constructor was renamed `new_with_versioning_and_buffered_queries_and_history_wait_registry` →
  `new_with_stores_and_buffered_queries_and_history_wait_registry` (one fewer param).
  `deterministic_bucket` (GA ramp-split hash) moved into `runtime/workflow_task.rs` — it was the only
  non-V2 resident of the deleted module.
- `crates/tokeira-compatibility/src/matrix.rs` — `worker-versioning-v2` entry (Experimental) replaced
  by `worker-versioning-v1-v2` (Implemented-as-stock-rejection).
- `crates/tokeira-edge/UNSUPPORTED_FIELDS.md` — schedule `versioning_override` rationale updated
  (scheduled starts are unversioned; no more assignment-rule evaluation).

**Safety of the deletion:** the store could only ever be populated through
`UpdateWorkerVersioningRules`, which is now rejected — so every dispatch-path consumer was operating on
permanently-empty rules and the removal is behavior-neutral. This was adversarially reviewed (3-lens
multi-agent review, every non-nit finding independently verified); confirmed findings were all fixed.

## 2. One thing I touched in YOUR in-progress files

`crates/tokeira-edge/tests/grpc_properties.rs` had a **corrupted hunk** that broke the whole workspace
build: `workflow_id,` → `workflow_id: workflow_id.clone(),` inside the `arb_start_request` **closure
pattern** (syntactically invalid) — evidently an automated edit that was meant for the
`WorkflowExecutionSummary` struct literal at ~line 1096 (where `workflow_id` was moved then re-used at
~1119). I repaired both sites to the evident intent: pattern restored to `workflow_id,`, the struct
literal now clones at the first use and moves at `root_workflow_id`. Nothing else in that file was
altered; your `UpsertMemoPatch` / `InvalidSearchAttributes` hunks are untouched.

## 3. Test-bar state at handover

`cargo test --workspace --no-fail-fast` (halted during doc-tests at the operator's request; all 115
unit/integration binaries had completed): **2,125 passed, 2 failed** — both failures are in your
in-progress (uncommitted) work, not versioning-related:

1. `grpc_properties::property_workflow_command_roundtrip` — `UpsertMemo(Memo({}))` round-trips back as
   `UpsertMemoPatch(MemoPatch({}))` (empty-memo ambiguity in the new patch-command translation).
2. `tokeira_projection` `memory::tests::text_filter_uses_normalized_token_matching`
   (`crates/tokeira-projection/src/memory.rs:1336`) — your visibility text-filter work.

`cargo fmt --check` is clean workspace-wide. `tokeira-runtime`/`tokeira-edge`/`tokeira-compatibility`
compile warning-free.

## 4. Loose ends (deliberate)

- `.kiro/specs/edge-schedule-transport/design.md:61` still cites `VersioningRuleStore` as a live
  pattern — left untouched (accepted specs are point-in-time records; flag it if Kiro revisits).
- tokeira does not model upstream's `NamespaceValidatorInterceptor` (empty namespace →
  `INVALID_ARGUMENT: Namespace not set on request.`, unknown → `NOT_FOUND`, *before* any handler).
  This is a pre-existing, repo-wide surface difference on degenerate inputs, now documented in the
  decision record — **not** introduced by this change and not specific to the five RPCs. Worth a
  future raise if a corpus leaf ever pins it.
- Nothing is committed: this change set shares the working tree with your Tier-3.22-adjacent work.
  Suggested split when committing: (a) docs (decision record + ledger/index updates), (b) engine
  (rejections + store retirement), or one commit — your call as drive owner.
