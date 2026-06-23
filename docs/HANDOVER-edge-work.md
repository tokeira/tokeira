# Hand-over — complete the edge "Work to be done" (v1.31.0 surface)

**Author:** Kiro · **Date:** 2026-06-23 · **For:** Claude Code (long continuous session)
**Branch:** `main` (tokeira pushes directly to `main`) · **Baseline commit:** `8849464a`

> **Mission:** drive [`docs/readiness/edge-unimplemented.md`](./readiness/edge-unimplemented.md)'s
> **Work to be done** table to **empty** — implement every in-surface public-edge RPC that currently
> answers `UNIMPLEMENTED`, GA-conformant to Temporal **v1.31.0**, with tests, until the worklist is
> empty and every enforced command is green. This doc is self-contained: it is the worklist, the order,
> the per-item loop, the conventions, and the done-definition for one long session.

---

## 1. The worklist (single source of truth)

`docs/readiness/edge-unimplemented.md` is the authoritative worklist. It is **work only** — every row is
an in-surface RPC answering `UNIMPLEMENTED`. As of baseline it holds **26 RPCs** across 8 owning specs.
Each row carries: RPC · service · how it stubs (`unimplemented` / `deferred_unary!`) · owning spec ·
spec-task progress.

Two companion facts you must respect:

- **Scope is fixed by** [`docs/conformance/v1.31.0/supported.md`](./conformance/v1.31.0/supported.md)
  (in-surface), [`excluded.md`](./conformance/v1.31.0/excluded.md) (out), and
  [`decisions.md`](./conformance/v1.31.0/decisions.md) (TBD — **do not** work these: auth, worker-
  versioning V1/V2). Never implement anything excluded or under decision.
- **Per-RPC remediation owners and a dependency matrix** are in
  `.kiro/specs/api-conformance-tracker/tracker.md` (advisory; status counts there are stale — trust the
  worklist and the specs' own `tasks.md`).

## 2. Non-negotiable conventions (this repo)

- **Ground truth = v1.31.0.** `proto/upstream/` for wire shape; Temporal source at tag `v1.31.0` via
  `git -C ../temporal show v1.31.0:<path>` and `git grep <pat> v1.31.0 -- <path>`. Never web-search
  Temporal; never read generated `target/` artifacts. Cite the source path + tag in a comment for any
  non-obvious behaviour. A green test on a guessed contract is worse than a raised question (AGENTS.md §8).
- **Layering.** `tokeira-kernel` is pure (no I/O/async/storage/metrics/network/config). Side effects
  (delivery, scanners, AWS, HTTP) are `tokeira-runtime`; request translation is `tokeira-edge` (thin —
  no workflow semantics). Put new logic in the right plane; the edge handler usually translates a
  request into a runtime call, not into business logic.
- **Build-phase migrations (storage).** Fold new columns into the base `CREATE TABLE` migration; no
  `ALTER` (see `crates/tokeira-storage/AGENTS.md`). DSQL DDL subset: one statement per file, secondary
  indexes `ASYNC`, no `CHECK`, no `BIGSERIAL`.
- **Comments are part of the deliverable** (AGENTS.md §9): module `//!` docs, `///` on public items,
  inline WHY on correctness-critical decisions, v1.31.0 citations on ground-truthed behaviour. WHY, not
  WHAT.
- **No explicit sleeps in tests** — synchronize on observable state (channels / `tokio::sync::Notify` /
  condvar).
- **Verification gates** (per touched crate, then workspace):
  `cargo +nightly fmt` · `cargo lint` (the alias = clippy `-D warnings`; NOT raw clippy) ·
  `cargo test -p <pkg>` · `cargo check --workspace` (catches exhaustive-match breakage) ·
  `RUSTDOCFLAGS="-D warnings" cargo doc -p <pkg> --no-deps` for doc changes.
- **Commits:** author the message via `fsWrite` to `artifacts/cm-*.txt`, then
  `git commit -F artifacts/cm-*.txt`, then `rm -rf artifacts` (the embedded terminal truncates long
  `-m`). One focused commit per RPC or per spec. Push to `main` after each.
- **NEVER commit:** `.claude/` and
  `.kiro/specs/temporal-functional-conformance/reference/runall-results.json` (both untracked — leave
  them). Stage files explicitly; never `git add .` / `git add -A` (it grabs the forbidden files).
- **Revert safety** (AGENTS.md §5): never `git checkout`/`reset --hard`/`restore`/`clean -f` on the
  worktree without explicit approval.

## 3. The work, grouped and ordered

Do them in this order; it front-loads the spec-ready items and isolates the spec-authoring work.

### Wave A — spec-ready (`tasks.md` exists, 0/N). Implement directly.

| Spec | RPCs | Notes / ground-truth |
|------|------|----------------------|
| `api-conformance-task-queue` | `ListTaskQueuePartitions` (+ DescribeTaskQueue field gaps) | Smallest; good warm-up. |
| `api-conformance-workflow-options` | `UpdateWorkflowExecutionOptions` | FieldChange semantics; kernel already has `UpdateExecutionOptions`. |
| `api-conformance-namespace-full` | `DeprecateNamespace` (WF), `DeleteNamespace` (Op), `UpdateNamespace` | Adds one **namespace admission guard** reused by every start-like path (see tracker dependency matrix). |
| `api-conformance-remote-cluster` | `AddOrUpdateRemoteCluster`, `RemoveRemoteCluster`, `ListClusters` | **Registry / metadata CRUD ONLY.** Must NOT imply replication/failover (`excluded.md` §2). |
| `api-conformance-multi-operation` | `ExecuteMultiOperation` | **Same-run atomic admission, one kernel transition — NOT sequential independent mutations** (its `design.md` is explicit). Validation-first. Most complex in Wave A. |

For each: read `requirements.md` → `design.md` → `tasks.md`, implement per the design, then check the
tasks. `RemoveSearchAttributes` (Operator) has **no spec** — handle it in Wave B.

### Wave B — no usable spec yet. Author the spec first, then implement.

These are placeholder-only (`.placeholder.md`) or absent. Authoring a real spec
(`requirements.md` + `design.md` + `tasks.md`, ground-truthed to v1.31.0, validated against the Kiro
spec format) is the first sub-task. **Surface scope questions; do not invent surface.**

| Spec to author | RPCs | Source material |
|----------------|------|-----------------|
| `worker-config-management` | `DescribeWorker`, `ListWorkers`, `FetchWorkerConfig`, `UpdateWorkerConfig` | placeholder cites `temporal-api-v1.62-sync`; server-side worker-config store. |
| `workflow-rules` | `CreateWorkflowRule`, `DescribeWorkflowRule`, `DeleteWorkflowRule`, `ListWorkflowRules`, `TriggerWorkflowRule` | consumes `temporal.api.rules.v1`; define rule-evaluation semantics. |
| (search attributes) | `RemoveSearchAttributes` (Operator) | No owning spec. Author a small spec or fold into the existing search-attribute surface — **confirm which** before building. |

### Wave C — partial, larger.

| Spec | RPCs | State |
|------|------|-------|
| `activity-executions-first-class` | 8 standalone-activity RPCs | **15/19 tasks done.** Finish the remaining read-side proto mapping (`Describe/Poll/List/Count`) + tasks. **Do NOT flip `enable_standalone_activities` to on by default** — off-by-default matches v1.31.0 (`chasm/lib/activity/frontend.go @ v1.31.0`); changing the default is a separate decision, not part of this work. |

## 4. The per-item loop (repeat until the worklist is empty)

For each RPC (or each spec):

1. **Read the owning spec** (`requirements` → `design` → `tasks`). If placeholder/absent (Wave B),
   **author it first** — ground-truthed, format-valid; raise any genuine scope question (§6) rather than
   guessing.
2. **Ground-truth** the behaviour against v1.31.0 (`proto/upstream/` for shape; `../temporal` @ tag for
   behaviour). Note the citations you will put in comments.
3. **Implement**, respecting the planes (§2). Replace the `unimplemented` / `deferred_unary!` edge stub
   with a real handler that translates to the runtime/kernel/storage path the design specifies.
4. **Test** per the spec's task list (golden / property / unit). No sleeps. Tag property tests
   `// Feature: <spec>, Property N` where the spec defines one.
5. **Gates:** `cargo +nightly fmt` · `cargo lint` · `cargo test -p <pkg>` · `cargo check --workspace`.
6. **Check the spec's `tasks.md`** boxes you completed.
7. **Regenerate the worklist:** re-grep `Status::unimplemented` + `deferred_unary!` in
   `crates/tokeira-edge/src/grpc/{workflow_service,operator_service}.rs`, drop the now-implemented rows
   from `edge-unimplemented.md`, refresh its "As of" commit and the spec-implemented counts, and confirm
   the minimality cross-reference still holds.
8. **Commit** (`-F` file, §2) and **push** to `main`.
9. Next item.

## 5. Edge entry points (grep the handler name — line numbers drift)

All handlers are in `crates/tokeira-edge/src/grpc/`. Find each by name; the stub is the body to replace.

- **`workflow_service.rs`** — `deprecate_namespace`, `execute_multi_operation`,
  `list_task_queue_partitions`, `update_workflow_execution_options` (direct `Status::unimplemented`);
  the `deferred_unary!` block for `describe_worker`, `list_workers`, `fetch_worker_config`,
  `update_worker_config`, and the five `*_workflow_rule`; the standalone-activity handlers
  `start_/describe_/poll_/list_/count_/request_cancel_/terminate_/delete_activity_execution` (stub when
  the CHASM `chasm_activity` bridge is absent).
- **`operator_service.rs`** — `delete_namespace`, `remove_search_attributes`,
  `add_or_update_remote_cluster`, `remove_remote_cluster`, `list_clusters`.

The `deferred_unary!` macro and the `Reject`→`Status` mapping live alongside; new reject variants must be
handled in `grpc/errors.rs`.

## 6. Decisions to surface, not guess

- **`RemoveSearchAttributes`** has no owning spec — confirm whether to author one or fold into the
  search-attribute surface before implementing.
- **Standalone-activities default gate** — completing the spec does not mean enabling the feature by
  default; off-by-default is the v1.31.0 baseline. Flipping it is a separate decision.
- **Remote-cluster** — registry/metadata CRUD only; multi-cluster replication/failover is out
  (`excluded.md`). Do not build replication behaviour.
- **`ExecuteMultiOperation`** — atomic same-run admission, not sequential mutations (its design).
- Anything in `decisions.md` (auth, worker-versioning V1/V2) is **out of this worklist**.

## 7. Definition of done (session)

- `edge-unimplemented.md` "Work to be done" table is **empty** (every listed RPC implemented; the
  regenerate-grep finds no in-surface RPC answering `UNIMPLEMENTED`).
- Every owning spec's `tasks.md` for the worked items is checked; Wave B specs are authored and complete.
- Enforced commands green from the workspace root (AGENTS.md):
  `cargo +nightly fmt --all --check` · `cargo lint` · `cargo test-lint` · `cargo check --workspace` ·
  `cargo test --workspace` · `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`.
- All commits pushed to `main`.

## 8. Context map

- `docs/readiness/edge-unimplemented.md` — the worklist (start here every loop).
- `docs/conformance/v1.31.0/{supported,excluded,decisions}.md` — scope boundary (never cross it).
- `.kiro/specs/api-conformance-tracker/tracker.md` — RPC→spec index + dependency matrix (advisory).
- `.kiro/specs/temporal-functional-conformance/reference/FINDINGS.md` — per-cluster investigation.
- `docs/readiness/conformance.md` — measured status (the numerator).
- `AGENTS.md` — the binding rules (§8 ground truth, §9 documentation, §5 revert safety, §7 commits).
