# Decision note — DSQL physical schema for the generalized visibility index

**For:** the DSQL visibility store port (task 23.7 DSQL half; design.md "Visibility Generalization →
DSQL physical schema").
**Status:** decided, ground-truthed to `docs/architecture/050-dsql-storage.md`, the storage
`AGENTS.md`, and Aurora DSQL's documented constraints. Review-gated (schema). Implement after one-pass
review.

---

## Ground-truth correction (load-bearing): DSQL has multi-table transactions

design.md previously asserted "DSQL has no cross-table transactions." That is **wrong** and is
corrected here. `050-dsql-storage.md:28` — "**One workflow transition is one fenced DSQL
transaction**" — and the representative recipes (e.g. `StartWorkflowExecution` inserts
`current_execution` + `workflow_hot`, appends `WorkflowExecutionStarted` + `WorkflowTaskScheduled`,
arms timers, and emits a projection mutation, all in **one** transaction, `050-dsql-storage.md:153-164`)
show DSQL admits **multi-table ACID transactions**, bounded by: `Repeatable Read` isolation, optimistic
concurrency (commit-time conflict detection), a **3,000-row mutation limit**, and a **5-minute**
transaction age (`050-dsql-storage.md:12-16`).

This correction **simplifies** two of the decisions below: the rollup needs no `rollup_delta` ledger
(the counter update rides in the apply transaction), and the generation pointer's purpose is
transaction-size bounding, not a missing cross-table transaction.

---

## TL;DR — the six decisions

1. **One wide cross-archetype current-row table** `execution_visibility_current`, keyed
   `(namespace_id, archetype_id, run_key)`, with nullable archetype-fidelity columns so workflow
   List/Describe is unchanged (Requirement 10.13). *(Schema gap 1 — already closed in commit
   `a7a77212`.)*
2. **Archetype-scoped striped rollups** — `execution_visibility_rollup(namespace_id, archetype_id,
   dimension, value, stripe)`, `stripe = hash(run_key) % N`. `RollupDelta` and `count_from_rollup` gain
   `archetype_id`; this also fixes a real *current* in-memory bug (rollups mix archetypes today).
3. **Generation-pattern attr index** — one typed `execution_visibility_attr_index` whose **generation
   is the snapshot version** `(authority_epoch, source_transition_seq)` (Kiro Item 1), replacing
   `sa_current` + every `sa_*_idx`. Keep `sa_registry` for name → `(attr_id, attr_type)`.
4. **Partitioned checkpoints** — `projection_checkpoint(partition_id, last_applied_version)`.
5. **Apply-iff-newer fenced OCC transaction** — gate on `(authority_epoch, source_transition_seq)`;
   row + rollup counters + generation flip in one narrow commit.
6. **Retire the old workflow-only migrations** (`vis_execution`, `vis_rollup`, `projector_checkpoint`,
   `sa_current`, `sa_*_idx` + their indexes) and recreate under the DSQL DDL subset — legal because the
   schema is **build-phase** (no baseline cut).

---

## The five hazards Kiro flagged, resolved

### H1 — Build-phase confirmation (load-bearing for Decision 6)

**Confirmed build-phase; Decision 6 is legal.** Storage `AGENTS.md:17-20`: "Build phase (now): no
`ALTER TABLE` … the moment that rule is removed from the root file is the signal the baseline exists."
The rule is still present in both the root and storage `AGENTS.md`, so no baseline has been cut. The
migrations directory is the single authoritative schema and no environment has applied a cut baseline.
**Stated assumption:** if any deployed environment has applied these migrations (baseline cut), Decision
6 is **forbidden** — the change must then be forward-only `ALTER`/new-`VNNN` migrations instead, and
this note must be revised before implementing. Verify this assumption holds at implementation time.

### H2 — DSQL DDL-subset compliance (the `DdlValidator` enforces it)

Every new/edited migration: **one DDL statement per file**; secondary indexes `CREATE INDEX ASYNC` in
their **own** one-statement files; **no `CHECK`** (validate in app); **no `BIGSERIAL`** (ids in app);
columns **folded into the base `CREATE TABLE`** (no `ALTER` in build phase). The list index
`(namespace_id, archetype_id, status_keyword, start_time DESC, run_key)` is its own `ASYNC` migration
(V051, already present). Source: storage `AGENTS.md:23-26`, `050-dsql-storage.md:257`.

### H3 — Idempotent rollups under OCC retry

Striping alone is **not** the idempotency mechanism (Kiro's point). The mechanism is: the rollup counter
deltas (`-1` old value, `+1` new value, on the `hash(run_key)` stripe) are applied **in the same fenced
apply-iff-newer transaction** as the current-row upsert. The transaction gates on the stored
`(authority_epoch, source_transition_seq)`; a retried or duplicate apply observes a non-newer version
and is a **no-op on the row and the counters together**. So the **row's version is the applied-version
guard** — no separate `rollup_delta` ledger is needed, because DSQL admits the atomic multi-table update
(see the ground-truth correction). The transaction touches a handful of rows (current row + two counter
rows), far under the 3,000-row limit. Striping only avoids a hot counter row. Rollups remain rebuildable
from `execution_visibility_current` (Requirement 10.8).

### H4 — Generation numbering (Kiro Item 1, correctness fix), justification, GC, crash-safety

**Generation = the snapshot version, NOT `N = current+1`** (the must-fix gap). Attribute rows are
written at `generation = (authority_epoch, source_transition_seq)` — the version of the snapshot being
applied. The naive `N = current+1` scheme has a silent data-corruption bug under concurrency: two
applies to the same run (a retry racing the original, or two distinct snapshots) both compute
`current+1`, write attr rows at the **same** generation, and the pointer flip then exposes a generation
containing **both** sets, corrupting the winner's image. Tying the generation to the version removes it:
distinct snapshots → distinct generations (no collision); a retry of the same snapshot → the same
generation (idempotent `ON CONFLICT DO NOTHING`); the apply-iff-newer flip selects the winner. The
**pointer is the current row's own `(authority_epoch, source_transition_seq)`** — there is no separate
`search_attr_generation` column; advancing the row's version in the fenced apply transaction **is** the
flip, and queries join attr rows whose `(gen_authority_epoch, gen_source_transition_seq)` equals the
current row's version.

**Why a generation at all, not in-transaction delete+insert (Kiro Item 3).** Chosen deliberately for
**hot-path conflict-surface narrowing**: under OCC, folding the whole attr-set replacement into the
fenced commit widens its write set and retry probability on the hottest path; pre-writing the attr rows
in a prior narrow transaction and advancing one pointer keeps the fenced commit tiny. For tiny attr
sets the simpler in-transaction delete+insert is defensible — we pick the generation form because it
matches the DSQL/OCC posture. The 3,000-row limit is a secondary bound, not the justification.

**GC vs. concurrent reads (Kiro Item 2).** (i) Queries join only the current generation. (ii) The
pointer read and the attr join run in **one Repeatable-Read snapshot transaction** (the CTE), so a
concurrent pointer advance + GC cannot pull rows from under an in-flight join. (iii) GC reaps
generations **strictly below** the current pointer, **after a grace window**, never inside the apply
transaction. (iv) A crash between the prior attr write and the fenced apply leaves orphaned
higher-generation rows that are never visible (the row version still names the prior generation) and are
later reaped — crash-safe.

### H5 — Sequencing coherence

The DSQL rewrite (step 3) builds on already-landed, green in-memory work — confirmed:

- Workflow producer emits **versioned snapshots**, not deltas (task 23.4, committed `31c6ca7c`). ✓
- In-memory `status_keyword` read/query/filter migration landed (task 23.7 in-memory, committed
  `578494c6`, 47 tests green). ✓
- **Steps 1–2 (archetype-scoped `RollupDelta` + in-memory rollups) land before step 3.** They are
  not yet done; they are the first, in-memory-verifiable part of this plan, so the DSQL store is never
  rewritten against a still-half-delta or archetype-unscoped contract.

---

## Implementation order (each step compiles; in-memory steps are test-verified)

1. **Types** — `RollupDelta` + `count_from_rollup` gain `archetype_id` (and the stripe assignment).
2. **In-memory store** — archetype-scoped rollups (fixes the mixing bug; `cargo test -p
   tokeira-projection` green, Properties 12/13 hold).
3. **DSQL store** — rewrite `apply`/list/count/group-by/get/delete against
   `execution_visibility_current` / `execution_visibility_rollup` / `execution_visibility_attr_index` /
   `projection_checkpoint`: the fenced apply-iff-newer transaction (row + striped counters + generation
   flip), CTE-based archetype-scoped queries, generation-joined attr lookups.
4. **Retire** the old migrations. **Before retiring**, grep the codebase for `vis_execution`,
   `vis_rollup`, `projector_checkpoint`, `sa_current`, and `sa_*_idx` and update **every** reference
   (queries, fixtures, tests) in the same step — not just the migration files (Kiro minor). Then full
   `--features dsql` compile + the `dsql-integration` test.

## Minor revisions folded in (Kiro)

- **`history_size_bytes` is archetype-fidelity, not generic** — it is the workflow event-history size
  (N/A for activities), so it moves to the nullable workflow-fidelity column group alongside
  `execution_time`/`execution_duration`/`history_length`/parent/root ids. (Reflected in the design.md
  schema subsection.)
- **Stripe count `N = 16`**, pinned. `count_from_rollup` fans in 16 rows per `(dimension, value)`;
  `stripe = hash(run_key) % 16`.
- **Reserved-name rejection (Requirement 10.10)** — `register_attr` / the SA-registration path MUST
  reject the reserved system names `archetype`, `status`, `lifecycle_state`, `namespace`, `run_id`,
  `business_id` as user search attributes. Req 10.10 is already task-referenced (tasks 23.3, 24.1); the
  concrete `register_attr` check lands in (and is verified by) the DSQL attr-index step (3).

## Ground-truth sources

- `docs/architecture/050-dsql-storage.md` — DSQL transaction model (`:12-16`, `:28`, `:139-164`), CTE
  discipline (`:238-240`), one-DDL-per-migration (`:257`).
- `crates/tokeira-storage/AGENTS.md` — build-phase rule (`:17-20`), DDL subset (`:23-26`),
  `max_idle_conns == max_conns` survival invariant (`:28-33`).
- v1.31.0 status representation — `reference/DECISION-visibility-status-keyword.md`.
