# Handoff: quiet the single-permit DSQL budget warning (Claude → Codex)

Working doc for the agent taking this slice; delete it in the slice's final commit.
Authored 2026-09-03 by Claude on branch `agent/claude/dsql-budget-warning-handoff`
(base this task's worktree on `origin/main`; the branch carries only this file).

## Mission

Make the DSQL connection-class budget warning mean something for a class whose budget
is one permit, so an idle embedded engine no longer writes it tens of thousands of
times, and keep the warning for the case it exists for: a class that is genuinely
saturated. One pull request in `tokeira-storage` (and `tokeira-config` only if the
embedded defaults move), with a unit test that pins the new rule.

## Evidence (do not re-derive)

Observed from an embedded engine started with `EmbeddedStorageConfig::ManagedDsql`,
`ManagedClusterIntent::CreateOrRecover`, `EmbeddedDsqlLimits::default()`, default
placement, no workflows, held idle for three hours on a dedicated managed cluster
(the idle-burn measurement in the tokeira-cloud repository, task 3.2 of its spec):

- 54,258 WARN lines in 2 h 13 min of log, between the end of startup and the moment
  of reading. Their shapes, with the ANSI escapes stripped:

  ```text
  read_from{partition_id=N fanout=1}: tokeira_storage::dsql::connection: DSQL class budget utilization is above 90% class="projection" total_permits=1 in_use_permits=1   (52,443 lines)
  tokeira_storage::dsql::connection: DSQL class budget utilization is above 90% class="read" total_permits=1 in_use_permits=1          (455)
  tokeira_storage::dsql::connection: DSQL class budget utilization is above 90% class="projection" total_permits=1 in_use_permits=1    (172)
  tokeira_storage::dsql::connection: DSQL class budget utilization is above 90% class="control" total_permits=1 in_use_permits=1       (86)
  load_checkpoint: tokeira_storage::dsql::connection: DSQL class budget utilization is above 90% class="projection" total_permits=1 in_use_permits=1   (13)
  ```

- Roughly 6.5 warnings a second, all from the projection reader's per-partition
  `read_from` span: the reader polls every partition continuously, each poll checks
  out the class's only permit, and one permit in use is 100% utilization.
- Two other warnings appeared once each and are not this slice's subject; they are
  recorded so nobody chases them twice. During startup, while the engine waited on
  cluster creation, `CALL sys.wait_for_job($1)` tripped the slow-statement alert at
  about 9 s twice (expected: that call blocks until the job finishes), and one
  `DSQL connection checkout exceeded leak suspicion deadline class="control"
  call_site="db_class_control" suspect_after_seconds=30` fired, almost certainly the
  control connection held across that same wait. Leave both alone unless the fix for
  the budget warning makes a one-line improvement obvious and safe.

## Where the code is

- The warning: `crates/tokeira-storage/src/dsql/connection.rs`,
  `record_class_metric` (search for `class budget utilization`). It records the class
  metric and warns whenever `in_use / total > 0.9` with `total > 0`. No hysteresis, no
  waiter check, no rate limit.
- Class budgets: `ClassBudgets` in the same file, built from
  `default_allocations(&config.reservoir)`; the per-class permit counts derive from the
  reservoir configuration, which is what gives every class one permit under the
  embedded defaults.
- The leak suspicion scan: `scan` and the warning site near `LEAK_SUSPECT_AFTER` in
  the same file.
- Embedded limits: `EmbeddedDsqlLimits` and its `Default` in
  `crates/tokeira-config/src/embedded.rs` (`max_connections`,
  `concurrent_connection_creations`, `connection_rate_per_second`,
  `connection_burst`).
- The crate's binding rules: `crates/tokeira-storage/AGENTS.md`, in particular the
  `max_idle_conns == max_conns` invariant, which any change to pool sizing must keep.

## The decision

Pick one, and say in the pull request why it is the right one:

1. **Make the warning conditional on real pressure** (recommended). Warn when a
   checkout had to wait for a permit, or when utilization stays above the threshold
   for a sustained window, and never on a class whose budget is a single permit
   unless a waiter is queued. The metric keeps recording utilization unconditionally;
   only the log line changes. This keeps the embedded defaults and their DSQL
   connection-limit reasoning untouched.
2. **Resize the embedded defaults** so the projection, read and control classes have
   more than one permit. Only if the DSQL connection limits the embedded mode
   documents leave room, and only with the reservoir and class derivation understood
   end to end; this is the heavier change and touches `tokeira-config`.

Whichever you pick, the operator-empathy value in `AGENTS.md` is the bar: a warning
that fires on every normal operation is worse than none.

## Constraints

- Stay inside `tokeira-storage` (and `tokeira-config` for option 2). No kernel,
  runtime, edge or engine changes; no spike changes; no dependency changes; build
  with `--locked`.
- Document the WHY at the warning site (`AGENTS.md` §9): what pressure the line now
  signals and why a single-permit class is exempt without a waiter.
- Unit test in the same module, next to the existing `class_budget_*` tests: a
  single-permit class fully in use does not warn without a waiter (or the sustained
  condition is required), and a saturated class with a waiter does. Use the tracing
  test capture the crate already uses elsewhere if one exists; otherwise assert on the
  decision function rather than on log output.
- Finish bar (`AGENTS.md` §10.4) before the pull request; inner loop
  `cargo clippy -p tokeira-storage --all-targets` and
  `cargo nextest run -p tokeira-storage`.
- Branch `agent/codex/<slug>`, trailers per `AGENTS.md` §11, pull request body per
  §10.6. Delete this file in the final commit.

## Acceptance

- The unit test above passes and the existing `tokeira-storage` tests still pass.
- A reviewer can read the warning site and state the condition under which the line
  fires.
- Optional, if a managed cluster is available: rerunning the tokeira-cloud idle-burn
  spike for a few minutes shows no budget warnings while the engine idles. The spike
  and its run are not part of this slice.
