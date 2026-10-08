# Workflow dispatch implementation evidence

## Second implementation PR: delivery components and deadline recovery

Original base: `eb80ab4aa839010c26708d75d4f1a62492bb92ac`.
Rebased base: `445b3f80cec731be34726ab465ea9a2262aa7286`.
Implementation commit: `6f569b23a986357844766706421b7644f16478e4`.
The PR records its final head.

Tasks 5–7 are constructed only through the temporary internal delivery choice;
ordinary runtime constructors still use notification, backlog and recovery
delivery. Tests enable the new path explicitly, including a poll that discovers
and starts a task with no publication and a different execution home. The cutover
must remove this choice together with complete reconciliation and the serving
gate. No migration, dependency or lockfile change is introduced.

The offer pool shares incarnation deduplication and capacity across notifications
and discovery. Takes retain identity until a definitive result or the five-second
lease ends; ready retention is also five seconds. Reservations account for pages
in flight, are bounded by the home cap, and release unused capacity on cancellation.
Discovery preserves continuation across one-page slices, advances across rejected
coordinates and incompatible/held rows, and discards continuation on a capacity
stop. Poll guards reference-count demand, coalesce Live ranges, retain idle passes
for renewal and retire only idle registrations under pressure. The wire sticky
queue kind reaches registration independently of the optional normal queue name.

Task 8 is active with existing delivery. Recovery derives the pending normal
task's absolute deadline even after affinity reset. Installation, activation and
cleanup check an acquisition's epoch and local generation; failed, cancelled and
superseded sweeps cannot affect a successor's workflow-task tracking. Timeout
results retire the submitted revision, preserving a concurrent start or retry.
Post-commit tracking installs start-to-close before returning the committed start,
including when its reply is lost. Temporal's durable sticky timer behavior is
verified in `service/history/workflow/task_generator.go:419–444 @ v1.31.0`;
matching's discarded NotFound results are verified in
`service/matching/matching_engine.go:767–778 @ v1.31.0`.

The generated contracts configure at least 100 cases per test:

- Property 3: shared memory/DSQL ordered traversal, tuple ties, varied page sizes,
  read-only state/row checks, multiple execution homes and moved-behind-cursor
  rediscovery. Existing forced-collision contracts complement generated runtime
  filtering of held and incompatible candidates.
- Property 4: 129–269-row prefixes across multiple slices, per-slice examination
  and admission bounds, and generated capacity-stop/release turns among competing
  ranges. Direct checks cover all scheduler defaults and full home/queue capacity.
- Property 5: poll/renewal/cancellation counts, coalesced ranges, home generation
  replacement, overlapping homes, retirement and registry restart. Fixed runtime
  tests prove duplicate offers cannot commit two starts and idle pressure preserves
  active registrations. Eviction cancels an in-flight page and invalidates its
  completion identity, so a late result cannot overwrite a recreated registration.
- Property 6: notification/page duplicates, takes, cancellation, ambiguous results,
  expiry and retirement against an independent lease model; runtime restart with
  an unstarted offer or a lost committed-start reply. Repository transition counts
  and dispatch pages distinguish volatile delivery from the single durable start.
- Property 7: shared memory/DSQL deadline derivation for affinity present/reset,
  paused, started, closed, speculative and legacy states; valid timeout creates
  normal discoverable replacement work. Channel-controlled success and stale
  rejection preserve a replacement installed during submission. Fixed tests use
  injected time to verify exact-deadline firing only after Active, and pause actual
  sweeps in both acquisition paths across failure, abort, relinquish and replacement.

Tasks 9, 15, 17 and 18 remain incomplete. Query-plan observations below are evidence
for Task 15, not completion of its transaction-boundary and read-cost verification.
Requirements 8.4–8.6 still require the transaction-local competing-owner lease fence.

### Workspace validation

The branch was rebased once onto the base above, with no conflicts. The full
`AGENTS.md` §10.4 bar was rerun after rebasing and correcting the shared home-page
test. Nothing in the bar was omitted:

| Command | Result |
| --- | --- |
| `cargo +nightly fmt --all` | Passed |
| `cargo lint --locked` | Passed |
| `cargo check --workspace --locked` | Passed |
| `cargo nextest run --workspace --locked --no-fail-fast` | 3,725 passed; 2 existing ignored |
| `cargo test --workspace --doc --locked` | 1 passed; 21 existing ignored |
| `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked` | Passed |

The DSQL integration feature also passed all-target Clippy with `-D warnings`.
Focused discovery tests, the paused-acquisition recovery checks, Markdown relative
link checks and `git diff --check` passed. No dependency, lockfile, migration or
proptest seed changes were introduced.

One earlier workspace run hit the attempt assertion in the unchanged
`backoff_interval_rule_between_publication_and_start_pauses_durably` test: the
offer-only poll returned a stale attempt-one candidate. That API leaves durable
revalidation to start. Its test and activity paths remain unchanged; 30 isolated
repetitions and the final full workspace run passed. Earlier runs also reported
isolated leaked-test warnings; the final full run had none.

### Live validation and query plans

Live checks use ephemeral Aurora DSQL clusters in `eu-west-1`, profile `default`,
with deletion protection disabled and tags `purpose=tokeira-test` and
`task=workflow-dispatch-2`. The lifecycle runner deletes its cluster after success
or failure. The migration runner owns schema-conflict retries and ASYNC index
readiness; the fixture no longer has a separate migration retry loop.

The first cluster was created at **2026-10-08 18:56:16 UTC** and deletion was
confirmed at **2026-10-08 19:27:25 UTC**. Five of six tests passed:

| Live contract | Result | Duration |
| --- | --- | ---: |
| Atomic reference traces, 100 cases | Passed | 898.456 s |
| Property 3 ordered traversal, 100 cases | Passed | 321.053 s |
| Property 7 sticky recovery, 100 cases | Passed | 25.701 s |
| Ordered queue/home pages | Fixture isolation failure | 10.563 s |
| Queue/home query plans | Passed | 140.870 s |
| Reset uses execution home | Passed | 6.609 s |

The failed home-page assertion assumed that another shard had no rows. The
generated multi-home fixtures legitimately leave rows there. The corrected
contract walks the complete other-home range and rejects only this fixture's run
keys, preserving the exclusion check without assuming an empty shared database.
The memory counterpart passes with the corrected assertion.

The second cluster was created at **2026-10-08 19:29:50 UTC** and deletion was
confirmed at **2026-10-08 19:49:07 UTC**. All three selected checks passed:

| Live contract | Result | Duration |
| --- | --- | ---: |
| Property 3 ordered traversal, 100 cases, including fresh schema bootstrap | Passed | 895.863 s |
| Corrected ordered queue/home pages, after multi-home fixtures | Passed | 13.286 s |
| Final queue/home query-plan fixture | Passed | 134.352 s |

Property 7 and the unchanged atomic/reset contracts were not repeated on the
second cluster; their successful first-run results stand. No cluster was retained.

The first plan fixture added **8,192 rows**, producing **9,433 total rows**:
128 queue families, 32 rows per Live/Exact range, and 32 execution homes with
256 added rows each. Queue selectivity was **32 / 9,433 = 0.3392%**; the selected
home had **463 / 9,433 = 4.9083%**. The actual repository SQL and bound arguments
were used for first and continuation pages in both routing modes and for home
pages.

All four queue plans used `Index Scan using idx_workflow_dispatch_queue` with
range-coordinate equality predicates in `Index Cond`. Continuations applied
`ROW(priority_key, scheduled_at, run_key) > ROW(...)` as a **Filter**, not an
index lower bound. Both home plans used `Index Only Scan using
idx_workflow_dispatch_home`; continuation included `run_key > ...` in
`Index Cond`. None used a sequential table scan. These are `EXPLAIN` planner
observations, not measured rows read or latency bounds; continuation filtering
and selectivity remain inputs to Task 15's tuning and read-cost work.

The second plan fixture again added **8,192 rows**, producing **9,517 total rows**.
Its queue range selectivity was **32 / 9,517 = 0.3362%**; the selected home had
**448 / 9,517 = 4.7074%**. The index choices and continuation treatment were the
same as in the first run. [Recorded plans](query-plans-20261008.md) preserve the
complete final `EXPLAIN` output apart from fixture UUID/digest placeholders.

## First implementation PR

Original base: `11ce84109d55167e4b737ac0224dc4004f3dbbbf`.
Rebased PR base: `8daa6040d3eef518d92d0ba5e03cee6cf7c9a273`.
Implementation commit: `94272ae1e055b4a122e8e6c19fe9591a1016ff70`.
Review implementation commit: `27639f90f0c63825417ac7e764445b5fce4a2374`.
The PR records its final head revision.

This increment adds checked kernel incarnation allocation, shared dispatch
derivation, V074–V076, atomic maintenance in both stores, and read-only queue/home
pages. Existing notification, backlog, and acquisition delivery remain active.
Discovery scheduling, bounded offers, reconstruction, serving gates, and backlog
retirement are subsequent increments. Requirements 8.4–8.6 and Property 9 remain
deferred until the transaction-local lease fence is implemented and verified.

## Executable contracts

- Kernel Property 2 runs 100 generated traces over retained failures, pause/resume,
  priority supersession, timeout replacement, delayed old offers, and repeated
  starts. It checks attempts, transient history suppression, virtual schedule IDs,
  publication identity, and checked exhaustion. Temporal references are
  `service/history/workflow/workflow_task_state_machine.go` and
  `service/history/api/recordworkflowtaskstarted/api.go` at `v1.31.0`.
- Storage Property 1 runs 100 generated traces through the same independent
  reference model on memory and real DSQL. It alternates commit entry points,
  generates reset boundaries, replaces stale prior rows, and covers running,
  started, paused, speculative, absent, and closed pending states; routing,
  priority/fairness, sticky affinity and affinity reset; duplicate/CAS rejection;
  and fenced deletion. An unencodable dispatch identity fails after DSQL's
  hot-state write and verifies transaction rollback. Memory rejects it before
  mutating its maps.
- Direct contracts check ordered first/continuation pages, Exact/Live selection,
  absent versus empty build coordinates, long queue names, complete home scans,
  sticky inclusion, and forced digest collisions. Memory additionally verifies
  multi-home queue scans and snapshot reconstruction. Reset tests seed stale
  successor rows before materialization, including successors with no wanted row.
  A live eight-shard reset regression selects a successor whose run-hash shard
  differs from its execution home and checks hot-state, timer, and dispatch
  placement immediately after materialization. Both stores also check that
  legacy listings deliver speculative tasks without creating dispatch rows.
- Runtime regressions check an unchanged start submission across OCC retries and
  publication/token identity after resuming a retained task. Review regressions
  also check an empty, successful poll while paused and recovery publication of
  a stored speculative task without a durable dispatch row. Both failed on the
  original implementation and passed after the delivery fixes. No start-result
  cache or new retry policy is introduced.
- Fixed digest vectors, microsecond timestamp normalization, schema/index shape,
  migration-prefix integrity, and existing frozen-state tests complement the
  generated traces. No dependency or state-extension layout changes are made.

The page tests do not complete Property 3's later runtime-filtering and mutation
traces. Query-plan/read-cost evidence, aggregate transaction-limit boundaries,
reconciliation, and complete end-to-end loss tests remain in their later tasks.

## Real Aurora DSQL

Runs use profile `default`, Region `eu-west-1`, newly created ephemeral clusters
with deletion protection disabled and tags `purpose=tokeira-test` and
`task=workflow-dispatch-1`. Cleanup is installed before running tests. The suites
use SQLx through the repository's dedicated URL gate and embedded migration
runner, await ASYNC index readiness, and run serially. The separate IAM connector
test exercises the production connection factory. No PostgreSQL substitute is
used. Resource identities and credentials are omitted.

| Run | Created (UTC, 2026-10-07) | Deleted (UTC) | Executed results |
|---|---|---|---|
| Development contracts | 21:02:49 | 21:11:04 | Ordered pages/reset/collisions passed (41.430 s); 100 atomic traces passed (322.377 s); IAM connector passed (0.473 s). |
| Expanded final contracts | 21:13:08 | 21:26:29 | Ordered pages/routing/home/reset/collisions passed (45.742 s); 100 atomic traces with generated reset cases and both commit entry points passed (342.968 s); IAM connector passed (0.554 s). |
| Review regressions | 22:13:48 | 22:22:09 | Eight-shard reset placement passed (38.676 s); ordered pages including speculative legacy delivery passed (11.965 s); 100 atomic traces passed (347.510 s); IAM connector passed (0.833 s). |

The first run preceded the expanded reset/routing regressions. The second run
establishes those additions; the third adds the review regressions and reruns
the existing contracts. A temporary nextest profile extends the live
suite timeout to 20 minutes; repository nextest configuration is unchanged.

```bash
cargo nextest run -p tokeira-storage --features dsql-integration --locked --test-threads 1 -E 'test(workflow_dispatch_live_ordered_pages)'
cargo nextest run -p tokeira-storage --features dsql-integration --locked --test-threads 1 -E 'test(workflow_dispatch_live_atomic_reference_traces)'
cargo nextest run -p tokeira-storage --features dsql-integration --locked --test-threads 1 -E 'test(workflow_dispatch_live_reset_uses_execution_home)'
cargo nextest run -p tokeira-storage --features dsql-integration --locked --test dsql_connector_iam
```

## Workspace validation

All AGENTS.md §10.4 commands passed on 2026-10-07:

| Command | Result |
|---|---|
| `cargo +nightly fmt --all` | Passed |
| `cargo lint --locked` | Passed |
| `cargo check --workspace --locked` | Passed |
| `cargo nextest run --workspace --locked --no-fail-fast` | 3,697 passed; 2 existing ignored SDK integration tests |
| `cargo test --workspace --doc --locked` | Passed: 1 executable example; 21 existing ignored examples |
| `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked` | Passed |

Affected-crate all-target Clippy also passed with
`--features tokeira-storage/dsql-integration`. The focused runtime/storage
workflow-dispatch filter passed 16 tests, including three gated tests that return
without connecting when unset; kernel/storage suites previously passed
801 tests. The live gates were unset in those local runs; the credentialed results
above are the live evidence. No completion-bar command was omitted. The review
run repeated the entire bar. Two passing tests were marked leaky in the parallel
workspace run (`lock_diff_classifies_supply_chain_inputs` and
`embedded_metric_manifest_is_valid`); both passed without leak warnings in a
serial nextest recheck.

The initial PR-boundary rebase added only the migration-retry specification and an
internal changelog fragment. Comparing pre/post-rebase trees confirmed identical
code and build inputs. The full bar was not repeated for this documentation-only
delta; Markdown and whitespace checks were repeated after rebase.

The two ignored SDK integration tests start separate server/worker or scoped
worker/JWKS setups and remain outside the default suite. Existing ignored
documentation examples retain their declared opt-in status. Later feature tasks,
including query plans and competing-owner tests, are not marked complete here.

Offline Markdown links and `git diff --check` passed. The local link check
excludes the ignored `.tokeira-build` scratch workspace generated by integration
tests, whose copied README refers to a document outside that scoped build.

An unset live-test gate is never counted as live DSQL evidence.

The initial full run exposed a pre-existing timing race in the backlog routing property:
50 ms polls could disappear while it synchronized both waiter registrations.
Its test-only deadlines now outlive nextest's termination deadline, retaining the
waiters until the test drains them. Delivery logic and operational deadlines are
unchanged.
