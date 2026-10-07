# Workflow dispatch implementation evidence

## First implementation PR

Original base: `11ce84109d55167e4b737ac0224dc4004f3dbbbf`.
Rebased PR base: `8daa6040d3eef518d92d0ba5e03cee6cf7c9a273`.
Implementation commit: `94272ae1e055b4a122e8e6c19fe9591a1016ff70`.
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
- Runtime regressions check an unchanged start submission across OCC retries and
  publication/token identity after resuming a retained task. No start-result cache
  or new retry policy is introduced.
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

The first run preceded the expanded reset/routing regressions. Only the final
run establishes those additions. A temporary nextest profile extends the live
suite timeout to 20 minutes; repository nextest configuration is unchanged.

```bash
cargo nextest run -p tokeira-storage --features dsql-integration --locked --test-threads 1 -E 'test(workflow_dispatch_live_ordered_pages)'
cargo nextest run -p tokeira-storage --features dsql-integration --locked --test-threads 1 -E 'test(workflow_dispatch_live_atomic_reference_traces)'
cargo nextest run -p tokeira-storage --features dsql-integration --locked --test dsql_connector_iam
```

## Workspace validation

All AGENTS.md §10.4 commands passed on 2026-10-07:

| Command | Result |
|---|---|
| `cargo +nightly fmt --all` | Passed |
| `cargo lint --locked` | Passed |
| `cargo check --workspace --locked` | Passed |
| `cargo nextest run --workspace --locked --no-fail-fast` | 3,695 passed; 2 existing ignored SDK integration tests |
| `cargo test --workspace --doc --locked` | Passed: 1 executable example; 21 existing ignored examples |
| `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked` | Passed |

Affected-crate all-target Clippy also passed with
`--features tokeira-storage/dsql-integration`. The focused runtime/storage
workflow-dispatch filter passed 13 tests; kernel/storage suites previously passed
801 tests. The live gates were unset in those local runs; the credentialed results
above are the live evidence. No completion-bar command was omitted.

The PR-boundary rebase added only the migration-retry specification and an
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

The process environment selects the already-installed ARM `protoc`; the default PATH resolves an Intel
binary that cannot run on this host. Shared build/cache/toolchain settings remain
unchanged. Test linking reports existing native-object deployment-target warnings.
An unset live-test gate is never counted as live DSQL evidence.

The full run exposed a pre-existing timing race in the backlog routing property:
50 ms polls could disappear while it synchronized both waiter registrations.
Its test-only deadlines now outlive nextest's termination deadline, retaining the
waiters until the test drains them. Delivery logic and operational deadlines are
unchanged.
