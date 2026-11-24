# Activity dispatch verification and rollout

Activity dispatch combines execution-owner push with durable queue-home discovery
and an authoritative execution-owner start.
The evidence below distinguishes implementation, memory/transport checks and
physical DSQL checks. A compiled live test is not a completed live check.

## Delivery operation inventory

| Operation | Final authority and path | Executable coverage |
|---|---|---|
| Ordinary publication | Storage desired-set delta commits with the run; the execution owner publishes each still-wanted due incarnation through local admission or bounded `OfferActivityTask` push | Shared atomic/derivation properties and authenticated publication/recovery cases |
| Pair/successor publication | Both authoritative runs and their wanted rows commit atomically; publication uses each final committed image and its execution home | Shared pair/successor tests and publication cases |
| Staged reset/bulk publication | Complete canonical set before the visibility barrier; bounded orphan removal; restored obligations push only after publication and Active ownership | Shared staging/bulk contracts and reset recovery cases |
| Logical deletion/purge | Desired set becomes empty; existing run-key purge removes physical rows | Shared atomic/staging cases |
| Retry eligibility/rules | Execution-owner tracker pushes at the original due time, including already-due registrations, after current rule and deadline checks; no detached delayed publication | Rule timing/routing properties and retry recovery cases |
| Poll/discovery | Read-only queue-family pages and hydration recover lost, refused or unanswered pushes; shared eight-query scheduler and local admission without another push | Retained-prefix, cursor, scheduler, loss recovery and foreign-mutation audits |
| Ordinary/eager/remote start | Volatile execution-home hint routes ordinary polls without a queue-owner run load; owner reloads and checks admission, incarnation, routing, rules and fence before start/receipt commit. Eager start stays in completion and emits no offer | Competing starts, takeover, real HTTP/2, exact read counts and eager response properties |
| Lost reply/replay | Same UUID and incarnation, read-only fenced observation | Runtime controlled races and physical observation race |
| Acquisition | Candidate walk plus complete home-row walk before Active; generation-scoped repair restores obligations, waking due pushes after activation and future pushes at their original deadlines | Failed acquisition, multi-page repair, 101-run live fixture and recovery cases |
| Queue-home retirement | Cancellation/deadline watchers retire ready/in-flight accounting, reservations and discovery work under the broker lock independently of discovery; serving-generation checks refuse old admission/takes and cleanup preserves replacements | Retirement and queue-movement cases with late deadlines and results |
| Registration/republish | Wake discovery without resetting retained continuation | Routing properties and retired-call audit |
| Legacy spill/drain | Removed runtime producers and consumers | Production source inventory and fail-on-call workload property |
| Legacy disposal | At most 64 physical keys, delete only covered/absent authority; defer invalid copies | Shared disposal and physical replacement race |
| Inspection/sampling | Compatibility repository reads remain non-authoritative | Canonical row/page adapter tests |
| Repository adapters | Transparent trait forwarding, including HistoryWaitRepository | Compile bar and inventory's exact adapter exception |

Prompt push and retirement are tested obligations, not proved latency bounds.
An offer acknowledgement confers no start authority; a committed start and its
receipt survive retirement of the queue-home hint.

The production-source audit rejects legacy spill/drain and old due-scan calls in
runtime/edge, with one exact transparent-adapter exception per method. Negative
controls inject each forbidden operation. Separate probes fail if delivery calls
legacy methods or discovery acquires a foreign mutation path. These audits are
complementary to the storage oracles; they are not a proof about arbitrary future
code outside the scanned production sources.

## Reproducible selections

Run the [live-suite preflight and isolation rules](dsql-live-suites.md) first.
All `activity_dispatch_live_` and `activity_queue_home_live_` entries fail when
the dedicated test URL is absent.
Keep credentials out of command lines, saved logs and source archives.

```sh
cargo nextest run -p tokeira-storage --features dsql-integration --locked --profile dsql-live --test-threads 1 --no-tests fail -E 'test(activity_dispatch_live_)'
# Separate empty database, after schema bootstrap and before any lease/preparation test; it prepares 4096 homes:
cargo nextest run -p tokeira-runtime --features dsql-integration --locked --profile dsql-live --run-ignored all --no-tests fail -E 'test(activity_dispatch_live_acquisition_spans_recovery_pages)'
# Isolated empty eight-home database; read counts bootstrap schema and placement first:
cargo nextest run -p tokeira-edge --features dsql-integration --locked --profile dsql-live --run-ignored all --no-tests fail -E 'test(activity_dispatch_live_read_counts)'
# The same cleaned eight-home database, in this order; each prepares placement before any lease:
cargo nextest run -p tokeira-engine --features dsql-integration --locked --profile dsql-live --run-ignored all --no-tests fail -E 'test(activity_queue_home_live_recovery)'
cargo nextest run -p tokeira-engine --features dsql-integration --locked --profile dsql-live --run-ignored all --no-tests fail -E 'test(activity_queue_home_live_retirement)'
cargo nextest run -p tokeira-engine --features dsql-integration --locked --profile dsql-live --run-ignored all --no-tests fail -E 'test(activity_queue_home_live_competing_starts)'
cargo nextest run -p tokeira-engine --features dsql-integration --locked --profile dsql-live --run-ignored all --no-tests fail -E 'test(activity_queue_home_live_takeover)'
```

Run these selections serially in separate commands; a filter does not establish
order. The four engine entries require an already bootstrapped isolated database,
prepare eight homes before taking leases, and purge their runs and release their
leases after each case. Each executes 100 generated cases through authenticated
internal gRPC. Keep this database separate from the 4096-home acquisition and
shared storage fixtures.

The shared storage entries execute 100 generated cases each for complete derivation, atomic commits,
pairs, ordered and malformed traversal, repair, staging, observation and disposal.
Additional physical races hold repair/replay/disposal transactions across a
committed replacement. A separate limit fixture requires DSQL SQLSTATE `54000`
for an oversized ordinary transition and checks that authority and dispatch
remain unchanged. The query-plan fixture seeds 16,384 rows, including deep
2,048-row equal-time groups and 8,192 slots in one run; it checks the production
SQL builders and exact traversal, not a separately written query. Set
`TOKEIRA_ACTIVITY_DISPATCH_PLAN_OUTPUT` to retain plans and
`TOKEIRA_ACTIVITY_READ_REPORT` to retain phase counts. EXPLAIN does not measure
storage-engine rows examined.

The read report counts logical authoritative-state observations and their SQL
statements. Discovery hydration and unscoped background observations remain in
the report, separately from start/replay phases. It does not claim to count every
SQL statement for history payloads, routing registries or the write transaction.
Exact expected operations are asserted per phase in the executable fixture:

| Phase | Logical state observations | Expected physical state-read statements |
|---|---:|---|
| Fresh start, no conflict | Queue owner 0; `execution-owner/activity-start` 1 | Inline: 1 `hot-state-fast`; split: 5 (`begin-read-snapshot`, `hot-state-fast`, `hot-state-snapshot`, `value-parts`, `commit-read-snapshot`) |
| Fresh start, one injected conflict | Queue owner 0; `execution-owner/activity-start` 2 | The same operations per load: inline 2; split 10 |
| Same-request replay | `execution-owner/activity-start` 1; `execution-owner/activity-replay` receipt validation 1 | Inline: 5 total; split: 10 total |
| Replacement-owner replay | `execution-owner/activity-start` 1; `execution-owner/activity-replay` receipt validation 1 | Inline: 5 total; split: 10 total, using the replacement's current lease |
| Discovery hydration | `activity-discovery/resolve`: at least 1 for a discovered start; 1 measured | Inline: 1; split: 5, with the same load operations as fresh start; counted against discovery, not start |

The fixture asserts all 16 combinations of pushed/discovered delivery, warm/cold
caches, inline/split state and zero/one injected conflict. Warm and cold starts
have identical counts. Both replay paths run for every combination, without an
injected replay conflict. Receipt validation uses `begin-activity-observation`,
`activity-observation-state`, `activity-observation-fence` and
`commit-activity-observation`, plus `value-parts` for split state; the owner load
accounts for the remaining statements in each replay total.

Three live runs produced identical read shapes. Their background phases remain
separate from the start totals:

| Background phase / scope | Logical reads | Inline SQL statements | Split SQL statements |
|---|---:|---:|---:|
| Publication / `execution-owner/lane-cache-or-retry` | 1 | 1 | 1 |
| Publication / `execution-owner/preparation` | 1 | 1 | 1 |
| Publication / `normal-discovery` | 1 | 1 | 1 |
| Publication / `outside-start` | 2 | 2 | 2 |
| Acquisition and offer / `outside-start`, discovered | 1 | 1 | 16 |
| Acquisition and offer / `outside-start`, pushed | 2 | 2 | 21 |
| Acquisition and offer / `execution-owner/admission-cache`, warm only | 1 | 1 | 5 |

Publication is identical in all 16 combinations; each read is one `hot-state-fast`
statement. During acquisition and offer, each split read adds
`begin-read-snapshot`, `hot-state-snapshot` and `commit-read-snapshot` to
`hot-state-fast`. Split state also adds one `value-parts` statement to the warm
admission-cache read, and 12 to `outside-start` when discovered or 13 when pushed.
Cold acquisition has no admission-cache read. These are measured background
counts, additional to the asserted start/replay totals.

## Rollout and rollback

Activity queue-home push adds no upgrade or rollback step: it uses the existing
peer credentials and adds no migration, persisted format or installation step.
An older peer answers `Unimplemented`, which ends the push attempt, and discovery
covers the task. The steps below are unchanged.

1. Stop every old runtime/edge node and retain a recoverable pre-upgrade database.
   Mixed-version serving is unsupported for this cutover.
2. Apply forward migrations V128–V132 in order and wait for both asynchronous
   index builds. The locked V1–V127 prefix is unchanged. Validate the schema
   contract before starting new nodes.
3. Deploy TKW4/snapshot-8 capable binaries together. Existing positional state
   payloads and supported legacy formats remain readable; tag 7 carries activity
   start receipts. Acquisition repairs legacy NULL queue digests and missing or
   obsolete slots before serving.
4. Remove `RuntimeConfig.backlog`, `BacklogConfig` (activity grace window, scan
   interval, drain interval and batch limit) and
   `ActivityTimeoutScannerConfig.max_dispatch_reconciliations_per_scan` from
   embedded callers. Timeout and retry-eligibility scanning remain required.
5. Retain `dispatch_backlog`. Bounded disposal can defer unknown/corrupt copies;
   deferred rows are not delivery authority and must not be blindly dropped.

Once TKW4 state or a version-8 snapshot is written, an old reader is not a
rollback target, even if that state has no activity start receipt. Roll back
with a binary that understands the new schema/state formats, or restore the
pre-upgrade database with its matching binaries after stopping every node. Do
not reverse migrations or reuse old volatile offers across the cutover.

## Verification receipt

Verification completed against the approved stopped-upgrade
contract. Source archives/manifests, immutable nextest archive identities, result logs,
query plans, read reports and failed-attempt receipts are retained in the private
delivery evidence. Resource identities and credentials are excluded from this
public receipt. The task base is
`bc5fc2070943b640a76dbc0c3147de990f207953`; the PR records the exact delivery
head. Documentation-only completion updates follow the tested code.

### Workspace and model checks

The final code archive passes local nightly formatting, remote dated-nightly
format verification, all-target storage/runtime/edge clippy with
`dsql-integration`, and the complete enforced workspace bar:

```sh
cargo +nightly fmt --all
cargo clippy -p tokeira-storage -p tokeira-runtime -p tokeira-edge --all-targets --features dsql-integration --locked -- -D warnings
cargo lint --locked
cargo check --workspace --locked
cargo nextest run --workspace --locked --no-fail-fast
cargo test --workspace --doc --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked
```

Nextest passes all 4,194 selected tests. The default suite leaves
`sdk_echo_roundtrip_against_post_sync_tokeirad` and
`sdk_scoped_worker_roundtrip_uses_production_authorization_bootstrap` skipped;
this receipt does not claim those SDK checks. One doctest passes and 21 examples
are ignored. Locked offline dependency bans/licenses/sources, the full offline
Markdown link check, all 18 property tags and required task tags pass. No separate
functional-conformance campaign is claimed. Hosted Rust CI jobs remain disabled
by existing repository policy; the full bar ran on the disposable devbox.

`tla 0.21.2` passes six positive dispatch-handoff/repair configurations. All nine
negative controls report their intended violations. Commands use
`--max-states 20000000 --max-depth 10000 --check-liveness`; model actions and
configurations are unchanged. The refinement mapping states the proof boundary.

### Physical DSQL results

All selections use the `dsql-live` profile, one process/test at a time, no retry,
`--run-ignored all --no-tests fail --no-fail-fast --success-output final`, and
an exact test filter from the immutable archive. Acquisition and read counts each
use their own empty database. The final traversal reruns use another fresh database.
Each bootstrap executes
`test(=token_zero_empty_ledger_state_converges_through_the_embedded_target)`.

Exact activity selections, each supplied separately through `-E`:

```text
test(activity_dispatch_live_atomic)
test(activity_dispatch_live_pair)
test(activity_dispatch_live_repair)
test(activity_dispatch_live_staged)
test(activity_dispatch_live_replay_observation)
test(activity_dispatch_live_derivation)
test(activity_dispatch_live_legacy_disposal)
test(activity_dispatch_live_ordered_pages)
test(activity_dispatch_live_malformed_pages)
test(activity_dispatch_live_competing_repair_and_replay)
test(activity_dispatch_live_fenced_repair_and_observation)
test(activity_dispatch_live_disposal_replacement)
test(activity_dispatch_live_physical_refusal_is_atomic)
test(activity_dispatch_live_query_plans)
test(activity_dispatch_live_acquisition_spans_recovery_pages)
test(activity_dispatch_live_read_counts)
```

| Successful selection | Cases / result | Archive |
|---|---|---|
| Atomic maintenance, pairs, repair, staged publication, replay observation | 100 generated cases each | A |
| Derivation and conditional legacy disposal | 100 generated cases each | B |
| Ordered and malformed traversal | 100 generated cases each | C |
| Competing repair/replay; lease renewal/takeover/release | 100 controlled cases each | B |
| Disposal replacement race | 100 controlled cases | A |
| Physical ordinary-transaction limit | 3,001 activities refused with SQLSTATE `54000`; no partial state or rows | A |
| Production query plans | Complete queue/home seeks at offsets 0/1,792/4,096/7,936; 8,192 rows in exactly 129 queue pages | C |
| Acquisition across recovery pages | 101 runs, 229 slots, 129-slot run with 2 MiB split state; exact deadlines and serving barrier | B |
| Authenticated fresh/replay/handoff reads | Two logical observations each; phase SQL counts 2/5/5; background reads retained separately | B |

The final acquisition fixture passes in 739 seconds and its schema bootstrap in
793 seconds. The read-count fixture passes in 34 seconds after a 771-second
bootstrap. Shared and final traversal bootstraps pass in 874 and 855 seconds.
The final plan, ordered traversal and malformed traversal selections pass in
687, 507 and 440 seconds respectively. Legacy disposal passes all 100 cases
in 1,081 seconds within the unchanged
20-minute limit. Repair/replay and lease race reruns pass in 277 and 154 seconds.

| Archive | Source archive SHA-256 | Nextest archive SHA-256 |
|---|---|---|
| A | `4286e78d78e80c5a3f15f5a84dfe142f881830aede02f39524a8865be22d649e` | `0b1fdb73b15238a960f3d3a710a13a65c9cee24a5f85d1e96194cfe6f8f7afc4` |
| B | `6d212e5329a60832768f244cd7be4733c5473d6fc9d22a96cc3aac0062802967` | `9d5d72f0184f9f84a2afe132837081a374a50386b72b5df2636522f9a273f651` |
| C | `dc372deeb74ada2cf5cba24de961a6ddca8c68c328b35c747664cdbcc59eac08` | `f88f9b4018f678f67d57f9cf175a4b733bf3f34ce2b925b11ceff11f481742ab` |

These are distinct receipts, not a claim that every selection ran on one archive.
Manifest comparisons preserve the unchanged mutation, repair, disposal and
observation implementations. The only production change after B is the tied-time
continuation predicate in `due_query`; C separately verifies that query through
production plans and both traversal properties. Acquisition's home walk and the
one-activity read-count fixture do not use the changed continuation branch. The
full workspace bar passes on C. All migration inputs are identical across A–C.

### Retained failures and cleanup

Earlier failures remain recorded rather than counted as passes. The first final
plan selection exposed a redundant timestamp range that left the cursor key in a
residual storage filter. Comparing the fixed cursor time with the cutoff restores
the full indexed equality/key seek. The corrected final plan and traversal results
above close that failure. EXPLAIN does not report storage-engine rows examined.

Two physical replay/fence fixtures originally expected an error from the public
adapter, which intentionally maps SQL serialization conflicts to `Ok(false)` for
an owner reread. Corrected fixtures require the raw SQLSTATE `40001` before that
mapping and preserve the 100-case count and authoritative-state assertions.
Production replay remains unchanged. A disposal attempt timed out; batching only
its legacy setup writes preserves its production path, cases and deadline. Earlier
schema/inventory/telemetry and restart-fixture failures, sandbox-refused loopback
runs and the original devbox capacity failure also retain separate receipts.

All five task-owned ephemeral DSQL clusters and both disposable devboxes are
deleted. Cluster deletion waiters and fresh devbox listings verify absence.
Redacted reports, source archives and source/binary/result manifests were retained
locally before deleting the boxes. No production deployment was performed.

## Security advisory follow-up

The approved CI follow-up updates `pulldown-cmark` from 0.13.5 to 0.13.6 for
RUSTSEC-2026-0433 and `zip` from 8.2.0 to 9.0.4 for RUSTSEC-2026-0429. ZIP adds
its required `nix` 0.31.3 transitive dependency. The three obsolete ZIP extraction
exceptions are removed from `deny.toml`; no advisory exception is added.

The Dagger SDK, engine and CLI remain on the existing release. The narrow vendor
manifest patch and its retirement condition are recorded in
[the vendoring notes](https://github.com/tokeira/tokeira/blob/b7d38fcb04c165f402ecf6de477d84634fc9c7ed/vendor/README.md). SDK Rust sources, application code,
reviewed activity specifications and the live DSQL paths are unchanged.

The tested source archive is
`c31d4c040d0ed7b5c836581bdeafd0779e412b0d093cd00a2cc267d767c92547`.
The full workspace bar passes again on Rust 1.97.1: formatting, lint, check,
4,194 nextest tests, one doctest and warning-free documentation. The two existing
SDK skips and 21 ignored doctest examples remain unchanged. Source-manifest
verification passes before and after the bar. Current RustSec advisories,
dependency bans, licenses, sources and offline Markdown links all pass.

An isolated copy of the vendored SDK also passes 33 existing archive, preflight
and launch tests on Rust 1.97.1. Its source is unchanged; the four unpackaged
compile-time fixtures come from `iw/dagger` at
`sdk/rust/v1.0.0-beta.11.rust.3`. The fixture uses a separate lockfile seeded from
the tested workspace and the local macro companion. The exact selection is:

```sh
cargo +1.97.1 nextest run --locked --lib --no-fail-fast -E 'test(provisioning_tests::) | test(preflight_tests::) | test(launch_tests::)'
```

The selection reports 120 other tests skipped; it is not a claim about the full
SDK suite. Its archive property checks bounded extraction, duplicate members,
symlink refusal and path confinement with 128 generated cases. The original
live DSQL and model receipts above remain the evidence for the unchanged activity
implementation; those campaigns were not repeated for this dependency correction.

Both disposable boxes used for this follow-up are deleted and fresh listings
verify their absence. Logs, fixture lockfile and source manifests are retained.
Only this receipt text was added after the archived run; executable inputs are
unchanged.

## Queue-home delivery verification receipt

The following selections passed on DSQL. Acquisition and the lost-publication
and backlog checks used a 4096-home database after schema bootstrap. Activity
inputs used a separate eight-home database after schema bootstrap. Read counts
bootstrapped an isolated eight-home database; the four engine selections followed
on that database in the order below, with placement prepared before any lease.

| Selection | Result | Cases |
|---|---|---|
| Schema bootstrap | Passed on the 4096-home and activity-input databases | 1 bootstrap test per database |
| `activity_dispatch_live_acquisition_spans_recovery_pages` | Passed | 1 test: 101 runs, 229 slots, including a 129-slot run with split state |
| `activity_dispatch_live_lost_publication_offered_by_next_pass` | Passed; discovery delivered to the parked poll within the asserted bound | 1 test |
| `workflow_dispatch_live_lost_publication_offered_by_next_pass` | Passed; discovery delivered to the parked poll within the asserted bound | 1 test |
| `activity_dispatch_live_backlog_counts_rows_without_pollers` | Passed | 1 test |
| `dsql_activity_inputs` | Passed | All 3 tests |
| `activity_dispatch_live_read_counts` | Passed; identical read shapes in all three runs | 16 combinations per run, each with fresh start, same-request replay and handoff replay |
| `activity_queue_home_live_recovery` | Passed | 100 generated cases |
| `activity_queue_home_live_retirement` | Passed | 100 generated cases |
| `activity_queue_home_live_competing_starts` | Passed | 100 generated cases |
| `activity_queue_home_live_takeover` | Passed | 100 generated cases |

Prompt push and retirement are tested obligations, not proved latency bounds.
Storage-only selections were not rerun because the storage implementation is
unchanged; their existing receipt above remains separate evidence.
