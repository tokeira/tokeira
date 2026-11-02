# Activity dispatch verification and rollout

This change delivers remediation item 1.4 using the approved
[activity-dispatch plan](../../.kiro/specs/activity-dispatch/tasks.md).
The evidence below distinguishes implementation, memory/transport checks and
physical DSQL checks. A compiled live test is not a completed live check.

## Delivery operation inventory

| Operation | Final authority and path | Executable coverage |
|---|---|---|
| Ordinary publication | Storage desired-set delta in the run transaction; notifications only wake/admit disposable offers | Shared atomic and derivation properties |
| Pair/successor publication | Both authoritative runs and their wanted rows commit atomically | Shared pair and successor tests |
| Staged reset/bulk publication | Existing staging barrier; complete canonical set before publication, bounded orphan removal | Shared staging property and bounded bulk contracts |
| Logical deletion/purge | Desired set becomes empty; existing run-key purge removes physical rows | Shared atomic/staging cases |
| Retry eligibility/rules | Execution-owner tracker and fenced transitions; no detached delayed publication | Rule timing and routing properties |
| Poll/discovery | Read-only queue-family pages and hydration; shared eight-query scheduler | Retained-prefix, cursor, scheduler and foreign-mutation audits |
| Ordinary/eager/remote start | Common execution-owner start checks and receipt commit | Competing starts, real HTTP/2 and eager response properties |
| Lost reply/replay | Same UUID and incarnation, read-only fenced observation | Runtime controlled races and physical observation race |
| Acquisition | Candidate walk plus complete home-row walk before Active; generation-scoped repair and restored deadlines | Failed acquisition, multi-page repair and 101-run live fixture |
| Registration/republish | Wake discovery without resetting retained continuation | Routing properties and retired-call audit |
| Legacy spill/drain | Removed runtime producers and consumers | Production source inventory and fail-on-call workload property |
| Legacy disposal | At most 64 physical keys, delete only covered/absent authority; defer invalid copies | Shared disposal and physical replacement race |
| Inspection/sampling | Compatibility repository reads remain non-authoritative | Canonical row/page adapter tests |
| Repository adapters | Transparent trait forwarding, including HistoryWaitRepository | Compile bar and inventory's exact adapter exception |

The production-source audit rejects legacy spill/drain and old due-scan calls in
runtime/edge, with one exact transparent-adapter exception per method. Negative
controls inject each forbidden operation. Separate probes fail if delivery calls
legacy methods or discovery acquires a foreign mutation path. These audits are
complementary to the storage oracles; they are not a proof about arbitrary future
code outside the scanned production sources.

## Reproducible selections

Run the [live-suite preflight and isolation rules](dsql-live-suites.md) first.
All `activity_dispatch_live_` entries fail when the dedicated test URL is absent.
Keep credentials out of command lines, saved logs and source archives.

```sh
cargo nextest run -p tokeira-storage --features dsql-integration --locked --profile dsql-live --test-threads 1 --no-tests fail -E 'test(activity_dispatch_live_)'
# Separate empty database, after schema bootstrap, before any lease/preparation test:
cargo nextest run -p tokeira-runtime --features dsql-integration --locked --profile dsql-live --run-ignored all --no-tests fail -E 'test(activity_dispatch_live_acquisition_spans_recovery_pages)'
# A third isolated empty database; this fixture bootstraps its schema:
cargo nextest run -p tokeira-edge --features dsql-integration --locked --profile dsql-live --run-ignored all --no-tests fail -E 'test(activity_dispatch_live_read_counts)'
```

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
| Fresh start | Queue owner 1; execution owner 1 | One `hot-state-fast` per owner, 2 total |
| Same-request replay | Owner preparation 1; receipt validation 1 | `hot-state-fast`, observation BEGIN, lease key-share read, hot-state `FOR UPDATE`, COMMIT: 5 total |
| Replacement-owner replay | Owner preparation 1; receipt validation 1 | The same five operations, using the replacement's current lease |
| Autonomous discovery | Retained separately, at least one hydration in this fixture | Every observed hydration's SQL remains attributed to discovery |

The isolated live fixture passes these exact counts. Its retained report also
records one discovery hydration and two unscoped state reads during fresh start,
and one unscoped state read during each replay phase. These background reads are
additional to the phase totals above; the report does not erase them.

## Rollout and rollback

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

Verification completed on 2 November 2026 against the approved stopped-upgrade
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
