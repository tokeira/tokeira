# Workflow dispatch implementation evidence

## Acquisition repair ownership fence

Live-tested implementation revision: `4f2bee04cfc0bdfadb8c86096eb0b034a5475faf`.

Repair now takes the acquisition's captured `WriteFence`. DSQL checks the
execution-home owner and epoch with the existing `FOR KEY SHARE` protocol as the
first statement of the repair transaction; memory checks under the same mutex as
the authoritative read and derived write. Whole-call OCC retries retain the
original fence. `LeaseFenceLost` is terminal. Forwarding repositories preserve the
argument. There is no new migration, dependency, lockfile or lease protocol change.

The final local comparison still checks generation, cancellation and monotonic
deadline. A separate database read is not an atomic guard on local Active
publication. Tests distinguish that local guarantee from the durable fence that
rejects every later stale-owner write, including orphan dispatch removal.

### Validation executed

| Check | Outcome |
|---|---|
| Nightly formatting | Passed locally and with the pinned nightly check on Linux |
| Workspace lint and check, locked | Passed |
| Storage/runtime/edge Clippy with DSQL integration features and all targets | Passed with warnings denied |
| Workspace nextest, locked | 3,903 passed; two existing ignored SDK integration tests remained skipped |
| Workspace doctests and warnings-as-errors documentation, locked | Passed |
| Focused repair and runtime fence audit | Six passed, including 128 generated memory ownership cases and 128 runtime final-gate cases |
| Fragment validation | Pinned changie scratch-copy batch passed |
| Changed-document offline links and whitespace | Passed |
| Exploration before the fix | Released-owner repair incorrectly succeeded; the invariant assertion failed |
| Memory fence negative control | Omitting the repair fence failed the released-owner assertion |
| Local activation negative control | Omitting the final comparison failed the invalid-acquisition assertion |
| Live DSQL isolated repair controls | Ten passed: insertion/deletion, takeover/release in both commit orders, and compatible renewal |
| Live DSQL generated repair interleavings | 100 passed, including successor writes and terminal stale-owner retry rejection |
| Live DSQL final-gate interleavings | 100 executed and passed; the test asserts the executed count |
| Live DSQL fence negative control | Omitting only the repair fence, while retaining its pause barrier, failed the stale-repair invariant assertion |
| Live DSQL restored positive controls | All ten fixed controls passed again after exact source restoration |

Each negative control was applied alone and restored byte for byte. The fixed
local gate controls cover both orders for valid ownership, cancellation, expiry,
same-epoch replacement and next-epoch replacement on an empty home. The actual
runtime recovery regression retains the original fence across two injected OCC
conflicts, terminates on lease loss rather than retrying it, remains non-serving,
and allows a successor's fresh acquisition. Its execution home differs from the
run-hash mapping. The writer audit now includes repair.

The local focused build could not link dependency build scripts against the Mac
SDK, before executing tests. No local toolchain or cache configuration changed;
the complete bar and focused checks ran on an ephemeral Linux validation host.
Default-suite URL-gated returns are not live evidence. These live checks had the
dedicated URL set and ran against an owned ephemeral Aurora DSQL cluster. Storage
executed 100 generated interleavings and ten fixed controls; runtime executed 100
generated final-gate interleavings. Broader live suites from earlier checkpoints
were not rerun for this lease-fence integration.

The live suites used `cargo nextest run`, `--features dsql-integration`, `--locked`,
`--profile dsql-live`, `--lib` and `--test-threads 1`, selecting each test below
with `-E 'test(<name>)'`. Shared-cluster ordering and complete commands are in the
[live-suite runbook](../../../docs/testing/dsql-live-suites.md).

- Storage: `workflow_dispatch_live_fenced_repair_isolates_lease_conflicts`.
- Storage: `workflow_dispatch_live_fenced_repair_generated_interleavings`.
- Runtime: `workflow_dispatch_live_fenced_repair_final_gate`.

All changed Rust sources on the validation host matched that committed revision
after control restoration. The negative controls failed invariant assertions,
not compilation or timeouts. Subsequent lint cleanup replaced two live-test
helper `unwrap()` calls with explanatory `expect()` messages; it changed failure
diagnostics only. The complete workspace bar and feature-enabled Clippy passed
again after that cleanup; live checks were not repeated for it.
The owned ephemeral cluster was deleted and its absence confirmed. Lifecycle
details remain private.

The default suite retains its two existing ignored SDK integration tests, which
spawn a server and pinned SDK workers (one also starts an authorization endpoint).
This ownership change does not alter their public API behavior.

Earlier sections below describe their own historical implementation checkpoints;
their single-owner outcomes do not establish competing-owner repair correctness.

## Default delivery cutover

Base: `1ebc763f56a50db20c450f12bacaf7459ba054f7`; the PR records its final base
and head. Discovery, poll registration, bounded offers, acquisition repair and
serving admission now form the default runtime path. The temporary construction
switch and workflow backlog grace/drain/spill paths are removed. Activity backlog
and activity-dispatch reconciliation remain wired. Legacy workflow copies are
removed only when current state and dispatch reconstruction agree, with at most
64 examined keys under the shared write budgets and a volatile restartable cursor.

Placement-backed queue homes share edge routing's namespace-UUID/task-queue hash.
Retirement cancels the acquisition generation's discovery and offers. Cross-home
starts reach the execution owner through an authenticated internal RPC, which
rechecks worker authorization, namespace, queue, version and incarnation before
its serving gate. Update messages and eligible buffered queries travel back with
the committed start; their waiters remain on the execution owner. Requests are
bounded at 64 KiB and responses at 128 MiB; attachments use protobuf bytes.
The owner freezes and size-checks the selection before committing the start, and
retains buffered query payloads and waiters until answered.

Public workflow-task completion and failure RPCs forward their original fields
to the execution owner after authorization, preserving its update/query waiters.
The owner repeats admission; a second forwarding hop is rejected. Per-connection
framing is rebuilt, and the public payload limits remain unchanged. Local and
forwarded stale completions return the same `NotFound` status and message.

Restored or populated seeded runtimes await both complete repair walks before
becoming Active. Only an empty memory repository can take the immediate path.
Unequal shard and bundle counts fail configuration validation before listeners
or embedded DSQL lifecycle work; there is no placement-count migration. Activity
follow-up commands pass their already-known execution home to avoid another lane
identity request. Normal backlog accounting derives from durable dispatch without
counting retained offers twice; speculative and activity sources remain separate.

### Review fixes

The internal start response uses protobuf update/query attachments within the
128 MiB envelope, matching `common/rpc/grpc.go @ v1.31.0`. Selection and size
checks precede the commit; later admissions cannot enlarge the response. Query
payloads remain buffered across a lost reply, with only the current task attempt
bound to the waiter. Regressions cover a nearly 2 MiB admitted update over the
authenticated wire, ten near-limit updates plus a query in one protobuf envelope,
pre-commit capacity rejection, and retained query redelivery. Authentication
headers are explicitly selected and compression metadata is rebuilt per hop.
Peer channels are reused by endpoint. Transient owner failures retry within the
original poll deadline; authorization, cancellation and capacity statuses survive
the public adapter, while namespace-cache misses cannot discard valid offers.

Default DSQL shard/bundle counts are promoted together; explicit unequal counts
remain rejected without a placement migration. Initial controller placement is
validated before runtime construction, and later incompatible snapshots stop
serving. Queue statistics inspect at most four dispatch pages per observation
and may undercount large backlogs. The late-poller regression advances paused time
beyond the actual offer lifetime and confirms its expiry before rediscovery.
Zero-duration polls may finish already-matched work but cannot retry an unavailable owner after
that attempt, even when its expired offer has already been republished.

The active owner of home zero coordinates legacy disposal. Each page examines at
most 64 physical keys before selecting workflow copies, advances past corrupt
rows with diagnostics, and retries deferred work after acquisition progress or a
thirty-second interval. Activity keys are preserved. The repair metric documents
successful calls, including no-ops. Maintenance key limits share one constant.

Generated configuration documentation matches its sources and retains own-namespace
scoped-worker admission. Two bounded changelog fragments cover the cutover and
matching-count guidance. Generated bindings, dependency policy, offline Markdown
links and whitespace checks passed. No dependency or lockfile changed.

### Model verification

TLC 1.7.4 and tla-rs 0.21.2 were both run for every `40_dispatch_handoff` and
`41_dispatch_repair` configuration: six positive configurations passed with each
checker, and nine negative controls failed with each checker on their intended
property (30 expected outcomes). Positive configurations cover handoff, migration,
ordering, the explicitly weaker stale-row variant, repair and concrete pages.
Negative targets cover bare-limit and durable-cursor liveness, stale incarnation,
sticky timeout conversion, missing inserts/deletes, restarting every slice,
retained retry identity and omitted stale-row deletion. No model source changed.

Commands ran from `spec/tla`, with each matching configuration substituted:

```bash
java -cp "$TLA_TOOLS_JAR" tlc2.TLC -workers 2 -metadir "$MODEL_OUTPUT" -config "$CONFIG" "$MODEL"
tla "$MODEL" --config "$CONFIG" --max-states 20000000 --max-depth 10000
```

### Workspace verification

Implementation revision: `c6087044d36ed47a8bf2b88c86e017b2b877ca50`.
The full bar covers the review fixes on the base above, including the distributed
startup schema check. Schema validation precedes placement preparation and runtime
activation; the durable upgrade-note assertion survives changelog consumption.

| Check | Outcome |
|---|---|
| `cargo +nightly fmt --all` | Passed |
| `cargo lint --locked` | Passed |
| `cargo check --workspace --locked` | Passed |
| `cargo nextest run --workspace --locked --no-fail-fast --test-threads 4` | 3,863 passed; 2 existing SDK integration tests skipped |
| `cargo test --workspace --doc --locked` | Passed; existing ignored examples remain ignored |
| `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked` | Passed |
| All-target Clippy for storage, runtime, projection, edge and engine with DSQL integration features and `-D warnings` | Passed |

No completion-bar command was omitted. Initial review runs found the old internal
attachment field name in the binding-inventory fixture and a zero-duration poll
race introduced by the new deadline wrapper. The fixture now matches the descriptor,
retaining its field number and byte wire type. Zero-duration polls still start
already-matched tasks; ordinary long polls retain their original deadline. A
redundant rustdoc link was also corrected. The complete rerun passed. The earlier
workspace run exposed six integration failures in restored-engine repair delegation, the protobuf binding
inventory and a configuration fixture; the complete reruns passed after fixes.
An earlier pre-publication workspace run passed 3,847 tests and timed out two
unchanged history-pagination properties during concurrent compilation. The complete suite
then passed with four test threads and the same timeout limits; no source fix
or test retry policy was introduced for those timeouts.

The authenticated wire regression verifies owner-held update and query waiters
through real forwarded public completion/failure calls, owner-side update
hydration, authorization and loop refusal before mutation, matching local and
forwarded stale-task status, per-hop framing, query barriers, stale offers, both
encoded start-envelope limits and public `ResourceExhausted` mapping.

### Query and placement measurements

The first completed measurements are retained below; the final live sequence
repeated the dispatch-plan checks with unchanged SQL.

[Recorded cutover plans](query-plans-cutover.md) preserve the complete queue,
home and placement reports with fixture literals anonymized. The dispatch
fixture added 16,384 rows to reach 17,806 rows. Both target ranges contained
4,096 positions, including long equal-time groups. The current continuation
plans used three disjoint scalar index bounds, followed by at most 64 primary-key
payload lookups; the legacy tuple comparison remained a residual filter.

| Complete traversal | Rows returned | Page SQL statements | Duration |
|---|---:|---:|---:|
| Live | 4,096 | 65 | 5.279 s |
| Exact | 4,096 | 65 | 5.490 s |

The statement counts include the final empty page. They measure returned rows
and page API queries, not storage-engine rows examined. The selected home had
487 dispatch rows when its first/continuation plans were captured.

The placement fixture's bounded timer-page join preserved scalar continuation
bounds. Screening examined 24,778 committed unchanged hot/timer keys in 4.362 s.
A linear extrapolation gives 352.1 s for one million runs plus one million timers;
this excludes relocation and is not a million-row benchmark.

### Live verification and boundaries

The review validation used a fresh Aurora DSQL cluster and two temporary
DynamoDB tables. All three distributed-schema engine checks ran on the combined
implementation: a target schema starts and serves; an older or unsupported future
schema is refused without starting listeners or ownership resources. The affected
disposal suites then ran serially on the same cluster, with shard-zero counts
after each suite. All owned resources were deleted; cluster absence was confirmed.

| Review suite | Outcome | Test duration | Shard-zero candidates / dispatch after |
|---|---|---:|---|
| Distributed startup, three checks | Passed | 670.177 s | 0 / 0 |
| Physical-page bounds, corrupt-row deferral and continuation plan | Passed | 6.967 s | 1 / 1 |
| Cancelled-page rollback and transaction bounds | Passed | 15.071 s | 1 / 1 |
| Generated legacy disposal, 100 cases | Passed | 131.693 s | 1 / 1 |

The [recorded disposal continuation plan](query-plans-cutover.md#legacy-workflow-backlog-disposal)
uses the primary-key index with a scalar continuation bound and no task-kind
filter. The fixture proves that 64 activity keys consume exactly one page and
that a corrupt workflow row does not hide a later covered row.

These live runs precede the final zero-duration poll retry guard, which changes
neither startup nor disposal. The final workspace bar covers that refinement.

Earlier full-suite results below are retained from implementation revision
`1bc447522233322647b4d52bf94bba3894c10395`. Unchanged live paths were not repeated
for the review fixes; the additional runs above cover the inherited startup
integration and changed disposal adapter.

Suites ran serially with the URL and endpoint gates set against real Aurora
DSQL. The resumed sequence used this common invocation, selecting one suite
filter per command as listed in the [live runbook](../../../docs/testing/dsql-live-suites.md):

```bash
cargo nextest run -p tokeira-storage -p tokeira-runtime -p tokeira-projection \
  --features tokeira-storage/dsql-integration,tokeira-runtime/dsql-integration,tokeira-projection/dsql-integration \
  --locked --profile dsql-live --test-threads 1 -E "$SUITE_FILTER"
```

The seven accumulator contracts additionally used `--run-ignored only`.

The first shared-cluster sequence exposed a projection pagination
fixture that assumed exclusive use of partition 10. Its fix selects a fresh
positive partition and fresh run keys, then removes only its own rows.

| Suite | Outcome | Test duration | Shard-zero candidates / dispatch, before → after |
|---|---|---:|---|
| complete-repair | Passed | 1016.469 s | schema absent → 0 / 0 |
| placement | Passed | 111.271 s | 0 / 0 → 12 / 12 |
| runtime-acquisition | Passed | 7.064 s | 12 / 12 → 12 / 12 |
| bulk-writes | Passed | 405.381 s | 12 / 12 → 17 / 13 |
| workflow-dispatch | Passed | 1184.100 s | 17 / 13 → 300 / 488 |
| complete-repair-after-dispatch | Passed | 431.755 s | 300 / 488 → 300 / 488 |
| transaction-limits | Passed | 11.850 s | 300 / 488 → 300 / 488 |
| shard-leasing | Passed | 22.132 s | 300 / 488 → 301 / 489 |
| conflict-counts | Passed | 30.329 s | 301 / 489 → 301 / 489 |
| embedded-ownership | Passed | 1.822 s | 301 / 489 → 301 / 489 |
| lease-conflict-rules | Passed | 2.807 s | 301 / 489 → 301 / 489 |
| repository-lease-fence | Passed | 239.477 s | 301 / 489 → 301 / 489 |
| projection-persistence | Four passed; one failed; one not run | 25.084 s | 301 / 489 → 301 / 489 |

A fresh continuation verified the corrected projection fixture, placement,
activity inputs, acquisition, history reads and bulk writes. It was explicitly
interrupted during dispatch verification; the test process received `SIGTERM`,
and cleanup completed. That interrupted case is not counted as a passing check.

| Suite | Outcome | Test duration | Shard-zero candidates / dispatch, before → after |
|---|---|---:|---|
| projection-persistence | Passed | 602.061 s | schema absent → 0 / 0 |
| placement | Passed | 98.105 s | 0 / 0 → 14 / 14 |
| activity-inputs | Passed | 63.708 s | 14 / 14 → 14 / 14 |
| runtime-acquisition | Passed | 3.418 s | 14 / 14 → 14 / 14 |
| history-event-reads | Passed | 26.485 s | 14 / 14 → 16 / 15 |
| bulk-writes | Passed | 386.807 s | 16 / 15 → 21 / 16 |
| workflow-dispatch | Interrupted after two tests passed | 418.338 s | 21 / 16 → 104 / 42 |

The resumed shared-cluster sequence completed every selected suite, including
projection pagination after bulk/dispatch leftovers and the seven ignored
projection-accumulator contracts. Preparation-sensitive fixtures had already
passed in the preceding continuation and were unchanged.

| Suite | Outcome | Test duration | Shard-zero candidates / dispatch, before → after |
|---|---|---:|---|
| projection-persistence | Passed | 580.545 s | schema absent → 0 / 0 |
| bulk-writes | Passed | 430.772 s | 0 / 0 → 5 / 1 |
| workflow-dispatch | Passed | 1211.738 s | 5 / 1 → 286 / 481 |
| complete-repair-after-dispatch | Passed | 439.285 s | 286 / 481 → 286 / 481 |
| transaction-limits | Passed | 16.434 s | 286 / 481 → 286 / 481 |
| shard-leasing | Passed | 22.969 s | 286 / 481 → 287 / 482 |
| conflict-counts | Passed | 31.077 s | 287 / 482 → 287 / 482 |
| embedded-ownership | Passed | 1.474 s | 287 / 482 → 287 / 482 |
| lease-conflict-rules | Passed | 2.833 s | 287 / 482 → 287 / 482 |
| repository-lease-fence | Passed | 254.006 s | 287 / 482 → 287 / 482 |
| projection-persistence-after-dispatch | Passed | 30.254 s | 287 / 482 → 287 / 482 |
| projection-version | Passed | 16.957 s | 287 / 482 → 287 / 482 |
| chasm | Passed | 118.460 s | 287 / 482 → 287 / 482 |
| task-queue-and-deployment | Passed | 8.396 s | 287 / 482 → 287 / 482 |
| projection-accumulators | Passed | 2774.782 s | 287 / 482 → 287 / 900 |
| iam-connector | Passed | 0.977 s | 287 / 900 → 287 / 900 |

Durations are nextest test durations, excluding compilation and count probes.
The first test on each fresh database includes the full schema bootstrap and
ASYNC index readiness. Counts are taken between serial suites through the
read-only SQLx probe, so the table identifies their actual shard-zero leftovers.

The complete-repair contract reserves home 1023 and cleans each generated case.
Its first 100 cases passed in 1,016.469 s including schema bootstrap. The same
100-case contract passed after bulk writes and dispatch in both recorded
repetitions, leaving their shard-zero counts unchanged. The original twenty-minute
test limit is unchanged. Placement, activity-input and runtime acquisition checks
preceded the other lease-taking suites.

All six independent service-limit probes passed: 3,000 modified rows committed,
with deletes included; 9 MiB of values committed while 10 MiB of values exceeded
the transaction budget once overhead was included; a 1 MiB value committed and
one extra byte was rejected; deleted bytes and `FOR UPDATE` reads were excluded
from their respective limits. Every rejected multi-statement transaction was
checked for absence of partial writes. The ten bulk-write tests separately
passed paged reset, deletion, recovery and application-budget checks.

All test clusters were deleted with absence confirmed. An initial attempt ran
no suites because its local SQL client could not execute; subsequent count
probes use the repository's SQLx integration binary. No PostgreSQL substitute
was used. Dedicated interrupted-bootstrap, historical-schema upgrade, plain
migration-runner and managed-engine lifecycle opt-ins were not run: they require
separate empty or historical databases and exercise lifecycle paths unchanged
by this cutover. Ordinary schema bootstrap and ASYNC readiness were exercised
by the live contracts above. The endpoint-gated IAM test also passed using the
repository's connection factory.

Competing-owner acquisition-repair integration and verification remain deferred
in Tasks 17–18 and Requirements 8.4–8.6; the landed lease fence does not complete
those checks. Stopping every older node remains an operational precondition;
the raw live-lease check is an additional guard, not complete enforcement.
Internal attachment size checks now precede the committed start and retain
buffered query payloads across lost replies. No dependency, lockfile, state-format or
migration change is introduced. The durable upgrade-line assertion reads
`design.md`, so changelog consumption cannot remove its subject.

## Fourth implementation PR: placement and execution-home prerequisites

Base: `2a4474057aff90e46468afdc3bdde51b8f5c6065`; the PR records its final base
and head. Task 11.1 remains unchecked until default cutover. Tasks 17–18 and
requirements 8.4–8.6 still depend on the transaction-local competing-owner lease
fence.

The approved [placement note](placement-recovery-note.md) is implemented before
runtime construction in the shared engine path. V081 adds the versioned startup
marker. Its revision and each page's hot/timer moves commit atomically; a losing
or uncertain participant rereads durable progress after jittered cancellable
backoff. Screening reads up to 1,000 metadata keys; pages that move data process
at most 64 source keys under the existing 1,000-row/4 MiB write budgets, including
marker overhead. Timer scanning is independent of hot-row placement and retains
orphan keys.

Public constructors retain synchronous convenience for fresh memory stores,
while restored memory and DSQL repositories require asynchronous preparation.
The constructor checks readiness before spawning tasks or seeding Active ownership.
Memory snapshots preserve physical hot locations, timer duplicates and upgrade
progress through snapshot extension section 3; frozen workflow-state layout and
its extension tags are unchanged. An existing malformed-extension generator now
chooses unknown snapshot tags above that assigned section.

Both delivery modes use execution-home admission and token epochs. Discovery,
offers and acquisition repair remain opt-in. No dependency, feature or lockfile
change is introduced; V001–V080 remain byte-for-byte unchanged.

### Routing audit

| Call site | Identity/home source |
|---|---|
| `runtime/commit.rs::submit` and `submit_for_owned_shard` | Start, signal-with-start and start-and-update commands supply namespace/workflow identity; other commands share the lane cache's ordinary load. |
| `runtime/commit.rs::shard_id_for` | Asks the owning local lane for immutable execution identity; derives the home using the configured shard count. |
| `runtime/mod.rs::validate_workflow_task_token` | Resolves the home once through that cache, then checks its current completion epoch. |
| `runtime/workflow_task.rs` normal and reserved task-token minting | Passes already-loaded committed state to `current_shard_epoch`. |
| `runtime/activity.rs` activity task-token minting | Passes already-loaded committed state to `current_shard_epoch`. |
| `runtime/activity.rs` heartbeat, by-ID resolution, forced-start token creation and completion validation | Passes existing loaded state to `shard_epoch_for_completion`. |
| `lane.rs` tracking, publication and processing-span shard fields | Uses execution identity from the existing transition/committed state; no run-hash fallback. |
| Local lane selection | Still hashes the run key to serialize commands. Remaining calls to `shard::shard_for` are test oracles, not admission or epoch routing. |

### Executable contracts

Memory runs 100 generated relocation/restart traces with an independent expected
placement map, whole-page failure injection, snapshot restoration between pages,
row conservation, semantic snapshot preservation, committed-only counters and
shared budget assertions. Fixed fixtures cover the actual reset materialization,
timers remaining on the old shard after hot placement is correct, equivalent and
conflicting copies, orphan timers, unexplained placement, invalid identity,
1,000-key screening and a byte-limited page of large states. An oversized memory
timer whose relocation fits alone but cannot fit with its cursor fails with row
identity before any page progress or physical mutation commits.

The same reset fixture is exercised through real DSQL. Live contracts cover
concurrent starters, a stale transaction attempting to write after completion,
unchanged authoritative bytes, complete dispatch repair, raw live leases with
unrecognized shard encodings, corruption diagnostics, duplicates/orphans, and the
exact SQL used by first and deep continuation pages. Runtime regressions cover
both construction modes with only the execution home held, distinct nonzero
epochs, wrong-home rejection, heartbeat and completion, cold-load reuse and no
extra hot-cache load. Both modes mint tokens at the home's epoch 17, reacquire
that home at 18, and reject the original workflow completion and activity
heartbeat without mutating storage. The history-notifying wrapper preserves
not-ready status for a restored memory snapshot and completes its scan through
the wrapper. Retry coverage checks warning-level causes and consecutive attempts
beyond the backoff cap, as well as cancellation without restart.

### Live DSQL and workspace validation

Two disposable Aurora DSQL clusters were used and deleted, with absence confirmed.
The initial run exposed an unbounded hot-index hash build in a plain timer-page
join. The final query materializes the bounded timer keys and restricts the hot
input to those keys. The final cluster ran all four placement contracts and the
runtime acquisition check serially under the `dsql-live` profile:

| Contract | Result |
|---|---|
| Concurrent starters and rejection of a late page after completion | Passed, 640.480 s including fresh schema bootstrap |
| Fail-stop identity/placement checks and raw legacy-encoded live lease guard | Passed, 7.821 s |
| Joined continuation plans and unchanged-row screening | Passed, 28.220 s including fixture setup and cleanup |
| Actual stopped reset, equivalent/conflicting timer copies and orphan preservation | Passed, 11.214 s |
| Runtime acquisition and superseded tracker installs | Passed, 3.829 s |

Commands:

```sh
cargo nextest run -p tokeira-storage --features dsql-integration --locked --profile dsql-live --test-threads 1 --lib --no-fail-fast -E 'test(placement_live_)'
cargo nextest run -p tokeira-runtime --features dsql-integration --locked --profile dsql-live --test-threads 1 --lib -E 'test(workflow_dispatch_live_acquisition)'
```

The exact query builders are explained at the first timer page and continuation
positions 4,096 and 11,000 in a 12,288-pair fixture. Every continuation component
appears in a primary-key `Index Cond`, including equality on the preceding
components. The hot side uses `workflow_hot_pkey` with
`Index Cond: (run_key = ANY ($1))`, where the array is derived from the
materialized timer page. The optimizer may choose a hash join, but both inputs
are bounded. The hot-row walk's deep continuation also has a primary-key
`run_key >` bound.

With the other contract fixtures present, the unchanged-row pass screened
24,778 committed hot/timer keys in 4.535 s. This is about 183 ms per 1,000 keys,
including marker initialization and empty phase-ending pages. Linear extrapolation
for one million runs plus one million timers is 366.1 s, about six minutes.
This is a small-sample extrapolation with short identities, no relocation and
one participant, not a million-row benchmark or a relocation-throughput claim.
The deliberately undecodable payloads in this fixture prove unchanged rows do
not require state or timer decoding.

The full root §10.4 bar passed: nightly formatting, `cargo lint --locked`,
workspace check, workspace nextest, workspace doctests, and documentation with
`RUSTDOCFLAGS="-D warnings"`. Nextest ran **3,806 tests, all passed**; the two
existing ignored external SDK integration tests remained ignored. The additional
DSQL-feature check passed:

```sh
cargo clippy -p tokeira-storage -p tokeira-runtime --all-targets --features tokeira-storage/dsql-integration,tokeira-runtime/dsql-integration --locked
```

The review update repeated the full bar and DSQL-feature Clippy. Its focused
filter passed seven checks, including same-home token staleness, retry diagnostics,
wrapper preparation and oversized timer/cursor rejection. The live DSQL checks
above were not repeated: their SQL and transaction logic are unchanged. No new
cluster was created for this update.

Offline Markdown links and `git diff --check` passed. The link command uses the
CI exclusions plus the ignored generated `.tokeira-build` scratch directory.
No default-bar command was skipped. The separate functional conformance corpus,
Dagger CI gates and full cutover/service-limit suites were not run for this
prerequisite PR; the latter remain in the following cutover PR.

### Known risks and boundaries

Every older node must remain stopped during unfinished preparation. The raw
live-lease check detects an existing owner but cannot fence an older binary
acquiring or renewing after its snapshot. The marker protocol excludes competing
new starters; it does not close that older-writer enforcement gap. The runbook
documents fail-stop diagnostics and investigation without a skip override.

The first startup scans all hot and timer keys. Screening estimates exclude
relocation, larger identity columns and contention. Marker shard-count mismatch
or unclassifiable corruption fails startup instead of serving an incomplete home.
Per-home acquisition repair never moves authoritative placement. The unchanged
delivery switch, backlog retirement, telemetry and full cutover contracts belong
to the following PR. No modelled dispatch/repair step is changed here, so the
existing model-checker results remain applicable; neither checker was rerun.

## Third implementation PR: reconciliation and serving admission

Original base: `059f2c1b1291bc176c5005617df6328925e56845`.
The PR records the rebased base, implementation commit and final head.

This change implements complete single-owner acquisition repair behind the same
private queue-home construction choice as discovery. Ordinary constructors retain
the existing delivery and recovery effects. The subsequent cutover removes that
choice together with the workflow backlog paths; activity backlog remains separate.
No migration, state-format, dependency-version or lockfile change is introduced.
The runtime's new opt-in `dsql-integration` feature forwards the existing storage
feature solely for the live acquisition test.

One-run repair reads authoritative state and replaces or deletes only its derived
dispatch row in the same transaction. Recovery walks NULL/true candidates and then
the complete home index, including sticky, excluded and orphan rows. Acquisition
publishes Sweeping and drains already-admitted writers before reading either walk.
Lane commits retain the permit through post-commit tracking and reset materialization;
direct activity and retention writers use the same execution home. Due timers and
activity preparation, including rule-pause transitions, wait for Active. An aborted
or failed acquisition never serves and its replacement starts both walks at the head.

All five timeout/callback trackers are acquisition-scoped in both construction modes.
Installs check current generation under the owner lock; scans require Active;
retirement compares the submitted revision. Activity retry completions also carry
that revision, so a delayed reply cannot update or remove a replacement key.
Callback HTTP completions retain their
entry until a scanner observes committed terminal state, preventing a late result
from erasing replacement tracking. Callback lifecycle grounding is
`components/callbacks/statemachine.go @ v1.31.0` and
`components/callbacks/executors.go @ v1.31.0`. Lane and publisher tracking now use
stable execution-home placement, and the memory repository's recovery/timer placement
matches that same rule. Local lease deadlines bound activation and admission;
these are not a transaction-local competing-owner fence. Requirements 8.4–8.6 and
tasks 17–18 remain deferred for that prerequisite.

### Contracts and model checks

Property 8 runs 100 generated cases on each backend, with missing, stale and
mismatched rows; sticky, closed, speculative and absent runs; legacy recovery flags;
interruption and head-restart convergence. An independent expected-row model checks
complete repair. Raw authoritative state, history and projection snapshots remain
unchanged after partial and complete repair. Runtime fault fixtures cover either
walk's query failure, repair failure, whole-transaction OCC retry/exhaustion,
interruption, an already-admitted lane commit whose run hash differs from its
execution home, and overdue timer/activity-pause work deferred until Active.
Encoding checks exercise the 1 MiB column and conservative 2 MiB row budgets.
The single-row repair budget is distinct from DSQL's 3,000-row/10 MiB service limits;
the service-boundary abort suites remain part of task 15.

The companion `41_dispatch_repair` model covers three blocked positions ahead of
a serviceable task, page continuation, retained failure retry and interrupted
NULL/true/home walks. Intermediate repair steps are non-serving stutters; activation
maps to Reconcile. TLC 1.7.4 and tla-rs 0.21.2 agree on all six positive and nine
negative configurations across models 40 and 41. Every negative fails its intended
property; temporal negatives were also checked with only that target property.
Commands, state counts and code/test mappings are in
[the model README](../../../spec/README.md#tla41_dispatch_repairtla).
No state/depth limit truncated a run. The stale-rows configuration retains its
explicitly weaker claim; it does not prove complete repair.

### Live DSQL validation

Runs use temporary Aurora DSQL clusters under the authorized profile,
without deletion protection and with `purpose=tokeira-test` and
`task=workflow-dispatch-3`. Each runner deletes its own cluster in cleanup.

The first cluster was deleted and subsequent lookup confirmed absence. The generated
100-case repair contract passed, including schema bootstrap. The continuation-plan
run then exposed a nested-ordering SQL syntax error. The corrected query gives the
outer ordering its own subquery boundary; its rerun is recorded below.

The second cluster was deleted. Its bootstrap run returned DSQL schema-change conflict
`OC001`; the corruption contract passed on the migrated database (7.235 s).
The real acquisition check passed (2.825 s), as did the ordered-page contract
(15.637 s) and complete 16,384-row plan/traversal fixture (287.795 s). The two
ad-hoc follow-up checks overlapped the plan fixture, so the final run repeats
these suites serially as required by the live-test runbook.

Inspection of that plan found a selective primary-key tail scan at the timestamp
boundary despite correct returned ordering. The final SQL first materializes a
bounded page of index-covered keys, then looks up its payloads. The live test now
asserts complete scalar index bounds for every disjoint continuation interval;
returned rows alone cannot satisfy the seek check.

The final cluster was deleted. All four suites ran serially with the `dsql-live`
profile and passed:

| Contract | Result |
|---|---|
| Repair home/decode/encoding failures | Passed, 418.815 s including schema bootstrap |
| Real acquisition and superseded tracker installs | Passed, 2.819 s |
| Ordered pages, routing, reset and speculative legacy delivery | Passed, 12.332 s |
| First/deep continuation plans and full traversal | Passed, 293.465 s |

[The final before/after plans](query-plans-reconciliation.md) record the
16,384 seeded rows, 16,413 total rows, 24.9558% target-range selectivity and deep
positions inside one priority band, including equal-time ties. Every continuation
uses all three scalar ordering bounds as index conditions. The bounded key page
feeds primary-key equality payload lookups in the same read snapshot. Both Live
and Exact full traversals preserved all 4,096 positions. Home continuations also
seek by run key. No migration or query-concurrency increase was needed.

```bash
cargo nextest run -p tokeira-storage --features dsql-integration --locked --profile dsql-live --test-threads 1 -E 'test(workflow_dispatch_live_generated_complete_repair)'
cargo nextest run -p tokeira-storage --features dsql-integration --locked --profile dsql-live --test-threads 1 -E 'test(workflow_dispatch_live_repair_decode_and_encoding_failures_preserve_authority)'
cargo nextest run -p tokeira-runtime --features dsql-integration --locked --profile dsql-live --test-threads 1 -E 'test(workflow_dispatch_live_acquisition_rejects_superseded_tracker_installs)'
cargo nextest run -p tokeira-storage --features dsql-integration --locked --profile dsql-live --test-threads 1 -E 'test(workflow_dispatch_live_ordered_pages)'
cargo nextest run -p tokeira-storage --features dsql-integration --locked --profile dsql-live --test-threads 1 -E 'test(workflow_dispatch_live_query_plans)'
```

### Review regressions

Workflow execution/run deadlines now install in the lane's post-commit path for
all committed start events, including plain, update-with-start, signal-with-start,
retry, cron and continue-as-new starts. The install uses committed state and its
execution home, while write admission is held and before the reply. Reset's
separately materialized successor retains its explicit install. All caller-side
installs after an awaited start have been removed. The publisher receives the
committed execution home for the whole batch; tracker home lookup performs no
state read and cannot truncate dispatch on a read failure.

Deterministic runtime tests exercise real lane commits and the production timeout
submission path with an explicit scan instant. A timed-out run's retry successor
and cron successor both time out without reacquisition. A lost reply after each
of the three start forms retains its deadline when only the execution home is
held, even after another shard is acquired twice. A 32-activity batch installs on
that home and delivers every offer while a repository fixture rejects state reads;
the fixture records zero reads. The focused suite passes all 11 tests, including
the existing scanner properties. No new tests sleep.

The restart-slices negative configuration now lists only `EventuallyResolved`.
TLC 1.7.4 reports its expected temporal violation (exit 13), and tla-rs 0.21.2 names
`EventuallyResolved` (exit 1). Both were rerun; the other unchanged
model configurations retain the full results above.

Two cutover gaps are recorded explicitly in the design and remain unchecked in
task 11.1. Legacy reset materialization can leave hot state and timers on the
run-key shard until the first successor commit; current repair rejects that
placement mismatch. Separately, the outer runtime admission/token helper still
uses run-key placement. The multi-home tracker tests submit directly to the lane,
so they do not claim public admission works when only the execution home is held.
Default enablement requires placement handling and public admission regressions.

No live cluster was created for these runtime and test changes. The storage SQL,
migrations and repository transactions are unchanged; prior real DSQL results
remain the evidence for them.

The full AGENTS.md §10.4 bar passed after these fixes: nightly
formatting, locked workspace lint and check, all 3,757 nextest tests (2 existing
ignored SDK integration tests), doctests (1 passed, 21 existing ignored examples),
and documentation with warnings denied. DSQL-feature all-target Clippy also
passed with warnings denied. No bar command was omitted. Changed Markdown links, including fragments, and whitespace checks passed. The runtime
changes introduced no dependency, lockfile, migration or shared-config changes.

### Workspace validation before rebase

The complete bar passed: nightly formatting, `cargo lint --locked`, workspace
check, 3,736 nextest tests (2 existing ignored), workspace doctests (1 passed,
21 existing ignored), and documentation with warnings denied. DSQL integration
all-target Clippy also passed with warnings denied. An earlier unchanged projection
checkpoint test hit its 500 ms cancellation; its isolated rerun and the full rerun
passed. Nextest reported one leaked process handle in an unchanged proto field-number
test during the green run. Native cached archives emitted linker deployment-target
warnings; the compiler/Clippy lint gates passed. No bar command was skipped.
A subsequent complete bar also passed with the bounded-key query (3,736 tests,
2 ignored; no leak warnings in that run). The final activity-retry revision guard
passed its generated replacement test, 72 focused activity/acquisition/repair
tests, and feature-enabled all-target Clippy with warnings denied. The PR records
the final full bar after rebasing onto the newer main revision.

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
gate. No migration or state-format change is introduced. The same PR's protobuf
build changes are recorded below.

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
test, and again after completing the protox build changes. Final Rust validation
uses `PROTOC=/nonexistent/protoc`. Nothing in the bar was omitted:

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
link checks and `git diff --check` passed. No migration or proptest seed changes
were introduced.

One earlier workspace run hit the attempt assertion in the unchanged
`backoff_interval_rule_between_publication_and_start_pauses_durably` test: the
offer-only poll returned a stale attempt-one candidate. That API leaves durable
revalidation to start. Its test and activity paths remain unchanged; 30 isolated
repetitions and the final full workspace run passed. Earlier runs also reported
isolated leaked-test warnings; the final full run had none.

### Protobuf build change

The engine, bench and standalone README example enable the Temporal SDK's
`vendored-protox` feature. Compatibility and conformance build scripts compile
descriptors with protox and feed them to Connect generation, retaining imports
and source information. The controller generation step in `proto-sync` reuses
the already-compiled internal descriptor. `proto-sync check` passes with the
invalid `PROTOC`, proving checked-in bindings remain byte-for-byte identical.

The workspace lockfile adds four dependency edges to the existing protox package;
it adds no package versions. The example's independent lockfile adds protox and
its compiler dependencies without changing existing package versions. Its locked
standalone build passes with the same invalid `PROTOC`, retaining the existing
`Runtime::new_assume_tokio` deprecation warning.

CI and builder recipes no longer install system protobuf packages. README,
quick-start and agent build prerequisites match the new build paths. CI retains
the invalid `PROTOC` guard. Container image builds were not run; they are outside
the requested §10.4 bar.

### Live validation and query plans

Live checks use ephemeral Aurora DSQL clusters under the authorized profile,
with deletion protection disabled and tags `purpose=tokeira-test` and
`task=workflow-dispatch-2`. The lifecycle runner deletes its cluster after success
or failure. The migration runner owns schema-conflict retries and ASYNC index
readiness; the fixture no longer has a separate migration retry loop.

Deletion of the first cluster was confirmed. Five of six tests passed:

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

Deletion of the second cluster was confirmed. All three selected checks passed:

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
same as in the first run. [Recorded plans](query-plans-discovery.md) preserve the
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

Runs use the authorized profile and newly created ephemeral clusters
with deletion protection disabled and tags `purpose=tokeira-test` and
`task=workflow-dispatch-1`. Cleanup is installed before running tests. The suites
use SQLx through the repository's dedicated URL gate and embedded migration
runner, await ASYNC index readiness, and run serially. The separate IAM connector
test exercises the production connection factory. No PostgreSQL substitute is
used. Resource identities and credentials are omitted.

| Run | Executed results |
|---|---|
| Development contracts | Ordered pages/reset/collisions passed (41.430 s); 100 atomic traces passed (322.377 s); IAM connector passed (0.473 s). |
| Expanded final contracts | Ordered pages/routing/home/reset/collisions passed (45.742 s); 100 atomic traces with generated reset cases and both commit entry points passed (342.968 s); IAM connector passed (0.554 s). |
| Review regressions | Eight-shard reset placement passed (38.676 s); ordered pages including speculative legacy delivery passed (11.965 s); 100 atomic traces passed (347.510 s); IAM connector passed (0.833 s). |

All three clusters were deleted. The first run preceded the expanded reset/routing regressions. The second run
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

All AGENTS.md §10.4 commands passed:

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
