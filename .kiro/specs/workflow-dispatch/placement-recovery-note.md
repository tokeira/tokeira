# Legacy execution-home placement recovery: design note

Status: approved with review qualifications; implementation evidence is recorded separately.

This note resolves the placement prerequisite for workflow dispatch. It is based
on the merged bounded-bulk-writes implementation at `63ec34f5`. It does not
change the deferred transaction-local execution-home lease fence.

## Decision and authority

Run a bounded, restartable **storage upgrade before runtime construction**.
Every node must observe its durable completion before starting workflow writers,
scanners, a purger, discovery, shard acquisition, or request admission. Concurrent
starting nodes may help advance the same upgrade; they do not acquire workflow
ownership to do so.

This is a placement migration under stopped-cluster upgrade authority, separate
from per-home dispatch repair. It may change `workflow_hot.shard_id` and timer
row locations across homes. It preserves the bytes of authoritative run state,
history, transition sequences, projection state, dedupe records, current pointers,
and timer payloads. No synthetic workflow command or history event is introduced.
Per-home repair retains its existing prohibition on writing another home's rows.

The tradeoff is one bounded-page scan of existing hot rows and timers before any
workflow serves on the first startup. A valid legacy row cannot subsequently
poison an unrelated shard's acquisition. This is a startup delay for the cluster,
not a promise that unrelated runs serve during the migration.

## Why the gate belongs before construction

The runtime constructor already starts the timer scanner and other background
tasks, and can seed an Active shard. Gating only `acquire_shard` is too late.

The shared engine startup path performs preparation after schema readiness
and before constructing the runtime. Lower-level runtime construction over an
existing repository must follow the same preparation contract; it must not expose
an alternate path that starts tasks first. Use an explicit asynchronous, fallible
preparation/construction boundary, with tests covering direct runtime construction
as well as network and embedded engine startup. Empty in-memory stores complete
preparation immediately.

Preparation is independent of the temporary delivery choice. The first
implementation PR installs this startup gate in both modes; acquisition dispatch
repair, discovery and offers remain opt-in until cutover. Normal commands gain no
marker lookup or additional state reload.

## Stopped-cluster precondition and diagnostics

Every older node must be stopped throughout preparation. Before unfinished
preparation advances, check raw shard_lease rows for live owned leases, including
unrecognized legacy shard encodings. Do not use list_bundle_leases, which filters
those encodings. This guard detects an obvious violation; it cannot prevent an
older node acquiring or renewing after the check, or a stale writer committing.
Older binaries do not participate in the marker protocol. Record this enforcement
gap as a known risk; the stopped-cluster precondition is still operational.

Unexplained placement, invalid identity encountered during relocation, and
conflicting timer payloads fail startup. Name the run key, stored shard, computed
home when available, and failed check. Skipping cannot safely isolate corruption:
the actual home could otherwise serve without discovering the row. The operator
runbook explains what to inspect and provides no bypass.

## Durable progress and concurrent starters

V081 adds the dedicated workflow_placement_upgrade progress table. Do not reuse `run_bulk_write`: its records describe invisible runs destined
for materialization or purge, neither of which describes this upgrade.

One row identifies the placement algorithm and records:

- the configured shard count;
- a monotonically increasing revision;
- phase: hot rows, timers, or complete;
- the exclusive keyset cursor for the current phase.

Participants must agree on the recorded routing parameters. A mismatch is an
actionable startup error, not permission to continue using different homes.

Every page transaction reads that row, verifies the phase and revision, performs
its bounded changes, and conditionally advances the same row's revision and
cursor. A failed comparison aborts the whole transaction. An actual marker write,
including on a page containing only correctly placed rows, puts all concurrent
page attempts in the same write-conflict domain. Initialization uses a unique
marker key and retries a conflicting creation.

DSQL adjudicates concurrent modifications of the same row at commit; a losing
page backs off with jitter, then retries from fresh durable progress. This deliberately uses ordinary
write/write conflicts, without depending on the deferred lease fence or
`FOR KEY SHARE` behavior. See
[AWS concurrency control](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-concurrency-control.html).

There is no process-owned migration lease to strand. A second node can make
progress if the first exits or stalls. A node arriving mid-pass joins this
protocol and remains non-serving. Retryable failures use cancellation-aware
backoff and fresh transactions; bounded inner retries must not require a process
restart to resume the outer preparation loop. An uncertain commit result is
resolved by rereading the marker.

Persist phase, rows examined and rows moved with the cursor. Progress logs report
only these committed totals, including after an uncertain reply. For a million
runs plus a million timers with no relocation, 1,000-key screening needs about
2,000 commits. The live unchanged-row sample screened 24,778 keys in 4.535 seconds
(about 183 ms per 1,000 screened keys, including fixed setup/empty-page costs).
Linear extrapolation gives about 366 seconds, or six minutes, for a million runs
plus a million timers. This is not a million-row benchmark: it uses short identity
columns, no relocation and one participant, and includes client round trips.
Relocation adds state validation, payload reads and writes under the smaller
64-key budget. See the measured contract and its limits in
[implementation evidence](implementation-evidence.md).

The final empty timer page advances the marker to complete in its transaction.
Any older page still in flight must also write the marker, so it cannot commit
after that final advance. Once complete, subsequent startups perform a marker
read and no data scan. The completion marker is never reset by normal operation.

## Two bounded walks

**Hot rows.** Walk all `workflow_hot` primary keys in ascending `run_key`
order. Do not restrict this to recovery candidates, pending workflow tasks, reset
timestamps, or currently misplaced rows. Screen namespace/workflow identity columns
using the existing routing function. Decode state only for runs whose hot row or
timers will move, validating identity before any relocation.

A row at that home needs no change. A row at the legacy run-key shard is moved by
updating only its placement column. A different unexplained placement or invalid
identity fails preparation with a diagnostic rather than being silently
classified as the reset defect. The page's placement changes and progress commit
together.

**Timers.** Walk the entire `timer_bucket` primary key in order:
`(shard_id, fire_at, run_key, timer_id)`. Resolve each row's home from its
existing hot row through a bounded page LEFT JOIN, preserving orphan rows and
cursor progress. Materialize the bounded timer keys first and constrain the hot
side with those run keys as well; an unconstrained hash join can otherwise scan
the whole hot index on every page. The live first/deep continuation plans verify
primary-key bounds on both sides. Fetch state only for distinct runs whose timers
will move. This independent walk finds timers left at the old shard even when an
earlier ordinary commit already moved their hot row.

For a misplaced timer belonging to an existing run:

1. Validate that the source is the known legacy run-key location.
2. Insert the destination row, preserving its key components other than shard,
   its timer payload and its creation metadata.
3. If the destination already exists, require matching timer identity and payload;
   an equivalent destination is retained. Conflicting payloads are an explicit
   error, not last-writer-wins.
4. Delete the exact source primary key in that same page transaction.

Correctly placed rows are unchanged. Timers without a hot row are outside the
live-run placement migration: preserve them for the existing orphan cleanup and
`run_bulk_write` recovery/purge paths. In particular, do not publish or purge a
partially materialized successor while relocating placement.

The timer cursor advances by examined source keys. Moving a row behind the cursor
is safe because the destination is already correct. Moving it ahead can cause
one later read of the correct destination, not a missed source or another move.
No operation creates a newly misplaced row. Together with the startup gate, this
makes exhaustion a completeness check without an unbounded final verification
query.

Screen up to 1,000 keys per page without state/timer payloads. A page that moves
rows processes at most 64 keys; stop at the processed prefix and fetch relocation
payloads incrementally. Cap each
transaction using the shared `write_budget` limits: 1,000 modified rows and
4 MiB of written values, reserving room for the marker. Count a timer copy and
source delete separately; stop before exceeding either budget and advance only
past the processed prefix. Keep read payload accumulation bounded as well.
Use primary-key keyset predicates, not offsets; verify the continuation plans on
DSQL. This does not change the separate 64-key legacy-backlog disposal cap.

## Completion, acquisition and preservation

After placement completion, the existing two-walk acquisition repair reads each
run under its actual execution home, reconstructs wanted dispatch rows, removes
unwanted rows, restores timeouts, and only then marks the home Active. This
establishes requirements 8.1, 8.2 and 8.11 without allowing serving while the
placement pass is incomplete. The placement pass itself neither invents dispatch
state nor relaxes repair's identity checks.

Acquisition repair still changes neither history nor authoritative run state
(requirement 8.10). Its restartability without a durable continuation
(requirement 8.7) is unchanged: the durable progress above belongs only to the
separate startup migration.

Interrupted bulk writes from the merged implementation retain their existing
ownership-independent purge contract and acquisition handoff. Their records and
history are untouched by placement migration.

## Verification required before implementation is accepted

Run the same semantic contract against both stores, including:

- the exact interrupted legacy-reset fixture, with distinct execution and run-key
  homes;
- the timer-only mismatch after the hot row has already moved;
- unchanged timers, equivalent destination duplicates, and conflicting payloads;
- interruption before and after every page commit, uncertain commit replies, and
  recovery after transient faults cease without restarting participants;
- live-lease refusal including legacy encodings, and explicit documentation that
  it is not an old-writer fence;
- a restored memory fixture whose physical misplacement actually runs the pass;
- two simultaneous starters, a third arriving mid-pass, and a stale page trying
  to commit after completion;
- no runtime task, seeded Active shard, acquisition, or request admission before
  completion; completed-marker startup performs no data scan;
- 1,000-key screening and 64-key relocation pages across row and byte boundaries,
  including marker overhead and committed-only progress;
- unchanged state/history/projection/dedupe/current-pointer data, preserved bulk
  records, and dispatch completeness after acquisition.

Generated properties use an independent placement model and at least 100 cases
for row conservation, state preservation, bounded pages and restartable progress.
Fault and concurrency tests synchronize through barriers and injected clocks,
without explicit sleeps.

The memory store represents stored hot placement and timer physical keys
separately from semantic state, including duplicate locations. Test fixtures
inject that physical state; interruption tests preserve it and migration progress
across reconstruction instead of letting snapshot normalization repair the
fixture. Snapshot extension section 3 carries this metadata separately from the
frozen run-state extensions. Restored stores require preparation even when a
fresh empty store can use the convenient synchronous runtime constructor.

Live Aurora DSQL tests must verify page atomicity, concurrent marker updates,
late-page exclusion after completion, timer relocation and continuation plans.
Credentialed results and their limits belong in
[implementation evidence](implementation-evidence.md).

## Source anchors

- Historical reset placement:
  `crates/tokeira-storage/src/dsql/run_repository/load.rs @ a0addfa2^1`.
- Hot-row upsert and explicit timer operations:
  [commit.rs](../../../crates/tokeira-storage/src/dsql/run_repository/commit.rs).
- Physical timer key:
  [V008](../../../crates/tokeira-storage/migrations/V008__timer_bucket.sql).
- Runtime construction and background tasks:
  [runtime/mod.rs](../../../crates/tokeira-runtime/src/runtime/mod.rs).
- Shared engine construction:
  [engine/lib.rs](../../../crates/tokeira-engine/src/lib.rs).
- Memory placement and snapshot reconstruction:
  [memory.rs](../../../crates/tokeira-storage/src/memory.rs).
- Existing transaction budgets:
  [write_budget.rs](../../../crates/tokeira-storage/src/write_budget.rs).
- Existing acquisition contract:
  [requirements](requirements.md), [design](design.md).
