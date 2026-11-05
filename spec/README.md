# Tokeira TLA+ Specs

This directory is the beginning of a **small, readable, executable specification stack** for Tokeira.

The goal is not to formalize everything at once.
The goal is to formalize the parts of the system where concurrency, retries, stale routing, batching, or failure could violate durable-execution semantics.

For the first pass, the focus is intentionally narrow:

- model the **semantic contract** of a workflow run,
- relate that contract directly to `tokeira-kernel`,
- make the spec readable by engineers who are new to TLA+.

## What is in this directory

```text
spec/
  README.md
  refinement/
    kernel.md
  tla/
    00_execution_contract.tla
    00_execution_contract.cfg
    30_bundle_lease.tla
    30_bundle_lease.cfg
    30_bundle_lease_for_update.cfg
    40_dispatch_handoff.tla
    40_dispatch_handoff.cfg
    40_dispatch_handoff_order.cfg
    40_dispatch_handoff_migration.cfg
    40_dispatch_handoff_stale_rows.cfg
    41_dispatch_repair.tla
    41_dispatch_repair.cfg
    41_dispatch_repair_pages.cfg
    42_outbound_discovery.tla
    42_outbound_discovery.cfg
    42_outbound_discovery_one_home.cfg
    negative/
      30_bundle_lease_*.cfg
      40_dispatch_handoff_*.cfg
      41_dispatch_repair_restart_slices.cfg
      41_dispatch_repair_retained_retry.cfg
      41_dispatch_repair_skips_deletes.cfg
      42_outbound_discovery_continue.cfg
      42_outbound_discovery_full_sweeps.cfg
      42_outbound_discovery_reset.cfg
```

### `refinement/kernel.md`

This is the bridge between the specification and the Rust code.
It explains:

- which parts of the system the kernel owns,
- which parts belong to storage/runtime/projection instead,
- how abstract spec variables map onto Rust types,
- how to evolve the kernel and the spec together.

### `tla/00_execution_contract.tla`

This is the first executable TLA+ model.
It intentionally models a **small semantic subset** of Tokeira:

- `Start`
- `Signal`
- `WorkflowTaskStarted`
- `WorkflowTaskCompleted`
- `ActivityResolved`
- `TimerDue`

It models the behavior of **a single workflow run**.
That is deliberate.
It keeps the first spec readable and lets us pin down the kernel's semantic contract before we add storage fencing, current-run pointers, bundle leases, broker reservations, or projection prefixes.

### `tla/00_execution_contract.cfg`

This is the TLC model configuration for the first spec.
It gives the constants small finite values so TLC can exhaustively explore the state space.

### `tla/30_bundle_lease.tla`

This models how a bundle's lease fences writes on Aurora DSQL, which takes no locks.
DSQL adjudicates conflicts at commit time, and of two conflicting transactions, whichever commits last fails.
The model encodes AWS's conflict rules between key writes, updates of non-key columns, `SELECT ... FOR UPDATE` and `SELECT ... FOR KEY SHARE`.
A plain `SELECT` never conflicts.

The protocol it checks:

- `epoch` is a key column, through a unique index on `(shard_id, epoch)`;
- every run commit, creation and deletion reads the lease row `FOR KEY SHARE` in its own transaction and checks owner and epoch;
- only an acquisition or a release writes `epoch`, and renewal updates only the expiry, without `FOR UPDATE`;
- an owner whose lease has expired by its own clock stops starting commits.

It checks that no run write commits unless its node owns the lease at the epoch it checked (`NoStaleOwnerCommit`).
It checks that renewals and the owner's other commits never abort the owner's commits (`NoSpuriousAbort`).
And it checks that an expired lease is eventually taken over, renewed or released (`LapsedLeaseResolves`).

The last property depends on the owner stopping at its expiry.
Under DSQL's rule, a fenced commit that lands first aborts a concurrent takeover, so an owner that keeps committing can starve every takeover.
Safety never depends on the owner stopping; failover does.

| Configuration | What it exercises | Expected |
|---|---|---|
| `30_bundle_lease.cfg` | the protocol above: two nodes, two runs, three epochs | pass |
| `30_bundle_lease_for_update.cfg` | run writes that read the lease `FOR UPDATE`, with `NoSpuriousAbort` unchecked | pass: safe, but contended |
| `negative/30_bundle_lease_plain_read.cfg` | a plain read of the epoch inside the commit | `NoStaleOwnerCommit` fails |
| `negative/30_bundle_lease_separate_check.cfg` | the epoch checked in its own transaction before the commit | `NoStaleOwnerCommit` fails |
| `negative/30_bundle_lease_epoch_not_key.cfg` | `epoch` outside any unique index | `NoStaleOwnerCommit` fails |
| `negative/30_bundle_lease_renew_for_update.cfg` | renewal that reads the lease `FOR UPDATE` | `NoSpuriousAbort` fails |
| `negative/30_bundle_lease_commit_for_update.cfg` | run writes that read the lease `FOR UPDATE` | `NoSpuriousAbort` fails |
| `negative/30_bundle_lease_no_self_fence.cfg` | an owner that keeps committing after its lease expires | `LapsedLeaseResolves` fails |

The first four negative controls are today's code, so the model also shows what has to change: the in-transaction read and the controller-mode check are plain reads, `shard_lease` is keyed on `shard_id` alone, and renewal reads `FOR UPDATE`.

The conflict rules the model takes from AWS's documentation are also checked on a real cluster, through Tokeira's own SQLx client, by `crates/tokeira-storage/tests/dsql_lease_fence.rs`.
Each of its ten cases runs two transactions on one lease row in a fixed interleaving and checks which commit DSQL refuses.

| Rule the model assumes | Live case | DSQL's answer |
|---|---|---|
| `FOR KEY SHARE` reads don't conflict | two fenced run commits | both commit |
| an update of non-key columns doesn't conflict with `FOR KEY SHARE` | a renewal and a fenced run commit, in each order | both commit |
| a write of a key column conflicts with `FOR KEY SHARE` | a takeover and a fenced run commit, in each order | the later commit is refused |
| `FOR UPDATE` conflicts with `FOR KEY SHARE` | a renewal that reads `FOR UPDATE` and a fenced run commit, in each order | the later commit is refused |
| only a key column's change fences | a change of owner alone, then a fenced run commit | both commit |
| `epoch` fences only through a unique index | a takeover on a lease keyed on `shard_id` alone, then a fenced run commit | both commit |
| a plain read never conflicts | a takeover, then a run commit that read the epoch plainly | both commit |

The `FOR UPDATE` cases are the live counterpart of `negative/30_bundle_lease_renew_for_update.cfg`, the lease keyed on `shard_id` of `negative/30_bundle_lease_epoch_not_key.cfg`, and the plain read of `negative/30_bundle_lease_plain_read.cfg`.
The suite creates its own probe tables and needs no migrations; run it under the `dsql-live` profile ([docs/testing/dsql-live-suites.md](../docs/testing/dsql-live-suites.md)).

### `tla/40_dispatch_handoff.tla`

The [activity mapping](refinement/activity-dispatch.md) records concrete slot and
incarnation identity and the boundary between modeled authority and receipt
replay. Replay is observation; its request UUID and response are checked by
executable properties, not by this model.

This models how a scheduled task reaches a worker when dispatch is durable state rather than a message.
The commit that schedules a task also writes a dispatch row.
Queue homes find rows with discovery passes that start at the head of the queue every time.
Claiming a task writes nothing, and the run's start transition, which checks the task's incarnation, is the only fence.
Notifications, broker memory, offers and start replies may all be lost.
The design it checks is the proposed [042-durable-actionable-state](../docs/architecture/042-durable-actionable-state.md).

It is a sibling of `00_execution_contract`, not a refinement of it.
It keeps only the part of a run's state that dispatch depends on, and models the layers around it: storage rows, brokers, pollers and shard ownership.
Activity tasks have the same shape.

It checks, using weak fairness only and none on notifications:

- at most one start commits per incarnation, and never for a stale incarnation;
- a serving shard has exactly the dispatch rows its committed state derives;
- an incarnation that stays wanted, on a queue a compatible poller serves, is eventually started or no longer wanted;
- a sticky incarnation, which only a notification announces, is eventually started or converted into one that discovery can find.

Each configuration exercises one shape, and each file under `negative/` breaks one part of the protocol.
Every negative control must fail: a property that no broken variant violates proves nothing.

| Configuration | What it exercises | Expected |
|---|---|---|
| `40_dispatch_handoff.cfg` | one run, two queue homes that overlap, sticky incarnations | pass |
| `40_dispatch_handoff_order.cfg` | two runs; the head run's routing class has no poller | pass |
| `40_dispatch_handoff_migration.cfg` | writes by an old release, repaired at acquisition | pass |
| `40_dispatch_handoff_stale_rows.cfg` | stale rows left in place, with `RowsAreWanted` unchecked | pass |
| `negative/40_dispatch_handoff_no_incarnation_check.cfg` | a start that doesn't check the incarnation | a start-safety invariant fails |
| `negative/40_dispatch_handoff_bare_limit.cfg` | discovery that reads a fixed window with no paging | `EventuallyResolved` fails |
| `negative/40_dispatch_handoff_durable_cursor.cfg` | discovery that continues after a durable cursor | `EventuallyResolved` fails |
| `negative/40_dispatch_handoff_no_sticky_timeout.cfg` | no sticky schedule-to-start timeout | `StickyConverts` fails |
| `negative/40_dispatch_handoff_reconcile_skips_inserts.cfg` | acquisition that leaves rows missing | `WantedRowsExist` fails |
| `negative/40_dispatch_handoff_reconcile_skips_deletes.cfg` | acquisition that leaves stale rows | `RowsAreWanted` fails |

The stale-rows configuration shows why the two row invariants are separate: a stale dispatch row only costs an offer that the start transition rejects, while a missing one can stop work for ever.

### `tla/41_dispatch_repair.tla`

This concrete companion adds three blocked ordering positions before a serviceable
task, one- and two-row discovery slices, a retained failure retry, and interrupted
acquisition across the NULL/true recovery phases and the complete home-row walk.
Each repair changes at most one run. `abstractRow` holds the acquisition image
while the home is non-serving: intermediate repairs are stuttering steps and
`Activate` is the abstract `Reconcile` step. `NonServingStutters` also checks that
an interrupted repair cannot expose its partial row image. The model uses weak fairness for
protocol actions and a finite `MaxFaults` budget; it assumes no notification delivery.
It does not model competing durable owners or replace the deferred lease fence.

| Concrete action/property | Code and test anchor |
|---|---|
| `Slice`, `EventuallyResolved` | `discovery.rs`: continuation survives a page slice; `workflow_dispatch_live_query_plans` traverses equal-time deep pages |
| `Start`, `Retry`, `AtMostOneStartPerIncarnation` | Kernel logical task sequence and start validation; storage atomic reference traces |
| `Repair`, `NextWalk`, both row invariants | `recovery.rs::sweep_shard_inner` and `reconcile_workflow_dispatch_run`; `workflow_dispatch_generated_complete_repair` on both stores |
| `Fault`, `Activate`, `EventuallyServing`, `NonServingStutters` | `serving_gate.rs`, acquisition cleanup and `runtime::repair_tests`; no partial acquisition serves |

TLC 1.7.4 and tla-rs 0.21.2 agreed on 2026-10-08. Every positive configuration
of both `40_dispatch_handoff` and `41_dispatch_repair` passed. Every negative
configuration failed on its target. The existing stale-rows positive configuration
still omits `RowsAreWanted`; its weaker result does not establish complete repair.

| New configuration | Both checkers |
|---|---|
| `41_dispatch_repair.cfg` | Pass; 264 reachable states |
| `41_dispatch_repair_pages.cfg` | Pass; 209 reachable states |
| `negative/41_dispatch_repair_restart_slices.cfg` | `EventuallyResolved` fails |
| `negative/41_dispatch_repair_retained_retry.cfg` | `AtMostOneStartPerIncarnation` fails |
| `negative/41_dispatch_repair_skips_deletes.cfg` | `RowsAreWanted` fails |

Run from `spec/tla`, with `TLC_JAR` pointing to the installed TLC jar. Substitute
each module's positive and negative configuration for `CONFIG`:

```sh
java -cp "$TLC_JAR" tlc2.TLC -workers 2 -metadir /tmp/tlc-dispatch -config CONFIG MODULE.tla
tla MODULE.tla --config CONFIG --max-states 20000000 --max-depth 10000
```

For TLC's liveness negatives, which report a generic temporal violation, the run
also repeated each negative with only its target in `PROPERTIES`. All four target
checks failed: bare limit, durable cursor and restart-every-slice on
`EventuallyResolved`, and disabled sticky timeouts on `StickyConverts`.

### `tla/42_outbound_discovery.tla`

This models the outbound driver's discovery rounds over a fixed set of active
home/kind streams, bounded by pages per round, global admission and admission per
home. Rows arrive without fairness; admitted attempts weakly fairly retire or fail
back to pending, and completions can interleave with visits. A full home is visited
without reading a page. Payloads, row cursors, backoff, wakes and ownership changes
are outside this scheduling model.

Four rules choose the next stream:

- `reset`: walk in home/kind key order, then set `previous` to the round's first
  stream after every round. The next round starts after that stream.
- `continue`: keep walking after the last stream, across round boundaries.
- `full`: use `reset` only when the round visited every stream; otherwise continue.
- `home`: walk homes in key order. Read each home's kinds in key order from its
  offset, wrapping within the home, then advance that offset by one. Carry a
  partial home visit into the next round.

`TypeOK` checks the state domains. `AdmissionBounded` checks both admission caps.
`BoundedWait` checks the maximum number of other stream visits between reads:
N + K − 2 visits, or 2N − 2 when a round can visit every stream. With one home,
the two bounds are equal. `FirstPickRotates` requires every kind to lead its home
infinitely often under weakly fair rounds and attempt completion. The Rust property
also checks the stronger bound: every kind leads once in every K home visits.

`EventuallyOffered` fails under every rule. A home's kinds share its admission,
and nothing is reserved: another kind can take the available slots before a
pending row is offered. Fair stream visits and rotating first picks do not imply
fair delivery of every row. This property is retained in the module as a limit,
not enabled in the five configurations.

| Configuration | What it exercises | Expected |
|---|---|---|
| `42_outbound_discovery.cfg` | `home`, two homes, two kinds, two visits per round | pass |
| `42_outbound_discovery_one_home.cfg` | `home`, one home, three kinds, up to a full sweep per round | pass |
| `negative/42_outbound_discovery_continue.cfg` | `continue`, one home and full sweeps | `FirstPickRotates` fails |
| `negative/42_outbound_discovery_full_sweeps.cfg` | `full`, two homes and partial rounds | `FirstPickRotates` fails |
| `negative/42_outbound_discovery_reset.cfg` | `reset`, two homes and partial rounds | `BoundedWait` fails |

Code anchors below are in `crates/tokeira-runtime/src/outbound/` unless stated otherwise.

| Action/property | Code and test anchor |
|---|---|
| `Init` | `driver.rs::Discovery::default`; empty admission and attempt maps in `run` |
| `Arrive` | `RunRepository::commit_transition`; `driver_tests.rs::many_homes_discover_new_row_within_one_cycle` commits after discovery has started |
| `Tick`, `EndKeep` | `driver.rs::run`, tick/wake selection and bounded round; partial visits survive the round |
| `VisitHomeWithin`, `VisitHomeLast` | `driver.rs::Discovery::next`; `driver_tests.rs::discovery_rotates_each_homes_starting_kind` checks order, wrap and offset advancement |
| `VisitKeyOrder`, `EndFirst` | Comparison rules with no current code: `reset` is the driver's rule before per-home rotation; `continue` and `full` are alternatives the model rejects |
| `VisitStream`, `AdmissionBounded` | `driver.rs::run`, `MAX_IN_FLIGHT` and `MAX_PER_HOME`; `admission_is_bounded_and_shutdown_drops_blocked_attempts`, `blocked_home_cannot_take_another_homes_admission` |
| `Retire`, `Fail` | `driver.rs::run`, joined attempts and retry backoff; `finite_rounds_find_lower_insertions_and_pass_failing_first_rows` |
| `TypeOK` | `driver.rs::Discovery`, `StreamKind`, typed work keys and counters |
| `BoundedWait` | `discovery_rotates_each_homes_starting_kind`, `many_homes_discover_new_row_within_one_cycle` |
| `FirstPickRotates` | `discovery_rotates_each_homes_starting_kind`, `backlogged_first_kind_cannot_starve_another_kind`: 32 homes, 128 first-kind rows and one cancellation, delivered within four home visits |
| `EventuallyOffered` (not guaranteed) | Shared home admission in `driver.rs::run`; the starvation regression checks immediate completions, not all possible interleavings |

TLC 1.7.4 and tla-rs 0.24.11 agree on every configuration.
Each liveness negative lists only its target in `PROPERTIES`, so TLC's generic
temporal violation identifies that property.

| Configuration | Both checkers |
|---|---|
| `42_outbound_discovery.cfg` | Pass; 2,088 distinct states |
| `42_outbound_discovery_one_home.cfg` | Pass; 640 distinct states |
| `negative/42_outbound_discovery_continue.cfg` | `FirstPickRotates` fails |
| `negative/42_outbound_discovery_full_sweeps.cfg` | `FirstPickRotates` fails |
| `negative/42_outbound_discovery_reset.cfg` | `BoundedWait` fails |

## What this first spec does **not** model

This is just as important as what it *does* model.

Out of scope for `00_execution_contract`:

- `current_execution` and single-current-run semantics,
- request dedupe persistence,
- atomic storage commit,
- bundle leases / epochs / stale routing,
- edge pollers and broker reservations,
- sticky routing,
- projections and visibility,
- archival,
- autoscaling,
- multi-run interactions.

Those belong to later specs.

The first spec should answer only this question:

> Given a run's current semantic state, which commands are allowed, and what semantic transition do they produce?

That question is the heart of `tokeira-kernel`.

## Why start with the kernel

Tokeira's architecture is intentionally layered:

- the **kernel** owns semantic transitions,
- **storage** owns atomic durability,
- **runtime** owns activation, routing, parking, and broker interactions,
- **projection** owns derived read models.

The first spec should therefore map to the pure part of the system first.
If the kernel is unclear, every higher-level protocol becomes harder to reason about.

## How to read the first spec

If you are new to TLA+, read `00_execution_contract.tla` in this order:

1. The large header comment at the top.
2. The helper definitions:
   - `NoWFT`
   - `ScheduleWorkflowTask`
   - `TypeInvariant`
3. `Init`
4. The actions:
   - `Start`
   - `Signal`
   - `WorkflowTaskStarted`
   - `WorkflowTaskComplete*`
   - `ActivityResolved`
   - `TimerDue`
5. `Next`
6. `Spec`
7. The invariants at the end.

Do **not** try to learn all of TLA+ before reading the file.
Read it as a very explicit state machine.

## Running the spec on Apple Silicon macOS

There are two sane paths:

- **recommended:** VS Code + TLA+ extension
- **minimal:** command-line TLC

### Option A: VS Code (recommended)

1. Install Java 17.

   The easiest Homebrew route on Apple Silicon is:

   ```bash
   brew install --cask temurin@17
   export JAVA_HOME=$(/usr/libexec/java_home -v 17)
   java -version
   ```

2. Install Visual Studio Code.
3. Install the **TLA+** extension from the TLA+ Foundation (`tlaplus.vscode-ide`).
4. Open the repository root, or at minimum the `spec/` directory, in VS Code.
5. Open `spec/tla/00_execution_contract.tla`.
6. Use the command palette and search for `TLA+`.
7. Run the parser / model checker commands from the extension.

If the extension cannot find Java, set the extension's `Java Home` setting or make sure `java` is visible on your `PATH`.

### Option B: command-line TLC

1. Install Java 17 as above.
2. Download `tla2tools.jar` from the `tlaplus/tlaplus` releases page.
3. From this directory, run:

   ```bash
   cd spec/tla
   java -jar /path/to/tla2tools.jar -config 00_execution_contract.cfg 00_execution_contract.tla
   ```

### Optional: parsing only

If you want only a syntax / parsing pass:

```bash
cd spec/tla
java -cp /path/to/tla2tools.jar tla2sany.SANY 00_execution_contract.tla
```

### Checking the protocol models with TLC

Run every configuration of a module. Those at the top level must pass, and each one under `negative/` must report a violation.
`-metadir` keeps TLC's working files out of the tree.

```bash
cd spec/tla
for module in 30_bundle_lease 40_dispatch_handoff 41_dispatch_repair 42_outbound_discovery; do
  for cfg in "$module"*.cfg negative/"$module"_*.cfg; do
    java -cp /path/to/tla2tools.jar tlc2.TLC -workers auto -metadir /tmp/tlc -config "$cfg" "$module.tla"
  done
done
```

The largest configurations, `40_dispatch_handoff.cfg` and `30_bundle_lease_for_update.cfg`, explore about 240,000 and 285,000 distinct states and take about a minute each; the others take seconds.

### Option C: tla-rs

[tla-rs](https://github.com/fabracht/tla-rs) is a TLA+ model checker written in Rust that reads the same modules and configuration files.
It is useful for fast feedback and for its interactive explorer (`-i`).
Treat TLC as the reference, and record a result only when both agree.

```bash
cargo install tla-checker --version 0.24.11 --locked
cd spec/tla
tla 40_dispatch_handoff.tla --config 40_dispatch_handoff.cfg --max-states 20000000 --max-depth 10000
```

With 0.24.11, tla-rs and TLC 1.7.4 agree on every configuration of every module here: each positive configuration passes with the same number of distinct states, and each negative fails on its target.

Its default limits (1,000,000 states and depth 100) are smaller than models like this may need, so set them explicitly.
It runs on one thread, so the largest configurations take a few minutes.

If the two checkers disagree, TLC is the reference.
Write each action as a flat conjunction, one action per outcome, rather than branching with `CASE` or `IF` over primed assignments.
tla-rs 0.21.2 misjudged the fairness of the branching form in a draft of `30_bundle_lease`.

## What TLC will do on the first run

The configuration deliberately uses:

- a tiny finite set of activity IDs,
- a tiny finite set of timer IDs,
- a small `MaxTransitions` bound.

That keeps the state space finite and makes the first model-check run fast enough for a newcomer.

If you make the constants much larger, TLC will explore far more states.
That is not wrong, but it is easy to surprise yourself.

## How this should evolve

As Tokeira evolves, the intended spec sequence is:

1. `00_execution_contract.tla`
2. `10_history_authority.tla`
3. `20_current_execution.tla`
4. `30_bundle_lease.tla`, written.
5. `40_dispatch_handoff.tla`, written. It replaces the planned `40_broker_reservations.tla`: claiming a task writes nothing, so there are no broker reservations to model.
6. `41_dispatch_repair.tla`, written.
7. `42_outbound_discovery.tla`, written.
8. `70_projection_prefix.tla`

Each later spec should either:

- refine an earlier spec, or
- state clearly why it is a sibling protocol.

## Contribution rules for spec changes

When changing `tokeira-kernel`:

1. Check whether the semantic contract changed.
2. If it did, update `refinement/kernel.md`.
3. If it changed allowed transitions or state meaning, update `00_execution_contract.tla`.
4. If the bug lives in storage/runtime/projection instead, add or update the later spec in the correct layer.

The aim is not maximal formality.
The aim is to keep the executable specification aligned with the executable system.
