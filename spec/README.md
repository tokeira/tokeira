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
    40_dispatch_handoff.tla
    40_dispatch_handoff.cfg
    40_dispatch_handoff_order.cfg
    40_dispatch_handoff_migration.cfg
    40_dispatch_handoff_stale_rows.cfg
    negative/
      40_dispatch_handoff_*.cfg
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

### `tla/40_dispatch_handoff.tla`

This models how a scheduled task reaches a worker when dispatch is durable state rather than a message.
The commit that schedules a task also writes a dispatch row.
Queue homes find rows with discovery passes that start at the head of the queue every time.
Claiming a task writes nothing, and the run's start transition, which checks the task's incarnation, is the only fence.
Notifications, broker memory, offers and start replies may all be lost.

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

### Checking `40_dispatch_handoff` with TLC

Run every configuration. The four at the top level must pass, and each one under `negative/` must report a violation.
`-metadir` keeps TLC's working files out of the tree.

```bash
cd spec/tla
for cfg in 40_dispatch_handoff*.cfg negative/40_dispatch_handoff_*.cfg; do
  java -cp /path/to/tla2tools.jar tlc2.TLC -workers auto -metadir /tmp/tlc -config "$cfg" 40_dispatch_handoff.tla
done
```

The main configuration explores about 240,000 distinct states and takes about a minute; the others take seconds.

### Option C: tla-rs

[tla-rs](https://github.com/fabracht/tla-rs) is a TLA+ model checker written in Rust that reads the same modules and configuration files.
It is useful for fast feedback and for its interactive explorer (`-i`).
Treat TLC as the reference, and record a result only when both agree.

```bash
cargo install tla-checker --version 0.21.2 --locked
cd spec/tla
tla 40_dispatch_handoff.tla --config 40_dispatch_handoff.cfg --max-states 20000000 --max-depth 10000
```

Its default limits (1,000,000 states and depth 100) are smaller than models like this may need, so set them explicitly.
It runs on one thread, so the main configuration takes about three minutes.

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
4. `30_bundle_lease.tla`
5. `40_dispatch_handoff.tla`, written. It replaces the planned `40_broker_reservations.tla`: claiming a task writes nothing, so there are no broker reservations to model.
6. `70_projection_prefix.tla`

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
