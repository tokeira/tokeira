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
5. `40_broker_reservations.tla`
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
