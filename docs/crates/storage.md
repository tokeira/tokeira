# tokeira-storage

Semantic persistence contracts plus in-memory and feature-gated Aurora DSQL
implementations. Runtime code depends on repository traits rather than physical
tables or SQL statements.

## Where it sits

The crate is the persistence boundary of the authoritative runtime and storage
plane. It stores committed workflow history and CHASM node state, enforces
optimistic and ownership fences, and exposes ordered projection records.

## Core contracts

| Area | Representative contracts |
|---|---|
| Workflow authority | `RunRepository`, `CommitResult`, request deduplication, transition audit, reset successor materialization |
| Ownership | `LeaseRepository`, `ControlRepository`, `BundleLease`, `LeaseOutcome` |
| Derived work | Dispatchable workflow/activity tasks, durable backlog, timer and timeout sweep records |
| Projection feed | `ProjectionLog`, `ProjectionRecord`, partitioned `ProjectionCursor` batches |
| CHASM | `ChasmNodeRepository`, atomic dirty-node batches, `ExpectedVersion`, archetype-keyed current-execution pointers, `CurrentExecution` scans |
| Worker control | Deployment, task-queue configuration, provenance, and Worker Compute repository contracts |
| Connections | `ConnectionDirector`, operation-class `DbClass`, admitted `DbPermit` values |

## Implementations

`InMemoryStore` implements the workflow repository, projection log, and bundle
leases for embedded use, examples, and tests. `InMemoryChasmNodeStore` and
`InMemoryWorkerComputeRepository` cover their corresponding contracts. These
stores are process-local implementations, not the concurrency reference for a
cluster.

With the `dsql` feature, the crate provides the production Aurora DSQL
foundation: migration and schema checks, IAM-authenticated connection
management, operation-class admission, `DsqlRunRepository`,
`DsqlProjectionLog`, `DsqlChasmNodeRepository`, and Worker Compute
persistence.

## Invariants

- Workflow commits are fenced by the expected transition sequence and shard
  ownership epoch.
- History, state, deduplication, audit, and the versioned projection record for a
  transition commit atomically.
- Workflow-state and history-batch blobs carry a versioned envelope; a blob of any
  other version fails to decode with `BlobFormatError` instead of being
  reinterpreted under the current layout.
- Each commit accounts the run's persisted history size in the same transaction
  as the batch it appends (`RunHistoryStats`); readers consult that statistic and
  never re-read history to derive it.
- CHASM dirty-node batches apply all-or-nothing after every node precondition is
  checked; conflicts never force-overwrite newer state.
- The current-execution pointer is keyed by `(namespace_id, archetype_id,
  business_id)`, so a business id is independent per archetype. Creating an
  execution writes its nodes and its pointer in one transaction, conditional on
  the pointer captured during start-policy evaluation; any mismatch rolls the
  nodes back, leaving nothing to be found later.
- Projection records are ordered inputs to a rebuildable read model, not
  authoritative workflow state.
- Storage implements persistence mechanics; transition correctness remains in
  the kernel or CHASM substrate.

## The current-execution pointer, and moving onto it

`chasm_current_execution` holds one row per live execution, primary-keyed by
namespace, archetype and business id, with an async index for scanning Running
pointers in that order. `scan_current_executions` pages over it, which is how the
runtime rebuilds pending effects and armed timers after a restart.

It replaces the archetype-blind `chasm_current_run`, and the move is designed to
be invisible:

- `backfill_current_executions` copies legacy rows under an archetype id, bounded
  per transaction and idempotent, and is repeated until it copies nothing. It runs
  at engine start for the activity archetype.
- Until the backfill completes, `current_run` reads the new table first and falls
  back to the old one — and only when the legacy run's root node actually carries
  the requested archetype id, so the fallback cannot answer for another archetype.
- On completion a marker row in `chasm_backfill_marker` retires the fallback. The
  in-memory repository mirrors the same pointer and marker checks.

`chasm_current_run` itself is left in place; dropping it is a later migration, so
a rollback still has its rows.

## It does not own

The crate does not choose when to run transitions, execute broker delivery,
shape public API errors, or interpret visibility filters. The DSQL visibility
store lives in `tokeira-projection`, which owns projection semantics.

## Pointers

- [Crate root](../../crates/tokeira-storage/src/lib.rs)
- [Storage-specific contract](../../crates/tokeira-storage/AGENTS.md)
- [DSQL module](../../crates/tokeira-storage/src/dsql/mod.rs)
- [Runtime](runtime.md)
- [Projection](projection.md)
