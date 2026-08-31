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
| CHASM | `ChasmNodeRepository`, atomic dirty-node batches, `ExpectedVersion`, current-run pointers |
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
- CHASM dirty-node batches apply all-or-nothing after every node precondition is
  checked; conflicts never force-overwrite newer state.
- Projection records are ordered inputs to a rebuildable read model, not
  authoritative workflow state.
- Storage implements persistence mechanics; transition correctness remains in
  the kernel or CHASM substrate.

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
