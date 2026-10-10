# Bugfix Requirements Document

## Introduction

A node that owns an execution home holds that home's bundle lease, a `shard_lease` row with an owner, an epoch and an
expiry. A node takes the lease over once it has expired, and advances the epoch as it does. Every write of a run is
meant to be fenced by the lease: it commits only while its node still owns the home at the epoch it was admitted
under, so that a node that has lost its lease can't write over its successor.

Aurora DSQL takes no locks. It adjudicates conflicts when a transaction commits, and of two conflicting transactions
the one that commits last fails ([concurrency control](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-concurrency-control.html)).
For one row, AWS documents these rules:
- a plain `SELECT` conflicts with nothing;
- two `SELECT … FOR KEY SHARE` reads don't conflict;
- a `FOR KEY SHARE` read doesn't conflict with an `UPDATE` of non-key columns;
- it conflicts with a write of a key column, and with `SELECT … FOR UPDATE`, which DSQL treats as a write.

A key column belongs to a unique index. `crates/tokeira-storage/tests/dsql_lease_fence.rs` confirms each of these
rules on a real cluster through Tokeira's own client.

The model `spec/tla/30_bundle_lease.tla` checks a lease protocol against those rules:
- `epoch` is a key column, through a unique index on `(shard_id, epoch)`;
- every run commit, creation and deletion reads the lease row `FOR KEY SHARE` in its own transaction, and checks owner
  and epoch;
- only an acquisition or a release writes `epoch`, and renewal updates only the expiry, without `FOR UPDATE`;
- an owner whose lease has expired by its own clock stops starting writes.

Under it, no write of a node that has lost its lease commits (`NoStaleOwnerCommit`), renewals and the owner's other
writes never abort the owner's writes (`NoSpuriousAbort`), and an expired lease is eventually taken over
(`LapsedLeaseResolves`). The model's negative controls reproduce today's code, and each fails a property.

Temporal v1.31.0 fences writes the same way on its SQL stores. Every write of a workflow takes a shared lock on the
shard's row in its own transaction and checks the shard's range id (`txExecuteShardLocked` and `readLockShard`,
`common/persistence/sql/execution.go:39-57` and `common/persistence/sql/shard.go:152-175 @ v1.31.0`). A change of
ownership takes the row's exclusive lock (`lockShard`, `common/persistence/sql/shard.go:124-150 @ v1.31.0`). On
PostgreSQL those are `SELECT … FOR SHARE` and `SELECT … FOR UPDATE`
(`common/persistence/sql/sqlplugin/postgresql/shard.go:22-23 @ v1.31.0`). DSQL has no shared lock; `FOR KEY SHARE`
against a write of a key column gives the same exclusion at commit.

The fence applies under controller-managed placement, where nodes acquire and renew homes' leases as the controller
directs, and pass their epochs to their writes. A single node that assigns itself every home acquires those leases
once at startup, renews none, and passes no epoch to its writes; this spec leaves that path as it is.

## Bug Analysis

### Current Behavior (Defect)

1.1 WHEN `commit_transition_for_bundle` commits a transition under a non-zero epoch THEN it reads `shard_lease.epoch` in
a transaction of its own, rolls that transaction back, and commits the transition in a second transaction with
`ShardEpoch::ZERO`, which checks nothing (`crates/tokeira-storage/src/dsql/run_repository/commit.rs:248-283`). A
takeover that commits between the two lets a node that has lost its lease commit. [commit-fencing-correctness](../commit-fencing-correctness/bugfix.md)
criterion 2.1 required the check and the write in one transaction; the separate check remains.

1.2 WHEN `commit_transition` or `delete_run_for_bundle` checks a non-zero epoch inside its transaction THEN it reads
the lease with a plain `SELECT` (`commit.rs:46-73`; `crates/tokeira-storage/src/dsql/run_repository/delete.rs:61-80`).
A plain read conflicts with nothing. A node that read the old epoch before a takeover commits after it, to a run its
successor may already be writing.

1.3 WHEN a takeover advances `epoch` THEN even a `FOR KEY SHARE` read of the lease wouldn't conflict with it: `epoch`
is outside every unique index, because `shard_lease` is keyed on `shard_id` alone
(`crates/tokeira-storage/migrations/V002__shard_lease.sql`), so the takeover's update is an update of non-key columns.

1.4 WHEN a node renews its lease THEN it reads the row `FOR UPDATE` before updating the expiry
(`crates/tokeira-storage/src/dsql/run_repository/leases.rs:166-196`). DSQL treats that read as a write of the row.
Under a `FOR KEY SHARE` fence, every renewal would abort the owner's own writes in flight.

1.5 WHEN the owner of an unexpired lease acquires it again THEN the acquisition writes `epoch` with its current value
(`epoch = CASE WHEN owner = $2 AND lease_expiry > $4 THEN epoch ELSE epoch + 1 END`, `leases.rs:66-90`). Once `epoch` is
a key column, whether DSQL counts that as a write of a key column is a question the protocol shouldn't depend on.

1.6 WHEN a reset materializes its successor, or a takeover's recovery abandons a materialization THEN none of their
transactions read the lease at all ([bounded-bulk-writes](../bounded-bulk-writes/design.md)). The materialization's
final transaction creates a run, and the abandonment switches a record that another owner's materialization may be
using.

1.7 WHEN a fenced write finds another owner or another epoch THEN storage answers `CommitResult::Conflict` or
`DeleteRunResult::Conflict`, which the lane retries as an optimistic-concurrency conflict up to its retry limit
(`crates/tokeira-runtime/src/lane.rs`), although the node has lost the home and no retry can succeed.

1.8 WHEN a node's lease has expired by its own clock THEN, unless workflow dispatch reconciliation is enabled, the node
keeps starting writes: the local admission deadline is set only in that mode
(`crates/tokeira-runtime/src/runtime/membership.rs`). Under DSQL's rule a fenced write that lands first aborts a
concurrent takeover, so an owner that keeps writing after its expiry can starve every takeover.

### Expected Behavior (Correct)

2.1 A migration at the next free number SHALL add a unique index on `shard_lease (shard_id, epoch)`, built `ASYNC`, so
that `epoch` is a key column. The schema SHALL be ready only once the index is built, as for the existing asynchronous
indexes.

2.2 Every write that changes a run's authoritative state, under controller-managed placement, SHALL be fenced in its
own transaction:
- it SHALL read the execution home's lease row `FOR KEY SHARE`;
- it SHALL commit nothing unless the row names the write's owner at the write's epoch.

The fenced writes SHALL be:
- a transition's commit, through `commit_transition` and `commit_transition_for_bundle`, with no separate check;
- a deletion's first transaction;
- every transaction of a reset's materialization: its record, its copy and its final transaction;
- a takeover's switch of a materialization record to purging.

2.3 Only an acquisition that changes owner, a takeover of an expired lease, and a release SHALL write `epoch`.
- A renewal SHALL update only the expiry and the endpoint, in a conditional update on the shard, the owner and the
  epoch, without reading the row `FOR UPDATE`.
- An acquisition by the owner of an unexpired lease SHALL refresh the expiry and the endpoint without writing `epoch`.
- The outcomes of acquisition, renewal and release SHALL stay as [dsql-shard-leasing](../dsql-shard-leasing/requirements.md)
  specifies them.

2.4 WHEN a fenced write finds no lease row, another owner or another epoch THEN storage SHALL answer a typed lease-loss
error that names the home, the write's epoch and the lease it found. The write SHALL change nothing. The lane SHALL not
retry it, and the runtime SHALL treat it as the loss of the home: it SHALL cancel the home's acquisition and stop
admitting writes for it.

2.5 WHEN a fenced write fails to commit because a takeover or release committed first THEN the failure SHALL be the
serialization failure DSQL returns. The lane's retry SHALL read the lease again in its next transaction, and so SHALL
end with the lease-loss error of 2.4 rather than exhausting its retries.

2.6 A node SHALL stop admitting writes for a home once that home's lease has expired by the node's own clock, under
controller-managed placement whether or not dispatch reconciliation is enabled:
- The deadline SHALL be the start of the acquiring or renewing request plus the lease duration, so it never passes the
  expiry the lease records.
- A renewal SHALL move the deadline only after it commits.
- A write admitted before the deadline SHALL still be fenced at commit.

2.7 The runtime SHALL pass the fence as one value, the execution home with its owner and epoch, from where it admits a
write to the storage call that writes. Every caller SHALL build it from the same ownership state that admitted the
write. Under controller-managed placement, no write of a run's authoritative state SHALL go to storage unfenced.

### Unchanged Behavior (Regression Prevention)

3.1 A single node that assigns itself every home SHALL CONTINUE TO acquire the leases at startup without renewing
them, and to pass no epoch and write unfenced.

3.2 Writes by the owner of a home SHALL CONTINUE TO commit as today. A renewal, a refresh by the owner, and the owner's
other writes SHALL never abort them.

3.3 Acquisition, renewal and release SHALL CONTINUE TO answer as [dsql-shard-leasing](../dsql-shard-leasing/requirements.md)
specifies: a takeover only of an expired lease, a takeover by the same owner after expiry advancing the epoch, and an
optimistic-concurrency conflict surfaced without a silent retry.

3.4 A purge SHALL CONTINUE TO run from any node without a fence ([bounded-bulk-writes](../bounded-bulk-writes/design.md)).

3.5 A transition's commit SHALL CONTINUE TO check the run's transition sequence, its idempotency and its current
execution as today.

3.6 Writes outside a run's authoritative state SHALL CONTINUE TO run unfenced as today: the backlog spill and drain,
the timer scanner's deletion of a stale timer row, projection, visibility and control-plane tables.

### Out of Scope

- Workflow dispatch's repair transactions, and its final activation, consuming this fence: [workflow-dispatch](../workflow-dispatch/tasks.md)
  tasks 17 and 18 take that on once this fence exists.
- CHASM executions' writes, which keep their own current-run pointer and transition-count fences and run behind their
  feature gates. A later change can extend this fence to them.
- Enabling multi-node operation, placement, and the controller's assignment of homes.
- Bounds on clock skew between nodes. Safety holds whatever the clocks: a stale write is refused at commit. Only how
  soon a takeover succeeds depends on the old owner's clock (2.6).
- The in-memory store's lack of lease expiry. It keeps its lock-based check of the epoch.
