# Bundle Lease Fence — Bugfix Design

## Overview

Every write of a run's authoritative state, under controller-managed placement, carries a fence: its execution home,
its owner and its epoch. In its own transaction the write reads the home's lease row `FOR KEY SHARE` and commits
nothing unless the row still names that owner at that epoch.
- **The epoch becomes a key column,** through a unique index on `(shard_id, epoch)`. A takeover or a release then
  writes a key column, which conflicts with every fence read of the row: whichever of the two commits last fails.
- **Only a change of ownership writes the epoch.** Renewal updates the expiry without reading the row `FOR UPDATE`, and
  the owner's own re-acquisition refreshes the expiry without writing the epoch. Neither conflicts with a fence read,
  so the owner's writes never abort one another or a renewal.
- **A lost fence is final.** A write that finds another owner or epoch answers a typed lease-loss error, which no lane
  retries; the runtime drops the home.
- **An owner stops at its own expiry.** It stops admitting writes once its lease has expired by its own clock, so a
  takeover isn't starved by writes that keep landing first.

This is the protocol `spec/tla/30_bundle_lease.tla` checks. `crates/tokeira-storage/tests/dsql_lease_fence.rs` confirms
on a real cluster each of the DSQL conflict rules the model assumes.

## Glossary

- **Lease row:** the `shard_lease` row of an execution home: owner, epoch, expiry and endpoint.
- **Fence:** an `ExecutionHomeFence`: the execution home, the owner and the epoch a write was admitted under.
- **Fence read:** `SELECT owner, epoch FROM shard_lease WHERE shard_id = $1 FOR KEY SHARE`, in the write's own
  transaction.
- **Fenced write:** a transaction that performs a fence read and commits only if the row matches its fence (bugfix 2.2).
- **Change of ownership:** an acquisition by another owner, a takeover of an expired lease, or a release. Each writes
  the epoch.
- **Refresh:** an acquisition by the owner of an unexpired lease. It extends the expiry and keeps the epoch.
- **Renewal:** the owner's periodic extension of its expiry.
- **Local deadline:** the instant after which a node treats its lease as expired: the start of the request that last
  acquired or renewed it, plus the lease duration.
- **Lease-loss error:** the typed storage error a fenced write answers when the row doesn't match its fence (2.4).

## How this maps onto Tokeira's architecture

v1.31.0's SQL stores take a shared lock on the shard's row in every workflow write and compare the shard's range id,
and a change of ownership takes the row's exclusive lock (`txExecuteShardLocked` and `readLockShard`,
`common/persistence/sql/execution.go:39-57` and `common/persistence/sql/shard.go:124-175 @ v1.31.0`). Tokeira keeps
that shape, with three departures:

1. **Conflicts at commit, not locks.** DSQL takes no locks. A fence read and a write of a key column conflict, and of
   the two transactions the one that commits last fails. A takeover therefore doesn't wait for writes in flight, as
   v1.31.0's exclusive lock waits for shared ones: it either aborts them or is aborted by them.
2. **The owner stops itself.** Since a fenced write that lands first aborts a concurrent takeover, an owner that keeps
   writing could starve every takeover. v1.31.0's locks can't starve the exclusive locker that way. So the owner stops
   admitting writes at its local deadline (2.6). The model's `LapsedLeaseResolves` depends on exactly this; safety
   doesn't.
3. **Owner and epoch.** v1.31.0 compares the range id alone. The fence compares the owner and the epoch, as the model
   does. The epoch changes with every change of ownership, so the epoch decides; the owner is a second check that costs
   nothing, since the read returns the row.

Writes outside a run's authoritative state keep their current behaviour (3.6). A purge removes rows of a run that has
no authoritative state left, so it stays ownership-independent (3.4).

## Bug Details

### Bug Condition

Under controller-managed placement, a write reaches commit after its node has lost the home, and its check of the lease
either ran in another transaction (1.1), read the lease plainly (1.2), or read a row whose epoch no write of a key
column changes (1.3). Or a write isn't fenced at all (1.6).

### Examples

- **A check before the write.** Node A admits a transition at epoch 4. `commit_transition_for_bundle` reads epoch 4 and
  rolls back. A's lease expires, node B takes it over at epoch 5, and B commits a transition of the same run. A's commit
  then runs with no check and commits over B's.
- **A plain read inside the write.** Node A deletes a run. Its deletion reads epoch 4 plainly. B takes the lease over
  and commits. A's deletion commits after, since a plain read conflicts with nothing.
- **A renewal that aborts the owner.** With fence reads in place but renewal still reading `FOR UPDATE`, every renewal
  of A's lease would abort A's writes in flight.
- **A lapsed owner.** A's clock passes its expiry but A keeps admitting. Each of A's fenced writes that commits before
  B's takeover aborts the takeover, and B's next attempt meets the next of A's writes.

## Expected Behavior

### Preservation Requirements

- A single node that assigns itself every home writes as today (3.1).
- The owner's writes commit as today, and renewals never abort them (3.2).
- Acquisition, renewal and release answer as their spec says (3.3).
- A purge runs from any node (3.4).
- A commit's own checks are unchanged (3.5).
- Other tables' writes stay unfenced (3.6).

## Root Cause

- `commit_transition_for_bundle` checks the epoch in a transaction it rolls back, then commits with `ShardEpoch::ZERO`.
- `commit_transition` and `delete_run_for_bundle` read the epoch with a plain `SELECT`.
- `shard_lease` has no unique index that includes `epoch`.
- Renewal reads the row `FOR UPDATE`; refresh assigns the epoch its current value.
- Materialization and the recovery switch have no lease read.
- A fence mismatch is reported as an ordinary conflict, which the lane retries.
- The local deadline exists only with dispatch reconciliation enabled.

## Correctness Properties

Property 1: No stale owner commits

_For any_ interleaving, on DSQL, of fenced writes (transition commits, deletions' first transactions, materialization
transactions and recovery switches) with renewals, refreshes, takeovers and releases, a fenced write SHALL commit only
if, at its commit, the lease row names its owner at its epoch.

**Validates: Requirements 1.1, 1.2, 1.3, 1.6, 2.1, 2.2**

Property 2: The owner's writes never abort each other

_For any_ interleaving, on DSQL, of one owner's fenced writes with its renewals and refreshes, and no change of
ownership, no fenced write and no renewal SHALL fail because of another of them.

**Validates: Requirements 1.4, 1.5, 2.3, 3.2**

Property 3: A lapsed lease is taken over

_For any_ owner whose lease reaches its local deadline, the owner SHALL admit no write for that home after it. Once the
writes it admitted before have finished, a takeover SHALL succeed, whatever the owner's write load.

**Validates: Requirements 1.8, 2.6**

Property 4: A lost fence is final

_For any_ fenced write that finds no lease row, another owner or another epoch:
- the write SHALL change nothing, and storage SHALL answer the lease-loss error;
- the lane SHALL not retry it, and the runtime SHALL cancel the home's acquisition and admit no further write for it;
- a write that DSQL aborted because a change of ownership committed first SHALL end, on its retry, with the lease-loss
  error.

**Validates: Requirements 1.7, 2.4, 2.5**

Property 5: Only a change of ownership writes the epoch

_For any_ acquisition, refresh, renewal and release, on either store, the epoch SHALL change exactly when ownership
changes. No statement of a refresh or a renewal SHALL write the epoch, and no renewal SHALL read the row `FOR UPDATE`.

**Validates: Requirements 1.4, 1.5, 2.3, 3.3**

## Fix Implementation

### The index (`crates/tokeira-storage/migrations/`)

- One migration at the next free number: `CREATE UNIQUE INDEX ASYNC IF NOT EXISTS idx_shard_lease_epoch ON shard_lease
  (shard_id, epoch)`.
  - `shard_id` alone is already unique. The index exists only to make `epoch` a key column, which DSQL's conflict
    rules need. `dsql_lease_fence` shows the difference live: without such an index a takeover doesn't fence a write.
  - Schema readiness waits for the build, as it does for the other asynchronous indexes. Until then the fence read
    wouldn't conflict with a takeover.
- The bookkeeping follows the earlier migrations: the schema contracts in `crates/tokeira-storage` and
  `crates/tokeira-build-info`, the baseline lock, the build information, the migration and schema-bootstrap tests, and
  the migration paragraph of `crates/tokeira-storage/AGENTS.md`.
- Upgrade with every node stopped, as the release note says. A node of the earlier release writes with a plain read,
  so it mustn't run beside one of this release.

### The fence (`crates/tokeira-storage`)

- `ExecutionHomeFence { home: ShardId, owner: String, epoch: ShardEpoch }`. The workflow-dispatch design names this
  type; this change defines it.
- Each fenced write method takes a `WriteFence`: either `Unfenced`, for a node that assigns itself every home, or
  `Lease(ExecutionHomeFence)`. It replaces the bare `ShardEpoch` that `commit_transition`, `commit_transition_for_bundle`
  and `delete_run_for_bundle` take today, where zero meant unfenced. `materialize_reset_successor` and
  `abandon_materialization` gain it.
- **On DSQL** the fence read is the first statement of the transaction. It compares the row's owner and epoch with the
  fence. On a match the transaction goes on, and on no row or a mismatch it rolls back and returns `LeaseFenceLost
  { home, epoch, found }`, where `found` is the row's owner and epoch, if any. The read uses the primary key's equality
  predicate, as the lease's other reads do.
- **The fenced transactions:**
  - `commit_transition`: the fence read replaces today's plain read.
  - `commit_transition_for_bundle`: the separate check goes, and the fence passes into the commit's own transaction.
  - `delete_run_for_bundle`: the fence read replaces today's plain read in the first transaction.
  - `materialize_reset_successor`: each transaction reads the fence before the record: the record's insert, every copy
    page and the final transaction.
  - `abandon_materialization`: a takeover's recovery passes its fence, so a recovery that has lost the lease switches
    nothing. A purge's own switch stays unfenced, since a purge runs from any node and a successor's run key is never
    materialized twice ([bounded-bulk-writes](../bounded-bulk-writes/design.md)).
- **In memory** the store compares the fence with its lease map under its lock, which already orders the write with any
  change of ownership. It answers the same `LeaseFenceLost`.

### The lease repository (`leases.rs`, and the in-memory store's)

- **Acquisition** is three statements, each of which writes the epoch only if ownership changes:
  - the insert of a new row at epoch 1, as today;
  - a refresh: `UPDATE … SET lease_expiry, node_endpoint WHERE shard_id = $1 AND owner = $2 AND lease_expiry > $now
    RETURNING epoch`, which leaves the epoch alone;
  - a takeover: `UPDATE … SET owner, epoch = epoch + 1, lease_expiry, node_endpoint WHERE shard_id = $1 AND (owner IS
    NULL OR lease_expiry <= $now) RETURNING epoch`. That includes the same owner after expiry, as today.

  Otherwise the acquisition reads the holder and answers `Rejected`, as today.
- **Renewal** is `UPDATE … SET lease_expiry, node_endpoint WHERE shard_id = $1 AND owner = $2 AND epoch = $3`. One
  changed row is `Renewed`. Otherwise it reads the row and answers `Rejected`, as today. Nothing reads the row
  `FOR UPDATE`.
- **Release** stays an update that clears the owner and advances the epoch.
- A renewal and a takeover update the same row, so DSQL commits at most one of them if they overlap. A renewal that
  loses answers a conflict, and the renewer's next attempt answers `Rejected`.

### The runtime

- **Building the fence.** Under controller-managed placement, every writer builds its fence from the ownership state
  that admitted it: the home's epoch from `ShardOwner`, and the node's owner identity. Otherwise it passes `Unfenced`,
  as today's zero epoch. The writers are:
  - the lane's commits;
  - the activity paths that commit directly;
  - `delete_workflow`;
  - a reset's materialization in the lane;
  - the takeover's recovery switch.

  A test audits every caller of the fenced write methods, so that none passes `Unfenced` under controller-managed
  placement.
- **A lost fence.** `LeaseFenceLost` maps to `NotShardOwner`, which the lane doesn't retry. The runtime then cancels
  the home's acquisition, as a lost renewal does, so the home admits nothing more. A serialization failure is still
  retried, and the retry's fence read finds the new owner (Property 4).
- **The local deadline.** Under controller-managed placement, an acquisition sets the deadline from its request's start
  plus the lease duration, and each committed renewal moves it the same way, whether or not dispatch reconciliation is
  enabled. `ShardAcquisition::valid` and `OwnedShard::active` already refuse admission past the deadline. The renewer's
  loss handling already cancels the acquisition when a renewal can't commit in time.

### Specs this changes

The code PR aligns these specs with the fix:
- [dsql-shard-leasing](../dsql-shard-leasing/design.md): acquisition's statements and renewal without `FOR UPDATE`. Its
  requirements keep their outcomes.
- [commit-fencing-correctness](../commit-fencing-correctness/design.md): its Property 1 holds that an in-transaction
  read fences through DSQL's optimistic concurrency. A plain read doesn't, so the property points here, and so does
  criterion 2.1, whose separate check this change removes.
- [dsql-core-persistence](../dsql-core-persistence/requirements.md): the commit's epoch validation becomes the fence
  read, comparing the owner as well.
- [bounded-bulk-writes](../bounded-bulk-writes/design.md): the materialization's transactions and the recovery switch
  are fenced. The known risk of an unfenced switch is closed.
- [workflow-dispatch](../workflow-dispatch/design.md): its `ExecutionHomeFence` is this one. Its tasks 17 and 18 stay
  its own.

## Testing Strategy

### Exploratory Bug Condition Checking

Live, on an ephemeral cluster, with a test-only pause point in the DSQL repository that holds a write after its lease
check and before its commit, so that a takeover commits in between:
- a transition's commit through `commit_transition_for_bundle`;
- a transition's commit through `commit_transition` with an epoch;
- a deletion's first transaction.

On the code before the fix, each stale write commits, so each test fails. After the fix each stale write is refused
with `LeaseFenceLost`, or aborted by DSQL and then refused on its retry.

### Live DSQL

The live suite runs under the `dsql-live` profile ([docs/testing/dsql-live-suites.md](../../../docs/testing/dsql-live-suites.md)).
It covers:
- the exploration cases above;
- a materialization stopped between transactions, whose next transaction is refused after a takeover;
- a recovery switch under a lost lease, which switches nothing;
- renewals and refreshes interleaved with the owner's fenced writes, none of which aborts another;
- a fenced write that commits before a takeover, which aborts the takeover.

### Property-Based Tests

- Properties 1 and 2 run live on DSQL, over at least 100 generated interleavings of fenced writes, renewals, refreshes,
  takeovers and releases, driven by the pause points and barriers, with no sleeps.
- Property 3 runs in `tokeira-runtime` with an injected clock: an owner past its deadline admits nothing, and a
  takeover then succeeds under a steady write load.
- Property 4 runs in `tokeira-runtime` on the in-memory store, and live for the retry after an abort.
- Property 5 runs on both stores, over generated sequences of acquisitions, refreshes, renewals, releases and
  takeovers. The DSQL statements' text is also checked: no refresh or renewal writes `epoch`, and no renewal reads
  `FOR UPDATE`.

### Negative controls

Each is patched in alone, run, then reversed so the tree is byte-identical. Each must fail the test that covers it:
- the fence read made a plain read;
- the fence checked in a separate transaction;
- the fence read's comparison of the epoch removed;
- renewal reading the row `FOR UPDATE`;
- a refresh that writes the epoch;
- `LeaseFenceLost` retried as a conflict;
- the local deadline left unset;
- a materialization copy page without its fence read;
- one of the runtime's writers passing `Unfenced` under controller-managed placement.

The missing index has no runtime control, since a migration doesn't come out of a live schema. `dsql_lease_fence`
already shows DSQL's behaviour without it, and a unit test asserts the migration's index.

### Preservation Checking

- The existing lease, commit, deletion, reset and recovery tests stay green. Those that pass `ShardEpoch::ZERO` pass
  `WriteFence::Unfenced`.
- The single-node engine's startup keeps acquiring every lease without a renewer, and its writes stay unfenced.
