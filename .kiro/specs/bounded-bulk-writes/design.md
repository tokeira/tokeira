# Bounded Bulk Writes — Bugfix Design

## Overview

Each of the four writes becomes a series of transactions within fixed budgets:
- **The spill** keeps its one call per pass. Storage splits the call into transactions and reports how far it got, so the scanner re-publishes only what wasn't persisted.
- **A deletion** makes the run unreachable in one fenced transaction, which also records the run for purging, and DeleteWorkflowExecution returns once it commits. A purge then removes the run's other rows in pages, history last, and removes the record with the last of them.
- **A reset** records the successor as being materialized, writes the copied history and the timer rows in pages, and then writes the successor's mutable state, dispatch row and current pointer in a final transaction that removes the record. That transaction makes the successor current only if the pointer still names the run the reset found current.
- **An interrupted write is finished by a purge.** A purge recorded by a deletion runs to completion. A materialization that never reached its final transaction is purged like a deleted run.

One new table, `run_bulk_write`, holds the records. A run has mutable state or a record, never both, and that is what keeps every existing reader correct at every step without a change to any of them.

## Glossary

- **Budgets:** at most 1,000 rows changed and 4 MiB inserted or updated per transaction, and at most 512 KiB of events and of principals per history batch a reset writes (bugfix 2.1).
- **DSQL's limits:** 3,000 rows changed per transaction, deleted rows included; 1,048,576 bytes per value; and 10 MiB inserted or updated per transaction, counting keys and per-row overhead with the values, so 9 MiB of values commits and 10 MiB is refused. Deleted rows don't count toward the 10 MiB. The bugfix's introduction gives the sources.
- **Bulk-write record:** a `run_bulk_write` row naming a run whose rows are being written or removed across transactions. Its phase is `materializing` or `purging`.
- **First transaction:** a deletion's fenced transaction that appends the tombstone, makes the run unreachable and records it for purging (bugfix 2.3).
- **Purge:** the removal of a recorded run's remaining rows (bugfix 2.4).
- **Materialization:** the creation of a reset's successor (bugfix 2.7).
- **Final transaction:** the materialization's transaction that writes the successor's mutable state, dispatch row and current pointer, and removes its record.
- **Abandoned materialization:** one that will never reach its final transaction, because it failed or its node stopped.
- **Straggler:** a transaction of an abandoned materialization that runs after its record has been switched to `purging`.

## How this maps onto Tokeira's architecture

v1.31.0 splits these writes too, but around structures Tokeira doesn't have. The fix keeps v1.31.0's order and makes five departures.

1. **No delete task queue.** v1.31.0 runs a deletion from a durable task that its queue retries, and DeleteWorkflowExecution returns once it has added that task (`service/history/api/deleteworkflow/api.go:86-98`). If a failure follows the removal of mutable state, the history branch stays until a scavenger finds it and deletes it (`context_impl.go:941-963`; `scavenger.go:251-289 @ v1.31.0`). Tokeira has no task queue for deletions. Its first transaction records the purge in the same commit that makes the run unreachable, and DeleteWorkflowExecution returns then. The record is the durable task: the owning node retries it until it finishes, and a node that takes the shard over finds it. Nothing is left for a scavenger.
2. **A copy, not a fork.** v1.31.0 forks the base's history branch, sharing its nodes, and writes only the new run's own events. Tokeira copies the prefix into the successor's own rows and replays it ([workflow-reset](../workflow-reset/design.md)), so the copy is a write v1.31.0 doesn't make. The copy follows v1.31.0's order for a new run: history rows first, then the transaction that writes mutable state and makes the run current (`common/persistence/sql/execution.go:334-358, 446-474 @ v1.31.0`). An abandoned copy is purged where v1.31.0 leaves an orphan branch to its scavenger.
3. **Timer rows before the run.** v1.31.0 writes a new run's timer tasks in the transaction that writes its mutable state. Tokeira's timer rows are its timer tasks, and a successor's may not fit in one transaction. So they are written with the history, before the successor is visible, and the timer scanner keeps a due timer whose run is still being materialized.
4. **One fence for both.** The same record type fences a purge and a materialization. Every reader looks a run up by its mutable state or current pointer, and a recorded run has neither, so no reader changes.
5. **A checked move of the current pointer.** v1.31.0 creates a reset's run and moves the current pointer to it in one transaction, and moves the pointer only while it names the run the reset updates (`workflow_resetter.go:358-430`; `common/persistence/sql/execution_util.go:966-1009 @ v1.31.0`). Tokeira commits the reset on its base first and creates the successor later, so a start can land in between. The final transaction therefore checks the pointer against the run the runtime found current when it admitted the reset. It fails rather than move the pointer off a run that started since.

The backlog is a delivery optimisation ([`crates/tokeira-runtime/AGENTS.md`](../../../crates/tokeira-runtime/AGENTS.md)): losing an entry loses no work. A spill that persists part of a pass is therefore safe as long as no task is dropped.

## Bug Details

### Bug Condition

One of the four writes is asked to put into a single transaction more than 3,000 changed rows, more than 10 MiB inserted, or a value over 1 MiB. Or a start of the same workflow id lands between a reset's commit on its base and its successor's creation (1.6).

### Examples

- **A spill.** Workers stop polling a task queue while its workflows keep scheduling activities. Five seconds later the grace scanner demotes the unclaimed tasks. A pass that finds 3,001 of them, or eleven whose inputs are 1 MB each, fails, and every task goes back to the broker. After the fix the pass writes them in pages of up to 1,000 entries and 4 MiB.
- **A deletion.** A workflow that received 3,000 signals with request ids owns 3,000 history batches and 3,000 request records. DeleteWorkflowExecution fails on every attempt. After the fix it returns once the first transaction makes the run unreachable, and the purge removes the run's 6,000 rows in about seven transactions.
- **A reset over 1 MiB.** A workflow's activities returned 2 MiB of results before the reset point. The reset terminates the run, then fails to create its successor. After the fix the copied history goes into four or five batches of up to 512 KiB, written before the successor's final transaction.
- **A reset over many timers.** A workflow started 3,500 timers before the reset point. The successor's creation would write 3,500 timer rows in one transaction, and fails. After the fix the timer rows are written in four transactions before the final one.
- **A start during a reset.** A client resets a closed run and, a moment later, starts the same workflow id under a reuse policy that allows it. The start lands before the successor's final transaction. Today that transaction moves the pointer to the successor and leaves both runs open. After the fix it finds the pointer naming the started run and fails, the successor's rows are purged, and the started run stays current.

## Expected Behavior

### Preservation Requirements

- A spill that fits the budgets is one transaction (3.1), and a stored entry keeps its row (3.2).
- A deletion checks and answers as today (3.3), and readers see a deleted run as today (3.4).
- While the current pointer is unchanged, a reset's successor and the reset RPC are as today (3.5).
- The timer scanner deletes stale timers as today, except a materializing run's (3.6).
- The drain is unchanged (3.7).

## Root Cause

Each write puts a set of unbounded size into one transaction:
- `scan_grace_once` takes every expired task, and `do_persist_to_backlog` inserts them all in one transaction.
- `do_delete_run_for_bundle` runs each of `RUN_OWNED_DELETE_STATEMENTS` over all of a run's rows, in the transaction that appends the tombstone.
- `do_materialize_reset_successor` encodes the whole prefix as one value (`prefix_data`), and upserts every timer in the transaction that creates the successor. That transaction also moves the current pointer to the successor without checking what it names.
- The in-memory store has no limits, so its tests can't show any of this.

## Correctness Properties

Property 1: Every transaction stays within the budgets

_For any_ spill, deletion, purge and materialization over generated sizes — entry counts and payload sizes, rows in each run-owned table, prefix lengths and event sizes, timer counts — on either store, every transaction the write issues SHALL change at most 1,000 rows, SHALL insert or update at most 4 MiB, and SHALL hold at least one item.

**Validates: Requirements 2.1, 2.2, 2.4, 2.7**

Property 2: A failed spill keeps what it persisted and re-publishes the rest

_For any_ pass of expired tasks, and any transaction of its spill that fails, the entries of the transactions before it SHALL be stored, every other task SHALL be back in its broker, and no task SHALL be both stored and back in a broker. A spill with no failure SHALL store every entry.

**Validates: Requirements 2.2, 3.1, 3.2**

Property 3: A run has mutable state or a bulk-write record, never both

_For any_ interleaving of deletions, purges and materializations, each stopped after any of its transactions:
- no run SHALL have both mutable state and a bulk-write record;
- a run with a record SHALL be absent from every lookup: `load_run`, `resolve_execution` with or without its run id, and `find_latest_run`;
- the tombstone of a run whose first transaction committed SHALL be in the projection log.

**Validates: Requirements 2.3, 2.6, 2.7, 2.8**

Property 4: A purge finishes, removes only its run's rows, and removes history last

_For any_ run with any mix of owned rows, beside other runs' rows, and any sequence of purge attempts, each stopped after any transaction and some running at the same time, once an attempt returns success:
- no row of the run and no record SHALL remain;
- every other run's rows SHALL be unchanged;
- at no point SHALL the run have lost a history row while it still had a row in another owned table.

**Validates: Requirements 2.4, 2.5, 2.9**

Property 5: A materialized successor is complete, or invisible

_For any_ base history and fork point, any number of successor timers, and any transaction after which the materialization stops:
- Until the final transaction commits, the successor SHALL be absent from every lookup, and its record SHALL say `materializing`.
- Once it commits, the successor SHALL have no record. Its history SHALL equal the copied prefix with its principals, and its timer rows SHALL match its timers. Its History Size SHALL be the sum of its batches' encoded sizes, and its state SHALL be what today's materialization derives.

**Validates: Requirements 2.7, 2.8, 2.11, 3.5**

Property 6: An abandoned materialization leaves nothing behind

_For any_ materialization stopped before its final transaction, purging it SHALL remove every row it wrote, and its successor SHALL never become visible, even when the stopped attempt's remaining transactions run after the purge has begun.

**Validates: Requirements 2.8, 2.9**

Property 7: The copied history is split at event boundaries within the batch budget

_For any_ sequence of copied events and their principals, the batches SHALL concatenate to the sequence in order. Each batch SHALL hold at most 512 KiB of encoded events and at most 512 KiB of encoded principals, unless it holds one event. Both stores SHALL cut the same batches.

**Validates: Requirements 2.1, 2.7, 2.11**

Property 8: The timer scanner keeps a materializing run's timers

_For any_ due timer the kernel rejected, the scanner SHALL keep the timer's row when:
- the rejection said its run doesn't exist;
- and, in the transaction that would delete the row, the run is being materialized or has mutable state.

The scanner SHALL delete every other stale due timer's row as today. A kept timer SHALL fire once its run is visible.

**Validates: Requirements 2.10, 3.6**

Property 9: The in-memory store refuses what DSQL refuses

_For any_ transaction of the covered writes, the in-memory store SHALL refuse it exactly when it changes more than 3,000 rows, writes a value over 1,048,576 bytes, or inserts or updates more than 9 MiB of values, and SHALL then change nothing. The row and value limits match DSQL's to the row and the byte. The 9 MiB is the largest size measured to commit, since the store doesn't model the keys and per-row overhead DSQL counts with the values.

**Validates: Requirements 1.5, 2.12**

Property 10: A successor replaces only the run the reset found current

_For any_ reset, and any start, close or deletion of a run of the same workflow id that lands between the reset's commit on its base and its successor's final transaction:
- the final transaction SHALL make the successor current exactly when the current pointer still names the run that was current when the reset was admitted, or is still absent if there was none;
- otherwise the successor SHALL never become visible, its rows SHALL be purged, and the pointer SHALL keep naming the run it names.

**Validates: Requirements 1.6, 2.13, 3.5**

## Fix Implementation

### Budgets (`crates/tokeira-storage/src/write_budget.rs`, new)

- Constants for the budgets of bugfix 2.1: `MAX_ROWS_PER_TRANSACTION` (1,000), `MAX_BYTES_PER_TRANSACTION` (4 MiB) and `MAX_RESET_BATCH_BYTES` (512 KiB).
- One helper cuts an ordered sequence of items, each with a row count and a byte count, into consecutive pages within the transaction budgets, with at least one item in each page. The spill, the purge and the materialization all page through it. A transaction that also changes a few fixed rows, such as a record, takes them off its page's row budget.
- The in-memory store's model of DSQL: `DSQL_MAX_ROWS_PER_TRANSACTION` (3,000) and `DSQL_MAX_VALUE_BYTES` (1,048,576), DSQL's own limits, and `MODEL_MAX_BYTES_PER_TRANSACTION` (9 MiB), the largest size measured to commit.
- A transaction's bytes are the lengths of the values it inserts or updates: payloads, encoded events and principals, encoded state, timers and tombstones. The 5 MiB between the budget and the 9 MiB measured to commit covers keys, fixed-width columns and per-row overhead for up to 1,000 rows. Deleted rows count only toward rows (bugfix introduction).

### The bulk-write record (`crates/tokeira-storage/migrations/`)

The spec adds four migrations, which take the next four free numbers: V077 to V080 at the time of writing. If another change adds a migration first, they move up, and nothing else here changes.

- `V077__run_bulk_write.sql` creates `run_bulk_write (run_key UUID NOT NULL, shard_id UUID NOT NULL, phase SMALLINT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY (run_key))`. The phase is 1 for `materializing` and 2 for `purging`. The application validates it, since DSQL has no `CHECK`.
- `V078__idx_run_bulk_write_shard.sql` adds `CREATE INDEX ASYNC ... ON run_bulk_write (shard_id, run_key)`, so a shard's records page by run key.
- `shard_id` is the run's execution home, the shard whose owner runs its writes and its recovery.
- The invariant is that **a run has a `workflow_hot` row or a record, never both** (Property 3):
  - A deletion's first transaction deletes the hot row and inserts the record.
  - A materialization inserts the record while the successor has no hot row. Its final transaction inserts the hot row and deletes the record.
  - A purge deletes the record with the run's last rows.

  Every reader finds a run through its hot row or current pointer, so a recorded run is absent to all of them, at every step, without a change to any reader.
- Storage gains `RunBulkWrite { run_key, shard_id, phase }`, `BulkWritePhase { Materializing, Purging }` and a listing, `list_run_bulk_writes(shard, after, limit)`, in run-key order.

### Indexes for the purge

- `V079__idx_request_dedupe_run_key.sql` and `V080__idx_dispatch_backlog_run_key.sql` index those tables by `run_key`, `ASYNC`. Today's deletion scans both tables in full, once per deleted run. A paged purge would scan them once per page, so a namespace's reclaim would scan them once per page of every run. The other run-owned tables already have a run-key index or key prefix: V021, V022, V028 and the primary keys of `workflow_dispatch` and `history_batch`.
- The migration bookkeeping follows V074-V076:
  - the schema contracts in `crates/tokeira-storage` and `crates/tokeira-build-info`;
  - the baseline lock and the build information;
  - the migration and schema-bootstrap tests;
  - the migration paragraph of [`crates/tokeira-storage/AGENTS.md`](../../../crates/tokeira-storage/AGENTS.md).
- The change is additive: an older node ignores the new table and indexes. A purge that a newer node records waits for a newer node to own its shard.

### The backlog spill (`persist_to_backlog`, `scan_grace_once`)

- `persist_to_backlog(entries)` keeps one call per pass ([runtime-durable-backlog](../runtime-durable-backlog/requirements.md) criterion 3.5). It writes the entries in their given order, in pages within the budgets:
  - an entry's bytes are its encoded payload;
  - every entry fits a page alone, since a payload DSQL can store is at most 1 MiB.
- Each page is one transaction of `INSERT ... ON CONFLICT (key) DO NOTHING`, as today, so an entry whose identity is stored keeps its row (3.2).
- The call stops at the first page that fails and returns `BacklogPersistError { persisted, source }`, where `persisted` counts the entries of the pages before it. The memory store pages the same way.
- `scan_grace_once` builds its entries as today, workflow tasks first. On failure it re-publishes the tasks from index `persisted` on, and only those. The tasks before it are stored and stay out of the brokers.
- A page whose commit fails ambiguously, possibly committed, is counted as not persisted. Its tasks are re-published, and may also be stored. That is today's outcome for an ambiguous commit: the drain later delivers the stored copy, and the run fences the duplicate at start, as it fences any stale task.
- The runtime's single-entry callers of `persist_to_backlog` treat the error as today.

### Deletion's first transaction (`delete_run_for_bundle`)

The signature and results are unchanged. Inside today's transaction, after today's checks:
- insert the tombstone into `projection_log`;
- delete the current pointer if it names the run;
- delete the run's `workflow_dispatch` row;
- delete its `workflow_hot` row;
- insert its record with phase `purging`.

That is at most five rows. The tombstone is the only value of any size, and it is the deleted run's redacted visibility context. The trait's documentation states the new contract: the run is unreachable and recorded for purging, and `purge_run` removes the rest.

Until the purge reaches them, the run's activity dispatch rows and backlog entries can still be offered to a worker. Finding no run, the runtime discards each as stale, as it discards any task of a run that no longer exists (`start_activity_task` and the backlog drain, `crates/tokeira-runtime/src/runtime/activity.rs`).

The memory store mirrors this under its lock: it removes the run, its pointers and execution index entry, its dispatch row and its History Size, and inserts the record.

### The purge (`purge_run`, new on `RunRepository`)

- `purge_run(run_key)` does nothing and succeeds when the run has no record. A run without a record either has mutable state, so it isn't to be purged, or is already purged.
- If the record says `materializing`, it first switches it to `purging` in a transaction of its own, through `abandon_materialization(run_key)`, which does nothing to a record already `purging` or to none. Through the checks of the materialization's transactions, the switch fences any of them still running. A caller must pass a `materializing` record only when its materialization will never finish (see *Runtime*).
- It then removes the run's rows table by table, one table per transaction, at most 1,000 rows per transaction:
  - `request_dedupe`;
  - `activity_state`, the rows older releases wrote;
  - `timer_bucket`;
  - `activity_dispatch`;
  - `dispatch_backlog`;
  - `history_batch`, last.

  Each transaction selects a page of the table's keys for the run, in key order, and deletes them.
- The transaction that deletes the run's last history rows also deletes the record, within the same 1,000 rows. A run with no history rows left has its record deleted alone.
- Every step is a delete of rows that no one recreates: the run has no mutable state, so no commit writes to it. Repeating a step, or running two purges of the run at once, deletes nothing twice. A transaction refused with a serialization conflict, as one of two concurrent purges can be, is retried, as the materialization's are. The record goes only when every table is empty, so a purge that stops early leaves the record for the next attempt (Property 4).

### Materialization (`materialize_reset_successor`)

The call gains the run the reset expects to replace as current: `materialize_reset_successor(base_run_key, fork_event_id, successor_run_id, expected_current)`, where `expected_current` is an `Option<RunKey>`. `reset_workflow` already resolves the current run before it submits the reset. The Reset command carries it to the lane in `ResetRequest::expected_current_run_key`, a field the kernel ignores, as the command already carries the reapply exclusions and post-reset options the lane reads after the commit.

Today the materialization is one transaction. It becomes:
1. **Read the base.** Read the base's state and its history in one read transaction, which is today's reads moved out of the write transaction, and replay the prefix into the successor's state, as today. The copied events must run from event 1 without a gap. A purge of the base removes its history in pages, first events first, so a read that overlaps one would otherwise copy a prefix with its start missing.
2. **Cut the batches.** Split the copied events, with their principals, into batches in event order. A batch closes when the next event would take its encoded events, or its encoded principals, over 512 KiB, and an event that alone exceeds that forms a batch of its own (Property 7). A history DSQL accepted holds no event over 1 MiB, since each of its batches fit a value. Every batch carries the transition sequence today's single batch carries.
3. **Record.** In one transaction, check that the successor has no hot row and insert its record with phase `materializing` and the successor's execution home.
4. **Copy.** Write the batches and then the successor's timer rows, paged within the budgets. Each transaction first reads the record `FOR UPDATE` and stops unless it says `materializing`. Batches are inserted with `ON CONFLICT (run_key, first_event_id) DO NOTHING`, and timers upserted as today, so a retried transaction writes nothing twice.
5. **Final transaction.** Read the record `FOR UPDATE` and require `materializing`. Read the current pointer `FOR UPDATE` and require that it names `expected_current`, or is absent if that is `None` (2.13). Then:
   - insert the successor's hot row with History Size equal to the sum of the batches' encoded sizes (2.11);
   - maintain its `workflow_dispatch` row at the execution home, as today;
   - point the current pointer at the successor;
   - delete the record.

   That is at most four rows, and the state is the only value of any size. A run that is current but no longer open still names the same run key, so a close of the expected run doesn't fail the check, while a start that moved the pointer does.
6. **Errors.** A transaction refused with a serialization conflict is retried up to 5 times. Any other failure ends the attempt, a changed pointer included, and the error returns to the caller as today. The record stays `materializing`, and the runtime purges the rows the attempt wrote.

The record's check in every transaction is the fence. A purge's switch to `purging` updates the record, and every transaction of the materialization reads it `FOR UPDATE`, so DSQL commits at most one of two that overlap. A straggler that starts after the switch reads `purging` and changes nothing. So once a purge's switch commits, the abandoned materialization writes no further row, and its final transaction can't make the successor visible (Property 6).

The memory store runs the same steps, each under its lock and its limits, so the steps commit separately and a test can stop it between them.

### The timer scanner (`delete_due_timer_if_matches`, `settle_failed_due_timer`)

- The scanner passes the reason the kernel gave: `StaleTimer::RunClosed` or `StaleTimer::RunMissing`.
- For `RunClosed`, the row is deleted as today.
- For `RunMissing`, one transaction reads whether the run has a hot row and whether its record says `materializing`. It deletes the row, if it still matches, only when neither holds.

Plain reads suffice, and the delete never fences a materialization. If the final transaction committed before the read, the read sees the hot row. If it hasn't, the read sees the record. Either way the row stays. A kept timer is submitted again by the next pass and fires once its successor is visible.

A due timer row can exist without its run's hot row only while the run is being materialized or once it is deleted, since every other timer row is written by a commit of its run. A kept row therefore always belongs to a successor that will either become visible or be purged.

### Runtime: the purger and recovery

- **The purger** (`crates/tokeira-runtime/src/purge.rs`, new) is a background task the runtime starts with its other loops.
  - It holds a deduplicated queue of run keys and calls `purge_run` for each.
  - It retries a failure after a backoff that doubles from one second up to one minute, logging each failure, until the purge succeeds or the runtime stops.
  - Its queue is volatile: recovery rebuilds it from the records, as it rebuilds the runtime's other trackers.
- **Deletion.** `delete_workflow` runs the first transaction and then hands the run to the purger, both in a task the caller's cancellation doesn't stop, and returns the deletion once the run is handed over (2.6). A caller whose deadline expires during the first transaction can't then strand a committed deletion without its purge.
  - `DeleteWorkflowRequest` gains `purge_inline`. DeleteWorkflowExecution leaves it unset.
  - A batch delete and a namespace's reclaim set it. `delete_workflow` then purges the run itself before it returns, and hands the run to the purger only if that purge fails. Each moves to its next run only once a run's rows are gone, or queued after a failure, so a large reclaim doesn't fill the purger's queue.
- **Reset.** When `materialize_reset_successor` fails, the lane hands the successor's run key to the purger, and reports the error to the reset RPC as today.
- **Recovery.** `sweep_shard` pages through the shard's records.
  - Before the shard admits commands, the sweep switches each `materializing` record to `purging` with `abandon_materialization`, one small transaction per record (2.9). A materialization still running from an earlier ownership of the shard then fails at its next transaction, so none can complete once the shard is active. Otherwise one could make its successor visible after the sweep's walks had passed it, so no tracker would be installed for the successor until the next acquisition, and its follow-up command would be refused by a node that no longer owns the shard.
  - Every record found here is abandoned: the sweep finishes before the shard admits commands, so no materialization started under this ownership is running. Such records are rare, so the switches don't hold activation up in practice. A switch that fails fails the sweep, and the shard isn't activated until a sweep succeeds.
  - The sweep then hands every record's run to the purger and doesn't wait for the purges. The switch is the only write the sweep makes.

### The in-memory store's model of DSQL (`crates/tokeira-storage/src/memory.rs`)

- Each transaction of the covered writes counts the rows it changes and the bytes it inserts or updates, and checks each value it writes. Past the model's limits — DSQL's rows and value size, and 9 MiB of values — it fails with an error naming the limit, as DSQL's SQLSTATE 54000 does, and changes nothing (Property 9). It counts as `write_budget` defines.
- The store keeps each run's history as DSQL stores it, one batch per commit with events and one per batch a reset writes, so it counts and removes history rows as DSQL does. A reset's batches also appear in `read_transition_audit`, one record per batch, as DSQL's do.
- It keeps the records, and its snapshot carries them, empty when a snapshot is from before this change.
- For tests, it can stop a covered write after a given number of transactions, and fail a given transaction.
- The model applies only to the writes this spec covers. A transition's own commit is out of scope (bugfix Out of Scope).

### Specs this changes

The code PR aligns these specs with the fix:
- [runtime-durable-backlog](../runtime-durable-backlog/requirements.md): criterion 3.7 re-publishes only the tasks the call didn't persist. The design's batching note says the one call writes in pages.
- [temporal-ui-support](../temporal-ui-support/design.md): the deletion steps become the first transaction and the purge, DeleteWorkflowExecution returns after the first, and its tests stop requiring one transaction.
- [continue-as-new-advice](../continue-as-new-advice/requirements.md): Requirement 1.6 and Property 1 measure a copied prefix as the sum of its batches.
- [run-growth-limits](../run-growth-limits/bugfix.md): criterion 3.4 and its Out of Scope point here for a reset's copied history.

### Out of scope

- The items in the bugfix's Out of Scope.

## Testing Strategy

### Exploratory Bug Condition Checking

- First the in-memory store gets its model of DSQL (Property 9), with unit tests at each limit's boundary.
- One contract test per defect then runs on both stores: on the in-memory store in the default suite, and on DSQL in the live suite.
  - a pass of 3,001 small expired tasks, and one of eleven tasks whose payloads are about 1 MB each;
  - the deletion of a closed run owning more than 3,000 rows;
  - a reset whose copied history encodes to more than 1 MiB, and one to more than 10 MiB;
  - a reset whose successor holds 3,500 timers;
  - a start of the same workflow id between a reset's commit on its base and its successor's materialization.
- On the code before the fix, each must fail on both stores:
  - the spill persists nothing and re-publishes every task;
  - the deletion fails and the run stays;
  - the materialization fails and no successor exists;
  - with the start between, the successor becomes current and both runs stay open.

  DSQL may refuse the reset over 10 MiB before it reaches the transaction, since a statement over 10 MiB is refused as a message, so the tests assert the outcome rather than the error code. After the fix every one passes on both stores.
- Negative controls, each patched in alone, run, then reversed so the tree is byte-identical:
  - the row budget set above DSQL's limit;
  - a failed spill re-publishing every task;
  - the purge removing history before another table, or the record before the last rows;
  - a copy transaction that skips the record's check;
  - a final transaction that keeps the record;
  - a final transaction that moves the pointer without its check;
  - a sweep that hands a `materializing` record to the purger without switching it first;
  - the scanner deleting a materializing run's timer;
  - batches cut without the principals budget;
  - History Size seeded from a single batch.

  Each must fail the test that covers it.

### Live DSQL

The live suite runs on an ephemeral cluster under the `dsql-live` profile ([docs/testing/dsql-live-suites.md](../../../docs/testing/dsql-live-suites.md)). It covers:
- **The contract tests above.**
- **An interrupted deletion.** The first transaction commits, the purge stops after some of its transactions, and a later purge finishes the run with no row left.
- **An abandoned materialization.** It stops after some of its transactions and is purged, and a straggler commits nothing.
- **DSQL's limits, apart from the budgets**, so that a change in DSQL fails a test rather than a purge:
  - 3,000 inserted rows commit and 3,001 are refused;
  - 3,000 deleted rows commit and 3,001 are refused;
  - a transaction deleting rows that hold more than 10 MiB commits;
  - 3,000 rows read `FOR UPDATE` beside one insert commit;
  - a value of 1,048,576 bytes is stored and one of 1,048,577 is refused.

### Property-Based Tests

- Properties 1 to 7, 9 and 10 run in `tokeira-storage` against the in-memory store, over generated sizes, interruption points and interleavings. Property 1 also asserts the budgets, not only DSQL's limits.
- Property 2 also runs through `scan_grace_once` with a store that fails a chosen page.
- Property 8 runs in `tokeira-runtime`'s scanner tests with the in-memory store.

### Unit Tests

- The first transaction changes the five rows the design lists, and nothing else.
- The purge's table order, and the purge of an abandoned materialization, which has only history and timer rows.
- The listing of a shard's records in run-key order.
- `delete_workflow` returns after the first transaction, and the purger removes the run's rows. With `purge_inline`, the rows are gone when it returns, and an inline purge that fails is handed to the purger.
- `sweep_shard` switches a `materializing` record before the shard is activated, after which the stopped materialization's final transaction changes nothing. It hands the shard's records to the purger without waiting for them.
- A reset whose materialization fails answers the error, and its successor's rows are purged.
- A reset's copied history of one batch keeps today's History Size (2.11).

### Preservation Checking

- The existing storage, runtime and edge tests stay green: deletion, reset, the backlog and the timer scanner.
- The memory store's tests of deletion change only where they assumed one transaction, such as `property_authoritative_workflow_deletion`, which then drives the purge to its end.
- The runtime's deletion tests read a run's history right after `delete_workflow` returns, so they set `purge_inline`.
