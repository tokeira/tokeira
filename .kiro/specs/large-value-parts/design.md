# Large Value Parts — Bugfix Design

## Overview

A value too large for one DSQL column is stored in parts, in DSQL itself:
- **The format.** A value of at most the part size, 960 KiB, is stored whole, as today. A larger value is cut into
  parts of the part size, the last one shorter. The first part stays in the value's own column, the others go to a
  parts table, and a count column beside the value says how many there are.
- **The writes.** A value's parts are written in the transaction that writes its row, under the same fence. Replacing
  a value replaces its parts, and removing a value removes them.
- **The reads.** A reader joins a split value's parts in the transaction that reads its row, and refuses parts that
  don't fit together.
- **Old rows.** A row written before this change has no count, and its column holds the whole value.
- **The in-memory store** keeps storing values whole.

The values fall into four groups, each with its own parts table. Each group lands as its own change, in this order:
1. **A run's values:** its state and its history batches, written by its commits, copied by a reset and removed by
   its purge.
2. **Visibility:** each write's visibility image, and the memo of each visibility row.
3. **CHASM nodes:** each node's data.
4. **Worker invocation requests.**

## Glossary

- **Part size:** 983,040 bytes (960 KiB), the most bytes a part may hold.
- **Split value:** a value stored in parts: its first part in its own column, its other parts in its parts table,
  and their number, two or more, in its count column.
- **Whole value:** a value stored in its own column, with no count. Every value of at most the part size is stored
  whole, and so is every value written before this change.
- **Parts table:** the table that holds the parts after the first, keyed by the owning row's key, a value kind where an
  owner has several values, and the part's number, from 1.
- **Join:** rebuilding a split value from its first part, its other parts and its count.

## What DSQL allows

The live suite `dsql_transaction_limits` pins these, and the parts rely on them:
- A value may hold 1,048,576 bytes, and no more.
- A transaction may change 3,000 rows, deleted rows included, and rows it reads `FOR UPDATE` don't count.
- A transaction may write 9 MiB of values, but not 10 MiB. Deleted rows don't count toward it.

These also bear on the parts:
- **A row's limit.** Measured live, a row whose values come to 2,000,000 bytes commits, but one holding two values of
  1 MiB doesn't. Step 1's live tests pin the row the parts need.
- **A statement's limit.** DSQL drops the connection for a message over 10 MiB. The suite's own probes keep each
  statement to 1 MiB of values; that is their practice, not a limit. A statement that writes parts carries at most two,
  about 1.9 MiB, well under 10 MiB.

The part size leaves room for both:
- **Rows.** A row may hold two first parts, as a history batch's events and principals do. Two parts come to 1,966,080
  bytes, which leaves 33,920 bytes of the 2,000,000 that committed for the row's other columns. A history batch row's
  others are its run key, event ids, transition sequence, counts and timestamp, a few dozen bytes. DSQL's own ceiling
  lies between 2,000,000 bytes and two values of 1 MiB, so a row that holds two parts must keep its other columns
  small.
- **Statements.** Each statement writes one row, so it carries at most two parts.
- **Transactions.** Parts count toward DSQL's 3,000 rows and 9 MiB like any other row, and the paged writes count
  them against their budgets. They add rows but not bytes: an 8 MiB state adds eight rows to its commit, and a 4 MiB
  history batch four.

## Bug Details

### Bug Condition

The bug condition holds when a DSQL write puts a value that encodes to more than 1 MiB in one column. That value is
one of these:
- a run's state;
- a history batch's events or principals;
- a visibility image's context, or a visibility row's memo;
- a CHASM node's data;
- a worker invocation request.

### Examples

- **A 1.5 MiB signal.** Today the signal fails with `INTERNAL`, and nothing is written. After the fix, its batch is
  stored as two parts, and `GetWorkflowExecutionHistory` returns the signal whole.
- **A 2 MiB memo.** Today the start fails: its state and its visibility image are each over 1 MiB. After the fix, both
  are stored in parts, and so are those of each later write.
- **Large heartbeat details.** An activity heartbeats 1.5 MiB of details. Today that write fails, and so does every
  later write of the run. After the fix, the state is stored in parts, and each later write replaces them. When the
  activity completes and the state shrinks below the part size, the write stores it whole and removes its parts.
- **A reset past a large event.** A run recorded a 2 MiB activity result before the reset point. Today that event
  can't have been stored. After the fix, the reset copies it in a batch of its own, in parts.
- **A standalone activity with a 1.5 MiB input.** Today its start fails. After the fix, its node's data is stored in
  parts.

## Expected Behavior

### Preservation Requirements

- A write whose values are all within the part size writes the rows and uses the statements it does today (3.1).
- The growth limits measure encoded sizes, not parts (3.2).
- The paged writes keep within their budgets, counting parts (3.3).
- History reads fetch only the batches they return, plus those batches' parts (3.4).
- The in-memory store applies none of DSQL's limits (3.5).
- A history read that meets a purge returns the batches the purge hasn't removed, each whole (3.6).

## Root Cause

- **One column per value.** Each writer binds a value to one column and never compares its size with DSQL's value
  limit. `DSQL_MAX_VALUE_BYTES` is read only by tests.
- **Nothing to fail on in memory.** The in-memory store applies none of DSQL's limits, by design
  ([bounded-bulk-writes](../bounded-bulk-writes/bugfix.md) criterion 2.12), so only a live DSQL run shows the bug.

## Correctness Properties

Property 1: Splitting and joining a value gives the value back

_For any_ byte string, splitting SHALL give no count and the whole string when it is at most the part size. Otherwise
it SHALL give a count of two or more, and parts that each hold the part size except the last, which holds at least one
byte. Joining the parts SHALL give the string back byte for byte.

**Validates: Requirements 2.1, 2.3**

Property 2: A join refuses parts that don't fit together

_For any_ split value, and any change to its parts — one removed, one added, one numbered out of place, one shortened
or lengthened, or a count that disagrees — the join SHALL refuse the parts with an error that names the value.

**Validates: Requirements 2.3**

Property 3: A stored value reads back whole

_For any_ value of any group in the overview, of any size its writers allow, every reader SHALL return it byte for
byte, on both stores. On DSQL, a value within the part size SHALL be stored whole, with no count and no parts.

**Validates: Requirements 2.1, 2.3, 2.4, 2.7, 3.1**

Property 4: A replaced value leaves only its own parts

_For any_ sequence of writes of a run's state, a visibility row's memo or a CHASM node's data, with sizes above and
below the part size, after each write the parts table SHALL hold exactly the parts of the value the row now holds.

**Validates: Requirements 2.2**

Property 5: A reset copies values over the part size

_For any_ base run whose history holds events or batches over the part size, the successor's copied history SHALL read
back equal to the base's prefix, on both stores. Each transaction of the copy SHALL stay within the paged writes'
budgets, counting parts.

**Validates: Requirements 2.5, 3.3**

Property 6: Removing a value removes its parts

_For any_ run with split values, once its purge succeeds, no part of the run SHALL remain. A history read at any point
of the purge SHALL return each batch it returns whole. Deleting a visibility image, or a CHASM execution, SHALL leave
none of its parts.

**Validates: Requirements 2.6, 3.6**

## Fix Implementation

### The format (`crates/tokeira-storage/src/dsql/value_parts.rs`, new)

- `PART_BYTES` (983,040).
- **Splitting** gives the first part, the other parts and the count. A value of at most `PART_BYTES` gives itself and
  no count.
- **Joining** takes the first part, the count and the other parts in part-number order. It refuses them unless:
  - the count is absent and there are no other parts; or
  - the count is two or more, the parts are numbered from 1 to one less than the count, and every part but the last
    holds exactly `PART_BYTES`, while the last holds between 1 and `PART_BYTES`.
- A count of zero, one or less than zero is refused as corrupt.
- Joining refuses with an error naming the value's table, key and kind, as a decode failure does today.
- Each writer writes a value's row and all its parts in one transaction. A reader joins a split value's parts in the
  transaction that reads its row. A reader whose fast read, outside a transaction, finds a count reads the row and its
  parts again in one read-only transaction. So a reader never sees a count without its parts, nor mixes the parts of
  two versions of a value.

### Step 1: a run's values

**Migrations**, at the next free numbers (V084–V087 as of this writing):
- `run_value_part`: `run_key UUID`, `value_kind SMALLINT`, `value_key BIGINT`, `part_no INTEGER`, `data BYTEA`,
  primary key `(run_key, value_kind, value_key, part_no)`. The kinds:
  - 1, the run's state, with value key 0;
  - 2, a history batch's events, and 3, its principals, each with the batch's first event id as value key.
- `ALTER TABLE workflow_hot ADD COLUMN IF NOT EXISTS state_parts INTEGER`.
- `ALTER TABLE history_batch ADD COLUMN IF NOT EXISTS events_parts INTEGER`.
- `ALTER TABLE history_batch ADD COLUMN IF NOT EXISTS principals_parts INTEGER`.

Each statement is one the runner proves idempotent, so the embedded engine's automatic path applies them.

**Bookkeeping**, as for every migration:
- the schema contract, in storage and in build-info's copy;
- `schema-baseline.lock`;
- the runner's migration-count test;
- the migration paragraph in storage's `AGENTS.md`.

**The commit** (`write_transition`, `crates/tokeira-storage/src/dsql/run_repository/commit.rs`):
- The commit's existing `SELECT transition_seq, history_size_bytes FROM workflow_hot … FOR UPDATE` also reads
  `state_parts`, so the commit knows the stored count without another statement.
- **The state.** `WORKFLOW_HOT_UPSERT_SQL` binds the first part and the count. The commit then upserts each other part,
  one statement each. When the stored count is greater than the new one, it deletes the parts from the new count on,
  in one statement. A whole value counts as one part.
- **Every part, every write.** A split state rewrites all its parts on each commit, as the whole state is rewritten
  today: a run whose state is 8 MiB issues eight part upserts per write. That writes no more bytes than the whole
  value. Writing only the parts that changed would cut it, and is the design this spec leaves out of scope.
- **A history batch.** Its events and its principals are split apart. The `history_batch` insert binds each one's
  first part and count, and the commit then inserts each other part, one statement each.
- Nothing else changes. A commit whose values are within the part size issues exactly today's statements.

**The reads:**
- **`load_run`.** Its fast read also reads `state_parts`. A whole state, with no count, loads with that one statement,
  as today. Only for a count of two or more does it read the row and the parts again, in one `REPEATABLE READ READ
  ONLY` transaction, as its accumulator seed already does, and join them there. So a split state costs one more
  transaction on each load.
- **The other readers of a run's state** call one helper.
  - Inside a transaction, it reads the parts there. These run in one: the deletion, a reset's read of its base,
    workflow dispatch repair, the legacy backlog disposal and placement.
  - Outside one, it reads each split state's row and parts again in a read-only transaction of its own. These run
    outside one: `list_dispatchable_workflow_tasks`, `has_open_pinned_workflows` and the recovery sweep's candidate
    pages.
- **History.** `READ_HISTORY_BATCHES_SQL`, `READ_HISTORY_EVENT_BATCH_SQL` and the transition audit's read also read the
  two counts. A page with no split batch is read as today. When a page holds split batches, the read reads the page
  again in one read-only transaction, with one more statement fetching their parts by value key. A purge removes a
  batch's parts only after every batch row of the run, so a page read in one snapshot holds each of its batches whole.

**A reset** (`do_materialize_reset_successor`, `crates/tokeira-storage/src/dsql/run_repository/bulk_write.rs`):
- `read_reset_base` reads the base's state and batches with their parts, in its one read transaction, and joins them.
- The batches are cut as today, at most 512 KiB unless a batch holds one event, so a batch can now be over the part
  size.
- A copied batch's item carries its parts. Its cost counts a row for each part and the batch's encoded size in bytes.
  The copy page inserts the parts with the batch, each `ON CONFLICT DO NOTHING`, so a repeated page still changes
  nothing.
- **The successor's state.** Its parts after the first are written as items of the copy pages. The final transaction
  writes the row with the first part and the count, so it stays as small as today. A reader can't see those parts
  before the row exists. An abandoned materialization's parts are purged with its other rows.

**The purge:**
- `run_value_part` joins `PURGE_TABLES` after `history_batch`, as the last table. The purge removes the record with
  its last page, instead of with history's. Its pages select `(value_kind, value_key, part_no)` in key order and
  delete what they selected, so each part counts as one of a page's 1,000 rows.
- So a batch's parts outlast every batch row of the run, and a reader that sees a batch row in its snapshot also sees
  the batch's parts. Parts left without their row, the state's once its row is gone or those of batches already
  purged, are invisible to every reader, and a resumed purge removes them.
- The deletion's first transaction leaves the state's parts to the purge, so it stays at most five rows.
- A later value that a run owns takes a kind of its own in `run_value_part`, and the purge removes it with the run.

**The in-memory store** is unchanged.

### Step 2: visibility

- **Migrations:**
  - `projection_image_part`, keyed by the image's key and the part number;
  - `ALTER TABLE projection_log ADD COLUMN IF NOT EXISTS context_parts INTEGER`;
  - `visibility_memo_part`, keyed by the visibility row's key and the part number;
  - `ALTER TABLE execution_visibility_current ADD COLUMN IF NOT EXISTS memo_parts INTEGER`.
- **An image** is written once and never changes. The commit writes its parts with it. The projector joins them in
  the transaction that reads the image, and `load_run`'s accumulator seed in its snapshot. Deleting an image deletes
  its parts in the same transaction.
- **A visibility row's memo** is replaced when an apply writes a memo: the apply replaces its parts and deletes any the
  new memo doesn't use, in its transaction. An apply that keeps the stored memo keeps its parts. List reads join the
  memos of a page's split rows in one read-only transaction.
- [projection-passes](../projection-passes/design.md) is replacing how images are read and deleted. This step builds
  on that change.

### Step 3: CHASM nodes

- **Migrations:**
  - `chasm_node_part`, keyed by the node's key and the part number;
  - `ALTER TABLE chasm_node ADD COLUMN IF NOT EXISTS data_parts INTEGER`.
- **Writes.** `persist_dirty` and `persist_new_execution` split a node's data. The fence check that reads each node's
  stored version also reads its count. The write upserts the parts, and deletes those the new data doesn't use.
- **Deletion.** `delete_execution` deletes the execution's parts with its nodes.
- **Reads.** Readers join each node's data in their transaction, or in a read-only one of their own.
- A node's metadata stays whole: it holds the node's version and the short descriptors of its pending tasks.

### Step 4: worker invocation requests

- **Migrations:**
  - `worker_compute_action_part`, keyed by the action's id and the part number;
  - `ALTER TABLE worker_compute_action ADD COLUMN IF NOT EXISTS request_parts INTEGER`.
- A request is written once, with its action, and never changes. Its parts are written in the insert's transaction,
  and joined in the transaction that reads the action, or in a read-only one of their own.

### Release

Each release that carries a step says "Upgrade with every node stopped." once, unless an unreleased fragment already
says it. A node of an earlier release would read a split value's first part as the whole value and fail to decode it.
A node of a release that checks its schema refuses the new schema before it starts.

### Specs and documentation this changes

Each says what holds once values are stored in parts:
- [run-growth-limits](../run-growth-limits/bugfix.md): its Out of Scope entry on DSQL's 1 MiB and 2 MiB limits points
  here. The transaction limit stays out of scope.
- [bounded-bulk-writes](../bounded-bulk-writes/design.md):
  - its Out of Scope entry on a value over 1 MiB points here;
  - the reset section's "A history DSQL accepted holds no event over 1 MiB" becomes: an event over the part size is
    copied in a batch of its own, in parts;
  - a copied batch's cost counts its parts;
  - the purge removes history, then the run's parts, last, and the record with the last parts (Property 4 and the
    purge's description).
- [dsql-core-persistence](../dsql-core-persistence/requirements.md) and
  [dsql-side-tables](../dsql-side-tables/requirements.md): the parts tables and count columns in the schema.
- [projection-passes](../projection-passes/design.md): an image's parts are read and deleted with it (step 2).
- [DSQL storage](../../../docs/architecture/050-dsql-storage.md): its note that encoded rows stay within 1 MiB.
- [DSQL live suites](../../../docs/testing/dsql-live-suites.md): the new suites.
- `write_budget::pages` documentation: an item no longer always fits DSQL's value limit, but its parts do.

## Testing Strategy

### Exploratory Bug Condition Checking

Written and run on DSQL, on the code before each step, each failing there with SQLSTATE 54000:
- **Step 1:**
  - a commit whose history batch encodes to more than 1 MiB;
  - a commit whose state does.
- **Step 2:**
  - a commit whose visibility image does;
  - an apply whose memo does.
- **Step 3:** a CHASM node write whose data does.
- **Step 4:** an action whose request does.

The in-memory store accepts all of these, so no offline test shows the bug.

### Property-Based Tests

- Properties 1 and 2 run offline, with `proptest`, over lengths around every multiple of the part size up to 9 MiB.
- Property 3 runs on both stores for each step's values. It runs live on DSQL, with values on each side of the part
  size and at the largest each writer allows.
- Property 4 runs live, over generated sequences of state sizes, memo sizes and node data sizes.
- Property 5 runs on both stores, live on DSQL, with base histories that hold single events of up to 4 MiB.
- Property 6 runs live: a run's purge, with history reads during it, an image's deletion and a CHASM execution's
  deletion.

### Live Tests

On an ephemeral cluster:
- a 2 MiB payload, written and read back;
- a history batch over 1 MiB;
- a reset of a history over 1 MiB, with an event over the part size;
- a state that grows over the part size and shrinks below it, checking the parts table after each write;
- a purge of a run with split values, paused after each of its transactions for a history read, which must return each
  batch it returns whole;
- a history read paused between its read of a page and its read of the page's parts, while the purge runs to its end.
  The read must still return each batch it returns whole;
- a row holding two parts of the part size, which DSQL must commit;
- a load paused between its read of a split state's row and its read of the parts, while a commit replaces the state.
  The load must still return one version of the state whole.

### Negative Controls

Each control is patched in alone, then run, then reversed so that the tree is byte-identical. Each must fail the test
that covers it:
- the first part written without its other parts, which every read then refuses;
- a shrinking state that keeps its old parts, which Property 4 finds;
- `load_run` joining a split state outside a transaction, which fails the paused load;
- the purge without `run_value_part`, which leaves parts that Property 6 finds;
- the purge removing parts before history, which fails a read at one of the purge's pauses;
- a history page's parts read outside the transaction that reads its batches, which fails the read paused between the
  two;
- the reset's copy without parts, whose successor's history then fails to read;
- a part size of 1 MiB, whose row of two parts DSQL refuses.

### Preservation Checking

The storage, runtime and projection suites stay green, and so do the live suites of the bulk writes, the history
reads, the activity inputs and the transaction limits.
