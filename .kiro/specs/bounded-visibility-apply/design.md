# Bounded Visibility Apply — Bugfix Design

## Overview

The fix returns the DSQL attribute index to the schema decision's generation pattern, with values stored once:
- **Index rows at a version.** A new table, `execution_visibility_attr_cell`, keys each index row by its image's
  version. A run's rows at its visibility row's current version are its attributes, and queries match only those.
- **The flip.** The apply writes an image's rows at its version, then advances the run's row under the version guard.
  That advance is what makes the new rows the run's attributes. Rows below the current version are deleted afterwards.
- **Small images stay in one transaction.** When the rows fit, the writes, the advance and the deletes share one
  transaction, as now. When they don't, each step takes transactions of at most 2,500 rows.
- **Values once.** A second new table, `execution_visibility_attr_value`, holds each attribute's encoded value once
  per image. The component List hydration and the load's used-versions seed read it.
- **The copy.** At its first start, a node of this release copies the old index into the new tables before it serves,
  and records completion.

## Glossary

- **Image**: one `ProjectionRecord`, a run's full visibility state at one transition.
- **Version**: an image's `(authority_epoch, source_transition_seq)`, compared lexicographically. The version guard
  writes an image's row only when its version is strictly newer than the stored row's (`upsert_execution_row`).
- **Current version**: the version of the run's row in `execution_visibility_current`.
- **Cell**: one index row: a single-valued attribute's value, a keyword list's element, or a `Text` value's distinct
  token, as `stored_attr_cells` produces them today.
- **Value**: an attribute's whole encoded `SearchAttrValue` (`codec::encode`).
- **Flip**: the transaction that advances the run's row to an image's version under the guard.
- **Cleanup**: deleting a run's cells and values below its current version.
- **Orphan**: a cell written at a version the run's row never takes. This happens when an apply is interrupted after
  writing some cells, or its image is superseded before its flip.
- **Budget**: 2,500 rows written per transaction, under DSQL's 3,000.
- **The copy**: the one-time move of the old index's rows into the new tables.

## Bug Details

### Bug Condition

```
isBugCondition(previous, image) =
    1 + cells(previous) + cells(image) + deltas(previous, image) > 3,000
  or bytes(the apply's transaction) > 10 MiB
```

Here `cells(x)` is the number of index rows `stored_attr_cells` gives for x's attributes, and `deltas` is the number of
rollup rows `compute_rollup_deltas` gives. Each of today's index rows also carries its attribute's whole value, so the
row count is reached before the byte count for values within the limits. The condition holds for images within the
search-attribute limits:
- a 2 KiB `Text` value holds about 690 distinct tokens;
- 40 KiB holds 19 such values, so about 13,000 cells.

### Examples

1. **A first apply.** Five `Text` attributes, each a 2 KiB value of distinct one- and two-character tokens: 5 × 694 =
   3,470 cells. The first apply writes 3,471 rows or more, and DSQL refuses it on every attempt. The run never appears
   in List.
2. **An update.** Two images of three such attributes with disjoint tokens, 2,082 cells each. The first applies. The
   second writes 2,082 deletes and 2,082 inserts and is refused, though each image alone fits. List keeps answering
   with the first image's tokens.
3. **Repetition.** A 2 KiB `KeywordList` of 400 elements writes its whole value 400 times, about 800 KiB.
4. **The pass repeats it.** The refused image stays in the log. Every pass sends its thousands of statements again,
   one per cell, before DSQL refuses the transaction.

## Expected Behavior

Expected behaviour is criteria 2.1 to 2.7 of [bugfix.md](bugfix.md).

### Preservation Requirements

From bugfix.md 3.1 to 3.6:
- **The guard and its transaction.** The version guard, the primary-key read and the rollup deltas stay as they are,
  in the transaction that writes the run's row.
- **Failed applies.** A failed apply keeps its images.
- **Filters.** Filters match what they match today.
- **Tombstones.** A tombstone removes a run's attributes.
- **Memory and the edge.** The in-memory store and the edge's limits are untouched.

## Root Cause

1. **The replace is inside the apply's transaction.** V052's comment records why: "attribute sets are tiny". The
   schema decision chose the generation pattern instead and kept every transaction small. It named the 3,000-row limit
   as a secondary reason (DECISION-visibility-dsql-schema.md, Decision 3 and H4).
2. **Every index row carries the value.** `upsert_search_attr_index_row` binds the whole encoded value into
   `value_data` on each row, because the hydration and the seed read it from any one row.
3. **One statement per cell.** `upsert_search_attr_index_row` sends an `INSERT` for each cell.

## Correctness Properties

### Property 1: Any image within the limits applies

_For any_ image whose attributes are within the search-attribute limits (100 keys, 2 KiB a value, 40 KiB in all), and
any previous image, the apply commits. Every transaction it commits writes at most 2,500 rows, and together they write
exactly the image's cells and values at its version.

**Validates: Requirements 2.1, 2.3**

### Property 2: A query matches one image

_For any_ interleaving of the steps that apply two or more images of one run (cell writes, flips and cleanups), by
one projector or two, the cells any query matches for the run are exactly the cells of the image at the run's current
version.

**Validates: Requirements 2.2, 2.4**

### Property 3: Cleanup never touches the current image

_For any_ such interleaving, no cleanup deletes a cell or value at or above the run's current version.

**Validates: Requirement 2.4**

### Property 4: Values read as before

_For any_ image, the hydrated attributes and the seeded used-versions list read from the value table equal those read
from today's index.

**Validates: Requirements 2.5, 2.7**

### Property 5: The copy keeps every answer

_For any_ index written the old way, after the copy every filter, Count, hydration and seed answers as it did on the old
index. This holds when the copy restarts part-way, and when two nodes copy at once.

**Validates: Requirements 2.6, 2.7**

## Fix Implementation

### Schema

Three migrations, at the next free numbers after large-value-parts' V084–V087 (V088–V090 as of this writing):

```sql
CREATE TABLE IF NOT EXISTS execution_visibility_attr_cell (
    namespace_id          UUID             NOT NULL,
    run_key               UUID             NOT NULL,
    authority_epoch       BIGINT           NOT NULL,
    source_transition_seq BIGINT           NOT NULL,
    attr_id               BIGINT           NOT NULL,
    attr_type             SMALLINT         NOT NULL,
    value_discriminator   TEXT             NOT NULL,
    keyword_value         TEXT,
    int_value             BIGINT,
    bool_value            BOOLEAN,
    double_value          DOUBLE PRECISION,
    datetime_value        TIMESTAMPTZ,
    text_token            TEXT,
    PRIMARY KEY (namespace_id, run_key, authority_epoch, source_transition_seq,
                 attr_id, attr_type, value_discriminator)
);
```

```sql
CREATE INDEX ASYNC IF NOT EXISTS idx_execution_visibility_attr_cell_lookup
    ON execution_visibility_attr_cell
    (namespace_id, attr_id, attr_type, keyword_value, run_key, authority_epoch, source_transition_seq);
```

```sql
CREATE TABLE IF NOT EXISTS execution_visibility_attr_value (
    namespace_id          UUID     NOT NULL,
    run_key               UUID     NOT NULL,
    authority_epoch       BIGINT   NOT NULL,
    source_transition_seq BIGINT   NOT NULL,
    attr_id               BIGINT   NOT NULL,
    attr_type             SMALLINT NOT NULL,
    value_data            BYTEA    NOT NULL,
    PRIMARY KEY (namespace_id, run_key, authority_epoch, source_transition_seq, attr_id, attr_type)
);
```

What each piece is for:
- **The cell table** keeps V052's typed columns and discriminator, puts the version into the key, and drops
  `value_data`.
- **The index** is V053's, with the version after the run key. An equality on a keyword then returns the run and the
  version it matches against, from the index alone.
- **The value table** holds one row per attribute per image.
- **The copy's completion** is a row named `visibility_attr_cell_copy` in `chasm_backfill_marker` (V071). That table
  records durable completions by name, and CHASM's retirement rule reads only its own row.
- **The old table.** `execution_visibility_attr_index` and V053's index stay, unread, until a later migration drops
  them.

### The apply

`apply_row` in `crates/tokeira-projection/src/dsql_store.rs`:

1. **Cells and values.** Build the image's cells as `stored_attr_cells` does today, and encode each attribute's value
   once.
2. **The first transaction.** As today, read the run's row by its primary key and check the version guard. Then count
   the run's cells and values below the image's version: two reads over the run's key prefix, with
   `(authority_epoch, source_transition_seq) < ($e, $s)`.
3. **One transaction when it fits.** Do this when `1 + new cells + older cells + new values + older values + deltas`
   is at most 2,500. In the same transaction:
   - write the image's cells at its version;
   - upsert the row under the guard;
   - write the values at the version;
   - delete the cells and values below the version;
   - add the rollup deltas;
   - commit.
   This is today's single transaction, with the old rows deleted by version instead of all at once.
4. **Otherwise, three steps.**
   - **Write the cells.** End the first transaction without writing. Write the image's cells at its version, in
     transactions of at most 2,500 rows, with `ON CONFLICT DO NOTHING`, so a retried or concurrent apply of the same
     image writes the same rows. Cells at a version the run's row hasn't reached match no query.
   - **The flip.** In a transaction of its own: read the row by its primary key, check the guard, upsert the row, write
     the values at the version, delete the values below it, add the deltas, and commit. When the guard fails, the image
     is current (`ApplyOutcome::Current`). If another apply flipped this same image, the cells are the run's
     attributes; otherwise they are orphans.
   - **Cleanup.** Delete the run's cells below the version, in transactions of at most 2,500 rows. Each one selects up
     to 2,500 keys below the version and deletes them by key, until none are left.
   The cell writes and the cleanup run outside the stripe lock, since they touch no counter. The flip holds the lock,
   as the apply does today.
5. **Statements.** Every statement that writes cells writes at most 128 rows, the batch size
   [projection-batched-apply-and-failure-policies](../projection-batched-apply-and-failure-policies/requirements.md)
   gives for multi-row statements. Today it is one statement per cell.

**Outcomes.** An apply reports `Advanced` when the row advanced: the single transaction committed, or the flip did. A
failure while writing cells fails the apply, so the pass keeps the image and the next pass rewrites the same cells. A
failed flip retries on a write conflict, as today. A failed cleanup still reports `Advanced`, since the row advanced and
nothing left behind matches a query. The run's next apply counts and deletes what's left.

**Why the flip exposes a complete image.** Two facts make this hold:
- Cleanup deletes only rows below the run's current version.
- The guard advances the row to a version only while the row is below it.

So while an image's cells are being written, the current version is below the image's, and no cleanup deletes any of
them. If a newer image's flip commits first, this image's flip fails the guard. After a flip, the image's cells fall
below the current version only once a newer image's flip has committed. Two projectors of one partition, as during a
move, keep this, since each step reads and writes in its own transaction.

**Orphans** match nothing:
- one below the current version goes at the run's next cleanup;
- one above it is removed by the cleanup of the first newer image that passes it.
A run's deletion tombstone is newer than all its images, so its cleanup removes everything that remains.

**Tombstones** go through the same apply. A tombstone has no attributes, so its flip writes no cells or values, and its
cleanup removes every older row.

**The trait's single-attribute writes.** On DSQL, `upsert_search_attr_index` and `remove_search_attr_index` replace one
attribute's cells and value at the run's current version, read in the same transaction. Nothing on DSQL calls them
outside tests.

### Queries

Every predicate on cells matches the run's current version:
- **Correlated forms** (`NOT EXISTS` for `IS NULL` and `!=`) add
  `cell.authority_epoch = execution_visibility_current.authority_epoch AND cell.source_transition_seq =
  execution_visibility_current.source_transition_seq`.
- **Uncorrelated forms** become
  `(run_key, authority_epoch, source_transition_seq) IN (SELECT run_key, authority_epoch, source_transition_seq FROM
  execution_visibility_attr_cell WHERE ...)`. That covers equality on a keyword or keyword list, a `Text` token, and
  the typed comparisons.
- **Count grouped by a custom attribute** joins cells at the run's version.

A List or Count reads the row and its cells in one statement, and a component List in one transaction. So the version
and the cells always come from one snapshot. Rows still below the version before cleanup, and orphans, match nothing.
The scan metric labels the new table.

### Reading values

- **Component hydration.** `hydrate_component_attributes` reads values from `execution_visibility_attr_value` for the
  listed runs, keeping the row at each run's listed version, in the list's own transaction. It no longer needs
  `DISTINCT`.
- **The seed.** `READ_APPLIED_PROJECTION_SEED` joins `execution_visibility_attr_value` on the run and the row's version
  and `r.attr_id`, where it joined the index. Projection-passes criterion 2.4 is unchanged: the load still reads the
  applied value in the same snapshot as the state.

### The copy

Add `run_visibility_attr_copy` beside the DSQL visibility store. The engine calls it once in `build_and_serve`'s DSQL
branch, after `check_distributed_schema` and before it builds the projector supervisor
(`crates/tokeira-engine/src/lib.rs`). That is before anything can apply an image: the projectors, the CHASM visibility
adapter, the visibility repair scanner and the deletion path all start after it. It follows the pattern of
`run_current_execution_backfill`, the CHASM pointer copy. Each of its transactions:
1. Reads the completion row, and stops if it's set.
2. Reads the next page of runs from `execution_visibility_current` in primary-key order, after a position kept in
   memory, with each run's version.
3. Copies each run's old rows into cells at the run's version, and one value per `(attr_id, attr_type)`. The old layout
   puts the whole value on every row, so any one row gives it. The old table's key prefix `(namespace_id, run_key)`
   serves each run's read.
4. Stops adding runs before its writes would pass 2,500 rows. One run's old rows number fewer than 3,000, since one
   transaction wrote them, so they split across at most two transactions, keyed on the old primary key.
5. Writes with `ON CONFLICT DO NOTHING`, so a restarted copy, or two nodes copying at once, write the same rows.

When the page reaches the end of the table, the copy writes the completion row.

**Why the version read in the copy's own transaction is right:**
- Every node of this release runs the copy to completion before it starts anything that applies an image, so no image
  is applied before completion is recorded.
- A copy transaction that reads the completion row unset therefore reads each run's row as the earlier release left
  it. That is the version its old rows belong to.
- A copy that starts after the completion commits stops at once.
- A copy transaction that read before the completion, but commits after some node's flip, writes rows below that run's
  new version. They match nothing, and cleanup removes them.

**After this release** nothing writes the old table. It can be dropped by a later migration, after a full release with
the completion recorded and no reads of it, as `crates/tokeira-storage/AGENTS.md` has it for CHASM's legacy pointers.

### Budget

`TRANSACTION_ROW_BUDGET = 2_500` leaves room under DSQL's 3,000 rows for the row and its deltas.

Bytes stay far under 10 MiB. A cell holds its token twice (discriminator and typed column) plus fixed columns. So an
image's cells come to at most about twice its 40 KiB of values plus about 100 bytes a row: under 1.5 MiB for 13,000
cells, split across transactions anyway. Its values come to at most 40 KiB.

### Upgrade

- **The release** already upgrades with every node stopped ("Upgrade with every node stopped.", in the unreleased
  notes). The copy relies on that: a node of an earlier release would write the old layout.
- **The schema contract** gains the three migrations as part of the release's target, since startup needs them.
- **Start-up time.** The copy makes the first start take longer, the more attribute rows the cluster holds. It logs its
  progress.

### Specs this changes

The code PR aligns these:
- **[projection-passes](../projection-passes/design.md)**, "Deleting and the accumulator": the seed's SQL reads the
  value table at the row's version, and the bullet "The value is stored verbatim" names the value table. Its criterion
  2.4 is unchanged.
- **[chasm-foundation](../chasm-foundation/reference/DECISION-visibility-dsql-schema.md)**: a note under Decision 3 and
  H4 that the cell and value tables restore the generation pattern V052 departed from, with values stored once.
- **[chasm-extension-visibility](../chasm-extension-visibility/design.md)**: component hydration reads the value table.
- **[dsql-projection-persistence](../dsql-projection-persistence/requirements.md)**: criterion 7 still calls the
  search-attribute methods stubs. It is aligned to the methods as they now are.

### Documentation

- **`docs/testing/dsql-live-suites.md`**: the new live tests.
- **`docs/diagrams/projection-plane.svg`**: it names the attribute index.
- **The crate docs** in `dsql_store.rs`: the apply, cleanup and copy.

## Testing Strategy

### Exploratory Bug Condition Checking

Live tests on an ephemeral cluster, run on the current code before the fix:
1. **A first apply** (example 1): five `Text` attributes registered in the test namespace, each a 2 KiB value of
   distinct tokens. Predicted: the apply fails with DSQL's limit error on every attempt, and the run has no visibility
   row.
2. **An update** (example 2): the first image applies; the second fails on every attempt; a filter on one of the second
   image's tokens matches nothing; one on the first image's matches the run.

### Property-Based Tests

Tagged `// Feature: bounded-visibility-apply, Property N: <title>`.
- **Property 1**: the apply's planner, a pure function from the image's cells and values and the counts below its
  version to the transactions it will commit, over generated images within the limits. Every transaction is within the
  budget, and the union is the image's cells and values.
- **Properties 2 and 3**: a model of the protocol over sets of cells and values keyed by version and a current version.
  Its steps are the cell batches, guarded flips and cleanups of two or more images, interleaved by generated schedules,
  with a query check after every step.
- **Property 4**: for generated images, the values decoded from the value table equal those decoded from today's
  index rows.
- **Property 5**: the copy over a model of the old index, restarted at generated points and run twice at once.

### Unit Tests

- Every query shape from the compiler names the version match.
- The cleanup's predicate is strictly below the version.
- Cells are written by statements of at most 128 rows.
- The copy stops when the completion row exists.
- The engine runs the copy after the schema check and before it builds the projector supervisor.

### Live Tests

On an ephemeral cluster:
1. **The exploration tests**, which pass after the fix.
2. **Interleaved applies**: two projectors apply interleaved large images of one run, with a test hook between steps.
   Filters checked at every hook match exactly one image's tokens.
3. **Cleanup and orphans**: no cells or values are left below the current version after cleanup. Cells of an
   interrupted apply match nothing, and the run's next newer image removes them.
4. **The copy**: an old index written with V052's statements is copied. Filters, Count, hydration and the seed answer
   as before. A restart part-way copies the rest. A second copy after completion writes nothing.
5. **The seed**: projection-passes' seed tests, through the value table.
6. **EXPLAIN**:
   - equality on a keyword uses the lookup index;
   - the correlated probes and the cleanup's delete use the cell table's primary key;
   - the copy's per-run read uses the old table's key prefix.

### Negative controls

Each is patched in alone, and each must fail its test:
1. Cleanup deletes at or below the current version: live test 1 fails, because the image's tokens are gone.
2. Queries without the version match: live test 3 fails, because an old image's token matches before cleanup.
3. The single-transaction check leaves out the older rows: exploration test 2 fails with DSQL's limit.
4. The flip before the cells are written: live test 2 fails, because a filter matches part of an image.
5. The copy writes cells one version above the row's: live test 4 fails, because old runs match nothing.
6. The seed reads the cell table: live test 5 fails, because the list is missing.

### Preservation Checking

These pass unchanged:
- the projection store's live tests;
- `dsql_projection_persistence`;
- the passes' live tests;
- the projection-accumulator live tests;
- the visibility filter tests on both stores.
