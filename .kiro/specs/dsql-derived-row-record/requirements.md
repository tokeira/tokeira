# Requirements Document

## Introduction

Every commit that changes a workflow run on Aurora DSQL also keeps three kinds of derived row in step with the run's
state: its workflow dispatch row, its activity dispatch rows and its outbound operation rows. The commit derives the rows
the run's next state wants and makes the stored rows match. To do that it reads the run's operation rows and activity
dispatch rows, and it deletes the run's workflow dispatch row whenever it wants none, whether or not one is there.

Most commits find nothing. An activity heartbeat for a run with no outbound operation, no workflow task waiting and no
activity waiting to start reads two empty sets and deletes a row that isn't there: three of its nine round trips to DSQL
do nothing.

This feature records, on the run's `workflow_hot` row, which kinds of derived row the run may hold, stamped with the
transition sequence the record describes. Every commit already locks that row, in the first statement that reads run
state, so the record comes back with it, at no extra cost. A commit skips the read or delete for each kind the record
rules out. It writes the record it leaves behind in the statement that already writes the row.

The scope is the DSQL store: its four commit paths (ordinary, paired, successor and staged publication) and every writer
of the three derived tables. Out of scope:

- the in-memory store, which makes no round trips;
- the parent-reference read, which a commit already skips for a run without a parent;
- the lease-fence read, the hot-row lock itself, and every read outside a commit.

Neither the kernel nor the storage trait changes: the record belongs to the DSQL store alone.

No public API behaviour changes, so the targeted Temporal release (v1.31.0) has nothing to say about it. The derived rows
stay what they are today: effects derived from the committed state, never authority (AGENTS.md §3). The record says only
which rows may exist; it never decides what a run wants or what may be dispatched.

## Glossary

- **Derived row**: a row a commit writes from the run's committed state alone. Three kinds:
  - the **workflow dispatch row** (`workflow_dispatch`, at most one per run);
  - **activity dispatch rows** (`activity_dispatch`);
  - **operation rows** (`run_operation`).
- **Wanted rows**: the derived rows a state derives (`derive_workflow_dispatch`, `derive_activity_dispatch`,
  `derive_operations`).
- **Record**: two nullable columns on `workflow_hot`:
  - `derived_kinds`: the kinds of derived row the run may hold;
  - `derived_kinds_seq`: the transition sequence the record describes.
- **Valid record**: a record whose two columns are both set and whose `derived_kinds_seq` equals the `transition_seq`
  read in the same statement.
- **Unknown record**: any record that isn't valid. That includes a run with no `workflow_hot` row.
- **Rules out**: a valid record rules out a kind when `derived_kinds` leaves it out.
- **Hot-row lock**: the `SELECT … FROM workflow_hot WHERE run_key = $1 FOR UPDATE` each commit makes before it writes.
- **Writer outside a commit**: any transaction other than a commit that inserts, replaces or deletes derived rows. These
  are:
  - activity dispatch repair;
  - workflow dispatch repair;
  - outbound reconciliation;
  - reset materialization;
  - staged preparation;
  - the stopped-cluster outbound upgrade;
  - the dispatcher's stale activity row delete;
  - run deletion.

## Target State

- A commit skips the statements for each kind its valid record rules out:
  - for operation rows and activity dispatch rows, it skips the read and inserts each wanted row;
  - for the workflow dispatch row, it skips the delete when it wants no row.
- A commit whose record is unknown, or includes the kind, reads and maintains that kind as it does today.
- Every commit writes a valid record: the kinds the run holds once the commit lands.
- Every writer outside a commit leaves the record sound: a valid record that rules out a kind means the run holds no row
  of that kind.
- The derived rows each commit leaves are the rows it leaves today.

Expected round trips for the lifecycle the live statement budget drives:

| Step | Today | With the record | Skipped |
|---|---|---|---|
| Start | 17 | 17 | none: the run has no `workflow_hot` row yet |
| Workflow task start | 10 | 8 | operation read, activity read |
| Workflow task complete, scheduling two activities | 12 | 9 | operation read, workflow dispatch delete, activity read |
| Activity start | 11 | 9 | operation read, workflow dispatch delete |
| Second activity start | 10 | 8 | operation read, workflow dispatch delete |
| Activity heartbeat | 9 | 6 | operation read, workflow dispatch delete, activity read |
| Activity complete | 10 | 8 | operation read, activity read |
| Activity fail, retry scheduled | 10 | 8 | operation read, activity read |
| Retry start | 10 | 9 | operation read |
| Second activity complete | 10 | 8 | operation read, activity read |
| Signal | 12 | 10 | operation read, activity read |
| Second workflow task start | 10 | 8 | operation read, activity read |
| Workflow task complete, starting a timer | 11 | 8 | operation read, workflow dispatch delete, activity read |
| Timer fire | 11 | 9 | operation read, activity read |
| Workflow task start after the timer | 10 | 8 | operation read, activity read |
| Workflow task complete, closing the run | 12 | 9 | operation read, workflow dispatch delete, activity read |
| **Lifecycle** | **175** | **142** | **33 (19%)** |

The statement budget test pins the counts the implementation measures.

## Evidence From Current Code

- **Commit order.** `write_transition_mode` (`crates/tokeira-storage/src/dsql/run_repository/commit.rs`) does these in
  order:
  1. maintains operation rows (`outbound::maintain` → `maintain_operations`);
  2. upserts the hot row (`insert_workflow_hot`, `WORKFLOW_HOT_UPSERT_SQL`);
  3. maintains the workflow dispatch row (`workflow_dispatch::maintain`);
  4. maintains activity dispatch rows (`activity_dispatch::maintain`).
- **The three statements.**
  - `maintain_operations` (`outbound.rs`) reads every operation row of the run before it compares.
  - `workflow_dispatch::maintain` deletes by run key when the state wants no row.
  - `activity_dispatch::maintain` reads the run's rows in pages through `existing_page`.
- **The hot-row locks.** The four commit paths each select `transition_seq, history_size_bytes, state_parts … FOR
  UPDATE` before writing:
  - `do_commit_guarded` (`commit.rs`);
  - `check_closing` (`pair.rs`);
  - the successor commit (`successor.rs`);
  - each participant of a staged publication (`preparation/publish.rs`).

  The lock already brings back the history size and state part count so the write needs no other statement (precedents:
  `run-growth-limits`, `large-value-parts`).
- **Obsolete operation rows can outlive a commit.** `close_batch::operation_changes` makes every wanted upsert but
  defers obsolete-row deletes past the transaction's row budget. Reconciliation removes them later
  ([outbound-dispatch-recovery](../outbound-dispatch-recovery/design.md), Property 21). So the rows a run holds aren't
  always the rows its stored state wants. A record of the rows themselves, not a recomputation from state, is what makes
  a skip sound.
- **Writers outside a commit that insert or replace rows:**
  - activity dispatch repair (`activity_repair.rs`, under the hot-row lock);
  - workflow dispatch repair (`do_reconcile_workflow_dispatch_run`, a plain hot-row read; it upserts every wanted row);
  - outbound reconciliation (`do_reconcile_outbound_run`, under `read_source`'s lock);
  - reset materialization (`bulk_write.rs`: activity rows staged before the hot row exists, the hot row created by
    `insert_workflow_hot`);
  - staged preparation (`preparation/stage.rs`: activity rows staged before publication creates the hot row);
  - the stopped-cluster outbound upgrade (`outbound_upgrade.rs`: operation rows rebuilt in pages that don't touch the
    hot row).

  Acquisition runs workflow dispatch repair for every run with a workflow dispatch row on the home
  (`crates/tokeira-runtime/src/recovery.rs`).
- **Writers that only delete:**
  - the dispatcher's conditional stale delete (`do_delete_activity_dispatch_if_matches`);
  - the repairs' delete branches;
  - reset cleanup (`clear_materialization_activity_page`);
  - the upgrade's delete pages;
  - run deletion (`delete.rs`, `bulk_write.rs`).
- **Writers of `workflow_hot` outside a commit:**
  - reset materialization (`insert_workflow_hot`);
  - the placement upgrade (`UPDATE workflow_hot SET shard_id`);
  - the outbound upgrade's publication (`UPDATE workflow_hot SET transition_seq, state_data, …`).
- **Older releases.** An older release that keeps running after the migration writes the hot row through its own
  `WORKFLOW_HOT_UPSERT_SQL`, which doesn't name the new columns, so it leaves them as they were.
- **The deferred optimisation.**
  [workflow-dispatch design](../workflow-dispatch/design.md) ("Avoiding unchanged writes is a later measured
  optimization…") names this cut, on the condition that it keeps the same invariant without making old dispatch state
  authoritative.
- **Measurement.** The live statement budget (`crates/tokeira-runtime/tests/dsql_statement_budget.rs`) pins each
  committed step's round trips. The "Today" column above is its current pin.

## Requirements

### Requirement 1: A commit skips what its record rules out

**User Story:** As an operator running Tokeira on Aurora DSQL, I want a commit to skip statements that can only find
nothing, so that each workflow step makes fewer round trips and holds its transaction for less time.

#### Acceptance Criteria

1. WHEN a commit's valid record rules out operation rows, THE commit SHALL NOT read the run's operation rows, and SHALL
   treat the run as holding none.
2. WHEN a commit's valid record rules out activity dispatch rows, THE commit SHALL NOT read the run's activity dispatch
   rows, and SHALL insert each wanted activity dispatch row.
3. WHEN a commit's valid record rules out the workflow dispatch row AND the next state wants none, THE commit SHALL make
   no `workflow_dispatch` statement.
4. WHEN a commit's record is unknown, THE commit SHALL read and maintain all three kinds as it does without a record.
5. WHEN a commit's valid record includes a kind, THE commit SHALL read and maintain that kind as it does without a
   record.
6. THE commit SHALL read the record in its hot-row lock statement, adding no statement to the commit.
7. THE ordinary, paired, successor and staged-publication commit paths SHALL each apply criteria 1–6. A run that has no
   `workflow_hot` row when the commit locks it, such as a new or successor run, has an unknown record.

### Requirement 2: Every commit writes the record it leaves

**User Story:** As a Tokeira developer, I want each commit to leave an exact record, so that the next commit can trust it
without reading the rows.

#### Acceptance Criteria

1. THE commit SHALL write the record in the same statement that writes the run's `workflow_hot` row, adding no
   statement.
2. THE commit SHALL stamp the record with the transition sequence it writes to the row.
3. THE record SHALL include the workflow dispatch row exactly when the next state wants one.
4. THE record SHALL include activity dispatch rows exactly when the next state wants at least one.
5. THE record SHALL include operation rows exactly when the next state wants at least one, OR the commit deferred the
   delete of an obsolete operation row.
6. IF the commit refuses, conflicts or fails, THEN THE record SHALL stay as it was, with the rest of the commit's
   writes.

### Requirement 3: Writers outside a commit keep the record sound

**User Story:** As a Tokeira developer, I want every writer of derived rows to keep the record honest, so that a skipped
read can never miss a row.

#### Acceptance Criteria

1. WHEN a transaction outside a commit inserts or replaces a derived row for a run whose `workflow_hot` row exists, THEN
   that transaction SHALL leave the run's record unknown or including that row's kind.
2. WHEN a writer outside a commit creates a run's `workflow_hot` row, THE writer SHALL create it with an unknown record.
3. WHEN a writer outside a commit only deletes derived rows, THE writer SHALL leave the record as it is.
4. WHEN workflow dispatch repair or activity dispatch repair inserts or replaces a row of a kind the run's record
   doesn't rule out, THE repair SHALL NOT write the record. Acquisition repairs every run with a workflow dispatch row,
   and a record write per run would add a statement to each.
5. THE outbound reconciliation and the stopped-cluster outbound upgrade SHALL leave the record unknown in each
   transaction that writes an operation row.

### Requirement 4: The record is sound

**User Story:** As an operator, I want the skipped statements never to change what a run holds, so that dispatch and
outbound delivery behave exactly as they do today.

#### Acceptance Criteria

1. THE store SHALL keep this invariant: WHEN a run's record is valid and rules out a kind, THE run SHALL hold no row of
   that kind.
2. WHEN a commit lands, THE run SHALL hold exactly the derived rows it would hold after the same commit without a
   record. That is: its wanted rows, plus any obsolete operation rows whose delete the commit deferred.
3. THE record SHALL NOT decide which rows a run wants, whether a row may be dispatched, or whether an operation may be
   delivered.

### Requirement 5: Mixed releases and the schema change

**User Story:** As an operator upgrading a fleet, I want the record to stay correct while older nodes still run, so that
the upgrade needs no stop.

#### Acceptance Criteria

1. THE schema change SHALL add `workflow_hot.derived_kinds SMALLINT` and `workflow_hot.derived_kinds_seq BIGINT`, both
   nullable, as two forward-only migrations in DSQL's DDL subset (`ALTER TABLE … ADD COLUMN IF NOT EXISTS`).
2. WHEN a `workflow_hot` row was written before the migrations, THE row's record SHALL read as unknown.
3. WHEN a commit by a release that doesn't write the record advances a run's transition sequence, THE run's record SHALL
   read as unknown from then on, until a commit that writes the record.
4. WHEN a writer outside a commit rewrites a run's transition sequence without writing the record, THE record SHALL read
   as unknown.
5. THE release that carries the schema change SHALL NOT require every node to be stopped for the upgrade.

### Requirement 6: Unchanged behaviour

**User Story:** As a Tokeira developer, I want the change confined to skipped statements, so that nothing else moves.

#### Acceptance Criteria

1. THE in-memory store SHALL CONTINUE TO maintain derived rows as it does today.
2. THE kernel, the `RunRepository` trait and the `Transition` type SHALL NOT change.
3. WHEN a commit's record is unknown, THE commit SHALL CONTINUE TO make the statements it makes today.
4. A writer outside a commit SHALL CONTINUE TO make the statements it makes today. The only exceptions are a record
   column added to a hot-row read the writer already makes, and the record write that Requirement 3 needs.
5. THE dispatch discovery, the repair walks, the outbound delivery and the dispatcher's stale delete SHALL CONTINUE TO
   read and act on derived rows as they do today.

### Requirement 7: Measured cut

**User Story:** As an operator, I want the saving measured on a real cluster, so that the claim rests on evidence.

#### Acceptance Criteria

1. THE live statement budget SHALL pin each committed step's round trips with the record in place.
2. THE implementation SHALL report the before and after counts from the same live test on a throwaway cluster, deleted
   afterwards.
3. WHEN a step skips a statement, THE statement budget SHALL show that statement absent from the step's commit.
