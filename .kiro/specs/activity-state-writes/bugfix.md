# Bugfix Requirements Document

## Introduction

Every commit that changes an activity writes the whole `ActivityState` to an `activity_state` row on DSQL, and the in-memory store keeps the same copy in `activity_state_table`. Nothing reads either. Their last reader, the per-shard activity listing behind the recovery sweep, went with `recovery-index`, which takes a shard's activities from run state (`crates/tokeira-storage/src/recovery_index.rs`). The run's state in `workflow_hot` already holds every activity, so each row is a second copy of it: input, header, last failure and heartbeat details included.

The copy is paid on every activity change: one more statement, the activity's payloads written twice, and three secondary index entries (V014, V015, V021). It also brings DSQL's limits closer. The row's `state_data` is one `BYTEA` column, which DSQL caps at 1 MiB, and its bytes and rows count toward the commit transaction's limits of 10 MiB and 3,000 rows. A workflow task completion that schedules N activities writes N `activity_state` rows, and a reset writes one for each activity of its successor.

## Bug Analysis

### Current Behavior (Defect)

1.1 WHEN `commit_transition` applies an `ActivityOp::Upsert` on the DSQL repository THEN the system upserts an `activity_state` row holding the encoded `ActivityState`, which no query reads

1.2 WHEN `commit_transition` applies an `ActivityOp::Delete` on the DSQL repository THEN the system deletes the activity's `activity_state` row in a statement of its own

1.3 WHEN `materialize_reset_successor` creates a successor run on the DSQL repository THEN the system inserts an `activity_state` row for each activity in the successor's state

1.4 WHEN the in-memory store applies an activity op, a reset or a run deletion THEN the system maintains `activity_state_table`, which only the store's tests and its snapshot read

### Expected Behavior (Correct)

2.1 WHEN `commit_transition` applies an `ActivityOp::Upsert` or an `ActivityOp::Delete` THEN neither repository SHALL record the activity outside the run's state: the DSQL repository SHALL issue no statement against `activity_state`, and the in-memory store SHALL keep no activity state table

2.2 WHEN `materialize_reset_successor` creates a successor run THEN neither repository SHALL record the successor's activities outside its state

2.3 WHEN the in-memory store writes a snapshot THEN the snapshot's activity state table SHALL be empty. WHEN it restores a snapshot whose table holds entries, as one written by an earlier release does, THEN it SHALL discard those entries and restore everything else as before.

### Unchanged Behavior (Regression Prevention)

3.1 WHEN `commit_transition` applies an activity op THEN both repositories SHALL CONTINUE TO maintain the activity's dispatch row or entry, as `dsql-side-tables` Requirements 16 and 19 require

3.2 WHEN a run is deleted THEN the DSQL repository SHALL CONTINUE TO delete the run's `activity_state` rows, which removes those that earlier releases wrote

3.3 WHEN the recovery sweep lists a shard's activities THEN it SHALL CONTINUE TO take them from run state (`recovery-index`)

3.4 The run's `workflow_hot` row SHALL CONTINUE TO hold every activity in its state, with its encoding unchanged, and the in-memory snapshot SHALL CONTINUE TO carry the activity state table's slot, so snapshots decode across releases in both directions

### Out of Scope

- Dropping the `activity_state` table and its indexes. A node running an earlier release still writes the table, so a migration can drop it only once no such node remains.
- Taking activity inputs out of run state and out of `activity_dispatch`, which is a separate change.
