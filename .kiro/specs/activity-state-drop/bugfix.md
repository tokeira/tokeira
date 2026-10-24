# Bugfix Requirements Document

## Introduction

Nothing writes the DSQL `activity_state` table any more, and nothing reads it. The run's state in `workflow_hot` holds every activity ([activity-state-writes](../activity-state-writes/bugfix.md)). The table still has a place in the schema, and a cost:
- **Every schema builds it.** Migrations create the table and three secondary indexes on it, which DSQL builds asynchronously: V007, V014 (`idx_activity_state_shard`), V015 (`idx_activity_state_queue`) and V021 (`idx_activity_state_run_activity`).
- **Every run deletion clears it.** The DSQL purge spends a step deleting the run's `activity_state` rows, which only earlier releases wrote (`crates/tokeira-storage/src/dsql/run_repository/bulk_write.rs`).
- **A codec survives for it.** `encode_activity_state` and `decode_activity_state` (`crates/tokeira-storage/src/dsql/codec.rs`) serialize rows that nothing reads, and [activity-heartbeat-time](../activity-heartbeat-time/requirements.md) criterion 3.6 fixes their layout.

The table stayed because a node of an earlier release still wrote it. A release upgrades with every node stopped, so no such node runs while the table goes:
- A node of a release that checks its schema at startup refuses a schema past what it reads ([distributed-schema-startup-check](../distributed-schema-startup-check/bugfix.md)), so it never starts on the dropped schema.
- A node of an earlier release doesn't check. Only the release note's "Upgrade with every node stopped." keeps it off.

## Bug Analysis

### Current Behavior (Defect)

1.1 WHEN migrations bring a DSQL schema to this release's target THEN the schema holds the `activity_state` table and its three secondary indexes, which no node of this release reads or writes.

1.2 WHEN the DSQL repository deletes a run THEN its purge deletes the run's `activity_state` rows, in a step of their own.

1.3 WHEN the storage crate is built THEN it keeps a codec for `activity_state` rows, whose layout activity-heartbeat-time criterion 3.6 requires to stay fixed.

### Expected Behavior (Correct)

2.1 A migration at the next free number SHALL drop the `activity_state` table with the single statement `DROP TABLE IF EXISTS activity_state`, which on DSQL drops the table's indexes with it.

2.2 THE migration runner SHALL treat `DROP TABLE IF EXISTS` as idempotent, so that the embedded engine's automatic migration applies the drop, and applies it again after a crash between the drop and its ledger row.

2.3 WHEN the DSQL repository deletes a run THEN its purge SHALL NOT touch `activity_state`.

2.4 THE storage crate SHALL keep no codec for `activity_state` rows, and activity-heartbeat-time SHALL no longer fix their layout.

2.5 THE schema contract's target and maximum readable versions SHALL be the new migration's version. Its minimum supported version SHALL stay 1:
- This release migrates any earlier schema forward, so it needs no floor.
- A floor only makes this release refuse an older schema. It does nothing about older nodes, which their own maximum readable version and schema check refuse, or, in a release before that check, the stopped upgrade keeps off.

2.6 THE release that carries the drop SHALL say "Upgrade with every node stopped." once.

### Unchanged Behavior (Regression Prevention)

3.1 A schema that an earlier release migrated, with rows in `activity_state`, SHALL CONTINUE TO migrate forward to this release's target, under both `tkr schema setup` and the embedded engine's automatic policy.

3.2 Migrations V007, V014, V015 and V021 SHALL CONTINUE TO be applied as they are, since migrations are forward-only.

3.3 Run deletion SHALL CONTINUE TO purge every other table a run owns, history after the rows derived from it and the parts of the run's values last ([large-value-parts](../large-value-parts/design.md)).

3.4 The in-memory store SHALL CONTINUE TO keep its activity state table's slot in its snapshots, so snapshots decode across releases in both directions.

### Out of Scope

- **The in-memory store's snapshot slot** (activity-state-writes 2.3 and 3.4). Snapshots carry it so that they decode across releases; retiring it is a change of the snapshot format.
- **Nodes of releases before distributed-schema-startup-check.** They don't check the schema, and "Upgrade with every node stopped." covers them.
