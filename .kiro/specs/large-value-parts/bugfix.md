# Bugfix Requirements Document

## Introduction

Aurora DSQL refuses any single value over 1 MiB (1,048,576 bytes), and a row that holds two values of 1 MiB. The
repository's live limits suite pins the value limit (`crates/tokeira-storage/tests/dsql_transaction_limits.rs`). A run
holds larger values than that:
- v1.31.0 accepts a payload of up to 2 MiB, such as a workflow input, a signal input, an activity's result or a
  heartbeat's details, and a memo of up to 2 MiB ([payload-admission-limits](../payload-admission-limits/bugfix.md)).
- A run's state may reach 8 MiB, and one write's history batch 4 MiB
  ([run-growth-limits](../run-growth-limits/bugfix.md)).

The DSQL store writes each value whole, in one column:
- **A run's state:** `workflow_hot.state_data`, which every write of the run rewrites whole
  (`crates/tokeira-storage/src/dsql/run_repository/commit.rs`). It holds the run's memo and its predecessor's result.
  It also holds each pending activity's heartbeat details, each pending Nexus operation's input, and the events
  buffered while a workflow task is started.
- **A write's history:** `history_batch.events_data`, beside the events' principals in `principals_data`.
- **A write's visibility image:** `projection_log.context_data`. Every write inserts one, with the run's whole memo and
  search attributes.
- **A visibility row's memo:** `execution_visibility_current.memo_blob`
  (`crates/tokeira-projection/src/dsql_store.rs`).
- **A CHASM node's data:** `chasm_node.data`, which for a standalone activity holds its input, result and heartbeat
  details (`crates/tokeira-storage/src/dsql/chasm_node.rs`).
- **A worker invocation request:** `worker_compute_action.request_data`, which the provider contract allows up to
  2 MiB (`crates/tokeira-runtime/src/worker_compute/provider_contract.rs`).

So on DSQL, a value over 1 MiB fails its write with SQLSTATE 54000, and a client's call that made the write fails
with `INTERNAL`. A run whose state or memo passes 1 MiB can't be written again, since every write rewrites the state
and inserts a visibility image. The in-memory store applies none of DSQL's limits and accepts every one of these values.

## Bug Analysis

### Current Behavior (Defect)

1.1 WHEN a write's history batch encodes to more than 1 MiB on DSQL THEN DSQL refuses the write, and the call fails
with `INTERNAL`. Such a batch comes from a start, signal or update whose input is over 1 MiB, an activity whose result
or failure is, or a workflow task's completion whose commands carry more than 1 MiB between them.

1.2 WHEN a run's state encodes to more than 1 MiB on DSQL THEN DSQL refuses the write, and every later write of the
run, so the run makes no further progress. A memo, heartbeat details, a predecessor's result, a Nexus operation's
input or buffered events can each take the state there.

1.3 WHEN a run's memo encodes to more than 1 MiB on DSQL THEN DSQL refuses the visibility image of each of the run's
writes, and so each write.

1.4 WHEN a CHASM node's data encodes to more than 1 MiB on DSQL THEN DSQL refuses the node's write: a standalone
activity started with an input over 1 MiB, or one whose result or heartbeat details take its data there.

1.5 WHEN a worker invocation request encodes to more than 1 MiB on DSQL THEN DSQL refuses the action that holds it.

### Expected Behavior (Correct)

2.1 THE DSQL store SHALL store a value that encodes to more than the part size, 960 KiB, in parts of at most the part
size:
- the first part in the value's own column;
- the other parts in rows of a parts table;
- the number of parts in a count column beside the value.

A value of at most the part size SHALL be stored whole, as today, with no added row or statement. This SHALL apply to
every value 1.1-1.5 name: a run's state, a history batch's events and its principals, a visibility image's context, a
visibility row's memo, a CHASM node's data, and a worker invocation request.

2.2 A value's parts SHALL be written in the transaction that writes its row, under the same fence. A write that
replaces a value SHALL replace its parts in that transaction, and SHALL remove any part the new value doesn't use.

2.3 Every reader SHALL return a value whole. It SHALL read a split value's row and its parts in one transaction, so that
it never sees a count without its parts, nor the parts of two versions of a value. A reader SHALL refuse a value whose
parts don't fit together, with an error that names the value: a part missing, extra, out of place or of the wrong
size.

2.4 A row written before this change has no count and holds its value whole, and SHALL be read as today.

2.5 A reset SHALL copy a history whose events or batches exceed the part size. It SHALL write each copied batch over
the part size, and the successor's state when it is over the part size, in parts, within the budgets of the paged
writes ([bounded-bulk-writes](../bounded-bulk-writes/design.md)).

2.6 Parts SHALL be removed with their value:
- a purge SHALL remove a run's parts after its history, so that while a history batch remains, so do its parts;
- deleting a visibility image SHALL delete its parts;
- removing a CHASM node or a worker invocation action SHALL remove its parts.

2.7 THE in-memory store SHALL keep storing each value whole. A value written through either store SHALL read back
byte for byte.

2.8 Each release that carries part of this change SHALL say "Upgrade with every node stopped." once. A node of an
earlier release can't read a value stored in parts. A node of a release that checks its schema at startup refuses the
schema that adds the parts tables ([distributed-schema-startup-check](../distributed-schema-startup-check/bugfix.md)).

### Unchanged Behavior (Regression Prevention)

3.1 WHEN every value of a write is at most the part size THEN the system SHALL CONTINUE TO write the rows it writes
today, with the statements it uses today.

3.2 The growth limits SHALL CONTINUE TO measure each value's encoded size, however it is stored
([run-growth-limits](../run-growth-limits/bugfix.md) criterion 2.9).

3.3 Each transaction of a paged write SHALL CONTINUE TO change at most 1,000 rows and insert or update at most 4 MiB,
counting the parts it writes ([bounded-bulk-writes](../bounded-bulk-writes/design.md) Property 1).

3.4 A history read SHALL CONTINUE TO fetch only the batches it returns
([history-pagination](../history-pagination/bugfix.md) criterion 2.3). A read of one event SHALL CONTINUE TO
look up the one batch that holds it ([activity-inputs-from-history](../activity-inputs-from-history/bugfix.md)
criterion 2.3). Each SHALL also fetch the parts of those batches.

3.5 THE in-memory store SHALL CONTINUE TO apply none of DSQL's limits.

3.6 WHEN a history read meets a run's purge THEN it SHALL CONTINUE TO return the batches the purge hasn't removed, each
whole.

### Out of Scope

- **A write over DSQL's transaction limit.** Parts don't change how much a write stores. A write whose values come to
  more than DSQL commits in one transaction still fails as today. An example is a workflow task's completion with a
  4 MiB history batch, on a run whose state is 6 MiB. Writing only the parts of a state that changed would bound such
  a write; that needs a design of its own.
- **A worker deployment's record.** `worker_deployments.record_data` holds all of a deployment's versions, up to 100,
  with metadata payloads that nothing bounds, so parts can't keep its write within one transaction. v1.31.0 keeps each
  version in a workflow of its own (`service/worker/workerdeployment/version_workflow.go @ v1.31.0`).
