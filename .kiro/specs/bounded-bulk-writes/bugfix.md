# Bugfix Requirements Document

## Introduction

Aurora DSQL refuses a write transaction that changes more than 3,000 rows or writes more than 10 MiB, and it refuses any single value over 1 MiB ([quotas](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/CHAP_quotas.html)). Live probes on 2026-10-08 established how those limits count:
- 3,000 inserted rows commit, and 3,001 are refused. The count is cumulative across statements, and deleted rows count too.
- A transaction writing 9 MiB commits, and one writing 10 MiB is refused.
- A value of 1,048,576 bytes is stored, and one byte more is refused.
- Deleted rows don't count toward the 10 MiB, and rows read `FOR UPDATE` don't count toward the 3,000. A transaction deleting rows that hold 11.4 MiB commits.
- Every refusal is SQLSTATE 54000 and aborts the whole transaction.

Four of Tokeira's writes put a set of unbounded size into one transaction:
- the grace scanner's spill of expired tasks to the backlog;
- a run's deletion;
- a reset's copy of its base run's history;
- the timer rows a reset writes for its successor.

Input Tokeira accepts can make each of them fail, and fail again on every retry, since the retry is the same transaction.

Temporal v1.31.0 splits the same work:
- It deletes a run in four stages, each safe to retry, with history last (`DeleteWorkflowExecution`, `service/history/shard/context_impl.go:941-963 @ v1.31.0`).
- Its SQL store appends a run's history before the transaction that writes the run's mutable state (`common/persistence/sql/execution.go:334-358 @ v1.31.0`).
- It holds a write to persistence to 4 MiB (`DefaultTransactionSizeLimit`, `common/primitives/constants.go:10-11 @ v1.31.0`).

This spec splits each of the four writes into transactions within fixed budgets, each safe to retry, and makes an interrupted write finish or undo itself later. It also makes the in-memory store refuse what DSQL refuses for these writes, so a test on either store shows the defects.

## Bug Analysis

### Current Behavior (Defect)

1.1 WHEN the grace scanner demotes more than 3,000 expired tasks in one pass, or tasks whose stored payloads total more than 10 MiB THEN the system writes them all in one transaction, which DSQL refuses. The scanner re-publishes every task to the brokers, and the pass after the next grace window refuses them again (`scan_grace_once`, `crates/tokeira-runtime/src/backlog.rs`; `do_persist_to_backlog`, `crates/tokeira-storage/src/dsql/run_repository/dispatch.rs`).

1.2 WHEN a closed run owns so many rows that deleting them, with its tombstone and current pointer, changes more than 3,000 rows THEN the system deletes them in one transaction, which DSQL refuses on every attempt. The rows are the run's history batches, request records, timers, activity dispatch rows and backlog entries (`do_delete_run_for_bundle`, `crates/tokeira-storage/src/dsql/run_repository/delete.rs`):
- DeleteWorkflowExecution and a batch delete never delete the run;
- a namespace's reclaim stops at it.

1.3 WHEN a reset copies a history prefix whose encoding is larger than 1 MiB THEN the system writes the prefix as one history batch, whose value DSQL refuses. Above 10 MiB, DSQL refuses the transaction as well (`do_materialize_reset_successor`, `crates/tokeira-storage/src/dsql/run_repository/load.rs`). The reset has already committed on its base run, so:
- an open base is terminated;
- the base's history names a successor run that never exists;
- each later reset of the base to the same point fails the same way.

1.4 WHEN a reset's successor holds more pending timers than fit in 3,000 rows beside its state, workflow dispatch row and current pointer THEN the transaction that creates the successor writes a timer row for each, which DSQL refuses, with the outcome 1.3 describes.

1.5 WHEN any of 1.1 to 1.4 runs on the in-memory store THEN it succeeds, so no test on that store shows these defects.

### Expected Behavior (Correct)

2.1 Every transaction issued by the writes this spec covers SHALL change at most 1,000 rows and write at most 4 MiB:
- The writes covered are the backlog spill, a run's deletion and purge, and a reset's materialization of its successor.
- The rows SHALL count every row the transaction inserts, updates or deletes.
- The bytes SHALL count the values of the rows it inserts or updates. DSQL doesn't count deleted rows toward its 10 MiB, so a deletion is bounded by its rows.

Every history batch a reset writes SHALL hold at most 512 KiB of encoded events and at most 512 KiB of encoded principals, unless it holds one event.

The budgets are fixed: a third of DSQL's row limit, two-fifths of its transaction size, and half its value size. Their margin covers keys, fixed-width columns and per-row overhead. A transaction SHALL always take at least one item, since each item fits the budgets alone.

2.2 WHEN the grace scanner demotes a pass's expired tasks THEN the system SHALL persist them with one call, as today. That call SHALL write them in the scanner's order, in transactions within the budgets of 2.1. IF one of those transactions fails THEN:
- the call SHALL stop and report how many entries the transactions before it persisted;
- those entries SHALL stay persisted;
- the scanner SHALL re-publish only the tasks the call didn't persist.

2.3 WHEN a closed run is deleted THEN the system SHALL, in one transaction under today's checks of the run's sequence, its execution home and that home's epoch:
- append the run's deletion tombstone;
- remove the current pointer if it names the run;
- remove the run's mutable state and its workflow dispatch row;
- record that the run's remaining rows are to be purged.

From that commit on, every lookup of the run SHALL find no run. v1.31.0's run is likewise unreachable once its current pointer and mutable state are gone (`context_impl.go:941-963 @ v1.31.0`).

2.4 WHEN a run is recorded for purging THEN the system SHALL remove its remaining rows in transactions within the budgets: its request records, timers, activity dispatch rows, backlog entries and legacy activity rows, and then its history batches. History is last. The system SHALL remove the record in the transaction that removes the run's last rows. Each purge transaction SHALL be safe to repeat, and to run alongside another purge of the same run.

2.5 IF a purge stops before it finishes THEN the system SHALL finish it:
- the node that started it SHALL retry it until it finishes;
- a node that takes over the run's shard, or that restarts, SHALL find the record and finish it.

v1.31.0 leaves a history branch whose mutable state is gone to a garbage collector instead (`context_impl.go:961-962`; `service/worker/scanner/history/scavenger.go:251-289 @ v1.31.0`).

2.6 WHEN a deletion's first transaction commits THEN DeleteWorkflowExecution, a batch delete and a namespace's reclaim SHALL count the run as deleted, whether or not its purge has finished.

2.7 WHEN a reset materializes its successor THEN the system SHALL write the successor's copied history and its timer rows in transactions within the budgets, before a final transaction that writes the successor's mutable state, its workflow dispatch row and the current pointer:
- The history SHALL be split into batches at event boundaries, within the batch budget of 2.1.
- No lookup SHALL find the successor before the final transaction commits.

v1.31.0 likewise appends a new run's history before the transaction that writes its mutable state (`common/persistence/sql/execution.go:334-358, 446-474 @ v1.31.0`).

2.8 The first transaction of a materialization SHALL record that the successor is being materialized, and its final transaction SHALL remove that record. Every transaction of the materialization SHALL check the record in the same transaction, and SHALL change nothing once the record no longer says the successor is being materialized.

2.9 IF a materialization fails, or stops with its node, before its final transaction THEN the system SHALL purge the rows it wrote, as 2.4 and 2.5 purge a deleted run's, and the successor SHALL never become visible.

2.10 WHEN the timer scanner finds a due timer whose run doesn't exist because that run is still being materialized THEN the scanner SHALL keep the timer's row, so the timer fires once the run is visible.

2.11 A reset successor's History Size SHALL be the sum of the encoded sizes of the batches written for its copied history, which is how [continue-as-new-advice](../continue-as-new-advice/requirements.md) Requirement 1.6 measures a copied prefix. A prefix that fits one batch keeps today's value.

2.12 WHEN any write this spec covers runs on the in-memory store THEN the store SHALL refuse any of its transactions that DSQL would refuse:
- a transaction that changes more than 3,000 rows;
- a transaction that inserts or updates more than 10 MiB;
- a transaction that writes a value over 1,048,576 bytes.

The store SHALL count a run's history as DSQL stores it, one row per batch.

### Unchanged Behavior (Regression Prevention)

3.1 A spill whose entries fit the budgets SHALL CONTINUE TO be written in one transaction.

3.2 WHEN the grace scanner persists an entry whose backlog identity is already stored THEN the system SHALL CONTINUE TO keep the stored entry ([runtime-durable-backlog](../runtime-durable-backlog/requirements.md) criterion 3.8).

3.3 A deletion SHALL CONTINUE TO check the run's sequence, its execution home and that home's epoch, and that the run is closed. It SHALL CONTINUE TO answer `Deleted` with the tombstone, `NotFound` or `Conflict`, and a `Conflict` SHALL CONTINUE TO change nothing.

3.4 For a deleted run, visibility SHALL CONTINUE TO receive the tombstone, and DescribeWorkflowExecution, GetWorkflowExecutionHistory and the List APIs SHALL CONTINUE TO answer as [temporal-ui-support](../temporal-ui-support/requirements.md) Requirement 9 requires.

3.5 A reset's successor SHALL CONTINUE TO have the mutable state, history events, timers, workflow dispatch row and current pointer it has today, and the reset RPC SHALL CONTINUE TO answer as today.

3.6 The timer scanner SHALL CONTINUE TO delete the row of a due timer whose run has closed, or whose run doesn't exist and isn't being materialized.

3.7 The backlog drain SHALL CONTINUE TO take up to its requested number of entries in one transaction. It deletes at most that many rows, and DSQL counts neither its reads nor the size of what it deletes.

### Out of Scope

- A value over DSQL's 1 MiB limit. A backlog payload or an activity dispatch row can carry such a value in an activity input, and a run's state or a history batch can reach it ([run-growth-limits](../run-growth-limits/bugfix.md) Out of Scope). A later change takes activity inputs out of state, dispatch rows and backlog payloads. A value that is still too large waits for a step that splits values.
- A single transition whose own writes exceed DSQL's limits, such as a workflow task completion with thousands of commands.
- A reset's commit on its base and its successor's creation being separate. If the materialization fails, the base stays reset and names a successor that never exists, as today; v1.31.0 commits both in one transaction (`persistToDB`, `service/history/ndc/workflow_resetter.go:340-432 @ v1.31.0`). The successor's first commit, which fails its fork-point workflow task and reapplies the base's later events, is still lost if its node stops before making it.
- Reads. A reset still reads its base's history in one read transaction, and the drain still reads up to its requested number of entries at once.
- Backlog entries the grace scanner writes for a run after the run's purge has removed its backlog entries, from tasks the brokers still held. The drain discards them as it delivers them, as today.
- The cleanup and repair transactions of the workflow dispatch cutover, which follow their own budgets ([workflow-dispatch](../workflow-dispatch/design.md)).
