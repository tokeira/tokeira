# Activity State Writes — Bugfix Design

## Overview

A run's activities live in its state, which every commit writes to `workflow_hot`. Both repositories also keep a second copy of each activity, the DSQL `activity_state` table and the in-memory `activity_state_table`, which nothing reads. This fix stops writing the copy and keeps everything that does have a reader: the dispatch rows, run deletion's cleanup, and the snapshot format.

## Glossary

- **Activity state copy:** an `activity_state` row on DSQL, or an `activity_state_table` entry in memory, holding a whole encoded `ActivityState`.
- **Dispatch row:** an `activity_dispatch` row on DSQL, or entry in memory, for an activity that can be offered to a worker (`dsql-side-tables` Requirements 15–19).
- **Earlier release:** a release that writes the activity state copy.

## Bug Details

### Bug Condition

Every transition with an activity op (1.1, 1.2), every reset with activities in the successor's state (1.3), and every in-memory commit, reset and deletion that touches activities (1.4).

### Examples

- A workflow task completion that schedules 20 activities writes 20 `activity_state` rows, each holding the activity's input, alongside the 20 dispatch rows that also hold it.
- A heartbeat commit writes the activity's heartbeat details to the run's state and again to its `activity_state` row.
- An activity's completion deletes its `activity_state` row in a separate statement, though no query would ever have read it.

## Expected Behavior

### Preservation Requirements

- Dispatch rows keep their lifecycle: created by `DispatchOp::EnqueueActivityTask`, deleted when the activity starts, pauses or goes, updated in place otherwise, and cleared when the workflow pauses (3.1).
- Run deletion keeps deleting the run's `activity_state` rows, so rows that earlier releases wrote go with their run (3.2).
- The recovery sweep keeps reading activities from run state (3.3).
- The `workflow_hot` encoding and the snapshot document's layout are unchanged (3.4).

## Root Cause

`dsql-side-tables` introduced `activity_state` as "a materialized open-activity table for timeout/sweep reconstruction", read by `list_open_activities_for_shard`, and `storage-memory-fidelity` mirrored it in memory so that sweeps "operate on normalized data". `recovery-index` replaced the per-shard listings, that one included, with a candidate listing that takes every entry from run state. It removed the reader and left the writers.

No release has read the copy successfully on DSQL. Up to 0.5.1, the sweep asks for `usize::MAX` rows, which fails to bind (`crates/tokeira-runtime/src/recovery.rs:181`, `crates/tokeira-storage/src/dsql/run_repository/activity.rs:211 @ v0.5.1`), and `recovery-index` is not yet released. So a cluster that mixes an earlier release with this one misses nothing when this release stops writing rows.

## Correctness Properties

Property 1: No activity state outside run state

_For any_ sequence of committed transitions with activity upserts and deletes, resets and run deletions, the in-memory store's snapshot SHALL carry an empty activity state table, and its dispatch entries SHALL be those the same transitions produce today.

**Validates: Requirements 2.1, 2.2, 2.3, 3.1**

Property 2: Snapshots from earlier releases restore

_For any_ store, restoring a snapshot whose activity state table holds entries SHALL give the same store as restoring the snapshot with that table empty, and the next snapshot SHALL carry the table empty.

**Validates: Requirements 2.3, 3.4**

## Fix Implementation

### DSQL repository (`crates/tokeira-storage/src/dsql`)

- `commit_transition` stops calling `upsert_activity` for `ActivityOp::Upsert` and drops the `DELETE FROM activity_state` for `ActivityOp::Delete`. The `activity_dispatch` delete and update that follow each op stay as they are.
- `materialize_reset_successor` stops inserting an `activity_state` row for each activity of the successor.
- `upsert_activity` and its SQL go. The activity state codec stays: `activity-heartbeat-time` Requirement 3.6 pins its layout, and rows that earlier releases wrote still have it.
- Run deletion keeps `DELETE FROM activity_state WHERE run_key = $1` in `RUN_OWNED_DELETE_STATEMENTS`.
- No migration. The table and its indexes (V007, V014, V015, V021) stay until no node runs an earlier release; dropping them is a later change.

### In-memory store (`crates/tokeira-storage/src/memory.rs`)

- The store's state drops `activity_state_table`, and with it the insert on `ActivityOp::Upsert`, the removal on `ActivityOp::Delete`, the insert per activity on reset, and the retain on run deletion.
- The snapshot document keeps its `activity_state_table` slot. The store writes it empty and discards whatever a restored snapshot holds there. `SNAPSHOT_FORMAT_VERSION` stays 4, because the layout doesn't change.

### Comments and documents

- `crates/tokeira-storage/src/dsql/codec.rs` and `docs/architecture/010-history-as-authority.md` call `activity_state` the timeout sweep's source; both say instead that run state is. Architecture docs 040, 050 and 090 also presented it as live state, and are corrected too.

### Other specs

- `dsql-core-persistence` Requirements 3.3, 3.4 and 11.7, and the design's write sets, point here.
- `storage-memory-fidelity` Requirement 6's activity criteria, and its design, point here.
- `dsql-side-tables`: Requirements 9 and 13.2 are withdrawn with the listing they describe, Requirements 11.2 and 16.2 drop `activity_state`, and the overview says no release reads it.
- `runtime-sweeper-recovery` Requirement 5.1 and `runtime-complete-implementation` Requirement 11.1.3 name the due `activity_dispatch` rows that the sweep pages through, instead of `activity_state`.
- `inmemory-store-snapshots`, `runtime-durable-backlog` and `activity-heartbeat-time` describe the table as it now is.

## Testing Strategy

### Exploratory Bug Condition Checking

- A negative control that writes the snapshot's table from run state, as the store's copy used to fill it, fails Properties 1 and 2.

### Property-Based Tests

- Property 1 on the in-memory store, with generated schedules, starts, task queue moves and deletes of activities on one run. The run's dispatch entries are checked against a model of `dsql-side-tables` Requirement 19. Resets and run deletions can't fill a table the store no longer holds, so the snapshot's slot is what the property observes.
- Property 2 on the in-memory store, restoring generated snapshots with the activity state table filled and empty.

### Unit Tests

- The existing run-deletion statement test (`authoritative_delete_covers_every_run_owned_table_and_history_is_last`) still asserts the `activity_state` delete.

### Preservation Checking

- The existing dispatch tests for `dsql-side-tables` Requirements 16 and 19, and the snapshot round-trip tests, stay green.
