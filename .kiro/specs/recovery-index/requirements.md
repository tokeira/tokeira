# Requirements Document: Recovery Index

## Introduction

When a node takes a shard, the recovery sweep (`sweep_shard`, [runtime-sweeper-recovery](../runtime-sweeper-recovery/requirements.md)) rebuilds the shard's volatile delivery and timeout state from durable state. Today it reads every `workflow_hot` row of the shard five times, once per kind of work, decoding each state each time, and it reads every `activity_state` row of the shard. Its cost and memory therefore grow with every run the shard holds, open or closed, rather than with the work that is pending. Worker-compute sampling reads every `workflow_hot` row of every active shard the same way on each sampling tick to find reconstructible Nexus deliveries.

This feature indexes the runs that hold work the sweep rebuilds. Every `workflow_hot` write records, in the same statement, whether the state it writes holds such work. A secondary index orders those rows by run key within a shard. The sweep and the sampler page through indexed rows only, decode each state once, and derive every entry from that one decode. Delivery stays memory-first: the alternative, durable task rows written in each commit, is out of scope.

This feature changes no public API. Temporal v1.31.0 makes recovery proportional to pending work through durable task queues that resume from their persisted ack level when a shard loads (`newQueueBase`, `service/history/queues/queue_base.go:104-132 @ v1.31.0`). Tokeira keeps its memory-first delivery and indexes the runs instead; what each run's recovery produces is unchanged.

Depends on: runtime-sweeper-recovery (the sweep, its tracking structures and its entry types), dsql-core-persistence (`workflow_hot`), and the forward-only migration rules in `crates/tokeira-storage/AGENTS.md`. A later change that persists each activity's last heartbeat time builds on this one: the sweep reads that time from the run's workflow state, which this feature makes the sweep's only source.

## Glossary

- **Sweep**: `sweep_shard`, the one-time rebuild of a shard's volatile state on takeover.
- **Sampler**: the worker-compute backlog sampler that counts reconstructible Nexus deliveries per worker-compute queue.
- **Recovery_Work**: state of a run that the Sweep or the Sampler rebuilds from durable state:
  - a pending workflow task that is scheduled but not started, in a run whose status is `Running`;
  - a pending workflow task that has started;
  - a workflow execution timeout or run timeout, in an open run;
  - an activity, in an open run;
  - a pending Nexus operation, in an open run;
  - a completion callback in state `Scheduled` or `BackingOff`, in a run of any status.
- **Recovery_Predicate**: the pure function of a `WorkflowState` that is true exactly when the state holds Recovery_Work.
- **Recovery_Flag**: the nullable boolean column `workflow_hot.recovery_needed`.
- **Legacy_Row**: a `workflow_hot` row written before this feature, whose Recovery_Flag is NULL.
- **Candidate**: a row whose Recovery_Flag is true, or a Legacy_Row.
- **Recovery_Entries**: everything the Sweep and the Sampler derive from one run's state: its dispatchable workflow task, workflow-timeout entry, started-workflow-task entry, activity entries, Nexus timeout entries, completion-callback entries and reconstructible Nexus deliveries, with the entry types the per-kind listings return today.

## Target State

- Every write of a `workflow_hot` row sets the Recovery_Flag from the state written, in the same statement. No statement is added to a commit.
- The Sweep and the Sampler read Candidates only, a bounded page at a time in run-key order, and decode each state once.
- Recovery_Entries are derived from the run's state by one shared function. Activity entries come from the run's state, not from the `activity_state` side table, and only for open runs.
- The six per-kind shard listings the Sweep uses, and the reconstructible-delivery listing the Sampler uses, are replaced by the paged Candidate listing.
- Out of scope: volatile state the Sweep does not rebuild today (speculative workflow-task timers, the update registry, buffered queries); timers and activity dispatch rows, which are durable rows with their own paged listings and scanners; persisting the last heartbeat time; a backfill of Legacy_Rows.

## Evidence From Current Code

- **Schema:** `workflow_hot` is keyed by `run_key` (`migrations/V004__workflow_hot.sql`) with a secondary index on `shard_id` alone (`V013__idx_workflow_hot_shard.sql`). V068 added `history_size_bytes` with a nullable `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, and the load path reads NULL as zero (`crates/tokeira-storage/src/dsql/run_repository/load.rs:103-112`). The next migration is V072.
- **Single writer:** `insert_workflow_hot` (`dsql/run_repository/commit.rs:472-505`) is the only statement that writes `workflow_hot`. It is called by the commit path (`commit.rs:324`) and by reset-successor materialization (`load.rs:390`).
- **Full-shard reads:** each per-kind listing runs `SELECT run_key, state_data FROM workflow_hot WHERE shard_id = $1` with `fetch_all` and filters in memory: dispatchable workflow tasks (`dispatch.rs:263`), workflow timeouts, started workflow tasks, Nexus operations, reconstructible Nexus deliveries and completion callbacks (`visibility.rs:33, 68, 100, 134, 171`). Open activities read every `activity_state` row of the shard (`activity.rs:221`).
- **Consumers:** the Sweep is the only production caller of six of these listings (`crates/tokeira-runtime/src/recovery.rs:117-223`). The Sampler calls the reconstructible-delivery listing for every active shard on each tick (`crates/tokeira-runtime/src/worker_compute/sampling.rs:186-215`).
- **Derivation today:** the per-kind rules live in `collect_dispatchable_workflow_tasks` (`dispatch.rs:298`), `collect_workflow_timeout_entries`, `collect_started_workflow_task_entries`, `collect_nexus_sweep_entries`, `collect_reconstructible_nexus_deliveries`, `collect_completion_callback_sweep_entries` (`visibility.rs:209-360`), `collect_activity_sweep_entries` (`activity.rs:295`), `dispatchable_workflow_task` and `reconstructible_nexus_deliveries` (`crates/tokeira-storage/src/api.rs:1259, 1635`), with matching code in `memory.rs`.

## Requirements

### Requirement 1: Recovery Flag on every hot-state write

**User Story:** As a runtime developer, I want each run's hot-state row to record whether the run holds Recovery_Work, so that recovery can find those runs without decoding every run.

#### Acceptance Criteria

1. THE storage schema SHALL add the Recovery_Flag with a forward-only migration: `ALTER TABLE workflow_hot ADD COLUMN IF NOT EXISTS recovery_needed BOOLEAN`.
2. WHEN the DsqlRunRepository writes a `workflow_hot` row, THE same statement SHALL set the Recovery_Flag to the Recovery_Predicate of the state it writes.
3. THE Recovery_Predicate SHALL be true WHEN the state holds Recovery_Work and false otherwise.
4. THE Recovery_Predicate SHALL be one pure function of `WorkflowState` that both stores use.
5. WHEN a commit writes `workflow_hot`, THE commit SHALL issue no statement beyond those it issues today.

### Requirement 2: Index of Candidates per shard

**User Story:** As a runtime developer, I want a shard's Candidates in run-key order, so that recovery can page through them with a cursor.

#### Acceptance Criteria

1. THE storage schema SHALL add, with a forward-only migration, the asynchronous index `idx_workflow_hot_recovery ON workflow_hot (shard_id, recovery_needed, run_key)`.
2. THE RunRepository SHALL list a shard's Candidates in run-key order within each listing phase (criterion 5), a page of at most `limit` rows at a time, each row with its decoded state.
3. WHEN given a cursor that a previous page returned, THE RunRepository SHALL resume strictly after the last row of that page.
4. THE RunRepository SHALL return every Candidate of the shard exactly once across the pages of one listing, provided no row changes during the listing.
5. THE DsqlRunRepository SHALL list Legacy_Rows before rows whose Recovery_Flag is true, each phase in run-key order, behind one opaque cursor. A Legacy_Row rewritten during the listing gains a non-NULL flag and moves into the phase that has not yet run, so it is not skipped.

### Requirement 3: Legacy rows

**User Story:** As an operator, I want recovery to stay complete after upgrading, so that runs written before this feature are still recovered.

#### Acceptance Criteria

1. WHILE a row's Recovery_Flag is NULL, THE RunRepository SHALL return it as a Candidate.
2. WHEN a Legacy_Row is next written, THE write SHALL set its Recovery_Flag (Requirement 1.2).
3. THE feature SHALL NOT require a backfill for correctness. A Legacy_Row of a closed run stays a Candidate until the run is deleted.

### Requirement 4: Paged sweep

**User Story:** As a runtime developer, I want the Sweep to read only Candidates, a page at a time, so that its cost and memory follow the work that is pending.

#### Acceptance Criteria

1. THE Sweep SHALL read its shard's Candidates a page at a time and SHALL hold at most one page of decoded states at once.
2. FOR EACH Candidate THE Sweep SHALL derive all Recovery_Entries from one decode of its state.
3. THE Sweep SHALL derive each kind of entry with the same rules and contents as the per-kind listings it replaces (runtime-sweeper-recovery Requirements 4, 7, 8, 9 and 10, and completion-callback tracking).
4. THE Sweep SHALL derive activity entries from the run's `WorkflowState`, only for runs whose status is open.
5. WHEN a Candidate yields no Recovery_Entries, THE Sweep SHALL skip it.

### Requirement 5: Paged sampling

**User Story:** As a runtime developer, I want worker-compute sampling to read only Candidates, so that a sampling tick no longer reads every run of every active shard.

#### Acceptance Criteria

1. THE Sampler SHALL derive reconstructible Nexus deliveries from each active shard's Candidates, read a page at a time.
2. THE deliveries the Sampler counts SHALL be the deliveries the replaced listing returns for the same states.

### Requirement 6: In-memory store parity

**User Story:** As a runtime developer, I want the in-memory store to list Candidates the same way, so that tests exercise the same paging contract.

#### Acceptance Criteria

1. THE InMemoryStore SHALL list as Candidates the runs of the shard whose state satisfies the Recovery_Predicate, in run-key order, with the cursor contract of Requirement 2.
2. THE InMemoryStore SHALL have no Legacy_Rows.

### Requirement 7: Safety of the index

**User Story:** As a runtime developer, I want the index never to hide a run that holds Recovery_Work, so that recovery stays complete.

#### Acceptance Criteria

1. FOR ALL `WorkflowState` values, IF any Recovery_Entries derive from the state, THEN the Recovery_Predicate of the state SHALL be true.
2. FOR ALL transitions committed through either store, THE stored Recovery_Flag SHALL equal the Recovery_Predicate of the committed state.
3. WHEN the Sweep or the Sampler gains a new kind of rebuilt state, THE Recovery_Predicate SHALL be extended in the same change. Requirement 7.1's property test enforces this.

## Iteration and Feedback Notes

- **Needs live DSQL validation:** that the planner serves both phases of the Candidate listing (`recovery_needed = true` and `recovery_needed IS NULL`, each with `ORDER BY run_key` after a cursor) as ordered ranges of `idx_workflow_hot_recovery`, and the index build time on a large table.
- **Follow-up, not required:** a backfill that sets the Recovery_Flag of Legacy_Rows, if deployments keep closed runs long enough for them to matter.
