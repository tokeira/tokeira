# Bugfix Requirements Document

## Introduction

The `commit_transition_for_bundle` function in the DSQL run repository has a fencing hole: the epoch check (verifying bundle ownership via `shard_lease`) is performed in a separate transaction that is rolled back before the actual workflow mutation is committed via `commit_transition(..., ShardEpoch::ZERO)`. This creates a TOCTOU race where another runtime can acquire ownership between the check and the write, violating the key invariant that every production transition commit must be fenced by the authoritative DSQL lease epoch in the same transaction as the mutation.

Additionally, multiple runtime mutation paths bypass the fenced commit entirely by calling `commit_transition(..., ShardEpoch::ZERO)` directly, the continue-as-new successor timeout path uses a hard-coded shard count of 1 instead of the actual shard count from ownership state, and the largest source files hold multiple independent correctness domains in single files — making the fencing audit harder and increasing the chance of cross-path divergence.

## Bug Analysis

### Current Behavior (Defect)

1.1 WHEN `commit_transition_for_bundle` is called with a valid epoch THEN the system checks the shard_lease epoch in a transaction that is rolled back, then calls `commit_transition(..., ShardEpoch::ZERO)` in a separate transaction, creating a TOCTOU race window where ownership can transfer between the check and the write

1.2 WHEN the activity start path in `runtime.rs` commits a transition THEN the system calls `repo.commit_transition(task.run_key, transition, ShardEpoch::ZERO)` bypassing the fenced commit path entirely

1.3 WHEN the activity retry path in `runtime.rs` commits a transition THEN the system calls `repo.commit_transition(token.run_key, transition, ShardEpoch::ZERO)` bypassing the fenced commit path entirely

1.4 WHEN a continue-as-new successor is committed and has workflow timeouts THEN the system routes the timeout entry using `shard_for(new_state.run_key, 1)` which always maps to shard 0 regardless of the actual shard count

1.5 WHEN any unfenced mutation path commits while another runtime has already acquired the bundle lease at a higher epoch THEN the system silently accepts the stale write, corrupting workflow state

1.6 WHEN reviewing or auditing the fencing correctness of `crates/tokeira-storage/src/dsql/run_repository.rs` (~2000+ lines) THEN the reviewer cannot easily identify which methods are fenced vs unfenced because the file holds multiple independent correctness domains (commit, load, dispatch, timers, leases, visibility) in a single module

1.7 WHEN reviewing or auditing the runtime mutation paths in `crates/tokeira-runtime/src/runtime.rs` (~3000+ lines) THEN the reviewer cannot easily trace which paths go through fenced commit vs unfenced commit because workflow-task, activity, timer, query, and timeout logic are interleaved in a single file

### Expected Behavior (Correct)

2.1 WHEN `commit_transition_for_bundle` is called with a valid epoch THEN the system SHALL verify the shard_lease epoch and write the workflow mutation within the SAME DSQL transaction, so that OCC detects any concurrent epoch change and rejects the stale write atomically

2.2 WHEN the activity start path commits a transition THEN the system SHALL route through the fenced `commit_transition_for_bundle` path with the caller's current shard epoch and execution-home bundle

2.3 WHEN the activity retry path commits a transition THEN the system SHALL route through the fenced `commit_transition_for_bundle` path with the caller's current shard epoch and execution-home bundle

2.4 WHEN a continue-as-new successor is committed and has workflow timeouts THEN the system SHALL route the timeout entry using `shard_for(new_state.run_key, owner.shard_count())` where `shard_count` is obtained from the actual ownership state

2.5 WHEN any mutation path attempts to commit while another runtime holds the bundle lease at a higher epoch THEN the system SHALL reject the commit with a `CommitResult::Conflict` indicating the stale epoch, preventing state corruption

2.6 THE `crates/tokeira-storage/src/dsql/run_repository.rs` module SHALL be split into focused sub-modules along correctness boundaries: `commit.rs` (fenced commit primitive), `load.rs` (run loading and history reads), `activity.rs` (activity side-table operations), `timers.rs` (timer side-table operations), `leases.rs` (bundle lease operations), `dispatch.rs` (dispatch backlog), `visibility.rs` (projection log append). Each sub-module SHALL have a single correctness domain so that fenced vs unfenced paths are immediately obvious from the module boundary.

2.7 THE `crates/tokeira-runtime/src/runtime.rs` module SHALL be split into focused sub-modules: `workflow_task.rs` (WFT started/completed/failed/timed-out), `activity.rs` (activity start/complete/fail/retry/timeout), `query.rs` (query dispatch and buffering), `timeout.rs` (workflow/activity/nexus timeout scanning), `commit.rs` (the single entry point that routes all mutations through the fenced storage path). Each sub-module SHALL make it obvious that all durable mutations flow through the fenced commit entry point.

### Unchanged Behavior (Regression Prevention)

3.1 WHEN `ShardEpoch::ZERO` is passed in single-node compose or test environments THEN the system SHALL CONTINUE TO skip the epoch check and commit without fencing, preserving the local development and test experience

3.2 WHEN a commit is attempted and the epoch matches the current durable lease THEN the system SHALL CONTINUE TO apply the transition successfully and return `CommitResult::Applied`

3.3 WHEN an OCC conflict occurs due to concurrent writes to the same run (not epoch mismatch) THEN the system SHALL CONTINUE TO return `CommitResult::Conflict` and allow the lane retry loop to re-attempt

3.4 WHEN the in-memory store is used for testing THEN the system SHALL CONTINUE TO enforce fencing semantics when a non-zero epoch is provided, maintaining test fidelity with the DSQL implementation

3.5 WHEN the lane OCC retry loop encounters a conflict THEN the system SHALL CONTINUE TO retry up to `max_occ_retries` before surfacing the error to the caller

3.6 WHEN the storage and runtime modules are split into sub-modules THEN all existing public API signatures, trait implementations, and test coverage SHALL be preserved — the split is a reorganisation, not a rewrite
