# Atomic Successors — Bugfix Design

## Approved recovery refinement

[Outbound recovery](../outbound-dispatch-recovery/design.md) supersedes this original
delivery's separate-start fallback, reset preservation and no-migration boundary.
Its frozen intent plus reservation replaces the detached fallback task; full reset
preparation precedes atomic publication. The original implementation sketches and
completed task evidence below describe the earlier delivery where not yet reconciled.

## Overview

A close that continues into a successor commits with it, in one storage transaction, as v1.31.0's
`updateWorkflowExecutionTx` does:
- **The successor's plan.** The command that closes a run carries what its successor needs, in the lane's message
  beside the kernel's command. For continue-as-new, that is the close's own event, as today.
- **Two kernel applications.** The lane applies the close, builds the successor's `StartRequest` with today's code, and
  applies the start to the successor, which doesn't exist yet. The kernel still sees one run at a time.
- **One transaction.** Storage writes both transitions in one transaction: one fence read, the close first, and the
  current-run pointer moved from the closing run to the successor only while it names the closing run.
- **After the commit.** The lane does the closing run's post-commit work, then the successor's, as it does after a
  successor's start today, and replies.
- **The size fallback.** An explicit size refusal commits the close, frozen successor
  intent and chain reservation together; the owned recovery driver publishes the
  successor and retires the reservation/intent atomically.

## Glossary

- **Successor:** the run a close continues into: by continue-as-new, or a retry or cron successor after a workflow task
  or a run timeout. The close names it, on `WorkflowExecutionContinuedAsNew` or as `new_execution_run_id`.
- **Successor plan:** what the lane needs, beyond the closing run's state after the close, to build the successor's
  `StartRequest` with today's code. It travels in the lane's message, never in the kernel's `Command`.
- **Pair commit:** the storage transaction that writes a close and its successor's start.
- **Current-run pointer:** the workflow id's `current_execution` row, which names its current run and whether it is
  open.
- **Size refusal:** DSQL's SQLSTATE 54000, which refuses a transaction over 3,000 rows or 10 MiB, or a value over 1 MiB,
  and aborts the whole transaction (`crates/tokeira-storage/src/write_budget.rs`).

## Bug Details

### Bug Condition

The bug condition holds when a close with a successor commits, and its successor's start then doesn't commit, or
commits after another start has taken the workflow id.

### Examples

- **A crash.** A workflow task completes with `ContinueAsNew`. The close commits, and the process dies before the
  spawned start runs. Today the close names a successor that no restart creates. After the fix, the successor committed
  with the close, and the restart's sweep offers its first workflow task.
- **A drain.** A run with a retry policy fails on a node that is draining. Today the close commits and
  `TokeiraRuntime::submit` refuses the successor's start. After the fix, the successor is written with the close.
- **A racing client.** A cron run completes, and a client starts the same workflow id before the cron successor's start
  lands. Today the client's run takes the id, and the cron successor's start fails. After the fix, the cron successor
  is current from the close on, and the client's start meets it under the client's own conflict policy.
- **A large successor.** A continue-as-new carries an input and memo so large that the close and the successor together
  pass 10 MiB, though each fits alone. DSQL refuses the pair; the close, frozen successor
  intent and chain reservation commit together. Owned recovery stages and publishes
  that same successor, including after process loss or drain.

## Expected Behavior

### Preservation Requirements

- The kernel applies one command to one run (3.1).
- A close without a successor commits alone (3.2).
- Successor fields outside the corrections in [successor-semantics](../successor-semantics/bugfix.md)
  retain their existing policy (3.3).
- A repeated close is answered as today, and writes no second successor (3.4).
- Complete reset publication follows the approved outbound-recovery refinement (3.5).
- The original change added no migrations; outbound recovery has its own versioned
  schema and stopped-writer cutover (3.6).

## Root Cause

- **Storage writes one run per transaction.** `RunRepository::commit_transition` takes one run's transition. Nothing
  can write a close and a start together, though both runs share the execution home that fences them: every run of a
  workflow id does (`execution_home_bundle`, `crates/tokeira-types/src/routing.rs`).
- **The successor is built from committed state.** Each path builds the successor after the close has committed, from
  the close's event or by reloading the run and its started event, and then submits a start like any other.
- **A start is a client's command.** `TokeiraRuntime::submit` admits a `Command::Start` as it admits a client's, so a
  drain refuses it. A start that fails is logged, and nothing records that it is owed.

## Correctness Properties

Property 1: A close and its successor commit together

_For any_ close with a successor, and any failure of a statement in its transaction or a crash before its commit,
storage SHALL then hold both the close and the successor, or neither.

**Validates: Requirements 2.1, 2.2**

Property 2: The pointer moves only from the closing run

_For any_ current-run pointer at the time of a pair commit, the commit SHALL move the pointer only when it names the
closing run, and SHALL then leave it naming the successor, open. WHEN the pointer names any other run THEN the commit
SHALL write nothing and SHALL answer a conflict.

**Validates: Requirements 2.3**

Property 3: Paired and fallback starts agree

_For any_ close with a successor, the paired path and the size fallback SHALL use the same built
start, including identity and corrected fields. The original legacy-builder equivalence test
remains a preservation oracle only for fields outside
[successor-semantics](../successor-semantics/bugfix.md).

**Validates: Requirements 2.4, 3.3**

Property 4: The fallback keeps the close and its successor obligation

_For any_ pair commit that DSQL refuses explicitly for its size, the close SHALL commit
with the frozen successor intent and reservation, or none of them SHALL commit. The
successor SHALL become current through fenced staged publication after that commit;
drain or process loss SHALL NOT erase the obligation. See outbound-recovery Property 14
for its stronger crash and competing-writer contract.

**Validates: Requirements 2.6**

Property 5: A rejected successor fails the close

_For any_ close whose successor's start the kernel rejects, nothing SHALL be committed, and the close's command SHALL
fail with the rejection.

**Validates: Requirements 2.1, 2.8**

## Fix Implementation

### Storage

- **The pair commit.** `RunRepository` gains a commit that takes the closing run's key and transition and the
  successor's key and transition, with one `WriteFence`. It answers as `commit_transition` does, with both runs' new
  states when it applies, and adds a size refusal.
- **On DSQL,** in one transaction:
  1. the fence read, first, for the execution home both runs share;
  2. the closing run's checks, as `do_commit_transition` makes them: its `workflow_hot` row `FOR UPDATE` and its
     transition sequence, then its request dedupe;
  3. the successor's checks: no `workflow_hot` row, and its request dedupe;
  4. the current-run pointer, read `FOR UPDATE`. A pointer that names another run rolls the transaction back, and the
     commit answers a conflict. A reset's materialization checks the pointer the same way (`finish_materialization`,
     `crates/tokeira-storage/src/dsql/run_repository/bulk_write.rs`);
  5. the closing run's write set, then the successor's, each as `write_transition` writes it. The closing run's write
     marks the pointer closed, and the successor's start points it at the successor, open;
  6. the commit. A serialization failure (40001) answers a conflict, as today. A size refusal, at any statement or at
     the commit, answers the size refusal.
- **In memory,** both transitions apply under the store's one lock, with the same checks and the same pointer
  condition. The in-memory store applies none of DSQL's limits, so it never answers a size refusal.

### Runtime

- **The plan.** Each close command gets its successor's plan in the lane's message:
  - **continue-as-new:** none beyond the close's `WorkflowExecutionContinuedAsNew` event, as today;
  - **retry after a workflow task:** the run's started event's input and versioning info, which the runtime reads
    before it submits the completion, as `cron_continuation_for_completion` already reads the started event for cron;
  - **cron after a workflow task:** the cron continuation the command already carries, and the completion's time, which
    anchors the backoff;
  - **retry or cron after a run timeout:** the policy, started event and anchor that `submit_workflow_timeout` already
    reads before it submits the close.
- **Building and applying.** After the kernel applies the close, the lane builds the successor's `StartRequest` from
  the transition's new state and the plan, with today's code: the lane's continue-as-new block,
  `build_retry_successor_start` and `build_cron_successor_start`. It then applies the start to `LoadedRun::Absent`. A
  rejected start fails the close's command, and nothing is committed. v1.31.0 also fails a completion whose new run it
  can't set up (`workflow_task_completed_handler.go:1029-1039, 1354-1369 @ v1.31.0`).
- **Committing.** The lane commits the pair with the fence it would use for the close. A conflict retries the whole
  command, close and successor, as a conflict on a close does today. The successor's run id comes from the command or
  its plan, so a retry builds the same successor.
- **After the commit.** The lane does the close's post-commit work, as today, then the successor's, as it does after
  committing a start: workflow timeout tracking, workflow-task tracking and dispatch. Then it replies to the close's
  caller. A crash before that work leaves it to the sweep, which recovers the successor like any committed start.
- **The size fallback.** `commit_successor_fallback` stores the close with its frozen
  start and chain reservation. Publication uses the owned, fenced preparation path,
  and remains discoverable after loss of the original caller or notifications.
- **What goes.** The spawned continue-as-new start, `start_retry_successor`, `start_cron_successor`,
  `start_timeout_retry_successor` and `start_timeout_cron_successor` no longer run after a close. The fallback's
  staged publication is the one place a successor is materialized after its close,
  protected by the durable reservation and frozen intent.

### Specs this changes

- [runtime-continue-as-new](../runtime-continue-as-new/requirements.md):
  - criteria 5.1 and 5.3: the successor is written in the close's transaction, not started after it;
  - criteria 6.1-6.4: they apply to the size fallback's separate start only;
  - criterion 7.1: the successor's tracking is installed when the pair commits;
  - the design's Properties 5 and 6, and its error handling for the successor's start, follow.
- [runtime-complete-implementation](../runtime-complete-implementation/requirements.md): criterion 8.1.2, in the same
  way, and Requirement 8.3, which applies to the size fallback only.
- [workflow-retry-chain](../workflow-retry-chain/requirements.md): criteria 2.2.1 and 2.2.3, the successor is written
  with the failed close and started separately only in the size fallback; criterion 5.1.3, exactly one successor,
  written with the close.
- [kernel-continue-as-new-timeout](../kernel-continue-as-new-timeout/requirements.md): criterion 2.1.4 and the design's
  overview of `ContinueAsNew`: the runtime builds the successor's start and commits it with the close.
- [kernel-complete-implementation](../kernel-complete-implementation/requirements.md): criterion 4.1.4, in the same
  way.
- [edge-history-parent-chain](../edge-history-parent-chain/requirements.md): no change. Its criteria give a successor's
  fields, not when it is written.

### Documentation

- `docs/architecture/020-kernel.md`: its Continue-As-New and workflow retry sections.
- `docs/architecture/042-durable-actionable-state.md`: the successor's "today" lines.
- `docs/readiness/conformance.md`: a note on the cron tier's row, which records the successor as a start after the
  close.
- `docs/testing/dsql-live-suites.md`: the live test.

## Testing Strategy

### Exploratory Bug Condition Checking

Written and run on the code before the fix, each failing there. Each asserts that the successor exists once the close
is committed:
- **offline:** a continue-as-new whose successor's start fails, on the in-memory store. On the code before the fix, the
  close is committed and the successor is absent;
- **offline:** a workflow task that fails a run with retries left, on a node that is draining. On the code before the
  fix, the successor's start is refused;
- **offline:** a cron run's completion, then a client's start of the same workflow id before the cron successor's
  start. It also asserts that the client's start meets the running successor. On the code before the fix, the client's
  run is current and the cron successor is absent.

### Property-Based Tests

- Property 1 runs offline on the in-memory store, over generated closes with successors and an injected failure at each
  step of the pair commit, and a crash before it.
- Property 2 runs offline over generated pointer states: absent, naming the closing run, naming another run, open or
  closed.
- Property 3 runs offline over generated closes of each kind, comparing the start used by normal
  pair and explicit size fallback. Legacy-builder comparison is retained only for unchanged fields;
  corrected fields use the independent successor-semantics oracle.
- Property 4 runs offline with a store that refuses every pair for its size, on a node that is draining.
- Property 5 runs offline with a kernel that rejects the successor's start.

### Unit Tests

- A size refusal at a statement and at the commit each answer the size refusal.
- The fence read is the pair commit's first statement.
- A conflict retries the close and the successor with the same successor run id.

### Live Test

One test, on an ephemeral cluster:
1. **Each kind.** A continue-as-new, a retry and a cron close, each by a workflow task, and a retry by a run timeout.
   After each, the closing run is closed, the successor's `workflow_hot` row and started event exist, and the pointer
   names the successor, open.
2. **All or nothing.** A pair commit whose successor's key already has a `workflow_hot` row. The commit fails, and the
   closing run is still open, its pointer unchanged.
3. **The pointer.** The pointer moved to another run key with SQL before a close. The pair commit answers a conflict
   and writes nothing.
4. **A large successor.** A continue-as-new whose input, memo and header make the pair pass DSQL's transaction limit,
   though the close fits alone. The close commits with its durable successor obligation,
   and recovery publishes the successor. If no values within the
   payload limits can do that today, the test records why, and Property 4 is checked offline only.

### Negative controls

Each control is patched in alone, then run, then reversed so that the tree is byte-identical. Each must fail the test
that covers it:
- **offline:** the successor written in a transaction after the close's;
- **offline:** the pointer condition skipped;
- **offline:** the fallback taken on any error, not only a size refusal;
- **offline:** the fallback's start submitted through `TokeiraRuntime::submit`;
- **live:** the successor's write set left out of the pair commit;
- **live:** the pointer condition skipped.

### Preservation Checking

The continue-as-new, retry-chain, cron, reset and timeout tests, the runtime's lane tests and the corpus tiers for
continue-as-new, retry and cron stay green.
