# Bugfix Requirements Document

## Introduction

Some closes continue a workflow into a new run, its successor:
- **continue-as-new**, when a workflow task completes with a `ContinueAsNew` command;
- **retry**, when a run with a retry policy fails, through a workflow task's `FailWorkflow` command or a run timeout,
  and has attempts left;
- **cron**, when a cron run completes, fails or times out, and no retry continues it.

The close records the successor's run id: on `WorkflowExecutionContinuedAsNew`, or as `new_execution_run_id` on
`WorkflowExecutionFailed` or `WorkflowExecutionTimedOut`.

In v1.31.0 the successor is written in the close's own transaction. `updateWorkflowExecutionTx` writes the closing run
and the new run together, and moves the workflow's current-run pointer to the new run only while the pointer names the
closing run (`common/persistence/sql/execution.go:360-439`; `assertRunIDAndUpdateCurrentExecution`,
`common/persistence/sql/execution_util.go:966-1009 @ v1.31.0`). Every path that creates a successor goes through it: a
workflow task's completion (`service/history/api/respondworkflowtaskcompleted/api.go:608-626 @ v1.31.0`) and a run
timeout (`service/history/timer_queue_active_task_executor.go:743-784 @ v1.31.0`). A close either has its successor or
didn't happen.

Tokeira commits the close alone, then starts the successor with a separate command:
- **Continue-as-new.** After the close commits, the lane builds the successor's `StartRequest` from the close's event,
  and submits it to the successor's lane in a spawned task. A failure is only logged (the `ContinuedAsNew` branch of the
  lane's post-commit path, `crates/tokeira-runtime/src/lane.rs`).
- **Retry and cron after a workflow task.** After the close commits, `start_retry_successor` or `start_cron_successor`
  reloads the run, reads its started event, builds the successor and submits its start through `TokeiraRuntime::submit`.
  A failure is logged (`crates/tokeira-runtime/src/runtime/workflow_task.rs`).
- **Retry and cron after a run timeout.** After the close, `submit_workflow_timeout` calls
  `start_timeout_retry_successor` or `start_timeout_cron_successor`, which submit the start to the successor's lane. A
  failure is logged (`crates/tokeira-runtime/src/timeout.rs`).

Nothing drives a successor's start again: no sweep, scanner or restart reads a committed close that lacks its successor
([Durable Actionable State](../../../docs/architecture/042-durable-actionable-state.md), "Why").

## Bug Analysis

### Current Behavior (Defect)

1.1 WHEN a close with a successor commits and the successor's start then doesn't commit, because the process crashes,
the node loses the execution home, the start fails or its retries run out, THEN the close stands with a successor run
id that names no run. The workflow's chain ends, and a parent waiting for the chain to end is never told.

1.2 WHEN the node is draining THEN the start of a retry or cron successor after a workflow task is refused:
`TokeiraRuntime::submit` refuses a `Command::Start` on a draining node, as it refuses a client's
(`is_externally_routed_command` in `crates/tokeira-runtime/src/runtime/mod.rs`). The chain ends as in 1.1.

1.3 WHEN a client starts the workflow id between a close and its successor's start THEN the client's start finds the
current run closed and takes the workflow id, and the successor's start fails with a current-execution conflict. The
chain ends as in 1.1. In v1.31.0 the successor is already current when the client's start arrives, and the client's own
conflict policy decides.

### Expected Behavior (Correct)

2.1 WHEN a close continues into a successor, by continue-as-new or by a retry or cron successor after a workflow task or
a run timeout, THEN the system SHALL write the successor run in the close's own storage transaction: the close's write
set and the successor's start, its history, state, dispatch rows and current-run pointer, together. Either both SHALL
commit, or neither SHALL.

2.2 The transaction SHALL read the execution home's fence once, for both runs, which share the home.

2.3 The transaction SHALL move the workflow's current-run pointer from the closing run to the successor only while the
pointer names the closing run. WHEN it names another run THEN the transaction SHALL write nothing, and the commit SHALL
fail as a conflict.

2.4 THE runtime SHALL use the same successor start for the paired commit and its size fallback,
with the same run id and request id. The field policy is refined by
[successor-semantics](../successor-semantics/bugfix.md); preserving the original delivery's
builders does not require preserving the defects that specification corrects.

2.5 WHEN the transaction commits THEN the runtime SHALL do for the successor what it does after a successor's start
commits today: install its workflow timeouts, its workflow-task tracking and its dispatch, before it replies to the
close's caller.

2.6 WHEN DSQL refuses the transaction for its size (SQLSTATE 54000), which writes nothing, THEN the runtime SHALL commit
the close alone and then start the successor with a separate command, as today. It SHALL start the successor only if
the close applied, and a drain SHALL NOT refuse that start.

2.7 A drain SHALL NOT stop a close's successor: a successor written by 2.1 needs no command of its own.

2.8 WHEN the kernel rejects the successor's start THEN nothing SHALL be committed, and the close's command SHALL fail
with the rejection, as v1.31.0 fails a workflow task's completion whose new run it can't set up
(`service/history/api/respondworkflowtaskcompleted/workflow_task_completed_handler.go:1029-1039, 1354-1369 @
v1.31.0`).

### Unchanged Behavior (Regression Prevention)

3.1 The kernel SHALL CONTINUE TO apply one command to one run. The close and the successor's start SHALL be two
applications of the kernel, the second to a run that doesn't exist yet (`LoadedRun::Absent`).

3.2 A close without a successor SHALL CONTINUE TO commit alone, as today.

3.3 THE runtime SHALL preserve successor fields outside the explicit corrections in
[successor-semantics](../successor-semantics/bugfix.md), including the existing identity
and lineage handling.

3.4 A repeated close SHALL CONTINUE TO be answered as today: a close that has committed is a conflict or a duplicate on
its retry, and no second successor is written.

3.5 A reset SHALL CONTINUE TO materialize its new run as it does today.

3.6 This change SHALL add no migration and change no version in the schema contract.

### Out of Scope

- **How a successor is built.** The original atomicity delivery retained existing field behaviour.
  [Successor semantics](../successor-semantics/bugfix.md) now specifies the separate corrections:
  original metadata and links, cron preservation across retry, timeout outcomes, retry-to-cron
  selection and minimum delay. Explicit continue-as-new already carries its header and minimum
  delay. These refinements do not weaken this specification's atomic persistence contract.
- **The size fallback.** 2.6's separate start is still lost on a crash, as today. It becomes durable with the rows
  [Durable Actionable State](../../../docs/architecture/042-durable-actionable-state.md) gives the deliveries a commit
  leaves to happen later.
- **Closes by earlier releases.** A run whose successor an earlier release lost stays without one.
- **Reset's follow-up, a child's resolution and the dispatch operations.** Each is a delivery after the commit, and each
  has a change of its own.
