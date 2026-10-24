# Bugfix Requirements Document

## Introduction

v1.31.0 decides whether to take an update request for a running workflow in one function, `Updater.ApplyRequest`
(`service/history/api/updateworkflow/api.go:104-195 @ v1.31.0`). Update-with-start uses the same function for a
running workflow (`service/history/api/multioperation/api.go:114, 281, 318 @ v1.31.0`). Holding the workflow's lock,
it checks in this order:
1. A closed workflow answers with the update's outcome when it knows the update, and with `ErrWorkflowCompleted`
   otherwise (`:118-125`).
2. A paused workflow answers `FailedPrecondition` "Workflow is paused. Cannot update the workflow." (`:128-130`).
3. A workflow whose workflow task keeps failing, at attempt 3 or more, answers `WorkflowNotReady` "Unable to perform
   workflow execution update due to Workflow Task in failed state." (`:34, 132-143`).
4. A workflow that has tried to close, and has a workflow task started, answers `ErrWorkflowClosing` (`:148-155`). That
   is `ResourceExhausted` with cause `BUSY_WORKFLOW`, scope `NAMESPACE` and the message "workflow operation can not be
   applied because workflow is closing" (`service/history/consts/const.go:63-67 @ v1.31.0`). A workflow has tried to
   close when a close command failed its workflow task with cause `UnhandledCommand`. The flag clears when a started
   workflow task closes (`service/history/workflow/workflow_task_state_machine.go:924-926, 1094-1108 @ v1.31.0`).
5. Only then does it look the update up (`FindOrCreate`, `service/history/workflow/update/registry.go:226-236 @
   v1.31.0`). An id the workflow already holds, in flight or completed, finds its update, and `Admit` does nothing
   (`service/history/workflow/update/update.go:301-307 @ v1.31.0`). A new id is admitted under the update limits.

So the three refusals for a running workflow come before any lookup. A request that repeats an update's id is refused
like a new one while the workflow is paused, failing or closing.

When a workflow task's completion carries an update protocol message, v1.31.0 also looks the update up first
(`handleMessage`, `service/history/api/respondworkflowtaskcompleted/workflow_task_completed_handler.go:350-395 @
v1.31.0`). `Find` returns:
- an update in flight;
- or a completed one, which mutable state still holds (`registry.go:455-480`; `GetUpdateOutcome`,
  `service/history/workflow/mutable_state_impl.go:1412-1442 @ v1.31.0`).

Only an update it can't find at all is rebuilt from the message (`TryResurrect`, `registry.go:238-281`).

A completed update takes no message:
- An acceptance or a rejection needs the update in state Admitted or Sent, and a response needs it in state
  ProvisionallyAccepted or Accepted (`update.go:470, 562, 609`).
- The check fails with `InvalidArgument` "invalid state transition attempted for Update `<id>`: received
  `*update.<Type>` message while in state Completed" (`update.go:648-656`; `service/history/workflow/update/state.go:45-46
  @ v1.31.0`).
- The handler then fails the workflow task with cause `BAD_UPDATE_WORKFLOW_EXECUTION_MESSAGE`
  (`workflow_task_completed_handler.go:383-386`).

Tokeira takes update requests in two places:
- **The runtime.** `update_workflow` (`crates/tokeira-runtime/src/runtime/query.rs`) looks the update id up in the run's
  history first. It attaches the request to an update it finds, before the kernel sees the request. Update-with-start
  calls it for a running workflow (`execute_multi_operation`, `crates/tokeira-runtime/src/runtime/lifecycle.rs`).
- **The kernel.** `apply_update` (`crates/tokeira-kernel/src/kernel.rs`) refuses a closed run and a paused run, then a
  duplicate id, then a request over the update limits.

Three things go wrong:
- **The refusals.** Neither place refuses a failing or a closing workflow. The paused refusal comes after the lookup.
  The edge answers it with `INTERNAL`, because only the pause and unpause RPCs' refusals map to `FailedPrecondition`
  (`crates/tokeira-edge/src/errors.rs`).
- **No record of completion.** The kernel keeps no record of which updates completed. A completion removes the id from
  the admitted and accepted sets, and adds one to `completed_update_count`.
- **So a completed update looks unknown.** A message for it gets the unknown update's treatment: resurrection, or
  `NotFound`.

## Bug Analysis

### Current Behavior (Defect)

1.1 WHEN an update request reaches a running workflow whose workflow task attempt is 3 or more THEN the system takes it
and attaches it to the failing workflow task, instead of refusing it with `WorkflowNotReady`.

1.2 WHEN an update request reaches a running workflow that has tried to close, while a workflow task is started, THEN
the system takes it instead of refusing it with `ErrWorkflowClosing`.

1.3 WHEN an update request reaches a paused workflow THEN the system answers `INTERNAL` "kernel rejected command:
workflow is paused" instead of `FailedPrecondition` "Workflow is paused. Cannot update the workflow."

1.4 WHEN an update request repeats the id of an update that a running workflow holds, while the workflow is paused,
failing or closing, THEN the system attaches the request to the update, instead of refusing it as 1.1-1.3 describe.

1.5 WHEN a workflow task's completion carries an acceptance for an update that has completed THEN the system treats the
update as unknown:
- with the update's request in the acceptance, it rebuilds the update and writes a second
  `WorkflowExecutionUpdateAccepted` event for it, so the update runs again;
- without the request, it fails the workflow task with `NotFound` "update `<id>` wasn't found on the server…".

1.6 WHEN a completion carries a rejection for an update that has completed THEN the system rebuilds the update and drops
it, and the workflow task completes, instead of failing.

1.7 WHEN a completion carries a response for an update that has completed THEN the system fails the workflow task with
`NotFound` "update `<id>` wasn't found on the server…", instead of the invalid state transition `InvalidArgument`.

### Expected Behavior (Correct)

2.1 WHEN an update request reaches a running workflow THEN the system SHALL apply 2.2, 2.3 and 2.4, in that order,
before it looks up the update's id or admits the request. The first of them that holds SHALL answer.

2.2 WHEN the workflow is paused THEN the system SHALL refuse the request with `FailedPrecondition` "Workflow is paused.
Cannot update the workflow."

2.3 WHEN the workflow's workflow task attempt is 3 or more THEN the system SHALL refuse the request with
`WorkflowNotReady` "Unable to perform workflow execution update due to Workflow Task in failed state.": gRPC
`FAILED_PRECONDITION` with a `WorkflowNotReadyFailure` detail.

2.4 WHEN the workflow has tried to close and a workflow task is started THEN the system SHALL refuse the request with
`ErrWorkflowClosing`, as it refuses a signal there today.

2.5 2.1-2.4 SHALL apply to UpdateWorkflowExecution, and to the update of an update-with-start whose workflow is running.
A refused update SHALL leave no trace: no admitted update, no workflow task and no waiter.

2.6 THE system SHALL keep, in a run's state, the id of each update that has completed in the run. A reset's new run SHALL
keep those whose completion it copies. A continue-as-new run SHALL start with none.

2.7 WHEN a workflow task's completion carries an acceptance, a rejection or a response for an update that has completed
THEN the system SHALL fail the workflow task with cause `BAD_UPDATE_WORKFLOW_EXECUTION_MESSAGE`. It SHALL answer the
worker with `InvalidArgument` "invalid state transition attempted for Update `<id>`: received `*update.Acceptance`
message while in state Completed", with `Rejection` or `Response` in place of `Acceptance` for those messages. It SHALL
NOT rebuild the update, write an event for the message, or count the message toward the update limits.

2.8 WHEN the system loads a run whose state holds no record of its completed updates but whose count of completed
updates is above zero THEN it SHALL recover the ids from the run's history before the run takes a command. A run that
has completed no update SHALL need no recovery.

### Unchanged Behavior (Regression Prevention)

3.1 WHEN an update request reaches a running workflow that is not paused, failing or closing THEN the system SHALL
CONTINUE TO find a known update and attach the request to it, or admit a new one under the update limits and schedule
or reuse a workflow task, as it does today.

3.2 WHEN an update request reaches a closed workflow THEN the system SHALL CONTINUE TO answer with the update's outcome
when it knows the update, and with `NotFound` "workflow execution already completed" otherwise.

3.3 WHEN an update request names a first execution run id that the run doesn't have THEN the edge SHALL CONTINUE TO
answer `NotFound` before any of 2.1-2.4.

3.4 WHEN a completion carries a message for an update in flight, or for an update the run doesn't know, THEN the system
SHALL CONTINUE TO treat it as it does today. That covers rebuilding an unknown update from an acceptance or a rejection
under the total limit, and `NotFound` for one it can't rebuild.

3.5 THE count of completed updates, the total limit and the continue-as-new advice SHALL CONTINUE TO count as they do.

3.6 THE state of a run that has completed no update SHALL CONTINUE TO encode byte for byte as it does today.

3.7 THE closing refusal of a signal SHALL CONTINUE TO work as it does.

## Out of Scope

- Update-with-start's retry of the whole operation when a closing workflow aborts its update. That is a deferred item of
  [api-conformance-multi-operation](../api-conformance-multi-operation/design.md) (Requirement 6), with its corpus leaf
  in the conformance skip registry.
- The legacy `UpdateCompleted` and `UpdateRejected` workflow commands, which have no wire form
  (`crates/tokeira-edge/src/grpc/translate.rs`). Workers send update messages, which 2.7 covers.
- A run's closing flag across a restart. v1.31.0 keeps it in memory too, so a reload clears it there as well
  (`mutable_state_impl.go:191, 2357-2359 @ v1.31.0`).
