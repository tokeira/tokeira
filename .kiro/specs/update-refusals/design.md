# Update Refusals — Bugfix Design

## Overview

The fix gives update requests and update messages v1.31.0's answers:
- **Refusals before the lookup.** The runtime refuses an update request for a paused, failing or closing workflow, in
  v1.31.0's order, before it looks the update up. The kernel checks the paused and failing refusals again when it
  applies the request.
- **Completed updates in state.** A run's state keeps the id of each update that has completed in it, as v1.31.0's
  mutable state keeps each update's record (`ExecutionInfo.UpdateInfos`). A message for one of those ids fails the
  workflow task, as v1.31.0's state machine does.
- **Runs from before this change.** Their completed ids are recovered from history when the run is loaded, in the read
  that loads it.

## Glossary

- **Running**: the run's status is open: Running or Paused.
- **Failing**: the run's `workflow_task_attempt` is 3 or more. It is the attempt of the run's next or current workflow
  task, which v1.31.0 calls `WorkflowTaskAttempt`. It rises with each failed or timed-out task and returns to 1 when a
  task completes.
- **Closing**: the runtime has recorded that a close command of the run failed its workflow task with cause
  `UnhandledCommand`, and the run has a workflow task started. The record is the runtime's `close_attempt_tracking`,
  which the signal refusal reads today (`crates/tokeira-runtime/src/runtime/lifecycle.rs`).
- **Lookup**: `look_up_update` in `crates/tokeira-runtime/src/runtime/query.rs`. It finds an update the run holds, in
  flight or completed, so that the request can attach to it.
- **Completed update**: an update whose `WorkflowExecutionUpdateCompleted` event, in either form, is in the run's
  history.
- **Update message**: an acceptance, a rejection or a response that a workflow task's completion carries for an update.

## Bug Details

### Bug Condition

```
isBugCondition(request) =
    running(run) and (paused(run) or failing(run) or closing(run))

isBugCondition(message) =
    message.update_id in completed_updates(run)
```

### Examples

1. **Failing.** A workflow's task fails twice, so its attempt is 3. An update request is admitted and waits behind the
   failing task. v1.31.0 answers `WorkflowNotReady` at once, so the caller can stop waiting.
2. **Closing.** A workflow task completes with a CompleteWorkflowExecution command while a signal is buffered. The task
   fails with `UnhandledCommand`, and the retry is started. An update request is admitted. v1.31.0 answers
   `ErrWorkflowClosing`, a retryable error.
3. **Paused.** An update request to a paused workflow answers `INTERNAL`. v1.31.0 answers `FailedPrecondition`.
4. **A repeat while paused.** An update is admitted, and the workflow is paused. A second request with the same id
   attaches to the update. v1.31.0 refuses it as paused.
5. **An acceptance after completion.** A worker that lost its update cache replays an acceptance, with the request, for
   an update that completed. The run writes a second UpdateAccepted event for it, and runs the handler again.

## Expected Behavior

Expected behaviour is criteria 2.1 to 2.8 of [bugfix.md](bugfix.md).

### Preservation Requirements

From bugfix.md 3.1 to 3.7:
- **Admission.** A running workflow that is not paused, failing or closing takes a request as it does today.
- **Closed workflows.** A closed workflow answers as it does today.
- **The first execution run id.** The edge checks it first, as today.
- **Other messages.** A message for an update in flight, or for an update the run doesn't know, is handled as today.
- **Counts and encoding.** The counts of updates, and a state with no completed update, are unchanged.
- **Signals.** The signal's closing refusal is unchanged.

## Root Cause

1. **The lookup comes first.** `update_workflow` attaches a request to an update it finds before anything checks the
   workflow. v1.31.0 checks the workflow first, and finds the update only after that (`api.go:118-161`).
2. **Two refusals are missing.** Neither the runtime nor the kernel checks the task's attempt or the close attempt for
   an update. Only signals check the close attempt.
3. **The paused refusal has no answer of its own.** The kernel refuses with `Reject::WorkflowPaused`, which crosses the
   lane as an `anyhow` error. The edge's catch-all maps it to `INTERNAL`.
4. **Completion leaves no record.** The kernel removes a completed update from its admitted and accepted sets, and only
   counts it. Its message handler then can't tell a completed update from one it never knew.

## Correctness Properties

### Property 1: The refusals come first, in order

_For any_ running run, with any combination of paused, failing and closing, and any update id, known or new, the answer
to an update request SHALL be the first of v1.31.0's refusals that holds, paused then failing then closing. When none
holds, it SHALL be today's answer: attach to the known update, or admit the new one.

**Validates: Requirements 2.1, 2.2, 2.3, 2.4, 3.1**

### Property 2: A refused request leaves no trace

_For any_ refused update request, the run's state, its history, its workflow task and the runtime's update registry
SHALL be as they were before the request.

**Validates: Requirement 2.5**

### Property 3: The completed ids are the history's

_For any_ sequence of admissions, acceptances, rejections and completions of updates, and resets, the run's completed
update ids SHALL equal the ids of the completion events in its history. The state replayed from that history SHALL hold
the same ids.

**Validates: Requirement 2.6**

### Property 4: A message for a completed update fails the task

_For any_ completed update and any acceptance, rejection or response for it, the workflow task's completion SHALL fail
with cause `BAD_UPDATE_WORKFLOW_EXECUTION_MESSAGE` and v1.31.0's message for that message's type. The run SHALL write no
event for it and SHALL leave its counts unchanged.

**Validates: Requirements 2.7, 3.4, 3.5**

### Property 5: A run from before this change recovers its ids

_For any_ open run whose state was encoded without the section, with any history of updates, loading the run SHALL
give the ids of the completion events in its history. Re-encoding a state with no completed update SHALL give the same
bytes as before this change.

**Validates: Requirements 2.8, 3.6**

## Fix Implementation

### The refusals

**The kernel** (`apply_update`, `crates/tokeira-kernel/src/kernel.rs`):
- After the closed and paused checks, refuse a failing run with a new `Reject::UpdateWorkflowTaskFailing`, before the
  duplicate check and the limits. The order follows `api.go:118-143`.
- The kernel can't see the close attempt, which is volatile runtime state, as in v1.31.0 (`mutable_state_impl.go:191`).
  So the kernel has no closing refusal.

**The runtime** (`update_workflow`, `crates/tokeira-runtime/src/runtime/query.rs`). It reads the run's state once, then:
1. For a closed run, does what it does today: it looks the update up and answers with its outcome, or submits the
   request, and the kernel refuses it as closed.
2. For a running run, refuses in this order:
   - a paused run, with a new `UpdatePaused` error;
   - a failing run, with `WorkflowNotReady`, carrying v1.31.0's message;
   - a closing run, with `WorkflowClosing`.
3. Only then looks the update up and attaches the request, or submits a new one.

A refusal comes before the update registry takes an entry, so it leaves no waiter. The kernel's paused and failing
refusals, which a change between the runtime's read and the lane's apply can still raise, map to the same two errors.
`execute_multi_operation` reaches all of this through `update_workflow`.

**Why a refusal after the read is right.** v1.31.0 checks the workflow and admits the update under one lock. The
runtime's read and the lane's apply are two steps, and the workflow can change between them:
- **The paused and failing checks.** The kernel repeats them when it applies the request, so admission holds them at
  that point as v1.31.0's does.
- **The closing check.** It runs only before the lane, as the signal refusal's does today. A close attempt that starts
  after the read lets one more update in. v1.31.0 would have taken that update a moment earlier too.
- **A request that attaches to an update in flight.** It never reaches the lane, so it is checked only at the read.
  That gives the answer v1.31.0 gives to a request that arrives just before the workflow changes.

**The answers** (`crates/tokeira-runtime/src/errors.rs`; `crates/tokeira-edge/src/errors.rs`):

| Refusal | Runtime error | Wire |
|---------|---------------|------|
| Paused | `UpdatePaused` | `FAILED_PRECONDITION` "Workflow is paused. Cannot update the workflow." |
| Failing | `WorkflowNotReady` with the update message | `FAILED_PRECONDITION` with a `WorkflowNotReadyFailure` detail |
| Closing | `WorkflowClosing` | `RESOURCE_EXHAUSTED`, cause `BUSY_WORKFLOW`, scope `NAMESPACE` |

`WorkflowNotReady` and `WorkflowClosing` exist, and the edge maps them today. `UpdatePaused` is new, and the edge maps
it beside them.

### Completed updates

**The state** (`crates/tokeira-kernel/src/state.rs`): `WorkflowState` gains `completed_updates: BTreeSet<String>`. It is
the ids of the updates that have completed in the run, and it is bounded by the total update limit, as v1.31.0's
records are. A continue-as-new run starts with an empty set.

**The kernel**:
- **On completion.** Each arm that writes or replays a `WorkflowExecutionUpdateCompleted` event, in either form, adds
  the id: the response arm of the message handler, the replay arm, and the legacy `UpdateCompleted` command arm. So a
  reset's new run holds the ids of its copied history.
- **On a message.** The message handler looks the update up as v1.31.0's `Find` does:
  - in flight first: admitted or accepted;
  - then completed;
  - then, for an update it doesn't know, as today.

  For a completed update, every message type returns `Reject::BadUpdateMessage` with `not_found: false`. The message is
  "invalid state transition attempted for Update `<id>`: received `*update.Acceptance` message while in state
  Completed", with `Rejection` or `Response` for those types. It comes before any resurrection, so the message rebuilds
  nothing and counts toward no limit. The message's size check still comes first, as today (`workflow_task_completed_handler.go:358-365`).
- **A pure helper,** `completed_update_ids(events) -> BTreeSet<String>`, gives the ids of the completion events in a
  history. The kernel's replay and the stores' recovery both use it.

**The encoding** (`crates/tokeira-storage/src/codec.rs`):
- A new extension section, `COMPLETED_UPDATES_SECTION = 5`, holds the ids as a postcard `Vec<String>` in ascending
  order. The layout is frozen.
- It is written only when the set isn't empty, so a run that has completed no update encodes as it does today.
- A decoder that meets the section checks the ids are strictly ascending, as the admitted-updates section's does.
- A release before this one drops the section as a later release's, by the codec's rule.

**Recovery at load.** A state decoded without the section, from an open run whose `completed_update_count` is above
zero, was written before this change, or rewritten by a release before it. It is the only state with an empty set and a
count above zero, since each completion adds an id. The stores recover its ids when they load it:
- **DSQL** (`load_run`, `crates/tokeira-storage/src/dsql/run_repository/load.rs`): such a state takes the load's
  `REPEATABLE READ READ ONLY` path, which the accumulator seed already uses. That path reads the run's history batches
  in the same snapshot as the state, with their parts, and applies `completed_update_ids`.
- **In memory** (`crates/tokeira-storage/src/memory.rs`): its load reads the run's history under the same lock as its
  state.

A closed run needs no recovery, since no message reaches it. So a closed run from before this change loads as it does
today. A recovered run keeps its ids from its next commit on.

### Specs this changes

The code PR aligns these:
- **[runtime-update-lifecycle](../runtime-update-lifecycle/requirements.md)**:
  - Requirement 8 and the design's error table answer a paused workflow with `FailedPrecondition` and v1.31.0's
    message;
  - the paused, failing and closing refusals come before the lookup.
- **[signal-update-limits](../signal-update-limits/bugfix.md)**: criterion 2.13 covers an update the run has neither
  admitted, accepted nor completed. A completed one is this spec's 2.7.
- **[speculative-wft](../speculative-wft/design.md)**: its table of message outcomes gains the completed update.
- **[admitted-updates-after-restart](../admitted-updates-after-restart/design.md)**: the registry v1.31.0 rebuilds from
  mutable state includes completed updates, which this spec keeps in state.

## Testing Strategy

### Exploratory Bug Condition Checking

Runtime tests on the in-memory store, run on the code before the fix. Each predicts today's answer:
1. **Failing**: an update request after two failed tasks is admitted. It should be refused.
2. **Closing**: an update request while the retried close is started is admitted. It should be refused.
3. **Paused**: an update request to a paused workflow answers `INTERNAL`.
4. **A repeat while paused**: a request repeating an admitted update's id attaches.
5. **An acceptance after completion**: the task completes, and the history holds two UpdateAccepted events for the
   update.
6. **A rejection after completion**: the task completes.
7. **A response after completion**: the task fails with `NotFound`.

### Property-Based Tests

Tagged `// Feature: update-refusals, Property N: <title>`.
- **Property 1**: the runtime's refusal, as a pure function of the run's status, attempt, closing record and lookup,
  over generated combinations. A runtime test over generated sequences of task failures, pauses and close attempts
  checks the answers end to end.
- **Property 2**: for each generated refusal, the state, the history, the task and the registry before and after.
- **Property 3**: generated sequences of update admissions, acceptances, rejections, completions and resets in the
  kernel. After each step, the completed ids equal those the history records, and the replayed state's equal the live
  one's.
- **Property 4**: generated completed updates and message types. The completion fails with the exact message and
  writes nothing.
- **Property 5**: the codec over generated states, encoded with and without the section; recovery over generated
  histories, on both stores.

### Unit Tests

- The section's encoding: absent when empty, ascending ids, a non-ascending payload refused.
- `UpdatePaused`'s wire answer, and the failing message on `WorkflowNotReady`.
- `completed_update_ids` over each completion event form.

### Live Tests

On an ephemeral cluster, for the DSQL store's part of Property 5:
1. **Recovery on DSQL.** A state written without the section, for an open run whose history holds completed updates,
   loads with their ids.
2. **A closed run.** A closed run from before this change loads by the fast path, with no history read.

### Negative controls

Each is patched in alone, and each must fail its test:
1. The lookup before the refusals: exploration test 4 fails.
2. The failing check at attempt 4 instead of 3: Property 1 fails.
3. The kernel without the failing refusal: the runtime test of a task failing between the read and the apply fails.
4. A completion that doesn't add its id: Property 3 fails.
5. The completed check after resurrection: exploration test 5 fails.
6. A response for a completed update still answered `NotFound`: Property 4 fails.
7. No recovery at load: live test 1 fails.
8. The section written when empty: Property 5's encoding check fails.

### Preservation Checking

These pass unchanged:
- the kernel's, runtime's and storage's update tests;
- the signal closing tests;
- the update and update-with-start leaves of the functional conformance corpus.
