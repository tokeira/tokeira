# Admitted Updates After a Restart — Bugfix Design

## Overview

The run's state gains the set of its updates admitted by a WorkflowExecutionUpdateAdmitted event. Before the kernel applies any command, the lane forgets the run's other admitted updates whose requests the registry no longer holds; that step lives in the kernel crate. After it, the run holds only admitted updates it can deliver, by a protocol message or by history, so:
- the kernel counts them from its own state;
- a follow-up speculative task is scheduled only for updates a message must carry;
- a client's retry of a forgotten update is admitted as a new one.

## Glossary

- **Held update:** an admitted, unaccepted update whose request the owning node's `UpdateRegistry` holds. A workflow task delivers it as a protocol message.
- **History-admitted update:** an admitted, unaccepted update recorded as a WorkflowExecutionUpdateAdmitted event: one a reset reapplied, or the replay of a copied history admitted. It reaches the worker in history, and v1.31.0 keeps it across reloads.
- **Lost update:** an admitted, unaccepted update that is neither held nor history-admitted. Its request went with a restart or a move of the run to another node.
- **In flight:** accepted updates, held updates and history-admitted updates (bugfix 2.5).

## How this maps onto Tokeira's architecture

v1.31.0 keeps a run's updates in an in-memory registry that it rebuilds from mutable state when it loads the run. Mutable state holds accepted and completed updates, and history-admitted ones without their requests (`registry.go:168-224 @ v1.31.0`). Tokeira differs in four ways that shape the fix.

1. **Admitted ids are stored with the run.** Tokeira keeps every admitted id in `WorkflowState::admitted_updates` and the requests in the registry, so after a restart the state names updates whose requests are gone. Forgetting those ids is the step that matches v1.31.0's rebuild. The lane takes it, since it alone sees both the state it loaded and the registry, immediately before the kernel applies the next command; the forget lands in that command's transition. It is the same seam through which the lane reports the held updates' request bytes today (`signal-update-limits`).
2. **History admission is not recorded apart.** The state doesn't say which admitted ids have a WorkflowExecutionUpdateAdmitted event. The fix records them in a new state-extension section, since the state's own layout is positional.
3. **Speculative workflow tasks are stored.** v1.31.0 keeps a speculative task only in memory, so a reload drops it. Tokeira stores it and recovery republishes it. So when the forget leaves an unstarted speculative task with nothing to deliver, it drops that task too.
4. **The registry dedupes requests before the kernel.** The runtime looks a client's update id up in the run's state before it registers the request. A lost id must therefore be recognised there, and its retry sent to the kernel as a re-admission.

## Bug Details

### Bug Condition

A run holds a lost update (1.1-1.3), or a history-admitted one (1.4).

### Examples

- **A retry after a restart.** A client sends update `u1`. The workflow's worker is busy, so `u1` is admitted and a speculative task is scheduled. The node restarts. The client's retry of `u1` today returns after 30 seconds, answered as admitted with no outcome, and so does every later retry. After the fix, the retry admits `u1` anew, the worker's next task carries it, and the retry gets the update's result.
- **The empty-task loop.** In the same run, the worker polls after the restart and gets a speculative task that carries nothing. It completes the task empty, and today a new speculative task follows at once, round after round. After the fix, the first command after the restart forgets `u1` and drops the unstarted speculative task, so there is nothing to poll.
- **The advice.** A run has completed 1,795 updates and holds ten lost ones. Its next workflow task today suggests continuing as new, since 1,805 reaches 1,800. After the fix it doesn't.
- **A reset.** A run whose last ten accepted updates followed the reset point is reset, and those ten come back history-admitted. An eleventh update is admitted today, but refused after the fix, as in v1.31.0.

## Expected Behavior

### Preservation Requirements

- Runs whose admitted updates are all held behave as today (3.1).
- Empty speculative completions are still dropped (3.2).
- Retries of accepted, completed and history-admitted updates still join them (3.3).
- States without history-admitted updates encode as today (3.4).
- A reset still reapplies its update events and schedules a workflow task (3.5).

## Root Cause

- Nothing removes a lost id from `admitted_updates`. The pruning of a completion's delivered updates uses the ids the registry sent, and a lost id never is.
- `update_workflow`'s dedupe finds a lost id in the state and waits for it (`crates/tokeira-runtime/src/runtime/query.rs`).
- Both follow-up sites schedule a speculative task whenever `admitted_updates` isn't empty, on the speculative drop path and after a normal completion (`apply_workflow_task_completed`, `crates/tokeira-kernel/src/kernel.rs`).
- The continue-as-new advice counts `admitted_updates.len() + pending_updates.len()`, lost ids included.
- The update limits and the re-admission check count the held updates the lane reports, which leaves out history-admitted updates (`with_held_updates`, `crates/tokeira-runtime/src/lane.rs`).

## Correctness Properties

Property 1: The run keeps only the admitted updates it can deliver

_For any_ run whose admitted updates are any mix of held, lost and history-admitted, and any pending workflow task, the transition of the next command applied to it SHALL hold as admitted exactly its held and history-admitted updates. The pending task SHALL be dropped exactly when it is speculative, unstarted, and no held update is left for it to deliver. Neither change SHALL record an event.

**Validates: Requirements 2.1, 2.3**

Property 2: A lost update's retry is a new update

_For any_ lost update, a client's retry with its id SHALL be admitted, under the update limits, with the retry's request. A workflow task SHALL be scheduled to deliver it when none is pending, and the retry SHALL get the update's outcome.

**Validates: Requirement 2.2**

Property 3: A follow-up task carries something

_For any_ workflow task completion, the system SHALL schedule a speculative task after it exactly when a held update remains that the completed task didn't deliver. A history-admitted update SHALL never cause one.

**Validates: Requirement 2.4**

Property 4: In flight is held, history-admitted and accepted

_For any_ run, the in-flight count that the update limits, a worker's re-admission and the continue-as-new advice read SHALL equal its accepted updates plus its held updates plus its history-admitted updates.

**Validates: Requirement 2.5**

Property 5: History admission is stored

_For any_ run state, decoding its encoding SHALL give back its history-admitted updates and every other field. A state without history-admitted updates SHALL encode to the bytes it encodes to today, and a stored state without their section SHALL decode with none.

**Validates: Requirements 2.6, 3.4**

Property 6: History admission follows the run

_For any_ reset and the commands after it, a run's history-admitted updates SHALL be the updates the reset reapplied, or the replay of its copied history admitted, that it hasn't since accepted, rejected or closed with.

**Validates: Requirement 2.6**

## Fix Implementation

### State (`crates/tokeira-kernel/src/state.rs`, `crates/tokeira-storage/src/codec.rs`)

- `WorkflowState` gains `history_admitted_updates: BTreeSet<String>`, `#[serde(skip)]` like `signal_count`, since the state's postcard layout is positional.
- The kernel adds an id where it records or replays a WorkflowExecutionUpdateAdmitted event: the reset reapply arm of `apply_workflow_task_failed`, and `apply_replayed_event`. At the end of every transition the set keeps only ids still in `admitted_updates`, so accepting, rejecting or closing removes an id without a change at each of those sites.
- A new state-extension section, `HISTORY_ADMITTED_UPDATES_SECTION`, tag 4, holds the set as a postcard `Vec<String>` in ascending order, a layout frozen from its first release. It is written only when the set isn't empty, after the sections before it, so a state without history-admitted updates encodes to today's bytes. A state stored before this change has no section and holds none. An older release drops the section when it rewrites a run, after which the run holds none, as the section rules allow.

### Forgetting lost updates (`crates/tokeira-kernel`, `crates/tokeira-runtime/src/lane.rs`)

- The kernel exposes `forget_lost_updates(state: &mut WorkflowState, held: impl Fn(&str) -> bool)`. It removes from `admitted_updates` each id that isn't history-admitted and for which `held` is false. If the pending workflow task is speculative, unstarted, and no admitted id outside the history-admitted set remains, it removes the task too. It records no event; the change reaches storage in the transition of the command applied next.
- Immediately before `kernel.apply`, the lane calls it on the state it loaded, with `held` asking the `UpdateRegistry` for an entry. This replaces the held-update count `with_held_updates` sets today. The lane still sets `in_flight_request_bytes`, the held requests' sizes, on an update command.
- An admitted id with an entry is never lost: a request's entry is created before its admission, and is removed only once the update leaves `admitted_updates`. So in a process that hasn't restarted, nothing is forgotten (3.1).

### Re-admitting a retry (`crates/tokeira-runtime/src/runtime/query.rs`, `crates/tokeira-kernel`)

- `update_workflow`'s dedupe treats an id that the state holds as admitted, not history-admitted, with no entry in the registry, as lost. It registers the retry's request and submits the update command with `readmit: true`.
- `apply_update` admits an update with `readmit` whose id is admitted and not history-admitted as a new one. It checks the limits with the update itself left out, and schedules a workflow task when none is pending. Any other update for an admitted id is still `DuplicateUpdateId`.
- By the time the command reaches the lane, its own entry exists, so the forget keeps the id, and the re-admission takes it over.

### Follow-up tasks and counting (`crates/tokeira-kernel/src/kernel.rs`)

- Both follow-up sites schedule a speculative task only when an admitted id outside `history_admitted_updates` remains. After the forget, every such id is held, so the task carries something (2.4).
- Once the forget has run, `admitted_updates.len() + pending_updates.len()` is the in-flight count of 2.5. The update limits' admission check and a completion's re-admission slots use it, and the continue-as-new advice already does. `UpdateRequest::held_updates` and the completion request's `held_updates` go.

### Out of scope

- The items in the bugfix's Out of Scope.

## Testing Strategy

### Exploratory Bug Condition Checking

- Before the fix, through the runtime on the in-memory store, with a new runtime over the same store standing in for the restart:
  - the retry of a lost update returns without an outcome;
  - the worker's empty completion of the post-restart speculative task is followed by another speculative task;
  - a run's advice counts its lost updates;
  - after a reset that reapplies ten updates, an eleventh is admitted.

  Each test fails on the current code as 1.1-1.4 predict, and passes after the fix.
- Negative controls, each patched in, run, then reversed so the tree is byte-identical:
  - no forget;
  - the forget dropping history-admitted updates;
  - a started speculative task dropped;
  - a retry without `readmit`;
  - a follow-up scheduled for history-admitted updates;
  - the count leaving out history-admitted updates;
  - the section unwritten, or written for an empty set.

  Each fails the test that covers it.

### Property-Based Tests

- Properties 1, 3, 4 and 6 run in the kernel over generated states and command sequences (`crates/tokeira-kernel/tests/admitted_updates_after_restart.rs`). Each state mixes held, lost and history-admitted updates, with any pending task.
- Property 2 runs through the runtime on the in-memory store, over generated mixes of lost and held updates and retries (`crates/tokeira-runtime/tests/runtime_admitted_updates_after_restart.rs`).
- Property 5 runs in the codec's tests, with the other sections alongside.

### Unit Tests

- The four exploratory cases, kept as regression tests.
- A reset run's history-admitted updates after a reload.
- `forget_lost_updates` on a run with a started speculative task, which it keeps.

### Preservation Checking

- The existing kernel, storage and runtime tests stay green, `signal-update-limits`' included, with their held-update cases rewritten to the run's own count.
