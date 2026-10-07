# Signal and Update Limits — Bugfix Design

## Overview

The kernel counts a run's signals and refuses a signal at the limit. It refuses an update for the in-flight count, the total and the in-flight payload, in v1.31.0's order, from counts it already keeps and request sizes the runtime hands it. The stores keep the signal count in the state extension of the run's stored state. The runtime and the publisher turn a refused signal into what each caller gets from v1.31.0, and the edge answers with v1.31.0's codes.

## Glossary

- **Signal limit:** `history.maximumSignalsPerExecution`, 10,000 (`common/dynamicconfig/constants.go:2351-2355 @ v1.31.0`).
- **Signal count:** the number of WorkflowExecutionSignaled events a run has recorded (criterion 2.6).
- **Update limits:** 10 updates in flight (`history.maxInFlightUpdates`), 20 MiB of in-flight update requests (`history.maxInFlightUpdatePayloads`) and 2,000 updates in all (`history.maxTotalUpdates`) (`constants.go:2289-2303 @ v1.31.0`).
- **In-flight update:** an update admitted and neither completed nor rejected, whether or not the workflow has accepted it. In the kernel, the run's `admitted_updates` and `pending_updates`.
- **Completed update count:** `completed_update_count`, the updates the workflow completed after accepting them.
- **Request size:** an update request's protobuf-encoded size as a `temporal.api.update.v1.Request`.
- **In-flight request bytes:** the request sizes of a run's admitted updates that the workflow hasn't accepted or rejected.

## How this maps onto Tokeira's architecture

v1.31.0 checks these limits in its history service, under the run's lock, against the run's mutable state and its in-memory update registry. Tokeira is built differently in five ways that matter here, and the fix follows them.

1. **Request ids are deduplicated at commit.** v1.31.0 keeps a run's signal request ids in its mutable state and checks them before the count. Tokeira's stores check request ids when they commit, after the kernel has applied the command. So when the kernel refuses a signal for the count, the caller asks the store whether the run already applied the request id, and answers a duplicate if it did. SignalWithStart's signal to a running run skips this, since v1.31.0 checks its count first.
2. **The closing check runs in the runtime.** Tokeira refuses a signal to a closing run in the runtime, before the kernel sees it (`signal_workflow`). v1.31.0 checks the count first. So the runtime's check stands aside when the run it has loaded is at the signal limit, and the kernel's refusal answers.
3. **The publisher delivers a signal from another workflow.** After the sender commits, the publisher submits the signal to the target's lane and resolves the sender with `ExternalSignalResolved`. The target's refusal reaches the publisher as an error, which it turns into the failure's cause.
4. **Admitted updates are part of the run's state.** v1.31.0 keeps admitted updates in its in-memory registry, and rebuilds accepted and completed updates from mutable state when it loads a run. Tokeira keeps admitted and accepted update ids and the completed count in the run's state, and the requests of admitted updates in the owner's in-memory `UpdateRegistry`. The in-flight and total checks count from the state, as the continue-as-new advice does. The in-flight request bytes come from the registry, for the admitted updates of the state the kernel is about to check.
5. **A reset run is built in two steps.** The store builds the new run by replaying the copied history through the kernel (`replay_history_prefix`), and the kernel applies the reapplied events when it fails the fork's workflow task. v1.31.0 rebuilds the new run's mutable state from the copied events, then reapplies. Each step adds to the new run's counts.

## Bug Details

### Bug Condition

A signal to a running run whose signal count has reached 10,000 (1.1, 1.2); an update with a new id to a running run with 10 updates in flight, 2,000 updates in flight and completed, or in-flight request bytes that would reach 20 MiB with its own (1.3-1.5); a completion that accepts or rejects an update the run doesn't hold while the run is at the total limit (1.6); a reset (1.7).

### Examples

- A workflow receives its 10,001st signal. Tokeira records it. v1.31.0 answers `InvalidArgument` "exceeded workflow execution limit for signal events".
- A client retries a signal whose first attempt was the run's 10,000th and succeeded, but whose answer was lost. v1.31.0 answers the retry with success, since the run already applied its request id.
- A workflow signals a run at its limit. v1.31.0 records SignalExternalWorkflowExecutionFailed with `SIGNAL_COUNT_LIMIT_EXCEEDED` on the sender. Tokeira records the signal on the target.
- A workflow whose worker is down receives eleven updates. v1.31.0 refuses the eleventh with `ResourceExhausted`. Tokeira admits all eleven.
- With the total limit at 1, a run completes one update, and an update-with-start sends it a second. v1.31.0 answers `MultiOperationExecution`, with "Operation was aborted." for the start and the total limit's `FailedPrecondition` for the update (`TestReturnUpdateRateLimitError`, `tests/update_workflow_test.go:5767-5804 @ v1.31.0`).
- A run that has completed 1,700 updates is reset. v1.31.0's new run counts 1,700 completed updates, so it suggests continue-as-new after 100 more. Tokeira's counts none.

## Expected Behavior

### Preservation Requirements

- A run within every limit behaves as today (3.1).
- The continue-as-new advice keeps its threshold (3.2).
- A rejected update leaves no trace (3.3).
- A signal during a started workflow task is buffered (3.4).
- A state without signals encodes as today (3.5).

## Root Cause

- `WorkflowState` has no signal count, and the kernel's `apply_signal` checks none (`crates/tokeira-kernel/src/kernel.rs`).
- The kernel's `apply_update` checks that the run is open and not paused and that the update id is new. It keeps the in-flight and completed counts the limits need, but only the continue-as-new advice reads them.
- The kernel's replay of a copied history (`apply_replayed_event`) counts no completed updates.
- When a target refuses a signal, the publisher records the refusal's text as the failure's cause (`handle_signal_external_workflow`, `crates/tokeira-runtime/src/publisher.rs`), which the history serializer renders as an unspecified cause (`signal_external_workflow_failed_cause_i32`, `crates/tokeira-edge/src/translate/history_serializer.rs`).
- The edge has no `ResourceExhausted` answer with the cause `CONCURRENT_LIMIT` (`crates/tokeira-edge/src/grpc/errors.rs`).
- The conformance key registry lists `history.maxTotalUpdates` for the advice alone, and neither `history.maxInFlightUpdates` nor `history.maximumSignalsPerExecution` (`crates/tokeira-conformance/src/lib.rs`).

## Correctness Properties

Property 1: The signal count is the run's recorded signals

_For any_ sequence of signals to a run, by any path, with signals buffered and flushed, duplicates, continue-as-new and reset, the run's signal count SHALL equal the number of WorkflowExecutionSignaled events it has recorded, those of its copied history and its reapplied signals included, each counted once.

**Validates: Requirements 2.4, 2.6, 2.7**

Property 2: A signal is refused exactly at the limit

_For any_ running run and any signal that neither starts the run nor is reapplied by a reset: when the run's signal count has reached the signal limit, the system SHALL record nothing on the run, and SHALL answer success when the signal is a SignalWorkflowExecution, or a signal from another workflow, whose request id the run has already applied, and 2.1's error otherwise, whether or not the run is closing. When the count is below the limit, the system SHALL handle the signal as today. A run whose signal to another run was answered with that error SHALL record SignalExternalWorkflowExecutionFailed with `SIGNAL_COUNT_LIMIT_EXCEEDED`, and one whose signal was answered with success SHALL record ExternalWorkflowExecutionSignaled.

**Validates: Requirements 2.1-2.5**

Property 3: The stored count round-trips

_For any_ state, decoding its encoding SHALL give back its signal count and every other field. A state with no signals SHALL encode to the same bytes as before this change, and a stored state without the count's section SHALL decode with a count of zero.

**Validates: Requirements 2.8, 3.5**

Property 4: Update admission matches v1.31.0's

_For any_ running run, in-flight and completed counts, in-flight request bytes, request size and limits, the kernel SHALL refuse an update with a new id exactly when one of these holds, checked in this order: its in-flight updates number the in-flight limit or more; its updates in flight and completed number the total limit or more; its in-flight request bytes and the request's size reach the payload limit. A limit of 0 disables its check. The answer SHALL be the first that holds. Otherwise the kernel SHALL admit the update, and the run's in-flight count SHALL grow by one.

**Validates: Requirements 2.9-2.12**

Property 5: Updates are counted as v1.31.0 counts them

_For any_ sequence of admissions, acceptances, completions with a result or a failure, rejections and resets, an update SHALL be in flight from its admission until it is completed or rejected; the completed count SHALL grow by one for each completion after an acceptance, and never for a rejection; and a reset run's completed count SHALL be the number of completions in its copied history.

**Validates: Requirements 2.7, 2.9, 2.10**

Property 6: Resurrecting an update respects the total limit

_For any_ run and any completion that accepts or rejects an update the run has neither admitted nor accepted, the completion SHALL fail with 2.10's error, recording nothing, exactly when the run's updates in flight and completed number the total limit or more, whatever its in-flight count and request bytes.

**Validates: Requirement 2.13**

## Fix Implementation

### Limits (`crates/tokeira-kernel/src/limits.rs`)

- `MAXIMUM_SIGNALS_PER_EXECUTION` (10,000), and `UpdateLimits`: the in-flight count, in-flight payload and total limits, with v1.31.0's values as its default (criterion 2.14). A limit of 0 disables its check, as in v1.31.0, but only the Temporal functional harness's build can set one.
- `UpdateLimitExceeded`, which names the limit and carries v1.31.0's message for it, and two new rejects, `Reject::SignalLimitExceeded` and `Reject::UpdateLimitExceeded`. The runtime and the edge depend on the kernel, so both can name them.

### Counting signals (`crates/tokeira-kernel`)

- `WorkflowState` gains `signal_count: u64`, `#[serde(skip)]` like `ActivityState::last_heartbeat_at`. Postcard's layout is positional, so a new field would make every stored state undecodable (`codec.rs` module docs). The stores keep it in the state extension.
- The kernel counts a signal where it admits one: in `apply_signal`, in `apply_signal_with_start`, and for each signal a reset reapplies in `apply_workflow_task_failed`. Flushing a buffered signal counts nothing (criterion 2.6). `replay_history_prefix` counts each WorkflowExecutionSignaled event of the copied history (criterion 2.7). Every other new run starts at zero.
- `apply_signal` refuses a signal with `Reject::SignalLimitExceeded` when the count has reached the limit, after it checks that the run is open (criterion 2.1). `apply_signal_with_start` and a reset's reapplied signals aren't checked (criteria 2.4, 2.7). The kernel reads the limit as a constant, since no corpus test overrides it.

### Storing the count (`crates/tokeira-storage/src/codec.rs`)

- A new state-extension section, `SIGNAL_COUNT_SECTION`, tag 2, holds the count as a postcard `u64`, a layout frozen from its first release. The encoder writes it only when the count isn't zero, after the heartbeat section, so that tags stay in ascending order and a state with no signals encodes to the bytes it encodes to today (criterion 3.5). The decoder applies it; a second tag-2 section is a malformed extension, as a repeated tag is today.
- A state stored before this change has no section, and its count reads zero (criterion 2.8). An older release reading a newer state ignores the section and drops it when it rewrites the run, after which the count restarts from zero. That is how a reader without the count already behaves, as the section rules require.
- Both stores encode a run's state with `encode_workflow_state`, and the in-memory store's snapshot carries each run's state extension, so neither store needs other changes. No table changes.

### Signal paths (`crates/tokeira-runtime`)

- **SignalWorkflowExecution and batch signals** (`signal_workflow`, `runtime/lifecycle.rs`): when the commit fails with `SignalLimitExceeded` and the signal carries a request id, the runtime looks the id up among the requests the run has applied (`RunRepository::lookup_request_dedupe`, with the run's id). If the run applied it, the call answers as a duplicate, `CommitResult::Duplicate` (criterion 2.2). Otherwise the refusal stands.
- **The closing check**: `signal_workflow` refuses a signal while the run is closing before it submits the signal. When the run it loads for that check is at the signal limit, it submits the signal instead, so that the kernel's refusal answers (criterion 2.3).
- **SignalWithStartWorkflowExecution to a running run** (`signal_with_start_workflow`): its signal takes the same path without the request id lookup, so that a duplicate at the limit is refused (criterion 2.2).
- **Signals from other workflows** (`handle_signal_external_workflow`, `publisher.rs`): when the target's lane refuses the signal with `SignalLimitExceeded`, the publisher looks up the delivery's request id on the target run in the same way. If the target applied it, the sender is resolved as signaled; otherwise it is resolved with the cause `SIGNAL_COUNT_LIMIT_EXCEEDED`, which the history serializer already renders as that enum value (criterion 2.5).

### Update limits (`crates/tokeira-kernel`, `crates/tokeira-runtime`, `crates/tokeira-edge`)

- **The request's size.** The edge measures each update request's encoded size, `prost::Message::encoded_len` of the `Request` it received, which is the size v1.31.0's `req.Size()` returns, and passes it to the runtime with the update. This covers UpdateWorkflowExecution and the update in ExecuteMultiOperation.
- **The registry.** `UpdateRegistryEntry` keeps the request's size. A new registry method sums the sizes of a run's entries whose ids are in a given admitted set, other than one given id.
- **The command.** `UpdateRequest` gains `limits: UpdateLimits`, `request_bytes` and `in_flight_request_bytes`. The runtime resolves the limits as it resolves a workflow task's limits: the Temporal functional harness's build reads its overrides of `history.maxInFlightUpdates` and `history.maxTotalUpdates`, and production builds use the constants.
- **The lane sets the in-flight request bytes.** Immediately before the kernel applies an update command, the lane, which has just loaded the run and holds the runtime's `UpdateRegistry`, sets `in_flight_request_bytes` to the sizes of the registry's entries for the admitted updates of that state, other than the command's own. A figure computed before the command reached the lane could miss an update admitted in between, or count one never admitted. An admitted update whose entry a restart lost counts zero bytes, as v1.31.0 counts an update it has forgotten.
- **The checks** (`apply_update`): once the run is open and not paused and the id is new, in v1.31.0's order (criterion 2.12): the in-flight count, `admitted_updates.len() + pending_updates.len()`, against the in-flight limit; that count plus `completed_update_count` against the total limit; then `in_flight_request_bytes + request_bytes` against the payload limit. Each refuses with `Reject::UpdateLimitExceeded`. On that error the runtime removes the update's registry entry and answers the caller, as it does for any refused update. The runtime's dedupe of an update id the run already knows comes first and is unchanged. `apply_start_and_update` starts a run with no updates, which no limit refuses at any value the harness sets.
- **Resurrection** (`apply_workflow_task_completed`): the arms that accept or reject an update the run has neither admitted nor accepted check the total first, against the total limit, which `WorkflowTaskCompletionLimits` now carries, and refuse the completion with `Reject::UpdateLimitExceeded` (criterion 2.13). The completion records nothing, like any completion the kernel refuses, and the runtime returns the error to the worker.
- **Counting after a reset** (`apply_replayed_event`): each WorkflowExecutionUpdateCompleted event of the copied history counts as a completed update (criterion 2.7). The in-flight sets are rebuilt as today. The continue-as-new advice reads the same count (criterion 3.2).

### Answers (`crates/tokeira-edge`)

- `Reject::SignalLimitExceeded` maps to `InvalidArgument` with 2.1's message (criterion 2.1). A batch operation records it against the run as it records any failed signal.
- `Reject::UpdateLimitExceeded` maps to `ResourceExhausted`, with a `ResourceExhaustedFailure` detail of cause `CONCURRENT_LIMIT` and scope `NAMESPACE`, for the in-flight count and payload limits, and to `FailedPrecondition` for the total limit (criteria 2.9-2.11). The status builder that answers `ErrWorkflowClosing` with cause `BUSY_WORKFLOW` takes the cause as an argument. ExecuteMultiOperation already carries each operation's status with its details (`multi_operation_failure_to_status`), so the update's refusal keeps its detail there (criterion 2.12).

### Functional harness wiring

- The harness's key registry (`crates/tokeira-conformance/src/lib.rs`) adds `history.maxInFlightUpdates` as `Wired`, and `history.maximumSignalsPerExecution` as `KernelExcluded`, like the buffered event count. `history.maxTotalUpdates` stays `Wired`, and now sets the total limit as well as the advice threshold. The compatibility ledger records the same, with `history.maxInFlightUpdatePayloads` as a pinned constant, and `docs/conformance/v1.31.0/temporal-configuration.md` is regenerated from it. This serves only the Temporal functional harness, as `conformance-config-override`'s key table records. It is not a Tokeira setting.
- With both update keys wired, a conformance build can run `TestReturnUpdateRateLimitError` and `TestReturnUpdateInFlightLimitError` (`tests/update_workflow_test.go:5767-5858 @ v1.31.0`), which the fork's skip registry lists as needing an override.

### Other specs

- `run-growth-limits`, `payload-admission-limits` and `workflow-task-command-limits` called the limits on a run's signals and updates a separate change; they now point here.
- `continue-as-new-advice` put `history.maxTotalUpdates` enforcement out of scope; it now points here, and its rebuild of a reset run counts completed updates.
- `conformance-config-override`'s key table records the update keys' consult sites and the signal limit as kernel-excluded.

### Out of scope

- The items in the bugfix's Out of Scope.

## Testing Strategy

### Exploratory Bug Condition Checking

- Negative controls: with each check removed, moved or miscounted, the test that covers it fails. They cover: no signal check; a flushed signal counted again; a reset's copied or reapplied signals left uncounted; the request id lookup missing, or applied to SignalWithStart; the closing check before the count; the publisher's refusal left unmapped; the count's section unwritten, or written for a zero count; accepted updates left out of the in-flight count; a rejection counted as a completion; the checks out of order; the requests of accepted updates counted in the in-flight bytes; the bytes summed over registry entries the state hasn't admitted; the resurrection unchecked; the replay counting no completions; and the edge's answers without their detail.

### Property-Based Tests

- Properties 1 and 2 in the kernel, over generated sequences of signals, flushes and resets on runs seeded near the limit (`crates/tokeira-kernel/tests/signal_update_limits.rs`), and Property 2's answers through the runtime on the in-memory store, for client signals, SignalWithStart and signals from other workflows, with request ids repeated at random.
- Property 3 over generated states in the codec's tests (`crates/tokeira-storage/src/codec.rs`), with heartbeats and unknown sections alongside.
- Properties 4 to 6 in the kernel, over generated counts, sizes and limits, zeros included, and generated update lifecycles.

### Unit Tests

- Each answer's code, message and detail, through the engine's in-process gRPC endpoint on the in-memory store (`crates/tokeira-engine/tests/signal_update_limits.rs`): a run that has recorded 10,000 signals refuses the next client signal, SignalWithStart and signal from another workflow; a run whose worker is idle refuses the eleventh of eleven updates; six updates of nearly 4 MiB each, the sixth refused, since gRPC caps a request at 4 MiB; and an update-with-start to a running run at the in-flight limit, refused with the update's detail.
- The total limit and a resurrection through the runtime, on a run seeded with 1,999 completed updates.
- A run that signal-with-start starts counts one signal, and a continue-as-new successor counts none.
- A reset run's counts, from its copied history and its reapplied signals.
- A run stored without the section counts from zero.

### Preservation Checking

- The existing kernel, runtime, storage, edge and engine tests stay green, the codec's tests that a state without an extension encodes as before included.
