# Run Growth Limits — Bugfix Design

## Overview

The stores check each commit's growth against limits the runtime hands them with the transition: the run's stored History Size, its event count and its state size before the write, and the size of the write's history batch. A refused commit writes nothing and returns a typed error. The lane, and the activity writes that commit directly, then terminate the run through an ordinary terminate command, so the termination has every effect a termination has, and answer the caller `InvalidArgument`. The kernel checks buffered events at the end of every transition, and the runtime truncates an activity's failure before storing it.

## Glossary

- **History Size:** the sum of the run's stored history batch sizes, as `continue-as-new-advice` defines it and both stores keep it (`history_size_bytes`).
- **Growth limits:** the history size (50 MiB), history count (51,200 events), state size (8 MiB) and history batch size (4 MiB) limits, with their warn limits (`common/dynamicconfig/constants.go:138-142, 360-406 @ v1.31.0`).
- **Buffered event limits:** 100 events and 2 MiB (`constants.go:2340-2350 @ v1.31.0`).
- **Stored activity failure limit:** 4 KiB (`limit.mutableStateActivityFailureSize.error`, `constants.go:386-396 @ v1.31.0`).
- **Terminating breach:** a commit refused for history size, count or state size, or for its batch size when it completes a workflow task. The run is terminated. Any other refusal for batch size only fails the write.
- **Measured state:** a run's encoded state less each activity's encoded input.
- **Finishing events:** the events of a write that v1.31.0 numbers only when it finishes the write, after the growth checks (criterion 2.2).

## Bug Details

### Bug Condition

A write that finds or leaves a run over a growth limit (1.1-1.3); events that take a started workflow task's buffer over a limit other than by a signal, or over 2 MiB (1.4); an activity retry after a failure over 4 KiB (1.5).

### Examples

- A workflow receives 30 signals of 2 MiB each. Tokeira keeps 60 MiB of history, and on DSQL each signal's batch fails at the 1 MiB column. In v1.31.0 the 26th signal finds the History Size over 50 MiB: the run is terminated with `Workflow history size exceeds limit.` and the signal call fails with `InvalidArgument`.
- A workflow task completes with three commands of 1.5 MiB each. Tokeira writes a 4.5 MiB batch. v1.31.0 refuses the batch, terminates the run with `Transaction size exceeds limit.`, and answers the worker `InvalidArgument` `transaction size of N bytes exceeds limit of 4194304 bytes`, N being the batch's encoded size.
- 101 activity results arrive while a workflow task is started. Tokeira buffers all of them. v1.31.0 force-closes the task at the 101st.
- With the history count limit at 20, a run has 20 events and a scheduled workflow task. v1.31.0 records the next signal as event 21, since it numbers the signal only when it finishes the write, after the check. The signal after that finds 21 events and terminates the run, whose last events are WorkflowExecutionSignaled (21) and WorkflowExecutionTerminated (22) (`tests/sizelimit_test.go:40-224 @ v1.31.0`).

## Expected Behavior

### Preservation Requirements

- A run within every limit behaves as today (3.1).
- The continue-as-new advice keeps its thresholds (3.2).
- The force-close records what it records today (3.3).
- A reset writes its successor as today (3.4).

## Root Cause

Nothing on the commit path measures growth. Both stores already measure the history batch, to keep the History Size, and the DSQL store encodes the state, but neither compares them with anything (`crates/tokeira-storage/src/dsql/run_repository/commit.rs`, `crates/tokeira-storage/src/memory.rs`). The kernel checks the buffered event count only after a signal (`enforce_buffered_event_limit`, `crates/tokeira-kernel/src/kernel.rs`). The runtime stores an activity's failure whole (`commit_activity_retry`, `crates/tokeira-runtime/src/runtime/activity.rs`). The conformance key registry lists the three hard limits as not enforced.

## Correctness Properties

Property 1: Growth checks match v1.31.0's

_For any_ run and commit, a store SHALL refuse the commit exactly when: the commit leaves an existing run open and the stored History Size is over the history size limit, or the event count, less the commit's finishing events, would be over the history count limit, or the measured state would be over the state size limit, in that order; or, for any commit with events, its encoded batch is over the history batch size limit. A refused commit SHALL write nothing.

**Validates: Requirements 2.1-2.4, 2.6, 2.9**

Property 2: A terminating breach terminates the run as v1.31.0 does

_For any_ run, with or without a started workflow task and buffered events, a terminating breach SHALL leave the run's history as it was before the refused write, followed by WorkflowTaskFailed with `ForceCloseCommand` when an attempt-1 task was started, the buffered events, and WorkflowExecutionTerminated with the breach's reason, details and the `history-service` identity. The caller SHALL get `InvalidArgument` with the breach's message. A batch-size refusal of any other write SHALL leave the run unchanged.

**Validates: Requirements 2.1-2.3, 2.5, 2.6**

Property 3: Buffered events are bounded on every path

_For any_ sequence of externally originated events while a workflow task is started, the task SHALL be force-closed exactly when the buffer first holds more events than the count limit or more than the size limit, whichever kind of event took it there.

**Validates: Requirement 2.7**

Property 4: A stored activity failure fits the limit

_For any_ failure, the activity's stored last failure SHALL be the failure itself when its encoded size is within the limit, and otherwise the server failure `Failure exceeds size limit.`, not marked non-retryable, which fits the limit, with the failure cut down as its cause.

**Validates: Requirement 2.8**

## Fix Implementation

### Limits (`crates/tokeira-kernel`, `crates/tokeira-runtime`)

- `tokeira_kernel::limits` gains v1.31.0's growth, buffered event and stored activity failure values (criterion 2.10).
- The module also defines `RunGrowthLimits`, the four growth limits with the warn limits of the first three, and `RunLimitExceeded`, the typed error a refused commit returns, which names the limit and carries v1.31.0's message. The storage, runtime and edge crates all depend on the kernel, so each can name both.
- The kernel's builder marks where a transition's finishing events begin: at the start of the run of events at its end that are each of a kind v1.31.0 buffers, or written by a finishing step. v1.31.0 buffers every kind but a workflow's start and close, a workflow task's events, the events written for commands, and an update's acceptance and completion (`service/history/historybuilder/event_store.go:263-318 @ v1.31.0`). The finishing steps are a force-close for buffered events, a workflow task scheduled for a Nexus operation's event, and a speculative task's conversion. Any other event resets the mark: v1.31.0 numbers its events in order, so an event before one it numbers while handling the request was numbered then too. `Transition` gains `events_numbered_at_close: u32`, the number of events from the mark on, which the history count check subtracts from the run's last event id. The transitions the runtime builds itself, for a heartbeat, a retry and the pause a workflow rule applies to a retry, write no events, so theirs is zero.
- `Transition` gains `growth_limits: Option<RunGrowthLimits>`. The kernel leaves it empty; the runtime sets it before each commit it makes (the lane's, and the activity writes'). A commit without it isn't checked, which keeps other callers, such as tests, unchanged. `Transition` isn't persisted, and wrappers of the repository pass it through, so the limits reach the store.
- The runtime resolves the limits as it resolves a workflow task's limits (`run_growth.rs`). The Temporal functional harness's build reads its overrides for the same keys, warn keys included; `conformance-config-override` owns that wiring, and production builds use the constants.

### Checks in the stores (`crates/tokeira-storage`)

- One check function, shared by both stores, takes the stored History Size, the transition, the measured state's size and the batch's size, and returns `RunLimitExceeded` for the first breach in criterion 2.4's order, then the batch (criterion 2.6). The history, count and state checks apply only when the transition has an existing run (`expected_seq` above zero) and leaves it open.
- **DSQL** (`run_repository/commit.rs`): `write_transition` encodes the state and the batch first, measures them, and runs the check. On a breach it returns the error before its first write, and the transaction rolls back. `insert_workflow_hot` takes the encoded state rather than encoding it again.
- **In memory** (`memory.rs`): `commit_transition` runs the check before its first change, when the transition carries limits, measuring the state's encoded size as DSQL stores it without encoding it (`codec::workflow_state_encoded_len`).
- The measured state subtracts each activity's input, measured with the codec's encoding of `Payloads`, from the encoded state (criterion 2.9; `codec::measured_state_len`).
- Above a warn limit, the check logs, at most once a second for each limit, as v1.31.0 logs through its throttled logger (criterion 2.10).
- Nothing retries the error: neither store's commit retries an error, and the lane retries only a conflict.

### Terminating the run (`crates/tokeira-runtime`)

- **The lane** (`lane.rs`): when a command's commit fails with a terminating breach, the lane applies `Command::Terminate` to the run as stored, in the same activation, with the breach's reason, details for a batch-size breach, the identity `history-service` and a request id derived from the run. The terminate's commit leaves the run closed, so only its batch is checked, as v1.31.0 checks its own terminate write. The lane treats that commit as the command it committed, running every post-commit step for it, and replies to the original caller with the breach's error. If the terminate fails, the lane logs it and still replies with the breach's error.
- A batch-size breach terminates only for `WorkflowTaskCompleted`, `WorkflowTaskCompletedWithRetry` and `WorkflowTaskCompletedWithCron`; any other command gets the error alone.
- **The activity writes that commit directly** (`runtime/activity.rs`: heartbeat, start, forced start, retry, and the pause a workflow rule applies to a retry) terminate the run with the same command through its lane on a breach of history size, count or state size, and return the breach's error (`terminate_on_growth_breach`). The retry and the rule's pause commit from free functions, which reach the lanes through `ActivityRetryDeps`, so the activity timeout processor and the recovery pass terminate as the runtime's handlers do. The worker's heartbeat, forced start and failure calls are answered with the error; a start inside a worker's poll drops the task and goes on, as for a task whose run has closed (criterion 2.4).
- **The edge** (`crates/tokeira-edge/src/errors.rs`): `RunLimitExceeded` maps to `InvalidArgument` with its message.

### Buffered events (`crates/tokeira-kernel/src/kernel.rs`)

- `enforce_buffered_event_limit` runs once at the end of every transition, rather than only after a signal, and also force-closes the task when the buffered events are over the size limit (criterion 2.7). What it records is unchanged (criterion 3.3).
- An activity's start is the exception: the runtime commits it outside the lane, which couldn't deliver the workflow task a force-close schedules, so the run's next transition checks what the start buffered. v1.31.0's own start writes no event, since its activities always carry a retry policy; only Tokeira's start of an activity without one, or by id, buffers an event.
- A buffered event's size is the protobuf-encoded size of the payloads it carries: inputs, results, details, failures and headers (`payload_size.rs`). The kernel holds domain events, not protos, and payloads are what make events large. This errs low by each event's other fields, which v1.31.0 counts too: tens of bytes to a few hundred an event, so at most about 1% of the 2 MiB limit across 100 events.

### Stored activity failures (`crates/tokeira-proto`, `crates/tokeira-runtime`)

- The server failure builder and the truncation move from the edge's `grpc/payload_limits.rs` to `tokeira_proto::failure_limits`, which both the edge and the runtime use. The truncation is rewritten to cut a failure down by its encoded size, so a replacement always fits its limit (`payload-admission-limits` Property 2). The builder takes v1.31.0's non-retryable flag, which the edge's callers set and the stored activity failure clears.
- `commit_activity_retry` decodes the failure it is given (`temporal/failure+proto`) and replaces one over the stored activity failure limit before storing it (`run_growth::stored_activity_failure`): a server failure `Failure exceeds size limit.`, not marked non-retryable, whose cause is the original cut down so that the whole fits the limit (criterion 2.8; `failure_limits::oversized_failure`). Workflow rules read the stored failure, as in v1.31.0. The final ActivityTaskFailed event keeps the worker's failure.

### Functional harness wiring

- The harness's key registry (`crates/tokeira-conformance/src/lib.rs`) marks `limit.historySize.error`, `limit.historyCount.error` and `limit.mutableStateSize.error` `Wired`, adds their warn keys, `system.transactionSizeLimit` and `limit.mutableStateActivityFailureSize.error` as `Wired`, and adds `history.maximumBufferedEventsSizeInBytes` as `KernelExcluded`, like the buffered event count. The compatibility ledger records the same, and `docs/conformance/v1.31.0/temporal-configuration.md` is regenerated. The warn keys are wired because the logs consult them and v1.31.0's size-limit tests set them with the error keys (`tests/sizelimit_test.go:43-51, 326-327, 460-461 @ v1.31.0`). This serves only the Temporal functional harness, as `conformance-config-override`'s key table records. It is not a Tokeira setting.

### Other specs

- `continue-as-new-advice` put the hard history limits out of scope as a separate decision; it now points here.
- `kernel-event-buffering` records that only a signal triggers the buffered event limit, and that the state size limit isn't implemented; both now point here.
- `conformance-config-override`'s key table lists the hard limits as not enforced; it now records the new consult sites.
- `payload-admission-limits` and `workflow-task-command-limits` called a run's growth limits a separate change; they now point here.

### Out of scope

- The state check on a write that closes the run, DSQL's column and row limits, a reset's copied history, and what timers and scanners do with a refused write (bugfix Out of Scope).
- The limits on a run's signals and updates.

## Testing Strategy

### Exploratory Bug Condition Checking

- Negative controls: with each check removed or moved, the test that covers it fails. Sixteen were run: no history size check; the count including the finishing events; first writes checked; the state with its activities' inputs; finishing steps unmarked; no buffered size limit; the speculative conversion, and the Nexus operation's task, numbered while handling the request; an activity's start checking the buffered limits; the lane setting no limits; the lane not terminating; the activity writes not terminating; a refused start returning the error; any batch terminating; a failure stored whole; and a breach answered as an internal error. The rewritten truncation has seven more, recorded in `payload-admission-limits`.

### Property-Based Tests

- Property 1 over the shared check function, with generated History Sizes, event counts, finishing events, state and batch sizes on both sides of each limit, and through the in-memory store, showing a refused commit writes nothing (`memory.rs` tests, `run_growth`).
- Property 2 through the engine's in-process gRPC endpoint on the in-memory store, at v1.31.0's limits (`crates/tokeira-engine/tests/run_growth_limits.rs`): signals of 2 MiB until Describe's History Size passes 50 MiB, with no task started; heartbeat details of 2 MiB on up to five activities until one would take the state past 8 MiB, with a task started and a signal buffered; and a workflow task completion whose batch passes 4 MiB with the signal it flushes, since gRPC caps a request at 4 MiB. The runtime's tests cover a completion over the limit on its own, and the count (`crates/tokeira-runtime/tests/runtime_run_growth.rs`).
- Property 3 in the kernel, over generated sequences of signals and activity results of generated sizes (`crates/tokeira-kernel/tests/run_growth_limits.rs`).
- Property 4 over generated failures (`run_growth.rs` tests): a failure within the limit is stored as it is, and one over it is replaced by the server failure, which fits the limit. How the cause is cut down is `payload-admission-limits`' Property 2.

### Unit Tests

- The finishing events the kernel marks, against v1.31.0's numbering: a signal while a workflow task is scheduled, a signal and an activity result with none pending, a Nexus operation's result, a force-close for buffered events, and a speculative task's conversion.
- The history count limit through the runtime on a run seeded at the limit: a signal while its workflow task is scheduled succeeds, and the next one terminates the run, as in v1.31.0's `TestTerminateWorkflowCausedByHistoryCountLimit`.
- An activity start and a retry on a run seeded over the History Size limit: the run is terminated through its lane, the poll hands out no task, and the failure call is answered with the breach.
- An activity's start that buffers the 101st event leaves the force-close to the run's next transition.
- The measured state against the encoded state of the same run with its activities' inputs emptied.
- A run's first write with a batch over 4 MiB is refused and creates nothing.
- Each breach's message and reason.

### Preservation Checking

- The existing kernel, runtime, storage, edge and engine tests stay green, the buffered event tests included.
