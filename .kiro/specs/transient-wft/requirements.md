# Requirements Document: Transient Workflow-Task Model (Kernel + Runtime + Edge)

## Introduction

This spec adopts Temporal's **transient workflow-task model** into `tokeira-kernel` and the derived
runtime/edge paths. It was raised by the functional-conformance drive
([docs/HANDOVER-transient-wft.md](../../../docs/HANDOVER-transient-wft.md)) as the load-bearing gap
behind the remaining `TestWorkflowTaskTestSuite` failures (Tier 1.2).

A continuously-failing workflow task is **transient**: at `attempt > 1` its Scheduled/Started/Failed
events are **not persisted** — they live only in mutable state with *virtual* event ids beyond
`last_event_id`, and they materialize into history only when an attempt finally succeeds (or when new
events force the task back to a normal, attempt-1 task). Tokeira's current model — Feature 2
(`kernel-wft-failure-timeout`) — emits **one history event per attempt** and re-dispatches the same
pending task. Adopting the transient model **reverses** that documented decision and changes observable
history for every WFT-retry path.

This is a two-feature spec (the third raised item is deliberately out of scope — see below):

- **Item A — buffered-flush-resets-retry** (kernel, small): when a WFT-failed close flushes buffered
  events, the next task is reset to a *normal* attempt-1 task with a real `WorkflowTaskScheduled`.
- **Item B — transient-WFT model** (kernel + runtime/edge, large): event suppression at `attempt > 1`,
  virtual id assignment, the transient→normal conversion rules, transient-aware force-close, late
  materialization on success, and poll/GetHistory synthesis of the unpersisted suffix.

### Ground truth (v1.31.0, agent-verified)

- **Transient = `attempt > 1`:** `MutableStateImpl.IsTransientWorkflowTask()` returns
  `WorkflowTaskAttempt > 1` (`service/history/workflow/mutable_state_impl.go:2250 @ v1.31.0`).
- **Transient failure writes no event:** `AddWorkflowTaskFailedEvent` is emitted only when
  `!IsTransientWorkflowTask()` (`workflow_task_state_machine.go:892 @ v1.31.0`).
- **Transient scheduling persists no Scheduled event:**
  `createWorkflowTaskScheduledEvent = !IsTransientWorkflowTask() && !speculative`; otherwise
  `scheduledEventID = GetNextEventID()` (virtual) (`workflow_task_state_machine.go:326,368-379 @ v1.31.0`).
- **Buffered/new events at scheduling reset to normal (Item A):** `if HasBufferedEvents() { attempt = 1;
  type = NORMAL; createWorkflowTaskScheduledEvent = true; flush }`
  (`workflow_task_state_machine.go:329-334 @ v1.31.0`). A failover during a transient WFT likewise resets
  to attempt 1 (`:346-350`).
- **Transient start persists no Started event:** `startedEventID = scheduledEventID + 1` (virtual)
  (`workflow_task_state_machine.go:480 @ v1.31.0`).
- **Terminate force-close of a transient writes nothing:** "Failing transient WT doesn't create any
  events at all and wtFailedEvent is nil" (`service/history/workflow/util.go:117 @ v1.31.0`).
- **Poll/GetHistory synthesis:** `GetTransientWorkflowTaskInfo` (`mutable_state_impl.go:1189-1250`),
  `recordworkflowtaskstarted/api.go:430`, and `getworkflowexecutionhistory` `appendTransientTasks`
  (`get_history_util.go`) synthesize the unpersisted Scheduled/Started suffix for the worker and clients.

## Architectural decision required (blocking) — Requirement 0

Adopting the transient model reverses the Feature-2 per-attempt-event decision
(`.kiro/specs/kernel-wft-failure-timeout/design.md:5`) and changes observable history for every
WFT-retry path, so it is an **Architectural** change (AGENTS classification) requiring **spec update AND
explicit approval**.

> **ACCEPTED (2026-07-03, owner).** The transient-WFT model is adopted; the Feature-2 per-attempt-event
> design note is amended by this spec. Items A and B may proceed. The heartbeat-timeout leaf (raised item
> C) is **out of scope** (see below). The Tier-1.2 leaves stay classified skips until each phase flips
> them.

## Glossary

- **Kernel:** the pure deterministic state machine (`tokeira-kernel`) — no I/O, async, storage, metrics
  (AGENTS §2). Event suppression, virtual-id assignment, and conversion are pure state-machine logic.
- **Transient WFT:** a `PendingWorkflowTask` at `attempt > 1`. Its Scheduled/Started events are virtual
  (ids ≥ `last_event_id + 1`) and unpersisted; its failures/timeouts persist nothing.
- **Virtual event id:** an event id assigned beyond `last_event_id` for a transient task's
  Scheduled/Started, used for the worker's task token and history synthesis, never written to history
  until materialization.
- **Materialization:** on successful completion (or transient→normal conversion), the previously-virtual
  Scheduled/Started events are written to history contiguously before the completion event.
- **Transient→normal conversion:** a transient task becomes a normal attempt-1 task (with real
  Scheduled, and at start time real Scheduled+Started) when buffered/new events appear, because those
  events invalidate the frozen virtual suffix.
- **Synthesis:** poll responses and CLOSE-inclusive GetHistory reads append the unpersisted virtual
  Scheduled/Started suffix so the worker/client see a complete history.

## Requirements

---

## Requirement 0: Architectural Decision — Adopt the Transient-WFT Model (ACCEPTED)

**User Story:** As the Tokeira owner, I want the reversal of the Feature-2 per-attempt-event model
recorded explicitly with its blast radius, so that adopting Temporal's transient model is a deliberate
accepted decision and not a silent history change.

#### Acceptance Criteria

1. THE decision SHALL record that a WFT with `attempt > 1` is transient: its Scheduled/Started events are
   virtual and unpersisted, and its failures/timeouts/force-closes persist no history, superseding the
   per-attempt-event note at `.kiro/specs/kernel-wft-failure-timeout/design.md:5`.
2. THE decision record SHALL state the blast radius: observable history changes for every WFT-retry path
   (attempt>1 events no longer appear; the frozen history during retries is
   `[…, WorkflowTaskScheduled, WorkflowTaskStarted, WorkflowTaskFailed]` from attempt 1 only; events
   materialize late on success), plus the transient→normal conversions and transient-aware force-close.
3. WHEN accepted, THE Feature-2 design note and the kernel architecture doc SHALL be amended to describe
   the transient model (documentation is part of the deliverable, AGENTS §9).
4. UNTIL each phase lands, THE conformance leaves it unblocks SHALL remain classified skips with a cited
   reason; they flip to required-pass per phase (Item A leaf on Phase A; the three Item B leaves on
   Phase B).
5. **Status: ACCEPTED (2026-07-03, owner).**

---

## Item A — Buffered-Flush-Resets-Retry

### Requirement A.1: WFT-Failed Flush Schedules a Fresh Normal Task

**User Story:** As an SDK client, I want a workflow task that fails after events buffered during it to be
followed by a fresh normal workflow task, so that the buffered events are delivered on an attempt-1 task
with a real `WorkflowTaskScheduled`, matching v1.31.0.

Live failure this fixes: `…AfterRegularWorkflowTaskStartedAndFailWorkflowTask` — history length 5 vs
expected 6, missing `6 WorkflowTaskScheduled` after `4 WorkflowTaskFailed, 5 WorkflowExecutionSignaled`.

#### Acceptance Criteria

1. WHEN `apply_workflow_task_failed` flushes one or more buffered events, THE Kernel SHALL reset
   `workflow_task_attempt = 1`, drop the failed pending WFT, and schedule a **fresh** WFT that emits a
   real `WorkflowTaskScheduled` event with a new `logical_seq` and an `EnqueueWorkflowTask` dispatch.
   (`workflow_task_state_machine.go:329-334 @ v1.31.0`.)
2. WHEN the WFT-failed close flushes **no** buffered events, THE Kernel SHALL preserve today's behavior
   (the retry path of Item B applies — a transient task with no new Scheduled event).
3. THE fresh scheduled task SHALL be a normal (non-transient) task — `attempt = 1` — so its subsequent
   Scheduled/Started events are real, not virtual.

---

## Item B — Transient-WFT Model

### Requirement B.1: Transient Classification and Virtual Scheduled Id

**User Story:** As a Tokeira developer, I want a WFT at `attempt > 1` treated as transient with a virtual
Scheduled id, so that retries do not pollute history.

#### Acceptance Criteria

1. A `PendingWorkflowTask` SHALL be *transient* iff `workflow_task_attempt > 1`
   (`mutable_state_impl.go:2250 @ v1.31.0`).
2. WHEN a transient WFT is scheduled (a retry after a fail/timeout with no new events), THE Kernel SHALL
   NOT emit a `WorkflowTaskScheduled` history event; the task's `scheduled_event_id` SHALL be the virtual
   `last_event_id + 1`. (`workflow_task_state_machine.go:326,368-379 @ v1.31.0`.)
3. `last_event_id` SHALL NOT advance for a transient scheduling (no event is written).

### Requirement B.2: Virtual Started Id, No Persisted Started Event

**User Story:** As a Tokeira developer, I want a transient WFT's start to persist no event and use a
virtual started id, so that the worker's task token is consistent while history stays frozen.

#### Acceptance Criteria

1. WHEN a transient WFT is started, THE Kernel SHALL NOT emit a `WorkflowTaskStarted` history event; the
   task's `started_event_id` SHALL be the virtual `scheduled_event_id + 1`
   (`workflow_task_state_machine.go:480 @ v1.31.0`).
2. `last_event_id` SHALL NOT advance for a transient start.

### Requirement B.3: Transient Fail/Timeout Persist Nothing

**User Story:** As a Tokeira developer, I want transient failures/timeouts to persist no event, so that a
workflow failing its WFT N times keeps a history frozen at the attempt-1 failure.

#### Acceptance Criteria

1. WHEN a transient WFT (`attempt > 1`) fails, THE Kernel SHALL NOT emit `WorkflowTaskFailed`; it SHALL
   increment the attempt and reschedule (transiently, per B.1). Only the attempt-1 failure emits
   `WorkflowTaskFailed`. (`workflow_task_state_machine.go:892 @ v1.31.0`.)
2. WHEN a transient WFT times out, THE Kernel SHALL NOT emit `WorkflowTaskTimedOut`; it SHALL reschedule
   transiently. (`workflow_task_state_machine.go:965-967 @ v1.31.0`.)
3. FOR a 10× failing WFT with no intervening events, the persisted history SHALL remain
   `[…, WorkflowTaskScheduled, WorkflowTaskStarted, WorkflowTaskFailed]` from attempt 1 only.

### Requirement B.4: Transient→Normal Conversion

**User Story:** As a Tokeira developer, I want a transient WFT to convert back to a normal attempt-1 task
when new events appear, so that the frozen virtual suffix is not invalidated and history stays valid.

#### Acceptance Criteria

1. WHEN new/buffered events appear **at scheduling time** (e.g. a signal arrives while continuously
   failing), THE Kernel SHALL reset `attempt = 1`, force the task NORMAL, and emit a real
   `WorkflowTaskScheduled` (this is Requirement A.1 generalized). (`workflow_task_state_machine.go:329-338
   @ v1.31.0`.)
2. WHEN new events have appeared **by start time** — detected as `scheduled_event_id != last_event_id + 1`
   (the virtual id no longer aligns) — THE Kernel SHALL reset `attempt = 1` and emit **both** a real
   `WorkflowTaskScheduled` and `WorkflowTaskStarted` before proceeding. (`workflow_task_state_machine.go:559-576
   @ v1.31.0`.) This produces the tests' `[6 WorkflowTaskScheduled, 7 WorkflowTaskStarted]` after a signal.

### Requirement B.5: Transient-Aware Terminate Force-Close

**User Story:** As a Tokeira developer, I want terminate's force-close to write nothing when the started
WFT is transient, so that terminating a continuously-failing workflow does not fabricate a
`WorkflowTaskFailed`.

#### Acceptance Criteria

1. WHEN a terminate (or message-too-large force-close) runs while the started WFT is transient, THE
   Kernel SHALL NOT emit a `WorkflowTaskFailed`; the terminate batch's first event SHALL be the
   `WorkflowExecutionTerminated` itself. (`service/history/workflow/util.go:117 @ v1.31.0`.) The Phase-1
   `force_close_started_workflow_task` (event-buffering) SHALL become transient-aware.
2. WHEN the started WFT is non-transient (attempt 1), THE existing force-close behavior (emit
   `WorkflowTaskFailed(ForceCloseCommand)` then flush) SHALL be preserved.

### Requirement B.6: Late Materialization on Successful Completion

**User Story:** As a Tokeira developer, I want a successful completion of a transient WFT to materialize
its Scheduled/Started events into history, so that a run that eventually succeeds has a complete history.

#### Acceptance Criteria

1. WHEN a transient WFT completes successfully, THE Kernel SHALL emit the previously-virtual
   `WorkflowTaskScheduled` and `WorkflowTaskStarted` events (with contiguous real ids) immediately before
   the `WorkflowTaskCompleted`, then proceed with the completion. (`workflow_task_state_machine.go:750-800
   @ v1.31.0`.)
2. After materialization, `last_event_id` SHALL reflect the newly-written Scheduled/Started/Completed
   events, and the run SHALL carry no residual virtual ids.

### Requirement B.7: Poll and GetHistory Synthesis (Runtime/Edge)

**User Story:** As an SDK worker/client, I want the unpersisted virtual Scheduled/Started suffix
synthesized on poll and on CLOSE-inclusive GetHistory, so that a worker can process a transient WFT and a
client sees a coherent history.

#### Acceptance Criteria

1. THE workflow-task poll response for a transient WFT SHALL carry the synthesized `WorkflowTaskScheduled`
   and `WorkflowTaskStarted` (with their virtual ids) so the worker's replay is complete.
   (`recordworkflowtaskstarted/api.go:430 @ v1.31.0`.)
2. THE `GetWorkflowExecutionHistory` response SHALL append the synthesized transient suffix on the last
   page (`appendTransientTasks`, `get_history_util.go @ v1.31.0`). The edge's GetHistory port already has
   the cached-transient plumbing stubbed; this requirement completes it.
3. Synthesis SHALL be a read/derived effect only — it SHALL NOT persist the virtual events.

---

## Item C — WFT Heartbeat Timeout (OUT OF SCOPE)

**Decision (2026-07-03, owner): out of scope; classify-skip.** `TestWorkflowTaskHeartbeatingWithEmptyResult`
depends on `OverrideDynamicConfig(WorkflowTaskHeartbeatTimeout = 5s)` against the default 30m
(`respondworkflowtaskcompleted/api.go:298`, `dynamicconfig constants.go:2427`). Per the conformance
config-as-constant convention, the 30m default is not operationally wrong for tokeira, so the heartbeat
timeout does not earn a new deployment knob merely to pass a test; the leaf is registered as an
**OverrideDynamicConfig out-of-scope skip** (same class as `MaxCallbacksPerWorkflow`), not implemented.
No `original_scheduled_at` field, no heartbeat-timeout config knob is added by this spec.

---

## Structural Invariants

### Requirement I.1: Event-Id Contiguity and Virtual-Id Discipline

#### Acceptance Criteria

1. FOR ALL persisted events, ids SHALL remain contiguous; virtual ids SHALL never be persisted except via
   materialization (B.6) or conversion (B.4).
2. FOR ALL transient transitions (schedule/start/fail/timeout at attempt>1), `last_event_id` SHALL be
   unchanged and the transition SHALL emit zero history events.
3. FOR ALL closed runs, there SHALL be no residual transient/virtual WFT state.

### Requirement I.2: At-Most-One-WFT Preserved

1. FOR ALL transient and conversion transitions, `next_state` SHALL contain at most one
   `PendingWorkflowTask`.

---

## Property Tests

### Requirement P1: Flush-Reset Schedules a Fresh Normal Task (Item A)
1. FOR an open run with a started WFT and a buffered signal, WHEN the WFT is failed, THE emitted history
   SHALL end with `WorkflowTaskFailed`, the flushed `WorkflowExecutionSignaled`, and a real
   `WorkflowTaskScheduled`, and `next_state.workflow_task_attempt` SHALL be 1.
   `// Feature: transient-wft, Property 1`

### Requirement P2: Transient Fail Persists Nothing
1. FOR an open run whose started WFT is at `attempt > 1`, WHEN it fails with no buffered events, THE
   transition SHALL emit zero history events, `last_event_id` SHALL be unchanged, and the attempt SHALL
   increment. `// Feature: transient-wft, Property 2`

### Requirement P3: Virtual Ids for Transient Schedule/Start
1. FOR a transient WFT, `scheduled_event_id == last_event_id + 1` and `started_event_id ==
   scheduled_event_id + 1`, with no events emitted. `// Feature: transient-wft, Property 3`

### Requirement P4: New Events Convert Transient→Normal
1. FOR a transient WFT, WHEN a signal buffers/arrives, the next scheduling SHALL reset `attempt = 1` and
   emit a real `WorkflowTaskScheduled` (and, if by start time, a real `WorkflowTaskStarted`).
   `// Feature: transient-wft, Property 4`

### Requirement P5: Successful Completion Materializes the Suffix
1. FOR a transient WFT that completes successfully, THE emitted history SHALL contain contiguous
   `WorkflowTaskScheduled`, `WorkflowTaskStarted`, `WorkflowTaskCompleted`, and `next_state` SHALL carry
   no virtual ids. `// Feature: transient-wft, Property 5`

### Requirement P6: Transient Force-Close Writes Nothing
1. FOR a transient started WFT, WHEN terminate force-closes it, THE emitted history SHALL be exactly
   `WorkflowExecutionTerminated` (no `WorkflowTaskFailed`). `// Feature: transient-wft, Property 6`

---

## Golden Transition Tests

### Requirement G1: 10× Transient Failure Keeps History Frozen
1. WHEN a WFT fails 10× with no intervening events, THE persisted history SHALL remain the attempt-1
   `[…, WorkflowTaskScheduled, WorkflowTaskStarted, WorkflowTaskFailed]`, matching the
   `TestWorkflowTaskTestSuite` transient leaves (`tests/workflow_task_test.go @ v1.31.0`).

### Requirement G2: Signal-Triggered Conversion Suffix
1. WHEN a transient WFT is interrupted by a signal, THE materialized history SHALL show the converted
   `WorkflowTaskScheduled`/`WorkflowTaskStarted` at the expected ids (the tests' `[6 Scheduled,
   7 Started]` after the signal).

---

## Out of Scope / Dependencies

- **Item C (heartbeat timeout)** — out of scope (above): OverrideDynamicConfig-dependent skip.
- **Speculative WFTs** — a separate mode (`WORKFLOW_TASK_TYPE_SPECULATIVE`) sharing the virtual-id
  machinery; not required by the Tier-1.2 leaves and not built here.
- **Sequencing (A before B):** Item A *adds* a real Scheduled event on the flush path; Item B *suppresses*
  attempt>1 events. They touch the same scheduling/`apply_workflow_task_failed` paths in opposite
  directions, so A lands and is golden-tested first, then B generalizes A's reset into the full
  conversion rules without invalidating A's goldens (see design.md).
