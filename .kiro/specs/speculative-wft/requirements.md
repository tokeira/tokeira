# Requirements Document: Speculative Workflow-Task Model (Kernel + Runtime + Edge)

## Introduction

This spec adopts Temporal's **speculative workflow-task model** (`WORKFLOW_TASK_TYPE_SPECULATIVE`)
into `tokeira-kernel` and the derived runtime/edge paths. It was raised by the Tier 2.12
update-suite map as the dominant history-shape gap behind `TestWorkflowUpdateSuite`: ~36 gated
leaves (leaf-inventory clusters 3/4/5/7/8) name speculative-WFT behaviour directly.

A speculative WFT is the **attempt-1 analogue of the transient WFT** (spec `transient-wft`,
accepted 2026-07-03), with one decisive difference: it needs its **own existence bit**. Transient
classification is derivable (`attempt > 1`); a speculative task is attempt 1 and must still be
distinguishable from a normal task **at completion time**, because that is when the model decides
*drop* (rejection-only completion — nothing ever persists, the SDK rewinds) versus *materialize*
(Scheduled/Started written late, exactly the transient late-materialization precedent).

Lifecycle in one line: an update admitted against a run with no pending WFT schedules a speculative
WFT (no persisted `WorkflowTaskScheduled`), dispatched **directly to matching** (sticky first, no
transfer task); the poll synthesizes the unpersisted Scheduled/Started suffix; completion commits
or rolls back; signals/heartbeats convert it to normal; timeouts persist it and retry.

Four riders land with the model because the same corpus leaves gate them: the worker-message
validation taxonomy (Req 6, the K4 invalid-command-seam extension), update-event attribute
fidelity (Req 7), the follow-up WFT for still-admitted updates (Req 8), and server-side rejection
of unprocessed updates (Req 9).

### Ground truth (v1.31.0, agent-verified)

- **Creation:** an update with no pending WFT calls
  `AddWorkflowTaskScheduledEvent(false, WORKFLOW_TASK_TYPE_SPECULATIVE)` — **no** persisted
  Scheduled event, `ScheduledEventID = GetNextEventID()` (virtual); buffered events downgrade it to
  NORMAL (`updateworkflow/api.go:171-197 @ v1.31.0`; `workflow_task_state_machine.go:309-410`).
- **Direct dispatch, no transfer task:** the API handler pushes the task to matching itself —
  sticky queue first, normal-queue retry on `StickyWorkerUnavailable`, other errors only logged
  (the 5s schedule-to-start timeout recovers) (`updateworkflow/api.go:218-252 @ v1.31.0`).
- **5s normal-queue schedule-to-start:** `tasks.SpeculativeWorkflowTaskScheduleToStartTimeout = 5s`,
  a hardcoded const, applied via an **in-memory** timer task (`workflow_task_timer.go:15-19`;
  `task_generator.go:466-512 @ v1.31.0`).
- **Poll synthesis:** the polled task carries unpersisted trailing Scheduled/Started events and
  `Messages[]` with `SequencingId.EventId = StartedEventID - 1` == the delivering (virtual)
  Scheduled id (`mutable_state_impl.go:1189-1252`; `recordworkflowtaskstarted/api.go:399-461`).
- **Drop vs commit:** `skipWorkflowTaskCompletedEvent` returns true iff SPECULATIVE ∧ no commands ∧
  not heartbeat ∧ events-window check ∧ every message is a Rejection; otherwise
  `AddWorkflowTaskCompletedEvent` writes Scheduled+Started late, then Completed
  (`workflow_task_state_machine.go:676-748, 750-819 @ v1.31.0`).
- **SDK rewind:** when the completed event is nil (dropped speculative),
  `resp.ResetHistoryEventId = LastCompletedWorkflowTaskStartedEventId`
  (`respondworkflowtaskcompleted/api.go:768-774 @ v1.31.0`); 0 on commit.
- **Convert-to-normal:** any transaction closing with a pending speculative WFT converts it —
  Scheduled (and Started if started) written now (`workflow_task_state_machine.go:1466-1530`).
- **Metrics:** `speculative_workflow_task_commits` / `_rollbacks` (`metric_defs.go:940-941`);
  timer-task operation tag `TimerActiveTaskSpeculativeWorkflowTaskTimeout` (`metric_defs.go:597`).

---

## Requirement 0: Architectural Decision — Adopt the Speculative-WFT Model (BLOCKING)

The kernel is **frozen** under the conformance conventions; this spec is the raise. It changes the
kernel WFT model (a second no-trace task mode beside transient), the `PendingWorkflowTask` shape,
the `WorkflowTaskFailedCause` enum, and the update-event fidelity surface — all Architectural
(AGENTS classification): **spec update AND explicit approval required before any kernel work.**

**User Story:** As the Tokeira owner, I want the speculative mode and its riders recorded with
their blast radius, so kernel changes are deliberate accepted decisions, not silent drift.

#### Acceptance Criteria

1. THE decision SHALL record: a speculative WFT persists nothing at schedule/start, is dispatched
   directly to matching, and at completion either drops without trace (rejection-only) or
   materializes late — requiring a new existence bit on `PendingWorkflowTask` (attempt-1, so the
   transient `attempt > 1` predicate cannot classify it).
2. THE decision SHALL record the rider kernel changes: `WorkflowTaskFailedCause::
   BadUpdateWorkflowExecutionMessage` (append-last), worker-provided acceptance sequencing id, a
   failure-capable `WorkflowExecutionUpdateCompleted` outcome (append-only event-model extension),
   and the follow-up speculative WFT on completion while admitted updates remain unsent.
3. UNTIL each phase lands, THE ~36 gated leaves SHALL remain red/classified; they flip per the
   tasks.md checkpoint. Harness-hazard siblings (CloseShard, nil AdminClient, OverrideDynamicConfig
   classes) SHALL be registered as skips before the first full suite run.
4. **Status: ACCEPTED — 2026-07-06, Kiro (owner-side review, see `owner-review.md`).**
   Acceptance is contingent on two design amendments, both landed the same day before any
   Phase K work:
   - **F1** — the completion wire-message model crossing into the kernel is a KERNEL-OWNED enum
     decoded at the edge; no proto type crosses the boundary (design.md K3 amendment).
   - **F2** — a pending speculative WFT's in-memory timers RE-ARM on bundle/shard load, derived
     from persisted state (design.md Runtime amendment; tasks R.2 + reload test in T.2).

   Carried as precision/robustness amendments (not blockers): **F3** (buffered-events-with-no-
   pending at the update site is an inconsistency error, tasks K.2), **F4** (resolved with
   evidence: commit/rollback leaves assert counts only, no reason tags — tasks M.1), **F5**
   (bounds-check the worker-provided sequencing id, design K6 + tasks K.6). Phase S (fork skip
   hygiene) was already landed in fork commit `9a9bd7bcb` ahead of acceptance.

---

## Glossary

- **Speculative WFT:** an attempt-1 `PendingWorkflowTask` flagged speculative. Scheduled/Started
  ids are virtual (`last_event_id + 1`, `+ 2`); nothing is persisted at schedule/start time.
- **Commit / materialize:** at completion, write the real Scheduled/Started (original times,
  attempt) immediately before `WorkflowTaskCompleted` — the transient-WFT late-materialization
  path reused at attempt 1.
- **Rollback / drop:** a rejection-only completion erases the task: zero history events, waiters
  still resolved with the rejection outcomes, SDK rewound via `ResetHistoryEventId`.
- **Convert-to-normal:** a speculative task becomes a normal persisted task when other events
  intrude (signal, heartbeat, timeout) — its events are written at conversion time.
- **K4 seam:** the existing invalid-command reject path (`Reject::InvalidCommandAttributes` →
  persisted `WorkflowTaskFailed` + `INVALID_ARGUMENT`, drop-without-persist on transient attempts;
  `crates/tokeira-runtime/src/runtime/workflow_task.rs:444-510`).

## Requirements

### Requirement 1: Speculative Creation and Direct Dispatch

**User Story:** As an SDK client sending an update to an idle workflow, I want the delivery WFT to
be speculative, so that a rejected update leaves no trace in history.

#### Acceptance Criteria

1. WHEN an update is admitted against an open run with no pending WFT and no buffered events, THE
   Kernel SHALL schedule a **speculative** WFT: no `WorkflowTaskScheduled` event, virtual
   `scheduled_event_id = last_event_id + 1`, `last_event_id` unchanged.
   (`updateworkflow/api.go:171-186`; `workflow_task_state_machine.go:309-410 @ v1.31.0`.)
2. WHEN buffered events exist at that moment, THE Kernel SHALL schedule a **normal** attempt-1 WFT
   instead (buffer flushed) — the transient downgrade rule generalized.
   (`workflow_task_state_machine.go:329-334 @ v1.31.0`.)
3. WHEN an update arrives while a WFT is scheduled-not-started, THE update SHALL attach to it (no
   new task); WHEN a WFT is already started, no task SHALL be created now (Req 8 handles it).
   (`updateworkflow/api.go:168-176 @ v1.31.0`.)
4. THE Runtime SHALL dispatch the speculative WFT **directly** to the broker/matching — no durable
   transfer/backlog record — sticky queue first, falling back to the normal queue on
   sticky-worker-unavailable; other dispatch errors are logged only (the Req 5 schedule-to-start
   timeout recovers). (`updateworkflow/api.go:218-252 @ v1.31.0`.)
5. A speculative dispatch with zero outgoing messages SHALL NOT reach a worker (v1.31.0: NotFound
   "No messages for speculative workflow task.", `recordworkflowtaskstarted/api.go:399-461`).

### Requirement 2: Poll-Side Synthesis and Message Anchoring

**User Story:** As an SDK worker, I want the polled speculative task to carry a complete history
and correctly anchored messages, so replay and update delivery line up.

#### Acceptance Criteria

1. THE poll response for a speculative WFT SHALL append synthesized `WorkflowTaskScheduled` +
   `WorkflowTaskStarted` at the virtual ids (started = scheduled + 1), reusing the transient
   suffix machinery (`crates/tokeira-edge/src/translate/from_internal.rs:47-76`;
   `append_transient_suffix`, `crates/tokeira-edge/src/workflow_service.rs:5414`), with the
   predicate widened from `attempt > 1` to `attempt > 1 ∨ speculative`.
   (`recordworkflowtaskstarted/api.go:430`; `mutable_state_impl.go:1189-1252 @ v1.31.0`.)
2. `Messages[]` SHALL carry `SequencingId.EventId = started_event_id - 1` — i.e. the delivering
   WFTScheduled id (empty-speculative = 5, non-empty = 6, first-normal-WFT = 2, signal-created = 6
   in the corpus histories). (`registry.Send`, `update/registry.go:327-358 @ v1.31.0`.)
3. `NextEventId` SHALL equal the virtual scheduled id; `PreviousStartedEventId` SHALL be the last
   **persisted** WFT-started id (a dropped speculative WFT never advances it).
4. GetHistory SHALL stay clean: speculative events appear only in the polled task's history, never
   in `GetWorkflowExecutionHistory`, until commit/conversion (synthesis is read-only).

### Requirement 3: Completion — Commit vs Rollback

**User Story:** As an SDK worker, I want a rejection-only completion of a speculative WFT to
vanish, and any substantive completion to persist it, matching v1.31.0 exactly.

#### Acceptance Criteria

1. WHEN a speculative WFT completes with **no commands, not a heartbeat, every message a
   Rejection** (or no messages), and the events-window check passes, THE Kernel SHALL drop it:
   zero history events, `last_event_id` unchanged, rejection outcomes still delivered to waiters,
   and the run left as if the task never existed.
   (`skipWorkflowTaskCompletedEvent`, `workflow_task_state_machine.go:676-748 @ v1.31.0`.)
2. ON drop, THE RespondWorkflowTaskCompleted response SHALL carry
   `ResetHistoryEventId = LastCompletedWorkflowTaskStartedEventId` so the SDK rewinds; ON commit
   it SHALL be 0. (`respondworkflowtaskcompleted/api.go:768-774 @ v1.31.0`.)
3. Events-window check: without the client capability, drop only if no events shipped after the
   previous WFT completed (`next_event_id ≤ last_completed_wft_started + 2`); with
   `client_discards_speculative_with_events` (already decoded at
   `crates/tokeira-edge/src/grpc/translate.rs:2312-2314`, reserved for this spec), up to
   `+ 2 + 10` — `DiscardSpeculativeWorkflowTaskMaximumEventsCount` pinned as a constant at its
   v1.31.0 default 10, per the config-as-constant convention.
   (`constants.go:2447-2451 @ v1.31.0`.) A non-empty speculative WFT (events shipped) therefore
   PERSISTS even on reject when the capability window is exceeded.
4. WHEN the completion carries commands, an acceptance/response message, or heartbeat, THE Kernel
   SHALL **convert**: write `WorkflowTaskScheduled` (original attempt/scheduled time) +
   `WorkflowTaskStarted` (original started time/request id) late, then `WorkflowTaskCompleted` —
   the transient late-materialization precedent at attempt 1.
   (`AddWorkflowTaskCompletedEvent`, `workflow_task_state_machine.go:750-819 @ v1.31.0`.)
5. ON drop, THE Runtime SHALL also roll back its started/sticky bookkeeping — start-to-close
   timeout tracking, broker in-flight state — so the next WFT dispatches cleanly (Invariant I.1).

### Requirement 4: Convert-to-Normal Triggers

#### Acceptance Criteria

1. WHEN a signal arrives while the speculative WFT is SCHEDULED, THE Kernel SHALL convert it in
   place: history shows `WorkflowTaskScheduled`, `WorkflowExecutionSignaled`, `WorkflowTaskStarted`
   (signal between scheduled and started).
   (`convertSpeculativeWorkflowTaskToNormal`, `workflow_task_state_machine.go:1466-1530 @ v1.31.0`.)
2. WHEN a signal buffers while the speculative WFT is STARTED, THE task SHALL persist and convert
   (history: Scheduled, Started, Completed, then the flushed Signaled).
3. WHEN the worker heartbeats a speculative WFT (`ForceCreateNewWorkflowTask`, no
   commands/messages), THE task SHALL convert (commit, reason `force_create_task`); the successor
   WFT SHALL be **normal**, and the update message SHALL NOT be redelivered on it (heartbeat sends
   only newly-admitted updates, `includeAlreadySent = !heartbeat`), while remaining rejectable by
   referencing the earlier request message.
   (`workflow_task_state_machine.go:690-698`; `update/registry.go:327-358 @ v1.31.0`.)

### Requirement 5: Timeouts

#### Acceptance Criteria

1. WHEN a **started** speculative WFT exceeds start-to-close, THE Kernel SHALL persist Scheduled +
   Started + `WorkflowTaskTimedOut(START_TO_CLOSE)`, increment the attempt, and schedule an
   attempt-2 **transient** retry on which the still-admitted update is redelivered; a late
   completion of the timed-out task gets NotFound "Workflow task not found.".
   (`workflow_task_state_machine.go:934-990`; `timer_queue_active_task_executor.go:364-462 @ v1.31.0`.)
2. WHEN a **scheduled** speculative WFT on the **sticky** queue exceeds the sticky
   schedule-to-start timeout, THE Kernel SHALL persist Scheduled + `WorkflowTaskTimedOut
   (SCHEDULE_TO_START)`, clear stickiness, **reset attempt to 1**, and schedule a real normal-queue
   WFT (real events); the update is redelivered. (`workflow_task_state_machine.go:270-306 @ v1.31.0`.)
3. ON the normal queue, THE schedule-to-start timeout SHALL be the hardcoded **5s**
   `SpeculativeWorkflowTaskScheduleToStartTimeout` (`workflow_task_timer.go:15-19 @ v1.31.0`),
   with the same persist-and-reschedule shape; the timed-out Scheduled event records the NORMAL
   queue kind. THE timer SHALL be runtime-in-memory (no durable timer record), invalidated when
   the tracked speculative task changes (stale-timer guard).

### Requirement 6: Worker-Message Validation Taxonomy (K4-seam rider)

#### Acceptance Criteria

1. THE Kernel SHALL gain `WorkflowTaskFailedCause::BadUpdateWorkflowExecutionMessage`,
   **appended last** — the enum is embedded in persisted events and postcard encodes variants by
   declaration index (discipline comment on `BadRequestCancelExternalWorkflowExecutionAttributes`,
   `crates/tokeira-kernel/src/command.rs:225-228`).
2. Bad update messages on RespondWorkflowTaskCompleted SHALL fail the WFT with that cause and the
   exact wire errors: NotFound "update {id} wasn't found on the server. This is most likely a
   transient error which will be resolved automatically by retries"
   (`workflow_task_completed_handler.go:381 @ v1.31.0`); InvalidArgument "ProtocolMessageCommand
   referenced absent message ID {id}" (`workflow_task_completed_handler.go:319`); InvalidArgument
   "invalid state transition attempted for Update {id}: received {type} message while in state
   {state}" (`update/update.go:648-656 @ v1.31.0`).
3. ON such a WFT failure, THE in-flight update waiter SHALL abort with WorkflowNotReady "Unable to
   perform workflow execution update due to unexpected workflow task failure."
   (`errors_failures.go:14 @ v1.31.0`); an explicit `RespondWorkflowTaskFailed` SHALL instead keep
   the update admitted and redeliver it (`TestSpeculativeWorkflowTask_Fail` distinction).
4. WHEN an acceptance for an unknown update carries the original request, THE update SHALL be
   **resurrected** from it and processed. (`TryResurrect`, `update/registry.go:238-281 @ v1.31.0`.)
5. Completion messages NOT referenced by a `PROTOCOL_MESSAGE` command SHALL be processed after
   commands, in request order (rejections and the no-commands success shape are currently dropped
   at `crates/tokeira-edge/src/translate/to_internal.rs:398-416`).

### Requirement 7: Update-Event Attribute Fidelity (rider)

#### Acceptance Criteria

1. `WorkflowExecutionUpdateAccepted.accepted_request_sequencing_event_id` SHALL carry the
   worker-provided value (hardcoded 0 today, `crates/tokeira-kernel/src/kernel.rs:4129`);
   `accepted_request_message_id` SHALL be the server's outgoing `"{update_id}/request"` id
   (serializer writes the bare update id today). (`event_factory.go:424-467 @ v1.31.0`.)
2. `WorkflowExecutionUpdateCompleted` SHALL be failure-capable: the kernel event's
   `result: Payloads` becomes a success/failure **outcome** (append-only event-model extension —
   see design), serialized with `Meta.UpdateId` and `accepted_event_id`; an update `Response`
   whose outcome is a Failure SHALL land as Completed-with-failure, not as a rejection (misroute
   today at `crates/tokeira-edge/src/grpc/translate.rs:2342-2367`).
   (`mutable_state_impl.go:5288-5378`; `update/validation.go:43-88 @ v1.31.0`.)

### Requirement 8: Follow-up WFT for Still-Admitted Updates (rider)

#### Acceptance Criteria

1. WHEN a WFT completes (not failing) while admitted updates remain unsent, THE next WFT SHALL be
   created **speculative** (normal if buffered events or heartbeat force it); today `apply_update`
   never re-kicks after the started WFT completes, stalling the caller.
   (`respondworkflowtaskcompleted/api.go:512-541 @ v1.31.0`;
   `crates/tokeira-kernel/src/kernel.rs:727-770`.)

### Requirement 9: Server-Side RejectUnprocessed (rider)

#### Acceptance Criteria

1. AFTER a completion that is not a heartbeat and not failing (and the run still open), updates
   still in Sent state SHALL be auto-rejected with the exact failure: Message "Workflow Update is
   rejected because it wasn't processed by worker. Probably, Workflow Update is not supported by
   the worker.", Source "Server", ApplicationFailureInfo Type "UnprocessedUpdate", NonRetryable.
   (`workflow_task_completed_handler.go:213-262`; `update/registry.go:297-317`;
   `errors_failures.go:18-25 @ v1.31.0`.)
2. THE rejected update SHALL NOT be redelivered; invariant: no update remains Sent after a
   completed WFT.

### Requirement 10: Metrics (rider)

#### Acceptance Criteria

1. THE Runtime SHALL emit namespace-labelled counters `speculative_workflow_task_commits` (on
   convert/commit, incl. heartbeat and command-bearing completions) and
   `speculative_workflow_task_rollbacks` (on drop). (`metric_defs.go:940-941 @ v1.31.0`.)
2. THE start-to-close timeout firing for a speculative WFT SHALL emit the timer-task metrics
   (`task_requests`, `start_to_close_timeout`) tagged operation
   `TimerActiveTaskSpeculativeWorkflowTaskTimeout`. (`metric_defs.go:597 @ v1.31.0`.)
3. THE fork's conformance scrape bridge SHALL gain the matching `tokeiraMetricRename` entries
   (`tests/testcore/tokeira_metrics_bridge.go:37-46`); leaves assert exact counts per namespace.

---

## Structural Invariants

### Requirement I.1: Rollback Leaves No Residue

1. FOR a dropped speculative WFT: zero history events, `last_event_id` and
   `PreviousStartedEventId` unchanged, no residual pending-task state, **and** no residual runtime
   bookkeeping (start-to-close/schedule-to-start timers disarmed, broker in-flight entry cleared,
   sticky state consistent) — the started/sticky rollback rides with the kernel drop.

### Requirement I.2: At-Most-One WFT; Modes Are Exclusive

1. FOR ALL transitions, `next_state` holds at most one `PendingWorkflowTask`; a task is exactly one
   of normal, transient (`attempt > 1`), or speculative (flag, attempt 1). A speculative task never
   survives a conversion or completion still flagged speculative.

## Out of Scope

- **ExecuteMultiOperation / Update-with-Start** — separate accepted spec
  (`api-conformance-multi-operation`); its running-attach paths *consume* this spec's speculative
  machinery but are not built here.
- **CloseShard / registry-loss / stale-speculative leaves** (cluster 9),
  **nil-AdminClient terminate leaves** (cluster 10), **OverrideDynamicConfig leaves** (cluster 11)
  — harness-class skips, registered before the first full run (tasks Phase S). The
  terminate-while-speculative *behaviour* (scheduled → no trace; started → convert then fail)
  falls out of Req 3/4 but is not corpus-gated here.
- **`TestSpeculativeWorkflowTask_QueryFailureClearsWFContext`** — couples to the Tier 2.10
  query-buffer machinery still open; tracked there, not gated here.
