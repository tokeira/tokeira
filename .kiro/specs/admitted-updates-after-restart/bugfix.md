# Bugfix Requirements Document

## Introduction

An update that a run admits but that its workflow hasn't yet accepted lives in two places in Tokeira. Its id is in the run's stored state, and its request is in the owning node's in-memory update registry. A restart, or a move of the run to another node, loses the registry and keeps the ids. Temporal v1.31.0 keeps such an update only in memory and forgets it when it reloads a run, so its client's retry is admitted as a new update (`NewRegistry` and `FindOrCreate`, `service/history/workflow/update/registry.go:168-236 @ v1.31.0`). Tokeira instead keeps an id whose request is gone, and that does three things wrong:
- a retry waits for an update that is never delivered;
- an empty speculative workflow task is scheduled for it, over and over;
- the continue-as-new advice counts it.

The opposite holds for updates a reset reapplies. v1.31.0 records each in history as WorkflowExecutionUpdateAdmitted, keeps it across reloads, and counts it in flight (`registry.go:189-204`; `service/history/ndc/workflow_resetter.go:880-923 @ v1.31.0`). Tokeira records the event but holds no request for the update, so the update limits leave it out.

This spec makes a run keep exactly the admitted updates v1.31.0 keeps, and count them as v1.31.0 counts them. It builds on [signal-update-limits](../signal-update-limits/bugfix.md), whose limits count the admitted updates the registry holds, and replaces that count with the run's own once the run holds only updates it can deliver.

## Bug Analysis

### Current Behavior (Defect)

1.1 WHEN a run's admitted, unaccepted update has no request in the registry, because the node restarted or the run moved, and its client retries with the same update id, THEN the system finds the id in the run's state and waits for the update, re-reading the run's history every 50 ms until the call times out, and answers it as admitted without an outcome. The update is never delivered, so every retry does the same (`update_workflow` and `wait_for_history_stage`, `crates/tokeira-runtime/src/runtime/query.rs`)

1.2 WHEN such an update is the run's only reason for a workflow task THEN the system schedules a speculative workflow task that carries nothing. The worker completes it empty, the completion is dropped, and the system schedules another for the same update, at the worker's pace, until the run closes:
- the empty completion is dropped, since it holds no command other than a rejection (`apply_workflow_task_completed`'s drop path, `crates/tokeira-kernel/src/kernel.rs`);
- the follow-up is scheduled because the admitted set isn't empty, on the drop path and after a normal completion alike.

1.3 WHEN such updates remain in a run's state THEN the continue-as-new advice counts them in flight, and suggests continuing as new sooner than v1.31.0 would

1.4 WHEN a run that a reset created holds updates the reset reapplied, as WorkflowExecutionUpdateAdmitted events THEN the update limits count none of them in flight, since the registry holds no request for them, while v1.31.0 counts each one (`with_held_updates`, `crates/tokeira-runtime/src/lane.rs`)

### Expected Behavior (Correct)

2.1 WHEN an admitted update that the run hasn't accepted has neither a request in the registry of the node that owns the run nor a WorkflowExecutionUpdateAdmitted event THEN the system SHALL forget it the next time it applies a command to the run, recording no event, as v1.31.0 forgets it when it reloads the run (`registry.go:168-224 @ v1.31.0`). Nothing else about the run SHALL change, except as 2.3 says.

2.2 WHEN a client sends an update whose id the run holds as admitted but the system would forget under 2.1 THEN the system SHALL admit it as a new update, with this request, under the update limits, and schedule a workflow task to deliver it when none is pending, as v1.31.0 admits a retry after a reload (`registry.go:226-236, 461-480`; `service/history/api/updateworkflow/api.go:161-188 @ v1.31.0`).

2.3 WHEN the system forgets updates under 2.1 and the run's pending workflow task is a speculative one that no worker has started and that has nothing left to deliver THEN the system SHALL drop that task, recording no event, as v1.31.0 keeps speculative tasks only in memory (`updateworkflow/api.go:216-251 @ v1.31.0`).

2.4 WHEN a workflow task completes THEN the system SHALL schedule a speculative workflow task for the run's admitted updates only if an admitted update whose request the registry holds remains undelivered. An update admitted by a WorkflowExecutionUpdateAdmitted event reaches the worker in history and SHALL NOT cause one. v1.31.0 sends a protocol message only for an update with a request (`needToSend` and `Send`, `service/history/workflow/update/update.go:404-437`; `service/history/api/respondworkflowtaskcompleted/api.go:512-542 @ v1.31.0`).

2.5 The updates a run counts in flight, for the update limits, for a worker's re-admission and for the continue-as-new advice, SHALL be:
- its accepted updates;
- its admitted updates whose requests the registry holds;
- its updates admitted by a WorkflowExecutionUpdateAdmitted event, whatever the registry holds.

That is v1.31.0's `len(r.updates)` after a reload (`registry.go:189-204, 367-369, 496-510 @ v1.31.0`). The in-flight payload limit SHALL CONTINUE TO count only the requests the registry holds.

2.6 A run's updates admitted by a WorkflowExecutionUpdateAdmitted event SHALL be stored with the run and SHALL survive a reload. They SHALL be the ones a reset reapplied, or that the replay of a copied history admitted, and not yet accepted, rejected or closed with the run. A run stored before this change SHALL hold none.

### Unchanged Behavior (Regression Prevention)

3.1 WHEN the registry holds the request of every admitted update THEN the system SHALL CONTINUE TO handle the run's updates as today.

3.2 A speculative workflow task's completion with no commands, and only rejections among its messages, SHALL CONTINUE TO be dropped, empty ones included, as v1.31.0 drops it (`skipWorkflowTaskCompletedEvent`, `service/history/workflow/workflow_task_state_machine.go:676-748 @ v1.31.0`).

3.3 A client update whose id the run has accepted, completed, or admitted by a WorkflowExecutionUpdateAdmitted event SHALL CONTINUE TO join that update.

3.4 A run's stored state SHALL CONTINUE TO encode to the same bytes as today while the run holds no update admitted by a WorkflowExecutionUpdateAdmitted event.

3.5 A reset SHALL CONTINUE TO reapply its eligible update events and schedule a workflow task after them.

### Out of Scope

- What a worker does with a WorkflowExecutionUpdateAdmitted event, and the UpdateAccepted event Tokeira records when the worker accepts such an update, whose request it takes from the acceptance.
- A speculative workflow task that a worker had started when the node restarted. v1.31.0 loses it with the node; Tokeira keeps it, and its empty completion is dropped without a follow-up (2.4).
- The schedule-to-start timer of a speculative workflow task that recovery republishes after a restart, which recovery doesn't re-arm.
- The admitted updates a run keeps when its copied history is replayed and the replay closes it.
- Updates a reset reapplied in a run stored before this change. Such a run holds no history-admitted updates (2.6), so after a restart those updates look lost and are forgotten, though their WorkflowExecutionUpdateAdmitted events would have delivered them through history. Only reset runs stored before the change, with reapplied updates still pending, are affected.
