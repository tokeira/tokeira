# Bugfix Requirements Document

## Introduction

When a workflow schedules an activity, history keeps the activity's input and header in its ActivityTaskScheduled event. Tokeira also copies them:
- into the run's state, whose entry for the activity keeps the input and the header until the activity finishes;
- the input into the activity's dispatch row (`activity_dispatch.input_data`) while the activity waits to start;
- the input into a backlog entry when the activity's task is spilled to the durable backlog.

Delivery uses only the state's copy: a start answers with the input and header of the activity's entry in the run's state (`start_activity_task_inner`, `crates/tokeira-runtime/src/runtime/activity.rs`). The dispatch row's and the backlog entry's copies are carried and written, never read.

Temporal v1.31.0 keeps an activity's input and header only in history:
- Its persisted activity info holds the scheduled event's id and the id of the history batch that holds the event, and no input or header (`ActivityInfo`, `proto/internal/temporal/server/api/persistence/v1/executions.proto:523-525 @ v1.31.0`).
- Recording an activity task's start reads the scheduled event (`GetActivityScheduledEvent`, `service/history/api/recordactivitytaskstarted/api.go:139-145`; `service/history/workflow/mutable_state_impl.go:1449-1485 @ v1.31.0`).
- The task the worker receives carries that event's input and header (`service/matching/matching_engine.go:3199-3203 @ v1.31.0`).

A run's state is one value, which every commit of the run writes whole. Holding the inputs makes it grow with each pending activity, even though each history batch stays small. DSQL refuses any value over 1,048,576 bytes ([bounded-bulk-writes](../bounded-bulk-writes/bugfix.md)). So a run whose pending activities' inputs add up past that can no longer commit anything, and every retry fails the same way. Before it reaches that point, every commit, a heartbeat included, rewrites every pending input.

The other copies cost more writes, and the backlog copy can stall the spill. Bounded-bulk-writes left an activity input over the value limit to this change: since the spill writes in pages, a backlog entry over the limit fails its page and holds back the entries after it on every pass.

This spec keeps an activity's input and header only in history, as v1.31.0 does. Starting an activity task reads its scheduled event with one bounded read.

## Bug Analysis

### Current Behavior (Defect)

1.1 WHEN a workflow has pending activities whose inputs, together, take its state past 1,048,576 bytes THEN DSQL refuses the state's value, and every later commit of the run fails, however small its own history batch is. For example, a workflow schedules one activity per workflow task, each with an input of about 200 KB. Each workflow task's history batch is about 200 KB. But once six activities are pending, the state is over 1 MiB, so the workflow task that schedules the sixth can't complete. After that, nothing else of that run commits.

1.2 WHEN any commit writes a run that has pending activities THEN it rewrites each pending activity's input and header with the state, a heartbeat's commit included.

1.3 WHEN an activity is scheduled, retried or re-dispatched THEN its dispatch row stores another copy of its input, and every update of the activity before it starts rewrites that copy.

1.4 WHEN an activity's task is spilled to the durable backlog THEN its backlog entry stores another copy of its input. An entry that the copy takes over 1,048,576 bytes fails its spill page and holds back the entries after it on every pass.

### Expected Behavior (Correct)

2.1 WHEN an activity is scheduled, started, retried, updated, paused, unpaused, reset or re-dispatched THEN the run's state, the activity's dispatch row and any backlog entry for its task SHALL hold no copy of its input or header. The ActivityTaskScheduled event SHALL be their only store, as in v1.31.0.

2.2 WHEN a worker is given an activity task THEN the task SHALL carry the input and header of the activity's ActivityTaskScheduled event. This covers a task the worker polls, and one it receives eagerly in the answer to the workflow task completion that scheduled the activity. The event SHALL be read from the run's history before the start commits. v1.31.0 also reads it before it records the start (`recordactivitytaskstarted/api.go:139, 242 @ v1.31.0`). For an eager start, v1.31.0 takes the input and header from the command it has just recorded as that event (`service/history/api/respondworkflowtaskcompleted/workflow_task_completed_handler.go:594-598 @ v1.31.0`), so the values are the same.

2.3 The read SHALL be one bounded read. It SHALL be one lookup by the run and the event's id, returning the history batch that holds the event and no other batch. That SHALL hold whatever the length of the run's history, and however its batches were cut, including a reset successor's copied batches.

2.4 IF the read fails THEN the start SHALL change nothing, and the task SHALL go back to its queue, as when the start's commit fails. IF the run's history holds no event with the activity's scheduled event id THEN the start SHALL change nothing and SHALL answer an internal error, as v1.31.0 answers `ErrMissingActivityScheduledEvent` (`mutable_state_impl.go:109-110, 1476-1481 @ v1.31.0`).

2.5 WHEN an activity's task is delivered THEN it SHALL carry its scheduled event's input and header, unchanged, as v1.31.0's tasks do. That SHALL hold:
- after a retry, an options update, a pause and unpause, or a reset of the activity;
- after its task is re-published by recovery, a backlog drain or dispatch reconciliation;
- for a reset successor's activity that was scheduled in its copied history.

2.6 WHEN this release reads a run's state written by the release before it THEN it SHALL read the state, and SHALL drop the inputs and headers that the state's activities hold, so the run's next write stores none. WHEN it reads a backlog entry or a dispatch row that carries an input THEN it SHALL read the entry or row and ignore the input.

2.7 This release SHALL write a run's state under a new envelope version, which the release before it refuses to read. Each activity's input and header SHALL keep their positions in the state's layout, written empty. So a state of the previous version decodes with the same layout, and no state needs converting on upgrade. Downgrade after the upgrade is unsupported: the earlier release refuses every state this one has written.

2.8 The measured state of the run growth limits ([run-growth-limits](../run-growth-limits/bugfix.md) criterion 2.9) SHALL be the state's encoded size, since the state no longer holds the inputs that the measure left out.

### Unchanged Behavior (Regression Prevention)

3.1 A delivered activity task SHALL CONTINUE TO carry its other fields as today: activity id and type, attempt, timeouts, heartbeat details, priority, its scheduled and started times, and its token.

3.2 A start SHALL CONTINUE TO be refused as today, changing nothing, in these cases:
- the attempt or stamp is stale;
- the activity has already started, is paused or is gone;
- the run is closed;
- a workflow rule applies;
- a deployment transition is in progress.

3.3 History SHALL CONTINUE TO hold each ActivityTaskScheduled event, with its input and header, as today.

3.4 Restoring an activity's original options SHALL CONTINUE TO read its scheduled event by the event's id ([history-pagination](../history-pagination/bugfix.md) criterion 2.8).

3.5 Nexus operations and standalone activities SHALL CONTINUE TO keep their inputs as today.

3.6 The run growth limits SHALL CONTINUE TO refuse, and terminate runs, at the sizes they do today for the same runs.

### Out of Scope

- A pending Nexus operation's input, which the run's state also copies (`PendingNexusOperation.input`). Its structures are separate from the activity's, and another change can move it to history the same way.
- Standalone activities, whose input lives in their own CHASM state.
- A history batch over DSQL's value limit, such as one workflow task that schedules activities whose inputs add up past it. Run-growth-limits leaves DSQL's value limit for history batches to a separate change.
- An events cache like v1.31.0's (`service/history/events/cache.go @ v1.31.0`). Here each start reads the store once.
- Dropping the `activity_dispatch.input_data` column. It stays, written empty.
